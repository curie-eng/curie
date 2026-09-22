//! Rotation-owner orchestration (ADR 0163 decision 7): read the rotated-key
//! backup, seed each rotated key create-if-absent, then apply the
//! ExternalSecret and PushSecret as one List.

use std::collections::BTreeMap;

use anyhow::{anyhow, Context, Result};
use serde::Serialize;
use serde_json::Value;

use super::eso::{apply, render_external_secret, render_push_secret, seed_key, Kubectl, SyncEntry};
use super::{ProviderError, SecretMaterial, SecretsProvider};

/// Attempts `seed_key` gets before giving up on a live conflict.
pub const SEED_ATTEMPTS: u32 = 5;

/// What seeding one rotated key did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeySeed {
    NoBackup,
    NotInBackup,
    Created,
    Added,
    AlreadyPresent,
}

/// One rotated key's seed outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct KeyReport {
    pub key: String,
    pub outcome: KeySeed,
}

/// The whole entry's apply result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ApplyReport {
    pub entry: String,
    pub seeds: Vec<KeyReport>,
}

/// Read the rotated-key backup object. `NotFound` is `Ok(None)`; any other
/// provider failure or malformed material is an error naming the object only.
pub fn read_backup(
    provider: &dyn SecretsProvider,
    name: &str,
) -> Result<Option<BTreeMap<String, SecretMaterial>>> {
    let stored = match provider.get(name, None) {
        Ok(stored) => stored,
        Err(ProviderError::NotFound { .. }) => return Ok(None),
        Err(err) => return Err(anyhow!("could not read backup {name}: {err}")),
    };
    let parsed: Value = serde_json::from_str(stored.material.expose())
        .map_err(|_| anyhow!("backup {name} is not a JSON object"))?;
    let object = parsed
        .as_object()
        .ok_or_else(|| anyhow!("backup {name} is not a JSON object"))?;
    let mut material = BTreeMap::new();
    for (key, value) in object {
        let value = value
            .as_str()
            .ok_or_else(|| anyhow!("backup {name} is not a JSON object of strings"))?;
        material.insert(key.clone(), SecretMaterial::new(value));
    }
    Ok(Some(material))
}

/// Read the backup once, seed each rotated key in declared order when the
/// backup holds a non-empty value for it, then apply the ExternalSecret and,
/// for a rotated entry, the PushSecret, as one List. A static entry (no
/// rotated keys) never calls the provider and applies only the
/// ExternalSecret.
pub fn apply_sync_entry(
    k: &dyn Kubectl,
    provider: &dyn SecretsProvider,
    entry: &SyncEntry,
    namespace: &str,
    store: &str,
    refresh_interval: &str,
) -> Result<ApplyReport> {
    let mut seeds = Vec::new();
    if !entry.rotated_keys.is_empty() {
        let backup_name = entry.backup_key();
        let backup = read_backup(provider, &backup_name)?;
        for key in &entry.rotated_keys {
            let value = backup.as_ref().and_then(|material| material.get(key));
            let outcome = match value {
                None if backup.is_none() => KeySeed::NoBackup,
                None => KeySeed::NotInBackup,
                Some(value) if value.expose().is_empty() => KeySeed::NotInBackup,
                Some(value) => {
                    let seeded = seed_key(k, namespace, &entry.target, key, value, SEED_ATTEMPTS)
                        .with_context(|| {
                        format!("seeding {key} into {}/{}", namespace, entry.target)
                    })?;
                    match seeded {
                        super::eso::SeedOutcome::Created => KeySeed::Created,
                        super::eso::SeedOutcome::Added => KeySeed::Added,
                        super::eso::SeedOutcome::AlreadyPresent => KeySeed::AlreadyPresent,
                    }
                }
            };
            seeds.push(KeyReport {
                key: key.clone(),
                outcome,
            });
        }
    }

    let mut objects = vec![render_external_secret(
        entry,
        namespace,
        store,
        refresh_interval,
    )];
    if let Some(push) = render_push_secret(entry, namespace, store) {
        objects.push(push);
    }
    apply(k, namespace, &objects)?;

    Ok(ApplyReport {
        entry: entry.name.clone(),
        seeds,
    })
}
