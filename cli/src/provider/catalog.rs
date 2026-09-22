//! The platform credential inventory and its bundle extension (ADR 0163
//! decision 4).
//!
//! The platform half is checked in as `platform-inventory.yaml` and embedded,
//! so a released binary carries the same list CI checked. Bundles extend it by
//! name only: each connector's Curie-resolved secret names become entries for
//! the per-agent Secrets the deploy path writes.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use super::inventory::{InventoryClass, InventoryEntry, RotationOwner, Store, UpdatePolicy};
use crate::connector_build::{ConnectorsFileDecl, SecretDecl};

/// The checked-in platform inventory.
pub const PLATFORM_INVENTORY: &str = include_str!("platform-inventory.yaml");

/// The only inventory document shape this build reads.
const INVENTORY_VERSION: u32 = 1;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryDoc {
    version: u32,
    entries: Vec<InventoryEntry>,
}

/// The embedded platform inventory, parsed and validated.
pub fn platform_inventory() -> Result<Vec<InventoryEntry>> {
    parse_inventory(PLATFORM_INVENTORY).context("embedded platform inventory")
}

/// Parse an inventory document. Each entry validates on its own while it is
/// read; the whole list then passes [`validate_inventory`].
pub fn parse_inventory(text: &str) -> Result<Vec<InventoryEntry>> {
    let doc: InventoryDoc = serde_norway::from_str(text).context("parse inventory")?;
    if doc.version != INVENTORY_VERSION {
        bail!(
            "inventory version {} is not supported; this build reads version {INVENTORY_VERSION}",
            doc.version
        );
    }
    validate_inventory(&doc.entries)?;
    Ok(doc.entries)
}

/// Whole-inventory rules on top of each entry's own: a logical name appears
/// once, and a (target, key) pair belongs to exactly one entry, so one key has
/// one owner, one store and one update policy.
pub fn validate_inventory(entries: &[InventoryEntry]) -> Result<()> {
    let mut names = BTreeSet::new();
    let mut pairs: BTreeMap<(&str, &str), &str> = BTreeMap::new();
    for entry in entries {
        entry
            .validate()
            .with_context(|| format!("inventory entry {}", entry.logical_name))?;
        if !names.insert(entry.logical_name.as_str()) {
            bail!(
                "inventory lists logical_name {} more than once",
                entry.logical_name
            );
        }
        for key in &entry.keys {
            if let Some(first) =
                pairs.insert((entry.target.as_str(), key.as_str()), &entry.logical_name)
            {
                bail!(
                    "inventory lists target {} key {key} twice, in {first} and {}",
                    entry.target,
                    entry.logical_name
                );
            }
        }
    }
    Ok(())
}

/// Inventory entries one agent's bundle adds.
///
/// Every connector with Curie-resolved names (bare `secrets` plus
/// `secret_files` keys) contributes a sandbox entry, the per-agent Secret the
/// runner template reads. A hosted connector also contributes a hosted entry,
/// the Secret its Deployment reads, and that is the only Secret a workload can
/// rotate, so `secret_rotation` lands there. A key two connectors share is
/// listed once per target, under the first connector; a later connector that
/// declares rotation on a key it does not own is refused.
pub fn bundle_entries(decl: &ConnectorsFileDecl, agent: &str) -> Result<Vec<InventoryEntry>> {
    let sandbox_target = format!("{{fullname}}-agent-{agent}-connector-secrets");
    let hosted_target = format!("{{release}}-{agent}-connector-secrets");
    let mut sandbox_seen = BTreeSet::new();
    let mut hosted_seen = BTreeSet::new();
    let mut entries = Vec::new();
    for (connector, spec) in &decl.connectors {
        let resolved: Vec<String> = spec
            .secrets
            .iter()
            .filter_map(|declared| match declared {
                SecretDecl::Name(name) => Some(name.clone()),
                SecretDecl::Ref { .. } => None,
            })
            .chain(spec.secret_files.keys().cloned())
            .collect();
        for rotated in spec.secret_rotation.keys() {
            if !resolved.contains(rotated) {
                bail!(
                    "agent {agent} connector {connector}: secret_rotation names {rotated}, \
                     which is not a name Curie resolves for it"
                );
            }
        }
        let hosted = spec.url.is_none();
        if !spec.secret_rotation.is_empty() && !hosted {
            bail!("agent {agent} connector {connector}: secret_rotation needs a hosted connector");
        }

        let sandbox_keys: Vec<String> = resolved
            .iter()
            .filter(|key| sandbox_seen.insert((*key).clone()))
            .cloned()
            .collect();
        if !sandbox_keys.is_empty() {
            entries.push(bundle_entry(
                format!("{agent}.{connector}.sandbox"),
                sandbox_target.clone(),
                sandbox_keys,
                vec!["runner".into()],
                RotationOwner::Sm,
                Vec::new(),
            ));
        }

        if !hosted {
            continue;
        }
        let mut hosted_keys = Vec::new();
        for key in &resolved {
            if hosted_seen.insert(key.clone()) {
                hosted_keys.push(key.clone());
            } else if spec.secret_rotation.contains_key(key) {
                bail!(
                    "agent {agent} connector {connector}: declares rotation on {key}, which \
                     an earlier connector already lists in {hosted_target}"
                );
            }
        }
        if hosted_keys.is_empty() {
            continue;
        }
        let rotated: Vec<String> = spec.secret_rotation.keys().cloned().collect();
        let owner = if rotated.is_empty() {
            RotationOwner::Sm
        } else {
            RotationOwner::Workload(connector.clone())
        };
        entries.push(bundle_entry(
            format!("{agent}.{connector}.hosted"),
            hosted_target.clone(),
            hosted_keys,
            vec![connector.clone()],
            owner,
            rotated,
        ));
    }
    for entry in &entries {
        entry
            .validate()
            .with_context(|| format!("bundle entry {}", entry.logical_name))?;
    }
    Ok(entries)
}

fn bundle_entry(
    logical_name: String,
    target: String,
    keys: Vec<String>,
    consumers: Vec<String>,
    rotation_owner: RotationOwner,
    rotated_keys: Vec<String>,
) -> InventoryEntry {
    InventoryEntry {
        logical_name,
        class: InventoryClass::External,
        target,
        keys,
        consumers,
        rotation_owner,
        update_policy: UpdatePolicy::Replace,
        store: Store::Sm,
        rotated_keys,
        chart: None,
    }
}

/// The platform inventory plus every bundle's entries, validated as one.
pub fn merge(
    platform: Vec<InventoryEntry>,
    bundles: Vec<InventoryEntry>,
) -> Result<Vec<InventoryEntry>> {
    let mut all = platform;
    all.extend(bundles);
    validate_inventory(&all)?;
    Ok(all)
}
