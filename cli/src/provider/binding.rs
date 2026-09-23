//! Inventory rows these cluster writers store, and JSON key merges on top of
//! [`SecretsProvider`].

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};

use super::{platform_inventory, ProviderError, PutRequest, SecretMaterial, SecretsProvider};

/// One inventory row rendered for a release: the object name, the JSON key,
/// the Secret the chart should reference, and the two helm knobs that do it.
pub struct HelmRef {
    pub logical_name: &'static str,
    pub key: String,
    pub target: String,
    pub secret_knob: String,
    pub key_knob: String,
}

pub fn helm_ref(logical_name: &'static str, release: &str) -> Result<HelmRef> {
    let entries = platform_inventory()?;
    let entry = entries
        .into_iter()
        .find(|entry| entry.logical_name == logical_name)
        .with_context(|| format!("platform inventory has no {logical_name}"))?;
    let key = entry
        .keys
        .into_iter()
        .next()
        .with_context(|| format!("inventory entry {logical_name} has no key"))?;
    let knob = entry
        .chart
        .and_then(|chart| chart.knobs.into_iter().next())
        .with_context(|| format!("inventory entry {logical_name} has no chart knob"))?;
    let key_knob = knob
        .key
        .with_context(|| format!("inventory entry {logical_name} knob has no key path"))?;
    let target = entry.target.replace("{release}", release);
    if target.contains('{') {
        bail!("inventory target for {logical_name} needs more than the release name");
    }
    Ok(HelmRef {
        logical_name,
        key,
        target,
        secret_knob: knob.secret,
        key_knob,
    })
}

/// Insert one JSON key into an object, creating it when absent. Sibling keys
/// stay. The write uses the current version as a precondition.
pub fn merge_json_key(
    provider: &dyn SecretsProvider,
    name: &str,
    key: &str,
    value: &str,
) -> Result<()> {
    if value.is_empty() {
        bail!("refusing to store an empty value for {name}/{key}");
    }
    let (mut values, expected) = match provider.get(name, None) {
        Ok(stored) => {
            let values: BTreeMap<String, String> =
                serde_json::from_str(stored.material.expose())
                    .with_context(|| format!("provider object {name} is not a JSON string map"))?;
            (values, Some(stored.version.id))
        }
        Err(ProviderError::NotFound { .. }) => (BTreeMap::new(), None),
        Err(error) => return Err(error.into()),
    };
    values.insert(key.to_string(), value.to_string());
    let material =
        SecretMaterial::new(serde_json::to_string(&values).context("serializing provider object")?);
    provider.put(&PutRequest {
        name,
        material: &material,
        expected_version: expected.as_deref(),
    })?;
    Ok(())
}

/// Drop one JSON key. A missing object or key is success. An object with no
/// keys left is deleted.
pub fn remove_json_key(provider: &dyn SecretsProvider, name: &str, key: &str) -> Result<()> {
    let (mut values, expected) = match provider.get(name, None) {
        Ok(stored) => {
            let values: BTreeMap<String, String> =
                serde_json::from_str(stored.material.expose())
                    .with_context(|| format!("provider object {name} is not a JSON string map"))?;
            (values, stored.version.id)
        }
        Err(ProviderError::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if values.remove(key).is_none() {
        return Ok(());
    }
    if values.is_empty() {
        provider.delete(name, Some(&expected))?;
        return Ok(());
    }
    let material =
        SecretMaterial::new(serde_json::to_string(&values).context("serializing provider object")?);
    provider.put(&PutRequest {
        name,
        material: &material,
        expected_version: Some(&expected),
    })?;
    Ok(())
}
