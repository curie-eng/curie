//! One row of the install credential inventory.
//!
//! The row names a target, its keys, who rotates it, and whether a later
//! `set` may replace it. It does not carry values.

use anyhow::{bail, Result};
use serde::de::Error as DeserializeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// `external` is a third party credential. `stateful` is bound to persisted
/// data. `throwaway` is regenerated with its consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryClass {
    External,
    Stateful,
    Throwaway,
}

/// `replace` may be overwritten. `immutable` is refused by `set`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpdatePolicy {
    Replace,
    Immutable,
}

/// Who rotates the value. `sm` is the provider. `workload:<name>` is a
/// consumer that writes the value back. `mint:<name>` is a Curie mint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RotationOwner {
    Sm,
    Workload(String),
    Mint(String),
}

impl RotationOwner {
    fn tag(&self) -> String {
        match self {
            Self::Sm => "sm".to_string(),
            Self::Workload(name) => format!("workload:{name}"),
            Self::Mint(name) => format!("mint:{name}"),
        }
    }

    fn parse(raw: &str) -> std::result::Result<Self, String> {
        if raw == "sm" {
            return Ok(Self::Sm);
        }
        if let Some(name) = raw.strip_prefix("workload:") {
            return named(name, "workload").map(Self::Workload);
        }
        if let Some(name) = raw.strip_prefix("mint:") {
            return named(name, "mint").map(Self::Mint);
        }
        Err(format!(
            "rotation_owner must be sm, workload:<name>, or mint:<name>, not {raw:?}"
        ))
    }
}

fn named(name: &str, kind: &str) -> std::result::Result<String, String> {
    if name.is_empty() || name.contains(':') || name.chars().any(char::is_whitespace) {
        return Err(format!(
            "rotation_owner {kind} name must be a non-empty token without whitespace or ':', not {name:?}"
        ));
    }
    Ok(name.to_string())
}

impl Serialize for RotationOwner {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.tag())
    }
}

/// A checked-in inventory row. Deserialize rejects an incomplete or unknown row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct InventoryEntry {
    pub logical_name: String,
    pub class: InventoryClass,
    pub target: String,
    pub keys: Vec<String>,
    pub consumers: Vec<String>,
    pub rotation_owner: RotationOwner,
    pub update_policy: UpdatePolicy,
}

impl InventoryEntry {
    pub fn validate(&self) -> Result<()> {
        token(&self.logical_name, "logical_name")?;
        if self.target.is_empty()
            || self.target.len() > 253
            || self.target.chars().any(char::is_whitespace)
        {
            bail!("target must be a non-empty name of at most 253 characters");
        }
        if self.keys.is_empty() {
            bail!("keys must not be empty");
        }
        for key in &self.keys {
            token(key, "keys")?;
        }
        for consumer in &self.consumers {
            if consumer.is_empty() || consumer.chars().any(char::is_whitespace) {
                bail!("consumers must be non-empty names");
            }
        }
        match &self.rotation_owner {
            RotationOwner::Sm => {}
            RotationOwner::Workload(name) | RotationOwner::Mint(name) => {
                if name.is_empty() || name.contains(':') {
                    bail!("rotation_owner name must be a non-empty token");
                }
            }
        }
        Ok(())
    }
}

fn token(value: &str, field: &str) -> Result<()> {
    let ok = !value.is_empty()
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || "._-".contains(character));
    if !ok {
        bail!("{field} must use letters, digits, '.', '_', or '-'");
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InventoryEntryRaw {
    logical_name: String,
    class: InventoryClass,
    target: String,
    keys: Vec<String>,
    consumers: Vec<String>,
    rotation_owner: String,
    update_policy: UpdatePolicy,
}

impl<'de> Deserialize<'de> for InventoryEntry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let raw = InventoryEntryRaw::deserialize(deserializer)?;
        let rotation_owner =
            RotationOwner::parse(&raw.rotation_owner).map_err(DeserializeError::custom)?;
        let entry = Self {
            logical_name: raw.logical_name,
            class: raw.class,
            target: raw.target,
            keys: raw.keys,
            consumers: raw.consumers,
            rotation_owner,
            update_policy: raw.update_policy,
        };
        entry.validate().map_err(DeserializeError::custom)?;
        Ok(entry)
    }
}
