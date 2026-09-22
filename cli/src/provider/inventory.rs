//! One row of the install credential inventory.
//!
//! The row names a target, its keys, who rotates it, where it is stored, and
//! whether a later `set` may replace it. It does not carry values.

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

/// Where the value lives. `sm` means Secrets Manager is the source of truth
/// and ESO syncs the target. `cluster` means the value stays in-cluster and is
/// re-minted or re-provisioned on rebuild (ADR 0163 decision 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Store {
    Sm,
    Cluster,
}

/// How the chart points at this entry's keys: the Secret it renders when no
/// knob is set, and the values paths that redirect the reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChartBinding {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub knobs: Vec<ChartKnob>,
}

/// One `existingSecret` style knob: the values path naming the Secret and,
/// when the chart lets the key be renamed too, the path naming the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChartKnob {
    pub secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Placeholders a target or default_secret pattern may use. `{release}` and
/// `{fullname}` expand from the release; `{agent}` matches one agent name;
/// `{operator}` matches any name the operator chose.
pub const PLACEHOLDERS: [&str; 4] = ["release", "fullname", "agent", "operator"];

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
    pub store: Store,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub rotated_keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chart: Option<ChartBinding>,
}

impl InventoryEntry {
    /// Keys something other than the provider rotates: every key for a mint,
    /// the declared rotated keys (or every key when none are declared) for a
    /// workload, none for `sm`.
    pub fn rotation_owned_keys(&self) -> Vec<&str> {
        match &self.rotation_owner {
            RotationOwner::Sm => Vec::new(),
            RotationOwner::Mint(_) => self.keys.iter().map(String::as_str).collect(),
            RotationOwner::Workload(_) if self.rotated_keys.is_empty() => {
                self.keys.iter().map(String::as_str).collect()
            }
            RotationOwner::Workload(_) => self.rotated_keys.iter().map(String::as_str).collect(),
        }
    }

    /// Keys an ExternalSecret would own: every non-rotated key when the store
    /// is `sm`, none when the value stays in-cluster.
    pub fn eso_managed_keys(&self) -> Vec<&str> {
        match self.store {
            Store::Cluster => Vec::new(),
            Store::Sm => self
                .keys
                .iter()
                .filter(|key| !self.rotated_keys.contains(key))
                .map(String::as_str)
                .collect(),
        }
    }

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
        placeholders(&self.target, "target")?;
        if let Some(chart) = &self.chart {
            if let Some(default_secret) = &chart.default_secret {
                placeholders(default_secret, "chart.default_secret")?;
            }
            for knob in &chart.knobs {
                values_path(&knob.secret)?;
                if let Some(key) = &knob.key {
                    values_path(key)?;
                }
            }
        }
        let name = &self.logical_name;
        for rotated in &self.rotated_keys {
            if !self.keys.contains(rotated) {
                bail!("inventory entry {name}: rotated key {rotated} is not one of its keys");
            }
        }
        if !self.rotated_keys.is_empty()
            && !matches!(self.rotation_owner, RotationOwner::Workload(_))
        {
            bail!(
                "inventory entry {name}: rotated_keys {} need a workload:<name> rotation_owner, \
                 not {}",
                self.rotated_keys.join(", "),
                self.rotation_owner.tag()
            );
        }
        let managed = self.eso_managed_keys();
        if let Some(key) = self
            .rotation_owned_keys()
            .into_iter()
            .find(|key| managed.contains(key))
        {
            bail!(
                "inventory entry {name}: key {key} is rotation-owned by {} and ESO-managed \
                 (store: sm); ESO would revert the rotation. Mark it store: cluster, or list \
                 it in rotated_keys under a workload owner.",
                self.rotation_owner.tag()
            );
        }
        Ok(())
    }
}

/// Refuse an unknown `{...}` placeholder in a name pattern.
fn placeholders(pattern: &str, field: &str) -> Result<()> {
    let mut rest = pattern;
    while let Some(open) = rest.find('{') {
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            bail!("{field} {pattern} has an unclosed '{{'");
        };
        let name = &after[..close];
        if !PLACEHOLDERS.contains(&name) {
            bail!(
                "{field} {pattern} uses unknown placeholder {{{name}}}; known: {}",
                PLACEHOLDERS.map(|p| format!("{{{p}}}")).join(", ")
            );
        }
        rest = &after[close + 1..];
    }
    if rest.contains('}') {
        bail!("{field} {pattern} has an unmatched '}}'");
    }
    Ok(())
}

/// A dotted values path such as `postgres.existingSecret`.
fn values_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path
            .split('.')
            .any(|segment| token(segment, "knob").is_err())
    {
        bail!("chart knob {path:?} must be a dotted values path");
    }
    Ok(())
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
    store: Store,
    #[serde(default)]
    rotated_keys: Vec<String>,
    #[serde(default)]
    chart: Option<ChartBinding>,
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
            store: raw.store,
            rotated_keys: raw.rotated_keys,
            chart: raw.chart,
        };
        entry.validate().map_err(DeserializeError::custom)?;
        Ok(entry)
    }
}
