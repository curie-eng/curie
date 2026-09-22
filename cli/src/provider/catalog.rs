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

use super::inventory::{
    ChartBinding, ChartKnob, InventoryClass, InventoryEntry, RotationOwner, Store, UpdatePolicy,
};
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
/// `secret_files` keys) contributes to the sandbox entry, the per-agent Secret
/// the runner template reads. A hosted connector also contributes to the
/// hosted Secret its Deployment reads, and that is the only Secret a workload
/// can rotate, so `secret_rotation` lands there.
///
/// Keys aggregate by (target, key) before any entry is built: a key's
/// consumers are every connector declaring it, and a key one of them rotates
/// is owned by that workload. Two connectors rotating one key is refused. The
/// result does not depend on the order connectors are read in.
pub fn bundle_entries(decl: &ConnectorsFileDecl, agent: &str) -> Result<Vec<InventoryEntry>> {
    let sandbox_target = format!("{{fullname}}-agent-{agent}-connector-secrets");
    let hosted_target = format!("{{release}}-{agent}-connector-secrets");
    let mut sandbox_keys = BTreeSet::new();
    // key -> (connectors declaring it, connectors rotating it)
    let mut hosted: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)> = BTreeMap::new();
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
        let is_hosted = spec.url.is_none();
        if !spec.secret_rotation.is_empty() && !is_hosted {
            bail!("agent {agent} connector {connector}: secret_rotation needs a hosted connector");
        }
        sandbox_keys.extend(resolved.iter().cloned());
        if !is_hosted {
            continue;
        }
        for key in resolved {
            let (consumers, rotators) = hosted.entry(key.clone()).or_default();
            consumers.insert(connector.clone());
            // Parsing admits only `workload`, so a declared key is a rotated key.
            if spec.secret_rotation.contains_key(&key) {
                rotators.insert(connector.clone());
            }
        }
    }

    let mut entries = Vec::new();
    if !sandbox_keys.is_empty() {
        let mut sandbox = bundle_entry(
            format!("{agent}.sandbox"),
            sandbox_target,
            sandbox_keys.into_iter().collect(),
            vec!["runner".into()],
            RotationOwner::Sm,
            Vec::new(),
        );
        // The chart's per-agent BYO knob redirects the runner to a Secret the
        // operator (or ESO) owns; each key keeps its env var name.
        sandbox.chart = Some(ChartBinding {
            default_secret: None,
            knobs: vec![ChartKnob {
                secret: format!("agentSandbox.connectorExistingSecrets.{agent}.existingSecret"),
                key: None,
            }],
        });
        entries.push(sandbox);
    }

    // consumers -> rotator (None for provider-rotated keys) -> keys
    let mut groups: BTreeMap<Vec<String>, BTreeMap<Option<String>, Vec<String>>> = BTreeMap::new();
    for (key, (consumers, rotators)) in hosted {
        if rotators.len() > 1 {
            bail!(
                "agent {agent}: connectors {} all declare rotation on {key} in {hosted_target}; \
                 one key has one rotating workload",
                rotators.into_iter().collect::<Vec<_>>().join(" and ")
            );
        }
        groups
            .entry(consumers.into_iter().collect())
            .or_default()
            .entry(rotators.into_iter().next())
            .or_default()
            .push(key);
    }
    for (consumers, mut by_rotator) in groups {
        let base = format!("{agent}.{}.hosted", consumers.join("."));
        let static_keys = by_rotator.remove(&None).unwrap_or_default();
        if by_rotator.is_empty() {
            entries.push(bundle_entry(
                base,
                hosted_target.clone(),
                static_keys,
                consumers,
                RotationOwner::Sm,
                Vec::new(),
            ));
            continue;
        }
        // Keys the provider rotates ride with the first rotating workload's
        // entry; each further rotator gets an entry of its own.
        let several = by_rotator.len() > 1;
        let mut static_keys = Some(static_keys);
        for (rotator, rotated) in by_rotator {
            let rotator = rotator.expect("the provider group was removed");
            let mut keys = static_keys.take().unwrap_or_default();
            keys.extend(rotated.iter().cloned());
            keys.sort();
            let name = if several {
                format!("{base}.{rotator}")
            } else {
                base.clone()
            };
            entries.push(bundle_entry(
                name,
                hosted_target.clone(),
                keys,
                consumers.clone(),
                RotationOwner::Workload(rotator),
                rotated,
            ));
        }
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
