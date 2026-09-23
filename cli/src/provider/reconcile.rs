//! Decide whether a provider write also syncs the cluster.
//!
//! An install is provisioned only when its namespace and SecretStore both
//! exist. Anything short of that, including no kubeconfig, stops after Secrets
//! Manager. kubectl is not started when no kubeconfig file is present.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Result};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::{Duration as TimeDuration, OffsetDateTime};

use super::eso::{
    self, apply, force_sync_and_wait, rollout_consumers, Kubectl, SyncEntry,
    PROVIDER_VERSION_ANNOTATION,
};
use super::{InventoryEntry, ObjectMetadata, Store, UpdatePolicy, EXPIRY_TAG};

pub const REFRESH_INTERVAL: &str = "10s";
pub const SYNC_TIMEOUT: Duration = Duration::from_secs(180);
pub const SYNC_POLL: Duration = Duration::from_secs(2);
pub const ROLLOUT_TIMEOUT: Duration = Duration::from_secs(180);

/// SecretStore name `curie apply` and `curie secrets` both address.
pub fn store_name(release: &str) -> String {
    format!("{release}-aws")
}

/// Chart fullname when the chart name is `curie` and no override is set.
pub fn fullname(release: &str) -> String {
    if release.contains("curie") {
        release.to_string()
    } else {
        format!("{release}-curie")
    }
}

/// Deployment an inventory consumer token rolls.
pub fn deployment_name(release: &str, consumer: &str) -> String {
    format!("{}-{consumer}", fullname(release))
}

/// How far a standalone secrets command can see the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    NoKubeconfig,
    Unreachable,
    NamespaceAbsent,
    StoreAbsent,
    Ready,
}

impl Reach {
    pub fn provisioned(self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn detail(self, namespace: &str, store: &str) -> String {
        match self {
            Self::NoKubeconfig => "no kubeconfig".to_string(),
            Self::Unreachable => "kubernetes API is unreachable".to_string(),
            Self::NamespaceAbsent => format!("namespace {namespace} is absent"),
            Self::StoreAbsent => format!("SecretStore {namespace}/{store} is absent"),
            Self::Ready => "provisioned".to_string(),
        }
    }
}

/// First existing kubeconfig file, if any. A set but missing `KUBECONFIG`
/// does not fall through to `~/.kube/config`.
pub fn kubeconfig_file() -> Option<PathBuf> {
    if let Some(raw) = std::env::var_os("KUBECONFIG") {
        return std::env::split_paths(&raw).find(|path| path.is_file());
    }
    let home = std::env::var_os("HOME")?;
    let path = Path::new(&home).join(".kube/config");
    path.is_file().then_some(path)
}

/// Read namespace and SecretStore. `None` means the caller already knows
/// there is no kubeconfig and must not start kubectl.
pub fn reach(k: Option<&dyn Kubectl>, namespace: &str, store: &str) -> Result<Reach> {
    let Some(k) = k else {
        return Ok(Reach::NoKubeconfig);
    };
    match k.run(&argv(&["get", "namespace", namespace, "-o", "name"]), None)? {
        output if output.success => {}
        output if not_found(&output.stderr) => return Ok(Reach::NamespaceAbsent),
        output if unreachable(&output.stderr) => return Ok(Reach::Unreachable),
        output => bail!(
            "could not read namespace {namespace}: {}",
            one_line(&output.stderr)
        ),
    }
    match k.run(
        &argv(&["-n", namespace, "get", "secretstore", store, "-o", "name"]),
        None,
    )? {
        output if output.success => Ok(Reach::Ready),
        output if not_found(&output.stderr) || missing_type(&output.stderr) => {
            Ok(Reach::StoreAbsent)
        }
        output if unreachable(&output.stderr) => Ok(Reach::Unreachable),
        output => bail!(
            "could not read SecretStore {namespace}/{store}: {}",
            one_line(&output.stderr)
        ),
    }
}

/// Apply the ExternalSecret for one inventory row and roll only its consumers.
pub fn publish(
    k: &dyn Kubectl,
    namespace: &str,
    release: &str,
    scoped_prefix: &str,
    entry: &InventoryEntry,
    version: &str,
) -> Result<Vec<String>> {
    let expanded = expand_target(entry, release);
    let sync = SyncEntry::from_inventory(&expanded, scoped_prefix)?;
    let store = store_name(release);
    let mut objects = vec![eso::render_external_secret(
        &sync,
        namespace,
        &store,
        REFRESH_INTERVAL,
    )];
    if let Some(push) = eso::render_push_secret(&sync, namespace, &store) {
        objects.push(push);
    }
    apply(k, namespace, &objects)?;
    force_sync_and_wait(k, namespace, &sync.name, SYNC_TIMEOUT, SYNC_POLL)?;
    let deployments: Vec<String> = expanded
        .consumers
        .iter()
        .map(|consumer| deployment_name(release, consumer))
        .collect();
    rollout_consumers(k, namespace, &deployments, version, ROLLOUT_TIMEOUT)?;
    Ok(expanded.consumers.clone())
}

/// Delete the ExternalSecret and rotated-key backup, ignoring ones that are
/// already gone. A missing ExternalSecret CRD means nothing was installed.
pub fn delete_objects(k: &dyn Kubectl, namespace: &str, logical: &str) -> Result<()> {
    for (kind, name) in [
        ("externalsecret", logical.to_string()),
        ("pushsecret", format!("{logical}-rotated-backup")),
    ] {
        let output = k.run(
            &argv(&["-n", namespace, "delete", kind, &name, "--ignore-not-found"]),
            None,
        )?;
        if output.success || not_found(&output.stderr) || missing_type(&output.stderr) {
            continue;
        }
        bail!(
            "could not delete {kind} {namespace}/{name}: {}",
            one_line(&output.stderr)
        );
    }
    Ok(())
}

/// Pod-template provider version for one consumer, or `None` when the
/// Deployment is absent.
pub fn consumer_stamp(
    k: &dyn Kubectl,
    namespace: &str,
    release: &str,
    consumer: &str,
) -> Result<Option<String>> {
    let deployment = deployment_name(release, consumer);
    let output = k.run(
        &argv(&[
            "-n",
            namespace,
            "get",
            "deployment",
            &deployment,
            "-o",
            "json",
        ]),
        None,
    )?;
    if not_found(&output.stderr) {
        return Ok(None);
    }
    if !output.success {
        bail!(
            "could not read Deployment {namespace}/{deployment}: {}",
            one_line(&output.stderr)
        );
    }
    let document: Value = serde_json::from_str(&output.stdout).map_err(|_| {
        anyhow::anyhow!("Deployment {namespace}/{deployment} returned invalid JSON")
    })?;
    Ok(
        document["spec"]["template"]["metadata"]["annotations"][PROVIDER_VERSION_ANNOTATION]
            .as_str()
            .map(str::to_string),
    )
}

/// One `secrets check` row. Values are never copied onto it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectCheck {
    pub name: String,
    pub version: String,
    pub expires_at: Option<String>,
    pub status: &'static str,
    pub missing_keys: Vec<String>,
    pub stale_consumers: Vec<String>,
}

pub fn assess(
    metadata: &ObjectMetadata,
    entry: Option<&InventoryEntry>,
    now: OffsetDateTime,
    provisioned: bool,
    stamps: &BTreeMap<String, Option<String>>,
) -> Result<ObjectCheck> {
    let expires_at = metadata.tags.get(EXPIRY_TAG).cloned();
    let expiry = match expires_at.as_deref() {
        None => "ok",
        Some(raw) => {
            let parsed = OffsetDateTime::parse(raw, &Rfc3339)
                .map_err(|_| anyhow::anyhow!("expiry timestamp {raw:?} must be RFC 3339"))?;
            if parsed <= now {
                "expired"
            } else if parsed - now <= TimeDuration::days(30) {
                "warning"
            } else {
                "ok"
            }
        }
    };
    let mut missing_keys = Vec::new();
    if let Some(entry) = entry.filter(|entry| entry.store == Store::Sm) {
        for key in &entry.keys {
            if !metadata.key_names.iter().any(|present| present == key) {
                missing_keys.push(key.clone());
            }
        }
    }
    let mut stale_consumers = Vec::new();
    if provisioned {
        if let Some(entry) = entry.filter(|entry| entry.store == Store::Sm) {
            for consumer in &entry.consumers {
                let stamp = stamps.get(consumer).cloned().flatten();
                if stamp.as_deref() != Some(metadata.version.id.as_str()) {
                    stale_consumers.push(consumer.clone());
                }
            }
        }
    }
    let status = if expiry == "expired" {
        "expired"
    } else if !missing_keys.is_empty() {
        "missing"
    } else if !stale_consumers.is_empty() {
        "stale"
    } else {
        expiry
    };
    Ok(ObjectCheck {
        name: metadata.name.clone(),
        version: metadata.version.id.clone(),
        expires_at,
        status,
        missing_keys,
        stale_consumers,
    })
}

pub fn refuses_set(entry: &InventoryEntry, key: &str) -> Option<String> {
    if entry.store != Store::Sm {
        return Some(format!(
            "{} stays in the cluster and is not written to the provider",
            entry.logical_name
        ));
    }
    if !entry.keys.iter().any(|declared| declared == key) {
        return Some(format!(
            "{key} is not a key of inventory entry {}",
            entry.logical_name
        ));
    }
    if entry.update_policy == UpdatePolicy::Immutable {
        return Some(format!(
            "refusing to set {}/{key}: inventory update policy is immutable",
            entry.logical_name
        ));
    }
    None
}

fn expand_target(entry: &InventoryEntry, release: &str) -> InventoryEntry {
    let mut copy = entry.clone();
    let full = fullname(release);
    copy.target = copy
        .target
        .replace("{release}", release)
        .replace("{fullname}", &full);
    copy
}

fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| (*part).to_string()).collect()
}

fn not_found(stderr: &str) -> bool {
    stderr.contains("(NotFound)")
}

fn missing_type(stderr: &str) -> bool {
    stderr.contains("doesn't have a resource type")
        || stderr.contains("the server doesn't have a resource type")
}

fn unreachable(stderr: &str) -> bool {
    let lower = stderr.to_ascii_lowercase();
    [
        "connection refused",
        "unable to connect",
        "dial tcp",
        "no such host",
        "i/o timeout",
        "context deadline exceeded",
        "network is unreachable",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn one_line(stderr: &str) -> &str {
    stderr
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("kubectl failed")
}
