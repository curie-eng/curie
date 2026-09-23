//! Decide whether a provider write also syncs the cluster.
//!
//! An install is provisioned only when its namespace and SecretStore both
//! exist. Anything short of that, including no kubeconfig, stops after Secrets
//! Manager. kubectl is not started when no kubeconfig file is present.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use time::format_description::well_known::Rfc3339;
use time::{Duration as TimeDuration, OffsetDateTime};

use super::eso::{
    self, apply, force_sync_and_wait, Kubectl, SyncEntry, PROVIDER_VERSION_ANNOTATION,
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

/// Chart workload behind an inventory consumer token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkloadKind {
    Deployment,
    StatefulSet,
    /// One-shot chart work. Its pod template is immutable once started, so a
    /// secret write does not try to roll it.
    Job,
    /// Not a namespaced workload this command can roll (ingress, a sandbox
    /// runner, or a hook that deletes itself).
    Unmanaged,
}

pub fn workload_kind(consumer: &str) -> WorkloadKind {
    match consumer {
        "api" | "worker" | "dispatcher" | "langfuse-web" | "langfuse-worker" | "otel-collector"
        | "mail-adapter" | "ui" => WorkloadKind::Deployment,
        "postgres" | "valkey" | "clickhouse" | "rustfs" => WorkloadKind::StatefulSet,
        "schema-migrate"
        | "rustfs-init"
        | "langfuse-model-pricing"
        | "upgrade-drain"
        | "publication-job" => WorkloadKind::Job,
        _ => WorkloadKind::Unmanaged,
    }
}

pub fn version_annotation(logical: &str) -> String {
    format!("{PROVIDER_VERSION_ANNOTATION}.{logical}")
}

pub fn target_of(entry: &InventoryEntry, release: &str) -> String {
    expand_target(entry, release).target
}

/// Apply one ExternalSecret for every present row that shares the target,
/// then roll only the long-running consumers of those rows.
pub fn publish(
    k: &dyn Kubectl,
    namespace: &str,
    release: &str,
    scoped_prefix: &str,
    group: &[InventoryEntry],
    logical: &str,
    version: &str,
) -> Result<Vec<String>> {
    if group.is_empty() {
        bail!("cannot publish an empty inventory group");
    }
    let expanded: Vec<InventoryEntry> = group
        .iter()
        .map(|entry| expand_target(entry, release))
        .collect();
    let store = store_name(release);
    let (external_name, objects) = if expanded.len() == 1 {
        let sync = SyncEntry::from_inventory(&expanded[0], scoped_prefix)?;
        let mut objects = vec![eso::render_external_secret(
            &sync,
            namespace,
            &store,
            REFRESH_INTERVAL,
        )];
        if let Some(push) = eso::render_push_secret(&sync, namespace, &store) {
            objects.push(push);
        }
        (sync.name, objects)
    } else {
        let object = render_shared(&expanded, namespace, &store, scoped_prefix)?;
        let name = object["metadata"]["name"]
            .as_str()
            .context("shared ExternalSecret has no name")?
            .to_string();
        (name, vec![object])
    };
    apply(k, namespace, &objects)?;
    force_sync_and_wait(k, namespace, &external_name, SYNC_TIMEOUT, SYNC_POLL)?;
    let mut rolled = Vec::new();
    let mut seen = BTreeMap::<String, ()>::new();
    for entry in &expanded {
        for consumer in &entry.consumers {
            if workload_kind(consumer) != WorkloadKind::Deployment
                && workload_kind(consumer) != WorkloadKind::StatefulSet
            {
                continue;
            }
            let name = deployment_name(release, consumer);
            if seen.insert(name.clone(), ()).is_some() {
                continue;
            }
            roll_one(k, namespace, consumer, &name, logical, version)?;
            rolled.push(consumer.clone());
        }
    }
    Ok(rolled)
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

/// Pod-template provider version for one logical object on one consumer.
///
/// `Ok(None)` means the consumer is not a long-running workload, so it is
/// not stale. `Ok(Some(None))` means it is missing, not ready, or unstamped.
pub fn consumer_stamp(
    k: &dyn Kubectl,
    namespace: &str,
    release: &str,
    consumer: &str,
    logical: &str,
) -> Result<Option<Option<String>>> {
    let kind = workload_kind(consumer);
    if !matches!(kind, WorkloadKind::Deployment | WorkloadKind::StatefulSet) {
        return Ok(None);
    }
    let kind_arg = match kind {
        WorkloadKind::Deployment => "deployment",
        WorkloadKind::StatefulSet => "statefulset",
        WorkloadKind::Job | WorkloadKind::Unmanaged => unreachable!("filtered above"),
    };
    let name = deployment_name(release, consumer);
    let output = k.run(
        &argv(&["-n", namespace, "get", kind_arg, &name, "-o", "json"]),
        None,
    )?;
    if not_found(&output.stderr) {
        return Ok(Some(None));
    }
    if !output.success {
        bail!(
            "could not read {kind_arg} {namespace}/{name}: {}",
            one_line(&output.stderr)
        );
    }
    let document: Value = serde_json::from_str(&output.stdout)
        .map_err(|_| anyhow::anyhow!("{kind_arg} {namespace}/{name} returned invalid JSON"))?;
    if !workload_ready(&document) {
        return Ok(Some(None));
    }
    let annotation = version_annotation(logical);
    Ok(Some(
        document["spec"]["template"]["metadata"]["annotations"][&annotation]
            .as_str()
            .map(str::to_string),
    ))
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
                if !matches!(
                    workload_kind(consumer),
                    WorkloadKind::Deployment | WorkloadKind::StatefulSet
                ) {
                    continue;
                }
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

fn roll_one(
    k: &dyn Kubectl,
    namespace: &str,
    consumer: &str,
    name: &str,
    logical: &str,
    version: &str,
) -> Result<()> {
    let kind = match workload_kind(consumer) {
        WorkloadKind::Deployment => "deployment",
        WorkloadKind::StatefulSet => "statefulset",
        WorkloadKind::Job | WorkloadKind::Unmanaged => return Ok(()),
    };
    let annotations = serde_json::json!({
        PROVIDER_VERSION_ANNOTATION: version,
        version_annotation(logical): version,
    });
    let patch = serde_json::json!({
        "spec": { "template": { "metadata": { "annotations": annotations } } }
    })
    .to_string();
    let output = k.run(
        &argv(&[
            "-n",
            namespace,
            "patch",
            kind,
            name,
            "--type=merge",
            "-p",
            &patch,
        ]),
        None,
    )?;
    if !output.success {
        bail!(
            "could not stamp {kind} {namespace}/{name}: {}",
            one_line(&output.stderr)
        );
    }
    let target = format!("{kind}/{name}");
    let timeout = format!("--timeout={}s", ROLLOUT_TIMEOUT.as_secs().max(1));
    let output = k.run(
        &argv(&["-n", namespace, "rollout", "status", &target, &timeout]),
        None,
    )?;
    if !output.success {
        bail!(
            "{kind} {namespace}/{name} did not roll out: {}",
            one_line(&output.stderr)
        );
    }
    Ok(())
}

fn render_shared(
    group: &[InventoryEntry],
    namespace: &str,
    store: &str,
    scoped_prefix: &str,
) -> Result<Value> {
    let target = &group[0].target;
    let mut data = Vec::new();
    for entry in group {
        let sync = SyncEntry::from_inventory(entry, scoped_prefix)?;
        let remote = sync.remote_key.clone();
        for key in sync.static_keys {
            data.push(serde_json::json!({
                "secretKey": key,
                "remoteRef": { "key": remote, "property": key },
            }));
        }
    }
    Ok(serde_json::json!({
        "apiVersion": eso::EXTERNAL_SECRET_API,
        "kind": "ExternalSecret",
        "metadata": { "name": target, "namespace": namespace },
        "spec": {
            "refreshInterval": REFRESH_INTERVAL,
            "secretStoreRef": { "name": store, "kind": "SecretStore" },
            "target": { "name": target, "creationPolicy": "Owner" },
            "data": data,
        },
    }))
}

fn workload_ready(document: &Value) -> bool {
    let desired = document["spec"]["replicas"].as_i64().unwrap_or(1);
    let ready = document["status"]["readyReplicas"].as_i64().unwrap_or(0);
    ready == desired && desired > 0
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
