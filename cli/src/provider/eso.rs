//! External Secrets Operator objects and the kubectl drivers that use them.
//!
//! Renderers are pure and return JSON. Drivers go through [`Kubectl`] so tests
//! can script it. Secret values travel on stdin only and never appear in argv,
//! errors, or logs.

use std::collections::BTreeMap;
use std::io::{Read as _, Write};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use serde_json::{json, Value};

use super::{InventoryEntry, SecretMaterial, Store};

pub const ESO_VERSION: &str = "2.11.0";
pub const EXTERNAL_SECRET_API: &str = "external-secrets.io/v1";
pub const PUSH_SECRET_API: &str = "external-secrets.io/v1alpha1";
pub const FIELD_MANAGER: &str = "curie-secrets";
pub const ROLE_ARN_ANNOTATION: &str = "eks.amazonaws.com/role-arn";
pub const PROVIDER_VERSION_ANNOTATION: &str = "curie.dev/provider-version";
pub const FORCE_SYNC_ANNOTATION: &str = "force-sync";
pub const BACKUP_INTERVAL: &str = "10s";

/// The SecretStore and the IRSA service account it authenticates as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreSpec {
    pub name: String,
    pub namespace: String,
    pub region: String,
    pub service_account: String,
    pub role_arn: String,
}

/// The per-release store `cluster deploy` syncs connector credentials through:
/// SecretStore `<release>-secrets-manager` authenticating as the IRSA service
/// account `<release>-secrets-sync`. One helper so every caller names the same
/// objects.
pub fn release_store_spec(
    release: &str,
    namespace: &str,
    secrets: &crate::installation::SecretsBlock,
) -> StoreSpec {
    StoreSpec {
        name: format!("{release}-secrets-manager"),
        namespace: namespace.to_string(),
        region: secrets.region.clone(),
        service_account: format!("{release}-secrets-sync"),
        role_arn: secrets.role_arn.clone(),
    }
}

/// Service account annotated with the IRSA role.
pub fn render_service_account(spec: &StoreSpec) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {
            "name": spec.service_account,
            "namespace": spec.namespace,
            "annotations": { ROLE_ARN_ANNOTATION: spec.role_arn },
        },
    })
}

/// SecretStore for Secrets Manager. The role comes from the service account.
pub fn render_secret_store(spec: &StoreSpec) -> Value {
    json!({
        "apiVersion": EXTERNAL_SECRET_API,
        "kind": "SecretStore",
        "metadata": { "name": spec.name, "namespace": spec.namespace },
        "spec": {
            "provider": {
                "aws": {
                    "service": "SecretsManager",
                    "region": spec.region,
                    "auth": { "jwt": { "serviceAccountRef": { "name": spec.service_account } } },
                },
            },
        },
    })
}

/// One inventory row resolved to a remote key. Rotated keys are written by
/// the workload and backed up, never pulled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncEntry {
    pub name: String,
    pub target: String,
    pub remote_key: String,
    pub static_keys: Vec<String>,
    pub rotated_keys: Vec<String>,
    /// Labels for the ESO objects' own metadata. Empty renders exactly as before.
    pub labels: BTreeMap<String, String>,
    /// Labels for the Secret ESO creates, through the ExternalSecret's target
    /// template. Empty emits no template.
    pub target_labels: BTreeMap<String, String>,
    /// Render `CreateOrMerge` even without rotated keys, because another
    /// ExternalSecret merges into the same target and none may claim Owner.
    pub merge_into_target: bool,
}

impl SyncEntry {
    /// Attach labels. A connector deploy labels its objects with the owner so
    /// the worker's reconcile and the CLI prune recognise them.
    /// The target Secret gets the same labels unless [`Self::with_target_labels`]
    /// overrides them.
    pub fn with_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.target_labels = labels.clone();
        self.labels = labels;
        self
    }

    /// Labels for the target Secret only.
    pub fn with_target_labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.target_labels = labels;
        self
    }

    /// Merge into the target instead of owning it (see [`Self::merge_into_target`]).
    pub fn merging_into_target(mut self) -> Self {
        self.merge_into_target = true;
        self
    }

    /// Only a provider-held (`store: sm`) entry syncs. Its rotated keys come
    /// from the entry, whose validation already requires them to be drawn
    /// from `keys` under a workload owner.
    pub fn from_inventory(entry: &InventoryEntry, prefix: &str) -> Result<Self> {
        entry
            .validate()
            .with_context(|| format!("inventory entry {}", entry.logical_name))?;
        let name = &entry.logical_name;
        if entry.store != Store::Sm {
            bail!("inventory entry {name} stays in the cluster (store: cluster); ESO does not sync it");
        }
        let rotated_keys = &entry.rotated_keys;
        let static_keys = entry
            .keys
            .iter()
            .filter(|key| !rotated_keys.contains(key))
            .cloned()
            .collect();
        let rotated = entry
            .keys
            .iter()
            .filter(|key| rotated_keys.contains(key))
            .cloned()
            .collect();
        Ok(Self {
            name: name.clone(),
            target: entry.target.clone(),
            remote_key: format!("{}/{}", prefix.trim_end_matches('/'), name),
            static_keys,
            rotated_keys: rotated,
            labels: BTreeMap::new(),
            target_labels: BTreeMap::new(),
            merge_into_target: false,
        })
    }

    /// Remote key the PushSecret backs rotated keys up to.
    pub fn backup_key(&self) -> String {
        format!("{}-rotated", self.remote_key)
    }
}

/// ExternalSecret pulling the static keys. A split entry merges into the
/// target so the workload's rotated keys survive.
pub fn render_external_secret(
    entry: &SyncEntry,
    namespace: &str,
    store: &str,
    refresh_interval: &str,
) -> Value {
    let data: Vec<Value> = entry
        .static_keys
        .iter()
        .map(|key| {
            json!({
                "secretKey": key,
                "remoteRef": { "key": entry.remote_key, "property": key },
            })
        })
        .collect();
    let policy = if entry.rotated_keys.is_empty() && !entry.merge_into_target {
        "Owner"
    } else {
        "CreateOrMerge"
    };
    let mut rendered = json!({
        "apiVersion": EXTERNAL_SECRET_API,
        "kind": "ExternalSecret",
        "metadata": { "name": entry.name, "namespace": namespace },
        "spec": {
            "refreshInterval": refresh_interval,
            "secretStoreRef": { "name": store, "kind": "SecretStore" },
            "target": { "name": entry.target, "creationPolicy": policy },
            "data": data,
        },
    });
    if !entry.labels.is_empty() {
        rendered["metadata"]["labels"] = json!(entry.labels);
    }
    if !entry.target_labels.is_empty() {
        // `Merge` keeps the data keys ESO writes; the template only adds the
        // labels to the Secret it creates or merges into (probed on ESO 2.11.0
        // under both Owner and CreateOrMerge).
        rendered["spec"]["target"]["template"] = json!({
            "engineVersion": "v2",
            "mergePolicy": "Merge",
            "metadata": { "labels": entry.target_labels },
        });
    }
    rendered
}

/// PushSecret backing rotated keys up. `None` when nothing rotates.
pub fn render_push_secret(entry: &SyncEntry, namespace: &str, store: &str) -> Option<Value> {
    if entry.rotated_keys.is_empty() {
        return None;
    }
    let backup = entry.backup_key();
    let data: Vec<Value> = entry
        .rotated_keys
        .iter()
        .map(|key| {
            json!({
                "match": {
                    "secretKey": key,
                    "remoteRef": { "remoteKey": backup, "property": key },
                },
                "metadata": {
                    "apiVersion": "kubernetes.external-secrets.io/v1alpha1",
                    "kind": "PushSecretMetadata",
                    "spec": { "secretPushFormat": "string" },
                },
            })
        })
        .collect();
    let mut rendered = json!({
        "apiVersion": PUSH_SECRET_API,
        "kind": "PushSecret",
        "metadata": { "name": format!("{}-rotated-backup", entry.name), "namespace": namespace },
        "spec": {
            "refreshInterval": BACKUP_INTERVAL,
            "updatePolicy": "Replace",
            "deletionPolicy": "None",
            "secretStoreRefs": [{ "name": store, "kind": "SecretStore" }],
            "selector": { "secret": { "name": entry.target } },
            "data": data,
        },
    });
    if !entry.labels.is_empty() {
        rendered["metadata"]["labels"] = json!(entry.labels);
    }
    Some(rendered)
}

/// Result of one kubectl invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KubectlOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

/// Runs kubectl. `stdin` is the only channel for secret material.
pub trait Kubectl {
    fn run(&self, args: &[String], stdin: Option<&[u8]>) -> Result<KubectlOutput>;
}

/// `kubectl` from PATH, with an optional context and kubeconfig.
#[derive(Debug, Clone, Default)]
pub struct SystemKubectl {
    pub context: Option<String>,
    pub kubeconfig: Option<PathBuf>,
    /// Per-call ceiling. An overdue kubectl child is killed.
    pub call_timeout: Option<Duration>,
}

impl Kubectl for SystemKubectl {
    fn run(&self, args: &[String], stdin: Option<&[u8]>) -> Result<KubectlOutput> {
        let mut command = Command::new("kubectl");
        if let Some(context) = &self.context {
            command.arg("--context").arg(context);
        }
        if let Some(kubeconfig) = &self.kubeconfig {
            command.env("KUBECONFIG", kubeconfig);
        }
        command
            .args(args)
            .stdin(if stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().context("could not start kubectl")?;
        let writer = match (stdin, child.stdin.take()) {
            (Some(bytes), Some(mut pipe)) => {
                let bytes = bytes.to_vec();
                Some(std::thread::spawn(move || pipe.write_all(&bytes)))
            }
            _ => None,
        };
        let mut stdout_pipe = child.stdout.take().expect("stdout was piped");
        let mut stderr_pipe = child.stderr.take().expect("stderr was piped");
        let stdout_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            stdout_pipe.read_to_end(&mut buf).map(|_| buf)
        });
        let stderr_reader = std::thread::spawn(move || {
            let mut buf = Vec::new();
            stderr_pipe.read_to_end(&mut buf).map(|_| buf)
        });
        if let Some(limit) = self.call_timeout {
            let give_up = Instant::now() + limit;
            loop {
                if child
                    .try_wait()
                    .context("kubectl did not finish")?
                    .is_some()
                {
                    break;
                }
                if Instant::now() >= give_up {
                    let _ = child.kill();
                    let _ = child.wait();
                    if let Some(writer) = writer {
                        let _ = writer.join();
                    }
                    let _ = stdout_reader.join();
                    let _ = stderr_reader.join();
                    let verb = args
                        .iter()
                        .find(|a| !a.starts_with('-') && !is_flag_value(args, a))
                        .map(String::as_str)
                        .unwrap_or("command");
                    bail!(
                        "kubectl {verb} did not finish within {}s",
                        limit.as_secs_f64()
                    );
                }
                std::thread::sleep(Duration::from_millis(20));
            }
        }
        let status = child.wait().context("kubectl did not finish")?;
        if let Some(writer) = writer {
            writer
                .join()
                .map_err(|_| anyhow!("kubectl stdin writer panicked"))?
                .context("could not write kubectl stdin")?;
        }
        let stdout = stdout_reader
            .join()
            .map_err(|_| anyhow!("kubectl stdout reader panicked"))?
            .context("could not read kubectl stdout")?;
        let stderr = stderr_reader
            .join()
            .map_err(|_| anyhow!("kubectl stderr reader panicked"))?
            .context("could not read kubectl stderr")?;
        Ok(KubectlOutput {
            success: status.success(),
            stdout: String::from_utf8_lossy(&stdout).into_owned(),
            stderr: String::from_utf8_lossy(&stderr).into_owned(),
        })
    }
}

/// True when `arg` is the value of a preceding `-n`/`--namespace`/`--context`.
fn is_flag_value(args: &[String], arg: &String) -> bool {
    args.iter()
        .position(|a| std::ptr::eq(a, arg))
        .and_then(|i| i.checked_sub(1))
        .and_then(|i| args.get(i))
        .is_some_and(|prev| matches!(prev.as_str(), "-n" | "--namespace" | "--context"))
}

pub(super) fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| part.to_string()).collect()
}

/// Server-side apply of `objects` as one List on stdin.
pub fn apply(k: &dyn Kubectl, namespace: &str, objects: &[Value]) -> Result<()> {
    if objects.is_empty() {
        return Ok(());
    }
    let list = json!({ "apiVersion": "v1", "kind": "List", "items": objects });
    let body = serde_json::to_vec(&list)?;
    let field_manager = format!("--field-manager={FIELD_MANAGER}");
    let args = argv(&[
        "-n",
        namespace,
        "apply",
        "--server-side",
        &field_manager,
        "-f",
        "-",
    ]);
    let out = k.run(&args, Some(&body))?;
    if !out.success {
        bail!(
            "server-side apply in namespace {namespace} failed: {}",
            out.stderr.trim()
        );
    }
    Ok(())
}

static FORCE_SYNC_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_mark() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let count = FORCE_SYNC_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}-{}-{count}", std::process::id())
}

fn read_external(k: &dyn Kubectl, namespace: &str, name: &str) -> Result<Value> {
    let args = argv(&["-n", namespace, "get", "externalsecret", name, "-o", "json"]);
    let out = k.run(&args, None)?;
    if !out.success {
        bail!(
            "could not read ExternalSecret {namespace}/{name}: {}",
            out.stderr.trim()
        );
    }
    serde_json::from_str(&out.stdout)
        .with_context(|| format!("ExternalSecret {namespace}/{name} returned invalid JSON"))
}

fn go_map(map: &Value) -> String {
    let mut pairs: Vec<(String, String)> = map
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| {
                    let v = v.as_str().map_or_else(|| v.to_string(), str::to_string);
                    (k.clone(), v)
                })
                .collect()
        })
        .unwrap_or_default();
    pairs.sort();
    let body: Vec<String> = pairs.into_iter().map(|(k, v)| format!("{k}:{v}")).collect();
    format!("map[{}]", body.join(" "))
}

/// The `status.syncedResourceVersion` ESO would record for `metadata`.
///
/// Pinned to ESO 2.11.0 (`pkg/controllers/util/util.go` and
/// `runtime/esutils/utils.go`): `"{generation}-{hex sha3_224(text)}"`, where
/// `text` is Go's `%+v` of `{Annotations, Labels}`. Re-check on an ESO upgrade.
pub fn synced_version(metadata: &Value) -> String {
    use sha3::{Digest, Sha3_224};
    let generation = metadata["generation"].as_i64().unwrap_or(0);
    let text = format!(
        "{{annotations:{} labels:{}}}",
        go_map(&metadata["annotations"]),
        go_map(&metadata["labels"])
    );
    let digest = Sha3_224::digest(text.as_bytes());
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    format!("{generation}-{hex}")
}

fn is_ready(external: &Value) -> bool {
    external["status"]["conditions"]
        .as_array()
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|c| c["type"] == json!("Ready") && c["status"] == json!("True"))
        })
}

/// Annotate a unique force-sync value and wait until ESO reconciled exactly
/// that metadata: Ready and `syncedResourceVersion` equal to
/// [`synced_version`] of the current metadata, which must still carry our
/// annotation value. A reconcile that started before the annotate hashes
/// older metadata and never matches. The deadline bounds the whole call.
pub fn force_sync_and_wait(
    k: &dyn Kubectl,
    namespace: &str,
    external_secret: &str,
    timeout: Duration,
    poll: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let timed_out = || {
        anyhow!(
            "ExternalSecret {namespace}/{external_secret} did not sync within {}s",
            timeout.as_secs_f64()
        )
    };
    let value = unique_mark();
    let mark = format!("{FORCE_SYNC_ANNOTATION}={value}");
    let args = argv(&[
        "-n",
        namespace,
        "annotate",
        "externalsecret",
        external_secret,
        &mark,
        "--overwrite",
    ]);
    let out = k.run(&args, None)?;
    if !out.success {
        bail!(
            "could not annotate ExternalSecret {namespace}/{external_secret}: {}",
            out.stderr.trim()
        );
    }
    loop {
        if Instant::now() >= deadline {
            return Err(timed_out());
        }
        let current = read_external(k, namespace, external_secret)?;
        if Instant::now() >= deadline {
            return Err(timed_out());
        }
        let metadata = &current["metadata"];
        if metadata["annotations"][FORCE_SYNC_ANNOTATION].as_str() != Some(value.as_str()) {
            bail!(
                "ExternalSecret {namespace}/{external_secret}: another writer replaced the \
                 force-sync annotation"
            );
        }
        let expected = synced_version(metadata);
        let synced = current["status"]["syncedResourceVersion"].as_str();
        if is_ready(&current) && synced == Some(expected.as_str()) {
            return Ok(());
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(timed_out());
        }
        std::thread::sleep(poll.min(deadline - now));
    }
}

/// If the ExternalSecret exists, force it to the current provider value
/// before a consumer rollout. A missing object is success: `curie apply`
/// creates it later, and this command still records the helm reference.
pub fn sync_if_present(namespace: &str, name: &str) -> Result<()> {
    let kubectl = SystemKubectl::default();
    let args = argv(&["-n", namespace, "get", "externalsecret", name, "-o", "name"]);
    let out = kubectl.run(&args, None)?;
    if !out.success {
        let detail = out.stderr.to_ascii_lowercase();
        if detail.contains("notfound") || detail.contains("not found") {
            return Ok(());
        }
        bail!(
            "could not read ExternalSecret {namespace}/{name}: {}",
            out.stderr.trim()
        );
    }
    force_sync_and_wait(
        &kubectl,
        namespace,
        name,
        Duration::from_secs(60),
        Duration::from_secs(1),
    )
}

/// Stamp each Deployment's pod template with `provider_version`, then wait
/// for its rollout.
pub fn rollout_consumers(
    k: &dyn Kubectl,
    namespace: &str,
    deployments: &[String],
    provider_version: &str,
    timeout: Duration,
) -> Result<()> {
    let patch = json!({
        "spec": { "template": { "metadata": { "annotations": {
            PROVIDER_VERSION_ANNOTATION: provider_version,
        } } } },
    })
    .to_string();
    let timeout_flag = format!("--timeout={}s", timeout.as_secs().max(1));
    for deployment in deployments {
        let args = argv(&[
            "-n",
            namespace,
            "patch",
            "deployment",
            deployment,
            "--type=merge",
            "-p",
            &patch,
        ]);
        let out = k.run(&args, None)?;
        if !out.success {
            bail!(
                "could not stamp Deployment {namespace}/{deployment}: {}",
                out.stderr.trim()
            );
        }
        let target = format!("deployment/{deployment}");
        let args = argv(&["-n", namespace, "rollout", "status", &target, &timeout_flag]);
        let out = k.run(&args, None)?;
        if !out.success {
            bail!(
                "Deployment {namespace}/{deployment} did not roll out: {}",
                out.stderr.trim()
            );
        }
    }
    Ok(())
}

/// What `seed_key` did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedOutcome {
    Created,
    Added,
    AlreadyPresent,
}

#[derive(PartialEq)]
enum Refusal {
    NotFound,
    AlreadyExists,
    Conflict,
    Other,
}

fn refusal(stderr: &str) -> Refusal {
    if stderr.contains("(NotFound)") {
        Refusal::NotFound
    } else if stderr.contains("(AlreadyExists)") {
        Refusal::AlreadyExists
    } else if stderr.contains("(Conflict)") {
        Refusal::Conflict
    } else {
        Refusal::Other
    }
}

/// Add `key` to a Secret only when absent. An existing key is never
/// overwritten, including one a concurrent writer adds mid-call. Writes carry
/// the read `resourceVersion`; a conflict re-reads, up to `max_attempts`.
pub fn seed_key(
    k: &dyn Kubectl,
    namespace: &str,
    secret: &str,
    key: &str,
    value: &SecretMaterial,
    max_attempts: u32,
) -> Result<SeedOutcome> {
    let object = format!("Secret {namespace}/{secret}");
    let encoded = base64::engine::general_purpose::STANDARD.encode(value.expose());
    let get = argv(&["-n", namespace, "get", "secret", secret, "-o", "json"]);
    let write_from_stdin = |verb: &str| argv(&["-n", namespace, verb, "-f", "-"]);
    for _ in 0..max_attempts.max(1) {
        let out = k.run(&get, None)?;
        if !out.success {
            if refusal(&out.stderr) != Refusal::NotFound {
                bail!("could not read {object}");
            }
            let body = json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": { "name": secret, "namespace": namespace },
                "type": "Opaque",
                "data": { key: encoded },
            });
            let out = k.run(
                &write_from_stdin("create"),
                Some(&serde_json::to_vec(&body)?),
            )?;
            if out.success {
                return Ok(SeedOutcome::Created);
            }
            match refusal(&out.stderr) {
                Refusal::AlreadyExists | Refusal::Conflict => continue,
                _ => bail!("could not create {object} with key {key}"),
            }
        }
        let mut current: Value = serde_json::from_str(&out.stdout)
            .map_err(|_| anyhow!("{object} returned invalid JSON"))?;
        if current["data"].get(key).is_some() {
            return Ok(SeedOutcome::AlreadyPresent);
        }
        if current["metadata"]["resourceVersion"].as_str().is_none() {
            bail!("{object} has no resourceVersion");
        }
        if !current["data"].is_object() {
            current["data"] = json!({});
        }
        current["data"][key] = json!(encoded);
        let out = k.run(
            &write_from_stdin("replace"),
            Some(&serde_json::to_vec(&current)?),
        )?;
        if out.success {
            return Ok(SeedOutcome::Added);
        }
        match refusal(&out.stderr) {
            Refusal::Conflict | Refusal::NotFound => continue,
            _ => bail!("could not add key {key} to {object}"),
        }
    }
    bail!("could not add key {key} to {object}: it kept changing after {max_attempts} attempts")
}
