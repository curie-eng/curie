//! `curie cluster github-app`: give the platform its own GitHub identity
//! (ADR-0092), so an agent repository needs no deploy workflow and no
//! per-repository credential.
//!
//! The private key is passed with `--set-file`, not `--set`. That is not a
//! style choice: a PEM is multi-line, and more importantly `--set` puts the
//! value in `argv`, where `ps` can read it and a subprocess error can echo it.
//! `--set-file` puts only the *path* there. This is the one credential in the
//! chart that can mint tokens for every repository in the installation, so it
//! is the one that most deserves never being in a process list.
//!
//! `--existing-secret` (#1255) goes one better and hands helm nothing at all:
//! the release records a Secret NAME, the chart resolves it with a
//! `secretKeyRef`, and no path and no PEM ever reach helm -- so the key cannot
//! land in retained release history the way #1236 found it. That path is only
//! reachable when the operator asks for it; `--set-file` above is still what a
//! chart-held connect does.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

use crate::ops::{
    helm_history_cmd, parse_helm_history, plain, require_on_path, resolve_existing_secret_ref,
    run_capture, CommonOpts, OpsCommand,
};

#[derive(Debug, Clone)]
pub struct GithubAppOpts {
    pub common: CommonOpts,
    pub chart: String,
    /// The App's numeric id, from its settings page. Not secret.
    pub app_id: String,
    /// Path to the App's PEM private key. The path, never the contents.
    pub private_key_path: String,
    /// Name of an operator-managed Secret holding the PEM. The chart only
    /// REFERENCES it, so the key never passes through helm values and cannot
    /// land in retained release history. Empty means the chart-held path.
    pub existing_secret: String,
    /// Which data key inside `existing_secret` holds the PEM. Always emitted
    /// alongside `existing_secret` so `--existing-secret X` is deterministic
    /// rather than silently inheriting a stale custom key from a previous run.
    pub existing_secret_key: String,
    /// Clear the App credentials and fall back to `api.githubToken`.
    pub disconnect: bool,
}

/// Where the platform clones from. Set alongside the App because an empty base
/// makes git-flow fail before it ever reaches a credential -- it derives
/// `<base>/<repo>.git`, and with no base that is a path with no scheme, which
/// is rejected as a configuration error. An operator wiring up the App has
/// exactly the wrong context to debug that, so we set both together.
pub const DEFAULT_CLONE_BASE: &str = "https://github.com";

/// Default GitHub REST API for github.com. GHE is `{host}/api/v3`. Overridable
/// by `CURIE_GITHUB_API_URL` (tests) or `GITHUB_API_URL` (same env the API
/// process already reads).
pub const DEFAULT_GITHUB_API_URL: &str = "https://api.github.com";

/// Match `apps/api/src/curie_api/github_app.py`: GitHub rejects a JWT whose
/// `iat` is in its future, so backdate by a minute. `exp` stays inside the
/// documented 10-minute ceiling.
const JWT_BACKDATE_SECONDS: i64 = 60;
const JWT_LIFETIME_SECONDS: i64 = 480;

/// The data key the chart defaults to inside a BYO Secret
/// (`charts/curie/values.yaml`: `api.githubAppExistingSecretKey: privateKey`,
/// and the fallback `charts/curie/templates/api.yaml` renders). Mirrored here
/// so `--existing-secret-key` has a discoverable default in `--help` and in the
/// command manifest. If the two ever drift, `--existing-secret X` with no
/// `--existing-secret-key` writes a key name the chart never defaults to and
/// the api pod fails to start on a Secret that is perfectly correct.
pub const DEFAULT_APP_KEY_DATA_KEY: &str = "privateKey";

pub fn connect_commands(opts: &GithubAppOpts, clone_base: &str) -> Vec<OpsCommand> {
    let mut args = vec![
        plain("upgrade"),
        plain(&opts.common.release),
        plain(&opts.chart),
        plain("-n"),
        plain(&opts.common.namespace),
        plain("--reuse-values"),
        // --set-string, NOT --set. A numeric App ID round-trips through
        // helm's stored values as a float64, and `| quote` then renders it
        // in scientific notation: app id 1234567 reaches the API as
        // "1.234567e+06", the JWT's `iss` claim is wrong, and GitHub answers
        // 401 on every call. Found on a live cluster; a chart-render test
        // cannot see it, because it only appears once a real numeric value
        // has been through a --reuse-values round trip.
        plain("--set-string"),
        // The TRIMMED form, never the raw field. `--app-id ' 1234567 '` is what
        // a paste out of the App's settings page actually produces, and helm
        // stores the surrounding whitespace verbatim: the api pod then signs a
        // JWT whose `iss` claim is " 1234567 ", GitHub answers 401 on every
        // call, and `helm get values` prints something that LOOKS right to the
        // operator reading it back. That is #1236's symptom reached by a
        // different route -- a wrong `iss` -- and no chart-render test can see
        // it. `require_connect_inputs` has already proven the trimmed form is
        // all digits, so this is a normalisation, not a sanitisation.
        plain(format!("api.githubAppId={}", opts.app_id.trim())),
    ];
    if opts.existing_secret.trim().is_empty() {
        // The key's CONTENTS never enter argv; helm reads the file itself.
        args.push(plain("--set-file"));
        args.push(plain(format!(
            "api.githubAppPrivateKey={}",
            opts.private_key_path
        )));
    } else {
        // The BYO path emits no --set-file at all: helm is never told where
        // the PEM lives, so it cannot copy the contents into the release the
        // way #1236 found them sitting in revision 15 of a live install. The
        // release holds a Secret NAME, and the chart resolves it at pod start.
        //
        // --set-string for BOTH entries, never --set. `1234567` is a valid
        // RFC-1123 label and a valid Secret data key; under --set helm parses
        // it as a number, a --reuse-values round trip stores it as a float64,
        // and the next upgrade renders `1.234567e+06` -- the secretKeyRef then
        // names a Secret that does not exist and the api pod never starts.
        // That is #1236's App-ID float bug transplanted into a new field, and
        // a chart-render test cannot see it because it only appears after a
        // real round trip.
        args.push(plain("--set-string"));
        args.push(plain(format!(
            "api.githubAppExistingSecret={}",
            opts.existing_secret
        )));
        args.push(plain("--set-string"));
        args.push(plain(format!(
            "api.githubAppExistingSecretKey={}",
            opts.existing_secret_key
        )));
        // Clear the inline key while adopting the Secret. --reuse-values
        // copies a still-set api.githubAppPrivateKey into every future
        // revision forever -- including the ones `curie cluster up` runs --
        // so leaving it gives the operator the ceremony of the recommended
        // path and none of its benefit. Harmless to the running pod: the
        // chart's BYO branch wins, so it was already reading the Secret.
        args.push(plain("--set"));
        args.push(plain("api.githubAppPrivateKey="));
    }
    args.push(plain("--set"));
    args.push(plain(format!("api.githubCloneBase={clone_base}")));
    vec![OpsCommand::new("helm", args)]
}

pub fn disconnect_commands(opts: &GithubAppOpts) -> Vec<OpsCommand> {
    vec![OpsCommand::new(
        "helm",
        vec![
            plain("upgrade"),
            plain(&opts.common.release),
            plain(&opts.chart),
            plain("-n"),
            plain(&opts.common.namespace),
            plain("--reuse-values"),
            plain("--set"),
            plain("api.githubAppId="),
            plain("--set"),
            plain("api.githubAppPrivateKey="),
            // Only the Secret NAME is cleared. Setting
            // api.githubAppExistingSecretKey="" would NOT restore the chart
            // default (`privateKey`) -- --reuse-values re-supplies the empty
            // string on every later upgrade, so the release overrides the
            // default permanently. An operator who later hand-set
            // githubAppExistingSecret with no key would then render `key: ""`
            // and the api pod would sit in CreateContainerConfigError with
            // nothing in the release to explain why. The field is inert while
            // the name is empty, so leaving it alone is strictly safer.
            plain("--set"),
            plain("api.githubAppExistingSecret="),
        ],
    )]
}

/// Roll the API so the Secret-backed key is actually read. Without this the
/// upgrade succeeds and nothing changes until the next unrelated restart --
/// the operator sees "configured" and pushes still fail to clone.
///
/// Takes a RESOLVED `ReleaseFullname` (#1533): the chart names the Deployment
/// `{{ include "curie.fullname" . }}-api`, which is NOT `{release}-api` unless
/// the release name already contains the chart name. A raw release name cannot
/// reach this builder, which is the point of the newtype.
pub fn rollout_commands(
    namespace: &str,
    fullname: &crate::ops::ReleaseFullname,
) -> Vec<OpsCommand> {
    let target = format!("deployment/{}", fullname.resource("api"));
    vec![
        OpsCommand::new(
            "kubectl",
            vec![
                plain("-n"),
                plain(namespace),
                plain("rollout"),
                plain("restart"),
                plain(&target),
            ],
        ),
        OpsCommand::new(
            "kubectl",
            vec![
                plain("-n"),
                plain(namespace),
                plain("rollout"),
                plain("status"),
                plain(&target),
                plain("--timeout=180s"),
            ],
        ),
    ]
}

/// What the RELEASE's raw `api.githubAppExistingSecret` leaf means to the
/// CHART, judged by Helm's own truthiness rather than by Rust's idea of a
/// string.
///
/// The chart's BYO branch is `{{- if .Values.api.githubAppExistingSecret }}` --
/// plain Go-template truthiness, which sees far more than strings.
/// [`configured_existing_secret`] delegates to `resolve_existing_secret_ref`,
/// which reads the leaf with `.as_str()` and so answers `None` for ANY
/// non-string value. On its own that makes the conflict guard fail OPEN: a
/// release configured by hand as `--set api.githubAppExistingSecret=true`, or
/// with an all-digit Secret name (which helm stores as a float64 -- #1236), is
/// genuinely BYO to the chart while the guard concludes "no BYO configured".
/// The next `--private-key` then writes an ignored PEM, rolls the API and
/// reports success over the OLD key, which is exactly the false-success bug
/// #1255 exists to remove.
///
/// Classified here, locally, rather than by widening `resolve_existing_secret_ref`:
/// that helper is shared with the eight other direct-passthrough credentials
/// (#1759), so changing its contract would silently change behaviour for all of
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ByoSecretField {
    /// Falsy to Helm's `if` -- absent, `null`, `""`, `false`, `0`, or an empty
    /// list/map. The chart really does take the chart-held branch, so a
    /// `--private-key` connect is legitimate here and must NOT be refused:
    /// refusing would brick the documented chart-held rotation.
    ChartHeld,
    /// A non-empty string: the chart takes the BYO branch and the CLI can say
    /// which Secret it resolves.
    Named,
    /// Truthy to Helm, but not a name. The chart WILL take the BYO branch while
    /// the CLI cannot determine which Secret the API is reading. Fail closed.
    Opaque,
}

/// Classify the raw `api.githubAppExistingSecret` leaf by Helm's truthiness.
///
/// A small local walk of the values JSON, deliberately not
/// `resolve_existing_secret_ref` -- see [`ByoSecretField`] for why the shared
/// helper must not learn about non-string values.
pub(crate) fn classify_existing_secret_field(
    existing: Option<&serde_json::Value>,
) -> ByoSecretField {
    let Some(value) = existing
        .and_then(|v| v.get("api"))
        .and_then(|api| api.get("githubAppExistingSecret"))
    else {
        return ByoSecretField::ChartHeld;
    };
    match value {
        serde_json::Value::Null => ByoSecretField::ChartHeld,
        serde_json::Value::Bool(true) => ByoSecretField::Opaque,
        serde_json::Value::Bool(false) => ByoSecretField::ChartHeld,
        serde_json::Value::String(s) if s.is_empty() => ByoSecretField::ChartHeld,
        serde_json::Value::String(_) => ByoSecretField::Named,
        // A Go template treats zero as false. Compared as f64 because a
        // --reuse-values round trip stores every number as one (#1236).
        serde_json::Value::Number(n) if n.as_f64() == Some(0.0) => ByoSecretField::ChartHeld,
        // An EMPTY list or map is falsy to a Go template exactly as `""` is, so
        // it leaves the chart on the chart-held branch; only a populated one
        // reaches the BYO branch we cannot read a name out of.
        serde_json::Value::Array(a) if a.is_empty() => ByoSecretField::ChartHeld,
        serde_json::Value::Object(o) if o.is_empty() => ByoSecretField::ChartHeld,
        _ => ByoSecretField::Opaque,
    }
}

/// The JSON type of the leaf, for a refusal that tells the operator what shape
/// their release is actually in. The type only, never the value.
fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "list",
        serde_json::Value::Object(_) => "map",
    }
}

/// The BYO Secret the RELEASE currently resolves the App private key from, if
/// any: `(secret name, data key)`.
///
/// Pure over the values JSON `helm get values -o json` returns; the async read
/// stays in the caller. Delegates to [`resolve_existing_secret_ref`] rather
/// than re-deriving the precedence, because a CLI read that disagreed with the
/// chart's own BYO-wins rule would report a plausible but wrong answer, which
/// is worse than reporting none (#1759). The three literals are the exact
/// strings `charts/curie/templates/api.yaml` reads.
pub(crate) fn configured_existing_secret(
    existing: Option<&serde_json::Value>,
) -> Option<(String, String)> {
    resolve_existing_secret_ref(
        existing,
        "api.githubAppExistingSecret",
        "api.githubAppExistingSecretKey",
        DEFAULT_APP_KEY_DATA_KEY,
    )
}

/// True when this invocation could silently write a key nothing reads: a
/// chart-held connect (`--private-key`, no `--existing-secret`) that we have
/// not yet checked against the release.
///
/// This predicate scopes only the credential-conflict decision. Every real
/// invocation now reads one exact revision's values to derive the sandbox
/// inventory, including disconnect and explicit BYO paths.
pub(crate) fn needs_byo_conflict_check(opts: &GithubAppOpts) -> bool {
    !opts.disconnect
        && opts.existing_secret.trim().is_empty()
        && !opts.private_key_path.trim().is_empty()
}

/// True when THIS invocation must read the release's values before it acts.
///
/// The chart-held credential-conflict check needs a values read on a real run.
/// The caller also reads values for every real invocation's sandbox inventory;
/// this predicate remains scoped to the older conflict guard so its sibling
/// policy can be tested independently. A `--dry-run` is always offline.
#[cfg(test)]
pub(crate) fn needs_release_read(opts: &GithubAppOpts) -> bool {
    !opts.common.dry_run && needs_byo_conflict_check(opts)
}

/// Refuse rather than report success over an unchanged live key.
///
/// The chart resolves `GITHUB_APP_PRIVATE_KEY` from the BYO Secret whenever
/// `api.githubAppExistingSecret` is non-empty, so `--set-file
/// api.githubAppPrivateKey=...` on such a release writes a value nothing
/// reads, rolls the API, and prints "GitHub App configured" while the pod
/// keeps signing with the OLD key. The README's next rotation step is "delete
/// the first key on GitHub", at which point every clone 401s and nothing the
/// CLI printed ever hinted at it.
///
/// We refuse instead of writing into the Secret: it is operator-managed
/// precisely so External Secrets or Sealed Secrets can own it, and a CLI write
/// there would be reverted on the next reconcile -- a second, subtler
/// misreport.
///
/// Judged in two stages, because "non-empty" is a Rust question and the chart
/// asks a Helm one. [`classify_existing_secret_field`] answers what the CHART
/// will do with the raw leaf, and only a leaf that is a real string goes on to
/// [`configured_existing_secret`] to be named. A leaf that is truthy to Helm
/// but not a string is refused without a name rather than read as "nothing
/// configured", which is the same false success one layer down.
///
/// Called with `existing = None` under `--dry-run`,
/// where it is a no-op: the plan is offline, and the refusal comes on the real
/// invocation before helm is ever run.
pub(crate) fn guard_byo_key_conflict(
    opts: &GithubAppOpts,
    existing: Option<&serde_json::Value>,
) -> Result<()> {
    if !needs_byo_conflict_check(opts) {
        return Ok(());
    }
    match classify_existing_secret_field(existing) {
        // Falsy to the chart's own `{{- if }}`, so the API really is on the
        // chart-held key and this rotation is exactly what it looks like.
        ByoSecretField::ChartHeld => return Ok(()),
        // Truthy to the chart, unreadable to us. Refusing is the only honest
        // answer: guessing "not configured" here is the #1255 bug itself.
        ByoSecretField::Opaque => return Err(opaque_byo_field_error(opts, existing)),
        ByoSecretField::Named => {}
    }
    let Some((name, key)) = configured_existing_secret(existing) else {
        return Ok(());
    };
    // CliError::failure + with_fix rather than bail!, so the --json path emits
    // an actionable `fix` alongside `error` (ADR-0021) instead of an untyped
    // anyhow the agent driving the CLI cannot act on.
    Err(crate::exit::CliError::failure(format!(
        "release {} already reads the GitHub App private key from Secret {name} (key {key}); \
         --private-key would write a value the API never reads and report success over the OLD key",
        opts.common.release
    ))
    .with_fix(format!(
        "update Secret {name} yourself, then re-run with --existing-secret {name} \
         --existing-secret-key {key} to roll the API onto it; or run --disconnect first \
         to go back to the chart-held key"
    ))
    .into())
}

/// The refusal for a release whose `api.githubAppExistingSecret` is truthy to
/// the chart but not a string.
///
/// An error on the centralized error emit, never a new `GithubAppOutput`
/// variant: `cli/schema/github-app.schema.json` is a frozen committed contract,
/// and a refusal is not a success-path value.
fn opaque_byo_field_error(
    opts: &GithubAppOpts,
    existing: Option<&serde_json::Value>,
) -> anyhow::Error {
    let kind = existing
        .and_then(|v| v.get("api"))
        .and_then(|api| api.get("githubAppExistingSecret"))
        .map(json_type_name)
        .unwrap_or("non-string value");
    crate::exit::CliError::failure(format!(
        "release {} stores api.githubAppExistingSecret as a {kind}, not a string. The chart's \
         BYO branch is plain truthiness, so the API IS reading its private key from a Secret -- \
         but the CLI cannot determine WHICH Secret, and it will not guess by writing a \
         --private-key the API may never read",
        opts.common.release
    ))
    .with_fix(
        "re-set the field as a string, e.g. helm upgrade --reuse-values --set-string \
         api.githubAppExistingSecret=<secret-name>, then re-run; or run --disconnect first to go \
         back to the chart-held key",
    )
    .into()
}

const SANDBOX_API_VERSION: &str = "extensions.agents.x-k8s.io/v1beta1";
const SANDBOX_TEMPLATE_KIND: &str = "SandboxTemplate";
const SANDBOX_POOL_KIND: &str = "SandboxWarmPool";
const SANDBOX_RECOVERY_ATTEMPTS: usize = 3;
const KUBECTL_REQUEST_TIMEOUT: &str = "5s";
const KUBECTL_WALL_TIMEOUT: Duration = Duration::from_secs(7);
const SANDBOX_RECONCILE_WALL_TIMEOUT: Duration = Duration::from_secs(30);
const HELM_READ_WALL_TIMEOUT: Duration = Duration::from_secs(15);
const HELM_MUTATION_WALL_TIMEOUT: Duration = Duration::from_secs(200);
const KUBECTL_ROLLOUT_WALL_TIMEOUT: Duration = Duration::from_secs(200);
const HELM_MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";
const HELM_RELEASE_NAME_ANNOTATION: &str = "meta.helm.sh/release-name";
const HELM_RELEASE_NAMESPACE_ANNOTATION: &str = "meta.helm.sh/release-namespace";
const RECOVERY_OPERATION_ANNOTATION: &str = "curietech.ai/github-app-recovery";

#[derive(Clone, Copy)]
struct HelmOwnership<'a> {
    release: &'a str,
    namespace: &'a str,
}

#[derive(Debug)]
struct RecoverySandboxMarker {
    kind: String,
    name: String,
    operation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HelmHistorySnapshot {
    active_revision: u32,
    head_revision: u32,
    head_status: String,
}

impl HelmHistorySnapshot {
    fn is_normal_failed_successor_of(&self, prior: &Self) -> bool {
        self.active_revision == prior.active_revision
            && prior
                .head_revision
                .checked_add(1)
                .is_some_and(|successor| self.head_revision == successor)
            && self.head_status == "failed"
    }
}

#[derive(Debug)]
enum SandboxReconcileFailure {
    RevisionDrift {
        expected: Box<HelmHistorySnapshot>,
        observed: Box<HelmHistorySnapshot>,
        phase: String,
        recovery_object: Option<RecoverySandboxMarker>,
    },
    RevisionIndeterminate {
        expected: Box<HelmHistorySnapshot>,
        phase: String,
        recovery_object: Option<RecoverySandboxMarker>,
    },
    StableRevisionFailure {
        proven_snapshot: Box<HelmHistorySnapshot>,
        error: anyhow::Error,
    },
    Other(anyhow::Error),
}

impl From<anyhow::Error> for SandboxReconcileFailure {
    fn from(error: anyhow::Error) -> Self {
        Self::Other(error)
    }
}

fn helm_mutation_wall_timeout() -> Duration {
    #[cfg(debug_assertions)]
    if let Ok(raw) = std::env::var("CURIE_TEST_GITHUB_APP_HELM_TIMEOUT_MS") {
        if let Ok(milliseconds) = raw.parse::<u64>() {
            if (1..=HELM_MUTATION_WALL_TIMEOUT.as_millis() as u64).contains(&milliseconds) {
                return Duration::from_millis(milliseconds);
            }
        }
    }
    HELM_MUTATION_WALL_TIMEOUT
}

#[derive(Debug, Clone)]
struct ExpectedSandboxInventory {
    deploy: bool,
    agents: Vec<String>,
}

impl ExpectedSandboxInventory {
    fn from_values(values: Option<&serde_json::Value>) -> Result<Self> {
        let sandbox = values.and_then(|value| value.get("agentSandbox"));
        let deploy = match sandbox.and_then(|value| value.get("deploy")) {
            None | Some(serde_json::Value::Null) => true,
            Some(serde_json::Value::Bool(value)) => *value,
            Some(_) => anyhow::bail!("agentSandbox.deploy in Helm values is not a boolean"),
        };
        let mut agents = match sandbox.and_then(|value| value.get("connectorSecrets")) {
            None | Some(serde_json::Value::Null) => Vec::new(),
            Some(serde_json::Value::Object(values)) => values.keys().cloned().collect(),
            Some(_) => anyhow::bail!("agentSandbox.connectorSecrets in Helm values is not a map"),
        };
        agents.sort();
        Ok(Self { deploy, agents })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DesiredSandboxObject {
    kind: String,
    name: String,
    template_ref: Option<String>,
    manifest: serde_json::Value,
}

impl DesiredSandboxObject {
    fn key(&self) -> (String, String) {
        (self.kind.clone(), self.name.clone())
    }
}

#[derive(Debug, Clone, PartialEq)]
struct DesiredSandboxSet {
    objects: Vec<DesiredSandboxObject>,
}

impl DesiredSandboxSet {
    fn parse(manifest: &str) -> Result<Self> {
        let mut objects = Vec::new();
        for document in serde_norway::Deserializer::from_str(manifest) {
            let value = serde_json::Value::deserialize(document)
                .map_err(|_| anyhow::anyhow!("could not parse the deployed Helm manifest"))?;
            let Some(kind) = value.get("kind").and_then(serde_json::Value::as_str) else {
                continue;
            };
            if kind != SANDBOX_TEMPLATE_KIND && kind != SANDBOX_POOL_KIND {
                continue;
            }
            if value.get("apiVersion").and_then(serde_json::Value::as_str)
                != Some(SANDBOX_API_VERSION)
            {
                continue;
            }
            let name = value
                .pointer("/metadata/name")
                .and_then(serde_json::Value::as_str)
                .filter(|name| !name.is_empty())
                .context("a sandbox object in the deployed Helm manifest has no metadata.name")?
                .to_string();
            let template_ref = if kind == SANDBOX_POOL_KIND {
                Some(
                    value
                        .pointer("/spec/sandboxTemplateRef/name")
                        .and_then(serde_json::Value::as_str)
                        .filter(|name| !name.is_empty())
                        .context(
                            "a SandboxWarmPool in the deployed Helm manifest has no sandboxTemplateRef.name",
                        )?
                        .to_string(),
                )
            } else {
                None
            };
            objects.push(DesiredSandboxObject {
                kind: kind.to_string(),
                name,
                template_ref,
                manifest: value,
            });
        }
        objects.sort_by_key(|object| {
            (
                if object.kind == SANDBOX_TEMPLATE_KIND {
                    0
                } else {
                    1
                },
                object.name.clone(),
            )
        });
        let set = Self { objects };
        set.validate_pairs()?;
        Ok(set)
    }

    fn validate_pairs(&self) -> Result<()> {
        let mut keys = BTreeSet::new();
        let templates: BTreeSet<&str> = self
            .objects
            .iter()
            .filter(|object| object.kind == SANDBOX_TEMPLATE_KIND)
            .map(|object| object.name.as_str())
            .collect();
        let mut referenced = BTreeSet::new();
        for object in &self.objects {
            if !keys.insert(object.key()) {
                anyhow::bail!(
                    "the deployed Helm manifest contains duplicate {} {}",
                    object.kind,
                    object.name
                );
            }
            if let Some(template_ref) = object.template_ref.as_deref() {
                if !templates.contains(template_ref) {
                    anyhow::bail!(
                        "SandboxWarmPool {} references missing SandboxTemplate {}",
                        object.name,
                        template_ref
                    );
                }
                referenced.insert(template_ref);
            }
        }
        if let Some(unpaired) = templates.difference(&referenced).next() {
            anyhow::bail!("SandboxTemplate {unpaired} has no SandboxWarmPool pair");
        }
        Ok(())
    }

    fn validate_inventory(&self, expected: &ExpectedSandboxInventory) -> Result<()> {
        if !expected.deploy {
            if self.objects.is_empty() {
                return Ok(());
            }
            anyhow::bail!(
                "agentSandbox.deploy=false but the deployed manifest still owns sandbox objects"
            );
        }
        if self.objects.is_empty() {
            anyhow::bail!(
                "the deployed Helm manifest contains no SandboxTemplate/SandboxWarmPool pairs"
            );
        }

        let templates: BTreeSet<&str> = self
            .objects
            .iter()
            .filter(|object| object.kind == SANDBOX_TEMPLATE_KIND)
            .map(|object| object.name.as_str())
            .collect();
        let pools: BTreeMap<&str, &DesiredSandboxObject> = self
            .objects
            .iter()
            .filter(|object| object.kind == SANDBOX_POOL_KIND)
            .map(|object| (object.name.as_str(), object))
            .collect();
        let generic = templates
            .iter()
            .filter(|name| {
                name.strip_suffix("-runner").is_some_and(|prefix| {
                    pools
                        .get(format!("{prefix}-runner-pool").as_str())
                        .is_some_and(|pool| pool.template_ref.as_deref() == Some(**name))
                })
            })
            .min_by_key(|name| name.len())
            .copied()
            .context("the deployed manifest has no generic SandboxTemplate/SandboxWarmPool pair")?;
        let prefix = generic
            .strip_suffix("-runner")
            .expect("generic candidate has runner suffix");
        for agent in &expected.agents {
            let template = format!("{prefix}-agent-{agent}-runner");
            let pool = format!("{template}-pool");
            if !templates.contains(template.as_str())
                || pools
                    .get(pool.as_str())
                    .is_none_or(|object| object.template_ref.as_deref() != Some(template.as_str()))
            {
                anyhow::bail!(
                    "agent {agent} is missing expected SandboxTemplate/SandboxWarmPool pair {template} / {pool}"
                );
            }
        }
        Ok(())
    }

    fn by_key(&self) -> BTreeMap<(String, String), &serde_json::Value> {
        self.objects
            .iter()
            .map(|object| (object.key(), &object.manifest))
            .collect()
    }

    fn preserves(&self, prior: &Self) -> bool {
        let current = self.by_key();
        prior
            .objects
            .iter()
            .all(|object| current.get(&object.key()) == Some(&&object.manifest))
    }
}

fn helm_manifest_command(opts: &GithubAppOpts, revision: Option<u32>) -> OpsCommand {
    let mut args = vec![
        plain("get"),
        plain("manifest"),
        plain(&opts.common.release),
        plain("-n"),
        plain(&opts.common.namespace),
    ];
    if let Some(revision) = revision {
        args.push(plain("--revision"));
        args.push(plain(revision.to_string()));
    }
    OpsCommand::new("helm", args)
}

fn helm_values_command(opts: &GithubAppOpts, revision: u32) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("get"),
            plain("values"),
            plain(&opts.common.release),
            plain("-n"),
            plain(&opts.common.namespace),
            plain("--revision"),
            plain(revision.to_string()),
            plain("-o"),
            plain("json"),
        ],
    )
}

fn sandbox_list_command(namespace: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain(
                "sandboxtemplates.extensions.agents.x-k8s.io,sandboxwarmpools.extensions.agents.x-k8s.io",
            ),
            plain("-n"),
            plain(namespace),
            plain("-o"),
            plain("json"),
            plain(format!("--request-timeout={KUBECTL_REQUEST_TIMEOUT}")),
        ],
    )
}

fn rollback_command(opts: &GithubAppOpts, revision: u32) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("rollback"),
            plain(&opts.common.release),
            plain(revision.to_string()),
            plain("-n"),
            plain(&opts.common.namespace),
            plain("--wait"),
            plain("--timeout"),
            plain("180s"),
        ],
    )
}

fn rollback_recovery_command(opts: &GithubAppOpts, revision: u32) -> String {
    rollback_command(opts, revision).display()
}

async fn run_helm_bounded(
    command: &OpsCommand,
    timeout: Duration,
    operation: &str,
) -> Result<(bool, String, String)> {
    tokio::time::timeout(timeout, run_capture(command))
        .await
        .map_err(|_| anyhow::anyhow!("{operation} timed out at its wall-clock deadline"))?
}

async fn current_history_snapshot(opts: &GithubAppOpts) -> Result<HelmHistorySnapshot> {
    let command = helm_history_cmd(&opts.common);
    let recovery = command.display();
    let (ok, out, _) = run_helm_bounded(
        &command,
        HELM_READ_WALL_TIMEOUT,
        "reading Helm release history",
    )
    .await?;
    if !ok {
        return Err(crate::exit::CliError::failure(format!(
            "could not capture the deployed Helm revision for release {}",
            opts.common.release
        ))
        .with_fix(format!("inspect it with `{recovery}`"))
        .into());
    }
    let history = parse_helm_history(&out)?;
    let active_revision = history
        .iter()
        .filter(|row| row.status.trim().eq_ignore_ascii_case("deployed"))
        .map(|row| row.revision)
        .max()
        .ok_or_else(|| {
            crate::exit::CliError::failure(format!(
                "release {} has no revision Helm marks deployed",
                opts.common.release
            ))
            .with_fix(format!("inspect it with `{recovery}`"))
        })?;
    let head = history
        .iter()
        .max_by_key(|row| row.revision)
        .ok_or_else(|| {
            crate::exit::CliError::failure(format!(
                "release {} has no revisions in Helm history",
                opts.common.release
            ))
            .with_fix(format!("inspect it with `{recovery}`"))
        })?;
    Ok(HelmHistorySnapshot {
        active_revision,
        head_revision: head.revision,
        head_status: head.status.trim().to_ascii_lowercase(),
    })
}

async fn read_desired_sandboxes(
    opts: &GithubAppOpts,
    revision: Option<u32>,
) -> Result<DesiredSandboxSet> {
    let command = helm_manifest_command(opts, revision);
    let (ok, out, _) = run_helm_bounded(
        &command,
        HELM_READ_WALL_TIMEOUT,
        "reading the Helm release manifest",
    )
    .await?;
    if !ok {
        anyhow::bail!("helm could not read the deployed manifest")
    }
    DesiredSandboxSet::parse(&out)
}

async fn read_release_values_at_revision(
    opts: &GithubAppOpts,
    revision: u32,
) -> Result<Option<serde_json::Value>> {
    let command = helm_values_command(opts, revision);
    let (ok, out, _) = run_helm_bounded(
        &command,
        HELM_READ_WALL_TIMEOUT,
        "reading the Helm release values",
    )
    .await?;
    if !ok {
        anyhow::bail!("helm could not read the captured revision's values")
    }
    let value: serde_json::Value = serde_json::from_str(&out)
        .map_err(|_| anyhow::anyhow!("helm returned malformed values JSON"))?;
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Object(_) => Ok(Some(value)),
        _ => anyhow::bail!("helm returned values JSON that is neither an object nor null"),
    }
}

async fn live_sandboxes(namespace: &str) -> Result<BTreeMap<String, serde_json::Value>> {
    let command = sandbox_list_command(namespace);
    let (ok, out, err) = tokio::time::timeout(KUBECTL_WALL_TIMEOUT, run_capture(&command))
        .await
        .map_err(|_| {
            anyhow::anyhow!("kubectl sandbox read timed out at its wall-clock deadline")
        })??;
    if !ok {
        if err.to_ascii_lowercase().contains("deadline")
            || err.to_ascii_lowercase().contains("timed out")
        {
            anyhow::bail!("kubectl sandbox read timed out at its request deadline")
        }
        anyhow::bail!("kubectl could not list SandboxTemplate and SandboxWarmPool objects")
    }
    let value: serde_json::Value =
        serde_json::from_str(&out).context("kubectl returned malformed sandbox JSON")?;
    let items = value
        .get("items")
        .and_then(serde_json::Value::as_array)
        .context("kubectl sandbox JSON has no items array")?;
    let mut live = BTreeMap::new();
    for item in items {
        let Some(kind) = item.get("kind").and_then(serde_json::Value::as_str) else {
            continue;
        };
        if kind != SANDBOX_TEMPLATE_KIND && kind != SANDBOX_POOL_KIND {
            continue;
        }
        let name = item
            .pointer("/metadata/name")
            .and_then(serde_json::Value::as_str)
            .filter(|name| !name.is_empty())
            .context("a live sandbox object has no metadata.name")?;
        if live.insert(name.to_string(), item.clone()).is_some() {
            anyhow::bail!("multiple live sandbox objects share the name {name}");
        }
    }
    Ok(live)
}

fn value_is_required_subset(desired: &serde_json::Value, live: &serde_json::Value) -> bool {
    match (desired, live) {
        (serde_json::Value::Object(desired), serde_json::Value::Object(live)) => {
            desired.iter().all(|(key, value)| {
                live.get(key)
                    .is_some_and(|item| value_is_required_subset(value, item))
            })
        }
        (serde_json::Value::Array(desired), serde_json::Value::Array(live)) => {
            desired.len() == live.len()
                && desired
                    .iter()
                    .zip(live)
                    .all(|(desired, live)| value_is_required_subset(desired, live))
        }
        _ => desired == live,
    }
}

fn validate_live_object(
    desired: &DesiredSandboxObject,
    live: &serde_json::Value,
    ownership: HelmOwnership<'_>,
) -> Result<()> {
    if live.get("apiVersion") != desired.manifest.get("apiVersion")
        || live.get("kind") != desired.manifest.get("kind")
        || live.pointer("/metadata/name") != desired.manifest.pointer("/metadata/name")
    {
        anyhow::bail!("apiVersion, kind, or metadata.name diverges");
    }
    if live
        .pointer("/metadata/labels/app.kubernetes.io~1managed-by")
        .and_then(serde_json::Value::as_str)
        != Some("Helm")
        || live
            .pointer("/metadata/annotations/meta.helm.sh~1release-name")
            .and_then(serde_json::Value::as_str)
            != Some(ownership.release)
        || live
            .pointer("/metadata/annotations/meta.helm.sh~1release-namespace")
            .and_then(serde_json::Value::as_str)
            != Some(ownership.namespace)
    {
        anyhow::bail!("Helm ownership metadata is missing or does not match this release");
    }
    let empty = serde_json::Value::Object(serde_json::Map::new());
    let desired_labels = desired
        .manifest
        .pointer("/metadata/labels")
        .unwrap_or(&empty);
    let live_labels = live.pointer("/metadata/labels").unwrap_or(&empty);
    let desired_spec = desired
        .manifest
        .get("spec")
        .unwrap_or(&serde_json::Value::Null);
    let live_spec = live.get("spec").unwrap_or(&serde_json::Value::Null);
    if !value_is_required_subset(desired_labels, live_labels) {
        anyhow::bail!("Helm-desired labels diverge");
    }
    if !value_is_required_subset(desired_spec, live_spec) {
        anyhow::bail!("Helm-desired spec diverges");
    }
    Ok(())
}

fn missing_from_live<'a>(
    desired: &'a DesiredSandboxSet,
    live: &BTreeMap<String, serde_json::Value>,
    ownership: HelmOwnership<'_>,
) -> Result<Vec<&'a DesiredSandboxObject>> {
    let mut missing = Vec::new();
    for object in &desired.objects {
        match live.get(&object.name) {
            Some(value) => {
                if let Err(error) = validate_live_object(object, value, ownership) {
                    anyhow::bail!(
                        "live sandbox object {} diverges from its Helm-desired state: {error}",
                        object.name
                    );
                }
            }
            None => missing.push(object),
        }
    }
    Ok(missing)
}

async fn create_sandbox(
    ownership: HelmOwnership<'_>,
    object: &DesiredSandboxObject,
    recovery_operation: &str,
) -> Result<()> {
    tokio::time::timeout(
        KUBECTL_WALL_TIMEOUT,
        create_sandbox_inner(ownership, object, recovery_operation),
    )
    .await
    .map_err(|_| anyhow::anyhow!("kubectl sandbox create timed out at its wall-clock deadline"))?
}

async fn create_sandbox_inner(
    ownership: HelmOwnership<'_>,
    object: &DesiredSandboxObject,
    recovery_operation: &str,
) -> Result<()> {
    let mut recovery_manifest = object.manifest.clone();
    let metadata = recovery_manifest
        .get_mut("metadata")
        .and_then(serde_json::Value::as_object_mut)
        .context("a sandbox recovery object has no metadata map")?;
    let labels = metadata
        .entry("labels")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .context("a sandbox recovery object's metadata.labels is not a map")?;
    labels.insert(
        HELM_MANAGED_BY_LABEL.to_string(),
        serde_json::Value::String("Helm".to_string()),
    );
    let annotations = metadata
        .entry("annotations")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .context("a sandbox recovery object's metadata.annotations is not a map")?;
    annotations.insert(
        HELM_RELEASE_NAME_ANNOTATION.to_string(),
        serde_json::Value::String(ownership.release.to_string()),
    );
    annotations.insert(
        HELM_RELEASE_NAMESPACE_ANNOTATION.to_string(),
        serde_json::Value::String(ownership.namespace.to_string()),
    );
    annotations.insert(
        RECOVERY_OPERATION_ANNOTATION.to_string(),
        serde_json::Value::String(recovery_operation.to_string()),
    );
    let body = serde_norway::to_string(&recovery_manifest)
        .context("could not serialize a sandbox recovery object")?;
    let mut child = tokio::process::Command::new("kubectl")
        .args([
            "create",
            "-n",
            ownership.namespace,
            "-f",
            "-",
            &format!("--request-timeout={KUBECTL_REQUEST_TIMEOUT}"),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("failed to invoke `kubectl`; is it on PATH?")?;
    let mut stdin = child
        .stdin
        .take()
        .context("could not open kubectl stdin for sandbox recovery")?;
    stdin
        .write_all(body.as_bytes())
        .await
        .context("could not send a sandbox recovery object to kubectl")?;
    drop(stdin);
    let output = child
        .wait_with_output()
        .await
        .context("could not wait for kubectl sandbox recovery")?;
    if !output.status.success() {
        anyhow::bail!("kubectl could not recreate {} {}", object.kind, object.name);
    }
    Ok(())
}

async fn ensure_reconcile_revision(
    opts: &GithubAppOpts,
    expected: &HelmHistorySnapshot,
    phase: &'static str,
) -> std::result::Result<(), SandboxReconcileFailure> {
    let observed = match current_history_snapshot(opts).await {
        Ok(observed) => observed,
        Err(_) => {
            return Err(SandboxReconcileFailure::RevisionIndeterminate {
                expected: Box::new(expected.clone()),
                phase: phase.to_string(),
                recovery_object: None,
            });
        }
    };
    if &observed == expected {
        Ok(())
    } else {
        Err(SandboxReconcileFailure::RevisionDrift {
            expected: Box::new(expected.clone()),
            observed: Box::new(observed),
            phase: phase.to_string(),
            recovery_object: None,
        })
    }
}

async fn reconcile_live_sandboxes(
    opts: &GithubAppOpts,
    ownership: HelmOwnership<'_>,
    expected_snapshot: &HelmHistorySnapshot,
    desired: &DesiredSandboxSet,
) -> std::result::Result<(), SandboxReconcileFailure> {
    tokio::time::timeout(
        SANDBOX_RECONCILE_WALL_TIMEOUT,
        reconcile_live_sandboxes_inner(opts, ownership, expected_snapshot, desired),
    )
    .await
    .map_err(|_| {
        SandboxReconcileFailure::Other(anyhow::anyhow!(
            "sandbox reconciliation timed out at its overall deadline"
        ))
    })?
}

async fn reconcile_live_sandboxes_inner(
    opts: &GithubAppOpts,
    ownership: HelmOwnership<'_>,
    expected_snapshot: &HelmHistorySnapshot,
    desired: &DesiredSandboxSet,
) -> std::result::Result<(), SandboxReconcileFailure> {
    for _ in 0..SANDBOX_RECOVERY_ATTEMPTS {
        let live = live_sandboxes(ownership.namespace).await?;
        let missing = missing_from_live(desired, &live, ownership)?;
        if missing.is_empty() {
            return ensure_reconcile_revision(
                opts,
                expected_snapshot,
                "after the sandbox reconciliation barrier",
            )
            .await;
        }
        ensure_reconcile_revision(opts, expected_snapshot, "before a sandbox recovery attempt")
            .await?;
        for object in missing {
            ensure_reconcile_revision(opts, expected_snapshot, "before a sandbox recovery write")
                .await?;
            let recovery_operation = uuid::Uuid::new_v4().to_string();
            let create = create_sandbox(ownership, object, &recovery_operation).await;
            let recovery_object = || RecoverySandboxMarker {
                kind: object.kind.clone(),
                name: object.name.clone(),
                operation: recovery_operation.clone(),
            };
            let observed = match current_history_snapshot(opts).await {
                Ok(observed) => observed,
                Err(_) => {
                    return Err(SandboxReconcileFailure::RevisionIndeterminate {
                        expected: Box::new(expected_snapshot.clone()),
                        phase: "after a sandbox recovery write".to_string(),
                        recovery_object: Some(recovery_object()),
                    });
                }
            };
            if &observed != expected_snapshot {
                return Err(SandboxReconcileFailure::RevisionDrift {
                    expected: Box::new(expected_snapshot.clone()),
                    observed: Box::new(observed),
                    phase: "during a sandbox recovery write".to_string(),
                    recovery_object: Some(recovery_object()),
                });
            }
            if create.is_err() {
                // A concurrent controller or operator may have won the create
                // race. Relist immediately and accept that outcome only when
                // the object now contains every Helm-desired field.
                let live = live_sandboxes(ownership.namespace).await?;
                let still_missing = missing_from_live(desired, &live, ownership)?;
                if !still_missing
                    .iter()
                    .any(|candidate| candidate.name == object.name)
                {
                    continue;
                }
            }
        }
    }
    let live = live_sandboxes(ownership.namespace).await?;
    let missing = missing_from_live(desired, &live, ownership)?
        .into_iter()
        .map(|object| object.name.as_str())
        .collect::<Vec<_>>();
    ensure_reconcile_revision(
        opts,
        expected_snapshot,
        "after the sandbox reconciliation barrier",
    )
    .await?;
    if missing.is_empty() {
        Ok(())
    } else {
        Err(SandboxReconcileFailure::StableRevisionFailure {
            proven_snapshot: Box::new(expected_snapshot.clone()),
            error: anyhow::anyhow!("unreconciled sandbox object(s): {}", missing.join(", ")),
        })
    }
}

fn recovery_failure(
    opts: &GithubAppOpts,
    revision: u32,
    message: impl Into<String>,
) -> anyhow::Error {
    crate::exit::CliError::failure(message.into())
        .with_fix(format!(
            "run `{}` and inspect every SandboxTemplate/SandboxWarmPool pair before retrying",
            rollback_recovery_command(opts, revision)
        ))
        .into()
}

fn manifest_preflight_failure(
    opts: &GithubAppOpts,
    revision: u32,
    error: &anyhow::Error,
) -> anyhow::Error {
    let command = helm_manifest_command(opts, Some(revision)).display();
    crate::exit::CliError::failure(format!(
        "release {} revision {} has no complete, usable SandboxTemplate/SandboxWarmPool set: {error}",
        opts.common.release, revision
    ))
    .with_fix(format!(
        "inspect it with `{command}` and restore every complete template/pool pair before retrying"
    ))
    .into()
}

fn revision_stability_failure(
    opts: &GithubAppOpts,
    captured: &HelmHistorySnapshot,
    observed: &HelmHistorySnapshot,
    phase: &str,
) -> anyhow::Error {
    revision_stability_failure_with_recovery(opts, captured, observed, phase, None)
}

fn snapshot_description(snapshot: &HelmHistorySnapshot) -> String {
    format!(
        "active revision {}, history head revision {} ({})",
        snapshot.active_revision, snapshot.head_revision, snapshot.head_status
    )
}

fn pending_head_failure(opts: &GithubAppOpts, snapshot: &HelmHistorySnapshot) -> anyhow::Error {
    let command = helm_history_cmd(&opts.common).display();
    crate::exit::CliError::failure(format!(
        "release {} has Helm history head revision {} in {}; refusing all mutation while that operation is pending",
        opts.common.release, snapshot.head_revision, snapshot.head_status
    ))
    .with_fix(format!(
        "inspect current release history with `{command}` and resolve the pending Helm operation before retrying; no automatic rollback was attempted"
    ))
    .into()
}

fn sandbox_inspection_command(
    opts: &GithubAppOpts,
    recovery_object: &RecoverySandboxMarker,
) -> OpsCommand {
    let resource = if recovery_object.kind == SANDBOX_TEMPLATE_KIND {
        "sandboxtemplates.extensions.agents.x-k8s.io"
    } else {
        "sandboxwarmpools.extensions.agents.x-k8s.io"
    };
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain(format!("{resource}/{}", recovery_object.name)),
            plain("-n"),
            plain(&opts.common.namespace),
            plain("-o"),
            plain("yaml"),
            plain(format!("--request-timeout={KUBECTL_REQUEST_TIMEOUT}")),
        ],
    )
}

fn revision_stability_failure_with_recovery(
    opts: &GithubAppOpts,
    captured: &HelmHistorySnapshot,
    observed: &HelmHistorySnapshot,
    phase: &str,
    recovery_object: Option<&RecoverySandboxMarker>,
) -> anyhow::Error {
    let history_command = helm_history_cmd(&opts.common).display();
    let recovery_detail = recovery_object.map_or_else(String::new, |object| {
        format!(
            "; the create attempt for {} {} used recovery marker {}={} and was left untouched for operator inspection",
            object.kind, object.name, RECOVERY_OPERATION_ANNOTATION, object.operation
        )
    });
    let fix = recovery_object.map_or_else(
        || {
            format!(
                "inspect concurrent release history with `{history_command}` and reconcile the active revision; no automatic rollback was attempted"
            )
        },
        |object| {
            let inspect_command = sandbox_inspection_command(opts, object).display();
            format!(
                "inspect the operation-marked object with `{inspect_command}` and verify {}={}; inspect concurrent release history with `{history_command}`, then reconcile only against the active revision; no automatic deletion or rollback was attempted",
                RECOVERY_OPERATION_ANNOTATION, object.operation
            )
        },
    );
    crate::exit::CliError::failure(
        format!(
            "release {} moved from captured Helm history snapshot ({}) to ({}) {phase}; refusing stale state",
            opts.common.release,
            snapshot_description(captured),
            snapshot_description(observed)
        ) + &recovery_detail,
    )
    .with_fix(fix)
    .into()
}

fn revision_indeterminate_failure_with_recovery(
    opts: &GithubAppOpts,
    expected: &HelmHistorySnapshot,
    phase: &str,
    recovery_object: Option<&RecoverySandboxMarker>,
) -> anyhow::Error {
    let history_command = helm_history_cmd(&opts.common).display();
    let recovery_detail = recovery_object.map_or_else(String::new, |object| {
        format!(
            "; the create attempt for {} {} used recovery marker {}={} and was left untouched for operator inspection",
            object.kind, object.name, RECOVERY_OPERATION_ANNOTATION, object.operation
        )
    });
    let fix = recovery_object.map_or_else(
        || {
            format!(
                "inspect current release history with `{history_command}` before reconciling the expected snapshot ({}); no automatic rollback was attempted",
                snapshot_description(expected)
            )
        },
        |object| {
            let inspect_command = sandbox_inspection_command(opts, object).display();
            format!(
                "inspect the operation-marked object with `{inspect_command}` and verify {}={}; inspect current release history with `{history_command}`, then reconcile only against the active revision; no automatic deletion or rollback was attempted",
                RECOVERY_OPERATION_ANNOTATION, object.operation
            )
        },
    );
    crate::exit::CliError::failure(
        format!(
            "could not establish whether release {} still has expected Helm history snapshot ({}) {phase}; refusing stale state",
            opts.common.release,
            snapshot_description(expected)
        ) + &recovery_detail,
    )
    .with_fix(fix)
    .into()
}

fn map_reconcile_failure(
    opts: &GithubAppOpts,
    recovery_revision: u32,
    expected_snapshot: &HelmHistorySnapshot,
    context: impl FnOnce(anyhow::Error) -> String,
    failure: SandboxReconcileFailure,
) -> anyhow::Error {
    match failure {
        SandboxReconcileFailure::RevisionDrift {
            expected,
            observed,
            phase,
            recovery_object,
        } => revision_stability_failure_with_recovery(
            opts,
            &expected,
            &observed,
            &phase,
            recovery_object.as_ref(),
        ),
        SandboxReconcileFailure::RevisionIndeterminate {
            expected,
            phase,
            recovery_object,
        } => revision_indeterminate_failure_with_recovery(
            opts,
            &expected,
            &phase,
            recovery_object.as_ref(),
        ),
        SandboxReconcileFailure::StableRevisionFailure {
            proven_snapshot,
            error,
        } => recovery_failure(
            opts,
            recovery_revision,
            format!(
                "{}; Helm history snapshot ({}) was rechecked immediately before this failure",
                context(error),
                snapshot_description(&proven_snapshot)
            ),
        ),
        SandboxReconcileFailure::Other(error) => revision_indeterminate_failure_with_recovery(
            opts,
            expected_snapshot,
            &format!("while reconciling sandbox state: {}", context(error)),
            None,
        ),
    }
}

async fn restore_captured_sandboxes(
    opts: &GithubAppOpts,
    ownership: HelmOwnership<'_>,
    expected_snapshot: &HelmHistorySnapshot,
    prior: &DesiredSandboxSet,
) -> std::result::Result<(), SandboxReconcileFailure> {
    reconcile_live_sandboxes(opts, ownership, expected_snapshot, prior).await
}

pub enum GithubAppOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { configured: bool },
}

impl crate::ui::CliOutput for GithubAppOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            GithubAppOutput::DryRun(plan) => plan.to_json(),
            GithubAppOutput::Done { configured } => {
                serde_json::json!({"github_app_configured": configured})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if let GithubAppOutput::DryRun(plan) = self {
            plan.render(ui);
        }
    }
}

/// The GitHub REST API this probe should call.
///
/// Precedence: `CURIE_GITHUB_API_URL` (tests and an explicit CLI override),
/// then `GITHUB_API_URL` (the same env the API already reads), then github.com
/// vs GHE derived from `--clone-base`. No new clap flag: a command-surface
/// change would force a manifest regen this ticket does not need.
pub(crate) fn github_api_url(clone_base: &str) -> String {
    for var in ["CURIE_GITHUB_API_URL", "GITHUB_API_URL"] {
        if let Ok(url) = std::env::var(var) {
            let trimmed = url.trim().trim_end_matches('/');
            if !trimmed.is_empty() {
                return trimmed.to_string();
            }
        }
    }
    let base = clone_base.trim().trim_end_matches('/');
    if base.is_empty() || base == DEFAULT_CLONE_BASE || base == "http://github.com" {
        return DEFAULT_GITHUB_API_URL.to_string();
    }
    format!("{base}/api/v3")
}

#[derive(Serialize)]
struct GitHubAppJwtClaims {
    iat: i64,
    exp: i64,
    iss: String,
}

/// Sign a GitHub App JWT the same way the API does (`_app_jwt` in
/// `apps/api/src/curie_api/github_app.py`): RS256, `iss` = App id, `iat`
/// backdated 60s, `exp` 480s. The two cannot share code across Python/Rust;
/// the constants and claim names are the sibling.
fn sign_app_jwt(app_id: &str, pem: &str) -> Result<String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| {
            crate::exit::CliError::failure(format!("system clock is before Unix epoch: {err}"))
        })?
        .as_secs() as i64;
    let claims = GitHubAppJwtClaims {
        iat: now - JWT_BACKDATE_SECONDS,
        exp: now + JWT_LIFETIME_SECONDS,
        iss: app_id.to_string(),
    };
    let key = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).map_err(|_| {
        crate::exit::CliError::failure(
            "could not sign a GitHub App JWT from the supplied private key; it is PEM-shaped \
             but is not a usable RSA key",
        )
        .with_fix(
            "re-download the App's private key (its settings page, under 'Private keys') and \
             rerun; the last known-good credential was left unchanged",
        )
    })?;
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &key,
    )
    .map_err(|_| {
        crate::exit::CliError::failure(
            "could not sign a GitHub App JWT from the supplied private key",
        )
        .with_fix(
            "re-download the App's private key (its settings page, under 'Private keys') \
                 and rerun; the last known-good credential was left unchanged",
        )
        .into()
    })
}

fn pem_from_secret_json(body: &str, secret: &str, key: &str) -> Result<String> {
    let value: serde_json::Value = serde_json::from_str(body).map_err(|_| {
        crate::exit::CliError::failure(format!(
            "kubectl get secret {secret} did not return JSON; cannot read the GitHub App private key"
        ))
        .with_fix(format!(
            "inspect the Secret with kubectl -n <namespace> get secret {secret} -o json and \
             rerun; the last known-good credential was left unchanged"
        ))
    })?;
    if let Some(encoded) = value
        .get("data")
        .and_then(|d| d.get(key))
        .and_then(|v| v.as_str())
    {
        let bytes =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded.trim())
                .map_err(|_| {
                    crate::exit::CliError::failure(format!(
                "Secret {secret} key {key} is not standard base64; cannot read the GitHub App \
                 private key"
            ))
            .with_fix(
                "store the PEM as a normal Secret data value (kubectl create secret generic \
                 --from-file) and rerun; the last known-good credential was left unchanged",
            )
                })?;
        return String::from_utf8(bytes).map_err(|_| {
            crate::exit::CliError::failure(format!(
                "Secret {secret} key {key} is not UTF-8; a PEM private key is ASCII"
            ))
            .with_fix(
                "store the PEM as UTF-8 text in that Secret data key and rerun; the last \
                 known-good credential was left unchanged",
            )
            .into()
        });
    }
    if let Some(plain_pem) = value
        .get("stringData")
        .and_then(|d| d.get(key))
        .and_then(|v| v.as_str())
    {
        return Ok(plain_pem.to_string());
    }
    Err(crate::exit::CliError::failure(format!(
        "Secret {secret} has no data key {key}; cannot probe GitHub App identity"
    ))
    .with_fix(format!(
        "put the App PEM in Secret {secret} under key {key} and rerun; the last known-good \
         credential was left unchanged"
    ))
    .into())
}

async fn load_existing_secret_pem(opts: &GithubAppOpts) -> Result<String> {
    let args = crate::connectors::get_secret_args(&opts.common.namespace, &opts.existing_secret);
    let cmd = OpsCommand::new(
        "kubectl",
        args.iter().skip(1).map(|a| plain(a.clone())).collect(),
    );
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        return Err(crate::exit::CliError::failure(format!(
            "could not read Secret {} in namespace {}: {err}",
            opts.existing_secret, opts.common.namespace
        ))
        .with_fix(format!(
            "create Secret {} (key {}) in namespace {} and rerun; the last known-good \
             credential was left unchanged",
            opts.existing_secret, opts.existing_secret_key, opts.common.namespace
        ))
        .into());
    }
    pem_from_secret_json(&out, &opts.existing_secret, &opts.existing_secret_key)
}

async fn load_connect_pem(opts: &GithubAppOpts) -> Result<String> {
    if !opts.existing_secret.trim().is_empty() {
        return load_existing_secret_pem(opts).await;
    }
    std::fs::read_to_string(&opts.private_key_path).map_err(|err| {
        crate::exit::CliError::failure(format!(
            "--private-key: cannot re-read {} for the GitHub identity probe: {err}",
            opts.private_key_path
        ))
        .with_fix(
            "rerun with --private-key pointing at a readable PEM file; the last known-good \
             credential was left unchanged",
        )
        .into()
    })
}

fn github_id_matches(body: &serde_json::Value, app_id: &str) -> bool {
    match body.get("id") {
        Some(serde_json::Value::Number(n)) => {
            n.to_string() == app_id
                || n.as_u64().is_some_and(|u| u.to_string() == app_id)
                || n.as_i64().is_some_and(|i| i.to_string() == app_id)
        }
        Some(serde_json::Value::String(s)) => s == app_id,
        _ => false,
    }
}

fn reported_github_id(body: &serde_json::Value) -> String {
    match body.get("id") {
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => "<missing>".into(),
    }
}

/// Sign a JWT and `GET /app` before any helm mutation. A 401 or an App id
/// that does not match `--app-id` is a configuration error (exit 1) with a
/// fix; the last known-good credential is preserved because helm is never
/// run. Network failures stay transient (exit 3) via reqwest classification.
///
/// Skipped on `--dry-run` (offline) and `--disconnect` by the caller.
pub(crate) async fn guard_app_identity(opts: &GithubAppOpts, clone_base: &str) -> Result<()> {
    let app_id = opts.app_id.trim();
    let pem = load_connect_pem(opts).await?;
    if !is_pem_private_key(&pem) {
        return Err(crate::exit::CliError::failure(
            "the GitHub App private key is not a PEM private key; refusing to change credentials",
        )
        .with_fix(
            "put a PEM downloaded from the App's settings page under 'Private keys' and rerun; \
             the last known-good credential was left unchanged",
        )
        .into());
    }
    let jwt = sign_app_jwt(app_id, &pem)?;
    let api = github_api_url(clone_base);
    let url = format!("{}/app", api.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|err| {
            crate::exit::CliError::failure(format!("could not build an HTTP client: {err}"))
        })?;
    let response = client
        .get(&url)
        .header("Authorization", format!("Bearer {jwt}"))
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .header("User-Agent", format!("curie/{}", env!("CARGO_PKG_VERSION")))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return Err(crate::exit::CliError::failure(format!(
            "the GitHub App private key does not authenticate as App {app_id}. GitHub \
             returned HTTP {} for GET /app",
            status.as_u16()
        ))
        .with_fix(format!(
            "download the private key that belongs to App {app_id} (Settings -> Developer \
             settings -> GitHub Apps -> your app -> Private keys) and rerun; the last \
             known-good credential was left unchanged"
        ))
        .into());
    }
    if status.is_server_error() {
        return Err(crate::exit::CliError::transient(format!(
            "GitHub returned HTTP {} probing GET /app; the last known-good credential was \
             left unchanged",
            status.as_u16()
        ))
        .into());
    }
    if !status.is_success() {
        return Err(crate::exit::CliError::failure(format!(
            "GitHub returned HTTP {} probing GET /app for App {app_id}",
            status.as_u16()
        ))
        .with_fix(
            "check the App id and private key, then rerun; the last known-good credential \
             was left unchanged",
        )
        .into());
    }
    let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    if !github_id_matches(&parsed, app_id) {
        let got = reported_github_id(&parsed);
        return Err(crate::exit::CliError::failure(format!(
            "GitHub authenticated the key as App {got}, not the requested --app-id {app_id}"
        ))
        .with_fix(format!(
            "pass --app-id {got} for this key, or supply the private key that belongs to \
             App {app_id}; the last known-good credential was left unchanged"
        ))
        .into());
    }
    Ok(())
}

pub async fn github_app(opts: GithubAppOpts, clone_base: &str) -> Result<GithubAppOutput> {
    let ui = crate::ui::ui();
    require_connect_inputs(&opts)?;

    // On a real run helm must be on PATH before either the values read below
    // or the upgrade further down; a --dry-run has always worked with no
    // tooling and no cluster, so it stays exempt.
    if !opts.common.dry_run {
        require_on_path("helm")?;
    }
    // The dry-run guard has no release state to judge and remains a no-op. A
    // real run evaluates the guard below against the exact captured revision,
    // so inventory and credential decisions cannot come from different reads.
    guard_byo_key_conflict(&opts, None)?;

    let cmds = if opts.disconnect {
        disconnect_commands(&opts)
    } else {
        connect_commands(&opts, clone_base)
    };
    // `--dry-run` stays offline, so it renders the chart's no-override rule
    // rather than asking the cluster (#1533). The live path below discovers the
    // rendered fullname, which is override-proof.
    if opts.common.dry_run {
        let rollout = rollout_commands(
            &opts.common.namespace,
            &crate::ops::chart_fullname(&opts.common.release),
        );
        return Ok(GithubAppOutput::DryRun(crate::ui::DryRunPlan {
            lines: cmds
                .iter()
                .chain(rollout.iter())
                .map(|cmd| cmd.display())
                .collect(),
        }));
    }

    require_on_path("kubectl")?;
    let ownership = HelmOwnership {
        release: &opts.common.release,
        namespace: &opts.common.namespace,
    };
    let prior_snapshot = current_history_snapshot(&opts).await?;
    if prior_snapshot.head_status.starts_with("pending-") {
        return Err(pending_head_failure(&opts, &prior_snapshot));
    }
    let prior_revision = prior_snapshot.active_revision;
    let revision_values = read_release_values_at_revision(&opts, prior_revision)
        .await
        .map_err(|error| manifest_preflight_failure(&opts, prior_revision, &error))?;
    guard_byo_key_conflict(&opts, revision_values.as_ref())?;
    // Probe the configured App before either sandbox reconciliation or Helm
    // mutation. Disconnect has no credential to authenticate; dry-run is offline.
    if !opts.disconnect {
        guard_app_identity(&opts, clone_base).await?;
    }
    let expected_inventory = ExpectedSandboxInventory::from_values(revision_values.as_ref())?;
    let prior_sandboxes = read_desired_sandboxes(&opts, Some(prior_revision))
        .await
        .map_err(|error| manifest_preflight_failure(&opts, prior_revision, &error))?;
    prior_sandboxes
        .validate_inventory(&expected_inventory)
        .map_err(|error| manifest_preflight_failure(&opts, prior_revision, &error))?;
    reconcile_live_sandboxes(&opts, ownership, &prior_snapshot, &prior_sandboxes)
        .await
        .map_err(|failure| {
            map_reconcile_failure(
                &opts,
                prior_revision,
                &prior_snapshot,
                |error| format!(
                    "release {} is unsafe to mutate because its pre-existing sandbox set could not be reconciled: {error}",
                    opts.common.release
                ),
                failure,
            )
        })?;

    let cl = ui.checklist();
    let label = if opts.disconnect {
        format!(
            "clearing the GitHub App from release {}",
            opts.common.release
        )
    } else {
        format!(
            "configuring the GitHub App on release {}",
            opts.common.release
        )
    };
    let ok_detail = if opts.disconnect {
        "cleared"
    } else {
        "configured"
    };
    let mut helm_failure = None;
    for cmd in &cmds {
        ui.plumbing(&format!("+ {}", cmd.display()));
        let step = cl.step(&label);
        match run_helm_bounded(
            cmd,
            helm_mutation_wall_timeout(),
            "the GitHub App Helm mutation",
        )
        .await
        {
            Ok((true, _, _)) => step.done(ok_detail),
            Ok((false, _, _)) | Err(_) => {
                step.fail("failed; recovering sandbox resources");
                helm_failure = Some(());
                break;
            }
        }
    }

    let post_snapshot = current_history_snapshot(&opts).await.map_err(|_| {
        revision_indeterminate_failure_with_recovery(
            &opts,
            &prior_snapshot,
            "immediately after the Helm mutation",
            None,
        )
    })?;
    if helm_failure.is_some() {
        if post_snapshot != prior_snapshot
            && !post_snapshot.is_normal_failed_successor_of(&prior_snapshot)
        {
            return Err(revision_stability_failure(
                &opts,
                &prior_snapshot,
                &post_snapshot,
                "after the Helm mutation failed or became indeterminate",
            ));
        }
    } else {
        let expected_revision = prior_snapshot.head_revision.checked_add(1).ok_or_else(|| {
            crate::exit::CliError::failure(
                "the captured Helm history head cannot have a representable successor",
            )
            .with_fix(format!(
                "inspect release history with `{}`",
                helm_history_cmd(&opts.common).display()
            ))
        })?;
        let expected_post_snapshot = HelmHistorySnapshot {
            active_revision: expected_revision,
            head_revision: expected_revision,
            head_status: "deployed".to_string(),
        };
        if post_snapshot != expected_post_snapshot {
            return Err(revision_stability_failure(
                &opts,
                &expected_post_snapshot,
                &post_snapshot,
                &format!(
                    "after the credential mutation from active prior revision {prior_revision}"
                ),
            ));
        }
    }
    let post_revision = post_snapshot.active_revision;
    let post_sandboxes = read_desired_sandboxes(&opts, Some(post_revision)).await;
    let manifest_is_safe = post_sandboxes.as_ref().is_ok_and(|current| {
        if helm_failure.is_some() {
            current == &prior_sandboxes
        } else {
            current.preserves(&prior_sandboxes)
        }
    });
    if !manifest_is_safe {
        let reason = match &post_sandboxes {
            Ok(_) => "the credential Helm attempt removed or changed a previously deployed sandbox object".to_string(),
            Err(error) => format!("the post-attempt Helm manifest could not prove sandbox ownership: {error}"),
        };
        let restoration =
            restore_captured_sandboxes(&opts, ownership, &post_snapshot, &prior_sandboxes).await;
        let message = match restoration {
            Ok(()) => format!(
                "{reason}; the captured pre-mutation sandbox objects were restored and verified, but Helm ownership is still unsafe"
            ),
            Err(failure) => {
                return Err(map_reconcile_failure(
                    &opts,
                    prior_revision,
                    &post_snapshot,
                    |error| {
                        format!(
                            "{reason}; captured sandbox restoration failed within the bounded recovery barrier: {error}"
                        )
                    },
                    failure,
                ));
            }
        };
        return Err(recovery_failure(&opts, prior_revision, message));
    }

    let post_sandboxes = post_sandboxes.expect("manifest_is_safe requires a parsed manifest");
    reconcile_live_sandboxes(&opts, ownership, &post_snapshot, &post_sandboxes)
        .await
        .map_err(|failure| {
            map_reconcile_failure(
                &opts,
                prior_revision,
                &post_snapshot,
                |error| {
                    format!(
                        "release {} still has {error} after {} bounded recovery attempts",
                        opts.common.release, SANDBOX_RECOVERY_ATTEMPTS
                    )
                },
                failure,
            )
        })?;

    if helm_failure.is_some() {
        return Err(recovery_failure(
            &opts,
            prior_revision,
            "Helm mutation failed after the sandbox recovery barrier restored and verified the captured release-owned set; no API rollout was started",
        ));
    }
    // A secretKeyRef env var is resolved once at pod start, so the Secret
    // change alone leaves the running API on the old credential.
    //
    // Resolved HERE rather than above: the rollout is the only consumer of the
    // fullname, so a helm failure in the loop above no longer pays for a
    // discovery round-trip whose answer nothing reads (#1533). The live path
    // discovers the rendered name, which is override-proof.
    let fullname = crate::ops::release_fullname(&opts.common.namespace, &opts.common.release).await;
    let rollout = rollout_commands(&opts.common.namespace, &fullname);
    let roll_label = format!("rolling {} to pick up the credential", opts.common.release);
    let mut rollout_failure = None;
    for cmd in &rollout {
        ui.plumbing(&format!("+ {}", cmd.display()));
        let step = cl.step(&roll_label);
        match tokio::time::timeout(KUBECTL_ROLLOUT_WALL_TIMEOUT, run_capture(cmd)).await {
            Ok(Ok((true, _, _))) => step.done("rolled"),
            Ok(Ok((false, _, _))) | Ok(Err(_)) | Err(_) => {
                step.fail("failed; verifying sandbox recovery");
                rollout_failure = Some(());
                break;
            }
        }
    }

    let final_snapshot = current_history_snapshot(&opts).await.map_err(|_| {
        revision_indeterminate_failure_with_recovery(
            &opts,
            &post_snapshot,
            "after API rollout",
            None,
        )
    })?;
    if final_snapshot != post_snapshot {
        return Err(revision_stability_failure(
            &opts,
            &post_snapshot,
            &final_snapshot,
            "during API rollout",
        ));
    }
    let final_revision = final_snapshot.active_revision;
    let final_sandboxes = read_desired_sandboxes(&opts, Some(final_revision)).await;
    let final_sandboxes = match final_sandboxes {
        Ok(current) if current == post_sandboxes => current,
        Ok(_) | Err(_) => {
            let restoration =
                restore_captured_sandboxes(&opts, ownership, &final_snapshot, &prior_sandboxes)
                    .await;
            let detail = match restoration {
                Ok(()) => String::new(),
                Err(failure) => {
                    return Err(map_reconcile_failure(
                        &opts,
                        prior_revision,
                        &final_snapshot,
                        |error| format!("captured sandbox restoration failed: {error}"),
                        failure,
                    ));
                }
            };
            let command = helm_manifest_command(&opts, Some(final_revision)).display();
            return Err(crate::exit::CliError::failure(format!(
                "release {} revision {} manifest was not stable through API rollout{detail}",
                opts.common.release, final_revision
            ))
            .with_fix(format!(
                "inspect the immutable revision with `{command}` and reconcile its SandboxTemplate/SandboxWarmPool ownership; no automatic rollback was attempted"
            ))
            .into());
        }
    };
    reconcile_live_sandboxes(&opts, ownership, &final_snapshot, &final_sandboxes)
        .await
        .map_err(|failure| {
            map_reconcile_failure(
                &opts,
                prior_revision,
                &final_snapshot,
                |error| {
                    format!(
                        "release {} lost sandbox state during API rollout: {error}",
                        opts.common.release
                    )
                },
                failure,
            )
        })?;
    if rollout_failure.is_some() {
        return Err(recovery_failure(
            &opts,
            prior_revision,
            "API rollout failed; the bounded sandbox recovery barrier restored and verified the release-owned sandbox set before returning",
        ));
    }
    if opts.disconnect {
        ui.note("GitHub App cleared; the platform falls back to api.githubToken");
    } else if !opts.existing_secret.trim().is_empty() {
        // The Secret NAME is safe to print -- it is a name, not a credential,
        // and the CLI never reads the Secret's contents on this path at all.
        // Naming it is the point: the operator has to know which Secret and
        // which data key they now own the rotation of.
        ui.note(&format!(
            "GitHub App configured to read its private key from Secret {} (key {}); \
             the API has been rolled onto it. You own that Secret's contents -- rotate \
             by updating it and re-running this command.",
            opts.existing_secret, opts.existing_secret_key
        ));
    } else {
        ui.note(
            "GitHub App configured. Install it on the repositories you deploy from, \
             then a push to your dev/main branch deploys with no workflow in the agent repo.",
        );
    }
    Ok(GithubAppOutput::Done {
        configured: !opts.disconnect,
    })
}

/// The Kubernetes cap on an RFC-1123 subdomain, which is what a Secret name is.
const MAX_SECRET_NAME_LEN: usize = 253;

/// True when `value` is a syntactically valid Kubernetes Secret NAME: an
/// RFC-1123 subdomain.
///
/// Validated positively, against the syntax the flag names, because
/// `--set-string` is NOT an escaping mechanism. It stops helm *typing* a value,
/// but helm still splits the expression on commas STRUCTURALLY, so
/// `--existing-secret-key 'privateKey,api.githubAppExistingSecret='` is one
/// argv entry that helm reads as TWO assignments -- the second blanking the BYO
/// reference the same command just set. The run then also clears the inline
/// key, rolls the API and reports success on a release with no usable key at
/// all. An unvalidated name here is an EXPRESSION-INJECTION vector, not merely
/// an invalid name, and k8s never gets to reject it with its own message
/// because the injected assignment blanked the field before it ever rendered.
///
/// A positive charset rather than a blocklist of dangerous characters: `,`,
/// `=`, `\`, spaces and newlines fall out as rejected by construction, along
/// with whatever a future helm decides is structural.
fn is_rfc1123_subdomain(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SECRET_NAME_LEN
        && value.split('.').all(is_rfc1123_label)
}

/// One dot-separated label of an RFC-1123 subdomain: lowercase alphanumerics
/// and `-`, starting and ending alphanumeric, never empty.
fn is_rfc1123_label(label: &str) -> bool {
    let alnum = |c: char| c.is_ascii_lowercase() || c.is_ascii_digit();
    let (Some(first), Some(last)) = (label.chars().next(), label.chars().next_back()) else {
        return false;
    };
    alnum(first) && alnum(last) && label.chars().all(|c| alnum(c) || c == '-')
}

/// True when `value` is a syntactically valid Secret DATA key: `[-._a-zA-Z0-9]+`
/// and neither `.` nor `..`, which is what the API server accepts inside a
/// Secret's `data` map. Same injection reasoning as [`is_rfc1123_subdomain`];
/// the charset is wider because the key names a map entry, not a DNS name.
fn is_secret_data_key(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

/// True when `value` is a GitHub App id: one or more ASCII digits, no leading
/// zero, and not `"0"`.
///
/// The CHARSET is validated, deliberately not a parse into an integer type.
/// `9007199254740993` is above 2^53 and must round-trip EXACTLY -- that is the
/// #1236 fix on the CLI path, and a `f64` hop silently renders it as
/// `9007199254740992`. Parsing into a `u64` would survive today and still put a
/// ceiling on an id GitHub is free to grow past, at which point a perfectly
/// valid App would be refused by our own arithmetic. Nothing here needs the
/// NUMBER; the value is a string on the wire, in the release, and in the JWT's
/// `iss` claim, so it stays a string the whole way.
///
/// A positive charset rather than a blocklist, for exactly the reason spelled
/// out above [`is_rfc1123_subdomain`]: `--set-string` stops helm TYPING a value
/// but helm still splits the expression on commas STRUCTURALLY, so
/// `--app-id '1,api.githubCloneBase=https://evil.example.com'` is one argv entry
/// helm reads as TWO assignments -- the second silently re-pointing every clone
/// at a host the operator never named. With digits-only, that case and
/// `123,api.githubToken=INJECTED-PAT` fall out as refused by construction, along
/// with `=`, spaces, newlines and whatever a future helm decides is structural.
///
/// The leading zero is refused rather than normalised: `0001234` is not the id
/// on the settings page, so accepting it would mean guessing that the operator
/// meant `1234` on the one credential that mints tokens for every repository in
/// the installation. `0` is not an App id at all.
fn is_github_app_id(value: &str) -> bool {
    !value.is_empty() && !value.starts_with('0') && value.chars().all(|c| c.is_ascii_digit())
}

/// True when `body` has the SHAPE of a PEM private key: a `-----BEGIN` line and
/// a matching `-----END` line that name the SAME PRIVATE KEY block.
///
/// A shape check, not a parse: the chart hands helm the PATH and helm reads the
/// file, so the CLI is not in the business of validating key material -- it is
/// in the business of catching the file that is obviously not one. The two
/// realistic mistakes are the `.pub`/`.txt` next to the PEM in ~/Downloads, and
/// a file the download never wrote anything into.
///
/// The empty case is why this exists at all. A 0-byte file passes `is_file`,
/// helm renders `githubAppPrivateKey: ""`, and the platform's `is_configured`
/// then answers False and falls back to `api.githubToken` -- so the App is
/// silently not in use while the CLI printed "GitHub App configured" and rolled
/// the API to prove it. Refusing here is the only point in the chain where that
/// is still visible to the operator.
///
/// The BEGIN and END labels must match. Checking each marker line
/// independently (any BEGIN line plus any END line, regardless of which block
/// each names) let a `-----BEGIN ... PRIVATE KEY-----` naming one block type
/// pair with an unrelated `-----END ... PRIVATE KEY-----` naming a different
/// one -- e.g. two truncated halves of different keys concatenated -- pass as
/// "shaped like a PEM". That file is not usable, and it reaches the exact same
/// silent-failure path as the empty file above: helm ships it, `is_configured`
/// answers True, and signing 401s at runtime with the operator already told
/// the App is configured.
///
/// This intentionally does NOT narrow the accepted labels to `RSA PRIVATE
/// KEY`. A GitHub App key is RSA today, but PKCS#8 (`PRIVATE KEY`),
/// `ENCRYPTED PRIVATE KEY`, and `EC PRIVATE KEY` are all real, legitimately
/// downloaded key shapes. Tightening this to an RSA-only allowlist would
/// falsely refuse a real key the operator downloaded, which is a worse
/// regression than the false-positive this function exists to close -- so any
/// label ending in `PRIVATE KEY` is accepted as long as BEGIN and END agree on
/// it.
fn is_pem_private_key(body: &str) -> bool {
    let marker_label = |line: &str, marker: &str| -> Option<String> {
        line.trim()
            .strip_prefix(marker)?
            .strip_suffix("-----")
            .map(|label| label.trim().to_string())
    };
    let begin_label = body.lines().find_map(|l| marker_label(l, "-----BEGIN"));
    let end_label = body.lines().find_map(|l| marker_label(l, "-----END"));
    matches!(
        (begin_label, end_label),
        (Some(begin), Some(end)) if begin == end && end.ends_with("PRIVATE KEY")
    )
}

/// Render a rejected value for an error message without echoing something that
/// might be key material.
///
/// Both flags take short names, so a short value is quoted back verbatim (with
/// `{:?}`, so an embedded newline or tab shows as an escape rather than
/// mangling the message). Anything long enough to be a pasted PEM is described
/// by its length only -- the operator knows what they typed, and the terminal,
/// the shell history and the `--json` error payload do not need a copy of it.
fn describe_rejected_value(value: &str) -> String {
    const MAX_ECHO: usize = 63;
    let len = value.chars().count();
    if len <= MAX_ECHO {
        format!("{value:?}")
    } else {
        format!("<{len} characters, not shown>")
    }
}

/// Validate the flag combination before anything reaches helm.
///
/// Takes the whole record rather than five positional `&str`/`bool` arguments:
/// the old three-argument form was already one argument swap away from a
/// silent bug, and the rules below now read five of the fields.
///
/// All thirteen refusals here are deterministic input errors -- the identical
/// argv fails identically every time -- so they exit 2 (ADR-0021 Usage) with a
/// non-null `fix` naming the flag to correct (#1261). A bare `bail!` classified
/// them as exit 1 with a null fix -- indistinguishable to an agent from the helm
/// upgrade itself failing, which is retryable and these are not. clap gives
/// every one of these flags a `default_value`, so clap never raises its own exit
/// 2 for them and this is the only place the class is set. That covers the three
/// original refusals (a missing `--app-id`, a missing `--private-key`, a
/// `--private-key` path that is not a file), the five `--existing-secret` rules
/// #1255 adds below, and the five #1260 adds: an `--app-id` that is not a
/// positive decimal integer, and a `--private-key` that is a directory,
/// unreadable, empty or not PEM-shaped. Every one is the same category and takes
/// the same class.
///
/// The #1260 arms exist because "the file is there" and "the id is non-empty"
/// were the whole of the check: `--app-id abc` and a 0-byte PEM both reached
/// helm, exited 0, and printed "GitHub App configured" over an install where the
/// App was never in use.
///
/// The refusals that are NOT here stay `CliError::failure` on purpose:
/// [`guard_byo_key_conflict`] and [`opaque_byo_field_error`] judge the state of
/// the DEPLOYED release, so the same argv succeeds once the operator updates it.
pub fn require_connect_inputs(opts: &GithubAppOpts) -> Result<()> {
    // The one fact every rule below branches on: an empty --existing-secret is
    // the chart-held path, a non-empty one is BYO. Bound once so a new rule
    // cannot pick the wrong polarity.
    let byo = !opts.existing_secret.trim().is_empty();
    // The path itself is used verbatim for the stat and the message, as it
    // has been since #1223.
    let key_path = &opts.private_key_path;

    // "--disconnect --existing-secret X" reads as "point at X while
    // disconnecting". Accepting it would clear the release and leave the
    // operator believing a reference to X was set. Every OTHER connect input
    // stays silently tolerated under --disconnect, as it has been since #1223.
    if opts.disconnect {
        if byo {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(
                    "--existing-secret contradicts --disconnect: --disconnect clears the App \
                     credentials, so there is nothing left to point at a Secret. Run \
                     --disconnect on its own, or drop it to configure the Secret.",
                )
                .with_fix(
                    "drop --existing-secret to clear the App, or drop --disconnect to point \
                     the release at that Secret",
                ),
            ));
        }
        return Ok(());
    }
    // Syntax-checked BEFORE any command is constructed, because helm splits a
    // --set-string expression on commas structurally: an unvalidated name is an
    // expression-injection vector, not merely an invalid name. See
    // `is_rfc1123_subdomain` for the full mechanism.
    //
    // The checks below use the RAW value, not `byo`: `connect_commands` formats
    // the raw field into argv, so a name with surrounding whitespace would pass
    // a trimmed check and still reach helm -- and k8s -- wrong.
    if byo {
        if !is_rfc1123_subdomain(&opts.existing_secret) {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "--existing-secret {} is not a Kubernetes Secret name. It must be an \
                     RFC-1123 subdomain: lowercase letters, digits, '-' and '.', starting and \
                     ending with a letter or digit, at most {MAX_SECRET_NAME_LEN} characters. \
                     It names a Secret you already created; to hand the PEM itself to the \
                     chart, use --private-key.",
                    describe_rejected_value(&opts.existing_secret)
                ))
                .with_fix(
                    "rerun with --existing-secret <the name of a Secret you already created>, \
                     or use --private-key <path to the PEM> to hand the key to the chart",
                ),
            ));
        }
        if !is_secret_data_key(&opts.existing_secret_key) {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "--existing-secret-key {} is not a Kubernetes Secret data key. It must be \
                     one or more of [-._a-zA-Z0-9] and cannot be '.' or '..'. It names a key \
                     INSIDE that Secret -- not the PEM, and not a helm expression.",
                    describe_rejected_value(&opts.existing_secret_key)
                ))
                .with_fix(
                    "rerun with --existing-secret-key <the data key inside that Secret>, made \
                     only of [-._a-zA-Z0-9]",
                ),
            ));
        }
    }
    if byo && !key_path.trim().is_empty() {
        return Err(anyhow::Error::from(
            crate::exit::CliError::usage(
                "--existing-secret and --private-key are two mutually exclusive ways to supply \
                 the App's private key; pick one. --existing-secret references a Secret you \
                 manage, --private-key hands the PEM to the chart.",
            )
            .with_fix("pass either --existing-secret <name> or --private-key <path>, not both"),
        ));
    }
    if !byo && opts.existing_secret_key.trim() != DEFAULT_APP_KEY_DATA_KEY {
        return Err(anyhow::Error::from(
            crate::exit::CliError::usage(
                "--existing-secret-key configures nothing without --existing-secret: the chart \
                 only reads a data key once a Secret name is set. Pass --existing-secret <name>, \
                 or drop --existing-secret-key.",
            )
            .with_fix(
                "rerun with --existing-secret <the Secret that holds the PEM>, or drop \
                 --existing-secret-key to stay on the chart-held key",
            ),
        ));
    }
    if opts.app_id.trim().is_empty() {
        return Err(anyhow::Error::from(
            crate::exit::CliError::usage(
                "--app-id is required. Find it on the App's settings page \
                 (Settings -> Developer settings -> GitHub Apps -> your app).",
            )
            .with_fix("rerun with --app-id <numeric app id from the App's settings page>"),
        ));
    }
    // Checked on the TRIMMED form, and `connect_commands` emits that same
    // trimmed form -- the two must agree, or a value validated here reaches
    // helm in a shape that was never validated. See `is_github_app_id` for why
    // the charset is checked rather than the value parsed.
    if !is_github_app_id(opts.app_id.trim()) {
        return Err(anyhow::Error::from(
            crate::exit::CliError::usage(format!(
                "--app-id {} is not a GitHub App id. It must be a positive decimal integer: \
                 digits only, no leading zero. The id is the number on the App's settings page \
                 (Settings -> Developer settings -> GitHub Apps -> your app); it is not the App \
                 slug, the client id, or the installation id.",
                describe_rejected_value(opts.app_id.trim())
            ))
            .with_fix(
                "rerun with --app-id <the numeric App ID from the App's settings page>, digits \
                 only",
            ),
        ));
    }
    // Both remaining checks are chart-held only: a BYO run supplies no PEM by
    // design, so it must never be asked for one and must never stat a path it
    // was never given.
    if !byo {
        if key_path.trim().is_empty() {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(
                    "--private-key is required: the path to the App's PEM file, \
                     downloaded from the App's settings page under 'Private keys'.",
                )
                .with_fix("rerun with --private-key <path to the App's PEM file>"),
            ));
        }
        let path = std::path::Path::new(key_path);
        // A directory BEFORE the is_file check, which answers false for one and
        // would report "no such file" about something that plainly exists --
        // sending the operator to look for a typo in a path that is correct.
        // `~/Downloads/my-app.private-key.pem` tab-completing to its parent is
        // the ordinary way this happens.
        if path.is_dir() {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "--private-key: {key_path} is a directory, not a PEM file"
                ))
                .with_fix("rerun with --private-key pointing at the .pem file inside it"),
            ));
        }
        if !path.is_file() {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!("--private-key: no such file: {key_path}"))
                    .with_fix("rerun with --private-key pointing at an existing PEM file"),
            ));
        }
        // Read here even though helm is the one that reads the file for real:
        // `--set-file` on an unreadable path fails DURING the upgrade, after
        // the release has already begun changing, and helm's own message about
        // it is opaque. Doing it first turns that into an input error, which is
        // what it is. A non-UTF-8 file lands in this arm too, which is correct:
        // a PEM is ASCII.
        let body = match std::fs::read_to_string(path) {
            Ok(body) => body,
            Err(err) => {
                return Err(anyhow::Error::from(
                    crate::exit::CliError::usage(format!(
                        "--private-key: cannot read {key_path}: {err}"
                    ))
                    .with_fix(
                        "rerun with --private-key pointing at a readable PEM file, or fix the \
                         file's permissions",
                    ),
                ));
            }
        };
        // An empty file is the one that MUST be refused rather than passed
        // through. helm renders `githubAppPrivateKey: ""` from it, the
        // platform's `is_configured` then answers False and silently falls back
        // to `api.githubToken` -- so the App is not in use at all, while this
        // command printed "GitHub App configured" and rolled the API to prove
        // it. Nothing downstream ever surfaces that; this is the last place it
        // is visible. Checked with `trim` because a file holding only a newline
        // is the same nothing.
        if body.trim().is_empty() {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!("--private-key: {key_path} is empty"))
                    .with_fix(
                        "re-download the App's private key (its settings page, under 'Private \
                         keys') and rerun with --private-key pointing at that file",
                    ),
            ));
        }
        // Same false-success shape, one step less obvious: a non-PEM file (the
        // .pub or the .txt sitting beside it in ~/Downloads) renders as a
        // perfectly non-empty value that no JWT can ever be signed with, and
        // every GitHub call 401s long after this command reported success.
        //
        // The refusal names the PATH and the SHAPE only. The contents are never
        // echoed -- not into the terminal, the shell history, or the `--json`
        // error payload -- because on the day this fires against a real PEM
        // (a misdetected shape) the message would be a copy of the one
        // credential that mints tokens for every repository in the installation.
        if !is_pem_private_key(&body) {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "--private-key: {key_path} is not a PEM private key. It must contain a \
                     '-----BEGIN ... PRIVATE KEY-----' line and a matching '-----END' line. \
                     A public key, a .txt, or a partial download all look like a configured App \
                     to this command and then 401 on every GitHub call."
                ))
                .with_fix(
                    "rerun with --private-key pointing at the .pem file downloaded from the \
                     App's settings page under 'Private keys'",
                ),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One PEM marker line, composed from its boundary word rather than
    /// written out whole.
    ///
    /// These fixtures carry no key material -- the bytes between the
    /// markers (where present) are inert, and nothing here is ever parsed
    /// as a key; `is_pem_private_key` only checks the SHAPE. Composing the
    /// line keeps a fixture that exists to test PEM validation from reading
    /// as a pasted credential to a secret scanner, which is a false
    /// positive that would otherwise have to be allowlisted in every repo
    /// that vendors this test.
    fn pem_marker(boundary: &str) -> String {
        pem_marker_labeled(boundary, "RSA PRIVATE KEY")
    }

    /// Sibling to [`pem_marker`] that lets a test pick the label, so a BEGIN
    /// and END pair with mismatched labels can be composed without ever
    /// writing the literal marker text into the test source.
    fn pem_marker_labeled(boundary: &str, label: &str) -> String {
        format!("-----{boundary} {label}-----")
    }

    fn opts(disconnect: bool) -> GithubAppOpts {
        GithubAppOpts {
            common: CommonOpts {
                namespace: "curie".into(),
                release: "curie".into(),
                dry_run: true,
            },
            chart: "charts/curie".into(),
            app_id: "12345".into(),
            private_key_path: "/tmp/app.pem".into(),
            existing_secret: String::new(),
            existing_secret_key: DEFAULT_APP_KEY_DATA_KEY.into(),
            disconnect,
        }
    }

    /// A valid BYO connect: the operator owns the Secret, so no PEM path is
    /// supplied at all. Supplying both is what `require_connect_inputs` refuses
    /// (`existing_secret_with_a_private_key_is_refused`), so this is the shape
    /// a real `--existing-secret` invocation actually has.
    fn byo_opts() -> GithubAppOpts {
        let mut o = opts(false);
        o.private_key_path = String::new();
        o.existing_secret = "my-github-app".into();
        o
    }

    fn argv(cmd: &OpsCommand) -> Vec<String> {
        cmd.argv()
    }

    /// True when `args` carries `value` as a WHOLE argv entry.
    ///
    /// Whole entries, never `contains` on the joined string:
    /// `contains("api.githubAppExistingSecret=")` is also satisfied by
    /// `api.githubAppExistingSecret=something-else`, so it tests for a prefix
    /// rather than for the value that was actually set (#1263).
    fn has_entry(args: &[String], value: &str) -> bool {
        args.iter().any(|a| a == value)
    }

    /// True when any whole entry begins with `prefix`. Only ever used to assert
    /// ABSENCE of a whole value family, which is the one question a prefix
    /// legitimately answers.
    fn has_entry_starting(args: &[String], prefix: &str) -> bool {
        args.iter().any(|a| a.starts_with(prefix))
    }

    /// The whole argv entry immediately preceding `value`.
    ///
    /// Panics rather than returning an Option: a test that silently skips its
    /// own assertion because the entry moved is the decoration #1263 found.
    fn flag_before(args: &[String], value: &str) -> String {
        let at = args
            .iter()
            .position(|a| a == value)
            .unwrap_or_else(|| panic!("no argv entry equal to `{value}`: {args:?}"));
        assert!(at > 0, "`{value}` has no preceding flag: {args:?}");
        args[at - 1].clone()
    }

    /// The ADR-0021 `fix` hint an error carries, recovered through the very
    /// `exit::classify` the `--json` error emitter uses. A refusal whose fix
    /// does not survive that path is invisible to the agent driving the CLI,
    /// which is the consumer this ticket exists to stop misleading.
    fn fix_of(err: &anyhow::Error) -> String {
        let (class, fix) = crate::exit::classify(err);
        assert_eq!(
            class,
            crate::exit::ExitClass::Failure,
            "the refusal must exit non-zero as a real classification: {err}"
        );
        fix.unwrap_or_else(|| panic!("the refusal must carry an actionable fix: {err}"))
    }

    #[test]
    fn the_app_id_is_set_as_a_string() {
        // helm's `--set` parses a bare number, and a --reuse-values round trip
        // turns it into a float64. App id 1234567 then renders as
        // "1.234567e+06", the JWT's `iss` claim is wrong, and EVERY GitHub call
        // answers 401. Found on a live cluster -- a chart-render test cannot
        // see it, because it only appears once a real numeric value has been
        // through helm's stored values.
        let cmds = connect_commands(&opts(false), DEFAULT_CLONE_BASE);
        let flat = argv(&cmds[0]).join(" ");
        assert!(
            flat.contains("--set-string api.githubAppId="),
            "app id must use --set-string, not --set: {flat}"
        );
    }

    /// #1533 (S18): the rollout is what makes a `cluster github-app` connect
    /// take effect. Its own doc comment names the failure a wrong target
    /// reintroduces -- "the operator sees 'configured' and pushes still fail to
    /// clone" -- which is exactly the symptom PR #1223 was filed to eliminate.
    /// Under `--release platform` the chart renders `platform-curie-api` and the
    /// CLI asked for `platform-api`.
    #[test]
    fn rollout_commands_target_the_chart_rendered_api_deployment() {
        let cmds = rollout_commands("acme-system", &crate::ops::chart_fullname("platform"));
        let rendered: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
        assert_eq!(
            rendered,
            vec![
                "kubectl -n acme-system rollout restart deployment/platform-curie-api",
                "kubectl -n acme-system rollout status deployment/platform-curie-api \
                 --timeout=180s",
            ]
        );
    }

    /// Negative control: the default release renders byte-identically to what
    /// shipped before #1533.
    #[test]
    fn the_default_release_rollout_is_unchanged() {
        let cmds = rollout_commands("curie", &crate::ops::chart_fullname("curie"));
        let rendered: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
        assert_eq!(
            rendered,
            vec![
                "kubectl -n curie rollout restart deployment/curie-api",
                "kubectl -n curie rollout status deployment/curie-api --timeout=180s",
            ]
        );
    }

    /// A real file holding a PEM-shaped body, so "the contents never reach
    /// argv" is checked against contents that exist.
    ///
    /// The previous version pointed at `/tmp/app.pem`, which was never created.
    /// Inlining `read_to_string(path)` into argv therefore stayed green -- it
    /// read `""` -- so the assertion guarded a literal `BEGIN` and not the
    /// realistic regression (#1263).
    fn key_fixture() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("app.pem");
        std::fs::write(
            &path,
            "-----BEGIN RSA PRIVATE KEY-----\nMIIEowIBAAKCAQEAtestkeymaterial\n-----END RSA PRIVATE KEY-----\n",
        )
        .expect("write fixture");
        let as_str = path.to_string_lossy().into_owned();
        (dir, as_str)
    }

    #[test]
    fn the_private_key_contents_never_reach_argv() {
        // The whole reason for --set-file. A PEM in argv is readable by `ps`
        // and can be echoed by a subprocess error.
        let (_dir, path) = key_fixture();
        let body = std::fs::read_to_string(&path).expect("fixture readable");
        let mut o = opts(false);
        o.private_key_path = path.clone();

        let flat = argv(&connect_commands(&o, DEFAULT_CLONE_BASE)[0]).join(" ");
        assert!(flat.contains("--set-file"), "{flat}");
        assert!(
            flat.contains(&format!("api.githubAppPrivateKey={path}")),
            "{flat}"
        );
        // The real assertion: no line of the file's CONTENT appears anywhere.
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            assert!(!flat.contains(line), "key material reached argv: {line}");
        }
    }

    #[test]
    fn connecting_also_sets_the_clone_base() {
        // An empty base fails git-flow before a credential is ever consulted,
        // with an error about schemes that reads like a bug rather than a
        // missing setting.
        let flat = argv(&connect_commands(&opts(false), DEFAULT_CLONE_BASE)[0]).join(" ");
        assert!(flat.contains("api.githubCloneBase=https://github.com"));
    }

    #[test]
    fn a_custom_clone_base_is_honoured() {
        // Passing DEFAULT_CLONE_BASE and asserting the default appears also
        // passes when the parameter is ignored entirely (#1263). A GitHub
        // Enterprise install would silently get github.com and every clone
        // would fail an origin check.
        let flat = argv(&connect_commands(&opts(false), "https://ghe.example.com")[0]).join(" ");
        assert!(
            flat.contains("api.githubCloneBase=https://ghe.example.com"),
            "the supplied clone base was ignored: {flat}"
        );
    }

    #[test]
    fn the_upgrade_reuses_existing_values() {
        // Dropping --reuse-values resets every other value to chart defaults:
        // Slack tokens, the model credential, the connector reconciler flag.
        // Silent, destructive, and uncaught (#1263). The BYO branch is a third
        // command builder and carries the same obligation.
        for cmds in [
            connect_commands(&opts(false), DEFAULT_CLONE_BASE),
            connect_commands(&byo_opts(), DEFAULT_CLONE_BASE),
            disconnect_commands(&opts(true)),
        ] {
            let flat = argv(&cmds[0]).join(" ");
            assert!(
                flat.contains("--reuse-values"),
                "would reset other values: {flat}"
            );
        }
    }

    #[test]
    fn disconnect_clears_both_app_fields_and_touches_nothing_else() {
        // Asserted as whole argv entries, not with `contains` on the joined
        // string: `contains("api.githubAppId=")` is also satisfied by
        // `api.githubAppId=999`, so it checked for the presence of a prefix
        // rather than for clearing (#1263).
        let args = argv(&disconnect_commands(&opts(true))[0]);
        assert!(
            args.iter().any(|a| a == "api.githubAppId="),
            "the App id was not cleared to empty: {args:?}"
        );
        assert!(
            args.iter().any(|a| a == "api.githubAppPrivateKey="),
            "the private key was not cleared to empty: {args:?}"
        );
        let flat = args.join(" ");
        // The PAT fallback must survive: clearing the App is how an operator
        // goes back to it.
        assert!(!flat.contains("api.githubToken"));
    }

    #[test]
    fn the_api_is_rolled_so_the_new_key_is_actually_read() {
        let cmds = rollout_commands("curie", &crate::ops::chart_fullname("curie"));
        let flat: Vec<String> = cmds.iter().map(|c| argv(c).join(" ")).collect();
        assert!(flat[0].contains("rollout restart deployment/curie-api"));
        assert!(flat[1].contains("rollout status deployment/curie-api"));
    }

    // ---- T1: the BYO connect names the Secret and its data key -------------

    #[test]
    fn the_byo_connect_names_the_secret_and_the_data_key_as_whole_entries() {
        // AC1. If either entry is missing, the chart falls back to the inline
        // key that this same command just cleared: GITHUB_APP_PRIVATE_KEY
        // resolves to nothing, the api pod mints no JWT, and every clone 401s
        // -- while the CLI still reports "GitHub App configured".
        let args = argv(&connect_commands(&byo_opts(), DEFAULT_CLONE_BASE)[0]);
        assert!(
            has_entry(&args, "api.githubAppExistingSecret=my-github-app"),
            "the BYO Secret name never reached helm: {args:?}"
        );
        assert!(
            has_entry(&args, "api.githubAppExistingSecretKey=privateKey"),
            "the BYO data key never reached helm: {args:?}"
        );
        assert_eq!(
            flag_before(&args, "api.githubAppExistingSecret=my-github-app"),
            "--set-string",
            "the Secret name must not be helm-typed: {args:?}"
        );
        assert_eq!(
            flag_before(&args, "api.githubAppExistingSecretKey=privateKey"),
            "--set-string",
            "the data key must not be helm-typed: {args:?}"
        );
    }

    // ---- T2: a custom data key is honoured ---------------------------------

    #[test]
    fn a_custom_existing_secret_key_is_honoured() {
        // Passing the default (`privateKey`) and asserting the default appears
        // also passes when the parameter is ignored entirely (#1263). An
        // operator whose ESO-managed Secret stores the PEM under `app-pem`
        // would get `key: privateKey`, a key that does not exist in that
        // Secret, and the api pod would sit in CreateContainerConfigError.
        let mut o = byo_opts();
        o.existing_secret_key = "app-pem".into();
        let args = argv(&connect_commands(&o, DEFAULT_CLONE_BASE)[0]);
        assert!(
            has_entry(&args, "api.githubAppExistingSecretKey=app-pem"),
            "the supplied data key was ignored: {args:?}"
        );
        assert!(
            !has_entry(&args, "api.githubAppExistingSecretKey=privateKey"),
            "the chart default was emitted over the supplied key: {args:?}"
        );
    }

    // ---- T3: the security property -----------------------------------------

    #[test]
    fn the_byo_connect_never_passes_the_pem_path_to_helm() {
        // THE security property of this path. `--set-file` makes helm read the
        // file and write its CONTENTS into the release, where every retained
        // revision keeps them and `helm get values` prints them back (#1236
        // found the PEM in revision 15 of a live release). On the BYO path the
        // release holds a Secret NAME only, so the PEM's path must not be in
        // the plan at all -- helm must never be told where the file is.
        //
        // These opts also carry a real key path, a combination
        // `require_connect_inputs` refuses. That is deliberate: the input
        // check must not be the only thing standing between a PEM and helm, so
        // the builder is proven to drop the path on its own.
        let (_dir, path) = key_fixture();
        let body = std::fs::read_to_string(&path).expect("fixture readable");
        let mut o = byo_opts();
        o.private_key_path = path.clone();

        let args = argv(&connect_commands(&o, DEFAULT_CLONE_BASE)[0]);
        assert!(
            !has_entry(&args, "--set-file"),
            "the BYO plan makes helm read a file off disk: {args:?}"
        );
        assert!(
            !args.iter().any(|a| a.contains(&path)),
            "the PEM's path reached the BYO plan: {args:?}"
        );
        assert!(
            !has_entry_starting(&args, "api.githubAppPrivateKey=/"),
            "the BYO plan carries a filesystem path as the key: {args:?}"
        );
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            assert!(
                !args.iter().any(|a| a.contains(line)),
                "key material reached argv: {line}"
            );
        }
    }

    // ---- T4: adopting BYO clears the inline key ----------------------------

    #[test]
    fn the_byo_connect_clears_the_inline_private_key() {
        // Decision 2, half one, and the whole reason the BYO path exists.
        // `--reuse-values` copies a still-set api.githubAppPrivateKey into
        // every future revision forever, and `curie cluster up` runs exactly
        // that. Adopting the recommended path while leaving the PEM in release
        // history gives the operator its ceremony and none of its benefit.
        let args = argv(&connect_commands(&byo_opts(), DEFAULT_CLONE_BASE)[0]);
        assert!(
            has_entry(&args, "api.githubAppPrivateKey="),
            "the inline key rides every later revision unless cleared: {args:?}"
        );
    }

    // ---- T5: the chart-held branch must not leak the BYO fields ------------

    #[test]
    fn the_chart_held_connect_never_mentions_the_byo_fields() {
        // Sibling path. If the branch leaks, a plain `--private-key` run writes
        // api.githubAppExistingSecret= into the release and, through
        // --reuse-values, permanently overrides an operator's hand-set BYO
        // reference -- silently moving a working install off the Secret their
        // External Secrets Operator owns.
        let args = argv(&connect_commands(&opts(false), DEFAULT_CLONE_BASE)[0]);
        assert!(
            !has_entry_starting(&args, "api.githubAppExistingSecret"),
            "the BYO branch leaked into the chart-held path: {args:?}"
        );
    }

    // ---- T6: the chart-held plan is unchanged ------------------------------

    #[test]
    fn the_chart_held_connect_plan_is_byte_identical_to_before() {
        // The chart-held path is what every existing install already runs;
        // this ticket adds a branch beside it and must not perturb it. An
        // exact whole-vector comparison pins order, flags, values and the
        // absence of any extra entry at once -- a `contains` sweep cannot see
        // an ADDED entry, which is exactly how a leaked BYO clear arrives.
        let args = argv(&connect_commands(&opts(false), DEFAULT_CLONE_BASE)[0]);
        assert_eq!(
            args,
            vec![
                "upgrade",
                "curie",
                "charts/curie",
                "-n",
                "curie",
                "--reuse-values",
                "--set-string",
                "api.githubAppId=12345",
                "--set-file",
                "api.githubAppPrivateKey=/tmp/app.pem",
                "--set",
                "api.githubCloneBase=https://github.com",
            ]
        );
    }

    // ---- T7 / T8: disconnect (AC3) ----------------------------------------

    #[test]
    fn disconnect_clears_the_byo_secret_name() {
        // AC3. Without this, `--disconnect` leaves api.githubAppExistingSecret
        // set: the CLI reports "GitHub App cleared", the chart still resolves
        // GITHUB_APP_PRIVATE_KEY from the operator's Secret, and the platform
        // keeps authenticating as an App the operator believes is gone.
        let args = argv(&disconnect_commands(&opts(true))[0]);
        assert!(
            has_entry(&args, "api.githubAppExistingSecret="),
            "the BYO Secret reference survived the disconnect: {args:?}"
        );
        assert!(
            has_entry(&args, "api.githubAppId="),
            "the App id was not cleared to empty: {args:?}"
        );
        assert!(
            has_entry(&args, "api.githubAppPrivateKey="),
            "the private key was not cleared to empty: {args:?}"
        );
    }

    #[test]
    fn disconnect_does_not_clear_the_byo_data_key_name() {
        // Decision 2, half two, and the test that stops a future "for
        // symmetry, clear both" refactor.
        //
        // api.githubAppExistingSecretKey has a chart default of `privateKey`.
        // Setting it to "" does NOT restore that default -- `--reuse-values`
        // re-supplies the empty string on every later upgrade, so the release
        // overrides the default permanently. An operator who later hand-sets
        // githubAppExistingSecret with no key then renders `key: ""`, and the
        // api pod sits in CreateContainerConfigError with nothing in the
        // release to explain why. The field is inert while the Secret NAME is
        // empty, so leaving it alone is both correct and strictly safer.
        let args = argv(&disconnect_commands(&opts(true))[0]);
        assert!(
            !has_entry_starting(&args, "api.githubAppExistingSecretKey"),
            "clearing the data key overrides the chart default forever: {args:?}"
        );
    }

    // ---- T9: --set-string, not --set ---------------------------------------

    #[test]
    fn an_all_digit_secret_name_and_data_key_are_set_as_strings() {
        // `1234567` is a valid RFC-1123 label and a valid Secret data key.
        // Under `--set`, helm parses it as a number and a --reuse-values round
        // trip stores it as a float64: the next upgrade renders
        // `1.234567e+06`, the secretKeyRef names a Secret that does not exist,
        // and the api pod never starts. This is #1236's App-ID float bug
        // transplanted into a new field, and a chart-render test cannot see it
        // because it only appears after a real round trip.
        let mut o = byo_opts();
        o.existing_secret = "1234567".into();
        o.existing_secret_key = "8901234".into();
        let args = argv(&connect_commands(&o, DEFAULT_CLONE_BASE)[0]);
        assert_eq!(
            flag_before(&args, "api.githubAppExistingSecret=1234567"),
            "--set-string",
            "an all-digit Secret name must not go through --set: {args:?}"
        );
        assert_eq!(
            flag_before(&args, "api.githubAppExistingSecretKey=8901234"),
            "--set-string",
            "an all-digit data key must not go through --set: {args:?}"
        );
    }

    // ---- T10: the AC2 guard ------------------------------------------------

    #[test]
    fn a_configured_byo_secret_refuses_a_chart_held_private_key() {
        // THIS IS THE TICKET. Without the guard the CLI prints "GitHub App
        // configured", returns {"github_app_configured": true} and rolls the
        // API, while the pod keeps signing with the OLD key -- because the
        // chart resolves GITHUB_APP_PRIVATE_KEY from the BYO Secret whenever
        // api.githubAppExistingSecret is non-empty, so --set-file writes a
        // value nothing reads. The README's next rotation step is "delete the
        // first key on GitHub", at which point every clone 401s and nothing
        // the CLI printed ever hinted at it.
        let existing = serde_json::json!({"api": {"githubAppExistingSecret": "my-github-app"}});
        let refusal = guard_byo_key_conflict(&opts(false), Some(&existing));
        let err = refusal.expect_err("a BYO release must refuse --private-key");
        let msg = err.to_string();
        assert!(
            msg.contains("my-github-app"),
            "the refusal must name the Secret the release actually reads: {msg}"
        );
        assert!(
            msg.contains("privateKey"),
            "the refusal must name the data key inside it: {msg}"
        );
        let fix = fix_of(&err);
        assert!(
            fix.contains("--existing-secret"),
            "the fix must name the way forward: {fix}"
        );
        assert!(
            fix.contains("--disconnect"),
            "the fix must name the way back: {fix}"
        );
        assert!(
            fix.contains("my-github-app"),
            "the fix must name the Secret the operator has to update: {fix}"
        );
    }

    #[test]
    fn a_present_but_empty_byo_secret_does_not_refuse() {
        // `--disconnect` writes api.githubAppExistingSecret="", so the key is
        // PRESENT and empty on every disconnected release. A guard that fires
        // on presence rather than on a non-empty value bricks the documented
        // recovery path: after a disconnect the operator could never return to
        // a chart-held key through the CLI at all.
        let existing = serde_json::json!({"api": {"githubAppExistingSecret": ""}});
        let outcome = guard_byo_key_conflict(&opts(false), Some(&existing));
        assert!(
            outcome.is_ok(),
            "an empty BYO reference is not a BYO release: {:?}",
            outcome.err()
        );
    }

    #[test]
    fn an_absent_release_does_not_refuse() {
        // `fetch_release_values` returns Ok(None) only when helm positively
        // reports the release does not exist. Refusing there would make the
        // verb unusable on a fresh install, and helm's own "release not found"
        // two lines later is the honest error.
        let outcome = guard_byo_key_conflict(&opts(false), None);
        assert!(
            outcome.is_ok(),
            "a release that does not exist configures nothing: {:?}",
            outcome.err()
        );
    }

    #[test]
    fn a_release_with_null_values_does_not_refuse() {
        // helm prints `null` for an existing release with no user-supplied
        // values -- the shape of a default install. Reading that as "a BYO
        // Secret is configured" would refuse the very first github-app run on
        // every such cluster.
        let existing = serde_json::Value::Null;
        let outcome = guard_byo_key_conflict(&opts(false), Some(&existing));
        assert!(
            outcome.is_ok(),
            "null values configure nothing: {:?}",
            outcome.err()
        );
    }

    #[test]
    fn a_custom_data_key_is_echoed_in_the_refusal() {
        // The operator must be told WHICH data key to update, not the chart
        // default. Naming `privateKey` when the release reads `app-pem` sends
        // them to write the PEM into a key nothing reads -- the same
        // misreport one layer down. Non-default value, per #1263.
        let existing = serde_json::json!({
            "api": {"githubAppExistingSecret": "s", "githubAppExistingSecretKey": "app-pem"}
        });
        let refusal = guard_byo_key_conflict(&opts(false), Some(&existing));
        let err = refusal.expect_err("a BYO release must refuse --private-key");
        let both = format!("{}\n{}", err, fix_of(&err));
        assert!(
            both.contains("app-pem"),
            "the refusal must name the release's own data key: {both}"
        );
        assert!(
            !both.contains("privateKey"),
            "the refusal named the chart default over the real key: {both}"
        );
    }

    #[test]
    fn the_guard_does_not_fire_on_an_explicit_byo_connect() {
        // Re-running `--existing-secret` on a BYO release IS the supported
        // rotation path: the operator updated the Secret and needs the rollout
        // this verb performs. A guard that refuses here leaves them with no
        // CLI way to roll the API at all.
        let existing = serde_json::json!({"api": {"githubAppExistingSecret": "my-github-app"}});
        let outcome = guard_byo_key_conflict(&byo_opts(), Some(&existing));
        assert!(
            outcome.is_ok(),
            "re-pointing at the same Secret is the rotation path: {:?}",
            outcome.err()
        );
    }

    #[test]
    fn the_guard_does_not_fire_on_disconnect() {
        // Clearing a reference must always be possible. A guard that refuses
        // `--disconnect` on a BYO release makes that release unrecoverable
        // through the CLI -- the operator would have to hand-run helm, which
        // is the thing this verb exists to avoid.
        let existing = serde_json::json!({"api": {"githubAppExistingSecret": "my-github-app"}});
        let outcome = guard_byo_key_conflict(&opts(true), Some(&existing));
        assert!(
            outcome.is_ok(),
            "a disconnect must never be blocked by what it clears: {:?}",
            outcome.err()
        );
    }

    #[test]
    fn only_a_chart_held_connect_pays_for_the_values_read() {
        // `needs_byo_conflict_check` decides whether the verb makes a `helm
        // get values` round trip at all. Answering `true` for a disconnect or
        // an explicit BYO connect adds a cluster read -- and on a real run a
        // hard failure when helm is unreachable -- to two paths that need
        // nothing from the release. Answering `false` for a chart-held connect
        // disables the guard entirely and restores the bug.
        assert!(
            needs_byo_conflict_check(&opts(false)),
            "the chart-held connect is the only path that can misreport"
        );
        assert!(
            !needs_byo_conflict_check(&opts(true)),
            "a disconnect must not pay for a values read"
        );
        assert!(
            !needs_byo_conflict_check(&byo_opts()),
            "an explicit BYO connect must not pay for a values read"
        );
    }

    #[test]
    fn the_configured_secret_is_read_with_the_charts_own_key_names() {
        // The three literals must be the exact strings
        // charts/curie/templates/api.yaml reads. A CLI that looked up a
        // different values key would resolve a different Secret than the
        // workload's own env and report a plausible but wrong answer, which is
        // worse than reporting none (#1759).
        let custom = serde_json::json!({
            "api": {"githubAppExistingSecret": "s", "githubAppExistingSecretKey": "app-pem"}
        });
        assert_eq!(
            configured_existing_secret(Some(&custom)),
            Some(("s".to_string(), "app-pem".to_string()))
        );
        let defaulted = serde_json::json!({"api": {"githubAppExistingSecret": "s"}});
        assert_eq!(
            configured_existing_secret(Some(&defaulted)),
            Some(("s".to_string(), DEFAULT_APP_KEY_DATA_KEY.to_string())),
            "an unset data key must fall back to the chart's own default"
        );
        assert_eq!(configured_existing_secret(None), None);
    }

    #[test]
    fn the_default_data_key_mirrors_the_chart_default() {
        // DEFAULT_APP_KEY_DATA_KEY mirrors charts/curie/values.yaml's
        // api.githubAppExistingSecretKey. If the two drift, `--existing-secret
        // X` with no `--existing-secret-key` writes a key name the chart never
        // defaults to, and the api pod fails to start on a Secret that is
        // perfectly correct.
        assert_eq!(DEFAULT_APP_KEY_DATA_KEY, "privateKey");
    }

    // ---- T11: input validation ---------------------------------------------

    #[test]
    fn missing_inputs_say_where_to_find_them() {
        let mut o = opts(false);
        o.app_id = String::new();
        let err = require_connect_inputs(&o).unwrap_err();
        assert!(err.to_string().contains("Developer settings"));

        let mut o = opts(false);
        o.private_key_path = String::new();
        let err = require_connect_inputs(&o).unwrap_err();
        assert!(err.to_string().contains("Private keys"));
    }

    #[test]
    fn a_key_path_that_does_not_exist_fails_before_helm_runs() {
        // helm's own error for a missing --set-file is opaque, and by then the
        // upgrade has already started.
        let mut o = opts(false);
        o.private_key_path = "/nope/missing.pem".into();
        let err = require_connect_inputs(&o).unwrap_err();
        assert!(err.to_string().contains("no such file"));
    }

    /// Both halves of the #1261 contract for one refusal, checked through the
    /// very `exit::classify` and `exit::error_json` the `--json` error emitter
    /// uses: it classifies as Usage (exit 2), and it carries a non-null `fix`
    /// naming the flag to correct. A revert to `bail!` classifies as
    /// (Failure, None), so the class assertion fails and so does the `expect`.
    fn assert_usage_with_a_fix_naming(err: &anyhow::Error, flag: &str, case: &str) {
        let (class, fix) = crate::exit::classify(err);
        assert_eq!(
            class,
            crate::exit::ExitClass::Usage,
            "a deterministic input error must exit 2 ({case}): {err:#}"
        );
        let fix = fix.expect("a usage refusal must carry a fix, not a null one");
        assert!(!fix.trim().is_empty(), "the fix must not be empty ({case})");
        assert!(
            fix.contains(flag),
            "the fix must name the offending flag {flag} ({case}): {fix}"
        );
        let json = crate::exit::error_json(err);
        assert!(
            json["fix"] != serde_json::Value::Null,
            "the rendered payload must not have a null fix ({case}): {json}"
        );
    }

    // Each of the three arms is a deterministic input error: the same argv fails
    // identically, so it is exit 2 and must name the flag to fix (#1261).
    #[test]
    fn each_missing_input_is_a_usage_error_with_a_flag_naming_fix() {
        for (app_id, key_path, flag) in [
            ("", "/tmp/app.pem", "--app-id"),
            ("1", "", "--private-key"),
            ("1", "/nope/missing.pem", "--private-key"),
        ] {
            let mut o = opts(false);
            o.app_id = app_id.into();
            o.private_key_path = key_path.into();
            let err = require_connect_inputs(&o).unwrap_err();
            assert_usage_with_a_fix_naming(
                &err,
                flag,
                &format!("--app-id {app_id:?}, --private-key {key_path:?}"),
            );
        }
    }

    // The five refusals #1255 adds are the same category as the three above:
    // argv-only input errors where the identical argv fails identically. Class
    // them as the retryable exit 1 and the agent driving the CLI is told to
    // retry a command that can never succeed (#1261). The two refusals that
    // stay `failure` -- `guard_byo_key_conflict` and the non-string field --
    // are deliberately absent: they judge the deployed release, not the argv.
    #[test]
    fn each_new_flag_refusal_is_a_usage_error_with_a_flag_naming_fix() {
        let (_dir, path) = key_fixture();

        let mut disconnect_and_byo = opts(true);
        disconnect_and_byo.existing_secret = "my-github-app".into();

        let mut injected_name = byo_opts();
        injected_name.existing_secret = "my-github-app,api.githubAppId=999".into();

        let mut injected_data_key = byo_opts();
        injected_data_key.existing_secret_key = "privateKey,api.githubAppExistingSecret=".into();

        let mut both_key_sources = byo_opts();
        both_key_sources.private_key_path = path;

        let mut orphan_data_key = opts(false);
        orphan_data_key.existing_secret_key = "app-pem".into();

        for (case, o, flag) in [
            (
                "--disconnect with a Secret name",
                disconnect_and_byo,
                "--existing-secret",
            ),
            (
                "a Secret name that is not an RFC-1123 subdomain",
                injected_name,
                "--existing-secret",
            ),
            (
                "a data key that is not a Secret data key",
                injected_data_key,
                "--existing-secret-key",
            ),
            (
                "both ways of supplying the key at once",
                both_key_sources,
                "--private-key",
            ),
            (
                "a data key with no Secret name",
                orphan_data_key,
                "--existing-secret",
            ),
        ] {
            let err = require_connect_inputs(&o).expect_err(&format!("{case} must be refused"));
            assert_usage_with_a_fix_naming(&err, flag, case);
        }
    }

    #[test]
    fn disconnect_needs_no_inputs() {
        let mut o = opts(true);
        o.app_id = String::new();
        o.private_key_path = String::new();
        assert!(require_connect_inputs(&o).is_ok());
    }

    #[test]
    fn existing_secret_with_a_private_key_is_refused() {
        // Two mutually exclusive ways to supply one key. Picking one silently
        // is a guess about operator intent on the single credential that can
        // mint read tokens for every repository in the installation -- and
        // whichever we guessed, the other would look configured and not be.
        let (_dir, path) = key_fixture();
        let mut o = byo_opts();
        o.private_key_path = path;
        let err = require_connect_inputs(&o).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--existing-secret"),
            "the refusal must name the flag that was accepted: {msg}"
        );
        assert!(
            msg.contains("--private-key"),
            "the refusal must name the flag that was ignored: {msg}"
        );
    }

    #[test]
    fn existing_secret_with_disconnect_is_refused() {
        // "--disconnect --existing-secret X" reads as "point at X while
        // disconnecting". Accepting it clears the release and leaves the
        // operator believing a reference to X was set.
        let mut o = opts(true);
        o.existing_secret = "my-github-app".into();
        let err = require_connect_inputs(&o).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--existing-secret"),
            "the refusal must name the contradicting flag: {msg}"
        );
        assert!(
            msg.contains("--disconnect"),
            "the refusal must name what it contradicts: {msg}"
        );
    }

    #[test]
    fn a_data_key_without_a_secret_name_is_refused() {
        // A non-default data key with no Secret name configures nothing at
        // all: the BYO branch never runs, and the operator who typed
        // `--existing-secret-key app-pem` gets a chart-held connect reported
        // as success. Silently doing nothing is this ticket's own defect
        // class, so it must not be reintroduced by the new flag pair.
        let mut o = opts(false);
        o.existing_secret_key = "app-pem".into();
        let outcome = require_connect_inputs(&o);
        assert!(
            outcome.is_err(),
            "a data key with no Secret name configures nothing"
        );

        // The DEFAULT key with no Secret name is the ordinary chart-held run
        // and must stay accepted, or every existing invocation breaks.
        let (_dir, path) = key_fixture();
        let mut o = opts(false);
        o.private_key_path = path;
        let outcome = require_connect_inputs(&o);
        assert!(
            outcome.is_ok(),
            "the chart-held default must stay accepted: {:?}",
            outcome.err()
        );
    }

    #[test]
    fn the_app_id_is_still_required_on_the_byo_path() {
        // The chart needs BOTH githubAppId and a key; the App id is not secret
        // and "set both, or neither" is the existing contract. Without the id
        // the JWT carries no `iss` and every GitHub call 401s -- with a
        // perfectly configured Secret sitting right there.
        let mut o = byo_opts();
        o.app_id = String::new();
        let err = require_connect_inputs(&o).unwrap_err();
        assert!(err.to_string().contains("Developer settings"));
    }

    #[test]
    fn a_private_key_is_not_required_on_the_byo_path() {
        // Directly falsifies "we forgot to move the two chart-held checks
        // under the branch". Left where they are, the empty path trips
        // "--private-key is required" (and then the is_file check), so EVERY
        // BYO invocation dies before helm ever runs and the recommended path
        // stays unreachable from the CLI -- which is this ticket.
        let outcome = require_connect_inputs(&byo_opts());
        assert!(
            outcome.is_ok(),
            "the BYO path supplies no PEM by design: {:?}",
            outcome.err()
        );
    }

    #[test]
    fn an_empty_existing_secret_degrades_to_the_chart_held_path() {
        // `--existing-secret ""` is indistinguishable from omitting it. There
        // is no third mode: it must still require --private-key and still emit
        // --set-file, rather than take the BYO branch with an empty name and
        // write api.githubAppExistingSecret= over an operator's real one.
        let mut o = opts(false);
        o.existing_secret = String::new();
        o.private_key_path = String::new();
        let err = require_connect_inputs(&o).unwrap_err();
        assert!(err.to_string().contains("Private keys"), "{err}");

        let args = argv(&connect_commands(&opts(false), DEFAULT_CLONE_BASE)[0]);
        assert!(
            has_entry(&args, "--set-file"),
            "an empty --existing-secret must still read the PEM: {args:?}"
        );
        assert!(
            !has_entry_starting(&args, "api.githubAppExistingSecret"),
            "an empty --existing-secret took the BYO branch: {args:?}"
        );
    }

    // ---- T12: the new flags are Kubernetes syntax, not helm expressions -----

    #[test]
    fn the_comma_injection_through_the_data_key_is_refused() {
        // The literal attack from the #1255 review. `--set-string` stops helm
        // TYPING a value, but helm still splits the expression on commas
        // STRUCTURALLY: this one argv entry is read as TWO assignments, and the
        // second blanks api.githubAppExistingSecret that the entry before it
        // just set. The run then also clears the inline private key, rolls the
        // API and reports success on a release with NO usable key at all -- and
        // k8s never gets to reject an invalid name, because the injected
        // assignment blanked the field before it could ever render.
        let mut o = byo_opts();
        o.existing_secret_key = "privateKey,api.githubAppExistingSecret=".into();
        let err = require_connect_inputs(&o).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--existing-secret-key"),
            "the refusal must name the offending flag: {msg}"
        );
        assert!(
            msg.contains("[-._a-zA-Z0-9]"),
            "the refusal must say what the allowed form is: {msg}"
        );
    }

    #[test]
    fn a_comma_in_the_secret_name_is_refused() {
        // Same injection, other flag. Here the second assignment would point
        // the chart-held key at an attacker-chosen path via --set-string, so
        // the Secret NAME field is no less structural than the data key.
        let mut o = byo_opts();
        o.existing_secret = "my-github-app,api.githubAppId=999".into();
        let err = require_connect_inputs(&o).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--existing-secret"),
            "the refusal must name the offending flag: {msg}"
        );
        assert!(
            msg.contains("RFC-1123"),
            "the refusal must say what the allowed form is: {msg}"
        );
    }

    #[test]
    fn realistic_secret_names_and_data_keys_are_accepted() {
        // The other half of the validator's contract, and the more dangerous
        // half to get wrong: a validator that rejected a LEGAL Kubernetes name
        // would break real installs while looking like hardening. A dotted
        // Secret name and a data key carrying '.', '_' and '-' are both
        // ordinary, and so is the all-digit pair #1236's float coercion is
        // about -- none of them may be refused.
        for (name, key) in [
            ("curie.github-app.prod", "app.private-key_2026"),
            ("1234567", "8901234"),
            ("my-github-app", DEFAULT_APP_KEY_DATA_KEY),
        ] {
            let mut o = byo_opts();
            o.existing_secret = name.into();
            o.existing_secret_key = key.into();
            assert!(
                require_connect_inputs(&o).is_ok(),
                "a legal Secret name/key pair was refused ({name}, {key}): {:?}",
                require_connect_inputs(&o).err()
            );
            let args = argv(&connect_commands(&o, DEFAULT_CLONE_BASE)[0]);
            assert!(
                has_entry(&args, &format!("api.githubAppExistingSecret={name}")),
                "the accepted Secret name did not reach helm verbatim: {args:?}"
            );
            assert!(
                has_entry(&args, &format!("api.githubAppExistingSecretKey={key}")),
                "the accepted data key did not reach helm verbatim: {args:?}"
            );
        }
    }

    #[test]
    fn a_pem_pasted_into_existing_secret_is_refused_without_echoing_it() {
        // --existing-secret and --private-key sit next to each other in --help,
        // so the PEM lands in the wrong one eventually. It must be refused (it
        // is not a Secret name), the refusal must point at the flag that DOES
        // take a PEM, and it must not print the key material back into the
        // terminal, the shell history or the --json error payload.
        let (_dir, path) = key_fixture();
        let body = std::fs::read_to_string(&path).expect("fixture readable");
        let mut o = byo_opts();
        o.existing_secret = body.clone();
        let err = require_connect_inputs(&o).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--private-key"),
            "the refusal must name the flag that takes a PEM: {msg}"
        );
        for line in body.lines().filter(|l| !l.trim().is_empty()) {
            assert!(
                !msg.contains(line),
                "key material was echoed back in the refusal: {line}"
            );
        }
    }

    // ---- T13: the guard fails CLOSED on a non-string stored value -----------

    #[test]
    fn a_truthy_non_string_byo_field_refuses_rather_than_guessing() {
        // The guard's inputs come from `helm get values -o json`, and helm
        // stores what it was given: `--set api.githubAppExistingSecret=true`
        // lands as a bool, and an all-digit Secret name lands as a float64
        // (#1236). The chart's BYO branch is plain truthiness, so both ARE
        // reading the operator's Secret -- but a guard that reads the leaf with
        // .as_str() sees None and concludes "no BYO configured". It then writes
        // an ignored PEM, rolls the API and reports success over the OLD key,
        // which is #1255 itself.
        for value in [
            serde_json::json!(true),
            serde_json::json!(42),
            serde_json::json!(1234567.0),
        ] {
            let existing = serde_json::json!({"api": {"githubAppExistingSecret": value}});
            let err = guard_byo_key_conflict(&opts(false), Some(&existing))
                .expect_err("a truthy non-string BYO field must refuse");
            let msg = err.to_string();
            assert!(
                msg.contains("api.githubAppExistingSecret"),
                "the refusal must name the field the operator has to fix: {msg}"
            );
            let fix = fix_of(&err);
            assert!(
                fix.contains("--set-string"),
                "the fix must name the way to make it a string: {fix}"
            );
            assert!(
                fix.contains("--disconnect"),
                "the fix must name the way back: {fix}"
            );
        }
    }

    #[test]
    fn a_falsy_byo_field_does_not_refuse() {
        // The mirror obligation, and the one that keeps the fail-closed rule
        // from bricking a legitimate rotation. `false`, `0` and `""` are all
        // FALSY to the chart's `{{- if .Values.api.githubAppExistingSecret }}`,
        // so the release genuinely is on the chart-held key and --private-key
        // is exactly the right command. Refusing here would leave an operator
        // with no CLI way to rotate at all.
        for value in [
            serde_json::json!(false),
            serde_json::json!(0),
            serde_json::json!(0.0),
            serde_json::json!(""),
            serde_json::Value::Null,
        ] {
            let existing = serde_json::json!({"api": {"githubAppExistingSecret": value}});
            let outcome = guard_byo_key_conflict(&opts(false), Some(&existing));
            assert!(
                outcome.is_ok(),
                "a value the chart treats as falsy is not a BYO release: {:?}",
                outcome.err()
            );
        }
    }

    #[test]
    fn a_string_byo_field_still_resolves_exactly_as_before() {
        // Fail-closed must not disturb the ordinary path: a non-empty string
        // still delegates to resolve_existing_secret_ref, and the refusal still
        // names the real Secret and the real data key rather than the generic
        // non-string message.
        let existing = serde_json::json!({
            "api": {
                "githubAppExistingSecret": "my-github-app",
                "githubAppExistingSecretKey": "app-pem"
            }
        });
        assert_eq!(
            classify_existing_secret_field(Some(&existing)),
            ByoSecretField::Named
        );
        assert_eq!(
            configured_existing_secret(Some(&existing)),
            Some(("my-github-app".to_string(), "app-pem".to_string()))
        );
        let err = guard_byo_key_conflict(&opts(false), Some(&existing))
            .expect_err("a BYO release must refuse --private-key");
        let msg = err.to_string();
        assert!(
            msg.contains("my-github-app") && msg.contains("app-pem"),
            "the string path must still name the Secret and its data key: {msg}"
        );
    }

    // ---- T14: --dry-run is offline --------------------------------------

    #[test]
    fn a_dry_run_never_reads_the_release() {
        // cli/CLAUDE.md: pure argv builders never fetch, and --dry-run never
        // touches the network. A best-effort `helm get values` under --dry-run
        // also degrades silently, so automation cannot tell a conflict-checked
        // plan from one whose check was skipped because the read failed --
        // false assurance, which is worse than none. The guard still runs on
        // the real invocation, before any mutation.
        //
        // `opts(_)` is already a dry run; the real-run cases flip the flag.
        assert!(
            !needs_release_read(&opts(false)),
            "a dry run must make no cluster read at all"
        );
        let mut real = opts(false);
        real.common.dry_run = false;
        assert!(
            needs_release_read(&real),
            "a real chart-held connect is the one path that must read the release"
        );
        let mut real_byo = byo_opts();
        real_byo.common.dry_run = false;
        assert!(
            !needs_release_read(&real_byo),
            "an explicit BYO connect needs nothing from the release"
        );
        let mut real_disconnect = opts(true);
        real_disconnect.common.dry_run = false;
        assert!(
            !needs_release_read(&real_disconnect),
            "a disconnect needs nothing from the release"
        );
        // What a dry run therefore hands the guard, and the guard's answer to
        // it: no read, no refusal, an offline plan.
        assert!(
            guard_byo_key_conflict(&opts(false), None).is_ok(),
            "a dry run must still produce a plan with no release knowledge"
        );
    }

    // ---- T15: --app-id is validated, and emitted trimmed (#1260) -----------

    /// A chart-held connect with a real PEM on disk and the given App id, so an
    /// `--app-id` case is judged by the `--app-id` rules and never trips the
    /// key checks on the way there.
    fn app_id_opts(app_id: &str) -> (tempfile::TempDir, GithubAppOpts) {
        let (dir, path) = key_fixture();
        let mut o = opts(false);
        o.app_id = app_id.into();
        o.private_key_path = path;
        (dir, o)
    }

    #[test]
    fn a_padded_app_id_is_accepted_and_reaches_helm_trimmed() {
        // AC1. `--app-id ' 1234567 '` is what a paste out of the settings page
        // produces. helm stores the surrounding whitespace verbatim, the api
        // pod signs a JWT whose `iss` is " 1234567 ", and GitHub answers 401 on
        // every call -- while `helm get values` prints something that reads as
        // correct. Asserted as the WHOLE argv entry, never a `contains` on the
        // joined line: `contains("api.githubAppId=1234567")` is also satisfied
        // by `api.githubAppId= 1234567 ` (#1263).
        let (_dir, o) = app_id_opts(" 1234567 ");
        assert!(
            require_connect_inputs(&o).is_ok(),
            "a padded id is a valid id: {:?}",
            require_connect_inputs(&o).err()
        );
        let args = argv(&connect_commands(&o, DEFAULT_CLONE_BASE)[0]);
        assert!(
            has_entry(&args, "api.githubAppId=1234567"),
            "the padded id reached helm unnormalised: {args:?}"
        );
        assert_eq!(
            flag_before(&args, "api.githubAppId=1234567"),
            "--set-string",
            "the App id must not be helm-typed: {args:?}"
        );
    }

    #[test]
    fn an_app_id_above_two_to_the_fifty_third_round_trips_exactly() {
        // AC4, and the red-on-revert guard for #1236's fix on the CLI path.
        // 9007199254740993 is 2^53 + 1: any hop through an f64 -- helm's `--set`
        // typing, or a `parse::<f64>()` in a validator written here -- renders
        // it as 9007199254740992, and the JWT's `iss` names an App that does not
        // exist. Validating the CHARSET rather than parsing is what keeps this
        // exact, and a `u64` parse would merely move the ceiling.
        let (_dir, o) = app_id_opts("9007199254740993");
        assert!(
            require_connect_inputs(&o).is_ok(),
            "an id above 2^53 is still an id: {:?}",
            require_connect_inputs(&o).err()
        );
        let args = argv(&connect_commands(&o, DEFAULT_CLONE_BASE)[0]);
        assert!(
            has_entry(&args, "api.githubAppId=9007199254740993"),
            "an id above 2^53 did not round-trip exactly: {args:?}"
        );
        assert_eq!(
            flag_before(&args, "api.githubAppId=9007199254740993"),
            "--set-string",
            "the App id must not be helm-typed: {args:?}"
        );
    }

    #[test]
    fn a_malformed_app_id_is_a_usage_error_naming_the_flag() {
        // AC2. Every one of these exited 0 and reported "GitHub App configured"
        // before #1260: a non-numeric id renders into the release, the api pod
        // mints a JWT with a nonsense `iss`, and every GitHub call 401s with
        // nothing the CLI printed to explain it.
        for (case, app_id) in [
            ("a non-numeric id", "abc"),
            ("a zero-padded id", "0001234"),
            ("an id with an interior space", "12 34"),
            (
                "a comma-injected clone base",
                "1,api.githubCloneBase=https://evil.example.com",
            ),
            ("a comma-injected PAT", "123,api.githubToken=INJECTED-PAT"),
            ("zero", "0"),
            ("a negative id", "-5"),
            ("a decimal id", "1.0"),
            ("an empty id", ""),
            ("a whitespace-only id", "   "),
        ] {
            let (_dir, o) = app_id_opts(app_id);
            let err = require_connect_inputs(&o)
                .expect_err(&format!("{case} ({app_id:?}) must be refused"));
            assert_usage_with_a_fix_naming(&err, "--app-id", case);
        }
    }

    #[test]
    fn a_comma_injected_app_id_is_refused_by_the_gate_before_any_argv_is_built() {
        // The negative control for the two comma cases above, and the reason
        // the charset is positive rather than a blocklist. `--set-string` stops
        // helm TYPING a value but helm still splits the expression on commas
        // STRUCTURALLY, so one argv entry is read as TWO assignments: the
        // second silently re-points every clone at a host the operator never
        // named, or writes an attacker-supplied PAT into the release.
        //
        // `connect_commands` is a pure argv builder with no validation and no
        // `Result`; it is only ever reached after `require_connect_inputs` has
        // already refused, so this layer can only prove the refusal happens
        // and names the right flag -- not the builder's argv shape. The
        // ordering half of the guarantee -- that no plan is built or printed
        // before the refusal -- is proven end to end, against the real binary,
        // by `a_comma_injected_app_id_is_refused_with_an_actionable_fix_and_no_plan`
        // in cli/tests/github_app_input_validation.rs. Do not re-add a
        // `connect_commands` assertion here; add to that test instead.
        for (case, injected) in [
            (
                "a comma-injected clone base",
                "1,api.githubCloneBase=https://evil.example.com",
            ),
            ("a comma-injected PAT", "123,api.githubToken=INJECTED-PAT"),
        ] {
            let (_dir, o) = app_id_opts(injected);
            let err = require_connect_inputs(&o)
                .expect_err(&format!("the injected id was accepted: {injected}"));
            assert_usage_with_a_fix_naming(&err, "--app-id", case);
        }
    }

    // ---- T16: --private-key is a readable, non-empty PEM (#1260) -----------

    #[test]
    fn a_private_key_that_is_not_a_usable_pem_is_a_usage_error_naming_the_flag() {
        // AC3. Each of these passed the old `is_file` check (or, for the
        // directory, was misreported as "no such file" about something that
        // exists). The 0-byte case is the worst: helm renders
        // githubAppPrivateKey: "", the platform's `is_configured` answers False
        // and silently falls back to api.githubToken, so the App is not in use
        // at all -- while this command printed "GitHub App configured" and
        // rolled the API to prove it.
        //
        // No assertion here reads the files' CONTENTS, and none may: the day a
        // shape check misfires on a real PEM, an assertion on contents is a copy
        // of the credential in the test output.
        let dir = tempfile::tempdir().expect("tempdir");

        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "").expect("write empty");

        let not_pem = dir.path().join("notes.txt");
        std::fs::write(&not_pem, "the app id is 1234567\n").expect("write non-pem");

        for (case, path) in [
            ("a directory", dir.path().to_path_buf()),
            ("a 0-byte file", empty),
            ("a file that is not PEM-shaped", not_pem),
        ] {
            let mut o = opts(false);
            o.private_key_path = path.to_string_lossy().into_owned();
            let err = require_connect_inputs(&o).expect_err(&format!("{case} must be refused"));
            assert_usage_with_a_fix_naming(&err, "--private-key", case);
        }
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_private_key_is_a_usage_error_naming_the_flag() {
        // `--set-file` on an unreadable path fails DURING the helm upgrade,
        // after the release has begun changing, with an opaque message. Split
        // out from its siblings because it is the one case root cannot observe:
        // root reads a 0600-cleared file regardless of its mode, so the check
        // would look broken rather than skipped. Skipping beats asserting a
        // property the environment cannot have.
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("locked.pem");
        std::fs::write(&path, format!("{}\n", pem_marker("BEGIN"))).expect("write");
        let mut perms = std::fs::metadata(&path).expect("stat").permissions();
        perms.set_mode(0o000);
        std::fs::set_permissions(&path, perms).expect("chmod 000");
        // Root reads a 0o000 file regardless of its mode, and so does a
        // filesystem mounted without permission enforcement. Probe the
        // ENVIRONMENT's capability rather than the CLI's behaviour, and skip
        // when the case is unobservable -- asserting a property the box cannot
        // have would make this a flake, and asserting the opposite would make
        // it vacuous. The probe is not the assertion; the refusal below is.
        if std::fs::read(&path).is_ok() {
            return;
        }

        let mut o = opts(false);
        o.private_key_path = path.to_string_lossy().into_owned();
        let err = require_connect_inputs(&o).expect_err("an unreadable key must be refused");
        assert_usage_with_a_fix_naming(&err, "--private-key", "a chmod 000 file");
    }

    #[test]
    fn mismatched_begin_and_end_labels_are_refused() {
        // The core of this fix. Each marker line passes on its own -- both
        // name SOME private key -- but they name DIFFERENT blocks, which is
        // what two truncated/concatenated halves of different keys look like.
        // A pre-fix `is_pem_private_key` checked each marker independently
        // and accepted this.
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("mismatched.pem");
        let body = format!(
            "{}\nMIIEowIBAAKCAQEAtestkeymaterial\n{}\n",
            pem_marker_labeled("BEGIN", "RSA PRIVATE KEY"),
            pem_marker_labeled("END", "PRIVATE KEY"),
        );
        std::fs::write(&path, body).expect("write mismatched pem");

        let mut o = opts(false);
        o.private_key_path = path.to_string_lossy().into_owned();
        let err =
            require_connect_inputs(&o).expect_err("mismatched BEGIN/END labels must be refused");
        assert_usage_with_a_fix_naming(&err, "--private-key", "mismatched BEGIN/END labels");
    }

    #[test]
    fn every_real_private_key_label_shape_is_still_accepted() {
        // The "do not narrow" half of the contract. RSA is what GitHub Apps
        // issue today, but PKCS#8, encrypted PKCS#8, and EC keys are all real
        // downloads an operator might have, and CRLF is what a Windows editor
        // or a Windows `Send-MailMessage` attachment produces. Any of these
        // going red is a future "tightened" allowlist rejecting a real key,
        // not a caught bug.
        let dir = tempfile::tempdir().expect("tempdir");

        for (case, label, crlf) in [
            ("PKCS#8 PRIVATE KEY", "PRIVATE KEY", false),
            ("ENCRYPTED PRIVATE KEY", "ENCRYPTED PRIVATE KEY", false),
            ("EC PRIVATE KEY", "EC PRIVATE KEY", false),
            ("CRLF line endings", "RSA PRIVATE KEY", true),
        ] {
            let mut body = format!(
                "{}\nMIIEowIBAAKCAQEAtestkeymaterial\n{}\n",
                pem_marker_labeled("BEGIN", label),
                pem_marker_labeled("END", label),
            );
            if crlf {
                body = body.replace('\n', "\r\n");
            }
            let path = dir.path().join(format!("{label}.pem").replace(' ', "_"));
            std::fs::write(&path, body).expect("write fixture");

            let mut o = opts(false);
            o.private_key_path = path.to_string_lossy().into_owned();
            assert!(
                require_connect_inputs(&o).is_ok(),
                "a real PEM shape ({case}) was refused: {:?}",
                require_connect_inputs(&o).err()
            );
        }
    }

    #[test]
    fn a_valid_pem_and_a_byo_connect_are_both_still_accepted() {
        // The other half of the validator's contract, and the more dangerous
        // half to get wrong: a check that refused a LEGAL input would break
        // every existing install while looking like hardening. The BYO case is
        // the sharper one -- it supplies no PEM by design, so the new arms must
        // not merely accept it but must never stat a path at all.
        let (_dir, path) = key_fixture();
        let mut chart_held = opts(false);
        chart_held.private_key_path = path;
        assert!(
            require_connect_inputs(&chart_held).is_ok(),
            "a real PEM was refused: {:?}",
            require_connect_inputs(&chart_held).err()
        );

        let mut byo = byo_opts();
        // A path that cannot exist. If any new arm stats it, this refuses --
        // and the recommended BYO path becomes unreachable from the CLI.
        byo.private_key_path = String::new();
        assert!(
            require_connect_inputs(&byo).is_ok(),
            "the BYO path must never be asked for a PEM: {:?}",
            require_connect_inputs(&byo).err()
        );
    }
}
