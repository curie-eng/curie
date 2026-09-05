//! `curie cluster up | cluster status | cluster rollback | cluster down`: the operator
//! day-1 lifecycle, wrapping the Helm chart and `kubectl` the way linkerd or
//! cilium wrap theirs -- a deliberately thin CLI over the chart, which stays the
//! source of truth. Every verb shells out to the `helm`/`kubectl` binaries; the
//! CLI never re-derives what a values file already declares.
//!
//! Each verb builds its command lines as a pure function returning
//! [`OpsCommand`] vectors; the executor (or the `--dry-run` printer) consumes
//! them. That split keeps the argv construction unit-testable with no cluster
//! and gives one place to mask secrets before anything is printed.

use std::collections::{BTreeMap, BTreeSet};
use std::process::Stdio;
#[cfg(unix)]
use std::sync::{LazyLock, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::process::{Child, ChildStdout, Command};

mod convergence;

/// One external command: the program plus its argument vector, with secret
/// argument values tagged so they can be masked in any printed form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpsCommand {
    pub program: String,
    pub args: Vec<CmdArg>,
    pub env: Vec<(String, String)>,
    pub secret_env: Vec<(String, String)>,
}

/// A single argv token.
///
/// `HelmSetExpression` preserves a complete `--set` or `--set-string`
/// expression for execution while masking credential shaped values in every
/// rendered form.
///
/// `SecretSet` is a `helm --set key=value` whose value is a credential: the real
/// value is used for execution, but only a masked prefix is ever printed (dry-run
/// or the echoed command line). Note the value still lands in the process argv --
/// acceptable only for low-sensitivity tokens that already live in a k8s Secret.
///
/// `SecretValuesFile` carries one or more secret `helm` values (dotted key ->
/// value) that must **never** reach the process table. Before execution it is
/// materialized into a private (0600) temporary values file and replaced by a
/// `-f <path>` pair (see [`OpsCommand::materialize_secret_files`]); the file is
/// removed as soon as the command finishes. This keeps the secret off `ps -ef`
/// and out of `/proc/<pid>/cmdline`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CmdArg {
    Plain(String),
    HelmSetExpression(String),
    SecretSet { key: String, value: String },
    SecretValuesFile(Vec<(String, String)>),
    PrivateJsonValuesFile(PrivateHelmValues),
}

impl CmdArg {
    /// The real argv token(s) passed to the process. Most args map to a single
    /// token; `SecretValuesFile` is expected to have been replaced by a `-f
    /// <path>` pair during materialization, so reaching it here (an unmaterialized
    /// secret file about to be executed) is a bug -- we emit nothing rather than
    /// risk leaking, and trip a debug assertion.
    fn value_tokens(&self) -> Vec<String> {
        match self {
            CmdArg::Plain(s) => vec![s.clone()],
            CmdArg::HelmSetExpression(expression) => vec![expression.clone()],
            CmdArg::SecretSet { key, value } => vec![format!("{key}={value}")],
            CmdArg::SecretValuesFile(_) | CmdArg::PrivateJsonValuesFile(_) => {
                debug_assert!(
                    false,
                    "SecretValuesFile must be materialized before argv(); \
                     call OpsCommand::materialize_secret_files first"
                );
                Vec::new()
            }
        }
    }

    /// The token(s) as shown to a human: secret values are masked. A
    /// `SecretValuesFile` prints as `-f <secret values file: key=masked, ...>` so
    /// the operator can see which values are applied without any secret leaking.
    fn masked_tokens(&self) -> Vec<String> {
        match self {
            CmdArg::Plain(s) => vec![s.clone()],
            CmdArg::HelmSetExpression(expression) => {
                vec![mask_helm_set_expression(expression)]
            }
            CmdArg::SecretSet { key, value } => vec![format!("{key}={}", mask_secret(value))],
            CmdArg::PrivateJsonValuesFile(values) => vec![
                "-f".to_string(),
                format!(
                    "<private retained mail values: {}>",
                    values.keys().join(", ")
                ),
            ],
            CmdArg::SecretValuesFile(pairs) => {
                let masked: Vec<String> = pairs
                    .iter()
                    .map(|(key, value)| format!("{key}={}", mask_secret(value)))
                    .collect();
                vec![
                    "-f".to_string(),
                    format!("<secret values file: {}>", masked.join(", ")),
                ]
            }
        }
    }
}

impl OpsCommand {
    pub(crate) fn new(program: &str, args: Vec<CmdArg>) -> Self {
        Self {
            program: program.to_string(),
            args,
            env: Vec::new(),
            secret_env: Vec::new(),
        }
    }

    pub fn with_env(mut self, env: Vec<(String, String)>) -> Self {
        self.env = env;
        self
    }

    pub fn with_secret_env(mut self, secret_env: Vec<(String, String)>) -> Self {
        self.secret_env = secret_env;
        self
    }

    /// The argv tail (real values) handed to `tokio::process::Command`. Call
    /// [`materialize_secret_files`](Self::materialize_secret_files) first when the
    /// command may carry a [`CmdArg::SecretValuesFile`], otherwise those secret
    /// values are dropped rather than executed.
    pub fn argv(&self) -> Vec<String> {
        self.args.iter().flat_map(CmdArg::value_tokens).collect()
    }

    /// The full shell-quoted command line with secrets masked, one line as it
    /// would be typed into a shell.
    pub fn display(&self) -> String {
        let mut env: Vec<String> = self
            .env
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .chain(
                self.secret_env
                    .iter()
                    .map(|(key, value)| format!("{key}={}", mask_secret(value))),
            )
            .collect();
        env.sort();
        let mut parts: Vec<String> = env.iter().map(|item| shell_quote(item)).collect();
        parts.push(shell_quote(&self.program));
        for a in &self.args {
            for token in a.masked_tokens() {
                parts.push(shell_quote(&token));
            }
        }
        parts.join(" ")
    }

    /// Materialize every [`CmdArg::SecretValuesFile`] into a private (0600)
    /// temporary values file and return an equivalent command whose secrets are
    /// delivered via `helm -f <path>` instead of the argv, plus RAII guards that
    /// delete any remaining files when dropped. The signal cleanup handler also
    /// removes registered files on SIGINT or SIGTERM. Commands without a secret
    /// values file are returned unchanged with no guards. Hold the returned guards
    /// until the process has finished.
    pub(crate) fn materialize_secret_files(
        &self,
    ) -> Result<(OpsCommand, Vec<SecretValuesFileGuard>)> {
        let mut new_args = Vec::with_capacity(self.args.len());
        let mut guards = Vec::new();
        for a in &self.args {
            match a {
                CmdArg::SecretValuesFile(pairs) => {
                    let guard = SecretValuesFileGuard::write(pairs)?;
                    new_args.push(plain("-f"));
                    new_args.push(plain(guard.path.to_string_lossy().into_owned()));
                    guards.push(guard);
                }
                CmdArg::PrivateJsonValuesFile(values) => {
                    let guard = SecretValuesFileGuard::write_document(&values.0)?;
                    new_args.push(plain("-f"));
                    new_args.push(plain(guard.path.to_string_lossy().into_owned()));
                    guards.push(guard);
                }
                other => new_args.push(other.clone()),
            }
        }
        Ok((
            OpsCommand {
                program: self.program.clone(),
                args: new_args,
                env: self.env.clone(),
                secret_env: self.secret_env.clone(),
            },
            guards,
        ))
    }
}

#[cfg(unix)]
#[derive(Default)]
struct SecretFileRegistry {
    terminating: bool,
    paths: BTreeSet<std::path::PathBuf>,
}

#[cfg(unix)]
static SECRET_FILE_REGISTRY: LazyLock<Mutex<SecretFileRegistry>> =
    LazyLock::new(|| Mutex::new(SecretFileRegistry::default()));

#[cfg(unix)]
static SECRET_SIGNAL_INSTALLATION: OnceLock<std::result::Result<(), String>> = OnceLock::new();

#[cfg(unix)]
fn lock_secret_file_registry() -> MutexGuard<'static, SecretFileRegistry> {
    SECRET_FILE_REGISTRY
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(unix)]
fn ensure_secret_signal_cleanup() -> Result<()> {
    match SECRET_SIGNAL_INSTALLATION
        .get_or_init(|| install_secret_signal_cleanup().map_err(|error| error.to_string()))
    {
        Ok(()) => Ok(()),
        Err(error) => bail!("installing secret values signal cleanup: {error}"),
    }
}

#[cfg(not(unix))]
fn ensure_secret_signal_cleanup() -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn install_secret_signal_cleanup() -> std::io::Result<()> {
    let mut signals = signal_hook::iterator::Signals::new([
        signal_hook::consts::signal::SIGINT,
        signal_hook::consts::signal::SIGTERM,
    ])?;
    std::thread::Builder::new()
        .name("curie-secret-cleanup".to_string())
        .spawn(move || {
            if let Some(signal) = signals.forever().next() {
                terminate_after_secret_cleanup(signal);
            }
        })?;
    Ok(())
}

#[cfg(unix)]
fn terminate_after_secret_cleanup(signal: i32) -> ! {
    {
        let mut registry = lock_secret_file_registry();
        registry.terminating = true;
        for path in std::mem::take(&mut registry.paths) {
            let _ = std::fs::remove_file(path);
        }
    }

    test_mark_coordination("CURIE_TEST_SECRET_SIGNAL_CLEANED");
    test_wait_for_coordination("CURIE_TEST_SECRET_RESUME_SIGNAL");

    let _ = signal_hook::low_level::emulate_default_handler(signal);
    signal_hook::low_level::exit(128 + signal);
}

#[cfg(unix)]
fn park_terminating_secret_writer() -> ! {
    test_mark_coordination("CURIE_TEST_SECRET_WRITER_PARKED");
    loop {
        std::thread::park();
    }
}

#[cfg(debug_assertions)]
fn test_mark_coordination(env_name: &str) {
    if let Some(path) = std::env::var_os(env_name).map(std::path::PathBuf::from) {
        let _ = std::fs::write(path, b"ready");
    }
}

#[cfg(not(debug_assertions))]
fn test_mark_coordination(_env_name: &str) {}

#[cfg(debug_assertions)]
fn test_wait_for_coordination(env_name: &str) {
    if let Some(path) = std::env::var_os(env_name).map(std::path::PathBuf::from) {
        while !path.exists() {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }
}

#[cfg(not(debug_assertions))]
fn test_wait_for_coordination(_env_name: &str) {}

#[cfg(debug_assertions)]
fn test_pause_after_first_secret_file() {
    static PAUSED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

    if std::env::var_os("CURIE_TEST_SECRET_FIRST_FILE_WRITTEN").is_none()
        || PAUSED.swap(true, std::sync::atomic::Ordering::SeqCst)
    {
        return;
    }
    test_mark_coordination("CURIE_TEST_SECRET_FIRST_FILE_WRITTEN");
    test_wait_for_coordination("CURIE_TEST_SECRET_RESUME_WRITER");
}

#[cfg(not(debug_assertions))]
fn test_pause_after_first_secret_file() {}

/// A 0600 temporary helm values file holding secret values. Signal cleanup
/// removes it on SIGINT or SIGTERM; `Drop` removes it on normal completion or
/// error, so the secret never outlives the `helm` invocation.
pub(crate) struct SecretValuesFileGuard {
    path: std::path::PathBuf,
}

impl SecretValuesFileGuard {
    /// Write `pairs` (dotted helm keys -> secret values) into a fresh 0600 temp
    /// file as nested YAML (a JSON document, which helm parses as YAML), created
    /// with restrictive permissions atomically so the secret is never briefly
    /// world-readable.
    fn write(pairs: &[(String, String)]) -> Result<Self> {
        Self::write_document(&nest_dotted_keys(pairs))
    }

    fn write_document(doc: &serde_json::Value) -> Result<Self> {
        ensure_secret_signal_cleanup()?;
        let body = serde_json::to_vec(doc).context("serializing secret helm values")?;

        let mut path = std::env::temp_dir();
        path.push(format!("curie-helm-values-{}.yaml", uuid::Uuid::new_v4()));

        #[cfg(unix)]
        {
            let mut registry = lock_secret_file_registry();
            if registry.terminating {
                drop(registry);
                park_terminating_secret_writer();
            }
            if !registry.paths.insert(path.clone()) {
                bail!(
                    "secret helm values file path collision at {}",
                    path.display()
                );
            }
            if let Err(error) = create_secret_values_file(&path, &body) {
                let _ = std::fs::remove_file(&path);
                registry.paths.remove(&path);
                return Err(error);
            }
        }

        #[cfg(not(unix))]
        if let Err(error) = create_secret_values_file(&path, &body) {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }

        let guard = Self { path };
        test_pause_after_first_secret_file();
        Ok(guard)
    }
}

fn create_secret_values_file(path: &std::path::Path, body: &[u8]) -> Result<()> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts
        .open(path)
        .with_context(|| format!("creating secret helm values file {}", path.display()))?;
    // Belt-and-suspenders on platforms where create-time mode is not honored.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("securing secret helm values file {}", path.display()))?;
    }
    use std::io::Write;
    file.write_all(body)
        .with_context(|| format!("writing secret helm values file {}", path.display()))?;
    Ok(())
}

impl Drop for SecretValuesFileGuard {
    fn drop(&mut self) {
        // Best-effort cleanup; nothing actionable if the temp file is already gone.
        #[cfg(unix)]
        {
            let mut registry = lock_secret_file_registry();
            let _ = std::fs::remove_file(&self.path);
            registry.paths.remove(&self.path);
        }
        #[cfg(not(unix))]
        {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Expand dotted helm keys (`a.b.c=value`) into a nested JSON object suitable as
/// a helm values file. JSON is a subset of YAML, so helm parses it directly, and
/// serde handles all value escaping so a secret with YAML-special characters
/// cannot break the document.
fn nest_dotted_keys(pairs: &[(String, String)]) -> serde_json::Value {
    let mut root = serde_json::Map::new();
    for (dotted, value) in pairs {
        let parts: Vec<&str> = dotted.split('.').collect();
        let mut cursor = &mut root;
        for part in &parts[..parts.len() - 1] {
            cursor = cursor
                .entry((*part).to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()))
                .as_object_mut()
                .expect("dotted key prefix maps to an object");
        }
        cursor.insert(
            parts[parts.len() - 1].to_string(),
            serde_json::Value::String(value.clone()),
        );
    }
    serde_json::Value::Object(root)
}

pub fn plain(s: impl Into<String>) -> CmdArg {
    CmdArg::Plain(s.into())
}

pub(crate) fn secret_set(key: &str, value: &str) -> CmdArg {
    CmdArg::SecretSet {
        key: key.to_string(),
        value: value.to_string(),
    }
}

/// Mask a secret for display. Values of eight characters or fewer are fully
/// masked; longer values retain the first eight characters for recognition.
pub fn mask_secret(value: &str) -> String {
    if value.chars().nth(8).is_none() {
        return "***".to_string();
    }
    let shown: String = value.chars().take(8).collect();
    format!("{shown}***")
}

/// POSIX shell-quote a single token: leave it bare when it is composed only of
/// safe characters, otherwise wrap in single quotes (so `--set` keys carrying
/// `[0]` array indices print quoted, matching how they must be typed).
/// POSIX single-quote an argument for a copy-pasteable command line, leaving
/// unambiguously-safe tokens bare. The one canonical implementation (#497): the
/// TUI command echo (`interactive::render_command`) and the helm/kubectl argv
/// printers both call this, so an empty argument renders as `''` everywhere
/// rather than silently vanishing (the interactive copy's empty-`all()` bug).
pub(crate) fn shell_quote(s: &str) -> String {
    fn is_safe(c: char) -> bool {
        c.is_ascii_alphanumeric()
            || matches!(c, '_' | '.' | '/' | ':' | '=' | '@' | ',' | '-' | '+')
    }
    if !s.is_empty() && s.chars().all(is_safe) {
        s.to_string()
    } else {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
}

// ---------------------------------------------------------------------------
// Options structs (mirror the clap flags in main.rs)
// ---------------------------------------------------------------------------

/// Common flags every verb carries.
#[derive(Debug, Clone)]
pub struct CommonOpts {
    pub namespace: String,
    pub release: String,
    pub dry_run: bool,
}

/// Typed retained values use the same protected file lifecycle as credentials.
/// Debug and command rendering expose field names only.
#[derive(Clone, PartialEq, Eq)]
pub struct PrivateHelmValues(serde_json::Value, BTreeMap<String, String>);

impl PrivateHelmValues {
    fn keys(&self) -> Vec<String> {
        [
            "mailAdapter",
            "worker.adapterCredentials",
            "worker.adapterCredentialsExistingSecret",
            "worker.adapterCredentialsExistingSecretKey",
        ]
        .into_iter()
        .filter(|key| {
            self.0
                .pointer(&format!("/{}", key.replace('.', "/")))
                .is_some()
        })
        .map(str::to_string)
        .collect()
    }
}

impl std::fmt::Debug for PrivateHelmValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrivateHelmValues")
            .field("keys", &self.keys())
            .finish()
    }
}

pub struct UpOpts {
    /// Existing mail lifecycle and paired worker credentials, with explicit
    /// operator overrides removed. Populated by the one release-values read.
    pub retained_mail_values: Option<PrivateHelmValues>,
    pub common: CommonOpts,
    pub chart: String,
    pub no_expose: bool,
    pub set: Vec<String>,
    pub set_string: Vec<String>,
    /// Named model providers (validated against [`parse_egress_provider`]) whose
    /// API host(s) runner egress is opened to. Resolved to narrow host-route
    /// CIDRs at install time into [`resolved_egress_cidrs`]; empty means no
    /// provider egress. This is the explicit replacement for the old
    /// unconditional Anthropic carve-out (#362).
    pub allow_egress_host: Vec<String>,
    /// The single-host CIDRs the named providers resolved to, populated by [`up`]
    /// from [`resolve_provider_egress_cidrs`] (offline under `--dry-run`, so
    /// empty there). Empty in the pure argv tests. Emitted as the first
    /// `allowedEgress` entries, before any `allow_web_egress` destination.
    pub resolved_egress_cidrs: Vec<String>,
    /// Operator declared CIDRs to open runner egress to for skill or tool web
    /// access, additive to the resolved provider egress. Empty means fail closed
    /// by default.
    pub allow_web_egress: Vec<String>,
    /// Whether `--fake-model` was passed (forces the sealed install and
    /// suppresses the fake-model warning even when the env credential is set).
    pub fake_model: bool,
    /// The model credential to install with, resolved from `CURIE_CREDENTIALS`
    /// (or the deprecated `CURIE_MODEL_CREDENTIALS`). `Some(non-empty)` enables the real model;
    /// `None` installs sealed (fake model). A credential alone opens NO egress --
    /// the model stays unreachable behind the fail-closed sandbox until a
    /// provider (`allow_egress_host`) or a raw range (`allow_web_egress`) is
    /// named (#362).
    pub credentials: Option<String>,
    pub local_model: Option<String>,
    /// The shell `CURIE_MODEL` resolved by the caller (`None` when unset or
    /// empty), used to default `agentSandbox.runner.model` for cross-tier parity
    /// with `local up` (#361).
    pub model: Option<String>,
    /// Required chart secrets (dotted helm key -> value) the CLI supplies so a
    /// no-override install never ships the published dev defaults (see #196).
    /// Populated by [`up`] from [`resolve_generated_secrets`] on a sealed
    /// install, and from [`resolve_preserved_values`] on both sealed and
    /// `--dev` upgrades so a sibling verb's recorded values (`cluster comms`,
    /// `cluster github-app`, a previously generated sealing key) survive a
    /// full Helm upgrade (#1134). Empty in the pure argv tests and on a fresh
    /// `--dev` install, which keeps the chart's published defaults. Delivered
    /// through a private 0600 `-f` values file, never the argv.
    pub secrets: Vec<(String, String)>,
    /// What this run does with `api.githubToken`, resolved by [`up`] from
    /// `--github-token` / `CURIE_GITHUB_TOKEN` / `--clear-github-token` and the
    /// value the release already recorded. `Untouched` in the pure argv tests.
    /// A `Set` value is a live GitHub credential and is delivered through the
    /// private 0600 `-f` values file, never the argv.
    pub github_token: GithubTokenPlan,
    /// `--dev`: keep the chart's deterministic dev-default secrets instead of
    /// generating strong per-release randoms (the first-class dev escape hatch
    /// that replaces hand-passing `--set` for every secret).
    pub dev: bool,
}

impl UpOpts {
    fn operator_sets(&self) -> Vec<String> {
        self.set.iter().chain(&self.set_string).cloned().collect()
    }
}

pub struct DownOpts {
    pub common: CommonOpts,
    pub yes: bool,
}

pub struct RollbackOpts {
    pub common: CommonOpts,
    /// An operator-named revision. `None` lets [`select_rollback_revision`] pick
    /// the newest `deployed`/`superseded` revision below the current one, which
    /// is the whole point of the verb (#1899).
    pub revision: Option<u32>,
    /// Admit a `--revision` whose status is not `deployed`/`superseded`. Refused
    /// without this flag, since helm never finished applying such a revision.
    pub allow_failed_revision: bool,
    pub yes: bool,
}

// ---------------------------------------------------------------------------
// Command builders (pure; unit-tested below)
// ---------------------------------------------------------------------------

/// Egress port shared by every runner allowlist entry (provider + web): TLS only.
const EGRESS_TCP_PORT: u16 = 443;

/// Resolve the model credential `up` installs with. `--fake-model` forces the
/// sealed install regardless of the environment; otherwise a non-empty
/// credential value enables the real model.
pub fn resolve_up_credentials(fake_model: bool, env_value: Option<String>) -> Option<String> {
    if fake_model {
        return None;
    }
    env_value.filter(|v| !v.is_empty())
}

/// The operator's model credential from the shell for `cluster up`, canonically
/// `CURIE_CREDENTIALS` -- the same name the runtime plane (runner/worker/chart)
/// uses everywhere. The CLI's historical `CURIE_MODEL_CREDENTIALS` is accepted
/// as a deprecated alias for one release, with a warning naming the replacement,
/// so an operator who set the one name for `skill up` isn't met with a silent
/// no-op at `cluster up` (#496). Private storage is the final fallback. Returns
/// None when no source has a nonempty value.
pub fn model_credential_env() -> Result<Option<String>> {
    if let Some(value) = std::env::var("CURIE_CREDENTIALS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        return Ok(Some(value));
    }
    if let Some(value) = std::env::var("CURIE_MODEL_CREDENTIALS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        eprintln!(
            "warning: CURIE_MODEL_CREDENTIALS is deprecated and will be removed in a future \
             release; set CURIE_CREDENTIALS instead."
        );
        return Ok(Some(value));
    }
    match crate::commands::secret_store_env("CURIE_CREDENTIALS") {
        Ok(stored) => Ok(stored.map(|(_, value)| value)),
        Err(error) => {
            crate::ui::ui().warn(&format!(
                "Saved model credentials could not be read; continuing without them: {error}"
            ));
            Ok(None)
        }
    }
}

/// The helm value key that pins the sandbox runner model in the chart.
const RUNNER_MODEL_KEY: &str = "agentSandbox.runner.model";

pub(crate) const INFERENCE_PERSISTENCE_ENABLED_KEY: &str = "inference.persistence.enabled";
pub(crate) const INFERENCE_PULL_MODEL_KEY: &str = "inference.pullModel";

pub(crate) fn inference_asset_policy_is_safe(
    persistence_enabled: Option<bool>,
    pull_model: Option<bool>,
) -> bool {
    persistence_enabled == Some(true) || pull_model == Some(false)
}

/// The value of the last explicit `--set agentSandbox.runner.model=VAL` in
/// `set`, if the operator passed one (last wins, matching helm precedence).
/// Helm accepts comma-joined `--set a=1,b=2`, so each element is split on `,`
/// (mirroring `operator_set_keys`) before the prefix match — a runner model
/// pinned alongside other keys is detected, and a trailing key after the model
/// assignment is not swallowed into the value.
fn explicit_runner_model(set: &[String]) -> Option<&str> {
    let prefix = format!("{RUNNER_MODEL_KEY}=");
    // `strip_prefix` returns a slice of `part` (borrowing `set`), not of
    // `prefix`, so the returned borrow outlives the temporary `prefix`.
    set.iter()
        .flat_map(|s| s.split(','))
        .filter_map(|part| part.strip_prefix(&prefix))
        .next_back()
}

/// Fail loud when the shell `CURIE_MODEL` and an explicit
/// `--set agentSandbox.runner.model=` disagree, so the runner model is never
/// silently ambiguous (#361).
pub fn check_runner_model_conflict(model: Option<&str>, set: &[String]) -> Result<()> {
    if let (Some(y), Some(x)) = (model, explicit_runner_model(set)) {
        if x != y {
            bail!(
                "conflicting sandbox runner model: CURIE_MODEL=`{y}` but \
                 `--set {RUNNER_MODEL_KEY}={x}` was also passed. Remove one so the \
                 runner model is unambiguous."
            );
        }
    }
    Ok(())
}

/// Reject supplying the GitHub credential through BOTH the private input and the
/// argv `--set` pass-through. Silently letting `--set` win would discard the
/// operator's protected input AND leak the `--set` value into the process table,
/// which is the exact defect #1124 exists to close, so this is a usage error
/// rather than a precedence rule.
pub fn check_github_token_conflict(flag: Option<&str>, clear: bool, set: &[String]) -> Result<()> {
    let explicit = clear || flag.is_some_and(|v| !v.is_empty());
    if explicit && operator_set_keys(set).contains(GITHUB_TOKEN_KEY) {
        bail!(
            "conflicting GitHub credential: `--set {GITHUB_TOKEN_KEY}=` was passed \
             alongside `--github-token` / `--clear-github-token`. Remove the \
             `--set`: it puts the complete token in the process table and shell \
             history, which the dedicated input exists to avoid."
        );
    }
    Ok(())
}

/// Validate every input that must fail before the installer reads cluster
/// state. `curie apply` and `curie diff` call this through their shared local
/// planner, while `cluster up` keeps the same validation before its own read.
pub(crate) fn validate_up_inputs(
    opts: &UpOpts,
    github_token: Option<&str>,
    clear_github_token: bool,
) -> Result<()> {
    validate_local_model_asset_policy(opts)?;
    validate_web_egress_cidrs(&opts.allow_web_egress)
        .context("invalid --allow-web-egress value")?;
    let operator_sets = opts.operator_sets();
    check_runner_model_conflict(opts.model.as_deref(), &operator_sets)?;
    check_github_token_conflict(github_token, clear_github_token, &operator_sets)?;
    for host in &opts.allow_egress_host {
        parse_egress_provider(host)?;
    }
    Ok(())
}

fn last_typed_bool(sets: &[String], key: &str) -> Option<bool> {
    operator_set_entries(sets)
        .into_iter()
        .filter_map(|(candidate, value)| (candidate.trim() == key).then_some(value.trim()))
        .next_back()
        .and_then(|value| match value {
            "true" => Some(true),
            "false" => Some(false),
            _ => None,
        })
}

fn validate_local_model_asset_policy(opts: &UpOpts) -> Result<()> {
    if opts.local_model.is_none() {
        return Ok(());
    }
    let persistence_enabled = last_typed_bool(&opts.set, INFERENCE_PERSISTENCE_ENABLED_KEY);
    let pull_model = last_typed_bool(&opts.set, INFERENCE_PULL_MODEL_KEY);
    if inference_asset_policy_is_safe(persistence_enabled, pull_model) {
        return Ok(());
    }

    Err(crate::exit::CliError::usage(
        "`--local-model` requires an explicit model-weight policy: pass \
         `--set inference.persistence.enabled=true` to pull weights into persistent storage, \
         or pass `--set inference.pullModel=false` when the model is already present; \
         `--set-string` cannot express the required typed boolean policy",
    )
    .with_fix(
        "re-run with `--set inference.persistence.enabled=true` or \
         `--set inference.pullModel=false`",
    )
    .into())
}

/// Validate every operator-supplied `--allow-web-egress` value is a real CIDR
/// (`addr/prefix`) before it is interpolated into a `helm --set` argument. A
/// value containing a comma or `=` would otherwise be split by helm into
/// multiple `--set` assignments and could overwrite the model rule at index
/// `[0]`; requiring a parseable `IpAddr` plus an in-range prefix naturally
/// rejects those (and whitespace) because they fail to parse.
pub fn validate_web_egress_cidrs(cidrs: &[String]) -> Result<()> {
    for cidr in cidrs {
        let (addr, prefix) = cidr.split_once('/').ok_or_else(|| {
            anyhow::anyhow!("`--allow-web-egress` value `{cidr}` is not a CIDR (expected addr/prefix, e.g. 10.0.0.0/8)")
        })?;
        let ip: std::net::IpAddr = addr.parse().map_err(|_| {
            anyhow::anyhow!(
                "`--allow-web-egress` value `{cidr}` has an unparseable address `{addr}`"
            )
        })?;
        let bits: u8 = prefix.parse().map_err(|_| {
            anyhow::anyhow!(
                "`--allow-web-egress` value `{cidr}` has an unparseable prefix `{prefix}`"
            )
        })?;
        let max = if ip.is_ipv4() { 32 } else { 128 };
        if bits > max {
            bail!("`--allow-web-egress` value `{cidr}` has an out-of-range prefix `/{bits}` (max /{max})");
        }
    }
    Ok(())
}

/// A CIDR is a *default route* when its prefix length is `/0` (`0.0.0.0/0`,
/// `::/0`, or any `addr/0`) -- a `/0` prefix ignores the address bits entirely
/// and matches the whole address space. Opening runner egress to such a route
/// removes the chart's default-deny internet rail. Assumes the value already
/// passed `validate_web_egress_cidrs`.
pub fn is_default_route(cidr: &str) -> bool {
    cidr.rsplit_once('/')
        .and_then(|(_, prefix)| prefix.trim().parse::<u8>().ok())
        .is_some_and(|bits| bits == 0)
}

/// The distinct rail-removal warning to emit when the web-egress allowlist
/// contains one or more default routes, or `None` when it does not. Returned as
/// a pure value (not printed here) so the warning text stays unit-testable
/// independently of the `up` handler's UI side effects.
pub fn default_route_egress_warning(cidrs: &[String]) -> Option<String> {
    let routes: Vec<&str> = cidrs
        .iter()
        .map(String::as_str)
        .filter(|c| is_default_route(c))
        .collect();
    if routes.is_empty() {
        return None;
    }
    Some(format!(
        "`--allow-web-egress` includes a default route ({}); this removes the egress rail -- the sandbox can reach the entire internet",
        routes.join(", ")
    ))
}

/// Credential prefixes whose runtime routing selects one unambiguous provider.
/// Keep this aligned with `runner/src/curie_runner/sdk_auth.py`: credentials
/// outside these exact prefixes do not carry enough information to infer an
/// egress destination.
const CREDENTIAL_PREFIX_PROVIDERS: &[(&str, &str)] =
    &[("sk-ant-", "anthropic"), ("sk-or-", "openrouter")];

/// Return the provider unambiguously selected by a credential prefix.
///
/// Callers that inspect a credential must discard it after deriving this
/// non-secret provider name; it is safe to render the returned value but never
/// the credential itself.
pub fn provider_from_credential_prefix(credential: &str) -> Option<&'static str> {
    CREDENTIAL_PREFIX_PROVIDERS
        .iter()
        .find(|(prefix, _)| credential.starts_with(prefix))
        .map(|(_, provider)| *provider)
}

/// The canonical model providers `--allow-egress-host` accepts, each paired with
/// the API hostname(s) its runner must reach, in the order shown in help and
/// error text. The single source of truth for both the accepted-provider set and
/// their egress hosts.
///
/// This set is deliberately limited to the providers the runner can drive
/// end to end today. Opening egress to a host the runner cannot actually talk to
/// gives false confidence, so a provider is only listed once the runner has
/// runtime support for it. Native OpenAI and Gemini remain unsupported here.
///
/// HOSTNAMES, never CIDRs: provider IPs rotate, so they are resolved to narrow
/// host routes at install time (see [`resolve_provider_egress_cidrs`]) instead of
/// baked into this binary where a stale literal would silently break a real model
/// call.
const EGRESS_PROVIDERS: &[(&str, &[&str])] = &[
    ("anthropic", &["api.anthropic.com"]),
    ("openrouter", &["openrouter.ai"]),
    ("zhipu", &["api.z.ai"]),
    ("moonshot", &["api.moonshot.ai"]),
    ("deepseek", &["api.deepseek.com"]),
];

/// The API hostname(s) a named model provider's runner must reach, or `None`
/// when the value is not one of the known providers. Lowercase-exact only, so an
/// uppercased spelling is rejected rather than silently normalized.
pub fn provider_egress_hosts(provider: &str) -> Option<&'static [&'static str]> {
    EGRESS_PROVIDERS
        .iter()
        .find(|(n, _)| *n == provider)
        .map(|(_, hosts)| *hosts)
}

/// Validate one `--allow-egress-host` value against the known providers,
/// returning its canonical `'static` name. An unknown value is a deterministic
/// input error (exit 2 / Usage) that enumerates the accepted providers and
/// points at the `--allow-web-egress` escape hatch for arbitrary destinations.
pub fn parse_egress_provider(value: &str) -> Result<&'static str, crate::exit::CliError> {
    EGRESS_PROVIDERS
        .iter()
        .find(|(n, _)| *n == value)
        .map(|(n, _)| *n)
        .ok_or_else(|| {
            let known = EGRESS_PROVIDERS
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>()
                .join(", ");
            crate::exit::CliError::usage(format!(
                "`--allow-egress-host` value `{value}` is not a known provider (expected one of: {known})"
            ))
            .with_fix(
                "pick a named provider, or open a raw range with `--allow-web-egress <CIDR>`",
            )
        })
}

fn credential_egress_provider(credential: &str) -> Option<&'static str> {
    if credential.starts_with("sk-ant-") {
        Some("anthropic")
    } else if credential.starts_with("sk-or-") {
        Some("openrouter")
    } else {
        None
    }
}

fn validate_credential_egress_consistency(
    opts: &UpOpts,
) -> std::result::Result<(), crate::exit::CliError> {
    let operator_sets = opts.operator_sets();
    let explicit_credential = operator_set_entries(&operator_sets)
        .into_iter()
        .filter(|(key, _)| key.trim() == MODEL_CREDENTIAL_KEY)
        .map(|(_, value)| value.trim())
        .next_back();
    let Some(detected) = explicit_credential
        .or(opts.credentials.as_deref())
        .and_then(credential_egress_provider)
    else {
        return Ok(());
    };

    if opts.allow_egress_host.is_empty()
        || opts
            .allow_egress_host
            .iter()
            .any(|provider| provider == detected)
    {
        return Ok(());
    }

    let explicit = opts
        .allow_egress_host
        .iter()
        .map(|provider| format!("--allow-egress-host {provider}"))
        .collect::<Vec<_>>()
        .join(" ");
    Err(crate::exit::CliError::usage(format!(
        "the configured model credential identifies `{detected}`, but `--allow-egress-host` permits only: {explicit}"
    ))
    .with_fix(format!(
        "include `--allow-egress-host {detected}`, or remove the contradictory provider selection"
    )))
}

/// A resolved host address as a single-host CIDR: `/32` for IPv4, `/128` for
/// IPv6. The egress rule opens exactly that address, nothing wider.
pub fn ip_to_egress_cidr(ip: std::net::IpAddr) -> String {
    let prefix = if ip.is_ipv4() { 32 } else { 128 };
    format!("{ip}/{prefix}")
}

/// Whether a resolved provider address is safe to open a runner egress route to:
/// a globally-routable unicast address. A poisoned or split-horizon DNS answer
/// that maps a provider host to the node metadata endpoint or any internal /
/// overlay host must never mint an egress /32 -- the chart emits no
/// metadataExcept for an exact-host allow, so this predicate is the only guard.
///
/// This is a COMPREHENSIVE denylist that mirrors, by hand, the special-use
/// ranges excluded by `std`'s `Ipv4Addr::is_global`/`Ipv6Addr::is_global` --
/// those APIs are still unstable, so we cannot call them and a partial denylist
/// would give false assurance. Every non-global-unicast range is rejected,
/// including ones reachable on internal/overlay networks (CGNAT, benchmarking,
/// reserved/future) that the earlier selective list let slip through.
fn is_globally_routable_egress(ip: std::net::IpAddr) -> bool {
    use std::net::IpAddr;
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            // Reject if the address falls in ANY special-use / non-global range.
            let non_global = o[0] == 0                        // 0.0.0.0/8 "this host on this network"
                || v4.is_private()                            // 10/8, 172.16/12, 192.168/16
                || (o[0] == 100 && (o[1] & 0xc0) == 0x40)     // CGNAT 100.64.0.0/10 (RFC6598)
                || v4.is_loopback()                           // 127.0.0.0/8
                || v4.is_link_local()                         // 169.254.0.0/16 (incl. IMDS 169.254.169.254)
                || (o[0] == 192 && o[1] == 0 && o[2] == 0)    // IETF protocol assignments 192.0.0.0/24
                || v4.is_documentation()                      // 192.0.2/24, 198.51.100/24, 203.0.113/24
                || (o[0] == 192 && o[1] == 88 && o[2] == 99)  // 6to4 relay anycast 192.88.99.0/24
                || (o[0] == 198 && (o[1] & 0xfe) == 18)       // benchmarking 198.18.0.0/15 (RFC2544)
                || o[0] >= 240                                // reserved/future 240.0.0.0/4 (incl. 255.255.255.255 broadcast)
                || v4.is_multicast()                          // 224.0.0.0/4
                || v4.is_unspecified()                        // 0.0.0.0 (belt-and-suspenders; covered by o[0]==0)
                || v4.is_broadcast(); // 255.255.255.255 (belt-and-suspenders; covered by o[0]>=240)
            !non_global
        }
        IpAddr::V6(v6) => {
            if v6.is_loopback() || v6.is_unspecified() || v6.is_multicast() {
                return false;
            }
            // Map an IPv4-mapped v6 back to v4 and re-check.
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_globally_routable_egress(IpAddr::V4(v4));
            }
            let seg = v6.segments();
            let is_ula = (seg[0] & 0xfe00) == 0xfc00; // fc00::/7
            let is_link_local = (seg[0] & 0xffc0) == 0xfe80; // fe80::/10
            let is_documentation = seg[0] == 0x2001 && seg[1] == 0x0db8; // 2001:db8::/32
            !(is_ula || is_link_local || is_documentation)
        }
    }
}

/// Resolve each named provider's API host(s) to single-host egress CIDRs. The
/// resolver is injected so the pure logic (dedup, sort, empty/error handling) is
/// unit-testable without touching real DNS. An unknown provider, a resolver
/// failure, or a host that resolves to no addresses is a hard error naming the
/// host -- never a silent skip, which would leave a real model call failing
/// closed with no clue why. The result is deduplicated and sorted so the install
/// argv is stable across runs.
pub fn resolve_provider_egress_cidrs(
    providers: &[String],
    resolve: impl Fn(&str) -> std::io::Result<Vec<std::net::IpAddr>>,
) -> Result<Vec<String>> {
    let mut cidrs = Vec::new();
    for p in providers {
        let hosts = provider_egress_hosts(p)
            .ok_or_else(|| anyhow::anyhow!("unknown egress provider `{p}`"))?;
        for host in hosts {
            let ips = resolve(host)
                .with_context(|| format!("resolving egress host {host} for provider {p}"))?;
            if ips.is_empty() {
                bail!("egress host {host} (provider {p}) resolved to no addresses");
            }
            for ip in ips {
                if !is_globally_routable_egress(ip) {
                    bail!("egress host {host} (provider {p}) resolved to non-routable address {ip}; refusing to open an egress route (possible DNS poisoning or split-horizon)");
                }
                cidrs.push(ip_to_egress_cidr(ip));
            }
        }
    }
    cidrs.sort();
    cidrs.dedup();
    Ok(cidrs)
}

type ProviderAddressResolver = Box<dyn Fn(&str) -> std::io::Result<Vec<std::net::IpAddr>>>;

fn system_provider_address_resolver() -> ProviderAddressResolver {
    Box::new(|host| {
        use std::net::ToSocketAddrs;
        (host, 443u16)
            .to_socket_addrs()
            .map(|addresses| addresses.map(|address| address.ip()).collect())
    })
}

#[cfg(not(debug_assertions))]
fn provider_address_resolver() -> Result<ProviderAddressResolver> {
    Ok(system_provider_address_resolver())
}

#[cfg(debug_assertions)]
fn provider_address_resolver() -> Result<ProviderAddressResolver> {
    let raw = match std::env::var("CURIE_TEST_PROVIDER_EGRESS_JSON") {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(system_provider_address_resolver()),
        Err(error) => {
            return Err(anyhow::anyhow!(
                "reading CURIE_TEST_PROVIDER_EGRESS_JSON: {error}"
            ));
        }
    };

    let values: serde_json::Value = serde_json::from_str(&raw).context(
        "CURIE_TEST_PROVIDER_EGRESS_JSON must be a JSON object mapping hosts to IP lists",
    )?;
    let values = values.as_object().ok_or_else(|| {
        anyhow::anyhow!(
            "CURIE_TEST_PROVIDER_EGRESS_JSON must be a JSON object mapping hosts to IP lists"
        )
    })?;
    let mut resolved = BTreeMap::new();
    for (host, addresses) in values {
        let addresses = addresses.as_array().ok_or_else(|| {
            anyhow::anyhow!(
                "CURIE_TEST_PROVIDER_EGRESS_JSON entry for {host} must be an array of IP strings"
            )
        })?;
        let mut parsed = Vec::with_capacity(addresses.len());
        for address in addresses {
            let address = address.as_str().ok_or_else(|| {
                anyhow::anyhow!(
                    "CURIE_TEST_PROVIDER_EGRESS_JSON entry for {host} must contain only IP strings"
                )
            })?;
            parsed.push(address.parse().with_context(|| {
                format!(
                    "CURIE_TEST_PROVIDER_EGRESS_JSON entry for {host} has invalid IP address {address}"
                )
            })?);
        }
        resolved.insert(host.clone(), parsed);
    }
    Ok(Box::new(move |host| {
        resolved.get(host).cloned().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "CURIE_TEST_PROVIDER_EGRESS_JSON has no address list for declared egress host {host}"
                ),
            )
        })
    }))
}

/// Resolve provider egress through the system resolver. Debug builds may inject
/// deterministic addresses, which still pass through the routability checks in
/// [`resolve_provider_egress_cidrs`].
pub(crate) fn resolve_provider_egress_cidrs_for_current_environment(
    providers: &[String],
) -> Result<Vec<String>> {
    let resolver = provider_address_resolver()?;
    resolve_provider_egress_cidrs(providers, |host| resolver(host))
}

/// A note naming the model provider(s) whose egress `cluster up` opened, or
/// `None` when no provider was requested.
pub fn provider_egress_note(providers: &[String]) -> Option<String> {
    if providers.is_empty() {
        return None;
    }
    Some(format!(
        "real model egress opened to provider(s): {}",
        providers.join(", ")
    ))
}

/// The warning to emit when a real model credential is installed but no egress
/// was opened: the runner sandbox is fail-closed, so the model is unreachable.
/// `Some` only in that one combination (a credential present with nothing opened);
/// every other case stays silent. Names both the provider flag and the raw
/// escape hatch so the operator can fix it without reading source.
pub fn sealed_credential_warning(
    credentials_present: bool,
    any_egress_opened: bool,
) -> Option<String> {
    if credentials_present && !any_egress_opened {
        Some(
            "a real model credential is set but the sandbox is sealed -- no egress opened, so the \
             model is unreachable. Pass --allow-egress-host \
             <anthropic|openrouter|zhipu|moonshot|deepseek> \
             (or --allow-web-egress <CIDR>) and re-run."
                .to_string(),
        )
    } else {
        None
    }
}

/// Shared tail of the no-credential guidance: both the live fake-model note
/// and the dry-run fresh-install note end with exactly this text, so the two
/// paths cannot drift apart (#1898).
const NO_CREDENTIAL_GUIDANCE: &str = "Set CURIE_CREDENTIALS to an Anthropic, OpenRouter, Zhipu, \
     Moonshot, or DeepSeek credential and configure matching egress before re-running \
     `curie cluster up` to enable the real model. Provider native Zhipu, Moonshot, and \
     DeepSeek also need their matching worker runtime base URL.";

/// The ordered model+egress status lines `up` prints, as (is_warning, message)
/// pairs, derived purely so every credential/egress combination is unit-tested.
/// The web-egress *count* note and the default-route warning stay in the handler
/// (they keep their own tested helpers). `any_egress_opened` folds resolved
/// provider routes, declared web egress, and (under dry-run) the intent to open.
/// Under `--dry-run`, the no-credential arm reports that whether the model is
/// preserved is unknown offline, instead of asserting the fake-model outcome
/// (#1898).
pub fn model_egress_status_lines(
    credentials_present: bool,
    local_model: bool,
    fake_model: bool,
    providers: &[String],
    any_egress_opened: bool,
    dry_run: bool,
) -> Vec<(bool, String)> {
    let mut lines: Vec<(bool, String)> = Vec::new();
    // Past-tense provider note only on a live run; under dry-run the handler
    // prints its own "a live run resolves..." note instead.
    if !providers.is_empty() && !dry_run {
        lines.push((
            false,
            provider_egress_note(providers).expect("providers non-empty"),
        ));
        lines.push((
            false,
            "resolved provider IPs can rotate; re-run `curie cluster up` if model calls start failing".into(),
        ));
    }
    if credentials_present {
        if let Some(w) = sealed_credential_warning(true, any_egress_opened) {
            lines.push((true, w));
        }
    } else if local_model {
        lines.push((
            false,
            "local model enabled; installing the chart inference deployment".into(),
        ));
    } else if !fake_model && !dry_run {
        lines.push((
            true,
            format!(
                "no CURIE_CREDENTIALS set; installing with the fake model{}",
                if any_egress_opened {
                    ""
                } else {
                    " (model egress stays sealed)"
                }
            ),
        ));
        lines.push((
            false,
            format!("Replies will be canned. {NO_CREDENTIAL_GUIDANCE}"),
        ));
    } else if !fake_model {
        // `--dry-run` stays offline (#1898): it cannot read the release's
        // recorded model configuration the way `resolve_preserved_runner_identity_values`
        // does on the live path, so it cannot know whether a rerun would
        // preserve a real credential or land on the fake model. Asserting the
        // fake-model outcome here contradicted the live run and `cluster up
        // --help`, which is corrosive for the one preflight signal an operator
        // has before an upgrade -- so state what is unknown offline instead.
        lines.push((
            true,
            format!(
                "no CURIE_CREDENTIALS set; a live run preserves the release's recorded model \
                 configuration when there is one -- not read under --dry-run{}",
                if any_egress_opened {
                    ""
                } else {
                    "; no model egress is opened by this run"
                }
            ),
        ));
        lines.push((
            false,
            format!(
                "With nothing recorded -- a fresh install -- the release comes up on the fake \
                 model and replies will be canned. {NO_CREDENTIAL_GUIDANCE}"
            ),
        ));
    }
    lines
}

/// The chart secrets a bare `helm install` would otherwise render from the
/// published dev defaults in `values.yaml` (see #57): every backing-store
/// password plus the Langfuse crypto material and the first-party app keys.
/// Each entry is `(dotted helm value key, random byte length)`. `cluster up`
/// supplies a strong random for each on a fresh install so the release never
/// boots on a credential that lives in this public repo. Slack tokens and the
/// model credential are deliberately absent -- they are operator-supplied via
/// their own paths (`cluster comms`, `CURIE_MODEL_CREDENTIALS`), not
/// generated. `langfuse.encryptionKey` must be exactly 64 hex chars, so its 32
/// bytes are load-bearing.
/// Values owned by `cluster comms`, not by `cluster up`.
///
/// `comms` writes these with `helm upgrade --reuse-values`; `up` does a full
/// upgrade and therefore drops anything it does not itself pass. That silently
/// deleted the dispatcher and its Slack tokens whenever `up` ran after `comms`
/// -- the bot simply stopped answering, with no error and nothing in the diff
/// to suggest `up` had touched Slack at all (#1067).
///
/// Preserved the same way generated secrets are: read back from the release and
/// re-supplied through the values file, never argv, so a bot token cannot leak
/// into `ps` or shell history.
const COMMS_MANAGED_KEYS: &[&str] = &["dispatcher.slack.appToken", "dispatcher.slack.botToken"];

/// The chart values `cluster github-app` records (ADR-0092), preserved across a
/// plain `cluster up` for exactly the reason [`COMMS_MANAGED_KEYS`] is.
///
/// A plain `up` does a FULL upgrade and drops anything it does not re-pass, so
/// an operator who wired the App and later ran `up` to change something
/// unrelated silently lost it: `githubAppPrivateKey` back to `""`,
/// `GITHUB_APP_ID` back to empty. The platform then falls through to the PAT
/// path or to no credential at all, and every private-repo deploy 404s -- with
/// nothing in the diff mentioning the App (#1256).
///
/// The chart's `lookup` preserve pattern does not cover these: the private key
/// renders with no guard, and the other two are plain Deployment env values
/// where no lookup is possible. So it has to happen here.
///
/// The private key is preserved the same way a bot token is -- read back from
/// the release and re-supplied through the private 0600 values file, never
/// argv, so it cannot reach `ps` or shell history.
const GITHUB_APP_MANAGED_KEYS: &[&str] = &[
    "api.githubAppId",
    "api.githubAppPrivateKey",
    "api.githubAppExistingSecret",
    "api.githubAppExistingSecretKey",
    "api.githubCloneBase",
];

const REQUIRED_SECRETS: &[(&str, usize)] = &[
    ("postgres.auth.password", 24),
    ("valkey.password", 24),
    ("clickhouse.auth.password", 24),
    ("rustfs.auth.rootPassword", 24),
    ("langfuse.salt", 16),
    ("langfuse.encryptionKey", 32),
    ("langfuse.nextauthSecret", 24),
    ("api.apiKey", 24),
    ("api.githubWebhookSecret", 24),
];

/// The chart value carrying the API's OUTBOUND GitHub credential: the eval
/// commit status and the git-flow bundle clone (#1058/#1097/#1109).
///
/// Deliberately NOT in [`REQUIRED_SECRETS`]: that list GENERATES a random for a
/// key it finds absent, which is right for a credential the install must have
/// and wrong for this one. Empty here means "no GitHub credential, public repos
/// only"; a generated random would be 32 characters of noise sent to GitHub as a
/// bearer token, failing auth in a way that reads like a permissions problem
/// rather than a missing one (#1109). Preserved, never invented.
pub(crate) const GITHUB_TOKEN_KEY: &str = "api.githubToken";

/// What `cluster up` does with [`GITHUB_TOKEN_KEY`] on this run.
///
/// Three states, resolved once in [`up`] and consumed by the pure builder:
/// - `Untouched`: supply nothing. Either the operator pinned the key through
///   `--set` (theirs to own), or there is no recorded value to keep.
/// - `Set`: write this value through the private 0600 `-f` values file -- an
///   explicit `--github-token`, or the value the last run recorded.
/// - `Clear`: write an empty value, the same shape `comms --disconnect` writes,
///   so a subsequent plain `up` finds an empty record and does not resurrect it.
#[derive(Clone, PartialEq, Eq)]
pub enum GithubTokenPlan {
    Untouched,
    Set(String),
    Clear,
}

/// Hand-written so `{:?}` on this type (or on the `UpOpts` that holds it)
/// never prints the live token -- AC2 requires the credential be absent from
/// debug output too. `Set` renders the same masked form [`mask_secret`]
/// produces everywhere else, never the raw value; a derived `Debug` would
/// print it in full.
impl std::fmt::Debug for GithubTokenPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Untouched => write!(f, "Untouched"),
            Self::Set(v) => write!(f, "Set({})", mask_secret(v)),
            Self::Clear => write!(f, "Clear"),
        }
    }
}

/// `n_bytes` of OS CSPRNG output, lowercase-hex encoded (so `2 * n_bytes`
/// chars). Hex keeps the value shell-, env- and URL-safe and satisfies every
/// backing store's charset/min-length rule, and a hex `langfuse.encryptionKey`
/// is the exact `openssl rand -hex 32` shape the chart documents.
fn random_hex(n_bytes: usize) -> Result<String> {
    use std::fmt::Write;
    let mut buf = vec![0u8; n_bytes];
    getrandom::fill(&mut buf)
        .map_err(|e| anyhow::anyhow!("OS random number generator unavailable: {e}"))?;
    let mut out = String::with_capacity(n_bytes * 2);
    for b in buf {
        let _ = write!(out, "{b:02x}");
    }
    Ok(out)
}

/// Split one Helm set element into raw `(key, value)` halves. This is the one
/// entry parser shared by rendering and the consumers of operator sets.
fn operator_set_entry(part: &str) -> Option<(&str, &str)> {
    part.split_once('=')
}

/// The operator's `--set` arguments split into raw `(key, value)` halves for
/// [`operator_set_keys`] and [`set_passthrough_leaks_github_token`]. Rendering
/// uses the separate [`mask_helm_set_expression`] parser, which preserves
/// escaped commas and brace lists while masking credential-shaped values; do
/// not reuse this naive split for rendering. This is not the only reader of
/// this grammar in the file ([`explicit_runner_model`] hand-rolls a last-wins
/// prefix match with different semantics, and is deliberately left alone).
/// Handles both repeated
/// `--set` flags and helm's comma-joined `a=1,b=2` form; an element with no `=`
/// (a bare `KEY`, an empty element, the tail of a trailing comma) assigns
/// nothing and contributes no entry.
///
/// Both halves are returned VERBATIM, whitespace included, because the callers
/// want different things from it: a key is matched trimmed, while a value's
/// surrounding whitespace is only ever shell noise. Trimming here would decide
/// that for them.
fn operator_set_entries(sets: &[String]) -> Vec<(&str, &str)> {
    sets.iter()
        .flat_map(|s| s.split(','))
        .filter_map(operator_set_entry)
        .collect()
}

/// Render one complete Helm set expression while preserving every executed
/// byte except credential values, which are replaced by their standard mask.
fn mask_helm_set_expression(expression: &str) -> String {
    let render_part = |part: &str| match operator_set_entry(part) {
        Some((key, value)) if !value.is_empty() && is_secret_value_key(key.trim()) => {
            format!("{key}={}", mask_secret(value))
        }
        _ => part.to_string(),
    };

    let mut rendered = String::with_capacity(expression.len());
    let mut start = 0;
    let mut in_brace_list = false;
    let mut escaped = false;
    let mut has_equals = false;
    let mut at_value_start = false;

    for (index, ch) in expression.char_indices() {
        if escaped {
            escaped = false;
            at_value_start = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            '=' if !has_equals => {
                has_equals = true;
                at_value_start = true;
            }
            '{' if at_value_start => {
                in_brace_list = true;
                at_value_start = false;
            }
            '}' if in_brace_list => in_brace_list = false,
            ',' if !in_brace_list => {
                rendered.push_str(&render_part(&expression[start..index]));
                rendered.push(',');
                start = index + ch.len_utf8();
                has_equals = false;
                at_value_start = false;
            }
            _ => at_value_start = false,
        }
    }

    if in_brace_list {
        return "<secret helm set expression>".to_string();
    }

    rendered.push_str(&render_part(&expression[start..]));
    rendered
}

/// The bare value keys an operator already pinned through `--set` (so the CLI
/// leaves those to the operator rather than generating over them).
fn operator_set_keys(sets: &[String]) -> std::collections::HashSet<String> {
    operator_set_entries(sets)
        .into_iter()
        .map(|(key, _)| key.trim().to_string())
        .collect()
}

/// Read a dotted helm key (`langfuse.encryptionKey`) out of a values JSON
/// object, returning the string leaf if present.
fn lookup_dotted(values: &serde_json::Value, dotted: &str) -> Option<String> {
    let mut cursor = values;
    for part in dotted.split('.') {
        cursor = cursor.get(part)?;
    }
    cursor.as_str().map(str::to_string)
}

/// Read a dotted helm key the way the chart's own truthiness gate reads it:
/// `true` for the JSON boolean `true` and for the JSON string `"true"`, and
/// `false` for everything else -- `false`, `"false"`, `"TRUE"`, `""`, any other
/// string, a number, an object, a null leaf, a missing leaf, and a missing or
/// non-object intermediate segment all read as off.
///
/// It mirrors two contracts and must not drift from either: the
/// `eq (toString ...) "true"` branch of `curie.managedSecret` in
/// `charts/curie/templates/_helpers.tpl`, which decides whether a chart-owned
/// credential renders as the published dev default, and the quoted-`"false"`
/// render assertion in `charts/curie/ci/render-assertions.sh`, which pins that
/// spelling as fail-closed. Reading as on what the chart reads as off would
/// wave through exactly the flip this exists to catch, so every ambiguous
/// gate-shaped value fails closed (#1375, #1145).
///
/// This cannot reuse [`lookup_dotted`]: helm records `--set KEY=true` as a JSON
/// *boolean*, and `lookup_dotted` ends in `as_str()`, so it returns `None` for
/// exactly the shape this key normally has.
fn lookup_dotted_flag(values: &serde_json::Value, dotted: &str) -> bool {
    let mut cursor = values;
    for part in dotted.split('.') {
        let Some(next) = cursor.get(part) else {
            return false;
        };
        cursor = next;
    }
    match cursor {
        serde_json::Value::Bool(flag) => *flag,
        serde_json::Value::String(raw) => raw == "true",
        _ => false,
    }
}

/// The value helm already recorded for `key`, when there is a real one. An
/// empty record is what a `--disconnect` / `--clear-*` wrote and is not a
/// credential; returning `None` for it is what stops a cleared value being
/// resurrected on the next plain `up`.
fn preserved_value(existing: Option<&serde_json::Value>, key: &str) -> Option<String> {
    lookup_dotted(existing?, key).filter(|current| !current.is_empty())
}

/// Which Secret and key a direct-passthrough credential (issue #1759) should
/// be read from, given the release's recorded Helm values: the operator's own
/// `existingSecret` when one is configured for this key, otherwise `None` (the
/// caller falls back to the chart's own release Secret and the published key
/// name for this credential).
///
/// MUST stay the same decision as the chart's own BYO-wins precedence
/// (`curie.secretRef.*` and the per-key if/else in
/// `charts/curie/templates/{worker,dispatcher,api,agent-sandbox}.yaml`): a CLI
/// read that resolves a different Secret than the workload's own env would
/// report a plausible but wrong value, which is worse than reporting none.
/// Pure and testable; the actual `helm get values` read stays in the async
/// caller.
pub(crate) fn resolve_existing_secret_ref(
    existing: Option<&serde_json::Value>,
    existing_secret_key: &str,
    existing_secret_key_key: &str,
    default_data_key: &str,
) -> Option<(String, String)> {
    let secret_name = lookup_dotted(existing?, existing_secret_key).filter(|s| !s.is_empty())?;
    let data_key = lookup_dotted(existing?, existing_secret_key_key)
        .filter(|k| !k.is_empty())
        .unwrap_or_else(|| default_data_key.to_string());
    Some((secret_name, data_key))
}

/// Re-supply the [`COMMS_MANAGED_KEYS`] a previous `cluster comms` recorded.
///
/// An operator `--set` for a key always wins, and a key helm has no record of
/// is left alone -- so this only ever preserves, never invents.
fn resolve_comms_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let overridden = operator_set_keys(operator_sets);
    COMMS_MANAGED_KEYS
        .iter()
        .filter(|key| !overridden.contains(**key))
        .filter_map(|key| {
            preserved_value(existing, key).map(|current| ((*key).to_string(), current))
        })
        .collect()
}

/// Re-supply the [`GITHUB_APP_MANAGED_KEYS`] a previous `cluster github-app` recorded.
///
/// Same contract as [`resolve_comms_values`]: an operator `--set` wins, a key
/// helm has no record of is left alone. Preserves, never invents -- inventing
/// an App id would be worse than dropping one.
fn resolve_github_app_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let overridden = operator_set_keys(operator_sets);
    GITHUB_APP_MANAGED_KEYS
        .iter()
        .filter(|key| !overridden.contains(**key))
        .filter_map(|key| {
            preserved_value(existing, key).map(|current| ((*key).to_string(), current))
        })
        .collect()
}

/// Supply the sealing keypair (ADR-0094), generating one only when safe.
///
/// The rules differ from [`resolve_generated_secrets`] in one place, and the
/// difference matters:
///
/// - An operator `--set` always wins.
/// - A release that already records a key gets exactly that key back. Never a
///   new one. Regenerating would render every sealed credential in every agent
///   repository permanently unreadable -- the #1256 preservation bug with a
///   blast radius no store password comes close to.
/// - A release with NO key recorded gets a fresh one, and this is where it
///   diverges from the store passwords. For those, minting on an existing
///   release would rotate a credential out from under a running store, so the
///   rule is "leave it alone". For sealing there is nothing to rotate: no key
///   means nothing has ever been sealed to this cluster, so generating one is
///   how an existing install gains the feature.
///
/// The PREVIOUS key is only ever preserved, never generated: it exists solely
/// because an operator deliberately rotated, and inventing one would claim an
/// overlap that never happened.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SealingPrivateKeyDisposition {
    OperatorSet,
    Deferred,
    Preserved,
    Generated,
}

fn sealing_private_key_disposition(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
    dry_run: bool,
) -> SealingPrivateKeyDisposition {
    if operator_set_keys(operator_sets).contains(crate::sealing::SEALING_PRIVATE_KEY) {
        return SealingPrivateKeyDisposition::OperatorSet;
    }
    if preserved_value(existing, crate::sealing::SEALING_PRIVATE_KEY).is_some() {
        return SealingPrivateKeyDisposition::Preserved;
    }
    if dry_run {
        SealingPrivateKeyDisposition::Deferred
    } else {
        SealingPrivateKeyDisposition::Generated
    }
}

fn resolve_preserved_sealing_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let overridden = operator_set_keys(operator_sets);
    let mut resolved = Vec::new();

    if !overridden.contains(crate::sealing::SEALING_PRIVATE_KEY) {
        if let Some(current) = preserved_value(existing, crate::sealing::SEALING_PRIVATE_KEY) {
            resolved.push((crate::sealing::SEALING_PRIVATE_KEY.to_string(), current));
        }
    }
    if !overridden.contains(crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY) {
        if let Some(previous) =
            preserved_value(existing, crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY)
        {
            resolved.push((
                crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY.to_string(),
                previous,
            ));
        }
    }
    resolved
}

fn resolve_sealing_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let mut resolved = resolve_preserved_sealing_values(existing, operator_sets);
    if sealing_private_key_disposition(existing, operator_sets, false)
        == SealingPrivateKeyDisposition::Generated
    {
        resolved.insert(
            0,
            (
                crate::sealing::SEALING_PRIVATE_KEY.to_string(),
                crate::sealing::generate_keypair().private_key,
            ),
        );
    }
    resolved
}

/// Preserve the installed mail surface as typed values: booleans, arrays and
/// credential maps must not become strings when delivered through Helm -f.
/// This family includes the paired worker credential source, because preserving
/// only the adapter side can leave the channel unable to receive replies.
fn resolve_retained_mail_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Result<Option<PrivateHelmValues>> {
    let Some(existing) = existing else {
        return Ok(None);
    };
    let mut retained = serde_json::json!({});
    if let Some(mail) = existing.get("mailAdapter") {
        retained["mailAdapter"] = mail.clone();
    }
    for key in [
        "adapterCredentials",
        "adapterCredentialsExistingSecret",
        "adapterCredentialsExistingSecretKey",
    ] {
        if let Some(value) = existing.get("worker").and_then(|worker| worker.get(key)) {
            retained["worker"][key] = value.clone();
        }
    }
    let overridden = operator_set_keys(operator_sets);
    let operator_values: BTreeMap<_, _> = operator_set_entries(operator_sets)
        .into_iter()
        .map(|(key, value)| (key.trim(), value))
        .collect();
    let replaces_inline = |inline: &str| {
        operator_values.iter().any(|(key, value)| {
            key_is_or_descends_from(key, inline)
                && !value.is_empty()
                && !(*key == inline
                    && inline == "worker.adapterCredentials"
                    && serde_json::from_str::<serde_json::Value>(value)
                        .is_ok_and(|value| value.as_object().is_some_and(|map| map.is_empty())))
        })
    };
    // An externally managed worker map is opaque: changing only the adapter's
    // source cannot safely derive or replace its paired worker credential.
    // Require an explicit paired decision instead of creating a silent 401.
    let changes_adapter_source = replaces_inline("mailAdapter.egressSecret")
        || operator_values
            .get("mailAdapter.egressSecretExistingSecret")
            .is_some_and(|value| {
                Some(*value)
                    != lookup_dotted(existing, "mailAdapter.egressSecretExistingSecret").as_deref()
            })
        || (lookup_dotted(existing, "mailAdapter.egressSecretExistingSecret")
            .is_some_and(|value| !value.is_empty())
            && operator_values
                .get("mailAdapter.egressSecretExistingSecretKey")
                .is_some_and(|value| {
                    *value
                        != lookup_dotted(existing, "mailAdapter.egressSecretExistingSecretKey")
                            .as_deref()
                            .unwrap_or("mailEgressSecret")
                }));
    let changes_worker_source = replaces_inline("worker.adapterCredentials")
        || overridden.contains("worker.adapterCredentialsExistingSecret")
        || overridden.contains("worker.adapterCredentialsExistingSecretKey");
    if changes_adapter_source
        && !changes_worker_source
        && lookup_dotted(existing, "worker.adapterCredentialsExistingSecret")
            .is_some_and(|value| !value.is_empty())
    {
        bail!(
            "changing the mail egress credential source requires an explicit paired worker \
             source decision: also set worker.adapterCredentialsExistingSecret (clear it \
             to use the chart-derived inline pair), its ExistingSecretKey, or supply \
             worker.adapterCredentials"
        );
    }
    let mut cleared = BTreeMap::new();
    for inline in [
        "mailAdapter.agentmail.apiKey",
        "mailAdapter.channelToken",
        "mailAdapter.egressSecret",
        "worker.adapterCredentials",
    ] {
        let reference = format!("{inline}ExistingSecret");
        // A nonempty inline value replaces the external source. An empty inline
        // clear leaves that source active; its explicit reference clear remains
        // the operator's way to remove it. Active sources never replay stale
        // inline copies left in Helm before token rotation.
        let remove = if replaces_inline(inline) {
            vec![reference.clone()]
        } else if overridden.contains(&reference)
            || lookup_dotted(&retained, &reference).is_some_and(|value| !value.is_empty())
        {
            vec![inline.to_string()]
        } else {
            vec![]
        };
        for path in remove {
            let (parent, leaf) = path.rsplit_once('.').expect("credential path");
            let pointer = format!("/{}", parent.replace('.', "/"));
            if let Some(object) = retained
                .pointer_mut(&pointer)
                .and_then(serde_json::Value::as_object_mut)
            {
                if let Some(removed) = object.remove(leaf) {
                    let mut removed_leaves = BTreeMap::new();
                    crate::installation::flatten_values(&removed, &path, &mut removed_leaves);
                    cleared.extend(removed_leaves.into_keys().map(|key| (key, String::new())));
                }
            }
        }
    }
    fn without_overrides(
        value: &serde_json::Value,
        path: &str,
        overridden: &std::collections::HashSet<String>,
    ) -> Option<serde_json::Value> {
        if overridden
            .iter()
            .any(|key| key_is_or_descends_from(path, key))
        {
            return None;
        }
        match value {
            serde_json::Value::Object(object) => {
                let remaining: serde_json::Map<String, serde_json::Value> = object
                    .iter()
                    .filter_map(|(key, value)| {
                        let child = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{path}.{key}")
                        };
                        without_overrides(value, &child, overridden)
                            .map(|value| (key.clone(), value))
                    })
                    .collect();
                (!remaining.is_empty() || object.is_empty())
                    .then_some(serde_json::Value::Object(remaining))
            }
            serde_json::Value::Array(_)
                if overridden
                    .iter()
                    .any(|key| key_is_or_descends_from(key, path)) =>
            {
                None
            }
            _ => Some(value.clone()),
        }
    }
    Ok(without_overrides(&retained, "", &overridden)
        .filter(|value| value.as_object().is_some_and(|object| !object.is_empty()))
        .map(|document| PrivateHelmValues(document, cleared)))
}

/// Every value a plain `cluster up` must carry forward, in one place.
///
/// `up` does a FULL upgrade and drops anything it does not re-pass, so each
/// family of keys recorded by a SIBLING verb has to be re-supplied here or it
/// is silently reverted -- Slack tokens by `comms` (#1067), the GitHub App by
/// `github-app` (#1256). Both failures looked identical to the operator:
/// something that had been working stopped, with nothing in the diff naming
/// it.
///
/// Composed rather than called separately from `up`, so a test can assert the
/// whole set. Unit-testing each family in isolation left the WIRING uncovered:
/// deleting the call from `up` kept every test green.
fn resolve_preserved_values(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Vec<(String, String)> {
    let mut all = resolve_comms_values(existing, operator_sets);
    all.extend(resolve_github_app_values(existing, operator_sets));
    all.extend(resolve_preserved_sealing_values(existing, operator_sets));
    all
}

/// Resolve managed values for an actual or previewed `cluster up`.
///
/// An offline preview has no evidence that the sealing key is absent, so it
/// carries only values that are positively known to exist. A live completion
/// may generate the current sealing key after the release read proves it is
/// absent.
fn resolve_managed_values_for_up(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
    dry_run: bool,
) -> Vec<(String, String)> {
    let mut values = resolve_preserved_values(existing, operator_sets);
    if sealing_private_key_disposition(existing, operator_sets, dry_run)
        == SealingPrivateKeyDisposition::Generated
    {
        values.extend(
            resolve_sealing_values(existing, operator_sets)
                .into_iter()
                .filter(|(key, _)| key == crate::sealing::SEALING_PRIVATE_KEY),
        );
    }
    values
}

/// The chart value holding the model credential. Named here so the secret
/// classifier below cannot drift from the key `up_commands` actually masks.
pub(crate) const MODEL_CREDENTIAL_KEY: &str = "agentSandbox.runner.credentials";

/// Emitted alongside [`MODEL_CREDENTIAL_KEY`], and only when a credential is
/// present -- see `up_commands`, which pushes both inside one `if let`.
pub(crate) const FAKE_MODEL_KEY: &str = "agentSandbox.runner.fakeModel";

const ALLOWED_EGRESS_KEY: &str = "security.networkPolicy.allowedEgress";
const WORKER_EXTRA_ENV_KEY: &str = "worker.extraEnv";

/// The ADDITIONAL Slack origins a per-turn reply endpoint may name (ADR-0096
/// D4.4). Named here so `up`'s preservation, `diff`'s reset reporting, and the
/// secret classifier below all read the one key.
const SLACK_TRUSTED_ORIGINS_KEY: &str = "worker.slackTrustedOrigins";

fn key_is_or_descends_from(key: &str, parent: &str) -> bool {
    key == parent
        || key
            .strip_prefix(parent)
            .is_some_and(|suffix| suffix.starts_with('[') || suffix.starts_with('.'))
}

fn escape_helm_set_string_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(ch, '\\' | ',' | '{' | '}') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

fn helm_set_string_entries(expression: &str) -> Vec<(String, String)> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_brace_list = false;
    let mut escaped = false;
    let mut has_equals = false;
    let mut at_value_start = false;

    for (index, ch) in expression.char_indices() {
        if escaped {
            escaped = false;
            at_value_start = false;
            continue;
        }

        match ch {
            '\\' => escaped = true,
            '=' if !has_equals => {
                has_equals = true;
                at_value_start = true;
            }
            '{' if at_value_start => {
                in_brace_list = true;
                at_value_start = false;
            }
            '}' if in_brace_list => in_brace_list = false,
            ',' if !in_brace_list => {
                parts.push(&expression[start..index]);
                start = index + ch.len_utf8();
                has_equals = false;
                at_value_start = false;
            }
            _ => at_value_start = false,
        }
    }
    parts.push(&expression[start..]);

    parts
        .into_iter()
        .filter_map(operator_set_entry)
        .map(|(key, value)| {
            let mut decoded = String::with_capacity(value.len());
            let mut chars = value.chars();
            while let Some(ch) = chars.next() {
                if ch == '\\' {
                    if let Some(next) = chars.next() {
                        decoded.push(next);
                    } else {
                        decoded.push(ch);
                    }
                } else {
                    decoded.push(ch);
                }
            }
            (key.trim().to_string(), decoded)
        })
        .collect()
}

/// Carry the runner configuration recorded by a prior real model install into
/// a plain rerun. Explicit inputs replace their recorded family.
fn resolve_preserved_runner_identity_values(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) {
    let overridden = operator_set_keys(operator_sets);

    if !opts.fake_model
        && opts.local_model.is_none()
        && opts.credentials.is_none()
        && !overridden.contains(MODEL_CREDENTIAL_KEY)
    {
        opts.credentials = preserved_value(existing, MODEL_CREDENTIAL_KEY);
    }

    if !opts.fake_model
        && opts.local_model.is_none()
        && opts.model.is_none()
        && !overridden.contains(RUNNER_MODEL_KEY)
    {
        opts.model = preserved_value(existing, RUNNER_MODEL_KEY);
    }
}

/// Carry an inferred gVisor posture into a later plain `cluster up`.
///
/// The RuntimeClass admission recovery writes `security.gvisor.mode=off` only
/// on its retry. Helm records that successful retry, but a normal `up` is a
/// full upgrade rather than `--reuse-values`; without re-supplying the recorded
/// posture, the next run falls back to the chart's `auto` default and repeats
/// the failed preflight. As with the other recorded-value families, an explicit
/// operator setting owns the key and always wins.
fn resolve_preserved_gvisor_mode_value(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) {
    if operator_set_keys(operator_sets).contains(GVISOR_MODE_KEY) {
        return;
    }
    if let Some(mode) = preserved_value(existing, GVISOR_MODE_KEY) {
        opts.set.push(format!("{GVISOR_MODE_KEY}={mode}"));
    }
}

/// Carry the worker environment recorded by a prior install into a plain rerun.
/// Explicit inputs replace the recorded family.
fn resolve_preserved_worker_extra_env_values(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) {
    let overridden = operator_set_keys(operator_sets);
    if overridden
        .iter()
        .any(|key| key_is_or_descends_from(key, WORKER_EXTRA_ENV_KEY))
    {
        return;
    }

    let mut recorded = BTreeMap::new();
    if let Some(values) = existing {
        crate::installation::flatten_values(values, "", &mut recorded);
    }
    opts.set_string.extend(
        recorded
            .into_iter()
            .filter(|(key, _)| key_is_or_descends_from(key, WORKER_EXTRA_ENV_KEY))
            .map(|(key, value)| format!("{key}={}", escape_helm_set_string_value(&value))),
    );
}

/// Carry an operator-recorded Slack trusted-origin list into a later plain
/// `cluster up` (issue #1897).
///
/// `up` is a FULL helm upgrade, not `--reuse-values`, so a
/// `worker.slackTrustedOrigins` an operator set once is reset to the chart's
/// fail-closed `""` default by the next unrelated `up` -- the #1256
/// preservation class again, and it silently re-breaks the dev reply path the
/// operator had working. Emitted via `--set-string` with
/// [`escape_helm_set_string_value`] because the value is a COMMA-SEPARATED
/// origin list: an unescaped comma would make Helm read it as a list.
///
/// Preserves, never invents. [`preserved_value`] already filters an empty
/// record, so a release that never set the key -- or one whose value was
/// deliberately cleared -- supplies nothing and keeps the chart default. An
/// explicit operator `--set` / `--set-string` owns the key and always wins.
fn resolve_preserved_slack_trusted_origins_value(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) {
    if operator_set_keys(operator_sets).contains(SLACK_TRUSTED_ORIGINS_KEY) {
        return;
    }
    if let Some(origins) = preserved_value(existing, SLACK_TRUSTED_ORIGINS_KEY) {
        opts.set_string.push(format!(
            "{SLACK_TRUSTED_ORIGINS_KEY}={}",
            escape_helm_set_string_value(&origins)
        ));
    }
}

fn resolve_preserved_runner_egress_values(
    opts: &mut UpOpts,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
    provider_was_inferred: bool,
) -> (usize, BTreeSet<String>) {
    let overridden = operator_set_keys(operator_sets);
    let egress_replaced = (!opts.allow_egress_host.is_empty() && !provider_was_inferred)
        || !opts.allow_web_egress.is_empty()
        || overridden
            .iter()
            .any(|key| key_is_or_descends_from(key, ALLOWED_EGRESS_KEY));
    if egress_replaced {
        return (0, BTreeSet::new());
    }

    let mut recorded = BTreeMap::new();
    if let Some(values) = existing {
        crate::installation::flatten_values(values, "", &mut recorded);
    }
    let next_index = recorded
        .keys()
        .filter_map(|key| {
            key.strip_prefix(ALLOWED_EGRESS_KEY)?
                .strip_prefix('[')?
                .split_once(']')?
                .0
                .parse::<usize>()
                .ok()
        })
        .max()
        .map_or(0, |index| index + 1);
    let recorded_cidrs = recorded
        .iter()
        .filter_map(|(key, value)| {
            let suffix = key.strip_prefix(ALLOWED_EGRESS_KEY)?.strip_prefix('[')?;
            let (index, field) = suffix.split_once(']')?;
            (index.parse::<usize>().is_ok() && field == ".cidr").then(|| value.clone())
        })
        .collect();
    opts.set.extend(
        recorded
            .into_iter()
            .filter(|(key, _)| key_is_or_descends_from(key, ALLOWED_EGRESS_KEY))
            .map(|(key, value)| format!("{key}={value}")),
    );
    (next_index, recorded_cidrs)
}

fn resolve_provider_egress_for_up(opts: &mut UpOpts, resolve: bool) -> Result<()> {
    if resolve && !opts.allow_egress_host.is_empty() && opts.resolved_egress_cidrs.is_empty() {
        opts.resolved_egress_cidrs =
            resolve_provider_egress_cidrs_for_current_environment(&opts.allow_egress_host)
                .context("resolving named provider egress hosts")?;
    }
    Ok(())
}

fn reindex_inferred_provider_egress(
    opts: &mut UpOpts,
    start_index: usize,
    recorded_cidrs: &BTreeSet<String>,
) {
    let resolved = std::mem::take(&mut opts.resolved_egress_cidrs);
    for (offset, cidr) in resolved
        .into_iter()
        .filter(|cidr| !recorded_cidrs.contains(cidr))
        .enumerate()
    {
        let index = start_index + offset;
        opts.set
            .push(format!("{ALLOWED_EGRESS_KEY}[{index}].cidr={cidr}"));
        opts.set.push(format!(
            "{ALLOWED_EGRESS_KEY}[{index}].ports[0].protocol=TCP"
        ));
        opts.set.push(format!(
            "{ALLOWED_EGRESS_KEY}[{index}].ports[0].port={EGRESS_TCP_PORT}"
        ));
    }
}

/// Does a plain `cluster up` carry this key forward when nothing re-passes it?
///
/// The honest half of `curie diff`. `up` does a FULL upgrade, so a key present
/// on the release but absent from `curie.yaml` is normally reset to the chart
/// default -- except for the families [`resolve_preserved_values`],
/// [`resolve_preserved_runner_identity_values`], and
/// [`resolve_preserved_runner_egress_values`],
/// [`resolve_preserved_gvisor_mode_value`], and
/// [`resolve_preserved_slack_trusted_origins_value`] re-supply, which survive
/// untouched.
/// Reporting those as removals would be the exact
/// "proposing to delete what it did not create" failure ADR-0097 named.
///
/// Reads the same constants `up` reads, so a new preserved family is picked up
/// by both or neither. That claim was false for one release: sealing (ADR-0094)
/// added a family to [`resolve_preserved_values`] and not to this list, so
/// `diff` announced that apply would reset `sealing.privateKey` to the chart
/// default. Apply does no such thing -- it hands the live key straight back.
///
/// A false reset here is worse than a missing one. The note `diff` prints on a
/// reset says to declare the value in `curie.yaml` to keep it, and following
/// that for this key means pasting a PRIVATE KEY into a file whose own header
/// says it carries names and never secrets. `sealing_test` below asserts the
/// two lists agree by construction rather than by a hand-kept fixture.
pub fn is_preserved_by_up(key: &str) -> bool {
    key_is_or_descends_from(key, "mailAdapter")
        || key_is_or_descends_from(key, "worker.adapterCredentials")
        || key == "worker.adapterCredentialsExistingSecret"
        || key == "worker.adapterCredentialsExistingSecretKey"
        || COMMS_MANAGED_KEYS.contains(&key)
        || GITHUB_APP_MANAGED_KEYS.contains(&key)
        || REQUIRED_SECRETS.iter().any(|(k, _)| *k == key)
        || crate::sealing::SEALING_MANAGED_KEYS.contains(&key)
        || key == GVISOR_MODE_KEY
        || key == SLACK_TRUSTED_ORIGINS_KEY
}

/// Substrings that mark a chart key as carrying a credential.
///
/// The masking rule has to be deny-by-default, and this is what makes it so.
/// Matching on NAME rather than on membership of a managed list is the whole
/// point: a list only knows the keys THIS chart version manages, and anything
/// outside it -- a key an older chart used, a key an operator set by hand --
/// gets printed.
///
/// Over-masking is the safe direction. Masking `api.githubCloneBase` costs a
/// reader one lookup; printing a password costs a rotation.
const SECRET_KEY_MARKERS: &[&str] = &[
    "password",
    "passwd",
    "secret",
    "token",
    "credential",
    "salt",
    "key",
    "auth",
];

/// Does this key's VALUE carry a secret?
///
/// `helm get values` returns real passwords and tokens, so anything rendering a
/// live release has to know which values must never reach a terminal, a log, or
/// a `--json` consumer.
///
/// This was an allowlist of managed keys, and it leaked. Run against a real
/// release, `curie diff` printed `minio.auth.rootPassword` in full: the chart
/// had since renamed that store to `rustfs`, so the live key matched no managed
/// list and fell through to "ordinary value". The store was still running. The
/// unit tests could not have caught it -- they chose their own fixture values,
/// so the "no secret in the output" assertion compared against a plaintext the
/// test itself invented.
///
/// The question is now "does this key NAME say it holds a credential?", which
/// is true of a renamed key, a legacy key, and an operator's own `--set`, none
/// of which any list can enumerate in advance.
pub fn is_secret_value_key(key: &str) -> bool {
    // Most preserve-on-up keys are credentials, but an inferred gVisor posture
    // is ordinary safety configuration and must remain visible in `curie diff`.
    // A Slack trusted-origin list (issue #1897) is the same shape: it is
    // operator-visible dev configuration -- hostnames, not a token -- and
    // masking it would hide the very value the operator opens `curie diff` to
    // confirm survived the upgrade.
    if (is_preserved_by_up(key) && key != GVISOR_MODE_KEY && key != SLACK_TRUSTED_ORIGINS_KEY)
        || key == GITHUB_TOKEN_KEY
        || key == MODEL_CREDENTIAL_KEY
    {
        return true;
    }
    let lowered = key.to_ascii_lowercase();
    SECRET_KEY_MARKERS
        .iter()
        .any(|marker| lowered.contains(marker))
}

/// `helm list -n <ns> -o json`, for reading the deployed chart version.
fn helm_list_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("list"),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// The chart the release was last installed with, e.g. `curie-0.5.1`.
///
/// `curie diff` compares VALUES, which says nothing about whether the chart
/// those values feed is the same chart. On a real release this mattered: the
/// cluster ran `curie-0.5.1`, whose object store is MinIO, while the CLI would
/// apply `0.6.0`, where that component is `rustfs` and does not exist under the
/// old name. A value-level diff renders that as a handful of `minio.*` resets
/// when it is really a component swap.
pub async fn fetch_release_chart(o: &CommonOpts) -> Result<Option<String>> {
    let (ok, out, _err) = run_capture(&helm_list_cmd(o)).await?;
    if !ok {
        return Ok(None);
    }
    let parsed: serde_json::Value = match serde_json::from_str(out.trim()) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    Ok(parsed
        .as_array()
        .and_then(|releases| {
            releases
                .iter()
                .find(|r| r.get("name").and_then(|n| n.as_str()) == Some(o.release.as_str()))
        })
        .and_then(|r| r.get("chart"))
        .and_then(|c| c.as_str())
        .map(str::to_string))
}

/// Has the operator told Helm to keep this resource across upgrades?
///
/// `helm.sh/resource-policy: keep` is Helm's own mechanism for "do not delete
/// this, even when the chart stops rendering it". A resource carrying it is not
/// at risk, so flagging it would be a false alarm -- and annotating it is
/// exactly how an operator detaches a store before a chart renames it, which is
/// the supported way through the very migration this guard exists to stop them
/// botching.
pub fn helm_keeps(resource: &serde_json::Value) -> bool {
    resource
        .get("metadata")
        .and_then(|m| m.get("annotations"))
        .and_then(|a| a.get("helm.sh/resource-policy"))
        .and_then(|p| p.as_str())
        .map(|p| p == "keep")
        .unwrap_or(false)
}

/// The COMPONENT identities of the StatefulSets the release currently owns.
///
/// Keyed on `app.kubernetes.io/component`, never on `metadata.name`. Resource
/// names embed the chart fullname, so `nameOverride` alone renames every one of
/// them -- and comparing names made a release installed with an override look
/// like it was losing every stateful component at once. A guard that cries wolf
/// teaches operators to pass the override flag by reflex, which is worse than
/// no guard for the one case that is real.
///
/// Returns (component, resource name): the component is the identity, the name
/// is what an operator needs to see in the error.
pub async fn live_stateful_components(o: &CommonOpts) -> Result<Vec<(String, String)>> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("statefulset"),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    );
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        // Fail closed. The ONLY "there is nothing here" answer this argv
        // produces is exit 0 with an empty items array, which a namespaced LIST
        // returns even for a namespace that does not exist (verified against a
        // real apiserver, kubectl v1.36.2). So a nonzero exit always means "I
        // could not find out", and returning an empty list from it told the
        // stateful-removal guard "fresh install, nothing to lose" while the
        // upgrade went on to prune the StatefulSet (#1351). The helm half of
        // this same guard already bails; this is the symmetric half.
        //
        // The wording stays NEUTRAL about why the read was wanted. Three of this
        // helper's four callers are running a migration rather than the
        // stateful-removal guard, so naming that guard here reported a check the
        // caller was not performing (#1351); the guard adds its own framing with
        // `.context(..)` at its call site.
        //
        // kubectl can exit nonzero with an empty stderr (a signal, a wrapper
        // script), so fall back to stdout and then to a literal rather than
        // handing the operator a message ending in a bare colon.
        let detail = [err.trim(), out.trim()]
            .into_iter()
            .find(|s| !s.is_empty())
            .unwrap_or("kubectl exited nonzero with no stderr");
        let message = format!(
            "could not list the StatefulSets in namespace {}: {detail}",
            o.namespace
        );
        // The bail is UNCONDITIONAL: every nonzero exit fails, so a Forbidden
        // still fails (as Failure) and is never swallowed into a vacuous pass.
        // `is_connectivity_failure` only picks the exit CLASS once that decision
        // is made. An unreachable apiserver is the retryable condition exit 3
        // names (`exit.rs`), and the helm half of a teardown already reports it
        // that way, so an automation loop retries the same argv instead of
        // reading a rolling restart as permanent (#1351).
        return Err((if is_connectivity_failure(&err) {
            crate::exit::CliError::transient(message)
        } else {
            crate::exit::CliError::failure(message)
        }
        .with_fix(
            "check the cluster is reachable and the kubeconfig points at the right context: `kubectl config current-context` then `kubectl get ns`",
        ))
        .into());
    }
    let parsed: serde_json::Value = match serde_json::from_str(out.trim()) {
        Ok(v) => v,
        Err(_) => return Ok(Vec::new()),
    };
    Ok(stateful_components_from_list(&parsed))
}

/// The pure half of [`live_stateful_components`]: a kubectl List in, the
/// at-risk components out.
///
/// Split out so the `helm.sh/resource-policy: keep` filter is covered by a test
/// that fails when the filter is REMOVED. Testing `helm_keeps` alone did not:
/// deleting its call site left every test green, which is the same vacuous
/// shape that hid three earlier bugs in this file's history.
pub fn stateful_components_from_list(list: &serde_json::Value) -> Vec<(String, String)> {
    list.get("items")
        .and_then(|i| i.as_array())
        .map(|items| {
            items
                .iter()
                .filter(|i| !helm_keeps(i))
                .filter_map(|i| {
                    let component = i
                        .get("spec")?
                        .get("selector")?
                        .get("matchLabels")?
                        .get("app.kubernetes.io/component")?
                        .as_str()?;
                    let name = i.get("metadata")?.get("name")?.as_str()?;
                    Some((component.to_string(), name.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// StatefulSet names the target chart would render for this release.
///
/// `helm template` rather than a dry-run upgrade: it needs no cluster and
/// cannot mutate, so the guard is safe to run before deciding whether to
/// proceed.
pub(crate) async fn chart_stateful_components(
    chart: &str,
    o: &CommonOpts,
    value_plan: &UpValuePlan,
) -> Result<Vec<(String, String)>> {
    let mut args = vec![
        plain("template"),
        plain(&o.release),
        plain(chart),
        plain("-n"),
        plain(&o.namespace),
    ];
    value_plan.append_command_args(&mut args);
    let (ok, out, err) = run_capture(&OpsCommand::new("helm", args)).await?;
    if !ok {
        bail!("could not render the target chart to check for removed stateful components: {err}");
    }
    Ok(parse_statefulset_components(&out))
}

/// The version declared by the chart at `chart`, read from its own `Chart.yaml`.
///
/// `helm show chart` accepts every reference form `--chart` does -- a
/// directory, a `.tgz`, a repo ref -- so this is the version of the chart that
/// was actually rendered rather than one guessed from the reference's spelling.
///
/// A non-zero exit is fatal on purpose: this feeds the CHART VERSION MISMATCH
/// comparison, and failing open to a guessed version would raise (or suppress)
/// that warning about a chart nothing looked at (#1352).
pub async fn chart_version(chart: &str) -> Result<String> {
    let cmd = OpsCommand::new("helm", vec![plain("show"), plain("chart"), plain(chart)]);
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        bail!("could not read the chart version of {chart}: {err}");
    }
    let chart_yaml: serde_json::Value = serde_norway::from_str(&out)
        .with_context(|| format!("could not parse the Chart.yaml of {chart}"))?;
    chart_yaml
        .get("version")
        .and_then(|version| version.as_str())
        .map(str::to_string)
        .with_context(|| format!("the Chart.yaml of {chart} declares no version"))
}

/// The component identities of StatefulSets in a multi-document helm render.
///
/// Split-and-parse rather than a regex: a `kind: StatefulSet` line can appear
/// inside an annotation or a ConfigMap payload, and matching that would invent
/// a component the chart does not actually create.
pub fn parse_statefulset_components(rendered: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for doc in rendered.split("\n---") {
        let Ok(value) = serde_norway::from_str::<serde_json::Value>(doc) else {
            continue;
        };
        if value.get("kind").and_then(|k| k.as_str()) != Some("StatefulSet") {
            continue;
        }
        let component = value
            .get("spec")
            .and_then(|s| s.get("selector"))
            .and_then(|s| s.get("matchLabels"))
            .and_then(|l| l.get("app.kubernetes.io/component"))
            .and_then(|c| c.as_str());
        let name = value
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str());
        if let (Some(component), Some(name)) = (component, name) {
            if !out.iter().any(|(c, _)| c == component) {
                out.push((component.to_string(), name.to_string()));
            }
        }
    }
    out
}

/// Why a live stateful component would not survive the apply.
///
/// Carried rather than collapsed to a bare name because the two causes have
/// DIFFERENT remedies, and the refusal is the only place an operator learns
/// which one they need. Offering `--migrate-store` for a rename would send
/// them to a flag that cannot help: both releases run the same store, so
/// there is nothing to migrate between.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemovalCause {
    /// The component is absent from the render entirely -- a chart version
    /// renamed or dropped it (minio -> rustfs). The data must be carried
    /// across, which is what `--migrate-store` does.
    ComponentGone,
    /// The component survives, under a different resource name. Nothing about
    /// the chart changed; the values did.
    RenamedTo(String),
}

/// A live stateful component the target chart would not recreate in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatefulRemoval {
    /// The live resource name, which is what an operator recognises in their
    /// own cluster.
    pub name: String,
    pub component: String,
    pub cause: RemovalCause,
}

/// Live stateful components the target chart would not recreate.
///
/// Pure, so the decision this guard turns on is testable without a cluster.
pub fn removed_stateful_components(
    live: &[(String, String)],
    rendered: &[(String, String)],
) -> Vec<StatefulRemoval> {
    live.iter()
        .filter_map(|(component, name)| {
            let cause = match rendered.iter().find(|(c, _)| c == component) {
                // The component is gone entirely: a rename or removal between
                // chart versions. This is the case the guard was built for.
                None => RemovalCause::ComponentGone,
                // The component survives under a DIFFERENT resource name. Helm
                // deletes the old object and creates the new one, so the
                // StatefulSet's volumes are orphaned and it comes up EMPTY.
                // Just as destructive, and far likelier: any curie.yaml that
                // does not reproduce the release's `nameOverride` renames every
                // object at once.
                //
                // Comparing components alone missed this, and comparing names
                // alone produced a 4-of-4 false positive on a release that
                // merely uses an override (#1323). Neither is the rule; the
                // rule is "same component, same name".
                Some((_, rendered_name)) if rendered_name != name => {
                    RemovalCause::RenamedTo(rendered_name.clone())
                }
                Some(_) => return None,
            };
            Some(StatefulRemoval {
                name: name.clone(),
                component: component.clone(),
                cause,
            })
        })
        .collect()
}

#[cfg(test)]
mod release_secret_name_tests {
    use super::*;

    /// The bug: `<release>-secrets` is only right when the release name
    /// contains the chart name. A default install renders
    /// `<release>-curie-secrets`, and every read silently found nothing.
    #[test]
    fn a_default_install_secret_is_found() {
        let listed = "t-curie-secrets\nsh.helm.release.v1.t.v1\n";
        assert_eq!(
            pick_release_secret(listed),
            Some("t-curie-secrets".to_string())
        );
    }

    /// The shape that hid the bug: with `nameOverride` equal to the release
    /// name, both forms collapse to the same string.
    #[test]
    fn a_name_override_install_secret_is_found() {
        assert_eq!(
            pick_release_secret("acme-bot-secrets\n"),
            Some("acme-bot-secrets".to_string())
        );
    }

    /// The collision the exclusion exists for. Per-agent connector Secrets
    /// carry the same release labels, so without it the selector could return
    /// one -- a confidently WRONG answer, which is worse than an empty one.
    #[test]
    fn a_connector_secret_is_never_mistaken_for_the_chart_secret() {
        let listed = "acme-bot-acme-bot-connector-secrets\n                      acme-bot-acme-dev-connector-secrets\n                      acme-bot-secrets\n";
        assert_eq!(
            pick_release_secret(listed),
            Some("acme-bot-secrets".to_string())
        );
    }

    /// Ordering must not decide it: the connector Secret sorting first is the
    /// realistic case, since kubectl lists alphabetically.
    #[test]
    fn ordering_does_not_change_the_answer() {
        let connector_first = "a-connector-secrets\nz-curie-secrets\n";
        assert_eq!(
            pick_release_secret(connector_first),
            Some("z-curie-secrets".to_string())
        );
    }

    /// An absent release must yield nothing, not a guess. The callers turn
    /// `None` into an actionable error naming their escape-hatch flag.
    #[test]
    fn no_matching_secret_yields_none() {
        assert_eq!(pick_release_secret(""), None);
        assert_eq!(pick_release_secret("sh.helm.release.v1.t.v1\n"), None);
        assert_eq!(pick_release_secret("only-connector-secrets\n"), None);
    }
}

#[cfg(test)]
mod chart_fullname_tests {
    use super::*;

    /// Every component the CLI derives from a release name. The chart names
    /// each one `{{ include "curie.fullname" . }}-<component>`
    /// (`charts/curie/templates/_helpers.tpl:16-26`), so this is the full set
    /// the sweep has to keep byte-identical for the default release.
    const COMPONENTS: [&str; 7] = [
        "api",
        "ui",
        "langfuse-web",
        "valkey",
        "dispatcher",
        "worker",
        "secrets",
    ];

    fn opts(release: &str, namespace: &str) -> CommonOpts {
        CommonOpts {
            namespace: namespace.into(),
            release: release.into(),
            dry_run: false,
        }
    }

    // --- the negative control -------------------------------------------------

    /// THE regression guard for the whole sweep.
    ///
    /// `"curie".contains("curie")` is true, so the chart's fullname for the
    /// default release is the release name itself and every derived resource
    /// name is exactly what the CLI built before this change. Every install
    /// anyone runs locally, in CI, and on the parity ladder uses `--release
    /// curie`; if this goes red the fix broke all of them.
    ///
    /// The expectation is a literal, never a second call to the rule under
    /// test -- comparing the rule against itself would pass whatever the rule
    /// happened to be.
    #[test]
    fn the_default_release_is_a_byte_identical_no_op() {
        for component in COMPONENTS {
            assert_eq!(
                chart_fullname("curie").resource(component),
                format!("curie-{component}"),
                "the default release must derive the same name it always did"
            );
        }
    }

    // --- the chart's naming rule ----------------------------------------------

    /// `contains`, not `starts_with` and not an exact match: the chart tests
    /// `contains $name .Release.Name`, so a release that merely embeds the
    /// chart name anywhere takes no suffix. A "stricter" reading here would
    /// diverge from what helm actually renders.
    #[test]
    fn a_release_containing_curie_takes_no_suffix() {
        assert_eq!(chart_fullname("curie").as_str(), "curie");
        assert_eq!(chart_fullname("curieish").as_str(), "curieish");
        assert_eq!(chart_fullname("my-curie-prod").as_str(), "my-curie-prod");
        assert_eq!(chart_fullname("curieish").resource("api"), "curieish-api");
    }

    /// The reported bug: `helm template platform charts/curie` renders
    /// `platform-curie-api`, and the CLI used to ask for `platform-api`.
    #[test]
    fn a_release_not_containing_curie_takes_the_chart_suffix() {
        assert_eq!(chart_fullname("platform").as_str(), "platform-curie");
        assert_eq!(chart_fullname("acme-prod").as_str(), "acme-prod-curie");
        assert_eq!(
            chart_fullname("platform").resource("api"),
            "platform-curie-api"
        );
        assert_eq!(
            chart_fullname("acme-prod").resource("worker"),
            "acme-prod-curie-worker"
        );
    }

    /// Helm applies `trunc 63` to the FULLNAME -- `printf "%s-%s" .Release.Name
    /// $name | trunc 63` -- and the template appends `-<component>` after that,
    /// so a rendered object name can exceed 63 characters. Truncating the
    /// joined string instead yields 63 and names an object the chart never
    /// created. This test is what pins that ordering.
    #[test]
    fn truncation_happens_before_the_component_suffix() {
        let release = "a".repeat(70);
        let fullname = chart_fullname(&release);

        assert_eq!(fullname.as_str(), "a".repeat(63));
        assert_eq!(fullname.as_str().len(), 63);

        let resource = fullname.resource("api");
        assert_eq!(resource, format!("{}-api", "a".repeat(63)));
        assert_eq!(
            resource.len(),
            67,
            "the component suffix is appended after truncation, so 63 + \"-api\""
        );
    }

    /// `trimSuffix "-"` runs after `trunc`, so a cut that lands on a dash
    /// leaves the object named `<...>-api`, never `<...>--api`.
    #[test]
    fn a_trailing_dash_after_truncation_is_trimmed() {
        // 62 `a`s and a dash: the fullname is `<62 a>--curie`, and the 63rd
        // character is the release's own trailing dash.
        let release = format!("{}-", "a".repeat(62));
        let fullname = chart_fullname(&release);

        assert_eq!(fullname.as_str(), "a".repeat(62));
        assert_eq!(fullname.resource("api"), format!("{}-api", "a".repeat(62)));
        assert!(
            !fullname.resource("api").contains("--"),
            "a doubled dash means the trim ran before the truncation"
        );
    }

    /// Sprig's `trimSuffix "-"` removes exactly ONE trailing dash;
    /// `str::trim_end_matches('-')` removes all of them. A release whose
    /// truncation boundary lands after a `--` is where the two diverge, and the
    /// CLI must follow the chart.
    ///
    /// Confirmed against helm rather than against a reading of Sprig. The
    /// release-name path cannot be rendered directly: helm rejects any release
    /// name longer than 53 characters, so no release can reach the 63-character
    /// cut. The `fullnameOverride` branch runs the byte-identical
    /// `| trunc 63 | trimSuffix "-"` pipeline and has no such limit, so
    ///
    ///   helm template platform charts/curie \
    ///     --set fullnameOverride=<61 a's>--<10 z's>
    ///
    /// renders the api Service as `<61 a's>--api` (66 characters): the cut left
    /// `<61 a's>--`, and exactly one dash was removed, leaving one behind for
    /// the component suffix to join to. That is the behavior pinned here.
    #[test]
    fn only_one_trailing_dash_is_trimmed_matching_helms_trimsuffix() {
        // 61 `a`s, then `--`, then filler so the fullname overruns 63. The
        // release does not contain "curie", so the chart appends the suffix and
        // the cut lands exactly on the second of the two dashes.
        let release = format!("{}--{}", "a".repeat(61), "z".repeat(10));
        let fullname = chart_fullname(&release);

        assert_eq!(
            fullname.as_str(),
            format!("{}-", "a".repeat(61)),
            "one dash is trimmed, not both: trimming both would name an object \
             the chart never rendered"
        );
        assert_eq!(fullname.as_str().len(), 62);
        assert!(
            !fullname.as_str().ends_with("--"),
            "the truncation must still have trimmed one dash"
        );

        // helm rendered `<61 a's>--api`: the retained dash plus the template's
        // own `-api`. A `trim_end_matches` implementation gives `<61 a's>-api`.
        assert_eq!(fullname.resource("api"), format!("{}--api", "a".repeat(61)));
        assert_eq!(fullname.resource("api").len(), 66);
    }
    /// Helm's `printf "%s-%s" "" "curie"` is `-curie`. Kubernetes rejects that
    /// name, which is the right place for the failure -- pin what the chart
    /// does rather than "improving" it here and diverging from the render.
    #[test]
    fn an_empty_release_still_produces_the_chart_name() {
        assert_eq!(chart_fullname("").as_str(), "-curie");
        assert_eq!(chart_fullname("").resource("api"), "-curie-api");
    }

    // --- the discovery parse --------------------------------------------------

    /// Discovery reads back the name of a Service selected by label, so the
    /// fullname is whatever precedes the component suffix. Both shapes are
    /// real: `platform-api` is an override install, `platform-curie-api` is
    /// the no-override chart rule.
    #[test]
    fn a_discovered_api_service_name_yields_the_fullname() {
        assert_eq!(
            fullname_from_resource_name("platform-api", "api"),
            Some("platform".to_string())
        );
        assert_eq!(
            fullname_from_resource_name("platform-curie-api", "api"),
            Some("platform-curie".to_string())
        );
    }

    /// A name that does not carry the suffix we asked for is not ours to
    /// truncate. Blind stripping would mint a confidently wrong fullname,
    /// which is worse than falling through to the chart rule.
    #[test]
    fn a_name_without_the_expected_suffix_is_rejected_not_stripped() {
        assert_eq!(fullname_from_resource_name("platform-ui", "api"), None);
        assert_eq!(fullname_from_resource_name("platformapi", "api"), None);
        assert_eq!(fullname_from_resource_name("api", "api"), None);
    }

    /// The jsonpath yields an empty string when the selector matches nothing.
    /// That must be `None` so the caller falls through to the worker probe and
    /// then to `chart_fullname`, rather than resolving to the empty fullname.
    #[test]
    fn empty_discovery_output_yields_none() {
        assert_eq!(fullname_from_resource_name("", "api"), None);
        assert_eq!(fullname_from_resource_name("", "worker"), None);
    }

    /// Step 2 of discovery: with `api.deploy=false` there is no api Service,
    /// but the worker Deployment still carries the release labels. It strips
    /// its OWN suffix, and must not accept a name carrying another one.
    #[test]
    fn the_worker_fallback_strips_its_own_suffix() {
        assert_eq!(
            fullname_from_resource_name("platform-worker", "worker"),
            Some("platform".to_string())
        );
        assert_eq!(
            fullname_from_resource_name("platform-curie-worker", "worker"),
            Some("platform-curie".to_string())
        );
        assert_eq!(fullname_from_resource_name("platform-api", "worker"), None);
    }

    // --- discovery cardinality ------------------------------------------------

    /// The ordinary case: one labelled object, one answer.
    #[test]
    fn exactly_one_match_yields_the_fullname() {
        assert_eq!(
            component_discovery("platform-curie-api\n", "api"),
            ComponentDiscovery::Found(chart_fullname("platform"))
        );
        assert_eq!(
            component_discovery("platform-api", "api"),
            ComponentDiscovery::Found(ReleaseFullname("platform".to_string()))
        );
    }

    /// Zero matches is absence, not failure: the caller falls through to the
    /// worker probe and then to the chart rule, and says nothing, because a
    /// not-yet-installed release is a supported state.
    #[test]
    fn zero_matches_is_not_present() {
        assert_eq!(
            component_discovery("", "api"),
            ComponentDiscovery::NotPresent
        );
        assert_eq!(
            component_discovery("\n  \n", "api"),
            ComponentDiscovery::NotPresent
        );

        let absent = ComponentDiscovery::NotPresent;
        let fallback = chart_fullname("platform");
        assert_eq!(
            absent.fallback_warning("platform-ns", "platform", &fallback),
            None,
            "an absent release must degrade quietly; only a FAILED probe warns"
        );
    }

    /// THE finding. Kubernetes does not enforce label uniqueness, so `items[0]`
    /// could name a workload that is not ours -- a stray `unexpected-api`
    /// carrying the release labels would resolve the fullname to `unexpected`
    /// and point `cluster message`/`comms`/`eval` at it. Two matches are
    /// refused, never resolved to the first.
    #[test]
    fn two_matches_are_refused_not_resolved_to_the_first() {
        let outcome = component_discovery("platform-curie-api\nunexpected-api\n", "api");
        match &outcome {
            ComponentDiscovery::Ambiguous { component, names } => {
                let listed: Vec<&str> = names.iter().map(String::as_str).collect();
                assert_eq!(component.as_str(), "api");
                assert_eq!(listed, ["platform-curie-api", "unexpected-api"]);
            }
            other => panic!("two matches must be refused, not resolved: {other:?}"),
        }
    }

    /// A single match that does not carry the component suffix is rejected, not
    /// blind-stripped -- the rule `fullname_from_resource_name` pins, asserted
    /// here at the level that actually decides what discovery returns.
    #[test]
    fn a_single_match_without_the_suffix_is_rejected_not_stripped() {
        assert_eq!(
            component_discovery("platform-ui\n", "api"),
            ComponentDiscovery::NotPresent
        );
        assert_eq!(
            component_discovery("api\n", "api"),
            ComponentDiscovery::NotPresent,
            "stripping `-api` off `api` leaves an empty fullname"
        );
    }

    /// A FAILED probe is not an absent release. It must warn, say why, and say
    /// that the name in use is a guess -- otherwise `cluster status` reports
    /// the Service as "not found" and masks an RBAC denial.
    #[test]
    fn a_failed_probe_warns_that_the_name_is_a_guess() {
        let denied = ComponentDiscovery::ProbeFailed {
            component: "api".to_string(),
            detail: "Error from server (Forbidden): services is forbidden".to_string(),
        };
        let fallback = chart_fullname("platform");
        let warning = denied
            .fallback_warning("platform-ns", "platform", &fallback)
            .expect("a failed probe must warn");

        assert!(warning.contains("FAILED"), "{warning}");
        assert!(warning.contains("Forbidden"), "{warning}");
        assert!(warning.contains("platform-ns"), "the namespace: {warning}");
        assert!(warning.contains("COMPUTED GUESS"), "{warning}");
        assert!(warning.contains("platform-curie-<component>"), "{warning}");
        assert!(
            warning.contains("nameOverride/fullnameOverride"),
            "{warning}"
        );
    }

    /// The ambiguity warning has to be actionable: the namespace, the selector,
    /// and every candidate name, so the operator can see the collision it has
    /// to resolve.
    #[test]
    fn the_ambiguity_warning_names_the_selector_and_the_candidates() {
        let ambiguous = ComponentDiscovery::Ambiguous {
            component: "api".to_string(),
            names: vec![
                "platform-curie-api".to_string(),
                "unexpected-api".to_string(),
            ],
        };
        let fallback = chart_fullname("platform");
        let warning = ambiguous
            .fallback_warning("platform-ns", "platform", &fallback)
            .expect("an ambiguous match must warn");

        assert!(warning.contains("platform-ns"), "{warning}");
        assert!(
            warning.contains(&component_selector("platform", "api")),
            "{warning}"
        );
        assert!(warning.contains("platform-curie-api"), "{warning}");
        assert!(warning.contains("unexpected-api"), "{warning}");
        assert!(warning.contains("COMPUTED GUESS"), "{warning}");
    }

    /// A resolved name wins from either probe; otherwise a PROBLEM outranks
    /// absence, so a denied api probe is not buried by an absent worker.
    #[test]
    fn a_probe_problem_outranks_an_absent_second_probe() {
        let denied = ComponentDiscovery::ProbeFailed {
            component: "api".to_string(),
            detail: "forbidden".to_string(),
        };
        let absent = ComponentDiscovery::NotPresent;
        let found = ComponentDiscovery::Found(chart_fullname("platform"));

        assert_eq!(
            preferred_probe_outcome(denied.clone(), absent.clone()),
            denied
        );
        assert_eq!(
            preferred_probe_outcome(absent.clone(), denied.clone()),
            denied
        );
        // A real answer from either probe still wins.
        assert_eq!(preferred_probe_outcome(denied, found.clone()), found);
        assert_eq!(
            preferred_probe_outcome(absent.clone(), absent.clone()),
            absent
        );
    }

    // --- the chart Secret's offline name --------------------------------------

    /// `release_secret_name_or_default` fell back to `format!("{release}-secrets")`,
    /// the raw chart-resource form #1533 names explicitly. A `platform` install
    /// renders `platform-curie-secrets`, so the old fallback had `migrate-store`
    /// stage a pod against a Secret that does not exist. The fallback is now the
    /// `secrets` resource of `chart_fullname`, pinned here as a literal rather
    /// than by re-deriving it.
    #[test]
    fn the_secret_fallback_uses_the_chart_rule_for_a_non_default_release() {
        assert_eq!(
            chart_fullname("platform").resource("secrets"),
            "platform-curie-secrets"
        );
        assert_eq!(
            chart_fullname("acme-prod").resource("secrets"),
            "acme-prod-curie-secrets"
        );
    }

    /// The negative control for that change: `"curie".contains("curie")`, so
    /// the default release's Secret name is byte-identical to the computed
    /// `format!("{release}-secrets")` it replaced. Every local, CI, and
    /// parity-ladder install uses `--release curie`.
    #[test]
    fn the_secret_fallback_is_byte_identical_for_the_default_release() {
        assert_eq!(chart_fullname("curie").resource("secrets"), "curie-secrets");
        assert_eq!(
            chart_fullname("my-curie-prod").resource("secrets"),
            "my-curie-prod-secrets"
        );
    }

    // --- rendered argv for a non-default release ------------------------------

    /// `cluster status` under a release that does not contain the chart name.
    /// The literals are what `helm template platform charts/curie` actually
    /// renders; before this change the CLI asked for `platform-ui` and
    /// `platform-langfuse-web`, which do not exist.
    #[test]
    fn status_queries_the_chart_rendered_services_for_a_non_default_release() {
        let o = opts("platform", "platform-ns");
        let lines: Vec<String> = status_commands(&o, &chart_fullname("platform"))
            .iter()
            .map(OpsCommand::display)
            .collect();

        assert!(
            lines.contains(&"kubectl get svc platform-curie-ui -n platform-ns -o json".to_string()),
            "status must query the chart-rendered ui Service: {lines:#?}"
        );
        assert!(
            lines.contains(
                &"kubectl get svc platform-curie-langfuse-web -n platform-ns -o json".to_string()
            ),
            "status must query the chart-rendered langfuse Service: {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("svc platform-ui ")
                || line.contains("svc platform-langfuse-web ")),
            "the raw release name must not reach a service query: {lines:#?}"
        );
    }

    /// `cluster observability` resolves the same two Services and had the same
    /// defect.
    #[test]
    fn observability_queries_the_chart_rendered_services_for_a_non_default_release() {
        let o = opts("platform", "platform-ns");
        let lines: Vec<String> = observability_commands(&o, &chart_fullname("platform"))
            .iter()
            .map(OpsCommand::display)
            .collect();

        assert!(
            lines.contains(&"kubectl get svc platform-curie-ui -n platform-ns -o json".to_string()),
            "observability must query the chart-rendered ui Service: {lines:#?}"
        );
        assert!(
            lines.contains(
                &"kubectl get svc platform-curie-langfuse-web -n platform-ns -o json".to_string()
            ),
            "observability must query the chart-rendered langfuse Service: {lines:#?}"
        );
        assert!(
            !lines.iter().any(|line| line.contains("svc platform-ui ")
                || line.contains("svc platform-langfuse-web ")),
            "the raw release name must not reach a service query: {lines:#?}"
        );
    }

    /// The default release renders the same argv it always has. The sibling
    /// `status_lists_the_readonly_commands` pins this at the argv level too;
    /// this is the observability half of that control.
    #[test]
    fn the_default_release_renders_the_same_service_argv_it_always_did() {
        let o = opts("curie", "curie");
        let lines: Vec<String> = observability_commands(&o, &chart_fullname("curie"))
            .iter()
            .map(OpsCommand::display)
            .collect();

        assert!(
            lines.contains(&"kubectl get svc curie-ui -n curie -o json".to_string()),
            "{lines:#?}"
        );
        assert!(
            lines.contains(&"kubectl get svc curie-langfuse-web -n curie -o json".to_string()),
            "{lines:#?}"
        );
    }
}

#[cfg(test)]
mod api_key_discovery_tests {

    // --- #1030: the worker Deployment lookup ---------------------------------

    #[test]
    fn the_worker_selector_does_not_guess_the_deployment_name() {
        // The chart names it `{{ curie.fullname }}-worker`, which equals
        // `<release>-worker` only when the release name contains the chart name.
        // `acme-prod` renders `acme-prod-curie-worker`, and nameOverride moves it
        // again. Selecting on labels is what `release_secret_name` already does.
        let selector = worker_deployment_selector("acme-prod");
        assert!(selector.contains("app.kubernetes.io/instance=acme-prod"));
        assert!(selector.contains("app.kubernetes.io/component=worker"));
        assert!(
            !selector.contains("acme-prod-worker"),
            "the selector must not encode a guessed name: {selector}"
        );
    }

    #[test]
    fn a_failed_lookup_is_unknown_and_never_reads_as_real_slack() {
        // The distinction that keeps #1030 from returning in another shape. A
        // kubectl failure is not evidence that the worker talks to real Slack, and
        // treating it as such posts a real token wherever real Slack is while the
        // worker edits through a proxy the CLI never saw.
        assert_eq!(parse_slack_api_base(false, ""), SlackApiBase::Unknown);
        assert_eq!(
            parse_slack_api_base(false, "https://proxy.example/api"),
            SlackApiBase::Unknown
        );
    }

    #[test]
    fn an_empty_successful_lookup_means_real_slack() {
        // The chart renders SLACK_API_BASE_URL only when worker.slackApiBaseUrl is
        // non-empty, so a clean empty result is the ordinary case, not a failure.
        assert_eq!(parse_slack_api_base(true, ""), SlackApiBase::RealSlack);
        assert_eq!(parse_slack_api_base(true, "  \n "), SlackApiBase::RealSlack);
    }

    #[test]
    fn a_configured_base_is_returned_trimmed() {
        assert_eq!(
            parse_slack_api_base(true, "  https://proxy.example/api \n"),
            SlackApiBase::Configured("https://proxy.example/api".to_string())
        );
    }

    #[test]
    fn two_containers_reporting_a_base_is_unknown_not_a_coin_flip() {
        // Cannot happen in this chart today. If it ever does, picking one half is
        // exactly the ambiguity this issue is about, so say so instead.
        assert_eq!(
            parse_slack_api_base(true, "https://a/api\nhttps://b/api\n"),
            SlackApiBase::Unknown
        );
    }
    use super::*;

    struct EnvRestore {
        path: Option<std::ffi::OsString>,
        requested: Option<std::ffi::OsString>,
        requested_default: Option<std::ffi::OsString>,
        all: Option<std::ffi::OsString>,
        all_default: Option<std::ffi::OsString>,
        all_forbidden: Option<std::ffi::OsString>,
        log: Option<std::ffi::OsString>,
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (name, previous) in [
                ("PATH", &self.path),
                ("CURIE_TEST_HELM_REQUESTED", &self.requested),
                ("CURIE_TEST_HELM_REQUESTED_DEFAULT", &self.requested_default),
                ("CURIE_TEST_HELM_ALL", &self.all),
                ("CURIE_TEST_HELM_ALL_DEFAULT", &self.all_default),
                ("CURIE_TEST_HELM_ALL_FORBIDDEN", &self.all_forbidden),
                ("CURIE_TEST_HELM_LOG", &self.log),
            ] {
                match previous {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        }
    }

    fn write_executable(path: &std::path::Path, body: &str) {
        std::fs::write(path, body).expect("write fake cluster executable");
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = std::fs::metadata(path)
            .expect("read fake cluster executable metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(path, permissions).expect("make fake cluster executable runnable");
    }

    fn install_cluster_diagnosis_tools(tools: &std::path::Path) -> EnvRestore {
        write_executable(
            &tools.join("kubectl"),
            r#"#!/bin/sh
case "$*" in
  *"get secret -l app.kubernetes.io/instance=curie"*)
    printf '%s\n' 'curie-secrets'
    ;;
  *"get secret curie-secrets"*)
    printf '%s\n' 'Error from server (NotFound): secrets "curie-secrets" not found' >&2
    exit 1
    ;;
  *)
    printf 'unexpected kubectl invocation: %s\n' "$*" >&2
    exit 64
    ;;
esac
"#,
        );
        write_executable(
            &tools.join("helm"),
            r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_HELM_LOG"
case "$*" in
  *"-n curie"*)
    case "$*" in
      *"--all"*) printf '%s\n' "$CURIE_TEST_HELM_REQUESTED" ;;
      *) printf '%s\n' "${CURIE_TEST_HELM_REQUESTED_DEFAULT:-$CURIE_TEST_HELM_REQUESTED}" ;;
    esac
    ;;
  *)
    if [ "${CURIE_TEST_HELM_ALL_FORBIDDEN:-}" = 1 ]; then
      printf '%s\n' 'forbidden: cannot list releases across namespaces' >&2
      exit 1
    fi
    case "$*" in
      *"--all"*) printf '%s\n' "$CURIE_TEST_HELM_ALL" ;;
      *) printf '%s\n' "${CURIE_TEST_HELM_ALL_DEFAULT:-$CURIE_TEST_HELM_ALL}" ;;
    esac
    ;;
esac
"#,
        );

        let restore = EnvRestore {
            path: std::env::var_os("PATH"),
            requested: std::env::var_os("CURIE_TEST_HELM_REQUESTED"),
            requested_default: std::env::var_os("CURIE_TEST_HELM_REQUESTED_DEFAULT"),
            all: std::env::var_os("CURIE_TEST_HELM_ALL"),
            all_default: std::env::var_os("CURIE_TEST_HELM_ALL_DEFAULT"),
            all_forbidden: std::env::var_os("CURIE_TEST_HELM_ALL_FORBIDDEN"),
            log: std::env::var_os("CURIE_TEST_HELM_LOG"),
        };
        let mut path = vec![tools.to_path_buf()];
        path.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        std::env::set_var("PATH", std::env::join_paths(path).expect("join test PATH"));
        restore
    }

    fn assert_state_was_read(log: &std::path::Path) {
        let invocations = std::fs::read_to_string(log).expect("read Helm invocation log");
        assert!(
            invocations
                .lines()
                .any(|line| line == "list -n curie --all -o json"),
            "the requested release state was not read: {invocations}"
        );
        assert!(
            invocations
                .lines()
                .any(|line| line == "list -A --all -o json"),
            "the all namespace release state was not read: {invocations}"
        );
    }

    #[tokio::test]
    async fn api_key_failure_names_a_deployed_same_name_release_in_another_namespace() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"failed"}]"#,
        );
        std::env::set_var(
            "CURIE_TEST_HELM_ALL",
            r#"[{"name":"curie","namespace":"curie","status":"failed"},{"name":"curie","namespace":"healthy","status":"deployed"}]"#,
        );

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("an unreadable secret must not yield an API key");
        let message = error.to_string();

        assert_eq!(
            crate::exit::classify(&error).0,
            crate::exit::ExitClass::Usage,
            "release state guidance must preserve the command's usage exit"
        );
        assert!(
            message.contains("failed"),
            "missing requested state: {message}"
        );
        assert!(
            message.contains("healthy"),
            "missing deployed alternate namespace: {message}"
        );
        assert!(
            !message.contains("--api-key") && !message.contains("CURIE_API_KEY"),
            "a failed release cannot be repaired by supplying its key: {message}"
        );
        assert_state_was_read(&log);
    }

    #[tokio::test]
    async fn api_key_failure_without_a_deployed_alternate_does_not_offer_a_key_remedy() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"failed"}]"#,
        );
        std::env::set_var(
            "CURIE_TEST_HELM_ALL",
            r#"[{"name":"curie","namespace":"curie","status":"failed"}]"#,
        );

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("a failed release with no healthy alternate must fail");
        let message = error.to_string();

        assert!(
            message.contains("failed"),
            "missing requested state: {message}"
        );
        assert!(
            !message.contains("--api-key") && !message.contains("CURIE_API_KEY"),
            "a failed release cannot be repaired by supplying its key: {message}"
        );
        assert_state_was_read(&log);
    }

    #[tokio::test]
    async fn deployed_release_key_failure_does_not_require_all_namespace_access() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"deployed"}]"#,
        );
        std::env::set_var("CURIE_TEST_HELM_ALL", "[]");

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("an unreadable secret must not yield an API key");
        let message = error.to_string();

        assert_eq!(
            crate::exit::classify(&error).0,
            crate::exit::ExitClass::Usage,
            "a missing deployed release key remains a usage error"
        );
        assert!(
            message.contains("--api-key"),
            "missing flag remedy: {message}"
        );
        assert!(
            message.contains("CURIE_API_KEY"),
            "missing environment remedy: {message}"
        );

        let invocations = std::fs::read_to_string(&log).expect("read Helm invocation log");
        assert!(
            invocations
                .lines()
                .any(|line| line == "list -n curie --all -o json"),
            "the requested release state was not read: {invocations}"
        );
        assert!(
            !invocations
                .lines()
                .any(|line| line == "list -A --all -o json"),
            "cluster wide Helm access is forbidden once the requested release is deployed: {invocations}"
        );
    }

    #[tokio::test]
    async fn pending_upgrade_is_read_instead_of_reported_as_a_missing_release() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"pending-upgrade"}]"#,
        );
        std::env::set_var("CURIE_TEST_HELM_REQUESTED_DEFAULT", "[]");
        std::env::set_var(
            "CURIE_TEST_HELM_ALL",
            r#"[{"name":"curie","namespace":"curie","status":"pending-upgrade"}]"#,
        );
        std::env::set_var("CURIE_TEST_HELM_ALL_DEFAULT", "[]");

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("a pending release must not yield an API key");
        let message = error.to_string();

        assert!(
            message.contains("pending-upgrade"),
            "pending Helm state was hidden: {message}"
        );
        assert!(
            !message.contains("no deployed release named")
                && !message.contains("deploy the release"),
            "pending state was mistaken for a missing release: {message}"
        );
        assert!(
            !message.contains("--api-key") && !message.contains("CURIE_API_KEY"),
            "a pending release cannot be repaired by configuring its key: {message}"
        );
        assert_state_was_read(&log);
    }

    #[tokio::test]
    async fn failed_release_state_survives_a_forbidden_all_namespace_scan() {
        let _lock = crate::PROCESS_ENV_LOCK.lock().await;
        let tools = tempfile::tempdir().expect("create fake cluster tools");
        let _restore = install_cluster_diagnosis_tools(tools.path());
        let log = tools.path().join("helm.log");
        std::env::set_var("CURIE_TEST_HELM_LOG", &log);
        std::env::set_var(
            "CURIE_TEST_HELM_REQUESTED",
            r#"[{"name":"curie","namespace":"curie","status":"failed"}]"#,
        );
        std::env::set_var("CURIE_TEST_HELM_ALL", "[]");
        std::env::set_var("CURIE_TEST_HELM_ALL_FORBIDDEN", "1");

        let error = discover_api_key("curie", "curie")
            .await
            .expect_err("a failed release must not yield an API key");
        let message = error.to_string();
        let (class, fix) = crate::exit::classify(&error);

        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(
            message.contains("failed"),
            "known requested release state was discarded: {message}"
        );
        assert!(
            !message.contains("could not inspect Helm state across namespaces"),
            "the optional namespace scan replaced known state: {message}"
        );
        assert!(
            !message.contains("--api-key") && !message.contains("CURIE_API_KEY"),
            "a failed release cannot be repaired by configuring its key: {message}"
        );
        assert!(
            fix.as_deref()
                .is_some_and(|guidance| guidance.contains("curie cluster status")),
            "missing cluster status guidance: {fix:?}"
        );
    }
}

#[cfg(test)]
mod sealing_preservation_tests {
    use super::*;

    fn values(json: serde_json::Value) -> serde_json::Value {
        json
    }

    fn get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
        pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// The catastrophic case. A plain `cluster up` drops anything it does not
    /// re-pass, and dropping this key makes every sealed credential in every
    /// agent repository permanently unreadable.
    #[test]
    fn an_upgrade_re_supplies_the_existing_key_unchanged() {
        let existing = values(serde_json::json!({
            "sealing": {"privateKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="}
        }));
        let resolved = resolve_sealing_values(Some(&existing), &[]);
        assert_eq!(
            get(&resolved, crate::sealing::SEALING_PRIVATE_KEY),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            "the recorded key must come back byte-identical"
        );
    }

    /// And it must be reachable through the composed set `up` actually calls,
    /// not only through the family function. Unit-testing the family alone left
    /// the WIRING uncovered for the comms and App families before (#1256).
    #[test]
    fn the_composed_preserved_set_carries_the_sealing_key() {
        let existing = values(serde_json::json!({
            "sealing": {"privateKey": "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB="}
        }));
        let all = resolve_preserved_values(Some(&existing), &[]);
        assert_eq!(
            get(&all, crate::sealing::SEALING_PRIVATE_KEY),
            Some("BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB=")
        );
    }

    /// The seam that actually broke, closed by construction.
    ///
    /// `up` re-supplies a set of keys; `diff` decides which keys survive an
    /// apply via `is_preserved_by_up`. Those two answers must be the same
    /// answer. They were kept in step by a hand-written list, and sealing was
    /// added to one and not the other -- so `curie diff` against a real 0.6.0
    /// release announced `sealing.privateKey (reset to chart default)` for a
    /// key apply hands straight back.
    ///
    /// This drives the REAL resolver over a release carrying every preserved
    /// family and asserts `diff` agrees about every key it returns. A future
    /// family added to `resolve_preserved_values` alone fails here instead of
    /// reaching an operator as a false alarm about their own data.
    #[test]
    fn diff_agrees_with_every_key_up_actually_re_supplies() {
        let existing = values(serde_json::json!({
            "sealing": {
                "privateKey": "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC=",
                "previousPrivateKey": "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD="
            },
            "dispatcher": {"slack": {"appToken": "xapp-EXAMPLE", "botToken": "xoxb-EXAMPLE"}},
            "api": {
                "githubAppId": "1",
                "githubAppExistingSecret": "an-app-secret",
                "githubAppExistingSecretKey": "privateKey"
            }
        }));
        let supplied = resolve_preserved_values(Some(&existing), &[]);
        assert!(
            !supplied.is_empty(),
            "fixture must exercise the resolver, not pass vacuously"
        );
        let disagreements: Vec<&str> = supplied
            .iter()
            .map(|(k, _)| k.as_str())
            .filter(|k| !is_preserved_by_up(k))
            .collect();
        assert!(
            disagreements.is_empty(),
            "`up` re-supplies these keys but `is_preserved_by_up` says apply drops them, \
             so `curie diff` would report a reset that never happens: {disagreements:?}"
        );
    }

    /// A fresh install gets a usable key, or the feature never starts.
    #[test]
    fn a_fresh_install_generates_a_usable_key() {
        let resolved = resolve_sealing_values(None, &[]);
        let key = get(&resolved, crate::sealing::SEALING_PRIVATE_KEY).expect("generated");
        let public = crate::sealing::public_key_of(key).expect("a real keypair");
        let blob = crate::sealing::seal(&public, "value").expect("seals");
        assert_eq!(
            crate::sealing::open_with_any(&[key.to_string()], &blob).unwrap(),
            "value"
        );
    }

    /// An existing release with no key gains one -- this is how an install that
    /// predates the feature starts using it. Safe precisely because no key
    /// means nothing has ever been sealed to this cluster.
    #[test]
    fn an_existing_release_without_a_key_gains_one() {
        let existing = values(serde_json::json!({"ui": {"deploy": false}}));
        let resolved = resolve_sealing_values(Some(&existing), &[]);
        assert!(get(&resolved, crate::sealing::SEALING_PRIVATE_KEY).is_some());
    }

    /// The previous key is preserved when present, so a rotation overlap
    /// survives the upgrades that happen during it.
    #[test]
    fn a_rotation_in_progress_keeps_both_keys() {
        let existing = values(serde_json::json!({"sealing": {
            "privateKey": "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC=",
            "previousPrivateKey": "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD="
        }}));
        let resolved = resolve_sealing_values(Some(&existing), &[]);
        assert_eq!(
            get(&resolved, crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY),
            Some("DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD=")
        );
    }

    /// Never invented: a previous key claims a rotation happened.
    #[test]
    fn a_previous_key_is_never_generated() {
        for existing in [None, Some(values(serde_json::json!({})))] {
            let resolved = resolve_sealing_values(existing.as_ref(), &[]);
            assert!(
                get(&resolved, crate::sealing::SEALING_PREVIOUS_PRIVATE_KEY).is_none(),
                "an overlap that never happened must not be claimed"
            );
        }
    }

    /// An operator `--set` wins, as it does for every other managed family.
    #[test]
    fn an_operator_set_is_not_overridden() {
        let sets = vec![format!("{}=mine", crate::sealing::SEALING_PRIVATE_KEY)];
        let resolved = resolve_sealing_values(None, &sets);
        assert!(get(&resolved, crate::sealing::SEALING_PRIVATE_KEY).is_none());
    }
}

#[cfg(test)]
mod stateful_guard_tests {
    use super::*;

    fn render(component: &str, name: &str) -> String {
        format!(
            "apiVersion: apps/v1\nkind: StatefulSet\nmetadata:\n  name: {name}\nspec:\n  \
             selector:\n    matchLabels:\n      app.kubernetes.io/component: {component}\n"
        )
    }

    fn pair(component: &str, name: &str) -> (String, String) {
        (component.to_string(), name.to_string())
    }

    #[test]
    fn components_and_names_both_come_from_the_render() {
        let rendered = format!(
            "{}\n---\n{}",
            render("rustfs", "acme-bot-curie-rustfs"),
            render("postgres", "acme-bot-curie-postgres")
        );
        assert_eq!(
            parse_statefulset_components(&rendered),
            vec![
                pair("rustfs", "acme-bot-curie-rustfs"),
                pair("postgres", "acme-bot-curie-postgres"),
            ]
        );
    }

    /// `kind: StatefulSet` inside a ConfigMap payload is data, not a component.
    #[test]
    fn a_kind_line_inside_another_document_is_not_a_component() {
        let rendered = "\
apiVersion: v1
kind: ConfigMap
metadata:
  name: acme-bot-docs
data:
  example: |
    kind: StatefulSet
    metadata:
      name: not-a-real-component
";
        assert!(parse_statefulset_components(rendered).is_empty());
    }

    /// The original case: the release runs minio, the chart renders rustfs.
    #[test]
    fn a_renamed_component_is_reported_as_a_removal() {
        let live = vec![
            pair("minio", "acme-bot-minio"),
            pair("postgres", "acme-bot-postgres"),
        ];
        let rendered = vec![
            pair("rustfs", "acme-bot-rustfs"),
            pair("postgres", "acme-bot-postgres"),
        ];
        let removed = removed_stateful_components(&live, &rendered);
        assert_eq!(
            removed,
            vec![StatefulRemoval {
                name: "acme-bot-minio".to_string(),
                component: "minio".to_string(),
                cause: RemovalCause::ComponentGone,
            }]
        );
    }

    /// The case a live `apply --dry-run` exposed, and the reason this rule is
    /// not "same component".
    ///
    /// A curie.yaml that does not reproduce the release's `nameOverride`
    /// renames EVERY object. The component labels are identical either way, so
    /// a component-only comparison sees nothing wrong -- while helm deletes
    /// `acme-bot-postgres` and creates an empty `acme-bot-curie-postgres`
    /// beside the orphaned volume. All four data stores, silently.
    #[test]
    fn the_same_component_under_a_new_name_is_still_a_deletion() {
        let live = vec![
            pair("clickhouse", "acme-bot-clickhouse"),
            pair("postgres", "acme-bot-postgres"),
            pair("rustfs", "acme-bot-rustfs"),
            pair("valkey", "acme-bot-valkey"),
        ];
        // What the chart renders when nameOverride is absent from the file.
        let rendered = vec![
            pair("clickhouse", "acme-bot-curie-clickhouse"),
            pair("postgres", "acme-bot-curie-postgres"),
            pair("rustfs", "acme-bot-curie-rustfs"),
            pair("valkey", "acme-bot-curie-valkey"),
        ];
        let removed = removed_stateful_components(&live, &rendered);
        assert_eq!(
            removed.len(),
            4,
            "every store is deleted and recreated empty; none may be missed"
        );
        // The cause is what steers the operator to `nameOverride` instead of
        // `--migrate-store`, so it is part of the answer, not a detail.
        assert_eq!(
            removed[1].cause,
            RemovalCause::RenamedTo("acme-bot-curie-postgres".to_string())
        );
    }

    /// And the false positive #1323 fixed must not come back: matching names
    /// under matching components is not a removal.
    #[test]
    fn an_unchanged_component_set_is_not_a_removal() {
        let live = vec![pair("postgres", "r-postgres"), pair("minio", "r-minio")];
        let rendered = vec![pair("postgres", "r-postgres"), pair("minio", "r-minio")];
        assert!(removed_stateful_components(&live, &rendered).is_empty());
    }

    #[test]
    fn a_new_component_is_not_a_removal() {
        let live = vec![pair("postgres", "r-postgres")];
        let rendered = vec![
            pair("postgres", "r-postgres"),
            pair("clickhouse", "r-clickhouse"),
        ];
        assert!(removed_stateful_components(&live, &rendered).is_empty());
    }

    /// Source: https://github.com/helm/helm/blob/v3.16.4/pkg/kube/client.go#L452-L455
    /// Helm upgrade honors only the exact lowercase `keep` value.
    #[test]
    fn helm_upgrade_exact_keep_annotation_is_not_at_risk() {
        let kept = serde_json::json!({
            "metadata": {"annotations": {"helm.sh/resource-policy": "keep"}}
        });
        assert!(helm_keeps(&kept), "exact lowercase keep must be accepted");

        for value in ["Keep", "KEEP", " keep "] {
            let kept = serde_json::json!({
                "metadata": {"annotations": {"helm.sh/resource-policy": value}}
            });
            assert!(!helm_keeps(&kept), "{value:?} must not read as exact keep");
        }
    }

    #[test]
    fn other_resource_policies_do_not_count_as_kept() {
        for value in ["delete", "", "keepalive"] {
            let r = serde_json::json!({
                "metadata": {"annotations": {"helm.sh/resource-policy": value}}
            });
            assert!(!helm_keeps(&r), "{value:?} must not read as keep");
        }
        assert!(!helm_keeps(&serde_json::json!({"metadata": {}})));
    }

    #[test]
    fn a_kept_component_is_excluded_from_the_at_risk_list() {
        let list = serde_json::json!({"items": [
            {
                "metadata": {
                    "name": "acme-bot-minio",
                    "annotations": {"helm.sh/resource-policy": "keep"}
                },
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "minio"}}}
            },
            {
                "metadata": {"name": "acme-bot-postgres"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "postgres"}}}
            }
        ]});
        assert_eq!(
            stateful_components_from_list(&list),
            vec![pair("postgres", "acme-bot-postgres")]
        );
    }

    #[test]
    fn an_unannotated_component_is_still_at_risk() {
        let list = serde_json::json!({"items": [
            {
                "metadata": {"name": "acme-bot-minio"},
                "spec": {"selector": {"matchLabels": {"app.kubernetes.io/component": "minio"}}}
            }
        ]});
        assert_eq!(
            stateful_components_from_list(&list),
            vec![pair("minio", "acme-bot-minio")]
        );
    }
}

/// The user supplied values Helm recorded for a release. `None` means Helm
/// positively reported that the release does not exist. Read failures and
/// malformed values fail closed before callers can plan a mutation or diff.
/// This is the read only half of [`fetch_existing_values`], exposed for
/// `curie diff`.
pub async fn fetch_release_values(o: &CommonOpts) -> Result<Option<serde_json::Value>> {
    fetch_existing_values(o).await
}

/// The **computed** values of an existing release: the chart's own defaults
/// merged with whatever the operator overrode.
///
/// That merge is the whole difference from [`fetch_release_values`], which
/// reports only what the operator supplied. An operator who ran `curie cluster
/// up` and never set a model supplied nothing, so helm recorded nothing, and
/// the user-supplied read cannot see the chart default the sandboxes actually
/// boot. `--all` is the only way to observe a default nobody supplied (#1950).
///
/// `None` only when Helm positively reports the release does not exist; read
/// failures, malformed JSON, and non-object/non-null shapes fail closed.
pub async fn fetch_release_computed_values(o: &CommonOpts) -> Result<Option<serde_json::Value>> {
    fetch_helm_values(o, helm_get_all_values_cmd(o), "computed Helm values").await
}

/// Resolve [`GITHUB_TOKEN_KEY`] for this run.
///
/// - `flag` is the `--github-token` value: `None` when the flag and
///   `CURIE_GITHUB_TOKEN` are both unset.
/// - `clear` is `--clear-github-token`.
///
/// Precedence, and why:
/// 1. An operator `--set api.githubToken=` wins outright and we supply nothing,
///    matching every other secret (`operator_set_secret_is_left_to_the_operator`).
///    Passing BOTH is rejected earlier by [`check_github_token_conflict`].
/// 2. An explicit clear removes it.
/// 3. An explicit non-empty value replaces it.
/// 4. Otherwise preserve whatever helm recorded -- so a plain `cluster up`
///    (which does a FULL upgrade and drops anything it does not re-pass) cannot
///    silently reset a working credential to the chart's empty default.
///
/// An EMPTY explicit value is state 4, not state 2. An exported-but-empty
/// `CURIE_GITHUB_TOKEN` is a routine shell accident, and letting an ambiguous
/// signal destroy a live credential is the wrong failure direction; destroying
/// requires the unambiguous `--clear-github-token`.
///
/// Pure and non-interactive, like the resolvers around it.
fn resolve_github_token(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
    flag: Option<&str>,
    clear: bool,
) -> GithubTokenPlan {
    if operator_set_keys(operator_sets).contains(GITHUB_TOKEN_KEY) {
        return GithubTokenPlan::Untouched;
    }
    if clear {
        return GithubTokenPlan::Clear;
    }
    if let Some(value) = flag.filter(|v| !v.is_empty()) {
        return GithubTokenPlan::Set(value.to_string());
    }
    match preserved_value(existing, GITHUB_TOKEN_KEY) {
        Some(current) => GithubTokenPlan::Set(current),
        None => GithubTokenPlan::Untouched,
    }
}

fn complete_up_opts_without_runner_egress(
    mut opts: UpOpts,
    existing: Option<&serde_json::Value>,
    github_token: Option<&str>,
    clear_github_token: bool,
) -> Result<UpOpts> {
    let operator_sets = opts.operator_sets();
    opts.retained_mail_values = resolve_retained_mail_values(existing, &operator_sets)?;
    resolve_preserved_runner_identity_values(&mut opts, existing, &operator_sets);
    resolve_preserved_gvisor_mode_value(&mut opts, existing, &operator_sets);
    resolve_preserved_worker_extra_env_values(&mut opts, existing, &operator_sets);
    resolve_preserved_slack_trusted_origins_value(&mut opts, existing, &operator_sets);
    if !opts.dev {
        opts.secrets = resolve_generated_secrets(existing, &operator_sets)?;
        opts.secrets.extend(resolve_managed_values_for_up(
            existing,
            &operator_sets,
            opts.common.dry_run,
        ));
    } else {
        // `--dev` keeps the chart's published credential defaults (#195) and
        // must not mint a sealing key, but it is still a FULL helm upgrade:
        // values a sibling verb recorded have to be re-supplied or they reset
        // to the empty chart default (#1134, #1125). Preserve, never invent.
        opts.secrets
            .extend(resolve_preserved_values(existing, &operator_sets));
    }
    opts.github_token =
        resolve_github_token(existing, &operator_sets, github_token, clear_github_token);
    Ok(opts)
}

/// Finish an already validated up plan with the one live values read and, when
/// requested, resolved provider addresses. This is kept separate from command
/// execution so apply and diff can compare the same completed values.
pub(crate) fn complete_up_opts(
    opts: UpOpts,
    existing: Option<&serde_json::Value>,
    github_token: Option<&str>,
    clear_github_token: bool,
    resolve_provider_egress: bool,
) -> Result<UpOpts> {
    let mut opts =
        complete_up_opts_without_runner_egress(opts, existing, github_token, clear_github_token)?;
    let operator_sets = opts.operator_sets();
    resolve_preserved_runner_egress_values(&mut opts, existing, &operator_sets, false);
    resolve_provider_egress_for_up(&mut opts, resolve_provider_egress)?;
    Ok(opts)
}

/// Whether an operator `--set` assigns a NON-EMPTY value to
/// [`GITHUB_TOKEN_KEY`], i.e. whether the complete token is riding in argv.
///
/// The pass-through stays legal (it is verbatim by design and breaking it would
/// break existing operators), but a non-empty one leaks into the process table
/// and shell history, so `up` steers the operator to the private input. An EMPTY
/// assignment is the operator clearing the key by hand:
/// nothing leaks, so warning about it would be noise on a correct command.
/// Reads the same [`operator_set_entries`] parse the rest of this file does,
/// since helm accepts the comma-joined `a=1,b=2` form.
///
/// The trims are asymmetric on purpose. Whitespace AROUND an assignment is
/// shell noise (`--set " api.githubToken=x "` still leaks a token), so it is
/// trimmed off both ends; whitespace INSIDE the key is not (`api.githubToken =x`
/// assigns a differently-named helm key, which this credential does not ride
/// on), so a key is compared with its leading noise removed and nothing else.
fn set_passthrough_leaks_github_token(set: &[String]) -> bool {
    operator_set_entries(set)
        .into_iter()
        .any(|(key, value)| key.trim_start() == GITHUB_TOKEN_KEY && !value.trim_end().is_empty())
}

/// Decide which [`REQUIRED_SECRETS`] values `cluster up` supplies, and how.
///
/// - `existing` is `Some(user-supplied values JSON)` when the release already
///   exists (from `helm get values -o json`), `None` on a fresh install.
/// - An operator `--set <key>=...` for a secret always wins: we supply nothing
///   for it.
/// - Fresh install: generate a strong random for every remaining key.
/// - Existing release: re-supply exactly the value helm already recorded for a
///   key (so a `helm upgrade` never rotates a live store's credential -- the
///   chart has no `lookup`-persist yet, that is #195), and never mint a new one
///   for a key helm has no record of (leaving a pre-existing release on
///   whatever it already booted with rather than rotating it out from under a
///   running data store).
///
/// Pure and non-interactive by construction: it never reads a TTY, so a
/// non-interactive / CI `cluster up` cannot hang here.
fn resolve_generated_secrets(
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Result<Vec<(String, String)>> {
    let overridden = operator_set_keys(operator_sets);
    let mut resolved = Vec::new();
    for (key, len) in REQUIRED_SECRETS {
        if overridden.contains(*key) {
            continue;
        }
        match existing {
            Some(values) => {
                if let Some(current) = preserved_value(Some(values), key) {
                    resolved.push(((*key).to_string(), current));
                }
            }
            None => resolved.push(((*key).to_string(), random_hex(*len)?)),
        }
    }
    Ok(resolved)
}

/// `helm get values <release> -n <ns> [--all] -o json`. `all` is the only
/// difference between the two wrappers below, and it is load bearing rather
/// than stylistic: without it helm reports only what the operator supplied.
fn helm_get_values_cmd_with(o: &CommonOpts, all: bool) -> OpsCommand {
    let mut args = vec![
        plain("get"),
        plain("values"),
        plain(&o.release),
        plain("-n"),
        plain(&o.namespace),
    ];
    if all {
        args.push(plain("--all"));
    }
    args.push(plain("-o"));
    args.push(plain("json"));
    OpsCommand::new("helm", args)
}

/// `helm get values <release> -n <ns> -o json`: helm's record of the values a
/// prior install supplied. `cluster up` reads it back so an upgrade re-supplies
/// the same generated secrets instead of rotating them.
fn helm_get_values_cmd(o: &CommonOpts) -> OpsCommand {
    helm_get_values_cmd_with(o, false)
}

/// `helm get values <release> -n <ns> --all -o json`: the COMPUTED values --
/// chart defaults merged with the operator's overrides. The sibling above
/// reports only what the operator supplied, which is why `--all` is load
/// bearing here and not a stylistic difference.
fn helm_get_all_values_cmd(o: &CommonOpts) -> OpsCommand {
    helm_get_values_cmd_with(o, true)
}

/// Whether Helm positively reported that the requested release does not exist.
/// Other failures must remain errors because the release state is unknown.
fn helm_release_is_absent(stderr: &str) -> bool {
    failure_reason(stderr) == "Error: release: not found"
}

/// The user supplied values of an existing release, or `None` only when Helm
/// positively reports that the release does not exist. A valid JSON object or
/// `null` is returned as `Some`; failed reads, malformed JSON, and other JSON
/// shapes fail closed. Helm prints `null` for an existing release with no user
/// supplied values.
async fn fetch_existing_values(o: &CommonOpts) -> Result<Option<serde_json::Value>> {
    fetch_helm_values(o, helm_get_values_cmd(o), "Helm values").await
}

/// The shared read for both `helm get values` shapes. `what` names the read in
/// the operator facing message ("Helm values" or "computed Helm values") and is
/// the only thing that varies between callers; the absent-release, connectivity
/// vs failure, malformed-JSON and non-object ladders are deliberately single
/// sourced so the fail-closed contract cannot drift between them.
async fn fetch_helm_values(
    o: &CommonOpts,
    cmd: OpsCommand,
    what: &str,
) -> Result<Option<serde_json::Value>> {
    let (ok, out, err) = run_capture(&cmd).await?;
    let fix = format!(
        "verify the release and cluster access with `helm status {} -n {}`, then retry",
        o.release, o.namespace
    );
    if !ok {
        if helm_release_is_absent(&err) {
            return Ok(None);
        }
        let reason = failure_reason(&err);
        let message = format!(
            "could not read {what} for release {} in namespace {}: {reason}",
            o.release, o.namespace
        );
        let error = if is_connectivity_failure(&err) {
            crate::exit::CliError::transient(message)
        } else {
            crate::exit::CliError::failure(message)
        };
        return Err(error.with_fix(fix).into());
    }

    let values: serde_json::Value = serde_json::from_str(out.trim()).map_err(|error| {
        crate::exit::CliError::failure(format!(
            "could not read {what} for release {} in namespace {}: malformed Helm values JSON ({error})",
            o.release, o.namespace
        ))
        .with_fix(fix.clone())
    })?;
    if !values.is_object() && !values.is_null() {
        return Err(crate::exit::CliError::failure(format!(
            "could not read {what} for release {} in namespace {}: malformed Helm values JSON, expected an object or null",
            o.release, o.namespace
        ))
        .with_fix(fix)
        .into());
    }
    Ok(Some(values))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffParticipation {
    Include,
    Preserve,
}

#[derive(Clone, PartialEq, Eq)]
enum PlannedHelmValues {
    RetainedMail(PrivateHelmValues),
    Set {
        flag: HelmSetFlag,
        expression: String,
        effective: Vec<(String, String)>,
    },
    SecretFile {
        values: Vec<(String, String)>,
        diff: DiffParticipation,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum HelmSetFlag {
    Set,
    SetString,
    SetJson,
}

fn typed_empty_worker_map_override(expression: &str) -> Option<String> {
    let (key, value) = expression.split_once('=')?;
    (key.trim() == "worker.adapterCredentials"
        && (value.is_empty()
            || serde_json::from_str::<serde_json::Value>(value)
                .is_ok_and(|value| value.as_object().is_some_and(|map| map.is_empty()))))
    .then(|| "worker.adapterCredentials={}".to_string())
}

#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct UpValuePlan {
    entries: Vec<PlannedHelmValues>,
}

impl UpValuePlan {
    fn set(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        self.entries.push(PlannedHelmValues::Set {
            flag: HelmSetFlag::Set,
            expression: format!("{key}={value}"),
            effective: vec![(key, value)],
        });
    }

    fn set_expression(&mut self, expression: String) {
        let effective = operator_set_entries(std::slice::from_ref(&expression))
            .into_iter()
            .map(|(key, value)| (key.trim().to_string(), value.to_string()))
            .collect();
        let typed = typed_empty_worker_map_override(&expression);
        self.entries.push(PlannedHelmValues::Set {
            flag: if typed.is_some() {
                HelmSetFlag::SetJson
            } else {
                HelmSetFlag::Set
            },
            expression: typed.unwrap_or(expression),
            effective,
        });
    }

    fn set_string_expression(&mut self, expression: String) {
        let effective = if expression.split_once('=').is_some_and(|(key, _)| {
            key_is_or_descends_from(key.trim(), WORKER_EXTRA_ENV_KEY)
                || key_is_or_descends_from(key.trim(), SLACK_TRUSTED_ORIGINS_KEY)
        }) {
            helm_set_string_entries(&expression)
        } else {
            operator_set_entries(std::slice::from_ref(&expression))
                .into_iter()
                .map(|(key, value)| (key.trim().to_string(), value.to_string()))
                .collect()
        };
        let typed = typed_empty_worker_map_override(&expression);
        self.entries.push(PlannedHelmValues::Set {
            flag: if typed.is_some() {
                HelmSetFlag::SetJson
            } else {
                HelmSetFlag::SetString
            },
            expression: typed.unwrap_or(expression),
            effective,
        });
    }

    fn secret_file(&mut self, values: Vec<(String, String)>, diff: DiffParticipation) {
        if !values.is_empty() {
            self.entries
                .push(PlannedHelmValues::SecretFile { values, diff });
        }
    }

    fn append_command_args(&self, args: &mut Vec<CmdArg>) {
        for entry in &self.entries {
            match entry {
                PlannedHelmValues::RetainedMail(values) => {
                    args.push(CmdArg::PrivateJsonValuesFile(values.clone()));
                }
                PlannedHelmValues::Set {
                    flag, expression, ..
                } => {
                    args.push(plain(match flag {
                        HelmSetFlag::Set => "--set",
                        HelmSetFlag::SetString => "--set-string",
                        HelmSetFlag::SetJson => "--set-json",
                    }));
                    args.push(CmdArg::HelmSetExpression(expression.clone()));
                }
                PlannedHelmValues::SecretFile { values, .. } => {
                    args.push(CmdArg::SecretValuesFile(values.clone()));
                }
            }
        }
    }

    pub(crate) fn effective_values(&self) -> BTreeMap<String, String> {
        let mut values = BTreeMap::new();
        for entry in &self.entries {
            match entry {
                PlannedHelmValues::RetainedMail(retained) => {
                    values.extend(
                        retained
                            .1
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone())),
                    );
                }
                PlannedHelmValues::Set { effective, .. } => {
                    values.extend(effective.iter().cloned());
                }
                PlannedHelmValues::SecretFile {
                    values: secrets,
                    diff: DiffParticipation::Include,
                } => {
                    values.extend(secrets.iter().cloned());
                }
                PlannedHelmValues::SecretFile {
                    diff: DiffParticipation::Preserve,
                    ..
                } => {}
            }
        }
        values
    }
}

/// The ordered chart values supplied by one `cluster up`. Both the Helm command
/// and installation diff consume this representation, so adding a value cannot
/// update one path without updating the other.
pub(crate) fn up_value_plan(o: &UpOpts) -> UpValuePlan {
    let mut plan = UpValuePlan::default();
    if o.dev {
        plan.set(ALLOW_DEV_DEFAULTS_KEY, "true");
    }
    if !o.no_expose {
        plan.set("ui.service.type", "NodePort");
        plan.set("langfuse.web.service.type", "NodePort");
    }
    if let Some(model) = &o.local_model {
        plan.set("inference.deploy", "true");
        plan.set("inference.model", model);
    }
    if let Some(credentials) = &o.credentials {
        plan.set(FAKE_MODEL_KEY, "false");
        plan.secret_file(
            vec![(MODEL_CREDENTIAL_KEY.to_string(), credentials.clone())],
            DiffParticipation::Include,
        );
    }
    match &o.github_token {
        GithubTokenPlan::Untouched => {}
        GithubTokenPlan::Set(token) => {
            plan.secret_file(
                vec![(GITHUB_TOKEN_KEY.to_string(), token.clone())],
                DiffParticipation::Include,
            );
        }
        GithubTokenPlan::Clear => {
            plan.set(GITHUB_TOKEN_KEY, "");
        }
    }
    for (index, cidr) in o
        .resolved_egress_cidrs
        .iter()
        .chain(o.allow_web_egress.iter())
        .enumerate()
    {
        plan.set(
            format!("security.networkPolicy.allowedEgress[{index}].cidr"),
            cidr,
        );
        plan.set(
            format!("security.networkPolicy.allowedEgress[{index}].ports[0].protocol"),
            "TCP",
        );
        plan.set(
            format!("security.networkPolicy.allowedEgress[{index}].ports[0].port"),
            EGRESS_TCP_PORT.to_string(),
        );
    }
    plan.secret_file(o.secrets.clone(), DiffParticipation::Preserve);
    if let Some(values) = &o.retained_mail_values {
        plan.entries
            .push(PlannedHelmValues::RetainedMail(values.clone()));
    }
    if let Some(model) = &o.model {
        if explicit_runner_model(&o.operator_sets()).is_none() {
            plan.set(RUNNER_MODEL_KEY, model);
        }
    }
    for expression in &o.set {
        plan.set_expression(expression.clone());
    }
    // Helm merges `--set-string` after `--set`, while `effective_values` uses
    // insertion order. Keep the declared lane after the typed lane so duplicate
    // keys resolve identically.
    for expression in &o.set_string {
        plan.set_string_expression(expression.clone());
    }
    plan
}

fn gvisor_preflight_job_name_from_render(rendered: &str) -> Result<Option<String>> {
    let mut found = None;
    for document in rendered.split("\n---") {
        let document = document.trim();
        if document.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_norway::from_str(document)
            .context("could not parse the rendered gVisor preflight Job")?;
        if value.is_null() {
            continue;
        }
        if value.get("kind").and_then(|kind| kind.as_str()) != Some("Job") {
            continue;
        }
        let name = value
            .get("metadata")
            .and_then(|metadata| metadata.get("name"))
            .and_then(|name| name.as_str())
            .filter(|name| !name.is_empty())
            .context("the rendered gVisor preflight Job has no name")?;
        if found.replace(name.to_string()).is_some() {
            bail!("the gVisor preflight template rendered more than one Job");
        }
    }
    Ok(found)
}

async fn rendered_gvisor_preflight_job(
    chart: &str,
    common: &CommonOpts,
    plan: &UpValuePlan,
) -> Result<Option<String>> {
    let mut args = vec![
        plain("template"),
        plain(&common.release),
        plain(chart),
        plain("-n"),
        plain(&common.namespace),
    ];
    plan.append_command_args(&mut args);
    args.push(plain("--show-only"));
    args.push(plain("templates/preflight-gvisor.yaml"));
    let (ok, out, err) = run_capture(&OpsCommand::new("helm", args)).await?;
    if !ok {
        if err.trim() == "Error: could not find template templates/preflight-gvisor.yaml in chart" {
            return Ok(None);
        }
        bail!(
            "could not render the gVisor preflight Job: {}",
            failure_reason(&err)
        );
    }
    gvisor_preflight_job_name_from_render(&out)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PriorityClassRole {
    Platform,
    Sandbox,
}

impl PriorityClassRole {
    const ALL: [Self; 2] = [Self::Platform, Self::Sandbox];

    fn key(self) -> &'static str {
        match self {
            Self::Platform => "platform",
            Self::Sandbox => "sandbox",
        }
    }

    fn sibling(self) -> Self {
        match self {
            Self::Platform => Self::Sandbox,
            Self::Sandbox => Self::Platform,
        }
    }
}

const CONTROLLER_DEPLOYMENT_NAME: &str = "agent-sandbox-controller";
const CONTROLLER_DEPLOYMENT_NAMESPACE: &str = "agent-sandbox-system";
const CONTROLLER_DEPLOY_KEY: &str = "agentSandbox.controller.deploy";
const GVISOR_MODE_KEY: &str = "security.gvisor.mode";

/// The chart value that switches every chart-owned credential to its published
/// dev default (`curie.managedSecret` in `charts/curie/templates/_helpers.tpl`).
/// Named once because three call sites are ONE decision and must not drift
/// (#1145): the value `up_value_plan` emits for `--dev`, the recorded-value read
/// [`guard_dev_defaults_flip`] does to decide whether a release is already on
/// dev defaults, and the operator-override membership test that exempts a run
/// the operator has taken ownership of. A literal in any one of them silently
/// desynchronises the guard from what helm actually renders.
const ALLOW_DEV_DEFAULTS_KEY: &str = "security.allowDevDefaults";

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClusterUpInference {
    Provider {
        provider: &'static str,
    },
    PriorityClassReuse {
        role: PriorityClassRole,
        name: String,
        owner_release: String,
    },
    ControllerReuse {
        owner_release: String,
    },
    GvisorOff,
}

impl ClusterUpInference {
    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            Self::Provider { provider } => ui.note(&format!(
                "inferred model provider from the bound credential prefix; applying `--allow-egress-host {provider}`"
            )),
            Self::PriorityClassReuse {
                role,
                name,
                owner_release,
            } => ui.note(&format!(
                "inferred reuse of PriorityClass `{name}` from Helm release `{owner_release}`; applying `--set priorityClasses.{}.create=false`",
                role.key()
            )),
            Self::ControllerReuse { owner_release } => ui.note(&format!(
                "inferred reuse of `{CONTROLLER_DEPLOYMENT_NAME}` from Helm release `{owner_release}`; applying `--set {CONTROLLER_DEPLOY_KEY}=false`"
            )),
            Self::GvisorOff => ui.note(&format!(
                "inferred that the cluster has no `gvisor` RuntimeClass from admission; applying `--set {GVISOR_MODE_KEY}=off`"
            )),
        }
    }
}

fn final_operator_value<'a>(opts: &'a UpOpts, key: &str) -> Option<&'a str> {
    let in_lane = |sets: &'a [String]| {
        operator_set_entries(sets)
            .into_iter()
            .rev()
            .find_map(|(candidate, value)| (candidate.trim() == key).then_some(value.trim()))
    };
    in_lane(&opts.set_string).or_else(|| in_lane(&opts.set))
}

fn detected_provider_from_plan(opts: &UpOpts, plan: &UpValuePlan) -> Option<&'static str> {
    if opts.fake_model || opts.local_model.is_some() {
        return None;
    }
    plan.effective_values()
        .get(MODEL_CREDENTIAL_KEY)
        .and_then(|credential| provider_from_credential_prefix(credential))
}

fn provider_contradiction(opts: &UpOpts, plan: &UpValuePlan) -> Result<()> {
    let Some(provider) = detected_provider_from_plan(opts, plan) else {
        return Ok(());
    };
    if opts.allow_egress_host.is_empty()
        || opts
            .allow_egress_host
            .iter()
            .any(|declared| declared == provider)
    {
        return Ok(());
    }
    let declared = opts
        .allow_egress_host
        .iter()
        .map(|value| format!("--allow-egress-host {value}"))
        .collect::<Vec<_>>()
        .join(" ");
    let fix =
        format!("include `--allow-egress-host {provider}`, or remove the explicit provider list");
    Err(crate::exit::CliError::usage(format!(
        "the bound credential selects provider `{provider}`, but the explicit provider list `{declared}` omits it; {fix}"
    ))
    .with_fix(fix)
    .into())
}

fn reconcile_provider_inference(
    opts: &mut UpOpts,
    plan: &UpValuePlan,
) -> Result<Option<ClusterUpInference>> {
    provider_contradiction(opts, plan)?;
    let Some(provider) = detected_provider_from_plan(opts, plan) else {
        return Ok(None);
    };
    if !opts.allow_egress_host.is_empty() {
        return Ok(None);
    }
    opts.allow_egress_host.push(provider.to_string());
    Ok(Some(ClusterUpInference::Provider { provider }))
}

#[derive(Debug, PartialEq, Eq)]
struct PriorityClassOwner {
    release: String,
    namespace: String,
}

#[derive(Debug, PartialEq, Eq)]
enum PriorityClassOwnership {
    Absent,
    Existing(Option<PriorityClassOwner>),
}

#[derive(Debug, PartialEq, Eq)]
struct PriorityClassConflict {
    role: PriorityClassRole,
    name: String,
    owner: Option<PriorityClassOwner>,
}

fn priority_class_name_from_render(
    rendered: &str,
    role: PriorityClassRole,
) -> Result<Option<String>> {
    let mut found = None;
    for document in rendered.split("\n---") {
        let document = document.trim();
        if document.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_norway::from_str(document).with_context(|| {
            format!(
                "could not parse the rendered PriorityClass for role {}",
                role.key()
            )
        })?;
        if value.is_null() {
            continue;
        }
        if value.get("kind").and_then(|kind| kind.as_str()) != Some("PriorityClass") {
            bail!(
                "the PriorityClass template rendered an unexpected object for role {}",
                role.key()
            );
        }
        let name = value
            .get("metadata")
            .and_then(|metadata| metadata.get("name"))
            .and_then(|name| name.as_str())
            .filter(|name| !name.is_empty())
            .with_context(|| {
                format!(
                    "the rendered PriorityClass for role {} has no name",
                    role.key()
                )
            })?;
        if found.replace(name.to_string()).is_some() {
            bail!(
                "the PriorityClass template rendered more than one object for role {}",
                role.key()
            );
        }
    }
    Ok(found)
}

async fn rendered_priority_classes(
    chart: &str,
    common: &CommonOpts,
    plan: &UpValuePlan,
) -> Result<Vec<(PriorityClassRole, String)>> {
    let mut rendered = Vec::new();
    for role in PriorityClassRole::ALL {
        let mut args = vec![
            plain("template"),
            plain(&common.release),
            plain(chart),
            plain("-n"),
            plain(&common.namespace),
        ];
        plan.append_command_args(&mut args);
        args.push(plain("--show-only"));
        args.push(plain("templates/priorityclass.yaml"));
        args.push(plain("--set"));
        args.push(plain(format!(
            "priorityClasses.{}.create=false",
            role.sibling().key()
        )));
        let (ok, out, err) = run_capture(&OpsCommand::new("helm", args)).await?;
        if !ok {
            if err.trim() == "Error: could not find template templates/priorityclass.yaml in chart"
            {
                continue;
            }
            bail!(
                "could not render the PriorityClass for role {}: {}",
                role.key(),
                failure_reason(&err)
            );
        }
        if let Some(name) = priority_class_name_from_render(&out, role)? {
            rendered.push((role, name));
        }
    }
    Ok(rendered)
}

fn priority_class_read_error(
    name: &str,
    detail: impl std::fmt::Display,
    transient: bool,
) -> anyhow::Error {
    let fix = "run `curie cluster status`".to_string();
    let message = format!("could not inspect PriorityClass `{name}`: {detail}; {fix}");
    let error = if transient {
        crate::exit::CliError::transient(message)
    } else {
        crate::exit::CliError::failure(message)
    };
    error.with_fix(fix).into()
}

fn priority_class_metadata_map<'a>(
    metadata: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
    name: &str,
) -> Result<Option<&'a serde_json::Map<String, serde_json::Value>>> {
    match metadata.get(field) {
        None => Ok(None),
        Some(serde_json::Value::Object(values)) => Ok(Some(values)),
        Some(_) => Err(priority_class_read_error(
            name,
            format!("kubectl returned invalid object JSON with nonobject metadata.{field}"),
            false,
        )),
    }
}

fn priority_class_metadata_value<'a>(
    values: Option<&'a serde_json::Map<String, serde_json::Value>>,
    key: &str,
    name: &str,
) -> Result<Option<&'a str>> {
    match values.and_then(|values| values.get(key)) {
        None => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(priority_class_read_error(
            name,
            format!("kubectl returned invalid object JSON at metadata key `{key}`"),
            false,
        )),
    }
}

async fn priority_class_owner(name: &str) -> Result<PriorityClassOwnership> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("priorityclass"),
            plain(name),
            plain("--ignore-not-found"),
            plain("-o"),
            plain("json"),
        ],
    );
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        return Err(priority_class_read_error(
            name,
            failure_reason(&err),
            is_connectivity_failure(&err),
        ));
    }
    if out.trim().is_empty() {
        return Ok(PriorityClassOwnership::Absent);
    }
    let value: serde_json::Value = serde_json::from_str(out.trim())
        .map_err(|_| priority_class_read_error(name, "kubectl returned invalid JSON", false))?;
    let object = value.as_object().ok_or_else(|| {
        priority_class_read_error(name, "kubectl returned invalid object JSON", false)
    })?;
    let metadata = object
        .get("metadata")
        .and_then(|metadata| metadata.as_object())
        .ok_or_else(|| {
            priority_class_read_error(
                name,
                "kubectl returned invalid object JSON without metadata",
                false,
            )
        })?;
    if metadata.get("name").and_then(|value| value.as_str()) != Some(name) {
        return Err(priority_class_read_error(
            name,
            "kubectl returned invalid object JSON with another metadata.name",
            false,
        ));
    }

    let labels = priority_class_metadata_map(metadata, "labels", name)?;
    if priority_class_metadata_value(labels, "app.kubernetes.io/managed-by", name)? != Some("Helm")
    {
        return Ok(PriorityClassOwnership::Existing(None));
    }
    let annotations = priority_class_metadata_map(metadata, "annotations", name)?;
    let Some(release) =
        priority_class_metadata_value(annotations, "meta.helm.sh/release-name", name)?
    else {
        return Ok(PriorityClassOwnership::Existing(None));
    };
    let Some(namespace) =
        priority_class_metadata_value(annotations, "meta.helm.sh/release-namespace", name)?
    else {
        return Ok(PriorityClassOwnership::Existing(None));
    };
    Ok(PriorityClassOwnership::Existing(Some(PriorityClassOwner {
        release: release.to_string(),
        namespace: namespace.to_string(),
    })))
}

async fn priority_class_observations(
    opts: &UpOpts,
    plan: &UpValuePlan,
) -> Result<Vec<PriorityClassConflict>> {
    let rendered = rendered_priority_classes(&opts.chart, &opts.common, plan).await?;
    let mut observations = Vec::new();
    for (role, name) in rendered {
        let owner = match priority_class_owner(&name).await? {
            PriorityClassOwnership::Absent => continue,
            PriorityClassOwnership::Existing(owner) => owner,
        };
        observations.push(PriorityClassConflict { role, name, owner });
    }
    Ok(observations)
}

fn priority_class_conflict_error(conflicts: Vec<PriorityClassConflict>) -> anyhow::Error {
    let mut message = String::from("PriorityClass ownership conflicts block installation:");
    for conflict in conflicts {
        if let Some(owner) = conflict.owner {
            message.push_str(&format!(
                "\nPriorityClass `{}` is owned by Helm release `{}` in namespace `{}`.",
                conflict.name, owner.release, owner.namespace
            ));
        } else {
            message.push_str(&format!(
                "\nPriorityClass `{}` exists without complete Helm ownership metadata.",
                conflict.name
            ));
        }
        message.push_str(&format!(
            "\nReuse it with `--set priorityClasses.{}.create=false --set priorityClasses.{}.name={}`.",
            conflict.role.key(),
            conflict.role.key(),
            conflict.name
        ));
        message.push_str(&format!(
            "\nKeep creation enabled with `--set priorityClasses.{}.name=<different-name>`.",
            conflict.role.key()
        ));
    }
    crate::exit::CliError::failure(message).into()
}

async fn preflight_priority_class_ownership(opts: &UpOpts, plan: &UpValuePlan) -> Result<()> {
    let mut conflicts = Vec::new();
    for observation in priority_class_observations(opts, plan).await? {
        let PriorityClassConflict { role, name, owner } = observation;
        let conflicts_with_target = match owner.as_ref() {
            None => true,
            Some(owner) => {
                owner.release != opts.common.release || owner.namespace != opts.common.namespace
            }
        };
        if conflicts_with_target {
            conflicts.push(PriorityClassConflict { role, name, owner });
        }
    }
    if conflicts.is_empty() {
        return Ok(());
    }

    Err(priority_class_conflict_error(conflicts))
}

async fn reconcile_priority_class_ownership(
    opts: &UpOpts,
    plan: &mut UpValuePlan,
) -> Result<Vec<ClusterUpInference>> {
    let mut conflicts = Vec::new();
    let mut inferred = Vec::new();
    for observation in priority_class_observations(opts, plan).await? {
        let PriorityClassConflict { role, name, owner } = observation;
        let Some(owner) = owner else {
            conflicts.push(PriorityClassConflict {
                role,
                name,
                owner: None,
            });
            continue;
        };
        if owner.release == opts.common.release && owner.namespace == opts.common.namespace {
            continue;
        }
        let key = format!("priorityClasses.{}.create", role.key());
        match final_operator_value(opts, &key) {
            Some("true") => {
                let assignment = format!("{key}=true");
                let fix = format!("remove `--set {assignment}`, or pass `--set {key}=false`");
                return Err(crate::exit::CliError::usage(format!(
                    "PriorityClass `{name}` is owned by Helm release `{}` in namespace `{}`, which contradicts explicit `{assignment}`; {fix}",
                    owner.release, owner.namespace
                ))
                .with_fix(fix)
                .into());
            }
            Some(_) => {}
            None => inferred.push(ClusterUpInference::PriorityClassReuse {
                role,
                name,
                owner_release: owner.release,
            }),
        }
    }
    if !conflicts.is_empty() {
        return Err(priority_class_conflict_error(conflicts));
    }
    for inference in &inferred {
        if let ClusterUpInference::PriorityClassReuse { role, .. } = inference {
            plan.set(format!("priorityClasses.{}.create", role.key()), "false");
        }
    }
    Ok(inferred)
}

#[derive(Debug, PartialEq, Eq)]
enum ControllerOwnership {
    Absent,
    Existing(Option<PriorityClassOwner>),
}

fn controller_read_error(detail: impl std::fmt::Display, transient: bool) -> anyhow::Error {
    let fix = "run `curie cluster status`".to_string();
    let message = format!(
        "could not inspect Deployment `{CONTROLLER_DEPLOYMENT_NAME}` in namespace `{CONTROLLER_DEPLOYMENT_NAMESPACE}`: {detail}; {fix}"
    );
    let error = if transient {
        crate::exit::CliError::transient(message)
    } else {
        crate::exit::CliError::failure(message)
    };
    error.with_fix(fix).into()
}

fn controller_metadata_map<'a>(
    metadata: &'a serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> Result<Option<&'a serde_json::Map<String, serde_json::Value>>> {
    match metadata.get(field) {
        None => Ok(None),
        Some(serde_json::Value::Object(values)) => Ok(Some(values)),
        Some(_) => Err(controller_read_error(
            format!("kubectl returned invalid object JSON with nonobject metadata.{field}"),
            false,
        )),
    }
}

fn controller_metadata_value<'a>(
    values: Option<&'a serde_json::Map<String, serde_json::Value>>,
    key: &str,
) -> Result<Option<&'a str>> {
    match values.and_then(|values| values.get(key)) {
        None => Ok(None),
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(controller_read_error(
            format!("kubectl returned invalid object JSON at metadata key `{key}`"),
            false,
        )),
    }
}

async fn controller_owner() -> Result<ControllerOwnership> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("deployment"),
            plain(CONTROLLER_DEPLOYMENT_NAME),
            plain("-n"),
            plain(CONTROLLER_DEPLOYMENT_NAMESPACE),
            plain("--ignore-not-found"),
            plain("-o"),
            plain("json"),
        ],
    );
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        let missing_namespace = err.contains("NotFound")
            && err.contains("namespaces")
            && err.contains(CONTROLLER_DEPLOYMENT_NAMESPACE);
        if missing_namespace {
            return Ok(ControllerOwnership::Absent);
        }
        return Err(controller_read_error(
            failure_reason(&err),
            is_connectivity_failure(&err),
        ));
    }
    if out.trim().is_empty() {
        return Ok(ControllerOwnership::Absent);
    }
    let value: serde_json::Value = serde_json::from_str(out.trim())
        .map_err(|_| controller_read_error("kubectl returned invalid JSON", false))?;
    let object = value
        .as_object()
        .ok_or_else(|| controller_read_error("kubectl returned invalid object JSON", false))?;
    if object.get("kind").and_then(|value| value.as_str()) != Some("Deployment") {
        return Err(controller_read_error(
            "kubectl returned an object that is not a Deployment",
            false,
        ));
    }
    let metadata = object
        .get("metadata")
        .and_then(|metadata| metadata.as_object())
        .ok_or_else(|| {
            controller_read_error(
                "kubectl returned invalid object JSON without metadata",
                false,
            )
        })?;
    if metadata.get("name").and_then(|value| value.as_str()) != Some(CONTROLLER_DEPLOYMENT_NAME) {
        return Err(controller_read_error(
            "kubectl returned invalid object JSON with another metadata.name",
            false,
        ));
    }
    let labels = controller_metadata_map(metadata, "labels")?;
    if controller_metadata_value(labels, "app.kubernetes.io/managed-by")? != Some("Helm") {
        return Ok(ControllerOwnership::Existing(None));
    }
    let annotations = controller_metadata_map(metadata, "annotations")?;
    let Some(release) = controller_metadata_value(annotations, "meta.helm.sh/release-name")? else {
        return Ok(ControllerOwnership::Existing(None));
    };
    let Some(namespace) = controller_metadata_value(annotations, "meta.helm.sh/release-namespace")?
    else {
        return Ok(ControllerOwnership::Existing(None));
    };
    Ok(ControllerOwnership::Existing(Some(PriorityClassOwner {
        release: release.to_string(),
        namespace: namespace.to_string(),
    })))
}

async fn reconcile_controller_ownership(
    opts: &UpOpts,
    plan: &mut UpValuePlan,
) -> Result<Option<ClusterUpInference>> {
    let explicit = final_operator_value(opts, CONTROLLER_DEPLOY_KEY);
    if explicit == Some("false") {
        return Ok(None);
    }
    let owner = match controller_owner().await? {
        ControllerOwnership::Absent => return Ok(None),
        ControllerOwnership::Existing(Some(owner)) => owner,
        ControllerOwnership::Existing(None) => {
            return Err(controller_read_error(
                "the Deployment exists without complete Helm ownership metadata",
                false,
            ));
        }
    };
    if owner.release == opts.common.release && owner.namespace == opts.common.namespace {
        return Ok(None);
    }
    if explicit == Some("true") {
        let assignment = format!("{CONTROLLER_DEPLOY_KEY}=true");
        let fix =
            format!("remove `--set {assignment}`, or pass `--set {CONTROLLER_DEPLOY_KEY}=false`");
        return Err(crate::exit::CliError::usage(format!(
            "Deployment `{CONTROLLER_DEPLOYMENT_NAME}` is owned by Helm release `{}` in namespace `{}`, which contradicts explicit `{assignment}`; {fix}",
            owner.release, owner.namespace
        ))
        .with_fix(fix)
        .into());
    }
    if explicit.is_some() {
        return Ok(None);
    }
    plan.set(CONTROLLER_DEPLOY_KEY, "false");
    Ok(Some(ClusterUpInference::ControllerReuse {
        owner_release: owner.release,
    }))
}

fn up_commands_with_plan(o: &UpOpts, plan: &UpValuePlan) -> Vec<OpsCommand> {
    let mut args = vec![
        plain("upgrade"),
        plain("--install"),
        plain(&o.common.release),
        plain(&o.chart),
        plain("-n"),
        plain(&o.common.namespace),
        plain("--create-namespace"),
    ];
    plan.append_command_args(&mut args);
    vec![OpsCommand::new("helm", args)]
}

/// `helm upgrade --install` for the release. Its chart values are rendered from
/// [`up_value_plan`], which is also the source of the installation diff.
pub fn up_commands(o: &UpOpts) -> Vec<OpsCommand> {
    up_commands_with_plan(o, &up_value_plan(o))
}

/// The read-only commands `curie cluster status` runs (and prints under `--dry-run`).
///
/// Pure: the caller resolves the release's [`ReleaseFullname`] and passes it in
/// (live for a real run, [`chart_fullname`] under `--dry-run`), so this builder
/// still makes no cluster call of its own.
pub fn status_commands(o: &CommonOpts, fullname: &ReleaseFullname) -> Vec<OpsCommand> {
    let mut commands = vec![
        helm_status_cmd(o),
        pods_cmd(o),
        svc_cmd(o, fullname, "ui"),
        svc_cmd(o, fullname, "langfuse-web"),
        kubeconfig_host_cmd(),
    ];
    commands.extend(convergence::dry_run_commands(o));
    commands
}

fn helm_status_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("status"),
            plain(&o.release),
            plain("-n"),
            plain(&o.namespace),
        ],
    )
}

fn pods_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("pods"),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// `kubectl get svc <fullname>-<suffix> -n <ns> -o json`.
///
/// The choke point for every release Service the CLI reads. It takes a resolved
/// [`ReleaseFullname`], never the release name: the chart names each Service
/// `{{ include "curie.fullname" . }}-<component>`, which equals
/// `<release>-<component>` only when the release name happens to contain the
/// chart name (#1533).
fn svc_cmd(o: &CommonOpts, fullname: &ReleaseFullname, suffix: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("svc"),
            plain(fullname.resource(suffix)),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// `kubectl config view --minify -o jsonpath={...server}`: the current-context
/// API-server URL. The one canonical builder (#497) shared by the ops egress-IP
/// resolution and the message driver's advertise-host detection.
pub(crate) fn kubeconfig_host_cmd() -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("config"),
            plain("view"),
            plain("--minify"),
            plain("-o"),
            plain("jsonpath={.clusters[0].cluster.server}"),
        ],
    )
}

pub(crate) fn nodes_cmd() -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![plain("get"), plain("nodes"), plain("-o"), plain("json")],
    )
}

/// `helm uninstall` then a namespace sweep of only the namespaces THIS release
/// created (runtime sandboxes, PVCs and job pods Helm does not own). #707: the
/// sweep is scoped by the ownership labels `up` stamped rather than a hardcoded
/// namespace pair, so a pre-existing (unlabeled) namespace is never deleted.
/// `--ignore-not-found` keeps a partial teardown re-runnable and the label
/// selector tolerates zero matches. CRDs are never targeted (retention is
/// by-construction).
///
/// #1654: the selector is the CONJUNCTION of both ownership labels
/// (`curietech.ai/created-by=<release>,curietech.ai/created-in=<namespace>`),
/// because a release name alone is not an identity on a shared cluster. Two
/// independent installs normally both take the default release name `curie`
/// while living in different install namespaces, so a `created-by`-only
/// selector matched the OTHER install's namespaces and one `cluster down`
/// deleted them (observed live: `agent-sandbox-system` stamped
/// `created-by=curie` but annotated `meta.helm.sh/release-namespace: curie-other`,
/// i.e. owned by a different release, swept anyway). The identity is therefore
/// the PAIR (release name, install namespace), and both terms are required.
///
/// A namespace stamped by an older CLI carries only `created-by` and so does
/// NOT match this selector: that is deliberate. The sweep fails safe toward
/// retention rather than deleting a namespace whose owner cannot be
/// established; there is no fallback selector, since a fallback is exactly the
/// cross-release delete #1654 reports.
pub fn down_commands(o: &CommonOpts) -> Vec<OpsCommand> {
    vec![
        OpsCommand::new(
            "helm",
            vec![
                plain("uninstall"),
                plain(&o.release),
                plain("-n"),
                plain(&o.namespace),
            ],
        ),
        OpsCommand::new(
            "kubectl",
            vec![
                plain("delete"),
                plain("namespace"),
                plain("-l"),
                plain(format!(
                    "curietech.ai/created-by={},curietech.ai/created-in={}",
                    o.release, o.namespace
                )),
                plain("--ignore-not-found"),
            ],
        ),
    ]
}

/// Outcome of the `helm uninstall` teardown step. `Absent` is the existing
/// already-absent ("not found") case, which counts as done, never outstanding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelmOutcome {
    Removed,
    Absent,
    Failed,
}

/// Outcome of the label-scoped namespace sweep step. `NoMatch` (#768) is the
/// zero-match case: the selector's `kubectl delete` exits 0 (success) but
/// deleted nothing, because it printed nothing to stdout. This happens by
/// design when Curie was installed into a PRE-EXISTING namespace (#707
/// deliberately leaves that namespace unlabeled, so it is never a sweep
/// target). `NoMatch` counts as a completed step, same as `Removed`
/// (`outstanding_steps` never re-queues it), but it is NOT the same as
/// `Removed` for messaging: a `NoMatch` sweep stopped no compute, so
/// `teardown_result` must not describe it as "swept".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SweepOutcome {
    Removed,
    NoMatch,
    Failed,
}

/// A teardown step that can remain outstanding after a fail-forward `down`.
#[derive(Debug, PartialEq, Eq)]
enum TeardownStep {
    HelmUninstall,
    NamespaceSweep,
}

/// Pure decision (#767): which teardown steps did NOT complete. A `Removed` or
/// `Absent` helm is done; a `Failed` helm leaves `HelmUninstall` outstanding. A
/// `Removed` sweep is done; a `Failed` sweep leaves `NamespaceSweep`
/// outstanding. Order matches `down_commands` (helm before sweep).
fn outstanding_steps(helm: HelmOutcome, sweep: SweepOutcome) -> Vec<TeardownStep> {
    let mut out = Vec::new();
    if matches!(helm, HelmOutcome::Failed) {
        out.push(TeardownStep::HelmUninstall);
    }
    if matches!(sweep, SweepOutcome::Failed) {
        out.push(TeardownStep::NamespaceSweep);
    }
    out
}

/// Pure builder (#767, aggregation #768): the exact copy-pasteable resume
/// command for the outstanding steps, mapping each back to its
/// `down_commands(o)` entry (index 0 = HelmUninstall, 1 = NamespaceSweep).
///
/// When BOTH steps are outstanding the emitted ORDER matches `down()`'s own
/// execution order: the HELM UNINSTALL FIRST, then the namespace sweep. Helm
/// first because Helm stores its release metadata as Secrets INSIDE the
/// release namespace, and this chart owns cluster-scoped resources
/// (ClusterRole/ClusterRoleBinding). Sweeping the namespace first would destroy
/// that metadata, the following `helm uninstall` would report "not found"
/// (which this code reads as already-absent success), and the cluster-scoped
/// resources would be orphaned with no cleanup path.
///
/// #768: a plain "; " join runs both commands unconditionally (the compute-
/// stopping sweep is never skipped by a repeated helm failure), but a shell
/// only returns the LAST command's exit status, so an agent or CI executing
/// the resume line verbatim could read exit 0 even though helm failed. A
/// naive "&&" join would fix the status but reintroduce the fail-hard hazard
/// this whole feature exists to remove (a repeated helm failure would again
/// skip the sweep). Instead the two-step remainder is wrapped so each
/// command's status is captured into its own shell variable, both commands
/// still run unconditionally, and the final expression is nonzero UNLESS both
/// succeeded -- runs-both-aggregates-nonzero, not runs-both-or-skips-second.
///
/// The argv itself comes from `down_commands`, which stays the single source of
/// truth for the teardown commands, so the resume line cannot drift from what
/// would actually finish the job. A single-step remainder is one command with
/// no wrapper; an empty remainder yields the empty string.
fn resume_command(remaining: &[TeardownStep], o: &CommonOpts) -> String {
    let cmds = down_commands(o);
    let mut steps: Vec<&TeardownStep> = remaining.iter().collect();
    // Helm before sweep, matching `down_commands` execution order.
    steps.sort_by_key(|step| match step {
        TeardownStep::HelmUninstall => 0,
        TeardownStep::NamespaceSweep => 1,
    });
    let lines: Vec<String> = steps
        .iter()
        .map(|step| {
            let idx = match step {
                TeardownStep::HelmUninstall => 0,
                TeardownStep::NamespaceSweep => 1,
            };
            cmds[idx].display()
        })
        .collect();
    match lines.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [first, second] => {
            format!("{first}; s1=$?; {second}; s2=$?; [ \"$s1\" -eq 0 ] && [ \"$s2\" -eq 0 ]")
        }
        // `remaining` only ever holds HelmUninstall and/or NamespaceSweep, so a
        // third element is unreachable; stay defensive rather than panic.
        _ => lines.join("; "),
    }
}

/// Classifies whether a shelled-out `helm`/`kubectl` subprocess's stderr names a
/// transient connectivity failure (the API server was unreachable) rather than a
/// permanent one (RBAC, authz, invalid context, a failed hook). Subprocess-stderr
/// sibling of `crate::exit::is_transient_reqwest`, the HTTP-client side's single
/// definition of unreachable; the two cover the two transports and should stay
/// conceptually aligned. Only CONCRETE network signatures count: Helm wraps
/// permanent auth/exec-plugin/kubeconfig errors as `Kubernetes cluster
/// unreachable: ...`, so a bare `unreachable`/`timeout` prefix is not a reliable
/// transient signal and is deliberately excluded (a permanent error wearing that
/// generic prefix must classify false).
///
/// A host RESOLUTION failure is a permanent override checked BEFORE the marker
/// scan: `dial tcp: lookup bad-host: no such host` carries the `dial tcp`
/// connectivity marker but names a kubeconfig hostname that does not resolve,
/// a deterministic configuration error that retrying can never fix. The
/// override list is kept to the single `no such host` signature (the Go
/// resolver's permanent NXDOMAIN wording that both helm and kubectl surface);
/// broader DNS wordings such as `temporary failure in name resolution` name a
/// DNS-SERVER problem, which really is transient, so they stay out. `no route
/// to host` is a routing/connectivity failure, not name resolution, and is
/// deliberately not matched by the override.
fn is_connectivity_failure(stderr: &str) -> bool {
    let lower = stderr.to_lowercase();
    const PERMANENT_RESOLUTION: &[&str] = &["no such host"];
    if PERMANENT_RESOLUTION
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return false;
    }
    const MARKERS: &[&str] = &[
        "connection refused",
        "tls handshake",
        "no route to host",
        "i/o timeout",
        "network is unreachable",
        "could not connect",
        "dial tcp",
        "connection reset",
        "context deadline exceeded",
        // kubectl's own refusal wording, added when a kubectl call site started
        // classifying with this (#1351). client-go prints "The connection to
        // the server <host> was refused - did you specify the right host or
        // port?", which carries none of the Go-client signatures above: the
        // words are separated, so the literal "connection refused" never
        // appears. Without this marker every unreachable-apiserver kubectl read
        // classified as a permanent Failure. Kept to that one phrasing, which
        // client-go emits only for a refused connection.
        "connection to the server",
    ];
    MARKERS.iter().any(|marker| lower.contains(marker))
}

/// A usage-block header, e.g. docker's `Usage:  docker [OPTIONS] COMMAND ...`.
/// Matched on the `usage:` prefix only: a diagnosis line that happens to talk
/// about usage (`usage limit exceeded`) never starts with the colon form, while
/// every CLI that renders help on a bad invocation does.
///
/// Checked over bytes rather than `&str` slicing: a fixed byte index is not
/// guaranteed to fall on a char boundary, which panicked on third-party
/// stderr containing multi-byte characters (#1251).
fn is_usage_header(line: &str) -> bool {
    line.as_bytes()
        .get(..6)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"usage:"))
}

/// A trailing help pointer a CLI appends instead of, or after, a usage block --
/// docker's `Run 'docker --help' for more information`, kubectl's
/// `See 'kubectl get --help' for usage.`. Matched on SHAPE (a `Run '`/`See '`
/// opener that mentions `--help`), not on a tool name, so this does not need a
/// new marker per tool.
fn is_help_pointer(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    let opens = ["run '", "run \"", "see '", "see \""]
        .iter()
        .any(|opener| lower.starts_with(opener));
    opens && lower.contains("--help")
}

/// One-line reason drawn from a captured stderr: the last non-empty trimmed
/// line that is part of the DIAGNOSIS, or a short default when the stderr is
/// empty. The single implementation behind both a failing teardown's Display
/// message and the `run_step` failure line, so neither drops the stderr to
/// `--debug` plumbing and the two cannot drift apart.
///
/// Last-non-empty is the base rule because `helm` and `kubectl` print warnings
/// BEFORE their `Error:` line, so the tail is where their diagnosis lives. What
/// #1230 fixed is that a CLI rejecting its own invocation inverts that: the
/// diagnosis comes first and a usage block comes last, so the raw base rule
/// surfaced `Run 'docker --help' for more information` and threw away
/// `unknown flag: --profile`. Trailing help text is therefore cut before the
/// base rule is applied.
///
/// Safety property: a stderr that is ONLY help text has no diagnosis to
/// recover, so the cut falls back to the base rule's answer rather than to an
/// empty string. This can improve the surfaced reason, never blank it.
fn failure_reason(stderr: &str) -> &str {
    let lines: Vec<&str> = stderr.lines().map(str::trim).collect();
    let base = lines.iter().rev().find(|l| !l.is_empty()).copied();
    // Everything from the last usage header on is the tool's own help text.
    let diagnosis_end = lines
        .iter()
        .rposition(|l| is_usage_header(l))
        .unwrap_or(lines.len());
    lines[..diagnosis_end]
        .iter()
        .rev()
        .find(|l| !l.is_empty() && !is_help_pointer(l))
        .copied()
        .or(base)
        .unwrap_or("command failed")
}

/// Pure decision (#767, #768): turn the two teardown-step outcomes plus their
/// stderr into the `cluster down` result. An empty remainder is success.
/// Otherwise it is a fail-forward error carrying the exact resume command in
/// BOTH the human Display message (P1: `main` renders Display and drops the
/// fix) and the `fix` (for `--json`). The exit class is Transient (exit 3, safe
/// to retry) IFF every outstanding failed step's stderr is a connectivity
/// failure; any permanent outstanding failure makes it a plain Failure (exit 1,
/// P2). #768: a `SweepOutcome::NoMatch` (the label selector matched nothing,
/// e.g. a pre-existing namespace #707 never stamped) is a distinct case from
/// `Removed` -- it must never be worded as having swept/removed compute, since
/// nothing was actually deleted.
fn teardown_result(
    helm: HelmOutcome,
    sweep: SweepOutcome,
    helm_err: &str,
    sweep_err: &str,
    o: &CommonOpts,
) -> anyhow::Result<ClusterDownOutput> {
    let remaining = outstanding_steps(helm, sweep);
    if remaining.is_empty() {
        return Ok(ClusterDownOutput::Down {
            release_was_absent: matches!(helm, HelmOutcome::Absent),
        });
    }
    let cmd = resume_command(&remaining, o);

    // Transient only when every OUTSTANDING failed step is a connectivity failure;
    // a non-failed step never blocks retryability, a permanent failed step always
    // does.
    let helm_retryable = !matches!(helm, HelmOutcome::Failed) || is_connectivity_failure(helm_err);
    let sweep_retryable =
        !matches!(sweep, SweepOutcome::Failed) || is_connectivity_failure(sweep_err);
    let transient = helm_retryable && sweep_retryable;

    // The one-line reason drawn from the failed step that DETERMINES the class,
    // composed into the message so the human Display (P1) names WHY teardown
    // failed. On a non-transient result at least one failed step is permanent,
    // and that is the actionable one the operator must fix, so it wins over a
    // merely transient sibling (helm preferred when both are permanent). On a
    // transient result every failed step is connectivity, so prefer helm.
    let reason = if transient {
        if matches!(helm, HelmOutcome::Failed) {
            failure_reason(helm_err)
        } else {
            failure_reason(sweep_err)
        }
    } else if !helm_retryable {
        failure_reason(helm_err)
    } else {
        failure_reason(sweep_err)
    };

    let message = if matches!(sweep, SweepOutcome::Removed) {
        // Sweep succeeded (compute stopped); only the stale helm record remains.
        format!(
            "helm uninstall failed ({reason}) but the run-created namespaces were swept; the release record remains. Resume with: {cmd}"
        )
    } else if matches!(sweep, SweepOutcome::NoMatch) {
        // #768: the sweep's label selector matched nothing -- this release never
        // created (or was installed into a pre-existing) namespace, so nothing
        // was actually removed. This is NOT the swept case above: do not claim
        // compute was stopped, since it may still be running.
        format!(
            "helm uninstall failed ({reason}); no run-created namespaces matched the sweep, so no compute was stopped (this release may be running in a pre-existing namespace); the release record remains. Resume with: {cmd}"
        )
    } else if transient {
        format!(
            "cluster down could not complete; the API server is unreachable. Resume with: {cmd}"
        )
    } else {
        format!(
            "cluster down could not complete; teardown did not finish ({reason}). Resume with: {cmd}"
        )
    };

    let err = if transient {
        crate::exit::CliError::transient(message).with_fix(cmd)
    } else {
        crate::exit::CliError::failure(message).with_fix(cmd)
    };
    Err(err.into())
}

/// #707 ownership stamp. Returns the single `kubectl label namespace` step that
/// records THIS release as the creator of the target namespace `o.namespace`
/// (callers retarget `o` with `ns_common`, so this is the namespace being
/// labelled, NOT the release's install namespace), but ONLY when `up` actually
/// created it (`namespace_existed == false`); an empty vec when the namespace
/// pre-existed, so a namespace `up` merely adopted is never stamped and
/// therefore never swept by a later `down`. A release-scoped label (not a
/// per-invocation run-id) is what lets a separate `down` invocation match what
/// `up` created. `--overwrite` keeps a re-run idempotent, so an `up` interrupted
/// after create but before stamp fails safe toward retention.
///
/// #1654: the stamp carries TWO labels,
/// `curietech.ai/created-by=<release>` plus
/// `curietech.ai/created-in=<release_namespace>`, written in a single
/// `kubectl label` invocation so the step and checklist accounting is
/// unchanged. `release_namespace` is passed explicitly rather than read off
/// `o.namespace`, which the `ns_common` retarget has already pointed at the
/// namespace being labelled rather than at the install namespace. See
/// `down_commands` for why the PAIR (release name, install namespace) is the
/// identity and why a namespace stamped by an older, single-label CLI is
/// deliberately left unswept.
fn ownership_label_commands(
    o: &CommonOpts,
    release_namespace: &str,
    namespace_existed: bool,
) -> Vec<OpsCommand> {
    if namespace_existed {
        return Vec::new();
    }
    vec![OpsCommand::new(
        "kubectl",
        vec![
            plain("label"),
            plain("namespace"),
            plain(&o.namespace),
            plain(format!("curietech.ai/created-by={}", o.release)),
            plain(format!("curietech.ai/created-in={release_namespace}")),
            plain("--overwrite"),
        ],
    )]
}

/// Whether `up` should attempt the ownership stamp for a candidate namespace,
/// given whether it existed BEFORE the install and whether it exists AFTER.
/// A namespace that pre-existed is never stamped (adopted, not created).
/// A namespace that did not exist before AND still does not exist after is
/// also never stamped: `agent-sandbox-system` is chart-conditional (created
/// only when `agentSandbox.controller.deploy` is true), so under
/// `--set agentSandbox.controller.deploy=false` it stays absent through the
/// whole install and `kubectl label namespace <missing>` would fail. Only a
/// namespace this run actually brought into existence gets stamped.
fn should_stamp_ownership(existed_before: bool, exists_after: bool) -> bool {
    !existed_before && exists_after
}

/// Re-targets `opts` at namespace `ns` with an explicit `dry_run`, for the two
/// `up()` ownership-stamping call sites (`--dry-run` preview and the post-helm
/// stamp attempt) that otherwise duplicate the same `CommonOpts` construction.
fn ns_common(opts: &CommonOpts, ns: &str, dry_run: bool) -> CommonOpts {
    CommonOpts {
        namespace: ns.to_string(),
        release: opts.release.clone(),
        dry_run,
    }
}

/// `kubectl get namespace <ns>`: the pre-existence probe `up` runs before the
/// install so it stamps ownership only on namespaces it creates.
fn namespace_get_cmd(namespace: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![plain("get"), plain("namespace"), plain(namespace)],
    )
}

/// Whether `namespace` already exists on the cluster. A nonzero `kubectl get`
/// (typically NotFound) reads as absent, so `up` treats it as fresh and stamps
/// it; any other transport error surfaces later on the install itself.
async fn namespace_exists(namespace: &str) -> Result<bool> {
    let (ok, _out, _err) = run_capture(&namespace_get_cmd(namespace)).await?;
    Ok(ok)
}

/// Parse the hostname out of a kubeconfig `cluster.server` URL
/// (`https://host:6443` -> `host`). Delegates to the shared parser in
/// `message::split_server_url` so IPv6 and scheme/path handling stay in one place.
pub fn host_from_server_url(server: &str) -> Option<String> {
    crate::message::split_server_url(server).map(|(host, _)| host.to_string())
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Fail with a clear one-line error if `bin` is not on `PATH`.
pub(crate) fn require_on_path(bin: &str) -> Result<()> {
    let found = std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(bin).is_file()))
        .unwrap_or(false);
    if found {
        Ok(())
    } else {
        bail!("`{bin}` is not on PATH; install it (or add it to PATH) and retry")
    }
}

/// Run one command capturing stdout; returns (success, stdout, stderr).
pub async fn run_capture(cmd: &OpsCommand) -> Result<(bool, String, String)> {
    // Materialize any secret values into a private 0600 `-f` file so the secret
    // stays out of the argv/process table. `_secret_files` guards live until the
    // end of this function, so the temp files are removed after `helm` exits
    // (including on error paths below).
    let (cmd, _secret_files) = cmd.materialize_secret_files()?;
    // `kill_on_drop` so a caller that ABANDONS this future does not leave the
    // child running. Dropping the future only stops the wait; without this the
    // process keeps going, which is worst precisely where the abandonment is
    // deliberate -- `message::local_connected_transport` bounds its `docker`
    // reads by a timeout for a wedged daemon, and each timed-out call would
    // otherwise strand another hung `docker` client (#1031). Inert for every
    // caller that awaits to completion: the child has already exited by then.
    let output = Command::new(&cmd.program)
        .args(cmd.argv())
        .envs(cmd.env.iter().chain(cmd.secret_env.iter()).cloned())
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("failed to invoke `{}`; is it on PATH?", cmd.program))?;
    Ok((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    ))
}

fn finish_captured_step(
    step: crate::ui::Step,
    ok_detail: &str,
    cmd: &OpsCommand,
    ok: bool,
    out: String,
    err: String,
) -> Result<String> {
    let ui = crate::ui::ui();
    if ok {
        step.done(ok_detail);
    } else {
        step.fail("failed");
    }
    for line in out.lines().chain(err.lines()) {
        ui.plumbing(line);
    }
    // One implementation, shared with the teardown Display message (#1230):
    // an inline second copy of this rule is how the two drifted before.
    if !ok {
        let reason = failure_reason(&err);
        ui.failure(&format!("`{}` failed: {reason}", cmd.program));
        bail!("`{}` exited nonzero", cmd.program);
    }
    Ok(out)
}

struct RunningInstall {
    child: Child,
    stdout: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    stderr: tokio::task::JoinHandle<std::io::Result<Vec<u8>>>,
    _secret_files: Vec<SecretValuesFileGuard>,
    program: String,
}

impl RunningInstall {
    fn spawn(cmd: &OpsCommand) -> Result<Self> {
        let (cmd, secret_files) = cmd.materialize_secret_files()?;
        let mut child = Command::new(&cmd.program)
            .args(cmd.argv())
            .envs(cmd.env.iter().chain(cmd.secret_env.iter()).cloned())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to invoke `{}`; is it on PATH?", cmd.program))?;
        let mut stdout = child
            .stdout
            .take()
            .expect("piped install stdout must be available");
        let mut stderr = child
            .stderr
            .take()
            .expect("piped install stderr must be available");
        let stdout = tokio::spawn(async move {
            let mut output = Vec::new();
            stdout.read_to_end(&mut output).await?;
            Ok(output)
        });
        let stderr = tokio::spawn(async move {
            let mut output = Vec::new();
            stderr.read_to_end(&mut output).await?;
            Ok(output)
        });
        Ok(Self {
            child,
            stdout,
            stderr,
            _secret_files: secret_files,
            program: cmd.program,
        })
    }

    async fn finish(
        mut self,
        status: std::io::Result<std::process::ExitStatus>,
    ) -> Result<(bool, String, String)> {
        let status = match status {
            Ok(status) => status,
            Err(error) => {
                let _ = terminate_process(&mut self.child).await;
                return Err(error).with_context(|| {
                    format!("failed to invoke `{}`; is it on PATH?", self.program)
                });
            }
        };
        let stdout = self
            .stdout
            .await
            .context("joining the Helm stdout reader")??;
        let stderr = self
            .stderr
            .await
            .context("joining the Helm stderr reader")??;
        Ok((
            status.success(),
            String::from_utf8_lossy(&stdout).to_string(),
            String::from_utf8_lossy(&stderr).to_string(),
        ))
    }

    async fn terminate(mut self) {
        let _ = terminate_helm_process(&mut self.child).await;
        self.stdout.abort();
        self.stderr.abort();
        let _ = self.stdout.await;
        let _ = self.stderr.await;
    }
}

struct RunningGvisorEventWatch {
    child: Child,
    stdout: tokio::io::Lines<BufReader<ChildStdout>>,
    existing_event_uids: BTreeSet<String>,
}

fn gvisor_event_selector(namespace: &str, job: &str) -> String {
    format!(
        "involvedObject.kind=Job,involvedObject.namespace={namespace},involvedObject.name={job},reason=FailedCreate"
    )
}

fn gvisor_event_watch_cmd(namespace: &str, job: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("events"),
            plain("-n"),
            plain(namespace),
            plain("--field-selector"),
            plain(gvisor_event_selector(namespace, job)),
            plain("--watch"),
            plain("--output-watch-events"),
            plain("-o"),
            plain(
                r#"jsonpath={.type}{"\u001f"}{.object.metadata.uid}{"\u001f"}{.object.involvedObject.kind}{"\u001f"}{.object.involvedObject.namespace}{"\u001f"}{.object.involvedObject.name}{"\u001f"}{.object.reason}{"\u001f"}{.object.message}{"\n"}"#,
            ),
        ],
    )
}

fn gvisor_existing_event_uids_cmd(namespace: &str, job: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("events"),
            plain("-n"),
            plain(namespace),
            plain("--field-selector"),
            plain(gvisor_event_selector(namespace, job)),
            plain("-o"),
            plain(r#"jsonpath={range .items[*]}{.metadata.uid}{"\n"}{end}"#),
        ],
    )
}

enum GvisorEventWatchStart {
    Watching(Box<RunningGvisorEventWatch>),
    Unavailable,
}

enum GvisorEventWatchLine {
    RuntimeClassRejected(String),
    Ignore,
}

async fn gvisor_existing_event_uids(namespace: &str, job: &str) -> Option<BTreeSet<String>> {
    let ui = crate::ui::ui();
    let snapshot = gvisor_existing_event_uids_cmd(namespace, job);
    ui.plumbing(&format!("+ {}", snapshot.display()));
    match run_capture(&snapshot).await {
        Ok((true, out, _)) => Some(
            out.lines()
                .map(str::trim)
                .filter(|uid| !uid.is_empty())
                .map(str::to_string)
                .collect(),
        ),
        Ok((false, out, err)) => {
            let detail = [err.trim(), out.trim()]
                .into_iter()
                .find(|detail| !detail.is_empty())
                .unwrap_or("kubectl exited nonzero with no output");
            ui.plumbing(&format!("gVisor event snapshot unavailable: {detail}"));
            None
        }
        Err(error) => {
            ui.plumbing(&format!("gVisor event snapshot unavailable: {error}"));
            None
        }
    }
}

fn start_gvisor_event_watch(
    namespace: &str,
    job: &str,
    existing_event_uids: BTreeSet<String>,
) -> GvisorEventWatchStart {
    let ui = crate::ui::ui();
    let watch = gvisor_event_watch_cmd(namespace, job);
    ui.plumbing(&format!("+ {}", watch.display()));
    let mut child = match Command::new(&watch.program)
        .args(watch.argv())
        .envs(watch.env.iter().chain(watch.secret_env.iter()).cloned())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(error) => {
            ui.plumbing(&format!("gVisor event watch unavailable: {error}"));
            return GvisorEventWatchStart::Unavailable;
        }
    };
    let stdout = child
        .stdout
        .take()
        .expect("piped kubectl event stdout must be available");
    GvisorEventWatchStart::Watching(Box::new(RunningGvisorEventWatch {
        child,
        stdout: BufReader::new(stdout).lines(),
        existing_event_uids,
    }))
}

fn gvisor_event_watch_line(
    line: &str,
    namespace: &str,
    job: &str,
    existing_event_uids: &BTreeSet<String>,
) -> GvisorEventWatchLine {
    let mut fields = line.splitn(7, '\u{001f}');
    let source = fields.next().unwrap_or_default();
    let uid = fields.next().unwrap_or_default();
    let inspect = match source {
        "ADDED" | "MODIFIED" => !existing_event_uids.contains(uid),
        _ => false,
    };
    if uid.is_empty() || !inspect {
        return GvisorEventWatchLine::Ignore;
    }
    let kind = fields.next().unwrap_or_default();
    let event_namespace = fields.next().unwrap_or_default();
    let name = fields.next().unwrap_or_default();
    let reason = fields.next().unwrap_or_default();
    let message = fields.next().unwrap_or_default();
    let missing_runtimeclass = message.contains("RuntimeClass \"gvisor\" not found");
    if kind == "Job"
        && event_namespace == namespace
        && name == job
        && reason == "FailedCreate"
        && missing_runtimeclass
    {
        GvisorEventWatchLine::RuntimeClassRejected(message.to_string())
    } else {
        GvisorEventWatchLine::Ignore
    }
}

async fn terminate_process(child: &mut Child) -> std::io::Result<std::process::ExitStatus> {
    if let Ok(Some(status)) = child.try_wait() {
        return Ok(status);
    }
    let _ = child.start_kill();
    child.wait().await
}

/// Give Helm its interrupt path so it can mark the release failed before a
/// bounded forced cleanup. A pending install would block the printed recovery.
async fn terminate_helm_process(child: &mut Child) -> std::io::Result<std::process::ExitStatus> {
    if let Ok(Some(status)) = child.try_wait() {
        return Ok(status);
    }
    let interrupted = match child.id() {
        Some(pid) => Command::new("kill")
            .arg("-INT")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .await
            .is_ok_and(|status| status.success()),
        None => false,
    };
    if interrupted {
        if let Ok(status) = tokio::time::timeout(Duration::from_secs(2), child.wait()).await {
            return status;
        }
    }
    terminate_process(child).await
}

enum GvisorInstallRace {
    Helm(std::io::Result<std::process::ExitStatus>),
    RuntimeClassRejected(String),
}

enum GvisorInstallOutcome {
    Installed,
    RuntimeClassRejected {
        rejection: String,
        step: crate::ui::Step,
    },
}

async fn run_install_with_gvisor_observer(
    cl: &crate::ui::Checklist,
    label: &str,
    ok_detail: &str,
    cmd: &OpsCommand,
    namespace: &str,
    job: &str,
    namespace_existed_before_install: bool,
) -> Result<GvisorInstallOutcome> {
    let ui = crate::ui::ui();
    ui.plumbing(&format!("+ {}", cmd.display()));
    let step = cl.step(label);
    let mut watch_start = if namespace_existed_before_install {
        Some(match gvisor_existing_event_uids(namespace, job).await {
            Some(existing_event_uids) => {
                start_gvisor_event_watch(namespace, job, existing_event_uids)
            }
            None => GvisorEventWatchStart::Unavailable,
        })
    } else {
        None
    };
    let mut install = match RunningInstall::spawn(cmd) {
        Ok(install) => install,
        Err(error) => {
            if let Some(GvisorEventWatchStart::Watching(watch)) = &mut watch_start {
                let _ = terminate_process(&mut watch.child).await;
            }
            step.fail("failed");
            return Err(error);
        }
    };

    let mut early_helm_status = None;
    if !namespace_existed_before_install {
        // Helm owns `--create-namespace`. Wait for that one object, then use a
        // single list and watch request that cannot lose an Event between calls.
        let mut retry_delay = Duration::from_millis(50);
        loop {
            match namespace_exists(namespace).await {
                Ok(true) => {
                    watch_start = Some(start_gvisor_event_watch(namespace, job, BTreeSet::new()));
                    break;
                }
                Ok(false) => {}
                Err(error) => {
                    ui.plumbing(&format!(
                        "gVisor event watch unavailable while waiting for namespace: {error:#}"
                    ));
                    watch_start = Some(GvisorEventWatchStart::Unavailable);
                    break;
                }
            }
            match install.child.try_wait() {
                Ok(Some(status)) => {
                    early_helm_status = Some(Ok(status));
                    break;
                }
                Err(error) => {
                    early_helm_status = Some(Err(error));
                    break;
                }
                Ok(None) => {}
            }
            tokio::time::sleep(retry_delay).await;
            retry_delay = retry_delay
                .saturating_mul(2)
                .min(Duration::from_millis(500));
        }
    }

    let watch_start = watch_start.unwrap_or(GvisorEventWatchStart::Unavailable);
    let mut watch = None;
    let race = if let Some(status) = early_helm_status {
        GvisorInstallRace::Helm(status)
    } else {
        match watch_start {
            GvisorEventWatchStart::Watching(running_watch) => {
                watch = Some(*running_watch);
                let running_watch = watch.as_mut().expect("watch was just installed");
                let existing_event_uids = running_watch.existing_event_uids.clone();
                loop {
                    let outcome = tokio::select! {
                        status = install.child.wait() => Some(GvisorInstallRace::Helm(status)),
                        line = running_watch.stdout.next_line() => {
                            match line {
                                Ok(Some(line)) => match gvisor_event_watch_line(
                                    &line,
                                    namespace,
                                    job,
                                    &existing_event_uids,
                                ) {
                                    GvisorEventWatchLine::RuntimeClassRejected(rejection) => {
                                        Some(GvisorInstallRace::RuntimeClassRejected(rejection))
                                    }
                                    GvisorEventWatchLine::Ignore => None,
                                },
                                Ok(None) | Err(_) => {
                                    let _ = terminate_process(&mut running_watch.child).await;
                                    Some(GvisorInstallRace::Helm(install.child.wait().await))
                                }
                            }
                        }
                    };
                    if let Some(outcome) = outcome {
                        break outcome;
                    }
                }
            }
            GvisorEventWatchStart::Unavailable => {
                GvisorInstallRace::Helm(install.child.wait().await)
            }
        }
    };

    match race {
        GvisorInstallRace::Helm(status) => {
            if let Some(watch) = watch.as_mut() {
                let _ = terminate_process(&mut watch.child).await;
            }
            let captured = install.finish(status).await;
            match captured {
                Ok((ok, out, err)) => {
                    finish_captured_step(step, ok_detail, cmd, ok, out, err)?;
                    Ok(GvisorInstallOutcome::Installed)
                }
                Err(error) => {
                    step.fail("failed");
                    Err(error)
                }
            }
        }
        GvisorInstallRace::RuntimeClassRejected(rejection) => {
            if let Some(watch) = watch.as_mut() {
                let _ = terminate_process(&mut watch.child).await;
            }
            install.terminate().await;
            Ok(GvisorInstallOutcome::RuntimeClassRejected { rejection, step })
        }
    }
}

/// Run one command under a checklist `step` labeled `label`, capturing its
/// stdio. Echoes the masked command line and replays the captured output as dim
/// plumbing (both no-ops unless `--debug`, so default runs stay quiet and the
/// helm/kubectl/compose chatter is hidden). On success the step freezes done
/// with `ok_detail`; on a nonzero exit it freezes failed, surfaces the captured
/// stderr via `ui.failure`, and bails. Returns captured stdout.
pub(crate) async fn run_step(
    cl: &crate::ui::Checklist,
    label: &str,
    ok_detail: &str,
    cmd: &OpsCommand,
) -> Result<String> {
    let ui = crate::ui::ui();
    ui.plumbing(&format!("+ {}", cmd.display()));
    let step = cl.step(label);
    let (ok, out, err) = run_capture(cmd).await?;
    finish_captured_step(step, ok_detail, cmd, ok, out, err)
}

// ---------------------------------------------------------------------------
// Verb handlers
// ---------------------------------------------------------------------------

/// Output of `cluster up`: the dry-run plan, or the installed release. `--json`
/// emits a JSON object; the real path formerly ended in `ui.payload` (suppressed
/// under `--json`, #485).
#[derive(Debug)]
pub enum ClusterUpOutput {
    DryRun(crate::ui::DryRunPlan),
    Up { namespace: String, release: String },
}

impl crate::ui::CliOutput for ClusterUpOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterUpOutput::DryRun(plan) => plan.to_json(),
            ClusterUpOutput::Up { namespace, release } => serde_json::json!({
                "status": "up",
                "namespace": namespace,
                "release": release,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterUpOutput::DryRun(plan) => plan.render(ui),
            ClusterUpOutput::Up { .. } => {
                ui.payload("curie is up");
                ui.note("Run `curie cluster status` for pod health and URLs.");
            }
        }
    }
}

/// Whether `up()` should read the release's existing helm values before
/// resolving secrets and the GitHub credential.
///
/// Deliberately does NOT depend on `dev`: a `cluster up --dev` must still
/// preserve whatever credential (e.g. the GitHub token, #1124) a real release
/// already recorded -- `--dev` only governs the chart's dev-default secret
/// values, not whether the read happens. Re-adding a `dev` term here is the
/// specific regression this function exists to make visible. `dry_run` is the
/// only thing that skips the read, since `--dry-run` stays fully offline and
/// never touches helm.
fn should_read_existing(dev: bool, dry_run: bool) -> bool {
    let _ = dev;
    !dry_run
}

/// Refuse a `--dev` run that would flip an already-installed release onto the
/// chart's published dev defaults (#1145).
///
/// `curie.managedSecret`'s dev-defaults branch short-circuits *ahead of* its
/// `hasKey .existingData` preservation branch, so pointing an existing sealed
/// release at dev defaults rewrites every chart-owned credential in the release
/// Secret while the PVC-backed Postgres and RustFS data keeps the originals.
/// That leaves the install unbootable until those PVCs are wiped -- a state the
/// operator cannot back out of -- so the run is refused up front rather than
/// diagnosed afterwards.
///
/// The decision, in order:
///
/// 1. `dev == false` -> always allowed. A plain `up` over a *dev* release is
///    safe by construction: [`resolve_generated_secrets`] re-supplies only what
///    the release already recorded and never mints a value for an unrecorded
///    key, so `managedSecret` sees `value == default` and preserves. If that
///    ever became mint-on-missing, this arm has to change with it.
/// 2. `existing.is_none()` -> allowed. Either helm positively reported
///    "release: not found" (the fresh install `--dev` exists for), or the read
///    was skipped. `--dry-run` is that second case -- `should_read_existing`
///    returns `false` for it -- so a `--dev --dry-run` plan is never refused:
///    it reaches no helm invocation and so has no release to damage.
/// 3. The release already records dev defaults -> allowed. That is the
///    idempotent `--dev` re-run, and also the retry after a `--dev` install
///    that failed partway, since helm records a failed install's values.
/// 4. The operator explicitly supplied [`ALLOW_DEV_DEFAULTS_KEY`] through
///    `--set` or `--set-string` -> allowed. `up_value_plan` emits the CLI's own
///    `=true` FIRST and appends the operator's expressions AFTER it, and helm is
///    last-wins, so the operator's value is the one the chart actually renders:
///    `--dev --set security.allowDevDefaults=false` renders the flag OFF and
///    preserves the recorded credentials. Honouring it is therefore not a
///    bypass, it is reading the same effective value the chart will. Kept
///    DELIBERATELY NARROW: membership of that EXACT dotted key, never a
///    substring and never "some override was passed" -- almost every real
///    `cluster up` carries unrelated overrides, so the wider spelling would
///    disable the guard outright.
/// 5. Anything else -> refused, Usage class (exit 2), a deterministic input
///    error rather than a runtime failure. The truthiness read fails closed, so
///    an unrecognised recorded shape refuses rather than waves through.
///
/// Arm 4 sits ahead of the refusal because it decides what helm renders, not
/// what the CLI intended. It also subsumes `--set security.allowDevDefaults=true`
/// staying unguarded on purpose: that is the documented verbatim escape hatch,
/// and this file's standing rule is that an operator `--set` always wins. Do not
/// "close the gap" by guarding it. It is kept out of the operator-facing message
/// and fix hint too, because against an existing release it is destructive
/// advice.
fn guard_dev_defaults_flip(
    dev: bool,
    existing: Option<&serde_json::Value>,
    operator_sets: &[String],
) -> Result<()> {
    if !dev {
        return Ok(());
    }
    let Some(values) = existing else {
        return Ok(());
    };
    if lookup_dotted_flag(values, ALLOW_DEV_DEFAULTS_KEY) {
        return Ok(());
    }
    if operator_set_entries(operator_sets)
        .into_iter()
        .any(|(key, _)| key.trim() == ALLOW_DEV_DEFAULTS_KEY)
    {
        return Ok(());
    }
    // `curie cluster down` is deliberately NOT the headline advice. It deletes
    // only the namespaces the release itself created and leaves a pre-existing
    // one untouched (#707), and the runtime PVCs are not helm-owned -- they go
    // only with that namespace sweep. Followed there it would delete the release
    // Secret holding the store credentials while the data outlives it, which is
    // the very lockout this guard exists to prevent, so the teardown path has to
    // name the PVCs explicitly (#1145).
    let fix = "re-run without `--dev`, which preserves the credentials the release already \
               recorded; if a dev-default stack is genuinely what you want here, the \
               backing-store PVCs have to go WITH the release before you reinstall -- \
               `curie cluster down` alone does not remove them when the release was \
               installed into a pre-existing namespace, and dropping the release while \
               that data survives locks you out of your own stores";
    Err(crate::exit::CliError::usage(
        "this release was not installed with `--dev`, so `--dev` is refused: it would rewrite \
         every chart-owned credential in the release Secret to the published dev defaults \
         while the Postgres and RustFS data on the release's PVCs still holds the original \
         generated credentials, so the store init hook times out and the API migrate init \
         container fails password authentication, leaving the install broken until the PVCs \
         are wiped",
    )
    .with_fix(fix)
    .into())
}

enum UpInferencePolicy {
    Detect(Vec<ClusterUpInference>),
    Disabled,
}

pub async fn up(
    mut opts: UpOpts,
    github_token: Option<String>,
    clear_github_token: bool,
) -> Result<ClusterUpOutput> {
    validate_up_inputs(&opts, github_token.as_deref(), clear_github_token)?;
    validate_credential_egress_consistency(&opts)?;
    provider_contradiction(&opts, &up_value_plan(&opts))?;
    let resolve_provider_egress = !opts.common.dry_run;
    let existing = if should_read_existing(opts.dev, opts.common.dry_run) {
        require_on_path("helm")?;
        fetch_existing_values(&opts.common).await?
    } else {
        None
    };
    opts = complete_up_opts_without_runner_egress(
        opts,
        existing.as_ref(),
        github_token.as_deref(),
        clear_github_token,
    )?;
    validate_credential_egress_consistency(&opts)?;
    let completed_identity_plan = up_value_plan(&opts);
    let mut inferences = Vec::new();
    let provider_inference = reconcile_provider_inference(&mut opts, &completed_identity_plan)?;
    let provider_was_inferred = provider_inference.is_some();
    if let Some(inference) = provider_inference {
        inferences.push(inference);
    }
    let operator_sets = opts.operator_sets();
    let (next_preserved_egress_index, recorded_egress_cidrs) =
        resolve_preserved_runner_egress_values(
            &mut opts,
            existing.as_ref(),
            &operator_sets,
            provider_was_inferred,
        );
    resolve_provider_egress_for_up(&mut opts, resolve_provider_egress)?;
    if provider_was_inferred && next_preserved_egress_index > 0 {
        reindex_inferred_provider_egress(
            &mut opts,
            next_preserved_egress_index,
            &recorded_egress_cidrs,
        );
    }
    let value_plan = up_value_plan(&opts);
    run_prepared_up(
        opts,
        value_plan,
        existing,
        github_token.as_deref(),
        UpInferencePolicy::Detect(inferences),
    )
    .await
}

/// Execute an up plan whose local validation and live completion already ran.
/// Installation apply uses this after it has shared that completed plan with
/// diff, avoiding another values read or provider lookup.
pub(crate) async fn up_prepared(
    opts: UpOpts,
    value_plan: UpValuePlan,
    existing: Option<serde_json::Value>,
    github_token: Option<String>,
) -> Result<ClusterUpOutput> {
    validate_up_inputs(&opts, github_token.as_deref(), false)?;
    run_prepared_up(
        opts,
        value_plan,
        existing,
        github_token.as_deref(),
        UpInferencePolicy::Disabled,
    )
    .await
}

async fn run_prepared_up(
    opts: UpOpts,
    mut value_plan: UpValuePlan,
    existing: Option<serde_json::Value>,
    github_token: Option<&str>,
    inference_policy: UpInferencePolicy,
) -> Result<ClusterUpOutput> {
    // The single call site, and deliberately the first statement of the single
    // choke point both `up()` and `up_prepared()` funnel through: upstream of
    // the `inference.render(ui)` loop, of the preservation notes, and of every
    // helm invocation, so a refused run (#1145) emits the error and nothing
    // else.
    guard_dev_defaults_flip(opts.dev, existing.as_ref(), &opts.operator_sets())?;
    let ui = crate::ui::ui();
    let (detect_facts, initial_inferences) = match inference_policy {
        UpInferencePolicy::Detect(inferences) => (true, inferences),
        UpInferencePolicy::Disabled => (false, Vec::new()),
    };
    for inference in &initial_inferences {
        inference.render(ui);
    }
    let operator_sets = opts.operator_sets();
    if let Some(values) = &opts.retained_mail_values {
        ui.note(&format!(
            "preserving {} mail value(s) recorded by the release",
            values.keys().len()
        ));
    }
    let preserved = resolve_preserved_values(existing.as_ref(), &operator_sets);
    if !preserved.is_empty() {
        let sealing_values = preserved
            .iter()
            .filter(|(key, _)| crate::sealing::SEALING_MANAGED_KEYS.contains(&key.as_str()))
            .count();
        let message = if sealing_values == preserved.len() {
            format!(
                "preserving {} sealing value(s) recorded by the release",
                preserved.len()
            )
        } else if sealing_values == 0 {
            format!(
                "preserving {} value(s) recorded by `cluster comms` / `cluster github-app`; re-run those verbs only to change them",
                preserved.len()
            )
        } else {
            format!(
                "preserving {} value(s), including {} sealing value(s), recorded by the release",
                preserved.len(),
                sealing_values
            )
        };
        ui.note(&message);
    }
    if !opts.dev {
        match sealing_private_key_disposition(
            existing.as_ref(),
            &operator_sets,
            opts.common.dry_run,
        ) {
            SealingPrivateKeyDisposition::Generated => {
                ui.note("generated a sealing private key for this release; later cluster up runs preserve it");
            }
            SealingPrivateKeyDisposition::Deferred => {
                ui.note("a live run discovers sealing state and preserves an existing private key or generates one when absent; skipped here to keep --dry-run offline");
            }
            SealingPrivateKeyDisposition::OperatorSet | SealingPrivateKeyDisposition::Preserved => {
            }
        }
        if existing.is_none() && !opts.common.dry_run {
            let generated_required_secrets = opts
                .secrets
                .iter()
                .filter(|(key, _)| {
                    REQUIRED_SECRETS
                        .iter()
                        .any(|(required, _)| *required == key.as_str())
                })
                .count();
            if generated_required_secrets > 0 {
                ui.note(&format!(
                    "generated strong per-release secrets for {generated_required_secrets} required chart credential(s); re-running `cluster up` reuses them"
                ));
            }
        }
    }
    // `existing.is_some()` means this is an UPGRADE: an API pod is already
    // running on the old value and keeps it until it restarts (the api pod
    // template carries no checksum/secret annotation). On a FRESH install the
    // pod has not started yet and comes up with the new value, so the restart
    // advice would be false and is suppressed.
    let upgrading = existing.is_some();
    // Named by label selector, not `{release}-api`: the chart derives the
    // actual Deployment name through `curie.fullname`, which is only
    // `{release}-curie` unless the release name already contains "curie"
    // (`charts/curie/templates/_helpers.tpl:15-25`), so a literal `-api`
    // suffix on the release name is wrong under a non-default `--release`.
    // `app.kubernetes.io/instance={release}` plus the fixed `component: api`
    // label match the Deployment's own `selectorLabels` regardless of any
    // `nameOverride`/`fullnameOverride`, so this is correct in every case.
    let restart_hint = format!(
        "kubectl -n {} rollout restart deployment -l {}",
        opts.common.namespace,
        component_selector(&opts.common.release, "api")
    );
    // An explicit but EMPTY `--github-token` / `CURIE_GITHUB_TOKEN`: a routine
    // shell accident that preserves rather than clears (`resolve_github_token`),
    // so each arm below folds it into its own single note.
    let empty_flag = github_token.is_some_and(str::is_empty);
    match &opts.github_token {
        GithubTokenPlan::Set(_) if github_token.is_some_and(|v| !v.is_empty()) => {
            if upgrading {
                ui.note(&format!(
                    "GitHub credential set through the private values path; the running API keeps the old one until it restarts: {restart_hint}"
                ));
            } else {
                ui.note("GitHub credential set through the private values path");
            }
        }
        GithubTokenPlan::Clear => {
            // A clear only actually removes something when a credential was
            // recorded; on a fresh install (or an existing release that never
            // had one) there is nothing to remove, so the wording must not
            // claim there was.
            if preserved_value(existing.as_ref(), GITHUB_TOKEN_KEY).is_some() {
                // This is an incident-response verb, not a revocation: the
                // running API keeps the old token until it restarts, and the
                // token stays valid at GitHub until revoked there directly.
                ui.note(&format!(
                    "GitHub credential cleared here, not revoked: the running API keeps the old token until it restarts ({restart_hint}), and the token is still valid at GitHub until you revoke it at https://github.com/settings/tokens (or rotate the App installation credential)"
                ));
            } else {
                ui.note("no GitHub credential was recorded; nothing to clear");
            }
        }
        GithubTokenPlan::Set(_) => {
            // Preservation, reached either by a plain `up` or by an EMPTY
            // explicit value (state 4 in `resolve_github_token`). One note per
            // outcome: when the value was empty, say so here rather than
            // trailing a second, overlapping note after this match.
            if empty_flag {
                ui.note("--github-token (or CURIE_GITHUB_TOKEN) was empty; preserving the GitHub credential recorded by an earlier cluster up. Pass --clear-github-token to remove it.");
            } else {
                ui.note("preserving the GitHub credential recorded by an earlier cluster up; pass --github-token to change it or --clear-github-token to remove it");
            }
        }
        GithubTokenPlan::Untouched => {
            // Distinct wording: an empty value with nothing recorded preserves
            // nothing, so it must not read as if it kept a credential.
            if empty_flag {
                ui.note("--github-token (or CURIE_GITHUB_TOKEN) was empty; no GitHub credential is recorded to keep.");
            }
        }
    }
    if set_passthrough_leaks_github_token(&opts.operator_sets()) {
        ui.warn("a GitHub credential passed with --set lands in the process table and shell history; use --github-token, or CURIE_GITHUB_TOKEN to keep it out of shell history too");
    }

    if !opts.allow_egress_host.is_empty()
        && opts.common.dry_run
        && opts.resolved_egress_cidrs.is_empty()
    {
        let named = opts
            .allow_egress_host
            .iter()
            .map(|provider| match provider_egress_hosts(provider) {
                Some(hosts) if !hosts.is_empty() => format!("{provider} ({})", hosts.join(", ")),
                _ => provider.clone(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        ui.note(&format!(
            "a live run resolves {named} to narrow /32+/128 host routes and opens runner egress to the resolved addresses (skipped here to keep --dry-run offline)"
        ));
    }

    let mut cmds = up_commands_with_plan(&opts, &value_plan);

    // #707 record ownership only on namespaces THIS run creates, so a later
    // `down` sweeps exactly what `up` made and leaves pre-existing state alone.
    // `up` may create the release namespace (via `helm --create-namespace`) and
    // the chart-created `agent-sandbox-system` -- but the latter is
    // chart-conditional (created only when `agentSandbox.controller.deploy` is
    // true), so a `--set agentSandbox.controller.deploy=false` release never
    // creates it. Probe each candidate BEFORE the install so a namespace that
    // already existed is adopted, not stamped (`existed_before`, recorded in
    // `ownership_candidates`); the actual stamp attempt is gated a SECOND time
    // AFTER the install (see `should_stamp_ownership` below, run once `cmds` has
    // executed) against whether the namespace exists now, so a namespace the
    // chart never created is simply not stamped instead of failing `kubectl
    // label namespace <missing>`. Mirror the resolve_generated_secrets
    // existing/fresh split: both runtime probes live here in the executor, the
    // argv stays in the pure `ownership_label_commands` builder. `--dry-run`
    // stays offline and previews the fresh-install stamp for every candidate
    // (existed == false), never touching the cluster and never running the
    // post-install probe.
    let mut owned_namespaces = vec![opts.common.namespace.clone()];
    if opts.common.namespace != "agent-sandbox-system" {
        owned_namespaces.push("agent-sandbox-system".to_string());
    }
    if !opts.common.dry_run {
        require_on_path("kubectl")?;
    }
    let mut ownership_candidates: Vec<(String, bool)> = Vec::new();
    for ns in owned_namespaces {
        let existed_before = if opts.common.dry_run {
            false
        } else {
            namespace_exists(&ns).await?
        };
        if opts.common.dry_run {
            // existed_before is provably false on this branch (set above).
            let common = ns_common(&opts.common, &ns, true);
            cmds.extend(ownership_label_commands(
                &common,
                &opts.common.namespace,
                false,
            ));
        }
        ownership_candidates.push((ns, existed_before));
    }

    // Count named provider intent on both live and dry runs, plus egress
    // implied by allow_egress_host resolution. Preserve explicit web egress
    // and nonempty allowedEgress overrides supplied through either value lane.
    let any_egress = !opts.allow_egress_host.is_empty()
        || !opts.allow_web_egress.is_empty()
        || operator_set_entries(&opts.operator_sets())
            .into_iter()
            .any(|(key, value)| {
                key_is_or_descends_from(key.trim(), ALLOWED_EGRESS_KEY)
                    && !matches!(value.trim(), "" | "[]")
            });
    for (warn, msg) in model_egress_status_lines(
        opts.credentials.is_some(),
        opts.local_model.is_some(),
        opts.fake_model,
        &opts.allow_egress_host,
        any_egress,
        opts.common.dry_run,
    ) {
        if warn {
            ui.warn(&msg)
        } else {
            ui.note(&msg)
        }
    }
    if let Some(warning) = default_route_egress_warning(&opts.allow_web_egress) {
        ui.warn(&warning);
    }
    if !opts.allow_web_egress.is_empty() {
        ui.note(&format!(
            "web egress opened to {} declared destination(s)",
            opts.allow_web_egress.len()
        ));
    }
    if opts.common.dry_run {
        let mut lines: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
        lines.push("# After Helm and ownership stamping, verify convergence for at most 300 seconds; repeat observations every 2 seconds while rollout is pending.".to_owned());
        lines.push(convergence::DRY_RUN_NOTE.to_owned());
        lines.extend(
            convergence::dry_run_commands(&opts.common)
                .iter()
                .map(OpsCommand::display),
        );
        return Ok(ClusterUpOutput::DryRun(crate::ui::DryRunPlan { lines }));
    }
    let release_namespace_existed_before_install = ownership_candidates
        .iter()
        .find_map(|(namespace, existed)| (namespace == &opts.common.namespace).then_some(*existed))
        .unwrap_or(false);
    require_on_path("helm")?;
    if detect_facts {
        for inference in reconcile_priority_class_ownership(&opts, &mut value_plan).await? {
            inference.render(ui);
        }
        if let Some(inference) = reconcile_controller_ownership(&opts, &mut value_plan).await? {
            inference.render(ui);
        }
    } else {
        preflight_priority_class_ownership(&opts, &value_plan).await?;
    }
    cmds = up_commands_with_plan(&opts, &value_plan);
    let gvisor_preflight_job =
        rendered_gvisor_preflight_job(&opts.chart, &opts.common, &value_plan).await?;
    let cl = ui.checklist();
    let label = format!("installing release {}", opts.common.release);
    for cmd in &cmds {
        if let Some(job) = gvisor_preflight_job.as_deref() {
            let outcome = match run_install_with_gvisor_observer(
                &cl,
                &label,
                "installed",
                cmd,
                &opts.common.namespace,
                job,
                release_namespace_existed_before_install,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    return Err(convergence::installation_failure(&opts.common, error).await)
                }
            };
            match outcome {
                GvisorInstallOutcome::Installed => {}
                GvisorInstallOutcome::RuntimeClassRejected { rejection, step } if detect_facts => {
                    if let Some(mode @ ("auto" | "require")) =
                        final_operator_value(&opts, GVISOR_MODE_KEY)
                    {
                        step.fail("failed");
                        let assignment = format!("{GVISOR_MODE_KEY}={mode}");
                        let fix = format!(
                            "remove the explicit `{assignment}` setting and rerun to accept the inferred gVisor posture"
                        );
                        return Err(crate::exit::CliError::usage(format!(
                            "explicit `{assignment}` contradicts the detected admission result `{rejection}`; {fix}"
                        ))
                        .with_fix(fix)
                        .into());
                    }
                    step.warn("retrying");
                    value_plan.set(GVISOR_MODE_KEY, "off");
                    ClusterUpInference::GvisorOff.render(ui);
                    let retry = up_commands_with_plan(&opts, &value_plan)
                        .into_iter()
                        .next()
                        .expect("cluster up always has one Helm command");
                    if let Err(error) = run_step(&cl, &label, "installed", &retry).await {
                        return Err(convergence::installation_failure(&opts.common, error).await);
                    }
                }
                GvisorInstallOutcome::RuntimeClassRejected { rejection, step } => {
                    step.fail("failed");
                    let fix = "curie cluster up --set security.gvisor.mode=off";
                    return Err(crate::exit::CliError::failure(format!(
                        "gVisor preflight Job `{job}` could not create its pod: {rejection}. To install without gVisor isolation, run `{fix}`."
                    ))
                    .with_fix(fix)
                    .into());
                }
            }
        } else {
            if let Err(error) = run_step(&cl, &label, "installed", cmd).await {
                return Err(convergence::installation_failure(&opts.common, error).await);
            }
        }
    }

    // #707 stamp ownership only on namespaces this run actually created. A
    // candidate that did not exist before the install may still not exist
    // after it -- `agent-sandbox-system` under
    // `--set agentSandbox.controller.deploy=false` is the concrete case, since
    // the chart only creates that namespace when the sandbox controller
    // subchart is deployed -- so re-probe existence here, post-helm, and skip
    // the label attempt for anything still absent rather than let `kubectl
    // label namespace <missing>` fail the whole `up`. A namespace that
    // pre-existed is never re-probed or stamped at all.
    for (ns, existed_before) in &ownership_candidates {
        if *existed_before {
            continue;
        }
        let exists_after = namespace_exists(ns).await?;
        if !should_stamp_ownership(false, exists_after) {
            continue;
        }
        let common = ns_common(&opts.common, ns, false);
        for cmd in ownership_label_commands(&common, &opts.common.namespace, false) {
            run_step(&cl, &label, "installed", &cmd).await?;
        }
    }

    let step = cl.step("waiting for exact target workload convergence");
    if let Err(error) = convergence::wait(&opts.common).await {
        step.fail("not converged");
        return Err(error);
    }
    step.done("converged");
    Ok(ClusterUpOutput::Up {
        namespace: opts.common.namespace.clone(),
        release: opts.common.release.clone(),
    })
}

/// Output of `cluster status`: the dry-run plan, or the release/pod/URL summary.
/// `--json` emits one JSON object; the real path formerly printed via
/// `ui.payload`/scattered helpers (all suppressed under `--json`, #485).
#[derive(Debug)]
pub enum ClusterStatusOutput {
    DryRun(crate::ui::DryRunPlan),
    Status(Box<ClusterStatus>),
}

/// The assembled `cluster status` reading. Owns its data so `to_json`/`render`
/// can run after every capture completes.
#[derive(Debug)]
pub struct ClusterStatus {
    pub namespace: String,
    pub revision: String,
    pub release_state: String,
    pub release_found: bool,
    pub release_missing_note: Option<String>,
    pub pods: Vec<PodRow>,
    pub ready: usize,
    pub total: usize,
    pub unhealthy: Vec<String>,
    pub pods_listed: bool,
    pub urls: Vec<ServiceUrl>,
}

impl crate::ui::CliOutput for ClusterStatusOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterStatusOutput::DryRun(plan) => plan.to_json(),
            ClusterStatusOutput::Status(s) => {
                let healthy = s.total > 0 && s.ready == s.total && s.unhealthy.is_empty();
                serde_json::json!({
                    "namespace": s.namespace,
                    "revision": s.revision,
                    "release_state": s.release_state,
                    "release_found": s.release_found,
                    "pods": {
                        "ready": s.ready,
                        "total": s.total,
                        "unhealthy": s.unhealthy,
                        "rows": s.pods.iter().map(PodRow::to_json).collect::<Vec<_>>(),
                    },
                    "urls": s.urls.iter().map(ServiceUrl::to_json).collect::<Vec<_>>(),
                    "healthy": healthy,
                })
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterStatusOutput::DryRun(plan) => plan.render(ui),
            ClusterStatusOutput::Status(s) => {
                ui.payload(&format!(
                    "curie · namespace {} · revision {} · {}",
                    s.namespace, s.revision, s.release_state
                ));
                if let Some(note) = &s.release_missing_note {
                    ui.note(note);
                }
                render_pod_table(ui, &s.pods);
                if !s.pods_listed {
                    ui.warn(&format!("could not list pods in namespace {}", s.namespace));
                }
                for url in &s.urls {
                    url.render(ui);
                }
                if s.total > 0 && s.ready == s.total && s.unhealthy.is_empty() {
                    ui.success(&format!("healthy ({}/{} pods ready)", s.ready, s.total));
                } else if s.total == 0 {
                    ui.warn("no pods running");
                } else {
                    let mut msg = format!("{}/{} pods ready", s.ready, s.total);
                    if !s.unhealthy.is_empty() {
                        msg.push_str(&format!("; not ready: {}", s.unhealthy.join(", ")));
                    }
                    ui.warn(&msg);
                }
            }
        }
    }
}

pub async fn status(opts: CommonOpts) -> Result<ClusterStatusOutput> {
    if opts.dry_run {
        // A dry run makes no cluster call, so the release's fullname cannot be
        // discovered and the chart's no-override rule is the honest best guess.
        // `dry_run_fullname` computes it and emits the caveat that says so.
        let fullname = dry_run_fullname(&opts.release);
        return Ok(ClusterStatusOutput::DryRun(crate::ui::DryRunPlan {
            lines: std::iter::once(convergence::DRY_RUN_NOTE.to_owned())
                .chain(
                    status_commands(&opts, &fullname)
                        .iter()
                        .map(OpsCommand::display),
                )
                .collect(),
        }));
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;

    // Five independent reads, issued as ONE stage.
    //
    // Safe because none of them consumes another's output: the helm status
    // line, the pod list, the convergence observation, the release's rendered
    // fullname and the node host each depend only on `opts`. Awaiting them in
    // turn made an operator wait through five round trips to a cluster that
    // could have answered all five at once, and the report is assembled below
    // in exactly the order it was before -- concurrency here changes when the
    // answers arrive, never which answer wins.
    //
    // `?` is applied to the helm result FIRST and the pod result second, so the
    // error an unreachable cluster surfaces is the same one it surfaced when
    // these ran in sequence.
    let helm_status = helm_status_cmd(&opts);
    let pods = pods_cmd(&opts);
    let (helm, pods_read, observed, fullname, host) = tokio::join!(
        run_capture(&helm_status),
        run_capture(&pods),
        convergence::observe(&opts),
        release_fullname(&opts.namespace, &opts.release),
        discover_host(),
    );

    // (a) Helm release state -> a bright header line.
    let (helm_ok, helm_out, helm_err) = helm?;
    let field = |name: &str, default: &str| -> String {
        helm_out
            .lines()
            .find(|l| l.trim_start().starts_with(name))
            .and_then(|l| l.split_once(':'))
            .map(|(_, v)| v.trim().to_string())
            .unwrap_or_else(|| default.to_string())
    };
    let (release_state, revision) = if helm_ok {
        (field("STATUS:", "unknown"), field("REVISION:", "?"))
    } else {
        ("not found".to_string(), "none".to_string())
    };
    let release_missing_note = (!helm_ok).then(|| {
        format!(
            "release {} not found: {}",
            opts.release,
            helm_err.trim().lines().next().unwrap_or("no such release")
        )
    });

    // (b) Pod health.
    let (ok, out, _) = pods_read?;
    let (pods, ready, total, mut unhealthy) = if ok {
        let items: Vec<serde_json::Value> = serde_json::from_str::<serde_json::Value>(&out)
            .ok()
            .and_then(|v| v.get("items").and_then(|i| i.as_array()).cloned())
            .unwrap_or_default();
        collect_pod_summary(&items)
    } else {
        (Vec::new(), 0, 0, Vec::new())
    };

    // The same target-manifest check gates up/apply and this read surface.
    // Keep the existing JSON schema: diagnoses use its unhealthy string list.
    // Consumed here, in the position it was awaited in before, so the
    // `unhealthy` list keeps its order: pod-summary entries, then the
    // convergence verdict, then the listing failure.
    match observed {
        Ok(observation) => unhealthy.extend(observation.issues),
        Err(error) => unhealthy.push(format!("convergence could not be verified: {error}")),
    }
    if !ok {
        unhealthy.push("could not list release pods".to_string());
    }

    // (c) URL discovery. The release's rendered fullname and the node host were
    // resolved in the stage above -- both are live-branch only, and `--dry-run`
    // returned before reaching any of it. The two Service reads DO consume the
    // fullname, so they wait for that stage and then fan out against each other
    // rather than queueing up one behind the other.
    let (ui_url, langfuse_url) = tokio::join!(
        resolve_service_url(&opts, &fullname, "ui", "UI", &host, true),
        resolve_service_url(&opts, &fullname, "langfuse-web", "Langfuse", &host, false),
    );
    let urls = vec![ui_url, langfuse_url];

    let output = ClusterStatusOutput::Status(Box::new(ClusterStatus {
        namespace: opts.namespace.clone(),
        revision,
        release_state,
        release_found: helm_ok,
        release_missing_note,
        pods,
        ready,
        total,
        unhealthy,
        pods_listed: ok,
        urls,
    }));
    let json = crate::ui::CliOutput::to_json(&output);
    if json["healthy"] != true {
        return Err(crate::ui::ui().failed_report(
            &output,
            crate::exit::CliError::failure(format!(
                "target release has not converged: {}",
                json["pods"]["unhealthy"].as_array().map(|reasons| reasons.iter().filter_map(serde_json::Value::as_str).collect::<Vec<_>>().join("; ")).unwrap_or_default()
            ))
                .with_fix("inspect the unhealthy rollout reasons, correct the target configuration and rerun `curie cluster up`")
                .into(),
        ));
    }
    Ok(output)
}

/// Output of `cluster down`: the dry-run plan, an operator abort, or the removed
/// release. `--json` emits a JSON object; the real path formerly ended in
/// `ui.payload` (suppressed under `--json`, #485).
#[derive(Debug)]
pub enum ClusterDownOutput {
    DryRun(crate::ui::DryRunPlan),
    Aborted,
    Down { release_was_absent: bool },
}

impl crate::ui::CliOutput for ClusterDownOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterDownOutput::DryRun(plan) => plan.to_json(),
            ClusterDownOutput::Aborted => serde_json::json!({"down": false, "aborted": true}),
            ClusterDownOutput::Down { release_was_absent } => serde_json::json!({
                "down": true,
                "release_was_absent": release_was_absent,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterDownOutput::DryRun(plan) => plan.render(ui),
            ClusterDownOutput::Aborted => ui.note("aborted"),
            ClusterDownOutput::Down { .. } => {
                ui.payload("curie is down");
                ui.note("The agents.x-k8s.io CRDs are left in place intentionally.");
            }
        }
    }
}

pub async fn down(opts: DownOpts) -> Result<ClusterDownOutput> {
    let ui = crate::ui::ui();
    let cmds = down_commands(&opts.common);
    if opts.common.dry_run {
        return Ok(ClusterDownOutput::DryRun(crate::ui::DryRunPlan {
            lines: cmds.iter().map(|cmd| cmd.display()).collect(),
        }));
    }
    ui.warn(&format!(
        "this uninstalls release '{0}' in namespace '{1}' and deletes only the namespaces that release created (labeled curietech.ai/created-by={0} AND curietech.ai/created-in={1}, so a namespace another release created is out of reach), leaving any pre-existing namespaces untouched",
        opts.common.release, opts.common.namespace
    ));
    if !opts.yes
        && !confirm(&format!(
            "This uninstalls release '{0}' in namespace '{1}' and deletes only the namespaces that release created (labeled curietech.ai/created-by={0} AND curietech.ai/created-in={1}, so a namespace another release created is out of reach). Continue? [y/N] ",
            opts.common.release, opts.common.namespace
        ))?
    {
        return Ok(ClusterDownOutput::Aborted);
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;

    let cl = ui.checklist();

    // helm uninstall, tolerating an already-absent release. On any OTHER failure
    // (e.g. a transient API-server blip) we do NOT bail: keep the stderr and fall
    // through to the sweep, so the run-created namespaces are never orphaned
    // (#767 fail-forward).
    let uninstall = &cmds[0];
    ui.plumbing(&format!("+ {}", uninstall.display()));
    let step = cl.step("uninstalling release");
    let (ok, out, helm_err) = run_capture(uninstall).await?;
    let helm_outcome = if ok {
        step.done("removed");
        HelmOutcome::Removed
    } else if helm_err.contains("not found") || out.contains("not found") {
        step.done("already absent");
        HelmOutcome::Absent
    } else {
        step.fail("failed");
        HelmOutcome::Failed
    };
    for line in out.lines().chain(helm_err.lines()) {
        ui.plumbing(line);
    }

    // Namespace sweep (runtime artifacts Helm does not own). Runs
    // UNCONDITIONALLY: it is Helm-independent by design (#707) and is what
    // actually stops compute, so a failed helm uninstall must not skip it. Not
    // `run_step`, which bails on a nonzero exit; here we tolerate and classify.
    let sweep = &cmds[1];
    ui.plumbing(&format!("+ {}", sweep.display()));
    let step = cl.step("sweeping namespaces");
    let (ok, out, sweep_err) = run_capture(sweep).await?;
    // #768: `--ignore-not-found` makes a zero-match selector exit 0 with EMPTY
    // stdout (no "namespace ... deleted" line), the same exit code an actual
    // removal gets. That is exactly the pre-existing-namespace case (#707
    // deliberately never stamps it), so a bare `ok` check cannot tell "nothing
    // to do" apart from "removed it"; only the stdout content can. Distinguish
    // them into their own outcome so `teardown_result` never claims compute was
    // stopped when the sweep matched nothing.
    let sweep_outcome = if ok {
        if out.trim().is_empty() {
            step.done("no matching namespaces");
            SweepOutcome::NoMatch
        } else {
            step.done("removed");
            SweepOutcome::Removed
        }
    } else {
        step.fail("failed");
        SweepOutcome::Failed
    };
    for line in out.lines().chain(sweep_err.lines()) {
        ui.plumbing(line);
    }

    // Pure decision (#767): success on a complete teardown, else a fail-forward
    // error whose exit class and message follow from the outcomes plus stderr.
    teardown_result(
        helm_outcome,
        sweep_outcome,
        &helm_err,
        &sweep_err,
        &opts.common,
    )
}

// ---------------------------------------------------------------------------
// `cluster rollback` (#1899)
// ---------------------------------------------------------------------------

/// One row of `helm history -o json`. Only the fields the rollback decision
/// needs are modelled; helm adds others (`updated`, `app_version`) that are
/// deliberately ignored so a helm version bump cannot break parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelmRevision {
    pub revision: u32,
    pub status: String,
    pub chart: String,
    pub description: String,
}

/// The revision statuses it is safe to roll back TO. Everything else
/// (`failed`, the four `pending-*` states, `uninstalling`, `uninstalled`,
/// `unknown`) names a revision helm never finished putting on the cluster, so
/// rolling back to it re-applies a manifest that was never known good.
const ROLLBACK_ELIGIBLE_STATUSES: [&str; 2] = ["deployed", "superseded"];

fn is_eligible_rollback_status(status: &str) -> bool {
    ROLLBACK_ELIGIBLE_STATUSES.contains(&status.trim().to_ascii_lowercase().as_str())
}

/// The revision the release is on right now.
///
/// Normally that is the one helm marks `deployed`. The fallback to the highest
/// revision is the real recovery case this verb exists for: when the newest
/// revision FAILED there is no `deployed` row at all, and treating the failed
/// revision as "current" is what lets the selector look below it.
fn current_revision(history: &[HelmRevision]) -> Option<u32> {
    history
        .iter()
        .filter(|r| r.status.trim().eq_ignore_ascii_case("deployed"))
        .map(|r| r.revision)
        .max()
        .or_else(|| history.iter().map(|r| r.revision).max())
}

/// [`current_revision`], or the error every entry point owes an operator whose
/// history came back empty. One copy on purpose: the wording here has already
/// been reworked twice, and a second hand-written copy would silently keep the
/// old text the next time it changes.
fn require_current_revision(history: &[HelmRevision]) -> Result<u32> {
    match current_revision(history) {
        Some(current) => Ok(current),
        None => Err(crate::exit::CliError::failure(
            "`helm history` returned no revisions for this release, so there is nothing to roll back to",
        )
        .with_fix("confirm the release name and namespace with `helm list -n <namespace>`")
        .into()),
    }
}

/// The INELIGIBLE revisions between `to` and `from`. The status filter is not
/// redundant: it is only the auto-select path that leaves nothing eligible in
/// the gap. An operator-named `--revision` can step over revisions that were
/// perfectly good, and reporting those as "not deployed/superseded" is a false
/// claim in both the note and the `--json` `skipped` field, so the list is
/// computed rather than assumed. Reporting the rest is the whole point of AC2.
///
/// Deduplicated defensively: [`parse_helm_history`] already collapses a corrupt
/// history to one row per revision, but this is pure over any slice a caller
/// hands it, and a number must never surface twice in the report.
fn skipped_between(history: &[HelmRevision], to: u32, from: u32) -> Vec<u32> {
    let mut skipped: Vec<u32> = history
        .iter()
        .filter(|r| r.revision > to && r.revision < from)
        .filter(|r| !is_eligible_rollback_status(&r.status))
        .map(|r| r.revision)
        .collect();
    skipped.sort_unstable();
    skipped.dedup();
    skipped
}

/// A resolved rollback: where the release is now, where it is going, and what
/// was stepped over on the way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackChoice {
    pub from_revision: u32,
    pub to_revision: u32,
    pub skipped: Vec<u32>,
    /// True only when `--allow-failed-revision` admitted a revision whose status
    /// is not `deployed`/`superseded`.
    pub forced: bool,
}

/// The outcome of the pure selection. `NoEligible` is a legitimate reading of a
/// real history (a first install, or a release whose every prior revision
/// failed), not a parse error, so it is carried as a value and turned into the
/// operator-facing refusal by [`RollbackTarget::require_eligible`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RollbackTarget {
    Eligible(RollbackChoice),
    NoEligible { current: u32, skipped: Vec<u32> },
}

impl RollbackTarget {
    /// The chosen revision, or the AC4 refusal: never a silent no-op, and never
    /// a rollback to a revision helm never finished applying.
    pub fn require_eligible(self) -> Result<RollbackChoice> {
        match self {
            RollbackTarget::Eligible(choice) => Ok(choice),
            RollbackTarget::NoEligible { current, skipped } => {
                let detail = if skipped.is_empty() {
                    format!("revision {current} is the only revision in its history")
                } else {
                    format!(
                        "every revision below {current} ({}) has a status helm never finished applying",
                        skipped
                            .iter()
                            .map(u32::to_string)
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                };
                // The next step rides in the message as well as the fix: a bare
                // `CliError` shows only its message to a human presenter, so an
                // operator on a TTY would otherwise never learn there is one.
                Err(crate::exit::CliError::failure(format!(
                    "no revision is safe to roll back to: {detail}; inspect \
                     `helm history <release> -n <namespace>` and, if you accept the risk, name one \
                     explicitly with --revision <n> --allow-failed-revision"
                ))
                .with_fix(
                    "inspect `helm history <release> -n <namespace>` and, if you accept the risk, \
                     name one explicitly with --revision <n> --allow-failed-revision",
                )
                .into())
            }
        }
    }
}

/// Pick the rollback target: the NEWEST revision strictly below the current one
/// whose status is `deployed` or `superseded`.
///
/// This is the whole fix for #1899. A `cluster up` against a cluster with no
/// `runsc` RuntimeClass records a FAILED revision before its successful retry,
/// so the history alternates failed/superseded and the immediately preceding
/// revision -- the one bare `helm rollback` targets -- is a failed one. Skipping
/// ineligible statuses is what stops an operator having to know that.
///
/// Pure by construction so the decision is unit-testable with no cluster.
pub fn select_rollback_revision(history: &[HelmRevision]) -> Result<RollbackTarget> {
    let current = require_current_revision(history)?;

    match history
        .iter()
        .filter(|r| r.revision < current && is_eligible_rollback_status(&r.status))
        .map(|r| r.revision)
        .max()
    {
        Some(to_revision) => Ok(RollbackTarget::Eligible(RollbackChoice {
            from_revision: current,
            to_revision,
            skipped: skipped_between(history, to_revision, current),
            forced: false,
        })),
        None => Ok(RollbackTarget::NoEligible {
            current,
            skipped: skipped_between(history, 0, current),
        }),
    }
}

/// Validate an operator-named `--revision`. A revision that is not in the
/// history is always refused (helm would otherwise fail obscurely mid-rollback);
/// an ineligible status is refused unless `allow_failed` was passed, and the
/// refusal names both the status and the override so the operator does not have
/// to guess (AC3).
pub fn resolve_explicit_revision(
    history: &[HelmRevision],
    revision: u32,
    allow_failed: bool,
) -> Result<RollbackChoice> {
    let current = require_current_revision(history)?;

    let row = history.iter().find(|r| r.revision == revision).ok_or_else(|| {
        let known: Vec<String> = history.iter().map(|r| r.revision.to_string()).collect();
        crate::exit::CliError::usage(format!(
            "revision {revision} is not in this release's Helm history (it has {})",
            known.join(", ")
        ))
        .with_fix("run `curie cluster rollback` with no --revision to let it pick the newest safe revision")
    })?;

    let eligible = is_eligible_rollback_status(&row.status);
    if !eligible && !allow_failed {
        return Err(crate::exit::CliError::usage(format!(
            "revision {revision} has status `{}`, not `deployed` or `superseded`; \
             helm never finished applying it, so rolling back to it re-applies a manifest \
             that was never known good; pass --allow-failed-revision to roll back to it anyway, \
             or omit --revision to let Curie pick the newest safe one",
            row.status
        ))
        .with_fix("pass --allow-failed-revision to roll back to it anyway, or omit --revision to let Curie pick the newest safe one")
        .into());
    }

    Ok(RollbackChoice {
        from_revision: current,
        to_revision: revision,
        skipped: skipped_between(history, revision, current),
        forced: !eligible,
    })
}

/// Parse `helm history -o json`: an array of objects. Unknown fields are
/// tolerated (helm adds them across versions) and `revision` is accepted as a
/// JSON number or a string, since that shape has moved between helm releases.
/// Never panics on operator-visible input -- a malformed payload becomes a
/// `CliError` naming the command to run by hand.
pub fn parse_helm_history(json: &str) -> Result<Vec<HelmRevision>> {
    let malformed = |detail: String| {
        crate::exit::CliError::failure(format!(
            "could not read `helm history -o json` output: {detail}"
        ))
        .with_fix(
            "run `helm history <release> -n <namespace> -o json` by hand to see what helm returned",
        )
    };

    let rows: serde_json::Value =
        serde_json::from_str(json).map_err(|e| malformed(e.to_string()))?;
    let rows = rows
        .as_array()
        .ok_or_else(|| malformed("expected a JSON array of revisions".to_string()))?;

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let revision = row
            .get("revision")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
            })
            .and_then(|n| u32::try_from(n).ok())
            .ok_or_else(|| {
                malformed(format!("a revision has no usable `revision` field: {row}"))
            })?;
        let field = |name: &str| {
            row.get(name)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string()
        };
        // A row with no `status` is treated as `unknown`, which is ineligible:
        // the safe reading of a field we could not see is "not known good".
        let status = match field("status").as_str() {
            "" => "unknown".to_string(),
            s => s.to_string(),
        };
        out.push(HelmRevision {
            revision,
            status,
            chart: field("chart"),
            description: field("description"),
        });
    }
    // The invariant every consumer below relies on: ascending by revision, and
    // exactly ONE row per revision number. Eligibility is a property of the
    // REVISION, not of a row, so both the auto-selector (which filters rows by
    // status) and `--revision` (which looks a row up) must read the same answer
    // for the same number. A truncated or corrupt helm secret can yield two
    // rows for one revision that disagree -- say 20 `superseded` and 20
    // `failed` -- and before collapsing them the two paths contradicted each
    // other: `--revision 20` refused it as failed while a bare rollback happily
    // picked it. A revision whose own history contradicts itself was never
    // known good, so it collapses to its ineligible reading: sort ineligible
    // first within a revision number (`false` orders before `true`), then keep
    // the first row of each run.
    out.sort_by_key(|r| (r.revision, is_eligible_rollback_status(&r.status)));
    out.dedup_by_key(|r| r.revision);
    Ok(out)
}

/// `helm history` for the release, as JSON. `--max 256` because helm's default
/// of 10 silently truncates the history, and a truncated history would make the
/// selector pick a revision that merely looks like the newest safe one.
pub fn helm_history_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("history"),
            plain(&o.release),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
            plain("--max"),
            plain("256"),
        ],
    )
}

/// `helm rollback` to an explicit revision. Always explicit: the whole point of
/// the verb is that helm's own default target (the immediately preceding
/// revision) is the wrong one on a failed/superseded history.
pub fn helm_rollback_cmd(o: &CommonOpts, revision: u32) -> OpsCommand {
    helm_rollback_cmd_to(o, revision.to_string())
}

/// The revision slot of the rollback argv, as printed by `--dry-run` before the
/// history that decides it has been read.
const SELECTED_REVISION: &str = "<selected-revision>";

/// [`helm_rollback_cmd`] with the revision slot left as text, so the plan a dry
/// run prints and the command a live run executes are built by one function
/// rather than a builder and a `format!` that drift apart.
fn helm_rollback_cmd_to(o: &CommonOpts, revision: String) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("rollback"),
            plain(&o.release),
            plain(revision),
            plain("-n"),
            plain(&o.namespace),
        ],
    )
}

/// The commands `curie cluster rollback` runs (and prints under `--dry-run`):
/// the history read that decides the target, then the rollback itself. A `None`
/// revision is one the caller has not resolved yet, and stands in the argv as
/// [`SELECTED_REVISION`].
pub fn rollback_commands(o: &CommonOpts, revision: Option<u32>) -> Vec<OpsCommand> {
    let target = revision.map_or_else(|| SELECTED_REVISION.to_string(), |r| r.to_string());
    vec![helm_history_cmd(o), helm_rollback_cmd_to(o, target)]
}

/// One printed plan line. `display()` everywhere, except that it shell-quotes
/// [`SELECTED_REVISION`] (the angle brackets are not shell-safe) into something
/// that reads like a literal argument. The placeholder is a prompt to the
/// reader, not a value, so it is unquoted back out; everything else in the line
/// keeps `display()`'s quoting and masking.
fn plan_line(cmd: &OpsCommand) -> String {
    cmd.display()
        .replace(&shell_quote(SELECTED_REVISION), SELECTED_REVISION)
}

/// Output of `cluster rollback`: the dry-run plan, an operator abort, or the
/// completed rollback. `skipped` is carried into `--json` so an agent sees the
/// revisions bare `helm rollback` would have landed on.
#[derive(Debug)]
pub enum ClusterRollbackOutput {
    DryRun(crate::ui::DryRunPlan),
    Aborted,
    RolledBack {
        from_revision: u32,
        to_revision: u32,
        skipped: Vec<u32>,
        forced: bool,
    },
}

impl crate::ui::CliOutput for ClusterRollbackOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ClusterRollbackOutput::DryRun(plan) => plan.to_json(),
            ClusterRollbackOutput::Aborted => {
                serde_json::json!({"rolled_back": false, "aborted": true})
            }
            ClusterRollbackOutput::RolledBack {
                from_revision,
                to_revision,
                skipped,
                forced,
            } => serde_json::json!({
                "rolled_back": true,
                "from_revision": from_revision,
                "to_revision": to_revision,
                "skipped": skipped,
                "forced": forced,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ClusterRollbackOutput::DryRun(plan) => plan.render(ui),
            ClusterRollbackOutput::Aborted => ui.note("aborted"),
            ClusterRollbackOutput::RolledBack {
                from_revision,
                to_revision,
                skipped,
                forced,
            } => {
                ui.payload(&format!(
                    "curie rolled back from revision {from_revision} to revision {to_revision}"
                ));
                if let Some(note) = skipped_note(skipped, *from_revision) {
                    ui.note(&note);
                }
                if *forced {
                    ui.warn(&format!(
                        "revision {to_revision} was admitted by --allow-failed-revision; helm never finished applying it, so verify the release with `curie cluster status`"
                    ));
                }
            }
        }
    }
}

/// The AC2 line: which revisions were passed over, and -- only when it is
/// actually so -- which of them a bare `helm rollback` would have targeted.
///
/// helm's own default target is always the revision immediately below `from`,
/// so that half of the sentence is true only when the highest skipped revision
/// IS `from - 1`. On an explicit `--revision` the gap can contain eligible
/// revisions that are not in this list, and naming the highest ineligible one
/// as helm's target would be a fabrication. `None` when nothing was skipped, so
/// the common case stays quiet.
fn skipped_note(skipped: &[u32], from: u32) -> Option<String> {
    let highest = *skipped.iter().max()?;
    let list = skipped
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    Some(if from.checked_sub(1) == Some(highest) {
        format!(
            "skipped revision(s) {list} (not deployed/superseded); a bare `helm rollback` would have targeted {highest}"
        )
    } else {
        format!("skipped revision(s) {list} (not deployed/superseded)")
    })
}

pub async fn rollback(opts: RollbackOpts) -> Result<ClusterRollbackOutput> {
    let ui = crate::ui::ui();
    let history_cmd = helm_history_cmd(&opts.common);

    if opts.common.dry_run {
        // The target revision is a function of the live history, so a dry run
        // that has not read it can only name the revision when the operator did.
        return Ok(ClusterRollbackOutput::DryRun(crate::ui::DryRunPlan {
            lines: rollback_commands(&opts.common, opts.revision)
                .iter()
                .map(plan_line)
                .collect(),
        }));
    }
    require_on_path("helm")?;

    ui.plumbing(&format!("+ {}", history_cmd.display()));
    let (ok, history_out, history_err) = run_capture(&history_cmd).await?;
    if !ok {
        // A missing release fails HERE, with helm's own words, rather than
        // downstream as a misleading "no eligible revision" (AC6).
        let detail = history_err
            .trim()
            .lines()
            .next()
            .unwrap_or("helm exited nonzero with no message");
        return Err(crate::exit::CliError::failure(format!(
            "could not read the Helm history of release '{}' in namespace '{}': {detail}",
            opts.common.release, opts.common.namespace
        ))
        .with_fix(format!(
            "confirm the release exists with `helm list -n {}`",
            opts.common.namespace
        ))
        .into());
    }
    let history = parse_helm_history(&history_out)?;

    let choice = match opts.revision {
        Some(revision) => {
            resolve_explicit_revision(&history, revision, opts.allow_failed_revision)?
        }
        None => select_rollback_revision(&history)?.require_eligible()?,
    };

    // Disclosed BEFORE the prompt, and on stderr like `cluster down` does it:
    // the operator confirms knowing both the target and what was passed over,
    // which is the difference this verb exists to make. It cannot go through
    // `payload` -- ADR-0021 (#474) reserves that channel for `CliOutput::render`,
    // and routing it here told an operator who then declined the prompt that the
    // release was rolling back, on stdout, while printing it twice on success.
    ui.note(&format!(
        "rolling release '{}' back from revision {} to revision {}",
        opts.common.release, choice.from_revision, choice.to_revision
    ));
    if let Some(note) = skipped_note(&choice.skipped, choice.from_revision) {
        ui.warn(&note);
    }

    if !opts.yes
        && !confirm(&format!(
            "This rolls back release '{}' in namespace '{}' from revision {} to revision {}. Continue? [y/N] ",
            opts.common.release, opts.common.namespace, choice.from_revision, choice.to_revision
        ))?
    {
        return Ok(ClusterRollbackOutput::Aborted);
    }

    let cl = ui.checklist();
    let rollback_cmd = helm_rollback_cmd(&opts.common, choice.to_revision);
    ui.plumbing(&format!("+ {}", rollback_cmd.display()));
    let step = cl.step("rolling back release");
    let (ok, out, err) = run_capture(&rollback_cmd).await?;
    for line in out.lines().chain(err.lines()) {
        ui.plumbing(line);
    }
    if !ok {
        step.fail("failed");
        let detail = err
            .trim()
            .lines()
            .next()
            .unwrap_or("helm exited nonzero with no message");
        return Err(crate::exit::CliError::failure(format!(
            "helm could not roll release '{}' back to revision {}: {detail}",
            opts.common.release, choice.to_revision
        ))
        .with_fix(format!(
            "inspect the release with `curie cluster status --release {} --namespace {}`",
            opts.common.release, opts.common.namespace
        ))
        .into());
    }
    step.done("rolled back");

    Ok(ClusterRollbackOutput::RolledBack {
        from_revision: choice.from_revision,
        to_revision: choice.to_revision,
        skipped: choice.skipped,
        forced: choice.forced,
    })
}

/// Read a y/N confirmation from stderr/stdin for `down` when `--yes` is absent.
/// Prompt on stderr for a y/N confirmation of a destructive action, returning
/// whether the operator affirmed. The one canonical implementation (#497): every
/// destructive verb passes its own fully-formed prompt (ending in `[y/N] `). A
/// non-interactive session (piped/agent stdin) can never answer, so it refuses
/// with the `--yes` remediation instead of blocking on a read that never returns.
pub(crate) fn confirm(prompt: &str) -> Result<bool> {
    use std::io::{IsTerminal, Write};
    if !std::io::stdin().is_terminal() {
        return Err(crate::exit::CliError::usage(
            "refusing to prompt for confirmation in a non-interactive session; re-run with --yes to proceed",
        )
        .with_fix("pass --yes")
        .into());
    }
    eprint!("{prompt}");
    std::io::stderr().flush().ok();
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .context("reading confirmation from stdin")?;
    Ok(matches!(line.trim(), "y" | "Y" | "yes" | "Yes"))
}

/// One pod's row in the `cluster status` table.
#[derive(Debug)]
pub struct PodRow {
    pub name: String,
    pub ready: String,
    pub status: String,
}

impl PodRow {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"pod": self.name, "ready": self.ready, "status": self.status})
    }
}

/// Render the collected pod rows as a borderless table to stdout (human path).
fn render_pod_table(ui: &crate::ui::Ui, pods: &[PodRow]) {
    if pods.is_empty() {
        return;
    }
    let rows: Vec<Vec<String>> = pods
        .iter()
        .map(|p| vec![p.name.clone(), p.ready.clone(), p.status.clone()])
        .collect();
    ui.payload_plain(&crate::ui::table(&["pod", "ready", "status"], &rows, &[]));
}

/// Summarise `kubectl get pods` output into (rows, ready count, steady-state
/// total, names of pods not Running) WITHOUT printing, so the caller can render
/// or serialize it (#485). Terminal and terminating pods stay in the rows but are
/// excluded from the tally.
fn collect_pod_summary(pods: &[serde_json::Value]) -> (Vec<PodRow>, usize, usize, Vec<String>) {
    let mut ready = 0usize;
    let mut total = 0usize;
    let mut unhealthy: Vec<String> = Vec::new();
    let mut table_rows: Vec<PodRow> = Vec::new();
    for pod in pods {
        let name = pod
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .unwrap_or("?")
            .to_string();
        let terminating = pod
            .get("metadata")
            .and_then(|m| m.get("deletionTimestamp"))
            .is_some();
        let phase = pod
            .get("status")
            .and_then(|s| s.get("phase"))
            .and_then(|p| p.as_str())
            .unwrap_or("");
        let reason = pod
            .get("status")
            .and_then(|s| s.get("reason"))
            .and_then(|r| r.as_str())
            .unwrap_or("");
        let containers = pod
            .get("status")
            .and_then(|s| s.get("containerStatuses"))
            .and_then(|c| c.as_array());
        let (ready_n, total_m) = match containers {
            Some(cs) => {
                let m = cs.len();
                let n = cs
                    .iter()
                    .filter(|c| c.get("ready").and_then(|r| r.as_bool()) == Some(true))
                    .count();
                (n, m)
            }
            None => (0, 0),
        };
        let ready_col = format!("{ready_n}/{total_m}");
        let display_status = if terminating {
            "Terminating"
        } else if !reason.is_empty() {
            convergence::reason(reason)
        } else {
            phase
        };
        table_rows.push(PodRow {
            name: name.clone(),
            ready: ready_col,
            status: display_status.to_string(),
        });
        if phase == "Succeeded" || reason == "Completed" || terminating {
            continue;
        }
        total += 1;
        let all_ready = total_m > 0 && ready_n == total_m;
        if all_ready {
            ready += 1;
        }
        if phase != "Running" {
            unhealthy.push(name);
        }
    }
    (table_rows, ready, total, unhealthy)
}

/// Resolve a routable node host: kubeconfig cluster server hostname, falling
/// back to the first node's InternalIP. None when neither is available.
async fn resolve_node_host() -> Option<String> {
    if let Ok((true, out, _)) = run_capture(&kubeconfig_host_cmd()).await {
        if let Some(host) = host_from_server_url(out.trim()) {
            return Some(host);
        }
    }
    if let Ok((true, out, _)) = run_capture(&nodes_cmd()).await {
        if let Some(ip) = node_internal_ip(&out) {
            return Some(ip);
        }
    }
    None
}

/// Resolve the node host: the kubeconfig cluster server hostname, falling back
/// to the first node's InternalIP, then to the literal `localhost`.
async fn discover_host() -> String {
    resolve_node_host()
        .await
        .unwrap_or_else(|| "localhost".to_string())
}

/// Format an `http://host:port<path>` URL for a node, bracketing an IPv6 host
/// literal so the authority is valid (`::1` -> `[::1]`). `host` is expected
/// unbracketed (as `resolve_node_host` returns it); `path` is appended verbatim
/// (`/api`, `/?api=1`, or `""`).
fn node_http_url(host: &str, port: u16, path: &str) -> String {
    if host.contains(':') {
        format!("http://[{host}]:{port}{path}")
    } else {
        format!("http://{host}:{port}{path}")
    }
}

/// A usage error (exit 2) whose fix hint points the operator at `--api-url`,
/// the escape hatch for every UI-proxy discovery failure.
fn api_url_usage_err(msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix("pass --api-url")
        .into()
}

/// A usage error (exit 2) whose fix hint points the operator at `--api-key`,
/// the escape hatch when the release's API key cannot be read from its Secret.
fn api_key_usage_err(msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix("pass --api-key")
        .into()
}

/// A release-state failure while discovering an API key. The release cannot be
/// authenticated until its state is known, so do not suggest an API key as a
/// remedy for an unreadable Helm inspection.
fn api_key_state_err(namespace: &str, release: &str, msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix(format!(
            "run `curie cluster status --namespace {namespace} --release {release}` and retry"
        ))
        .into()
}

fn helm_release_entries(output: &str) -> Result<Vec<serde_json::Value>> {
    let releases: serde_json::Value =
        serde_json::from_str(output.trim()).context("malformed Helm release list JSON")?;
    releases
        .as_array()
        .cloned()
        .context("malformed Helm release list JSON: expected an array")
}

/// Find the requested release's Helm status in a `helm list -o json` result.
/// Missing releases are a valid result; malformed Helm output is not.
fn helm_release_status(output: &str, release: &str) -> Result<Option<String>> {
    let releases = helm_release_entries(output)?;
    let Some(found) = releases
        .iter()
        .find(|entry| entry.get("name").and_then(|name| name.as_str()) == Some(release))
    else {
        return Ok(None);
    };
    found
        .get("status")
        .and_then(|status| status.as_str())
        .map(|status| Some(status.to_string()))
        .context("malformed Helm release list JSON: release has no status")
}

/// Find a deployed release with the requested name in another namespace.
/// The all-namespace Helm listing makes a same-name alternate explicit rather
/// than guessing that a credential override can repair a failed release.
fn deployed_release_namespace(
    output: &str,
    release: &str,
    requested_namespace: &str,
) -> Result<Option<String>> {
    let releases = helm_release_entries(output)?;

    for entry in releases {
        if entry.get("name").and_then(|name| name.as_str()) != Some(release)
            || !entry
                .get("status")
                .and_then(|status| status.as_str())
                .is_some_and(|status| status.eq_ignore_ascii_case("deployed"))
        {
            continue;
        }
        let namespace = entry
            .get("namespace")
            .and_then(|namespace| namespace.as_str())
            .context("malformed Helm release list JSON: deployed release has no namespace")?;
        if namespace != requested_namespace {
            return Ok(Some(namespace.to_string()));
        }
    }
    Ok(None)
}

/// Discover a Helm release's platform API key by reading it out of the chart
/// Secret (data key `apiKey`), whose name is discovered by label selector
/// rather than computed -- see [`release_secret_name`] -- decoded server-side
/// by kubectl's `base64decode` so the plaintext never lands in argv (#524). The
/// governance verbs use this so they authenticate against a REAL release whose
/// `api.apiKey` was randomized at `cluster up`, instead of silently sending the
/// dev sentinel `curie-dev-key` and 401-ing. An explicit `--api-key`/env still
/// wins (the caller only reaches here when neither was supplied). The value is
/// never printed — it flows straight into the `X-API-Key` header.
pub async fn discover_api_key(namespace: &str, release: &str) -> Result<String> {
    if let Some(api_key) = read_release_secret(namespace, release, "apiKey").await {
        return Ok(api_key);
    }

    let requested_cmd = OpsCommand::new(
        "helm",
        vec![
            plain("list"),
            plain("-n"),
            plain(namespace),
            plain("--all"),
            plain("-o"),
            plain("json"),
        ],
    );
    let (requested_ok, requested_output, requested_error) = run_capture(&requested_cmd)
        .await
        .map_err(|error| {
            api_key_state_err(
                namespace,
                release,
                format!("could not inspect Helm state for release {release} in namespace {namespace}: {error}"),
            )
        })?;
    if !requested_ok {
        return Err(api_key_state_err(
            namespace,
            release,
            format!(
                "could not inspect Helm state for release {release} in namespace {namespace}: {}",
                failure_reason(&requested_error)
            ),
        ));
    }
    let requested_status = helm_release_status(&requested_output, release).map_err(|error| {
        api_key_state_err(
            namespace,
            release,
            format!("could not inspect Helm state for release {release} in namespace {namespace}: {error}"),
        )
    })?;

    if requested_status
        .as_deref()
        .is_some_and(|status| status.eq_ignore_ascii_case("deployed"))
    {
        return Err(api_key_usage_err(format!(
            "release {release} in namespace {namespace} is deployed, but its chart Secret API key could not be read; \
             pass --api-key or set CURIE_API_KEY to the release's api.apiKey"
        )));
    }

    let all_cmd = OpsCommand::new(
        "helm",
        vec![
            plain("list"),
            plain("-A"),
            plain("--all"),
            plain("-o"),
            plain("json"),
        ],
    );
    let deployed_alternate = match run_capture(&all_cmd).await {
        Ok((true, all_output, _)) => {
            deployed_release_namespace(&all_output, release, namespace).unwrap_or(None)
        }
        _ => None,
    };

    let status_command =
        format!("`curie cluster status --namespace {namespace} --release {release}`");
    let (message, fix) = match (requested_status, deployed_alternate) {
        (Some(status), Some(alternate_namespace)) => (
            format!(
                "release {release} in namespace {namespace} is {status}, not deployed; a deployed release named {release} is available in namespace {alternate_namespace}. Inspect the failed release with {status_command} or retry this command with `--namespace {alternate_namespace}`"
            ),
            format!("retry this command with `--namespace {alternate_namespace}`"),
        ),
        (Some(status), None) => (
            format!(
                "release {release} in namespace {namespace} is {status}, not deployed; inspect its state with {status_command}"
            ),
            format!("run {status_command} and repair or redeploy the release"),
        ),
        (None, Some(alternate_namespace)) => (
            format!(
                "no release named {release} was found in namespace {namespace}; a deployed release with that name is available in namespace {alternate_namespace}. Retry this command with `--namespace {alternate_namespace}`"
            ),
            format!("retry this command with `--namespace {alternate_namespace}`"),
        ),
        (None, None) => (
            format!(
                "no deployed release named {release} was found in namespace {namespace}; inspect the target with {status_command}"
            ),
            format!("run {status_command} and deploy the release before retrying"),
        ),
    };
    Err(crate::exit::CliError::usage(message).with_fix(fix).into())
}

/// A usage error (exit 2) whose fix hint points the operator at
/// `--valkey-password`, the escape hatch when the release's Valkey password
/// cannot be read from its Secret.
fn valkey_password_usage_err(msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix("pass --valkey-password")
        .into()
}

/// Discover a Helm release's Valkey password from the same chart Secret
/// (name discovered by label selector -- see [`release_secret_name`] -- data
/// key `valkeyPassword`). `cluster message` enqueues
/// onto the release's Valkey, whose password `cluster up` randomizes, so without
/// this the dev sentinel `valkeypass` reaches a strong-secrets install and the
/// connection fails authentication (#786). An explicit
/// `--valkey-password`/`CURIE_VALKEY_PASSWORD` still wins (the caller only
/// reaches here when neither was supplied); the value is never printed.
pub async fn discover_valkey_password(namespace: &str, release: &str) -> Result<String> {
    read_release_secret(namespace, release, "valkeyPassword")
        .await
        .ok_or_else(|| {
            valkey_password_usage_err(format!(
                "could not read the Valkey password from the chart Secret for release {release} in namespace \
                 {namespace}; pass --valkey-password or set CURIE_VALKEY_PASSWORD to the \
                 release's valkey.password"
            ))
        })
}

/// A usage error whose fix hint points at the Slack bot-token escape hatch.
fn slack_bot_token_usage_err(msg: impl Into<String>) -> anyhow::Error {
    crate::exit::CliError::usage(msg)
        .with_fix("set CURIE_SLACK_BOT_TOKEN, or connect the workspace with `curie cluster comms --slack`")
        .into()
}

/// Discover a Helm release's Slack bot token from the chart Secret
/// (name discovered by label selector -- see [`release_secret_name`] -- data
/// key `slackBotToken`), or from the operator's own
/// `dispatcher.slack.botTokenExistingSecret` when one is configured (#1759).
/// In connected mode `cluster message` posts a real placeholder to the
/// workspace with this token so the approval card and resumed reply ride the
/// connected transport, instead of the throwaway stub (#770/ADR-0078). Only
/// reached when the release's dispatcher Deployment is present (a workspace IS
/// connected), so the token is expected to be set; an empty or unreadable
/// value is an actionable error. The value is never printed -- it flows only
/// into the `chat.postMessage` auth header.
pub async fn discover_slack_bot_token(namespace: &str, release: &str) -> Result<String> {
    read_direct_passthrough_secret(
        namespace,
        release,
        "slackBotToken",
        "dispatcher.slack.botTokenExistingSecret",
        "dispatcher.slack.botTokenExistingSecretKey",
    )
    .await
    .filter(|token| !token.is_empty())
    .ok_or_else(|| {
            slack_bot_token_usage_err(format!(
                "could not read a Slack bot token from the chart Secret for release {release} in namespace \
                 {namespace}; the workspace may not be connected (run `curie cluster comms \
                 --slack`), or set CURIE_SLACK_BOT_TOKEN"
            ))
        })
}

/// What a Slack API base lookup found for a release.
///
/// The three outcomes are deliberately distinct because they demand different
/// behavior, and collapsing them is how #1030 could come back wearing a different
/// hat. "Configured nothing" is the ordinary case and means real Slack. "Could not
/// look" is not evidence of anything and must not be read as the ordinary case,
/// because the CLI would then post a real token wherever real Slack is while the
/// worker edits through a proxy the CLI never saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlackApiBase {
    /// The worker renders `SLACK_API_BASE_URL` with this value.
    Configured(String),
    /// The worker renders no `SLACK_API_BASE_URL`, so it talks to real Slack.
    RealSlack,
    /// The lookup itself could not run or could not find the worker.
    Unknown,
}

/// The label selector that finds a release's worker Deployment whatever it is named.
///
/// The name is `{{ include "curie.fullname" . }}-worker`, which equals
/// `<release>-worker` only when the release name happens to contain the chart
/// name. A release named `acme-prod` renders `acme-prod-curie-worker`, and
/// `nameOverride`/`fullnameOverride` move it again. Guessing the name is the
/// defect `release_secret_name` already avoids by selecting on labels, and this
/// selects the same way for the same reason.
fn worker_deployment_selector(release: &str) -> String {
    component_selector(release, "worker")
}

/// The label pair that finds one component of a release whatever it is named.
///
/// `curie.selectorLabels` (`charts/curie/templates/_helpers.tpl:40-44`) emits
/// `app.kubernetes.io/instance: {{ .Release.Name }}` and
/// `app.kubernetes.io/component: {{ .component }}` and reads NEITHER
/// `nameOverride` nor `fullnameOverride`, so these labels are stable on exactly
/// the installs whose rendered NAMES are not. One spelling, because three
/// hand-built copies of the same pair is three places for it to drift from the
/// chart.
///
/// Not the shape [`release_secret_name`] uses: that one deliberately selects on
/// `instance` alone and filters by name suffix, because the chart Secret
/// carries no component label.
fn component_selector(release: &str, component: &str) -> String {
    format!("app.kubernetes.io/instance={release},app.kubernetes.io/component={component}")
}

/// Parse `kubectl get deployment -o jsonpath=...` output into an outcome.
///
/// Pure, so every branch is unit-testable without a cluster. `ok=false` is the
/// case that must not read as "nothing configured": see [`SlackApiBase`].
fn parse_slack_api_base(ok: bool, out: &str) -> SlackApiBase {
    if !ok {
        return SlackApiBase::Unknown;
    }
    let value = out.trim();
    if value.is_empty() {
        return SlackApiBase::RealSlack;
    }
    // The jsonpath ranges over containers, so two containers each rendering the
    // var would concatenate. That cannot happen in this chart (the worker
    // Deployment has one container), and if it ever does, guessing which half is
    // the base is exactly the ambiguity this issue is about.
    if value.lines().count() > 1 {
        return SlackApiBase::Unknown;
    }
    SlackApiBase::Configured(value.to_string())
}

/// The Slack API base the release's own worker is configured with.
///
/// Read from the SAME release the bot token is read from (#1030). The base used to
/// come from `SLACK_API_BASE_URL` in the CLI's own process environment while the
/// token came from the release Secret, so a developer with a stub URL exported
/// from earlier testing sent a production workspace token to their local stub.
/// The two halves now share one source.
pub async fn discover_slack_api_base_url(namespace: &str, release: &str) -> SlackApiBase {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("deployment"),
            plain("-l"),
            plain(worker_deployment_selector(release)),
            plain("-o"),
            plain(
                "jsonpath={range .items[*].spec.template.spec.containers[*]}                 {.env[?(@.name=='SLACK_API_BASE_URL')].value}{\"\\n\"}{end}",
            ),
        ],
    );
    match run_capture(&cmd).await {
        Ok((ok, out, _)) => parse_slack_api_base(ok, &out),
        Err(_) => SlackApiBase::Unknown,
    }
}

/// Whether the release's dispatcher Deployment exists in `namespace` -- i.e. a
/// real Slack workspace is connected (via `curie cluster comms --slack`). In
/// that case `cluster message` posts a real placeholder and routes the approval
/// card + resumed reply over that connected transport rather than a throwaway
/// stub (#770/ADR-0078). A kubectl failure (cluster unreachable, no such
/// namespace) reads as NOT connected, so the caller safely falls back to the
/// stub path instead of failing the whole command. `--ignore-not-found` makes an
/// absent Deployment an empty success, so "connected" is exactly "non-empty
/// output on a zero exit".
pub async fn dispatcher_connected(namespace: &str, release: &str) -> bool {
    // The chart renders `{{ curie.fullname }}-dispatcher`, so the name must be
    // resolved and not computed from the release. `--ignore-not-found` turns a
    // wrong name into a confident empty success, which reads as "no workspace
    // connected" -- a silent wrong answer rather than a visible failure (#1533).
    let fullname = release_fullname(namespace, release).await;
    let dispatcher = fullname.resource("dispatcher");
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("deployment"),
            plain(&dispatcher),
            plain("--ignore-not-found"),
            plain("-o"),
            plain("name"),
        ],
    );
    match run_capture(&cmd).await {
        Ok((true, out, _)) => !out.trim().is_empty(),
        // A probe that could not run is NOT evidence of "no workspace connected"
        // (#957 mode C). Silently downgrading to the stub path here means the
        // operator asked for the connected mode, got the other one, and was told
        // nothing. Still fall back -- the stub path is the safe direction, and
        // failing the command on a flaky kubectl would be worse -- but say so.
        Ok((false, _, err)) => {
            crate::ui::ui().warn(&format!(
                "could not determine whether a Slack workspace is connected \
                 (kubectl probe for {dispatcher} failed: {}); assuming \
                 NOT connected and using the local reply stub",
                err.trim().lines().next().unwrap_or("no stderr")
            ));
            false
        }
        Err(exc) => {
            crate::ui::ui().warn(&format!(
                "could not determine whether a Slack workspace is connected \
                 ({exc}); assuming NOT connected and using the local reply stub"
            ));
            false
        }
    }
}

/// The name of the release's chart Secret, discovered rather than computed.
///
/// It was computed as `<release>-secrets`, which is only right when the release
/// name happens to contain the chart name. The chart uses helm's standard
/// `fullname`, so a default install renders `<release>-curie-secrets` and every
/// read silently found nothing. It went unnoticed because the installs that
/// exercise these paths set `nameOverride` to the release name, which collapses
/// the two forms.
///
/// Discovered because it cannot be computed from what the CLI knows: both
/// `nameOverride` and `fullnameOverride` change the answer, and neither is
/// visible from the release name alone. Selecting on the instance label works
/// whatever the operator set.
///
/// The `-connector-secrets` exclusion is load-bearing: per-agent connector
/// Secrets carry the same release labels, and one of those would be a
/// confidently wrong answer rather than an empty one.
async fn release_secret_name(namespace: &str, release: &str) -> Option<String> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("secret"),
            plain("-l"),
            plain(format!("app.kubernetes.io/instance={release}")),
            plain("-o"),
            plain("jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"),
        ],
    );
    let (ok, out, _err) = run_capture(&cmd).await.ok()?;
    if !ok {
        return None;
    }
    pick_release_secret(&out)
}

/// The chart Secret among a release's Secrets, or `None` if it is not there.
///
/// Pure, so the selection rule is testable without a cluster -- which is how
/// the `-connector-secrets` collision was caught before it could pick one.
pub fn pick_release_secret(names: &str) -> Option<String> {
    names
        .lines()
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .find(|n| n.ends_with("-secrets") && !n.ends_with("-connector-secrets"))
        .map(str::to_string)
}

/// The chart Secret's name, falling back to the chart's own naming rule.
///
/// A caller that must NAME the Secret (rather than read through it) still needs
/// a string when the cluster is unreachable -- a `--dry-run` plan, for
/// instance. That fallback used to be `format!("{release}-secrets")`, the raw
/// chart-resource form this sweep exists to delete: for an ordinary `platform`
/// install the chart renders `platform-curie-secrets`, so a transient discovery
/// failure had `migrate-store` stage a pod against a Secret that does not
/// exist. It now goes through [`chart_fullname`], which is the chart's
/// no-override rule and byte-identical for the default `curie` release.
///
/// Only the FALLBACK changed. Live discovery still selects on the instance
/// label alone and filters by suffix afterwards ([`pick_release_secret`]),
/// which is what makes it correct under `nameOverride`/`fullnameOverride`.
pub async fn release_secret_name_or_default(namespace: &str, release: &str) -> String {
    release_secret_name(namespace, release)
        .await
        .unwrap_or_else(|| chart_fullname(release).resource("secrets"))
}

/// A release's rendered `curie.fullname` -- the prefix every Curie-owned object
/// in the chart is named from.
///
/// A newtype rather than a bare `String` on purpose. Every affected call site
/// used to build `format!("{release}-{component}")` from a raw release name, so
/// a `&str` parameter merely RENAMED to `fullname` would still compile when a
/// caller passed the release name -- the original defect, type-checked as
/// correct. This value is constructible only by [`chart_fullname`] and
/// [`release_fullname`], so a raw release name cannot reach a resource name at
/// all, and the compiler finds every site that has not been routed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseFullname(String);

impl ReleaseFullname {
    /// The chart's name for one of the release's components:
    /// `{{ include "curie.fullname" . }}-<component>`
    /// (`charts/curie/templates/_helpers.tpl:16-26`).
    ///
    /// The suffix is appended AFTER the fullname's own `trunc 63`, exactly as
    /// the chart templates do, so a rendered object name can legitimately
    /// exceed 63 characters. Truncating the joined string instead would name an
    /// object helm never created; `truncation_happens_before_the_component_suffix`
    /// pins the ordering.
    pub fn resource(&self, component: &str) -> String {
        format!("{}-{component}", self.0)
    }

    /// The fullname itself, for a caller that needs the prefix rather than a
    /// component's name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The chart's `curie.fullname` computed from the release name alone.
///
/// **Offline fallback only.** This is the chart's NO-OVERRIDE path
/// (`charts/curie/templates/_helpers.tpl:16-26`):
///
/// ```text
/// fullnameOverride                 if set
/// .Release.Name                    if it contains the chart name
/// "{.Release.Name}-curie"          otherwise
///                                  ... then | trunc 63 | trimSuffix "-"
/// ```
///
/// It cannot see `nameOverride` or `fullnameOverride`, and both move the
/// rendered name somewhere this rule computes WRONGLY: `helm template platform
/// charts/curie --set fullnameOverride=platform` renders `platform-api` while
/// this rule says `platform-curie-api`. So a live path calls
/// [`release_fullname`], which asks the cluster and only falls back here when
/// the cluster cannot answer (`--dry-run`, no kubectl, no release yet).
/// `cli/tests/chart_fullname_parity.rs` pins both the rule and its limit
/// against the chart's own render.
pub fn chart_fullname(release: &str) -> ReleaseFullname {
    const CHART_NAME: &str = "curie";

    let fullname = if release.contains(CHART_NAME) {
        release.to_string()
    } else {
        format!("{release}-{CHART_NAME}")
    };
    // `trunc 63` first, then `trimSuffix "-"`, with sprig's exact semantics:
    // `trimSuffix` removes EXACTLY ONE trailing dash where
    // `str::trim_end_matches('-')` removes all of them. Confirmed against the
    // chart rather than against a reading of sprig -- `--set
    // fullnameOverride=<61 a's>--<10 z's>` renders the api Service as
    // `<61 a's>--api`, so one dash survives for the component suffix to join to.
    let truncated: String = fullname.chars().take(63).collect();
    let trimmed = truncated.strip_suffix('-').unwrap_or(truncated.as_str());
    ReleaseFullname(trimmed.to_string())
}

/// The fullname a `--dry-run` plan prints, plus the caveat that goes with it.
///
/// A dry run makes no cluster call, so the release's rendered name cannot be
/// discovered and [`chart_fullname`]'s no-override rule is the honest best
/// guess. Every dry-run branch owes the reader the same caveat, so the note
/// lives with the value rather than being restated at each verb.
fn dry_run_fullname(release: &str) -> ReleaseFullname {
    let fullname = chart_fullname(release);
    crate::ui::ui().note(&format!(
        "dry run: service names assume the chart's default naming ({}); an install \
         using nameOverride/fullnameOverride renders them differently",
        fullname.resource("<component>")
    ));
    fullname
}

/// The fullname implied by a discovered object name, or `None` when that name
/// does not carry the component suffix we selected on.
///
/// Pure, extracted for the same reason [`pick_release_secret`] is: the part
/// that can be wrong is the selection rule, and it has to be testable with no
/// cluster. Rejecting rather than blind-stripping is load-bearing -- a name
/// that does not end in `-<component>` is not ours to truncate, and a
/// confidently wrong fullname is worse than falling through to the chart rule.
pub fn fullname_from_resource_name(name: &str, component: &str) -> Option<String> {
    let fullname = name.trim().strip_suffix(&format!("-{component}"))?;
    if fullname.is_empty() {
        return None;
    }
    Some(fullname.to_string())
}

/// The outcome of one discovery probe, kept as four distinct cases on purpose.
///
/// The probe used to collapse "this release is not installed", "kubectl is not
/// on PATH", "RBAC denied the read" and "two objects matched" into a single
/// `None`, and the caller then silently computed a name. The distinction that
/// matters: falling back for a genuinely ABSENT or not-yet-installed release is
/// defensible -- `doctor` and a fresh namespace must still work -- while
/// falling back because the probe FAILED is a guess dressed as an answer, and
/// doing it silently is the defect. Each case is its own variant so
/// [`release_fullname`] can say which one happened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComponentDiscovery {
    /// Exactly one labelled object matched and its name carries the component
    /// suffix: the release's rendered fullname, read off the cluster.
    Found(ReleaseFullname),
    /// The probe ran and answered: this release has no such object.
    NotPresent,
    /// The probe did not answer -- kubectl missing, unreachable API server,
    /// RBAC denial, malformed request. Carries the first line of stderr.
    ProbeFailed { component: String, detail: String },
    /// More than one object carried the release's labels for this component.
    /// Kubernetes does not enforce label uniqueness, so this is reachable with
    /// a hand-applied or copy-pasted object, and taking `items[0]` would
    /// silently point every downstream verb at a workload that is not ours.
    Ambiguous {
        component: String,
        names: Vec<String>,
    },
}

impl ComponentDiscovery {
    /// The warning an operator must see before this outcome degrades into a
    /// COMPUTED name, or `None` when degrading is the honest, normal thing.
    ///
    /// Pure, so the wording -- the part that has to be actionable -- is
    /// testable with no cluster.
    pub fn fallback_warning(
        &self,
        namespace: &str,
        release: &str,
        fallback: &ReleaseFullname,
    ) -> Option<String> {
        let guess = format!(
            "the name being used, `{}`, is a COMPUTED GUESS from the chart's default \
             naming and is WRONG on an install using nameOverride/fullnameOverride",
            fallback.resource("<component>")
        );
        match self {
            // A real answer needs no caveat, and an absent release is the
            // documented reason this path has a fallback at all.
            Self::Found(_) | Self::NotPresent => None,
            Self::ProbeFailed { component, detail } => Some(format!(
                "could not discover release `{release}` resource names in namespace \
                 `{namespace}`: the kubectl probe for the `{component}` object FAILED \
                 ({detail}). Continuing, but {guess}. Check kubectl access and RBAC \
                 (get/list on services and deployments in `{namespace}`)."
            )),
            Self::Ambiguous { component, names } => Some(format!(
                "refusing to choose: {} objects in namespace `{namespace}` match \
                 `{}` ({}). Curie cannot tell which one belongs to release `{release}`, \
                 so it targets NONE of them. Continuing, but {guess}. Relabel or remove \
                 the objects that are not part of release `{release}`.",
                names.len(),
                component_selector(release, component),
                names.join(", ")
            )),
        }
    }
}

/// The outcome implied by the names a probe matched.
///
/// Pure, extracted for the same reason [`fullname_from_resource_name`] is: the
/// cardinality rule is the part that can be wrong, and it has to be testable
/// with no cluster.
///
/// EXACTLY ONE match resolves. Zero is absence. Two or more is
/// [`ComponentDiscovery::Ambiguous`] and never `names[0]`: those labels are not
/// unique by construction, so a stray Service named `unexpected-api` carrying
/// the release's labels would otherwise resolve the fullname to `unexpected`
/// and send `cluster message`/`comms`/`eval` at an unrelated workload.
///
/// Two legitimate releases are NOT this case: the selector pins
/// `app.kubernetes.io/instance=<release>`, so another release's objects never
/// match in the first place.
pub fn component_discovery(names: &str, component: &str) -> ComponentDiscovery {
    let matched: Vec<String> = names
        .lines()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect();

    match matched.len() {
        0 => ComponentDiscovery::NotPresent,
        1 => match fullname_from_resource_name(&matched[0], component) {
            Some(fullname) => ComponentDiscovery::Found(ReleaseFullname(fullname)),
            // Labelled for the component but not NAMED for it: not ours to
            // truncate. Fall through to the next probe rather than mint a
            // confidently wrong fullname.
            None => ComponentDiscovery::NotPresent,
        },
        _ => ComponentDiscovery::Ambiguous {
            component: component.to_string(),
            names: matched,
        },
    }
}

/// One discovery probe: select the release's `<component>` objects by label and
/// read their names back.
///
/// The jsonpath ranges over ALL matches rather than reading `.items[0]`, so
/// cardinality is observable at all -- a single-item jsonpath cannot tell one
/// match from three.
async fn discover_component_fullname(
    namespace: &str,
    release: &str,
    kind: &str,
    component: &str,
) -> ComponentDiscovery {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain(kind),
            plain("-l"),
            plain(component_selector(release, component)),
            plain("-o"),
            plain("jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}"),
        ],
    );
    match run_capture(&cmd).await {
        Ok((true, out, _err)) => component_discovery(&out, component),
        Ok((false, _out, err)) => {
            let detail = err.trim().lines().next().unwrap_or("no stderr");
            ComponentDiscovery::ProbeFailed {
                component: component.to_string(),
                detail: detail.to_string(),
            }
        }
        Err(exc) => ComponentDiscovery::ProbeFailed {
            component: component.to_string(),
            detail: exc.to_string(),
        },
    }
}

/// Which of the two probes' outcomes to report.
///
/// A resolved name wins from either probe. Otherwise a PROBLEM (a failed probe,
/// an ambiguous match) outranks absence, because absence is the one case that
/// degrades silently and it must not mask the other two.
fn preferred_probe_outcome(
    api: ComponentDiscovery,
    worker: ComponentDiscovery,
) -> ComponentDiscovery {
    if matches!(api, ComponentDiscovery::Found(_)) {
        return api;
    }
    if matches!(worker, ComponentDiscovery::Found(_)) {
        return worker;
    }
    if matches!(api, ComponentDiscovery::NotPresent) {
        return worker;
    }
    api
}

/// The release's rendered fullname, read off the cluster.
///
/// Discovered rather than computed for the same reason [`release_secret_name`]
/// is: `nameOverride` and `fullnameOverride` both change the rendered name and
/// neither is visible from the release name alone. It works because
/// `curie.selectorLabels` (`charts/curie/templates/_helpers.tpl:40-44`) emits
/// `app.kubernetes.io/instance: {{ .Release.Name }}` and
/// `app.kubernetes.io/component: {{ .component }}` and reads NEITHER override,
/// so the labels stay stable on exactly the installs whose NAMES do not.
/// Verified against both override renders, and pinned by
/// `overrides_preserve_the_discovery_labels`.
///
/// Two probes, deliberately: the api Service first, then the worker Deployment.
/// `api.deploy=false` is a supported install with no api Service, and the
/// worker Deployment still carries the release labels.
///
/// Neither probe picks among matches. `app.kubernetes.io/instance=<release>`
/// separates two legitimate releases, but nothing in Kubernetes stops a stray
/// object from carrying the same pair of labels, so a set of size two or more
/// is refused rather than resolved -- see [`component_discovery`].
async fn discover_release_fullname(namespace: &str, release: &str) -> ComponentDiscovery {
    let api = discover_component_fullname(namespace, release, "svc", "api").await;
    if matches!(api, ComponentDiscovery::Found(_)) {
        return api;
    }
    let worker = discover_component_fullname(namespace, release, "deployment", "worker").await;
    preferred_probe_outcome(api, worker)
}

/// The per-process memo behind [`release_fullname`]. One
/// [`tokio::sync::OnceCell`] per `(namespace, release)`, handed out under a std
/// mutex that is never held across an await.
type ReleaseFullnameCache = std::sync::Mutex<
    std::collections::HashMap<
        (String, String),
        std::sync::Arc<tokio::sync::OnceCell<ReleaseFullname>>,
    >,
>;

static RELEASE_FULLNAME_CACHE: std::sync::LazyLock<ReleaseFullnameCache> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// The release's fullname: discovered from the cluster, falling back to the
/// chart's no-override rule.
///
/// THE live entry point. Every path that can reach a cluster resolves here, and
/// [`chart_fullname`] is what it degrades to. Discovery finding nothing is
/// normal rather than an error -- `doctor` and a not-yet-installed release must
/// still work -- so this never fails.
///
/// It is not, however, silent about WHY it degraded. A failed probe (RBAC
/// denial, no kubectl, unreachable API server) and an ambiguous match both warn
/// before the computed name is used, because on an install using
/// `nameOverride`/`fullnameOverride` that name is known to be wrong: without
/// the warning `cluster status` reports "not found" for a Service that exists
/// and a self-plumbed deploy fails against a name helm never rendered. Control
/// flow is deliberately unchanged -- the fallback still happens, loudly.
/// Failing mutating verbs closed on a failed probe is the stronger fix and is
/// left as a follow-up policy decision.
///
/// Resolve LAZILY, on the branch that actually needs a cluster-derived name.
/// Resolving at a verb's entry point fires kubectl on the explicit-`--api-url`
/// and `--dry-run` paths, which are contractually cluster-offline
/// (`cli/tests/cluster_connection_transport.rs`). Under `--dry-run`, call
/// [`chart_fullname`] directly and make no cluster call at all.
///
/// MEMOIZED for the lifetime of the process, keyed by `(namespace, release)`.
/// A single verb resolves the same release's fullname from several places --
/// `doctor` asks once through `discover_api_url` and again through
/// `api_nodeport`, and `cluster status` needs it for two Service reads -- and
/// each of those was a separate kubectl round trip for an answer the process
/// already had. The [`tokio::sync::OnceCell`] also dedups CONCURRENT callers,
/// so two probes joined into one stage issue one discovery between them rather
/// than racing to make the same call twice.
///
/// Two consequences, both deliberate:
///
/// - The fallback warning is emitted ONCE per process instead of once per
///   call. It says the rendered name could not be discovered, which is a fact
///   about the run, not about the call site; repeating it per caller was noise.
/// - Every outcome is cached, the [`chart_fullname`] fallback included. That is
///   safe because no verb resolves a fullname both BEFORE and AFTER mutating
///   the cluster within one process: `cluster up` and `cluster down` never call
///   this (they name chart resources through the chart's own templates), so
///   there is no window in which a cached miss could outlive the install that
///   would have turned it into a hit.
pub async fn release_fullname(namespace: &str, release: &str) -> ReleaseFullname {
    // The std mutex is held only long enough to hand back this key's cell --
    // never across the await below, which is what would deadlock the runtime.
    let cell = {
        let mut cache = RELEASE_FULLNAME_CACHE
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cache
            .entry((namespace.to_string(), release.to_string()))
            .or_default()
            .clone()
    };
    cell.get_or_init(|| async {
        match discover_release_fullname(namespace, release).await {
            ComponentDiscovery::Found(fullname) => fullname,
            outcome => {
                let fallback = chart_fullname(release);
                if let Some(warning) = outcome.fallback_warning(namespace, release, &fallback) {
                    crate::ui::ui().warn(&warning);
                }
                fallback
            }
        }
    })
    .await
    .clone()
}

/// The release's sealing keys (ADR-0094): current first, then the previous one
/// if a rotation is in progress. Follows the operator's own
/// `sealing.{privateKey,previousPrivateKey}ExistingSecret` when configured
/// (#1759), otherwise the chart's own Secret.
///
/// Returns whatever is present. An empty vector means this release has no
/// sealing key, which the caller must report rather than work around -- sealing
/// against nothing, or "decrypting" without a key, both produce a connector
/// that starts and then fails every call.
pub async fn read_sealing_keys(namespace: &str, release: &str) -> Vec<String> {
    let mut keys = Vec::new();
    for (default_data_key, existing_secret_key, existing_secret_key_key) in [
        (
            "sealingPrivateKey",
            "sealing.privateKeyExistingSecret",
            "sealing.privateKeyExistingSecretKey",
        ),
        (
            "sealingPreviousPrivateKey",
            "sealing.previousPrivateKeyExistingSecret",
            "sealing.previousPrivateKeyExistingSecretKey",
        ),
    ] {
        if let Some(value) = read_direct_passthrough_secret(
            namespace,
            release,
            default_data_key,
            existing_secret_key,
            existing_secret_key_key,
        )
        .await
        {
            if !value.trim().is_empty() {
                keys.push(value);
            }
        }
    }
    keys
}

/// Read one data key out of a release's chart Secret, decoded server-side by
/// kubectl's `base64decode` so the plaintext never lands in argv (#524). `None`
/// when the Secret, the key, or the cluster is unreachable; the caller turns
/// that into an actionable error naming its own escape-hatch flag.
async fn read_release_secret(namespace: &str, release: &str, data_key: &str) -> Option<String> {
    let secret = release_secret_name(namespace, release).await?;
    read_secret_key(namespace, &secret, data_key).await
}

/// Read one data key out of a NAMED Secret -- not necessarily the release's own
/// chart Secret, since a direct-passthrough credential's `existingSecret` names
/// an operator-managed Secret elsewhere in the namespace. Decoded server-side by
/// kubectl's `base64decode` so the plaintext never lands in argv (#524). `None`
/// when the Secret, the key, or the cluster is unreachable.
async fn read_secret_key(namespace: &str, secret_name: &str, data_key: &str) -> Option<String> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("secret"),
            plain(secret_name),
            plain("-o"),
            plain(format!(
                "go-template={{{{ index .data \"{data_key}\" | base64decode }}}}"
            )),
        ],
    );
    match run_capture(&cmd).await {
        Ok((true, out, _)) if !out.trim().is_empty() => Some(out.trim().to_string()),
        _ => None,
    }
}

/// Read a direct-passthrough credential (issue #1759), following the operator's
/// own `existingSecret` when one is configured for this key and otherwise
/// falling back to the chart's own Secret and its published key name -- the
/// same BYO-wins precedence the chart templates themselves use.
///
/// Without this, a CLI verb that reads the credential straight from the
/// chart's own Secret (as every one of these did before the BYO escape
/// existed) silently reads nothing for anyone who adopts it, even though the
/// deployed workload resolves correctly from the operator's Secret.
async fn read_direct_passthrough_secret(
    namespace: &str,
    release: &str,
    default_data_key: &str,
    existing_secret_key: &str,
    existing_secret_key_key: &str,
) -> Option<String> {
    let common = CommonOpts {
        namespace: namespace.to_string(),
        release: release.to_string(),
        dry_run: false,
    };
    let existing = fetch_existing_values(&common).await.ok().flatten();
    match resolve_existing_secret_ref(
        existing.as_ref(),
        existing_secret_key,
        existing_secret_key_key,
        default_data_key,
    ) {
        Some((secret_name, data_key)) => read_secret_key(namespace, &secret_name, &data_key).await,
        None => read_release_secret(namespace, release, default_data_key).await,
    }
}

/// Build the UI `/api` proxy base URL (`http://<host>:<ui-nodeport>/api`) from
/// the UI service JSON and a resolved node host, or an actionable usage error.
/// `cluster deploy` reaches the platform API through this proxy (the UI pod
/// serves `/api`), so it never falls back to a port-forward.
fn ui_api_url_from_parts(ui_svc_json: &str, host: Option<&str>) -> Result<String> {
    match parse_service(ui_svc_json) {
        Some((svc_type, node_port, _)) if svc_type == "NodePort" => {
            let np = node_port.ok_or_else(|| {
                api_url_usage_err(
                    "the UI service is NodePort but has not been assigned a nodePort yet; wait for the release to settle or pass --api-url to target the API directly",
                )
            })?;
            let host = host.ok_or_else(|| {
                api_url_usage_err(
                    "could not determine a node host to reach the UI /api proxy; pass --api-url to target the API directly",
                )
            })?;
            Ok(node_http_url(host, np, "/api"))
        }
        Some(_) => Err(api_url_usage_err(
            "the UI service is not NodePort-exposed (installed with --no-expose?); re-run `cluster up` without --no-expose or pass --api-url to target the API directly",
        )),
        None => Err(api_url_usage_err(
            "could not read the UI service to discover the platform API URL; pass --api-url to target the API directly",
        )),
    }
}

/// Build a direct platform-API base URL from the api service, used when the UI
/// is not deployed. No `/api` suffix: this is the API itself, not the UI proxy.
fn api_url_from_parts(api_svc_json: &str, host: Option<&str>) -> Option<String> {
    match parse_service(api_svc_json) {
        Some((svc_type, Some(np), _)) if svc_type == "NodePort" => {
            host.map(|h| node_http_url(h, np, ""))
        }
        _ => None,
    }
}

/// Discover the platform API URL for a release.
///
/// Prefers the UI's `/api` proxy, which is how a default install is reached
/// with no port-forward. Falls back to the api service directly when the UI is
/// absent: `ui.deploy=false` is a legitimate way to run a Slack-only bot with a
/// smaller footprint, and it used to break EVERY `cluster` verb -- deploy,
/// versions, kill, delete -- with an error naming only the UI, which reads like
/// a broken release rather than a supported configuration (#1068).
pub async fn discover_api_url(namespace: &str, release: &str) -> Result<String> {
    let common = CommonOpts {
        namespace: namespace.to_string(),
        release: release.to_string(),
        dry_run: false,
    };
    // This function is only reached when no `--api-url` was supplied, so it is
    // already a cluster path: resolving the fullname here keeps the explicit
    // `--api-url` and `--dry-run` routes free of any kubectl call.
    //
    // The host lookup needs no fullname, so it runs alongside the resolution
    // rather than behind it -- the same idiom as
    // `cluster_observability_endpoints`. Only the Service reads below have to
    // wait for the name.
    let (fullname, host) = tokio::join!(release_fullname(namespace, release), resolve_node_host());
    let ui_svc = fullname.resource("ui");
    let api_svc = fullname.resource("api");

    if let Ok((true, ui_json, _)) = run_capture(&svc_cmd(&common, &fullname, "ui")).await {
        return ui_api_url_from_parts(&ui_json, host.as_deref());
    }

    // No UI. The api service may still be reachable on its own NodePort.
    if let Ok((true, api_json, _)) = run_capture(&svc_cmd(&common, &fullname, "api")).await {
        if let Some(url) = api_url_from_parts(&api_json, host.as_deref()) {
            return Ok(url);
        }
        // The port-forward hint names 8000:8000, not the old 8123: `cluster
        // deploy` no longer binds 8123 for its own tunnel, and a hint naming
        // that exact port would send the operator at the thing #1533 fixed.
        return Err(api_url_usage_err(format!(
            "the {ui_svc} service is absent (ui.deploy=false?) and {api_svc} is not NodePort-exposed, so there is no reachable platform API URL; expose it with --set api.service.type=NodePort, or pass --api-url (e.g. via `kubectl port-forward svc/{api_svc} 8000:8000`)"
        )));
    }

    Err(api_url_usage_err(format!(
        "could not read the {ui_svc} or {api_svc} service in namespace {namespace} to discover the platform API URL; pass --api-url to target the API directly"
    )))
}

/// First node InternalIP from `kubectl get nodes -o json`.
fn node_internal_ip(nodes_json: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(nodes_json).ok()?;
    for node in v.get("items")?.as_array()? {
        let addrs = node.get("status")?.get("addresses")?.as_array()?;
        for a in addrs {
            if a.get("type").and_then(|t| t.as_str()) == Some("InternalIP") {
                if let Some(ip) = a.get("address").and_then(|s| s.as_str()) {
                    return Some(ip.to_string());
                }
            }
        }
    }
    None
}

/// Print one service's access URL: a NodePort URL when exposed, else the
/// port-forward command to reach a ClusterIP service.
/// One resolved service URL row for `cluster status`. Owns its data so the
/// status reading can be rendered (byte-identical to the prior `ui.kv` output)
/// or serialized under `--json` (#485), instead of printing inline.
#[derive(Debug)]
pub struct ServiceUrl {
    label: String,
    name: String,
    namespace: String,
    api: bool,
    kind: ServiceUrlKind,
}

#[derive(Debug)]
enum ServiceUrlKind {
    NotFound,
    NodePortUrl(String),
    UnassignedNodePort,
    PortForward { local: u16, port: u16 },
    Unreadable,
}

impl ServiceUrl {
    /// Build the shared port-forward text after the caller chooses whether the
    /// URL target is plain (JSON) or styled (human output).
    fn port_forward_hint(&self, local: u16, port: u16, target: &str) -> String {
        port_forward_hint_with(&self.namespace, &self.name, local, port, target)
    }

    fn to_json(&self) -> serde_json::Value {
        let (url, note): (Option<String>, Option<String>) = match &self.kind {
            ServiceUrlKind::NodePortUrl(url) => (Some(url.clone()), None),
            ServiceUrlKind::NotFound => (None, Some(format!("service {} not found", self.name))),
            ServiceUrlKind::UnassignedNodePort => (
                None,
                Some(format!(
                    "service {} is NodePort but exposes no nodePort yet",
                    self.name
                )),
            ),
            ServiceUrlKind::PortForward { local, port } => {
                let suffix_path = api_suffix_path(self.api);
                (
                    None,
                    Some(self.port_forward_hint(
                        *local,
                        *port,
                        &format!("http://localhost:{local}{suffix_path}"),
                    )),
                )
            }
            ServiceUrlKind::Unreadable => {
                (None, Some(format!("could not read service {}", self.name)))
            }
        };
        serde_json::json!({"name": self.label, "url": url, "note": note})
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match &self.kind {
            ServiceUrlKind::NodePortUrl(url) => ui.kv(&self.label, &ui.url(url)),
            ServiceUrlKind::NotFound => {
                ui.kv(&self.label, &format!("service {} not found", self.name))
            }
            ServiceUrlKind::UnassignedNodePort => ui.kv(
                &self.label,
                &format!(
                    "service {} is NodePort but exposes no nodePort yet",
                    self.name
                ),
            ),
            ServiceUrlKind::PortForward { local, port } => {
                let suffix_path = api_suffix_path(self.api);
                ui.kv(
                    &self.label,
                    &self.port_forward_hint(
                        *local,
                        *port,
                        &ui.url(&format!("http://localhost:{local}{suffix_path}")),
                    ),
                )
            }
            ServiceUrlKind::Unreadable => ui.kv(
                &self.label,
                &format!("could not read service {}", self.name),
            ),
        }
    }
}

/// One `cluster status` URL row.
///
/// The displayed `name` and the name `svc_cmd` QUERIES come from the same
/// resolved fullname on purpose: they diverged before, so status could report a
/// service it had never asked about.
async fn resolve_service_url(
    o: &CommonOpts,
    fullname: &ReleaseFullname,
    suffix: &str,
    label: &str,
    host: &str,
    api: bool,
) -> ServiceUrl {
    let name = fullname.resource(suffix);
    let mk = |kind| ServiceUrl {
        label: label.to_string(),
        name: name.clone(),
        namespace: o.namespace.clone(),
        api,
        kind,
    };
    let (ok, out, _) = match run_capture(&svc_cmd(o, fullname, suffix)).await {
        Ok(res) => res,
        Err(_) => return mk(ServiceUrlKind::NotFound),
    };
    if !ok {
        return mk(ServiceUrlKind::NotFound);
    }
    // Same discovery core as `cluster observability` (#460); this owns the
    // wording, so the status output stays byte-identical.
    let kind = match resolve_service_endpoint(&out, host, api) {
        ServiceEndpoint::NodePortUrl(url) => ServiceUrlKind::NodePortUrl(url),
        ServiceEndpoint::UnassignedNodePort => ServiceUrlKind::UnassignedNodePort,
        ServiceEndpoint::PortForwardHint { local, port } => {
            ServiceUrlKind::PortForward { local, port }
        }
        ServiceEndpoint::Unreadable => ServiceUrlKind::Unreadable,
    };
    mk(kind)
}

/// From `kubectl get svc -o json`, return (type, first nodePort, first port).
fn parse_service(svc_json: &str) -> Option<(String, Option<u16>, u16)> {
    let v: serde_json::Value = serde_json::from_str(svc_json).ok()?;
    let spec = v.get("spec")?;
    let svc_type = spec.get("type").and_then(|t| t.as_str())?.to_string();
    let first_port = spec.get("ports")?.as_array()?.first()?;
    let node_port = first_port
        .get("nodePort")
        .and_then(|p| p.as_u64())
        .map(|p| p as u16);
    let port = first_port.get("port").and_then(|p| p.as_u64()).unwrap_or(0) as u16;
    Some((svc_type, node_port, port))
}

// ---------------------------------------------------------------------------
// Observability twin (issue #460).
// ---------------------------------------------------------------------------

/// Structured result of resolving one service's access endpoint.
///
/// A pure, structured value rather than a pre-formatted string: the caller owns
/// all formatting, because `cluster status`'s notes embed the service **name**
/// and its ClusterIP hint embeds **namespace + name** plus a styled `ui.url(..)`
/// mid-string. Pre-formatting that into a plain URL would break the PR#34
/// "status output visually unchanged" prior intent.
///
/// The four variants map the exact `parse_service` match arms in
/// `print_service_url`; the svc-fetch-failure / `!ok` "service not found" arms
/// stay in the async wrapper, before any JSON exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceEndpoint {
    /// NodePort-exposed: a fully built URL via `node_http_url(host, np, path)`.
    NodePortUrl(String),
    /// Type NodePort but no nodePort assigned yet.
    UnassignedNodePort,
    /// ClusterIP/other: reachable only via a port-forward.
    /// `local` is non-privileged: service ports below 1024 are offset by
    /// 18000, while an absent service port falls back to 8080.
    PortForwardHint { local: u16, port: u16 },
    /// `parse_service` returned None (malformed/unreadable JSON).
    Unreadable,
}

/// The path suffix that selects the console's API-backed view.
fn api_suffix_path(api: bool) -> &'static str {
    if api {
        "/?api=1"
    } else {
        ""
    }
}

/// Pure discovery core shared by `cluster status` and `cluster observability`:
/// map a service's JSON + a resolved node host to a structured endpoint.
/// `api` appends the Console's `/?api=1` suffix path.
fn resolve_service_endpoint(svc_json: &str, host: &str, api: bool) -> ServiceEndpoint {
    let path = api_suffix_path(api);
    match parse_service(svc_json) {
        Some((svc_type, node_port, _)) if svc_type == "NodePort" => match node_port {
            Some(np) => ServiceEndpoint::NodePortUrl(node_http_url(host, np, path)),
            None => ServiceEndpoint::UnassignedNodePort,
        },
        Some((_, _, port)) => ServiceEndpoint::PortForwardHint {
            local: match port {
                0 => 8080,
                1..=1023 => port + 18000,
                _ => port,
            },
            port,
        },
        None => ServiceEndpoint::Unreadable,
    }
}

/// The port-forward hint wording. `target` is the already-rendered URL text --
/// plain for machine payloads, styled for human output -- so the wording cannot
/// drift between the two callers.
fn port_forward_hint_with(ns: &str, name: &str, local: u16, port: u16, target: &str) -> String {
    format!("kubectl -n {ns} port-forward svc/{name} {local}:{port}  then {target}")
}

/// Plain, machine-safe hint (no ANSI): used for the observability `Endpoint.note`
/// that `--json` serializes.
fn port_forward_hint(ns: &str, name: &str, local: u16, port: u16, path: &str) -> String {
    port_forward_hint_with(
        ns,
        name,
        local,
        port,
        &format!("http://localhost:{local}{path}"),
    )
}

/// The platform API service's port (`<fullname>-api`, `api.service.port` in the
/// chart). Owned here so the port-forward hint carries no bare literal.
const API_SERVICE_PORT: u16 = 8000;

/// Map the UI service JSON + node host to the cluster's **API base** endpoint:
/// the UI `/api` proxy URL (the in-cluster way to reach the platform API, #360),
/// which is never browsable. Degrades to a `note` endpoint on any error.
///
/// The notes are minted here rather than borrowed from `ui_api_url_from_parts`
/// on purpose: that helper speaks `cluster deploy`'s error vocabulary, where
/// `--api-url` is a real escape hatch. `cluster observability` has no such flag,
/// so its rows must never name it. Instead the row reports the true condition
/// (`ui` service missing) or hands back an actionable port-forward for the API
/// service -- plain text, since `--json` serializes this note.
fn api_base_endpoint(
    o: &CommonOpts,
    fullname: &ReleaseFullname,
    ui_svc_json: Option<&str>,
    host: Option<&str>,
) -> crate::observability::Endpoint {
    let row = |url, note| crate::observability::Endpoint {
        name: "Curie API".to_string(),
        url,
        note,
        browsable: false,
    };
    let Some(ui_svc_json) = ui_svc_json else {
        return row(
            None,
            Some(format!("service {} not found", fullname.resource("ui"))),
        );
    };
    match ui_api_url_from_parts(ui_svc_json, host) {
        Ok(url) => row(Some(url), None),
        // Any other failure -- ClusterIP / `--no-expose` (a supported install
        // mode), an unassigned nodePort, an unreadable service, or an
        // unresolvable host -- still leaves a way in: port-forward the API
        // service directly. The operator copies and runs that line, so it must
        // name the object the chart actually rendered.
        Err(_) => row(
            None,
            Some(port_forward_hint(
                &o.namespace,
                &fullname.resource("api"),
                API_SERVICE_PORT,
                API_SERVICE_PORT,
                "",
            )),
        ),
    }
}

/// Map one release service to an observability [`Endpoint`], degrading to a
/// `note` row (never a hard failure, never a message smuggled into `url`) when
/// the service is missing, unsettled, unreadable, or reachable only by a
/// port-forward.
fn service_surface(
    o: &CommonOpts,
    fullname: &ReleaseFullname,
    suffix: &str,
    name: &str,
    svc_json: Option<&str>,
    host: Option<&str>,
    api: bool,
) -> crate::observability::Endpoint {
    let svc_name = fullname.resource(suffix);
    let degraded = |note: String| crate::observability::Endpoint {
        name: name.to_string(),
        url: None,
        note: Some(note),
        browsable: false,
    };
    let Some(svc_json) = svc_json else {
        return degraded(format!("service {svc_name} not found"));
    };
    let Some(host) = host else {
        return degraded(format!(
            "could not determine a node host to reach service {svc_name}"
        ));
    };
    match resolve_service_endpoint(svc_json, host, api) {
        ServiceEndpoint::NodePortUrl(url) => crate::observability::Endpoint {
            name: name.to_string(),
            url: Some(url),
            note: None,
            browsable: true,
        },
        ServiceEndpoint::UnassignedNodePort => degraded(format!(
            "service {svc_name} is NodePort but exposes no nodePort yet"
        )),
        ServiceEndpoint::PortForwardHint { local, port } => degraded(port_forward_hint(
            &o.namespace,
            &svc_name,
            local,
            port,
            api_suffix_path(api),
        )),
        ServiceEndpoint::Unreadable => degraded(format!("could not read service {svc_name}")),
    }
}

/// Fetch one release service's JSON, or None when kubectl cannot read it.
async fn fetch_service(o: &CommonOpts, fullname: &ReleaseFullname, suffix: &str) -> Option<String> {
    match run_capture(&svc_cmd(o, fullname, suffix)).await {
        Ok((true, out, _)) => Some(out),
        _ => None,
    }
}

/// The cluster tier's three observability surfaces (payload parity with local):
/// Console via the `ui` service, Langfuse via `langfuse-web`, and the API base
/// via the UI `/api` proxy. Degrades per endpoint; never hard-fails.
pub async fn cluster_observability_endpoints(
    opts: &CommonOpts,
) -> Vec<crate::observability::Endpoint> {
    // Deliberately `resolve_node_host()` (Option -> a degraded note), NOT
    // `cluster status`'s `discover_host()` (which fabricates `localhost` when
    // neither the kubeconfig server URL nor a node InternalIP is readable).
    // This twin's primary consumer is a coding agent reading `--json`
    // (ADR-0021/0038), and a `localhost` URL that will not resolve is worse for
    // it than an explicit note saying the host could not be determined. It also
    // matches the `resolve_node_host()`+Option pattern #360 set for every
    // URL-producing path (`discover_api_url`) and the `api_base_endpoint`
    // row. `cluster status` stays human-facing and keeps its display
    // convenience.
    //
    // Resolved once, before the fan-out: both service reads and all three rows
    // must agree on the release's rendered name. This is a live path only --
    // `observability`'s `--dry-run` branch returns before reaching here.
    // `resolve_node_host()` needs no fullname, so it runs alongside the
    // resolution instead of behind it; only the service reads have to wait.
    let (fullname, host) = tokio::join!(
        release_fullname(&opts.namespace, &opts.release),
        resolve_node_host(),
    );
    let (ui_svc, langfuse_svc) = tokio::join!(
        fetch_service(opts, &fullname, "ui"),
        fetch_service(opts, &fullname, "langfuse-web"),
    );
    vec![
        service_surface(
            opts,
            &fullname,
            "ui",
            "Curie Console",
            ui_svc.as_deref(),
            host.as_deref(),
            true,
        ),
        service_surface(
            opts,
            &fullname,
            "langfuse-web",
            "Langfuse UI (traces / cost / evals)",
            langfuse_svc.as_deref(),
            host.as_deref(),
            false,
        ),
        api_base_endpoint(opts, &fullname, ui_svc.as_deref(), host.as_deref()),
    ]
}

/// The read-only commands `curie cluster observability` runs (and prints under
/// `--dry-run`).
///
/// A superset of what actually runs, not a 1:1 trace: `resolve_node_host` only
/// falls through to `nodes_cmd()` when `kubeconfig_host_cmd()` yields no host.
pub fn observability_commands(o: &CommonOpts, fullname: &ReleaseFullname) -> Vec<OpsCommand> {
    vec![
        kubeconfig_host_cmd(),
        nodes_cmd(),
        svc_cmd(o, fullname, "ui"),
        svc_cmd(o, fullname, "langfuse-web"),
    ]
}

/// `cluster observability`: resolve the release's observability surfaces with
/// the same discovery `cluster status` does, and return them for `emit`.
///
/// Agent-first: a browser is opened only when the human passes `--open`, and
/// never under `--json`.
pub async fn observability(
    opts: CommonOpts,
    open: bool,
) -> Result<crate::observability::ObservabilityOutput> {
    if opts.dry_run {
        // No cluster call, so no discovery: the printed names follow the
        // chart's no-override rule and an override install renders them
        // differently. `dry_run_fullname` emits that caveat with the value.
        let fullname = dry_run_fullname(&opts.release);
        return Ok(crate::observability::ObservabilityOutput::DryRun(
            crate::ui::DryRunPlan {
                lines: observability_commands(&opts, &fullname)
                    .iter()
                    .map(|cmd| cmd.display())
                    .collect(),
            },
        ));
    }
    require_on_path("kubectl")?;
    let surfaces = cluster_observability_endpoints(&opts).await;
    let ui = crate::ui::ui();
    crate::observability::open_endpoints(&surfaces, open, ui.json()).await;
    // The cluster counterpart of the local tier's hint: stderr guidance, not
    // payload, since resolving a service says nothing about whether the release
    // is actually serving.
    ui.note("start these surfaces with `curie cluster up` if they are unreachable");
    Ok(crate::observability::ObservabilityOutput::Surfaces(
        surfaces,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn common() -> CommonOpts {
        CommonOpts {
            namespace: "curie".into(),
            release: "curie".into(),
            dry_run: false,
        }
    }

    /// The default release's resolved fullname, for the builders that now take
    /// one. `curie` contains the chart name, so this is byte-identical to the
    /// names these tests have always asserted -- see
    /// `chart_fullname_tests::the_default_release_is_a_byte_identical_no_op`.
    fn fullname() -> ReleaseFullname {
        chart_fullname("curie")
    }

    #[test]
    fn credential_prefix_inference_matches_the_shared_provider_registry() {
        #[derive(serde::Deserialize)]
        struct Registry {
            providers: Vec<Provider>,
        }

        #[derive(serde::Deserialize)]
        struct Provider {
            name: String,
            inferred_provider: Option<String>,
            credential_examples: Vec<String>,
        }

        let registry: Registry = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tests/vectors/model-provider-registry.json"
        )))
        .expect("parse provider registry");

        for provider in registry.providers {
            for credential in provider.credential_examples {
                assert_eq!(
                    provider_from_credential_prefix(&credential),
                    provider.inferred_provider.as_deref(),
                    "credential example for {}",
                    provider.name
                );
            }
        }
    }

    fn mail_upgrade_opts(existing: &serde_json::Value, set: Vec<String>) -> UpOpts {
        complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                chart: "charts/curie".into(),
                no_expose: true,
                set,
                set_string: vec![],
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
                secrets: vec![],
                github_token: GithubTokenPlan::Untouched,
                dev: true,
            },
            Some(existing),
            None,
            false,
        )
        .unwrap()
    }

    fn retained_mail_fixture() -> serde_json::Value {
        serde_json::json!({
            "mailAdapter": {
                "deploy": true,
                "inbox": "mail@example.com",
                "allowedSenders": ["operator@example.com"],
                "pollIntervalSeconds": 37,
                "persistence": {"existingClaim": "acme-mail-state"},
                "agentmail": {
                    "apiKeyExistingSecret": "acme-mail-credentials",
                    "apiKeyExistingSecretKey": "provider-key",
                    "httpsCidrs": ["203.0.113.8/32"]
                },
                "channelTokenExistingSecret": "acme-mail-credentials",
                "channelTokenExistingSecretKey": "channel-key",
                "egressSecretExistingSecret": "acme-mail-credentials",
                "egressSecretExistingSecretKey": "egress-key"
            },
            "worker": {
                "adapterCredentialsExistingSecret": "acme-mail-credentials",
                "adapterCredentialsExistingSecretKey": "worker-map"
            }
        })
    }

    fn materialized_mail_values(opts: &UpOpts) -> serde_json::Value {
        let commands = up_commands(opts);
        let command = &commands[0];
        let (materialized, _guards) = command.materialize_secret_files().unwrap();
        let argv = materialized.argv();
        for pair in argv.windows(2).filter(|pair| pair[0] == "-f") {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&pair[1]).unwrap()).unwrap();
            if value.get("mailAdapter").is_some() {
                return value;
            }
        }
        panic!("plain cluster up dropped the installed mail values");
    }

    #[test]
    fn plain_up_preserves_mail_lifecycle_and_external_secret_pairs() {
        let existing = retained_mail_fixture();
        let opts = mail_upgrade_opts(&existing, vec![]);
        let actual = materialized_mail_values(&opts);
        assert_eq!(
            actual, existing,
            "mail and worker credential state must survive unchanged"
        );
        let mut leaves = BTreeMap::new();
        crate::installation::flatten_values(&actual, "", &mut leaves);
        for key in leaves.keys() {
            assert!(is_preserved_by_up(key), "diff disagrees with up for {key}");
        }
        let shown = up_commands(&opts)[0].display();
        for private in ["mail@example.com", "acme-mail-credentials", "provider-key"] {
            assert!(
                !shown.contains(private),
                "private retained mail values leaked"
            );
        }
    }

    #[test]
    fn retained_mail_command_and_debug_hide_dynamic_keys_and_inline_secrets() {
        let mut existing = retained_mail_fixture();
        existing["worker"]["adapterCredentials"] = serde_json::json!({
            "private-adapter-identity": "private-inline-credential"
        });
        existing["worker"]
            .as_object_mut()
            .unwrap()
            .remove("adapterCredentialsExistingSecret");
        existing["worker"]
            .as_object_mut()
            .unwrap()
            .remove("adapterCredentialsExistingSecretKey");
        existing["mailAdapter"]["podAnnotations"] = serde_json::json!({
            "private.example.com/identity": "private-annotation-value"
        });
        let opts = mail_upgrade_opts(&existing, vec![]);
        let command = &up_commands(&opts)[0];
        let display = command.display();
        let debug = format!("{command:?}");
        let (materialized, _guards) = command.materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        for private in [
            "private-adapter-identity",
            "private-inline-credential",
            "private.example.com/identity",
            "private-annotation-value",
        ] {
            for surface in [&display, &debug, &argv] {
                assert!(!surface.contains(private), "retained mail metadata leaked");
            }
        }
        assert_eq!(materialized_mail_values(&opts), existing);
    }

    #[test]
    fn plain_mail_upgrade_never_replays_inline_copies_behind_external_sources() {
        let expected = retained_mail_fixture();
        let mut existing = expected.clone();
        existing["mailAdapter"]["channelToken"] = "stale-channel-token".into();
        existing["mailAdapter"]["egressSecret"] = "stale-egress-secret".into();
        existing["mailAdapter"]["agentmail"]["apiKey"] = "stale-provider-key".into();
        existing["worker"]["adapterCredentials"] =
            serde_json::json!({"mail-adapter": "stale-egress-secret"});
        let opts = mail_upgrade_opts(&existing, vec![]);
        assert_eq!(materialized_mail_values(&opts), expected);
    }

    #[test]
    fn explicit_mail_disable_and_secret_clear_override_retained_values() {
        let mut existing = retained_mail_fixture();
        existing["mailAdapter"]["channelToken"] = "obsolete-inline-token".into();
        let opts = mail_upgrade_opts(
            &existing,
            vec![
                "mailAdapter.deploy=false".into(),
                "mailAdapter.channelTokenExistingSecret=".into(),
                "mailAdapter.allowedSenders={}".into(),
            ],
        );
        let actual = materialized_mail_values(&opts);
        assert!(actual["mailAdapter"].get("deploy").is_none());
        assert!(actual["mailAdapter"].get("allowedSenders").is_none());
        assert!(
            actual["mailAdapter"].get("channelToken").is_none(),
            "clearing an external reference cannot resurrect an obsolete inline token"
        );
        assert!(actual["mailAdapter"]
            .get("channelTokenExistingSecret")
            .is_none());
        assert_eq!(actual["mailAdapter"]["pollIntervalSeconds"], 37);
        let effective = up_value_plan(&opts).effective_values();
        assert_eq!(
            effective.get("mailAdapter.deploy").map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn explicit_inline_mail_credential_replaces_the_external_reference() {
        let opts = mail_upgrade_opts(
            &retained_mail_fixture(),
            vec!["mailAdapter.channelToken=new-inline-token".into()],
        );
        let actual = materialized_mail_values(&opts);
        assert!(actual["mailAdapter"]
            .get("channelTokenExistingSecret")
            .is_none());
        assert_eq!(
            actual["mailAdapter"]["channelTokenExistingSecretKey"],
            "channel-key"
        );
        assert_eq!(
            actual["worker"]["adapterCredentialsExistingSecretKey"],
            "worker-map"
        );
    }

    #[test]
    fn up_defaults_expose_ui_and_langfuse() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        assert_eq!(cmds.len(), 1);
        let line = cmds[0].display();
        assert_eq!(
            line,
            "helm upgrade --install curie charts/curie -n curie --create-namespace \
             --set ui.service.type=NodePort --set langfuse.web.service.type=NodePort"
        );
    }

    #[test]
    fn plain_up_re_supplies_recorded_worker_extra_env_without_reuse_values() {
        let existing = serde_json::json!({
            "worker": {
                "extraEnv": [
                    {
                        "name": "PROVIDER_BASE_URL",
                        "value": "https://provider.example.com/v1"
                    },
                    {
                        "name": "FALLBACK_BASE_URL",
                        "value": "https://fallback.example.com/v1"
                    }
                ]
            }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: false,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        for assignment in [
            "worker.extraEnv[0].name=PROVIDER_BASE_URL",
            "worker.extraEnv[0].value=https://provider.example.com/v1",
            "worker.extraEnv[1].name=FALLBACK_BASE_URL",
            "worker.extraEnv[1].value=https://fallback.example.com/v1",
        ] {
            assert!(
                argv.contains(&format!("--set-string {assignment}")),
                "plain up dropped recorded worker extraEnv leaf {assignment}: {argv}"
            );
        }
        assert!(
            !argv.contains("--reuse-values"),
            "up must remain a full Helm upgrade: {argv}"
        );
    }

    #[test]
    fn explicit_worker_extra_env_leaves_override_the_recorded_family() {
        let existing = serde_json::json!({
            "worker": {
                "extraEnv": [{
                    "name": "RECORDED_PROVIDER_BASE_URL",
                    "value": "https://recorded.example.com/v1"
                }]
            }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![
                    "worker.extraEnv[0].name=OPERATOR_PROVIDER_BASE_URL".into(),
                    "worker.extraEnv[0].value=https://operator.example.com/v1".into(),
                ],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("--set-string worker.extraEnv[0].name=OPERATOR_PROVIDER_BASE_URL"),
            "the explicit worker extraEnv name must reach Helm: {argv}"
        );
        assert!(
            argv.contains("--set-string worker.extraEnv[0].value=https://operator.example.com/v1"),
            "the explicit worker extraEnv value must reach Helm: {argv}"
        );
        assert!(
            !argv.contains("RECORDED_PROVIDER_BASE_URL")
                && !argv.contains("https://recorded.example.com/v1"),
            "explicit worker extraEnv input must suppress the recorded family: {argv}"
        );
    }

    #[test]
    fn plain_up_escapes_commas_in_recorded_worker_extra_env_values() {
        let existing = serde_json::json!({
            "worker": {
                "extraEnv": [{
                    "name": "NO_PROXY",
                    "value": "10.0.0.0/8,localhost"
                }]
            }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
        )
        .unwrap();

        assert_eq!(
            up_value_plan(&opts)
                .effective_values()
                .get("worker.extraEnv[0].value"),
            Some(&"10.0.0.0/8,localhost".to_string()),
            "the escaped Helm expression must retain the recorded semantic value"
        );

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv();
        assert!(
            argv.contains(&"worker.extraEnv[0].value=10.0.0.0/8\\,localhost".into()),
            "recorded worker extraEnv values must escape commas for Helm: {argv:?}"
        );
    }

    /// A plain `cluster up` for an unrelated reason must not silently switch
    /// the worker back to refusing every dev reply endpoint the operator had
    /// already trusted (issue #1897).
    #[test]
    fn plain_up_re_supplies_recorded_slack_trusted_origins_without_reuse_values() {
        let existing = serde_json::json!({
            "worker": { "slackTrustedOrigins": "http://host.docker.internal" }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("--set-string worker.slackTrustedOrigins=http://host.docker.internal"),
            "plain up dropped the recorded Slack trusted origin: {argv}"
        );
        assert!(
            !argv.contains("--reuse-values"),
            "up must remain a full Helm upgrade: {argv}"
        );
    }

    /// An operator who names the trusted origins on this run owns the key: the
    /// stale recorded list must not be smuggled back alongside it, or the
    /// worker keeps trusting a host the operator just removed.
    #[test]
    fn explicit_slack_trusted_origins_override_the_recorded_value() {
        let existing = serde_json::json!({
            "worker": { "slackTrustedOrigins": "https://recorded.example.com" }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec!["worker.slackTrustedOrigins=https://trusted.example.com".into()],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("--set-string worker.slackTrustedOrigins=https://trusted.example.com"),
            "the explicit Slack trusted origin must reach Helm: {argv}"
        );
        assert!(
            !argv.contains("recorded.example.com"),
            "explicit trusted origins must suppress the recorded value: {argv}"
        );
    }

    /// A plain `--set` names the trusted origins on this run just as much as
    /// `--set-string` does -- an operator using the shorthand flag must still
    /// own the key, not have the stale recorded list smuggled back alongside it.
    #[test]
    fn explicit_set_slack_trusted_origins_override_the_recorded_value() {
        let existing = serde_json::json!({
            "worker": { "slackTrustedOrigins": "https://recorded.example.com" }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec!["worker.slackTrustedOrigins=https://trusted.example.com".into()],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
        )
        .unwrap();

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("--set worker.slackTrustedOrigins=https://trusted.example.com"),
            "the explicit Slack trusted origin must reach Helm: {argv}"
        );
        assert!(
            !argv.contains("recorded.example.com"),
            "explicit trusted origins must suppress the recorded value: {argv}"
        );
    }

    /// The key is a COMMA-SEPARATED origin list, so an operator who trusts two
    /// hosts must not have Helm read the second one as a list element and the
    /// diff report an origin the worker was never sent.
    #[test]
    fn plain_up_round_trips_multi_origin_slack_trusted_origins() {
        let recorded = "http://host.docker.internal,http://10.20.30.40";
        let existing = serde_json::json!({
            "worker": { "slackTrustedOrigins": recorded }
        });
        let opts = complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: false,
                no_expose: true,
                set: vec![],
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            Some(&existing),
            None,
            false,
        )
        .unwrap();

        assert_eq!(
            up_value_plan(&opts)
                .effective_values()
                .get("worker.slackTrustedOrigins"),
            Some(&recorded.to_string()),
            "the escaped Helm expression must retain the recorded origin list verbatim"
        );

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv();
        assert!(
            argv.contains(
                &"worker.slackTrustedOrigins=http://host.docker.internal\\,http://10.20.30.40"
                    .into()
            ),
            "a recorded origin list must escape its commas for Helm: {argv:?}"
        );
    }

    /// Preservation must never widen where the platform bot token can go: a
    /// release that never trusted an extra origin -- whose list was
    /// deliberately cleared, or that has no recorded release at all (a fresh
    /// install) -- keeps the chart's fail-closed empty default.
    #[test]
    fn up_invents_no_slack_trusted_origins_when_none_is_recorded() {
        for existing in [
            Some(serde_json::json!({ "worker": { "slackTrustedOrigins": "" } })),
            Some(serde_json::json!({ "worker": {} })),
            None,
        ] {
            let opts = complete_up_opts_without_runner_egress(
                UpOpts {
                    retained_mail_values: None,
                    common: common(),
                    github_token: GithubTokenPlan::Untouched,
                    allow_egress_host: vec![],
                    resolved_egress_cidrs: vec![],
                    chart: "charts/curie".into(),
                    secrets: vec![],
                    dev: false,
                    no_expose: true,
                    set: vec![],
                    set_string: vec![],
                    allow_web_egress: vec![],
                    fake_model: false,
                    credentials: None,
                    local_model: None,
                    model: None,
                },
                existing.as_ref(),
                None,
                false,
            )
            .unwrap();

            let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
            let argv = materialized.argv().join(" ");
            assert!(
                !argv.contains("slackTrustedOrigins"),
                "up must not supply a trusted-origin value it has no record of: {argv}"
            );
        }
    }

    /// `curie diff` has to agree with what `up` actually does: announcing a
    /// reset for a value `up` hands straight back sends the operator chasing a
    /// change that never happens. And the list is hostnames, not a credential
    /// -- masking it would hide the very dev configuration the operator opens
    /// `diff` to confirm.
    #[test]
    fn slack_trusted_origins_are_preserved_and_never_masked() {
        assert!(
            is_preserved_by_up("worker.slackTrustedOrigins"),
            "diff must not report a reset for a key up re-supplies"
        );
        assert!(
            !is_secret_value_key("worker.slackTrustedOrigins"),
            "the trusted-origin list is operator-visible configuration, not a credential"
        );
    }

    #[test]
    fn up_no_expose_drops_the_nodeport_sets() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("NodePort"), "{line}");
        assert!(line.ends_with("--create-namespace"), "{line}");
    }

    #[test]
    fn up_passthrough_set_is_appended_verbatim() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec!["worker.replicas=2".into(), "dispatcher.deploy=false".into()],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.ends_with("--set worker.replicas=2 --set dispatcher.deploy=false"),
            "{line}"
        );
    }

    #[test]
    fn up_without_credentials_installs_sealed() {
        // No credential and not --fake-model: a plain install with no real-model
        // or egress sets (the fake model stays on, egress stays fail-closed).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("agentSandbox.runner.fakeModel"), "{line}");
        assert!(!line.contains("agentSandbox.runner.credentials"), "{line}");
        assert!(!line.contains("allowedEgress"), "{line}");
    }

    #[test]
    fn up_fake_model_installs_sealed_like_no_credential() {
        // --fake-model resolves to no credential, so the argv is the sealed
        // install even when the caller had a credential in the environment.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: true,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("agentSandbox.runner"), "{line}");
        assert!(!line.contains("allowedEgress"), "{line}");
    }

    #[test]
    fn up_with_credentials_enables_real_model_and_masks() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["192.0.2.10/32".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("agentSandbox.runner.fakeModel=false"),
            "{line}"
        );
        // Credential is masked in the printed form and never leaks. It is now
        // shown as part of a `-f` secret values file, not a `--set`.
        assert!(
            line.contains("agentSandbox.runner.credentials=sk-ant-s***"),
            "{line}"
        );
        assert!(
            line.contains("-f '<secret values file:"),
            "credential should be delivered via a -f values file: {line}"
        );
        assert!(!line.contains("secretsecret"), "secret leaked: {line}");
        // Model-provider egress entry (array-index keys print single-quoted).
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=192.0.2.10/32'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].port=443'"),
            "{line}"
        );

        // Success criterion: the live credential must NOT reach the executed argv
        // (the process table). Instead helm gets `-f <path>` pointing at a private
        // 0600 file that carries the secret. Materialize the command the way the
        // executor does and inspect the real argv + file.
        let (materialized, guards) = cmds[0]
            .materialize_secret_files()
            .expect("materializing the secret values file");
        let argv = materialized.argv();
        let argv_joined = argv.join(" ");
        assert!(
            !argv_joined.contains("secretsecret"),
            "credential leaked into argv: {argv_joined}"
        );
        assert!(
            !argv_joined.contains("agentSandbox.runner.credentials="),
            "credential --set leaked into argv: {argv_joined}"
        );

        // A `-f <values-file>` pair is present; the file exists, is 0600, and
        // contains the real credential (as nested YAML/JSON helm can read).
        let f_pos = argv
            .iter()
            .position(|a| a == "-f")
            .expect("a -f flag in the materialized argv");
        let values_path = std::path::PathBuf::from(&argv[f_pos + 1]);
        assert!(values_path.exists(), "values file {values_path:?} missing");
        let body = std::fs::read_to_string(&values_path).expect("reading the values file");
        assert!(
            body.contains("sk-ant-secretsecret"),
            "credential missing from values file: {body}"
        );
        // It nests the dotted key correctly for helm.
        assert!(
            body.contains("agentSandbox")
                && body.contains("runner")
                && body.contains("credentials"),
            "values file is not the expected nested shape: {body}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&values_path)
                .expect("stat values file")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "values file must be 0600, was {mode:o}");
        }

        // The guard removes the file when dropped, so the secret never outlives
        // the helm run.
        drop(guards);
        assert!(
            !values_path.exists(),
            "values file should be deleted once the guard drops"
        );
    }

    #[test]
    fn resolve_up_credentials_reflects_env_and_fake_model() {
        // Env set, not fake: real model.
        assert_eq!(
            resolve_up_credentials(false, Some("sk-ant-x".into())).as_deref(),
            Some("sk-ant-x")
        );
        // --fake-model wins even with a credential in the environment.
        assert_eq!(resolve_up_credentials(true, Some("sk-ant-x".into())), None);
        // Empty and absent both mean sealed.
        assert_eq!(resolve_up_credentials(false, Some(String::new())), None);
        assert_eq!(resolve_up_credentials(false, None), None);
    }

    #[test]
    fn with_env_stores_the_pairs() {
        let cmd =
            OpsCommand::new("docker", vec![plain("ps")]).with_env(vec![("A".into(), "1".into())]);
        assert_eq!(cmd.env, vec![("A".to_string(), "1".to_string())]);
    }

    #[test]
    fn display_renders_sorted_env_before_program() {
        let cmd = OpsCommand::new("docker", vec![plain("ps")])
            .with_env(vec![("B".into(), "2".into()), ("A".into(), "1".into())]);
        assert!(cmd.display().starts_with("A=1 "));
    }

    #[test]
    fn up_local_model_adds_inference_sets() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: Some("qwen3:4b".into()),
            model: None,
        });
        let line = cmds[0].display();
        assert!(line.contains("--set inference.deploy=true"), "{line}");
        assert!(line.contains("--set inference.model=qwen3:4b"), "{line}");
    }

    #[test]
    fn up_without_local_model_omits_inference_sets() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("inference.deploy"), "{line}");
        assert!(!line.contains("inference.model"), "{line}");
    }

    #[test]
    fn up_defaults_runner_model_from_env() {
        // CURIE_MODEL set, no explicit --set: inject the runner model (#361).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: Some("z-ai/glm-5.2".into()),
        });
        let line = cmds[0].display();
        assert!(
            line.contains("agentSandbox.runner.model=z-ai/glm-5.2"),
            "{line}"
        );
    }

    #[test]
    fn up_without_env_model_omits_runner_model_set() {
        // No CURIE_MODEL: inject nothing, the chart default stands (#361).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("agentSandbox.runner.model="), "{line}");
    }

    #[test]
    fn up_explicit_set_model_suppresses_env_injection() {
        // CURIE_MODEL set AND an explicit matching --set: the operator's set
        // already carries it, so no duplicate injection (#361).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec!["agentSandbox.runner.model=z-ai/glm-5.2".into()],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: Some("z-ai/glm-5.2".into()),
        });
        let line = cmds[0].display();
        assert_eq!(
            line.matches("agentSandbox.runner.model=z-ai/glm-5.2")
                .count(),
            1,
            "runner model should appear exactly once (no duplicate injection): {line}"
        );
    }

    #[test]
    fn up_commands_comma_joined_explicit_suppresses_injection() {
        // The runner model pinned alongside another key in a comma-joined
        // `--set` must be detected so `up` does not inject a redundant
        // `--set agentSandbox.runner.model=<model>` on top of it (#361).
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec!["worker.replicas=2,agentSandbox.runner.model=glm".into()],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: Some("glm".into()),
        });
        let line = cmds[0].display();
        assert_eq!(
            line.matches("agentSandbox.runner.model=glm").count(),
            1,
            "runner model should appear exactly once (no duplicate injection): {line}"
        );
    }

    #[test]
    fn check_runner_model_conflict_mismatch_is_err() {
        let set = vec!["agentSandbox.runner.model=sonnet".into()];
        let err = check_runner_model_conflict(Some("glm"), &set).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("glm"), "{msg}");
        assert!(msg.contains("sonnet"), "{msg}");
    }

    #[test]
    fn check_runner_model_conflict_matching_is_ok() {
        let set = vec!["agentSandbox.runner.model=glm".into()];
        assert!(check_runner_model_conflict(Some("glm"), &set).is_ok());
    }

    #[test]
    fn check_runner_model_conflict_no_env_is_ok() {
        // No CURIE_MODEL: an explicit operator set stands, no conflict.
        let set = vec!["agentSandbox.runner.model=sonnet".into()];
        assert!(check_runner_model_conflict(None, &set).is_ok());
    }

    #[test]
    fn check_runner_model_conflict_no_explicit_set_is_ok() {
        // CURIE_MODEL set, no explicit set: nothing to conflict with.
        assert!(check_runner_model_conflict(Some("glm"), &[]).is_ok());
    }

    #[test]
    fn check_runner_model_conflict_comma_joined_detects_mismatch() {
        // Helm accepts `--set a=1,b=2`; the runner model pinned alongside another
        // key must still be detected so the conflict fails loud (#361).
        let set = vec!["worker.replicas=2,agentSandbox.runner.model=glm".into()];
        let err = check_runner_model_conflict(Some("sonnet"), &set).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("sonnet"), "{msg}");
        assert!(msg.contains("glm"), "{msg}");
    }

    #[test]
    fn check_runner_model_conflict_comma_joined_model_first_matches() {
        // The model assignment leading a comma-joined element must not swallow
        // the trailing key into its value (which would falsely report a
        // conflict); a matching model is a legitimate, non-conflicting install.
        let set = vec!["agentSandbox.runner.model=glm,worker.replicas=2".into()];
        assert!(check_runner_model_conflict(Some("glm"), &set).is_ok());
    }

    #[test]
    fn up_opens_web_egress_after_model() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["192.0.2.10/32".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec!["203.0.113.0/24".into()],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=192.0.2.10/32'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].cidr=203.0.113.0/24'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].ports[0].protocol=TCP'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].ports[0].port=443'"),
            "{line}"
        );
    }

    #[test]
    fn up_web_egress_without_model_uses_index_zero() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            allow_web_egress: vec!["0.0.0.0/0".into()],
            fake_model: true,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("160.79.104.0/23"), "{line}");
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=0.0.0.0/0'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].port=443'"),
            "{line}"
        );
    }

    #[test]
    fn up_web_egress_multiple_cidrs_contiguous() {
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["192.0.2.10/32".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec!["203.0.113.0/24".into(), "198.51.100.0/24".into()],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=192.0.2.10/32'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].cidr=203.0.113.0/24'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[2].cidr=198.51.100.0/24'"),
            "{line}"
        );
    }

    #[test]
    fn up_no_web_egress_stays_sealed() {
        let sealed_cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let sealed_line = sealed_cmds[0].display();
        assert!(!sealed_line.contains("allowedEgress"), "{sealed_line}");

        let model_cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
            model: None,
        });
        let model_line = model_cmds[0].display();
        assert!(!model_line.contains("allowedEgress[1]"), "{model_line}");
    }

    #[test]
    fn validate_web_egress_cidrs_accepts_valid_and_rejects_bad() {
        // Valid IPv4 CIDR and both catch-all forms pass.
        assert!(validate_web_egress_cidrs(&["203.0.113.0/24".into()]).is_ok());
        assert!(validate_web_egress_cidrs(&["0.0.0.0/0".into()]).is_ok());
        assert!(validate_web_egress_cidrs(&["::/0".into()]).is_ok());

        // A value with a comma is rejected (would split into multiple --set).
        let err = validate_web_egress_cidrs(&[
            "10.0.0.0/8,security.networkPolicy.allowedEgress[0].cidr=0.0.0.0/0".into(),
        ])
        .unwrap_err()
        .to_string();
        assert!(err.contains("10.0.0.0/8,"), "{err}");

        // A value with an `=` is rejected.
        assert!(validate_web_egress_cidrs(&["10.0.0.0/8=x".into()]).is_err());

        // A bare address with no /prefix is rejected.
        assert!(validate_web_egress_cidrs(&["10.0.0.0".into()]).is_err());

        // An out-of-range prefix is rejected.
        assert!(validate_web_egress_cidrs(&["10.0.0.0/33".into()]).is_err());
    }

    #[test]
    fn default_route_egress_warning_fires_on_default_routes() {
        // The distinct rail-removal warning names the offending route and says
        // the sandbox can reach the entire internet -- for both catch-all forms
        // and for any `/0` prefix, which ignores the address bits.
        for route in ["0.0.0.0/0", "::/0", "10.0.0.0/0"] {
            let warning = default_route_egress_warning(&[route.into()])
                .unwrap_or_else(|| panic!("expected a warning for {route}"));
            assert!(warning.contains("removes the egress rail"), "{warning}");
            assert!(warning.contains("entire internet"), "{warning}");
            assert!(warning.contains(route), "{warning}");
        }

        // The offending route is called out even when mixed with scoped CIDRs.
        let warning = default_route_egress_warning(&["203.0.113.0/24".into(), "0.0.0.0/0".into()])
            .expect("expected a warning when a default route is present");
        assert!(warning.contains("0.0.0.0/0"), "{warning}");

        // No default route -> no warning (and it is distinct from the generic
        // "N declared destination(s)" note, which still fires separately).
        assert!(default_route_egress_warning(&[]).is_none());
        assert!(default_route_egress_warning(&["203.0.113.0/24".into()]).is_none());
        assert!(default_route_egress_warning(&["10.0.0.0/8".into()]).is_none());
        // A `/0`-suffixed *host* octet is not a default route (prefix is 24).
        assert!(default_route_egress_warning(&["10.0.0.10/24".into()]).is_none());
    }

    // A fixture whose release differs from its namespace, so an assertion on the
    // ownership label VALUE unambiguously locks it to the release (not the ns).
    fn common_distinct_release() -> CommonOpts {
        CommonOpts {
            namespace: "agent-ns".into(),
            release: "prod-release".into(),
            dry_run: false,
        }
    }

    // #707 ownership-aware teardown. `down` deletes only the namespaces THIS
    // release created (carrying the release-scoped ownership label `up` stamped),
    // instead of the old hardcoded `curie agent-sandbox-system` literal sweep.
    // A pre-existing (unlabeled) namespace is left untouched.
    #[test]
    fn down_deletes_only_release_owned_namespaces_by_label() {
        let cmds = down_commands(&common_distinct_release());
        assert_eq!(cmds.len(), 2);
        assert_eq!(cmds[0].display(), "helm uninstall prod-release -n agent-ns");
        let sweep = cmds[1].display();
        // Label-selector-scoped delete keyed on THIS release's ownership labels.
        // #1654: the selector is the conjunction of release name AND install
        // namespace, so it cannot reach another release's namespaces.
        assert_eq!(
            sweep,
            "kubectl delete namespace -l curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns --ignore-not-found"
        );
        // Negative case: the pre-existing shared namespace is no longer an
        // unconditional delete target (that would strand pre-existing state).
        assert!(!sweep.contains("agent-sandbox-system"), "{sweep}");
        // ignore-not-found preserved so a partial teardown stays re-runnable.
        assert!(sweep.contains("--ignore-not-found"), "{sweep}");
    }

    // #707 CRD retention is by-construction; lock it so no future edit sweeps the
    // agents.x-k8s.io CRDs during teardown.
    #[test]
    fn down_never_deletes_crds() {
        let cmds = down_commands(&common_distinct_release());
        for cmd in &cmds {
            let line = cmd.display();
            assert!(
                !line.contains("delete crd"),
                "CRD deletion must never appear: {line}"
            );
            assert!(
                !line.to_lowercase().contains("customresourcedefinition"),
                "{line}"
            );
        }
    }

    // #707 the up-side ownership SEAM (both branches mandatory). PRODUCTION
    // SYMBOL: a pure builder that gates the ownership stamp on the
    // pre-existence probe result:
    //
    //   fn ownership_label_commands(o: &CommonOpts, release_namespace: &str,
    //                               namespace_existed: bool) -> Vec<OpsCommand>
    //
    // It returns the `kubectl label namespace` stamp step ONLY when `up` created
    // the namespace (namespace_existed == false); an empty vec when the namespace
    // pre-existed. `up()` gates the runtime probe (mirrors the resolve_generated_secrets
    // existing/fresh split), keeping this builder pure and unit-testable.
    #[test]
    fn up_stamps_ownership_label_when_namespace_created() {
        let cmds = ownership_label_commands(&common_distinct_release(), "install-ns", false);
        assert_eq!(cmds.len(), 1);
        // namespace arg is the TARGET namespace; the created-by label VALUE is
        // the release, and the #1654 created-in label VALUE is the release's
        // INSTALL namespace (distinct from the target here, so neither value can
        // be satisfied by the other).
        assert_eq!(
            cmds[0].display(),
            "kubectl label namespace agent-ns curietech.ai/created-by=prod-release curietech.ai/created-in=install-ns --overwrite"
        );
    }

    #[test]
    fn up_does_not_stamp_ownership_label_when_namespace_preexisting() {
        let cmds = ownership_label_commands(&common_distinct_release(), "install-ns", true);
        assert!(
            cmds.is_empty(),
            "a pre-existing namespace must not be stamped (would adopt then delete pre-existing state): {:?}",
            cmds.iter().map(OpsCommand::display).collect::<Vec<_>>()
        );
    }

    /// Parse a `kubectl label namespace <ns> k=v [k=v ...] --overwrite` stamp
    /// into the label map it would actually set on that namespace.
    fn parse_stamped_labels(cmd: &OpsCommand) -> std::collections::BTreeMap<String, String> {
        let line = cmd.display();
        let mut parts = line.split_whitespace();
        assert_eq!(parts.next(), Some("kubectl"), "{line}");
        assert_eq!(parts.next(), Some("label"), "{line}");
        assert_eq!(parts.next(), Some("namespace"), "{line}");
        parts.next().expect("the target namespace arg");
        parts
            .take_while(|tok| !tok.starts_with("--"))
            .map(|tok| {
                let (k, v) = tok.split_once('=').unwrap_or_else(|| {
                    panic!("every stamped label must be key=value, got {tok:?}: {line}")
                });
                (k.to_string(), v.to_string())
            })
            .collect()
    }

    /// Parse the `-l` selector out of a `kubectl delete namespace -l <sel>
    /// --ignore-not-found` sweep into the `key=value` terms it REQUIRES (a
    /// comma-joined kubectl selector is a conjunction: all terms must match).
    fn parse_selector_terms(cmd: &OpsCommand) -> Vec<(String, String)> {
        let line = cmd.display();
        let toks: Vec<&str> = line.split_whitespace().collect();
        let at = toks.iter().position(|t| *t == "-l").expect("a -l selector");
        toks[at + 1]
            .split(',')
            .map(|term| {
                let (k, v) = term.split_once('=').unwrap_or_else(|| {
                    panic!("every selector term must be key=value, got {term:?}: {line}")
                });
                (k.to_string(), v.to_string())
            })
            .collect()
    }

    /// Whether a sweep selector's required terms are all satisfied by the labels
    /// a namespace actually carries, i.e. whether that sweep would delete it.
    fn selector_matches(
        terms: &[(String, String)],
        labels: &std::collections::BTreeMap<String, String>,
    ) -> bool {
        terms
            .iter()
            .all(|(k, v)| labels.get(k).map(String::as_str) == Some(v.as_str()))
    }

    // #1654 cross-release teardown scope. Two independent Curie installs on one
    // cluster normally BOTH take the default release name `curie` and differ
    // only in their install namespace, so a sweep selector keyed on the release
    // name alone matched the OTHER install's namespaces and deleted them
    // (observed live: `agent-sandbox-system` stamped `created-by=curie` while
    // annotated `meta.helm.sh/release-namespace: curie-other`, swept by an
    // unrelated release's `cluster down`, killing a running bot).
    //
    // Modeled BEHAVIORALLY rather than by string comparison: stamp release B's
    // namespace, build release A's sweep selector, then evaluate that selector
    // against those labels the way kubectl would. A FAILURE on the first
    // assertion means release A's teardown can delete a namespace release B
    // created -- the #1654 defect itself, which is what dropping `created-in`
    // from either the stamp or the selector reintroduces. The second assertion
    // is the anti-vacuity control: a selector that matched nothing at all would
    // also pass the first one, so release A must still sweep its OWN namespace.
    #[test]
    fn down_selector_never_matches_another_releases_namespace() {
        // Release B: name `curie`, installed into `curie-other`, and it created
        // the shared-looking `agent-sandbox-system`.
        let b = CommonOpts {
            namespace: "curie-other".into(),
            release: "curie".into(),
            dry_run: false,
        };
        let b_stamp = ownership_label_commands(
            &ns_common(&b, "agent-sandbox-system", false),
            &b.namespace,
            false,
        );
        assert_eq!(b_stamp.len(), 1);
        let b_labels = parse_stamped_labels(&b_stamp[0]);

        // Release A: the SAME release name, installed into a different
        // namespace. Its teardown sweep must not reach release B's namespace.
        let a = CommonOpts {
            namespace: "curie-a".into(),
            release: "curie".into(),
            dry_run: false,
        };
        let a_sweep = down_commands(&a);
        let a_terms = parse_selector_terms(&a_sweep[1]);
        assert!(
            !selector_matches(&a_terms, &b_labels),
            "release A's sweep selector {a_terms:?} must NOT match release B's namespace labels \
             {b_labels:?}; matching means one install's `cluster down` deletes another live \
             install's namespaces (#1654)"
        );

        // Anti-vacuity: release A must still sweep a namespace A itself created.
        let a_stamp = ownership_label_commands(
            &ns_common(&a, "agent-sandbox-system", false),
            &a.namespace,
            false,
        );
        assert_eq!(a_stamp.len(), 1);
        let a_labels = parse_stamped_labels(&a_stamp[0]);
        assert!(
            selector_matches(&a_terms, &a_labels),
            "release A's sweep selector {a_terms:?} must still match its OWN stamp {a_labels:?}; \
             a selector that matches nothing is not a fix"
        );
    }

    // #707 code-reviewer finding A2: `agent-sandbox-system` is chart-conditional
    // (created only when `agentSandbox.controller.deploy` is true), so under a
    // `--set agentSandbox.controller.deploy=false` release it is absent both
    // BEFORE and AFTER the helm install. `up()` gates the actual stamp attempt
    // on `should_stamp_ownership(existed_before, exists_after)`, re-probed AFTER
    // `cmds` executes -- this is the pure decision table that gate encodes, unit
    // tested directly since `up()` itself needs a live cluster to exercise.
    #[test]
    fn should_stamp_ownership_only_when_created_by_this_run() {
        // Created by this run: absent before, present after -> stamp.
        assert!(
            should_stamp_ownership(false, true),
            "a namespace this run created must be stamped"
        );
        // Pre-existing: present before (and therefore still present after) ->
        // never stamp, regardless of the post-install probe.
        assert!(
            !should_stamp_ownership(true, true),
            "a pre-existing namespace must never be stamped"
        );
        // controller.deploy=false: absent before AND still absent after -- the
        // chart never created it (e.g. agent-sandbox-system with the sandbox
        // controller subchart disabled). Must not stamp: `kubectl label
        // namespace <missing>` would fail and break `up`.
        assert!(
            !should_stamp_ownership(false, false),
            "a namespace the chart never created must not be stamped"
        );
    }

    // #767 fail-forward teardown (aggregation hardened by #768). PRODUCTION
    // SYMBOLS: two pure functions plus three small enums so `cluster down` runs
    // both teardown steps to completion and then decides the exit from the
    // combined result, instead of bailing the instant `helm uninstall` returns a
    // non-"not found" nonzero exit:
    //
    //   enum HelmOutcome { Removed, Absent, Failed }
    //   enum SweepOutcome { Removed, NoMatch, Failed }
    //   enum TeardownStep { HelmUninstall, NamespaceSweep }  // derive Debug, PartialEq, Eq
    //   fn outstanding_steps(helm: HelmOutcome, sweep: SweepOutcome) -> Vec<TeardownStep>
    //   fn resume_command(remaining: &[TeardownStep], o: &CommonOpts) -> String
    //
    // `outstanding_steps` is the pure decision function: Removed/Absent helm is
    // done, Failed helm leaves HelmUninstall outstanding; Removed/NoMatch sweep
    // is done, Failed sweep leaves NamespaceSweep outstanding; order is
    // HelmUninstall before NamespaceSweep (matching `down_commands` order).
    // `resume_command` maps the outstanding steps back to the matching
    // `down_commands(o)` entries. When BOTH steps are outstanding it emits the
    // HELM UNINSTALL FIRST, then the namespace sweep: helm first because Helm's
    // release metadata lives as Secrets inside the release namespace, so
    // sweeping first would destroy it and orphan the chart's cluster-scoped
    // resources. #768: the two commands are still run unconditionally (never
    // gated behind "&&", which would let a repeated helm failure block the
    // sweep), but each command's own exit status is now captured into a shell
    // variable ($?), and the resume line ends with a boolean expression that is
    // nonzero unless BOTH captured statuses were 0 -- fixing the old "; " join's
    // silent-exit-0-on-helm-failure bug without reintroducing the "&&" fail-hard
    // hazard.

    // AC: an exact resumable cleanup command is surfaced. `resume_command`
    // renders the exact copy-pasteable line for a helm-only remainder.
    #[test]
    fn resume_command_helm_only_is_the_exact_uninstall_line() {
        let o = common_distinct_release();
        let cmd = resume_command(&[TeardownStep::HelmUninstall], &o);
        assert_eq!(cmd, "helm uninstall prod-release -n agent-ns");
    }

    // AC (preserves #707): a sweep-only remainder renders the exact
    // ownership-label-scoped delete, never an unscoped `delete namespace`.
    #[test]
    fn resume_command_sweep_only_is_label_scoped() {
        let o = common_distinct_release();
        let cmd = resume_command(&[TeardownStep::NamespaceSweep], &o);
        assert_eq!(
            cmd,
            "kubectl delete namespace -l curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns --ignore-not-found"
        );
        // #707 ownership-scope invariant: the sweep stays keyed on THIS release's
        // label and is never widened to an unconditional namespace delete.
        assert!(
            cmd.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "{cmd}"
        );
        assert!(
            !cmd.contains("delete namespace prod-release"),
            "must never be an unscoped delete: {cmd}"
        );
        // ignore-not-found preserved so the resume stays re-runnable.
        assert!(cmd.contains("--ignore-not-found"), "{cmd}");
    }

    // AC + review (#768 aggregation): both steps outstanding -> the HELM
    // UNINSTALL FIRST, then the namespace sweep, both run unconditionally, with
    // each captured exit status aggregated into a nonzero-unless-both-succeeded
    // trailing expression. Ordering rationale: Helm stores its release metadata
    // as Secrets inside the release namespace, and the chart owns cluster-scoped
    // resources (ClusterRole/ClusterRoleBinding); sweeping the namespace first
    // would destroy that metadata, the subsequent helm uninstall would report
    // "not found", and those cluster-scoped resources would be orphaned.
    // Aggregation rationale: a plain "; " join runs both commands unconditionally
    // but only returns the LAST command's exit status, so a helm failure
    // followed by a successful sweep silently reads as exit 0. A " && " join
    // would fix the status but let a repeated helm failure short-circuit and
    // block the compute-stopping sweep, so it is explicitly rejected. The
    // wrapper here keeps both commands unconditional and only combines their
    // CAPTURED statuses afterward.
    #[test]
    fn resume_command_both_runs_both_steps_helm_first() {
        let o = common_distinct_release();
        let cmd = resume_command(
            &[TeardownStep::HelmUninstall, TeardownStep::NamespaceSweep],
            &o,
        );
        let helm_cmd = "helm uninstall prod-release -n agent-ns";
        let sweep_cmd =
            "kubectl delete namespace -l curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns --ignore-not-found";
        assert_eq!(
            cmd,
            format!(
                "{helm_cmd}; s1=$?; {sweep_cmd}; s2=$?; [ \"$s1\" -eq 0 ] && [ \"$s2\" -eq 0 ]"
            )
        );
        // The helm uninstall must appear BEFORE the sweep, so Helm's release
        // metadata (in the release namespace) survives long enough for helm to
        // remove the chart-owned cluster-scoped resources.
        let helm_at = cmd.find("helm uninstall").expect("helm present");
        let sweep_at = cmd.find("kubectl delete namespace").expect("sweep present");
        assert!(
            helm_at < sweep_at,
            "helm uninstall must run before the sweep: {cmd}"
        );
        // The two commands themselves are joined by a plain "; " immediately
        // followed by capturing their own exit status -- NOT " && " -- so a
        // repeated helm failure can never short-circuit and block the sweep.
        assert!(
            cmd.starts_with(&format!("{helm_cmd}; s1=$?; {sweep_cmd}; s2=$?;")),
            "helm and the sweep must both run unconditionally, not gated behind &&: {cmd}"
        );
        // The trailing "&&" combines two `[ ... -eq 0 ]` tests over the ALREADY
        // captured statuses; it does not gate whether the sweep runs, only
        // whether the whole line's own exit status reports both as successful.
        assert!(
            cmd.ends_with("[ \"$s1\" -eq 0 ] && [ \"$s2\" -eq 0 ]"),
            "the resume line's own exit status must aggregate both captured statuses: {cmd}"
        );
        // The sweep half stays label-scoped even when combined with helm.
        assert!(
            cmd.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "{cmd}"
        );
        assert!(cmd.contains("--ignore-not-found"), "{cmd}");
    }

    // Nothing remaining -> nothing to resume -> the empty string.
    #[test]
    fn resume_command_empty_remainder_is_empty_string() {
        let o = common_distinct_release();
        assert_eq!(resume_command(&[], &o), "");
    }

    // AC: helm-uninstall failure no longer aborts before the sweep. The decision
    // table proves the sweep runs (and is scored) even when helm failed, and
    // that a swept-but-helm-stale run surfaces only the helm step as outstanding.
    #[test]
    fn outstanding_steps_decision_table() {
        use TeardownStep::*;

        // Happy path: helm removed, sweep clean -> nothing outstanding.
        assert_eq!(
            outstanding_steps(HelmOutcome::Removed, SweepOutcome::Removed),
            Vec::<TeardownStep>::new()
        );
        // Already-absent release, sweep clean -> still nothing outstanding.
        assert_eq!(
            outstanding_steps(HelmOutcome::Absent, SweepOutcome::Removed),
            Vec::<TeardownStep>::new()
        );
        // Fail-forward win: helm failed but the sweep removed the namespaces
        // (compute stopped); only the stale helm release record remains.
        assert_eq!(
            outstanding_steps(HelmOutcome::Failed, SweepOutcome::Removed),
            vec![HelmUninstall]
        );
        // Nothing could be removed; the API server is still unreachable.
        assert_eq!(
            outstanding_steps(HelmOutcome::Failed, SweepOutcome::Failed),
            vec![HelmUninstall, NamespaceSweep]
        );
        // Helm removed but the sweep failed -> only the sweep is outstanding.
        assert_eq!(
            outstanding_steps(HelmOutcome::Removed, SweepOutcome::Failed),
            vec![NamespaceSweep]
        );
        // #768: a zero-match sweep (pre-existing namespace, never labeled by
        // #707) is a completed step, same as an actual removal -- there is
        // nothing left for THIS step to do, so it must not be outstanding.
        assert_eq!(
            outstanding_steps(HelmOutcome::Removed, SweepOutcome::NoMatch),
            Vec::<TeardownStep>::new()
        );
        // #768: helm failed and the sweep matched nothing -> only the helm
        // record is outstanding; the sweep is done (there was nothing to sweep),
        // but critically it did NOT stop any compute, unlike the Removed case
        // above.
        assert_eq!(
            outstanding_steps(HelmOutcome::Failed, SweepOutcome::NoMatch),
            vec![HelmUninstall]
        );
    }

    // #767 fail-forward HARDENING. PRODUCTION SYMBOLS: a connectivity
    // classifier and the pure teardown-result decision that `down()` calls
    // AFTER running both teardown steps and capturing their outcomes plus
    // stderr:
    //
    //   fn is_connectivity_failure(stderr: &str) -> bool
    //   fn teardown_result(
    //       helm: HelmOutcome,
    //       sweep: SweepOutcome,
    //       helm_err: &str,
    //       sweep_err: &str,
    //       o: &CommonOpts,
    //   ) -> anyhow::Result<ClusterDownOutput>
    //
    // `is_connectivity_failure` lower-cases stderr and matches only concrete
    // network signatures (connection refused, tls handshake, no route to
    // host, i/o timeout, network is unreachable, could not connect, dial tcp,
    // connection reset, context deadline exceeded, and -- since a kubectl call
    // site started classifying with it in #1351 -- client-go's own "connection
    // to the server" refusal wording). Bare "unreachable" and
    // "timeout" are deliberately excluded: Helm wraps permanent
    // auth/exec-plugin/kubeconfig errors as `Kubernetes cluster unreachable:
    // ...`, so that generic prefix alone is not a reliable transient signal.
    // Permanent errors (forbidden, rbac, unauthorized, invalid, not
    // authorized) stay false so they are never mislabeled retryable.
    // `teardown_result` is the pure decision: an empty remainder returns
    // Ok(Down{release_was_absent}); otherwise it builds the resume command (via
    // resume_command), composes it INTO the message so the human Display carries
    // it (P1: `main` renders Display and drops the fix), attaches it as the fix for
    // `--json`, and tags the exit class Transient IFF an outstanding failed step's
    // stderr is a connectivity failure, else a plain Failure (P2).

    // P2: recognized connectivity stderrs are transient (retryable).
    #[test]
    fn is_connectivity_failure_true_for_unreachable_markers() {
        assert!(is_connectivity_failure(
            "Kubernetes cluster unreachable: Get \"https://h:6443/version\": net/http: TLS handshake timeout"
        ));
        assert!(is_connectivity_failure(
            "dial tcp 1.2.3.4:6443: connect: connection refused"
        ));
        assert!(is_connectivity_failure("i/o timeout"));
        assert!(is_connectivity_failure("no route to host"));
    }

    // #1351: kubectl's own refusal wording, verbatim as kubectl v1.36.2 wrote it
    // to stderr against an unreachable apiserver during this ticket's live
    // reproduction. client-go separates the words, so the "connection refused"
    // marker above never appears in it and every unreadable-cluster kubectl read
    // classified as a permanent Failure until the "connection to the server"
    // marker was added.
    #[test]
    fn is_connectivity_failure_true_for_kubectls_own_refusal_wording() {
        assert!(is_connectivity_failure(
            "The connection to the server localhost:8080 was refused - did you specify the right host or port?"
        ));
    }

    // P2: permanent failures (RBAC, authz, invalid) are NOT connectivity, so they
    // must classify as a plain Failure, never a retryable transient.
    #[test]
    fn is_connectivity_failure_false_for_permanent_errors() {
        assert!(!is_connectivity_failure(
            "Error: query: failed to query with labels: namespaces is forbidden: User cannot list resource"
        ));
        assert!(!is_connectivity_failure("Error: rbac: access denied"));
        assert!(!is_connectivity_failure("error: You must be logged in"));
        assert!(!is_connectivity_failure(""));
    }

    // Codex P2: Helm wraps permanent errors (auth, exec-plugin, kubeconfig) as
    // "Kubernetes cluster unreachable: ...", so the bare "unreachable"/"timeout"
    // prefix is NOT a reliable transient signal. Only concrete network signatures
    // (connection refused, tls handshake, no route to host, i/o timeout, dial tcp,
    // network is unreachable, connection reset, context deadline exceeded) count;
    // a permanent error wearing Helm's generic prefix must classify FALSE.
    #[test]
    fn is_connectivity_failure_false_for_helm_wrapped_permanent_errors() {
        // Auth exec-plugin failure wrapped by Helm's generic prefix.
        assert!(!is_connectivity_failure(
            "Error: Kubernetes cluster unreachable: Get \"https://h:6443/version\": getting credentials: exec plugin: exec: \"gke-gcloud-auth-plugin\": executable file not found in $PATH"
        ));
        // RBAC/authz failure wrapped by Helm's generic prefix.
        assert!(!is_connectivity_failure(
            "Error: Kubernetes cluster unreachable: namespaces is forbidden: User cannot list resource \"namespaces\""
        ));
        // Bare unreachable with no concrete network signature at all.
        assert!(!is_connectivity_failure(
            "Error: Kubernetes cluster unreachable"
        ));
    }

    // Review: a host that does not RESOLVE is a deterministic configuration error
    // (a bad kubeconfig hostname), not a transient network blip. Retrying cannot
    // fix it, so it must classify FALSE even though the stderr also carries the
    // "dial tcp" marker; otherwise automation retries forever instead of fixing
    // the context.
    #[test]
    fn is_connectivity_failure_false_for_permanent_host_resolution_errors() {
        assert!(!is_connectivity_failure(
            "Error: Kubernetes cluster unreachable: Get \"https://bad-host:6443/version\": dial tcp: lookup bad-host: no such host"
        ));
        assert!(!is_connectivity_failure(
            "dial tcp: lookup bad-host on 127.0.0.53:53: no such host"
        ));
    }

    // #1230: a CLI that rejects its invocation prints the diagnosis FIRST and
    // its usage block last, so "last non-empty line" surfaces the boilerplate
    // and drops the only actionable line. This is the transcript that provoked
    // the issue -- `curie local up` reported "Run 'docker --help' for more
    // information" while the real cause sat four lines above it.
    #[test]
    fn failure_reason_skips_a_trailing_usage_block() {
        let stderr = "unknown flag: --profile\n\
                      \n\
                      Usage:  docker [OPTIONS] COMMAND [ARG...]\n\
                      \n\
                      Run 'docker --help' for more information\n";
        assert_eq!(failure_reason(stderr), "unknown flag: --profile");
    }

    // The same shape without a usage block: some rejections print only the
    // diagnosis plus a bare help pointer.
    #[test]
    fn failure_reason_skips_a_bare_help_pointer() {
        let stderr = "docker: 'compos' is not a docker command.\nSee 'docker --help'\n";
        assert_eq!(
            failure_reason(stderr),
            "docker: 'compos' is not a docker command."
        );
        let kubectl = "error: unknown flag: --foo\nSee 'kubectl get --help' for usage.\n";
        assert_eq!(failure_reason(kubectl), "error: unknown flag: --foo");
    }

    // REGRESSION GUARD, and the reason the naive inverse ("take the first
    // line") is wrong: helm prints warnings BEFORE its `Error:` line, so the
    // last line is the right answer there and must stay the right answer.
    #[test]
    fn failure_reason_keeps_the_last_line_when_a_warning_precedes_the_error() {
        let stderr = "WARNING: Kubernetes configuration file is group-readable\n\
                      Error: INSTALLATION FAILED: cannot re-use a name that is still in use\n";
        assert_eq!(
            failure_reason(stderr),
            "Error: INSTALLATION FAILED: cannot re-use a name that is still in use"
        );
    }

    // The unchanged base cases: one line is that line, nothing is the default.
    #[test]
    fn failure_reason_is_unchanged_for_single_line_and_empty_stderr() {
        assert_eq!(
            failure_reason("Error: release: not found\n"),
            "Error: release: not found"
        );
        assert_eq!(failure_reason(""), "command failed");
        assert_eq!(failure_reason("   \n\n  \n"), "command failed");
    }

    // The safety property: stripping must never blank the reason. A stderr that
    // is ONLY a usage block has no diagnosis to recover, so the result falls
    // back to today's last-line answer rather than to an empty string.
    #[test]
    fn failure_reason_falls_back_rather_than_blanking_on_a_pure_usage_block() {
        let stderr = "Usage:  docker [OPTIONS] COMMAND [ARG...]\n\
                      \n\
                      Run 'docker --help' for more information\n";
        assert_eq!(
            failure_reason(stderr),
            "Run 'docker --help' for more information"
        );
    }

    // Both teardown steps completed: success, release present.
    #[test]
    fn teardown_result_all_removed_is_success() {
        let o = common_distinct_release();
        let res = teardown_result(HelmOutcome::Removed, SweepOutcome::Removed, "", "", &o)
            .expect("a complete teardown is Ok");
        assert!(matches!(
            res,
            ClusterDownOutput::Down {
                release_was_absent: false
            }
        ));
    }

    // Already-absent release, sweep clean: success, release_was_absent true.
    #[test]
    fn teardown_result_absent_release_is_success_absent() {
        let o = common_distinct_release();
        let res = teardown_result(HelmOutcome::Absent, SweepOutcome::Removed, "", "", &o)
            .expect("an already-absent release still completes");
        assert!(matches!(
            res,
            ClusterDownOutput::Down {
                release_was_absent: true
            }
        ));
    }

    // P1 + P2: both steps failed on an unreachable API server. Transient (exit 3),
    // and the label-scoped resume command rides in BOTH the human Display message
    // (P1: `main` renders Display, so a no-json operator must still see it) and the
    // fix (for `--json`).
    #[test]
    fn teardown_result_connectivity_both_failed_is_transient_with_resume_in_message_and_fix() {
        let o = common_distinct_release();
        let helm_err =
            "Kubernetes cluster unreachable: Get \"https://h:6443/version\": net/http: TLS handshake timeout";
        let sweep_err = "Kubernetes cluster unreachable: connection refused";
        let err = teardown_result(
            HelmOutcome::Failed,
            SweepOutcome::Failed,
            helm_err,
            sweep_err,
            &o,
        )
        .expect_err("an incomplete teardown is an error");

        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Transient);
        assert_eq!(class.code(), 3);

        // P1: the resume command is IN the human Display message, not only the fix.
        let shown = err.to_string();
        assert!(
            shown.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "the human message must carry the label-scoped resume command: {shown}"
        );

        // --json path: the fix carries the same label-scoped resume command.
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert!(
            fix.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "fix must carry the label-scoped resume command: {fix}"
        );
    }

    // Fail-forward win: helm failed (unreachable) but the sweep removed the
    // namespaces, so only the stale helm release record remains. Transient, the
    // message distinguishes the swept case (stable substring "swept"), and the
    // resume command is the helm-only line in both message and fix.
    #[test]
    fn teardown_result_connectivity_helm_only_failed_surfaces_swept_and_helm_resume() {
        let o = common_distinct_release();
        let helm_err = "Kubernetes cluster unreachable: net/http: TLS handshake timeout";
        let err = teardown_result(HelmOutcome::Failed, SweepOutcome::Removed, helm_err, "", &o)
            .expect_err("a stale helm record is still an incomplete teardown");

        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Transient);

        let shown = err.to_string();
        // Distinguishes the swept-but-helm-stale case.
        assert!(shown.contains("swept"), "{shown}");
        // Only the helm step is outstanding, so the helm-only resume line rides in
        // the message (P1) ...
        assert!(
            shown.contains("helm uninstall prod-release -n agent-ns"),
            "the message must carry the helm-only resume line: {shown}"
        );
        // ... and must not drag in the (completed) sweep command.
        assert!(
            !shown.contains("delete namespace"),
            "a swept run must not list the sweep as outstanding: {shown}"
        );
        // ... and the fix is exactly the helm-only line for `--json`.
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert_eq!(fix, "helm uninstall prod-release -n agent-ns");
    }

    // #768 core anti-regression: a zero-match sweep is NOT the same as an
    // actual removal. When Curie was installed into a pre-existing namespace
    // (#707 never labels it), the label-scoped sweep exits 0 with nothing
    // matched. Before #768 this was mapped to the same `SweepOutcome::Removed`
    // as a real deletion, so a failed helm uninstall paired with a zero-match
    // sweep produced the exact same "the run-created namespaces were swept"
    // message as a real removal, even though the pre-existing namespace's
    // workloads (and the failed release's compute) may still be running. This
    // test locks the fix: `SweepOutcome::NoMatch` must NEVER be worded as
    // "swept", and the message must say plainly that no compute was stopped.
    #[test]
    fn teardown_result_zero_match_sweep_never_claims_compute_was_removed() {
        let o = common_distinct_release();
        let helm_err = "Kubernetes cluster unreachable: net/http: TLS handshake timeout";
        let err = teardown_result(HelmOutcome::Failed, SweepOutcome::NoMatch, helm_err, "", &o)
            .expect_err("a stale helm record is still an incomplete teardown");

        let shown = err.to_string();
        // The core anti-regression: must NOT claim the run-created namespaces
        // were swept, since nothing actually matched the selector.
        assert!(
            !shown.contains("were swept"),
            "a zero-match sweep must never be worded as an actual removal: {shown}"
        );
        // Must plainly say no compute was stopped, so an operator does not
        // mistakenly believe the failed release's workloads are gone.
        assert!(
            shown.to_lowercase().contains("no compute was stopped")
                || shown.to_lowercase().contains("no run-created namespaces matched"),
            "the message must not imply compute was removed when the sweep matched nothing: {shown}"
        );
        // The sweep itself is done (nothing to sweep), so only the helm step is
        // outstanding -- the resume command is the helm-only line, exactly like
        // the real-removal ("swept") case, in both the message and the fix.
        assert!(
            shown.contains("helm uninstall prod-release -n agent-ns"),
            "the message must carry the helm-only resume line: {shown}"
        );
        assert!(
            !shown.contains("delete namespace"),
            "a zero-match sweep has nothing outstanding, so the sweep must not be listed as a resume step: {shown}"
        );
        let (_, fix) = crate::exit::classify(&err);
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert_eq!(fix, "helm uninstall prod-release -n agent-ns");
    }

    // #768: a zero-match sweep still counts as a COMPLETED step when helm itself
    // succeeds -- the pre-existing namespace was correctly left untouched (#707),
    // so `cluster down` overall succeeds exactly as it would if the release had
    // created and then swept its own namespace.
    #[test]
    fn teardown_result_zero_match_sweep_with_helm_removed_is_still_success() {
        let o = common_distinct_release();
        let res = teardown_result(HelmOutcome::Removed, SweepOutcome::NoMatch, "", "", &o)
            .expect("a zero-match sweep alongside a clean helm uninstall is a complete teardown");
        assert!(matches!(
            res,
            ClusterDownOutput::Down {
                release_was_absent: false
            }
        ));
    }

    // P2: permanent failures (RBAC, authz) are NOT retryable. Both steps failed
    // with a forbidden error, so the class is a plain Failure (exit 1), NOT a
    // transient, while still failing forward: the resume command rides in message
    // and fix.
    #[test]
    fn teardown_result_permanent_failure_is_plain_failure_not_transient() {
        let o = common_distinct_release();
        let forbidden =
            "Error: query: failed to query with labels: namespaces is forbidden: User cannot list resource \"namespaces\"";
        let err = teardown_result(
            HelmOutcome::Failed,
            SweepOutcome::Failed,
            forbidden,
            forbidden,
            &o,
        )
        .expect_err("an incomplete teardown is an error");

        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(
            class,
            crate::exit::ExitClass::Failure,
            "an RBAC/permanent failure must classify as Failure, not Transient"
        );
        assert_eq!(class.code(), 1);
        assert_ne!(class, crate::exit::ExitClass::Transient);

        // Fail-forward still surfaces the resume command in message and fix.
        let shown = err.to_string();
        assert!(
            shown.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "even a permanent failure surfaces the label-scoped resume command: {shown}"
        );
        // Codex P2: the permanent-failure message must surface the underlying
        // reason drawn from the failed step's stderr, not a generic line that
        // drops it to --debug plumbing. The operator must see WHY teardown failed.
        assert!(
            shown.contains("forbidden"),
            "the permanent-failure message must surface the underlying stderr reason: {shown}"
        );
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert!(
            fix.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
            "{fix}"
        );
    }

    // Codex P2: in a MIXED failure the surfaced reason must be the failure that
    // DETERMINES the exit class (the permanent, actionable problem the operator
    // must fix), not merely the first failed step. Here helm fails transiently
    // (connectivity) but the sweep then fails permanently (RBAC): the permanent
    // sweep failure blocks retry, so the class is Failure (exit 1), and the
    // message must name the permanent sweep reason (`forbidden`), not hide it
    // behind the transient helm connectivity reason.
    #[test]
    fn teardown_result_mixed_failure_surfaces_the_determining_permanent_reason() {
        let o = common_distinct_release();
        let helm_err =
            "Error: Kubernetes cluster unreachable: Get \"https://h:6443/version\": dial tcp 10.0.0.1:6443: connect: connection refused";
        let sweep_err =
            "Error: namespaces is forbidden: User \"sa\" cannot delete resource \"namespaces\" in API group";
        let err = teardown_result(
            HelmOutcome::Failed,
            SweepOutcome::Failed,
            helm_err,
            sweep_err,
            &o,
        )
        .expect_err("an incomplete teardown is an error");

        let (class, _fix) = crate::exit::classify(&err);
        // The permanent sweep failure blocks retry: Failure (exit 1), not Transient.
        assert_eq!(
            class,
            crate::exit::ExitClass::Failure,
            "the permanent sweep failure must classify as Failure, not Transient"
        );
        assert_eq!(class.code(), 1);
        assert_ne!(class, crate::exit::ExitClass::Transient);

        // The message must surface the DETERMINING permanent reason (`forbidden`),
        // not just the transient helm connectivity reason. It is acceptable if the
        // message also includes the helm reason, but `forbidden` must be present.
        let shown = err.to_string();
        assert!(
            shown.contains("forbidden"),
            "the determining permanent sweep reason must be surfaced: {shown}"
        );
    }

    // #1251: `is_usage_header` slices `line[..6]` guarded only by a BYTE length
    // check, but the char-boundary requirement is not a byte count -- a
    // diagnosis line where the 7th byte lands inside a multi-byte character
    // panics with "byte index 6 is not a char boundary" instead of returning a
    // reason. Reverting the fix turns this RED.
    #[test]
    fn failure_reason_does_not_panic_on_multibyte_diagnosis_line() {
        let stderr = "abcdeé: dépôt manquant\n";
        assert_eq!(failure_reason(stderr), "abcdeé: dépôt manquant");
    }

    // #1251, the exact production path: `run_capture` decodes subprocess
    // stderr with `String::from_utf8_lossy`, so an invalid byte from a real
    // CLI becomes a U+FFFD replacement character (3 bytes) rather than a
    // valid multi-byte character typed by hand. Built via the same lossy
    // conversion so the test documents the real path instead of hardcoding
    // the glyph. Reverting the fix turns this RED (this is the transcript
    // from the issue: `printf "abcde\xe9fghij\n"` on a fake docker stderr).
    #[test]
    fn failure_reason_does_not_panic_on_lossy_replacement_character() {
        let stderr = String::from_utf8_lossy(b"abcde\xe9fghij\n").into_owned();
        let expected = stderr.trim().to_string();
        assert_eq!(failure_reason(&stderr), expected);
    }

    // Guard that the fix does not also break detection: a genuine `Usage:`
    // header, followed by multi-byte content the header wraps, must still be
    // recognized and cut so the diagnosis above it is what gets returned.
    #[test]
    fn failure_reason_still_cuts_a_multibyte_usage_header() {
        let stderr = "erreur: dépôt manquant\n\
                      Usage: café [OPTIONS] COMMAND\n\
                      Run 'café --help' for more information\n";
        assert_eq!(failure_reason(stderr), "erreur: dépôt manquant");
    }

    // Guard that the fix does not also break detection: a line shorter than
    // six bytes is excluded by the length check outright, and a line that is
    // exactly six bytes but is not the `usage:` prefix must not be
    // misclassified as a usage header either. The candidate line sits above a
    // real diagnosis line so a misclassification is observable through the
    // cut: an always-true predicate would treat the candidate as the usage
    // header, truncate the diagnosis after it, and return the candidate line
    // instead of the diagnosis.
    #[test]
    fn failure_reason_does_not_misclassify_short_or_nonmatching_multibyte_lines() {
        // "café!" is exactly six bytes but is not the usage prefix, so it must not
        // truncate the diagnosis that follows it.
        assert_eq!(
            failure_reason("café!\nerreur: dépôt manquant\n"),
            "erreur: dépôt manquant"
        );
        assert_eq!(
            failure_reason("café\nerreur: dépôt manquant\n"),
            "erreur: dépôt manquant"
        );
    }

    // #1251, same defect class via `teardown_result`: `helm_err`/`sweep_err`
    // come from `run_capture`'s lossy stderr too, so a failed helm step whose
    // stderr trips the same byte-boundary case must return a result carrying
    // a reason rather than panicking through `teardown_result` ->
    // `failure_reason`. Reverting the fix turns this RED.
    #[test]
    fn teardown_result_helm_failure_with_multibyte_stderr_does_not_panic() {
        let o = common_distinct_release();
        let helm_err = String::from_utf8_lossy(b"abcde\xe9fghij\n").into_owned();
        let err = teardown_result(
            HelmOutcome::Failed,
            SweepOutcome::Removed,
            &helm_err,
            "",
            &o,
        )
        .expect_err("a failed helm step is an incomplete teardown");
        let shown = err.to_string();
        assert!(
            shown.contains(helm_err.trim()),
            "the message must surface the helm stderr reason: {shown}"
        );
    }

    #[test]
    fn status_lists_the_readonly_commands() {
        let cmds = status_commands(&common(), &fullname());
        let lines: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
        assert_eq!(lines[0], "helm status curie -n curie");
        assert_eq!(lines[1], "kubectl get pods -n curie -o json");
        assert_eq!(lines[2], "kubectl get svc curie-ui -n curie -o json");
        assert_eq!(
            lines[3],
            "kubectl get svc curie-langfuse-web -n curie -o json"
        );
        assert!(
            lines[4].starts_with("kubectl config view --minify -o "),
            "{}",
            lines[4]
        );
    }

    #[test]
    fn mask_secret_uses_shared_long_and_short_contract() {
        assert_eq!(mask_secret("xoxb-abcdefghijk"), "xoxb-abc***");
        for value in ["", "a", "short", "12345678"] {
            assert_eq!(
                mask_secret(value),
                "***",
                "values of eight characters or fewer must reveal no characters: {value:?}"
            );
        }
    }

    #[test]
    fn shell_quote_quotes_only_special_tokens() {
        assert_eq!(
            shell_quote("ui.service.type=NodePort"),
            "ui.service.type=NodePort"
        );
        assert_eq!(shell_quote("a[0]=b"), "'a[0]=b'");
        assert_eq!(shell_quote(""), "''");
    }

    #[test]
    fn display_masks_secret_env_values() {
        let line = OpsCommand::new("docker", vec![plain("ps")])
            .with_secret_env(vec![(
                "SLACK_BOT_TOKEN".into(),
                "xoxb-1-secretsecret".into(),
            )])
            .display();
        assert!(line.contains("SLACK_BOT_TOKEN=xoxb-1-s***"), "{line}");
        assert!(!line.contains("secretsecret"), "secret leaked: {line}");
    }

    #[test]
    fn host_from_server_url_strips_scheme_and_port() {
        assert_eq!(
            host_from_server_url("https://10.1.2.3:6443").as_deref(),
            Some("10.1.2.3")
        );
        assert_eq!(
            host_from_server_url("https://k3s.local:6443").as_deref(),
            Some("k3s.local")
        );
        assert_eq!(
            host_from_server_url("https://host").as_deref(),
            Some("host")
        );
        assert_eq!(host_from_server_url(""), None);
    }

    #[test]
    fn host_from_server_url_parses_bracketed_ipv6() {
        assert_eq!(
            host_from_server_url("https://[::1]:6443").as_deref(),
            Some("::1")
        );
        assert_eq!(
            host_from_server_url("https://[2001:db8::1]:8443").as_deref(),
            Some("2001:db8::1")
        );
        assert_eq!(
            host_from_server_url("https://[::1]").as_deref(),
            Some("::1")
        );
    }

    #[test]
    fn parse_service_reads_type_and_ports() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;
        assert_eq!(
            parse_service(json),
            Some(("NodePort".into(), Some(31234), 80))
        );
        let cluster = r#"{"spec":{"type":"ClusterIP","ports":[{"port":3000}]}}"#;
        assert_eq!(
            parse_service(cluster),
            Some(("ClusterIP".into(), None, 3000))
        );
    }

    #[test]
    fn node_internal_ip_finds_first_internal_address() {
        let json = r#"{"items":[{"status":{"addresses":[
            {"type":"Hostname","address":"node1"},
            {"type":"InternalIP","address":"192.168.1.5"}
        ]}}]}"#;
        assert_eq!(node_internal_ip(json).as_deref(), Some("192.168.1.5"));
    }

    #[test]
    fn pod_summary_does_not_panic_on_empty() {
        // No items: empty items array.
        let items: Vec<serde_json::Value> = Vec::new();
        let _ = collect_pod_summary(&items);
    }

    #[test]
    fn pod_summary_excludes_completed_and_terminating() {
        let json = r#"[
            {"metadata":{"name":"api0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"api1"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"worker0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"worker1"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"dispatcher0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"ui0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"postgres0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"valkey0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"langfuse0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"otel0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"runnerold","deletionTimestamp":"2024-01-01T00:00:00Z"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"preflight0"},"status":{"phase":"Succeeded","reason":"Completed","containerStatuses":[{"ready":false,"restartCount":0}]}},
            {"metadata":{"name":"preflight1"},"status":{"phase":"Succeeded","reason":"Completed","containerStatuses":[{"ready":false,"restartCount":0}]}},
            {"metadata":{"name":"job0"},"status":{"phase":"Succeeded","containerStatuses":[{"ready":false,"restartCount":0}]}}
        ]"#;

        let items: Vec<serde_json::Value> = serde_json::from_str(json).unwrap();
        let (_, ready, total, unhealthy) = collect_pod_summary(&items);
        assert_eq!((ready, total, unhealthy), (10, 10, vec![]));
    }

    #[test]
    fn pod_summary_flags_genuinely_unhealthy_steady_state_pod() {
        let json = r#"[
            {"metadata":{"name":"api0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"worker0"},"status":{"phase":"Running","containerStatuses":[{"ready":true,"restartCount":0}]}},
            {"metadata":{"name":"dispatcher0"},"status":{"phase":"Pending","containerStatuses":[]}}
        ]"#;

        let items: Vec<serde_json::Value> = serde_json::from_str(json).unwrap();
        let (_, ready, total, unhealthy) = collect_pod_summary(&items);
        assert_eq!(
            (ready, total, unhealthy),
            (2, 3, vec!["dispatcher0".to_string()])
        );
    }

    // -- #196: generate / reuse the required chart secrets ------------------

    #[test]
    fn random_hex_is_the_right_length_hex_and_unpredictable() {
        let a = random_hex(24).unwrap();
        let b = random_hex(24).unwrap();
        assert_eq!(a.len(), 48, "24 bytes -> 48 hex chars");
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()), "{a}");
        assert_ne!(a, b, "two draws must differ");
        // The langfuse ENCRYPTION_KEY contract: exactly 64 hex chars.
        assert_eq!(random_hex(32).unwrap().len(), 64);
    }

    #[test]
    fn operator_set_keys_parses_repeated_and_comma_joined() {
        let keys = operator_set_keys(&[
            "api.apiKey=x".into(),
            "postgres.auth.password=y,valkey.password=z".into(),
        ]);
        assert!(keys.contains("api.apiKey"));
        assert!(keys.contains("postgres.auth.password"));
        assert!(keys.contains("valkey.password"));
        assert!(!keys.contains("api.githubWebhookSecret"));
    }

    #[test]
    fn lookup_dotted_navigates_nested_values() {
        let v: serde_json::Value =
            serde_json::from_str(r#"{"postgres":{"auth":{"password":"secretpw"}}}"#).unwrap();
        assert_eq!(
            lookup_dotted(&v, "postgres.auth.password").as_deref(),
            Some("secretpw")
        );
        assert_eq!(lookup_dotted(&v, "postgres.auth.missing"), None);
        assert_eq!(lookup_dotted(&serde_json::Value::Null, "api.apiKey"), None);
    }

    #[test]
    fn resolve_existing_secret_ref_prefers_operator_secret_and_custom_key() {
        // #1759 follow-up: a release with a BYO existingSecret configured must
        // resolve to that Secret and its (possibly overridden) key name, not
        // the chart's own Secret -- this is the read-path half of the bug
        // `curie seal`/`curie cluster message` hit before this fix, where they
        // read straight from the chart's own Secret and never checked for a
        // BYO override.
        let v: serde_json::Value = serde_json::from_str(
            r#"{"sealing":{"privateKeyExistingSecret":"my-sealing-secret","privateKeyExistingSecretKey":"customKey"}}"#,
        )
        .unwrap();
        assert_eq!(
            resolve_existing_secret_ref(
                Some(&v),
                "sealing.privateKeyExistingSecret",
                "sealing.privateKeyExistingSecretKey",
                "sealingPrivateKey"
            ),
            Some(("my-sealing-secret".to_string(), "customKey".to_string()))
        );
    }

    #[test]
    fn resolve_existing_secret_ref_defaults_key_when_key_field_absent() {
        // The chart's own default for *ExistingSecretKey is the published key
        // name (e.g. `botTokenExistingSecretKey: slackBotToken`), so a release
        // that set only the Secret name and left the key at its default must
        // still resolve to the published key, not an empty/missing key.
        let v: serde_json::Value = serde_json::from_str(
            r#"{"dispatcher":{"slack":{"botTokenExistingSecret":"my-slack-secret"}}}"#,
        )
        .unwrap();
        assert_eq!(
            resolve_existing_secret_ref(
                Some(&v),
                "dispatcher.slack.botTokenExistingSecret",
                "dispatcher.slack.botTokenExistingSecretKey",
                "slackBotToken"
            ),
            Some(("my-slack-secret".to_string(), "slackBotToken".to_string()))
        );
    }

    #[test]
    fn resolve_existing_secret_ref_none_when_not_configured() {
        // Negative control: no release at all, and a release that only set the
        // plain value (no BYO escape) -- both must fall through to None so the
        // caller reads the chart's own Secret, exactly as it did before #1759.
        assert_eq!(
            resolve_existing_secret_ref(
                None,
                "sealing.privateKeyExistingSecret",
                "sealing.privateKeyExistingSecretKey",
                "sealingPrivateKey"
            ),
            None
        );
        let v: serde_json::Value =
            serde_json::from_str(r#"{"sealing":{"privateKey":"plain-value"}}"#).unwrap();
        assert_eq!(
            resolve_existing_secret_ref(
                Some(&v),
                "sealing.privateKeyExistingSecret",
                "sealing.privateKeyExistingSecretKey",
                "sealingPrivateKey"
            ),
            None
        );
    }

    #[test]
    fn fresh_install_generates_every_required_secret() {
        // No existing release -> a strong random for each required key.
        let secrets = resolve_generated_secrets(None, &[]).unwrap();
        assert_eq!(secrets.len(), REQUIRED_SECRETS.len());
        for (key, _) in REQUIRED_SECRETS {
            let (_, value) = secrets
                .iter()
                .find(|(k, _)| k == key)
                .unwrap_or_else(|| panic!("missing generated secret for {key}"));
            assert!(!value.is_empty(), "{key} generated empty");
            assert!(
                value.chars().all(|c| c.is_ascii_hexdigit()),
                "{key}={value}"
            );
        }
        // encryptionKey keeps its exact 64-hex-char contract.
        let enc = secrets
            .iter()
            .find(|(k, _)| k == "langfuse.encryptionKey")
            .unwrap();
        assert_eq!(enc.1.len(), 64);
    }

    #[test]
    fn fresh_install_secrets_are_unpredictable_per_release() {
        let a = resolve_generated_secrets(None, &[]).unwrap();
        let b = resolve_generated_secrets(None, &[]).unwrap();
        assert_ne!(a, b, "each release must get its own randoms");
    }

    #[test]
    fn operator_set_secret_is_left_to_the_operator() {
        // A secret the operator pinned via --set is not generated over.
        let secrets = resolve_generated_secrets(None, &["api.apiKey=my-own-key".into()]).unwrap();
        assert!(
            !secrets.iter().any(|(k, _)| k == "api.apiKey"),
            "operator --set must win: {secrets:?}"
        );
        // Every other required secret is still generated.
        assert_eq!(secrets.len(), REQUIRED_SECRETS.len() - 1);
    }

    #[test]
    fn upgrade_reuses_recorded_secrets_and_never_rotates() {
        // helm get values shows what a prior install supplied; upgrade must
        // re-supply exactly those so a live store's credential is unchanged, and
        // must NOT mint a new value for a key with no record (leaving the
        // running release as-is rather than rotating it out from under a store).
        let existing: serde_json::Value = serde_json::from_str(
            r#"{"postgres":{"auth":{"password":"kept-pg-pw"}},"api":{"apiKey":"kept-api-key"}}"#,
        )
        .unwrap();
        let secrets = resolve_generated_secrets(Some(&existing), &[]).unwrap();
        assert_eq!(
            secrets,
            vec![
                (
                    "postgres.auth.password".to_string(),
                    "kept-pg-pw".to_string()
                ),
                ("api.apiKey".to_string(), "kept-api-key".to_string()),
            ],
            "upgrade must reuse recorded secrets and generate none: {secrets:?}"
        );
    }

    #[test]
    fn upgrade_ignores_empty_recorded_secret() {
        // An empty recorded value is not a real secret; do not re-supply it.
        let existing: serde_json::Value =
            serde_json::from_str(r#"{"valkey":{"password":""}}"#).unwrap();
        let secrets = resolve_generated_secrets(Some(&existing), &[]).unwrap();
        assert!(secrets.is_empty(), "{secrets:?}");
    }

    #[test]
    fn resolve_is_non_interactive_and_cannot_hang() {
        // The whole generate/reuse path is a pure function: no stdin, no TTY, so
        // a non-interactive / CI `cluster up` resolves secrets without blocking.
        // (Exercising it here would hang the test run if it ever read a TTY.)
        let _ = resolve_generated_secrets(None, &[]).unwrap();
        let _ = resolve_generated_secrets(Some(&serde_json::Value::Null), &[]).unwrap();
    }

    #[test]
    fn up_routes_preserved_slack_tokens_through_the_values_file_not_argv() {
        // End of the path, not just the resolver: a preserved bot token is a
        // live credential, so it must land in the private -f file like any other
        // secret and never appear in argv or the printed line.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![(
                "dispatcher.slack.botToken".into(),
                "xoxb-preserved-secret".into(),
            )],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(!line.contains("xoxb-preserved-secret"), "leaked: {line}");

        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(!argv.contains("xoxb-preserved-secret"), "argv leak: {argv}");
        let f_pos = materialized.argv().iter().position(|a| a == "-f").unwrap();
        let body = std::fs::read_to_string(&materialized.argv()[f_pos + 1]).unwrap();
        assert!(body.contains("xoxb-preserved-secret"), "{body}");
    }

    #[test]
    fn up_preserves_slack_tokens_a_previous_comms_recorded() {
        // `comms` writes with --reuse-values; `up` does a full upgrade and drops
        // whatever it does not pass. That deleted the dispatcher and its tokens
        // with no error and nothing in the diff mentioning Slack (#1067).
        let existing = serde_json::json!({
            "dispatcher": {"slack": {"appToken": "xapp-existing", "botToken": "xoxb-existing"}}
        });
        let preserved = resolve_comms_values(Some(&existing), &[]);
        assert_eq!(
            preserved,
            vec![
                (
                    "dispatcher.slack.appToken".to_string(),
                    "xapp-existing".to_string()
                ),
                (
                    "dispatcher.slack.botToken".to_string(),
                    "xoxb-existing".to_string()
                ),
            ]
        );
    }

    #[test]
    fn up_carries_forward_both_families_of_sibling_verb_values() {
        // The wiring, not the helpers. Testing each family in isolation left
        // this uncovered: deleting the call from `up` kept every test green,
        // which is how #1256 shipped in the first place.
        let existing = serde_json::json!({
            "dispatcher": {"slack": {"appToken": "xapp-x", "botToken": "xoxb-x"}},
            "api": {"githubAppId": "1234567", "githubAppPrivateKey": "FAKEKEY"},
        });
        let keys: Vec<String> = resolve_preserved_values(Some(&existing), &[])
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert!(
            keys.contains(&"dispatcher.slack.botToken".to_string()),
            "lost the Slack tokens (#1067): {keys:?}"
        );
        assert!(
            keys.contains(&"api.githubAppPrivateKey".to_string()),
            "lost the GitHub App (#1256): {keys:?}"
        );
    }

    #[test]
    fn a_plain_up_preserves_the_github_app_recorded_by_github_app() {
        // A full upgrade drops anything it does not re-pass. An operator who
        // wired the App and later ran `up` for something unrelated silently
        // lost it, and every private-repo deploy 404d with nothing in the diff
        // mentioning the App (#1256).
        let existing = serde_json::json!({
            "api": {
                "githubAppId": "1234567",
                "githubAppPrivateKey": "FAKEKEY",
                "githubCloneBase": "https://github.example.com",
            }
        });
        let preserved = resolve_github_app_values(Some(&existing), &[]);
        assert_eq!(
            preserved,
            vec![
                ("api.githubAppId".to_string(), "1234567".to_string()),
                ("api.githubAppPrivateKey".to_string(), "FAKEKEY".to_string()),
                (
                    "api.githubCloneBase".to_string(),
                    "https://github.example.com".to_string()
                ),
            ]
        );
    }

    #[test]
    fn the_byo_secret_reference_is_preserved_too() {
        // The recommended path (#1236) records a Secret NAME rather than the
        // key. Dropping that reference reverts the API to the chart's own
        // Secret, whose default is empty -- the same silent revocation.
        let existing = serde_json::json!({
            "api": {
                "githubAppId": "1234567",
                "githubAppExistingSecret": "curie-github-app",
                "githubAppExistingSecretKey": "privateKey",
            }
        });
        let keys: Vec<String> = resolve_github_app_values(Some(&existing), &[])
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert!(keys.contains(&"api.githubAppExistingSecret".to_string()));
        assert!(keys.contains(&"api.githubAppExistingSecretKey".to_string()));
    }

    #[test]
    fn an_operator_set_wins_over_the_recorded_app_value() {
        // Preserve, never override. `--set` is the operator saying they own it.
        let existing = serde_json::json!({"api": {"githubAppId": "1234567"}});
        let preserved =
            resolve_github_app_values(Some(&existing), &["api.githubAppId=999".to_string()]);
        assert!(preserved.is_empty());
    }

    #[test]
    fn nothing_is_invented_when_no_app_was_ever_configured() {
        // Preserving must not fabricate. An invented App id is worse than a
        // dropped one: it fails auth in a way that reads as a permissions
        // problem rather than a missing credential.
        assert!(resolve_github_app_values(None, &[]).is_empty());
        assert!(resolve_github_app_values(Some(&serde_json::json!({})), &[]).is_empty());
    }

    #[test]
    fn up_lets_an_explicit_set_override_a_preserved_comms_value() {
        let existing = serde_json::json!({
            "dispatcher": {"slack": {"appToken": "xapp-old", "botToken": "xoxb-old"}}
        });
        let preserved = resolve_comms_values(
            Some(&existing),
            &["dispatcher.slack.botToken=xoxb-new".to_string()],
        );
        // Only the untouched key is re-supplied; the operator's --set wins.
        assert_eq!(
            preserved,
            vec![(
                "dispatcher.slack.appToken".to_string(),
                "xapp-old".to_string()
            )]
        );
    }

    #[test]
    fn up_preserves_nothing_on_a_fresh_install_or_when_slack_was_never_set() {
        assert!(resolve_comms_values(None, &[]).is_empty());
        let no_slack = serde_json::json!({"nameOverride": "acme-bot"});
        assert!(resolve_comms_values(Some(&no_slack), &[]).is_empty());
        // An empty string is what `comms --disconnect` writes; do not resurrect it.
        let disconnected = serde_json::json!({
            "dispatcher": {"slack": {"appToken": "", "botToken": ""}}
        });
        assert!(resolve_comms_values(Some(&disconnected), &[]).is_empty());
    }

    fn secret_for<'a>(opts: &'a UpOpts, key: &str) -> Option<&'a str> {
        opts.secrets
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    fn completed_dev_up(existing: Option<&serde_json::Value>, set: Vec<String>) -> UpOpts {
        complete_up_opts_without_runner_egress(
            UpOpts {
                retained_mail_values: None,
                common: common(),
                github_token: GithubTokenPlan::Untouched,
                allow_egress_host: vec![],
                resolved_egress_cidrs: vec![],
                chart: "charts/curie".into(),
                secrets: vec![],
                dev: true,
                no_expose: true,
                set,
                set_string: vec![],
                allow_web_egress: vec![],
                fake_model: false,
                credentials: None,
                local_model: None,
                model: None,
            },
            existing,
            None,
            false,
        )
        .unwrap()
    }

    /// #1134 / #1125: `cluster up --dev` after `cluster comms` is a FULL
    /// upgrade. Helm reuse never engages because `--dev` always passes
    /// `--set security.allowDevDefaults=true`, so anything this path does not
    /// re-pass resets to the empty chart default.
    ///
    /// This is the wiring, not the family helpers: `resolve_comms_values` and
    /// `resolve_preserved_values` already return the keys. Deleting the `--dev`
    /// arm that copies them onto `opts.secrets` (or gating it behind
    /// `if !opts.dev`) is what this fails on -- the same class of hole that
    /// left #1256 green.
    #[test]
    fn a_dev_upgrade_re_supplies_recorded_comms_and_sealing_values() {
        let existing = serde_json::json!({
            "security": {"allowDevDefaults": true},
            "dispatcher": {"slack": {"appToken": "xapp-EXAMPLE", "botToken": "xoxb-EXAMPLE"}},
            "sealing": {"privateKey": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
            "api": {
                "githubAppId": "1234567",
                "githubAppPrivateKey": "FAKEKEY",
            }
        });
        let opts = completed_dev_up(Some(&existing), vec![]);
        assert_eq!(
            secret_for(&opts, "dispatcher.slack.appToken"),
            Some("xapp-EXAMPLE"),
            "a second `cluster up --dev` dropped the Slack app token (#1134): {:?}",
            opts.secrets
        );
        assert_eq!(
            secret_for(&opts, "dispatcher.slack.botToken"),
            Some("xoxb-EXAMPLE"),
            "a second `cluster up --dev` dropped the Slack bot token (#1134): {:?}",
            opts.secrets
        );
        assert_eq!(
            secret_for(&opts, crate::sealing::SEALING_PRIVATE_KEY),
            Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            "a second `cluster up --dev` dropped the recorded sealing key (#1134): {:?}",
            opts.secrets
        );
        assert_eq!(
            secret_for(&opts, "api.githubAppPrivateKey"),
            Some("FAKEKEY"),
            "a second `cluster up --dev` dropped the GitHub App on the --dev arm: {:?}",
            opts.secrets
        );
        for (key, _) in REQUIRED_SECRETS {
            assert!(
                secret_for(&opts, key).is_none(),
                "--dev must keep published chart credential defaults, not mint {key}: {:?}",
                opts.secrets
            );
        }

        let (materialized, _guards) = up_commands(&opts)[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            argv.contains("security.allowDevDefaults=true"),
            "dev mode must still opt into chart defaults: {argv}"
        );
        assert!(
            !argv.contains("--reuse-values"),
            "up must remain a full Helm upgrade: {argv}"
        );
        assert!(
            !argv.contains("xapp-EXAMPLE") && !argv.contains("xoxb-EXAMPLE"),
            "Slack tokens leaked into argv: {argv}"
        );
        let joined = secret_values_file_bodies(&materialized).join("\n");
        assert!(
            joined.contains("xapp-EXAMPLE") && joined.contains("xoxb-EXAMPLE"),
            "the --dev values file dropped the Slack tokens: {joined}"
        );
        assert!(
            joined.contains("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
            "the --dev values file dropped the sealing key: {joined}"
        );
    }

    #[test]
    fn a_dev_upgrade_does_not_mint_a_sealing_key_when_none_is_recorded() {
        let existing = serde_json::json!({"security": {"allowDevDefaults": true}});
        let opts = completed_dev_up(Some(&existing), vec![]);
        assert!(
            secret_for(&opts, crate::sealing::SEALING_PRIVATE_KEY).is_none(),
            "--dev must not mint a sealing key on a release that never had one: {:?}",
            opts.secrets
        );
        assert!(
            opts.secrets.is_empty(),
            "a --dev rerun with nothing recorded must not invent secrets: {:?}",
            opts.secrets
        );
    }

    #[test]
    fn a_dev_upgrade_stays_disconnected_after_comms_disconnect() {
        let existing = serde_json::json!({
            "security": {"allowDevDefaults": true},
            "dispatcher": {"slack": {"appToken": "", "botToken": ""}}
        });
        let opts = completed_dev_up(Some(&existing), vec![]);
        assert!(
            secret_for(&opts, "dispatcher.slack.appToken").is_none(),
            "empty disconnect values must not be resurrected: {:?}",
            opts.secrets
        );
        assert!(secret_for(&opts, "dispatcher.slack.botToken").is_none());
    }

    #[test]
    fn a_dev_upgrade_lets_an_explicit_set_replace_a_recorded_comms_value() {
        let existing = serde_json::json!({
            "security": {"allowDevDefaults": true},
            "dispatcher": {"slack": {"appToken": "xapp-old", "botToken": "xoxb-old"}}
        });
        let opts = completed_dev_up(
            Some(&existing),
            vec!["dispatcher.slack.botToken=xoxb-new".into()],
        );
        assert_eq!(
            secret_for(&opts, "dispatcher.slack.appToken"),
            Some("xapp-old")
        );
        assert!(
            secret_for(&opts, "dispatcher.slack.botToken").is_none(),
            "an explicit --set must own the key, not ride alongside the recorded value: {:?}",
            opts.secrets
        );
    }

    #[test]
    fn up_injects_generated_secrets_via_values_file_not_argv() {
        // Success criterion: a missing secret's generated value lands in the
        // private -f values file, never in the executed argv / process table.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![
                ("api.apiKey".into(), "generated-api-key".into()),
                (
                    "langfuse.encryptionKey".into(),
                    "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef0".into(),
                ),
            ],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        // Printed form masks the values and shows the -f secret values file.
        let line = cmds[0].display();
        assert!(line.contains("-f '<secret values file:"), "{line}");
        assert!(line.contains("api.apiKey=generate***"), "{line}");
        assert!(!line.contains("generated-api-key"), "secret leaked: {line}");

        // Materialize the way the executor does: the secret must be in the file,
        // not in argv.
        let (materialized, guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            !argv.contains("generated-api-key"),
            "leaked into argv: {argv}"
        );
        assert!(
            !argv.contains("api.apiKey="),
            "secret --set leaked into argv: {argv}"
        );
        let f_pos = materialized.argv().iter().position(|a| a == "-f").unwrap();
        let path = std::path::PathBuf::from(&materialized.argv()[f_pos + 1]);
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("generated-api-key"), "{body}");
        assert!(
            body.contains("api") && body.contains("apiKey"),
            "values file is not the expected nested shape: {body}"
        );
        drop(guards);
    }

    #[test]
    fn up_without_generated_secrets_is_unchanged() {
        // The pure builder with no supplied secrets (the --dev path, and every
        // pre-#196 argv test) emits no secret values file.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: true,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        assert!(!cmds[0].display().contains("secret values file"));
    }

    #[test]
    fn up_dev_emits_allow_dev_defaults_flag() {
        // Under --dev the operator opts into the deterministic published chart
        // credentials, so `up` must pass security.allowDevDefaults=true through
        // to helm (issue #195). Without it the sealed chart generates strong
        // random values and the dev/e2e stack would not match compose.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: true,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("security.allowDevDefaults=true"),
            "expected --dev to emit security.allowDevDefaults=true: {line}"
        );
    }

    #[test]
    fn up_without_dev_omits_allow_dev_defaults_flag() {
        // The default (non-dev) path must NOT opt into the published defaults;
        // the sealed chart generates strong per-release credentials there.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(
            !line.contains("security.allowDevDefaults"),
            "non-dev up must not emit security.allowDevDefaults: {line}"
        );
    }

    #[test]
    fn helm_get_values_reads_user_supplied_values_as_json() {
        let cmd = helm_get_values_cmd(&common());
        assert_eq!(cmd.display(), "helm get values curie -n curie -o json");
    }

    /// `--all` is the entire point of this helper, and its absence is invisible
    /// at runtime: helm answers happily either way, just without the chart
    /// defaults. That is exactly the bug #1950 reports -- an operator who never
    /// supplied a model has no user-supplied value to read, so the sibling
    /// command above returns nothing and `doctor` reports the floating chart
    /// default as "not applicable". A test that does not pin `--all` lets a
    /// refactor reintroduce that silently.
    #[test]
    fn helm_get_all_values_reads_computed_values_as_json() {
        let cmd = helm_get_all_values_cmd(&common());
        assert_eq!(
            cmd.display(),
            "helm get values curie -n curie --all -o json"
        );
    }

    #[test]
    fn ui_api_url_nodeport_with_host_builds_proxy_url() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;
        let url = ui_api_url_from_parts(json, Some("10.0.0.5")).expect("should build a proxy URL");
        assert_eq!(url, "http://10.0.0.5:31234/api");
    }

    #[test]
    fn node_http_url_brackets_ipv6_and_appends_path() {
        assert_eq!(
            node_http_url("10.0.0.5", 31234, "/api"),
            "http://10.0.0.5:31234/api"
        );
        assert_eq!(
            node_http_url("::1", 31234, "/api"),
            "http://[::1]:31234/api"
        );
        assert_eq!(
            node_http_url("node.local", 30080, "/?api=1"),
            "http://node.local:30080/?api=1"
        );
        assert_eq!(
            node_http_url("10.0.0.5", 30080, ""),
            "http://10.0.0.5:30080"
        );
    }

    #[test]
    fn ui_api_url_ipv6_host_is_bracketed() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;
        let url = ui_api_url_from_parts(json, Some("::1")).expect("should build a proxy URL");
        assert_eq!(url, "http://[::1]:31234/api");
    }

    #[test]
    fn ui_api_url_nodeport_without_host_errs_mentioning_api_url() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;
        let err = ui_api_url_from_parts(json, None).expect_err("a missing host must error");
        assert!(err.to_string().contains("--api-url"), "{err}");
    }

    #[test]
    fn ui_api_url_nodeport_without_assigned_nodeport_errs_mentioning_api_url() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":80}]}}"#;
        let err = ui_api_url_from_parts(json, Some("10.0.0.5"))
            .expect_err("an unassigned nodePort must error");
        assert!(err.to_string().contains("--api-url"), "{err}");
    }

    #[test]
    fn ui_api_url_clusterip_errs_mentioning_no_expose_and_api_url() {
        let json = r#"{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}"#;
        let err = ui_api_url_from_parts(json, Some("10.0.0.5"))
            .expect_err("a non-NodePort service must error");
        let msg = err.to_string();
        assert!(msg.contains("--no-expose"), "{msg}");
        assert!(msg.contains("--api-url"), "{msg}");
    }

    #[test]
    fn api_url_falls_back_to_the_api_service_nodeport() {
        // ui.deploy=false is a supported way to run a Slack-only bot; when the
        // UI is absent the api service's own NodePort is still a valid target.
        // No /api suffix -- that path is the UI's proxy, not the API itself.
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":8000,"nodePort":30799}]}}"#;
        let url = api_url_from_parts(json, Some("10.0.0.5")).expect("should build a direct URL");
        assert_eq!(url, "http://10.0.0.5:30799");
    }

    #[test]
    fn api_url_fallback_declines_a_clusterip_api_service() {
        // ClusterIP is unreachable from outside the cluster, so there is no URL
        // to return; the caller turns this into an actionable error.
        let json = r#"{"spec":{"type":"ClusterIP","ports":[{"port":8000}]}}"#;
        assert!(api_url_from_parts(json, Some("10.0.0.5")).is_none());
    }

    #[test]
    fn api_url_fallback_declines_without_a_host() {
        let json = r#"{"spec":{"type":"NodePort","ports":[{"port":8000,"nodePort":30799}]}}"#;
        assert!(api_url_from_parts(json, None).is_none());
    }

    #[test]
    fn ui_api_url_malformed_json_errs_mentioning_api_url() {
        let err =
            ui_api_url_from_parts("", Some("10.0.0.5")).expect_err("malformed JSON must error");
        assert!(err.to_string().contains("--api-url"), "{err}");
    }

    // -----------------------------------------------------------------------
    // Observability twin (issue #460): the pure discovery core that both
    // `cluster status` and `cluster observability` build on. Only the kubectl
    // boundary is mocked -- by feeding the service JSON strings kubectl returns.
    // -----------------------------------------------------------------------

    /// The NodePort service fixture kubectl returns for an exposed service.
    const NODEPORT_SVC: &str =
        r#"{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":31234}]}}"#;

    /// The ClusterIP service fixture kubectl returns for a `--no-expose` install.
    const CLUSTERIP_SVC: &str = r#"{"spec":{"type":"ClusterIP","ports":[{"port":3000}]}}"#;

    #[test]
    fn resolve_service_endpoint_nodeport_builds_the_node_url() {
        // api=true appends the Console's `/?api=1` suffix path.
        assert_eq!(
            resolve_service_endpoint(NODEPORT_SVC, "10.0.0.5", true),
            ServiceEndpoint::NodePortUrl("http://10.0.0.5:31234/?api=1".to_string())
        );
        // api=false yields the bare node URL -- no `?api=1`.
        assert_eq!(
            resolve_service_endpoint(NODEPORT_SVC, "10.0.0.5", false),
            ServiceEndpoint::NodePortUrl("http://10.0.0.5:31234".to_string())
        );
        // An IPv6 host is bracketed so the authority stays valid (via node_http_url).
        assert_eq!(
            resolve_service_endpoint(NODEPORT_SVC, "::1", true),
            ServiceEndpoint::NodePortUrl("http://[::1]:31234/?api=1".to_string())
        );
    }

    #[test]
    fn resolve_service_endpoint_clusterip_yields_a_port_forward_hint() {
        // ClusterIP: not node-exposed, so the caller must port-forward. Each
        // privileged service port maps to a deterministic non-privileged local port.
        let http_clusterip = r#"{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}"#;
        let http_endpoint = resolve_service_endpoint(http_clusterip, "10.0.0.5", true);
        let ServiceEndpoint::PortForwardHint {
            local: http_local,
            port: http_port,
        } = http_endpoint
        else {
            panic!("ClusterIP services must yield a port-forward hint");
        };
        assert_eq!(http_local, 18080);
        assert_eq!(http_port, 80);
        assert!(
            http_local >= 1024,
            "the local port must be bindable by a non-root user"
        );

        let https_clusterip = r#"{"spec":{"type":"ClusterIP","ports":[{"port":443}]}}"#;
        let https_endpoint = resolve_service_endpoint(https_clusterip, "10.0.0.5", true);
        let ServiceEndpoint::PortForwardHint {
            local: https_local,
            port: https_port,
        } = https_endpoint
        else {
            panic!("ClusterIP services must yield a port-forward hint");
        };
        assert_eq!(https_local, 18443);
        assert_eq!(https_port, 443);
        assert!(
            https_local >= 1024,
            "the local port must be bindable by a non-root user"
        );

        // An absent port parses as 0, which falls back to local port 8080.
        let no_port = r#"{"spec":{"type":"ClusterIP","ports":[{}]}}"#;
        assert_eq!(
            resolve_service_endpoint(no_port, "10.0.0.5", true),
            ServiceEndpoint::PortForwardHint {
                local: 8080,
                port: 0
            }
        );
    }

    #[test]
    fn resolve_service_endpoint_boundary_variants_do_not_panic() {
        // NodePort type but the nodePort is not assigned yet (release settling).
        let unassigned = r#"{"spec":{"type":"NodePort","ports":[{"port":80}]}}"#;
        assert_eq!(
            resolve_service_endpoint(unassigned, "10.0.0.5", true),
            ServiceEndpoint::UnassignedNodePort
        );
        // Malformed / empty JSON is unreadable, never a panic.
        assert_eq!(
            resolve_service_endpoint("", "10.0.0.5", true),
            ServiceEndpoint::Unreadable
        );
        assert_eq!(
            resolve_service_endpoint("{not json", "10.0.0.5", true),
            ServiceEndpoint::Unreadable
        );
        // Well-formed JSON with no spec is also unreadable.
        assert_eq!(
            resolve_service_endpoint(r#"{"metadata":{"name":"ui"}}"#, "10.0.0.5", true),
            ServiceEndpoint::Unreadable
        );
    }

    #[test]
    fn port_forward_hint_reproduces_the_status_hint_text() {
        // The exact hint `cluster status` prints today for a ClusterIP service
        // (PR#34 visual-parity guard): two spaces before `then`.
        assert_eq!(
            port_forward_hint("curie", "curie-ui", 18080, 80, "/?api=1"),
            "kubectl -n curie port-forward svc/curie-ui 18080:80  then http://localhost:18080/?api=1"
        );
        // The 0-port fallback surfaces local 8080 while still forwarding to 0.
        assert_eq!(
            port_forward_hint("curie", "curie-langfuse-web", 8080, 0, ""),
            "kubectl -n curie port-forward svc/curie-langfuse-web 8080:0  then http://localhost:8080"
        );
    }

    #[test]
    fn service_url_json_uses_the_privileged_port_forward_hint() {
        let clusterip = r#"{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}"#;
        let endpoint = resolve_service_endpoint(clusterip, "10.0.0.5", true);
        let ServiceEndpoint::PortForwardHint { local, port } = endpoint else {
            panic!("ClusterIP services must yield a port-forward hint");
        };
        let service_url = ServiceUrl {
            label: "UI".to_string(),
            name: "curie-ui".to_string(),
            namespace: "curie".to_string(),
            api: true,
            kind: ServiceUrlKind::PortForward { local, port },
        };

        assert_eq!(
            service_url.to_json(),
            serde_json::json!({
                "name": "UI",
                "url": null,
                "note": "kubectl -n curie port-forward svc/curie-ui 18080:80  then http://localhost:18080/?api=1",
            })
        );
    }

    #[test]
    fn service_url_port_forward_hint_preserves_a_human_identity_target() {
        let service_url = ServiceUrl {
            label: "UI".to_string(),
            name: "curie-ui".to_string(),
            namespace: "curie".to_string(),
            api: true,
            kind: ServiceUrlKind::PortForward {
                local: 18080,
                port: 80,
            },
        };
        let ui = crate::ui::Ui::resolve(
            crate::ui::ColorFlag::Never,
            false,
            false,
            false,
            &crate::ui::UiEnv {
                no_color: false,
                clicolor_zero: false,
                clicolor_force: false,
                term_dumb: false,
                ci: false,
                stderr_tty: true,
                stdout_tty: true,
                utf8: true,
                truecolor: false,
            },
        );
        let target = ui.url("http://localhost:18080/?api=1");
        assert_eq!(target, "http://localhost:18080/?api=1");
        assert_eq!(
            service_url.port_forward_hint(18080, 80, &target),
            "kubectl -n curie port-forward svc/curie-ui 18080:80  then http://localhost:18080/?api=1"
        );
    }

    #[test]
    fn api_base_endpoint_maps_ui_service_to_a_non_browsable_api_endpoint() {
        // A NodePort ui service resolves to the UI /api proxy URL (#360) and is
        // NEVER browsable -- it is an agent target, not a webapp.
        let ep = api_base_endpoint(&common(), &fullname(), Some(NODEPORT_SVC), Some("10.0.0.5"));
        assert_eq!(ep.name, "Curie API");
        assert_eq!(ep.url.as_deref(), Some("http://10.0.0.5:31234/api"));
        assert_eq!(ep.note, None);
        assert!(!ep.browsable);
    }

    #[test]
    fn api_base_endpoint_degrades_to_a_note_when_the_ui_service_is_unreadable() {
        // Unreadable ui service: degrade to a note endpoint rather than failing
        // the whole command, and never smuggle the message into `url`.
        let ep = api_base_endpoint(&common(), &fullname(), Some(""), Some("10.0.0.5"));
        assert_eq!(ep.name, "Curie API");
        assert_eq!(ep.url, None, "a degraded endpoint must not carry a url");
        assert!(
            ep.note.is_some(),
            "a degraded endpoint must explain itself in `note`"
        );
        assert!(!ep.browsable);
    }

    /// The API-base row must NEVER name `--api-url`: `cluster observability`
    /// has no such flag (only --namespace/--release/--dry-run/--open), so the
    /// hint inherited from `cluster deploy`'s error vocabulary is dead here.
    fn assert_no_api_url_hint(ep: &crate::observability::Endpoint) {
        let note = ep.note.as_deref().unwrap_or("");
        assert!(
            !note.contains("--api-url"),
            "`cluster observability` has no --api-url flag; dead hint in: {note}"
        );
    }

    #[test]
    fn api_base_endpoint_reports_a_missing_ui_service_as_not_found() {
        // Not "could not read" (the deploy-path wording): the true condition is
        // not-found, and this row must agree with the `ui` row from
        // `service_surface`.
        let ep = api_base_endpoint(&common(), &fullname(), None, Some("10.0.0.5"));
        assert_eq!(ep.url, None);
        assert_eq!(ep.note.as_deref(), Some("service curie-ui not found"));
        assert!(!ep.browsable);
        assert_no_api_url_hint(&ep);
    }

    #[test]
    fn api_base_endpoint_hints_a_port_forward_for_a_clusterip_ui_service() {
        // `--no-expose` is a supported install mode, so this is a real path,
        // not an error: hand back an actionable port-forward for the API
        // service instead of deploy's dead --api-url hint.
        let ep = api_base_endpoint(
            &common(),
            &fullname(),
            Some(CLUSTERIP_SVC),
            Some("10.0.0.5"),
        );
        assert_eq!(ep.url, None);
        assert_eq!(
            ep.note.as_deref(),
            Some(
                "kubectl -n curie port-forward svc/curie-api 8000:8000  then http://localhost:8000"
            )
        );
        assert!(!ep.browsable);
        assert_no_api_url_hint(&ep);
    }

    #[test]
    fn api_base_endpoint_notes_stay_plain_for_the_json_payload() {
        // `Ui::emit_json` documents the payload as machine-consumed: no ANSI.
        for ep in [
            api_base_endpoint(&common(), &fullname(), None, Some("10.0.0.5")),
            api_base_endpoint(
                &common(),
                &fullname(),
                Some(CLUSTERIP_SVC),
                Some("10.0.0.5"),
            ),
            api_base_endpoint(&common(), &fullname(), Some(""), Some("10.0.0.5")),
            api_base_endpoint(&common(), &fullname(), Some(NODEPORT_SVC), None),
        ] {
            let note = ep.note.as_deref().unwrap_or("");
            assert!(
                !note.contains('\u{1b}'),
                "note must carry no ANSI: {note:?}"
            );
            assert_no_api_url_hint(&ep);
        }
    }

    #[test]
    fn api_base_endpoint_hints_a_port_forward_when_the_host_is_unresolvable() {
        let ep = api_base_endpoint(&common(), &fullname(), Some(NODEPORT_SVC), None);
        assert_eq!(ep.url, None);
        assert_eq!(
            ep.note.as_deref(),
            Some(
                "kubectl -n curie port-forward svc/curie-api 8000:8000  then http://localhost:8000"
            )
        );
        assert!(!ep.browsable);
        assert_no_api_url_hint(&ep);
    }

    // ---- service_surface: the whole cluster-tier ServiceEndpoint -> Endpoint
    // mapper. It decides url-vs-note and owns `browsable`, the --open gate.

    #[test]
    fn service_surface_maps_a_nodeport_service_to_a_browsable_url_row() {
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            Some(NODEPORT_SVC),
            Some("10.0.0.5"),
            true,
        );
        assert_eq!(ep.name, "Curie Console");
        assert_eq!(ep.url.as_deref(), Some("http://10.0.0.5:31234/?api=1"));
        assert_eq!(ep.note, None);
        assert!(ep.browsable, "a resolved NodePort URL is the --open target");
    }

    #[test]
    fn service_surface_degrades_when_the_service_is_not_found() {
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            None,
            Some("10.0.0.5"),
            true,
        );
        assert_eq!(ep.url, None, "a degraded row must never carry a url");
        assert_eq!(ep.note.as_deref(), Some("service curie-ui not found"));
        assert!(!ep.browsable, "--open must not fire on a degraded row");
    }

    #[test]
    fn service_surface_degrades_when_the_node_host_is_unresolvable() {
        // Pins the deliberate divergence from `cluster status`: this twin does
        // NOT inherit `discover_host()`'s `localhost` fallback, so an
        // unresolvable host is an explicit note, never a fabricated URL.
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            Some(NODEPORT_SVC),
            None,
            true,
        );
        assert_eq!(ep.url, None, "must not fabricate a localhost URL");
        assert_eq!(
            ep.note.as_deref(),
            Some("could not determine a node host to reach service curie-ui")
        );
        assert!(!ep.browsable);
    }

    #[test]
    fn service_surface_degrades_an_unassigned_nodeport_to_a_note() {
        let unassigned = r#"{"spec":{"type":"NodePort","ports":[{"port":80}]}}"#;
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            Some(unassigned),
            Some("10.0.0.5"),
            true,
        );
        assert_eq!(ep.url, None);
        assert_eq!(
            ep.note.as_deref(),
            Some("service curie-ui is NodePort but exposes no nodePort yet")
        );
        assert!(!ep.browsable);
    }

    #[test]
    fn service_surface_maps_a_clusterip_service_to_a_plain_port_forward_note() {
        let ep = service_surface(
            &common(),
            &fullname(),
            "langfuse-web",
            "Langfuse UI",
            Some(CLUSTERIP_SVC),
            Some("10.0.0.5"),
            false,
        );
        assert_eq!(ep.url, None);
        let note = ep.note.as_deref().expect("a port-forward hint");
        assert_eq!(
            note,
            "kubectl -n curie port-forward svc/curie-langfuse-web 3000:3000  then http://localhost:3000"
        );
        // Serialized into the --json payload, which is machine-consumed.
        assert!(
            !note.contains('\u{1b}'),
            "note must carry no ANSI: {note:?}"
        );
        assert!(!ep.browsable, "a port-forward row is not a browser target");
    }

    #[test]
    fn service_surface_degrades_an_unreadable_service_to_a_note() {
        let ep = service_surface(
            &common(),
            &fullname(),
            "ui",
            "Curie Console",
            Some("{not json"),
            Some("10.0.0.5"),
            true,
        );
        assert_eq!(ep.url, None);
        assert_eq!(ep.note.as_deref(), Some("could not read service curie-ui"));
        assert!(!ep.browsable);
    }

    #[test]
    fn observability_dry_run_plan_lists_the_read_only_lookups() {
        let lines: Vec<String> = observability_commands(&common(), &fullname())
            .iter()
            .map(|c| c.display())
            .collect();
        assert_eq!(lines.len(), 4, "{lines:?}");
        assert!(
            lines.iter().any(|l| l.contains("get svc curie-ui")),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("get svc curie-langfuse-web")),
            "{lines:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Explicit provider egress (issue #362): the model-provider carve-out is no
    // longer a hardcoded Anthropic CIDR pushed whenever a credential is present;
    // egress is opened only for operator-named providers, resolved to their API
    // host IPs, so a real model call fails closed unless the provider is asked
    // for by name.
    // -----------------------------------------------------------------------

    #[test]
    fn provider_egress_hosts_maps_known_providers_and_rejects_unknown() {
        for (provider, hosts) in [
            ("anthropic", vec!["api.anthropic.com"]),
            ("openrouter", vec!["openrouter.ai"]),
            ("zhipu", vec!["api.z.ai"]),
            ("moonshot", vec!["api.moonshot.ai"]),
            ("deepseek", vec!["api.deepseek.com"]),
        ] {
            assert_eq!(
                provider_egress_hosts(provider).unwrap(),
                hosts,
                "{provider}"
            );
        }

        // `openai` and `gemini` are not runner-drivable today, so they are NOT
        // known providers: they fall through to `None` rather than minting an
        // egress route to a host the harness cannot talk to (#362).
        assert!(provider_egress_hosts("openai").is_none());
        assert!(provider_egress_hosts("gemini").is_none());

        // Anything that is not a canonical provider name is unknown: a bare
        // domain, a host, the empty string.
        assert!(provider_egress_hosts("acme.com").is_none());
        assert!(provider_egress_hosts("api.anthropic.com").is_none());
        assert!(provider_egress_hosts("").is_none());

        // Case-sensitive: only the lowercase canonical names resolve, so an
        // uppercased spelling is rejected rather than silently normalized.
        assert!(provider_egress_hosts("Anthropic").is_none());
        assert!(provider_egress_hosts("ANTHROPIC").is_none());
    }

    #[test]
    fn parse_egress_provider_accepts_known_and_errs_usage_on_unknown() {
        // Each runner-drivable provider parses to its own canonical name.
        for p in ["anthropic", "openrouter", "zhipu", "moonshot", "deepseek"] {
            assert_eq!(parse_egress_provider(p).unwrap(), p);
        }

        // `openai` and `gemini` are no longer accepted -- the runner cannot
        // drive them, so they are usage errors like any other unknown value.
        for p in ["openai", "gemini"] {
            assert_eq!(
                parse_egress_provider(p).unwrap_err().class,
                crate::exit::ExitClass::Usage
            );
        }

        // An unknown value is a deterministic input error (exit 2 / Usage).
        let err = parse_egress_provider("acme.com").unwrap_err();
        assert_eq!(err.class, crate::exit::ExitClass::Usage);
        assert!(err.message.contains("acme.com"), "{}", err.message);
        assert!(
            err.message.contains("not a known provider"),
            "{}",
            err.message
        );
        // The message enumerates the accepted providers so the operator can fix
        // the flag without reading source.
        for p in ["anthropic", "openrouter", "zhipu", "moonshot", "deepseek"] {
            assert!(
                err.message.contains(p),
                "message should list `{p}`: {}",
                err.message
            );
        }
        // ...and does NOT advertise the providers the runner cannot drive.
        assert!(
            !err.message.contains("openai") && !err.message.contains("gemini"),
            "message should not list undrivable providers: {}",
            err.message
        );
        // The fix hint points at the escape hatch for arbitrary destinations.
        let fix = err.fix.expect("a usage error should carry a fix hint");
        assert!(fix.contains("--allow-web-egress"), "{fix}");

        // Case-sensitivity is enforced here too: `Anthropic` is not `anthropic`.
        assert_eq!(
            parse_egress_provider("Anthropic").unwrap_err().class,
            crate::exit::ExitClass::Usage
        );
    }

    #[test]
    fn ip_to_egress_cidr_appends_full_host_prefix() {
        use std::net::IpAddr;
        // An IPv4 host is a /32; an IPv6 host is a /128 -- a single-host CIDR so
        // the egress rule opens exactly that resolved address, nothing wider.
        let v4: IpAddr = "1.2.3.4".parse().unwrap();
        assert_eq!(ip_to_egress_cidr(v4), "1.2.3.4/32");
        let v6: IpAddr = "2001:db8::1".parse().unwrap();
        assert_eq!(ip_to_egress_cidr(v6), "2001:db8::1/128");
    }

    #[test]
    fn resolve_provider_egress_cidrs_dedups_sorts_and_covers_all_hosts() {
        use std::net::IpAddr;
        // Injected resolver so the test never touches real DNS. Anthropic and
        // OpenRouter share 1.1.1.1 to prove deduplication. Anthropic also
        // yields an IPv6 address to prove the v4/v6 mix. All addresses are
        // globally routable so they survive the split horizon guard.
        let resolve = |host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(match host {
                "api.anthropic.com" => {
                    vec![
                        "1.1.1.1".parse().unwrap(),
                        "2606:4700::1111".parse().unwrap(),
                    ]
                }
                "openrouter.ai" => {
                    vec!["1.1.1.1".parse().unwrap(), "1.0.0.1".parse().unwrap()]
                }
                "api.z.ai" => vec!["8.8.8.8".parse().unwrap()],
                "api.moonshot.ai" => vec!["8.8.4.4".parse().unwrap()],
                "api.deepseek.com" => vec!["9.9.9.9".parse().unwrap()],
                other => panic!("unexpected host {other}"),
            })
        };
        let providers = ["anthropic", "openrouter", "zhipu", "moonshot", "deepseek"]
            .map(str::to_string)
            .to_vec();
        let cidrs = resolve_provider_egress_cidrs(&providers, resolve).unwrap();
        // Deduplicated to one 1.1.1.1/32 and sorted for a stable install argv.
        assert_eq!(
            cidrs,
            vec![
                "1.0.0.1/32",
                "1.1.1.1/32",
                "2606:4700::1111/128",
                "8.8.4.4/32",
                "8.8.8.8/32",
                "9.9.9.9/32"
            ]
        );
    }

    #[test]
    fn resolve_provider_egress_cidrs_errs_when_host_resolves_empty() {
        use std::net::IpAddr;
        // A host that resolves to nothing is a hard error naming the host, not a
        // silent skip -- a real model call would otherwise fail closed with no
        // clue why.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> { Ok(vec![]) };
        let err = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("api.anthropic.com"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_propagates_resolver_error_naming_host() {
        use std::net::IpAddr;
        // A resolver failure propagates as an error that names the host that
        // failed to resolve.
        let resolve = |host: &str| -> std::io::Result<Vec<IpAddr>> {
            Err(std::io::Error::other(format!("dns down for {host}")))
        };
        let err = resolve_provider_egress_cidrs(&["openrouter".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("openrouter.ai"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_errs_on_unknown_provider() {
        use std::net::IpAddr;
        // An unknown provider in the slice fails loudly (should be pre-validated,
        // but never silently skipped).
        let resolve =
            |_host: &str| -> std::io::Result<Vec<IpAddr>> { Ok(vec!["10.0.0.1".parse().unwrap()]) };
        let err = resolve_provider_egress_cidrs(&["acme.com".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("acme.com"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_imds_address() {
        use std::net::IpAddr;
        // A poisoned DNS answer mapping a provider host to the node metadata
        // endpoint must fail loud, naming both the host and the address.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(vec!["169.254.169.254".parse().unwrap()])
        };
        let err = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("api.anthropic.com"), "{msg}");
        assert!(msg.contains("169.254.169.254"), "{msg}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_private_v4() {
        use std::net::IpAddr;
        let resolve =
            |_host: &str| -> std::io::Result<Vec<IpAddr>> { Ok(vec!["10.0.0.5".parse().unwrap()]) };
        let err = resolve_provider_egress_cidrs(&["openrouter".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("10.0.0.5"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_non_routable_v6() {
        use std::net::IpAddr;
        // Loopback, link-local, and ULA v6 answers all fail closed.
        for addr in ["::1", "fe80::1", "fc00::1"] {
            let resolve = move |_host: &str| -> std::io::Result<Vec<IpAddr>> {
                Ok(vec![addr.parse().unwrap()])
            };
            let err =
                resolve_provider_egress_cidrs(&["openrouter".to_string()], resolve).unwrap_err();
            assert!(format!("{err:#}").contains(addr), "{addr}: {err:#}");
        }
    }

    #[test]
    fn resolve_provider_egress_cidrs_accepts_public_addresses() {
        use std::net::IpAddr;
        // A normal public v4 + v6 pair mints the expected single-host CIDRs.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(vec![
                "1.1.1.1".parse().unwrap(),
                "2606:4700::1111".parse().unwrap(),
            ])
        };
        let cidrs = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve).unwrap();
        assert_eq!(cidrs, vec!["1.1.1.1/32", "2606:4700::1111/128"]);
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_mix_with_one_private() {
        use std::net::IpAddr;
        // A host that resolves to a public AND a private address fails loud --
        // the private one must never be silently dropped.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(vec![
                "1.1.1.1".parse().unwrap(),
                "10.0.0.5".parse().unwrap(),
            ])
        };
        let err = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("10.0.0.5"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_rejects_ipv4_mapped_private_v6() {
        use std::net::IpAddr;
        // An IPv4-mapped v6 of a private v4 is unmapped and re-checked, so it
        // is rejected just like the bare private v4.
        let resolve = |_host: &str| -> std::io::Result<Vec<IpAddr>> {
            Ok(vec!["::ffff:10.0.0.5".parse().unwrap()])
        };
        let err = resolve_provider_egress_cidrs(&["openrouter".to_string()], resolve).unwrap_err();
        assert!(format!("{err:#}").contains("10.0.0.5"), "{err:#}");
    }

    #[test]
    fn resolve_provider_egress_cidrs_routability_table() {
        use std::net::IpAddr;
        // Every non-globally-routable range must fail closed (Err), and every
        // public address must succeed (Ok). Injecting a single resolved answer
        // per case exercises `is_globally_routable_egress` end to end through
        // the resolver seam.
        let cases: &[(&str, bool)] = &[
            // Non-routable v4 -- each must be rejected.
            ("0.0.0.0", false),         // 0.0.0.0/8 / unspecified
            ("10.0.0.5", false),        // private 10/8
            ("100.64.0.1", false),      // CGNAT 100.64.0.0/10
            ("169.254.169.254", false), // link-local / IMDS
            ("192.0.0.1", false),       // IETF protocol assignments 192.0.0.0/24
            ("192.88.99.1", false),     // 6to4 relay anycast 192.88.99.0/24
            ("198.18.0.1", false),      // benchmarking 198.18.0.0/15
            ("240.0.0.1", false),       // reserved/future 240.0.0.0/4
            ("255.255.255.255", false), // broadcast (240/4)
            // Non-routable v6 -- each must be rejected.
            ("::1", false),             // loopback
            ("fe80::1", false),         // link-local
            ("fc00::1", false),         // ULA
            ("2001:db8::1", false),     // documentation
            ("::ffff:10.0.0.5", false), // IPv4-mapped private
            // Public addresses -- each must succeed.
            ("1.1.1.1", true),
            ("8.8.8.8", true),
            ("2606:4700::1111", true),
            ("2001:4860:4860::8888", true),
        ];
        for (addr, expect_ok) in cases {
            let a = *addr;
            let resolve =
                move |_host: &str| -> std::io::Result<Vec<IpAddr>> { Ok(vec![a.parse().unwrap()]) };
            let res = resolve_provider_egress_cidrs(&["anthropic".to_string()], resolve);
            if *expect_ok {
                let cidrs = res.unwrap_or_else(|e| panic!("{a} should be routable: {e:#}"));
                assert_eq!(cidrs.len(), 1, "{a} should mint one CIDR");
            } else {
                let err = res
                    .err()
                    .unwrap_or_else(|| panic!("{a} should be rejected as non-routable"));
                assert!(format!("{err:#}").contains(a), "{a}: {err:#}");
            }
        }
    }

    #[test]
    fn provider_egress_note_none_on_empty_and_lists_providers() {
        // No providers -> no note.
        assert!(provider_egress_note(&[]).is_none());
        // Non-empty -> a note that says egress was opened and names each provider.
        let note = provider_egress_note(&["anthropic".to_string(), "openrouter".to_string()])
            .expect("a note for a non-empty provider list");
        assert!(note.contains("egress opened"), "{note}");
        assert!(note.contains("anthropic"), "{note}");
        assert!(note.contains("openrouter"), "{note}");
    }

    #[test]
    fn sealed_credential_warning_only_when_cred_present_and_no_egress() {
        // The one combination that warns: a credential is present but nothing
        // opened egress, so the model is unreachable behind the sealed sandbox.
        let warn =
            sealed_credential_warning(true, false).expect("cred present + no egress must warn");
        assert!(warn.contains("sealed"), "{warn}");
        assert!(warn.contains("unreachable"), "{warn}");
        assert!(warn.contains("--allow-egress-host"), "{warn}");
        assert!(warn.contains("--allow-web-egress"), "{warn}");
        for provider in ["anthropic", "openrouter", "zhipu", "moonshot", "deepseek"] {
            assert!(
                warn.contains(provider),
                "warning should name {provider}: {warn}"
            );
        }

        // Every other combination stays silent.
        assert!(sealed_credential_warning(true, true).is_none());
        assert!(sealed_credential_warning(false, false).is_none());
        assert!(sealed_credential_warning(false, true).is_none());
    }

    #[test]
    fn model_egress_status_lines_no_cred_open_egress_never_says_sealed() {
        // The exact contradiction bug: no credential but egress opened via a
        // provider. The provider note must report the open, and the fake-model
        // warning must NOT claim the egress is sealed.
        let lines =
            model_egress_status_lines(false, false, false, &["anthropic".to_string()], true, false);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(msgs.iter().any(|m| m.contains("egress opened")), "{msgs:?}");
        for m in &msgs {
            assert!(!m.contains("sealed"), "{m}");
        }
    }

    #[test]
    fn model_egress_status_lines_cred_no_egress_warns_sealed() {
        // A credential present with nothing opened surfaces the sealed warning
        // naming both flags.
        let lines = model_egress_status_lines(true, false, false, &[], false, false);
        let warn = lines
            .iter()
            .find(|(w, _)| *w)
            .map(|(_, m)| m.as_str())
            .expect("a warn line");
        assert!(warn.contains("sealed"), "{warn}");
        assert!(warn.contains("--allow-egress-host"), "{warn}");
        assert!(warn.contains("--allow-web-egress"), "{warn}");
    }

    #[test]
    fn model_egress_status_lines_cred_open_egress_no_sealed() {
        // A credential with a provider egress opened: provider note + rotation
        // present, and no message claims the sandbox is sealed.
        let lines =
            model_egress_status_lines(true, false, false, &["openrouter".to_string()], true, false);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(msgs.iter().any(|m| m.contains("egress opened")), "{msgs:?}");
        assert!(msgs.iter().any(|m| m.contains("can rotate")), "{msgs:?}");
        for m in &msgs {
            assert!(!m.contains("sealed"), "{m}");
        }
    }

    #[test]
    fn model_egress_status_lines_fake_model_sealed_and_canned() {
        // No credential, no egress, real (not --fake-model) install: the
        // fake-model warning keeps the "(model egress stays sealed)" clause and
        // a canned-replies note follows.
        let lines = model_egress_status_lines(false, false, false, &[], false, false);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(
            msgs.iter()
                .any(|m| m.contains("(model egress stays sealed)")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter().any(|m| m.contains("Replies will be canned")),
            "{msgs:?}"
        );
    }

    #[test]
    fn model_egress_status_lines_canned_guidance_requires_native_base_urls() {
        let lines = model_egress_status_lines(false, false, false, &[], false, false);
        let canned = lines
            .iter()
            .map(|(_, message)| message.as_str())
            .find(|message| message.contains("Replies will be canned"))
            .expect("canned reply guidance");

        for provider in ["Zhipu", "Moonshot", "DeepSeek"] {
            assert!(canned.contains(provider), "{canned}");
        }
        assert!(canned.contains("worker runtime base URL"), "{canned}");
        assert!(canned.contains("matching egress"), "{canned}");
    }

    #[test]
    fn model_egress_status_lines_dry_run_skips_past_tense_note() {
        // Under dry-run the handler prints its own "a live run resolves..."
        // note, so this fn emits no past-tense "egress opened" line.
        let lines =
            model_egress_status_lines(true, false, false, &["anthropic".to_string()], true, true);
        for (_, m) in &lines {
            assert!(!m.contains("egress opened"), "{m}");
        }
    }

    #[test]
    fn model_egress_status_lines_dry_run_does_not_assert_fake_model() {
        // #1898: under --dry-run there is no `existing` release to read, so the
        // no-credential arm must not assert the fake-model outcome -- it must
        // say preservation is unknown offline instead, as a warning. It also
        // must not claim the sandbox "stays sealed": a live rerun could
        // re-supply the release's recorded egress, so that assertion would be
        // just as false offline as the fake-model one.
        let lines = model_egress_status_lines(false, false, false, &[], false, true);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        for m in &msgs {
            assert!(!m.contains("installing with the fake model"), "{m}");
            assert!(!m.contains("sealed"), "{m}");
        }
        let (is_warning, preservation_msg) = lines
            .iter()
            .find(|(_, m)| m.contains("preserves the release's recorded model configuration"))
            .expect("a preservation-unknown message");
        assert!(
            preservation_msg.contains("not read under --dry-run"),
            "{preservation_msg}"
        );
        assert!(*is_warning, "{msgs:?}");
    }

    #[test]
    fn model_egress_status_lines_live_run_still_asserts_fake_model_install() {
        // Sibling of model_egress_status_lines_dry_run_does_not_assert_fake_model:
        // same inputs but a live run, which must keep asserting the fake-model
        // outcome. Pins that only the dry-run path changed under #1898.
        let lines = model_egress_status_lines(false, false, false, &[], false, false);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(
            msgs.iter()
                .any(|m| m.contains("installing with the fake model")),
            "{msgs:?}"
        );
        assert!(
            msgs.iter()
                .any(|m| m.contains("(model egress stays sealed)")),
            "{msgs:?}"
        );
        for m in &msgs {
            assert!(!m.contains("not read under --dry-run"), "{m}");
        }
    }

    #[test]
    fn model_egress_status_lines_dry_run_keeps_credential_guidance() {
        // An operator on a fresh install must still be told how to enable the
        // real model, so softening the assertion under --dry-run must not drop
        // the guidance.
        let lines = model_egress_status_lines(false, false, false, &[], false, true);
        let note = lines
            .iter()
            .find(|(is_warning, _)| !*is_warning)
            .map(|(_, m)| m.as_str())
            .expect("a non-warn note");
        assert!(note.contains("CURIE_CREDENTIALS"), "{note}");
        assert!(note.contains("fresh install"), "{note}");
        assert!(note.contains("replies will be canned"), "{note}");
        assert!(note.contains("worker runtime base URL"), "{note}");
    }

    #[test]
    fn model_egress_status_lines_dry_run_open_egress_never_says_sealed() {
        // Dry-run sibling of model_egress_status_lines_no_cred_open_egress_never_says_sealed:
        // no credential, dry-run, but a provider egress is opened. The
        // preservation-unknown line must not carry the live-only "sealed" or
        // "installing with the fake model" language, and its "no model egress
        // is opened by this run" suffix must drop when egress is in fact open.
        let lines =
            model_egress_status_lines(false, false, false, &["anthropic".to_string()], true, true);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        for m in &msgs {
            assert!(!m.contains("sealed"), "{m}");
            assert!(!m.contains("installing with the fake model"), "{m}");
            assert!(!m.contains("no model egress is opened by this run"), "{m}");
        }
        assert!(
            msgs.iter().any(|m| m.contains("not read under --dry-run")),
            "{msgs:?}"
        );
    }

    #[test]
    fn model_egress_status_lines_explicit_fake_model_stays_silent_under_dry_run() {
        // No test above ever passes fake_model = true. An explicit --fake-model
        // run has already declared the outcome, so this helper must emit
        // nothing for it even under --dry-run.
        let lines = model_egress_status_lines(false, false, true, &[], false, true);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(lines.is_empty(), "{msgs:?}");
    }

    #[test]
    fn model_egress_status_lines_local_model_wins_over_dry_run_arm() {
        // No test above ever passes local_model = true. --dry-run --local-model
        // must keep reporting the local-model install, not the new
        // preservation-unknown warning.
        let lines = model_egress_status_lines(false, true, false, &[], false, true);
        let msgs: Vec<&str> = lines.iter().map(|(_, m)| m.as_str()).collect();
        assert!(
            msgs.iter().any(|m| m.contains("local model enabled")),
            "{msgs:?}"
        );
        for m in &msgs {
            assert!(!m.contains("not read under --dry-run"), "{m}");
            assert!(!m.contains("installing with the fake model"), "{m}");
        }
    }

    #[test]
    fn up_emits_resolved_provider_cidrs_before_web_egress_contiguously() {
        // Resolved provider CIDRs take the first slots (in order), then declared
        // web destinations continue contiguously -- one array, no gaps.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            model: None,
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["10.0.0.1/32".into(), "2001:db8::1/128".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec!["203.0.113.0/24".into()],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
        });
        let line = cmds[0].display();
        // Provider CIDRs occupy [0] and [1], each with the shared TCP/443 shape.
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=10.0.0.1/32'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].protocol=TCP'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].ports[0].port=443'"),
            "{line}"
        );
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[1].cidr=2001:db8::1/128'"),
            "{line}"
        );
        // The declared web destination continues at the next index, not [0].
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[2].cidr=203.0.113.0/24'"),
            "{line}"
        );
        // The old unconditional Anthropic carve-out is gone.
        assert!(!line.contains("160.79.104.0/23"), "{line}");
    }

    #[test]
    fn up_credential_without_any_egress_emits_no_allowed_egress() {
        // A credential with neither a resolved provider CIDR nor a web egress
        // destination enables the real model but opens NO egress -- the old
        // unconditional Anthropic carve-out is removed entirely (#362). The
        // sandbox stays sealed and the model is unreachable by design.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            model: None,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: Some("sk-ant-secretsecret".into()),
            local_model: None,
        });
        let line = cmds[0].display();
        // Real model still enabled and the credential still delivered by file.
        assert!(
            line.contains("agentSandbox.runner.fakeModel=false"),
            "{line}"
        );
        assert!(line.contains("-f '<secret values file:"), "{line}");
        // But NO egress rule at all -- and specifically not the old Anthropic one.
        assert!(!line.contains("160.79.104.0/23"), "{line}");
        assert!(!line.contains("allowedEgress"), "{line}");
    }

    #[test]
    fn up_web_egress_alone_still_starts_at_index_zero() {
        // Existing behavior preserved: with no credential and no provider host,
        // a declared web destination still occupies index [0].
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            model: None,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec!["203.0.113.0/24".into()],
            fake_model: true,
            credentials: None,
            local_model: None,
        });
        let line = cmds[0].display();
        assert!(
            line.contains("'security.networkPolicy.allowedEgress[0].cidr=203.0.113.0/24'"),
            "{line}"
        );
        assert!(!line.contains("allowedEgress[1]"), "{line}");
        assert!(!line.contains("160.79.104.0/23"), "{line}");
    }

    // -----------------------------------------------------------------------
    // `api.githubToken` as a private durable cluster input (#1124)
    //
    // The API's OUTBOUND GitHub credential (git-flow bundle clone + eval commit
    // status, #1058/#1097/#1109). Every assertion below reads a user-visible
    // outcome -- what `display()` renders, what `argv()` carries after
    // materialization, what the `--dry-run --json` plan serializes to -- never an
    // internal `CmdArg` shape, so renaming any of the machinery breaks nothing.
    // -----------------------------------------------------------------------

    /// One sentinel everywhere, so a leak is unambiguous in any output form and
    /// its masked prefix (`mask_secret` keeps 8 chars) is `ghp-SENT***`.
    const GH_SENTINEL: &str = "ghp-SENTINEL-1124-leak-canary";

    /// The masked form the operator SHOULD see: enough prefix to recognise the
    /// credential is applied, not enough to use it.
    const GH_MASKED: &str = "api.githubToken=ghp-SENT***";

    /// A `cluster up` carrying nothing but the GitHub credential plan, so each
    /// assertion below reads exactly one variable.
    fn up_with_github_token(plan: GithubTokenPlan) -> Vec<OpsCommand> {
        up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: plan,
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        })
    }

    /// Every values file a materialized command hands helm, in argv order.
    ///
    /// Plural on purpose: a real sealed `cluster up` emits more than one
    /// `SecretValuesFile` (the model credential, the GitHub credential, and the
    /// generated/preserved chart secrets are three separate args), so a helper
    /// that read only the FIRST `-f` would silently assert against the model
    /// credential file while believing it was reading the token's.
    fn secret_values_file_bodies(cmd: &OpsCommand) -> Vec<String> {
        let argv = cmd.argv();
        let bodies: Vec<String> = argv
            .iter()
            .enumerate()
            .filter(|(_, a)| a.as_str() == "-f")
            .map(|(i, _)| {
                std::fs::read_to_string(&argv[i + 1]).expect("reading the secret values file")
            })
            .collect();
        assert!(
            !bodies.is_empty(),
            "no -f values file in the materialized argv: {argv:?}"
        );
        bodies
    }

    #[test]
    fn github_token_rides_the_private_values_file_not_argv() {
        // AC1. A live GitHub bearer token gets the model credential's treatment:
        // a private 0600 `-f` values file, never a `--set`. `SecretSet` would
        // mask the printed form but still put `key=value` in the real argv, which
        // is the exact defect #1124 exists to close.
        let cmds = up_with_github_token(GithubTokenPlan::Set(GH_SENTINEL.into()));
        let line = cmds[0].display();
        assert!(
            !line.contains(GH_SENTINEL),
            "token leaked into display: {line}"
        );
        assert!(line.contains(GH_MASKED), "masked form missing: {line}");

        let (materialized, guards) = cmds[0]
            .materialize_secret_files()
            .expect("materializing the secret values file");
        let argv = materialized.argv();
        let argv_joined = argv.join(" ");
        assert!(
            !argv_joined.contains(GH_SENTINEL),
            "token leaked into argv: {argv_joined}"
        );
        assert!(
            !argv_joined.contains("api.githubToken="),
            "the credential must not ride as a --set at all: {argv_joined}"
        );

        // The file helm actually reads carries the real value, nested for helm.
        let f_pos = argv
            .iter()
            .position(|a| a == "-f")
            .expect("a -f flag in the materialized argv");
        let values_path = std::path::PathBuf::from(&argv[f_pos + 1]);
        let body = std::fs::read_to_string(&values_path).expect("reading the values file");
        assert!(
            body.contains(GH_SENTINEL),
            "token missing from values file: {body}"
        );
        assert!(
            body.contains("api") && body.contains("githubToken"),
            "values file is not the expected nested shape: {body}"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&values_path)
                .expect("stat values file")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600, "values file must be 0600, was {mode:o}");
        }
        drop(guards);
        assert!(
            !values_path.exists(),
            "values file should be deleted once the guard drops"
        );
    }

    #[test]
    fn github_token_absent_from_every_rendered_form() {
        // AC2, all four renderings in ONE test so none can be silently forgotten:
        // the human preview (`display()`), the executed argv, the `--dry-run`
        // plan built the way `up()` builds it, and that plan's `--json` string.
        // The `--verbose` plumbing echo is the same `display()` (run_step's
        // `ui.plumbing("+ {display()}")`), so it is covered by the first form.
        //
        // AC2's fifth surface, "errors", is NOT covered here and has no test:
        // `run_step`'s failure line is built inline
        // (`ui.failure(&format!("`{}` failed: {reason}", cmd.program))`) and
        // written to a process-global `Ui` with no capture seam, so a unit test
        // cannot read it. Asserting on a hand-rebuilt copy of that string would
        // be a tautology that survives changing `cmd.program` to `cmd.argv()`,
        // so there is deliberately no such test. Closing it needs `run_step`'s
        // failure line extracted into a named pure function, a production change.
        use crate::ui::CliOutput;

        let cmds = up_with_github_token(GithubTokenPlan::Set(GH_SENTINEL.into()));

        // 1. Human preview.
        let line = cmds[0].display();
        assert!(!line.contains(GH_SENTINEL), "display leak: {line}");

        // 2. Executed argv, after the executor's materialization step.
        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(!argv.contains(GH_SENTINEL), "argv leak: {argv}");

        // 3. The `--dry-run` plan, constructed exactly as `up()` constructs it.
        let plan = crate::ui::DryRunPlan {
            lines: cmds.iter().map(|cmd| cmd.display()).collect(),
        };
        for planned in &plan.lines {
            assert!(
                !planned.contains(GH_SENTINEL),
                "dry-run plan leak: {planned}"
            );
        }

        // 4. The `--dry-run --json` serialization of that same plan.
        let json = plan.to_json().to_string();
        assert!(!json.contains(GH_SENTINEL), "dry-run --json leak: {json}");

        // And the masked form IS present in each printed form, so an operator can
        // still see the credential is being applied rather than inferring it.
        assert!(
            line.contains(GH_MASKED),
            "masked form missing from display: {line}"
        );
        assert!(
            plan.lines.iter().any(|l| l.contains(GH_MASKED)),
            "masked form missing from the dry-run plan: {:?}",
            plan.lines
        );
        assert!(
            json.contains(GH_MASKED),
            "masked form missing from --json: {json}"
        );
    }

    #[test]
    fn github_token_masked_form_shows_only_the_prefix() {
        // AC2. `mask_secret`'s contract is 8 chars plus `***`; a ninth character
        // of a live bearer token is a leak, not a nicety.
        let cmds = up_with_github_token(GithubTokenPlan::Set(GH_SENTINEL.into()));
        let line = cmds[0].display();
        assert!(line.contains(GH_MASKED), "{line}");
        assert!(
            !line.contains("ghp-SENTI"),
            "a ninth token character reached the printed form: {line}"
        );
    }

    #[test]
    fn preserved_github_token_never_reaches_argv_either() {
        // AC2 secondary path. A token arriving through the PRESERVE path (what a
        // plain `cluster up` takes) is exactly as live as one just typed, so it
        // gets the same four-form treatment. Armed through the resolver over a
        // recorded release, never through the flag.
        use crate::ui::CliOutput;

        let existing = serde_json::json!({"api": {"githubToken": GH_SENTINEL}});
        let plan_state = resolve_github_token(Some(&existing), &[], None, false);
        assert_eq!(plan_state, GithubTokenPlan::Set(GH_SENTINEL.to_string()));

        let cmds = up_with_github_token(plan_state);
        let line = cmds[0].display();
        assert!(!line.contains(GH_SENTINEL), "display leak: {line}");
        assert!(line.contains(GH_MASKED), "masked form missing: {line}");

        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(!argv.contains(GH_SENTINEL), "argv leak: {argv}");

        let plan = crate::ui::DryRunPlan {
            lines: cmds.iter().map(|cmd| cmd.display()).collect(),
        };
        for planned in &plan.lines {
            assert!(
                !planned.contains(GH_SENTINEL),
                "dry-run plan leak: {planned}"
            );
        }
        let json = plan.to_json().to_string();
        assert!(!json.contains(GH_SENTINEL), "dry-run --json leak: {json}");
    }

    #[test]
    fn set_and_flag_together_is_rejected() {
        // AC2 guard. Letting `--set` quietly win would discard the operator's
        // protected input AND put the token in the process table, so supplying
        // both is a usage error rather than a precedence rule.
        let err = check_github_token_conflict(
            Some(GH_SENTINEL),
            false,
            &["api.githubToken=ghp-other".to_string()],
        )
        .expect_err("flag + --set must be rejected")
        .to_string();
        assert!(err.contains("--github-token"), "{err}");
        assert!(err.contains("--set"), "{err}");
        assert!(err.contains("api.githubToken"), "{err}");

        // The comma-joined form helm also accepts, which `operator_set_keys`
        // splits -- a guard that only matched the bare form would miss it.
        assert!(
            check_github_token_conflict(
                Some(GH_SENTINEL),
                false,
                &["worker.replicas=2,api.githubToken=ghp-other".to_string()],
            )
            .is_err(),
            "the comma-joined --set form must be rejected too"
        );

        // The clear is just as explicit an input as a value.
        assert!(
            check_github_token_conflict(None, true, &["api.githubToken=ghp-other".to_string()])
                .is_err(),
            "--clear-github-token + --set must be rejected"
        );

        // Legal: exactly one side supplied.
        assert!(check_github_token_conflict(Some(GH_SENTINEL), false, &[]).is_ok());
        assert!(check_github_token_conflict(None, true, &[]).is_ok());
        assert!(
            check_github_token_conflict(None, false, &["api.githubToken=ghp-other".to_string()])
                .is_ok(),
            "a lone --set stays legal (it is a verbatim pass-through by design)"
        );
        // An EMPTY flag value is not an explicit input (an exported-but-empty
        // CURIE_GITHUB_TOKEN is a routine shell accident), so it does not conflict.
        assert!(
            check_github_token_conflict(
                Some(""),
                false,
                &["api.githubToken=ghp-other".to_string()]
            )
            .is_ok(),
            "an empty --github-token is absence, not a competing input"
        );
    }

    #[test]
    fn plain_up_preserves_the_recorded_github_token() {
        // AC3. `up` does a FULL upgrade and drops whatever it does not re-pass,
        // so without this a later plain `cluster up` silently resets the
        // credential to the chart's empty default and private clones start
        // failing with nothing in the diff mentioning GitHub (#1067's shape).
        let existing = serde_json::json!({"api": {"githubToken": "ghp-recorded"}});
        assert_eq!(
            resolve_github_token(Some(&existing), &[], None, false),
            GithubTokenPlan::Set("ghp-recorded".to_string())
        );
    }

    #[test]
    fn fresh_install_or_never_set_preserves_nothing() {
        // AC3. Nothing recorded means nothing to keep, and an EMPTY record is
        // what `--clear-github-token` wrote -- resurrecting it would make the
        // clear a one-shot. Mirrors
        // `up_preserves_nothing_on_a_fresh_install_or_when_slack_was_never_set`.
        assert_eq!(
            resolve_github_token(None, &[], None, false),
            GithubTokenPlan::Untouched
        );
        let no_token = serde_json::json!({"nameOverride": "acme-bot"});
        assert_eq!(
            resolve_github_token(Some(&no_token), &[], None, false),
            GithubTokenPlan::Untouched
        );
        let cleared = serde_json::json!({"api": {"githubToken": ""}});
        assert_eq!(
            resolve_github_token(Some(&cleared), &[], None, false),
            GithubTokenPlan::Untouched
        );
    }

    #[test]
    fn github_token_is_not_in_the_generating_list() {
        // AC3, the CLI half of #1109's pin. `REQUIRED_SECRETS` GENERATES a random
        // for a key it finds absent, which is right for a credential the install
        // must have and wrong for this one: 32 characters of noise sent to GitHub
        // as a bearer token fails auth in a way that reads like a permissions
        // problem rather than a missing credential. It joins the preserve-only
        // semantics instead.
        assert!(
            !REQUIRED_SECRETS
                .iter()
                .any(|(k, _)| *k == "api.githubToken"),
            "api.githubToken must never join the generating list: {REQUIRED_SECRETS:?}"
        );
        let secrets = resolve_generated_secrets(None, &[]).unwrap();
        assert_eq!(secrets.len(), REQUIRED_SECRETS.len());
        assert!(
            !secrets.iter().any(|(k, _)| k == "api.githubToken"),
            "a fresh install minted a GitHub token: {secrets:?}"
        );
    }

    #[test]
    fn dev_never_skips_the_existing_values_read_only_dry_run_does() {
        // AC3, and the pin on the change that actually makes a `cluster up --dev`
        // preserve a recorded GitHub token: the existing-values read must happen
        // on the `--dev` path too. Before the hoist that read sat inside
        // `if !opts.dev`, so a `--dev` upgrade saw no record, resolved
        // `Untouched`, and the full helm upgrade reset the credential to the
        // chart's empty default with nothing in the output mentioning GitHub.
        //
        // All four combinations, so this states the whole rule rather than one
        // corner: `dry_run` is the ONLY input that may skip the read (`--dry-run`
        // stays fully offline), and `dev` must never be a term. Re-adding a `dev`
        // term is the specific regression this catches, and it fails here in the
        // `dev = true, dry_run = false` case regardless of which direction the
        // term is written.
        assert!(
            should_read_existing(false, false),
            "a plain live cluster up must read the release's existing values"
        );
        assert!(
            should_read_existing(true, false),
            "a live `cluster up --dev` must ALSO read them: --dev governs the chart's \
             dev-default secret VALUES, not whether an operator's recorded credential \
             is preserved (#1124)"
        );
        assert!(
            !should_read_existing(false, true),
            "--dry-run stays offline and never touches helm"
        );
        assert!(
            !should_read_existing(true, true),
            "--dev --dry-run stays offline too"
        );
    }

    #[test]
    fn dev_flag_does_not_suppress_the_github_token_values_file() {
        // AC3 secondary path, and the name states EXACTLY what it proves: the
        // `--dev` branch of `up_commands` adds only
        // `security.allowDevDefaults=true`, so a resolved credential still rides
        // the private values file beside it rather than being swallowed.
        //
        // The other half of `--dev` preservation, the decision to read the
        // release's existing values at all, is pinned by
        // `dev_never_skips_the_existing_values_read_only_dry_run_does`: that test
        // is what fails if someone re-adds a `dev` term to
        // `should_read_existing` and reinstates the pre-hoist `if !opts.dev` gate.
        //
        // Neither test covers the path end to end, which is worth stating rather
        // than leaving implied: `up()` is async and the read runs behind
        // `require_on_path("helm")` plus a live `helm get values`, so no unit test
        // exercises a real `--dev` upgrade actually re-supplying a recorded token.
        // Between them these two pin the read DECISION and the argv it feeds; the
        // live evidence is the E2E's `--dev` arm.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Set(GH_SENTINEL.into()),
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: true,
            no_expose: true,
            set: vec![],
            set_string: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });
        let line = cmds[0].display();
        assert!(line.contains("security.allowDevDefaults=true"), "{line}");
        assert!(line.contains(GH_MASKED), "{line}");
        assert!(!line.contains(GH_SENTINEL), "display leak: {line}");

        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(!argv.contains(GH_SENTINEL), "argv leak: {argv}");
        assert!(
            secret_values_file_bodies(&materialized)
                .iter()
                .any(|body| body.contains(GH_SENTINEL)),
            "the --dev install dropped the credential"
        );
    }

    /// #1145 / G1, G2: the guard's `dev == false` arm is unconditional -- a
    /// plain `cluster up` is never refused, whatever the release recorded.
    ///
    /// The reverse flip (a sealed run over a release that IS on dev defaults)
    /// is safe by construction, which is why this arm can be unconditional:
    /// `resolve_generated_secrets` re-supplies only what the release already
    /// recorded and never mints a value for an unrecorded key, so
    /// `curie.managedSecret` sees `value == default`, falls through to its
    /// `hasKey .existingData` branch, and preserves the dev values. If that
    /// ever became mint-on-missing, a plain `up` would rotate the credential
    /// and this arm would have to change with it.
    ///
    /// Inverting the `dev == false` arm fails here.
    #[test]
    fn a_non_dev_up_is_never_refused_whatever_the_release_recorded() {
        // The guard is a pure function precisely so it can be pinned here.
        // `run_prepared_up`, its single call site, is `async` and shells out to
        // helm, so no unit test drives that call site end to end -- worth
        // stating rather than leaving implied, exactly as the #1124 tests above
        // do. These tests pin the DECISION; the live evidence that a refused
        // `--dev` mutates nothing (`helm history` shows no new revision) is the
        // E2E arm's.
        let sealed = serde_json::json!({"api": {"githubToken": "gh"}});
        guard_dev_defaults_flip(false, Some(&sealed), &[])
            .expect("a plain `cluster up` over a sealed release must never be refused");

        let dev_release = serde_json::json!({"security": {"allowDevDefaults": true}});
        guard_dev_defaults_flip(false, Some(&dev_release), &[])
            .expect("a plain `cluster up` over a DEV release must never be refused either");

        guard_dev_defaults_flip(false, None, &[])
            .expect("a fresh non-dev install is never refused");
    }

    /// #1145 / G3, AC3: `--dev` on a fresh namespace is the supported case and
    /// the whole point of the flag. `existing == None` means helm positively
    /// reported "release: not found" -- and it is also exactly what `--dry-run`
    /// produces (`should_read_existing(_, true)` is `false`), so a
    /// `--dev --dry-run` plan is never refused either.
    #[test]
    fn dev_is_allowed_when_there_is_no_existing_release() {
        guard_dev_defaults_flip(true, None, &[])
            .expect("`--dev` on a fresh namespace is the supported case (AC3)");
    }

    /// #1145 / G4: a real release that recorded no user-supplied values yields
    /// `Some(Value::Null)`, not `None` -- `fetch_existing_values` returns
    /// `None` only when helm positively reports "release: not found". It is
    /// still an existing release whose Secret the flip would rewrite, so it
    /// must be refused. A guard written as a bare `existing.is_none()` check
    /// gets this exact case wrong.
    #[test]
    fn an_existing_release_with_no_recorded_values_is_still_an_existing_release() {
        let err = guard_dev_defaults_flip(true, Some(&serde_json::Value::Null), &[])
            .expect_err("Some(Value::Null) is an existing release and must be refused");
        assert_eq!(crate::exit::classify(&err).0, crate::exit::ExitClass::Usage);
    }

    /// #1145 / G5, G6: an idempotent `--dev` re-run over a release that is
    /// already on dev defaults is allowed, in BOTH shapes helm can record the
    /// key in -- the JSON boolean that `--set security.allowDevDefaults=true`
    /// produces, and the JSON string that `--set-string` or a quoted
    /// `curie.yaml` `set:` map produces (#1375). The boolean case is also the
    /// retry-after-a-failed-`--dev`-install case, since helm records the values
    /// of an install that failed partway.
    #[test]
    fn dev_reruns_over_a_release_already_on_dev_defaults_are_allowed() {
        let json_bool = serde_json::json!({"security": {"allowDevDefaults": true}});
        guard_dev_defaults_flip(true, Some(&json_bool), &[])
            .expect("an idempotent `--dev` re-run over a dev release must be allowed");

        let json_string = serde_json::json!({"security": {"allowDevDefaults": "true"}});
        guard_dev_defaults_flip(true, Some(&json_string), &[]).expect(
            "the `--set-string` / quoted `curie.yaml` spelling records the STRING \"true\" \
             and must read as dev-on too (#1375)",
        );
    }

    /// #1145 / G7-G10: every recorded shape the chart's
    /// `eq (toString .root.Values.security.allowDevDefaults) "true"` reads as
    /// OFF must be refused, and every refusal must be Usage class (exit 2) --
    /// a deterministic input error, not a runtime failure (AC5).
    ///
    /// `{}` with the key absent is the #1145 defect itself. The quoted
    /// `"false"` mirrors `charts/curie/ci/render-assertions.sh:247-258`, which
    /// asserts that spelling fails closed: the CLI must not read as dev-on what
    /// the chart reads as off. `"TRUE"`, a bare `security` map, a number and a
    /// null leaf pin the fail-closed default against a reader that treats any
    /// non-empty string (or any present key) as truthy.
    #[test]
    fn dev_refuses_every_recorded_shape_the_chart_reads_as_off() {
        for existing in [
            serde_json::json!({"security": {"allowDevDefaults": false}}),
            serde_json::json!({"security": {"allowDevDefaults": "false"}}),
            serde_json::json!({}),
            serde_json::json!({"security": {"allowDevDefaults": "TRUE"}}),
            serde_json::json!({"security": {}}),
            serde_json::json!({"security": {"allowDevDefaults": 1}}),
            serde_json::json!({"security": {"allowDevDefaults": null}}),
        ] {
            let refused = guard_dev_defaults_flip(true, Some(&existing), &[]);
            assert!(
                refused.is_err(),
                "`--dev` must refuse this recorded shape: {existing}"
            );
            let err = refused.unwrap_err();
            assert_eq!(
                crate::exit::classify(&err).0,
                crate::exit::ExitClass::Usage,
                "a refused `--dev` is a deterministic input error (exit 2), not a runtime \
                 failure: {existing}"
            );
        }
    }

    /// #1145 / AC2: the refusal has to SAY WHY, and the part an operator cannot
    /// recover from without being told is that the PVC-backed Postgres and
    /// RustFS data still holds the ORIGINAL generated credentials -- so the
    /// flip breaks authentication rather than merely reconfiguring. Asserted on
    /// the concept, case-insensitively, rather than by pinning a whole sentence
    /// that will churn as the wording improves.
    ///
    /// A fix hint must ride along (`classify(&err).1`), steering to a re-run
    /// without `--dev` or a teardown first.
    #[test]
    fn the_refusal_explains_the_pvc_held_original_credentials_and_carries_a_fix() {
        let existing = serde_json::json!({"api": {"githubToken": "gh"}});
        let err = guard_dev_defaults_flip(true, Some(&existing), &[])
            .expect_err("`--dev` over a sealed release is the #1145 defect and must be refused");

        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(
            fix.is_some(),
            "a refused `--dev` must carry a fix hint: the operator has to be told what to \
             run instead: {err}"
        );

        let message = err.to_string().to_lowercase();
        assert!(
            message.contains("pvc"),
            "the refusal must name the PVC-backed store data as why the flip breaks: {err}"
        );
        assert!(
            message.contains("original"),
            "the refusal must say the store data still holds the ORIGINAL credentials: {err}"
        );
        assert!(
            message.contains("--dev"),
            "the refusal must name the flag it is refusing: {err}"
        );
    }

    /// #1145: `--set security.allowDevDefaults=true` stays a deliberately
    /// unguarded operator escape hatch, but on an existing release it is
    /// DESTRUCTIVE advice and must never be advertised in operator-facing text
    /// -- not in the message and not in the fix hint. It belongs in the guard's
    /// doc comment only. This fails loudly if someone "helpfully" adds it.
    #[test]
    fn the_refusal_never_advertises_the_dev_defaults_escape_hatch() {
        let existing = serde_json::json!({"security": {"allowDevDefaults": false}});
        let err = guard_dev_defaults_flip(true, Some(&existing), &[]).expect_err("must refuse");
        let (_, fix) = crate::exit::classify(&err);

        let operator_text = format!("{err}\n{}", fix.unwrap_or_default()).to_lowercase();
        assert!(
            !operator_text.contains("allowdevdefaults"),
            "the refusal leaked the escape hatch into operator-facing text: {operator_text}"
        );
    }

    /// #1145 follow-up: an operator who explicitly supplies
    /// `security.allowDevDefaults` through `--set` or `--set-string` OWNS the
    /// effective value, so the guard must not refuse the run.
    ///
    /// `up_value_plan` emits the CLI's own `security.allowDevDefaults=true`
    /// FIRST when `--dev` is set and appends the operator's own expressions
    /// AFTER it. Helm is last-wins, so `--dev --set
    /// security.allowDevDefaults=false` actually renders the chart with the
    /// flag OFF and preserves the credentials the release already recorded --
    /// a safe run the guard used to refuse, and refuse with a message that
    /// misdescribed what was about to happen. The `=true` spelling is the
    /// documented unguarded escape hatch and stays open too. Both match this
    /// file's standing rule that an operator `--set` always wins.
    ///
    /// Every lane `UpOpts::operator_sets` can deliver the key through is
    /// covered: repeated `--set`, helm's comma-joined `a=1,b=2` form (which
    /// `operator_set_keys` also parses), and `--set-string`.
    #[test]
    fn an_explicit_operator_allow_dev_defaults_set_is_never_refused() {
        // Non-empty and recording no `security.allowDevDefaults`: a SEALED
        // release, which is exactly the shape the guard refuses on its own.
        let sealed = serde_json::json!({"security": {"gvisor": {"mode": "off"}}});

        for sets in [
            vec!["security.allowDevDefaults=false".to_string()],
            vec!["security.allowDevDefaults=true".to_string()],
            // Helm's comma-joined form, with the key in a non-leading position.
            vec!["api.replicas=2,security.allowDevDefaults=false".to_string()],
            vec!["security.gvisor.mode=off,security.allowDevDefaults=true".to_string()],
            // An unrelated override ahead of the explicit one.
            vec![
                "api.githubToken=x".to_string(),
                "security.allowDevDefaults=false".to_string(),
            ],
        ] {
            guard_dev_defaults_flip(true, Some(&sealed), &sets).unwrap_or_else(|error| {
                panic!(
                    "an explicit operator `security.allowDevDefaults` override owns the \
                     effective value and must not be refused: {sets:?}: {error}"
                )
            });
        }

        // The `--set-string` lane, threaded through `UpOpts::operator_sets()`
        // itself rather than a hand-built Vec, so this pins the real wiring:
        // `operator_sets` chains `--set` THEN `--set-string`, and a key the
        // operator supplied only through the latter must exempt the run too.
        let opts = UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: true,
            no_expose: false,
            set: vec![],
            set_string: vec!["security.allowDevDefaults=false".into()],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        };
        guard_dev_defaults_flip(true, Some(&sealed), &opts.operator_sets()).expect(
            "an explicit `--set-string security.allowDevDefaults=false` must exempt the run \
             too: `operator_sets()` chains both lanes",
        );
    }

    /// #1145 follow-up, mutation resistance: the exemption is keyed to the ONE
    /// key the operator overrode, NOT to "any `--set` was passed". A run
    /// carrying only unrelated overrides still leaves the CLI's own
    /// `security.allowDevDefaults=true` as helm's last word, so it is still the
    /// destructive flip and must still be refused, Usage class (exit 2).
    ///
    /// Widening the exemption to `!operator_sets.is_empty()` -- the easy way to
    /// write it -- fails here, and would in practice disable the guard for
    /// almost every real `cluster up`, since those nearly always carry
    /// overrides. Near-miss keys are included so a substring or bare-leaf match
    /// cannot stand in for the real dotted key either.
    #[test]
    fn unrelated_operator_sets_do_not_exempt_a_dev_flip() {
        let sealed = serde_json::json!({"security": {"gvisor": {"mode": "off"}}});

        for sets in [
            vec!["api.githubToken=x".to_string()],
            vec!["security.gvisor.mode=off,api.replicas=2".to_string()],
            // Same prefix, different leaf.
            vec!["security.allowDevDefaultsExtra=false".to_string()],
            // The bare leaf name, without the `security.` path helm records.
            vec!["allowDevDefaults=false".to_string()],
        ] {
            let refused = guard_dev_defaults_flip(true, Some(&sealed), &sets);
            assert!(
                refused.is_err(),
                "only an explicit `security.allowDevDefaults` override exempts a `--dev` \
                 flip over a sealed release: {sets:?}"
            );
            let err = refused.unwrap_err();
            assert_eq!(
                crate::exit::classify(&err).0,
                crate::exit::ExitClass::Usage,
                "a refused `--dev` stays a deterministic input error (exit 2): {sets:?}"
            );
        }
    }

    /// #1145: the reader mirrors the chart's
    /// `eq (toString .root.Values.security.allowDevDefaults) "true"`
    /// (`charts/curie/templates/_helpers.tpl:453`) and the fail-closed contract
    /// `charts/curie/ci/render-assertions.sh:247-258` pins. ONLY the JSON
    /// boolean `true` and the JSON string `"true"` read as dev-on; every other
    /// shape -- including a plausible-looking `"TRUE"`, a number, an object, a
    /// null leaf, a missing leaf and a missing intermediate segment -- fails
    /// closed to dev-off, continuing #1375's rule that a gate-shaped value of
    /// ambiguous spelling must fail closed.
    #[test]
    fn lookup_dotted_flag_mirrors_the_charts_tostring_coercion() {
        const KEY: &str = "security.allowDevDefaults";

        for on in [
            serde_json::json!({"security": {"allowDevDefaults": true}}),
            serde_json::json!({"security": {"allowDevDefaults": "true"}}),
        ] {
            assert!(lookup_dotted_flag(&on, KEY), "must read as dev-on: {on}");
        }

        for off in [
            serde_json::json!({"security": {"allowDevDefaults": false}}),
            serde_json::json!({"security": {"allowDevDefaults": "false"}}),
            serde_json::json!({"security": {"allowDevDefaults": "TRUE"}}),
            serde_json::json!({"security": {"allowDevDefaults": ""}}),
            serde_json::json!({"security": {"allowDevDefaults": 1}}),
            serde_json::json!({"security": {"allowDevDefaults": {"enabled": true}}}),
            serde_json::json!({"security": {"allowDevDefaults": null}}),
            serde_json::json!({"security": {}}),
            serde_json::json!({}),
            serde_json::json!({"security": "true"}),
            serde_json::json!({"other": {"allowDevDefaults": true}}),
            serde_json::Value::Null,
        ] {
            assert!(!lookup_dotted_flag(&off, KEY), "must fail closed: {off}");
        }
    }

    /// #1145: WHY this sibling reader exists rather than reusing
    /// `lookup_dotted`. Helm records `--set security.allowDevDefaults=true` as
    /// a JSON BOOLEAN, and `lookup_dotted` ends in `as_str()`, so it returns
    /// `None` for exactly the shape this key normally has. "Simplifying" the
    /// guard back onto `lookup_dotted` would make every idempotent `--dev`
    /// re-run (G5) refuse, silently reintroducing #1145's class of surprise --
    /// so this test states the structural gap rather than trusting a comment.
    #[test]
    fn lookup_dotted_cannot_read_the_json_boolean_this_flag_reader_exists_for() {
        let doc = serde_json::json!({"security": {"allowDevDefaults": true}});
        assert_eq!(
            lookup_dotted(&doc, "security.allowDevDefaults"),
            None,
            "if this ever returns Some, re-check whether the sibling reader is still needed"
        );
        assert!(
            lookup_dotted_flag(&doc, "security.allowDevDefaults"),
            "the boolean-aware reader is the one that can see helm's recorded shape"
        );
    }

    #[test]
    fn github_token_and_generated_secrets_ride_separate_values_files_that_merge() {
        // The shape a real sealed (`!dev`) `cluster up` actually emits, which no
        // other test covers: TWO `SecretValuesFile` args whose bodies both carry
        // a top-level `api` map -- `{"api":{"githubToken":...}}` from the
        // credential arm and `{"api":{"apiKey":...}}` from the generated/preserved
        // chart secrets. That is only correct because helm's `mergeMaps`
        // DEEP-merges successive `-f` files rather than replacing them
        // ("Priority is given to the last (right-most) file specified",
        // `helm install --help`, helm v3; values files are merged, not
        // overwritten, per helm's Values Files docs). A change that flattened
        // these into one map or made a later file replace an earlier one would
        // silently drop either `api.apiKey` (breaking platform auth) or the
        // credential, so both keys must survive to their own file and neither
        // may reach argv.
        let cmds = up_commands(&UpOpts {
            retained_mail_values: None,
            common: common(),
            github_token: GithubTokenPlan::Set(GH_SENTINEL.into()),
            set_string: vec![],
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![
                ("api.apiKey".into(), "generated-api-key".into()),
                (
                    "api.githubWebhookSecret".into(),
                    "generated-webhook-secret".into(),
                ),
            ],
            dev: false,
            no_expose: true,
            set: vec![],
            allow_web_egress: vec![],
            fake_model: false,
            credentials: None,
            local_model: None,
            model: None,
        });

        // Both credentials are masked in the printed form, neither is raw.
        let line = cmds[0].display();
        assert!(
            !line.contains(GH_SENTINEL),
            "token leaked into display: {line}"
        );
        assert!(
            !line.contains("generated-api-key"),
            "api key leaked into display: {line}"
        );
        assert!(line.contains(GH_MASKED), "{line}");
        assert!(line.contains("api.apiKey=generate***"), "{line}");

        let (materialized, _guards) = cmds[0].materialize_secret_files().unwrap();
        let argv = materialized.argv().join(" ");
        assert!(
            !argv.contains(GH_SENTINEL),
            "token leaked into argv: {argv}"
        );
        assert!(
            !argv.contains("generated-api-key"),
            "api key leaked into argv: {argv}"
        );

        // Two distinct `-f` files, not one: the token must not be folded into
        // the generated-secret map (that would put it in the same document helm
        // already had, losing the separation) and neither may be dropped.
        let bodies = secret_values_file_bodies(&materialized);
        assert_eq!(
            bodies.len(),
            2,
            "a sealed up with a credential must emit exactly two values files: {bodies:?}"
        );
        let token_file = bodies
            .iter()
            .find(|body| body.contains(GH_SENTINEL))
            .unwrap_or_else(|| panic!("no values file carries the GitHub credential: {bodies:?}"));
        let secrets_file = bodies
            .iter()
            .find(|body| body.contains("generated-api-key"))
            .unwrap_or_else(|| panic!("no values file carries the generated secrets: {bodies:?}"));
        assert_ne!(
            token_file, secrets_file,
            "the credential and the generated secrets must stay in separate files"
        );
        assert!(
            secrets_file.contains("generated-webhook-secret"),
            "the generated-secret file lost a key: {secrets_file}"
        );
        // Each nests under `api`, which is exactly why the deep merge matters.
        for body in [token_file, secrets_file] {
            let doc: serde_json::Value =
                serde_json::from_str(body).expect("each values file is a JSON document helm reads");
            assert!(
                doc.get("api").and_then(|v| v.as_object()).is_some(),
                "expected a top-level `api` map: {body}"
            );
        }
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(token_file).unwrap()["api"]["githubToken"],
            serde_json::Value::String(GH_SENTINEL.to_string())
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(secrets_file).unwrap()["api"]["apiKey"],
            serde_json::Value::String("generated-api-key".to_string())
        );
    }

    #[test]
    fn explicit_flag_replaces_the_recorded_value() {
        // AC4. An explicit value is a rotation; it must win over the record.
        let existing = serde_json::json!({"api": {"githubToken": "ghp-recorded"}});
        assert_eq!(
            resolve_github_token(Some(&existing), &[], Some("ghp-new"), false),
            GithubTokenPlan::Set("ghp-new".to_string())
        );
    }

    #[test]
    fn explicit_clear_writes_an_empty_value() {
        // AC4. The clear is the EMPTY string, which is not secret: it rides as a
        // visible plain `--set` (the shape `comms --disconnect` writes) so the
        // operator sees the removal in the preview instead of inferring it from
        // an absence, and so the release records the empty value.
        let existing = serde_json::json!({"api": {"githubToken": GH_SENTINEL}});
        assert_eq!(
            resolve_github_token(Some(&existing), &[], None, true),
            GithubTokenPlan::Clear
        );

        // Assert on argv, which is what helm actually receives.
        let cmds = up_with_github_token(GithubTokenPlan::Clear);
        let argv = cmds[0].argv();
        let pos = argv
            .iter()
            .position(|a| a == "api.githubToken=")
            .unwrap_or_else(|| panic!("no empty api.githubToken assignment in argv: {argv:?}"));
        assert!(pos > 0, "the assignment has no preceding flag: {argv:?}");
        assert_eq!(argv[pos - 1], "--set", "{argv:?}");
        assert!(
            !argv.join(" ").contains(GH_SENTINEL),
            "a clear must carry no token at all: {argv:?}"
        );
    }

    #[test]
    fn clear_then_plain_up_stays_cleared() {
        // AC4. The real state chain: a clear writes `""` into the release, and
        // the next plain `cluster up` reads that record back. If an empty record
        // preserved as a value the clear would be one-shot; if it were treated as
        // a credential the operator would see it re-applied.
        assert_eq!(
            resolve_github_token(
                Some(&serde_json::json!({"api": {"githubToken": GH_SENTINEL}})),
                &[],
                None,
                true
            ),
            GithubTokenPlan::Clear
        );
        assert_eq!(
            resolve_github_token(
                Some(&serde_json::json!({"api": {"githubToken": ""}})),
                &[],
                None,
                false
            ),
            GithubTokenPlan::Untouched
        );
    }

    #[test]
    fn empty_value_preserves_and_never_clears() {
        // AC4 negative, the destructive-ambiguity pin. `export
        // CURIE_GITHUB_TOKEN="$UNSET_VAR"` is a routine shell accident and clap
        // hands an exported-but-empty variable through as `Some("")`, not `None`.
        // An ambiguous signal must never trigger the sticky destructive state:
        // destroying a live credential requires the unambiguous
        // `--clear-github-token`.
        let existing = serde_json::json!({"api": {"githubToken": "ghp-recorded"}});
        assert_eq!(
            resolve_github_token(Some(&existing), &[], Some(""), false),
            GithubTokenPlan::Set("ghp-recorded".to_string())
        );
        // With nothing recorded an empty value is simply absence, not a clear.
        assert_eq!(
            resolve_github_token(None, &[], Some(""), false),
            GithubTokenPlan::Untouched
        );
    }

    #[test]
    fn set_passthrough_leaves_the_key_to_the_operator() {
        // AC4. A key the operator pinned through `--set` is theirs to own, and
        // the CLI supplies nothing for it -- matching
        // `operator_set_secret_is_left_to_the_operator`. (Passing BOTH is
        // rejected earlier by `check_github_token_conflict`.)
        let existing = serde_json::json!({"api": {"githubToken": "ghp-recorded"}});
        assert_eq!(
            resolve_github_token(
                Some(&existing),
                &["api.githubToken=ghp-operator".to_string()],
                None,
                false
            ),
            GithubTokenPlan::Untouched
        );
    }

    #[test]
    fn lone_set_passthrough_leak_is_detected() {
        // AC2's one mitigation on the surviving leak path. A lone `--set
        // api.githubToken=<value>` stays legal (the pass-through is verbatim by
        // design and breaking it would break existing operators), but it lands
        // the complete token in the process table, shell history and the printed
        // plan, so `up` steers the operator to the private input.
        //
        // Named for DETECTION, not for the warning: this pins the predicate
        // `up()` gates that `ui.warn` on, and nothing more. The `ui.warn` call
        // site itself is unguarded -- deleting it leaves this green -- because
        // `crate::ui::ui()` is a process-global `OnceLock<Ui>` writing straight
        // to `anstream::stderr()` with no capture seam, and the warning lives
        // inside async `up()` behind a live `helm get values`. Covering the call
        // site needs either a `Ui` capture seam or the note/warn set lifted into
        // a pure function returning the lines, both production changes.
        assert!(set_passthrough_leaks_github_token(&[
            "api.githubToken=ghp-operator".to_string()
        ]));
        // The comma-joined form helm also accepts leaks identically.
        assert!(set_passthrough_leaks_github_token(&[
            "worker.replicas=2,api.githubToken=ghp-operator".to_string()
        ]));
        // An empty assignment is the operator clearing the key by hand. Nothing
        // leaks, so warning about it would be noise on a correct command.
        assert!(!set_passthrough_leaks_github_token(&[
            "api.githubToken=".to_string()
        ]));
        // And an unrelated `--set` must stay silent.
        assert!(!set_passthrough_leaks_github_token(&[
            "worker.replicas=2".to_string()
        ]));
        assert!(!set_passthrough_leaks_github_token(&[]));
    }

    #[test]
    fn set_passthrough_shell_noise_still_leaks_but_a_respaced_key_is_a_different_key() {
        // The whitespace contract on the `--set` grammar, which decides both
        // which invocations warn AND (through the same
        // `operator_set_entries` parse) which ones
        // `check_github_token_conflict` rejects.
        //
        // The two trims are asymmetric ON PURPOSE, and this test exists so a
        // future reader does not "clean it up" into symmetry: the pair
        // reproduces, exactly, the behavior of the two hand-rolled parsers the
        // shared `operator_set_entries` replaced. Whitespace AROUND an
        // assignment is shell noise -- `--set " api.githubToken=ghp-x "` is the
        // same command an operator meant to type, and the token leaks either
        // way -- while whitespace INSIDE the key is not noise: helm reads
        // `api.githubToken ` as a differently-named value that this credential
        // does not ride on, so there is nothing to warn about.

        // Leading noise on the key is stripped: the token still leaks.
        assert!(set_passthrough_leaks_github_token(&[
            " api.githubToken=ghp-x".to_string()
        ]));
        // Noise on BOTH ends of the assignment, the shape a real shell produces.
        assert!(set_passthrough_leaks_github_token(&[
            " api.githubToken=ghp-x ".to_string()
        ]));
        // Trailing noise on a non-empty value does not make it empty: still a leak.
        assert!(set_passthrough_leaks_github_token(&[
            "api.githubToken=ghp-x   ".to_string()
        ]));
        // Whitespace BEFORE the `=` names a different helm key. Nothing this
        // credential rides on is being set, so nothing leaks. This is the case
        // that dies if the key's `trim_start` is widened to a `trim`.
        assert!(!set_passthrough_leaks_github_token(&[
            "api.githubToken =ghp-x".to_string()
        ]));
        // A whitespace-only value is an empty assignment, not a credential.
        assert!(!set_passthrough_leaks_github_token(&[
            "api.githubToken=   ".to_string()
        ]));

        // The sibling reader trims the key SYMMETRICALLY
        // (`operator_set_keys`), so `api.githubToken =x` is a key the CLI leaves
        // to the operator and a value the conflict guard rejects, even though it
        // raises no leak warning. Recorded here so the divergence between the two
        // consumers of one parse is deliberate rather than discovered later.
        assert!(
            operator_set_keys(&["api.githubToken =ghp-x".to_string()]).contains(GITHUB_TOKEN_KEY)
        );
        assert!(
            check_github_token_conflict(
                Some(GH_SENTINEL),
                false,
                &["api.githubToken =ghp-x".to_string()]
            )
            .is_err(),
            "the conflict guard matches the key trimmed on both ends"
        );
    }
}
