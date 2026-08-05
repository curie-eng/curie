//! `curie cluster up | cluster status | cluster down`: the operator
//! day-1 lifecycle, wrapping the Helm chart and `kubectl` the way linkerd or
//! cilium wrap theirs -- a deliberately thin CLI over the chart, which stays the
//! source of truth. Every verb shells out to the `helm`/`kubectl` binaries; the
//! CLI never re-derives what a values file already declares.
//!
//! Each verb builds its command lines as a pure function returning
//! [`OpsCommand`] vectors; the executor (or the `--dry-run` printer) consumes
//! them. That split keeps the argv construction unit-testable with no cluster
//! and gives one place to mask secrets before anything is printed.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use tokio::process::Command;

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
    SecretSet { key: String, value: String },
    SecretValuesFile(Vec<(String, String)>),
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
            CmdArg::SecretSet { key, value } => vec![format!("{key}={value}")],
            CmdArg::SecretValuesFile(_) => {
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
            CmdArg::SecretSet { key, value } => vec![format!("{key}={}", mask_secret(value))],
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
    /// delete those files when dropped (so they are cleaned up even if the helm
    /// run fails). Commands without a secret values file are returned unchanged
    /// with no guards. Hold the returned guards until the process has finished.
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

/// A 0600 temporary helm values file holding secret values; deleted on drop so
/// the secret never outlives the `helm` invocation, even on error.
pub(crate) struct SecretValuesFileGuard {
    path: std::path::PathBuf,
}

impl SecretValuesFileGuard {
    /// Write `pairs` (dotted helm keys -> secret values) into a fresh 0600 temp
    /// file as nested YAML (a JSON document, which helm parses as YAML), created
    /// with restrictive permissions atomically so the secret is never briefly
    /// world-readable.
    fn write(pairs: &[(String, String)]) -> Result<Self> {
        let doc = nest_dotted_keys(pairs);
        let body = serde_json::to_vec(&doc).context("serializing secret helm values")?;

        let mut path = std::env::temp_dir();
        path.push(format!("curie-helm-values-{}.yaml", uuid::Uuid::new_v4()));

        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let mut file = opts
            .open(&path)
            .with_context(|| format!("creating secret helm values file {}", path.display()))?;
        // Belt-and-suspenders on platforms where create-time mode is not honored.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                .with_context(|| format!("securing secret helm values file {}", path.display()))?;
        }
        use std::io::Write;
        file.write_all(&body)
            .with_context(|| format!("writing secret helm values file {}", path.display()))?;
        Ok(Self { path })
    }
}

impl Drop for SecretValuesFileGuard {
    fn drop(&mut self) {
        // Best-effort cleanup; nothing actionable if the temp file is already gone.
        let _ = std::fs::remove_file(&self.path);
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

pub(crate) fn plain(s: impl Into<String>) -> CmdArg {
    CmdArg::Plain(s.into())
}

pub(crate) fn secret_set(key: &str, value: &str) -> CmdArg {
    CmdArg::SecretSet {
        key: key.to_string(),
        value: value.to_string(),
    }
}

/// Mask a secret for display: the first 8 characters, then `***`. Long enough to
/// recognise a token by its prefix (e.g. `xoxb-...`), short enough to leak
/// nothing usable.
pub fn mask_secret(value: &str) -> String {
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

pub struct UpOpts {
    pub common: CommonOpts,
    pub chart: String,
    pub no_expose: bool,
    pub set: Vec<String>,
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
    /// Populated by [`up`] from [`resolve_generated_secrets`]; empty in the pure
    /// argv tests and whenever `--dev` keeps the chart's dev defaults. Delivered
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

pub struct DownOpts {
    pub common: CommonOpts,
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
/// no-op at `cluster up` (#496). Returns None when neither is set non-empty.
pub fn model_credential_env() -> Option<String> {
    if let Some(value) = std::env::var("CURIE_CREDENTIALS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        return Some(value);
    }
    if let Some(value) = std::env::var("CURIE_MODEL_CREDENTIALS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        eprintln!(
            "warning: CURIE_MODEL_CREDENTIALS is deprecated and will be removed in a future \
             release; set CURIE_CREDENTIALS instead."
        );
        return Some(value);
    }
    None
}

/// The helm value key that pins the sandbox runner model in the chart.
const RUNNER_MODEL_KEY: &str = "agentSandbox.runner.model";

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
    validate_web_egress_cidrs(&opts.allow_web_egress)
        .context("invalid --allow-web-egress value")?;
    check_runner_model_conflict(opts.model.as_deref(), &opts.set)?;
    check_github_token_conflict(github_token, clear_github_token, &opts.set)?;
    for host in &opts.allow_egress_host {
        parse_egress_provider(host)?;
    }
    Ok(())
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

/// The canonical model providers `--allow-egress-host` accepts, each paired with
/// the API hostname(s) its runner must reach, in the order shown in help and
/// error text. The single source of truth for both the accepted-provider set and
/// their egress hosts, so adding a provider is a one-line edit here.
///
/// This set is deliberately limited to the providers the runner can drive
/// end-to-end today (`anthropic` via `sk-ant-` keys, `openrouter` via `sk-or-`
/// keys). Opening egress to a host the runner cannot actually talk to gives
/// false confidence, so a provider is only listed once the runner has runtime
/// support for it. When the runner gains that support for additional providers
/// (e.g. the `PROVIDER_BASE_URLS` base-URL providers zhipu/moonshot/deepseek, or
/// native OpenAI/Gemini), layer them in here at the same time so the egress
/// convenience list never advertises a provider the harness cannot use.
///
/// HOSTNAMES, never CIDRs: provider IPs rotate, so they are resolved to narrow
/// host routes at install time (see [`resolve_provider_egress_cidrs`]) instead of
/// baked into this binary where a stale literal would silently break a real model
/// call.
const EGRESS_PROVIDERS: &[(&str, &[&str])] = &[
    ("anthropic", &["api.anthropic.com"]),
    ("openrouter", &["openrouter.ai"]),
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
             model is unreachable. Pass --allow-egress-host <anthropic|openrouter> \
             (or --allow-web-egress <CIDR>) and re-run."
                .to_string(),
        )
    } else {
        None
    }
}

/// The ordered model+egress status lines `up` prints, as (is_warning, message)
/// pairs, derived purely so every credential/egress combination is unit-tested.
/// The web-egress *count* note and the default-route warning stay in the handler
/// (they keep their own tested helpers). `any_egress_opened` folds resolved
/// provider routes, declared web egress, and (under dry-run) the intent to open.
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
    } else if !fake_model {
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
            "Replies will be canned. Set CURIE_CREDENTIALS (an Anthropic API key) and re-run `curie cluster up` to enable the real model.".into(),
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

/// The operator's `--set` arguments split into raw `(key, value)` halves: the
/// single parser behind [`operator_set_keys`] and
/// [`set_passthrough_leaks_github_token`], though not the only reader of this
/// grammar in the file ([`explicit_runner_model`] hand-rolls a last-wins
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
        .filter_map(|part| part.split_once('='))
        .collect()
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

/// The value helm already recorded for `key`, when there is a real one. An
/// empty record is what a `--disconnect` / `--clear-*` wrote and is not a
/// credential; returning `None` for it is what stops a cleared value being
/// resurrected on the next plain `up`.
fn preserved_value(existing: Option<&serde_json::Value>, key: &str) -> Option<String> {
    lookup_dotted(existing?, key).filter(|current| !current.is_empty())
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
    all
}

/// The chart value holding the model credential. Named here so the secret
/// classifier below cannot drift from the key `up_commands` actually masks.
pub(crate) const MODEL_CREDENTIAL_KEY: &str = "agentSandbox.runner.credentials";

/// Emitted alongside [`MODEL_CREDENTIAL_KEY`], and only when a credential is
/// present -- see `up_commands`, which pushes both inside one `if let`.
pub(crate) const FAKE_MODEL_KEY: &str = "agentSandbox.runner.fakeModel";

/// Does a plain `cluster up` carry this key forward when nothing re-passes it?
///
/// The honest half of `curie diff`. `up` does a FULL upgrade, so a key present
/// on the release but absent from `curie.yaml` is normally reset to the chart
/// default -- except for the families [`resolve_preserved_values`] re-supplies,
/// which survive untouched. Reporting those as removals would be the exact
/// "proposing to delete what it did not create" failure ADR-0097 named.
///
/// Reads the same constants `up` reads, so a new preserved family is picked up
/// by both or neither.
pub fn is_preserved_by_up(key: &str) -> bool {
    COMMS_MANAGED_KEYS.contains(&key)
        || GITHUB_APP_MANAGED_KEYS.contains(&key)
        || REQUIRED_SECRETS.iter().any(|(k, _)| *k == key)
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
    if is_preserved_by_up(key) || key == GITHUB_TOKEN_KEY || key == MODEL_CREDENTIAL_KEY {
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
        .map(|p| p.trim().eq_ignore_ascii_case("keep"))
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
    let (ok, out, _err) = run_capture(&cmd).await?;
    if !ok {
        return Ok(Vec::new());
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
pub async fn chart_stateful_components(
    chart: &str,
    o: &CommonOpts,
    value_sets: &[String],
) -> Result<Vec<String>> {
    let mut args = vec![
        plain("template"),
        plain(&o.release),
        plain(chart),
        plain("-n"),
        plain(&o.namespace),
    ];
    for entry in value_sets {
        args.push(plain("--set"));
        args.push(plain(entry));
    }
    let (ok, out, err) = run_capture(&OpsCommand::new("helm", args)).await?;
    if !ok {
        bail!("could not render the target chart to check for removed stateful components: {err}");
    }
    Ok(parse_statefulset_components(&out))
}

/// The component identities of StatefulSets in a multi-document helm render.
///
/// Split-and-parse rather than a regex: a `kind: StatefulSet` line can appear
/// inside an annotation or a ConfigMap payload, and matching that would invent
/// a component the chart does not actually create.
pub fn parse_statefulset_components(rendered: &str) -> Vec<String> {
    let mut components = Vec::new();
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
        if let Some(component) = component {
            if !components.iter().any(|c: &String| c == component) {
                components.push(component.to_string());
            }
        }
    }
    components
}

/// Live stateful components the target chart would not recreate.
///
/// Compares COMPONENTS; reports the resource NAMES, which is what an operator
/// recognises in their own cluster.
///
/// Pure, so the decision this guard turns on is testable without a cluster.
pub fn removed_stateful_components(live: &[(String, String)], rendered: &[String]) -> Vec<String> {
    live.iter()
        .filter(|(component, _)| !rendered.iter().any(|r| r == component))
        .map(|(_, name)| name.clone())
        .collect()
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

    #[test]
    fn components_come_from_the_render() {
        let rendered = format!(
            "{}\n---\n{}",
            render("rustfs", "acme-bot-curie-rustfs"),
            render("postgres", "acme-bot-curie-postgres")
        );
        assert_eq!(
            parse_statefulset_components(&rendered),
            vec!["rustfs", "postgres"]
        );
    }

    /// `kind: StatefulSet` inside a ConfigMap payload is data, not a component.
    /// A regex over the render would invent one; parsing cannot.
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

    /// The real case: the release runs minio, chart 0.6.0 renders rustfs.
    #[test]
    fn a_renamed_store_is_reported_as_a_removal() {
        let live = vec![
            ("minio".to_string(), "acme-bot-minio".to_string()),
            ("postgres".to_string(), "acme-bot-postgres".to_string()),
        ];
        let rendered = vec!["rustfs".to_string(), "postgres".to_string()];
        assert_eq!(
            removed_stateful_components(&live, &rendered),
            vec!["acme-bot-minio"],
            "only the renamed store is lost, and it is named as the operator sees it"
        );
    }

    /// The false positive that a live run exposed, pinned so it cannot return.
    ///
    /// The release was installed with a `nameOverride`, so every resource is
    /// `<override>-<component>` while the chart renders
    /// `<override>-curie-<component>`.
    /// Comparing NAMES reported all four as removals -- postgres, valkey and
    /// clickhouse included -- which would have taught the operator to pass
    /// --allow-stateful-removal by reflex and lose minio for real.
    #[test]
    fn a_name_override_does_not_make_every_component_look_removed() {
        let live = vec![
            ("clickhouse".to_string(), "acme-bot-clickhouse".to_string()),
            ("minio".to_string(), "acme-bot-minio".to_string()),
            ("postgres".to_string(), "acme-bot-postgres".to_string()),
            ("valkey".to_string(), "acme-bot-valkey".to_string()),
        ];
        // What the chart renders WITHOUT the override: different names entirely.
        let rendered = vec![
            "clickhouse".to_string(),
            "postgres".to_string(),
            "rustfs".to_string(),
            "valkey".to_string(),
        ];
        assert_eq!(
            removed_stateful_components(&live, &rendered),
            vec!["acme-bot-minio"],
            "differing resource names must not be mistaken for removed components"
        );
    }

    /// The WIRING, not just the predicate. Removing the `helm_keeps` filter from
    /// `stateful_components_from_list` leaves every `helm_keeps` unit test green
    /// -- that mutation survived, so this test exists to kill it.
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
        let at_risk = stateful_components_from_list(&list);
        assert_eq!(
            at_risk,
            vec![("postgres".to_string(), "acme-bot-postgres".to_string())],
            "a component annotated keep is not at risk and must not be listed"
        );
    }

    /// And the same list WITHOUT the annotation must still surface it, or the
    /// test above would pass against a function that returns nothing at all.
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
            vec![("minio".to_string(), "acme-bot-minio".to_string())]
        );
    }

    /// Helm's own opt-out. A store annotated `keep` survives the upgrade, so
    /// flagging it is a false alarm -- and annotating it is precisely how an
    /// operator detaches a store before a chart renames it.
    #[test]
    fn a_resource_helm_is_told_to_keep_is_not_at_risk() {
        for value in ["keep", "Keep", " keep "] {
            let kept = serde_json::json!({
                "metadata": {"annotations": {"helm.sh/resource-policy": value}}
            });
            assert!(helm_keeps(&kept), "{value:?} must read as keep");
        }
    }

    /// The escape must be narrow: everything else is still at risk.
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

    /// The guard must stay quiet on an ordinary upgrade, or it becomes a flag
    /// everyone passes by reflex and protects nothing.
    #[test]
    fn an_unchanged_component_set_is_not_a_removal() {
        let live = vec![
            ("postgres".to_string(), "r-postgres".to_string()),
            ("minio".to_string(), "r-minio".to_string()),
        ];
        let rendered = vec!["postgres".to_string(), "minio".to_string()];
        assert!(removed_stateful_components(&live, &rendered).is_empty());
    }

    /// A chart ADDING a store is not a removal.
    #[test]
    fn a_new_component_is_not_a_removal() {
        let live = vec![("postgres".to_string(), "r-postgres".to_string())];
        let rendered = vec!["postgres".to_string(), "clickhouse".to_string()];
        assert!(removed_stateful_components(&live, &rendered).is_empty());
    }
}

/// The user-supplied values helm recorded for a release, or `None` when the
/// release does not exist. The read-only half of [`fetch_existing_values`],
/// exposed for `curie diff`.
pub async fn fetch_release_values(o: &CommonOpts) -> Result<Option<serde_json::Value>> {
    fetch_existing_values(o).await
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

/// Finish an already validated up plan with the one live values read and, when
/// requested, resolved provider addresses. This is kept separate from command
/// execution so apply and diff can compare the same completed values.
pub(crate) fn complete_up_opts(
    mut opts: UpOpts,
    existing: Option<&serde_json::Value>,
    github_token: Option<&str>,
    clear_github_token: bool,
    resolve_provider_egress: bool,
) -> Result<UpOpts> {
    if !opts.dev {
        opts.secrets = resolve_generated_secrets(existing, &opts.set)?;
        opts.secrets
            .extend(resolve_preserved_values(existing, &opts.set));
    }
    opts.github_token = resolve_github_token(existing, &opts.set, github_token, clear_github_token);
    if resolve_provider_egress
        && !opts.allow_egress_host.is_empty()
        && opts.resolved_egress_cidrs.is_empty()
    {
        opts.resolved_egress_cidrs =
            resolve_provider_egress_cidrs_for_current_environment(&opts.allow_egress_host)
                .context("resolving named provider egress hosts")?;
    }
    Ok(opts)
}

/// Whether an operator `--set` assigns a NON-EMPTY value to
/// [`GITHUB_TOKEN_KEY`], i.e. whether the complete token is riding in argv.
///
/// The pass-through stays legal (it is verbatim by design and breaking it would
/// break existing operators), but a non-empty one leaks into the process table,
/// shell history and the printed plan, so `up` steers the operator to the
/// private input. An EMPTY assignment is the operator clearing the key by hand:
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

/// `helm get values <release> -n <ns> -o json`: helm's record of the values a
/// prior install supplied. `cluster up` reads it back so an upgrade re-supplies
/// the same generated secrets instead of rotating them.
fn helm_get_values_cmd(o: &CommonOpts) -> OpsCommand {
    OpsCommand::new(
        "helm",
        vec![
            plain("get"),
            plain("values"),
            plain(&o.release),
            plain("-n"),
            plain(&o.namespace),
            plain("-o"),
            plain("json"),
        ],
    )
}

/// The user-supplied values of an existing release, or `None` when the release
/// does not exist yet (or helm cannot reach it -- treated as a fresh install;
/// the subsequent `helm upgrade --install` surfaces any real connectivity
/// error). `helm get values` prints `null` for a release with no user values,
/// which parses to `Value::Null` and yields no reusable secrets.
async fn fetch_existing_values(o: &CommonOpts) -> Result<Option<serde_json::Value>> {
    let (ok, out, _err) = run_capture(&helm_get_values_cmd(o)).await?;
    if !ok {
        return Ok(None);
    }
    Ok(Some(
        serde_json::from_str(out.trim()).unwrap_or(serde_json::Value::Null),
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffParticipation {
    Include,
    Preserve,
}

#[derive(Clone, PartialEq, Eq)]
enum PlannedHelmValues {
    Set {
        expression: String,
        effective: Vec<(String, String)>,
    },
    SecretFile {
        values: Vec<(String, String)>,
        diff: DiffParticipation,
    },
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
            expression: format!("{key}={value}"),
            effective: vec![(key, value)],
        });
    }

    fn set_expression(&mut self, expression: String) {
        let effective = operator_set_entries(std::slice::from_ref(&expression))
            .into_iter()
            .map(|(key, value)| (key.trim().to_string(), value.to_string()))
            .collect();
        self.entries.push(PlannedHelmValues::Set {
            expression,
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
                PlannedHelmValues::Set { expression, .. } => {
                    args.push(plain("--set"));
                    args.push(plain(expression));
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
        plan.set("security.allowDevDefaults", "true");
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
    if let Some(model) = &o.model {
        if explicit_runner_model(&o.set).is_none() {
            plan.set(RUNNER_MODEL_KEY, model);
        }
    }
    for expression in &o.set {
        plan.set_expression(expression.clone());
    }
    plan
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
pub fn status_commands(o: &CommonOpts) -> Vec<OpsCommand> {
    vec![
        helm_status_cmd(o),
        pods_cmd(o),
        svc_cmd(o, "ui"),
        svc_cmd(o, "langfuse-web"),
        kubeconfig_host_cmd(),
    ]
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

fn svc_cmd(o: &CommonOpts, suffix: &str) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("get"),
            plain("svc"),
            plain(format!("{}-{}", o.release, suffix)),
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
/// sweep is scoped by the release ownership label `up` stamped
/// (`curietech.ai/created-by=<release>`) rather than a hardcoded namespace pair,
/// so a pre-existing (unlabeled) namespace is never deleted. `--ignore-not-found`
/// keeps a partial teardown re-runnable and the label selector tolerates zero
/// matches. CRDs are never targeted (retention is by-construction).
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
                plain(format!("curietech.ai/created-by={}", o.release)),
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
/// records THIS release as the creator of `o.namespace`, but ONLY when `up`
/// actually created the namespace (`namespace_existed == false`); an empty vec
/// when the namespace pre-existed, so a namespace `up` merely adopted is never
/// stamped and therefore never swept by a later `down`. A release-scoped label
/// (not a per-invocation run-id) is what lets a separate `down` invocation match
/// what `up` created. `--overwrite` keeps a re-run idempotent, so an `up`
/// interrupted after create but before stamp fails safe toward retention.
fn ownership_label_commands(o: &CommonOpts, namespace_existed: bool) -> Vec<OpsCommand> {
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
pub(crate) async fn run_capture(cmd: &OpsCommand) -> Result<(bool, String, String)> {
    // Materialize any secret values into a private 0600 `-f` file so the secret
    // stays out of the argv/process table. `_secret_files` guards live until the
    // end of this function, so the temp files are removed after `helm` exits
    // (including on error paths below).
    let (cmd, _secret_files) = cmd.materialize_secret_files()?;
    let output = Command::new(&cmd.program)
        .args(cmd.argv())
        .envs(cmd.env.iter().chain(cmd.secret_env.iter()).cloned())
        .output()
        .await
        .with_context(|| format!("failed to invoke `{}`; is it on PATH?", cmd.program))?;
    Ok((
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    ))
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
    if ok {
        step.done(ok_detail);
    } else {
        step.fail("failed");
    }
    for line in out.lines().chain(err.lines()) {
        ui.plumbing(line);
    }
    if !ok {
        // One implementation, shared with the teardown Display message (#1230):
        // an inline second copy of this rule is how the two drifted before.
        let reason = failure_reason(&err);
        ui.failure(&format!("`{}` failed: {reason}", cmd.program));
        bail!("`{}` exited nonzero", cmd.program);
    }
    Ok(out)
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

pub async fn up(
    opts: UpOpts,
    github_token: Option<String>,
    clear_github_token: bool,
) -> Result<ClusterUpOutput> {
    validate_up_inputs(&opts, github_token.as_deref(), clear_github_token)?;
    let resolve_provider_egress = !opts.common.dry_run;
    let existing = if should_read_existing(opts.dev, opts.common.dry_run) {
        require_on_path("helm")?;
        fetch_existing_values(&opts.common).await?
    } else {
        None
    };
    let opts = complete_up_opts(
        opts,
        existing.as_ref(),
        github_token.as_deref(),
        clear_github_token,
        resolve_provider_egress,
    )?;
    let value_plan = up_value_plan(&opts);
    run_prepared_up(opts, value_plan, existing, github_token.as_deref()).await
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
    run_prepared_up(opts, value_plan, existing, github_token.as_deref()).await
}

async fn run_prepared_up(
    opts: UpOpts,
    value_plan: UpValuePlan,
    existing: Option<serde_json::Value>,
    github_token: Option<&str>,
) -> Result<ClusterUpOutput> {
    let ui = crate::ui::ui();
    if !opts.dev {
        let preserved = resolve_preserved_values(existing.as_ref(), &opts.set);
        if !preserved.is_empty() {
            ui.note(&format!(
                "preserving {} value(s) recorded by `cluster comms` / `cluster github-app`; \
                 re-run those verbs only to change them",
                preserved.len()
            ));
        }
        if existing.is_none() && !opts.secrets.is_empty() && !opts.common.dry_run {
            ui.note(&format!(
                "generated strong per-release secrets for {} required chart credential(s); re-running `cluster up` reuses them",
                opts.secrets.len()
            ));
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
        "kubectl -n {} rollout restart deployment -l app.kubernetes.io/instance={},app.kubernetes.io/component=api",
        opts.common.namespace, opts.common.release
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
    if set_passthrough_leaks_github_token(&opts.set) {
        ui.warn("a GitHub credential passed with --set lands in the process table, shell history and the printed plan; use --github-token, or CURIE_GITHUB_TOKEN to keep it out of shell history too");
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
            cmds.extend(ownership_label_commands(&common, false));
        }
        ownership_candidates.push((ns, existed_before));
    }

    // Provider egress is opened iff a provider was named: on a live run
    // resolve_provider_egress_cidrs bails on an empty/failed resolution (so a
    // non-empty allow_egress_host always yields non-empty resolved_egress_cidrs),
    // and under --dry-run resolution is skipped but the intent still counts.
    let any_egress = !opts.allow_egress_host.is_empty() || !opts.allow_web_egress.is_empty();
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
        return Ok(ClusterUpOutput::DryRun(crate::ui::DryRunPlan {
            lines: cmds.iter().map(|cmd| cmd.display()).collect(),
        }));
    }
    require_on_path("helm")?;
    let cl = ui.checklist();
    let label = format!("installing release {}", opts.common.release);
    for cmd in &cmds {
        run_step(&cl, &label, "installed", cmd).await?;
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
        for cmd in ownership_label_commands(&common, false) {
            run_step(&cl, &label, "installed", &cmd).await?;
        }
    }

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
        return Ok(ClusterStatusOutput::DryRun(crate::ui::DryRunPlan {
            lines: status_commands(&opts)
                .iter()
                .map(|cmd| cmd.display())
                .collect(),
        }));
    }
    require_on_path("helm")?;
    require_on_path("kubectl")?;

    // (a) Helm release state -> a bright header line.
    let (helm_ok, helm_out, helm_err) = run_capture(&helm_status_cmd(&opts)).await?;
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
    let (ok, out, _) = run_capture(&pods_cmd(&opts)).await?;
    let (pods, ready, total, unhealthy) = if ok {
        let items: Vec<serde_json::Value> = serde_json::from_str::<serde_json::Value>(&out)
            .ok()
            .and_then(|v| v.get("items").and_then(|i| i.as_array()).cloned())
            .unwrap_or_default();
        collect_pod_summary(&items)
    } else {
        (Vec::new(), 0, 0, Vec::new())
    };

    // (c) URL discovery.
    let host = discover_host().await;
    let urls = vec![
        resolve_service_url(&opts, "ui", "UI", &host, true).await,
        resolve_service_url(&opts, "langfuse-web", "Langfuse", &host, false).await,
    ];

    Ok(ClusterStatusOutput::Status(Box::new(ClusterStatus {
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
    })))
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
        "this uninstalls release '{0}' and deletes the namespaces it created (labeled curietech.ai/created-by={0}), leaving any pre-existing namespaces untouched",
        opts.common.release
    ));
    if !opts.yes
        && !confirm(&format!(
            "This uninstalls release '{0}' and deletes the namespaces it created (labeled curietech.ai/created-by={0}). Continue? [y/N] ",
            opts.common.release
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
            reason
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

/// Discover a Helm release's platform API key by reading it out of the chart
/// Secret (`<release>-secrets`, data key `apiKey`), decoded server-side by
/// kubectl's `base64decode` so the plaintext never lands in argv (#524). The
/// governance verbs use this so they authenticate against a REAL release whose
/// `api.apiKey` was randomized at `cluster up`, instead of silently sending the
/// dev sentinel `curie-dev-key` and 401-ing. An explicit `--api-key`/env still
/// wins (the caller only reaches here when neither was supplied). The value is
/// never printed — it flows straight into the `X-API-Key` header.
pub async fn discover_api_key(namespace: &str, release: &str) -> Result<String> {
    read_release_secret(namespace, release, "apiKey")
        .await
        .ok_or_else(|| {
            api_key_usage_err(format!(
                "could not read the API key from secret {release}-secrets in namespace {namespace}; \
                 pass --api-key or set CURIE_API_KEY to the release's api.apiKey"
            ))
        })
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
/// (`<release>-secrets`, data key `valkeyPassword`). `cluster message` enqueues
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
                "could not read the Valkey password from secret {release}-secrets in namespace \
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

/// Discover a Helm release's Slack bot token from the same chart Secret
/// (`<release>-secrets`, data key `slackBotToken`). In connected mode
/// `cluster message` posts a real placeholder to the workspace with this token so
/// the approval card and resumed reply ride the connected transport, instead of
/// the throwaway stub (#770/ADR-0078). Only reached when a `<release>-dispatcher`
/// is present (a workspace IS connected), so the token is expected to be set; an
/// empty or unreadable value is an actionable error. The value is never printed
/// -- it flows only into the `chat.postMessage` auth header.
pub async fn discover_slack_bot_token(namespace: &str, release: &str) -> Result<String> {
    read_release_secret(namespace, release, "slackBotToken")
        .await
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            slack_bot_token_usage_err(format!(
                "could not read a Slack bot token from secret {release}-secrets in namespace \
                 {namespace}; the workspace may not be connected (run `curie cluster comms \
                 --slack`), or set CURIE_SLACK_BOT_TOKEN"
            ))
        })
}

/// Whether a `<release>-dispatcher` Deployment exists in `namespace` -- i.e. a
/// real Slack workspace is connected (via `curie cluster comms --slack`). In
/// that case `cluster message` posts a real placeholder and routes the approval
/// card + resumed reply over that connected transport rather than a throwaway
/// stub (#770/ADR-0078). A kubectl failure (cluster unreachable, no such
/// namespace) reads as NOT connected, so the caller safely falls back to the
/// stub path instead of failing the whole command. `--ignore-not-found` makes an
/// absent Deployment an empty success, so "connected" is exactly "non-empty
/// output on a zero exit".
pub async fn dispatcher_connected(namespace: &str, release: &str) -> bool {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("deployment"),
            plain(format!("{release}-dispatcher")),
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
                 (kubectl probe for {release}-dispatcher failed: {}); assuming \
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

/// Read one data key out of a release's chart Secret, decoded server-side by
/// kubectl's `base64decode` so the plaintext never lands in argv (#524). `None`
/// when the Secret, the key, or the cluster is unreachable; the caller turns
/// that into an actionable error naming its own escape-hatch flag.
async fn read_release_secret(namespace: &str, release: &str, data_key: &str) -> Option<String> {
    let cmd = OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("secret"),
            plain(format!("{release}-secrets")),
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
    let host = resolve_node_host().await;

    if let Ok((true, ui_json, _)) = run_capture(&svc_cmd(&common, "ui")).await {
        return ui_api_url_from_parts(&ui_json, host.as_deref());
    }

    // No UI. The api service may still be reachable on its own NodePort.
    if let Ok((true, api_json, _)) = run_capture(&svc_cmd(&common, "api")).await {
        if let Some(url) = api_url_from_parts(&api_json, host.as_deref()) {
            return Ok(url);
        }
        return Err(api_url_usage_err(format!(
            "the {release}-ui service is absent (ui.deploy=false?) and {release}-api is not NodePort-exposed, so there is no reachable platform API URL; expose it with --set api.service.type=NodePort, or pass --api-url (e.g. via `kubectl port-forward svc/{release}-api 8123:8000`)"
        )));
    }

    Err(api_url_usage_err(format!(
        "could not read the {release}-ui or {release}-api service in namespace {namespace} to discover the platform API URL; pass --api-url to target the API directly"
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
                    Some(port_forward_hint_with(
                        &self.namespace,
                        &self.name,
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
                    &port_forward_hint_with(
                        &self.namespace,
                        &self.name,
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

async fn resolve_service_url(
    o: &CommonOpts,
    suffix: &str,
    label: &str,
    host: &str,
    api: bool,
) -> ServiceUrl {
    let name = format!("{}-{}", o.release, suffix);
    let mk = |kind| ServiceUrl {
        label: label.to_string(),
        name: name.clone(),
        namespace: o.namespace.clone(),
        api,
        kind,
    };
    let (ok, out, _) = match run_capture(&svc_cmd(o, suffix)).await {
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
    /// `local = if port == 0 { 8080 } else { port }`.
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
            local: if port == 0 { 8080 } else { port },
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

/// The platform API service's port (`{release}-api`, `api.service.port` in the
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
        return row(None, Some(format!("service {}-ui not found", o.release)));
    };
    match ui_api_url_from_parts(ui_svc_json, host) {
        Ok(url) => row(Some(url), None),
        // Any other failure -- ClusterIP / `--no-expose` (a supported install
        // mode), an unassigned nodePort, an unreadable service, or an
        // unresolvable host -- still leaves a way in: port-forward the API
        // service directly.
        Err(_) => row(
            None,
            Some(port_forward_hint(
                &o.namespace,
                &format!("{}-api", o.release),
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
    suffix: &str,
    name: &str,
    svc_json: Option<&str>,
    host: Option<&str>,
    api: bool,
) -> crate::observability::Endpoint {
    let svc_name = format!("{}-{}", o.release, suffix);
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
async fn fetch_service(o: &CommonOpts, suffix: &str) -> Option<String> {
    match run_capture(&svc_cmd(o, suffix)).await {
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
    let (host, ui_svc, langfuse_svc) = tokio::join!(
        resolve_node_host(),
        fetch_service(opts, "ui"),
        fetch_service(opts, "langfuse-web"),
    );
    vec![
        service_surface(
            opts,
            "ui",
            "Curie Console",
            ui_svc.as_deref(),
            host.as_deref(),
            true,
        ),
        service_surface(
            opts,
            "langfuse-web",
            "Langfuse UI (traces / cost / evals)",
            langfuse_svc.as_deref(),
            host.as_deref(),
            false,
        ),
        api_base_endpoint(opts, ui_svc.as_deref(), host.as_deref()),
    ]
}

/// The read-only commands `curie cluster observability` runs (and prints under
/// `--dry-run`).
///
/// A superset of what actually runs, not a 1:1 trace: `resolve_node_host` only
/// falls through to `nodes_cmd()` when `kubeconfig_host_cmd()` yields no host.
pub fn observability_commands(o: &CommonOpts) -> Vec<OpsCommand> {
    vec![
        kubeconfig_host_cmd(),
        nodes_cmd(),
        svc_cmd(o, "ui"),
        svc_cmd(o, "langfuse-web"),
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
        return Ok(crate::observability::ObservabilityOutput::DryRun(
            crate::ui::DryRunPlan {
                lines: observability_commands(&opts)
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

    #[test]
    fn up_defaults_expose_ui_and_langfuse() {
        let cmds = up_commands(&UpOpts {
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
        assert_eq!(cmds.len(), 1);
        let line = cmds[0].display();
        assert_eq!(
            line,
            "helm upgrade --install curie charts/curie -n curie --create-namespace \
             --set ui.service.type=NodePort --set langfuse.web.service.type=NodePort"
        );
    }

    #[test]
    fn up_no_expose_drops_the_nodeport_sets() {
        let cmds = up_commands(&UpOpts {
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec!["worker.replicas=2".into(), "dispatcher.deploy=false".into()],
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: true,
            set: vec!["agentSandbox.runner.model=z-ai/glm-5.2".into()],
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["192.0.2.10/32".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec!["anthropic".into()],
            resolved_egress_cidrs: vec!["192.0.2.10/32".into()],
            chart: "charts/curie".into(),
            secrets: vec![],
            dev: false,
            no_expose: false,
            set: vec![],
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
            allow_egress_host: vec![],
            resolved_egress_cidrs: vec![],
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
        // Label-selector-scoped delete keyed on THIS release's ownership label.
        assert_eq!(
            sweep,
            "kubectl delete namespace -l curietech.ai/created-by=prod-release --ignore-not-found"
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
    //   fn ownership_label_commands(o: &CommonOpts, namespace_existed: bool) -> Vec<OpsCommand>
    //
    // It returns the `kubectl label namespace` stamp step ONLY when `up` created
    // the namespace (namespace_existed == false); an empty vec when the namespace
    // pre-existed. `up()` gates the runtime probe (mirrors the resolve_generated_secrets
    // existing/fresh split), keeping this builder pure and unit-testable.
    #[test]
    fn up_stamps_ownership_label_when_namespace_created() {
        let cmds = ownership_label_commands(&common_distinct_release(), false);
        assert_eq!(cmds.len(), 1);
        // namespace arg is the namespace; the label VALUE is the release.
        assert_eq!(
            cmds[0].display(),
            "kubectl label namespace agent-ns curietech.ai/created-by=prod-release --overwrite"
        );
    }

    #[test]
    fn up_does_not_stamp_ownership_label_when_namespace_preexisting() {
        let cmds = ownership_label_commands(&common_distinct_release(), true);
        assert!(
            cmds.is_empty(),
            "a pre-existing namespace must not be stamped (would adopt then delete pre-existing state): {:?}",
            cmds.iter().map(OpsCommand::display).collect::<Vec<_>>()
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
            "kubectl delete namespace -l curietech.ai/created-by=prod-release --ignore-not-found"
        );
        // #707 ownership-scope invariant: the sweep stays keyed on THIS release's
        // label and is never widened to an unconditional namespace delete.
        assert!(
            cmd.contains("curietech.ai/created-by=prod-release"),
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
            "kubectl delete namespace -l curietech.ai/created-by=prod-release --ignore-not-found";
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
            cmd.contains("curietech.ai/created-by=prod-release"),
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
    // connection reset, context deadline exceeded). Bare "unreachable" and
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
            shown.contains("curietech.ai/created-by=prod-release"),
            "the human message must carry the label-scoped resume command: {shown}"
        );

        // --json path: the fix carries the same label-scoped resume command.
        let fix = fix.expect("a fail-forward teardown carries a resume command");
        assert!(
            fix.contains("curietech.ai/created-by=prod-release"),
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
            shown.contains("curietech.ai/created-by=prod-release"),
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
            fix.contains("curietech.ai/created-by=prod-release"),
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
        let cmds = status_commands(&common());
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
    fn mask_secret_shows_eight_then_stars() {
        assert_eq!(mask_secret("xoxb-abcdefghijk"), "xoxb-abc***");
        assert_eq!(mask_secret("short"), "short***");
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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

    #[test]
    fn up_injects_generated_secrets_via_values_file_not_argv() {
        // Success criterion: a missing secret's generated value lands in the
        // private -f values file, never in the executed argv / process table.
        let cmds = up_commands(&UpOpts {
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
            common: common(),
            github_token: GithubTokenPlan::Untouched,
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
        // ClusterIP: not node-exposed, so the caller must port-forward. The local
        // port mirrors the service port.
        let clusterip = r#"{"spec":{"type":"ClusterIP","ports":[{"port":80}]}}"#;
        assert_eq!(
            resolve_service_endpoint(clusterip, "10.0.0.5", true),
            ServiceEndpoint::PortForwardHint {
                local: 80,
                port: 80
            }
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
            port_forward_hint("curie", "curie-ui", 80, 80, "/?api=1"),
            "kubectl -n curie port-forward svc/curie-ui 80:80  then http://localhost:80/?api=1"
        );
        // The 0-port fallback surfaces local 8080 while still forwarding to 0.
        assert_eq!(
            port_forward_hint("curie", "curie-langfuse-web", 8080, 0, ""),
            "kubectl -n curie port-forward svc/curie-langfuse-web 8080:0  then http://localhost:8080"
        );
    }

    #[test]
    fn api_base_endpoint_maps_ui_service_to_a_non_browsable_api_endpoint() {
        // A NodePort ui service resolves to the UI /api proxy URL (#360) and is
        // NEVER browsable -- it is an agent target, not a webapp.
        let ep = api_base_endpoint(&common(), Some(NODEPORT_SVC), Some("10.0.0.5"));
        assert_eq!(ep.name, "Curie API");
        assert_eq!(ep.url.as_deref(), Some("http://10.0.0.5:31234/api"));
        assert_eq!(ep.note, None);
        assert!(!ep.browsable);
    }

    #[test]
    fn api_base_endpoint_degrades_to_a_note_when_the_ui_service_is_unreadable() {
        // Unreadable ui service: degrade to a note endpoint rather than failing
        // the whole command, and never smuggle the message into `url`.
        let ep = api_base_endpoint(&common(), Some(""), Some("10.0.0.5"));
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
        let ep = api_base_endpoint(&common(), None, Some("10.0.0.5"));
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
        let ep = api_base_endpoint(&common(), Some(CLUSTERIP_SVC), Some("10.0.0.5"));
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
            api_base_endpoint(&common(), None, Some("10.0.0.5")),
            api_base_endpoint(&common(), Some(CLUSTERIP_SVC), Some("10.0.0.5")),
            api_base_endpoint(&common(), Some(""), Some("10.0.0.5")),
            api_base_endpoint(&common(), Some(NODEPORT_SVC), None),
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
        let ep = api_base_endpoint(&common(), Some(NODEPORT_SVC), None);
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
        let lines: Vec<String> = observability_commands(&common())
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
        // The two runner-drivable providers map to their canonical API host(s).
        assert_eq!(
            provider_egress_hosts("anthropic").unwrap().to_vec(),
            vec!["api.anthropic.com"]
        );
        assert_eq!(
            provider_egress_hosts("openrouter").unwrap().to_vec(),
            vec!["openrouter.ai"]
        );

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
        for p in ["anthropic", "openrouter"] {
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
        for p in ["anthropic", "openrouter"] {
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
        // OpenRouter share 1.1.1.1 to prove deduplication; Anthropic also
        // yields an IPv6 address to prove the v4/v6 mix. All addresses are
        // globally routable so they survive the split-horizon guard.
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
                other => panic!("unexpected host {other}"),
            })
        };
        let providers = vec!["anthropic".to_string(), "openrouter".to_string()];
        let cidrs = resolve_provider_egress_cidrs(&providers, resolve).unwrap();
        // Deduplicated (one 1.1.1.1/32) and sorted for a stable install argv.
        assert_eq!(
            cidrs,
            vec!["1.0.0.1/32", "1.1.1.1/32", "2606:4700::1111/128"]
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
    fn up_emits_resolved_provider_cidrs_before_web_egress_contiguously() {
        // Resolved provider CIDRs take the first slots (in order), then declared
        // web destinations continue contiguously -- one array, no gaps.
        let cmds = up_commands(&UpOpts {
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
            common: common(),
            github_token: plan,
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
            common: common(),
            github_token: GithubTokenPlan::Set(GH_SENTINEL.into()),
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
            common: common(),
            github_token: GithubTokenPlan::Set(GH_SENTINEL.into()),
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
