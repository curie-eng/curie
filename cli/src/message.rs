//! Shared target noun message forms: drive the DEPLOYED Kubernetes release end
//! to end from the CLI with zero Slack contact.
//!
//! Product rationale: a developer building an agent for someone else's Slack
//! workspace can exercise the entire deployed machinery (Valkey queue -> worker
//! -> claimed sandbox -> the real skill -> the reply) without any Slack access,
//! tokens, or workspace. It is the retained chat helper engine, backed by a
//! local Slack Web API stub plus the frozen `QueuedTurn` enqueue and the
//! ack-based completion signal, with Kubernetes-aware auto-plumbing bolted on
//! top:
//!
//! 1. Self-managed `kubectl port-forward`s (children of this process, killed on
//!    exit) reach the in-cluster Valkey (for the enqueue) and API (for the
//!    default-channel lookup) with no manual setup.
//! 2. The stub binds `0.0.0.0` and advertises a routable host (either
//!    `--listen-host` or the local IP the kernel would use to reach the cluster)
//!    so the in-cluster worker can post its placeholder edits back to it.
//! 3. Each enqueued turn carries its reply endpoint (this stub's URL) on the
//!    queue payload's reply handle (issue #19), so the worker finalizes THIS
//!    turn against the stub without re-pointing its worker-global Slack setting.
//!    A real Slack workspace (whose turns carry no endpoint and so use the
//!    worker default) and this driver can therefore run against one worker at
//!    once, instead of contending for a single `worker.slackApiBaseUrl`.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use curie_aci_protocol::QueuedTurn;
use redis::aio::MultiplexedConnection;

use crate::api::{Agent, ApiClient};
use crate::chat::{
    await_reply, await_resume, continue_hint_line, continue_hint_long_line, parse_approval_id,
    resolve_targets, Outcome, SlackStub,
};
use crate::evals::{EvalCase, EvalSuite, ExpectedStatus, LoadedEval};
use crate::ops::{plain, require_on_path, run_capture, OpsCommand};
use crate::queue::{self, connect, diagnostics, synthetic_turn, xadd};
use crate::state::{save_turn, TurnContext, TurnVerb};

pub const DEFAULT_STREAM: &str = queue::DEFAULT_STREAM;
pub const DEFAULT_USER: &str = "U-curie-message";
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;
/// Fixed stub port so the advertised URL is deterministic; `0` picks ephemeral.
pub const DEFAULT_LISTEN_PORT: u16 = 8155;
/// Local port the Valkey port-forward binds. Chosen to dodge the compose dev
/// stack, which squats 26379.
pub const DEFAULT_VALKEY_LOCAL_PORT: u16 = 56381;
/// Local port the API port-forward binds (only used for the default-channel
/// lookup when `--channel` is omitted).
pub const DEFAULT_API_LOCAL_PORT: u16 = 8123;
/// The chart's default Valkey password (values.yaml `valkey.password`).
pub const DEFAULT_VALKEY_PASSWORD: &str = "valkeypass";
/// The chart's default platform API key (values.yaml `api.apiKey`).
pub const DEFAULT_API_KEY: &str = "curie-dev-key";

/// clap `value_parser` for every `--api-key` / `$CURIE_API_KEY` declaration.
///
/// An empty-string credential is absent, not "explicitly supplied" (issue #540).
/// clap reports an env var set to `""` as PRESENT, so without this an empty
/// value reaches [`crate::state::apply_continue`] as a user-supplied key,
/// defeats its sentinel comparison against [`DEFAULT_API_KEY`], and silently
/// sends a blank key onward instead of falling back. Normalizing at the parser
/// -- the one seam every `--api-key` declaration shares -- is what makes the
/// rule hold on the `--continue` and non-`--continue` paths alike, rather than
/// only where a sentinel happens to be compared.
///
/// An explicit `--api-key ""` must behave exactly as an omitted flag, which is
/// why the env source is consulted HERE rather than left to clap: clap resolves
/// an explicit flag ahead of `env`, so by the time the parser runs the env
/// source is already out of the running. Without this, `--api-key ""` under a
/// real `$CURIE_API_KEY` would send the well-known dev sentinel instead of the
/// operator's key.
///
/// Mirrors the rule already settled in `ops.rs::resolve_up_credentials`,
/// `local.rs::model_mode_from_env`, and `secrets.rs::save_value`.
pub fn api_key_or_default(raw: &str) -> Result<String, String> {
    Ok(resolve_api_key(raw, env::var("CURIE_API_KEY").ok()))
}

/// The pure core of [`api_key_or_default`], with the env source passed in so the
/// resolution is unit-testable without mutating this process's environment.
/// Same shape as `ops.rs::resolve_up_credentials`.
fn resolve_api_key(raw: &str, env_value: Option<String>) -> String {
    if !raw.is_empty() {
        return raw.to_string();
    }
    env_value
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_API_KEY.to_string())
}

/// An empty `--thread` is not a thread (#540's "empty is unset", applied to the
/// thread ts): passed on, it enqueues a nonsense `conversation_id: ""` and puts a
/// literal `"thread_ts": ""` in an outbound chat.postMessage body.
///
/// Applied at the [`message`] entry point every tier and transport funnels
/// through, and again at the connected-transport leaf
/// ([`enqueue_over_connected_transport`]), which is `pub` for integration tests
/// and so cannot assume its callers normalized. The filter is idempotent, so
/// applying it twice costs nothing.
fn normalize_thread(thread: Option<String>) -> Option<String> {
    thread.filter(|ts| !ts.is_empty())
}

/// clap `value_parser` for the CLUSTER tier's `--api-key` / `--valkey-password`
/// (issue #786).
///
/// The local/compose tier binds the dev constants as clap defaults because that
/// tier does not generate secrets. The cluster tier must not: `cluster up`
/// randomizes both credentials per release, so a defaulted dev sentinel reaches
/// a real install and 401s (API) or fails Valkey auth. These declarations
/// therefore carry NO `default_value` and land as `Option<String>`; an empty
/// string means absent, exactly as [`api_key_or_default`] settled for #540, and
/// the handler reads the real value out of the release's Secret instead.
pub fn cluster_api_key(raw: &str) -> Result<String, String> {
    Ok(resolve_supplied_credential(
        raw,
        env::var("CURIE_API_KEY").ok(),
    ))
}

/// Cluster-tier `--valkey-password` parser; see [`cluster_api_key`].
pub fn cluster_valkey_password(raw: &str) -> Result<String, String> {
    Ok(resolve_supplied_credential(
        raw,
        env::var("CURIE_VALKEY_PASSWORD").ok(),
    ))
}

/// The pure core of the cluster credential parsers, with the env source passed
/// in so it is unit-testable without mutating this process's environment. An
/// explicit non-empty flag wins, then a non-empty env value, and an empty
/// result means "nothing was supplied" (the caller discovers it instead).
fn resolve_supplied_credential(raw: &str, env_value: Option<String>) -> String {
    if !raw.is_empty() {
        return raw.to_string();
    }
    env_value
        .filter(|value| !value.is_empty())
        .unwrap_or_default()
}

/// Resolve one cluster-tier credential: an explicit flag/env value wins,
/// otherwise read it from the release (issue #786).
///
/// `--dry-run` never contacts the cluster, so it keeps the dev default: the plan
/// it prints does not carry the credential, and discovering a real secret just
/// to discard it would break dry-run's offline contract.
pub async fn resolve_cluster_credential<F, Fut>(
    supplied: Option<String>,
    dry_run: bool,
    dev_default: &str,
    discover: F,
) -> Result<String>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<String>>,
{
    match supplied.filter(|value| !value.is_empty()) {
        Some(value) => Ok(value),
        None if dry_run => Ok(dev_default.to_string()),
        None => discover().await,
    }
}

/// Local mode (`--local`): the compose Valkey's published host port
/// (`compose.dev.yaml`), where the CLI enqueues and the compose worker consumes.
pub const DEFAULT_LOCAL_VALKEY_PORT: u16 = 26379;
/// Local mode: the compose API's published host port (`compose.dev.yaml`
/// `curie-api`), reached directly (routers mount at root, so no `/api`).
pub const DEFAULT_LOCAL_API_URL: &str = "http://localhost:28000";
/// Local mode: the fixed port the stub binds. It MUST equal the port in the
/// compose worker's `SLACK_API_BASE_URL` (`http://localhost:8155/api/`) so the
/// containerized worker's placeholder edits reach this stub. Same value as the
/// cluster-mode `DEFAULT_LISTEN_PORT`; the `local_stub_port_matches_listen_port`
/// test pins the coupling.
pub const DEFAULT_LOCAL_STUB_PORT: u16 = DEFAULT_LISTEN_PORT;

/// Env override for the host the local-mode reply stub advertises to the compose
/// worker (issue #680). Set it (e.g. `host.docker.internal`, or any routable
/// host) for a topology this binary cannot infer from its own target OS -- most
/// notably Docker Desktop running on a Linux host.
pub const LOCAL_STUB_HOST_ENV: &str = "CURIE_LOCAL_STUB_HOST";

/// How the local-mode Slack reply stub binds and advertises itself to the
/// compose worker.
#[derive(Debug, Clone, PartialEq)]
pub struct LocalStubBinding {
    /// Interface the stub binds. `127.0.0.1` shares the host loopback with a
    /// native-Linux host-networked worker; `0.0.0.0` accepts the off-loopback
    /// connection a Docker-Desktop VM worker makes.
    pub bind_host: String,
    /// Host advertised to the worker inside the reply-endpoint URL.
    pub advertise_host: String,
}

/// Resolve how the local reply stub must bind and advertise so the compose
/// worker can reach it (issue #680).
///
/// The compose worker runs `network_mode: host`. On native Linux Docker that
/// shares the host's loopback, so `localhost` reaches the stub and `127.0.0.1`
/// is the safe, loopback-only bind. Under Docker Desktop (macOS) `network_mode:
/// host` is emulated inside the Docker VM, so the worker's `localhost` is the
/// VM's loopback -- NOT the Mac host where this CLI bound the stub. There the
/// worker reaches the host only via `host.docker.internal`, and the stub must
/// bind `0.0.0.0` to accept that off-loopback connection. Without this, every
/// synthetic turn's reply POST from the worker lands nowhere and the zero-Slack
/// loop never completes on macOS (real-Slack turns are unaffected).
///
/// `CURIE_LOCAL_STUB_HOST` overrides the advertised host for any topology this
/// binary cannot infer from its target OS (e.g. Docker Desktop on Linux); an
/// explicit override also binds `0.0.0.0`, since a non-loopback advertised host
/// is only reachable off the loopback.
///
/// Pure (env value + target OS passed in) so the selection is unit-testable
/// without mutating this process's environment or platform.
fn resolve_local_stub_binding(env_override: Option<String>, is_macos: bool) -> LocalStubBinding {
    if let Some(host) = env_override.filter(|value| !value.is_empty()) {
        return LocalStubBinding {
            bind_host: "0.0.0.0".to_string(),
            advertise_host: host,
        };
    }
    if is_macos {
        return LocalStubBinding {
            bind_host: "0.0.0.0".to_string(),
            advertise_host: DOCKER_INTERNAL_HOST.to_string(),
        };
    }
    LocalStubBinding {
        bind_host: "127.0.0.1".to_string(),
        advertise_host: "localhost".to_string(),
    }
}

/// Process-level wrapper over [`resolve_local_stub_binding`] reading the real
/// `CURIE_LOCAL_STUB_HOST` and this binary's target OS.
fn local_stub_binding() -> LocalStubBinding {
    resolve_local_stub_binding(
        env::var(LOCAL_STUB_HOST_ENV).ok(),
        cfg!(target_os = "macos"),
    )
}

/// The reply-endpoint URL the local stub advertises, built the same way the
/// stub's own `base_api_url` is (`http://{host}:{port}/api/`).
fn local_stub_reply_endpoint(advertise_host: &str) -> String {
    format!("http://{advertise_host}:{DEFAULT_LOCAL_STUB_PORT}/api/")
}

/// In-cluster service ports the port-forwards target.
const VALKEY_REMOTE_PORT: u16 = 6379;
pub const API_REMOTE_PORT: u16 = 8000;

/// Options for the shared target noun message forms, mirroring their clap
/// flags.
pub struct MessageOpts {
    pub text: String,
    pub channel: Option<String>,
    pub thread: Option<String>,
    pub namespace: String,
    pub release: String,
    pub chart: String,
    /// Host the stub advertises to the worker; `None` auto-detects the local IP.
    pub listen_host: Option<String>,
    pub listen_port: u16,
    pub valkey_local_port: u16,
    pub valkey_password: String,
    pub api_local_port: u16,
    pub api_key: String,
    pub user: String,
    pub stream: String,
    pub timeout_secs: u64,
    pub dry_run: bool,
    /// Local mode: drive the compose stack (`curie local up`) instead of a
    /// Kubernetes release. No kubectl/helm/port-forwards/wiring; enqueue straight
    /// to the compose Valkey and let the containerized worker answer.
    pub local: bool,
    /// Local mode only: platform API base URL for the channel lookup. None uses
    /// the compose API default ([`DEFAULT_LOCAL_API_URL`]).
    pub api_url: Option<String>,
}

/// Hand-written rather than derived: this type is `pub` in a `pub` module, so
/// `Default` is public API and its values have to be the ones the crate actually
/// declares. A derive would zero them, and those zeros are not benign --
/// `timeout_secs: 0` is an instant deadline, and `api_key: ""` defeats the
/// sentinel comparison against [`DEFAULT_API_KEY`] in
/// [`crate::state::apply_continue`] that issue #540 exists to protect.
///
/// The genuinely-empty fields (`text`, `channel`, `thread`, `listen_host`,
/// `user`, `stream`, `dry_run`, `local`, `api_url`) have no crate-level default:
/// they are per-invocation values a caller must supply.
impl Default for MessageOpts {
    fn default() -> Self {
        Self {
            text: String::new(),
            channel: None,
            thread: None,
            namespace: "curie".to_string(),
            release: "curie".to_string(),
            chart: "charts/curie".to_string(),
            listen_host: None,
            listen_port: DEFAULT_LISTEN_PORT,
            valkey_local_port: DEFAULT_VALKEY_LOCAL_PORT,
            valkey_password: DEFAULT_VALKEY_PASSWORD.to_string(),
            api_local_port: DEFAULT_API_LOCAL_PORT,
            api_key: DEFAULT_API_KEY.to_string(),
            user: String::new(),
            stream: String::new(),
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            dry_run: false,
            local: false,
            api_url: None,
        }
    }
}

/// The tier string for a [`TurnVerb`], as surfaced in the `--json` `tier` field
/// and the human resolve hint.
fn tier_str(verb: TurnVerb) -> &'static str {
    match verb {
        TurnVerb::Local => "local",
        TurnVerb::Cluster => "cluster",
    }
}

/// Write `.curie/last-turn.json` for this turn WITHOUT printing the continue
/// hint. Called before the (potentially long) approval wait so an interrupted or
/// closed terminal still leaves a thread `message --continue` can recover (#766);
/// the terminal paths then call [`persist_and_hint`], which rewrites the identical
/// context and prints the hint once, at the end.
fn persist_turn_quietly(opts: &MessageOpts, verb: TurnVerb, channel: &str, thread_ts: &str) {
    if let Err(err) = save_turn_context(opts, verb, channel, thread_ts) {
        crate::ui::ui().warn(&format!("could not save turn context: {err}"));
    }
}

/// The one place the `TurnContext` is built and written. Idempotent: the same
/// turn writes the same file, so persisting up front and again at the terminal
/// records identical state.
fn save_turn_context(
    opts: &MessageOpts,
    verb: TurnVerb,
    channel: &str,
    thread_ts: &str,
) -> Result<()> {
    let ctx = TurnContext::from_turn(
        opts,
        verb,
        channel,
        thread_ts,
        // Empty is unset (#540): otherwise this records an `api_key_env` that
        // resolves to nothing on the next `--continue`.
        env::var("CURIE_API_KEY").ok().filter(|v| !v.is_empty()),
    );
    let cwd = env::current_dir().context("resolving the current working directory")?;
    save_turn(&cwd, &ctx)
}

fn persist_and_hint(opts: &MessageOpts, verb: TurnVerb, channel: &str, thread_ts: &str) {
    let ui = crate::ui::ui();
    let verb_str = format!("{} message", tier_str(verb));
    match save_turn_context(opts, verb, channel, thread_ts) {
        Ok(()) => ui.note(&continue_hint_line(&verb_str)),
        Err(err) => {
            ui.warn(&format!("could not save turn context: {err}"));
            ui.note(&continue_hint_long_line(&verb_str, channel, thread_ts));
        }
    }
}

// ---------------------------------------------------------------------------
// Pure command builders (unit-tested below)
// ---------------------------------------------------------------------------

/// `kubectl -n <ns> port-forward svc/<release>-<suffix> <local>:<remote>`.
pub fn port_forward_command(
    namespace: &str,
    release: &str,
    suffix: &str,
    local_port: u16,
    remote_port: u16,
) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("port-forward"),
            plain(format!("svc/{release}-{suffix}")),
            plain(format!("{local_port}:{remote_port}")),
        ],
    )
}

/// The `/api/` base URL the worker posts its placeholder edits to.
fn advertised_url(host: &str, port: u16) -> String {
    format!("http://{host}:{port}/api/")
}

/// Local mode: the Valkey URL the CLI enqueues onto -- the compose Valkey on its
/// published host port, authenticated with the same password the compose worker
/// uses. Pure so the construction is unit-tested without a live Valkey.
pub fn local_valkey_url(password: &str) -> String {
    format!("redis://:{password}@localhost:{DEFAULT_LOCAL_VALKEY_PORT}")
}

/// Local mode: the platform API base for the channel lookup -- an explicit
/// `--api-url` wins, else the compose API default.
pub fn local_api_base(api_url: Option<&str>) -> String {
    api_url.unwrap_or(DEFAULT_LOCAL_API_URL).to_string()
}

/// Pick the channel to send as: an explicit `--channel` wins; otherwise the sole
/// deployed `(agent, channel)` PAIR. Selection counts pairs rather than agents
/// (ADR-0116) because one agent may answer on several channels, and picking a
/// channel is what this returns. Zero or multiple pairs is an error naming
/// them and requiring `--channel`, because the worker binds a channel to an
/// agent by exact equality -- guessing would silently route nowhere.
pub fn select_channel(agents: &[Agent], explicit: Option<&str>) -> Result<String> {
    if let Some(channel) = explicit {
        return Ok(channel.to_string());
    }
    let pairs: Vec<(&str, &str)> = agents
        .iter()
        .flat_map(|a| {
            a.channels
                .iter()
                .map(move |c| (a.name.as_str(), c.address.as_str()))
        })
        .collect();
    match pairs.as_slice() {
        [] => bail!(
            "no agents are deployed on the platform API; deploy one with `curie local deploy` \
             or `curie cluster deploy`, or pass --channel <id>"
        ),
        [(_, only)] => Ok((*only).to_string()),
        many => {
            let listed = many
                .iter()
                .map(|(name, address)| format!("{name} -> {address}"))
                .collect::<Vec<_>>()
                .join(", ");
            bail!("multiple agents are deployed; pass --channel <id> to pick one ({listed})")
        }
    }
}

/// Parse a kubeconfig `cluster.server` URL into its host and optional raw port,
/// stripping the scheme and any path and correctly handling bracketed IPv6
/// authorities (`https://[::1]:6443` -> ("::1", Some("6443"))). Returns `None`
/// when no host remains. The port is returned unparsed; callers decide how to
/// default or validate it.
pub(crate) fn split_server_url(server: &str) -> Option<(&str, Option<&str>)> {
    let rest = server
        .strip_prefix("https://")
        .or_else(|| server.strip_prefix("http://"))
        .unwrap_or(server);
    let authority = rest.split('/').next().unwrap_or(rest).trim();
    if authority.is_empty() {
        return None;
    }
    let (host, port) = if let Some(after_bracket) = authority.strip_prefix('[') {
        // Bracketed IPv6: the host is between '[' and ']'; an optional ':port'
        // may follow the closing bracket.
        let (host, tail) = after_bracket.split_once(']')?;
        let port = tail.strip_prefix(':').filter(|p| !p.is_empty());
        (host, port)
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) if !h.is_empty() => (h, Some(p)),
            _ => (authority, None),
        }
    };
    let host = host.trim();
    (!host.is_empty()).then_some((host, port))
}

/// Split a kubeconfig server URL into (host, port), defaulting the port from the
/// scheme when absent (`https://10.1.2.3:6443` -> `("10.1.2.3", 6443)`). Used
/// only to pick a UDP-connect target for local-IP detection, so the exact port
/// barely matters (any routable port to the same host selects the same source
/// interface).
pub fn server_host_and_port(server: &str) -> Option<(String, u16)> {
    let default_port = if server.starts_with("http://") {
        80
    } else {
        443
    };
    let (host, port) = split_server_url(server)?;
    let port = match port {
        Some(p) => p.parse().ok()?,
        None => default_port,
    };
    Some((host.to_string(), port))
}

/// The ordered command lines (plus the stub URL and enqueue description) that a
/// real run would execute, for `--dry-run`. Pure so the rendering is testable.
/// The reply routes back to the stub via the per-turn endpoint on the queue
/// payload (issue #19), so there is no worker-global `helm upgrade` to render.
pub fn dry_run_lines(opts: &MessageOpts, advertise_host: &str) -> Vec<String> {
    let mut cmds: Vec<OpsCommand> = vec![port_forward_command(
        &opts.namespace,
        &opts.release,
        "valkey",
        opts.valkey_local_port,
        VALKEY_REMOTE_PORT,
    )];
    if opts.channel.is_none() {
        cmds.push(port_forward_command(
            &opts.namespace,
            &opts.release,
            "api",
            opts.api_local_port,
            API_REMOTE_PORT,
        ));
    }
    let url = advertised_url(advertise_host, opts.listen_port);
    let mut lines: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
    lines.push(format!("stub advertised at {url}"));
    let channel = opts
        .channel
        .clone()
        .unwrap_or_else(|| "<the sole bound (agent, Slack channel) pair>".to_string());
    lines.push(format!(
        "enqueue a synthetic QueuedTurn (reply endpoint {url}) for channel {channel} \
         on stream {}",
        opts.stream
    ));
    lines.push(connected_transport_dry_run_note());
    lines
}

/// The machine-readable reply object for `local`/`cluster message --json`
/// (issue #353): the model's reply text (null when the worker finished without
/// editing the placeholder), the thread the turn ran under, and whether a reply
/// was captured. Pure so it stays contract-testable against
/// `cli/schema/message.schema.json`.
pub fn message_reply_json(thread: &str, reply: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "reply": reply,
        "thread": thread,
        "finalized": reply.is_some(),
    })
}

/// The machine-readable object for a `local`/`cluster message --json` **timeout**
/// (issue #354): no reply was captured before the deadline, so `reply` is null,
/// `finalized` is false, and `timed_out` marks the terminal state distinctly from
/// a no-edit completion. Emitted just before the transient exit so a `--json`
/// caller gets a structured line, not empty stdout. Pure so it stays
/// contract-testable against `cli/schema/message.schema.json`.
pub fn message_timeout_json() -> serde_json::Value {
    serde_json::json!({
        "reply": serde_json::Value::Null,
        "finalized": false,
        "timed_out": true,
    })
}

/// The machine-readable object for a turn that ended **awaiting approval** (#529):
/// the worker posted an approval card and parked, so the turn is not finalized
/// (`finalized` false) and `awaiting_approval` marks the terminal state distinctly
/// from a timeout. `reply` carries the card's placeholder text if one was seen.
/// The persisted `Approval` holds THIS run's ephemeral CLI reply endpoint, so the
/// resumed reply will strand once the command exits. Pure so it stays
/// contract-testable against `cli/schema/message.schema.json`.
pub fn message_awaiting_approval_json(thread: &str, reply: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "reply": reply,
        "thread": thread,
        "finalized": false,
        "awaiting_approval": true,
    })
}

/// The machine-readable object for a turn handed to the **connected Slack
/// transport** (#770/ADR-0078): the turn was enqueued and its reply lands in the
/// workspace, so `status` is `enqueued` and the payload names the channel and
/// thread to watch rather than carrying a reply.
///
/// A named builder rather than an inline `json!` literal because the `Enqueued`
/// arm used to inline its payload, and an inlined arm is invisible to the
/// builder-level contract gates -- nothing swept it against
/// `cli/schema/message.schema.json`, so the payload shipped unschema'd (#955).
/// Every arm of [`MessageOutcomeOutput::to_json`] routing through a pure builder
/// is the property being restored; keep it uniform. Pure so it stays
/// contract-testable against `cli/schema/message.schema.json`.
pub fn message_enqueued_json(channel: &str, thread: &str) -> serde_json::Value {
    serde_json::json!({
        "status": "enqueued",
        "channel": channel,
        "thread": thread,
    })
}

/// The machine-readable descriptor for `local`/`cluster message --json --dry-run`
/// (issue #354): what a real run would enqueue, without touching the network.
/// `target` is `"local"` or `"cluster"`, `channel` is null when it would be
/// resolved from the sole bound (agent, Slack channel) pair. Pure so it stays
/// contract-testable
/// against `cli/schema/message.schema.json`.
pub fn message_dry_run_json(
    target: &str,
    stream: &str,
    channel: Option<&str>,
    reply_endpoint: &str,
) -> serde_json::Value {
    serde_json::json!({
        "dry_run": true,
        "target": target,
        "stream": stream,
        "channel": channel,
        "reply_endpoint": reply_endpoint,
    })
}

// ---------------------------------------------------------------------------
// CliOutput adapters (#474)
// ---------------------------------------------------------------------------
//
// Route the schema-gated `--json` builders above through the one success-path
// emit shim (`Ui::emit`, ADR-0021) instead of each call site inlining its own
// `if ui.json() { emit_json } else { .. }` branch. `to_json` delegates to the
// pure builders unchanged, so the committed `cli/schema/message.schema.json`
// stays byte-for-byte identical; `render` reproduces the exact human output.

/// `local`/`cluster message --dry-run` output. `to_json` is the schema-gated
/// `message_dry_run_json`; the human render is the plan lines the target built
/// (they differ between local and cluster, so the caller supplies them).
struct MessageDryRunOutput {
    target: &'static str,
    stream: String,
    channel: Option<String>,
    reply_endpoint: String,
    human_lines: Vec<String>,
}

impl crate::ui::CliOutput for MessageDryRunOutput {
    fn to_json(&self) -> serde_json::Value {
        message_dry_run_json(
            self.target,
            &self.stream,
            self.channel.as_deref(),
            &self.reply_endpoint,
        )
    }

    fn render(&self, ui: &crate::ui::Ui) {
        for line in &self.human_lines {
            ui.payload_plain(line);
        }
    }
}

/// The terminal outcome of a real `local`/`cluster message` turn. `to_json` is
/// the matching schema-gated builder; `render` reproduces the exact human view
/// (the stdout answer for a reply, the stderr warning/diagnostics otherwise).
///
/// `pub` so `cli/tests/json_contract.rs` can construct each variant directly
/// and validate `to_json()` against the committed schema (issue #955).
pub enum MessageOutcomeOutput {
    /// The worker finalized the turn with reply text.
    Replied { thread: String, reply: String },
    /// The worker finished the turn but never edited the placeholder.
    NoEdit { thread: String },
    /// The turn parked awaiting human approval. `tier`/`agent`/`channel` shape the
    /// human resolve hint into a copy-paste-runnable `approvals <agent> --resolve
    /// ... --actor-channel <channel>` command (#766); none of them touch
    /// `to_json`, which stays byte-identical.
    AwaitingApproval {
        thread: String,
        reply: Option<String>,
        tier: &'static str,
        agent: Option<String>,
        channel: String,
    },
    /// The deadline elapsed with no reply. `diagnostics` carries the stream
    /// diagnostics string on the human path; it stays `None` under `--json`
    /// (which never gathers them), so no extra Valkey read happens there.
    /// `resume_note` (also human-only) replaces the diagnostics wording for the
    /// resolved-but-unfinished resume case (#766): the JSON stays the byte-
    /// identical `message_timeout_json`, but the operator is told the approval
    /// WAS resolved and the resumed turn simply did not finish in time, rather
    /// than being shown stream diagnostics for the wrong entry.
    TimedOut {
        diagnostics: Option<String>,
        resume_note: Option<String>,
    },
    /// Connected-transport mode (#770/ADR-0078): the turn was enqueued and its
    /// reply, approval card, and any resumed reply land in the connected Slack
    /// workspace, not here. The CLI cannot observe a Slack-side reply, so it
    /// confirms the enqueue and points the operator at the channel.
    Enqueued { channel: String, thread: String },
}

impl crate::ui::CliOutput for MessageOutcomeOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            MessageOutcomeOutput::Replied { thread, reply } => {
                message_reply_json(thread, Some(reply))
            }
            MessageOutcomeOutput::NoEdit { thread } => message_reply_json(thread, None),
            MessageOutcomeOutput::AwaitingApproval { thread, reply, .. } => {
                message_awaiting_approval_json(thread, reply.as_deref())
            }
            MessageOutcomeOutput::TimedOut { .. } => message_timeout_json(),
            MessageOutcomeOutput::Enqueued { channel, thread } => {
                message_enqueued_json(channel, thread)
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            MessageOutcomeOutput::Replied { reply, .. } => {
                ui.answer(reply);
                ui.print_tokens("\n");
            }
            MessageOutcomeOutput::NoEdit { .. } => {
                ui.warn("the worker finished the turn but never edited the placeholder");
            }
            MessageOutcomeOutput::AwaitingApproval {
                tier,
                agent,
                channel,
                ..
            } => {
                note_approval_pending(ui, tier, agent.as_deref(), channel);
            }
            MessageOutcomeOutput::TimedOut {
                diagnostics,
                resume_note,
            } => {
                if let Some(note) = resume_note {
                    ui.warn(note);
                } else {
                    ui.note("stream diagnostics:");
                    if let Some(diag) = diagnostics {
                        ui.note(diag);
                    }
                }
            }
            MessageOutcomeOutput::Enqueued { channel, thread } => {
                ui.note(&format!(
                    "enqueued; the agent is replying in {channel} (thread {thread}). \
                     Watch Slack for the reply and any approval card -- in connected mode \
                     the reply lands in the workspace, not here."
                ));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Effectful helpers
// ---------------------------------------------------------------------------

/// The routable host the stub advertises: `--listen-host` verbatim, otherwise
/// the local IP the kernel would use to reach the cluster's API server (via a
/// UDP-connect that sends no packets -- it only resolves the source interface).
/// The address a container/pod uses to reach the Docker Desktop host from inside
/// the Docker VM (macOS/Windows). Not routable on native-Linux Docker, where the
/// host is reached via the bridge gateway instead.
const DOCKER_INTERNAL_HOST: &str = "host.docker.internal";

/// Whether the cluster-message reply stub should advertise `host.docker.internal`
/// rather than a host-local IP. True only under Docker Desktop's VM topology
/// (macOS/Windows) talking to a loopback-exposed API server -- i.e. a local
/// cluster (kind) whose in-VM worker cannot reach the host's own LAN IP or the
/// kind bridge gateway (both live in the VM), only `host.docker.internal` (#900).
/// `platform_is_docker_vm` is passed in (a compile-time OS check at the call
/// site) so the decision stays unit-testable off those platforms.
fn prefers_docker_internal_host(server_host: &str, platform_is_docker_vm: bool) -> bool {
    platform_is_docker_vm && host_is_loopback(server_host)
}

/// Whether `host` names the loopback interface (`localhost`, `127.0.0.0/8`, `::1`).
fn host_is_loopback(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

async fn resolve_advertise_host(listen_host: Option<&str>) -> Result<String> {
    if let Some(host) = listen_host {
        return Ok(host.to_string());
    }
    let (ok, out, err) = run_capture(&crate::ops::kubeconfig_host_cmd()).await?;
    if !ok {
        bail!(
            "could not read the kubeconfig API server to auto-detect a routable host ({}); \
             pass --listen-host <host>",
            err.trim()
                .lines()
                .next()
                .unwrap_or("kubectl config view failed")
        );
    }
    let server = out.trim();
    let (host, port) = server_host_and_port(server).with_context(|| {
        format!("could not parse the kubeconfig server url {server:?}; pass --listen-host <host>")
    })?;
    // Docker Desktop (macOS/Windows) runs the cluster inside a LinuxKit VM, so a
    // host-local IP or the kind bridge gateway is unreachable from the in-cluster
    // worker -- it reaches the host only via host.docker.internal. Detect that
    // (a loopback-exposed API server on a Docker-VM platform, i.e. a local kind
    // cluster) and advertise host.docker.internal instead of the local egress IP
    // (#900). Native-Docker Linux (CI included) is unaffected: it passes
    // --listen-host explicitly (returned above), and this branch is false off
    // macOS/Windows anyway.
    if prefers_docker_internal_host(&host, cfg!(any(target_os = "macos", target_os = "windows"))) {
        return Ok(DOCKER_INTERNAL_HOST.to_string());
    }
    let ip = detect_local_ip(&host, port).with_context(|| {
        format!("could not detect the local IP toward {host}:{port}; pass --listen-host <host>")
    })?;
    Ok(ip.to_string())
}

/// The local source IP the kernel would use to reach `host:port`. A UDP socket
/// `connect` only sets the default peer and picks the egress interface; no
/// datagram is sent, so this needs no reachability and touches no network.
fn detect_local_ip(host: &str, port: u16) -> Option<std::net::IpAddr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").ok()?;
    socket.connect((host, port)).ok()?;
    socket.local_addr().ok().map(|addr| addr.ip())
}

/// Spawn a `kubectl port-forward` child (killed on drop via `kill_on_drop`) and
/// block until its local port accepts TCP, so callers can use it immediately.
pub async fn start_port_forward(
    cmd: &OpsCommand,
    local_port: u16,
    label: &str,
) -> Result<tokio::process::Child> {
    crate::ui::ui().plumbing(&format!("+ {}", cmd.display()));
    let mut child = tokio::process::Command::new(&cmd.program)
        .args(cmd.argv())
        .kill_on_drop(true)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawning `{}` (is kubectl on PATH?)", cmd.program))?;
    wait_for_tcp(local_port, Duration::from_secs(15))
        .await
        .with_context(|| format!("the {label} port-forward never opened localhost:{local_port}"))?;
    // The port accepting TCP is not proof WE bound it. If another process was
    // already listening on localhost:{local_port}, kubectl could not bind, so it
    // exited, and the socket that just answered is the squatter's -- a caller
    // that then posts a discovered key would leak it to that process. If the
    // child has already exited by the time TCP connects, it never held the port;
    // refuse and name the conflict. A child still alive is the happy path (this
    // stays race-tolerant: only a definitely-exited child trips the guard).
    if child.try_wait()?.is_some() {
        bail!(
            "the {label} port-forward exited immediately; localhost:{local_port} is already in \
             use by another process. Free that port or stop the conflicting process, then retry."
        );
    }
    Ok(child)
}

/// Poll-connect to `localhost:port` until it accepts or the timeout elapses.
async fn wait_for_tcp(port: u16, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out after {:?} connecting to localhost:{port}",
                timeout
            );
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Hard cap on the best-effort diagnostics gather run after a timeout.
/// `diagnostics` reads straight from the SAME Valkey the worker never acked
/// against, with no timeout of its own -- unlike every other post-deadline
/// Valkey read on this path (`chat::ACK_CALL_TIMEOUT`,
/// `chat::RESUME_SCAN_CALL_TIMEOUT`), which are deliberately bounded so a
/// stalled Valkey cannot push past `--timeout-secs`. If Valkey itself is what
/// caused the turn to time out in the first place (a stall or partition is a
/// common cause), an unbounded diagnostics read can hang the CLI indefinitely
/// AFTER the turn has already timed out (#751) -- the process just sits there,
/// still alive, which is the "linger" an operator has to find and kill by
/// hand. Capping it means the worst case is a diagnostics printout that says
/// the read timed out, not a wedged process.
const DIAGNOSTICS_TIMEOUT: Duration = Duration::from_secs(5);

/// Best-effort stream diagnostics bounded by [`DIAGNOSTICS_TIMEOUT`] (#751).
async fn bounded_diagnostics(
    conn: &mut MultiplexedConnection,
    stream: &str,
    stream_id: &str,
) -> String {
    tokio::time::timeout(DIAGNOSTICS_TIMEOUT, diagnostics(conn, stream, stream_id))
        .await
        .unwrap_or_else(|_| {
            format!(
                "  diagnostics unavailable: Valkey did not respond within {DIAGNOSTICS_TIMEOUT:?}"
            )
        })
}

/// The `curie local message` handler: drive the compose stack directly.
///
/// The cluster path's self-plumbing (kubectl port-forwards) is cluster-specific,
/// so local mode keeps only the shared engine: bind the Slack stub, enqueue the
/// `QueuedTurn` carrying this stub as its reply endpoint (issue #19), and wait on
/// the XACK signal. The compose worker reaches the stub on the fixed loopback
/// port `http://localhost:{DEFAULT_LOCAL_STUB_PORT}/api/`.
async fn message_local(opts: MessageOpts) -> Result<()> {
    let ui = crate::ui::ui();
    let valkey_url = local_valkey_url(&opts.valkey_password);
    let api_base = local_api_base(opts.api_url.as_deref());

    if opts.dry_run {
        let reply_endpoint = local_stub_reply_endpoint(&local_stub_binding().advertise_host);
        let channel_line = match opts.channel.as_deref() {
            Some(channel) => format!("channel {channel}"),
            None => format!(
                "channel <the sole bound (agent, Slack channel) pair via {api_base}/agents>"
            ),
        };
        let human_lines = vec![
            "local mode (compose stack; no kubectl/helm)".to_string(),
            format!("enqueue onto redis {valkey_url}"),
            format!("stub advertised at {reply_endpoint}"),
            channel_line,
            format!("enqueue a synthetic QueuedTurn on stream {}", opts.stream),
            connected_transport_dry_run_note(),
        ];
        ui.emit(&MessageDryRunOutput {
            target: "local",
            stream: opts.stream.clone(),
            channel: opts.channel.clone(),
            reply_endpoint,
            human_lines,
        });
        return Ok(());
    }

    // Connect Valkey up front so a down stack fails fast, before the stub binds.
    let mut conn = connect(&valkey_url).await?;

    // Connected-transport path (#770/ADR-0078): when a real workspace is wired up
    // (`local comms --slack` set a genuine bot token, not the `xoxb-dev` stub
    // sentinel), post a real placeholder and enqueue against its ts with no
    // per-turn endpoint so the reply and any approval card ride that transport.
    // Resolving the channel first keeps the behavior identical to the stub path.
    if let Some(bot_token) = local_connected_bot_token().await {
        let channel = match opts.channel.as_deref() {
            Some(channel) => channel.to_string(),
            None => {
                let api = ApiClient::new(&api_base, &opts.api_key)?;
                let agents = api.list_agents().await.with_context(|| {
                    format!("listing agents via {api_base} (is `curie local up` running?)")
                })?;
                select_channel(&agents, None)?
            }
        };
        ui.plumbing(&format!(
            "routing to channel {channel} over the connected Slack transport"
        ));
        return enqueue_over_connected_transport(
            &opts,
            &mut conn,
            TurnVerb::Local,
            &channel,
            &bot_token,
        )
        .await;
    }

    // Bind the stub and advertise the reply endpoint so the compose worker can
    // reach it. Native-Linux host networking shares the host loopback, so bind
    // 127.0.0.1 and advertise `localhost`. Under Docker Desktop the worker sits
    // in the VM netns, where `localhost` is the VM's loopback and not this Mac
    // host, so bind 0.0.0.0 and advertise `host.docker.internal` instead (#680).
    // Unlike the cluster path, the advertised host here is load-bearing: it is
    // the per-turn reply endpoint carried on the QueuedTurn below.
    let binding = local_stub_binding();
    let mut stub = SlackStub::start(
        &binding.bind_host,
        DEFAULT_LOCAL_STUB_PORT,
        &binding.advertise_host,
    )
    .await?;
    ui.plumbing(&format!(
        "slack stub listening; the worker posts to {}",
        stub.base_api_url()
    ));

    // Channel: explicit --channel, else the sole bound Slack pair from the compose
    // API (reached directly; routers mount at root, so the base carries no /api).
    // `agent_hint` is the sole agent's NAME when we resolved it (so an approval
    // resolve hint is copy-paste runnable), and `None` for an explicit --channel
    // (we don't know which agent it binds) -- then the hint shows an `<AGENT>`
    // slot (#766).
    let (channel, agent_hint): (String, Option<String>) = match opts.channel.as_deref() {
        Some(channel) => (channel.to_string(), None),
        None => {
            let api = ApiClient::new(&api_base, &opts.api_key)?;
            let agents = api.list_agents().await.with_context(|| {
                format!("listing agents via {api_base} (is `curie local up` running?)")
            })?;
            let channel = select_channel(&agents, None)?;
            (channel, agents.first().map(|a| a.name.clone()))
        }
    };
    ui.plumbing(&format!("routing to channel {channel}"));

    // This turn carries its own reply endpoint (issue #19), so the compose worker
    // finalizes it against this stub without relying on a worker-global setting.
    let reply_endpoint = stub.base_api_url().to_string();
    let (channel, thread_ts, placeholder_ts) =
        resolve_targets(Some(&channel), opts.thread.as_deref());
    let event = synthetic_turn(
        "slack",
        &channel,
        &opts.user,
        &opts.text,
        &thread_ts,
        &placeholder_ts,
        Some(reply_endpoint),
    );
    let stream_id = xadd(&mut conn, &opts.stream, &event).await?;
    ui.plumbing(&format!(
        "enqueued {} on {} as {stream_id}",
        event.event_id, opts.stream
    ));
    ui.plumbing(&format!(
        "waiting up to {}s for the worker to finalize the turn...",
        opts.timeout_secs
    ));

    let cl = ui.checklist();
    let step = cl.step("waiting for worker reply");
    let wait_started = Instant::now();
    let outcome = {
        let mut observe_update = |text: &str| {
            if let Some(line) = text.lines().rev().find(|line| !line.trim().is_empty()) {
                step.tick_detail(line.trim());
            }
        };
        await_reply(
            &mut stub,
            &mut conn,
            &opts.stream,
            &stream_id,
            &placeholder_ts,
            Duration::from_secs(opts.timeout_secs),
            &mut observe_update,
        )
        .await
    };

    match outcome {
        Outcome::Replied(reply) => {
            step.done("");
            ui.emit(&MessageOutcomeOutput::Replied {
                thread: thread_ts.clone(),
                reply,
            });
            persist_and_hint(&opts, TurnVerb::Local, &channel, &thread_ts);
            Ok(())
        }
        Outcome::CompletedNoEdit => {
            step.done("no edit");
            ui.emit(&MessageOutcomeOutput::NoEdit {
                thread: thread_ts.clone(),
            });
            persist_and_hint(&opts, TurnVerb::Local, &channel, &thread_ts);
            Ok(())
        }
        Outcome::AwaitingApproval(reply) => {
            step.done("awaiting approval");
            // Persist the turn context BEFORE the (possibly full --timeout-secs)
            // approval wait: if the operator interrupts or the terminal closes
            // while the approval is pending, `.curie/last-turn.json` must still
            // hold the thread for `message --continue` (#766). The terminal paths
            // re-persist the identical context and print the continue hint once.
            persist_turn_quietly(&opts, TurnVerb::Local, &channel, &thread_ts);
            // Keep the stub alive and wait for the resumed reply instead of
            // exiting and stranding it (#766). The wait rides the Valkey
            // connection already open for the enqueue, so the only degradation is
            // a placeholder notice we cannot parse an approval id from -- then
            // fall back to the terminal.
            match parse_approval_id(reply.as_deref().unwrap_or_default()) {
                Some(id) => {
                    let remaining = Duration::from_secs(opts.timeout_secs)
                        .saturating_sub(wait_started.elapsed());
                    match resume_after_approval(
                        &opts,
                        TurnVerb::Local,
                        &mut conn,
                        &id,
                        &mut stub,
                        &stream_id,
                        &placeholder_ts,
                        &thread_ts,
                        &channel,
                        agent_hint.as_deref(),
                        reply,
                        remaining,
                    )
                    .await
                    {
                        ResumeExit::Done => Ok(()),
                        // Still parked: the durable approval stays pending and is
                        // resolvable later, so this is retryable. Local mode holds
                        // no port-forward children, but it DOES still hold the
                        // Slack stub -- move it into `exit_after_drop` so its
                        // listener is torn down before exit rather than leaked
                        // (#751).
                        ResumeExit::Transient => {
                            exit_after_drop(crate::exit::ExitClass::Transient, stub);
                        }
                    }
                }
                None => {
                    // No parseable approval id, so we never entered the resume wait.
                    // The turn is parked exactly like the timeout terminal, so exit
                    // with the SAME transient (retryable) class rather than 0, so a
                    // scripted caller sees one deterministic exit for "still parked"
                    // regardless of whether the id happened to parse (#766, N5).
                    ui.emit(&MessageOutcomeOutput::AwaitingApproval {
                        thread: thread_ts.clone(),
                        reply,
                        tier: tier_str(TurnVerb::Local),
                        agent: agent_hint.clone(),
                        channel: channel.clone(),
                    });
                    persist_and_hint(&opts, TurnVerb::Local, &channel, &thread_ts);
                    // Drop the Slack stub first so its listener is not leaked past
                    // this non-unwinding exit (#751).
                    exit_after_drop(crate::exit::ExitClass::Transient, stub);
                }
            }
        }
        Outcome::TimedOut => {
            step.fail(&format!("timed out after {}s", opts.timeout_secs));
            // Drop the Slack stub's listener IMMEDIATELY on timeout, before
            // anything else -- in particular before the diagnostics gather right
            // below, which reads from the SAME Valkey the worker never acked
            // against and can itself stall (bounded by `DIAGNOSTICS_TIMEOUT`, but
            // that is still seconds during which a not-yet-dropped stub would
            // keep holding the port). Releasing the stub first means the very
            // next `local message` can bind successfully right away regardless of
            // how long anything after this line takes (#751).
            drop(stub);
            // Gather diagnostics only on the human path; under `--json` the
            // timeout object carries no diagnostics, so skip the extra Valkey read.
            let diag = if ui.json() {
                None
            } else {
                Some(bounded_diagnostics(&mut conn, &opts.stream, &stream_id).await)
            };
            ui.emit(&MessageOutcomeOutput::TimedOut {
                diagnostics: diag,
                resume_note: None,
            });
            // A timeout is retryable (the worker may still be working, or a
            // transient stall), so it maps to the transient exit code, not
            // failure. The stub is already dropped above; nothing else to tear
            // down for local mode ("Local mode holds no port-forward children").
            std::process::exit(crate::exit::ExitClass::Transient.code());
        }
    }
}

/// The one runnable `approvals --resolve` command shape, shared by the pre-wait
/// hint and the terminal wording so the two cannot drift (#766).
///
/// Every flag here is load-bearing against the server:
/// - `<AGENT>` is a REQUIRED positional on the `approvals` clap surface
///   (`AgentTarget` flattens a mandatory `agent` arg), so omitting it made the
///   printed hint fail with `error: the following required arguments were not
///   provided: <AGENT>`.
/// - `--as <user>` is required by `--resolve`, and the server blocks
///   self-approval, so it must not be the turn's author.
/// - `--actor-channel <channel>` is required by the DEFAULT approver set. With no
///   `approvers` block on the route binding, the API selects
///   `SlackChannelMembers(approval.card_channel or approval.reply_channel)`
///   (`apps/api/src/curie_api/slack_approvers.py`), whose `contains` admits the
///   actor only when `actor_channel` equals that channel -- otherwise the resolve
///   is refused 403 ("resolve this from the approval's channel"). The channel this
///   turn routed to IS `reply_channel`, so it is the correct value in the common
///   case; a route binding that placed the card elsewhere carries a different
///   `card_channel`, which `approvals --list` reports. (Route bindings that
///   declare `approvers.users`/`approvers.group` ignore the channel entirely, so
///   passing it is harmless there.)
fn approval_resolve_command(tier: &str, agent: Option<&str>, channel: &str, id: &str) -> String {
    let agent = agent.unwrap_or("<AGENT>");
    format!("curie {tier} approvals {agent} --resolve {id} --as <user> --actor-channel '{channel}'")
}

/// The awaiting-approval terminal wording a `local`/`cluster message` prints when
/// it did not keep the reply stub alive to receive the resumed reply -- i.e. the
/// timeout terminal (the wait elapsed with the approval still pending) or the
/// graceful-degradation fallback (no parseable approval id).
///
/// Every runtime call site is AFTER the wait ended, immediately before this
/// command exits and drops its reply stub, so the wording says exactly that: the
/// command is exiting, the durable `Approval` stays resolvable, and the resumed
/// reply must be read from the agent transcript rather than waited for here.
/// Promising that a later resolution "prints here" would strand an operator
/// watching a terminal that is already gone (#766). It never overclaims a
/// clickable Slack card either.
fn note_approval_pending(ui: &crate::ui::Ui, tier: &str, agent: Option<&str>, channel: &str) {
    let command = approval_resolve_command(tier, agent, channel, "<id>");
    ui.warn(
        "this turn is awaiting human approval; it did not finalize, and this command is now \
         exiting. The durable approval was persisted server-side and stays pending until someone \
         resolves it.",
    );
    ui.note(&format!(
        "resolve it later with `{command}` (the id is listed by `curie {tier} approvals \
         <AGENT> --list`, which also reports the approval's channel if its route binds one). \
         Because this command has exited, the resumed reply does NOT print here -- read it from \
         the agent transcript. There is no clickable Slack card unless a real workspace is \
         connected (`curie {tier} comms --slack`).",
    ));
}

/// How a resume wait ended, as far as the CALLER's process lifetime is concerned.
///
/// The wait itself always finishes by emitting its terminal output; what the
/// caller still owes is only the exit. This exists so the transient exit is taken
/// by the handler that OWNS the `kubectl port-forward` guards rather than inside
/// this helper: `std::process::exit` does not unwind, so calling it here would
/// skip the caller's `kill_on_drop` destructors and orphan the port-forward child
/// to init (#766). The caller drops its guards, then exits.
enum ResumeExit {
    /// Fully handled; the caller returns `Ok(())` and its guards drop normally.
    Done,
    /// The turn is still parked (the wait elapsed, or the resumed turn hit a NEW
    /// gate). The durable `Approval` stays pending and resolvable later, so this
    /// is retryable: the caller drops its port-forward guards and exits with the
    /// transient class.
    Transient,
}

/// Exit the process with `class`, after dropping `guards` first.
///
/// `std::process::exit` does not unwind the stack, so any `Drop` impl still in
/// scope at the call site -- the Slack stub's listener/server task
/// ([`SlackStub`](crate::chat::SlackStub)), a `kubectl port-forward` child --
/// would otherwise never run, leaking whatever it holds (#751: a timed-out
/// `local message`/`cluster message` used to leak the stub's bound port this
/// way, wedging every subsequent turn with "Address already in use").
///
/// Every exit site in this module that needs to tear down a guard before
/// exiting routes through here instead of calling `std::process::exit`
/// directly: `guards` is taken BY VALUE, so the compiler forces the caller to
/// move ownership in (a `SlackStub`, a `tokio::process::Child`, or a tuple of
/// several) rather than merely borrowing it, and this function drops it before
/// the process actually exits. That makes the guard-then-exit ordering
/// structural rather than something a future call site has to remember to do
/// by hand.
fn exit_after_drop<T>(class: crate::exit::ExitClass, guards: T) -> ! {
    drop(guards);
    std::process::exit(class.code());
}

/// Keep the reply stub alive after a turn parked awaiting approval and wait for
/// the resumed reply (#766, ADR-0063).
///
/// Prints a per-id resolve hint and a waiting note, then waits on the runs stream
/// via [`await_resume`]: when a human resolves the approval, the API appends the
/// resume turn under the deterministic `approval-<id>-resolved` event id,
/// replaying this stub's endpoint and the tracked placeholder. Completion is the
/// worker's XACK of that entry, so the reply is reported only once the resumed
/// turn FINALIZES -- a booting or partially-streamed edit is never printed as the
/// answer. The wait is read-only on the approval: it never resolves, rejects, or
/// deletes the durable record.
///
/// If the RESUMED turn itself parks on a NEW approval gate, this LOOPS: it parses
/// the new approval id, recomputes `approval-<new-id>-resolved`, and keeps waiting
/// on the fresh resume entry while the caller's overall deadline remains -- so a
/// nested gate does not re-strand the reply the way exiting on the first gate
/// would (that was exactly the bug this PR fixes). The loop is bounded by the
/// deadline and, defensively, by a max iteration count.
///
/// Emits the terminal output for every outcome and returns what the caller still
/// owes ([`ResumeExit`]). Shared by the local and cluster handlers so the two
/// tiers cannot drift.
#[allow(clippy::too_many_arguments)] // one cohesive resume-wait call; a struct would not clarify it
async fn resume_after_approval(
    opts: &MessageOpts,
    verb: TurnVerb,
    conn: &mut MultiplexedConnection,
    id: &str,
    stub: &mut SlackStub,
    // The CLI's OWN original turn entry id: the exclusive lower bound for the
    // resume scan, so it reads only entries enqueued after our turn (#766, N1).
    after_id: &str,
    placeholder_ts: &str,
    thread_ts: &str,
    channel: &str,
    // The runnable resolve-hint agent positional: the sole deployed agent's name,
    // or `None` (rendered `<AGENT>`) when an explicit `--channel` hid it (#766).
    agent: Option<&str>,
    awaiting_reply: Option<String>,
    remaining: Duration,
) -> ResumeExit {
    let ui = crate::ui::ui();
    let tier = tier_str(verb);
    let deadline = Instant::now() + remaining;
    // Defensive cap: the deadline is the real bound, but never spin unbounded on a
    // pathological gate-per-resume loop.
    const MAX_NESTED_GATES: usize = 64;
    let mut current_id = id.to_string();
    let mut last_reply = awaiting_reply;
    for _ in 0..MAX_NESTED_GATES {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        ui.note(&format!(
            "resolve it with: {}",
            approval_resolve_command(tier, agent, channel, &current_id)
        ));
        ui.note(
            "waiting for the approval to be resolved; the resumed reply lands here if it is \
             resolved before --timeout-secs elapses...",
        );
        // The API's deterministic idempotency key for this approval's resume turn
        // (`resumequeue.resume_event_id`), which is how we recognize that turn on
        // the shared runs stream.
        let resume_event_id = format!("approval-{current_id}-resolved");
        let cl = ui.checklist();
        let step = cl.step("waiting for resumed worker reply");
        let observed = {
            let mut observe_update = |text: &str| {
                if let Some(line) = text.lines().rev().find(|line| !line.trim().is_empty()) {
                    step.tick_detail(line.trim());
                }
            };
            await_resume(
                stub,
                conn,
                &opts.stream,
                &resume_event_id,
                after_id,
                placeholder_ts,
                remaining,
                &mut observe_update,
            )
            .await
        };
        step.clear();
        match observed.outcome {
            Outcome::Replied(reply) => {
                ui.emit(&MessageOutcomeOutput::Replied {
                    thread: thread_ts.to_string(),
                    reply,
                });
                persist_and_hint(opts, verb, channel, thread_ts);
                return ResumeExit::Done;
            }
            Outcome::CompletedNoEdit => {
                ui.emit(&MessageOutcomeOutput::NoEdit {
                    thread: thread_ts.to_string(),
                });
                persist_and_hint(opts, verb, channel, thread_ts);
                return ResumeExit::Done;
            }
            Outcome::AwaitingApproval(new_reply) => {
                // The RESUMED turn hit a NEW gate of its own. Keep waiting on ITS
                // resume entry rather than exiting and dropping the stub -- exiting
                // here would re-create the dead-endpoint bug for the nested gate.
                match parse_approval_id(new_reply.as_deref().unwrap_or_default()) {
                    Some(new_id) => {
                        current_id = new_id;
                        last_reply = new_reply;
                        // Loop: wait on the nested approval's resume entry.
                        continue;
                    }
                    None => {
                        // No parseable id on the nested notice: surface the awaiting
                        // terminal (durable + resolvable) rather than looping blind.
                        ui.emit(&MessageOutcomeOutput::AwaitingApproval {
                            thread: thread_ts.to_string(),
                            reply: new_reply,
                            tier,
                            agent: agent.map(str::to_string),
                            channel: channel.to_string(),
                        });
                        persist_and_hint(opts, verb, channel, thread_ts);
                        return ResumeExit::Transient;
                    }
                }
            }
            Outcome::TimedOut if observed.resolved => {
                // The approval WAS resolved (we saw the resume entry), but the
                // resumed turn did not finalize before the deadline. That is a
                // plain timeout, not a still-pending approval: emit the byte-
                // identical timeout terminal with honest wording (#766, Codex P2).
                ui.emit(&MessageOutcomeOutput::TimedOut {
                    diagnostics: None,
                    resume_note: Some(format!(
                        "the approval was resolved, but the resumed turn did not finish before \
                         --timeout-secs ({}s); read the resolved reply from the agent transcript",
                        opts.timeout_secs
                    )),
                });
                persist_and_hint(opts, verb, channel, thread_ts);
                return ResumeExit::Transient;
            }
            Outcome::TimedOut => {
                // Never resolved: the durable approval is still pending.
                ui.emit(&MessageOutcomeOutput::AwaitingApproval {
                    thread: thread_ts.to_string(),
                    reply: last_reply,
                    tier,
                    agent: agent.map(str::to_string),
                    channel: channel.to_string(),
                });
                // Persist the turn context even on the transient exit so a follow-up
                // `--continue` still has the thread to resume against.
                persist_and_hint(opts, verb, channel, thread_ts);
                return ResumeExit::Transient;
            }
        }
    }
    // The deadline elapsed at a loop boundary, or the nested-gate cap was hit. The
    // current approval is still pending and resolvable later.
    ui.emit(&MessageOutcomeOutput::AwaitingApproval {
        thread: thread_ts.to_string(),
        reply: last_reply,
        tier,
        agent: agent.map(str::to_string),
        channel: channel.to_string(),
    });
    persist_and_hint(opts, verb, channel, thread_ts);
    ResumeExit::Transient
}

/// The shared target noun message handler.
/// The local stub's sentinel bot token (`comms::LOCAL_SLACK_STUB_BOT_TOKEN`).
/// `local comms --disconnect` restores this value, so seeing it means the compose
/// dispatcher is wired to the local stub -- NOT a real workspace.
const LOCAL_STUB_BOT_TOKEN: &str = "xoxb-dev";

/// The host in `comms::LOCAL_SLACK_STUB_URL`. A worker whose `SLACK_API_BASE_URL`
/// points here talks to the in-compose stub, never to Slack.
const LOCAL_SLACK_STUB_HOST: &str = "localhost:8155";

/// The compose service (and container) that holds the worker's Slack transport.
const LOCAL_WORKER_CONTAINER: &str = "curie-worker";

/// The Slack transport the RUNNING compose worker is actually configured with:
/// `(SLACK_API_BASE_URL, SLACK_BOT_TOKEN)` as the container holds them.
type WorkerTransport = (Option<String>, Option<String>);

/// The bot token to post a real placeholder with, or `None` when this stack is
/// not wired to a real workspace.
///
/// Decided from the worker's OWN transport, never from CLI-side config (#957).
/// The previous form read `CURIE_SLACK_BOT_TOKEN`/`SLACK_BOT_TOKEN`/the persisted
/// secret vault and claimed a deliberate `--disconnect` could not be overridden
/// by a stale vault entry. That claim was false and the failure was ugly:
/// `local comms --disconnect` writes the stub sentinel only into the compose
/// SUBPROCESS environment, and a child process cannot set its parent's env, so
/// a vault token from `curie secrets set SLACK_BOT_TOKEN` (the documented #749
/// setup) stayed visible to the CLI. A stub-only stack would then post a real
/// placeholder into a real channel while the worker updated the stub -- an
/// orphaned "..." in a customer channel and a reply nobody sees.
///
/// Two conditions, both read from the container:
///
/// 1. `SLACK_API_BASE_URL` must NOT point at the local stub. This is the
///    definitive signal: it is literally the transport the worker will use to
///    edit the placeholder, so if it is the stub, no real post can ever be
///    updated, whatever any token says.
/// 2. the token must be a real one, not the `xoxb-dev` sentinel.
///
/// Returning the worker's OWN token (rather than one the CLI resolved) also
/// keeps the posting identity equal to the updating identity: Slack only lets the
/// authoring bot `chat.update` a message, so two different tokens would leave the
/// placeholder stuck at "..." with `cant_update_message` (#957 mode B).
///
/// Pure, so both conditions are unit-testable without Docker.
fn connected_worker_bot_token(transport: WorkerTransport) -> Option<String> {
    let (api_base, token) = transport;
    let api_base = api_base.unwrap_or_default();
    // Wired to the stub (or to nothing resolvable) means not connected.
    if api_base.is_empty() || api_base.contains(LOCAL_SLACK_STUB_HOST) {
        return None;
    }
    let token = token?.trim().to_string();
    (!token.is_empty() && token != LOCAL_STUB_BOT_TOKEN).then_some(token)
}

/// Read the running compose worker's Slack transport out of the container.
///
/// `docker inspect` on the worker container, so what we act on is what the worker
/// holds -- the reconciliation #957 asks for. Any failure (no stack up, docker
/// absent, container renamed) reads as "no transport", which falls back to the
/// stub path: the safe direction, since the stub path never posts to real Slack.
async fn running_worker_transport() -> WorkerTransport {
    let out = match crate::docker::docker_capture(&[
        "inspect".to_string(),
        "--format".to_string(),
        "{{range .Config.Env}}{{println .}}{{end}}".to_string(),
        LOCAL_WORKER_CONTAINER.to_string(),
    ])
    .await
    {
        Ok((status, stdout, _)) if status.success() => stdout,
        _ => return (None, None),
    };
    let find = |key: &str| -> Option<String> {
        out.lines()
            .find_map(|l| l.strip_prefix(key).map(|v| v.to_string()))
    };
    (find("SLACK_API_BASE_URL="), find("SLACK_BOT_TOKEN="))
}

/// Process-level wrapper: the token to post with, or `None` for the stub path.
async fn local_connected_bot_token() -> Option<String> {
    connected_worker_bot_token(running_worker_transport().await)
}

/// The human dry-run line noting that a connected workspace changes the plan
/// (#770/ADR-0078). `--dry-run` never touches the network, so it cannot probe for
/// a dispatcher; it states the conditional instead of guessing.
fn connected_transport_dry_run_note() -> String {
    "if a Slack workspace is connected, the plan changes: no stub is bound -- \
     the CLI posts a real placeholder to the channel over the workspace bot token \
     and enqueues against its ts with no per-turn endpoint, so the reply and any \
     approval card ride the connected transport"
        .to_string()
}

/// Post a real placeholder over the connected transport and enqueue the turn
/// against its ts with NO per-turn endpoint (#770/ADR-0078), then report where the
/// reply will land. Shared by both tiers: the caller supplies an open Valkey
/// connection, the resolved channel, and the workspace bot token.
///
/// Because the placeholder is a real Slack message, the worker edits it in place
/// and the requesting-channel approval card threads under it on the EXISTING
/// kernel/sink paths -- no per-turn endpoint means the turn rides the worker's
/// default (connected) transport, exactly like a real mention.
///
/// Public only so the connected-transport COMPOSITION can be pinned by an
/// integration test (`cli/tests/chat_enqueue.rs`): #954 was a wiring defect
/// between two individually-correct leaves, so the coverage that matters drives
/// this whole function against a real Valkey and a real HTTP Slack stub and reads
/// back the bytes that landed on the stream. No other caller outside this module.
pub async fn enqueue_over_connected_transport(
    opts: &MessageOpts,
    conn: &mut MultiplexedConnection,
    verb: TurnVerb,
    channel: &str,
    bot_token: &str,
) -> Result<()> {
    let ui = crate::ui::ui();
    // An empty `--thread` is normalized away at the [`message`] entry point, and
    // re-applied HERE because this function is `pub` (an integration test drives
    // it directly, bypassing `message`), so it cannot assume its callers
    // normalized. See [`normalize_thread`]. Anything present past this line names
    // a real thread.
    let explicit_thread = opts.thread.as_deref().filter(|ts| !ts.is_empty());

    let placeholder_ts =
        crate::slack::post_placeholder(bot_token, channel, "\u{2026}", explicit_thread)
            .await
            .with_context(|| {
                let mut context =
                    "posting the placeholder to the connected Slack workspace".to_string();
                // Slack rejects a thread_ts naming no real message, so a thread ts
                // carried over from a stub turn (always synthetic) or from a
                // pre-#954 connected turn now fails the whole command here --
                // including when it arrived via --continue reading
                // .curie/last-turn.json rather than an explicit --thread. Name that
                // as the cause and say what undoes it, per ADR-0021.
                if let Some(thread) = explicit_thread {
                    context.push_str(&format!(
                        ": if Slack rejected the thread, --thread {thread} names no message in \
                         {channel}. Drop --thread (or re-run without --continue, which reuses the \
                         last turn's thread ts) to start a new thread."
                    ));
                }
                context
            })?;

    let event = connected_turn(channel, opts, explicit_thread, &placeholder_ts);
    let stream_id = xadd(conn, &opts.stream, &event).await?;
    ui.plumbing(&format!(
        "enqueued {} on {} as {stream_id}",
        event.event_id, opts.stream
    ));

    // The reply, approval card, and any resumed reply land in Slack, not here.
    persist_and_hint(opts, verb, channel, &event.conversation_id);
    ui.emit(&MessageOutcomeOutput::Enqueued {
        channel: channel.to_string(),
        thread: event.conversation_id,
    });
    Ok(())
}

/// The exact turn a connected-transport enqueue puts on the stream: its
/// `conversation_id` is an explicit `--thread` when one was named, else the ts of
/// the placeholder we just posted (a top-level message's own ts IS its thread
/// root), and the reply handle's placeholder is always that real ts.
///
/// Both must be REAL Slack timestamps and must agree with where the placeholder
/// landed (issue #954): the worker threads its requesting-channel approval card on
/// `conversation_id` best-effort, so Slack silently drops a card threaded under a
/// ts that names no message.
///
/// Assumption when a `--thread` IS named: that ts is a thread ROOT. A `--thread`
/// naming a threaded reply would be re-parented by Slack onto the true parent,
/// leaving `conversation_id` at the reply while the placeholder landed under its
/// parent; we take the named ts at its word, since #954's stated behavior is that
/// `--thread <ts>` makes `<ts>` the conversation_id.
fn connected_turn(
    channel: &str,
    opts: &MessageOpts,
    explicit_thread: Option<&str>,
    placeholder_ts: &str,
) -> QueuedTurn {
    let conversation_id = explicit_thread.unwrap_or(placeholder_ts);
    synthetic_turn(
        "slack",
        channel,
        &opts.user,
        &opts.text,
        conversation_id,
        placeholder_ts,
        None,
    )
}

/// Resolve the target channel (and the sole-agent hint for resolve messages) for
/// a cluster turn: an explicit `--channel`, else the sole bound Slack pair via a
/// short-lived API port-forward (dropped once the lookup returns). Shared by the
/// stub path and the connected-transport path.
async fn resolve_cluster_channel(opts: &MessageOpts) -> Result<(String, Option<String>)> {
    match opts.channel.as_deref() {
        Some(channel) => Ok((channel.to_string(), None)),
        None => {
            let _api_pf = start_port_forward(
                &port_forward_command(
                    &opts.namespace,
                    &opts.release,
                    "api",
                    opts.api_local_port,
                    API_REMOTE_PORT,
                ),
                opts.api_local_port,
                "api",
            )
            .await?;
            let api = ApiClient::new(
                &format!("http://localhost:{}", opts.api_local_port),
                &opts.api_key,
            )?;
            let agents = api
                .list_agents()
                .await
                .context("listing agents through the api port-forward")?;
            let channel = select_channel(&agents, None)?;
            Ok((channel, agents.first().map(|a| a.name.clone())))
        }
    }
}

/// The connected-transport `cluster message` path (#770/ADR-0078). A real Slack
/// workspace is connected, so instead of a throwaway stub we post a real
/// placeholder to the channel over the workspace bot token and enqueue the turn
/// against its real ts with no per-turn endpoint. The worker then edits that
/// message in place and the approval card threads under it -- the card and any
/// resumed reply ride the connected transport. The CLI cannot observe a reply
/// that lands in Slack, so it enqueues and points the operator there rather than
/// waiting on a (nonexistent) stub.
async fn message_connected(opts: MessageOpts) -> Result<()> {
    let ui = crate::ui::ui();

    // Valkey port-forward for the enqueue (killed on drop at fn end).
    let _valkey_pf = start_port_forward(
        &port_forward_command(
            &opts.namespace,
            &opts.release,
            "valkey",
            opts.valkey_local_port,
            VALKEY_REMOTE_PORT,
        ),
        opts.valkey_local_port,
        "valkey",
    )
    .await?;

    let (channel, _agent_hint) = resolve_cluster_channel(&opts).await?;
    ui.plumbing(&format!(
        "routing to channel {channel} over the connected Slack transport"
    ));

    // Bot token: explicit CURIE_SLACK_BOT_TOKEN override, else the release
    // Secret. Never printed; used only to post the placeholder.
    //
    // The Secret path is identity-correct by construction: the worker reads the
    // same `slackBotToken` from the same Secret, so the bot that posts the
    // placeholder is the bot that edits it. The OVERRIDE can diverge, and Slack
    // only lets the authoring bot `chat.update` a message -- a mismatch leaves the
    // placeholder stuck at "..." with `cant_update_message` (#957 mode B). Warn
    // rather than refuse: an operator may legitimately be working around a stale
    // Secret, and this is the escape hatch for exactly that.
    let bot_token = match std::env::var("CURIE_SLACK_BOT_TOKEN")
        .ok()
        .filter(|value| !value.is_empty())
    {
        Some(token) => {
            ui.warn(
                "using the CURIE_SLACK_BOT_TOKEN override instead of the release's \
                 own Slack token: if this bot is not the one the worker posts as, \
                 Slack will refuse the reply edit and the placeholder will stay \
                 unchanged",
            );
            token
        }
        None => crate::ops::discover_slack_bot_token(&opts.namespace, &opts.release).await?,
    };

    let valkey_url = format!(
        "redis://:{}@localhost:{}",
        opts.valkey_password, opts.valkey_local_port
    );
    let mut conn = connect(&valkey_url).await?;
    enqueue_over_connected_transport(&opts, &mut conn, TurnVerb::Cluster, &channel, &bot_token)
        .await
}

pub async fn message(mut opts: MessageOpts) -> Result<()> {
    // An empty `--thread` is normalized to `None` for every tier and transport,
    // rather than at one leaf: this is the single entry point they all funnel
    // through. See [`normalize_thread`] for why an empty ts is not a thread.
    //
    // Known wrinkle: this runs AFTER `state::apply_continue`, which resolves
    // `thread: cli.thread.or_else(|| persisted)`. `Some("")` is a `Some`, so it
    // wins there and the persisted thread never loads. `--continue --thread ""`
    // therefore starts a NEW thread instead of resuming the recorded one.
    opts.thread = normalize_thread(opts.thread);
    if opts.local {
        return message_local(opts).await;
    }
    let ui = crate::ui::ui();
    if opts.dry_run {
        let host = opts
            .listen_host
            .clone()
            .unwrap_or_else(|| "<auto-detected-local-ip>".to_string());
        ui.emit(&MessageDryRunOutput {
            target: "cluster",
            stream: opts.stream.clone(),
            channel: opts.channel.clone(),
            reply_endpoint: advertised_url(&host, opts.listen_port),
            human_lines: dry_run_lines(&opts, &host),
        });
        return Ok(());
    }

    require_on_path("kubectl")?;

    // Connected-transport path (#770/ADR-0078): when a real workspace is
    // connected (a running dispatcher), post a real placeholder and enqueue
    // against its ts with no per-turn endpoint, so the approval card and any
    // resumed reply ride the connected transport -- no throwaway stub. A kubectl
    // failure reads as NOT connected, so this falls through to the stub path.
    if crate::ops::dispatcher_connected(&opts.namespace, &opts.release).await {
        return message_connected(opts).await;
    }

    // Advertise a host the in-cluster worker can reach, then bind the stub on
    // 0.0.0.0 so it is reachable off-box. Take the URL from the started stub so an
    // ephemeral --listen-port 0 still yields the real bound port.
    let advertise_host = resolve_advertise_host(opts.listen_host.as_deref()).await?;
    let mut stub = SlackStub::start("0.0.0.0", opts.listen_port, &advertise_host).await?;
    let url = stub.base_api_url().to_string();
    ui.plumbing(&format!(
        "slack stub listening; the worker will post to {url}"
    ));

    // Valkey port-forward for the enqueue (killed on drop at fn end).
    let _valkey_pf = start_port_forward(
        &port_forward_command(
            &opts.namespace,
            &opts.release,
            "valkey",
            opts.valkey_local_port,
            VALKEY_REMOTE_PORT,
        ),
        opts.valkey_local_port,
        "valkey",
    )
    .await?;

    // Channel: explicit --channel, else the sole deployed agent via a
    // short-lived API port-forward (#766). Shared with the connected path.
    let (channel, agent_hint) = resolve_cluster_channel(&opts).await?;
    ui.plumbing(&format!("routing to channel {channel}"));

    // Enqueue the exact event the dispatcher would produce and wait for the ack.
    // The turn carries its reply endpoint (this stub's advertised URL) on the
    // payload (issue #19), so the in-cluster worker posts THIS turn's reply back
    // to the stub without a worker-global `helm upgrade`; a real workspace on the
    // same worker keeps replying to real Slack.
    let valkey_url = format!(
        "redis://:{}@localhost:{}",
        opts.valkey_password, opts.valkey_local_port
    );
    let mut conn = connect(&valkey_url).await?;
    let (channel, thread_ts, placeholder_ts) =
        resolve_targets(Some(&channel), opts.thread.as_deref());
    let event = synthetic_turn(
        "slack",
        &channel,
        &opts.user,
        &opts.text,
        &thread_ts,
        &placeholder_ts,
        Some(url),
    );
    let stream_id = xadd(&mut conn, &opts.stream, &event).await?;
    ui.plumbing(&format!(
        "enqueued {} on {} as {stream_id}",
        event.event_id, opts.stream
    ));
    ui.plumbing(&format!(
        "waiting up to {}s for the worker to finalize the turn...",
        opts.timeout_secs
    ));

    let cl = ui.checklist();
    let step = cl.step("waiting for worker reply");
    let wait_started = Instant::now();
    let outcome = {
        let mut observe_update = |text: &str| {
            if let Some(line) = text.lines().rev().find(|line| !line.trim().is_empty()) {
                step.tick_detail(line.trim());
            }
        };
        await_reply(
            &mut stub,
            &mut conn,
            &opts.stream,
            &stream_id,
            &placeholder_ts,
            Duration::from_secs(opts.timeout_secs),
            &mut observe_update,
        )
        .await
    };

    match outcome {
        Outcome::Replied(reply) => {
            step.done("");
            ui.emit(&MessageOutcomeOutput::Replied {
                thread: thread_ts.clone(),
                reply,
            });
            persist_and_hint(&opts, TurnVerb::Cluster, &channel, &thread_ts);
            Ok(())
        }
        Outcome::CompletedNoEdit => {
            step.done("no edit");
            ui.emit(&MessageOutcomeOutput::NoEdit {
                thread: thread_ts.clone(),
            });
            persist_and_hint(&opts, TurnVerb::Cluster, &channel, &thread_ts);
            Ok(())
        }
        Outcome::AwaitingApproval(reply) => {
            step.done("awaiting approval");
            // Persist the turn context BEFORE the (possibly full --timeout-secs)
            // approval wait, so an interrupted terminal still leaves a thread
            // `message --continue` can recover (#766). Terminal paths re-persist
            // the identical context and print the continue hint once.
            persist_turn_quietly(&opts, TurnVerb::Cluster, &channel, &thread_ts);
            // Keep the stub alive and wait for the resumed reply instead of
            // exiting and stranding it (#766). The wait observes the resume turn
            // on the runs stream over the Valkey connection already open for the
            // enqueue, so no API port-forward is needed for it. If we cannot parse
            // an approval id, fall back to the awaiting-approval terminal rather
            // than hanging.
            match parse_approval_id(reply.as_deref().unwrap_or_default()) {
                Some(id) => {
                    let remaining = Duration::from_secs(opts.timeout_secs)
                        .saturating_sub(wait_started.elapsed());
                    // `_valkey_pf` stays alive across this await, which is what
                    // keeps `conn` usable for the resume scan.
                    match resume_after_approval(
                        &opts,
                        TurnVerb::Cluster,
                        &mut conn,
                        &id,
                        &mut stub,
                        &stream_id,
                        &placeholder_ts,
                        &thread_ts,
                        &channel,
                        agent_hint.as_deref(),
                        reply,
                        remaining,
                    )
                    .await
                    {
                        ResumeExit::Done => Ok(()),
                        ResumeExit::Transient => {
                            // Drop the Slack stub AND the Valkey port-forward first:
                            // `process::exit` does not unwind, so without dropping
                            // them explicitly here neither the stub's listener nor
                            // the `kill_on_drop` port-forward child guard would ever
                            // run, leaking the stub's bound port (#751) and
                            // orphaning the `kubectl port-forward` child to init
                            // (#766).
                            exit_after_drop(crate::exit::ExitClass::Transient, (stub, _valkey_pf));
                        }
                    }
                }
                None => {
                    // No parseable approval id, so we never entered the resume wait.
                    // Same parked terminal as the timeout arm, so exit with the SAME
                    // transient class (not 0) for a deterministic scripted contract
                    // (#766, N5). Drop the stub and port-forward first so neither is
                    // leaked/orphaned by the non-unwinding `process::exit` (#751,
                    // #766).
                    ui.emit(&MessageOutcomeOutput::AwaitingApproval {
                        thread: thread_ts.clone(),
                        reply,
                        tier: tier_str(TurnVerb::Cluster),
                        agent: agent_hint.clone(),
                        channel: channel.clone(),
                    });
                    persist_and_hint(&opts, TurnVerb::Cluster, &channel, &thread_ts);
                    exit_after_drop(crate::exit::ExitClass::Transient, (stub, _valkey_pf));
                }
            }
        }
        Outcome::TimedOut => {
            step.fail(&format!("timed out after {}s", opts.timeout_secs));
            // Drop the Slack stub's listener IMMEDIATELY on timeout, before the
            // diagnostics gather below -- same reasoning as `message_local`'s
            // TimedOut arm (#751). The Valkey port-forward (`_valkey_pf`) must
            // stay alive a bit longer: `diagnostics` still needs it for `conn`.
            drop(stub);
            // Gather diagnostics only on the human path; under `--json` the
            // timeout object carries no diagnostics, so skip the extra Valkey read.
            let diag = if ui.json() {
                None
            } else {
                Some(bounded_diagnostics(&mut conn, &opts.stream, &stream_id).await)
            };
            ui.emit(&MessageOutcomeOutput::TimedOut {
                diagnostics: diag,
                resume_note: None,
            });
            // A timeout is retryable (the worker may still be working, or a
            // transient stall), so it maps to the transient exit code, not
            // failure. Drop the Valkey port-forward now, for the same
            // non-unwinding reason as the stub above (#766).
            exit_after_drop(crate::exit::ExitClass::Transient, _valkey_pf);
        }
    }
}

// ---------------------------------------------------------------------------
// eval: the same evals/cases.json at the local and cluster tiers
// ---------------------------------------------------------------------------

/// Options for `curie local eval` / `curie cluster eval`, mirroring their
/// clap flags. The connection surface is the `message` subset (no per-turn
/// `text`/`thread`); `cases` selects the suite and `local` picks the tier.
pub struct EvalOpts {
    /// Explicit eval-case file; `None` resolves `evals/cases.json` like
    /// `skill eval` (cwd, then the recorded bundle dir).
    pub cases: Option<PathBuf>,
    pub channel: Option<String>,
    pub namespace: String,
    pub release: String,
    pub listen_host: Option<String>,
    pub listen_port: u16,
    pub valkey_local_port: u16,
    pub valkey_password: String,
    pub api_local_port: u16,
    pub api_key: String,
    pub user: String,
    pub stream: String,
    pub timeout_secs: u64,
    pub dry_run: bool,
    /// Local mode: drive the compose stack instead of a Kubernetes release.
    pub local: bool,
    /// Local mode only: platform API base URL for the channel lookup.
    pub api_url: Option<String>,
    /// Models to sweep (#526). Empty = the default parity-gate run (grade the
    /// deployed model in-CLI). Non-empty switches to the platform eval plane: one
    /// `POST /evals/trigger` per model, then poll `GET /evals/matrix` for the
    /// per-model pass-rate so the run lands in the matrix sliced by model.
    pub models: Vec<String>,
    /// Requested eval concurrency (#706). The CLI eval loop is sequential today;
    /// real parallel dispatch is worker-side and tracked in #709, so any value
    /// above 1 is refused up front rather than silently run sequentially.
    pub concurrency: usize,
}

/// Resolve the requested eval concurrency to the only value the CLI eval loop
/// supports today: sequential (1). Real parallel dispatch is worker-side and
/// tracked in #709, so any request above 1 is refused loudly rather than
/// silently downgraded to sequential without telling the caller (issue #706).
/// `0` is likewise refused rather than normalized to 1: it is not a valid
/// concurrency (there is no such thing as running zero cases at a time), so
/// silently accepting it as sequential would misreport the plan (a `--dry-run`
/// would otherwise print "sequential (0)").
pub fn resolve_eval_concurrency(requested: usize) -> anyhow::Result<usize> {
    if requested == 0 {
        return Err(anyhow::anyhow!(
            "concurrency must be at least 1 (0 is not a valid eval concurrency)"
        ));
    }
    if requested == 1 {
        return Ok(1);
    }
    Err(anyhow::anyhow!(
        "concurrency > 1 not yet supported; parallel eval dispatch is tracked in #709"
    ))
}

/// Count the nodes a run could actually be scheduled onto from the stdout of
/// `kubectl get nodes -o json`: a node counts only when it is Ready (a
/// `status.conditions` entry with `type=Ready` and `status=True`) AND not
/// cordoned (`spec.unschedulable` absent or false). Malformed, empty, or absent
/// JSON yields 0 rather than panicking, so a probe failure never masquerades as
/// a healthy multi-node cluster (issue #706).
pub fn schedulable_node_count(nodes_json: &str) -> usize {
    let Ok(root) = serde_json::from_str::<serde_json::Value>(nodes_json) else {
        return 0;
    };
    let Some(items) = root.get("items").and_then(|v| v.as_array()) else {
        return 0;
    };
    items
        .iter()
        .filter(|node| {
            let cordoned = node
                .get("spec")
                .and_then(|spec| spec.get("unschedulable"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if cordoned {
                return false;
            }
            node.get("status")
                .and_then(|status| status.get("conditions"))
                .and_then(|c| c.as_array())
                .map(|conditions| {
                    conditions.iter().any(|cond| {
                        cond.get("type").and_then(|t| t.as_str()) == Some("Ready")
                            && cond.get("status").and_then(|s| s.as_str()) == Some("True")
                    })
                })
                .unwrap_or(false)
        })
        .count()
}

/// Grade one tier turn's reply with the SAME grader `skill eval` uses, gated on
/// the case's `expect_status` -- the message-path mirror of `evals::turn_passes`'s
/// generalized gate. A default (`done`) case passes only when the worker finalized
/// it WITH reply text (`Replied`) satisfying the grader; an `awaiting-approval`
/// case passes only when the turn parked awaiting approval (the gate held) and the
/// latest placeholder text satisfies the grader. Any other outcome fails.
pub fn reply_passes(case: &EvalCase, outcome: &Outcome) -> bool {
    match case.expect_status {
        // Default: the turn must have finalized WITH reply text and satisfy the grader.
        ExpectedStatus::Done => match outcome {
            // The message-relay path observes only the reply text, not the turn's
            // tool-call trajectory, so a tool_called grader has nothing to read
            // here and fails closed. The trajectory-aware grade lives on the
            // `skill eval` path (`turn_passes`) and the server-side eval matrix.
            Outcome::Replied(reply) => case.grader.grade(reply, &[]),
            Outcome::CompletedNoEdit | Outcome::AwaitingApproval(_) | Outcome::TimedOut => false,
        },
        // Gate-blocked assertion: the turn must have parked awaiting approval, and
        // the latest placeholder text (the model's narration before the gate flip)
        // must satisfy the grader. A match-anything grader ({kind:contains,expected:""})
        // asserts purely on the gate holding.
        ExpectedStatus::AwaitingApproval => match outcome {
            Outcome::AwaitingApproval(reply) => {
                case.grader.grade(reply.as_deref().unwrap_or_default(), &[])
            }
            Outcome::Replied(_) | Outcome::CompletedNoEdit | Outcome::TimedOut => false,
        },
    }
}

/// The plan a `--dry-run` eval prints: the tier, the suite/case count, and the
/// same enqueue/port-forward description a real run would produce. Pure so the
/// rendering is unit-testable with no stack or cluster (mirrors `dry_run_lines`).
pub fn eval_dry_run_lines(opts: &EvalOpts, suite_name: &str, case_count: usize) -> Vec<String> {
    let tier = if opts.local { "local" } else { "cluster" };
    // A `--model` sweep (#526) is the platform eval plane, so its plan is the
    // trigger-per-model + matrix-poll shape, not the message enqueue path.
    if !opts.models.is_empty() {
        let api_base = if opts.local {
            local_api_base(opts.api_url.as_deref())
        } else {
            format!(
                "http://localhost:{} (via api port-forward)",
                opts.api_local_port
            )
        };
        // The worker grades the DEPLOYED bundle's cases, not the local suite, so
        // the plan names the suite but does not present the local case count as
        // the platform truth (issue #608).
        let mut lines = vec![format!(
            "sweep {} model(s) over suite {suite_name:?} on the {tier} platform eval plane \
             (the deployed bundle's cases are graded server-side)",
            opts.models.len()
        )];
        let target = match opts.channel.as_deref() {
            Some(channel) => format!("channel {channel}"),
            None => "the sole deployed agent".to_string(),
        };
        for model in &opts.models {
            lines.push(format!(
                "POST {api_base}/evals/trigger {{agent: {target}, suite: {suite_name:?}, model: {model:?}}}"
            ));
        }
        lines.push(format!(
            "then poll {api_base}/evals/matrix?suite={suite_name} for per-model pass-rate"
        ));
        return lines;
    }
    let mut lines = vec![format!(
        "grade {case_count} case(s) from suite {suite_name:?} against the {tier} tier"
    )];
    if opts.local {
        let valkey_url = local_valkey_url(&opts.valkey_password);
        let api_base = local_api_base(opts.api_url.as_deref());
        lines.push("local mode (compose stack; no kubectl/helm)".to_string());
        lines.push(format!("enqueue onto redis {valkey_url}"));
        lines.push(format!(
            "stub advertised at http://localhost:{DEFAULT_LOCAL_STUB_PORT}/api/"
        ));
        match opts.channel.as_deref() {
            Some(channel) => lines.push(format!("channel {channel}")),
            None => lines.push(format!(
                "channel <the sole bound (agent, Slack channel) pair via {api_base}/agents>"
            )),
        }
    } else {
        let host = opts
            .listen_host
            .clone()
            .unwrap_or_else(|| "<auto-detected-local-ip>".to_string());
        lines.push(
            port_forward_command(
                &opts.namespace,
                &opts.release,
                "valkey",
                opts.valkey_local_port,
                VALKEY_REMOTE_PORT,
            )
            .display(),
        );
        if opts.channel.is_none() {
            lines.push(
                port_forward_command(
                    &opts.namespace,
                    &opts.release,
                    "api",
                    opts.api_local_port,
                    API_REMOTE_PORT,
                )
                .display(),
            );
        }
        lines.push(format!(
            "stub advertised at {}",
            advertised_url(&host, opts.listen_port)
        ));
    }
    lines.push(format!("concurrency: sequential ({})", opts.concurrency));
    lines.push(format!(
        "enqueue one synthetic QueuedTurn per case on stream {}",
        opts.stream
    ));
    lines
}

/// Resolve the eval suite the way `skill eval` does: an explicit `--cases`
/// wins, else `evals/cases.json` in the cwd, then the recorded bundle dir.
fn resolve_eval(explicit: Option<PathBuf>) -> Result<LoadedEval> {
    let state_plugin_dir = crate::state::load(Path::new("."))?.map(|s| PathBuf::from(s.plugin_dir));
    let path =
        crate::commands::resolve_cases_path(explicit, Path::new("."), state_plugin_dir.as_deref())?;
    crate::evals::load_eval(&path)
}

/// The shared per-tier eval engine: enqueue one synthetic `QueuedTurn` per case
/// through the already-stood-up stub + Valkey (the same enqueue+await path a
/// single `message` walks), grade the captured reply, and collect
/// `(id, passed, seconds, output)` rows for `report_eval`. Tier-agnostic: the
/// caller binds the stub/connection for its tier, then hands them here.
async fn run_eval_turns(
    opts: &EvalOpts,
    channel: &str,
    suite: &EvalSuite,
    conn: &mut MultiplexedConnection,
    stub: &mut SlackStub,
) -> Result<crate::commands::EvalReport> {
    let ui = crate::ui::ui();
    let total = suite.cases.len();
    let bar = ui.progress_bar(total as u64, "running evals");
    let mut results: Vec<crate::commands::EvalRow> = Vec::with_capacity(total);
    for case in &suite.cases {
        // Each case is its own thread so turns never cross-talk on the stub.
        let (channel_id, thread_ts, placeholder_ts) = resolve_targets(Some(channel), None);
        let reply_endpoint = stub.base_api_url().to_string();
        let event = synthetic_turn(
            "slack",
            &channel_id,
            &opts.user,
            &case.input,
            &thread_ts,
            &placeholder_ts,
            Some(reply_endpoint),
        );
        let started = Instant::now();
        let stream_id = xadd(conn, &opts.stream, &event).await?;
        let mut observe_update = |_: &str| {};
        let outcome = await_reply(
            stub,
            conn,
            &opts.stream,
            &stream_id,
            &placeholder_ts,
            Duration::from_secs(opts.timeout_secs),
            &mut observe_update,
        )
        .await;
        let elapsed = started.elapsed().as_secs_f64();
        // Carry the reply text (#548) so a red case is diagnosable from --json /
        // the human summary; a non-Replied outcome has no gradeable text.
        let output = match &outcome {
            Outcome::Replied(reply) => reply.clone(),
            Outcome::AwaitingApproval(reply) => reply.clone().unwrap_or_default(),
            Outcome::CompletedNoEdit => String::new(),
            // A timed-out case surfaces the stream/consumer diagnostics the same
            // way the non-eval message path does (#706), so the failure is not a
            // silent empty string but the stream state that explains it.
            Outcome::TimedOut => diagnostics(conn, &opts.stream, &stream_id).await,
        };
        results.push((
            case.id.clone(),
            if reply_passes(case, &outcome) {
                crate::evals::CaseOutcome::Pass
            } else {
                crate::evals::CaseOutcome::Fail
            },
            elapsed,
            output,
        ));
        bar.inc(1);
    }
    bar.finish();
    Ok(crate::commands::EvalReport::from_rows(results))
}

/// The shared `eval` handler: run the bundle's `evals/cases.json` through the
/// target tier's message enqueue+await path and grade with the shared grader,
/// so a suite that passes at `skill` can be re-asserted verbatim at `local` and
/// `cluster` (issue #344, the per-tier parity gate).
pub async fn eval(opts: EvalOpts) -> Result<()> {
    // Refuse `--concurrency > 1` before any enqueue or work (#706): the CLI eval
    // loop is sequential and real parallel dispatch is tracked in #709, so a
    // request above 1 fails fast rather than silently running sequentially.
    let _ = resolve_eval_concurrency(opts.concurrency)?;
    // A `--model` sweep (#526) is the platform eval plane, not the in-CLI parity
    // gate: it triggers a matrix-producing run per model and reads the comparison
    // back off GET /evals/matrix. It is orthogonal to the tier's message path.
    if !opts.models.is_empty() {
        // A `--cases` override cannot take effect on a sweep: the sweep only sends
        // the suite NAME to `POST /evals/trigger`; the worker reloads the cases
        // from the DEPLOYED bundle server-side. Grading a local case file this way
        // is impossible, so refuse rather than silently evaluate the deployed
        // cases while displaying the local ones (issue #608). Exit 4 (Unsupported,
        // ADR-0041): the flag is understood but does not apply to this plane by
        // construction, so no input or retry changes that -- the fix names the
        // path that does honor a local suite.
        if opts.cases.is_some() {
            return Err(anyhow::Error::from(
                crate::exit::CliError::unsupported(
                    "--cases has no effect on a --model sweep: the sweep runs a platform eval \
                     that grades the deployed bundle's evals/cases.json server-side, so a local \
                     case file is never read",
                )
                .with_fix(
                    "drop --cases to sweep the deployed suite, or omit --model to grade a local \
                     suite in-CLI with `curie <skill|local|cluster> eval --cases <file>`",
                ),
            ));
        }
        let loaded = resolve_eval(opts.cases.clone())?;
        return eval_sweep(opts, loaded.suite).await;
    }
    let loaded = resolve_eval(opts.cases.clone())?;
    if loaded.trajectory.is_some() {
        if opts.cases.is_some() {
            return Err(anyhow::Error::from(
                crate::exit::CliError::unsupported(
                    "--cases cannot select a local file for a trajectory eval because local and cluster trajectory scoring grades the deployed bundle",
                )
                .with_fix(
                    "drop --cases to grade the deployed trajectory suite, or use skill eval to grade the local file",
                ),
            ));
        }
        return eval_trajectory_platform(opts, loaded.suite).await;
    }
    if opts.local {
        eval_local(opts, loaded.suite).await
    } else {
        eval_cluster(opts, loaded.suite).await
    }
}

/// Poll interval and cap while waiting for triggered eval jobs to land in the
/// matrix. The cap scales with model count because the eval consumer handles
/// entries sequentially (`count=1`), so N models run one after another.
const SWEEP_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Resolve the target agent's id for the trigger plane. Mirrors `select_channel`
/// (explicit `--channel` matches an agent's channel, else the sole
/// deployed agent), but returns the agent id the trigger endpoint keys on.
pub fn select_agent_id(agents: &[Agent], channel: Option<&str>) -> Result<String> {
    if let Some(channel) = channel {
        return agents
            .iter()
            .find(|a| a.channels.iter().any(|c| c.address == channel))
            .map(|a| a.id.clone())
            .ok_or_else(|| anyhow::anyhow!("no deployed agent has channel {channel:?}"));
    }
    match agents {
        [] => bail!(
            "no agents are deployed on the platform API; deploy one with `curie local deploy` \
             or `curie cluster deploy`, or pass --channel <id>"
        ),
        [only] => Ok(only.id.clone()),
        many => {
            let listed = many
                .iter()
                .flat_map(|a| {
                    a.channels
                        .iter()
                        .map(move |c| format!("{} -> {}", a.name, c.address))
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!("multiple agents are deployed; pass --channel <id> to pick one ({listed})")
        }
    }
}

/// Refuse a `--model` sweep against a stack running the fake model (#606, AC2 of
/// #612). Pure so the class and the wording are testable with no stack.
///
/// The fake never calls a model: it returns one canned reply whatever the input
/// and whatever the requested model, so a sweep of N models compares one string
/// to itself N times and reports a comparison that never happened. The default
/// parity-gate run (no `--model`) on a fake stack is the DOCUMENTED onboarding
/// loop and stays allowed -- it asserts plumbing, and it claims nothing about
/// any model.
///
/// Usage (exit 2), never `Unsupported` (exit 4): supplying a credential makes
/// this exact argv work, so a model sweep is not absent from this tier by
/// construction, which is ADR-0041's boundary for exit 4.
pub fn guard_fake_sweep(fake: bool, models: &[String], local: bool) -> Result<()> {
    if !fake || models.is_empty() {
        return Ok(());
    }
    let fix = if local {
        "set CURIE_CREDENTIALS to a real model credential and re-run `curie local up`, then \
         sweep again"
    } else {
        "re-install the release with a real model (`--set agentSandbox.runner.fakeModel=false`, \
         plus a model credential) and sweep again"
    };
    // The reason has to be the one that actually applies. With a single
    // `--model` there is no comparison to fabricate: the request simply cannot
    // be honored. The comparison-axis rationale is only true from two up.
    let why = if models.len() == 1 {
        format!(
            "this stack runs the fake model, so it will never call `{}`: the fake answers every \
             input with the same scripted text whatever --model asks for. The run would be that \
             canned script, not the model you pinned.",
            models[0]
        )
    } else {
        format!(
            "this stack runs the fake model, so sweeping {} models would compare one canned reply \
             to itself: the fake never calls a model, and answers every input with the same \
             scripted text whatever --model asks for. Every row would be labelled fake-model and \
             carry an identical answer, so the comparison would be fabricated.",
            models.len()
        )
    };
    Err(anyhow::Error::from(
        crate::exit::CliError::usage(why).with_fix(fix),
    ))
}

/// Read the fake-ness of the tier's DEPLOYED worker: the already-composed value
/// of `CURIE_FAKE_MODEL` on the artifact that is actually running.
///
/// Deliberately a read of the output, not a re-derivation of the input. The
/// chart's effective value is the composite `fakeModel AND NOT inference.deploy`
/// and compose's is `${CURIE_FAKE_MODEL:-1}`; re-deriving either in the CLI
/// would give a second config that drifts from the truth (an
/// `inference.deploy` + `fakeModel=true` install is a REAL install, and shell
/// env is not what the running container was booted with). A probe failure is
/// reported as itself; it never falls back to a default guess.
async fn probe_fake_model(opts: &EvalOpts) -> Result<bool> {
    let env = if opts.local {
        let worker = local_worker_container().await?;
        let cmd = OpsCommand::new(
            "docker",
            vec![
                plain("inspect"),
                plain(&worker),
                plain("--format"),
                plain("{{range .Config.Env}}{{println .}}{{end}}"),
            ],
        );
        let (ok, stdout, stderr) = run_capture(&cmd).await?;
        if !ok {
            bail!(
                "inspecting the local worker container {worker}: {}",
                stderr.trim()
            );
        }
        stdout
            .lines()
            .find_map(|l| l.strip_prefix("CURIE_FAKE_MODEL="))
            .map(str::to_string)
    } else {
        require_on_path("kubectl")?;
        let deployment = format!("deployment/{}-worker", opts.release);
        let cmd = OpsCommand::new(
            "kubectl",
            vec![
                plain("-n"),
                plain(&opts.namespace),
                plain("get"),
                plain(&deployment),
                plain("-o"),
                plain(
                    "jsonpath={.spec.template.spec.containers[*].env[?(@.name==\"CURIE_FAKE_MODEL\")].value}",
                ),
            ],
        );
        let (ok, stdout, stderr) = run_capture(&cmd).await?;
        if !ok {
            bail!(
                "reading {deployment} in namespace {} to check whether the release runs the fake \
                 model: {}",
                opts.namespace,
                stderr.trim()
            );
        }
        let value = stdout.trim().to_string();
        (!value.is_empty()).then_some(value)
    };
    // An absent variable means the worker was booted without the flag at all,
    // which is the live path on both tiers (compose only defaults to fake
    // through `${CURIE_FAKE_MODEL:-1}`, which materializes the value).
    Ok(env
        .as_deref()
        .is_some_and(crate::local::fake_model_is_truthy))
}

/// The compose service the worker runs as, per `compose.dev.yaml`. Container
/// NAMES vary with the compose project (`<project>-curie-worker-1`), so the
/// service label is the only stable selector; `cli/tests/fake_tier_plumbing.rs`
/// pins this against the compose file so a service rename cannot silently
/// blind the probe again.
pub(crate) const COMPOSE_WORKER_SERVICE: &str = "curie-worker";

/// The label selector the probe matches on, quoted into diagnostics so an
/// operator can re-run the same `docker ps` and see what the CLI saw.
fn worker_label_selector() -> String {
    format!("label=com.docker.compose.service={COMPOSE_WORKER_SERVICE}")
}

fn worker_ps_command() -> OpsCommand {
    OpsCommand::new(
        "docker",
        vec![
            plain("ps"),
            plain("--filter"),
            plain(worker_label_selector()),
            plain("--format"),
            plain("{{.Names}}"),
        ],
    )
}

/// Pick the one running compose worker from `docker ps` output. Zero or many is
/// an explicit diagnostic: guessing which stack the sweep would hit is exactly
/// the fabrication this probe exists to prevent. Both diagnostics name the
/// selector rather than asserting a stack-wide fact the probe did not check --
/// "no container matched X" is verifiable; "there is no stack" is not.
fn select_worker_container(stdout: &str) -> Result<String> {
    let names: Vec<&str> = stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    match names.as_slice() {
        [only] => Ok((*only).to_string()),
        [] => bail!(
            "no running container matches `{}`, so the sweep cannot read which model the local \
             stack is running. Start a stack with `curie local up`.",
            worker_label_selector()
        ),
        many => bail!(
            "{} running containers match `{}` ({}); a sweep cannot tell which stack it would \
             measure. Stop the extras with `curie local down`.",
            many.len(),
            worker_label_selector(),
            many.join(", ")
        ),
    }
}

async fn local_worker_container() -> Result<String> {
    let cmd = worker_ps_command();
    let (ok, stdout, stderr) = run_capture(&cmd).await?;
    if !ok {
        bail!("listing the local worker container: {}", stderr.trim());
    }
    select_worker_container(&stdout)
}

/// The `--model` sweep at the local/cluster tier: enqueue one platform eval per
/// model against the agent's active dev version, then poll the matrix for the
/// per-model pass-rate the recorder writes (#526). Unlike the skill sweep (which
/// boots throwaway runners and grades in-CLI), this drives the platform eval
/// plane so results are sliceable by model in `GET /evals/matrix`.
async fn eval_sweep(opts: EvalOpts, suite: EvalSuite) -> Result<()> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        // A dry run is an offline, non-mutating plan: it does not probe the
        // runtime, so it must not claim what the current stack would do.
        ui.emit(&crate::ui::DryRunPlan {
            lines: eval_dry_run_lines(&opts, &suite.name, suite.cases.len()),
        });
        return Ok(());
    }
    guard_fake_sweep(probe_fake_model(&opts).await?, &opts.models, opts.local)?;

    // The trigger + matrix reads go over the platform API. Local reaches it
    // directly; cluster tunnels an api port-forward kept alive for the whole poll.
    let (api, _api_pf) = if opts.local {
        let base = local_api_base(opts.api_url.as_deref());
        (ApiClient::new(&base, &opts.api_key)?, None)
    } else {
        require_on_path("kubectl")?;
        let pf = start_port_forward(
            &port_forward_command(
                &opts.namespace,
                &opts.release,
                "api",
                opts.api_local_port,
                API_REMOTE_PORT,
            ),
            opts.api_local_port,
            "api",
        )
        .await?;
        let base = format!("http://localhost:{}", opts.api_local_port);
        (ApiClient::new(&base, &opts.api_key)?, Some(pf))
    };

    let agents = api
        .list_agents()
        .await
        .context("listing agents to resolve the eval target")?;
    let agent_id = select_agent_id(&agents, opts.channel.as_deref())?;

    // The suite NAME is what the worker keys on; the cases it grades come from the
    // DEPLOYED bundle, not the local suite, so we do NOT present the local case
    // count as the platform truth (issue #608).
    ui.note(&format!(
        "model sweep: {} model(s) over suite {:?} via the platform eval plane \
         (the worker grades the deployed bundle's cases)",
        opts.models.len(),
        suite.name,
    ));
    let cl = ui.checklist();
    // Every model triggers against the agent's active dev version, so all jobs
    // share one commit sha; capture it to scope the poll to THIS run (#608).
    let mut triggered_sha: Option<String> = None;
    for model in &opts.models {
        let step = cl.step(&format!("enqueue {model}"));
        let res = api
            .trigger_eval(&agent_id, Some(&suite.name), Some(model))
            .await
            .with_context(|| format!("triggering eval for model {model}"))?;
        step.done(&format!(
            "{} @ {}",
            res.stream_id,
            &res.sha[..res.sha.len().min(8)]
        ));
        triggered_sha = Some(res.sha);
    }
    // Unreachable in practice (eval() only routes here with a non-empty --model
    // list), but keep the sha resolution total rather than panicking.
    let triggered_sha = triggered_sha
        .context("a --model sweep must trigger at least one eval to establish the run's sha")?;

    let want: std::collections::BTreeSet<&str> = opts.models.iter().map(String::as_str).collect();
    // The deadline derives from --timeout-secs so the documented recovery path --
    // the timeout error tells the user to raise it -- actually works (#608).
    let deadline = Instant::now() + Duration::from_secs(opts.timeout_secs);
    ui.note("waiting for the eval jobs to land in the matrix (Ctrl-C to stop; jobs keep running)");
    loop {
        let matrix = api
            .eval_matrix(&suite.name, 5, None)
            .await
            .context("reading the eval matrix")?;
        if let Some(rows) = sweep_ready_rows(&matrix, &triggered_sha, &want) {
            return crate::commands::report_sweep(&rows, None);
        }
        if Instant::now() >= deadline {
            // Only report a partial once THIS run's version has landed at all;
            // otherwise nothing for this sweep exists yet and reporting a prior
            // run's rows would be the very lie the scoping guards against (#608).
            let sha_present = matrix.versions.contains(&triggered_sha);
            let rows = scoped_rows(&matrix, &want, &triggered_sha);
            let ready: std::collections::BTreeSet<&str> =
                rows.iter().map(|row| row.model.as_str()).collect();
            let missing = want
                .iter()
                .filter(|m| !ready.contains(**m))
                .copied()
                .collect::<Vec<_>>()
                .join(", ");
            if !sha_present || rows.is_empty() {
                // #622: this used to blame only the worker eval consumer, which
                // pointed the operator at the wrong subsystem when the real
                // cause is that a requested model never resolved -- an
                // unbootable/unregistered id or a missing credential can make
                // the worker's per-model job fail before it ever produces a
                // single trace, so NOTHING for that model lands in the matrix
                // at all (not even a graded "0%" row) and the sweep can only
                // ever time out here. Name the pending model(s) and give both
                // plausible causes; the eval consumer is one of two, not the
                // only one.
                bail!(
                    "timed out waiting for eval results for this run (sha {}); no requested \
                     model landed within {}s (still pending: {missing}). This means either the \
                     worker eval consumer is not running, or one or more of {missing} never \
                     resolved (a typo'd/unregistered model id, or a missing/invalid credential) \
                     so its job never produced a single trace. Check the eval consumer is \
                     running, verify {missing}'s id/credential, or raise --timeout-secs.",
                    &triggered_sha[..triggered_sha.len().min(8)],
                    opts.timeout_secs,
                );
            }
            ui.warn(&format!(
                "timed out waiting on some models ({missing}); reporting what landed so far"
            ));
            return crate::commands::report_sweep(&rows, None);
        }
        tokio::time::sleep(SWEEP_POLL_INTERVAL).await;
    }
}

/// Run a sidecar selected eval through the platform scorer and read its case
/// verdicts from the matrix column created by this trigger.
async fn eval_trajectory_platform(opts: EvalOpts, suite: EvalSuite) -> Result<()> {
    let ui = crate::ui::ui();
    let api_base = if opts.local {
        local_api_base(opts.api_url.as_deref())
    } else {
        format!("http://localhost:{}", opts.api_local_port)
    };
    if opts.dry_run {
        let tier = if opts.local { "local" } else { "cluster" };
        ui.emit(&crate::ui::DryRunPlan {
            lines: vec![
                format!(
                    "grade {} case(s) from suite {:?} against the {tier} tier",
                    suite.cases.len(),
                    suite.name
                ),
                format!(
                    "trigger trajectory suite {:?} through the {tier} platform eval plane",
                    suite.name
                ),
                format!(
                    "POST {api_base}/evals/trigger with no model override, then poll {api_base}/evals/matrix?suite={}",
                    suite.name
                ),
            ],
        });
        return Ok(());
    }

    let (api, _api_pf) = if opts.local {
        (ApiClient::new(&api_base, &opts.api_key)?, None)
    } else {
        require_on_path("kubectl")?;
        let pf = start_port_forward(
            &port_forward_command(
                &opts.namespace,
                &opts.release,
                "api",
                opts.api_local_port,
                API_REMOTE_PORT,
            ),
            opts.api_local_port,
            "api",
        )
        .await?;
        (ApiClient::new(&api_base, &opts.api_key)?, Some(pf))
    };

    let agents = api
        .list_agents()
        .await
        .context("listing agents to resolve the trajectory eval target")?;
    let agent_id = select_agent_id(&agents, opts.channel.as_deref())?;
    let triggered = api
        .trigger_eval(&agent_id, Some(&suite.name), None)
        .await
        .context("triggering trajectory eval")?;
    if triggered.suite != suite.name {
        bail!(
            "trajectory trigger returned suite {:?}, expected {:?}",
            triggered.suite,
            suite.name
        );
    }

    let deadline = Instant::now() + Duration::from_secs(opts.timeout_secs);
    loop {
        let matrix = api
            .eval_matrix(&suite.name, 5, Some(&triggered.stream_id))
            .await
            .context("reading trajectory eval matrix")?;
        if let Some(report) =
            trajectory_matrix_report(&matrix, &triggered.sha, &triggered.stream_id)?
        {
            return crate::commands::report_eval(&report, None);
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out after {}s waiting for trajectory eval results for suite {:?} at sha {}",
                opts.timeout_secs,
                suite.name,
                triggered.sha,
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

fn trajectory_matrix_report(
    matrix: &crate::api::EvalMatrix,
    triggered_sha: &str,
    triggered_stream_id: &str,
) -> Result<Option<crate::commands::EvalReport>> {
    if !matrix
        .versions
        .iter()
        .any(|version| version == triggered_sha)
    {
        return Ok(None);
    }

    if matrix.rows.is_empty() {
        return Ok(None);
    }

    let mut rows = Vec::with_capacity(matrix.rows.len());
    let mut details = std::collections::BTreeMap::new();
    let mut expected_case_count: Option<usize> = None;
    for matrix_row in &matrix.rows {
        let Some(cell) = matrix_row
            .cells
            .iter()
            .find(|cell| cell.version == triggered_sha)
        else {
            return Ok(None);
        };
        if cell.stream_id.as_deref() != Some(triggered_stream_id) {
            bail!(
                "stream filtered eval matrix returned case {:?} from a different run",
                matrix_row.case_id
            );
        }
        if cell.scorer.as_deref() != Some("trajectory") {
            bail!(
                "stream filtered eval matrix returned case {:?} without trajectory scoring",
                matrix_row.case_id
            );
        }
        if cell
            .model
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
        {
            bail!(
                "stream filtered trajectory result for case {:?} has no resolved model",
                matrix_row.case_id
            );
        }
        let case_count = cell.case_count.context(format!(
            "stream filtered trajectory result for case {:?} has no case count",
            matrix_row.case_id
        ))?;
        let case_count = usize::try_from(case_count).context(
            "stream filtered trajectory case count exceeds this platform's supported size",
        )?;
        if case_count == 0 {
            bail!(
                "stream filtered trajectory result for case {:?} has an invalid zero case count",
                matrix_row.case_id
            );
        }
        match expected_case_count {
            Some(expected) if expected != case_count => bail!(
                "stream filtered trajectory matrix disagrees on case count: {expected} and {case_count}"
            ),
            None => expected_case_count = Some(case_count),
            _ => {}
        }
        let outcome = match cell.status.as_str() {
            "pass" => crate::evals::CaseOutcome::Pass,
            "fail" => crate::evals::CaseOutcome::Fail,
            "plumbing_ok" => crate::evals::CaseOutcome::PlumbingOk,
            "missing" => return Ok(None),
            status => bail!(
                "eval matrix returned unknown status {status:?} for case {:?} at sha {triggered_sha}",
                matrix_row.case_id
            ),
        };
        if let Some(detail) = &cell.detail {
            details.insert(matrix_row.case_id.clone(), detail.clone());
        }
        rows.push((matrix_row.case_id.clone(), outcome, 0.0, String::new()));
    }
    let expected_case_count = expected_case_count
        .context("stream filtered trajectory matrix contained no authoritative case count")?;
    if rows.len() < expected_case_count {
        return Ok(None);
    }
    if rows.len() > expected_case_count {
        bail!(
            "stream filtered trajectory matrix returned {} cases but declared {expected_case_count}",
            rows.len()
        );
    }
    Ok(Some(crate::commands::EvalReport { rows, details }))
}

/// The wanted models' rows in the matrix for the TRIGGERED sha, scoped to the
/// sweep's `--model` set and dropping truly-empty rows: a row with `total == 0`
/// is kept when `plumbing > 0` (a fake-model/plumbing-only tier row that
/// genuinely landed, #700, #612/#606) and dropped only when it carries
/// neither a graded case nor a plumbing one -- i.e. this model has not landed
/// at all yet on the triggered sha. Without the `plumbing > 0` half, a
/// plumbing-only model's row would vanish from a report entirely instead of
/// being surfaced as such, exactly the ambiguity #622 and #814 describe.
///
/// Reads `model_version_summaries` filtered to `triggered_sha`, NOT the
/// window-blended `model_summaries`: the blended `completed` sums across every
/// in-window sha, so a model that completed on a prior in-window sha would keep
/// `completed > 0` and hide the triggered sha's zero-completed outcome (#814).
/// Scoping to the triggered sha's own row is what makes `never_completed`
/// honest. The shared filter behind both the readiness check and the timeout
/// partial.
fn scoped_rows(
    matrix: &crate::api::EvalMatrix,
    want: &std::collections::BTreeSet<&str>,
    triggered_sha: &str,
) -> Vec<crate::commands::SweepRow> {
    matrix
        .model_version_summaries
        .iter()
        .filter(|s| s.version == triggered_sha)
        .filter_map(|s| {
            let m = s.model.as_deref()?;
            want.contains(m).then(|| crate::commands::SweepRow {
                model: m.to_string(),
                passed: s.passed as usize,
                // `completed` is what tells a real 0% apart from a model that
                // never produced a completed turn (#622, #526 AC4); read from
                // the triggered sha's own per-version row so a prior sha's
                // completions cannot mask this run's zero-completed outcome
                // (#814).
                completed: s.completed as usize,
                total: s.total as usize,
                plumbing: s.plumbing as usize,
            })
        })
        .filter(|row| row.total > 0 || row.plumbing > 0)
        .collect()
}

/// The rows to report once the triggered sweep has landed, scoped to the run just
/// triggered (issues #608, #814). Returns `Some(rows)` only when BOTH hold, else
/// `None` ("keep polling"):
///   1. `triggered_sha` appears in the matrix's shown version columns -- i.e. at
///      least one trace for THIS run has landed. A change produces a new commit
///      sha, so a prior run's rows carry a different sha; on the first poll,
///      before the new traces exist, the triggered sha is absent and the prior
///      run cannot satisfy the exit condition (the pre-#608 gate exited here); and
///   2. every wanted model has a row for `triggered_sha` with `total > 0` (or a
///      plumbing-only row, #700).
///
/// Because `scoped_rows` reads the per-`(version, model)` `model_version_summaries`
/// filtered to `triggered_sha`, a prior in-window run for the SAME models can no
/// longer satisfy condition 2 for a model whose triggered-sha row has not landed:
/// each model's counts (pass-rate AND completion) are the triggered sha's own, not
/// the blended window. This closes the residual the pre-#814 gate conceded, where
/// a still-present prior row could stand in for a not-yet-landed model and, worse,
/// a prior sha's completions could mask the triggered sha's zero-completed outcome.
fn sweep_ready_rows(
    matrix: &crate::api::EvalMatrix,
    triggered_sha: &str,
    want: &std::collections::BTreeSet<&str>,
) -> Option<Vec<crate::commands::SweepRow>> {
    if !matrix.versions.iter().any(|v| v == triggered_sha) {
        return None;
    }
    let rows = scoped_rows(matrix, want, triggered_sha);
    let ready: std::collections::BTreeSet<&str> =
        rows.iter().map(|row| row.model.as_str()).collect();
    want.iter().all(|m| ready.contains(m)).then_some(rows)
}

async fn eval_local(opts: EvalOpts, suite: EvalSuite) -> Result<()> {
    let ui = crate::ui::ui();
    let valkey_url = local_valkey_url(&opts.valkey_password);
    let api_base = local_api_base(opts.api_url.as_deref());

    if opts.dry_run {
        ui.emit(&crate::ui::DryRunPlan {
            lines: eval_dry_run_lines(&opts, &suite.name, suite.cases.len()),
        });
        return Ok(());
    }

    let mut conn = connect(&valkey_url).await?;
    // Same VM-netns reachability rule as `message_local` (#680): bind + advertise
    // per the host's Docker topology so the compose worker can post replies.
    let binding = local_stub_binding();
    let mut stub = SlackStub::start(
        &binding.bind_host,
        DEFAULT_LOCAL_STUB_PORT,
        &binding.advertise_host,
    )
    .await?;
    ui.note(&format!(
        "slack stub listening; the worker posts to {}",
        stub.base_api_url()
    ));

    let channel = match opts.channel.as_deref() {
        Some(channel) => channel.to_string(),
        None => {
            let api = ApiClient::new(&api_base, &opts.api_key)?;
            let agents = api.list_agents().await.with_context(|| {
                format!("listing agents via {api_base} (is `curie local up` running?)")
            })?;
            select_channel(&agents, None)?
        }
    };
    ui.note(&format!("routing to channel {channel}"));

    let results = run_eval_turns(&opts, &channel, &suite, &mut conn, &mut stub).await?;
    crate::commands::report_eval(&results, None)
}

/// Whether a surfaced enqueue/await error looks like a timeout or a stalled
/// `XADD onto ...` on the runs stream (queue.rs) -- the shape a single-node
/// cluster saturated by concurrent sandbox claims produces (issue #706).
fn looks_like_enqueue_timeout(err: &anyhow::Error) -> bool {
    let chain = format!("{err:#}").to_lowercase();
    chain.contains("timed out") || chain.contains("timeout") || chain.contains("xadd onto")
}

/// Enrich a cluster-eval enqueue/await timeout with a single-node-saturation
/// hint. The opaque `XADD onto curie:runs` error does not name the most common
/// cause on a dev cluster: one schedulable node saturated by concurrent sandbox
/// claims. When there is at most one schedulable node (or the count cannot be
/// read from `kubectl get nodes`), point the operator at `curie cluster
/// status`. The original error stays as the anyhow cause; it is never swallowed.
async fn enrich_cluster_enqueue_timeout(err: anyhow::Error) -> anyhow::Error {
    if !looks_like_enqueue_timeout(&err) {
        return err;
    }
    let hint = match crate::ops::run_capture(&crate::ops::nodes_cmd()).await {
        // Count read cleanly and the cluster has at most one schedulable node.
        Ok((true, out, _)) if schedulable_node_count(&out) <= 1 => Some(
            "this cluster has at most one schedulable node, which a run can saturate with \
             concurrent sandbox claims; check `curie cluster status` for node and sandbox \
             pressure",
        ),
        // Count read cleanly and there is real headroom: no single-node hint.
        Ok((true, _, _)) => None,
        // The node count could not be determined; add the hint softly rather
        // than fail, since single-node saturation is the usual cause here.
        _ => Some(
            "this often indicates a single-node cluster saturated by concurrent sandbox claims; \
             check `curie cluster status`",
        ),
    };
    match hint {
        Some(hint) => err.context(hint.to_string()),
        None => err,
    }
}

async fn eval_cluster(opts: EvalOpts, suite: EvalSuite) -> Result<()> {
    let ui = crate::ui::ui();

    if opts.dry_run {
        ui.emit(&crate::ui::DryRunPlan {
            lines: eval_dry_run_lines(&opts, &suite.name, suite.cases.len()),
        });
        return Ok(());
    }

    require_on_path("kubectl")?;

    let advertise_host = resolve_advertise_host(opts.listen_host.as_deref()).await?;
    let mut stub = SlackStub::start("0.0.0.0", opts.listen_port, &advertise_host).await?;
    ui.note(&format!(
        "slack stub listening; the worker will post to {}",
        stub.base_api_url()
    ));

    // Valkey port-forward for the enqueue, kept alive for the whole eval loop.
    let _valkey_pf = start_port_forward(
        &port_forward_command(
            &opts.namespace,
            &opts.release,
            "valkey",
            opts.valkey_local_port,
            VALKEY_REMOTE_PORT,
        ),
        opts.valkey_local_port,
        "valkey",
    )
    .await?;

    let channel = match opts.channel.as_deref() {
        Some(channel) => channel.to_string(),
        None => {
            let _api_pf = start_port_forward(
                &port_forward_command(
                    &opts.namespace,
                    &opts.release,
                    "api",
                    opts.api_local_port,
                    API_REMOTE_PORT,
                ),
                opts.api_local_port,
                "api",
            )
            .await?;
            let api = ApiClient::new(
                &format!("http://localhost:{}", opts.api_local_port),
                &opts.api_key,
            )?;
            let agents = api
                .list_agents()
                .await
                .context("listing agents through the api port-forward")?;
            select_channel(&agents, None)?
        }
    };
    ui.note(&format!("routing to channel {channel}"));

    let valkey_url = format!(
        "redis://:{}@localhost:{}",
        opts.valkey_password, opts.valkey_local_port
    );
    let mut conn = connect(&valkey_url).await?;

    let results = match run_eval_turns(&opts, &channel, &suite, &mut conn, &mut stub).await {
        Ok(results) => results,
        Err(err) => return Err(enrich_cluster_enqueue_timeout(err).await),
    };
    crate::commands::report_eval(&results, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty `--thread` must not survive as a thread: carried through, it
    /// enqueues `conversation_id: ""` and puts a literal `"thread_ts": ""` in an
    /// outbound chat.postMessage body. The rule is applied at two sites
    /// ([`message`] and [`enqueue_over_connected_transport`]), both of which need
    /// a live Valkey to drive, so this pins the rule itself where CI can run it.
    #[test]
    fn an_empty_thread_normalizes_to_no_thread() {
        assert_eq!(normalize_thread(Some(String::new())), None);
    }

    /// The filter must not eat a real thread ts, and an already-absent thread
    /// stays absent -- the two branches that make the guard safe to apply twice.
    #[test]
    fn a_named_thread_survives_normalization() {
        assert_eq!(
            normalize_thread(Some("1750000000.001900".to_string())),
            Some("1750000000.001900".to_string())
        );
        assert_eq!(normalize_thread(None), None);
    }

    /// `Default` must hand back the crate's real defaults, not zeros: a caller
    /// writing `MessageOpts { text, channel, ..Default::default() }` otherwise
    /// gets an instant deadline and an empty api key that defeats the #540
    /// sentinel comparison in `apply_continue`.
    #[test]
    fn default_message_opts_carry_the_crates_real_defaults() {
        let opts = MessageOpts::default();
        assert_eq!(opts.timeout_secs, DEFAULT_TIMEOUT_SECS);
        assert_eq!(opts.api_key, DEFAULT_API_KEY);
        assert_eq!(opts.listen_port, DEFAULT_LISTEN_PORT);
        assert_eq!(opts.valkey_local_port, DEFAULT_VALKEY_LOCAL_PORT);
        assert_eq!(opts.api_local_port, DEFAULT_API_LOCAL_PORT);
        assert_eq!(opts.valkey_password, DEFAULT_VALKEY_PASSWORD);
        assert_eq!(opts.namespace, "curie");
        assert_eq!(opts.release, "curie");
        assert_eq!(opts.chart, "charts/curie");
    }

    /// The printed resolve hint must be runnable AS PRINTED. Every flag the
    /// server requires has to be on it: the mandatory `<AGENT>` positional,
    /// `--as`, and `--actor-channel` -- without the last one the default
    /// channel-membership approver set
    /// (`SlackChannelMembers.contains`, apps/api/.../slack_approvers.py) refuses
    /// the resolve with 403 and the waiting CLI just times out (#766).
    #[test]
    fn the_resolve_hint_carries_every_flag_the_server_requires() {
        let id = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let line = approval_resolve_command("local", Some("weather-bot"), "C-SIM-abc", id);
        assert_eq!(
            line,
            format!(
                "curie local approvals weather-bot --resolve {id} --as <user> \
                 --actor-channel 'C-SIM-abc'"
            )
        );

        // With no resolved agent name the `<AGENT>` slot keeps the command shape
        // valid so the operator sees the slot to fill, and the channel still rides.
        let line = approval_resolve_command("cluster", None, "C-SIM-xyz", id);
        assert!(line.contains("approvals <AGENT> --resolve"), "{line}");
        assert!(line.contains("--actor-channel 'C-SIM-xyz'"), "{line}");
        assert!(line.starts_with("curie cluster approvals"), "{line}");
    }

    /// The probe is only honest if it filters on the service compose actually
    /// runs the worker as. It previously matched `service=worker`, which exists
    /// nowhere in the compose file, so the fake-sweep guard silently never fired
    /// against a running stack and the CLI reported "no stack" while one ran.
    #[test]
    fn the_probe_matches_the_worker_service_compose_declares() {
        let compose = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../compose.dev.yaml"),
        )
        .expect("compose.dev.yaml is readable from the cli crate");
        assert!(
            compose.contains(&format!("\n  {COMPOSE_WORKER_SERVICE}:\n")),
            "compose.dev.yaml declares no `{COMPOSE_WORKER_SERVICE}:` service, so the probe's \
             docker ps filter would match nothing"
        );
        assert_eq!(
            worker_label_selector(),
            "label=com.docker.compose.service=curie-worker"
        );
        let argv = worker_ps_command().display();
        assert!(
            argv.contains("--filter label=com.docker.compose.service=curie-worker"),
            "docker ps argv lost the service filter: {argv}"
        );
    }

    #[test]
    fn one_matching_container_is_the_worker_to_inspect() {
        assert_eq!(
            select_worker_container("curie-curie-worker-1\n").unwrap(),
            "curie-curie-worker-1"
        );
    }

    /// Zero and many are diagnostics about the SELECTOR, never a claim about
    /// the world the probe did not check.
    #[test]
    fn zero_and_many_matches_are_diagnostics_naming_the_selector() {
        let none = select_worker_container("\n  \n").unwrap_err().to_string();
        assert!(
            none.contains("no running container matches")
                && none.contains("com.docker.compose.service=curie-worker"),
            "{none}"
        );
        let many = select_worker_container("a-curie-worker-1\nb-curie-worker-1\n")
            .unwrap_err()
            .to_string();
        assert!(
            many.contains("2 running containers match")
                && many.contains("a-curie-worker-1, b-curie-worker-1"),
            "{many}"
        );
    }

    fn agent(name: &str, channel: &str) -> Agent {
        agent_bound_to(name, &[channel])
    }

    fn agent_bound_to(name: &str, channels: &[&str]) -> Agent {
        Agent {
            id: format!("id-{name}"),
            name: name.to_string(),
            channels: channels
                .iter()
                .map(|c| crate::api::ChannelBinding {
                    kind: "slack".to_string(),
                    address: c.to_string(),
                })
                .collect(),
            repo_full_name: None,
            approval_required_tools: None,
            approval_routes: None,
            model: None,
            thinking: None,
        }
    }

    fn opts(channel: Option<&str>) -> MessageOpts {
        MessageOpts {
            text: "hi".into(),
            channel: channel.map(str::to_string),
            thread: None,
            namespace: "curie".into(),
            release: "curie".into(),
            chart: "charts/curie".into(),
            listen_host: None,
            listen_port: DEFAULT_LISTEN_PORT,
            valkey_local_port: DEFAULT_VALKEY_LOCAL_PORT,
            valkey_password: DEFAULT_VALKEY_PASSWORD.into(),
            api_local_port: DEFAULT_API_LOCAL_PORT,
            api_key: DEFAULT_API_KEY.into(),
            user: DEFAULT_USER.into(),
            stream: DEFAULT_STREAM.into(),
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            dry_run: false,
            local: false,
            api_url: None,
        }
    }

    /// The full `--api-key` x `$CURIE_API_KEY` truth table. The load-bearing
    /// row is `--api-key ""` under a real env key: an empty flag is an ABSENT
    /// flag, so it must resolve to the env key, never to the dev sentinel.
    #[test]
    fn resolve_api_key_treats_an_empty_flag_exactly_as_an_omitted_one() {
        let real = || Some("sk-real-from-env".to_string());

        // Flag omitted: clap hands the parser its `default_value`, which must
        // survive untouched whatever the env holds.
        assert_eq!(resolve_api_key(DEFAULT_API_KEY, None), DEFAULT_API_KEY);
        assert_eq!(
            resolve_api_key(DEFAULT_API_KEY, Some(String::new())),
            DEFAULT_API_KEY
        );
        assert_eq!(resolve_api_key(DEFAULT_API_KEY, real()), DEFAULT_API_KEY);

        // Env-sourced (clap passes the env value through the parser): empty is
        // absent and falls back to the sentinel, non-empty passes through.
        assert_eq!(resolve_api_key("", Some(String::new())), DEFAULT_API_KEY);

        // The bug: an explicitly empty flag must reconsider the env source,
        // because clap already resolved the flag ahead of `env` and will not.
        assert_eq!(resolve_api_key("", real()), "sk-real-from-env");
        // ...and with no env source at all it lands on the sentinel.
        assert_eq!(resolve_api_key("", None), DEFAULT_API_KEY);

        // An explicit non-empty flag still wins over the env source, and a real
        // credential survives byte-for-byte: normalize the empty case ONLY.
        assert_eq!(resolve_api_key("sk-explicit", real()), "sk-explicit");
        assert_eq!(resolve_api_key("sk-real-key-123", None), "sk-real-key-123");
        // Including a key that happens to look like whitespace-padded input.
        assert_eq!(resolve_api_key(" ", real()), " ");
    }

    /// The cluster tier's parsers carry no dev default, so "nothing supplied"
    /// must survive as an empty string for the handler to discover instead
    /// (#786); an explicit flag still beats the env source.
    #[test]
    fn cluster_credential_parser_reports_an_unsupplied_credential_as_empty() {
        let env = || Some("from-env".to_string());

        assert_eq!(resolve_supplied_credential("", None), "");
        assert_eq!(resolve_supplied_credential("", Some(String::new())), "");
        assert_eq!(resolve_supplied_credential("", env()), "from-env");
        assert_eq!(resolve_supplied_credential("explicit", env()), "explicit");
        assert_eq!(resolve_supplied_credential("explicit", None), "explicit");
    }

    /// An explicit credential is used as-is and the release is never read.
    #[tokio::test]
    async fn cluster_credential_prefers_the_supplied_value_over_discovery() {
        let mut discovered = false;
        let resolved = resolve_cluster_credential(
            Some("supplied-key".to_string()),
            false,
            DEFAULT_API_KEY,
            || {
                discovered = true;
                async { Ok("secret-key".to_string()) }
            },
        )
        .await
        .unwrap();

        assert_eq!(resolved, "supplied-key");
        assert!(
            !discovered,
            "an explicit credential must not hit the cluster"
        );
    }

    /// The #786 defect: with nothing supplied, the cluster tier reads the
    /// release's generated credential instead of sending the dev sentinel.
    #[tokio::test]
    async fn cluster_credential_falls_back_to_release_discovery() {
        let resolved = resolve_cluster_credential(None, false, DEFAULT_API_KEY, || async {
            Ok("generated-from-the-release".to_string())
        })
        .await
        .unwrap();

        assert_eq!(resolved, "generated-from-the-release");

        // An empty env value is absent, not "explicitly supplied", so it
        // discovers too (the #540 rule, held at this seam as well).
        let resolved = resolve_cluster_credential(
            Some(String::new()),
            false,
            DEFAULT_VALKEY_PASSWORD,
            || async { Ok("generated-valkey-password".to_string()) },
        )
        .await
        .unwrap();

        assert_eq!(resolved, "generated-valkey-password");
    }

    /// A discovery failure surfaces its actionable error rather than silently
    /// degrading to the dev default.
    #[tokio::test]
    async fn cluster_credential_propagates_a_discovery_failure() {
        let err = resolve_cluster_credential(None, false, DEFAULT_API_KEY, || async {
            Err(anyhow::anyhow!(
                "could not read the API key from secret curie-secrets in namespace curie"
            ))
        })
        .await
        .unwrap_err();

        assert!(err.to_string().contains("curie-secrets"), "{err}");
    }

    /// `--dry-run` stays offline: no cluster read, and the printed plan carries
    /// the dev default it always did.
    #[tokio::test]
    async fn cluster_credential_stays_offline_under_dry_run() {
        let resolved = resolve_cluster_credential(None, true, DEFAULT_VALKEY_PASSWORD, || async {
            panic!("--dry-run must not read the release secret")
        })
        .await
        .unwrap();

        assert_eq!(resolved, DEFAULT_VALKEY_PASSWORD);
    }

    #[test]
    fn port_forward_command_renders_svc_and_ports() {
        let cmd = port_forward_command("curie", "curie", "valkey", 56381, 6379);
        assert_eq!(
            cmd.display(),
            "kubectl -n curie port-forward svc/curie-valkey 56381:6379"
        );
    }

    #[test]
    fn select_channel_prefers_explicit() {
        let agents = [agent("a", "C1"), agent("b", "C2")];
        assert_eq!(select_channel(&agents, Some("CX")).unwrap(), "CX");
    }

    #[test]
    fn select_channel_uses_the_sole_agent() {
        let agents = [agent("only", "C-ONLY")];
        assert_eq!(select_channel(&agents, None).unwrap(), "C-ONLY");
    }

    #[test]
    fn select_channel_errors_on_zero_agents_naming_the_flag() {
        let err = select_channel(&[], None).unwrap_err().to_string();
        assert!(err.contains("--channel"), "{err}");
        assert!(err.contains("no agents"), "{err}");
    }

    #[test]
    fn select_channel_errors_on_many_agents_listing_them() {
        let agents = [agent("alpha", "C1"), agent("beta", "C2")];
        let err = select_channel(&agents, None).unwrap_err().to_string();
        assert!(err.contains("--channel"), "{err}");
        assert!(err.contains("alpha -> C1"), "{err}");
        assert!(err.contains("beta -> C2"), "{err}");
    }

    #[test]
    fn select_channel_uses_the_sole_bound_channel_across_agents() {
        // Selection counts (agent, channel) PAIRS, not agents (D4). One pair
        // across the whole platform is unambiguous however many agents are
        // deployed, so an agent bound to nothing must not make the one real
        // binding ambiguous.
        let agents = [
            agent_bound_to("only", &["C0EXAMPLE1"]),
            agent_bound_to("idle", &[]),
        ];
        assert_eq!(select_channel(&agents, None).unwrap(), "C0EXAMPLE1");
    }

    #[test]
    fn select_channel_errors_when_one_agent_has_two_channels_listing_both_pairs() {
        // The heart of D4. Today's `[only]` arm returns `only.channel.address`
        // and structurally CANNOT see a second binding, so a single deployed
        // agent bound to two channels silently routes to whichever one the
        // scalar column happened to hold. Two pairs is ambiguous, and the
        // error must name BOTH so the operator can pick.
        let agents = [agent_bound_to("solo", &["C0EXAMPLE1", "C0EXAMPLE2"])];
        let err = select_channel(&agents, None).unwrap_err().to_string();
        assert!(err.contains("--channel"), "{err}");
        assert!(err.contains("solo -> C0EXAMPLE1"), "{err}");
        assert!(err.contains("solo -> C0EXAMPLE2"), "{err}");
    }

    #[test]
    fn select_agent_id_matches_any_of_an_agents_channels() {
        // `select_agent_id` resolves by channel too, and today's
        // `.find(|a| a.channel.address == channel)` sees only the first
        // binding: an explicit --channel naming the SECOND one would report
        // "no deployed agent has channel ..." for an agent that plainly does.
        let agents = [
            agent_bound_to("one", &["C0EXAMPLE1", "C0EXAMPLE2"]),
            agent_bound_to("two", &["C0EXAMPLE3"]),
        ];
        assert_eq!(
            select_agent_id(&agents, Some("C0EXAMPLE2")).unwrap(),
            "id-one"
        );
        assert_eq!(
            select_agent_id(&agents, Some("C0EXAMPLE3")).unwrap(),
            "id-two"
        );
        // An address bound to nobody still errors, naming it.
        assert!(select_agent_id(&agents, Some("C0EXAMPLE9"))
            .unwrap_err()
            .to_string()
            .contains("C0EXAMPLE9"));
        // Agent selection still counts AGENTS, not pairs: a sole agent with
        // two bindings is one agent, so it resolves with no flag. This is the
        // deliberate asymmetry with `select_channel` above -- do not "fix" it.
        assert_eq!(select_agent_id(&agents[..1], None).unwrap(), "id-one");
    }

    #[test]
    fn server_host_and_port_parses_scheme_host_port() {
        assert_eq!(
            server_host_and_port("https://10.1.2.3:6443"),
            Some(("10.1.2.3".into(), 6443))
        );
        assert_eq!(
            server_host_and_port("https://k3s.local:6443/"),
            Some(("k3s.local".into(), 6443))
        );
        // No explicit port defaults from the scheme.
        assert_eq!(
            server_host_and_port("https://host"),
            Some(("host".into(), 443))
        );
        assert_eq!(
            server_host_and_port("http://host"),
            Some(("host".into(), 80))
        );
        assert_eq!(server_host_and_port(""), None);
    }

    #[test]
    fn server_host_and_port_parses_bracketed_ipv6() {
        assert_eq!(
            server_host_and_port("https://[::1]:6443"),
            Some(("::1".to_string(), 6443))
        );
        assert_eq!(
            server_host_and_port("https://[2001:db8::1]:8443"),
            Some(("2001:db8::1".to_string(), 8443))
        );
        // No explicit port defaults from the scheme; brackets are stripped.
        assert_eq!(
            server_host_and_port("https://[::1]"),
            Some(("::1".to_string(), 443))
        );
    }

    #[test]
    fn local_valkey_url_targets_the_compose_valkey_with_the_password() {
        assert_eq!(
            local_valkey_url("valkeypass"),
            "redis://:valkeypass@localhost:26379"
        );
        // A custom password flows through unchanged.
        assert_eq!(
            local_valkey_url("s3cr3t"),
            "redis://:s3cr3t@localhost:26379"
        );
    }

    #[test]
    fn local_api_base_prefers_explicit_then_falls_back_to_compose_default() {
        assert_eq!(local_api_base(Some("http://host:9999")), "http://host:9999");
        assert_eq!(local_api_base(None), DEFAULT_LOCAL_API_URL);
        // The compose API default carries no /api (routers mount at root).
        assert!(!DEFAULT_LOCAL_API_URL.ends_with("/api"));
    }

    #[test]
    fn local_stub_port_matches_listen_port() {
        // The stub port is coupled to the compose worker's SLACK_API_BASE_URL
        // (http://localhost:8155/api/); pin it so a change to one flags the other.
        assert_eq!(DEFAULT_LOCAL_STUB_PORT, 8155);
        assert_eq!(DEFAULT_LOCAL_STUB_PORT, DEFAULT_LISTEN_PORT);
    }

    /// Native Linux Docker: `network_mode: host` shares the host loopback, so the
    /// worker reaches the stub on `localhost`, bound loopback-only. Preserves the
    /// pre-#680 behavior on the platform where it always worked.
    #[test]
    fn local_stub_binding_is_loopback_on_native_linux() {
        let binding = resolve_local_stub_binding(None, false);
        assert_eq!(binding.bind_host, "127.0.0.1");
        assert_eq!(binding.advertise_host, "localhost");
    }

    /// Issue #680: under Docker Desktop (macOS) the worker sits in the VM netns,
    /// so `localhost` is the VM's loopback, not the Mac host stub. The worker-facing
    /// reply endpoint must resolve to a VM-reachable host (`host.docker.internal`),
    /// NOT `localhost`, and the stub must bind `0.0.0.0` to accept that off-loopback
    /// connection.
    #[test]
    fn local_stub_binding_is_vm_reachable_on_docker_desktop() {
        let binding = resolve_local_stub_binding(None, true);
        assert_eq!(binding.bind_host, "0.0.0.0");
        assert_eq!(binding.advertise_host, "host.docker.internal");

        let endpoint = local_stub_reply_endpoint(&binding.advertise_host);
        assert_eq!(endpoint, "http://host.docker.internal:8155/api/");
        assert!(
            !endpoint.contains("localhost"),
            "the Docker-Desktop reply endpoint must not point at localhost: {endpoint}"
        );
    }

    /// `CURIE_LOCAL_STUB_HOST` overrides the advertised host on any topology
    /// this binary cannot infer (e.g. Docker Desktop on Linux), and an explicit
    /// override binds `0.0.0.0` since a non-loopback host is only reachable off the
    /// loopback -- on both the Linux and macOS target-OS branches.
    #[test]
    fn local_stub_binding_env_override_wins_on_every_os() {
        for is_macos in [false, true] {
            let binding = resolve_local_stub_binding(Some("host.docker.internal".into()), is_macos);
            assert_eq!(binding.bind_host, "0.0.0.0", "is_macos={is_macos}");
            assert_eq!(
                binding.advertise_host, "host.docker.internal",
                "is_macos={is_macos}"
            );
        }
    }

    /// An empty `CURIE_LOCAL_STUB_HOST` is absent, not an explicit choice (same
    /// empty-is-unset rule as the api-key parser): it falls back to the OS default.
    #[test]
    fn local_stub_binding_ignores_empty_env_override() {
        assert_eq!(
            resolve_local_stub_binding(Some(String::new()), false),
            resolve_local_stub_binding(None, false),
        );
        assert_eq!(
            resolve_local_stub_binding(Some(String::new()), true),
            resolve_local_stub_binding(None, true),
        );
    }

    #[test]
    fn connected_only_when_the_worker_itself_talks_to_real_slack() {
        let t = |s: &str| Some(s.to_string());

        // Connected: the worker's transport is real Slack and its token is real.
        assert_eq!(
            connected_worker_bot_token((t("https://slack.com/api/"), t("xoxb-real"))).as_deref(),
            Some("xoxb-real")
        );

        // #957 mode A, the failure this replaced: a REAL token is present but the
        // worker is wired to the stub, so posting would orphan a "..." in a real
        // channel that the worker can never edit. Not connected.
        assert_eq!(
            connected_worker_bot_token((
                t("http://localhost:8155/api/"),
                t("xoxb-real-from-the-vault")
            )),
            None
        );

        // The stub sentinel is not a workspace token even if the base URL is odd.
        assert_eq!(
            connected_worker_bot_token((t("https://slack.com/api/"), t(LOCAL_STUB_BOT_TOKEN))),
            None
        );
        // No stack running / nothing resolvable -> stub path, never a real post.
        assert_eq!(connected_worker_bot_token((None, None)), None);
        assert_eq!(connected_worker_bot_token((t(""), t("xoxb-real"))), None);
        assert_eq!(
            connected_worker_bot_token((t("https://slack.com/api/"), None)),
            None
        );
        assert_eq!(
            connected_worker_bot_token((t("https://slack.com/api/"), t("  "))),
            None
        );
    }

    #[test]
    fn both_dry_run_plans_note_the_connected_transport_branch() {
        // --dry-run never touches the network, so it cannot probe for a
        // dispatcher; it must state the conditional instead (#770/ADR-0078).
        let lines = dry_run_lines(&opts(Some("C123")), "10.1.2.3");
        assert!(
            lines.iter().any(|l| l.contains("no stub is bound")),
            "cluster plan must note the connected branch: {lines:?}"
        );
    }

    #[test]
    fn connected_turn_conversation_id_is_the_real_placeholder_ts() {
        // Issue #954: with no --thread the connected path posts a TOP-LEVEL
        // placeholder, and a top-level message's own ts is its thread root -- so
        // the enqueued conversation_id must be exactly that placeholder ts. The
        // pre-fix code fed a clock-derived synthetic thread here, which can never
        // equal the ts Slack returned, so the card had nothing real to thread on.
        // Asserting on the turn the enqueue actually builds is the point: the bug
        // was the wiring, so wiring a synthetic thread back in must fail HERE.
        let placeholder_ts = "1717171717.000900";
        let turn = connected_turn("C-real", &opts(Some("C-real")), None, placeholder_ts);
        assert_eq!(
            turn.reply_handle.placeholder.as_deref(),
            Some(turn.conversation_id.as_str()),
            "the connected turn must thread on the placeholder we actually posted"
        );
        assert_eq!(turn.conversation_id, placeholder_ts);
        assert_eq!(turn.reply_handle.channel, "C-real");
        // #770/ADR-0078: no per-turn endpoint, so the reply rides the connected
        // transport.
        assert!(turn.reply_handle.endpoint.is_none());
    }

    #[test]
    fn connected_turn_conversation_id_is_the_explicit_thread() {
        // With --thread <ts> the turn's conversation_id IS that thread, while the
        // placeholder stays the distinct real message we posted into it. (The
        // outbound post itself lives in slack::post_body, not here.)
        let thread = "1717171717.000100";
        let placeholder_ts = "1717171717.000900";
        let turn = connected_turn(
            "C-real",
            &opts(Some("C-real")),
            Some(thread),
            placeholder_ts,
        );
        assert_eq!(turn.conversation_id, thread);
        assert_eq!(
            turn.reply_handle.placeholder.as_deref(),
            Some(placeholder_ts)
        );
        assert!(turn.reply_handle.endpoint.is_none());
    }

    #[test]
    fn host_is_loopback_recognizes_loopback_names_and_addresses() {
        assert!(host_is_loopback("localhost"));
        assert!(host_is_loopback("LocalHost")); // case-insensitive
        assert!(host_is_loopback("127.0.0.1"));
        assert!(host_is_loopback("127.0.0.5")); // all of 127.0.0.0/8
        assert!(host_is_loopback("::1"));
        assert!(!host_is_loopback("10.0.0.5"));
        assert!(!host_is_loopback("192.168.65.254"));
        assert!(!host_is_loopback("my-eks.example.com"));
    }

    #[test]
    fn docker_internal_advertise_only_under_vm_platform_and_loopback_server() {
        // Docker Desktop (VM platform) + a loopback-exposed API server (local
        // kind) -> advertise host.docker.internal (#900).
        assert!(prefers_docker_internal_host("127.0.0.1", true));
        assert!(prefers_docker_internal_host("localhost", true));
        assert!(prefers_docker_internal_host("::1", true));
        // Native-Docker Linux never rewrites, even with a loopback API server
        // (it reaches the host via the bridge gateway / an explicit --listen-host).
        assert!(!prefers_docker_internal_host("127.0.0.1", false));
        // A remote / non-loopback API server keeps the local-IP detection path
        // even on a VM platform (not the local-kind case #900 addresses).
        assert!(!prefers_docker_internal_host("10.0.0.5", true));
        assert!(!prefers_docker_internal_host("my-eks.example.com", true));
    }

    const ADVERTISE_HOST_CHILD_CASE: &str = "CURIE_TEST_ADVERTISE_HOST_CASE";

    fn run_advertise_host_child(case: &str, path: &std::path::Path) {
        let output =
            std::process::Command::new(std::env::current_exe().expect("resolve test executable"))
                .arg("message::tests::advertise_host_child")
                .arg("--exact")
                .arg("--nocapture")
                .env(ADVERTISE_HOST_CHILD_CASE, case)
                .env("PATH", path)
                .output()
                .expect("run advertise host child");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "advertise host child failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        let sentinel = format!("ADVERTISE_HOST_OK {case}");
        assert!(
            stdout
                .lines()
                .any(|line| line.trim() == sentinel.as_str()),
            "advertise host child did not prove case {case} ran\nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
    }

    #[tokio::test]
    async fn advertise_host_child() {
        let Ok(case) = std::env::var(ADVERTISE_HOST_CHILD_CASE) else {
            return;
        };
        match case.as_str() {
            "kernel_route" => {
                let target = "192.0.2.1";
                let socket =
                    std::net::UdpSocket::bind("0.0.0.0:0").expect("bind route source probe");
                socket
                    .connect((target, 6443))
                    .expect("select route toward documentation address");
                let expected = socket.local_addr().expect("read route source").ip();
                let actual = resolve_advertise_host(None)
                    .await
                    .expect("derive advertise host");
                assert_eq!(actual, expected.to_string());
                assert_ne!(actual, target);
            }
            "explicit" => {
                let actual = resolve_advertise_host(Some("192.0.2.44"))
                    .await
                    .expect("accept explicit listen host");
                assert_eq!(actual, "192.0.2.44");
            }
            other => panic!("unknown advertise host child case {other}"),
        }
        println!("ADVERTISE_HOST_OK {case}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_advertise_host_uses_kernel_route_source() {
        use std::os::unix::fs::PermissionsExt;

        let tools = tempfile::tempdir().expect("create kubectl stub directory");
        let kubectl = tools.path().join("kubectl");
        std::fs::write(
            &kubectl,
            "#!/bin/sh\nprintf '%s\\n' 'https://192.0.2.1:6443'\n",
        )
        .expect("write kubectl stub");
        let mut permissions = std::fs::metadata(&kubectl)
            .expect("read kubectl stub metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&kubectl, permissions).expect("make kubectl stub executable");

        run_advertise_host_child("kernel_route", tools.path());
    }

    #[test]
    fn explicit_listen_host_bypasses_auto_detection() {
        let no_tools = tempfile::tempdir().expect("create empty executable directory");
        run_advertise_host_child("explicit", no_tools.path());
    }

    #[test]
    fn dry_run_lists_the_valkey_forward_and_the_enqueue_with_the_reply_endpoint() {
        let lines = dry_run_lines(&opts(Some("C123")), "10.1.2.3");
        // Explicit channel -> only the Valkey forward, no API forward.
        assert!(
            lines
                .iter()
                .any(|l| l == "kubectl -n curie port-forward svc/curie-valkey 56381:6379"),
            "{lines:?}"
        );
        assert!(
            !lines.iter().any(|l| l.contains("svc/curie-api")),
            "explicit channel needs no api forward: {lines:?}"
        );
        // Issue #19: the reply routes per turn, so there is no worker-global
        // helm upgrade / rollout / dispatcher guard in the plan.
        assert!(
            !lines.iter().any(|l| l.contains("helm upgrade")),
            "no worker-global wiring: {lines:?}"
        );
        assert!(
            !lines.iter().any(|l| l.contains("rollout status")),
            "no rollout wait: {lines:?}"
        );
        assert!(
            !lines.iter().any(|l| l.contains("dispatcher")),
            "no dispatcher guard: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l == "stub advertised at http://10.1.2.3:8155/api/"),
            "{lines:?}"
        );
        // The enqueue line names the channel and the per-turn reply endpoint.
        assert!(
            lines.iter().any(|l| l.contains("enqueue")
                && l.contains("C123")
                && l.contains("reply endpoint http://10.1.2.3:8155/api/")),
            "{lines:?}"
        );
    }

    #[test]
    fn dry_run_adds_api_forward_when_no_channel() {
        let lines = dry_run_lines(&opts(None), "host");
        assert!(
            lines
                .iter()
                .any(|l| l == "kubectl -n curie port-forward svc/curie-api 8123:8000"),
            "no --channel -> api forward: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("the sole bound (agent, Slack channel) pair")),
            "channel placeholder when omitted: {lines:?}"
        );
    }

    #[test]
    fn message_timeout_json_marks_the_terminal_state() {
        let v = message_timeout_json();
        assert!(v["reply"].is_null(), "{v}");
        assert_eq!(v["finalized"], serde_json::json!(false));
        assert_eq!(v["timed_out"], serde_json::json!(true));
        // The reply builder's key set is a different shape (no timed_out), so a
        // consumer can discriminate the two terminal states.
        assert!(v.get("thread").is_none(), "timeout carries no thread: {v}");
    }

    #[test]
    fn message_dry_run_json_carries_the_planned_action() {
        // Explicit channel passes through verbatim.
        let v = message_dry_run_json(
            "local",
            "curie:turns",
            Some("C123"),
            "http://localhost:8155/api/",
        );
        assert_eq!(v["dry_run"], serde_json::json!(true));
        assert_eq!(v["target"], serde_json::json!("local"));
        assert_eq!(v["stream"], serde_json::json!("curie:turns"));
        assert_eq!(v["channel"], serde_json::json!("C123"));
        assert_eq!(
            v["reply_endpoint"],
            serde_json::json!("http://localhost:8155/api/")
        );
        // Omitted channel is JSON null, not a placeholder string.
        let v = message_dry_run_json("cluster", "s", None, "http://10.1.2.3:8155/api/");
        assert!(v["channel"].is_null(), "{v}");
        assert_eq!(v["target"], serde_json::json!("cluster"));
    }

    // --- eval parity verb ---------------------------------------------------

    use crate::evals::{EvalCase, ExpectedStatus, Grader, GraderKind};

    fn eval_opts(local: bool, channel: Option<&str>) -> EvalOpts {
        EvalOpts {
            cases: None,
            channel: channel.map(str::to_string),
            namespace: "curie".into(),
            release: "curie".into(),
            listen_host: None,
            listen_port: DEFAULT_LISTEN_PORT,
            valkey_local_port: DEFAULT_VALKEY_LOCAL_PORT,
            valkey_password: DEFAULT_VALKEY_PASSWORD.into(),
            api_local_port: DEFAULT_API_LOCAL_PORT,
            api_key: DEFAULT_API_KEY.into(),
            user: DEFAULT_USER.into(),
            stream: DEFAULT_STREAM.into(),
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            dry_run: true,
            local,
            api_url: None,
            models: Vec::new(),
            concurrency: 1,
        }
    }

    fn eval_case(kind: GraderKind, expected: &str) -> EvalCase {
        eval_case_with_status(kind, expected, ExpectedStatus::Done)
    }

    fn eval_case_with_status(
        kind: GraderKind,
        expected: &str,
        expect_status: ExpectedStatus,
    ) -> EvalCase {
        EvalCase {
            id: "c1".into(),
            input: "ping".into(),
            grader: Grader {
                kind,
                expected: expected.into(),
                case_sensitive: false,
            },
            shared_history: false,
            expect_status,
        }
    }

    #[test]
    fn reply_passes_only_on_a_matching_captured_reply() {
        let case = eval_case(GraderKind::Contains, "pong");
        // Replied + grader matches -> pass; the shared Grader grades the reply.
        assert!(reply_passes(
            &case,
            &Outcome::Replied("the answer is PONG".into())
        ));
        // Replied but grader misses -> fail.
        assert!(!reply_passes(&case, &Outcome::Replied("nope".into())));
        // No reply text and no completion never pass, mirroring turn_passes.
        assert!(!reply_passes(&case, &Outcome::CompletedNoEdit));
        assert!(!reply_passes(&case, &Outcome::TimedOut));
        // A turn parked awaiting approval produced no graded reply -> never passes,
        // even if the card text happens to contain the expected token (#529).
        assert!(!reply_passes(
            &case,
            &Outcome::AwaitingApproval(Some("pong pending approval".into()))
        ));
    }

    #[test]
    fn awaiting_approval_case_passes_only_when_the_gate_holds() {
        // The message-path mirror of the run-7 anti-correlation (#262): a case that
        // asserts `awaiting-approval` with a match-anything grader is GREEN when the
        // turn parked awaiting approval (the gate held) and RED when it merely
        // replied (the agent narrated and the turn completed).
        let case =
            eval_case_with_status(GraderKind::Contains, "", ExpectedStatus::AwaitingApproval);
        assert!(reply_passes(
            &case,
            &Outcome::AwaitingApproval(Some("blocked the close".into()))
        ));
        assert!(reply_passes(&case, &Outcome::AwaitingApproval(None)));
        // The agent merely replied -> the gate did not hold -> RED.
        assert!(!reply_passes(
            &case,
            &Outcome::Replied("I asked for approval".into())
        ));
        assert!(!reply_passes(&case, &Outcome::CompletedNoEdit));
        assert!(!reply_passes(&case, &Outcome::TimedOut));
    }

    #[test]
    fn local_eval_dry_run_plan_names_the_tier_suite_and_enqueue() {
        // The `local eval` path with no live stack: the plan is a pure render.
        let lines = eval_dry_run_lines(&eval_opts(true, Some("C123")), "smoke", 3);
        assert!(
            lines
                .iter()
                .any(|l| l == "grade 3 case(s) from suite \"smoke\" against the local tier"),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l == "enqueue onto redis redis://:valkeypass@localhost:26379"),
            "{lines:?}"
        );
        assert!(lines.iter().any(|l| l == "channel C123"), "{lines:?}");
        // No cluster plumbing (a kubectl port-forward command) leaks into the
        // local plan; the "no kubectl/helm" descriptor line is fine.
        assert!(
            !lines.iter().any(|l| l.starts_with("kubectl ")),
            "local plan has no kubectl command: {lines:?}"
        );
        assert!(
            lines.iter().any(
                |l| l.contains("enqueue one synthetic QueuedTurn per case on stream")
                    && l.contains(DEFAULT_STREAM)
            ),
            "{lines:?}"
        );
    }

    #[test]
    fn local_eval_dry_run_names_the_channel_lookup_when_omitted() {
        let lines = eval_dry_run_lines(&eval_opts(true, None), "smoke", 1);
        assert!(
            lines
                .iter()
                .any(|l| l.contains("the sole bound (agent, Slack channel) pair")),
            "channel placeholder when omitted: {lines:?}"
        );
    }

    #[test]
    fn cluster_eval_dry_run_plan_lists_the_valkey_forward_and_stub() {
        let lines = eval_dry_run_lines(&eval_opts(false, Some("C1")), "smoke", 2);
        assert!(
            lines
                .iter()
                .any(|l| l == "grade 2 case(s) from suite \"smoke\" against the cluster tier"),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l == "kubectl -n curie port-forward svc/curie-valkey 56381:6379"),
            "{lines:?}"
        );
        // Explicit channel -> no api forward.
        assert!(
            !lines.iter().any(|l| l.contains("svc/curie-api")),
            "explicit channel needs no api forward: {lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l.starts_with("stub advertised at http://")),
            "{lines:?}"
        );
    }

    fn sweep_opts(local: bool, channel: Option<&str>, models: &[&str]) -> EvalOpts {
        let mut opts = eval_opts(local, channel);
        opts.models = models.iter().map(|m| m.to_string()).collect();
        opts
    }

    #[test]
    fn model_sweep_dry_run_plans_a_trigger_per_model_and_a_matrix_poll() {
        // A `--model` sweep prints the platform-eval-plane plan (one trigger per
        // model + a matrix poll), NOT the message enqueue path (#526).
        let lines = eval_dry_run_lines(
            &sweep_opts(true, Some("C7"), &["opus", "sonnet"]),
            "smoke",
            2,
        );
        assert!(
            lines
                .iter()
                .any(|l| l.contains("sweep 2 model(s) over suite \"smoke\"")),
            "{lines:?}"
        );
        // One trigger line per model, naming the model and the explicit channel.
        for model in ["opus", "sonnet"] {
            assert!(
                lines.iter().any(|l| l.contains("/evals/trigger")
                    && l.contains(&format!("{model:?}"))
                    && l.contains("channel C7")),
                "a trigger line for {model}: {lines:?}"
            );
        }
        assert!(
            lines
                .iter()
                .any(|l| l.contains("/evals/matrix?suite=smoke")),
            "a matrix poll line: {lines:?}"
        );
        // The sweep plan does NOT walk the synthetic-turn enqueue path.
        assert!(
            !lines.iter().any(|l| l.contains("synthetic QueuedTurn")),
            "sweep is the eval plane, not the message path: {lines:?}"
        );
    }

    #[test]
    fn cluster_model_sweep_dry_run_reaches_the_api_via_port_forward() {
        let lines = eval_dry_run_lines(&sweep_opts(false, None, &["opus"]), "smoke", 1);
        assert!(
            lines
                .iter()
                .any(|l| l.contains("api port-forward") && l.contains("/evals/trigger")),
            "cluster sweep triggers through the api port-forward: {lines:?}"
        );
    }

    #[test]
    fn select_agent_id_resolves_by_channel_then_falls_back_to_sole_agent() {
        let agents = vec![
            Agent {
                id: "a1".into(),
                name: "one".into(),
                channels: vec![crate::api::ChannelBinding {
                    kind: "slack".into(),
                    address: "C1".into(),
                }],
                repo_full_name: None,
                approval_required_tools: None,
                approval_routes: None,
                model: None,
                thinking: None,
            },
            Agent {
                id: "a2".into(),
                name: "two".into(),
                channels: vec![crate::api::ChannelBinding {
                    kind: "slack".into(),
                    address: "C2".into(),
                }],
                repo_full_name: None,
                approval_required_tools: None,
                approval_routes: None,
                model: None,
                thinking: None,
            },
        ];
        // Explicit channel picks the matching agent's id.
        assert_eq!(select_agent_id(&agents, Some("C2")).unwrap(), "a2");
        // An unknown channel errors, naming the channel.
        assert!(select_agent_id(&agents, Some("C9"))
            .unwrap_err()
            .to_string()
            .contains("C9"));
        // Many agents + no channel is ambiguous.
        assert!(select_agent_id(&agents, None).is_err());
        // A sole agent + no channel resolves without a flag.
        assert_eq!(select_agent_id(&agents[..1], None).unwrap(), "a1");
    }

    fn model_summary(
        version: &str,
        model: &str,
        passed: u64,
        total: u64,
    ) -> crate::api::EvalModelVersionSummary {
        // `completed` defaults to `total`: every one of these fixture rows models
        // a normal graded run (every case reached a verdict), so it is not the
        // #622 "never answered" outcome unless a test opts into that separately
        // via `model_summary_never_completed`.
        plumbing_model_summary(version, model, passed, total, 0)
    }

    fn plumbing_model_summary(
        version: &str,
        model: &str,
        passed: u64,
        total: u64,
        plumbing: u64,
    ) -> crate::api::EvalModelVersionSummary {
        crate::api::EvalModelVersionSummary {
            version: version.to_string(),
            model: Some(model.to_string()),
            passed,
            total,
            completed: total,
            plumbing,
        }
    }

    fn model_summary_never_completed(
        version: &str,
        model: &str,
        total: u64,
    ) -> crate::api::EvalModelVersionSummary {
        crate::api::EvalModelVersionSummary {
            version: version.to_string(),
            model: Some(model.to_string()),
            passed: 0,
            total,
            completed: 0,
            plumbing: 0,
        }
    }

    fn matrix(
        versions: &[&str],
        summaries: Vec<crate::api::EvalModelVersionSummary>,
    ) -> crate::api::EvalMatrix {
        crate::api::EvalMatrix {
            suite: "smoke".into(),
            versions: versions.iter().map(|v| v.to_string()).collect(),
            model_version_summaries: summaries,
            rows: Vec::new(),
        }
    }

    #[test]
    fn sweep_not_ready_when_only_a_prior_runs_rows_are_present() {
        // The #608 regression guard: a repeat sweep after a change triggers a NEW
        // sha, but the matrix still holds the PRIOR run's FULL rows (same models,
        // total > 0) within its version window. Readiness must NOT be satisfied by
        // the prior run -- the pre-#608 gate (model membership + total > 0, with no
        // version scope) WOULD have reported those stale rows on the first poll.
        let want: std::collections::BTreeSet<&str> = ["opus", "sonnet"].into_iter().collect();
        let m = matrix(
            &["old-sha"],
            vec![
                model_summary("old-sha", "opus", 3, 3),
                model_summary("old-sha", "sonnet", 2, 3),
            ],
        );
        assert!(
            sweep_ready_rows(&m, "new-sha", &want).is_none(),
            "a prior run's rows must not satisfy readiness for a different triggered sha"
        );
    }

    #[test]
    fn sweep_ready_once_the_triggered_sha_has_landed_for_every_model() {
        let want: std::collections::BTreeSet<&str> = ["opus", "sonnet"].into_iter().collect();
        let m = matrix(
            &["new-sha", "old-sha"],
            vec![
                model_summary("new-sha", "opus", 3, 3),
                model_summary("new-sha", "sonnet", 3, 3),
            ],
        );
        let rows = sweep_ready_rows(&m, "new-sha", &want).expect("all models landed for the run");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| row.model == "opus"));
        assert!(rows.iter().any(|row| row.model == "sonnet"));
    }

    #[test]
    fn sweep_not_ready_until_every_wanted_model_has_a_row() {
        // The triggered sha has landed, but only one of the two swept models has a
        // row yet -- keep polling rather than report a half-finished sweep.
        let want: std::collections::BTreeSet<&str> = ["opus", "sonnet"].into_iter().collect();
        let m = matrix(&["new-sha"], vec![model_summary("new-sha", "opus", 3, 3)]);
        assert!(sweep_ready_rows(&m, "new-sha", &want).is_none());
    }

    #[test]
    fn a_local_cluster_row_for_a_model_that_never_completed_a_turn_is_ready_and_distinct() {
        // #622 at the local/cluster tier: the platform matrix's `EvalModelSummary`
        // for a model that never produced a completed turn reports `total > 0,
        // completed == 0` (every case landed as a graded FAIL with `error` set --
        // see `apps/api/src/curie_api/evals.py::_completed`). The row still
        // counts toward readiness (the sweep DID land, unlike the timeout path
        // below), but `SweepRow::never_completed` reads it as the distinct
        // outcome rather than a real 0%.
        let want: std::collections::BTreeSet<&str> = ["bogus-model-xyz"].into_iter().collect();
        let m = matrix(
            &["new-sha"],
            vec![model_summary_never_completed(
                "new-sha",
                "bogus-model-xyz",
                5,
            )],
        );
        let rows = sweep_ready_rows(&m, "new-sha", &want)
            .expect("a landed-but-all-failed row still satisfies readiness");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].never_completed());
        assert_eq!(rows[0].passed, 0);
        assert_eq!(rows[0].total, 5);
        // Feeding this row straight into the shared reporter fails the sweep
        // loudly, exactly as the skill-tier row does -- same signal, same gate,
        // regardless of which tier produced it.
        let err = crate::commands::report_sweep(&rows, None).unwrap_err();
        assert!(err.to_string().contains("bogus-model-xyz"));
    }

    #[test]
    fn sweep_ready_when_a_wanted_model_is_plumbing_only() {
        // #700: a model whose every row is a plumbing fixture (#612/#606, e.g. the
        // fake-model tier) reports total == 0 forever -- it will never satisfy a
        // `total > 0` readiness check. Before this fix that model's row (and the
        // sweep it belongs to) would hang until timeout even though it landed;
        // `plumbing > 0` alone must be enough to count as landed.
        let want: std::collections::BTreeSet<&str> = ["opus", "fake"].into_iter().collect();
        let m = matrix(
            &["new-sha"],
            vec![
                model_summary("new-sha", "opus", 3, 3),
                plumbing_model_summary("new-sha", "fake", 0, 0, 3),
            ],
        );
        let rows = sweep_ready_rows(&m, "new-sha", &want)
            .expect("a plumbing-only row must still count as landed");
        assert_eq!(rows.len(), 2);
        let fake_row = rows
            .iter()
            .find(|row| row.model == "fake")
            .expect("the plumbing-only model's row must be reported, not dropped");
        assert_eq!(fake_row.total, 0);
        assert_eq!(fake_row.plumbing, 3);
        assert!(fake_row.is_plumbing_only());
        let opus_row = rows.iter().find(|row| row.model == "opus").unwrap();
        assert!(!opus_row.is_plumbing_only());
    }

    #[test]
    fn scoped_rows_drops_a_model_with_no_graded_and_no_plumbing_rows() {
        // A model with a summary row but total == 0 and plumbing == 0 has not
        // landed at all yet (distinct from a genuine plumbing-only row); it must
        // still be dropped so readiness keeps polling for it.
        let want: std::collections::BTreeSet<&str> = ["opus"].into_iter().collect();
        let m = matrix(&["new-sha"], vec![model_summary("new-sha", "opus", 0, 0)]);
        assert!(scoped_rows(&m, &want, "new-sha").is_empty());
    }

    #[test]
    fn never_completed_is_scoped_to_the_triggered_sha_not_the_window() {
        // #814: a model that COMPLETED cases on an older in-window sha but never
        // completes a turn on the TRIGGERED sha must be reported never_completed
        // and fail the sweep. The platform matrix now exposes the per-(version,
        // model) dimension (model_version_summaries), so `scoped_rows` reads the
        // triggered sha's own row rather than the window-blended `completed`. With
        // the old blended count, opus's completions on `old-sha` (completed == 5)
        // would keep `completed > 0`, masking the zero-completed run on `new-sha`
        // and reporting a fabricated blended pass-rate.
        let want: std::collections::BTreeSet<&str> = ["opus"].into_iter().collect();
        let m = matrix(
            &["new-sha", "old-sha"],
            vec![
                // The triggered sha: every case landed as a graded FAIL whose turn
                // never completed (error set) -- completed == 0.
                model_summary_never_completed("new-sha", "opus", 5),
                // A prior in-window sha: opus completed and passed every case.
                model_summary("old-sha", "opus", 5, 5),
            ],
        );
        let rows = sweep_ready_rows(&m, "new-sha", &want)
            .expect("the triggered sha's row has landed for the wanted model");
        assert_eq!(rows.len(), 1);
        let opus = &rows[0];
        assert_eq!(opus.model, "opus");
        assert_eq!(opus.total, 5);
        assert_eq!(
            opus.completed, 0,
            "completed must be scoped to the triggered sha (new-sha), not blended with old-sha"
        );
        assert!(
            opus.never_completed(),
            "scoped to new-sha opus never completed a turn, so the sweep must fail loudly"
        );
        // The shared reporter fails the sweep and names the offending model.
        let err = crate::commands::report_sweep(&rows, None).unwrap_err();
        assert!(err.to_string().contains("opus"));
    }

    #[tokio::test]
    async fn model_sweep_refuses_an_explicit_cases_override() {
        // AC3 (#608): a `--cases` override cannot reach the worker on a platform
        // sweep, so it is refused with a reason and exit 4 (Unsupported), never
        // silently evaluating the deployed cases while displaying the local ones.
        // The refusal returns before any network or suite resolution, so this
        // exercises the guard directly.
        let mut opts = sweep_opts(true, None, &["opus"]);
        opts.cases = Some(PathBuf::from("/tmp/does-not-need-to-exist.json"));
        let err = eval(opts)
            .await
            .expect_err("--cases + --model must be refused");
        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(
            class,
            crate::exit::ExitClass::Unsupported,
            "the refusal is ADR-0041 Unsupported (exit 4): {err:#}"
        );
        let fix = fix.expect("the refusal names the honest alternative path");
        assert!(
            fix.contains("--cases") && fix.contains("--model"),
            "the fix points at both the drop-flag and the in-CLI path: {fix}"
        );
    }
}
