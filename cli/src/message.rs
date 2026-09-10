//! Shared target noun message forms: drive the DEPLOYED Kubernetes release end
//! to end from the CLI with zero Slack contact.
//!
//! Product rationale: a developer building an agent for someone else's Slack
//! workspace can exercise the entire deployed machinery (Valkey queue -> worker
//! -> claimed sandbox -> the real skill -> the reply) without any Slack access,
//! tokens, or workspace. It is the retained chat helper engine, backed by a
//! ref-keyed in-cluster API relay plus the frozen `QueuedTurn` enqueue, with
//! Kubernetes-aware auto-plumbing bolted on top:
//!
//! 1. Separate process-owned `kubectl port-forward`s reach Valkey for enqueue
//!    and the API for reply polling, with no worker Deployment mutation.
//! 2. A disconnected turn keeps its Slack binding kind/channel, carries no
//!    endpoint, and selects the reserved relay adapter with a UUIDv4 ref.
//! 3. Connected Slack bypasses the relay unchanged, so both paths share one
//!    stable worker replica set without callback-trust rollouts.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use curie_aci_protocol::QueuedTurn;
use redis::aio::MultiplexedConnection;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::api::{Agent, ApiClient};
use crate::chat::{
    await_reply, await_resume, capped, continue_hint_line, continue_hint_long_line,
    parse_approval_id, resolve_targets, Outcome, SlackStub,
};
use crate::evals::{EvalCase, EvalSuite, ExpectedStatus, LoadedEval};
use crate::ops::{plain, require_on_path, run_capture, OpsCommand};
use crate::queue::{
    self, connect, diagnostics, eval_case_turn, queue_thread_reset, synthetic_turn,
    thread_key_for_turn, xadd,
};
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

/// Flags on `local message` / `cluster message` that consume a following value.
/// Keep in sync with the clap declarations in `main.rs`; a new value-taking
/// flag added there must be added here or a `--flag <value>` pair is counted
/// as a positional and the two-positional trap fires on a valid invocation.
const MESSAGE_VALUE_FLAGS: &[&str] = &[
    "--channel",
    "--thread",
    "--namespace",
    "--release",
    "--chart",
    "--listen-host",
    "--listen-port",
    "--valkey-local-port",
    "--valkey-password",
    "--api-local-port",
    "--api-key",
    "--user",
    "--stream",
    "--timeout-secs",
    "--api-url",
    "--color",
];

/// Boolean flags that may appear on the message verbs, including globals.
const MESSAGE_BOOL_FLAGS: &[&str] = &[
    "--continue",
    "--dry-run",
    "--json",
    "--debug",
    "--quiet",
    "-q",
    "--help",
    "-h",
    "--version",
    "-V",
];

/// Detect the two-positional `local|cluster message <agent> <text>` shape
/// copied from sibling verbs that take `<AGENT>` first (#2498).
///
/// Returns a usage error naming the existing `--channel` routing model. Does
/// not invent an agent-name routing API.
pub fn reject_agent_named_message(args: &[String]) -> Option<anyhow::Error> {
    let rest = skip_global_flags(args);
    let tier = rest.first().map(String::as_str)?;
    if tier != "local" && tier != "cluster" {
        return None;
    }
    // Globals (`--json`, `--debug`, ...) may sit between the target and the
    // verb because they are `global = true` on `Cli`.
    let after_tier = skip_global_flags(&rest[1..]);
    if after_tier.first().map(String::as_str) != Some("message") {
        return None;
    }
    if message_positional_count(&after_tier[1..]) < 2 {
        return None;
    }
    let verb = format!("{tier} message");
    let siblings = format!("`{tier} versions`, `{tier} kill`, and `{tier} delete`");
    Some(anyhow::Error::from(
        crate::exit::CliError::usage(format!(
            "`curie {verb}` does not take an agent name. The two-positional form \
             <AGENT> <TEXT> is the shape of {siblings}, not this verb."
        ))
        .with_fix(format!(
            "Pass only the message text. Route with `--channel <CHANNEL>`, or omit \
             `--channel` when exactly one channel is bound. Example: `curie {verb} \
             'Who are you?'`"
        )),
    ))
}

/// Skip the `global = true` flags on `Cli` (debug, quiet, color, json) so the
/// subcommand token is the first remaining argument. Keep in sync with
/// `retired::retired_hint` plus `--json`.
fn skip_global_flags(args: &[String]) -> &[String] {
    let mut index = 0usize;
    while index < args.len() {
        match args[index].as_str() {
            "--debug" | "-q" | "--quiet" | "--json" => {
                index += 1;
            }
            "--color" => {
                index += 1;
                if index < args.len() && !args[index].starts_with('-') {
                    index += 1;
                }
            }
            token if token.starts_with("--color=") => {
                index += 1;
            }
            _ => break,
        }
    }
    &args[index..]
}

fn message_positional_count(args: &[String]) -> usize {
    let mut count = 0usize;
    let mut index = 0usize;
    while index < args.len() {
        let token = args[index].as_str();
        if token == "--" {
            return count + args.len().saturating_sub(index + 1);
        }
        if let Some(rest) = token.strip_prefix("--") {
            if rest.contains('=') {
                index += 1;
                continue;
            }
            if MESSAGE_VALUE_FLAGS.contains(&token) {
                index += 1;
                if index < args.len() && !args[index].starts_with('-') {
                    index += 1;
                }
                continue;
            }
            if MESSAGE_BOOL_FLAGS.contains(&token) {
                index += 1;
                continue;
            }
            // Unknown long option: leave it as a flag so clap still owns
            // unknown-flag errors when only one text token remains.
            index += 1;
            continue;
        }
        if matches!(token, "-q" | "-h" | "-V") {
            index += 1;
            continue;
        }
        count += 1;
        index += 1;
    }
    count
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

/// `kubectl -n <ns> port-forward svc/<fullname>-<suffix> <local>:<remote>`.
///
/// Takes a RESOLVED `ReleaseFullname` (#1533). Every component this tunnels to
/// (`api`, `valkey`) is rendered by the chart as
/// `{{ include "curie.fullname" . }}-<suffix>`, which is NOT `{release}-<suffix>`
/// unless the release name already contains the chart name. A raw release name
/// cannot reach this builder, which is the point of the newtype.
pub fn port_forward_command(
    namespace: &str,
    fullname: &crate::ops::ReleaseFullname,
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
            plain(format!("svc/{}", fullname.resource(suffix))),
            plain(format!("{local_port}:{remote_port}")),
        ],
    )
}

/// The `/api/` base URL the worker posts its placeholder edits to.
fn advertised_url(host: &str, port: u16) -> String {
    format!("http://{host}:{port}/api/")
}

/// Reserved built-in worker adapter for disconnected `cluster message` turns.
/// It is intentionally language-local and byte-identical to the worker/API
/// literal: the frozen queue contract already has an adapter slot, so no wire
/// change is needed.
const CLUSTER_MESSAGE_RELAY_ADAPTER: &str = "curie-cluster-message";

/// The kubectl read behind `dispatcher_connected_strict`, extracted pure so the
/// Deployment NAME is unit-testable without a cluster (#1533).
///
/// `--ignore-not-found` is what makes a wrong name dangerous here: a Deployment
/// that does not exist returns success with EMPTY output, which the caller
/// reads as "Slack is disconnected". That is a silent wrong-direction routing
/// decision, so the name must be the chart-rendered one.
fn dispatcher_probe_command(namespace: &str, fullname: &crate::ops::ReleaseFullname) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain("deployment"),
            plain(fullname.resource("dispatcher")),
            plain("--ignore-not-found"),
            plain("-o"),
            plain("name"),
        ],
    )
}

/// Probe the connected transport without collapsing a kubectl/RBAC failure into
/// "dispatcher absent". An indeterminate probe refuses before enqueue so the
/// CLI never diverts a connected turn into the disconnected relay by guessing.
async fn dispatcher_connected_strict(
    namespace: &str,
    fullname: &crate::ops::ReleaseFullname,
) -> Result<bool> {
    let command = dispatcher_probe_command(namespace, fullname);
    let (ok, out, err) = run_capture(&command).await?;
    if !ok {
        bail!(
            "could not prove that Slack is disconnected for {} in namespace \
             {namespace}; refusing to guess the reply transport: {}",
            fullname.resource("dispatcher"),
            err.trim().lines().next().unwrap_or("kubectl probe failed")
        );
    }
    Ok(!out.trim().is_empty())
}

const CLUSTER_MESSAGE_RELAY_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// One disconnected cluster turn plus the opaque API bucket the worker will
/// write. The normal Slack binding coordinates stay intact so agent resolution
/// does not diverge; only reply delivery selects the reserved built-in adapter.
fn cluster_relay_turn(
    channel: &str,
    opts: &MessageOpts,
    conversation_id: &str,
) -> (QueuedTurn, uuid::Uuid) {
    let reply_ref = uuid::Uuid::new_v4();
    let mut turn = synthetic_turn(
        "slack",
        channel,
        &opts.user,
        &opts.text,
        conversation_id,
        reply_ref.hyphenated().to_string(),
        None,
    );
    turn.reply_handle.adapter = Some(CLUSTER_MESSAGE_RELAY_ADAPTER.to_string());
    (turn, reply_ref)
}

struct ClusterRelayObservation {
    outcome: Outcome,
    next_cursor: usize,
}

/// Observe one ref-keyed API relay until the worker reports a semantic
/// completion, parks for approval, or the caller's deadline expires.
///
/// A valid bucket may not exist during the enqueue-to-first-event race; the API
/// client exposes that 404 as `None`, which is retried within the same bounded
/// poll. No stream/PENDING read is used as completion: the worker's relay event
/// is the reply-delivery outcome, while XACK remains worker-owned and continues
/// even if this CLI exits.
async fn await_cluster_relay(
    api: &ApiClient,
    reply_ref: &uuid::Uuid,
    mut cursor: usize,
    timeout: Duration,
    observer: &mut impl FnMut(&str),
) -> Result<ClusterRelayObservation> {
    let deadline = Instant::now() + timeout;
    let mut latest: Option<String> = None;
    loop {
        if Instant::now() >= deadline {
            return Ok(ClusterRelayObservation {
                outcome: Outcome::TimedOut,
                next_cursor: cursor,
            });
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let page =
            match tokio::time::timeout(remaining, api.cluster_message_replies(reply_ref, cursor))
                .await
            {
                Ok(page) => page?,
                Err(_) => {
                    return Ok(ClusterRelayObservation {
                        outcome: Outcome::TimedOut,
                        next_cursor: cursor,
                    });
                }
            };
        if let Some(page) = page {
            if page.next_cursor < cursor {
                bail!(
                    "cluster-message reply cursor moved backwards from {cursor} to {}",
                    page.next_cursor
                );
            }
            let mut awaiting_approval = false;
            let mut completed = false;
            for event in page.events {
                match event.kind.as_str() {
                    "turn.status" => {
                        if let Some(status) = event.status.as_deref() {
                            observer(status);
                        }
                    }
                    "reply.update" => {
                        if let Some(text) = event.text {
                            if latest.as_deref() != Some(text.as_str()) {
                                observer(&text);
                                latest = Some(text);
                            }
                        }
                    }
                    "reply.post" => {}
                    "turn.completed" => match event.outcome.as_deref() {
                        Some("awaiting-approval") => awaiting_approval = true,
                        Some("delivered" | "dropped" | "escalated") => {
                            awaiting_approval = false;
                            completed = true;
                        }
                        Some(outcome) => {
                            bail!("cluster-message relay returned unknown outcome {outcome:?}")
                        }
                        None => bail!("cluster-message completion omitted its outcome"),
                    },
                    _ => {}
                }
            }
            cursor = page.next_cursor;
            if completed || page.terminal {
                return Ok(ClusterRelayObservation {
                    outcome: latest.map_or(Outcome::CompletedNoEdit, Outcome::Replied),
                    next_cursor: cursor,
                });
            }
            if awaiting_approval {
                let approval_id = latest.as_deref().and_then(parse_approval_id);
                return Ok(ClusterRelayObservation {
                    outcome: Outcome::AwaitingApproval {
                        reply: latest,
                        approval_id,
                    },
                    next_cursor: cursor,
                });
            }
        }

        let wake = (Instant::now() + CLUSTER_MESSAGE_RELAY_POLL_INTERVAL).min(deadline);
        tokio::time::sleep_until(tokio::time::Instant::from_std(wake)).await;
    }
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
/// (ADR-0118) because one agent may answer on several channels, and picking a
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
pub fn dry_run_lines(opts: &MessageOpts, _advertise_host: &str) -> Vec<String> {
    // Offline by contract: a dry run contacts no cluster, so it renders the
    // chart's no-override `curie.fullname` rule rather than discovering the
    // rendered name (#1533). Under `nameOverride`/`fullnameOverride` the printed
    // service names are therefore the chart-default ones, not this install's.
    let fullname = crate::ops::chart_fullname(&opts.release);
    let mut cmds: Vec<OpsCommand> = vec![port_forward_command(
        &opts.namespace,
        &fullname,
        "valkey",
        opts.valkey_local_port,
        VALKEY_REMOTE_PORT,
    )];
    cmds.push(port_forward_command(
        &opts.namespace,
        &fullname,
        "api",
        opts.api_local_port,
        API_REMOTE_PORT,
    ));
    let poll_url = format!(
        "http://127.0.0.1:{}/cluster-message-replies/<uuid-v4>",
        opts.api_local_port
    );
    let mut lines: Vec<String> = cmds.iter().map(OpsCommand::display).collect();
    lines.push(format!("poll replies at {poll_url}"));
    let channel = opts
        .channel
        .clone()
        .unwrap_or_else(|| "<the sole bound (agent, Slack channel) pair>".to_string());
    lines.push(format!(
        "enqueue a synthetic QueuedTurn (adapter {CLUSTER_MESSAGE_RELAY_ADAPTER}, no reply \
         endpoint, UUIDv4 reply ref) for channel {channel} on stream {}",
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
    reply_endpoint: Option<&str>,
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
    reply_endpoint: Option<String>,
    human_lines: Vec<String>,
}

impl crate::ui::CliOutput for MessageDryRunOutput {
    fn to_json(&self) -> serde_json::Value {
        message_dry_run_json(
            self.target,
            &self.stream,
            self.channel.as_deref(),
            self.reply_endpoint.as_deref(),
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
    /// ...` command plus the authenticated card's real channel (#766, #1531);
    /// none of them touch `to_json`, which stays byte-identical.
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
/// block until its effective local port accepts TCP, so callers can use it
/// immediately. The returned port is the requested value unless kubectl assigns
/// one for a zero request.
pub async fn start_port_forward(
    cmd: &OpsCommand,
    local_port: u16,
    label: &str,
) -> Result<(tokio::process::Child, u16)> {
    crate::ui::ui().plumbing(&format!("+ {}", cmd.display()));
    let mut child = tokio::process::Command::new(&cmd.program)
        .args(cmd.argv())
        .kill_on_drop(true)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("spawning `{}` (is kubectl on PATH?)", cmd.program))?;
    let stdout = child
        .stdout
        .take()
        .context("kubectl port forward stdout was not captured")?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let effective_port =
        wait_for_port_forward_readiness(stdout, local_port, deadline, label).await?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    wait_for_tcp(effective_port, remaining)
        .await
        .with_context(|| {
            format!("the {label} port-forward never opened localhost:{effective_port}")
        })?;
    // For a fixed request, the exact IPv4 readiness line proves that this child
    // owned the socket before the TCP check. The TCP check then proves that the
    // effective IPv4 address is reachable. A child can still exit after printing
    // readiness, so the try_wait guard catches that exit before returning it.
    if child.try_wait()?.is_some() {
        bail!(
            "the {label} port-forward exited immediately; localhost:{effective_port} is already in \
             use by another process. Free that port or stop the conflicting process, then retry."
        );
    }
    Ok((child, effective_port))
}

fn parse_forwarded_port(line: &str) -> Result<Option<u16>> {
    let Some(readiness) = line.strip_prefix("Forwarding from ") else {
        return Ok(None);
    };
    let (source, _) = readiness
        .split_once(" -> ")
        .context("kubectl reported malformed forwarding readiness")?;
    let port = source
        .parse::<std::net::SocketAddr>()
        .context("kubectl reported an invalid assigned local address")?
        .port();
    if port == 0 {
        bail!("kubectl reported zero as its assigned local port");
    }
    Ok(Some(port))
}

async fn wait_for_port_forward_readiness(
    stdout: tokio::process::ChildStdout,
    local_port: u16,
    deadline: Instant,
    label: &str,
) -> Result<u16> {
    // Kubernetes documents `Forwarding from 127.0.0.1:<port> -> <remote>` at:
    // https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/
    let readiness = if local_port == 0 {
        "a kubectl forwarding readiness line".to_string()
    } else {
        format!("Forwarding from 127.0.0.1:{local_port} ->")
    };
    let mut lines = BufReader::new(stdout).lines();
    let remaining = deadline.saturating_duration_since(Instant::now());
    let effective_port = match tokio::time::timeout(remaining, async {
        loop {
            let line = lines
                .next_line()
                .await
                .context("reading kubectl forwarding readiness")?
                .with_context(|| {
                    format!(
                        "the {label} port forward did not open 127.0.0.1:{local_port}; \
                         kubectl closed stdout before reporting {readiness}. The port may already be in use."
                    )
                })?;
            if let Some(port) = parse_forwarded_port(&line)? {
                if local_port == 0 || (port == local_port && line.starts_with(&readiness)) {
                    return Ok::<u16, anyhow::Error>(port);
                }
            }
        }
    })
    .await
    {
        Ok(result) => result?,
        Err(_) => bail!(
            "the {label} port forward did not open 127.0.0.1:{local_port} within 15 seconds; \
             kubectl never reported {readiness}."
        ),
    };
    std::mem::drop(tokio::spawn(async move {
        while let Ok(Some(_)) = lines.next_line().await {}
    }));
    Ok(effective_port)
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

/// Observe the claim gate for the same tier the message targets. The local
/// message surface has no compose-file flag, so it follows the fixed local
/// lifecycle file and project used by `curie local up`.
async fn observe_message_claims(
    opts: &MessageOpts,
    verb: TurnVerb,
) -> crate::worker_claims::ClaimsState {
    match verb {
        TurnVerb::Local => {
            crate::worker_claims::observe_local(crate::local::DEFAULT_COMPOSE_FILE).await
        }
        TurnVerb::Cluster => {
            crate::worker_claims::observe_cluster(&opts.namespace, &opts.release)
                .await
                .state
        }
    }
}

/// A known marker is useful before the first reply poll: print it on stderr and
/// attach it to the active checklist step without exposing any subprocess text.
fn report_initial_claim_wait(
    ui: &crate::ui::Ui,
    step: &crate::ui::Step,
    state: &crate::worker_claims::ClaimsState,
) {
    if let Some(reason) = state.wait_reason() {
        ui.note(&reason);
        step.tick_detail(&reason);
    }
}

/// Keep the existing diagnostics slot and prepend only the authored current
/// marker reason. No claim reason leaves the old diagnostics byte shape alone.
fn diagnostics_with_claim_wait(diagnostics: String, reason: Option<&str>) -> String {
    match reason {
        Some(reason) => format!("  {reason}\n{diagnostics}"),
        None => diagnostics,
    }
}

const COMPOSE_CONFIG_FILES_LABEL: &str = "com.docker.compose.project.config_files";
const COMPOSE_DISPATCHER_SERVICE: &str = "curie-dispatcher";
const DISPATCHER_ENQUEUE_MODULE: &str = "curie_dispatcher.enqueue_once";
const DISPATCHER_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(30);
// The one-shot dispatcher shares the runner's bridge network, not the
// worker's host network. Inspect this separately; it is never forwarded as a
// Curie configuration key into the dispatcher.
const BRIDGE_OTEL_ENDPOINT_KEY: &str = "CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT";
const OTEL_EXPORTER_ENV_KEYS: [&str; 38] = [
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_PROTOCOL",
    "OTEL_EXPORTER_OTLP_HEADERS",
    "OTEL_EXPORTER_OTLP_CERTIFICATE",
    "OTEL_EXPORTER_OTLP_CLIENT_KEY",
    "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
    "OTEL_EXPORTER_OTLP_INSECURE",
    "OTEL_EXPORTER_OTLP_COMPRESSION",
    "OTEL_EXPORTER_OTLP_TIMEOUT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
    "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
    "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
    "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
    "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
    "OTEL_EXPORTER_OTLP_TRACES_INSECURE",
    "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
    "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL",
    "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
    "OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE",
    "OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY",
    "OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE",
    "OTEL_EXPORTER_OTLP_LOGS_INSECURE",
    "OTEL_EXPORTER_OTLP_LOGS_COMPRESSION",
    "OTEL_EXPORTER_OTLP_LOGS_TIMEOUT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
    "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
    "OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE",
    "OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY",
    "OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE",
    "OTEL_EXPORTER_OTLP_METRICS_INSECURE",
    "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
    "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
    "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
    "OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
];
static DISPATCHER_ENQUEUE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Parse Compose's canonical config-file label. Compose stores multiple `-f`
/// inputs as a comma-separated list; an ordinary Curie stack carries one.
fn compose_config_files(label: &str) -> Result<Vec<String>> {
    let files: Vec<String> = label
        .trim()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "<no value>" && *value != "<nil>")
        .map(str::to_string)
        .collect();
    if files.is_empty() {
        bail!(
            "the running local worker has no `{COMPOSE_CONFIG_FILES_LABEL}` label; restart it with `curie local up`"
        );
    }
    Ok(files)
}

/// The dispatcher image the one-shot producer should run, or None to leave
/// compose's default alone.
///
/// Derived from the RUNNING api container's tag, then checked for existence, and
/// both halves are load-bearing:
///
/// - Derived, because the tag is a property of the stack rather than of this
///   invocation. Compose substitutes its variables from each caller's own
///   environment, so after `local up --build` the stack ran `:dev` while this
///   verb resolved `:latest` and died on `No module named
///   curie_dispatcher.enqueue_once`.
/// - Checked, because a tag that exists for the platform images need not exist
///   for the dispatcher. CI runs the ladder with `CURIE_BASE_TAG=ci-local` and
///   builds its dispatcher as `:latest`, so pointing this at `:ci-local` asked
///   for an image nothing had built and failed the stack.
///
/// Best-effort throughout: any unreadable step leaves compose's default in
/// force, which is the behaviour that existed before #1915.
///
/// Both halves now live in `local.rs` (#1925), because every `local` verb that
/// recreates a service needs the same derivation -- this one just narrows it to
/// the single image the one-shot producer runs.
async fn one_shot_dispatcher_image() -> Option<String> {
    let tag = crate::local::running_stack_tag().await?;
    let candidate = crate::local::image_ref("curie-dispatcher", &tag);
    crate::local::image_present(&candidate)
        .await
        .then_some(candidate)
}

fn worker_compose_config_command(container: &str) -> OpsCommand {
    OpsCommand::new(
        "docker",
        vec![
            plain("inspect"),
            plain("--format"),
            plain(format!(
                "{{{{ index .Config.Labels \"{COMPOSE_CONFIG_FILES_LABEL}\" }}}}"
            )),
            plain(container),
        ],
    )
}

fn worker_otel_exporter_env_command(container: &str) -> OpsCommand {
    // Filter before captured stdout crosses the worker process boundary. Each
    // selected entry is encoded as one JSON string so the first `=` and
    // everything after it survive intact.
    let mut template = concat!(
        "{{range .Config.Env}}",
        "{{$entry := .}}",
        "{{$name := index (split $entry \"=\") 0}}",
        "{{if or "
    )
    .to_string();
    for name in OTEL_EXPORTER_ENV_KEYS {
        template.push_str(&format!("(eq $name \"{name}\") "));
    }
    template.push_str(&format!("(eq $name \"{BRIDGE_OTEL_ENDPOINT_KEY}\") "));
    template.push_str("}}{{json $entry}}{{println}}{{end}}{{end}}");
    OpsCommand::new(
        "docker",
        vec![
            plain("inspect"),
            plain("--format"),
            plain(template),
            plain(container),
        ],
    )
}

fn parse_worker_otel_env(stdout: &str) -> Result<Vec<(String, String)>> {
    let mut selected = Vec::new();
    let mut bridge_endpoint = None;
    for (index, line) in stdout
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        let line = line.trim();
        let entry = if line.starts_with('"') {
            serde_json::from_str::<String>(line).with_context(|| {
                format!(
                    "parsing the local worker's telemetry environment entry {}",
                    index + 1
                )
            })?
        } else {
            // Some already-filtered callers and test doubles return one plain
            // Docker environment entry per line. Production inspection emits
            // JSON strings, but retaining this narrow seam is harmless because
            // the exact allowlist below still gates every accepted name.
            line.to_string()
        };
        let Some((name, value)) = entry.split_once('=') else {
            continue;
        };
        // Keep the same allowlist here as defense in depth if the Docker
        // formatting boundary ever changes independently.
        if name == BRIDGE_OTEL_ENDPOINT_KEY {
            bridge_endpoint = Some(value.to_string());
        } else if OTEL_EXPORTER_ENV_KEYS.contains(&name) {
            selected.push((name.to_string(), value.to_string()));
        }
    }
    if let Some(endpoint) = bridge_endpoint {
        // An explicit empty value disables export; absence retains legacy
        // behavior. Signal-specific exporter overrides remain unchanged.
        selected.retain(|(name, _)| name != "OTEL_EXPORTER_OTLP_ENDPOINT");
        selected.push(("OTEL_EXPORTER_OTLP_ENDPOINT".to_string(), endpoint));
    }
    Ok(selected)
}

struct LocalDispatcherContext {
    compose_files: Vec<String>,
    /// The dispatcher image the running stack would use, when it can be
    /// determined and is actually present.
    dispatcher_image: Option<String>,
    otel_env: Vec<(String, String)>,
}

async fn local_dispatcher_context() -> Result<LocalDispatcherContext> {
    let container = local_worker_container().await?;
    let cmd = worker_compose_config_command(&container);
    let (ok, stdout, stderr) = run_capture(&cmd).await?;
    if !ok {
        bail!(
            "reading the local stack's Compose configuration: {}",
            stderr.trim()
        );
    }
    let compose_files = compose_config_files(&stdout)?;

    let telemetry_cmd = worker_otel_exporter_env_command(&container);
    let (ok, telemetry_stdout, telemetry_stderr) = run_capture(&telemetry_cmd).await?;
    if !ok {
        bail!(
            "reading the local worker's telemetry configuration: {}",
            telemetry_stderr.trim()
        );
    }
    let otel_env = parse_worker_otel_env(&telemetry_stdout)?;

    // #1915: the stack's own image tag, so the one-shot producer below runs what
    // the stack runs. Best-effort: an unreadable image is not worth failing an
    // enqueue over, and compose's default then applies exactly as before.
    let dispatcher_image = one_shot_dispatcher_image().await;

    Ok(LocalDispatcherContext {
        compose_files,
        otel_env,
        dispatcher_image,
    })
}

/// Build the bounded one-shot dispatcher producer. Secrets ride named process
/// environment entries, never argv; Slack variables are explicitly cleared so
/// this process cannot acquire or use a workspace credential.
fn dispatcher_enqueue_command(
    compose_files: &[String],
    container_name: &str,
    stream: &str,
    valkey_password: &str,
    otel_env: &[(String, String)],
    dispatcher_image: Option<&str>,
) -> OpsCommand {
    // The dispatcher service lives in the slack profile but depends on core
    // services (notably the API and Valkey). Compose resolves that dependency
    // graph before the one-shot Python producer starts.
    let mut args = vec![
        plain("compose"),
        plain("--profile"),
        plain("core"),
        plain("--profile"),
        plain("slack"),
    ];
    for file in compose_files {
        args.extend([plain("-f"), plain(file)]);
    }
    args.extend([
        plain("run"),
        plain("--rm"),
        plain("--name"),
        plain(container_name),
        plain("--no-deps"),
        plain("-T"),
        plain("-e"),
        plain("VALKEY_PASSWORD"),
        plain("-e"),
        plain("CURIE_STREAM"),
        plain("-e"),
        plain("SLACK_APP_TOKEN="),
        plain("-e"),
        plain("SLACK_BOT_TOKEN="),
        plain("-e"),
        plain("SLACK_SIGNING_SECRET="),
    ]);
    for (name, _) in otel_env {
        if OTEL_EXPORTER_ENV_KEYS.contains(&name.as_str()) {
            args.extend([plain("-e"), plain(name)]);
        }
    }
    args.extend([
        plain(COMPOSE_DISPATCHER_SERVICE),
        plain("python"),
        plain("-m"),
        plain(DISPATCHER_ENQUEUE_MODULE),
    ]);
    let mut secret_env = vec![("VALKEY_PASSWORD".to_string(), valkey_password.to_string())];
    secret_env.extend(
        otel_env
            .iter()
            .filter(|(name, _)| OTEL_EXPORTER_ENV_KEYS.contains(&name.as_str()))
            .cloned(),
    );
    let mut env = vec![
        (
            "COMPOSE_PROJECT_NAME".to_string(),
            crate::local::COMPOSE_PROJECT.to_string(),
        ),
        ("CURIE_STREAM".to_string(), stream.to_string()),
    ];
    // Only when the running stack has one. Passing nothing leaves compose's
    // `${CURIE_BASE_TAG:-latest}` default in force, so a published stack behaves
    // exactly as it did.
    if let Some(image) = dispatcher_image {
        env.push(("CURIE_DISPATCHER_IMAGE".to_string(), image.to_string()));
    }
    OpsCommand::new("docker", args)
        .with_env(env)
        .with_secret_env(secret_env)
}

fn redact_dispatcher_diagnostic(stderr: &str, sensitive_values: &[&str]) -> String {
    let clean = sensitive_values
        .iter()
        .filter(|value| !value.is_empty())
        .fold(stderr.to_string(), |text, value| {
            text.replace(value, "[REDACTED]")
        });
    let clean = clean.trim();
    if clean.is_empty() {
        "one-shot dispatcher exited without a diagnostic".to_string()
    } else {
        clean.chars().take(4096).collect()
    }
}

fn parse_dispatcher_stream_id(stdout: &str) -> Result<String> {
    let mut lines = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty());
    let stream_id = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("one-shot dispatcher returned no Stream id"))?;
    let valid = stream_id.split_once('-').is_some_and(|(ms, sequence)| {
        !ms.is_empty()
            && !sequence.is_empty()
            && ms.bytes().all(|byte| byte.is_ascii_digit())
            && sequence.bytes().all(|byte| byte.is_ascii_digit())
    });
    if !valid || lines.next().is_some() {
        bail!("one-shot dispatcher returned malformed stdout instead of one Stream id");
    }
    Ok(stream_id.to_string())
}

/// Enqueue a local synthetic turn through code running in the dispatcher image.
/// The CLI retains its open Valkey connection only for completion observation;
/// the producer write itself happens inside this bounded child process.
async fn dispatcher_enqueue_local(opts: &MessageOpts, turn: &QueuedTurn) -> Result<String> {
    let context = local_dispatcher_context().await?;
    let sequence = DISPATCHER_ENQUEUE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let container_name = format!("curie-dispatcher-enqueue-{}-{sequence}", std::process::id());
    let cmd = dispatcher_enqueue_command(
        &context.compose_files,
        &container_name,
        &opts.stream,
        &opts.valkey_password,
        &context.otel_env,
        context.dispatcher_image.as_deref(),
    );
    crate::ui::ui().plumbing(&format!("+ {}", cmd.display()));
    let payload = queue::payload_json(turn)?;

    let mut child = tokio::process::Command::new(&cmd.program)
        .args(cmd.argv())
        .envs(cmd.env.iter().chain(cmd.secret_env.iter()).cloned())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .context("starting the one-shot local dispatcher producer")?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| anyhow::anyhow!("one-shot dispatcher stdin was unavailable"))?;
    let child_run = async move {
        stdin
            .write_all(payload.as_bytes())
            .await
            .context("sending the queued turn to the one-shot dispatcher")?;
        drop(stdin);
        child
            .wait_with_output()
            .await
            .context("waiting for the one-shot dispatcher producer")
    };
    let output = match tokio::time::timeout(DISPATCHER_ENQUEUE_TIMEOUT, child_run).await {
        Ok(result) => result?,
        Err(_) => {
            // Dropping child_run kills the Docker CLI. Remove the explicitly
            // named Compose container too: otherwise a blocked Valkey connect
            // can outlive the CLI with its secret environment still attached.
            let _ = crate::docker::remove_container(&container_name).await;
            bail!(
                "one-shot dispatcher did not finish within {}s",
                DISPATCHER_ENQUEUE_TIMEOUT.as_secs()
            );
        }
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let mut sensitive_values = vec![opts.valkey_password.as_str()];
    sensitive_values.extend(context.otel_env.iter().map(|(_, value)| value.as_str()));
    if !stderr.trim().is_empty() {
        let sanitized = redact_dispatcher_diagnostic(&stderr, &sensitive_values);
        for line in sanitized.lines() {
            crate::ui::ui().plumbing(line);
        }
    }
    if !output.status.success() {
        let diagnostic = redact_dispatcher_diagnostic(&stderr, &sensitive_values);
        bail!("one-shot dispatcher failed: {diagnostic}");
    }
    parse_dispatcher_stream_id(&stdout)
}

async fn enqueue_for_turn_verb(
    opts: &MessageOpts,
    conn: &mut MultiplexedConnection,
    verb: TurnVerb,
    turn: &QueuedTurn,
) -> Result<String> {
    match verb {
        TurnVerb::Local => dispatcher_enqueue_local(opts, turn).await,
        // Deliberately retain the direct lane as the missing-carrier
        // compatibility control required by #1817.
        TurnVerb::Cluster => xadd(conn, &opts.stream, turn).await,
    }
}

/// The `curie local message` handler: drive the compose stack directly.
///
/// The cluster path's self-plumbing (kubectl port-forwards) is cluster-specific,
/// so local mode keeps only the shared engine: bind the Slack stub, send the
/// `QueuedTurn` through the compose dispatcher's bounded producer, and wait on
/// the XACK signal. The turn still carries this stub as its reply endpoint
/// (issue #19). The compose worker reaches the stub on the fixed loopback port
/// `http://localhost:{DEFAULT_LOCAL_STUB_PORT}/api/`.
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
            reply_endpoint: Some(reply_endpoint),
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
    if let Some(transport) = local_connected_transport().await {
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
            &transport,
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
    let stream_id = enqueue_for_turn_verb(&opts, &mut conn, TurnVerb::Local, &event).await?;
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
    let initial_claims = observe_message_claims(&opts, TurnVerb::Local).await;
    report_initial_claim_wait(ui, &step, &initial_claims);
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
        Outcome::AwaitingApproval { reply, approval_id } => {
            step.done("awaiting approval");
            // Persist the turn context BEFORE the (possibly full --timeout-secs)
            // approval wait: if the operator interrupts or the terminal closes
            // while the approval is pending, `.curie/last-turn.json` must still
            // hold the thread for `message --continue` (#766). The terminal paths
            // re-persist the identical context and print the continue hint once.
            persist_turn_quietly(&opts, TurnVerb::Local, &channel, &thread_ts);
            // Keep the stub alive and wait for the resumed reply instead of
            // exiting and stranding it (#766). The wait rides the Valkey
            // connection already open for the enqueue. The explicit id normally
            // comes from the card and falls back to the route-bound notice; if
            // neither carries a valid UUID, report the parked terminal.
            match approval_id {
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
                            crate::exit::exit_after_drop(crate::exit::ExitClass::Transient, stub);
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
                    crate::exit::exit_after_drop(crate::exit::ExitClass::Transient, stub);
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
            let latest_claims = observe_message_claims(&opts, TurnVerb::Local).await;
            let latest_reason = latest_claims.wait_reason();
            // Gather diagnostics only on the human path; under `--json` the
            // timeout object carries no diagnostics, so skip the extra Valkey read.
            let diag = if ui.json() {
                if let Some(reason) = latest_reason.as_deref() {
                    ui.note(reason);
                }
                None
            } else {
                Some(diagnostics_with_claim_wait(
                    bounded_diagnostics(&mut conn, &opts.stream, &stream_id).await,
                    latest_reason.as_deref(),
                ))
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

/// The whole budget for one advisory hint-channel lookup, port-forward startup
/// included (#1531 finding 3).
///
/// Modeled on [`ApiClient::check_git_flow_routing`], which is this crate's
/// established shape for an advisory read that must never become a hang: 10s is
/// far past a healthy answer and far short of an operator's patience. The bound
/// is applied ONCE around the entire lookup rather than per request, because the
/// cluster arm spends most of its time in `start_port_forward` and a per-request
/// deadline would leave that unbounded. `ApiClient::new` sets only
/// `connect_timeout`, so a peer that accepts the connection and then never sends
/// headers would otherwise wait forever -- and this runs INSIDE the resume wait,
/// where forever means a frozen terminal on a turn whose durable approval is
/// already fine.
///
/// A CEILING, not the effective budget. Because this runs inside the resume
/// wait, spending it on top of the turn's own deadline would make
/// `--timeout-secs 1` take about eleven seconds, and every nested gate would add
/// another ten. [`hint_channel`] therefore caps it with
/// [`crate::chat::capped`] against the turn deadline, which is the invariant
/// `cli/src/chat.rs:497-499` already states for the resume scan: "Every per-op
/// budget is capped by what is LEFT of the overall deadline, so the advertised
/// `--timeout-secs` is a hard bound on this path too rather than being overrun
/// by up to one fixed scan budget" (#1531).
const HINT_CHANNEL_LOOKUP_BUDGET: Duration = Duration::from_secs(10);

/// The channel the pre-wait resolve hint should name for approval `id`: the
/// approval's own `card_channel` when a route binding placed the card somewhere
/// other than where this turn spoke, else `turn_channel` (#1531 finding 3).
///
/// ADVISORY, and deliberately incapable of failing or delaying the turn. Every
/// non-answer -- an unreachable API, a 404 because another operator resolved the
/// approval first, a 5xx, a body that does not decode, an expired budget, a null
/// or empty `card_channel` -- returns `turn_channel`, which is byte-for-byte what the
/// hint printed before this change. The worst case is therefore the status quo,
/// never a regression. Nothing here propagates: no `?` and no `unwrap` escapes
/// the wrapper, since the caller is mid-wait on a durable approval and has
/// nothing useful to do with an error.
///
/// A null `card_channel` means "an older row or a direct API write, so the
/// requesting channel applies" (#1431), and the requesting channel IS the turn
/// channel -- so it takes the same fallback rather than printing an empty or
/// literal-null card location.
///
/// An EMPTY (or whitespace-only) `card_channel` takes that same fallback, and
/// the reason is the server, not caution. The wire model admits
/// `card_channel: ""` (`packages/aci-protocol/src/aci_protocol/wire.py`), and
/// the authorizer selects the approver set as
/// `approval.card_channel or approval.reply_channel`
/// (`apps/api/src/curie_api/slack_approvers.py`). An empty string is FALSY in
/// Python, so the server itself reads it as absent and falls back to
/// `reply_channel` -- which is the turn channel. The CLI therefore describes
/// that requesting channel as the authenticated card's location. "Empty string
/// is not the same as absent" is true in Rust and false on this wire, so do not
/// collapse this arm back into a plain `Some(_)` match.
///
/// `deadline` is the TURN's overall deadline, not this lookup's. The effective
/// bound is `capped(HINT_CHANNEL_LOOKUP_BUDGET, deadline)`, so a short
/// `--timeout-secs` shortens the lookup instead of being overrun by it
/// (`cli/src/chat.rs:497-499`, #1531).
///
/// Tier dispatch mirrors how each tier already reaches the API for the
/// default-channel lookup: local talks straight to the compose API at
/// [`local_api_base`], cluster opens a short-lived `kubectl port-forward` the way
/// [`resolve_cluster_channel`] does. The cluster guard is created and dropped
/// entirely INSIDE this function, so no forward is ever held across
/// [`await_resume`] and the (possibly full `--timeout-secs`) wait. Because the
/// budget wraps the port-forward startup too, an expiry drops the in-flight
/// future, whose `kill_on_drop` Drop reaps the child: there is no path where the
/// deadline fires and leaves a `kubectl port-forward` orphaned to init, which is
/// the tracked regression class from #751/#766.
async fn hint_channel(
    opts: &MessageOpts,
    verb: TurnVerb,
    turn_channel: &str,
    id: &str,
    deadline: Instant,
) -> String {
    let lookup = async {
        // The port-forward guard is bound HERE, in the enclosing async block,
        // and deliberately NOT inside the cluster match arm. `start_port_forward`
        // returns the `kubectl port-forward` child with `kill_on_drop(true)`, so
        // the binding's scope IS the forward's lifetime. An arm-scoped binding
        // ends when the arm yields its value, which is one line BEFORE
        // `get_approval` runs: the child is reaped, the local port goes dead, the
        // request fails, and the advisory wrapper silently degrades to the turn
        // channel, so the cluster tier could never resolve a card channel. That
        // was observed against a live cluster, where the approval row held the
        // route's channel and `approvals --list` read it correctly through its
        // own forward while this hint still printed the turn channel (#1531).
        // Do not "tidy" this back into the arm.
        //
        // Binding it here still keeps the guard entirely inside this helper: it
        // drops at the end of this async block, after `get_approval` has
        // resolved, so no forward is held across `await_resume`, and a timeout
        // drops this future part way through and runs the same Drop, which is
        // what keeps an expiry from orphaning a `kubectl port-forward` to init
        // (the tracked leak class from #751/#766).
        let (_api_pf, api_base) = match verb {
            TurnVerb::Local => (
                // No forward on the local tier: compose publishes the API, so the
                // guard slot stays empty and this arm behaves exactly as before.
                None,
                local_api_base(opts.api_url.as_deref()),
            ),
            TurnVerb::Cluster => {
                let fullname = crate::ops::release_fullname(&opts.namespace, &opts.release).await;
                let (api_pf, api_local_port) = start_port_forward(
                    &port_forward_command(
                        &opts.namespace,
                        &fullname,
                        "api",
                        opts.api_local_port,
                        API_REMOTE_PORT,
                    ),
                    opts.api_local_port,
                    "api",
                )
                .await
                .ok()?;
                (Some(api_pf), format!("http://127.0.0.1:{api_local_port}"))
            }
        };
        let api = ApiClient::new(&api_base, &opts.api_key).ok()?;
        api.get_approval(id).await.ok()?.card_channel
    };
    // Capped, never fixed: the lookup may spend the advisory budget or what is
    // LEFT of the turn, whichever is smaller, so it can never outlive the turn
    // it is decorating (#1531; `cli/src/chat.rs:497-499`). Reuses the same
    // `capped` helper the resume scan uses rather than a second copy of the
    // bound.
    match tokio::time::timeout(capped(HINT_CHANNEL_LOOKUP_BUDGET, deadline), lookup).await {
        // A present, non-empty card channel is the only real answer. The
        // emptiness guard is load-bearing, not defensive: the server reads
        // `approval.card_channel or approval.reply_channel`, and in Python
        // that `or` treats ONLY the empty string as falsy. A whitespace-only
        // value such as a single space is truthy there, so the server treats it
        // as the real card channel. This arm must mirror that exactly and print
        // a whitespace-only channel verbatim rather than trimming it away
        // (#1531, see the doc comment above). Do not add `.trim()` back: a
        // trimmed whitespace-only channel would confidently direct the operator
        // to the wrong authenticated card location.
        Ok(Some(card_channel)) if !card_channel.is_empty() => card_channel,
        // Every remaining arm is the same answer: "no answer". An expired budget
        // is indistinguishable from a 500 here, and a null or empty
        // `card_channel` means the requesting channel applies -- all of them
        // print what the hint printed before this change.
        Ok(_) | Err(_) => turn_channel.to_string(),
    }
}

/// The one runnable `approvals --resolve` command shape, shared by the pre-wait
/// hint and the terminal wording so the two cannot drift (#766).
///
/// Every part here is load-bearing against the server and operator recovery:
/// - `<AGENT>` is a REQUIRED positional on the `approvals` clap surface
///   (`AgentTarget` flattens a mandatory `agent` arg), so omitting it made the
///   printed hint fail with `error: the following required arguments were not
///   provided: <AGENT>`.
/// - The terminal command carries no asserted actor or channel. It authenticates
///   with `CURIE_APPROVAL_PRINCIPAL_TOKEN`, and that channel-less operator
///   principal is eligible only for an explicit-user approver set (ADR-0106).
/// - `channel` is still the approval's real card location. The default approver
///   set is that authenticated card channel's membership, so an operator without
///   an explicit-user token route must act through the authenticated card. The
///   pre-wait hint resolves `card_channel` per approval ([`hint_channel`],
///   #1531), including nested gates, rather than pointing at the turn/stub
///   channel.
///
/// This stays a PURE formatter: it renders the channel it is GIVEN, verbatim,
/// and does no I/O. The lookup lives in the caller precisely so a string helper
/// shared by an async wait and a terminal print does not acquire a network
/// dependency.
fn approval_resolve_command(tier: &str, agent: Option<&str>, channel: &str, id: &str) -> String {
    let agent = agent.unwrap_or("<AGENT>");
    format!(
        "curie {tier} approvals {agent} --resolve {id}  # authenticated card channel: \
         {channel:?}; a terminal principal works only for an explicit-user route"
    )
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
         <AGENT> --list`). Set CURIE_APPROVAL_PRINCIPAL_TOKEN only for a route whose approvers \
         are an explicit user list; otherwise use the authenticated card in the channel named \
         above. Because this command has exited, the resumed reply does NOT print here -- read \
         it from the agent transcript. There is no clickable Slack card unless a real workspace \
         is connected (`curie {tier} comms --slack`).",
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
    // The channel the most recent pre-wait hint was resolved for, hoisted out of
    // the loop the same way `last_reply` is and for the same reason: the POST-loop
    // terminal has to report what the last iteration observed. The terminal line
    // is the one an operator actually copies once the wait gives up, so it must
    // not restate the turn channel the pre-wait hint just corrected (#1531).
    // Invariant: this is the channel resolved for the CURRENT `current_id`, or the
    // turn channel when nothing has been resolved for that id yet -- never a value
    // resolved for a different approval.
    let mut last_hint_channel = channel.to_string();
    for _ in 0..MAX_NESTED_GATES {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        // Resolve the hint's channel PER ITERATION, against `current_id`. A
        // nested gate advances `current_id` to a different approval that may be
        // bound to a different route, and reusing the first approval's channel
        // for the second would be a confidently wrong value -- strictly worse
        // than the turn channel, because the operator has no signal it is wrong
        // (#1531). Skipped under `--json`, where `ui.note` prints nothing and
        // the lookup would be a pure cost, the same way the timeout terminal
        // skips its diagnostics read on that path.
        // Assigned rather than re-bound per iteration so the post-loop terminal
        // reads the SAME resolved value the hint printed. Under `--json` this is
        // the turn channel with no lookup, exactly as before, so that path stays
        // byte-identical.
        last_hint_channel = if ui.json() {
            channel.to_string()
        } else {
            hint_channel(opts, verb, channel, &current_id, deadline).await
        };
        // Recompute AFTER the lookup, because the lookup itself consumes turn
        // time. The pre-lookup value is stale by up to the whole lookup budget,
        // and `await_resume` starts a FRESH deadline from whatever it is handed
        // -- so passing the stale value made `--timeout-secs 1` take about
        // eleven seconds and let every nested gate add another lookup on top
        // (#1531). Capping the lookup alone does not fix this half: the lookup
        // is bounded either way, but the wait must be told what is actually
        // left, or the advertised `--timeout-secs` stops being the hard bound
        // `cli/src/chat.rs:497-499` promises.
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            // The lookup consumed the remainder. Exit exactly as the top-of-loop
            // check would on any other exhausted deadline -- the approval stays
            // durable and resolvable -- rather than entering the wait with a
            // zero budget, which would print "waiting..." and return instantly.
            break;
        }
        ui.note(&format!(
            "resolve it with: {}",
            approval_resolve_command(tier, agent, &last_hint_channel, &current_id)
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
            Outcome::AwaitingApproval {
                reply: new_reply,
                approval_id,
            } => {
                // The RESUMED turn hit a NEW gate of its own. Keep waiting on ITS
                // resume entry rather than exiting and dropping the stub -- exiting
                // here would re-create the dead-endpoint bug for the nested gate.
                match approval_id {
                    Some(new_id) => {
                        current_id = new_id;
                        last_reply = new_reply;
                        // The hoisted hint belonged to the PREVIOUS approval, which
                        // may be bound to a different route. Drop it back to the
                        // turn channel so a break at the next loop boundary reports
                        // a merely-imprecise channel rather than a confidently wrong
                        // one the operator has no signal about (#1531); the next
                        // iteration's lookup replaces it before any wait.
                        last_hint_channel = channel.to_string();
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
                // Never resolved: the durable approval is still pending. Report the
                // channel the pre-wait hint just resolved for THIS `current_id`,
                // not the turn channel: this terminal is the last line printed and
                // the one the operator copies after the wait gives up, so restating
                // the already-corrected channel here would undo the fix on the very
                // path that needs it (#1531).
                ui.emit(&MessageOutcomeOutput::AwaitingApproval {
                    thread: thread_ts.to_string(),
                    reply: last_reply,
                    tier,
                    agent: agent.map(str::to_string),
                    channel: last_hint_channel,
                });
                // Persist the turn context even on the transient exit so a follow-up
                // `--continue` still has the thread to resume against.
                persist_and_hint(opts, verb, channel, thread_ts);
                return ResumeExit::Transient;
            }
        }
    }
    // The deadline elapsed at a loop boundary, or the nested-gate cap was hit. The
    // current approval is still pending and resolvable later. Same reason as the
    // in-loop timeout terminal: this is the last line the operator sees and copies,
    // so it reports the hoisted hint rather than re-stating the turn channel
    // (#1531). No lookup happens here -- if no iteration ever ran (the deadline was
    // already spent on entry), the hoisted value is still the turn channel, exactly
    // as this emit read before.
    ui.emit(&MessageOutcomeOutput::AwaitingApproval {
        thread: thread_ts.to_string(),
        reply: last_reply,
        tier,
        agent: agent.map(str::to_string),
        channel: last_hint_channel,
    });
    persist_and_hint(opts, verb, channel, thread_ts);
    ResumeExit::Transient
}

/// Keep polling the same API relay bucket after an approval parks the turn.
/// The resume queue preserves the original reply handle, so neither a new
/// callback destination nor a stream scan is needed: resumed and nested-gate
/// events append under the same opaque ref.
#[allow(clippy::too_many_arguments)]
async fn resume_cluster_after_approval(
    opts: &MessageOpts,
    api: &ApiClient,
    reply_ref: &uuid::Uuid,
    mut cursor: usize,
    id: &str,
    thread_ts: &str,
    channel: &str,
    agent: Option<&str>,
    awaiting_reply: Option<String>,
    remaining: Duration,
) -> Result<ResumeExit> {
    let ui = crate::ui::ui();
    let deadline = Instant::now() + remaining;
    const MAX_NESTED_GATES: usize = 64;
    let mut current_id = id.to_string();
    let mut last_reply = awaiting_reply;
    let mut last_hint_channel = channel.to_string();

    for _ in 0..MAX_NESTED_GATES {
        if Instant::now() >= deadline {
            break;
        }
        last_hint_channel = if ui.json() {
            channel.to_string()
        } else {
            hint_channel(opts, TurnVerb::Cluster, channel, &current_id, deadline).await
        };
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        ui.note(&format!(
            "resolve it with: {}",
            approval_resolve_command(
                tier_str(TurnVerb::Cluster),
                agent,
                &last_hint_channel,
                &current_id,
            )
        ));
        ui.note(
            "waiting for the approval to be resolved; the resumed reply lands here if it is \
             resolved before --timeout-secs elapses...",
        );
        let cl = ui.checklist();
        let step = cl.step("waiting for resumed worker reply");
        let observed = {
            let mut observe_update = |text: &str| {
                if let Some(line) = text.lines().rev().find(|line| !line.trim().is_empty()) {
                    step.tick_detail(line.trim());
                }
            };
            await_cluster_relay(api, reply_ref, cursor, remaining, &mut observe_update).await?
        };
        step.clear();
        cursor = observed.next_cursor;
        match observed.outcome {
            Outcome::Replied(reply) => {
                ui.emit(&MessageOutcomeOutput::Replied {
                    thread: thread_ts.to_string(),
                    reply,
                });
                persist_and_hint(opts, TurnVerb::Cluster, channel, thread_ts);
                return Ok(ResumeExit::Done);
            }
            Outcome::CompletedNoEdit => {
                ui.emit(&MessageOutcomeOutput::NoEdit {
                    thread: thread_ts.to_string(),
                });
                persist_and_hint(opts, TurnVerb::Cluster, channel, thread_ts);
                return Ok(ResumeExit::Done);
            }
            Outcome::AwaitingApproval {
                reply: new_reply,
                approval_id,
            } => {
                if let Some(new_id) = approval_id {
                    current_id = new_id;
                    last_reply = new_reply;
                    continue;
                }
                ui.emit(&MessageOutcomeOutput::AwaitingApproval {
                    thread: thread_ts.to_string(),
                    reply: new_reply,
                    tier: tier_str(TurnVerb::Cluster),
                    agent: agent.map(str::to_string),
                    channel: channel.to_string(),
                });
                persist_and_hint(opts, TurnVerb::Cluster, channel, thread_ts);
                return Ok(ResumeExit::Transient);
            }
            Outcome::TimedOut => break,
        }
    }

    ui.emit(&MessageOutcomeOutput::AwaitingApproval {
        thread: thread_ts.to_string(),
        reply: last_reply,
        tier: tier_str(TurnVerb::Cluster),
        agent: agent.map(str::to_string),
        channel: last_hint_channel,
    });
    persist_and_hint(opts, TurnVerb::Cluster, channel, thread_ts);
    Ok(ResumeExit::Transient)
}

/// The shared target noun message handler.
/// The local stub's sentinel bot token (`comms::LOCAL_SLACK_STUB_BOT_TOKEN`).
/// `local comms --disconnect` restores this value, so seeing it means the compose
/// dispatcher is wired to the local stub -- NOT a real workspace.
const LOCAL_STUB_BOT_TOKEN: &str = "xoxb-dev";

/// The host in `comms::LOCAL_SLACK_STUB_URL`. A worker whose `SLACK_API_BASE_URL`
/// points here talks to the in-compose stub, never to Slack.
const LOCAL_SLACK_STUB_HOST: &str = "localhost:8155";

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
///
///    An **empty** value is the CONNECTED signal here, not the disconnected one,
///    and reading it the other way is what made this whole path unreachable
///    (#1031). `local comms --connect` un-wires the stub by setting
///    `SLACK_API_BASE_URL` to the empty string (`comms::local_connect_commands`),
///    compose's single-dash `${SLACK_API_BASE_URL-http://localhost:8155/api/}`
///    preserves an explicitly empty value rather than re-defaulting it, and
///    `docs/slack-local-runbook.md` documents the convention; the cluster tier
///    says the same thing with `worker.slackApiBaseUrl=`. Empty therefore means
///    "no override -> real slack.com", which is exactly what
///    `SlackTransport::new(None, ..)` resolves to. What genuinely carries no
///    information is an ABSENT value (`None`): the probe could not read the
///    container at all, and that stays the stub path.
/// 2. the token must be a real one, not the `xoxb-dev` sentinel.
///
/// Returning the worker's OWN token (rather than one the CLI resolved) also
/// keeps the posting identity equal to the updating identity: Slack only lets the
/// authoring bot `chat.update` a message, so two different tokens would leave the
/// placeholder stuck at "..." with `cant_update_message` (#957 mode B).
///
/// Returning the base ALONGSIDE the token is what closes #1030 at this tier. Both
/// values were already read out of the same container here; only the token was
/// kept, and the base was then re-read from the CLI's own process environment at
/// post time. Carrying the pair means the placeholder goes exactly where the
/// worker that will edit it is pointed, and a stale `SLACK_API_BASE_URL` in the
/// operator's shell has nothing left to decide.
///
/// Pure, so both conditions are unit-testable without Docker.
fn connected_worker_transport(transport: WorkerTransport) -> Option<crate::slack::SlackTransport> {
    let (api_base, token) = transport;
    // Absent, not empty. `None` means the value was never read off the container
    // -- there is no worker transport to trust, so take the stub path (#1031).
    let api_base = api_base?;
    // Wired to the stub is not connected: the stub is literally the transport the
    // worker will edit the placeholder over, so no real post can ever be updated.
    if api_base.contains(LOCAL_SLACK_STUB_HOST) {
        return None;
    }
    let token = token?.trim().to_string();
    // An EMPTY base is carried through as "the source named no base", which
    // `SlackTransport::new` resolves to real Slack -- the same answer the worker
    // itself reaches. See the empty-is-connected paragraph above (#1031).
    (!token.is_empty() && token != LOCAL_STUB_BOT_TOKEN)
        .then(|| crate::slack::SlackTransport::new(Some(api_base), token))
}

/// How long the whole worker-transport probe (resolve the container, then read
/// its env) may take before `local message` gives up on it.
///
/// A wedged docker daemon accepts the request and then never answers, so an
/// unbounded probe hangs `curie local message` forever -- which also makes the
/// "a probe failure falls back to the safe stub path" claim vacuous, since
/// nothing ever fails. Two local `docker` reads have no business taking longer
/// than this (#1031).
const WORKER_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Read the running compose worker's Slack transport out of the container.
///
/// `docker inspect` on the worker container, so what we act on is what the worker
/// holds -- the reconciliation #957 asks for.
///
/// The container is resolved through the compose SERVICE LABEL
/// ([`local_worker_container`] / [`COMPOSE_WORKER_SERVICE`]), never by a
/// hardcoded name. Container names carry the compose project
/// (`<project>-curie-worker-1`, and the CLI pins `COMPOSE_PROJECT_NAME=curie`),
/// so inspecting a bare `curie-worker` matched nothing on a default stack and
/// silently classified every stack as disconnected (#1031). The selector also
/// closes the inverse hazard: an unrelated container that merely happens to be
/// NAMED `curie-worker` carries no compose service label, so it can no longer
/// impersonate the worker and hand this probe a real token the actual worker
/// does not hold.
///
/// Failures are returned rather than swallowed, so the caller can say that the
/// probe could not run instead of asserting a disconnected stack it never saw.
async fn running_worker_transport() -> Result<WorkerTransport> {
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
    let find = |key: &str| -> Option<String> {
        stdout
            .lines()
            .find_map(|l| l.strip_prefix(key).map(|v| v.to_string()))
    };
    Ok((find("SLACK_API_BASE_URL="), find("SLACK_BOT_TOKEN=")))
}

/// Bound `probe` by `budget`, flattening both a probe error and a timeout into
/// one operator-facing reason string. Generic over the future purely so the
/// timeout itself is testable without a wedged daemon.
async fn bounded_worker_probe<F>(
    probe: F,
    budget: Duration,
) -> std::result::Result<WorkerTransport, String>
where
    F: std::future::Future<Output = Result<WorkerTransport>>,
{
    match tokio::time::timeout(budget, probe).await {
        Ok(Ok(transport)) => Ok(transport),
        Ok(Err(err)) => Err(format!("{err:#}")),
        Err(_) => Err(format!("the docker probe did not answer within {budget:?}")),
    }
}

/// The warning a probe that could not RUN emits, mirroring the cluster sibling
/// (`ops::dispatcher_connected`, #957 mode C) that the same diff introduced this
/// path alongside. A probe that could not run is not evidence that no workspace
/// is connected: silently downgrading to the stub path means the operator asked
/// for the connected mode, got the other one, and was told nothing. Still fall
/// back -- the stub path never posts to real Slack, and failing the whole command
/// on a flaky docker would be worse -- but say so (#1031).
fn worker_probe_warning(reason: &str) -> String {
    format!(
        "could not determine whether a Slack workspace is connected (the local worker \
         transport probe failed: {}); assuming NOT connected and using the local reply stub",
        reason.trim().lines().next().unwrap_or("no detail")
    )
}

/// Process-level wrapper: the transport to post over, or `None` for the stub path.
///
/// `pub` so `cli/tests/local_connectedness_probe.rs` can drive the whole probe
/// (resolve by label -> inspect -> classify) against a `docker` shim; the pure
/// predicate alone cannot see the wiring both #1031 defects lived in.
pub async fn local_connected_transport() -> Option<crate::slack::SlackTransport> {
    match bounded_worker_probe(running_worker_transport(), WORKER_PROBE_TIMEOUT).await {
        Ok(transport) => connected_worker_transport(transport),
        Err(reason) => {
            crate::ui::ui().warn(&worker_probe_warning(&reason));
            None
        }
    }
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
    transport: &crate::slack::SlackTransport,
) -> Result<()> {
    let ui = crate::ui::ui();
    // An empty `--thread` is normalized away at the [`message`] entry point, and
    // re-applied HERE because this function is `pub` (an integration test drives
    // it directly, bypassing `message`), so it cannot assume its callers
    // normalized. See [`normalize_thread`]. Anything present past this line names
    // a real thread.
    let explicit_thread = opts.thread.as_deref().filter(|ts| !ts.is_empty());

    let placeholder_ts =
        crate::slack::post_placeholder(transport, channel, "\u{2026}", explicit_thread)
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
    let stream_id = enqueue_for_turn_verb(opts, conn, verb, &event).await?;
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
async fn resolve_cluster_channel(
    opts: &MessageOpts,
    fullname: &crate::ops::ReleaseFullname,
) -> Result<(String, Option<String>)> {
    match opts.channel.as_deref() {
        Some(channel) => Ok((channel.to_string(), None)),
        None => {
            let (_api_pf, api_local_port) = start_port_forward(
                &port_forward_command(
                    &opts.namespace,
                    fullname,
                    "api",
                    opts.api_local_port,
                    API_REMOTE_PORT,
                ),
                opts.api_local_port,
                "api",
            )
            .await?;
            let api = ApiClient::new(&format!("http://127.0.0.1:{api_local_port}"), &opts.api_key)?;
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
async fn message_connected(
    opts: MessageOpts,
    fullname: &crate::ops::ReleaseFullname,
) -> Result<()> {
    let ui = crate::ui::ui();

    // Valkey port-forward for the enqueue (killed on drop at fn end).
    let (_valkey_pf, valkey_local_port) = start_port_forward(
        &port_forward_command(
            &opts.namespace,
            fullname,
            "valkey",
            opts.valkey_local_port,
            VALKEY_REMOTE_PORT,
        ),
        opts.valkey_local_port,
        "valkey",
    )
    .await?;

    let (channel, _agent_hint) = resolve_cluster_channel(&opts, fullname).await?;
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
    // The base comes from the same release as the token (#1030), never from the
    // CLI's own environment. A `CURIE_SLACK_BOT_TOKEN` override changes only the
    // identity, not the destination: an operator working around a stale Secret is
    // still posting into this release's workspace, and letting the override drag
    // the URL along would restore exactly the split this issue is about.
    let transport = match crate::ops::discover_slack_api_base_url(&opts.namespace, &opts.release)
        .await
    {
        crate::ops::SlackApiBase::Configured(base) => {
            ui.note(&format!(
                "posting over the release's configured Slack base {base}"
            ));
            crate::slack::SlackTransport::new(Some(base), bot_token)
        }
        crate::ops::SlackApiBase::RealSlack => crate::slack::SlackTransport::new(None, bot_token),
        // Refusing here rather than defaulting is the whole point of the tri-state.
        // "Could not look" is not evidence that the worker talks to real Slack, and
        // acting as if it were would post a real placeholder somewhere the worker
        // can never edit -- an orphaned ellipsis in a real channel, which is the
        // #957 mode A failure arriving by a different road.
        crate::ops::SlackApiBase::Unknown => {
            anyhow::bail!(
                "could not read the Slack API base from the worker Deployment for release \
                 {} in namespace {}; refusing to guess where a real workspace token should be \
                 sent. Check `kubectl -n {} get deployment -l \
                 app.kubernetes.io/instance={},app.kubernetes.io/component=worker`.",
                opts.release,
                opts.namespace,
                opts.namespace,
                opts.release
            );
        }
    };

    let valkey_url = format!(
        "redis://:{}@127.0.0.1:{valkey_local_port}",
        opts.valkey_password
    );
    let mut conn = connect(&valkey_url).await?;
    enqueue_over_connected_transport(&opts, &mut conn, TurnVerb::Cluster, &channel, &transport)
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
        let relay_poll = format!(
            "http://127.0.0.1:{}/cluster-message-replies/<uuid-v4>",
            opts.api_local_port
        );
        ui.emit(&MessageDryRunOutput {
            target: "cluster",
            stream: opts.stream.clone(),
            channel: opts.channel.clone(),
            reply_endpoint: None,
            human_lines: dry_run_lines(&opts, &relay_poll),
        });
        return Ok(());
    }

    require_on_path("kubectl")?;

    // Resolve the release's rendered fullname once, here: every kubectl target
    // below is a chart resource, and `--dry-run` returned above without ever
    // reaching a cluster (#1533).
    let fullname = crate::ops::release_fullname(&opts.namespace, &opts.release).await;

    // Connected-transport path (#770/ADR-0078): when a real workspace is
    // connected (a running dispatcher), post a real placeholder and enqueue
    // against its ts with no per-turn endpoint, so the approval card and any
    // resumed reply ride the connected transport. A kubectl failure remains
    // fail-closed; only positively observed absence selects the built-in relay.
    if dispatcher_connected_strict(&opts.namespace, &fullname).await? {
        return message_connected(opts, &fullname).await;
    }

    // Valkey remains the enqueue transport. Replies use a separate API
    // port-forward below; neither path mutates any Deployment or replaces pods.
    let (_valkey_pf, valkey_local_port) = start_port_forward(
        &port_forward_command(
            &opts.namespace,
            &fullname,
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
    let (channel, agent_hint) = resolve_cluster_channel(&opts, &fullname).await?;
    ui.plumbing(&format!("routing to channel {channel}"));

    // The worker self-dials the in-cluster API; this distinct loopback tunnel is
    // read-only and exists only for the CLI's ref-keyed polling.
    let (_api_pf, api_local_port) = start_port_forward(
        &port_forward_command(
            &opts.namespace,
            &fullname,
            "api",
            opts.api_local_port,
            API_REMOTE_PORT,
        ),
        opts.api_local_port,
        "api",
    )
    .await?;
    let api = ApiClient::new(&format!("http://127.0.0.1:{api_local_port}"), &opts.api_key)?;

    // Enqueue the exact Slack binding coordinates the dispatcher would produce,
    // with only the reserved adapter/ref selecting the built-in relay.
    let valkey_url = format!(
        "redis://:{}@127.0.0.1:{valkey_local_port}",
        opts.valkey_password
    );
    let mut conn = connect(&valkey_url).await?;
    let (channel, thread_ts, _) = resolve_targets(Some(&channel), opts.thread.as_deref());
    let (event, reply_ref) = cluster_relay_turn(&channel, &opts, &thread_ts);
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
    let initial_claims = observe_message_claims(&opts, TurnVerb::Cluster).await;
    report_initial_claim_wait(ui, &step, &initial_claims);
    let wait_started = Instant::now();
    let observed = {
        let mut observe_update = |text: &str| {
            if let Some(line) = text.lines().rev().find(|line| !line.trim().is_empty()) {
                step.tick_detail(line.trim());
            }
        };
        await_cluster_relay(
            &api,
            &reply_ref,
            0,
            Duration::from_secs(opts.timeout_secs),
            &mut observe_update,
        )
        .await?
    };

    match observed.outcome {
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
        Outcome::AwaitingApproval { reply, approval_id } => {
            step.done("awaiting approval");
            // Persist the turn context BEFORE the (possibly full --timeout-secs)
            // approval wait, so an interrupted terminal still leaves a thread
            // `message --continue` can recover (#766). Terminal paths re-persist
            // the identical context and print the continue hint once.
            persist_turn_quietly(&opts, TurnVerb::Cluster, &channel, &thread_ts);
            // The API resume queue preserves this turn's reply handle, so keep
            // polling the same opaque bucket through any approval resolution.
            match approval_id {
                Some(id) => {
                    let remaining = Duration::from_secs(opts.timeout_secs)
                        .saturating_sub(wait_started.elapsed());
                    match resume_cluster_after_approval(
                        &opts,
                        &api,
                        &reply_ref,
                        observed.next_cursor,
                        &id,
                        &thread_ts,
                        &channel,
                        agent_hint.as_deref(),
                        reply,
                        remaining,
                    )
                    .await?
                    {
                        ResumeExit::Done => Ok(()),
                        ResumeExit::Transient => crate::exit::exit_after_drop(
                            crate::exit::ExitClass::Transient,
                            (_api_pf, _valkey_pf),
                        ),
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
                    crate::exit::exit_after_drop(
                        crate::exit::ExitClass::Transient,
                        (_api_pf, _valkey_pf),
                    );
                }
            }
        }
        Outcome::TimedOut => {
            step.fail(&format!("timed out after {}s", opts.timeout_secs));
            let latest_claims = observe_message_claims(&opts, TurnVerb::Cluster).await;
            let latest_reason = latest_claims.wait_reason();
            // Gather diagnostics only on the human path; under `--json` the
            // timeout object carries no diagnostics, so skip the extra Valkey read.
            let diag = if ui.json() {
                if let Some(reason) = latest_reason.as_deref() {
                    ui.note(reason);
                }
                None
            } else {
                Some(diagnostics_with_claim_wait(
                    bounded_diagnostics(&mut conn, &opts.stream, &stream_id).await,
                    latest_reason.as_deref(),
                ))
            };
            ui.emit(&MessageOutcomeOutput::TimedOut {
                diagnostics: diag,
                resume_note: None,
            });
            // A timeout is retryable. Drop both process-owned port-forwards
            // before the non-unwinding transient exit.
            crate::exit::exit_after_drop(crate::exit::ExitClass::Transient, (_api_pf, _valkey_pf));
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
    /// Case selector (`--case-id`, repeatable). Empty runs the whole suite.
    /// A value matching no case in the suite exits 2 (Usage) rather than
    /// silently narrowing to nothing -- a mistyped selector fails the gate.
    pub case_ids: Vec<String>,
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
    /// Independent samples per case and the aggregation policy (#1907).
    pub sampling: crate::eval_sampling::SampleConfig,
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
            Outcome::CompletedNoEdit | Outcome::AwaitingApproval { .. } | Outcome::TimedOut => {
                false
            }
        },
        // Gate-blocked assertion: the turn must have parked awaiting approval, and
        // the latest placeholder text (the model's narration before the gate flip)
        // must satisfy the grader. A match-anything grader ({kind:contains,expected:""})
        // asserts purely on the gate holding.
        ExpectedStatus::AwaitingApproval => match outcome {
            Outcome::AwaitingApproval { reply, .. } => {
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
        // Offline by contract: a dry run contacts no cluster, so it renders the
        // chart's no-override `curie.fullname` rule rather than discovering the
        // rendered name (#1533).
        let fullname = crate::ops::chart_fullname(&opts.release);
        lines.push(
            port_forward_command(
                &opts.namespace,
                &fullname,
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
                    &fullname,
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
        "sampling: {} sample(s), {}",
        opts.sampling.n, opts.sampling.policy
    ));
    lines.push(format!(
        "enqueue one synthetic QueuedTurn per case on stream {}",
        opts.stream
    ));
    lines.push(
        "without ambient durable agent memory (eval: conversation prefix per case)".to_string(),
    );
    lines
}

/// Resolve the eval suite the way `skill eval` does: an explicit `--cases`
/// wins, else `evals/cases.json` in the cwd, then the recorded bundle dir.
fn resolve_eval(explicit: Option<PathBuf>) -> Result<LoadedEval> {
    let state_plugin_dir = crate::state::load(Path::new("."))?.map(|s| PathBuf::from(s.plugin_dir));
    let path = crate::commands::resolve_cases_path(
        explicit,
        Path::new("."),
        None,
        state_plugin_dir.as_deref(),
    )?;
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
    let sampling = opts.sampling;
    let total = suite.cases.len();
    let bar = ui.progress_bar(
        (total as u64).saturating_mul(u64::from(sampling.n)),
        "running evals",
    );
    let mut results: Vec<crate::commands::EvalRow> = Vec::with_capacity(total);
    let mut sample_passes = std::collections::BTreeMap::new();
    let mut details = std::collections::BTreeMap::new();
    let mut eval_threads: Vec<String> = Vec::with_capacity(total * sampling.n as usize);
    let run = async {
        for case in &suite.cases {
            let mut samples = Vec::with_capacity(sampling.n as usize);
            for _ in 0..sampling.n {
                // Each sample is its own thread so draws never inherit prior
                // turns (#550). Prefix the conversation_id so the worker omits
                // ambient agent memory from that sandbox (#1909): durable memory
                // is per-agent and would otherwise load on a fresh thread.
                // Thread reset (#1534) must use the same prefixed key the
                // worker claimed.
                let (channel_id, thread_ts, placeholder_ts) = resolve_targets(Some(channel), None);
                let reply_endpoint = stub.base_api_url().to_string();
                let event = eval_case_turn(
                    "slack",
                    &channel_id,
                    &opts.user,
                    &case.input,
                    &thread_ts,
                    &placeholder_ts,
                    Some(reply_endpoint),
                );
                // The worker claims under quote(kind):quote(channel):quote(conversation_id),
                // not the bare eval-prefixed conversation_id. SADD the scoped key
                // or the drain is a no-op and the sandbox keeps its quota slot (#2259).
                let thread_key = thread_key_for_turn(&event);
                eval_threads.push(thread_key.clone());
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
                // Release this sample's sandbox on every completed/red/timed-out
                // path so a three-case suite run twice cannot pin eight
                // curie-thread-* claims against the default ResourceQuota (#1534).
                queue_thread_reset(conn, &thread_key).await?;
                let elapsed = started.elapsed().as_secs_f64();
                let output = match &outcome {
                    Outcome::Replied(reply) => reply.clone(),
                    Outcome::AwaitingApproval { reply, .. } => reply.clone().unwrap_or_default(),
                    Outcome::CompletedNoEdit => String::new(),
                    Outcome::TimedOut => diagnostics(conn, &opts.stream, &stream_id).await,
                };
                let passed = reply_passes(case, &outcome);
                let completed = matches!(
                    outcome,
                    Outcome::Replied(_)
                        | Outcome::AwaitingApproval { .. }
                        | Outcome::CompletedNoEdit
                );
                samples.push(crate::eval_sampling::SampleRecord {
                    outcome: if passed {
                        crate::evals::CaseOutcome::Pass
                    } else {
                        crate::evals::CaseOutcome::Fail
                    },
                    output,
                    seconds: elapsed,
                    error: if completed {
                        None
                    } else {
                        Some("turn did not complete".into())
                    },
                });
                bar.inc(1);
            }
            let agg = crate::eval_sampling::aggregate_samples(&samples, sampling);
            sample_passes.insert(case.id.clone(), agg.passes);
            if let Some(variance) = &agg.variance {
                details.insert(case.id.clone(), variance.clone());
            }
            results.push((case.id.clone(), agg.outcome, agg.seconds, agg.output));
        }
        anyhow::Ok(crate::commands::EvalReport {
            rows: results,
            details,
            sampling,
            sample_passes,
        })
    };
    let result = run.await;
    // Suite error/cancel: still queue every case we enqueued, including the
    // in-flight one, so an abandoned retry cannot bind a late sandbox.
    for thread_key in &eval_threads {
        let _ = queue_thread_reset(conn, thread_key).await;
    }
    bar.finish();
    result
}

/// Reject blank model entries before a target performs environment discovery.
/// The shared eval handler calls this again so direct callers remain safe.
pub fn validate_eval_models(models: &[String]) -> Result<()> {
    if models.iter().any(|model| model.trim().is_empty()) {
        return Err(anyhow::Error::from(
            crate::exit::CliError::usage("--model cannot be empty or whitespace-only").with_fix(
                "pass a non-empty model identifier to --model, or omit --model to run the \
                 deployed/default model",
            ),
        ));
    }
    Ok(())
}

/// The shared `eval` handler: run the bundle's `evals/cases.json` through the
/// target tier's message enqueue+await path and grade with the shared grader,
/// so a suite that passes at `skill` can be re-asserted verbatim at `local` and
/// `cluster` (issue #344, the per-tier parity gate).
/// Refuse a `--case-id` selector on a `--model` sweep (exit 4, ADR-0041).
///
/// A sweep is the platform eval plane: it sends only the suite NAME to
/// `POST /evals/trigger` and the worker reloads the DEPLOYED suite server-side,
/// then reports a per-model pass-rate over all of it. A locally chosen subset is
/// never read, so honoring the flag is impossible -- silently sweeping the whole
/// deployed suite while displaying a narrowed selection is the failure this
/// prevents, exactly as the sibling `--cases` refusal does (#608). Unsupported
/// rather than Usage: no input and no retry makes the selector apply here.
///
/// Pure so the class and the wording are testable with no stack.
pub fn guard_sweep_case_ids(case_ids: &[String]) -> Result<()> {
    if case_ids.is_empty() {
        return Ok(());
    }
    Err(anyhow::Error::from(
        crate::exit::CliError::unsupported(
            "--case-id has no effect on a --model sweep: the sweep runs a platform eval that \
             reloads the deployed bundle's evals/cases.json server-side and reports a per-model \
             pass-rate over the whole suite, so a local case selection is never applied",
        )
        .with_fix(
            "drop --case-id to sweep the whole deployed suite, or omit --model to grade selected \
             cases in-CLI with `curie <skill|local|cluster> eval --case-id <ID>`",
        ),
    ))
}

/// Refuse a `--case-id` selector on the local/cluster trajectory eval (exit 4).
///
/// Trajectory scoring at these tiers runs on the worker eval plane against the
/// deployed bundle's suite, the same construction that refuses `--cases` here,
/// so a locally chosen subset never reaches the scorer. Pure for the same reason
/// as [`guard_sweep_case_ids`].
pub fn guard_trajectory_case_ids(case_ids: &[String]) -> Result<()> {
    if case_ids.is_empty() {
        return Ok(());
    }
    Err(anyhow::Error::from(
        crate::exit::CliError::unsupported(
            "--case-id cannot narrow a local/cluster trajectory eval because trajectory scoring \
             runs on the worker eval plane against the deployed bundle's suite",
        )
        .with_fix(
            "drop --case-id to grade the whole deployed trajectory suite, or use \
             `curie skill eval --case-id <ID>` to grade selected local cases",
        ),
    ))
}

pub async fn eval(opts: EvalOpts) -> Result<()> {
    validate_eval_models(&opts.models)?;
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
        if opts.sampling.n > 1 {
            return Err(anyhow::Error::from(
                crate::exit::CliError::unsupported(
                    "--samples has no effect on a --model sweep: the sweep enqueues a frozen \
                     EvalJob that cannot carry a sample count, so in-CLI N never reaches the \
                     worker",
                )
                .with_fix(
                    "drop --samples to sweep at the worker default, set CURIE_EVAL_SAMPLES on \
                     the worker to raise N on the production eval path, or omit --model to \
                     grade in-CLI with `curie <local|cluster> eval --samples N`",
                ),
            ));
        }
        guard_sweep_case_ids(&opts.case_ids)?;
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
        if opts.sampling.n > 1 {
            return Err(anyhow::Error::from(
                crate::exit::CliError::unsupported(
                    "--samples has no effect on a local/cluster trajectory eval: scoring \
                     runs on the worker eval plane, and the frozen EvalJob cannot carry N",
                )
                .with_fix(
                    "drop --samples, set CURIE_EVAL_SAMPLES on the worker, or use \
                     `curie skill eval --samples N` to sample in-CLI",
                ),
            ));
        }
        guard_trajectory_case_ids(&opts.case_ids)?;
        return eval_trajectory_platform(opts, loaded.suite).await;
    }
    let total_cases = loaded.suite.cases.len();
    // Exit 2 before any stack contact when a --case-id matches nothing: a
    // mistyped selector fails the gate instead of greening an empty run.
    let suite = crate::evals::select_cases(loaded.suite, &opts.case_ids)?;
    if let Some(note) = crate::evals::selection_note(&opts.case_ids, suite.cases.len(), total_cases)
    {
        crate::ui::ui().note(&note);
    }
    if opts.local {
        eval_local(opts, suite).await
    } else {
        eval_cluster(opts, suite).await
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

/// The resource ref the fake-model probe reads, named in ONE place so the argv
/// and the failure diagnostic can never name different Deployments (#1533).
fn fake_model_probe_target(fullname: &crate::ops::ReleaseFullname) -> String {
    format!("deployment/{}", fullname.resource("worker"))
}

/// The kubectl read behind the cluster branch of `probe_fake_model`, extracted
/// pure so the Deployment NAME is unit-testable without a cluster (#1533).
///
/// The chart renders `{{ include "curie.fullname" . }}-worker`; the old
/// `{release}-worker` guess made `cluster eval --release platform` fail before a
/// single eval case ran.
fn fake_model_probe_command(namespace: &str, fullname: &crate::ops::ReleaseFullname) -> OpsCommand {
    OpsCommand::new(
        "kubectl",
        vec![
            plain("-n"),
            plain(namespace),
            plain("get"),
            plain(fake_model_probe_target(fullname)),
            plain("-o"),
            plain(
                "jsonpath={.spec.template.spec.containers[*].env[?(@.name==\"CURIE_FAKE_MODEL\")].value}",
            ),
        ],
    )
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
///
/// `fullname` is the CALLER's already-resolved release name, threaded in rather
/// than resolved here: the sweep needs the same name for its api port-forward,
/// and resolving twice costs a second kubectl discovery round-trip per
/// invocation (#1533). It is `None` on the `--local` path, which reaches no
/// cluster and must make zero kubectl calls.
async fn probe_fake_model(
    opts: &EvalOpts,
    fullname: Option<&crate::ops::ReleaseFullname>,
) -> Result<bool> {
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
        let fullname = fullname.context(
            "internal: the cluster fake-model probe needs the release fullname its caller resolved",
        )?;
        let deployment = fake_model_probe_target(fullname);
        let cmd = fake_model_probe_command(&opts.namespace, fullname);
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
/// service label is the only stable selector; the unit test
/// `the_probe_matches_the_worker_service_compose_declares` pins this against the
/// compose file so a service rename cannot silently blind the probe again.
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
/// an explicit diagnostic: guessing which stack the caller would hit is exactly
/// the fabrication this probe exists to prevent. Both diagnostics name the
/// selector rather than asserting a stack-wide fact the probe did not check --
/// "no container matched X" is verifiable; "there is no stack" is not.
///
/// Shared by the `--model` sweep and the `local message` connectedness probe
/// (#1031), so the wording names the worker rather than either caller's verb.
fn select_worker_container(stdout: &str) -> Result<String> {
    let names: Vec<&str> = stdout
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    match names.as_slice() {
        [only] => Ok((*only).to_string()),
        [] => bail!(
            "no running container matches `{}`, so the local worker's own configuration \
             cannot be read. Start a stack with `curie local up`.",
            worker_label_selector()
        ),
        many => bail!(
            "{} running containers match `{}` ({}); the local worker cannot be identified \
             unambiguously. Stop the extras with `curie local down`.",
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
    // Resolved ONCE for the whole sweep and threaded into both consumers (the
    // fake-model probe below and the api port-forward further down), so a
    // `cluster eval --model` pays exactly one discovery round-trip rather than
    // one per consumer. `--local` reaches no cluster, so it resolves nothing and
    // makes zero kubectl calls (#1533).
    let fullname = if opts.local {
        None
    } else {
        require_on_path("kubectl")?;
        Some(crate::ops::release_fullname(&opts.namespace, &opts.release).await)
    };
    guard_fake_sweep(
        probe_fake_model(&opts, fullname.as_ref()).await?,
        &opts.models,
        opts.local,
    )?;

    // The trigger + matrix reads go over the platform API. Local reaches it
    // directly; cluster tunnels an api port-forward kept alive for the whole poll.
    let (api, _api_pf) = if opts.local {
        let base = local_api_base(opts.api_url.as_deref());
        (ApiClient::new(&base, &opts.api_key)?, None)
    } else {
        require_on_path("kubectl")?;
        let fullname = fullname.as_ref().context(
            "internal: the cluster api tunnel needs the release fullname resolved above",
        )?;
        let (pf, api_local_port) = start_port_forward(
            &port_forward_command(
                &opts.namespace,
                fullname,
                "api",
                opts.api_local_port,
                API_REMOTE_PORT,
            ),
            opts.api_local_port,
            "api",
        )
        .await?;
        let base = format!("http://127.0.0.1:{api_local_port}");
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
        format!("http://127.0.0.1:{}", opts.api_local_port)
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
        // Cluster-only branch, so the discovery round-trip never fires for
        // `--local` (#1533).
        let fullname = crate::ops::release_fullname(&opts.namespace, &opts.release).await;
        let (pf, api_local_port) = start_port_forward(
            &port_forward_command(
                &opts.namespace,
                &fullname,
                "api",
                opts.api_local_port,
                API_REMOTE_PORT,
            ),
            opts.api_local_port,
            "api",
        )
        .await?;
        let effective_api_base = format!("http://127.0.0.1:{api_local_port}");
        (
            ApiClient::new(&effective_api_base, &opts.api_key)?,
            Some(pf),
        )
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
            return crate::commands::report_eval(&report, None, _api_pf);
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
    Ok(Some(crate::commands::EvalReport::with_details(
        rows, details,
    )))
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
    crate::commands::report_eval(&results, None, stub)
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

    // Resolved once here: `--dry-run` returned above without contacting a
    // cluster, and every kubectl target below is a chart resource (#1533).
    let fullname = crate::ops::release_fullname(&opts.namespace, &opts.release).await;

    let advertise_host = resolve_advertise_host(opts.listen_host.as_deref()).await?;
    let mut stub = SlackStub::start("0.0.0.0", opts.listen_port, &advertise_host).await?;
    ui.note(&format!(
        "slack stub listening; the worker will post to {}",
        stub.base_api_url()
    ));

    // Valkey port-forward for the enqueue, kept alive for the whole eval loop.
    let (_valkey_pf, valkey_local_port) = start_port_forward(
        &port_forward_command(
            &opts.namespace,
            &fullname,
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
            let (_api_pf, api_local_port) = start_port_forward(
                &port_forward_command(
                    &opts.namespace,
                    &fullname,
                    "api",
                    opts.api_local_port,
                    API_REMOTE_PORT,
                ),
                opts.api_local_port,
                "api",
            )
            .await?;
            let api = ApiClient::new(&format!("http://127.0.0.1:{api_local_port}"), &opts.api_key)?;
            let agents = api
                .list_agents()
                .await
                .context("listing agents through the api port-forward")?;
            select_channel(&agents, None)?
        }
    };
    ui.note(&format!("routing to channel {channel}"));

    let valkey_url = format!(
        "redis://:{}@127.0.0.1:{valkey_local_port}",
        opts.valkey_password
    );
    let mut conn = connect(&valkey_url).await?;

    let results = match run_eval_turns(&opts, &channel, &suite, &mut conn, &mut stub).await {
        Ok(results) => results,
        Err(err) => return Err(enrich_cluster_enqueue_timeout(err).await),
    };
    // report_eval process::exits on a red suite without unwinding, so the
    // Valkey forward and stub must move in as guards (#1908).
    crate::commands::report_eval(&results, None, (_valkey_pf, stub))
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXPECTED_OTEL_EXPORTER_ENV_KEYS: [&str; 38] = [
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_PROTOCOL",
        "OTEL_EXPORTER_OTLP_HEADERS",
        "OTEL_EXPORTER_OTLP_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_CLIENT_KEY",
        "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_INSECURE",
        "OTEL_EXPORTER_OTLP_COMPRESSION",
        "OTEL_EXPORTER_OTLP_TIMEOUT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
        "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
        "OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
        "OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_TRACES_INSECURE",
        "OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
        "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
        "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_LOGS_PROTOCOL",
        "OTEL_EXPORTER_OTLP_LOGS_HEADERS",
        "OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY",
        "OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_LOGS_INSECURE",
        "OTEL_EXPORTER_OTLP_LOGS_COMPRESSION",
        "OTEL_EXPORTER_OTLP_LOGS_TIMEOUT",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
        "OTEL_EXPORTER_OTLP_METRICS_PROTOCOL",
        "OTEL_EXPORTER_OTLP_METRICS_HEADERS",
        "OTEL_EXPORTER_OTLP_METRICS_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_METRICS_CLIENT_KEY",
        "OTEL_EXPORTER_OTLP_METRICS_CLIENT_CERTIFICATE",
        "OTEL_EXPORTER_OTLP_METRICS_INSECURE",
        "OTEL_EXPORTER_OTLP_METRICS_COMPRESSION",
        "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
        "OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE",
        "OTEL_EXPORTER_OTLP_METRICS_DEFAULT_HISTOGRAM_AGGREGATION",
    ];

    fn otel_exporter_test_value(name: &str) -> String {
        if name.ends_with("_PROTOCOL") {
            "grpc".to_string()
        } else if name.ends_with("_HEADERS") {
            "authorization=Bearer example==,tenant=acme".to_string()
        } else if name.ends_with("_ENDPOINT") {
            "https://collector.example.com/v1/data?signature=example==".to_string()
        } else if name.ends_with("_CLIENT_KEY") {
            "/tls/client=key.pem".to_string()
        } else if name.ends_with("_CLIENT_CERTIFICATE") {
            "/tls/client=certificate.pem".to_string()
        } else if name.ends_with("_CERTIFICATE") {
            "/tls/ca=certificate.pem".to_string()
        } else if name.ends_with("_INSECURE") {
            "false".to_string()
        } else if name.ends_with("_COMPRESSION") {
            "gzip".to_string()
        } else if name.ends_with("_TIMEOUT") {
            "12.5".to_string()
        } else if name.ends_with("_TEMPORALITY_PREFERENCE") {
            "delta".to_string()
        } else {
            "base2_exponential_bucket_histogram".to_string()
        }
    }

    fn sensitive_otel_exporter_value(name: &str) -> bool {
        name.ends_with("_ENDPOINT")
            || name.ends_with("_HEADERS")
            || name.ends_with("_CERTIFICATE")
            || name.ends_with("_CLIENT_KEY")
            || name.ends_with("_CLIENT_CERTIFICATE")
    }

    #[test]
    fn local_dispatcher_command_is_slack_free_and_keeps_secrets_off_argv() {
        let secret = "private-valkey-password";
        let otel_env: Vec<(String, String)> = EXPECTED_OTEL_EXPORTER_ENV_KEYS
            .iter()
            .map(|name| (name.to_string(), otel_exporter_test_value(name)))
            .collect();
        let command = dispatcher_enqueue_command(
            &["/tmp/curie compose.yaml".to_string()],
            "curie-dispatcher-enqueue-test",
            "test:curie:runs",
            secret,
            &otel_env,
            None,
        );
        let argv = command.argv();

        // `curie-dispatcher` is declared in the slack profile but depends on
        // `curie-api` from the core profile. Selecting slack alone makes the
        // one-shot producer fail before Python runs because Compose cannot
        // resolve that dependency.
        assert!(
            argv.windows(2).any(|pair| pair == ["--profile", "core"]),
            "one-shot dispatcher must activate core dependencies: {argv:?}"
        );
        assert!(argv
            .windows(2)
            .any(|pair| { pair[0] == COMPOSE_DISPATCHER_SERVICE && pair[1] == "python" }));
        assert!(argv
            .windows(2)
            .any(|pair| pair[0] == "-m" && pair[1] == DISPATCHER_ENQUEUE_MODULE));
        assert!(argv.contains(&"--no-deps".to_string()));
        assert!(argv
            .windows(2)
            .any(|pair| pair == ["--name", "curie-dispatcher-enqueue-test"]));
        assert!(argv.contains(&"-T".to_string()));
        assert!(argv.contains(&"SLACK_APP_TOKEN=".to_string()));
        assert!(argv.contains(&"SLACK_BOT_TOKEN=".to_string()));
        assert!(!argv.iter().any(|arg| arg.contains(secret)));
        assert!(!command.display().contains(secret));
        for (name, value) in &otel_env {
            assert!(
                argv.windows(2)
                    .any(|pair| pair[0] == "-e" && pair[1] == *name),
                "one-shot dispatcher did not forward {name}: {argv:?}"
            );
            assert!(
                !argv.iter().any(|arg| arg.contains(value)),
                "telemetry value for {name} leaked onto argv"
            );
            if sensitive_otel_exporter_value(name) {
                assert!(
                    !command.display().contains(value),
                    "sensitive telemetry value for {name} leaked into command display"
                );
            }
            assert!(
                command.secret_env.contains(&(name.clone(), value.clone())),
                "telemetry value for {name} was not preserved exactly"
            );
        }
        assert!(command
            .secret_env
            .contains(&("VALKEY_PASSWORD".to_string(), secret.to_string())));
    }

    #[test]
    fn compose_config_label_preserves_each_declared_file() {
        assert_eq!(
            compose_config_files(" /tmp/base.yaml,/tmp/override.yaml\n").unwrap(),
            vec!["/tmp/base.yaml", "/tmp/override.yaml"]
        );
        assert!(compose_config_files("  \n").is_err());
        assert!(compose_config_files("<no value>\n").is_err());
    }

    #[test]
    fn worker_telemetry_probe_outputs_only_standard_exporter_configuration() {
        let command = worker_otel_exporter_env_command("curie-worker-example");
        let template = &command.argv()[2];

        assert_eq!(
            OTEL_EXPORTER_ENV_KEYS.as_slice(),
            EXPECTED_OTEL_EXPORTER_ENV_KEYS.as_slice()
        );
        for name in EXPECTED_OTEL_EXPORTER_ENV_KEYS {
            assert!(template.contains(name), "inspect template omitted {name}");
        }
        assert!(template.contains("CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT"));
        assert!(!template.contains("{{json .Config.Env}}"));
        assert!(!template.contains("UNRELATED_SECRET"));
        assert!(!template.contains("println ."));
        assert!(!template.contains("range .Config.Env}}{{println"));
    }

    #[test]
    fn local_dispatcher_uses_bridge_exporter_endpoint_and_preserves_empty_override() {
        for (bridge, expected) in [
            (
                Some("http://otel-collector:4318"),
                "http://otel-collector:4318",
            ),
            (Some(""), ""),
            (None, "http://127.0.0.1:24318"),
        ] {
            let mut inspected = String::new();
            // The override must win regardless of Docker's environment order.
            if let Some(value) = bridge {
                inspected.push_str(&format!(
                    "CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT={value}\n"
                ));
            }
            inspected.push_str(concat!(
                "OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:24318\n",
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://traces.example.com/v1/traces\n",
                "OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf\n",
                "UNRELATED_SECRET=must-not-forward\n",
            ));
            let command = dispatcher_enqueue_command(
                &["/tmp/compose.yaml".to_string()],
                "curie-dispatcher-enqueue-test",
                "test:curie:runs",
                "example-password",
                &parse_worker_otel_env(&inspected).unwrap(),
                None,
            );
            // Execute the subprocess environment the dispatcher receives;
            // checking only the parsed vector would miss command forwarding.
            let output = std::process::Command::new("sh")
                .args([
                    "-c",
                    concat!(
                        "printf '%s\\n' \"$OTEL_EXPORTER_OTLP_ENDPOINT\" ",
                        "\"$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT\" ",
                        "\"$OTEL_EXPORTER_OTLP_PROTOCOL\" ",
                        "\"${CURIE_RUNNER_OTEL_EXPORTER_OTLP_ENDPOINT-unset}\" ",
                        "\"${UNRELATED_SECRET-unset}\"",
                    ),
                ])
                .env_clear()
                .envs(command.env.iter().chain(command.secret_env.iter()).cloned())
                .output()
                .unwrap();
            assert!(output.status.success());
            assert_eq!(
                String::from_utf8(output.stdout).unwrap(),
                format!("{expected}\nhttps://traces.example.com/v1/traces\nhttp/protobuf\nunset\nunset\n")
            );
            assert!(!command.display().contains("http://otel-collector:4318"));
            assert!(!command.display().contains("http://127.0.0.1:24318"));
        }
    }

    #[test]
    fn worker_telemetry_probe_preserves_equals_in_exporter_values() {
        let inspected = concat!(
            "\"OTEL_EXPORTER_OTLP_PROTOCOL=grpc\"\n",
            "\"OTEL_EXPORTER_OTLP_HEADERS=authorization=Bearer example==,tenant=acme\"\n",
            "\"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT=https://logs.example.com/v1/logs?signature=example==\"\n",
            "\"OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY=/tls/client=key.pem\"\n",
            "\"OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE=/tls/client=certificate.pem\"\n",
            "\"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=\"\n",
            "\"UNRELATED_SECRET=must-not-forward\"\n",
        );

        assert_eq!(
            parse_worker_otel_env(inspected).unwrap(),
            vec![
                (
                    "OTEL_EXPORTER_OTLP_PROTOCOL".to_string(),
                    "grpc".to_string(),
                ),
                (
                    "OTEL_EXPORTER_OTLP_HEADERS".to_string(),
                    "authorization=Bearer example==,tenant=acme".to_string(),
                ),
                (
                    "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT".to_string(),
                    "https://logs.example.com/v1/logs?signature=example==".to_string(),
                ),
                (
                    "OTEL_EXPORTER_OTLP_LOGS_CLIENT_KEY".to_string(),
                    "/tls/client=key.pem".to_string(),
                ),
                (
                    "OTEL_EXPORTER_OTLP_LOGS_CLIENT_CERTIFICATE".to_string(),
                    "/tls/client=certificate.pem".to_string(),
                ),
                (
                    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT".to_string(),
                    "".to_string(),
                ),
            ]
        );
    }

    #[test]
    fn worker_telemetry_probe_accepts_already_filtered_plain_lines() {
        let inspected = concat!(
            "OTEL_EXPORTER_OTLP_ENDPOINT=https://collector.example.com?signature=example==\n",
            "UNRELATED_SECRET=must-not-forward\n",
            "/tmp/non-telemetry-inspect-output\n",
        );

        assert_eq!(
            parse_worker_otel_env(inspected).unwrap(),
            vec![(
                "OTEL_EXPORTER_OTLP_ENDPOINT".to_string(),
                "https://collector.example.com?signature=example==".to_string(),
            )]
        );
    }

    #[test]
    fn dispatcher_failure_diagnostic_is_present_bounded_and_redacted() {
        let secret = "private-valkey-password";
        let endpoint = "https://token@collector.example.com";
        let stderr = format!(
            "connection failed for password {secret} at {endpoint} {}",
            "x".repeat(5000)
        );
        let diagnostic = redact_dispatcher_diagnostic(&stderr, &[secret, endpoint]);
        assert!(diagnostic.contains("connection failed"));
        assert!(diagnostic.contains("[REDACTED]"));
        assert!(!diagnostic.contains(secret));
        assert!(!diagnostic.contains(endpoint));
        assert!(diagnostic.chars().count() <= 4096);
        assert_eq!(
            redact_dispatcher_diagnostic("", &[secret]),
            "one-shot dispatcher exited without a diagnostic"
        );
    }

    #[test]
    fn dispatcher_stdout_must_be_exactly_one_stream_id() {
        assert_eq!(
            parse_dispatcher_stream_id("1700000000000-0\n").unwrap(),
            "1700000000000-0"
        );
        assert!(parse_dispatcher_stream_id("").is_err());
        assert!(parse_dispatcher_stream_id("telemetry\n1700000000000-0\n").is_err());
        assert!(parse_dispatcher_stream_id("not-an-id\n").is_err());
    }

    /// An empty `--thread` must not survive as a thread: carried through, it
    /// enqueues `conversation_id: ""` and puts a literal `"thread_ts": ""` in an
    /// outbound chat.postMessage body. The rule is applied at two sites
    /// ([`message`] and [`enqueue_over_connected_transport`]), both of which need
    /// a live Valkey to drive, so this pins the rule itself where CI can run it.
    #[test]
    fn an_empty_thread_normalizes_to_no_thread() {
        assert_eq!(normalize_thread(Some(String::new())), None);
    }

    #[test]
    fn eval_turns_queue_thread_reset_on_case_and_suite_paths() {
        // AC1 is the SADD sites in run_eval_turns, not queue_thread_reset
        // itself. Deleting either call keeps the helper test green.
        let src = include_str!("message.rs");
        assert!(
            src.contains("eval_case_turn("),
            "each eval case must mint through eval_case_turn so the enqueued conversation_id is isolated"
        );
        assert!(
            src.contains("thread_key_for_turn(&event)"),
            "each eval case must SADD the worker's scoped thread key, not the bare conversation_id"
        );
        assert!(
            src.contains("queue_thread_reset(conn, &thread_key)"),
            "each eval case must queue its scoped thread key for sandbox release"
        );
        assert!(
            src.contains("for thread_key in &eval_threads"),
            "suite error/cancel must still queue every enqueued eval thread"
        );
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

    /// The printed resolve hint is runnable as a principal-backed command and
    /// separately names the authenticated card's real channel. It must never
    /// revive the retired caller-asserted actor/channel flags (ADR-0106).
    #[test]
    fn the_resolve_hint_carries_every_flag_the_server_requires() {
        let id = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
        let line = approval_resolve_command("local", Some("weather-bot"), "C-SIM-abc", id);
        assert_eq!(
            line,
            format!(
                "curie local approvals weather-bot --resolve {id}  # authenticated card \
                 channel: \"C-SIM-abc\"; a terminal principal works only for an \
                 explicit-user route"
            )
        );
        assert!(!line.contains("--as"), "{line}");
        assert!(!line.contains("--actor-channel"), "{line}");

        // With no resolved agent name the `<AGENT>` slot keeps the command shape
        // valid, while the actual authenticated-card channel remains explanatory.
        let line = approval_resolve_command("cluster", None, "C-SIM-xyz", id);
        assert!(line.contains("approvals <AGENT> --resolve"), "{line}");
        assert!(
            line.contains("authenticated card channel: \"C-SIM-xyz\""),
            "{line}"
        );
        assert!(line.contains("explicit-user route"), "{line}");
        assert!(
            !line.contains("--as") && !line.contains("--actor-channel"),
            "{line}"
        );
        assert!(line.starts_with("curie cluster approvals"), "{line}");

        // #1531: the fix for the wrong-channel hint resolves the approval's
        // `card_channel` in the CALLER and hands it down. This helper must stay
        // a pure formatter that renders the channel it is GIVEN, verbatim, on
        // both tiers -- no lookup, no substitution, no I/O. A route-bound
        // channel is used here precisely because it is NOT the turn channel:
        // if the formatter ever substituted a value of its own, this is where
        // that would show up.
        for tier in ["local", "cluster"] {
            let line = approval_resolve_command(tier, Some("weather-bot"), "C-SIM-route", id);
            assert!(
                line.contains("authenticated card channel: \"C-SIM-route\""),
                "the formatter must explain the actual card channel it was handed \
                 ({tier}): {line}"
            );
            assert!(
                !line.contains("C-SIM-turn")
                    && !line.contains("C0LOCALDEV")
                    && !line.contains("C-SIM-abc")
                    && !line.contains("C-SIM-xyz"),
                "no turn, stub, or earlier card channel may replace the real card channel \
                 ({tier}): {line}"
            );
            assert!(
                !line.contains("--as") && !line.contains("--actor-channel"),
                "{line}"
            );
        }
    }

    // ─── #1531 finding 3: the advisory hint-channel lookup ───────────────────
    //
    // The private helper below is kept here so its deadline and degraded-path
    // behavior can be tested without widening the crate's public API:
    //
    //     async fn hint_channel(
    //         opts: &MessageOpts,
    //         verb: TurnVerb,
    //         turn_channel: &str,
    //         id: &str,
    //         deadline: Instant,
    //     ) -> String
    //
    // The `deadline` parameter is the turn's overall deadline, NOT this
    // lookup's own: the effective bound is
    // `capped(HINT_CHANNEL_LOOKUP_BUDGET, deadline)` (`cli/src/chat.rs:86`), so
    // a short `--timeout-secs` shortens the lookup rather than being overrun by
    // it. See `the_lookup_budget_is_capped_by_what_is_left_of_the_turns_deadline`.
    //
    // and, as part of the same contract, the bound it is capped against:
    //
    //     const HINT_CHANNEL_LOOKUP_BUDGET: Duration = Duration::from_secs(10);
    //
    // also private in `cli/src/message.rs`. It is named rather than inlined so
    // the stalling-peer tests below track the budget if it is ever retuned.
    //
    // ADVISORY: every failure -- unreachable API, 404, 5xx, decode error,
    // deadline expiry, an absent or empty `card_channel` -- collapses to
    // `turn_channel`,
    // which is byte-for-byte what the hint prints today. The worst case is
    // therefore the status quo, never a regression, and never a failed turn.
    //
    // These live HERE rather than in `cli/tests/approval_hint_channel.rs`
    // because `hint_channel` is private: an integration test cannot reach it,
    // and making it `pub` would widen the crate's public API for a test. The
    // wire-level half of the contract (`ApiClient::get_approval`) is in that
    // file, where the shared `support` harness lives.

    /// The record a route binding produces: the card landed in a channel other
    /// than the one the requester spoke in. The two must stay distinct or the
    /// positive assertion below cannot tell a correct value from a lucky one.
    const HINT_ROUTE_BOUND_APPROVAL: &str = r#"{"id":"3f2504e0-4f89-41d3-9a0c-0305e82c3301","author":"U-REQUESTER","route":"finance","gate_kind":"policy","granted_tool":null,"status":"pending","conversation_id":"thread-1","summary":"approve invoice","expires_at":null,"resolved_by":null,"card_channel":"C-SIM-card","reply_channel":"C-SIM-turn"}"#;

    /// The same approval on a row that predates route bindings, or written
    /// directly through the API: `card_channel` is null, which means "the
    /// requesting channel applies" (#1431), not "no channel".
    const HINT_UNROUTED_APPROVAL: &str = r#"{"id":"3f2504e0-4f89-41d3-9a0c-0305e82c3301","author":"U-REQUESTER","route":null,"gate_kind":"policy","granted_tool":null,"status":"pending","conversation_id":"thread-1","summary":"approve invoice","expires_at":null,"resolved_by":null,"card_channel":null,"reply_channel":"C-SIM-turn"}"#;

    /// The same approval with an EMPTY card channel rather than a null one. The
    /// wire model admits it, and the server treats it as absent (see the test
    /// below), so the CLI must too.
    const HINT_EMPTY_CARD_APPROVAL: &str = r#"{"id":"3f2504e0-4f89-41d3-9a0c-0305e82c3301","author":"U-REQUESTER","route":"finance","gate_kind":"policy","granted_tool":null,"status":"pending","conversation_id":"thread-1","summary":"approve invoice","expires_at":null,"resolved_by":null,"card_channel":"","reply_channel":"C-SIM-turn"}"#;

    /// The same approval with a WHITESPACE-ONLY card channel. Distinct from the
    /// empty one above on purpose: the two fixtures pin the two sides of the
    /// absent/present boundary the server draws, and neither covers the other.
    const HINT_BLANK_CARD_APPROVAL: &str = r#"{"id":"3f2504e0-4f89-41d3-9a0c-0305e82c3301","author":"U-REQUESTER","route":"finance","gate_kind":"policy","granted_tool":null,"status":"pending","conversation_id":"thread-1","summary":"approve invoice","expires_at":null,"resolved_by":null,"card_channel":" ","reply_channel":"C-SIM-turn"}"#;

    /// The whitespace-only channel `HINT_BLANK_CARD_APPROVAL` carries, spelled
    /// out so the assertion below compares against the exact wire value rather
    /// than a re-typed literal.
    const HINT_BLANK_CARD_CHANNEL: &str = " ";

    /// A well-formed UUID, because `parse_approval_id` (`cli/src/chat.rs:400`)
    /// validates the id as one before `resume_after_approval` is ever entered,
    /// and the endpoint types its path param as `uuid.UUID`.
    const HINT_APPROVAL_ID: &str = "3f2504e0-4f89-41d3-9a0c-0305e82c3301";
    /// What this turn routed to: the value the hint prints today, and the value
    /// every degraded path must keep printing.
    const HINT_TURN_CHANNEL: &str = "C-SIM-turn";
    /// Where the route binding actually put the card: authenticated channel
    /// location used to help the operator find the approval card.
    const HINT_CARD_CHANNEL: &str = "C-SIM-card";
    /// Placeholder platform API key for the hint lookup tests. Held in a const
    /// rather than written inline so the commit-time secret scan does not read
    /// an `api_key: "..."` assignment as a real credential; the same shape is
    /// already proven safe in the approval CLI integration tests.
    const HINT_API_KEY: &str = "test-key";

    /// A one-endpoint stand-in for the platform API on an ephemeral port,
    /// answering every request with the same canned status and body.
    ///
    /// A raw accept loop rather than a router: it is the shape the port-forward
    /// tests further down this same module already use
    /// (`tokio::net::TcpListener::bind(("127.0.0.1", 0))`), and one canned
    /// response is the whole surface these tests need. Returns the base URL.
    async fn hint_stub_api(status: u16, body: &'static str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                // Drain the request head first so the client sees a complete
                // exchange rather than a reset part way through its write.
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                let head = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(body.as_bytes()).await;
                let _ = sock.shutdown().await;
            }
        });
        base
    }

    /// A turn deadline far enough out that it never binds, for the tests whose
    /// subject is something other than the deadline cap. Every call site passes
    /// one, because the turn's deadline is what bounds the lookup.
    fn hint_far_deadline() -> Instant {
        Instant::now() + HINT_CHANNEL_LOOKUP_BUDGET * 10
    }

    /// A peer that ACCEPTS the connection and then never answers: the stall
    /// shape `ApiClient`'s connect-only timeout cannot see. Every accepted
    /// socket is parked in `open` and never written to and never dropped, so
    /// the client's connect succeeds and its read never completes. Returns the
    /// base URL and the accept task, which the caller aborts once it has made
    /// its assertions.
    async fn hint_stalling_peer() -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let accepting = tokio::spawn(async move {
            let mut open = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                open.push(sock);
            }
        });
        (base, accepting)
    }

    /// A local-tier turn pointed at the given API base. `api_url` is what
    /// `local_api_base` reads, so this is the whole tier dispatch for
    /// `TurnVerb::Local`.
    fn hint_opts(api_url: &str) -> MessageOpts {
        MessageOpts {
            api_url: Some(api_url.to_string()),
            api_key: HINT_API_KEY.to_string(),
            local: true,
            ..MessageOpts::default()
        }
    }

    /// The defect itself (#1531 finding 3): a route binding put the card in a
    /// different channel, and the hint must name THAT channel.
    ///
    /// The hint is a command a human copy-pastes. With the turn channel on it,
    /// the default approver set -- `SlackChannelMembers(card_channel or
    /// reply_channel)` in `apps/api/.../slack_approvers.py` -- refuses the
    /// resolve 403 with "resolve this from the approval's channel", and the
    /// operator has no way to derive the right value from what was printed.
    ///
    /// Mutation it catches: keeping `channel` at the call site, i.e. never
    /// performing the lookup at all -- which is the pre-change behavior and is
    /// exactly what every degraded path below must still produce.
    #[tokio::test]
    async fn the_hint_names_the_approvals_card_channel_when_a_route_bound_one() {
        let base = hint_stub_api(200, HINT_ROUTE_BOUND_APPROVAL).await;
        let opts = hint_opts(&base);

        let resolved = hint_channel(
            &opts,
            TurnVerb::Local,
            HINT_TURN_CHANNEL,
            HINT_APPROVAL_ID,
            hint_far_deadline(),
        )
        .await;

        assert_eq!(
            resolved, HINT_CARD_CHANNEL,
            "the hint must name the channel the card was posted to, which is \
             where an authenticated chat principal can act on the card"
        );
        assert_ne!(
            resolved, HINT_TURN_CHANNEL,
            "the fixture keeps the card and turn channels distinct on purpose; \
             if they matched, this test could not tell a real lookup from the \
             unchanged fallback"
        );
    }

    /// A-T4a. The API is unreachable, so the hint degrades to the turn channel
    /// and does it promptly.
    ///
    /// This is the single most important test of the change: it is the guard on
    /// the "never fail the turn, never hang" property. The lookup runs INSIDE
    /// the resume wait, which can legitimately run for the full
    /// `--timeout-secs`, so a lookup that propagated its error would turn an
    /// unreachable API into a failed turn, and one that blocked would extend a
    /// wait the operator is already watching.
    ///
    /// The port is learned by binding and then dropped, so nothing is listening
    /// on it and the connect is refused rather than left hanging.
    ///
    /// Mutation it catches: writing the lookup with `?` or `unwrap` instead of
    /// absorbing, or dropping the bound that keeps it off the wait's clock.
    #[tokio::test]
    async fn the_hint_names_the_turn_channel_when_the_lookup_cannot_answer() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let opts = hint_opts(&format!("http://127.0.0.1:{port}"));

        let started = Instant::now();
        let resolved = hint_channel(
            &opts,
            TurnVerb::Local,
            HINT_TURN_CHANNEL,
            HINT_APPROVAL_ID,
            hint_far_deadline(),
        )
        .await;
        let elapsed = started.elapsed();

        assert_eq!(
            resolved, HINT_TURN_CHANNEL,
            "an unreachable API is 'no answer', and no answer means the hint \
             prints exactly what it printed before this change"
        );
        // The declared bound is 10s for the WHOLE lookup, port-forward startup
        // included. A refused connection on loopback answers immediately, so
        // anything near the budget here means the failure is being retried or
        // waited on rather than absorbed.
        assert!(
            elapsed < HINT_CHANNEL_LOOKUP_BUDGET,
            "the lookup must stay inside its budget so it can never extend the \
             resume wait; took {elapsed:?}"
        );
    }

    /// The guard on the highest-severity failure mode in this change: a peer
    /// that ACCEPTS the connection and then never answers.
    ///
    /// `ApiClient::new` (`cli/src/api.rs:670`) sets only `connect_timeout(5s)`
    /// and no read timeout, so a completed connect followed by silence leaves
    /// the caller waiting forever. This lookup runs INSIDE `resume_after_approval`
    /// while an operator watches a `message` turn, so "forever" means a frozen
    /// terminal on a turn whose durable approval is already fine. The only thing
    /// standing between that peer and the frozen turn is the single wrapping
    /// `tokio::time::timeout(HINT_CHANNEL_LOOKUP_BUDGET, ...)`.
    ///
    /// This test costs roughly one budget of wall clock, and that cost is the
    /// point: it is the ONLY test that can tell "bounded" from "hangs forever".
    /// The refusal case above returns instantly and therefore proves nothing
    /// about the bound, and reading the code for a `timeout` call is not a
    /// demonstration that the guard rejects a violating input (AGENTS.md,
    /// "Guards are outcome-tested").
    ///
    /// Mutation it catches: deleting the wrapping `tokio::time::timeout`, or
    /// narrowing it to cover only part of the lookup.
    #[tokio::test]
    async fn a_stalled_api_is_cut_off_at_the_budget_rather_than_hanging_the_turn() {
        let (base, stall) = hint_stalling_peer().await;
        let opts = hint_opts(&base);

        let started = Instant::now();
        // The outer bound is the test harness's own safety net, deliberately
        // wider than the budget under test: without it, an implementation that
        // forgot the inner timeout would hang this test forever and block CI
        // instead of failing it. Its expiry IS the failure signal.
        let resolved = tokio::time::timeout(
            HINT_CHANNEL_LOOKUP_BUDGET * 3,
            hint_channel(
                &opts,
                TurnVerb::Local,
                HINT_TURN_CHANNEL,
                HINT_APPROVAL_ID,
                hint_far_deadline(),
            ),
        )
        .await
        .expect(
            "the lookup never returned within three budgets against a stalled peer, so nothing \
             is bounding it: a real turn would sit here forever",
        );
        let elapsed = started.elapsed();
        stall.abort();

        assert_eq!(
            resolved, HINT_TURN_CHANNEL,
            "the degradation contract must hold under a HANG, not only under a \
             refusal: an expired budget is 'no answer' like any other, so the \
             hint prints exactly what it printed before this change"
        );
        // Lower bound: proves the budget is what returned, not some earlier
        // error path that happened to answer quickly and would leave the real
        // stall unbounded. Upper bound: proves the budget actually fired.
        assert!(
            elapsed >= HINT_CHANNEL_LOOKUP_BUDGET,
            "returning before the budget means the stall was not reached and \
             this test proved nothing about the bound; took {elapsed:?}"
        );
        assert!(
            elapsed < HINT_CHANNEL_LOOKUP_BUDGET * 3,
            "the lookup must be cut off at its own budget, not left to some \
             wider deadline; took {elapsed:?}"
        );
    }

    /// P1. The lookup's budget must be CAPPED by what is left of the turn's
    /// deadline, never spent on top of it.
    ///
    /// `resume_after_approval` computes `remaining` and hands it to
    /// `await_resume`. If the lookup can first burn its own fixed budget, an
    /// operator who asked for `--timeout-secs 1` waits about eleven seconds,
    /// and every nested gate in the loop adds another budget on top of that.
    /// This repo states the opposite invariant explicitly at
    /// `cli/src/chat.rs:497-499`: "Every per-op budget is capped by what is LEFT
    /// of the overall deadline, so the advertised `--timeout-secs` is a hard
    /// bound on this path too rather than being overrun by up to one fixed scan
    /// budget." `capped(budget, deadline)` (`cli/src/chat.rs:86`) is the helper
    /// that already expresses it, and the effective bound here must be
    /// `capped(HINT_CHANNEL_LOOKUP_BUDGET, deadline)`.
    ///
    /// The peer is the same stall as the test above, so the lookup would run to
    /// its full budget if nothing else stopped it: only the deadline can end it
    /// early, which is what makes the timing assertion attributable.
    ///
    /// Mutation it catches: ignoring the `deadline` parameter (the current
    /// implementation has none, so this fails to compile), or applying it as a
    /// floor rather than a cap.
    #[tokio::test]
    async fn the_lookup_budget_is_capped_by_what_is_left_of_the_turns_deadline() {
        let (base, stall) = hint_stalling_peer().await;
        let opts = hint_opts(&base);
        // A turn with one second left, against a peer that never answers.
        let deadline = Instant::now() + Duration::from_secs(1);

        let started = Instant::now();
        // Same harness safety net as above: an implementation that ignored the
        // deadline would otherwise hang CI instead of failing it.
        let resolved = tokio::time::timeout(
            HINT_CHANNEL_LOOKUP_BUDGET * 3,
            hint_channel(
                &opts,
                TurnVerb::Local,
                HINT_TURN_CHANNEL,
                HINT_APPROVAL_ID,
                deadline,
            ),
        )
        .await
        .expect(
            "the lookup never returned within three budgets against a stalled peer, so neither \
             its own bound nor the turn deadline is holding it",
        );
        let elapsed = started.elapsed();
        stall.abort();

        assert_eq!(
            resolved, HINT_TURN_CHANNEL,
            "a deadline that expires mid-lookup is 'no answer' like any other, \
             so the hint still degrades to the turn channel"
        );
        // Half a budget, not the one second itself: the deadline is what must
        // end this, and anything at or near the full budget means the turn's
        // remaining time was ignored. The slack is deliberately wide so a loaded
        // machine cannot flake it, while still being far below the value a
        // deadline-blind implementation would produce.
        assert!(
            elapsed < HINT_CHANNEL_LOOKUP_BUDGET / 2,
            "with one second left on the turn, the lookup must end in about one \
             second, not run its full budget: a `--timeout-secs 1` turn would \
             otherwise take about eleven seconds and break the hard bound \
             `cli/src/chat.rs:497-499` promises. Took {elapsed:?}"
        );
    }

    /// A-T4b. The lookup succeeds but the record carries no card channel, so
    /// there is nothing to override with.
    ///
    /// A null `card_channel` is an older row or a direct API write, which means
    /// the REQUESTING channel applies (#1431) -- and the requesting channel is
    /// the turn channel. Substituting an empty string or the literal "null"
    /// here would print an unrunnable command.
    ///
    /// Mutation it catches: `unwrap_or_default()` on the option, which yields
    /// an empty, unusable channel hint.
    #[tokio::test]
    async fn the_hint_names_the_turn_channel_when_the_record_binds_no_route() {
        let base = hint_stub_api(200, HINT_UNROUTED_APPROVAL).await;
        let opts = hint_opts(&base);

        let resolved = hint_channel(
            &opts,
            TurnVerb::Local,
            HINT_TURN_CHANNEL,
            HINT_APPROVAL_ID,
            hint_far_deadline(),
        )
        .await;

        assert_eq!(
            resolved, HINT_TURN_CHANNEL,
            "a null card_channel means the requesting channel applies, not that \
             the hint should print an empty or literal-null channel"
        );
    }

    /// P2. An EMPTY `card_channel` is not a channel, and must degrade exactly
    /// like a null one.
    ///
    /// The wire model admits `"card_channel": ""`, and the SERVER already reads
    /// it as absent: the authorizer resolves the approver set as
    /// `approval.card_channel or approval.reply_channel`
    /// (`apps/api/src/curie_api/slack_approvers.py:174`), and an empty string is
    /// falsy in Python, so the members of the REPLY channel are the approver
    /// set. A CLI that echoed the empty value would print an unusable hint --
    /// the exact failure #1531 exists to remove, reintroduced by the fix for it
    /// and on a record shape nothing else in the suite covers.
    ///
    /// The turn channel is the right answer rather than merely a safe one: it
    /// IS the reply channel, which is what the server falls back to.
    ///
    /// Mutation it catches: `Ok(Some(card_channel)) => card_channel` with no
    /// emptiness check, which is what the current implementation does.
    #[tokio::test]
    async fn the_hint_names_the_turn_channel_when_the_card_channel_is_empty() {
        let base = hint_stub_api(200, HINT_EMPTY_CARD_APPROVAL).await;
        let opts = hint_opts(&base);

        let resolved = hint_channel(
            &opts,
            TurnVerb::Local,
            HINT_TURN_CHANNEL,
            HINT_APPROVAL_ID,
            hint_far_deadline(),
        )
        .await;

        assert!(
            !resolved.is_empty(),
            "the hint must never report an empty card location; fall back to \
             the requesting channel when no route-bound location exists"
        );
        assert_eq!(
            resolved, HINT_TURN_CHANNEL,
            "an empty card_channel is what the server itself treats as absent, \
             so the hint must name the reply channel the server falls back to, \
             which is the turn channel"
        );
    }

    /// The other side of that boundary: a WHITESPACE-ONLY `card_channel` is a
    /// real channel to the server, so the hint must print it VERBATIM.
    ///
    /// This test and
    /// `the_hint_names_the_turn_channel_when_the_card_channel_is_empty` are
    /// deliberately a PAIR, and the pair is the point. The server picks the
    /// approver set with `approval.card_channel or approval.reply_channel`
    /// (`apps/api/src/curie_api/slack_approvers.py:174`), and in Python ONLY the
    /// empty string is falsy. `" "` is truthy, so the authorizer takes that
    /// exact whitespace value as the card channel. A CLI that trimmed before
    /// testing for emptiness would degrade to the turn channel and hand the
    /// operator the wrong card location -- which is the very failure #1531
    /// exists to remove, so a guard meant to prevent it would be causing it.
    ///
    /// The CLI's job here is to mirror Python falsiness exactly, not to improve
    /// on it: only `""` is absent, and everything else is printed as-is. A later
    /// reader must not "simplify" these two tests into one with a `trim()`; the
    /// two fixtures differ by a single space precisely so that collapse fails.
    ///
    /// Mutation it catches: `!card_channel.trim().is_empty()` in place of
    /// `!card_channel.is_empty()`, which is what the current implementation
    /// does.
    #[tokio::test]
    async fn a_whitespace_only_card_channel_is_a_channel_and_prints_verbatim() {
        let base = hint_stub_api(200, HINT_BLANK_CARD_APPROVAL).await;
        let opts = hint_opts(&base);

        let resolved = hint_channel(
            &opts,
            TurnVerb::Local,
            HINT_TURN_CHANNEL,
            HINT_APPROVAL_ID,
            hint_far_deadline(),
        )
        .await;

        assert_eq!(
            resolved, HINT_BLANK_CARD_CHANNEL,
            "a whitespace-only card_channel is TRUTHY in Python, so the server \
             authorizes against that exact value; the hint must reproduce it \
             byte for byte rather than trimming it away"
        );
        assert_ne!(
            resolved, HINT_TURN_CHANNEL,
            "degrading here prints a channel the server will not accept, which \
             is the 403 this whole change exists to remove"
        );
    }

    /// A 404 is absorbed by the advisory wrapper, not surfaced.
    ///
    /// Real rather than theoretical: another operator can resolve or expire the
    /// approval between the pending notice and the hint. The client method
    /// propagates the 404 (see `cli/tests/approval_hint_channel.rs`), and this
    /// is the layer that turns it into today's behavior. The operator then
    /// discovers the resolution through the wait itself.
    ///
    /// Mutation it catches: bubbling the client error out of the wrapper, which
    /// would make a race between two operators fail the turn.
    #[tokio::test]
    async fn the_hint_names_the_turn_channel_when_the_approval_is_already_gone() {
        let base = hint_stub_api(404, r#"{"detail":"approval not found"}"#).await;
        let opts = hint_opts(&base);

        let resolved = hint_channel(
            &opts,
            TurnVerb::Local,
            HINT_TURN_CHANNEL,
            HINT_APPROVAL_ID,
            hint_far_deadline(),
        )
        .await;

        assert_eq!(
            resolved, HINT_TURN_CHANNEL,
            "an approval resolved out from under the wait is 'no answer'; the \
             hint degrades rather than the turn failing"
        );
    }

    // ─── #1531 finding 3, cluster arm: degradation without a leaked child ────
    //
    // Everything above drives `TurnVerb::Local`, whose tier dispatch is a plain
    // base URL. The CLUSTER arm reaches the API through a short-lived
    // `kubectl port-forward` child instead, and until now no automated test
    // entered it at all. The test below covers its FAILURE path only: the
    // forward cannot start, so the lookup has no answer. The SUCCESS path,
    // where the forward binds and the GET returns a card channel, needs a live
    // cluster with a real release in it and is therefore recorded separately as
    // tier evidence rather than asserted here.

    /// The namespace and release this test's cluster-tier `MessageOpts` name.
    ///
    /// Deliberately values no real deployment would ever use, because the leak
    /// assertion counts processes by these strings. A developer running an
    /// unrelated `kubectl port-forward` against a real release on the same box
    /// must not be counted by that scan, must not be killed, and must not be
    /// able to fail this test.
    const HINT_CLUSTER_NAMESPACE: &str = "curie-hint-1531-absent-namespace";
    const HINT_CLUSTER_RELEASE: &str = "curie-hint-1531-absent-release";

    /// A cluster-tier turn. There is no `api_url` to point anywhere, unlike
    /// [`hint_opts`]: on this tier the namespace and release ARE the dispatch,
    /// since they are what [`port_forward_command`] renders into the child's
    /// argv. The local port is likewise a value nothing else on the box is
    /// expected to hold, so a real forward is never disturbed.
    fn hint_cluster_opts() -> MessageOpts {
        MessageOpts {
            api_key: HINT_API_KEY.to_string(),
            namespace: HINT_CLUSTER_NAMESPACE.to_string(),
            release: HINT_CLUSTER_RELEASE.to_string(),
            api_local_port: 18531,
            local: false,
            ..MessageOpts::default()
        }
    }

    /// The `svc/<release>-api` argument [`port_forward_command`] builds for
    /// [`hint_cluster_opts`]: the token that identifies a child THIS test
    /// caused, and nothing else.
    fn hint_cluster_forward_target() -> String {
        format!("svc/{HINT_CLUSTER_RELEASE}-api")
    }

    /// How many live processes carry both this test's namespace and its
    /// `svc/<release>-api` target on their command line.
    ///
    /// A count, never a kill: the assertion compares this before and after, so
    /// an unrelated pre existing forward cancels out instead of failing the
    /// test, and no process this test did not start is ever signalled.
    ///
    /// A zombie has an EMPTY `cmdline` in `/proc`, so a child that has already
    /// exited but is still awaiting reap is not counted. That is what keeps the
    /// assertion free of reap timing flake: the question is whether a forward is
    /// still RUNNING, not whether its slot is cleared.
    #[cfg(target_os = "linux")]
    fn hint_cluster_port_forwards() -> usize {
        let target = hint_cluster_forward_target();
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return 0;
        };
        entries
            .filter_map(Result::ok)
            .filter(|entry| {
                let Ok(raw) = std::fs::read(entry.path().join("cmdline")) else {
                    return false;
                };
                // `/proc` separates argv with NUL; joining on spaces makes the
                // needles read like the argv `port_forward_command` renders.
                let argv = String::from_utf8_lossy(&raw).replace('\0', " ");
                argv.contains(HINT_CLUSTER_NAMESPACE) && argv.contains(&target)
            })
            .count()
    }

    /// The same count where there is no `/proc` to walk. `pgrep -f` matches the
    /// same space joined argv, and a box with neither `/proc` nor `pgrep`
    /// answers zero on both sides of the call, which leaves the delta assertion
    /// true rather than falsely red.
    #[cfg(not(target_os = "linux"))]
    fn hint_cluster_port_forwards() -> usize {
        let Ok(out) = std::process::Command::new("pgrep")
            .arg("-f")
            .arg(hint_cluster_forward_target())
            .output()
        else {
            return 0;
        };
        out.stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .count()
    }

    /// The cluster arm degrades to the turn channel when the port forward cannot
    /// start, stays inside its budget, and leaves no `kubectl port-forward`
    /// child behind ON THAT PATH.
    ///
    /// The input is always "the forward did not bind", in every environment this
    /// test can run in, and the contract is identical in each, so none of them
    /// is skipped: a developer box whose `kubectl` has no current context errors
    /// out at once, CI where `kubectl` is not on PATH fails the spawn, and a box
    /// with a live cluster still has no such namespace as
    /// [`HINT_CLUSTER_NAMESPACE`].
    ///
    /// What it covers:
    ///
    /// 1. DEGRADATION (#1531 finding 3). The hint tells a human where the
    ///    approval card was posted. A cluster whose API cannot be reached knows
    ///    nothing about the card, so it must print exactly what the hint printed
    ///    before this change. A wrong or empty channel sends the operator to the
    ///    wrong place, which is the failure #1531 exists to remove.
    /// 2. BOUND. The lookup runs INSIDE the resume wait, so a cluster arm that
    ///    sat on `start_port_forward` would freeze a terminal on a turn whose
    ///    durable approval is already fine.
    /// 3. NO CHILD LEFT BEHIND on the failed-bind path: a spawn that somehow
    ///    outlives a bind that failed would show up as a nonzero delta.
    ///
    /// What it does NOT cover, said plainly so no reader takes more from it than
    /// it gives. Because `start_port_forward` always ERRORS here, no guard is
    /// ever constructed, and the child count is zero on both sides of the call.
    /// The guard's drop on the SUCCESS path -- which is the #751/#766 regression
    /// class proper -- is therefore untested by this test: hoisting the guard out
    /// of the lookup, leaking it with `std::mem::forget`, or dropping
    /// `kill_on_drop` would all still pass here, because none of them can run.
    /// That property rests on the live cluster verification this ticket requires
    /// (`pgrep -f "kubectl port-forward"` empty after a turn that actually bound
    /// one), and nothing in `cargo test` can stand in for it.
    ///
    /// Mutations it does catch: replacing the degraded arm with the card channel
    /// unwrapped, or with an empty string, fails assertion 1; deleting the
    /// wrapping `tokio::time::timeout` so `start_port_forward`'s own 15 second
    /// readiness deadline governs fails assertion 2, and a lookup that hangs
    /// outright is caught by the outer harness timeout.
    #[tokio::test]
    async fn the_cluster_arm_degrades_without_leaking_a_port_forward() {
        let opts = hint_cluster_opts();
        let before = hint_cluster_port_forwards();

        let started = Instant::now();
        // The same harness safety net the local stall tests use, and wider than
        // the budget under test on purpose: an implementation that lost its
        // bound would otherwise hang CI instead of failing it.
        let resolved = tokio::time::timeout(
            HINT_CHANNEL_LOOKUP_BUDGET * 3,
            hint_channel(
                &opts,
                TurnVerb::Cluster,
                HINT_TURN_CHANNEL,
                HINT_APPROVAL_ID,
                hint_far_deadline(),
            ),
        )
        .await
        .expect(
            "the cluster lookup never returned within three budgets against a cluster it cannot \
             reach, so nothing is bounding it: a real turn would sit here forever",
        );
        let elapsed = started.elapsed();

        assert_eq!(
            resolved, HINT_TURN_CHANNEL,
            "a cluster whose API cannot be reached is 'no answer', and no answer \
             means the hint prints exactly what it printed before this change"
        );
        assert!(
            !resolved.is_empty(),
            "the cluster hint must never report an empty card location; its \
             unreachable-API fallback is the requesting channel"
        );
        // A forward that cannot bind fails at once on every environment listed
        // above, so anything near the budget means the failure is being waited
        // on rather than absorbed, and the bound is named rather than a literal
        // so it tracks the constant if that is ever retuned.
        assert!(
            elapsed < HINT_CHANNEL_LOOKUP_BUDGET,
            "the cluster lookup must stay inside its budget so it can never \
             extend the resume wait; took {elapsed:?}"
        );

        // The kill is delivered on drop, so give the kernel a moment to land it
        // before concluding a child survived (the same poll the abandoned docker
        // child test uses).
        let mut after = hint_cluster_port_forwards();
        for _ in 0..100 {
            if after <= before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            after = hint_cluster_port_forwards();
        }
        assert!(
            after <= before,
            "the cluster lookup leaked a `kubectl port-forward` for \
             {} (before {before}, after {after}); an orphaned forward is the \
             #751/#766 regression class, and this lookup runs once per gate",
            hint_cluster_forward_target()
        );
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
        // #1031(a): container NAMES carry the compose project, so a non-default
        // `COMPOSE_PROJECT_NAME` renames the container out from under any
        // hardcoded name. The label selector is what still resolves it.
        assert_eq!(
            select_worker_container("acme-staging-curie-worker-1\n").unwrap(),
            "acme-staging-curie-worker-1"
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

    fn test_agent(name: &str, channel: &str) -> Agent {
        test_agent_bound_to(name, &[channel])
    }

    fn test_agent_bound_to(name: &str, channels: &[&str]) -> Agent {
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
            memory: false,
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
        let cmd = port_forward_command(
            "curie",
            &crate::ops::chart_fullname("curie"),
            "valkey",
            56381,
            6379,
        );
        assert_eq!(
            cmd.display(),
            "kubectl -n curie port-forward svc/curie-valkey 56381:6379"
        );
    }

    /// #1533 (S14): a release name that does not contain the chart name renders
    /// `{release}-curie-{component}` in the chart, so the tunnel must ask for
    /// that. `platform` renders `platform-curie`, never `platform`.
    ///
    /// The Implementer builds this against a RESOLVED `ReleaseFullname`; a raw
    /// release name cannot reach the builder, which is the point of the newtype.
    #[test]
    fn port_forward_command_uses_the_resolved_fullname() {
        let fullname = crate::ops::chart_fullname("platform");
        assert_eq!(
            port_forward_command("curie", &fullname, "api", 18000, 8000).display(),
            "kubectl -n curie port-forward svc/platform-curie-api 18000:8000"
        );
        assert_eq!(
            port_forward_command("curie", &fullname, "valkey", 56381, 6379).display(),
            "kubectl -n curie port-forward svc/platform-curie-valkey 56381:6379"
        );

        // Negative control: the default release must stay byte-identical to
        // what shipped before this change.
        let default = crate::ops::chart_fullname("curie");
        assert_eq!(
            port_forward_command("curie", &default, "api", 18000, 8000).display(),
            "kubectl -n curie port-forward svc/curie-api 18000:8000"
        );
        assert_eq!(
            port_forward_command("curie", &default, "valkey", 56381, 6379).display(),
            "kubectl -n curie port-forward svc/curie-valkey 56381:6379"
        );
    }

    /// #1533 (S15), the highest-consequence site. `dispatcher_connected_strict`
    /// probes with `--ignore-not-found`, so a Deployment name that does not
    /// exist returns success with EMPTY output -- which the caller reads as
    /// "Slack is disconnected" and acts on by widening worker Slack trust.
    /// That collapses the disconnected-vs-unprobeable distinction PR #1839 /
    /// issue #1812 exists to preserve, silently and in the wrong direction.
    ///
    /// Asserted on the whole rendered argv rather than the name alone: the
    /// `--ignore-not-found` and `-o name` flags are what make a wrong name
    /// silent, so they are pinned here too.
    #[test]
    fn dispatcher_probe_names_the_chart_rendered_deployment() {
        assert_eq!(
            dispatcher_probe_command("acme-system", &crate::ops::chart_fullname("platform"))
                .display(),
            "kubectl -n acme-system get deployment platform-curie-dispatcher \
             --ignore-not-found -o name"
        );

        // Negative control: unchanged for the default release.
        assert_eq!(
            dispatcher_probe_command("curie", &crate::ops::chart_fullname("curie")).display(),
            "kubectl -n curie get deployment curie-dispatcher --ignore-not-found -o name"
        );
    }

    /// #1533 (S34): `cluster eval --release platform` used to read
    /// `deployment/platform-worker` and fail before a single eval case ran.
    #[test]
    fn probe_fake_model_targets_the_chart_rendered_worker() {
        let argv =
            fake_model_probe_command("acme-system", &crate::ops::chart_fullname("platform")).argv();
        assert!(
            argv.iter().any(|a| a == "deployment/platform-curie-worker"),
            "the fake-model probe must read the chart-rendered worker: {argv:?}"
        );
        assert!(
            !argv.iter().any(|a| a == "deployment/platform-worker"),
            "the fake-model probe must not compute `{{release}}-worker`: {argv:?}"
        );

        // Negative control.
        let control =
            fake_model_probe_command("curie", &crate::ops::chart_fullname("curie")).argv();
        assert!(
            control.iter().any(|a| a == "deployment/curie-worker"),
            "the default release must be unchanged: {control:?}"
        );
    }

    #[tokio::test]
    async fn port_forward_rejects_an_occupied_port_without_owned_readiness() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let cmd = OpsCommand::new("sh", vec![plain("-c"), plain("sleep 0.05; exit 1")]);

        let err = start_port_forward(&cmd, port, "valkey")
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains("valkey"), "{err}");
        assert!(err.contains(&port.to_string()), "{err}");
        assert!(err.contains("closed stdout before reporting"), "{err}");
    }

    #[tokio::test]
    async fn port_forward_accepts_the_kubectl_readiness_line() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let cmd = OpsCommand::new(
            "sh",
            vec![
                plain("-c"),
                plain(format!(
                    "printf 'Forwarding from 127.0.0.1:{port} -> 6379\\n'; sleep 1"
                )),
            ],
        );

        let (mut child, effective_port) = start_port_forward(&cmd, port, "valkey").await.unwrap();

        assert_eq!(effective_port, port);
        assert!(child.try_wait().unwrap().is_none());
    }

    #[tokio::test]
    async fn port_forward_rejects_ipv6_readiness_for_a_fixed_ipv4_port() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let cmd = OpsCommand::new(
            "sh",
            vec![
                plain("-c"),
                plain(format!(
                    "printf 'Forwarding from [::1]:{port} -> 6379\\n'; sleep 0.5"
                )),
            ],
        );

        let err = start_port_forward(&cmd, port, "valkey")
            .await
            .unwrap_err()
            .to_string();

        assert!(err.contains(&port.to_string()), "{err}");
        assert!(err.contains("closed stdout before reporting"), "{err}");
    }

    #[test]
    fn select_channel_prefers_explicit() {
        let agents = [test_agent("a", "C1"), test_agent("b", "C2")];
        assert_eq!(select_channel(&agents, Some("CX")).unwrap(), "CX");
    }

    #[test]
    fn select_channel_uses_the_sole_agent() {
        let agents = [test_agent("only", "C-ONLY")];
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
        let agents = [test_agent("alpha", "C1"), test_agent("beta", "C2")];
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
            test_agent_bound_to("only", &["C0EXAMPLE1"]),
            test_agent_bound_to("idle", &[]),
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
        let agents = [test_agent_bound_to("solo", &["C0EXAMPLE1", "C0EXAMPLE2"])];
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
            test_agent_bound_to("one", &["C0EXAMPLE1", "C0EXAMPLE2"]),
            test_agent_bound_to("two", &["C0EXAMPLE3"]),
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
        let token = |r: Option<crate::slack::SlackTransport>| r.map(|t| t.bot_token);

        // Connected: the worker's transport is real Slack and its token is real.
        assert_eq!(
            token(connected_worker_transport((
                t("https://slack.com/api/"),
                t("xoxb-real")
            ))),
            Some("xoxb-real".to_string())
        );

        // #957 mode A, the failure this replaced: a REAL token is present but the
        // worker is wired to the stub, so posting would orphan a "..." in a real
        // channel that the worker can never edit. Not connected.
        assert_eq!(
            connected_worker_transport((
                t("http://localhost:8155/api/"),
                t("xoxb-real-from-the-vault")
            )),
            None
        );

        // The stub sentinel is not a workspace token even if the base URL is odd.
        assert_eq!(
            connected_worker_transport((t("https://slack.com/api/"), t(LOCAL_STUB_BOT_TOKEN))),
            None
        );
        // #1031: an EMPTY `SLACK_API_BASE_URL` is this repo's "talk to real Slack"
        // signal, not its "nothing configured" one. `local comms --connect` sets
        // exactly that (`comms::local_connect_commands`), compose's single-dash
        // `${SLACK_API_BASE_URL-...}` preserves it, and the runbook documents it.
        // Reading it as disconnected made the connected local transport
        // unreachable. Connected, over real Slack's own base.
        let empty_base = connected_worker_transport((t(""), t("xoxb-real")))
            .expect("an empty SLACK_API_BASE_URL is the connected signal, not the stub one");
        assert_eq!(empty_base.api_base, crate::slack::DEFAULT_API_BASE);
        assert_eq!(empty_base.bot_token, "xoxb-real");

        // Absent is NOT empty. `None` means the value was never read off the
        // container (no stack, docker down, container renamed), so there is no
        // worker transport to trust -> stub path, never a real post.
        assert_eq!(connected_worker_transport((None, None)), None);
        assert_eq!(connected_worker_transport((None, t("xoxb-real"))), None);
        assert_eq!(
            connected_worker_transport((t("https://slack.com/api/"), None)),
            None
        );
        assert_eq!(
            connected_worker_transport((t("https://slack.com/api/"), t("  "))),
            None
        );
    }

    fn local_comms_opts(disconnect: bool) -> crate::comms::LocalCommsOpts {
        crate::comms::LocalCommsOpts {
            file: "compose.dev.yaml".to_string(),
            dry_run: false,
            app_token: "xapp-real-workspace".to_string(),
            bot_token: "xoxb-real-workspace".to_string(),
            disconnect,
            model_mode: crate::local::ModelMode::DefaultFake,
            model_credentials: Vec::new(),
            model: None,
            minimal: false,
            stack_image_env: Vec::new(),
        }
    }

    /// The worker Slack env a `local comms` command actually applies, read back
    /// out of the built `OpsCommand`s the same way compose would.
    fn worker_slack_env(commands: &[crate::ops::OpsCommand]) -> WorkerTransport {
        let get = |key: &str| -> Option<String> {
            commands
                .iter()
                .flat_map(|cmd| cmd.env.iter().chain(cmd.secret_env.iter()))
                .find(|(name, _)| name == key)
                .map(|(_, value)| value.clone())
        };
        (get("SLACK_API_BASE_URL"), get("SLACK_BOT_TOKEN"))
    }

    /// #1031 end to end across the two modules: the env `curie local comms
    /// --connect` really applies to the worker must classify as CONNECTED, and
    /// `--disconnect`'s must classify as NOT connected. Both halves are read from
    /// the command builder rather than restated here, so inverting either the
    /// predicate or the builder fails this test instead of silently stranding the
    /// connected path (which is exactly how the defect shipped).
    #[test]
    fn local_comms_connect_and_disconnect_round_trip_through_the_probe() {
        let connect = crate::comms::local_connect_commands(&local_comms_opts(false));
        let connected_env = worker_slack_env(&connect);
        assert_eq!(
            connected_env.0,
            Some(String::new()),
            "`local comms --connect` un-wires the stub by setting an EMPTY base; if that \
             ever changes, the probe's reading of empty must change with it"
        );

        let transport = connected_worker_transport(connected_env)
            .expect("`local comms --connect` must leave the worker classified as connected");
        assert_eq!(transport.api_base, crate::slack::DEFAULT_API_BASE);
        assert_eq!(transport.bot_token, "xoxb-real-workspace");

        // Negative control on the same seam: --disconnect restores the stub, which
        // must stay classified as not connected however real the token looks.
        let disconnect = crate::comms::local_disconnect_commands(&local_comms_opts(true));
        let disconnected_env = worker_slack_env(&disconnect);
        assert!(
            disconnected_env
                .0
                .as_deref()
                .is_some_and(|base| base.contains(LOCAL_SLACK_STUB_HOST)),
            "`local comms --disconnect` must point the worker back at the stub: {:?}",
            disconnected_env.0
        );
        assert_eq!(connected_worker_transport(disconnected_env), None);
    }

    /// #1031 mode C at the local tier: a probe that could not RUN is not evidence
    /// that no workspace is connected, so it says so rather than silently taking
    /// the stub path (the asymmetry `ops::dispatcher_connected` already closed).
    #[test]
    fn a_probe_that_could_not_run_warns_rather_than_claiming_disconnected() {
        let warning = worker_probe_warning("no running container matches `label=x`");
        assert!(
            warning.contains("could not determine")
                && warning.contains("no running container matches `label=x`")
                && warning.contains("NOT connected"),
            "{warning}"
        );
        // Only the first line of a multi-line failure, so a docker stack trace
        // cannot swamp the operator's terminal.
        let multi = worker_probe_warning("first line\nsecond line");
        assert!(
            multi.contains("first line") && !multi.contains("second line"),
            "{multi}"
        );
    }

    /// #1031(d): a wedged docker daemon accepts the request and never answers.
    /// Without a bound, `curie local message` hangs forever -- which contradicts
    /// the "probe failure is the safe direction" claim, because it never fails.
    /// `std::future::pending()` is that daemon; the assertion is that the probe
    /// RETURNS at all.
    #[tokio::test]
    async fn a_wedged_docker_daemon_times_the_probe_out_instead_of_hanging() {
        let budget = Duration::from_millis(20);
        // The outer bound is the test harness's own safety net: without the guard
        // under test this call never returns, and a CI job that HANGS is a worse
        // signal than one that fails. Five seconds is 250x the budget, so it can
        // only fire when the budget is not being applied at all.
        let reason = tokio::time::timeout(
            Duration::from_secs(5),
            bounded_worker_probe(std::future::pending(), budget),
        )
        .await
        .expect("the probe must be bounded; it never returned")
        .expect_err("a probe that never answers must time out, not hang");
        assert!(
            reason.contains("did not answer within"),
            "the timeout must be reported as a timeout: {reason}"
        );
    }

    /// The `/proc` state letter for `pid`: `None` once the entry is gone, `Some('Z')`
    /// while it is a reaped-pending zombie, `Some('S')` while it is still sleeping.
    #[cfg(target_os = "linux")]
    fn proc_state(pid: u32) -> Option<char> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        // comm can contain spaces and parens, so the fields start after the LAST ')'.
        let rest = stat.rsplit_once(')')?.1;
        rest.split_whitespace().next()?.chars().next()
    }

    /// #1031(d), second half: bounding the WAIT is not bounding the WORK. A
    /// timeout that only drops the future leaves the `docker` client running
    /// against the wedged daemon, so every timed-out `local message` strands
    /// another one -- the leak is worst in exactly the scenario the timeout
    /// exists for. `run_capture` sets `kill_on_drop`, so abandoning the probe
    /// takes the child with it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_timed_out_probe_kills_the_docker_child_it_abandoned() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("create temporary directory");
        let pidfile = temp.path().join("pid");
        let script = temp.path().join("wedged-docker");
        std::fs::write(&script, "#!/bin/sh\necho $$ > \"$1\"\nexec sleep 60\n")
            .expect("write wedged docker shim");
        let mut permissions = std::fs::metadata(&script)
            .expect("shim metadata")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script, permissions).expect("make shim executable");

        let cmd = OpsCommand::new(
            script.to_str().expect("shim path is UTF 8"),
            vec![plain(pidfile.to_str().expect("pidfile path is UTF 8"))],
        );
        let reason = bounded_worker_probe(
            async {
                let (_ok, _out, _err) = run_capture(&cmd).await?;
                Ok((None, None))
            },
            Duration::from_millis(300),
        )
        .await
        .expect_err("a shim that never answers must time out");
        assert!(reason.contains("did not answer within"), "{reason}");

        let pid: u32 = std::fs::read_to_string(&pidfile)
            .expect("the shim recorded its pid before sleeping")
            .trim()
            .parse()
            .expect("the recorded pid is a number");

        // The kill is delivered on drop; give the kernel a moment to land it.
        let mut state = proc_state(pid);
        for _ in 0..100 {
            if !matches!(state, Some('S') | Some('R') | Some('D')) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
            state = proc_state(pid);
        }
        assert!(
            !matches!(state, Some('S') | Some('R') | Some('D')),
            "the abandoned child is still running (pid {pid}, state {state:?})"
        );
    }

    #[test]
    fn the_probe_budget_is_bounded_and_usable() {
        assert!(
            WORKER_PROBE_TIMEOUT > Duration::ZERO
                && WORKER_PROBE_TIMEOUT <= Duration::from_secs(30),
            "{WORKER_PROBE_TIMEOUT:?}"
        );
    }

    #[test]
    fn the_local_transport_carries_the_workers_own_base_not_the_shells() {
        // The #1030 regression at local tier. The base and the token are read from
        // the same `docker inspect`, so the placeholder goes where the worker that
        // will edit it is pointed. Before this, only the token survived and the base
        // came from whatever the operator happened to have exported.
        let resolved = connected_worker_transport((
            Some("https://proxy.example/api".to_string()),
            Some("xoxb-worker-actual".to_string()),
        ))
        .expect("a real base and a real token are a connected transport");
        assert_eq!(resolved.api_base, "https://proxy.example/api");
        assert_eq!(resolved.bot_token, "xoxb-worker-actual");

        std::env::set_var("SLACK_API_BASE_URL", "http://127.0.0.1:18081/api");
        let again = connected_worker_transport((
            Some("https://proxy.example/api".to_string()),
            Some("xoxb-worker-actual".to_string()),
        ))
        .expect("still connected");
        assert_eq!(
            again.api_base, "https://proxy.example/api",
            "an ambient SLACK_API_BASE_URL must not redirect a resolved token"
        );
        std::env::remove_var("SLACK_API_BASE_URL");
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
    fn dry_run_lists_distinct_valkey_and_api_forwards_without_a_callback_endpoint() {
        let lines = dry_run_lines(&opts(Some("C123")), "10.1.2.3");
        assert!(
            lines
                .iter()
                .any(|l| l == "kubectl -n curie port-forward svc/curie-valkey 56381:6379"),
            "{lines:?}"
        );
        assert!(
            lines
                .iter()
                .any(|l| l == "kubectl -n curie port-forward svc/curie-api 8123:8000"),
            "reply polling always needs its own api forward: {lines:?}"
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
            lines
                .iter()
                .any(|l| l
                    == "poll replies at http://127.0.0.1:8123/cluster-message-replies/<uuid-v4>"),
            "{lines:?}"
        );
        assert!(
            lines.iter().any(|l| l.contains("enqueue")
                && l.contains("C123")
                && l.contains("adapter curie-cluster-message")
                && l.contains("no reply endpoint")
                && l.contains("UUIDv4 reply ref")),
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
            Some("http://localhost:8155/api/"),
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
        let v = message_dry_run_json("cluster", "s", None, None);
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
            sampling: crate::eval_sampling::SampleConfig::default(),
            case_ids: Vec::new(),
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
            &Outcome::AwaitingApproval {
                reply: Some("pong pending approval".into()),
                approval_id: None,
            }
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
            &Outcome::AwaitingApproval {
                reply: Some("blocked the close".into()),
                approval_id: None,
            }
        ));
        assert!(reply_passes(
            &case,
            &Outcome::AwaitingApproval {
                reply: None,
                approval_id: None,
            }
        ));
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
        assert!(
            lines
                .iter()
                .any(|l| l.contains("without ambient durable agent memory")),
            "default local eval must declare memory isolation: {lines:?}"
        );
    }

    #[test]
    fn eval_dry_run_plan_names_sampling_policy() {
        let lines = eval_dry_run_lines(&eval_opts(true, None), "smoke", 1);
        assert!(
            lines
                .iter()
                .any(|l| l.contains("sampling: 1 sample(s), majority")),
            "dry-run must document the default one-sample majority policy: {lines:?}"
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
                memory: false,
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
                memory: false,
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

    /// #1915: the one-shot producer must run the dispatcher image the STACK
    /// runs, not whatever this shell resolves.
    ///
    /// Compose substitutes its variables from each invocation's own environment,
    /// so after `local up --build` the stack ran `:dev` while `local message`
    /// resolved `:latest` and died on `No module named
    /// curie_dispatcher.enqueue_once` -- the module the published image predates.
    #[test]
    fn the_one_shot_producer_runs_the_stacks_dispatcher_image() {
        let cmd = dispatcher_enqueue_command(
            &["compose.dev.yaml".to_string()],
            "enqueue-1",
            "curie:runs",
            "pw",
            &[],
            Some("ghcr.io/curie-eng/curie-dispatcher:dev"),
        );

        assert!(
            cmd.env.iter().any(|(k, v)| k == "CURIE_DISPATCHER_IMAGE"
                && v == "ghcr.io/curie-eng/curie-dispatcher:dev"),
            "expected the stack's dispatcher image in the child env, got {:?}",
            cmd.env
        );
    }

    /// A stack on the published images passes nothing, so compose's own default
    /// still applies. This is also what keeps CI working: the ladder builds its
    /// dispatcher as `:latest` while setting CURIE_BASE_TAG=ci-local, so a
    /// derived-but-unchecked tag would ask for an image nothing built.
    #[test]
    fn a_published_stack_leaves_the_image_to_compose() {
        let cmd = dispatcher_enqueue_command(
            &["compose.dev.yaml".to_string()],
            "enqueue-1",
            "curie:runs",
            "pw",
            &[],
            None,
        );

        assert!(!cmd.env.iter().any(|(k, _)| k == "CURIE_DISPATCHER_IMAGE"));
    }

    // --- --case-id selector (#2007) -----------------------------------------

    fn selector_suite() -> EvalSuite {
        EvalSuite {
            name: "smoke".into(),
            cases: ["greets-the-user", "looks-up-the-order"]
                .iter()
                .map(|id| {
                    let mut case = eval_case(GraderKind::Contains, "hi");
                    case.id = (*id).into();
                    case
                })
                .collect(),
        }
    }

    #[test]
    fn an_unmatched_case_id_selector_is_a_usage_error_at_local_and_cluster() {
        // The headline of #2007 on the parity tiers: a mistyped selector fails
        // the gate (exit 2) rather than greening an empty run.
        let err = crate::evals::select_cases(
            selector_suite(),
            std::slice::from_ref(&"greets-the-usr".to_string()),
        )
        .expect_err("a mistyped --case-id must fail");
        assert_eq!(
            crate::exit::classify(&err).0,
            crate::exit::ExitClass::Usage,
            "{err:#}"
        );
        assert!(format!("{err:#}").contains("greets-the-usr"), "{err:#}");
    }

    #[test]
    fn a_case_id_selector_on_a_model_sweep_is_unsupported() {
        assert!(guard_sweep_case_ids(&[]).is_ok(), "no selector, no refusal");
        let err = guard_sweep_case_ids(std::slice::from_ref(&"greets-the-user".to_string()))
            .expect_err("--case-id + --model must be refused");
        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(
            class,
            crate::exit::ExitClass::Unsupported,
            "the refusal is ADR-0041 Unsupported (exit 4): {err:#}"
        );
        assert!(format!("{err:#}").contains("--case-id"), "{err:#}");
        assert!(
            fix.expect("the refusal names the honest alternative")
                .contains("--case-id"),
            "the fix names the in-CLI path that honors a selector",
        );
    }

    #[test]
    fn a_case_id_selector_on_the_trajectory_platform_path_is_unsupported() {
        assert!(
            guard_trajectory_case_ids(&[]).is_ok(),
            "no selector, no refusal"
        );
        let err = guard_trajectory_case_ids(std::slice::from_ref(&"greets-the-user".to_string()))
            .expect_err("--case-id on a trajectory eval must be refused");
        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Unsupported, "{err:#}");
        assert!(
            format!("{err:#}").contains("trajectory"),
            "the message names the plane that cannot honor it: {err:#}"
        );
        assert!(
            fix.expect("the refusal names the honest alternative")
                .contains("skill eval --case-id"),
            "the fix names the tier that does honor a selector",
        );
    }

    #[tokio::test]
    async fn a_mistyped_case_id_fails_a_local_eval_dry_run_rather_than_greening_it() {
        // End of the wire at this tier: even a --dry-run refuses, so a mistyped
        // selector cannot be discovered only after a full stack run.
        let dir = tempfile::tempdir().expect("tempdir");
        let cases = dir.path().join("cases.json");
        std::fs::write(
            &cases,
            r#"{"name":"smoke","cases":[{"id":"greets-the-user","input":"hi","grader":{"kind":"contains","expected":"hi"}}]}"#,
        )
        .expect("write suite");
        let mut opts = eval_opts(true, Some("C7"));
        opts.cases = Some(cases);
        opts.case_ids = vec!["greets-the-usr".to_string()];
        let err = eval(opts)
            .await
            .expect_err("a mistyped --case-id must fail the run");
        assert_eq!(
            crate::exit::classify(&err).0,
            crate::exit::ExitClass::Usage,
            "{err:#}"
        );
        assert!(format!("{err:#}").contains("greets-the-usr"), "{err:#}");
    }

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|part| (*part).to_string()).collect()
    }

    #[test]
    fn reject_agent_named_message_fires_on_the_issue_2498_argv() {
        let err = reject_agent_named_message(&argv(&[
            "cluster",
            "message",
            "acme-bot",
            "Who are you? Reply with the plugin name.",
            "--namespace",
            "curie",
            "--release",
            "curie",
        ]))
        .expect("two positionals must be a usage error");
        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        let shown = format!("{err:#}");
        assert!(shown.contains("does not take an agent name"), "{shown}");
        assert!(shown.contains("<AGENT>"), "{shown}");
        let fix = fix.expect("fix must name --channel");
        assert!(fix.contains("--channel"), "{fix}");
        assert!(fix.contains("exactly one channel"), "{fix}");
    }

    #[test]
    fn reject_agent_named_message_ignores_single_text_continue_and_channel() {
        for parts in [
            &["cluster", "message", "Who are you?"][..],
            &["cluster", "message", "--continue", "what's 2 + 2?"],
            &[
                "cluster",
                "message",
                "--channel",
                "C0EXAMPLE1",
                "Who are you?",
            ],
            &["local", "message", "Who are you?", "--dry-run"],
            &["cluster", "versions", "acme-bot"],
            &["cluster", "versions", "acme-bot", "extra"],
            &["skill", "message", "acme-bot", "Who are you?"],
        ] {
            assert_eq!(
                reject_agent_named_message(&argv(parts)).map(|err| format!("{err:#}")),
                None,
                "must not reject {parts:?}"
            );
        }
    }

    #[test]
    fn reject_agent_named_message_sees_json_before_the_verb() {
        let err = reject_agent_named_message(&argv(&[
            "--json",
            "local",
            "message",
            "acme-bot",
            "Who are you?",
        ]))
        .expect("global --json must not hide the two-positional trap");
        assert!(format!("{err:#}").contains("local message"), "{err:#}");
    }

    #[test]
    fn reject_agent_named_message_sees_json_between_target_and_verb() {
        let err = reject_agent_named_message(&argv(&[
            "cluster",
            "--json",
            "message",
            "acme-bot",
            "Who are you?",
        ]))
        .expect("global --json between target and verb must not hide the trap");
        assert!(format!("{err:#}").contains("cluster message"), "{err:#}");
    }
}
