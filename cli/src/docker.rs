//! Local runner orchestration via the Docker CLI.
//!
//! `curie skill up` boots the D1 runner image with the ACI-frozen boot env
//! (runner/README.md documents the recipe); `stop` tears it down. Shelling out
//! to `docker` keeps the CLI dependency-light: the target machine is a dev
//! laptop that already has Docker if it can run the runner at all.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use curie_aci_protocol::env_keys;
use tokio::process::Command;

/// Everything `docker run` needs to boot a local runner container.
#[derive(Debug, Clone)]
pub struct StartSpec {
    pub image: String,
    pub container_name: String,
    pub host_port: u16,
    pub plugin_dir: PathBuf,
    pub session_id: String,
    pub sandbox_id: String,
    pub budget_json: String,
    pub fake_model: bool,
    pub network: Option<String>,
    pub otel_endpoint: Option<String>,
    pub model_base_url: Option<String>,
    /// Model id forwarded as `CURIE_MODEL`; `None` leaves the runner on its
    /// SDK default.
    pub model: Option<String>,
    /// Env vars forwarded from the caller's environment when set (model
    /// credentials for real-model runs; never baked into the args as values).
    pub passthrough_env: Vec<String>,
    /// Env values supplied only to the Docker CLI process. Used for secrets
    /// loaded from Curie private storage so they can be forwarded by `-e NAME`
    /// without mutating the Curie process env or appearing in argv.
    pub docker_env: Vec<(String, String)>,
}

/// Everything `docker run` needs for a one shot offline MCP load check.
#[derive(Debug, Clone)]
pub struct CheckSpec {
    pub image: String,
    pub plugin_dir: String,
    pub timeout_s: u64,
}

/// Container-isolation flags applied to every local runner container (#631):
/// a read-only root filesystem (+ tmpfs for `/tmp` and the non-root runner's
/// HOME so it can still write), all Linux capabilities dropped, and
/// no-new-privileges. Mirrors the K8s runner `securityContext` and the worker
/// Docker substrate's `RunnerHardening`
/// (`apps/worker/src/curie_worker/sandbox/docker.py`). Local mode accepts
/// TRUSTED bundles only and is not the Kubernetes security boundary; these are
/// practical defense-in-depth so a trusted-but-buggy bundle cannot escalate on
/// the host. Docker's default seccomp profile stays active (never unconfined).
/// Resource caps (memory/cpu) are intentionally left to the worker substrate,
/// which runs the untrusted product loop; the interactive skill-dev loop here
/// keeps them off so a developer's heavy local run is not throttled.
fn runner_hardening_args() -> Vec<String> {
    vec![
        "--read-only".into(),
        "--tmpfs".into(),
        "/tmp:rw,mode=1777".into(),
        "--tmpfs".into(),
        "/home/runner:rw,mode=1777".into(),
        "--cap-drop".into(),
        "ALL".into(),
        "--security-opt".into(),
        "no-new-privileges".into(),
    ]
}

impl CheckSpec {
    /// The one shot check container argv (after the `docker` executable).
    pub fn run_args(&self) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "run".into(),
            "--rm".into(),
            // Offline contract: the check must never reach the network. A bundle
            // with a remote (`url:`) MCP server, or a stdio server that phones
            // home at startup, must fail (red) rather than pass by connecting
            // out. `--network none` is empirically verified NOT to break the
            // legitimate in-bundle stdio-server case.
            "--network".into(),
            "none".into(),
        ];
        // Same container isolation as the long-lived runner (#631): the check
        // executes an untrusted bundle's MCP servers, so it must not run looser.
        args.extend(runner_hardening_args());
        args.extend([
            "-v".into(),
            format!("{}:/plugin:ro", self.plugin_dir),
            "-e".into(),
            format!("{}=/plugin", env_keys::CURIE_PLUGIN_DIR),
            "-e".into(),
            format!("CURIE_CHECK_TIMEOUT_S={}", self.timeout_s),
            self.image.clone(),
            "python".into(),
            "-m".into(),
            "curie_runner.check".into(),
        ]);
        args
    }
}

impl StartSpec {
    /// The `docker run` argument vector (after the `docker` executable).
    pub fn run_args(&self) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            self.container_name.clone(),
            "-p".into(),
            format!("{}:8080", self.host_port),
            "-v".into(),
            format!("{}:/plugin:ro", self.plugin_dir.display()),
            "-e".into(),
            format!("{}=/plugin", env_keys::CURIE_PLUGIN_DIR),
            "-e".into(),
            format!("{}={}", env_keys::CURIE_SESSION_ID, self.session_id),
            "-e".into(),
            format!("{}={}", env_keys::CURIE_SANDBOX_ID, self.sandbox_id),
            "-e".into(),
            format!("{}={}", env_keys::CURIE_BUDGET, self.budget_json),
        ];
        // Container isolation (#631): read-only rootfs + tmpfs, cap-drop ALL,
        // no-new-privileges. Mirrors the worker Docker substrate + K8s runner.
        args.extend(runner_hardening_args());
        // Identify CLI-booted runners (#747). Deliberately NOT `SANDBOX_LABEL`:
        // that one is the worker substrate's, and `local down` reaps by it.
        args.push("--label".into());
        args.push(CLI_MANAGED_LABEL.into());
        args.push("--label".into());
        args.push(RUNNER_COMPONENT_LABEL.into());
        if self.fake_model {
            args.push("-e".into());
            args.push(format!("{}=1", env_keys::CURIE_FAKE_MODEL));
        }
        if let Some(model) = &self.model {
            args.push("-e".into());
            args.push(format!("{}={model}", env_keys::CURIE_MODEL));
        }
        if let Some(url) = &self.model_base_url {
            args.push("-e".into());
            args.push(format!("{}={url}", env_keys::ANTHROPIC_BASE_URL));
        }
        if let Some(network) = &self.network {
            args.push("--network".into());
            args.push(network.clone());
        }
        if let Some(endpoint) = &self.otel_endpoint {
            args.push("-e".into());
            args.push(format!(
                "{}={endpoint}",
                env_keys::OTEL_EXPORTER_OTLP_ENDPOINT
            ));
        }
        for var in &self.passthrough_env {
            if std::env::var_os(var).is_some()
                || self.docker_env.iter().any(|(name, _)| name == var)
            {
                args.push("-e".into());
                args.push(var.clone());
            }
        }
        args.push(self.image.clone());
        args
    }
}

/// Run a docker subcommand, returning trimmed stdout; stderr on failure.
pub async fn docker(args: &[String]) -> Result<String> {
    docker_with_env(args, &[]).await
}

/// Run a docker subcommand with extra environment values supplied only to the
/// Docker CLI child process.
pub async fn docker_with_env(args: &[String], env: &[(String, String)]) -> Result<String> {
    let (status, stdout, stderr) = docker_capture_with_env(args, env).await?;
    if !status.success() {
        bail!(
            "docker {} failed ({}): {}",
            args.first().map(String::as_str).unwrap_or(""),
            status,
            stderr
        );
    }
    Ok(stdout)
}

fn docker_operator_error(source: anyhow::Error, message: &str) -> anyhow::Error {
    crate::exit::operator_context(
        source,
        message,
        Some(
            "Run `docker info` to verify Docker is installed and the daemon is running, then retry."
                .to_string(),
        ),
    )
}

/// Run a docker subcommand and capture its status plus both output streams.
///
/// A check container's nonzero verdict is data, so unlike [`docker`] this does
/// not turn an unsuccessful child exit into an error. Failure to invoke Docker
/// remains an error.
pub async fn docker_capture(args: &[String]) -> Result<(std::process::ExitStatus, String, String)> {
    docker_capture_with_env(args, &[]).await
}

pub async fn docker_capture_with_env(
    args: &[String],
    env: &[(String, String)],
) -> Result<(std::process::ExitStatus, String, String)> {
    let mut cmd = Command::new("docker");
    cmd.args(args);
    for (name, value) in env {
        cmd.env(name, value);
    }
    let output = cmd
        .output()
        .await
        .context("failed to invoke docker; is Docker installed and on PATH?")
        .map_err(|err| docker_operator_error(err, "Docker is unavailable for this command."))?;
    Ok((
        output.status,
        String::from_utf8_lossy(&output.stdout).trim().to_string(),
        String::from_utf8_lossy(&output.stderr).trim().to_string(),
    ))
}

/// The complete startup window shared by every connector that one command
/// starts. Keeping it here prevents one boot with multiple connectors from
/// granting each container a separate sixty second wait.
pub(crate) const CONNECTOR_START_TIMEOUT: Duration = Duration::from_secs(60);

/// The bounded window for resolving the actual Docker IDs of local Compose
/// connector services after `compose up` completes. It is separate from the
/// final readiness observation because resolution scales with connector count.
pub(crate) const CONNECTOR_ID_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(10);

/// The read-only final readiness observation window after local Compose IDs
/// have resolved. It confirms successful starts without extending the startup
/// window, and a failed Compose start remains failed even when this pass finds
/// ready containers.
pub(crate) const CONNECTOR_DIAGNOSTIC_TIMEOUT: Duration = Duration::from_secs(5);

const CONNECTOR_STABLE_RUNNING_FOR: Duration = Duration::from_secs(2);
const CONNECTOR_INSPECT_FORMAT: &str = "{{.State.Status}}\t{{.State.Running}}\t{{.State.Restarting}}\t{{.State.ExitCode}}\t{{.RestartCount}}\t{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}";

#[derive(Debug)]
struct ConnectorRuntimeState {
    status: String,
    running: bool,
    restarting: bool,
    exit_code: i32,
    restart_count: u64,
    health: Option<String>,
}

#[derive(Default)]
struct ConnectorWaitState {
    restart_count: Option<u64>,
    running_since: Option<Instant>,
}

fn connector_readiness_error(message: String) -> anyhow::Error {
    let remedy = "check the connector configuration and logs, then retry".to_string();
    let source = anyhow::Error::from(
        crate::exit::CliError::failure(message.clone()).with_fix(remedy.clone()),
    );
    crate::exit::operator_context(source, message, Some(remedy))
}

fn connector_readiness_failure(container: &str, reason: &str) -> anyhow::Error {
    connector_readiness_error(format!(
        "connector '{container}' failed to become ready: {reason}"
    ))
}

fn connector_readiness_failures(containers: &[String], reason: &str) -> anyhow::Error {
    if containers.len() == 1 {
        return connector_readiness_failure(&containers[0], reason);
    }
    let names = if containers.is_empty() {
        "connector".to_string()
    } else {
        containers
            .iter()
            .map(|container| format!("'{container}'"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    connector_readiness_error(format!(
        "connectors {names} failed to become ready: {reason}"
    ))
}

/// Preserve a failed Compose outcome even when its short diagnostic pass finds
/// only ready containers, such as when a pre-existing container survived a
/// failed update.
pub(crate) fn connector_compose_start_failure(containers: &[String]) -> anyhow::Error {
    let names = containers
        .iter()
        .map(|container| format!("'{container}'"))
        .collect::<Vec<_>>()
        .join(", ");
    connector_readiness_error(format!(
        "Docker Compose startup did not complete successfully for declared connectors: {names}"
    ))
}

fn connector_readiness_timeout(
    containers: &[(String, String)],
    last_pending: &[String],
) -> anyhow::Error {
    let pending = if last_pending.is_empty() {
        containers
            .iter()
            .map(|(display_name, _)| display_name.clone())
            .collect::<Vec<_>>()
    } else {
        last_pending.to_vec()
    };
    connector_readiness_failures(
        &pending,
        "timed out waiting for readiness before the deadline",
    )
}

async fn inspect_connector_runtime_state(
    display_name: &str,
    inspect_ref: &str,
    deadline: Instant,
) -> Result<Option<ConnectorRuntimeState>> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining == Duration::ZERO {
        return Ok(None);
    }
    let command = crate::connector_build::plain_command(
        "docker",
        vec![
            "inspect".into(),
            "--format".into(),
            CONNECTOR_INSPECT_FORMAT.into(),
            inspect_ref.to_string(),
        ],
    );
    let captured = match tokio::time::timeout(remaining, crate::ops::run_capture(&command)).await {
        Ok(captured) => captured,
        Err(_) => return Ok(None),
    };
    let (ok, stdout, _stderr) = captured.map_err(|_| {
        connector_readiness_failure(display_name, "Docker could not inspect its readiness state")
    })?;
    if !ok {
        return Err(connector_readiness_failure(
            display_name,
            "Docker could not inspect its readiness state",
        ));
    }

    let mut fields = stdout.trim().split('\t');
    let status = fields.next().map(str::to_string);
    let running = fields.next().and_then(|field| field.parse::<bool>().ok());
    let restarting = fields.next().and_then(|field| field.parse::<bool>().ok());
    let exit_code = fields.next().and_then(|field| field.parse::<i32>().ok());
    let restart_count = fields.next().and_then(|field| field.parse::<u64>().ok());
    let health = fields.next().map(str::to_string);
    if fields.next().is_some()
        || status.is_none()
        || running.is_none()
        || restarting.is_none()
        || exit_code.is_none()
        || restart_count.is_none()
        || health.is_none()
    {
        return Err(connector_readiness_failure(
            display_name,
            "Docker returned an unreadable readiness state",
        ));
    }

    Ok(Some(ConnectorRuntimeState {
        status: status.expect("validated above"),
        running: running.expect("validated above"),
        restarting: restarting.expect("validated above"),
        exit_code: exit_code.expect("validated above"),
        restart_count: restart_count.expect("validated above"),
        health: match health.expect("validated above").as_str() {
            "none" => None,
            value => Some(value.to_string()),
        },
    }))
}

/// Wait for every named connector started by one command to prove process
/// readiness. Health checked containers require Docker's `healthy` state;
/// containers without a health check must stay running continuously for two
/// seconds. The deadline covers every inspect subprocess, which is killed if
/// the timed-out future is dropped by `ops::run_capture`.
pub(crate) async fn wait_for_connectors_ready(
    containers: &[(String, String)],
    deadline: Instant,
) -> Result<()> {
    if containers.is_empty() {
        return Ok(());
    }
    let mut states: Vec<ConnectorWaitState> = (0..containers.len())
        .map(|_| ConnectorWaitState::default())
        .collect();
    let mut last_pending: Vec<String> = containers
        .iter()
        .map(|(display_name, _)| display_name.clone())
        .collect();

    loop {
        if Instant::now() >= deadline {
            return Err(connector_readiness_timeout(containers, &last_pending));
        }

        let mut all_ready = true;
        let mut observed_pending = Vec::new();
        for ((display_name, inspect_ref), wait_state) in containers.iter().zip(states.iter_mut()) {
            if inspect_ref.is_empty() {
                return Err(connector_readiness_failure(
                    display_name,
                    "its container was not created",
                ));
            }

            let Some(state) =
                inspect_connector_runtime_state(display_name, inspect_ref, deadline).await?
            else {
                return Err(connector_readiness_timeout(containers, &last_pending));
            };
            if let Some(previous) = wait_state.restart_count {
                if previous != state.restart_count {
                    return Err(connector_readiness_failure(
                        display_name,
                        "its restart count changed during startup",
                    ));
                }
            } else {
                wait_state.restart_count = Some(state.restart_count);
            }

            if state.restarting || state.status == "restarting" {
                return Err(connector_readiness_failure(
                    display_name,
                    "it is restarting during startup",
                ));
            }
            if matches!(state.status.as_str(), "exited" | "dead") {
                return Err(connector_readiness_failure(
                    display_name,
                    &format!("it exited with code {}", state.exit_code),
                ));
            }
            if state.health.as_deref() == Some("unhealthy") {
                return Err(connector_readiness_failure(
                    display_name,
                    "its health check reported unhealthy",
                ));
            }

            match state.health.as_deref() {
                Some("healthy") if state.running => {
                    wait_state.running_since = None;
                }
                Some("starting") | Some("healthy") => {
                    all_ready = false;
                    observed_pending.push(display_name.clone());
                    wait_state.running_since = None;
                }
                Some(_) => {
                    return Err(connector_readiness_failure(
                        display_name,
                        "its health check returned an unknown state",
                    ));
                }
                None if state.running => {
                    let running_since = wait_state.running_since.get_or_insert_with(Instant::now);
                    if running_since.elapsed() < CONNECTOR_STABLE_RUNNING_FOR {
                        all_ready = false;
                        observed_pending.push(display_name.clone());
                    }
                }
                None => {
                    all_ready = false;
                    observed_pending.push(display_name.clone());
                    wait_state.running_since = None;
                }
            }
        }

        if all_ready {
            return Ok(());
        }

        last_pending = observed_pending;

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining == Duration::ZERO {
            return Err(connector_readiness_timeout(containers, &last_pending));
        }
        tokio::time::sleep(Duration::from_millis(200).min(remaining)).await;
    }
}

/// Create a docker network. Returns `Ok(true)` when this call created it and
/// `Ok(false)` when the network already existed, so the caller only claims
/// ownership (and thus teardown responsibility) for networks it actually made.
pub async fn create_network(name: &str) -> Result<bool> {
    let args = ["network".into(), "create".into(), name.to_string()];
    let (status, _stdout, stderr) = docker_capture(&args).await?;
    if status.success() {
        return Ok(true);
    }
    if stderr.contains("already exists") {
        return Ok(false);
    }
    bail!("docker network create failed ({}): {}", status, stderr)
}

pub async fn remove_network(name: &str) -> Result<()> {
    docker(&["network".into(), "rm".into(), name.to_string()])
        .await
        .map(|_| ())
}

/// The named volume that persists an ollama container's model cache
/// (`/root/.ollama`) across `skill down`/`skill up`, so a repeat demo reuses
/// the pulled model instead of re-downloading it.
pub fn ollama_volume(container: &str) -> String {
    format!("{container}-data")
}

// ---------------------------------------------------------------------------
// Local-model preflight (ADR 0093, issue #1183)
// ---------------------------------------------------------------------------

/// The download `--local-model` used to trigger implicitly for the pinned image.
/// Stated in the preflight failure so the operator sees the bill before paying
/// it; `DEFAULT_OLLAMA_IMAGE`'s tag is what this measures.
pub const OLLAMA_IMAGE_DOWNLOAD_SIZE: &str = "~8.9 GB";

/// The characters an Ollama reference segment may contain (#1254). An allowlist,
/// not a denylist of shell metacharacters: a denylist has to enumerate every
/// dangerous byte for every consumer downstream, and gets it wrong the first time
/// a new consumer appears. This is what Ollama itself accepts in a registry host,
/// namespace, name or tag.
fn is_model_segment_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')
}

/// Reject an Ollama model reference that is not one (#1254).
///
/// The preflight this guards (`preflight_local_model`) exists to stop an implicit
/// multi-GB download, and a reference carrying shell metacharacters used to defeat
/// it outright: `--local-model 'missing; true #'` made the probe's `test -e` exit 0
/// regardless, the guard reported the model present, and `compose up` performed
/// exactly the download ADR-0093 forbids.
///
/// The probe no longer builds a shell command out of this value, so injection is
/// structurally impossible there now. This validator is the other half, and it is
/// worth having on its own terms: the value also reaches `CURIE_MODEL` in the
/// compose env, where `ollama-pull` expands it inside a container, and it will
/// reach whatever consumer is added next. Rejecting a nonsense reference at the
/// argument boundary is cheaper than auditing every future site, and it turns a
/// silent wrong answer into an error the operator can read.
///
/// Args:
///   model: the raw `--local-model` value.
///
/// Returns:
///   `Ok(())` when the reference is well-formed, otherwise an error naming the
///   offending character and the grammar.
pub fn validate_model_ref(model: &str) -> Result<()> {
    if model.is_empty() {
        bail!("model reference is empty; expected something like `qwen3:4b`");
    }
    // Segment the same way `model_manifest_path` does, so a reference this accepts
    // is exactly a reference that function can map to a path. The two are a pair;
    // validating a different grammar than the one we then parse is how a validator
    // ends up guarding nothing.
    let (name, tag) = match model.rsplit_once(':') {
        Some((n, t)) if !t.contains('/') => (n, t),
        _ => (model, "latest"),
    };
    if tag.is_empty() {
        bail!("model reference `{model}` has an empty tag; expected e.g. `qwen3:4b`");
    }
    // A registry host may carry a port, and only there: `localhost:5000/org/repo`
    // is a reference `model_manifest_path` maps to `localhost:5000/org/repo/latest`.
    // Accepting the colon only in that position keeps the validator's grammar equal
    // to the parser's rather than merely close to it.
    let segments: Vec<&str> = name.split('/').collect();
    let host_is_ported = segments.len() >= 3;
    let named: Vec<(&str, &str)> = segments
        .iter()
        .enumerate()
        .flat_map(|(i, seg)| match seg.split_once(':') {
            Some((host, port)) if i == 0 && host_is_ported => {
                vec![("registry host", host), ("registry port", port)]
            }
            _ => vec![("name segment", *seg)],
        })
        .collect();
    for (label, part) in [("tag", tag)].into_iter().chain(named) {
        if part.is_empty() {
            bail!("model reference `{model}` has an empty {label}");
        }
        if let Some(bad) = part.chars().find(|c| !is_model_segment_char(*c)) {
            bail!(
                "model reference `{model}` contains {bad:?} in a {label}; \
                 only letters, digits, `.`, `-` and `_` are allowed, \
                 separated by `/` with an optional `:tag` (e.g. `qwen3:4b`)"
            );
        }
        // Every character of `..` is on the allowlist above, so the #1254 grammar
        // waved it through -- and `model_manifest_path` then joined it verbatim
        // into a path the probe's `test -e` resolves for real, letting any
        // existing file answer "already pulled" (#1363). The rule is stated
        // POSITIVELY -- a name contains a letter or a digit -- rather than as a
        // denylist of `.` and `..`, because a denylist has to enumerate every
        // spelling of "this is a path operator, not a name" and misses the next
        // one. Ollama has no such reference either way.
        if !part.chars().any(|c| c.is_ascii_alphanumeric()) {
            bail!(
                "model reference `{model}` has a {label} (`{part}`) with no letter or \
                 digit; a segment like `.` or `..` is a path operator, not a name, and \
                 the presence probe would resolve it against the real filesystem"
            );
        }
    }
    // The grammar and the path it composes are a pair, and only the second one is
    // what the probe acts on. Checking where the reference LANDS closes the class
    // rather than the one spelling of it that was reported: this holds for any
    // future loosening of the character rule above, and it is the same predicate
    // `model_probe_args` re-checks at the probe boundary.
    let manifest = model_manifest_path(model);
    if !manifest_path_is_contained(&manifest) {
        bail!(
            "model reference `{model}` composes the manifest path `{manifest}`, \
             which does not stay under `models/manifests/`; it does not name a model"
        );
    }
    Ok(())
}

/// The directory every model manifest lives under, relative to `/root/.ollama`.
/// A composed path that does not land strictly BELOW this is not naming a model.
const MANIFEST_ROOT: [&str; 2] = ["models", "manifests"];

/// Resolve `.` and `..` in a relative path lexically -- no filesystem, no
/// symlinks, just the arithmetic a shell would do (#1363).
///
/// Args:
///   path: a relative, `/`-separated path.
///
/// Returns:
///   The surviving segments, or `None` when a `..` climbs above the root. Climbing
///   above the root is the escape itself, so it is an answer, not an error case to
///   paper over with an empty vector.
fn normalize_relative_path(path: &str) -> Option<Vec<&str>> {
    let mut resolved: Vec<&str> = Vec::new();
    for segment in path.split('/') {
        match segment {
            // A trailing or doubled separator, and `.`, both mean "stay here".
            "" | "." => {}
            ".." => {
                resolved.pop()?;
            }
            name => resolved.push(name),
        }
    }
    Some(resolved)
}

/// Whether a composed manifest path still names something strictly inside
/// `models/manifests/` (#1363).
///
/// Landing ON the prefix counts as an escape, not as containment: the
/// `models/manifests` directory exists in any volume Ollama has ever written to,
/// so `test -e` on it answers "present" for a model that was never pulled. That
/// is exactly what `--local-model '..:..'` composes.
///
/// Args:
///   path: a manifest path relative to the Ollama data root.
///
/// Returns:
///   `true` when the path resolves to a location below `models/manifests/`.
fn manifest_path_is_contained(path: &str) -> bool {
    match normalize_relative_path(path) {
        Some(resolved) => {
            resolved.len() > MANIFEST_ROOT.len() && resolved.starts_with(&MANIFEST_ROOT)
        }
        None => false,
    }
}

/// Where Ollama stores a model's manifest inside its data directory, relative
/// to `/root/.ollama`. Presence of this path is what "the model is already
/// pulled" means on disk.
///
/// Mirrors Ollama's own naming: a bare `qwen3:4b` is the `library` namespace on
/// the default registry, `ns/name` names a namespace, and three or more
/// segments carry their own registry host. A ref with no tag is `latest`, the
/// same default `ollama pull` applies.
///
/// Args:
///   model: an Ollama model reference, e.g. `qwen3:4b` or `hf.co/org/repo:q4`.
///
/// Returns:
///   The manifest path relative to the Ollama data root, with no leading slash.
pub fn model_manifest_path(model: &str) -> String {
    // Split the tag off the LAST colon so a registry host keeps any port it has.
    let (name, tag) = match model.rsplit_once(':') {
        // A colon inside the final path segment is the tag; one before a `/` is
        // a host port and belongs to the name.
        Some((n, t)) if !t.contains('/') => (n, t),
        _ => (model, "latest"),
    };
    let segments: Vec<&str> = name.split('/').filter(|s| !s.is_empty()).collect();
    let qualified = match segments.len() {
        0 | 1 => format!(
            "registry.ollama.ai/library/{}",
            segments.first().unwrap_or(&"")
        ),
        2 => format!("registry.ollama.ai/{}", segments.join("/")),
        _ => segments.join("/"),
    };
    format!("models/manifests/{qualified}/{tag}")
}

/// Whether an image is already in the local image cache. Offline and fast
/// (measured at ~32ms); a missing image is a clean `false`, not an error, since
/// `docker image inspect` exits nonzero for exactly that case.
pub async fn image_present(image: &str) -> Result<bool> {
    let (status, _out, _err) =
        docker_capture_with_env(&["image".into(), "inspect".into(), image.to_string()], &[])
            .await?;
    Ok(status.success())
}

/// Whether a named volume exists. A volume that was never created cannot hold a
/// model, so this settles the cold case without starting anything.
pub async fn volume_present(volume: &str) -> Result<bool> {
    let (status, _out, _err) = docker_capture_with_env(
        &["volume".into(), "inspect".into(), volume.to_string()],
        &[],
    )
    .await?;
    Ok(status.success())
}

/// Whether `model` is already pulled into `volume`.
///
/// Reads the volume through a throwaway container built from `image` -- the
/// Ollama image the caller has just established is present -- so the probe pulls
/// nothing and needs no second image. On Docker Desktop a volume's host
/// mountpoint lives inside the VM, so mounting it is the only way to look.
///
/// Args:
///   volume: the Docker volume mounted at Ollama's `/root/.ollama`.
///   image: an Ollama image known to be present locally.
///   model: the model reference to look for.
///
/// Returns:
///   `true` when the model's manifest is on disk in that volume.
pub async fn model_present_in_volume(volume: &str, image: &str, model: &str) -> Result<bool> {
    if !volume_present(volume).await? {
        return Ok(false);
    }
    // `model_probe_args` owns both guards -- argv placement against #1254's shell
    // injection, and path containment against #1363's traversal -- and refuses to
    // build a probe it cannot vouch for. Propagating that refusal is the
    // fail-closed answer: an unvouchable reference must not read as "present",
    // which is the reading that skips the download disclosure.
    let (status, _out, _err) =
        docker_capture_with_env(&model_probe_args(volume, image, model)?, &[]).await?;
    Ok(status.success())
}

/// The `docker run` argv for the model-presence probe, pure so the properties
/// #1254 and #1363 turned on are assertable with no daemon.
///
/// Two different holes are closed here, and they are closed in two different
/// ways -- conflating them is how #1363 happened.
///
/// The path is a POSITIONAL ARGUMENT, never text spliced into the command.
/// Interpolating it made `--local-model 'missing; true #'` end the probe's
/// `test -e` early and leave a `true` behind, so the probe exited 0, the guard
/// reported the model present, and `compose up` performed exactly the multi-GB
/// download ADR-0093 exists to prevent (#1254). `"$1"` cannot be reparsed as code
/// no matter what it holds, so THAT hole is genuinely closed by construction.
/// `_` is the conventional `$0` filler.
///
/// Being inert as code is not the same as naming the right file, which is what
/// #1363 cost us: an inert `../../../../etc/hostname` is still a path `test -e`
/// resolves, and any existing file answers "already pulled". Nothing about argv
/// placement can fix that, so containment is CHECKED here rather than assumed --
/// and checked at this boundary, not only at the argument boundary, so the
/// guarantee survives a caller that never ran the validator.
///
/// Args:
///   volume: the Docker volume mounted at Ollama's `/root/.ollama`.
///   image: an Ollama image known to be present locally.
///   model: the model reference to look for.
///
/// Returns:
///   The probe argv, or an error when the reference composes a path outside
///   `models/manifests/`.
pub fn model_probe_args(volume: &str, image: &str, model: &str) -> Result<Vec<String>> {
    let manifest = model_manifest_path(model);
    if !manifest_path_is_contained(&manifest) {
        bail!(
            "refusing to probe for model `{model}`: it composes the manifest path \
             `{manifest}`, which does not stay under `models/manifests/`. A reference \
             whose segments are path operators would let some unrelated existing file \
             answer the presence probe, skipping the download disclosure (#1363)"
        );
    }
    Ok(vec![
        "run".into(),
        "--rm".into(),
        "-v".into(),
        format!("{volume}:/root/.ollama"),
        "--entrypoint".into(),
        "/bin/sh".into(),
        image.to_string(),
        "-c".into(),
        "test -e \"$1\"".into(),
        "_".into(),
        format!("/root/.ollama/{manifest}"),
    ])
}

/// The operator-facing refusal when `--local-model` is asked for assets that are
/// not on the machine (ADR 0093). Pure so its wording is testable: this text is
/// the ONLY place the download is disclosed before it would be spent, which is
/// what makes it load-bearing rather than decoration.
///
/// Args:
///   image: the pinned Ollama image, named when it is the missing one.
///   image_missing: whether the image has to be downloaded.
///   model: the requested model reference, named when it is the missing one.
///   model_missing: whether the model has to be downloaded.
///   fetch_hint: the exact `curie` command that fetches what is missing.
///
/// Returns:
///   A multi-line message naming each missing asset, its cost, and the fix.
pub fn missing_local_model_assets_message(
    image: &str,
    image_missing: bool,
    model: &str,
    model_missing: bool,
    fetch_hint: &str,
) -> String {
    let mut lines = vec![
        "local model assets are not on this machine, and curie does not download them implicitly:"
            .to_string(),
    ];
    if image_missing {
        lines.push(format!(
            "  - docker image  {image}  ({OLLAMA_IMAGE_DOWNLOAD_SIZE})"
        ));
    }
    if model_missing {
        lines.push(format!(
            "  - model         {model}  (size depends on the model; the qwen3:4b default is ~2.5 GB)"
        ));
    }
    lines.push(format!("fetch them now with:\n  {fetch_hint}"));
    lines.push(
        "a first fetch can take ~30 min on a 50 Mbit/s link; once both are cached a re-up is seconds".to_string(),
    );
    lines.join("\n")
}

/// The ADR-0093 refusal as a tagged [`crate::exit::CliError`], so the runnable
/// fetch command reaches an agent in the ADR-0021 `fix` field instead of only
/// inside the prose (#1261). A bare `bail!` classified as Failure with a null
/// `fix`, which is exactly the shape an agent consumer cannot branch on.
///
/// The class stays [`crate::exit::ExitClass::Failure`] (exit 1): the argv was
/// well-formed and `--local-model <m>` is a legitimate request, so this is a
/// machine-state failure and not a usage error -- re-running the same argv after
/// the fetch succeeds, which is the opposite of what exit 2 promises.
///
/// Pure and synchronous on purpose: [`preflight_local_model`] shells out to
/// docker and so is not unit-testable, while this guard -- the part that
/// carries the contract -- is. It also owns the `image_missing || model_missing`
/// decision itself, not just the error shape, so `preflight_local_model` has no
/// refusal branch of its own left to revert to a bare `bail!`: the async docker
/// probing stays there, but everything that carries the ADR-0021 contract lives
/// in this pure, testable function.
///
/// Args:
///   image: the pinned Ollama image, named when it is the missing one.
///   image_missing: whether the image has to be downloaded.
///   model: the requested model reference, named when it is the missing one.
///   model_missing: whether the model has to be downloaded.
///   fetch_hint: the exact `curie` command that fetches what is missing.
///
/// Returns:
///   `Ok(())` when neither asset is missing; otherwise an `Err` whose message
///   is [`missing_local_model_assets_message`] and whose `fix` is `fetch_hint`
///   alone -- the command, not the prose around it.
pub fn refuse_missing_local_model_assets(
    image: &str,
    image_missing: bool,
    model: &str,
    model_missing: bool,
    fetch_hint: &str,
) -> anyhow::Result<()> {
    if !image_missing && !model_missing {
        return Ok(());
    }
    Err(anyhow::Error::from(
        crate::exit::CliError::failure(missing_local_model_assets_message(
            image,
            image_missing,
            model,
            model_missing,
            fetch_hint,
        ))
        .with_fix(fetch_hint),
    ))
}

/// Refuse a `--local-model` run whose assets are not already on the machine
/// (ADR 0093). Shared by `skill up` and `local up` so the two tiers answer the
/// verb the same way (ADR 0041); only the volume and the fix hint differ.
///
/// Escalates only as far as it must: the image check settles the 8.9 GB half in
/// milliseconds, and the volume probe is reached only once the image is known to
/// be present -- so a fully cold machine never pays for it. Strictly offline.
///
/// Args:
///   image: the pinned Ollama image this tier boots.
///   volume: the Docker volume holding this tier's Ollama model cache.
///   model: the requested model reference.
///   fetch_hint: the exact `curie` command that provisions what is missing.
///
/// Returns:
///   `Ok(())` when both assets are present; otherwise whatever
///   [`refuse_missing_local_model_assets`] returns -- exit 1 with the fetch
///   command in the ADR-0021 `fix` field.
pub async fn preflight_local_model(
    image: &str,
    volume: &str,
    model: &str,
    fetch_hint: &str,
) -> Result<()> {
    let image_missing = !image_present(image).await?;
    // Without the image there is no container to read the volume WITH, so the
    // model is reported as missing too rather than probed. That is also the
    // honest answer: a machine with neither asset needs both.
    let model_missing = if image_missing {
        true
    } else {
        !model_present_in_volume(volume, image, model).await?
    };
    refuse_missing_local_model_assets(image, image_missing, model, model_missing, fetch_hint)
}

/// The `docker run` argument vector (after the `docker` executable) that boots
/// the ollama container. A named volume for `/root/.ollama` keeps the pulled
/// model cached across teardown; Docker auto-creates it on first use.
pub fn ollama_run_args(container: &str, network: &str, image: &str) -> Vec<String> {
    vec![
        "run".into(),
        "-d".into(),
        "--name".into(),
        container.to_string(),
        "--network".into(),
        network.to_string(),
        "-v".into(),
        format!("{}:/root/.ollama", ollama_volume(container)),
        // The sidecar is CLI-booted too, so it carries the same managed-by label
        // (#747): without it, `skill down --name <bundle>-ollama` would warn that
        // a container this very code path created may not be ours. No component
        // label: it is not a runner. Never SANDBOX_LABEL, which would put it in
        // `local down`'s reap set.
        "--label".into(),
        CLI_MANAGED_LABEL.into(),
        image.to_string(),
    ]
}

pub async fn run_ollama(container: &str, network: &str, image: &str) -> Result<String> {
    docker(&ollama_run_args(container, network, image)).await
}

pub async fn wait_ollama_ready(container: &str, timeout: Duration) -> Result<()> {
    let started = Instant::now();
    loop {
        let args = [
            "exec".into(),
            container.to_string(),
            "ollama".into(),
            "list".into(),
        ];
        let (status, _stdout, stderr) = docker_capture(&args).await?;
        if status.success() {
            return Ok(());
        }
        if started.elapsed() >= timeout {
            bail!(
                "ollama container '{container}' did not become ready within {}s: {}",
                timeout.as_secs(),
                stderr
            );
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

pub async fn pull_model(container: &str, model: &str) -> Result<()> {
    docker(&[
        "exec".into(),
        container.to_string(),
        "ollama".into(),
        "pull".into(),
        model.to_string(),
    ])
    .await
    .map(|_| ())
}

/// Best-effort container teardown (used for cleanup paths).
pub async fn remove_container(name_or_id: &str) -> Result<()> {
    docker(&["rm".into(), "-f".into(), name_or_id.to_string()])
        .await
        .map(|_| ())
}

/// The outcome of reaping labeled containers: what was removed, what is still
/// running, and whether the reap could be confirmed clean (#613).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReapReport {
    /// Containers `docker rm -f` confirmed removed (echoed on its stdout, #551).
    pub removed: usize,
    /// Labeled containers still present after the reap, established by re-listing
    /// rather than inferred from the `rm` exit. Non-empty means a failed or
    /// partial removal that must NOT be reported as a clean teardown.
    pub still_present: Vec<String>,
    /// A docker error that made the reap uncertain (the list step failed, the
    /// remove exited nonzero while containers remain, or the confirming re-list
    /// failed). `None` only when the teardown is confirmed clean.
    pub error: Option<String>,
}

async fn labeled_container_ids(label: &str) -> Result<Vec<String>> {
    let list_args: Vec<String> = vec![
        "ps".into(),
        "-a".into(),
        "--filter".into(),
        format!("label={label}"),
        "-q".into(),
    ];
    let out = docker(&list_args).await?;
    Ok(out
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect())
}

/// Assemble a [`ReapReport`] from the `rm` output and the post-`rm` re-list.
///
/// Pure so the disclosure logic (#613) is testable without a Docker daemon.
/// `still_present` is the ground truth for what is left running: a container
/// that raced away between the two lists is gone from both `removed` (it is not
/// echoed by `rm`, #551) and here, so it is neither over-counted nor falsely
/// reported as leftover. A nonzero `rm` exit whose containers are nonetheless
/// gone (the "No such container" of exactly that race) is therefore clean.
fn reap_report(
    rm_stdout: &str,
    rm_error: Option<String>,
    still_present: Vec<String>,
) -> ReapReport {
    let removed = count_removed(rm_stdout);
    let error = if still_present.is_empty() {
        None
    } else {
        Some(rm_error.unwrap_or_else(|| {
            format!(
                "{} container(s) still running after docker rm -f",
                still_present.len()
            )
        }))
    };
    ReapReport {
        removed,
        still_present,
        error,
    }
}

/// Remove all containers matching a Docker label filter, reporting what was and
/// was not removed.
///
/// Unlike the old best-effort version, neither the list step nor the removal
/// step is collapsed to a silent zero: a list failure, a nonzero `rm`, or a
/// confirming re-list failure is surfaced on [`ReapReport::error`] so a caller
/// (e.g. `local down`) cannot report a clean teardown while runner containers
/// keep holding ports and credentials (#613).
pub async fn reap_labeled(label: &str) -> ReapReport {
    let candidates = match labeled_container_ids(label).await {
        Ok(ids) => ids,
        Err(e) => {
            return ReapReport {
                removed: 0,
                still_present: Vec::new(),
                error: Some(format!("could not list containers labeled {label}: {e}")),
            }
        }
    };
    if candidates.is_empty() {
        return ReapReport {
            removed: 0,
            still_present: Vec::new(),
            error: None,
        };
    }

    let mut rm_args: Vec<String> = vec!["rm".into(), "-f".into()];
    rm_args.extend(candidates.iter().cloned());
    // Capture (not `docker`) so a nonzero exit keeps its stdout: `docker rm -f`
    // exits nonzero if ANY id fails, yet still echoes the ids it did remove.
    let (rm_stdout, rm_error) = match docker_capture_with_env(&rm_args, &[]).await {
        Ok((status, stdout, _)) if status.success() => (stdout, None),
        Ok((status, stdout, stderr)) => (
            stdout,
            Some(format!("docker rm -f exited {status}: {}", stderr.trim())),
        ),
        Err(e) => (
            String::new(),
            Some(format!("could not run docker rm -f: {e}")),
        ),
    };

    // Re-list for the ground truth of what is still running.
    match labeled_container_ids(label).await {
        Ok(still_present) => reap_report(&rm_stdout, rm_error, still_present),
        Err(e) => ReapReport {
            removed: count_removed(&rm_stdout),
            still_present: Vec::new(),
            error: Some(match rm_error {
                Some(r) => format!("{r}; also could not confirm teardown: {e}"),
                None => format!("could not confirm teardown of containers labeled {label}: {e}"),
            }),
        },
    }
}

/// The number of containers `docker rm -f` confirmed removed: it echoes one
/// removed id per stdout line. Pure so the teardown-count fix (#551) is testable
/// without a Docker daemon.
fn count_removed(rm_stdout: &str) -> usize {
    rm_stdout
        .lines()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .count()
}

/// The label that worker-local stamps on every runner container it spawns.
pub const SANDBOX_LABEL: &str = "curietech.ai/managed-by=curie-sandbox-substrate";

/// The runner image's short name (the dev/local tag, and the base of the GHCR
/// release ref). One definition (#497) instead of the literal scattered across
/// the artifact resolver, the boot path, and the clap defaults.
pub const RUNNER_IMAGE: &str = "curie-runner";

/// The fixed container name for the single local/skill runner (`skill up` /
/// `local` boot it, `skill down` reaps it). One definition (#497).
pub const RUNNER_CONTAINER_LOCAL: &str = "curie-runner-local";

/// The label every CLI-booted container carries (#747), so a leftover runner is
/// identifiable as ours. Distinct from [`SANDBOX_LABEL`] on purpose: that one is
/// the worker substrate's and drives `local down`'s reap set, which this must
/// not widen.
pub const CLI_MANAGED_LABEL: &str = "curietech.ai/managed-by=curie-cli";

/// The component label on CLI-booted runner containers (#747).
pub const RUNNER_COMPONENT_LABEL: &str = "curietech.ai/component=runner";

/// The key and value halves of [`CLI_MANAGED_LABEL`]. Split from the one
/// declaration rather than restated, so the `--format` read below cannot drift
/// from the `--label` the container is stamped with.
fn cli_managed_label_parts() -> (&'static str, &'static str) {
    CLI_MANAGED_LABEL
        .split_once('=')
        .expect("CLI_MANAGED_LABEL is a key=value pair")
}

/// What Docker reports about the container holding a name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContainerFacts {
    /// The container id, which is what makes a recorded runner identifiable
    /// across boots: a name can be reused by an entirely different container
    /// (#747).
    pub id: String,
    /// Whether it carries [`CLI_MANAGED_LABEL`], i.e. the Curie CLI booted it.
    pub cli_managed: bool,
}

/// Everything one teardown needs to know about the container holding exactly
/// this name, running or stopped. `None` when the name is free.
///
/// Id and label come from ONE `docker ps`, so they cannot disagree because the
/// container was replaced between two probes.
pub async fn container_facts(name: &str) -> Result<Option<ContainerFacts>> {
    let (label_key, label_value) = cli_managed_label_parts();
    let args: Vec<String> = vec![
        "ps".into(),
        "-a".into(),
        "--filter".into(),
        format!("name=^{name}$"),
        "--format".into(),
        format!("{{{{.Names}}}}\t{{{{.ID}}}}\t{{{{.Label \"{label_key}\"}}}}"),
    ];
    let out = docker(&args).await?;
    Ok(out
        .lines()
        .filter_map(|line| {
            let mut fields = line.split('\t');
            Some((fields.next()?.trim(), fields.next()?.trim(), fields.next()))
        })
        .find(|(found, _, _)| *found == name)
        .map(|(_, id, label)| ContainerFacts {
            id: id.to_string(),
            cli_managed: label.map(str::trim) == Some(label_value),
        }))
}

/// Whether a container with exactly this name exists, running or stopped.
pub async fn container_exists(name: &str) -> Result<bool> {
    Ok(container_facts(name).await?.is_some())
}

/// Whether these facts describe a container the Curie CLI booted. A container
/// left by a pre-label release reads as false, so this can inform a warning but
/// must never gate a removal (#747).
pub fn is_cli_managed(facts: Option<&ContainerFacts>) -> bool {
    facts.is_some_and(|f| f.cli_managed)
}

/// What to do about the target container name before booting (#747).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamePlan {
    /// The name is free; boot straight away.
    Proceed,
    /// The name is taken and `--replace` was passed; remove the leftover first.
    Replace,
}

/// Which verb hit the conflict, and therefore which remedies are real.
///
/// `skill eval`'s per-model sweep boots its own `curie-eval-sweep-<i>`
/// containers and accepts none of `--replace`, `--name` or `--port`, so it must
/// not be offered them (#747).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictContext {
    SkillUp,
    EvalSweep,
}

/// Resolve the name conflict, or produce the operator-facing error text.
///
/// Pure so the remedy wording (#747) is testable without a Docker daemon. The
/// error must be what the user sees instead of docker's raw exit-125 conflict,
/// and every remedy it names must exist on the verb that hit the conflict.
/// `port` is the host port the boot wanted, and `None` for a container that
/// publishes none (the local-model sidecar): `--port` is not the relevant knob
/// there, so the clause naming it is dropped rather than misdirecting.
pub fn plan_container_name(
    name: &str,
    port: Option<u16>,
    exists: bool,
    replace: bool,
    context: ConflictContext,
) -> std::result::Result<NamePlan, String> {
    match (exists, replace) {
        (false, _) => Ok(NamePlan::Proceed),
        (true, true) => Ok(NamePlan::Replace),
        (true, false) => Err(match context {
            ConflictContext::SkillUp => format!(
                "container name conflict: '{name}' already exists, likely a leftover runner from an earlier session; \
re-run with --replace to remove it and boot fresh, \
or remove it with 'curie skill down --name {name}', \
or boot this bundle beside it with a different --name{}",
                match port {
                    Some(port) => format!(" and --port (this run wanted port {port})"),
                    None => String::new(),
                }
            ),
            // No --replace here on purpose: a concurrent sweep's container must
            // not be force-removed out from under it.
            ConflictContext::EvalSweep => format!(
                "container name conflict: '{name}' already exists, likely a leftover from an interrupted model sweep{}; \
remove it with 'curie skill down --name {name}' and re-run the sweep",
                match port {
                    Some(port) => format!(" (it was to serve port {port})"),
                    None => String::new(),
                }
            ),
        }),
    }
}

/// Preflight the target container name, removing a leftover when `replace` is
/// set. Shared by `skill up`, its local-model sidecar and the per-model eval
/// runners so all three surface the same actionable error rather than docker's
/// raw conflict (#747). Reports the replacement itself, so no caller has to.
pub async fn ensure_container_name_free(
    name: &str,
    port: Option<u16>,
    replace: bool,
    context: ConflictContext,
) -> Result<()> {
    let exists = container_exists(name).await?;
    match plan_container_name(name, port, exists, replace, context).map_err(crate::exit::usage)? {
        NamePlan::Proceed => Ok(()),
        NamePlan::Replace => {
            remove_container(name)
                .await
                .with_context(|| format!("removing existing container '{name}' for --replace"))?;
            crate::ui::ui().note(&format!(
                "removed pre-existing container '{name}' (--replace)"
            ));
            Ok(())
        }
    }
}

/// Whether a docker failure is the name conflict docker reports (its exit 125)
/// when a container of that name already exists.
///
/// Pure so the lost-race mapping (#747) is testable without racing a real
/// daemon. Matched on docker's stable phrasing: `Conflict. The container name
/// "/x" is already in use by container "..."`.
pub fn is_name_conflict_error(message: &str) -> bool {
    let lowered = message.to_ascii_lowercase();
    lowered.contains("already in use by container")
        || (lowered.contains("conflict") && lowered.contains("container name"))
}

/// Map a `docker run` failure that lost the name-conflict race onto the same
/// actionable error the preflight produces (#747).
///
/// The probe and the run are separate operations, so a container created in
/// between still yields docker's raw exit-125 text without this. Anything that
/// is not a name conflict passes through untouched.
pub fn map_name_conflict(
    err: anyhow::Error,
    name: &str,
    port: Option<u16>,
    context: ConflictContext,
) -> anyhow::Error {
    // The whole chain, not just the outermost context: the docker text is the
    // root cause under a `starting runner container`-style wrapper.
    if !is_name_conflict_error(&format!("{err:#}")) {
        return err;
    }
    // An existing container without --replace is unconditionally a conflict, so
    // there is no plan to fall through to here.
    crate::exit::usage(
        plan_container_name(name, port, true, false, context)
            .expect_err("an existing container without --replace is always a conflict"),
    )
}

/// The last log lines of a container, for boot-failure diagnostics.
pub async fn container_logs(name_or_id: &str, tail: u32) -> String {
    let args: Vec<String> = vec![
        "logs".into(),
        "--tail".into(),
        tail.to_string(),
        name_or_id.to_string(),
    ];
    match Command::new("docker").args(&args).output().await {
        Ok(output) => format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
        Err(err) => format!("(could not read container logs: {err})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_removed_counts_actual_rm_output_not_the_candidate_set() {
        // `docker rm -f` echoes one removed id per line; that is the truthful
        // count. A container that vanished before rm simply is not echoed (#551).
        assert_eq!(count_removed("abc123\ndef456\n"), 2);
        // Trailing/blank lines and surrounding whitespace do not inflate it.
        assert_eq!(count_removed("  abc123  \n\n"), 1);
        // Nothing removed -> zero, not a phantom "removed 1".
        assert_eq!(count_removed(""), 0);
        assert_eq!(count_removed("\n   \n"), 0);
    }

    #[test]
    fn reap_report_clean_when_nothing_remains() {
        // Both candidates removed, re-list empty: a clean teardown, no error.
        let r = reap_report("abc123\ndef456\n", None, Vec::new());
        assert_eq!(r.removed, 2);
        assert!(r.still_present.is_empty());
        assert_eq!(r.error, None);
    }

    #[test]
    fn reap_report_discloses_a_partial_failure() {
        // rm exited nonzero and a container is still present: NOT a clean
        // teardown. The count reflects what was confirmed gone; the leftover and
        // the docker error are surfaced so `local down` cannot report success.
        let r = reap_report(
            "abc123\n",
            Some("docker rm -f exited 1: permission denied".into()),
            vec!["def456".into()],
        );
        assert_eq!(r.removed, 1);
        assert_eq!(r.still_present, vec!["def456".to_string()]);
        assert_eq!(
            r.error.as_deref(),
            Some("docker rm -f exited 1: permission denied")
        );
    }

    #[test]
    fn reap_report_treats_a_raced_away_container_as_clean() {
        // `docker rm -f` exited nonzero because a candidate vanished between the
        // list and the remove ("No such container"), but the re-list is empty:
        // nothing is actually left, so the teardown is clean despite the nonzero.
        let r = reap_report(
            "abc123\n",
            Some("docker rm -f exited 1: No such container: def456".into()),
            Vec::new(),
        );
        assert_eq!(r.removed, 1);
        assert!(r.still_present.is_empty());
        assert_eq!(r.error, None);
    }

    #[test]
    fn reap_report_flags_leftovers_even_without_an_rm_error() {
        // A container remains but `rm` reported success (e.g. it was recreated by
        // a restart policy): still disclosed with a generated message.
        let r = reap_report("abc123\n", None, vec!["ghi789".into()]);
        assert_eq!(r.removed, 1);
        assert_eq!(r.still_present, vec!["ghi789".to_string()]);
        assert!(r.error.unwrap().contains("still running"));
    }

    fn spec() -> StartSpec {
        StartSpec {
            image: "curie-runner".into(),
            container_name: "curie-runner-local".into(),
            host_port: 7245,
            plugin_dir: PathBuf::from("/tmp/deal-desk"),
            session_id: "local-1".into(),
            sandbox_id: "local".into(),
            budget_json: r#"{"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}"#.into(),
            fake_model: true,
            network: Some("curie_default".into()),
            otel_endpoint: Some("http://otel-collector:4318".into()),
            model_base_url: None,
            model: None,
            passthrough_env: vec!["CURIE_TEST_ENV_THAT_DOES_NOT_EXIST".into()],
            docker_env: vec![],
        }
    }

    #[test]
    fn ollama_run_args_mount_the_model_cache_volume() {
        let args = ollama_run_args("curie-ollama", "curie-net", "ollama/ollama:0.24.0");
        let joined = args.join(" ");
        assert!(joined.starts_with("run -d --name curie-ollama --network curie-net"));
        assert!(joined.contains("-v curie-ollama-data:/root/.ollama"));
        assert_eq!(args.last().unwrap(), "ollama/ollama:0.24.0");
        assert_eq!(ollama_volume("curie-ollama"), "curie-ollama-data");
    }

    // ADR 0093. The manifest path IS the "is this model already pulled" test, so
    // getting the naming wrong would report a cached model as missing (a false
    // refusal) or a missing one as cached (the implicit download this closes).
    // Verified against a real volume: qwen3:4b lives at
    // models/manifests/registry.ollama.ai/library/qwen3/4b.
    // #1254. The preflight exists to stop an implicit multi-GB download, and a
    // reference with shell metacharacters defeated it: `missing; true #` ended the
    // probe's `test -e` early and left a `true`, so the probe exited 0, the guard
    // reported the model present, and `compose up` performed the download.
    #[test]
    fn a_model_ref_with_shell_metacharacters_is_rejected() {
        for payload in [
            "missing; true #",     // the reported payload, verbatim
            "$(touch /tmp/pwned)", // command substitution
            "`id`",                // the backtick spelling of the same
            "a && b",
            "a | b",
            "a > /tmp/x",
            "a\nb",
            "a b", // a bare space is not a reference either
            "'quoted'",
            "a\\b",
        ] {
            assert!(
                validate_model_ref(payload).is_err(),
                "must reject {payload:?}"
            );
        }
    }

    // The refusal has to be readable: an operator who typed something odd needs to
    // know which character and what the grammar is, not just that it failed.
    #[test]
    fn the_rejection_names_the_offending_character_and_the_grammar() {
        let err = validate_model_ref("missing; true #")
            .unwrap_err()
            .to_string();
        assert!(err.contains("missing; true #"), "{err}");
        assert!(
            err.contains(';'),
            "the offending character must be named: {err}"
        );
        assert!(
            err.contains("qwen3:4b"),
            "the grammar needs an example: {err}"
        );
    }

    // The allowlist must not be so tight it rejects references Ollama accepts --
    // a validator that refuses real input is its own outage.
    #[test]
    fn every_shape_ollama_accepts_still_validates() {
        for good in [
            "qwen3",
            "qwen3:4b",
            "qwen3-coder:30b",
            "deepseek-v4-flash-0731",
            "myorg/mymodel:v1",
            "hf.co/org/repo:q4",
            "localhost:5000/org/repo",
            "localhost:5000/org/repo:v2",
            "a_b.c-d:e_f.g-h",
        ] {
            assert!(validate_model_ref(good).is_ok(), "must accept {good:?}");
        }
    }

    #[test]
    fn empty_refs_and_empty_segments_are_rejected() {
        for bad in ["", ":", "qwen3:", "/qwen3", "a//b:1"] {
            assert!(validate_model_ref(bad).is_err(), "must reject {bad:?}");
        }
    }

    // #1363. Every character in `..` is on the allowlist, so the #1254 grammar
    // waved it through -- but `..` is a path OPERATOR, not a name, and the probe's
    // `test -e` resolves it against the real filesystem. That let any existing
    // path answer "the model is already pulled" and skip the disclosure ADR-0093
    // calls the ONLY place the download is named before it would be spent.
    #[test]
    fn a_model_ref_whose_segments_are_path_operators_is_rejected() {
        for payload in [
            "../../../../etc:hostname", // the reported payload, verbatim
            "..:..",                    // both halves, the minimal spelling
            "x/../../../../../etc:passwd",
            "..",          // a bare traversal with the implicit `latest` tag
            "qwen3:..",    // only the tag is an operator
            "../qwen3:4b", // only a leading name segment is
            ".",           // `.` resolves to the parent dir, which also exists
            "qwen3:.",
        ] {
            assert!(
                validate_model_ref(payload).is_err(),
                "must reject {payload:?}"
            );
        }
    }

    // A segment made only of punctuation is never a model name, and each spelling
    // of it is another way to reach the same escape. The rule is positive -- a
    // name has to contain a letter or a digit -- so it holds for spellings nobody
    // has thought of yet, which a denylist of `.`/`..` would not.
    #[test]
    fn a_segment_with_no_letter_or_digit_is_rejected() {
        for payload in ["...", "-", "_", "._-", "qwen3:...", "a/-/b:1"] {
            assert!(
                validate_model_ref(payload).is_err(),
                "must reject {payload:?}"
            );
        }
    }

    // The refusal has to tell the operator WHICH segment and WHY, or they will
    // retype the same thing. A traversal is not a typo they can see.
    #[test]
    fn the_traversal_rejection_names_the_segment_and_the_reason() {
        let err = validate_model_ref("../../../../etc:hostname")
            .unwrap_err()
            .to_string();
        assert!(err.contains("../../../../etc:hostname"), "{err}");
        assert!(
            err.contains("letter") || err.contains("digit"),
            "the rule the segment broke must be stated: {err}"
        );
    }

    // The containment check is the structural half, and it is checked on the
    // COMPOSED path, so it holds for any spelling the grammar might still admit.
    #[test]
    fn a_composed_manifest_path_that_escapes_is_not_contained() {
        for escaping in [
            "models/manifests/../../../../etc/hostname",
            "models/manifests/registry.ollama.ai/library/../..", // lands ON the prefix
            "models/manifests",                                  // the prefix itself
            "models/manifests/",
            "etc/hostname",
        ] {
            assert!(
                !manifest_path_is_contained(escaping),
                "must not accept {escaping:?}"
            );
        }
        for real in [
            "models/manifests/registry.ollama.ai/library/qwen3/4b",
            "models/manifests/localhost:5000/org/repo/latest",
        ] {
            assert!(manifest_path_is_contained(real), "must accept {real:?}");
        }
    }

    // `models/manifests` itself exists in any seeded volume, so a payload that
    // merely climbs back TO the prefix still answers `test -e` with 0. Landing on
    // the prefix has to count as an escape, not as containment.
    #[test]
    fn climbing_back_to_the_prefix_does_not_count_as_contained() {
        // This is exactly what `--local-model '..:..'` composes.
        let path = model_manifest_path("..:..");
        assert_eq!(path, "models/manifests/registry.ollama.ai/library/../..");
        assert!(!manifest_path_is_contained(&path));
    }

    // The probe boundary refuses too, not just the argument boundary. #1332's own
    // doc comment claimed the hole was "closed by construction rather than by the
    // validator agreeing to be perfect", and #1363 was the validator not being
    // perfect -- so the guarantee has to hold for a caller that never validated.
    #[test]
    fn the_probe_refuses_to_build_argv_for_a_path_that_escapes() {
        let err = model_probe_args("vol", "img", "../../../../etc:hostname")
            .expect_err("an escaping ref must not produce probe argv")
            .to_string();
        assert!(err.contains("models/manifests"), "{err}");
    }

    // The structural half of the fix, independent of the validator: the probe
    // passes the path as a POSITIONAL ARGUMENT, so even a caller that skipped
    // validation cannot get the payload reparsed as shell code.
    #[test]
    fn the_probe_passes_the_path_as_an_argument_not_as_command_text() {
        // The #1254 payload carried all the way into the argv the probe would run.
        let args = model_probe_args("vol", "img", "missing; true #")
            .expect("this payload is inert, not a traversal; containment is a separate test");
        let command = &args[args.len() - 3];
        let path = args.last().expect("the path is the final argument");

        assert_eq!(command, r#"test -e "$1""#, "the command must reference $1");
        assert!(
            !command.contains("missing"),
            "the payload must never reach the command string: {command}"
        );
        assert!(
            path.contains("missing; true #"),
            "the payload belongs in the path argument, verbatim: {path}"
        );
        // Every byte of the payload sits inside ONE argv element, which is what
        // makes it inert: `sh` never reparses a positional parameter as code.
        assert_eq!(args.iter().filter(|a| a.contains("true #")).count(), 1);
    }

    #[test]
    fn model_manifest_path_mirrors_ollama_layout() {
        assert_eq!(
            model_manifest_path("qwen3:4b"),
            "models/manifests/registry.ollama.ai/library/qwen3/4b"
        );
        assert_eq!(
            model_manifest_path("qwen3-coder:30b"),
            "models/manifests/registry.ollama.ai/library/qwen3-coder/30b"
        );
        // No tag is `latest`, the same default `ollama pull` applies.
        assert_eq!(
            model_manifest_path("qwen3"),
            "models/manifests/registry.ollama.ai/library/qwen3/latest"
        );
        // A namespace displaces `library`, not the registry.
        assert_eq!(
            model_manifest_path("myorg/mymodel:v1"),
            "models/manifests/registry.ollama.ai/myorg/mymodel/v1"
        );
        // Three or more segments carry their own registry host.
        assert_eq!(
            model_manifest_path("hf.co/org/repo:q4"),
            "models/manifests/hf.co/org/repo/q4"
        );
    }

    // A colon in a registry host's PORT is not a tag. Splitting on the first
    // colon instead of the last would silently look for a model named after the
    // host, always miss, and refuse a machine that has the model.
    #[test]
    fn model_manifest_path_does_not_mistake_a_host_port_for_a_tag() {
        assert_eq!(
            model_manifest_path("localhost:5000/org/repo"),
            "models/manifests/localhost:5000/org/repo/latest"
        );
        assert_eq!(
            model_manifest_path("localhost:5000/org/repo:v2"),
            "models/manifests/localhost:5000/org/repo/v2"
        );
    }

    // The refusal message is the ONLY place the download is disclosed before it
    // would be spent (ADR 0093), which makes its content load-bearing: each
    // missing asset named separately, the size stated, and a runnable fix. A
    // present asset must NOT be listed, or the operator fetches more than needed.
    #[test]
    fn missing_assets_message_names_only_what_is_missing_and_how_to_get_it() {
        let both = missing_local_model_assets_message(
            "ollama/ollama:0.24.0",
            true,
            "qwen3:4b",
            true,
            "curie local up --local-model qwen3:4b --pull-model",
        );
        assert!(both.contains("does not download them implicitly"), "{both}");
        assert!(both.contains("ollama/ollama:0.24.0"), "{both}");
        assert!(both.contains(OLLAMA_IMAGE_DOWNLOAD_SIZE), "{both}");
        assert!(both.contains("qwen3:4b"), "{both}");
        assert!(
            both.contains("curie local up --local-model qwen3:4b --pull-model"),
            "{both}"
        );

        // Image cached, model not: the image line must be absent so the operator
        // is not told to re-fetch 8.9 GB they already have.
        let model_only = missing_local_model_assets_message(
            "ollama/ollama:0.24.0",
            false,
            "qwen3-coder:30b",
            true,
            "curie local up --local-model qwen3-coder:30b --pull-model",
        );
        assert!(!model_only.contains("docker image"), "{model_only}");
        assert!(
            !model_only.contains(OLLAMA_IMAGE_DOWNLOAD_SIZE),
            "{model_only}"
        );
        assert!(model_only.contains("qwen3-coder:30b"), "{model_only}");

        // And the mirror case: image missing, model already cached.
        let image_only = missing_local_model_assets_message(
            "ollama/ollama:0.24.0",
            true,
            "qwen3:4b",
            false,
            "curie skill up --local-model qwen3:4b --pull-model --name demo",
        );
        assert!(image_only.contains("docker image"), "{image_only}");
        assert!(
            !image_only.contains("size depends on the model"),
            "{image_only}"
        );
    }

    // The remediation must be a `curie` command. CLAUDE.md's single-surface rule
    // forbids sending an operator to a raw docker invocation, and this message is
    // the most tempting place in the codebase to break it.
    #[test]
    fn missing_assets_message_never_hands_out_a_raw_docker_command() {
        let msg = missing_local_model_assets_message(
            "ollama/ollama:0.24.0",
            true,
            "qwen3:4b",
            true,
            "curie local up --local-model qwen3:4b --pull-model",
        );
        let fix = msg
            .lines()
            .find(|l| l.trim_start().starts_with("curie ") || l.trim_start().starts_with("docker "))
            .expect("the message must offer a runnable fix");
        assert!(
            fix.trim_start().starts_with("curie "),
            "the fix must be a curie command, got: {fix}"
        );
        assert!(!msg.contains("docker pull"), "{msg}");
        assert!(!msg.contains("docker run"), "{msg}");
    }

    // The prose satisfies a human; an agent branches on the ADR-0021 `fix`
    // field, which a bare `bail!` leaves null (#1261). Asserting on Some(fix)
    // is what arms this test: reverting this guard's body to
    // `bail!(missing_local_model_assets_message(...))` makes `classify` return
    // (Failure, None) and the `expect` below fails.
    #[test]
    fn the_missing_assets_refusal_carries_the_fetch_command_as_its_fix() {
        let hint = "curie local up --local-model qwen3:4b --pull-model";
        let err =
            refuse_missing_local_model_assets("ollama/ollama:0.24.0", true, "qwen3:4b", true, hint)
                .unwrap_err();
        let (class, fix) = crate::exit::classify(&err);
        // Exit 1, not 2: the argv was well-formed and re-running it after the
        // fetch succeeds, which exit 2 would deny.
        assert_eq!(class, crate::exit::ExitClass::Failure);
        let fix = fix.expect("the refusal must carry a fix, not a null one");
        assert!(fix.contains(hint), "the fix must be runnable: {fix}");
        // Separate from the prose: dumping the whole message into `fix` would
        // also contain the hint, so pin that the lead-in is NOT in there.
        assert!(
            !fix.contains("local model assets are not on this machine"),
            "the fix must be the command alone, not the prose: {fix}"
        );

        let json = crate::exit::error_json(&err);
        assert!(
            json["fix"] != serde_json::Value::Null,
            "the rendered payload must not have a null fix: {json}"
        );
        assert!(
            json["error"]
                .as_str()
                .expect("error is a string")
                .contains("does not download them implicitly"),
            "the prose still rides the error field: {json}"
        );

        // Positive control: an always-Err guard would also pass every
        // assertion above, so pin that the guard refuses ONLY when something
        // is actually missing.
        assert!(refuse_missing_local_model_assets(
            "ollama/ollama:0.24.0",
            false,
            "qwen3:4b",
            false,
            hint,
        )
        .is_ok());
    }

    #[test]
    fn ollama_run_args_label_the_sidecar_as_cli_managed() {
        // The sidecar is CLI-booted and removable by name, so it must be
        // identifiable as ours (#747) or `skill down --name <bundle>-ollama`
        // warns about a container this feature itself created.
        let joined = ollama_run_args("curie-ollama", "curie-net", "ollama/ollama:0.24.0").join(" ");
        assert!(
            joined.contains("--label curietech.ai/managed-by=curie-cli"),
            "{joined}"
        );
        // Still outside the worker substrate's reap set.
        assert!(!joined.contains(SANDBOX_LABEL), "{joined}");
    }

    #[test]
    fn run_args_carry_the_aci_boot_env() {
        let args = spec().run_args();
        let joined = args.join(" ");
        assert!(joined.starts_with("run -d --name curie-runner-local -p 7245:8080"));
        assert!(joined.contains("-v /tmp/deal-desk:/plugin:ro"));
        assert!(joined.contains("-e CURIE_PLUGIN_DIR=/plugin"));
        assert!(joined.contains("-e CURIE_SESSION_ID=local-1"));
        assert!(joined.contains("-e CURIE_SANDBOX_ID=local"));
        assert!(joined.contains(
            "-e CURIE_BUDGET={\"max_output_tokens_per_run\":100000,\"max_usd_per_day\":5.0}"
        ));
        assert!(joined.contains("-e CURIE_FAKE_MODEL=1"));
        assert!(joined.contains("--network curie_default"));
        assert!(joined.contains("-e OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318"));
        assert_eq!(args.last().unwrap(), "curie-runner");
    }

    /// #1087: the runner mounts whatever `plugin_dir` the spec carries, and
    /// `skill up` now carries the materialized snapshot rather than the editable
    /// source. Asserted on the argv the user's Docker daemon actually receives:
    /// exactly one `-v ...:/plugin:ro`, naming the snapshot, with the bare source
    /// path never mounted.
    #[test]
    fn run_args_mount_whatever_plugin_dir_the_spec_carries() {
        let snapshot = format!("/tmp/deal-desk/.curie/snapshots/{}", "b".repeat(64));
        let args = StartSpec {
            plugin_dir: PathBuf::from(&snapshot),
            ..spec()
        }
        .run_args();

        let mounts: Vec<String> = args
            .windows(2)
            .filter(|pair| pair[0] == "-v")
            .map(|pair| pair[1].clone())
            .collect();
        assert_eq!(
            mounts,
            vec![format!("{snapshot}:/plugin:ro")],
            "the only bundle mount must be the snapshot: {args:?}"
        );
        assert!(
            !args.join(" ").contains("-v /tmp/deal-desk:/plugin:ro"),
            "the editable source must never be mounted alongside it: {args:?}"
        );
    }

    #[test]
    fn run_args_apply_container_hardening() {
        // Every local runner boots hardened (#631): read-only rootfs with tmpfs
        // for /tmp and HOME, all caps dropped, and no privilege escalation.
        let joined = spec().run_args().join(" ");
        assert!(joined.contains("--read-only"), "{joined}");
        assert!(joined.contains("--tmpfs /tmp:rw,mode=1777"), "{joined}");
        assert!(
            joined.contains("--tmpfs /home/runner:rw,mode=1777"),
            "{joined}"
        );
        assert!(joined.contains("--cap-drop ALL"), "{joined}");
        assert!(
            joined.contains("--security-opt no-new-privileges"),
            "{joined}"
        );
        // The default seccomp profile is left active -- never disabled.
        assert!(!joined.contains("seccomp=unconfined"), "{joined}");
    }

    #[test]
    fn run_args_label_the_container_as_cli_managed() {
        // CLI-booted runners are identifiable as ours (#747)...
        let joined = spec().run_args().join(" ");
        assert!(
            joined.contains("--label curietech.ai/managed-by=curie-cli"),
            "{joined}"
        );
        assert!(
            joined.contains("--label curietech.ai/component=runner"),
            "{joined}"
        );
        // ...without joining the worker substrate's reap set, which `local down`
        // clears by SANDBOX_LABEL.
        assert!(!joined.contains(SANDBOX_LABEL), "{joined}");
    }

    #[test]
    fn plan_container_name_proceeds_when_the_name_is_free() {
        assert_eq!(
            plan_container_name(
                "curie-runner-local",
                Some(7245),
                false,
                false,
                ConflictContext::SkillUp
            ),
            Ok(NamePlan::Proceed)
        );
        assert_eq!(
            plan_container_name(
                "curie-runner-local",
                Some(7245),
                false,
                true,
                ConflictContext::SkillUp
            ),
            Ok(NamePlan::Proceed)
        );
    }

    #[test]
    fn plan_container_name_replaces_only_when_asked() {
        assert_eq!(
            plan_container_name(
                "curie-runner-local",
                Some(7245),
                true,
                true,
                ConflictContext::SkillUp
            ),
            Ok(NamePlan::Replace)
        );
    }

    #[test]
    fn name_conflict_names_the_container_and_every_remedy() {
        // The operator must never meet docker's raw exit-125 conflict here
        // (#747): the error names the container and all three ways out.
        let err = plan_container_name(
            "curie-runner-local",
            Some(7245),
            true,
            false,
            ConflictContext::SkillUp,
        )
        .expect_err("an existing container without --replace must fail");
        assert!(err.contains("curie-runner-local"), "{err}");
        assert!(err.contains("--replace"), "{err}");
        assert!(
            err.contains("curie skill down --name curie-runner-local"),
            "{err}"
        );
        assert!(err.contains("--name"), "{err}");
        assert!(err.contains("--port"), "{err}");
        assert!(err.contains("7245"), "{err}");
        // Docker's own wording never reaches the user.
        assert!(!err.contains("125"), "{err}");
        assert!(!err.contains("already in use"), "{err}");
    }

    #[test]
    fn name_conflict_covers_the_eval_sweep_containers_too() {
        // Sibling-path parity (#747), but only with remedies `skill eval`
        // actually has: it accepts no --replace, no --name and no --port, so
        // offering them would send the operator down a dead end.
        let err = plan_container_name(
            "curie-eval-sweep-0",
            Some(7345),
            true,
            false,
            ConflictContext::EvalSweep,
        )
        .expect_err("an existing container without --replace must fail");
        assert!(err.contains("curie-eval-sweep-0"), "{err}");
        assert!(err.contains("7345"), "{err}");
        assert!(
            err.contains("curie skill down --name curie-eval-sweep-0"),
            "{err}"
        );
        assert!(!err.contains("--replace"), "{err}");
        assert!(!err.contains("--port"), "{err}");
        assert!(!err.contains("125"), "{err}");
    }

    #[test]
    fn a_container_with_no_host_port_is_not_offered_the_port_knob() {
        // The local-model sidecar publishes no host port, so `--port` is not the
        // knob that moves it out of the way; naming it would misdirect (#747).
        let err = plan_container_name(
            "curie-runner-local-ollama",
            None,
            true,
            false,
            ConflictContext::SkillUp,
        )
        .expect_err("an existing container without --replace must fail");
        assert!(err.contains("curie-runner-local-ollama"), "{err}");
        assert!(err.contains("--replace"), "{err}");
        assert!(
            err.contains("curie skill down --name curie-runner-local-ollama"),
            "{err}"
        );
        // No port clause at all, not just no `--port` flag.
        assert!(!err.contains("port"), "{err}");
    }

    #[test]
    fn a_lost_name_conflict_race_maps_onto_the_same_actionable_error() {
        // The probe and `docker run` are separate operations, so a container
        // created in between yields docker's raw exit-125 text. That is the exact
        // surface this change removes, so the race must land on the same message
        // (#747).
        let raw = anyhow::anyhow!(
            "docker run failed (exit status: 125): docker: Error response from daemon: Conflict. \
The container name \"/curie-runner-local\" is already in use by container \"9f2c\"."
        );
        assert!(is_name_conflict_error(&raw.to_string()));
        let mapped = map_name_conflict(
            raw.context("starting runner container"),
            "curie-runner-local",
            Some(7245),
            ConflictContext::SkillUp,
        );
        let text = format!("{mapped:#}");
        assert!(text.contains("--replace"), "{text}");
        assert!(
            text.contains("curie skill down --name curie-runner-local"),
            "{text}"
        );
        assert!(!text.contains("125"), "{text}");
        assert!(!text.contains("already in use"), "{text}");
    }

    #[test]
    fn a_failure_that_is_not_a_name_conflict_passes_through_untouched() {
        // Only the conflict is rewritten; every other docker failure keeps its
        // own diagnosis rather than being mislabeled as a leftover container.
        let raw = anyhow::anyhow!(
            "docker run failed (exit status: 125): docker: Error response from daemon: \
driver failed programming external connectivity: port is already allocated."
        );
        assert!(!is_name_conflict_error(&raw.to_string()));
        let mapped = map_name_conflict(
            raw,
            "curie-runner-local",
            Some(7245),
            ConflictContext::SkillUp,
        );
        let text = format!("{mapped:#}");
        assert!(text.contains("port is already allocated"), "{text}");
        assert!(!text.contains("--replace"), "{text}");
    }

    #[test]
    fn check_run_args_are_isolated_and_hardened() {
        // The offline MCP-load check runs untrusted bundle code, so it stays
        // network-isolated AND carries the same container hardening.
        let spec = CheckSpec {
            image: "curie-runner".into(),
            plugin_dir: "/tmp/deal-desk".into(),
            timeout_s: 30,
        };
        let joined = spec.run_args().join(" ");
        assert!(joined.contains("--network none"), "{joined}");
        assert!(joined.contains("--read-only"), "{joined}");
        assert!(joined.contains("--cap-drop ALL"), "{joined}");
        assert!(
            joined.contains("--security-opt no-new-privileges"),
            "{joined}"
        );
        assert!(
            joined.trim_end().ends_with("curie_runner.check"),
            "{joined}"
        );
    }

    #[test]
    fn unset_passthrough_env_is_not_forwarded_and_real_model_omits_fake_flag() {
        let mut s = spec();
        s.fake_model = false;
        let joined = s.run_args().join(" ");
        assert!(!joined.contains("CURIE_FAKE_MODEL"));
        assert!(!joined.contains("CURIE_TEST_ENV_THAT_DOES_NOT_EXIST"));
    }

    #[test]
    fn docker_env_marks_passthrough_name_without_leaking_value() {
        let mut s = spec();
        s.passthrough_env = vec!["GITHUB_PERSONAL_ACCESS_TOKEN".into()];
        s.docker_env = vec![("GITHUB_PERSONAL_ACCESS_TOKEN".into(), "ghp-secret".into())];
        let joined = s.run_args().join(" ");
        assert!(joined.contains("-e GITHUB_PERSONAL_ACCESS_TOKEN"));
        assert!(!joined.contains("ghp-secret"));
    }

    #[test]
    fn model_is_forwarded_only_when_set() {
        let mut s = spec();
        s.model = None;
        assert!(!s.run_args().join(" ").contains("CURIE_MODEL"));

        s.model = Some("claude-opus-4-8".into());
        assert!(s
            .run_args()
            .join(" ")
            .contains("-e CURIE_MODEL=claude-opus-4-8"));
    }

    #[test]
    fn model_base_url_is_forwarded_when_set() {
        let mut s = spec();
        s.model_base_url = Some("http://x-ollama:11434".into());
        assert!(s
            .run_args()
            .join(" ")
            .contains("-e ANTHROPIC_BASE_URL=http://x-ollama:11434"));
    }

    #[test]
    fn model_base_url_is_omitted_when_unset() {
        let mut s = spec();
        s.model_base_url = None;
        assert!(!s.run_args().join(" ").contains("ANTHROPIC_BASE_URL"));
    }

    #[test]
    fn real_model_with_model_base_url_still_omits_fake_flag() {
        let mut s = spec();
        s.fake_model = false;
        s.model_base_url = Some("http://x-ollama:11434".into());
        let joined = s.run_args().join(" ");
        assert!(joined.contains("-e ANTHROPIC_BASE_URL=http://x-ollama:11434"));
        assert!(!joined.contains("CURIE_FAKE_MODEL"));
    }
}

// ---------------------------------------------------------------------------
// Connector containers (ADR 0113): the skill tier's emitter
// ---------------------------------------------------------------------------

/// The component label every connector container carries, in both the skill
/// start path and the compose overlay. Teardown resolves containers by label,
/// never by a generated file, so a container that is not labeled at creation
/// can never be reaped.
pub const CONNECTOR_COMPONENT_LABEL: &str = "curietech.ai/component=connector";

/// The per-stack label that keeps two concurrent bring-ups apart: the compose
/// project at the local tier, the runner session id at the skill tier.
pub fn connector_project_label(project: &str) -> String {
    format!("curietech.ai/project={project}")
}

/// The agent a connector container belongs to.
///
/// The project label is not enough to name one agent's containers at the local
/// tier: every locally deployed agent shares the single `curie` compose project
/// with the api/worker stack, so reaping by project alone would take out another
/// agent's connectors (and `--remove-orphans` would take out the stack itself).
pub const CONNECTOR_AGENT_LABEL_KEY: &str = "curietech.ai/agent";

/// Which declared connector a container is, as the object name both emitters
/// derive from `(release, agent, connector)`. This is what a redeploy compares
/// against the new desired set.
pub const CONNECTOR_OBJECT_LABEL_KEY: &str = "curietech.ai/connector";

/// The `label=key=value` filter that selects one agent's connector containers.
pub fn connector_agent_label(agent: &str) -> String {
    format!("{CONNECTOR_AGENT_LABEL_KEY}={agent}")
}

/// The identity labels every connector container carries, from ONE definition:
/// the skill tier joins them into `--label key=value`, the compose overlay
/// writes them as map entries. A container that is not labeled at creation can
/// never be reconciled, so the two emitters cannot be allowed to drift.
pub fn connector_identity_labels(agent: &str, object: &str) -> Vec<(String, String)> {
    vec![
        (CONNECTOR_AGENT_LABEL_KEY.to_string(), agent.to_string()),
        (CONNECTOR_OBJECT_LABEL_KEY.to_string(), object.to_string()),
    ]
}

/// One connector container Docker currently reports for an agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunningConnector {
    pub id: String,
    /// The object name its [`CONNECTOR_OBJECT_LABEL_KEY`] label carries. Empty
    /// for a container an older CLI started without the label.
    pub object: String,
}

/// List THIS agent's connector containers, id and object name.
///
/// Three filters, all of them load-bearing: the component label excludes the
/// runner and the sandbox, the project label excludes another stack, and the
/// agent label excludes another agent deployed into this same compose project.
pub fn agent_connectors_argv(project: &str, agent: &str) -> Vec<String> {
    vec![
        "ps".into(),
        "-a".into(),
        "--filter".into(),
        format!("label={CONNECTOR_COMPONENT_LABEL}"),
        "--filter".into(),
        format!("label={}", connector_project_label(project)),
        "--filter".into(),
        format!("label={}", connector_agent_label(agent)),
        "--format".into(),
        format!("{{{{.ID}}}}\t{{{{.Label \"{CONNECTOR_OBJECT_LABEL_KEY}\"}}}}"),
    ]
}

/// Parse [`agent_connectors_argv`]'s output.
pub fn parse_agent_connectors(listing: &str) -> Vec<RunningConnector> {
    listing
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (id, object) = line.split_once('\t').unwrap_or((line, ""));
            RunningConnector {
                id: id.trim().to_string(),
                object: object.trim().to_string(),
            }
        })
        .collect()
}

/// Which of this agent's running connector containers a redeploy must remove:
/// exactly those the new bundle no longer declares.
///
/// Pure, so the scope decision is testable without a daemon -- and the scope IS
/// the decision. The input is already narrowed to one agent's containers by
/// [`agent_connectors_argv`], so nothing here can reach the main stack or
/// another agent; all this adds is "and it is not in the new desired set". A
/// container carrying no object label (started before the label existed) matches
/// no desired name and is removed, which is correct either way: if the bundle
/// still declares it, the bring-up that follows recreates it labeled.
pub fn connectors_to_reap(
    running: &[RunningConnector],
    desired: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    running
        .iter()
        .filter(|container| !desired.contains(&container.object))
        .map(|container| container.id.clone())
        .collect()
}

/// Reconcile one agent's connector containers against what the bundle now
/// declares, returning what it could not do.
///
/// Best-effort and warning-shaped, the same choice [`run_connector_teardown`]
/// makes: a redeploy that refuses to finish because a leftover container would
/// not die leaves the operator worse off than one that says so and continues.
pub async fn reap_undesired_connectors(
    project: &str,
    agent: &str,
    desired: &std::collections::BTreeSet<String>,
) -> Vec<String> {
    let listing = match docker(&agent_connectors_argv(project, agent)).await {
        Ok(listing) => listing,
        Err(err) => {
            return vec![format!(
                "could not list the connector containers already running for '{agent}': {err}"
            )]
        }
    };
    let stale = connectors_to_reap(&parse_agent_connectors(&listing), desired);
    if stale.is_empty() {
        return Vec::new();
    }
    let mut rm: Vec<String> = vec!["rm".into(), "-f".into()];
    rm.extend(stale);
    match docker(&rm).await {
        Ok(_) => Vec::new(),
        Err(err) => vec![format!(
            "could not remove the connector containers this bundle no longer declares: {err}"
        )],
    }
}

/// The uid a connector container runs as, mirroring `render_deployment`'s
/// `runAsUser`.
const CONNECTOR_UID: &str = "65532:65532";

/// Everything `docker run` needs to start one hosted connector.
///
/// Field for field the container `connector_render.render_deployment` renders:
/// substituted `args`, substituted `env`, declared secrets, `secret_files`, the
/// hardening set and the port. An approximation does not degrade gracefully --
/// dropping `args` silently restores a server's default tool surface.
#[derive(Debug, Clone)]
pub struct ConnectorStartSpec {
    pub image: String,
    pub container_name: String,
    pub network: String,
    /// The Service DNS name the runner independently derives and dials.
    pub alias: String,
    pub args: Vec<String>,
    /// Substituted `env` entries, forwarded as `-e NAME=VALUE`.
    pub env: Vec<(String, String)>,
    /// Declared secret NAMES, forwarded as a bare `-e NAME`.
    pub secret_names: Vec<String>,
    /// `host:container:ro` bind mounts for the staged credential files.
    pub mounts: Vec<String>,
    pub labels: Vec<String>,
    /// Resolved secret values handed to the Docker CLI child process only, so
    /// they never enter argv. The same rule [`StartSpec::docker_env`] follows.
    pub docker_env: Vec<(String, String)>,
}

impl ConnectorStartSpec {
    /// Resolve one declared connector into the container that runs it.
    #[allow(clippy::too_many_arguments)]
    pub fn from_declaration(
        connector: &str,
        spec: &crate::connector_build::ConnectorSpecDecl,
        image: &str,
        identity: &crate::connector_build::ConnectorScope,
        network: &str,
        project: &str,
        plugin_dir: &std::path::Path,
        secret_values: &std::collections::BTreeMap<String, String>,
    ) -> Result<Self> {
        crate::connector_build::refuse_out_of_band_secrets(connector, spec)?;
        let subs = crate::connector_build::connector_substitutions(identity, connector, spec.port);
        let secret_names = crate::connector_build::declared_secret_names(spec);
        let docker_env: Vec<(String, String)> = secret_names
            .iter()
            .filter_map(|name| {
                secret_values
                    .get(name)
                    .map(|value| (name.clone(), value.clone()))
            })
            .collect();
        Ok(Self {
            image: image.to_string(),
            container_name: format!("curie-connector-{project}-{connector}"),
            network: network.to_string(),
            alias: crate::connector_build::service_dns(
                &identity.release,
                &identity.agent,
                connector,
                &identity.namespace,
            ),
            args: spec
                .args
                .iter()
                .map(|arg| crate::connector_build::substitute(arg, &subs))
                .collect(),
            env: spec
                .env
                .iter()
                .map(|(key, value)| {
                    (
                        key.clone(),
                        crate::connector_build::substitute(value, &subs),
                    )
                })
                .collect(),
            secret_names,
            mounts: spec
                .secret_files
                .values()
                .map(|declared_path| {
                    format!(
                        "{}:{declared_path}:ro",
                        crate::connector_build::staged_secret_path(
                            plugin_dir,
                            connector,
                            declared_path
                        )
                        .display()
                    )
                })
                .collect(),
            labels: {
                let mut labels = vec![
                    CLI_MANAGED_LABEL.to_string(),
                    CONNECTOR_COMPONENT_LABEL.to_string(),
                    connector_project_label(project),
                ];
                labels.extend(
                    connector_identity_labels(
                        &identity.agent,
                        &crate::connector_build::object_name(
                            &identity.release,
                            &identity.agent,
                            connector,
                        ),
                    )
                    .into_iter()
                    .map(|(key, value)| format!("{key}={value}")),
                );
                labels
            },
            docker_env,
        })
    }

    /// The `docker run` argument vector (after the `docker` executable).
    ///
    /// No `-p`, ever: the runner reaches the connector over the private network
    /// by alias, so a published port would collide between two concurrent
    /// bundles and expose a credential-holding server on the host.
    pub fn run_args(&self) -> Vec<String> {
        let mut args: Vec<String> = vec![
            "run".into(),
            "-d".into(),
            "--name".into(),
            self.container_name.clone(),
            "--network".into(),
            self.network.clone(),
            "--network-alias".into(),
            self.alias.clone(),
        ];
        // Hardened by construction, exactly as the rendered pod is. The bundle
        // author never writes this, so the author cannot omit it.
        args.extend([
            "--read-only".to_string(),
            "--cap-drop".to_string(),
            "ALL".to_string(),
            "--security-opt".to_string(),
            "no-new-privileges".to_string(),
            "--user".to_string(),
            CONNECTOR_UID.to_string(),
        ]);
        for label in &self.labels {
            args.push("--label".into());
            args.push(label.clone());
        }
        for (key, value) in &self.env {
            args.push("-e".into());
            args.push(format!("{key}={value}"));
        }
        // The bare NAME: the value travels to the Docker CLI child's
        // environment instead, so it never lands in `ps -ef`.
        for name in &self.secret_names {
            args.push("-e".into());
            args.push(name.clone());
        }
        for mount in &self.mounts {
            args.push("-v".into());
            args.push(mount.clone());
        }
        args.push(self.image.clone());
        args.extend(self.args.iter().cloned());
        args
    }
}

/// One step of a connector teardown, in the order it must run.
#[derive(Debug, Clone)]
pub enum ConnectorTeardownStep {
    /// List the containers this project's connectors created, by label.
    ReapLabeled(crate::ops::OpsCommand),
    RemoveNetwork(crate::ops::OpsCommand),
    WipeSecrets(PathBuf),
}

/// Containers first, then the network, then the staged credential tree.
///
/// The order is the property: a network still attached to a running container
/// cannot be removed, and a tree still bind-mounted into one cannot be wiped
/// cleanly. Reaping is label-scoped rather than file-scoped because a `down`
/// run from another directory carries no plugin directory and must still reap.
pub fn connector_teardown_plan(
    project: &str,
    network: Option<&str>,
    plugin_dir: Option<&std::path::Path>,
) -> Vec<ConnectorTeardownStep> {
    let mut steps = vec![ConnectorTeardownStep::ReapLabeled(
        crate::connector_build::plain_command(
            "docker",
            vec![
                "ps".into(),
                "-a".into(),
                "-q".into(),
                "--filter".into(),
                format!("label={CONNECTOR_COMPONENT_LABEL}"),
                "--filter".into(),
                format!("label={}", connector_project_label(project)),
            ],
        ),
    )];
    if let Some(network) = network {
        steps.push(ConnectorTeardownStep::RemoveNetwork(
            crate::connector_build::plain_command(
                "docker",
                vec!["network".into(), "rm".into(), network.to_string()],
            ),
        ));
    }
    if let Some(plugin_dir) = plugin_dir {
        steps.push(ConnectorTeardownStep::WipeSecrets(
            crate::connector_build::connector_secrets_root(plugin_dir),
        ));
    }
    steps
}

/// Run a connector teardown plan, tolerating what is already gone.
///
/// Every failure is a warning rather than a hard stop: a teardown that refuses
/// to finish leaves the operator worse off than one that reports what it could
/// not remove.
pub async fn run_connector_teardown(steps: &[ConnectorTeardownStep]) -> Vec<String> {
    let mut problems = Vec::new();
    for step in steps {
        match step {
            ConnectorTeardownStep::ReapLabeled(command) => match docker(&command.argv()).await {
                Ok(listing) => {
                    let ids: Vec<String> = listing
                        .lines()
                        .map(str::trim)
                        .filter(|line| !line.is_empty())
                        .map(str::to_string)
                        .collect();
                    if ids.is_empty() {
                        continue;
                    }
                    let mut rm: Vec<String> = vec!["rm".into(), "-f".into()];
                    rm.extend(ids);
                    if let Err(err) = docker(&rm).await {
                        problems.push(format!("could not remove connector containers: {err}"));
                    }
                }
                Err(err) => problems.push(format!("could not list connector containers: {err}")),
            },
            ConnectorTeardownStep::RemoveNetwork(command) => {
                if let Err(err) = docker(&command.argv()).await {
                    let text = err.to_string();
                    if !text.contains("No such network") && !text.contains("not found") {
                        problems.push(format!("could not remove the connector network: {err}"));
                    }
                }
            }
            ConnectorTeardownStep::WipeSecrets(root) => {
                if let Err(err) = crate::connector_build::wipe_secrets_root(root) {
                    problems.push(format!(
                        "could not wipe staged connector credentials: {err}"
                    ));
                }
            }
        }
    }
    problems
}
