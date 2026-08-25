//! `curie local <up|down|status>`: wrap the repo's local dev stack
//! (`compose.dev.yaml`: Postgres + Valkey + Langfuse + ClickHouse + RustFS +
//! OTel) the same way `ops.rs` wraps Helm -- a deliberately thin CLI over
//! `docker compose`, which stays the source of truth. Each verb builds its
//! command line as a pure function returning an [`OpsCommand`]; the executor
//! (or the `--dry-run` printer) consumes it, so argv construction stays
//! unit-testable with no Docker daemon.

use std::path::Path;

use anyhow::{bail, Context, Result};

use crate::commands::OLLAMA_PORT;
use crate::docker;
use crate::ops::{plain, require_on_path, run_capture, run_step, CmdArg, OpsCommand};

/// Dev-channel local-candidate filename probed by the artifact resolver.
pub const DEFAULT_COMPOSE_FILE: &str = "compose.dev.yaml";

/// The compose project every local-tier command pins, injected as
/// `COMPOSE_PROJECT_NAME`. Named once so the connector overlay joins the same
/// project (and therefore the same `curie_runner` network) the stack runs under.
pub const COMPOSE_PROJECT: &str = "curie";

/// The Docker volume holding this tier's Ollama model cache: compose's
/// `ollama_data` under the pinned `curie` project name that `up_command`
/// injects as `COMPOSE_PROJECT_NAME`. Hardcoded to match the compose file, the
/// same way `ENDPOINTS` is, and guarded by
/// `compose_ollama_volume_and_image_match_the_compose_file`.
pub const COMPOSE_OLLAMA_VOLUME: &str = "curie_ollama_data";

/// The service endpoints the dev stack exposes, as committed in
/// `compose.dev.yaml`'s port mappings. Printed after `local up` so the operator
/// has the URLs in hand. Hardcoded to match the compose file (see the
/// `endpoints_match_compose_file` test, which asserts the file still maps them).
///
/// The `core` flag marks endpoints backed by a service in the `core` profile
/// (started under `--minimal`); the rest are `full`-only and are hidden under
/// `--minimal` so `up` never advertises a URL for a service it did not start.
const ENDPOINTS: &[(&str, &str, bool)] = &[
    // The three observability port literals live once, in `observability.rs`
    // (#460); these rows reference them so the two cannot drift. Values are
    // unchanged, so `endpoints_match_compose_file` stays green.
    ("Curie API", crate::observability::LOCAL_API_URL, true),
    (
        "Curie Console",
        crate::observability::LOCAL_CONSOLE_URL,
        false,
    ),
    (
        "Langfuse UI",
        crate::observability::LOCAL_LANGFUSE_URL,
        false,
    ),
    ("Postgres", "localhost:25432", true),
    ("Valkey", "localhost:26379", true),
    ("ClickHouse HTTP", "localhost:28123", false),
    ("RustFS S3", "localhost:29000", true),
    ("RustFS console", "localhost:29001", true),
    ("OTel gRPC", "localhost:24317", false),
    ("OTel HTTP", "localhost:24318", false),
];

/// Credential env vars the compose stack forwards from the shell (bare names in
/// `compose.dev.yaml`). Any one set non-empty makes `local up` go live, matching
/// `skill up`. Empty counts as unset (the empty-string-is-not-a-credential rule).
pub const CREDENTIAL_ENV_VARS: &[&str] = &[
    "CURIE_CREDENTIALS",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
];

/// The model mode `local up` resolves from the shell so the local tier reaches
/// skill-tier parity: a credential present makes local go live exactly like
/// `skill up`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ModelMode {
    /// A credential is present and fake is not pinned truthy: inject
    /// CURIE_FAKE_MODEL=0 so local goes live like `skill up`.
    LiveFromCredential,
    /// A credential is present but CURIE_FAKE_MODEL is pinned truthy: run fake
    /// anyway (the operator asked for it) but warn loudly.
    FakePinnedDespiteCredential,
    /// No credential: compose's fake default stands; nothing to inject.
    DefaultFake,
}

/// Match the runner's truthy parse of `CURIE_FAKE_MODEL`
/// (`runner/src/curie_runner/__main__.py`): lowercase one of `1`/`true`/`yes`.
/// The runtime's own reading of `CURIE_FAKE_MODEL`. Shared with the eval
/// sweep's worker probe (`message::probe_fake_model`) so the CLI judges a
/// deployed worker's fake-ness by the same rule the worker judges itself.
pub(crate) fn fake_model_is_truthy(v: &str) -> bool {
    matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
}

/// Pure parity core. `explicit_fake_model` is the shell CURIE_FAKE_MODEL (None
/// when unset or empty). `has_credential` is whether any CREDENTIAL_ENV_VARS is
/// set non-empty.
pub fn resolve_model_mode(explicit_fake_model: Option<&str>, has_credential: bool) -> ModelMode {
    if !has_credential {
        return ModelMode::DefaultFake;
    }
    match explicit_fake_model {
        Some(v) if fake_model_is_truthy(v) => ModelMode::FakePinnedDespiteCredential,
        _ => ModelMode::LiveFromCredential,
    }
}

/// The single injection step every worker-restarting command shares: flip
/// compose's fake default to live when (and only when) a credential is present
/// and fake is not explicitly pinned. `FakePinnedDespiteCredential` and
/// `DefaultFake` return `None` so compose's `${CURIE_FAKE_MODEL:-1}` default
/// stands. This is the one place that decision is made -- `up_command` and
/// `local comms`'s connect/disconnect commands all call it instead of each
/// re-deriving the pair inline, which is what let `local comms` drift out of
/// parity with `local up` (issue #450).
pub fn fake_model_env_override(mode: ModelMode) -> Option<(String, String)> {
    match mode {
        ModelMode::LiveFromCredential => Some(("CURIE_FAKE_MODEL".into(), "0".into())),
        ModelMode::FakePinnedDespiteCredential | ModelMode::DefaultFake => None,
    }
}

/// The other injection step every worker-restarting command shares: suppress
/// the OTel endpoint on a `core`-only stack. `otel-collector` is a `full`-profile
/// service, so a `--minimal` stack has no collector to export to, and every span
/// the runner emits would pay a synchronous DNS retry against a name that cannot
/// resolve. An empty value (not an absent one) is what does it: compose writes
/// the endpoint as `${OTEL_EXPORTER_OTLP_ENDPOINT-...}`, whose `-` (unset-only)
/// form substitutes its default only when the var is UNSET, so exporting it
/// empty resolves to empty and the runner exports nothing. `false` returns
/// `None` so compose's shipped collector default stands. This is the one place
/// that decision is made -- `up_command` and `local comms`'s connect/disconnect
/// commands all call it instead of each re-deriving the pair inline, the same
/// drift that let `local comms` fall out of parity with `local up` on the fake
/// model (issue #450).
pub fn otel_endpoint_env_override(minimal: bool) -> Option<(String, String)> {
    if minimal {
        Some(("OTEL_EXPORTER_OTLP_ENDPOINT".into(), String::new()))
    } else {
        None
    }
}

/// Snapshot the shell for the parity decision. An empty CURIE_FAKE_MODEL is
/// treated as unset (matches compose's `${CURIE_FAKE_MODEL:-1}` and the
/// empty-string-is-not-a-credential rule); a credential is any non-empty
/// CREDENTIAL_ENV_VARS value.
pub fn model_mode_from_env() -> ModelMode {
    model_mode_from_env_with_file_credential(false)
}

/// The shell parity decision, folding in whether an opt-in `.env` supplied a
/// model credential the shell did not (#749). `file_has_credential` makes the
/// stack go live exactly like a shell credential would, unless the shell pins
/// `CURIE_FAKE_MODEL` truthy -- in which case the pin still wins
/// (`FakePinnedDespiteCredential`). Reads the shell for the pin and for its own
/// credentials; a `.env`-only credential is the sole extra input.
pub fn model_mode_from_env_with_file_credential(file_has_credential: bool) -> ModelMode {
    let explicit = std::env::var("CURIE_FAKE_MODEL")
        .ok()
        .filter(|s| !s.is_empty());
    let has_credential = file_has_credential
        || CREDENTIAL_ENV_VARS
            .iter()
            .any(|v| std::env::var(v).map(|s| !s.is_empty()).unwrap_or(false));
    resolve_model_mode(explicit.as_deref(), has_credential)
}

/// Resolve the opt-in bundle `.env` plan for `local up` (#749, ADR-0070): the
/// model credentials to inject into the compose child, plus the effective model
/// mode. Pure -- the shell-presence predicate and the explicit `CURIE_FAKE_MODEL`
/// pin are injected -- so both the precedence (shell env wins over `.env`) and
/// the "a `.env`-only credential still boots live" rule are unit-testable without
/// touching the process environment.
///
/// `parsed` is the recognized model-credential names read from the file;
/// `shell_present(name)` is whether the shell already exports it non-empty (a
/// shell value always wins, so a present name is never taken from the file);
/// `explicit_fake_model` / `shell_has_credential` are the shell inputs to the
/// parity decision. The returned credentials are only those absent from the
/// shell, ready to be attached as masked `secret_env` (never argv).
pub fn resolve_env_file_up_plan(
    parsed: &[(String, String)],
    shell_present: &dyn Fn(&str) -> bool,
    explicit_fake_model: Option<&str>,
    shell_has_credential: bool,
) -> (Vec<(String, String)>, ModelMode) {
    let creds = crate::commands::resolve_env_file_credentials(parsed, shell_present);
    let has_credential = shell_has_credential || !creds.is_empty();
    let mode = resolve_model_mode(explicit_fake_model, has_credential);
    (creds, mode)
}

/// Read the opt-in bundle `.env` and resolve the credentials `local up` should
/// inject plus the effective model mode, from the live process environment. A
/// `None` path means no file is read (returns no credentials and the shell-only
/// mode). Thin IO wrapper over the pure [`resolve_env_file_up_plan`]; the file
/// is parsed for the recognized model-credential names only, every other key
/// ignored.
pub fn load_env_file_up_plan(
    env_file: Option<&Path>,
) -> Result<(Vec<(String, String)>, ModelMode)> {
    let Some(path) = env_file else {
        return Ok((Vec::new(), model_mode_from_env()));
    };
    let parsed = crate::commands::parse_credential_env_file(path)?;
    let explicit = std::env::var("CURIE_FAKE_MODEL")
        .ok()
        .filter(|s| !s.is_empty());
    let shell_has_credential = CREDENTIAL_ENV_VARS
        .iter()
        .any(|v| crate::commands::env_credential_present(v));
    Ok(resolve_env_file_up_plan(
        &parsed,
        &|name| crate::commands::env_credential_present(name),
        explicit.as_deref(),
        shell_has_credential,
    ))
}

/// Flags shared by every `local` verb.
pub struct LocalOpts {
    pub file: String,
    pub dry_run: bool,
    pub minimal: bool,
    pub local_model: Option<String>,
    pub slack: bool,
    /// Model mode resolved from the shell (skill-tier parity). Only `up`
    /// consumes it; `down`/`status` set `DefaultFake`.
    pub model_mode: ModelMode,
    /// Opt-in bundle-local `.env` path (#749, ADR-0070): the LOWEST-priority
    /// model-credential source for the compose stack, so a bundle boots live
    /// with no `set -a; source .env` step. `None` means no file is read. Only
    /// `up` consumes it; the other verbs set `None`.
    pub env_file: Option<std::path::PathBuf>,
    /// Opt in to downloading the `--local-model` assets this run (ADR 0093).
    /// Without it, `up` refuses rather than fetching ~11.4 GB implicitly. Only
    /// `up` consumes it; the other verbs set `false`.
    pub pull_model: bool,
}

pub struct LocalDownOpts {
    pub common: LocalOpts,
    /// Add `-v` to destroy volumes (throwaway).
    pub wipe: bool,
    /// Skip the interactive confirmation that `--wipe` otherwise requires.
    pub yes: bool,
}

pub struct LocalRebuildOpts {
    pub common: LocalOpts,
    /// The compose service to rebuild + recreate, e.g. `curie-worker`.
    pub service: String,
    /// Explicit provider model id to preserve from `local up`.
    pub model: Option<String>,
}

// ---------------------------------------------------------------------------
// Command builders (pure; unit-tested below)
// ---------------------------------------------------------------------------

/// `docker compose -f <file> <tail...>`.
fn compose(file: &str, tail: &[&str]) -> OpsCommand {
    let mut args = vec![plain("compose"), plain("-f"), plain(file)];
    for t in tail {
        args.push(plain(*t));
    }
    OpsCommand::new("docker", args)
}

/// `docker compose --profile <core|full> [--profile local-model] [--profile slack] -f <file> up -d --wait`.
pub fn up_command(o: &LocalOpts) -> OpsCommand {
    up_command_with_model(o, None)
}

fn compose_profile_args(o: &LocalOpts) -> Vec<CmdArg> {
    let profile = if o.minimal { "core" } else { "full" };
    let mut args = vec![plain("compose"), plain("--profile"), plain(profile)];
    if o.local_model.is_some() {
        args.push(plain("--profile"));
        args.push(plain("local-model"));
    }
    if o.slack {
        args.push(plain("--profile"));
        args.push(plain("slack"));
    }
    args
}

// `with_env` REPLACES the env vec, so both compose builders construct the
// model and OTel wiring once. `--local-model` and the credential-driven live
// injection are mutually exclusive: local-model carries its own live env
// (CURIE_FAKE_MODEL=0 + the ollama routing), so the parity injection only
// applies when no local model is requested.
fn compose_model_env(o: &LocalOpts, model: Option<&str>) -> Vec<(String, String)> {
    let mut env: Vec<(String, String)> = if let Some(model) = &o.local_model {
        vec![
            ("CURIE_FAKE_MODEL".into(), "0".into()),
            (
                "CURIE_MODEL_BASE_URL".into(),
                format!("http://ollama:{OLLAMA_PORT}"),
            ),
            ("CURIE_MODEL".into(), model.clone()),
            // Spawned runners join the dedicated, data-tier-free runner network
            // (#631). ollama is multi-homed onto it, so `--local-model` resolves
            // `ollama` by name without exposing postgres/valkey/rustfs.
            ("CURIE_DOCKER_NETWORK".into(), "curie_runner".into()),
            // Pin the compose project name so the default network is always
            // `curie_default`, regardless of the working-directory basename
            // (which is what compose otherwise derives the project name from).
            ("COMPOSE_PROJECT_NAME".into(), COMPOSE_PROJECT.into()),
        ]
    } else {
        // Delegate to `fake_model_env_override`, which discriminates on
        // `o.model_mode`: LiveFromCredential injects CURIE_FAKE_MODEL=0 so
        // compose goes live, matching `skill up`. FakePinnedDespiteCredential
        // and DefaultFake inject nothing, so compose's
        // `${CURIE_FAKE_MODEL:-1}` default stands for those two modes.
        let mut env: Vec<(String, String)> =
            fake_model_env_override(o.model_mode).into_iter().collect();
        if let Some(model) = model {
            env.push(("CURIE_MODEL".into(), model.to_string()));
        }
        env
    };
    // Delegate to `otel_endpoint_env_override`, the single source of truth for
    // the `core`-profile collector suppression. This sits AFTER the branch
    // above, not inside it, because the `--local-model` arm does not fall
    // through to the else: `--minimal --local-model` needs suppressing too.
    env.extend(otel_endpoint_env_override(o.minimal));
    env
}

fn up_command_with_model(o: &LocalOpts, model: Option<&str>) -> OpsCommand {
    let mut args = compose_profile_args(o);
    args.extend([
        plain("-f"),
        plain(&o.file),
        plain("up"),
        plain("-d"),
        plain("--wait"),
    ]);
    let mut cmd = OpsCommand::new("docker", args);
    let env = compose_model_env(o, model);
    if !env.is_empty() {
        cmd = cmd.with_env(env);
    }
    cmd
}

/// `docker compose --profile <core|full> [--profile local-model] [--profile slack]
/// -f <file> up -d --build --force-recreate --no-deps <service>` (#714).
///
/// A targeted single-service rebuild -- e.g. picking up a code change to just
/// `curie-worker` -- carries the SAME env injection as `up_command`
/// (credential/model-mode parity, the core-profile OTel suppression). Without
/// it, a raw `docker compose up --no-deps <service>` silently reverts that one
/// service to compose's fake-model/dev-stub defaults, because compose's
/// `${VAR-default}` substitution reads the INVOKING shell's env, not whatever
/// the rest of the stack was already running with -- exactly the footgun that
/// cost a debugging session getting a real agent working locally. `--no-deps`
/// keeps the blast radius to the one named service; `--build` picks up a local
/// code change before recreating.
pub fn rebuild_command(o: &LocalOpts, service: &str, model: Option<&str>) -> OpsCommand {
    let mut args = compose_profile_args(o);
    args.extend([
        plain("-f"),
        plain(&o.file),
        plain("up"),
        plain("-d"),
        plain("--build"),
        plain("--force-recreate"),
        plain("--no-deps"),
        plain(service),
    ]);
    let mut cmd = OpsCommand::new("docker", args);
    let env = compose_model_env(o, model);
    if !env.is_empty() {
        cmd = cmd.with_env(env);
    }
    cmd
}

/// Every compose profile `up` can activate. `down` passes all of them
/// unconditionally so it always tears down what any `up` could start -- most
/// importantly the `slack` dispatcher (`restart: unless-stopped`), which a bare
/// `down` leaves running after a forgot-to-disconnect where it keeps holding the
/// Socket Mode connection and posting placeholders into real Slack with no
/// backend (issue #233). These are deliberately independent of the `LocalOpts`
/// flags: a plain `local down` carries none of `--slack`/`--minimal`/
/// `--local-model`, so gating teardown on them would orphan exactly the
/// profile-only services this exists to reap.
const ALL_PROFILES: &[&str] = &["core", "full", "local-model", "slack"];

/// `docker compose --profile <all> -f <file> down` (keep volumes), or
/// `... down -v` with `--wipe` (destroy volumes). The profiles cover every
/// service `up` can start so `down` never orphans a profile-only container.
pub fn down_command(o: &LocalDownOpts) -> OpsCommand {
    let mut args = vec![plain("compose")];
    for p in ALL_PROFILES {
        args.push(plain("--profile"));
        args.push(plain(*p));
    }
    args.extend([plain("-f"), plain(&o.common.file), plain("down")]);
    if o.wipe {
        args.push(plain("-v"));
    }
    OpsCommand::new("docker", args)
}

/// `docker compose -f <file> ps`.
pub fn status_command(o: &LocalOpts) -> OpsCommand {
    compose(&o.file, &["ps"])
}

// ---------------------------------------------------------------------------
// Verb handlers
// ---------------------------------------------------------------------------

/// Output of `local up`: the dry-run plan, or the started dev stack's advertised
/// endpoints. Routes through `Ui::emit` so `--json` emits a JSON object instead
/// of empty stdout (#485): the real path formerly ended in `ui.kv`, which
/// suppresses under `--json`.
#[derive(Debug)]
pub enum LocalUpOutput {
    DryRun(crate::ui::DryRunPlan),
    Up {
        endpoints: Vec<(String, String)>,
        slack: bool,
    },
}

impl crate::ui::CliOutput for LocalUpOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            LocalUpOutput::DryRun(plan) => plan.to_json(),
            LocalUpOutput::Up { endpoints, slack } => serde_json::json!({
                "status": "up",
                "endpoints": endpoints
                    .iter()
                    .map(|(name, url)| serde_json::json!({"name": name, "url": url}))
                    .collect::<Vec<_>>(),
                "slack": slack,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            LocalUpOutput::DryRun(plan) => plan.render(ui),
            LocalUpOutput::Up { endpoints, slack } => {
                for (label, url) in endpoints {
                    ui.kv(label, &ui.url(url));
                }
                if *slack {
                    ui.note("Slack dispatcher started (Socket Mode; no host port).");
                }
                ui.note("Drive the local product loop (no Slack, no Kubernetes):");
                ui.note(
                    "  curie local deploy --plugin-dir <dir> --slack-channel <C...> --api-url http://localhost:28000",
                );
                ui.note("  curie local message \"<your question>\"");
            }
        }
    }
}

/// Resolve model credentials for a worker starting local verb. Private storage
/// follows the shell and precedes the optional bundle `.env` file. Values are
/// returned as masked `secret_env`, and every source contributes to the model
/// mode decision. Shared by `local up` and `local rebuild` so both paths keep
/// identical credential behavior.
pub fn apply_credential_plan(
    o: &mut LocalOpts,
    ui: &crate::ui::Ui,
) -> Result<Vec<(String, String)>> {
    let explicit_fake_model = std::env::var("CURIE_FAKE_MODEL")
        .ok()
        .filter(|value| !value.is_empty());
    let shell_has_credential = CREDENTIAL_ENV_VARS
        .iter()
        .any(|name| crate::commands::env_credential_present(name));
    // A saved provider credential is an implicit input. Do not even open
    // private storage when the operator explicitly selected the fake model or
    // a local model, because neither path can use that credential. An explicit
    // --env-file remains opt-in and retains its existing behavior below.
    let skip_private_store = o.local_model.is_some()
        || explicit_fake_model
            .as_deref()
            .is_some_and(fake_model_is_truthy);
    let mut resolved = if skip_private_store {
        Vec::new()
    } else {
        match crate::commands::load_model_credentials_from_secret_store() {
            Ok(credentials) => credentials,
            Err(error) => {
                ui.warn(&format!(
                    "Saved model credentials could not be read; continuing without them: {error}"
                ));
                Vec::new()
            }
        }
    };
    if let Some(path) = o.env_file.as_deref() {
        let parsed = crate::commands::parse_credential_env_file(path)?;
        let (from_file, model_mode) = resolve_env_file_up_plan(
            &parsed,
            &|name| {
                crate::commands::env_credential_present(name)
                    || resolved
                        .iter()
                        .any(|(resolved_name, _)| resolved_name == name)
            },
            explicit_fake_model.as_deref(),
            shell_has_credential || !resolved.is_empty(),
        );
        for (name, _) in &from_file {
            ui.note(&format!(
                "{name}: loaded from --env-file {} for this run",
                path.display()
            ));
        }
        resolved.extend(from_file);
        o.model_mode = model_mode;
    } else {
        o.model_mode = resolve_model_mode(
            explicit_fake_model.as_deref(),
            shell_has_credential || !resolved.is_empty(),
        );
    }
    Ok(resolved)
}

pub async fn up(mut o: LocalOpts, model: Option<String>) -> Result<LocalUpOutput> {
    let ui = crate::ui::ui();
    // #749/ADR-0070: an opt-in bundle `.env` is the LOWEST-priority model
    // credential source, injected into the compose child as masked `secret_env`.
    let env_creds = apply_credential_plan(&mut o, ui)?;
    let mut cmd = up_command_with_model(&o, model.as_deref());
    if !env_creds.is_empty() {
        cmd = cmd.with_secret_env(env_creds);
    }
    if o.dry_run {
        return Ok(LocalUpOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![cmd.display()],
        }));
    }
    require_on_path("docker")?;
    // ADR 0093: `--local-model` never downloads its ~11.4 GB of assets
    // implicitly. Refuse before anything is brought up, unless the operator
    // asked for the fetch on this invocation.
    if let Some(model) = &o.local_model {
        if !o.pull_model {
            docker::preflight_local_model(
                crate::commands::DEFAULT_OLLAMA_IMAGE,
                COMPOSE_OLLAMA_VOLUME,
                model,
                &format!("curie local up --local-model {model} --pull-model"),
            )
            .await?;
        }
    }
    let cl = ui.checklist();
    run_step(&cl, "starting dev stack", "up", &cmd).await?;
    // `--local-model` is its own live path (routes to ollama); the resolved
    // credential parity note only applies when no local model was requested.
    if o.local_model.is_none() {
        match o.model_mode {
            ModelMode::LiveFromCredential => ui.note(
                "Running the LIVE model: a credential is available for this run (parity with `curie skill up`). Set CURIE_FAKE_MODEL=1 to force the offline fake model.",
            ),
            ModelMode::FakePinnedDespiteCredential => ui.warn(
                "Running the FAKE model despite an available credential: CURIE_FAKE_MODEL is pinned on. Unset it or set CURIE_FAKE_MODEL=0 to go live.",
            ),
            ModelMode::DefaultFake => ui.note(
                "Running the fake model (no credential set). Provide a credential (ANTHROPIC_API_KEY / CLAUDE_CODE_OAUTH_TOKEN / CURIE_CREDENTIALS) or --local-model to go live.",
            ),
        }
    }
    let endpoints = ENDPOINTS
        .iter()
        // Under `--minimal` only the `core` services started, so advertise only
        // their endpoints; the `full`-only URLs would 404.
        .filter(|(_, _, is_core)| !o.minimal || *is_core)
        .map(|(label, url, _)| (label.to_string(), url.to_string()))
        .collect();
    Ok(LocalUpOutput::Up {
        endpoints,
        slack: o.slack,
    })
}

/// Output of `local rebuild`: the dry-run plan, or which service was rebuilt
/// and which model mode it came back up running (#714).
#[derive(Debug)]
pub enum LocalRebuildOutput {
    DryRun(crate::ui::DryRunPlan),
    Rebuilt {
        service: String,
        model_mode: ModelMode,
    },
}

impl crate::ui::CliOutput for LocalRebuildOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            LocalRebuildOutput::DryRun(plan) => plan.to_json(),
            LocalRebuildOutput::Rebuilt {
                service,
                model_mode,
            } => serde_json::json!({
                "status": "rebuilt",
                "service": service,
                "model_mode": format!("{model_mode:?}"),
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            LocalRebuildOutput::DryRun(plan) => plan.render(ui),
            LocalRebuildOutput::Rebuilt {
                service,
                model_mode,
            } => {
                ui.note(&format!("Rebuilt and recreated `{service}`."));
                match model_mode {
                    ModelMode::LiveFromCredential => ui.note(
                        "Came back up on the LIVE model: a credential is available for this run.",
                    ),
                    ModelMode::FakePinnedDespiteCredential => ui.warn(
                        "Came back up on the FAKE model despite an available credential: CURIE_FAKE_MODEL is pinned on.",
                    ),
                    ModelMode::DefaultFake => ui.note(
                        "Came back up on the fake model (no credential available for this run).",
                    ),
                }
            }
        }
    }
}

/// `curie local rebuild <service>`: rebuild + recreate ONE compose service
/// (e.g. after a code change) without losing the stack's already-resolved
/// credential/model-mode wiring (#714) -- see `rebuild_command`'s doc comment
/// for the footgun this exists to close. Re-resolves `ModelMode` from THIS
/// invocation's inputs exactly as `local up` does -- the shell AND the same
/// opt-in `--env-file` (#853) -- rather than trying to read back whatever the
/// rest of the stack is currently running with (not generally recoverable from
/// outside the containers). So given the same shell and the same `--env-file`
/// the original `up` used, the rebuilt service comes back on the identical
/// model/credential wiring; drop `--env-file` on a stack that was brought up
/// with one and the rebuilt service silently reverts to compose's fake default,
/// which is exactly the regression #853 reports.
pub async fn rebuild(mut o: LocalRebuildOpts) -> Result<LocalRebuildOutput> {
    let ui = crate::ui::ui();
    // The same opt-in bundle `.env` plan `local up` applies: fold a file-only
    // credential into the model mode and inject it as masked `secret_env`, so
    // the resolved plan matches `up`'s for identical inputs (#853).
    let env_creds = apply_credential_plan(&mut o.common, ui)?;
    let mut cmd = rebuild_command(&o.common, &o.service, o.model.as_deref());
    if !env_creds.is_empty() {
        cmd = cmd.with_secret_env(env_creds);
    }
    if o.common.dry_run {
        return Ok(LocalRebuildOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![cmd.display()],
        }));
    }
    require_on_path("docker")?;
    let cl = ui.checklist();
    run_step(&cl, &format!("rebuilding {}", o.service), "rebuild", &cmd).await?;
    Ok(LocalRebuildOutput::Rebuilt {
        service: o.service,
        model_mode: o.common.model_mode,
    })
}

/// Output of `local status`: the dry-run plan, or the `docker compose ps` rows.
/// `--json` emits `{"services":[...lines]}` rather than the empty stdout the
/// former `payload_plain` loop produced (#485).
#[derive(Debug)]
pub enum LocalStatusOutput {
    DryRun(crate::ui::DryRunPlan),
    Services { rows: Vec<String> },
}

impl crate::ui::CliOutput for LocalStatusOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            LocalStatusOutput::DryRun(plan) => plan.to_json(),
            LocalStatusOutput::Services { rows } => serde_json::json!({"services": rows}),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            LocalStatusOutput::DryRun(plan) => plan.render(ui),
            LocalStatusOutput::Services { rows } => {
                for line in rows {
                    ui.payload_plain(line);
                }
            }
        }
    }
}

pub async fn status(o: LocalOpts) -> Result<LocalStatusOutput> {
    let ui = crate::ui::ui();
    let cmd = status_command(&o);
    if o.dry_run {
        return Ok(LocalStatusOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![cmd.display()],
        }));
    }
    require_on_path("docker")?;
    // `docker compose ps` output is itself the payload table.
    let (ok, out, err) = run_capture(&cmd).await?;
    if !ok {
        for line in err.lines() {
            ui.plumbing(line);
        }
        let reason = err
            .lines()
            .rev()
            .map(str::trim)
            .find(|l| !l.is_empty())
            .unwrap_or("command failed");
        ui.failure(&format!("`docker compose ps` failed: {reason}"));
        bail!("`docker compose ps` exited nonzero");
    }
    Ok(LocalStatusOutput::Services {
        rows: out.lines().map(str::to_string).collect(),
    })
}

/// Output of `local down`: the dry-run plan, an operator abort, or the stopped
/// stack. `--json` emits a JSON object; the real path formerly ended in
/// `ui.payload` (suppressed under `--json`, #485).
#[derive(Debug)]
pub enum LocalDownOutput {
    DryRun(crate::ui::DryRunPlan),
    Aborted,
    Down { volumes_wiped: bool, reaped: usize },
}

impl crate::ui::CliOutput for LocalDownOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            LocalDownOutput::DryRun(plan) => plan.to_json(),
            LocalDownOutput::Aborted => serde_json::json!({"stopped": false, "aborted": true}),
            LocalDownOutput::Down {
                volumes_wiped,
                reaped,
            } => serde_json::json!({
                "stopped": true,
                "volumes_wiped": volumes_wiped,
                "runners_reaped": reaped,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            LocalDownOutput::DryRun(plan) => plan.render(ui),
            LocalDownOutput::Aborted => ui.note("aborted"),
            LocalDownOutput::Down {
                volumes_wiped,
                reaped,
            } => {
                if *reaped > 0 {
                    ui.note(&format!("removed {reaped} runner container(s)"));
                }
                if *volumes_wiped {
                    ui.payload("dev stack stopped; volumes wiped");
                } else {
                    ui.payload("dev stack stopped");
                    ui.note("volumes kept (fast restart with `curie local up`)");
                }
            }
        }
    }
}

/// The connector teardown `local down` runs, for a `down` invoked from `cwd`.
///
/// Container reaping stays label-scoped rather than file-scoped: `LocalDownOpts`
/// carries no plugin directory, so a `down` run from anywhere must still reap
/// what a `local deploy` started (ADR 0113, block B1-8). The staged credential
/// tree can only be addressed by path, and the local tier records the deployed
/// bundle's directory nowhere (`.curie/runner.json` is the skill tier's), so the
/// honest resolution is the one `local deploy` itself defaults to: the current
/// directory (`--plugin-dir .`). The wipe is added only when that directory
/// actually holds a staged tree.
///
/// The limit that leaves: a `down` run from an unrelated directory, or after a
/// deploy that pointed `--plugin-dir` elsewhere, still cannot find that bundle's
/// tree. Accepted, because guessing at another bundle is worse than missing one
/// -- the containers holding the credentials are reaped either way, and a `down`
/// or `deploy` from the bundle's own directory clears the tree.
pub fn connector_teardown_plan_for_down(cwd: &Path) -> Vec<docker::ConnectorTeardownStep> {
    let staged = crate::connector_build::connector_secrets_root(cwd).is_dir();
    docker::connector_teardown_plan(COMPOSE_PROJECT, None, staged.then_some(cwd))
}

pub async fn down(o: LocalDownOpts) -> Result<LocalDownOutput> {
    let ui = crate::ui::ui();
    let cmd = down_command(&o);
    let cwd = std::env::current_dir().context("resolving the current directory")?;
    let teardown = connector_teardown_plan_for_down(&cwd);
    if o.common.dry_run {
        let mut lines = vec![
            cmd.display(),
            format!(
                "docker rm -f $(docker ps -a --filter label={} -q)",
                docker::SANDBOX_LABEL
            ),
            format!(
                "docker rm -f $(docker ps -a -q --filter label={} --filter label={})",
                docker::CONNECTOR_COMPONENT_LABEL,
                docker::connector_project_label(COMPOSE_PROJECT)
            ),
        ];
        // A removal of files on disk must not be a surprise the plan omitted.
        for step in &teardown {
            if let docker::ConnectorTeardownStep::WipeSecrets(path) = step {
                lines.push(format!("rm -rf {}", path.display()));
            }
        }
        return Ok(LocalDownOutput::DryRun(crate::ui::DryRunPlan { lines }));
    }
    if o.wipe {
        ui.warn(&format!(
            "this destroys all volumes for the '{}' dev stack (Postgres, ClickHouse, RustFS, Valkey data)",
            o.common.file
        ));
        if !o.yes
            && !crate::ops::confirm(&format!(
                "This destroys all volumes for the '{}' dev stack (Postgres, ClickHouse, RustFS, Valkey data). Continue? [y/N] ",
                o.common.file
            ))?
        {
            return Ok(LocalDownOutput::Aborted);
        }
    }
    require_on_path("docker")?;
    let cl = ui.checklist();
    let label = if o.wipe {
        "stopping stack and wiping volumes"
    } else {
        "stopping stack"
    };
    run_step(&cl, label, "stopped", &cmd).await?;
    // Containers first, then this bundle's staged credential tree when the
    // directory `down` was run from holds one -- see
    // `connector_teardown_plan_for_down` for what that resolution can and
    // cannot reach.
    for problem in docker::run_connector_teardown(&teardown).await {
        ui.warn(&problem);
    }
    let report = docker::reap_labeled(docker::SANDBOX_LABEL).await;
    if let Some(err) = report.error {
        // The stack stopped, but the runner reap did not complete cleanly. Fail
        // loudly rather than report success with orphaned containers still
        // holding ports and credentials (#613).
        let still = if report.still_present.is_empty() {
            "unknown".to_string()
        } else {
            report.still_present.join(", ")
        };
        bail!(
            "dev stack stopped, but reaping runner containers did not complete cleanly: {err}. \
             Still running: {still}. Remove them with `docker rm -f <id>` before restarting."
        );
    }
    Ok(LocalDownOutput::Down {
        volumes_wiped: o.wipe,
        reaped: report.removed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(file: &str) -> LocalOpts {
        LocalOpts {
            file: file.into(),
            dry_run: false,
            minimal: false,
            local_model: None,
            pull_model: false,
            slack: false,
            model_mode: ModelMode::DefaultFake,
            env_file: None,
        }
    }

    fn opts_with_local_model(file: &str, model: &str) -> LocalOpts {
        LocalOpts {
            file: file.into(),
            dry_run: false,
            minimal: false,
            local_model: Some(model.into()),
            pull_model: false,
            slack: false,
            model_mode: ModelMode::DefaultFake,
            env_file: None,
        }
    }

    /// Every `--profile` token `up` can emit across all flag combinations,
    /// derived from `up_command` itself so a newly added up profile that `down`
    /// forgets fails `down_passes_every_up_profile` instead of silently
    /// orphaning that service. `--minimal` swaps `full`->`core`, so both modes
    /// are sampled with `--slack` and `--local-model` on.
    fn up_activatable_profiles() -> std::collections::BTreeSet<String> {
        let mut profiles = std::collections::BTreeSet::new();
        for minimal in [false, true] {
            let mut o = opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b");
            o.minimal = minimal;
            o.slack = true;
            let display = up_command(&o).display();
            let mut tokens = display.split_whitespace().peekable();
            while let Some(tok) = tokens.next() {
                if tok == "--profile" {
                    if let Some(p) = tokens.next() {
                        profiles.insert(p.to_string());
                    }
                }
            }
        }
        profiles
    }

    fn read_compose(name: &str) -> String {
        std::fs::read_to_string(format!("{}/../{}", env!("CARGO_MANIFEST_DIR"), name))
            .unwrap_or_else(|e| panic!("read {name}: {e}"))
    }

    #[test]
    fn up_uses_detached_wait() {
        let cmd = up_command(&opts(DEFAULT_COMPOSE_FILE));
        assert_eq!(
            cmd.display(),
            "docker compose --profile full -f compose.dev.yaml up -d --wait"
        );
    }

    #[test]
    fn up_local_model_uses_profile_and_env() {
        let cmd = up_command(&opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b"));
        let display = cmd.display();
        assert!(display.contains("--profile full"), "{display}");
        assert!(display.contains("--profile local-model"), "{display}");
        assert!(display.contains("up -d --wait"), "{display}");
        assert!(cmd
            .env
            .contains(&(String::from("CURIE_FAKE_MODEL"), String::from("0"))));
        assert!(cmd.env.contains(&(
            String::from("CURIE_MODEL_BASE_URL"),
            String::from("http://ollama:11434"),
        )));
        assert!(cmd
            .env
            .contains(&(String::from("CURIE_MODEL"), String::from("qwen3:4b"))));
        assert!(cmd.env.contains(&(
            String::from("CURIE_DOCKER_NETWORK"),
            String::from("curie_runner"),
        )));
        assert!(cmd
            .env
            .contains(&(String::from("COMPOSE_PROJECT_NAME"), String::from("curie"),)));
    }

    /// `--minimal` starts the `core` profile, which has no otel-collector, so
    /// `up` must hand compose an EMPTY endpoint. Compose's `${VAR-default}` form
    /// substitutes only when the var is unset, so the empty value suppresses the
    /// default instead of pointing every spawned runner at a host that never
    /// resolves (each span then eats ~7s of synchronous export retry).
    #[test]
    fn up_minimal_suppresses_otel_endpoint() {
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.minimal = true;
        let cmd = up_command(&o);
        assert!(
            cmd.env
                .contains(&(String::from("OTEL_EXPORTER_OTLP_ENDPOINT"), String::new(),)),
            "--minimal must pass an empty OTEL_EXPORTER_OTLP_ENDPOINT; env={:?}",
            cmd.env
        );
    }

    /// The `--local-model` arm of `up_command`'s env build does not fall through
    /// to the else, so the suppression has to sit outside both arms. This is the
    /// combination that regresses if it ever moves back inside one.
    #[test]
    fn up_minimal_suppresses_otel_endpoint_with_local_model() {
        let mut o = opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b");
        o.minimal = true;
        let cmd = up_command(&o);
        assert!(
            cmd.env
                .contains(&(String::from("OTEL_EXPORTER_OTLP_ENDPOINT"), String::new(),)),
            "--minimal --local-model must pass an empty OTEL_EXPORTER_OTLP_ENDPOINT; env={:?}",
            cmd.env
        );
        // The local-model wiring must survive the suppression.
        assert!(cmd
            .env
            .contains(&(String::from("CURIE_MODEL"), String::from("qwen3:4b"))));
    }

    /// The default (full-profile) `up` starts otel-collector, so it must NOT
    /// suppress: leaving the var unset is what lets compose's default resolve to
    /// `http://otel-collector:4318`.
    #[test]
    fn up_default_does_not_suppress_otel_endpoint() {
        for o in [
            opts(DEFAULT_COMPOSE_FILE),
            opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b"),
        ] {
            let cmd = up_command(&o);
            assert!(
                !cmd.env
                    .iter()
                    .any(|(k, _)| k == "OTEL_EXPORTER_OTLP_ENDPOINT"),
                "a non-minimal up must leave compose's endpoint default alone; env={:?}",
                cmd.env
            );
        }
    }

    /// #714: the argv shape is `up -d --build --force-recreate --no-deps
    /// <service>` (not `up_command`'s `up -d --wait`), and the named service
    /// lands as the final token.
    #[test]
    fn rebuild_command_targets_one_service() {
        let cmd = rebuild_command(&opts(DEFAULT_COMPOSE_FILE), "curie-worker", None);
        let display = cmd.display();
        assert!(display.contains("up -d --build --force-recreate --no-deps curie-worker"));
        assert!(!display.contains("--wait"));
    }

    /// #714: the whole point -- a credential in the shell must still flip
    /// CURIE_FAKE_MODEL=0 on a targeted rebuild, exactly like `local up`,
    /// instead of the rebuilt service silently reverting to compose's fake
    /// default.
    #[test]
    fn rebuild_command_carries_live_model_parity() {
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.model_mode = ModelMode::LiveFromCredential;
        let cmd = rebuild_command(&o, "curie-worker", None);
        assert!(cmd
            .env
            .contains(&(String::from("CURIE_FAKE_MODEL"), String::from("0"))));
    }

    #[test]
    fn rebuild_command_carries_explicit_model_parity() {
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.model_mode = ModelMode::LiveFromCredential;
        let up = up_command_with_model(&o, Some("z-ai/glm-5.2"));
        let rebuilt = rebuild_command(&o, "curie-worker", Some("z-ai/glm-5.2"));

        assert_eq!(up.env, rebuilt.env, "rebuild model env drifted from up");
        assert!(rebuilt
            .env
            .contains(&(String::from("CURIE_MODEL"), String::from("z-ai/glm-5.2"))));
    }

    #[test]
    fn rebuild_command_default_fake_injects_nothing() {
        let cmd = rebuild_command(&opts(DEFAULT_COMPOSE_FILE), "curie-worker", None);
        assert!(cmd.env.is_empty(), "env={:?}", cmd.env);
    }

    #[test]
    fn rebuild_command_carries_local_model_wiring() {
        let cmd = rebuild_command(
            &opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b"),
            "curie-worker",
            None,
        );
        assert!(cmd
            .env
            .contains(&(String::from("CURIE_MODEL"), String::from("qwen3:4b"))));
        assert!(cmd
            .env
            .contains(&(String::from("CURIE_FAKE_MODEL"), String::from("0"))));
    }

    #[test]
    fn rebuild_command_minimal_suppresses_otel_endpoint() {
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.minimal = true;
        let cmd = rebuild_command(&o, "curie-worker", None);
        assert!(cmd
            .env
            .contains(&(String::from("OTEL_EXPORTER_OTLP_ENDPOINT"), String::new())));
        assert!(cmd.display().contains("--profile core"));
    }

    #[test]
    fn rebuild_command_respects_slack_profile() {
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.slack = true;
        let display = rebuild_command(&o, "curie-dispatcher", None).display();
        assert!(display.contains("--profile slack"));
    }

    /// #749/ADR-0070 precedence: a credential exported in the SHELL always wins
    /// over the same name in the opt-in `.env`, so a shell-present name is never
    /// taken from the file. Here the shell already has `ANTHROPIC_API_KEY`, so the
    /// file copy is dropped; nothing is injected and the mode stays whatever the
    /// shell dictated (live, since the shell has the credential).
    #[test]
    fn env_file_up_plan_shell_env_wins_over_dotenv() {
        let parsed = vec![("ANTHROPIC_API_KEY".to_string(), "from-file".to_string())];
        let (creds, mode) = resolve_env_file_up_plan(
            &parsed,
            &|name| name == "ANTHROPIC_API_KEY", // shell already exports it
            None,
            true, // shell has the credential
        );
        assert!(
            creds.is_empty(),
            "a shell-present credential must not be taken from the file; got {creds:?}"
        );
        assert_eq!(mode, ModelMode::LiveFromCredential);
    }

    /// A credential present ONLY in the `.env` (absent from the shell) is
    /// injected and still flips the stack live, so a bundle boots on the real
    /// model with no `source` step.
    #[test]
    fn env_file_up_plan_dotenv_only_credential_boots_live() {
        let parsed = vec![("ANTHROPIC_API_KEY".to_string(), "sk-from-file".to_string())];
        let (creds, mode) = resolve_env_file_up_plan(
            &parsed,
            &|_| false, // shell exports nothing
            None,
            false,
        );
        assert_eq!(
            creds,
            vec![("ANTHROPIC_API_KEY".to_string(), "sk-from-file".to_string())]
        );
        assert_eq!(mode, ModelMode::LiveFromCredential);
    }

    /// The shell's `CURIE_FAKE_MODEL` pin still wins over a `.env` credential:
    /// the operator explicitly asked for the fake model, so a file credential
    /// does not silently override it (parity with a shell credential).
    #[test]
    fn env_file_up_plan_shell_fake_pin_beats_dotenv_credential() {
        let parsed = vec![("ANTHROPIC_API_KEY".to_string(), "sk-from-file".to_string())];
        let (creds, mode) = resolve_env_file_up_plan(&parsed, &|_| false, Some("1"), false);
        // The credential is still injected (present for the runner), but the mode
        // honors the pin.
        assert_eq!(creds.len(), 1);
        assert_eq!(mode, ModelMode::FakePinnedDespiteCredential);
    }

    /// The injected `.env` credential travels as masked `secret_env`: its raw
    /// value never appears in the argv or the printed command line, only a masked
    /// prefix. This is the leak-prevention property (cli/CLAUDE.md: credentials
    /// masked, never printed).
    #[test]
    fn env_file_credential_is_masked_never_printed() {
        let secret = "sk-ant-supersecretvalue";
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.model_mode = ModelMode::LiveFromCredential;
        let cmd = up_command(&o)
            .with_secret_env(vec![("ANTHROPIC_API_KEY".to_string(), secret.to_string())]);
        let display = cmd.display();
        assert!(
            display.contains("ANTHROPIC_API_KEY=sk-ant-s***"),
            "the token must be masked in the printed command line; got {display}"
        );
        assert!(
            !display.contains(secret),
            "the raw token leaked into the printed command line: {display}"
        );
        assert!(
            !cmd.argv().iter().any(|a| a.contains(secret)),
            "the raw token leaked into argv: {:?}",
            cmd.argv()
        );
        // The credential rides secret_env, not the plain env vec.
        assert!(cmd.env.iter().all(|(k, _)| k != "ANTHROPIC_API_KEY"));
        assert!(cmd
            .secret_env
            .contains(&("ANTHROPIC_API_KEY".to_string(), secret.to_string())));
    }

    /// #853: given the SAME inputs a `local up --env-file` resolved -- a
    /// file-only credential folded into `LiveFromCredential`, plus that
    /// credential to inject as masked `secret_env` -- `local rebuild` builds the
    /// identical model/credential wiring, so the rebuilt service comes back LIVE
    /// rather than reverting to compose's fake default. Asserted on the resolved
    /// plan (the env and secret_env of the built command), not on flag presence.
    /// Both verbs share `apply_credential_plan`, so identical inputs cannot diverge.
    #[test]
    fn rebuild_matches_up_env_file_wiring() {
        let secret = "sk-ant-fromfile";
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        // The mode a `.env`-only credential resolves to (see
        // env_file_up_plan_dotenv_only_credential_boots_live).
        o.model_mode = ModelMode::LiveFromCredential;
        let creds = vec![("ANTHROPIC_API_KEY".to_string(), secret.to_string())];

        let up = up_command(&o).with_secret_env(creds.clone());
        let rebuilt = rebuild_command(&o, "curie-worker", None).with_secret_env(creds.clone());

        // The live-model flip is present on both, and the plain env + masked
        // credential wiring match exactly across the two verbs.
        assert!(up
            .env
            .contains(&(String::from("CURIE_FAKE_MODEL"), String::from("0"))));
        assert_eq!(up.env, rebuilt.env, "rebuild env drifted from up env");
        assert_eq!(
            up.secret_env, rebuilt.secret_env,
            "rebuild secret_env drifted from up secret_env"
        );
        assert!(rebuilt
            .secret_env
            .contains(&("ANTHROPIC_API_KEY".to_string(), secret.to_string())));
    }

    /// #853: the `--env-file` credential a rebuild injects is masked in the
    /// printed command line and never lands in argv -- the same leak-prevention
    /// property `env_file_credential_is_masked_never_printed` proves for `up`
    /// must hold on the rebuild path too (cli/CLAUDE.md: credentials masked,
    /// never printed).
    #[test]
    fn rebuild_env_file_credential_is_masked_never_printed() {
        let secret = "sk-ant-supersecretvalue";
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.model_mode = ModelMode::LiveFromCredential;
        let cmd = rebuild_command(&o, "curie-worker", None)
            .with_secret_env(vec![("ANTHROPIC_API_KEY".to_string(), secret.to_string())]);
        let display = cmd.display();
        assert!(
            display.contains("ANTHROPIC_API_KEY=sk-ant-s***"),
            "the token must be masked in the printed command line; got {display}"
        );
        assert!(
            !display.contains(secret),
            "the raw token leaked into the printed command line: {display}"
        );
        assert!(
            !cmd.argv().iter().any(|a| a.contains(secret)),
            "the raw token leaked into argv: {:?}",
            cmd.argv()
        );
        assert!(cmd.env.iter().all(|(k, _)| k != "ANTHROPIC_API_KEY"));
        assert!(cmd
            .secret_env
            .contains(&("ANTHROPIC_API_KEY".to_string(), secret.to_string())));
    }

    /// No `--env-file` means no file read and no injected credentials -- the
    /// mode is the shell-only decision.
    #[test]
    fn load_env_file_up_plan_none_reads_nothing() {
        let (creds, _mode) = load_env_file_up_plan(None).unwrap();
        assert!(creds.is_empty());
    }

    #[test]
    fn resolve_model_mode_truth_table() {
        // No credential -> DefaultFake regardless of any pin.
        assert_eq!(resolve_model_mode(None, false), ModelMode::DefaultFake);
        assert_eq!(resolve_model_mode(Some("1"), false), ModelMode::DefaultFake);
        assert_eq!(
            resolve_model_mode(Some("banana"), false),
            ModelMode::DefaultFake
        );
        // Credential + no explicit pin -> live.
        assert_eq!(
            resolve_model_mode(None, true),
            ModelMode::LiveFromCredential
        );
        // Credential + truthy pin (any casing the runner accepts) -> fake pinned.
        for pin in ["1", "true", "YES", "Yes"] {
            assert_eq!(
                resolve_model_mode(Some(pin), true),
                ModelMode::FakePinnedDespiteCredential,
                "pin {pin:?} should pin fake"
            );
        }
        // Credential + non-truthy pin -> live (0/off/garbage are not "fake on").
        // A whitespace-padded value like " true " is not truthy because the
        // runner does not trim before comparing.
        for pin in ["0", "banana", "off", "", " true "] {
            assert_eq!(
                resolve_model_mode(Some(pin), true),
                ModelMode::LiveFromCredential,
                "pin {pin:?} should stay live"
            );
        }
    }

    #[test]
    fn fake_model_env_override_maps_all_three_modes() {
        assert_eq!(
            fake_model_env_override(ModelMode::LiveFromCredential),
            Some(("CURIE_FAKE_MODEL".to_string(), "0".to_string()))
        );
        assert_eq!(
            fake_model_env_override(ModelMode::FakePinnedDespiteCredential),
            None
        );
        assert_eq!(fake_model_env_override(ModelMode::DefaultFake), None);
    }

    #[test]
    fn up_live_from_credential_injects_fake_zero() {
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.model_mode = ModelMode::LiveFromCredential;
        let cmd = up_command(&o);
        assert!(
            cmd.env
                .contains(&(String::from("CURIE_FAKE_MODEL"), String::from("0"))),
            "live-from-credential must inject CURIE_FAKE_MODEL=0; env={:?}",
            cmd.env
        );
        assert!(
            cmd.display().contains("CURIE_FAKE_MODEL=0"),
            "display must show the injected env: {}",
            cmd.display()
        );
    }

    #[test]
    fn up_fake_pinned_does_not_inject() {
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.model_mode = ModelMode::FakePinnedDespiteCredential;
        let cmd = up_command(&o);
        assert!(
            !cmd.env.iter().any(|(k, _)| k == "CURIE_FAKE_MODEL"),
            "fake-pinned must leave compose's default alone; env={:?}",
            cmd.env
        );
    }

    #[test]
    fn up_default_fake_does_not_inject() {
        let cmd = up_command(&opts(DEFAULT_COMPOSE_FILE));
        assert!(
            !cmd.env.iter().any(|(k, _)| k == "CURIE_FAKE_MODEL"),
            "default-fake must leave compose's default alone; env={:?}",
            cmd.env
        );
    }

    #[test]
    fn up_local_model_unchanged_by_model_mode() {
        // --local-model owns the live env; a LiveFromCredential model_mode must
        // not duplicate or override it (exactly one CURIE_FAKE_MODEL=0, plus the
        // ollama routing env).
        let mut o = opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b");
        o.model_mode = ModelMode::LiveFromCredential;
        let cmd = up_command(&o);
        assert_eq!(
            cmd.env
                .iter()
                .filter(|(k, _)| k == "CURIE_FAKE_MODEL")
                .count(),
            1,
            "exactly one CURIE_FAKE_MODEL under --local-model; env={:?}",
            cmd.env
        );
        assert!(cmd
            .env
            .contains(&(String::from("CURIE_MODEL"), String::from("qwen3:4b"))));
        assert!(cmd.env.contains(&(
            String::from("CURIE_MODEL_BASE_URL"),
            String::from("http://ollama:11434"),
        )));
    }

    #[test]
    fn up_slack_appends_slack_profile() {
        let mut opts = opts(DEFAULT_COMPOSE_FILE);
        opts.slack = true;
        let cmd = up_command(&opts);
        assert_eq!(
            cmd.display(),
            "docker compose --profile full --profile slack -f compose.dev.yaml up -d --wait"
        );
    }

    #[test]
    fn up_minimal_slack_uses_core_and_slack() {
        let mut opts = opts(DEFAULT_COMPOSE_FILE);
        opts.minimal = true;
        opts.slack = true;
        let display = up_command(&opts).display();
        assert!(display.contains("--profile core"), "{display}");
        assert!(display.contains("--profile slack"), "{display}");
        assert!(!display.contains("--profile full"), "{display}");
    }

    #[test]
    fn up_local_model_and_slack_keep_profile_order() {
        let mut opts = opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b");
        opts.slack = true;
        let display = up_command(&opts).display();
        assert!(
            display.contains("--profile full --profile local-model --profile slack"),
            "{display}"
        );
    }

    #[test]
    fn up_minimal_uses_core_profile() {
        let mut o = opts(DEFAULT_COMPOSE_FILE);
        o.minimal = true;
        let cmd = up_command(&o);
        // The empty endpoint is `--minimal`'s collector suppression (the `core`
        // profile starts no collector); `display` renders env before the program.
        assert_eq!(
            cmd.display(),
            "OTEL_EXPORTER_OTLP_ENDPOINT= docker compose --profile core -f compose.dev.yaml up -d --wait"
        );
    }

    #[test]
    fn minimal_and_local_model_combine() {
        let mut o = opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b");
        o.minimal = true;
        let cmd = up_command(&o);
        let display = cmd.display();
        assert!(display.contains("--profile core"), "{display}");
        assert!(display.contains("--profile local-model"), "{display}");
        assert!(!display.contains("--profile full"), "{display}");
        assert!(cmd
            .env
            .contains(&(String::from("CURIE_MODEL"), String::from("qwen3:4b"))));
        assert!(cmd
            .env
            .contains(&(String::from("COMPOSE_PROJECT_NAME"), String::from("curie"),)));
    }

    #[test]
    fn status_runs_ps() {
        let cmd = status_command(&opts(DEFAULT_COMPOSE_FILE));
        assert_eq!(cmd.display(), "docker compose -f compose.dev.yaml ps");
    }

    #[test]
    fn down_keeps_volumes_by_default() {
        let cmd = down_command(&LocalDownOpts {
            common: opts(DEFAULT_COMPOSE_FILE),
            wipe: false,
            yes: false,
        });
        assert_eq!(
            cmd.display(),
            "docker compose --profile core --profile full --profile local-model --profile slack -f compose.dev.yaml down"
        );
    }

    #[test]
    fn down_wipe_adds_volume_flag() {
        let cmd = down_command(&LocalDownOpts {
            common: opts(DEFAULT_COMPOSE_FILE),
            wipe: true,
            yes: false,
        });
        assert_eq!(
            cmd.display(),
            "docker compose --profile core --profile full --profile local-model --profile slack -f compose.dev.yaml down -v"
        );
    }

    /// `down` must tear down every profile `up` can start, regardless of which
    /// flags this particular invocation carries. Concretely: a plain `local
    /// down` (no `--slack`) must still pass `--profile slack` so a
    /// forgot-to-disconnect dispatcher (`restart: unless-stopped`) is reaped
    /// instead of orphaned holding a live Socket Mode connection (issue #233).
    #[test]
    fn down_passes_every_up_profile() {
        // A default `local down` -- no --slack, no --minimal, no --local-model.
        let display = down_command(&LocalDownOpts {
            common: opts(DEFAULT_COMPOSE_FILE),
            wipe: false,
            yes: false,
        })
        .display();
        for profile in ["core", "full", "local-model", "slack"] {
            assert!(
                display.contains(&format!("--profile {profile}")),
                "down must pass --profile {profile}; got: {display}"
            );
        }
        // Every profile `up` can activate must be covered by `down`.
        for profile in up_activatable_profiles() {
            assert!(
                display.contains(&format!("--profile {profile}")),
                "down omits --profile {profile} that up can start; got: {display}"
            );
        }
    }

    #[test]
    fn custom_file_flows_through_every_verb() {
        let f = "compose.other.yaml";
        assert!(up_command(&opts(f))
            .display()
            .contains("-f compose.other.yaml"));
        assert!(status_command(&opts(f))
            .display()
            .contains("-f compose.other.yaml"));
        let down = down_command(&LocalDownOpts {
            common: opts(f),
            wipe: true,
            yes: true,
        });
        assert_eq!(
            down.display(),
            "docker compose --profile core --profile full --profile local-model --profile slack -f compose.other.yaml down -v"
        );
    }

    /// The endpoint constants are hardcoded; this asserts they still match the
    /// port mappings in the committed compose file (the "verify against the
    /// file" the task asks for, kept mechanical).
    #[test]
    fn endpoints_match_compose_file() {
        let compose = read_compose("compose.dev.yaml");
        // Each printed host port must appear as a `"<host>:<container>"` mapping.
        for (label, host_port) in [
            ("Curie API", "28000"),
            ("Curie Console", "28080"),
            ("Langfuse UI", "23000"),
            ("Postgres", "25432"),
            ("Valkey", "26379"),
            ("ClickHouse HTTP", "28123"),
            ("RustFS S3", "29000"),
            ("RustFS console", "29001"),
            ("OTel gRPC", "24317"),
            ("OTel HTTP", "24318"),
        ] {
            assert!(
                compose.contains(&format!("\"{host_port}:")),
                "compose.dev.yaml no longer maps host port {host_port} for {label}"
            );
            assert!(
                ENDPOINTS.iter().any(|(_, url, _)| url.contains(host_port)),
                "ENDPOINTS missing {host_port} for {label}"
            );
        }
        // The console must be advertised in wired mode (?api=1); the published UI
        // image is fixture-by-default and only talks to the API when the URL
        // carries this param.
        let console = ENDPOINTS
            .iter()
            .find(|(label, _, _)| *label == "Curie Console")
            .expect("Curie Console endpoint present");
        assert!(
            console.1.contains("api=1"),
            "Curie Console endpoint must be the wired ?api=1 URL, got {}",
            console.1
        );
    }

    // ADR 0093's preflight reads compose's model cache and inspects compose's
    // ollama image, but names both from Rust -- two sibling definitions of the
    // same fact, the drift shape this repo keeps getting bitten by. If the
    // compose file renames the volume or bumps the image tag without this
    // constant following, the preflight probes an empty volume and refuses a
    // machine that has the model, or inspects an image compose never runs and
    // waves through a machine that does not. Same guard shape as
    // `endpoints_match_compose_file` directly above.
    #[test]
    fn compose_ollama_volume_and_image_match_the_compose_file() {
        let compose = read_compose("compose.dev.yaml");
        // `up_command` pins COMPOSE_PROJECT_NAME=curie, so compose's declared
        // `ollama_data` volume is created as `curie_ollama_data`.
        assert!(
            compose.contains("\n  ollama_data:"),
            "compose.dev.yaml no longer declares the `ollama_data` volume that {COMPOSE_OLLAMA_VOLUME} names"
        );
        assert!(
            compose.contains("- ollama_data:/root/.ollama"),
            "compose.dev.yaml no longer mounts ollama_data at /root/.ollama, which is the path the preflight probes"
        );
        assert_eq!(
            COMPOSE_OLLAMA_VOLUME, "curie_ollama_data",
            "the volume name is <COMPOSE_PROJECT_NAME>_<declared volume>, and up_command pins the project to `curie`"
        );
        let project_pinned = up_command(&opts_with_local_model(DEFAULT_COMPOSE_FILE, "qwen3:4b"))
            .env
            .contains(&(String::from("COMPOSE_PROJECT_NAME"), String::from("curie")));
        assert!(
            project_pinned,
            "COMPOSE_OLLAMA_VOLUME's `curie_` prefix depends on up_command pinning the project name"
        );
        // The image the preflight inspects must be the image compose runs.
        assert!(
            compose.contains(&format!("image: {}", crate::commands::DEFAULT_OLLAMA_IMAGE)),
            "compose.dev.yaml no longer runs {}, so the preflight would inspect an image the stack never uses",
            crate::commands::DEFAULT_OLLAMA_IMAGE
        );
    }

    /// The 8 services that must carry `profiles: *core_profiles`.
    const CORE_SERVICES: &[&str] = &[
        "postgres",
        "valkey",
        "rustfs-perms",
        "rustfs",
        "rustfs-init",
        "curie-migrate",
        "curie-api",
        "curie-worker",
    ];

    /// The 6 services that must carry `profiles: *full_profiles`.
    const FULL_SERVICES: &[&str] = &[
        "clickhouse",
        "langfuse-worker",
        "langfuse-web",
        "otel-collector-perms",
        "otel-collector",
        "curie-ui",
    ];

    /// Return the YAML block for `service`: everything from its `  <service>:`
    /// header up to the next top-level (2-space-indented) service header. Used
    /// to assert a profile anchor lives inside the *right* service block, so a
    /// per-service profile swap fails the test rather than passing on counts.
    fn service_block<'a>(compose: &'a str, service: &str) -> &'a str {
        let header = format!("\n  {service}:\n");
        let start = compose
            .find(&header)
            .unwrap_or_else(|| panic!("service {service} not found"));
        let after = start + header.len();
        let rest = &compose[after..];
        // The next service header is the next "\n  " whose following char is not
        // a space (deeper-indented keys start with "\n    ").
        let end = rest
            .match_indices("\n  ")
            .find(|(i, _)| rest[i + 3..].starts_with(|c: char| c != ' '))
            .map(|(i, _)| i)
            .unwrap_or(rest.len());
        &rest[..end]
    }

    /// Assert the shared core(8)/full(6) profile binding in a compose file:
    /// the anchors are declared, the counts hold, AND each service block carries
    /// the anchor it should (so swapping a service's profile fails the test).
    fn assert_core_full_bindings(compose: &str, file: &str) {
        assert!(
            compose.contains("x-core-profiles: &core_profiles [core, full]"),
            "{file} missing core anchor"
        );
        assert!(
            compose.contains("x-full-profiles: &full_profiles [full]"),
            "{file} missing full anchor"
        );
        assert_eq!(
            compose.matches("profiles: *core_profiles").count(),
            8,
            "{file} core-profile count"
        );
        assert_eq!(
            compose.matches("profiles: *full_profiles").count(),
            6,
            "{file} full-profile count"
        );
        for service in CORE_SERVICES {
            let block = service_block(compose, service);
            assert!(
                block.contains("profiles: *core_profiles"),
                "{file}: {service} block must bind *core_profiles"
            );
            assert!(
                !block.contains("profiles: *full_profiles"),
                "{file}: {service} block must not bind *full_profiles"
            );
        }
        for service in FULL_SERVICES {
            let block = service_block(compose, service);
            assert!(
                block.contains("profiles: *full_profiles"),
                "{file}: {service} block must bind *full_profiles"
            );
            assert!(
                !block.contains("profiles: *core_profiles"),
                "{file}: {service} block must not bind *core_profiles"
            );
        }
    }

    /// Lock which endpoints are advertised under `--minimal`: exactly the five
    /// backed by a `core`-profile service. A core/full mislabel here would print
    /// a dead URL (or hide a live one) under `--minimal`.
    #[test]
    fn minimal_advertises_only_core_endpoints() {
        let core: Vec<&str> = ENDPOINTS
            .iter()
            .filter(|(_, _, is_core)| *is_core)
            .map(|(label, _, _)| *label)
            .collect();
        assert_eq!(
            core,
            vec![
                "Curie API",
                "Postgres",
                "Valkey",
                "RustFS S3",
                "RustFS console",
            ]
        );
    }

    #[test]
    fn compose_file_declares_core_and_full_profiles() {
        let compose = read_compose("compose.dev.yaml");
        assert_core_full_bindings(&compose, "compose.dev.yaml");
    }

    #[test]
    fn compose_file_makes_worker_slack_stub_overridable() {
        let compose = read_compose("compose.dev.yaml");
        assert!(compose.contains(
            "      - SLACK_API_BASE_URL=${SLACK_API_BASE_URL-http://localhost:8155/api/}"
        ));
        assert!(compose.contains("      - SLACK_BOT_TOKEN=${SLACK_BOT_TOKEN:-xoxb-dev}"));
    }

    /// The dev trio of trusted Slack origins (ADR-0096 D4.4) must stay on the
    /// compose worker, overridable by the operator. `local message` binds its
    /// reply stub on `localhost`/`127.0.0.1` (native Linux) or
    /// `host.docker.internal` (Docker Desktop), and the single
    /// `SLACK_API_BASE_URL` above cannot name all three, so without these the
    /// worker's origin pin refuses the local reply loop and the turn never
    /// finalizes. `compose/generate_release_compose.py` derives
    /// compose.release.yaml from this file by text transform, so the release
    /// asset (and the `local-release` ladder rung) inherits whatever this line
    /// says -- the two move together by construction, and this test is what
    /// keeps the dev half from being dropped. DEV ONLY: the chart's
    /// `worker.slackTrustedOrigins` default stays empty.
    #[test]
    fn compose_file_trusts_the_local_stub_origins() {
        let compose = read_compose("compose.dev.yaml");
        assert!(
            compose.contains(
                "      - CURIE_SLACK_TRUSTED_ORIGINS=${CURIE_SLACK_TRUSTED_ORIGINS-\
                 http://localhost,http://127.0.0.1,http://host.docker.internal}"
            ),
            "compose.dev.yaml must trust the local stub origins so the release \
             compose derived from it does too"
        );
    }

    #[test]
    fn compose_file_declares_slack_dispatcher_profile() {
        let compose = read_compose("compose.dev.yaml");
        let dispatcher = compose
            .split("  curie-dispatcher:")
            .nth(1)
            .expect("curie-dispatcher service present");
        assert!(dispatcher.contains("    profiles: [slack]"));
        assert!(!dispatcher.contains("profiles: *core_profiles"));
        assert!(!dispatcher.contains("profiles: *full_profiles"));
        assert!(dispatcher.contains("      VALKEY_HOST: valkey"));
        assert!(dispatcher.contains("      SLACK_APP_TOKEN: ${SLACK_APP_TOKEN:-}"));
    }
}
