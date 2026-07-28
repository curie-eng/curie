//! Command handlers behind the `curie` subcommands.
//!
//! main.rs owns the clap surface; each handler here owns one subcommand's
//! behavior and speaks only through the library modules (docker, runner, api,
//! scaffold, state, evals, render).

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::ValueEnum;
use curie_aci_protocol::{Budget, EventType, OutboundEvent, SessionStatus};
use serde::{Deserialize, Serialize};

use crate::api::{ApiClient, BudgetConfig, ChannelOutcome};
use crate::bundle::pack_tar_gz;
use crate::docker::{self, CheckSpec, StartSpec};
use crate::evals::{
    graded_answer, load_suite, outcome_label, rollup_line, turn_completed, turn_outcome,
    CaseOutcome, EvalSuite,
};
use crate::render::{boxed_summary, status_str, TurnPart, TurnPrinter};
use crate::runner::RunnerClient;
use crate::scaffold::{
    derive_plugin_name, read_declared_secrets, read_manifest, scaffold, scaffold_from_spec,
};
use crate::state::{self, RunnerState};

pub const DEFAULT_PORT: u16 = 7245; // the design canon's local bot port
pub const DEFAULT_BUDGET: &str = r#"{"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}"#;
pub const DEFAULT_LOCAL_MODEL: &str = "qwen3:4b";
pub const DEFAULT_OLLAMA_IMAGE: &str = "ollama/ollama:0.24.0";
pub const OLLAMA_PORT: u16 = 11434;

#[derive(Clone, Copy, ValueEnum)]
pub enum SendType {
    Message,
    Job,
    EvalCase,
}

impl From<SendType> for EventType {
    fn from(value: SendType) -> Self {
        match value {
            SendType::Message => EventType::Message,
            SendType::Job => EventType::Job,
            SendType::EvalCase => EventType::EvalCase,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
pub enum DeployEnv {
    Dev,
    Prod,
}

impl DeployEnv {
    pub fn as_str(self) -> &'static str {
        match self {
            DeployEnv::Dev => "dev",
            DeployEnv::Prod => "prod",
        }
    }
}

/// Options for `curie skill up`, mirroring its clap flags.
pub struct StartOpts {
    pub plugin_dir: PathBuf,
    pub image: String,
    pub port: u16,
    pub name: String,
    pub fake_model: bool,
    pub network: Option<String>,
    pub otel_endpoint: Option<String>,
    pub budget: String,
    pub model: Option<String>,
    pub local_model: Option<String>,
    /// Extra env var NAMES to forward by name into the runner sandbox, for a
    /// bundle's authed MCP server to read a secret. Forwarded exactly like the
    /// model credentials (docker reads the value from the caller's env; the
    /// value never appears in argv). From `skill up --secret <NAME>`.
    pub secret: Vec<String>,
    /// Opt-in path to a bundle-local `.env` read as the LOWEST-priority model-
    /// credential source (#749, ADR-0070): shell env > vault > this file. Only
    /// the recognized credential names are read; every other key is ignored.
    /// From `skill up --env-file <PATH>`.
    pub env_file: Option<PathBuf>,
    /// Remove a pre-existing container of the same name before booting, instead
    /// of failing on the conflict. From `skill up --replace` (#747).
    pub replace: bool,
}

/// The versioned report emitted by `curie_runner.check`.
#[derive(Debug, Deserialize, Serialize)]
pub struct CheckReport {
    pub check: String,
    pub version: u64,
    pub plugin_dir: String,
    pub declared: Vec<DeclaredServer>,
    /// Opaque pass-through of the runner's registered-server list. Never read by
    /// the human render (only round-tripped through `--json`), so it is kept as
    /// raw JSON: it round-trips losslessly and can never fail `parse_check_report`
    /// on a future tool/server shape.
    pub registered: Vec<serde_json::Value>,
    pub matches: Vec<CheckMatch>,
    pub verdict: String,
    pub reasons: Vec<String>,
    pub hints: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DeclaredServer {
    pub name: String,
    pub source: String,
    pub form: String,
    /// True when the server carries a credential (env/headers) the credential-free
    /// offline check never exercised. `#[serde(default)]` keeps older reports that
    /// predate the field parsing (they default to false).
    #[serde(default)]
    pub authed: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CheckMatch {
    pub declared: String,
    pub registered: Option<String>,
    pub connected: bool,
    pub tool_count: u64,
}

/// Parse the frozen runner to CLI check report contract.
pub fn parse_check_report(stdout: &str) -> Result<CheckReport> {
    let report: CheckReport = serde_json::from_str(stdout)
        .context("runner check output is not valid JSON for the check report contract")?;
    if report.version != 1 {
        bail!(
            "runner check report contract version {} is unsupported; expected version 1",
            report.version
        );
    }
    Ok(report)
}

/// Map a runner check verdict to the CLI semantic exit contract.
pub fn check_outcome(report: &CheckReport) -> std::result::Result<(), crate::exit::CliError> {
    match report.verdict.as_str() {
        "green" => Ok(()),
        "red" => Err(crate::exit::CliError {
            message: "MCP load check reported red".into(),
            fix: None,
            class: crate::exit::ExitClass::Failure,
        }
        // A structurally bad bundle is `invalid_bundle` (the runner's `run_check`
        // rejects it at step 1), so every remaining red cause is a runtime one:
        // a declared server that never registered or failed to start, one that
        // registered zero tools, one that needs a credential the offline check
        // never forwards, or MCP init exceeding the deadline. The printed
        // `reason:` lines say which, so point at them rather than guess.
        .with_fix(
            "read the printed reason(s): fix the server's command/args, forward its credential with curie skill up --secret <NAME>, or raise --timeout if MCP init ran long",
        )),
        "invalid_bundle" => {
            // An invalid bundle is a deterministic input error (exit 2, Usage),
            // matching the runner's own `check.py` exit-2 for this verdict: the
            // bundle dir exists but fails structural validation, so retrying the
            // same argv fails identically. Surface the structural `reasons` so
            // the user sees WHY the bundle is invalid.
            let mut message = String::from("MCP load check reported an invalid bundle");
            if !report.reasons.is_empty() {
                message.push_str(": ");
                message.push_str(&report.reasons.join("; "));
            }
            Err(crate::exit::CliError::usage(message).with_fix(
                "fix the reported bundle-structure errors (.claude-plugin/plugin.json and skills/) and run curie skill check again",
            ))
        }
        verdict => Err(crate::exit::CliError {
            message: format!("MCP load check reported unknown verdict '{verdict}'"),
            fix: None,
            class: crate::exit::ExitClass::Failure,
        }),
    }
}

/// Run the offline MCP load check for a plugin bundle and return its report.
///
/// Split out of `check` so a second caller (`info --check-mcp`) probes MCP load
/// through the identical container path instead of forking one: the report is
/// the shared product, and only what each caller does with it differs (`check`
/// emits it and maps the verdict to an exit; `info` folds it into `load`).
pub(crate) async fn run_check_report(
    plugin_dir: PathBuf,
    image: String,
    timeout_s: u64,
) -> Result<CheckReport> {
    let requested_dir = plugin_dir.display().to_string();
    let plugin_dir = plugin_dir.canonicalize().map_err(|err| {
        crate::exit::CliError::usage(format!("plugin dir not found: {requested_dir}: {err}"))
    })?;
    read_manifest(&plugin_dir).map_err(|err| {
        crate::exit::CliError::usage(format!("plugin dir is not a usable bundle: {err}"))
    })?;

    let spec = CheckSpec {
        image,
        plugin_dir: plugin_dir.display().to_string(),
        timeout_s,
    };
    let (status, stdout, stderr) = docker::docker_capture(&spec.run_args()).await?;
    // A container that DID run and produced parseable JSON is data (a
    // green/red/invalid verdict) regardless of its exit code. Only when the
    // stdout is NOT a valid report is this a real docker failure -- surface the
    // captured stderr (e.g. "Cannot connect to the Docker daemon") so the true
    // cause is visible instead of being dropped. Stays a plain Failure (exit 1);
    // Transient/exit 3 is reserved for reqwest connect/timeout errors (#323).
    let report = parse_check_report(&stdout).map_err(|err| {
        anyhow::anyhow!(
            "runner check output violated the check report contract: {err}; \
             docker exited {status}; stdout: {stdout}; stderr: {stderr}"
        )
    })?;

    Ok(report)
}

/// Run the offline MCP load check for a plugin bundle.
pub async fn check(plugin_dir: PathBuf, image: String, timeout_s: u64) -> Result<()> {
    let report = run_check_report(plugin_dir, image, timeout_s).await?;

    crate::ui::ui().emit(&CheckOutput { report: &report });
    check_outcome(&report).map_err(anyhow::Error::from)
}

/// Output of `skill check` (#474): the MCP-load report, structured under `--json`
/// and rendered line-by-line otherwise, routed through the one `Ui::emit` point.
/// Borrows the report so the caller can still pass it to `check_outcome`.
struct CheckOutput<'a> {
    report: &'a CheckReport,
}

impl crate::ui::CliOutput for CheckOutput<'_> {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self.report).unwrap_or_else(|_| serde_json::json!({}))
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let report = self.report;
        let mut lines = vec![format!("declared: {}", report.declared.len())];
        lines.extend(report.matches.iter().map(|entry| {
            format!(
                "match: {} -> {} (connected: {}, tools: {})",
                entry.declared,
                entry.registered.as_deref().unwrap_or("none"),
                entry.connected,
                entry.tool_count
            )
        }));
        lines.push(format!("verdict: {}", report.verdict));
        lines.extend(
            report
                .reasons
                .iter()
                .map(|reason| format!("reason: {reason}")),
        );
        lines.extend(report.hints.iter().map(|hint| format!("hint: {hint}")));
        ui.payload_plain(&lines.join("\n"));
    }
}

pub fn init(
    name: Option<String>,
    dir: Option<PathBuf>,
    from_spec: Option<PathBuf>,
    adopt: Option<PathBuf>,
) -> Result<()> {
    let ui = crate::ui::ui();

    // Spec-file path (ADR-0021 decision 5): fully non-interactive. The bundle
    // name comes from the spec, never a prompt.
    if let Some(spec_path) = from_spec {
        let body = std::fs::read_to_string(&spec_path)
            .with_context(|| format!("reading spec file {}", spec_path.display()))?;
        let spec = crate::spec::parse(&body)?;
        // A positional name is allowed only if it matches the spec's name; a
        // mismatch is an authoring error, not a silent override.
        if let Some(positional) = &name {
            if positional != &spec.name {
                bail!(
                    "positional name {:?} does not match the spec name {:?}; \
                     the bundle name comes from the spec -- omit the name or make them match",
                    positional,
                    spec.name
                );
            }
        }
        let dir = dir.unwrap_or_else(|| PathBuf::from(&spec.name));
        let created = scaffold_from_spec(&dir, &spec)?;
        report_scaffold(
            ui,
            spec.name.clone(),
            Some(spec_path.clone()),
            format!(
                "initialized plugin bundle '{}' in {} (from spec {})",
                spec.name,
                dir.display(),
                spec_path.display()
            ),
            created,
            &dir,
        );
        return Ok(());
    }

    // Adopt an existing directory (#745, ADR-0071): scaffold the plugin skeleton
    // INTO <dir> alongside whatever is already there, deriving the name from the
    // directory unless an explicit NAME overrides it. The logic port is the
    // operator's (docs/adopting-a-bundle.md); this only lays the skeleton.
    if let Some(adopt_dir) = adopt {
        if !adopt_dir.is_dir() {
            bail!(
                "--adopt {}: not a directory. Point it at the existing bundle to adopt.",
                adopt_dir.display()
            );
        }
        let name = match name {
            Some(name) => name,
            None => derive_plugin_name(&adopt_dir).ok_or_else(|| {
                anyhow::anyhow!(
                    "could not derive a kebab-case plugin name from {}; pass one explicitly: \
                     curie init <name> --adopt {}",
                    adopt_dir.display(),
                    adopt_dir.display()
                )
            })?,
        };
        let created = scaffold(&adopt_dir, &name)?;
        report_scaffold(
            ui,
            name.clone(),
            None,
            format!(
                "adopted {} as plugin bundle '{name}' -- scaffolded the skeleton alongside \
                 your existing files. Port your agent's logic into skills/{name}/SKILL.md and \
                 .mcp.json (see docs/adopting-a-bundle.md), then run `curie skill up`.",
                adopt_dir.display()
            ),
            created,
            &adopt_dir,
        );
        return Ok(());
    }

    let name = match name {
        Some(name) => name,
        None => bail!("provide a plugin NAME, --from-spec <path>, or --adopt <dir>"),
    };
    let dir = dir.unwrap_or_else(|| PathBuf::from(&name));
    let created = scaffold(&dir, &name)?;
    report_scaffold(
        ui,
        name.clone(),
        None,
        format!("initialized plugin bundle '{name}' in {}", dir.display()),
        created,
        &dir,
    );
    Ok(())
}

/// Report a freshly scaffolded bundle through the one success-path decision point
/// (`Ui::emit`, issue #485): under `--json` emit one structured `InitOutput`
/// object to stdout; otherwise render the success line, a `created` note per
/// written path, and the `Next:` hint on stderr (byte-identical to before).
/// Shared by both `init` branches so the only per-branch difference is the
/// success message text and whether a spec sourced the bundle.
fn report_scaffold(
    ui: &crate::ui::Ui,
    name: String,
    from_spec: Option<PathBuf>,
    success_msg: String,
    created: Vec<PathBuf>,
    dir: &Path,
) {
    ui.emit(&InitOutput {
        name,
        dir: dir.to_path_buf(),
        from_spec,
        created,
        success_msg,
    });
}

/// The result of `curie init` (both the plain-name and `--from-spec` branches),
/// carried through `Ui::emit`. Under `--json` an agent gets the bundle name, the
/// directory, the spec source (null for the plain-name path), the list of created
/// paths, and the next-step command -- never empty stdout (issue #485). Owns its
/// data so `to_json`/`render` outlive the scaffold call.
pub struct InitOutput {
    pub name: String,
    pub dir: PathBuf,
    pub from_spec: Option<PathBuf>,
    pub created: Vec<PathBuf>,
    pub success_msg: String,
}

impl InitOutput {
    /// The copy-pasteable next-step command. The dir is shell-quoted (only when
    /// it carries a special char -- a kebab bundle name stays bare) so a path
    /// with a space yields a valid `cd`, not a broken two-token one. Shared by
    /// `to_json` and `render` so the machine and human forms never drift.
    fn next_command(&self) -> String {
        format!(
            "cd {} && curie skill up",
            crate::ops::shell_quote(&self.dir.display().to_string())
        )
    }
}

impl crate::ui::CliOutput for InitOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "initialized": true,
            "name": self.name,
            "dir": self.dir.display().to_string(),
            "from_spec": self.from_spec.as_ref().map(|p| p.display().to_string()),
            "created": self
                .created
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>(),
            "next": self.next_command(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.success(&self.success_msg);
        for path in &self.created {
            ui.note(&format!("created {}", path.display()));
        }
        ui.note(&format!("Next: {}", self.next_command()));
    }
}

/// `curie build`: build the runner image locally from the repo's Dockerfile.
/// The one-command equivalent of `docker build -f runner/Dockerfile -t <tag> .`
/// run from the repo root. Errors clearly when Docker is missing or when run
/// outside a source checkout (a release binary pulls the image from GHCR).
pub async fn build(tag: &str) -> Result<()> {
    let ui = crate::ui::ui();
    if !on_path("docker") {
        bail!(
            "Docker is not installed or not on PATH. Install Docker \
             (https://docs.docker.com/get-docker/) and retry."
        );
    }
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie build` \
         from a curie repo checkout -- a release binary pulls the runner image from GHCR \
         automatically and never needs to build.",
    )?;
    ui.note(&format!(
        "=== docker build -f runner/Dockerfile -t {tag} . (in {}) ===",
        root.display()
    ));
    // Inherit stdio so the build log streams to the terminal like a hand-run build.
    let status = tokio::process::Command::new("docker")
        .args(["build", "-f", "runner/Dockerfile", "-t", tag, "."])
        .current_dir(&root)
        .status()
        .await
        .context("failed to invoke docker")?;
    if !status.success() {
        bail!("docker build failed ({status})");
    }
    ui.success(&format!("built runner image '{tag}'"));
    Ok(())
}

/// `curie install`: from-a-checkout dev bootstrap/update -- install deps and
/// build the runner image, but start nothing. Each step is idempotent and
/// streams its output; update mode reuses already-present heavyweight artifacts.
/// A missing tool prints a friendly pointer and stops. A release binary has no
/// source tree to install, so this errors clearly outside a checkout.
pub async fn install(update: bool) -> Result<()> {
    let ui = crate::ui::ui();
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie install` \
         from a curie source checkout -- a release binary has nothing to install.",
    )?;

    // 1. Local config is user-owned. It is gitignored and only created once,
    // so pulling newer Curie sources and rerunning install cannot replace it.
    match seed_env_if_missing(&root)? {
        EnvSeed::Preserved => ui.note("=== .env already exists; leaving it untouched ==="),
        EnvSeed::Created => ui.note("=== seeded .env from .env.example ==="),
        EnvSeed::NoTemplate => ui.note("=== no .env.example to seed .env from; skipping ==="),
    }

    // 2. uv sync (repo root).
    require_tool("uv", "uv is not installed - https://docs.astral.sh/uv/")?;
    run_step(&root, "uv", &["sync"], "uv sync").await?;

    // 3. pnpm install in apps/ui.
    require_tool(
        "pnpm",
        "pnpm is not installed - https://pnpm.io/installation",
    )?;
    run_step(
        &root.join("apps/ui"),
        "pnpm",
        &["install"],
        "pnpm install (apps/ui)",
    )
    .await?;

    // 4. cargo install the CLI onto PATH (~/.cargo/bin), not just `cargo build`
    // into target/debug. `install` should make the CLI it builds LIVE -- like
    // `npm i` reconciling to the manifest -- so re-running it after a code change
    // refreshes what the user actually runs, instead of silently leaving a stale
    // on-PATH binary. `curie update` is the fast CLI-only subset of this.
    require_tool("cargo", "cargo is not installed - https://rustup.rs/")?;
    run_step(
        &root,
        "cargo",
        &["install", "--path", "cli", "--force"],
        "cargo install (cli -> ~/.cargo/bin)",
    )
    .await?;

    // 5. Build the runner image via the existing `build` handler. Update mode
    // keeps reruns quick when the image is already present locally.
    let runner_image = docker::RUNNER_IMAGE;
    if update && docker_image_exists(runner_image).await? {
        ui.note(&format!(
            "=== runner image '{runner_image}' already exists; skipping rebuild for --update ==="
        ));
    } else {
        build(runner_image).await?;
    }

    ui.success("Setup complete. Start the stack with: curie local up");
    Ok(())
}

/// `curie update`: rebuild the CLI from this source checkout and reinstall it
/// on PATH (`cargo install --path cli --force` -> ~/.cargo/bin), so a code change
/// is picked up on the next `curie` invocation without re-running the bootstrap
/// script. Optionally rebuilds the local runner image too. Source-checkout only,
/// like `install` -- a release binary has no source to rebuild from. Replacing the
/// running binary is safe: the current process keeps running from the old inode
/// and the next invocation is the freshly installed one.
pub async fn update(image: bool) -> Result<()> {
    let ui = crate::ui::ui();
    // `update` rebuilds from a source checkout; a release-installed binary has no
    // checkout to rebuild from. Point that user at the release assets instead of
    // the generic install error, and be explicit that self-update-from-release is
    // not built here (#443 review).
    let root = find_repo_root().ok_or_else(|| {
        crate::exit::usage(
            "`curie update` rebuilds the CLI from a source checkout, but this binary is not \
             running inside one.\n  - From a git clone: run `curie update` from the repo.\n  \
             - Installed from a GitHub release: download the latest curie-<target> asset from \
             https://github.com/curie-eng/curie/releases and replace this binary (updating a \
             released binary from the latest release is not built yet).",
        )
    })?;
    require_tool("cargo", "cargo is not installed - https://rustup.rs/")?;
    run_step(
        &root,
        "cargo",
        &["install", "--path", "cli", "--force"],
        "cargo install (cli -> ~/.cargo/bin)",
    )
    .await?;
    if image {
        build(docker::RUNNER_IMAGE).await?;
    }
    ui.success("curie updated. The new binary is live on your next `curie` invocation.");
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EnvSeed {
    Preserved,
    Created,
    NoTemplate,
}

fn seed_env_if_missing(root: &Path) -> Result<EnvSeed> {
    let env_path = root.join(".env");
    if env_path.exists() {
        return Ok(EnvSeed::Preserved);
    }
    let env_example = root.join(".env.example");
    if !env_example.exists() {
        return Ok(EnvSeed::NoTemplate);
    }
    std::fs::copy(&env_example, &env_path).context("failed to copy .env.example to .env")?;
    Ok(EnvSeed::Created)
}

/// `curie dev <script>`: run a repo dev script by relative path. Thin wrapper
/// -- finds the repo root, confirms the script exists, shells `bash <script>`
/// from the root, streams its output, and propagates its exit code. A release
/// binary has no scripts, so this errors clearly outside a checkout.
pub async fn dev_script(rel_path: &str) -> Result<()> {
    let ui = crate::ui::ui();
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie dev` \
         from a curie source checkout -- a release binary has no dev scripts.",
    )?;
    let script = root.join(rel_path);
    if !script.is_file() {
        bail!("script not found: {}", script.display());
    }
    ui.note(&format!("=== bash {rel_path} (in {}) ===", root.display()));
    let status = tokio::process::Command::new("bash")
        .arg(rel_path)
        .current_dir(&root)
        .status()
        .await
        .context("failed to invoke bash")?;
    if !status.success() {
        bail!("{rel_path} failed ({status})");
    }
    Ok(())
}

/// `curie list-agents`: list the plugin bundles under `agents/`, a personal,
/// gitignored directory (sibling of `examples/`) for in-progress agent
/// projects ready to hand to `curie deploy-local <folder>`. A release binary has
/// no checkout to scan, so this errors clearly outside one, same as `dev_script`.
pub async fn list_agents() -> Result<()> {
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie list-agents` \
         from a curie source checkout.",
    )?;
    let bundles = crate::discover::discover_bundles(&root.join("agents"))?;
    crate::ui::ui().emit(&ListAgentsOutput {
        agents: bundles
            .into_iter()
            .map(|b| LocalAgentSummary {
                name: b.name,
                description: b.description,
                directory: b.directory.display().to_string(),
            })
            .collect(),
    });
    Ok(())
}

pub struct LocalAgentSummary {
    pub name: String,
    pub description: String,
    pub directory: String,
}

/// Output of `list-agents`. Routes through the one `Ui::emit` point rather
/// than an inline `if json()` branch (mirrors `secrets list`'s
/// `SecretsListOutput`). Public so the schema contract test (#634) can build one
/// and validate `to_json` against `cli/schema/list-agents.schema.json`.
pub struct ListAgentsOutput {
    pub agents: Vec<LocalAgentSummary>,
}

impl crate::ui::CliOutput for ListAgentsOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "agents": self.agents.iter().map(|a| serde_json::json!({
                "name": a.name,
                "description": a.description,
                "directory": a.directory,
            })).collect::<Vec<_>>(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if self.agents.is_empty() {
            ui.note("no local agents under agents/ (none found, or the directory doesn't exist)");
        } else {
            let lines: Vec<String> = self
                .agents
                .iter()
                .map(|a| format!("{} -- {} ({})", a.name, a.description, a.directory))
                .collect();
            ui.payload_plain(&lines.join("\n"));
        }
    }
}

/// Resolve `agents/<folder>` under the repo root to a bundle directory for
/// `curie deploy-local <folder>`. Errors with the available folder names (from
/// `discover::discover_bundles`) when `folder` doesn't match one, so a typo
/// doesn't dead-end without a next step.
fn resolve_agent_folder(folder: &str) -> Result<std::path::PathBuf> {
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie deploy-local` from \
         a curie source checkout.",
    )?;
    let agents_root = root.join("agents");
    let dir = agents_root.join(folder);
    if dir.join(".claude-plugin/plugin.json").is_file() {
        return Ok(dir);
    }
    let available = crate::discover::discover_bundles(&agents_root)?;
    if available.is_empty() {
        bail!(
            "no agent bundle named {folder:?} under agents/ (the directory has no bundles yet -- \
             create one with `curie init` inside agents/{folder})"
        );
    }
    let names: Vec<String> = available
        .iter()
        .filter_map(|b| b.directory.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .collect();
    bail!(
        "no agent bundle named {folder:?} under agents/. Available: {}",
        names.join(", ")
    );
}

/// `curie deploy-local <folder>`: shorthand for
/// `curie local deploy --plugin-dir agents/<folder>` -- same underlying
/// `deploy()` call, just resolved by name instead of a hand-typed path. Local
/// tier only: cluster deploy's API-key discovery and port-forward
/// self-plumbing (`main.rs`'s `ClusterAction::Deploy` arm) is not duplicated
/// here; use `curie cluster deploy --plugin-dir agents/<folder>` directly
/// for that tier.
pub async fn deploy_named(folder: &str, opts: DeployNamedOpts) -> Result<DeployOutput> {
    let plugin_dir = resolve_agent_folder(folder)?;
    let connect_hint = format!(
        "the platform API at {} is unreachable. Start the local stack first with `curie local \
         up`, then re-run (or pass --api-url if your API is elsewhere).",
        opts.api_url
    );
    deploy(DeployOpts {
        plugin_dir,
        api_url: opts.api_url,
        api_key: opts.api_key,
        slack_channel: opts.slack_channel,
        env: opts.env,
        label: opts.label,
        secret: opts.secret,
        secret_binding_supported: true,
        connect_hint,
    })
    .await
}

/// The `curie deploy-local <folder>` flags, mirroring `local deploy`'s minus
/// `plugin_dir` (resolved from `folder` instead).
pub struct DeployNamedOpts {
    pub api_url: String,
    pub api_key: String,
    pub slack_channel: Option<String>,
    pub env: DeployEnv,
    pub label: Option<String>,
    pub secret: Vec<String>,
}

/// `curie dev bump-version <X.Y.Z>`: set the release-coupled version across
/// cli/Cargo.toml + Chart.yaml version/appVersion in one shot, so a release cut
/// cannot leave the three out of sync (the drift the #489 consistency gate
/// catches). It rewrites ONLY the line-anchored release fields (never a
/// dependency `version = ` line), refreshes the CLI lockfile, and prints the
/// commit + tag follow-up -- it does not commit, tag, or push. `--dry-run` prints
/// the planned edits and writes nothing.
pub async fn bump_version(version: &str, dry_run: bool) -> Result<()> {
    let ui = crate::ui::ui();
    // semver X.Y.Z with an optional -rc.N (the only pre-release shape we cut).
    let semver = regex::Regex::new(r"^\d+\.\d+\.\d+(-rc\.\d+)?$").expect("static regex");
    if !semver.is_match(version) {
        return Err(crate::exit::usage(format!(
            "version {version:?} must be semver X.Y.Z or X.Y.Z-rc.N"
        )));
    }
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie dev \
         bump-version` from a curie source checkout.",
    )?;

    let cargo_path = root.join("cli/Cargo.toml");
    let chart_path = root.join("charts/curie/Chart.yaml");
    let cargo = std::fs::read_to_string(&cargo_path)
        .with_context(|| format!("reading {}", cargo_path.display()))?;
    let chart = std::fs::read_to_string(&chart_path)
        .with_context(|| format!("reading {}", chart_path.display()))?;

    // Line-anchored so a dependency `version = "x"` line is never touched: only
    // the first `version = ` at column 0 (the [package] version) is rewritten.
    let cargo_new = replace_first_line(&cargo, "version = ", &format!("version = \"{version}\""))
        .context("cli/Cargo.toml has no top-level `version = ` line")?;
    let chart_new_v = replace_first_line(&chart, "version:", &format!("version: {version}"))
        .context("Chart.yaml has no `version:` line")?;
    let chart_new = replace_first_line(
        &chart_new_v,
        "appVersion:",
        &format!("appVersion: \"{version}\""),
    )
    .context("Chart.yaml has no `appVersion:` line")?;

    if dry_run {
        ui.emit(&crate::ui::DryRunPlan {
            lines: vec![
                format!("cli/Cargo.toml: version = \"{version}\""),
                format!("charts/curie/Chart.yaml: version: {version}"),
                format!("charts/curie/Chart.yaml: appVersion: \"{version}\""),
                "cargo update -p curie (refresh Cargo.lock)".to_string(),
            ],
        });
        return Ok(());
    }

    std::fs::write(&cargo_path, cargo_new)
        .with_context(|| format!("writing {}", cargo_path.display()))?;
    std::fs::write(&chart_path, chart_new)
        .with_context(|| format!("writing {}", chart_path.display()))?;
    ui.note(&format!(
        "set version {version} in cli/Cargo.toml and charts/curie/Chart.yaml"
    ));

    // Refresh the CLI lockfile so the committed Cargo.lock matches the new crate
    // version. Best-effort: a missing cargo or offline registry must not fail the
    // bump (the fields are already written); warn and let the operator run it.
    let lock_ok = tokio::process::Command::new("cargo")
        .args(["update", "-p", "curie", "--precise", version])
        .current_dir(root.join("cli"))
        .status()
        .await
        .map(|s| s.success())
        .unwrap_or(false);
    if !lock_ok {
        ui.warn(
            "could not refresh cli/Cargo.lock automatically; run `cargo update -p curie` in cli/",
        );
    }

    ui.emit(&BumpVersionOutput {
        version: version.to_string(),
    });
    Ok(())
}

/// Replace the first line beginning with `prefix` (after optional leading
/// whitespace) with `replacement`, preserving the line's indentation. Returns
/// None when no such line exists.
fn replace_first_line(content: &str, prefix: &str, replacement: &str) -> Option<String> {
    let mut out = Vec::new();
    let mut replaced = false;
    for line in content.lines() {
        if !replaced && line.trim_start().starts_with(prefix) {
            let indent = &line[..line.len() - line.trim_start().len()];
            out.push(format!("{indent}{replacement}"));
            replaced = true;
        } else {
            out.push(line.to_string());
        }
    }
    if !replaced {
        return None;
    }
    let mut joined = out.join("\n");
    if content.ends_with('\n') {
        joined.push('\n');
    }
    Some(joined)
}

/// Output of `dev bump-version`: the version now set across the release fields.
#[derive(Debug)]
pub struct BumpVersionOutput {
    pub version: String,
}

impl crate::ui::CliOutput for BumpVersionOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({"version": self.version})
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.payload(&format!("bumped release version to {}", self.version));
        ui.note(&format!(
            "commit the change, then tag it: git commit -am \"release {v}\" && git tag v{v}",
            v = self.version
        ));
    }
}

/// Bail with a friendly pointer when a required tool is not on PATH.
fn require_tool(bin: &str, hint: &str) -> Result<()> {
    if on_path(bin) {
        Ok(())
    } else {
        bail!("{hint}")
    }
}

/// Run one install step in `dir`, streaming its output and failing on nonzero.
async fn run_step(dir: &Path, bin: &str, args: &[&str], label: &str) -> Result<()> {
    let ui = crate::ui::ui();
    ui.note(&format!("=== {label} (in {}) ===", dir.display()));
    let status = tokio::process::Command::new(bin)
        .args(args)
        .current_dir(dir)
        .status()
        .await
        .with_context(|| format!("failed to invoke {bin}"))?;
    if !status.success() {
        bail!("{label} failed ({status})");
    }
    Ok(())
}

async fn docker_image_exists(tag: &str) -> Result<bool> {
    require_tool(
        "docker",
        "Docker is not installed or not on PATH. Install Docker Desktop/Engine and retry.",
    )?;
    let status = tokio::process::Command::new("docker")
        .args(["image", "inspect", tag])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .context("failed to invoke docker")?;
    Ok(status.success())
}

/// Whether `bin` resolves on PATH.
fn on_path(bin: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(bin).is_file()))
        .unwrap_or(false)
}

/// Walk up from the current directory to the repo root: the nearest ancestor
/// that contains `runner/Dockerfile`.
fn find_repo_root() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join("runner/Dockerfile").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// Pick the model-credential env vars to forward into the runner container, BY
/// NAME (docker reads their values from the caller's env; no secret is put in
/// argv). Mirrors the worker docker substrate's positive single-credential
/// selection (apps/worker/src/curie_worker/sandbox/docker.py:199-207), which is
/// the authority this function mirrors. Three states:
/// - `fake_model`: forward NONE, and fake dominates every other state -- a fake
///   runner resolves no Anthropic credential, and a real token must not sit in an
///   untrusted, egress-rail-less container readable via /proc/1/environ.
/// - an explicit non-empty CURIE_CREDENTIALS (`byo_credential`): the operator's
///   chosen BYO credential, forwarded ALONE so an ambient SDK token can neither
///   shadow it nor ride into the sandbox. Kept under a `base_url_override` when it
///   is a provider key -- the runner routes an sk-or- OpenRouter key into
///   ANTHROPIC_API_KEY with a preset base URL, so dropping it would break BYO
///   OpenRouter -- but DROPPED under an override when it is OAuth-shaped
///   (`sk-ant-oat`): the runner blanks such a token behind an override
///   (runner sdk_auth.resolve_sdk_env), so forwarding it authenticates nothing and
///   only lands a real token in the container's /proc/1/environ (issue #603).
/// - otherwise: the ambient SDK creds for the legacy real-Anthropic path, each
///   only when `ambient_present` reports it, and only when there is no
///   `base_url_override` -- a local endpoint needs no real Anthropic token.
///
/// The rule is frozen as data in tests/vectors/model-credential-forwarding.json,
/// which both this lane and the worker lane assert against: changing the rule
/// here without changing the worker (or the vectors) fails that gate (issue #495).
pub(crate) fn select_passthrough_env(
    fake_model: bool,
    base_url_override: bool,
    byo_credential: Option<&str>,
    ambient_present: &dyn Fn(&str) -> bool,
) -> Vec<String> {
    if fake_model {
        return Vec::new();
    }
    if let Some(cred) = byo_credential.filter(|c| !c.is_empty()) {
        // An OAuth-shaped token under a base-URL override authenticates nothing
        // (the runner blanks it), so drop it rather than leave a real token inert
        // in /proc/1/environ; a provider key is still routed and kept (issue #603).
        if base_url_override && cred.starts_with(OAUTH_TOKEN_PREFIX) {
            return Vec::new();
        }
        return vec!["CURIE_CREDENTIALS".into()];
    }
    if base_url_override {
        return Vec::new();
    }
    ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"]
        .into_iter()
        .filter(|name| ambient_present(name))
        .map(String::from)
        .collect()
}

/// A Claude Code OAuth token shares the sk-ant- prefix with an API key; this more
/// specific prefix marks it (issue #603). A literal mirror of
/// runner/src/curie_runner/sdk_auth.py::OAUTH_TOKEN_PREFIX, the authority for the
/// prefix semantics, and of the worker lane's `_OAUTH_TOKEN_PREFIX`.
const OAUTH_TOKEN_PREFIX: &str = "sk-ant-oat";

/// Append `--secret` env var NAMES to the model-credential passthrough list,
/// de-duplicating. Unlike the model credential these are NOT suppressed under a
/// fake/local model run: a bundle's authed MCP server needs its token
/// regardless of which model drives the session. Names already present (a user
/// passing a model-credential var as a secret) are not duplicated.
fn merge_secret_env(mut passthrough: Vec<String>, secrets: &[String]) -> Vec<String> {
    for name in secrets {
        if !passthrough.contains(name) {
            passthrough.push(name.clone());
        }
    }
    passthrough
}

/// Is `name` exported with a usable value?
///
/// An empty-string credential is absent, not supplied (issue #540): `var_os`
/// alone reports `NAME=""` as present, which would suppress the vault fallback
/// and forward nothing usable. Mirrors `ops.rs::resolve_up_credentials` and
/// `interactive.rs::env_credential_present`.
pub(crate) fn env_credential_present(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| !value.is_empty())
}

fn secret_store_env(name: &str) -> Result<Option<(String, String)>> {
    if env_credential_present(name) {
        return Ok(None);
    }
    if !crate::secrets::is_saved(name)? {
        return Ok(None);
    }
    if let Some(value) = crate::secrets::get_value(name)? {
        crate::ui::ui().note(&format!(
            "{name}: loaded from Curie private storage for this run"
        ));
        return Ok(Some((name.to_string(), value)));
    }
    Ok(None)
}

fn stored_env_contains(env: &[(String, String)], name: &str) -> bool {
    env.iter().any(|(stored_name, _)| stored_name == name)
}

/// The ambient-presence rule `select_passthrough_env` selects on.
///
/// Presence must match what `StartSpec::run_args` later filters the NAMES on
/// (docker.rs:117), or selection and emission disagree.
fn ambient_present_for(docker_env: &[(String, String)]) -> impl Fn(&str) -> bool + '_ {
    move |name| std::env::var_os(name).is_some() || stored_env_contains(docker_env, name)
}

fn load_model_credentials_from_secret_store() -> Result<Vec<(String, String)>> {
    // Prefer an explicitly BYO Curie credential when saved, otherwise hydrate
    // the SDK credential names in the same order `select_passthrough_env` uses.
    if let Some(pair) = secret_store_env("CURIE_CREDENTIALS")? {
        return Ok(vec![pair]);
    }
    let mut env = Vec::new();
    if let Some(pair) = secret_store_env("CLAUDE_CODE_OAUTH_TOKEN")? {
        env.push(pair);
    }
    if let Some(pair) = secret_store_env("ANTHROPIC_API_KEY")? {
        env.push(pair);
    }
    Ok(env)
}

/// The model-credential names, in the precedence order the vault loader uses
/// (`CURIE_CREDENTIALS` dominates the SDK pair). These are the ONLY keys read
/// from an opt-in `--env-file` (#749, ADR-0070); every other key in the dotfile
/// is ignored, never absorbed into any process env.
const MODEL_CREDENTIAL_ENV_NAMES: [&str; 3] = [
    "CURIE_CREDENTIALS",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_API_KEY",
];

/// Parse just the recognized model-credential names out of a dotenv file,
/// dropping every other key. An empty value is absent, not supplied (issue
/// #540), so it is dropped too. A missing/unreadable file is a hard error --
/// `--env-file` is an explicit opt-in, so pointing it at nothing is a mistake.
pub(crate) fn parse_credential_env_file(path: &Path) -> Result<Vec<(String, String)>> {
    let mut found = Vec::new();
    for item in dotenvy::from_path_iter(path)
        .with_context(|| format!("reading --env-file {}", path.display()))?
    {
        let (key, value) =
            item.with_context(|| format!("parsing --env-file {}", path.display()))?;
        if MODEL_CREDENTIAL_ENV_NAMES.contains(&key.as_str()) && !value.is_empty() {
            found.push((key, value));
        }
    }
    Ok(found)
}

/// Which parsed `.env` credentials to add, given what a higher-priority source
/// already supplied (`is_present`: shell env OR vault). Pure, so the precedence
/// (#749: shell env > vault > file) is unit-testable without touching the
/// process env. Mirrors `load_model_credentials_from_secret_store`'s shape:
/// `CURIE_CREDENTIALS` dominates and suppresses the SDK pair, matching
/// `select_passthrough_env`'s byo branch.
pub(crate) fn resolve_env_file_credentials(
    parsed: &[(String, String)],
    is_present: &dyn Fn(&str) -> bool,
) -> Vec<(String, String)> {
    let take = |name: &str| -> Option<(String, String)> {
        if is_present(name) {
            return None;
        }
        parsed
            .iter()
            .find(|(key, _)| key == name)
            .map(|(key, value)| (key.clone(), value.clone()))
    };
    if let Some(pair) = take("CURIE_CREDENTIALS") {
        return vec![pair];
    }
    // A BYO credential from a higher source dominates: the SDK pair is never
    // forwarded alongside it, so do not pull it from the file either.
    if is_present("CURIE_CREDENTIALS") {
        return Vec::new();
    }
    ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"]
        .into_iter()
        .filter_map(take)
        .collect()
}

/// Load model credentials from an opt-in bundle `.env` as the LOWEST-priority
/// source (#749, ADR-0070). `already` is the vault-hydrated `docker_env`, so a
/// name supplied by the shell env or the vault always wins; only a name missing
/// from both is taken from the file.
fn load_model_credentials_from_env_file(
    env_file: Option<&Path>,
    already: &[(String, String)],
) -> Result<Vec<(String, String)>> {
    let Some(path) = env_file else {
        return Ok(Vec::new());
    };
    let parsed = parse_credential_env_file(path)?;
    let is_present =
        |name: &str| env_credential_present(name) || stored_env_contains(already, name);
    let resolved = resolve_env_file_credentials(&parsed, &is_present);
    for (name, _) in &resolved {
        crate::ui::ui().note(&format!(
            "{name}: loaded from --env-file {} for this run",
            path.display()
        ));
    }
    Ok(resolved)
}

/// What a recorded runner means for a fresh `skill up` (#747).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedStatePlan {
    /// Nothing recorded; boot.
    Proceed,
    /// `--replace` names the very container the record describes, so the record
    /// is about to become stale anyway: clear it and boot.
    ClearAndProceed,
    /// A runner is recorded that `--replace` does not cover; refuse, so a second
    /// bundle's live runner cannot be silently forgotten.
    Refuse,
}

/// Resolve the recorded-state gate. Pure so both branches are testable without a
/// bundle on disk. `--replace` only clears the record when it is replacing that
/// exact container: a record naming a DIFFERENT runner still blocks, since
/// removing one container is no reason to forget another.
pub fn plan_recorded_state(
    recorded: Option<&str>,
    target: &str,
    replace: bool,
) -> RecordedStatePlan {
    match recorded {
        None => RecordedStatePlan::Proceed,
        Some(container) if replace && container == target => RecordedStatePlan::ClearAndProceed,
        Some(_) => RecordedStatePlan::Refuse,
    }
}

pub async fn start(opts: StartOpts) -> Result<()> {
    let plugin_dir = opts
        .plugin_dir
        .canonicalize()
        .with_context(|| format!("plugin dir not found: {}", opts.plugin_dir.display()))?;
    // Fail fast on a directory that is not a bundle; the runner would reject
    // it at boot anyway (real-model mode), with a worse error surface.
    let (plugin_name, manifest_version) = read_manifest(&plugin_dir)?;

    if opts.local_model.is_some() && opts.fake_model {
        return Err(crate::exit::usage(
            "--local-model cannot be combined with --fake-model",
        ));
    }
    if opts.local_model.is_some() && opts.model.is_some() {
        return Err(crate::exit::usage(
            "--local-model cannot be combined with --model",
        ));
    }

    // Decided here, ACTED ON below: refusing is free, but replacing tears down a
    // live runner and must not happen until nothing cheap can still abort (#747).
    let recorded_runner = state::load(&plugin_dir)?;
    let recorded_plan = plan_recorded_state(
        recorded_runner.as_ref().map(|s| s.container_name.as_str()),
        &opts.name,
        opts.replace,
    );
    if recorded_plan == RecordedStatePlan::Refuse {
        return Err(crate::exit::usage(format!(
            "a local runner is already recorded in {}/.curie/runner.json; run 'curie skill down' there first",
            plugin_dir.display()
        )));
    }

    // Parse (not just forward) the budget so a typo fails here, not in-container.
    let _: Budget = serde_json::from_str(&opts.budget).map_err(|e| {
        crate::exit::usage(format!(
            "--budget is not a valid ACI budget: {}: {e}",
            opts.budget
        ))
    })?;

    // The replacement itself, once every cheap validation has passed. Tearing
    // down the record means EVERYTHING it describes -- container, ollama sidecar,
    // network, then the file -- because clearing the record while its runner is
    // still live strands exactly the untracked orphan this ticket removes (#747).
    if let (RecordedStatePlan::ClearAndProceed, Some(saved)) = (recorded_plan, recorded_runner) {
        crate::ui::ui().note(&format!(
            "--replace: tearing down the recorded runner '{}' first",
            saved.container_name
        ));
        let live = docker::container_facts(&saved.container_name).await?;
        stop_recorded(&plugin_dir, crate::ui::ui(), saved, live.as_ref()).await?;
    }

    // Catch a leftover container of the same name here, before anything is
    // booted, so the operator gets the remedies instead of docker's raw
    // exit-125 conflict at the very end of the boot (#747).
    docker::ensure_container_name_free(
        &opts.name,
        Some(opts.port),
        opts.replace,
        docker::ConflictContext::SkillUp,
    )
    .await?;

    let session_id = format!("local-{}", unix_now());
    let mut network = opts.network.clone();
    let mut owned_network: Option<String> = None;
    let mut ollama_container: Option<String> = None;
    let mut model_base_url: Option<String> = None;
    let mut model = opts.model.clone();

    if let Some(local_model) = &opts.local_model {
        // The sidecar is derived from the same --name, so a leftover
        // `<name>-ollama` is the same wedge one step over (#747). Preflight it
        // before creating anything, and let --replace cover it too.
        let ollama = format!("{}-ollama", opts.name);
        // No host port on the sidecar, so the remedy never offers --port.
        docker::ensure_container_name_free(
            &ollama,
            None,
            opts.replace,
            docker::ConflictContext::SkillUp,
        )
        .await?;
        let (net, owned) = match &opts.network {
            Some(net) => (net.clone(), false),
            None => (format!("{}-net", opts.name), true),
        };
        if owned {
            // Only claim ownership (and teardown responsibility) when this call
            // actually created the network; a pre-existing one is not ours to rm.
            let created = docker::create_network(&net).await?;
            if created {
                owned_network = Some(net.clone());
            }
        }
        if let Err(err) = docker::run_ollama(&ollama, &net, DEFAULT_OLLAMA_IMAGE).await {
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(docker::map_name_conflict(
                err.context("starting local model container"),
                &ollama,
                None,
                docker::ConflictContext::SkillUp,
            ));
        }
        if let Err(err) = docker::wait_ollama_ready(&ollama, Duration::from_secs(120)).await {
            let _ = docker::remove_container(&ollama).await;
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(err.context("waiting for local model container"));
        }
        if let Err(err) = docker::pull_model(&ollama, local_model).await {
            let _ = docker::remove_container(&ollama).await;
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(err.context("pulling local model"));
        }
        let url = format!("http://{ollama}:{OLLAMA_PORT}");
        network = Some(net);
        ollama_container = Some(ollama);
        model_base_url = Some(url);
        model = Some(local_model.clone());
    }

    // Forward exactly one model credential (or none under fake/local) -- never
    // the ambient SDK token alongside a chosen BYO credential. See
    // select_passthrough_env.
    // `--local-model` is a base-URL override, not a fake-model run: it keeps an
    // explicit BYO credential (the runner routes it at the local endpoint) and
    // drops only the ambient SDK fallback. Derive both states the way the
    // container actually gets them, so the seam cannot drift from the argv.
    let fake_model = opts.local_model.is_none() && opts.fake_model;
    let base_url_override = model_base_url.is_some();
    let mut docker_env = Vec::new();
    if !fake_model {
        docker_env.extend(load_model_credentials_from_secret_store()?);
        // #749/ADR-0070: an opt-in bundle `.env` is the lowest-priority source
        // -- appended after the vault, filling only names the shell env and the
        // vault did not (shell env > vault > file). `select_passthrough_env`
        // below stays the frozen authority on what is actually forwarded.
        let from_env_file =
            load_model_credentials_from_env_file(opts.env_file.as_deref(), &docker_env)?;
        docker_env.extend(from_env_file);
    }
    let byo_credential = std::env::var("CURIE_CREDENTIALS")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            stored_env_contains(&docker_env, "CURIE_CREDENTIALS").then_some("stored".to_string())
        });
    // Hydrate `--secret NAME` from Curie private storage when it is not
    // already present in the process env. The docker argv still forwards only
    // the NAME (`-e NAME`); the value is supplied only to the Docker CLI child
    // process so Docker can copy it into the runner container.
    for name in &opts.secret {
        if !env_credential_present(name) && !stored_env_contains(&docker_env, name) {
            match secret_store_env(name)? {
                Some(pair) => docker_env.push(pair),
                None => {
                    crate::ui::ui().note(&format!(
                        "--secret {name}: not set in the environment or Curie secret store; nothing will be forwarded for it"
                    ));
                }
            }
        }
    }
    // Scoped so the borrow of `docker_env` ends before it is moved into the spec.
    let passthrough_env = {
        let ambient_present = ambient_present_for(&docker_env);
        merge_secret_env(
            select_passthrough_env(
                fake_model,
                base_url_override,
                byo_credential.as_deref(),
                &ambient_present,
            ),
            &opts.secret,
        )
    };

    let spec = StartSpec {
        image: opts.image.clone(),
        container_name: opts.name.clone(),
        host_port: opts.port,
        plugin_dir: plugin_dir.clone(),
        session_id: session_id.clone(),
        sandbox_id: "local".into(),
        budget_json: opts.budget,
        fake_model,
        network,
        otel_endpoint: opts.otel_endpoint,
        model_base_url: model_base_url.clone(),
        model,
        passthrough_env,
        docker_env,
    };

    let ui = crate::ui::ui();
    ui.note(&format!(
        "starting runner container '{}' from '{}'",
        opts.name, opts.image
    ));
    let container_id = match docker::docker_with_env(&spec.run_args(), &spec.docker_env).await {
        Ok(id) => id,
        Err(err) => {
            if let Some(ollama) = &ollama_container {
                let _ = docker::remove_container(ollama).await;
            }
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            // The preflight above can lose the race to a container created
            // between the probe and here; map that onto the same actionable
            // error rather than docker's raw conflict (#747).
            return Err(docker::map_name_conflict(
                err.context("starting runner container"),
                &opts.name,
                Some(opts.port),
                docker::ConflictContext::SkillUp,
            ));
        }
    };

    let base_url = format!("http://localhost:{}", opts.port);
    let client = RunnerClient::new(&base_url)?;
    let cl = ui.checklist();
    let step = cl.step("waiting for runner");
    if let Err(err) = client.wait_healthy(Duration::from_secs(60)).await {
        step.fail("unhealthy");
        let logs = docker::container_logs(&opts.name, 40).await;
        ui.note(&logs);
        let _ = docker::remove_container(&opts.name).await;
        if let Some(ollama) = &ollama_container {
            let _ = docker::remove_container(ollama).await;
        }
        if let Some(net) = &owned_network {
            let _ = docker::remove_network(net).await;
        }
        ui.failure(&format!("runner failed to become healthy: {err}"));
        bail!("runner failed to become healthy: {err}");
    }
    step.done("healthy");

    // State lives with the bundle: init gitignores .curie/ there, and the
    // follow-up commands are documented to run from the bundle directory. If
    // the save fails (e.g. a read-only bundle), tear the container down again:
    // a live runner with no recorded state would be invisible to stop/status.
    if let Err(err) = state::save(
        &plugin_dir,
        &RunnerState {
            container_id,
            container_name: opts.name.clone(),
            image: opts.image,
            port: opts.port,
            base_url: base_url.clone(),
            session_id,
            plugin_dir: plugin_dir.display().to_string(),
            fake_model: opts.fake_model,
            ollama_container: ollama_container.clone(),
            network: owned_network.clone(),
            model_base_url: model_base_url.clone(),
        },
    ) {
        let _ = docker::remove_container(&opts.name).await;
        if let Some(ollama) = &ollama_container {
            let _ = docker::remove_container(ollama).await;
        }
        if let Some(net) = &owned_network {
            let _ = docker::remove_network(net).await;
        }
        return Err(err.context("recording runner state (container removed again)"));
    }

    let version = git_short_sha(&plugin_dir)
        .await
        .map(|sha| format!("dev @ {sha}"))
        .unwrap_or_else(|| format!("{plugin_name} @ {manifest_version}"));
    let rows = [
        ("Local bot", base_url),
        (
            "Skill message",
            "curie skill message \"<message>\"".to_string(),
        ),
        ("Skill eval", "curie skill eval".to_string()),
        ("Version", version),
    ];
    ui.payload_plain(&boxed_summary("curie dev environment", &rows));
    if let Some(local_model) = &opts.local_model {
        ui.note(&format!(
            "local model running in container '{}' from '{}' with model '{}'",
            ollama_container.as_deref().unwrap_or("unknown"),
            DEFAULT_OLLAMA_IMAGE,
            local_model
        ));
    }
    let cwd = Path::new(".").canonicalize()?;
    if cwd != plugin_dir {
        ui.note(&format!(
            "State recorded in {}/.curie/runner.json; run skill down from that directory. skill message, skill eval, and skill status also work there. skill message and skill eval also accept --url.",
            plugin_dir.display()
        ));
    }
    Ok(())
}

/// What `curie skill down` should tear down, resolved from the recorded state
/// and an explicit `--name` (#747).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownPlan {
    /// A runner is recorded and `container` IS it: remove it along with its
    /// ollama container and network, then clear the state file.
    Recorded { container: String },
    /// An explicit `--name` that is not the recorded runner, and that container
    /// is present: remove only it. The state file and the recorded runner's
    /// ollama container and network are left alone, since they describe a
    /// different, still-running runner (#747).
    Targeted { container: String },
    /// The same, except the named container is not there. A no-op teardown is
    /// not an error, but it must not claim a removal that never happened:
    /// `docker rm -f` exits 0 on a missing name, so absence is established by
    /// the probe rather than inferred from the removal (#747).
    TargetedAbsent { container: String },
    /// No state file, but a container of that name exists: remove it. Nothing to
    /// clear, so a stray runner is no longer un-stoppable from the CLI.
    Orphan { container: String },
    /// Nothing to remove; the message names what was looked for and the remedy.
    Nothing { message: String },
}

/// Resolve the teardown target. Pure so the no-state fallback (#747) is testable
/// without a Docker daemon or a bundle on disk. An explicit `--name` that
/// disagrees with the recorded runner is a TARGETED removal, never a reason to
/// clear state that describes a different container.
/// What the recorded teardown does once the container actually holding the
/// recorded NAME has been identified (#747).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordedTeardown {
    /// The container holding the name IS the recorded one: full teardown. The
    /// verified `id` is what gets removed, never the name: another bundle could
    /// take the name between this check and the removal, and the whole point of
    /// the check is that the removal hits the container it approved (#747).
    Remove { id: String },
    /// Nothing holds the name any more: clear the record without claiming a
    /// removal that did not happen.
    AlreadyGone,
    /// A DIFFERENT container now holds the recorded name. Removing it would
    /// destroy someone else's live runner, so it is left alone and the stale
    /// record is cleared instead.
    Hijacked { message: String },
}

/// Resolve the recorded teardown by container IDENTITY rather than by name.
///
/// The name alone is not identity: another bundle's `skill up --replace` can
/// create a new container under the same name, and a later plain `skill down`
/// here would then destroy that live runner (#747). Pure so both branches are
/// testable without a Docker daemon.
pub fn plan_recorded_teardown(
    recorded_id: &str,
    container: &str,
    live_id: Option<&str>,
) -> RecordedTeardown {
    let Some(live) = live_id else {
        return RecordedTeardown::AlreadyGone;
    };
    // `docker ps` reports a 12-char short id while `docker run` returns the full
    // 64-char one, so identity is a prefix match, not equality. A record with no
    // id at all cannot be compared, so it keeps the old name-based behavior
    // rather than refusing to tear down.
    if recorded_id.is_empty() || recorded_id.starts_with(live) || live.starts_with(recorded_id) {
        return RecordedTeardown::Remove {
            id: live.to_string(),
        };
    }
    RecordedTeardown::Hijacked {
        message: format!(
            "the runner recorded in .curie/runner.json is gone, and container '{container}' is now a different container ({live}); \
nothing was removed and the stale record has been cleared. \
To remove the container currently holding that name, run 'curie skill down --name {container}'"
        ),
    }
}

pub fn plan_skill_down(recorded: Option<&str>, requested: Option<&str>, exists: bool) -> DownPlan {
    match (recorded, requested) {
        (Some(recorded), Some(requested)) if requested != recorded => {
            let container = requested.to_string();
            if exists {
                DownPlan::Targeted { container }
            } else {
                DownPlan::TargetedAbsent { container }
            }
        }
        (Some(recorded), _) => DownPlan::Recorded {
            container: recorded.to_string(),
        },
        (None, requested) => {
            let container = requested.unwrap_or(docker::RUNNER_CONTAINER_LOCAL);
            if exists {
                DownPlan::Orphan {
                    container: container.to_string(),
                }
            } else {
                DownPlan::Nothing {
                    message: format!(
                        "no local runner recorded in .curie/runner.json and no container named '{container}' is running; \
run 'curie skill down' from the bundle directory, \
or name the container with 'curie skill down --name <container>'"
                    ),
                }
            }
        }
    }
}

/// Warn before removing a container by NAME that is not identifiably ours.
///
/// `skill down --name postgres` would otherwise destroy an unrelated container
/// with no signal at all. A warning, never a refusal: a runner left by a release
/// that predates the CLI label carries no label and must stay removable, which
/// is the whole point of #747. The label is read from the same probe that
/// established the container exists, so a Docker error has already aborted the
/// teardown before this is reached; here, absent means genuinely unlabeled.
fn warn_if_not_cli_managed(
    container: &str,
    facts: Option<&docker::ContainerFacts>,
    ui: &crate::ui::Ui,
) {
    if docker::is_cli_managed(facts) {
        return;
    }
    ui.warn(&format!(
        "container '{container}' does not carry the {} label, so it may not be a Curie runner; removing it anyway",
        docker::CLI_MANAGED_LABEL
    ));
}

/// Said on every `--name` teardown that deliberately leaves the recorded runner
/// running, so the two arms cannot drift apart.
const RECORDED_RUNNER_LEFT_ALONE: &str =
    "left the recorded runner in .curie/runner.json alone; run 'curie skill down' with no --name to stop it";

/// The note for a container that turned out to be gone before it was removed.
///
/// `state_cleared` is the recorded path, which ALSO clears
/// `.curie/runner.json`; that half of the sentence is the user's only signal
/// that it did, so it is stated here once rather than left to each caller to
/// remember (#747). Pure so the wording is testable.
fn absent_container_note(container: &str, state_cleared: bool) -> String {
    format!(
        "container '{container}' was already gone{}",
        if state_cleared {
            "; cleared stale state"
        } else {
            ""
        }
    )
}

/// Remove a container, reporting success, and treat "already gone" as success
/// too (the same tolerance the recorded-runner path has always had).
async fn remove_container_tolerating_absence(
    target: &str,
    display: &str,
    state_cleared: bool,
    ui: &crate::ui::Ui,
) -> Result<()> {
    match docker::remove_container(target).await {
        Ok(()) => ui.success(&format!("stopped and removed container '{display}'")),
        Err(err) if err.to_string().contains("No such container") => {
            ui.note(&absent_container_note(display, state_cleared));
        }
        Err(err) => return Err(err),
    }
    Ok(())
}

pub async fn stop(name: Option<String>) -> Result<()> {
    let dir = Path::new(".");
    let ui = crate::ui::ui();
    let saved = state::load(dir)?;
    let recorded = saved.as_ref().map(|s| s.container_name.clone());
    // Ask Docker what actually holds the target name. Every path needs it:
    // `docker rm -f` exits 0 on a missing name, so only the probe can tell a real
    // removal from a no-op, and the recorded path compares the live container's
    // ID against the recorded one before removing anything (#747). Exactly one
    // branch below runs, so one probe of one name is enough, and taking the id
    // and the managed-by label from that same probe leaves no window for the two
    // to disagree.
    let target = name
        .as_deref()
        .or(recorded.as_deref())
        .unwrap_or(docker::RUNNER_CONTAINER_LOCAL);
    // Propagated, never swallowed: an unreachable daemon reported as "no such
    // container" would hand the user a remedy that cannot work and hide the real
    // fault.
    let live = docker::container_facts(target)
        .await
        .with_context(|| format!("checking whether container '{target}' exists"))?;

    let plan = plan_skill_down(recorded.as_deref(), name.as_deref(), live.is_some());
    // `Recorded` is returned only when a runner is recorded, so pairing the plan
    // with the record here is what makes the teardown total.
    if let (DownPlan::Recorded { .. }, Some(saved)) = (&plan, saved) {
        return stop_recorded(dir, ui, saved, live.as_ref()).await;
    }
    match plan {
        DownPlan::Targeted { container } => {
            // A different container than the one on record: remove exactly it and
            // leave the recorded runner (and its state, ollama, network) intact.
            warn_if_not_cli_managed(&container, live.as_ref(), ui);
            remove_container_tolerating_absence(&container, &container, false, ui).await?;
            ui.note(RECORDED_RUNNER_LEFT_ALONE);
            Ok(())
        }
        DownPlan::TargetedAbsent { container } => {
            ui.note(&format!(
                "no container named '{container}' is present; nothing was removed"
            ));
            ui.note(RECORDED_RUNNER_LEFT_ALONE);
            Ok(())
        }
        DownPlan::Orphan { container } => {
            // No state file to clear, so the container IS the identity (#747).
            warn_if_not_cli_managed(&container, live.as_ref(), ui);
            remove_container_tolerating_absence(&container, &container, false, ui).await?;
            ui.note("no .curie/runner.json was present, so nothing to clear");
            Ok(())
        }
        DownPlan::Nothing { message } => bail!(message),
        // Unreachable: `plan_skill_down` returns `Recorded` only when a runner
        // is recorded, and the `if let` above takes that pairing.
        DownPlan::Recorded { container } => {
            bail!("internal: a recorded teardown of '{container}' reached the unrecorded path")
        }
    }
}

/// Tear down the runner recorded in `.curie/runner.json`, plus the ollama
/// sidecar, network and state file it owns.
async fn stop_recorded(
    dir: &Path,
    ui: &crate::ui::Ui,
    saved: RunnerState,
    live: Option<&docker::ContainerFacts>,
) -> Result<()> {
    match plan_recorded_teardown(
        &saved.container_id,
        &saved.container_name,
        live.map(|f| f.id.as_str()),
    ) {
        // By ID, not by name: the identity check above approved exactly this
        // container, and a name can change hands before the removal lands.
        RecordedTeardown::Remove { id } => {
            remove_container_tolerating_absence(&id, &saved.container_name, true, ui).await?
        }
        // Nothing holds the name, so there is no removal to claim; the stale
        // record still gets cleared below.
        RecordedTeardown::AlreadyGone => {
            ui.note(&absent_container_note(&saved.container_name, true))
        }
        // Another bundle's runner now holds this name. Removing it would destroy
        // a live container this bundle never booted.
        RecordedTeardown::Hijacked { message } => {
            ui.warn(&message);
            state::remove(dir)?;
            return Ok(());
        }
    }
    if let Some(ollama) = &saved.ollama_container {
        // A sidecar that will not die is a warning, not a failed teardown.
        if let Err(err) = remove_container_tolerating_absence(ollama, ollama, false, ui).await {
            ui.warn(&format!("could not remove container '{ollama}': {err}"));
        }
        // Keep the model-cache volume so the next `skill up` reuses the pulled
        // model instead of re-downloading it (mirrors compose `down` keeping
        // `ollama_data`). Removal is left to the user.
        let volume = docker::ollama_volume(ollama);
        ui.note(&format!(
            "kept model-cache volume '{volume}' for fast re-up; remove it with 'docker volume rm {volume}'"
        ));
    }
    if let Some(net) = &saved.network {
        match docker::remove_network(net).await {
            Ok(()) => ui.success(&format!("removed network '{net}'")),
            Err(err) if err.to_string().contains("No such network") => {
                ui.note(&format!("network '{net}' was already gone"));
            }
            Err(err) => ui.warn(&format!("could not remove network '{net}': {err}")),
        }
    }
    state::remove(dir)?;
    Ok(())
}

/// The `curie skill status --json` payload: the runner base URL plus the
/// serialized session status. Generic over the status shape so it serves both
/// the frozen `SessionStatus` (contract test) and the runner's raw `/status`
/// body (the live call site), which are both left unconstrained by
/// `cli/schema/status.schema.json`. Pure so it stays contract-testable.
pub fn status_json<T: serde::Serialize>(url: &str, status: &T) -> serde_json::Value {
    serde_json::json!({ "url": url, "session": status })
}

/// One eval case's result: `(id, outcome, seconds, output)`. `output` is the
/// graded answer text (the reply `turn_outcome`/`reply_passes` judged), carried
/// so a red case is diagnosable from `--json` without a manual re-run (#548).
/// Shared by the skill runner path and the local/cluster message path so both
/// report the same shape through `report_eval`/`eval_json`.
pub type EvalRow = (String, CaseOutcome, f64, String);

/// The three counts every eval surface reports. Split out so the `--json`
/// payload, the human roll-up, and the exit code all read the SAME tally rather
/// than each re-deriving it -- `failed` in particular must be counted, never
/// inferred as `total - passed`, which would book every non-graded plumbing row
/// as a failure (#606).
fn eval_counts(results: &[EvalRow]) -> (usize, usize, usize) {
    let count = |want: CaseOutcome| results.iter().filter(|(_, o, _, _)| *o == want).count();
    (
        count(CaseOutcome::Pass),
        count(CaseOutcome::Fail),
        count(CaseOutcome::PlumbingOk),
    )
}

/// The `curie skill eval --json` payload: the outcome roll-up plus one row per
/// case. Pure so it stays unit/contract-testable against
/// `cli/schema/eval.schema.json`.
pub fn eval_json(results: &[EvalRow]) -> serde_json::Value {
    // Derive every count from `results` in one pass so the rollup can never
    // disagree with the per-case rows (no caller-supplied passed/total to drift).
    let total = results.len();
    let (passed, failed, plumbing_ok) = eval_counts(results);
    let cases: Vec<serde_json::Value> = results
        .iter()
        .map(|(id, outcome, seconds, output)| {
            serde_json::json!({
                "id": id,
                "outcome": outcome,
                // Tri-state (ADR-0055): a non-graded row claims neither verdict.
                // `null` keeps a truthiness reader fail-safe (it under-reports,
                // never false-greens) without ever alleging a failure that did
                // not happen.
                "passed": outcome.passed(),
                "seconds": seconds,
                "output": output,
            })
        })
        .collect();
    serde_json::json!({
        "total": total,
        "passed": passed,
        "failed": failed,
        "plumbing_ok": plumbing_ok,
        "cases": cases,
    })
}

pub async fn status(url: Option<String>) -> Result<()> {
    let url = resolve_url(url)?;
    let client = RunnerClient::new(&url)?;
    let status = client.status().await?;
    crate::ui::ui().emit(&StatusOutput {
        url,
        status: serde_json::to_value(&status)?,
    });
    Ok(())
}

/// Output of `skill status` (#474). `to_json` delegates to the schema-gated
/// `status_json` builder (byte-identical, so `cli/schema/status.schema.json` and
/// `json_contract.rs` stay green); `render` reproduces the note + pretty payload.
struct StatusOutput {
    url: String,
    status: serde_json::Value,
}

impl crate::ui::CliOutput for StatusOutput {
    fn to_json(&self) -> serde_json::Value {
        status_json(&self.url, &self.status)
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.note(&format!("runner {}", self.url));
        ui.payload_plain(
            &serde_json::to_string_pretty(&self.status).unwrap_or_else(|_| self.status.to_string()),
        );
    }
}

pub async fn send(
    text: &str,
    user: &str,
    event_type: EventType,
    url: Option<String>,
) -> Result<()> {
    let url = resolve_url(url)?;
    let client = RunnerClient::new(&url)?;
    let ui = crate::ui::ui();
    let mut printer = TurnPrinter::default();

    // Under `--json`, answer tokens are suppressed on stdout (they route through
    // `ui.answer`), so a streamed turn would exit 0 with empty stdout (#485).
    // Accumulate the full reply and emit one JSON object at the end instead. The
    // human path is unchanged: it streams live and this buffer is never emitted.
    let json = ui.json();
    let mut reply = String::new();

    // A "thinking" spinner marks the wait for the first token; it is cleared the
    // instant streaming begins (committing no line) so the agent answer streams
    // clean. `streamed` tracks whether any answer token reached stdout;
    // `at_line_start` tracks whether stdout is at a fresh line (no un-terminated
    // streamed text) so a stderr diagnostic never glues onto a token line.
    let cl = ui.checklist();
    let mut step = Some(cl.step("thinking"));
    let mut streamed = false;
    let mut at_line_start = true;

    let events = client
        .send_event(event_type, text, user, |event| {
            let part = printer.part_for(event);
            // Clear the "thinking" spinner on the FIRST rendered event of any
            // kind (token, note, or failure). A Note/Fail is written to stderr
            // immediately, so if one arrives before the first token it would
            // garble the still-live spinner line unless we drop it first.
            if matches!(
                part,
                Some(TurnPart::Token(_) | TurnPart::Note(_) | TurnPart::Fail(_))
            ) {
                if let Some(step) = step.take() {
                    step.clear();
                }
            }
            match part {
                // Answer tokens are raw payload -> stdout, concatenated at network
                // pace with no per-delta newline. Track mid-line state so a later
                // note closes an un-terminated line first.
                Some(TurnPart::Token(token)) => {
                    if json {
                        reply.push_str(&token);
                    } else {
                        ui.answer(&token);
                    }
                    streamed = true;
                    at_line_start = token.ends_with('\n');
                }
                // Tool notes and errors are diagnostics -> stderr. If stdout is
                // mid-line, close that streamed line with a single newline first
                // so the note does not glue onto the token text. Under `-q` the
                // note itself is a no-op; the lone separating newline lands in
                // the middle of the streamed answer, which is just whitespace and
                // harmless to `| jq` (a median newline in the payload is fine).
                Some(TurnPart::Note(msg)) => {
                    if !at_line_start {
                        ui.print_tokens("\n");
                        at_line_start = true;
                    }
                    ui.note(&msg);
                }
                Some(TurnPart::Fail(msg)) => {
                    if !at_line_start {
                        ui.print_tokens("\n");
                        at_line_start = true;
                    }
                    ui.failure(&msg);
                }
                // The status trailer is emitted once at the end from events.last().
                Some(TurnPart::Status(_)) | None => {}
            }
        })
        .await?;

    // Nothing ever streamed (e.g. an empty final): drop the spinner silently.
    if let Some(step) = step.take() {
        step.clear();
    }

    if let Some(OutboundEvent::Final { status, .. }) = events.last() {
        // Under `--json`, emit the one buffered turn object (reply + final
        // status) rather than the streamed/human trailer (#485). Emit BEFORE any
        // exit so a classified failure still carries its data to the consumer.
        if json {
            ui.emit(&SkillMessageOutput {
                reply: std::mem::take(&mut reply),
                status: status_str(status).to_string(),
                finalized: true,
            });
            if *status == SessionStatus::ClassifiedFailure {
                std::process::exit(1);
            }
            return Ok(());
        }
        // Close the streamed answer on stdout only if the last thing written was
        // un-terminated token text; if a note already added its own newline (or
        // the last token ended in one) skip it to avoid a blank line. The status
        // trailer is a diagnostic -> stderr.
        if streamed && !at_line_start {
            ui.print_tokens("\n");
        }
        ui.note(&format!("-- final ({})", status_str(status)));
        if *status == SessionStatus::ClassifiedFailure {
            std::process::exit(1);
        }
    }
    Ok(())
}

/// Output of `skill message` under `--json`: the full buffered reply plus the
/// final session status. The human path streams tokens live and never builds
/// this; it exists so `--json` emits one object instead of empty stdout (#485).
#[derive(Debug)]
pub struct SkillMessageOutput {
    pub reply: String,
    pub status: String,
    pub finalized: bool,
}

impl crate::ui::CliOutput for SkillMessageOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "reply": self.reply,
            "status": self.status,
            "finalized": self.finalized,
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        // Only reached if a caller routes this through the human path; mirror the
        // streamed output as a single block for completeness.
        ui.answer(&self.reply);
        ui.note(&format!("-- final ({})", self.status));
    }
}

pub async fn eval(
    cases_path: Option<PathBuf>,
    url: Option<String>,
    models: Vec<String>,
    secrets: Vec<String>,
    image: String,
) -> Result<()> {
    let saved = state::load(Path::new("."))?;
    let state_plugin_dir = saved.as_ref().map(|s| PathBuf::from(s.plugin_dir.clone()));
    let cases_path = resolve_cases_path(cases_path, Path::new("."), state_plugin_dir.as_deref())?;
    let suite = load_suite(&cases_path)?;

    // Model selection (#526): with `--model`, boot a transient runner per model,
    // run the suite against each, and report pass-rate per model -- the one
    // command a "can we move to a cheaper model" decision needs, instead of a
    // manual `skill up --model X` + `skill eval` loop per model. Without it, the
    // default path drives the already-running runner (whatever model it booted).
    if !models.is_empty() {
        return eval_sweep(
            &suite,
            &models,
            &secrets,
            &image,
            state_plugin_dir.as_deref(),
        )
        .await;
    }

    let fake = drives_a_fake_runner(saved.as_ref(), url.as_deref());
    let url = resolve_url(url)?;
    let client = RunnerClient::new(&url)?;
    let ui = crate::ui::ui();
    let bar = ui.progress_bar(suite.cases.len() as u64, "running evals");
    // `run_suite_cases` also tallies completion for the `--model` sweep path;
    // the single-runner report doesn't need the count (it already reports the
    // per-case `Fail` either way and exits on any of them), so it is discarded.
    let (results, _completed) = run_suite_cases(&client, &suite, fake, |_| bar.inc(1)).await?;
    bar.finish();

    report_eval(&results)
}

/// Whether the runner `skill eval` is about to drive is the fake. Learned from
/// `.curie/runner.json` -- the CLI's own record of the runner IT booted, not a
/// guess at the shell env.
///
/// An explicit `--url` that is not the recorded runner points somewhere the
/// saved state says nothing about, so the recorded fake-ness does not transfer
/// and the run stays graded. `resolve_url`'s precedence is explicit-wins, so
/// this must mirror it: absent or matching URL only.
fn drives_a_fake_runner(saved: Option<&state::RunnerState>, explicit_url: Option<&str>) -> bool {
    match saved {
        Some(s) if s.fake_model => explicit_url.is_none_or(|u| u == s.base_url),
        _ => false,
    }
}

/// Run every case in `suite` against a runner, returning `(id, outcome, seconds,
/// output)` rows plus how many cases *completed* (reached a `final` matching
/// `expect_status`, independent of whether the grader then agreed). `fake` says
/// the runner is the fake model, in which case the cases are not graded at all.
/// `on_case` is called once per completed case (progress). Shared by the
/// single-runner path and the per-model sweep so both judge identically; the
/// completed count is what lets the sweep tell a real 0% apart from a model
/// that never produced one completed turn (#622, #526 AC4) -- `CaseOutcome`
/// alone collapses both into the same `Fail`.
async fn run_suite_cases(
    client: &RunnerClient,
    suite: &EvalSuite,
    fake: bool,
    mut on_case: impl FnMut(usize),
) -> Result<(Vec<EvalRow>, usize)> {
    let mut results = Vec::with_capacity(suite.cases.len());
    let mut completed = 0usize;
    for (i, case) in suite.cases.iter().enumerate() {
        // Fresh conversation by default (#550): reset the runner before a case so
        // it cannot answer from an earlier case's history instead of actually
        // invoking its tools. A shared_history case skips the reset and inherits
        // the prior case's conversation on purpose (a multi-turn scenario).
        if !case.shared_history {
            client.reset().await.with_context(|| {
                format!(
                    "resetting the runner conversation before case {:?}",
                    case.id
                )
            })?;
        }
        let started = Instant::now();
        let events = client
            .send_event(EventType::EvalCase, &case.input, "U-eval", |_| {})
            .await?;
        let elapsed = started.elapsed().as_secs_f64();
        if turn_completed(case, &events) {
            completed += 1;
        }
        // Capture the graded answer -- the exact text `turn_outcome` judged -- so
        // a red case can be diagnosed from `--json` without a manual re-run
        // (#548). A fake row carries its canned reply for the same reason.
        results.push((
            case.id.clone(),
            turn_outcome(case, &events, fake),
            elapsed,
            graded_answer(&events),
        ));
        on_case(i);
    }
    Ok((results, completed))
}

/// Boot a throwaway runner for one model on `port`, forwarding the model
/// credential and any `--secret` from the env or the host vault exactly like
/// `skill up` (never in argv). Returns its base URL; the caller removes the
/// container when done. Does NOT touch `.curie/runner.json`, so a sweep never
/// clobbers a persistent `skill up` runner's recorded state.
async fn boot_eval_runner(
    plugin_dir: &Path,
    image: &str,
    port: u16,
    name: &str,
    model: &str,
    secrets: &[String],
) -> Result<String> {
    // Same name-conflict preflight as `skill up` (#747), with the remedies the
    // sweep actually has: never --replace, since a concurrent sweep's container
    // must not be force-removed out from under it.
    docker::ensure_container_name_free(name, Some(port), false, docker::ConflictContext::EvalSweep)
        .await?;
    // Real-model run: forward the model credential (env or vault) and the
    // bundle's --secret connector secrets, mirroring `start`'s resolution.
    let mut docker_env = load_model_credentials_from_secret_store()?;
    let byo_credential = std::env::var("CURIE_CREDENTIALS").ok().or_else(|| {
        stored_env_contains(&docker_env, "CURIE_CREDENTIALS").then_some("stored".to_string())
    });
    for secret in secrets {
        if std::env::var_os(secret).is_none() && !stored_env_contains(&docker_env, secret) {
            if let Some(pair) = secret_store_env(secret)? {
                docker_env.push(pair);
            }
        }
    }
    // Scoped so the borrow of `docker_env` ends before it is moved into the spec.
    let passthrough_env = {
        let ambient_present = ambient_present_for(&docker_env);
        merge_secret_env(
            select_passthrough_env(false, false, byo_credential.as_deref(), &ambient_present),
            secrets,
        )
    };
    let spec = StartSpec {
        image: image.to_string(),
        container_name: name.to_string(),
        host_port: port,
        plugin_dir: plugin_dir.to_path_buf(),
        session_id: format!("eval-{}", unix_now()),
        sandbox_id: "local".into(),
        budget_json: DEFAULT_BUDGET.to_string(),
        fake_model: false,
        network: None,
        otel_endpoint: None,
        model_base_url: None,
        model: Some(model.to_string()),
        passthrough_env,
        docker_env,
    };
    docker::docker_with_env(&spec.run_args(), &spec.docker_env)
        .await
        .with_context(|| format!("booting eval runner for model {model}"))
        // A container created between the preflight and here still loses the
        // race; report the sweep's remedies, not docker's raw conflict (#747).
        .map_err(|err| {
            docker::map_name_conflict(err, name, Some(port), docker::ConflictContext::EvalSweep)
        })?;
    let url = format!("http://localhost:{port}");
    if let Err(err) = RunnerClient::new(&url)?
        .wait_healthy(Duration::from_secs(60))
        .await
    {
        let logs = docker::container_logs(name, 40).await;
        let _ = docker::remove_container(name).await;
        bail!("eval runner for model {model} failed to become healthy: {err}\n{logs}");
    }
    Ok(url)
}

/// One model's row in a `--model` sweep report: pass-rate, total, how many
/// completed (issue #622, #526 AC4), and how many of its rows were
/// plumbing-only (ran but never graded, ADR-0055, #612/#606). `completed` is
/// a subset of `total`: the graded rows whose turn actually reached a verdict
/// (`expect_status` matched, whatever the grader then said -- see
/// `evals::turn_completed`), as opposed to a graded fail that never completed
/// at all (a classified failure, the wrong terminal status, or a
/// transport/runner exception). `total > 0 && completed == 0` is a model that
/// never produced one completed turn across the whole suite -- distinct from
/// a real 0%, which the sweep reports and never gates on; `CaseOutcome` alone
/// cannot tell the two apart, since `turn_outcome` collapses both into `Fail`.
/// `plumbing` is always `0` on the in-CLI skill sweep (`eval_sweep` below,
/// which always boots a real, non-fake runner); it is populated from the
/// platform eval matrix's `EvalModelSummary.plumbing` on the `local`/`cluster`
/// `--model` sweep (`message.rs`'s `scoped_rows`), where a fake-model tier row
/// can legitimately have `total == 0` and `plumbing > 0` (#700). Shared by all
/// three tiers: the skill sweep boots throwaway runners and grades in-CLI,
/// local/cluster read the platform's `EvalModelSummary` -- `report_sweep` is
/// the single point that renders and gates a sweep however its rows were
/// produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepRow {
    pub model: String,
    pub passed: usize,
    pub completed: usize,
    pub total: usize,
    pub plumbing: usize,
}

impl SweepRow {
    /// A row with zero graded cases and at least one plumbing row is entirely
    /// a fixture: every case that ran for this model was plumbing, so `passed`
    /// and `total` carry no real signal at all (not "0% failing", but "never
    /// graded"). Distinguishing this from a genuine 0/0 (no cases assigned)
    /// keeps a plumbing-only model from reading as a failing real result.
    pub fn is_plumbing_only(&self) -> bool {
        self.total == 0 && self.plumbing > 0
    }

    /// A model that produced zero completed turns across the whole suite: the
    /// distinct "never answered" outcome, not a real (if unlucky) 0%. Guarded
    /// on `total > 0` so a row with no cases at all is never mistaken for
    /// this -- and, since a plumbing-only row also has `total == 0`, this and
    /// `is_plumbing_only` are mutually exclusive by construction.
    pub fn never_completed(&self) -> bool {
        self.total > 0 && self.completed == 0
    }

    fn pass_rate(&self) -> f64 {
        if self.total > 0 {
            self.passed as f64 / self.total as f64
        } else {
            0.0
        }
    }
}

/// Run the suite once per model in a fresh runner and report pass-rate per model.
async fn eval_sweep(
    suite: &EvalSuite,
    models: &[String],
    secrets: &[String],
    image: &str,
    state_plugin_dir: Option<&Path>,
) -> Result<()> {
    let ui = crate::ui::ui();
    // Mount the recorded runner's bundle dir if one is known, else the cwd.
    let plugin_dir = state_plugin_dir
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
        .canonicalize()
        .context("resolving the bundle directory for the model sweep")?;
    ui.note(&format!(
        "model sweep: {} model(s) x {} case(s)",
        models.len(),
        suite.cases.len()
    ));
    let cl = ui.checklist();
    let mut rows: Vec<SweepRow> = Vec::with_capacity(models.len());
    for (i, model) in models.iter().enumerate() {
        let name = format!("curie-eval-sweep-{i}");
        let port = DEFAULT_PORT + 100 + i as u16;
        let step = cl.step(&format!("model {model}"));
        let url = match boot_eval_runner(&plugin_dir, image, port, &name, model, secrets).await {
            Ok(url) => url,
            Err(err) => {
                step.fail("boot failed");
                return Err(err);
            }
        };
        let client = RunnerClient::new(&url)?;
        // `boot_eval_runner` pins `fake_model: false`, so every sweep runner is a
        // REAL model whatever the standing dev runner is -- the sweep grades,
        // so this in-CLI path never produces a plumbing-only row.
        let run = run_suite_cases(&client, suite, false, |_| {}).await;
        let _ = docker::remove_container(&name).await;
        let (results, completed) = run?;
        let passed = results
            .iter()
            .filter(|(_, o, _, _)| *o == CaseOutcome::Pass)
            .count();
        let total = suite.cases.len();
        // Immediate per-model feedback (#622): a model that never completed a
        // single case is a boot/resolution problem, not a graded loss, so the
        // checklist marks it failed rather than "done" with a misleading score.
        if completed == 0 {
            step.fail(&format!("0/{total} completed -- {model} never answered"));
        } else {
            step.done(&format!("{passed}/{total}"));
        }
        rows.push(SweepRow {
            model: model.clone(),
            passed,
            completed,
            total,
            plumbing: 0,
        });
    }
    report_sweep(&rows)
}

/// The `--json` sweep payload for one row: pure and independent of `Ui` so it
/// is unit-testable without a process-level stdout capture. Carries the raw
/// `plumbing` count (mirroring the API field) plus a `plumbing_only` boolean
/// derived from `SweepRow::is_plumbing_only` for a scripted consumer to filter
/// fixture rows out of a real model comparison (#700), and the raw
/// `completed` count plus a `never_completed` boolean (#622, #526 AC4) so a
/// model that never produced one completed turn is distinguishable from a
/// real 0%. `pass_rate` is withheld (null) on a never-completed row rather
/// than a fabricated 0.0, since there is no comparison to rate.
fn sweep_json_row(row: &SweepRow) -> serde_json::Value {
    serde_json::json!({
        "model": row.model,
        "passed": row.passed,
        "completed": row.completed,
        "total": row.total,
        "pass_rate": if row.never_completed() { None } else { Some(row.pass_rate()) },
        "plumbing": row.plumbing,
        "plumbing_only": row.is_plumbing_only(),
        "never_completed": row.never_completed(),
    })
}

/// The whole `--json` sweep payload: `{"sweep": [<row>, ...]}`. Pure and
/// independent of `Ui` so the schema contract test (#634) can validate it
/// against `cli/schema/sweep.schema.json` without a process-level stdout
/// capture. `report_sweep` emits exactly this via `Ui::emit_json`, so the two
/// never drift.
pub fn sweep_json(rows: &[SweepRow]) -> serde_json::Value {
    serde_json::json!({ "sweep": rows.iter().map(sweep_json_row).collect::<Vec<_>>() })
}

/// The human table row for one sweep row: `[model, "passed/total", pass rate,
/// plumbing count]`. A plumbing-only row (#700) is marked distinctly rather
/// than blended into the pass-rate list: the model name gets a `(plumbing)`
/// suffix and the rate column reads `n/a` instead of a misleading `0%`, since
/// every case for that model was a fixture, never graded, not a real failure.
/// A never-completed row (#622) takes priority over both: the rate column
/// reads `NEVER COMPLETED` rather than a percentage, since the model produced
/// zero completed turns across the whole suite -- not a real, if unlucky, 0%.
fn sweep_table_row(row: &SweepRow) -> Vec<String> {
    let model = if row.is_plumbing_only() {
        format!("{} (plumbing)", row.model)
    } else {
        row.model.clone()
    };
    let rate = if row.never_completed() {
        "NEVER COMPLETED".to_string()
    } else if row.is_plumbing_only() {
        "n/a".to_string()
    } else {
        format!("{:.0}%", row.pass_rate() * 100.0)
    };
    let plumbing = if row.plumbing > 0 {
        row.plumbing.to_string()
    } else {
        "-".to_string()
    };
    vec![
        model,
        format!("{}/{}", row.passed, row.total),
        rate,
        plumbing,
    ]
}

/// Render a model-sweep roll-up: pass-rate per model. Under `--json` the whole
/// comparison is one payload; otherwise a table. A sweep is a comparison, not a
/// gate, so it never exits non-zero on a model that scored below 100% -- a real
/// 0% still reports as `0/N (0%)` and exits `Ok`.
///
/// The one exception (#622, #526 AC4): a row whose model produced ZERO
/// completed turns across the whole suite is not a comparison result at all --
/// it means the model never answered (an unresolvable id, a missing credential,
/// a runner that never came up for it), and reporting it as `0%` is
/// indistinguishable from a real failing model. That row is rendered distinctly
/// (never as a percentage) and turns the whole sweep into an `Err` naming every
/// such model, so the caller's normal `?`-propagation exits non-zero at every
/// tier without skipping any guard the caller still holds (a kept-alive
/// port-forward at local/cluster) -- this function never calls
/// `std::process::exit` itself.
pub fn report_sweep(rows: &[SweepRow]) -> Result<()> {
    let ui = crate::ui::ui();
    if ui.json() {
        ui.emit_json(&sweep_json(rows));
    } else {
        let table: Vec<Vec<String>> = rows.iter().map(sweep_table_row).collect();
        ui.payload_plain(&crate::ui::table(
            &["model", "passed", "pass rate", "plumbing"],
            &table,
            &[1, 2, 3],
        ));
    }

    let never_completed: Vec<&SweepRow> = rows.iter().filter(|r| r.never_completed()).collect();
    if never_completed.is_empty() {
        return Ok(());
    }
    // Name the model AND the likely cause -- the whole point of #622 is that
    // this must not read like a graded 0%, and must not point at the eval
    // consumer the way the local/cluster sweep timeout used to (#526's AC4).
    let detail = never_completed
        .iter()
        .map(|r| format!("{} (0/{} completed)", r.model, r.total))
        .collect::<Vec<_>>()
        .join(", ");
    Err(anyhow::Error::from(
        crate::exit::CliError::failure(format!(
            "{detail}: produced zero completed turns across the suite. This is not a real 0% \
             score -- the model most likely never resolved (a typo'd or unregistered id, a \
             missing/invalid credential, or a runner that never came up for it), so the sweep is \
             failing loudly instead of reporting a comparison that never happened."
        ))
        .with_fix(
            "verify each named model's id and credential (or its BYO endpoint registration), \
             then re-run the sweep",
        ),
    ))
}

/// Render a finished eval run identically for every tier (`skill`, `local`,
/// `cluster`): under `--json` the whole roll-up is one machine payload on
/// stdout; otherwise the per-case table is payload -> stdout and the roll-up
/// verdict is a diagnostic -> stderr. Shared so `local eval`/`cluster eval`
/// print the same summary `skill eval` does (the per-tier parity gate), not a
/// hand-mirrored one.
///
/// Only a genuine `Fail` exits `Failure`. A run that graded nothing because it
/// ran on the fake tier is operationally successful without being a pass, so it
/// exits 0 and says "plumbing OK" in words -- the documented onboarding loop is
/// not red (#612), and it is not fake-green either (#606).
pub fn report_eval(results: &[EvalRow]) -> Result<()> {
    let (_passed, failed, _plumbing_ok) = eval_counts(results);
    // Emit through the one success point (#474), then apply the exit-code side
    // effect for BOTH paths -- the json path had it inline, the human path after.
    // Only a genuine `Fail` (failed > 0) exits non-zero: a plumbing-only run
    // graded nothing but is operationally successful, so it exits 0 (#606/#612).
    crate::ui::ui().emit(&EvalOutput { results });
    if failed > 0 {
        std::process::exit(crate::exit::ExitClass::Failure.code());
    }
    Ok(())
}

/// Output of `<tier> eval` (#474). `to_json` delegates to the schema-gated
/// `eval_json` builder (byte-identical, so `cli/schema/eval.schema.json` and
/// `json_contract.rs` stay green); `render` reproduces the per-case table and the
/// roll-up verdict + per-red-case reply notes.
struct EvalOutput<'a> {
    results: &'a [EvalRow],
}

impl crate::ui::CliOutput for EvalOutput<'_> {
    fn to_json(&self) -> serde_json::Value {
        eval_json(self.results)
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let results = self.results;
        let (passed, failed, plumbing_ok) = eval_counts(results);
        let rows: Vec<Vec<String>> = results
            .iter()
            .map(|(name, outcome, seconds, _)| {
                vec![
                    name.clone(),
                    outcome_label(*outcome),
                    format!("{seconds:.1}s"),
                ]
            })
            .collect();
        ui.payload_plain(&crate::ui::table(&["case", "result", "time"], &rows, &[2]));
        if failed == 0 {
            ui.success(&rollup_line(passed, failed, plumbing_ok));
            if plumbing_ok > 0 {
                ui.note(
                    "the fake model returns one canned reply whatever the input, so these cases \
                     were not graded -- they prove the turn completed, nothing more. Re-run with \
                     a real credential to grade them.",
                );
            }
        } else {
            // Surface WHAT each red case actually replied, so a human need not
            // re-run by hand to see why it failed (#548). Empty means the turn
            // never produced gradeable text (no `done`/reply) -- the diagnosis.
            for (name, _, _, output) in results
                .iter()
                .filter(|(_, o, _, _)| *o == CaseOutcome::Fail)
            {
                let shown = if output.is_empty() {
                    "<no reply text>".to_string()
                } else {
                    output.clone()
                };
                ui.note(&format!("{name} replied: {shown}"));
            }
            ui.warn(&format!(
                "{}; {failed} failed",
                rollup_line(passed, failed, plumbing_ok)
            ));
        }
    }
}

/// Where the eval cases live: an explicit `--cases` wins; otherwise
/// `evals/cases.json` in the current directory, falling back to the started
/// runner's recorded bundle directory (so `curie skill eval` works from
/// wherever `curie skill up` was run).
pub fn resolve_cases_path(
    explicit: Option<PathBuf>,
    cwd: &Path,
    state_plugin_dir: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let local = cwd.join("evals/cases.json");
    if local.is_file() {
        return Ok(local);
    }
    if let Some(plugin_dir) = state_plugin_dir {
        let in_bundle = plugin_dir.join("evals/cases.json");
        if in_bundle.is_file() {
            return Ok(in_bundle);
        }
    }
    Err(crate::exit::CliError::usage(format!(
        "no eval cases found: looked for {} and the running bundle's evals/cases.json; pass --cases",
        local.display()
    ))
    .with_fix("pass --cases")
    .into())
}

pub struct DeployOpts {
    pub plugin_dir: PathBuf,
    pub api_url: String,
    pub api_key: String,
    /// Explicit `--slack-channel`; None when the flag was omitted so a redeploy
    /// leaves an existing agent's channel untouched instead of masking intent
    /// with a default.
    pub slack_channel: Option<String>,
    pub env: DeployEnv,
    pub label: Option<String>,
    /// Per-agent connector secret NAMES to bind on deploy (ADR-0009, #429). Each
    /// value is resolved from the caller's env or the host secret vault and sent
    /// to the platform API, which stores it on the agent for the worker to
    /// forward into the sandbox. From `deploy --secret <NAME>`.
    pub secret: Vec<String>,
    /// Whether this tier offers `--secret` binding, gating the declared-secrets
    /// policy check (#464): true for tiers that can bind a declared secret
    /// (local), false where secret delivery is not yet wired (cluster, until
    /// #440 flips it). When false the gate is skipped -- otherwise every
    /// secrets-declaring bundle would hard-fail with a `--secret <NAME>`
    /// remediation that does not exist on that tier.
    pub secret_binding_supported: bool,
    /// Actionable remediation line printed when the platform API connection
    /// fails (e.g. the kubectl port-forward command for cluster, or
    /// `curie local up` for local). Naming the fix turns a raw
    /// "Connection refused" into something the operator can act on.
    pub connect_hint: String,
}

/// The declared connector-secret NAMES not present in the operator's bound
/// `--secret` set (#464). A non-empty result is a deploy-time gap: the bundle
/// expects a secret nothing will bind, which would surface at runtime as an
/// auth failure (#429).
///
/// Only WELL-FORMED, bindable names count as a gap: a declared name is diffed
/// only when it passes `crate::secrets::validate_name`, the same env-var-syntax
/// check (`^[A-Z_][A-Z0-9_]*$`) used for `curie secrets set`. Malformed or
/// reserved names are the plugin-format validator's responsibility (server-side
/// on upload); the gate excludes them so it never preempts that real validation
/// error with a misleading "bind `--secret <NAME>`" message. The reserved-name
/// list is deliberately NOT mirrored into Rust (drift risk) -- the regex filter
/// is the intended scope.
fn unbound_declared_secrets(declared: &[String], bound: &[String]) -> Vec<String> {
    declared
        .iter()
        .filter(|name| {
            crate::secrets::validate_name(name).is_ok() && !bound.iter().any(|b| b == *name)
        })
        .cloned()
        .collect()
}

/// The kubectl port-forward the auto `cluster deploy` path opens to the
/// release's api service (ADR-0057, superseding ADR-0024's deploy transport).
/// When no `--api-url` is given, deploy self-plumbs this loopback tunnel and
/// posts to `localhost:<local>`, so the discovered strong release key travels
/// only in the X-API-Key header over the tunnel, never over the cleartext UI
/// `/api` NodePort proxy. An explicit `--api-url` direct-dials the given URL, so
/// no tunnel is built (`None`).
pub fn deploy_port_forward(
    api_url: Option<&str>,
    namespace: &str,
    release: &str,
    local_port: u16,
    remote_port: u16,
) -> Option<crate::ops::OpsCommand> {
    match api_url {
        Some(_) => None,
        None => Some(crate::message::port_forward_command(
            namespace,
            release,
            "api",
            local_port,
            remote_port,
        )),
    }
}

/// True when `cluster deploy` must auto-discover the release Secret key: no
/// explicit `--api-key`/`CURIE_API_KEY` was given. An explicit key wins and
/// skips discovery (ADR-0057).
pub fn deploy_needs_key_discovery(explicit_api_key: Option<&str>) -> bool {
    explicit_api_key.is_none()
}

/// An empty `--api-key`/`CURIE_API_KEY=""` is absent, not a key: normalize
/// `Some("")` (after trim) to `None` so a blank value triggers discovery like
/// an omitted flag instead of posting an empty key (401). Same empty-credential
/// rule settled in `message::api_key_or_default` and
/// `ops::resolve_up_credentials`.
pub fn normalize_deploy_api_key(api_key: Option<String>) -> Option<String> {
    api_key.filter(|k| !k.trim().is_empty())
}

pub async fn deploy(opts: DeployOpts) -> Result<DeployOutput> {
    let plugin_dir = opts
        .plugin_dir
        .canonicalize()
        .with_context(|| format!("plugin dir not found: {}", opts.plugin_dir.display()))?;
    let (plugin_name, manifest_version) = read_manifest(&plugin_dir)?;
    let label = opts
        .label
        .unwrap_or_else(|| format!("{manifest_version}-{}", unix_now()));
    let created_by = std::env::var("USER").unwrap_or_else(|_| "curie-cli".to_string());

    // Deploy-time secrets-policy gate (#464 / ADR-0009): every NAME the bundle's
    // manifest `secrets` policy declares must be in the operator's bound
    // `--secret` set, else deploy FAILS naming the gap. Decision is fail-loud per
    // the ticket -- a missing binding otherwise surfaces later as a runtime auth
    // failure (#429). This runs in the shared deploy() path, pre-network, so it
    // covers BOTH `local deploy` and `cluster deploy`. It is gated on
    // `secret_binding_supported` (AC2): cluster deploy cannot bind a `--secret`
    // until #440 wires delivery, so enforcing there would hard-fail every
    // secrets-declaring bundle with a remediation the tier cannot satisfy. It
    // runs first, before the archive is even packed: the check is a pure
    // name-set diff on `opts.secret` (the bound NAME set) and needs no packed
    // bundle or resolved values, so a declared-but-unbound policy fails fast
    // without doing any of that work.
    if opts.secret_binding_supported {
        let declared = read_declared_secrets(&plugin_dir)?;
        let unbound = unbound_declared_secrets(&declared, &opts.secret);
        if !unbound.is_empty() {
            return Err(crate::exit::usage(format!(
                "{plugin_name} declares connector secret(s) that were not bound on deploy: {}. \
                 Bind each with `--secret <NAME>` (value read from the environment or from \
                 `curie secrets set <NAME>`).",
                unbound.join(", ")
            )));
        }
    }

    let ui = crate::ui::ui();
    if let Some(channel) = opts.slack_channel.as_deref() {
        validate_slack_channel(channel)?;
    }
    let archive = pack_tar_gz(&plugin_dir)?;
    let env = opts.env.as_str();
    ui.note(&format!(
        "deploying {plugin_name} ({} bytes) to {} [{env}]",
        archive.len(),
        opts.api_url,
    ));

    // Resolve each --secret NAME to a value (env wins, else the host vault) so
    // the connector secret is bound on the agent for the worker to forward into
    // the sandbox (ADR-0009, #429). The value never appears in argv.
    let mut secrets: std::collections::BTreeMap<String, String> = std::collections::BTreeMap::new();
    for name in &opts.secret {
        let value = std::env::var(name)
            .ok()
            .filter(|v| !v.is_empty())
            .or(crate::secrets::get_value(name)?);
        match value {
            Some(v) => {
                secrets.insert(name.clone(), v);
            }
            None => {
                return Err(crate::exit::usage(format!(
                    "--secret {name}: not set in the environment and not saved in Curie \
                     storage; export it or run `curie secrets set {name}` first"
                )));
            }
        }
    }
    if !secrets.is_empty() {
        ui.note(&format!(
            "binding {} connector secret(s): {}",
            secrets.len(),
            secrets.keys().cloned().collect::<Vec<_>>().join(", ")
        ));
    }

    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let cl = ui.checklist();
    let step = cl.step(&format!("deploying {plugin_name}"));
    let outcome = match client
        .deploy(
            &plugin_name,
            opts.slack_channel.as_deref(),
            &label,
            &created_by,
            env,
            archive,
            &secrets,
        )
        .await
    {
        Ok(outcome) => {
            step.done(env);
            outcome
        }
        Err(err) => {
            step.fail("failed");
            if crate::exit::is_transient_reqwest(&err) {
                return Err(err.context(opts.connect_hint));
            }
            return Err(err);
        }
    };

    let channel = match &outcome.channel {
        ChannelOutcome::Created(channel) => channel.clone(),
        ChannelOutcome::Updated { from, to } => format!("updated to {to} (was {from})"),
        ChannelOutcome::Unchanged { channel, passed } => {
            if *passed {
                format!("unchanged ({channel})")
            } else {
                format!("unchanged ({channel}); pass --slack-channel to move it")
            }
        }
    };
    Ok(DeployOutput {
        plugin_name,
        label,
        env: env.to_string(),
        agent_name: outcome.agent.name,
        agent_id: outcome.agent.id,
        version_label: outcome.version.version_label,
        version_id: outcome.version.id,
        channel,
        bundle_ref: outcome.bundle.bundle_ref,
        bundle_sha256: outcome.bundle.bundle_sha256,
        bundle_size_bytes: outcome.bundle.size_bytes,
        deployment_id: outcome.deployment.id,
        deployment_environment: outcome.deployment.environment,
        deployment_status: outcome.deployment.status,
    })
}

/// Output of `<tier> deploy`: the deployed agent/version/channel/bundle/deployment
/// summary. Owns its data so `to_json`/`render` outlive the `ApiClient`; the
/// json-vs-human choice is made once in `Ui::emit` (#456, #485). Without this the
/// real-path success emitted only `payload`/`kv`, which suppress under `--json`,
/// so `deploy --json` exited 0 with empty stdout.
#[derive(Debug)]
pub struct DeployOutput {
    pub plugin_name: String,
    pub label: String,
    pub env: String,
    pub agent_name: String,
    pub agent_id: String,
    pub version_label: String,
    pub version_id: String,
    pub channel: String,
    pub bundle_ref: String,
    pub bundle_sha256: String,
    pub bundle_size_bytes: u64,
    pub deployment_id: String,
    pub deployment_environment: String,
    pub deployment_status: String,
}

impl crate::ui::CliOutput for DeployOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "plugin": self.plugin_name,
            "label": self.label,
            "environment": self.env,
            "agent": {"name": self.agent_name, "id": self.agent_id},
            "version": {"label": self.version_label, "id": self.version_id},
            "channel": self.channel,
            "bundle": {
                "ref": self.bundle_ref,
                "sha256": self.bundle_sha256,
                "size_bytes": self.bundle_size_bytes,
            },
            "deployment": {
                "id": self.deployment_id,
                "environment": self.deployment_environment,
                "status": self.deployment_status,
            },
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.payload(&format!(
            "deployed {} {} -> {}",
            self.plugin_name, self.label, self.env
        ));
        ui.kv(
            "agent",
            &format!("{} ({})", self.agent_name, ui.url(&self.agent_id)),
        );
        ui.kv(
            "version",
            &format!("{} ({})", self.version_label, ui.url(&self.version_id)),
        );
        ui.kv("channel", &self.channel);
        ui.kv(
            "bundle",
            &format!(
                "{} sha256:{} {} bytes",
                self.bundle_ref, self.bundle_sha256, self.bundle_size_bytes
            ),
        );
        ui.kv(
            "deployment",
            &format!(
                "{} [{}] {}",
                self.deployment_id, self.deployment_environment, self.deployment_status
            ),
        );
    }
}

/// Shared flags for the agent-lifecycle verbs (`cluster kill|resume|budget|delete`).
/// Like `deploy`, these speak the committed platform-API contract through the
/// existing `ApiClient` (no second HTTP client).
pub struct AgentActionOpts {
    pub api_url: String,
    pub api_key: String,
    /// Agent name or id to act on. Resolved to the API's `{agent_id}` via the
    /// same name lookup `deploy` uses (`ApiClient::find_agent`).
    pub agent: String,
    pub dry_run: bool,
}

/// Output of `<tier> kill <agent>`: the dry-run plan, or the resulting kill
/// state. Owns its data (agent name) so `to_json` / `render` outlive the
/// `ApiClient`. The json-vs-human choice is made once, in `Ui::emit` (#456).
#[derive(Debug)]
pub enum KillOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { agent: String, killed: bool },
}

impl crate::ui::CliOutput for KillOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            KillOutput::DryRun(plan) => plan.to_json(),
            KillOutput::Done { agent, killed } => {
                serde_json::json!({"agent": agent, "killed": killed})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            KillOutput::DryRun(plan) => plan.render(ui),
            KillOutput::Done { agent, killed } => {
                ui.payload(&format!("agent {agent} killed (killed={killed})"));
                ui.note("Run `curie cluster resume <agent>` to bring it back.");
            }
        }
    }
}

/// `curie cluster kill <agent> --yes`: flip the agent kill switch on
/// (`POST /agents/{id}/kill`). Destructive (it stops the agent's runs), so it
/// refuses without `--yes`, mirroring `cluster down`. `--dry-run` returns the
/// plan and makes no request.
pub async fn kill(opts: AgentActionOpts, yes: bool) -> Result<KillOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(KillOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/agents/<id>/kill  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    if !yes {
        return Err(crate::exit::CliError::usage(format!(
            "`curie cluster kill {}` stops the agent's runs; re-run with --yes to confirm",
            opts.agent
        ))
        .with_fix("re-run with --yes")
        .into());
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    let step = cl.step(&format!("killing {}", agent.name));
    let state = match client.kill_agent(&agent.id).await {
        Ok(state) => {
            step.done("killed");
            state
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(KillOutput::Done {
        agent: agent.name,
        killed: state.killed,
    })
}

/// Output of `<tier> resume <agent>`: the dry-run plan, or the resulting kill
/// state. Owns its data so it outlives the `ApiClient`.
#[derive(Debug)]
pub enum ResumeOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { agent: String, killed: bool },
}

impl crate::ui::CliOutput for ResumeOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ResumeOutput::DryRun(plan) => plan.to_json(),
            ResumeOutput::Done { agent, killed } => {
                serde_json::json!({"agent": agent, "killed": killed})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ResumeOutput::DryRun(plan) => plan.render(ui),
            ResumeOutput::Done { agent, killed } => {
                ui.payload(&format!("agent {agent} resumed (killed={killed})"));
            }
        }
    }
}

/// `curie cluster resume <agent>`: flip the agent kill switch off
/// (`POST /agents/{id}/resume`). Non-destructive, so no `--yes` gate.
/// `--dry-run` returns the plan and makes no request.
pub async fn resume(opts: AgentActionOpts) -> Result<ResumeOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(ResumeOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/agents/<id>/resume  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    let step = cl.step(&format!("resuming {}", agent.name));
    let state = match client.resume_agent(&agent.id).await {
        Ok(state) => {
            step.done("resumed");
            state
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(ResumeOutput::Done {
        agent: agent.name,
        killed: state.killed,
    })
}

/// Output of `<tier> budget <agent>`: the dry-run plan, or the saved budget.
/// `max_usd_per_day` is `None` when the platform default applies. Owns its data
/// so it outlives the `ApiClient`.
#[derive(Debug)]
pub enum BudgetOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        max_usd_per_day: Option<f64>,
    },
}

impl crate::ui::CliOutput for BudgetOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            BudgetOutput::DryRun(plan) => plan.to_json(),
            BudgetOutput::Done {
                agent,
                max_usd_per_day,
            } => serde_json::json!({"agent": agent, "max_usd_per_day": max_usd_per_day}),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            BudgetOutput::DryRun(plan) => plan.render(ui),
            BudgetOutput::Done {
                agent,
                max_usd_per_day,
            } => {
                let usd = max_usd_per_day
                    .map(|v| format!("${v}/day"))
                    .unwrap_or_else(|| "platform default".to_string());
                ui.payload(&format!("budget for {agent} set: max $/day {usd}"));
            }
        }
    }
}

/// `curie cluster budget <agent> --limit <n>`: set the agent budget
/// (`PUT /agents/{id}/budget`). `--limit` sets the daily spend cap
/// (`max_usd_per_day`, the primary `BudgetConfig` field the console surfaces as
/// "Max $/day"); the per-run token cap is left at the platform default.
/// `--dry-run` returns the plan and makes no request.
pub async fn budget(opts: AgentActionOpts, limit: f64) -> Result<BudgetOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(BudgetOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "PUT {}/agents/<id>/budget  {{\"max_usd_per_day\":{limit}}}  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    if !limit.is_finite() || limit <= 0.0 {
        return Err(crate::exit::usage(format!(
            "--limit must be a finite value greater than 0 (got {limit})"
        )));
    }
    let cfg = BudgetConfig {
        max_output_tokens_per_run: None,
        max_usd_per_day: Some(limit),
    };
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    let step = cl.step(&format!("setting budget for {}", agent.name));
    let saved = match client.set_budget(&agent.id, &cfg).await {
        Ok(saved) => {
            step.done("updated");
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(BudgetOutput::Done {
        agent: agent.name,
        max_usd_per_day: saved.max_usd_per_day,
    })
}

/// Output of `<tier> reset-thread <agent> --thread-key <key>`: the dry-run
/// plan, or the resulting reset-request state. Owns its data so it outlives
/// the `ApiClient`.
#[derive(Debug)]
pub enum ResetThreadOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        thread_key: String,
        /// The reset was accepted and queued (always true on the `Done` path).
        requested: bool,
        /// The CLI polled `GET .../reset` and observed the worker actually
        /// release the sandbox within the wait window (#735). False means the
        /// release is still pending -- queued, but not yet confirmed drained.
        released: bool,
    },
}

impl crate::ui::CliOutput for ResetThreadOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ResetThreadOutput::DryRun(plan) => plan.to_json(),
            ResetThreadOutput::Done {
                agent,
                thread_key,
                requested,
                released,
            } => {
                serde_json::json!({"agent": agent, "thread_key": thread_key, "requested": requested, "released": released})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ResetThreadOutput::DryRun(plan) => plan.render(ui),
            ResetThreadOutput::Done {
                agent,
                thread_key,
                released,
                ..
            } => {
                if *released {
                    ui.payload(&format!(
                        "thread {thread_key} on agent {agent}: reset complete, sandbox released"
                    ));
                    ui.note("The next message on this thread cold-creates a fresh sandbox.");
                } else {
                    ui.payload(&format!(
                        "thread {thread_key} on agent {agent}: reset queued, release still pending"
                    ));
                    ui.note(
                        "The worker did not confirm the release within the wait window; its next maintenance tick will release the sandbox. Poll GET .../reset (or re-run) to confirm before the next message.",
                    );
                }
            }
        }
    }
}

/// How long `reset-thread` waits for the worker to actually release the sandbox
/// before it reports the release as still pending (#735). Comfortably covers the
/// worker's default 30s `reclaim_interval_s` drain tick.
const RESET_RELEASE_TIMEOUT: Duration = Duration::from_secs(45);
/// How often `reset-thread` re-polls `GET .../reset` while waiting (#735).
const RESET_RELEASE_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// `curie <tier> reset-thread <agent> --thread-key <key> --yes`: force the
/// thread's sandbox to be released (`POST
/// /agents/{id}/threads/{thread_key}/reset`, #737). Interrupts a live turn on
/// the thread first, so it refuses without `--yes`, mirroring `kill`.
/// `--dry-run` returns the plan and makes no request.
pub async fn reset_thread(
    opts: AgentActionOpts,
    thread_key: String,
    yes: bool,
) -> Result<ResetThreadOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(ResetThreadOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/agents/<id>/threads/{}/reset  (would resolve agent {:?} first)",
                opts.api_url, thread_key, opts.agent
            )],
        }));
    }
    if !yes {
        return Err(crate::exit::CliError::usage(format!(
            "`curie ... reset-thread {} --thread-key {}` interrupts any live turn on the thread; re-run with --yes to confirm",
            opts.agent, thread_key
        ))
        .with_fix("re-run with --yes")
        .into());
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    let step = cl.step(&format!("resetting thread {thread_key} on {}", agent.name));
    if let Err(err) = client.reset_thread(&agent.id, &thread_key).await {
        step.fail("failed");
        return Err(err);
    }
    step.done("reset requested");

    // The POST only *queues* the release; the worker drains it on its next
    // maintenance tick (up to `reclaim_interval_s`, 30s by default). Poll the
    // reset state to completion so this command -- and therefore the operator --
    // does not move on until the sandbox has actually been released, closing the
    // window where the next message adopts the pre-reset sandbox (#735). A poll
    // failure never fails an already-accepted reset: it degrades to "release
    // unconfirmed, still pending".
    let wait = cl.step("waiting for the sandbox to be released");
    let deadline = Instant::now() + RESET_RELEASE_TIMEOUT;
    let released = loop {
        match client.thread_reset_state(&agent.id, &thread_key).await {
            Ok(state) if !state.requested => break true,
            Ok(_) => {}
            Err(_) => break false,
        }
        if Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(RESET_RELEASE_POLL_INTERVAL).await;
    };
    if released {
        wait.done("released");
    } else {
        wait.done("still pending");
    }

    Ok(ResetThreadOutput::Done {
        agent: agent.name,
        thread_key,
        requested: true,
        released,
    })
}

/// Output of `<tier> delete <agent>`: the dry-run plan, or the deleted agent's
/// name. Owns its data so it outlives the `ApiClient`.
#[derive(Debug)]
pub enum DeleteOutput {
    DryRun(crate::ui::DryRunPlan),
    Done { agent: String },
}

impl crate::ui::CliOutput for DeleteOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            DeleteOutput::DryRun(plan) => plan.to_json(),
            DeleteOutput::Done { agent } => serde_json::json!({"agent": agent, "deleted": true}),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            DeleteOutput::DryRun(plan) => plan.render(ui),
            DeleteOutput::Done { agent } => ui.payload(&format!("agent {agent} deleted")),
        }
    }
}

/// `curie cluster delete <agent> --yes`: delete the agent
/// (`DELETE /agents/{id}`). Destructive and irreversible, so it refuses without
/// `--yes`, mirroring `cluster down`. `--dry-run` returns the plan and makes no
/// request.
pub async fn delete(opts: AgentActionOpts, yes: bool) -> Result<DeleteOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(DeleteOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "DELETE {}/agents/<id>  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    if !yes {
        return Err(crate::exit::CliError::usage(format!(
            "`curie cluster delete {}` permanently deletes the agent; re-run with --yes to confirm",
            opts.agent
        ))
        .with_fix("re-run with --yes")
        .into());
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    let step = cl.step(&format!("deleting {}", agent.name));
    match client.delete_agent(&agent.id).await {
        Ok(()) => step.done("deleted"),
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    }
    Ok(DeleteOutput::Done { agent: agent.name })
}

/// Output of `<tier> versions <agent>`: the dry-run plan, the empty case, or the
/// version list. Owns its data (agent name + cloned versions) so `to_json` /
/// `render` outlive the `ApiClient`. The json-vs-human choice is made once, in
/// `Ui::emit` (issue #456).
pub enum VersionsOutput {
    DryRun(crate::ui::DryRunPlan),
    Empty {
        agent: String,
    },
    /// `versions` is held **newest-first**, normalized once by the `versions`
    /// handler (the API returns them oldest-first). Both `to_json` and `render`
    /// iterate it plainly, so any future constructor must preserve that order or
    /// the two paths silently diverge.
    List {
        agent: String,
        versions: Vec<crate::api::Version>,
    },
}

impl crate::ui::CliOutput for VersionsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            VersionsOutput::DryRun(plan) => plan.to_json(),
            VersionsOutput::Empty { agent } => {
                serde_json::json!({"agent": agent, "versions": []})
            }
            VersionsOutput::List { agent, versions } => {
                let versions: Vec<serde_json::Value> = versions
                    .iter()
                    .map(|v| {
                        serde_json::json!({
                            "id": v.id,
                            "version_label": v.version_label,
                            "commit_sha": v.commit_sha,
                            "bundle_sha256": v.bundle_sha256,
                            "created_by": v.created_by,
                            "created_at": v.created_at,
                            "bundle_ref": v.bundle_ref,
                        })
                    })
                    .collect();
                serde_json::json!({"agent": agent, "versions": versions})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            VersionsOutput::DryRun(plan) => plan.render(ui),
            VersionsOutput::Empty { agent } => {
                ui.payload(&format!("{agent} has no versions yet (deploy it first)"));
            }
            VersionsOutput::List { agent, versions } => {
                ui.payload(&format!(
                    "{agent} — {} version(s), newest first:",
                    versions.len()
                ));
                for v in versions.iter() {
                    let commit = v.commit_sha.as_deref().unwrap_or("-");
                    let by = v.created_by.as_deref().unwrap_or("-");
                    let at = v.created_at.as_deref().unwrap_or("-");
                    // Show the bundle hash consistently across tiers (#548): it is
                    // the parity evidence, so a human-readable listing must carry it
                    // too, not just `cluster deploy`'s printout.
                    let sha = v.bundle_sha256.as_deref().unwrap_or("-");
                    ui.kv(
                        &v.version_label,
                        &format!("sha256 {sha}  commit {commit}  by {by}  at {at}"),
                    );
                }
            }
        }
    }
}

/// `<tier> versions <agent>`: list the agent's immutable versions (newest first).
pub async fn versions(opts: AgentActionOpts) -> Result<VersionsOutput> {
    if opts.dry_run {
        return Ok(VersionsOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "GET {}/agents/<id>/versions  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let versions = client.list_versions(&agent.id).await?;
    if versions.is_empty() {
        return Ok(VersionsOutput::Empty { agent: agent.name });
    }
    // The API returns versions oldest-first; normalize to the documented
    // newest-first order HERE, once, so the json and human paths cannot diverge.
    Ok(VersionsOutput::List {
        agent: agent.name,
        versions: versions.into_iter().rev().collect(),
    })
}

/// Output of `<tier> memory <agent>`: the dry-run plan, the empty case, or the
/// learned-memory list. Owns its data so it outlives the `ApiClient`.
pub enum MemoryOutput {
    DryRun(crate::ui::DryRunPlan),
    Empty {
        agent: String,
    },
    List {
        agent: String,
        entries: Vec<crate::api::MemoryEntry>,
    },
}

impl crate::ui::CliOutput for MemoryOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            MemoryOutput::DryRun(plan) => plan.to_json(),
            MemoryOutput::Empty { agent } => {
                serde_json::json!({"agent": agent, "entries": []})
            }
            MemoryOutput::List { agent, entries } => {
                let entries: Vec<serde_json::Value> = entries
                    .iter()
                    .map(|e| serde_json::json!({"index": e.index, "content": e.content}))
                    .collect();
                serde_json::json!({"agent": agent, "entries": entries})
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            MemoryOutput::DryRun(plan) => plan.render(ui),
            MemoryOutput::Empty { agent } => {
                ui.payload(&format!("{agent} has no learned memory yet"));
            }
            MemoryOutput::List { agent, entries } => {
                ui.payload(&format!("{agent} — {} memory entr(ies):", entries.len()));
                for e in entries {
                    ui.kv(&format!("#{}", e.index), &e.content);
                }
            }
        }
    }
}

/// `<tier> memory <agent>`: show what the agent has learned (its memory log).
pub async fn memory(opts: AgentActionOpts) -> Result<MemoryOutput> {
    if opts.dry_run {
        return Ok(MemoryOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "GET {}/agents/<id>/memory  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let entries = client.list_memory(&agent.id).await?;
    if entries.is_empty() {
        return Ok(MemoryOutput::Empty { agent: agent.name });
    }
    Ok(MemoryOutput::List {
        agent: agent.name,
        entries,
    })
}

/// The pending-list / resolve flags for `local approvals` (#506). Defaulted so
/// the skill/cluster tiers, which keep only the gate view/set surface, pass an
/// empty value.
#[derive(Default)]
pub struct ApprovalCmd {
    pub list: bool,
    pub resolve: Option<String>,
    pub as_actor: Option<String>,
    pub reject: bool,
    pub note: Option<String>,
    pub actor_channel: Option<String>,
}

/// Output of `<tier> approvals <agent>`: the dry-run plan, the gate list (empty
/// vec == "no tools gated"), the pending records, or a resolved record (#506).
/// Owns its data so it outlives the `ApiClient`.
///
/// `manifest_unreadable` carries the third gate-list state (#607): `gated_tools`
/// alone cannot distinguish "the deployed bundle manifest declares no gates" from
/// "the manifest could not be read at all" -- both used to arrive as an empty vec
/// and render as the affirmative "calls run without approval". `Some(reason)`
/// means the manifest lookup failed, so the list is what we could see rather than
/// what is armed. Always `None` on the set path (`--gate`/`--clear`).
pub enum ApprovalsOutput {
    DryRun(crate::ui::DryRunPlan),
    Gates {
        agent: String,
        gated_tools: Vec<String>,
        manifest_unreadable: Option<String>,
    },
    Pending {
        agent: String,
        records: Vec<crate::api::ApprovalRecord>,
        /// `true` when `records.len()` hit the server's page-size cap
        /// (`ApiClient::APPROVALS_LIST_LIMIT`), meaning more pending approvals
        /// may exist beyond what was fetched (#670). Always present (never
        /// conditionally omitted), per the repo's superset-JSON convention.
        truncated: bool,
    },
    Resolved {
        record: crate::api::ApprovalRecord,
    },
}

fn approval_record_json(r: &crate::api::ApprovalRecord) -> serde_json::Value {
    serde_json::json!({
        "id": r.id,
        "author": r.author,
        "route": r.route,
        "gate_kind": r.gate_kind,
        "granted_tool": r.granted_tool,
        "status": r.status,
        "conversation_id": r.conversation_id,
        "summary": r.summary,
        "expires_at": r.expires_at,
        "resolved_by": r.resolved_by,
    })
}

impl crate::ui::CliOutput for ApprovalsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ApprovalsOutput::DryRun(plan) => plan.to_json(),
            ApprovalsOutput::Gates {
                agent,
                gated_tools,
                manifest_unreadable,
            } => serde_json::json!({
                "agent": agent,
                "gated_tools": gated_tools,
                "manifest_unreadable": manifest_unreadable,
            }),
            ApprovalsOutput::Pending {
                agent,
                records,
                truncated,
            } => serde_json::json!({
                "agent": agent,
                "pending": records.iter().map(approval_record_json).collect::<Vec<_>>(),
                "count": records.len(),
                "truncated": truncated,
            }),
            ApprovalsOutput::Resolved { record } => serde_json::json!({
                "resolved": approval_record_json(record),
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ApprovalsOutput::DryRun(plan) => plan.render(ui),
            ApprovalsOutput::Gates {
                agent,
                gated_tools,
                manifest_unreadable,
            } => {
                ui.payload(&approvals_summary_line(
                    agent,
                    gated_tools,
                    manifest_unreadable.as_deref(),
                ));
                for tool in gated_tools {
                    ui.kv("gated", tool);
                }
            }
            ApprovalsOutput::Pending {
                agent,
                records,
                truncated,
            } => {
                if records.is_empty() {
                    ui.payload(&format!("{agent}: no pending approvals"));
                } else {
                    ui.payload(&format!("{agent} — {} pending approval(s):", records.len()));
                    for r in records {
                        let tool = r.granted_tool.as_deref().unwrap_or("-");
                        let route = r.route.as_deref().unwrap_or("(requesting channel)");
                        ui.kv(
                            &r.id,
                            &format!(
                                "{} — {} [tool: {tool}, route: {route}, by: {}]",
                                r.summary, r.conversation_id, r.author
                            ),
                        );
                    }
                }
                if *truncated {
                    ui.payload(&format!(
                        "this list is capped at {} (the server max) and more pending approvals \
                         may exist; resolve some and re-run to see the rest",
                        crate::api::ApiClient::APPROVALS_LIST_LIMIT
                    ));
                }
            }
            ApprovalsOutput::Resolved { record } => {
                ui.payload(&format!(
                    "approval {} -> {} (by {})",
                    record.id,
                    record.status,
                    record.resolved_by.as_deref().unwrap_or("?")
                ));
            }
        }
    }
}

/// The human summary line for `<tier> approvals`' gate view.
///
/// Four lines for three states, because whether gates were found is orthogonal to
/// whether we could read the manifest that declares them. The `unreadable` arm is
/// the one that matters: an unanswered lookup must not borrow the vocabulary of an
/// answered one. Reporting "no tools are gated (calls run without approval)"
/// because the deployment list request errored tells the reader the runner will
/// not pause, which is a claim this command never checked -- and the reader acts on
/// it. Same reasoning as the skill tier's `gates_summary_line`, where the unseen
/// source is the boot-time env override rather than the deployed manifest.
///
/// The unreadable-with-gates arm is not redundant: gates from the platform's
/// `approval_required_tools` field are real, but presenting them without the
/// caveat implies the list is the whole set.
fn approvals_summary_line(agent: &str, gated_tools: &[String], unreadable: Option<&str>) -> String {
    match (gated_tools.is_empty(), unreadable) {
        (true, None) => format!("{agent}: no tools are gated (calls run without approval)"),
        (false, None) => format!("{agent} — {} gated tool(s):", gated_tools.len()),
        (true, Some(reason)) => format!(
            "{agent}: the deployed bundle manifest could not be read ({reason}), so whether it \
             gates any tool is unknown. The platform's approval_required_tools field lists none, \
             which is not the same as nothing being gated"
        ),
        (false, Some(reason)) => format!(
            "{agent} — {} gated tool(s), and this list may be incomplete: the deployed bundle \
             manifest could not be read ({reason}), so any gate it declares is not shown:",
            gated_tools.len()
        ),
    }
}

/// `<tier> approvals <agent> [--gate TOOL]... [--clear]`: view or set the tool
/// names whose calls pause for human approval. No flags => show current gates.
pub async fn approvals(
    opts: AgentActionOpts,
    gate: Vec<String>,
    clear: bool,
    cmd: ApprovalCmd,
) -> Result<ApprovalsOutput> {
    let gate_mode = clear || !gate.is_empty();

    // --resolve <id> --as <user>: resolve one live approval record (#506). It is
    // id-scoped, not gate config, so it is mutually exclusive with --gate/--clear/
    // --list. Approve by default; --reject rejects.
    if let Some(approval_id) = cmd.resolve {
        if gate_mode || cmd.list {
            return Err(crate::exit::usage(
                "--resolve cannot be combined with --gate/--clear/--list",
            ));
        }
        let actor = cmd.as_actor.ok_or_else(|| {
            crate::exit::usage("--resolve requires --as <user> (the actor resolving it)")
        })?;
        let decision = if cmd.reject { "rejected" } else { "approved" };
        if opts.dry_run {
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![format!(
                    "POST {}/approvals/{approval_id}/resolve decision={decision} resolved_by={actor:?}",
                    opts.api_url
                )],
            }));
        }
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        let record = client
            .resolve_approval(
                &approval_id,
                decision,
                &actor,
                cmd.note.as_deref(),
                cmd.actor_channel.as_deref(),
            )
            .await?;
        return Ok(ApprovalsOutput::Resolved { record });
    }

    // --list: the agent's pending approval records (#506).
    if cmd.list {
        if gate_mode {
            return Err(crate::exit::usage(
                "--list cannot be combined with --gate/--clear",
            ));
        }
        if opts.dry_run {
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![format!(
                    "GET {}/approvals?status_filter=pending&agent_id=<id>&limit={}  (would resolve agent {:?} first)",
                    opts.api_url,
                    crate::api::ApiClient::APPROVALS_LIST_LIMIT,
                    opts.agent
                )],
            }));
        }
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        let agent = client.find_agent(&opts.agent).await?;
        let records = client.list_pending_approvals(&agent.id).await?;
        let truncated = records.len() >= crate::api::ApiClient::APPROVALS_LIST_LIMIT;
        return Ok(ApprovalsOutput::Pending {
            agent: agent.name,
            records,
            truncated,
        });
    }

    if clear && !gate.is_empty() {
        return Err(crate::exit::usage(
            "--clear cannot be combined with --gate (clear removes all gates)",
        ));
    }
    let setting = clear || !gate.is_empty();
    if opts.dry_run {
        let action = if setting {
            format!(
                "PATCH {}/agents/<id> approval_required_tools={:?}",
                opts.api_url, gate
            )
        } else {
            format!("GET {}/agents/<id> (show current gates)", opts.api_url)
        };
        return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "{action}  (would resolve agent {:?} first)",
                opts.agent
            )],
        }));
    }
    let ui = crate::ui::ui();
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let (gates, unreadable) = if setting {
        let cl = ui.checklist();
        let step = cl.step(&format!("updating approval gates for {}", agent.name));
        match client.set_approval_tools(&agent.id, &gate).await {
            Ok(updated) => {
                step.done("updated");
                (updated.approval_required_tools.unwrap_or_default(), None)
            }
            Err(err) => {
                step.fail("failed");
                return Err(err);
            }
        }
    } else {
        // Report what the runner actually arms (#546): the UNION of the platform's
        // mutable `approval_required_tools` field (delivered as
        // CURIE_APPROVAL_REQUIRED_TOOLS) AND the in-force deployed bundle
        // manifest's `approvalPolicy` gates. Reading only the API field reported an
        // empty set while a manifest gate was armed and blocking.
        //
        // When that manifest half cannot be read, the union is only the field half
        // and the report says so (#607) rather than passing a partial answer off as
        // the effective set.
        let mut gated = agent.approval_required_tools.clone().unwrap_or_default();
        match deployed_manifest_gate_names(&client, &agent.id).await? {
            ManifestGates::Readable(names) => {
                for name in names {
                    if !gated.contains(&name) {
                        gated.push(name);
                    }
                }
                (gated, None)
            }
            ManifestGates::Unreadable(reason) => (gated, Some(reason)),
        }
    };
    Ok(ApprovalsOutput::Gates {
        agent: agent.name,
        gated_tools: gates,
        manifest_unreadable: unreadable,
    })
}

/// `local observability`: print the local platform's observability surfaces --
/// the Curie Console, the Langfuse UI, and the API base -- resolved through the
/// shared tier-aware endpoint seam (`crate::observability`).
///
/// `ObservabilityOutput` moved to `crate::observability` (#460) so both tiers
/// return one type; the hardcoded URL array that used to live here is replaced
/// by `observability::local_endpoints()`, whose consts `local.rs::ENDPOINTS`
/// also references (one source of truth for the port literals).
///
/// Agent-first: a browser is opened only when the human passes `--open`, and
/// never under `--json` (gated by `observability::should_open`). A missing
/// opener (headless/CI) is not an error -- the URLs are printed either way.
pub async fn observability(open: bool) -> Result<crate::observability::ObservabilityOutput> {
    let ui = crate::ui::ui();
    let surfaces = crate::observability::local_endpoints();
    crate::observability::open_endpoints(&surfaces, open, ui.json()).await;
    // A hint, not payload: `observability` never checks whether the stack is
    // up, so this is stderr guidance rather than a claim about what happened.
    ui.note("start these surfaces with `curie local up` if they are unreachable");
    Ok(crate::observability::ObservabilityOutput::Surfaces(
        surfaces,
    ))
}

/// Reject a Slack channel value that is a `#name` rather than a channel ID.
///
/// Real Slack events carry the channel **ID** (e.g. `C0123ABCD`), and the
/// worker's binding resolver matches on that ID, so a `#name` value is stored
/// verbatim and never routes -- a silently dead binding. Fail the deploy up
/// front instead.
fn validate_slack_channel(channel: &str) -> Result<()> {
    if channel.trim_start().starts_with('#') {
        return Err(crate::exit::usage(format!(
            "slack channel {channel:?} is a name, not an ID: real Slack events carry the \
             channel ID (e.g. C0123ABCD) and the worker routes on it, so a #name binding \
             never receives messages. Pass the channel ID instead -- find it in the \
             channel's About tab, or the channel URL (.../archives/C0123ABCD)."
        )));
    }
    Ok(())
}

fn resolve_url(explicit: Option<String>) -> Result<String> {
    if let Some(url) = explicit {
        return Ok(url);
    }
    if let Some(saved) = state::load(Path::new("."))? {
        return Ok(saved.base_url);
    }
    Ok(format!("http://localhost:{DEFAULT_PORT}"))
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the epoch")
        .as_secs()
}

/// Short git SHA of the plugin dir's checkout, for the version line.
async fn git_short_sha(dir: &Path) -> Option<String> {
    let output = tokio::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .current_dir(dir)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!sha.is_empty()).then_some(sha)
}

// --- skill tier parity (issue #459) -----------------------------------------

/// The env var the runner reads for the operator's approval-gate override.
const APPROVAL_TOOLS_ENV: &str = "CURIE_APPROVAL_REQUIRED_TOOLS";

/// The manifest locations the runner's `load_approval_policy` probes, in order.
const MANIFEST_LOCATIONS: [&str; 2] = [".claude-plugin/plugin.json", "plugin.json"];

/// One `approvalPolicy.gates[]` entry: the tool the runner intercepts plus the
/// approval route the platform binds per deployment. A local mirror of
/// `plugin_format.models.ApprovalGate`, kept private here so `scaffold`'s
/// `read_manifest` (used by `skill up`/`skill check`) keeps its narrow shape.
///
/// Both fields are `Option` so a MISSING key stays distinguishable from one
/// present but empty. Since #520 both refuse the manifest (the runner arms a
/// declared policy exactly or not at all), so the distinction no longer changes
/// the verdict -- but it still names the actual defect in the error, which is
/// the difference between a fixable message and a puzzle. `#[serde(default)]`
/// on a `String` would collapse them and report "empty" for a key that is
/// simply absent.
#[derive(Deserialize)]
struct ApprovalGateDecl {
    gate: Option<String>,
    route: Option<String>,
    /// Operator opt-in (#558). Unlike `gate`/`route`, collapsing absent/false is
    /// intentional here: a bool with a safe default carries no absent-vs-empty
    /// distinction worth preserving. Unread -- this struct only mirrors the
    /// manifest shape for round-trip parsing on the display/parse path.
    #[allow(dead_code)]
    #[serde(default, rename = "grantableViaPolicy")]
    grantable_via_policy: bool,
}

/// The manifest `approvalPolicy` object; mirrors `plugin_format.models.ApprovalPolicy`.
#[derive(Deserialize, Default)]
struct ApprovalPolicyDecl {
    #[serde(default)]
    gates: Vec<ApprovalGateDecl>,
}

/// Just the slice of the plugin manifest this verb reads.
///
/// `name` is carried only to mirror `plugin_format.models.PluginManifest`, whose
/// sole required field it is: without it `model_validate` raises and the runner
/// arms zero gates, so a narrower struct that parsed happily would report gates
/// the runner never arms.
///
/// FORMERLY A KNOWN LIMITATION (ADR-0041), closed by #701: this struct still
/// validates only the approval-relevant subset of the manifest -- `name` +
/// `approvalPolicy` -- so on its own a manifest invalid in some OTHER modeled
/// field (say `commands: 123`) would parse into it happily and report gates as
/// armed for a manifest the runner's `PluginManifest.model_validate` rejects
/// outright. `parse_manifest_gates` closes that gap by additionally validating
/// the RAW manifest against the frozen `packages/plugin-format` JSON Schema
/// (`validate_against_plugin_format_schema`) whenever `approvalPolicy` is
/// declared -- the same condition under which the runner's
/// `resolve_approval_policy` promotes to full-manifest validation (ADR-0041
/// decision 1). That is schema-driven, not a hand-mirror of every
/// `PluginManifest` field, so it tracks the frozen contract with no manual
/// upkeep here. `cli/plugin-format-mirrors.json` + `curie dev field-parity`
/// (which now also runs `cli/tests/plugin_format_field_parity.rs`) separately
/// gate that THIS struct's own fields (and its sibling mirrors in
/// `cli/src/spec.rs`) stay honest about which `plugin_format` fields they
/// cover.
#[derive(Deserialize)]
struct ManifestApprovals {
    name: Option<String>,
    #[serde(rename = "approvalPolicy")]
    approval_policy: Option<ApprovalPolicyDecl>,
}

/// The frozen `packages/plugin-format` JSON Schema (issue #701), embedded at
/// compile time. Committed and drift-checked by `plugin-format`'s own
/// `test_schema_compat.py` (the export is regenerated and diffed against this
/// exact file at CI), so this constant tracks the frozen contract with zero
/// manual upkeep on the Rust side: a schema change picks up automatically the
/// next time the CLI is built against this checkout.
const PLUGIN_FORMAT_SCHEMA: &str =
    include_str!("../../packages/plugin-format/schema/plugin-format.schema.json");

/// Validate a RAW parsed `.claude-plugin/plugin.json` body against the frozen
/// `PluginManifest` schema (issue #701, sibling of #691 on the `plugin_format`
/// seam).
///
/// `ManifestApprovals` deliberately reads only `name` + `approvalPolicy` (see
/// its doc comment): hand-mirroring every `PluginManifest` field in Rust would
/// itself be a second ungated mirror of a Python model, which is the drift
/// class this repo already tracks elsewhere (ADR-0041). Validating the raw
/// JSON against the committed schema instead means an invalid OTHER field
/// (e.g. `commands: 123`) is caught here, matching the runner's
/// `PluginManifest.model_validate` failing on the exact same input, without
/// this Rust code needing to know that field exists at all.
///
/// Returns the joined validator failures on failure so the CLI's error names the
/// actual offending field rather than an approximation of one -- as LOCATIONS
/// only, via `redact::schema_violation`. `ValidationError`'s own `Display`
/// serializes the failing instance into its message, and this string reaches two
/// places a manifest's own content must never reach: `skill approvals`' error and
/// (through `parse_manifest_gates`) `info`'s exit-0 `diagnostics` payload.
fn validate_against_plugin_format_schema(
    raw: &serde_json::Value,
) -> std::result::Result<(), String> {
    static VALIDATOR: std::sync::OnceLock<jsonschema::Validator> = std::sync::OnceLock::new();
    let validator = VALIDATOR.get_or_init(|| {
        let mut doc: serde_json::Value = serde_json::from_str(PLUGIN_FORMAT_SCHEMA).expect(
            "packages/plugin-format/schema/plugin-format.schema.json is committed and valid JSON",
        );
        // The committed document's root is the bare `$defs` container (no
        // `type`/`required` of its own); point the root at `PluginManifest`
        // instead. Same document, same `$defs`, different entry point.
        doc["$ref"] = serde_json::Value::String("#/$defs/PluginManifest".to_string());
        jsonschema::validator_for(&doc)
            .expect("plugin-format.schema.json's PluginManifest def compiles to a validator")
    });
    let errors: Vec<String> = validator
        .iter_errors(raw)
        .map(|e| crate::redact::schema_violation(&e))
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// Output of `skill approvals`: the bundle's declared gates, or the env
/// assignment a set/clear WOULD need (this tier boots the runner from env, so
/// there is nothing to mutate; see ADR-0041).
#[derive(Debug)]
pub enum SkillApprovalsOutput {
    Gates {
        gates: Vec<(String, String)>,
    },
    Env {
        env: String,
        restart: String,
        bundle_note: String,
    },
}

impl crate::ui::CliOutput for SkillApprovalsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            SkillApprovalsOutput::Gates { gates } => {
                let gates: Vec<serde_json::Value> = gates
                    .iter()
                    .map(|(gate, route)| serde_json::json!({"gate": gate, "route": route}))
                    .collect();
                serde_json::json!({ "gates": gates })
            }
            SkillApprovalsOutput::Env {
                env,
                restart,
                bundle_note,
            } => serde_json::json!({
                "env": env,
                "restart": restart,
                "bundle_note": bundle_note,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            SkillApprovalsOutput::Gates { gates } => {
                ui.payload(&gates_summary_line(gates));
                for (gate, route) in gates {
                    ui.kv(gate, route);
                }
            }
            SkillApprovalsOutput::Env {
                env,
                restart,
                bundle_note,
            } => {
                ui.payload(&human_env_line(env));
                ui.kv("restart", restart);
                ui.kv("bundle", bundle_note);
            }
        }
    }
}

/// The human-rendered form of the `NAME=value` assignment `skill approvals`
/// hands back.
///
/// The guidance tells the caller to export this, so the human line is read as
/// shell text and the value must survive being pasted into one. A gate name is
/// only rejected here for a comma or for being whitespace-only, so `--gate 'Foo
/// Bar'` or `--gate '$(cmd)'` are accepted and would otherwise be word-split or
/// command-substituted by the shell -- the runner would receive a different gate
/// than the one printed, or the paste would execute shell text outright.
/// Quoting the right-hand side is what keeps the printed line and the applied
/// value the same string.
///
/// The `--json` `env` field deliberately does NOT get this treatment: a machine
/// consumer wants the raw assignment, not a shell literal it would have to
/// unquote.
fn human_env_line(env: &str) -> String {
    match env.split_once('=') {
        Some((name, value)) => format!("{name}={}", shell_quote(value)),
        // Not reachable from `skill_approvals` (it always formats `NAME=`), but
        // echoing the input beats inventing an assignment that was never made.
        None => env.to_string(),
    }
}

/// The human summary line for `skill approvals`' gate view.
///
/// Scoped to what this command actually knows: it reads the bundle on disk and
/// nothing else. The runner also unions in `CURIE_APPROVAL_REQUIRED_TOOLS`,
/// resolved once at container boot and invisible from here, so neither branch may
/// present the bundle's gates as the complete effective set. Saying "no gates
/// declared, so calls run without approval" would be flatly false against a runner
/// booted with that override set.
fn gates_summary_line(gates: &[(String, String)]) -> String {
    let unseen = "a CURIE_APPROVAL_REQUIRED_TOOLS override applied at container boot may gate more, and is not visible from the bundle";
    if gates.is_empty() {
        format!("the bundle declares no approval gates ({unseen})")
    } else {
        format!("{} bundle-declared gate(s) ({unseen}):", gates.len())
    }
}

/// Read the bundle's declared approval gates as `(gate, route)` pairs.
///
/// The manifest is probed at `.claude-plugin/plugin.json` then `plugin.json`,
/// mirroring the runner's `load_approval_policy`. Since #520 that function is
/// single-tier and fail-closed: ANY gate it cannot arm exactly as declared --
/// a required key missing (the manifest's `name`, or a gate's `gate`/`route`),
/// or a key present but empty/whitespace so it keys nothing -- raises rather
/// than degrading to "nothing gated". So both shapes are reported here as one
/// usage error naming the problem. The manifest is invalid input, deterministic
/// and fixable by hand; reporting an empty list instead would read as "no gates
/// configured", a different lie (#607).
///
/// A manifest with no `approvalPolicy` at all, or an explicitly empty `gates`
/// list, declares no gate: no gates and no error. A bundle with no manifest is a
/// usage error (the plugin dir is simply wrong).
fn read_bundle_gates(plugin_dir: &Path) -> Result<Vec<(String, String)>> {
    let manifest_path = MANIFEST_LOCATIONS
        .iter()
        .map(|loc| plugin_dir.join(loc))
        .find(|path| path.is_file())
        .ok_or_else(|| crate::exit::usage(crate::scaffold::no_manifest_message(plugin_dir)))?;
    let body = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    parse_manifest_gates(&body, &manifest_path.display().to_string())
}

/// Parse the `approvalPolicy` gates out of a plugin-manifest JSON body, mirroring
/// the runner's `load_approval_policy` fail-closed semantics (#520): any declared
/// gate the runner cannot arm exactly as declared -- a missing REQUIRED key, or a
/// key present but empty -- refuses the whole manifest rather than arming a
/// subset, so this reports a usage error for both. `source` labels the manifest in
/// errors. Shared by the skill tier (manifest on local disk) and the local/cluster
/// tiers (manifest pulled from the deployed bundle over the API, #546) so both
/// read gates identically. Also validates the raw manifest against the frozen
/// `plugin_format` schema once a policy is declared (#701) -- see
/// `validate_against_plugin_format_schema`.
pub(crate) fn parse_manifest_gates(body: &str, source: &str) -> Result<Vec<(String, String)>> {
    let invalid = |problem: &str| {
        crate::exit::usage(format!(
            "invalid plugin manifest {source}: {problem}. The runner rejects this manifest and arms ZERO approval gates, including any well-formed ones"
        ))
    };
    let raw: serde_json::Value =
        serde_json::from_str(body).map_err(|e| invalid(&format!("not valid JSON ({e})")))?;
    // #701: the runner only promotes to full-`PluginManifest` validation once
    // `approvalPolicy` is actually declared (an absent or explicit-null policy
    // returns the honest empty policy without reading the rest of the
    // manifest, matching `resolve_approval_policy`'s early return). Mirror that
    // gate exactly: only above this line would a manifest invalid in some
    // OTHER field silently slip past, since `ManifestApprovals` below never
    // looks at it.
    if raw.get("approvalPolicy").is_some_and(|v| !v.is_null()) {
        if let Err(detail) = validate_against_plugin_format_schema(&raw) {
            return Err(invalid(&format!(
                "manifest fails plugin_format schema validation ({detail})"
            )));
        }
    }
    let manifest: ManifestApprovals = serde_json::from_value(raw)
        .map_err(|e| invalid(&format!("does not match the expected manifest shape ({e})")))?;
    if manifest.name.is_none() {
        return Err(invalid("missing the required `name` field"));
    }
    let mut gates = Vec::new();
    for g in manifest.approval_policy.unwrap_or_default().gates {
        let (gate, route) = match (&g.gate, &g.route) {
            (None, _) => return Err(invalid("a gate in `approvalPolicy.gates` omits `gate`")),
            (_, None) => return Err(invalid("a gate in `approvalPolicy.gates` omits `route`")),
            (Some(gate), Some(route)) => (gate.trim(), route.trim()),
        };
        // Present but empty: parses, but keys nothing once trimmed, so the
        // runner refuses to boot rather than arm a partial policy (#520).
        // Reporting it as armed here would name a gate that stops the runner.
        if gate.is_empty() || route.is_empty() {
            return Err(invalid(
                "a gate in `approvalPolicy.gates` has an empty `gate` or `route`",
            ));
        }
        // The runner keys a dict by the trimmed gate name, so a repeated gate
        // collapses to one entry: the first declaration fixes the position, the
        // last one wins the route. Mirror both halves -- keeping the duplicate
        // would report a gate the runner never arms plus a stale route.
        match gates
            .iter_mut()
            .find(|(name, _): &&mut (String, String)| name == gate)
        {
            Some((_, existing_route)) => *existing_route = route.to_string(),
            None => gates.push((gate.to_string(), route.to_string())),
        }
    }
    Ok(gates)
}

/// The active deployment whose bundle is in force for an agent: prod outranks dev
/// (mirroring `binding.py`), then most recent. `list_deployments` returns rows
/// oldest-first, so "most recent" is the last match. `None` when the agent has no
/// active deployment (nothing is running its bundle yet).
pub(crate) fn select_in_force_deployment(
    deployments: &[crate::api::Deployment],
) -> Option<&crate::api::Deployment> {
    let active: Vec<&crate::api::Deployment> = deployments
        .iter()
        .filter(|d| d.status == "active")
        .collect();
    active
        .iter()
        .rev()
        .find(|d| d.environment == "prod")
        .or_else(|| active.iter().rev().find(|d| d.environment == "dev"))
        .or_else(|| active.last())
        .copied()
}

/// The approval-gate tool names armed by the agent's in-force DEPLOYED bundle
/// manifest (#546): resolve the active deployment → its version → the version's
/// stored manifest → `approvalPolicy.gates[].gate`. This is the source the runner
/// consults that the platform's mutable `approval_required_tools` field does NOT
/// carry, so `local`/`cluster approvals` must union it in or it reports an empty
/// gate set while the manifest gate is armed and blocking. Best-effort on the
/// fetch (no deployment / no bundle / API hiccup → no manifest gates), but a
/// deployed manifest that is actually invalid is surfaced (it disarms every gate).
///
/// The empty-vec outcomes are NOT interchangeable, which is why this returns
/// `ManifestGates` rather than a bare list (#607): "the manifest declares nothing"
/// is an answer, "the API call failed" is the absence of one, and the caller's
/// report reads very differently for each.
enum ManifestGates {
    /// The lookup completed. The vec is the manifest's armed gates, empty when
    /// there is no deployed bundle, no manifest in it, or no `approvalPolicy`.
    Readable(Vec<String>),
    /// The lookup did not complete, so the manifest's gates are unknown. Carries
    /// the reason, which is reported rather than swallowed.
    Unreadable(String),
}

async fn deployed_manifest_gate_names(client: &ApiClient, agent_id: &str) -> Result<ManifestGates> {
    let deployments = match client.list_deployments(agent_id).await {
        Ok(d) => d,
        Err(err) => {
            return Ok(ManifestGates::Unreadable(format!(
                "listing the agent's deployments failed: {err}"
            )))
        }
    };
    // No active deployment is a real answer: nothing is running this agent's
    // bundle, so no manifest gate can be armed from one.
    let Some(deployment) = select_in_force_deployment(&deployments) else {
        return Ok(ManifestGates::Readable(Vec::new()));
    };
    // A deployment IS in force but names no version. `version_id` is
    // `#[serde(default)]`, so this is response drift rather than a stated absence
    // -- the bundle exists and we simply cannot address it.
    let Some(version_id) = deployment.version_id.clone() else {
        return Ok(ManifestGates::Unreadable(format!(
            "the in-force deployment {} reports no version id",
            deployment.id
        )));
    };
    let files = match client.bundle_files(agent_id, &version_id).await {
        Ok(f) => f,
        Err(err) => {
            return Ok(ManifestGates::Unreadable(format!(
                "fetching the deployed bundle's files failed: {err}"
            )))
        }
    };
    let Some(manifest) = files
        .iter()
        .find(|f| MANIFEST_LOCATIONS.contains(&f.path.as_str()))
    else {
        return Ok(ManifestGates::Readable(Vec::new()));
    };
    let gates = parse_manifest_gates(
        &manifest.content,
        &format!("deployed bundle manifest ({})", manifest.path),
    )?;
    Ok(ManifestGates::Readable(
        gates.into_iter().map(|(gate, _route)| gate).collect(),
    ))
}

/// POSIX-shell-quote a value for safe interpolation into emitted shell text.
///
/// Two callers, both emitting text a caller reads as shell: the bundle path named
/// by the `restart` guidance, and the right-hand side of the human-rendered
/// `NAME=value` assignment the guidance says to export. A value holding
/// whitespace or shell metacharacters (`/tmp/my bundle`, `$(cmd)`) would
/// otherwise be word-split or substituted, so what the shell sees differs from
/// what we printed. Single-quoting is the one POSIX form that quotes every
/// character literally; the only byte it cannot contain is
/// `'` itself, which is escaped by closing the quote, emitting an escaped quote,
/// and reopening (`'\''`). Done by hand rather than by pulling in a crate: the
/// rule is four lines and a dependency here is not worth the supply-chain surface.
///
/// Not shared with `ops::shell_quote`, which is deliberately different: that one
/// leaves shell-safe tokens bare because it renders helm `--set` argv for humans
/// to read, where quoting every token is noise. This one always quotes, because
/// these values are copied into a shell and an unquoted one is a silent
/// mis-target rather than a visible mistake.
fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

/// `skill approvals [--plugin-dir DIR] [--gate TOOL]... [--clear]`: view the
/// bundle's declared approval gates, or print the env assignment that sets or
/// clears the runner's override.
///
/// Unlike the `local`/`cluster` tiers there is no platform record to PATCH: this
/// tier's runner resolves `CURIE_APPROVAL_REQUIRED_TOOLS` once at container
/// boot. So set/clear mutate nothing and instead hand back the assignment plus
/// the two caveats that make it honest (issue #459).
pub async fn skill_approvals(
    plugin_dir: PathBuf,
    gate: Vec<String>,
    clear: bool,
) -> Result<SkillApprovalsOutput> {
    if clear && !gate.is_empty() {
        return Err(crate::exit::usage(
            "--clear cannot be combined with --gate (clear removes the env override)",
        ));
    }
    for g in &gate {
        if g.trim().is_empty() {
            return Err(crate::exit::usage("--gate cannot be empty"));
        }
        if g.contains(',') {
            return Err(crate::exit::usage(format!(
                "--gate {g:?} cannot contain a comma: {APPROVAL_TOOLS_ENV} is comma-separated"
            )));
        }
    }
    if !clear && gate.is_empty() {
        return Ok(SkillApprovalsOutput::Gates {
            gates: read_bundle_gates(&plugin_dir)?,
        });
    }
    // The set/clear path emits guidance that names this bundle and tells the
    // caller to re-boot a runner for it, so it must be at least as sure the
    // bundle exists as the view path is -- otherwise `--plugin-dir /does/not/exist`
    // exits 0 with instructions that fail at `skill up`, which is the tier-parity
    // lie this command exists to avoid (issue #459). Same resolution and same
    // validation as the view path, deliberately reusing the one function so the
    // two paths cannot diverge on what counts as a usable bundle. A manifest
    // present and valid but declaring no `approvalPolicy` yields an empty list
    // and no error: setting an override for a bundle that declares no gates is
    // exactly the legitimate case, so only a missing, unreadable, or invalid
    // manifest is rejected. The gates themselves are irrelevant here; the call is
    // for its validation.
    read_bundle_gates(&plugin_dir)?;
    let tools: Vec<&str> = gate.iter().map(|g| g.trim()).collect();
    Ok(SkillApprovalsOutput::Env {
        env: format!("{APPROVAL_TOOLS_ENV}={}", tools.join(",")),
        // This states the MECHANISM and the DELTA; it deliberately does not
        // synthesize a command line to paste. `skill up` carries the runner's
        // whole configuration in its flags (`StartOpts`: image, port, name,
        // network, otel_endpoint, budget, model, local_model, fake_model, and
        // repeatable secret), and `skill approvals` reads only the bundle on
        // disk -- it has no idea which of those the caller passed. A synthesized
        // `skill up --secret ...` would therefore re-boot the runner on DEFAULTS
        // plus the approval var: a different model provider, a different image
        // and port, and every other `--secret` connector credential silently
        // dropped. Naming the caller's own invocation as the thing to re-run is
        // the only form that stays true without knowing it.
        //
        // The clauses that remain are each verifiable:
        // 1. `skill up` forwards an env var into the runner only when its NAME is
        //    on the passthrough list, and the model-credential names are all that
        //    list holds by default (`select_passthrough_env`). `--secret NAME`
        //    appends to it (`merge_secret_env`), so a re-run without it arms
        //    nothing.
        // 2. `start` hard-errors when a runner is already recorded for the dir, so
        //    an existing runner must be stopped first.
        // 3. `stop` takes no args and hardcodes `Path::new(".")`, so `skill down`
        //    can only act on the bundle in the CWD -- there is no `--plugin-dir`
        //    for it. Naming the bundle dir (shell-quoted, since it is read as a
        //    path in shell text) tells a caller working elsewhere which bundle
        //    this output is about.
        restart: format!(
            "env resolves once at container boot, so nothing changes until the runner re-boots. This output is about the bundle at {}. To apply it: export the assignment above, then re-run your own original `curie skill up` invocation for that bundle with `--secret {APPROVAL_TOOLS_ENV}` added -- a plain `curie skill up` does not forward it. This command cannot see how that runner was started, so re-run your invocation rather than a fresh one, which would boot on defaults and drop your other flags. Stop an already-recorded runner first with `curie skill down`, run from that bundle directory (it takes no --plugin-dir and acts on the bundle in the current directory).",
            shell_quote(&plugin_dir.display().to_string())
        ),
        // The runner UNIONS the bundle's declared gates with this env override,
        // so saying only "set/cleared" would lie by omission about what is armed.
        bundle_note: if clear {
            "clears only the env override; gates declared in the bundle manifest stay armed"
                .to_string()
        } else {
            "adds to the gates declared in the bundle manifest; it cannot remove one".to_string()
        },
    })
}

// The reason/alternative for each tier-unavailable skill verb has TWO consumers:
// the runtime `{error, fix}` payload built by `exit::unsupported` below, and the
// clap `about` text in `main.rs` that flows into the committed
// `command-manifest.json` (the discovery surface the UI parity mirror reads).
// Nothing gates prose against prose, so they are single-sourced here: a stale
// help string is the same class of lie as a stale runtime answer, just on the
// discovery surface (issue #459, ADR-0041).

/// Why `skill versions` cannot be answered at this tier.
pub const VERSIONS_REASON: &str =
    "`skill up` runs the bundle bytes on disk, so no deployed version is assigned";
/// Where to run `versions` instead.
pub const VERSIONS_ALT: &str =
    "use `curie local versions <agent>` or `curie cluster versions <agent>` for a deployed agent";
/// Why `skill memory` cannot be answered at this tier.
pub const MEMORY_REASON: &str =
    "this tier configures no memory namespace: `skill up` never sets a memory ref, and there is no platform here to own or address one";
/// Where to run `memory` instead.
pub const MEMORY_ALT: &str =
    "use `curie local memory <agent>` or `curie cluster memory <agent>` for a deployed agent";

/// `skill versions`: answered, but unavailable at this tier by construction.
///
/// A version exists only because the platform assigns a `bundle_sha256` and a
/// `version_label` at deploy; `skill up` runs whatever bytes are on disk, so
/// there is no release to inspect here (issue #459, ADR-0041).
pub fn skill_versions_unavailable() -> anyhow::Error {
    crate::exit::unsupported("versions", VERSIONS_REASON, VERSIONS_ALT)
}

/// `skill memory`: answered, but not a capability of this tier.
///
/// Memory is a namespace some *platform* provisions, addresses, and owns; the
/// `local`/`cluster` tiers have one, and this tier has none. `skill up` never
/// sets `CURIE_MEMORY_REF`, so the runner it boots resolves a
/// `NullMemoryStore` and nothing is persisted.
///
/// Deliberately NOT phrased as "cannot exist by construction". `--secret` has no
/// reserved-name fence (`merge_secret_env`), so an operator CAN hand-forward
/// `--secret CURIE_MEMORY_REF --secret CURIE_MEMORY_TOKEN` and the runner's
/// `resolve_memory` will dereference an `http(s)://` ref into a real
/// `StateApiMemoryStore`. That escape hatch is an operator wiring a foreign
/// tier's namespace through this one by hand -- not this tier growing the
/// capability -- and this command could not report on it regardless: it has no
/// way to read a running container's env. So the verb stays unavailable (exit 4)
/// and the reason claims only what is true: the tier configures no namespace
/// (issue #459, ADR-0041).
pub fn skill_memory_unavailable() -> anyhow::Error {
    crate::exit::unsupported("memory", MEMORY_REASON, MEMORY_ALT)
}

/// Why `skill approvals --list`/`--resolve` cannot be answered at this tier.
pub const APPROVALS_LIST_REASON: &str =
    "`skill message` talks straight to the local runner, bypassing the worker, Valkey, and the durable-Approval + resume machinery (ADR-0063), so the skill tier keeps no durable approval record to list or resolve";
/// Where to list/resolve durable approvals instead.
pub const APPROVALS_LIST_ALT: &str =
    "use `curie local approvals <agent> --list`/`--resolve` or `curie cluster approvals <agent> --list`/`--resolve` for a deployed agent, or resolve the gate within the same `skill message` session";

/// `skill approvals --list`/`--resolve`: answered, but unavailable at this tier
/// by construction (ADR-0077). The bundle's gate config (view/set/clear) still
/// works; only the durable pending-record list/resolve the local+cluster tiers
/// gained (#506/#736) has no meaning where there is no durable Approval store or
/// resume path (#766). The flags are accepted so this reports WHY (exit 4) rather
/// than erroring like an unknown-flag typo, matching `cluster deploy --secret`'s
/// decline-with-reason (issue #771, ADR-0041, ADR-0077).
pub fn skill_approvals_list_unavailable() -> anyhow::Error {
    crate::exit::unsupported(
        "approvals --list/--resolve",
        APPROVALS_LIST_REASON,
        APPROVALS_LIST_ALT,
    )
}

/// Why `info --check-mcp` is not answered at the local/cluster tiers today.
pub const INFO_CHECK_MCP_REASON: &str = "the MCP load probe boots a runner container against a bundle DIRECTORY (`curie_runner.check`), and this CLI does not yet write a deployed bundle's stored files out to a directory to probe; the bytes themselves are reachable over the API, so this is a step this CLI has not built rather than something the tier makes impossible";
/// Where to probe MCP load instead, and what would close the gap.
pub const INFO_CHECK_MCP_ALT: &str = "run `curie skill info --plugin-dir <dir> --check-mcp` (or `curie skill check`) against the bundle source, then deploy the bundle that probed clean; probing a deployed bundle directly is a tracked follow-up (materialize `ApiClient::bundle_files` into a temp directory and hand it to `run_check_report`)";

/// `<local|cluster> info --check-mcp`: answered with a plain failure (exit 1),
/// deliberately NOT `ExitClass::Unsupported`.
///
/// The static half of `info` works at every tier; only the container probe is
/// missing here. The flag is still DECLARED at every tier so this reports WHY
/// rather than erroring like an unknown-flag typo, matching
/// `skill_approvals_list_unavailable` (issue #771, ADR-0041) -- but that is
/// where the resemblance stops, and the exit class differs on purpose.
///
/// Exit 4 is reserved for a concept absent **by construction**, one no input and
/// no future release can answer here (ADR-0041). This is not that. ADR-0083
/// decision 3 already settled the underlying fact for the verb: a deployed
/// bundle's bytes are fully reachable as `(path, content)` pairs via
/// `ApiClient::bundle_files`, and `run_check_report` takes a `PathBuf`, so
/// writing those files to a temp directory and running the identical probe is
/// mechanically available today. Nothing but the code to do it is missing.
///
/// The tempting counter-argument -- that a probe on this workstation measures
/// whether the servers load HERE rather than in the platform's own sandbox -- is
/// refuted by the alternative this very decline recommends, which is itself a
/// local probe standing in for the deployed one. The probe is offline and
/// credential-free against the same runner image at every tier; if a local probe
/// answered a different question, the recommended workaround would mislead too.
///
/// So the reason claims only what is true (the bar set by
/// `skill_memory_unavailable`): the reconstruction step is unbuilt. "Not yet" is
/// not "never", and an agent that reads exit 4 stops retrying and stops asking.
pub fn info_check_mcp_unavailable() -> anyhow::Error {
    anyhow::Error::from(
        crate::exit::CliError::failure(crate::exit::not_available_here_message(
            "info --check-mcp",
            INFO_CHECK_MCP_REASON,
            INFO_CHECK_MCP_ALT,
        ))
        .with_fix(INFO_CHECK_MCP_ALT),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        absent_container_note, merge_secret_env, parse_credential_env_file, parse_manifest_gates,
        plan_recorded_state, plan_recorded_teardown, plan_skill_down, replace_first_line,
        report_sweep, resolve_cases_path, resolve_env_file_credentials, seed_env_if_missing,
        select_in_force_deployment, select_passthrough_env, sweep_json_row, sweep_table_row,
        validate_slack_channel, ApprovalGateDecl, DownPlan, EnvSeed, RecordedStatePlan,
        RecordedTeardown, SweepRow,
    };
    use serde::Deserialize;
    use std::path::{Path, PathBuf};

    #[test]
    fn env_file_resolver_respects_shell_and_vault_precedence() {
        let parsed = vec![
            (
                "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                "oat-file".to_string(),
            ),
            ("ANTHROPIC_API_KEY".to_string(), "sk-file".to_string()),
        ];
        // Nothing higher present -> the file fills both SDK names.
        let none_present = |_: &str| false;
        assert_eq!(
            resolve_env_file_credentials(&parsed, &none_present),
            vec![
                (
                    "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                    "oat-file".to_string()
                ),
                ("ANTHROPIC_API_KEY".to_string(), "sk-file".to_string()),
            ]
        );
        // A higher source (shell env or vault) already has the OAuth token, so
        // the file fills only the still-missing API key: shell > vault > file.
        let oauth_present = |name: &str| name == "CLAUDE_CODE_OAUTH_TOKEN";
        assert_eq!(
            resolve_env_file_credentials(&parsed, &oauth_present),
            vec![("ANTHROPIC_API_KEY".to_string(), "sk-file".to_string())]
        );
    }

    #[test]
    fn env_file_curie_credential_dominates_the_sdk_pair() {
        let parsed = vec![
            ("CURIE_CREDENTIALS".to_string(), "byo-file".to_string()),
            ("ANTHROPIC_API_KEY".to_string(), "sk-file".to_string()),
        ];
        // The file's CURIE_CREDENTIALS wins and is returned alone.
        let none = |_: &str| false;
        assert_eq!(
            resolve_env_file_credentials(&parsed, &none),
            vec![("CURIE_CREDENTIALS".to_string(), "byo-file".to_string())]
        );
        // A BYO credential from a higher source dominates -> nothing from the
        // file (the SDK pair never rides alongside CURIE_CREDENTIALS).
        let byo_present = |name: &str| name == "CURIE_CREDENTIALS";
        assert!(resolve_env_file_credentials(&parsed, &byo_present).is_empty());
    }

    #[test]
    fn parse_credential_env_file_reads_only_recognized_nonempty_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(".env");
        std::fs::write(
            &path,
            "ANTHROPIC_API_KEY=sk-real\n\
             CURIE_CREDENTIALS=\n\
             UNRELATED=leaked\n\
             CLAUDE_CODE_OAUTH_TOKEN=oat-real\n",
        )
        .unwrap();
        let parsed = parse_credential_env_file(&path).unwrap();
        // Recognized + non-empty only: the empty CURIE_CREDENTIALS and the
        // UNRELATED key are dropped, never absorbed (#749/#540).
        assert_eq!(
            parsed,
            vec![
                ("ANTHROPIC_API_KEY".to_string(), "sk-real".to_string()),
                (
                    "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                    "oat-real".to_string()
                ),
            ]
        );
        assert!(!parsed.iter().any(|(key, _)| key == "UNRELATED"));
    }

    #[test]
    fn parse_credential_env_file_errors_on_a_missing_file() {
        let err = parse_credential_env_file(Path::new("/no/such/curie/env/file")).unwrap_err();
        assert!(err.to_string().contains("--env-file"), "{err}");
    }

    fn row(model: &str, passed: usize, completed: usize, total: usize) -> SweepRow {
        SweepRow {
            model: model.into(),
            passed,
            completed,
            total,
            plumbing: 0,
        }
    }

    #[test]
    fn never_completed_requires_a_nonempty_row_with_zero_completions() {
        // The distinct #622 outcome: cases ran, none completed.
        assert!(row("bogus", 0, 0, 5).never_completed());
        // A real 0% (every case completed and lost on the grader) is NOT this
        // outcome -- the negative control the acceptance criteria calls out.
        assert!(!row("opus", 0, 5, 5).never_completed());
        // A model that completed and passed some cases is obviously not this
        // outcome either.
        assert!(!row("opus", 3, 5, 5).never_completed());
        // An empty row (no cases at all) must not be misread as never-completed;
        // there is nothing to have failed to complete.
        assert!(!row("opus", 0, 0, 0).never_completed());
    }

    #[test]
    fn a_real_zero_percent_model_still_exits_ok_and_reports_zero_percent() {
        // Negative control (acceptance criterion 4): a model that legitimately
        // scores 0% -- every case completed, the grader just disagreed -- must
        // still report 0% and exit 0. A sweep stays a comparison, not a gate.
        let rows = vec![row("opus", 0, 5, 5), row("sonnet", 2, 5, 5)];
        assert!(report_sweep(&rows).is_ok());
    }

    #[test]
    fn a_model_with_zero_completed_turns_fails_the_sweep_loudly() {
        // The bug this issue fixes: an unresolvable model must not read as an
        // indistinguishable 0%. `report_sweep` returns `Err` (never
        // `std::process::exit` itself, so a caller's port-forward guard still
        // drops via normal unwind) and the message names the model and a likely
        // cause instead of the eval consumer.
        let rows = vec![row("bogus-model-xyz", 0, 0, 5), row("opus", 3, 5, 5)];
        let err = report_sweep(&rows).expect_err("a never-completed row must fail the sweep");
        let msg = err.to_string();
        assert!(msg.contains("bogus-model-xyz"), "{msg}");
        assert!(!msg.contains("eval consumer"), "{msg}");
        assert!(
            msg.contains("never resolved") || msg.contains("zero completed turns"),
            "{msg}"
        );
        let (class, _fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Failure);
    }

    #[test]
    fn every_model_never_completed_still_names_every_one() {
        let rows = vec![row("model-alpha", 0, 0, 3), row("model-beta", 0, 0, 3)];
        let err = report_sweep(&rows).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("model-alpha"), "{msg}");
        assert!(msg.contains("model-beta"), "{msg}");
    }

    #[test]
    fn replace_clears_a_stale_record_for_the_very_container_it_replaces() {
        // A bundle holding both a stale runner.json and a live container of that
        // name was unrecoverable with --replace: the record refused the boot
        // before the preflight could remove anything (#747).
        assert_eq!(
            plan_recorded_state(Some("curie-runner-local"), "curie-runner-local", true),
            RecordedStatePlan::ClearAndProceed
        );
        assert_eq!(
            plan_recorded_state(None, "curie-runner-local", false),
            RecordedStatePlan::Proceed
        );
    }

    #[test]
    fn replace_does_not_clear_a_record_naming_a_different_runner() {
        // Removing one container is no reason to forget another: a record for a
        // different, still-live runner keeps refusing, with or without --replace.
        assert_eq!(
            plan_recorded_state(Some("curie-runner-local"), "curie-example-42", true),
            RecordedStatePlan::Refuse
        );
        assert_eq!(
            plan_recorded_state(Some("curie-runner-local"), "curie-runner-local", false),
            RecordedStatePlan::Refuse
        );
    }

    #[test]
    fn an_absent_container_note_says_the_stale_state_was_cleared() {
        // Only the recorded path clears `.curie/runner.json`, and this sentence
        // is the user's only signal that it did, so the two notes are NOT
        // interchangeable (#747).
        assert_eq!(
            absent_container_note("curie-runner-local", true),
            "container 'curie-runner-local' was already gone; cleared stale state"
        );
        // The --name paths clear nothing, so they must not claim to.
        assert_eq!(
            absent_container_note("curie-example-42", false),
            "container 'curie-example-42' was already gone"
        );
    }

    #[test]
    fn recorded_teardown_removes_the_container_it_actually_recorded() {
        // `docker ps` reports a short id and `docker run` a full one, so the same
        // container must still be recognized across that truncation.
        // And the removal targets the PROBED id, not the recorded one and never
        // the name: a name can change hands between the check and the removal.
        assert_eq!(
            plan_recorded_teardown(
                "9f2c1d3e4b5a6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f",
                "curie-runner-local",
                Some("9f2c1d3e4b5a")
            ),
            RecordedTeardown::Remove {
                id: "9f2c1d3e4b5a".into()
            }
        );
        // Nothing holds the name: no removal to claim, the record still clears.
        assert_eq!(
            plan_recorded_teardown("9f2c1d3e4b5a", "curie-runner-local", None),
            RecordedTeardown::AlreadyGone
        );
    }

    #[test]
    fn recorded_teardown_refuses_a_container_that_merely_reuses_the_name() {
        // Bundle B booted a NEW container under the same name (its own
        // `skill up --replace`). A plain `skill down` in bundle A must not
        // destroy it just because the name still matches (#747).
        let plan = plan_recorded_teardown(
            "aaaa1111bbbb2222",
            "curie-runner-local",
            Some("cccc3333dddd"),
        );
        let RecordedTeardown::Hijacked { message } = plan else {
            panic!("a different container holding the recorded name must not be removed");
        };
        assert!(message.contains("curie-runner-local"), "{message}");
        assert!(message.contains("cccc3333dddd"), "{message}");
        assert!(message.contains("nothing was removed"), "{message}");
        assert!(
            message.contains("curie skill down --name curie-runner-local"),
            "{message}"
        );
    }

    #[test]
    fn skill_down_removes_the_recorded_runner() {
        assert_eq!(
            plan_skill_down(Some("curie-runner-local"), None, false),
            DownPlan::Recorded {
                container: "curie-runner-local".into()
            }
        );
    }

    #[test]
    fn skill_down_targets_a_name_that_is_not_the_recorded_runner() {
        // Only `Recorded` clears `.curie/runner.json`. An explicit --name that
        // disagrees with the record is a targeted removal, so the still-running
        // recorded runner keeps its state file, ollama container, and network
        // instead of being silently orphaned (#747).
        assert_eq!(
            plan_skill_down(Some("curie-runner-local"), Some("curie-example-42"), true),
            DownPlan::Targeted {
                container: "curie-example-42".into()
            }
        );
    }

    #[test]
    fn skill_down_does_not_claim_a_removal_of_an_absent_targeted_container() {
        // `docker rm -f <missing>` exits 0, so the removal itself cannot tell a
        // real teardown from a no-op. Absence has to come from the probe, or the
        // verb reports "stopped and removed" for a container that was never
        // there (#747). Still not an error -- just not a removal.
        let plan = plan_skill_down(Some("curie-runner-local"), Some("curie-747-absent"), false);
        assert_eq!(
            plan,
            DownPlan::TargetedAbsent {
                container: "curie-747-absent".into()
            }
        );
        // The only variants that report a removal are the ones that do one.
        assert!(!matches!(
            plan,
            DownPlan::Targeted { .. } | DownPlan::Recorded { .. } | DownPlan::Orphan { .. }
        ));
    }

    #[test]
    fn skill_down_with_the_recorded_name_is_the_full_recorded_teardown() {
        // Naming the recorded container explicitly is the state-clearing
        // teardown, not a targeted removal that would strand the record.
        assert_eq!(
            plan_skill_down(
                Some("curie-runner-local"),
                Some("curie-runner-local"),
                false
            ),
            DownPlan::Recorded {
                container: "curie-runner-local".into()
            }
        );
    }

    #[test]
    fn skill_down_falls_back_to_container_identity_without_state() {
        // The reported wedge (#747): an orphaned container and no runner.json.
        // `skill down` must be able to clear it.
        assert_eq!(
            plan_skill_down(None, None, true),
            DownPlan::Orphan {
                container: "curie-runner-local".into()
            }
        );
        assert_eq!(
            plan_skill_down(None, Some("curie-example-42"), true),
            DownPlan::Orphan {
                container: "curie-example-42".into()
            }
        );
    }

    #[test]
    fn skill_down_with_nothing_to_remove_names_the_container_and_the_remedy() {
        let DownPlan::Nothing { message } = plan_skill_down(None, None, false) else {
            panic!("no state and no container is nothing to remove");
        };
        assert!(message.contains("curie-runner-local"), "{message}");
        assert!(message.contains(".curie/runner.json"), "{message}");
        assert!(message.contains("--name"), "{message}");

        let DownPlan::Nothing { message } =
            plan_skill_down(None, Some("curie-eval-sweep-0"), false)
        else {
            panic!("no state and no container is nothing to remove");
        };
        assert!(message.contains("curie-eval-sweep-0"), "{message}");
    }

    #[test]
    fn replace_first_line_rewrites_only_the_first_anchored_line() {
        // The [package] version, not a dependency `version = ` line below it.
        let cargo = "[package]\nname = \"curie\"\nversion = \"0.4.0\"\n\n[dependencies]\nserde = { version = \"1\" }\n";
        let out = replace_first_line(cargo, "version = ", "version = \"0.5.0\"").unwrap();
        assert!(out.contains("version = \"0.5.0\""));
        // The dependency's inline version is untouched.
        assert!(out.contains("serde = { version = \"1\" }"));
        assert_eq!(out.matches("0.5.0").count(), 1);
        assert!(out.ends_with('\n'));
    }

    #[test]
    fn replace_first_line_preserves_indentation_and_reports_absence() {
        let chart = "apiVersion: v2\nname: curie\nappVersion: \"0.4.0\"\n";
        let out = replace_first_line(chart, "appVersion:", "appVersion: \"0.5.0\"").unwrap();
        assert!(out.contains("appVersion: \"0.5.0\""));
        assert!(replace_first_line(chart, "nonexistent:", "x").is_none());
    }

    /// Scaffold a bundle at `dir` under `name`, then overwrite its manifest's
    /// `secrets` policy with `secrets`. Shared setup for tests exercising the
    /// declared-secrets gate in `deploy()`.
    fn scaffold_with_secrets(dir: &Path, name: &str, secrets: &[&str]) {
        crate::scaffold::scaffold(dir, name).unwrap();
        let manifest_path = dir.join(".claude-plugin/plugin.json");
        let mut manifest: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
        manifest["secrets"] = serde_json::json!(secrets);
        std::fs::write(
            &manifest_path,
            serde_json::to_string_pretty(&manifest).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn default_channel_passes_local_validation() {
        assert!(validate_slack_channel(crate::api::DEFAULT_SLACK_CHANNEL).is_ok());
    }

    #[test]
    fn parse_check_report_accepts_declared_authed_flag() {
        // The frozen check-report contract gains `authed` on each declared server
        // so the CLI/UI can flag credential-gated servers the offline check never
        // exercised. It must round-trip through parse_check_report.
        let json = r#"{
            "check": "mcp-load",
            "version": 1,
            "plugin_dir": "/x",
            "declared": [
                {"name": "github", "source": ".mcp.json", "form": "bare_file", "authed": true}
            ],
            "registered": [],
            "matches": [],
            "verdict": "green",
            "reasons": [],
            "hints": []
        }"#;
        let report = super::parse_check_report(json).expect("authed report must parse");
        assert!(
            report.declared[0].authed,
            "declared[].authed must round-trip true"
        );
    }

    #[test]
    fn parse_check_report_defaults_authed_false_when_absent() {
        // Backward compat: a report from an older runner has no `authed` key. It
        // must still parse and default to false (#[serde(default)]), never fail
        // the contract on the missing field.
        let json = r#"{
            "check": "mcp-load",
            "version": 1,
            "plugin_dir": "/x",
            "declared": [
                {"name": "plain", "source": "plugin.json", "form": "inline"}
            ],
            "registered": [],
            "matches": [],
            "verdict": "green",
            "reasons": [],
            "hints": []
        }"#;
        let report = super::parse_check_report(json).expect("report without authed must parse");
        assert!(
            !report.declared[0].authed,
            "absent authed must default to false"
        );
    }

    #[test]
    fn install_preserves_existing_local_config() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".env"), "USER_SETTING=keep-me\n").unwrap();
        std::fs::write(
            root.path().join(".env.example"),
            "USER_SETTING=new-default\n",
        )
        .unwrap();

        assert_eq!(
            seed_env_if_missing(root.path()).unwrap(),
            EnvSeed::Preserved
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join(".env")).unwrap(),
            "USER_SETTING=keep-me\n"
        );
    }

    #[test]
    fn explicit_cases_path_wins() {
        let path = resolve_cases_path(
            Some(PathBuf::from("/x/cases.json")),
            std::path::Path::new("/nowhere"),
            None,
        )
        .unwrap();
        assert_eq!(path, PathBuf::from("/x/cases.json"));
    }

    #[test]
    fn falls_back_from_cwd_to_the_recorded_bundle_dir() {
        let cwd = tempfile::tempdir().unwrap();
        let bundle = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(bundle.path().join("evals")).unwrap();
        std::fs::write(bundle.path().join("evals/cases.json"), "[]").unwrap();

        // cwd has no cases: resolve into the bundle dir from the state file.
        let resolved = resolve_cases_path(None, cwd.path(), Some(bundle.path())).unwrap();
        assert_eq!(resolved, bundle.path().join("evals/cases.json"));

        // cwd cases take precedence once present.
        std::fs::create_dir_all(cwd.path().join("evals")).unwrap();
        std::fs::write(cwd.path().join("evals/cases.json"), "[]").unwrap();
        let resolved = resolve_cases_path(None, cwd.path(), Some(bundle.path())).unwrap();
        assert_eq!(resolved, cwd.path().join("evals/cases.json"));
    }

    #[test]
    fn errors_when_nothing_is_found() {
        let cwd = tempfile::tempdir().unwrap();
        let err = resolve_cases_path(None, cwd.path(), None).unwrap_err();
        assert!(err.to_string().contains("--cases"), "{err}");
    }

    #[test]
    fn rejects_hash_prefixed_channel_name() {
        let err = validate_slack_channel("#testing").unwrap_err().to_string();
        assert!(err.contains("channel ID"), "{err}");
    }

    #[test]
    fn accepts_channel_id() {
        assert!(validate_slack_channel("C0BF2CL1U2F").is_ok());
    }

    #[test]
    fn rejects_leading_whitespace_hash() {
        assert!(validate_slack_channel("  #testing").is_err());
    }

    /// A fully-credentialed host, for the cases below that are not about which
    /// ambient names happen to be exported.
    fn all_ambient_present(_name: &str) -> bool {
        true
    }

    #[test]
    fn fake_model_forwards_nothing_even_with_byo() {
        // A fake model run needs no credential: forward none, even when an
        // explicit BYO reference is present, so a real token never leaks into
        // the untrusted runner.
        assert_eq!(
            select_passthrough_env(true, false, Some("sk-or-x"), &all_ambient_present),
            Vec::<String>::new()
        );
    }

    #[test]
    fn explicit_byo_credential_forwarded_alone() {
        // A non-empty BYO credential is forwarded alone -- the ambient SDK vars
        // must not shadow the operator's chosen credential.
        assert_eq!(
            select_passthrough_env(false, false, Some("sk-or-x"), &all_ambient_present),
            vec!["CURIE_CREDENTIALS".to_string()]
        );
    }

    #[test]
    fn oauth_shaped_byo_dropped_under_base_url_override() {
        // An sk-ant-oat OAuth token authenticates nothing behind a base-URL
        // override, so it is dropped rather than left inert in /proc/1/environ
        // (issue #603). The ambient fallback is also suppressed under the override.
        assert_eq!(
            select_passthrough_env(false, true, Some("sk-ant-oat-x"), &all_ambient_present),
            Vec::<String>::new()
        );
    }

    #[test]
    fn provider_byo_kept_under_base_url_override() {
        // A non-OAuth provider key (sk-or- OpenRouter) is routed into
        // ANTHROPIC_API_KEY even behind a preset base URL, so it is still
        // forwarded -- dropping it would break BYO OpenRouter (issue #603).
        assert_eq!(
            select_passthrough_env(false, true, Some("sk-or-x"), &all_ambient_present),
            vec!["CURIE_CREDENTIALS".to_string()]
        );
    }

    #[test]
    fn oauth_shaped_byo_kept_without_override() {
        // The OAuth drop is gated on the override: on the legacy real-Anthropic
        // path an sk-ant-oat token is a valid credential and is forwarded alone.
        assert_eq!(
            select_passthrough_env(false, false, Some("sk-ant-oat-x"), &all_ambient_present),
            vec!["CURIE_CREDENTIALS".to_string()]
        );
    }

    #[test]
    fn empty_byo_credential_falls_back_to_sdk_vars() {
        // An empty CURIE_CREDENTIALS (a blank line in .env) is treated as unset,
        // so the ambient SDK vars carry the legacy real-Anthropic credential.
        assert_eq!(
            select_passthrough_env(false, false, Some(""), &all_ambient_present),
            vec![
                "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                "ANTHROPIC_API_KEY".to_string()
            ]
        );
    }

    #[test]
    fn no_byo_credential_falls_back_to_sdk_vars() {
        assert_eq!(
            select_passthrough_env(false, false, None, &all_ambient_present),
            vec![
                "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                "ANTHROPIC_API_KEY".to_string()
            ]
        );
    }

    /// One row of the committed cross-language forwarding matrix. The five
    /// inputs are booleans: the rule keys on presence, never on a credential's
    /// content.
    ///
    /// `deny_unknown_fields` makes an unrecognized key a hard parse failure
    /// rather than a silently ignored input: a row that grows a sixth input
    /// this lane cannot see would otherwise pass vacuously, which is the exact
    /// drift the gate exists to catch. A new input must be taught to this
    /// struct, to the Python lane's expected key set
    /// (apps/worker/tests/sandbox/test_vector_credential_forwarding.py), and to
    /// the vector file itself.
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ForwardingVector {
        name: String,
        /// Documentation carried by the vector file; parsed so the row's own
        /// rationale is not an unknown field, and read back into the assertion
        /// message so a failing vector explains itself.
        why: String,
        fake_model: bool,
        base_url_override: bool,
        byo_credential: bool,
        byo_oauth_shaped: bool,
        ambient_oauth: bool,
        ambient_api_key: bool,
        expected: Vec<String>,
    }

    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ForwardingVectors {
        /// The file-level rationale; parsed so it is not an unknown field.
        /// Underscore-prefixed so rustc's dead_code lint skips it; the serde
        /// rename keeps the JSON key it matches on as `comment`.
        #[serde(rename = "comment")]
        _comment: String,
        vectors: Vec<ForwardingVector>,
    }

    #[test]
    fn cli_matches_every_forwarding_vector() {
        // The Rust half of the cross-language gate (#495). The Python worker lane
        // (apps/worker/tests/sandbox/test_vector_credential_forwarding.py) reads
        // this same file, so a rule changed in one language without the other
        // fails that language's test. The rule is not restated here.
        let raw = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../tests/vectors/model-credential-forwarding.json"
        ))
        .expect("read tests/vectors/model-credential-forwarding.json");
        let parsed: ForwardingVectors = serde_json::from_str(&raw).unwrap_or_else(|err| {
            panic!(
                "parse tests/vectors/model-credential-forwarding.json: {err}\n\
                 An unknown field is rejected on purpose: a new input this lane cannot see \
                 would pass vacuously. Teach the new key to ForwardingVector here, to \
                 _EXPECTED_VECTOR_KEYS in \
                 apps/worker/tests/sandbox/test_vector_credential_forwarding.py, and to both \
                 implementations of the rule."
            )
        });
        // Guards against a rename or a truncated file making this loop vacuously pass.
        assert!(!parsed.vectors.is_empty(), "no vectors parsed");

        for vector in &parsed.vectors {
            let ambient_present = |name: &str| match name {
                "CLAUDE_CODE_OAUTH_TOKEN" => vector.ambient_oauth,
                "ANTHROPIC_API_KEY" => vector.ambient_api_key,
                _ => false,
            };
            // An OAuth-shaped BYO is sk-ant-oat; a provider key is sk-or-. Both are
            // placeholders and forwarded by NAME, so neither value enters the argv.
            let byo = vector.byo_credential.then_some(if vector.byo_oauth_shaped {
                "sk-ant-oat-PLACEHOLDER-byo"
            } else {
                "sk-or-PLACEHOLDER-byo"
            });
            assert_eq!(
                select_passthrough_env(
                    vector.fake_model,
                    vector.base_url_override,
                    byo,
                    &ambient_present
                ),
                vector.expected,
                "{}: {}",
                vector.name,
                vector.why
            );
        }
    }

    #[test]
    fn secret_env_appends_after_the_model_credential() {
        // --secret names ride alongside the model credential, in order, so an
        // authed MCP server gets its token next to the model token.
        assert_eq!(
            merge_secret_env(
                select_passthrough_env(false, false, None, &all_ambient_present),
                &["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
            ),
            vec![
                "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                "ANTHROPIC_API_KEY".to_string(),
                "GITHUB_PERSONAL_ACCESS_TOKEN".to_string(),
            ]
        );
    }

    #[test]
    fn secret_env_forwarded_even_when_model_credential_suppressed() {
        // A fake/local model suppresses the model credential but a bundle's MCP
        // secret must still reach the sandbox.
        assert_eq!(
            merge_secret_env(
                select_passthrough_env(true, false, None, &all_ambient_present),
                &["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
            ),
            vec!["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
        );
    }

    #[test]
    fn secret_env_deduplicates_against_the_credential_vars() {
        // Passing a model-credential var as --secret must not duplicate it.
        assert_eq!(
            merge_secret_env(
                select_passthrough_env(false, false, None, &all_ambient_present),
                &["ANTHROPIC_API_KEY".to_string()]
            ),
            vec![
                "CLAUDE_CODE_OAUTH_TOKEN".to_string(),
                "ANTHROPIC_API_KEY".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn deploy_names_the_remediation_when_api_is_unreachable() {
        let dir = tempfile::tempdir().unwrap();
        crate::scaffold::scaffold(dir.path(), "test-agent").unwrap();
        let hint = "kubectl -n curie port-forward svc/curie-api 8000:8000";
        let opts = super::DeployOpts {
            plugin_dir: dir.path().to_path_buf(),
            // port 1 is reserved/closed -> deterministic connection refused
            api_url: "http://127.0.0.1:1".to_string(),
            api_key: "k".to_string(),
            slack_channel: None,
            env: super::DeployEnv::Dev,
            label: Some("v0".to_string()),
            secret: vec![],
            secret_binding_supported: true,
            connect_hint: hint.to_string(),
        };
        let err = super::deploy(opts).await.unwrap_err();
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains(hint),
            "hint missing from error: {rendered}"
        );
    }

    #[test]
    fn unbound_declared_secrets_diffs_declared_against_bound() {
        // All declared names bound -> nothing unbound.
        assert!(super::unbound_declared_secrets(
            &["GH_TOKEN".to_string()],
            &["GH_TOKEN".to_string()]
        )
        .is_empty());
        // A declared name not in the bound set is returned.
        assert_eq!(
            super::unbound_declared_secrets(
                &["GH_TOKEN".to_string(), "SLACK".to_string()],
                &["GH_TOKEN".to_string()]
            ),
            vec!["SLACK".to_string()]
        );
        // Nothing declared -> nothing unbound (even with bound extras).
        assert!(super::unbound_declared_secrets(&[], &["GH_TOKEN".to_string()]).is_empty());
        // The #464 mismatch: declared the connector name, bound a different one.
        assert_eq!(
            super::unbound_declared_secrets(
                &["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()],
                &["GH_TOKEN".to_string()]
            ),
            vec!["GITHUB_PERSONAL_ACCESS_TOKEN".to_string()]
        );
        // A MALFORMED declared name (not env-var syntax) is excluded from the
        // gap: it is the plugin-format validator's job to reject it server-side,
        // so the gate must not preempt that with a misleading `--secret` message.
        assert!(super::unbound_declared_secrets(&["github-token".to_string()], &[]).is_empty());
        // A well-formed unbound name alongside a malformed one: only the
        // well-formed one is a gap.
        assert_eq!(
            super::unbound_declared_secrets(
                &["github-token".to_string(), "GITHUB_TOKEN".to_string()],
                &[]
            ),
            vec!["GITHUB_TOKEN".to_string()]
        );
    }

    #[tokio::test]
    async fn deploy_fails_when_declared_secret_is_not_bound() {
        // AC3: a declared secret NAME with no matching --secret binding fails the
        // deploy BEFORE any network attempt -- a true deploy-time error, not a
        // runtime/connection failure.
        let dir = tempfile::tempdir().unwrap();
        // Declares a NAME we will bind under the wrong key.
        scaffold_with_secrets(dir.path(), "test-agent", &["GITHUB_PERSONAL_ACCESS_TOKEN"]);

        let opts = super::DeployOpts {
            plugin_dir: dir.path().to_path_buf(),
            api_url: "http://127.0.0.1:1".to_string(),
            api_key: "k".to_string(),
            slack_channel: None,
            env: super::DeployEnv::Dev,
            label: Some("v0".to_string()),
            secret: vec!["GH_TOKEN".to_string()],
            secret_binding_supported: true,
            connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
        };
        let err = super::deploy(opts).await.unwrap_err();
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("GITHUB_PERSONAL_ACCESS_TOKEN"),
            "error must name the missing secret: {rendered}"
        );
        assert!(
            !rendered.contains("UNREACHABLE-HINT-SENTINEL"),
            "gate must fire before any network attempt (no connect hint): {rendered}"
        );
    }

    #[tokio::test]
    async fn deploy_skips_secrets_gate_when_binding_unsupported() {
        // AC2: the cluster tier cannot bind a `--secret` until #440, so the
        // declared-secrets gate is SKIPPED there. A secrets-declaring bundle must
        // NOT be preempted by the gate; deploy proceeds to the network and fails
        // on the connect path instead (naming the connect hint, never the secret).
        let dir = tempfile::tempdir().unwrap();
        scaffold_with_secrets(dir.path(), "test-agent", &["GITHUB_PERSONAL_ACCESS_TOKEN"]);

        let opts = super::DeployOpts {
            plugin_dir: dir.path().to_path_buf(),
            // port 1 is reserved/closed -> deterministic connection refused
            api_url: "http://127.0.0.1:1".to_string(),
            api_key: "k".to_string(),
            slack_channel: None,
            env: super::DeployEnv::Dev,
            label: Some("v0".to_string()),
            secret: vec![],
            secret_binding_supported: false,
            connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
        };
        let err = super::deploy(opts).await.unwrap_err();
        let rendered = format!("{err:#}");
        // The error is the network/connect path, not the secrets gate.
        assert!(
            rendered.contains("UNREACHABLE-HINT-SENTINEL"),
            "gate should be skipped, so deploy reaches the network: {rendered}"
        );
        assert!(
            !rendered.contains("GITHUB_PERSONAL_ACCESS_TOKEN"),
            "the skipped gate must not name the declared secret: {rendered}"
        );
    }

    // --- skill approvals (tier parity, issue #459) --------------------------

    /// Write a plugin manifest at `rel` under `dir`, creating parent dirs.
    fn write_manifest(dir: &std::path::Path, rel: &str, body: &str) {
        let path = dir.join(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(path, body).unwrap();
    }

    /// A minimal valid manifest, declaring no `approvalPolicy`.
    ///
    /// The set/clear path validates the bundle the same way the view path does,
    /// so every env-path test needs a real bundle on disk. Declaring no gates is
    /// the legitimate no-policy case, which that path must still accept.
    const MINIMAL_MANIFEST: &str = r#"{"name":"x","version":"1"}"#;

    /// Give `dir` the minimal valid bundle manifest the env path requires.
    fn write_minimal_manifest(dir: &std::path::Path) {
        write_manifest(dir, ".claude-plugin/plugin.json", MINIMAL_MANIFEST);
    }

    /// The gate names listed by a `skill approvals` view output's JSON.
    fn gate_names(json: &serde_json::Value) -> Vec<String> {
        json["gates"]
            .as_array()
            .expect("view JSON exposes a `gates` array")
            .iter()
            .map(|g| {
                g["gate"]
                    .as_str()
                    .expect("gate name is a string")
                    .to_string()
            })
            .collect()
    }

    fn usage_class(err: &anyhow::Error) -> crate::exit::ExitClass {
        crate::exit::classify(err).0
    }

    fn deployment(env: &str, status: &str, version: &str, ts: &str) -> crate::api::Deployment {
        crate::api::Deployment {
            id: format!("dep-{version}"),
            environment: env.into(),
            status: status.into(),
            version_id: Some(version.into()),
            deployed_at: Some(ts.into()),
        }
    }

    #[test]
    fn select_in_force_deployment_prefers_prod_then_most_recent() {
        // Oldest-first, mixed envs/statuses. prod outranks dev; among a rank the
        // most recent (last) active row wins; inactive rows are ignored (#546).
        let deps = vec![
            deployment("dev", "active", "v1", "2026-07-01"),
            deployment("prod", "superseded", "v2", "2026-07-02"),
            deployment("prod", "active", "v3", "2026-07-03"),
            deployment("dev", "active", "v4", "2026-07-04"),
        ];
        assert_eq!(
            select_in_force_deployment(&deps).and_then(|d| d.version_id.clone()),
            Some("v3".to_string()),
            "active prod wins over a newer active dev"
        );
        // No prod: newest active dev.
        let dev_only = vec![
            deployment("dev", "active", "a", "2026-07-01"),
            deployment("dev", "active", "b", "2026-07-05"),
        ];
        assert_eq!(
            select_in_force_deployment(&dev_only).and_then(|d| d.version_id.clone()),
            Some("b".to_string())
        );
        // No active deployment at all -> nothing in force.
        let none = vec![deployment("dev", "superseded", "x", "2026-07-01")];
        assert!(select_in_force_deployment(&none).is_none());
        assert!(select_in_force_deployment(&[]).is_none());
    }

    #[test]
    fn approvals_summary_line_never_claims_ungated_when_the_manifest_is_unreadable() {
        // The whole point of the three-state split (#607): "no gates found" and
        // "could not look" are different answers, and only the first one licenses
        // the affirmative claim. A failed manifest fetch used to collapse into the
        // second branch here and report the agent as running without approval.
        let ungated = super::approvals_summary_line("weather", &[], None);
        assert!(
            ungated.contains("no tools are gated (calls run without approval)"),
            "a genuinely readable, gate-free agent still gets the affirmative claim: {ungated}"
        );

        let blind = super::approvals_summary_line("weather", &[], Some("the deploy list failed"));
        assert!(
            !blind.contains("no tools are gated"),
            "an unreadable manifest must not render as an affirmative un-gated claim: {blind}"
        );
        assert!(
            blind.contains("could not be read") && blind.contains("the deploy list failed"),
            "the reason we could not look is disclosed: {blind}"
        );

        // Gates found from the platform field while the manifest was unreadable:
        // the list is real but partial, and silence about that implies complete.
        let partial =
            super::approvals_summary_line("weather", &["Bash".into()], Some("the fetch failed"));
        assert!(
            partial.contains("incomplete") && partial.contains("could not be read"),
            "a partial list discloses that more gates may be armed: {partial}"
        );

        let complete = super::approvals_summary_line("weather", &["Bash".into()], None);
        assert!(
            !complete.contains("incomplete") && complete.contains("1 gated tool(s)"),
            "a fully-read gate list makes no incompleteness caveat: {complete}"
        );
    }

    #[test]
    fn parse_manifest_gates_extracts_gate_route_pairs() {
        // The shared parser (#546) recovers approvalPolicy gates from raw manifest
        // text, the same shape `local`/`cluster approvals` union into the report.
        let gates = parse_manifest_gates(
            r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"mcp__plugin_gh_github__create_issue","route":"eng"}]}}"#,
            "test manifest",
        )
        .expect("valid manifest parses");
        assert_eq!(
            gates,
            vec![(
                "mcp__plugin_gh_github__create_issue".to_string(),
                "eng".to_string()
            )]
        );
        // A manifest missing the required `name` disarms every gate -> surfaced.
        assert!(parse_manifest_gates(
            r#"{"version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"}]}}"#,
            "bad manifest",
        )
        .is_err());
    }

    #[test]
    fn parse_manifest_gates_refuses_a_gate_the_runner_cannot_arm() {
        // NEGATIVE CONTROL for the #520 CLI mirror. The runner refuses to boot
        // on a declared gate it cannot arm, so reporting `Bash` as armed here
        // would name a gate that in fact stops the runner. Restoring the old
        // `continue` (drop the empty gate, keep its siblings) makes this fail.
        for body in [
            // Present but empty/whitespace: parses, keys nothing once trimmed.
            r#"{"name":"x","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"   ","route":"eng"}]}}"#,
            r#"{"name":"x","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"Write","route":""}]}}"#,
            // Required key missing entirely.
            r#"{"name":"x","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"Write"}]}}"#,
        ] {
            assert!(
                parse_manifest_gates(body, "partial manifest").is_err(),
                "reported gates for a manifest the runner refuses: {body}"
            );
        }
        // An explicitly empty gates list declares nothing: no gates, no error.
        assert_eq!(
            parse_manifest_gates(r#"{"name":"x","approvalPolicy":{"gates":[]}}"#, "empty")
                .expect("an empty gates list is a valid declaration of no gates"),
            Vec::new()
        );
    }

    #[test]
    fn parse_manifest_gates_refuses_a_manifest_invalid_in_an_unrelated_field() {
        // NEGATIVE CONTROL for issue #701 (sibling of #691, ADR-0041's formerly
        // "known limitation"). `ManifestApprovals` reads only `name` +
        // `approvalPolicy`, so a manifest with a well-formed policy but a
        // TYPE-INVALID unrelated modeled field (`commands` must be a string, a
        // list of strings, or null per `plugin_format.models.PluginManifest`)
        // used to parse straight through: this view would report `Bash` as
        // armed while the runner's own `PluginManifest.model_validate` raises
        // and refuses to boot with ANY of the declared gates armed -- the
        // exact silent-drift class #691 closed on the api.rs seam, reproduced
        // here on the plugin_format seam. Deleting
        // `validate_against_plugin_format_schema`'s call in
        // `parse_manifest_gates` makes this test fail (back to `Ok(vec![("Bash",
        // "eng")])`).
        let body = r#"{
            "name": "deal-desk",
            "commands": 123,
            "approvalPolicy": {"gates": [{"gate": "Bash", "route": "eng"}]}
        }"#;
        let err = parse_manifest_gates(body, "test manifest").expect_err(
            "a manifest invalid in an unrelated modeled field must not report gates as armed",
        );
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[test]
    fn parse_manifest_gates_tolerates_an_unmodeled_extra_field() {
        // Positive control paired with the test above: `plugin_format`'s models
        // are deliberately lenient (`extra="allow"`) so a real bundle carrying a
        // field this schema does not model yet (e.g. a future Claude Code key)
        // must still validate and report its gates -- the gate here is on TYPE
        // validity of MODELED fields, never on the presence of an unmodeled one.
        let body = r#"{
            "name": "deal-desk",
            "someFutureClaudeCodeKey": {"nested": true},
            "approvalPolicy": {"gates": [{"gate": "Bash", "route": "eng"}]}
        }"#;
        assert_eq!(
            parse_manifest_gates(body, "test manifest")
                .expect("an unmodeled extra field must not be rejected"),
            vec![("Bash".to_string(), "eng".to_string())]
        );
    }

    #[test]
    fn parse_manifest_gates_skips_full_validation_when_no_policy_is_declared() {
        // A manifest with no `approvalPolicy` (or an explicit `null`) never
        // reaches the runner's full-`PluginManifest` validation either (see
        // `resolve_approval_policy`'s early return), so an unrelated
        // type-invalid field must not be surfaced here -- there is no policy to
        // falsely report as armed, so the honest answer stays the empty list.
        for body in [
            r#"{"name": "deal-desk", "commands": 123}"#,
            r#"{"name": "deal-desk", "commands": 123, "approvalPolicy": null}"#,
        ] {
            assert_eq!(
                parse_manifest_gates(body, "test manifest")
                    .expect("no declared policy must not trip the full-manifest schema gate"),
                Vec::new()
            );
        }
    }

    #[test]
    fn skill_approvals_list_resolve_reported_unavailable_not_absent() {
        // ADR-0077 / ADR-0041: --list/--resolve are answered-as-unavailable at
        // the skill tier (exit 4, carrying a cross-tier fix), with the reason and
        // the local/cluster alternative -- not silently broken.
        let err = super::skill_approvals_list_unavailable();
        let (class, fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Unsupported);
        assert!(
            fix.expect("an unsupported error carries the cross-tier fix")
                .contains("approvals"),
            "the fix must point at the local/cluster alternative"
        );
        let shown = format!("{err:#}");
        assert!(shown.contains("durable approval record"), "reason: {shown}");
        assert!(shown.contains("cluster approvals"), "alternative: {shown}");
    }

    #[test]
    fn approval_gate_decl_parses_grantable_via_policy() {
        // #558: the operator opt-in on a manifest gate. Absent -> defaults false
        // (old manifests keep the no-grant baseline); present true -> parses.
        let without: ApprovalGateDecl =
            serde_json::from_str(r#"{"gate":"close_issue","route":"deal-desk"}"#)
                .expect("a gate without grantableViaPolicy parses");
        assert!(!without.grantable_via_policy);

        let with: ApprovalGateDecl = serde_json::from_str(
            r#"{"gate":"close_issue","route":"deal-desk","grantableViaPolicy":true}"#,
        )
        .expect("a gate with grantableViaPolicy:true parses");
        assert!(with.grantable_via_policy);
    }

    #[tokio::test]
    async fn skill_approvals_view_lists_bundle_gates() {
        use crate::ui::CliOutput;
        let dir = tempfile::tempdir().unwrap();
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"mcp__x__y","route":"eng"}]}}"#,
        );
        let out = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap();
        let names = gate_names(&out.to_json());
        assert!(names.contains(&"Bash".to_string()), "{names:?}");
        assert!(names.contains(&"mcp__x__y".to_string()), "{names:?}");
    }

    #[tokio::test]
    async fn skill_approvals_view_reads_fallback_plugin_json() {
        use crate::ui::CliOutput;
        let dir = tempfile::tempdir().unwrap();
        write_manifest(
            dir.path(),
            "plugin.json",
            r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"}]}}"#,
        );
        let out = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap();
        assert_eq!(gate_names(&out.to_json()), vec!["Bash".to_string()]);
    }

    #[tokio::test]
    async fn skill_approvals_view_empty_when_no_policy() {
        use crate::ui::CliOutput;
        let dir = tempfile::tempdir().unwrap();
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"name":"x","version":"1"}"#,
        );
        let out = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap();
        assert!(gate_names(&out.to_json()).is_empty());
    }

    #[tokio::test]
    async fn skill_approvals_view_refuses_an_incomplete_gate() {
        let dir = tempfile::tempdir().unwrap();
        // The second gate has an empty route, so it keys nothing and the runner
        // refuses to boot on it (#520). Mirror that refusal: reporting `Bash` as
        // armed while the runner will not start is the drift this test pins.
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"NoRoute","route":""}]}}"#,
        );
        assert!(
            super::skill_approvals(dir.path().to_path_buf(), vec![], false)
                .await
                .is_err(),
            "reported gates for a manifest the runner refuses to boot on"
        );
    }

    #[tokio::test]
    async fn skill_approvals_view_duplicate_gate_collapses_to_last_route() {
        use crate::ui::CliOutput;
        let dir = tempfile::tempdir().unwrap();
        // The runner keys a dict by trimmed gate name, so `Bash` declared twice
        // arms ONCE with the LAST route. Reporting both would name a gate the
        // runner never arms and a route it never fires.
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"stale"},{"gate":"Other","route":"ops"},{"gate":" Bash ","route":"eng"}]}}"#,
        );
        let out = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap();
        let json = out.to_json();
        let gates = json["gates"].as_array().unwrap();
        assert_eq!(
            gates.len(),
            2,
            "a gate declared twice must collapse to one entry, as the runner's dict does: {gates:?}"
        );
        let bash: Vec<&serde_json::Value> = gates.iter().filter(|g| g["gate"] == "Bash").collect();
        assert_eq!(bash.len(), 1, "exactly one Bash entry: {gates:?}");
        assert_eq!(
            bash[0]["route"], "eng",
            "the LAST declaration must win the route, mirroring the runner's dict comprehension: {gates:?}"
        );
        // First declaration fixes position, as Python dict insertion order does.
        assert_eq!(
            gates[0]["gate"], "Bash",
            "order must stay stable: {gates:?}"
        );
    }

    #[test]
    fn skill_approvals_render_never_claims_calls_are_ungated() {
        // The bundle is not the effective policy: CURIE_APPROVAL_REQUIRED_TOOLS
        // is resolved at container boot and cannot be seen from here, so neither
        // branch may imply the listed gates are the complete set.
        let empty = super::gates_summary_line(&[]);
        assert!(
            !empty.contains("without approval"),
            "an empty bundle policy must not claim calls run without approval -- an env override may gate them: {empty}"
        );
        assert!(
            empty.contains("CURIE_APPROVAL_REQUIRED_TOOLS"),
            "the empty render must name the override it cannot see: {empty}"
        );
        let listed = super::gates_summary_line(&[("Bash".into(), "eng".into())]);
        assert!(
            listed.contains("CURIE_APPROVAL_REQUIRED_TOOLS"),
            "the non-empty render must not imply the listed gates are the complete effective set: {listed}"
        );
    }

    // --- tier 1: a REQUIRED key missing disarms the WHOLE policy in the runner.
    // `plugin_format.models.ApprovalGate` declares `gate: str` / `route: str`
    // with no default, so `model_validate` raises and `load_approval_policy`
    // returns {} -- zero gates armed. Reporting the well-formed sibling as armed
    // would claim a safety control the runner never arms.

    #[tokio::test]
    async fn skill_approvals_view_gate_missing_route_key_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"gate":"NoRoute"}]}}"#,
        );
        let err = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
        // The sibling must not be reported as armed anywhere in the message.
        assert!(
            !format!("{err:#}").contains("Bash -> eng"),
            "a key-missing gate disarms every gate: {err:#}"
        );
    }

    #[tokio::test]
    async fn skill_approvals_view_gate_missing_gate_key_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"name":"x","version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"},{"route":"eng"}]}}"#,
        );
        let err = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[tokio::test]
    async fn skill_approvals_view_manifest_without_name_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        // `PluginManifest` requires `name`; without it the runner's parse raises
        // and it arms zero gates, so listing `Bash` here would be a false report.
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"version":"1","approvalPolicy":{"gates":[{"gate":"Bash","route":"eng"}]}}"#,
        );
        let err = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[tokio::test]
    async fn skill_approvals_view_malformed_json_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"name":"x",,}"#,
        );
        let err = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[tokio::test]
    async fn skill_approvals_view_without_manifest_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = super::skill_approvals(dir.path().to_path_buf(), vec![], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[tokio::test]
    async fn skill_approvals_set_emits_env_assignment() {
        use crate::ui::CliOutput;
        let dir = tempfile::tempdir().unwrap();
        write_minimal_manifest(dir.path());
        let out = super::skill_approvals(
            dir.path().to_path_buf(),
            vec!["A".into(), "B".into()],
            false,
        )
        .await
        .unwrap();
        let json = out.to_json();
        assert_eq!(
            json["env"].as_str().unwrap(),
            "CURIE_APPROVAL_REQUIRED_TOOLS=A,B"
        );
        let restart = json["restart"].as_str().unwrap();
        assert!(
            restart.contains("--secret CURIE_APPROVAL_REQUIRED_TOOLS"),
            "the restart caveat must name the --secret forwarding that actually applies the env, not a bare `skill up` (which forwards only model credentials): {restart}"
        );
        assert!(
            restart.contains("boot"),
            "the restart caveat must still say the env resolves once at container boot: {restart}"
        );
        assert!(
            restart.contains(&dir.path().display().to_string()),
            "the restart caveat must carry the caller's --plugin-dir so the re-boot targets the bundle whose approvals were read, not whatever bundle happens to be in the CWD: {restart}"
        );
        assert!(
            restart.contains("curie skill down"),
            "the restart caveat must name the stop-first step: `start` hard-errors when a runner is already recorded for the dir: {restart}"
        );
        let bundle_note = json["bundle_note"].as_str().unwrap();
        assert!(
            bundle_note.contains("adds to") && bundle_note.contains("cannot remove"),
            "the set path's bundle note must state the add-only semantics (the runner unions the bundle gates with the override): {bundle_note}"
        );
    }

    #[tokio::test]
    async fn skill_approvals_clear_emits_empty_env_assignment() {
        use crate::ui::CliOutput;
        let dir = tempfile::tempdir().unwrap();
        write_minimal_manifest(dir.path());
        let out = super::skill_approvals(dir.path().to_path_buf(), vec![], true)
            .await
            .unwrap();
        let json = out.to_json();
        assert_eq!(
            json["env"].as_str().unwrap(),
            "CURIE_APPROVAL_REQUIRED_TOOLS="
        );
        let restart = json["restart"].as_str().unwrap();
        assert!(
            restart.contains("--secret CURIE_APPROVAL_REQUIRED_TOOLS"),
            "the clear path's restart caveat must name the --secret forwarding too: a bare `skill up` never forwards the cleared assignment either: {restart}"
        );
        assert!(
            restart.contains(&dir.path().display().to_string()),
            "the clear path's restart caveat must carry the caller's --plugin-dir too, or the re-boot clears the override on the wrong bundle: {restart}"
        );
        assert!(
            restart.contains("curie skill down"),
            "the clear path's restart caveat must name the stop-first step too: {restart}"
        );
        let bundle_note = json["bundle_note"].as_str().unwrap();
        assert!(
            bundle_note.contains("only the env override") && bundle_note.contains("stay armed"),
            "the clear path's bundle note must state that it clears only the override and leaves the bundle-declared gates armed: {bundle_note}"
        );
    }

    /// `skill approvals` reads only the bundle on disk, so it cannot know which
    /// of `skill up`'s flags (image, port, name, network, otel-endpoint, budget,
    /// model, local-model, fake-model, repeatable --secret) the caller passed.
    /// A synthesized `skill up --secret ...` presented as the command to run is
    /// therefore actively destructive: following it re-boots the runner on
    /// defaults, switching model provider and dropping every other connector
    /// `--secret`. The guidance must point at the caller's OWN invocation.
    #[tokio::test]
    async fn skill_approvals_restart_points_at_the_callers_own_up_invocation() {
        use crate::ui::CliOutput;
        let dir = tempfile::tempdir().unwrap();
        write_minimal_manifest(dir.path());
        for (gate, clear) in [(vec!["A".to_string()], false), (vec![], true)] {
            let out = super::skill_approvals(dir.path().to_path_buf(), gate, clear)
                .await
                .unwrap();
            let json = out.to_json();
            let restart = json["restart"].as_str().unwrap();
            assert!(
                !restart.contains("`curie skill up --secret"),
                "the guidance must not synthesize a `skill up --secret ...` command line: this command cannot reconstruct the caller's original flags, so pasting it re-boots on defaults and drops their other --secret credentials (clear={clear}): {restart}"
            );
            assert!(
                restart.contains("your own original `curie skill up` invocation"),
                "the guidance must direct the caller to re-run their own original invocation with the flag added (clear={clear}): {restart}"
            );
        }
    }

    #[tokio::test]
    async fn skill_approvals_restart_shell_quotes_a_bundle_path_with_a_space() {
        use crate::ui::CliOutput;
        // The guidance names the bundle dir inside shell-facing text, so a path
        // the shell would split must travel quoted or it names a different dir.
        let dir = tempfile::tempdir().unwrap();
        let spaced = dir.path().join("my bundle");
        std::fs::create_dir(&spaced).unwrap();
        write_minimal_manifest(&spaced);
        let out = super::skill_approvals(spaced.clone(), vec!["A".to_string()], false)
            .await
            .unwrap();
        let json = out.to_json();
        let restart = json["restart"].as_str().unwrap();
        assert!(
            restart.contains(&format!("'{}'", spaced.display())),
            "a bundle path containing a space must be emitted single-quoted: {restart}"
        );
    }

    /// The guidance says to export the assignment, so the human line is read as
    /// shell text. `--gate` rejects only commas and whitespace-only names, so a
    /// gate with a space reaches this line; unquoted, bash word-splits it and the
    /// runner is handed a different gate than the one printed.
    #[tokio::test]
    async fn skill_approvals_human_render_shell_quotes_an_assignment_with_a_space() {
        use crate::ui::CliOutput;
        let dir = tempfile::tempdir().unwrap();
        write_minimal_manifest(dir.path());
        let out = super::skill_approvals(dir.path().to_path_buf(), vec!["Foo Bar".into()], false)
            .await
            .unwrap();
        // The --json field stays the raw assignment: a machine consumer wants the
        // value, not a shell literal it would have to unquote.
        assert_eq!(
            out.to_json()["env"].as_str().unwrap(),
            "CURIE_APPROVAL_REQUIRED_TOOLS=Foo Bar"
        );
        assert_eq!(
            super::human_env_line("CURIE_APPROVAL_REQUIRED_TOOLS=Foo Bar"),
            "CURIE_APPROVAL_REQUIRED_TOOLS='Foo Bar'"
        );
        assert_eq!(
            super::human_env_line("CURIE_APPROVAL_REQUIRED_TOOLS=$(cmd)"),
            "CURIE_APPROVAL_REQUIRED_TOOLS='$(cmd)'",
            "shell syntax in a gate name must be quoted, not left to be substituted on paste"
        );
        // The cleared assignment still renders as an assignment to an empty value.
        assert_eq!(
            super::human_env_line("CURIE_APPROVAL_REQUIRED_TOOLS="),
            "CURIE_APPROVAL_REQUIRED_TOOLS=''"
        );
    }

    #[test]
    fn shell_quote_escapes_an_embedded_single_quote() {
        // The one byte single-quoting cannot carry literally. Closing, escaping,
        // and reopening is what keeps the rest of the path inside the quotes.
        assert_eq!(super::shell_quote("/tmp/it's here"), r"'/tmp/it'\''s here'");
        assert_eq!(super::shell_quote("/tmp/plain"), "'/tmp/plain'");
    }

    #[tokio::test]
    async fn skill_approvals_clear_with_gate_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = super::skill_approvals(dir.path().to_path_buf(), vec!["X".into()], true)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[tokio::test]
    async fn skill_approvals_comma_in_gate_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        // A comma cannot round-trip through the CSV env encoding.
        let err = super::skill_approvals(dir.path().to_path_buf(), vec!["a,b".into()], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[tokio::test]
    async fn skill_approvals_whitespace_gate_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = super::skill_approvals(dir.path().to_path_buf(), vec!["  ".into()], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    // --- the set path must not be more credulous than the view path ----------
    // Both emit an answer ABOUT a specific bundle. The view path errors when the
    // bundle has no manifest; the set path emitted export-then-reboot guidance
    // naming a directory it had never opened, so `--plugin-dir /does/not/exist`
    // exited 0 and the guidance failed later at `skill up`.

    #[tokio::test]
    async fn skill_approvals_set_without_manifest_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = super::skill_approvals(dir.path().to_path_buf(), vec!["A".into()], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[tokio::test]
    async fn skill_approvals_clear_without_manifest_is_usage_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = super::skill_approvals(dir.path().to_path_buf(), vec![], true)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    #[tokio::test]
    async fn skill_approvals_set_with_valid_manifest_and_no_policy_succeeds() {
        use crate::ui::CliOutput;
        // Regression guard on the two-tier semantics: a manifest that parses but
        // declares no `approvalPolicy` is the legitimate no-gates case, not an
        // invalid bundle. Setting an env override for it must still work -- the
        // validation may only reject a missing, unreadable, or invalid manifest.
        let dir = tempfile::tempdir().unwrap();
        write_minimal_manifest(dir.path());
        let out = super::skill_approvals(dir.path().to_path_buf(), vec!["A".into()], false)
            .await
            .unwrap();
        assert_eq!(
            out.to_json()["env"].as_str().unwrap(),
            "CURIE_APPROVAL_REQUIRED_TOOLS=A"
        );
    }

    #[tokio::test]
    async fn skill_approvals_set_with_invalid_manifest_is_usage_error() {
        // The view path rejects a manifest the runner's parse would reject; the
        // set path names the same bundle, so it must reject it identically.
        let dir = tempfile::tempdir().unwrap();
        write_manifest(
            dir.path(),
            ".claude-plugin/plugin.json",
            r#"{"name":"x",,}"#,
        );
        let err = super::skill_approvals(dir.path().to_path_buf(), vec!["A".into()], false)
            .await
            .unwrap_err();
        assert_eq!(usage_class(&err), crate::exit::ExitClass::Usage);
    }

    /// AC2: an unavailable verb must name the concept's absence AND point at the
    /// tier that answers it. `main`'s human path renders `{err:#}` and discards
    /// the fix, so both halves have to survive on the Display surface alone.
    #[test]
    fn skill_versions_unavailable_message_names_reason_and_alternative() {
        let shown = format!("{:#}", super::skill_versions_unavailable());
        assert!(
            shown.contains(super::VERSIONS_REASON),
            "the human message must carry the reason: {shown}"
        );
        assert!(
            shown.contains(super::VERSIONS_ALT),
            "the human message must carry the cross-tier redirect: {shown}"
        );
    }

    #[test]
    fn skill_memory_unavailable_message_names_reason_and_alternative() {
        let shown = format!("{:#}", super::skill_memory_unavailable());
        assert!(
            shown.contains(super::MEMORY_REASON),
            "the human message must carry the reason: {shown}"
        );
        assert!(
            shown.contains(super::MEMORY_ALT),
            "the human message must carry the cross-tier redirect: {shown}"
        );
    }

    #[test]
    fn skill_versions_unavailable_is_unsupported() {
        let err = super::skill_versions_unavailable();
        assert_eq!(
            crate::exit::classify(&err).0,
            crate::exit::ExitClass::Unsupported
        );
        let json = crate::exit::error_json(&err);
        assert!(
            json["error"].as_str().unwrap().contains("versions"),
            "error names the concept: {}",
            json["error"]
        );
        let fix = json["fix"].as_str().unwrap();
        assert!(
            fix.contains("cluster") || fix.contains("local"),
            "fix names a cross-tier alternative: {fix}"
        );
    }

    #[test]
    fn skill_memory_unavailable_is_unsupported() {
        let err = super::skill_memory_unavailable();
        assert_eq!(
            crate::exit::classify(&err).0,
            crate::exit::ExitClass::Unsupported
        );
        let json = crate::exit::error_json(&err);
        assert!(
            json["error"].as_str().unwrap().contains("memory"),
            "error names the concept: {}",
            json["error"]
        );
        let fix = json["fix"].as_str().unwrap();
        assert!(
            fix.contains("cluster") || fix.contains("local"),
            "fix names a cross-tier alternative: {fix}"
        );
    }

    // ─── #700: plumbing rows render distinctly from real rows in a sweep ─────

    fn real_row(model: &str, passed: usize, total: usize) -> SweepRow {
        SweepRow {
            model: model.to_string(),
            passed,
            // Every case in these plumbing-focused fixtures actually completed;
            // that axis is #622's concern, not this one's.
            completed: total,
            total,
            plumbing: 0,
        }
    }

    fn plumbing_only_row(model: &str, plumbing: usize) -> SweepRow {
        SweepRow {
            model: model.to_string(),
            passed: 0,
            completed: 0,
            total: 0,
            plumbing,
        }
    }

    #[test]
    fn plumbing_only_row_is_detected_and_a_real_row_is_not() {
        assert!(plumbing_only_row("fake", 3).is_plumbing_only());
        assert!(!real_row("opus", 3, 3).is_plumbing_only());
        // A real row that never ran any case (no cases assigned this model, no
        // plumbing either) is 0/0 but NOT plumbing-only -- there is nothing to
        // distinguish it from, so it must not get the plumbing marker.
        assert!(!real_row("idle", 0, 0).is_plumbing_only());
    }

    #[test]
    fn sweep_json_row_carries_the_plumbing_count_and_a_filterable_boolean() {
        let real = sweep_json_row(&real_row("opus", 2, 3));
        assert_eq!(real["model"], "opus");
        assert_eq!(real["passed"], 2);
        assert_eq!(real["total"], 3);
        assert_eq!(real["plumbing"], 0);
        assert_eq!(real["plumbing_only"], false);

        let plumbing = sweep_json_row(&plumbing_only_row("fake", 5));
        assert_eq!(plumbing["model"], "fake");
        assert_eq!(plumbing["passed"], 0);
        assert_eq!(plumbing["total"], 0);
        assert_eq!(plumbing["plumbing"], 5);
        assert_eq!(
            plumbing["plumbing_only"], true,
            "a scripted consumer filters on this flag to drop fixture rows: {plumbing}"
        );
    }

    #[test]
    fn sweep_table_row_marks_a_plumbing_only_row_distinctly_from_a_real_one() {
        let real = sweep_table_row(&real_row("opus", 3, 3));
        assert_eq!(real[0], "opus");
        assert_eq!(real[1], "3/3");
        assert_eq!(real[2], "100%");
        assert_eq!(real[3], "-");

        let plumbing = sweep_table_row(&plumbing_only_row("fake", 4));
        assert_eq!(
            plumbing[0], "fake (plumbing)",
            "the model name must be marked so it cannot be skimmed as a real row"
        );
        assert_eq!(plumbing[1], "0/0");
        assert_eq!(
            plumbing[2], "n/a",
            "a plumbing-only row must not read as a real 0% failure"
        );
        assert_eq!(plumbing[3], "4");
    }

    #[test]
    fn sweep_table_rows_for_a_mixed_sweep_stay_distinguishable_side_by_side() {
        // A sweep containing both a plumbing row and a real row (#700 AC): the two
        // must render differently enough that scanning the table cannot mistake
        // one for the other.
        let rows = [real_row("opus", 2, 3), plumbing_only_row("fake-model", 3)];
        let table: Vec<Vec<String>> = rows.iter().map(sweep_table_row).collect();
        assert_eq!(table[0], vec!["opus", "2/3", "67%", "-"]);
        assert_eq!(table[1], vec!["fake-model (plumbing)", "0/0", "n/a", "3"]);
        assert_ne!(table[0][2], table[1][2], "pass-rate columns must differ");
    }
}
