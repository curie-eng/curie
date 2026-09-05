//! Command handlers behind the `curie` subcommands.
//!
//! main.rs owns the clap surface; each handler here owns one subcommand's
//! behavior and speaks only through the library modules (docker, runner, api,
//! scaffold, state, evals, render).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::ValueEnum;
use curie_aci_protocol::{Budget, EventType, OutboundEvent, SessionStatus};
use serde::{Deserialize, Serialize};

use crate::api::{ApiClient, BudgetConfig, ChannelOutcome, RoutingCheck};
use crate::bundle::{git_status_is_clean_for_pack, pack_tar_gz};
use crate::docker::{self, CheckSpec, StartSpec};
use crate::evals::{
    graded_answer, load_eval, outcome_label, rollup_line, score_turn, turn_completed, CaseOutcome,
    EvalSuite, TrajectoryScorer,
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
    /// Opt in to downloading the `--local-model` assets this run (ADR 0093).
    /// Without it, `up` refuses when the pinned Ollama image or the requested
    /// model is not already on the machine. From `skill up --pull-model`.
    pub pull_model: bool,
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

/// Run the offline MCP load check for a plugin bundle.
pub async fn check(plugin_dir: PathBuf, image: String, timeout_s: u64) -> Result<()> {
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

/// Scaffold and drive the existing local skill path for one first reply.
pub async fn try_first_run(keep: bool, image: String) -> Result<()> {
    const DEMO_NAME: &str = "curie-demo";
    const DEMO_PROMPT: &str = "hello, are you there?";

    let ui = crate::ui::ui();
    let dir = if keep {
        PathBuf::from(DEMO_NAME)
    } else {
        std::env::temp_dir().join(format!("curie-try-{}", uuid::Uuid::new_v4()))
    };

    if let Err(err) = scaffold(&dir, DEMO_NAME) {
        if !keep && dir.exists() {
            if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
                ui.warn(&format!(
                    "could not remove incomplete demo at {}: {cleanup_err}",
                    dir.display()
                ));
            }
        }
        return Err(err);
    }
    ui.note(&format!("scaffolded demo at {}", dir.display()));

    let mut credential_name = None;
    let mut discovery_error = None;
    for name in MODEL_CREDENTIAL_ENV_NAMES {
        if env_credential_present(name) {
            credential_name = Some(name);
            break;
        }
        match crate::secrets::is_saved(name) {
            Ok(true) => {
                credential_name = Some(name);
                break;
            }
            Ok(false) => {}
            Err(err) => {
                discovery_error = Some(err);
                break;
            }
        }
    }
    if let Some(err) = discovery_error {
        if !keep {
            if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
                ui.warn(&format!(
                    "could not remove temporary demo at {}: {cleanup_err}",
                    dir.display()
                ));
            }
        }
        return Err(err);
    }
    let fake_model = credential_name.is_none();
    if let Some(name) = credential_name {
        ui.note(&format!("using discovered model credential {name}"));
    } else {
        ui.note("no model credential found; using the scripted fake model");
    }

    let started = start(StartOpts {
        plugin_dir: dir.clone(),
        image,
        port: DEFAULT_PORT,
        name: docker::RUNNER_CONTAINER_LOCAL.to_string(),
        fake_model,
        network: None,
        otel_endpoint: None,
        budget: DEFAULT_BUDGET.to_string(),
        model: None,
        local_model: None,
        pull_model: false,
        secret: Vec::new(),
        env_file: None,
        replace: false,
    })
    .await;
    if let Err(err) = started {
        if !keep {
            if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
                ui.warn(&format!(
                    "could not remove temporary demo at {}: {cleanup_err}",
                    dir.display()
                ));
            }
        }
        return Err(err);
    }

    let message = send(
        DEMO_PROMPT,
        crate::message::DEFAULT_USER,
        EventType::Message,
        Some(format!("http://localhost:{DEFAULT_PORT}")),
        true,
    )
    .await;
    let teardown = stop(None, &dir).await;

    let classified_failure = match message {
        Ok(classified_failure) => classified_failure,
        Err(message_err) => {
            if let Err(cleanup_err) = &teardown {
                ui.warn(&format!(
                    "could not tear down the demo at {}: {cleanup_err}",
                    dir.display()
                ));
            } else if !keep {
                if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
                    ui.warn(&format!(
                        "could not remove temporary demo at {}: {cleanup_err}",
                        dir.display()
                    ));
                }
            }
            return Err(message_err);
        }
    };

    if let Err(cleanup_err) = teardown {
        ui.failure(&format!(
            "could not tear down the demo at {}: {cleanup_err}",
            dir.display()
        ));
        ui.note(&format!(
            "recover with: cd {} && curie skill down",
            dir.display()
        ));
        std::process::exit(1);
    }

    if keep {
        let next = if fake_model {
            "cd curie-demo && curie skill up --fake-model"
        } else {
            "cd curie-demo && curie skill up"
        };
        ui.note(&format!("kept ./curie-demo; next: {next}"));
    } else if let Err(cleanup_err) = std::fs::remove_dir_all(&dir) {
        ui.failure(&format!(
            "could not remove temporary demo at {}: {cleanup_err}",
            dir.display()
        ));
        std::process::exit(1);
    } else {
        ui.note(&format!("removed temporary demo at {}", dir.display()));
    }

    if classified_failure {
        std::process::exit(1);
    }
    Ok(())
}

/// Tags `docker build` applies for one platform image.
///
/// The runner has two identities: the short name [`crate::docker::RUNNER_IMAGE`]
/// (`curie skill up`, `curie update --image`) and the ghcr `:dev` ref a
/// `--build` stack runs. Building either also tags the other, so the two
/// paths share one image (#1931). A custom `curie build --tag` is left alone.
pub(crate) fn platform_image_tags(dockerfile: &str, tag: &str) -> Vec<String> {
    let mut tags = vec![tag.to_string()];
    if dockerfile == "runner/Dockerfile" {
        let short = crate::docker::RUNNER_IMAGE;
        let qualified = crate::local::source_image_ref(short);
        if tag == short || tag == qualified {
            if tag != short {
                tags.push(short.to_string());
            }
            if tag != qualified {
                tags.push(qualified);
            }
        }
    }
    tags
}

/// Build one platform image. The single `docker build` invocation for Curie
/// images: `curie build` and `curie local up --build` both route here (#1931).
pub(crate) async fn build_image(dockerfile: &str, tag: &str) -> Result<()> {
    let ui = crate::ui::ui();
    if !on_path("docker") {
        bail!(
            "Docker is not installed or not on PATH. Install Docker \
             (https://docs.docker.com/get-docker/) and retry."
        );
    }
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run this from a \
         curie repo checkout -- a release binary pulls published images and never needs to build.",
    )?;
    let tags = platform_image_tags(dockerfile, tag);
    let mut args = vec![
        "build".to_string(),
        "-f".to_string(),
        dockerfile.to_string(),
    ];
    for image_tag in &tags {
        args.push("-t".to_string());
        args.push(image_tag.clone());
    }
    args.push(".".to_string());
    let rendered = tags
        .iter()
        .map(|image_tag| format!("-t {image_tag}"))
        .collect::<Vec<_>>()
        .join(" ");
    ui.note(&format!(
        "=== docker build -f {dockerfile} {rendered} . (in {}) ===",
        root.display()
    ));
    // Inherit stdio so the build log streams to the terminal like a hand-run build.
    let status = tokio::process::Command::new("docker")
        .args(&args)
        .current_dir(&root)
        .status()
        .await
        .context("failed to invoke docker")?;
    if !status.success() {
        bail!("docker build failed for {dockerfile} ({status})");
    }
    Ok(())
}

/// `curie build`: build the runner image locally from the repo's Dockerfile.
/// The one-command equivalent of `docker build -f runner/Dockerfile -t <tag> .`
/// run from the repo root. Errors clearly when Docker is missing or when run
/// outside a source checkout (a release binary pulls the image from GHCR).
///
/// When `tag` is the default short name, the same image is also tagged
/// [`crate::local::source_image_ref`] so a `--build` stack sees it.
pub async fn build(tag: &str) -> Result<()> {
    let ui = crate::ui::ui();
    build_image("runner/Dockerfile", tag).await?;
    ui.success(&format!("built runner image '{tag}'"));
    Ok(())
}

#[cfg(test)]
mod platform_image_tags_tests {
    use super::platform_image_tags;
    use crate::docker::RUNNER_IMAGE;
    use crate::local::source_image_ref;

    #[test]
    fn runner_short_name_also_tags_the_build_stack_ref() {
        let tags = platform_image_tags("runner/Dockerfile", RUNNER_IMAGE);
        assert_eq!(
            tags,
            vec![RUNNER_IMAGE.to_string(), source_image_ref(RUNNER_IMAGE),]
        );
    }

    #[test]
    fn runner_build_stack_ref_also_tags_the_short_name() {
        let qualified = source_image_ref(RUNNER_IMAGE);
        let tags = platform_image_tags("runner/Dockerfile", &qualified);
        assert_eq!(tags, vec![qualified, RUNNER_IMAGE.to_string()]);
    }

    #[test]
    fn custom_runner_tag_is_left_alone() {
        let tags = platform_image_tags("runner/Dockerfile", "my-runner");
        assert_eq!(tags, vec!["my-runner".to_string()]);
    }

    #[test]
    fn non_runner_image_keeps_its_single_tag() {
        let tag = source_image_ref("curie-api");
        let tags = platform_image_tags("apps/api/Dockerfile", &tag);
        assert_eq!(tags, vec![tag]);
    }
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
/// -- finds the repo root, confirms the script exists, shells `bash <script> [args]`
/// from the root, streams its output, and propagates its exit code. A release
/// binary has no scripts, so this errors clearly outside a checkout.
pub async fn dev_script(rel_path: &str, args: &[&str]) -> Result<()> {
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
        .args(args)
        .current_dir(&root)
        .status()
        .await
        .context("failed to invoke bash")?;
    if !status.success() {
        bail!("{rel_path} failed ({status})");
    }
    Ok(())
}

pub async fn dev_e2e_ci_selection(
    paths: &[PathBuf],
    base: Option<&str>,
    head: Option<&str>,
    push: bool,
) -> Result<()> {
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie dev` \
         from a curie source checkout.",
    )?;
    let selector = root.join("tools/e2e-ci-selection/select_tiers.py");
    let registry = root.join(".github/e2e-selection.yaml");
    if !selector.is_file() {
        bail!("selector not found: {}", selector.display());
    }
    if !registry.is_file() {
        bail!("selection registry not found: {}", registry.display());
    }

    let output_path = (0..100)
        .find_map(|attempt| {
            let path = std::env::temp_dir().join(format!(
                "curie-e2e-ci-selection-{}-{attempt}",
                std::process::id()
            ));
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .ok()
                .map(|_| path)
        })
        .context("failed to create temporary selector output")?;

    let selection = async {
        let mut command = tokio::process::Command::new("uv");
        command
            .args([
                "run",
                "--no-project",
                "--with",
                "pyyaml==6.0.3",
                "python",
                "tools/e2e-ci-selection/select_tiers.py",
                "--registry",
                ".github/e2e-selection.yaml",
            ])
            .env("GITHUB_OUTPUT", &output_path)
            .current_dir(&root);
        for path in paths {
            command.arg("--path").arg(path);
        }
        if let Some(base) = base {
            command.arg("--base").arg(base);
        }
        if let Some(head) = head {
            command.arg("--head").arg(head);
        }
        if push {
            command.arg("--push");
        }

        let status = command
            .status()
            .await
            .context("failed to invoke the end to end CI selector")?;
        if !status.success() {
            bail!("end to end CI selector failed ({status})");
        }
        std::fs::read_to_string(&output_path).context("failed to read selector output")
    }
    .await;

    let cleanup = std::fs::remove_file(&output_path)
        .with_context(|| format!("failed to remove {}", output_path.display()));
    let selection = selection?;
    cleanup?;
    print!("{selection}");
    Ok(())
}

/// The chart assertion scripts live here, and helm-ci runs every one of them on
/// any `charts/curie/**` change.
pub const CHART_CI_DIR: &str = "charts/curie/ci";

/// One chart assertion script and how it fared.
#[derive(Serialize)]
pub struct ChartCheckOutcome {
    /// The script's file name, e.g. `render-assertions.sh`.
    pub name: String,
    pub passed: bool,
}

/// The result of a successful `curie dev chart-check` run.
#[derive(Serialize)]
pub struct ChartCheckOutput {
    pub passed: usize,
    pub total: usize,
    pub scripts: Vec<ChartCheckOutcome>,
}

impl crate::ui::CliOutput for ChartCheckOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!({}))
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.success(&format!(
            "all {} chart assertion scripts passed",
            self.total
        ));
    }
}

/// Discover the assertion scripts `curie dev chart-check` runs: every executable
/// `*.sh` in `dir`, sorted by name so the run order is stable.
///
/// Discovery is a directory listing rather than a hardcoded list, so a script
/// added to `charts/curie/ci/` is picked up with no edit to `cli/` (#1481). That
/// matters because helm-ci runs the whole directory and is release-blocking, so
/// a verb that knows about only some of it reports a local green CI will refuse.
pub fn discover_chart_check_scripts(dir: &Path) -> Result<Vec<PathBuf>> {
    let entries = std::fs::read_dir(dir)
        .with_context(|| format!("failed to read chart assertion directory {}", dir.display()))?;
    let mut scripts: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let path = entry
            .with_context(|| format!("failed to read an entry in {}", dir.display()))?
            .path();
        if path.is_file() && path.extension().is_some_and(|ext| ext == "sh") && is_executable(&path)
        {
            scripts.push(path);
        }
    }
    scripts.sort();
    Ok(scripts)
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(_path: &Path) -> bool {
    true
}

/// Run every discovered script from `root`, streaming its stdout and stderr to
/// this process's stderr, and report how each one fared.
///
/// A failure does not stop the run: the point of the verb is that one invocation
/// surfaces every problem, rather than making a contributor fix, re-run, and
/// discover the next failure one at a time.
pub async fn run_chart_check_scripts(
    root: &Path,
    scripts: &[PathBuf],
) -> Result<Vec<ChartCheckOutcome>> {
    let ui = crate::ui::ui();
    let mut outcomes = Vec::with_capacity(scripts.len());
    for (index, script) in scripts.iter().enumerate() {
        let name = script
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let rel = script.strip_prefix(root).unwrap_or(script);
        ui.note(&format!(
            "=== [{}/{}] bash {} ===",
            index + 1,
            scripts.len(),
            rel.display()
        ));
        let status = tokio::process::Command::new("bash")
            .arg(script)
            .current_dir(root)
            .stdout(std::io::stderr())
            .stderr(std::process::Stdio::inherit())
            .status()
            .await
            .with_context(|| format!("failed to invoke bash for {name}"))?;
        outcomes.push(ChartCheckOutcome {
            name,
            passed: status.success(),
        });
    }
    Ok(outcomes)
}

/// `curie dev chart-check`: run the chart assertion suite helm-ci runs.
///
/// helm-ci executes every script in `charts/curie/ci/` on any `charts/curie/**`
/// change and is release-blocking (#1466), so this verb covers the same set. It
/// reports per-script pass or fail, runs them all before deciding, and exits
/// non-zero if any failed. A release binary has no checkout, so this errors
/// clearly outside one, same as `dev_script`.
pub async fn dev_chart_check() -> Result<()> {
    let ui = crate::ui::ui();
    let root = find_repo_root().context(
        "runner/Dockerfile not found here or in any parent directory. Run `curie dev` \
         from a curie source checkout -- a release binary has no dev scripts.",
    )?;
    let ci_dir = root.join(CHART_CI_DIR);
    let scripts = discover_chart_check_scripts(&ci_dir)?;
    if scripts.is_empty() {
        bail!(
            "no executable *.sh assertion scripts found in {}",
            ci_dir.display()
        );
    }

    ui.note(&format!(
        "=== {} chart assertion scripts from {CHART_CI_DIR} (in {}) ===",
        scripts.len(),
        root.display()
    ));
    let outcomes = run_chart_check_scripts(&root, &scripts).await?;

    ui.note("=== chart-check summary ===");
    for outcome in &outcomes {
        let mark = if outcome.passed { "PASS" } else { "FAIL" };
        ui.note(&format!("{mark}  {}", outcome.name));
    }

    let failed: Vec<&str> = outcomes
        .iter()
        .filter(|o| !o.passed)
        .map(|o| o.name.as_str())
        .collect();
    if !failed.is_empty() {
        bail!(
            "{} of {} chart assertion scripts failed: {}",
            failed.len(),
            outcomes.len(),
            failed.join(", ")
        );
    }
    let passed = outcomes.iter().filter(|outcome| outcome.passed).count();
    let total = outcomes.len();
    ui.emit(&ChartCheckOutput {
        passed,
        total,
        scripts: outcomes,
    });
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
    let api_url = opts.api_url.clone();
    let result = deploy(DeployOpts {
        plugin_dir,
        // deploy_named is the multi-agent-folder path: the folder IS the agent
        // identity there, so there is nothing to override.
        agent: None,
        target: None,
        api_url,
        api_key: opts.api_key,
        slack_channel: opts.slack_channel,
        repo: opts.repo,
        workspace: WorkspaceIntent::Preserve,
        tier: DeployTier::Local,
        env: Some(opts.env),
        label: opts.label,
        secret: opts.secret,
        secret_binding_supported: true,
        connect_hint: "the platform API is unreachable.".to_string(),
    })
    .await;
    crate::local::with_deploy_unreachable_hint(result, &opts.api_url).await
}

/// The `curie deploy-local <folder>` flags, mirroring `local deploy`'s minus
/// `plugin_dir` (resolved from `folder` instead).
pub struct DeployNamedOpts {
    pub api_url: String,
    pub api_key: String,
    pub slack_channel: Option<String>,
    /// `owner/name` binding the repository whose pushes deploy this agent
    /// (ADR-0014). Bound when the agent is created, or on a later deploy if the
    /// agent has no binding yet (#1194). An agent already bound to a DIFFERENT
    /// repository is left alone and warned about, because a deploy does not
    /// reroute an existing binding.
    pub repo: Option<String>,
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
///
/// `pub(crate)` for `build_image` and `local::build_source_images` (#1915),
/// which build the dev stack's images from the same root and want the same
/// "are we in a checkout" answer rather than a second walk with its own
/// anchor file.
pub(crate) fn find_repo_root() -> Option<PathBuf> {
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
fn select_passthrough_env(
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

/// What `skill up` will run the model as, for one panel row and one warning.
///
/// The gap this closes: `select_passthrough_env` above resolves the model
/// credential AT BOOT, and `skill up` then said nothing about it. A first-run
/// user who had not exported anything got a clean boot panel, ran the very
/// command that panel recommends, and only then hit
/// `model-credential-rejected` from the provider -- one command after the CLI
/// already knew. The README does say to export `CURIE_CREDENTIALS` first, but
/// nothing in `init`'s `Next:` hint or this panel repeats it, so following the
/// CLI rather than the README walks straight into it.
///
/// Names only, never values: the masking rule in cli/CLAUDE.md applies here as
/// everywhere, and a name is all that is diagnostic anyway.
fn model_credential_summary(
    fake_model: bool,
    local_model: Option<&str>,
    names: &[String],
) -> (String, Option<String>) {
    // Local model first: `--local-model` overrides `--fake-model` at the call
    // site, so checking fake first would mislabel a run that set both.
    if let Some(model) = local_model {
        return (format!("local ollama ({model})"), None);
    }
    if fake_model {
        return ("fake (offline, scripted replies)".to_string(), None);
    }
    if !names.is_empty() {
        return (names.join(" + "), None);
    }
    (
        "none".to_string(),
        Some(
            "no model credential resolved, so `curie skill message` will fail with \
             model-credential-rejected. Either export one (CURIE_CREDENTIALS, \
             ANTHROPIC_API_KEY, or CLAUDE_CODE_OAUTH_TOKEN) and re-run `curie skill up \
             --replace`, or re-run with `--fake-model` to drive the loop offline."
                .to_string(),
        ),
    )
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

pub(crate) fn secret_store_env(name: &str) -> Result<Option<(String, String)>> {
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

pub(crate) fn load_model_credentials_from_secret_store() -> Result<Vec<(String, String)>> {
    // Prefer an explicitly BYO Curie credential when saved, otherwise hydrate
    // the SDK credential names in the same order `select_passthrough_env` uses.
    if env_credential_present("CURIE_CREDENTIALS") {
        return Ok(Vec::new());
    }
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
pub const MODEL_CREDENTIAL_ENV_NAMES: [&str; 3] = [
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

/// What a recorded runner means for a fresh `skill up` (#747, #1905).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordedStatePlan {
    /// Nothing recorded; boot.
    Proceed,
    /// Tear the recorded runner down and boot. Either `--replace` names that
    /// exact container, or a verified same-bundle identity is serving a stale
    /// snapshot of this source.
    ClearAndProceed,
    /// The recorded runner is this bundle and already serves the current
    /// source; do not restart.
    AlreadyRunning,
    /// A runner is recorded that this `up` must not remove: a different name,
    /// a foreign or unverifiable identity, or a snapshot that cannot be
    /// compared. Refuse so a second bundle's live runner cannot be silently
    /// forgotten.
    Refuse,
}

/// Inputs to [`plan_recorded_state`]. Identity fields are optional so the
/// `--replace` name gate stays testable without Docker; auto-reload requires
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordedStateQuery<'a> {
    pub recorded_name: Option<&'a str>,
    pub target_name: &'a str,
    pub replace: bool,
    pub recorded_id: Option<&'a str>,
    pub live_id: Option<&'a str>,
    pub same_bundle_dir: bool,
    pub recorded_digest: Option<&'a str>,
    pub current_digest: Option<&'a str>,
}

/// Whether a recorded container id and a `docker ps` id name the same
/// container. `docker ps` reports a 12-char short id while `docker run`
/// returns the full 64-char one, so identity is a prefix match, not equality.
/// An empty recorded id cannot be compared and is not a match: auto-reload
/// must not treat an unverified identity as owned (#1905 / #747).
pub fn recorded_ids_match(recorded_id: &str, live_id: &str) -> bool {
    !recorded_id.is_empty()
        && (recorded_id.starts_with(live_id) || live_id.starts_with(recorded_id))
}

/// Resolve the recorded-state gate. Pure so every branch is testable without a
/// bundle on disk or a Docker daemon.
///
/// `--replace` still clears only the record for the exact target name: a record
/// naming a DIFFERENT runner still blocks, since removing one container is no
/// reason to forget another (#747). Without `--replace`, a verified
/// same-directory runner whose snapshot no longer matches this source is
/// replaced automatically (#1905); an unverified identity never is.
pub fn plan_recorded_state(q: RecordedStateQuery<'_>) -> RecordedStatePlan {
    let Some(recorded) = q.recorded_name else {
        return RecordedStatePlan::Proceed;
    };
    if q.replace && recorded == q.target_name {
        return RecordedStatePlan::ClearAndProceed;
    }
    if recorded != q.target_name {
        return RecordedStatePlan::Refuse;
    }
    let identity_verified = q.same_bundle_dir
        && match (q.recorded_id, q.live_id) {
            (Some(recorded_id), Some(live_id)) => recorded_ids_match(recorded_id, live_id),
            _ => false,
        };
    if !identity_verified {
        return RecordedStatePlan::Refuse;
    }
    match (q.recorded_digest, q.current_digest) {
        (Some(recorded_digest), Some(current_digest)) if recorded_digest == current_digest => {
            RecordedStatePlan::AlreadyRunning
        }
        (Some(_), Some(_)) => RecordedStatePlan::ClearAndProceed,
        _ => RecordedStatePlan::Refuse,
    }
}

/// The leading characters of a bundle digest, for a one-line summary (#1087).
/// Taken by character rather than by byte slice so an unexpectedly short digest
/// truncates instead of panicking.
fn short_digest(digest: &str) -> String {
    digest.chars().take(12).collect()
}

/// Release everything a boot that has already packed its snapshot leaves behind
/// when it aborts: the ollama sidecar, the hosted connectors, the network
/// `start` owns, the credentials it staged, and the snapshot itself.
///
/// One definition for all three abort arms between the pack and a recorded
/// state, because nothing else will ever collect these: no state was saved, so
/// no `skill down` can find them (#1087). A fourth abort path added later gets
/// the whole sequence by calling this rather than by remembering three lines.
///
/// Order is load bearing: the connectors are attached to the owned network, and
/// `docker network rm` fails while any container is still on it, so every
/// connector goes before the network does (ADR 0113).
///
/// Every release is best effort and deliberately swallows its error: these run
/// while a command is already failing for its own reason, and must not change
/// the error it reports or the code it exits with.
async fn release_boot_scaffolding(
    ollama_container: Option<&String>,
    connector_containers: &[String],
    owned_network: Option<&String>,
    snapshot_dir: &Path,
    plugin_dir: &Path,
) {
    if let Some(ollama) = ollama_container {
        let _ = docker::remove_container(ollama).await;
    }
    for connector in connector_containers {
        let _ = docker::remove_container(connector).await;
    }
    if let Some(net) = owned_network {
        let _ = docker::remove_network(net).await;
    }
    // After the connectors, never before: a tree still bind-mounted into a
    // running container cannot be wiped cleanly, the same ordering
    // `connector_teardown_plan` holds. Resolved credentials must not outlive a
    // boot that no `skill down` can find.
    let _ = crate::connector_build::wipe_connector_secrets(plugin_dir);
    let _ = crate::bundle::remove_snapshot(snapshot_dir, plugin_dir);
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
    // `--replace` decides from the name alone and must stay a cheap abort
    // until budget / model preflights pass (#747). Auto-reload needs the live
    // container id, so only probe Docker when that path could fire.
    let live_for_plan = match recorded_runner.as_ref() {
        Some(saved) if !opts.replace => docker::container_facts(&saved.container_name).await?,
        _ => None,
    };
    let same_bundle_dir = recorded_runner.as_ref().is_some_and(|saved| {
        Path::new(&saved.plugin_dir)
            .canonicalize()
            .ok()
            .is_some_and(|recorded| recorded == plugin_dir)
    });
    let identity_verified = recorded_runner.as_ref().is_some_and(|saved| {
        same_bundle_dir
            && live_for_plan
                .as_ref()
                .is_some_and(|facts| recorded_ids_match(&saved.container_id, &facts.id))
    });
    // Packing is cheap relative to teardown and is how we know the snapshot is
    // stale. Skip it unless auto-reload could actually fire: `--replace` does
    // not need a digest, and an unverified identity must still refuse.
    let current_digest = if identity_verified && !opts.replace {
        crate::bundle::digest_source(&plugin_dir).ok()
    } else {
        None
    };
    let recorded_plan = plan_recorded_state(RecordedStateQuery {
        recorded_name: recorded_runner.as_ref().map(|s| s.container_name.as_str()),
        target_name: &opts.name,
        replace: opts.replace,
        recorded_id: recorded_runner.as_ref().map(|s| s.container_id.as_str()),
        live_id: live_for_plan.as_ref().map(|f| f.id.as_str()),
        same_bundle_dir,
        recorded_digest: recorded_runner
            .as_ref()
            .and_then(|s| s.bundle_digest.as_deref()),
        current_digest: current_digest.as_deref(),
    });
    if recorded_plan == RecordedStatePlan::Refuse {
        let recorded_name = &recorded_runner
            .as_ref()
            .expect("a refusal requires a recorded runner")
            .container_name;
        return Err(crate::exit::usage(format!(
            "a local runner is already recorded in {}/.curie/runner.json; run 'curie skill down' there first, or rerun 'curie skill up --replace --name {recorded_name}' to replace it",
            plugin_dir.display(),
        )));
    }
    if recorded_plan == RecordedStatePlan::AlreadyRunning {
        let saved = recorded_runner.expect("already-running requires a recorded runner");
        let ui = crate::ui::ui();
        ui.success(&format!(
            "runner '{}' is already running this bundle snapshot",
            saved.container_name
        ));
        if let Some(digest) = saved.bundle_digest.as_deref() {
            ui.note(&format!("bundle {}", short_digest(digest)));
        }
        return Ok(());
    }

    // Parse (not just forward) the budget so a typo fails here, not in-container.
    let _: Budget = serde_json::from_str(&opts.budget).map_err(|e| {
        crate::exit::usage(format!(
            "--budget is not a valid ACI budget: {}: {e}",
            opts.budget
        ))
    })?;

    // The sidecar's container name, derived once here so the preflight probe
    // below and the sidecar setup after the teardown derive their volume from
    // one name and cannot drift.
    let ollama = format!("{}-ollama", opts.name);

    // ADR 0093: preflight --local-model before ANY teardown below, since
    // refusing is free but replacing tears down a live runner (the #747
    // invariant above). This is the same refusal `local up` gives, so both
    // tiers answer `--local-model` identically (ADR 0041). The skill tier's
    // model cache is the sidecar's own volume, not compose's -- and
    // stop_recorded explicitly keeps that volume, so this preflight returns
    // the identical answer whether it runs here or after the teardown;
    // moving it earlier is purely about ordering, not about its verdict.
    if let Some(local_model) = &opts.local_model {
        if !opts.pull_model {
            docker::preflight_local_model(
                DEFAULT_OLLAMA_IMAGE,
                &docker::ollama_volume(&ollama),
                local_model,
                &format!(
                    "curie skill up --local-model {local_model} --pull-model --name {}",
                    opts.name
                ),
            )
            .await?;
        }
    }

    // The replacement itself, once every cheap validation has passed. Tearing
    // down the record means EVERYTHING it describes -- container, ollama sidecar,
    // network, then the file -- because clearing the record while its runner is
    // still live strands exactly the untracked orphan this ticket removes (#747).
    if let (RecordedStatePlan::ClearAndProceed, Some(saved)) = (recorded_plan, recorded_runner) {
        let reason = if opts.replace {
            "--replace: tearing down the recorded runner"
        } else {
            "bundle changed: replacing the recorded runner"
        };
        crate::ui::ui().note(&format!("{reason} '{}' first", saved.container_name));
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
        // `<name>-ollama` is the same wedge one step over (#747). Catch it
        // before creating anything, and let --replace cover it too. (The
        // --local-model preflight itself, when --pull-model is absent, already
        // ran above the teardown against this same binding.)
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
    // `model_cred_names` is kept separately from the merged list so the summary
    // below reports the MODEL credential alone -- a `--secret` name riding in
    // `passthrough_env` is not one, and counting it would let the panel claim a
    // credential for a runner that has none.
    let (model_cred_names, mut passthrough_env) = {
        let ambient_present = ambient_present_for(&docker_env);
        let model_cred_names = select_passthrough_env(
            fake_model,
            base_url_override,
            byo_credential.as_deref(),
            &ambient_present,
        );
        let passthrough = merge_secret_env(model_cred_names.clone(), &opts.secret);
        (model_cred_names, passthrough)
    };

    // Hosted-connector preflights (ADR 0113) run BEFORE the snapshot is packed:
    // the runner validates the packed snapshot, so a lock rewritten after
    // packing would leave the runner refusing the stale copy it was handed
    // while the source directory holds the fresh one.
    let connector_decl = crate::connector_build::load(&plugin_dir)?;
    let declares_hosted_connectors = connector_decl
        .connectors
        .values()
        .any(|spec| spec.url.is_none() && spec.unhosted_url.is_none());
    if declares_hosted_connectors {
        // Fail closed on a missing credential BEFORE anything is created. It
        // runs ahead of the rebuild below because there is no point spending
        // minutes building images for a bring-up that will refuse, and ahead of
        // the network and every container because a partial bring-up that then
        // aborts leaks work this checks for up front instead.
        if let Err(err) = refuse_missing_connector_secrets(&connector_decl) {
            if let Some(ollama) = &ollama_container {
                let _ = docker::remove_container(ollama).await;
            }
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(err);
        }
        // ADR 0113's Decision 3, and its deliberate asymmetry: at the skill tier
        // a missing or stale `connectors.lock.yaml` is REBUILT here, where the
        // source and a Docker daemon are both within reach; at the cluster tier
        // `lock_preflight` refuses instead. Without this, an edit to a connector's
        // source silently brings up the previously locked image. A fresh lock
        // computes no rebuild and issues no `docker build`, so the offline
        // guarantee `cli/CLAUDE.md` makes for `skill up` still holds. It runs
        // before the snapshot pack so the runner's copy carries the lock this
        // writes, and before `start_skill_connectors`, which re-reads the lock
        // from disk and so picks up whatever this wrote.
        let stale = connectors_needing_rebuild(
            &connector_decl,
            crate::connector_build::load_lock(&plugin_dir)?.as_ref(),
            &recompute_source_digests(&plugin_dir, &connector_decl)?,
        );
        if !stale.is_empty() {
            // Not forced: a lock resolved to a pushed registry image is the
            // operator's, and `write_lock` refuses to quietly downgrade it to a
            // local-daemon one behind a `skill up`.
            if let Err(err) = build_connectors(ConnectorBuildOpts {
                plugin_dir: plugin_dir.clone(),
                registry: None,
                force: false,
            })
            .await
            {
                // A build is minutes long and fails for ordinary reasons (a bad
                // Dockerfile, no daemon), so it releases what boot has created so
                // far rather than leaving an ollama sidecar to collide with the
                // operator's next attempt. No snapshot or connector container
                // exists yet.
                if let Some(ollama) = &ollama_container {
                    let _ = docker::remove_container(ollama).await;
                }
                if let Some(net) = &owned_network {
                    let _ = docker::remove_network(net).await;
                }
                return Err(err.context("rebuilding the bundle's connectors"));
            }
        }
    }

    // #1087: the skill tier executes an immutable, content-addressed snapshot,
    // not the editable source -- matching what local and cluster already do. The
    // packer is the deploy path's packer, so the digest is the same one the API
    // records for this source. Placed AFTER the --replace teardown above so a
    // re-up of unchanged source (same digest, same directory) cannot have the
    // snapshot it just created torn down again, and after the connector
    // rebuild above so the packed bundle carries the lock the runner validates.
    let snapshot = match crate::bundle::snapshot(&plugin_dir) {
        Ok(snapshot) => snapshot,
        Err(err) => {
            // Nothing of the runner exists yet, but the ollama sidecar above may:
            // release it the same way every other abort on this path does.
            if let Some(ollama) = &ollama_container {
                let _ = docker::remove_container(ollama).await;
            }
            if let Some(net) = &owned_network {
                let _ = docker::remove_network(net).await;
            }
            return Err(err.context("packaging the bundle snapshot for the runner"));
        }
    };

    // Hosted connectors (ADR 0113). A bundle that declares none takes the
    // existing hermetic path untouched; one that declares some gets a private
    // network the runner and the connectors share, and the connector scope in
    // the runner's boot env so `derive_mcp_servers` takes its normal path.
    let mut connector_containers: Vec<String> = Vec::new();
    let mut connector_network: Option<String> = None;
    if declares_hosted_connectors {
        let net = match &network {
            Some(net) => net.clone(),
            None => {
                let net = format!("{}-net", opts.name);
                if docker::create_network(&net).await? {
                    owned_network = Some(net.clone());
                }
                net
            }
        };
        let identity = crate::connector_build::ConnectorScope {
            release: "curie".to_string(),
            agent: plugin_name.clone(),
            namespace: "default".to_string(),
        };
        match start_skill_connectors(&plugin_dir, &connector_decl, &identity, &net, &session_id)
            .await
        {
            Ok(started) => connector_containers = started,
            Err(err) => {
                // Nothing is recorded yet, so this is the only chance to release
                // whatever the partial bring-up created (#747).
                let steps = docker::connector_teardown_plan(
                    &session_id,
                    owned_network.as_deref(),
                    Some(&plugin_dir),
                );
                let _ = docker::run_connector_teardown(&steps).await;
                if let Some(ollama) = &ollama_container {
                    let _ = docker::remove_container(ollama).await;
                }
                return Err(err.context("starting the bundle's connectors"));
            }
        }
        for (key, value) in crate::connector_build::connector_scope_env(&identity) {
            passthrough_env.push(key.clone());
            docker_env.push((key, value));
        }
        network = Some(net.clone());
        connector_network = Some(net);
    }

    let spec = StartSpec {
        image: opts.image.clone(),
        container_name: opts.name.clone(),
        host_port: opts.port,
        plugin_dir: snapshot.dir.clone(),
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
            // Nothing recorded the snapshot, so no teardown will ever find it
            // (#1087); release it here alongside the sidecar, the connectors,
            // and the network.
            release_boot_scaffolding(
                ollama_container.as_ref(),
                &connector_containers,
                owned_network.as_ref(),
                &snapshot.dir,
                &plugin_dir,
            )
            .await;
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
        // Same as the start-failure arm above: unrecorded, so this is the only
        // chance to release it (#1087).
        release_boot_scaffolding(
            ollama_container.as_ref(),
            &connector_containers,
            owned_network.as_ref(),
            &snapshot.dir,
            &plugin_dir,
        )
        .await;
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
            bundle_digest: Some(snapshot.digest.clone()),
            bundle_snapshot_dir: Some(snapshot.dir.display().to_string()),
            connector_containers: connector_containers.clone(),
            connector_network: connector_network.clone(),
        },
    ) {
        let _ = docker::remove_container(&opts.name).await;
        // The last of the three abort paths between the pack and a recorded
        // state; each releases the snapshot itself, so a failed boot leaves
        // nothing behind (#1087).
        release_boot_scaffolding(
            ollama_container.as_ref(),
            &connector_containers,
            owned_network.as_ref(),
            &snapshot.dir,
            &plugin_dir,
        )
        .await;
        return Err(err.context("recording runner state (container removed again)"));
    }

    let version = git_short_sha(&plugin_dir)
        .await
        .map(|sha| format!("dev @ {sha}"))
        .unwrap_or_else(|| format!("{plugin_name} @ {manifest_version}"));
    // `fake_model`, not `opts.fake_model`: a `--local-model` overrides a
    // `--fake-model` (line ~1432), and that resolved value is the one that
    // actually drove the credential selection being reported.
    let (model_credential, model_warning) =
        model_credential_summary(fake_model, opts.local_model.as_deref(), &model_cred_names);
    let rows = [
        ("Local bot", base_url),
        (
            "Skill message",
            "curie skill message \"<message>\"".to_string(),
        ),
        ("Skill eval", "curie skill eval".to_string()),
        ("Version", version),
        // What makes AC1/AC3 confirmable by eye: re-up after a source edit and
        // this visibly changes, while a source edit under a live runner does not.
        (
            "Bundle",
            format!("{} (snapshot)", short_digest(&snapshot.digest)),
        ),
        ("Model", model_credential),
    ];
    ui.payload_plain(&boxed_summary("curie dev environment", &rows));
    if let Some(warning) = model_warning {
        ui.note(&warning);
    }
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
    // id at all cannot be compared, so teardown keeps the old name-based
    // behavior rather than refusing to tear down. Auto-reload does NOT use this
    // empty-id fallback: [`recorded_ids_match`] rejects it (#1905).
    if recorded_id.is_empty() || recorded_ids_match(recorded_id, live) {
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

pub async fn stop(name: Option<String>, dir: &Path) -> Result<()> {
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
    // Connector containers, then the staged credential tree (ADR 0113,
    // block B1-8). Label-scoped, so a connector this bundle started is reaped
    // even if the record is incomplete; the network is left to the owned-network
    // branch below, which alone knows whether this boot created it.
    let connector_problems = docker::run_connector_teardown(&docker::connector_teardown_plan(
        &saved.session_id,
        None,
        Some(dir),
    ))
    .await;
    for problem in connector_problems {
        ui.warn(&problem);
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
    // Remove the snapshot this record owns (#1087), the same way the ollama
    // sidecar and the network above are released. Guarded and tolerant: a
    // snapshot that will not delete is a warning, not a failed teardown (#323 --
    // the agent consumer needs the teardown to succeed), and a recorded path
    // outside <bundle>/.curie/snapshots/ is refused rather than deleted.
    if let Some(snapshot_dir) = &saved.bundle_snapshot_dir {
        if let Err(err) = crate::bundle::remove_snapshot(Path::new(snapshot_dir), dir) {
            ui.warn(&format!(
                "could not remove bundle snapshot '{snapshot_dir}': {err}"
            ));
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
///
/// `bundle_digest` (#1087) is the sha256 of the snapshot this runner mounted.
/// The key is always emitted -- `null` when no runner is recorded or the record
/// predates #1087 -- so an agent consumer can read it unconditionally.
pub fn status_json<T: serde::Serialize>(
    url: &str,
    status: &T,
    bundle_digest: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({ "url": url, "session": status, "bundle_digest": bundle_digest })
}

/// One eval case's result: `(id, outcome, seconds, output)`. `output` is the
/// graded answer text (the reply `turn_outcome`/`reply_passes` judged), carried
/// so a red case is diagnosable from `--json` without a manual re-run (#548).
/// Shared by the skill runner path and the local/cluster message path so both
/// report the same shape through `report_eval`/`eval_json`.
pub type EvalRow = (String, CaseOutcome, f64, String);

/// Eval rows plus optional scorer explanations keyed by case id.
pub struct EvalReport {
    pub rows: Vec<EvalRow>,
    pub details: BTreeMap<String, String>,
    /// Sampling policy this run used (#1907). Default n=1 majority.
    pub sampling: crate::eval_sampling::SampleConfig,
    /// Per-case count of samples that passed, keyed by case id.
    pub sample_passes: BTreeMap<String, u32>,
}

impl EvalReport {
    pub fn from_rows(rows: Vec<EvalRow>) -> Self {
        Self::with_details(rows, BTreeMap::new())
    }

    pub fn with_details(rows: Vec<EvalRow>, details: BTreeMap<String, String>) -> Self {
        let sample_passes = n1_sample_passes(&rows);
        Self {
            rows,
            details,
            sampling: crate::eval_sampling::SampleConfig::default(),
            sample_passes,
        }
    }
}

fn n1_sample_passes(rows: &[EvalRow]) -> BTreeMap<String, u32> {
    rows.iter()
        .map(|(id, outcome, _, _)| (id.clone(), u32::from(*outcome == CaseOutcome::Pass)))
        .collect()
}

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
///
/// `bundle_digest` (#1087 AC2) is the sha256 of the snapshot the evaluated
/// runner mounted, carried on the MACHINE surface so an agent can confirm
/// `skill message` and `skill eval` executed the same bundle without reading a
/// human note off stderr. The key is always emitted -- `null` at the
/// local/cluster tiers, which evaluate a deployed version rather than a locally
/// snapshotted bundle, and on a skill run against a runner this checkout never
/// recorded.
pub fn eval_json(results: &[EvalRow], bundle_digest: Option<&str>) -> serde_json::Value {
    eval_json_with_details(&EvalReport::from_rows(results.to_vec()), bundle_digest)
}

fn eval_json_with_details(report: &EvalReport, bundle_digest: Option<&str>) -> serde_json::Value {
    // Derive every count from `results` in one pass so the rollup can never
    // disagree with the per-case rows (no caller-supplied passed/total to drift).
    let results = &report.rows;
    let total = results.len();
    let (passed, failed, plumbing_ok) = eval_counts(results);
    let n = report.sampling.n;
    let policy = report.sampling.policy.as_str();
    let cases: Vec<serde_json::Value> = results
        .iter()
        .map(|(id, outcome, seconds, output)| {
            let sample_passes = report.sample_passes.get(id).copied().unwrap_or(0);
            let mut row = serde_json::json!({
                "id": id,
                "outcome": outcome,
                // Tri-state (ADR-0055): a non-graded row claims neither verdict.
                // `null` keeps a truthiness reader fail-safe (it under-reports,
                // never false-greens) without ever alleging a failure that did
                // not happen.
                "passed": outcome.passed(),
                "seconds": seconds,
                "output": output,
                "samples": n,
                "passes": sample_passes,
                "policy": policy,
            });
            if let Some(detail) = report.details.get(id) {
                row["detail"] = serde_json::json!(detail);
            }
            if n > 1 {
                let bar = match report.sampling.policy {
                    crate::eval_sampling::AggregationPolicy::PassAtK => {
                        format!("pass@{}", report.sampling.effective_k())
                    }
                    crate::eval_sampling::AggregationPolicy::Majority => "majority".to_string(),
                };
                row["variance"] =
                    serde_json::json!(format!("{sample_passes}/{n} samples passed ({bar})"));
            }
            row
        })
        .collect();
    serde_json::json!({
        "total": total,
        "passed": passed,
        "failed": failed,
        "plumbing_ok": plumbing_ok,
        "bundle_digest": bundle_digest,
        "samples": n,
        "policy": policy,
        "cases": cases,
    })
}

/// The recorded bundle digest that honestly applies to the runner at `url`
/// (#1087) -- the one home of that rule, shared by `skill status` and
/// `skill eval`.
///
/// A digest is only knowable for the runner this checkout recorded:
/// `resolve_url` is explicit-wins, so an explicit `--url` at some other runner
/// would otherwise marry a foreign session to the local bundle's digest.
/// Matching the resolved url against the recorded `base_url` is the honest
/// test -- with no `--url` the resolved value IS the recorded one, so the digest
/// still reports. `None` when nothing is recorded here, when the record predates
/// #1087, or when it points elsewhere: a null digest, never an error.
fn recorded_bundle_digest(saved: Option<&state::RunnerState>, url: &str) -> Option<String> {
    saved
        .filter(|s| s.base_url == url)
        .and_then(|s| s.bundle_digest.clone())
}

pub async fn status(url: Option<String>) -> Result<()> {
    let url = resolve_url(url)?;
    // The bundle the runner BEING SHOWN is executing (#1087); see
    // `recorded_bundle_digest` for why a foreign `--url` reports none. The load
    // is tolerant because an unreadable `.curie/runner.json` must not break an
    // explicit `--url`, a path that never read local state before #1087 (the
    // no-`--url` path still hard-errors on it, inside `resolve_url`).
    let saved = state::load(Path::new(".")).unwrap_or(None);
    let bundle_digest = recorded_bundle_digest(saved.as_ref(), &url);
    let client = RunnerClient::new(&url)?;
    let status = client.status().await?;
    crate::ui::ui().emit(&StatusOutput {
        url,
        status: serde_json::to_value(&status)?,
        bundle_digest,
    });
    Ok(())
}

/// Output of `skill status` (#474). `to_json` delegates to the schema-gated
/// `status_json` builder (byte-identical, so `cli/schema/status.schema.json` and
/// `json_contract.rs` stay green); `render` reproduces the note + pretty payload.
struct StatusOutput {
    url: String,
    status: serde_json::Value,
    /// The digest recorded in `.curie/runner.json` (#1087), and only when that
    /// record is the runner at `url`; `None` otherwise, so the key never claims
    /// a digest for a runner it was not recorded against.
    bundle_digest: Option<String>,
}

impl crate::ui::CliOutput for StatusOutput {
    fn to_json(&self) -> serde_json::Value {
        status_json(&self.url, &self.status, self.bundle_digest.as_deref())
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.note(&format!("runner {}", self.url));
        // Diagnostics on stderr (#11): the digest is a note, so the machine
        // payload on stdout is unchanged for a human-path consumer.
        ui.note(&format!(
            "bundle {}",
            self.bundle_digest.as_deref().unwrap_or("<none recorded>")
        ));
        ui.payload_plain(
            &serde_json::to_string_pretty(&self.status).unwrap_or_else(|_| self.status.to_string()),
        );
    }
}

/// What one `curie <tier> surfaces <agent>` invocation does to the agent's
/// binding set: nothing (list), add one pair, or remove one pair.
///
/// Exactly one mutation per invocation, never a batch: the API has no batch
/// endpoint, so a half-applied run would leave the operator guessing what took.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChannelChange {
    /// No flags: read the agent's bindings and write nothing.
    List,
    Add {
        kind: String,
        address: String,
        endpoint: Option<String>,
        adapter: Option<String>,
    },
    Remove {
        kind: String,
        address: String,
    },
}

impl ChannelChange {
    /// Resolve the `--add` / `--remove` flag pair into one intent.
    ///
    /// clap already refuses the two together (`conflicts_with`), so this parses
    /// whichever arrived. Usage errors are raised here, before any I/O, so a
    /// mistyped pair costs no network round trip.
    ///
    /// Args:
    ///   add: the `--add KIND=ADDRESS` value, if passed.
    ///   remove: the `--remove KIND=ADDRESS` value, if passed.
    ///
    /// Returns:
    ///   The intent, or a usage error when the pair is malformed.
    pub fn resolve(
        add: Option<String>,
        remove: Option<String>,
        endpoint: Option<String>,
        adapter: Option<String>,
    ) -> Result<Self> {
        match (add, remove) {
            (Some(spec), _) => {
                let (kind, address) = parse_channel_pair(&spec)?;
                Ok(ChannelChange::Add {
                    kind,
                    address,
                    endpoint,
                    adapter,
                })
            }
            (None, Some(spec)) => {
                let (kind, address) = parse_channel_pair(&spec)?;
                Ok(ChannelChange::Remove { kind, address })
            }
            (None, None) => Ok(ChannelChange::List),
        }
    }
}

/// Split `KIND=ADDRESS` on the FIRST `=` only. A kind may not contain one; an
/// address may (an email- or URL-shaped address for a non-Slack ingress is the
/// whole reason bindings went channel-neutral), so everything after the first
/// separator is the address, `=` included.
fn parse_channel_pair(spec: &str) -> Result<(String, String)> {
    let malformed = || {
        crate::exit::usage(format!(
            "--add/--remove takes KIND=ADDRESS (e.g. slack=C0EXAMPLE1), got {spec:?}. \
             The kind is never inferred: a binding names the ingress explicitly"
        ))
    };
    let (kind, address) = spec.split_once('=').ok_or_else(malformed)?;
    if kind.is_empty() || address.is_empty() {
        return Err(malformed());
    }
    Ok((kind.to_string(), address.to_string()))
}

/// Output of `<tier> surfaces <agent>`: the dry-run plan, or the agent's
/// binding set as the API stored it. Owns its data so it outlives the
/// `ApiClient`.
///
/// `channels` carries the PAIRS, not bare addresses, so an agent consumer reads
/// the kind without guessing it. `changed` distinguishes a list from a
/// mutation, so a consumer can tell "this is what it is" from "this is what it
/// now is" without diffing.
#[derive(Debug)]
pub enum ChannelsOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        channels: Vec<crate::api::ChannelBinding>,
        changed: bool,
    },
}

const CHANNEL_BINDING_NEVER_RESOLVES_WARNING: &str =
    "mentions match on the channel ID, not the name, so this binding never resolves";

#[derive(Serialize)]
struct ChannelBindingPresentation<'a> {
    kind: &'a str,
    address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    warning: Option<&'static str>,
}

fn channel_binding_never_resolves(kind: &str, address: &str) -> bool {
    kind == "slack" && address.trim_start().starts_with('#')
}

fn channel_binding_presentation(
    binding: &crate::api::ChannelBinding,
) -> ChannelBindingPresentation<'_> {
    ChannelBindingPresentation {
        kind: &binding.kind,
        address: &binding.address,
        warning: channel_binding_never_resolves(&binding.kind, &binding.address)
            .then_some(CHANNEL_BINDING_NEVER_RESOLVES_WARNING),
    }
}

impl crate::ui::CliOutput for ChannelsOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            ChannelsOutput::DryRun(plan) => plan.to_json(),
            ChannelsOutput::Done {
                agent,
                channels,
                changed,
            } => {
                let surfaces = channels
                    .iter()
                    .map(channel_binding_presentation)
                    .collect::<Vec<_>>();
                serde_json::json!({
                    "agent": agent,
                    // Serialize each CLI presentation row wholesale. The raw
                    // API mirror remains unchanged while this output adds its
                    // optional, derived warning without hand-projecting fields.
                    "surfaces": serde_json::to_value(surfaces)
                        .unwrap_or(serde_json::Value::Null),
                    "changed": changed,
                })
            }
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            ChannelsOutput::DryRun(plan) => plan.render(ui),
            ChannelsOutput::Done {
                agent,
                channels,
                changed,
            } => {
                let verb = if *changed { " now" } else { "" };
                let bound = if channels.is_empty() {
                    "none".to_string()
                } else {
                    channels
                        .iter()
                        .map(|c| format!("{}:{}", c.kind, c.address))
                        .collect::<Vec<_>>()
                        .join(", ")
                };
                ui.payload(&format!("surfaces for {agent}{verb}: {bound}"));
                for channel in channels {
                    if channel_binding_never_resolves(&channel.kind, &channel.address) {
                        ui.warn(&format!(
                            "{}:{}: {CHANNEL_BINDING_NEVER_RESOLVES_WARNING}",
                            channel.kind, channel.address
                        ));
                    }
                }
            }
        }
    }
}

/// `curie <tier> surfaces <agent> [--add KIND=ADDRESS | --remove KIND=ADDRESS]`.
///
/// With no flags this LISTS: one `GET`-resolved agent, no write. With one flag
/// it adds or removes exactly that binding, then reports the set as the API
/// holds it, so the operator sees what took rather than what was intended.
///
/// Args:
///   opts: api url/key, the agent name or id, and the dry-run flag.
///   change: the intent already parsed from the flag pair.
///
/// Returns:
///   The agent's bindings, or the dry-run plan.
///
/// Named `channel_bindings` rather than `channels` after the verb: the
/// emit-parity gate's reachability walk follows a `to_json` body's bare
/// identifiers to same-named free functions (`cli/tests/support/emit_parity.rs`),
/// and `channels` is now a field identifier several unrelated bodies mention,
/// so a free fn by that name gets pulled into their keysets and reports
/// omissions on other verbs as stale.
pub async fn channel_bindings(
    opts: AgentActionOpts,
    change: ChannelChange,
) -> Result<ChannelsOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        let plan =
            match &change {
                ChannelChange::List => format!(
                    "GET {}/agents  (read-only: would resolve agent {:?} and print its surfaces)",
                    opts.api_url, opts.agent
                ),
                ChannelChange::Add {
                    kind,
                    address,
                    endpoint,
                    adapter,
                } => format!(
                "POST {}/agents/<id>/channels  {{\"kind\":\"{kind}\",\"address\":\"{address}\"}}  \
                 (would resolve agent {:?} first; reply route: {})",
                opts.api_url,
                opts.agent,
                if endpoint.is_some() && adapter.is_some() { "configured" } else { "implicit" }
            ),
                ChannelChange::Remove { kind, address } => format!(
                    "DELETE {}/agents/<id>/channels?kind={kind}&address={address}  \
                 (would resolve agent {:?} first)",
                    opts.api_url, opts.agent
                ),
            };
        return Ok(ChannelsOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![plan],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let (kind, address, adding) = match &change {
        ChannelChange::List => {
            // find_agent already carries the bindings, so there is nothing
            // further to fetch and nothing to write.
            return Ok(ChannelsOutput::Done {
                agent: agent.name,
                channels: agent.channels,
                changed: false,
            });
        }
        ChannelChange::Add { kind, address, .. } => (kind, address, true),
        ChannelChange::Remove { kind, address } => (kind, address, false),
    };
    let cl = ui.checklist();
    let verb = if adding { "adding" } else { "removing" };
    let step = cl.step(&format!(
        "{verb} {kind}:{address} on {name}",
        name = agent.name
    ));
    let saved = if adding {
        let (endpoint, adapter) = match &change {
            ChannelChange::Add {
                endpoint, adapter, ..
            } => (endpoint.as_deref(), adapter.as_deref()),
            _ => (None, None),
        };
        client
            .add_agent_channel(&agent.id, kind, address, endpoint, adapter)
            .await
    } else {
        // The DELETE answers 204 with no body, so the remaining set comes from
        // a fresh read rather than from locally subtracting the pair -- the CLI
        // reports what the API holds, never what it assumes it holds.
        match client.remove_agent_channel(&agent.id, kind, address).await {
            Ok(()) => client.get_agent(&agent.id).await,
            Err(err) => Err(err),
        }
    };
    let saved = match saved {
        Ok(saved) => {
            step.done(if adding { "added" } else { "removed" });
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(ChannelsOutput::Done {
        agent: saved.name,
        channels: saved.channels,
        changed: true,
    })
}

#[cfg(test)]
mod channels_tests {
    use super::ChannelChange;

    #[test]
    fn channel_change_parses_kind_and_address_on_first_equals() {
        // KIND=ADDRESS splits on the FIRST `=` only. A kind may not contain
        // one; an address may -- an email-shaped or URL-shaped address for a
        // non-Slack ingress is the whole reason bindings went channel-neutral.
        // Splitting on the last `=`, or rejecting the second one, would make
        // those addresses unbindable through the CLI.
        let change =
            ChannelChange::resolve(Some("slack=C0EXAMPLE1".into()), None, None, None).unwrap();
        assert_eq!(
            change,
            ChannelChange::Add {
                kind: "slack".into(),
                address: "C0EXAMPLE1".into(),
                endpoint: None,
                adapter: None,
            }
        );

        let odd =
            ChannelChange::resolve(Some("email=ops+a=b@example.com".into()), None, None, None)
                .unwrap();
        assert_eq!(
            odd,
            ChannelChange::Add {
                kind: "email".into(),
                address: "ops+a=b@example.com".into(),
                endpoint: None,
                adapter: None,
            },
            "everything after the first `=` is the address, `=` included"
        );

        // The same rule on the remove side: one parser, both flags.
        let removed =
            ChannelChange::resolve(None, Some("slack=C0EXAMPLE2".into()), None, None).unwrap();
        assert_eq!(
            removed,
            ChannelChange::Remove {
                kind: "slack".into(),
                address: "C0EXAMPLE2".into(),
            }
        );

        // Neither flag is an inspect, not an error: `channels <agent>` lists.
        assert_eq!(
            ChannelChange::resolve(None, None, None, None).unwrap(),
            ChannelChange::List
        );
    }

    #[test]
    fn channel_change_rejects_a_bare_address_with_no_kind() {
        // `--add C0EXAMPLE1` is the mistake this catches. Defaulting the kind
        // to "slack" would be the silent-wrong-thing: the operator learns the
        // kind is optional, and the first non-Slack ingress binds to the wrong
        // one. The error must exit USAGE, before any network call.
        let err = ChannelChange::resolve(Some("C0EXAMPLE1".into()), None, None, None).unwrap_err();
        let (class, _fix) = crate::exit::classify(&err);
        assert_eq!(class, crate::exit::ExitClass::Usage);
        assert!(err.to_string().contains("KIND=ADDRESS"), "{err}");

        // An empty kind or an empty address is the same mistake wearing a
        // separator, and must not slip through as a half-empty pair.
        for bad in ["=C0EXAMPLE1", "slack=", "="] {
            assert!(
                ChannelChange::resolve(Some(bad.into()), None, None, None).is_err(),
                "{bad:?} must not resolve to a binding"
            );
        }
    }
}

pub async fn send(
    text: &str,
    user: &str,
    event_type: EventType,
    url: Option<String>,
    r#continue: bool,
) -> Result<bool> {
    let url = resolve_url(url)?;
    let saved = state::load(Path::new(".")).unwrap_or(None);
    let bundle_warning = editable_bundle_warning(saved.as_ref(), &url);
    let client = RunnerClient::new(&url)?;
    let ui = crate::ui::ui();
    if let Some(warning) = bundle_warning {
        ui.warn(&warning);
    }
    let mut printer = TurnPrinter::default();

    if !r#continue {
        client
            .reset()
            .await
            .context("resetting the runner conversation before message")?;
    }

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

    if let Some(final_event @ OutboundEvent::Final { status, .. }) = events.last() {
        // Under `--json`, project the real final frame into one buffered turn
        // object (reply, status, and approval metadata) rather than the
        // streamed/human trailer (#485, #2108). Emit BEFORE any exit so a
        // classified failure still carries its data to the consumer.
        if json {
            let output = SkillMessageOutput::from_final(std::mem::take(&mut reply), final_event)
                .expect("the matched outbound event is final");
            ui.emit(&output);
            return Ok(*status == SessionStatus::ClassifiedFailure);
        }
        // Close the streamed answer on stdout only if the last thing written was
        // un-terminated token text; if a note already added its own newline (or
        // the last token ended in one) skip it to avoid a blank line. The status
        // trailer is a diagnostic -> stderr.
        if streamed && !at_line_start {
            ui.print_tokens("\n");
        }
        ui.note(&format!("-- final ({})", status_str(status)));
        return Ok(*status == SessionStatus::ClassifiedFailure);
    }
    Ok(false)
}

/// Output of `skill message` under `--json`: the full buffered reply, final
/// session status, and approval metadata copied from the runner's final frame.
/// The human path streams tokens live and never builds this; it exists so
/// `--json` emits one complete object instead of empty stdout (#485, #2108).
#[derive(Debug)]
pub struct SkillMessageOutput {
    pub reply: String,
    pub status: String,
    pub finalized: bool,
    pub approval_summary: Option<String>,
    pub approval_route: Option<String>,
    pub approval_gate_kind: Option<String>,
    pub approval_granted_tool: Option<String>,
}

impl SkillMessageOutput {
    /// Project a runner final frame into the agent-facing result without
    /// inventing terminal state. An approval park is resumable, so it is the
    /// only final-frame status for which `finalized` is false.
    pub fn from_final(reply: String, event: &OutboundEvent) -> Option<Self> {
        let OutboundEvent::Final {
            status,
            approval_summary,
            approval_route,
            approval_gate_kind,
            approval_granted_tool,
            ..
        } = event
        else {
            return None;
        };

        Some(Self {
            reply,
            status: status_str(status).to_string(),
            finalized: !matches!(status, SessionStatus::AwaitingApproval),
            approval_summary: approval_summary.clone(),
            approval_route: approval_route.clone(),
            approval_gate_kind: approval_gate_kind.clone(),
            approval_granted_tool: approval_granted_tool.clone(),
        })
    }
}

fn editable_bundle_warning(saved: Option<&state::RunnerState>, url: &str) -> Option<String> {
    let saved = saved.filter(|saved| saved.base_url == url)?;
    let recorded_digest = saved.bundle_digest.as_deref()?;
    let editable_digest = match crate::bundle::digest_source(Path::new(&saved.plugin_dir)) {
        Ok(digest) => digest,
        Err(_) => {
            return Some(
                "could not compare the editable bundle with the running snapshot. Run `curie skill up` after fixing the bundle."
                    .to_string(),
            );
        }
    };
    if editable_digest == recorded_digest {
        return None;
    }
    Some(
        "editable bundle differs from the running snapshot. Run `curie skill up` to load the changes."
            .to_string(),
    )
}

impl crate::ui::CliOutput for SkillMessageOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "reply": self.reply,
            "status": self.status,
            "finalized": self.finalized,
            "approval_summary": self.approval_summary,
            "approval_route": self.approval_route,
            "approval_gate_kind": self.approval_gate_kind,
            "approval_granted_tool": self.approval_granted_tool,
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        ui.answer(&self.reply);
        ui.note(&format!("-- final ({})", self.status));
    }
}

pub async fn eval(
    cases_path: Option<PathBuf>,
    case_ids: Vec<String>,
    url: Option<String>,
    models: Vec<String>,
    secrets: Vec<String>,
    image: String,
    sampling: crate::eval_sampling::SampleConfig,
) -> Result<()> {
    let saved = state::load(Path::new("."))?;
    let state_plugin_dir = saved.as_ref().map(|s| PathBuf::from(s.plugin_dir.clone()));

    // Model selection (#526): with `--model`, boot a transient runner per model,
    // run the suite against each, and report pass-rate per model -- the one
    // command a "can we move to a cheaper model" decision needs, instead of a
    // manual `skill up --model X` + `skill eval` loop per model. Without it, the
    // default path drives the already-running runner (whatever model it booted).
    if !models.is_empty() {
        let recorded_snapshot_dir = sweep_snapshot(saved.as_ref()).map(|(dir, _)| dir);
        let cases_path = resolve_cases_path(
            cases_path,
            Path::new("."),
            recorded_snapshot_dir.as_deref(),
            state_plugin_dir.as_deref(),
        )?;
        let loaded = load_eval(&cases_path)?;
        let total_cases = loaded.suite.cases.len();
        let trajectory = loaded.trajectory;
        // Unlike the local/cluster sweep -- the platform eval plane, which
        // `POST /evals/trigger`s a suite NAME and lets the worker reload the
        // deployed suite server-side, so a local selection can never reach it --
        // the skill-tier sweep boots a transient LOCAL runner per model and runs
        // the suite in-CLI via `run_suite_cases`. A selection made here DOES
        // reach the run, so it is honored rather than refused.
        let suite = crate::evals::select_cases(loaded.suite, &case_ids)?;
        if let Some(note) = crate::evals::selection_note(&case_ids, suite.cases.len(), total_cases)
        {
            crate::ui::ui().note(&note);
        }
        return eval_sweep(
            &suite,
            trajectory.as_ref(),
            &models,
            &secrets,
            &image,
            sweep_snapshot(saved.as_ref()),
            state_plugin_dir.as_deref(),
            sampling,
        )
        .await;
    }

    let fake = drives_a_fake_runner(saved.as_ref(), url.as_deref());
    let url = resolve_url(url)?;
    let recorded_snapshot_dir = saved
        .as_ref()
        .filter(|saved| saved.base_url == url)
        .and_then(|saved| saved.bundle_snapshot_dir.as_ref())
        .map(PathBuf::from);
    let cases_path = resolve_cases_path(
        cases_path,
        Path::new("."),
        recorded_snapshot_dir.as_deref(),
        state_plugin_dir.as_deref(),
    )?;
    let loaded = load_eval(&cases_path)?;
    let total_cases = loaded.suite.cases.len();
    let trajectory = loaded.trajectory;
    // A selector that matches nothing exits 2 before any runner contact, so a
    // mistyped --case-id fails the gate rather than greening an empty run.
    let suite = crate::evals::select_cases(loaded.suite, &case_ids)?;
    // #1087 AC2: the bundle this eval graded, on the machine surface, so an
    // agent can confirm it is the SAME digest `skill status`/`skill message`
    // report without reading a human note off stderr (docs/agents.md bans
    // stderr as agent-facing evidence). The honesty rule itself lives in
    // `recorded_bundle_digest`, shared with `status`.
    let bundle_digest = recorded_bundle_digest(saved.as_ref(), &url);
    let client = RunnerClient::new(&url)?;
    let ui = crate::ui::ui();
    if let Some(note) = crate::evals::selection_note(&case_ids, suite.cases.len(), total_cases) {
        ui.note(&note);
    }
    // `run_suite_cases` also tallies completion for the `--model` sweep path;
    // the single-runner report doesn't need the count (it already reports the
    // per-case `Fail` either way and exits on any of them), so it is discarded.
    let bar = ui.progress_bar(
        (suite.cases.len() as u64).saturating_mul(u64::from(sampling.n)),
        "running evals",
    );
    let (results, _completed) =
        run_suite_cases(&client, &suite, fake, trajectory.as_ref(), sampling, |_| {
            bar.inc(1)
        })
        .await?;
    bar.finish();

    report_eval(&results, bundle_digest.as_deref(), ())
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
    trajectory_scorer: Option<&TrajectoryScorer>,
    sampling: crate::eval_sampling::SampleConfig,
    mut on_sample: impl FnMut(usize),
) -> Result<(EvalReport, usize)> {
    let mut results = Vec::with_capacity(suite.cases.len());
    let mut details = BTreeMap::new();
    let mut sample_passes = BTreeMap::new();
    let mut completed = 0usize;
    for case in &suite.cases {
        let mut samples = Vec::with_capacity(sampling.n as usize);
        let mut case_completed = false;
        for _ in 0..sampling.n {
            // Fresh conversation before every sample (#550 / #1907): two samples
            // of the same case must each start clean, or the second inherits the
            // first's turn. A shared_history case still skips the reset.
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
            let sample_completed = turn_completed(case, &events);
            if sample_completed {
                case_completed = true;
            }
            let scored = score_turn(case, &events, fake, trajectory_scorer);
            if let Some(detail) = scored.detail.clone() {
                details.insert(case.id.clone(), detail);
            }
            samples.push(crate::eval_sampling::SampleRecord {
                outcome: scored.outcome,
                output: graded_answer(&events),
                seconds: elapsed,
                error: if sample_completed {
                    None
                } else {
                    Some("turn did not complete".into())
                },
            });
            on_sample(0);
        }
        if case_completed {
            completed += 1;
        }
        let agg = crate::eval_sampling::aggregate_samples(&samples, sampling);
        sample_passes.insert(case.id.clone(), agg.passes);
        if let Some(variance) = &agg.variance {
            details
                .entry(case.id.clone())
                .or_insert_with(|| variance.clone());
        }
        results.push((case.id.clone(), agg.outcome, agg.seconds, agg.output));
    }
    Ok((
        EvalReport {
            rows: results,
            details,
            sampling,
            sample_passes,
        },
        completed,
    ))
}

/// The `docker run` spec one eval-sweep runner boots with. Split out of
/// [`boot_eval_runner`] (which needs a Docker daemon and the host credential
/// store) so the mount that actually reaches the daemon is unit-testable: the
/// #1087 AC2 seam ends in `run_args()`, not in a struct field. `boot_eval_runner`
/// builds its spec ONLY through here, so there is one place the eval path names
/// what it mounts, and that place can take nothing but an [`EvalBundle`].
fn eval_runner_spec(
    bundle: &EvalBundle,
    image: &str,
    port: u16,
    name: &str,
    model: &str,
    passthrough_env: Vec<String>,
    docker_env: Vec<(String, String)>,
) -> StartSpec {
    StartSpec {
        image: image.to_string(),
        container_name: name.to_string(),
        host_port: port,
        plugin_dir: bundle.dir().to_path_buf(),
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
    }
}

/// Boot a throwaway runner for one model on `port`, forwarding the model
/// credential and any `--secret` from the env or the host vault exactly like
/// `skill up` (never in argv). Returns its base URL; the caller removes the
/// container when done. Does NOT touch `.curie/runner.json`, so a sweep never
/// clobbers a persistent `skill up` runner's recorded state.
///
/// `bundle` is a materialized bundle snapshot (#1087) -- the recorded runner's,
/// or one this sweep packed -- and, being an [`EvalBundle`], cannot be a source
/// directory: the skill tier executes an immutable bundle, so handing this the
/// editable source would reopen exactly the gap the snapshot closes, and the
/// type is what stops a caller doing it.
async fn boot_eval_runner(
    bundle: &EvalBundle,
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
    let spec = eval_runner_spec(
        bundle,
        image,
        port,
        name,
        model,
        passthrough_env,
        docker_env,
    );
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

/// The snapshot the model sweep must mount (#1087 AC2). Reusing the recorded
/// runner's snapshot is what makes `skill message` and `skill eval` report the
/// SAME digest; `None` means no runner is recorded (or the record predates
/// #1087), and the sweep packs its own snapshot rather than falling back to
/// mutable source. Pure so the sibling path is testable without Docker.
fn sweep_snapshot(saved: Option<&state::RunnerState>) -> Option<(PathBuf, String)> {
    let saved = saved?;
    // Both halves or nothing: a directory with no digest has nothing to report,
    // and a digest with no directory has nothing to mount.
    let dir = saved.bundle_snapshot_dir.as_ref()?;
    let digest = saved.bundle_digest.as_ref()?;
    Some((PathBuf::from(dir), digest.clone()))
}

/// What the model sweep mounts (#1087 AC2). Pure decision, split out of
/// `eval_sweep` so the wiring regression -- a sweep handing SOURCE to
/// `boot_eval_runner` -- reds a unit test instead of only the live run.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SweepMount {
    /// Mount the recorded runner's snapshot at this path, under this digest.
    /// No re-pack: reusing the recorded snapshot is what makes the eval digest
    /// the SAME value as the messaging digest, not merely equal-by-recompute.
    Recorded { dir: PathBuf, digest: String },
    /// No runner recorded: pack an EPHEMERAL snapshot from this source dir.
    /// Never a fall-back to mounting the source itself.
    PackEphemeral { source: PathBuf },
}

/// Resolve the sweep's mount. A recorded snapshot always wins, even when a
/// bundle source is known too -- that is the decision `eval_sweep` must not
/// re-make in its own body. There is deliberately no variant that mounts the
/// editable source: that is the hole #1087 closes.
fn resolve_sweep_mount(
    recorded: Option<(PathBuf, String)>,
    state_plugin_dir: Option<&Path>,
) -> SweepMount {
    match recorded {
        Some((dir, digest)) => SweepMount::Recorded { dir, digest },
        None => SweepMount::PackEphemeral {
            source: state_plugin_dir
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from(".")),
        },
    }
}

pub use eval_bundle::EvalBundle;

/// Home of [`EvalBundle`], the only directory type the eval runner path accepts.
///
/// It is a module rather than a bare struct so its fields are private to it:
/// nothing in `commands` -- `eval_sweep` above all -- can build one out of a
/// path it happens to be holding. That is the #1087 AC2 wiring guard. Before
/// this, restoring the old source-directory mount was a one-line variable swap
/// in `eval_sweep` that no unit test could see, because `eval_sweep` needs
/// Docker to run at all. Now that swap does not compile, and the only place the
/// eval path can still name a directory to mount is
/// [`EvalBundle::materialize`], which `cargo test` covers directly.
mod eval_bundle {
    use super::SweepMount;
    use anyhow::{Context, Result};
    use std::path::{Path, PathBuf};

    /// A materialized, immutable bundle snapshot plus the digest that names it.
    ///
    /// Construct only via [`EvalBundle::materialize`]. There is deliberately no
    /// constructor taking a bare path: a source directory cannot become one of
    /// these without editing this module.
    #[derive(Debug)]
    pub struct EvalBundle {
        dir: PathBuf,
        digest: String,
        /// The bundle source this snapshot was packed from, and only when THIS
        /// value owns the snapshot: a recorded runner's snapshot belongs to
        /// that runner's record and is released by `skill down`, never here.
        ephemeral_source: Option<PathBuf>,
    }

    impl EvalBundle {
        /// Turn a resolved [`SweepMount`] into a directory that exists on disk:
        /// canonicalize the recorded snapshot, or pack an ephemeral one from
        /// source. Neither arm may yield the source directory itself -- that is
        /// the whole point of #1087, and it is what the unit tests pin.
        ///
        /// `pub(super)` rather than `pub`: the only caller is `eval_sweep` (and
        /// the unit tests), and [`SweepMount`] is private to `commands`, so a
        /// wider visibility would only leak that type out of the module.
        pub(super) fn materialize(mount: SweepMount) -> Result<Self> {
            match mount {
                SweepMount::Recorded { dir, digest } => Ok(Self {
                    dir: dir
                        .canonicalize()
                        .context("resolving the recorded bundle snapshot for the model sweep")?,
                    digest,
                    ephemeral_source: None,
                }),
                SweepMount::PackEphemeral { source } => {
                    let source = source
                        .canonicalize()
                        .context("resolving the bundle directory for the model sweep")?;
                    let snapshot = crate::bundle::snapshot_ephemeral(&source)
                        .context("packaging the bundle snapshot for the model sweep")?;
                    Ok(Self {
                        dir: snapshot.dir,
                        digest: snapshot.digest,
                        ephemeral_source: Some(source),
                    })
                }
            }
        }

        /// The directory to mount read-only at `/plugin`.
        pub fn dir(&self) -> &Path {
            &self.dir
        }

        /// The sha256 this bundle is content-addressed by -- the same value
        /// `skill status`/`skill message` report when the snapshot is the
        /// recorded runner's (#1087 AC2).
        pub fn digest(&self) -> &str {
            &self.digest
        }

        /// The source dir to release this snapshot against when the run ends,
        /// or `None` when the snapshot is not this value's to remove.
        pub fn ephemeral_source(&self) -> Option<&Path> {
            self.ephemeral_source.as_deref()
        }
    }
}

/// Run the suite once per model in a fresh runner and report pass-rate per model.
#[allow(clippy::too_many_arguments)]
async fn eval_sweep(
    suite: &EvalSuite,
    trajectory_scorer: Option<&TrajectoryScorer>,
    models: &[String],
    secrets: &[String],
    image: &str,
    recorded: Option<(PathBuf, String)>,
    state_plugin_dir: Option<&Path>,
    sampling: crate::eval_sampling::SampleConfig,
) -> Result<()> {
    let ui = crate::ui::ui();
    // The mount is decided once, purely, by `resolve_sweep_mount`, then
    // materialized once by `EvalBundle`; this body only acts on the result and
    // has no path of its own it could mount instead.
    let bundle = EvalBundle::materialize(resolve_sweep_mount(recorded, state_plugin_dir))?;
    ui.note(&format!(
        "model sweep: bundle {} ({})",
        bundle.digest(),
        if bundle.ephemeral_source().is_some() {
            "packed"
        } else {
            "recorded runner's snapshot"
        }
    ));
    ui.note(&format!(
        "model sweep: {} model(s) x {} case(s)",
        models.len(),
        suite.cases.len()
    ));
    let cl = ui.checklist();
    let sweep = async {
        let mut rows: Vec<SweepRow> = Vec::with_capacity(models.len());
        for (i, model) in models.iter().enumerate() {
            let name = format!("curie-eval-sweep-{i}");
            let port = DEFAULT_PORT + 100 + i as u16;
            let step = cl.step(&format!("model {model}"));
            let url = match boot_eval_runner(&bundle, image, port, &name, model, secrets).await {
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
            let run =
                run_suite_cases(&client, suite, false, trajectory_scorer, sampling, |_| {}).await;
            let _ = docker::remove_container(&name).await;
            let (results, completed) = run?;
            let passed = results
                .rows
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
        // #1087 AC2: the digest rides the machine payload, so an agent confirms
        // `skill message` and `skill eval` ran the same bundle from `--json`
        // rather than from the stderr note above (docs/agents.md bans stderr as
        // agent-facing evidence). It is the RECORDED runner's digest only when
        // this sweep reused the recorded snapshot; a sweep that packed its own
        // reports that ephemeral snapshot's digest instead, never a borrowed one.
        report_sweep(&rows, Some(bundle.digest()))
    }
    .await;
    // A snapshot this sweep packed is owned by this sweep alone, so it is
    // released on the failure path as well as the success one (#1087). It can
    // only ever be a `sweep-*` directory, never the canonical `<digest>/` one a
    // live `skill up` runner may have mounted.
    if let Some(source) = bundle.ephemeral_source() {
        let _ = crate::bundle::remove_snapshot(bundle.dir(), source);
    }
    sweep
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
///
/// `bundle_digest` (#1087 AC2) is the snapshot every runner in the sweep
/// mounted: the recorded runner's digest when the sweep reused it (the value
/// `skill status`/`skill message` report, which is what makes AC2 confirmable
/// from the machine surface), or the ephemeral snapshot's own digest when the
/// sweep packed one because nothing was recorded. Always emitted, `null` at the
/// local/cluster tiers where no locally snapshotted bundle applies.
pub fn sweep_json(rows: &[SweepRow], bundle_digest: Option<&str>) -> serde_json::Value {
    serde_json::json!({
        "sweep": rows.iter().map(sweep_json_row).collect::<Vec<_>>(),
        "bundle_digest": bundle_digest,
    })
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
///
/// `bundle_digest` (#1087 AC2) rides the `--json` payload for the same reason
/// it rides `report_eval`'s: the digest was previously a stderr note only, and
/// `docs/agents.md` bans stderr as agent-facing evidence. Callers pass `None`
/// when no locally snapshotted bundle applies.
pub fn report_sweep(rows: &[SweepRow], bundle_digest: Option<&str>) -> Result<()> {
    let ui = crate::ui::ui();
    if ui.json() {
        ui.emit_json(&sweep_json(rows, bundle_digest));
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
///
/// `bundle_digest` (#1087 AC2) rides the `--json` payload so the bundle a run
/// graded is confirmable from the machine surface. Callers pass `None` when no
/// locally snapshotted bundle applies (the local/cluster tiers grade a deployed
/// version), never a digest they did not observe.
///
/// `guards` are dropped before a red eval's non-unwinding `process::exit`
/// (#1908). `std::process::exit` does not run Drop, so a `kubectl port-forward`
/// child or Slack stub still in the caller's scope would otherwise be orphaned
/// onto PID 1. Callers with no such resource pass `()`.
pub fn report_eval<G>(report: &EvalReport, bundle_digest: Option<&str>, guards: G) -> Result<()> {
    let (_passed, failed, _plumbing_ok) = eval_counts(&report.rows);
    // Emit through the one success point (#474), then apply the exit-code side
    // effect for BOTH paths -- the json path had it inline, the human path after.
    // Only a genuine `Fail` (failed > 0) exits non-zero: a plumbing-only run
    // graded nothing but is operationally successful, so it exits 0 (#606/#612).
    crate::ui::ui().emit(&EvalOutput {
        report,
        bundle_digest,
    });
    if failed > 0 {
        crate::exit::exit_after_drop(crate::exit::ExitClass::Failure, guards);
    }
    Ok(())
}

/// Output of `<tier> eval` (#474). `to_json` delegates to the schema-gated
/// `eval_json` builder (byte-identical, so `cli/schema/eval.schema.json` and
/// `json_contract.rs` stay green); `render` reproduces the per-case table and the
/// roll-up verdict + per-red-case reply notes.
struct EvalOutput<'a> {
    report: &'a EvalReport,
    /// The snapshot digest the evaluated runner mounted (#1087), or `None` when
    /// none applies to this run.
    bundle_digest: Option<&'a str>,
}

impl crate::ui::CliOutput for EvalOutput<'_> {
    fn to_json(&self) -> serde_json::Value {
        eval_json_with_details(self.report, self.bundle_digest)
    }

    fn render(&self, ui: &crate::ui::Ui) {
        let results = &self.report.rows;
        let (passed, failed, plumbing_ok) = eval_counts(results);
        let n = self.report.sampling.n;
        let rows: Vec<Vec<String>> = results
            .iter()
            .map(|(name, outcome, seconds, _)| {
                let passes = self.report.sample_passes.get(name).copied().unwrap_or(0);
                let mut cols = vec![
                    name.clone(),
                    outcome_label(*outcome),
                    format!("{seconds:.1}s"),
                ];
                if n > 1 {
                    cols.insert(2, format!("{passes}/{n}"));
                }
                cols
            })
            .collect();
        let headers: &[&str] = if n > 1 {
            &["case", "result", "samples", "time"]
        } else {
            &["case", "result", "time"]
        };
        let right_align: &[usize] = if n > 1 { &[3] } else { &[2] };
        ui.payload_plain(&crate::ui::table(headers, &rows, right_align));
        ui.note(&format!(
            "sampling: {} sample(s), {}",
            n, self.report.sampling.policy
        ));
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
                if let Some(detail) = self.report.details.get(name) {
                    ui.note(&format!("{name}: {detail}"));
                } else {
                    let shown = if output.is_empty() {
                        "<no reply text>".to_string()
                    } else {
                        output.clone()
                    };
                    ui.note(&format!("{name} replied: {shown}"));
                }
            }
            ui.warn(&format!(
                "{}; {failed} failed",
                rollup_line(passed, failed, plumbing_ok)
            ));
        }
    }
}

/// Where the eval cases live: an explicit `--cases` wins; otherwise
/// `evals/cases.json` in the recorded running snapshot wins, then the current
/// directory and, only when no snapshot is recorded, the started runner's
/// recorded bundle directory (so `curie skill eval` works from wherever `curie
/// skill up` was run).
pub fn resolve_cases_path(
    explicit: Option<PathBuf>,
    cwd: &Path,
    recorded_snapshot_dir: Option<&Path>,
    state_plugin_dir: Option<&Path>,
) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    if let Some(snapshot_dir) = recorded_snapshot_dir {
        let in_snapshot = snapshot_dir.join("evals/cases.json");
        if in_snapshot.is_file() {
            return Ok(in_snapshot);
        }
        return Err(crate::exit::CliError::usage(format!(
            "no eval cases found in the running snapshot: {}. Run `curie skill up` or pass --cases",
            in_snapshot.display()
        ))
        .with_fix("run `curie skill up` or pass --cases")
        .into());
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
    /// Deploy under this agent name instead of the manifest's.
    ///
    /// One repository is otherwise structurally one agent (#1166): the name
    /// comes from `plugin.json`, so a bundle cannot run as both a dev and a
    /// prod agent. Overriding at deploy keeps the ARTIFACT identical -- the
    /// bundle bytes are untouched, only the binding differs -- which is the
    /// property that lets prod promote exactly what dev validated.
    pub agent: Option<String>,
    /// Resolve agent/env/channel from a `deploy.yaml` target (ADR-0089).
    pub target: Option<String>,
    pub plugin_dir: PathBuf,
    pub api_url: String,
    pub api_key: String,
    /// Explicit `--slack-channel`; None when the flag was omitted so a redeploy
    /// leaves an existing agent's channel untouched instead of masking intent
    /// with a default.
    pub slack_channel: Option<String>,
    /// `owner/name` binding the repository whose pushes deploy this agent
    /// (ADR-0014). Bound when the agent is created, or on a later deploy if the
    /// agent has no binding yet (#1194). An agent already bound to a DIFFERENT
    /// repository is left alone and warned about, because a deploy does not
    /// reroute an existing binding.
    pub repo: Option<String>,
    /// Deployment-level managed workspace intent. Preserve deliberately omits
    /// the workspace field so the server can carry the previous value forward.
    pub workspace: WorkspaceIntent,
    /// None means the caller did not pass --env, so a declared target may
    /// supply it. An explicit flag still wins (ADR-0089).
    pub env: Option<DeployEnv>,
    pub label: Option<String>,
    /// Per-agent connector secret NAMES to bind on deploy (ADR-0009, #429). Each
    /// value is resolved from the caller's env or the host secret vault and sent
    /// to the platform API, which stores it on the agent for the worker to
    /// forward into the sandbox. From `deploy --secret <NAME>`.
    pub secret: Vec<String>,
    /// Whether this tier offers `--secret` binding, gating the declared-secrets
    /// policy check (#464): true when the tier can bind a declared secret
    /// (local by value, cluster via the per-agent Helm Secret). When false the
    /// gate is skipped.
    pub secret_binding_supported: bool,
    /// Actionable remediation line printed when the platform API connection
    /// fails (e.g. the kubectl port-forward command for cluster, or
    /// `curie local up` for local). Naming the fix turns a raw
    /// "Connection refused" into something the operator can act on.
    pub connect_hint: String,
    /// Which tier this deploy targets. Explicit and without a `Default` impl on
    /// purpose: `local deploy`, `cluster deploy` and `deploy_named` all reach
    /// one `deploy()`, so the cluster-only connector checks have nothing to
    /// select on without it, and a new caller must not silently inherit
    /// `Local` and skip them.
    pub tier: DeployTier,
}

/// The tier a deploy targets. Two variants, no default: the compiler enumerates
/// every construction site when the field is added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeployTier {
    Local,
    Cluster,
}

pub use crate::api::WorkspaceIntent;

pub const EMPTY_GITHUB_REPO_ALLOWLIST_WARNING: &str =
    "api.githubRepoAllowlist is empty, so runtime workspace selection denies every repository. \
     Allow `owner/repo` or `owner/*` in the chart values, for example \
     `curie cluster up --set 'api.githubRepoAllowlist[0]=owner/repo'`";

pub fn github_repo_allowlist_is_empty(values: &serde_json::Value) -> bool {
    match values.pointer("/api/githubRepoAllowlist") {
        None | Some(serde_json::Value::Null) => true,
        Some(serde_json::Value::Array(items)) => items.iter().all(|item| {
            item.as_str()
                .map(|entry| entry.trim().is_empty())
                .unwrap_or(true)
        }),
        Some(serde_json::Value::String(raw)) => {
            let trimmed = raw.trim();
            trimmed.is_empty() || trimmed == "[]"
        }
        Some(_) => false,
    }
}

/// Best-effort warning when `--workspace` is passed against an empty chart allowlist.
pub async fn warn_if_empty_github_repo_allowlist(namespace: &str, release: &str) {
    let opts = crate::ops::CommonOpts {
        namespace: namespace.to_string(),
        release: release.to_string(),
        dry_run: false,
    };
    let Ok(Some(values)) = crate::ops::fetch_release_computed_values(&opts).await else {
        return;
    };
    if github_repo_allowlist_is_empty(&values) {
        crate::ui::ui().warn(EMPTY_GITHUB_REPO_ALLOWLIST_WARNING);
    }
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

/// True when a cluster verb SELF-PLUMBS its API transport: no explicit
/// `--api-url`/`CURIE_API_URL` was given, so the release's api Service is
/// reached over a loopback kubectl port-forward rather than direct-dialed.
///
/// The single statement of that discriminant (#1533). [`deploy_port_forward`]
/// and [`deploy_api_tunnel`] both key on this one predicate, so "is a tunnel in
/// play?" cannot answer differently in the two places a call site consults it.
pub fn deploy_self_plumbs(api_url: Option<&str>) -> bool {
    api_url.is_none()
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
    fullname: &crate::ops::ReleaseFullname,
    local_port: u16,
    remote_port: u16,
) -> Option<crate::ops::OpsCommand> {
    if !deploy_self_plumbs(api_url) {
        return None;
    }
    Some(crate::message::port_forward_command(
        namespace,
        fullname,
        "api",
        local_port,
        remote_port,
    ))
}

/// The self-plumbed API tunnel for a cluster verb: the release's RESOLVED
/// [`crate::ops::ReleaseFullname`] and the port-forward command that reaches its
/// api Service, returned TOGETHER. `None` when an explicit
/// `--api-url`/`CURIE_API_URL` was given, since that path direct-dials the URL
/// and builds no tunnel.
///
/// The fullname is resolved LAZILY, on the self-plumbed branch only: the
/// explicit `--api-url` path names no Service, and
/// `cli/tests/cluster_connection_transport.rs` pins a fully explicit connection
/// as never invoking kubectl at all, so resolving eagerly would fire kubectl on
/// a path proven not to.
///
/// Paired rather than handed back as two independent `Option`s (#1533): the
/// fullname the tunnel forwards to is the same one the caller needs for its
/// `svc/<name>` diagnostics and unreachable hints. Carrying them apart left
/// every call site re-deriving the self-plumbed discriminant for itself and
/// asserting at runtime that the two `Option`s agreed.
pub async fn deploy_api_tunnel(
    api_url: Option<&str>,
    namespace: &str,
    release: &str,
    local_port: u16,
    remote_port: u16,
) -> Option<(crate::ops::ReleaseFullname, crate::ops::OpsCommand)> {
    if !deploy_self_plumbs(api_url) {
        return None;
    }
    let fullname = crate::ops::release_fullname(namespace, release).await;
    let command =
        crate::message::port_forward_command(namespace, &fullname, "api", local_port, remote_port);
    Some((fullname, command))
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

pub struct PreparedDeploy {
    client: ApiClient,
    outcome: crate::api::PreparedDeployOutcome,
    plugin_name: String,
    label: String,
    env: String,
    requested_repo: Option<String>,
    /// The bundle's `deploy.yaml` TEXT, or None when the bundle has no such
    /// file. Carried rather than re-read so the routing check (#1221) asks
    /// about the file that was actually packed, and unparsed because ADR-0089
    /// keeps exactly one parser for this format and it is not in this binary.
    deploy_targets_yaml: Option<String>,
    connect_hint: String,
    step: crate::ui::Step,
    tier: DeployTier,
    plugin_dir: PathBuf,
}

fn is_documentation_placeholder_channel(channel: &str) -> bool {
    channel.strip_prefix("C0EXAMPLE").is_some_and(|suffix| {
        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn reject_documentation_placeholder_target(
    target_name: &str,
    target: &crate::api::ResolvedTarget,
) -> Result<()> {
    let Some(channel) = target.slack_channel.as_deref() else {
        return Ok(());
    };
    if !is_documentation_placeholder_channel(channel) {
        return Ok(());
    }

    Err(crate::exit::CliError::usage(format!(
        "deploy target `{target_name}` uses documentation placeholder Slack channel `{channel}`; \
         refusing before creating or changing an agent, version, or deployment"
    ))
    .with_fix(
        "replace the target's slack_channel with a real Slack channel ID, or remove \
         slack_channel from deploy.yaml",
    )
    .into())
}

impl PreparedDeploy {
    pub fn agent_id(&self) -> &str {
        &self.outcome.agent.id
    }

    pub fn agent_name(&self) -> &str {
        &self.outcome.agent.name
    }

    pub fn version_id(&self) -> &str {
        &self.outcome.version.id
    }
}

pub async fn prepare_deploy(opts: DeployOpts) -> Result<PreparedDeploy> {
    prepare_deploy_with_commit_sha(opts, None).await
}

/// Prepare a deploy with commit provenance supplied by an installer binary.
/// Ordinary deploys pass no override and continue discovering the clean bundle
/// checkout's HEAD. The override is deliberately crate-private: it is not a
/// user-facing way to claim arbitrary provenance.
async fn prepare_deploy_with_commit_sha(
    opts: DeployOpts,
    installer_commit_sha: Option<&str>,
) -> Result<PreparedDeploy> {
    let plugin_dir = opts
        .plugin_dir
        .canonicalize()
        .with_context(|| format!("plugin dir not found: {}", opts.plugin_dir.display()))?;
    let (plugin_name, manifest_version) = read_manifest(&plugin_dir)?;

    // Connector lock preflight (ADR 0113), before the bundle is packed so the
    // failure names the operator's own directory rather than a temp path, and
    // long before anything is applied. `opts.plugin_dir` (not the canonicalized
    // copy) is the path they typed.
    {
        let decl = crate::connector_build::load(&plugin_dir)?;
        if decl.connectors.values().any(|spec| spec.build.is_some()) {
            let recomputed = recompute_source_digests(&plugin_dir, &decl)?;
            let lock = crate::connector_build::load_lock(&plugin_dir)?;
            lock_preflight(
                &opts.plugin_dir,
                &decl,
                lock.as_ref(),
                &recomputed,
                opts.tier,
            )?;
            // What the REGISTRY answers for the locked digest, not what the lock
            // claims about it, is what a node has to pull (see
            // `registry_preflight`). Cluster only: no local deploy pulls this.
            if opts.tier == DeployTier::Cluster {
                run_registry_preflight(&decl, lock.as_ref()).await?;
            }
        }
    }

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
    // `secret_binding_supported` (AC2): skip only on a tier that still cannot
    // bind. Cluster binding is #1488. It
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
        validate_channel_binding("slack", channel)?;
    }
    let archive = pack_tar_gz(&plugin_dir)?;
    let commit_sha = match installer_commit_sha {
        Some(commit_sha) => Some(commit_sha.to_string()),
        None => {
            let git_prefix = tokio::process::Command::new("git")
                .args(["rev-parse", "--show-prefix"])
                .current_dir(&plugin_dir)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .output()
                .await
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|prefix| PathBuf::from(prefix.trim_end_matches(['\r', '\n'])));
            let git_status = tokio::process::Command::new("git")
                .args([
                    "status",
                    "--porcelain=v1",
                    "-z",
                    "--no-renames",
                    "--untracked-files=all",
                    "--ignored=matching",
                    "--",
                    ".",
                ])
                .current_dir(&plugin_dir)
                .env_remove("GIT_DIR")
                .env_remove("GIT_WORK_TREE")
                .output()
                .await
                .ok();
            match (git_prefix, git_status) {
                (Some(prefix), Some(output))
                    if output.status.success()
                        && git_status_is_clean_for_pack(&plugin_dir, &prefix, &output.stdout) =>
                {
                    tokio::process::Command::new("git")
                        .args(["rev-parse", "HEAD"])
                        .current_dir(&plugin_dir)
                        .env_remove("GIT_DIR")
                        .env_remove("GIT_WORK_TREE")
                        .output()
                        .await
                        .ok()
                        .filter(|output| output.status.success())
                        .and_then(|output| String::from_utf8(output.stdout).ok())
                        .map(|sha| sha.trim().to_string())
                        .filter(|sha| !sha.is_empty())
                }
                _ => None,
            }
        }
    };
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    // Resolve a declared target, if one was named (ADR-0089). The file is sent
    // as TEXT and parsed server-side: one parser means the CLI and the
    // validator cannot disagree about where this deploy lands.
    // Read ONCE, for two consumers: `--target` resolution below and the
    // post-deploy routing check (#1221). Absence is the ordinary case, not an
    // error -- most bundles declare no targets -- so the read is kept as a
    // Result only so `--target` can still name the io failure precisely.
    let deploy_targets_path = plugin_dir.join("deploy.yaml");
    let deploy_targets_read = std::fs::read_to_string(&deploy_targets_path);
    let deploy_targets_yaml = deploy_targets_read.as_ref().ok().cloned();
    let resolved = match &opts.target {
        Some(name) => {
            let content = deploy_targets_read.as_ref().map_err(|err| {
                crate::exit::usage(format!(
                    "--target {name} needs a deploy.yaml in the bundle, but {} could not be \
                     read: {err}",
                    deploy_targets_path.display()
                ))
            })?;
            let target = client.resolve_deploy_target(content, name).await?;
            reject_documentation_placeholder_target(name, &target)?;
            Some(target)
        }
        None => None,
    };

    // A target states its environment, which is the point: the flag's `dev`
    // default is what let a prod workflow deploy to dev in silence (#1166).
    let env_owned = opts
        .env
        .map(|e| e.as_str().to_string())
        .or_else(|| resolved.as_ref().map(|r| r.env.clone()))
        .unwrap_or_else(|| "dev".to_string());
    let env = env_owned.as_str();
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
        let value = crate::secrets::resolve_env_or_saved(name)?;
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

    // The manifest name is the default, not the law (#1166). `plugin_name`
    // stays the DISPLAY name so the log still says which bundle was deployed;
    // `agent_name` is what the platform binds. Explicit flags beat the target,
    // so a one-off deploy never requires editing a committed file.
    let agent_name = opts
        .agent
        .clone()
        .or_else(|| resolved.as_ref().and_then(|r| r.agent.clone()))
        .unwrap_or_else(|| plugin_name.clone());
    let cl = ui.checklist();
    let step = cl.step(&format!("deploying {plugin_name} as {agent_name}"));
    let outcome = match client
        .prepare_deploy(
            &agent_name,
            opts.slack_channel
                .as_deref()
                .or_else(|| resolved.as_ref().and_then(|r| r.slack_channel.as_deref())),
            &label,
            &created_by,
            archive,
            &match opts.tier {
                DeployTier::Local => secrets.clone(),
                DeployTier::Cluster => crate::cluster_secrets::agent_record_secrets(&secrets),
            },
            opts.repo.as_deref(),
            commit_sha.as_deref(),
            opts.workspace,
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(err) => {
            step.fail("failed");
            if crate::exit::is_transient_reqwest(&err) {
                return Err(crate::exit::operator_context(err, opts.connect_hint, None));
            }
            return Err(err);
        }
    };

    Ok(PreparedDeploy {
        client,
        outcome,
        plugin_name,
        label,
        env: env.to_string(),
        requested_repo: opts.repo,
        deploy_targets_yaml,
        connect_hint: opts.connect_hint,
        step,
        tier: opts.tier,
        plugin_dir,
    })
}

/// The operator-facing text for a repository whose pushes no longer route (#1221).
///
/// Pure, so the wording is unit-testable without a platform. Three things it
/// must do and one it must not:
///
/// - name the repository and every agent now bound to it, because the operator
///   who just deployed one of them cannot otherwise tell who else is affected;
/// - carry the resolver's own `message` VERBATIM -- paraphrasing it would put a
///   second statement of the routing rule in the client, free to drift from the
///   one `gitflow.py` enforces on a push (the drift #1212 exists to correct);
/// - say plainly that the affected pushes break for EVERY agent bound to the
///   repository, including ones this deploy never touched, since the surprise
///   is that a working agent breaks without being deployed to.
///
/// What it must NOT do is widen the damage past what the resolver reported. A
/// bundle can declare a valid `dev` target and a `prod` target naming an agent
/// that does not exist: the answer comes back unresolvable carrying ONLY the
/// prod problem, while dev pushes still deploy exactly as before. Claiming
/// every push is rejected would be false there, and appending a fixed "declare
/// a target" remedy would be worse than false -- targets already exist, and the
/// real remedy is the one the resolver named in its own `message`. So the
/// affected environments are read off `unresolvable`, and no generic remedy is
/// appended at all: each problem's own text carries the fix for its own code
/// (`deploy.no_targets` already ends with "Declare a target (ADR-0089)."), and
/// a hardcoded second remedy could only contradict it.
fn routing_warning(check: &RoutingCheck) -> String {
    let agents = if check.agents.is_empty() {
        "(none)".to_string()
    } else {
        check.agents.join(", ")
    };
    // Environments as the resolver named them, deduplicated in the order it
    // sent them. A problem with a blank `environment` (the field is
    // `serde(default)`) contributes no name rather than an empty one.
    let mut envs: Vec<&str> = Vec::new();
    for problem in &check.unresolvable {
        let env = problem.environment.trim();
        if !env.is_empty() && !envs.contains(&env) {
            envs.push(env);
        }
    }
    // With no environment named, the response says routing is broken without
    // saying where. Staying vague is the honest rendering: inventing a list
    // would be the same overstatement this function exists to avoid.
    let scope = if envs.is_empty() {
        "some pushes to this repository no longer deploy anything".to_string()
    } else {
        format!(
            "pushes that deploy the {} environment{} no longer deploy anything",
            envs.join(", "),
            if envs.len() == 1 { "" } else { "s" }
        )
    };
    let mut lines = vec![format!(
        "git-flow routing for {} is broken: {} agents are bound to it ({agents}), and {scope} \
         -- for every one of those agents, including agents this deploy did not touch.",
        check.repo_full_name, check.agent_count
    )];
    for problem in &check.unresolvable {
        lines.push(format!(
            "  {} ({}): {}",
            problem.environment, problem.code, problem.message
        ));
    }
    lines.join("\n")
}

pub async fn deploy_prepared(prepared: PreparedDeploy) -> Result<DeployOutput> {
    let ui = crate::ui::ui();
    let PreparedDeploy {
        client,
        outcome,
        plugin_name,
        label,
        env,
        requested_repo,
        deploy_targets_yaml,
        connect_hint,
        step,
        tier,
        plugin_dir,
    } = prepared;
    let outcome = match client.activate_deploy(outcome, &env).await {
        Ok(outcome) => {
            step.done(&env);
            outcome
        }
        Err(err) => {
            step.fail("failed");
            if crate::exit::is_transient_reqwest(&err) {
                return Err(crate::exit::operator_context(err, connect_hint, None));
            }
            return Err(err);
        }
    };

    // A declined --repo is otherwise silent: the deploy succeeds, the agent
    // looks fine, and the operator believes the rebind took (#1064, #1212).
    if let Some(note) = &outcome.repo_note {
        ui.warn(note);
    }

    // An APPLIED --repo was equally silent: writing the field that decides
    // which repository's pushes deploy this agent produced no output at all.
    // The value read here is the one the API stored, not the one asked for, so
    // a bind the platform dropped falls to the warning above instead (#1212).
    let bound_repo = requested_repo
        .as_deref()
        .filter(|want| outcome.agent.repo_full_name.as_deref() == Some(*want));
    if let Some(repo) = bound_repo {
        ui.note(&format!(
            "repo binding: git-flow pushes to {repo} deploy this agent"
        ));
        // ...unless they no longer route anywhere (#1221). Migration 0018
        // (ADR-0091) dropped the unique index on `repo_full_name`, so a SECOND
        // agent may bind the same repository -- and with no declared targets
        // that silently flips every future push for the agent that was ALREADY
        // bound from "deploys" to "rejected". This is the one point BOTH
        // binding paths converge on: an agent bound at creation and one bound
        // by a later PATCH (#1212) both arrive here.
        //
        // The API answers, because the API owns the resolver. Asking it is what
        // keeps this warning from becoming a second copy of the routing rule,
        // free to drift from the one a push actually enforces. It is advisory
        // in every direction: an older platform, an unreachable one, or an
        // undecodable answer all print nothing rather than souring a deploy
        // that already succeeded.
        if let Ok(Some(check)) = client
            .check_git_flow_routing(repo, deploy_targets_yaml.as_deref())
            .await
        {
            if !check.resolvable {
                ui.warn(&routing_warning(&check));
            }
        }
    }

    let channel = match &outcome.channel {
        ChannelOutcome::Created(channel) => channel.clone(),
        ChannelOutcome::Added { address } => format!("added {address}"),
        ChannelOutcome::Unchanged { channels, passed } => {
            let bound = channels.join(", ");
            if *passed {
                format!("unchanged ({bound})")
            } else {
                format!("unchanged ({bound}); pass --slack-channel to bind another")
            }
        }
    };
    // The local tier runs the bundle's connectors itself (ADR 0113). Reached by
    // both local callers -- `local deploy` and `deploy-local` -- because both
    // arrive here, so the shorthand cannot upload a source-built bundle and
    // start no connector. The identity is the one the deploy RESOLVED, never
    // re-read from plugin.json: `--agent`/`--target` can override it, and an
    // alias built from the manifest name is one the runner never dials.
    if tier == DeployTier::Local {
        let lock = crate::connector_build::load_lock(&plugin_dir)?.unwrap_or_else(|| {
            crate::connector_build::ConnectorLockFileDecl {
                version: crate::connector_build::LOCK_VERSION,
                connectors: std::collections::BTreeMap::new(),
            }
        });
        let identity = crate::connector_build::ConnectorScope {
            release: "curie".to_string(),
            agent: outcome.agent.name.clone(),
            namespace: "default".to_string(),
        };
        bring_up_local(&plugin_dir, &lock, &identity, crate::local::COMPOSE_PROJECT).await?;
    }

    Ok(DeployOutput {
        plugin_name,
        label,
        env,
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

pub async fn deploy(opts: DeployOpts) -> Result<DeployOutput> {
    deploy_with_commit_sha(opts, None).await
}

/// Installer-only deploy entry point. A stamped binary commit takes precedence
/// over bundle-directory Git discovery; `None` preserves ordinary deploy
/// behavior for no-Git builds.
pub(crate) async fn deploy_with_commit_sha(
    opts: DeployOpts,
    installer_commit_sha: Option<&str>,
) -> Result<DeployOutput> {
    deploy_prepared(prepare_deploy_with_commit_sha(opts, installer_commit_sha).await?).await
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

/// One completed entry in a deploy across every declared target, retaining the
/// target name beside the full deploy result.
#[derive(Debug)]
pub struct AllTargetsDeployResult {
    pub target: String,
    pub result: DeployOutput,
}

impl AllTargetsDeployResult {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "target": self.target,
            "result": <DeployOutput as crate::ui::CliOutput>::to_json(&self.result),
        })
    }
}

/// Ordered result of a cluster deploy across every declared target.
#[derive(Debug)]
pub struct AllTargetsDeployOutput {
    pub results: Vec<AllTargetsDeployResult>,
}

impl crate::ui::CliOutput for AllTargetsDeployOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "results": self
                .results
                .iter()
                .map(AllTargetsDeployResult::to_json)
                .collect::<Vec<_>>(),
        })
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if let Some(last) = self.results.last() {
            <DeployOutput as crate::ui::CliOutput>::render(&last.result, ui);
        }
    }
}

/// Build the reconciliation payload for a failed cluster deploy across every
/// target. A connector failure includes the completed deploy result.
pub fn all_targets_deploy_failure_json(
    failed_target: &str,
    completed: &[AllTargetsDeployResult],
    failed_result: Option<&DeployOutput>,
    err: &anyhow::Error,
) -> serde_json::Value {
    let completed = completed
        .iter()
        .map(AllTargetsDeployResult::to_json)
        .collect::<Vec<_>>();
    let fix = crate::exit::classify(err).1;

    match failed_result {
        Some(result) => serde_json::json!({
            "failed_target": failed_target,
            "stage": "connector_sync",
            "completed": completed,
            "failed_result": <DeployOutput as crate::ui::CliOutput>::to_json(result),
            "error": format!("{err:#}"),
            "fix": fix,
        }),
        None => serde_json::json!({
            "failed_target": failed_target,
            "stage": "deploy",
            "completed": completed,
            "error": format!("{err:#}"),
            "fix": fix,
        }),
    }
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

/// `curie <tier> delete <agent> --yes`: end active deployments, then delete the
/// agent. Destructive and irreversible, so it refuses without `--yes`.
/// `--dry-run` returns the plan and makes no request.
pub async fn delete(opts: AgentActionOpts, yes: bool) -> Result<DeleteOutput> {
    let ui = crate::ui::ui();
    if opts.dry_run {
        return Ok(DeleteOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![
                format!(
                    "GET {}/agents  (would resolve agent {:?})",
                    opts.api_url, opts.agent
                ),
                format!("GET {}/deployments?agent_id=<id>", opts.api_url),
                format!(
                    "DELETE {}/deployments/<id>  (for each active deployment)",
                    opts.api_url
                ),
                format!("DELETE {}/agents/<id>", opts.api_url),
            ],
        }));
    }
    if !yes {
        return Err(crate::exit::CliError::usage(format!(
            "`curie ... delete {}` permanently deletes the agent; re-run with --yes to confirm",
            opts.agent
        ))
        .with_fix("re-run with --yes")
        .into());
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let cl = ui.checklist();
    for deployment in client.list_deployments(&agent.id).await? {
        if deployment.status == "active" {
            let step = cl.step(&format!("ending deployment {}", deployment.id));
            if let Err(err) = client.end_deployment(&deployment.id).await {
                step.fail("failed");
                return Err(err);
            }
            step.done("ended");
        }
    }
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
/// A minted console login code, and how to use it.
///
/// ADR-0083: the operator copies a CODE into the console, never the platform
/// key. Under `--json` the code is a field like any other; the human render
/// spells out the exchange, because a bare token with no instruction is how
/// people end up pasting the wrong thing into the wrong box.
#[derive(Debug)]
pub enum ConsoleLoginOutput {
    DryRun(crate::ui::DryRunPlan),
    Minted {
        code: String,
        expires_at: String,
        console_url: String,
    },
}

impl crate::ui::CliOutput for ConsoleLoginOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            Self::DryRun(plan) => plan.to_json(),
            Self::Minted {
                code,
                expires_at,
                console_url,
            } => serde_json::json!({
                "code": code,
                "expires_at": expires_at,
                "console_url": console_url,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            Self::DryRun(plan) => plan.render(ui),
            Self::Minted {
                code,
                expires_at,
                console_url,
            } => {
                ui.kv("login code", code);
                ui.kv("expires", expires_at);
                ui.payload(&format!(
                    "Open {console_url} and paste the code to sign in."
                ));
                ui.note("Single use, and not the platform key: it authorizes this browser only.");
            }
        }
    }
}

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
/// Mint a console login code (ADR-0083).
///
/// The key is used HERE, by the CLI, and the operator carries away a code. That
/// asymmetry is the decision: a browser that never receives the platform key
/// cannot leak it through history, a referrer, or a screenshot.
pub async fn console_login(
    api_url: String,
    api_key: String,
    subject: String,
    console_url: String,
    dry_run: bool,
) -> Result<ConsoleLoginOutput> {
    if dry_run {
        return Ok(ConsoleLoginOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {api_url}/console/login-codes  (subject {subject})"
            )],
        }));
    }
    let client = ApiClient::new(&api_url, &api_key)?;
    // The subject-bound mint, which is the only one the API still accepts: a
    // code now carries who it is for, so the session it becomes is an identity
    // rather than an anonymous grant.
    let minted = client.mint_console_login_code(&subject).await?;
    Ok(ConsoleLoginOutput::Minted {
        code: minted.code,
        expires_at: minted.expires_at,
        console_url,
    })
}

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

/// Output of `<tier> memory <agent>`: the dry-run plan, the empty case, the
/// learned-memory list, or an operator-seeded add (#1904). Owns its data so it
/// outlives the `ApiClient`.
#[derive(Debug)]
pub enum MemoryOutput {
    DryRun(crate::ui::DryRunPlan),
    Empty {
        agent: String,
    },
    List {
        agent: String,
        entries: Vec<crate::api::MemoryEntry>,
    },
    Added {
        agent: String,
        index: u64,
        content: String,
        source: String,
        fresh_session_required: bool,
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
            MemoryOutput::Added {
                agent,
                index,
                content,
                source,
                fresh_session_required,
            } => serde_json::json!({
                "agent": agent,
                "index": index,
                "content": content,
                "source": source,
                "fresh_session_required": fresh_session_required,
            }),
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
            MemoryOutput::Added {
                agent,
                index,
                content,
                source,
                fresh_session_required,
            } => {
                ui.payload(&format!("{agent} — added memory #{index} ({source})"));
                ui.kv("content", content);
                if *fresh_session_required {
                    ui.payload(
                        "A fresh session is required before this entry is injected at boot.",
                    );
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

/// `<tier> memory <agent> --add <content>`: append an operator-authored record.
pub async fn memory_add(opts: AgentActionOpts, content: String) -> Result<MemoryOutput> {
    let content = content.trim().to_string();
    if content.is_empty() {
        return Err(crate::exit::usage(
            "memory content must not be empty. Pass the durable lesson with --add.",
        ));
    }
    if opts.dry_run {
        return Ok(MemoryOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![format!(
                "POST {}/agents/<id>/memory  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            )],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let entry = client.create_memory(&agent.id, &content).await?;
    Ok(MemoryOutput::Added {
        agent: agent.name,
        index: entry.index,
        content: entry.content,
        source: entry
            .provenance
            .source
            .unwrap_or_else(|| "operator".to_string()),
        fresh_session_required: true,
    })
}

/// The pending-list / resolve flags for `local approvals` (#506). Defaulted so
/// the skill/cluster tiers, which keep only the gate view/set surface, pass an
/// empty value.
#[derive(Default)]
pub struct ApprovalCmd {
    pub list: bool,
    pub resolve: Option<String>,
    pub reject: bool,
    pub note: Option<String>,
    /// Administrative subject for a reusable operator-principal mint.
    pub mint_operator_principal: Option<String>,
    /// Administrative subject for a single-use console login-code mint.
    pub mint_console_login_code: Option<String>,
    /// `--route-resolution NAME=CHANNEL`, repeatable.
    pub route_resolution: Vec<String>,
    /// `--route-approvers NAME=users:U1,U2` or `NAME=group:S1`, repeatable.
    pub route_approvers: Vec<String>,
    /// `--routes-from FILE`: the whole binding map as JSON.
    pub routes_from: Option<PathBuf>,
    pub list_routes: bool,
    pub clear_routes: bool,
}

// --- Approval route bindings (#1052) -----------------------------------------
//
// The verified resolution card, optional text-only notification, and who may
// resolve live as separate fields in the agent's `approval_routes` map. Until
// this verb existed the only way to write one was a hand-rolled
// `PATCH /agents/{id}`, against the repo's own "one entry point: curie
// <command>" rule.
//
// Two properties shape the code below.
//
// The write is a FULL REPLACEMENT, exactly as `--gate` already is for
// `approval_required_tools`. That is the field's semantics on `AgentUpdate`, and
// a merge would make the route inputs unable to express removal.
//
// Every parse and shape error is collected BEFORE any HTTP call. A partial write
// of a binding map is a silently widened (or silently narrowed) approver set,
// which is the failure ADR-0034 fails closed against, so a malformed entry
// anywhere aborts the whole invocation with nothing sent.

/// Slack ID shapes, mirroring the authoritative validators in
/// `apps/api/src/curie_api/schemas.py`. The API is the gate for every caller and
/// re-checks all of these; these exist only so a typo is answered locally with a
/// fix hint instead of a round trip (the same split the API's own
/// `_validate_channel_binding` docstring describes).
static SLACK_USERGROUP_ID: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^S[A-Z0-9]{7,}$").expect("usergroup id re"));
static SLACK_USER_ID: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^[UW][A-Z0-9]{7,}$").expect("user id re"));
static CHANNEL_KIND: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"^[a-z0-9]+(?:[-_][a-z0-9]+)*$").expect("channel kind re")
});

/// Split `NAME=VALUE` once, rejecting an empty half.
///
/// Splits at the FIRST `=` so a value containing one is preserved; a route name
/// containing `=` is not expressible, which is fine because a route name is a
/// manifest identifier matched verbatim against `approvalPolicy`.
fn split_route_arg<'a>(flag: &str, raw: &'a str) -> Result<(&'a str, &'a str)> {
    let (name, value) = raw.split_once('=').ok_or_else(|| {
        crate::exit::usage(format!(
            "{flag} {raw:?} is not NAME=VALUE; pass e.g. {flag} deal_desk=C0EXAMPLE1"
        ))
    })?;
    let (name, value) = (name.trim(), value.trim());
    if name.is_empty() {
        return Err(crate::exit::usage(format!(
            "{flag} {raw:?} has an empty route name; the name must match a route the \
             bundle manifest's approvalPolicy declares"
        )));
    }
    if value.is_empty() {
        return Err(crate::exit::usage(format!(
            "{flag} {raw:?} has an empty value"
        )));
    }
    Ok((name, value))
}

/// Parse one `--route-approvers NAME=KIND:VALUES` value into its approver set.
fn parse_route_approvers(value: &str) -> Result<crate::api::ApprovalApprovers> {
    let (kind, rest) = value.split_once(':').ok_or_else(|| {
        crate::exit::usage(format!(
            "--route-approvers value {value:?} is not KIND:VALUES; pass \
             users:U0123ABCD,U0456DEFG or group:S0123ABCD"
        ))
    })?;
    match kind.trim() {
        "users" => {
            let users: Vec<String> = rest
                .split(',')
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty())
                .collect();
            if users.is_empty() {
                // The API refuses an empty list for the same reason: as silent
                // config, "nobody may approve" means the request can only expire.
                return Err(crate::exit::usage(
                    "--route-approvers users: needs at least one Slack user ID",
                ));
            }
            for user in &users {
                if !SLACK_USER_ID.is_match(user) {
                    return Err(crate::exit::CliError::usage(format!(
                        "approvers user {user:?} is not a Slack user ID"
                    ))
                    .with_fix(
                        "pass the user ID (e.g. U0123ABCD, or W0123ABCD on enterprise \
                         grid), not a @handle or a display name: a member's ID is under \
                         their profile's More menu, Copy member ID",
                    )
                    .into());
                }
            }
            Ok(crate::api::ApprovalApprovers {
                group: None,
                users: Some(users),
            })
        }
        "group" => {
            let group = rest.trim().to_string();
            if !SLACK_USERGROUP_ID.is_match(&group) {
                // Naming the C-prefix case explicitly: a channel ID here is the
                // likely mistake, and it is the one that would otherwise look
                // plausible enough to debug for a while.
                return Err(crate::exit::CliError::usage(format!(
                    "approvers group {group:?} is not a Slack user-group ID"
                ))
                .with_fix(
                    "pass a user-group ID (e.g. S0123ABCD), not a @handle or a name; a \
                     C-prefixed value is a CHANNEL, not a user group. List group IDs via \
                     the Slack usergroups.list API",
                )
                .into());
            }
            Ok(crate::api::ApprovalApprovers {
                group: Some(group),
                users: None,
            })
        }
        other => Err(crate::exit::usage(format!(
            "--route-approvers kind {other:?} is not recognized; pass `users:` or `group:`"
        ))),
    }
}

/// Build the binding map a write should send from a strict file plus overrides.
///
/// `--routes-from` seeds the map and the repeatable flags apply on top, so a
/// committed file can be spot-overridden on the command line. Every error is a
/// usage error raised before the caller opens a connection.
fn build_route_bindings(
    route_resolution: &[String],
    route_approvers: &[String],
    routes_from: Option<&PathBuf>,
) -> Result<BTreeMap<String, crate::api::ApprovalRouteBindingWrite>> {
    let mut bindings: BTreeMap<String, crate::api::ApprovalRouteBindingWrite> = BTreeMap::new();

    if let Some(path) = routes_from {
        let text = std::fs::read_to_string(path).map_err(|e| {
            crate::exit::usage(format!(
                "--routes-from {}: {e}; the file must be JSON shaped \
                 {{\"<route>\": {{\"resolution\": \
                 {{\"kind\": \"slack\", \"address\": \"C0EXAMPLE1\"}}}}}}",
                path.display()
            ))
        })?;
        // Decode in two steps rather than straight into the binding map, so an
        // unknown key can be reported against the ROUTE that carries it. Serde's
        // own error names the key but not which entry of the map it came from,
        // and "unknown field `approver`" with no route is a poor thing to hand
        // someone holding a twenty-route file.
        let raw: BTreeMap<String, serde_json::Value> =
            serde_json::from_str(&text).map_err(|e| {
                crate::exit::usage(format!(
                    "--routes-from {}: {e}; expected JSON shaped \
                     {{\"<route>\": {{\"resolution\": \
                     {{\"kind\": \"slack\", \"address\": \"C0EXAMPLE1\"}}, \
                     \"approvers\": {{\"group\": \"S0123ABCD\"}}}}}}",
                    path.display()
                ))
            })?;
        for (name, value) in raw {
            // RouteBindingInput is the strict, operator-file twin of the
            // response-side type: it refuses an unknown key instead of dropping
            // it (#1072). A dropped `approver` would leave authority falling
            // back to the whole resolution-card channel.
            let input: crate::api::RouteBindingInput =
                serde_json::from_value(value).map_err(|e| {
                    anyhow::Error::from(
                        crate::exit::CliError::usage(format!(
                            "--routes-from {}: route {name:?}: {e}",
                            path.display()
                        ))
                        .with_fix(
                            "a route binding requires `resolution: {kind: \"slack\", \
                             address: \"C...\"}` and accepts optional `notification` and \
                             `approvers` blocks, and nothing else. The retired `channel` key \
                             is not accepted",
                        ),
                    )
                })?;
            let binding: crate::api::ApprovalRouteBindingWrite = input.into();
            if let Some(approvers) = &binding.approvers {
                validate_parsed_approvers(&name, approvers)?;
            }
            bindings.insert(name, binding);
        }
    }

    for raw in route_resolution {
        let (name, channel) = split_route_arg("--route-resolution", raw)?;
        bindings
            .entry(name.to_string())
            .and_modify(|b| {
                b.resolution = crate::api::ApprovalResolutionTargetWrite {
                    kind: "slack".to_string(),
                    address: channel.to_string(),
                }
            })
            .or_insert_with(|| crate::api::ApprovalRouteBindingWrite {
                resolution: crate::api::ApprovalResolutionTargetWrite {
                    kind: "slack".to_string(),
                    address: channel.to_string(),
                },
                notification: None,
                approvers: None,
            });
    }

    for raw in route_approvers {
        let (name, value) = split_route_arg("--route-approvers", raw)?;
        let approvers = parse_route_approvers(value)?;
        // Approvers narrow an EXISTING binding; without a resolution there is
        // no verified card surface. Refuse rather than invent one.
        let binding = bindings.get_mut(name).ok_or_else(|| {
            anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "--route-approvers {name:?} names a route with no resolution"
                ))
                .with_fix(format!(
                    "add --route-resolution {name}=<CHANNEL> to the same invocation (or name the \
                     route in --routes-from): a write replaces the whole route map, so \
                     every route it should keep must be present"
                )),
            )
        })?;
        binding.approvers = Some(approvers);
    }

    // Overrides can invalidate a previously valid file binding (for example,
    // by moving resolution onto its notification target), so validate only the
    // complete final map. Nothing reaches HTTP unless every binding survives.
    for (name, binding) in &bindings {
        validate_resolution_target(name, &binding.resolution)?;
        if let Some(notification) = &binding.notification {
            validate_notification_target(name, notification)?;
            reject_identical_targets(name, &binding.resolution, notification)?;
        }
        if let Some(approvers) = &binding.approvers {
            validate_parsed_approvers(name, approvers)?;
        }
    }

    Ok(bindings)
}

/// The interactive extension point is explicit but remains Slack-only until a
/// second channel can mint a scoped, verified resolver credential.
fn validate_resolution_target(
    route: &str,
    target: &crate::api::ApprovalResolutionTargetWrite,
) -> Result<()> {
    if target.kind != "slack" {
        return Err(crate::exit::usage(format!(
            "route {route:?}: resolution kind {:?} is unsupported; only slack can carry \
             a verified resolver identity",
            target.kind
        )));
    }
    validate_route_channel(route, &target.address)
}

/// Validate the notification identity and its independent transport route.
fn validate_notification_target(
    route: &str,
    target: &crate::api::NotificationTargetWrite,
) -> Result<()> {
    if !CHANNEL_KIND.is_match(&target.kind) {
        return Err(crate::exit::usage(format!(
            "route {route:?}: notification kind {:?} is not a lowercase channel-kind slug",
            target.kind
        )));
    }
    if target.kind == "slack" {
        validate_route_channel(route, &target.address)?;
    } else if target.address.is_empty() || target.address.chars().any(char::is_whitespace) {
        return Err(crate::exit::usage(format!(
            "route {route:?}: notification address must be non-empty and contain no whitespace"
        )));
    }
    let complete_transport = target.endpoint.is_some() && target.adapter.is_some();
    let empty_transport = target.endpoint.is_none() && target.adapter.is_none();
    if !complete_transport && !empty_transport {
        return Err(crate::exit::usage(format!(
            "route {route:?}: notification endpoint and adapter must be supplied together"
        )));
    }
    if target.kind != "slack" && !complete_transport {
        return Err(crate::exit::CliError::usage(format!(
            "route {route:?}: non-Slack notification kind {:?} requires both endpoint and adapter",
            target.kind
        ))
        .with_fix(
            "put the complete notification object in --routes-from, including an absolute \
             endpoint URL and adapter name",
        )
        .into());
    }
    if let Some(adapter) = &target.adapter {
        if !CHANNEL_KIND.is_match(adapter) {
            return Err(crate::exit::usage(format!(
                "route {route:?}: notification adapter is not a lowercase slug"
            )));
        }
    }
    if let Some(endpoint) = &target.endpoint {
        let parsed = reqwest::Url::parse(endpoint).map_err(|_| {
            crate::exit::usage(format!(
                "route {route:?}: notification endpoint must be an absolute http(s) URL with a host"
            ))
        })?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return Err(crate::exit::usage(format!(
                "route {route:?}: notification endpoint must be an absolute http(s) URL with a host"
            )));
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(crate::exit::usage(format!(
                "route {route:?}: notification endpoint must not contain userinfo; use adapter credentials"
            )));
        }
    }
    Ok(())
}

fn reject_identical_targets(
    route: &str,
    resolution: &crate::api::ApprovalResolutionTargetWrite,
    notification: &crate::api::NotificationTargetWrite,
) -> Result<()> {
    if resolution.kind == notification.kind && resolution.address == notification.address {
        return Err(crate::exit::usage(format!(
            "route {route:?}: notification must differ from resolution; a duplicate target \
             is not a second notification surface"
        )));
    }
    Ok(())
}

/// Render route bindings for a dry-run without exposing credential-bearing
/// adapter URL paths, queries, or fragments. The origin is enough to identify
/// the transport destination; the real PATCH retains the complete endpoint.
fn route_bindings_plan_json(
    bindings: &BTreeMap<String, crate::api::ApprovalRouteBindingWrite>,
) -> String {
    let mut display = bindings.clone();
    for binding in display.values_mut() {
        let Some(endpoint) = binding
            .notification
            .as_mut()
            .and_then(|target| target.endpoint.as_mut())
        else {
            continue;
        };
        *endpoint = reqwest::Url::parse(endpoint)
            .map(|url| url.origin().ascii_serialization())
            .unwrap_or_else(|_| "<redacted>".to_string());
    }
    serde_json::to_string(&display).unwrap_or_else(|_| "<unserializable>".to_string())
}

/// Channel-shape check for one route binding, with the route named in the error.
fn validate_route_channel(route: &str, channel: &str) -> Result<()> {
    if !crate::credcheck::looks_like_slack_channel_id(channel) {
        return Err(crate::exit::CliError::usage(format!(
            "route {route:?}: {channel:?} is not a Slack channel ID. Real Slack events \
             carry the ID and the worker routes on it, so a #name binding never \
             receives messages"
        ))
        .with_fix(
            "pass the channel ID (e.g. C0EXAMPLE1): find it in the channel's About tab, \
             or at the end of the channel URL (.../archives/C0EXAMPLE1)",
        )
        .into());
    }
    Ok(())
}

/// Re-run the flag-path approver checks over a `--routes-from` block, so the two
/// input forms cannot disagree about what a valid binding is.
fn validate_parsed_approvers(route: &str, approvers: &crate::api::ApprovalApprovers) -> Result<()> {
    if approvers.group.is_none() && approvers.users.is_none() {
        return Err(crate::exit::usage(format!(
            "route {route:?}: an approvers block must declare group or users; omit the \
             block entirely to keep card-channel membership"
        )));
    }
    if let Some(group) = &approvers.group {
        if !SLACK_USERGROUP_ID.is_match(group) {
            return Err(crate::exit::usage(format!(
                "route {route:?}: approvers group {group:?} is not a Slack user-group ID \
                 (e.g. S0123ABCD); a C-prefixed value is a channel, not a user group"
            )));
        }
    }
    if let Some(users) = &approvers.users {
        if users.is_empty() {
            return Err(crate::exit::usage(format!(
                "route {route:?}: approvers users, when present, must contain at least \
                 one user ID"
            )));
        }
        for user in users {
            if !SLACK_USER_ID.is_match(user) {
                return Err(crate::exit::usage(format!(
                    "route {route:?}: approvers user {user:?} is not a Slack user ID \
                     (e.g. U0123ABCD, or W0123ABCD on enterprise grid)"
                )));
            }
        }
    }
    Ok(())
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
    /// One-time delivery of a reusable operator credential. The token is never
    /// persisted and is emitted only from this explicit mint result.
    OperatorPrincipal {
        delivery: crate::api::OperatorPrincipalDelivery,
    },
    /// One-time delivery of a subject-bound console login code.
    ConsoleLoginCode {
        delivery: crate::api::ConsoleLoginCodeDelivery,
    },
    /// The agent's approval route bindings, read back after `--list-routes` or
    /// after a write. `routes` empty means no route is bound, so any route the
    /// bundle names escalates to a human rather than posting a card.
    Routes {
        agent: String,
        routes: BTreeMap<String, crate::api::ApprovalRouteBindingResponse>,
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
        // #1078: the one field --resolve cannot be driven without. Null only
        // for an older row or a direct API write that omitted this field.
        "card_channel": r.card_channel,
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
            ApprovalsOutput::OperatorPrincipal { delivery } => serde_json::json!({
                "operator_principal": {
                    "token": delivery.token,
                    "subject": delivery.subject,
                    "expires_at": delivery.expires_at,
                },
            }),
            ApprovalsOutput::ConsoleLoginCode { delivery } => serde_json::json!({
                "console_login_code": {
                    "code": delivery.code,
                    "subject": delivery.subject,
                    "expires_at": delivery.expires_at,
                },
            }),
            ApprovalsOutput::Routes { agent, routes } => serde_json::json!({
                "agent": agent,
                "routes": routes,
                "count": routes.len(),
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
                        // A null card_channel means an older row or a direct API
                        // write that omitted the field, for which the requesting
                        // channel applies (#1431); it must NOT render as "null",
                        // "none" or "-", which would state the wrong fact.
                        // The server picks approvers from `card_channel or
                        // reply_channel`, and in Python only the empty string
                        // is falsy, so an empty card_channel is absent too and
                        // must show the same requesting-channel meaning as the
                        // resolve hint in message.rs. A whitespace-only channel
                        // is truthy in Python and is NOT absent, so it is still
                        // printed verbatim here; do not trim it.
                        let card = r
                            .card_channel
                            .as_deref()
                            .filter(|c| !c.is_empty())
                            .unwrap_or("(requesting channel)");
                        ui.kv(
                            &r.id,
                            &format!(
                                "{} — {} [tool: {tool}, route: {route}, channel: {card}, by: {}]",
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
            ApprovalsOutput::OperatorPrincipal { delivery } => {
                ui.note(&format!(
                    "minted an operator approval principal for {} (expires {}); assign the \
                     token to CURIE_APPROVAL_PRINCIPAL_TOKEN without pasting it into shell history",
                    delivery.subject, delivery.expires_at
                ));
                // This explicit mint result is the token's one delivery. Keep
                // it off progress/error output and do not cache it.
                ui.payload_plain(&delivery.token);
            }
            ApprovalsOutput::ConsoleLoginCode { delivery } => {
                ui.note(&format!(
                    "minted a console login code for {} (expires {})",
                    delivery.subject, delivery.expires_at
                ));
                // The browser exchanges this plaintext once; the CLI never
                // stores or repeats it.
                ui.payload_plain(&delivery.code);
            }
            ApprovalsOutput::Routes { agent, routes } => {
                if routes.is_empty() {
                    // Not a neutral empty list: with nothing bound, a route the
                    // bundle names is escalated to a human instead of posting a
                    // card, which is the state operators most often mistake for
                    // "approvals are broken".
                    ui.payload(&format!(
                        "{agent}: no approval routes bound. Any route the bundle's \
                         approvalPolicy names will escalate to a human rather than post \
                         a card, since there is no channel to post it to"
                    ));
                } else {
                    ui.payload(&format!(
                        "{agent} — {} approval route(s) bound:",
                        routes.len()
                    ));
                    for (name, binding) in routes {
                        let resolution =
                            format!("{}:{}", binding.resolution.kind, binding.resolution.address);
                        ui.kv(
                            name,
                            &format!("resolution {resolution} (verified interactive card)"),
                        );
                        let notification = binding
                            .notification
                            .as_ref()
                            .map(|target| format!("{}:{} (text-only)", target.kind, target.address))
                            .unwrap_or_else(|| "(none)".to_string());
                        ui.kv("", &format!("  notification: {notification}"));
                        ui.kv("", &format!("  approvers: {}", describe_approvers(binding)));
                    }
                }
            }
        }
    }
}

/// One line naming who may resolve a route's approvals, including the default.
fn describe_approvers(binding: &crate::api::ApprovalRouteBindingResponse) -> String {
    match &binding.approvers {
        None => format!(
            "members of {}:{} (the default: no approvers block declared)",
            binding.resolution.kind, binding.resolution.address
        ),
        Some(a) => match (&a.users, &a.group) {
            // Mirror the API's precedence in the wording rather than hiding it:
            // `users` wins over `group`, so a binding carrying both must not read
            // as though the group also decides.
            (Some(users), Some(group)) => format!(
                "users {} (an explicit list wins over group {group}; the click channel is ignored)",
                users.join(", ")
            ),
            (Some(users), None) => {
                format!("users {} (the click channel is ignored)", users.join(", "))
            }
            (None, Some(group)) => {
                format!("members of Slack user group {group} (the click channel is ignored)")
            }
            (None, None) => "unreadable: the block declares neither users nor group".to_string(),
        },
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

    // The split target/approver flags address the agent's approval route map.
    // Handled ahead of every other branch because it is a distinct
    // object from both the tool gates and the pending records, and mixing it with
    // either in one invocation would make the write's replace-the-whole-map
    // semantics ambiguous.
    let route_write = !cmd.route_resolution.is_empty()
        || !cmd.route_approvers.is_empty()
        || cmd.routes_from.is_some()
        || cmd.clear_routes;

    let mint_subject = match (
        cmd.mint_operator_principal.as_deref(),
        cmd.mint_console_login_code.as_deref(),
    ) {
        (Some(_), Some(_)) => {
            return Err(crate::exit::usage(
                "--mint-operator-principal and --mint-console-login-code are separate \
                 administrative actions; run one per invocation",
            ));
        }
        (Some(subject), None) => Some(("operator", subject)),
        (None, Some(subject)) => Some(("console", subject)),
        (None, None) => None,
    };
    if let Some((kind, subject)) = mint_subject {
        if subject.trim().is_empty() {
            return Err(crate::exit::usage(
                "the principal subject must not be blank",
            ));
        }
        if gate_mode
            || cmd.list
            || cmd.resolve.is_some()
            || route_write
            || cmd.list_routes
            || cmd.reject
            || cmd.note.is_some()
        {
            return Err(crate::exit::usage(
                "principal bootstrap cannot be combined with gate, route, pending-list, \
                 resolution, reject, or note flags; run it as a separate invocation",
            ));
        }
        if opts.dry_run {
            let endpoint = if kind == "operator" {
                "approvals/principals/operator"
            } else {
                "console/login-codes"
            };
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![format!(
                    "POST {}/{endpoint} subject={subject:?} (one-time credential is returned only by a real run)",
                    opts.api_url
                )],
            }));
        }
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        return if kind == "operator" {
            Ok(ApprovalsOutput::OperatorPrincipal {
                delivery: client.mint_operator_principal(subject).await?,
            })
        } else {
            Ok(ApprovalsOutput::ConsoleLoginCode {
                delivery: client.mint_console_login_code(subject).await?,
            })
        };
    }

    if route_write || cmd.list_routes {
        if gate_mode || cmd.list || cmd.resolve.is_some() {
            return Err(crate::exit::usage(
                "the route-binding flags (--route-resolution/--route-approvers/--routes-from/\
                 --list-routes/--clear-routes) address the agent's approval ROUTES; they \
                 cannot be combined with --gate/--clear (tool gates) or --list/--resolve \
                 (pending records). Run them as separate invocations",
            ));
        }
        if route_write && cmd.list_routes {
            return Err(crate::exit::usage(
                "--list-routes reads; drop it to write, or run it as a second invocation",
            ));
        }
        if cmd.clear_routes
            && (!cmd.route_resolution.is_empty()
                || !cmd.route_approvers.is_empty()
                || cmd.routes_from.is_some())
        {
            return Err(crate::exit::usage(
                "--clear-routes cannot be combined with --route-resolution/\
                 --route-approvers/--routes-from \
                 (clear removes every binding)",
            ));
        }

        // Parse and validate EVERYTHING before any network call, so a malformed
        // entry can never leave a half-written binding map behind.
        let bindings = if cmd.clear_routes {
            BTreeMap::new()
        } else {
            build_route_bindings(
                &cmd.route_resolution,
                &cmd.route_approvers,
                cmd.routes_from.as_ref(),
            )?
        };

        if opts.dry_run {
            let action = if route_write {
                format!(
                    "PATCH {}/agents/<id> approval_routes={} (a FULL REPLACEMENT of the map)",
                    opts.api_url,
                    // Deliberately not valid JSON on serialization failure. "{}" is the
                    // real clear payload, so a fallback that looked like "{}" would
                    // misreport a full revocation.
                    route_bindings_plan_json(&bindings)
                )
            } else {
                format!(
                    "GET {}/agents/<id> (show approval route bindings)",
                    opts.api_url
                )
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
        let agent = if route_write {
            let cl = ui.checklist();
            let step = cl.step(&format!("updating approval routes for {}", agent.name));
            match client.set_approval_routes(&agent.id, &bindings).await {
                Ok(updated) => {
                    step.done("updated");
                    updated
                }
                Err(err) => {
                    step.fail("failed");
                    return Err(err);
                }
            }
        } else {
            agent
        };
        return Ok(ApprovalsOutput::Routes {
            agent: agent.name,
            routes: agent.approval_routes.unwrap_or_default(),
        });
    }

    // --resolve <id>: resolve one live approval record as the authenticated
    // principal from CURIE_APPROVAL_PRINCIPAL_TOKEN (#1531, ADR-0106). It is
    // id-scoped, not gate config, so it is mutually exclusive with --gate/--clear/
    // --list. Approve by default; --reject rejects.
    if let Some(approval_id) = cmd.resolve {
        if gate_mode || cmd.list {
            return Err(crate::exit::usage(
                "--resolve cannot be combined with --gate/--clear/--list",
            ));
        }
        let decision = if cmd.reject { "rejected" } else { "approved" };
        if opts.dry_run {
            return Ok(ApprovalsOutput::DryRun(crate::ui::DryRunPlan {
                lines: vec![format!(
                    "POST {}/approvals/{approval_id}/resolve decision={decision} \
                     (authenticated principal read from CURIE_APPROVAL_PRINCIPAL_TOKEN at execution)",
                    opts.api_url
                )],
            }));
        }
        let principal_token = std::env::var("CURIE_APPROVAL_PRINCIPAL_TOKEN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                crate::exit::usage(
                    "--resolve requires CURIE_APPROVAL_PRINCIPAL_TOKEN. Mint a reusable \
                     operator credential with `curie <local|cluster> approvals <AGENT> \
                     --mint-operator-principal <SUBJECT>`, export the one-time result, and retry",
                )
            })?;
        let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
        let record = client
            .resolve_approval(
                &approval_id,
                decision,
                &principal_token,
                cmd.note.as_deref(),
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

/// Reject a channel binding this CLI can cheaply prove is wrong before a round
/// trip. Dispatches on `kind`, mirroring the API's kind-dispatched
/// `_validate_channel_binding`: a kind with no local rule passes here and is
/// answered authoritatively by the API.
///
/// The `slack` arm rejects a `#name` rather than a channel ID: real Slack
/// events carry the channel **ID** (e.g. `C0EXAMPLE1`), and the worker's
/// binding resolver matches on that ID, so a `#name` value is stored verbatim
/// and never routes -- a silently dead binding. Fail the deploy up front
/// instead.
fn validate_channel_binding(kind: &str, address: &str) -> Result<()> {
    if channel_binding_never_resolves(kind, address) {
        return Err(crate::exit::usage(format!(
            "slack channel {address:?} is a name, not an ID: real Slack events carry the \
             channel ID (e.g. C0EXAMPLE1) and the worker routes on it, so a #name binding \
             never receives messages. Pass the channel ID instead -- find it in the \
             channel's About tab, or the channel URL (.../archives/C0EXAMPLE1)."
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
/// Returns the joined validator error messages on failure so the CLI's error
/// names the actual offending field/type rather than an approximation of one.
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
        .map(|e| format!("{e} (at instance path {})", e.instance_path()))
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
fn parse_manifest_gates(body: &str, source: &str) -> Result<Vec<(String, String)>> {
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
fn select_in_force_deployment(
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
    "`skill up` runs a local snapshot of the bundle on disk (its digest is on `skill status`), and nothing is deployed, so no version is assigned";
/// Where to run `versions` instead.
pub const VERSIONS_ALT: &str =
    "use `curie local versions <agent>` or `curie cluster versions <agent>` for a deployed agent";
/// Why `skill memory` cannot be answered at this tier.
pub const MEMORY_REASON: &str =
    "this tier configures no memory namespace: `skill up` never sets a memory ref, and there is no platform here to own or address one";
/// Where to run `memory` instead.
pub const MEMORY_ALT: &str =
    "use `curie local memory <agent>` or `curie cluster memory <agent>` for a deployed agent";
/// Why observability query verbs cannot be answered at this tier.
pub const OBSERVABILITY_REASON: &str =
    "the skill tier runs only a bundle runner and has no platform API or observability read service; `--otel-endpoint` can export telemetry but does not create a query API";
/// Where to query observability, or how to export telemetry from a skill runner.
pub const OBSERVABILITY_ALT: &str =
    "use `curie local observability runs|run|metrics` or `curie cluster observability runs|run|metrics`; to export this skill runner's telemetry, restart it with `curie skill up --otel-endpoint <OTLP_URL>` and query through a platform API";

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

/// `skill observability runs|run|metrics`: understood, but unavailable here.
///
/// A skill runner can emit OTLP when explicitly wired, but it does not host the
/// API read models these query verbs intentionally use. Decline with ADR-0041's
/// capability exit (4) rather than bypassing the API to query a backend.
pub fn skill_observability_unavailable() -> anyhow::Error {
    crate::exit::unsupported(
        "observability runs|run|metrics",
        OBSERVABILITY_REASON,
        OBSERVABILITY_ALT,
    )
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

/// Why the route-binding flags cannot be answered at the skill tier (#1052).
///
/// A different reason from `APPROVALS_LIST_REASON`, and the difference matters:
/// a pending record is missing because this tier runs no durable store, while a
/// route binding is missing because it is per-AGENT platform config and this
/// tier has no agent. Collapsing them would tell an operator to look in the
/// wrong place.
pub const APPROVALS_ROUTES_REASON: &str =
    "an approval route binding is per-agent platform config (agents.approval_routes), and the skill tier runs a bare runner with no platform, no agent record, and therefore nothing to bind a route on";
/// Where to bind approval routes instead.
pub const APPROVALS_ROUTES_ALT: &str =
    "use `curie local approvals <agent> --route-resolution <name>=<channel>` or `curie cluster approvals <agent> --route-resolution <name>=<channel>` for a deployed agent; use `--routes-from <file>` to declare a complete route with a text-only notification; the bundle-side half (which routes exist) is the manifest's approvalPolicy, which `curie skill approvals` does show";

/// The route-binding inputs are answered but unavailable at this tier by
/// construction (ADR-0041). Accepted so the tier reports WHY (exit 4) rather
/// than erroring like an unknown-flag typo, matching `--list`/`--resolve` above.
pub fn skill_approval_routes_unavailable() -> anyhow::Error {
    crate::exit::unsupported(
        "approvals --route-resolution/--route-approvers/--routes-from/--list-routes/--clear-routes",
        APPROVALS_ROUTES_REASON,
        APPROVALS_ROUTES_ALT,
    )
}

#[cfg(test)]
mod tests {
    use super::{
        absent_container_note, github_repo_allowlist_is_empty, merge_secret_env,
        model_credential_summary, parse_credential_env_file, parse_manifest_gates,
        plan_recorded_state, plan_recorded_teardown, plan_skill_down, recorded_ids_match,
        replace_first_line, report_sweep, resolve_cases_path, resolve_env_file_credentials,
        routing_warning, seed_env_if_missing, select_in_force_deployment, select_passthrough_env,
        sweep_json_row, sweep_table_row, validate_channel_binding, ApprovalGateDecl, DownPlan,
        EnvSeed, RecordedStatePlan, RecordedStateQuery, RecordedTeardown, SweepRow,
    };
    use serde::Deserialize;
    use serde_json::json;
    use std::path::{Path, PathBuf};

    #[test]
    fn github_repo_allowlist_is_empty_for_missing_null_and_empty_values() {
        assert!(github_repo_allowlist_is_empty(&json!({})));
        assert!(github_repo_allowlist_is_empty(&json!({"api": {}})));
        assert!(github_repo_allowlist_is_empty(
            &json!({"api": {"githubRepoAllowlist": serde_json::Value::Null}})
        ));
        assert!(github_repo_allowlist_is_empty(
            &json!({"api": {"githubRepoAllowlist": []}})
        ));
        assert!(github_repo_allowlist_is_empty(
            &json!({"api": {"githubRepoAllowlist": ["", "  "]}})
        ));
        assert!(!github_repo_allowlist_is_empty(
            &json!({"api": {"githubRepoAllowlist": ["acme-corp/acme-bot"]}})
        ));
        assert!(!github_repo_allowlist_is_empty(
            &json!({"api": {"githubRepoAllowlist": ["acme-labs/*"]}})
        ));
    }

    // --- the eval exit-code contract (#2007) --------------------------------
    //
    // The exit-0/exit-1 halves of the contract are proven end-to-end in
    // `cli/tests/eval_case_selector.rs`, against the real binary's process exit
    // code and the requests the runner actually received. A unit test here
    // could only re-derive the verdict rule from a row vector, which stays
    // green under a mutation that runs the UNFILTERED suite.

    #[tokio::test]
    async fn a_mistyped_case_id_fails_a_skill_eval_model_sweep_rather_than_greening_it() {
        // #2007: the skill-tier `--model` sweep boots a transient LOCAL runner
        // per model and grades in-CLI via `run_suite_cases`, so a `--case-id`
        // selection reaches it (unlike the local/cluster sweep, which is the
        // platform plane and only ever sees a suite NAME). `select_cases` runs
        // before `eval_sweep` boots anything, and an explicit --cases path
        // skips cwd-dependent resolution, so this reaches the exit-2 gate with
        // no Docker daemon and no `.curie/runner.json` needed.
        let dir = tempfile::tempdir().expect("tempdir");
        let cases = dir.path().join("cases.json");
        std::fs::write(
            &cases,
            r#"{"name":"smoke","cases":[{"id":"greets-the-user","input":"hi","grader":{"kind":"contains","expected":"hi"}}]}"#,
        )
        .expect("write suite");
        let err = super::eval(
            Some(cases),
            vec!["greets-the-usr".to_string()],
            None,
            vec!["opus".to_string()],
            Vec::new(),
            "curie-runner:test".to_string(),
            crate::eval_sampling::SampleConfig::default(),
        )
        .await
        .expect_err("a mistyped --case-id must fail the sweep, not silently sweep everything");
        assert_eq!(
            crate::exit::classify(&err).0,
            crate::exit::ExitClass::Usage,
            "{err:#}"
        );
        assert!(format!("{err:#}").contains("greets-the-usr"), "{err:#}");
    }

    /// The platform's answer for a repository two agents bind with no declared
    /// targets -- built by deserializing the WIRE shape, so the test cannot
    /// drift from what the endpoint actually sends (#1221).
    fn unroutable_check() -> crate::api::RoutingCheck {
        serde_json::from_str(
            r#"{
                "repo_full_name": "octo/shared-repo",
                "agent_count": 2,
                "agents": ["acme-bot", "acme-dev"],
                "resolvable": false,
                "unresolvable": [
                    {
                        "environment": "dev",
                        "code": "deploy.no_targets",
                        "message": "2 agents are built from this repository but the bundle has no deploy.yaml, so there is nothing to say which one this branch deploys to. Declare a target (ADR-0089)."
                    }
                ]
            }"#,
        )
        .expect("the routing-check wire shape should decode")
    }

    #[test]
    fn the_routing_warning_names_the_repository_and_every_bound_agent() {
        // The operator just deployed ONE of these agents. Naming only that one
        // would hide the actual damage: the sibling that was working stops
        // deploying without anyone touching it.
        let warning = routing_warning(&unroutable_check());
        assert!(warning.contains("octo/shared-repo"), "was {warning}");
        assert!(warning.contains("acme-bot"), "was {warning}");
        assert!(warning.contains("acme-dev"), "was {warning}");
    }

    #[test]
    fn the_routing_warning_carries_the_resolvers_own_words_verbatim() {
        // Paraphrasing here would put a second statement of the routing rule in
        // the client, free to drift from the one a push enforces (#1212).
        let check = unroutable_check();
        let warning = routing_warning(&check);
        assert!(
            warning.contains(&check.unresolvable[0].message),
            "was {warning}"
        );
        assert!(warning.contains("deploy.no_targets"), "was {warning}");
        assert!(warning.contains("dev"), "was {warning}");
    }

    #[test]
    fn the_routing_warning_states_the_blast_radius_it_was_told_about() {
        let warning = routing_warning(&unroutable_check());
        // The damage is real and must read as such, but scoped to the
        // environment the resolver actually named.
        assert!(warning.contains("dev"), "was {warning}");
        assert!(
            warning.contains("no longer deploy anything"),
            "was {warning}"
        );
        assert!(
            warning.contains("did not touch"),
            "the warning must say untouched agents are affected too: {warning}"
        );
        // The remedy is the resolver's, carried in its own message. A second,
        // hardcoded one is what made this warning misleading for a bundle that
        // already declares targets.
        assert!(
            !warning.contains("Fix: declare a target"),
            "the client must not append its own remedy: {warning}"
        );
    }

    /// One environment broken and one fine -- a bundle with a good `dev` target
    /// and a `prod` target naming an agent that does not exist. Dev pushes
    /// still deploy, so the warning must not say otherwise (#1221).
    fn prod_only_unroutable_check() -> crate::api::RoutingCheck {
        serde_json::from_str(
            r#"{
                "repo_full_name": "octo/shared-repo",
                "agent_count": 2,
                "agents": ["acme-bot", "acme-dev"],
                "resolvable": false,
                "unresolvable": [
                    {
                        "environment": "prod",
                        "code": "deploy.unknown_agent",
                        "message": "The prod target names agent 'acme-prod', which does not exist."
                    }
                ]
            }"#,
        )
        .expect("the routing-check wire shape should decode")
    }

    #[test]
    fn the_routing_warning_does_not_widen_one_broken_environment_to_all() {
        let check = prod_only_unroutable_check();
        let warning = routing_warning(&check);
        assert!(warning.contains("prod"), "was {warning}");
        assert!(
            warning.contains(&check.unresolvable[0].message),
            "was {warning}"
        );
        // Nothing may suggest the dev lane broke: it did not, and telling the
        // operator it did sends them to fix working configuration.
        assert!(
            !warning.contains("dev environment"),
            "dev still routes and must not be named as broken: {warning}"
        );
        assert!(
            !warning.contains("every push"),
            "only the reported environments are affected: {warning}"
        );
    }

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
        assert!(report_sweep(&rows, None).is_ok());
    }

    #[test]
    fn a_model_with_zero_completed_turns_fails_the_sweep_loudly() {
        // The bug this issue fixes: an unresolvable model must not read as an
        // indistinguishable 0%. `report_sweep` returns `Err` (never
        // `std::process::exit` itself, so a caller's port-forward guard still
        // drops via normal unwind) and the message names the model and a likely
        // cause instead of the eval consumer.
        let rows = vec![row("bogus-model-xyz", 0, 0, 5), row("opus", 3, 5, 5)];
        let err = report_sweep(&rows, None).expect_err("a never-completed row must fail the sweep");
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
        let err = report_sweep(&rows, None).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("model-alpha"), "{msg}");
        assert!(msg.contains("model-beta"), "{msg}");
    }

    fn recorded_query<'a>(
        recorded: Option<&'a str>,
        target: &'a str,
        replace: bool,
    ) -> RecordedStateQuery<'a> {
        RecordedStateQuery {
            recorded_name: recorded,
            target_name: target,
            replace,
            recorded_id: None,
            live_id: None,
            same_bundle_dir: true,
            recorded_digest: None,
            current_digest: None,
        }
    }

    #[test]
    fn replace_clears_a_stale_record_for_the_very_container_it_replaces() {
        // A bundle holding both a stale runner.json and a live container of that
        // name was unrecoverable with --replace: the record refused the boot
        // before the preflight could remove anything (#747).
        assert_eq!(
            plan_recorded_state(recorded_query(
                Some("curie-runner-local"),
                "curie-runner-local",
                true
            )),
            RecordedStatePlan::ClearAndProceed
        );
        assert_eq!(
            plan_recorded_state(recorded_query(None, "curie-runner-local", false)),
            RecordedStatePlan::Proceed
        );
    }

    #[test]
    fn replace_does_not_clear_a_record_naming_a_different_runner() {
        // Removing one container is no reason to forget another: a record for a
        // different, still-live runner keeps refusing, with or without --replace.
        assert_eq!(
            plan_recorded_state(recorded_query(
                Some("curie-runner-local"),
                "curie-example-42",
                true
            )),
            RecordedStatePlan::Refuse
        );
        assert_eq!(
            plan_recorded_state(recorded_query(
                Some("curie-runner-local"),
                "curie-runner-local",
                false
            )),
            RecordedStatePlan::Refuse
        );
    }

    fn verified_query<'a>(
        replace: bool,
        recorded_digest: Option<&'a str>,
        current_digest: Option<&'a str>,
    ) -> RecordedStateQuery<'a> {
        RecordedStateQuery {
            recorded_name: Some("curie-runner-local"),
            target_name: "curie-runner-local",
            replace,
            recorded_id: Some("deadbeef00001111"),
            live_id: Some("deadbeef0000"),
            same_bundle_dir: true,
            recorded_digest,
            current_digest,
        }
    }

    #[test]
    fn plain_up_replaces_a_verified_same_bundle_when_the_snapshot_differs() {
        assert_eq!(
            plan_recorded_state(verified_query(false, Some("aaa"), Some("bbb"))),
            RecordedStatePlan::ClearAndProceed
        );
    }

    #[test]
    fn plain_up_reports_already_running_when_the_verified_snapshot_matches() {
        assert_eq!(
            plan_recorded_state(verified_query(false, Some("aaa"), Some("aaa"))),
            RecordedStatePlan::AlreadyRunning
        );
    }

    #[test]
    fn replace_still_restarts_a_verified_unchanged_bundle() {
        assert_eq!(
            plan_recorded_state(verified_query(true, Some("aaa"), Some("aaa"))),
            RecordedStatePlan::ClearAndProceed
        );
    }

    #[test]
    fn plain_up_refuses_when_the_snapshot_cannot_be_compared() {
        assert_eq!(
            plan_recorded_state(verified_query(false, None, Some("aaa"))),
            RecordedStatePlan::Refuse
        );
        assert_eq!(
            plan_recorded_state(verified_query(false, Some("aaa"), None)),
            RecordedStatePlan::Refuse
        );
    }

    #[test]
    fn plain_up_refuses_a_verified_name_whose_container_id_does_not_match() {
        let mut q = verified_query(false, Some("aaa"), Some("bbb"));
        q.live_id = Some("cccccccccccc");
        assert_eq!(plan_recorded_state(q), RecordedStatePlan::Refuse);
    }

    #[test]
    fn plain_up_refuses_when_the_record_is_not_this_bundle_directory() {
        let mut q = verified_query(false, Some("aaa"), Some("bbb"));
        q.same_bundle_dir = false;
        assert_eq!(plan_recorded_state(q), RecordedStatePlan::Refuse);
    }

    #[test]
    fn recorded_ids_match_across_short_and_long_docker_ids() {
        assert!(recorded_ids_match(
            "9f2c1d3e4b5a6c7d8e9f0a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f",
            "9f2c1d3e4b5a"
        ));
        assert!(recorded_ids_match("9f2c1d3e4b5a", "9f2c1d3e4b5a6c7d"));
        assert!(!recorded_ids_match("", "9f2c1d3e4b5a"));
        assert!(!recorded_ids_match("aaaa", "bbbb"));
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
        assert!(validate_channel_binding("slack", crate::api::DEFAULT_SLACK_CHANNEL).is_ok());
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
        let snapshot = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(snapshot.path().join("evals")).unwrap();
        std::fs::write(snapshot.path().join("evals/cases.json"), "[]").unwrap();
        let path = resolve_cases_path(
            Some(PathBuf::from("/x/cases.json")),
            std::path::Path::new("/nowhere"),
            Some(snapshot.path()),
            None,
        )
        .unwrap();
        assert_eq!(path, PathBuf::from("/x/cases.json"));
    }

    #[test]
    fn missing_recorded_snapshot_cases_do_not_fall_back_to_cwd() {
        let cwd = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(cwd.path().join("evals")).unwrap();
        std::fs::write(cwd.path().join("evals/cases.json"), "[]").unwrap();
        let snapshot = tempfile::tempdir().unwrap();

        let err = resolve_cases_path(None, cwd.path(), Some(snapshot.path()), None).unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("no eval cases found in the running snapshot"),
            "{message}"
        );
        assert!(message.contains("--cases"), "{message}");
    }

    #[test]
    fn falls_back_from_cwd_to_the_recorded_bundle_dir() {
        let cwd = tempfile::tempdir().unwrap();
        let bundle = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(bundle.path().join("evals")).unwrap();
        std::fs::write(bundle.path().join("evals/cases.json"), "[]").unwrap();

        // cwd has no cases: resolve into the bundle dir from the state file.
        let resolved = resolve_cases_path(None, cwd.path(), None, Some(bundle.path())).unwrap();
        assert_eq!(resolved, bundle.path().join("evals/cases.json"));

        // cwd cases take precedence once present.
        std::fs::create_dir_all(cwd.path().join("evals")).unwrap();
        std::fs::write(cwd.path().join("evals/cases.json"), "[]").unwrap();
        let resolved = resolve_cases_path(None, cwd.path(), None, Some(bundle.path())).unwrap();
        assert_eq!(resolved, cwd.path().join("evals/cases.json"));
    }

    #[test]
    fn errors_when_nothing_is_found() {
        let cwd = tempfile::tempdir().unwrap();
        let err = resolve_cases_path(None, cwd.path(), None, None).unwrap_err();
        assert!(err.to_string().contains("--cases"), "{err}");
    }

    #[test]
    fn rejects_hash_prefixed_channel_name() {
        let err = validate_channel_binding("slack", "#testing")
            .unwrap_err()
            .to_string();
        assert!(err.contains("channel ID"), "{err}");
    }

    #[test]
    fn accepts_channel_id() {
        assert!(validate_channel_binding("slack", "C0EXAMPLE4").is_ok());
    }

    #[test]
    fn rejects_leading_whitespace_hash() {
        assert!(validate_channel_binding("slack", "  #testing").is_err());
    }

    #[test]
    fn a_kind_with_no_local_rule_passes_locally() {
        // No local shape rule for a non-slack kind; the API is the authoritative
        // gate for it.
        assert!(validate_channel_binding("webhook", "#anything").is_ok());
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

    /// The state that used to boot silently and fail one command later.
    #[test]
    fn no_resolved_credential_warns_and_says_none() {
        let (row, warning) = model_credential_summary(false, None, &[]);
        assert_eq!(row, "none");
        let warning = warning.expect("a runner that cannot reach a model must say so at boot");
        assert!(
            warning.contains("model-credential-rejected"),
            "the warning must name the error the next command will actually print, \
             so the two are recognizably the same problem: {warning}"
        );
        for way_out in ["CURIE_CREDENTIALS", "--fake-model"] {
            assert!(
                warning.contains(way_out),
                "the warning must offer {way_out} as a way forward: {warning}"
            );
        }
    }

    /// The warning has to stay rare to stay meaningful: every configured path
    /// reports itself and stays quiet.
    #[test]
    fn each_configured_model_path_reports_itself_without_warning() {
        let cases = [
            (true, None, None, "fake (offline, scripted replies)"),
            (false, Some("llama3"), None, "local ollama (llama3)"),
            (false, None, Some("CURIE_CREDENTIALS"), "CURIE_CREDENTIALS"),
            (false, None, Some("ANTHROPIC_API_KEY"), "ANTHROPIC_API_KEY"),
        ];
        for (fake, local, name, expected) in cases {
            let names: Vec<String> = name.into_iter().map(String::from).collect();
            let (row, warning) = model_credential_summary(fake, local, &names);
            assert_eq!(row, expected, "row for fake={fake} local={local:?}");
            assert!(
                warning.is_none(),
                "a configured model path must not warn (fake={fake} local={local:?})"
            );
        }
    }

    /// `--local-model` beats `--fake-model` at the call site, and the panel has
    /// to describe the runner that actually booted.
    #[test]
    fn local_model_wins_over_fake_in_the_summary() {
        let (row, _) = model_credential_summary(true, Some("llama3"), &[]);
        assert_eq!(row, "local ollama (llama3)");
    }

    /// Names, never values -- the row is printed.
    #[test]
    fn summary_reports_names_only() {
        let (row, _) = model_credential_summary(false, None, &["CURIE_CREDENTIALS".to_string()]);
        assert!(
            !row.contains("sk-"),
            "the panel must never carry a credential value: {row}"
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
        // Exercise local state guidance with its default URL while port 1 below
        // remains the deterministic connection refusal target.
        let hint = crate::local::deploy_unreachable_hint(
            crate::message::DEFAULT_LOCAL_API_URL,
            Some("curie-api-1   curie-api:dev   curie-api   Up 8 seconds (health: starting)"),
        );
        let opts = super::DeployOpts {
            agent: None,
            target: None,
            plugin_dir: dir.path().to_path_buf(),
            // port 1 is reserved/closed -> deterministic connection refused
            api_url: "http://127.0.0.1:1".to_string(),
            api_key: "k".to_string(),
            slack_channel: None,
            repo: None,
            workspace: super::WorkspaceIntent::Preserve,
            tier: super::DeployTier::Local,
            env: Some(super::DeployEnv::Dev),
            label: Some("v0".to_string()),
            secret: vec![],
            secret_binding_supported: true,
            connect_hint: hint.clone(),
        };
        let err = super::deploy(opts).await.unwrap_err();
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("local stack is still starting"),
            "the connection error must retain the state aware recovery: {rendered}"
        );
        assert!(
            rendered.contains("curie local status"),
            "the connection error must direct the operator to inspect local state: {rendered}"
        );
        assert!(
            !rendered.contains("curie local up"),
            "a starting stack must not be told to start again: {rendered}"
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
            agent: None,
            target: None,
            plugin_dir: dir.path().to_path_buf(),
            api_url: "http://127.0.0.1:1".to_string(),
            api_key: "k".to_string(),
            slack_channel: None,
            repo: None,
            workspace: super::WorkspaceIntent::Preserve,
            env: Some(super::DeployEnv::Dev),
            label: Some("v0".to_string()),
            secret: vec!["GH_TOKEN".to_string()],
            secret_binding_supported: true,
            connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
            tier: super::DeployTier::Local,
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
        // AC2: a tier that cannot bind `--secret` skips the declared-secrets
        // gate so a secrets-declaring bundle is not preempted with a
        // remediation that does not exist. Cluster now binds (#1488); this
        // pin is the skip itself.
        let dir = tempfile::tempdir().unwrap();
        scaffold_with_secrets(dir.path(), "test-agent", &["GITHUB_PERSONAL_ACCESS_TOKEN"]);

        let opts = super::DeployOpts {
            agent: None,
            target: None,
            plugin_dir: dir.path().to_path_buf(),
            // port 1 is reserved/closed -> deterministic connection refused
            api_url: "http://127.0.0.1:1".to_string(),
            api_key: "k".to_string(),
            slack_channel: None,
            repo: None,
            workspace: super::WorkspaceIntent::Preserve,
            env: Some(super::DeployEnv::Dev),
            label: Some("v0".to_string()),
            secret: vec![],
            secret_binding_supported: false,
            connect_hint: "UNREACHABLE-HINT-SENTINEL".to_string(),
            tier: super::DeployTier::Cluster,
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
            workspace_enabled: false,
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
    /// tier that answers it. The normal human path renders only the outer message
    /// and does not emit this error's fix, so both halves must remain in Display.
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

    // ───────────────────────────────────────────────────────────────────────
    // #1087 AC2: `skill message` and `skill eval` execute the SAME recorded
    // bundle. The mount decision is pure, so the parity seam is provable in CI
    // rather than only by the live sweep; each assertion terminates in the argv
    // the Docker daemon receives, not in a struct field or an enum variant.
    // ───────────────────────────────────────────────────────────────────────

    use super::{
        eval_runner_spec, recorded_bundle_digest, resolve_sweep_mount, sweep_snapshot, EvalBundle,
        SweepMount,
    };
    use crate::docker::StartSpec;
    use crate::state::RunnerState;

    const RECORDED_SOURCE: &str = "/src";
    const RECORDED_SNAPSHOT: &str = "/src/.curie/snapshots/abc";
    const RECORDED_DIGEST: &str = "abc";
    const RECORDED_URL: &str = "http://localhost:7245";

    /// A recorded runner, parameterized by the two #1087 fields so the
    /// pre-#1087 and half-written shapes are the SAME fixture with different
    /// values rather than three hand-built structs that can drift apart.
    fn recorded_runner(digest: Option<&str>, snapshot_dir: Option<&str>) -> RunnerState {
        RunnerState {
            container_id: "abc123".into(),
            container_name: "curie-runner-local".into(),
            image: "curie-runner".into(),
            port: 7245,
            base_url: RECORDED_URL.into(),
            session_id: "local-1".into(),
            plugin_dir: RECORDED_SOURCE.into(),
            fake_model: false,
            ollama_container: None,
            network: None,
            model_base_url: None,
            bundle_digest: digest.map(str::to_string),
            bundle_snapshot_dir: snapshot_dir.map(str::to_string),
            connector_containers: Vec::new(),
            connector_network: None,
        }
    }

    /// The eval sweep's runner spec, mounting `dir`. Mirrors what
    /// `boot_eval_runner` builds, so these tests end where the user's Docker
    /// daemon does: in `run_args()`.
    fn sweep_spec(dir: &Path) -> StartSpec {
        StartSpec {
            image: "curie-runner".into(),
            container_name: "curie-eval-sweep-0".into(),
            host_port: 7345,
            plugin_dir: dir.to_path_buf(),
            session_id: "eval-1".into(),
            sandbox_id: "local".into(),
            budget_json: r#"{"max_output_tokens_per_run":100000,"max_usd_per_day":5.0}"#.into(),
            fake_model: false,
            network: None,
            otel_endpoint: None,
            model_base_url: None,
            model: Some("opus".into()),
            passthrough_env: vec![],
            docker_env: vec![],
        }
    }

    fn mounts(spec: &StartSpec) -> Vec<String> {
        spec.run_args()
            .windows(2)
            .filter(|pair| pair[0] == "-v")
            .map(|pair| pair[1].clone())
            .collect()
    }

    // #1087 AC2's honesty rule, tested where it now lives. Both `skill status`
    // and `skill eval` call this, and both of their own paths need either a cwd
    // with recorded state or a live Docker run to reach, so these are the only
    // tests that can red when the rule is broken.

    #[test]
    fn recorded_bundle_digest_reports_the_digest_of_the_recorded_runner() {
        let saved = recorded_runner(Some(RECORDED_DIGEST), Some(RECORDED_SNAPSHOT));

        assert_eq!(
            recorded_bundle_digest(Some(&saved), RECORDED_URL),
            Some(RECORDED_DIGEST.to_string()),
            "the record IS the runner being reported on, so its digest applies"
        );
    }

    #[test]
    fn recorded_bundle_digest_is_none_for_a_runner_the_record_is_not_about() {
        let saved = recorded_runner(Some(RECORDED_DIGEST), Some(RECORDED_SNAPSHOT));

        assert_eq!(
            recorded_bundle_digest(Some(&saved), "http://localhost:9999"),
            None,
            "an explicit --url elsewhere must not be married to this bundle's digest"
        );
    }

    #[test]
    fn recorded_bundle_digest_is_none_when_the_record_predates_the_feature() {
        let saved = recorded_runner(None, None);

        assert_eq!(
            recorded_bundle_digest(Some(&saved), RECORDED_URL),
            None,
            "a pre-#1087 record has no digest to report, and that is not an error"
        );
    }

    #[test]
    fn recorded_bundle_digest_is_none_without_a_record() {
        assert_eq!(recorded_bundle_digest(None, RECORDED_URL), None);
    }

    #[test]
    fn sweep_snapshot_reuses_the_recorded_runners_snapshot() {
        let saved = recorded_runner(Some(RECORDED_DIGEST), Some(RECORDED_SNAPSHOT));

        let (dir, digest) = sweep_snapshot(Some(&saved))
            .expect("a recorded runner's snapshot is what the sweep must mount");

        assert_eq!(dir, PathBuf::from(RECORDED_SNAPSHOT));
        assert_eq!(
            digest, RECORDED_DIGEST,
            "the sweep reports the SAME digest the messaging path recorded"
        );
        assert_eq!(
            mounts(&sweep_spec(&dir)),
            vec![format!("{RECORDED_SNAPSHOT}:/plugin:ro")],
            "the sweep's runner must boot on the recorded snapshot"
        );
    }

    #[test]
    fn sweep_snapshot_is_none_without_a_recorded_snapshot() {
        // No runner recorded at all.
        assert!(sweep_snapshot(None).is_none());
        // A record written before #1087: both fields absent.
        assert!(sweep_snapshot(Some(&recorded_runner(None, None))).is_none());
        // Half written (digest recorded, directory missing): there is nothing to
        // mount, so the sweep packs its own rather than guessing a path.
        assert!(sweep_snapshot(Some(&recorded_runner(Some(RECORDED_DIGEST), None))).is_none());
        // ...and the mirror-image half: a directory with no digest to report.
        assert!(sweep_snapshot(Some(&recorded_runner(None, Some(RECORDED_SNAPSHOT)))).is_none());
    }

    #[test]
    fn sweep_snapshot_never_returns_the_source_dir() {
        let saved = recorded_runner(Some(RECORDED_DIGEST), Some(RECORDED_SNAPSHOT));

        let (dir, _) = sweep_snapshot(Some(&saved)).expect("a snapshot is recorded");

        assert_ne!(
            dir,
            PathBuf::from(RECORDED_SOURCE),
            "reading plugin_dir would remount the editable source the ticket removes"
        );
        assert!(dir.starts_with(format!("{RECORDED_SOURCE}/.curie/snapshots")));
        assert!(
            !mounts(&sweep_spec(&dir)).contains(&format!("{RECORDED_SOURCE}:/plugin:ro")),
            "the mutable source must never reach the sweep's argv"
        );
    }

    #[test]
    fn resolve_sweep_mount_returns_the_recorded_snapshot_when_one_exists() {
        // A source dir is supplied too: reusing the recorded snapshot regardless
        // is the decision `eval_sweep` must not re-make in its own body.
        let resolved = resolve_sweep_mount(
            Some((
                PathBuf::from(RECORDED_SNAPSHOT),
                RECORDED_DIGEST.to_string(),
            )),
            Some(Path::new(RECORDED_SOURCE)),
        );

        match resolved {
            SweepMount::Recorded { dir, digest } => {
                assert_eq!(dir, PathBuf::from(RECORDED_SNAPSHOT));
                assert_eq!(digest, RECORDED_DIGEST);
                assert_eq!(
                    mounts(&sweep_spec(&dir)),
                    vec![format!("{RECORDED_SNAPSHOT}:/plugin:ro")],
                    "the resolved mount is what the sweep's docker run receives"
                );
            }
            SweepMount::PackEphemeral { source } => panic!(
                "a recorded snapshot must be reused, not repacked from {}",
                source.display()
            ),
        }
    }

    #[test]
    fn resolve_sweep_mount_packs_ephemeral_when_nothing_is_recorded() {
        // With a recorded bundle source but no snapshot: pack from that source.
        match resolve_sweep_mount(None, Some(Path::new(RECORDED_SOURCE))) {
            SweepMount::PackEphemeral { source } => {
                assert_eq!(source, PathBuf::from(RECORDED_SOURCE))
            }
            SweepMount::Recorded { dir, .. } => {
                panic!(
                    "nothing is recorded, so {} cannot be mounted",
                    dir.display()
                )
            }
        }
        // With nothing recorded at all: pack from the cwd. The fall-back is
        // always "pack" -- `SweepMount` has no variant that mounts mutable
        // source, which is the hole this ticket closes.
        match resolve_sweep_mount(None, None) {
            SweepMount::PackEphemeral { source } => assert_eq!(source, PathBuf::from(".")),
            SweepMount::Recorded { dir, .. } => {
                panic!(
                    "nothing is recorded, so {} cannot be mounted",
                    dir.display()
                )
            }
        }
    }

    /// A minimal on-disk bundle source, returned with its owning tempdir so the
    /// caller keeps it alive. `EvalBundle::materialize` canonicalizes and packs
    /// for real, so these cases need real files rather than the string paths the
    /// pure-resolver tests above use.
    fn bundle_source() -> (tempfile::TempDir, PathBuf) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let source = tmp.path().join("bundle");
        std::fs::create_dir_all(source.join("skills/demo")).unwrap();
        std::fs::write(source.join("skills/demo/SKILL.md"), "# demo\n").unwrap();
        (tmp, source)
    }

    /// The argv-terminating helper for the two `EvalBundle` tests: what
    /// `boot_eval_runner` would hand the Docker daemon for this bundle.
    fn eval_mounts(bundle: &EvalBundle) -> Vec<String> {
        mounts(&eval_runner_spec(
            bundle,
            "curie-runner",
            7345,
            "curie-eval-sweep-0",
            "opus",
            vec![],
            vec![],
        ))
    }

    /// #1087 AC2's wiring guard. The pure-resolver tests above prove
    /// `resolve_sweep_mount` decides correctly; they cannot prove the eval path
    /// OBEYS it, because `eval_sweep` needs a Docker daemon to run at all. This
    /// closes that gap from the other end: the eval path's only mountable value
    /// is an `EvalBundle`, whose fields are private to its module, so
    /// re-introducing the source-directory mount is a compile error in
    /// `eval_sweep` and can only be written inside `materialize` -- where this
    /// test sees it. Mutating the `PackEphemeral` arm to return `source` reds
    /// this test.
    #[test]
    fn the_eval_bundle_packs_a_snapshot_and_never_mounts_the_mutable_source() {
        let (_tmp, source) = bundle_source();

        let bundle = EvalBundle::materialize(SweepMount::PackEphemeral {
            source: source.clone(),
        })
        .expect("packing an ephemeral snapshot from a real bundle source");

        let canonical_source = source.canonicalize().unwrap();
        assert_ne!(
            bundle.dir(),
            canonical_source,
            "the sweep must execute a snapshot, never the editable source"
        );
        assert!(
            bundle
                .dir()
                .starts_with(canonical_source.join(".curie/snapshots")),
            "a packed snapshot lives under the source's snapshot root, got {}",
            bundle.dir().display()
        );
        assert!(
            !bundle.digest().is_empty(),
            "a packed snapshot reports its own digest for the --json payload"
        );
        // The source it must release when the sweep ends -- and the only reason
        // this variant carries one.
        assert_eq!(bundle.ephemeral_source(), Some(canonical_source.as_path()));

        let mounts = eval_mounts(&bundle);
        assert_eq!(
            mounts,
            vec![format!("{}:/plugin:ro", bundle.dir().display())],
            "the snapshot is what reaches the Docker daemon"
        );
        assert!(
            !mounts.contains(&format!("{}:/plugin:ro", canonical_source.display())),
            "the mutable source must never reach the eval runner's argv"
        );
    }

    /// The recorded arm of the same guard: the recorded runner's snapshot is
    /// mounted as-is and its digest is carried through unchanged, which is what
    /// makes `skill message` and `skill eval` report the SAME value rather than
    /// two independently recomputed ones. It is also not this run's to delete.
    #[test]
    fn the_eval_bundle_reuses_the_recorded_snapshot_and_its_digest() {
        let (_tmp, source) = bundle_source();
        let recorded = source.join(".curie/snapshots/abc");
        std::fs::create_dir_all(&recorded).unwrap();

        let bundle = EvalBundle::materialize(SweepMount::Recorded {
            dir: recorded.clone(),
            digest: RECORDED_DIGEST.to_string(),
        })
        .expect("a recorded snapshot on disk resolves");

        assert_eq!(bundle.dir(), recorded.canonicalize().unwrap());
        assert_eq!(
            bundle.digest(),
            RECORDED_DIGEST,
            "the recorded digest is reused, not recomputed"
        );
        assert_eq!(
            bundle.ephemeral_source(),
            None,
            "the recorded runner owns this snapshot; the sweep must not release it"
        );
        assert_eq!(
            eval_mounts(&bundle),
            vec![format!("{}:/plugin:ro", bundle.dir().display())]
        );
    }

    /// An aborted boot releases the credentials it staged, not just the
    /// containers. Deleting the wipe from `release_boot_scaffolding` fails here.
    ///
    /// Nothing else will ever collect them: no state was recorded, so no `skill
    /// down` can find the bundle (#1087). Driven with no sidecar, no connector
    /// and no network so it reaches no Docker daemon -- what is under test is
    /// the release list, not the removals.
    #[tokio::test]
    async fn an_aborted_boot_releases_the_staged_credentials() {
        let dir = tempfile::tempdir().unwrap();
        crate::connector_build::stage_secret_file(
            dir.path(),
            "kubernetes",
            "/secrets/kubeconfig",
            "creds",
        )
        .unwrap();
        let root = crate::connector_build::connector_secrets_root(dir.path());
        assert!(root.exists());

        super::release_boot_scaffolding(
            None,
            &[],
            None,
            &dir.path().join(".curie/snapshots/never-packed"),
            dir.path(),
        )
        .await;

        assert!(
            !root.exists(),
            "a resolved credential must not outlive the boot that staged it"
        );
    }
}

/// Which nullable override a `<tier> overrides` invocation intends to change.
///
/// Three states, because the API's PATCH semantics have three (#1310): omitted
/// leaves the stored value alone, an explicit JSON null clears it back to the
/// platform default, and a string pins it. A plain `Option<String>` can only
/// express two of those, and collapsing "clear" onto the empty string is the
/// exact defect #1355 fixed on the console -- an empty override reaches the
/// worker falsy, emits no boot key, and skips the platform default that
/// clearing is supposed to restore. So the CLI never sends `""` either; the
/// operator says `--clear-model` and the wire carries `null`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OverrideChange {
    /// Not mentioned on the command line: leave whatever is stored.
    Unchanged,
    /// `--clear-<field>`: send explicit null, restoring the platform default.
    Clear,
    /// `--<field> <value>`: pin this value for this agent.
    Set(String),
}

impl OverrideChange {
    /// Resolve the `--<field>` / `--clear-<field>` pair into one intent.
    ///
    /// Args:
    ///   field: the field name, used only in the usage error.
    ///   value: the `--<field>` value, if the operator passed one.
    ///   clear: whether `--clear-<field>` was passed.
    ///
    /// Returns:
    ///   The intent, or a usage error when both were passed or the value is
    ///   blank.
    pub fn resolve(field: &str, value: Option<String>, clear: bool) -> Result<Self> {
        match (value, clear) {
            (Some(_), true) => Err(crate::exit::usage(format!(
                "--{field} and --clear-{field} contradict each other; pass one. \
                 --clear-{field} restores the platform default"
            ))),
            // Refused here rather than forwarded, so the operator gets the fix
            // from the CLI instead of a 422 from the API. The API refuses it
            // too (#1355), and for the reason the hint states.
            (Some(v), false) if v.trim().is_empty() => Err(crate::exit::usage(format!(
                "--{field} must not be blank: an empty value skips the platform \
                 default instead of restoring it. Pass --clear-{field} to clear \
                 the override, or a real value"
            ))),
            // Trimmed before it becomes an intent (#1392). The API strips too
            // and is the authoritative gate, but doing it here as well is what
            // makes `--dry-run` honest: it must print the body that would
            // actually be sent, not the argv the operator typed.
            (Some(v), false) => Ok(OverrideChange::Set(v.trim().to_string())),
            (None, true) => Ok(OverrideChange::Clear),
            (None, false) => Ok(OverrideChange::Unchanged),
        }
    }

    /// The JSON value this intent contributes to a `PATCH /agents/{id}` body,
    /// or `None` when the field must be OMITTED from the body entirely.
    ///
    /// Returns:
    ///   `Some(Value::Null)` to clear, `Some(Value::String)` to pin, `None` to
    ///   leave the field out so the API's `model_fields_set` check reads it as
    ///   unchanged.
    fn patch_value(&self) -> Option<serde_json::Value> {
        match self {
            OverrideChange::Unchanged => None,
            OverrideChange::Clear => Some(serde_json::Value::Null),
            OverrideChange::Set(v) => Some(serde_json::Value::String(v.clone())),
        }
    }
}

/// The `PATCH /agents/{id}` body for a `<tier> overrides` write, or `None` when
/// nothing was asked for (the inspect path).
///
/// Pure so the three-way semantics are assertable with no server: the property
/// that matters is that an Unchanged field is ABSENT from the body rather than
/// present-and-null, since the API tells those apart with `model_fields_set`
/// and present-and-null is the clear.
///
/// Args:
///   model: the intent for the model override.
///   thinking: the intent for the thinking override.
///
/// Returns:
///   The body to PATCH, or `None` when both intents are `Unchanged`.
pub fn overrides_patch_body(
    model: &OverrideChange,
    thinking: &OverrideChange,
) -> Option<serde_json::Value> {
    let mut body = serde_json::Map::new();
    if let Some(v) = model.patch_value() {
        body.insert("model".to_string(), v);
    }
    if let Some(v) = thinking.patch_value() {
        body.insert("thinking".to_string(), v);
    }
    if body.is_empty() {
        return None;
    }
    Some(serde_json::Value::Object(body))
}

/// The one-line human summary of an `overrides` result.
///
/// Pure so the wording is assertable. The spacing defect this replaced (#1394)
/// existed precisely because the line was built inline in `render` and could
/// only be checked by running the command and looking at it: an inspect
/// interpolated an empty verb before the colon and printed
/// `overrides for x : model ...`.
///
/// "platform default" rather than "none": a null here is not an absence, it is
/// a deferral to the platform-level setting, and an operator reading "none"
/// would reasonably expect no model at all.
///
/// Args:
///   agent: the agent's name.
///   model: the stored model override, `None` when the platform default applies.
///   thinking: the stored thinking override, same convention.
///   changed: whether this invocation wrote, as opposed to inspecting.
///
/// Returns:
///   The summary line, with no trailing newline.
pub fn overrides_summary(
    agent: &str,
    model: &Option<String>,
    thinking: &Option<String>,
    changed: bool,
) -> String {
    let show = |v: &Option<String>| v.clone().unwrap_or_else(|| "platform default".to_string());
    // The verb carries its own leading space, so an inspect closes straight
    // onto the colon instead of leaving a gap where a word used to be.
    let verb = if changed { " now" } else { "" };
    format!(
        "overrides for {agent}{verb}: model {}, thinking {}",
        show(model),
        show(thinking)
    )
}

/// Output of `<tier> overrides <agent>`: the dry-run plan, or the agent's two
/// nullable overrides as the API stored them. Owns its data so it outlives the
/// `ApiClient`.
///
/// `model`/`thinking` are `None` when no override is pinned, which is the same
/// fact the API returns as JSON null: the platform default applies. `changed`
/// distinguishes an inspect from a write, so an agent consumer can tell "this
/// is what it is" from "this is what it now is" without diffing.
#[derive(Debug)]
pub enum OverridesOutput {
    DryRun(crate::ui::DryRunPlan),
    Done {
        agent: String,
        model: Option<String>,
        thinking: Option<String>,
        changed: bool,
    },
}

impl crate::ui::CliOutput for OverridesOutput {
    fn to_json(&self) -> serde_json::Value {
        match self {
            OverridesOutput::DryRun(plan) => plan.to_json(),
            OverridesOutput::Done {
                agent,
                model,
                thinking,
                changed,
            } => serde_json::json!({
                "agent": agent,
                "model": model,
                "thinking": thinking,
                "changed": changed,
            }),
        }
    }

    fn render(&self, ui: &crate::ui::Ui) {
        match self {
            OverridesOutput::DryRun(plan) => plan.render(ui),
            OverridesOutput::Done {
                agent,
                model,
                thinking,
                changed,
            } => {
                ui.payload(&overrides_summary(agent, model, thinking, *changed));
            }
        }
    }
}

/// `curie <tier> overrides <agent> [--model V|--clear-model] [--thinking V|--clear-thinking]`.
///
/// With no change flags this INSPECTS: one `GET`-resolved agent, no write. With
/// any change flag it PATCHes only the fields named, then reports the row as the
/// API stored it, so the operator sees what took rather than what was intended.
///
/// Covers both nullable overrides in one verb on purpose. The issue (#1311)
/// names `thinking` only, but `model` has the identical gap -- also settable
/// only by raw PATCH, also omitted from the CLI DTO -- and the two share the
/// API's `_nullable_override_validator` and its three-way semantics. Splitting
/// them would mean two verbs, two committed schemas and two manifest
/// regenerations for one concept, and would leave the sibling defect open for
/// exactly as long as it took someone to file it again.
///
/// Args:
///   opts: api url/key, the agent name or id, and the dry-run flag.
///   model: the intent for the model override.
///   thinking: the intent for the thinking override.
///
/// Returns:
///   The stored overrides, or the dry-run plan.
pub async fn overrides(
    opts: AgentActionOpts,
    model: OverrideChange,
    thinking: OverrideChange,
) -> Result<OverridesOutput> {
    let ui = crate::ui::ui();
    let body = overrides_patch_body(&model, &thinking);
    if opts.dry_run {
        let plan = match &body {
            Some(b) => format!(
                "PATCH {}/agents/<id>  {b}  (would resolve agent {:?} first)",
                opts.api_url, opts.agent
            ),
            None => format!(
                "GET {}/agents  (read-only: would resolve agent {:?} and print its overrides)",
                opts.api_url, opts.agent
            ),
        };
        return Ok(OverridesOutput::DryRun(crate::ui::DryRunPlan {
            lines: vec![plan],
        }));
    }
    let client = ApiClient::new(&opts.api_url, &opts.api_key)?;
    let agent = client.find_agent(&opts.agent).await?;
    let Some(body) = body else {
        // Inspect: find_agent already carries both fields, so there is nothing
        // further to fetch and nothing to write.
        return Ok(OverridesOutput::Done {
            agent: agent.name,
            model: agent.model,
            thinking: agent.thinking,
            changed: false,
        });
    };
    let cl = ui.checklist();
    let step = cl.step(&format!("updating overrides for {}", agent.name));
    let saved = match client.update_agent(&agent.id, &body).await {
        Ok(saved) => {
            step.done("updated");
            saved
        }
        Err(err) => {
            step.fail("failed");
            return Err(err);
        }
    };
    Ok(OverridesOutput::Done {
        agent: saved.name,
        model: saved.model,
        thinking: saved.thinking,
        changed: true,
    })
}

#[cfg(test)]
mod overrides_tests {
    use super::{overrides_patch_body, OverrideChange};

    // The property the whole verb rests on (#1310, #1311): an UNCHANGED field is
    // absent from the body, a CLEARED field is present and null. The API tells
    // those apart with `model_fields_set`, so collapsing them means "leave this
    // alone" silently becomes "reset this to the platform default".
    #[test]
    fn an_unchanged_field_is_absent_and_a_cleared_field_is_present_and_null() {
        let body = overrides_patch_body(&OverrideChange::Unchanged, &OverrideChange::Clear)
            .expect("a clear is a write");
        let obj = body.as_object().expect("an object");
        assert!(
            !obj.contains_key("model"),
            "unchanged must be absent: {body}"
        );
        assert_eq!(obj.get("thinking"), Some(&serde_json::Value::Null));
    }

    #[test]
    fn both_unchanged_is_no_body_at_all_which_is_the_inspect_path() {
        assert!(
            overrides_patch_body(&OverrideChange::Unchanged, &OverrideChange::Unchanged).is_none()
        );
    }

    #[test]
    fn a_set_field_carries_its_value() {
        let body = overrides_patch_body(
            &OverrideChange::Set("kimi-k2".into()),
            &OverrideChange::Set("adaptive".into()),
        )
        .expect("a set is a write");
        assert_eq!(body["model"], "kimi-k2");
        assert_eq!(body["thinking"], "adaptive");
    }

    // --value and --clear-value are contradictory, not a precedence puzzle:
    // picking a winner would make one of them silently do nothing.
    #[test]
    fn setting_and_clearing_the_same_field_is_a_usage_error() {
        let err = OverrideChange::resolve("model", Some("m".into()), true)
            .expect_err("contradictory flags must be refused");
        let msg = err.to_string();
        assert!(msg.contains("--model"), "{msg}");
        assert!(msg.contains("--clear-model"), "{msg}");
    }

    // Refused at the CLI rather than forwarded to earn a 422: the operator gets
    // the fix from the tool they typed into, and the message names the flag that
    // actually does what they meant.
    #[test]
    fn a_blank_value_is_refused_and_points_at_the_clear_flag() {
        for blank in ["", "   ", "\t"] {
            let err = OverrideChange::resolve("thinking", Some(blank.into()), false)
                .expect_err("a blank value must be refused");
            assert!(
                err.to_string().contains("--clear-thinking"),
                "the refusal must name the flag that clears: {err}"
            );
        }
    }

    // #1394: an inspect used to print "overrides for a : model ..." because the
    // verb was interpolated as an empty string before the colon.
    #[test]
    fn the_inspect_summary_has_no_gap_where_the_verb_would_be() {
        let line = super::overrides_summary("a", &Some("kimi-k2".into()), &None, false);
        assert_eq!(
            line,
            "overrides for a: model kimi-k2, thinking platform default"
        );
        assert!(!line.contains("  "), "no double space anywhere: {line}");
    }

    #[test]
    fn a_write_summary_says_now_and_names_a_cleared_field_as_the_default() {
        assert_eq!(
            super::overrides_summary("a", &None, &Some("adaptive".into()), true),
            "overrides for a now: model platform default, thinking adaptive"
        );
    }

    // #1392: the CLI stored `" kimi-k2 "` verbatim while the console trimmed, so
    // the same paste produced two different stored values depending on the
    // surface. A padded id is then forwarded as CURIE_MODEL and rejected by the
    // provider at the agent's NEXT turn, far from the command that exited 0.
    #[test]
    fn a_padded_value_is_trimmed_before_it_becomes_an_intent() {
        assert_eq!(
            OverrideChange::resolve("model", Some("  kimi-k2  ".into()), false).unwrap(),
            OverrideChange::Set("kimi-k2".into())
        );
        assert_eq!(
            OverrideChange::resolve("thinking", Some("\tadaptive\n".into()), false).unwrap(),
            OverrideChange::Set("adaptive".into())
        );
    }

    // The dry-run plan is the operator's only preview of the write, so it has to
    // show the body that will actually be sent, not the argv they typed.
    #[test]
    fn the_dry_run_body_carries_the_trimmed_value() {
        let body = overrides_patch_body(
            &OverrideChange::resolve("model", Some(" kimi-k2 ".into()), false).unwrap(),
            &OverrideChange::Unchanged,
        )
        .expect("a set is a write");
        assert_eq!(body["model"], "kimi-k2");
    }

    #[test]
    fn the_three_intents_round_trip_from_their_flag_pairs() {
        assert_eq!(
            OverrideChange::resolve("model", None, false).unwrap(),
            OverrideChange::Unchanged
        );
        assert_eq!(
            OverrideChange::resolve("model", None, true).unwrap(),
            OverrideChange::Clear
        );
        assert_eq!(
            OverrideChange::resolve("model", Some("m".into()), false).unwrap(),
            OverrideChange::Set("m".into())
        );
    }
}

// ---------------------------------------------------------------------------
// Connector source builds (ADR 0113): `curie build --plugin-dir`
// ---------------------------------------------------------------------------

/// One connector's resolved identity, as `curie build --plugin-dir` reports it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectorBuildRecord {
    pub name: String,
    pub image: String,
    pub delivery: crate::connector_build::Delivery,
    pub platforms: Vec<String>,
    pub source_digest: String,
}

/// The `curie build --plugin-dir` receipt: one object, always, even when the
/// bundle declares nothing to build. Empty stdout under `--json` is the #485
/// failure -- an agent consumer cannot tell "nothing to build" from "the
/// command produced nothing".
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConnectorBuildOutput {
    pub connectors: Vec<ConnectorBuildRecord>,
}

impl crate::ui::CliOutput for ConnectorBuildOutput {
    fn to_json(&self) -> serde_json::Value {
        // Delegated wholesale to the `Serialize` value rather than hand-picked
        // field by field, so a record can never lose a field on the emit hop.
        serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!({ "connectors": [] }))
    }

    fn render(&self, ui: &crate::ui::Ui) {
        if self.connectors.is_empty() {
            ui.note("this bundle declares no connectors to build");
            return;
        }
        for record in &self.connectors {
            ui.payload_plain(&format!("{} -> {}", record.name, record.image));
        }
    }
}

/// The flags `curie build --plugin-dir` carries.
pub struct ConnectorBuildOpts {
    pub plugin_dir: PathBuf,
    /// `Some(ref)` pushes a multi-platform index there; `None` builds the host
    /// platform into the local Docker daemon.
    pub registry: Option<String>,
    /// Replace a registry lock with a local-daemon one deliberately.
    pub force: bool,
}

/// `curie build --plugin-dir <dir>`: build every connector the bundle declares
/// from source and write `connectors.lock.yaml`.
///
/// Deliberately does NOT look for a repo checkout the way `build` does: a
/// released binary building a bundle's connectors has none, and requiring one
/// would make the whole feature source-only.
pub async fn build_connectors(opts: ConnectorBuildOpts) -> Result<ConnectorBuildOutput> {
    use crate::connector_build as cb;

    let plugin_dir = opts
        .plugin_dir
        .canonicalize()
        .with_context(|| format!("plugin dir not found: {}", opts.plugin_dir.display()))?;
    let decl = cb::load(&plugin_dir)?;
    let buildable: Vec<(&String, &cb::ConnectorSpecDecl)> = decl
        .connectors
        .iter()
        .filter(|(_, spec)| spec.build.is_some())
        .collect();
    if buildable.is_empty() {
        return Ok(ConnectorBuildOutput {
            connectors: Vec::new(),
        });
    }
    if !on_path("docker") {
        bail!(
            "Docker is not installed or not on PATH. Install Docker \
             (https://docs.docker.com/get-docker/) and retry."
        );
    }
    let (bundle_name, _version) = read_manifest(&plugin_dir)?;

    // buildx writes its build result here; a private per-run directory rather
    // than the bundle, so a metadata file never rides the upload. The system
    // temp dir is shared with every other user on the box, so the directory is
    // the owner's alone rather than the 0755 a default umask would give it.
    let metadata_dir = std::env::temp_dir().join(format!("curie-build-{}", uuid::Uuid::new_v4()));
    create_private_dir(&metadata_dir)
        .with_context(|| format!("create {}", metadata_dir.display()))?;

    let ui = crate::ui::ui();
    let host = cb::host_platform();
    let mut records = Vec::new();
    let mut entries = std::collections::BTreeMap::new();
    let mut failure = None;
    for (connector, spec) in buildable {
        let plan = match cb::build_plan(
            &plugin_dir,
            &bundle_name,
            connector,
            spec,
            opts.registry.as_deref(),
            &host,
            &metadata_dir,
        ) {
            Ok(plan) => plan,
            Err(err) => {
                failure = Some(err);
                break;
            }
        };
        match run_one_connector_build(&plan, ui).await {
            Ok(image) => {
                entries.insert(
                    connector.clone(),
                    cb::ConnectorLockEntryDecl {
                        image: image.clone(),
                        delivery: plan.delivery,
                        platforms: plan.platforms.clone(),
                        source_digest: plan.source_digest.clone(),
                    },
                );
                records.push(ConnectorBuildRecord {
                    name: connector.clone(),
                    image,
                    delivery: plan.delivery,
                    platforms: plan.platforms.clone(),
                    source_digest: plan.source_digest,
                });
            }
            Err(err) => {
                failure = Some(err);
                break;
            }
        }
    }
    let _ = std::fs::remove_dir_all(&metadata_dir);
    if let Some(err) = failure {
        // A build that could not run writes no lock: a partial lock would claim
        // a resolved image for a connector that has none.
        return Err(err);
    }

    cb::write_lock(
        &plugin_dir,
        &cb::ConnectorLockFileDecl {
            version: cb::LOCK_VERSION,
            connectors: entries,
        },
        opts.force,
    )?;
    Ok(ConnectorBuildOutput {
        connectors: records,
    })
}

/// Create a directory only its owner can enter, in one step.
///
/// The mode rides the create rather than a `set_permissions` after it: in a
/// shared `/tmp` the window between the two is a window in which anyone can
/// read what lands there, and buildx starts writing as soon as it is handed the
/// path. Non-unix keeps the platform default, as the other cfg splits here do.
#[cfg(unix)]
pub fn create_private_dir(path: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(path)
}

#[cfg(not(unix))]
pub fn create_private_dir(path: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(path)
}

/// Run one connector's build and read back the immutable reference it produced.
async fn run_one_connector_build(
    plan: &crate::connector_build::ConnectorBuildPlan,
    ui: &crate::ui::Ui,
) -> Result<String> {
    use crate::connector_build as cb;

    let command = cb::build_argv(plan);
    ui.note(&format!("=== {} ===", command.display()));
    // Inherit stdio so the build log streams like a hand-run build.
    let status = tokio::process::Command::new(&command.program)
        .args(command.argv())
        .status()
        .await
        .context("failed to invoke docker")?;
    if !status.success() {
        bail!("building connector '{}' failed ({status})", plan.connector);
    }
    match plan.delivery {
        cb::Delivery::Registry => {
            let metadata_file = plan
                .metadata_file
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("a registry build records no metadata file"))?;
            let raw = std::fs::read_to_string(metadata_file)
                .with_context(|| format!("read {}", metadata_file.display()))?;
            Ok(cb::digest_pinned_ref(
                &plan.image_ref,
                &cb::digest_from_metadata(&raw)?,
            ))
        }
        cb::Delivery::LocalDaemon => {
            let inspect = cb::image_inspect_argv(&plan.image_ref);
            crate::docker::docker(&inspect.argv()).await
        }
    }
}

// ---------------------------------------------------------------------------
// The deploy-time lock preflight (ADR 0113)
// ---------------------------------------------------------------------------

/// The command that clears a missing or stale lock, as the operator would type
/// it against their own bundle directory.
fn rebuild_hint(plugin_dir: &Path, registry: bool) -> String {
    if registry {
        format!(
            "run `curie build --plugin-dir {} --registry <ref>` and redeploy",
            plugin_dir.display()
        )
    } else {
        format!(
            "run `curie build --plugin-dir {}` and redeploy",
            plugin_dir.display()
        )
    }
}

/// Refuse a deploy whose `build:` connectors have no usable lock.
///
/// The CLI mirror of the platform's intake rule, run before the bundle is even
/// packed so the operator gets a local failure naming their own directory
/// instead of an upload rejection about a file nobody told them to make. A
/// bundle with no `build:` connector is untouched.
pub fn lock_preflight(
    plugin_dir: &Path,
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: Option<&crate::connector_build::ConnectorLockFileDecl>,
    recomputed: &std::collections::BTreeMap<String, String>,
    tier: DeployTier,
) -> Result<()> {
    use crate::connector_build::Delivery;

    for (connector, spec) in &decl.connectors {
        if spec.build.is_none() {
            continue;
        }
        let Some(entry) = lock.and_then(|lock| lock.connectors.get(connector)) else {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "connectors.{connector} builds from source, but {} records no image for it. \
                     Build it before deploying.",
                    crate::connector_build::CONNECTOR_LOCK_FILE
                ))
                .with_fix(rebuild_hint(plugin_dir, false)),
            ));
        };
        if let Some(fresh) = recomputed.get(connector) {
            if &entry.source_digest != fresh {
                return Err(anyhow::Error::from(
                    crate::exit::CliError::usage(format!(
                        "connectors.{connector} has changed since {} was written, so the locked \
                         image no longer matches this source.",
                        crate::connector_build::CONNECTOR_LOCK_FILE
                    ))
                    .with_fix(rebuild_hint(plugin_dir, false)),
                ));
            }
        }
        if tier == DeployTier::Cluster && entry.delivery == Delivery::LocalDaemon {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "connectors.{connector} is locked to an image in your local Docker daemon, \
                     which no cluster node can pull. Push it to a registry first."
                ))
                .with_fix(rebuild_hint(plugin_dir, true)),
            ));
        }
    }
    Ok(())
}

/// Ask the REGISTRY, by digest, for the raw manifest.
///
/// `--raw` matters: without it `imagetools inspect` pretty-prints, and a parser
/// reading that output is reading a presentation format that has changed before.
pub fn registry_manifest_argv(image: &str) -> crate::ops::OpsCommand {
    crate::connector_build::plain_command(
        "docker",
        vec![
            "buildx".into(),
            "imagetools".into(),
            "inspect".into(),
            image.to_string(),
            "--raw".into(),
        ],
    )
}

/// The architectures the cluster's own nodes report.
pub fn node_architectures_argv() -> crate::ops::OpsCommand {
    crate::connector_build::plain_command(
        "kubectl",
        vec![
            "get".into(),
            "nodes".into(),
            "-o".into(),
            "jsonpath={.items[*].status.nodeInfo.architecture}".into(),
        ],
    )
}

/// Parse that jsonpath's output: one space-separated word per node, duplicates
/// being the common case.
pub fn node_architectures(raw: &str) -> std::collections::BTreeSet<String> {
    raw.split_whitespace().map(str::to_string).collect()
}

/// The platform set an OCI image index actually covers.
///
/// buildx's attestation manifests are `unknown/unknown` and are neither a
/// platform the image covers nor a defect: a real multi-arch `--push` emits
/// them alongside the real entries by default. A plain single-platform manifest
/// has no `manifests` array at all and covers nothing the declaration promised.
pub fn manifest_platforms(raw: &str) -> Result<std::collections::BTreeSet<String>> {
    let parsed: serde_json::Value =
        serde_json::from_str(raw).context("parse the registry manifest")?;
    let Some(manifests) = parsed.get("manifests").and_then(|m| m.as_array()) else {
        return Ok(std::collections::BTreeSet::new());
    };
    Ok(manifests
        .iter()
        .filter_map(|entry| {
            let platform = entry.get("platform")?;
            let os = platform.get("os")?.as_str()?;
            let arch = platform.get("architecture")?.as_str()?;
            (os != "unknown" && arch != "unknown").then(|| format!("{os}/{arch}"))
        })
        .collect())
}

/// Refuse a cluster deploy whose locked image is gone from the registry, or
/// whose resolved index cannot run on every node.
///
/// The comparison is against the REGISTRY's answer, never the lock's declared
/// platforms: a lock declaring two platforms while the push went out single-arch
/// passes a declaration check and fails after apply as `no matching manifest`.
pub fn registry_preflight(
    image: &str,
    inspect: std::result::Result<&str, String>,
    node_architectures: &std::collections::BTreeSet<String>,
    declared_platforms: &[String],
) -> Result<()> {
    let raw = match inspect {
        Ok(raw) => raw,
        Err(stderr) => {
            return Err(anyhow::Error::from(
                crate::exit::CliError::usage(format!(
                    "the registry could not resolve {image}: {}. The image the lock names is not \
                     there, so no node could pull it.",
                    stderr.trim()
                ))
                .with_fix(
                    "rebuild and push it with `curie build --plugin-dir <dir> --registry <ref>`, \
                     then redeploy",
                ),
            ));
        }
    };
    let covered = manifest_platforms(raw).map_err(|err| {
        anyhow::Error::from(
            crate::exit::CliError::usage(format!(
                "the registry's answer for {image} is not a manifest this build understands: {err}"
            ))
            .with_fix(
                "rebuild and push it with `curie build --plugin-dir <dir> --registry <ref>`, \
                 then redeploy",
            ),
        )
    })?;
    let missing: Vec<String> = node_architectures
        .iter()
        .filter(|arch| !covered.contains(&format!("linux/{arch}")))
        .cloned()
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    Err(anyhow::Error::from(
        crate::exit::CliError::usage(format!(
            "{image} covers [{}] in the registry, but this cluster's nodes report [{}], so [{}] \
             has no image to run. The lock declares [{}].",
            covered.iter().cloned().collect::<Vec<_>>().join(", "),
            node_architectures
                .iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", "),
            missing.join(", "),
            declared_platforms.join(", "),
        ))
        .with_fix(
            "rebuild every architecture the nodes run with `curie build --plugin-dir <dir> \
             --registry <ref>`, then redeploy",
        ),
    ))
}

/// The lock entries a cluster deploy has to ask the registry about: the entries
/// of the `build:` connectors whose lock delivers through a registry.
///
/// Everything else is out of scope by construction -- an `image:` connector
/// pulls a reference nobody here built, and a `local-daemon` build has already
/// been refused by [`lock_preflight`] at this tier. An empty result is the
/// common bundle, and it queries nothing.
pub fn registry_preflight_targets<'a>(
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: Option<&'a crate::connector_build::ConnectorLockFileDecl>,
) -> Vec<&'a crate::connector_build::ConnectorLockEntryDecl> {
    decl.connectors
        .iter()
        .filter(|(_, spec)| spec.build.is_some())
        .filter_map(|(connector, _)| lock.and_then(|lock| lock.connectors.get(connector)))
        .filter(|entry| entry.delivery == crate::connector_build::Delivery::Registry)
        .collect()
}

/// The impure half of [`registry_preflight`]: query the registry and the
/// cluster, then hand both answers to the decision.
///
/// The node architectures are read ONCE and shared across every connector --
/// they are a property of the cluster, not of the image, so one `kubectl get
/// nodes` covers a bundle building any number of connectors.
async fn run_registry_preflight(
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: Option<&crate::connector_build::ConnectorLockFileDecl>,
) -> Result<()> {
    let targets = registry_preflight_targets(decl, lock);
    if targets.is_empty() {
        return Ok(());
    }

    let (ok, out, err) = crate::ops::run_capture(&node_architectures_argv()).await?;
    if !ok {
        return Err(anyhow::anyhow!("{}", err.trim()))
            .context("read the architectures this cluster's nodes report");
    }
    let node_archs = node_architectures(&out);

    for entry in targets {
        let (ok, raw, err) = crate::ops::run_capture(&registry_manifest_argv(&entry.image)).await?;
        let inspect = if ok { Ok(raw.as_str()) } else { Err(err) };
        registry_preflight(&entry.image, inspect, &node_archs, &entry.platforms)?;
    }
    Ok(())
}

/// The recomputed `source_digest` of every `build:` connector in a bundle, for
/// the preflight to compare the lock against.
pub fn recompute_source_digests(
    plugin_dir: &Path,
    decl: &crate::connector_build::ConnectorsFileDecl,
) -> Result<std::collections::BTreeMap<String, String>> {
    let mut digests = std::collections::BTreeMap::new();
    for (connector, spec) in &decl.connectors {
        let Some(build) = &spec.build else { continue };
        let context = crate::connector_build::resolve_context(plugin_dir, &build.context)
            .with_context(|| format!("connectors.{connector}"))?;
        digests.insert(
            connector.clone(),
            crate::connector_build::source_digest_of(&context, build)
                .with_context(|| format!("connectors.{connector}"))?,
        );
    }
    Ok(digests)
}

/// The hosted `build:` connectors whose locked image no longer stands for this
/// source: either the lock records nothing for them, or it records a different
/// `source_digest` than the tree hashes to now.
///
/// The staleness test is `lock_preflight`'s, verbatim -- a missing lock entry
/// and a digest mismatch are the two failures it refuses a deploy on, and a
/// connector the recompute could not weigh in on is left alone by both. What
/// differs is only what the caller does with the answer: the skill tier rebuilds
/// (ADR 0113's Decision 3), the cluster tier refuses.
///
/// An `image:`-hosted connector has no source to be stale against, and one
/// pointed at an already-running process with `unhosted_url` is not started
/// here, so neither can put this bundle into a build.
pub fn connectors_needing_rebuild(
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: Option<&crate::connector_build::ConnectorLockFileDecl>,
    recomputed: &std::collections::BTreeMap<String, String>,
) -> Vec<String> {
    let mut stale = Vec::new();
    for (connector, spec) in &decl.connectors {
        if spec.build.is_none() || spec.url.is_some() || spec.unhosted_url.is_some() {
            continue;
        }
        let Some(entry) = lock.and_then(|lock| lock.connectors.get(connector)) else {
            stale.push(connector.clone());
            continue;
        };
        if let Some(fresh) = recomputed.get(connector) {
            if &entry.source_digest != fresh {
                stale.push(connector.clone());
            }
        }
    }
    stale
}

// ---------------------------------------------------------------------------
// The local tier's connector bring-up
// ---------------------------------------------------------------------------

/// Reconcile this agent's connector containers against the deployed bundle, then
/// generate the connector compose overlay and bring the declared set up.
///
/// One helper, both local deploy callers (`local deploy` and `deploy-local`), so
/// the shorthand cannot upload a source-built bundle and start no connector.
/// The RESOLVED identity is passed in rather than re-derived from `plugin.json`:
/// `--agent`/`--target` can override the manifest's name, and a helper that read
/// it again would alias the connectors under one identity while the runner
/// dialed another.
pub async fn bring_up_local(
    plugin_dir: &Path,
    lock: &crate::connector_build::ConnectorLockFileDecl,
    identity: &crate::connector_build::ConnectorScope,
    project: &str,
) -> Result<()> {
    use crate::connector_build as cb;

    let decl = cb::load(plugin_dir)?;
    let hosted: Vec<(&String, &cb::ConnectorSpecDecl)> = decl
        .connectors
        .iter()
        .filter(|(_, spec)| spec.url.is_none() && spec.unhosted_url.is_none())
        .collect();

    // Fail closed BEFORE anything is reaped, staged, written, or started -- the
    // same refusal `skill up` performs, which this path used to skip. A declared
    // secret with no value here would otherwise be written into the overlay as a
    // `${NAME}` nothing populates: compose warns, expands it to empty, and the
    // connector comes up authenticating with nothing. It runs above the reap so
    // a bundle that cannot come up does not first tear down the connectors that
    // are serving.
    refuse_missing_connector_secrets(&decl)?;

    // Reconcile before starting: compose only ADDS the services the overlay
    // names, so a connector this bundle version dropped or renamed would keep
    // running -- serving the runner an MCP endpoint the bundle no longer
    // declares. It is deliberately NOT `--remove-orphans` and not a
    // project-label sweep: this compose project holds the api/worker stack and
    // every other locally deployed agent's connectors, so the reap is scoped to
    // this agent's own containers and, within those, to the names the new
    // desired set does not contain. A zero-connector bundle reaches this with an
    // empty desired set, which is how the last one gets removed.
    let desired: std::collections::BTreeSet<String> = hosted
        .iter()
        .map(|(connector, _)| {
            cb::object_name(&identity.release, &identity.agent, connector.as_str())
        })
        .collect();
    for problem in docker::reap_undesired_connectors(project, &identity.agent, &desired).await {
        crate::ui::ui().warn(&problem);
    }

    if hosted.is_empty() {
        return Ok(());
    }

    let mut secret_values = std::collections::BTreeMap::new();
    for (connector, spec) in &hosted {
        for name in cb::declared_secret_names(spec) {
            // The refusal above already proved every one of these resolves, so a
            // gap here is reported as the missing credential it is rather than
            // silently skipped.
            let value = resolve_connector_secret(&name)?
                .ok_or_else(|| cb::missing_secrets_error(std::slice::from_ref(&name)))?;
            secret_values.insert(name, value);
        }
        // Credential files are staged where both emitters expect them, so the
        // container finds bytes rather than an empty mount.
        for (key, declared_path) in &spec.secret_files {
            let value = resolve_connector_secret(key)?
                .ok_or_else(|| cb::missing_secrets_error(std::slice::from_ref(key)))?;
            cb::stage_secret_file(plugin_dir, connector, declared_path, &value)?;
        }
    }

    let overlay = cb::compose_overlay(lock, &decl, identity, project, plugin_dir)?;
    let path = cb::compose_overlay_path(plugin_dir);
    std::fs::create_dir_all(path.parent().expect("the overlay path has a parent"))?;
    std::fs::write(
        &path,
        serde_norway::to_string(&overlay).context("serialize the connector compose overlay")?,
    )
    .with_context(|| format!("write {}", path.display()))?;

    // The values reach the containers through the compose child's environment,
    // where `${NAME}` in the file above expands from -- never through the file,
    // never through argv, and masked in anything printed.
    let command = cb::compose_up_command(&path, project, &secret_values);
    let (ok, _out, err) = crate::ops::run_capture(&command)
        .await
        .context("starting the bundle's connectors")?;
    if !ok {
        bail!("starting the bundle's connectors failed: {}", err.trim());
    }
    Ok(())
}

/// Refuse the WHOLE bundle when a hosted connector declares a secret this box
/// must not hand it, or has no value for -- before a network, a build, or a
/// container exists. Both tiers' single pre-resolution gate, so a name the
/// bundle must not own is refused here rather than at each caller.
///
/// Bring-up used to skip an unresolved secret silently: `skill up` reported
/// success, the connector container exited 1 on its own missing-credential
/// check, and the runner was left holding an MCP URL that connection-refused
/// mid-turn. Between a silent connection-refused mid-turn and an actionable
/// refusal at bring-up, the refusal is the correct behavior. Every gap across
/// every hosted connector is collected first so one run names them all, and the
/// check is resolve-only -- nothing is staged and no value is retained.
fn refuse_missing_connector_secrets(
    decl: &crate::connector_build::ConnectorsFileDecl,
) -> Result<()> {
    // Ahead of every resolve below: a name the bundle must not own is refused
    // before a single value is read, because reading it IS the exfiltration --
    // the sweep below would resolve the operator's own model credential and
    // report the declaration as satisfied.
    crate::connector_build::refuse_reserved_secret_names(decl)?;
    // Then, so a `from_secret` reference keeps its own, more specific refusal
    // (`refuse_out_of_band_secrets` names all three ways forward) instead of
    // being reported as an ordinary unset name by the sweep below.
    for (connector, spec) in &decl.connectors {
        if spec.url.is_some() || spec.unhosted_url.is_some() {
            continue;
        }
        crate::connector_build::refuse_out_of_band_secrets(connector, spec)?;
    }
    let mut missing = Vec::new();
    for name in crate::connector_build::hosted_secret_names(decl) {
        if resolve_connector_secret(&name)?.is_none() {
            missing.push(name);
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    Err(crate::connector_build::missing_secrets_error(&missing))
}

/// One connector credential, from the environment first and the host vault
/// second. Never printed.
fn resolve_connector_secret(name: &str) -> Result<Option<String>> {
    if let Some(value) = std::env::var(name).ok().filter(|v| !v.is_empty()) {
        return Ok(Some(value));
    }
    crate::secrets::get_value(name)
}

/// Which hosted connectors a skill-tier boot starts, each paired with the image
/// it runs, in declaration order.
///
/// The selection is `connector_build::resolved_image`'s, which is also what the
/// local tier's overlay emits -- one rule, so the two tiers cannot resolve the
/// same declaration to two different images. Extracted as a pure seam for two
/// reasons: it is decidable with no Docker daemon, and it leaves the starter
/// below with no image of its own to compute. The inline lock-first copy it
/// replaces is what let a stale lock entry hijack a connector whose declaration
/// had since switched from `build:` to `image:`.
///
/// Every image is resolved before the first container starts, so a bundle
/// missing one is refused whole rather than half-started.
pub fn skill_connector_plan(
    decl: &crate::connector_build::ConnectorsFileDecl,
    lock: &crate::connector_build::ConnectorLockFileDecl,
) -> Result<Vec<(String, String)>> {
    let mut plan = Vec::new();
    for (connector, spec) in &decl.connectors {
        if spec.url.is_some() || spec.unhosted_url.is_some() {
            continue;
        }
        plan.push((
            connector.clone(),
            crate::connector_build::resolved_image(connector, spec, lock)?,
        ));
    }
    Ok(plan)
}

/// Start one container per hosted connector on the runner's private network.
///
/// Returns the container names so `skill down` reaps exactly what this boot
/// created. Credentials are staged first, so each container finds bytes at its
/// declared mount path rather than an empty file.
async fn start_skill_connectors(
    plugin_dir: &Path,
    decl: &crate::connector_build::ConnectorsFileDecl,
    identity: &crate::connector_build::ConnectorScope,
    network: &str,
    project: &str,
) -> Result<Vec<String>> {
    use crate::connector_build as cb;

    let lock = cb::load_lock(plugin_dir)?.unwrap_or_else(|| cb::ConnectorLockFileDecl {
        version: cb::LOCK_VERSION,
        connectors: std::collections::BTreeMap::new(),
    });
    let mut started = Vec::new();
    for (connector, image) in skill_connector_plan(decl, &lock)? {
        let connector = connector.as_str();
        let spec = decl
            .connectors
            .get(connector)
            .expect("the plan names only this declaration's own connectors");
        cb::refuse_out_of_band_secrets(connector, spec)?;
        let mut secret_values = std::collections::BTreeMap::new();
        for name in cb::declared_secret_names(spec) {
            if let Some(value) = resolve_connector_secret(&name)? {
                secret_values.insert(name, value);
            }
        }
        for (key, declared_path) in &spec.secret_files {
            if let Some(value) = resolve_connector_secret(key)? {
                cb::stage_secret_file(plugin_dir, connector, declared_path, &value)?;
            }
        }
        let start = docker::ConnectorStartSpec::from_declaration(
            connector,
            spec,
            &image,
            identity,
            network,
            project,
            plugin_dir,
            &secret_values,
        )?;
        docker::docker_with_env(&start.run_args(), &start.docker_env)
            .await
            .with_context(|| format!("starting connector '{connector}'"))?;
        started.push(start.container_name);
    }
    Ok(started)
}
