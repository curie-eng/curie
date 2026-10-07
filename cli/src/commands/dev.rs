//! `curie build`, `install`, `update`, and the `curie dev` contributor commands.

use super::*;

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
        let qualified = crate::local::source_image_ref(short, crate::local::SOURCE_IMAGE_TAG);
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

/// The `docker build` command line [`build_image`] runs from the repo root,
/// as it announces it and as a `local up --build --dry-run` plan lists it.
pub(crate) fn build_image_command_line(dockerfile: &str, tag: &str) -> String {
    let rendered = platform_image_tags(dockerfile, tag)
        .iter()
        .map(|image_tag| format!("-t {image_tag}"))
        .collect::<Vec<_>>()
        .join(" ");
    format!("docker build -f {dockerfile} {rendered} .")
}

/// Build one platform image. The single `docker build` invocation for Curie
/// images: `curie build` and `curie local up --build` both route here (#1931).
pub(crate) async fn build_image(dockerfile: &str, tag: &str) -> Result<()> {
    let ui = crate::ui::ui();
    if !crate::ops::on_path("docker") {
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
    ui.note(&format!(
        "=== {} (in {}) ===",
        build_image_command_line(dockerfile, tag),
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
            vec![
                RUNNER_IMAGE.to_string(),
                source_image_ref(RUNNER_IMAGE, crate::local::SOURCE_IMAGE_TAG),
            ]
        );
    }

    #[test]
    fn runner_build_stack_ref_also_tags_the_short_name() {
        let qualified = source_image_ref(RUNNER_IMAGE, crate::local::SOURCE_IMAGE_TAG);
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
        let tag = source_image_ref("curie-api", crate::local::SOURCE_IMAGE_TAG);
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
    configure_source_hooks(&root)?;

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
    configure_source_hooks(&root)?;
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
pub(super) enum EnvSeed {
    Preserved,
    Created,
    NoTemplate,
}

pub(super) fn seed_env_if_missing(root: &Path) -> Result<EnvSeed> {
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

/// Install this checkout's tracked Git hooks through the relative shared path.
pub fn dev_hooks_install() -> Result<()> {
    let root = find_repo_root().ok_or_else(|| {
        crate::exit::usage("Run `curie dev hooks install` from a Curie source checkout.")
    })?;
    configure_source_hooks(&root)
}

/// A relative hooks path lets every linked worktree run its own tracked hook.
fn configure_source_hooks(root: &Path) -> Result<()> {
    let git_root = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(root)
        .output()
        .context("Git is required to install hooks")?;
    if !git_root.status.success() {
        bail!("This Curie source directory is not a Git checkout.");
    }
    let git_root_path =
        String::from_utf8(git_root.stdout).context("Git returned an invalid path")?;
    if Path::new(git_root_path.trim()).canonicalize()? != root.canonicalize()? {
        bail!("Run `curie dev hooks install` from the root Git checkout.");
    }

    let hook = root.join(".githooks/pre-push");
    if !hook.is_file() {
        bail!(
            "The tracked hook is missing from this source checkout: {}",
            hook.display()
        );
    }
    if !is_executable(&hook) {
        bail!(
            "The tracked source checkout hook is not executable: {}",
            hook.display()
        );
    }

    let current = std::process::Command::new("git")
        .args(["config", "--get", "core.hooksPath"])
        .current_dir(root)
        .output()
        .context("Could not read the local Git hook configuration")?;
    match current.status.code() {
        Some(0) => {
            let value = String::from_utf8(current.stdout)
                .context("The local Git hook path is not valid UTF8")?;
            if value.trim() != ".githooks" {
                crate::ui::ui().warn(&format!(
                    "Warning: core.hooksPath is already set to `{}`; preserving it. To use the tracked Curie hooks, run `git config --local core.hooksPath .githooks`.",
                    value.trim()
                ));
                return Ok(());
            }
        }
        Some(1) if current.stdout.is_empty() => {}
        _ => bail!("Could not read the Git hook configuration."),
    }

    let status = std::process::Command::new("git")
        .args(["config", "--local", "core.hooksPath", ".githooks"])
        .current_dir(root)
        .status()
        .context("Could not set the repository Git hook path")?;
    if !status.success() {
        bail!("Could not set the repository Git hook path.");
    }

    crate::ui::ui().success("Git hooks configured with core.hooksPath=.githooks.");
    Ok(())
}

/// One selected preflight gate, including its mirrored CI command when present.
#[derive(Deserialize, Serialize)]
pub struct PreflightCheck {
    pub name: String,
    pub status: String,
    pub detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workflow: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<String>,
}

#[derive(Deserialize, Serialize)]
pub struct PreflightFailure {
    pub check: String,
    pub output_tail: String,
    pub exit_code: i32,
}

#[derive(Deserialize, Serialize)]
pub struct PreflightBase {
    #[serde(rename = "ref")]
    pub reference: String,
    pub contains_tip: bool,
    pub failing_required_checks: Vec<String>,
}

/// The source-owned report is shared by both tiers and their dry runs.
#[derive(Deserialize, Serialize)]
pub struct PreflightOutput {
    pub checks: Vec<PreflightCheck>,
    pub failures: Vec<PreflightFailure>,
    pub passed: bool,
    pub dry_run: bool,
    pub tier: String,
    pub base: PreflightBase,
    pub ci_only: Vec<String>,
    pub head: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fix: Option<String>,
}

#[derive(Deserialize)]
struct PreflightSetupError {
    error: String,
    fix: String,
}

impl crate::ui::CliOutput for PreflightOutput {
    fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("preflight report contains only serializable fields")
    }

    fn render(&self, ui: &crate::ui::Ui) {
        for check in &self.checks {
            ui.payload_plain(&format!(
                "{}: {}\n{}",
                check.name, check.status, check.detail
            ));
        }
        for entry in &self.ci_only {
            ui.payload_plain(&format!("{entry}: runs in CI only"));
        }
        if self.dry_run {
            ui.note(&format!(
                "{} preflight plan: {} checks.",
                self.tier,
                self.checks.len()
            ));
        } else if self.passed {
            ui.success(&format!(
                "{} preflight passed: {} checks.",
                self.tier,
                self.checks.len()
            ));
        }
    }
}

/// Run the source-owned planner and preserve its single structured result.
pub async fn dev_preflight(
    fast: bool,
    base: &str,
    dry_run: bool,
    pr_body: Option<&Path>,
    title: Option<&str>,
) -> Result<PreflightOutput> {
    let root = find_repo_root().ok_or_else(|| {
        crate::exit::CliError::usage("Preflight requires a Curie source checkout.")
            .with_fix("Run `curie dev preflight` from a Curie source checkout.")
    })?;
    if !root.join("tools/preflight/preflight.py").is_file() {
        return Err(crate::exit::CliError::usage(
            "The source checkout has no preflight tool.",
        )
        .with_fix("Update this Curie source checkout to a revision containing the preflight tool.")
        .into());
    }
    // Resolve the caller's file before changing the subprocess working directory.
    let body_file = pr_body
        .map(|path| std::env::current_dir().map(|cwd| cwd.join(path)))
        .transpose()
        .context("Could not resolve the proposed pull request body file")?;
    let mut command = tokio::process::Command::new("uv");
    command
        .args([
            "run",
            "--no-project",
            "--with",
            "pyyaml==6.0.3",
            "python3",
            "tools/preflight/preflight.py",
            "--base",
            base,
            "--json",
        ])
        .current_dir(&root)
        .stderr(std::process::Stdio::inherit());
    if fast {
        command.arg("--fast");
    }
    if dry_run {
        command.arg("--dry-run");
    }
    if let Some(path) = body_file {
        command.arg("--pr-body").arg(path);
    }
    if let Some(title) = title {
        command.arg("--title").arg(title);
    }
    let output = command.output().await.map_err(|error| {
        crate::exit::CliError::transient(format!("Could not run the preflight tool: {error}"))
            .with_fix("Install uv, then run `curie install` from this source checkout and retry.")
    })?;
    let payload: serde_json::Value = serde_json::from_slice(&output.stdout)
        .context("The preflight tool did not return one JSON object")?;
    if output.status.code() == Some(2) {
        let error: PreflightSetupError = serde_json::from_value(payload)
            .context("The preflight tool returned an invalid setup error")?;
        return Err(crate::exit::CliError::usage(error.error)
            .with_fix(error.fix)
            .into());
    }
    let report: PreflightOutput =
        serde_json::from_value(payload).context("The preflight tool returned an invalid report")?;
    if !output.status.success() {
        let message = report
            .error
            .as_deref()
            .context("The failed preflight report omitted its error")?;
        let fix = report
            .fix
            .as_deref()
            .context("The failed preflight report omitted its fix")?;
        let error = if output.status.code() == Some(3) {
            crate::exit::CliError::transient(message)
        } else {
            crate::exit::CliError::failure(message)
        }
        .with_fix(fix);
        return Err(crate::ui::ui().failed_report(&report, error.into()));
    }
    Ok(report)
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
pub(super) fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|m| m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
pub(super) fn is_executable(_path: &Path) -> bool {
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

/// `curie dev bump-version <X.Y.Z>`: set the release-coupled version across
/// cli/Cargo.toml + Chart.yaml version/appVersion and promote the candidate
/// schema window under that version. It refuses to change a window whose
/// version is registered in the architecture atlas. It rewrites ONLY the
/// line-anchored release fields (never a
/// dependency `version = ` line), refreshes the CLI lockfile, and prints the
/// commit + tag follow-up -- it does not commit, tag, or push. `--dry-run` prints
/// the planned edits and writes nothing.
pub async fn bump_version(version: &str, dry_run: bool) -> Result<()> {
    let ui = crate::ui::ui();
    // semver X.Y.Z with an optional -rc.N (the only pre-release shape we cut).
    let semver = regex::Regex::new(
        r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.(0|[1-9][0-9]*))?$",
    )
    .expect("static regex");
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
    let catalog_path = root.join("cli/src/application_schema_windows.json");
    let atlas_path = root.join("docs/architecture-atlas/versions.json");
    let cargo = std::fs::read_to_string(&cargo_path)
        .with_context(|| format!("reading {}", cargo_path.display()))?;
    let chart = std::fs::read_to_string(&chart_path)
        .with_context(|| format!("reading {}", chart_path.display()))?;
    let catalog = std::fs::read_to_string(&catalog_path)
        .with_context(|| format!("reading {}", catalog_path.display()))?;
    let atlas = std::fs::read_to_string(&atlas_path)
        .with_context(|| format!("reading {}", atlas_path.display()))?;
    let published = atlas_version_registered(&atlas, version)?;
    let catalog_value: serde_json::Value =
        serde_json::from_str(&catalog).context("parsing application schema window catalog")?;
    let had_window = catalog_value
        .get("windows")
        .and_then(serde_json::Value::as_object)
        .context("catalog has no release windows")?
        .contains_key(version);
    let catalog_new = promote_candidate_window(&catalog, version, published)?;
    let catalog_changed = catalog_new != catalog;

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
                if catalog_changed {
                    if had_window {
                        format!(
                            "cli/src/application_schema_windows.json: update unpublished windows[{version}] from candidate"
                        )
                    } else {
                        format!(
                            "cli/src/application_schema_windows.json: promote candidate to windows[{version}]"
                        )
                    }
                } else {
                    format!(
                        "cli/src/application_schema_windows.json: windows[{version}] already matches candidate"
                    )
                },
                "cargo update -p curie (refresh Cargo.lock)".to_string(),
            ],
        });
        return Ok(());
    }

    if catalog_changed {
        std::fs::write(&catalog_path, catalog_new)
            .with_context(|| format!("writing {}", catalog_path.display()))?;
    }
    std::fs::write(&cargo_path, cargo_new)
        .with_context(|| format!("writing {}", cargo_path.display()))?;
    std::fs::write(&chart_path, chart_new)
        .with_context(|| format!("writing {}", chart_path.display()))?;
    ui.note(&format!(
        "set version {version} in cli/Cargo.toml and charts/curie/Chart.yaml"
    ));
    if catalog_changed {
        ui.note(&format!(
            "{} the candidate schema window for application {version}",
            if had_window { "updated" } else { "promoted" }
        ));
    } else {
        ui.note(&format!(
            "application {version} schema window already matches the candidate"
        ));
    }

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

pub(super) fn atlas_version_registered(manifest: &str, version: &str) -> Result<bool> {
    let atlas: serde_json::Value =
        serde_json::from_str(manifest).context("parsing architecture atlas versions")?;
    let versions = atlas
        .get("versions")
        .and_then(serde_json::Value::as_array)
        .context("architecture atlas versions manifest is malformed")?;
    let target = format!("v{version}");
    let mut registered = false;
    for entry in versions {
        let id = entry
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("architecture atlas version entry has no id")?;
        registered |= id == target;
    }
    Ok(registered)
}

pub(super) fn promote_candidate_window(
    catalog: &str,
    version: &str,
    published: bool,
) -> Result<String> {
    let mut payload: serde_json::Value =
        serde_json::from_str(catalog).context("parsing application schema window catalog")?;
    let candidate = payload
        .get("candidate")
        .and_then(serde_json::Value::as_object)
        .context("catalog has no candidate schema window")?
        .clone();
    let candidate_min = candidate
        .get("schema_min")
        .and_then(serde_json::Value::as_str)
        .context("candidate has no schema_min")?;
    let candidate_head = candidate
        .get("schema_head")
        .and_then(serde_json::Value::as_str)
        .context("candidate has no schema_head")?;
    let revisions = payload
        .get("revisions")
        .and_then(serde_json::Value::as_array)
        .context("catalog has no revisions")?;
    let min_index = revisions
        .iter()
        .position(|revision| revision.as_str() == Some(candidate_min))
        .context("candidate schema_min is not a catalog revision")?;
    let head_index = revisions
        .iter()
        .position(|revision| revision.as_str() == Some(candidate_head))
        .context("candidate schema_head is not a catalog revision")?;
    if min_index > head_index {
        bail!("candidate schema_min is after candidate schema_head");
    }
    let windows = payload
        .get_mut("windows")
        .and_then(serde_json::Value::as_object_mut)
        .context("catalog has no release windows")?;
    let candidate = serde_json::Value::Object(candidate);
    if let Some(existing) = windows.get(version) {
        if existing == &candidate {
            return Ok(catalog.to_string());
        }
        if published {
            bail!(
                "application {version} is registered in the architecture atlas; bump to the next version"
            );
        }
    }
    if published && !windows.contains_key(version) {
        bail!(
            "application {version} is registered in the architecture atlas but has no catalog window; cannot promote the candidate"
        );
    }
    windows.insert(version.to_string(), candidate);
    let mut updated = serde_json::to_string_pretty(&payload)
        .context("serializing application schema window catalog")?;
    updated.push('\n');
    Ok(updated)
}

/// Replace the first line beginning with `prefix` (after optional leading
/// whitespace) with `replacement`, preserving the line's indentation. Returns
/// None when no such line exists.
pub(super) fn replace_first_line(content: &str, prefix: &str, replacement: &str) -> Option<String> {
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
pub(super) fn require_tool(bin: &str, hint: &str) -> Result<()> {
    if crate::ops::on_path(bin) {
        Ok(())
    } else {
        bail!("{hint}")
    }
}

/// Run one install step in `dir`, streaming its output and failing on nonzero.
pub(super) async fn run_step(dir: &Path, bin: &str, args: &[&str], label: &str) -> Result<()> {
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

pub(super) async fn docker_image_exists(tag: &str) -> Result<bool> {
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
