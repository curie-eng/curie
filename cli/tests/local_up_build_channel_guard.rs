// Regression tests for curie issue 1926.
//
// `curie local up --build` guarded itself against the wrong axis: it checked
// whether the CWD looked like a source checkout (find_repo_root), not
// whether the BUILD CHANNEL resolves a fetched, version-pinned
// compose.release.yaml. On a release-channel binary the pinned compose file
// never reads the `:dev` tags --build writes, so the build silently has no
// effect on what actually runs. The fix must refuse before building or
// fetching anything whenever the resolved compose is a fetched release
// compose rather than a local/override one.
//
// Stream discipline, because it is what made the first version of these tests
// vacuous: the dry-run compose plan goes to STDOUT via `Ui::emit`, while
// `Ui::success` and `Ui::note` (the "built N image(s) as :dev" line and the
// "compose source: <url>" note) go to STDERR. An assertion about what was NOT
// built therefore has to read stderr.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// Make the temp cwd look like a real source checkout AND give the compose
/// resolver a local file to find. The two files aim at two different guards:
///
/// - `runner/Dockerfile` is the sentinel `commands::find_repo_root()` walks up
///   looking for, so it is what makes this directory a checkout. Without it the
///   "release channel INSIDE a checkout" combination from AC4 is never actually
///   set up, and the old cwd-axis guard this fix replaces would not even fire.
/// - `compose.dev.yaml` is what `artifacts::resolve_compose` looks for on the
///   DEV channel to return `Resolved::Local`. On the RELEASE channel that arm
///   ignores `local_exists` entirely, which is exactly why the release-channel
///   tests below still resolve a fetched `compose.release.yaml` even though
///   this file is present.
fn write_checkout(root: &Path) {
    fs::create_dir_all(root.join("runner")).expect("create runner directory");
    fs::write(root.join("runner/Dockerfile"), "FROM scratch\n").expect("write runner/Dockerfile");
    fs::write(root.join("compose.dev.yaml"), "services: {}\n").expect("write compose.dev.yaml");
}

/// A `docker` stub, first on PATH, that records every invocation to `log` and
/// exits 0. If the log file exists after a refused run, something reached
/// docker (an image build, or a `docker compose up`) before the refusal.
fn write_docker_stub(tools: &Path, log: &Path) {
    fs::create_dir_all(tools).expect("create tools directory");
    let path = tools.join("docker");
    let body = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nexit 0\n",
        log.display()
    );
    fs::write(&path, body).expect("write docker stub");
    let mut permissions = fs::metadata(&path)
        .expect("docker stub metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make docker stub executable");
}

fn path_with(tools: &Path) -> std::ffi::OsString {
    let mut paths = vec![tools.to_path_buf()];
    if let Some(path) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&path));
    }
    std::env::join_paths(paths).expect("join PATH")
}

/// Every file named `name` anywhere under `root` (missing root counts as none).
fn find_files_named(root: &Path, name: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = fs::read_dir(root) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(find_files_named(&path, name));
        } else if path.file_name().and_then(|n| n.to_str()) == Some(name) {
            found.push(path);
        }
    }
    found
}

/// Run `curie --color=never local up <args>` in `temp` with the env every test
/// here shares: a temp XDG cache and config dir, `LC_ALL=C`, and no ambient
/// model credential or fake-model pin. `channel` sets
/// `CURIE_TEST_ARTIFACT_CHANNEL` (`None` removes it, for the dev-channel case),
/// and `tools`, when given, is prepended to `PATH` so a stub `docker` wins.
fn run_local_up(
    temp: &Path,
    args: &[&str],
    channel: Option<&str>,
    tools: Option<&Path>,
) -> std::process::Output {
    let mut command = Command::new(bin());
    command
        .current_dir(temp)
        .args(["--color=never", "local", "up"])
        .args(args)
        .env("LC_ALL", "C")
        .env("XDG_CACHE_HOME", temp.join("cache"))
        .env("CURIE_CONFIG_DIR", temp.join("config"))
        .env_remove("CURIE_CREDENTIALS")
        .env_remove("ANTHROPIC_API_KEY")
        .env_remove("CLAUDE_CODE_OAUTH_TOKEN")
        .env_remove("CURIE_FAKE_MODEL");
    match channel {
        Some(channel) => command.env("CURIE_TEST_ARTIFACT_CHANNEL", channel),
        None => command.env_remove("CURIE_TEST_ARTIFACT_CHANNEL"),
    };
    if let Some(tools) = tools {
        command.env("PATH", path_with(tools));
    }
    command.output().expect("run curie local up")
}

/// AC1's ordering clause, proven without `--dry-run`.
///
/// `local::up` returns the dry-run plan BEFORE it ever reaches
/// `build_source_images`, so a `--dry-run` run could not have built anything
/// with or without the guard. Only a real run reaches the point where a build
/// and a release-compose download would actually be attempted, so this is the
/// test that pins the ordering.
#[test]
fn release_channel_build_inside_a_checkout_refuses_before_building() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    write_checkout(temp.path());

    let tools = temp.path().join("tools");
    let docker_log = temp.path().join("docker-invocations.log");
    write_docker_stub(&tools, &docker_log);

    let cache = temp.path().join("cache");
    fs::create_dir_all(&cache).expect("create cache directory");

    let output = run_local_up(
        temp.path(),
        &["--build", "--minimal"],
        Some("release"),
        Some(&tools),
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(2),
        "refusal must be the structured Usage exit class, not a bare failure\nstdout: {stdout}\nstderr: {stderr}"
    );

    assert!(
        stderr.contains("--build"),
        "stderr must name the flag that was refused\n{stderr}"
    );
    assert!(
        stderr.contains("compose.release.yaml"),
        "stderr must name the resolved release compose as the real reason\n{stderr}"
    );
    assert!(
        stderr.contains("compose.dev.yaml"),
        "stderr must name the escape hatch (-f compose.dev.yaml)\n{stderr}"
    );

    // Nothing ran docker: no image build, no `docker compose up`.
    assert!(
        !docker_log.exists(),
        "docker must never be invoked before the refusal; stub log: {}\nstderr: {stderr}",
        fs::read_to_string(&docker_log).unwrap_or_default()
    );

    // THIS is the assertion that kills the move-the-guard-after-materialize
    // mutation: `materialize_artifact` calls `artifacts::ensure_cached`, which
    // downloads the version-pinned compose.release.yaml into XDG_CACHE_HOME. If
    // the guard is moved to AFTER that call, the file lands here and this fails.
    let cached = find_files_named(&cache, "compose.release.yaml");
    assert!(
        cached.is_empty(),
        "the release compose must never be downloaded before the refusal; found {cached:?}\nstderr: {stderr}"
    );

    // The "compose source: <url>" note is the dry-run half of the same
    // ordering property, and it is on stderr, not stdout.
    assert!(
        !stderr.contains("compose source:"),
        "the compose-source note must not be emitted before the refusal\n{stderr}"
    );
}

/// The `--dry-run` variant of the refusal. It cannot carry the ordering proof
/// (see the test above), but it does pin the message, the exit class, and the
/// fact that no compose plan reaches stdout.
#[test]
fn release_channel_build_dry_run_inside_a_checkout_refuses() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    write_checkout(temp.path());

    let output = run_local_up(
        temp.path(),
        &["--build", "--dry-run", "--minimal"],
        Some("release"),
        None,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(
        output.status.code(),
        Some(2),
        "refusal must be the structured Usage exit class, not a bare failure\nstdout: {stdout}\nstderr: {stderr}"
    );

    assert!(
        stderr.contains("--build"),
        "stderr must name the flag that was refused\n{stderr}"
    );
    assert!(
        stderr.contains("compose.release.yaml"),
        "stderr must name the resolved release compose as the real reason\n{stderr}"
    );
    assert!(
        stderr.contains("compose.dev.yaml"),
        "stderr must name the escape hatch (-f compose.dev.yaml)\n{stderr}"
    );

    // The success line lives on STDERR ("built N image(s) as :dev"), so the
    // "nothing was built" assertion has to read stderr to mean anything.
    assert!(
        !stderr.contains("image(s) as :dev"),
        "nothing must be built before the refusal\n{stderr}"
    );
    // The dry-run compose plan does go to stdout, via Ui::emit.
    assert!(
        !stdout.contains("docker compose"),
        "no compose plan must be emitted before the refusal\n{stdout}"
    );
}

/// Over-refusal control: an explicit `-f` local compose is substitutable, so it
/// must still be accepted on the release channel.
#[test]
fn release_channel_build_with_an_explicit_local_compose_is_accepted() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    write_checkout(temp.path());
    let compose_file = temp.path().join("compose.dev.yaml");

    let output = run_local_up(
        temp.path(),
        &[
            "--build",
            "--dry-run",
            "--minimal",
            "-f",
            compose_file.to_str().expect("compose path is UTF 8"),
        ],
        Some("release"),
        None,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "an explicit local compose override must be accepted even on the release channel\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("docker compose"),
        "stdout must contain the compose plan\n{stdout}"
    );
    assert!(
        stdout.contains("CURIE_BASE_TAG=dev"),
        "stdout must show the dev base tag being written\n{stdout}"
    );
}

/// Axis control: the guard keys on the resolved compose, not on the cwd. On the
/// dev channel `resolve_compose` returns `Resolved::Local` for the cwd's
/// compose.dev.yaml, so `--build` must be allowed here even though this is the
/// same checkout-shaped directory the release-channel test above is refused in.
#[test]
fn dev_channel_build_with_a_local_compose_is_accepted() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    write_checkout(temp.path());

    let output = run_local_up(
        temp.path(),
        &["--build", "--dry-run", "--minimal"],
        None,
        None,
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "dev-channel --build with a local compose.dev.yaml must not be refused\nstdout: {stdout}\nstderr: {stderr}"
    );
    assert!(
        stdout.contains("CURIE_BASE_TAG=dev"),
        "stdout must show the dev base tag being written\n{stdout}"
    );
}

/// `--dry-run --build` lists the image builds the real run performs (#1929
/// item 7). They are its dominant cost and its only filesystem side effect,
/// so a plan that shows only the compose line understates what the run does.
///
/// The expected lines come from `local::source_images_for`, the list
/// `build_source_images` iterates, so the plan cannot name a different set of
/// images than the build builds. The docker stub proves the dry run still
/// builds nothing.
#[test]
fn build_dry_run_plans_every_image_build_before_the_compose_line() {
    for minimal in [true, false] {
        let temp = tempfile::tempdir().expect("create temporary directory");
        write_checkout(temp.path());
        let tools = temp.path().join("tools");
        let docker_log = temp.path().join("docker-invocations.log");
        write_docker_stub(&tools, &docker_log);

        let mut args = vec!["--build", "--dry-run", "--json"];
        if minimal {
            args.push("--minimal");
        }
        let output = run_local_up(temp.path(), &args, None, Some(&tools));
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success(),
            "dev-channel --build --dry-run must succeed\nstdout: {stdout}\nstderr: {stderr}"
        );

        let doc: serde_json::Value =
            serde_json::from_str(stdout.trim()).expect("--json dry run emits one JSON object");
        let plan: Vec<String> = doc["plan"]
            .as_array()
            .expect("the dry run carries a plan array")
            .iter()
            .map(|line| line.as_str().expect("plan lines are strings").to_string())
            .collect();

        let images = curie::local::source_images_for(minimal);
        assert_eq!(
            plan.len(),
            images.len() + 1,
            "one docker build per source image, then the compose line: {plan:#?}"
        );
        for (line, image) in plan.iter().zip(&images) {
            let tag = curie::local::source_image_ref(image.image, curie::local::SOURCE_IMAGE_TAG);
            assert!(
                line.starts_with(&format!("docker build -f {} -t {tag}", image.dockerfile))
                    && line.ends_with(" ."),
                "expected the {} build, got: {line}\nplan: {plan:#?}",
                image.image
            );
        }
        let last = plan.last().expect("the plan is not empty");
        assert!(
            last.contains("docker compose"),
            "the compose line comes after every build: {plan:#?}"
        );

        // A dry run may still READ docker state (the stack-key resolver
        // inspects the Postgres volume); it must never build.
        let invoked = fs::read_to_string(&docker_log).unwrap_or_default();
        assert!(
            !invoked.lines().any(|line| line.starts_with("build")),
            "a dry run must not run docker build; stub log: {invoked}"
        );
    }
}
