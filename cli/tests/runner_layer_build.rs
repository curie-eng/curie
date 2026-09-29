//! `curie build --plugin-dir` on a bundle that layers its own runner image
//! (ADR 0173 decisions 1 and 2, issue #3216).
//!
//! Black box: the real `curie` binary runs against a scratch bundle with a fake
//! `docker` first on PATH. The fake records every argv it receives, answers
//! `buildx imagetools inspect` with a manifest digest for the platform runner,
//! and answers `buildx build ... --metadata-file F` by writing the pushed index
//! digest to F, which is what buildx itself does. So the assertions are about
//! what an operator observes: the build that ran, and the lock left on disk.

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BASE_REF: &str = "ghcr.io/curie-eng/curie-runner:0.10.0";
const REGISTRY: &str = "registry.example/acme";
const BUNDLE: &str = "acme-bot";

fn base_digest() -> String {
    format!("sha256:{}", "b".repeat(64))
}

fn layer_digest() -> String {
    format!("sha256:{}", "c".repeat(64))
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write stub executable");
    let mut permissions = fs::metadata(path).expect("stat stub").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("chmod stub");
}

/// A fake `docker` that logs one line per invocation (argv joined by spaces).
///
/// The `imagetools inspect` answer is grounded in observed output, not guessed:
/// on 2026-09-25, `docker buildx imagetools inspect
/// ghcr.io/curie-eng/curie-runner:0.10.0 --format '{{json .Manifest}}'` printed
/// an index whose top level carries `schemaVersion`, `mediaType`, `digest`
/// (`sha256:97f8e848...`), `size` 1609 and `manifests`. The top-level `digest`
/// is the one field the CLI reads.
fn install_fake_docker(tools: &Path, log: &Path) {
    let script = format!(
        r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
case "$1 $2 $3" in
  "buildx imagetools inspect")
    printf '%s\n' '{{"mediaType":"application/vnd.oci.image.index.v1+json","digest":"{base}","size":1609}}'
    exit 0
    ;;
esac
if [ "$1" = "buildx" ] && [ "$2" = "build" ]; then
  meta=""
  prev=""
  for arg in "$@"; do
    if [ "$prev" = "--metadata-file" ]; then meta="$arg"; fi
    case "$arg" in --metadata-file=*) meta="${{arg#--metadata-file=}}" ;; esac
    prev="$arg"
  done
  if [ -n "$meta" ]; then
    printf '%s' '{{"containerimage.digest":"{layer}"}}' > "$meta"
  fi
  exit 0
fi
exit 0
"#,
        log = log.display(),
        base = base_digest(),
        layer = layer_digest(),
    );
    write_executable(&tools.join("docker"), &script);
}

fn stub_path(tools: &Path) -> OsString {
    let mut entries = vec![tools.to_path_buf()];
    entries.extend(["/bin", "/usr/bin"].iter().map(PathBuf::from));
    std::env::join_paths(entries).expect("join stub PATH")
}

/// A bundle declaring only a runner layer, whose Dockerfile body is `dockerfile`.
fn runner_bundle(root: &Path, dockerfile: &str) -> PathBuf {
    let bundle = root.join("bundle");
    fs::create_dir_all(bundle.join(".claude-plugin")).expect("mkdir manifest dir");
    fs::write(
        bundle.join(".claude-plugin/plugin.json"),
        format!(r#"{{"name":"{BUNDLE}","version":"0.1.0","description":"t"}}"#),
    )
    .expect("write plugin.json");
    fs::create_dir_all(bundle.join("skills/acme-bot")).expect("mkdir skill");
    fs::write(
        bundle.join("skills/acme-bot/SKILL.md"),
        "---\nname: acme-bot\ndescription: t\n---\nhi\n",
    )
    .expect("write SKILL.md");
    fs::write(
        bundle.join("connectors.yaml"),
        "connectors: {}\nrunner:\n  build:\n    context: runner\n    \
         platforms: [linux/amd64, linux/arm64]\n",
    )
    .expect("write connectors.yaml");
    fs::create_dir_all(bundle.join("runner")).expect("mkdir runner");
    fs::write(bundle.join("runner/Dockerfile"), dockerfile).expect("write Dockerfile");
    bundle
}

struct Run {
    output: Output,
    log: String,
}

fn run_build(root: &Path, bundle: &Path) -> Run {
    let tools = root.join("tools");
    let home = root.join("home");
    for dir in [&tools, &home] {
        fs::create_dir_all(dir).expect("mkdir fixture dir");
    }
    let log = root.join("docker.log");
    install_fake_docker(&tools, &log);

    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .current_dir(root)
        .args(["--color=never", "build", "--plugin-dir"])
        .arg(bundle)
        .args(["--registry", REGISTRY, "--runner-image", BASE_REF])
        .env_clear()
        .env("PATH", stub_path(&tools))
        .env("HOME", &home)
        .env("TMPDIR", root)
        .env("LC_ALL", "C")
        .output()
        .expect("run curie build");
    let log = fs::read_to_string(&log).unwrap_or_default();
    Run { output, log }
}

fn describe(run: &Run) -> String {
    format!(
        "status {:?}\nstdout:\n{}\nstderr:\n{}\ndocker log:\n{}",
        run.output.status,
        String::from_utf8_lossy(&run.output.stdout),
        String::from_utf8_lossy(&run.output.stderr),
        run.log
    )
}

fn build_lines(log: &str) -> Vec<&str> {
    log.lines()
        .filter(|line| line.starts_with("buildx build") || line.starts_with("build "))
        .collect()
}

/// AC2 + AC3: the runner layer is built on the digest-pinned platform runner,
/// pushed, and both digests land in `connectors.lock.yaml`.
#[test]
fn a_declared_runner_layer_is_built_on_the_pinned_base_pushed_and_locked() {
    let temp = tempfile::tempdir().expect("tempdir");
    let bundle = runner_bundle(
        temp.path(),
        "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE}\nRUN pip install acme-tools\n",
    );
    let run = run_build(temp.path(), &bundle);
    assert!(run.output.status.success(), "{}", describe(&run));

    let pinned_base = format!("ghcr.io/curie-eng/curie-runner@{}", base_digest());
    let builds = build_lines(&run.log);
    assert_eq!(
        builds.len(),
        1,
        "exactly the runner layer builds\n{}",
        describe(&run)
    );
    let build = builds[0];
    assert!(build.starts_with("buildx build"), "{build}");
    assert!(build.contains("--push"), "the layer is pushed: {build}");
    assert!(
        build.contains(&format!("--build-arg CURIE_RUNNER_IMAGE={pinned_base}")),
        "the build uses exactly the base it records: {build}"
    );
    assert!(
        build.contains(&format!("{REGISTRY}/{BUNDLE}-runner:")),
        "the pushed ref is <registry>/<bundle>-runner:<source tag>: {build}"
    );
    assert!(
        build.contains("linux/amd64") && build.contains("linux/arm64"),
        "every declared platform is built: {build}"
    );
    assert!(
        !build.contains(&format!("CURIE_RUNNER_IMAGE={BASE_REF}")),
        "the mutable tag must never reach the build: {build}"
    );

    let lock_text =
        fs::read_to_string(bundle.join("connectors.lock.yaml")).expect("a lock is written");
    let lock: serde_json::Value = serde_norway::from_str(&lock_text).expect("the lock is YAML");
    let runner = &lock["runner"];
    assert_eq!(
        runner["image"].as_str(),
        Some(format!("{REGISTRY}/{BUNDLE}-runner@{}", layer_digest()).as_str()),
        "{lock_text}"
    );
    assert_eq!(
        runner["base"].as_str(),
        Some(pinned_base.as_str()),
        "{lock_text}"
    );
    assert_eq!(runner["delivery"].as_str(), Some("registry"), "{lock_text}");
    assert!(
        runner["source_digest"]
            .as_str()
            .is_some_and(|d| d.starts_with("sha256:") && d.len() == 71),
        "{lock_text}"
    );

    // The written lock is one the CLI's own reader accepts.
    let parsed = curie::connector_build::parse_lock(&lock_text).expect("the lock round trips");
    assert!(parsed.runner.is_some(), "{lock_text}");
}

/// A runner Dockerfile naming a literal base is refused before any docker build
/// runs, and leaves no lock behind.
#[test]
fn a_literal_base_in_the_runner_dockerfile_is_refused_before_building() {
    let temp = tempfile::tempdir().expect("tempdir");
    let bundle = runner_bundle(
        temp.path(),
        "FROM ghcr.io/curie-eng/curie-runner:0.10.0\nRUN pip install acme-tools\n",
    );
    let run = run_build(temp.path(), &bundle);
    assert!(!run.output.status.success(), "{}", describe(&run));
    assert!(
        build_lines(&run.log).is_empty(),
        "no image may be built from an unpinned base\n{}",
        describe(&run)
    );
    assert!(
        String::from_utf8_lossy(&run.output.stderr).contains("CURIE_RUNNER_IMAGE"),
        "the refusal names the build argument to use\n{}",
        describe(&run)
    );
    assert!(
        !bundle.join("connectors.lock.yaml").exists(),
        "a refused build writes no lock"
    );
}
