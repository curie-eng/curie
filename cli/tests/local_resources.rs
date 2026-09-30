//! Isolation contract for local CLI resources (#2780).
//!
//! One configuration path: Compose project, ordered compose files, and matching
//! host endpoints. Default shared resources are that path's default, not a
//! second implementation. Outcomes are observed through the CLI binary
//! (`--help`, `--dry-run`, `--json` usage errors), never through internal
//! struct fields.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn compose_dev() -> PathBuf {
    repo_root().join("compose.dev.yaml")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn run_local_up(args: &[&str]) -> Output {
    Command::new(bin())
        .args(["--json", "local", "up"])
        .args(args)
        .current_dir(repo_root())
        .env_remove("COMPOSE_PROJECT_NAME")
        .env_remove("COMPOSE_FILE")
        .output()
        .expect("run curie local up")
}

fn write_ports_only_override(dir: &Path) -> PathBuf {
    let path = dir.join("compose.override.yaml");
    fs::write(
        &path,
        r#"services:
  postgres:
    ports: !override ["127.0.0.1:35432:5432"]
  valkey:
    ports: !override ["127.0.0.1:36379:6379"]
  rustfs:
    ports: !override ["127.0.0.1:39000:9000", "127.0.0.1:39001:9001"]
  curie-api:
    ports: !override ["127.0.0.1:38000:8000"]
networks:
  curie_runner:
    name: "${COMPOSE_PROJECT_NAME}_runner"
"#,
    )
    .expect("write ports-only override");
    path
}

#[test]
fn local_up_help_exposes_project_and_repeatable_files() {
    let output = Command::new(bin())
        .args(["local", "up", "--help"])
        .output()
        .expect("run curie local up --help");
    assert!(output.status.success(), "{}", output_text(&output));
    let text = output_text(&output);
    assert!(
        text.contains("--project") && text.contains("COMPOSE_PROJECT_NAME"),
        "local up must take --project from COMPOSE_PROJECT_NAME: {text}"
    );
    assert!(
        text.contains("-f") && (text.contains("repeat") || text.contains("ordered")),
        "local up must document ordered/repeated -f compose files: {text}"
    );
}

#[test]
fn incomplete_isolation_is_a_usage_error() {
    let output = run_local_up(&["--project", "curie-check-2780-a", "--dry-run"]);
    assert_eq!(
        output.status.code(),
        Some(2),
        "incomplete isolation must be usage; {}",
        output_text(&output)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let text = output_text(&output);
    assert!(
        !text.contains("unexpected argument"),
        "isolation must be a recognized flag, not clap rejection: {text}"
    );
    let value: serde_json::Value =
        serde_json::from_str(stdout.trim()).unwrap_or(serde_json::json!({}));
    let error = value["error"].as_str().unwrap_or(&text);
    assert!(
        error.contains("all together") || error.contains("incomplete"),
        "usage error must name the incomplete isolation contract: {text}"
    );
    assert!(
        error.contains("COMPOSE") || error.contains("compose"),
        "usage error must name the missing compose files: {text}"
    );
    assert!(
        value["fix"].as_str().is_some_and(|fix| !fix.is_empty()) || text.contains("fix"),
        "usage error must carry a fix: {text}"
    );
}

#[test]
fn compose_file_without_project_is_a_usage_error() {
    let compose = compose_dev();
    let output = Command::new(bin())
        .args(["--json", "local", "up", "--dry-run"])
        .current_dir(repo_root())
        .env_remove("COMPOSE_PROJECT_NAME")
        .env(
            "COMPOSE_FILE",
            format!("{}:/tmp/curie-missing-override.yaml", compose.display()),
        )
        .output()
        .expect("run curie local up with COMPOSE_FILE only");
    assert_eq!(
        output.status.code(),
        Some(2),
        "COMPOSE_FILE without COMPOSE_PROJECT_NAME must be usage; {}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("COMPOSE_PROJECT_NAME"),
        "must name the missing project: {text}"
    );
}

#[test]
fn default_local_up_dry_run_pins_project_curie() {
    let output = run_local_up(&[
        "--dry-run",
        "-f",
        compose_dev().to_str().expect("utf-8 compose path"),
    ]);
    assert!(
        output.status.success(),
        "default local up --dry-run must succeed; {}",
        output_text(&output)
    );
    let text = output_text(&output);
    assert!(
        text.contains("-p curie") || text.contains("COMPOSE_PROJECT_NAME=curie"),
        "default path must pin project curie: {text}"
    );
}

#[test]
fn ordered_compose_files_and_project_reach_dry_run_argv() {
    let dir = tempfile::tempdir().expect("tempdir");
    let override_path = write_ports_only_override(dir.path());
    let compose = compose_dev();
    let output = Command::new(bin())
        .args([
            "--json",
            "local",
            "up",
            "--dry-run",
            "--project",
            "curie-check-2780-a",
            "-f",
            compose.to_str().expect("utf-8"),
            "-f",
            override_path.to_str().expect("utf-8"),
        ])
        .current_dir(repo_root())
        .env("CURIE_API_URL", "http://127.0.0.1:38000")
        .env("VALKEY_HOST", "127.0.0.1")
        .env("VALKEY_PORT", "36379")
        .env("S3_ENDPOINT_URL", "http://127.0.0.1:39000")
        .env("CURIE_DOCKER_NETWORK", "curie-check-2780-a_runner")
        .env("CURIE_LOCAL_STUB_PORT", "18155")
        .env("CURIE_LOCAL_POSTGRES_HOST", "127.0.0.1")
        .env("CURIE_LOCAL_POSTGRES_PORT", "35432")
        .env("CURIE_LOCAL_STAGING_DIR", "/tmp/curie-bundles-2780-a")
        .env("CURIE_LOCAL_IMAGE_TAG", "dev-2780-a")
        .output()
        .expect("run isolated local up dry-run");
    let text = output_text(&output);
    // A ports-only override that leaves worker DATABASE_URL/VALKEY/S3 on the
    // default host ports is mismatched configuration, not a successful plan.
    assert_eq!(
        output.status.code(),
        Some(2),
        "ports-only override must be mismatched worker env; {text}"
    );
    assert!(
        text.contains("DATABASE_URL") || text.contains("VALKEY_PORT") || text.contains("worker"),
        "mismatch must name the host-network worker literals: {text}"
    );
}

#[test]
fn query_time_api_url_is_not_an_isolation_trigger() {
    let output = Command::new(bin())
        .args([
            "--json",
            "local",
            "observability",
            "runs",
            "--limit",
            "1",
            "--agent-id",
            "00000000-0000-0000-0000-000000000001",
        ])
        .current_dir(repo_root())
        .env("CURIE_API_URL", "http://127.0.0.1:1")
        .env_remove("COMPOSE_PROJECT_NAME")
        .env_remove("COMPOSE_FILE")
        .output()
        .expect("run observability against an unavailable API");
    assert_eq!(
        output.status.code(),
        Some(3),
        "query-time CURIE_API_URL=http://127.0.0.1:1 must stay transient exit 3, not isolation usage 2; {}",
        output_text(&output)
    );
}
