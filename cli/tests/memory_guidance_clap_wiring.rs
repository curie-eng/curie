//! Integration: the clap wiring of `<tier> memory <agent> --guidance`,
//! `--guidance-from <file>` and `--reset-guidance`, at both tiers (issue #1461).
//!
//! Drives the built binary with `--dry-run --json` and asserts on the plan
//! line, which names the HTTP method and path each flag must reach:
//! `--guidance` reads (`GET /agents/{id}/memory/guidance`), `--guidance-from`
//! stores (`PUT`), `--reset-guidance` removes (`DELETE`). A flag wired to the
//! wrong call shows up as the wrong method here.
//!
//! No server and no network: `--dry-run` returns before the HTTP client is
//! built. The cluster tier is given explicit `--api-url`/`--api-key` so it does
//! not go looking for a release to tunnel to.

use std::path::PathBuf;
use std::process::{Command, Output};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

const CLUSTER_CONN: [&str; 4] = [
    "--api-url",
    "http://127.0.0.1:9",
    "--api-key",
    "curie-test-key",
];

fn run(argv: &[&str]) -> Output {
    Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")))
}

/// The single `plan` line of a `--dry-run --json` run that must succeed.
fn dry_run_plan_line(argv: &[&str]) -> String {
    let output = run(argv);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "curie {} must exit 0; stdout: {stdout}; stderr: {stderr}",
        argv.join(" ")
    );
    let value: serde_json::Value = serde_json::from_str(stdout.trim())
        .unwrap_or_else(|e| panic!("stdout must be one JSON object: {e}; stdout: {stdout}"));
    value
        .get("plan")
        .and_then(|p| p.as_array())
        .and_then(|p| p.first())
        .and_then(|l| l.as_str())
        .unwrap_or_else(|| panic!("dry-run output must carry a plan line: {value}"))
        .to_string()
}

fn refused(argv: &[&str]) {
    let output = run(argv);
    assert!(
        !output.status.success(),
        "curie {} must be refused; stdout: {}",
        argv.join(" "),
        String::from_utf8_lossy(&output.stdout)
    );
}

/// A guidance file on disk, removed on drop.
struct GuidanceFile(PathBuf);

impl GuidanceFile {
    fn new(text: &str) -> Self {
        let path = std::env::temp_dir().join(format!("curie-guidance-{}.md", uuid::Uuid::new_v4()));
        std::fs::write(&path, text).expect("write guidance file");
        Self(path)
    }

    fn path(&self) -> &str {
        self.0.to_str().expect("utf-8 temp path")
    }
}

impl Drop for GuidanceFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn assert_plan(plan: &str, method: &str) {
    assert!(
        plan.starts_with(&format!("{method} ")),
        "plan must start with {method}: {plan}"
    );
    assert!(
        plan.contains("/agents/<id>/memory/guidance"),
        "plan must target the guidance endpoint: {plan}"
    );
}

fn argv<'a>(tier: &'a str, rest: &[&'a str]) -> Vec<&'a str> {
    let mut v = vec![tier, "memory", "deal-desk"];
    if tier == "cluster" {
        v.extend(CLUSTER_CONN);
    }
    v.extend(rest);
    v.extend(["--dry-run", "--json"]);
    v
}

#[test]
fn guidance_reads_the_guidance_endpoint_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let plan = dry_run_plan_line(&argv(tier, &["--guidance"]));
        assert_plan(&plan, "GET");
    }
}

#[test]
fn guidance_from_puts_to_the_guidance_endpoint_at_both_tiers() {
    let file = GuidanceFile::new("Remember preferences; never record secrets.");
    for tier in ["local", "cluster"] {
        let plan = dry_run_plan_line(&argv(tier, &["--guidance-from", file.path()]));
        assert_plan(&plan, "PUT");
    }
}

#[test]
fn reset_guidance_deletes_the_guidance_endpoint_at_both_tiers() {
    for tier in ["local", "cluster"] {
        let plan = dry_run_plan_line(&argv(tier, &["--reset-guidance"]));
        assert_plan(&plan, "DELETE");
    }
}

#[test]
fn plain_memory_still_lists_the_memory_log() {
    // The new flags must not change what a bare `memory <agent>` does.
    for tier in ["local", "cluster"] {
        let plan = dry_run_plan_line(&argv(tier, &[]));
        assert!(plan.starts_with("GET "), "{plan}");
        assert!(plan.contains("/agents/<id>/memory "), "{plan}");
        assert!(!plan.contains("guidance"), "{plan}");
    }
}

#[test]
fn guidance_from_together_with_reset_guidance_is_refused_at_both_tiers() {
    let file = GuidanceFile::new("some guidance");
    // Anchor: each flag parses on its own, so the refusal below is about the
    // combination and not about a binary that lacks either flag.
    dry_run_plan_line(&argv("local", &["--guidance-from", file.path()]));
    dry_run_plan_line(&argv("local", &["--reset-guidance"]));
    for tier in ["local", "cluster"] {
        refused(&argv(
            tier,
            &["--guidance-from", file.path(), "--reset-guidance"],
        ));
    }
}

#[test]
fn guidance_from_requires_a_file_argument() {
    dry_run_plan_line(&argv("local", &["--reset-guidance"]));
    let mut v = vec!["local", "memory", "deal-desk", "--dry-run", "--json"];
    v.push("--guidance-from");
    refused(&v);
}
