//! Issue #2246: the nightly SRE demo e2e workflow is the first automated tier
//! that exercises the six demo assertions (read, approved scale, re-arm,
//! configuration denial, RBAC ceiling, coding PR) on kind with the pinned
//! Kubernetes MCP server, a CI-only Socket Mode Slack app, a live provider,
//! and an allowlisted throwaway repo.
//!
//! This file is a text-contract test against the workflow YAML plus an
//! executing test of the skip script. A missing Slack app or throwaway repo
//! must skip with the reason in the run summary, never report a green that
//! proved the six assertions. Secrets must never appear on a `run:` line.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn workflow_text(name: &str) -> String {
    let path = repo_root().join(".github/workflows").join(name);
    fs::read_to_string(path).unwrap_or_default()
}

fn workflow() -> String {
    workflow_text("nightly-sre-demo-e2e.yaml")
}

fn script() -> String {
    fs::read_to_string(repo_root().join("cli/scripts/sre-demo-e2e.sh")).unwrap_or_default()
}

fn connectors() -> String {
    fs::read_to_string(repo_root().join("examples/sre-bot/connectors.yaml")).unwrap_or_default()
}

fn pinned_mcp_digest() -> String {
    let text = connectors();
    let needle = "ghcr.io/containers/kubernetes-mcp-server@sha256:";
    let start = text
        .find(needle)
        .expect("examples/sre-bot/connectors.yaml must pin kubernetes-mcp-server by digest");
    let rest = &text[start + needle.len()..];
    let hex: String = rest.chars().take_while(|c| c.is_ascii_hexdigit()).collect();
    assert_eq!(
        hex.len(),
        64,
        "pinned kubernetes-mcp-server digest must be 64 hex chars; got {hex:?}"
    );
    format!("sha256:{hex}")
}

fn count_lines_containing(text: &str, needle: &str) -> usize {
    text.lines().filter(|line| line.contains(needle)).count()
}

fn run_script(phase: &str, extra_env: &[(&str, &str)], work: &Path) -> std::process::Output {
    let script_path = repo_root().join("cli/scripts/sre-demo-e2e.sh");
    let summary = work.join("summary.md");
    let output = work.join("output.txt");
    fs::write(&summary, "").expect("create step summary");
    fs::write(&output, "").expect("create github output");
    let mut command = Command::new("bash");
    command
        .arg(&script_path)
        .env_clear()
        .env(
            "PATH",
            std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into()),
        )
        .env("HOME", work.join("home"))
        .env("CURIE_SRE_DEMO_PHASE", phase)
        .env("GITHUB_STEP_SUMMARY", &summary)
        .env("GITHUB_OUTPUT", &output);
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command.output().expect("run sre-demo-e2e.sh")
}

fn populated_prereqs() -> Vec<(&'static str, &'static str)> {
    vec![
        ("CURIE_CREDENTIALS", "sk-or-test-not-a-real-key"),
        ("CI_SLACK_APP_TOKEN", "xapp-test"),
        ("CI_SLACK_BOT_TOKEN", "xoxb-test"),
        ("CI_SLACK_USER_TOKEN", "xoxp-test"),
        ("CI_SLACK_CHANNEL_ID", "C0EXAMPLE1"),
        ("CI_THROWAY_REPO", "acme-corp/sre-demo-throwaway"),
    ]
}

#[test]
fn workflow_declares_dispatch_schedule_and_release_candidate_triggers() {
    let text = workflow();
    assert!(
        text.contains("workflow_dispatch:"),
        "the SRE demo workflow must run on workflow_dispatch; file contents:\n{text}"
    );
    assert!(
        text.contains("schedule:"),
        "the SRE demo workflow must run on a nightly schedule; file contents:\n{text}"
    );
    assert!(
        text.contains("v*-rc"),
        "the SRE demo workflow must also run on release-candidate tags; file contents:\n{text}"
    );
}

#[test]
fn workflow_is_the_next_train_nightly_and_checks_out_next_on_schedule() {
    let text = workflow();
    assert!(
        text.contains("ref: next")
            || text.contains("ref: 'next'")
            || text.contains("ref: \"next\""),
        "a scheduled run fires from the default branch, so the workflow must \
         check out next to grade the feature train; file contents:\n{text}"
    );
}

#[test]
fn workflow_declares_least_privilege_contents_read_permissions() {
    let text = workflow();
    assert!(
        text.contains("permissions:"),
        "the SRE demo workflow must declare a permissions: block; file contents:\n{text}"
    );
    assert!(
        text.contains("contents: read"),
        "the SRE demo workflow must include contents: read; file contents:\n{text}"
    );
}

#[test]
fn workflow_pairs_every_checkout_with_persist_credentials_false() {
    let text = workflow();
    let checkout_count = count_lines_containing(&text, "uses: actions/checkout");
    assert!(
        checkout_count > 0,
        "the SRE demo workflow must use actions/checkout at least once; file contents:\n{text}"
    );
    let persist_false_count = count_lines_containing(&text, "persist-credentials: false");
    assert!(
        persist_false_count >= checkout_count,
        "every actions/checkout use ({checkout_count}) must be paired with \
         persist-credentials: false ({persist_false_count} found); file contents:\n{text}"
    );
}

#[test]
fn workflow_never_echoes_secrets_on_a_run_line() {
    let text = workflow();
    assert!(
        text.contains("secrets."),
        "the SRE demo workflow must reference repository secrets; file contents:\n{text}"
    );
    for line in text.lines() {
        if line.contains("secrets.") {
            assert!(
                !line.contains("run:"),
                "a secret must never appear on a run: line (it would be echoed \
                 into job logs): {line}"
            );
        }
    }
}

#[test]
fn live_provider_secret_reaches_the_job_only_as_curie_credentials() {
    let text = workflow();
    assert!(
        text.contains("secrets.OPENROUTER_API_KEY"),
        "the SRE demo workflow must reference secrets.OPENROUTER_API_KEY; file contents:\n{text}"
    );
    for line in text.lines() {
        if line.contains("secrets.OPENROUTER_API_KEY") {
            assert!(
                line.contains("CURIE_CREDENTIALS:"),
                "every OPENROUTER_API_KEY reference must assign CURIE_CREDENTIALS \
                 on that same line: {line}"
            );
        }
    }
}

#[test]
fn workflow_installs_on_kind_with_live_openrouter_and_never_seals() {
    let text = workflow();
    assert!(
        text.contains("helm/kind-action"),
        "the SRE demo workflow must create a kind cluster; file contents:\n{text}"
    );
    assert!(
        text.contains("--allow-egress-host openrouter"),
        "the SRE demo cluster install must open OpenRouter egress; file contents:\n{text}"
    );
    assert!(
        !text.contains("--fake-model"),
        "the SRE demo workflow must never seal the install with --fake-model; \
         file contents:\n{text}"
    );
    assert!(
        !text.contains("dispatcher.deploy=false"),
        "the SRE demo workflow needs a real Socket Mode dispatcher, so it must \
         not disable dispatcher.deploy; file contents:\n{text}"
    );
}

#[test]
fn workflow_pins_the_same_kubernetes_mcp_digest_as_sre_bot() {
    let digest = pinned_mcp_digest();
    let text = workflow();
    let script = script();
    assert!(
        text.contains(&digest) || script.contains(&digest),
        "the SRE demo workflow or its script must name the pinned \
         kubernetes-mcp-server digest {digest} from examples/sre-bot/connectors.yaml"
    );
}

#[test]
fn workflow_wires_the_throwaway_repo_allowlist_from_a_secret() {
    let text = workflow();
    assert!(
        text.contains("api.githubRepoAllowlist"),
        "the SRE demo install must set api.githubRepoAllowlist; file contents:\n{text}"
    );
    assert!(
        text.contains("secrets.CI_THROWAY_REPO") || text.contains("CI_THROWAY_REPO"),
        "the allowlist value must come from the CI throwaway-repo secret, not a \
         committed slug; file contents:\n{text}"
    );
}

#[test]
fn workflow_skips_the_live_job_unless_prereqs_are_ready() {
    let text = workflow();
    assert!(
        text.contains("needs.prereqs.outputs.ready")
            || text.contains("needs.prereqs.outputs.ready == 'true'"),
        "the expensive live job must be gated on the prereqs job ready output; \
         file contents:\n{text}"
    );
    assert!(
        text.contains("GITHUB_STEP_SUMMARY") || script().contains("GITHUB_STEP_SUMMARY"),
        "a skip must write the reason into the GitHub run summary"
    );
}

#[test]
fn script_names_all_six_demo_assertions() {
    let text = script();
    for needle in [
        "namespaces_list",
        "resources_scale",
        "re-arm",
        "configuration_view",
        "RBAC",
        "throwaway",
    ] {
        assert!(
            text.contains(needle),
            "sre-demo-e2e.sh must name assertion surface {needle}; file contents:\n{text}"
        );
    }
}

#[test]
fn missing_slack_secret_skips_with_reason_in_the_summary() {
    let work = tempfile::tempdir().expect("tempdir");
    let output = run_script("prereqs", &[], work.path());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let summary = fs::read_to_string(work.path().join("summary.md")).unwrap_or_default();
    let github_output = fs::read_to_string(work.path().join("output.txt")).unwrap_or_default();
    assert!(
        output.status.success(),
        "a documented skip must exit 0, not fail the workflow\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        summary.contains("SKIPPED") || summary.contains("skipped"),
        "the run summary must say the demo was skipped; summary:\n{summary}"
    );
    assert!(
        summary.contains("CI_SLACK_APP_TOKEN")
            || summary.contains("Slack")
            || summary.contains("slack"),
        "the skip reason must name the missing Slack prerequisite; summary:\n{summary}"
    );
    assert!(
        github_output.contains("ready=false"),
        "GITHUB_OUTPUT must set ready=false on a skip; output:\n{github_output}"
    );
    assert!(
        !stdout.contains("kind create") && !stderr.contains("kind create"),
        "a skip must not create a cluster; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn missing_prerequisites_fail_required_live_acceptance() {
    let work = tempfile::tempdir().expect("tempdir");
    let output = run_script("prereqs", &[("CURIE_SRE_DEMO_REQUIRED", "1")], work.path());
    assert!(
        !output.status.success(),
        "required live acceptance cannot skip green"
    );
    let github_output = fs::read_to_string(work.path().join("output.txt")).unwrap_or_default();
    assert!(github_output.contains("ready=false"));
}

#[test]
fn outcome_checks_reject_false_positives_by_execution() {
    let output = Command::new("uv")
        .args(["run", "--locked", "--package", "curie-runner", "python"])
        .current_dir(repo_root())
        .arg(repo_root().join("cli/tests/sre_demo_e2e_test.py"))
        .output()
        .expect("execute outcome regression tests");
    assert!(
        output.status.success(),
        "outcome regression failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn populated_prereqs_report_ready_without_touching_slack_or_kind() {
    let work = tempfile::tempdir().expect("tempdir");
    let output = run_script("prereqs", &populated_prereqs(), work.path());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let github_output = fs::read_to_string(work.path().join("output.txt")).unwrap_or_default();
    assert!(
        output.status.success(),
        "populated prereqs must exit 0\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        github_output.contains("ready=true"),
        "GITHUB_OUTPUT must set ready=true when every prerequisite is present; \
         output:\n{github_output}"
    );
    assert!(
        !stdout.contains("chat.postMessage") && !stderr.contains("chat.postMessage"),
        "the prereqs phase must not call Slack; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

#[test]
fn run_phase_without_allow_live_refuses_instead_of_touching_a_cluster() {
    let work = tempfile::tempdir().expect("tempdir");
    let output = run_script("run", &populated_prereqs(), work.path());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !output.status.success(),
        "PHASE=run without CURIE_SRE_DEMO_ALLOW_LIVE=1 must refuse, so a laptop \
         run cannot touch Slack or a cluster\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let combined = format!("{stdout}{stderr}");
    assert!(
        combined.contains("CURIE_SRE_DEMO_ALLOW_LIVE") || combined.contains("ALLOW_LIVE"),
        "the refusal must name the live-run guard; output:\n{combined}"
    );
}

#[test]
fn run_phase_with_missing_prereqs_fails_closed_instead_of_skipping() {
    let work = tempfile::tempdir().expect("tempdir");
    let output = run_script("run", &[("CURIE_SRE_DEMO_ALLOW_LIVE", "1")], work.path());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let github_output = fs::read_to_string(work.path().join("output.txt")).unwrap_or_default();
    assert!(
        !output.status.success(),
        "PHASE=run with missing secrets must fail, not skip: the prereqs job \
         is what skips\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !github_output.contains("ready=false"),
        "the run phase must not claim a documented skip; output:\n{github_output}"
    );
}

#[test]
fn script_is_executable() {
    let path = repo_root().join("cli/scripts/sre-demo-e2e.sh");
    let mode = fs::metadata(&path)
        .expect("sre-demo-e2e.sh must exist")
        .permissions()
        .mode();
    assert!(
        mode & 0o111 != 0,
        "cli/scripts/sre-demo-e2e.sh must be executable; mode={mode:#o}"
    );
}
