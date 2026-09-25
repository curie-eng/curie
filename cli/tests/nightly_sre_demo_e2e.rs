//! Issue #2246 / #2854: the nightly SRE demo e2e workflow is the first
//! automated tier that exercises the five demo assertions (read, approved
//! scale, re-arm, configuration denial, RBAC ceiling) on kind with the pinned
//! Kubernetes MCP server and a live provider. Turns start with
//! `curie cluster message`. Approvals resolve through `curie cluster approvals`
//! and an operator principal. No Slack app is required.
//!
//! This file is a text contract test against the workflow YAML plus an
//! executing test of the skip script. A missing live provider must skip with
//! the reason in the run summary, never report a green that proved the five
//! assertions. Secrets must never appear on a
//! `run:` line.

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
    vec![("CURIE_CREDENTIALS", "sk-or-test-not-a-real-key")]
}

fn line_bounds(text: &str, index: usize) -> (usize, usize) {
    let start = text[..index].rfind('\n').map_or(0, |at| at + 1);
    let end = text[index..].find('\n').map_or(text.len(), |at| index + at);
    (start, end)
}

fn shell_command_containing(text: &str, index: usize) -> String {
    let (mut start, mut end) = line_bounds(text, index);
    while start > 0 {
        let prev_end = start - 1;
        let prev_start = text[..prev_end].rfind('\n').map_or(0, |at| at + 1);
        if text[prev_start..prev_end].trim_end().ends_with('\\') {
            start = prev_start;
        } else {
            break;
        }
    }
    while text[start..end].trim_end().ends_with('\\') && end < text.len() {
        let next = end + 1;
        end = text[next..].find('\n').map_or(text.len(), |at| next + at);
    }
    text[start..end].to_string()
}

fn is_workflow_job_key(line: &str) -> bool {
    let Some(body) = line.strip_prefix("  ") else {
        return false;
    };
    if body.starts_with(' ') {
        return false;
    }
    let Some(name) = body.trim_end().strip_suffix(':') else {
        return false;
    };
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

fn workflow_job_containing(text: &str, needle: &str) -> String {
    let jobs_at = text
        .find("\njobs:\n")
        .expect("the SRE demo workflow must declare a jobs block");
    let jobs = &text[jobs_at + 1..];
    let mut blocks = Vec::new();
    let mut current = String::new();
    let mut in_job = false;
    for line in jobs.lines() {
        if is_workflow_job_key(line) {
            if in_job {
                blocks.push(current);
                current = String::new();
            }
            in_job = true;
        }
        if in_job {
            current.push_str(line);
            current.push('\n');
        }
    }
    if in_job && !current.is_empty() {
        blocks.push(current);
    }
    blocks
        .into_iter()
        .find(|block| block.contains(needle))
        .unwrap_or_else(|| {
            panic!("no workflow job name contains {needle}; jobs section:\n{jobs}")
        })
}

fn connector_block<'a>(text: &'a str, name: &str) -> &'a str {
    let marker = format!("\n  {name}:");
    let Some(start) = text.find(&marker) else {
        panic!(
            "examples/sre-bot/connectors.yaml must declare connector {name}; file contents:\n{text}"
        );
    };
    let tail = &text[start..];
    let mut offset = marker.len();
    let mut end = tail.len();
    while let Some(rel) = tail[offset..].find("\n  ") {
        let at = offset + rel + 3;
        if tail[at..].starts_with(|c: char| !c.is_whitespace()) {
            end = offset + rel;
            break;
        }
        offset = at;
    }
    &tail[..end]
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
fn workflow_checks_out_the_triggering_ref_so_script_and_workflow_stay_paired() {
    let text = workflow();
    assert!(
        !text.contains("schedule' && 'next")
            && !text.contains("schedule && 'next")
            && !text.contains("ref: next"),
        "checking out next on schedule would pair this Slack-less workflow \
         with next's older Slack script; file contents:\n{text}"
    );
    assert!(
        text.contains("ref: ${{ github.ref }}"),
        "the SRE demo workflow must check out github.ref so the script matches \
         the workflow that invoked it; file contents:\n{text}"
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
        text.contains("dispatcher.deploy=false"),
        "the SRE demo workflow drives turns from cluster message with an \
         operator principal, so it must disable dispatcher.deploy; \
         file contents:\n{text}"
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
fn workflow_assigns_required_flag_on_non_pr_events() {
    let text = workflow();
    assert!(
        text.contains(
            "CURIE_SRE_DEMO_REQUIRED: ${{ github.event_name != 'pull_request' && '1' || '0' }}"
        ),
        "required live acceptance must set CURIE_SRE_DEMO_REQUIRED=1 on \
         schedule, dispatch, and RC events, and leave PR inventory skippable; \
         file contents:\n{text}"
    );
}

#[test]
fn workflow_paths_include_outcome_probe_and_python_tests() {
    let text = workflow();
    for needle in [
        "cli/scripts/sre-demo-mcp-probe.py",
        "cli/tests/sre_demo_e2e_test.py",
    ] {
        assert!(
            text.contains(needle),
            "the SRE demo workflow path filter must include {needle}; \
             file contents:\n{text}"
        );
    }
}

#[test]
fn script_names_exactly_five_demo_assertions_in_order() {
    let text = script();
    let assertions: Vec<_> = text
        .lines()
        .filter_map(|line| line.trim().strip_prefix("run_assertion "))
        .collect();
    assert_eq!(
        assertions,
        [
            "read assert_read",
            "scale assert_scale",
            "rearm assert_rearm",
            "configuration-denial assert_configuration_denial",
            "rbac-ceiling assert_rbac_ceiling",
        ],
        "sre-demo-e2e.sh must invoke exactly the five retained assertions in order"
    );
}

#[test]
fn missing_credentials_skip_with_reason_in_the_summary() {
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
        summary.contains("CURIE_CREDENTIALS") || summary.contains("live provider"),
        "the skip reason must name the missing live-provider prerequisite; \
         summary:\n{summary}"
    );
    assert!(
        !summary.contains("CI_SLACK_APP_TOKEN")
            && !summary.contains("CI_SLACK_BOT_TOKEN")
            && !summary.contains("CI_SLACK_USER_TOKEN")
            && !summary.contains("CI_SLACK_CHANNEL_ID"),
        "the skip reason must not require a Slack secret; summary:\n{summary}"
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
    let mut prereqs = populated_prereqs();
    prereqs.push(("CURIE_SRE_DEMO_REQUIRED", "1"));
    let output = run_script("prereqs", &prereqs, work.path());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let github_output = fs::read_to_string(work.path().join("output.txt")).unwrap_or_default();
    assert!(
        output.status.success(),
        "provider credentials alone must make required prereqs ready\nstdout:\n{stdout}\nstderr:\n{stderr}"
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
         run cannot touch a cluster\nstdout:\n{stdout}\nstderr:\n{stderr}"
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
fn cli_sre_demo_e2e_help_does_not_require_a_slack_app() {
    let text = fs::read_to_string(repo_root().join("cli/src/main.rs")).unwrap_or_default();
    let start = text
        .find("Nightly SRE demo e2e")
        .expect("SreDemoE2e help must exist");
    let rest = &text[start..];
    let end = rest
        .find("SreDemoE2e")
        .map(|idx| start + idx)
        .unwrap_or(text.len());
    let window = &text[start..end];
    assert!(
        !window.contains("Slack"),
        "curie dev sre-demo-e2e help must not require a Slack app; window:\n{window}"
    );
    assert!(
        window.contains("operator principal"),
        "curie dev sre-demo-e2e help must name the operator principal; window:\n{window}"
    );
}

#[test]
fn workflow_and_script_name_no_ci_slack_secrets() {
    let workflow = workflow();
    let script = script();
    for needle in [
        "CI_SLACK_APP_TOKEN",
        "CI_SLACK_BOT_TOKEN",
        "CI_SLACK_USER_TOKEN",
        "CI_SLACK_CHANNEL_ID",
    ] {
        assert!(
            !workflow.contains(needle),
            "the SRE demo workflow must not reference {needle}; file contents:\n{workflow}"
        );
        assert!(
            !script.contains(needle),
            "sre-demo-e2e.sh must not reference {needle}; file contents:\n{script}"
        );
    }
}

#[test]
fn script_starts_turns_with_cluster_message_and_operator_approvals() {
    let text = script();
    assert!(
        text.contains("message --timeout-secs"),
        "sre-demo-e2e.sh must start turns with curie cluster message; file contents:\n{text}"
    );
    assert!(
        text.contains("--mint-operator-principal"),
        "sre-demo-e2e.sh must mint an operator principal; file contents:\n{text}"
    );
    assert!(
        text.contains("--route-approvers") && text.contains("users:"),
        "sre-demo-e2e.sh must bind approvers.users on the approval route; \
         file contents:\n{text}"
    );
    assert!(
        text.contains("principal_kind") && text.contains("operator"),
        "sre-demo-e2e.sh must keep the operator-principal audit check; \
         file contents:\n{text}"
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

#[test]
fn phase_run_provisions_observability_before_the_first_cluster_deploy() {
    let text = script();
    let Some(deploy_at) = text.find("cluster deploy") else {
        panic!("sre-demo-e2e.sh must still cluster deploy; file contents:\n{text}");
    };
    let Some(provision_at) = text.find("example sre-bot provision-observability") else {
        panic!(
            "sre-demo-e2e.sh must invoke example sre-bot provision-observability \
             before the first cluster deploy; file contents:\n{text}"
        );
    };
    assert!(
        provision_at < deploy_at,
        "example sre-bot provision-observability must occur before the first \
         cluster deploy; file contents:\n{text}"
    );
    let (line_start, line_end) = line_bounds(&text, provision_at);
    assert!(
        !text[line_start..line_end].trim_start().starts_with('#'),
        "provision-observability must be a command, not a comment; file contents:\n{text}"
    );
    let invocation = shell_command_containing(&text, provision_at);
    for flag in ["--namespace", "--release", "--chart"] {
        assert!(
            invocation.contains(flag),
            "the provision-observability invocation must include {flag}; \
             invocation:\n{invocation}"
        );
    }
}

#[test]
fn cluster_deploy_still_passes_the_sre_bot_plugin_dir() {
    let text = script();
    let Some(deploy_at) = text.find("cluster deploy") else {
        panic!("sre-demo-e2e.sh must cluster deploy; file contents:\n{text}");
    };
    let invocation = shell_command_containing(&text, deploy_at);
    assert!(
        invocation.contains("--plugin-dir"),
        "cluster deploy must still pass --plugin-dir; invocation:\n{invocation}"
    );
    assert!(
        invocation.contains("examples/sre-bot"),
        "cluster deploy must still pass examples/sre-bot; invocation:\n{invocation}"
    );
}

#[test]
fn connectors_keep_grafana_and_tempo_on_the_shared_secret() {
    let text = connectors();
    for name in ["grafana", "tempo"] {
        let block = connector_block(&text, name);
        assert!(
            block.contains("from_secret: curie-grafana-connector"),
            "connector {name} must declare from_secret: curie-grafana-connector; \
             block:\n{block}"
        );
    }
}

#[test]
fn acceptance_does_not_disable_the_grafana_connector() {
    let script = script();
    let workflow = workflow();
    assert!(
        !script.contains("grafanaConnector.enabled=false"),
        "sre-demo-e2e.sh must not set grafanaConnector.enabled=false; file contents:\n{script}"
    );
    assert!(
        !workflow.contains("grafanaConnector.enabled=false"),
        "the SRE demo workflow must not set grafanaConnector.enabled=false; \
         file contents:\n{workflow}"
    );
}

#[test]
fn five_assertion_job_timeout_is_120_minutes() {
    let text = workflow();
    let job = workflow_job_containing(&text, "Five SRE demo assertions on kind");
    assert!(
        job.lines()
            .any(|line| line.trim() == "timeout-minutes: 120"),
        "the job whose name contains Five SRE demo assertions on kind must set \
         timeout-minutes: 120; job:\n{job}"
    );
}
