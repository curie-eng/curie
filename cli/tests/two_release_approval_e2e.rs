//! Issue #2307: two Helm releases on one kind cluster, one Slack app, owner-only
//! approval without retry-until-acked. Text-contract tests against the clap
//! verb, the driver script, and the path-triggered workflow. Missing artifacts
//! fail closed; a script that only passes by looping until some release acks
//! is not the fixture.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn workflow_text() -> String {
    fs::read_to_string(repo_root().join(".github/workflows/two-release-approval-e2e.yaml"))
        .unwrap_or_default()
}

fn script_text() -> String {
    fs::read_to_string(repo_root().join("cli/scripts/two-release-approval-e2e.sh"))
        .unwrap_or_default()
}

fn main_rs() -> String {
    fs::read_to_string(repo_root().join("cli/src/main.rs")).unwrap_or_default()
}

fn count_lines_containing(text: &str, needle: &str) -> usize {
    text.lines().filter(|line| line.contains(needle)).count()
}

fn logical_lines(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    for line in text.lines() {
        let trimmed_end = line.trim_end();
        if let Some(rest) = trimmed_end.strip_suffix('\\') {
            buf.push_str(rest);
            buf.push(' ');
        } else {
            buf.push_str(trimmed_end);
            out.push(std::mem::take(&mut buf));
        }
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}

fn uncommented_logical_lines(text: &str) -> Vec<String> {
    logical_lines(text)
        .into_iter()
        .filter(|line| {
            let trimmed = line.trim_start();
            !trimmed.is_empty() && !trimmed.starts_with('#')
        })
        .collect()
}

fn helm_release_commands(text: &str) -> Vec<String> {
    uncommented_logical_lines(text)
        .into_iter()
        .filter(|line| {
            line.contains("helm ") && (line.contains("install") || line.contains("upgrade"))
        })
        .collect()
}

fn assigned_kind_cluster_names(text: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in uncommented_logical_lines(text) {
        for prefix in ["KIND_CLUSTER=", "KIND_CLUSTER_NAME=", "CLUSTER_NAME="] {
            if let Some(rest) = line.split(prefix).nth(1) {
                let name = rest
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_matches('"')
                    .trim_matches('\'')
                    .to_string();
                if !name.is_empty() && !name.starts_with('$') {
                    names.push(name);
                }
            }
        }
        if line.contains("kind create cluster") || line.contains("kind delete cluster") {
            for flag in ["--name ", "--name="] {
                if let Some(idx) = line.find(flag) {
                    let rest = &line[idx + flag.len()..];
                    let name = rest
                        .split_whitespace()
                        .next()
                        .unwrap_or("")
                        .trim_matches('"')
                        .trim_matches('\'')
                        .to_string();
                    if !name.is_empty() && !name.starts_with('$') {
                        names.push(name);
                    }
                }
            }
        }
    }
    names
}

fn pull_request_paths(text: &str) -> Vec<String> {
    let mut in_pr = false;
    let mut in_paths = false;
    let mut paths = Vec::new();
    let mut paths_indent: Option<usize> = None;
    for line in text.lines() {
        let trimmed = line.trim_start();
        let indent = line.len() - trimmed.len();
        if !in_pr {
            if trimmed.starts_with("pull_request:") {
                in_pr = true;
            }
            continue;
        }
        if !in_paths {
            if trimmed.starts_with("paths:") {
                in_paths = true;
            } else if indent == 0 && !trimmed.is_empty() && !trimmed.starts_with('#') {
                break;
            }
            continue;
        }
        if let Some(item) = trimmed.strip_prefix("- ") {
            if paths_indent.is_none() {
                paths_indent = Some(indent);
            }
            if Some(indent) == paths_indent {
                let path = item.trim().trim_matches('"').trim_matches('\'').to_string();
                if !path.is_empty() {
                    paths.push(path);
                }
                continue;
            }
        }
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if paths_indent.map(|i| indent <= i).unwrap_or(false) || indent == 0 {
            break;
        }
    }
    paths
}

const REQUIRED_PATHS: &[&str] = &[
    "apps/dispatcher/src/curie_dispatcher/approval_actions.py",
    "apps/dispatcher/src/curie_dispatcher/handlers.py",
    "apps/dispatcher/tests/test_approval_actions.py",
    "cli/scripts/two-release-approval-e2e.sh",
    "cli/tests/two_release_approval_e2e.rs",
    ".github/workflows/two-release-approval-e2e.yaml",
    "charts/curie/values-e2e-two-release-consumer.yaml",
];

const FORBIDDEN_KIND_NAMES: &[&str] = &[
    "dark-factory",
    "curie-2589-resume",
    "curie-pilot3-verify",
    "curie-sre-demo",
    "curie-e2e",
];

#[test]
fn main_rs_declares_two_release_approval_e2e_dev_action() {
    let text = main_rs();
    assert!(
        text.contains("TwoReleaseApprovalE2e"),
        "cli/src/main.rs DevAction must include TwoReleaseApprovalE2e for #2307"
    );
    assert!(
        text.contains("cli/scripts/two-release-approval-e2e.sh"),
        "DevAction dispatch must wrap cli/scripts/two-release-approval-e2e.sh"
    );
    assert!(
        text.contains("[\"curie\", \"dev\", \"two-release-approval-e2e\"]"),
        "parse test beside sre-demo-e2e must cover two-release-approval-e2e"
    );
}

#[test]
fn script_exists_and_is_executable() {
    let path = repo_root().join("cli/scripts/two-release-approval-e2e.sh");
    let mode = fs::metadata(&path)
        .expect("cli/scripts/two-release-approval-e2e.sh must exist")
        .permissions()
        .mode();
    assert!(
        mode & 0o111 != 0,
        "cli/scripts/two-release-approval-e2e.sh must be executable; mode={mode:#o}"
    );
}

#[test]
fn second_release_skips_crds_and_uses_consumer_overlay() {
    let text = script_text();
    assert!(
        !text.is_empty(),
        "cli/scripts/two-release-approval-e2e.sh must exist"
    );
    let helm = helm_release_commands(&text);
    assert!(
        helm.len() >= 2,
        "the fixture needs two Helm releases; found {}: {helm:?}",
        helm.len()
    );
    assert!(
        !helm[0].contains("--skip-crds"),
        "the first release installs CRDs; --skip-crds belongs on the consumer: {}",
        helm[0]
    );
    assert!(
        helm[1..].iter().any(|line| line.contains("--skip-crds")),
        "the second release must use --skip-crds; helm commands: {helm:?}"
    );
    assert!(
        text.contains("values-e2e-two-release-consumer.yaml"),
        "the consumer overlay values-e2e-two-release-consumer.yaml must be used; \
         file contents:\n{text}"
    );
}

#[test]
fn script_kind_cluster_name_is_unique_to_this_job() {
    let text = script_text();
    assert!(
        !text.is_empty(),
        "cli/scripts/two-release-approval-e2e.sh must exist"
    );
    let names = assigned_kind_cluster_names(&text);
    assert!(
        !names.is_empty(),
        "script must name a unique kind cluster owned by this job"
    );
    for name in &names {
        assert!(
            !FORBIDDEN_KIND_NAMES.contains(&name.as_str()),
            "kind cluster name {name} collides with another job; use a name owned \
             by #2307, never {FORBIDDEN_KIND_NAMES:?}"
        );
    }
}

#[test]
fn script_does_not_pass_by_retrying_until_acked() {
    let text = script_text();
    assert!(
        !text.is_empty(),
        "cli/scripts/two-release-approval-e2e.sh must exist"
    );
    let body = uncommented_logical_lines(&text).join("\n");
    assert!(
        !body.contains("deliver_until_acked"),
        "the Helm fixture must not pass by looping until a release acks"
    );
    let retry_until_ack = uncommented_logical_lines(&text).into_iter().filter(|line| {
        let lower = line.to_ascii_lowercase();
        let looping =
            lower.contains("until ") || lower.contains("while ") || lower.contains("for ");
        looping
            && (lower.contains("ack") || lower.contains("acked") || lower.contains("acknowledge"))
    });
    let matching: Vec<String> = retry_until_ack.collect();
    assert!(
        matching.is_empty(),
        "script must not loop deliveries until some release succeeds; one-shot B \
         miss then A hit is the pass path. matching lines: {matching:?}"
    );
}

#[test]
fn workflow_path_triggers_are_exactly_the_approval_action_fixture() {
    let text = workflow_text();
    assert!(
        !text.is_empty(),
        ".github/workflows/two-release-approval-e2e.yaml must exist"
    );
    let mut got = pull_request_paths(&text);
    let mut want: Vec<String> = REQUIRED_PATHS.iter().map(|s| (*s).to_string()).collect();
    got.sort();
    want.sort();
    assert_eq!(
        got, want,
        "pull_request paths must be exactly the #2307 approval-action fixture list"
    );
    assert!(
        !got.iter()
            .any(|path| path.contains("curie_dispatcher/config.py")),
        "unrelated dispatcher PRs must not pay this cost: config.py is not a path trigger"
    );
    assert!(
        !got.iter()
            .any(|path| path == "runner" || path.starts_with("runner/")),
        "do not expand SDK approval-gate path triggers (#2308): runner/ is not in paths"
    );
    assert!(
        !got.iter().any(|path| path.contains("uv.lock")),
        "uv.lock must not trigger the two-release workflow"
    );
}

#[test]
fn workflow_declares_least_privilege_contents_read_permissions() {
    let text = workflow_text();
    assert!(
        text.contains("permissions:"),
        "the two-release workflow must declare a permissions: block; file contents:\n{text}"
    );
    assert!(
        text.contains("contents: read"),
        "the two-release workflow must include contents: read; file contents:\n{text}"
    );
}

#[test]
fn workflow_pairs_every_checkout_with_persist_credentials_false() {
    let text = workflow_text();
    let checkout_count = count_lines_containing(&text, "uses: actions/checkout");
    assert!(
        checkout_count > 0,
        "the two-release workflow must use actions/checkout at least once; file contents:\n{text}"
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
    let text = workflow_text();
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
