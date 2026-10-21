//! Integration: `surfaces --label` sets a binding's trigger label (#4361 AC 6,
//! decision 18, as amended by #4397 decision 10).
//!
//! Two factory agents can share one repository when their surfaces carry
//! different trigger labels. `curie <tier> surfaces <agent> --add KIND=ADDRESS
//! --label <label>` sends `trigger_label` on the binding write; `--label` is
//! accepted on the tracker surface kinds (`github`, `gitlab`, `jira`) and refused
//! on any other kind before a request is made. The listing shows the label in
//! human output (`github:o/r [label l]`) and as `trigger_label` under `--json`.
//!
//! Every test drives the BUILT BINARY against an in-process mock of the
//! platform API, as `surfaces_verb.rs` does, so clap wiring is covered too.
//! Whether a gitlab or jira surface resolves is the server's decision (it
//! answers `surface_unconfigured` for a kind this build does not install); the
//! client's job is only to send the label.

use crate::support::{serve, MockServer, Response};
use std::process::Command;

const AGENT_ID: &str = "45454545-4545-4545-4545-454545454545";
const AGENT_NAME: &str = "acme-factory-fast";
const REPO: &str = "acme-corp/acme-bot";
const OTHER_REPO: &str = "acme-corp/widget-svc";
const FAST: &str = "curie-factory-fast";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn channels_path() -> String {
    format!("/agents/{AGENT_ID}/channels")
}

/// The agent's wire JSON: a labelled GitHub surface, an unlabelled one, and a
/// Slack surface, in the `ChannelBindingOut` shape the API returns.
fn agent_json() -> String {
    format!(
        r#"{{"id":"{AGENT_ID}","name":"{AGENT_NAME}","channels":[{{"kind":"github","address":"{REPO}","adapter":null,"allowed_callers":null,"trigger_label":"{FAST}"}},{{"kind":"github","address":"{OTHER_REPO}","adapter":null,"allowed_callers":null,"trigger_label":null}},{{"kind":"slack","address":"C0EXAMPLE1","adapter":"default","allowed_callers":null,"trigger_label":null}}],"created_at":"2026-10-11T00:00:00Z","memory":false,"agent_wide_learning":false}}"#
    )
}

/// Answers the agent reads and records every write; a write answers the agent.
fn api() -> MockServer {
    serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => Response::json(200, &format!("[{}]", agent_json())),
        ("GET", p) if p == format!("/agents/{AGENT_ID}") => Response::json(200, &agent_json()),
        ("POST", p) if p == channels_path() => Response::json(201, &agent_json()),
        // Answered rather than panicked, so a write the CLI should not have
        // issued is recorded and the assertion about it is what fails.
        _ => Response::json(200, &agent_json()),
    })
}

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

impl Run {
    fn output(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

fn run(argv: &[&str]) -> Run {
    let output = Command::new(bin())
        .args(argv)
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env("KUBECONFIG", "/nonexistent/curie-test-kubeconfig")
        .output()
        .unwrap_or_else(|e| panic!("run curie {}: {e}", argv.join(" ")));
    Run {
        code: output.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn surfaces(tier: &str, server: &MockServer, extra: &[&str]) -> Run {
    let mut argv = vec![tier, "surfaces", AGENT_NAME];
    argv.extend_from_slice(extra);
    argv.extend_from_slice(&["--api-url", &server.base_url, "--api-key", "k"]);
    run(&argv)
}

/// A refusal from the `--label` rule itself, not clap rejecting an unknown flag.
fn refused_by_the_label_rule(run: &Run) -> bool {
    !run.output().contains("unexpected argument")
}

/// The one binding POST the CLI made, as JSON.
fn posted(server: &MockServer) -> serde_json::Value {
    let posts: Vec<_> = server
        .recorded()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path == channels_path())
        .collect();
    assert_eq!(posts.len(), 1, "exactly one binding add");
    serde_json::from_slice(&posts[0].body).expect("the add body should be JSON")
}

#[test]
fn trigger_label_add_sends_the_field() {
    for tier in ["cluster", "local"] {
        let server = api();
        let add = format!("github={REPO}");

        let run = surfaces(tier, &server, &["--add", &add, "--label", FAST]);

        assert_eq!(
            run.code,
            0,
            "{tier} add --label must succeed: {}",
            run.output()
        );
        assert_eq!(
            posted(&server),
            serde_json::json!({"kind": "github", "address": REPO, "trigger_label": FAST}),
            "{tier}: the POST carries the pair and the label, nothing else"
        );
    }
}

#[test]
fn trigger_label_absent_sends_no_field() {
    // Liveness for the field: no `--label`, no key at all, so an unlabelled
    // surface keeps serving the install's tracker label.
    for tier in ["cluster", "local"] {
        let server = api();
        let add = format!("github={REPO}");

        let run = surfaces(tier, &server, &["--add", &add]);

        assert_eq!(run.code, 0, "{tier} add must succeed: {}", run.output());
        assert_eq!(
            posted(&server),
            serde_json::json!({"kind": "github", "address": REPO}),
            "{tier}: an unlabelled add sends no trigger_label key"
        );
    }
}

#[test]
fn trigger_label_on_gitlab_and_jira_surfaces_is_sent() {
    // #4397 decision 10: the label may sit on any tracker surface kind. The
    // client sends it; refusing a kind this build does not install is the
    // server's job.
    for (kind, address) in [("gitlab", "acme/platform/infra"), ("jira", "acme-jira")] {
        for tier in ["cluster", "local"] {
            let server = api();
            let add = format!("{kind}={address}");

            let run = surfaces(tier, &server, &["--add", &add, "--label", FAST]);

            assert_eq!(run.code, 0, "{tier} {kind} add --label: {}", run.output());
            assert_eq!(
                posted(&server),
                serde_json::json!({"kind": kind, "address": address, "trigger_label": FAST}),
                "{tier}: the {kind} POST carries the label"
            );
        }
    }
}

#[test]
fn trigger_label_with_slack_exits_before_any_request() {
    for tier in ["cluster", "local"] {
        let server = api();

        let run = surfaces(
            tier,
            &server,
            &["--add", "slack=C0EXAMPLE2", "--label", FAST],
        );

        assert_ne!(
            run.code,
            0,
            "{tier}: --label on slack must fail: {}",
            run.output()
        );
        assert!(
            refused_by_the_label_rule(&run),
            "{tier}: the flag must exist and refuse the kind: {}",
            run.output()
        );
        assert!(
            run.output().contains("--label"),
            "{tier}: the refusal names the flag: {}",
            run.output()
        );
        assert!(
            server.recorded().is_empty(),
            "{tier}: refused before any request, got {:?}",
            server
                .recorded()
                .iter()
                .map(|r| format!("{} {}", r.method, r.path))
                .collect::<Vec<_>>()
        );
    }
}

#[test]
fn trigger_label_with_another_non_tracker_kind_exits_before_any_request() {
    let server = api();

    let run = surfaces(
        "local",
        &server,
        &[
            "--add",
            "discord=111111111111111111", // gitleaks:allow -- zero-entropy placeholder snowflake
            "--endpoint",
            "https://discord-adapter.example.com/replies",
            "--adapter",
            "discord-main",
            "--label",
            FAST,
        ],
    );

    assert_ne!(
        run.code,
        0,
        "--label on discord must fail: {}",
        run.output()
    );
    assert!(
        refused_by_the_label_rule(&run),
        "the flag must exist and refuse the kind: {}",
        run.output()
    );
    assert!(server.recorded().is_empty(), "refused before any request");
}

#[test]
fn trigger_label_invalid_value_exits_before_any_request() {
    // The CLI validator mirrors the server's rule: 1 to 50 characters, no
    // whitespace, and none of the state, base or repository label prefixes.
    let long = "x".repeat(51);
    for label in [
        "curie factory",
        long.as_str(),
        "curie-factory:x",
        "base:next",
        "repo:acme",
    ] {
        let server = api();
        let add = format!("github={REPO}");

        let run = surfaces("local", &server, &["--add", &add, "--label", label]);

        assert_ne!(run.code, 0, "{label:?} must be refused: {}", run.output());
        assert!(
            refused_by_the_label_rule(&run),
            "{label:?}: the flag must exist and refuse the value: {}",
            run.output()
        );
        assert!(
            server.recorded().is_empty(),
            "{label:?}: refused before any request"
        );
    }
}

#[test]
fn trigger_label_without_add_is_a_usage_error() {
    let server = api();

    let run = surfaces("cluster", &server, &["--label", FAST]);

    assert_eq!(run.code, 2, "--label needs --add: {}", run.output());
    assert!(
        refused_by_the_label_rule(&run),
        "the flag must exist; exit 2 comes from its requirement: {}",
        run.output()
    );
    assert!(server.recorded().is_empty());
}

#[test]
fn trigger_label_list_shows_label_in_human_and_json() {
    for tier in ["cluster", "local"] {
        let server = api();

        let human = surfaces(tier, &server, &[]);

        assert_eq!(human.code, 0, "{tier} list: {}", human.output());
        assert!(
            human
                .stdout
                .contains(&format!("github:{REPO} [label {FAST}]")),
            "{tier}: a labelled surface prints its label: {}",
            human.output()
        );
        assert!(
            human.stdout.contains(&format!("github:{OTHER_REPO}")),
            "{tier}: {}",
            human.output()
        );
        assert!(
            !human
                .stdout
                .contains(&format!("github:{OTHER_REPO} [label")),
            "{tier}: an unlabelled surface prints no label suffix: {}",
            human.output()
        );

        let json = surfaces(tier, &server, &["--json"]);

        assert_eq!(json.code, 0, "{tier} list --json: {}", json.output());
        let value: serde_json::Value = serde_json::from_str(json.stdout.trim())
            .unwrap_or_else(|e| panic!("--json must emit one object: {e}; {}", json.stdout));
        let listed = value["surfaces"].as_array().expect("a surfaces array");
        let labelled = listed
            .iter()
            .find(|s| s["address"] == REPO)
            .expect("the labelled surface");
        assert_eq!(labelled["trigger_label"], FAST, "{tier}: {value}");
        let plain = listed
            .iter()
            .find(|s| s["address"] == OTHER_REPO)
            .expect("the unlabelled surface");
        assert_eq!(
            plain.get("trigger_label"),
            Some(&serde_json::Value::Null),
            "{tier}: an unlabelled surface carries trigger_label null: {value}"
        );
        assert!(
            server.recorded().iter().all(|r| r.method == "GET"),
            "{tier}: a list writes nothing"
        );
    }
}
