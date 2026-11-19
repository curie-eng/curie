//! `curie cluster memory --rules | --accept | --reject | --remove` against a
//! stub API. An ambient platform key sits in the environment of every run and
//! must never reach an action request.

use crate::support;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};
use support::closed_port::ClosedPort;
use support::{serve, Request, Response};

const COOKIE: &str = "memory-rules-cookie-SENTINEL";
const AMBIENT_KEY: &str = "ambient-platform-key-SENTINEL";
const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const RULE_ID: &str = "22222222-2222-2222-2222-222222222222";

fn agent_list() -> Response {
    Response::json(
        200,
        &json!([{
            "id": AGENT_ID,
            "name": "acme-bot",
            "channels": [{"kind": "slack", "address": "C0EXAMPLE1"}],
            "memory": false,
            "created_at": "2026-07-05T00:00:00Z",
        }])
        .to_string(),
    )
}

fn rule(state: &str, version: i64) -> Value {
    json!({
        "id": RULE_ID, "number": 3, "label": "R3",
        "scope_kind": "repository", "scope_key": "github:github.com:4401",
        "scope_label": "acme-corp/acme-bot", "state": state,
        "text": "run the formatter before committing", "reason": "reviewer asked",
        "source_url": "https://example.com/pull/1", "proposed_by": "github:6601:octocat",
        "proposed_at": "2026-11-19T00:00:00Z", "version": version,
    })
}

fn rule_list() -> Value {
    json!({
        "rules": [rule("proposed", 1)],
        "scopes": [{"scope_kind": "repository", "scope_key": "github:github.com:4401",
                    "scope_label": "acme-corp/acme-bot", "in_force_count": 0}],
        "in_force_limit": 16,
    })
}

fn accept_path() -> String {
    format!("/agents/{AGENT_ID}/memory/rules/{RULE_ID}/accept")
}

struct Fixture {
    temp: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            temp: tempfile::tempdir().expect("create fixture"),
        }
    }

    fn session(&self, mode: u32) -> PathBuf {
        let path = self.temp.path().join("session");
        fs::write(&path, format!("{COOKIE}\n")).expect("write session file");
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).expect("set mode");
        path
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_curie"))
            .args(args)
            .current_dir(self.temp.path())
            .stdin(Stdio::null())
            .env("CURIE_CONFIG_DIR", self.temp.path().join("config"))
            .env("KUBECONFIG", "/nonexistent")
            .env("CURIE_API_KEY", AMBIENT_KEY)
            .env("NO_COLOR", "1")
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_NAMESPACE")
            .output()
            .expect("run memory rules command")
    }
}

fn act_args<'a>(verb: &'a str, session: &'a Path, base: &'a str) -> Vec<&'a str> {
    vec![
        "--json",
        "cluster",
        "memory",
        "acme-bot",
        verb,
        RULE_ID,
        "--expected-version",
        "1",
        "--session-file",
        session.to_str().expect("UTF-8 path"),
        "--api-url",
        base,
    ]
}

fn text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn object(output: &Output) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(stdout.trim_end().lines().count(), 1, "{}", text(output));
    let value: Value = serde_json::from_str(&stdout).expect("stdout is one JSON value");
    assert!(value.is_object(), "{value}");
    value
}

fn assert_cookie_only(request: &Request) {
    assert_eq!(
        request.header("cookie"),
        Some(format!("__Host-curie_console_session={COOKIE}").as_str())
    );
    assert_eq!(request.header("x-api-key"), None);
    assert_eq!(request.header("authorization"), None);
    assert!(!String::from_utf8_lossy(&request.body).contains(AMBIENT_KEY));
}

fn assert_no_secrets(output: &Output) {
    let rendered = text(output);
    assert!(!rendered.contains(COOKIE), "output disclosed the cookie");
    assert!(!rendered.contains(AMBIENT_KEY), "output disclosed the key");
}

#[test]
fn rules_json_prints_one_object_with_the_rule_list() {
    let fixture = Fixture::new();
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("GET", p) if p.starts_with(&format!("/agents/{AGENT_ID}/memory/rules")) => {
            Response::json(200, &rule_list().to_string())
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let output = fixture.run(&[
        "--json",
        "cluster",
        "memory",
        "acme-bot",
        "--rules",
        "--rule-state",
        "proposed",
        "--scope-key",
        "github:github.com:4401",
        "--api-url",
        &server.base_url,
        "--api-key",
        "list-key",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    let value = object(&output);
    assert_eq!(value, rule_list());
    assert_eq!(value["rules"][0]["label"], "R3");
    let requests = server.recorded();
    let list = requests
        .iter()
        .find(|r| r.path.contains("/memory/rules"))
        .expect("the list request reached the stub");
    assert_eq!(list.method, "GET");
    assert!(list.path.contains("state=proposed"), "{}", list.path);
    assert!(list.path.contains("scope_key=github"), "{}", list.path);
    assert_eq!(list.header("x-api-key"), Some("list-key"));
}

#[test]
fn rules_human_output_names_label_state_scope_and_text() {
    let fixture = Fixture::new();
    let server = serve(|req| match req.path.as_str() {
        "/agents" => agent_list(),
        _ => Response::json(200, &rule_list().to_string()),
    });
    let output = fixture.run(&[
        "cluster",
        "memory",
        "acme-bot",
        "--rules",
        "--api-url",
        &server.base_url,
        "--api-key",
        "list-key",
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    let shown = text(&output);
    for part in [
        "R3",
        RULE_ID,
        "proposed",
        "acme-corp/acme-bot",
        "run the formatter",
    ] {
        assert!(shown.contains(part), "missing {part}: {shown}");
    }
}

#[test]
fn accept_sends_the_cookie_and_no_key_and_prints_the_accepted_rule() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let response = json!({"rule": rule("accepted", 2), "applies_from": "next_ticket"});
    let expected = response.clone();
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if p == accept_path() => Response::json(200, &response.to_string()),
        other => panic!("unexpected request: {other:?}"),
    });
    let output = fixture.run(&act_args("--accept", &session, &server.base_url));
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    assert_eq!(object(&output), expected);
    let requests = server.recorded();
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert_cookie_only(request);
    }
    let post = &requests[1];
    assert_eq!(post.method, "POST");
    assert_eq!(post.path, accept_path());
    assert_eq!(
        serde_json::from_slice::<Value>(&post.body).expect("JSON body"),
        json!({"expected_version": 1})
    );
    assert_eq!(post.header("origin"), Some(server.base_url.as_str()));
    assert_no_secrets(&output);
}

#[test]
fn reject_and_remove_post_to_their_own_path() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    for (flag, segment) in [("--reject", "reject"), ("--remove", "remove")] {
        let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/agents") => agent_list(),
            ("POST", p) if p.ends_with(&format!("/{segment}")) => Response::json(
                200,
                &json!({"rule": rule("rejected", 2), "applies_from": "next_ticket"}).to_string(),
            ),
            other => panic!("unexpected request: {other:?}"),
        });
        let output = fixture.run(&act_args(flag, &session, &server.base_url));
        assert_eq!(output.status.code(), Some(0), "{}", text(&output));
        let requests = server.recorded();
        assert_eq!(
            requests[1].path,
            format!("/agents/{AGENT_ID}/memory/rules/{RULE_ID}/{segment}")
        );
    }
}

#[test]
fn a_session_file_open_to_group_or_others_is_refused_before_any_request() {
    let fixture = Fixture::new();
    let server = serve(|req| panic!("no request may be sent: {req:?}"));
    let session = fixture.session(0o644);
    let output = fixture.run(&act_args("--accept", &session, &server.base_url));
    assert_eq!(output.status.code(), Some(2), "{}", text(&output));
    assert!(server.recorded().is_empty());
    assert_no_secrets(&output);

    // Liveness: the same command with a 0600 file is accepted.
    let session = fixture.session(0o600);
    let live = serve(|req| match req.method.as_str() {
        "GET" => agent_list(),
        _ => Response::json(
            200,
            &json!({"rule": rule("accepted", 2), "applies_from": "next_ticket"}).to_string(),
        ),
    });
    let output = fixture.run(&act_args("--accept", &session, &live.base_url));
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
}

#[test]
fn an_api_refusal_exits_1_with_error_and_fix() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let server = serve(|req| match req.method.as_str() {
        "GET" => agent_list(),
        _ => Response::json(
            403,
            &json!({"detail": {
                "code": "memory_rules.write_access_required",
                "message": "You cannot write to this repository.",
                "fix": "Ask a maintainer of the repository to decide this rule.",
            }})
            .to_string(),
        ),
    });
    let output = fixture.run(&act_args("--accept", &session, &server.base_url));
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let value = object(&output);
    assert_eq!(value.as_object().expect("object").len(), 2);
    assert_eq!(
        value["error"],
        "memory_rules.write_access_required: You cannot write to this repository."
    );
    assert_eq!(
        value["fix"],
        "Ask a maintainer of the repository to decide this rule."
    );
    assert_no_secrets(&output);
}

#[test]
fn dry_run_prints_one_plan_object_and_makes_no_request() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let server = serve(|req| panic!("a dry run must not request: {req:?}"));
    let mut args = act_args("--accept", &session, &server.base_url);
    args.push("--dry-run");
    let output = fixture.run(&args);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    let value = object(&output);
    assert_eq!(value["dry_run"], true);
    let plan = value["plan"].as_array().expect("plan lines");
    let joined = plan
        .iter()
        .map(|line| line.as_str().expect("line").to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(joined.contains("POST "), "{joined}");
    assert!(joined.contains(&format!("/memory/rules/{RULE_ID}/accept")));
    assert!(joined.contains("\"expected_version\":1"));
    assert!(joined.contains("Console session cookie"));
    assert!(server.recorded().is_empty());
    assert_no_secrets(&output);
}

#[test]
fn conflicting_or_incomplete_flags_exit_2_before_any_request() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let session = session.to_str().expect("UTF-8 path");
    let conflict = "cannot be used with";
    let missing = "required arguments were not provided";
    let cases: Vec<(Vec<&str>, &str)> = vec![
        (vec!["--rules", "--accept", RULE_ID], conflict),
        (vec!["--rules", "--add", "text"], conflict),
        (vec!["--rules", "--delete", "fact"], conflict),
        (vec!["--rules", "--channel", "slack=C0EXAMPLE1"], conflict),
        (vec!["--rules", "--guidance"], conflict),
        (vec!["--accept", RULE_ID, "--reject", RULE_ID], conflict),
        (vec!["--accept", RULE_ID, "--add", "text"], conflict),
        (
            vec!["--accept", RULE_ID, "--session-file", session],
            missing,
        ),
        (
            vec!["--accept", RULE_ID, "--expected-version", "1"],
            missing,
        ),
        (vec!["--rule-state", "proposed"], missing),
        (
            vec!["--expected-version", "1", "--session-file", session],
            missing,
        ),
    ];
    for (case, evidence) in cases {
        let mut args = vec!["--json", "cluster", "memory", "acme-bot"];
        args.extend(&case);
        args.extend(["--api-url", "http://127.0.0.1:9"]);
        let output = fixture.run(&args);
        assert_eq!(output.status.code(), Some(2), "{case:?}: {}", text(&output));
        assert!(
            text(&output).contains(evidence),
            "{case:?}: {}",
            text(&output)
        );
    }
    let mut args = vec![
        "--json", "cluster", "memory", "acme-bot", "--accept", RULE_ID,
    ];
    args.extend(["--expected-version", "0", "--session-file", session]);
    let output = fixture.run(&args);
    assert_eq!(output.status.code(), Some(2), "{}", text(&output));
    assert!(text(&output).contains("0"), "{}", text(&output));
}

#[test]
fn an_unreachable_server_exits_3() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let closed = ClosedPort::new();
    let output = fixture.run(&act_args("--accept", &session, &closed.url()));
    assert_eq!(output.status.code(), Some(3), "{}", text(&output));
    assert!(output.status.code() != Some(0));
    assert_no_secrets(&output);
}

#[test]
fn rules_with_a_session_file_sends_the_cookie_and_never_a_key() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let server = serve(|req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("GET", p) if p.starts_with(&format!("/agents/{AGENT_ID}/memory/rules")) => {
            Response::json(200, &rule_list().to_string())
        }
        other => panic!("unexpected request: {other:?}"),
    });
    let output = fixture.run(&[
        "--json",
        "cluster",
        "memory",
        "acme-bot",
        "--rules",
        "--session-file",
        session.to_str().expect("UTF-8 path"),
        "--api-url",
        &server.base_url,
    ]);
    assert_eq!(output.status.code(), Some(0), "{}", text(&output));
    assert_eq!(object(&output)["rules"][0]["id"], RULE_ID);
    let requests = server.recorded();
    assert_eq!(requests.len(), 2, "{requests:?}");
    for request in &requests {
        assert_cookie_only(request);
    }
    assert_no_secrets(&output);
}

#[test]
fn rules_with_a_session_file_open_to_others_is_refused_before_any_request() {
    let fixture = Fixture::new();
    let session = fixture.session(0o644);
    let server = serve(|req| panic!("no request may be sent: {req:?}"));
    let output = fixture.run(&[
        "--json",
        "cluster",
        "memory",
        "acme-bot",
        "--rules",
        "--session-file",
        session.to_str().expect("UTF-8 path"),
        "--api-url",
        &server.base_url,
    ]);
    assert_eq!(output.status.code(), Some(2), "{}", text(&output));
    // The private-file check refused it, not a flag rule: the mode is named.
    assert!(
        text(&output).contains("accessible to group or others (mode 0644)"),
        "{}",
        text(&output)
    );
    assert!(server.recorded().is_empty());
    assert_no_secrets(&output);
}

#[test]
fn a_rule_label_or_malformed_id_is_a_usage_error_before_any_request() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let server = serve(|req| panic!("no request may be sent: {req:?}"));
    for bad in ["R3", "22222222222222222222222222222222", "not-a-uuid"] {
        let mut args = act_args("--accept", &session, &server.base_url);
        let at = args.iter().position(|a| *a == RULE_ID).expect("id arg");
        args[at] = bad;
        let output = fixture.run(&args);
        assert_eq!(output.status.code(), Some(2), "{bad}: {}", text(&output));
        assert!(
            text(&output).contains("--rules"),
            "{bad}: {}",
            text(&output)
        );
    }
    assert!(server.recorded().is_empty());
}
