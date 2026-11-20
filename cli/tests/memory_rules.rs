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

/// A response recorded from the real API routes (`tests/vectors/memory-rules-responses.json`,
/// refreshed and drift-checked by `apps/api/tests/test_memory_rule_routes.py`).
fn vector(key: &str) -> Value {
    let recorded: Value = serde_json::from_str(include_str!(
        "../../tests/vectors/memory-rules-responses.json"
    ))
    .expect("the memory rules vector is JSON");
    recorded[key].clone()
}

/// A recorded rule given this test's id, label and text.
fn named(mut rule: Value, state: &str, version: i64) -> Value {
    rule["id"] = json!(RULE_ID);
    rule["number"] = json!(3);
    rule["label"] = json!("R3");
    rule["text"] = json!("run the formatter before committing");
    rule["state"] = json!(state);
    rule["version"] = json!(version);
    rule
}

/// The recorded accept answer's rule, in `state` at `version`.
fn rule(state: &str, version: i64) -> Value {
    named(vector("accept")["rule"].clone(), state, version)
}

/// The recorded rule list, narrowed to its proposed rule.
fn rule_list() -> Value {
    let mut list = vector("list");
    list["rules"] = json!([named(list["rules"][0].clone(), "proposed", 1)]);
    list
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
fn a_coded_503_outage_exits_3_with_error_and_fix() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let server = serve(|req| match req.method.as_str() {
        "GET" => agent_list(),
        _ => Response::json(
            503,
            &json!({"detail": {
                "code": "memory_rules.code_host_unavailable",
                "message": "The code host did not answer.",
                "fix": "Retry once the code host is reachable.",
            }})
            .to_string(),
        ),
    });
    let output = fixture.run(&act_args("--accept", &session, &server.base_url));
    assert_eq!(output.status.code(), Some(3), "{}", text(&output));
    let value = object(&output);
    assert!(value["error"]
        .as_str()
        .expect("error text")
        .contains("memory_rules.code_host_unavailable"));
    assert_eq!(value["fix"], "Retry once the code host is reachable.");
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

#[test]
fn the_recorded_api_responses_parse_and_print_unchanged() {
    let list = curie::api::parse_memory_rules(vector("list")).expect("the recorded list parses");
    assert_eq!(list.rules.len(), 4);
    assert_eq!(
        serde_json::to_value(&list).expect("serializes"),
        vector("list")
    );
    let change =
        curie::api::parse_memory_rule_change(vector("accept")).expect("the recorded accept parses");
    assert_eq!(change.rule.state, curie::api::MemoryRuleState::Accepted);
    assert_eq!(
        serde_json::to_value(&change).expect("serializes"),
        vector("accept")
    );
}

#[test]
fn a_malformed_rule_response_is_refused_naming_what_differs() {
    let mut no_version = vector("list");
    no_version["rules"][0]
        .as_object_mut()
        .expect("a rule object")
        .remove("version");
    let mut candidate = vector("list");
    candidate["rules"][0]["state"] = json!("candidate");
    let mut no_scopes = vector("list");
    no_scopes
        .as_object_mut()
        .expect("a list object")
        .remove("scopes");
    for (body, named) in [
        (no_version, "missing field `version`"),
        (candidate, "unknown variant `candidate`"),
        (no_scopes, "missing field `scopes`"),
    ] {
        let err = curie::api::parse_memory_rules(body).expect_err("a malformed list is refused");
        let shown = format!("{err:#}");
        assert!(shown.contains("not a learned rule list"), "{shown}");
        assert!(shown.contains(named), "{shown}");
    }
    let mut now = vector("accept");
    now["applies_from"] = json!("now");
    let err = curie::api::parse_memory_rule_change(now).expect_err("an unknown applies_from");
    assert!(
        format!("{err:#}").contains("unknown variant `now`"),
        "{err:#}"
    );
    let mut no_decider_id = vector("accept");
    no_decider_id["rule"]["accepted_by"]
        .as_object_mut()
        .expect("a decider object")
        .remove("principal_id");
    let err =
        curie::api::parse_memory_rule_change(no_decider_id).expect_err("a decider with no id");
    assert!(
        format!("{err:#}").contains("missing field `principal_id`"),
        "{err:#}"
    );
}

#[test]
fn an_action_answer_without_a_version_exits_1_and_prints_no_rule() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let mut answer = json!({"rule": rule("accepted", 2), "applies_from": "next_ticket"});
    answer["rule"]
        .as_object_mut()
        .expect("a rule object")
        .remove("version");
    let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/agents") => agent_list(),
        ("POST", p) if p == accept_path() => Response::json(200, &answer.to_string()),
        other => panic!("unexpected request: {other:?}"),
    });
    let output = fixture.run(&act_args("--accept", &session, &server.base_url));
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let value = object(&output);
    let error = value["error"].as_str().expect("an error string");
    assert!(error.contains("not a learned rule change"), "{value}");
    assert!(error.contains("missing field `version`"), "{value}");
    assert!(value.get("rule").is_none(), "{value}");
    assert_no_secrets(&output);
}

#[test]
fn a_rule_list_in_another_shape_exits_1() {
    let fixture = Fixture::new();
    let server = serve(|req| match req.path.as_str() {
        "/agents" => agent_list(),
        _ => Response::json(200, &json!({"rules": [{"id": RULE_ID}]}).to_string()),
    });
    let output = fixture.run(&[
        "--json",
        "cluster",
        "memory",
        "acme-bot",
        "--rules",
        "--api-url",
        &server.base_url,
        "--api-key",
        "list-key",
    ]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    let error = object(&output)["error"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(error.contains("not a learned rule list"), "{error}");
}

/// The API always sends its nullable fields: a missing one is refused by name,
/// an explicit null is not.
#[test]
fn a_missing_nullable_field_is_refused_and_an_explicit_null_is_not() {
    let mut no_accepted_by = vector("list");
    no_accepted_by["rules"][0]
        .as_object_mut()
        .expect("a rule object")
        .remove("accepted_by");
    let err = curie::api::parse_memory_rules(no_accepted_by).expect_err("accepted_by missing");
    let shown = format!("{err:#}");
    assert!(shown.contains("missing field `accepted_by`"), "{shown}");
    assert!(shown.contains("at `rules[0]`"), "{shown}");

    let mut no_accepted_at = vector("accept");
    no_accepted_at["rule"]
        .as_object_mut()
        .expect("a rule object")
        .remove("accepted_at");
    let err =
        curie::api::parse_memory_rule_change(no_accepted_at).expect_err("accepted_at missing");
    let shown = format!("{err:#}");
    assert!(shown.contains("missing field `accepted_at`"), "{shown}");
    assert!(shown.contains("at `rule`"), "{shown}");

    let mut no_display_name = vector("accept");
    no_display_name["rule"]["accepted_by"]
        .as_object_mut()
        .expect("a decider object")
        .remove("display_name");
    let err = curie::api::parse_memory_rule_change(no_display_name).expect_err("no display_name");
    assert!(
        format!("{err:#}").contains("missing field `display_name`"),
        "{err:#}"
    );

    let mut nulls = vector("accept");
    nulls["rule"]["accepted_by"] = Value::Null;
    nulls["rule"]["accepted_at"] = Value::Null;
    nulls["rule"]["source_url"] = Value::Null;
    let change = curie::api::parse_memory_rule_change(nulls).expect("explicit nulls parse");
    assert_eq!(change.rule.accepted_by, None);
    assert_eq!(change.rule.accepted_at, None);
}

/// A field of the wrong type is named by its path.
#[test]
fn a_wrong_type_field_is_named_by_its_path() {
    let mut text_version = vector("accept");
    text_version["rule"]["version"] = json!("2");
    let err = curie::api::parse_memory_rule_change(text_version).expect_err("a string version");
    let shown = format!("{err:#}");
    assert!(shown.contains("at `rule.version`"), "{shown}");
    assert!(shown.contains("invalid type"), "{shown}");

    let mut text_limit = vector("list");
    text_limit["in_force_limit"] = json!("16");
    let err = curie::api::parse_memory_rules(text_limit).expect_err("a string limit");
    assert!(
        format!("{err:#}").contains("at `in_force_limit`"),
        "{err:#}"
    );

    let mut text_count = vector("list");
    text_count["scopes"][0]["in_force_count"] = json!(true);
    let err = curie::api::parse_memory_rules(text_count).expect_err("a bool count");
    assert!(
        format!("{err:#}").contains("at `scopes[0].in_force_count`"),
        "{err:#}"
    );
}

#[test]
fn an_action_answer_missing_accepted_at_or_with_a_text_version_exits_1_naming_it() {
    let fixture = Fixture::new();
    let session = fixture.session(0o600);
    let mut missing = json!({"rule": rule("accepted", 2), "applies_from": "next_ticket"});
    missing["rule"]
        .as_object_mut()
        .expect("a rule object")
        .remove("accepted_at");
    let mut text_version = json!({"rule": rule("accepted", 2), "applies_from": "next_ticket"});
    text_version["rule"]["version"] = json!("2");
    for (answer, named) in [
        (missing, "missing field `accepted_at`"),
        (text_version, "rule.version"),
    ] {
        let server = serve(move |req| match (req.method.as_str(), req.path.as_str()) {
            ("GET", "/agents") => agent_list(),
            ("POST", p) if p == accept_path() => Response::json(200, &answer.to_string()),
            other => panic!("unexpected request: {other:?}"),
        });
        let output = fixture.run(&act_args("--accept", &session, &server.base_url));
        assert_eq!(output.status.code(), Some(1), "{}", text(&output));
        let value = object(&output);
        let error = value["error"].as_str().expect("an error string");
        assert!(error.contains(named), "{value}");
        assert!(value.get("rule").is_none(), "{value}");
    }
}
