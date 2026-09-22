//! Binary-level contract for the factory work item read verbs (#2577).
//!
//! The platform API is the only mocked boundary: each test drives the built
//! `curie` process through clap, the real API client and the centralized
//! emitters against a wire-level HTTP peer, and asserts on stdout and the
//! process exit code (ADR 0021/0041: 0 ok, 1 failure, 2 usage, 3 transient,
//! 4 unsupported at this tier).

mod support;

use std::fs;
use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "curie-work-items-test-key";
const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const ITEM_ID: &str = "22222222-2222-4222-8222-222222222222";
const UNREACHABLE_API_URL: &str = "http://127.0.0.1:1";
/// A cause string the CLI cannot have special-cased: it must be rendered
/// verbatim, never re-derived.
const CAUSE: &str = "no capacity before the waiting deadline (capacity_wait_expired) zq-verbatim-7";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn run(args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .stdin(Stdio::null())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap_or_else(|error| panic!("run curie {}: {error}", args.join(" ")))
}

fn local(extra: &[&str], api_url: &str, json: bool) -> Output {
    let mut args = vec!["local", "work-items"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--api-url", api_url, "--api-key", TEST_API_KEY]);
    if json {
        args.push("--json");
    }
    run(&args)
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn describe(output: &Output) -> String {
    format!(
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        stdout(output),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn one_object(output: &Output) -> Value {
    let mut values = serde_json::Deserializer::from_slice(&output.stdout).into_iter::<Value>();
    let value = values
        .next()
        .unwrap_or_else(|| panic!("stdout must hold one JSON object: {}", describe(output)))
        .unwrap_or_else(|error| panic!("stdout must be JSON ({error}): {}", describe(output)));
    assert!(
        values.next().is_none(),
        "exactly one JSON value: {}",
        describe(output)
    );
    value
}

fn assert_schema(value: &Value) {
    let path = format!(
        "{}/schema/work-items.schema.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("committed schema {path} must exist: {error}"));
    let schema: Value = serde_json::from_str(&raw).expect("schema is JSON");
    let validator = jsonschema::validator_for(&schema).expect("schema compiles");
    assert!(
        validator.is_valid(value),
        "{value} must validate against {path}"
    );
}

fn item(state: &str) -> Value {
    json!({
        "id": ITEM_ID,
        "agent_id": AGENT_ID,
        "repo_full_name": "acme-corp/acme-bot",
        "github_issue_number": 2577,
        "issue_url": "https://github.com/acme-corp/acme-bot/issues/2577",
        "cancelled_at": null,
        "created_at": "2026-09-22T10:00:00Z",
        "updated_at": "2026-09-22T10:05:00Z",
        "objective": "Implement the admitted work item",
        "objective_truncated": false,
        "requester": "U0REQUEST1",
        "state": state,
        "actionable_cause": CAUSE,
        "pr": {"number": 123, "url": "https://github.com/acme-corp/acme-bot/pull/123", "status": "open"},
        "publication": {"status": "succeeded", "revision_number": 1, "approval_status": "approved"},
        "correctness": {"asserted": false, "owner": "bundle"},
        "ci": null,
        "requests": [{
            "sequence": 1,
            "status": "expired",
            "created_at": "2026-09-22T10:00:00Z",
            "wait_deadline": "2026-09-23T10:00:00Z",
            "started_at": null,
            "execution_deadline": null,
            "terminal_at": "2026-09-23T10:00:01Z",
            "terminal_cause": "capacity_wait_expired",
            "termination_observation": null,
            "capacity_deferrals": 2,
            "last_deferral_reason": "capacity"
        }]
    })
}

fn detail_item() -> Value {
    let mut value = item("published");
    value["ci"] = json!({
        "state": "unavailable",
        "reason": "github_forbidden",
        "observed_at": "2026-09-22T10:06:00Z",
        "head_sha": "1123456789abcdef0123456789abcdef01234567"
    });
    value
}

fn list_body(items: Vec<Value>, limit: u64, truncated: bool) -> String {
    json!({"items": items, "limit": limit, "truncated": truncated}).to_string()
}

fn agents_response() -> Response {
    Response::json(
        200,
        &format!(
            r##"[{{"id":"{AGENT_ID}","name":"weather","channels":[{{"kind":"slack","address":"#weather"}}],"approval_required_tools":[],"memory":false}}]"##
        ),
    )
}

fn route(path: &str) -> &str {
    path.split('?').next().unwrap()
}

fn list_server(items: Vec<Value>) -> MockServer {
    let body = list_body(items, 200, false);
    serve(move |req| match route(&req.path) {
        "/agents" => agents_response(),
        "/work-items" => Response::json(200, &body),
        other => Response::json(500, &format!(r#"{{"detail":"unexpected {other}"}}"#)),
    })
}

// --- list -------------------------------------------------------------------

#[test]
fn list_renders_api_state_and_cause_verbatim() {
    let server = list_server(vec![item("expired")]);

    let output = local(&[], &server.base_url, false);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let text = stdout(&output);
    assert!(text.contains("expired"), "state missing: {text}");
    assert!(
        text.contains("zq-verbatim-7"),
        "cause must be verbatim: {text}"
    );
    assert!(
        text.contains(&format!("acme-corp/acme-bot#{}", 2577)),
        "repo#issue missing: {text}"
    );
    assert!(text.contains("123"), "PR missing: {text}");
}

#[test]
fn list_json_is_the_stable_envelope_and_validates_against_the_schema() {
    let server = list_server(vec![item("expired")]);

    let output = local(&[], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_eq!(value["items"][0]["state"], "expired");
    assert_eq!(value["items"][0]["actionable_cause"], CAUSE);
    assert_eq!(
        value["items"][0]["correctness"],
        json!({"asserted": false, "owner": "bundle"})
    );
    assert_eq!(value["truncated"], false);
    assert_schema(&value);
    let recorded = server.recorded();
    let request = recorded
        .iter()
        .find(|r| route(&r.path) == "/work-items")
        .expect("GET /work-items was called");
    assert_eq!(request.method, "GET");
    assert_eq!(request.header("x-api-key"), Some(TEST_API_KEY));
}

#[test]
fn empty_install_is_an_empty_list_and_exit_zero() {
    let server = list_server(vec![]);

    let output = local(&[], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_eq!(value["items"], json!([]));
    assert_eq!(value["truncated"], false);
    assert_schema(&value);
}

#[test]
fn agent_filter_resolves_the_name_to_an_id_and_asks_for_the_max_page() {
    let server = list_server(vec![item("waiting")]);

    let output = local(&["--agent", "weather"], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let recorded = server.recorded();
    let request = recorded
        .iter()
        .find(|r| route(&r.path) == "/work-items")
        .expect("GET /work-items was called");
    assert!(
        request.path.contains(&format!("agent_id={AGENT_ID}")),
        "{}",
        request.path
    );
    assert!(request.path.contains("limit=200"), "{}", request.path);
}

#[test]
fn unknown_agent_name_is_exit_one_and_never_lists() {
    let server = list_server(vec![item("waiting")]);

    let output = local(&["--agent", "no-such-agent"], &server.base_url, true);

    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    assert!(
        server
            .recorded()
            .iter()
            .all(|r| route(&r.path) != "/work-items"),
        "an unresolved agent must not fall back to the unfiltered list"
    );
}

#[test]
fn truncated_list_is_reported() {
    let body = list_body(vec![item("waiting")], 1, true);
    let server = serve(move |req| match route(&req.path) {
        "/work-items" => Response::json(200, &body),
        _ => Response::json(500, "{}"),
    });

    let output = local(&[], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(one_object(&output)["truncated"], true);
}

// --- detail -----------------------------------------------------------------

#[test]
fn detail_shows_ci_state_and_reason() {
    let body = detail_item().to_string();
    let server = serve(move |req| {
        if route(&req.path) == format!("/work-items/{ITEM_ID}") {
            Response::json(200, &body)
        } else {
            Response::json(500, "{}")
        }
    });

    let human = local(&[ITEM_ID], &server.base_url, false);
    assert_eq!(human.status.code(), Some(0), "{}", describe(&human));
    let text = stdout(&human);
    assert!(text.contains("published"), "{text}");
    assert!(text.contains("unavailable"), "{text}");
    assert!(text.contains("github_forbidden"), "{text}");

    let machine = local(&[ITEM_ID], &server.base_url, true);
    assert_eq!(machine.status.code(), Some(0), "{}", describe(&machine));
    let value = one_object(&machine);
    assert_eq!(value["item"]["id"], ITEM_ID);
    assert_eq!(value["item"]["ci"]["reason"], "github_forbidden");
    assert_schema(&value);
}

#[test]
fn detail_scoped_to_an_agent_passes_the_agent_id() {
    let body = detail_item().to_string();
    let server = serve(move |req| match route(&req.path) {
        "/agents" => agents_response(),
        p if p == format!("/work-items/{ITEM_ID}") => Response::json(200, &body),
        _ => Response::json(500, "{}"),
    });

    let output = local(&[ITEM_ID, "--agent", "weather"], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let recorded = server.recorded();
    let request = recorded
        .iter()
        .find(|r| route(&r.path).starts_with("/work-items/"))
        .expect("detail was requested");
    assert!(
        request.path.contains(&format!("agent_id={AGENT_ID}")),
        "{}",
        request.path
    );
}

fn detail_status(status: u16) -> Output {
    let server = serve(move |_req| {
        Response::json(
            status,
            r#"{"detail":{"code":"not_found"},"code":"not_found"}"#,
        )
    });
    let output = local(&[ITEM_ID], &server.base_url, true);
    // The exit code must come from the API's answer, not from a clap refusal.
    assert!(
        server
            .recorded()
            .iter()
            .any(|r| route(&r.path) == format!("/work-items/{ITEM_ID}")),
        "status {status}: the detail route was never called: {}",
        describe(&output)
    );
    output
}

#[test]
fn detail_not_found_is_exit_one() {
    let output = detail_status(404);
    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
}

#[test]
fn detail_unauthorized_is_exit_one_with_the_api_key_fix() {
    let output = detail_status(401);
    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    let all = format!(
        "{}{}",
        stdout(&output),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        all.to_ascii_lowercase().contains("api key") || all.contains("api-key"),
        "{all}"
    );
    assert!(
        !all.contains(TEST_API_KEY),
        "the API key value must never be echoed: {all}"
    );
}

#[test]
fn detail_unprocessable_is_exit_two() {
    let output = detail_status(422);
    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
}

#[test]
fn server_unavailable_is_exit_three() {
    for status in [500, 502, 503, 504] {
        let output = detail_status(status);
        assert_eq!(
            output.status.code(),
            Some(3),
            "status {status}: {}",
            describe(&output)
        );
    }
}

#[test]
fn server_unavailable_does_not_echo_the_response_body() {
    // ADR 0021/0041 transient path (exit 3): the message names the status and
    // endpoint, never the upstream response text, in either output mode
    // (#2577 E2E defect 2).
    const MARKER: &str = "zq-body-must-never-surface-5c2e9f";
    let body = format!(r#"{{"detail":"{MARKER}"}}"#);
    let server = serve(move |_req| Response::json(503, &body));

    for json in [false, true] {
        let output = local(&[ITEM_ID], &server.base_url, json);
        assert_eq!(output.status.code(), Some(3), "{}", describe(&output));
        let all = format!(
            "{}{}",
            stdout(&output),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !all.contains(MARKER),
            "the upstream response body must never be echoed: {all}"
        );
    }
}

#[test]
fn unreachable_api_is_exit_three() {
    let list = local(&[], UNREACHABLE_API_URL, true);
    assert_eq!(list.status.code(), Some(3), "{}", describe(&list));
    let detail = local(&[ITEM_ID], UNREACHABLE_API_URL, false);
    assert_eq!(detail.status.code(), Some(3), "{}", describe(&detail));
}

#[test]
fn dry_run_makes_no_request() {
    let server = list_server(vec![item("waiting")]);

    let output = local(&["--dry-run"], &server.base_url, false);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert!(
        server.recorded().is_empty(),
        "dry-run must not call the API"
    );
}

#[test]
fn json_output_carries_no_token_like_or_runtime_owner_fields() {
    // Even if a future API leaked extra fields, the CLI envelope must not
    // grow them: the committed schema is additionalProperties:false.
    let mut leaky = detail_item();
    leaky["runtime_owner"] = json!("curie-workers-a");
    leaky["token"] = json!("ghs_SENTINELTOKEN");
    let body = leaky.to_string();
    let server = serve(move |_req| Response::json(200, &body));

    let output = local(&[ITEM_ID], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_eq!(value["item"]["state"], "published");
    let text = stdout(&output);
    assert!(!text.contains("ghs_SENTINELTOKEN"), "{text}");
    assert!(!text.contains("runtime_owner"), "{text}");
    assert!(!text.contains(TEST_API_KEY), "{text}");
}

// --- tiers ------------------------------------------------------------------

#[test]
fn skill_tier_is_unsupported_exit_four() {
    for args in [
        vec!["skill", "work-items"],
        vec!["skill", "work-items", ITEM_ID, "--agent", "weather"],
    ] {
        let output = run(&args);
        assert_eq!(
            output.status.code(),
            Some(4),
            "{args:?}: {}",
            describe(&output)
        );
        let all = format!(
            "{}{}",
            stdout(&output),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            all.contains("work-items"),
            "must point at local/cluster work-items: {all}"
        );
    }
}

#[test]
fn cluster_and_local_verbs_parse() {
    for args in [
        vec!["cluster", "work-items", "--help"],
        vec!["local", "work-items", "--help"],
    ] {
        let output = run(&args);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{args:?}: {}",
            describe(&output)
        );
        let text = stdout(&output);
        assert!(text.contains("--agent"), "{args:?}: {text}");
        assert!(text.contains("--dry-run"), "{args:?}: {text}");
    }
}

// --- review round 1 regressions ------------------------------------------------

const ENV_KEY_SENTINEL: &str = "curie-env-key-SENTINEL-2577";

#[test]
fn help_never_prints_the_api_key_from_the_environment() {
    for args in [
        vec!["local", "work-items", "--help"],
        vec!["cluster", "work-items", "--help"],
    ] {
        let output = Command::new(bin())
            .args(&args)
            .stdin(Stdio::null())
            .env_remove("CURIE_API_URL")
            .env("CURIE_API_KEY", ENV_KEY_SENTINEL)
            .output()
            .expect("run curie --help");
        assert_eq!(
            output.status.code(),
            Some(0),
            "{args:?}: {}",
            describe(&output)
        );
        let all = format!(
            "{}{}",
            stdout(&output),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !all.contains(ENV_KEY_SENTINEL),
            "{args:?}: help must not print the CURIE_API_KEY value: {all}"
        );
    }
}

#[test]
fn agent_lookup_unavailable_is_exit_three() {
    let server = serve(move |req| match route(&req.path) {
        "/agents" => Response::json(503, r#"{"detail":"unavailable"}"#),
        _ => Response::json(200, &list_body(vec![], 200, false)),
    });

    let output = local(&["--agent", "weather"], &server.base_url, true);

    assert_eq!(output.status.code(), Some(3), "{}", describe(&output));
    assert!(
        server
            .recorded()
            .iter()
            .all(|r| route(&r.path) != "/work-items"),
        "an unresolved agent must not fall back to the unfiltered list"
    );
}

#[test]
fn agent_lookup_that_never_answers_is_bounded_and_exit_three() {
    use std::io::Read;
    use std::net::TcpListener;
    use std::time::{Duration, Instant};

    // Accepts every connection and reads the request, but never writes a byte.
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind silent listener");
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming().flatten() {
            let mut reader = stream.try_clone().unwrap();
            std::thread::spawn(move || {
                let mut buf = [0u8; 4096];
                while matches!(reader.read(&mut buf), Ok(n) if n > 0) {}
            });
            held.push(stream);
        }
    });

    let started = Instant::now();
    let mut child = Command::new(bin())
        .args([
            "local",
            "work-items",
            "--agent",
            "weather",
            "--json",
            "--api-url",
            &base_url,
            "--api-key",
            TEST_API_KEY,
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .spawn()
        .expect("spawn curie");
    let bound = Duration::from_secs(45);
    loop {
        if child.try_wait().expect("poll curie").is_some() {
            break;
        }
        if started.elapsed() > bound {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!(
                "agent lookup hung past {bound:?} against a silent API: {}",
                describe(&output)
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let output = child.wait_with_output().unwrap();
    assert_eq!(output.status.code(), Some(3), "{}", describe(&output));
}

// --- E2E polish regressions ---------------------------------------------------

#[test]
fn non_uuid_id_is_a_usage_error_before_any_request() {
    let server = list_server(vec![item("waiting")]);

    let output = local(&["not-a-uuid"], &server.base_url, false);

    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("UUID"),
        "{}",
        describe(&output)
    );
    assert!(server.recorded().is_empty(), "no request may be made");
}

#[test]
fn detail_ci_without_a_reason_prints_no_parenthetical() {
    let mut value = detail_item();
    value["ci"]["state"] = json!("passing");
    value["ci"]["reason"] = Value::Null;
    let body = value.to_string();
    let server = serve(move |_req| Response::json(200, &body));

    let output = local(&[ITEM_ID], &server.base_url, false);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let text = stdout(&output);
    let ci = text
        .lines()
        .find(|l| l.starts_with("ci "))
        .unwrap_or_else(|| panic!("ci line: {text}"));
    assert_eq!(ci.trim_end(), "ci           passing", "{text}");
}

#[test]
fn list_does_not_pad_the_last_column() {
    let mut short = item("waiting");
    short["id"] = json!("33333333-3333-4333-8333-333333333333");
    short["actionable_cause"] = json!("short");
    let server = list_server(vec![item("expired"), short]);

    let output = local(&[], &server.base_url, false);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    for line in stdout(&output).lines() {
        assert_eq!(line, line.trim_end(), "trailing padding: {line:?}");
    }
}

// --- review round 2 regressions ------------------------------------------------

/// A non-UUID ID is a usage error (exit 2) at both tiers, in human and
/// `--json` modes, before any request; `--json` still emits the structured
/// ADR-0021 `{error, ...}` payload on stdout (cli/CLAUDE.md error contract).
#[test]
fn non_uuid_id_is_a_structured_usage_error_at_both_tiers() {
    for tier in ["local", "cluster"] {
        for json in [false, true] {
            let server = list_server(vec![item("waiting")]);
            let mut args = vec![
                tier,
                "work-items",
                "not-a-uuid",
                "--api-url",
                &server.base_url,
                "--api-key",
                TEST_API_KEY,
            ];
            if json {
                args.push("--json");
            }
            let output = run(&args);
            assert_eq!(
                output.status.code(),
                Some(2),
                "{args:?}: {}",
                describe(&output)
            );
            if json {
                let value = one_object(&output);
                let error = value["error"]
                    .as_str()
                    .unwrap_or_else(|| panic!("{args:?}: error string: {value}"));
                assert!(error.contains("UUID"), "{args:?}: {value}");
            } else {
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains("UUID"),
                    "{args:?}: {}",
                    describe(&output)
                );
            }
            assert!(
                server.recorded().is_empty(),
                "{args:?}: no request may be made"
            );
        }
    }
}
