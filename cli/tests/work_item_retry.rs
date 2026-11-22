//! Binary-level contract for `curie cluster work-items ID --rerun` (#5431).
//!
//! The platform API is the only mocked boundary. Response bodies follow the
//! "Wire contract" section of the #5431 plan (WorkItemRetryOut and the 409
//! `detail` shapes). Exit codes: 0 ok, 1 failure, 2 usage, 3 transient.

use crate::support;

use std::fs;
use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "curie-work-item-retry-test-key";
const ITEM_ID: &str = "22222222-2222-4222-8222-222222222222";
const REQUEST_ID: &str = "33333333-3333-4333-8333-333333333333";
const PREDECESSOR: &str = "44444444-4444-4444-8444-444444444444";
const SUCCESSOR: &str = "55555555-5555-4555-8555-555555555555";
const UNREACHABLE_API_URL: &str = "http://127.0.0.1:1";

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

/// `cluster work-items ID --rerun ...` against `api_url` with `extra` flags.
fn rerun(api_url: &str, extra: &[&str]) -> Output {
    let mut args = vec![
        "cluster",
        "work-items",
        ITEM_ID,
        "--rerun",
        "--request-id",
        REQUEST_ID,
        "--expected-version",
        "7",
        "--reason",
        "operator rerun after the outage",
        "--api-url",
        api_url,
        "--api-key",
        TEST_API_KEY,
        "--json",
    ];
    args.extend_from_slice(extra);
    run(&args)
}

fn describe(output: &Output) -> String {
    format!(
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
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

/// Error payload: `--json` writes it to stdout or stderr depending on the
/// centralized emitter, so read whichever holds the JSON.
fn error_payload(output: &Output) -> Value {
    for bytes in [&output.stdout, &output.stderr] {
        if let Ok(text) = std::str::from_utf8(bytes) {
            for line in text.lines() {
                if let Ok(value) = serde_json::from_str::<Value>(line.trim()) {
                    if value.get("error").is_some() {
                        return value;
                    }
                }
            }
        }
    }
    panic!("no JSON error payload: {}", describe(output));
}

fn assert_schema(value: &Value) {
    let path = format!(
        "{}/schema/work-item-retry.schema.json",
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

fn retry_out(dry_run: bool, replayed: bool) -> Value {
    json!({
        "work_item_id": ITEM_ID,
        "request_id": REQUEST_ID,
        "dry_run": dry_run,
        "replayed": replayed,
        "disposition": "implementation",
        "predecessor_request_id": PREDECESSOR,
        "predecessor_cause": "runner_failed",
        "successor_request_id": if dry_run { Value::Null } else { json!(SUCCESSOR) },
        "successor_dispatch_not_before": "2026-11-15T12:00:00Z",
        "successor_blocked_on": null,
        "work_item_version": 8,
        "retries_used": 2,
        "retry_limit": 3,
        "actor_kind": "platform_key",
        "recorded_at": if dry_run { Value::Null } else { json!("2026-11-15T12:00:01Z") },
    })
}

const CONTRACT_FIELDS: [&str; 15] = [
    "work_item_id",
    "request_id",
    "dry_run",
    "replayed",
    "disposition",
    "predecessor_request_id",
    "predecessor_cause",
    "successor_request_id",
    "successor_dispatch_not_before",
    "successor_blocked_on",
    "work_item_version",
    "retries_used",
    "retry_limit",
    "actor_kind",
    "recorded_at",
];

fn fixed(status: u16, body: Value) -> MockServer {
    let body = body.to_string();
    serve(move |_| Response::json(status, &body))
}

fn retry_posts(server: &MockServer) -> Vec<Value> {
    server
        .recorded()
        .iter()
        .filter(|r| r.method == "POST" && r.path == format!("/work-items/{ITEM_ID}/retry"))
        .map(|r| serde_json::from_slice(&r.body).expect("request body is JSON"))
        .collect()
}

#[test]
fn success_is_one_contract_object_and_sends_the_four_fields() {
    let server = fixed(200, retry_out(false, false));

    let output = rerun(&server.base_url, &["--yes"]);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    for field in CONTRACT_FIELDS {
        assert!(value.get(field).is_some(), "missing {field}: {value}");
    }
    assert_schema(&value);
    assert_eq!(value["successor_request_id"], SUCCESSOR);
    assert_eq!(value["replayed"], false);
    let posts = retry_posts(&server);
    assert_eq!(posts.len(), 1, "{:?}", server.recorded());
    assert_eq!(
        posts[0],
        json!({
            "request_id": REQUEST_ID,
            "expected_version": 7,
            "reason": "operator rerun after the outage",
            "dry_run": false,
        })
    );
    assert_eq!(server.recorded()[0].header("x-api-key"), Some(TEST_API_KEY));
}

#[test]
fn replay_is_reported_with_the_stored_result() {
    let server = fixed(200, retry_out(false, true));

    let output = rerun(&server.base_url, &["--yes"]);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(one_object(&output)["replayed"], true);
}

#[test]
fn dry_run_sends_dry_run_true_and_prints_the_plan_without_yes() {
    let server = fixed(200, retry_out(true, false));

    let output = rerun(&server.base_url, &["--dry-run"]);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_schema(&value);
    assert_eq!(value["dry_run"], true);
    assert!(value["successor_request_id"].is_null());
    let posts = retry_posts(&server);
    assert_eq!(posts.len(), 1);
    assert_eq!(posts[0]["dry_run"], true);
}

#[test]
fn every_409_code_is_exit_one_with_the_code_in_the_json_error() {
    for code in [
        "idempotency_conflict",
        "stale_version",
        "work_item_cancelled",
        "readmit_pending",
        "active_execution",
        "no_failed_request",
        "teardown_unconfirmed",
        "allowance_exhausted",
        "not_retryable",
        "side_effects_unproven",
        "admission_refused",
    ] {
        let message = format!("operator sentence for {code}");
        let server = fixed(
            409,
            json!({"detail": {"code": code, "message": message, "retries_used": 3, "retry_limit": 3}}),
        );

        let output = rerun(&server.base_url, &["--yes"]);

        assert_eq!(
            output.status.code(),
            Some(1),
            "{code}: {}",
            describe(&output)
        );
        let payload = error_payload(&output);
        let error = payload["error"].as_str().unwrap_or_default();
        assert!(error.contains(code), "{code}: {payload}");
        assert!(error.contains(&message), "{code}: {payload}");
        assert!(
            payload["fix"].as_str().is_some_and(|f| !f.is_empty()),
            "{code}: {payload}"
        );
    }
}

#[test]
fn stale_version_fix_names_the_reread() {
    let server = fixed(
        409,
        json!({"detail": {"code": "stale_version", "message": "moved", "current_version": 9}}),
    );

    let output = rerun(&server.base_url, &["--yes"]);

    let payload = error_payload(&output);
    assert!(payload["fix"]
        .as_str()
        .unwrap()
        .contains("curie cluster work-items ID"));
}

#[test]
fn not_found_is_exit_one() {
    let server = fixed(404, json!({"detail": {"code": "not_found"}}));

    let output = rerun(&server.base_url, &["--yes"]);

    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    assert!(error_payload(&output)["error"]
        .as_str()
        .unwrap()
        .contains("not_found"));
}

#[test]
fn validation_422_is_usage_exit_two() {
    let server = fixed(422, json!({"detail": [{"msg": "reason-422-sentinel"}]}));

    let output = rerun(&server.base_url, &["--yes"]);

    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
    let error = error_payload(&output)["error"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(error.contains("work item retry rejected"), "{error}");
    assert!(error.contains("reason-422-sentinel"), "{error}");
    assert_eq!(retry_posts(&server).len(), 1);
}

#[test]
fn server_error_is_transient_exit_three() {
    let server = fixed(500, json!({"detail": "boom"}));

    let output = rerun(&server.base_url, &["--yes"]);

    assert_eq!(output.status.code(), Some(3), "{}", describe(&output));
}

#[test]
fn unreachable_api_is_exit_three() {
    let output = rerun(UNREACHABLE_API_URL, &["--yes"]);

    assert_eq!(output.status.code(), Some(3), "{}", describe(&output));
}

#[test]
fn unauthorized_is_exit_one_without_echoing_the_body() {
    let server = fixed(401, json!({"detail": "leaky-body-sentinel-91"}));

    let output = rerun(&server.base_url, &["--yes"]);

    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    assert!(!describe(&output).contains("leaky-body-sentinel-91"));
    assert!(error_payload(&output)["fix"]
        .as_str()
        .unwrap()
        .contains("--api-key"));
}

#[test]
fn missing_yes_is_usage_with_no_request() {
    let server = fixed(200, retry_out(false, false));

    let output = rerun(&server.base_url, &[]);

    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
    assert!(describe(&output).contains("--yes"), "{}", describe(&output));
    assert!(
        server.recorded().is_empty(),
        "no request without --yes: {:?}",
        server.recorded()
    );
}

#[test]
fn agent_with_rerun_is_usage() {
    let server = fixed(200, retry_out(false, false));

    let output = rerun(
        &server.base_url,
        &["--yes", "--agent", "11111111-1111-1111-1111-111111111111"],
    );

    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
    let text = describe(&output);
    assert!(
        text.contains("'--rerun' cannot be used with '--agent"),
        "{text}"
    );
    assert!(!text.contains("unexpected argument"), "{text}");
    assert!(server.recorded().is_empty());
}

#[test]
fn non_uuid_request_id_is_usage() {
    let server = fixed(200, retry_out(false, false));
    let args = [
        "cluster",
        "work-items",
        ITEM_ID,
        "--rerun",
        "--request-id",
        "not-a-uuid",
        "--expected-version",
        "7",
        "--reason",
        "why",
        "--yes",
        "--api-url",
        &server.base_url,
        "--api-key",
        TEST_API_KEY,
        "--json",
    ];

    let output = run(&args);

    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
    let error = error_payload(&output)["error"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(error.contains("--request-id must be a UUID"), "{error}");
    assert!(server.recorded().is_empty());
}

#[test]
fn non_uuid_work_item_id_is_usage() {
    let server = fixed(200, retry_out(false, false));
    let args = [
        "cluster",
        "work-items",
        "nope",
        "--rerun",
        "--request-id",
        REQUEST_ID,
        "--expected-version",
        "7",
        "--reason",
        "why",
        "--yes",
        "--api-url",
        &server.base_url,
        "--api-key",
        TEST_API_KEY,
    ];

    let output = run(&args);

    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
    assert!(server.recorded().is_empty());
}

#[test]
fn reason_over_500_chars_is_usage() {
    let server = fixed(200, retry_out(false, false));
    let long = "x".repeat(501);
    let args = [
        "cluster",
        "work-items",
        ITEM_ID,
        "--rerun",
        "--request-id",
        REQUEST_ID,
        "--expected-version",
        "7",
        "--reason",
        long.as_str(),
        "--yes",
        "--api-url",
        &server.base_url,
        "--api-key",
        TEST_API_KEY,
        "--json",
    ];

    let output = run(&args);

    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
    let error = error_payload(&output)["error"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        error.contains("--reason must be 1 to 500 characters"),
        "{error}"
    );
    assert!(server.recorded().is_empty());
}

#[test]
fn rerun_only_flags_without_rerun_are_usage() {
    for flag in [
        vec!["--request-id", REQUEST_ID],
        vec!["--expected-version", "7"],
        vec!["--reason", "why"],
        vec!["--yes"],
    ] {
        let mut args = vec!["cluster", "work-items", ITEM_ID];
        args.extend(flag.iter().copied());
        args.extend(["--api-url", UNREACHABLE_API_URL, "--api-key", TEST_API_KEY]);

        let output = run(&args);

        assert_eq!(
            output.status.code(),
            Some(2),
            "{flag:?}: {}",
            describe(&output)
        );
        let text = describe(&output);
        assert!(
            text.contains("required arguments were not provided") && text.contains("--rerun"),
            "{flag:?}: {text}"
        );
        assert!(!text.contains("unexpected argument"), "{flag:?}: {text}");
    }
}

#[test]
fn plain_read_and_local_tier_are_unchanged() {
    let server = serve(|request| {
        if request.method == "GET" && request.path.starts_with("/work-items") {
            Response::json(200, r#"{"items":[],"truncated":false,"limit":200}"#)
        } else {
            Response::json(404, "{}")
        }
    });

    let cluster = run(&[
        "cluster",
        "work-items",
        "--api-url",
        &server.base_url,
        "--api-key",
        TEST_API_KEY,
        "--json",
    ]);
    let local = run(&[
        "local",
        "work-items",
        "--api-url",
        &server.base_url,
        "--api-key",
        TEST_API_KEY,
        "--json",
    ]);
    let local_rerun = run(&["local", "work-items", ITEM_ID, "--rerun"]);

    assert_eq!(cluster.status.code(), Some(0), "{}", describe(&cluster));
    assert_eq!(local.status.code(), Some(0), "{}", describe(&local));
    assert_eq!(
        local_rerun.status.code(),
        Some(2),
        "{}",
        describe(&local_rerun)
    );
    assert!(server.recorded().iter().all(|r| r.method == "GET"));
}
