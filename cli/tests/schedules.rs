//! Binary contract for `curie local schedules` and `curie cluster schedules` (#2933).
//!
//! The platform API is the only mocked boundary: each test drives the built
//! `curie` process through clap, the real API client, and the centralized
//! emitters against a wire-level HTTP peer, and asserts on stdout and the
//! process exit code (0 ok, 1 failure, 3 transient, 4 unsupported at this tier).

mod support;

use std::fs;
use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "curie-schedules-test-key";
const AGENT_ID: &str = "11111111-1111-4111-8111-111111111111";
const UNREACHABLE_API_URL: &str = "http://127.0.0.1:1";
const ENV_KEY_SENTINEL: &str = "curie-env-key-SENTINEL-2933";
/// A kubeconfig path that does not exist, so a cluster lookup cannot succeed quietly.
const MISSING_KUBECONFIG: &str = "/nonexistent/curie-test-kubeconfig";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn run_in(args: &[&str], extra_env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(bin());
    command
        .args(args)
        .stdin(Stdio::null())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY");
    for (key, value) in extra_env {
        command.env(key, value);
    }
    command
        .output()
        .unwrap_or_else(|error| panic!("run curie {}: {error}", args.join(" ")))
}

fn run(args: &[&str]) -> Output {
    run_in(args, &[])
}

fn local(extra: &[&str], api_url: &str, json_mode: bool) -> Output {
    let mut args = vec!["local", "schedules"];
    args.extend_from_slice(extra);
    args.extend_from_slice(&["--api-url", api_url, "--api-key", TEST_API_KEY]);
    if json_mode {
        args.push("--json");
    }
    run(&args)
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn combined(output: &Output) -> String {
    format!(
        "{}{}",
        stdout(output),
        String::from_utf8_lossy(&output.stderr)
    )
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
        "{}/schema/schedules.schema.json",
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

fn schedules_body() -> Value {
    json!({
        "schedules": [
            {
                "agent": "acme-bot",
                "agent_id": AGENT_ID,
                "bundle_error": null,
                "hooks": [
                    {
                        "name": "nightly-cleanup",
                        "trigger": "cron",
                        "schedule": "0 9 * * *",
                        "zone": "UTC",
                        "last_fire_at": "2026-09-25T09:00:00Z",
                        "last_outcome": "failed"
                    },
                    {
                        "name": "weekly-report",
                        "trigger": "cron",
                        "schedule": "0 9 * * 1",
                        "zone": "UTC",
                        "last_fire_at": "2026-09-21T09:00:00Z",
                        "last_outcome": "ran"
                    }
                ]
            }
        ]
    })
}

fn route(path: &str) -> &str {
    path.split('?').next().unwrap()
}

fn list_server() -> MockServer {
    let body = schedules_body().to_string();
    serve(move |req| match route(&req.path) {
        "/schedules" => Response::json(200, &body),
        other => Response::json(500, &format!(r#"{{"detail":"unexpected {other}"}}"#)),
    })
}

fn schedules_request(recorded: &[support::Request]) -> &support::Request {
    recorded
        .iter()
        .find(|request| route(&request.path) == "/schedules")
        .expect("GET /schedules was called")
}

fn assert_api_key(request: &support::Request) {
    assert_eq!(request.header("x-api-key"), Some(TEST_API_KEY));
    assert!(
        request
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("X-API-Key")),
        "header must be X-API-Key: {:?}",
        request.headers
    );
}

#[test]
fn local_schedules_json_equals_the_api_body() {
    let server = list_server();

    let output = local(&[], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_eq!(value, schedules_body());
    assert_schema(&value);
    let recorded = server.recorded();
    let request = schedules_request(&recorded);
    assert_eq!(request.method, "GET");
    assert_eq!(route(&request.path), "/schedules");
    assert_api_key(request);
}

#[test]
fn agent_filter_queries_schedules_and_does_not_list_agents() {
    let server = list_server();

    let output = local(&["--agent", "acme-bot"], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(one_object(&output), schedules_body());
    let recorded = server.recorded();
    let request = schedules_request(&recorded);
    assert_eq!(request.method, "GET");
    assert!(
        request.path.contains("agent=acme-bot"),
        "query must name the agent: {}",
        request.path
    );
    assert_api_key(request);
    assert!(
        recorded.iter().all(|request| route(&request.path) != "/agents"),
        "the CLI must not look the agent up itself: {:?}",
        recorded.iter().map(|request| request.path.clone()).collect::<Vec<_>>()
    );
}

#[test]
fn dry_run_performs_no_http_and_plans_schedules() {
    let server = list_server();

    let output = local(&["--dry-run"], &server.base_url, true);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert!(
        server.recorded().is_empty(),
        "dry-run must not call the API: {:?}",
        server.recorded()
    );
    let value = one_object(&output);
    assert_eq!(value["dry_run"], json!(true));
    let plan = value["plan"]
        .as_array()
        .unwrap_or_else(|| panic!("plan must be an array: {value}"));
    assert!(
        plan.iter()
            .any(|line| line.as_str().unwrap_or("").contains("/schedules")),
        "plan must name /schedules: {value}"
    );
    assert_schema(&value);
}

#[test]
fn human_output_contains_the_hook_name_and_failed() {
    let server = list_server();

    let output = local(&[], &server.base_url, false);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let text = stdout(&output);
    assert!(text.contains("nightly-cleanup"), "hook name missing: {text}");
    assert!(text.contains("failed"), "outcome missing: {text}");
}

#[test]
fn cluster_schedules_json_requests_schedules_without_kube() {
    let server = list_server();
    let args = [
        "cluster",
        "schedules",
        "--api-url",
        server.base_url.as_str(),
        "--api-key",
        TEST_API_KEY,
        "--json",
    ];

    let output = run_in(&args, &[("KUBECONFIG", MISSING_KUBECONFIG)]);

    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(one_object(&output), schedules_body());
    let recorded = server.recorded();
    let request = schedules_request(&recorded);
    assert_eq!(request.method, "GET");
    assert_eq!(route(&request.path), "/schedules");
    assert_api_key(request);
}

#[test]
fn skill_schedules_is_exit_four() {
    let output = run(&["skill", "schedules"]);

    assert_eq!(output.status.code(), Some(4), "{}", describe(&output));
    let all = combined(&output);
    assert!(
        all.contains("local schedules") || all.contains("cluster schedules"),
        "must point at local or cluster schedules: {all}"
    );
}

#[test]
fn help_mentions_agent_and_dry_run_and_hides_the_api_key() {
    for args in [
        ["local", "schedules", "--help"],
        ["cluster", "schedules", "--help"],
    ] {
        let output = Command::new(bin())
            .args(args)
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
        let all = combined(&output);
        assert!(all.contains("--agent"), "{args:?}: {all}");
        assert!(all.contains("--dry-run"), "{args:?}: {all}");
        assert!(
            !all.contains(ENV_KEY_SENTINEL),
            "{args:?}: help must not print the CURIE_API_KEY value: {all}"
        );
    }
}

#[test]
fn unreachable_api_is_exit_three() {
    let output = local(&[], UNREACHABLE_API_URL, true);

    assert_eq!(output.status.code(), Some(3), "{}", describe(&output));
}

#[test]
fn missing_agent_is_exit_one() {
    let server = serve(|req| {
        if route(&req.path) == "/schedules" && req.path.contains("agent=missing-bot") {
            Response::json(404, r#"{"detail":"agent not found"}"#)
        } else {
            Response::json(500, r#"{"detail":"unexpected"}"#)
        }
    });

    let output = local(&["--agent", "missing-bot"], &server.base_url, true);

    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    let recorded = server.recorded();
    let request = schedules_request(&recorded);
    assert_eq!(request.method, "GET");
    assert!(
        request.path.contains("agent=missing-bot"),
        "404 must come from the agent query: {}",
        request.path
    );
    assert!(
        recorded.iter().all(|request| route(&request.path) != "/agents"),
        "an unknown agent must not be resolved through /agents: {:?}",
        recorded.iter().map(|request| request.path.clone()).collect::<Vec<_>>()
    );
}
