//! Operator surface for a targetless webhook wake (ADR 0274).
//!
//! Drives the built `curie` binary. The platform API is the only stand-in.

use crate::cluster_deploy_no_docker::{
    cron, deploy_cron_fixture, CronDeployFixture, Run, CRON_AGENT_ID,
};
use crate::support;

use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use support::{serve, Request, Response};

const AGENT_ID: &str = CRON_AGENT_ID;
const AGENT_NAME: &str = "acme-bot";
const HOOK: &str = "queue-ready";
const SENTINEL: &str = "wake-key-SENTINEL-0274";
const ROTATED: &str = "wake-key-SENTINEL-0274-rotated";
const API_KEY: &str = "test-key";
const RUN_ID: &str = "22222222-2222-4222-8222-222222222222";
const RECORD_AGENT_ID: &str = "11111111-1111-4111-8111-111111111111";
const RECORD_NAME: &str = "nightly-cleanup";
const MISSING_KUBECONFIG: &str = "/nonexistent/curie-test-kubeconfig";
const DEPLOY_SCHEMA_ID: &str = "https://schemas.curietech.ai/cli/deploy/v1.3.json";

fn wake_route() -> String {
    format!("/hooks/{AGENT_ID}/{HOOK}/wake")
}

fn wake_secret_path() -> String {
    format!("/agents/{AGENT_ID}/hooks/{HOOK}/wake-secret")
}

fn wake_secret_rotate_path() -> String {
    format!("/agents/{AGENT_ID}/hooks/{HOOK}/wake-secret/rotate")
}

fn route_of(path: &str) -> &str {
    path.split('?').next().unwrap_or(path)
}

fn run(args: &[&str], cluster: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_curie"));
    command
        .args(args)
        .stdin(Stdio::null())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_NAMESPACE")
        .env_remove("CURIE_RELEASE")
        .env_remove("FORCE_COLOR")
        .env_remove("CLICOLOR_FORCE")
        .env("NO_COLOR", "1");
    if cluster {
        command.env("KUBECONFIG", MISSING_KUBECONFIG);
    }
    command
        .output()
        .unwrap_or_else(|error| panic!("run curie {}: {error}", args.join(" ")))
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn combined(output: &Output) -> String {
    format!("{}{}", stdout(output), stderr(output))
}

fn describe(output: &Output) -> String {
    format!(
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        stdout(output),
        stderr(output)
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

fn assert_no_sentinel(text: &str) {
    assert!(
        !text.contains(SENTINEL),
        "output must not contain the wake key: {text}"
    );
}

fn assert_exact_keys(value: &Value, expected: &[&str], context: &str) {
    let object = value
        .as_object()
        .unwrap_or_else(|| panic!("{context}: expected an object: {value}"));
    let mut actual: Vec<&str> = object.keys().map(String::as_str).collect();
    actual.sort_unstable();
    let mut expected: Vec<&str> = expected.to_vec();
    expected.sort_unstable();
    assert_eq!(actual, expected, "{context}: {value}");
}

/// `generation N`, `generation: N`, or a JSON `"generation"` field of N.
/// A following digit does not count, so generation 10 and generation 12 are not generation 1.
fn shows_generation(text: &str, generation: u64) -> bool {
    let number = generation.to_string();
    let patterns = [
        format!("generation {number}"),
        format!("generation: {number}"),
        format!("\"generation\": {number}"),
        format!("\"generation\":{number}"),
    ];
    patterns.iter().any(|pattern| {
        text.match_indices(pattern.as_str()).any(|(index, _)| {
            let before_ok = text[..index]
                .chars()
                .next_back()
                .map(|ch| !ch.is_ascii_alphanumeric())
                .unwrap_or(true);
            let after_ok = text[index + pattern.len()..]
                .chars()
                .next()
                .map(|ch| !ch.is_ascii_digit())
                .unwrap_or(true);
            before_ok && after_ok
        })
    })
}

fn deploy_value(run: &Run) -> Value {
    serde_json::from_str(&run.stdout()).unwrap_or_else(|err| {
        panic!(
            "one JSON deploy result is required: {err}\nstdout: {}\nstderr: {}",
            run.stdout(),
            run.stderr()
        )
    })
}

fn assert_deploy_schema_v1_3(value: &Value) {
    let schema: Value = serde_json::from_str(include_str!("../schema/deploy.schema.json"))
        .expect("deploy schema JSON");
    assert_eq!(schema["$id"], json!(DEPLOY_SCHEMA_ID));
    let validator = jsonschema::validator_for(&schema).expect("deploy schema compiles");
    assert!(
        validator.is_valid(value),
        "deploy JSON must validate against {DEPLOY_SCHEMA_ID}: {value}"
    );
}

fn wake_declaration() -> Value {
    json!({
        "type": "webhook",
        "wake": true,
        "name": HOOK,
        "path": "/events",
        "prompt": "Read pending work and record each result."
    })
}

fn wake_false_declaration() -> Value {
    json!({
        "type": "webhook",
        "wake": false,
        "name": "held",
        "path": "/held"
    })
}

fn wake_omitted_declaration() -> Value {
    json!({
        "type": "webhook",
        "name": "inbound",
        "path": "/inbound"
    })
}

fn bound_cron() -> Value {
    cron("nightly", "0 2 * * *", Some("UTC"), Some("C0EXAMPLE1"))
}

fn mixed_wake_manifest() -> Value {
    json!([
        wake_declaration(),
        wake_false_declaration(),
        wake_omitted_declaration(),
        bound_cron()
    ])
}

fn non_wake_manifest() -> Value {
    json!([
        bound_cron(),
        wake_false_declaration(),
        wake_omitted_declaration()
    ])
}

fn deploy(triggers: Value, json_output: bool) -> Run {
    deploy_cron_fixture(CronDeployFixture::new(triggers, json_output), |_| {})
}

fn agent_row() -> Value {
    json!({
        "id": AGENT_ID,
        "name": AGENT_NAME,
        "channels": [{"kind": "slack", "address": "C0EXAMPLE1"}],
        "created_at": "2026-09-29T00:00:00Z",
        "memory": false
    })
}

fn secret_body(generation: u64, secret: &str) -> String {
    json!({
        "agent_id": AGENT_ID,
        "hook": HOOK,
        "generation": generation,
        "secret": secret
    })
    .to_string()
}

fn key_args(tier: &str, rotate: bool, api_url: &str, tail: &[&str]) -> Vec<String> {
    let mut args = vec![tier.to_string(), "hook".to_string(), "wake-key".to_string()];
    if rotate {
        args.push("rotate".to_string());
    }
    args.push(AGENT_NAME.to_string());
    args.push(HOOK.to_string());
    args.extend([
        "--api-url".to_string(),
        api_url.to_string(),
        "--api-key".to_string(),
        API_KEY.to_string(),
    ]);
    args.extend(tail.iter().map(|arg| (*arg).to_string()));
    args
}

fn key_invoke(tier: &str, rotate: bool, api_url: &str, tail: &[&str]) -> Output {
    let args = key_args(tier, rotate, api_url, tail);
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    run(&refs, tier == "cluster")
}

fn assert_secret_get(request: &Request) {
    assert_eq!(request.method, "GET");
    assert_eq!(request.path, wake_secret_path());
    assert!(
        !request.path.contains('?'),
        "wake-secret path must not carry a query: {}",
        request.path
    );
    assert!(
        request.body.is_empty(),
        "wake-secret GET body must be empty: {:?}",
        request.body
    );
}

/// The wake-key stand-in enforces the admin contract before any success body.
/// A wrong key is 401. A query is 400 `wake_query_forbidden`. A body is 422
/// `wake_body_invalid`. A clean request returns None so the caller can answer.
fn refuse_wake_admin_misuse(request: &Request) -> Option<Response> {
    if request.header("X-API-Key") != Some(API_KEY) {
        return Some(Response::json(401, r#"{"detail":"Unauthorized"}"#));
    }
    if request.path.contains('?') {
        return Some(Response::json(400, r#"{"detail":"wake_query_forbidden"}"#));
    }
    if !request.body.is_empty() {
        return Some(Response::json(422, r#"{"detail":"wake_body_invalid"}"#));
    }
    None
}

/// `record(Some("ran"))` from `hook_fire.rs`, with `trigger` and `source` both
/// `webhook`.
fn webhook_record() -> Value {
    json!({
        "id": RUN_ID,
        "agent_id": RECORD_AGENT_ID,
        "agent": AGENT_NAME,
        "name": RECORD_NAME,
        "trigger": "webhook",
        "source": "webhook",
        "slot_utc": "2026-09-26T12:00:00Z",
        "outcome": "ran",
        "reason": null,
        "started_at": "2026-09-26T12:00:00Z",
        "ended_at": "2026-09-26T12:00:01Z"
    })
}

fn record_args(tier: &str, api_url: &str, json_mode: bool) -> Vec<String> {
    let mut args = Vec::new();
    if json_mode {
        args.push("--json".to_string());
    }
    args.extend([
        tier.to_string(),
        "hook".to_string(),
        "record".to_string(),
        AGENT_NAME.to_string(),
        RECORD_NAME.to_string(),
        RUN_ID.to_string(),
        "--api-url".to_string(),
        api_url.to_string(),
        "--api-key".to_string(),
        API_KEY.to_string(),
    ]);
    args
}

fn schedule_args(tier: &str, flag: &str, api_url: &str, dry_run: bool) -> Vec<String> {
    let mut args = vec![
        tier.to_string(),
        "schedules".to_string(),
        "--agent".to_string(),
        AGENT_NAME.to_string(),
        flag.to_string(),
        HOOK.to_string(),
        "--api-url".to_string(),
        api_url.to_string(),
        "--api-key".to_string(),
        API_KEY.to_string(),
    ];
    if dry_run {
        args.push("--dry-run".to_string());
    }
    args
}

fn invoke_owned(args: &[String], cluster: bool) -> Output {
    let refs: Vec<&str> = args.iter().map(String::as_str).collect();
    run(&refs, cluster)
}

#[test]
fn wake_operator_deploy_json_lists_the_one_targetless_route() {
    let run = deploy(mixed_wake_manifest(), true);
    let value = deploy_value(&run);
    let route = wake_route();
    assert_eq!(
        value["wake_triggers"],
        json!([
            {"name": HOOK, "path": route, "mode": "targetless"}
        ])
    );
    assert_ne!(value["wake_triggers"][0]["path"], json!("/events"));
    assert_no_sentinel(&run.stdout());
    assert_no_sentinel(&run.stderr());
    assert_deploy_schema_v1_3(&value);
}

#[test]
fn wake_operator_deploy_human_names_the_targetless_route() {
    let run = deploy(mixed_wake_manifest(), false);
    let text = run.stdout();
    let route = wake_route();
    assert!(
        text.contains(&route),
        "human stdout must include {route}: {text}"
    );
    assert!(
        text.contains("targetless"),
        "human stdout must include targetless: {text}"
    );
    assert_no_sentinel(&text);
    assert_no_sentinel(&run.stderr());
}

#[test]
fn wake_operator_deploy_without_wake_webhooks_lists_nothing() {
    let run = deploy(non_wake_manifest(), true);
    let value = deploy_value(&run);
    assert_eq!(value["wake_triggers"], json!([]));
    assert_no_sentinel(&run.stdout());
    assert_no_sentinel(&run.stderr());
    assert_deploy_schema_v1_3(&value);
}

#[test]
fn wake_operator_key_read_is_stable_on_local_and_cluster() {
    let agents = json!([agent_row()]).to_string();
    let body = secret_body(1, SENTINEL);
    let path = wake_secret_path();
    for tier in ["local", "cluster"] {
        let agents = agents.clone();
        let body = body.clone();
        let path = path.clone();
        let server = serve(move |request| {
            if let Some(refusal) = refuse_wake_admin_misuse(&request) {
                return refusal;
            }
            let route = route_of(&request.path);
            if request.method == "GET" && route == "/agents" {
                return Response::json(200, &agents);
            }
            if request.method == "GET" && route == path {
                return Response::json(200, &body);
            }
            Response::json(500, r#"{"detail":"unexpected"}"#)
        });
        let first = key_invoke(tier, false, &server.base_url, &["--json"]);
        let second = key_invoke(tier, false, &server.base_url, &["--json"]);
        assert_eq!(first.status.code(), Some(0), "{tier}: {}", describe(&first));
        assert_eq!(
            second.status.code(),
            Some(0),
            "{tier}: {}",
            describe(&second)
        );
        let first_json = one_object(&first);
        let second_json = one_object(&second);
        assert_eq!(first_json, second_json, "{tier}");
        assert_eq!(first_json["generation"], json!(1), "{tier}: {first_json}");
        assert_eq!(
            first_json["secret"],
            json!(SENTINEL),
            "{tier}: {first_json}"
        );
        assert_exact_keys(
            &first_json,
            &["agent_id", "hook", "generation", "secret"],
            tier,
        );
        assert_eq!(
            first_json["agent_id"],
            json!(AGENT_ID),
            "{tier}: {first_json}"
        );
        assert_eq!(first_json["hook"], json!(HOOK), "{tier}: {first_json}");
        let reads: Vec<Request> = server
            .recorded()
            .into_iter()
            .filter(|request| request.path.contains("wake-secret"))
            .collect();
        assert_eq!(reads.len(), 2, "{tier}: {reads:?}");
        for request in &reads {
            assert_secret_get(request);
        }
        let human = key_invoke(tier, false, &server.base_url, &[]);
        assert_eq!(human.status.code(), Some(0), "{tier}: {}", describe(&human));
        let human_stdout = stdout(&human);
        assert!(
            human_stdout.contains(SENTINEL),
            "{tier}: human read must show the wake key: {human_stdout}"
        );
        assert!(
            shows_generation(&human_stdout, 1),
            "{tier}: human read must show generation 1: {human_stdout}"
        );
    }
}

#[test]
fn wake_operator_key_rotate_hides_the_secret_on_local_and_cluster() {
    for tier in ["local", "cluster"] {
        let rotated = Arc::new(AtomicBool::new(false));
        let agents = json!([agent_row()]).to_string();
        let read_path = wake_secret_path();
        let rotate_path = wake_secret_rotate_path();
        let server = serve(move |request| {
            if let Some(refusal) = refuse_wake_admin_misuse(&request) {
                return refusal;
            }
            let route = route_of(&request.path);
            if request.method == "GET" && route == "/agents" {
                return Response::json(200, &agents);
            }
            if request.method == "GET" && route == read_path {
                let (generation, secret) = if rotated.load(Ordering::SeqCst) {
                    (2, ROTATED)
                } else {
                    (1, SENTINEL)
                };
                return Response::json(200, &secret_body(generation, secret));
            }
            if request.method == "POST" && route == rotate_path {
                rotated.store(true, Ordering::SeqCst);
                return Response::json(
                    200,
                    &json!({
                        "agent_id": AGENT_ID,
                        "hook": HOOK,
                        "generation": 2,
                        "secret": SENTINEL
                    })
                    .to_string(),
                );
            }
            Response::json(500, r#"{"detail":"unexpected"}"#)
        });

        let before = server.recorded().len();
        let rotated_output = key_invoke(tier, true, &server.base_url, &["--json"]);
        assert_eq!(
            rotated_output.status.code(),
            Some(0),
            "{tier}: {}",
            describe(&rotated_output)
        );
        assert_no_sentinel(&stdout(&rotated_output));
        assert_no_sentinel(&stderr(&rotated_output));
        let rotated_json = one_object(&rotated_output);
        assert_eq!(
            rotated_json["generation"],
            json!(2),
            "{tier}: {rotated_json}"
        );
        assert!(
            rotated_json.get("secret").is_none(),
            "{tier}: rotate JSON must not include secret: {rotated_json}"
        );
        assert_exact_keys(&rotated_json, &["agent_id", "hook", "generation"], tier);
        assert_eq!(
            rotated_json["agent_id"],
            json!(AGENT_ID),
            "{tier}: {rotated_json}"
        );
        assert_eq!(rotated_json["hook"], json!(HOOK), "{tier}: {rotated_json}");
        let posts: Vec<Request> = server
            .recorded()
            .into_iter()
            .skip(before)
            .filter(|request| request.method == "POST")
            .collect();
        assert_eq!(posts.len(), 1, "{tier}: {posts:?}");
        assert_eq!(posts[0].path, wake_secret_rotate_path());
        assert!(
            !posts[0].path.contains('?'),
            "{tier}: rotate path must not carry a query: {}",
            posts[0].path
        );
        assert!(
            posts[0].body.is_empty(),
            "{tier}: rotate body must be empty: {:?}",
            posts[0].body
        );

        let human_rotate = key_invoke(tier, true, &server.base_url, &[]);
        assert_eq!(
            human_rotate.status.code(),
            Some(0),
            "{tier}: {}",
            describe(&human_rotate)
        );
        let human_rotate_stdout = stdout(&human_rotate);
        assert!(
            shows_generation(&human_rotate_stdout, 2),
            "{tier}: human rotate must show generation 2: {human_rotate_stdout}"
        );
        assert_no_sentinel(&human_rotate_stdout);

        let read = key_invoke(tier, false, &server.base_url, &["--json"]);
        assert_eq!(read.status.code(), Some(0), "{tier}: {}", describe(&read));
        let read_json = one_object(&read);
        assert_eq!(read_json["generation"], json!(2), "{tier}: {read_json}");
        assert_eq!(read_json["secret"], json!(ROTATED), "{tier}: {read_json}");
        assert_ne!(read_json["secret"], json!(SENTINEL));
    }
}

#[test]
fn wake_operator_key_dry_run_makes_no_request() {
    let server = serve(|_| Response::json(500, r#"{"detail":"dry run touched the API"}"#));
    for tier in ["local", "cluster"] {
        for rotate in [false, true] {
            let output = key_invoke(tier, rotate, &server.base_url, &["--dry-run"]);
            assert_eq!(
                output.status.code(),
                Some(0),
                "{tier} rotate={rotate}: {}",
                describe(&output)
            );
            assert_no_sentinel(&combined(&output));
            let text = combined(&output);
            assert!(
                text.contains("redacted"),
                "{tier} rotate={rotate}: dry run must say redacted: {text}"
            );
            // Dry-run makes no request, so it can only name the identifier the
            // operator typed. The live call resolves that name to the agent id.
            let read_path = format!("/agents/{AGENT_NAME}/hooks/{HOOK}/wake-secret");
            let rotate_path = format!("/agents/{AGENT_NAME}/hooks/{HOOK}/wake-secret/rotate");
            if rotate {
                assert!(
                    text.contains(&rotate_path),
                    "{tier}: rotate dry run must include {rotate_path}: {text}"
                );
            } else {
                assert!(
                    text.contains(&read_path),
                    "{tier}: read dry run must include {read_path}: {text}"
                );
                assert!(
                    !text.contains("/wake-secret/rotate"),
                    "{tier}: read dry run must not include /wake-secret/rotate: {text}"
                );
            }
        }
    }
    assert!(
        server.recorded().is_empty(),
        "dry run must not call the API: {:?}",
        server.recorded()
    );
}

#[test]
fn wake_operator_runs_record_prints_webhook_source_and_trigger() {
    let expected = webhook_record();
    let body = expected.to_string();
    let run_path = format!("/agents/{AGENT_NAME}/hooks/{RECORD_NAME}/runs/{RUN_ID}");
    for tier in ["local", "cluster"] {
        let body = body.clone();
        let run_path = run_path.clone();
        let server = serve(move |request| {
            if request.method == "GET" && request.path == run_path {
                Response::json(200, &body)
            } else {
                Response::json(500, r#"{"detail":"unexpected"}"#)
            }
        });
        let json_args = record_args(tier, &server.base_url, true);
        let json_output = invoke_owned(&json_args, tier == "cluster");
        assert_eq!(
            json_output.status.code(),
            Some(0),
            "{tier}: {}",
            describe(&json_output)
        );
        let value = one_object(&json_output);
        assert_eq!(value["trigger"], json!("webhook"), "{tier}: {value}");
        assert_eq!(value["source"], json!("webhook"), "{tier}: {value}");
        assert_eq!(value, expected, "{tier}");

        let human_args = record_args(tier, &server.base_url, false);
        let human = invoke_owned(&human_args, tier == "cluster");
        assert_eq!(human.status.code(), Some(0), "{tier}: {}", describe(&human));
        let text = stdout(&human);
        assert!(
            text.contains("source webhook"),
            "{tier}: human stdout must contain `source webhook`: {text}"
        );
        assert!(
            text.contains("trigger webhook"),
            "{tier}: human stdout must contain `trigger webhook`: {text}"
        );
    }
}

#[test]
fn wake_operator_runs_pause_and_resume_keep_the_hook_name() {
    let server = serve(
        |request| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/schedules/acme-bot/queue-ready/pause") => Response::json(
                200,
                r#"{"agent":"acme-bot","name":"queue-ready","paused":true}"#,
            ),
            ("POST", "/schedules/acme-bot/queue-ready/resume") => Response::json(
                200,
                r#"{"agent":"acme-bot","name":"queue-ready","paused":false}"#,
            ),
            _ => Response::json(500, r#"{"detail":"unexpected"}"#),
        },
    );
    let mut expected = Vec::new();
    for tier in ["local", "cluster"] {
        for flag in ["--pause", "--resume"] {
            let args = schedule_args(tier, flag, &server.base_url, false);
            let output = invoke_owned(&args, tier == "cluster");
            assert_eq!(
                output.status.code(),
                Some(0),
                "{tier} {flag}: {}",
                describe(&output)
            );
            let action = if flag == "--pause" { "pause" } else { "resume" };
            expected.push(format!("POST /schedules/{AGENT_NAME}/{HOOK}/{action}"));
        }
    }
    let recorded: Vec<String> = server
        .recorded()
        .iter()
        .map(|request| format!("{} {}", request.method, request.path))
        .collect();
    assert_eq!(recorded, expected);
    for path in &recorded {
        assert!(
            path.contains(&format!("/{HOOK}/")),
            "the hook name must stay {HOOK}: {path}"
        );
    }

    let dry = serve(|_| Response::json(500, r#"{"detail":"dry run touched the API"}"#));
    for tier in ["local", "cluster"] {
        for flag in ["--pause", "--resume"] {
            let args = schedule_args(tier, flag, &dry.base_url, true);
            let output = invoke_owned(&args, tier == "cluster");
            assert_eq!(
                output.status.code(),
                Some(0),
                "{tier} {flag} dry-run: {}",
                describe(&output)
            );
            assert!(
                combined(&output).contains(HOOK),
                "{tier} {flag}: dry-run must print {HOOK}: {}",
                describe(&output)
            );
        }
    }
    assert!(
        dry.recorded().is_empty(),
        "schedule dry-run must not call the API: {:?}",
        dry.recorded()
    );
}

fn flags_before_rotate(tier: &str, api_url: &str, dry_run: bool) -> Vec<String> {
    let mut args = vec![tier.to_string(), "hook".to_string(), "wake-key".to_string()];
    if dry_run {
        args.push("--dry-run".to_string());
    }
    args.extend([
        "--api-url".to_string(),
        api_url.to_string(),
        "--api-key".to_string(),
        API_KEY.to_string(),
        "rotate".to_string(),
        AGENT_NAME.to_string(),
        HOOK.to_string(),
    ]);
    args
}

#[test]
fn wake_operator_key_dry_run_before_rotate_makes_no_request() {
    let server = serve(|_| Response::json(500, r#"{"detail":"dry run touched the API"}"#));
    for tier in ["local", "cluster"] {
        let args = flags_before_rotate(tier, &server.base_url, true);
        let output = invoke_owned(&args, tier == "cluster");
        assert_eq!(
            output.status.code(),
            Some(0),
            "{tier}: {}",
            describe(&output)
        );
        let text = combined(&output);
        assert!(
            text.contains("redacted"),
            "{tier}: dry run before rotate must say redacted: {text}"
        );
        let rotate_path = format!("/agents/{AGENT_NAME}/hooks/{HOOK}/wake-secret/rotate");
        assert!(
            text.contains(&rotate_path),
            "{tier}: dry run before rotate must include {rotate_path}: {text}"
        );
        assert_no_sentinel(&stdout(&output));
        assert_no_sentinel(&stderr(&output));
    }
    assert!(
        server.recorded().is_empty(),
        "dry run before rotate must not call the API: {:?}",
        server.recorded()
    );
    for tier in ["local", "cluster"] {
        let help = run(&[tier, "hook", "wake-key", "--help"], tier == "cluster");
        assert_eq!(help.status.code(), Some(0), "{tier}: {}", describe(&help));
        let text = combined(&help);
        assert!(
            !text.contains("[AGENT] [NAME] <COMMAND>"),
            "{tier}: usage must not require a command beside optional names: {text}"
        );
        assert!(
            text.contains(&format!(
                "{tier} hook wake-key rotate [OPTIONS] <AGENT> <NAME>"
            )),
            "{tier}: usage must show rotate with its own names: {text}"
        );
    }
}

#[test]
fn wake_operator_key_outer_api_url_is_honored() {
    for tier in ["local", "cluster"] {
        let agents = json!([agent_row()]).to_string();
        let rotate_path = wake_secret_rotate_path();
        let server = serve(move |request| {
            if let Some(refusal) = refuse_wake_admin_misuse(&request) {
                return refusal;
            }
            let route = route_of(&request.path);
            if request.method == "GET" && route == "/agents" {
                return Response::json(200, &agents);
            }
            if request.method == "POST" && route == rotate_path {
                return Response::json(
                    200,
                    &json!({
                        "agent_id": AGENT_ID,
                        "hook": HOOK,
                        "generation": 2
                    })
                    .to_string(),
                );
            }
            Response::json(500, r#"{"detail":"unexpected"}"#)
        });
        let args = flags_before_rotate(tier, &server.base_url, false);
        let output = invoke_owned(&args, tier == "cluster");
        assert_eq!(
            output.status.code(),
            Some(0),
            "{tier}: {}",
            describe(&output)
        );
        let posts: Vec<Request> = server
            .recorded()
            .into_iter()
            .filter(|request| request.method == "POST")
            .collect();
        assert_eq!(posts.len(), 1, "{tier}: {posts:?}");
        assert_eq!(posts[0].path, wake_secret_rotate_path(), "{tier}");
        assert_eq!(posts[0].header("X-API-Key"), Some(API_KEY), "{tier}");
    }
}

#[test]
fn wake_operator_admin_server_refuses_a_query_or_a_body() {
    let query = Request {
        method: "GET".to_string(),
        path: format!("{path}?unused=", path = wake_secret_path()),
        headers: vec![("X-API-Key".to_string(), API_KEY.to_string())],
        body: Vec::new(),
    };
    let query_refusal = refuse_wake_admin_misuse(&query).expect("a query is refused");
    assert_eq!(query_refusal.status, 400);
    assert_eq!(query_refusal.body, br#"{"detail":"wake_query_forbidden"}"#,);

    let body = Request {
        method: "POST".to_string(),
        path: wake_secret_rotate_path(),
        headers: vec![("X-API-Key".to_string(), API_KEY.to_string())],
        body: b"{}".to_vec(),
    };
    let body_refusal = refuse_wake_admin_misuse(&body).expect("a body is refused");
    assert_eq!(body_refusal.status, 422);
    assert_eq!(body_refusal.body, br#"{"detail":"wake_body_invalid"}"#);

    let missing_key = Request {
        method: "GET".to_string(),
        path: wake_secret_path(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    let key_refusal = refuse_wake_admin_misuse(&missing_key).expect("a missing key is refused");
    assert_eq!(key_refusal.status, 401);

    let clean = Request {
        method: "POST".to_string(),
        path: wake_secret_rotate_path(),
        headers: vec![("X-API-Key".to_string(), API_KEY.to_string())],
        body: Vec::new(),
    };
    assert!(refuse_wake_admin_misuse(&clean).is_none());
}

#[test]
fn wake_operator_key_refusals_name_the_reason_and_hide_the_secret() {
    // status, detail, exit, phrases that must appear, phrases that must not.
    let cases: &[(u16, &str, i32, &[&str], &[&str])] = &[
        (
            401,
            "Unauthorized",
            1,
            &["verify --api-key or CURIE_API_KEY"],
            &[],
        ),
        (
            404,
            "wake_not_declared",
            1,
            &["wake_not_declared", "wake: true", "in-force bundle"],
            &[],
        ),
        (
            409,
            "wake_source_not_ordinary",
            1,
            &[
                "wake_source_not_ordinary",
                "source policy",
                "unresolved source history",
            ],
            &[],
        ),
        (
            503,
            "authority_unavailable",
            3,
            &["authority_unavailable"],
            &["wake_generation_exhausted"],
        ),
        (
            503,
            "wake_generation_exhausted",
            1,
            &["wake_generation_exhausted", "was not rotated"],
            &["authority_unavailable"],
        ),
        (
            400,
            "wake_query_forbidden",
            2,
            &["wake_query_forbidden"],
            &[],
        ),
        (422, "wake_body_invalid", 2, &["wake_body_invalid"], &[]),
    ];
    for (status, detail, exit_code, needles, absent) in cases {
        let agents = json!([agent_row()]).to_string();
        let path = wake_secret_path();
        let body = json!({"detail": detail, "secret": SENTINEL}).to_string();
        let server = serve(move |request| {
            if let Some(refusal) = refuse_wake_admin_misuse(&request) {
                return refusal;
            }
            let route = route_of(&request.path);
            if request.method == "GET" && route == "/agents" {
                return Response::json(200, &agents);
            }
            if request.method == "GET" && route == path {
                return Response::json(*status, &body);
            }
            Response::json(500, r#"{"detail":"unexpected"}"#)
        });
        let output = key_invoke("local", false, &server.base_url, &[]);
        assert_eq!(
            output.status.code(),
            Some(*exit_code),
            "{detail}: {}",
            describe(&output)
        );
        let text = combined(&output);
        for needle in *needles {
            assert!(
                text.contains(needle),
                "{detail}: message must contain {needle}: {text}"
            );
        }
        for phrase in *absent {
            assert!(
                !text.contains(phrase),
                "{detail}: message must not contain {phrase}: {text}"
            );
        }
        assert_no_sentinel(&stdout(&output));
        assert_no_sentinel(&stderr(&output));
    }
}

#[test]
fn wake_operator_skill_refuses_read_and_rotate() {
    let commands: &[&[&str]] = &[
        &["skill", "hook", "wake-key", HOOK],
        &["skill", "hook", "wake-key", "rotate", HOOK],
    ];
    for args in commands {
        let output = run(args, false);
        assert_eq!(
            output.status.code(),
            Some(4),
            "skill wake-key must be unsupported (exit 4), not clap usage (exit 2): {}",
            describe(&output)
        );
        let text = combined(&output);
        assert!(
            text.contains("the skill tier has no platform API for a durable wake key"),
            "{args:?}: {text}"
        );
        assert!(
            text.contains("curie local hook wake-key"),
            "{args:?}: {text}"
        );
        assert!(
            text.contains("curie cluster hook wake-key"),
            "{args:?}: {text}"
        );
    }
}
