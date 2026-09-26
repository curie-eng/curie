//! `curie hook fire` at the skill, local, and cluster tiers (#2932).

mod support;

use std::fs;
use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "curie-hook-fire-test-key";
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
    assert!(values.next().is_none(), "{}", describe(output));
    value
}

fn assert_schema(value: &Value) {
    let path = format!(
        "{}/schema/hook-fire.schema.json",
        env!("CARGO_MANIFEST_DIR")
    );
    let raw = fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    let schema: Value = serde_json::from_str(&raw).expect("schema is JSON");
    let validator = jsonschema::validator_for(&schema).expect("schema compiles");
    assert!(validator.is_valid(value), "{value}");
}

fn record(outcome: Option<&str>) -> Value {
    json!({
        "id": "22222222-2222-4222-8222-222222222222",
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "agent": "acme-bot",
        "name": "nightly-cleanup",
        "trigger": "cron",
        "slot_utc": "2026-09-26T12:00:00Z",
        "outcome": outcome,
        "started_at": "2026-09-26T12:00:00Z",
        "ended_at": if outcome.is_some() { json!("2026-09-26T12:00:01Z") } else { Value::Null }
    })
}

fn fire_server(settle: bool) -> MockServer {
    let open = record(None).to_string();
    let closed = record(Some("ran")).to_string();
    let skipped = record(Some("skipped")).to_string();
    serve(move |req| {
        let path = req.path.split('?').next().unwrap_or("");
        if req.method == "POST" && path.ends_with("/fire") {
            if settle {
                return Response::json(200, &open);
            }
            return Response::json(200, &skipped);
        }
        if req.method == "GET" && path.contains("/runs/") {
            return Response::json(200, &closed);
        }
        Response::json(500, r#"{"detail":"unexpected"}"#)
    })
}

#[test]
fn local_fire_waits_until_the_record_settles() {
    let server = fire_server(true);
    let output = run_in(
        &[
            "--json",
            "local",
            "hook",
            "fire",
            "acme-bot",
            "nightly-cleanup",
            "--api-url",
            &server.base_url,
            "--api-key",
            TEST_API_KEY,
            "--wait-secs",
            "5",
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_eq!(value["outcome"], json!("ran"));
    assert_schema(&value);
    let recorded = server.recorded();
    assert!(recorded.iter().any(|req| req.method == "POST"));
    assert!(recorded.iter().any(|req| req.method == "GET"));
}

#[test]
fn skipped_fire_is_printed_without_waiting() {
    let server = fire_server(false);
    let output = run_in(
        &[
            "--json",
            "local",
            "hook",
            "fire",
            "acme-bot",
            "nightly-cleanup",
            "--api-url",
            &server.base_url,
            "--api-key",
            TEST_API_KEY,
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(one_object(&output)["outcome"], json!("skipped"));
    assert!(server.recorded().iter().all(|req| req.method != "GET"));
}

#[test]
fn dry_run_does_not_call_the_api() {
    let server = fire_server(false);
    let output = run_in(
        &[
            "--json",
            "local",
            "hook",
            "fire",
            "acme-bot",
            "nightly-cleanup",
            "--dry-run",
            "--api-url",
            &server.base_url,
            "--api-key",
            TEST_API_KEY,
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = one_object(&output);
    assert_eq!(value["dry_run"], json!(true));
    assert_schema(&value);
    assert!(server.recorded().is_empty());
}

#[test]
fn cluster_fire_uses_the_explicit_api() {
    let server = fire_server(false);
    let output = run_in(
        &[
            "--json",
            "cluster",
            "hook",
            "fire",
            "acme-bot",
            "nightly-cleanup",
            "--api-url",
            &server.base_url,
            "--api-key",
            TEST_API_KEY,
        ],
        &[("KUBECONFIG", MISSING_KUBECONFIG)],
    );
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(one_object(&output)["outcome"], json!("skipped"));
}

#[test]
fn skill_schedule_and_record_are_unavailable() {
    for verb in ["schedule", "record"] {
        let output = run_in(&["skill", "hook", verb], &[]);
        assert_eq!(
            output.status.code(),
            Some(4),
            "{verb}: {}",
            describe(&output)
        );
        let all = format!(
            "{}{}",
            stdout(&output),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            all.contains("local hook fire") || all.contains("cluster hook fire"),
            "{verb}: {all}"
        );
    }
}

#[test]
fn skill_fire_refuses_an_unknown_hook_without_a_runner() {
    let dir = std::env::temp_dir().join("curie-2932-hook-fire");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(dir.join(".claude-plugin")).unwrap();
    fs::write(
        dir.join(".claude-plugin/plugin.json"),
        r#"{"name":"acme-bot","version":"0.1.0","description":"t","triggers":[{"type":"cron","name":"nightly-cleanup","schedule":"0 9 * * *","prompt":"Run the scheduled check."}]}"#,
    )
    .unwrap();
    let output = run_in(
        &[
            "skill",
            "hook",
            "fire",
            "missing",
            "--plugin-dir",
            dir.to_str().unwrap(),
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    let all = format!(
        "{}{}",
        stdout(&output),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(all.contains("missing"), "{all}");
}
