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
    record_with_reason(outcome, None)
}

fn record_with_reason(outcome: Option<&str>, reason: Option<&str>) -> Value {
    // Wire shape and allowed outcomes/reasons come from the API producer:
    // apps/api/openapi.json components.schemas.HookFireOut and
    // apps/api/src/curie_api/routers/hook_fire.py::_record.
    json!({
        "id": "22222222-2222-4222-8222-222222222222",
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "agent": "acme-bot",
        "name": "nightly-cleanup",
        "trigger": "cron",
        "slot_utc": "2026-09-26T12:00:00Z",
        "outcome": outcome,
        "reason": reason,
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

fn fire_command(tier: &str, server: &MockServer, flags: &[&str], wait_secs: &str) -> Output {
    let mut args = flags.to_vec();
    args.extend([
        tier,
        "hook",
        "fire",
        "acme-bot",
        "nightly-cleanup",
        "--api-url",
        &server.base_url,
        "--api-key",
        TEST_API_KEY,
        "--wait-secs",
        wait_secs,
    ]);
    run_in(&args, &[("KUBECONFIG", MISSING_KUBECONFIG)])
}

fn assert_terminal_outcome(outcome: &str, reason: Option<&str>, expected_exit: i32) {
    let expected = record_with_reason(Some(outcome), reason);
    let body = expected.to_string();
    let server = serve(move |req| {
        if req.method == "POST" && req.path.ends_with("/fire") {
            return Response::json(200, &body);
        }
        Response::json(500, r#"{"detail":"unexpected"}"#)
    });
    for tier in ["local", "cluster"] {
        let output = fire_command(tier, &server, &["--json"], "5");
        assert_eq!(
            output.status.code(),
            Some(expected_exit),
            "{tier}: {}",
            describe(&output)
        );
        let value = one_object(&output);
        assert_eq!(value, expected, "{tier}: raw API record must be preserved");
        assert_schema(&value);

        for flags in [&[][..], &["-q"][..]] {
            let human = fire_command(tier, &server, flags, "5");
            assert_eq!(
                human.status.code(),
                Some(expected_exit),
                "{tier} {flags:?}: {}",
                describe(&human)
            );
            let text = stdout(&human);
            for field in ["acme-bot", "nightly-cleanup", outcome] {
                assert!(text.contains(field), "{tier} {flags:?}: {text}");
            }
            assert!(
                text.contains(expected["id"].as_str().unwrap()),
                "{tier} {flags:?}: {text}"
            );
            if expected_exit != 0 {
                let stderr = String::from_utf8_lossy(&human.stderr);
                let message = format!("hook nightly-cleanup ran and recorded {outcome}");
                if let Some(reason) = reason {
                    assert!(text.contains(reason), "{tier} {flags:?}: {text}");
                    assert!(
                        stderr.contains(&format!("{message}: {reason}")),
                        "{tier} {flags:?}: {stderr}"
                    );
                } else {
                    assert!(stderr.contains(&message), "{tier} {flags:?}: {stderr}");
                }
            }
        }
    }
    assert!(server.recorded().iter().all(|req| req.method == "POST"));
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
fn ran_fire_succeeds_at_both_tiers() {
    assert_terminal_outcome("ran", None, 0);
}

#[test]
fn failed_fire_preserves_the_record_and_fails_at_both_tiers() {
    assert_terminal_outcome("failed", Some("turn_error"), 1);
}

#[test]
fn blocked_fire_preserves_the_record_and_reason_at_both_tiers() {
    assert_terminal_outcome("blocked", Some("agent_killed"), 1);
}

#[test]
fn skipped_fire_preserves_the_record_and_fails_without_polling_at_both_tiers() {
    assert_terminal_outcome("skipped", Some("run_in_flight"), 1);
}

#[test]
fn reclaimed_fire_preserves_the_record_and_fails_at_both_tiers() {
    assert_terminal_outcome("reclaimed", Some("claim_expired"), 1);
}

#[test]
fn failed_fire_without_a_reason_still_reports_the_outcome_at_both_tiers() {
    assert_terminal_outcome("failed", None, 1);
}

#[test]
fn deferred_fire_waits_until_ran_at_both_tiers() {
    let deferred = record_with_reason(Some("deferred"), Some("live_session")).to_string();
    let ran = record(Some("ran"));
    let body = ran.to_string();
    let server = serve(move |req| {
        if req.method == "POST" && req.path.ends_with("/fire") {
            return Response::json(200, &deferred);
        }
        if req.method == "GET" && req.path.contains("/runs/") {
            return Response::json(200, &body);
        }
        Response::json(500, r#"{"detail":"unexpected"}"#)
    });
    for tier in ["local", "cluster"] {
        let before = server.recorded().len();
        let output = fire_command(tier, &server, &["--json"], "5");
        assert_eq!(
            output.status.code(),
            Some(0),
            "{tier}: {}",
            describe(&output)
        );
        let value = one_object(&output);
        assert_eq!(value, ran, "{tier}: deferred must not be the final result");
        assert_schema(&value);
        assert!(server.recorded()[before..]
            .iter()
            .any(|req| req.method == "GET"));
    }
}

#[test]
fn deferred_fire_times_out_at_both_tiers() {
    let deferred = record_with_reason(Some("deferred"), Some("live_session")).to_string();
    let server = serve(move |req| {
        if (req.method == "POST" && req.path.ends_with("/fire"))
            || (req.method == "GET" && req.path.contains("/runs/"))
        {
            return Response::json(200, &deferred);
        }
        Response::json(500, r#"{"detail":"unexpected"}"#)
    });
    for tier in ["local", "cluster"] {
        let before = server.recorded().len();
        let output = fire_command(tier, &server, &["--json"], "1");
        assert_eq!(
            output.status.code(),
            Some(3),
            "{tier}: {}",
            describe(&output)
        );
        let value = one_object(&output);
        assert!(
            value["error"]
                .as_str()
                .unwrap_or("")
                .contains("did not settle within 1s"),
            "{tier}: {value}"
        );
        assert!(server.recorded()[before..]
            .iter()
            .any(|req| req.method == "GET"));
    }
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
fn hook_fire_paths_percent_encode_agent_segments() {
    // Agent names may hold `#` (which truncates a raw-format path) or `?`
    // (which leaks into the query string); the plain name is the unchanged
    // control (#3731).
    let cases: [(&str, &str); 3] = [
        ("a#b", "a%23b"),
        ("a?x=1", "a%3Fx=1"),
        ("acme-bot", "acme-bot"),
    ];
    for (agent, encoded) in cases {
        let fire_path = format!("/agents/{encoded}/hooks/nightly-cleanup/fire");
        let run_path = format!(
            "/agents/{encoded}/hooks/nightly-cleanup/runs/22222222-2222-4222-8222-222222222222"
        );

        // The fire request and the run poll both hit the encoded path.
        let server = fire_server(true);
        let output = run_in(
            &[
                "--json",
                "local",
                "hook",
                "fire",
                agent,
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
        assert_eq!(
            output.status.code(),
            Some(0),
            "{agent}: {}",
            describe(&output)
        );
        let recorded = server.recorded();
        let posted = recorded
            .iter()
            .find(|request| request.method == "POST")
            .unwrap_or_else(|| panic!("{agent}: no fire request was recorded"));
        assert_eq!(posted.path, fire_path, "{agent}: fire path");
        let polled = recorded
            .iter()
            .find(|request| request.method == "GET")
            .unwrap_or_else(|| panic!("{agent}: no poll request was recorded"));
        assert_eq!(polled.path, run_path, "{agent}: poll path");

        // The dry-run plan shows the same encoded path and makes no request.
        let server = fire_server(true);
        let output = run_in(
            &[
                "--json",
                "local",
                "hook",
                "fire",
                agent,
                "nightly-cleanup",
                "--dry-run",
                "--api-url",
                &server.base_url,
                "--api-key",
                TEST_API_KEY,
            ],
            &[],
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "{agent}: {}",
            describe(&output)
        );
        let value = one_object(&output);
        let expected_line = format!("POST {}{fire_path}", server.base_url);
        let plan = value["plan"].as_array().cloned().unwrap_or_default();
        assert!(plan.contains(&json!(expected_line)), "{agent}: {value}");
        assert!(
            server.recorded().is_empty(),
            "{agent}: dry run called the API"
        );
    }
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
    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
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
