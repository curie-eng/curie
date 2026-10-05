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
    json!({
        "id": "22222222-2222-4222-8222-222222222222",
        "agent_id": "11111111-1111-4111-8111-111111111111",
        "agent": "acme-bot",
        "name": "nightly-cleanup",
        "trigger": "cron",
        "source": "manual",
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
fn blocked_fire_json_and_human_output_carry_the_reason() {
    let body = record_with_reason(Some("blocked"), Some("agent_killed")).to_string();
    let server = serve(move |req| {
        let path = req.path.split('?').next().unwrap_or("");
        if req.method == "POST" && path.ends_with("/fire") {
            return Response::json(200, &body);
        }
        Response::json(500, r#"{"detail":"unexpected"}"#)
    });
    let json_output = run_in(
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
    assert_eq!(
        json_output.status.code(),
        Some(0),
        "{}",
        describe(&json_output)
    );
    let value = one_object(&json_output);
    assert_eq!(value["outcome"], json!("blocked"));
    assert_eq!(value["reason"], json!("agent_killed"));
    assert_schema(&value);

    let human = run_in(
        &[
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
    assert_eq!(human.status.code(), Some(0), "{}", describe(&human));
    let text = stdout(&human);
    let outcome_at = text.find("blocked").expect(&text);
    let reason_at = text.find("agent_killed").expect(&text);
    assert!(reason_at > outcome_at, "{text}");
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
        let alternative = if verb == "record" { "record" } else { "fire" };
        assert!(
            all.contains(&format!("local hook {alternative}"))
                || all.contains(&format!("cluster hook {alternative}")),
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

#[test]
fn local_and_cluster_record_report_every_outcome_without_polling_or_judgment() {
    for tier in ["local", "cluster"] {
        for outcome in [
            None,
            Some("ran"),
            Some("blocked"),
            Some("skipped"),
            Some("failed"),
            Some("deferred"),
            Some("reclaimed"),
        ] {
            let expected = record(outcome);
            let body = expected.to_string();
            let server = serve(move |req| {
                if req.method == "GET"
                    && req.path.ends_with(
                        "/hooks/nightly-cleanup/runs/22222222-2222-4222-8222-222222222222",
                    )
                {
                    Response::json(200, &body)
                } else {
                    Response::json(500, r#"{"detail":"unexpected request"}"#)
                }
            });
            let output = run_in(
                &[
                    "--json",
                    tier,
                    "hook",
                    "record",
                    "acme-bot",
                    "nightly-cleanup",
                    "22222222-2222-4222-8222-222222222222",
                    "--api-url",
                    &server.base_url,
                    "--api-key",
                    TEST_API_KEY,
                ],
                &[("KUBECONFIG", MISSING_KUBECONFIG)],
            );
            assert_eq!(
                output.status.code(),
                Some(0),
                "{tier} {outcome:?}: {}",
                describe(&output)
            );
            let value = one_object(&output);
            assert_eq!(value, expected, "{tier} {outcome:?}");
            assert_schema(&value);
            let requests = server.recorded();
            assert_eq!(requests.len(), 1, "{tier} {outcome:?}: {requests:?}");
            assert_eq!(requests[0].method, "GET");
            assert_eq!(requests[0].header("x-api-key"), Some(TEST_API_KEY));
        }
    }
}

#[test]
fn hook_record_human_output_preserves_the_fire_record_line() {
    let body = record_with_reason(Some("blocked"), Some("agent_killed")).to_string();
    let server = serve(move |_| Response::json(200, &body));
    let output = run_in(
        &[
            "local",
            "hook",
            "record",
            "acme-bot",
            "nightly-cleanup",
            "22222222-2222-4222-8222-222222222222",
            "--api-url",
            &server.base_url,
            "--api-key",
            TEST_API_KEY,
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    assert_eq!(
        stdout(&output).trim(),
        "acme-bot nightly-cleanup 2026-09-26T12:00:00Z blocked agent_killed 22222222-2222-4222-8222-222222222222",
    );
}

#[test]
fn hook_record_encodes_every_segment_and_dry_run_uses_the_same_get() {
    let body = record(None).to_string();
    for tier in ["local", "cluster"] {
        let expected_path = "/agents/a%23b/hooks/nightly%3Fx=1/runs/run%2Fone";
        let expected_body = body.clone();
        let server = serve(move |req| {
            if req.method == "GET" && req.path == expected_path {
                Response::json(200, &expected_body)
            } else {
                Response::json(404, r#"{"detail":"wrong path"}"#)
            }
        });
        let output = run_in(
            &[
                "--json",
                tier,
                "hook",
                "record",
                "a#b",
                "nightly?x=1",
                "run/one",
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
            "{tier}: {}",
            describe(&output)
        );
        let requests = server.recorded();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, expected_path);

        let server = serve(|_| Response::json(500, r#"{"detail":"dry run touched API"}"#));
        let output = run_in(
            &[
                "--json",
                tier,
                "hook",
                "record",
                "a#b",
                "nightly?x=1",
                "run/one",
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
            "{tier}: {}",
            describe(&output)
        );
        let value = one_object(&output);
        assert_eq!(value["dry_run"], json!(true));
        assert_eq!(
            value["plan"],
            json!([format!("GET {}{expected_path}", server.base_url)])
        );
        assert_schema(&value);
        assert!(server.recorded().is_empty());
    }
}

#[test]
fn hook_record_missing_agent_hook_or_run_is_an_error() {
    for detail in ["agent not found", "hook not found", "hook run not found"] {
        let server = serve(move |_| Response::json(404, &json!({"detail": detail}).to_string()));
        let output = run_in(
            &[
                "--json",
                "local",
                "hook",
                "record",
                "acme-bot",
                "nightly-cleanup",
                "22222222-2222-4222-8222-222222222222",
                "--api-url",
                &server.base_url,
                "--api-key",
                TEST_API_KEY,
            ],
            &[],
        );
        assert_eq!(
            output.status.code(),
            Some(1),
            "{detail}: {}",
            describe(&output)
        );
        assert!(describe(&output).contains(detail), "{}", describe(&output));
        assert_eq!(server.recorded().len(), 1);
    }
}

#[test]
fn hook_record_rejects_a_missing_source_instead_of_defaulting_it() {
    let mut body = record(Some("ran"));
    body.as_object_mut().unwrap().remove("source");
    let encoded = body.to_string();
    let server = serve(move |_| Response::json(200, &encoded));
    let output = run_in(
        &[
            "--json",
            "local",
            "hook",
            "record",
            "acme-bot",
            "nightly-cleanup",
            "22222222-2222-4222-8222-222222222222",
            "--api-url",
            &server.base_url,
            "--api-key",
            TEST_API_KEY,
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    assert!(
        describe(&output).contains("decoding hook run"),
        "{}",
        describe(&output)
    );
}

#[test]
fn hook_fire_timeout_names_the_record_command_at_the_same_tier() {
    for tier in ["local", "cluster"] {
        let body = record(None).to_string();
        let server = serve(move |_| Response::json(200, &body));
        let output = run_in(
            &[
                "--json",
                tier,
                "hook",
                "fire",
                "acme-bot",
                "nightly-cleanup",
                "--wait-secs",
                "0",
                "--api-url",
                &server.base_url,
                "--api-key",
                TEST_API_KEY,
            ],
            &[],
        );
        assert_eq!(
            output.status.code(),
            Some(3),
            "{tier}: {}",
            describe(&output)
        );
        let remedy = format!(
            "curie {tier} hook record acme-bot nightly-cleanup 22222222-2222-4222-8222-222222222222",
        );
        assert!(
            describe(&output).contains(&remedy),
            "{tier}: {}",
            describe(&output)
        );
        assert_eq!(server.recorded().len(), 1);
    }
}
