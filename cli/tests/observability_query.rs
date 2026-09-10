//! Binary-level contract for the observability query verbs (#866).
//!
//! The platform API is the only mocked boundary: each test drives the built
//! `curie` process through clap, the real reqwest client, the centralized
//! success/error emitters, and a wire-level HTTP peer. No test reaches
//! Langfuse, Prometheus, Kubernetes, or another observability backend.

mod support;

use std::collections::BTreeSet;
use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use serde_json::{json, Value};
use support::{serve, MockServer, Response};

const TEST_API_KEY: &str = "curie-observability-test-key";
const TRACE_ID: &str = "trace-observability-866";
const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const START: &str = "2026-08-22T00:00:00Z";
const END: &str = "2026-08-23T00:00:00Z";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

/// Query connection flags belong to the query leaf: the bare observability
/// command stays a URL printer, while every query can target a disposable API.
fn query(tier: &str, leaf: &[&str], server: &MockServer, json: bool, quiet: bool) -> Output {
    query_url(tier, leaf, &server.base_url, json, quiet, None)
}

fn query_url(
    tier: &str,
    leaf: &[&str],
    api_url: &str,
    json: bool,
    quiet: bool,
    path: Option<&Path>,
) -> Output {
    let mut args = vec![tier.to_string(), "observability".to_string()];
    args.extend(leaf.iter().map(|part| (*part).to_string()));
    args.extend([
        "--api-url".to_string(),
        api_url.to_string(),
        "--api-key".to_string(),
        TEST_API_KEY.to_string(),
    ]);
    if quiet {
        args.push("--quiet".to_string());
    }
    if json {
        args.push("--json".to_string());
    }

    let mut command = Command::new(bin());
    command
        .args(&args)
        .stdin(Stdio::null())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY");
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command
        .output()
        .unwrap_or_else(|error| panic!("run curie {}: {error}", args.join(" ")))
}

fn one_stdout_object(output: &Output) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.trim().is_empty(), "stdout must not be empty");
    let mut values =
        serde_json::Deserializer::from_slice(&output.stdout).into_iter::<serde_json::Value>();
    let value = values
        .next()
        .unwrap_or_else(|| panic!("stdout must contain one JSON object: {stdout}"))
        .unwrap_or_else(|error| panic!("stdout must start with JSON: {error}\n{stdout}"));
    assert!(value.is_object(), "stdout must contain an object: {stdout}");
    assert!(
        values.next().is_none(),
        "stdout must contain exactly one JSON value: {stdout}"
    );
    value
}

fn assert_success(output: &Output, invocation: &str) -> Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(0),
        "{invocation} must exit 0\nstdout: {stdout}\nstderr: {stderr}"
    );
    one_stdout_object(output)
}

fn assert_schema(name: &str, value: &Value) {
    let path = format!("{}/schema/{name}", env!("CARGO_MANIFEST_DIR"));
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("committed query schema {path} must exist: {error}"));
    let schema: Value = serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("query schema {path} must be JSON: {error}"));
    let validator = jsonschema::validator_for(&schema)
        .unwrap_or_else(|error| panic!("query schema {path} must compile: {error}"));
    assert!(
        validator.is_valid(value),
        "{value} must validate against {name}"
    );
}

fn flags(help: &str) -> BTreeSet<String> {
    help.split_whitespace()
        .filter(|token| token.starts_with("--"))
        .map(|token| {
            token
                .trim_end_matches(|c: char| !c.is_ascii_alphanumeric() && c != '-')
                .to_string()
        })
        .collect()
}

fn leaf_help(tier: &str, leaf: &str) -> String {
    let output = Command::new(bin())
        .args([tier, "observability", leaf, "--help"])
        .stdin(Stdio::null())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap_or_else(|error| panic!("run {tier} observability {leaf} --help: {error}"));
    let text = String::from_utf8_lossy(&output.stdout).into_owned()
        + &String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{tier} observability {leaf} must parse\n{text}"
    );
    text
}

fn assert_only_error_fix(value: &Value) {
    let object = value.as_object().expect("error payload must be an object");
    assert_eq!(
        object.keys().cloned().collect::<BTreeSet<_>>(),
        BTreeSet::from(["error".to_string(), "fix".to_string()]),
        "failure payload must contain exactly error and fix: {value}"
    );
    assert!(
        value["error"].as_str().is_some_and(|text| !text.is_empty()),
        "error must be a non-empty string: {value}"
    );
    assert!(
        value["fix"].as_str().is_some_and(|text| !text.is_empty()),
        "fix must be an actionable non-empty string: {value}"
    );
}

fn trace_tree() -> Value {
    json!({
        "trace": {
            "id": TRACE_ID,
            "name": format!("curie-run:agent-{AGENT_ID}-thread-1"),
            "timestamp": "2026-08-22T12:34:56Z",
            "sessionId": "session-observability-866",
            "metadata": {
                "session_id": "session-observability-866",
                "terminal_outcome": "completed",
                "nested": {"kept": true}
            },
            "output": {"status": "done", "reply": "complete"}
        },
        "tree": [{
            "id": "span-root",
            "type": "SPAN",
            "name": "agent.run",
            "startTime": "2026-08-22T12:34:56Z",
            "model": null,
            "usageDetails": null,
            "children": [{
                "id": "generation-child",
                "type": "GENERATION",
                "name": "model.call",
                "startTime": "2026-08-22T12:34:57Z",
                "model": "example-model",
                "usageDetails": {"input": 11, "output": 7},
                "children": []
            }]
        }],
        "sandbox_id": "sandbox-observability-866",
        "approval_decision": "approved"
    })
}

fn metrics_summary() -> Value {
    json!({
        "start": START,
        "end": END,
        "runs": 7,
        "latency_p95_ms": 125.5,
        "tokens": 987,
        "cost_usd": 0.0,
        "cost_known": false,
        "error_rate": 0.125
    })
}

fn metric_series() -> Value {
    json!({
        "metric": "runs",
        "granularity": "day",
        "start": START,
        "end": END,
        "points": [
            {"ts": "2026-08-22T00:00:00Z", "value": 3.0},
            {"ts": "2026-08-23T00:00:00Z", "value": 4.0}
        ]
    })
}

#[test]
fn local_and_cluster_expose_the_same_query_grammar_and_defaults() {
    for leaf in ["runs", "run", "metrics"] {
        let local = leaf_help("local", leaf);
        let cluster = leaf_help("cluster", leaf);
        let local_flags = flags(&local);
        let mut cluster_flags = flags(&cluster);

        // Cluster adds only release discovery. Every query/filter/connection
        // flag is otherwise one shared contract across the sibling tiers.
        cluster_flags.remove("--namespace");
        cluster_flags.remove("--release");
        assert_eq!(
            local_flags, cluster_flags,
            "local/cluster observability {leaf} flags drifted\nlocal: {local}\ncluster: {cluster}"
        );
        assert!(
            !local.contains("--yes") && !cluster.contains("--yes"),
            "read queries are non-interactive and must not expose confirmation: {leaf}"
        );
        assert!(
            !local.contains("--open") && !cluster.contains("--open"),
            "query leaves never open a browser: {leaf}"
        );
        assert!(
            !local.contains("--latest") && !cluster.contains("--latest"),
            "#1664 owns trace-id discoverability; #866 must not add --latest: {leaf}"
        );
    }

    let runs = leaf_help("local", "runs");
    assert!(runs.contains("--limit"), "runs exposes a bound: {runs}");
    assert!(
        runs.contains("[default: 20]"),
        "runs must default to 20: {runs}"
    );
    assert!(runs.contains("--agent-id"), "runs exposes agent_id: {runs}");

    let metrics = leaf_help("local", "metrics");
    for flag in [
        "--metric",
        "--granularity",
        "--start",
        "--end",
        "--environment",
        "--agent",
    ] {
        assert!(
            metrics.contains(flag),
            "metrics is missing {flag}: {metrics}"
        );
    }
    for value in ["runs", "latency_p95_ms", "tokens", "cost_usd", "error_rate"] {
        assert!(
            metrics.contains(value),
            "metrics is missing {value}: {metrics}"
        );
    }
    for value in ["hour", "day", "week"] {
        assert!(
            metrics.contains(value),
            "metrics is missing {value}: {metrics}"
        );
    }

    for tier in ["local", "cluster"] {
        let secret = "must-not-appear-in-help";
        let output = Command::new(bin())
            .args([tier, "observability", "runs", "--help"])
            .env("CURIE_API_KEY", secret)
            .output()
            .expect("render observability runs help");
        let help = String::from_utf8_lossy(&output.stdout).into_owned()
            + &String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "help must succeed: {help}");
        assert!(
            !help.contains(secret),
            "observability help must never render CURIE_API_KEY values: {help}"
        );
    }
}

#[test]
fn runs_default_to_twenty_preserve_order_and_truncate_a_skewed_server() {
    let rows: Vec<Value> = (0..23)
        .map(|index| {
            json!({
                "id": format!("trace-{index:02}"),
                "name": format!("curie-run:agent-{AGENT_ID}-thread-{index}"),
                "timestamp": format!("2026-08-22T12:{:02}:00Z", 59 - index),
                "sessionId": format!("session-{index:02}"),
                "metadata": {"terminal_outcome": "completed", "ordinal": index}
            })
        })
        .collect();
    let body = serde_json::to_string(&rows).unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
            Response::json(200, &body)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    let mut values = Vec::new();
    for tier in ["local", "cluster"] {
        let output = query(tier, &["runs"], &server, true, false);
        let value = assert_success(&output, &format!("{tier} observability runs"));
        assert_schema("observability-runs.schema.json", &value);
        assert_eq!(value["limit"], 20, "default bound is explicit: {value}");
        assert_eq!(value["count"], 20, "count reflects returned rows: {value}");
        let runs = value["runs"].as_array().expect("runs is an array");
        assert_eq!(
            runs.len(),
            20,
            "the client defensively truncates over-return"
        );
        assert_eq!(runs, &rows[..20], "newest-first API order is preserved");
        values.push(value);
    }
    assert_eq!(
        values[0], values[1],
        "local/cluster list payloads must match"
    );

    let recorded = server.recorded();
    assert_eq!(recorded.len(), 2, "one API read per tier");
    for request in recorded {
        assert_eq!(request.method, "GET");
        assert!(
            request.path.starts_with("/langfuse/traces?"),
            "list must use the Curie API proxy: {}",
            request.path
        );
        assert!(request.path.contains("limit=20"), "path: {}", request.path);
        assert_eq!(request.header("x-api-key"), Some(TEST_API_KEY));
    }
}

#[test]
fn runs_accept_limit_boundaries_and_reject_zero_and_101_before_http() {
    let server = serve(|request| {
        if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
            Response::json(200, "[]")
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    for (tier, limit) in [("local", "1"), ("cluster", "100")] {
        let output = query(tier, &["runs", "--limit", limit], &server, true, false);
        let value = assert_success(&output, &format!("{tier} runs --limit {limit}"));
        assert_eq!(
            value,
            json!({"limit": limit.parse::<u64>().unwrap(), "count": 0, "runs": []})
        );
    }
    let accepted = server.recorded();
    assert_eq!(accepted.len(), 2);
    assert!(accepted[0].path.contains("limit=1"));
    assert!(accepted[1].path.contains("limit=100"));

    for (tier, limit) in [("local", "0"), ("cluster", "101")] {
        let output = query(tier, &["runs", "--limit", limit], &server, true, false);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{tier} --limit {limit} must be a usage error\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert_eq!(
        server.recorded().len(),
        2,
        "invalid limits must be refused before making an HTTP request"
    );
}

#[test]
fn runs_agent_filter_is_forwarded_without_dropping_typed_rows() {
    let row = json!({
        "id": TRACE_ID,
        "name": null,
        "timestamp": "2026-08-22T12:34:56Z",
        "sessionId": "session-observability-866",
        "metadata": {"terminal_outcome": "completed"}
    });
    let body = serde_json::to_string(&vec![row.clone()]).unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
            Response::json(200, &body)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    let output = query(
        "local",
        &["runs", "--limit", "1", "--agent-id", AGENT_ID],
        &server,
        true,
        false,
    );
    let value = assert_success(&output, "local observability runs --agent-id");
    assert_eq!(value, json!({"limit": 1, "count": 1, "runs": [row]}));
    let request = &server.recorded()[0];
    assert!(request.path.contains("limit=1"), "path: {}", request.path);
    assert!(
        request.path.contains(&format!("agent_id={AGENT_ID}")),
        "the CLI must keep the API route's agent_id spelling: {}",
        request.path
    );
}

#[test]
fn runs_rejects_an_api_row_without_usable_trace_identity() {
    let server = serve(|request| {
        if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
            Response::json(200, r#"[{"metadata":{"terminal_outcome":"completed"}}]"#)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    let output = query("local", &["runs", "--limit", "1"], &server, true, false);
    assert_eq!(output.status.code(), Some(1));
    let value = one_stdout_object(&output);
    assert_only_error_fix(&value);
    let error = value["error"].as_str().unwrap();
    assert!(
        error.contains("decoding") && error.contains("observability runs"),
        "an untyped server row must fail instead of becoming an unusable success: {value}"
    );
}

#[test]
fn run_returns_the_complete_api_trace_tree_at_both_tiers() {
    let expected = trace_tree();
    let body = serde_json::to_string(&expected).unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path == format!("/langfuse/traces/{TRACE_ID}") {
            Response::json(200, &body)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    for tier in ["local", "cluster"] {
        let output = query(tier, &["run", TRACE_ID], &server, true, false);
        let value = assert_success(&output, &format!("{tier} observability run"));
        assert_eq!(
            value, expected,
            "the CLI must not project away trace/session/outcome, correlation fields, or tree nodes"
        );
        assert!(
            !String::from_utf8_lossy(&output.stdout).contains(TEST_API_KEY),
            "the API credential is transport-only and must never enter the result"
        );
        assert_schema("observability-run.schema.json", &value);
    }

    let recorded = server.recorded();
    assert_eq!(recorded.len(), 2);
    assert!(recorded.iter().all(|request| {
        request.method == "GET"
            && request.path == format!("/langfuse/traces/{TRACE_ID}")
            && request.header("x-api-key") == Some(TEST_API_KEY)
    }));
}

#[test]
fn metrics_preserve_complete_summary_and_series_dtos_and_filters() {
    let summary = metrics_summary();
    let series = metric_series();
    let summary_body = serde_json::to_string(&summary).unwrap();
    let series_body = serde_json::to_string(&series).unwrap();
    let agents = serde_json::to_string(&json!([
        {"id": AGENT_ID, "name": "acme-bot", "channels": [{"kind": "slack", "address": "C0EXAMPLE1"}], "memory": false}
    ]))
    .unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path == "/agents" {
            Response::json(200, &agents)
        } else if request.method == "GET"
            && request.path.starts_with("/observability/metrics/summary?")
        {
            Response::json(200, &summary_body)
        } else if request.method == "GET"
            && request.path.starts_with("/observability/metrics/series?")
        {
            Response::json(200, &series_body)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    for tier in ["local", "cluster"] {
        let output = query(
            tier,
            &[
                "metrics",
                "--start",
                START,
                "--end",
                END,
                "--environment",
                "dev",
                "--agent",
                "acme-bot",
            ],
            &server,
            true,
            false,
        );
        let value = assert_success(&output, &format!("{tier} observability metrics summary"));
        assert_eq!(
            value, summary,
            "summary DTO fields, including cost_known, survive"
        );
        assert_schema("observability-metrics.schema.json", &value);

        let output = query(
            tier,
            &[
                "metrics",
                "--metric",
                "runs",
                "--start",
                START,
                "--end",
                END,
                "--environment",
                "prod",
                "--agent",
                "acme-bot",
            ],
            &server,
            true,
            false,
        );
        let value = assert_success(&output, &format!("{tier} observability metrics series"));
        assert_eq!(value, series, "series DTO and every point survive");
        assert_eq!(value["granularity"], "day", "series defaults to day");
        assert_schema("observability-metrics.schema.json", &value);
    }

    let recorded = server.recorded();
    assert_eq!(
        recorded.len(),
        8,
        "agent lookup plus summary and series per tier"
    );
    for request in &recorded {
        assert_eq!(request.header("x-api-key"), Some(TEST_API_KEY));
    }
    for request in recorded
        .iter()
        .filter(|request| request.path.starts_with("/observability/metrics/"))
    {
        assert!(request.path.contains("start=2026-08-22T00%3A00%3A00Z"));
        assert!(request.path.contains("end=2026-08-23T00%3A00%3A00Z"));
        // The name resolves to the agent's id and travels as the API's
        // documented trace-name token (apps/api/src/curie_api/metrics.py::
        // agent_trace_filter), never as the human-readable name.
        assert!(
            request.path.contains(&format!("agent=agent-{AGENT_ID}")),
            "path: {}",
            request.path
        );
        assert!(!request.path.contains("agent=acme-bot"));
    }
    for request in recorded
        .iter()
        .filter(|request| request.path.starts_with("/observability/metrics/series?"))
    {
        assert!(
            request.path.contains("metric=runs"),
            "path: {}",
            request.path
        );
        assert!(
            request.path.contains("granularity=day"),
            "omitting --granularity must send the documented day default: {}",
            request.path
        );
        assert!(request.path.contains("environment=prod"));
    }
}

#[test]
fn metric_enums_and_series_only_granularity_fail_as_usage_before_http() {
    let server = serve(|_| Response::json(500, r#"{"detail":"must not be called"}"#));
    for (tier, args) in [
        ("local", vec!["metrics", "--metric", "not-a-metric"]),
        (
            "cluster",
            vec!["metrics", "--metric", "runs", "--granularity", "minute"],
        ),
        ("local", vec!["metrics", "--granularity", "hour"]),
    ] {
        let output = query(tier, &args, &server, true, false);
        assert_eq!(
            output.status.code(),
            Some(2),
            "invalid metrics argv is usage: {tier} {args:?}\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(
        server.recorded().is_empty(),
        "invalid metric enum and granularity-without-series fail before HTTP"
    );
}

#[test]
fn cluster_explicit_api_url_never_receives_an_auto_discovered_key() {
    let server = serve(|_| Response::json(500, r#"{"detail":"must not be called"}"#));
    let tools = tempfile::tempdir().expect("side-effect tool directory");
    let marker = tools.path().join("kubectl-called");
    let kubectl = tools.path().join("kubectl");
    fs::write(
        &kubectl,
        format!(
            "#!/bin/sh\nprintf called > '{}'\nexit 97\n",
            marker.display()
        ),
    )
    .expect("write kubectl marker");
    let mut permissions = fs::metadata(&kubectl).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&kubectl, permissions).unwrap();

    let output = Command::new(bin())
        .args([
            "--json",
            "cluster",
            "observability",
            "runs",
            "--api-url",
            &server.base_url,
        ])
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_API_URL")
        .env("PATH", tools.path())
        .output()
        .expect("run cluster query with an unpaired URL override");
    assert_eq!(output.status.code(), Some(2));
    let value = one_stdout_object(&output);
    assert_only_error_fix(&value);
    assert!(
        value["fix"].as_str().unwrap().contains("--api-key"),
        "the recovery must require an explicitly paired key: {value}"
    );
    assert!(
        server.recorded().is_empty(),
        "the arbitrary URL is never dialed"
    );
    assert!(!marker.exists(), "no release key discovery is attempted");
}

#[test]
fn runs_guidance_stays_on_stderr_and_quiet_suppresses_only_guidance() {
    let row = json!({"id": TRACE_ID, "name": "acme-run", "timestamp": START});
    let body = serde_json::to_string(&vec![row.clone()]).unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
            Response::json(200, &body)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    let output = query("local", &["runs", "--limit", "1"], &server, true, false);
    let value = assert_success(&output, "local observability runs guidance");
    assert_eq!(value["runs"][0], row);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("observability run"),
        "stderr should guide the human to the detail verb: {stderr}"
    );
    assert!(
        !stdout.contains("observability run"),
        "guidance must never contaminate JSON stdout: {stdout}"
    );

    let quiet = query("local", &["runs", "--limit", "1"], &server, true, true);
    assert_success(&quiet, "quiet local observability runs");
    assert!(
        quiet.stderr.is_empty(),
        "--quiet suppresses guidance, not the JSON payload: {}",
        String::from_utf8_lossy(&quiet.stderr)
    );
}

#[test]
fn unknown_trace_and_unavailable_api_are_distinct_semantic_failures() {
    let unknown = serve(|request| {
        if request.method == "GET" && request.path == "/langfuse/traces/trace-unknown-866" {
            Response::json(404, r#"{"detail":"trace has no observations yet"}"#)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });
    let unknown_output = query(
        "local",
        &["run", "trace-unknown-866"],
        &unknown,
        true,
        false,
    );
    assert_eq!(
        unknown_output.status.code(),
        Some(1),
        "a well-formed unknown trace is failure, not usage/transient"
    );
    let unknown_json = one_stdout_object(&unknown_output);
    assert_only_error_fix(&unknown_json);
    assert!(
        unknown_json["error"]
            .as_str()
            .unwrap()
            .contains("trace-unknown-866"),
        "the error identifies the requested trace: {unknown_json}"
    );
    assert!(
        unknown_json["fix"]
            .as_str()
            .unwrap()
            .contains("observability runs"),
        "the fix points to bounded trace discovery: {unknown_json}"
    );

    // Reserve an ephemeral address and close it immediately. Both siblings
    // drive the same real reqwest connection refusal; only the actionable
    // recovery differs because local owns a start command while cluster owns a
    // discovered or explicitly supplied endpoint.
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve closed test port");
    let unavailable_url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let mut failures = Vec::new();
    for (tier, expected_fix) in [
        (
            "local",
            "start the local API with `curie local up`, or pass a reachable --api-url",
        ),
        (
            "cluster",
            "verify the cluster API endpoint, or pass a reachable --api-url",
        ),
    ] {
        let output = query_url(
            tier,
            &["runs", "--limit", "1"],
            &unavailable_url,
            true,
            false,
            None,
        );
        assert_eq!(
            output.status.code(),
            Some(3),
            "{tier} unavailable API must remain transient\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let value = one_stdout_object(&output);
        assert_only_error_fix(&value);
        assert_eq!(
            value["fix"], expected_fix,
            "{tier} must retain its exact tier-specific recovery action: {value}"
        );
        let error = value["error"].as_str().unwrap();
        assert!(
            error.starts_with("the Curie API is unavailable for this query:")
                && error.contains("GET observability:"),
            "the error keeps the shared API-operation context: {value}"
        );
        assert!(
            error.contains(&format!("{unavailable_url}/langfuse/traces")),
            "the error identifies the endpoint that could not be reached: {value}"
        );
        failures.push(value);
    }

    assert_eq!(
        failures[0]["error"], failures[1]["error"],
        "the transport failure itself is tier-neutral"
    );
    assert_ne!(
        failures[0]["fix"], failures[1]["fix"],
        "local and cluster recovery must not collapse to generic retry guidance"
    );
    assert_ne!(
        unknown_json, failures[0],
        "unknown data and unavailable infrastructure must be distinguishable"
    );
}

#[test]
fn retryable_api_statuses_are_transient_not_unknown_data() {
    for status in [500, 502, 503, 504] {
        let server = serve(move |request| {
            if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
                Response::json(
                    status,
                    r#"{"detail":"observability dependency unavailable"}"#,
                )
            } else {
                Response::json(418, r#"{"detail":"unexpected test request"}"#)
            }
        });
        let output = query("local", &["runs", "--limit", "1"], &server, true, false);
        assert_eq!(
            output.status.code(),
            Some(3),
            "reachable API status {status} is a retryable availability failure"
        );
        let value = one_stdout_object(&output);
        assert_only_error_fix(&value);
        assert!(value["error"]
            .as_str()
            .unwrap()
            .contains(&status.to_string()));
    }
}

#[test]
fn observability_api_errors_keep_specific_recovery_guidance() {
    let auth = serve(|request| {
        if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
            Response::json(401, r#"{"detail":"invalid API key"}"#)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });
    let auth_output = query("local", &["runs", "--limit", "1"], &auth, true, false);
    assert_eq!(auth_output.status.code(), Some(1));
    let auth_value = one_stdout_object(&auth_output);
    assert!(
        auth_value["fix"].as_str().unwrap().contains("--api-key"),
        "authentication failures need credential recovery, not DTO advice: {auth_value}"
    );

    let input = serve(|request| {
        if request.method == "GET" && request.path.starts_with("/observability/metrics/summary") {
            Response::json(422, r#"{"detail":"invalid start timestamp"}"#)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });
    let input_output = query(
        "local",
        &["metrics", "--start", "not-a-timestamp"],
        &input,
        true,
        false,
    );
    assert_eq!(input_output.status.code(), Some(2));
    let input_value = one_stdout_object(&input_output);
    assert!(
        input_value["fix"]
            .as_str()
            .unwrap()
            .contains("query filters"),
        "API input failures need flag recovery: {input_value}"
    );

    let points = (0..1001)
        .map(|index| json!({"ts": format!("point-{index}"), "value": 1.0}))
        .collect::<Vec<_>>();
    let oversized_body = serde_json::to_string(&json!({
        "metric": "runs",
        "granularity": "hour",
        "start": START,
        "end": END,
        "points": points,
    }))
    .unwrap();
    let oversized = serve(move |request| {
        if request.method == "GET" && request.path.starts_with("/observability/metrics/series?") {
            Response::json(200, &oversized_body)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });
    let oversized_output = query(
        "local",
        &["metrics", "--metric", "runs", "--granularity", "hour"],
        &oversized,
        true,
        false,
    );
    assert_eq!(oversized_output.status.code(), Some(1));
    let oversized_value = one_stdout_object(&oversized_output);
    assert!(
        oversized_value["fix"]
            .as_str()
            .unwrap()
            .contains("narrow --start/--end"),
        "the typed bound's recovery must survive classification: {oversized_value}"
    );
}

#[test]
fn runs_refuse_an_empty_or_whitespace_agent_id_before_http() {
    // #1948 AC1: a present-but-empty --agent-id is not an absent one. Forwarded
    // verbatim it reaches FastAPI as agent_id=, which the API treats as no
    // filter, and the "scoped" export silently returns every agent's traces.
    let server = serve(|_| Response::json(500, r#"{"detail":"must not be called"}"#));
    for tier in ["local", "cluster"] {
        for raw in ["", "   "] {
            let output = query(tier, &["runs", "--agent-id", raw], &server, true, false);
            assert_eq!(
                output.status.code(),
                Some(2),
                "{tier} --agent-id {raw:?} must be a usage refusal\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            let value = one_stdout_object(&output);
            assert_only_error_fix(&value);
            let error = value["error"].as_str().unwrap();
            assert!(
                error.contains("--agent-id"),
                "the error must name the offending flag: {value}"
            );
            let fix = value["fix"].as_str().unwrap();
            assert!(
                fix.contains("agent id"),
                "the fix must name what a usable --agent-id value is: {value}"
            );
        }
    }
    assert!(
        server.recorded().is_empty(),
        "an empty scope must be refused before any HTTP request is made"
    );
}

#[test]
fn metrics_agent_name_resolves_to_the_documented_trace_filter_token() {
    // #1948 AC2: production trace names carry agent-<agent_id> (the runner
    // names every trace curie-run:agent-<agent_id>-thread-<ts>), so the API's
    // `agent` filter is a trace-name contains token agent-<agent_id>
    // (apps/api/src/curie_api/metrics.py::agent_trace_filter). Forwarding the
    // human-readable name matched nothing and read as zeroes. The mock below
    // returns nonzero data ONLY for the resolved token, so the filter value
    // the CLI actually sends decides the result: on pre-fix code this test
    // reads the zero payloads and fails.
    let agents = serde_json::to_string(&json!([
        {"id": AGENT_ID, "name": "acme-bot", "channels": [{"kind": "slack", "address": "C0EXAMPLE1"}], "memory": false},
        {"id": "22222222-2222-2222-2222-222222222222", "name": "other-agent", "channels": [{"kind": "slack", "address": "C0EXAMPLE2"}], "memory": false}
    ]))
    .unwrap();
    let token = format!("agent-{AGENT_ID}");
    let summary_hit = json!({
        "start": START, "end": END, "runs": 7, "latency_p95_ms": 125.5,
        "tokens": 987, "cost_usd": 0.5, "cost_known": true, "error_rate": 0.125
    });
    let summary_zero = json!({
        "start": START, "end": END, "runs": 0, "latency_p95_ms": 0.0,
        "tokens": 0, "cost_usd": 0.0, "cost_known": true, "error_rate": 0.0
    });
    let series_hit = json!({
        "metric": "runs", "granularity": "day", "start": START, "end": END,
        "points": [{"ts": START, "value": 3.0}]
    });
    let series_zero = json!({
        "metric": "runs", "granularity": "day", "start": START, "end": END,
        "points": [{"ts": START, "value": 0.0}]
    });
    let summary_hit_body = serde_json::to_string(&summary_hit).unwrap();
    let summary_zero_body = serde_json::to_string(&summary_zero).unwrap();
    let series_hit_body = serde_json::to_string(&series_hit).unwrap();
    let series_zero_body = serde_json::to_string(&series_zero).unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path == "/agents" {
            Response::json(200, &agents)
        } else if request.method == "GET"
            && request.path.starts_with("/observability/metrics/summary?")
        {
            if request.path.contains(&format!("agent={token}")) {
                Response::json(200, &summary_hit_body)
            } else {
                Response::json(200, &summary_zero_body)
            }
        } else if request.method == "GET"
            && request.path.starts_with("/observability/metrics/series?")
        {
            if request.path.contains(&format!("agent={token}")) {
                Response::json(200, &series_hit_body)
            } else {
                Response::json(200, &series_zero_body)
            }
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    for tier in ["local", "cluster"] {
        // The documented human name resolves to the agent's id and is sent as
        // the API's trace-name token, not as the raw name.
        let output = query(
            tier,
            &["metrics", "--agent", "acme-bot"],
            &server,
            true,
            false,
        );
        let value = assert_success(&output, &format!("{tier} metrics --agent by name"));
        assert_schema("observability-metrics.schema.json", &value);
        assert_eq!(
            value["runs"], 7,
            "a named agent's metrics must come back nonzero: {value}"
        );

        let output = query(
            tier,
            &[
                "metrics", "--metric", "runs", "--agent", "acme-bot", "--start", START, "--end",
                END,
            ],
            &server,
            true,
            false,
        );
        let value = assert_success(&output, &format!("{tier} metrics series --agent by name"));
        assert_eq!(
            value["points"][0]["value"], 3.0,
            "a named agent's series must come back nonzero: {value}"
        );

        // Passing the agent id directly keeps working and normalizes to the
        // same documented token.
        let output = query(
            tier,
            &["metrics", "--agent", AGENT_ID],
            &server,
            true,
            false,
        );
        let value = assert_success(&output, &format!("{tier} metrics --agent by id"));
        assert_eq!(
            value["runs"], 7,
            "an id-valued --agent resolves too: {value}"
        );
    }

    let recorded = server.recorded();
    let lookups = recorded
        .iter()
        .filter(|request| request.path == "/agents")
        .count();
    assert_eq!(
        lookups, 6,
        "every --agent query resolves the name through one agent-list lookup"
    );
    for request in recorded
        .iter()
        .filter(|request| request.path.starts_with("/observability/metrics/"))
    {
        assert!(
            request.path.contains(&format!("agent=agent-{AGENT_ID}")),
            "the outgoing filter must be the documented trace-name token: {}",
            request.path
        );
        assert!(
            !request.path.contains("agent=acme-bot"),
            "the human name must never be forwarded as the filter value: {}",
            request.path
        );
    }
}

#[test]
fn metrics_refuse_an_unknown_agent_and_an_empty_agent_value() {
    // #1948 AC2, refusal half: --agent must resolve to a real platform agent,
    // and a present-but-empty value is refused without a lookup, mirroring the
    // runs --agent-id refusal so both flags answer the same falsey class.
    let agents = serde_json::to_string(&json!([
        {"id": "22222222-2222-2222-2222-222222222222", "name": "other-agent", "channels": [{"kind": "slack", "address": "C0EXAMPLE2"}], "memory": false}
    ]))
    .unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path == "/agents" {
            Response::json(200, &agents)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    let output = query(
        "local",
        &["metrics", "--agent", "acme-bot"],
        &server,
        true,
        false,
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "an unresolvable agent name must be a usage refusal\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value = one_stdout_object(&output);
    assert_only_error_fix(&value);
    assert!(
        value["error"].as_str().unwrap().contains("acme-bot"),
        "the error must name the unresolvable value: {value}"
    );
    let fix = value["fix"].as_str().unwrap();
    assert!(
        fix.contains("name") && fix.contains("agent id"),
        "the fix must name what --agent accepts: {value}"
    );

    for raw in ["", "   "] {
        let output = query("local", &["metrics", "--agent", raw], &server, true, false);
        assert_eq!(
            output.status.code(),
            Some(2),
            "--agent {raw:?} must be a usage refusal\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_only_error_fix(&one_stdout_object(&output));
    }
    let lookups = server
        .recorded()
        .iter()
        .filter(|request| request.path == "/agents")
        .count();
    assert_eq!(
        lookups, 1,
        "only the unknown-name case may consult the agent list; the empty refusals precede it"
    );
    assert!(
        !server
            .recorded()
            .iter()
            .any(|request| request.path.starts_with("/observability/metrics/")),
        "no metrics query is dispatched for a refused --agent"
    );
}

#[test]
fn series_span_is_validated_against_the_point_cap_before_dispatch() {
    // #1948 AC3: the 1,000-point series cap must be enforced before the
    // upstream request, because the backend has already paid for the query by
    // the time the response is buffered. The boundary control proves the guard
    // is the point cap itself: exactly 1,000 hour buckets dispatch, 1,001 do
    // not.
    let series = json!({
        "metric": "runs", "granularity": "hour", "start": START, "end": END,
        "points": [{"ts": START, "value": 1.0}]
    });
    let body = serde_json::to_string(&series).unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path.starts_with("/observability/metrics/series?") {
            Response::json(200, &body)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    // The issue's repro shape: an hourly 1970 to 2026 window.
    let output = query(
        "local",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "an over-cap span is a runtime failure, the same class the post-hoc bound used\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value = one_stdout_object(&output);
    assert_only_error_fix(&value);
    assert!(
        value["fix"]
            .as_str()
            .unwrap()
            .contains("narrow --start/--end"),
        "the pre-dispatch refusal keeps the typed bound's recovery: {value}"
    );
    assert!(
        !value["error"].as_str().unwrap().contains("500"),
        "the refusal is the CLI's own guard, not the server's answer: {value}"
    );

    // Boundary: exactly 1,000 hour buckets is at the cap, not above it.
    // 1970-01-01T00:00:00Z + 1000h = 1970-02-11T16:00:00Z.
    let output = query(
        "local",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00Z",
            "--end",
            "1970-02-11T16:00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_success(&output, "at-cap hourly span dispatches");

    // One bucket over the boundary is refused again.
    let output = query(
        "local",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00Z",
            "--end",
            "1970-02-11T17:00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(output.status.code(), Some(1));
    assert_only_error_fix(&one_stdout_object(&output));

    // A --start with no --end is unbounded server-side (the API defaults end to
    // now), so the CLI validates that shape against now as well.
    let output = query(
        "local",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(output.status.code(), Some(1));
    assert_only_error_fix(&one_stdout_object(&output));

    // A fractional-second end past the cap boundary opens the boundary's
    // bucket, so it is refused on the consumer path too (round 3).
    let output = query(
        "local",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00Z",
            "--end",
            "1970-02-11T16:00:00.500Z",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(output.status.code(), Some(1));
    assert_only_error_fix(&one_stdout_object(&output));

    // A fractional pre-epoch start floors down at nanosecond precision, so
    // this cap-length window touches 1,001 buckets (round 5).
    let output = query(
        "local",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1969-12-31T23:59:59.500Z",
            "--end",
            "1970-02-11T16:00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(output.status.code(), Some(1));
    assert_only_error_fix(&one_stdout_object(&output));

    // The API's fromisoformat accepts date-only values the strict RFC 3339
    // parser rejects, so the guard parses them too instead of forwarding the
    // span unprevalidated (scope review). The same applies to an
    // offset-bearing minute-truncated timestamp.
    let output = query(
        "local",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00Z",
            "--end",
            "2026-01-01T00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(output.status.code(), Some(1));
    assert_only_error_fix(&one_stdout_object(&output));

    // Compact ±HHMM is API-accepted (datetime.fromisoformat) and must not
    // skip the pre-dispatch cap (code review).
    let output = query(
        "local",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00+0000",
            "--end",
            "2026-01-01T00:00:00+0000",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(output.status.code(), Some(1));
    assert_only_error_fix(&one_stdout_object(&output));

    // Sibling path: the same query function serves cluster. At-cap still
    // dispatches; over-cap still does not.
    let output = query(
        "cluster",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00Z",
            "--end",
            "2026-01-01T00:00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(
        output.status.code(),
        Some(1),
        "cluster over-cap span must refuse before dispatch\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_only_error_fix(&one_stdout_object(&output));
    let output = query(
        "cluster",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00Z",
            "--end",
            "1970-02-11T16:00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_success(&output, "cluster at-cap hourly span dispatches");
    let output = query(
        "cluster",
        &[
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
            "1970-01-01T00:00:00Z",
            "--end",
            "1970-02-11T17:00:00Z",
        ],
        &server,
        true,
        false,
    );
    assert_eq!(output.status.code(), Some(1));
    assert_only_error_fix(&one_stdout_object(&output));

    let series_calls = server
        .recorded()
        .iter()
        .filter(|request| request.path.starts_with("/observability/metrics/series?"))
        .count();
    assert_eq!(
        series_calls, 2,
        "only the at-cap span may reach the API at each tier; every over-cap refusal precedes dispatch"
    );
}

#[test]
fn cluster_query_parent_namespace_and_release_drive_discovery_for_every_leaf() {
    const NAMESPACE: &str = "observability-parent-ns";
    const RELEASE: &str = "observability-parent-release";
    // The chart names every resource `{{ include "curie.fullname" . }}-<component>`,
    // and `curie.fullname` is the release name only when it already contains
    // "curie", else `<release>-curie`. This release does not contain it, so the
    // rendered Service names carry the suffix (#1533). The CLI discovers this
    // name by label selector rather than computing it.
    const FULLNAME: &str = "observability-parent-release-curie";
    const DISCOVERED_SECRET: &str = "observability-parent-release-secrets";

    let run = trace_tree();
    let run_body = serde_json::to_string(&run).unwrap();
    let summary = metrics_summary();
    let summary_body = serde_json::to_string(&summary).unwrap();
    let server = serve(move |request| {
        if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
            Response::json(200, "[]")
        } else if request.method == "GET" && request.path == format!("/langfuse/traces/{TRACE_ID}")
        {
            Response::json(200, &run_body)
        } else if request.method == "GET"
            && request.path.starts_with("/observability/metrics/summary")
        {
            Response::json(200, &summary_body)
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });

    let node_port = server
        .base_url
        .rsplit(':')
        .next()
        .expect("mock API URL has a port");
    let tools = tempfile::tempdir().expect("discovery tool directory");
    let kubectl_log = tools.path().join("kubectl.log");
    let helm_log = tools.path().join("helm.log");
    let proxy = tools.path().join("proxy.py");
    fs::write(
        &proxy,
        r#"import http.server, os, sys, urllib.error, urllib.request

target = "http://127.0.0.1:" + os.environ["CURIE_TEST_OBSERVABILITY_TARGET_PORT"]

class Proxy(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        request = urllib.request.Request(target + self.path)
        if self.headers.get("X-API-Key"):
            request.add_header("X-API-Key", self.headers["X-API-Key"])
        try:
            response = urllib.request.urlopen(request)
        except urllib.error.HTTPError as error:
            response = error
        body = response.read()
        self.send_response(response.status)
        self.send_header("Content-Type", response.headers.get("Content-Type", "application/json"))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass

server = http.server.ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Proxy)
assigned = server.server_address[1]
print(f"Forwarding from 127.0.0.1:{assigned} -> 8000", flush=True)
server.serve_forever()
"#,
    )
    .expect("write API port-forward proxy");
    let kubectl = tools.path().join("kubectl");
    fs::write(
        &kubectl,
        r#"#!/bin/sh
printf '%s\n' "$*" >> "$CURIE_TEST_OBSERVABILITY_KUBECTL_LOG"
case "$*" in
  *"config view --minify"*)
    printf '%s' 'https://127.0.0.1:6443'
    ;;
  *"-n observability-parent-ns get svc -l app.kubernetes.io/instance=observability-parent-release,app.kubernetes.io/component=api"*)
    printf '%s' 'observability-parent-release-curie-api'
    ;;
  *"get svc observability-parent-release-curie-ui -n observability-parent-ns"*)
    printf '{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":%s}]}}' "$CURIE_TEST_OBSERVABILITY_NODE_PORT"
    ;;
  *"-n observability-parent-ns get secret -l app.kubernetes.io/instance=observability-parent-release"*)
    printf '%s\n' 'observability-parent-release-secrets'
    ;;
  *"-n observability-parent-ns get secret observability-parent-release-secrets"*"apiKey"*)
    printf '%s' "$CURIE_TEST_OBSERVABILITY_API_KEY"
    ;;
  *"-n observability-parent-ns port-forward svc/observability-parent-release-curie-api 0:8000"*)
    exec python3 "$CURIE_TEST_OBSERVABILITY_PROXY_SCRIPT" 0
    ;;
  *)
    printf 'unexpected kubectl invocation: %s\n' "$*" >&2
    exit 64
    ;;
esac
"#,
    )
    .expect("write kubectl discovery stub");
    let mut permissions = fs::metadata(&kubectl).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&kubectl, permissions).unwrap();

    let helm = tools.path().join("helm");
    fs::write(
        &helm,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$CURIE_TEST_OBSERVABILITY_HELM_LOG\"\nexit 97\n",
    )
    .expect("write forbidden helm stub");
    let mut permissions = fs::metadata(&helm).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&helm, permissions).unwrap();

    let cases: &[(&[&str], &str)] = &[
        (&["runs", "--limit", "1"], "runs"),
        (&["run", TRACE_ID], "run"),
        (&["metrics"], "metrics"),
    ];
    let tool_path = format!(
        "{}:{}",
        tools.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    for (leaf, label) in cases {
        let mut args = vec![
            "cluster",
            "observability",
            "--namespace",
            NAMESPACE,
            "--release",
            RELEASE,
        ];
        args.extend_from_slice(leaf);
        args.push("--json");
        let output = Command::new(bin())
            .args(&args)
            .stdin(Stdio::null())
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_NAMESPACE")
            .env("PATH", &tool_path)
            .env("CURIE_TEST_OBSERVABILITY_KUBECTL_LOG", &kubectl_log)
            .env("CURIE_TEST_OBSERVABILITY_HELM_LOG", &helm_log)
            .env("CURIE_TEST_OBSERVABILITY_TARGET_PORT", node_port)
            .env("CURIE_TEST_OBSERVABILITY_PROXY_SCRIPT", &proxy)
            .env("CURIE_TEST_OBSERVABILITY_API_KEY", TEST_API_KEY)
            .output()
            .unwrap_or_else(|error| panic!("run cluster observability {label}: {error}"));
        assert_success(
            &output,
            &format!("cluster observability {label} via discovery"),
        );
    }

    let requests = server.recorded();
    assert_eq!(
        requests.len(),
        3,
        "each query leaf reaches the discovered API once"
    );
    assert!(requests.iter().any(|request| {
        request.path.starts_with("/langfuse/traces?") && request.path.contains("limit=1")
    }));
    assert!(requests
        .iter()
        .any(|request| request.path == format!("/langfuse/traces/{TRACE_ID}")));
    assert!(requests
        .iter()
        .any(|request| request.path.starts_with("/observability/metrics/summary")));
    assert!(requests
        .iter()
        .all(|request| request.header("x-api-key") == Some(TEST_API_KEY)));

    let discovery = fs::read_to_string(&kubectl_log).expect("read discovery log");
    assert_eq!(
        discovery
            .lines()
            .filter(|line| line.contains(&format!(
                "-n {NAMESPACE} get svc -l app.kubernetes.io/instance={RELEASE},app.kubernetes.io/component=api"
            )))
            .count(),
        3,
        "every query must resolve the rendered api Service name by label, not by \
         string-building it from the release name: {discovery}"
    );
    assert_eq!(
        discovery
            .lines()
            .filter(|line| line.contains(&format!(
                "-n {NAMESPACE} port-forward svc/{FULLNAME}-api 0:8000"
            )))
            .count(),
        3,
        "every query must self-plumb the parent-selected release API: {discovery}"
    );
    assert_eq!(
        discovery
            .lines()
            .filter(|line| line.contains(&format!(
                "-n {NAMESPACE} get secret -l app.kubernetes.io/instance={RELEASE}"
            )))
            .count(),
        3,
        "every query must discover the parent-selected release Secret: {discovery}"
    );
    assert_eq!(
        discovery
            .lines()
            .filter(|line| line.contains(&format!("-n {NAMESPACE} get secret {DISCOVERED_SECRET}")))
            .count(),
        3,
        "every query must read the key from the discovered Secret: {discovery}"
    );
    assert!(
        !discovery.contains("svc/curie-api")
            && !discovery.contains("app.kubernetes.io/instance=curie ")
            && !discovery.contains("-n curie "),
        "default namespace/release must not leak into explicit parent flags: {discovery}"
    );
    assert!(
        !helm_log.exists(),
        "read-only query discovery must not invoke Helm"
    );
}

#[test]
fn query_defaults_are_inert_and_open_or_latest_are_refused_before_http() {
    let server = serve(|request| {
        if request.method == "GET" && request.path.starts_with("/langfuse/traces?") {
            Response::json(200, "[]")
        } else {
            Response::json(500, r#"{"detail":"unexpected test request"}"#)
        }
    });
    let tools = tempfile::tempdir().expect("tool marker directory");
    let marker = tools.path().join("side-effects.log");
    for tool in ["xdg-open", "open", "kubectl", "helm"] {
        let script = format!(
            "#!/bin/sh\nprintf '%s\\n' '{tool}' >> '{}'\nexit 97\n",
            marker.display()
        );
        let path = tools.path().join(tool);
        fs::write(&path, script).expect("write side-effect sentinel");
        let mut permissions = fs::metadata(&path).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&path, permissions).unwrap();
    }

    for tier in ["local", "cluster"] {
        let output = query_url(
            tier,
            &["runs"],
            &server.base_url,
            true,
            false,
            Some(tools.path()),
        );
        assert_success(&output, &format!("inert {tier} observability runs"));
    }
    assert!(
        !marker.exists(),
        "a default query must not open a browser, run discovery, or prompt"
    );

    let before = server.recorded().len();
    let open = query_url(
        "local",
        &["--open", "runs"],
        &server.base_url,
        true,
        false,
        Some(tools.path()),
    );
    assert_eq!(
        open.status.code(),
        Some(2),
        "--open and a query are incompatible"
    );

    let latest = query_url(
        "cluster",
        &["runs", "--latest"],
        &server.base_url,
        true,
        false,
        Some(tools.path()),
    );
    assert_eq!(
        latest.status.code(),
        Some(2),
        "#1664 owns latest trace discoverability; #866 exposes no --latest"
    );
    assert_eq!(
        server.recorded().len(),
        before,
        "invalid side-effect/discoverability flags fail before HTTP"
    );
    assert!(!marker.exists(), "refused query combinations stay inert");
}

#[test]
fn skill_observability_queries_are_answered_as_unavailable_with_cross_tier_guidance() {
    // ADR-0041: parity means the skill tier answers every understood query
    // verb honestly. These are capability failures (exit 4), not clap usage
    // failures and not fabricated empty platform results.
    for (leaf, args) in [
        ("runs", vec!["runs"]),
        ("run", vec!["run", TRACE_ID]),
        ("metrics", vec!["metrics"]),
    ] {
        let output = Command::new(bin())
            .args(["--json", "skill", "observability"])
            .args(&args)
            .stdin(Stdio::null())
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY")
            .output()
            .unwrap_or_else(|error| panic!("run skill observability {leaf}: {error}"));

        assert_eq!(
            output.status.code(),
            Some(4),
            "skill observability {leaf} is an understood but unavailable capability\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let value = one_stdout_object(&output);
        assert_only_error_fix(&value);

        let error = value["error"].as_str().unwrap().to_ascii_lowercase();
        assert!(
            error.contains("not available at this tier") && error.contains("skill"),
            "the error must identify skill-tier capability absence: {value}"
        );

        let fix = value["fix"].as_str().unwrap().to_ascii_lowercase();
        assert!(
            fix.contains("curie local observability")
                || fix.contains("curie cluster observability")
                || fix.contains("otel"),
            "the fix must point to a platform query tier or skill OTLP setup: {value}"
        );
        assert!(
            !fix.contains("retry"),
            "an unsupported skill-tier query must not masquerade as transient: {value}"
        );
    }
}

/// ADR-0041 parity means the **bare** form (no leaf) is answered at every
/// tier, not just the query leaves covered above: the skill tier answers with
/// the capability refusal (exit 4, `{error, fix}`), while local and cluster
/// answer with their surface/plan payloads (exit 0). Before #1955 the skill
/// tier alone declared its subcommand field non-optional, so clap's
/// `subcommand_required` kicked in and `curie skill observability` with no
/// leaf died as a clap usage error -- exit 2, help on stderr, empty stdout --
/// instead of ever reaching `commands::skill_observability_unavailable()`.
#[test]
fn bare_observability_is_answered_at_every_tier_and_refused_at_skill() {
    // skill: the bare form must reach the exit-4 capability refusal, not
    // clap's subcommand-required usage error.
    let output = Command::new(bin())
        .args(["--json", "skill", "observability"])
        .stdin(Stdio::null())
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_API_KEY")
        .output()
        .unwrap_or_else(|error| panic!("run skill observability: {error}"));

    assert_eq!(
        output.status.code(),
        Some(4),
        "bare skill observability must exit 4, not clap's usage exit 2\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("Usage: curie skill observability"),
        "bare skill observability must not fall back to clap's subcommand-required usage help \
         (the #1955 regression): stderr: {stderr}"
    );

    let value = one_stdout_object(&output);
    assert_only_error_fix(&value);

    let error = value["error"].as_str().unwrap().to_ascii_lowercase();
    assert!(
        error.contains("not available at this tier") && error.contains("skill"),
        "the error must identify skill-tier capability absence: {value}"
    );

    let fix = value["fix"].as_str().unwrap().to_ascii_lowercase();
    assert!(
        fix.contains("curie local observability") || fix.contains("curie cluster observability"),
        "the fix must point to a platform query tier: {value}"
    );
    assert!(
        !fix.contains("retry"),
        "an unsupported skill-tier query must not masquerade as transient: {value}"
    );

    // local: the parity control -- the sibling tier answers the same bare
    // verb with its surface report, not a refusal. `commands::observability`
    // is a pure URL printer, so this needs no running stack.
    let output = Command::new(bin())
        .args(["--json", "local", "observability"])
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|error| panic!("run local observability: {error}"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("Usage: curie local observability"),
        "bare local observability must not fall back to clap usage help: stderr: {stderr}"
    );
    let value = assert_success(&output, "bare local observability");
    assert!(
        value["surfaces"].as_array().is_some(),
        "bare local observability must report its surfaces array: {value}"
    );

    // cluster: same parity control, via --dry-run so no kubectl/helm is
    // shelled out -- the plan payload is returned before
    // `require_on_path("kubectl")` runs.
    let output = Command::new(bin())
        .args(["--json", "cluster", "observability", "--dry-run"])
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|error| panic!("run cluster observability --dry-run: {error}"));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("Usage: curie cluster observability"),
        "bare cluster observability --dry-run must not fall back to clap usage help: stderr: {stderr}"
    );
    let value = assert_success(&output, "bare cluster observability --dry-run");
    assert!(
        value.as_object().is_some_and(|object| !object.is_empty()),
        "bare cluster observability --dry-run must report a non-empty plan payload: {value}"
    );
}
