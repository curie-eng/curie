//! Integration coverage for trajectory scoring on the platform eval plane.
//!
//! Local and cluster must both trigger the worker eval job and read its
//! structured matrix cells. Neither tier may infer a trajectory verdict from a
//! Slack reply string.

mod support;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use curie::api::ApiClient;
use support::{serve, Request, Response};

const AGENT_ID: &str = "11111111-1111-1111-1111-111111111111";
const VERSION_ID: &str = "22222222-2222-2222-2222-222222222222";
const SHA: &str = "abc123trajectory";
const CHANNEL: &str = "C0EXAMPLE1";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn output_text(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned() + &String::from_utf8_lossy(&output.stderr)
}

fn matrix_body() -> String {
    serde_json::json!({
        "suite": "trajectory",
        "versions": [SHA],
        "cases": ["ordered", "wrong_order", "missing_spec"],
        "rows": [
            {
                "case_id": "ordered",
                "cells": [
                    {
                        "version": SHA,
                        "status": "pass",
                        "model": "resolved-model",
                        "detail": null,
                        "stream_id": "1-0",
                        "scorer": "trajectory",
                        "case_count": 3,
                    }
                ],
            },
            {
                "case_id": "wrong_order",
                "cells": [
                    {
                        "version": SHA,
                        "status": "fail",
                        "model": "resolved-model",
                        "detail": "mode=in_order expected=['Read', 'Bash'] observed=['Bash', 'Read']",
                        "stream_id": "1-0",
                        "scorer": "trajectory",
                        "case_count": 3,
                    }
                ],
            },
            {
                "case_id": "missing_spec",
                "cells": [
                    {
                        "version": SHA,
                        "status": "fail",
                        "model": "resolved-model",
                        "detail": "no trajectory spec for case 'missing_spec'",
                        "stream_id": "1-0",
                        "scorer": "trajectory",
                        "case_count": 3,
                    }
                ],
            }
        ],
        "models": [null, "resolved-model"],
        "model_summaries": [],
        "model_version_summaries": [],
    })
    .to_string()
}

fn incomplete_matrix_body() -> String {
    let mut body: serde_json::Value =
        serde_json::from_str(&matrix_body()).expect("valid matrix fixture");
    body["cases"]
        .as_array_mut()
        .expect("matrix cases")
        .truncate(2);
    body["rows"]
        .as_array_mut()
        .expect("matrix rows")
        .truncate(2);
    body.to_string()
}

fn platform_server_with_matrix(
    matrix: impl Fn() -> String + Send + Sync + 'static,
) -> support::MockServer {
    serve(
        move |request| match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/agents") => Response::json(
                200,
                &serde_json::json!([{
                    "id": AGENT_ID,
                    "name": "acme-bot",
                    "channels": [{"kind": "slack", "address": CHANNEL}],
                    "created_at": "2026-08-19T00:00:00Z",
                }])
                .to_string(),
            ),
            ("POST", "/evals/trigger") => Response::json(
                200,
                &serde_json::json!({
                    "stream_id": "1-0",
                    "agent_id": AGENT_ID,
                    "version_id": VERSION_ID,
                    "sha": SHA,
                    "suite": "trajectory",
                    "bundle_ref": "s3://example.com/trajectory-bundle",
                    "model": null,
                })
                .to_string(),
            ),
            ("GET", path) if path.starts_with("/evals/matrix?") => Response::json(200, &matrix()),
            other => panic!("unexpected platform request: {other:?}"),
        },
    )
}

fn platform_server() -> support::MockServer {
    platform_server_with_matrix(matrix_body)
}

fn write_bundle() -> tempfile::TempDir {
    let bundle = tempfile::tempdir().expect("bundle temp directory");
    let evals = bundle.path().join("evals");
    std::fs::create_dir_all(&evals).expect("create eval directory");
    let cases = serde_json::to_vec_pretty(&serde_json::json!({
        "name": "trajectory",
        "cases": [
            {
                "id": "ordered",
                "input": "ordered",
                "grader": {"kind": "contains", "expected": "string says no"},
            },
            {
                "id": "wrong_order",
                "input": "wrong order",
                "grader": {"kind": "contains", "expected": "string says yes"},
            }
        ],
    }))
    .expect("serialize cases");
    std::fs::write(evals.join("cases.json"), &cases).expect("write cases");
    let sidecar = serde_json::json!({
        "specs": [
            {
                "case_id": "ordered",
                "expected": ["Read", "Bash"],
                "mode": "in_order",
                "threshold": 1.0,
            },
            {
                "case_id": "wrong_order",
                "expected": ["Read", "Bash"],
                "mode": "in_order",
                "threshold": 1.0,
            }
        ],
    });
    std::fs::write(
        evals.join("trajectory.json"),
        serde_json::to_vec_pretty(&sidecar).expect("serialize trajectory sidecar"),
    )
    .expect("write trajectory sidecar");
    bundle
}

fn unused_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind temporary port")
        .local_addr()
        .expect("temporary port address")
        .port()
}

fn write_kubectl_proxy(dir: &Path) -> PathBuf {
    let path = dir.join("kubectl");
    std::fs::write(
        &path,
        r#"#!/usr/bin/env python3
import os
import select
import socket
import sys
import ctypes
import signal

ctypes.CDLL(None).prctl(1, signal.SIGTERM)

args = sys.argv[1:]
if "port-forward" not in args:
    sys.stderr.write("unexpected kubectl invocation: " + " ".join(args) + "\n")
    sys.exit(64)

mapping = next(arg for arg in reversed(args) if ":" in arg and arg.split(":", 1)[0].isdigit())
local_port = int(mapping.split(":", 1)[0])
backend_host, backend_port = os.environ["CURIE_TEST_API_BACKEND"].rsplit(":", 1)

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", local_port))
listener.listen()

while True:
    client, _ = listener.accept()
    upstream = socket.create_connection((backend_host, int(backend_port)))
    peers = {client: upstream, upstream: client}
    open_sockets = [client, upstream]
    while open_sockets:
        readable, _, _ = select.select(open_sockets, [], [])
        for source in readable:
            data = source.recv(65536)
            if not data:
                open_sockets = []
                break
            peers[source].sendall(data)
    client.close()
    upstream.close()
"#,
    )
    .expect("write kubectl proxy");
    let mut permissions = std::fs::metadata(&path)
        .expect("read kubectl proxy metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&path, permissions).expect("make kubectl proxy executable");
    path
}

fn stub_path(dir: &Path) -> OsString {
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    std::env::join_paths(paths).expect("join tool path")
}

fn run_platform_eval_against(tier: &str, server: support::MockServer) -> (Output, Vec<Request>) {
    let bundle = write_bundle();
    let mut command = Command::new(bin());
    command
        .arg(tier)
        .arg("eval")
        .args(["--channel", CHANNEL, "--api-key", "test-key"])
        .args(["--timeout-secs", "2", "--json"])
        .current_dir(bundle.path())
        .stdin(std::process::Stdio::null());

    let _tools = if tier == "local" {
        command.args(["--api-url", &server.base_url]);
        None
    } else {
        let local_port = unused_port().to_string();
        let tools = tempfile::tempdir().expect("tool temp directory");
        let _kubectl = write_kubectl_proxy(tools.path());
        let backend = server
            .base_url
            .strip_prefix("http://")
            .expect("test server URL");
        command
            .args(["--api-local-port", &local_port])
            .args(["--valkey-password", "unused"])
            .env("CURIE_TEST_API_BACKEND", backend)
            .env("PATH", stub_path(tools.path()));
        Some(tools)
    };

    let output = command.output().expect("run platform trajectory eval");
    (output, server.recorded())
}

fn run_platform_eval(tier: &str) -> (Output, Vec<Request>) {
    run_platform_eval_against(tier, platform_server())
}

fn parsed_output(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "platform eval must emit one JSON result: {error}\n{}",
            output_text(output)
        )
    })
}

fn case_result<'a>(body: &'a serde_json::Value, id: &str) -> &'a serde_json::Value {
    body["cases"]
        .as_array()
        .expect("cases array")
        .iter()
        .find(|case| case["id"] == id)
        .unwrap_or_else(|| panic!("missing result for {id}: {body}"))
}

fn verdict_projection(body: &serde_json::Value) -> BTreeMap<String, (bool, Option<String>)> {
    body["cases"]
        .as_array()
        .expect("cases array")
        .iter()
        .map(|case| {
            (
                case["id"].as_str().expect("case id").to_string(),
                (
                    case["passed"].as_bool().expect("graded verdict"),
                    case["detail"].as_str().map(str::to_string),
                ),
            )
        })
        .collect()
}

fn assert_platform_flow(requests: &[Request]) {
    assert!(
        requests
            .iter()
            .any(|request| request.method == "POST" && request.path == "/evals/trigger"),
        "trajectory eval must trigger the worker eval plane: {requests:?}"
    );
    assert!(
        requests.iter().any(|request| {
            request.method == "GET"
                && request.path.starts_with("/evals/matrix?")
                && request.path.contains("stream_id=1-0")
        }),
        "trajectory eval must read structured matrix results for its exact trigger: {requests:?}"
    );
    let trigger = requests
        .iter()
        .find(|request| request.method == "POST" && request.path == "/evals/trigger")
        .expect("trigger request");
    let body: serde_json::Value = serde_json::from_slice(&trigger.body).expect("trigger JSON body");
    assert_eq!(body["agent_id"], AGENT_ID, "{body}");
    assert_eq!(body["suite"], "trajectory", "{body}");
    assert!(
        body.get("model").is_none(),
        "a normal trajectory eval must not invent a model override: {body}"
    );
}

#[tokio::test]
async fn eval_matrix_dto_preserves_case_status_and_failure_detail() {
    let server = platform_server();
    let matrix = ApiClient::new(&server.base_url, "test-key")
        .expect("API client")
        .eval_matrix("trajectory", 5, Some("1-0"))
        .await
        .expect("read eval matrix");

    assert_eq!(matrix.versions, vec![SHA]);
    assert_eq!(matrix.rows.len(), 3);
    let missing = matrix
        .rows
        .iter()
        .find(|row| row.case_id == "missing_spec")
        .expect("missing spec row");
    assert_eq!(missing.cells.len(), 1);
    let cell = missing
        .cells
        .iter()
        .find(|cell| {
            cell.stream_id.as_deref() == Some("1-0") && cell.scorer.as_deref() == Some("trajectory")
        })
        .expect("triggered trajectory cell");
    assert_eq!(cell.version, SHA);
    assert_eq!(cell.status, "fail");
    assert_eq!(cell.model.as_deref(), Some("resolved-model"));
    assert_eq!(cell.case_count, Some(3));
    assert_eq!(
        cell.detail.as_deref(),
        Some("no trajectory spec for case 'missing_spec'")
    );
}

#[test]
fn local_trajectory_eval_refuses_an_explicit_cases_override() {
    let server = platform_server();
    let bundle = write_bundle();
    let cases = bundle.path().join("evals/cases.json");
    let output = Command::new(bin())
        .args(["local", "eval", "--channel", CHANNEL])
        .args(["--api-key", "test-key", "--api-url", &server.base_url])
        .args(["--timeout-secs", "1", "--json"])
        .arg("--cases")
        .arg(&cases)
        .current_dir(bundle.path())
        .stdin(std::process::Stdio::null())
        .output()
        .expect("run local trajectory eval with explicit cases");

    assert_eq!(output.status.code(), Some(4), "{}", output_text(&output));
    let text = output_text(&output).to_lowercase();
    assert!(text.contains("--cases"), "{text}");
    assert!(text.contains("deployed"), "{text}");
}

#[test]
fn local_and_cluster_use_the_same_structured_trajectory_verdicts() {
    let (local_output, local_requests) = run_platform_eval("local");
    let (cluster_output, cluster_requests) = run_platform_eval("cluster");

    assert!(
        !local_output.status.success(),
        "the structured local result contains failures\n{}",
        output_text(&local_output)
    );
    assert!(
        !cluster_output.status.success(),
        "the structured cluster result contains failures\n{}",
        output_text(&cluster_output)
    );
    let local = parsed_output(&local_output);
    let cluster = parsed_output(&cluster_output);
    assert_eq!(verdict_projection(&local), verdict_projection(&cluster));
    assert_eq!(case_result(&local, "ordered")["passed"], true, "{local}");
    assert_eq!(
        case_result(&local, "wrong_order")["passed"],
        false,
        "{local}"
    );
    assert!(
        case_result(&local, "wrong_order")["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("observed")),
        "the wrong order verdict must carry worker scoring detail: {local}"
    );
    assert!(
        case_result(&local, "missing_spec")["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("no trajectory spec")),
        "the missing spec verdict must remain explanatory: {local}"
    );
    assert_platform_flow(&local_requests);
    assert_platform_flow(&cluster_requests);
}

#[test]
fn platform_eval_waits_for_the_deployed_case_count_before_reporting() {
    let matrix_polls = Arc::new(AtomicUsize::new(0));
    let handler_polls = Arc::clone(&matrix_polls);
    let server = platform_server_with_matrix(move || {
        if handler_polls.fetch_add(1, Ordering::SeqCst) == 0 {
            incomplete_matrix_body()
        } else {
            matrix_body()
        }
    });

    let (output, requests) = run_platform_eval_against("local", server);
    assert!(
        !output.status.success(),
        "the deployed missing spec row must make the eval fail\n{}",
        output_text(&output)
    );
    assert!(
        matrix_polls.load(Ordering::SeqCst) >= 2,
        "the first response had both locally authored cases but only two of the deployed run's three rows: {requests:?}"
    );
    let body = parsed_output(&output);
    assert_eq!(body["cases"].as_array().map(Vec::len), Some(3), "{body}");
    assert!(
        case_result(&body, "missing_spec")["detail"]
            .as_str()
            .is_some_and(|detail| detail.contains("no trajectory spec")),
        "the matrix only deployed case must be reported: {body}"
    );
}
