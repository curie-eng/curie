//! ADR-0081 / issue #872: the nightly graded parity ladder is the workflow
//! that runs the cold-start parity ladder (`curie dev e2e-ladder`) LIVE
//! against a real model, not the sealed fake-model install `ci.yaml` runs on
//! every PR. `ci.yaml`'s cluster ladder job installs with `--fake-model`
//! (ADR-0055 bounds what that fake green means: plumbing only, never a graded
//! turn). The nightly workflow sits on the opposite side of that seam: it
//! must arm `CURIE_E2E_LIVE`, never seal the install, and carry the
//! OpenRouter credential only as the one env key the runner reads.
//!
//! Grounding for the OpenRouter/env-key claims asserted below:
//! - `docs/interfaces/model-provider/INTERFACE.md:73-76`: an `sk-or-` credential
//!   auto-selects `OPENROUTER_BASE_URL`; no `ANTHROPIC_BASE_URL` is set for this
//!   provider, and the real key travels as `ANTHROPIC_API_KEY` inside the
//!   runner -- the CLI-facing input for that credential is `CURIE_CREDENTIALS`.
//! - `cli/src/ops/providers.rs` (`parse_egress_provider` / `provider_egress_hosts`): `--allow-egress-host` takes the provider
//!   KEYWORD `openrouter` (resolved to `openrouter.ai` at install time), never
//!   a bare hostname like `openrouter.ai` itself.
//!
//! This file is a text-contract test against the workflow YAML AS TEXT (no
//! `serde_yaml`, no new dependency -- std `fs` only), so it fails today
//! because `.github/workflows/nightly-graded-ladder.yaml` does not exist yet.
//! Each assertion targets a user-visible CI contract (arms live, opens
//! egress, never seals, never leaks the secret), not Rust internals, so it
//! survives a rename or reformat of the workflow as long as the contract
//! holds. Deleting the workflow fails every assertion below except the CI
//! sibling anchor (assertion group 3), which stays green against the
//! existing `ci.yaml` and only breaks if someone arms `ci.yaml`'s fake seal
//! off, proving the two workflows are pinned to opposite sides of the seam.

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;

use sha2::{Digest, Sha256};

/// Read a workflow file's raw text, or an empty string when it does not exist
/// yet. Assertions on an empty string fail with their own readable messages
/// rather than panicking on a missing file, so a missing nightly workflow
/// surfaces as a normal test failure naming the violated contract.
fn workflow_text(name: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../.github/workflows")
        .join(name);
    fs::read_to_string(path).unwrap_or_default()
}

fn nightly() -> String {
    workflow_text("nightly-graded-ladder.yaml")
}

fn ci() -> String {
    workflow_text("ci.yaml")
}

fn ladder() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/e2e-ladder.sh");
    fs::read_to_string(path).unwrap_or_default()
}

fn ladder_function(name: &str) -> String {
    let source = ladder();
    let marker = format!("{name}() {{");
    let (_, tail) = source
        .split_once(&marker)
        .unwrap_or_else(|| panic!("ladder must define {name}"));
    let (body, _) = tail
        .split_once("\n}\n")
        .unwrap_or_else(|| panic!("ladder function {name} must close"));
    format!("{marker}{body}\n}}\n")
}

fn ladder_function_before(name: &str, next_name: &str) -> String {
    let source = ladder();
    let marker = format!("{name}() {{");
    let (_, tail) = source
        .split_once(&marker)
        .unwrap_or_else(|| panic!("ladder must define {name}"));
    let next_marker = format!("\n{next_name}() {{");
    let (body, _) = tail
        .split_once(&next_marker)
        .unwrap_or_else(|| panic!("ladder function {name} must precede {next_name}"));
    format!("{marker}{body}\n")
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .canonicalize()
        .expect("canonicalize repo root")
}

fn sh_single_quote(path: &Path) -> String {
    format!("'{}'", path.display().to_string().replace('\'', "'\"'\"'"))
}

fn ladder_quoted_assignment(name: &str) -> String {
    let prefix = format!("{name}=\"");
    let source = ladder();
    let (_, tail) = source
        .split_once(&prefix)
        .unwrap_or_else(|| panic!("ladder must assign {name}"));
    let (value, _) = tail
        .split_once('"')
        .unwrap_or_else(|| panic!("ladder assignment {name} must be a quoted string"));
    value.to_string()
}

fn copy_example_bundle(name: &str, dest: &Path) {
    fs::create_dir_all(dest).expect("create scratch bundle directory");
    let src = repo_root().join("examples").join(name);
    let status = Command::new("cp")
        .arg("-a")
        .arg(format!("{}/.", src.display()))
        .arg(dest)
        .status()
        .expect("copy example bundle");
    assert!(
        status.success(),
        "copying examples/{name} into {} must succeed",
        dest.display()
    );
}

fn run_ladder_setup_function(function_name: &str, bundle: &Path) -> Output {
    let repo = repo_root();
    let function = ladder_function(function_name);
    let script = format!(
        "set -euo pipefail\n\
         REPO_ROOT={repo}\n\
         CONNECTOR_FIXTURE=\"$REPO_ROOT/cli/scripts/fixtures/sre-bot-connectors-enabled.yaml\"\n\
         MCP_RECEIPT_FIXTURE=\"$REPO_ROOT/cli/scripts/fixtures/mcp-receipt\"\n\
         MCP_RECEIPT_CONNECTOR={connector}\n\
         {function}\n\
         {function_name} {bundle}\n",
        repo = sh_single_quote(&repo),
        connector = sh_single_quote(Path::new(&ladder_quoted_assignment(
            "MCP_RECEIPT_CONNECTOR"
        ))),
        function = function,
        function_name = function_name,
        bundle = sh_single_quote(bundle),
    );
    Command::new("bash")
        .arg("-c")
        .arg(script)
        .output()
        .unwrap_or_else(|error| panic!("run {function_name}: {error}"))
}

fn validate_bundle_json(dir: &Path) -> serde_json::Value {
    let validation = Command::new("uv")
        .current_dir(repo_root())
        .args([
            "run",
            "--frozen",
            "python",
            "-c",
            "import sys\n\
             from plugin_format import validate_bundle\n\
             print(validate_bundle(sys.argv[1], enforces_tool_policy='curie/mcp-tool-policy@1').model_dump_json())\n",
        ])
        .arg(dir)
        .output()
        .expect("run plugin_format.validate_bundle");
    let stdout = String::from_utf8_lossy(&validation.stdout).to_string();
    let stderr = String::from_utf8_lossy(&validation.stderr).to_string();
    assert!(
        validation.status.success(),
        "the bundle validator must run: stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let reported = stdout
        .lines()
        .rev()
        .find(|line| !line.trim().is_empty())
        .unwrap_or_else(|| panic!("the validator must report a result: stderr:\n{stderr}"));
    serde_json::from_str(reported)
        .unwrap_or_else(|error| panic!("validator output must be JSON ({error}): {reported}"))
}

fn connector_name_forges_mcp_join(name: &str) -> bool {
    name.starts_with("mcp-") || name.contains("-mcp-")
}

/// Setup stages run before the ladder's one `curie build`. `build:` connectors
/// are therefore allowed to report `connectors.lock_missing` here; the live
/// boot failures were `connectors.ambiguous_name` and
/// `approval_policy.gate_not_namespaced`, which must not survive setup.
fn assert_setup_bundle_has_no_boot_blockers(result: &serde_json::Value) {
    let errors = result["errors"]
        .as_array()
        .expect("validator errors must be an array");
    let codes: Vec<&str> = errors
        .iter()
        .map(|error| error["code"].as_str().expect("error code"))
        .collect();
    assert!(
        !codes.iter().any(|code| *code == "connectors.ambiguous_name"
            || *code == "approval_policy.gate_not_namespaced"),
        "setup must not leave a bundle that skill up refuses to boot: {result}"
    );
    assert!(
        codes
            .iter()
            .all(|code| *code == "connectors.lock_missing"),
        "the only remaining validator errors before the ladder build must be lock_missing: {result}"
    );
}

fn ladder_python_heredoc(function_name: &str) -> String {
    let function = ladder_function(function_name);
    let (_, tail) = function
        .split_once("<<'PY'\n")
        .unwrap_or_else(|| panic!("ladder function {function_name} must contain Python"));
    tail.split_once("\nPY\n")
        .unwrap_or_else(|| panic!("ladder function {function_name} Python must close"))
        .0
        .to_owned()
}

fn run_seed_trace_matcher(matcher: &str, marker: &str, rows: &serde_json::Value) -> Output {
    let harness = tempfile::tempdir().expect("create seed matcher fixture directory");
    let bounded_slice = harness.path().join("bounded-stream.json");
    fs::write(
        &bounded_slice,
        serde_json::to_vec(rows).expect("serialize bounded stream fixture"),
    )
    .expect("write bounded stream fixture");
    let mut child = Command::new("python3")
        .arg("-")
        .arg(&bounded_slice)
        .arg(marker)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start exact seed matcher");
    child
        .stdin
        .take()
        .expect("open matcher stdin")
        .write_all(matcher.as_bytes())
        .expect("write exact seed matcher");
    child.wait_with_output().expect("wait for seed matcher")
}

/// The local OTel assertions deliberately live in the shell harness, where
/// they consume the Collector's raw JSON files.  Keep this test on that real
/// consumer instead of re-implementing its attribution rules in Rust.
fn local_otel_query_python() -> String {
    let source = ladder();
    let (_, after_command) = source
        .split_once("python3 - \"$mode\" \"$WORKDIR/otel-sink\"")
        .expect("local OTel query command must remain in the ladder");
    let (_, script) = after_command
        .split_once("<<'PY'\n")
        .expect("local OTel query must retain its Python heredoc");
    script
        .split_once("\nPY\n}")
        .expect("local OTel query heredoc must have a closing delimiter")
        .0
        .to_owned()
}

fn run_local_otel_query(script: &str, mode: &str, root: &Path, baseline: &Path) -> Output {
    let mut command = Command::new("python3");
    command
        .arg("-")
        .arg(mode)
        .arg(root)
        .arg(baseline)
        .arg("fixture prompt")
        .arg("fixture sentinel")
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".."))
        // The fixture has one canonical generation, not the live two-round
        // tool path; the live switch keeps that unrelated shape requirement out
        // of this metric-attribution regression.
        .arg("1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("start extracted local OTel query");
    child
        .stdin
        .take()
        .expect("open query stdin")
        .write_all(script.as_bytes())
        .expect("write extracted local OTel query");
    child.wait_with_output().expect("wait for local OTel query")
}

fn write_otlp_fixture(root: &Path, name: &str, documents: &[serde_json::Value]) {
    let mut rendered = String::new();
    for document in documents {
        rendered.push_str(&serde_json::to_string(document).expect("serialize OTLP fixture"));
        rendered.push('\n');
    }
    fs::write(root.join(format!("{name}.json")), rendered).expect("write OTLP fixture");
}

#[test]
fn local_otel_failure_recovery_scopes_classified_metrics_to_the_traces_worker_instance() {
    use serde_json::json;

    let harness = tempfile::tempdir().expect("create local OTel fixture directory");
    let root = harness.path();
    let script = local_otel_query_python();
    let empty = root.join("empty.json");
    fs::write(&empty, r#"{"trace_ids":[],"metrics":[]}"#).expect("write empty baseline");

    let attr = |key: &str, value: &str| json!({"key": key, "value": {"stringValue": value}});
    let resource = |service: &str, instance: &str| json!({"attributes": [attr("service.name", service), attr("service.instance.id", instance)]});
    let span = |name: &str, service: &str, status: i64, trace: &str, id: &str| json!({"name": name, "traceId": trace, "spanId": id, "status": {"code": status}, "service": service, "instance": format!("{service}-{trace}")});
    let failure_trace = "failure-trace";
    let mut failed = vec![
        span(
            "curie.queue.enqueue",
            "curie-dispatcher",
            1,
            failure_trace,
            "01",
        ),
        span(
            "curie.queue.process",
            "curie-dispatcher",
            1,
            failure_trace,
            "02",
        ),
        span("curie.turn.process", "curie-worker", 2, failure_trace, "03"),
        span(
            "curie.sandbox.claim",
            "curie-worker",
            1,
            failure_trace,
            "04",
        ),
        span("curie.runner.rpc", "curie-worker", 1, failure_trace, "05"),
        span("agent.run", "curie-runner", 2, failure_trace, "06"),
    ];
    failed[2]["events"] = json!([{"attributes": [attr("curie.outcome", "classified_failure")]}]);
    let trace_doc = |spans: Vec<serde_json::Value>| {
        json!({"resourceSpans": spans.into_iter().map(|span| {
        let service = span["service"].as_str().expect("fixture service").to_owned();
        let instance = span["instance"].as_str().expect("fixture instance").to_owned();
        let mut span = span;
        span.as_object_mut().expect("fixture span").remove("service");
        span.as_object_mut().expect("fixture span").remove("instance");
        json!({"resource": resource(&service, &instance), "scopeSpans": [{"spans": [span]}]})
    }).collect::<Vec<_>>()})
    };
    write_otlp_fixture(root, "traces", &[trace_doc(failed)]);
    write_otlp_fixture(
        root,
        "logs",
        &[
            json!({"resourceLogs": [{"resource": resource("curie-worker", "curie-worker-failure-trace"), "scopeLogs": [{"logRecords": [{"traceId": failure_trace, "spanId": "03", "severityNumber": 17}]}]}]}),
        ],
    );
    let metric = |service: &str,
                  instance: &str,
                  name: &str,
                  outcome: &str,
                  value: i64,
                  time: i64| {
        json!({
            "resourceMetrics": [{"resource": resource(service, instance), "scopeMetrics": [{"metrics": [{"name": name, "sum": {"dataPoints": [{"attributes": [attr("outcome", outcome)], "asInt": value, "timeUnixNano": time}]}}]}]}]
        })
    };

    // The runner has already emitted the failed trace, but its worker-owned
    // completed counter has not yet crossed the BatchSpanProcessor boundary.
    write_otlp_fixture(
        root,
        "metrics",
        &[metric(
            "curie-runner",
            "curie-runner-failure-trace",
            "curie.turn.completed",
            "classified_failure",
            1,
            1,
        )],
    );
    let runner_only = run_local_otel_query(&script, "failed", root, &empty);
    let racy_baseline = root.join("racy-baseline.json");
    let runner_snapshot: serde_json::Value =
        serde_json::from_slice(&run_local_otel_query(&script, "snapshot", root, &empty).stdout)
            .expect("parse runner-only snapshot");
    fs::write(
        &racy_baseline,
        serde_json::to_vec(&runner_snapshot).expect("serialize recovery baseline"),
    )
    .expect("write pre-export recovery baseline");
    let failed_metric_baseline = root.join("failed-metric-baseline.json");
    let mut failed_metric_snapshot = runner_snapshot;
    failed_metric_snapshot["trace_ids"] = json!([]);
    fs::write(
        &failed_metric_baseline,
        serde_json::to_vec(&failed_metric_snapshot).expect("serialize failed metric baseline"),
    )
    .expect("write failed metric baseline");

    // This is the late worker export that used to land after restored_before.
    let mut metrics = vec![metric(
        "curie-runner",
        "curie-runner-failure-trace",
        "curie.turn.completed",
        "classified_failure",
        1,
        1,
    )];
    metrics.push(metric(
        "curie-worker",
        "curie-worker-failure-trace",
        "curie.turn.completed",
        "classified_failure",
        1,
        2,
    ));
    write_otlp_fixture(root, "metrics", &metrics);
    let delayed_failed_worker =
        run_local_otel_query(&script, "failed", root, &failed_metric_baseline);

    let healthy_trace = "healthy-trace";
    let mut healthy = vec![
        span("curie.health", "curie-api", 1, healthy_trace, "10"),
        span(
            "curie.queue.enqueue",
            "curie-dispatcher",
            1,
            healthy_trace,
            "11",
        ),
        span(
            "curie.queue.process",
            "curie-dispatcher",
            1,
            healthy_trace,
            "12",
        ),
        span("curie.turn.process", "curie-worker", 1, healthy_trace, "13"),
        span(
            "curie.sandbox.claim",
            "curie-worker",
            1,
            healthy_trace,
            "14",
        ),
        span("curie.runner.rpc", "curie-worker", 1, healthy_trace, "15"),
        span(
            "curie.reply.post",
            "curie-dispatcher",
            1,
            healthy_trace,
            "16",
        ),
        json!({"name": "agent.run", "traceId": healthy_trace, "spanId": "17", "status": {"code": 1}, "attributes": [attr("curie.phase", "provider_wait"), attr("curie.terminal.cause", "completed"), attr("curie.terminal.status", "succeeded")], "service": "curie-runner", "instance": "curie-runner-healthy-trace"}),
        json!({"name": "llm.generation", "traceId": healthy_trace, "spanId": "18", "parentSpanId": "17", "status": {"code": 1}, "attributes": [attr("curie.phase", "provider_wait"), attr("curie.phase.start_kind", "query_observed"), attr("curie.phase.end_kind", "result_observed"), json!({"key": "curie.generation.round", "value": {"intValue": "1"}})], "service": "curie-runner", "instance": "curie-runner-healthy-trace"}),
    ];
    let mut traces = vec![trace_doc(vec![
        span(
            "curie.queue.enqueue",
            "curie-dispatcher",
            1,
            failure_trace,
            "01",
        ),
        span(
            "curie.queue.process",
            "curie-dispatcher",
            1,
            failure_trace,
            "02",
        ),
        span("curie.turn.process", "curie-worker", 2, failure_trace, "03"),
        span(
            "curie.sandbox.claim",
            "curie-worker",
            1,
            failure_trace,
            "04",
        ),
        span("curie.runner.rpc", "curie-worker", 1, failure_trace, "05"),
        json!({"name": "agent.run", "traceId": failure_trace, "spanId": "06", "status": {"code": 2}, "events": [{"attributes": [attr("curie.outcome", "classified_failure")]}], "service": "curie-runner", "instance": "curie-runner-failure-trace"}),
    ])];
    traces.push(trace_doc(std::mem::take(&mut healthy)));
    write_otlp_fixture(root, "traces", &traces);
    let resource_logs: Vec<serde_json::Value> = [
        "curie-api",
        "curie-dispatcher",
        "curie-worker",
        "curie-runner",
    ]
    .into_iter()
    .map(|service| {
        json!({"resource": resource(service, &format!("{service}-{healthy_trace}")), "scopeLogs": [{"logRecords": [{"traceId": healthy_trace, "spanId": "11", "severityNumber": 9}]}]})
    })
    .collect();
    write_otlp_fixture(root, "logs", &[json!({"resourceLogs": resource_logs})]);
    for (name, outcome) in [
        ("curie.turn.accepted", ""),
        ("curie.turn.completed", "done"),
        ("curie.turn.duration", "done"),
        ("curie.queue.message.age", ""),
        ("curie.sandbox.lifecycle", ""),
        ("curie.runner.rpc.result", "success"),
    ] {
        metrics.push(metric(
            "curie-worker",
            "curie-worker-healthy-trace",
            name,
            outcome,
            1,
            3,
        ));
    }
    write_otlp_fixture(root, "metrics", &metrics);
    let racy_healthy = run_local_otel_query(&script, "healthy", root, &racy_baseline);
    assert!(
        !runner_only.status.success(),
        "runner-only classified_failure must not prove the worker failure"
    );
    assert!(
        delayed_failed_worker.status.success(),
        "the causally matched failed worker metric must retain the failure proof: {}",
        String::from_utf8_lossy(&delayed_failed_worker.stderr)
    );
    assert!(
        racy_healthy.status.success(),
        "a delayed failure from the old worker instance must not poison healthy recovery: {}",
        String::from_utf8_lossy(&racy_healthy.stderr)
    );
    metrics.push(metric(
        "curie-worker",
        "curie-worker-healthy-trace",
        "curie.turn.completed",
        "classified_failure",
        1,
        4,
    ));
    write_otlp_fixture(root, "metrics", &metrics);
    let same_healthy_worker_failure =
        run_local_otel_query(&script, "healthy", root, &racy_baseline);
    assert!(
        !same_healthy_worker_failure.status.success(),
        "classified_failure on the healthy worker instance must remain a negative control"
    );
}

fn chart_runtime_e2e() -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../scripts/chart-runtime-e2e.sh");
    fs::read_to_string(path).unwrap_or_default()
}

fn count_lines_containing(text: &str, needle: &str) -> usize {
    text.lines().filter(|line| line.contains(needle)).count()
}

#[test]
fn chart_runtime_observes_each_refusal_series_and_keeps_healthy_failures_zero() {
    let text = chart_runtime_e2e();
    let healthy = text
        .split_once("banner \"ASSERT OTel healthy reception and delivery\"")
        .and_then(|(_, tail)| {
            tail.split_once("banner \"ASSERT backend outage queues and reports failure\"")
        })
        .map(|(case, _)| case)
        .expect("chart runtime must retain bounded healthy and outage controls");
    let overflow = text
        .split_once(
            "banner \"ASSERT tiny persistent queue overflows loudly and loses bounded excess\"",
        )
        .map(|(_, case)| case)
        .expect("chart runtime must retain the bounded overflow control");

    for metric in [
        "otelcol_receiver_refused_spans",
        "otelcol_receiver_refused_log_records",
        "otelcol_receiver_refused_metric_points",
    ] {
        assert!(text.contains(metric), "chart runtime omitted {metric}");
    }
    assert!(healthy.contains(
        "assert_metrics_zero \"$OTEL_COLLECTOR_SERVICE\" \"\" \"${OTELCOL_REFUSED_METRICS[@]}\""
    ));
    assert!(
        healthy.contains(
            "assert_metrics_zero \"$OTEL_COLLECTOR_SERVICE\" \"$OTEL_EXPORTER\" \\\n  \"${OTELCOL_FAILED_METRICS[@]}\""
        ),
        "the healthy path must require every Collector send_failed series to be present and zero"
    );
    assert!(
        !healthy.contains("OTELCOL_ENQUEUE_FAILED_METRICS"),
        "Collector 0.119 does not expose enqueue_failed series before an enqueue failure occurs"
    );
    assert!(
        overflow.contains(
            "wait_metrics_positive \"$OTEL_COLLECTOR_SERVICE\" \"$OTEL_EXPORTER\" \"${OTELCOL_ENQUEUE_FAILED_METRICS[@]}\""
        ),
        "the bounded overflow path must still require every enqueue_failed signal series to move"
    );
}

#[test]
fn chart_runtime_sustains_a_ready_bounded_outage_before_restart_and_overflow() {
    let text = chart_runtime_e2e();
    let bounded_function = text
        .split_once("assert_sustained_outage_bounded()")
        .and_then(|(_, tail)| tail.split_once("\n}\n\n"))
        .map(|(function, _)| function)
        .expect("chart runtime must retain the sustained-outage assertion function");
    let storage_observer = text
        .split_once("create_otlp_storage_observer()")
        .and_then(|(_, tail)| tail.split_once("\n}\n\n"))
        .map(|(function, _)| function)
        .expect("chart runtime must create a task-owned Collector PVC storage observer");
    let bounded = text
        .find("assert_sustained_outage_bounded \"$OUTAGE_COLLECTOR_POD\"")
        .expect("outage path must execute the bounded readiness control");
    let restart = text
        .find("ASSERT Collector restart retains the queued signals")
        .expect("chart runtime must retain the restart proof");
    let overflow = text
        .find("ASSERT tiny persistent queue overflows loudly")
        .expect("chart runtime must retain the overflow proof");
    assert!(bounded < restart && restart < overflow);
    assert!(text.contains("stopped being Ready during sustained exporter outage"));
    assert!(text.contains("Collector restarted during sustained exporter outage"));
    assert!(text.contains("Collector was OOMKilled during sustained exporter outage"));
    assert!(text.contains(".status.containerStatuses[0].restartCount"));
    assert!(text.contains("0 < value <= capacity"));
    assert!(text.contains("OTEL_STORAGE_OBSERVER=\"e2e-otel-storage-observer\""));
    assert!(text.contains("\ncreate_otlp_storage_observer\n"));
    assert!(storage_observer.contains("name: $OTEL_STORAGE_OBSERVER"));
    assert!(storage_observer.contains("app.kubernetes.io/component: e2e-otel-storage-observer"));
    assert!(storage_observer.contains("app.kubernetes.io/instance: $RELEASE"));
    assert!(storage_observer.contains("claimName: $COLLECTOR_PVC"));
    assert!(storage_observer.contains("mountPath: /var/lib/otelcol"));
    assert!(storage_observer.contains("readOnly: true"));
    let observer_exec = bounded_function
        .find("kubectl exec \"$OTEL_STORAGE_OBSERVER\"")
        .expect("the sustained-outage path must measure storage through its PVC observer");
    let usage = bounded_function
        .find("du -sk /var/lib/otelcol")
        .expect("the sustained-outage path must keep its durable PVC usage measurement");
    assert!(
        observer_exec < usage,
        "the durable PVC usage measurement must run through the task-owned storage observer"
    );
    assert!(
        !bounded_function.contains("kubectl exec \"$pod\" -n \"$NAMESPACE\" -- sh"),
        "the distroless Collector pod must not be used to run sh for PVC measurement"
    );
    assert!(
        !bounded_function.contains("kubectl exec \"$pod\" -n \"$NAMESPACE\" -- du"),
        "the distroless Collector pod must not be used to run du for PVC measurement"
    );
    let observer_delete = text
        .find("kubectl delete pod \"$OTEL_STORAGE_OBSERVER\"")
        .expect("the PVC observer must release its read-only mount before Collector restart");
    let collector_delete = text
        .find("kubectl delete pod \"$OLD_COLLECTOR_POD\"")
        .expect("chart runtime must retain the explicit Collector pod restart");
    assert!(
        bounded < observer_delete && observer_delete < collector_delete,
        "the storage observer must be deleted after bounded outage measurement and before the Collector pod restart"
    );
    assert!(text.contains("beyond declared ${bound_kib}Ki bound"));
    assert!(text.contains("while retrying a fixed bounded queue"));
}

#[test]
fn chart_runtime_otel_probe_and_metrics_observer_use_release_identity() {
    let text = chart_runtime_e2e();
    let sender = text
        .split("send_otlp_triplet()")
        .nth(1)
        .expect("OTLP triplet sender must remain in the chart runtime harness");
    assert!(sender.contains(
        "template:\n    metadata:\n      labels:\n        app.kubernetes.io/name: curie\n        app.kubernetes.io/instance: $RELEASE"
    ));
    assert!(sender.contains("app.kubernetes.io/component: e2e-otel-probe"));

    assert!(text.contains("ASSERT OTel healthy reception and delivery"));

    assert!(text.contains(
        "name: $OTEL_OBSERVER\n  labels:\n    app.kubernetes.io/name: curie\n    app.kubernetes.io/instance: $RELEASE"
    ));
    assert!(text.contains("metrics_snapshot \"$OTEL_COLLECTOR_SERVICE\""));
}

#[test]
fn chart_runtime_falsifies_collector_metrics_ingress_policy() {
    let text = chart_runtime_e2e();
    assert!(
        text.contains("OTEL_UNSELECTED_OBSERVER=\"e2e-otel-unselected-observer\""),
        "chart runtime must create a distinct same-namespace observer that the configured peer does not select"
    );
    assert!(
        text.contains("OTEL_METRICS_TEST_ALLOW=\"e2e-otel-metrics-test-allow\""),
        "chart runtime must own a temporary allow policy for the falsifiability control"
    );
    assert!(
        text.contains("security:\n  otelCollectorNetworkPolicy:\n    metricsIngress:"),
        "the runtime overlay must configure the selected observer through the chart's standard NetworkPolicyPeer surface"
    );

    let resources = text
        .split_once("create_otlp_probe_resources()")
        .and_then(|(_, tail)| tail.split_once("\n}\n\n"))
        .map(|(function, _)| function)
        .expect("chart runtime must retain its task-owned OTLP probe resources");
    assert!(resources.contains("name: $OTEL_UNSELECTED_OBSERVER"));
    assert!(resources.contains("app.kubernetes.io/instance: $RELEASE"));
    assert!(resources.contains("app.kubernetes.io/component: e2e-otel-unselected-observer"));

    let proof = text
        .split_once("assert_collector_metrics_network_policy()")
        .and_then(|(_, tail)| tail.split_once("\n}\n\n"))
        .map(|(function, _)| function)
        .expect("chart runtime must retain a bounded Collector metrics policy proof");
    let selected_positive = proof
        .find("metrics_scrape_from \"$OTEL_OBSERVER\"")
        .expect("selected observer must positively scrape Collector self-metrics");
    let unselected_negative = proof
        .find("assert_metrics_scrape_denied \"$OTEL_UNSELECTED_OBSERVER\"")
        .expect("unselected same-namespace observer must be denied before the control allow");
    let temporary_allow = proof
        .find("name: $OTEL_METRICS_TEST_ALLOW")
        .expect("proof must apply a temporary targeted NetworkPolicy allow");
    let unselected_positive = proof[temporary_allow..]
        .find("metrics_scrape_from \"$OTEL_UNSELECTED_OBSERVER\"")
        .map(|offset| temporary_allow + offset)
        .expect("the denied observer must scrape after only its targeted allow is applied");
    let remove_allow = proof
        .find("kubectl delete networkpolicy \"$OTEL_METRICS_TEST_ALLOW\"")
        .expect("proof must remove its temporary targeted allow");
    assert!(
        selected_positive < unselected_negative
            && unselected_negative < temporary_allow
            && temporary_allow < unselected_positive
            && unselected_positive < remove_allow,
        "metrics policy proof must observe allow, deny, targeted re-allow, then cleanup in causal order"
    );
    assert!(proof.contains("podSelector:"));
    assert!(proof.contains("app.kubernetes.io/component: otel-collector"));
    assert!(proof.contains("from:"));
    assert!(proof.contains("app.kubernetes.io/component: e2e-otel-unselected-observer"));
    assert!(proof.contains("port: 8888"));
    assert!(text.contains("\nassert_collector_metrics_network_policy\n"));
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write harness executable");
    let mut permissions = fs::metadata(path)
        .expect("read harness metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("mark harness executable");
}

/// The cluster ladder may run an otherwise standard `curie` release in an
/// owned namespace. Its direct worker probe must follow the same
/// `CURIE_NAMESPACE` setting as the CLI calls around it, or it reads an
/// unrelated shared install before the first cluster message is sent.
#[test]
fn cluster_worker_probe_uses_the_configured_namespace() {
    const NAMESPACE: &str = "test-2593-chart-ladder";

    let harness = tempfile::tempdir().expect("create cluster probe harness");
    let invocation_log = harness.path().join("kubectl-invocation.log");
    write_executable(
        &harness.path().join("kubectl"),
        r#"#!/bin/sh
set -eu
printf '%s\n' "$*" > "$STUB_KUBECTL_INVOCATION_LOG"
printf '1'
"#,
    );

    let helper = ladder_function("cluster_worker_deploy");
    let function = ladder_function("probe_cluster_fake_model");
    let script = format!("set -euo pipefail\n{helper}\n{function}\nprobe_cluster_fake_model\n");
    let path = format!(
        "{}:{}",
        harness.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .arg("-c")
        .arg(script)
        .env("PATH", path)
        .env("CURIE_NAMESPACE", NAMESPACE)
        .env("STUB_KUBECTL_INVOCATION_LOG", &invocation_log)
        .output()
        .expect("run the real cluster worker probe");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "the cluster worker probe must complete through the stub: stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(stdout, "1", "the probe must return kubectl's model mode");

    let invocation = fs::read_to_string(&invocation_log)
        .expect("read kubectl invocation")
        .trim()
        .to_owned();
    assert_eq!(
        invocation,
        format!(
            "-n {NAMESPACE} get deployment/curie-worker -o \
             jsonpath={{.spec.template.spec.containers[*].env[?(@.name==\"CURIE_FAKE_MODEL\")].value}}"
        ),
        "the direct worker read must use the configured namespace while preserving the standard release's deployment and model-mode assertion"
    );
}

// --- Assertion group 1: arms the GRADED path -------------------------------

/// The nightly workflow must arm live grading with the exact double-quoted
/// form, never a bare or unquoted `1` that a YAML parser could read as an
/// integer instead of the string the ladder script compares against.
#[test]
fn nightly_arms_live_grading_with_the_exact_quoted_form() {
    let text = nightly();
    assert!(
        text.contains("CURIE_E2E_LIVE: \"1\""),
        "the nightly workflow must arm CURIE_E2E_LIVE with the exact quoted \
         form `CURIE_E2E_LIVE: \"1\"` so the ladder runs live, not fake; \
         file contents:\n{text}"
    );
}

/// Every line referencing the OpenRouter secret must also assign
/// `CURIE_CREDENTIALS:` on that same line, so the secret reaches the ladder
/// only through the one env key the CLI reads (never bare, never aliased to
/// a differently-named env var that could accidentally land on a `run:` line
/// elsewhere).
#[test]
fn nightly_secret_reaches_the_ladder_only_as_curie_credentials() {
    let text = nightly();
    assert!(
        text.contains("secrets.OPENROUTER_API_KEY"),
        "the nightly workflow must reference secrets.OPENROUTER_API_KEY at \
         least once to supply the live model credential; file contents:\n{text}"
    );
    for line in text.lines() {
        if line.contains("secrets.OPENROUTER_API_KEY") {
            assert!(
                line.contains("CURIE_CREDENTIALS:"),
                "every line referencing secrets.OPENROUTER_API_KEY must also \
                 assign CURIE_CREDENTIALS: on that same line, so the secret \
                 reaches the ladder only as that one env key: {line}"
            );
        }
    }
}

/// The model default `z-ai/glm-5.2` must appear on a line that also names
/// `CURIE_MODEL`, so the graded default is wired as the model the ladder
/// actually calls, not just mentioned in a comment.
#[test]
fn nightly_wires_the_glm_default_model_via_curie_model() {
    let text = nightly();
    let has_model_line = text
        .lines()
        .any(|line| line.contains("z-ai/glm-5.2") && line.contains("CURIE_MODEL"));
    assert!(
        has_model_line,
        "the nightly workflow must have a line containing both the model id \
         z-ai/glm-5.2 and the CURIE_MODEL env key, wiring the default model \
         into the ladder; file contents:\n{text}"
    );
}

/// The nightly ladder must cover both tier sets: the fast `skill,local` rungs
/// and the separate `cluster` rung. Accept either quoted or unquoted YAML
/// scalars by checking substrings rather than exact-matching the whole line.
#[test]
fn nightly_covers_both_the_skill_local_and_cluster_tier_sets() {
    let text = nightly();
    assert!(
        text.contains("skill,local"),
        "the nightly workflow must set CURIE_E2E_TIERS to a value containing \
         `skill,local` so the fast rungs run graded too; file contents:\n{text}"
    );
    let has_cluster_tiers = text.lines().any(|line| {
        let normalized: String = line.split_whitespace().collect();
        normalized.contains("CURIE_E2E_TIERS:cluster")
            || normalized.contains("CURIE_E2E_TIERS:\"cluster\"")
    });
    assert!(
        has_cluster_tiers,
        "the nightly workflow must have a CURIE_E2E_TIERS: cluster (quoted or \
         unquoted) line covering the cluster rung separately from \
         skill,local; file contents:\n{text}"
    );
}

// --- Assertion group 2: cluster graded install -----------------------------

/// The cluster install must open egress to the `openrouter` provider keyword
/// (resolved to `openrouter.ai` at install time by `cli/src/ops/providers.rs`'s
/// `parse_egress_provider` / `provider_egress_hosts`),
/// and the workflow must never contain the sealed-install flag anywhere --
/// proving the cluster rung is graded, not fake.
#[test]
fn nightly_cluster_install_opens_openrouter_egress_and_never_seals() {
    let text = nightly();
    assert!(
        text.contains("--allow-egress-host openrouter"),
        "the nightly workflow's cluster install must open egress with \
         `--allow-egress-host openrouter` (the provider keyword, not a bare \
         hostname) so the graded model call is reachable; file contents:\n{text}"
    );
    assert!(
        !text.contains("--fake-model"),
        "the nightly workflow must never seal the install with --fake-model \
         anywhere in the file (including comments); a sealed install cannot \
         be the graded ladder; file contents:\n{text}"
    );
}

// --- Assertion group 3: sibling / negative parity (mandatory) --------------

/// `ci.yaml`'s cluster ladder job DOES seal its install with `--fake-model`.
/// This is the sibling anchor: it pins the two workflows on opposite sides of
/// the fake/graded seam. This assertion passes today against the existing
/// `ci.yaml` and fails only if someone arms ci.yaml graded (removing its
/// `--fake-model`) or de-arms the nightly ladder, collapsing the seam this
/// whole test file exists to guard.
#[test]
fn ci_yaml_still_seals_its_cluster_install_with_fake_model() {
    let text = ci();
    assert!(
        text.contains("--fake-model"),
        "ci.yaml must still contain --fake-model in its cluster ladder job; \
         if this ever fails, either ci.yaml was accidentally armed graded or \
         the fake/graded seam this test guards has collapsed; file \
         contents:\n{text}"
    );
}

// --- Assertion group 4: #632 secret posture --------------------------------

/// The workflow must declare least-privilege `permissions:` with
/// `contents: read`, so the nightly job cannot write back to the repo.
#[test]
fn nightly_declares_least_privilege_contents_read_permissions() {
    let text = nightly();
    assert!(
        text.contains("permissions:"),
        "the nightly workflow must declare a permissions: block (least \
         privilege, #632); file contents:\n{text}"
    );
    assert!(
        text.contains("contents: read"),
        "the nightly workflow's permissions: block must include \
         `contents: read`; file contents:\n{text}"
    );
}

/// Every `actions/checkout` use must be paired with
/// `persist-credentials: false`, so the checkout-injected token cannot
/// override a differently-scoped credential used later in the job (the same
/// class of hazard the checkout-credentials learning documents). Counting
/// occurrences (rather than requiring exact adjacency) keeps the assertion
/// robust to step reordering while still catching a checkout step that lacks
/// the setting entirely.
#[test]
fn nightly_pairs_every_checkout_with_persist_credentials_false() {
    let text = nightly();
    let checkout_count = count_lines_containing(&text, "uses: actions/checkout");
    assert!(
        checkout_count > 0,
        "the nightly workflow must use actions/checkout at least once; file \
         contents:\n{text}"
    );
    let persist_false_count = count_lines_containing(&text, "persist-credentials: false");
    assert!(
        persist_false_count >= checkout_count,
        "every actions/checkout use ({checkout_count}) must be paired with \
         persist-credentials: false ({persist_false_count} found); a bare \
         checkout leaves the job token in global git config for later steps \
         to pick up; file contents:\n{text}"
    );
}

/// The OpenRouter secret must never be echoed on a `run:` line. Combined with
/// assertion group 1's "only as CURIE_CREDENTIALS:" check, this closes off
/// the one remaining way the secret could leak into job logs.
#[test]
fn nightly_never_echoes_the_openrouter_secret_on_a_run_line() {
    let text = nightly();
    for line in text.lines() {
        if line.contains("secrets.OPENROUTER_API_KEY") {
            assert!(
                !line.contains("run:"),
                "the OpenRouter secret must never appear on a `run:` line \
                 (it would be echoed into job logs): {line}"
            );
        }
    }
}

// --- Assertion group 5: the eval-block TEXT contracts ----------------------
//
// The four tests below are exact-substring assertions on the ladder script's
// source text, and they are therefore WEAK BY CONSTRUCTION: a substring match
// proves no behavior at all, and a reformat that preserves behavior breaks
// them. They are kept, not deleted, because they encode a real CI contract --
// that the graded nightly path still fires the LIVE tier eval at every rung --
// and deleting them would remove the only guard on it. The real coverage for
// parity lives in the EXECUTING controls at the bottom of this file (assertion
// group 6), which run the script rather than read it.
//
// Each region now also carries the `--dry-run` suite-parity check, which runs
// in fake mode as well as live (no turn, no grading, no stack: the dry-run plan
// line is resolved by the tier's own frozen suite loader). At the cluster rung
// the dry-run and the live eval share one `eval_args` array so `--listen-host`
// is forwarded to BOTH, which is why the two calls sit adjacent. `--json` is the
// one flag NOT in that array: both calls pass it themselves, for different
// reasons (#1602's auditable green, and a machine-readable dry-run plan).
//
// The cluster rung's grade is REPORT ONLY (#1603), so its text contract here
// asserts the eval still RUNS and still runs under `--json`; the non-fatal half
// is proved by the executing control at the bottom of this file, which runs the
// ladder against a stub evaluator that exits 42.

#[test]
fn live_local_rung_grades_the_deployed_weather_cases() {
    let text = ladder();
    let local_rung = text
        .split_once("rung_local() {")
        .and_then(|(_, tail)| {
            tail.split_once("rung_local_release() {")
                .map(|(body, _)| body)
        })
        .expect("ladder must define rung_local before rung_local_release");
    let finalized = text
        .find(r#"assert_finalized_reply "local" "$out""#)
        .expect("the local rung must assert its finalized reply");
    let telemetry = text[finalized..]
        .find(r#"assert_local_otel_healthy_turn "$healthy_before""#)
        .map(|offset| finalized + offset)
        .expect("the local rung must prove healthy turn telemetry after the reply");
    let product_collector = text[telemetry..]
        .find("route_local_observability_to_product_collector")
        .map(|offset| telemetry + offset)
        .expect("the local rung must restore the product Collector before API-backed queries");
    let observability = text[product_collector..]
        .find(r#"prove_local_observability_queries "$agent_id""#)
        .map(|offset| product_collector + offset)
        .expect("the local rung must prove observability queries after telemetry");
    for removed_helper in [
        "discover_local_observability_trace() {",
        "assert_local_observability_detail() {",
    ] {
        assert!(
            !text.contains(removed_helper),
            "a newest-runs selector/raw-dump helper remains ({removed_helper}); variable interpolation must not evade exact-ID discovery"
        );
    }
    for required in [
        "discover_trace_id_for_seed",
        "query_exact_seed_trace",
        "sanitize_exact_trace_read",
        "seed_ordinary_turn",
        "seed_approval_resume_turn",
    ] {
        assert!(
            text.contains(required),
            "the local product proof is missing exact-seed contract {required}"
        );
    }
    let contract = r#"echo "=== curie local eval --dry-run (suite parity) ==="
    local eval_args=(local eval)
    if [[ ! -f "$WORKDIR/bundle/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle/evals/cases.json")
    fi
    assert_suite "local" "$(cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}" --dry-run)"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie local eval ==="
        (cd "$WORKDIR/bundle" && "$BIN" "${eval_args[@]}")
    fi"#;
    let eval = text[observability..]
        .find(contract)
        .map(|offset| observability + offset)
        .expect("the local rung must run its suite check and live eval");
    assert!(
        finalized < telemetry && telemetry < observability && observability < eval,
        "the live local rung must prove telemetry and observability after its \
         plumbing assertion, then run local eval against the deployed weather \
         bundle cases with the suite-parity dry-run check in front of it"
    );
    assert!(
        local_rung.contains("local_compose_cli_args local up") && local_rung.contains("--build"),
        "the local rung must start the full profile required by its \
         observability query proof via local_compose_cli_args and --build; ladder contents:\n{text}"
    );
    assert!(
        !local_rung.contains("up_args+=(--minimal)"),
        "the local rung must never add --minimal now that its observability \
          proof requires Langfuse/ClickHouse; ladder contents:\n{text}"
    );
}

#[test]
fn local_rung_honors_isolated_compose_project_and_ordered_files() {
    let source = ladder();
    let local_rung = ladder_function("rung_local");
    assert!(
        (source.contains("$COMPOSE_PROJECT_NAME") || source.contains("${COMPOSE_PROJECT_NAME"))
            && (source.contains("$COMPOSE_FILE") || source.contains("${COMPOSE_FILE")),
        "the ladder must read COMPOSE_PROJECT_NAME and COMPOSE_FILE rather than only mentioning them in comments"
    );
    assert!(
        !local_rung.contains("docker ps -q --filter 'name=curie-api'"),
        "reuse must not match any curie-api container by name substring:\n{local_rung}"
    );
    assert!(
        local_rung.contains("com.docker.compose.project="),
        "reuse and teardown must filter by the selected compose project:\n{local_rung}"
    );
    assert!(
        source.contains("local_compose_cli_args")
            && (source.contains("--project") || source.contains("-p ")),
        "isolated local up must pass the selected project to the CLI via local_compose_cli_args"
    );
    assert!(
        source.contains("compose.dev.yaml") && local_rung.contains("--build"),
        "isolation must still pin this checkout's compose.dev.yaml with --build"
    );
}

#[test]
fn connector_local_rungs_bind_routes_immediately_before_captured_deploy() {
    for (rung, bundle) in [
        ("rung_local", "$WORKDIR/bundle"),
        ("rung_local_release", "$WORKDIR/bundle-release"),
    ] {
        let function = ladder_function(rung);
        let contract = format!(
            "if connector_mode; then\n        bind_local_connector_approval_routes \"{bundle}\"\n    fi\n    capture_local_deploy \"{bundle}\""
        );
        assert!(
            function.contains(&contract),
            "{rung} must bind every retained connector route under connector_mode immediately before the status-preserving deploy capture:\n{function}"
        );
    }
}

#[test]
fn local_rung_sandbox_sweep_is_project_scoped() {
    let teardown = ladder();
    assert!(
        !teardown
            .contains("orphans=\"$(docker ps -aq --filter \"label=$SANDBOX_LABEL\" 2>/dev/null)\""),
        "sandbox sweep must not select every host-wide sandbox label"
    );
    assert!(
        teardown.contains("com.docker.compose.project=")
            || teardown.contains("CURIE_DOCKER_NETWORK"),
        "sandbox sweep must be scoped to this ladder's project or network"
    );
}

#[test]
fn exact_seed_matcher_recovers_embedded_marker_once_and_rejects_background() {
    let matcher = ladder_python_heredoc("discover_trace_id_for_seed");
    let marker = "curie-seed-ordinary-example";
    let target_trace = "a".repeat(32);
    let background_trace = "b".repeat(32);
    let target = serde_json::json!([
        "2-0",
        [
            "payload",
            serde_json::json!({"text": format!("ordinary correlation {marker}")}).to_string(),
            "traceparent",
            format!("00-{target_trace}-{}-01", "2".repeat(16))
        ]
    ]);
    let background = serde_json::json!([
        "1-0",
        [
            "payload",
            serde_json::json!({"text": "ordinary correlation another-seed"}).to_string(),
            "traceparent",
            format!("00-{background_trace}-{}-01", "1".repeat(16))
        ]
    ]);

    let matched = run_seed_trace_matcher(
        &matcher,
        marker,
        &serde_json::json!([background.clone(), target.clone()]),
    );
    assert!(
        matched.status.success(),
        "representative embedded marker was not recovered: {}",
        String::from_utf8_lossy(&matched.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&matched.stdout).trim(),
        target_trace,
        "background carrier must not substitute for the marker-adjacent carrier"
    );

    let mismatch = run_seed_trace_matcher(&matcher, marker, &serde_json::json!([background]));
    assert!(
        !mismatch.status.success(),
        "a background payload without the marker must fail closed"
    );
    let duplicate = run_seed_trace_matcher(
        &matcher,
        marker,
        &serde_json::json!([
            target,
            [
                "3-0",
                [
                    "payload",
                    serde_json::json!({"text": format!("ordinary correlation {marker}")})
                        .to_string(),
                    "traceparent",
                    format!("00-{}-{}-01", "c".repeat(32), "3".repeat(16))
                ]
            ]
        ]),
    );
    assert!(
        !duplicate.status.success(),
        "multiple matching adjacent carriers must fail closed"
    );
}

#[test]
fn product_observability_requires_four_valid_seeds_and_count_only_mcp_receipt() {
    let text = ladder();
    for required in [
        "seed_ordinary_turn() {",
        "seed_mcp_read_turn() {",
        "seed_approval_resume_turn() {",
        "seed_coding_tool_turn() {",
        "seed-invalid",
        "mcp_receipt_call_count() {",
        "discover_trace_id_for_seed",
        "query_exact_seed_trace",
        "fixtures/mcp-receipt",
    ] {
        assert!(
            text.contains(required),
            "the product observability oracle must pin independent ordinary, MCP, approval, and built-in coding-tool seed evidence; missing {required}"
        );
    }

    let receipt_fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/fixtures/mcp-receipt/server.py");
    let receipt_source = fs::read_to_string(&receipt_fixture).unwrap_or_default();
    assert!(
        receipt_source.contains(r#""tools/call""#),
        "the hosted MCP fixture must log exactly one private receipt per tools/call"
    );
    let receipt = ladder_function("mcp_receipt_call_count");
    assert!(
        receipt.contains("docker logs") || receipt.contains("kubectl logs"),
        "the independent MCP receipt must come from the hosted connector container"
    );
    assert!(
        receipt.contains("count") || receipt.contains("wc -l"),
        "only an aggregate MCP call count may reach evidence output"
    );

    let approval = ladder_function("seed_approval_resume_turn");
    assert!(
        approval.contains("curie.approval.suspend"),
        "approval evidence must require the worker's real parked-turn span"
    );
    assert!(
        !approval.contains("curie.approval.wait"),
        "the oracle must not invent a wait span that no current emitter produces"
    );
}

#[test]
fn coding_tool_seed_drives_a_builtin_tool_and_asserts_execute_tool_in_its_exact_trace() {
    let coding = ladder_function("seed_coding_tool_turn");
    assert!(
        coding.contains("Bash"),
        "the coding seed must drive a built-in coding tool by name, since the MCP seed only covers a hosted connector tool"
    );
    assert!(
        coding.contains("--json local message"),
        "the coding seed must issue a real product turn rather than inspect telemetry alone"
    );
    assert!(
        coding.contains("assert_finalized_reply"),
        "the coding seed must require a finalized reply before trusting its telemetry"
    );
    assert!(
        coding.contains("expected_receipt"),
        "the coding seed must carry an independent deterministic receipt the model cannot produce without executing the tool"
    );
    // A fixed product of two known primes is model-computable, and a DENIED
    // tool call still emits its span, so that receipt proved neither execution
    // nor success. Hash the run's own random marker instead: the digest is not
    // derivable without actually running the tool.
    assert!(
        !coding.contains("100160063"),
        "a literal arithmetic product is model-computable, so it cannot witness that the tool really executed"
    );
    assert!(
        coding.contains("sha256"),
        "the coding receipt must be a sha256, which a model cannot produce without running the tool"
    );
    let receipt_line = coding
        .lines()
        .find(|line| line.contains("expected_receipt="))
        .expect("the coding seed must compute its expected receipt");
    assert!(
        receipt_line.contains("marker"),
        "the receipt must hash this run's unique marker, so it is random per run and cannot be pinned or guessed: {receipt_line}"
    );
    assert!(
        coding.contains("$expected_receipt") && coding.contains("seed-invalid"),
        "the coding seed must check the reply against the expected literal receipt and fail closed when it is absent"
    );
    assert!(
        coding.contains("discover_trace_id_for_seed"),
        "the coding seed must derive the exact trace id of its own turn, never a newest-N or window query"
    );
    assert!(
        coding.contains("query_exact_seed_trace") && coding.contains("execute_tool"),
        "the coding seed must assert execute_tool membership in that exact trace"
    );
    assert!(
        !coding.contains("newest") && !coding.contains("--limit"),
        "a newest-N or windowed lookup would let an unrelated trace satisfy the coding seed"
    );
}

#[test]
fn coding_tool_seed_membership_gates_product_observability() {
    let rung = ladder_function("rung_local");
    assert!(
        rung.contains("seed_coding_tool_turn"),
        "the LIVE orchestration block must run the built-in coding-tool seed alongside the hosted MCP seed"
    );
    assert!(
        rung.contains("LAST_CODING_MEMBERSHIP"),
        "the coding seed must publish its membership result the way the MCP seed publishes LAST_MCP_MEMBERSHIP"
    );
    let gating = rung.lines().any(|line| {
        line.contains("LAST_CODING_MEMBERSHIP") && line.contains("product_membership=\"false\"")
    });
    assert!(
        gating,
        "a coding seed that runs without gating product_membership is inert; its membership must force product_membership=false"
    );
    assert!(
        ladder().contains("LAST_CODING_TRACE_ID=\"\""),
        "the coding seed's exact trace id must be declared beside the other LAST_*_TRACE_ID seed results"
    );
}

#[test]
fn product_collector_restore_covers_every_emitter_and_invalid_auth_is_observable() {
    let pins = ladder_function("pin_local_source_images");
    for required in [
        "CURIE_LOCAL_IMAGE_TAG:-dev",
        "export CURIE_BASE_TAG=",
        "export CURIE_RUNNER_IMAGE=",
        "export CURIE_DISPATCHER_IMAGE=",
        "curie-runner:",
        "curie-dispatcher:",
    ] {
        assert!(
            pins.contains(required),
            "raw Compose recreation loses {required}"
        );
    }
    assert!(ladder_function("rung_local").contains("pin_local_source_images"));
    let restore = ladder_function("route_local_observability_to_product_collector");
    for required in [
        "curie-api",
        "curie-dispatcher",
        "curie-worker",
        "curie-runner",
        "assert_product_collector_endpoint",
        "http/protobuf",
        "otel-collector:4318",
    ] {
        assert!(
            restore.contains(required),
            "worker-only exporter restoration can leave {required} routed to the disposable sink"
        );
    }
    assert!(
        restore.contains("export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318"),
        "product restoration must override unrelated shell or ignored-file routing"
    );
    assert!(
        restore.contains(
            "export CURIE_WORKER_OTEL_EXPORTER_OTLP_ENDPOINT=\"$PRODUCT_COLLECTOR_WORKER_ENDPOINT\""
        ),
        "host-network worker must use the selected collector host port"
    );
    assert!(
        ladder().contains("PRODUCT_COLLECTOR_WORKER_ENDPOINT=")
            && ladder().contains("http://127.0.0.1:24318"),
        "the default collector host port remains 24318 when isolation is unset"
    );
    assert!(
        restore.contains("export OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf"),
        "product restoration must pin the protocol expected by the task-owned collector"
    );
    assert!(
        !restore.contains("unset OTEL_EXPORTER_OTLP_ENDPOINT"),
        "unsetting the override permits ignored local configuration to redirect evidence"
    );

    let negative = ladder_function("case_local_langfuse_invalid_auth");
    // OTLP/HTTP forbids retries for 401; configured retry/queue settings do not
    // override that protocol rule. Issue #2204 requires an observable bounded
    // failure, not replay of a permanently rejected request.
    // https://opentelemetry.io/docs/specs/otlp/#retryable-response-codes
    for required in [
        "INVALID_LANGFUSE_OTLP_AUTH_HEADER",
        "langfuse-web",
        "otelcol_receiver_accepted_spans",
        "otelcol_exporter_send_failed_spans",
        "otelcol_exporter_queue_size",
        "assert_product_collector_permanent_auth_rejection",
        "Ready",
        "restart",
        "failed_trace_id",
    ] {
        assert!(
            negative.contains(required),
            "real pinned-Langfuse invalid-auth proof omits {required}"
        );
    }
    assert!(
        !negative.contains("down -v"),
        "the negative must not destroy the backing stack to manufacture absence"
    );
    assert!(
        !negative.contains("same_queued_trace_id"),
        "a permanent 401 rejection must not be misrepresented as durable replay"
    );

    let valid_control = negative
        .find(r#"query_exact_seed_trace local "$LAST_ORDINARY_TRACE_ID""#)
        .expect("invalid-auth proof must first read an exact known-valid trace");
    let invalidate = negative
        .find(r#"export LANGFUSE_OTLP_AUTH_HEADER="$INVALID_LANGFUSE_OTLP_AUTH_HEADER""#)
        .expect("invalid-auth proof must roll the Collector to the placeholder credential");
    assert!(
        valid_control < invalidate,
        "the valid exact-read control must precede credential invalidation"
    );

    let query = ladder_function("query_exact_seed_trace");
    let poll_start = query
        .find(r#"for attempt in $(seq 1 "$OBSERVABILITY_POLL_ATTEMPTS"); do"#)
        .expect("exact trace query must use the declared bounded poll count");
    let poll_end = poll_start
        + query[poll_start..]
            .find("\n    done")
            .expect("exact trace bounded poll must close");
    let absent_success = query
        .find("exact trace remained not-found through the full bounded observation poll")
        .expect("exact trace query must report a bounded absent verdict");
    assert!(
        absent_success > poll_end,
        "absence must stay stable for the full poll bound, not return on the first exit 1"
    );
    assert!(
        query.to_ascii_lowercase().contains("not found")
            || query.to_ascii_lowercase().contains("not_found"),
        "only a typed not-found result may satisfy absence; arbitrary exit 1 is not evidence"
    );

    let absent = negative
        .find(r#"query_exact_seed_trace local "$failed_trace_id" "" "" absent"#)
        .expect("the rejected exact ID must be checked absent while auth is invalid");
    let restored = negative
        .rfind("restore_local_langfuse_auth")
        .expect("valid auth must be restored after the failure evidence");
    let recovered = negative
        .rfind("seed_ordinary_turn local")
        .expect("a fresh healthy turn must prove exact ingestion after valid auth returns");
    assert!(
        absent < restored && restored < recovered,
        "fresh-turn recovery must follow bounded rejection evidence and credential restoration"
    );
}

#[test]
fn local_source_build_uses_the_daemon_backed_builder() {
    let (output, _, _, _) = run_local_observability_control(&[
        ("BUILDX_BUILDER", "curie-e2e-builder"),
        ("STUB_REQUIRE_DEFAULT_BUILDER", "1"),
    ]);
    let transcript = transcript(&output);
    assert!(
        output.status.success(),
        "the local source build must replace an isolated ambient builder with the Docker daemon builder: {transcript}"
    );
}

#[test]
fn local_source_build_uses_the_current_contexts_daemon_builder() {
    // Docker Desktop's context is `desktop-linux`. There `docker build`
    // refuses the `default` builder and `docker compose build` refuses
    // `desktop-linux`, and `local up --build` runs both. Addressing the
    // context's daemon through DOCKER_HOST makes `default` its builder for both.
    let (output, _, _, _) = run_local_observability_control(&[
        ("BUILDX_BUILDER", "curie-e2e-builder"),
        ("STUB_REQUIRE_DEFAULT_BUILDER", "1"),
        (
            "STUB_DOCKER_ENDPOINT",
            "unix:///Users/acme/.docker/run/docker.sock",
        ),
    ]);
    let transcript = transcript(&output);
    assert!(
        output.status.success(),
        "the local source build must select the daemon builder of the current Docker context: {transcript}"
    );
}

#[test]
fn local_source_build_refuses_an_unreadable_daemon_endpoint() {
    // An empty DOCKER_HOST is no DOCKER_HOST: the build would run in the
    // ambient context, whose builder need not be `default`.
    let (output, invocations, _, _) =
        run_local_observability_control(&[("STUB_DOCKER_ENDPOINT_EMPTY", "1")]);
    let transcript = transcript(&output);
    assert!(
        !output.status.success(),
        "an unreadable daemon endpoint must fail the rung: {transcript}"
    );
    assert!(
        transcript.contains("could not read the current Docker context's daemon endpoint"),
        "the refusal must say what it could not read: {transcript}"
    );
    assert!(
        !invocations.lines().any(is_current_source_local_up),
        "the rung must refuse before `local up --build`: {invocations}"
    );
}

#[test]
fn cluster_product_observability_is_private_preflight_and_query_only() {
    let preflight = ladder_function("preflight_cluster_product_observability");
    for required in [
        "CURIE_NAMESPACE",
        "CURIE_RELEASE",
        "default",
        "curie",
        "langfuse",
        "otel-collector",
        "api",
        "worker",
        "runner-prewarm",
        "imageID",
        "docker image inspect",
        "curie-api:local",
        "curie-worker:local",
        "curie-runner:latest",
        "mismatch",
    ] {
        assert!(
            preflight.contains(required),
            "cluster product preflight omits {required}"
        );
    }

    let query = ladder_function("run_cluster_product_observability");
    let wrapper = ladder_function("rung_cluster_product");
    for required in [
        "preflight_cluster_product_observability",
        "seed_cluster_missing_carrier_control",
        "cluster_external_ingress_seed",
        "cluster observability run",
    ] {
        assert!(
            query.contains(required),
            "cluster query mode omits {required}"
        );
    }
    for mutation in [
        "cluster up",
        "cluster down",
        "helm install",
        "helm upgrade",
        "helm uninstall",
        "kubectl delete",
    ] {
        for (helper_name, helper) in [
            ("preflight", preflight.as_str()),
            ("query", query.as_str()),
            ("product wrapper", wrapper.as_str()),
        ] {
            assert!(
                !helper.contains(mutation),
                "cluster product {helper_name} must never install, upgrade, uninstall, or delete the release: found {mutation}"
            );
        }
    }
    assert!(
        wrapper.contains("cluster deploy"),
        "the product wrapper must retain the intended agent seed deployment"
    );
    for manufactured in [
        "enqueue_cluster_carried_turn",
        "CLUSTER_SEEDED_TRACE_ID",
        "secrets.token_hex",
    ] {
        assert!(
            !ladder().contains(manufactured),
            "cluster correlation evidence must come from real Slack ingress, not harness-manufactured carrier {manufactured}"
        );
    }
    let missing_carrier = ladder_function("seed_cluster_missing_carrier_control");
    assert!(
        missing_carrier.contains("cluster message")
            && missing_carrier.contains("adjacent_traceparent=false"),
        "the dispatcher-absent cluster message path must remain an explicit executed missing-carrier compatibility negative"
    );
    let external = ladder_function("cluster_external_ingress_seed");
    for required in [
        "CURIE_E2E_CLUSTER_EXTERNAL_INGRESS_RECEIPT",
        "CURIE_E2E_PRODUCT_RUN_ID",
        "reply_observed",
        "completion_observed",
        "otelcol_receiver_accepted_spans_delta",
        "otelcol_exporter_sent_spans_delta",
        "discover_cluster_external_trace_id",
    ] {
        assert!(
            ladder().contains(required) || external.contains(required),
            "external Slack cluster evidence omits {required}"
        );
    }
    assert!(
        !preflight.contains(r#"-o json > "$inventory""#)
            && !preflight.contains("cluster-product-pods"),
        "cluster preflight must query Ready/imageID fields directly and never persist full pod JSON containing private spec/env data"
    );
}

fn selected_release_worker_target(text: &str) -> bool {
    text.contains("cluster_worker_deploy")
        || text.contains("${CURIE_RELEASE}-curie-worker")
        || text.contains("$CURIE_RELEASE-curie-worker")
        || text.contains("${CURIE_RELEASE}-worker")
        || text.contains("$CURIE_RELEASE-worker")
        || text.contains(r#""$CURIE_RELEASE-worker""#)
}

/// The cluster fake-model probe must read CURIE_FAKE_MODEL off the selected
/// release's worker. Hardcoding `curie`/`curie-worker` lets a nondefault
/// control stay green against the wrong Deployment.
#[test]
fn probe_cluster_fake_model_targets_selected_namespace_and_release() {
    let probe = ladder_function("probe_cluster_fake_model");
    assert!(
        !probe.contains("kubectl -n curie"),
        "probe_cluster_fake_model must not hardcode kubectl -n curie; a selected namespace would be ignored: {probe}"
    );
    assert!(
        !probe.contains("deployment/curie-worker"),
        "probe_cluster_fake_model must not hardcode deployment/curie-worker; a selected release would be ignored: {probe}"
    );
    assert!(
        probe.contains("$CURIE_NAMESPACE"),
        "probe_cluster_fake_model must read the selected CURIE_NAMESPACE: {probe}"
    );
    assert!(
        selected_release_worker_target(&probe),
        "probe_cluster_fake_model must target deployment/${{CURIE_RELEASE}}-worker or equivalent: {probe}"
    );
}

/// Regular cluster verbs must pass the selected namespace and release
/// explicitly. Relying on CLI defaults hides a script that forgot to thread
/// CURIE_NAMESPACE/CURIE_RELEASE, and a hardcoded worker probe cannot see a
/// nondefault target.
#[test]
fn rung_cluster_threads_selected_namespace_and_release() {
    let cluster = ladder_function("rung_cluster");
    assert!(
        !cluster.contains("kubectl -n curie"),
        "rung_cluster must not hardcode kubectl -n curie: {cluster}"
    );
    assert!(
        !cluster.contains("deployment/curie-worker"),
        "rung_cluster must not hardcode deployment/curie-worker: {cluster}"
    );
    assert!(
        cluster.contains("$CURIE_NAMESPACE") && selected_release_worker_target(&cluster),
        "rung_cluster connector probes must use the selected namespace and release worker: {cluster}"
    );
    assert!(
        cluster.contains("${ns_rel[@]}") || cluster.contains("cluster_ns_rel_args"),
        "rung_cluster must pass namespace and release through ns_rel or cluster_ns_rel_args: {cluster}"
    );
    assert!(
        !cluster.contains(r#"--json cluster status 2>/dev/null"#),
        "cluster status must pass namespace and release rather than using CLI defaults: {cluster}"
    );
    assert!(
        cluster.contains(r#"cluster status "${ns_rel[@]}""#)
            || cluster.contains("cluster_ns_rel_args"),
        "cluster status must receive the selected namespace and release: {cluster}"
    );
    assert!(
        !cluster.contains(r#"--json cluster deploy --plugin-dir"#),
        "cluster deploy must pass namespace and release before --plugin-dir: {cluster}"
    );
    assert!(
        cluster.contains(r#"cluster deploy "${ns_rel[@]}""#)
            || cluster.contains("cluster_ns_rel_args"),
        "cluster deploy must receive the selected namespace and release: {cluster}"
    );
    assert!(
        !cluster.contains(r#"local msg_args=(--json cluster message "$PROMPT")"#),
        "cluster message must prepend namespace and release rather than a flag-less argv: {cluster}"
    );
    assert!(
        cluster.contains(r#"eval_args+=("${ns_rel[@]}")"#)
            || cluster.contains("cluster_ns_rel_args"),
        "cluster eval must prepend the selected namespace and release: {cluster}"
    );
}

#[test]
fn product_sanitizer_and_message_failures_never_dump_private_json() {
    let sanitizer = ladder_function("sanitize_exact_trace_read");
    assert!(
        !sanitizer.contains("if private_fields.intersection(node):\n        pass"),
        "private-field handling must reject or remove data, not be a no-op"
    );
    for private in ["input", "output", "session", "user", "headers"] {
        assert!(
            sanitizer.contains(private),
            "sanitizer must explicitly account for private field {private}"
        );
    }

    let finalized = ladder_function("assert_finalized_reply");
    for raw_dump in [
        r#"printf '%s\n' "$payload""#,
        r#"printf "%s\n" "$payload""#,
        r#"echo "$payload""#,
    ] {
        assert!(
            !finalized.contains(raw_dump),
            "message parse/finalization failures must emit a bounded verdict, not private raw JSON"
        );
    }
}

#[test]
fn adopted_component_stop_requires_complete_available_surface_export() {
    let decision = ladder_function("classify_product_observability_owner");
    for required in [
        "raw_emitted_observations",
        "otelcol_receiver_accepted_spans",
        "otelcol_exporter_sent_spans",
        "langfuse_observation_membership",
        "local",
        "cluster",
        "image_ids_match",
        "seed_valid",
        "same_id_raw_collector_receipt",
        "run_id",
        "adopted-component",
    ] {
        assert!(
            decision.contains(required),
            "ownership classification omits prerequisite {required}"
        );
    }
    assert!(
        decision.find("otelcol_exporter_sent_spans") < decision.find("adopted-component"),
        "the harness cannot blame the adopted backend before proving export success"
    );
    assert!(
        decision.find("langfuse_observation_membership") < decision.find("adopted-component"),
        "the harness cannot blame the adopted backend before checking exact-ID membership"
    );
    for verdict in ["curie-unresolved", "curie-owned", "adopted-component"] {
        let tail = decision
            .split(verdict)
            .nth(1)
            .unwrap_or_else(|| panic!("classifier omits verdict {verdict}"));
        assert!(
            tail.contains("SystemExit(1)"),
            "blocking verdict {verdict} must terminate the supported run nonzero"
        );
    }
    assert!(
        decision.contains("curie-clear") && decision.contains("SystemExit(0)"),
        "only complete exact membership for every exercised surface may exit zero"
    );
}

#[test]
fn live_local_release_rung_grades_its_own_weather_cases_copy() {
    let text = ladder();
    let contract = r#"assert_finalized_reply "local-release" "$out"

    echo
    echo "=== curie local eval --dry-run (suite parity, release compose stack) ==="
    local eval_args=(local eval)
    if [[ ! -f "$WORKDIR/bundle-release/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle-release/evals/cases.json")
    fi
    assert_suite "local-release" "$(cd "$WORKDIR/bundle-release" && "$BIN" --json "${eval_args[@]}" --dry-run)"

    if [[ "$LIVE" == "1" ]]; then
        echo
        echo "=== curie local eval (release compose stack) ==="
        (cd "$WORKDIR/bundle-release" && "$BIN" "${eval_args[@]}")
    fi"#;
    assert!(
        text.contains(contract),
        "the live local release rung must run local eval after its plumbing \
         assertion against its own deployed weather bundle cases copy, with \
         the suite-parity dry-run check in front of it; \
         ladder contents:\n{text}"
    );
}

#[test]
fn live_cluster_rung_runs_weather_cases_with_the_message_listen_host() {
    let text = ladder();
    // The shared `eval_args` array and the dry-run call it feeds. Scoped to the
    // array plus that one call rather than the whole block, because the block
    // carries the #1602/#1603 rationale comments and a reworded comment must not
    // red this contract.
    let contract = r#"local eval_args=(cluster eval)
    eval_args+=("${ns_rel[@]}")
    if [[ ! -f "$WORKDIR/bundle/evals/trajectory.json" ]]; then
        eval_args+=(--cases "$WORKDIR/bundle/evals/cases.json")
    fi
    if [[ -n "${CURIE_E2E_LISTEN_HOST:-}" ]]; then
        eval_args+=(--listen-host "$CURIE_E2E_LISTEN_HOST")
    fi
    assert_suite "cluster" "$(cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}" --dry-run)""#;
    assert!(
        text.contains(r#"assert_finalized_reply "cluster" "$out""#),
        "the live cluster rung must keep its plumbing assertion; ladder \
         contents:\n{text}"
    );
    assert!(
        text.contains(contract),
        "the live cluster rung must resolve its suite through one shared \
         eval_args array and forward the message listen host to the \
         suite-parity dry-run; ladder contents:\n{text}"
    );
    assert!(
        text.contains(r#"if ! (cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}"); then"#),
        "the live cluster rung must still RUN cluster eval against the deployed \
         weather bundle cases and forward the message listen host -- through the \
         SAME array as the dry-run, so neither call can lose it -- and report \
         only (#1603) means the grade is non-fatal, never that the step was \
         deleted; ladder contents:\n{text}"
    );
}

/// The cluster rung's eval must run in `--json` mode. The human table prints a
/// reply only for a RED case, so a green would carry no evidence of HOW it was
/// earned. The weather case now requires the fetch capability through its
/// trajectory sidecar; its regex grades answer formatting only and cannot pass
/// the full case alone. The json payload carries `output` for every case, pass
/// included, which is what keeps the report only grade auditable in
/// the job log. Asserted for the cluster rung alone, since the sibling
/// assertions above pin the other rungs to the plain table.
///
/// The second half is the reconciliation of #1602 with the suite-parity dry-run
/// that shares this rung's `eval_args`: BOTH calls need `--json` (auditability of
/// a green here, a machine-readable plan there), so it is passed at each call
/// site and must stay OUT of the shared array -- inside it, every invocation
/// would carry `--json` twice.
#[test]
fn live_cluster_rung_emits_the_graded_reply_for_passing_cases() {
    let text = ladder();
    assert!(
        text.contains(r#"if ! (cd "$WORKDIR/bundle" && "$BIN" --json "${eval_args[@]}"); then"#),
        "the live cluster rung must run its eval with `--json` so a PASSING \
         case's reply text lands in the job log; without it a dishonest green \
         (a fabricated temperature) is indistinguishable from an honest one; \
         ladder contents:\n{text}"
    );
    assert!(
        !text.contains(r#"local eval_args=(--json cluster eval"#),
        "`--json` must be passed at each cluster eval call site, never stored in \
         the shared eval_args array: in the array both the dry-run and the live \
         grade would pass it twice, and neither invocation would be the one the \
         executing controls below drive; ladder contents:\n{text}"
    );
}

#[test]
fn cluster_rung_uses_the_worker_delivery_budget_without_weakening_reply_gates() {
    let timeout_reader = ladder_function("cluster_reply_timeout_seconds");
    assert!(
        timeout_reader.contains(r#"kubectl -n "$CURIE_NAMESPACE""#)
            && timeout_reader.contains("cluster_worker_deploy")
            && timeout_reader.contains("-o json"),
        "the timeout reader must inspect the selected worker Deployment as JSON; helper contents:\n{timeout_reader}"
    );
    assert!(
        timeout_reader.contains("CURIE_DELIVERY_BUDGET_S"),
        "the timeout reader must read CURIE_DELIVERY_BUDGET_S from the selected worker Deployment; helper contents:\n{timeout_reader}"
    );

    let cluster = ladder_function("rung_cluster");
    assert!(
        cluster.contains("#1534 repeated cluster eval then message still claims"),
        "the cluster rung must run repeated eval suites then a message so \
         retained eval sandboxes cannot exhaust the default ResourceQuota; \
         rung contents:\n{cluster}"
    );
    assert!(
        cluster.contains(
            r#"retention_thread="$(python3 -c 'import time; now = time.time_ns(); print(f"{now // 1_000_000_000}.{(now // 1_000) % 1_000_000:06d}")')""#,
        ),
        "the post-eval turn needs a unique timestamp-shaped explicit thread so \
         its worker claim log cannot be borrowed from an earlier turn; rung \
         contents:\n{cluster}"
    );
    let timeout_resolved = cluster
        .find(r#"cluster_reply_timeout_seconds="$(cluster_reply_timeout_seconds)""#)
        .expect("the cluster rung must resolve the selected worker delivery budget once");
    let first_message = cluster
        .find(r#"msg_args+=(--timeout-secs "$cluster_reply_timeout_seconds")"#)
        .expect("the first cluster message must use the resolved worker delivery timeout");
    let retention_message = cluster
        .find(
            r#"retention_args+=(--thread "$retention_thread" --timeout-secs "$cluster_reply_timeout_seconds")"#,
        )
        .expect("the post eval cluster message must use the same resolved worker delivery timeout");
    assert!(
        timeout_resolved < first_message && first_message < retention_message,
        "the worker delivery timeout must be resolved before the first enqueue and reused for the post eval message; rung contents:\n{cluster}"
    );
    assert!(
        !cluster.contains(r#"timeout 45 "$BIN" "${retention_args[@]}""#),
        "a 45 second process timeout conflates claim capacity with model latency; \
         rung contents:\n{cluster}"
    );
    let message_finished = cluster
        .find(r#"retention_out="$("$BIN" "${retention_args[@]}")"#)
        .expect("the post eval message must finish under the worker delivery timeout");
    let claim_proof = cluster
        .find(r#"assert_retention_claim "$retention_log" "slack:$retention_channel:$retention_thread" "$retention_launch_epoch""#)
        .expect("the completed message must be tied to its own bounded worker claim");
    let finalized_assertion = cluster
        .find(r#"if ! assert_finalized_reply "cluster" "$retention_out"; then"#)
        .expect("the post-eval message must still validate its finalized reply");
    assert!(
        message_finished < claim_proof && claim_proof < finalized_assertion,
        "the posthoc claim proof must judge the completed turn before its reply is \
         accepted; otherwise a slow model or an unrelated worker line can hide a \
         failed claim; rung contents:\n{cluster}"
    );
}

#[test]
fn cluster_context_precedes_message_in_the_real_cli_grammar() {
    let output = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args([
            "--json",
            "cluster",
            "--context",
            "ctx",
            "message",
            "retention",
            "--help",
        ])
        .output()
        .expect("run cluster message help through the compiled CLI");
    let help = transcript(&output);
    assert!(
        output.status.success(),
        "a cluster scoped context must parse before message: {help}"
    );
    assert!(
        help.contains("Drive the deployed Kubernetes release end to end"),
        "the parsed command must reach cluster message help: {help}"
    );
}

fn run_retention_claim_assertion(log: &str) -> Output {
    let harness = tempfile::tempdir().expect("create retention claim harness directory");
    let log_file = harness.path().join("worker.log");
    fs::write(&log_file, log).expect("write timestamped worker log fixture");
    let helper = ladder_function("assert_retention_claim");
    let script = format!(
        "set -euo pipefail\n{helper}\nassert_retention_claim {} {} 1735689600\n",
        sh_single_quote(&log_file),
        sh_single_quote(Path::new("slack:C0LOCALDEV:1735689600.000001")),
    );
    Command::new("bash")
        .arg("-c")
        .arg(script)
        .output()
        .expect("run retention claim assertion")
}

#[test]
fn retention_claim_assertion_accepts_only_the_exact_timely_worker_claim() {
    let expected_thread = "slack:C0LOCALDEV:1735689600.000001";
    let timely = run_retention_claim_assertion(
        r#"2025-01-01T00:00:44Z {"logger":"curie_worker.kernel","message":"claim latency for slack:C0LOCALDEV:1735689600.000001: 44 ms","timestamp":"2025-01-01T00:00:44+00:00"}
"#,
    );
    let timely_transcript = transcript(&timely);
    assert!(
        timely.status.success(),
        "a claim inside the first 45 seconds must pass even when reply timing is \
         outside this helper's proof: {timely_transcript}"
    );

    for (case, log) in [
        (
            "missing",
            r#"2025-01-01T00:00:44Z {"logger":"curie_worker.kernel","message":"worker completed another operation","timestamp":"2025-01-01T00:00:44+00:00"}
"#,
        ),
        (
            "unrelated",
            r#"2025-01-01T00:00:44Z {"logger":"curie_worker.kernel","message":"claim latency for slack:C0LOCALDEV:1735689600.000002: 1 ms","timestamp":"2025-01-01T00:00:44+00:00"}
"#,
        ),
        (
            "wrong logger",
            r#"2025-01-01T00:00:44Z {"logger":"other","message":"claim latency for slack:C0LOCALDEV:1735689600.000001: 1 ms","timestamp":"2025-01-01T00:00:44+00:00"}
"#,
        ),
        (
            "late",
            r#"2025-01-01T00:00:46Z {"logger":"curie_worker.kernel","message":"claim latency for slack:C0LOCALDEV:1735689600.000001: 1 ms","timestamp":"2025-01-01T00:00:46+00:00"}
"#,
        ),
        (
            "overlong",
            r#"2025-01-01T00:00:44Z {"logger":"curie_worker.kernel","message":"claim latency for slack:C0LOCALDEV:1735689600.000001: 45000 ms","timestamp":"2025-01-01T00:00:44+00:00"}
"#,
        ),
    ] {
        let output = run_retention_claim_assertion(log);
        let output_transcript = transcript(&output);
        assert_ne!(
            output.status.code(),
            Some(0),
            "a {case} worker claim must not prove post-eval capacity: {output_transcript}"
        );
        assert!(
            output_transcript.contains(expected_thread),
            "the {case} rejection must name the exact missing or invalid claim \
             key: {output_transcript}"
        );
    }
}

// --- Assertion group 6: the EXECUTING parity controls -----------------------
//
// Everything below runs the real `cli/scripts/e2e-ladder.sh` against a stub
// `curie`, `docker` and `kubectl`, so it exercises the ladder's own parity
// helpers rather than reading its source. No Docker daemon, no cluster, no
// credential (the one live-mode control supplies a dummy CURIE_CREDENTIALS,
// which apply_model_mode only checks for presence).
//
// The design that makes each negative control attributable: ONE stub body,
// configured entirely through STUB_* env vars, so every negative control is the
// positive control's configuration with exactly ONE knob moved. A non-zero exit
// therefore cannot come from an unrelated breakage -- the positive control
// proves the un-moved configuration is green -- and each control additionally
// requires the operator-facing message to name the divergence it injected.
//
// All of these assert on exit codes and operator-facing message text, never on
// the ladder's internal shell identifiers, so renaming `PARITY_DIGEST` or
// `assert_bundle_identity` leaves them passing.

/// The ids the stub deploy receipt reports. They are also what the stubbed
/// `GET /deployments` read must agree with, which is the whole point of the
/// sole-active-deployment control.
const AGENT_ID: &str = "6f3d1c2a-0000-4000-8000-000000000001";
const VERSION_ID: &str = "6f3d1c2a-0000-4000-8000-000000000002";
const DEPLOYMENT_ID: &str = "6f3d1c2a-0000-4000-8000-000000000003";
/// The stale `prod` row the second-active-deployment control injects. `prod`
/// because the worker's runtime binding prefers it over recency, so this is the
/// exact row that would silently serve the turn while the ladder reported the
/// digest of the `dev` bundle it just uploaded.
const STALE_PROD_DEPLOYMENT_ID: &str = "6f3d1c2a-0000-4000-8000-000000000099";

/// The default real-looking OTel trace id returned by the observability-runs
/// stub for controls that exercise the whole parity ladder. The dedicated
/// observability controls override it with a per-run value, so the local rung
/// must discover the id instead of hard-coding this fixture.
const OBSERVABILITY_TRACE_ID: &str = "0123456789abcdef0123456789abcdef";
/// The syntactically valid but absent trace used for the required 404 control.
/// It must differ from the discovered trace while retaining the same wire
/// shape, so the negative proves "unknown", not client-side validation.
const UNKNOWN_OBSERVABILITY_TRACE_ID: &str = "ffffffffffffffffffffffffffffffff";
const UNAVAILABLE_API_URL: &str = "http://127.0.0.1:1";

/// A digest value pinned by the digest-divergence control at the local rung.
const PINNED_DIGEST: &str = "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1";
/// The divergent digest the cluster rung reports in the digest-divergence
/// control.
const DIVERGENT_DIGEST: &str = "b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2b2";

/// The stub `curie`, `docker` and `kubectl` the executing controls drive the
/// ladder with. One body for all of them: behavior comes from STUB_* env vars
/// so the tests differ by configuration, not by a second stub.
///
/// Every arm matches only the argv shape the scripts actually issue. An
/// invocation variant nobody issues falls through to `exit 97`, deliberately: an
/// arm that tolerates a shape the script does not use is a compatibility path,
/// and it would keep a control green while hiding the regression that started
/// issuing that shape (the `--json`-less deploy is the exact case -- the stub
/// answers with JSON either way, so tolerating it would hide a deploy that
/// stopped asking for a receipt).
fn write_ladder_stubs(dir: &Path) {
    fs::write(
        dir.join("deploy-provider-wire.json"),
        include_str!("data/deploy-provider-wire.json"),
    )
    .expect("write deploy provider fixture");

    write_executable(
        &dir.join("curie"),
        r#"#!/bin/sh
set -u

if [ -n "${STUB_INVOCATION_LOG:-}" ]; then
    printf '%s\n' "$*" >> "$STUB_INVOCATION_LOG"
fi

# `--plugin-dir <dir>` is the last pair on every deploy the ladder makes, so the
# final argument is the bundle directory that rung packed.
bundle_dir=""
for arg in "$@"; do bundle_dir="$arg"; done

# The `--name <name>` value, read off argv: the #747 leftover-runner case asserts
# that `skill up`'s refusal names the exact container an operator must clear.
# `--namespace` / `--release` are parsed the same way so cluster arms can refuse
# a selected target that does not match the control.
name=""
namespace=""
release=""
context=""
channel=""
thread_key=""
timeout_secs=""
observability_start=""
observability_end=""
prev=""
for arg in "$@"; do
    if [ "$prev" = "--name" ]; then name="$arg"; fi
    if [ "$prev" = "--namespace" ]; then namespace="$arg"; fi
    if [ "$prev" = "--release" ]; then release="$arg"; fi
    if [ "$prev" = "--context" ]; then context="$arg"; fi
    if [ "$prev" = "--channel" ]; then channel="$arg"; fi
    if [ "$prev" = "--thread" ]; then thread_key="$arg"; fi
    if [ "$prev" = "--timeout-secs" ]; then timeout_secs="$arg"; fi
    if [ "$prev" = "--start" ]; then observability_start="$arg"; fi
    if [ "$prev" = "--end" ]; then observability_end="$arg"; fi
    prev="$arg"
done

# Cluster arms require the flags the ladder now always passes. A mismatch against
# STUB_EXPECT_* (default curie/curie) is unexpected, so a hardcoded curie
# invocation cannot satisfy a nondefault control.
require_expected_ns_rel() {
    expect_ns="${STUB_EXPECT_NAMESPACE:-curie}"
    expect_rel="${STUB_EXPECT_RELEASE:-curie}"
    if [ -z "$namespace" ] || [ -z "$release" ] \
        || [ "$namespace" != "$expect_ns" ] || [ "$release" != "$expect_rel" ]; then
        echo "unexpected curie invocation: $*" >&2
        exit 97
    fi
}

require_retention_context() {
    expect_context="${STUB_EXPECT_CONTEXT:-stub-context}"
    if [ "$context" != "$expect_context" ]; then
        echo "unexpected retention context: $context" >&2
        exit 97
    fi
}

require_parent_retention_context() {
    expect_context="${STUB_EXPECT_CONTEXT:-stub-context}"
    case "$*" in
        "--json cluster --context $expect_context message "*)
            ;;
        *)
            echo "retention context must be scoped to cluster before message: $*" >&2
            exit 97
            ;;
    esac
}

require_retention_channel() {
    expect_channel="${STUB_EXPECT_CHANNEL:-C0LOCALDEV}"
    if [ "$channel" != "$expect_channel" ]; then
        echo "unexpected retention channel: $channel" >&2
        exit 97
    fi
}

# The production ladder derives one UTC window around the just-completed turn.
# Validate the values, not merely the presence of two argv tokens: exact
# second-resolution UTC syntax, a two-hour -1h/+1h span, and a midpoint close
# to this process's current time. Summary and series echo these exact bounds,
# so the ladder's DTO validators also prove the API returned the requested
# window rather than a default.
validate_observability_window() {
    python3 -c 'from datetime import datetime, timedelta, timezone
import sys
try:
    start = datetime.strptime(sys.argv[1], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    end = datetime.strptime(sys.argv[2], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
except (ValueError, IndexError):
    sys.exit(1)
if end - start != timedelta(hours=2):
    sys.exit(1)
midpoint = start + (end - start) / 2
if abs((datetime.now(timezone.utc) - midpoint).total_seconds()) > 600:
    sys.exit(1)' "$1" "$2"
}

# A CONTENT-derived digest, not a canned one: the default makes two rungs that
# packed the same tree agree by construction and two rungs that packed different
# trees disagree by construction. That is what lets the case-ids-only control
# prove the digest carries the case-id claim, and what lets the positive control
# compare three independently derived digests instead of one canned constant.
#
# Hashes path plus bytes for every regular file, mtimes excluded (the ladder
# normalizes those itself) and `.curie` excluded (the real packer excludes it,
# cli/src/bundle.rs). The Rust side computes the same value for the pristine
# weather bundle in weather_bundle_sha256(); the two must stay in step.
sha_of_bundle() {
    python3 -c 'import hashlib, os, sys
root = sys.argv[1]
digest = hashlib.sha256()
for dirpath, dirnames, filenames in os.walk(root):
    dirnames[:] = sorted(d for d in dirnames if d != ".curie")
    for entry in sorted(filenames):
        path = os.path.join(dirpath, entry)
        digest.update(os.path.relpath(path, root).encode())
        digest.update(open(path, "rb").read())
print(digest.hexdigest())' "$1"
}

# The `deploy_result` branch of cli/schema/deploy.schema.json, with every field
# the ladder reads: bundle.sha256, agent.id, version.id, and
# deployment.{id,environment,status}.
emit_deploy() {
    python3 -c 'import json, sys
fixture = json.load(open(sys.argv[1]))
fixture["agent"]["id"] = sys.argv[2]
fixture["version"]["id"] = sys.argv[3]
fixture["deployment"]["id"] = sys.argv[4]
fixture["bundle"]["sha256"] = sys.argv[5]
print(json.dumps(fixture, separators=(",", ":")))' \
        "$STUB_STATE/deploy-provider-wire.json" \
        "$STUB_AGENT_ID" \
        "$STUB_VERSION_ID" \
        "$STUB_DEPLOYMENT_ID" \
        "$1"
}

# The DryRunPlan shape (cli/src/ui.rs: {"dry_run":true,"plan":[lines]}) carrying
# the one plan line the tier's suite loader emits.
emit_plan() {
    printf '{"dry_run":true,"plan":["grade %s case(s) from suite \\"%s\\" against the %s tier"]}\n' "$2" "$3" "$1"
}

case "$*" in
    "--version")
        echo "curie test harness"
        ;;
    "--json try")
        # The keyless first run is deliberately disposable: its finalized fake
        # reply is the only observable result, so no project may be retained
        # in the caller's clean directory.
        printf '%s\n' '{"status":"done","finalized":true,"reply":"all done"}'
        ;;
    "try")
        # The credential-discovery E2E supplies a known-invalid credential.
        # Name its source without ever expanding its value, and create neither
        # a scaffold nor a runner before the rejection.
        echo "error: discovered credential source ANTHROPIC_API_KEY was rejected" >&2
        exit 1
        ;;
    "try --keep")
        # Graduation keeps the standard plugin shape, then the normal skill
        # commands below operate on this directory exactly as they do for a
        # user-created project. A nonempty directory -- a previous generated
        # bundle or unowned user files -- must refuse before writing (#2423).
        if [ -e curie-demo ] && [ -n "$(find curie-demo -mindepth 1 -print -quit 2>/dev/null)" ]; then
            echo "error: refusing to recreate curie-demo: directory exists and is not empty" >&2
            exit 1
        fi
        mkdir -p curie-demo/.claude-plugin
        printf '%s\n' '{"name":"curie-demo"}' > curie-demo/.claude-plugin/plugin.json
        echo "stub try all done"
        ;;
    "--json cluster status --namespace "*" --release "*)
        # The case-ids-only control's single injection point: after the local
        # rung deployed and before the cluster rung does, rewrite ONLY the case
        # ids in the bundle both rungs deploy. Suite name and case count are
        # untouched, so every dry-run plan line still matches exactly and the
        # digest is the only thing that moves.
        if [ "${STUB_MUTATE_CASE_IDS:-0}" = "1" ] && [ -f "$STUB_STATE/last_plugin_dir" ]; then
            python3 -c 'import json,sys
p = sys.argv[1]
d = json.load(open(p))
for i, c in enumerate(d["cases"]):
    c["id"] = "drifted-case-%d" % i
json.dump(d, open(p, "w"))' "$(cat "$STUB_STATE/last_plugin_dir")/evals/cases.json"
        fi
        require_expected_ns_rel "$@"
        if [ "${STUB_CLUSTER_RELEASE_FOUND:-1}" = "0" ]; then
            printf '%s\n' '{"release_found":false}'
        else
            printf '%s\n' '{"release_found":true}'
        fi
        ;;
    "skill up --plugin-dir . --image curie-runner --port 7245 --name curie-e2e-runner --fake-model")
        if [ -n "${STUB_PRIMARY_SKILL_UP_ERROR:-}" ]; then
            printf '%s\n' "$STUB_PRIMARY_SKILL_UP_ERROR" >&2
            exit 7
        fi
        # The skill rung's identity surface. `skill up` packs an IMMUTABLE
        # snapshot of the source as it stands now and the runner keeps it for its
        # whole lifetime, so the digest is recorded here and replayed by
        # `skill status --json` below. That is what makes e2e.sh's two #1087 legs
        # resolve the way the real CLI resolves them: a host edit after boot is
        # invisible to the running runner, and a re-up on the edited source packs
        # a NEW digest.
        #
        # A second `up` after a source edit (#1905) also prints the replacement
        # line e2e.sh greps for, and snapshots SKILL.md so `docker exec` of the
        # /plugin mount sees the bytes packed at THIS up, not the live host file.
        if [ -f "$STUB_STATE/skill_digest" ]; then
            echo "bundle changed: replacing the recorded runner 'curie-e2e-runner' first"
        fi
        printf '%s' "$(sha_of_bundle "$PWD")" > "$STUB_STATE/skill_digest"
        skill=$(find "$PWD/skills" -mindepth 2 -maxdepth 2 -name SKILL.md | sort | head -1)
        if [ -n "$skill" ]; then
            cp "$skill" "$STUB_STATE/mounted_skill.md"
        fi
        echo "stub: runner up"
        # e2e.sh asserts the boot panel NAMES the model path it resolved, and
        # that it does not cry wolf about a missing credential. This arm is the
        # sealed argv, so the sealed row is the honest answer to it.
        echo "  Model    fake (offline, no credential)"
        ;;
    "skill up --fake-model")
        # The graduated demo uses the unadorned normal command. It records a
        # bundle digest and runner state so its status and teardown checks
        # exercise the same state transition as a regular project.
        mkdir -p .curie
        printf '%s' "$(sha_of_bundle "$PWD")" > "$STUB_STATE/skill_digest"
        printf '%s\n' '{"runner":"stub"}' > .curie/runner.json
        echo "stub: runner up"
        echo "  Model    fake (offline, no credential)"
        ;;
    "skill up --fake-model --plugin-dir "*"/bundle-hermetic --name curie-ladder-hermetic-"*)
        # The hermetic negative (ADR 0113): a scratch bundle declaring no
        # hosted connector boots cleanly. Must sit ABOVE the #747 arm, whose
        # pattern also matches this argv; the case then reads the session off
        # `docker inspect` and sweeps for connector containers, both answered
        # by the docker stub below.
        echo "stub: hermetic runner up"
        ;;
    "skill up --fake-model --plugin-dir "*" --name "*)
        # The #747 leftover-runner case: a taken container name must be a usage
        # refusal (exit 2) carrying the operator's own remedy, never docker's raw
        # "name is already in use" text.
        echo "error: container name conflict: a container named '$name' already exists." >&2
        echo "fix: curie skill down --name $name, then re-run." >&2
        exit 2
        ;;
    "skill status")
        echo "stub: runner running"
        ;;
    "skill status --json")
        printf '{"bundle_digest":"%s"}\n' "$(cat "$STUB_STATE/skill_digest")"
        ;;
    "skill message "*)
        echo "stub skill all done reply"
        ;;
    "--json skill eval")
        # The bundle's OWN suite, read off the bundle e2e.sh cd'd into, because
        # the skill rung is the one rung that reads its case ids back directly.
        # Sealed shape only (ADR-0055: a fake turn is reported as the non-graded
        # plumbing_ok and nothing is graded), which is what the one control that
        # drives this rung runs as; a live skill run would fall on e2e.sh's own
        # live-posture assertion rather than be silently accepted here.
        python3 -c 'import json, sys
suite = json.load(open(sys.argv[1]))
ids = [case["id"] for case in suite["cases"]]
print(json.dumps({
    "suite": suite["name"],
    "total": len(ids),
    "passed": 0,
    "failed": 0,
    "plumbing_ok": len(ids),
    "cases": [{"id": case_id, "status": "plumbing_ok"} for case_id in ids],
}))' "$PWD/evals/cases.json"
        ;;
    "skill down")
        rm -f .curie/runner.json
        echo "stub: runner down"
        ;;
    "skill down --name "*)
        echo "stub: removed the leftover runner named '$name'"
        ;;
    "local up --minimal")
        echo "stub: compose stack up"
        ;;
    "local up --project "*|"local up -f "*/compose.dev.yaml" --build")
        if [ "${STUB_REQUIRE_DEFAULT_BUILDER:-0}" = "1" ] \
            && { [ "${BUILDX_BUILDER:-}" != "default" ] \
                || [ "${DOCKER_HOST:-}" != "${STUB_DOCKER_ENDPOINT:-unix:///var/run/docker.sock}" ]; }; then
            echo "local source build did not select the Docker daemon builder" >&2
            exit 97
        fi
        echo "stub: full compose stack up"
        ;;
    "--json local deploy --plugin-dir "*)
        printf '%s' "$bundle_dir" > "$STUB_STATE/last_plugin_dir"
        if [ "${STUB_LOCAL_DEPLOY_REFUSAL:-0}" = "1" ]; then
            printf '%s\n' '{"error":"refusing connector deploy: missing approval route sre-approvals","fix":"curie local approvals acme-bot --route-resolution sre-approvals=C0LOCALDEV"}'
            exit 2
        fi
        emit_deploy "${STUB_LOCAL_SHA256:-$(sha_of_bundle "$bundle_dir")}"
        ;;
    "--json cluster deploy --namespace "*" --release "*" --plugin-dir "*)
        require_expected_ns_rel "$@"
        printf '%s' "$bundle_dir" > "$STUB_STATE/last_plugin_dir"
        emit_deploy "${STUB_CLUSTER_SHA256:-$(sha_of_bundle "$bundle_dir")}"
        ;;
    "--json cluster --context ${STUB_EXPECT_CONTEXT:-stub-context} surfaces $STUB_AGENT_ID --namespace "*)
        require_expected_ns_rel "$@"
        require_retention_context
        printf '{"agent":"%s","surfaces":[{"kind":"slack","address":"%s"}],"changed":false}\n' \
            "$STUB_AGENT_ID" "${STUB_EXPECT_CHANNEL:-C0LOCALDEV}"
        ;;
    "--json local message "*)
        printf '%s\n' '{"finalized":true,"reply":"stub local weather reply"}'
        ;;
    "--json cluster --context ${STUB_EXPECT_CONTEXT:-stub-context} message "*|"--json cluster message "*)
        require_expected_ns_rel "$@"
        if [ "$timeout_secs" != "${STUB_EXPECT_REPLY_TIMEOUT_SECS:-660}" ]; then
            echo "unexpected cluster reply timeout: ${timeout_secs:-missing}" >&2
            exit 97
        fi
        if [ -n "$thread_key" ]; then
            require_parent_retention_context "$@"
            require_retention_context
            require_retention_channel
            printf '%s' "$thread_key" > "$STUB_STATE/retention-thread"
            printf '%s' "$channel" > "$STUB_STATE/retention-channel"
        fi
        if [ -n "$thread_key" ] && [ "${STUB_RETENTION_REPLY_FINALIZED:-1}" = "0" ]; then
            printf '%s\n' '{"finalized":false,"reply":"stub cluster weather reply"}'
        else
            printf '%s\n' '{"finalized":true,"reply":"stub cluster weather reply"}'
        fi
        if [ -n "$thread_key" ] && [ "${STUB_RETENTION_MESSAGE_EXIT:-0}" != "0" ]; then
            exit "$STUB_RETENTION_MESSAGE_EXIT"
        fi
        ;;
    "--json local observability runs --limit 100")
        printf '{"limit":100,"count":1,"runs":[{"id":"%s","name":"curie-run","timestamp":"2026-08-22T12:34:56Z"}]}\n' \
            "$STUB_OBSERVABILITY_TRACE_ID"
        if [ "${STUB_RUNS_EXTRA_JSON:-0}" = "1" ]; then
            printf '%s\n' '{"unexpected":"second JSON object"}'
        fi
        ;;
    "--json local observability runs --limit 1 --agent-id $STUB_AGENT_ID")
        # The unavailable-API negative deliberately keeps the agent-filtered
        # candidate query while CURIE_API_URL points at a closed loopback
        # endpoint. That makes the semantic exit-code proof about transport
        # failure, not about a test-only command shape the real CLI never uses.
        if [ "${CURIE_API_URL:-}" = "$STUB_UNAVAILABLE_API_URL" ]; then
            if [ -n "${STUB_UNAVAILABLE_MARKER:-}" ]; then
                printf '%s\n' called > "$STUB_UNAVAILABLE_MARKER"
            fi
            if [ "${STUB_UNAVAILABLE_NO_FIX:-0}" = "1" ]; then
                printf '%s\n' '{"error":"Curie API is unavailable"}'
            else
                printf '%s\n' '{"error":"Curie API is unavailable","fix":"start the local stack or pass a reachable --api-url"}'
            fi
            exit "${STUB_UNAVAILABLE_EXIT:-3}"
        fi
        printf '%s\n' '{"error":"the unavailable-API control unexpectedly reached the live stub","fix":"use the closed control URL"}'
        exit 97
        ;;
    "--json local observability run $STUB_OBSERVABILITY_TRACE_ID")
        printf '{"trace":{"id":"%s","name":"agent-%s","metadata":{"session_id":"session-observability-control","terminal_outcome":"completed"}},"tree":[],"sandbox_id":"sandbox-observability-control","approval_decision":null}\n' \
            "$STUB_OBSERVABILITY_TRACE_ID" "$STUB_AGENT_ID"
        ;;
    "--json local observability run $STUB_UNKNOWN_OBSERVABILITY_TRACE_ID")
        if [ "${STUB_UNKNOWN_TRACE_NO_FIX:-0}" = "1" ]; then
            printf '%s\n' '{"error":"trace was not found"}'
        else
            printf '%s\n' '{"error":"trace was not found","fix":"list recent traces with `curie local observability runs --limit 20`"}'
        fi
        exit "${STUB_UNKNOWN_TRACE_EXIT:-1}"
        ;;
    "--json local observability metrics --start "*" --end "*)
        validate_observability_window "$observability_start" "$observability_end" || exit 96
        printf '{"start":"%s","end":"%s","runs":1,"latency_p95_ms":12.5,"tokens":42,"cost_usd":0.01,"cost_known":true,"error_rate":0.0}\n' \
            "$observability_start" "$observability_end"
        ;;
    "--json local observability metrics --metric runs --granularity hour --start "*" --end "*)
        validate_observability_window "$observability_start" "$observability_end" || exit 96
        printf '{"metric":"runs","granularity":"hour","start":"%s","end":"%s","points":[{"ts":"%s","value":1.0}]}\n' \
            "$observability_start" "$observability_end" "$observability_start"
        ;;
    "--json local eval --cases "*--dry-run*)
        emit_plan local 1 weather
        ;;
    "--json local eval --dry-run")
        emit_plan local 1 weather
        ;;
    "--json cluster eval --namespace "*" --release "*" --cases "*--dry-run*)
        require_expected_ns_rel "$@"
        # Only the cluster count is an override: it is the knob the suite
        # divergence control moves. Everything else is the constant the weather
        # bundle declares.
        emit_plan cluster "${STUB_CLUSTER_PLAN_COUNT:-1}" weather
        ;;
    "--json cluster eval --namespace "*" --release "*" --listen-host "*--dry-run*)
        require_expected_ns_rel "$@"
        emit_plan cluster "${STUB_CLUSTER_PLAN_COUNT:-1}" weather
        ;;
    "--json cluster eval --namespace "*" --release "*" --dry-run")
        require_expected_ns_rel "$@"
        emit_plan cluster "${STUB_CLUSTER_PLAN_COUNT:-1}" weather
        ;;
    # The two LIVE grades, and they are deliberately asymmetric: the local rung
    # runs the plain human table, the cluster rung runs `--json` so a passing
    # case's reply is auditable in the job log (#1602). The dry-run arms above
    # must stay above this one, since `--json cluster eval --cases *` matches a
    # dry-run argv too and the first matching arm wins. Flag-less cluster eval
    # is unexpected: the ladder always passes namespace and release.
    "local eval --cases "*|"local eval")
        if [ -n "${STUB_EVAL_MARKER:-}" ]; then
            printf '%s\n' called > "$STUB_EVAL_MARKER"
        fi
        exit "${STUB_EVAL_EXIT:-0}"
        ;;
    "--json cluster eval --namespace "*" --release "*" --cases "*|"--json cluster eval --namespace "*" --release "*)
        require_expected_ns_rel "$@"
        if [ -n "${STUB_EVAL_MARKER:-}" ]; then
            printf '%s\n' called > "$STUB_EVAL_MARKER"
        fi
        exit "${STUB_EVAL_EXIT:-0}"
        ;;
    "local down --project "*|"local down -f "*/compose.dev.yaml)
        echo "stub: compose stack down"
        ;;
    *)
        echo "unexpected curie invocation: $*" >&2
        exit 97
        ;;
esac
"#,
    );

    // The ladder's raw-docker uses are "is a stack already running",
    // "assert nothing survived", and e2e.sh's /plugin mount proof. An
    // unrecognized invocation returning nothing is the honest default; the
    // reads that carry a real answer (compose-worker selection, env inspect,
    // and the snapshotted SKILL.md) get explicit arms.
    write_executable(
        &dir.join("docker"),
        r#"#!/bin/sh
set -u
if [ -n "${STUB_DOCKER_INVOCATION_LOG:-}" ]; then
    printf '%s\n' "$*" >> "$STUB_DOCKER_INVOCATION_LOG"
fi
case "$*" in
    "context inspect --format {{.Endpoints.docker.Host}}")
        # The daemon the current context names.
        if [ "${STUB_DOCKER_ENDPOINT_EMPTY:-0}" = "1" ]; then
            echo
        else
            printf '%s\n' "${STUB_DOCKER_ENDPOINT:-unix:///var/run/docker.sock}"
        fi
        ;;
    "inspect curie-runner-local")
        # e2e.sh's ownership precondition: the standard interactive runner is
        # absent in this isolated harness unless a control explicitly says
        # otherwise.
        exit 1
        ;;
    "inspect curie-ladder-hermetic-"*)
        # Keep the connector-free negative non-vacuous by giving its runner the
        # same session-scoped project identity the real skill tier records.
        echo "CURIE_SESSION_ID=local-stub-hermetic"
        ;;
    *"name=curie-api"*|*"com.docker.compose.service=curie-api"*)
        if [ "${STUB_EXISTING_LOCAL_STACK:-0}" = "1" ]; then
            echo "stub-curie-api"
        fi
        ;;
    "exec "*" cat /plugin/"*)
        # e2e.sh proves the /plugin mount is the snapshot packed at skill up,
        # not the live host file. Replay the SKILL.md bytes that arm saved.
        if [ -f "$STUB_STATE/mounted_skill.md" ]; then
            cat "$STUB_STATE/mounted_skill.md"
        fi
        ;;
    "inspect "*)
        # A failed env read, on its own knob: an inspect that dies (the worker
        # exited since the `docker ps`, or a daemon blip) prints nothing, which
        # is indistinguishable from a worker carrying no CURIE_FAKE_MODEL unless
        # the probe checks the status. Under a live run that reads as "live", so
        # the unread probe would certify the mode it never managed to read.
        if [ "${STUB_DOCKER_INSPECT_EXIT:-0}" != "0" ]; then
            echo "stub docker: no such object: the container is gone" >&2
            exit "$STUB_DOCKER_INSPECT_EXIT"
        fi
        if [ -n "${STUB_FAKE_MODEL:-}" ]; then
            echo "CURIE_FAKE_MODEL=$STUB_FAKE_MODEL"
        fi
        # The default local rungs read the connector scope off the same worker
        # env: the stock weather bundle declares the netpol-probe fixture, so
        # the dual assertion derives its identity label from this release.
        echo "CURIE_RELEASE=curie"
        echo "PATH=/usr/local/bin:/usr/bin:/bin"
        ;;
    *"curietech.ai/connector=curie-weather-mcp-netpol-probe"*)
        # The dual assertion's hosted read: the stock weather bundle declares
        # the netpol-probe fixture, and this stubbed tier reports it running
        # under exactly the identity label the reconciler stamps.
        echo "curie-curie-weather-mcp-netpol-probe-1"
        ;;
    *"com.docker.compose.service=curie-worker"*)
        # Exactly one worker: the probe must refuse to guess when the scoping
        # assumption breaks, so returning one id is the only shape that lets the
        # mode assertion be reached at all.
        echo "stub-curie-worker"
        ;;
    *)
        ;;
esac
"#,
    );

    write_executable(
        &dir.join("kubectl"),
        r#"#!/bin/sh
set -u
if [ -n "${STUB_KUBECTL_INVOCATION_LOG:-}" ]; then
    printf '%s\n' "$*" >> "$STUB_KUBECTL_INVOCATION_LOG"
fi
case "$*" in
    "config current-context")
        printf '%s\n' 'stub-context'
        exit 0
        ;;
    *" logs "*)
        case "${STUB_RETENTION_CLAIM_MODE:-exact}" in
            missing)
                exit 0
                ;;
            exact|unrelated|late|overlong)
                ;;
            *)
                printf 'unknown retention claim mode: %s\n' "${STUB_RETENTION_CLAIM_MODE}" >&2
                exit 97
                ;;
        esac
        if [ ! -s "$STUB_STATE/retention-thread" ] || [ ! -s "$STUB_STATE/retention-channel" ]; then
            echo 'retention logs requested without an explicit cluster message channel and thread' >&2
            exit 97
        fi
        thread_key="$(cat "$STUB_STATE/retention-thread")"
        channel="$(cat "$STUB_STATE/retention-channel")"
        if [ "${STUB_RETENTION_CLAIM_MODE:-exact}" = "unrelated" ]; then
            thread_key='1735689600.000002'
        fi
        python3 - "$channel" "$thread_key" "${STUB_RETENTION_CLAIM_MODE:-exact}" <<'PYRETENTION'
import datetime, json, sys

channel, thread_key, mode = sys.argv[1:4]
claimed = datetime.datetime.now(datetime.timezone.utc)
if mode == "late":
    claimed += datetime.timedelta(seconds=46)
duration_ms = 45000 if mode == "overlong" else 1
record = {
    "logger": "curie_worker.kernel",
    "message": "claim latency for slack:%s:%s: %s ms" % (channel, thread_key, duration_ms),
    "timestamp": claimed.isoformat(),
}
prefix = claimed.isoformat(timespec="microseconds").replace("+00:00", "Z")
print(prefix, json.dumps(record, separators=(",", ":")))
PYRETENTION
        exit 0
        ;;
esac
expect_ns="${STUB_EXPECT_NAMESPACE:-curie}"
expect_rel="${STUB_EXPECT_RELEASE:-curie}"
case "$expect_rel" in
    *curie*) worker="${expect_rel}-worker" ;;
    *) worker="${expect_rel}-curie-worker" ;;
esac
matched_target=0
case " $* " in
    *" -n $expect_ns "*)
        case "$*" in
            *"deployment/${worker}"*)
                matched_target=1
                ;;
        esac
        ;;
esac
# Return the selected worker Deployment for the delivery budget read. The
# controls vary the literal env entries while preserving the real Kubernetes
# object shape consumed by the ladder.
case "$*" in
    *" -o json")
        if [ "$matched_target" = 1 ]; then
            python3 - "$worker" <<'PYWORKER'
import json
import os
import sys

worker = sys.argv[1]
mode = os.environ.get("STUB_WORKER_ENV_MODE", "valid")
budget = os.environ.get("STUB_DELIVERY_BUDGET_S", "600")
env = []
if mode == "budget_nonliteral":
    env.append({"name": "CURIE_DELIVERY_BUDGET_S", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}})
elif mode != "budget_absent":
    env.append({"name": "CURIE_DELIVERY_BUDGET_S", "value": budget})
if mode == "budget_duplicate":
    env.append({"name": "CURIE_DELIVERY_BUDGET_S", "value": budget})
print(json.dumps({
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {"name": worker},
    "spec": {"template": {"spec": {"containers": [
        {"name": "worker", "env": env},
    ]}}},
}, separators=(",", ":")))
PYWORKER
        fi
        exit 0
        ;;
esac
# Answer env probes only for the selected worker. A hardcoded curie probe must
# not satisfy a nondefault control.
case "$*" in
    *"CURIE_FAKE_MODEL"*)
        if [ "$matched_target" = 1 ]; then
            printf '%s' "${STUB_FAKE_MODEL:-}"
        fi
        ;;
    *"CURIE_RELEASE"*)
        if [ "$matched_target" = 1 ]; then
            printf '%s' "$expect_rel"
        fi
        ;;
    *"CURIE_NAMESPACE"*)
        if [ "$matched_target" = 1 ]; then
            printf '%s' "$expect_ns"
        fi
        ;;
    *)
        ;;
esac
"#,
    );
}

/// Serve one canned JSON body to every request on an ephemeral loopback port,
/// and return its base URL for `CURIE_API_URL`.
///
/// `CURIE_API_URL` is not a test-only knob: it is the documented env fallback of
/// every local verb's `--api-url` (`cli/src/main.rs:1193`), with the same
/// default the CLI itself applies, so pointing the ladder's deployments read at
/// a different API is production behavior that happens to also be the seam this
/// control needs.
fn spawn_deployments_stub(body: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind the deployments stub");
    let address = listener.local_addr().expect("read the stub address");
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            // The ladder only GETs, so the request is one small buffer and its
            // content is irrelevant; it is drained only so the client sees a
            // complete exchange before the response.
            let mut request = [0u8; 2048];
            let _ = stream.read(&mut request);
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });
    format!("http://{address}")
}

/// The `DeploymentOut` list shape (`apps/api/src/curie_api/schemas.py:654-660`)
/// for the healthy case: exactly one active row, and it is this run's.
fn one_active_deployment() -> String {
    format!(
        "[{{\"id\":\"{DEPLOYMENT_ID}\",\"agent_id\":\"{AGENT_ID}\",\
         \"version_id\":\"{VERSION_ID}\",\"environment\":\"dev\",\
         \"commit_sha\":null,\"status\":\"active\",\
         \"deployed_at\":\"2026-08-17T00:00:00Z\"}}]"
    )
}

/// This run's `dev` row plus a stale active `prod` row for the same agent: the
/// wrong-artifact-green shape.
fn two_active_deployments() -> String {
    format!(
        "[{{\"id\":\"{DEPLOYMENT_ID}\",\"agent_id\":\"{AGENT_ID}\",\
         \"version_id\":\"{VERSION_ID}\",\"environment\":\"dev\",\
         \"commit_sha\":null,\"status\":\"active\",\
         \"deployed_at\":\"2026-08-17T00:00:00Z\"}},\
         {{\"id\":\"{STALE_PROD_DEPLOYMENT_ID}\",\"agent_id\":\"{AGENT_ID}\",\
         \"version_id\":\"{VERSION_ID}\",\"environment\":\"prod\",\
         \"commit_sha\":null,\"status\":\"active\",\
         \"deployed_at\":\"2026-01-01T00:00:00Z\"}}]"
    )
}

/// Refuse to run a control that drives the local rung while something holds the
/// local reply stub's port.
///
/// `assert_stub_port_free` (`cli/scripts/e2e-ladder.sh`) is a real connect
/// probe against a HOST-GLOBAL port that the ladder deliberately does not let a
/// caller override, so no stub can satisfy it. On a box with several checkouts,
/// a concurrent `local message` or `local eval` holds it and reds every
/// local-rung control for a reason that has nothing to do with parity. Failing
/// here, with the ladder's own fix line, keeps that red diagnosable instead of
/// arriving as a confusing parity failure.
fn require_local_stub_port_free() {
    let address = "127.0.0.1:8155"
        .parse()
        .expect("parse the stub port address");
    if std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_millis(250)).is_ok()
    {
        panic!(
            "port 8155 is already in use, so the ladder's local rung cannot \
             start and this control cannot run. This is a host collision, not \
             a parity failure. fix: stop the process holding it (another ladder \
             run, or a stale local message/eval), then re-run."
        );
    }
}

/// Run the real ladder script against the stubs in `harness`, with `envs`
/// carrying the tier selection and the STUB_* knobs this control moves.
///
/// Every variable the ladder or `e2e.sh` reads is REMOVED before `envs` is
/// applied, so the child environment is fixed by the control rather than by
/// whatever the developer happens to have exported. Two classes, both of which
/// have already bitten:
/// - `CURIE_API_KEY` / `CURIE_API_URL`: the cluster rung's runtime-binding read
///   arms itself only when a key is present, so an exported key made a
///   cluster-only control reach the default `localhost:28000` and fail
///   unreachable -- green in CI, red on one box.
/// - `CURIE_E2E_IMAGE` / `_PORT` / `_NETWORK` / `_OTEL`: these change the argv
///   `e2e.sh` builds for `skill up`, which the stub matches exactly, so an
///   exported value turns the skill rung into an `exit 97`.
fn run_ladder(harness: &Path, envs: &[(&str, &str)]) -> Output {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/e2e-ladder.sh");
    run_ladder_script(&script, harness, envs)
}

fn run_ladder_script(script: &Path, harness: &Path, envs: &[(&str, &str)]) -> Output {
    let path = format!(
        "{}:{}",
        harness.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let mut command = Command::new("bash");
    command
        .arg(script)
        .env("CURIE_BIN", harness.join("curie"))
        .env("PATH", path)
        .env("STUB_STATE", harness)
        .env("STUB_AGENT_ID", AGENT_ID)
        .env("STUB_VERSION_ID", VERSION_ID)
        .env("STUB_DEPLOYMENT_ID", DEPLOYMENT_ID)
        .env("STUB_OBSERVABILITY_TRACE_ID", OBSERVABILITY_TRACE_ID)
        .env(
            "STUB_UNKNOWN_OBSERVABILITY_TRACE_ID",
            UNKNOWN_OBSERVABILITY_TRACE_ID,
        )
        .env("STUB_UNAVAILABLE_API_URL", UNAVAILABLE_API_URL)
        .env_remove("CURIE_E2E_TIERS")
        .env_remove("CURIE_E2E_LIVE")
        .env_remove("CURIE_E2E_LISTEN_HOST")
        .env_remove("CURIE_E2E_BUNDLE")
        .env_remove("CURIE_E2E_IMAGE")
        .env_remove("CURIE_E2E_PORT")
        .env_remove("CURIE_E2E_NETWORK")
        .env_remove("CURIE_E2E_OTEL")
        .env_remove("CURIE_FAKE_MODEL")
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_API_URL")
        .env_remove("CURIE_NAMESPACE")
        .env_remove("CURIE_RELEASE")
        .env_remove("STUB_EXPECT_NAMESPACE")
        .env_remove("STUB_EXPECT_RELEASE")
        .env_remove("STUB_CLUSTER_RELEASE_FOUND")
        .env_remove("STUB_KUBECTL_INVOCATION_LOG")
        .env_remove("STUB_PRIMARY_SKILL_UP_ERROR")
        .env_remove("STUB_DOCKER_INVOCATION_LOG")
        .env_remove("STUB_EXISTING_LOCAL_STACK")
        .env_remove("STUB_RUNS_EXTRA_JSON")
        .env_remove("STUB_UNAVAILABLE_EXIT")
        .env_remove("STUB_UNAVAILABLE_NO_FIX")
        .env_remove("STUB_UNAVAILABLE_MARKER")
        .env_remove("STUB_LOCAL_DEPLOY_REFUSAL")
        .env_remove("STUB_UNKNOWN_TRACE_EXIT")
        .env_remove("STUB_UNKNOWN_TRACE_NO_FIX")
        .env_remove("STUB_RETENTION_CLAIM_MODE")
        .env_remove("STUB_RETENTION_MESSAGE_EXIT")
        .env_remove("STUB_RETENTION_REPLY_FINALIZED")
        .env_remove("STUB_EXPECT_REPLY_TIMEOUT_SECS")
        .env_remove("STUB_WORKER_ENV_MODE")
        .env_remove("STUB_DELIVERY_BUDGET_S");
    for (key, value) in envs {
        command.env(key, value);
    }
    command.output().expect("run the real ladder script")
}

#[test]
fn capture_local_deploy_reprints_refusal_before_exit_trap_and_preserves_status() {
    let harness = tempfile::tempdir().expect("create deploy capture harness directory");
    write_ladder_stubs(harness.path());
    let bundle = harness.path().join("bundle");
    fs::create_dir_all(&bundle).expect("create stub bundle directory");
    let function = ladder_function("capture_local_deploy");
    let script = format!(
        r#"set -euo pipefail
BIN={}
deploy_json=''
{}
trap 'code=$?; printf "EXIT_TRAP status=%s\n" "$code"; exit "$code"' EXIT
capture_local_deploy {}
"#,
        sh_single_quote(&harness.path().join("curie")),
        function,
        sh_single_quote(&bundle),
    );
    let output = Command::new("bash")
        .arg("-c")
        .arg(script)
        .env("STUB_STATE", harness.path())
        .env("STUB_LOCAL_DEPLOY_REFUSAL", "1")
        .output()
        .expect("run status-preserving local deploy capture");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let refusal = r#"{"error":"refusing connector deploy: missing approval route sre-approvals","fix":"curie local approvals acme-bot --route-resolution sre-approvals=C0LOCALDEV"}"#;
    let refusal_at = stdout
        .find(refusal)
        .unwrap_or_else(|| panic!("capture must reprint the refusal JSON; stdout:\n{stdout}"));
    let trap_at = stdout
        .find("EXIT_TRAP status=2")
        .unwrap_or_else(|| panic!("the EXIT trap must observe status 2; stdout:\n{stdout}"));
    assert!(
        refusal_at < trap_at,
        "refusal JSON must be visible before teardown observes the failure; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "capture must return the deploy refusal status unchanged; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

fn run_approval_seed_route_harness(
    harness: &Path,
    api_url: &str,
    route_map: &Path,
    refuse_route_mutation: bool,
) -> Output {
    let helper = ladder_function("configure_deterministic_approval_seed_route");
    let curie = harness.join("approval-seed-curie");
    write_executable(
        &curie,
        r#"#!/usr/bin/env bash
set -euo pipefail

route_file=""
previous=""
for argument in "$@"; do
    if [ "$previous" = "--routes-from" ]; then
        route_file="$argument"
        break
    fi
    previous="$argument"
done
if [ -n "$route_file" ]; then
    cp "$route_file" "$STUB_ROUTE_MAP"
    if [ "${STUB_REFUSE_ROUTE_MUTATION:-0}" = "1" ]; then
        printf '%s\n' '{"error":"seed route mutation rejected","fix":"keep the existing route map"}'
        exit 22
    fi
    printf '%s\n' '{"routes":"updated"}'
    exit 0
fi

printf 'unexpected approval seed command: %s\n' "$*" >&2
exit 97
"#,
    );

    let script = [
        "set -euo pipefail\n".to_owned(),
        format!("BIN={}\n", sh_single_quote(&curie)),
        format!("WORKDIR={}\n", sh_single_quote(harness)),
        helper,
        format!("configure_deterministic_approval_seed_route local {AGENT_ID} C0EXAMPLE1\n"),
    ]
    .concat();
    Command::new("bash")
        .arg("-c")
        .arg(script)
        .env("CURIE_API_URL", api_url)
        .env("CURIE_API_KEY", "curie-dev-key")
        .env("STUB_ROUTE_MAP", route_map)
        .env(
            "STUB_REFUSE_ROUTE_MUTATION",
            if refuse_route_mutation { "1" } else { "0" },
        )
        .output()
        .expect("run approval seed route harness")
}

#[test]
fn approval_seed_keeps_every_representable_route_and_reprints_a_refused_route_write() {
    let harness = tempfile::tempdir().expect("create approval seed route harness directory");
    let retained_routes = serde_json::json!({
        "sre-approvals": {
            "resolution": {"kind": "slack", "address": "C0SREBOT"},
            "approvers": {"users": ["U0EXAMPLE2"]}
        },
        "existing-approvals": {
            "resolution": {"kind": "slack", "address": "C0EXAMPLE2"},
            "approvers": {"group": "S0EXAMPLE1"}
        }
    });
    let agents = serde_json::json!([{
        "id": AGENT_ID,
        "name": "sre-bot",
        "approval_routes": retained_routes,
    }]);
    let api_url = spawn_deployments_stub(&agents.to_string());
    let route_map = harness.path().join("approval-seed-routes.json");

    let applied = run_approval_seed_route_harness(harness.path(), &api_url, &route_map, false);
    let applied_transcript = transcript(&applied);
    assert!(
        applied.status.success(),
        "the approval seed must add e2e without dropping existing policy: {applied_transcript}"
    );
    let written: serde_json::Value = serde_json::from_slice(
        &fs::read(&route_map).expect("the seed route write must receive a full route map"),
    )
    .expect("the seed route map must be JSON");
    assert_eq!(
        written["sre-approvals"],
        serde_json::json!({
            "resolution": {"kind": "slack", "address": "C0SREBOT"},
            "approvers": {"users": ["U0EXAMPLE2"]}
        }),
        "the live sre-bot restriction must survive the deterministic e2e seed"
    );
    assert_eq!(
        written["existing-approvals"],
        serde_json::json!({
            "resolution": {"kind": "slack", "address": "C0EXAMPLE2"},
            "approvers": {"group": "S0EXAMPLE1"}
        }),
        "an unrelated route and its approver restriction must survive the seed"
    );
    assert_eq!(
        written["e2e"],
        serde_json::json!({
            "resolution": {"kind": "slack", "address": "C0EXAMPLE1"},
            "approvers": {"users": ["U0EXAMPLE1"]}
        }),
        "the seed must replace only its deterministic e2e route"
    );

    let refused = run_approval_seed_route_harness(harness.path(), &api_url, &route_map, true);
    let refused_transcript = transcript(&refused);
    assert_ne!(
        refused.status.code(),
        Some(0),
        "a refused approval route mutation must stop the seed: {refused_transcript}"
    );
    assert!(
        refused_transcript.contains(
            r#"{"error":"seed route mutation rejected","fix":"keep the existing route map"}"#
        ),
        "the seed must reprint the API refusal JSON rather than hiding it: {refused_transcript}"
    );

    let notification_agents = serde_json::json!([{
        "id": AGENT_ID,
        "name": "sre-bot",
        "approval_routes": {
            "sre-approvals": {
                "resolution": {"kind": "slack", "address": "C0SREBOT"},
                "notification": {"kind": "slack", "address": "C0NOTIFY"},
                "approvers": {"users": ["U0EXAMPLE2"]}
            }
        },
    }]);
    let notification_api_url = spawn_deployments_stub(&notification_agents.to_string());
    let notification_route_map = harness
        .path()
        .join("notification-approval-seed-routes.json");
    let notification_refused = run_approval_seed_route_harness(
        harness.path(),
        &notification_api_url,
        &notification_route_map,
        false,
    );
    let notification_transcript = transcript(&notification_refused);
    assert_ne!(
        notification_refused.status.code(),
        Some(0),
        "a retained notification route must stop the seed before a lossy write: {notification_transcript}"
    );
    assert!(
        notification_transcript.contains("carry notification targets"),
        "the refusal must explain why the retained policy cannot be represented: {notification_transcript}"
    );
    assert!(
        !notification_route_map.exists(),
        "the notification refusal must happen before any approvals route write is attempted"
    );
}

fn run_eval_argument_control(trajectory: bool) -> (Output, String) {
    require_local_stub_port_free();
    let root = tempfile::tempdir().expect("create isolated ladder root");
    let scripts = root.path().join("cli/scripts");
    let evals = root.path().join("examples/weather/evals");
    fs::create_dir_all(&scripts).expect("create script directory");
    fs::create_dir_all(&evals).expect("create eval directory");
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("scripts/e2e-ladder.sh");
    let script = scripts.join("e2e-ladder.sh");
    fs::copy(source, &script).expect("copy ladder script");
    fs::write(
        evals.join("cases.json"),
        r#"{"name":"weather","cases":[{"id":"weather","input":"weather","grader":{"kind":"contains","expected":"sunny"}}]}"#,
    )
    .expect("write cases");
    if trajectory {
        fs::write(
            evals.join("trajectory.json"),
            r#"{"specs":[{"case_id":"weather","expected":["WebSearch"],"mode":"exact","threshold":1.0}]}"#,
        )
        .expect("write trajectory sidecar");
    }

    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());
    let invocation_log = harness.path().join("invocations.log");
    let invocation_log_value = invocation_log.display().to_string();
    let output = run_ladder_script(
        &script,
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_E2E_LIVE", "1"),
            ("CURIE_CREDENTIALS", "test-credential"),
            ("CURIE_API_URL", &api_url),
            ("STUB_FAKE_MODEL", ""),
            ("STUB_INVOCATION_LOG", &invocation_log_value),
        ],
    );
    let invocations = fs::read_to_string(invocation_log).unwrap_or_default();
    (output, invocations)
}

/// Drive only the local rung with observability-aware stubs and return its
/// result, the exact candidate-CLI argv transcript, and whether the
/// unavailable-API branch was actually exercised.
///
/// The deployed ladder must use the candidate binary for these reads. A raw
/// curl to Langfuse, or even to the API route, never reaches one of the arms
/// above and therefore cannot create the unavailable marker or satisfy the
/// invocation assertions below.
fn run_local_observability_control(extra_envs: &[(&str, &str)]) -> (Output, String, bool, String) {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create observability harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());
    let trace_id = format!(
        "{:032x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after Unix epoch")
            .as_nanos()
    );
    let invocation_log = harness.path().join("observability-invocations.log");
    let invocation_log_value = invocation_log.display().to_string();
    let unavailable_marker = harness.path().join("unavailable-api-called");
    let unavailable_marker_value = unavailable_marker.display().to_string();
    let mut envs = vec![
        ("CURIE_E2E_TIERS", "local"),
        ("CURIE_API_URL", api_url.as_str()),
        ("STUB_FAKE_MODEL", "1"),
        ("STUB_OBSERVABILITY_TRACE_ID", trace_id.as_str()),
        ("STUB_INVOCATION_LOG", invocation_log_value.as_str()),
        ("STUB_UNAVAILABLE_MARKER", unavailable_marker_value.as_str()),
    ];
    envs.extend_from_slice(extra_envs);

    let output = run_ladder(harness.path(), &envs);
    let invocations = fs::read_to_string(invocation_log).unwrap_or_default();
    let unavailable_called = unavailable_marker.exists();
    (output, invocations, unavailable_called, trace_id)
}

fn invocation_count(invocations: &str, expected: &str) -> usize {
    invocations.lines().filter(|line| *line == expected).count()
}

/// The local rung must boot the current source tree rather than the release
/// compose resolved by the candidate binary. The compose file and `--build`
/// are both load-bearing parts of this argv.
fn is_current_source_local_up(invocation: &str) -> bool {
    let args = invocation.split_whitespace().collect::<Vec<_>>();
    args.windows(2)
        .any(|window| window[0] == "-f" && window[1].ends_with("/compose.dev.yaml"))
        && args.contains(&"--build")
        && args.first() == Some(&"local")
        && args.get(1) == Some(&"up")
}

/// Teardown must target the same current-source compose file that the rung
/// brought up; an unqualified `local down` could select a release compose.
fn is_current_source_local_down(invocation: &str) -> bool {
    let args = invocation.split_whitespace().collect::<Vec<_>>();
    args.windows(2)
        .any(|window| window[0] == "-f" && window[1].ends_with("/compose.dev.yaml"))
        && args.first() == Some(&"local")
        && args.get(1) == Some(&"down")
}

fn current_source_local_down_count(invocations: &str) -> usize {
    invocations
        .lines()
        .filter(|line| is_current_source_local_down(line))
        .count()
}

fn looks_like_utc_second(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 20
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            4 | 7 => *byte == b'-',
            10 => *byte == b'T',
            13 | 16 => *byte == b':',
            19 => *byte == b'Z',
            _ => byte.is_ascii_digit(),
        })
}

/// Pin the public argv the ladder itself must exercise. These are not test-only
/// probes: every line is a command an operator can paste against the candidate
/// binary. Exact trace discovery is private to the bounded seed stream slice;
/// the separate agent-filtered list is the unavailable-API negative with its
/// process environment pointed at a closed endpoint.
fn assert_observability_candidate_invocations(invocations: &str, trace_id: &str) {
    let legacy_runs = "--json local observability runs --limit 100";
    let unavailable = format!("--json local observability runs --limit 1 --agent-id {AGENT_ID}");
    let detail = format!("--json local observability run {trace_id}");
    let unknown = format!("--json local observability run {UNKNOWN_OBSERVABILITY_TRACE_ID}");

    assert_eq!(
        invocation_count(invocations, legacy_runs),
        0,
        "the local rung must never let a newest-runs page select an unrelated background trace; invocations:\n{invocations}"
    );
    assert!(
        invocations
            .lines()
            .filter(|line| line.starts_with("--json local message "))
            .count()
            >= 2,
        "the query proof must seed the product Collector with independently finalized turns after the raw sink controls; invocations:\n{invocations}"
    );
    assert_eq!(
        invocation_count(invocations, &unavailable),
        1,
        "the local rung must exercise the filtered runs command against an unavailable API; invocations:\n{invocations}"
    );
    for expected in [&detail, &unknown] {
        assert_eq!(
            invocation_count(invocations, expected),
            1,
            "the local rung must issue this candidate observability query exactly once: {expected}; invocations:\n{invocations}"
        );
    }
    let summary_lines = invocations
        .lines()
        .filter(|line| line.starts_with("--json local observability metrics --start "))
        .collect::<Vec<_>>();
    assert_eq!(
        summary_lines.len(),
        1,
        "the local rung must issue exactly one metrics summary with explicit bounds; invocations:\n{invocations}"
    );
    let summary_args = summary_lines[0].split_whitespace().collect::<Vec<_>>();
    assert_eq!(
        summary_args.len(),
        8,
        "metrics summary must contain only the candidate verb and explicit --start/--end values; invocations:\n{invocations}"
    );
    assert_eq!(
        &summary_args[..5],
        ["--json", "local", "observability", "metrics", "--start"]
    );
    assert_eq!(summary_args[6], "--end");

    let series_lines = invocations
        .lines()
        .filter(|line| {
            line.starts_with(
                "--json local observability metrics --metric runs --granularity hour --start ",
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        series_lines.len(),
        1,
        "the local rung must issue exactly one runs/hour series with explicit bounds; invocations:\n{invocations}"
    );
    let series_args = series_lines[0].split_whitespace().collect::<Vec<_>>();
    assert_eq!(
        series_args.len(),
        12,
        "metrics series must preserve metric=runs, granularity=hour, and explicit --start/--end values; invocations:\n{invocations}"
    );
    assert_eq!(
        &series_args[..9],
        [
            "--json",
            "local",
            "observability",
            "metrics",
            "--metric",
            "runs",
            "--granularity",
            "hour",
            "--start",
        ]
    );
    assert_eq!(series_args[10], "--end");

    let summary_window = (summary_args[5], summary_args[7]);
    let series_window = (series_args[9], series_args[11]);
    assert!(
        looks_like_utc_second(summary_window.0) && looks_like_utc_second(summary_window.1),
        "metrics bounds must be explicit second-resolution UTC timestamps; invocations:\n{invocations}"
    );
    assert_ne!(
        summary_window.0, summary_window.1,
        "metrics bounds must form a non-empty window; invocations:\n{invocations}"
    );
    assert_eq!(
        summary_window, series_window,
        "summary and series must share the one dynamically captured window; invocations:\n{invocations}"
    );
    let detail_position = invocations
        .lines()
        .position(|line| line == detail.as_str())
        .expect("discovered trace detail invocation");
    let seed_position = invocations
        .lines()
        .position(|line| line.starts_with("--json local message "))
        .expect("seed message invocation");
    assert!(
        seed_position < detail_position,
        "the rung must complete a seed before querying the exact trace ID privately derived from that seed; invocations:\n{invocations}"
    );
}

/// Whether some ONE line of the transcript carries every one of `needles`.
///
/// Line-scoped rather than whole-transcript, and that is the load-bearing part:
/// the stub's raw `deploy --json` receipt already echoes a digest into the
/// transcript, so a whole-transcript `contains` check for that digest passes
/// with no parity logic in the ladder at all. Requiring the digest to share a
/// line with a rung label, or two digests to share a line with each other,
/// demands an actual operator-facing report instead.
fn has_line_with(transcript: &str, needles: &[&str]) -> bool {
    transcript
        .lines()
        .any(|line| needles.iter().all(|needle| line.contains(needle)))
}

/// Both streams together, because a control asserts on the operator-facing
/// message without caring which stream carried it.
fn transcript(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The content digest of the weather bundle as it sits in the tree, which is the
/// digest the content-derived stub reports for every rung before anything
/// mutates the copy. Computed with the same path-plus-bytes walk the stub's
/// `sha_of_bundle` runs, so the two agree by construction.
fn weather_bundle_sha256() -> String {
    let bundle = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../examples/weather");
    let output = Command::new("python3")
        .arg("-c")
        .arg(
            "import hashlib, os, sys\n\
             root = sys.argv[1]\n\
             digest = hashlib.sha256()\n\
             for dirpath, dirnames, filenames in os.walk(root):\n\
             \x20   dirnames[:] = sorted(d for d in dirnames if d != '.curie')\n\
             \x20   for entry in sorted(filenames):\n\
             \x20       path = os.path.join(dirpath, entry)\n\
             \x20       digest.update(os.path.relpath(path, root).encode())\n\
             \x20       digest.update(open(path, 'rb').read())\n\
             print(digest.hexdigest())",
        )
        .arg(bundle)
        .output()
        .expect("hash the weather bundle tree");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

#[test]
fn ladder_selects_platform_trajectory_eval_without_overriding_deployed_cases() {
    let (trajectory_output, trajectory_invocations) = run_eval_argument_control(true);
    let trajectory_transcript = transcript(&trajectory_output);
    assert_eq!(
        trajectory_output.status.code(),
        Some(0),
        "trajectory ladder failed:\n{trajectory_transcript}"
    );
    let trajectory_evals = trajectory_invocations
        .lines()
        .filter(|line| line.contains("local eval") || line.contains("cluster eval"))
        .collect::<Vec<_>>();
    assert_eq!(trajectory_evals.len(), 6, "{trajectory_invocations}");
    assert!(
        trajectory_evals
            .iter()
            .all(|line| !line.contains("--cases")),
        "trajectory eval must use the deployed bundle instead of an explicit cases override: \
         {trajectory_invocations}"
    );
    let trajectory_starts = trajectory_invocations
        .lines()
        .filter(|line| line.starts_with("local up"))
        .collect::<Vec<_>>();
    assert_eq!(trajectory_starts.len(), 1, "{trajectory_invocations}");
    assert!(
        trajectory_starts
            .iter()
            .all(|line| is_current_source_local_up(line)),
        "trajectory scoring requires current-source compose.dev.yaml with --build, never --minimal: {trajectory_invocations}"
    );
    for tier in ["local", "cluster"] {
        assert!(
            has_line_with(
                &trajectory_transcript,
                &[tier, r#"grade 1 case(s) from suite "weather""#,],
            ),
            "trajectory dry run must expose the standard suite and case count for {tier}: \
             {trajectory_transcript}"
        );
    }

    let (ordinary_output, ordinary_invocations) = run_eval_argument_control(false);
    let ordinary_transcript = transcript(&ordinary_output);
    assert_eq!(
        ordinary_output.status.code(),
        Some(0),
        "ordinary ladder failed:\n{ordinary_transcript}"
    );
    let ordinary_evals = ordinary_invocations
        .lines()
        .filter(|line| line.contains("local eval") || line.contains("cluster eval"))
        .collect::<Vec<_>>();
    assert_eq!(ordinary_evals.len(), 6, "{ordinary_invocations}");
    assert!(
        ordinary_evals.iter().all(|line| line.contains("--cases")),
        "ordinary eval must retain its explicit cases path: {ordinary_invocations}"
    );
    let ordinary_starts = ordinary_invocations
        .lines()
        .filter(|line| line.starts_with("local up"))
        .collect::<Vec<_>>();
    assert_eq!(
        ordinary_starts.len(),
        1,
        "the local rung's observability query proof requires one current-source boot even when the suite has no trajectory sidecar: {ordinary_invocations}"
    );
    assert!(
        ordinary_starts
            .iter()
            .all(|line| is_current_source_local_up(line)),
        "the local rung's observability query proof must pin compose.dev.yaml and --build even when the suite has no trajectory sidecar: {ordinary_invocations}"
    );
}

/// #866 POSITIVE CONTROL. The local rung must prove every new read surface
/// against the candidate binary and a real API-backed stack. In particular,
/// the ordinary weather bundle can no longer select `--minimal`: observability
/// queries require Langfuse, which only the full profile starts.
#[test]
fn local_rung_proves_observability_queries_with_real_candidate_verbs() {
    let (output, invocations, unavailable_called, trace_id) = run_local_observability_control(&[]);
    let transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a local rung whose observability DTOs, negative shapes, and semantic exit codes all match must pass; transcript:\n{transcript}"
    );
    assert_observability_candidate_invocations(&invocations, &trace_id);
    assert!(
        unavailable_called,
        "the local rung must actually move CURIE_API_URL to the closed-loopback control and exercise the unavailable-API path through the candidate CLI; invocations:\n{invocations}"
    );
    let local_starts = invocations
        .lines()
        .filter(|line| line.starts_with("local up"))
        .collect::<Vec<_>>();
    assert_eq!(
        local_starts.len(),
        1,
        "observability proof requires exactly one local boot, never an additional minimal or release-compose boot; invocations:\n{invocations}"
    );
    assert!(
        local_starts.iter().all(|line| is_current_source_local_up(line)),
        "observability proof requires current-source compose.dev.yaml with --build, never --minimal; invocations:\n{invocations}"
    );
    assert_eq!(
        current_source_local_down_count(&invocations),
        1,
        "a successful rung must tear down the one current-source stack it claimed exactly once; invocations:\n{invocations}"
    );
}

/// A borrowed compose stack remains owned by the run that started it. The
/// observability proof still runs against it, but this ladder must neither
/// start nor stop it -- query coverage does not weaken the existing ownership
/// boundary around teardown.
#[test]
fn local_observability_proof_leaves_a_borrowed_stack_to_its_owner() {
    let (output, invocations, unavailable_called, trace_id) =
        run_local_observability_control(&[("STUB_EXISTING_LOCAL_STACK", "1")]);
    let transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "observability proof against a borrowed healthy stack must pass without claiming it; transcript:\n{transcript}"
    );
    assert_observability_candidate_invocations(&invocations, &trace_id);
    assert!(
        unavailable_called,
        "unavailable API negative was not exercised"
    );
    assert!(
        !invocations
            .lines()
            .any(|line| line.starts_with("local up") || line.starts_with("local down")),
        "the ladder must leave a pre-existing local stack to its owner; invocations:\n{invocations}"
    );
}

/// #866 NEGATIVE CONTROLS. Move one candidate query contract at a time while
/// the positive control above stays green: unknown-trace exit 1,
/// unavailable-API exit 3, and the mandatory recovery instruction must each
/// make the real ladder reject the candidate and still run owner teardown.
#[test]
fn local_observability_negative_controls_are_falsifiable() {
    let cases: &[(&str, &str, &[&str], bool)] = &[
        (
            "STUB_UNKNOWN_TRACE_EXIT",
            "0",
            &["unknown trace", "exit 1"],
            false,
        ),
        (
            "STUB_UNAVAILABLE_EXIT",
            "1",
            &["unavailable API", "exit 3"],
            true,
        ),
        (
            "STUB_UNKNOWN_TRACE_NO_FIX",
            "1",
            &["unknown trace", "error", "fix"],
            false,
        ),
    ];

    for (variable, value, expected_line, expect_unavailable) in cases {
        let (output, invocations, unavailable_called, _) =
            run_local_observability_control(&[(variable, value)]);
        let transcript = transcript(&output);
        assert_ne!(
            output.status.code(),
            Some(0),
            "{variable} must falsify the local observability proof; transcript:\n{transcript}"
        );
        assert!(
            has_line_with(&transcript, expected_line),
            "{variable} must name the rejected contract {expected_line:?}; transcript:\n{transcript}"
        );
        assert_eq!(
            unavailable_called, *expect_unavailable,
            "{variable} exercised the wrong unavailable-API path"
        );
        assert_eq!(
            current_source_local_down_count(&invocations),
            1,
            "the owner-scoped EXIT trap must tear down {variable} through compose.dev.yaml; invocations:\n{invocations}"
        );
    }
}

/// POSITIVE CONTROL. Every rung -- skill, local and cluster -- reports the same
/// bundle digest, a matching dry-run plan line, and a model mode consistent with
/// the run, so the ladder must pass and must NAME that one digest against each of
/// the three rungs.
///
/// Why it exists: without it, the five negative controls below are unanchored --
/// a ladder that failed unconditionally would satisfy all of them. This is the
/// test that makes "one knob moved" mean something, and it is also the only test
/// that catches a parity assertion so strict it can never pass.
///
/// The skill rung is in the tier list deliberately, and it is the only executing
/// coverage of that leg: it runs `cli/scripts/e2e.sh` as a child, so it is what
/// fails if the `CURIE_E2E_BUNDLE` handoff breaks, if the `tee`d
/// `initial bundle digest:` line stops being parsed, or if the skill rung stops
/// comparing its identity with the deployed rungs. With local and cluster alone,
/// every one of those regressions stayed green.
///
/// No digest is pinned through a STUB_* override here: all three rungs report the
/// content-derived digest of the tree they actually packed, so their agreement is
/// three independent derivations landing on one value rather than one canned
/// constant echoed three times.
#[test]
fn parity_passes_when_every_rung_reports_the_same_identity() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());
    let digest = weather_bundle_sha256();

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "skill,local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_FAKE_MODEL", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a ladder run whose rungs all report the same digest, the same suite \
         and a mode matching the run must pass; transcript:\n{transcript}"
    );
    for rung in ["skill", "local", "cluster"] {
        assert!(
            has_line_with(&transcript, &[&digest, rung]),
            "the ladder must report the common bundle digest against the {rung} \
             rung by name, since the transcript IS the parity evidence an \
             operator reads; transcript:\n{transcript}"
        );
    }
    assert!(
        transcript.contains("reports-a-temperature"),
        "the ladder must name the common suite's case ids, so a reader can see \
         WHICH case set every rung shared rather than taking `parity` on \
         trust; transcript:\n{transcript}"
    );
}

/// Ref #2423: a refused primary boot must expose its diagnostic through the real
/// skill ladder, retain its exit status, and still remove the owned runner.
#[test]
fn skill_up_failure_surfaces_diagnostic_preserves_exit_and_cleans_up() {
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let invocation_log = harness.path().join("curie-invocations.log");
    let docker_log = harness.path().join("docker-invocations.log");
    let diagnostic = "stub primary skill up refused: diagnostic-marker-2423";

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "skill"),
            ("STUB_PRIMARY_SKILL_UP_ERROR", diagnostic),
            ("STUB_INVOCATION_LOG", invocation_log.to_str().unwrap()),
            ("STUB_DOCKER_INVOCATION_LOG", docker_log.to_str().unwrap()),
        ],
    );

    let transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(7),
        "the primary boot refusal must retain its exit status; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains(diagnostic),
        "the captured skill up diagnostic must reach the operator; transcript:\n{transcript}"
    );
    let invocations = fs::read_to_string(invocation_log).expect("read Curie invocations");
    let commands: Vec<_> = invocations.lines().collect();
    let primary_up = commands
        .iter()
        .position(|command| {
            *command == "skill up --plugin-dir . --image curie-runner --port 7245 --name curie-e2e-runner --fake-model"
        })
        .expect("the primary skill up must have been attempted");
    assert!(
        commands[primary_up + 1..].is_empty(),
        "no subsequent skill, eval, or local action may follow the refused boot; invocations:\n{invocations}"
    );
    let docker_invocations = fs::read_to_string(docker_log).expect("read Docker invocations");
    assert!(
        docker_invocations
            .lines()
            .any(|command| command == "rm -f curie-e2e-runner"),
        "the failure must still invoke cleanup for the owned runner; invocations:\n{docker_invocations}"
    );
}

/// #2423: the hosted MCP receipt setup must not inject a connector name that
/// `connectors.ambiguous_name` refuses. The live default skill/local lane
/// applies this function to a scratch weather copy; a forging name makes
/// `skill up` unhealthy before any turn evidence.
#[test]
fn mcp_receipt_setup_owns_a_legal_connector_name() {
    let name = ladder_quoted_assignment("MCP_RECEIPT_CONNECTOR");
    assert!(
        !connector_name_forges_mcp_join(&name),
        "MCP_RECEIPT_CONNECTOR={name:?} forges a second -mcp- in the derived object name; \
         rename it so it does not start with mcp- or contain -mcp-"
    );
    let function = ladder_function("prepare_mcp_receipt_bundle");
    assert!(
        function.contains("MCP_RECEIPT_CONNECTOR")
            && !function.contains("connectors/mcp-receipt")
            && !function.contains("  mcp-receipt:"),
        "prepare_mcp_receipt_bundle must own $MCP_RECEIPT_CONNECTOR rather than hard-coding mcp-receipt"
    );
}

/// #2423: applying the MCP receipt setup to the actual weather scratch copy
/// must leave a bundle the real validator accepts. A second apply on the same
/// owned directory must refuse rather than append a duplicate connector.
#[test]
fn mcp_receipt_setup_leaves_a_valid_owned_weather_bundle_and_refuses_a_rerun() {
    if Command::new("uv").arg("--version").output().is_err() {
        eprintln!(
            "skipping mcp_receipt_setup_leaves_a_valid_owned_weather_bundle_and_refuses_a_rerun: uv is not on PATH"
        );
        return;
    }

    let harness = tempfile::tempdir().expect("create weather scratch directory");
    let bundle = harness.path().join("bundle");
    copy_example_bundle("weather", &bundle);

    let first = run_ladder_setup_function("prepare_mcp_receipt_bundle", &bundle);
    let first_out = format!(
        "{}\n{}",
        String::from_utf8_lossy(&first.stdout),
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(
        first.status.success(),
        "the MCP receipt setup must own the weather scratch copy: {first_out}"
    );

    let result = validate_bundle_json(&bundle);
    assert_setup_bundle_has_no_boot_blockers(&result);

    let second = run_ladder_setup_function("prepare_mcp_receipt_bundle", &bundle);
    let second_out = format!(
        "{}\n{}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );
    assert_ne!(
        second.status.code(),
        Some(0),
        "a second MCP receipt setup on the same directory must refuse rather than recreate: {second_out}"
    );
    assert!(
        second_out.contains("refusing") || second_out.contains("already exists"),
        "the rerun refusal must name the collision: {second_out}"
    );
}

/// #2423/#2295: the connector-fixture setup overwrites connectors.yaml on the
/// sre-bot scratch copy. It must also own the matching approval gates and
/// toolPolicy entries so the scratch bundle stays valid. The self-upgrade
/// fixture must retain both of its gates while gates and grants for every
/// unhosted connector are removed.
#[test]
fn connector_fixture_setup_owns_consistent_approval_gates() {
    if Command::new("uv").arg("--version").output().is_err() {
        eprintln!(
            "skipping connector_fixture_setup_owns_consistent_approval_gates: uv is not on PATH"
        );
        return;
    }

    let harness = tempfile::tempdir().expect("create sre-bot scratch directory");
    let bundle = harness.path().join("bundle");
    copy_example_bundle("sre-bot", &bundle);

    let output = run_ladder_setup_function("prepare_connector_bundle", &bundle);
    let transcript = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "the connector fixture setup must own the sre-bot scratch copy: {transcript}"
    );

    let plugin: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(bundle.join(".claude-plugin/plugin.json"))
            .expect("read scratch plugin.json"),
    )
    .expect("scratch plugin.json must be JSON");
    let gates: Vec<String> = plugin["approvalPolicy"]["gates"]
        .as_array()
        .expect("approvalPolicy.gates")
        .iter()
        .map(|gate| gate["gate"].as_str().expect("gate name").to_string())
        .collect();
    assert_eq!(
        gates,
        vec![
            "mcp__self-upgrade__upgrade_self".to_string(),
            "mcp__self-upgrade__upgrade_platform".to_string(),
            "mcp__kubernetes__pods_delete".to_string(),
            "mcp__kubernetes__pods_exec".to_string(),
            "mcp__kubernetes__pods_run".to_string(),
            "mcp__kubernetes__resources_create_or_update".to_string(),
            "mcp__kubernetes__resources_delete".to_string(),
            "mcp__kubernetes__resources_scale".to_string(),
        ],
        "the owned scratch copy must retain exactly the gates for the hosted self-upgrade and kubernetes connectors"
    );
    let allow: Vec<&str> = plugin["toolPolicy"]["allow"]
        .as_array()
        .expect("toolPolicy.allow")
        .iter()
        .map(|entry| entry.as_str().expect("allow entry"))
        .collect();
    assert!(
        allow.iter().all(|entry| !entry.starts_with("grafana/")),
        "the fixture does not host grafana, so grafana grants must be stripped: {allow:?}"
    );

    let result = validate_bundle_json(&bundle);
    assert_setup_bundle_has_no_boot_blockers(&result);
}

/// NEGATIVE CONTROL: bundle identity. The cluster rung's deploy receipt reports
/// a different `bundle.sha256` than the local rung's.
///
/// Prevents shipping silently: two tiers running two different artifacts while
/// the ladder reports a green "same bundle at every tier", which is the false
/// claim in the script header this whole change repairs.
#[test]
fn parity_fails_when_a_rung_reports_a_different_bundle_digest() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_LOCAL_SHA256", PINNED_DIGEST),
            // The one knob moved off the positive control.
            ("STUB_CLUSTER_SHA256", DIVERGENT_DIGEST),
            ("STUB_FAKE_MODEL", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a rung reporting a different bundle digest must fail the ladder; \
         transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass on a digest divergence; \
         transcript:\n{transcript}"
    );
    assert!(
        has_line_with(&transcript, &[PINNED_DIGEST, DIVERGENT_DIGEST]),
        "the failure must put the pinned digest and the divergent one on one \
         line, so an operator sees the mismatch itself rather than two \
         unrelated receipts; transcript:\n{transcript}"
    );
    assert!(
        has_line_with(&transcript, &[DIVERGENT_DIGEST, "cluster"]),
        "the failure must name WHICH rung shipped the different artifact; \
         transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: suite and case count. The cluster tier's own suite loader
/// reports two cases from the weather suite when the bundle the ladder packed
/// carries one.
///
/// Prevents shipping silently: a tier grading a different suite than the one the
/// ladder deployed -- the second half of the divergence this change repairs
/// (skill graded `introduces-itself`, local and cluster graded
/// `reports-a-temperature`, and nothing compared them).
#[test]
fn parity_fails_when_a_rung_resolves_a_different_case_set() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_LOCAL_SHA256", PINNED_DIGEST),
            ("STUB_CLUSTER_SHA256", PINNED_DIGEST),
            ("STUB_FAKE_MODEL", "1"),
            // The one knob moved off the positive control.
            ("STUB_CLUSTER_PLAN_COUNT", "2"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a tier whose suite loader resolves a different case count must fail \
         the ladder; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass on a suite divergence; \
         transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("cluster")
            && transcript
                .contains(r#"grade 2 case(s) from suite "weather" against the cluster tier"#),
        "the failure must name the rung and print the raw plan line the tier \
         reported, so a harness red is distinguishable from a parity red; \
         transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: fake-versus-live mode. The run asks for live, and the
/// deployed cluster worker reports `CURIE_FAKE_MODEL=1`.
///
/// Prevents shipping silently: a "graded" nightly rung actually running sealed
/// against the fake model. The cluster rung is the target on purpose -- it is
/// the rung whose mode the script used to DISCLAIM rather than verify.
#[test]
fn parity_fails_when_the_deployed_model_mode_contradicts_the_run() {
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "cluster"),
            ("CURIE_E2E_LIVE", "1"),
            ("CURIE_CREDENTIALS", "test-credential"),
            // The one knob moved: a live run against a sealed deployment.
            ("STUB_FAKE_MODEL", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a live run against a deployment whose worker reports the fake model \
         must fail the ladder, not warn; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass on a mode contradiction; \
         transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("cluster") && transcript.contains("CURIE_FAKE_MODEL"),
        "the failure must name the rung and the observed CURIE_FAKE_MODEL value \
         read off the running artifact; transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: a mode probe that could not read anything. The run asks for
/// live and `docker inspect` on the compose worker exits non-zero.
///
/// The sibling of the control above, on the failure mode it cannot reach: there
/// the probe read a contradiction, here it read NOTHING. An absent
/// CURIE_FAKE_MODEL is legitimately live, so a probe that swallowed the failed
/// read would hand assert_model_mode an empty string and the live rung would
/// certify a mode nobody observed. LIVE is the only mode where this hides, since
/// a sealed run already refuses an empty value.
#[test]
fn parity_fails_when_the_local_mode_probe_cannot_read_the_worker_env() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local"),
            ("CURIE_E2E_LIVE", "1"),
            ("CURIE_CREDENTIALS", "test-credential"),
            ("CURIE_API_URL", &api_url),
            // No fake model, which is what a live run wants to observe.
            ("STUB_FAKE_MODEL", ""),
            // The one knob moved: the env read itself fails.
            ("STUB_DOCKER_INSPECT_EXIT", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a live run whose worker env read failed must fail the ladder rather \
         than treat the unread probe as live; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass on a mode it never read; \
         transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("local mode probe") && transcript.contains("docker inspect"),
        "the failure must say the worker env read is what failed, so an operator \
         is not sent hunting a parity divergence; transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: case ids only. Between the local rung's deploy and the
/// cluster rung's, the bundle's suite file has every case id rewritten while its
/// suite NAME and case COUNT stay identical. Both rungs' dry-run plan lines
/// therefore match the expectation exactly, and the digest is the only signal
/// that moved.
///
/// This is the control that proves the digest, not the dry-run plan line, is
/// what carries the case-id claim: the plan line carries a name and a count and
/// no ids at all, so if the ladder's case-set evidence rested on it, this run
/// would go green while the two tiers graded different cases. Deleting this test
/// makes that transitive argument unfalsifiable and lets a decorative case-id
/// claim ship.
#[test]
fn parity_fails_when_only_the_case_ids_differ() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let api_url = spawn_deployments_stub(&one_active_deployment());
    let unmutated_digest = weather_bundle_sha256();

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_FAKE_MODEL", "1"),
            // The one knob moved off the positive control. No SHA overrides
            // here: the stub reports a content-derived digest, so the
            // divergence is CAUSED by the id rewrite rather than asserted.
            ("STUB_MUTATE_CASE_IDS", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a case-id-only divergence must still fail the ladder, through bundle \
         identity; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass when two rungs graded different \
         case ids; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains(r#"grade 1 case(s) from suite "weather" against the cluster tier"#),
        "the cluster tier's plan line must still have matched the expected \
         suite and count -- that is what makes this a case-ids-only \
         divergence rather than a suite divergence; transcript:\n{transcript}"
    );
    assert!(
        has_line_with(&transcript, &[&unmutated_digest, "cluster"]),
        "the failure must be an identity failure naming the cluster rung and \
         the pinned digest of the tree the ladder packed, not a suite failure; \
         transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL: runtime binding. A stale active `prod` deployment exists
/// for the same agent alongside the `dev` row this run just created.
///
/// Prevents shipping silently the worst failure in this area: a deploy receipt
/// proves what was UPLOADED, not what the following turn RAN. The worker's
/// binding prefers `prod` over recency over the active set, so a leftover active
/// prod row serves the turn while the ladder reports the dev bundle's digest --
/// a green ladder proving the wrong artifact. Deleting this test makes the
/// sole-active-deployment assertion unfalsifiable.
#[test]
fn parity_fails_when_a_second_active_deployment_exists() {
    require_local_stub_port_free();
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    // The one knob moved off the positive control: the deployments read.
    let api_url = spawn_deployments_stub(&two_active_deployments());

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "local,cluster"),
            ("CURIE_API_URL", &api_url),
            ("STUB_LOCAL_SHA256", PINNED_DIGEST),
            ("STUB_CLUSTER_SHA256", PINNED_DIGEST),
            ("STUB_FAKE_MODEL", "1"),
        ],
    );

    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a second active deployment for the same agent must stop the ladder \
         before the turn, since the turn may bind to the wrong artifact; \
         transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass while a stale active deployment \
         could have served the turn; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains(DEPLOYMENT_ID) && transcript.contains(STALE_PROD_DEPLOYMENT_ID),
        "the failure must name every active deployment row, so the operator \
         can see which one is the shadow; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("local down --wipe --yes"),
        "the failure must carry the actionable fix line, matching the ladder's \
         other preflight-style hard failures; transcript:\n{transcript}"
    );
}

/// The inverse of the local/local-release assertions above, which pin those
/// rungs to a bare `"$BIN" local eval ...` that `set -e` makes fatal. On the
/// cluster rung the grade is REPORT ONLY (#1603): the eval still runs and still
/// prints, but a red case must not fail the rung while cluster fetch success is
/// unproved. The trajectory sidecar records requested tool identity, not
/// successful execution.
///
/// This is the successor of the #872 coverage (commit 1d266a73's third bullet),
/// which asserted the ladder EXITED 42 on a red deployed evaluator. #1603
/// deliberately reversed that half for this one rung, so what survives from #872
/// is the other half, and it is the half that mattered: the evaluator must
/// actually have been REACHED. Report only means non-fatal, never skipped.
///
/// Driven through the shared stub so the cluster rung's deploy-receipt, suite and
/// mode reads resolve deterministically instead of falling through to `exit 97`
/// or reaching the host's real `kubectl`. `STUB_EVAL_EXIT=42` keeps the injected
/// failure the same one #872 used, so the only thing that changed is what the
/// ladder is allowed to do with it.
#[test]
fn live_cluster_rung_reports_but_does_not_propagate_evaluator_failure() {
    let harness = tempfile::tempdir().expect("create harness directory");
    write_ladder_stubs(harness.path());
    let eval_marker = harness.path().join("eval_called");

    let output = run_ladder(
        harness.path(),
        &[
            ("CURIE_E2E_TIERS", "cluster"),
            ("CURIE_E2E_LIVE", "1"),
            ("CURIE_CREDENTIALS", "test-credential"),
            // A live run needs the deployed worker to report NO fake model, or
            // the mode assertion would red the run before the evaluator ran.
            ("STUB_FAKE_MODEL", ""),
            (
                "STUB_EVAL_MARKER",
                eval_marker.to_str().expect("marker path"),
            ),
            ("STUB_EVAL_EXIT", "42"),
        ],
    );

    // Stream-scoped, not the merged transcript: the tolerated-grade notice is a
    // stderr diagnostic while LADDER PASS is the stdout verdict, and a control
    // that accepted either on either stream would pass with the two swapped.
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        eval_marker.exists(),
        "the deployed evaluator must still run after the cluster message \
         plumbing succeeds -- report only means non-fatal, not skipped; \
         status: {:?}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status.code()
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "the cluster rung's eval grade is report only (#1603), so an evaluator \
         failure must NOT fail the ladder; stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("LADDER PASS"),
        "the ladder must run to completion past a red cluster eval; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("report only (#1603)"),
        "a red cluster eval must say IN THE LOG that it was tolerated, and cite \
         the ticket, so a silent tolerance is never mistaken for a pass; \
         stdout:\n{stdout}\nstderr:\n{stderr}"
    );
}

/// Cluster-only fake-model harness for namespace/release selection. Credential
/// free: the cluster rung skips the runtime-binding read without CURIE_API_KEY.
fn run_cluster_target_control(extra_envs: &[(&str, &str)]) -> (Output, String, String) {
    let harness = tempfile::tempdir().expect("create cluster target harness directory");
    write_ladder_stubs(harness.path());
    let invocation_log = harness.path().join("cluster-invocations.log");
    let kubectl_log = harness.path().join("kubectl-invocations.log");
    let invocation_log_value = invocation_log.display().to_string();
    let kubectl_log_value = kubectl_log.display().to_string();
    let mut envs = vec![
        ("CURIE_E2E_TIERS", "cluster"),
        ("STUB_FAKE_MODEL", "1"),
        ("STUB_INVOCATION_LOG", invocation_log_value.as_str()),
        ("STUB_KUBECTL_INVOCATION_LOG", kubectl_log_value.as_str()),
    ];
    envs.extend_from_slice(extra_envs);
    let output = run_ladder(harness.path(), &envs);
    let invocations = fs::read_to_string(&invocation_log).unwrap_or_default();
    let kubectl = fs::read_to_string(&kubectl_log).unwrap_or_default();
    (output, invocations, kubectl)
}

fn cluster_message_invocations(invocations: &str) -> Vec<&str> {
    invocations
        .lines()
        .filter(|line| {
            line.starts_with("--json cluster message ")
                || (line.starts_with("--json cluster --context ") && line.contains(" message "))
        })
        .collect()
}

#[test]
fn cluster_ladder_uses_the_installed_worker_budget_for_both_messages() {
    for (budget, expected) in [("600", "660"), ("900", "960")] {
        let (output, invocations, kubectl) = run_cluster_target_control(&[
            ("STUB_DELIVERY_BUDGET_S", budget),
            ("STUB_EXPECT_REPLY_TIMEOUT_SECS", expected),
        ]);
        let output_transcript = transcript(&output);
        assert_eq!(
            output.status.code(),
            Some(0),
            "worker budget {budget} plus observation headroom must pass the cluster ladder; transcript:\n{output_transcript}"
        );
        assert!(
            output_transcript.contains(&format!(
                "cluster: worker delivery budget {budget}s plus 60s reply observation headroom; waiting {expected}s"
            )),
            "the ladder must report the installed delivery budget and fixed observation headroom; transcript:\n{output_transcript}"
        );
        let messages = cluster_message_invocations(&invocations);
        assert_eq!(
            messages.len(),
            2,
            "the rung must make its two ordinary message calls exactly once each; invocations:\n{invocations}"
        );
        assert!(
            messages
                .iter()
                .all(|line| line.contains(&format!("--timeout-secs {expected}"))),
            "both ordinary message calls must use the installed worker budget plus headroom {expected}; invocations:\n{invocations}"
        );
        assert_eq!(
            kubectl
                .lines()
                .filter(|line| line.contains("deployment/curie-worker") && line.ends_with(" -o json"))
                .count(),
            1,
            "the selected worker delivery budget must be resolved once before enqueue; kubectl invocations:\n{kubectl}"
        );
    }
}

#[test]
fn cluster_ladder_rejects_invalid_worker_budgets_before_enqueue() {
    let cases = [
        ("budget_absent", "600", "absent budget"),
        ("budget_duplicate", "600", "duplicate budget"),
        ("budget_nonliteral", "600", "nonliteral budget"),
        ("valid", "six hundred", "nonnumeric budget"),
        ("valid", "59", "budget below minimum"),
        ("valid", "1801", "budget above maximum"),
    ];

    for (mode, budget, label) in cases {
        let (output, invocations, kubectl) = run_cluster_target_control(&[
            ("STUB_WORKER_ENV_MODE", mode),
            ("STUB_DELIVERY_BUDGET_S", budget),
        ]);
        let output_transcript = transcript(&output);
        assert_ne!(
            output.status.code(),
            Some(0),
            "an {label} must fail the cluster ladder; transcript:\n{output_transcript}"
        );
        assert!(
            cluster_message_invocations(&invocations).is_empty(),
            "an {label} must fail before the first cluster message is enqueued; invocations:\n{invocations}"
        );
        assert!(
            output_transcript.contains("curie-worker")
                && output_transcript.contains("CURIE_DELIVERY_BUDGET_S"),
            "an {label} failure must name the selected Deployment and offending delivery budget field; transcript:\n{output_transcript}"
        );
        assert!(
            kubectl
                .lines()
                .any(|line| line.contains("deployment/curie-worker") && line.ends_with(" -o json")),
            "an {label} control must inspect the selected worker Deployment JSON; kubectl invocations:\n{kubectl}"
        );
    }
}

fn cluster_verbs_carry_ns_rel(invocations: &str, namespace: &str, release: &str) -> bool {
    let ns_flag = format!("--namespace {namespace}");
    let rel_flag = format!("--release {release}");
    ["status", "deploy", "message", "eval"].iter().all(|verb| {
        invocations.lines().any(|line| {
            line.contains(&format!("cluster {verb}"))
                && line.contains(&ns_flag)
                && line.contains(&rel_flag)
        })
    })
}

/// POSITIVE CONTROL. Unset CURIE_NAMESPACE/CURIE_RELEASE must still select
/// curie/curie and pass those flags. Without this, CI's default install would
/// keep passing while a forgotten hardcoded target silently misses a selected
/// namespace.
#[test]
fn cluster_ladder_defaults_to_curie_namespace_and_release() {
    let (output, invocations, kubectl) = run_cluster_target_control(&[]);
    let transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "unset namespace and release must still pass the cluster ladder against curie/curie; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("LADDER PASS"),
        "the default cluster ladder must announce a pass; transcript:\n{transcript}"
    );
    assert!(
        cluster_verbs_carry_ns_rel(&invocations, "curie", "curie"),
        "cluster status, deploy, message, and eval must pass --namespace curie --release curie; invocations:\n{invocations}"
    );
    assert!(
        kubectl.contains("-n curie") && kubectl.contains("deployment/curie-worker"),
        "the fake-model probe must read the default worker; kubectl:\n{kubectl}"
    );
}

#[test]
fn cluster_ladder_uses_the_bound_channel_and_captured_context_for_retention() {
    let (output, invocations, kubectl) =
        run_cluster_target_control(&[("STUB_EXPECT_CHANNEL", "C0BOUND")]);
    let output_transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "the retention turn must use the deployed agent's sole bound channel: {output_transcript}"
    );
    assert!(
        output_transcript.contains("LADDER PASS"),
        "a bound nondefault channel must pass the cluster ladder: {output_transcript}"
    );
    assert!(
        invocations.lines().any(|line| {
            line.starts_with(&format!(
                "--json cluster --context stub-context surfaces {AGENT_ID} "
            ))
                && line.contains("--namespace curie")
                && line.contains("--release curie")
        }),
        "the ladder must query this deployed agent's bound surfaces through the captured context: {invocations}"
    );
    assert!(
        invocations.lines().any(|line| {
            line.starts_with("--json cluster --context stub-context message ")
                && line.contains("--channel C0BOUND")
                && line.contains("--thread ")
        }),
        "the post-eval message must use the surfaced channel and captured context: {invocations}"
    );
    assert!(
        !invocations.contains("--channel C0LOCALDEV"),
        "a deployed nondefault channel must reject the former fixed C0LOCALDEV turn: {invocations}"
    );
    assert!(
        kubectl.contains("--context stub-context") && kubectl.contains(" logs "),
        "the retention proof must read worker logs through the same captured context: {kubectl}"
    );
}

#[test]
fn cluster_ladder_accepts_a_finalized_retention_reply_despite_its_cli_exit_status() {
    let (output, _, _) = run_cluster_target_control(&[("STUB_RETENTION_MESSAGE_EXIT", "23")]);
    let output_transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a finalized retention reply with its timely claim proved must retain the prior passing outcome: {output_transcript}"
    );
    assert!(
        output_transcript.contains("LADDER PASS"),
        "the nonzero CLI status must not override the finalized reply contract: {output_transcript}"
    );
}

#[test]
fn cluster_ladder_still_rejects_a_nonfinal_retention_reply_after_a_valid_claim() {
    let (output, invocations, kubectl) =
        run_cluster_target_control(&[("STUB_RETENTION_REPLY_FINALIZED", "0")]);
    let output_transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "a timely worker claim must not make a nonfinal reply pass: {output_transcript}"
    );
    assert!(
        output_transcript.contains("cluster: finalized=false status=not_finalized"),
        "the existing finalized reply gate must reject the nonfinal retention payload: {output_transcript}"
    );
    assert_eq!(
        cluster_message_invocations(&invocations).len(),
        2,
        "the nonfinal control must reach the post eval message without a retry: {invocations}"
    );
    assert!(
        kubectl.contains(" logs ") && kubectl.contains("--timestamps"),
        "the nonfinal reply must be judged after the exact worker claim proof: {kubectl}"
    );
}

#[test]
fn cluster_ladder_rejects_missing_or_unrelated_post_eval_worker_claims() {
    for mode in ["missing", "unrelated"] {
        let (output, invocations, kubectl) =
            run_cluster_target_control(&[("STUB_RETENTION_CLAIM_MODE", mode)]);
        let output_transcript = transcript(&output);
        assert_ne!(
            output.status.code(),
            Some(0),
            "a {mode} worker claim must fail the cluster ladder: {output_transcript}"
        );
        assert!(
            !output_transcript.contains("LADDER PASS"),
            "the ladder must not announce a pass without this turn's exact claim: {output_transcript}"
        );
        assert!(
            output_transcript.contains("expected one exact worker claim for slack:C0LOCALDEV:"),
            "the {mode} rejection must name the missing exact worker claim: {output_transcript}"
        );
        assert!(
            invocations.lines().any(|line| {
                line.starts_with("--json cluster --context stub-context message ")
                    && line.contains("--thread ")
                    && line.contains("--timeout-secs 660")
            }),
            "the negative must drive the real post-eval message caller: {invocations}"
        );
        assert!(
            kubectl.contains(" logs ") && kubectl.contains("--timestamps"),
            "the negative must reach the timestamped worker log proof: {kubectl}"
        );
    }
}

/// POSITIVE CONTROL. A task-named namespace and nondefault release must be
/// threaded through every cluster verb and kubectl probe. The silent failure
/// this prevents: CLI ClusterConn still defaulting to curie while the selected
/// target is never contacted.
#[test]
fn cluster_ladder_threads_nondefault_namespace_and_release() {
    let (output, invocations, kubectl) = run_cluster_target_control(&[
        ("CURIE_NAMESPACE", "task-ladder-ns"),
        ("CURIE_RELEASE", "task-ladder-rel"),
        ("STUB_EXPECT_NAMESPACE", "task-ladder-ns"),
        ("STUB_EXPECT_RELEASE", "task-ladder-rel"),
    ]);
    let transcript = transcript(&output);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a selected nondefault namespace and release must pass the cluster ladder; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("LADDER PASS"),
        "the nondefault cluster ladder must announce a pass; transcript:\n{transcript}"
    );
    assert!(
        cluster_verbs_carry_ns_rel(&invocations, "task-ladder-ns", "task-ladder-rel"),
        "cluster status, deploy, message, and eval must pass the selected namespace and release; invocations:\n{invocations}"
    );
    assert!(
        kubectl.contains("-n task-ladder-ns")
            && kubectl.contains("deployment/task-ladder-rel-curie-worker"),
        "kubectl probes must hit the selected worker; kubectl:\n{kubectl}"
    );
    assert!(
        !kubectl.contains("-n curie") && !kubectl.contains("deployment/curie-worker"),
        "a nondefault control must not contact the default curie worker; kubectl:\n{kubectl}"
    );
}

/// NEGATIVE CONTROL, one knob off the default-green path: cluster status reports
/// no release for the selected target. The failure must name namespace curie and
/// release curie so an operator is not sent to the wrong install.
#[test]
fn cluster_ladder_refuses_absent_selected_release() {
    let (output, _, _) = run_cluster_target_control(&[("STUB_CLUSTER_RELEASE_FOUND", "0")]);
    let transcript = transcript(&output);
    assert_ne!(
        output.status.code(),
        Some(0),
        "an absent selected release must fail the cluster ladder; transcript:\n{transcript}"
    );
    assert!(
        !transcript.contains("LADDER PASS"),
        "the ladder must not announce a pass when the selected release is absent; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("namespace")
            && (transcript.contains("--namespace curie")
                || transcript.contains("namespace curie")
                || has_line_with(&transcript, &["namespace", "curie"])),
        "the error or fix must name the selected namespace curie; transcript:\n{transcript}"
    );
    assert!(
        transcript.contains("--release curie") || transcript.contains("release curie"),
        "the error or fix must name the selected release curie; transcript:\n{transcript}"
    );
}

/// NEGATIVE CONTROL. Invalid or empty names must fail before cluster status.
/// A DNS-invalid value that still reached kubectl would look like a missing
/// release instead of a caller error.
#[test]
fn cluster_ladder_refuses_invalid_namespace_and_release() {
    let cases: &[(&str, &str, &str)] = &[
        ("CURIE_NAMESPACE", "Not_Valid", "Not_Valid"),
        ("CURIE_NAMESPACE", "", "CURIE_NAMESPACE"),
        ("CURIE_RELEASE", "Bad_Name", "Bad_Name"),
    ];
    for (variable, value, named) in cases {
        let (output, invocations, _) = run_cluster_target_control(&[(variable, value)]);
        let transcript = transcript(&output);
        assert_ne!(
            output.status.code(),
            Some(0),
            "{variable}={value:?} must fail the cluster ladder before any cluster verb; transcript:\n{transcript}"
        );
        assert!(
            !transcript.contains("LADDER PASS"),
            "{variable}={value:?} must not announce a pass; transcript:\n{transcript}"
        );
        assert!(
            transcript.contains(named),
            "{variable}={value:?} must name the invalid value {named:?} in the error or fix; transcript:\n{transcript}"
        );
        assert!(
            !invocations.contains("cluster status"),
            "{variable}={value:?} must refuse before cluster status; invocations:\n{invocations}"
        );
    }
}

// Issue #2204: Langfuse 3.225.5 maps any OTel span carrying `gen_ai.tool.name`
// to observation type `TOOL` and renames the observation to the tool name, so a
// real tool call never arrives under an observation literally named
// `execute_tool`. The sanitizer's name-keyed membership and its
// SPAN/GENERATION/EVENT type allowlist therefore cannot see any tool call at
// all. These regressions execute the ladder's own projection python against a
// crafted Langfuse tree, because a text-only pin survives the weakening
// mutation that caused the defect.

/// Extract `sanitize_exact_trace_read`'s embedded python, the real consumer of
/// the exact-trace response, so these assertions run the shipped projection
/// rather than a Rust restatement of it.
fn exact_trace_sanitizer_python() -> String {
    let function = ladder_function("sanitize_exact_trace_read");
    let (_, script) = function
        .split_once("<<'PY'\n")
        .expect("the exact-trace sanitizer must retain its Python heredoc");
    script
        .split_once("\nPY\n")
        .expect("the exact-trace sanitizer heredoc must have a closing delimiter")
        .0
        .to_owned()
}

fn run_exact_trace_sanitizer(trace_id: &str, response: &serde_json::Value) -> Output {
    let script = exact_trace_sanitizer_python();
    let harness = tempfile::tempdir().expect("create exact-trace fixture directory");
    let source = harness.path().join("exact-trace.json");
    fs::write(
        &source,
        serde_json::to_string(response).expect("serialize exact-trace fixture"),
    )
    .expect("write exact-trace fixture");
    let mut child = Command::new("python3")
        .arg("-")
        .arg(trace_id)
        .arg(&source)
        .arg("")
        .arg("")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start extracted exact-trace sanitizer");
    child
        .stdin
        .take()
        .expect("open sanitizer stdin")
        .write_all(script.as_bytes())
        .expect("write extracted exact-trace sanitizer");
    child.wait_with_output().expect("wait for sanitizer")
}

/// A realistic Langfuse read of one coding turn: the tool observation carries
/// the TOOL type and is named for the tool itself, exactly as Langfuse renames
/// it, with `execute_tool` surviving only inside the span attributes.
fn langfuse_tool_trace(trace_id: &str, tool_name: &str) -> serde_json::Value {
    use serde_json::json;
    json!({
        "trace": {"id": trace_id},
        "tree": [{
            "id": "01",
            "type": "SPAN",
            "name": "curie.queue.enqueue",
            "children": [{
                "id": "02",
                "type": "SPAN",
                "name": "curie.turn.process",
                "children": [{
                    "id": "03",
                    "type": "SPAN",
                    "name": "curie.sandbox.claim",
                    "children": [{
                        "id": "04",
                        "type": "SPAN",
                        "name": "curie.runner.rpc",
                        "children": [{
                            "id": "05",
                            "type": "SPAN",
                            "name": "agent.run",
                            "children": [{
                                "id": "06",
                                "type": "TOOL",
                                "name": tool_name,
                                "toolName": tool_name,
                                "metadata": {"attributes": {
                                    "gen_ai.operation.name": "execute_tool",
                                    "gen_ai.tool.name": tool_name,
                                    "curie.phase": "tool_wait",
                                }},
                                "children": [],
                            }],
                        }],
                    }],
                }],
            }],
        }],
    })
}

fn sanitized_evidence(trace_id: &str, response: &serde_json::Value) -> serde_json::Value {
    let output = run_exact_trace_sanitizer(trace_id, response);
    assert!(
        output.status.success(),
        "the exact-trace sanitizer must accept a real Langfuse tool observation; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "the sanitizer must print one JSON evidence object ({error}); stdout:\n{}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

#[test]
fn sanitizer_reports_execute_tool_for_a_renamed_langfuse_tool_observation() {
    let trace_id = "00000000000000000000000000002204";
    let evidence = sanitized_evidence(trace_id, &langfuse_tool_trace(trace_id, "Bash"));
    let operations = evidence["operation"]
        .as_array()
        .expect("sanitized evidence must carry an operation array");
    assert!(
        operations.iter().any(|op| op == "execute_tool"),
        "a TOOL observation renamed to its tool is the execute_tool operation, so name-keyed membership must not be the only path; evidence: {evidence}"
    );
    let services = evidence["service"]
        .as_array()
        .expect("sanitized evidence must carry a service array");
    assert!(
        services.iter().any(|service| service == "curie-runner"),
        "an observed tool call must attribute the runner service; evidence: {evidence}"
    );
}

#[test]
fn sanitizer_allowlists_the_tool_observation_type() {
    let trace_id = "00000000000000000000000000002204";
    let evidence = sanitized_evidence(trace_id, &langfuse_tool_trace(trace_id, "Bash"));
    let types = evidence["observation_type"]
        .as_array()
        .expect("sanitized evidence must carry an observation_type array");
    assert!(
        types.iter().any(|kind| kind == "TOOL"),
        "TOOL is a real Langfuse observation type and must not be excluded from the type allowlist; evidence: {evidence}"
    );
}

#[test]
fn sanitizer_projects_the_observed_tool_identity() {
    let trace_id = "00000000000000000000000000002204";
    let evidence = sanitized_evidence(trace_id, &langfuse_tool_trace(trace_id, "Bash"));
    let tools = evidence["tool_name"]
        .as_array()
        .expect("sanitized evidence must project the observed tool names under tool_name");
    assert!(
        tools.iter().any(|tool| tool == "Bash"),
        "evidence must say WHICH tool ran, not merely that some tool observation existed; evidence: {evidence}"
    );

    let sanitizer = ladder_function("sanitize_exact_trace_read");
    assert!(
        sanitizer.contains("\"tool_name\""),
        "tool_name must be an allowlisted evidence field, or the sanitizer's own closed-schema check rejects it"
    );
}

#[test]
fn sanitizer_and_exact_query_fail_closed_on_a_surviving_unrelated_tool() {
    let trace_id = "00000000000000000000000000002204";
    let evidence = sanitized_evidence(
        trace_id,
        &langfuse_tool_trace(trace_id, "mcp__receipt__receipt_read"),
    );
    let tools = evidence["tool_name"]
        .as_array()
        .expect("sanitized evidence must project the observed tool names under tool_name");
    assert!(
        !tools.iter().any(|tool| tool == "Bash"),
        "dropping only the Bash span while another tool survives must not satisfy a required Bash tool; evidence: {evidence}"
    );

    let query = ladder_function("query_exact_seed_trace");
    assert!(
        query.contains("expected_tool"),
        "the exact query must be able to require a specific tool identity in the exact trace"
    );
    assert!(
        query.contains("tool_name"),
        "the exact query must check its required tool against the sanitized tool_name evidence, not a raw private read"
    );
}

#[test]
fn coding_tool_seed_requires_the_bash_tool_by_name_in_its_exact_trace() {
    let coding = ladder_function("seed_coding_tool_turn");
    let query_line = coding
        .lines()
        .find(|line| line.contains("query_exact_seed_trace"))
        .expect("the coding seed must query its own exact trace");
    assert!(
        query_line.contains("Bash"),
        "the coding seed must require the Bash tool identity, since bare execute_tool existence is satisfied by any other tool: {query_line}"
    );
}

#[test]
fn cli_observation_node_carries_the_langfuse_tool_name() {
    let api = fs::read_to_string(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/api.rs"))
        .expect("read the CLI API module");
    let (_, node) = api
        .split_once("pub struct ObservationNode {")
        .expect("the CLI must model the observation tree node");
    let node = node
        .split_once("\n}\n")
        .expect("ObservationNode must close")
        .0;
    assert!(
        node.contains(r#"rename = "toolName""#) && node.contains("tool_name"),
        "the CLI must carry the hoisted toolName instead of silently stripping the tool identity the API now returns: {node}"
    );
}

const CLUSTER_ORDINARY_MARKER: &str = "curie-seed-external-ordinary-receipt";
const CLUSTER_MCP_MARKER: &str = "curie-seed-external-mcp-receipt";
const CLUSTER_CODING_MARKER: &str = "curie-seed-external-coding-receipt";
const CLUSTER_APPROVAL_MARKER: &str = "curie-seed-external-approval-receipt";

const CLUSTER_ORDINARY_TRACE: &str = "10000000000000000000000000000001";
const CLUSTER_MCP_TRACE: &str = "20000000000000000000000000000002";
const CLUSTER_CODING_TRACE: &str = "30000000000000000000000000000003";
const CLUSTER_APPROVAL_TRACE: &str = "40000000000000000000000000000004";

fn cluster_receipt_seed(
    kind: &str,
    marker: &str,
    start: &str,
    end: &str,
    accepted: f64,
    sent: f64,
) -> serde_json::Value {
    serde_json::json!({
        "kind": kind,
        "marker": marker,
        "stream_start": start,
        "stream_end": end,
        "reply_observed": true,
        "completion_observed": true,
        "otelcol_receiver_accepted_spans_delta": accepted,
        "otelcol_exporter_sent_spans_delta": sent,
    })
}

fn cluster_external_receipt(live: bool, coding_receipt: Option<&str>) -> serde_json::Value {
    let ordinary = cluster_receipt_seed(
        "ordinary",
        CLUSTER_ORDINARY_MARKER,
        "20-0",
        "21-0",
        1.0,
        2.0,
    );
    let mut seeds = vec![ordinary];
    if live {
        let mut mcp = cluster_receipt_seed("mcp", CLUSTER_MCP_MARKER, "30-0", "31-0", 2.0, 3.0);
        mcp["mcp_call_count_delta"] = serde_json::json!(1);
        seeds.push(mcp);
        if let Some(receipt) = coding_receipt {
            let mut coding =
                cluster_receipt_seed("coding", CLUSTER_CODING_MARKER, "40-0", "41-0", 4.0, 5.0);
            if !receipt.is_empty() {
                coding["coding_execution_receipt"] = serde_json::json!(receipt);
            }
            seeds.push(coding);
        }
    } else {
        let mut approval = cluster_receipt_seed(
            "approval",
            CLUSTER_APPROVAL_MARKER,
            "50-0",
            "51-0",
            2.0,
            3.0,
        );
        approval["approval_transition_observed"] = serde_json::json!(true);
        seeds.push(approval);
    }
    serde_json::json!({"run_id": "run-cluster-receipts", "seeds": seeds})
}

fn cluster_trace(
    trace_id: &str,
    operations: &[&str],
    tool_name: Option<&str>,
    approval_decision: Option<&str>,
) -> serde_json::Value {
    let mut tree = operations
        .iter()
        .enumerate()
        .map(|(index, operation)| {
            serde_json::json!({
                "id": format!("span-{index}"),
                "type": "SPAN",
                "name": operation,
                "children": [],
            })
        })
        .collect::<Vec<_>>();
    if let Some(tool) = tool_name {
        tree.push(serde_json::json!({
            "id": "tool-1",
            "type": "TOOL",
            "name": tool,
            "toolName": tool,
            "children": [],
        }));
    }
    serde_json::json!({
        "trace": {"id": trace_id},
        "tree": tree,
        "approval_decision": approval_decision,
    })
}

fn cluster_stream_rows() -> serde_json::Value {
    let seeds = [
        ("21-0", CLUSTER_ORDINARY_MARKER, CLUSTER_ORDINARY_TRACE),
        ("31-0", CLUSTER_MCP_MARKER, CLUSTER_MCP_TRACE),
        ("41-0", CLUSTER_CODING_MARKER, CLUSTER_CODING_TRACE),
        ("51-0", CLUSTER_APPROVAL_MARKER, CLUSTER_APPROVAL_TRACE),
    ];
    serde_json::Value::Array(
        seeds
            .into_iter()
            .map(|(entry_id, marker, trace_id)| {
                serde_json::json!([
                    entry_id,
                    [
                        "payload",
                        serde_json::json!({
                            "text": format!("Slack correlation {marker}"),
                            "reply_handle": {"kind": "slack"},
                        })
                        .to_string(),
                        "traceparent",
                        format!("00-{trace_id}-1111111111111111-01"),
                    ]
                ])
            })
            .collect(),
    )
}

fn run_cluster_receipt_consumers(
    receipt: &serde_json::Value,
    coding_tool: &str,
    command: &str,
) -> (Output, String, Option<serde_json::Value>) {
    let harness = tempfile::tempdir().expect("create cluster receipt harness");
    let receipt_path = harness.path().join("receipt.json");
    fs::write(
        &receipt_path,
        serde_json::to_vec(receipt).expect("serialize cluster receipt"),
    )
    .expect("write cluster receipt");
    let mut receipt_permissions = fs::metadata(&receipt_path)
        .expect("read cluster receipt metadata")
        .permissions();
    receipt_permissions.set_mode(0o600);
    fs::set_permissions(&receipt_path, receipt_permissions).expect("protect cluster receipt");

    fs::write(
        harness.path().join("stream.json"),
        serde_json::to_vec(&cluster_stream_rows()).expect("serialize cluster stream"),
    )
    .expect("write cluster stream");
    let traces = harness.path().join("traces");
    fs::create_dir(&traces).expect("create trace fixture directory");
    let fixtures = [
        (
            CLUSTER_ORDINARY_TRACE,
            cluster_trace(
                CLUSTER_ORDINARY_TRACE,
                &[
                    "curie.turn.ingress",
                    "curie.queue.enqueue",
                    "curie.queue.process",
                    "curie.turn.process",
                    "curie.sandbox.claim",
                    "curie.runner.rpc",
                    "agent.run",
                    "curie.reply.post",
                ],
                None,
                None,
            ),
        ),
        (
            CLUSTER_MCP_TRACE,
            cluster_trace(
                CLUSTER_MCP_TRACE,
                &[
                    "curie.turn.ingress",
                    "curie.queue.enqueue",
                    "curie.reply.post",
                ],
                Some("mcp__receipt__receipt_read"),
                None,
            ),
        ),
        (
            CLUSTER_CODING_TRACE,
            cluster_trace(
                CLUSTER_CODING_TRACE,
                &[
                    "curie.turn.ingress",
                    "curie.queue.enqueue",
                    "curie.reply.post",
                ],
                Some(coding_tool),
                None,
            ),
        ),
        (
            CLUSTER_APPROVAL_TRACE,
            cluster_trace(
                CLUSTER_APPROVAL_TRACE,
                &[
                    "curie.turn.ingress",
                    "curie.queue.enqueue",
                    "curie.approval.suspend",
                    "curie.approval.resolve",
                    "curie.approval.resume",
                    "curie.reply.post",
                ],
                None,
                Some("approved"),
            ),
        ),
    ];
    for (trace_id, fixture) in fixtures {
        fs::write(
            traces.join(format!("{trace_id}.json")),
            serde_json::to_vec(&fixture).expect("serialize trace fixture"),
        )
        .expect("write trace fixture");
    }

    write_executable(
        &harness.path().join("curie"),
        r#"#!/bin/sh
set -eu
printf '%s\n' "$*" >> "$BOUNDARY_LOG"
last=""
for arg in "$@"; do last="$arg"; done
case "$*" in
    *" cluster observability "*" run "*)
        cat "$TRACE_FIXTURES/$last.json"
        ;;
    *) exit 97 ;;
esac
"#,
    );

    let functions = [
        "cluster_external_ingress_seed",
        "discover_cluster_external_trace_id",
        "sanitize_exact_trace_read",
        "query_exact_seed_trace",
        "write_product_observability_evidence",
        "run_cluster_product_observability",
    ]
    .into_iter()
    .map(|name| {
        if name == "write_product_observability_evidence" {
            ladder_function_before(name, "run_cluster_product_observability")
        } else {
            ladder_function(name)
        }
    })
    .collect::<Vec<_>>()
    .join("\n");
    let script = format!(
        r#"set -euo pipefail
WORKDIR="$1"
BIN="$WORKDIR/curie"
CLUSTER_EXTERNAL_INGRESS_RECEIPT="$WORKDIR/receipt.json"
CLUSTER_PRODUCT_EVIDENCE="$WORKDIR/evidence.json"
PRODUCT_OBSERVABILITY_RUN_ID=run-cluster-receipts
OBSERVABILITY_POLL_ATTEMPTS=1
OBSERVABILITY_POLL_INTERVAL_SECONDS=0
CURIE_NAMESPACE=test-cluster-receipts
CURIE_RELEASE=test-cluster-receipts
CLUSTER_IMAGE_IDS_MATCH=false
LIVE="${{HARNESS_LIVE:-1}}"
ns_rel=(--namespace "$CURIE_NAMESPACE" --release "$CURIE_RELEASE")
preflight_cluster_product_observability() {{ CLUSTER_IMAGE_IDS_MATCH=true; }}
seed_cluster_missing_carrier_control() {{ :; }}
product_stream_json() {{
    printf '%s\n' "stream $*" >> "$BOUNDARY_LOG"
    cat "$WORKDIR/stream.json"
}}
{functions}
{command}
"#,
    );
    let boundary_log = harness.path().join("boundaries.log");
    let path = format!(
        "{}:{}",
        harness.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let output = Command::new("bash")
        .arg("-c")
        .arg(script)
        .arg("cluster receipt harness")
        .arg(harness.path())
        .env("PATH", path)
        .env("BOUNDARY_LOG", &boundary_log)
        .env("TRACE_FIXTURES", &traces)
        .env(
            "HARNESS_LIVE",
            if receipt["seeds"]
                .as_array()
                .is_some_and(|seeds| seeds.iter().any(|seed| seed["kind"] == "approval"))
            {
                "0"
            } else {
                "1"
            },
        )
        .output()
        .expect("run extracted cluster receipt consumers");
    let boundaries = fs::read_to_string(&boundary_log).unwrap_or_default();
    let evidence = fs::read_to_string(harness.path().join("evidence.json"))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok());
    (output, boundaries, evidence)
}

fn correct_cluster_coding_receipt() -> String {
    Sha256::digest(CLUSTER_CODING_MARKER.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn cluster_external_coding_receipt_rejects_wrong_or_missing_digest_before_telemetry() {
    let correct = correct_cluster_coding_receipt();
    let positive_receipt = cluster_external_receipt(true, Some(&correct));
    let (positive, boundaries, _) = run_cluster_receipt_consumers(
        &positive_receipt,
        "Bash",
        r#"cluster_external_ingress_seed coding "execute_tool"
printf 'membership=%s\n' "$LAST_QUERY_MEMBERSHIP""#,
    );
    assert!(
        positive.status.success(),
        "a bounded Slack entry with the marker digest and Bash observation must pass: {}",
        transcript(&positive)
    );
    assert!(
        String::from_utf8_lossy(&positive.stdout).contains("membership=true"),
        "the positive coding seed must establish exact Bash membership: {}",
        transcript(&positive)
    );
    assert!(
        boundaries.contains("stream cluster XRANGE curie:runs (40-0 41-0")
            && boundaries.contains(&format!("run {CLUSTER_CODING_TRACE}")),
        "the positive must consume its bounded Slack entry and exact trace: {boundaries}"
    );

    for (label, receipt) in [
        (
            "wrong",
            cluster_external_receipt(true, Some(&"0".repeat(64))),
        ),
        ("missing", cluster_external_receipt(true, Some(""))),
    ] {
        let (output, boundaries, _) = run_cluster_receipt_consumers(
            &receipt,
            "Bash",
            r#"cluster_external_ingress_seed coding "execute_tool""#,
        );
        assert!(
            !output.status.success(),
            "a {label} coding digest must be rejected: {}",
            transcript(&output)
        );
        assert!(
            boundaries.is_empty(),
            "a {label} coding digest must fail before stream or telemetry access: {boundaries}"
        );
    }
}

#[test]
fn cluster_external_coding_seed_rejects_an_unrelated_tool_with_execute_tool_present() {
    let receipt = cluster_external_receipt(true, Some(&correct_cluster_coding_receipt()));
    let (output, _, _) = run_cluster_receipt_consumers(
        &receipt,
        "mcp__receipt__receipt_read",
        r#"cluster_external_ingress_seed coding "execute_tool"
printf 'membership=%s\n' "$LAST_QUERY_MEMBERSHIP""#,
    );
    assert!(
        output.status.success(),
        "an observable but incomplete exact trace must return evidence: {}",
        transcript(&output)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("membership=false"),
        "execute_tool from an unrelated tool must not satisfy the coding seed: {}",
        transcript(&output)
    );
}

#[test]
fn cluster_product_observability_requires_coding_and_aggregates_all_live_seeds() {
    let receipt = cluster_external_receipt(true, Some(&correct_cluster_coding_receipt()));
    let (positive, _, evidence) = run_cluster_receipt_consumers(
        &receipt,
        "Bash",
        r#"run_cluster_product_observability agent-id agent-name"#,
    );
    assert!(
        positive.status.success(),
        "ordinary, MCP, and coding receipts must pass together: {}",
        transcript(&positive)
    );
    let evidence = evidence.expect("live aggregate must write evidence");
    assert_eq!(evidence["otelcol_receiver_accepted_spans"], 7.0);
    assert_eq!(evidence["otelcol_exporter_sent_spans"], 10.0);
    assert_eq!(evidence["langfuse_observation_membership"], true);

    let missing = cluster_external_receipt(true, None);
    let (output, _, _) = run_cluster_receipt_consumers(
        &missing,
        "Bash",
        r#"run_cluster_product_observability agent-id agent-name"#,
    );
    assert!(
        !output.status.success(),
        "a live aggregate without the coding seed must fail: {}",
        transcript(&output)
    );

    let (unrelated, _, evidence) = run_cluster_receipt_consumers(
        &receipt,
        "mcp__receipt__receipt_read",
        r#"run_cluster_product_observability agent-id agent-name"#,
    );
    assert!(
        unrelated.status.success(),
        "the aggregate must retain negative membership evidence: {}",
        transcript(&unrelated)
    );
    assert_eq!(
        evidence.expect("negative aggregate evidence")["langfuse_observation_membership"],
        false,
        "an unrelated tool must force aggregate membership false"
    );
}

#[test]
fn cluster_product_observability_preserves_the_fake_approval_path() {
    let receipt = cluster_external_receipt(false, None);
    let (output, _, evidence) = run_cluster_receipt_consumers(
        &receipt,
        "Bash",
        r#"run_cluster_product_observability agent-id agent-name"#,
    );
    assert!(
        output.status.success(),
        "the fake path must still require ordinary plus approved resume evidence: {}",
        transcript(&output)
    );
    let evidence = evidence.expect("fake aggregate evidence");
    assert_eq!(evidence["otelcol_receiver_accepted_spans"], 3.0);
    assert_eq!(evidence["otelcol_exporter_sent_spans"], 5.0);
    assert_eq!(evidence["langfuse_observation_membership"], true);
}
