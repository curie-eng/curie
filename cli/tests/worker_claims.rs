//! Issue #2374: operator surfaces must report the worker's claim gate without
//! guessing from pod health or leaking the status subprocess's output.
//!
//! These are public-surface tests.  The real `curie` binary talks to recording
//! Docker/kubectl/Helm shims, and the message cases use the same tiny
//! port-forward servers as the existing cluster-message integration fixture.
//! No test imports the observer module: reverting only the product
//! implementation still compiles this file and makes the observable assertions
//! red.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

const NAMESPACE: &str = "acme-claims";
const RELEASE: &str = "acme-claims";
const WORKER_POD: &str = "acme-claims-worker-current";
const OLD_COMPLETED_HOOK: &str = "acme-claims-upgrade-drain-completed";
const OLD_STUCK_HOOK: &str = "acme-claims-upgrade-drain-stuck";
const WORKER_SELECTOR: &str =
    "app.kubernetes.io/instance=acme-claims,app.kubernetes.io/component=worker";
const SINCE_17: &str = "2026-09-08T12:34:56Z";
const SINCE_18: &str = "2026-09-08T12:35:57Z";
const STATUS_WAITING_17: &str = "worker waiting for upgrade revision 17 since 2026-09-08T12:34:56Z";
const WAITING_17: &str = "waiting for upgrade revision 17 since 2026-09-08T12:34:56Z";
const WAITING_18: &str = "waiting for upgrade revision 18 since 2026-09-08T12:35:57Z";
const STATUS_QUIESCING_WITHOUT_METADATA: &str =
    "worker quiescing for upgrade; marker metadata unavailable";
const WAITING_WITHOUT_METADATA: &str = "waiting for upgrade; marker metadata unavailable";
const PRIVATE_SENTINEL: &str = "private-status-output-must-not-escape";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn write_executable(path: &Path, body: &str) {
    fs::write(path, body).expect("write tool shim");
    let mut permissions = fs::metadata(path)
        .expect("read shim metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("make tool shim executable");
}

fn shim_path(tools: &Path) -> OsString {
    let mut paths = vec![tools.to_path_buf()];
    paths.extend(["/usr/bin", "/bin"].iter().map(PathBuf::from));
    std::env::join_paths(paths).expect("join shim PATH")
}

fn describe(output: &Output) -> String {
    format!(
        "status: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn json_output(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout is not one JSON object ({error}): {}",
            describe(output)
        )
    })
}

fn claim_reply(
    state: &str,
    since: serde_json::Value,
    revision: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "stdout": serde_json::json!({
            "state": state,
            "since": since,
            "revision": revision,
        }).to_string(),
        "stderr": "",
        "exit": 0,
        "sleep": 0,
    })
}

fn claims_enabled() -> serde_json::Value {
    claim_reply(
        "claims_enabled",
        serde_json::Value::Null,
        serde_json::Value::Null,
    )
}

fn quiescing(since: &str, revision: u64) -> serde_json::Value {
    claim_reply(
        "quiescing",
        serde_json::Value::String(since.to_string()),
        serde_json::json!(revision),
    )
}

fn quiescing_without_metadata() -> serde_json::Value {
    let mut response = claim_reply(
        "quiescing",
        serde_json::Value::Null,
        serde_json::Value::Null,
    );
    response["stderr"] = serde_json::json!(PRIVATE_SENTINEL);
    response
}

fn unknown() -> serde_json::Value {
    claim_reply("unknown", serde_json::Value::Null, serde_json::Value::Null)
}

/// One shim body serves the operator reports and the message fixture.  Every
/// command is recorded as JSON so assertions do not depend on shell quoting.
/// The selector intentionally returns two same-image hook pods in addition to
/// the real worker: a client that selects by image/name, or trusts the shim to
/// enforce labels, will exec the wrong pod and fail loudly.
const TOOL_SHIM: &str = r###"#!/usr/bin/env python3
import fcntl
import json
import os
import shlex
import socket
import sys
import threading
import time
from urllib.parse import urlparse

args = sys.argv[1:]
tool = os.path.basename(sys.argv[0])
root = os.environ["CURIE_TEST_WORKER_CLAIMS_ROOT"]
calls_path = os.path.join(root, "calls.log")
WORKER_SELECTOR = "app.kubernetes.io/instance=acme-claims,app.kubernetes.io/component=worker"

def record(kind, argv=None):
    row = {
        "kind": kind,
        "tool": tool,
        "argv": args if argv is None else argv,
        "compose_project": os.environ.get("COMPOSE_PROJECT_NAME"),
    }
    with open(calls_path, "a", encoding="utf-8") as log:
        fcntl.flock(log, fcntl.LOCK_EX)
        log.write(json.dumps(row, separators=(",", ":")) + "\n")
        log.flush()

record("command")

def has_sequence(parts):
    if len(parts) > len(args):
        return False
    return any(args[i:i + len(parts)] == parts for i in range(len(args) - len(parts) + 1))

def next_claim():
    count_path = os.path.join(root, "claim-count")
    with open(count_path, "r+", encoding="utf-8") as counter:
        fcntl.flock(counter, fcntl.LOCK_EX)
        raw = counter.read().strip()
        index = int(raw or "0")
        counter.seek(0)
        counter.write(str(index + 1))
        counter.truncate()
    with open(os.path.join(root, "claim-sequence.json"), encoding="utf-8") as source:
        sequence = json.load(source)
    reply = sequence[min(index, len(sequence) - 1)]
    time.sleep(float(reply.get("sleep", 0)))
    sys.stdout.write(reply.get("stdout", ""))
    sys.stderr.write(reply.get("stderr", ""))
    sys.exit(int(reply.get("exit", 0)))

def pod(name, component, phase, ready, image="ghcr.io/curie-eng/curie-worker:test"):
    return {
        "metadata": {
            "name": name,
            "labels": {
                "app.kubernetes.io/instance": "acme-claims",
                "app.kubernetes.io/component": component,
            },
        },
        "spec": {"containers": [{"name": "worker", "image": image}]},
        "status": {
            "phase": phase,
            "containerStatuses": [{
                "name": "worker",
                "image": image,
                "imageID": "docker-pullable://ghcr.io/curie-eng/curie-worker@sha256:" + "a" * 64,
                "ready": ready,
                "state": {"running": {}} if ready else {"terminated": {"exitCode": 0, "reason": "Completed"}},
            }],
        },
    }

worker = pod("acme-claims-worker-current", "worker", "Running", True)
selector_pods = {
    "items": [
        pod("acme-claims-upgrade-drain-completed", "upgrade-drain", "Succeeded", False),
        pod("acme-claims-upgrade-drain-stuck", "upgrade-drain", "Running", True),
        pod("acme-claims-worker-pending", "worker", "Pending", False),
        worker,
    ]
}

is_outbox_exec = "exec" in args and has_sequence([
    "python", "-m", "curie_worker.completion_health", "--json"
])
if is_outbox_exec:
    if tool == "kubectl" and "acme-claims-worker-current" not in args:
        print("outbox probe selected a hook or non-running pod", file=sys.stderr)
        sys.exit(64)
    sys.stdout.write('{"count":0,"oldest_age_s":0.0,"inflight":0,"retry":0,"terminal":0,"state":"empty","degraded":false}\n')
    sys.exit(0)

is_claim_exec = "exec" in args and has_sequence([
    "python", "-m", "curie_worker.upgrade_drain", "--mode", "status", "--json"
])
if is_claim_exec:
    if tool == "kubectl" and "acme-claims-worker-current" not in args:
        print("claim probe selected a hook or non-running pod", file=sys.stderr)
        sys.exit(64)
    if tool == "docker":
        exact_project = os.environ.get("COMPOSE_PROJECT_NAME") == "curie" or has_sequence(["-p", "curie"])
        if "curie-worker" not in args or not exact_project:
            print("claim probe selected the wrong compose project/service", file=sys.stderr)
            sys.exit(64)
    record("claim_exec")
    next_claim()

if tool == "kubectl":
    joined = " ".join(args)
    if "port-forward" in args:
        requested, remote = args[-1].split(":", 1)
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", int(requested)))
        listener.listen()
        print("Forwarding from 127.0.0.1:%d -> %s" % (listener.getsockname()[1], remote), flush=True)
        is_valkey = "valkey" in joined

        def read_resp(stream):
            prefix = stream.read(1)
            if not prefix:
                return None
            line = stream.readline().rstrip(b"\r\n")
            if prefix == b"*":
                return [read_resp(stream) for _ in range(int(line))]
            if prefix == b"$":
                length = int(line)
                if length < 0:
                    return None
                value = stream.read(length)
                stream.read(2)
                return value
            if prefix in (b"+", b":"):
                return line
            return None

        def bulk(value):
            value = value.encode("utf-8")
            return b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"

        def serve_valkey(client):
            try:
                stream = client.makefile("rb")
                while True:
                    command = read_resp(stream)
                    if command is None:
                        return
                    words = [part.decode("utf-8") if isinstance(part, bytes) else str(part) for part in command]
                    verb = words[0].upper()
                    if verb == "PING":
                        response = b"+PONG\r\n"
                    elif verb in ("AUTH", "SELECT") or verb == "CLIENT":
                        response = b"+OK\r\n"
                    elif verb == "XADD":
                        response = bulk("101-0")
                    elif verb == "QUIT":
                        response = b"+OK\r\n"
                    else:
                        response = b"-ERR unsupported fixture command " + verb.encode() + b"\r\n"
                    client.sendall(response)
            finally:
                client.close()

        def serve_http(client):
            try:
                stream = client.makefile("rb")
                request_line = stream.readline().decode("latin1").strip()
                if not request_line:
                    return
                method, raw_path, _ = request_line.split(" ", 2)
                while True:
                    line = stream.readline()
                    if line in (b"\r\n", b"\n", b""):
                        break
                parsed = urlparse(raw_path)
                record("http_get", [method, parsed.path, parsed.query])
                body = b'{"events":[],"next_cursor":0,"terminal":false}'
                client.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                    + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body
                )
            finally:
                client.close()

        handler = serve_valkey if is_valkey else serve_http
        while True:
            client, _ = listener.accept()
            threading.Thread(target=handler, args=(client,), daemon=True).start()

    if has_sequence(["config", "current-context"]):
        print("claims-test-context")
        sys.exit(0)
    if has_sequence(["config", "view"]):
        print("https://127.0.0.1:6443", end="")
        sys.exit(0)
    if "get" in args and ("pod" in args or "pods" in args) and any(WORKER_SELECTOR in value for value in args):
        time.sleep(float(os.environ.get("CURIE_TEST_SELECTOR_SLEEP", "0")))
        print(json.dumps(selector_pods, separators=(",", ":")))
        sys.exit(0)
    if has_sequence(["get", "pods"]) and "-o" in args and "json" in args:
        print(json.dumps({"items": [worker]}, separators=(",", ":")))
        sys.exit(0)
    if "get" in args and "app.kubernetes.io/component=api" in joined:
        print("acme-claims-curie-api", end="")
        sys.exit(0)
    if "get" in args and "secret" in args:
        print("", end="")
        sys.exit(0)
    if "get" in args and "deployment" in args and "dispatcher" in joined:
        print("", end="")
        sys.exit(0)
    if has_sequence(["get", "svc", "acme-claims-curie-ui"]):
        print('{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":30080}]}}')
        sys.exit(0)
    if has_sequence(["get", "svc", "acme-claims-curie-langfuse-web"]):
        print('{"spec":{"type":"ClusterIP","ports":[{"port":3000}]}}')
        sys.exit(0)
    if "deployments,statefulsets,daemonsets,pods,jobs" in args:
        print('{"items":[{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"acme-probe","generation":1},"spec":{"replicas":1,"selector":{"matchLabels":{"component":"acme-probe"}},"template":{"spec":{"containers":[{"name":"probe","image":"busybox:1"}]}}},"status":{"observedGeneration":1,"replicas":1,"updatedReplicas":1,"readyReplicas":1}},{"kind":"Pod","metadata":{"name":"acme-probe-pod","labels":{"component":"acme-probe"}},"spec":{"containers":[{"name":"probe","image":"busybox:1"}]},"status":{"phase":"Running","containerStatuses":[{"name":"probe","image":"busybox:1","imageID":"containerd://sha256:example","ready":true,"state":{"running":{}}}]}}]}')
        sys.exit(0)
    if has_sequence(["get", "nodes"]):
        print('{"items":[]}')
        sys.exit(0)
    print("unexpected kubectl invocation: " + shlex.join(args), file=sys.stderr)
    sys.exit(64)

if tool == "helm":
    if args and args[0] == "version":
        print("v3.17.3")
        sys.exit(0)
    if args and args[0] == "list":
        print('[{"name":"acme-claims","namespace":"acme-claims","status":"deployed","chart":"curie-0.8.7"}]')
        sys.exit(0)
    if args and args[0] == "status":
        if "json" in args:
            print('{"version":1,"info":{"status":"deployed"},"hooks":[]}')
        else:
            print("NAME: acme-claims\nSTATUS: deployed\nREVISION: 17")
        sys.exit(0)
    if has_sequence(["get", "manifest"]):
        print('{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"acme-probe"},"spec":{"replicas":1,"selector":{"matchLabels":{"component":"acme-probe"}},"template":{"spec":{"containers":[{"name":"probe","image":"busybox:1"}]}}}}')
        sys.exit(0)
    if has_sequence(["get", "values"]):
        print('{"mailAdapter":{"deploy":false},"api":{"ingress":{"enabled":true}}}')
        sys.exit(0)
    print("unexpected helm invocation: " + shlex.join(args), file=sys.stderr)
    sys.exit(64)

if tool == "docker":
    if args == ["info"]:
        sys.exit(0)
    if "compose" in args and "ps" in args:
        print("NAME IMAGE COMMAND SERVICE CREATED STATUS PORTS")
        sys.exit(0)
    print("unexpected docker invocation: " + shlex.join(args), file=sys.stderr)
    sys.exit(64)

print("unexpected tool: " + tool, file=sys.stderr)
sys.exit(64)
"###;

#[derive(Debug)]
struct Call {
    kind: String,
    tool: String,
    argv: Vec<String>,
    compose_project: Option<String>,
}

struct Fixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    tools: PathBuf,
    home: PathBuf,
    cwd: PathBuf,
}

impl Fixture {
    fn new(sequence: &[serde_json::Value]) -> Self {
        assert!(!sequence.is_empty(), "claim sequence must not be empty");
        let temp = tempfile::tempdir().expect("create worker-claims fixture");
        let root = temp.path().to_path_buf();
        let tools = root.join("tools");
        let home = root.join("home");
        let cwd = root.join("cwd");
        for path in [&tools, &home, &cwd] {
            fs::create_dir_all(path).expect("create fixture directory");
        }
        for tool in ["docker", "kubectl", "helm"] {
            write_executable(&tools.join(tool), TOOL_SHIM);
        }
        fs::write(root.join("calls.log"), "").expect("create calls log");
        fs::write(root.join("claim-count"), "0").expect("create claim counter");
        fs::write(
            root.join("claim-sequence.json"),
            serde_json::to_vec(sequence).expect("serialize claim sequence"),
        )
        .expect("write claim sequence");
        Self {
            _temp: temp,
            root,
            tools,
            home,
            cwd,
        }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(bin());
        command
            .args(args)
            .current_dir(&self.cwd)
            .env_clear()
            .env("PATH", shim_path(&self.tools))
            .env("HOME", &self.home)
            .env("CURIE_CONFIG_DIR", self.home.join(".config/curie"))
            .env("CURIE_TEST_WORKER_CLAIMS_ROOT", &self.root)
            .env("LC_ALL", "C");
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command(args).output().expect("run curie binary")
    }

    fn calls(&self) -> Vec<Call> {
        fs::read_to_string(self.root.join("calls.log"))
            .expect("read calls log")
            .lines()
            .map(|line| {
                let row: serde_json::Value = serde_json::from_str(line).expect("parse call row");
                Call {
                    kind: row["kind"].as_str().expect("call kind").to_string(),
                    tool: row["tool"].as_str().expect("call tool").to_string(),
                    argv: row["argv"]
                        .as_array()
                        .expect("call argv")
                        .iter()
                        .map(|value| value.as_str().expect("argv string").to_string())
                        .collect(),
                    compose_project: row["compose_project"].as_str().map(str::to_string),
                }
            })
            .collect()
    }

    fn claim_execs(&self) -> Vec<Call> {
        self.calls()
            .into_iter()
            .filter(|call| call.kind == "claim_exec")
            .collect()
    }

    fn message_command(&self) -> Command {
        let chart = Path::new(env!("CARGO_MANIFEST_DIR")).join("../charts/curie");
        let mut command = Command::new(bin());
        command
            .args([
                "--color=never",
                "--json",
                "cluster",
                "message",
                "wait for the worker",
                "--channel",
                "C0EXAMPLE1",
                "--namespace",
                NAMESPACE,
                "--release",
                RELEASE,
                "--chart",
            ])
            .arg(chart)
            .args([
                "--listen-host",
                "127.0.0.1",
                "--valkey-local-port",
                "0",
                "--api-local-port",
                "0",
                "--api-key",
                "cluster-fixture-api-key",
                "--valkey-password",
                "fixture-valkey-password",
                "--stream",
                "curie:test:worker-claims-2374",
                "--timeout-secs",
                "1",
            ])
            .current_dir(&self.cwd)
            .env_clear()
            .env("PATH", shim_path(&self.tools))
            .env("HOME", &self.home)
            .env("CURIE_CONFIG_DIR", self.home.join(".config/curie"))
            .env("CURIE_TEST_WORKER_CLAIMS_ROOT", &self.root)
            .env("LC_ALL", "C");
        command
    }
}

fn cluster_status(fixture: &Fixture) -> Output {
    fixture.run(&[
        "--color=never",
        "--json",
        "cluster",
        "status",
        "--namespace",
        NAMESPACE,
        "--release",
        RELEASE,
    ])
}

fn doctor(fixture: &Fixture) -> Output {
    fixture.run(&[
        "--color=never",
        "--json",
        "doctor",
        "--namespace",
        NAMESPACE,
        "--release",
        RELEASE,
    ])
}

fn claim_check(value: &serde_json::Value) -> &serde_json::Value {
    value["checks"]
        .as_array()
        .expect("doctor checks")
        .iter()
        .find(|check| check["id"] == "worker-claims")
        .expect("worker-claims check")
}

fn assert_existing_cluster_status_shape(value: &serde_json::Value) {
    let keys: BTreeSet<_> = value
        .as_object()
        .expect("cluster status object")
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        BTreeSet::from([
            "delivery",
            "healthy",
            "namespace",
            "pods",
            "release_found",
            "release_state",
            "revision",
            "upgrade",
            "urls",
            "warnings",
        ]),
        "worker claims must use existing cluster-status fields: {value}"
    );
}

#[test]
fn cluster_status_selects_the_running_labeled_worker_and_annotates_claims_enabled() {
    let fixture = Fixture::new(&[claims_enabled()]);
    let output = cluster_status(&fixture);
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let value = json_output(&output);
    assert_existing_cluster_status_shape(&value);
    assert_eq!(value["healthy"], true, "{value}");
    let row = value["pods"]["rows"]
        .as_array()
        .expect("pod rows")
        .iter()
        .find(|row| row["pod"] == WORKER_POD)
        .expect("worker row");
    assert!(
        row["status"]
            .as_str()
            .unwrap_or_default()
            .contains("claims enabled"),
        "claims-enabled must annotate the existing worker row without a new JSON field: {value}"
    );

    let calls = fixture.calls();
    let selector_reads: Vec<_> = calls
        .iter()
        .filter(|call| {
            call.tool == "kubectl"
                && call.argv.iter().any(|arg| arg == "get")
                && call.argv.iter().any(|arg| arg == "pods" || arg == "pod")
                && call.argv.iter().any(|arg| arg == WORKER_SELECTOR)
        })
        .collect();
    assert_eq!(
        selector_reads.len(),
        1,
        "one exact release+component selector read is the only valid cluster selection: {calls:#?}"
    );
    let execs = fixture.claim_execs();
    assert_eq!(execs.len(), 1, "status observes once: {calls:#?}");
    assert!(execs[0].argv.iter().any(|arg| arg == WORKER_POD));
    assert!(!execs[0]
        .argv
        .iter()
        .any(|arg| arg == OLD_COMPLETED_HOOK || arg == OLD_STUCK_HOOK));
    assert!(execs[0].argv.windows(6).any(|args| args
        == [
            "python",
            "-m",
            "curie_worker.upgrade_drain",
            "--mode",
            "status",
            "--json",
        ]));
}

#[test]
fn current_quiesce_uses_authored_metadata_in_existing_cluster_status_fields() {
    let fixture = Fixture::new(&[quiescing(SINCE_17, 17)]);
    let output = cluster_status(&fixture);
    assert_eq!(output.status.code(), Some(1), "{}", describe(&output));
    let value = json_output(&output);
    assert_existing_cluster_status_shape(&value);
    assert_eq!(value["healthy"], false, "{value}");
    assert!(
        value["pods"]["unhealthy"]
            .as_array()
            .expect("unhealthy list")
            .iter()
            .any(|item| item == STATUS_WAITING_17),
        "a known current marker must affect the verdict with authored text: {value}"
    );
    assert!(value["warnings"].as_array().unwrap().is_empty(), "{value}");
}

#[test]
fn unreadable_and_malformed_claim_observations_never_become_claims_enabled_or_echo_output() {
    let cases = [
        ("worker-reported unknown", unknown(), false),
        (
            "invalid json",
            serde_json::json!({
                "stdout": format!("not-json-{PRIVATE_SENTINEL}"),
                "stderr": "",
                "exit": 0,
                "sleep": 0,
            }),
            false,
        ),
        (
            "process failure",
            serde_json::json!({
                "stdout": "",
                "stderr": format!("read failed: {PRIVATE_SENTINEL}"),
                "exit": 19,
                "sleep": 0,
            }),
            false,
        ),
        (
            "existing marker with unavailable metadata remains paused",
            quiescing_without_metadata(),
            true,
        ),
        (
            "revision is not a JSON integer",
            claim_reply(
                "quiescing",
                serde_json::json!(SINCE_17),
                serde_json::json!("17"),
            ),
            false,
        ),
        (
            "negative revision is rejected",
            claim_reply(
                "quiescing",
                serde_json::json!(SINCE_17),
                serde_json::json!(-1),
            ),
            false,
        ),
        (
            "floating revision is rejected",
            claim_reply(
                "quiescing",
                serde_json::json!(SINCE_17),
                serde_json::json!(17.0),
            ),
            false,
        ),
        (
            "overflowing revision is rejected",
            serde_json::json!({
                "stdout": "{\"state\":\"quiescing\",\"since\":\"2026-09-08T12:34:56Z\",\"revision\":18446744073709551616}",
                "stderr": "",
                "exit": 0,
                "sleep": 0,
            }),
            false,
        ),
        (
            "claims enabled cannot carry metadata",
            claim_reply(
                "claims_enabled",
                serde_json::json!(PRIVATE_SENTINEL),
                serde_json::json!(17),
            ),
            false,
        ),
        (
            "extra output field violates the exact internal shape",
            serde_json::json!({
                "stdout": format!(
                    "{{\"state\":\"claims_enabled\",\"since\":null,\"revision\":null,\"raw\":\"{PRIVATE_SENTINEL}\"}}"
                ),
                "stderr": "",
                "exit": 0,
                "sleep": 0,
            }),
            false,
        ),
        (
            "unsafe metadata is rejected",
            claim_reply(
                "quiescing",
                serde_json::json!(format!("{SINCE_17}\n\u{1b}[31m{PRIVATE_SENTINEL}")),
                serde_json::json!(17),
            ),
            false,
        ),
    ];

    for (label, response, paused_without_metadata) in cases {
        let fixture = Fixture::new(&[response]);
        let output = cluster_status(&fixture);
        let text = format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !text.contains(PRIVATE_SENTINEL),
            "{label} echoed raw output: {text}"
        );
        assert!(
            !text.to_ascii_lowercase().contains("claims enabled"),
            "{label} became the unsafe fail-open result: {text}"
        );
        let value = json_output(&output);
        assert_existing_cluster_status_shape(&value);
        if paused_without_metadata {
            assert_eq!(
                output.status.code(),
                Some(1),
                "{label}: {}",
                describe(&output)
            );
            assert_eq!(value["healthy"], false, "{label}: {value}");
            assert!(value["warnings"].as_array().unwrap().is_empty(), "{value}");
            let reasons = value["pods"]["unhealthy"]
                .as_array()
                .expect("unhealthy reasons");
            assert!(
                reasons
                    .iter()
                    .any(|reason| reason == STATUS_QUIESCING_WITHOUT_METADATA),
                "metadata-free quiesce must be a known unhealthy pause: {value}"
            );
            let reason = reasons
                .iter()
                .filter_map(|reason| reason.as_str())
                .find(|reason| reason.contains("metadata unavailable"))
                .expect("authored metadata-free pause reason");
            assert!(!reason.contains("revision"), "invented revision: {reason}");
            assert!(!reason.contains("since"), "invented timestamp: {reason}");
        } else {
            assert!(
                value["warnings"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|line| line == "worker claim state unknown"),
                "{label} must use the authored unknown warning: {value}"
            );
        }
    }
}

#[test]
fn the_whole_cluster_selection_and_exec_observation_is_bounded() {
    for hang in ["selection", "exec"] {
        let response = serde_json::json!({
            "stdout": serde_json::json!({
                "state": "claims_enabled",
                "since": null,
                "revision": null,
            }).to_string(),
            "stderr": "",
            "exit": 0,
            "sleep": if hang == "exec" { 30 } else { 0 },
        });
        let fixture = Fixture::new(&[response]);
        let mut command = fixture.command(&[
            "--color=never",
            "--json",
            "cluster",
            "status",
            "--namespace",
            NAMESPACE,
            "--release",
            RELEASE,
        ]);
        if hang == "selection" {
            command.env("CURIE_TEST_SELECTOR_SLEEP", "30");
        }
        let started = Instant::now();
        let output = command.output().expect("run bounded observer case");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(15),
            "{hang} was not bounded as one observation; elapsed {elapsed:?}: {}",
            describe(&output)
        );
        let value = json_output(&output);
        assert!(
            value["warnings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|line| line == "worker claim state unknown"),
            "a bounded timeout is unknown: {value}"
        );
    }
}

#[test]
fn local_status_keeps_services_json_exact_and_uses_the_fixed_compose_worker() {
    let expected_stdout: &[u8] =
        b"{\"services\":[\"NAME IMAGE COMMAND SERVICE CREATED STATUS PORTS\"]}\n";
    for (label, response, expected_stderr) in [
        ("known", quiescing(SINCE_17, 17), STATUS_WAITING_17),
        (
            "metadata unavailable",
            quiescing_without_metadata(),
            STATUS_QUIESCING_WITHOUT_METADATA,
        ),
        ("unknown", unknown(), "worker claim state unknown"),
    ] {
        let fixture = Fixture::new(&[response]);
        let output = fixture.run(&[
            "--color=never",
            "--json",
            "local",
            "status",
            "-f",
            "compose.dev.yaml",
        ]);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{label}: {}",
            describe(&output)
        );
        assert_eq!(
            output.stdout.as_slice(),
            expected_stdout,
            "{label}: claim diagnosis must not alter LocalStatusOutput"
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected_stderr), "{label}: {stderr}");
        assert!(!stderr.contains(PRIVATE_SENTINEL), "{label}: {stderr}");
        let execs = fixture.claim_execs();
        assert_eq!(execs.len(), 1, "local status observes once: {execs:#?}");
        let exec = &execs[0];
        assert_eq!(exec.tool, "docker");
        assert!(
            exec.compose_project.as_deref() == Some("curie")
                || exec.argv.windows(2).any(|args| args == ["-p", "curie"]),
            "local observation must select compose project curie explicitly: {exec:#?}"
        );
        assert!(exec.argv.iter().any(|arg| arg == "compose.dev.yaml"));
        assert!(exec.argv.iter().any(|arg| arg == "curie-worker"));
        assert!(
            !fixture.calls().iter().any(|call| call.tool == "kubectl"),
            "local status must stay on the exact compose project/service"
        );
    }
}

#[test]
fn doctor_reports_enabled_missing_and_unknown_with_the_existing_check_shape() {
    for (response, state, detail) in [
        (claims_enabled(), "ok", "claims enabled"),
        (quiescing(SINCE_17, 17), "missing", WAITING_17),
        (
            quiescing_without_metadata(),
            "missing",
            WAITING_WITHOUT_METADATA,
        ),
        (unknown(), "not_applicable", "worker claim state unknown"),
    ] {
        let fixture = Fixture::new(&[response]);
        let output = doctor(&fixture);
        assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
        let value = json_output(&output);
        let check = claim_check(&value);
        assert_eq!(check["state"], state, "{check}");
        if state == "not_applicable" {
            assert!(
                check["detail"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("unknown"),
                "unknown doctor detail must be authored: {check}"
            );
        } else {
            assert_eq!(check["detail"], detail, "{check}");
        }
        let keys: BTreeSet<_> = check
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let expected = if state == "missing" {
            BTreeSet::from(["detail", "fix", "id", "state", "title"])
        } else {
            BTreeSet::from(["detail", "id", "state", "title"])
        };
        assert_eq!(
            keys, expected,
            "worker claims must use the existing check shape"
        );
        if state == "missing" {
            let fix = check["fix"].as_str().expect("missing check fix");
            if detail == WAITING_17 {
                assert!(fix.contains("revision 17"), "{fix}");
            } else {
                assert!(
                    !fix.contains("revision "),
                    "metadata-free marker invented a revision: {fix}"
                );
            }
            assert!(fix.contains("curie doctor"), "{fix}");
            assert!(fix.contains("curie cluster status"), "{fix}");
            assert!(fix.contains("--namespace acme-claims"), "{fix}");
            assert!(fix.contains("--release acme-claims"), "{fix}");
            assert_eq!(value["ready"], false, "{value}");
        }
        let rendered = format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!rendered.contains(PRIVATE_SENTINEL), "{rendered}");
    }
}

fn assert_message_timeout_contract(output: &Output) {
    assert_eq!(output.status.code(), Some(3), "{}", describe(output));
    assert_eq!(
        output.stdout.as_slice(),
        b"{\"finalized\":false,\"reply\":null,\"timed_out\":true}\n".as_slice(),
        "message timeout JSON must remain byte-for-byte the existing object"
    );
    assert_eq!(
        json_output(output),
        serde_json::json!({"reply": null, "finalized": false, "timed_out": true})
    );
}

fn assert_two_message_observations_around_the_wait(fixture: &Fixture) {
    let calls = fixture.calls();
    let exec_indices: Vec<_> = calls
        .iter()
        .enumerate()
        .filter_map(|(index, call)| (call.kind == "claim_exec").then_some(index))
        .collect();
    assert_eq!(
        exec_indices.len(),
        2,
        "message must observe once before waiting and once after timeout, never in the hot poll: {calls:#?}"
    );
    let first_http_get = calls
        .iter()
        .position(|call| call.kind == "http_get")
        .expect("timeout fixture must enter the reply wait");
    assert!(
        exec_indices[0] < first_http_get,
        "the initial claim observation must precede the reply wait: {calls:#?}"
    );
    assert!(
        exec_indices[1] > first_http_get,
        "the only recheck belongs after the timeout: {calls:#?}"
    );
    let selection_count = calls
        .iter()
        .filter(|call| {
            call.tool == "kubectl"
                && call.argv.iter().any(|arg| arg == WORKER_SELECTOR)
                && call.argv.iter().any(|arg| arg == "pods" || arg == "pod")
        })
        .count();
    assert_eq!(
        selection_count, 2,
        "each bounded observation performs one exact worker selection: {calls:#?}"
    );
}

#[test]
fn message_reports_an_initial_marker_immediately_then_drops_it_when_release_finishes() {
    let fixture = Fixture::new(&[quiescing(SINCE_17, 17), claims_enabled()]);
    let output = fixture
        .message_command()
        .output()
        .expect("run marker-removed message timeout");
    assert_message_timeout_contract(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(WAITING_17),
        "known pre-wait reason was hidden: {stderr}"
    );
    assert_eq!(
        stderr.matches(WAITING_17).count(),
        1,
        "the timeout recheck must use its latest claims-enabled answer, not repeat stale metadata: {stderr}"
    );
    assert!(
        !stderr.contains(WAITING_18),
        "fixture invented a later marker: {stderr}"
    );
    assert_two_message_observations_around_the_wait(&fixture);
}

#[test]
fn message_timeout_reports_a_marker_introduced_during_the_wait() {
    let fixture = Fixture::new(&[claims_enabled(), quiescing(SINCE_18, 18)]);
    let output = fixture
        .message_command()
        .output()
        .expect("run marker-introduced message timeout");
    assert_message_timeout_contract(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(WAITING_18),
        "the timeout must report the one latest authored reason: {stderr}"
    );
    assert_eq!(stderr.matches(WAITING_18).count(), 1, "{stderr}");
    assert!(
        !stderr.contains(WAITING_17),
        "fixture surfaced stale metadata: {stderr}"
    );
    assert_two_message_observations_around_the_wait(&fixture);
}

#[test]
fn message_explains_an_initial_metadata_free_pause_without_inventing_details() {
    let fixture = Fixture::new(&[quiescing_without_metadata(), claims_enabled()]);
    let output = fixture
        .message_command()
        .output()
        .expect("run metadata-free marker message timeout");
    assert_message_timeout_contract(&output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let reasons: Vec<_> = stderr
        .lines()
        .filter(|line| line.contains("metadata unavailable"))
        .collect();
    assert_eq!(reasons, [WAITING_WITHOUT_METADATA], "{stderr}");
    assert!(!reasons[0].contains("revision"), "{stderr}");
    assert!(!reasons[0].contains("since"), "{stderr}");
    assert!(!stderr.contains(PRIVATE_SENTINEL), "{stderr}");
    assert_two_message_observations_around_the_wait(&fixture);
}
