//! Process-level regression coverage for #2096.
//!
//! The compiled CLI talks to fake external cluster boundaries: kubectl owns
//! real loopback port-forwards, a tiny RESP server records XADD payloads, and a
//! tiny HTTP server serves the ref-keyed API relay.  The assertions are all on
//! executable behavior and external state, not private Rust helpers.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{Duration, Instant};

mod support;
use support::{serve, Response};

const CHANNEL: &str = "C0EXAMPLE1";
const API_KEY: &str = "fixture-platform-key";
const RELAY_ADAPTER: &str = "curie-cluster-message";
const WORKER_CLAIM_SELECTION: &str = "kubectl get pods -n acme-system -l app.kubernetes.io/instance=acme-release,app.kubernetes.io/component=worker -o json";
const WORKER_CLAIM_EXEC: &str = "kubectl exec -n acme-system worker-stable-a -- python -m curie_worker.upgrade_drain --mode status --json --with-ttl";

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn write_tool(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    let body = r###"#!/usr/bin/env python3
import fcntl
import json
import os
import shlex
import socket
import sys
import threading
from urllib.parse import parse_qs, urlparse

args = sys.argv[1:]
tool = os.path.basename(sys.argv[0])
state_path = os.environ["CURIE_TEST_CLUSTER_STATE"]
log_path = os.environ["CURIE_TEST_CLUSTER_LOG"]

with open(log_path, "a", encoding="utf-8") as log:
    fcntl.flock(log, fcntl.LOCK_EX)
    log.write(tool + " " + shlex.join(args) + "\n")

def read_state():
    with open(state_path, encoding="utf-8") as state_file:
        fcntl.flock(state_file, fcntl.LOCK_SH)
        return json.load(state_file)

def update_state(change):
    with open(state_path, "r+", encoding="utf-8") as state_file:
        fcntl.flock(state_file, fcntl.LOCK_EX)
        state = json.load(state_file)
        result = change(state)
        state_file.seek(0)
        json.dump(state, state_file)
        state_file.truncate()
        return result

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

def redis_reply(command):
    words = [part.decode("utf-8") if isinstance(part, bytes) else str(part) for part in command]
    verb = words[0].upper()
    if verb == "PING":
        return b"+PONG\r\n"
    if verb in ("AUTH", "SELECT") or verb == "CLIENT":
        return b"+OK\r\n"
    if verb == "XADD":
        payload = words[words.index("payload") + 1]
        def append(state):
            state["next_stream"] += 1
            stream_id = f'{state["next_stream"]}-0'
            state["turns"].append({
                "stream_id": stream_id,
                "payload": json.loads(payload),
                "pending": True,
                "acked": False,
            })
            return stream_id
        return bulk(update_state(append))
    if verb == "XACK":
        entry_ids = words[3:]
        def acknowledge(state):
            changed = 0
            for turn in state["turns"]:
                if turn["stream_id"] in entry_ids and turn["pending"]:
                    turn["pending"] = False
                    turn["acked"] = True
                    changed += 1
            state["xack_commands"].append(entry_ids)
            return changed
        return b":" + str(update_state(acknowledge)).encode() + b"\r\n"
    if verb == "XINFO" and len(words) > 1 and words[1].upper() == "GROUPS":
        state = read_state()
        stream_id = state["turns"][-1]["stream_id"] if state["turns"] else "0-0"
        pending = sum(1 for turn in state["turns"] if turn["pending"])
        pairs = [
            ("name", "curie-workers"),
            ("consumers", 1),
            ("pending", pending),
            ("last-delivered-id", stream_id),
            ("entries-read", len(state["turns"])),
            ("lag", 0),
        ]
        reply = b"*1\r\n*12\r\n"
        for key, value in pairs:
            reply += bulk(key)
            reply += (b":" + str(value).encode() + b"\r\n") if isinstance(value, int) else bulk(value)
        return reply
    if verb == "XPENDING":
        state = read_state()
        pending = [turn for turn in state["turns"] if turn["pending"]]
        if len(words) > 3:
            reply = b"*" + str(len(pending)).encode() + b"\r\n"
            for turn in pending:
                reply += b"*4\r\n" + bulk(turn["stream_id"]) + bulk("worker-fixture") + b":0\r\n:1\r\n"
            return reply
        reply = b"*4\r\n:" + str(len(pending)).encode() + b"\r\n"
        if pending:
            reply += bulk(pending[0]["stream_id"]) + bulk(pending[-1]["stream_id"])
            reply += b"*1\r\n*2\r\n" + bulk("worker-fixture") + b":" + str(len(pending)).encode() + b"\r\n"
        else:
            reply += b"$-1\r\n$-1\r\n*0\r\n"
        return reply
    if verb == "XRANGE":
        return b"*0\r\n"
    if verb == "XLEN":
        return b":1\r\n"
    if verb == "QUIT":
        return b"+OK\r\n"
    return b"-ERR unsupported fixture command " + verb.encode() + b"\r\n"

def serve_redis(client):
    try:
        stream = client.makefile("rb")
        while True:
            command = read_resp(stream)
            if command is None:
                return
            client.sendall(redis_reply(command))
    finally:
        client.close()

def reply_text(payload):
    if payload["text"] == "alpha":
        return "reply-alpha"
    if payload["text"] == "beta":
        return "reply-beta"
    return "reply-" + payload["text"]

def relay_body(reply_ref, after):
    state = read_state()
    turn = next((item for item in state["turns"] if item["payload"]["reply_handle"].get("placeholder") == reply_ref), None)
    if turn is None or os.environ.get("CURIE_TEST_RELAY_MODE") == "timeout":
        return {"events": [], "next_cursor": after, "terminal": False}
    # The worker owns XACK. Model its terminal ordering explicitly: delivery to
    # the relay succeeds, then worker ownership leaves the PEL. This is not a
    # Redis command from the CLI; those are recorded separately above.
    def worker_xack(state):
        live = next(item for item in state["turns"] if item["payload"]["reply_handle"].get("placeholder") == reply_ref)
        if live["pending"]:
            live["pending"] = False
            live["acked"] = True
            state["worker_xacks"].append(live["stream_id"])
    update_state(worker_xack)
    payload = turn["payload"]
    handle = payload["reply_handle"]
    target = {
        "kind": handle["kind"],
        "address": handle["channel"],
        "conversation_id": payload["conversation_id"],
        "reply_ref": reply_ref,
    }
    events = [
        {
            "version": "1.0",
            "event": "reply.update",
            "target": target,
            "text": reply_text(payload),
            "message": None,
            "settled": None,
            "nav": None,
        },
        {
            "version": "1.0",
            "event": "turn.completed",
            "target": target,
            "event_id": payload["event_id"],
            "outcome": "delivered",
        },
    ]
    return {"events": events[after:], "next_cursor": len(events), "terminal": True}

def serve_http(client):
    try:
        stream = client.makefile("rb")
        request_line = stream.readline().decode("latin1").strip()
        if not request_line:
            return
        method, raw_path, _ = request_line.split(" ", 2)
        headers = {}
        while True:
            line = stream.readline().decode("latin1").rstrip("\r\n")
            if not line:
                break
            key, value = line.split(":", 1)
            headers[key.lower()] = value.strip()
        parsed = urlparse(raw_path)
        prefix = "/cluster-message-replies/"
        if method == "GET" and parsed.path.startswith(prefix):
            reply_ref = parsed.path[len(prefix):]
            after = int(parse_qs(parsed.query).get("after", ["0"])[0])
            mode = os.environ.get("CURIE_TEST_RELAY_MODE", "reply")
            def record(state):
                attempt = state["relay_attempts"].get(reply_ref, 0) + 1
                state["relay_attempts"][reply_ref] = attempt
                state["http_gets"].append({
                    "path": raw_path,
                    "api_key": headers.get("x-api-key"),
                    "attempt": attempt,
                })
                return attempt
            attempt = update_state(record)
            if mode == "404":
                status = "404 Not Found"
                body = b'{"detail":"Not Found"}'
            elif mode == "503_once" and attempt == 1:
                status = "503 Service Unavailable"
                body = b'{"detail":"relay temporarily unavailable"}'
            else:
                status = "200 OK"
                body = json.dumps(relay_body(reply_ref, after)).encode("utf-8")
        else:
            status = "404 Not Found"
            body = b'{"detail":"unexpected fixture route"}'
        client.sendall(
            f"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode("ascii") + body
        )
    finally:
        client.close()

def serve_port_forward(component):
    requested, remote = args[-1].split(":", 1)
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", int(requested)))
    listener.listen()
    print(f"Forwarding from 127.0.0.1:{listener.getsockname()[1]} -> {remote}", flush=True)
    handler = serve_redis if component == "valkey" else serve_http
    while True:
        client, _ = listener.accept()
        threading.Thread(target=handler, args=(client,), daemon=True).start()

if tool == "helm":
    sys.exit(0)

joined = " ".join(args)
if "port-forward" in args:
    serve_port_forward("valkey" if "valkey" in joined else "api")

# Live fullname discovery: the release's labelled API Service exists.
if "get" in args and "app.kubernetes.io/component=api" in joined:
    print("acme-release-curie-api", end="")
    sys.exit(0)

if "get deployment acme-release-curie-dispatcher" in joined:
    if os.environ.get("CURIE_TEST_DISPATCHER_PROBE_FAIL") == "1":
        print("intentional dispatcher probe failure", file=sys.stderr)
        sys.exit(1)
    if os.environ.get("CURIE_TEST_CONNECTED") == "1":
        print("deployment.apps/acme-release-curie-dispatcher")
    sys.exit(0)

# The message diagnosis may observe the claim gate through one exact, scoped,
# read-only worker selection followed by the worker's status-only entry point.
worker_selector = "app.kubernetes.io/instance=acme-release,app.kubernetes.io/component=worker"
if args == ["get", "pods", "-n", "acme-system", "-l", worker_selector, "-o", "json"]:
    print(json.dumps({"items": [{
        "metadata": {
            "name": "worker-stable-a",
            "labels": {
                "app.kubernetes.io/instance": "acme-release",
                "app.kubernetes.io/component": "worker",
            },
        },
        "status": {"phase": "Running"},
    }]}))
    sys.exit(0)

if args == [
    "exec", "-n", "acme-system", "worker-stable-a", "--",
    "python", "-m", "curie_worker.upgrade_drain", "--mode", "status", "--json", "--with-ttl",
]:
    print('{"state":"claims_enabled","since":null,"revision":null}')
    sys.exit(0)

# Connected mode reads the worker's configured Slack origin. This read is not a
# rollout/trust read and must remain on the connected branch.
if "get deployment" in joined and "app.kubernetes.io/component=worker" in joined and "jsonpath=" in joined:
    print(os.environ["CURIE_TEST_SLACK_BASE"])
    sys.exit(0)

# Any other call is deliberately refused. In particular, old main's worker
# Deployment trust read reaches this branch and makes the regression red.
print("unexpected fake kubectl invocation: " + joined, file=sys.stderr)
sys.exit(64)
"###;
    fs::write(&path, body).expect("write fake cluster tool");
    let mut permissions = fs::metadata(&path).expect("stat fake tool").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make fake tool executable");
    path
}

struct Fixture {
    _tools: tempfile::TempDir,
    state_home: tempfile::TempDir,
    path: std::ffi::OsString,
    state_path: PathBuf,
    log_path: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let tools = tempfile::tempdir().expect("create fake cluster tool directory");
        write_tool(tools.path(), "kubectl");
        write_tool(tools.path(), "helm");
        let state_home = tempfile::tempdir().expect("create isolated turn-state directory");
        let state_path = tools.path().join("cluster-state.json");
        let log_path = tools.path().join("cluster.log");
        fs::write(
            &state_path,
            serde_json::json!({
                "generation": 41,
                "pods": ["worker-stable-a", "worker-stable-b"],
                "next_stream": 100,
                "turns": [],
                "http_gets": [],
                "relay_attempts": {},
                "xack_commands": [],
                "worker_xacks": [],
            })
            .to_string(),
        )
        .expect("seed fake cluster state");
        fs::write(&log_path, "").expect("seed fake cluster log");
        let mut paths = vec![tools.path().to_path_buf()];
        paths.extend(std::env::split_paths(
            &std::env::var_os("PATH").unwrap_or_default(),
        ));
        Self {
            _tools: tools,
            state_home,
            path: std::env::join_paths(paths).expect("join fake cluster PATH"),
            state_path,
            log_path,
        }
    }

    fn command(&self, cwd: &Path, text: &str, continue_turn: bool) -> Command {
        let mut command = Command::new(bin());
        command
            .args(["cluster", "message", text])
            .args([
                "--channel",
                CHANNEL,
                "--namespace",
                "acme-system",
                "--release",
                "acme-release",
                "--chart",
                "charts/curie",
                "--listen-host",
                "127.0.0.1",
                "--valkey-local-port",
                "0",
                "--api-local-port",
                "0",
                "--api-key",
                API_KEY,
                "--valkey-password",
                "fixture-valkey-password",
                "--stream",
                "curie:test:cluster-message-2096",
                "--timeout-secs",
                "3",
                "--json",
            ])
            .current_dir(cwd)
            .env("PATH", &self.path)
            .env("CURIE_TEST_CLUSTER_LOG", &self.log_path)
            .env("CURIE_TEST_CLUSTER_STATE", &self.state_path)
            .env_remove("CURIE_API_KEY")
            .env_remove("CURIE_VALKEY_PASSWORD")
            .env_remove("CURIE_SLACK_BOT_TOKEN");
        if continue_turn {
            command.arg("--continue");
        }
        command
    }

    fn run(&self, text: &str, continue_turn: bool) -> Output {
        self.command(self.state_home.path(), text, continue_turn)
            .output()
            .expect("run cluster message")
    }

    fn state(&self) -> serde_json::Value {
        serde_json::from_str(
            &fs::read_to_string(&self.state_path).expect("read fake cluster state"),
        )
        .expect("parse fake cluster state")
    }

    fn log(&self) -> String {
        fs::read_to_string(&self.log_path).expect("read fake cluster log")
    }
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

fn assert_canonical_uuid_v4(value: &str) {
    let parsed = uuid::Uuid::parse_str(value)
        .unwrap_or_else(|error| panic!("relay ref is not a UUID ({error}): {value}"));
    assert_eq!(value, parsed.hyphenated().to_string());
    assert_eq!(value.len(), 36);
    assert_eq!(value.as_bytes()[14], b'4', "relay ref must be UUIDv4");
    assert!(
        value
            .bytes()
            .all(|byte| byte == b'-' || byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "relay ref is not canonical lowercase hex: {value}"
    );
}

fn assert_relay_turn<'a>(turn: &'a serde_json::Value, expected_text: &str) -> &'a str {
    assert_eq!(turn["text"], expected_text);
    let handle = &turn["reply_handle"];
    assert_eq!(handle["kind"], "slack", "binding kind must remain Slack");
    assert_eq!(
        handle["channel"], CHANNEL,
        "binding address must remain exact"
    );
    assert_eq!(handle["adapter"], RELAY_ADAPTER);
    assert!(
        handle["endpoint"].is_null(),
        "worker callback URL must never be serialized: {handle}"
    );
    let reply_ref = handle["placeholder"]
        .as_str()
        .expect("relay turn carries placeholder/ref");
    assert_canonical_uuid_v4(reply_ref);
    reply_ref
}

fn assert_no_worker_rollout(log: &str, state: &serde_json::Value) {
    assert_only_bounded_worker_claim_reads(log);
    let forbidden: Vec<&str> = log
        .lines()
        .filter(|line| {
            (line.contains("get deployment") && line.contains("worker"))
                || (line.contains("patch deployment") && line.contains("worker"))
                || (line.contains("set env") && line.contains("worker"))
                || (line.contains("rollout status") && line.contains("worker"))
                || (line.contains("get pods")
                    && line.contains("component=worker")
                    && *line != WORKER_CLAIM_SELECTION)
                || (line.starts_with("helm ") && line.contains("worker"))
        })
        .collect();
    assert!(
        forbidden.is_empty(),
        "cluster message touched worker rollout state: {forbidden:#?}\n{log}"
    );
    assert_eq!(state["generation"], 41, "Deployment generation changed");
    assert_eq!(
        state["pods"],
        serde_json::json!(["worker-stable-a", "worker-stable-b"]),
        "worker pod identities changed"
    );
}

fn assert_no_worker_mutation(log: &str, state: &serde_json::Value) {
    assert_only_bounded_worker_claim_reads(log);
    let forbidden: Vec<&str> = log
        .lines()
        .filter(|line| {
            (line.contains("patch deployment") && line.contains("worker"))
                || (line.contains("set env") && line.contains("worker"))
                || (line.contains("rollout status") && line.contains("worker"))
                || (line.contains("get pods")
                    && line.contains("component=worker")
                    && *line != WORKER_CLAIM_SELECTION)
                || (line.starts_with("helm ") && line.contains("worker"))
        })
        .collect();
    assert!(
        forbidden.is_empty(),
        "connected cluster message mutated worker rollout state: {forbidden:#?}\n{log}"
    );
    assert_eq!(state["generation"], 41);
    assert_eq!(
        state["pods"],
        serde_json::json!(["worker-stable-a", "worker-stable-b"])
    );
}

fn assert_only_bounded_worker_claim_reads(log: &str) {
    let unexpected: Vec<&str> = log
        .lines()
        .filter(|line| {
            (line.contains("get pods")
                && line.contains("component=worker")
                && *line != WORKER_CLAIM_SELECTION)
                || (line.contains("exec")
                    && line.contains("curie_worker.upgrade_drain")
                    && *line != WORKER_CLAIM_EXEC)
        })
        .collect();
    assert!(
        unexpected.is_empty(),
        "unexpected worker claim observation: {unexpected:#?}\n{log}"
    );

    let selections = log
        .lines()
        .filter(|line| *line == WORKER_CLAIM_SELECTION)
        .count();
    let executions = log
        .lines()
        .filter(|line| *line == WORKER_CLAIM_EXEC)
        .count();
    assert_eq!(
        executions, selections,
        "each scoped worker selection must have one status-only exec: {log}"
    );
    assert!(
        selections <= 2,
        "one message may observe claims only before and after its wait: {log}"
    );
}

#[test]
fn two_consecutive_disconnected_turns_reply_without_worker_rollout_or_replacement() {
    let fixture = Fixture::new();
    let first = fixture.run("alpha", false);
    assert!(
        first.status.success(),
        "first turn failed: {}",
        describe(&first)
    );
    let first_json = json_output(&first);
    assert_eq!(first_json["reply"], "reply-alpha");

    let second = fixture.run("beta", true);
    assert!(
        second.status.success(),
        "continued turn failed: {}",
        describe(&second)
    );
    let second_json = json_output(&second);
    assert_eq!(second_json["reply"], "reply-beta");
    assert_eq!(second_json["thread"], first_json["thread"]);

    let state = fixture.state();
    let turns = state["turns"].as_array().expect("recorded turns");
    assert_eq!(turns.len(), 2, "one XADD per command: {turns:#?}");
    let first_ref = assert_relay_turn(&turns[0]["payload"], "alpha").to_string();
    let second_ref = assert_relay_turn(&turns[1]["payload"], "beta").to_string();
    assert_ne!(first_ref, second_ref, "commands shared a callback ref");
    assert!(turns
        .iter()
        .all(|turn| turn["acked"] == true && turn["pending"] == false));
    assert_eq!(
        state["worker_xacks"].as_array().map(Vec::len),
        Some(2),
        "only terminal worker delivery clears each PEL owner: {state}"
    );
    assert_eq!(
        state["xack_commands"],
        serde_json::json!([]),
        "the CLI must never issue XACK"
    );

    let gets = state["http_gets"].as_array().expect("recorded relay GETs");
    assert_eq!(gets.len(), 2, "one terminal poll per ref: {gets:#?}");
    for (get, reply_ref) in gets.iter().zip([&first_ref, &second_ref]) {
        assert_eq!(
            get["path"],
            format!("/cluster-message-replies/{reply_ref}?after=0")
        );
        assert_eq!(get["api_key"], API_KEY);
    }
    let log = fixture.log();
    assert_eq!(
        log.lines()
            .filter(|line| line.contains("port-forward") && line.contains("-api"))
            .count(),
        2,
        "each turn must poll through an API loopback port-forward: {log}"
    );
    assert_no_worker_rollout(&log, &state);
}

#[test]
fn concurrent_commands_receive_only_their_own_ref_and_reply() {
    let fixture = Fixture::new();
    let alpha_home = tempfile::tempdir().expect("alpha turn state");
    let beta_home = tempfile::tempdir().expect("beta turn state");
    let (alpha, beta) = std::thread::scope(|scope| {
        let alpha = scope.spawn(|| {
            fixture
                .command(alpha_home.path(), "alpha", false)
                .output()
                .expect("run alpha")
        });
        let beta = scope.spawn(|| {
            fixture
                .command(beta_home.path(), "beta", false)
                .output()
                .expect("run beta")
        });
        (
            alpha.join().expect("alpha thread"),
            beta.join().expect("beta thread"),
        )
    });
    assert!(alpha.status.success(), "alpha failed: {}", describe(&alpha));
    assert!(beta.status.success(), "beta failed: {}", describe(&beta));
    assert_eq!(json_output(&alpha)["reply"], "reply-alpha");
    assert_eq!(json_output(&beta)["reply"], "reply-beta");

    let state = fixture.state();
    let turns = state["turns"].as_array().expect("concurrent turns");
    let alpha_turn = turns
        .iter()
        .find(|turn| turn["payload"]["text"] == "alpha")
        .expect("alpha payload");
    let beta_turn = turns
        .iter()
        .find(|turn| turn["payload"]["text"] == "beta")
        .expect("beta payload");
    assert_ne!(
        assert_relay_turn(&alpha_turn["payload"], "alpha"),
        assert_relay_turn(&beta_turn["payload"], "beta"),
        "concurrent commands shared callback ownership"
    );
    assert_no_worker_rollout(&fixture.log(), &state);
}

#[test]
fn dispatcher_probe_failure_refuses_before_enqueue_or_mutation() {
    let fixture = Fixture::new();
    let output = fixture
        .command(fixture.state_home.path(), "probe", false)
        .env("CURIE_TEST_DISPATCHER_PROBE_FAIL", "1")
        .output()
        .expect("run failed dispatcher probe");
    assert!(
        !output.status.success(),
        "indeterminate probe must fail closed"
    );
    let state = fixture.state();
    assert_eq!(state["turns"], serde_json::json!([]));
    assert_eq!(state["http_gets"], serde_json::json!([]));
    assert_no_worker_rollout(&fixture.log(), &state);
}

#[test]
fn nonterminal_relay_timeout_keeps_the_semantic_json_and_needs_no_restore() {
    let fixture = Fixture::new();
    let output = fixture
        .command(fixture.state_home.path(), "timeout", false)
        .env("CURIE_TEST_RELAY_MODE", "timeout")
        .output()
        .expect("run relay timeout");
    assert_eq!(output.status.code(), Some(3), "{}", describe(&output));
    assert_eq!(
        json_output(&output),
        serde_json::json!({"reply": null, "finalized": false, "timed_out": true})
    );
    let state = fixture.state();
    assert_eq!(state["turns"].as_array().map(Vec::len), Some(1));
    assert_eq!(
        state["turns"][0]["acked"], false,
        "XADD is not an acknowledgement"
    );
    assert_eq!(
        state["turns"][0]["pending"], true,
        "timeout leaves worker ownership pending"
    );
    assert_eq!(state["worker_xacks"], serde_json::json!([]));
    assert_eq!(
        state["xack_commands"],
        serde_json::json!([]),
        "process exit must not let the CLI steal worker-owned XACK"
    );
    assert_no_worker_rollout(&fixture.log(), &state);
}

#[test]
fn generic_fastapi_404_fails_immediately_as_a_stale_platform() {
    let fixture = Fixture::new();
    let started = Instant::now();
    let output = fixture
        .command(fixture.state_home.path(), "stale", false)
        .env("CURIE_TEST_RELAY_MODE", "404")
        .output()
        .expect("run against an API without the relay route");
    let elapsed = started.elapsed();

    assert_eq!(
        output.status.code(),
        Some(1),
        "an unrouted FastAPI 404 is a permanent stale-platform failure, not exit 3 timeout: {}",
        describe(&output)
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "unrouted 404 must fail before the 3s reply deadline, elapsed {elapsed:?}: {}",
        describe(&output)
    );
    let rendered = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        rendered.to_ascii_lowercase().contains("upgrade")
            && rendered.contains("cluster-message-replies"),
        "the error must identify the missing route and tell the operator to upgrade: {rendered}"
    );
    let state = fixture.state();
    assert_eq!(state["turns"].as_array().map(Vec::len), Some(1));
    assert_eq!(state["turns"][0]["pending"], true);
    assert_eq!(state["xack_commands"], serde_json::json!([]));
    assert_no_worker_rollout(&fixture.log(), &state);
}

#[test]
fn transient_relay_503_retries_without_duplicate_enqueue_then_replies() {
    let fixture = Fixture::new();
    let output = fixture
        .command(fixture.state_home.path(), "alpha", false)
        .env("CURIE_TEST_RELAY_MODE", "503_once")
        .output()
        .expect("run through one transient relay failure");
    assert!(
        output.status.success(),
        "one transient relay read must recover: {}",
        describe(&output)
    );
    assert_eq!(json_output(&output)["reply"], "reply-alpha");

    let state = fixture.state();
    assert_eq!(
        state["turns"].as_array().map(Vec::len),
        Some(1),
        "retrying a GET must never repeat XADD: {state}"
    );
    assert_eq!(
        state["http_gets"].as_array().map(Vec::len),
        Some(2),
        "the first 503 must be followed by exactly one successful GET: {state}"
    );
    assert_eq!(state["http_gets"][0]["attempt"], 1);
    assert_eq!(state["http_gets"][1]["attempt"], 2);
    assert_eq!(state["turns"][0]["acked"], true);
    assert_eq!(state["worker_xacks"].as_array().map(Vec::len), Some(1));
    assert_eq!(state["xack_commands"], serde_json::json!([]));
    assert_no_worker_rollout(&fixture.log(), &state);
}

#[test]
fn connected_dispatcher_uses_slack_placeholder_and_never_the_relay() {
    const PLACEHOLDER_TS: &str = "1717171717.000900";
    let fixture = Fixture::new();
    let slack = serve(|_request| {
        Response::json(
            200,
            r#"{"ok":true,"ts":"1717171717.000900","channel":"C0EXAMPLE1"}"#,
        )
    });
    let output = fixture
        .command(fixture.state_home.path(), "connected", false)
        .env("CURIE_TEST_CONNECTED", "1")
        .env("CURIE_TEST_SLACK_BASE", &slack.base_url)
        .env("CURIE_SLACK_BOT_TOKEN", "xoxb-example")
        .output()
        .expect("run connected cluster message branch");
    assert!(output.status.success(), "{}", describe(&output));
    let emitted = json_output(&output);
    assert_eq!(emitted["status"], "enqueued");
    assert_eq!(emitted["channel"], CHANNEL);
    assert_eq!(emitted["thread"], PLACEHOLDER_TS);

    let posts = slack.recorded();
    assert_eq!(posts.len(), 1, "connected mode posts one real placeholder");
    assert_eq!(posts[0].path, "/chat.postMessage");
    let body: serde_json::Value = serde_json::from_slice(&posts[0].body).expect("Slack JSON body");
    assert_eq!(body["channel"], CHANNEL);

    let state = fixture.state();
    assert_eq!(state["turns"].as_array().map(Vec::len), Some(1));
    let handle = &state["turns"][0]["payload"]["reply_handle"];
    assert!(
        handle["adapter"].is_null(),
        "connected turn selected relay: {handle}"
    );
    assert!(
        handle["endpoint"].is_null(),
        "connected turn carried callback endpoint: {handle}"
    );
    assert_eq!(handle["placeholder"], PLACEHOLDER_TS);
    assert_eq!(
        state["http_gets"],
        serde_json::json!([]),
        "connected mode must issue zero relay GETs"
    );
    let log = fixture.log();
    assert!(
        !log.lines()
            .any(|line| line.contains("port-forward") && line.contains("-api")),
        "connected mode must not open a relay API tunnel: {log}"
    );
    assert_no_worker_mutation(&log, &state);
}

#[test]
fn cluster_json_dry_run_hides_callback_endpoint_but_human_plan_names_poll_url() {
    let json = Command::new(bin())
        .args([
            "cluster",
            "message",
            "hello",
            "--channel",
            CHANNEL,
            "--api-local-port",
            "18157",
            "--dry-run",
            "--json",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .output()
        .expect("run JSON cluster message dry-run");
    assert!(json.status.success(), "{}", describe(&json));
    let payload = json_output(&json);
    assert!(
        payload["reply_endpoint"].is_null(),
        "the queue carries no callback endpoint, so JSON must not describe the poll URL as one: {payload}"
    );

    let human = Command::new(bin())
        .args([
            "cluster",
            "message",
            "hello",
            "--channel",
            CHANNEL,
            "--api-local-port",
            "18157",
            "--dry-run",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .output()
        .expect("run human cluster message dry-run");
    assert!(human.status.success(), "{}", describe(&human));
    let plan = format!(
        "{}\n{}",
        String::from_utf8_lossy(&human.stdout),
        String::from_utf8_lossy(&human.stderr)
    );
    assert!(
        plan.contains("poll replies at http://127.0.0.1:18157/cluster-message-replies/<uuid-v4>"),
        "human plan must still explain the separate loopback poll URL: {plan}"
    );
}
