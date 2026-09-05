//! Binary level regression coverage for cluster API credential transport.
//!
//! The release key is discovered through a fake external `kubectl`, while both
//! candidate HTTP routes are real TCP listeners. The fake port forward proxies
//! a kernel-assigned loopback port to one listener. Legacy URL discovery points at a
//! separate NodePort decoy. This makes the security assertion observable at the
//! network boundary: every governance verb must reach the tunnel, and the
//! cleartext decoy must receive nothing.

mod support;

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream, UdpSocket};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use support::{serve, MockServer, Request, Response};

const AGENT: &str = "acme-bot";
const NAMESPACE: &str = "acme-system";
const RELEASE: &str = "acme-release";
const DISCOVERED_KEY: &str = "fixture-release-platform-key";
const EXPLICIT_KEY: &str = "fixture-operator-platform-key";
const API_TUNNEL_PORT: u16 = 8123;
/// What the fake cluster's chart renders for `RELEASE` on a plain install:
/// `acme-release` does not contain the chart name, so `curie.fullname` is
/// `acme-release-curie` and every resource is `acme-release-curie-<component>`.
/// The CLI used to ask for `acme-release-<component>` (#1533).
const DISCOVERED_FULLNAME: &str = "acme-release-curie";

static TUNNEL_LOCK: Mutex<()> = Mutex::new(());

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn stub_path(bin_dir: &Path) -> std::ffi::OsString {
    let mut paths = vec![bin_dir.to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    std::env::join_paths(paths).expect("join fake kubectl PATH")
}

fn write_kubectl_stub(dir: &Path) {
    let path = dir.join("kubectl");
    fs::write(
        &path,
        r#"#!/usr/bin/env python3
import json
import os
import select
import socket
import sys
import threading
from urllib.parse import urlparse

args = sys.argv[1:]
with open(os.environ["CURIE_TEST_KUBECTL_LOG"], "a", encoding="utf-8") as log:
    log.write(" ".join(args) + "\n")

def proxy(client, backend_host, backend_port):
    try:
        upstream = socket.create_connection((backend_host, backend_port), timeout=5)
        try:
            # kubectl forwards a TCP stream, not one HTTP exchange. Preserve
            # keep-alive connections and half-closes in both directions.
            peers = {client: upstream, upstream: client}
            while peers:
                readable, _, _ = select.select(list(peers), [], [])
                for source in readable:
                    data = source.recv(65536)
                    if data:
                        peers[source].sendall(data)
                    else:
                        peers.pop(source).shutdown(socket.SHUT_WR)
        finally:
            upstream.close()
    finally:
        client.close()

if "port-forward" in args:
    local_port, remote_port = args[-1].split(":", 1)
    local_port = int(local_port)
    backend = urlparse(os.environ["CURIE_TEST_TUNNEL_BACKEND"])
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", local_port))
    listener.listen()
    assigned_port = listener.getsockname()[1]
    print(f"Forwarding from 127.0.0.1:{assigned_port} -> {remote_port}", flush=True)
    while True:
        client, _ = listener.accept()
        threading.Thread(
            target=proxy,
            args=(client, backend.hostname, backend.port),
            daemon=True,
        ).start()

if args[:2] == ["config", "view"]:
    print("https://" + os.environ["CURIE_TEST_NODE_HOST"] + ":6443", end="")
    sys.exit(0)

if "get" in args and "nodes" in args:
    print(json.dumps({
        "items": [{
            "status": {
                "addresses": [{
                    "type": "InternalIP",
                    "address": os.environ["CURIE_TEST_NODE_HOST"],
                }]
            }
        }]
    }), end="")
    sys.exit(0)

# Live `curie.fullname` discovery (#1533): the CLI selects the release's own
# api Service (then, as a fallback, its worker Deployment) by the chart's
# instance/component labels, which neither `nameOverride` nor
# `fullnameOverride` touches, and strips the component suffix back off.
#
# `CURIE_TEST_DISCOVERED_FULLNAME` is what this cluster "renders":
#   acme-release-curie  -- a plain `helm install acme-release` (the default)
#   acme-release        -- an override install, which the chart rule gets WRONG
#   ""                  -- nothing matches, so the CLI must fall back
#
# Keyed on the component label rather than the resource kind, so the api probe
# and the worker fallback are both answered whatever order they are tried in.
# The release Secret selector carries no component label, so it still falls
# through to its own branch below.
#
# `CURIE_TEST_EMPTY_COMPONENTS` (comma separated) makes the named components
# answer EMPTY while the rest still resolve. That is what an `api.deploy=false`
# install looks like from outside -- no api Service exists, and only the worker
# Deployment carries the release labels -- and it is the only way to drive the
# api probe and its worker fallback independently.
if "get" in args and "-l" in args and any("app.kubernetes.io/component=" in a for a in args):
    selector = next(a for a in args if "app.kubernetes.io/component=" in a)
    component = ""
    for part in selector.split(","):
        if part.startswith("app.kubernetes.io/component="):
            component = part.split("=", 1)[1]
    absent = [
        c for c in os.environ.get("CURIE_TEST_EMPTY_COMPONENTS", "").split(",") if c
    ]
    fullname = os.environ.get("CURIE_TEST_DISCOVERED_FULLNAME", "")
    if not fullname or component in absent:
        print("", end="")
        sys.exit(0)
    print(f"{fullname}-{component}", end="")
    sys.exit(0)

if "get" in args and "svc" in args:
    print(json.dumps({
        "spec": {
            "type": "NodePort",
            "ports": [{
                "port": 3000,
                "nodePort": int(os.environ["CURIE_TEST_NODEPORT"]),
            }],
        }
    }), end="")
    sys.exit(0)

if "get" in args and "secret" in args:
    if "-l" in args:
        print("acme-release-secrets")
        sys.exit(0)
    if any("apiKey" in arg for arg in args):
        print(os.environ["CURIE_TEST_DISCOVERED_KEY"], end="")
        sys.exit(0)

print("unexpected fake kubectl invocation: " + " ".join(args), file=sys.stderr)
sys.exit(64)
"#,
    )
    .expect("write fake kubectl");
    let mut permissions = fs::metadata(&path)
        .expect("read fake kubectl metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make fake kubectl executable");
}

/// kubectl port-forward carries TCP bytes, including multiple HTTP requests on
/// one connection. The fixture must preserve that contract: closing a socket
/// after a keep-alive response races reqwest's next non-idempotent POST.
#[test]
fn fixture_tunnel_preserves_a_connection_from_read_to_write() {
    let tools = tempfile::tempdir().expect("create tunnel fixture directory");
    write_kubectl_stub(tools.path());
    let backend = serve(|request| Response::json(200, &request.path));
    let output = Command::new("python3")
        .args([
            "-c",
            r#"
import http.client
import selectors
import subprocess
import sys

child = subprocess.Popen(
    [sys.argv[1], "port-forward", "svc/acme-api", "0:3000"],
    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
)
connection = None
try:
    with selectors.DefaultSelector() as ready:
        ready.register(child.stdout, selectors.EVENT_READ)
        assert ready.select(3), "fixture tunnel did not become ready"
    line = child.stdout.readline()
    assert line.startswith("Forwarding from 127.0.0.1:"), line
    port = int(line.split(":", 1)[1].split()[0])
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    connection.request("GET", "/agents")
    first = connection.getresponse()
    assert not first.will_close, "fixture promised a persistent connection"
    assert first.read() == b"/agents"
    original_socket = connection.sock
    connection.request("POST", "/agents", body=b"synthetic agent")
    second = connection.getresponse()
    assert second.read() == b"/agents"
    assert connection.sock is original_socket, "write must use the same tunnel"
finally:
    if connection is not None:
        connection.close()
    child.terminate()
    try:
        child.wait(timeout=3)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait(timeout=3)
"#,
        ])
        .arg(tools.path().join("kubectl"))
        .env("CURIE_TEST_KUBECTL_LOG", tools.path().join("kubectl.log"))
        .env("CURIE_TEST_TUNNEL_BACKEND", &backend.base_url)
        .output()
        .expect("run actual external tunnel fixture");
    assert!(output.status.success(), "{}", describe(&output));
    let requests = backend.recorded();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, "GET");
    assert_eq!(requests[1].method, "POST");
    assert_eq!(requests[1].body, b"synthetic agent");
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
struct WideRequest {
    method: String,
    path: String,
    api_key: Option<String>,
}

struct WideServer {
    host: IpAddr,
    port: u16,
    requests: Arc<Mutex<Vec<WideRequest>>>,
}

impl WideServer {
    fn start() -> Self {
        let host = local_nonloopback_ip();
        let listener = TcpListener::bind(("0.0.0.0", 0)).expect("bind nonloopback test listener");
        let port = listener.local_addr().expect("read listener address").port();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let recorded = Arc::clone(&recorded);
                thread::spawn(move || serve_wide_request(stream, recorded));
            }
        });
        Self {
            host,
            port,
            requests,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    fn recorded(&self) -> Vec<WideRequest> {
        self.requests.lock().unwrap().clone()
    }
}

fn local_nonloopback_ip() -> IpAddr {
    let socket = UdpSocket::bind(("0.0.0.0", 0)).expect("bind address probe");
    socket
        .connect(("192.0.2.1", 9))
        .expect("select the local routed address");
    let ip = socket.local_addr().expect("read routed address").ip();
    assert!(
        !ip.is_loopback() && !ip.is_unspecified(),
        "security test needs a nonloopback local address, got {ip}"
    );
    ip
}

/// Read one request off a raw connection, draining its body.
///
/// Shared by the two raw listeners below (`WideServer`, `RedirectServer`),
/// which both need to record what arrived rather than answer from a handler.
fn read_wide_request(reader: &mut BufReader<TcpStream>) -> Option<WideRequest> {
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return None;
    }
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let mut api_key = None;
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return None;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("x-api-key") {
                api_key = Some(value.trim().to_string());
            }
            if name.eq_ignore_ascii_case("content-length") {
                content_length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    if content_length > 0 {
        let mut body = vec![0; content_length];
        let _ = reader.read_exact(&mut body);
    }
    Some(WideRequest {
        method,
        path,
        api_key,
    })
}

fn serve_wide_request(stream: TcpStream, requests: Arc<Mutex<Vec<WideRequest>>>) {
    let mut reader = BufReader::new(stream);
    let Some(request) = read_wide_request(&mut reader) else {
        return;
    };
    let path = request.path.clone();
    requests.lock().unwrap().push(request);

    // `/health` answers exactly what the real API answers, so this decoy is
    // also usable as the TARGET of a redirect off the tunnel: a CLI that
    // followed the 3xx would certify THIS listener and carry on. That makes
    // "the decoy recorded nothing" a statement about the redirect policy, not
    // about the decoy being unable to satisfy the probe.
    let body = if path == "/health" {
        r#"{"status":"ok"}"#
    } else if path.ends_with("/agents") {
        r#"[{"id":"agent-1","name":"acme-bot","channel":{"kind":"slack","address":"C0EXAMPLE1"}}]"#
    } else if path.ends_with("/agents/agent-1/versions") {
        "[]"
    } else {
        r#"{"detail":"unexpected fixture path"}"#
    };
    let status = if path == "/health"
        || path.ends_with("/agents")
        || path.ends_with("/agents/agent-1/versions")
    {
        "200 OK"
    } else {
        "500 Internal Server Error"
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let stream = reader.get_mut();
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// A listener that answers EVERY request with `307 Location: <other origin>`,
/// recording what arrived first.
///
/// The shape reqwest's default redirect policy makes dangerous here: it strips
/// `Authorization` across hosts but NOT the custom `X-API-Key` this CLI
/// authenticates with, and a 307 preserves method and body. A `MockServer`
/// cannot express it -- its wire format has no `Location` header -- so this is a
/// raw listener, like `WideServer`.
struct RedirectServer {
    base_url: String,
    requests: Arc<Mutex<Vec<WideRequest>>>,
}

impl RedirectServer {
    /// Redirect every path to the same path on `location_origin`.
    fn start(location_origin: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind redirecting test listener");
        let base_url = format!(
            "http://{}",
            listener.local_addr().expect("listener address")
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&requests);
        let origin = Arc::new(location_origin);
        thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { break };
                let recorded = Arc::clone(&recorded);
                let origin = Arc::clone(&origin);
                thread::spawn(move || {
                    let mut reader = BufReader::new(stream);
                    let Some(request) = read_wide_request(&mut reader) else {
                        return;
                    };
                    let location = format!("{origin}{}", request.path);
                    recorded.lock().unwrap().push(request);
                    let response = format!(
                        "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\n\
                         Content-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    let stream = reader.get_mut();
                    let _ = stream.write_all(response.as_bytes());
                    let _ = stream.flush();
                });
            }
        });
        Self { base_url, requests }
    }

    fn recorded(&self) -> Vec<WideRequest> {
        self.requests.lock().unwrap().clone()
    }
}

struct Fixture {
    _tools: tempfile::TempDir,
    kubectl_path: std::ffi::OsString,
    kubectl_log: PathBuf,
    tunnel_backend: MockServer,
    /// Set when the tunnel's far end is a raw listener instead of
    /// `tunnel_backend`; it then owns `CURIE_TEST_TUNNEL_BACKEND`.
    redirect_backend: Option<RedirectServer>,
    nodeport_decoy: WideServer,
    proxy_decoy: MockServer,
}

impl Fixture {
    fn new() -> Self {
        Self::with_backend(|_| Response::json(200, "[]"))
    }

    /// The same fixture with a caller-supplied service at the far end of the
    /// tunnel. `cluster deploy` verification (#1533) turns on what that service
    /// answers, so each case needs its own responder.
    fn with_backend(handler: impl Fn(&Request) -> Response + Send + Sync + 'static) -> Self {
        let tools = tempfile::tempdir().expect("create fake tool directory");
        write_kubectl_stub(tools.path());
        let kubectl_path = stub_path(tools.path());
        let kubectl_log = tools.path().join("kubectl.log");
        let tunnel_backend = serve(handler);
        let nodeport_decoy = WideServer::start();
        let proxy_decoy = serve(versions_api);
        Self {
            _tools: tools,
            kubectl_path,
            kubectl_log,
            tunnel_backend,
            redirect_backend: None,
            nodeport_decoy,
            proxy_decoy,
        }
    }

    /// The same fixture with a tunnel that reaches a listener redirecting every
    /// request to the cleartext NodePort decoy -- a different origin, which is
    /// the case reqwest's default policy would follow with `X-API-Key` intact.
    fn with_redirecting_backend() -> Self {
        let mut fixture = Self::with_backend(curie_api);
        fixture.redirect_backend = Some(RedirectServer::start(fixture.nodeport_decoy.base_url()));
        fixture
    }

    /// What the fake port forward proxies to. The redirecting backend wins when
    /// one is installed.
    fn tunnel_url(&self) -> &str {
        self.redirect_backend
            .as_ref()
            .map(|server| server.base_url.as_str())
            .unwrap_or(&self.tunnel_backend.base_url)
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_with_env(args, &[])
    }

    fn run_with_env(&self, args: &[&str], env: &[(&str, &str)]) -> Output {
        let mut command = Command::new(bin());
        command
            .args(args)
            .args(["--namespace", NAMESPACE, "--release", RELEASE])
            .env("PATH", &self.kubectl_path)
            .env("CURIE_TEST_KUBECTL_LOG", &self.kubectl_log)
            .env("CURIE_TEST_TUNNEL_BACKEND", self.tunnel_url())
            .env("CURIE_TEST_NODEPORT", self.nodeport_decoy.port.to_string())
            .env("CURIE_TEST_NODE_HOST", self.nodeport_decoy.host.to_string())
            .env("CURIE_TEST_DISCOVERED_KEY", DISCOVERED_KEY)
            .env("CURIE_TEST_DISCOVERED_FULLNAME", DISCOVERED_FULLNAME)
            .env("HTTP_PROXY", &self.proxy_decoy.base_url)
            .env("http_proxy", &self.proxy_decoy.base_url)
            .env("ALL_PROXY", &self.proxy_decoy.base_url)
            .env("all_proxy", &self.proxy_decoy.base_url)
            .env_remove("NO_PROXY")
            .env_remove("no_proxy")
            .env_remove("CURIE_API_URL")
            .env_remove("CURIE_API_KEY");
        for (name, value) in env {
            command.env(name, value);
        }
        command
            .output()
            .unwrap_or_else(|error| panic!("run curie {}: {error}", args.join(" ")))
    }

    fn kubectl_log(&self) -> String {
        fs::read_to_string(&self.kubectl_log).unwrap_or_default()
    }
}

fn wait_for_tunnel_port_free() {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if TcpListener::bind(("127.0.0.1", API_TUNNEL_PORT)).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "localhost:{API_TUNNEL_PORT} remained occupied after the prior command"
        );
        thread::sleep(Duration::from_millis(20));
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

fn versions_api(request: &Request) -> Response {
    if request.path.ends_with("/agents") {
        Response::json(
            200,
            r#"[{"id":"agent-1","name":"acme-bot","channels":[{"kind":"slack","address":"C0EXAMPLE1"}],"memory":false}]"#,
        )
    } else if request.path.ends_with("/agents/agent-1/versions") {
        Response::json(200, "[]")
    } else {
        Response::json(500, r#"{"detail":"unexpected fixture path"}"#)
    }
}

#[test]
fn all_discovered_cluster_api_verbs_avoid_the_nodeport() {
    let _tunnel_lock = TUNNEL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let fixture = Fixture::new();
    let cases: &[(&str, &[&str])] = &[
        ("versions", &["cluster", "versions", AGENT]),
        ("kill", &["cluster", "kill", AGENT, "--yes"]),
        ("resume", &["cluster", "resume", AGENT]),
        ("budget", &["cluster", "budget", AGENT, "--limit", "1"]),
        ("overrides", &["cluster", "overrides", AGENT]),
        (
            "reset thread",
            &[
                "cluster",
                "reset-thread",
                AGENT,
                "--thread-key",
                "thread-1",
                "--yes",
            ],
        ),
        ("memory", &["cluster", "memory", AGENT]),
        (
            "memory add",
            &[
                "cluster",
                "memory",
                AGENT,
                "--add",
                "ask before translating to French",
            ],
        ),
        ("approvals", &["cluster", "approvals", AGENT, "--list"]),
        ("delete", &["cluster", "delete", AGENT, "--yes"]),
    ];

    for (name, args) in cases {
        wait_for_tunnel_port_free();
        let before = fixture.tunnel_backend.recorded().len();
        let output = fixture.run(args);
        let requests = fixture.tunnel_backend.recorded();
        assert_eq!(
            requests.len(),
            before + 1,
            "{name} must make its agent lookup through the loopback tunnel\n{}\nkubectl:\n{}",
            describe(&output),
            fixture.kubectl_log()
        );
        let request = &requests[before];
        assert_eq!(request.method, "GET", "{name} must resolve the agent");
        assert_eq!(request.path, "/agents", "{name} used the wrong API base");
        assert_eq!(
            request.header("x-api-key"),
            Some(DISCOVERED_KEY),
            "{name} must authenticate only through the tunnel"
        );
        assert!(
            fixture.nodeport_decoy.recorded().is_empty(),
            "{name} sent a request to the discovered cleartext NodePort: {:?}",
            fixture.nodeport_decoy.recorded()
        );
        assert!(
            fixture.proxy_decoy.recorded().is_empty(),
            "{name} exposed the discovered key to the system HTTP proxy: {:?}",
            fixture.proxy_decoy.recorded()
        );
    }

    assert_eq!(
        fixture.tunnel_backend.recorded().len(),
        cases.len(),
        "every named cluster API verb must exercise the shared tunnel"
    );
}

#[test]
fn discovered_key_with_explicit_remote_http_refuses_before_api_client_construction() {
    let fixture = Fixture::new();
    let url = fixture.nodeport_decoy.base_url();
    let output = fixture.run(&["cluster", "versions", AGENT, "--api-url", &url]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "cleartext request must be refused"
    );
    assert!(
        fixture.nodeport_decoy.recorded().is_empty(),
        "refusal must happen before any API request: {:?}",
        fixture.nodeport_decoy.recorded()
    );
    assert!(
        stderr.contains("refusing to send the auto-discovered release key over cleartext HTTP"),
        "missing refusal reason: {stderr}"
    );
    for recovery in ["--api-key", "https://", "omit --api-url"] {
        assert!(
            stderr.contains(recovery),
            "refusal must name recovery {recovery:?}: {stderr}"
        );
    }
    assert!(
        !stderr.contains("warning: API endpoint"),
        "ApiClient was constructed before the refusal: {stderr}"
    );
}

#[test]
fn blank_environment_key_is_omitted_for_refusal_and_automatic_discovery() {
    let fixture = Fixture::new();
    let remote_url = fixture.nodeport_decoy.base_url();
    let refused = fixture.run_with_env(
        &["cluster", "versions", AGENT, "--api-url", &remote_url],
        &[("CURIE_API_KEY", "")],
    );
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        !refused.status.success(),
        "a blank environment key must not acknowledge remote HTTP"
    );
    assert!(
        stderr.contains("refusing to send the auto-discovered release key over cleartext HTTP"),
        "blank CURIE_API_KEY must take the discovered key refusal path: {stderr}"
    );
    assert!(
        fixture.nodeport_decoy.recorded().is_empty(),
        "blank CURIE_API_KEY leaked to remote HTTP: {:?}",
        fixture.nodeport_decoy.recorded()
    );
    assert!(
        fixture.proxy_decoy.recorded().is_empty(),
        "blank CURIE_API_KEY leaked through the system proxy: {:?}",
        fixture.proxy_decoy.recorded()
    );
    fs::write(&fixture.kubectl_log, "").expect("clear kubectl log between blank key cases");

    let _tunnel_lock = TUNNEL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    wait_for_tunnel_port_free();
    let automatic = fixture.run_with_env(&["cluster", "versions", AGENT], &[("CURIE_API_KEY", "")]);
    let requests = fixture.tunnel_backend.recorded();
    assert_eq!(
        requests.len(),
        1,
        "automatic mode must use the tunnel after normalizing a blank key\n{}\nkubectl:\n{}",
        describe(&automatic),
        fixture.kubectl_log()
    );
    assert_eq!(requests[0].path, "/agents");
    assert_eq!(requests[0].header("x-api-key"), Some(DISCOVERED_KEY));
    assert!(
        fixture.kubectl_log().contains("apiKey"),
        "automatic mode must discover the real release key: {}",
        fixture.kubectl_log()
    );
    assert!(
        fixture.nodeport_decoy.recorded().is_empty(),
        "automatic mode must not use the NodePort"
    );
    assert!(
        fixture.proxy_decoy.recorded().is_empty(),
        "automatic mode must bypass the system proxy"
    );
}

#[test]
fn automatic_dry_run_is_cluster_and_network_offline() {
    let _tunnel_lock = TUNNEL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    wait_for_tunnel_port_free();
    let fixture = Fixture::new();
    let output = fixture.run(&["cluster", "versions", AGENT, "--dry-run"]);

    assert!(
        output.status.success(),
        "automatic dry run must remain offline and succeed\n{}",
        describe(&output)
    );
    assert!(
        fixture.kubectl_log().is_empty(),
        "dry run must not discover a Secret or start a tunnel: {}",
        fixture.kubectl_log()
    );
    assert!(fixture.tunnel_backend.recorded().is_empty());
    assert!(fixture.nodeport_decoy.recorded().is_empty());
    assert!(fixture.proxy_decoy.recorded().is_empty());
}

#[test]
fn loopback_looking_userinfo_does_not_hide_a_remote_http_host() {
    let fixture = Fixture::new();
    let url = format!(
        "http://127.0.0.1@{}:{}",
        fixture.nodeport_decoy.host, fixture.nodeport_decoy.port
    );
    let output = fixture.run(&["cluster", "versions", AGENT, "--api-url", &url]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "remote HTTP must be refused");
    assert!(
        stderr.contains("refusing to send the auto-discovered release key over cleartext HTTP"),
        "userinfo must not fool the remote host classifier: {stderr}"
    );
    assert!(
        fixture.nodeport_decoy.recorded().is_empty(),
        "userinfo bypass sent the key to the actual remote host: {:?}",
        fixture.nodeport_decoy.recorded()
    );
    assert!(
        fixture.proxy_decoy.recorded().is_empty(),
        "userinfo bypass sent the key through the system proxy: {:?}",
        fixture.proxy_decoy.recorded()
    );
    assert!(fixture.tunnel_backend.recorded().is_empty());
}

#[test]
fn discovered_key_with_explicit_https_is_accepted_without_a_tunnel() {
    let fixture = Fixture::new();
    let output = fixture.run(&[
        "cluster",
        "versions",
        AGENT,
        "--api-url",
        "https://api.example.com",
        "--dry-run",
    ]);
    assert!(
        output.status.success(),
        "discovered credentials over HTTPS must remain supported\n{}",
        describe(&output)
    );
    assert!(
        !fixture.kubectl_log().contains("port-forward"),
        "an explicit HTTPS URL must direct dial: {}",
        fixture.kubectl_log()
    );
}

#[test]
fn explicit_key_acknowledges_reachable_remote_http() {
    let fixture = Fixture::new();
    let url = fixture.nodeport_decoy.base_url();
    let output = fixture.run(&[
        "cluster",
        "versions",
        AGENT,
        "--api-url",
        &url,
        "--api-key",
        EXPLICIT_KEY,
    ]);
    assert!(
        output.status.success(),
        "an explicit key is the existing cleartext acknowledgement\n{}",
        describe(&output)
    );
    assert!(
        fixture.nodeport_decoy.recorded().is_empty(),
        "proxied explicit traffic must not contact the remote target directly: {:?}",
        fixture.nodeport_decoy.recorded()
    );
    let requests = fixture.proxy_decoy.recorded();
    assert_eq!(
        requests.len(),
        2,
        "the proxy must carry the explicit agent lookup and versions request"
    );
    assert!(requests[0].path.ends_with("/agents"));
    assert!(requests[1].path.ends_with("/agents/agent-1/versions"));
    assert!(requests.iter().all(|request| {
        request.method == "GET" && request.header("x-api-key") == Some(EXPLICIT_KEY)
    }));
    assert!(
        fixture.kubectl_log().is_empty(),
        "fully explicit HTTP credentials must not invoke kubectl: {}",
        fixture.kubectl_log()
    );
}

#[test]
fn discovered_key_with_explicit_loopback_http_remains_supported() {
    let fixture = Fixture::new();
    let server = serve(versions_api);
    let output = fixture.run(&["cluster", "versions", AGENT, "--api-url", &server.base_url]);
    assert!(
        output.status.success(),
        "loopback HTTP must remain supported\n{}",
        describe(&output)
    );
    let requests = server.recorded();
    assert_eq!(
        requests.len(),
        2,
        "versions must resolve then list versions"
    );
    assert!(requests.iter().all(|request| {
        request.method == "GET" && request.header("x-api-key") == Some(DISCOVERED_KEY)
    }));
    assert!(
        !fixture.kubectl_log().contains("port-forward"),
        "an explicit loopback URL must not start a tunnel: {}",
        fixture.kubectl_log()
    );
}

#[test]
fn fully_explicit_https_connection_never_invokes_kubectl() {
    let fixture = Fixture::new();
    let output = fixture.run(&[
        "cluster",
        "versions",
        AGENT,
        "--api-url",
        "https://api.example.com",
        "--api-key",
        EXPLICIT_KEY,
        "--dry-run",
    ]);
    assert!(
        output.status.success(),
        "fully explicit HTTPS connection must remain supported\n{}",
        describe(&output)
    );
    assert!(
        fixture.kubectl_log().is_empty(),
        "fully explicit connection must skip every discovery call: {}",
        fixture.kubectl_log()
    );
}

// ---------------------------------------------------------------------------
// `cluster deploy`: the self-plumbed tunnel's target, and the verification that
// what answers on it is actually the Curie API (#1533 symptoms 1 and 2).
//
// These are consumer-path tests on purpose. A unit test of the `/health` probe
// proves the probe works; it does NOT prove `cluster deploy` calls it -- delete
// the call and every unit test in this change still passes. What dies under
// that mutation is `deploy_refuses_and_posts_nothing_when_the_tunnel_reaches_a
// _non_curie_service`, because the bundle POST it forbids then happens.
// ---------------------------------------------------------------------------

/// A bundle real enough for `cluster deploy` to pack and post.
fn deploy_bundle() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("create deploy bundle directory");
    curie::scaffold::scaffold(dir.path(), AGENT).expect("scaffold the deploy bundle");
    dir
}

/// The real API's `/health`: 200, JSON, `{"status":"ok"}` and NOTHING else
/// (`apps/api/src/curie_api/main.py:318-320`), plus enough of the agent surface
/// for the deploy to reach its first write.
///
/// Byte-for-byte what production returns, deliberately. A fixture richer than
/// production certifies checks production would fail: with a `service` field
/// here, tightening the verification to require one would leave this suite
/// green while refusing every real deployment.
fn curie_api(request: &Request) -> Response {
    if request.path == "/health" {
        Response::json(200, r#"{"status":"ok"}"#)
    } else if request.method == "GET" && request.path.ends_with("/agents") {
        Response::json(200, "[]")
    } else {
        Response::json(500, r#"{"detail":"unexpected fixture path"}"#)
    }
}

/// The squatted-port case: something is listening and answering 200, but it is
/// a dev server, not the platform API. This is the shape the issue reported --
/// the bundle was posted at the stranger and its 404 was reported as the deploy
/// result.
fn html_squatter(_request: &Request) -> Response {
    Response {
        status: 200,
        content_type: "text/html".into(),
        body: b"<html><body>vite dev server</body></html>".to_vec(),
    }
}

/// A different service that routes nothing at `/health`.
fn wrong_service(_request: &Request) -> Response {
    Response::json(404, r#"{"detail":"Not Found"}"#)
}

/// THE consumer-path test (#1533 DW-15). Deleting the verification call from
/// `cluster deploy` makes this fail: the deploy would proceed, and the bundle
/// POST this asserts never happens would appear in the recording.
///
/// Asserted on the stub's request log rather than an error variant, because
/// "posted nothing at the stranger" is the property that matters and only the
/// wire can show it.
#[test]
fn deploy_refuses_and_posts_nothing_when_the_tunnel_reaches_a_non_curie_service() {
    for (name, fixture) in [
        ("200 text/html", Fixture::with_backend(html_squatter)),
        ("404", Fixture::with_backend(wrong_service)),
    ] {
        let bundle = deploy_bundle();
        let output = fixture.run(&[
            "cluster",
            "deploy",
            "--plugin-dir",
            bundle.path().to_str().expect("bundle path"),
        ]);

        assert!(
            !output.status.success(),
            "{name}: deploy must refuse a tunnel that does not reach the Curie API\n{}\nkubectl:\n{}",
            describe(&output),
            fixture.kubectl_log()
        );

        let requests = fixture.tunnel_backend.recorded();
        assert_eq!(
            requests.len(),
            1,
            "{name}: the ONLY request may be the health probe; anything else means the \
             bundle was posted at the stranger: {requests:?}\n{}",
            describe(&output)
        );
        assert_eq!(requests[0].method, "GET", "{name}");
        assert_eq!(requests[0].path, "/health", "{name}");
        assert!(
            requests
                .iter()
                .all(|request| request.header("x-api-key").is_none()),
            "{name}: no request may carry the release key before the endpoint is proven \
             to be Curie (#705): {requests:?}"
        );
        assert!(
            fixture.proxy_decoy.recorded().is_empty(),
            "{name}: the loopback probe must bypass the system HTTP proxy: {:?}",
            fixture.proxy_decoy.recorded()
        );

        // #1400's honesty standard: the refusal names what it looked at and
        // both ways out of it.
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("acme-release-curie-api"),
            "{name}: the refusal must name the resolved service: {stderr}"
        );
        for recovery in ["--api-local-port", "--api-url"] {
            assert!(
                stderr.contains(recovery),
                "{name}: the refusal must name recovery {recovery:?}: {stderr}"
            );
        }
    }
}

/// #705 / DW-16. At the moment of the probe the auto-discovered strong release
/// key is already in hand, and the endpoint is not yet known to be Curie.
/// Sending the key there is exactly the egress the cleartext guard exists to
/// prevent, so the probe must be unauthenticated -- and must stay so even
/// against an endpoint that answers correctly.
#[test]
fn the_health_probe_is_unauthenticated() {
    let fixture = Fixture::with_backend(curie_api);
    let bundle = deploy_bundle();
    let output = fixture.run(&[
        "cluster",
        "deploy",
        "--plugin-dir",
        bundle.path().to_str().expect("bundle path"),
    ]);

    let requests = fixture.tunnel_backend.recorded();
    let health = requests
        .iter()
        .find(|request| request.path == "/health")
        .unwrap_or_else(|| {
            panic!(
                "the self-plumbed deploy must verify the endpoint: {requests:?}\n{}",
                describe(&output)
            )
        });
    assert_eq!(
        health.header("x-api-key"),
        None,
        "the health probe must not carry the auto-discovered release key (#705)"
    );
    assert!(
        fixture.kubectl_log().contains("apiKey"),
        "the key really was discovered before the probe, so this is not vacuous: {}",
        fixture.kubectl_log()
    );
}

/// The positive control that stops the refusal from being unconditional: a real
/// Curie API answers `{"status":"ok"}` and the deploy goes on to write, with the
/// discovered key, over the same tunnel.
#[test]
fn deploy_proceeds_when_health_reports_ok() {
    let fixture = Fixture::with_backend(curie_api);
    let bundle = deploy_bundle();
    let output = fixture.run(&[
        "cluster",
        "deploy",
        "--plugin-dir",
        bundle.path().to_str().expect("bundle path"),
    ]);

    let requests = fixture.tunnel_backend.recorded();
    assert_eq!(
        requests.first().map(|request| request.path.as_str()),
        Some("/health"),
        "verification must come first, before any target is posted (#1279): \
         {requests:?}\n{}",
        describe(&output)
    );
    assert!(
        requests.len() > 1,
        "a healthy endpoint must not be refused: {requests:?}\n{}",
        describe(&output)
    );
    assert!(
        requests[1..]
            .iter()
            .all(|request| request.header("x-api-key") == Some(DISCOVERED_KEY)),
        "every request after verification must authenticate with the discovered key: \
         {requests:?}"
    );
    assert!(
        requests[1..].iter().any(|request| request.method == "POST"),
        "the deploy must reach its first write over the verified tunnel: {requests:?}\n{}",
        describe(&output)
    );
    assert!(
        fixture.nodeport_decoy.recorded().is_empty(),
        "deploy must not touch the cleartext NodePort: {:?}",
        fixture.nodeport_decoy.recorded()
    );
    assert!(
        fixture.proxy_decoy.recorded().is_empty(),
        "deploy must bypass the system HTTP proxy: {:?}",
        fixture.proxy_decoy.recorded()
    );
}

/// #1533 symptom 1, through the real binary. `acme-release` does not contain
/// the chart name, so the chart renders `acme-release-curie-api` and the CLI
/// used to port-forward `svc/acme-release-api` -- a Service that does not
/// exist. Every other naming test in this change is a unit test of a pure
/// builder; this is the one that proves the resolved name reaches kubectl.
#[test]
fn the_self_plumbed_tunnel_targets_the_chart_rendered_service() {
    let fixture = Fixture::with_backend(curie_api);
    let bundle = deploy_bundle();
    let output = fixture.run(&[
        "cluster",
        "deploy",
        "--plugin-dir",
        bundle.path().to_str().expect("bundle path"),
    ]);
    let log = fixture.kubectl_log();

    assert!(
        log.contains("svc/acme-release-curie-api"),
        "the tunnel must target the chart-rendered Service: {log}\n{}",
        describe(&output)
    );
    assert!(
        !log.contains("svc/acme-release-api"),
        "the tunnel must not compute `{{release}}-api`: {log}"
    );
}

/// The Finding 1 reversal, proved end to end. Under `nameOverride` or
/// `fullnameOverride` the chart renders `acme-release-api` while still carrying
/// `instance=acme-release,component=api`, so LIVE discovery gets it right and
/// the pure chart rule gets it wrong. An implementation that computes the name
/// instead of discovering it passes every other test here and fails this one.
#[test]
fn an_override_install_is_discovered_rather_than_computed() {
    let fixture = Fixture::with_backend(curie_api);
    let bundle = deploy_bundle();
    let output = fixture.run_with_env(
        &[
            "cluster",
            "deploy",
            "--plugin-dir",
            bundle.path().to_str().expect("bundle path"),
        ],
        &[("CURIE_TEST_DISCOVERED_FULLNAME", RELEASE)],
    );
    let log = fixture.kubectl_log();

    assert!(
        log.contains("svc/acme-release-api"),
        "an override install renders `acme-release-api`; discovery must follow the \
         cluster, not the chart rule: {log}\n{}",
        describe(&output)
    );
    assert!(
        !log.contains("svc/acme-release-curie-api"),
        "the computed chart-rule name must not win over what the cluster reports: {log}"
    );
}

/// The offline fallback is wired, not merely written: with nothing matching the
/// discovery selectors the CLI must still name a Service and proceed, using the
/// chart's own no-override rule.
#[test]
fn discovery_failure_falls_back_to_the_chart_rule() {
    let fixture = Fixture::with_backend(curie_api);
    let bundle = deploy_bundle();
    let output = fixture.run_with_env(
        &[
            "cluster",
            "deploy",
            "--plugin-dir",
            bundle.path().to_str().expect("bundle path"),
        ],
        &[("CURIE_TEST_DISCOVERED_FULLNAME", "")],
    );
    let log = fixture.kubectl_log();

    assert!(
        log.contains("app.kubernetes.io/component=api"),
        "discovery must have been ATTEMPTED, else the fallback is untested: {log}"
    );
    assert!(
        log.contains("svc/acme-release-curie-api"),
        "an unanswerable cluster must fall back to the chart rule, never hard-fail: \
         {log}\n{}",
        describe(&output)
    );
    assert!(
        fixture
            .tunnel_backend
            .recorded()
            .iter()
            .any(|request| request.path == "/health"),
        "the deploy must proceed to verification after falling back: {}",
        describe(&output)
    );
}

/// The exact argv of the api-Service probe, and of the worker-Deployment
/// fallback that follows it. Spelled out rather than rebuilt from the CLI's own
/// helper, so a change to the selector or the jsonpath has to be made
/// deliberately in both places. The `{range .items[*]}` form is load-bearing:
/// it makes CARDINALITY observable, so two matches are refused rather than
/// silently resolved to the first.
const API_SERVICE_PROBE: &str = concat!(
    "get svc -l app.kubernetes.io/instance=acme-release,app.kubernetes.io/component=api ",
    r#"-o jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}"#
);
const WORKER_DEPLOYMENT_PROBE: &str = concat!(
    "get deployment -l app.kubernetes.io/instance=acme-release,",
    "app.kubernetes.io/component=worker ",
    r#"-o jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}"#
);

/// The worker-Deployment fallback, through the real binary. `api.deploy=false`
/// is a supported install: no api Service exists, so the api probe answers
/// nothing and only the worker Deployment still carries the release labels.
///
/// The fixture renders an OVERRIDE install (`acme-release-worker`) on purpose.
/// Under the plain render the fallback's answer and the chart rule agree, so
/// the whole fallback could be deleted and a test built on it would still pass;
/// here they disagree. Deleting the second probe from
/// `ops::discover_release_fullname` drops discovery to `None` and the CLI back
/// to the chart rule's `svc/acme-release-curie-api`, which this forbids.
#[test]
fn the_worker_deployment_fallback_resolves_a_release_with_no_api_service() {
    let fixture = Fixture::with_backend(curie_api);
    let bundle = deploy_bundle();
    let output = fixture.run_with_env(
        &[
            "cluster",
            "deploy",
            "--plugin-dir",
            bundle.path().to_str().expect("bundle path"),
        ],
        &[
            ("CURIE_TEST_DISCOVERED_FULLNAME", RELEASE),
            ("CURIE_TEST_EMPTY_COMPONENTS", "api"),
        ],
    );
    let log = fixture.kubectl_log();

    let api_probe = log.find(API_SERVICE_PROBE).unwrap_or_else(|| {
        panic!(
            "the api Service must be probed at all, else the worker fallback was reached \
             for the wrong reason: {log}"
        )
    });
    let worker_probe = log.find(WORKER_DEPLOYMENT_PROBE).unwrap_or_else(|| {
        panic!(
            "an api Service that answers nothing must fall back to the worker Deployment: \
             {log}\n{}",
            describe(&output)
        )
    });
    // Relative to EACH OTHER, not to the whole log: release-key discovery
    // legitimately fires its own `get secret` calls before either probe, and
    // that ordering is incidental to this test.
    assert!(
        api_probe < worker_probe,
        "the worker Deployment is the FALLBACK; probing it before the api Service would \
         resolve installs that have an api Service from the wrong object: {log}"
    );
    assert!(
        log.contains("svc/acme-release-api"),
        "the tunnel must use the fullname the worker probe reported: {log}\n{}",
        describe(&output)
    );
    assert!(
        !log.contains("svc/acme-release-curie-api"),
        "falling through to the chart rule means the worker fallback did not run: {log}"
    );
    assert!(
        fixture
            .tunnel_backend
            .recorded()
            .iter()
            .any(|request| request.path == "/health"),
        "the deploy must proceed to verification over the worker-derived tunnel: {}",
        describe(&output)
    );
}

/// #705 again, one hop further out. reqwest strips `Authorization` when a
/// redirect crosses hosts but NOT a custom header, and this CLI authenticates
/// with `X-API-Key`, so under the default follow-up-to-10-hops policy a
/// `307 Location: http://elsewhere/...` on the tunnel would (1) certify the
/// redirect TARGET instead of the endpoint `/health` was asked about and
/// (2) hand that target the auto-discovered strong release key, method and body
/// intact -- which also walks straight around the cleartext refusal, since only
/// the INITIAL URL is classified.
///
/// Deleting `.redirect(reqwest::redirect::Policy::none())` from
/// `api::http_client` is the mutation this kills: the probe then follows to the
/// decoy, whose `/health` answers `{"status":"ok"}`, the deploy certifies, and
/// the bundle POST that follows carries the key to the decoy too. Asserted on
/// the two request logs rather than on an error variant, because "the other
/// origin received nothing" is the property and only the wire shows it.
#[test]
fn deploy_never_follows_a_redirect_off_the_tunnel_to_another_origin() {
    let fixture = Fixture::with_redirecting_backend();
    let bundle = deploy_bundle();
    let output = fixture.run(&[
        "cluster",
        "deploy",
        "--plugin-dir",
        bundle.path().to_str().expect("bundle path"),
    ]);

    let followed = fixture.nodeport_decoy.recorded();
    assert!(
        followed.is_empty(),
        "the deploy followed a 3xx off the loopback tunnel to another origin{}: {followed:?}\n{}",
        if followed.iter().any(|request| request.api_key.is_some()) {
            ", handing it the auto-discovered release key"
        } else {
            ""
        },
        describe(&output)
    );

    let tunnel = fixture
        .redirect_backend
        .as_ref()
        .expect("the redirecting backend")
        .recorded();
    assert_eq!(
        tunnel.len(),
        1,
        "the ONLY request may be the health probe; anything after it means a redirecting \
         endpoint was certified and the bundle posted through it: {tunnel:?}\n{}",
        describe(&output)
    );
    assert_eq!(tunnel[0].method, "GET");
    assert_eq!(tunnel[0].path, "/health");
    assert_eq!(
        tunnel[0].api_key, None,
        "the probe must stay unauthenticated even against a redirector (#705)"
    );
    assert!(
        !output.status.success(),
        "a redirect is not a certification: the deploy must refuse\n{}",
        describe(&output)
    );
    assert!(
        fixture.proxy_decoy.recorded().is_empty(),
        "the loopback probe must bypass the system HTTP proxy: {:?}",
        fixture.proxy_decoy.recorded()
    );

    // #1400's honesty standard: where it pointed IS the diagnosis, so the
    // refusal names the target rather than reporting a bare non-200.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(&fixture.nodeport_decoy.base_url()),
        "the refusal must name where the endpoint tried to send the request: {stderr}"
    );
    assert!(
        stderr.contains("acme-release-curie-api"),
        "the refusal must name the resolved Service: {stderr}"
    );
    for recovery in ["--api-local-port", "--api-url"] {
        assert!(
            stderr.contains(recovery),
            "the refusal must name recovery {recovery:?}: {stderr}"
        );
    }
}

/// Decision 2's deliberate asymmetry: an explicit `--api-url` is the operator's
/// own choice of endpoint, possibly a gateway that exposes no `/health`. Adding
/// a hard failure there would break a path that works today, so it is not
/// probed at all -- one path, one behavior, no warning either.
#[test]
fn an_explicit_api_url_is_not_health_probed() {
    let fixture = Fixture::with_backend(curie_api);
    let bundle = deploy_bundle();
    let api_url = fixture.tunnel_backend.base_url.clone();
    let output = fixture.run(&[
        "cluster",
        "deploy",
        "--plugin-dir",
        bundle.path().to_str().expect("bundle path"),
        "--api-url",
        &api_url,
    ]);

    let requests = fixture.tunnel_backend.recorded();
    assert!(
        !requests.iter().any(|request| request.path == "/health"),
        "an explicit --api-url must not be health-probed: {requests:?}\n{}",
        describe(&output)
    );
    assert!(
        !fixture.kubectl_log().contains("port-forward"),
        "an explicit --api-url must direct-dial with no tunnel: {}",
        fixture.kubectl_log()
    );
}
