use std::fs;
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use curie::message::{port_forward_command, start_port_forward};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_curie")
}

fn cluster_message_dry_run(extra: &[&str]) -> String {
    let output = Command::new(bin())
        .args(["cluster", "message", "hello"])
        .args(extra)
        .args(["--listen-host", "127.0.0.1", "--dry-run"])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_VALKEY_PASSWORD")
        .output()
        .expect("run cluster message dry run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "cluster message dry run must succeed; stdout: {stdout}; stderr: {stderr}"
    );
    format!("{stdout}\n{stderr}")
}

fn write_eval_cases(dir: &Path) -> PathBuf {
    let path = dir.join("cases.json");
    fs::write(
        &path,
        r#"{"name":"smoke","cases":[{"id":"one","input":"hi","grader":{"kind":"contains","expected":"hi"}}]}"#,
    )
    .expect("write eval cases");
    path
}

fn cluster_eval_dry_run(extra: &[&str]) -> String {
    let dir = tempfile::tempdir().expect("create eval cases directory");
    let cases = write_eval_cases(dir.path());
    let output = Command::new(bin())
        .args(["cluster", "eval"])
        .args(extra)
        .args([
            "--cases",
            cases.to_str().expect("cases path is utf-8"),
            "--listen-host",
            "127.0.0.1",
            "--channel",
            "C0EXAMPLE1",
            "--dry-run",
        ])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .env_remove("CURIE_API_KEY")
        .env_remove("CURIE_VALKEY_PASSWORD")
        .output()
        .expect("run cluster eval dry run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "cluster eval dry run must succeed; stdout: {stdout}; stderr: {stderr}"
    );
    format!("{stdout}\n{stderr}")
}

fn write_fake_kubectl(dir: &Path, name: &str, readiness_host: &str) -> PathBuf {
    let path = dir.join(name);
    let body = r#"#!/usr/bin/env python3
import socket
import sys

mapping = sys.argv[-1]
requested, remote = mapping.split(":", 1)
listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(requested)))
listener.listen()
assigned = listener.getsockname()[1]
ready_host = "__READY_HOST__"
if ready_host == "::1":
    print(f"Forwarding from [::1]:{assigned} -> {remote}", flush=True)
else:
    print(f"Forwarding from {ready_host}:{assigned} -> {remote}", flush=True)
while True:
    connection, _ = listener.accept()
    connection.close()
"#
    .replace("__READY_HOST__", readiness_host);
    fs::write(&path, body).expect("write fake kubectl");
    let mut permissions = fs::metadata(&path)
        .expect("read fake kubectl metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make fake kubectl executable");
    path
}

async fn wait_until_released(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            drop(listener);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "dropping the port forward guard must release assigned port {port}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn cluster_message_uses_ephemeral_defaults_and_propagates_assigned_ports() {
    let plan = cluster_message_dry_run(&[]);
    assert!(
        plan.contains("kubectl -n curie port-forward svc/curie-valkey 0:6379"),
        "an omitted Valkey local port must request zero: {plan}"
    );
    assert!(
        plan.contains("kubectl -n curie port-forward svc/curie-api 0:8000"),
        "an omitted API local port must request zero: {plan}"
    );
    assert!(
        plan.contains("poll replies at http://127.0.0.1:0/cluster-message-replies/<uuid-v4>"),
        "the ephemeral API port must also shape the loopback relay poll URL: {plan}"
    );
    assert!(
        plan.contains("no reply endpoint"),
        "the queued relay handle must advertise no callback endpoint: {plan}"
    );
    assert!(
        !plan.contains("stub advertised") && !plan.contains("/api/"),
        "disconnected cluster mode must not advertise a Slack callback stub: {plan}"
    );

    let dir = tempfile::tempdir().expect("create fake kubectl directory");
    let ipv4 = write_fake_kubectl(dir.path(), "kubectl-ipv4", "127.0.0.1");
    let ipv6 = write_fake_kubectl(dir.path(), "kubectl-ipv6", "::1");
    let fullname = curie::ops::chart_fullname("acme-release");
    let mut first_cmd = port_forward_command("acme-system", &fullname, "valkey", 0, 6379);
    first_cmd.program = ipv4.to_string_lossy().into_owned();
    let mut second_cmd = port_forward_command("acme-system", &fullname, "api", 0, 8000);
    second_cmd.program = ipv6.to_string_lossy().into_owned();

    let (first_guard, first_port) = start_port_forward(&first_cmd, 0, "Valkey")
        .await
        .expect("start zero requested IPv4 port forward");
    let (second_guard, second_port) = start_port_forward(&second_cmd, 0, "API")
        .await
        .expect("start zero requested IPv6 readiness port forward");

    assert_ne!(first_port, 0, "kubectl must report its assigned port");
    assert_ne!(second_port, 0, "kubectl must report its assigned port");
    assert_ne!(
        first_port, second_port,
        "independent zero requested forwards must not collide"
    );
    TcpStream::connect(("127.0.0.1", first_port))
        .expect("returned Valkey port must reach the forward");
    TcpStream::connect(("127.0.0.1", second_port))
        .expect("returned API port must reach the forward");

    drop(first_guard);
    drop(second_guard);
    wait_until_released(first_port).await;
    wait_until_released(second_port).await;
}

#[test]
fn cluster_message_preserves_explicit_tunnel_port_overrides() {
    let plan =
        cluster_message_dry_run(&["--valkey-local-port", "18156", "--api-local-port", "18157"]);
    assert!(
        plan.contains("kubectl -n curie port-forward svc/curie-valkey 18156:6379"),
        "the explicit Valkey local port must remain exact: {plan}"
    );
    assert!(
        plan.contains("kubectl -n curie port-forward svc/curie-api 18157:8000"),
        "the explicit API local port must remain exact: {plan}"
    );
    assert!(
        plan.contains("poll replies at http://127.0.0.1:18157/cluster-message-replies/<uuid-v4>"),
        "the explicit API port must also shape the loopback relay poll URL: {plan}"
    );
    assert!(
        plan.contains("no reply endpoint"),
        "the queued relay handle must advertise no callback endpoint: {plan}"
    );
    assert!(
        !plan.contains("stub advertised") && !plan.contains("/api/"),
        "disconnected cluster mode must not advertise a Slack callback stub: {plan}"
    );
}

#[test]
fn cluster_eval_uses_ephemeral_defaults() {
    let plan = cluster_eval_dry_run(&[]);
    assert!(
        plan.contains("kubectl -n curie port-forward svc/curie-valkey 0:6379"),
        "an omitted Valkey local port must request zero: {plan}"
    );
    assert!(
        !plan.contains("56381:6379"),
        "cluster eval must not select the historical fixed Valkey port: {plan}"
    );
    assert!(
        plan.contains("kubectl -n curie port-forward svc/curie-api 0:8000"),
        "an omitted API local port must request zero even when --channel is set: {plan}"
    );
    assert!(
        plan.contains("cluster-message-replies"),
        "cluster eval must name the relay route: {plan}"
    );
    assert!(
        plan.contains("poll replies at http://127.0.0.1:0/cluster-message-replies/<uuid-v4>"),
        "the ephemeral API port must shape the loopback relay poll URL: {plan}"
    );
    assert!(
        plan.contains("no reply endpoint"),
        "the queued relay handle must advertise no callback endpoint: {plan}"
    );
    assert!(
        !plan.contains("stub advertised"),
        "cluster eval must not advertise a Slack callback stub: {plan}"
    );
}

#[test]
fn cluster_eval_preserves_explicit_port_overrides() {
    let plan = cluster_eval_dry_run(&[
        "--listen-port",
        "18155",
        "--valkey-local-port",
        "18156",
        "--api-local-port",
        "18157",
    ]);
    assert!(
        plan.contains("kubectl -n curie port-forward svc/curie-valkey 18156:6379"),
        "the explicit Valkey local port must remain exact: {plan}"
    );
    assert!(
        plan.contains("kubectl -n curie port-forward svc/curie-api 18157:8000"),
        "the explicit API local port must remain exact: {plan}"
    );
    assert!(
        plan.contains("poll replies at http://127.0.0.1:18157/cluster-message-replies/<uuid-v4>"),
        "the explicit API port must shape the loopback relay poll URL: {plan}"
    );
    assert!(
        !plan.contains("stub advertised"),
        "--listen-host and --listen-port must not bring the Slack stub back: {plan}"
    );
}
