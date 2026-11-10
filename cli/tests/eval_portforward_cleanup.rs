//! #1908: a red `cluster eval` must reap its kubectl Valkey port-forward
//! before `report_eval` takes the non-unwinding Failure exit.
//!
//! `std::process::exit` skips Drop, so a live `kill_on_drop` child left in
//! scope at that call is orphaned onto PID 1 and keeps its local port. The
//! next eval then selects the leaked port and fails before it can run. This
//! regression drives the real red-eval exit through a fake kubectl subprocess
//! and asserts the child is gone and its port is immediately reusable.

use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use curie::commands::{report_eval, EvalReport};
use curie::evals::CaseOutcome;
use curie::message::{port_forward_command, start_port_forward};

const CHILD_FLAG: &str = "CURIE_TEST_RED_EVAL_REAP";
const META_FLAG: &str = "CURIE_TEST_RED_EVAL_META";
const KUBECTL_FLAG: &str = "CURIE_TEST_RED_EVAL_KUBECTL";

fn write_fake_kubectl(dir: &Path) -> PathBuf {
    let path = dir.join("kubectl");
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
print(f"Forwarding from 127.0.0.1:{assigned} -> {remote}", flush=True)
while True:
    connection, _ = listener.accept()
    connection.close()
"#;
    fs::write(&path, body).expect("write fake kubectl");
    let mut permissions = fs::metadata(&path)
        .expect("read fake kubectl metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make fake kubectl executable");
    path
}

fn wait_until_released(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if let Ok(listener) = TcpListener::bind(("127.0.0.1", port)) {
            drop(listener);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the red-eval Failure exit must release assigned port {port}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn pid_is_alive(pid: u32) -> bool {
    let Ok(status) = std::fs::read_to_string(format!("/proc/{pid}/status")) else {
        return false;
    };
    status.lines().any(|line| {
        line.starts_with("State:") && (line.contains("(running)") || line.contains("(sleeping)"))
    })
}

fn wait_until_dead(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        if !pid_is_alive(pid) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the kubectl child {pid} must be reaped, not orphaned onto PID 1"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

async fn run_red_eval_child() {
    let kubectl = PathBuf::from(std::env::var(KUBECTL_FLAG).expect("child kubectl path"));
    let meta = PathBuf::from(std::env::var(META_FLAG).expect("child meta path"));
    let fullname = curie::ops::chart_fullname("acme-release");
    let mut cmd = port_forward_command("acme-system", &fullname, "valkey", 0, 6379);
    cmd.program = kubectl.to_string_lossy().into_owned();
    let (child, port) = start_port_forward(&cmd, 0, "valkey")
        .await
        .expect("start fake kubectl port-forward");
    let pid = child.id().expect("kubectl child pid");
    fs::write(&meta, format!("pid={pid}\nport={port}\n")).expect("write child meta");
    let report = EvalReport::from_rows(vec![("one".into(), CaseOutcome::Fail, 0.1, "red".into())]);
    // The production red-eval path: emit the completed report, then Failure-exit
    // while this function still owns the kubectl child. Passing the child in is
    // what makes Drop run before process::exit.
    let _ = report_eval(&report, None, child);
    panic!("report_eval must process::exit on a red eval");
}

#[tokio::test]
async fn red_cluster_eval_exit_reaps_kubectl_child_and_frees_its_port() {
    if std::env::var(CHILD_FLAG).as_deref() == Ok("1") {
        run_red_eval_child().await;
        panic!("child must process::exit");
    }

    let dir = tempfile::tempdir().expect("create fake kubectl directory");
    let kubectl = write_fake_kubectl(dir.path());
    let meta = dir.path().join("meta.txt");
    let exe = std::env::current_exe().expect("test executable");
    let output = Command::new(&exe)
        .args([
            "--exact",
            "eval_portforward_cleanup::red_cluster_eval_exit_reaps_kubectl_child_and_frees_its_port",
        ])
        .env(CHILD_FLAG, "1")
        .env(META_FLAG, &meta)
        .env(KUBECTL_FLAG, &kubectl)
        .output()
        .expect("run red-eval child");
    assert_eq!(
        output.status.code(),
        Some(curie::exit::ExitClass::Failure.code()),
        "a red eval must still exit Failure; stdout: {}; stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let meta_text = fs::read_to_string(&meta).expect("child must record pid and port before exit");
    let mut pid = None;
    let mut port = None;
    for line in meta_text.lines() {
        if let Some(value) = line.strip_prefix("pid=") {
            pid = Some(value.parse::<u32>().expect("pid"));
        }
        if let Some(value) = line.strip_prefix("port=") {
            port = Some(value.parse::<u16>().expect("port"));
        }
    }
    let pid = pid.expect("child meta pid");
    let port = port.expect("child meta port");
    assert_ne!(port, 0, "kubectl must report its assigned port");
    wait_until_dead(pid);
    wait_until_released(port);
}

#[test]
fn a_green_eval_report_returns_ok_without_exiting() {
    let report = EvalReport::from_rows(vec![("one".into(), CaseOutcome::Pass, 0.1, "ok".into())]);
    report_eval(&report, None, ()).expect("a green eval must return Ok rather than process::exit");
}
