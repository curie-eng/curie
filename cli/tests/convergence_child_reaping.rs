//! Linux subreapers keep orphaned children observable until after the CLI assertions.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

use serde_json::Value;

const SUPERVISOR: &str = r#"
import ctypes, json, os, signal, subprocess, sys, time
from pathlib import Path

libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(36, 1, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), 'PR_SET_CHILD_SUBREAPER')
root = Path(os.environ['CONVERGENCE_DRIVER_ROOT'])
started = time.monotonic()
child = subprocess.Popen(sys.argv[1:], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         text=True, start_new_session=True)
try:
    stdout, stderr = child.communicate(timeout=315)
    elapsed = time.monotonic() - started
    pid_file = root / 'hung-pid'
    absent = True
    proc_state = None
    if pid_file.exists():
        proc = Path('/proc') / pid_file.read_text().strip()
        deadline = time.monotonic() + 0.5
        while proc.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        absent = not proc.exists()
        if not absent:
            proc_state = (proc / 'stat').read_text().split(') ', 1)[1].split()[:2]
    print(json.dumps(dict(code=child.returncode, stdout=stdout, stderr=stderr,
                          elapsed=elapsed, absent=absent, state=proc_state,
                          hung=pid_file.exists())))
finally:
    try:
        os.killpg(child.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    child.wait()
    while True:
        try:
            os.waitpid(-1, 0)
        except ChildProcessError:
            break
"#;

const WRAPPER: &str = r#"
import contextlib, io, json, os, sys, time
from pathlib import Path

root = Path(os.environ['CONVERGENCE_DRIVER_ROOT'])
sys.argv = sys.argv[1:]
tool, args = Path(sys.argv[0]).name, sys.argv[1:]
mode = os.environ['REAPING_MODE']
upgrade = os.environ['REAPING_VERB'] == 'upgrade'
status = tool == 'helm' and args[:1] == ['status'] and 'json' in args
manifest = tool == 'helm' and args[:2] == ['get', 'manifest']
workloads = tool == 'kubectl' and 'deployments,statefulsets,daemonsets,pods,jobs' in args
capture = status or manifest or workloads
if mode == 'spawn' and status:
    os.unlink(sys.argv[0])
if mode == 'nonzero' and status:
    sys.stdout.write('PRIVATE_MESSAGE_SENTINEL' * 10000)
    sys.stderr.write('PRIVATE_MESSAGE_SENTINEL' * 10000)
    sys.exit(1)
if mode == 'success' and capture:
    sys.stderr.write('PRIVATE_MESSAGE_SENTINEL' * 10000)
    sys.stderr.flush()
if mode == 'inner' and status:
    (root / 'hung-pid').write_text(str(os.getpid()))
    time.sleep(120)
if mode == 'outer' and capture:
    (root / 'hung-pid').write_text(str(os.getpid()))
    time.sleep(8)
if mode == 'aggregate' and capture:
    clock = root / 'clock'
    if not clock.exists():
        clock.write_text(str(time.monotonic()))
    if time.monotonic() - float(clock.read_text()) > 294:
        (root / 'hung-pid').write_text(str(os.getpid()))
        time.sleep(120)
    time.sleep(0.2)
source = (root / ('upgrade-driver.py' if upgrade else 'convergence-driver.py')).read_text()
if mode == 'aggregate' and upgrade and workloads:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        try:
            exec(compile(source, 'recorded-driver', 'exec'))
        except SystemExit as error:
            if error.code:
                raise
    value = json.loads(buffer.getvalue())
    for item in value['items']:
        if item['kind'] == 'Deployment':
            item['status']['updatedReplicas'] = 0
    print(json.dumps(value))
else:
    exec(compile(source, 'recorded-driver', 'exec'))
"#;

fn run(verb: &str, mode: &str) -> Value {
    let dir = tempfile::tempdir().unwrap();
    let python = Command::new("python3")
        .args(["-c", "import sys; print(sys.executable)"])
        .output()
        .expect("PATH Python is required");
    assert!(python.status.success());
    std::os::unix::fs::symlink(
        String::from_utf8(python.stdout).unwrap().trim(),
        dir.path().join("python3"),
    )
    .unwrap();
    for name in ["helm", "kubectl"] {
        let path = dir.path().join(name);
        fs::write(
            &path,
            "#!/bin/sh\nexec python3 \"$CONVERGENCE_DRIVER_ROOT/wrapper.py\" \"$0\" \"$@\"\n",
        )
        .unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }
    for (name, data) in [
        ("wrapper.py", WRAPPER),
        (
            "convergence-driver.py",
            include_str!("data/convergence-driver.py"),
        ),
        ("upgrade-driver.py", include_str!("data/upgrade-driver.py")),
        (
            "mixed-rollout.json",
            include_str!("data/mixed-rollout.json"),
        ),
        (
            "retained.json",
            r#"{"connectorCaller":{"existingSecret":"acme-caller-pair"}}"#,
        ),
    ] {
        fs::write(dir.path().join(name), data).unwrap();
    }
    let mut command = Command::new("python3");
    command
        .args([
            "-c",
            SUPERVISOR,
            env!("CARGO_BIN_EXE_curie"),
            "--json",
            "cluster",
            verb,
        ])
        .args([
            "--namespace",
            if verb == "upgrade" {
                "ns"
            } else {
                "convergence-test"
            },
            "--release",
            if verb == "upgrade" { "rel" } else { "acme-bot" },
        ]);
    if verb == "up" {
        command.args([
            "--chart",
            "charts/curie",
            "--dev",
            "--fake-model",
            "--set",
            "agentSandbox.controller.deploy=false",
            "--set",
            "priorityClasses.platform.create=false",
            "--set",
            "priorityClasses.sandbox.create=false",
        ]);
    } else if verb == "upgrade" {
        command.args(["--to", "0.9.0", "--chart", "charts/curie", "--yes"]);
    }
    let output = command
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/.."))
        .env("PATH", dir.path())
        .env("KUBECONFIG", dir.path().join("kubeconfig"))
        .env("CONVERGENCE_DRIVER_ROOT", dir.path())
        .env(
            "CONVERGENCE_DRIVER_SCENARIO",
            if mode == "aggregate" {
                "updated-short"
            } else {
                "healthy"
            },
        )
        .env("UPGRADE_DRIVER_ROOT", dir.path())
        .env("UPGRADE_DRIVER_SCENARIO", "healthy")
        .env("REAPING_VERB", verb)
        .env("REAPING_MODE", mode)
        .output()
        .expect("subprocess subreaper with PATH Python");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["absent"], true, "child survived CLI exit: {result}");
    assert!(result["elapsed"].as_f64().unwrap() < 315.0, "{result}");
    assert!(!result["stdout"]
        .as_str()
        .unwrap()
        .contains("PRIVATE_MESSAGE_SENTINEL"));
    assert!(!result["stderr"]
        .as_str()
        .unwrap()
        .contains("PRIVATE_MESSAGE_SENTINEL"));
    result
}

fn failure(result: &Value, message: &str) -> Value {
    assert_eq!(result["code"], 1, "{result}");
    let output: Value = serde_json::from_str(result["stdout"].as_str().unwrap()).unwrap();
    assert!(output.to_string().contains(message), "{output}");
    output
}

#[test]
fn inner_timeout_reaps_child() {
    let result = run("up", "inner");
    assert_eq!(result["hung"], true);
    failure(
        &result,
        "read Helm release state timed out after 10 seconds",
    );
    assert!(result["elapsed"].as_f64().unwrap() < 15.0);
}

#[test]
fn observation_timeout_reaps_child() {
    let result = run("up", "outer");
    assert_eq!(result["hung"], true);
    failure(
        &result,
        "convergence observation timed out after 30 seconds",
    );
    assert!(result["elapsed"].as_f64().unwrap() < 35.0);
}

#[test]
fn aggregate_up_deadline_reaps_child() {
    let result = run("up", "aggregate");
    assert_eq!(result["hung"], true);
    let output = failure(&result, "post-Helm convergence timed out after 300 seconds");
    assert!(output["error"]
        .as_str()
        .unwrap()
        .contains("last observed rollout reasons"));
    assert!(output["fix"]
        .as_str()
        .unwrap()
        .contains("curie cluster status"));
    assert!(result["elapsed"].as_f64().unwrap() >= 300.0);
}

#[test]
fn aggregate_upgrade_deadline_reaps_child() {
    let result = run("upgrade", "aggregate");
    assert_eq!(result["hung"], true);
    let output = failure(&result, "post-Helm convergence timed out after 300 seconds");
    assert_eq!(output["status"], "failed");
    assert_eq!(output["phase"], "converge");
    assert!(output["fail_forward"].is_object());
    for facet in [
        "exact",
        "images",
        "generations",
        "replicas",
        "unavailable_zero",
        "hooks_healthy",
        "queues_drained",
        "manifest_matches",
    ] {
        assert_eq!(output["convergence"][facet], false, "{facet}: {output}");
    }
    assert!(result["elapsed"].as_f64().unwrap() >= 300.0);
}

#[test]
fn successful_commands_drain_both_pipes() {
    let result = run("status", "success");
    assert_eq!(result["code"], 0, "{result}");
    let output: Value = serde_json::from_str(result["stdout"].as_str().unwrap()).unwrap();
    assert_eq!(output["healthy"], true);
}

#[test]
fn nonzero_exit_retains_safe_access_failure() {
    failure(
        &run("up", "nonzero"),
        "inspect Helm/Kubernetes access and retry",
    );
}

#[test]
fn spawn_failure_retains_path_guidance() {
    failure(&run("up", "spawn"), "is it on PATH?");
}
