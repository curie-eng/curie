//! Issue #1031: the `curie local message` connectedness probe, end to end
//! through a real `docker` process.
//!
//! The unit tests in `message.rs` pin the pure predicate. What they cannot see
//! is the wiring around it, which is where both shipped defects lived: the probe
//! inspected a hardcoded `curie-worker`, a name no compose project ever produces
//! (`COMPOSE_PROJECT_NAME` prefixes it), so every `docker inspect` failed and the
//! connected path was unreachable no matter what the predicate said.
//!
//! So this drives the real `curie::message::local_connected_transport()` with a
//! `docker` shim on PATH -- the same fake-executable idiom
//! `local_up_fake_warning.rs` uses -- and asserts on the argv the CLI actually
//! ran. A shim rather than Docker, because the assertion is about which container
//! the CLI asks for and how it reads the answer; a live daemon adds setup without
//! adding signal.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use curie::message::local_connected_transport;
use curie::slack::DEFAULT_API_BASE;

/// A stack whose compose project is NOT the CLI's default `curie`, so a probe
/// that resolves by name rather than by the compose service label finds nothing.
const WORKER_CONTAINER: &str = "acme-staging-curie-worker-1";
const SERVICE_LABEL: &str = "label=com.docker.compose.service=curie-worker";

/// A `docker` shim answering `ps` with `ps_stdout`, answering `inspect` of
/// [`WORKER_CONTAINER`] with `env_stdout`, and failing any other `inspect` the
/// way a real daemon does. Every invocation appends its argv to `argv.log`, so
/// the test asserts what the CLI asked for, not only what it got.
fn docker_shim(dir: &Path, ps_stdout: &str, env_stdout: &str) -> PathBuf {
    fs::create_dir_all(dir).expect("create shim directory");
    fs::write(dir.join("ps.out"), ps_stdout).expect("write ps fixture");
    fs::write(dir.join("env.out"), env_stdout).expect("write env fixture");
    let script = format!(
        r#"#!/bin/sh
d=$(dirname "$0")
echo "$@" >> "$d/argv.log"
case "$1" in
  ps)
    cat "$d/ps.out"
    exit 0
    ;;
  inspect)
    if [ "$2" = "{WORKER_CONTAINER}" ]; then
      cat "$d/env.out"
      exit 0
    fi
    echo "Error response from daemon: No such object: $2" >&2
    exit 1
    ;;
esac
exit 1
"#
    );
    let path = dir.join("docker");
    fs::write(&path, script).expect("write docker shim");
    let mut permissions = fs::metadata(&path).expect("shim metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make shim executable");
    dir.join("argv.log")
}

/// Point PATH at `dir` alone-first, from the process's original PATH each time.
fn use_shim(dir: &Path, original: &Option<std::ffi::OsString>) {
    let mut paths = vec![dir.to_path_buf()];
    if let Some(path) = original {
        paths.extend(std::env::split_paths(path));
    }
    std::env::set_var("PATH", std::env::join_paths(paths).expect("join PATH"));
}

fn argv_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_default()
}

/// Whether any logged `docker inspect` targeted the bare compose service name --
/// the #1031 defect, whatever order the argv happened to be built in.
fn inspected_the_bare_service_name(argv: &str) -> bool {
    argv.lines()
        .filter(|line| line.starts_with("inspect "))
        .any(|line| line.split_whitespace().any(|token| token == "curie-worker"))
}

/// One test function, because PATH is process-global: the scenarios run in
/// sequence, each pointing PATH at its own shim, under `support::ENV_LOCK`.
#[tokio::test]
async fn the_probe_resolves_the_worker_by_label_and_reads_empty_base_as_connected() {
    let temp = tempfile::tempdir().expect("create temporary directory");
    let _env = crate::support::ENV_LOCK.lock().await;
    let original_path = std::env::var_os("PATH");

    // ---- connected: the state `local comms --connect` leaves, on a non-default
    // compose project. The worker holds an EMPTY SLACK_API_BASE_URL (this repo's
    // "talk to real Slack" signal) and a genuine workspace token.
    let connected_dir = temp.path().join("connected");
    let connected_log = docker_shim(
        &connected_dir,
        &format!("{WORKER_CONTAINER}\n"),
        "PATH=/usr/local/bin\nSLACK_API_BASE_URL=\nSLACK_BOT_TOKEN=xoxb-real-workspace\n",
    );
    use_shim(&connected_dir, &original_path);

    let transport = local_connected_transport()
        .await
        .expect("an empty SLACK_API_BASE_URL plus a real token is a CONNECTED worker");
    assert_eq!(transport.bot_token, "xoxb-real-workspace");
    assert_eq!(
        transport.api_base, DEFAULT_API_BASE,
        "an empty base means no override, which for the worker is real Slack"
    );

    let argv = argv_log(&connected_log);
    assert!(
        argv.contains(SERVICE_LABEL),
        "the worker must be resolved through the compose service label: {argv}"
    );
    assert!(
        argv.contains(&format!("inspect {WORKER_CONTAINER}")),
        "the probe must inspect the container the selector resolved: {argv}"
    );
    assert!(
        !inspected_the_bare_service_name(&argv),
        "the probe must never inspect the bare service name -- no compose project \
         produces that container, which is how #1031 shipped: {argv}"
    );

    // ---- not connected: the same stack after `local comms --disconnect`.
    let stub_dir = temp.path().join("stub");
    let stub_log = docker_shim(
        &stub_dir,
        &format!("{WORKER_CONTAINER}\n"),
        "SLACK_API_BASE_URL=http://localhost:8155/api/\nSLACK_BOT_TOKEN=xoxb-real-workspace\n",
    );
    use_shim(&stub_dir, &original_path);
    assert!(
        local_connected_transport().await.is_none(),
        "a stub-wired worker is not connected however real its token looks"
    );
    assert!(
        argv_log(&stub_log).contains(SERVICE_LABEL),
        "the disconnected read goes through the same selector"
    );

    // ---- the probe could not run: nothing matches the selector. Not connected,
    // and it must RETURN rather than hang or panic.
    let empty_dir = temp.path().join("empty");
    let empty_log = docker_shim(&empty_dir, "", "");
    use_shim(&empty_dir, &original_path);
    assert!(
        local_connected_transport().await.is_none(),
        "no resolvable worker means the stub path, never a real post"
    );
    assert!(
        !argv_log(&empty_log).contains("inspect"),
        "with nothing resolved there is no container to inspect"
    );

    if let Some(path) = original_path {
        std::env::set_var("PATH", path);
    }
}
