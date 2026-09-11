//! Operator override for `curie cluster rollback` when the API pod cannot be
//! probed (#2558).
//!
//! The schema-window check stays; only the live-revision *source* is
//! overridable. This file talks to the clap binary only so the selected pin
//! still compiles against the parent `RollbackOpts` (no new struct field).

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write fake executable");
    let mut perms = fs::metadata(&path).expect("stat fake").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod fake executable");
}

fn run_rollback_json_args(dir: &Path, extra: &[&str]) -> std::process::Output {
    let existing = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(&existing));
    std::process::Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["cluster", "rollback"])
        .args(["--namespace", "agent-ns", "--release", "prod-release"])
        .args(["--yes", "--json"])
        .args(extra)
        .env("PATH", std::env::join_paths(paths).expect("join PATH"))
        .env("NO_COLOR", "1")
        .output()
        .expect("run curie cluster rollback --json")
}

fn write_fake_helm(dir: &Path, history_json: &str) {
    let history = dir.join("history.json");
    fs::write(&history, history_json).expect("write history");
    write_exec(
        dir,
        "helm",
        &format!(
            "#!/bin/sh\n\
             echo \"$*\" >> '{log}'\n\
             case \"$1\" in\n\
             history) cat '{history}' ;;\n\
             rollback) echo rollback-ran >> '{log}'; echo 'Rollback was a success.' ;;\n\
             *) echo \"unexpected helm verb: $1\" >&2; exit 1 ;;\n\
             esac\n",
            log = dir.join("helm-argv.log").display(),
            history = history.display(),
        ),
    );
}

fn write_failing_kubectl(dir: &Path) {
    write_exec(
        dir,
        "kubectl",
        &format!(
            "#!/bin/sh\n\
             echo \"$*\" >> '{log}'\n\
             echo 'postgresql://curie:secret-password@postgres:5432/curie' >&2\n\
             echo 'Error from server (BadRequest): container is not running' >&2\n\
             exit 1\n",
            log = dir.join("kubectl-argv.log").display(),
        ),
    );
}

fn issue_2296_history_json() -> &'static str {
    r#"[
      {"revision":1,"status":"superseded","chart":"curie-0.8.4","app_version":"0.8.4","description":"Upgrade complete"},
      {"revision":2,"status":"failed","chart":"curie-0.8.5","app_version":"0.8.5","description":"RuntimeClass \"gvisor\" not found"},
      {"revision":3,"status":"deployed","chart":"curie-0.8.5","app_version":"0.8.5","description":"Upgrade complete"}
    ]"#
}

fn compatible_085_086_history_json() -> &'static str {
    r#"[
      {"revision":1,"status":"superseded","chart":"curie-0.8.5","app_version":"0.8.5","description":"Upgrade complete"},
      {"revision":2,"status":"deployed","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"}
    ]"#
}

/// THE REGRESSION TEST for #2558: an unreadable API pod is the ordinary
/// reason to roll back. The refusal must name the override that supplies the
/// live revision without restoring the API first, stay redacted, and leave
/// Helm unmutated.
#[test]
fn unreadable_api_pod_names_the_live_schema_override() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fake_helm(dir.path(), issue_2296_history_json());
    write_failing_kubectl(dir.path());
    let out = run_rollback_json_args(dir.path(), &[]);
    assert!(!out.status.success(), "unreadable API pod must be nonzero");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let payload: serde_json::Value = serde_json::from_str(stdout.trim()).expect("error json");
    let error = payload["error"].as_str().unwrap_or_default();
    let fix = payload["fix"].as_str().unwrap_or_default();
    assert!(
        error.contains("could not read the live database revision"),
        "{payload}"
    );
    assert!(
        fix.contains("--live-schema-revision"),
        "the refusal must name the override instead of requiring a healthy API: {payload}"
    );
    assert!(
        !fix.contains("cluster status"),
        "the circular 'make the API healthy first' remedy must not be the fix: {payload}"
    );
    assert!(
        !stdout.contains("secret-password") && !stdout.contains("postgresql://"),
        "probe-failure refusal leaked a DSN: {stdout}"
    );
    let helm_log = fs::read_to_string(dir.path().join("helm-argv.log")).unwrap_or_default();
    assert!(
        !helm_log.contains("rollback-ran") && !helm_log.contains("rollback prod-release"),
        "probe failure must not invoke helm rollback: {helm_log}"
    );
}

/// Same unreadable pod, but stdout is not a revision. The second probe-failure
/// arm must name the same override.
#[test]
fn unparseable_api_pod_output_names_the_override() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fake_helm(dir.path(), issue_2296_history_json());
    write_exec(
        dir.path(),
        "kubectl",
        "#!/bin/sh\necho 'INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.'\n",
    );
    let out = run_rollback_json_args(dir.path(), &[]);
    assert!(
        !out.status.success(),
        "unparseable alembic current is nonzero"
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let payload: serde_json::Value = serde_json::from_str(stdout.trim()).expect("error json");
    let fix = payload["fix"].as_str().unwrap_or_default();
    assert!(
        fix.contains("--live-schema-revision"),
        "unparseable probe must name the override: {payload}"
    );
    let helm_log = fs::read_to_string(dir.path().join("helm-argv.log")).unwrap_or_default();
    assert!(
        !helm_log.contains("rollback-ran") && !helm_log.contains("rollback prod-release"),
        "unparseable probe must not invoke helm rollback: {helm_log}"
    );
}

/// #2558 plus #2296: asserting the live revision does not widen the gate. The
/// status-eligible v0.8.4 target is still refused for live 0039, and the
/// failing kubectl is not consulted.
#[test]
fn asserted_live_revision_still_refuses_incompatible_target_without_probing() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fake_helm(dir.path(), issue_2296_history_json());
    write_failing_kubectl(dir.path());
    let out = run_rollback_json_args(dir.path(), &["--live-schema-revision", "0039"]);
    assert!(
        !out.status.success(),
        "asserting 0039 must still refuse v0.8.4"
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let payload: serde_json::Value = serde_json::from_str(stdout.trim()).expect("error json");
    let error = payload["error"].as_str().unwrap_or_default();
    let fix = payload["fix"].as_str().unwrap_or_default();
    assert!(
        error.contains("0039") && error.contains("0.8.4"),
        "{payload}"
    );
    assert!(fix.contains("0.8.5"), "{payload}");
    let helm_log = fs::read_to_string(dir.path().join("helm-argv.log")).unwrap_or_default();
    assert!(
        !helm_log.contains("rollback-ran") && !helm_log.contains("rollback prod-release"),
        "incompatible asserted revision must not invoke helm rollback: {helm_log}"
    );
    let kubectl_log = fs::read_to_string(dir.path().join("kubectl-argv.log")).unwrap_or_default();
    assert!(
        kubectl_log.trim().is_empty(),
        "asserted live revision must not exec the API pod: {kubectl_log}"
    );
}

/// The recovery path #2558 exists for: every API replica is unexecutable, the
/// operator asserts the live revision, and the status-eligible target is
/// inside the declared window.
#[test]
fn asserted_live_revision_allows_compatible_target_when_api_pod_is_down() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fake_helm(dir.path(), compatible_085_086_history_json());
    write_failing_kubectl(dir.path());
    let out = run_rollback_json_args(dir.path(), &["--live-schema-revision", "0039"]);
    assert!(
        out.status.success(),
        "compatible asserted revision must succeed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let payload: serde_json::Value = serde_json::from_str(stdout.trim()).expect("success json");
    assert_eq!(payload["rolled_back"], true, "{payload}");
    assert_eq!(payload["to_revision"], 1, "{payload}");
    assert_eq!(payload["from_revision"], 2, "{payload}");
    let helm_log = fs::read_to_string(dir.path().join("helm-argv.log")).expect("helm ran");
    assert!(
        helm_log.contains("rollback prod-release 1 -n agent-ns"),
        "compatible override must invoke helm rollback: {helm_log}"
    );
    let kubectl_log = fs::read_to_string(dir.path().join("kubectl-argv.log")).unwrap_or_default();
    assert!(
        kubectl_log.trim().is_empty(),
        "asserted live revision must not exec the API pod: {kubectl_log}"
    );
}

/// An asserted value that is not an Alembic revision is refused before Helm
/// mutates, through the same consumer path as a probe failure.
#[test]
fn invalid_live_schema_revision_is_refused_before_helm_mutates() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fake_helm(dir.path(), compatible_085_086_history_json());
    write_failing_kubectl(dir.path());
    let out = run_rollback_json_args(dir.path(), &["--live-schema-revision", "not-a-revision"]);
    assert!(
        !out.status.success(),
        "garbage asserted revision must be nonzero"
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let payload: serde_json::Value = serde_json::from_str(stdout.trim()).expect("error json");
    let error = payload["error"].as_str().unwrap_or_default();
    let fix = payload["fix"].as_str().unwrap_or_default();
    assert!(error.contains("--live-schema-revision"), "{payload}");
    assert!(
        fix.contains("0039") || fix.contains("revision"),
        "{payload}"
    );
    let helm_log = fs::read_to_string(dir.path().join("helm-argv.log")).unwrap_or_default();
    assert!(
        !helm_log.contains("rollback-ran") && !helm_log.contains("rollback prod-release"),
        "invalid asserted revision must not invoke helm rollback: {helm_log}"
    );
}

/// Sibling path: the test-only negative control stays unreachable from clap.
#[test]
fn disable_schema_gate_is_not_a_clap_flag() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_fake_helm(dir.path(), issue_2296_history_json());
    write_failing_kubectl(dir.path());
    let out = run_rollback_json_args(dir.path(), &["--disable-schema-gate"]);
    assert!(!out.status.success(), "test-only flag must not be accepted");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unexpected argument") || stderr.contains("unrecognized"),
        "clap must reject --disable-schema-gate: {stderr}"
    );
    let helm_log = fs::read_to_string(dir.path().join("helm-argv.log")).unwrap_or_default();
    assert!(
        !helm_log.contains("rollback-ran") && !helm_log.contains("rollback prod-release"),
        "rejected clap flag must not invoke helm rollback: {helm_log}"
    );
}
