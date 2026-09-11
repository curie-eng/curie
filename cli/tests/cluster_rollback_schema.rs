//! Schema compatibility gate for `curie cluster rollback` (#2296).
//!
//! The v0.8.5 -> v0.8.4 incident: Helm marked 0.8.4 `superseded` (status-safe
//! under #1899) while the live database sat at Alembic revision 0039, which
//! 0.8.4's migrate init container does not know. This file pins the additional
//! pre-mutation gate. Status filtering stays in `cluster_rollback.rs`.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use curie::exit::classify;
use curie::ops::{
    parse_helm_history, rollback, select_rollback_revision, ClusterRollbackOutput, CommonOpts,
    HelmRevision, RollbackOpts,
};
use curie::schema_window::{live_in_window, window_for};

/// Release v0.8.7 carries the same Alembic head as v0.8.6. Pin both the
/// accepted live head and the fail-closed boundary for an unknown successor.
#[test]
fn v087_accepts_0039_and_refuses_an_unknown_newer_revision() {
    let window = window_for("0.8.7").expect("0.8.7 is catalogued");
    assert_eq!(window.schema_min, "0001");
    assert_eq!(window.schema_head, "0039");
    assert!(live_in_window("0039", &window));
    assert!(!live_in_window("0040", &window));
}

/// Release v0.8.8 carries the same Alembic head as v0.8.7. Pin both the
/// accepted live head and the fail-closed boundary for an unknown successor.
#[test]
fn v088_accepts_0039_and_refuses_an_unknown_newer_revision() {
    let window = window_for("0.8.8").expect("0.8.8 is catalogued");
    assert_eq!(window.schema_min, "0001");
    assert_eq!(window.schema_head, "0039");
    assert!(live_in_window("0039", &window));
    assert!(!live_in_window("0040", &window));
}

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write fake executable");
    let mut perms = fs::metadata(&path).expect("stat fake").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod fake executable");
}

fn rollback_opts() -> RollbackOpts {
    RollbackOpts {
        common: CommonOpts {
            namespace: "agent-ns".into(),
            release: "prod-release".into(),
            dry_run: false,
        },
        revision: None,
        allow_failed_revision: false,
        yes: true,
        disable_schema_gate: false,
        live_schema_revision: None,
    }
}

/// Issue #2296 history: a failed Helm revision sits between 0.8.4 and 0.8.5,
/// so the status filter still has something to skip, and 0.8.4 is the status-
/// eligible target.
fn issue_2296_history_json() -> &'static str {
    r#"[
      {"revision":1,"status":"superseded","chart":"curie-0.8.4","app_version":"0.8.4","description":"Upgrade complete"},
      {"revision":2,"status":"failed","chart":"curie-0.8.5","app_version":"0.8.5","description":"RuntimeClass \"gvisor\" not found"},
      {"revision":3,"status":"deployed","chart":"curie-0.8.5","app_version":"0.8.5","description":"Upgrade complete"}
    ]"#
}

fn revision(revision: u32, status: &str, chart: &str) -> HelmRevision {
    HelmRevision {
        revision,
        status: status.to_string(),
        chart: chart.to_string(),
        app_version: String::new(),
        description: "Upgrade complete".to_string(),
    }
}

/// THE REGRESSION TEST for the status half of #2296: schema compatibility is
/// an additional gate, not a replacement. On the incident history the #1899
/// selector still lands on Helm revision 1 (0.8.4) and reports that it skipped
/// the failed 2.
#[test]
fn status_filter_still_selects_the_superseded_v084_revision() {
    let history = parse_helm_history(issue_2296_history_json()).expect("history parses");
    let choice = select_rollback_revision(&history)
        .expect("selectable")
        .require_eligible()
        .expect("0.8.4 is deployed/superseded");
    assert_eq!(choice.to_revision, 1, "status-safe target is 0.8.4");
    assert_eq!(choice.from_revision, 3);
    assert_eq!(
        choice.skipped,
        vec![2],
        "failed Helm revision 2 is still skipped"
    );
    assert!(!choice.forced);
}

/// Same selector with no schema gate would still pick 0.8.4 when a failed
/// revision is the one bare helm would target. Pins that we did not fold
/// schema checks into eligibility status.
#[test]
fn a_failed_helm_revision_is_still_not_a_schema_question() {
    let history = vec![
        revision(1, "superseded", "curie-0.8.4"),
        revision(2, "failed", "curie-0.8.5"),
        revision(3, "deployed", "curie-0.8.5"),
    ];
    let choice = select_rollback_revision(&history)
        .expect("selectable")
        .require_eligible()
        .expect("eligible");
    assert_eq!(choice.to_revision, 1);
    assert_eq!(choice.skipped, vec![2]);
}

/// THE REGRESSION TEST for #2296 plus the negative control. One PATH-mutating
/// test in this binary so it cannot race itself (same discipline as
/// `cluster_rollback.rs`).
#[tokio::test]
async fn v085_revision_0039_to_v084_is_refused_before_helm_mutates() {
    let dir = tempfile::tempdir().expect("tempdir");
    let history_json = dir.path().join("history.json");
    fs::write(&history_json, issue_2296_history_json()).expect("write history");
    let rollback_log = dir.path().join("rollback-argv.log");
    let kubectl_log = dir.path().join("kubectl-argv.log");
    std::env::set_var("FAKE_HELM_HISTORY", &history_json);
    std::env::set_var("FAKE_HELM_ROLLBACK_LOG", &rollback_log);
    std::env::set_var("FAKE_KUBECTL_LOG", &kubectl_log);

    write_exec(
        dir.path(),
        "helm",
        "#!/bin/sh\n\
         case \"$1\" in\n\
         history) cat \"$FAKE_HELM_HISTORY\" ;;\n\
         rollback) echo \"$*\" >> \"$FAKE_HELM_ROLLBACK_LOG\"; echo 'Rollback was a success.' ;;\n\
         *) echo \"unexpected helm verb: $1\" >&2; exit 1 ;;\n\
         esac\n",
    );
    // Probe stdout is only the alembic current line. Stderr plants a DSN so a
    // leak in the refusal would fail the redaction assertion below.
    write_exec(
        dir.path(),
        "kubectl",
        "#!/bin/sh\n\
         echo \"$*\" >> \"$FAKE_KUBECTL_LOG\"\n\
         echo 'postgresql://curie:secret-password@postgres:5432/curie' >&2\n\
         echo '0039 (head)'\n",
    );

    let existing = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![dir.path().to_path_buf()];
    paths.extend(std::env::split_paths(&existing));
    std::env::set_var("PATH", std::env::join_paths(paths).expect("join PATH"));

    let err = rollback(rollback_opts())
        .await
        .expect_err("0.8.4 cannot start against live revision 0039");
    let shown = err.to_string();
    assert!(
        shown.contains("0039") && shown.contains("0.8.4"),
        "the refusal must name the live revision and the incompatible target: {shown}"
    );
    assert!(
        shown.contains("0038") || shown.contains("schema"),
        "the refusal must name the compatibility boundary: {shown}"
    );
    let (_class, fix) = classify(&err);
    let fix = fix.unwrap_or_default();
    assert!(
        fix.contains("0.8.5"),
        "the newest safe fail-forward application version is 0.8.5: {fix}"
    );
    assert!(
        !shown.contains("secret-password")
            && !shown.contains("postgresql://")
            && !fix.contains("secret-password"),
        "the refusal must not leak the probe DSN: shown={shown} fix={fix}"
    );
    assert!(
        !rollback_log.exists() || fs::read_to_string(&rollback_log).unwrap().is_empty(),
        "helm rollback must not have been invoked"
    );
    let kubectl = fs::read_to_string(&kubectl_log).unwrap_or_default();
    assert!(
        kubectl.contains("exec"),
        "the gate must have probed the live revision: {kubectl}"
    );
    for verb in ["rollout", "scale", "patch", "delete", "apply", "replace"] {
        assert!(
            !kubectl.split_whitespace().any(|token| token == verb),
            "refused rollback must not mutate workloads ({verb}): {kubectl}"
        );
    }

    // Negative control: removing the schema gate allows the unsafe attempt.
    let mut opts = rollback_opts();
    opts.disable_schema_gate = true;
    let out = rollback(opts)
        .await
        .expect("without the schema gate the status filter admits 0.8.4");
    match out {
        ClusterRollbackOutput::RolledBack {
            to_revision,
            skipped,
            ..
        } => {
            assert_eq!(to_revision, 1, "status-eligible target is v0.8.4");
            assert_eq!(skipped, vec![2]);
        }
        other => panic!("expected a completed rollback, got {other:?}"),
    }
    let logged = fs::read_to_string(&rollback_log).expect("helm rollback ran");
    assert_eq!(
        logged.trim(),
        "rollback prod-release 1 -n agent-ns",
        "removing the schema gate must allow the unsafe helm rollback"
    );

    match std::env::var_os("PATH") {
        Some(_) => std::env::set_var("PATH", existing),
        None => std::env::remove_var("PATH"),
    }
}

fn run_rollback_json(dir: &Path) -> std::process::Output {
    let existing = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(&existing));
    std::process::Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["cluster", "rollback"])
        .args(["--namespace", "agent-ns", "--release", "prod-release"])
        .args(["--yes", "--json"])
        .env("PATH", std::env::join_paths(paths).expect("join PATH"))
        .env("NO_COLOR", "1")
        .output()
        .expect("run curie cluster rollback --json")
}

/// Agent-facing `--json` refusal is nonzero, names the fail-forward version,
/// and stays inside the generic error schema (no DSN).
#[test]
fn json_refusal_is_nonzero_actionable_and_redacted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let history = dir.path().join("history.json");
    fs::write(&history, issue_2296_history_json()).expect("write history");
    write_exec(
        dir.path(),
        "helm",
        &format!(
            "#!/bin/sh\n\
             echo \"$*\" >> '{log}'\n\
             case \"$1\" in\n\
             history) cat '{history}' ;;\n\
             rollback) echo rollback-ran >> '{log}'; echo 'Rollback was a success.' ;;\n\
             *) echo \"unexpected helm verb: $1\" >&2; exit 1 ;;\n\
             esac\n",
            log = dir.path().join("helm-argv.log").display(),
            history = history.display(),
        ),
    );
    write_exec(
        dir.path(),
        "kubectl",
        "#!/bin/sh\necho 'postgresql://curie:secret-password@postgres:5432/curie' >&2\necho '0039 (head)'\n",
    );
    let out = run_rollback_json(dir.path());
    assert!(
        !out.status.success(),
        "incompatible rollback must be nonzero"
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
    assert!(
        !stdout.contains("secret-password") && !stdout.contains("postgresql://"),
        "json refusal leaked a DSN: {stdout}"
    );
    let helm_log = fs::read_to_string(dir.path().join("helm-argv.log")).unwrap_or_default();
    assert!(
        !helm_log.contains("rollback-ran") && !helm_log.contains("rollback prod-release"),
        "json refusal must not invoke helm rollback: {helm_log}"
    );
}
