//! Schema compatibility gate for `curie cluster rollback` (#2296).
//!
//! The v0.8.5 -> v0.8.4 incident: Helm marked 0.8.4 `superseded` (status-safe
//! under #1899) while the live database sat at Alembic revision 0039, which
//! 0.8.4's migrate init container does not know. This file pins the additional
//! pre-mutation gate. Status filtering stays in `cluster_rollback.rs`.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Output;

use curie::exit::classify;
use curie::ops::{
    parse_helm_history, rollback, select_rollback_revision, ClusterRollbackOutput, CommonOpts,
    HelmRevision, RollbackOpts,
};
use curie::schema_compat::{pending_revisions, plan_upgrade, TargetMetadata};
use curie::schema_window::{live_in_window, newest_fail_forward, window_for};

fn catalog_marks_artifact_identity_ambiguous(version: &str) -> bool {
    let catalog: serde_json::Value =
        serde_json::from_str(include_str!("../src/application_schema_windows.json"))
            .expect("application schema catalog parses");
    catalog["windows"][version]["artifact_identity_ambiguous"]
        .as_bool()
        .unwrap_or(false)
}

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

/// Published v0.8.8 carries Alembic head 0039. Historical next candidates used
/// the same application version, so the catalog must require artifact identity.
#[test]
fn v088_published_window_ends_at_0039_and_requires_artifact_identity() {
    let window = window_for("0.8.8").expect("0.8.8 is catalogued");
    assert_eq!(window.schema_min, "0001");
    assert_eq!(window.schema_head, "0039");
    assert!(live_in_window("0039", &window));
    assert!(!live_in_window("0044", &window));
    assert!(catalog_marks_artifact_identity_ambiguous("0.8.8"));
}

/// Published v0.8.9 also stops at 0039. A historical candidate can extend that
/// window only when its retained manifest establishes the different artifact.
#[test]
fn v089_published_window_ends_at_0039_and_requires_artifact_identity() {
    let window = window_for("0.8.9").expect("0.8.9 is catalogued");
    assert_eq!(window.schema_min, "0001");
    assert_eq!(window.schema_head, "0039");
    assert!(live_in_window("0039", &window));
    assert!(!live_in_window("0044", &window));
    assert!(catalog_marks_artifact_identity_ambiguous("0.8.9"));
}

/// The released 0.9.0 and 0.9.1 artifacts stop at Alembic head 0044. Pin the
/// accepted live head and the boundary before the later 0.9.2 migration.
#[test]
fn v090_and_v091_accept_0044_and_refuse_0045() {
    for version in ["0.9.0", "0.9.1"] {
        let window = window_for(version).unwrap_or_else(|| panic!("{version} is catalogued"));
        assert_eq!(window.schema_min, "0001", "{version}");
        assert_eq!(window.schema_head, "0044", "{version}");
        assert!(live_in_window("0044", &window), "{version}");
        assert!(live_in_window("0039", &window), "{version}");
        assert!(!live_in_window("0045", &window), "{version}");
        assert!(
            !catalog_marks_artifact_identity_ambiguous(version),
            "{version} has one unambiguous released artifact identity"
        );
    }
}

/// Released v0.9.2 is a single revision window at 0045. The feature train
/// chart continues past that window, so this pin is the catalog, not the
/// packaged 0.10.0 graph.
#[test]
fn v092_accepts_0045_and_refuses_outside_its_single_revision_window() {
    let window = window_for("0.9.2").expect("0.9.2 is catalogued");
    assert_eq!(window.schema_min, "0045");
    assert_eq!(window.schema_head, "0045");
    assert!(!live_in_window("0044", &window));
    assert!(live_in_window("0045", &window));
    assert!(!live_in_window("0046", &window));
    assert!(
        !catalog_marks_artifact_identity_ambiguous("0.9.2"),
        "0.9.2 has one unambiguous released artifact identity"
    );
}

#[test]
fn v0100_release_candidate_has_an_exact_catalog_window() {
    let catalog: serde_json::Value =
        serde_json::from_str(include_str!("../src/application_schema_windows.json"))
            .expect("application schema catalog parses");
    assert!(catalog["windows"].get("0.10.0-rc.1").is_some());

    let window = window_for("0.10.0-rc.1").expect("release candidate is catalogued");
    assert_eq!(window.schema_min, "0045");
    assert_eq!(window.schema_head, "0057");
    assert!(live_in_window("0045", &window));
    assert!(live_in_window("0056", &window));
    assert!(live_in_window("0057", &window));
    assert!(!live_in_window("0044", &window));
    assert_eq!(
        window_for("v0.10.0-rc.1")
            .expect("prefixed release candidate is catalogued")
            .schema_head,
        window.schema_head
    );
}

#[test]
fn stable_v0100_sorts_after_its_release_candidate_for_fail_forward() {
    assert_eq!(
        newest_fail_forward(["0.10.0-rc.1"], "0057").as_deref(),
        Some("0.10.0-rc.1")
    );
    assert_eq!(
        newest_fail_forward(["0.10.0-rc.1", "0.10.0"], "0057").as_deref(),
        Some("0.10.0")
    );
}

/// Released 0.9.1 reports catalog head 0044. This tree's packaged chart keeps
/// the 0045 floor and continues through feature train head 0057, so the
/// pending live migrations are 0045 through 0057 and the upgrade applies.
#[test]
fn v091_source_upgrades_through_the_packaged_chart_graph() {
    let source = window_for("0.9.1").expect("0.9.1 is catalogued");
    let target: TargetMetadata =
        serde_json::from_str(include_str!("../../charts/curie/files/schema-compat.json"))
            .expect("packaged chart schema compatibility metadata parses");

    assert_eq!(source.schema_head, "0044");
    assert_eq!(target.schema_min, "0045");
    assert_eq!(target.schema_head, "0057");

    let pending =
        pending_revisions(Some("0044"), &target).expect("0044 reaches the packaged chart head");
    let revisions: Vec<&str> = pending.iter().map(|step| step.revision.as_str()).collect();
    assert_eq!(
        revisions,
        [
            "0045", "0046", "0047", "0048", "0049", "0050", "0051", "0052", "0053", "0054", "0055",
            "0056", "0057"
        ]
    );
    assert!(pending.iter().all(|step| step.kind == "expand"));

    let decision = plan_upgrade(
        Some("0044"),
        &target,
        &pending,
        false,
        Some(&source.schema_head),
    );
    assert_eq!(decision.action, "apply");
    assert_eq!(decision.source_head.as_deref(), Some("0044"));
    assert_eq!(decision.target_min, "0045");
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

fn two_revision_history(target: &str, current: &str) -> String {
    format!(
        r#"[
          {{"revision":1,"status":"superseded","chart":"curie-{target}","app_version":"{target}","description":"Upgrade complete"}},
          {{"revision":2,"status":"deployed","chart":"curie-{current}","app_version":"{current}","description":"Upgrade complete"}}
        ]"#
    )
}

fn compatibility_manifest(name: &str, app: &str, schema_min: &str, schema_head: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  labels:
    app.kubernetes.io/component: schema-compat
data:
  application-version: "{app}"
  compatibility.json: |
    {{"schema_min":"{schema_min}","schema_head":"{schema_head}","revisions":[]}}
"#
    )
}

struct RollbackFixture {
    dir: tempfile::TempDir,
}

impl RollbackFixture {
    fn new(history: &str, live: &str, manifest: Result<&str, &str>) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let history_path = dir.path().join("history.json");
        let manifest_path = dir.path().join("manifest.yaml");
        let manifest_error_path = dir.path().join("manifest.err");
        let helm_log = dir.path().join("helm-argv.log");
        let kubectl_log = dir.path().join("kubectl-argv.log");
        fs::write(&history_path, history).expect("write history");
        let manifest_action = match manifest {
            Ok(rendered) => {
                fs::write(&manifest_path, rendered).expect("write manifest");
                format!("cat '{}'", manifest_path.display())
            }
            Err(error) => {
                fs::write(&manifest_error_path, error).expect("write manifest error");
                format!("cat '{}' >&2; exit 1", manifest_error_path.display())
            }
        };
        write_exec(
            dir.path(),
            "helm",
            &format!(
                "#!/bin/sh\n\
                 echo \"$*\" >> '{helm_log}'\n\
                 case \"$1:$2\" in\n\
                 history:*) cat '{history}' ;;\n\
                 get:manifest) {manifest_action} ;;\n\
                 rollback:*) echo 'Rollback was a success.' ;;\n\
                 *) echo \"unexpected helm invocation: $*\" >&2; exit 1 ;;\n\
                 esac\n",
                helm_log = helm_log.display(),
                history = history_path.display(),
            ),
        );
        write_exec(
            dir.path(),
            "kubectl",
            &format!(
                "#!/bin/sh\n\
                 echo \"$*\" >> '{kubectl_log}'\n\
                 echo '{live} (head)'\n",
                kubectl_log = kubectl_log.display(),
            ),
        );
        Self { dir }
    }

    fn run(&self, extra: &[&str]) -> Output {
        let existing = std::env::var_os("PATH").unwrap_or_default();
        let mut paths = vec![self.dir.path().to_path_buf()];
        paths.extend(std::env::split_paths(&existing));
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_curie"));
        cmd.args(["cluster", "rollback"])
            .args(["--namespace", "agent-ns", "--release", "prod-release"])
            .args(["--yes", "--json"])
            .args(extra)
            .env("PATH", std::env::join_paths(paths).expect("join PATH"))
            .env("NO_COLOR", "1");
        cmd.output().expect("run curie cluster rollback")
    }

    fn helm_log(&self) -> String {
        fs::read_to_string(self.dir.path().join("helm-argv.log")).unwrap_or_default()
    }

    fn kubectl_log(&self) -> String {
        fs::read_to_string(self.dir.path().join("kubectl-argv.log")).unwrap_or_default()
    }

    fn assert_no_mutation(&self) {
        let helm = self.helm_log();
        assert!(
            !helm.lines().any(|line| line.starts_with("rollback ")),
            "refusal must not invoke helm rollback: {helm}"
        );
        let kubectl = self.kubectl_log();
        for verb in ["rollout", "scale", "patch", "delete", "apply", "replace"] {
            assert!(
                !kubectl.split_whitespace().any(|token| token == verb),
                "refusal must not mutate workloads with {verb}: {kubectl}"
            );
        }
    }
}

fn json_payload(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "expected JSON output, got stdout={stdout:?} stderr={:?}: {error}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
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
         echo \"$*\" >> \"$FAKE_HELM_ROLLBACK_LOG\"\n\
         case \"$1\" in\n\
         history) cat \"$FAKE_HELM_HISTORY\" ;;\n\
         get) echo 'schema gate read must be disabled' >&2; exit 1 ;;\n\
         rollback) echo 'Rollback was a success.' ;;\n\
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
    let first_helm_log = fs::read_to_string(&rollback_log).unwrap_or_default();
    assert!(
        !first_helm_log
            .lines()
            .any(|line| line.starts_with("rollback ")),
        "helm rollback must not have been invoked: {first_helm_log}"
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

    // Negative control: an ambiguous target still performs no retained
    // manifest read when the whole schema gate is disabled.
    fs::write(&history_json, two_revision_history("0.8.9", "0.9.0"))
        .expect("write ambiguous history");
    let mut opts = rollback_opts();
    opts.disable_schema_gate = true;
    let out = rollback(opts)
        .await
        .expect("without the schema gate the status filter admits 0.8.9");
    match out {
        ClusterRollbackOutput::RolledBack {
            to_revision,
            skipped,
            ..
        } => {
            assert_eq!(to_revision, 1, "status eligible target is v0.8.9");
            assert!(skipped.is_empty());
        }
        other => panic!("expected a completed rollback, got {other:?}"),
    }
    let logged = fs::read_to_string(&rollback_log).expect("helm rollback ran");
    assert!(
        logged
            .lines()
            .any(|line| line == "rollback prod-release 1 -n agent-ns"),
        "removing the schema gate must allow the unsafe helm rollback: {logged}"
    );
    assert!(
        !logged.lines().any(|line| line.starts_with("get manifest ")),
        "the disabled schema gate must not inspect retained manifests: {logged}"
    );

    match std::env::var_os("PATH") {
        Some(_) => std::env::set_var("PATH", existing),
        None => std::env::remove_var("PATH"),
    }
}

#[test]
fn published_v089_without_labeled_metadata_refuses_live_0044_before_mutation() {
    let history = two_revision_history("v0.8.9", "0.9.0");
    let manifest = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: byo-config\n  labels:\n    app.kubernetes.io/component: api\ndata:\n  note: no schema metadata\n";
    let fixture = RollbackFixture::new(&history, "0044", Ok(manifest));
    let output = fixture.run(&[]);
    assert!(!output.status.success(), "published 0.8.9 must be refused");
    let payload = json_payload(&output);
    let error = payload["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("0.8.9") && error.contains("0039") && error.contains("0044"),
        "refusal must name 0.8.9, published head 0039, and live head 0044: {payload}"
    );
    let fix = payload["fix"].as_str().unwrap_or_default();
    assert!(
        fix.contains("fail forward to application 0.9.0"),
        "published refusal must lead with the safe fail forward path: {payload}"
    );
    assert!(
        !fix.contains("repair") && !fix.contains("retry"),
        "successfully classified published metadata must not suggest repair or retry: {payload}"
    );
    assert!(
        fix.contains("helm get manifest prod-release -n agent-ns --revision 1"),
        "published classification must explain how to inspect the retained manifest: {payload}"
    );
    assert!(
        fix.contains("absent") && fix.contains("published") && fix.contains("0039"),
        "published classification must explain that absent labeled metadata selects the catalog head: {payload}"
    );
    assert!(
        fix.contains("helm rollback prod-release 1 -n agent-ns")
            && fix.contains("operator")
            && fix.contains("risk"),
        "refusal must preserve the explicit operator owned raw Helm escape: {payload}"
    );
    assert!(
        fixture
            .helm_log()
            .contains("get manifest prod-release -n agent-ns --revision 1"),
        "selected retained manifest was not inspected: {}",
        fixture.helm_log()
    );
    fixture.assert_no_mutation();
}

#[test]
fn historical_v089_candidate_enforces_its_complete_window() {
    let history = two_revision_history("v0.8.9", "0.9.0");
    let manifest = compatibility_manifest("candidate", "0.8.9", "0044", "0044");
    let compatible = RollbackFixture::new(&history, "0044", Ok(&manifest));
    let output = compatible.run(&[]);
    assert!(
        output.status.success(),
        "known candidate window should roll back: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(json_payload(&output)["rolled_back"], true);
    assert!(compatible
        .helm_log()
        .lines()
        .any(|line| line == "rollback prod-release 1 -n agent-ns"));

    let below_floor = RollbackFixture::new(&history, "0039", Ok(&manifest));
    let output = below_floor.run(&[]);
    assert!(
        !output.status.success(),
        "candidate floor 0044 must refuse 0039"
    );
    let payload = json_payload(&output);
    let error = payload["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("0044") && error.contains("0039"),
        "{payload}"
    );
    below_floor.assert_no_mutation();
}

#[test]
fn ambiguous_v089_metadata_failures_refuse_without_mutation() {
    let history = two_revision_history("0.8.9", "0.9.0");
    let first = compatibility_manifest("first", "0.8.9", "0044", "0044");
    let conflicting = format!(
        "{first}\n---\n{}",
        compatibility_manifest("second", "0.8.9", "0039", "0044")
    );
    let cases = vec![
        ("invalid yaml", "apiVersion: [".to_string()),
        (
            "labeled missing payload",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: missing\n  labels:\n    app.kubernetes.io/component: schema-compat\ndata: {}\n".to_string(),
        ),
        (
            "malformed payload",
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: malformed\n  labels:\n    app.kubernetes.io/component: schema-compat\ndata:\n  application-version: \"0.8.9\"\n  compatibility.json: \"not json\"\n".to_string(),
        ),
        (
            "application version mismatch",
            compatibility_manifest("mismatch", "0.8.8", "0044", "0044"),
        ),
        ("conflicting metadata", conflicting),
        (
            "unknown minimum",
            compatibility_manifest("unknown-min", "0.8.9", "9998", "0044"),
        ),
        (
            "unknown head",
            compatibility_manifest("unknown-head", "0.8.9", "0044", "9999"),
        ),
    ];
    for (name, manifest) in cases {
        let fixture = RollbackFixture::new(&history, "0044", Ok(&manifest));
        let output = fixture.run(&[]);
        assert!(
            !output.status.success(),
            "{name} must fail closed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        fixture.assert_no_mutation();
    }

    let read_failure = RollbackFixture::new(
        &history,
        "0044",
        Err("postgresql://curie:secret-password@postgres:5432/curie"),
    );
    let output = read_failure.run(&[]);
    assert!(
        !output.status.success(),
        "manifest read failure must refuse"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let payload = json_payload(&output);
    let fix = payload["fix"].as_str().unwrap_or_default();
    assert!(fix.contains("helm get manifest prod-release -n agent-ns --revision 1"));
    assert!(fix.contains("0039") && fix.contains("absent") && fix.contains("published"));
    assert!(fix.contains("repair") && fix.contains("retry"));
    assert!(fix.contains("helm rollback prod-release 1 -n agent-ns") && fix.contains("risk"));
    assert!(!stdout.contains("secret-password") && !stderr.contains("secret-password"));
    assert!(!stdout.contains("postgresql://") && !stderr.contains("postgresql://"));
    read_failure.assert_no_mutation();
}

#[test]
fn identical_candidate_metadata_is_accepted() {
    let history = two_revision_history("0.8.9", "0.9.0");
    let manifest = format!(
        "{}\n---\n{}",
        compatibility_manifest("first", "v0.8.9", "0044", "0044"),
        compatibility_manifest("second", "0.8.9", "0044", "0044")
    );
    let fixture = RollbackFixture::new(&history, "0044", Ok(&manifest));
    let output = fixture.run(&[]);
    assert!(output.status.success(), "identical metadata must agree");
    assert_eq!(json_payload(&output)["rolled_back"], true);
}

#[test]
fn unambiguous_v090_does_not_read_retained_manifest() {
    let history = two_revision_history("0.9.0", "0.9.1");
    let fixture = RollbackFixture::new(&history, "0044", Err("must not read manifest"));
    let output = fixture.run(&[]);
    assert!(
        output.status.success(),
        "unambiguous 0.9.0 must remain supported"
    );
    let log = fixture.helm_log();
    assert!(!log.lines().any(|line| line.starts_with("get manifest ")));
    assert!(log
        .lines()
        .any(|line| line == "rollback prod-release 1 -n agent-ns"));
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
