//! `curie cluster rollback` (#1899): a bare `helm rollback` targets the
//! immediately preceding revision, and on a cluster with no `runsc`
//! RuntimeClass that is a FAILED one -- `cluster up` records a failed revision
//! before its successful gVisor-off retry, so the history alternates
//! failed/superseded. This file pins both halves of the fix:
//!
//! 1. The PURE selection (`select_rollback_revision`, `resolve_explicit_revision`,
//!    `parse_helm_history`), which needs no cluster and no PATH games, so those
//!    are plain `#[test]`s.
//! 2. The WIRING: that the selected revision is the one actually handed to
//!    `helm rollback`. A pure selector that is never plumbed through would pass
//!    every test in layer 1 and still ship the bug.
//!
//! EVERY PATH-dependent assertion lives in ONE `#[tokio::test]`. `rollback()`
//! resolves `helm` off the process PATH, so that test mutates process-global
//! PATH (and env vars the fake `helm` reads); a second parallel test touching
//! PATH in this file would race it. Each `tests/*.rs` file is its own test
//! binary, so one such test here is race-free, and it saves and restores the
//! original PATH. The pure tests below never touch PATH, so they cannot race it.

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use curie::exit::classify;
use curie::ops::{
    parse_helm_history, resolve_explicit_revision, rollback, select_rollback_revision,
    ClusterRollbackOutput, CommonOpts, HelmRevision, RollbackChoice, RollbackOpts, RollbackTarget,
};

fn revision(revision: u32, status: &str) -> HelmRevision {
    HelmRevision {
        revision,
        status: status.to_string(),
        chart: "curie-0.7.3".to_string(),
        app_version: "0.7.3".to_string(),
        description: "Upgrade complete".to_string(),
    }
}

/// The exact history from issue #1899: eight alternating superseded/failed
/// revisions from the runsc-less install loop, topped by the successful retry.
fn issue_1899_history() -> Vec<HelmRevision> {
    vec![
        revision(13, "superseded"),
        revision(14, "failed"),
        revision(15, "superseded"),
        revision(16, "failed"),
        revision(17, "superseded"),
        revision(18, "failed"),
        revision(19, "superseded"),
        revision(20, "failed"),
        revision(21, "deployed"),
    ]
}

fn eligible(history: &[HelmRevision]) -> RollbackChoice {
    select_rollback_revision(history)
        .expect("a non-empty history is selectable")
        .require_eligible()
        .expect("this history has an eligible revision")
}

// ---------------------------------------------------------------------------
// Layer 1: the pure selection.
// ---------------------------------------------------------------------------

/// THE REGRESSION TEST for #1899. On the issue's own history the verb must land
/// on 19, the newest `superseded` revision, and report that it stepped over 20 --
/// which is precisely where a bare `helm rollback` would have gone.
#[test]
fn rollback_skips_the_failed_revision_bare_helm_would_target() {
    let choice = eligible(&issue_1899_history());

    assert_eq!(
        choice.to_revision, 19,
        "the newest deployed/superseded revision below the current one is 19"
    );
    assert_ne!(
        choice.to_revision, 20,
        "bare `helm rollback` would target 20, which helm marks failed"
    );
    assert_eq!(choice.from_revision, 21, "21 is the deployed revision");
    assert!(
        choice.skipped.contains(&20),
        "the operator must be told 20 was passed over: {:?}",
        choice.skipped
    );
    assert!(
        !choice.forced,
        "no override was needed for a superseded target"
    );
}

/// The real recovery case: the newest revision FAILED, so helm marks nothing
/// `deployed` at all. The current revision falls back to the highest one, and
/// the selection still walks down to the newest superseded revision below it.
/// Without that fallback the verb would refuse exactly when it is needed most.
#[test]
fn rollback_falls_back_to_the_highest_revision_when_none_is_deployed() {
    let history = vec![
        revision(1, "superseded"),
        revision(2, "superseded"),
        revision(3, "failed"),
    ];

    let choice = eligible(&history);

    assert_eq!(
        choice.from_revision, 3,
        "the failed newest revision is current"
    );
    assert_eq!(choice.to_revision, 2);
}

#[test]
fn a_single_revision_release_is_refused_rather_than_silently_doing_nothing() {
    let target = select_rollback_revision(&[revision(1, "deployed")])
        .expect("one revision is still a readable history");
    assert!(matches!(
        target,
        RollbackTarget::NoEligible { current: 1, .. }
    ));

    let err = target
        .require_eligible()
        .expect_err("there is nothing below revision 1 to roll back to");
    let shown = err.to_string();
    assert!(
        shown.contains("only revision"),
        "the operator must be told why, not get a no-op: {shown}"
    );
}

#[test]
fn a_history_whose_every_prior_revision_failed_is_refused() {
    let history = vec![
        revision(1, "failed"),
        revision(2, "failed"),
        revision(3, "deployed"),
    ];

    let err = select_rollback_revision(&history)
        .expect("a readable history")
        .require_eligible()
        .expect_err("no revision below 3 was ever known good");
    let shown = err.to_string();
    assert!(
        shown.contains('1') && shown.contains('2'),
        "the refusal must name the revisions it rejected: {shown}"
    );
    let (_class, fix) = classify(&err);
    let fix = fix.unwrap_or_default();
    assert!(
        fix.contains("--allow-failed-revision"),
        "the refusal must name the override that would proceed anyway: {fix}"
    );
}

/// The four pending states, `uninstalling` and `unknown` are as ineligible as
/// `failed`: helm never finished putting any of them on the cluster.
#[test]
fn only_deployed_and_superseded_revisions_are_eligible() {
    let history = vec![
        revision(1, "superseded"),
        revision(2, "pending-upgrade"),
        revision(3, "pending-rollback"),
        revision(4, "uninstalling"),
        revision(5, "unknown"),
        revision(6, "deployed"),
    ];

    let choice = eligible(&history);

    assert_eq!(choice.to_revision, 1);
    assert_eq!(choice.skipped, vec![2, 3, 4, 5]);
}

#[test]
fn an_explicit_revision_that_is_not_in_the_history_is_refused() {
    let err = resolve_explicit_revision(&issue_1899_history(), 99, false)
        .expect_err("helm would fail obscurely on a revision it does not have");
    let shown = err.to_string();
    assert!(
        shown.contains("99") && shown.contains("13"),
        "the refusal must say which revisions do exist: {shown}"
    );
}

#[test]
fn an_explicit_failed_revision_is_refused_until_the_override_is_passed() {
    let err = resolve_explicit_revision(&issue_1899_history(), 20, false)
        .expect_err("20 is failed, so it needs the explicit override");
    let shown = err.to_string();
    assert!(
        shown.contains("failed"),
        "the refusal must name the status it read: {shown}"
    );
    let (_class, fix) = classify(&err);
    assert!(
        fix.unwrap_or_default().contains("--allow-failed-revision"),
        "the refusal must name the override"
    );

    let forced = resolve_explicit_revision(&issue_1899_history(), 20, true)
        .expect("--allow-failed-revision admits it");
    assert_eq!(forced.to_revision, 20);
    assert!(
        forced.forced,
        "the output must record that it was overridden"
    );
}

/// A real-shaped `helm history -o json` payload, extra fields and all.
#[test]
fn parse_helm_history_reads_a_real_helm_payload() {
    let payload = r#"[
      {"revision":19,"updated":"2026-08-20T09:11:02.1Z","status":"superseded","chart":"curie-0.7.3","app_version":"0.7.3","description":"Upgrade complete"},
      {"revision":20,"updated":"2026-08-20T09:14:41.7Z","status":"failed","chart":"curie-0.7.3","app_version":"0.7.3","description":"failed to create resource: RuntimeClass \"gvisor\" not found"},
      {"revision":21,"updated":"2026-08-20T09:14:58.3Z","status":"deployed","chart":"curie-0.7.3","app_version":"0.7.3","description":"Upgrade complete"}
    ]"#;

    let history = parse_helm_history(payload).expect("a real helm payload parses");

    assert_eq!(history.len(), 3);
    assert_eq!(history[1].status, "failed");
    assert_eq!(history[0].app_version, "0.7.3");
    assert!(history[1].description.contains("gvisor"));
    assert_eq!(eligible(&history).to_revision, 19);
}

/// A `revision` arriving as a string has to keep working: the shape has moved
/// between helm releases, and a hard failure here would take the verb down on a
/// cluster whose helm is a version older or newer than the one we tested on.
#[test]
fn parse_helm_history_tolerates_a_string_revision() {
    let history = parse_helm_history(r#"[{"revision":"7","status":"deployed"}]"#)
        .expect("a string revision is still a revision");
    assert_eq!(history[0].revision, 7);
}

#[test]
fn parse_helm_history_refuses_malformed_output_without_panicking() {
    let err =
        parse_helm_history("Error: release: not found").expect_err("helm prose is not a history");
    let (_class, fix) = classify(&err);
    assert!(
        fix.unwrap_or_default().contains("helm history"),
        "a parse failure must tell the operator what to run by hand"
    );
}

// ---------------------------------------------------------------------------
// Layer 2: the wiring, through a fake `helm` on PATH.
// ---------------------------------------------------------------------------

/// Write `body` to `dir/name` and mark it executable (0o755).
fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write fake executable");
    let mut perms = fs::metadata(&path).expect("stat fake").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod fake executable");
}

/// Prepend `dir` to the current process PATH so its fake binaries win resolution.
fn prepend_path(dir: &Path) {
    let existing = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(&existing));
    let joined = std::env::join_paths(paths).expect("join PATH");
    std::env::set_var("PATH", joined);
}

/// Restore PATH to the exact value captured at the start of the test.
fn restore_path(original: &Option<OsString>) {
    match original {
        Some(p) => std::env::set_var("PATH", p),
        None => std::env::remove_var("PATH"),
    }
}

/// A release whose name differs from its namespace, so an assertion on the
/// recorded argv unambiguously locks each token to the flag it came from.
fn rollback_opts(revision: Option<u32>, allow_failed_revision: bool) -> RollbackOpts {
    RollbackOpts {
        common: CommonOpts {
            namespace: "agent-ns".into(),
            release: "prod-release".into(),
            dry_run: false,
        },
        revision,
        allow_failed_revision,
        // Always: an unanswered prompt would hang the test binary.
        yes: true,
        disable_schema_gate: false,
        live_schema_revision: None,
    }
}

#[tokio::test]
async fn rollback_hands_helm_the_selected_revision() {
    let original_path = std::env::var_os("PATH");
    let dir = tempfile::tempdir().expect("tempdir");

    // The issue's history, served by the fake `helm history`.
    let history_json = dir.path().join("history.json");
    fs::write(
        &history_json,
        r#"[
          {"revision":13,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
          {"revision":14,"status":"failed","chart":"curie-0.8.6","app_version":"0.8.6","description":"RuntimeClass \"gvisor\" not found"},
          {"revision":15,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
          {"revision":16,"status":"failed","chart":"curie-0.8.6","app_version":"0.8.6","description":"RuntimeClass \"gvisor\" not found"},
          {"revision":17,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
          {"revision":18,"status":"failed","chart":"curie-0.8.6","app_version":"0.8.6","description":"RuntimeClass \"gvisor\" not found"},
          {"revision":19,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
          {"revision":20,"status":"failed","chart":"curie-0.8.6","app_version":"0.8.6","description":"RuntimeClass \"gvisor\" not found"},
          {"revision":21,"status":"deployed","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"}
        ]"#,
    )
    .expect("write fake history");

    // Records the FULL argv of every `helm rollback`, one invocation per line:
    // the argv is the user-visible contract this whole verb exists to change.
    let rollback_log = dir.path().join("rollback-argv.log");
    std::env::set_var("FAKE_HELM_HISTORY", &history_json);
    std::env::set_var("FAKE_HELM_ROLLBACK_LOG", &rollback_log);
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
    write_exec(dir.path(), "kubectl", "#!/bin/sh\necho '0039 (head)'\n");
    prepend_path(dir.path());

    // ----- The default path: no --revision -----
    let out = rollback(rollback_opts(None, false))
        .await
        .expect("the issue's history has an eligible revision");
    match out {
        ClusterRollbackOutput::RolledBack {
            from_revision,
            to_revision,
            skipped,
            forced,
        } => {
            assert_eq!(from_revision, 21);
            assert_eq!(to_revision, 19);
            assert_eq!(
                skipped,
                vec![20],
                "AC2: the passed-over revision is reported"
            );
            assert!(!forced);
        }
        other => panic!("expected a completed rollback, got {other:?}"),
    }
    // The end-to-end proof: the selector is genuinely wired to the invocation.
    let logged = fs::read_to_string(&rollback_log).expect("helm rollback ran");
    assert_eq!(
        logged.trim(),
        "rollback prod-release 19 -n agent-ns",
        "helm must be handed revision 19, not the 20 a bare `helm rollback` would take"
    );

    // ----- AC3: an explicit failed revision is refused, by execution -----
    let err = rollback(rollback_opts(Some(20), false))
        .await
        .expect_err("20 is failed, so it must not go through without the override");
    let shown = err.to_string();
    assert!(
        shown.contains("failed"),
        "the operator must read the status that blocked it: {shown}"
    );
    let (_class, fix) = classify(&err);
    assert!(
        fix.unwrap_or_default().contains("--allow-failed-revision"),
        "the refusal must name the override"
    );
    assert_eq!(
        fs::read_to_string(&rollback_log).unwrap().lines().count(),
        1,
        "the refused run must not have invoked helm rollback at all"
    );

    // ----- AC3: with the override, the same revision proceeds -----
    let out = rollback(rollback_opts(Some(20), true))
        .await
        .expect("--allow-failed-revision admits revision 20");
    match out {
        ClusterRollbackOutput::RolledBack {
            to_revision,
            forced,
            ..
        } => {
            assert_eq!(to_revision, 20);
            assert!(forced, "the output must record the override");
        }
        other => panic!("expected a completed rollback, got {other:?}"),
    }
    let logged = fs::read_to_string(&rollback_log).expect("helm rollback ran again");
    assert_eq!(
        logged.lines().last().unwrap(),
        "rollback prod-release 20 -n agent-ns"
    );

    // ----- AC6: a release helm cannot find fails HERE, not as "no eligible revision" -----
    write_exec(
        dir.path(),
        "helm",
        "#!/bin/sh\necho 'Error: release: not found' >&2\nexit 1\n",
    );
    let err = rollback(rollback_opts(None, false))
        .await
        .expect_err("an unreadable history is a hard failure");
    let shown = err.to_string();
    assert!(
        shown.contains("release: not found") && shown.contains("prod-release"),
        "the operator must see helm's own reason and the release name: {shown}"
    );
    assert!(
        !shown.contains("no revision is safe"),
        "a missing release must never be reported as an ineligible history: {shown}"
    );

    // Restore PATH so nothing else in this process observes the fakes.
    restore_path(&original_path);
}

// ---------------------------------------------------------------------------
// Layer 1 (continued): the gaps an adversarial read of the above found.
// ---------------------------------------------------------------------------

/// The bug this pins was real: `skipped_between` had no status filter, so an
/// explicit `--revision 15` on the issue's history reported every revision in
/// the gap -- including 17 and 19, which are `superseded` -- and labelled them
/// "not deployed/superseded". `skipped` is an operator-facing claim about
/// WHY a revision was passed over, so a perfectly good revision must never
/// appear in it.
#[test]
fn skipped_lists_only_ineligible_revisions_not_every_revision_stepped_over() {
    let choice = resolve_explicit_revision(&issue_1899_history(), 15, false)
        .expect("15 is superseded, so no override is needed");

    assert_eq!(choice.to_revision, 15);
    assert_eq!(choice.from_revision, 21);
    assert_eq!(
        choice.skipped,
        vec![16, 18, 20],
        "only the failed revisions in the gap were passed over for a reason"
    );
    assert!(
        !choice.skipped.contains(&17) && !choice.skipped.contains(&19),
        "17 and 19 are superseded; calling them not deployed/superseded is a false claim: {:?}",
        choice.skipped
    );
    assert!(!choice.forced, "a superseded target needs no override");
}

/// `helm history` can return an empty array (a release mid-uninstall). Both
/// entry points must refuse it with the operator's next step, not panic on the
/// `max()` of an empty iterator.
#[test]
fn an_empty_history_is_refused_rather_than_panicking() {
    let err = select_rollback_revision(&[]).expect_err("there is nothing to roll back to");
    let shown = err.to_string();
    assert!(
        shown.contains("no revisions"),
        "the operator must be told the history was empty: {shown}"
    );
    let (_class, fix) = classify(&err);
    assert!(
        fix.unwrap_or_default().contains("helm list"),
        "the refusal must name how to check the release exists"
    );

    let err = resolve_explicit_revision(&[], 3, false)
        .expect_err("an explicit revision cannot be resolved against an empty history");
    assert!(err.to_string().contains("no revisions"));
}

/// The current revision is the `deployed` one, NOT the highest one: a
/// `pending-upgrade` row sits ABOVE `deployed` while a helm upgrade is in
/// flight. Selecting from the top of the history instead would step the release
/// back to where it already is and report the in-flight revision as skipped.
#[test]
fn the_deployed_revision_is_current_even_when_a_newer_revision_sits_above_it() {
    let history = vec![
        revision(1, "superseded"),
        revision(2, "superseded"),
        revision(3, "deployed"),
        revision(4, "pending-upgrade"),
    ];

    let choice = eligible(&history);

    assert_eq!(
        choice.from_revision, 3,
        "3 is deployed; 4 is still in flight"
    );
    assert_eq!(choice.to_revision, 2, "selection happens below the current");
    assert!(
        choice.skipped.is_empty(),
        "4 is above the current revision, so it was never stepped over: {:?}",
        choice.skipped
    );
}

/// A history can carry more than one `deployed` row -- helm has left them
/// behind after an interrupted upgrade. Pin the reading rather than leaving it
/// incidental: the NEWEST deployed revision is where the release is now.
#[test]
fn more_than_one_deployed_row_resolves_to_the_newest_of_them() {
    let history = vec![
        revision(1, "superseded"),
        revision(2, "deployed"),
        revision(3, "failed"),
        revision(4, "deployed"),
    ];

    let choice = eligible(&history);

    assert_eq!(
        choice.from_revision, 4,
        "the newest deployed row is current"
    );
    assert_eq!(choice.to_revision, 2);
    assert_eq!(choice.skipped, vec![3]);
}

/// Naming the revision the release is already on is resolved, not refused, and
/// reports nothing skipped -- there is no gap to step over. Pinned because an
/// off-by-one in the `< current` filters would show up here first.
#[test]
fn an_explicit_revision_equal_to_the_current_one_skips_nothing() {
    let choice = resolve_explicit_revision(&issue_1899_history(), 21, false)
        .expect("21 is the deployed revision");

    assert_eq!(choice.from_revision, 21);
    assert_eq!(choice.to_revision, 21);
    assert!(
        choice.skipped.is_empty(),
        "nothing lies between 21 and itself: {:?}",
        choice.skipped
    );
    assert!(!choice.forced);
}

/// A truncated or corrupt helm secret can yield two rows for one revision.
/// `parse_helm_history` sorts the INELIGIBLE row first so the `find` in
/// `resolve_explicit_revision` reads the worse of the two: a revision whose
/// history disagrees with itself must refuse rather than roll back to a
/// manifest that may never have been applied. The skipped list must not report
/// the same number twice either.
#[test]
fn duplicate_revision_rows_refuse_rather_than_admitting_the_eligible_twin() {
    let payload = r#"[
      {"revision":19,"status":"superseded","chart":"curie-0.7.3","description":"Upgrade complete"},
      {"revision":20,"status":"superseded","chart":"curie-0.7.3","description":"Upgrade complete"},
      {"revision":20,"status":"failed","chart":"curie-0.7.3","description":"RuntimeClass \"gvisor\" not found"},
      {"revision":21,"status":"deployed","chart":"curie-0.7.3","description":"Upgrade complete"}
    ]"#;
    let history = parse_helm_history(payload).expect("a duplicated revision still parses");

    let err = resolve_explicit_revision(&history, 20, false)
        .expect_err("a revision whose history disagrees with itself must not go through");
    let shown = err.to_string();
    assert!(
        shown.contains("failed"),
        "the refusal must read the ineligible twin, not the flattering one: {shown}"
    );

    let choice = eligible(&history);
    assert_eq!(choice.to_revision, 19, "20 is not trustworthy, 19 is");
    assert_eq!(
        choice.skipped,
        vec![20],
        "one revision passed over is reported once, not once per row: {:?}",
        choice.skipped
    );
}

/// The `fix` hint only reaches an agent under `--json`; a human on a TTY sees
/// the message alone. The override has to be named in the sentence the operator
/// actually reads, or the refusal is a dead end for them.
#[test]
fn the_ineligible_status_refusal_names_the_override_in_its_own_message() {
    let err =
        resolve_explicit_revision(&issue_1899_history(), 18, false).expect_err("18 is failed");
    let shown = err.to_string();
    assert!(
        shown.contains("--allow-failed-revision"),
        "a human never sees the --json fix hint, so the message must carry it: {shown}"
    );
}

// ---------------------------------------------------------------------------
// Layer 3: the binary, with a fake `helm` on the CHILD's PATH only.
// ---------------------------------------------------------------------------
//
// These run `curie cluster rollback` as a subprocess, so PATH is set per child
// via `Command::env` and this process's own PATH is never touched -- they
// cannot race the one PATH-mutating test above. That is also the only way to
// assert the operator-visible stderr notes, which no in-process test can read.

/// Install a fake `helm` in `dir` that serves `history_json` and appends every
/// invocation's argv to `dir/helm-argv.log`.
fn fake_helm_dir(history_json: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let history = dir.path().join("history.json");
    fs::write(&history, history_json).expect("write fake history");
    write_exec(
        dir.path(),
        "helm",
        &format!(
            "#!/bin/sh\n\
             echo \"$*\" >> '{log}'\n\
             case \"$1\" in\n\
             history) cat '{history}' ;;\n\
             rollback) echo 'Rollback was a success.' ;;\n\
             *) echo \"unexpected helm verb: $1\" >&2; exit 1 ;;\n\
             esac\n",
            log = dir.path().join("helm-argv.log").display(),
            history = history.display(),
        ),
    );
    write_exec(dir.path(), "kubectl", "#!/bin/sh\necho '0039 (head)'\n");
    dir
}

/// `curie cluster rollback` against that fake helm, with PATH scoped to the
/// child process.
fn run_rollback(dir: &Path, args: &[&str]) -> std::process::Output {
    let existing = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(&existing));
    std::process::Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["cluster", "rollback"])
        .args(["--namespace", "agent-ns", "--release", "prod-release"])
        .args(args)
        .env("PATH", std::env::join_paths(paths).expect("join PATH"))
        // Plain text, and never an unanswered prompt in a test binary.
        .env("NO_COLOR", "1")
        .output()
        .expect("run curie cluster rollback")
}

fn helm_log(dir: &Path) -> String {
    fs::read_to_string(dir.join("helm-argv.log")).unwrap_or_default()
}

/// The AC2 note claims "a bare `helm rollback` would have targeted N" -- and
/// helm's own default target is always `from - 1`. On an explicit `--revision`
/// the highest SKIPPED revision need not be that one, and naming it as helm's
/// target would be a fabrication printed to an operator mid-incident. Both
/// branches, because a note that always claimed it would pass a test of only
/// the first.
#[test]
fn the_bare_helm_claim_is_made_only_when_the_skipped_revision_is_the_one_helm_would_target() {
    // Auto-select on the issue's history: 20 is skipped AND is 21 - 1.
    let dir = fake_helm_dir(
        r#"[
          {"revision":19,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
          {"revision":20,"status":"failed","chart":"curie-0.8.6","app_version":"0.8.6","description":"RuntimeClass \"gvisor\" not found"},
          {"revision":21,"status":"deployed","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"}
        ]"#,
    );
    let out = run_rollback(dir.path(), &["--yes"]);
    let shown = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "rollback failed: {shown}");
    assert!(
        shown.contains("skipped revision(s) 20") && shown.contains("would have targeted 20"),
        "the operator must be told bare helm would have landed on the failed 20: {shown}"
    );

    // Explicit --revision 17: the only INELIGIBLE skipped revision is 18, while
    // helm's own target would have been 20 -- which is superseded and fine.
    let dir = fake_helm_dir(
        r#"[
          {"revision":17,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
          {"revision":18,"status":"failed","chart":"curie-0.8.6","app_version":"0.8.6","description":"RuntimeClass \"gvisor\" not found"},
          {"revision":19,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
          {"revision":20,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
          {"revision":21,"status":"deployed","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"}
        ]"#,
    );
    let out = run_rollback(dir.path(), &["--yes", "--revision", "17"]);
    let shown = String::from_utf8_lossy(&out.stderr).to_string();
    assert!(out.status.success(), "rollback failed: {shown}");
    assert!(
        shown.contains("skipped revision(s) 18"),
        "18 is the one revision passed over for a reason: {shown}"
    );
    assert!(
        !shown.contains("would have targeted"),
        "bare helm would have targeted 20, which is superseded, so the claim is false here: {shown}"
    );
    assert!(
        helm_log(dir.path()).contains("rollback prod-release 17 -n agent-ns"),
        "the explicit revision must be the one handed to helm"
    );
}

/// `--dry-run` prints the plan and runs NOTHING: not even the `helm history`
/// read, since the fake helm records every invocation it receives. Both
/// spellings, because the plan can only name the target revision when the
/// operator did -- deleting either half of the early return leaves the fake
/// helm's log non-empty.
#[test]
fn dry_run_names_the_plan_and_runs_no_helm_command_at_all() {
    let history = r#"[
      {"revision":19,"status":"superseded","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"},
      {"revision":20,"status":"failed","chart":"curie-0.8.6","app_version":"0.8.6","description":"RuntimeClass \"gvisor\" not found"},
      {"revision":21,"status":"deployed","chart":"curie-0.8.6","app_version":"0.8.6","description":"Upgrade complete"}
    ]"#;

    // ----- No --revision: the target is a function of a history it has not read -----
    let dir = fake_helm_dir(history);
    let out = run_rollback(dir.path(), &["--dry-run"]);
    let plan = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        out.status.success(),
        "a dry run must exit clean: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        plan.contains("helm history prod-release -n agent-ns -o json --max 256"),
        "the plan must name the history read: {plan}"
    );
    assert!(
        plan.contains("helm rollback prod-release <selected-revision> -n agent-ns"),
        "with no --revision the plan cannot name a revision it never read: {plan}"
    );
    assert!(
        plan.contains("kubectl exec") && plan.contains("alembic"),
        "the plan must name the live-schema probe that runs before helm mutates: {plan}"
    );
    assert_eq!(
        helm_log(dir.path()),
        "",
        "a dry run must not invoke helm even to read the history"
    );

    // ----- With --revision: the plan names the exact revision, still runs nothing -----
    let dir = fake_helm_dir(history);
    let out = run_rollback(dir.path(), &["--dry-run", "--revision", "19"]);
    let plan = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(out.status.success());
    assert!(
        plan.contains("helm rollback prod-release 19 -n agent-ns"),
        "an operator-named revision belongs in the plan: {plan}"
    );
    assert_eq!(
        helm_log(dir.path()),
        "",
        "a dry run must mutate nothing, --revision or not"
    );
}
