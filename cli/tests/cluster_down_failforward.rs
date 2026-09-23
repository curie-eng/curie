//! Integration (#767, #768): drive the REAL `curie::ops::down` through fake
//! `helm`/`kubectl` on PATH to prove the fail-forward WIRING that the ops-module
//! unit tests cannot exercise: the namespace sweep runs UNCONDITIONALLY after a
//! failed helm uninstall (the core anti-regression, since a revert to `bail!`
//! skips it), the human-visible error message carries the resume command (P1),
//! the exit class tracks the failure kind (connectivity -> Transient exit 3,
//! permanent RBAC -> Failure exit 1, P2), a zero-match sweep is never worded as
//! an actual removal (#768), and the emitted resume command's own exit status
//! genuinely aggregates both steps when actually executed (#768).
//!
//! EVERYTHING lives in ONE test fn. `down()` resolves `helm`/`kubectl` off the
//! process PATH, so the test mutates process-global PATH (and a SWEEP_MARKER env
//! var the fake kubectl records into); a second parallel test in this file would
//! race on that shared state. Each `tests/*.rs` file is its own test binary, so a
//! single test here is race-free, and it saves and restores the original PATH.
//!
//! These assertions are RED until the implementer hardens `down()`: today the
//! resume command lives only in the error `fix` (never composed into the Display
//! message, so scenario 1's message assertion fails), and any nonzero helm exit
//! returns Transient (so scenario 2's permanent case is mislabeled retryable).

use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use curie::exit::{classify, ExitClass};
use curie::ops::{down, down_commands, CommonOpts, DownOpts};

/// `cluster down` reads the External Secrets ownership marker before the
/// release sweep. This release did not install that controller, so the probe
/// is NotFound and must not touch the sweep marker or its first-failure flip.
const OWNERSHIP_PROBE: &str = r#"if [ "$1" = -n ] && [ "$2" = external-secrets ] && [ "$3" = get ] && [ "$4" = configmap ]; then
  echo 'Error from server (NotFound): configmaps "curie-eso-install" not found' >&2
  exit 1
fi
"#;

fn kubectl_fake(rest: &str) -> String {
    let mut script = String::from("#!/bin/sh\n");
    script.push_str(OWNERSHIP_PROBE);
    script.push_str(rest);
    script
}

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

/// The `cluster down` opts for a release whose name differs from its namespace,
/// so an assertion on the label VALUE unambiguously locks it to the release.
fn down_opts() -> DownOpts {
    DownOpts {
        common: CommonOpts {
            namespace: "agent-ns".into(),
            release: "prod-release".into(),
            dry_run: false,
        },
        yes: true,
    }
}

#[tokio::test]
async fn cluster_down_fails_forward_through_real_down() {
    let original_path = std::env::var_os("PATH");

    // ----- Scenario 1: unreachable API server (connectivity) -----
    // Fake helm fails with a TLS-handshake-timeout stderr. Fake kubectl records
    // that the sweep RAN (touch marker) and also fails, so both steps are
    // outstanding and the label-scoped resume command surfaces in the message.
    let dir1 = tempfile::tempdir().expect("tempdir");
    let marker1 = dir1.path().join("sweep-ran-1");
    std::env::set_var("SWEEP_MARKER", &marker1);
    let unreachable =
        "Error: Kubernetes cluster unreachable: Get \"https://h:6443/version\": net/http: TLS handshake timeout";
    write_exec(
        dir1.path(),
        "helm",
        &format!("#!/bin/sh\necho '{unreachable}' >&2\nexit 1\n"),
    );
    write_exec(
        dir1.path(),
        "kubectl",
        &kubectl_fake(&format!(
            "if [ \"$1\" = get ] && [ \"$2\" = namespace ]; then\n  echo '{{\"apiVersion\":\"v1\",\"kind\":\"Namespace\",\"metadata\":{{\"name\":\"agent-ns\",\"labels\":{{}},\"uid\":\"uid-agent-ns\",\"resourceVersion\":\"17\"}}}}'\n  exit 0\nfi\nif [ \"$1\" = get ] && [ \"$2\" = jobs ]; then\n  echo '{{\"apiVersion\":\"v1\",\"kind\":\"List\",\"items\":[]}}'\n  exit 0\nfi\ntouch \"$SWEEP_MARKER\"\necho '{unreachable}' >&2\nexit 1\n"
        )),
    );
    prepend_path(dir1.path());

    let err = down(down_opts())
        .await
        .expect_err("an unreachable API server is an incomplete teardown");

    // Core anti-regression: the sweep ran even though helm failed first. A revert
    // to the old `bail!` on helm failure would leave this marker absent.
    assert!(
        marker1.exists(),
        "the namespace sweep must run unconditionally after a failed helm uninstall"
    );
    let (class, _fix) = classify(&err);
    assert_eq!(
        class,
        ExitClass::Transient,
        "a connectivity failure is retryable (exit 3)"
    );
    assert_eq!(class.code(), 3);
    // P1: a no-json human operator sees the resume command in the message itself.
    let shown = err.to_string();
    assert!(
        shown.contains("curietech.ai/created-by=prod-release"),
        "the human message must carry the label-scoped resume command: {shown}"
    );

    // ----- Scenario 2: permanent RBAC failure -----
    // Fake helm fails forbidden. Fake kubectl actually sweeps (exit 0 AND prints
    // a "namespace ... deleted" line, so `down()` reads this as a real removal,
    // not a #768 zero-match), so only the helm record remains, and a permanent
    // failure must be a plain Failure (exit 1), not a retryable transient. The
    // sweep still ran (marker present).
    let dir2 = tempfile::tempdir().expect("tempdir");
    let marker2 = dir2.path().join("sweep-ran-2");
    std::env::set_var("SWEEP_MARKER", &marker2);
    let forbidden =
        "Error: uninstall: namespaces is forbidden: User \"x\" cannot delete resource \"namespaces\"";
    write_exec(
        dir2.path(),
        "helm",
        &format!("#!/bin/sh\necho '{forbidden}' >&2\nexit 1\n"),
    );
    write_exec(
        dir2.path(),
        "kubectl",
        &kubectl_fake(
            "if [ \"$1\" = get ] && [ \"$2\" = namespace ]; then\n  echo '{\"apiVersion\":\"v1\",\"kind\":\"Namespace\",\"metadata\":{\"name\":\"agent-ns\",\"labels\":{},\"uid\":\"uid-agent-ns\",\"resourceVersion\":\"17\"}}'\n  exit 0\nfi\nif [ \"$1\" = get ] && [ \"$2\" = jobs ]; then\n  echo '{\"apiVersion\":\"v1\",\"kind\":\"List\",\"items\":[]}'\n  exit 0\nfi\ntouch \"$SWEEP_MARKER\"\necho 'namespace \"prod-release-agent-sandbox\" deleted'\nexit 0\n",
        ),
    );
    // Drop scenario 1's fakes: reset to the original PATH, then prepend dir2.
    restore_path(&original_path);
    prepend_path(dir2.path());

    let err = down(down_opts())
        .await
        .expect_err("a forbidden teardown is still an incomplete teardown");

    assert!(
        marker2.exists(),
        "the sweep still runs unconditionally on the permanent-failure path"
    );
    let (class, _fix) = classify(&err);
    assert_eq!(
        class,
        ExitClass::Failure,
        "a permanent RBAC failure must be a plain Failure (exit 1), not Transient"
    );
    assert_eq!(class.code(), 1);
    assert_ne!(class, ExitClass::Transient);
    // The sweep really did remove something here, so the message is allowed to
    // (and does) say "swept" -- this is the actual-removal case #768 must be
    // distinguished FROM in scenario 3 below.
    let shown = err.to_string();
    assert!(
        shown.contains("swept"),
        "a real removal must still be worded as swept: {shown}"
    );

    // ----- Scenario 3 (#768): zero-match sweep, not an actual removal -----
    // Fake helm fails (transient connectivity). Fake kubectl exits 0 but prints
    // NOTHING to stdout, exactly what `kubectl delete namespace -l ...
    // --ignore-not-found` does when the label selector matches no namespace
    // (the pre-existing-namespace case from #707: it was never stamped, so a
    // release's own down never touches it). Before #768 this was
    // indistinguishable from scenario 2's real removal and produced the same
    // "were swept" message even though nothing was actually deleted and the
    // failed release's own compute may still be running.
    let dir3 = tempfile::tempdir().expect("tempdir");
    let marker3 = dir3.path().join("sweep-ran-3");
    std::env::set_var("SWEEP_MARKER", &marker3);
    let unreachable3 =
        "Error: Kubernetes cluster unreachable: Get \"https://h:6443/version\": connection refused";
    write_exec(
        dir3.path(),
        "helm",
        &format!("#!/bin/sh\necho '{unreachable3}' >&2\nexit 1\n"),
    );
    write_exec(
        dir3.path(),
        "kubectl",
        &kubectl_fake(
            "if [ \"$1\" = get ] && [ \"$2\" = namespace ]; then\n  echo '{\"apiVersion\":\"v1\",\"kind\":\"Namespace\",\"metadata\":{\"name\":\"agent-ns\",\"labels\":{},\"uid\":\"uid-agent-ns\",\"resourceVersion\":\"17\"}}'\n  exit 0\nfi\nif [ \"$1\" = get ] && [ \"$2\" = jobs ]; then\n  echo '{\"apiVersion\":\"v1\",\"kind\":\"List\",\"items\":[]}'\n  exit 0\nfi\ntouch \"$SWEEP_MARKER\"\nexit 0\n",
        ),
    );
    restore_path(&original_path);
    prepend_path(dir3.path());

    let err = down(down_opts())
        .await
        .expect_err("a stale helm record after a zero-match sweep is still incomplete");

    assert!(
        marker3.exists(),
        "the zero-match sweep still ran unconditionally"
    );
    let shown = err.to_string();
    // The #768 anti-regression: must NOT be worded like scenario 2's real
    // removal, since nothing actually matched the selector here.
    assert!(
        !shown.contains("were swept"),
        "a zero-match sweep must never be worded as an actual removal: {shown}"
    );
    // Must still surface the helm-only resume command so the operator can
    // finish teardown.
    assert!(
        shown.contains("helm uninstall prod-release -n agent-ns"),
        "the message must still carry the helm-only resume command: {shown}"
    );

    // ----- Scenario 4 (#768): the resume command's own exit status aggregates
    // BOTH steps when actually executed, even when the LAST command (the
    // sweep) succeeds -----
    // Both fakes fail on their FIRST invocation (so the initial `down()` call
    // sees both steps outstanding and emits the two-step resume line), then
    // fake helm keeps failing forever while fake kubectl SUCCEEDS on every
    // invocation after its first. That flip means: when the emitted resume
    // line is actually re-executed, helm fails again but the sweep (the LAST
    // command in the line) succeeds. Under the OLD "; " join this would read
    // as exit 0 (the last command's status) even though helm is still broken
    // -- exactly the bug #768 reports. Each fake also appends a line to its own
    // log file on every invocation, so the test can additionally prove BOTH
    // commands actually ran during the re-execution (not short-circuited).
    let dir4 = tempfile::tempdir().expect("tempdir");
    let helm_log = dir4.path().join("helm.log");
    let kubectl_log = dir4.path().join("kubectl.log");
    let kubectl_switched = dir4.path().join("kubectl-switched");
    write_exec(
        dir4.path(),
        "helm",
        &format!(
            "#!/bin/sh\necho x >> '{}'\necho 'Kubernetes cluster unreachable: connection refused' >&2\nexit 1\n",
            helm_log.display()
        ),
    );
    write_exec(
        dir4.path(),
        "kubectl",
        &kubectl_fake(&format!(
            "if [ \"$1\" = get ] && [ \"$2\" = namespace ]; then\n  echo '{{\"apiVersion\":\"v1\",\"kind\":\"Namespace\",\"metadata\":{{\"name\":\"agent-ns\",\"labels\":{{}},\"uid\":\"uid-agent-ns\",\"resourceVersion\":\"17\"}}}}'\n  exit 0\nfi\nif [ \"$1\" = get ] && [ \"$2\" = jobs ]; then\n  echo '{{\"apiVersion\":\"v1\",\"kind\":\"List\",\"items\":[]}}'\n  exit 0\nfi\necho x >> '{log}'\nif [ -f '{switched}' ]; then\n  echo 'namespace \"x\" deleted'\n  exit 0\nelse\n  touch '{switched}'\n  echo 'Kubernetes cluster unreachable: connection refused' >&2\n  exit 1\nfi\n",
            log = kubectl_log.display(),
            switched = kubectl_switched.display(),
        )),
    );
    restore_path(&original_path);
    prepend_path(dir4.path());

    let err = down(down_opts())
        .await
        .expect_err("both steps failing on the first pass is still an incomplete teardown");
    let (_class, fix) = classify(&err);
    let resume_cmd = fix.expect("a fail-forward teardown carries a resume command");
    assert!(
        resume_cmd.contains("helm uninstall") && resume_cmd.contains("kubectl delete namespace"),
        "expected the two-step resume line since both steps failed on the first pass: {resume_cmd}"
    );
    assert_eq!(
        fs::read_to_string(&helm_log).unwrap().lines().count(),
        1,
        "helm should have run exactly once so far"
    );
    assert_eq!(
        fs::read_to_string(&kubectl_log).unwrap().lines().count(),
        1,
        "kubectl should have run exactly once so far"
    );

    // Actually EXECUTE the emitted resume command through a real shell, with the
    // SAME fakes still on PATH, exactly as an operator pasting it or CI running
    // the `--json` `fix` verbatim would. This time kubectl (the LAST command in
    // the line) succeeds, while helm (the FIRST) fails again.
    let status = std::process::Command::new("sh")
        .arg("-c")
        .arg(&resume_cmd)
        .status()
        .expect("run the emitted resume command");
    assert!(
        !status.success(),
        "helm is still broken, so the resume line's own exit status must be nonzero even \
         though the trailing sweep succeeded (the old \"; \" join misreported exit 0 here): \
         {resume_cmd}"
    );
    // Both commands must have run a SECOND time during that re-execution (not
    // short-circuited by helm's failure, and not skipped by the aggregation).
    assert_eq!(
        fs::read_to_string(&helm_log).unwrap().lines().count(),
        2,
        "helm must run again when the resume command is re-executed"
    );
    assert_eq!(
        fs::read_to_string(&kubectl_log).unwrap().lines().count(),
        2,
        "the sweep must still run unconditionally even though helm failed again"
    );

    // Restore PATH so nothing else in this process observes the fakes.
    restore_path(&original_path);
}

/// #1654 public-surface regression: the sweep `cluster down` emits must be
/// scoped by BOTH ownership labels, the release name AND the release's install
/// namespace. Two independent installs on one cluster normally share the
/// default release name and differ only in namespace, so a `created-by`-only
/// selector matched the OTHER install's namespaces and deleted them. This test
/// rides the PUBLIC `down_commands` builder (the same argv `down()` executes
/// and the same one the resume command is rendered from), so a revert that
/// drops `created-in` from the selector fails here even if the internal unit
/// tests were rewritten. This test builds argv only and never touches PATH, so
/// it does not race the PATH-mutating test above.
#[test]
fn down_sweep_selector_is_scoped_by_release_and_namespace() {
    let cmds = down_commands(&down_opts().common);
    assert_eq!(cmds.len(), 2);
    let sweep = cmds[1].display();
    assert_eq!(
        sweep,
        "kubectl delete namespace -l curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns --ignore-not-found"
    );
}
