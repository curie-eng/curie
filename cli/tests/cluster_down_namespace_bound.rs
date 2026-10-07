//! #4017: `cluster down` bounds the owned-namespace sweep and, when that wait
//! expires, names a remaining object instead of clearing finalizers (#707,
//! #767, #768).
//!
//! The PATH-mutating scenario lives in ONE async test. `down()` resolves
//! `helm`/`kubectl` off the process PATH, so a second parallel test in this
//! file would race on that shared state. The sync test below builds argv only
//! and never touches PATH. It is the fix pin.

#[path = "support/executable.rs"]
mod test_executable;

use std::ffi::OsString;
use std::fs;
use std::path::Path;

use curie::exit::{classify, ExitClass};
use curie::ops::{down, down_commands, CommonOpts, DownOpts};

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

const PRESENT_NAMESPACE: &str = r#"{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"agent-ns","labels":{},"uid":"uid-agent-ns","resourceVersion":"17"}}"#;

const TERMINATING_NAMESPACE_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"agent-ns","deletionTimestamp":"2026-01-01T00:00:00Z"},"status":{"phase":"Terminating","conditions":[{"type":"NamespaceFinalizersRemaining","status":"True"}]}}]}"#;

const HOLD_CONFIGMAP_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"hold","namespace":"agent-ns"}}]}"#;

/// #4017 fix pin: the public sweep argv carries a 300s bound and still refuses
/// to clear finalizers or force the delete. This test does not change PATH.
#[test]
fn sweep_command_carries_300s_timeout() {
    let opts = DownOpts {
        common: CommonOpts {
            namespace: "agent-ns".into(),
            release: "prod-release".into(),
            dry_run: false,
        },
        yes: true,
    };
    let cmds = down_commands(&opts.common);
    let sweep = cmds[1].display();
    assert!(sweep.contains("--timeout=300s"), "{sweep}");
    assert!(
        sweep.contains("curietech.ai/created-by=prod-release,curietech.ai/created-in=agent-ns"),
        "{sweep}"
    );
    assert!(sweep.contains("--ignore-not-found"), "{sweep}");
    assert!(!sweep.contains("finalize"), "{sweep}");
    assert!(!sweep.contains("--force"), "{sweep}");
}

#[tokio::test]
async fn cluster_down_names_a_remaining_configmap_and_exits_transient() {
    let original_path = std::env::var_os("PATH");

    // Scenario A: the sweep wait expires and a ConfigMap named hold is still
    // in the terminating namespace. The product must say so and exit 3, and
    // must not finalize or force the delete.
    let dir_a = tempfile::tempdir().expect("tempdir");
    let argv_log = dir_a.path().join("kubectl-argv.log");
    let present = dir_a.path().join("present.json");
    let terminating = dir_a.path().join("terminating.json");
    let hold = dir_a.path().join("hold.json");
    fs::write(&present, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&terminating, TERMINATING_NAMESPACE_LIST).expect("write terminating list");
    fs::write(&hold, HOLD_CONFIGMAP_LIST).expect("write hold configmap");
    test_executable::install_in(
        dir_a.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir_a.path(),
        "kubectl",
        &format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{argv}'
if [ "$1" = get ] && [ "$2" = namespace ] && [ "$3" = agent-ns ]; then
  cat '{present}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = jobs ]; then
  printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
  exit 0
fi
if [ "$1" = delete ]; then
  echo 'error: timed out waiting for the condition on namespaces/agent-ns' >&2
  exit 1
fi
if [ "$1" = get ] && [ "$2" = namespace ]; then
  for arg in "$@"; do
    if [ "$arg" = -l ]; then
      cat '{terminating}'
      exit 0
    fi
  done
fi
if [ "$1" = api-resources ]; then
  echo configmaps
  exit 0
fi
if [ "$1" = get ] && [ "$2" = configmaps ]; then
  cat '{hold}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            argv = argv_log.display(),
            present = present.display(),
            terminating = terminating.display(),
            hold = hold.display(),
        ),
    );
    prepend_path(dir_a.path());

    let err = down(down_opts())
        .await
        .expect_err("a sweep that times out with a namespace left is an incomplete teardown");
    let (class, _fix) = classify(&err);
    assert_eq!(
        class,
        ExitClass::Transient,
        "a bounded wait that expires is retryable (exit 3)"
    );
    assert_eq!(class.code(), 3);
    let shown = err.to_string();
    assert!(
        shown.contains("hold"),
        "the message must name the remaining object: {shown}"
    );
    assert!(
        shown.contains("agent-ns"),
        "the message must name the namespace: {shown}"
    );
    assert!(
        shown.contains("Terminating"),
        "the message must name the phase: {shown}"
    );
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert!(
        logged.contains("--timeout=300s"),
        "the sweep argv must carry the bound: {logged}"
    );
    assert!(
        !logged.contains("finalize"),
        "the sweep must not clear finalizers: {logged}"
    );
    assert!(
        !logged.contains("--force"),
        "the sweep must not force the delete: {logged}"
    );

    // Scenario B: a delete that finishes is not a wait expiry, so the product
    // must not inventory remaining objects.
    restore_path(&original_path);
    let dir_b = tempfile::tempdir().expect("tempdir");
    let marker = dir_b.path().join("api-resources-called");
    let present_b = dir_b.path().join("present.json");
    fs::write(&present_b, PRESENT_NAMESPACE).expect("write present namespace");
    test_executable::install_in(
        dir_b.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir_b.path(),
        "kubectl",
        &format!(
            r#"#!/bin/sh
if [ "$1" = get ] && [ "$2" = namespace ] && [ "$3" = agent-ns ]; then
  cat '{present}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = jobs ]; then
  printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
  exit 0
fi
if [ "$1" = delete ]; then
  printf '%s\n' 'namespace "agent-ns" deleted'
  exit 0
fi
if [ "$1" = api-resources ]; then
  touch '{marker}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            present = present_b.display(),
            marker = marker.display(),
        ),
    );
    prepend_path(dir_b.path());

    down(down_opts())
        .await
        .expect("a finished namespace delete is a complete teardown");
    assert!(
        !marker.exists(),
        "a successful sweep must not inventory remaining objects"
    );

    // Scenario C: an immediate RBAC refusal is not a 300s wait. Namespaces
    // may still be present, and the exit stays a permanent failure (#767).
    restore_path(&original_path);
    let dir_c = tempfile::tempdir().expect("tempdir");
    let present_c = dir_c.path().join("present.json");
    let terminating_c = dir_c.path().join("terminating.json");
    fs::write(&present_c, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&terminating_c, TERMINATING_NAMESPACE_LIST).expect("write terminating list");
    test_executable::install_in(
        dir_c.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir_c.path(),
        "kubectl",
        &format!(
            r#"#!/bin/sh
if [ "$1" = get ] && [ "$2" = namespace ] && [ "$3" = agent-ns ]; then
  cat '{present}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = jobs ]; then
  printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
  exit 0
fi
if [ "$1" = delete ]; then
  echo 'Error: namespaces is forbidden: User "x" cannot delete resource "namespaces"' >&2
  exit 1
fi
if [ "$1" = get ] && [ "$2" = namespace ]; then
  cat '{terminating}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            present = present_c.display(),
            terminating = terminating_c.display(),
        ),
    );
    prepend_path(dir_c.path());
    let err = down(down_opts())
        .await
        .expect_err("a forbidden sweep is an incomplete teardown");
    let (class, _fix) = classify(&err);
    assert_eq!(
        class,
        ExitClass::Failure,
        "an immediate RBAC refusal stays a permanent failure: {}",
        err
    );
    assert_eq!(class.code(), 1);
    assert!(
        !err.to_string().contains("waited 300s"),
        "an immediate refusal must not claim the wait expired: {err}"
    );

    // Scenario D: the sweep reports a wait expiry, then the namespace is
    // already gone. That is success, not a permanent failure.
    restore_path(&original_path);
    let dir_d = tempfile::tempdir().expect("tempdir");
    let present_d = dir_d.path().join("present.json");
    fs::write(&present_d, PRESENT_NAMESPACE).expect("write present namespace");
    test_executable::install_in(
        dir_d.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir_d.path(),
        "kubectl",
        &format!(
            r#"#!/bin/sh
if [ "$1" = get ] && [ "$2" = namespace ] && [ "$3" = agent-ns ]; then
  cat '{present}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = jobs ]; then
  printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
  exit 0
fi
if [ "$1" = delete ]; then
  echo 'error: timed out waiting for the condition on namespaces/agent-ns' >&2
  exit 1
fi
if [ "$1" = get ] && [ "$2" = namespace ]; then
  printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            present = present_d.display(),
        ),
    );
    prepend_path(dir_d.path());
    down(down_opts())
        .await
        .expect("a wait expiry that leaves no namespace is a completed teardown");

    // Scenario E: a permanent helm failure keeps exit 1 even when the sweep
    // wait expires and a namespace remains (#767).
    restore_path(&original_path);
    let dir_e = tempfile::tempdir().expect("tempdir");
    let present_e = dir_e.path().join("present.json");
    let terminating_e = dir_e.path().join("terminating.json");
    let hold_e = dir_e.path().join("hold.json");
    fs::write(&present_e, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&terminating_e, TERMINATING_NAMESPACE_LIST).expect("write terminating list");
    fs::write(&hold_e, HOLD_CONFIGMAP_LIST).expect("write hold configmap");
    test_executable::install_in(
        dir_e.path(),
        "helm",
        "#!/bin/sh\necho 'Error: uninstall: namespaces is forbidden: User \"x\" cannot delete resource \"namespaces\"' >&2\nexit 1\n",
    );
    test_executable::install_in(
        dir_e.path(),
        "kubectl",
        &format!(
            r#"#!/bin/sh
if [ "$1" = get ] && [ "$2" = namespace ] && [ "$3" = agent-ns ]; then
  cat '{present}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = jobs ]; then
  printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
  exit 0
fi
if [ "$1" = delete ]; then
  echo 'error: timed out waiting for the condition on namespaces/agent-ns' >&2
  exit 1
fi
if [ "$1" = get ] && [ "$2" = namespace ]; then
  for arg in "$@"; do
    if [ "$arg" = -l ]; then
      cat '{terminating}'
      exit 0
    fi
  done
fi
if [ "$1" = api-resources ]; then
  echo configmaps
  exit 0
fi
if [ "$1" = get ] && [ "$2" = configmaps ]; then
  cat '{hold}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            present = present_e.display(),
            terminating = terminating_e.display(),
            hold = hold_e.display(),
        ),
    );
    prepend_path(dir_e.path());
    let err = down(down_opts())
        .await
        .expect_err("a permanent helm failure stays incomplete");
    let (class, _fix) = classify(&err);
    assert_eq!(class, ExitClass::Failure, "{err}");
    assert_eq!(class.code(), 1);

    restore_path(&original_path);
}
