//! #4017: `cluster down` bounds the owned-namespace sweep and, when that wait
//! expires, names a remaining object instead of clearing finalizers (#707,
//! #767, #768).
//!
//! Each PATH-mutating test holds `support::ENV_LOCK` for the whole case,
//! including the extension-timing environment, so parallel tests cannot race
//! on PATH. The sync test below builds argv only and never touches PATH. It
//! is the #4017 fix pin. The #5735 pin is
//! `cluster_down_extends_for_controller_owned_residue`.

use crate::support::executable as test_executable;

use std::ffi::OsString;
use std::fs;
use std::os::unix::io::AsRawFd;
use std::path::Path;

use curie::exit::{classify, ExitClass};
use curie::ops::{down, down_commands, ClusterDownOutput, CommonOpts, DownOpts};

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

const EXTENSION_INTERVAL_ENV: &str = "CURIE_NAMESPACE_SWEEP_EXTENSION_INTERVAL_MS";
const EXTENSION_DEADLINE_ENV: &str = "CURIE_NAMESPACE_SWEEP_EXTENSION_DEADLINE_MS";

fn set_extension_timing(interval_ms: u64, deadline_ms: u64) {
    std::env::set_var(EXTENSION_INTERVAL_ENV, interval_ms.to_string());
    std::env::set_var(EXTENSION_DEADLINE_ENV, deadline_ms.to_string());
}

fn clear_extension_timing() {
    std::env::remove_var(EXTENSION_INTERVAL_ENV);
    std::env::remove_var(EXTENSION_DEADLINE_ENV);
}

/// Restores PATH and the extension-timing environment, including on panic.
struct RestoredProcessEnv {
    path: Option<OsString>,
}

impl RestoredProcessEnv {
    fn capture() -> Self {
        Self {
            path: std::env::var_os("PATH"),
        }
    }

    fn restore_now(&self) {
        restore_path(&self.path);
    }
}

impl Drop for RestoredProcessEnv {
    fn drop(&mut self) {
        restore_path(&self.path);
        clear_extension_timing();
    }
}

fn api_resource_calls(logged: &str) -> usize {
    logged
        .lines()
        .filter(|line| line.split_whitespace().next() == Some("api-resources"))
        .count()
}

fn resource_gets(logged: &str, resource: &str) -> usize {
    logged
        .lines()
        .filter(|line| {
            let args: Vec<&str> = line.split_whitespace().collect();
            args.first() == Some(&"get") && args.get(1) == Some(&resource)
        })
        .count()
}

fn labeled_namespace_lists(logged: &str) -> usize {
    logged
        .lines()
        .filter(|line| {
            let args: Vec<&str> = line.split_whitespace().collect();
            args.first() == Some(&"get")
                && args.get(1) == Some(&"namespace")
                && args.contains(&"-l")
        })
        .count()
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
    let _env = crate::support::ENV_LOCK.lock().await;
    let _restored = RestoredProcessEnv::capture();
    let original_path = std::env::var_os("PATH");
    // A mistaken extension must fail this control in well under five minutes.
    set_extension_timing(30, 400);

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
    assert!(
        shown.contains("waited 300s"),
        "a non-qualifying hold keeps the 300s wording: {shown}"
    );
    assert!(
        !shown.contains("waited 600s"),
        "a non-qualifying hold must not report the extension: {shown}"
    );
    assert_eq!(
        labeled_namespace_lists(&logged),
        1,
        "a non-qualifying hold must not poll the extension: {logged}"
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

    clear_extension_timing();
    restore_path(&original_path);
}

const QUALIFYING_NAMESPACE_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"agent-ns","deletionTimestamp":"2026-01-01T00:00:00Z"},"status":{"phase":"Terminating","conditions":[{"type":"NamespaceDeletionDiscoveryFailure","status":"False"},{"type":"NamespaceDeletionGroupVersionParsingFailure","status":"False"},{"type":"NamespaceDeletionContentFailure","status":"False"},{"type":"NamespaceContentRemaining","status":"False"},{"type":"NamespaceFinalizersRemaining","status":"False"}]}}]}"#;

const EVENT_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"Event","metadata":{"name":"evt-a","namespace":"agent-ns"}}]}"#;

const ENDPOINTS_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"Endpoints","metadata":{"name":"svc-a","namespace":"agent-ns"}}]}"#;

const ENDPOINTSLICE_LIST: &str = r#"{"apiVersion":"discovery.k8s.io/v1","kind":"List","items":[{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSlice","metadata":{"name":"svc-a","namespace":"agent-ns"}}]}"#;

const ROOT_CA_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"kube-root-ca.crt","namespace":"agent-ns"}}]}"#;

/// `empty_on_fourth` makes the fourth labeled namespace list empty. The first
/// list is the initial diagnosis and the next three are extension polls.
fn install_qualifying_kubectl(dir: &Path, empty_on_fourth: bool) -> std::path::PathBuf {
    let argv_log = dir.join("kubectl-argv.log");
    let present = dir.join("present.json");
    let qualifying = dir.join("qualifying.json");
    let events = dir.join("events.json");
    let endpoints = dir.join("endpoints.json");
    let slices = dir.join("slices.json");
    let root_ca = dir.join("root-ca.json");
    let count = dir.join("list-count");
    fs::write(&present, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&qualifying, QUALIFYING_NAMESPACE_LIST).expect("write qualifying list");
    fs::write(&events, EVENT_LIST).expect("write events");
    fs::write(&endpoints, ENDPOINTS_LIST).expect("write endpoints");
    fs::write(&slices, ENDPOINTSLICE_LIST).expect("write endpoint slices");
    fs::write(&root_ca, ROOT_CA_LIST).expect("write root ca");
    let list_body = if empty_on_fourth {
        format!(
            r#"n=0
if [ -f '{count}' ]; then
  n=$(wc -l < '{count}' | tr -d ' ')
fi
printf '\n' >> '{count}'
if [ "$n" -ge 3 ]; then
  printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
  exit 0
fi
cat '{qualifying}'
exit 0"#,
            count = count.display(),
            qualifying = qualifying.display(),
        )
    } else {
        format!(
            "cat '{qualifying}'\nexit 0",
            qualifying = qualifying.display()
        )
    };
    test_executable::install_in(
        dir,
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir,
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
  echo 'error: timed out waiting for the condition on namespaces/agent-ns' | tee -a '{argv}' >&2
  exit 1
fi
if [ "$1" = get ] && [ "$2" = namespace ]; then
  for arg in "$@"; do
    if [ "$arg" = -l ]; then
      {list_body}
    fi
  done
fi
if [ "$1" = api-resources ]; then
  printf '%s\n' events endpoints endpointslices configmaps
  exit 0
fi
if [ "$1" = get ] && [ "$2" = events ]; then
  cat '{events}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpoints ]; then
  cat '{endpoints}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpointslices ]; then
  cat '{slices}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = configmaps ]; then
  cat '{root_ca}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            argv = argv_log.display(),
            present = present.display(),
            events = events.display(),
            endpoints = endpoints.display(),
            slices = slices.display(),
            root_ca = root_ca.display(),
            list_body = list_body,
        ),
    );
    argv_log
}

#[tokio::test]
async fn cluster_down_extends_for_controller_owned_residue() {
    let _env = crate::support::ENV_LOCK.lock().await;
    let _restored = RestoredProcessEnv::capture();
    let original_path = std::env::var_os("PATH");
    set_extension_timing(30, 15_000);
    let started = std::time::Instant::now();
    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = install_qualifying_kubectl(dir.path(), true);
    prepend_path(dir.path());

    down(down_opts())
        .await
        .expect("controller-owned residue that disappears is a completed sweep");
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert!(
        logged.contains("--timeout=300s"),
        "the initial sweep keeps the 300s argv: {logged}"
    );
    assert!(
        logged.contains("timed out waiting"),
        "the sweep delete must expire before the extension: {logged}"
    );
    assert_eq!(
        labeled_namespace_lists(&logged),
        4,
        "the list becomes empty on the third extension poll: {logged}"
    );
    assert!(!logged.contains("finalize"), "{logged}");
    assert!(!logged.contains("--force"), "{logged}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(30),
        "the injected deadline must not wait out the production extension: {:?}",
        started.elapsed()
    );

    clear_extension_timing();
    restore_path(&original_path);
}

#[tokio::test]
async fn cluster_down_extension_expiry_stays_transient() {
    let _env = crate::support::ENV_LOCK.lock().await;
    let _restored = RestoredProcessEnv::capture();
    let original_path = std::env::var_os("PATH");
    set_extension_timing(40, 2_000);
    let started = std::time::Instant::now();
    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = install_qualifying_kubectl(dir.path(), false);
    prepend_path(dir.path());

    let err = down(down_opts())
        .await
        .expect_err("an extension that never clears the namespace stays incomplete");
    let (class, _fix) = classify(&err);
    assert_eq!(class, ExitClass::Transient, "{err}");
    assert_eq!(class.code(), 3);
    let shown = err.to_string();
    assert!(
        shown.contains("waited 600s"),
        "expiry reports the production 600s bound: {shown}"
    );
    assert!(
        shown.contains("agent-ns"),
        "expiry names the namespace: {shown}"
    );
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert!(
        labeled_namespace_lists(&logged) > 1,
        "expiry must poll before giving up: {logged}"
    );
    assert!(!logged.contains("finalize"), "{logged}");
    assert!(!logged.contains("--force"), "{logged}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(20),
        "a short injected deadline must not run for the production 300s: {:?}",
        started.elapsed()
    );

    clear_extension_timing();
    restore_path(&original_path);
}

const MIXED_NAMESPACE_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"agent-ns","deletionTimestamp":"2026-01-01T00:00:00Z"},"status":{"phase":"Terminating","conditions":[{"type":"NamespaceDeletionDiscoveryFailure","status":"False"},{"type":"NamespaceDeletionGroupVersionParsingFailure","status":"False"},{"type":"NamespaceDeletionContentFailure","status":"False"},{"type":"NamespaceContentRemaining","status":"False"},{"type":"NamespaceFinalizersRemaining","status":"False"}]}},{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"other-ns","deletionTimestamp":"2026-01-01T00:00:00Z"},"status":{"phase":"Terminating","conditions":[{"type":"NamespaceDeletionDiscoveryFailure","status":"False"},{"type":"NamespaceDeletionGroupVersionParsingFailure","status":"False"},{"type":"NamespaceDeletionContentFailure","status":"False"},{"type":"NamespaceContentRemaining","status":"False"},{"type":"NamespaceFinalizersRemaining","status":"False"}]}}]}"#;

const POD_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"Pod","metadata":{"name":"web","namespace":"other-ns"}}]}"#;

const NAMELESS_EVENT_LIST: &str = r#"{"apiVersion":"v1","kind":"List","items":[{"apiVersion":"v1","kind":"Event","metadata":{"name":"evt-a","namespace":"agent-ns"}},{"apiVersion":"v1","kind":"Event","metadata":{}}]}"#;

const CUSTOM_EVENT_LIST: &str = r#"{"apiVersion":"example.com/v1","kind":"List","items":[{"apiVersion":"example.com/v1","kind":"Event","metadata":{"name":"custom-evt","namespace":"agent-ns"}}]}"#;

struct CapturedStderr {
    saved: libc::c_int,
    path: std::path::PathBuf,
}

impl CapturedStderr {
    fn start(path: &Path) -> Self {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .expect("open stderr capture");
        let saved = unsafe { libc::dup(libc::STDERR_FILENO) };
        assert!(saved >= 0, "dup stderr");
        let redirected = unsafe { libc::dup2(file.as_raw_fd(), libc::STDERR_FILENO) };
        assert!(redirected >= 0, "redirect stderr");
        Self {
            saved,
            path: path.to_path_buf(),
        }
    }

    fn finish(mut self) -> String {
        let _ = std::io::Write::flush(&mut std::io::stderr());
        self.restore();
        fs::read_to_string(&self.path).unwrap_or_default()
    }

    fn restore(&mut self) {
        if self.saved >= 0 {
            unsafe {
                libc::dup2(self.saved, libc::STDERR_FILENO);
                libc::close(self.saved);
            }
            self.saved = -1;
        }
    }
}

impl Drop for CapturedStderr {
    fn drop(&mut self) {
        self.restore();
    }
}

fn process_gone(pid: u32) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let output = std::process::Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "pid="])
            .output()
            .expect("inspect stalled child");
        if !output.status.success() {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

fn read_pid(path: &Path) -> u32 {
    fs::read_to_string(path)
        .unwrap_or_default()
        .trim()
        .parse()
        .unwrap_or(0)
}

#[tokio::test]
async fn cluster_down_extension_refuses_a_mixed_namespace() {
    let _env = crate::support::ENV_LOCK.lock().await;
    let _restored = RestoredProcessEnv::capture();
    set_extension_timing(30, 800);
    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = dir.path().join("kubectl-argv.log");
    let present = dir.path().join("present.json");
    let mixed = dir.path().join("mixed.json");
    let events = dir.path().join("events.json");
    let endpoints = dir.path().join("endpoints.json");
    let slices = dir.path().join("slices.json");
    let root_ca = dir.path().join("root-ca.json");
    let pod = dir.path().join("pod.json");
    fs::write(&present, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&mixed, MIXED_NAMESPACE_LIST).expect("write mixed list");
    fs::write(&events, EVENT_LIST).expect("write events");
    fs::write(&endpoints, ENDPOINTS_LIST).expect("write endpoints");
    fs::write(&slices, ENDPOINTSLICE_LIST).expect("write slices");
    fs::write(&root_ca, ROOT_CA_LIST).expect("write root ca");
    fs::write(&pod, POD_LIST).expect("write pod");
    test_executable::install_in(
        dir.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir.path(),
        "kubectl",
        &format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{argv}'
ns=""
prev=""
for arg in "$@"; do
  if [ "$prev" = -n ]; then
    ns=$arg
  fi
  prev=$arg
done
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
      cat '{mixed}'
      exit 0
    fi
  done
fi
if [ "$1" = api-resources ]; then
  printf '%s\n' events endpoints endpointslices configmaps pods
  exit 0
fi
if [ "$1" = get ] && [ "$2" = pods ]; then
  if [ "$ns" = other-ns ]; then
    cat '{pod}'
  else
    printf '%s\n' '{{"apiVersion":"v1","kind":"List","items":[]}}'
  fi
  exit 0
fi
if [ "$1" = get ] && [ "$2" = events ]; then
  cat '{events}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpoints ]; then
  cat '{endpoints}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpointslices ]; then
  cat '{slices}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = configmaps ]; then
  cat '{root_ca}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            argv = argv_log.display(),
            present = present.display(),
            mixed = mixed.display(),
            pod = pod.display(),
            events = events.display(),
            endpoints = endpoints.display(),
            slices = slices.display(),
            root_ca = root_ca.display(),
        ),
    );
    prepend_path(dir.path());

    let err = down(down_opts())
        .await
        .expect_err("one non-qualifying namespace refuses the extension");
    let (class, _fix) = classify(&err);
    assert_eq!(class, ExitClass::Transient, "{err}");
    assert_eq!(class.code(), 3);
    let shown = err.to_string();
    assert!(shown.contains("waited 300s"), "{shown}");
    assert!(!shown.contains("waited 600s"), "{shown}");
    assert!(shown.contains("agent-ns"), "{shown}");
    assert!(shown.contains("other-ns"), "{shown}");
    assert!(shown.contains("web"), "{shown}");
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert_eq!(labeled_namespace_lists(&logged), 1, "{logged}");
    assert_eq!(api_resource_calls(&logged), 1, "{logged}");
    assert!(!logged.contains("finalize"), "{logged}");
    assert!(!logged.contains("--force"), "{logged}");
}

#[tokio::test]
async fn cluster_down_extension_refuses_incomplete_inventory() {
    let _env = crate::support::ENV_LOCK.lock().await;
    let restored = RestoredProcessEnv::capture();
    set_extension_timing(30, 800);

    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = dir.path().join("kubectl-argv.log");
    let present = dir.path().join("present.json");
    let qualifying = dir.path().join("qualifying.json");
    let nameless = dir.path().join("nameless.json");
    let endpoints = dir.path().join("endpoints.json");
    let slices = dir.path().join("slices.json");
    let root_ca = dir.path().join("root-ca.json");
    fs::write(&present, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&qualifying, QUALIFYING_NAMESPACE_LIST).expect("write qualifying list");
    fs::write(&nameless, NAMELESS_EVENT_LIST).expect("write nameless events");
    fs::write(&endpoints, ENDPOINTS_LIST).expect("write endpoints");
    fs::write(&slices, ENDPOINTSLICE_LIST).expect("write slices");
    fs::write(&root_ca, ROOT_CA_LIST).expect("write root ca");
    test_executable::install_in(
        dir.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir.path(),
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
      cat '{qualifying}'
      exit 0
    fi
  done
fi
if [ "$1" = api-resources ]; then
  printf '%s\n' events endpoints endpointslices configmaps
  exit 0
fi
if [ "$1" = get ] && [ "$2" = events ]; then
  cat '{nameless}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpoints ]; then
  cat '{endpoints}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpointslices ]; then
  cat '{slices}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = configmaps ]; then
  cat '{root_ca}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            argv = argv_log.display(),
            present = present.display(),
            qualifying = qualifying.display(),
            nameless = nameless.display(),
            endpoints = endpoints.display(),
            slices = slices.display(),
            root_ca = root_ca.display(),
        ),
    );
    prepend_path(dir.path());
    let err = down(down_opts())
        .await
        .expect_err("an object without a name cannot qualify");
    let (class, _fix) = classify(&err);
    assert_eq!(class, ExitClass::Transient, "{err}");
    assert_eq!(class.code(), 3);
    let shown = err.to_string();
    assert!(shown.contains("waited 300s"), "{shown}");
    assert!(!shown.contains("waited 600s"), "{shown}");
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert_eq!(labeled_namespace_lists(&logged), 1, "{logged}");
    assert!(!logged.contains("finalize"), "{logged}");
    assert!(!logged.contains("--force"), "{logged}");

    restored.restore_now();
    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = dir.path().join("kubectl-argv.log");
    let present = dir.path().join("present.json");
    let qualifying = dir.path().join("qualifying.json");
    let events = dir.path().join("events.json");
    let endpoints = dir.path().join("endpoints.json");
    let slices = dir.path().join("slices.json");
    let root_ca = dir.path().join("root-ca.json");
    let custom = dir.path().join("custom.json");
    fs::write(&present, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&qualifying, QUALIFYING_NAMESPACE_LIST).expect("write qualifying list");
    fs::write(&events, EVENT_LIST).expect("write events");
    fs::write(&endpoints, ENDPOINTS_LIST).expect("write endpoints");
    fs::write(&slices, ENDPOINTSLICE_LIST).expect("write slices");
    fs::write(&root_ca, ROOT_CA_LIST).expect("write root ca");
    fs::write(&custom, CUSTOM_EVENT_LIST).expect("write custom event");
    test_executable::install_in(
        dir.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir.path(),
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
      cat '{qualifying}'
      exit 0
    fi
  done
fi
if [ "$1" = api-resources ]; then
  printf '%s\n' events endpoints endpointslices configmaps widgets.example.com
  exit 0
fi
if [ "$1" = get ] && [ "$2" = events ]; then
  cat '{events}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpoints ]; then
  cat '{endpoints}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpointslices ]; then
  cat '{slices}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = configmaps ]; then
  cat '{root_ca}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = widgets.example.com ]; then
  cat '{custom}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            argv = argv_log.display(),
            present = present.display(),
            qualifying = qualifying.display(),
            events = events.display(),
            endpoints = endpoints.display(),
            slices = slices.display(),
            root_ca = root_ca.display(),
            custom = custom.display(),
        ),
    );
    prepend_path(dir.path());
    let err = down(down_opts())
        .await
        .expect_err("a custom resource group cannot qualify");
    let (class, _fix) = classify(&err);
    assert_eq!(class, ExitClass::Transient, "{err}");
    assert_eq!(class.code(), 3);
    let shown = err.to_string();
    assert!(shown.contains("waited 300s"), "{shown}");
    assert!(!shown.contains("waited 600s"), "{shown}");
    assert!(shown.contains("custom-evt"), "{shown}");
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert_eq!(labeled_namespace_lists(&logged), 1, "{logged}");
    assert_eq!(api_resource_calls(&logged), 1, "{logged}");

    restored.restore_now();
    let dir = tempfile::tempdir().expect("tempdir");
    let present = dir.path().join("present.json");
    fs::write(&present, PRESENT_NAMESPACE).expect("write present namespace");
    test_executable::install_in(
        dir.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir.path(),
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
  printf '%s\n' '{{'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            present = present.display(),
        ),
    );
    prepend_path(dir.path());
    let err = down(down_opts())
        .await
        .expect_err("a malformed namespace list is not an empty sweep");
    let (class, _fix) = classify(&err);
    assert_eq!(class, ExitClass::Transient, "{err}");
    assert_eq!(class.code(), 3);
    let shown = err.to_string();
    assert!(shown.contains("waited 300s"), "{shown}");
    assert!(!shown.contains("waited 600s"), "{shown}");
}

#[tokio::test]
async fn cluster_down_extension_does_not_repeat_discovery() {
    let _env = crate::support::ENV_LOCK.lock().await;
    let _restored = RestoredProcessEnv::capture();
    set_extension_timing(30, 15_000);
    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = install_qualifying_kubectl(dir.path(), true);
    prepend_path(dir.path());

    down(down_opts())
        .await
        .expect("controller-owned residue that disappears is a completed sweep");
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert_eq!(
        api_resource_calls(&logged),
        1,
        "discovery runs once, at admission: {logged}"
    );
    assert_eq!(resource_gets(&logged, "events"), 1, "{logged}");
    assert_eq!(resource_gets(&logged, "endpoints"), 1, "{logged}");
    assert_eq!(resource_gets(&logged, "endpointslices"), 1, "{logged}");
    assert_eq!(resource_gets(&logged, "configmaps"), 1, "{logged}");
    assert_eq!(labeled_namespace_lists(&logged), 4, "{logged}");
}

#[tokio::test]
async fn cluster_down_extension_kills_a_stalled_child() {
    let _env = crate::support::ENV_LOCK.lock().await;
    let _restored = RestoredProcessEnv::capture();
    set_extension_timing(40, 800);
    let started = std::time::Instant::now();
    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = dir.path().join("kubectl-argv.log");
    let present = dir.path().join("present.json");
    let qualifying = dir.path().join("qualifying.json");
    let parent_pid = dir.path().join("parent.pid");
    let child_pid = dir.path().join("child.pid");
    fs::write(&present, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&qualifying, QUALIFYING_NAMESPACE_LIST).expect("write qualifying list");
    test_executable::install_in(
        dir.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir.path(),
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
      cat '{qualifying}'
      exit 0
    fi
  done
fi
if [ "$1" = api-resources ]; then
  echo $$ > '{parent_pid}'
  (sleep 8) &
  echo $! > '{child_pid}'
  sleep 8
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            argv = argv_log.display(),
            present = present.display(),
            qualifying = qualifying.display(),
            parent_pid = parent_pid.display(),
            child_pid = child_pid.display(),
        ),
    );
    prepend_path(dir.path());

    let err = down(down_opts())
        .await
        .expect_err("a stalled discovery read stays inside the post-timeout deadline");
    let elapsed = started.elapsed();
    assert!(
        elapsed < std::time::Duration::from_secs(4),
        "the stalled child must be reaped inside the deadline, elapsed {elapsed:?}"
    );
    let (class, _fix) = classify(&err);
    assert_eq!(class, ExitClass::Transient, "{err}");
    assert_eq!(class.code(), 3);
    let shown = err.to_string();
    assert!(shown.contains("waited 300s"), "{shown}");
    assert!(!shown.contains("waited 600s"), "{shown}");
    assert!(shown.contains("agent-ns"), "{shown}");
    let parent = read_pid(&parent_pid);
    let child = read_pid(&child_pid);
    assert!(parent > 0, "the stalled kubectl must record its pid");
    assert!(child > 0, "the stalled kubectl must record its grandchild");
    assert!(
        process_gone(parent),
        "stalled kubectl {parent} is still running"
    );
    assert!(
        process_gone(child),
        "stalled grandchild {child} is still running"
    );
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert!(logged.contains("--timeout=300s"), "{logged}");
    assert_eq!(labeled_namespace_lists(&logged), 1, "{logged}");
    assert!(!logged.contains("finalize"), "{logged}");
    assert!(!logged.contains("--force"), "{logged}");
}

#[tokio::test]
async fn cluster_down_extension_reports_removed() {
    let _env = crate::support::ENV_LOCK.lock().await;
    let _restored = RestoredProcessEnv::capture();
    set_extension_timing(30, 15_000);
    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = install_qualifying_kubectl(dir.path(), true);
    let stderr_path = dir.path().join("stderr.txt");
    prepend_path(dir.path());
    let captured = CapturedStderr::start(&stderr_path);

    let output = down(down_opts())
        .await
        .expect("controller-owned residue that disappears is a completed sweep");
    let shown = captured.finish();
    assert!(
        matches!(
            output,
            ClusterDownOutput::Down {
                release_was_absent: false
            }
        ),
        "the normal down entry must finish as a removed release"
    );
    assert!(
        shown.contains("sweeping namespaces: ok")
            || (shown.contains("sweeping namespaces") && shown.contains("removed")),
        "down must report the removed sweep: {shown}"
    );
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert!(
        logged.contains("timed out waiting"),
        "the removed result follows the timed-out delete: {logged}"
    );
}

#[tokio::test]
async fn cluster_down_extension_failed_list_keeps_the_last_namespaces() {
    let _env = crate::support::ENV_LOCK.lock().await;
    let _restored = RestoredProcessEnv::capture();
    set_extension_timing(40, 1_200);
    let started = std::time::Instant::now();
    let dir = tempfile::tempdir().expect("tempdir");
    let argv_log = dir.path().join("kubectl-argv.log");
    let present = dir.path().join("present.json");
    let qualifying = dir.path().join("qualifying.json");
    let events = dir.path().join("events.json");
    let endpoints = dir.path().join("endpoints.json");
    let slices = dir.path().join("slices.json");
    let root_ca = dir.path().join("root-ca.json");
    let count = dir.path().join("list-count");
    fs::write(&present, PRESENT_NAMESPACE).expect("write present namespace");
    fs::write(&qualifying, QUALIFYING_NAMESPACE_LIST).expect("write qualifying list");
    fs::write(&events, EVENT_LIST).expect("write events");
    fs::write(&endpoints, ENDPOINTS_LIST).expect("write endpoints");
    fs::write(&slices, ENDPOINTSLICE_LIST).expect("write slices");
    fs::write(&root_ca, ROOT_CA_LIST).expect("write root ca");
    test_executable::install_in(
        dir.path(),
        "helm",
        "#!/bin/sh\necho 'release uninstalled'\nexit 0\n",
    );
    test_executable::install_in(
        dir.path(),
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
      n=0
      if [ -f '{count}' ]; then
        n=$(wc -l < '{count}' | tr -d ' ')
      fi
      printf '\n' >> '{count}'
      if [ "$n" -ge 1 ]; then
        echo 'error: the server rejected the list' >&2
        exit 1
      fi
      cat '{qualifying}'
      exit 0
    fi
  done
fi
if [ "$1" = api-resources ]; then
  printf '%s\n' events endpoints endpointslices configmaps
  exit 0
fi
if [ "$1" = get ] && [ "$2" = events ]; then
  cat '{events}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpoints ]; then
  cat '{endpoints}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = endpointslices ]; then
  cat '{slices}'
  exit 0
fi
if [ "$1" = get ] && [ "$2" = configmaps ]; then
  cat '{root_ca}'
  exit 0
fi
echo "unexpected kubectl $*" >&2
exit 1
"#,
            argv = argv_log.display(),
            present = present.display(),
            qualifying = qualifying.display(),
            count = count.display(),
            events = events.display(),
            endpoints = endpoints.display(),
            slices = slices.display(),
            root_ca = root_ca.display(),
        ),
    );
    prepend_path(dir.path());

    let err = down(down_opts())
        .await
        .expect_err("a failed namespace list is not an empty sweep");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(8),
        "a failed poll must stay inside the injected deadline: {:?}",
        started.elapsed()
    );
    let (class, _fix) = classify(&err);
    assert_eq!(class, ExitClass::Transient, "{err}");
    assert_eq!(class.code(), 3);
    let shown = err.to_string();
    assert!(shown.contains("waited 600s"), "{shown}");
    assert!(shown.contains("agent-ns"), "{shown}");
    assert!(
        shown.contains("evt-a"),
        "expiry keeps the last admitted objects: {shown}"
    );
    let logged = fs::read_to_string(&argv_log).expect("read kubectl argv log");
    assert_eq!(api_resource_calls(&logged), 1, "{logged}");
    assert!(labeled_namespace_lists(&logged) > 1, "{logged}");
    assert!(!logged.contains("finalize"), "{logged}");
    assert!(!logged.contains("--force"), "{logged}");
}
