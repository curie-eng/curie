//! Fix pin for #2350: reversing `cli/src/connectors.rs` must leave this file
//! compiling against the pre-wait `sync()`, then fail this test. That is why
//! it uses only `prepare`/`sync` (already on main) plus a fake kubectl, not the
//! new wait helpers.

use curie::connectors::{bind_current_cluster, prepare, sync};
use curie::exit::{classify, ExitClass};
use serde_json::json;
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

static PATH_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn write_exec(dir: &Path, name: &str, body: &str) {
    let path = dir.join(name);
    fs::write(&path, body).expect("write fake kubectl");
    let mut perms = fs::metadata(&path).expect("stat").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&path, perms).expect("chmod");
}

fn prepend_path(dir: &Path) -> Option<OsString> {
    let previous = std::env::var_os("PATH");
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(&previous.clone().unwrap_or_default()));
    std::env::set_var("PATH", std::env::join_paths(paths).expect("PATH"));
    previous
}

fn restore_path(previous: Option<OsString>) {
    match previous {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
}

/// THE REGRESSION TEST for #2350. A crashlooping connector Deployment makes
/// `sync` nonzero and names the connector plus the recovery command.
#[tokio::test]
async fn crashloop_observation_fails_named_connector() {
    let _lock = PATH_LOCK.lock().await;
    let bin = tempfile::tempdir().expect("bin");
    write_exec(
        bin.path(),
        "kubectl",
        r#"#!/bin/sh
case " $* " in
  *" config view "*)
    printf '%s\n' '{"clusters":[{"cluster":{"server":"https://cluster-a.example.com","certificate-authority-data":"Y2EtYQ=="}}]}'
    exit 0
    ;;
  *" apply -f - "*)
    cat >/dev/null
    exit 0
    ;;
  *" delete "*)
    exit 0
    ;;
  *" get secret "*)
    printf '%s\n' 'Error from server (NotFound): secrets "x" not found' >&2
    exit 1
    ;;
  *" get replicasets "*)
    printf '%s\n' '{"items":[{"metadata":{"annotations":{"deployment.kubernetes.io/revision":"1"},"labels":{"pod-template-hash":"abc"}}}]}'
    exit 0
    ;;
  *" get pods "*)
    printf '%s\n' '{"items":[{"metadata":{"name":"mcp-github-xyz","labels":{"pod-template-hash":"abc"}},"status":{"containerStatuses":[{"name":"server","state":{"waiting":{"reason":"CrashLoopBackOff"}}}]}}]}'
    exit 0
    ;;
  *" get deployment "*)
    printf '%s\n' '{"metadata":{"generation":1},"spec":{"replicas":1},"status":{"observedGeneration":1,"replicas":1,"updatedReplicas":1,"readyReplicas":0,"availableReplicas":0}}'
    exit 0
    ;;
esac
printf 'unexpected kubectl invocation: %s\n' "$*" >&2
exit 64
"#,
    );
    let previous = prepend_path(bin.path());
    let result = async {
        let target = bind_current_cluster("curie", "curie")
            .await
            .expect("bind cluster");
        let deployment = "curie-acme-bot-mcp-github";
        let manifests = vec![json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": deployment},
        })];
        let mut mcp = BTreeMap::new();
        mcp.insert(
            "github".into(),
            json!({"url": "http://curie-acme-bot-mcp-github.curie.svc.cluster.local:8000/mcp"}),
        );
        let prepared = prepare(
            &manifests,
            &mcp,
            "",
            &[],
            &target.scope,
            "acme-bot",
            &BTreeMap::new(),
        )
        .expect("prepare")
        .bind_target(target)
        .expect("bind");
        sync(prepared).await
    }
    .await;
    restore_path(previous);
    let err = result.expect_err("crashloop must make sync nonzero");
    let text = format!("{err:#}");
    assert!(text.contains("connector github"), "{text}");
    assert!(text.contains("CrashLoopBackOff"), "{text}");
    let (class, fix) = classify(&err);
    assert_eq!(class, ExitClass::Failure);
    let fix = fix.expect("recovery command");
    assert!(
        fix.contains("kubectl -n curie logs deploy/curie-acme-bot-mcp-github --tail=50"),
        "{fix}"
    );
    assert!(fix.contains("curie cluster deploy"), "{fix}");
}
