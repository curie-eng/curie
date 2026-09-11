//! Live Kubernetes proof for #2350. Needs `CURIE_E2E_CLUSTER=1`. Not the
//! fix-pin binary: that one must compile against a reversed `connectors.rs`.

use std::collections::BTreeMap;
use std::process::Command;
use std::time::{Duration, Instant};

use curie::connectors::{bind_current_cluster, wait_for_connector_rollouts, ConnectorWorkload};
use curie::exit::{classify, ExitClass};
use serde_json::{json, Value};

fn live_cluster() -> bool {
    std::env::var("CURIE_E2E_CLUSTER").ok().as_deref() == Some("1")
}

fn kubectl(args: &[&str]) -> (bool, String, String) {
    let output = Command::new("kubectl")
        .args(args)
        .output()
        .expect("kubectl");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn apply_doc(namespace: &str, doc: &Value) {
    let body = serde_json::to_string(doc).expect("json");
    let mut child = Command::new("kubectl")
        .args(["-n", namespace, "apply", "-f", "-"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("apply");
    {
        use std::io::Write;
        child
            .stdin
            .as_mut()
            .expect("stdin")
            .write_all(body.as_bytes())
            .expect("write");
    }
    let output = child.wait_with_output().expect("wait apply");
    assert!(
        output.status.success(),
        "apply failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn minimal_deployment(name: &str, image: &str, command: &[&str]) -> Value {
    let pull = if image.contains("does-not-exist") {
        "Always"
    } else {
        "IfNotPresent"
    };
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": name,
            "labels": {"app.kubernetes.io/name": name}
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app.kubernetes.io/name": name}},
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/name": name}},
                "spec": {
                    "containers": [{
                        "name": "server",
                        "image": image,
                        "imagePullPolicy": pull,
                        "command": command,
                    }]
                }
            }
        }
    })
}

#[tokio::test]
async fn live_cluster_ready_crashloop_image_pull_timeout_and_empty() {
    if !live_cluster() {
        eprintln!("skipping: set CURIE_E2E_CLUSTER=1 for disposable Kubernetes proof");
        return;
    }
    let ns = format!(
        "curie-2350-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs()
    );
    let (ok, _, err) = kubectl(&["create", "namespace", &ns]);
    assert!(ok, "create namespace {ns}: {err}");
    struct DeleteNs(String);
    impl Drop for DeleteNs {
        fn drop(&mut self) {
            let _ = Command::new("kubectl")
                .args(["delete", "namespace", &self.0, "--wait=false"])
                .output();
        }
    }
    let _guard = DeleteNs(ns.clone());

    let target = bind_current_cluster(&ns, "curie")
        .await
        .expect("bind cluster");

    wait_for_connector_rollouts(
        &target,
        &ns,
        &[],
        &BTreeMap::new(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("empty wait");

    apply_doc(
        &ns,
        &minimal_deployment("mcp-sleep", "busybox:1.36", &["sleep", "3600"]),
    );
    wait_for_connector_rollouts(
        &target,
        &ns,
        &[ConnectorWorkload {
            connector: "sleep".into(),
            deployment: "mcp-sleep".into(),
        }],
        &BTreeMap::new(),
        Instant::now() + Duration::from_secs(60),
    )
    .await
    .expect("healthy sleep connector");

    let leak = "gho_this_is_not_a_real_token";
    apply_doc(
        &ns,
        &minimal_deployment(
            "mcp-crash",
            "busybox:1.36",
            &["sh", "-c", &format!("echo {leak}; http --port 8000")],
        ),
    );
    let mut secrets = BTreeMap::new();
    secrets.insert("GITHUB_PERSONAL_ACCESS_TOKEN".into(), leak.into());
    let err = wait_for_connector_rollouts(
        &target,
        &ns,
        &[ConnectorWorkload {
            connector: "github".into(),
            deployment: "mcp-crash".into(),
        }],
        &secrets,
        Instant::now() + Duration::from_secs(60),
    )
    .await
    .expect_err("crashloop must be nonzero");
    let text = format!("{err:#}");
    assert!(text.contains("connector github"), "{text}");
    assert!(!text.contains(leak), "secret leaked in {text}");
    let (class, fix) = classify(&err);
    assert_eq!(class, ExitClass::Failure);
    let fix = fix.expect("fix");
    assert!(fix.contains("kubectl -n"), "recovery command: {fix}");
    assert!(!fix.contains(leak), "secret leaked in fix {fix}");

    apply_doc(
        &ns,
        &minimal_deployment(
            "mcp-pull",
            "ghcr.io/example/does-not-exist:2350-no-such-tag",
            &["http"],
        ),
    );
    let err = wait_for_connector_rollouts(
        &target,
        &ns,
        &[ConnectorWorkload {
            connector: "pullfail".into(),
            deployment: "mcp-pull".into(),
        }],
        &BTreeMap::new(),
        Instant::now() + Duration::from_secs(60),
    )
    .await
    .expect_err("image pull must be nonzero");
    let text = format!("{err:#}");
    assert!(text.contains("connector pullfail"), "{text}");
    assert!(
        text.contains("ImagePull")
            || text.contains("ErrImagePull")
            || text.contains("InvalidImage"),
        "{text}"
    );

    let stuck = json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": "mcp-stuck",
            "labels": {"app.kubernetes.io/name": "mcp-stuck"}
        },
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app.kubernetes.io/name": "mcp-stuck"}},
            "template": {
                "metadata": {"labels": {"app.kubernetes.io/name": "mcp-stuck"}},
                "spec": {
                    "nodeSelector": {"kubernetes.io/hostname": "no-such-node-2350"},
                    "containers": [{
                        "name": "server",
                        "image": "busybox:1.36",
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["sleep", "3600"],
                    }]
                }
            }
        }
    });
    apply_doc(&ns, &stuck);
    let started = Instant::now();
    let err = wait_for_connector_rollouts(
        &target,
        &ns,
        &[ConnectorWorkload {
            connector: "stuck".into(),
            deployment: "mcp-stuck".into(),
        }],
        &BTreeMap::new(),
        Instant::now() + Duration::from_secs(4),
    )
    .await
    .expect_err("unschedulable wait must time out");
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "deadline must bound the wait"
    );
    let text = format!("{err:#}");
    assert!(text.contains("connector stuck"), "{text}");
    assert!(text.contains("timeout"), "{text}");

    let (ok, _, err) = kubectl(&["delete", "namespace", &ns, "--wait=true", "--timeout=60s"]);
    assert!(
        ok || err.contains("NotFound"),
        "delete namespace {ns}: {err}"
    );
}
