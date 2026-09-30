//! Real two-cluster proof for #1913. Requires CURIE_E2E_TWO_CLUSTER=1
//! and KUBECONFIG_A / KUBECONFIG_B pointing at disposable kubeconfigs.

use std::collections::BTreeMap;
use std::process::Command;

use curie::connectors::{self, prepare, sync};
use curie::secrets::{self, SecretScope};
use curie::ui::CliOutput;

fn kubectl(kubeconfig: &str, args: &[&str]) -> (bool, String, String) {
    let output = Command::new("kubectl")
        .args(args)
        .env("KUBECONFIG", kubeconfig)
        .output()
        .expect("kubectl");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

async fn identity_for(kubeconfig: &str) -> String {
    let previous = std::env::var_os("KUBECONFIG");
    std::env::set_var("KUBECONFIG", kubeconfig);
    let identity = connectors::discover_cluster_identity()
        .await
        .expect("cluster identity");
    match previous {
        Some(value) => std::env::set_var("KUBECONFIG", value),
        None => std::env::remove_var("KUBECONFIG"),
    }
    identity
}

#[tokio::test]
#[ignore = "requires CURIE_E2E_TWO_CLUSTER=1 and disposable KUBECONFIG_A and KUBECONFIG_B"]
async fn two_disposable_clusters_refuse_mismatch_and_warn_on_replace() {
    assert_eq!(
        std::env::var("CURIE_E2E_TWO_CLUSTER").expect("CURIE_E2E_TWO_CLUSTER"),
        "1"
    );
    let kube_a = std::env::var("KUBECONFIG_A").expect("KUBECONFIG_A");
    let kube_b = std::env::var("KUBECONFIG_B").expect("KUBECONFIG_B");

    let config = tempfile::tempdir().expect("config");
    std::env::set_var("CURIE_CONFIG_DIR", config.path());

    let id_a = identity_for(&kube_a).await;
    let id_b = identity_for(&kube_b).await;
    assert_ne!(
        id_a, id_b,
        "disposable clusters must have distinct identities"
    );
    assert!(id_a.starts_with("ca:"));
    assert!(id_b.starts_with("ca:"));

    let ns_a = "acme-1913-a";
    let ns_b = "acme-1913-b";
    let (ok, _, err) = kubectl(
        &kube_a,
        &[
            "create",
            "namespace",
            ns_a,
            "--dry-run=client",
            "-o",
            "name",
        ],
    );
    assert!(ok, "kubectl A reachable: {err}");
    let (ok, _, err) = kubectl(&kube_a, &["create", "namespace", ns_a]);
    assert!(ok || err.contains("AlreadyExists"), "create ns A: {err}");
    let (ok, _, err) = kubectl(&kube_b, &["create", "namespace", ns_b]);
    assert!(ok || err.contains("AlreadyExists"), "create ns B: {err}");

    let scope_a = SecretScope {
        cluster_identity: id_a,
        release: "acme".into(),
        namespace: ns_a.into(),
    };
    let scope_b = SecretScope {
        cluster_identity: id_b,
        release: "acme".into(),
        namespace: ns_b.into(),
    };
    secrets::save_scoped_value("K8S_WRITE_KUBECONFIG", &scope_a, "token-cluster-a", None).unwrap();

    let mismatch = prepare(
        &[],
        &BTreeMap::new(),
        "acme-bot-connector-secrets",
        &["K8S_WRITE_KUBECONFIG".to_string()],
        &scope_b,
        "acme-bot",
        &BTreeMap::new(),
    )
    .unwrap_err()
    .to_string();
    assert!(mismatch.contains("refusing to inject"));
    assert!(!mismatch.contains("token-cluster-a"));

    let prepared_a = prepare(
        &[],
        &BTreeMap::new(),
        "acme-bot-connector-secrets",
        &["K8S_WRITE_KUBECONFIG".to_string()],
        &scope_a,
        "acme-bot",
        &BTreeMap::new(),
    )
    .unwrap();
    let intent = prepared_a.write_intent().unwrap();
    assert!(intent.contains("K8S_WRITE_KUBECONFIG"));
    assert!(!intent.contains("token-cluster-a"));

    std::env::set_var("KUBECONFIG", &kube_a);
    let captured_a = connectors::bind_current_cluster(ns_a, "acme")
        .await
        .unwrap();
    let prepared_a = prepared_a.bind_target(captured_a).unwrap();
    // A switch after prepare must refuse before it can apply A's credential.
    std::env::set_var("KUBECONFIG", &kube_b);
    let switched = sync(prepared_a).await.unwrap_err().to_string();
    assert!(switched.contains("no longer matches"));
    let (ok, _, _) = kubectl(
        &kube_b,
        &["-n", ns_b, "get", "secret", "acme-bot-connector-secrets"],
    );
    assert!(
        !ok,
        "context switch must not write the captured credential to B"
    );

    std::env::set_var("KUBECONFIG", &kube_a);
    let prepared_a = prepare(
        &[],
        &BTreeMap::new(),
        "acme-bot-connector-secrets",
        &["K8S_WRITE_KUBECONFIG".to_string()],
        &scope_a,
        "acme-bot",
        &BTreeMap::new(),
    )
    .unwrap()
    .bind_target(
        connectors::bind_current_cluster(ns_a, "acme")
            .await
            .unwrap(),
    )
    .unwrap();
    let first = sync(prepared_a).await.unwrap();
    assert!(
        first.replaced_keys.is_empty(),
        "first write is not a replace"
    );
    assert_eq!(first.written_keys, vec!["K8S_WRITE_KUBECONFIG".to_string()]);

    let prepared_again = prepare(
        &[],
        &BTreeMap::new(),
        "acme-bot-connector-secrets",
        &["K8S_WRITE_KUBECONFIG".to_string()],
        &scope_a,
        "acme-bot",
        &BTreeMap::new(),
    )
    .unwrap();
    let replaced = sync(
        prepared_again
            .bind_target(
                connectors::bind_current_cluster(ns_a, "acme")
                    .await
                    .unwrap(),
            )
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        replaced.replaced_keys,
        vec!["K8S_WRITE_KUBECONFIG".to_string()]
    );

    let (ok, secret_json, err) = kubectl(
        &kube_a,
        &[
            "-n",
            ns_a,
            "get",
            "secret",
            "acme-bot-connector-secrets",
            "-o",
            "json",
        ],
    );
    assert!(ok, "secret exists on A: {err}");
    assert!(secret_json.contains("K8S_WRITE_KUBECONFIG"));
    // The live Secret holds base64, but our operator notes must not.
    let listed = secrets::list_output().unwrap().to_json().to_string();
    assert!(!listed.contains("token-cluster-a"));
    let _ = kube_b;
}
