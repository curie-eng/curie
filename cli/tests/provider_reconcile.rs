//! Cluster sync for a provider write: scripted kubectl, no live API.

use std::collections::BTreeMap;
use std::sync::Mutex;

use anyhow::Result;
use serde_json::json;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use curie::provider::eso::{synced_version, Kubectl, KubectlOutput, PROVIDER_VERSION_ANNOTATION};
use curie::provider::reconcile::{self, ObjectCheck, Reach};
use curie::provider::{
    InventoryClass, InventoryEntry, ObjectMetadata, ObjectVersion, RotationOwner, Store,
    UpdatePolicy,
};

struct Scripted {
    calls: Mutex<Vec<Vec<String>>>,
    annotation: Mutex<Option<String>>,
    namespace_present: bool,
    store_present: bool,
}

impl Scripted {
    fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().expect("calls").clone()
    }
}

impl Kubectl for Scripted {
    fn run(&self, args: &[String], _stdin: Option<&[u8]>) -> Result<KubectlOutput> {
        self.calls.lock().expect("calls").push(args.to_vec());
        let joined = args.join(" ");
        if joined.contains("get namespace") {
            return Ok(if self.namespace_present {
                ok("namespace/curie\n")
            } else {
                err("Error from server (NotFound): namespaces \"curie\" not found\n")
            });
        }
        if joined.contains("get secretstore") {
            return Ok(if self.store_present {
                ok("secretstore.external-secrets.io/acme-aws\n")
            } else {
                err("Error from server (NotFound): secretstores.external-secrets.io \"acme-aws\" not found\n")
            });
        }
        if joined.contains("annotate") {
            let value = args
                .iter()
                .find_map(|arg| arg.strip_prefix("force-sync="))
                .expect("force-sync annotation")
                .to_string();
            *self.annotation.lock().expect("annotation") = Some(value);
            return Ok(ok("externalsecret annotated\n"));
        }
        if joined.contains("get externalsecret") {
            let value = self
                .annotation
                .lock()
                .expect("annotation")
                .clone()
                .unwrap_or_default();
            let metadata = json!({
                "generation": 1,
                "annotations": { "force-sync": value },
                "labels": {},
            });
            let body = json!({
                "metadata": metadata,
                "status": {
                    "conditions": [{ "type": "Ready", "status": "True" }],
                    "syncedResourceVersion": synced_version(&metadata),
                },
            });
            return Ok(ok(&format!("{body}\n")));
        }
        if joined.contains("patch") || joined.contains("rollout status") || joined.contains("apply")
        {
            return Ok(ok("ok\n"));
        }
        if joined.contains("get deployment") {
            let stamp = args
                .iter()
                .any(|arg| arg == "acme-curie-api")
                .then_some("version-old");
            let body = json!({
                "spec": { "template": { "metadata": { "annotations": {
                    PROVIDER_VERSION_ANNOTATION: stamp,
                } } } }
            });
            return Ok(ok(&format!("{body}\n")));
        }
        if joined.contains("delete") {
            return Ok(ok("deleted\n"));
        }
        Ok(err("unexpected kubectl\n"))
    }
}

fn ok(stdout: &str) -> KubectlOutput {
    KubectlOutput {
        success: true,
        stdout: stdout.to_string(),
        stderr: String::new(),
    }
}

fn err(stderr: &str) -> KubectlOutput {
    KubectlOutput {
        success: false,
        stdout: String::new(),
        stderr: stderr.to_string(),
    }
}

fn entry() -> InventoryEntry {
    InventoryEntry {
        logical_name: "github-webhook-secret".to_string(),
        class: InventoryClass::Stateful,
        target: "{release}-curie-github-webhook".to_string(),
        keys: vec!["githubWebhookSecret".to_string()],
        consumers: vec!["api".to_string()],
        rotation_owner: RotationOwner::Sm,
        update_policy: UpdatePolicy::Replace,
        store: Store::Sm,
        rotated_keys: Vec::new(),
        chart: None,
    }
}

fn scripted(namespace_present: bool, store_present: bool) -> Scripted {
    Scripted {
        calls: Mutex::new(Vec::new()),
        annotation: Mutex::new(None),
        namespace_present,
        store_present,
    }
}

#[test]
fn no_client_is_unprovisioned_without_a_call() {
    let cluster = scripted(true, true);
    let reach = reconcile::reach(None, "curie", "acme-aws").expect("reach");
    assert_eq!(reach, Reach::NoKubeconfig);
    assert!(cluster.calls().is_empty());
}

#[test]
fn a_missing_store_does_not_publish() {
    let cluster = scripted(true, false);
    let reach = reconcile::reach(Some(&cluster), "curie", "acme-aws").expect("reach");
    assert_eq!(reach, Reach::StoreAbsent);
    assert!(cluster
        .calls()
        .iter()
        .all(|call| !call.iter().any(|arg| arg == "apply")));
}

#[test]
fn publish_rolls_only_the_inventory_consumer() {
    let cluster = scripted(true, true);
    let rolled = reconcile::publish(
        &cluster,
        "curie",
        "acme",
        "curie-aws-secrets-e2e/acme",
        &entry(),
        "version-new",
    )
    .expect("publish");
    assert_eq!(rolled, vec!["api".to_string()]);
    let patches: Vec<Vec<String>> = cluster
        .calls()
        .into_iter()
        .filter(|call| call.iter().any(|arg| arg == "patch"))
        .collect();
    assert_eq!(patches.len(), 1, "exactly one deployment is patched");
    assert!(patches[0].iter().any(|arg| arg == "acme-curie-api"));
    assert!(!patches[0].iter().any(|arg| arg.contains("worker")));
}

#[test]
fn check_reports_a_missing_key_and_a_stale_consumer() {
    let now = OffsetDateTime::parse("2026-09-23T00:00:00Z", &Rfc3339).unwrap();
    let metadata = ObjectMetadata {
        name: "github-webhook-secret".to_string(),
        version: ObjectVersion {
            id: "version-new".to_string(),
        },
        tags: BTreeMap::new(),
        key_names: vec!["other".to_string()],
    };
    let stamps = BTreeMap::from([("api".to_string(), Some("version-old".to_string()))]);
    let checked = reconcile::assess(&metadata, Some(&entry()), now, true, &stamps).expect("assess");
    assert_eq!(
        checked,
        ObjectCheck {
            name: "github-webhook-secret".to_string(),
            version: "version-new".to_string(),
            expires_at: None,
            status: "missing",
            missing_keys: vec!["githubWebhookSecret".to_string()],
            stale_consumers: vec!["api".to_string()],
        }
    );
}

#[test]
fn immutable_and_cluster_entries_are_refused_before_a_write() {
    let mut immutable = entry();
    immutable.update_policy = UpdatePolicy::Immutable;
    assert!(reconcile::refuses_set(&immutable, "githubWebhookSecret")
        .unwrap()
        .contains("immutable"));
    let mut cluster = entry();
    cluster.store = Store::Cluster;
    assert!(reconcile::refuses_set(&cluster, "githubWebhookSecret")
        .unwrap()
        .contains("stays in the cluster"));
}

#[test]
fn delete_removes_the_external_object() {
    let cluster = scripted(true, true);
    reconcile::delete_objects(&cluster, "curie", "github-webhook-secret").expect("delete");
    let kinds: Vec<String> = cluster
        .calls()
        .into_iter()
        .filter_map(|call| {
            call.iter()
                .any(|arg| arg == "delete")
                .then(|| call[call.iter().position(|arg| arg == "delete").unwrap() + 1].clone())
        })
        .collect();
    assert_eq!(
        kinds,
        vec!["externalsecret".to_string(), "pushsecret".to_string()]
    );
}
