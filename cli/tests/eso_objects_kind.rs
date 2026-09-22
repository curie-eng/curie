//! Real-cluster proof for the ESO object library. Skips unless
//! CURIE_E2E_ESO_KIND=1 and KUBECONFIG point at a disposable cluster.
//! ESO CRDs gate the dry-run test; CURIE_E2E_ESO_CONTROLLER=1 gates the
//! force-sync test.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use base64::Engine as _;
use serde_json::{json, Value};

use curie::provider::eso::{
    self, render_external_secret, render_push_secret, render_secret_store, render_service_account,
    Kubectl, KubectlOutput, SeedOutcome, StoreSpec, SyncEntry, SystemKubectl,
};
use curie::provider::{
    InventoryClass, InventoryEntry, RotationOwner, SecretMaterial, Store, UpdatePolicy,
};

fn enabled() -> Option<PathBuf> {
    if std::env::var("CURIE_E2E_ESO_KIND").ok().as_deref() != Some("1") {
        eprintln!("skipping: CURIE_E2E_ESO_KIND != 1");
        return None;
    }
    match std::env::var("KUBECONFIG") {
        Ok(v) if !v.is_empty() => Some(PathBuf::from(v)),
        _ => {
            eprintln!("skipping: KUBECONFIG unset");
            None
        }
    }
}

fn kubectl(kubeconfig: &PathBuf, args: &[&str], stdin: Option<&str>) -> (bool, String, String) {
    let mut child = Command::new("kubectl")
        .args(args)
        .env("KUBECONFIG", kubeconfig)
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("kubectl");
    if let Some(input) = stdin {
        child
            .stdin
            .take()
            .unwrap()
            .write_all(input.as_bytes())
            .unwrap();
    }
    let out = child.wait_with_output().unwrap();
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn must(kubeconfig: &PathBuf, args: &[&str], stdin: Option<&str>) -> String {
    let (ok, out, err) = kubectl(kubeconfig, args, stdin);
    assert!(ok, "kubectl {args:?} failed: {err}");
    out
}

fn nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

struct Namespace {
    name: String,
    kubeconfig: PathBuf,
}

impl Namespace {
    fn create(kubeconfig: &PathBuf, tag: &str) -> Self {
        let name = format!("test-eso-{tag}-{}", nanos() % 1_000_000_000);
        must(kubeconfig, &["create", "namespace", &name], None);
        Self {
            name,
            kubeconfig: kubeconfig.clone(),
        }
    }
}

impl Drop for Namespace {
    fn drop(&mut self) {
        let _ = kubectl(
            &self.kubeconfig,
            &["delete", "namespace", &self.name, "--wait=false"],
            None,
        );
    }
}

fn system(kubeconfig: &Path) -> SystemKubectl {
    SystemKubectl {
        context: None,
        kubeconfig: Some(kubeconfig.to_path_buf()),
        call_timeout: Some(Duration::from_secs(60)),
    }
}

fn b64(v: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(v)
}

fn stored(kubeconfig: &PathBuf, ns: &str, secret: &str, key: &str) -> Option<String> {
    let raw = must(
        kubeconfig,
        &["-n", ns, "get", "secret", secret, "-o", "json"],
        None,
    );
    let v: Value = serde_json::from_str(&raw).unwrap();
    v["data"].get(key).map(|d| {
        String::from_utf8(
            base64::engine::general_purpose::STANDARD
                .decode(d.as_str().unwrap())
                .unwrap(),
        )
        .unwrap()
    })
}

fn create_secret(kubeconfig: &PathBuf, ns: &str, name: &str) {
    let body = json!({
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": name, "namespace": ns},
        "type": "Opaque",
        "data": {"STATIC": b64("static-1")}
    });
    must(
        kubeconfig,
        &["-n", ns, "create", "-f", "-"],
        Some(&body.to_string()),
    );
}

fn patch_key(kubeconfig: &PathBuf, ns: &str, name: &str, value: &str) {
    let patch = json!({"stringData": {"K": value}}).to_string();
    must(
        kubeconfig,
        &[
            "-n",
            ns,
            "patch",
            "secret",
            name,
            "--type=merge",
            "-p",
            &patch,
        ],
        None,
    );
}

/// Delegates to real kubectl; right after the first successful `get`, writes
/// the key through a separate kubectl so the seed's preconditioned write is stale.
struct InterleavingKubectl {
    inner: SystemKubectl,
    kubeconfig: PathBuf,
    namespace: String,
    secret: String,
    value: String,
    fired: AtomicBool,
}

impl Kubectl for InterleavingKubectl {
    fn run(&self, args: &[String], stdin: Option<&[u8]>) -> Result<KubectlOutput> {
        let out = self.inner.run(args, stdin)?;
        if out.success
            && args.iter().any(|a| a == "get")
            && !self.fired.swap(true, Ordering::SeqCst)
        {
            patch_key(&self.kubeconfig, &self.namespace, &self.secret, &self.value);
        }
        Ok(out)
    }
}

const SEED: &str = "seed-value-never-wins";

#[test]
fn seed_never_overwrites_concurrent_write() {
    let Some(kc) = enabled() else { return };
    let ns = Namespace::create(&kc, "seed");
    create_secret(&kc, &ns.name, "target");
    let k = InterleavingKubectl {
        inner: system(&kc),
        kubeconfig: kc.clone(),
        namespace: ns.name.clone(),
        secret: "target".into(),
        value: "live-0".into(),
        fired: AtomicBool::new(false),
    };
    let outcome =
        eso::seed_key(&k, &ns.name, "target", "K", &SecretMaterial::new(SEED), 5).unwrap();
    assert_eq!(outcome, SeedOutcome::AlreadyPresent);
    assert_eq!(
        stored(&kc, &ns.name, "target", "K").as_deref(),
        Some("live-0")
    );
    assert_eq!(
        stored(&kc, &ns.name, "target", "STATIC").as_deref(),
        Some("static-1")
    );
}

/// The writer always writes, unconditionally, either before or after the
/// seed. If before, the seed must skip; if after, it overwrites the seed. So
/// the final value is the writer's in every interleaving unless the seed
/// clobbered a value it did not read.
#[test]
fn seed_racing_writers_final_value_is_always_the_writers() {
    let Some(kc) = enabled() else { return };
    let ns = Namespace::create(&kc, "race");
    for i in 0..20u32 {
        let name = format!("race-{i}");
        create_secret(&kc, &ns.name, &name);
        let (kc2, ns2, name2) = (kc.clone(), ns.name.clone(), name.clone());
        let delay = Duration::from_millis(((nanos() / 1000) % 400) as u64);
        let writer = std::thread::spawn(move || {
            std::thread::sleep(delay);
            patch_key(&kc2, &ns2, &name2, &format!("live-{i}"));
        });
        let outcome = eso::seed_key(
            &system(&kc),
            &ns.name,
            &name,
            "K",
            &SecretMaterial::new(SEED),
            10,
        )
        .unwrap();
        writer.join().unwrap();
        assert!(
            matches!(outcome, SeedOutcome::Added | SeedOutcome::AlreadyPresent),
            "iteration {i}"
        );
        let expected = format!("live-{i}");
        assert_eq!(
            stored(&kc, &ns.name, &name, "K").as_deref(),
            Some(expected.as_str()),
            "iteration {i}"
        );
        assert_eq!(
            stored(&kc, &ns.name, &name, "STATIC").as_deref(),
            Some("static-1")
        );
    }
}

fn crds_installed(kc: &PathBuf) -> bool {
    kubectl(
        kc,
        &["get", "crd", "externalsecrets.external-secrets.io"],
        None,
    )
    .0
}

fn inventory(
    logical: &str,
    target: &str,
    keys: &[&str],
    owner: RotationOwner,
    rotated: &[&str],
) -> InventoryEntry {
    InventoryEntry {
        logical_name: logical.into(),
        class: InventoryClass::External,
        target: target.into(),
        keys: keys.iter().map(|k| k.to_string()).collect(),
        consumers: vec![],
        rotation_owner: owner,
        update_policy: UpdatePolicy::Replace,
        store: Store::Sm,
        rotated_keys: rotated.iter().map(|k| k.to_string()).collect(),
        chart: None,
    }
}

#[test]
fn rendered_objects_pass_server_dry_run() {
    let Some(kc) = enabled() else { return };
    if !crds_installed(&kc) {
        eprintln!("skipping: ESO CRDs not installed");
        return;
    }
    let ns = Namespace::create(&kc, "dry");
    let spec = StoreSpec {
        name: "curie-aws".into(),
        namespace: ns.name.clone(),
        region: "us-east-1".into(),
        service_account: "curie-eso".into(),
        role_arn: "arn:aws:iam::000000000000:role/curie-eso-test".into(),
    };
    let static_entry = SyncEntry::from_inventory(
        &inventory(
            "platform",
            "curie-platform",
            &["A", "B"],
            RotationOwner::Sm,
            &[],
        ),
        "curie/test",
    )
    .unwrap();
    let split = SyncEntry::from_inventory(
        &inventory(
            "finance",
            "curie-finance",
            &["CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN"],
            RotationOwner::Workload("finance-agent".into()),
            &["REFRESH_TOKEN"],
        ),
        "curie/test",
    )
    .unwrap();
    let objects = vec![
        render_service_account(&spec),
        render_secret_store(&spec),
        render_external_secret(&static_entry, &ns.name, "curie-aws", "1h"),
        render_external_secret(&split, &ns.name, "curie-aws", "1h"),
        render_push_secret(&split, &ns.name, "curie-aws").unwrap(),
    ];
    for object in objects {
        must(
            &kc,
            &["-n", &ns.name, "apply", "--dry-run=server", "-f", "-"],
            Some(&object.to_string()),
        );
    }
}

fn fake_store(ns: &str, name: &str) -> Value {
    json!({
        "apiVersion": "external-secrets.io/v1",
        "kind": "SecretStore",
        "metadata": {"name": name, "namespace": ns},
        "spec": {"provider": {"fake": {"data": [
            {"key": "curie/test/platform", "value": "{\"A\":\"a1\",\"B\":\"b1\"}"}
        ]}}}
    })
}

#[test]
fn force_sync_against_fake_provider() {
    let Some(kc) = enabled() else { return };
    if std::env::var("CURIE_E2E_ESO_CONTROLLER").ok().as_deref() != Some("1") {
        eprintln!("skipping: CURIE_E2E_ESO_CONTROLLER != 1");
        return;
    }
    let ns = Namespace::create(&kc, "sync");
    let k = system(&kc);
    let entry = SyncEntry::from_inventory(
        &inventory(
            "platform",
            "curie-platform",
            &["A", "B"],
            RotationOwner::Sm,
            &[],
        ),
        "curie/test",
    )
    .unwrap();
    let es = render_external_secret(&entry, &ns.name, "fake-store", "1h");
    eso::apply(&k, &ns.name, &[fake_store(&ns.name, "fake-store"), es]).unwrap();
    eso::force_sync_and_wait(
        &k,
        &ns.name,
        "platform",
        Duration::from_secs(60),
        Duration::from_millis(500),
    )
    .expect("synced against fake provider");
    assert_eq!(
        stored(&kc, &ns.name, "curie-platform", "A").as_deref(),
        Some("a1")
    );

    let broken = SyncEntry::from_inventory(
        &inventory("broken", "curie-broken", &["A"], RotationOwner::Sm, &[]),
        "curie/test",
    )
    .unwrap();
    eso::apply(
        &k,
        &ns.name,
        &[render_external_secret(
            &broken,
            &ns.name,
            "missing-store",
            "1h",
        )],
    )
    .unwrap();
    let start = Instant::now();
    let err = eso::force_sync_and_wait(
        &k,
        &ns.name,
        "broken",
        Duration::from_secs(5),
        Duration::from_millis(250),
    )
    .expect_err("missing store must not report synced");
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "{:?}",
        start.elapsed()
    );
    assert!(format!("{err:#}").contains("broken"), "{err:#}");
}

#[test]
fn rollout_stamps_pod_template() {
    let Some(kc) = enabled() else { return };
    let ns = Namespace::create(&kc, "roll");
    let deploy = json!({
        "apiVersion": "apps/v1", "kind": "Deployment",
        "metadata": {"name": "consumer", "namespace": ns.name},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "consumer"}},
            "template": {
                "metadata": {"labels": {"app": "consumer"}},
                "spec": {"containers": [{"name": "pause", "image": "registry.k8s.io/pause:3.9"}]}
            }
        }
    });
    must(
        &kc,
        &["-n", &ns.name, "apply", "-f", "-"],
        Some(&deploy.to_string()),
    );
    eso::rollout_consumers(
        &system(&kc),
        &ns.name,
        &["consumer".to_string()],
        "v-test-1",
        Duration::from_secs(180),
    )
    .unwrap();
    let raw = must(
        &kc,
        &[
            "-n",
            &ns.name,
            "get",
            "deployment",
            "consumer",
            "-o",
            "json",
        ],
        None,
    );
    let d: Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(
        d["spec"]["template"]["metadata"]["annotations"][eso::PROVIDER_VERSION_ANNOTATION],
        json!("v-test-1")
    );
    let raw = must(
        &kc,
        &[
            "-n",
            &ns.name,
            "get",
            "pods",
            "-l",
            "app=consumer",
            "-o",
            "json",
        ],
        None,
    );
    let pods: Value = serde_json::from_str(&raw).unwrap();
    assert!(
        pods["items"].as_array().unwrap().iter().any(|p| {
            p["metadata"]["annotations"][eso::PROVIDER_VERSION_ANNOTATION] == json!("v-test-1")
        }),
        "no pod carries the stamp: {raw}"
    );
}
