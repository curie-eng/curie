//! Rotation-owner orchestration (ADR 0163 decision 7): read the rotated-key
//! backup, seed each rotated key create-if-absent, then apply the
//! ExternalSecret and PushSecret as one List. Kubectl is a scripted fake
//! (style: tests/eso_objects.rs); the provider is a fake SecretsProvider
//! (style: tests/provider_contract.rs). No process is ever shelled out to:
//! this module proves it works with no aws binary and no ESO.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use base64::Engine as _;
use serde_json::{json, Value};

use curie::provider::eso::{Kubectl, KubectlOutput, SyncEntry};
use curie::provider::rotation::{
    apply_sync_entry, read_backup, ApplyReport, KeyReport, KeySeed, SEED_ATTEMPTS,
};
use curie::provider::{
    InventoryClass, InventoryEntry, ObjectMetadata, ObjectVersion, ProviderError, PutRequest,
    RejectedReason, RotationOwner, SecretMaterial, SecretsProvider, Store, StoredObject,
    UpdatePolicy,
};

const NS: &str = "curie";
const STORE: &str = "curie-aws";
const PREFIX: &str = "curie/test";
const REFRESH: &str = "1h";
const BACKUP_VALUE: &str = "backup-value-9q7Zx";

fn entry(
    logical: &str,
    target: &str,
    keys: &[&str],
    owner: RotationOwner,
    rotated: &[&str],
) -> InventoryEntry {
    InventoryEntry {
        logical_name: logical.to_string(),
        class: InventoryClass::External,
        target: target.to_string(),
        keys: keys.iter().map(|k| k.to_string()).collect(),
        consumers: vec!["curie-api".to_string()],
        rotation_owner: owner,
        update_policy: UpdatePolicy::Replace,
        store: Store::Sm,
        rotated_keys: rotated.iter().map(|k| k.to_string()).collect(),
        chart: None,
    }
}

fn rotated_entry() -> SyncEntry {
    SyncEntry::from_inventory(
        &entry(
            "finance",
            "curie-finance",
            &["CLIENT_ID", "REFRESH_TOKEN"],
            RotationOwner::Workload("finance-agent".to_string()),
            &["REFRESH_TOKEN"],
        ),
        PREFIX,
    )
    .expect("rotated entry")
}

fn static_entry() -> SyncEntry {
    SyncEntry::from_inventory(
        &entry(
            "platform",
            "curie-platform",
            &["A", "B"],
            RotationOwner::Sm,
            &[],
        ),
        PREFIX,
    )
    .expect("static entry")
}

// ---------------------------------------------------------------- fake kubectl

struct KState {
    calls: Vec<(Vec<String>, Option<Vec<u8>>)>,
    secret: Option<Value>,
    rv: u64,
    get_error: Option<String>,
}

#[derive(Clone)]
struct FakeKubectl(Arc<Mutex<KState>>);

fn b64(value: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(value)
}

fn unb64(value: &str) -> String {
    String::from_utf8(
        base64::engine::general_purpose::STANDARD
            .decode(value)
            .unwrap(),
    )
    .unwrap()
}

fn ok(stdout: impl Into<String>) -> Result<KubectlOutput> {
    Ok(KubectlOutput {
        success: true,
        stdout: stdout.into(),
        stderr: String::new(),
    })
}

fn fail(stderr: impl Into<String>) -> Result<KubectlOutput> {
    Ok(KubectlOutput {
        success: false,
        stdout: String::new(),
        stderr: stderr.into(),
    })
}

fn has(args: &[String], token: &str) -> bool {
    args.iter().any(|a| a == token)
}

impl FakeKubectl {
    fn new(secret: Option<Value>) -> Self {
        let mut state = KState {
            calls: Vec::new(),
            secret: None,
            rv: 100,
            get_error: None,
        };
        if let Some(mut s) = secret {
            state.rv += 1;
            s["metadata"]["resourceVersion"] = json!(state.rv.to_string());
            state.secret = Some(s);
        }
        Self(Arc::new(Mutex::new(state)))
    }

    fn with_get_error(self, stderr: &str) -> Self {
        self.0.lock().unwrap().get_error = Some(stderr.to_string());
        self
    }

    fn calls(&self) -> Vec<(Vec<String>, Option<Vec<u8>>)> {
        self.0.lock().unwrap().calls.clone()
    }

    fn stored_value(&self, key: &str) -> Option<String> {
        let s = self.0.lock().unwrap();
        s.secret.as_ref()?["data"]
            .get(key)
            .map(|v| unb64(v.as_str().unwrap()))
    }

    fn apply_call_index(&self) -> Option<usize> {
        self.calls()
            .iter()
            .position(|(a, _)| has(a, "apply") && has(a, "--server-side"))
    }

    fn create_call_index(&self) -> Option<usize> {
        self.calls().iter().position(|(a, _)| has(a, "create"))
    }

    fn replace_call_index(&self) -> Option<usize> {
        self.calls().iter().position(|(a, _)| has(a, "replace"))
    }
}

impl Kubectl for FakeKubectl {
    fn run(&self, args: &[String], stdin: Option<&[u8]>) -> Result<KubectlOutput> {
        let mut s = self.0.lock().unwrap();
        s.calls.push((args.to_vec(), stdin.map(<[u8]>::to_vec)));

        if has(args, "apply") && has(args, "--server-side") {
            return ok("applied");
        }
        if has(args, "get") && has(args, "secret") {
            if let Some(err) = s.get_error.clone() {
                return fail(err);
            }
            return match &s.secret {
                Some(v) => ok(v.to_string()),
                None => fail("Error from server (NotFound): secrets \"curie-finance\" not found"),
            };
        }
        if has(args, "create") {
            let mut body: Value = serde_json::from_slice(stdin.expect("create stdin")).unwrap();
            if s.secret.is_some() {
                return fail(
                    "Error from server (AlreadyExists): secrets \"curie-finance\" already exists",
                );
            }
            s.rv += 1;
            body["metadata"]["resourceVersion"] = json!(s.rv.to_string());
            s.secret = Some(body);
            return ok("created");
        }
        if has(args, "replace") {
            let mut body: Value = serde_json::from_slice(stdin.expect("replace stdin")).unwrap();
            let current_rv = s.secret.as_ref().and_then(|c| {
                c["metadata"]["resourceVersion"]
                    .as_str()
                    .map(str::to_string)
            });
            let incoming_rv = body["metadata"]["resourceVersion"]
                .as_str()
                .map(str::to_string);
            if current_rv != incoming_rv {
                return fail(
                    "Error from server (Conflict): the object has been modified; please apply your changes to the latest version and try again",
                );
            }
            s.rv += 1;
            body["metadata"]["resourceVersion"] = json!(s.rv.to_string());
            s.secret = Some(body);
            return ok("replaced");
        }
        panic!("FakeKubectl: unexpected call {args:?}");
    }
}

fn existing_secret_with(key: &str, value: &str) -> Value {
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": { "name": "curie-finance", "namespace": NS },
        "type": "Opaque",
        "data": { key: b64(value) },
    })
}

// ---------------------------------------------------------------- fake provider

struct FakeProvider {
    backups: Mutex<HashMap<String, String>>,
    unavailable: Mutex<HashSet<String>>,
    calls: Mutex<Vec<String>>,
    forbid_calls: bool,
}

impl FakeProvider {
    fn empty() -> Self {
        Self {
            backups: Mutex::new(HashMap::new()),
            unavailable: Mutex::new(HashSet::new()),
            calls: Mutex::new(Vec::new()),
            forbid_calls: false,
        }
    }

    fn forbidding_calls() -> Self {
        Self {
            forbid_calls: true,
            ..Self::empty()
        }
    }

    fn with_backup(self, name: &str, json_body: &str) -> Self {
        self.backups
            .lock()
            .unwrap()
            .insert(name.to_string(), json_body.to_string());
        self
    }

    fn with_unavailable(self, name: &str) -> Self {
        self.unavailable.lock().unwrap().insert(name.to_string());
        self
    }

    fn call_count(&self) -> usize {
        self.calls.lock().unwrap().len()
    }
}

impl SecretsProvider for FakeProvider {
    fn put(&self, _request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError> {
        panic!("rotation must never call put")
    }

    fn get(&self, name: &str, _version: Option<&str>) -> Result<StoredObject, ProviderError> {
        if self.forbid_calls {
            panic!("provider must not be called for a static entry");
        }
        self.calls.lock().unwrap().push(name.to_string());
        if self.unavailable.lock().unwrap().contains(name) {
            return Err(ProviderError::Unavailable {
                name: name.to_string(),
                status: 503,
            });
        }
        match self.backups.lock().unwrap().get(name) {
            Some(body) => Ok(StoredObject {
                version: ObjectVersion {
                    id: "1".to_string(),
                },
                material: SecretMaterial::new(body.clone()),
                key_names: vec![],
            }),
            None => Err(ProviderError::NotFound {
                name: name.to_string(),
            }),
        }
    }

    fn get_metadata(&self, _name: &str) -> Result<ObjectMetadata, ProviderError> {
        panic!("rotation must never call get_metadata")
    }

    fn list(&self, _prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError> {
        panic!("rotation must never call list")
    }

    fn tag(
        &self,
        _name: &str,
        _tags: &BTreeMap<String, String>,
        _expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        panic!("rotation must never call tag")
    }

    fn delete(
        &self,
        _name: &str,
        _expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        panic!("rotation must never call delete")
    }
}

// ---------------------------------------------------------------- apply_sync_entry

#[test]
fn no_backup_reports_no_backup_and_still_applies() {
    let entry = rotated_entry();
    let provider = FakeProvider::empty();
    let k = FakeKubectl::new(None);
    let report = apply_sync_entry(&k, &provider, &entry, NS, STORE, REFRESH).expect("apply");
    assert_eq!(
        report,
        ApplyReport {
            entry: "finance".to_string(),
            seeds: vec![KeyReport {
                key: "REFRESH_TOKEN".to_string(),
                outcome: KeySeed::NoBackup,
            }],
        }
    );
    assert!(k.create_call_index().is_none());
    assert!(k.replace_call_index().is_none());
    let apply_idx = k.apply_call_index().expect("apply call happened");
    let (args, stdin) = &k.calls()[apply_idx];
    assert!(has(args, "-f"));
    let list: Value = serde_json::from_slice(stdin.as_ref().unwrap()).unwrap();
    let kinds: Vec<&str> = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["kind"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"ExternalSecret"));
    assert!(kinds.contains(&"PushSecret"));
    let external = list["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["kind"] == "ExternalSecret")
        .unwrap();
    assert_eq!(
        external["spec"]["target"]["creationPolicy"],
        "CreateOrMerge"
    );
}

#[test]
fn backup_present_secret_absent_creates_before_apply_and_never_leaks_the_value() {
    let entry = rotated_entry();
    let backup_json = format!(r#"{{"REFRESH_TOKEN":"{BACKUP_VALUE}"}}"#);
    let provider = FakeProvider::empty().with_backup(&entry.backup_key(), &backup_json);
    let k = FakeKubectl::new(None);
    let report = apply_sync_entry(&k, &provider, &entry, NS, STORE, REFRESH).expect("apply");
    assert_eq!(
        report.seeds,
        vec![KeyReport {
            key: "REFRESH_TOKEN".to_string(),
            outcome: KeySeed::Created,
        }]
    );
    let create_idx = k.create_call_index().expect("create call happened");
    let apply_idx = k.apply_call_index().expect("apply call happened");
    assert!(create_idx < apply_idx, "create must precede apply");
    assert_eq!(
        k.stored_value("REFRESH_TOKEN").as_deref(),
        Some(BACKUP_VALUE)
    );

    // The value never appears in argv or in the report's Debug output.
    for (args, _) in k.calls() {
        for a in &args {
            assert!(!a.contains(BACKUP_VALUE), "value leaked into argv: {a}");
        }
    }
    assert!(!format!("{report:?}").contains(BACKUP_VALUE));
}

#[test]
fn backup_present_secret_has_only_static_key_adds_rotated_key_and_preserves_sibling() {
    let entry = rotated_entry();
    let backup_json = format!(r#"{{"REFRESH_TOKEN":"{BACKUP_VALUE}"}}"#);
    let provider = FakeProvider::empty().with_backup(&entry.backup_key(), &backup_json);
    let existing = existing_secret_with("CLIENT_ID", "client-id-value");
    let k = FakeKubectl::new(Some(existing));
    let report = apply_sync_entry(&k, &provider, &entry, NS, STORE, REFRESH).expect("apply");
    assert_eq!(
        report.seeds,
        vec![KeyReport {
            key: "REFRESH_TOKEN".to_string(),
            outcome: KeySeed::Added,
        }]
    );
    let replace_idx = k.replace_call_index().expect("replace call happened");
    let apply_idx = k.apply_call_index().expect("apply call happened");
    assert!(replace_idx < apply_idx);
    assert_eq!(
        k.stored_value("CLIENT_ID").as_deref(),
        Some("client-id-value")
    );
    assert_eq!(
        k.stored_value("REFRESH_TOKEN").as_deref(),
        Some(BACKUP_VALUE)
    );
}

#[test]
fn live_value_different_from_backup_is_never_overwritten() {
    let entry = rotated_entry();
    let backup_json = format!(r#"{{"REFRESH_TOKEN":"{BACKUP_VALUE}"}}"#);
    let provider = FakeProvider::empty().with_backup(&entry.backup_key(), &backup_json);
    let existing = existing_secret_with("REFRESH_TOKEN", "live-current-value");
    let k = FakeKubectl::new(Some(existing));
    let report = apply_sync_entry(&k, &provider, &entry, NS, STORE, REFRESH).expect("apply");
    assert_eq!(
        report.seeds,
        vec![KeyReport {
            key: "REFRESH_TOKEN".to_string(),
            outcome: KeySeed::AlreadyPresent,
        }]
    );
    assert!(k.create_call_index().is_none());
    assert!(k.replace_call_index().is_none());
    assert_eq!(
        k.stored_value("REFRESH_TOKEN").as_deref(),
        Some("live-current-value")
    );
    assert!(k.apply_call_index().is_some());
}

#[test]
fn backup_missing_key_or_empty_value_reports_not_in_backup_and_writes_nothing() {
    for backup_json in [
        r#"{"OTHER_KEY":"x"}"#.to_string(),
        r#"{"REFRESH_TOKEN":""}"#.to_string(),
    ] {
        let entry = rotated_entry();
        let provider = FakeProvider::empty().with_backup(&entry.backup_key(), &backup_json);
        let k = FakeKubectl::new(None);
        let report = apply_sync_entry(&k, &provider, &entry, NS, STORE, REFRESH).expect("apply");
        assert_eq!(
            report.seeds,
            vec![KeyReport {
                key: "REFRESH_TOKEN".to_string(),
                outcome: KeySeed::NotInBackup,
            }],
            "backup body {backup_json}"
        );
        assert!(k.create_call_index().is_none());
        assert!(k.replace_call_index().is_none());
    }
}

#[test]
fn static_entry_never_calls_provider_and_applies_owner_external_secret_only() {
    let entry = static_entry();
    let provider = FakeProvider::forbidding_calls();
    let k = FakeKubectl::new(None);
    let report = apply_sync_entry(&k, &provider, &entry, NS, STORE, REFRESH).expect("apply");
    assert_eq!(
        report,
        ApplyReport {
            entry: "platform".to_string(),
            seeds: vec![],
        }
    );
    assert_eq!(provider.call_count(), 0);
    assert!(k.create_call_index().is_none());
    assert!(k.replace_call_index().is_none());
    let apply_idx = k.apply_call_index().expect("apply call happened");
    let (_, stdin) = &k.calls()[apply_idx];
    let list: Value = serde_json::from_slice(stdin.as_ref().unwrap()).unwrap();
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["kind"], "ExternalSecret");
    assert_eq!(items[0]["spec"]["target"]["creationPolicy"], "Owner");
}

#[test]
fn seed_failure_aborts_before_apply() {
    let entry = rotated_entry();
    let backup_json = format!(r#"{{"REFRESH_TOKEN":"{BACKUP_VALUE}"}}"#);
    let provider = FakeProvider::empty().with_backup(&entry.backup_key(), &backup_json);
    let k = FakeKubectl::new(None)
        .with_get_error("Error from server (Forbidden): User cannot get resource \"secrets\"");
    let err = apply_sync_entry(&k, &provider, &entry, NS, STORE, REFRESH)
        .expect_err("a seed error must abort the apply");
    assert!(format!("{err:#}").contains("curie-finance"), "{err:#}");
    assert!(k.apply_call_index().is_none());
}

#[test]
fn seed_attempts_constant_is_used() {
    // Just document the contract value the plan names; a change here is a
    // deliberate behavior change, not a silent drift.
    assert_eq!(SEED_ATTEMPTS, 5);
}

// ---------------------------------------------------------------- read_backup

#[test]
fn read_backup_not_found_is_ok_none() {
    let provider = FakeProvider::empty();
    let result = read_backup(&provider, "curie/test/finance-rotated").expect("read_backup");
    assert!(result.is_none());
}

#[test]
fn read_backup_returns_material_by_key() {
    let provider = FakeProvider::empty().with_backup(
        "curie/test/finance-rotated",
        r#"{"REFRESH_TOKEN":"tok-abc","OTHER":"o"}"#,
    );
    let result = read_backup(&provider, "curie/test/finance-rotated")
        .expect("read_backup")
        .expect("backup present");
    assert_eq!(result.len(), 2);
    assert_eq!(result["REFRESH_TOKEN"].expose(), "tok-abc");
    assert_eq!(result["OTHER"].expose(), "o");
}

#[test]
fn read_backup_invalid_json_errors_naming_object_without_material() {
    let provider = FakeProvider::empty().with_backup("curie/test/finance-rotated", "not json");
    let err =
        read_backup(&provider, "curie/test/finance-rotated").expect_err("invalid JSON must error");
    let message = format!("{err:#}");
    assert!(message.contains("curie/test/finance-rotated"), "{message}");
    assert!(!message.contains("not json"), "{message}");
}

#[test]
fn read_backup_non_string_values_error_naming_object_without_material() {
    let provider = FakeProvider::empty().with_backup(
        "curie/test/finance-rotated",
        r#"{"REFRESH_TOKEN":{"nested":"leak-me-1234"}}"#,
    );
    let err = read_backup(&provider, "curie/test/finance-rotated")
        .expect_err("non-string value must error");
    let message = format!("{err:#}");
    assert!(message.contains("curie/test/finance-rotated"), "{message}");
    assert!(!message.contains("leak-me-1234"), "{message}");
}

#[test]
fn read_backup_provider_unavailable_errors() {
    let provider = FakeProvider::empty().with_unavailable("curie/test/finance-rotated");
    let err =
        read_backup(&provider, "curie/test/finance-rotated").expect_err("unavailable must error");
    assert!(format!("{err:#}").contains("curie/test/finance-rotated"));
}

// ---------------------------------------------------------------- store: cluster regression

#[test]
fn store_cluster_entry_is_refused_before_reaching_apply_sync_entry() {
    // Nothing here shells out: FakeKubectl and FakeProvider are pure
    // in-memory fakes, so this whole module works with no aws binary and
    // no ESO installed.
    let mut inventory_entry = entry(
        "api-key",
        "curie-api-key",
        &["apiKey"],
        RotationOwner::Sm,
        &[],
    );
    inventory_entry.store = Store::Cluster;
    let err = SyncEntry::from_inventory(&inventory_entry, PREFIX)
        .expect_err("a cluster-store entry must never reach apply_sync_entry");
    assert!(format!("{err:#}").contains("store: cluster"), "{err:#}");
}

// Silence an unused-import warning if RejectedReason ever stops being needed
// by a future test in this file; kept imported for parity with the provider
// contract this module exercises.
#[allow(dead_code)]
fn _touch(_: RejectedReason) {}
