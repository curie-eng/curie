//! `cluster deploy` with a provider (ADR 0163 decisions 3, 5, 6, 7): the
//! connector credential plan, the read-only preflight, the Secrets Manager
//! writes, the ESO objects, the Helm knob bind and the connector apply that
//! leaves the value Secret to External Secrets. Kubectl is a scripted fake
//! (style: tests/eso_objects.rs) and the provider is in memory (style:
//! tests/rotation_owner.rs). Nothing here needs aws, helm, kubectl or ESO.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Result;
use serde_json::{json, Value};

use curie::cluster_secrets::{
    bind_commands, provider_bind_commands, BindOpts, ProviderBindOpts,
};
use curie::connector_build::{parse_connectors, ConnectorsFileDecl};
use curie::connectors::prepare;
use curie::installation::{ProviderKind, SecretsBlock};
use curie::ops::{CmdArg, CommonOpts};
use curie::provider::connector_deploy::{
    apply_objects, eso_prune_args, plan, preflight, write_provider, Plan, PlanInput, RemoteKeys,
    ESO_MANAGER_PREFIX, EXTERNAL_SECRET_CRD,
};
use curie::provider::eso::{
    self, release_store_spec, render_external_secret, render_push_secret, Kubectl,
    KubectlOutput, SyncEntry,
};
use curie::provider::{
    InventoryClass, InventoryEntry, ObjectMetadata, ObjectVersion, ProviderError, PutRequest,
    RotationOwner, SecretMaterial, SecretsProvider, Store, StoredObject, UpdatePolicy,
};
use curie::secrets::SecretScope;

const RELEASE: &str = "r1";
const NS: &str = "curie";
const AGENT: &str = "bot";
const REMOTE_PREFIX: &str = "curie/r1";
const HOSTED_TARGET: &str = "r1-bot-connector-secrets";
const SANDBOX_TARGET: &str = "r1-curie-agent-bot-connector-secrets";
const HOSTED_ENTRY: &str = "bot.conn.hosted";
const SANDBOX_ENTRY: &str = "bot.sandbox";
const OWNER: &str = "curie.dev/connector-owner";
const V1: &str = "fixture-value-1";
const V2: &str = "fixture-value-2";
const V3: &str = "fixture-value-3";

const DECL: &str = "\
connectors:
  conn:
    image: ghcr.io/example/conn:1
    secrets:
      - TOKEN_A
      - TOKEN_B
    secret_rotation:
      TOKEN_B: workload
";

fn decl() -> ConnectorsFileDecl {
    parse_connectors(DECL).expect("fixture connectors.yaml parses")
}

fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn hosted_values() -> BTreeMap<String, String> {
    map(&[("TOKEN_A", V1), ("TOKEN_B", V2)])
}

fn sandbox_values() -> BTreeMap<String, String> {
    map(&[("TOKEN_A", V1), ("EXTRA_ENV", V3)])
}

fn build_plan(
    decl: &ConnectorsFileDecl,
    hosted: &BTreeMap<String, String>,
    sandbox: &BTreeMap<String, String>,
) -> Result<Plan> {
    plan(&PlanInput {
        release: RELEASE,
        namespace: NS,
        agent: AGENT,
        remote_prefix: REMOTE_PREFIX,
        decl,
        hosted_values: hosted,
        sandbox_values: sandbox,
    })
}

fn default_plan() -> Plan {
    build_plan(&decl(), &hosted_values(), &sandbox_values()).expect("plan")
}

fn exposed(values: &BTreeMap<String, SecretMaterial>) -> BTreeMap<String, String> {
    values
        .iter()
        .map(|(k, v)| (k.clone(), v.expose().to_string()))
        .collect()
}

fn owner_labels() -> BTreeMap<String, String> {
    map(&[(OWNER, AGENT)])
}

fn assert_no_values(text: &str) {
    for value in [V1, V2, V3] {
        assert!(!text.contains(value), "value bytes leaked: {text}");
    }
}

// ---------------------------------------------------------------- plan

/// The hosted entry targets `<release>-<agent>-connector-secrets`, keeps its
/// logical name, and splits the workload-rotated key from the static one.
#[test]
fn plan_hosted_entry_targets_the_release_secret_and_splits_rotation() {
    let plan = default_plan();
    assert_eq!(plan.hosted.len(), 1, "one hosted connector, one entry");
    let hosted = &plan.hosted[0];
    assert_eq!(hosted.sync.name, HOSTED_ENTRY);
    assert_eq!(hosted.sync.target, HOSTED_TARGET);
    assert_eq!(hosted.sync.remote_key, format!("{REMOTE_PREFIX}/{HOSTED_ENTRY}"));
    assert_eq!(hosted.sync.static_keys, vec!["TOKEN_A".to_string()]);
    assert_eq!(hosted.sync.rotated_keys, vec!["TOKEN_B".to_string()]);
    assert_eq!(hosted.sync.labels, owner_labels());
    assert_eq!(exposed(&hosted.static_values), map(&[("TOKEN_A", V1)]));
    assert_eq!(exposed(&hosted.rotated_values), map(&[("TOKEN_B", V2)]));
}

/// The sandbox entry targets the chart fullname's per-agent Secret and carries
/// exactly the sandbox bind map's keys, all static.
#[test]
fn plan_sandbox_entry_targets_the_fullname_secret() {
    let plan = default_plan();
    let sandbox = plan.sandbox.as_ref().expect("sandbox entry");
    assert_eq!(sandbox.sync.name, SANDBOX_ENTRY);
    assert_eq!(sandbox.sync.target, SANDBOX_TARGET);
    assert_eq!(
        sandbox.sync.remote_key,
        format!("{REMOTE_PREFIX}/{SANDBOX_ENTRY}")
    );
    assert_eq!(
        sandbox.sync.static_keys,
        vec!["EXTRA_ENV".to_string(), "TOKEN_A".to_string()]
    );
    assert!(sandbox.sync.rotated_keys.is_empty());
    assert_eq!(sandbox.sync.labels, owner_labels());
    assert_eq!(exposed(&sandbox.static_values), sandbox_values());
    assert!(sandbox.rotated_values.is_empty());

    let (name, keys) = plan.sandbox_target().expect("sandbox target");
    assert_eq!(name, SANDBOX_TARGET);
    assert_eq!(keys, vec!["EXTRA_ENV".to_string(), "TOKEN_A".to_string()]);
}

/// Entries come hosted first then sandbox; object names cover every
/// ExternalSecret and the rotated entry's PushSecret.
#[test]
fn plan_entries_and_object_names() {
    let plan = default_plan();
    let names: Vec<&str> = plan.entries().iter().map(|e| e.sync.name.as_str()).collect();
    assert_eq!(names, vec![HOSTED_ENTRY, SANDBOX_ENTRY]);
    let mut objects = plan.object_names();
    objects.sort();
    assert_eq!(
        objects,
        vec![
            HOSTED_ENTRY.to_string(),
            format!("{HOSTED_ENTRY}-rotated-backup"),
            SANDBOX_ENTRY.to_string(),
        ]
    );
}

/// Hosted keys that differ from the API's owned keys are refused, naming the
/// keys and never a value.
#[test]
fn plan_refuses_a_hosted_key_set_mismatch_without_values() {
    let short = map(&[("TOKEN_A", V1)]);
    let err = build_plan(&decl(), &short, &sandbox_values()).expect_err("mismatch refused");
    let text = format!("{err:#}");
    assert!(text.contains("TOKEN_B"), "{text}");
    assert_no_values(&text);

    let extra = map(&[("TOKEN_A", V1), ("TOKEN_B", V2), ("TOKEN_C", V3)]);
    let err = build_plan(&decl(), &extra, &sandbox_values()).expect_err("mismatch refused");
    let text = format!("{err:#}");
    assert!(text.contains("TOKEN_C"), "{text}");
    assert_no_values(&text);
}

/// An empty sandbox bind map plans no sandbox entry.
#[test]
fn plan_without_sandbox_values_has_no_sandbox_entry() {
    let plan = build_plan(&decl(), &hosted_values(), &BTreeMap::new()).expect("plan");
    assert!(plan.sandbox.is_none());
    assert!(plan.sandbox_target().is_none());
    assert_eq!(plan.entries().len(), 1);
}

/// An empty value is refused, naming the key.
#[test]
fn plan_refuses_an_empty_value() {
    let hosted = map(&[("TOKEN_A", ""), ("TOKEN_B", V2)]);
    let err = build_plan(&decl(), &hosted, &sandbox_values()).expect_err("empty hosted value");
    let text = format!("{err:#}");
    assert!(text.contains("TOKEN_A"), "{text}");
    assert_no_values(&text);

    let sandbox = map(&[("EXTRA_ENV", "")]);
    let err = build_plan(&decl(), &hosted_values(), &sandbox).expect_err("empty sandbox value");
    assert!(format!("{err:#}").contains("EXTRA_ENV"), "{err:#}");
}

// ---------------------------------------------------------------- scripted kubectl

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

const WRITE_VERBS: &[&str] = &[
    "apply", "create", "replace", "patch", "delete", "annotate", "label", "edit", "scale",
];

/// Answers the CRD read and Secret reads from a fixed table; records every call.
struct ReadKubectl {
    crd_present: bool,
    secrets: BTreeMap<String, Value>,
    calls: Mutex<Vec<Vec<String>>>,
}

impl ReadKubectl {
    fn new(crd_present: bool) -> Self {
        Self {
            crd_present,
            secrets: BTreeMap::new(),
            calls: Mutex::new(Vec::new()),
        }
    }

    fn with_secret(mut self, name: &str, managers: &[&str]) -> Self {
        let fields: Vec<Value> = managers
            .iter()
            .map(|m| json!({ "manager": m, "operation": "Apply" }))
            .collect();
        self.secrets.insert(
            name.to_string(),
            json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": { "name": name, "namespace": NS, "managedFields": fields },
                "type": "Opaque",
                "data": {},
            }),
        );
        self
    }

    fn calls(&self) -> Vec<Vec<String>> {
        self.calls.lock().unwrap().clone()
    }

    fn assert_read_only(&self) {
        let calls = self.calls();
        assert!(!calls.is_empty(), "preflight must read the cluster");
        for args in &calls {
            assert!(has(args, "get"), "non-read kubectl call: {args:?}");
            for verb in WRITE_VERBS {
                assert!(!has(args, verb), "write verb {verb} in {args:?}");
            }
        }
    }
}

impl Kubectl for ReadKubectl {
    fn run(&self, args: &[String], _stdin: Option<&[u8]>) -> Result<KubectlOutput> {
        self.calls.lock().unwrap().push(args.to_vec());
        let is_crd = args
            .iter()
            .any(|a| a.contains(EXTERNAL_SECRET_CRD) || a == "crd" || a.starts_with("crd/"));
        if is_crd {
            return if self.crd_present {
                ok(json!({ "kind": "CustomResourceDefinition",
                           "metadata": { "name": EXTERNAL_SECRET_CRD } })
                .to_string())
            } else {
                fail(format!(
                    "Error from server (NotFound): customresourcedefinitions.apiextensions.k8s.io \"{EXTERNAL_SECRET_CRD}\" not found"
                ))
            };
        }
        if has(args, "get") && args.iter().any(|a| a.starts_with("secret")) {
            let found = args.iter().find_map(|a| {
                let name = a.rsplit('/').next().unwrap_or(a);
                self.secrets.get(name)
            });
            return match found {
                Some(secret) => ok(secret.to_string()),
                None => fail("Error from server (NotFound): secrets \"x\" not found"),
            };
        }
        fail(format!("ReadKubectl: unexpected call {args:?}"))
    }
}

// ---------------------------------------------------------------- preflight

/// A missing ESO CRD is refused with the `curie apply` fix, before any write.
#[test]
fn preflight_refuses_when_the_crd_is_missing() {
    let k = ReadKubectl::new(false);
    let err = preflight(&k, NS, &default_plan()).expect_err("no CRD");
    let text = format!("{err:#}");
    assert!(text.contains("curie apply"), "{text}");
    k.assert_read_only();
}

/// Absent target Secrets are fine: ESO will create them.
#[test]
fn preflight_accepts_absent_targets() {
    let k = ReadKubectl::new(true);
    preflight(&k, NS, &default_plan()).expect("absent targets are ok");
    k.assert_read_only();
}

/// A target Secret whose managedFields name this entry's ESO manager is ours.
#[test]
fn preflight_accepts_an_eso_managed_target() {
    let hosted_manager = format!("{ESO_MANAGER_PREFIX}{HOSTED_ENTRY}");
    let sandbox_manager = format!("{ESO_MANAGER_PREFIX}{SANDBOX_ENTRY}");
    let k = ReadKubectl::new(true)
        .with_secret(HOSTED_TARGET, &[&hosted_manager, "curie-secrets"])
        .with_secret(SANDBOX_TARGET, &[&sandbox_manager]);
    preflight(&k, NS, &default_plan()).expect("eso-managed targets are ok");
    k.assert_read_only();
}

/// A target Secret written by kubectl client-side apply is refused by name.
#[test]
fn preflight_refuses_a_kubectl_applied_target() {
    let k = ReadKubectl::new(true).with_secret(HOSTED_TARGET, &["kubectl-client-side-apply"]);
    let err = preflight(&k, NS, &default_plan()).expect_err("foreign Secret");
    assert!(format!("{err:#}").contains(HOSTED_TARGET), "{err:#}");
    k.assert_read_only();
}

/// A target Secret owned by helm is refused by name.
#[test]
fn preflight_refuses_a_helm_owned_target() {
    let k = ReadKubectl::new(true).with_secret(SANDBOX_TARGET, &["helm"]);
    let err = preflight(&k, NS, &default_plan()).expect_err("helm Secret");
    assert!(format!("{err:#}").contains(SANDBOX_TARGET), "{err:#}");
    k.assert_read_only();
}

/// An ESO manager for a different ExternalSecret does not make the target ours.
#[test]
fn preflight_refuses_a_target_managed_by_another_external_secret() {
    let other = format!("{ESO_MANAGER_PREFIX}someone-else");
    let k = ReadKubectl::new(true).with_secret(HOSTED_TARGET, &[&other]);
    let err = preflight(&k, NS, &default_plan()).expect_err("other ExternalSecret");
    assert!(format!("{err:#}").contains(HOSTED_TARGET), "{err:#}");
    k.assert_read_only();
}

// ---------------------------------------------------------------- in-memory provider

#[derive(Debug, Clone, PartialEq)]
struct Put {
    name: String,
    material: String,
    expected_version: Option<String>,
}

#[derive(Default)]
struct MemProvider {
    objects: Mutex<BTreeMap<String, (u64, String)>>,
    puts: Mutex<Vec<Put>>,
    gets: Mutex<Vec<String>>,
    fail_puts: bool,
}

impl MemProvider {
    fn with_object(self, name: &str, body: Value) -> Self {
        self.objects
            .lock()
            .unwrap()
            .insert(name.to_string(), (7, body.to_string()));
        self
    }

    fn puts(&self) -> Vec<Put> {
        self.puts.lock().unwrap().clone()
    }

    fn gets(&self) -> Vec<String> {
        self.gets.lock().unwrap().clone()
    }

    fn stored(&self, name: &str) -> Option<Value> {
        self.objects
            .lock()
            .unwrap()
            .get(name)
            .map(|(_, body)| serde_json::from_str(body).unwrap())
    }

    fn version(&self, name: &str) -> Option<String> {
        self.objects
            .lock()
            .unwrap()
            .get(name)
            .map(|(v, _)| v.to_string())
    }
}

impl SecretsProvider for MemProvider {
    fn put(&self, request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError> {
        assert!(!request.name.contains('/'), "logical names only: {}", request.name);
        self.puts.lock().unwrap().push(Put {
            name: request.name.to_string(),
            material: request.material.expose().to_string(),
            expected_version: request.expected_version.map(str::to_string),
        });
        if self.fail_puts {
            return Err(ProviderError::Unavailable {
                name: request.name.to_string(),
                status: 503,
            });
        }
        let mut objects = self.objects.lock().unwrap();
        let current = objects.get(request.name).map(|(v, _)| v.to_string());
        if request.expected_version.map(str::to_string) != current && request.expected_version.is_some() {
            return Err(ProviderError::Conflict {
                name: request.name.to_string(),
                expected_version: request.expected_version.map(str::to_string),
                actual_version: current,
            });
        }
        let next = objects.get(request.name).map_or(1, |(v, _)| v + 1);
        objects.insert(
            request.name.to_string(),
            (next, request.material.expose().to_string()),
        );
        Ok(ObjectVersion {
            id: next.to_string(),
        })
    }

    fn get(&self, name: &str, _version: Option<&str>) -> Result<StoredObject, ProviderError> {
        self.gets.lock().unwrap().push(name.to_string());
        match self.objects.lock().unwrap().get(name) {
            Some((v, body)) => Ok(StoredObject {
                version: ObjectVersion { id: v.to_string() },
                material: SecretMaterial::new(body.clone()),
                key_names: vec![],
            }),
            None => Err(ProviderError::NotFound {
                name: name.to_string(),
            }),
        }
    }

    fn get_metadata(&self, name: &str) -> Result<ObjectMetadata, ProviderError> {
        match self.objects.lock().unwrap().get(name) {
            Some((v, _)) => Ok(ObjectMetadata {
                name: name.to_string(),
                version: ObjectVersion { id: v.to_string() },
                tags: BTreeMap::new(),
                key_names: vec![],
            }),
            None => Err(ProviderError::NotFound {
                name: name.to_string(),
            }),
        }
    }

    fn list(&self, _prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError> {
        Ok(Vec::new())
    }

    fn tag(
        &self,
        _name: &str,
        _tags: &BTreeMap<String, String>,
        _expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        panic!("connector deploy must never tag")
    }

    fn delete(
        &self,
        _name: &str,
        _expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        panic!("connector deploy must never delete a provider object")
    }
}

fn backup_name() -> String {
    format!("{HOSTED_ENTRY}-rotated")
}

fn puts_named(provider: &MemProvider, name: &str) -> Vec<Put> {
    provider.puts().into_iter().filter(|p| p.name == name).collect()
}

// ---------------------------------------------------------------- write_provider

/// A fresh store gets each entry's static keys as a JSON string map, created
/// without an expected version, and the rotated backup with only rotated keys.
#[test]
fn write_provider_creates_objects_and_backup() {
    let provider = MemProvider::default();
    let report = write_provider(&provider, &default_plan()).expect("write");

    let hosted = puts_named(&provider, HOSTED_ENTRY);
    assert_eq!(hosted.len(), 1);
    assert_eq!(hosted[0].expected_version, None);
    assert_eq!(provider.stored(HOSTED_ENTRY).unwrap(), json!({ "TOKEN_A": V1 }));

    let sandbox = puts_named(&provider, SANDBOX_ENTRY);
    assert_eq!(sandbox.len(), 1);
    assert_eq!(
        provider.stored(SANDBOX_ENTRY).unwrap(),
        json!({ "TOKEN_A": V1, "EXTRA_ENV": V3 })
    );

    assert_eq!(puts_named(&provider, &backup_name()).len(), 1);
    assert_eq!(provider.stored(&backup_name()).unwrap(), json!({ "TOKEN_B": V2 }));

    let mut written = report.written.clone();
    written.sort();
    assert_eq!(written, vec![HOSTED_ENTRY.to_string(), SANDBOX_ENTRY.to_string()]);
    assert!(report.unchanged.is_empty());
    assert_eq!(report.backups_created, vec![backup_name()]);
}

/// A redeploy with the same values issues no put at all.
#[test]
fn write_provider_skips_unchanged_objects() {
    let provider = MemProvider::default();
    write_provider(&provider, &default_plan()).expect("first write");
    let before = provider.puts().len();
    let report = write_provider(&provider, &default_plan()).expect("second write");
    assert_eq!(provider.puts().len(), before, "no version churn: {:?}", provider.puts());
    assert!(report.written.is_empty());
    assert!(report.backups_created.is_empty());
    let mut unchanged = report.unchanged.clone();
    unchanged.sort();
    assert_eq!(unchanged, vec![HOSTED_ENTRY.to_string(), SANDBOX_ENTRY.to_string()]);
}

/// A changed value is written with the stored version as the expected one.
#[test]
fn write_provider_updates_with_the_stored_version() {
    let provider = MemProvider::default();
    write_provider(&provider, &default_plan()).expect("first write");
    let stored_version = provider.version(HOSTED_ENTRY).unwrap();

    let changed = map(&[("TOKEN_A", "fixture-value-changed"), ("TOKEN_B", V2)]);
    let plan = build_plan(&decl(), &changed, &sandbox_values()).expect("plan");
    let report = write_provider(&provider, &plan).expect("second write");

    let hosted = puts_named(&provider, HOSTED_ENTRY);
    assert_eq!(hosted.len(), 2);
    assert_eq!(hosted[1].expected_version.as_deref(), Some(stored_version.as_str()));
    assert_eq!(
        provider.stored(HOSTED_ENTRY).unwrap(),
        json!({ "TOKEN_A": "fixture-value-changed" })
    );
    assert_eq!(report.written, vec![HOSTED_ENTRY.to_string()]);
    assert_eq!(puts_named(&provider, SANDBOX_ENTRY).len(), 1, "sandbox unchanged");
}

/// Keys already in the provider object that the plan does not own survive.
#[test]
fn write_provider_preserves_unrelated_keys() {
    let provider = MemProvider::default().with_object(
        HOSTED_ENTRY,
        json!({ "UNRELATED": "fixture-unrelated", "TOKEN_A": "fixture-old" }),
    );
    write_provider(&provider, &default_plan()).expect("write");
    let hosted = puts_named(&provider, HOSTED_ENTRY);
    assert_eq!(hosted.len(), 1);
    assert_eq!(hosted[0].expected_version.as_deref(), Some("7"));
    assert_eq!(
        provider.stored(HOSTED_ENTRY).unwrap(),
        json!({ "UNRELATED": "fixture-unrelated", "TOKEN_A": V1 })
    );
}

/// An existing rotated backup is never overwritten.
#[test]
fn write_provider_never_overwrites_an_existing_backup() {
    let provider = MemProvider::default()
        .with_object(&backup_name(), json!({ "TOKEN_B": "fixture-rotated-by-workload" }));
    let report = write_provider(&provider, &default_plan()).expect("write");
    assert!(puts_named(&provider, &backup_name()).is_empty());
    assert_eq!(
        provider.stored(&backup_name()).unwrap(),
        json!({ "TOKEN_B": "fixture-rotated-by-workload" })
    );
    assert!(report.backups_created.is_empty());
}

/// A provider failure surfaces the object name, never material.
#[test]
fn write_provider_errors_carry_no_material() {
    let provider = MemProvider {
        fail_puts: true,
        ..MemProvider::default()
    };
    let err = write_provider(&provider, &default_plan()).expect_err("put fails");
    let text = format!("{err:#}");
    assert_no_values(&text);
    assert!(
        text.contains(HOSTED_ENTRY) || text.contains(SANDBOX_ENTRY) || text.contains(&backup_name()),
        "{text}"
    );
}

// ---------------------------------------------------------------- RemoteKeys

/// A full remote key resolves through its logical name; a bare name passes through.
#[test]
fn remote_keys_strips_the_remote_prefix() {
    let inner = MemProvider::default();
    let remote = RemoteKeys {
        inner: &inner,
        remote_prefix: REMOTE_PREFIX,
    };
    let _ = remote.get(&format!("{REMOTE_PREFIX}/x"), None);
    let _ = remote.get("x", None);
    assert_eq!(inner.gets(), vec!["x".to_string(), "x".to_string()]);
}

// ---------------------------------------------------------------- eso renders

fn golden(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/eso")
        .join(name);
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&raw).expect("golden json")
}

fn inventory(logical: &str, target: &str, keys: &[&str], owner: RotationOwner, rotated: &[&str]) -> InventoryEntry {
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

fn golden_static() -> SyncEntry {
    SyncEntry::from_inventory(
        &inventory("platform", "curie-platform", &["A", "B"], RotationOwner::Sm, &[]),
        "curie/test",
    )
    .unwrap()
}

fn golden_split() -> SyncEntry {
    SyncEntry::from_inventory(
        &inventory(
            "finance",
            "curie-finance",
            &["CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN"],
            RotationOwner::Workload("finance-agent".to_string()),
            &["REFRESH_TOKEN"],
        ),
        "curie/test",
    )
    .unwrap()
}

/// `from_inventory` starts with no labels, and unlabelled renders match the goldens.
#[test]
fn unlabelled_renders_match_the_goldens() {
    assert!(golden_static().labels.is_empty());
    assert_eq!(
        render_external_secret(&golden_static(), "curie", "curie-aws", "1h"),
        golden("external-static.json")
    );
    assert_eq!(
        render_external_secret(&golden_split(), "curie", "curie-aws", "1h"),
        golden("external-split.json")
    );
    assert_eq!(
        render_push_secret(&golden_split(), "curie", "curie-aws").unwrap(),
        golden("push-backup.json")
    );
}

/// Labels land on the ExternalSecret and on the Secret it creates via a merge template.
#[test]
fn labelled_external_secret_adds_metadata_and_template_labels() {
    let entry = golden_static().with_labels(owner_labels());
    let rendered = render_external_secret(&entry, "curie", "curie-aws", "1h");
    assert_eq!(rendered["metadata"]["labels"], json!({ OWNER: AGENT }));
    assert_eq!(
        rendered["spec"]["target"]["template"],
        json!({
            "engineVersion": "v2",
            "mergePolicy": "Merge",
            "metadata": { "labels": { OWNER: AGENT } },
        })
    );
    // Everything else is the unlabelled golden.
    let mut stripped = rendered.clone();
    stripped["metadata"].as_object_mut().unwrap().remove("labels");
    stripped["spec"]["target"].as_object_mut().unwrap().remove("template");
    assert_eq!(stripped, golden("external-static.json"));
}

/// Labels land on the PushSecret metadata; the rest matches the golden.
#[test]
fn labelled_push_secret_adds_metadata_labels() {
    let entry = golden_split().with_labels(owner_labels());
    let push = render_push_secret(&entry, "curie", "curie-aws").unwrap();
    assert_eq!(push["metadata"]["labels"], json!({ OWNER: AGENT }));
    let mut stripped = push.clone();
    stripped["metadata"].as_object_mut().unwrap().remove("labels");
    assert_eq!(stripped, golden("push-backup.json"));
}

fn secrets_block() -> SecretsBlock {
    SecretsBlock {
        provider: ProviderKind::Aws,
        region: "us-east-1".to_string(),
        prefix: "curie".to_string(),
        role_arn: "arn:aws:iam::000000000000:role/curie-eso-test".to_string(),
    }
}

/// The release store is `<release>-secrets-manager` authenticating as `<release>-secrets-sync`.
#[test]
fn release_store_spec_names() {
    let spec = release_store_spec(RELEASE, NS, &secrets_block());
    assert_eq!(spec.name, "r1-secrets-manager");
    assert_eq!(spec.service_account, "r1-secrets-sync");
    assert_eq!(spec.namespace, NS);
    assert_eq!(spec.region, "us-east-1");
    assert_eq!(spec.role_arn, "arn:aws:iam::000000000000:role/curie-eso-test");
}

// ---------------------------------------------------------------- apply_objects

/// Accepts applies, records force-sync annotations, and answers ExternalSecret
/// reads as Ready at exactly the annotated metadata. Target Secrets are absent.
#[derive(Default)]
struct SyncKubectl {
    calls: Mutex<Vec<(Vec<String>, Option<Vec<u8>>)>>,
    externals: Mutex<BTreeMap<String, Value>>,
}

fn after(args: &[String], token: &str) -> Option<String> {
    args.iter()
        .position(|a| a == token)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

impl SyncKubectl {
    fn calls(&self) -> Vec<(Vec<String>, Option<Vec<u8>>)> {
        self.calls.lock().unwrap().clone()
    }
}

impl Kubectl for SyncKubectl {
    fn run(&self, args: &[String], stdin: Option<&[u8]>) -> Result<KubectlOutput> {
        self.calls
            .lock()
            .unwrap()
            .push((args.to_vec(), stdin.map(<[u8]>::to_vec)));
        if has(args, "apply") {
            return ok("applied");
        }
        if has(args, "annotate") {
            let name = after(args, "externalsecret").expect("annotated ExternalSecret name");
            let prefix = format!("{}=", eso::FORCE_SYNC_ANNOTATION);
            let value = args
                .iter()
                .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
                .expect("force-sync value");
            self.externals.lock().unwrap().insert(
                name.clone(),
                json!({
                    "name": name,
                    "namespace": NS,
                    "generation": 1,
                    "annotations": { eso::FORCE_SYNC_ANNOTATION: value },
                }),
            );
            return ok("annotated");
        }
        if has(args, "get") && has(args, "externalsecret") {
            let name = after(args, "externalsecret").expect("ExternalSecret name");
            let Some(metadata) = self.externals.lock().unwrap().get(&name).cloned() else {
                return fail("Error from server (NotFound): externalsecrets not found");
            };
            let synced = eso::synced_version(&metadata);
            return ok(json!({
                "metadata": metadata,
                "status": {
                    "conditions": [{ "type": "Ready", "status": "True" }],
                    "syncedResourceVersion": synced,
                },
            })
            .to_string());
        }
        if has(args, "get") {
            return fail("Error from server (NotFound): secrets \"x\" not found");
        }
        fail(format!("SyncKubectl: unexpected call {args:?}"))
    }
}

fn stdin_has(stdin: &Option<Vec<u8>>, needle: &str) -> bool {
    stdin
        .as_ref()
        .is_some_and(|b| String::from_utf8_lossy(b).contains(needle))
}

/// The store applies first, every ExternalSecret is applied and force-synced
/// once, and the backup is read by its logical name.
#[test]
fn apply_objects_orders_store_then_entries_then_sync() {
    let k = SyncKubectl::default();
    let provider = MemProvider::default();
    let plan = default_plan();
    let store = release_store_spec(RELEASE, NS, &secrets_block());
    apply_objects(
        &k,
        &provider,
        &plan,
        &store,
        REMOTE_PREFIX,
        Duration::from_secs(5),
        Duration::from_millis(1),
    )
    .expect("apply objects");

    let calls = k.calls();
    let applies: Vec<usize> = calls
        .iter()
        .enumerate()
        .filter(|(_, (a, _))| has(a, "apply"))
        .map(|(i, _)| i)
        .collect();
    let store_at = applies
        .iter()
        .copied()
        .find(|&i| stdin_has(&calls[i].1, "\"SecretStore\""))
        .expect("SecretStore applied");
    let sa_at = applies
        .iter()
        .copied()
        .find(|&i| stdin_has(&calls[i].1, "\"ServiceAccount\""))
        .expect("ServiceAccount applied");
    let first_external = applies
        .iter()
        .copied()
        .find(|&i| stdin_has(&calls[i].1, "\"ExternalSecret\""))
        .expect("ExternalSecret applied");
    assert!(store_at < first_external, "store before ExternalSecrets");
    assert!(sa_at < first_external, "service account before ExternalSecrets");
    assert!(stdin_has(&calls[store_at].1, "r1-secrets-manager"));

    for name in [HOSTED_ENTRY, SANDBOX_ENTRY] {
        let annotates: Vec<usize> = calls
            .iter()
            .enumerate()
            .filter(|(_, (a, _))| has(a, "annotate") && has(a, name))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(annotates.len(), 1, "one force-sync for {name}");
        assert!(annotates[0] > first_external, "sync after apply for {name}");
    }
    assert!(stdin_has(&calls[first_external].1, OWNER), "owner label on ExternalSecret");
    assert!(stdin_has(&calls[first_external].1, "\"refreshInterval\":\"1h\""));

    assert!(
        provider.gets().contains(&backup_name()),
        "backup read through its logical name: {:?}",
        provider.gets()
    );
    assert!(provider.gets().iter().all(|n| !n.contains('/')));
    assert!(provider.puts().is_empty(), "apply_objects writes nothing to the provider");
}

// ---------------------------------------------------------------- eso_prune_args

/// The ESO prune selects this agent's owner label and keeps names by one field selector.
#[test]
fn eso_prune_args_uses_one_field_selector() {
    let keep = vec![HOSTED_ENTRY.to_string(), SANDBOX_ENTRY.to_string()];
    let args = eso_prune_args(NS, AGENT, &keep);
    assert_ne!(args.first().map(String::as_str), Some("kubectl"));
    assert!(has(&args, "delete"), "{args:?}");
    assert!(has(&args, "externalsecret,pushsecret"), "{args:?}");
    assert_eq!(after(&args, "-l").as_deref(), Some("curie.dev/connector-owner=bot"));
    assert_eq!(after(&args, "-n").as_deref(), Some(NS));
    let selectors: Vec<&String> = args
        .iter()
        .filter(|a| a.starts_with("--field-selector"))
        .collect();
    assert_eq!(selectors.len(), 1, "{args:?}");
    assert_eq!(
        selectors[0],
        "--field-selector=metadata.name!=bot.conn.hosted,metadata.name!=bot.sandbox"
    );
}

/// With nothing to keep there is no field selector.
#[test]
fn eso_prune_args_without_keep_has_no_field_selector() {
    let args = eso_prune_args(NS, AGENT, &[]);
    assert!(!args.iter().any(|a| a.starts_with("--field-selector")), "{args:?}");
    assert!(has(&args, "externalsecret,pushsecret"), "{args:?}");
}

// ---------------------------------------------------------------- provider_bind_commands

fn common() -> CommonOpts {
    CommonOpts {
        namespace: NS.into(),
        release: RELEASE.into(),
        dry_run: false,
    }
}

fn provider_bind(agent: &str, keys: &[&str]) -> Result<Vec<curie::ops::OpsCommand>> {
    provider_bind_commands(&ProviderBindOpts {
        common: common(),
        chart: "charts/curie".into(),
        agent: agent.into(),
        secret_name: SANDBOX_TARGET.into(),
        keys: keys.iter().map(|k| k.to_string()).collect(),
    })
}

fn set_expressions(argv: &[String]) -> Vec<String> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < argv.len() {
        if argv[i] == "--set" {
            if let Some(v) = argv.get(i + 1) {
                out.push(v.clone());
            }
            i += 2;
            continue;
        }
        if let Some(v) = argv[i].strip_prefix("--set=") {
            out.push(v.to_string());
        }
        i += 1;
    }
    out
}

/// The sandbox binds by name: existingSecret, keys and a null for the old
/// value map, then the same sandboxclaim delete as the value bind.
#[test]
fn provider_bind_commands_set_names_only() {
    let cmds = provider_bind(AGENT, &["TOKEN_A", "TOKEN_B"]).expect("commands");
    assert_eq!(cmds.len(), 2);
    let helm = &cmds[0];
    assert_eq!(helm.program, "helm");
    let argv = helm.argv();
    assert_eq!(&argv[..3], &["upgrade".to_string(), RELEASE.into(), "charts/curie".into()]);
    assert_eq!(after(&argv, "-n").as_deref(), Some(NS));
    assert!(has(&argv, "--reuse-values"), "{argv:?}");
    assert_eq!(
        set_expressions(&argv),
        vec![
            format!("agentSandbox.connectorExistingSecrets.bot.existingSecret={SANDBOX_TARGET}"),
            "agentSandbox.connectorExistingSecrets.bot.keys={TOKEN_A,TOKEN_B}".to_string(),
            "agentSandbox.connectorSecrets.bot=null".to_string(),
        ]
    );
    for arg in &helm.args {
        assert!(
            !matches!(arg, CmdArg::SecretValuesFile(_) | CmdArg::SecretSet { .. }),
            "no value-bearing arg: {arg:?}"
        );
    }

    let value_bind = bind_commands(&BindOpts {
        common: common(),
        chart: "charts/curie".into(),
        agent: AGENT.into(),
        secrets: map(&[("TOKEN_A", V1)]),
    })
    .unwrap();
    assert_eq!(cmds[1].program, "kubectl");
    assert_eq!(cmds[1].args, value_bind[1].args, "same sandboxclaim delete");

    for cmd in &cmds {
        assert_no_values(&cmd.display());
        assert_no_values(&format!("{cmd:?}"));
    }
}

/// No keys, nothing to bind.
#[test]
fn provider_bind_commands_empty_keys_is_empty() {
    assert!(provider_bind(AGENT, &[]).expect("ok").is_empty());
}

/// An agent name that is not a DNS label is refused.
#[test]
fn provider_bind_commands_refuse_an_invalid_agent() {
    let err = provider_bind("Not_A_DNS", &["TOKEN_A"]).expect_err("invalid agent");
    assert!(format!("{err:#}").contains("Not_A_DNS"), "{err:#}");
}

/// Provider absent: the value bind still writes the per-agent values file.
#[test]
fn bind_commands_without_provider_still_write_values() {
    let cmds = bind_commands(&BindOpts {
        common: common(),
        chart: "charts/curie".into(),
        agent: AGENT.into(),
        secrets: map(&[("TOKEN_A", V1), ("TOKEN_B", V2)]),
    })
    .unwrap();
    let pairs = cmds[0]
        .args
        .iter()
        .find_map(|a| match a {
            CmdArg::SecretValuesFile(pairs) => Some(pairs.clone()),
            _ => None,
        })
        .expect("values file");
    assert_eq!(
        pairs,
        vec![
            ("agentSandbox.connectorSecrets.bot.TOKEN_A".to_string(), V1.to_string()),
            ("agentSandbox.connectorSecrets.bot.TOKEN_B".to_string(), V2.to_string()),
        ]
    );
}

// ---------------------------------------------------------------- connectors::prepare

fn manifests() -> Vec<Value> {
    vec![
        json!({
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": { "name": "r1-bot-mcp-conn", "namespace": NS },
            "spec": {},
        }),
        json!({
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": { "name": "r1-bot-mcp-conn", "namespace": NS },
            "spec": {},
        }),
    ]
}

fn prepared() -> curie::connectors::PreparedConnectorSync {
    let scope = SecretScope {
        cluster_identity: "ca:test".into(),
        release: RELEASE.into(),
        namespace: NS.into(),
    };
    prepare(
        &manifests(),
        &BTreeMap::new(),
        HOSTED_TARGET,
        &["TOKEN_A".to_string()],
        &scope,
        AGENT,
        &map(&[("TOKEN_A", V1)]),
    )
    .expect("prepare")
}

fn secret_object() -> (String, String) {
    ("Secret".to_string(), HOSTED_TARGET.to_string())
}

/// Provider absent: prepare still applies the rendered value Secret.
#[test]
fn prepare_without_provider_applies_the_value_secret() {
    let prepared = prepared();
    assert_eq!(prepared.owned_secret_name(), Some(HOSTED_TARGET));
    let applied = prepared.applied_objects();
    assert!(applied.contains(&secret_object()), "{applied:?}");
    assert!(prepared.keep_names().iter().any(|n| n == HOSTED_TARGET));
}

/// Provider delivery drops the value Secret from the apply but keeps its name
/// out of the prune; every other object is unchanged.
#[test]
fn into_provider_delivery_drops_the_secret_and_keeps_its_name() {
    let before = prepared();
    let before_objects = before.applied_objects();
    let before_keep = before.keep_names().to_vec();

    let after = prepared().into_provider_delivery();
    let after_objects = after.applied_objects();
    assert!(!after_objects.contains(&secret_object()), "{after_objects:?}");
    assert!(after.keep_names().iter().any(|n| n == HOSTED_TARGET));
    assert_eq!(after.keep_names(), before_keep.as_slice());

    let expected: Vec<(String, String)> = before_objects
        .into_iter()
        .filter(|o| o != &secret_object())
        .collect();
    assert_eq!(after_objects, expected);
    assert!(after_objects.contains(&("Deployment".to_string(), "r1-bot-mcp-conn".to_string())));
    assert!(after_objects.contains(&("Service".to_string(), "r1-bot-mcp-conn".to_string())));
}
