//! ESO object library: golden renders, inventory validation, and driver
//! behaviour against a scripted in-memory kubectl.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use base64::Engine as _;
use serde_json::{json, Value};

use curie::provider::eso::{
    self, render_external_secret, render_push_secret, render_secret_store, render_service_account,
    Kubectl, KubectlOutput, SeedOutcome, StoreSpec, SyncEntry,
};
use curie::provider::{
    InventoryClass, InventoryEntry, RotationOwner, SecretMaterial, UpdatePolicy,
};

const NS: &str = "curie";
const STORE: &str = "curie-aws";
const PREFIX: &str = "curie/test";
const REFRESH: &str = "1h";
const SEED_VALUE: &str = "s3cr3t-seed-value-9f2Q";

fn golden(name: &str) -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/eso")
        .join(name);
    let raw = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    serde_json::from_str(&raw).expect("golden json")
}

fn store_spec() -> StoreSpec {
    StoreSpec {
        name: STORE.to_string(),
        namespace: NS.to_string(),
        region: "us-east-1".to_string(),
        service_account: "curie-eso".to_string(),
        role_arn: "arn:aws:iam::000000000000:role/curie-eso-test".to_string(),
    }
}

fn entry(logical: &str, target: &str, keys: &[&str], owner: RotationOwner) -> InventoryEntry {
    InventoryEntry {
        logical_name: logical.to_string(),
        class: InventoryClass::External,
        target: target.to_string(),
        keys: keys.iter().map(|k| k.to_string()).collect(),
        consumers: vec!["curie-api".to_string()],
        rotation_owner: owner,
        update_policy: UpdatePolicy::Replace,
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|v| v.to_string()).collect()
}

fn static_entry() -> SyncEntry {
    SyncEntry::from_inventory(
        &entry("platform", "curie-platform", &["A", "B"], RotationOwner::Sm),
        PREFIX,
        &[],
    )
    .expect("static entry")
}

fn split_entry() -> SyncEntry {
    SyncEntry::from_inventory(
        &entry(
            "finance",
            "curie-finance",
            &["CLIENT_ID", "CLIENT_SECRET", "REFRESH_TOKEN"],
            RotationOwner::Workload("finance-agent".to_string()),
        ),
        PREFIX,
        &strings(&["REFRESH_TOKEN"]),
    )
    .expect("split entry")
}

// ---------------------------------------------------------------- goldens

#[test]
fn service_account_matches_golden() {
    assert_eq!(
        render_service_account(&store_spec()),
        golden("service-account.json")
    );
}

#[test]
fn secret_store_matches_golden_and_has_no_role() {
    let store = render_secret_store(&store_spec());
    assert_eq!(store, golden("store.json"));
    assert!(store["spec"]["provider"]["aws"].get("role").is_none());
}

#[test]
fn static_external_matches_golden() {
    let entry = static_entry();
    assert_eq!(entry.name, "platform");
    assert_eq!(entry.target, "curie-platform");
    assert_eq!(entry.remote_key, "curie/test/platform");
    assert_eq!(
        render_external_secret(&entry, NS, STORE, REFRESH),
        golden("external-static.json")
    );
}

#[test]
fn split_external_matches_golden_and_omits_rotated_key() {
    let entry = split_entry();
    assert_eq!(entry.static_keys, strings(&["CLIENT_ID", "CLIENT_SECRET"]));
    assert_eq!(entry.rotated_keys, strings(&["REFRESH_TOKEN"]));
    let rendered = render_external_secret(&entry, NS, STORE, REFRESH);
    assert_eq!(rendered, golden("external-split.json"));
    assert!(!rendered.to_string().contains("REFRESH_TOKEN"));
}

#[test]
fn push_backup_matches_golden() {
    let entry = split_entry();
    assert_eq!(entry.backup_key(), "curie/test/finance-rotated");
    let push = render_push_secret(&entry, NS, STORE).expect("push secret for rotated keys");
    assert_eq!(push, golden("push-backup.json"));
}

#[test]
fn push_secret_absent_for_static_entry() {
    assert!(render_push_secret(&static_entry(), NS, STORE).is_none());
}

#[test]
fn from_inventory_rejects_workload_without_rotated_keys() {
    let e = entry(
        "finance",
        "curie-finance",
        &["A", "B"],
        RotationOwner::Workload("w".into()),
    );
    assert!(SyncEntry::from_inventory(&e, PREFIX, &[]).is_err());
}

#[test]
fn from_inventory_rejects_rotated_key_not_in_keys() {
    let e = entry(
        "finance",
        "curie-finance",
        &["A", "B"],
        RotationOwner::Workload("w".into()),
    );
    assert!(SyncEntry::from_inventory(&e, PREFIX, &strings(&["C"])).is_err());
}

#[test]
fn from_inventory_rejects_sm_owner_with_rotated_keys() {
    let e = entry("platform", "curie-platform", &["A", "B"], RotationOwner::Sm);
    assert!(SyncEntry::from_inventory(&e, PREFIX, &strings(&["A"])).is_err());
}

// ---------------------------------------------------------------- fake kubectl

#[derive(Clone, Copy, PartialEq)]
enum WriteScript {
    Normal,
    /// First replace: a concurrent writer adds the key first, so the rv is stale.
    ConcurrentAddBeforeFirstWrite,
    /// Every write attempt loses to a concurrent write that does not add the key.
    PersistentConflict,
    /// Create: another writer created the Secret with the key first.
    CreateRace,
}

#[derive(Clone, Copy, PartialEq)]
enum SyncScript {
    Never,
    ReadyChanged,
    NotReadyChanged,
    /// Every post-annotate read first reports an older in-flight reconcile
    /// (Ready, hash of the pre-annotate metadata plus another label change);
    /// `stale_reads` of those, then the correct hash.
    StaleThenCorrect,
    /// Post-annotate `get` sleeps `slow_get` and then returns a correct Ready.
    SlowCorrect,
}

struct State {
    calls: Vec<(Vec<String>, Option<Vec<u8>>)>,
    secret: Option<Value>,
    rv: u64,
    write_script: WriteScript,
    writes_seen: u32,
    external: Value,
    sync_script: SyncScript,
    sync_counter: u32,
    annotated: bool,
    stale_reads: u32,
    stale_served: u32,
    pre_annotate_metadata: Value,
    slow_get: Duration,
}

#[derive(Clone)]
struct FakeKubectl(Arc<Mutex<State>>);

const CONCURRENT_VALUE: &str = "concurrent-writer-value";

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

fn conflict() -> Result<KubectlOutput> {
    fail("Error from server (Conflict): error when replacing \"STDIN\": Operation cannot be fulfilled on secrets \"curie-platform\": the object has been modified; please apply your changes to the latest version and try again")
}

/// Normalize stringData into base64 data, as the apiserver does.
fn normalize(mut object: Value) -> Value {
    let mut data = object.get("data").cloned().unwrap_or_else(|| json!({}));
    if let Some(Value::Object(string_data)) = object.get("stringData").cloned() {
        for (k, v) in string_data {
            data[k] = json!(b64(v.as_str().unwrap()));
        }
    }
    let map = object.as_object_mut().unwrap();
    map.remove("stringData");
    map.insert("data".into(), data);
    object
}

impl FakeKubectl {
    fn new(secret: Option<Value>) -> Self {
        let mut state = State {
            calls: Vec::new(),
            secret: None,
            rv: 100,
            write_script: WriteScript::Normal,
            writes_seen: 0,
            external: json!({}),
            sync_script: SyncScript::Never,
            sync_counter: 0,
            annotated: false,
            stale_reads: 0,
            stale_served: 0,
            pre_annotate_metadata: json!({}),
            slow_get: Duration::ZERO,
        };
        if let Some(s) = secret {
            state.rv += 1;
            let mut s = normalize(s);
            s["metadata"]["resourceVersion"] = json!(state.rv.to_string());
            state.secret = Some(s);
        }
        Self(Arc::new(Mutex::new(state)))
    }

    fn with_write_script(self, script: WriteScript) -> Self {
        self.0.lock().unwrap().write_script = script;
        self
    }

    fn with_external(self, external: Value, script: SyncScript) -> Self {
        {
            let mut s = self.0.lock().unwrap();
            s.external = external;
            s.sync_script = script;
        }
        self
    }

    fn with_stale_reads(self, n: u32) -> Self {
        self.0.lock().unwrap().stale_reads = n;
        self
    }

    fn with_slow_get(self, d: Duration) -> Self {
        self.0.lock().unwrap().slow_get = d;
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

    fn stored(&self) -> Option<Value> {
        self.0.lock().unwrap().secret.clone()
    }

    fn write_calls(&self) -> Vec<(Vec<String>, Option<Vec<u8>>)> {
        self.calls()
            .into_iter()
            .filter(|(a, _)| {
                a.iter()
                    .any(|t| ["create", "replace", "patch", "apply"].contains(&t.as_str()))
            })
            .collect()
    }
}

fn has(args: &[String], token: &str) -> bool {
    args.iter().any(|a| a == token)
}

fn mentions_external(args: &[String]) -> bool {
    args.iter()
        .any(|a| a.to_ascii_lowercase().contains("externalsecret"))
}

fn secret_name(state: &State) -> String {
    state
        .secret
        .as_ref()
        .and_then(|s| s["metadata"]["name"].as_str().map(str::to_string))
        .unwrap_or_else(|| "curie-platform".into())
}

impl Kubectl for FakeKubectl {
    fn run(&self, args: &[String], stdin: Option<&[u8]>) -> Result<KubectlOutput> {
        let mut s = self.0.lock().unwrap();
        s.calls.push((args.to_vec(), stdin.map(<[u8]>::to_vec)));
        let body: Option<Value> = stdin.and_then(|b| serde_json::from_slice(b).ok());

        if has(args, "rollout") {
            return ok("deployment successfully rolled out\n");
        }
        if has(args, "annotate") || (has(args, "patch") && mentions_external(args)) {
            s.sync_counter += 1;
            s.pre_annotate_metadata = s.external["metadata"].clone();
            // Record the force-sync annotation in metadata, as the apiserver does.
            let prefix = format!("{}=", eso::FORCE_SYNC_ANNOTATION);
            let mut value = args
                .iter()
                .find_map(|a| a.strip_prefix(&prefix).map(str::to_string));
            if value.is_none() {
                if let Some(b) = patch_body(args, &stdin.map(<[u8]>::to_vec)) {
                    value = b["metadata"]["annotations"][eso::FORCE_SYNC_ANNOTATION]
                        .as_str()
                        .map(str::to_string);
                }
            }
            let value = value.expect("force-sync annotation value in annotate call");
            if !s.external["metadata"]["annotations"].is_object() {
                s.external["metadata"]["annotations"] = json!({});
            }
            s.external["metadata"]["annotations"][eso::FORCE_SYNC_ANNOTATION] = json!(value);
            s.annotated = true;
            s.stale_served = 0;
            let ready = match s.sync_script {
                SyncScript::Never => None,
                SyncScript::NotReadyChanged => Some("False"),
                _ => Some("True"),
            };
            if let Some(ready) = ready {
                if matches!(
                    s.sync_script,
                    SyncScript::ReadyChanged | SyncScript::NotReadyChanged
                ) {
                    let synced = eso::synced_version(&s.external["metadata"]);
                    s.external["status"]["syncedResourceVersion"] = json!(synced);
                }
                s.external["status"]["refreshTime"] = json!("2026-09-22T00:00:00Z");
                s.external["status"]["conditions"] =
                    json!([{"type": "Ready", "status": ready, "reason": "x"}]);
            }
            return ok("externalsecret annotated\n");
        }
        if has(args, "patch") {
            return ok("patched\n");
        }
        let body_is_secret = body.as_ref().is_some_and(|b| b["kind"] == json!("Secret"));
        if has(args, "apply") && !body_is_secret {
            return ok("applied\n");
        }
        if has(args, "get") {
            if mentions_external(args) {
                if s.annotated {
                    match s.sync_script {
                        SyncScript::StaleThenCorrect => {
                            let synced = if s.stale_served < s.stale_reads {
                                s.stale_served += 1;
                                // An older reconcile that started before our
                                // annotate, over another writer's label change.
                                let mut older = s.pre_annotate_metadata.clone();
                                older["labels"]["other-writer"] = json!("x");
                                eso::synced_version(&older)
                            } else {
                                eso::synced_version(&s.external["metadata"])
                            };
                            s.external["status"]["syncedResourceVersion"] = json!(synced);
                        }
                        SyncScript::SlowCorrect => {
                            let d = s.slow_get;
                            s.external["status"]["syncedResourceVersion"] =
                                json!(eso::synced_version(&s.external["metadata"]));
                            let out = s.external.to_string();
                            drop(s);
                            std::thread::sleep(d);
                            return ok(out);
                        }
                        _ => {}
                    }
                }
                return ok(s.external.to_string());
            }
            return match &s.secret {
                Some(secret) => ok(secret.to_string()),
                None => fail(format!(
                    "Error from server (NotFound): secrets \"{}\" not found",
                    secret_name(&s)
                )),
            };
        }
        if has(args, "create") {
            if s.write_script == WriteScript::CreateRace && s.writes_seen == 0 {
                s.writes_seen += 1;
                s.rv += 1;
                let mut other = body.clone().unwrap_or_else(|| json!({"metadata": {}}));
                other = normalize(other);
                other["data"] = json!({ "K": b64(CONCURRENT_VALUE) });
                other["metadata"]["resourceVersion"] = json!(s.rv.to_string());
                s.secret = Some(other);
                return fail("Error from server (AlreadyExists): error when creating \"STDIN\": secrets \"curie-platform\" already exists");
            }
            if s.secret.is_some() {
                return fail(
                    "Error from server (AlreadyExists): secrets \"curie-platform\" already exists",
                );
            }
            s.rv += 1;
            let mut obj = normalize(body.expect("create body on stdin"));
            obj["metadata"]["resourceVersion"] = json!(s.rv.to_string());
            s.secret = Some(obj);
            return ok("secret/curie-platform created\n");
        }
        if has(args, "replace") || has(args, "apply") {
            let obj = body.expect("write body on stdin");
            s.writes_seen += 1;
            match s.write_script {
                WriteScript::ConcurrentAddBeforeFirstWrite if s.writes_seen == 1 => {
                    s.rv += 1;
                    let rv = s.rv.to_string();
                    let cur = s.secret.as_mut().expect("secret exists");
                    cur["data"]["K"] = json!(b64(CONCURRENT_VALUE));
                    cur["metadata"]["resourceVersion"] = json!(rv);
                }
                WriteScript::PersistentConflict => {
                    s.rv += 1;
                    let rv = s.rv.to_string();
                    let cur = s.secret.as_mut().expect("secret exists");
                    cur["metadata"]["resourceVersion"] = json!(rv);
                }
                _ => {}
            }
            let current_rv = s
                .secret
                .as_ref()
                .map(|c| c["metadata"]["resourceVersion"].clone());
            if current_rv != Some(obj["metadata"]["resourceVersion"].clone()) {
                return conflict();
            }
            s.rv += 1;
            let mut obj = normalize(obj);
            obj["metadata"]["resourceVersion"] = json!(s.rv.to_string());
            s.secret = Some(obj);
            return ok("secret/curie-platform replaced\n");
        }
        ok("")
    }
}

fn base_secret(data: &[(&str, &str)]) -> Value {
    let mut d = json!({});
    for (k, v) in data {
        d[*k] = json!(b64(v));
    }
    json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "curie-platform", "namespace": NS},
        "type": "Opaque",
        "data": d,
    })
}

fn assert_no_leak(fake: &FakeKubectl) {
    let encoded = b64(SEED_VALUE);
    for (args, _) in fake.calls() {
        for a in &args {
            assert!(!a.contains(SEED_VALUE), "value in argv: {args:?}");
            assert!(!a.contains(&encoded), "encoded value in argv: {args:?}");
        }
    }
}

fn seed(fake: &FakeKubectl, attempts: u32) -> Result<SeedOutcome> {
    eso::seed_key(
        fake,
        NS,
        "curie-platform",
        "K",
        &SecretMaterial::new(SEED_VALUE),
        attempts,
    )
}

// ---------------------------------------------------------------- seed

#[test]
fn seed_creates_absent_secret_without_value_in_argv() {
    let fake = FakeKubectl::new(None);
    assert_eq!(seed(&fake, 3).unwrap(), SeedOutcome::Created);
    assert_eq!(fake.stored_value("K").as_deref(), Some(SEED_VALUE));
    assert_no_leak(&fake);
}

#[test]
fn seed_skips_present_key_without_writing() {
    let fake = FakeKubectl::new(Some(base_secret(&[("K", "existing"), ("A", "a")])));
    assert_eq!(seed(&fake, 3).unwrap(), SeedOutcome::AlreadyPresent);
    assert!(
        fake.write_calls().is_empty(),
        "wrote: {:?}",
        fake.write_calls()
    );
    assert_eq!(fake.stored_value("K").as_deref(), Some("existing"));
    assert_no_leak(&fake);
}

#[test]
fn seed_adds_key_keeping_siblings_with_read_resource_version() {
    let fake = FakeKubectl::new(Some(base_secret(&[("A", "a"), ("B", "b")])));
    let read_rv = fake.stored().unwrap()["metadata"]["resourceVersion"].clone();
    assert_eq!(seed(&fake, 3).unwrap(), SeedOutcome::Added);
    let writes = fake.write_calls();
    assert_eq!(writes.len(), 1, "{writes:?}");
    let body: Value = serde_json::from_slice(writes[0].1.as_ref().expect("stdin")).unwrap();
    assert_eq!(body["metadata"]["resourceVersion"], read_rv);
    assert_eq!(fake.stored_value("A").as_deref(), Some("a"));
    assert_eq!(fake.stored_value("B").as_deref(), Some("b"));
    assert_eq!(fake.stored_value("K").as_deref(), Some(SEED_VALUE));
    assert_no_leak(&fake);
}

#[test]
fn seed_conflict_rereads_and_keeps_concurrent_value() {
    let fake = FakeKubectl::new(Some(base_secret(&[("A", "a")])))
        .with_write_script(WriteScript::ConcurrentAddBeforeFirstWrite);
    assert_eq!(seed(&fake, 5).unwrap(), SeedOutcome::AlreadyPresent);
    assert_eq!(fake.stored_value("K").as_deref(), Some(CONCURRENT_VALUE));
    assert_eq!(fake.stored_value("A").as_deref(), Some("a"));
    assert_eq!(
        fake.write_calls().len(),
        1,
        "no second write after the re-read"
    );
    assert_no_leak(&fake);
}

#[test]
fn seed_create_race_rereads_and_keeps_concurrent_value() {
    let fake = FakeKubectl::new(None).with_write_script(WriteScript::CreateRace);
    assert_eq!(seed(&fake, 5).unwrap(), SeedOutcome::AlreadyPresent);
    assert_eq!(fake.stored_value("K").as_deref(), Some(CONCURRENT_VALUE));
    assert_no_leak(&fake);
}

#[test]
fn seed_persistent_conflict_errors_without_value() {
    let fake = FakeKubectl::new(Some(base_secret(&[("A", "a")])))
        .with_write_script(WriteScript::PersistentConflict);
    let err = seed(&fake, 3).expect_err("attempts exhausted");
    let msg = format!("{err:#} {err:?}");
    assert!(!msg.contains(SEED_VALUE), "{msg}");
    assert!(!msg.contains(&b64(SEED_VALUE)), "{msg}");
    assert!(fake.write_calls().len() <= 3);
    assert!(fake.stored_value("K").is_none());
    assert_no_leak(&fake);
}

// ---------------------------------------------------------------- force sync

fn external(ready: &str, synced: &str) -> Value {
    json!({
        "apiVersion": "external-secrets.io/v1",
        "kind": "ExternalSecret",
        "metadata": {"name": "platform", "namespace": NS, "generation": 1},
        "status": {
            "refreshTime": "2026-09-22T00:00:00Z",
            "syncedResourceVersion": synced,
            "conditions": [{"type": "Ready", "status": ready, "reason": "SecretSynced"}]
        }
    })
}

fn force(fake: &FakeKubectl, timeout_ms: u64) -> Result<()> {
    eso::force_sync_and_wait(
        fake,
        NS,
        "platform",
        Duration::from_millis(timeout_ms),
        Duration::from_millis(20),
    )
}

#[test]
fn force_sync_times_out_when_already_ready_but_not_reconciled() {
    let fake = FakeKubectl::new(None).with_external(external("True", "1-aaa"), SyncScript::Never);
    let start = Instant::now();
    let err = force(&fake, 300).expect_err("unchanged syncedResourceVersion must not satisfy");
    assert!(start.elapsed() < Duration::from_secs(5));
    assert!(format!("{err:#}").contains("platform"), "{err:#}");
}

#[test]
fn force_sync_returns_once_version_changes_and_ready() {
    let fake =
        FakeKubectl::new(None).with_external(external("True", "1-aaa"), SyncScript::ReadyChanged);
    force(&fake, 2000).expect("reconciled");
}

#[test]
fn force_sync_uses_distinct_annotation_values() {
    let fake =
        FakeKubectl::new(None).with_external(external("True", "1-aaa"), SyncScript::ReadyChanged);
    force(&fake, 2000).unwrap();
    force(&fake, 2000).unwrap();
    let marks: Vec<String> = fake
        .calls()
        .into_iter()
        .filter(|(a, _)| has(a, "annotate") || (has(a, "patch") && mentions_external(a)))
        .map(|(a, stdin)| {
            let mut joined: Vec<String> = a
                .into_iter()
                .filter(|t| t.contains(eso::FORCE_SYNC_ANNOTATION))
                .collect();
            if let Some(b) = stdin {
                joined.push(String::from_utf8_lossy(&b).into_owned());
            }
            joined.join(" ")
        })
        .collect();
    assert_eq!(marks.len(), 2, "{marks:?}");
    assert!(
        marks.iter().all(|m| m.contains(eso::FORCE_SYNC_ANNOTATION)),
        "{marks:?}"
    );
    assert_ne!(marks[0], marks[1]);
}

#[test]
fn synced_version_matches_live_eso_probe() {
    let meta = golden("synced-version-probe.json");
    assert_eq!(
        eso::synced_version(&meta),
        "1-51ed6f9f417d350db50794445de96046d372e51e8b3951b5369ac4b7"
    );
}

#[test]
fn synced_version_changes_with_labels_and_annotations() {
    let meta = golden("synced-version-probe.json");
    let base = eso::synced_version(&meta);
    let mut labelled = meta.clone();
    labelled["labels"]["extra"] = json!("y");
    let mut annotated = meta.clone();
    annotated["annotations"]["extra"] = json!("y");
    assert_ne!(eso::synced_version(&labelled), base);
    assert_ne!(eso::synced_version(&annotated), base);
    assert_ne!(
        eso::synced_version(&labelled),
        eso::synced_version(&annotated)
    );
    let bare = json!({"generation": 3});
    assert!(eso::synced_version(&bare).starts_with("3-"));
}

#[test]
fn force_sync_rejects_in_flight_older_reconcile() {
    // Only stale (older-reconcile) Ready reads: never Ok, even though the
    // version changed and Ready=True.
    let fake = FakeKubectl::new(None)
        .with_external(external("True", "1-aaa"), SyncScript::StaleThenCorrect)
        .with_stale_reads(u32::MAX);
    assert!(
        force(&fake, 300).is_err(),
        "older reconcile must not satisfy"
    );
}

#[test]
fn force_sync_waits_past_in_flight_reconcile_then_succeeds() {
    let fake = FakeKubectl::new(None)
        .with_external(external("True", "1-aaa"), SyncScript::StaleThenCorrect)
        .with_stale_reads(2);
    force(&fake, 2000).expect("correct hash after the stale reads");
    let s = fake.0.lock().unwrap();
    assert_eq!(
        s.stale_served, 2,
        "stale reads must have been observed and rejected"
    );
}

#[test]
fn force_sync_late_success_after_deadline_is_timeout() {
    let fake = FakeKubectl::new(None)
        .with_external(external("True", "1-aaa"), SyncScript::SlowCorrect)
        .with_slow_get(Duration::from_millis(500));
    let start = Instant::now();
    let err = force(&fake, 100).expect_err("success observed after the deadline must be Err");
    assert!(start.elapsed() < Duration::from_secs(5));
    assert!(format!("{err:#}").contains("platform"), "{err:#}");
}

#[test]
fn force_sync_not_ok_when_not_ready() {
    let fake = FakeKubectl::new(None)
        .with_external(external("True", "1-aaa"), SyncScript::NotReadyChanged);
    assert!(force(&fake, 300).is_err());
}

// ---------------------------------------------------------------- rollout and apply

fn patch_body(args: &[String], stdin: &Option<Vec<u8>>) -> Option<Value> {
    if let Some(b) = stdin {
        if let Ok(v) = serde_json::from_slice(b) {
            return Some(v);
        }
    }
    for (i, a) in args.iter().enumerate() {
        if let Some(rest) = a.strip_prefix("--patch=").or_else(|| a.strip_prefix("-p=")) {
            return serde_json::from_str(rest).ok();
        }
        if a == "-p" || a == "--patch" {
            return args.get(i + 1).and_then(|n| serde_json::from_str(n).ok());
        }
    }
    None
}

#[test]
fn rollout_stamps_template_then_waits() {
    let fake = FakeKubectl::new(None);
    let deps = strings(&["curie-api", "curie-worker"]);
    eso::rollout_consumers(&fake, NS, &deps, "v-42", Duration::from_secs(90)).unwrap();
    let calls = fake.calls();
    for dep in &deps {
        let patch_idx = calls
            .iter()
            .position(|(a, stdin)| {
                has(a, "patch")
                    && a.iter().any(|t| t.contains(dep.as_str()))
                    && patch_body(a, stdin).is_some_and(|b| {
                        b["spec"]["template"]["metadata"]["annotations"]
                            [eso::PROVIDER_VERSION_ANNOTATION]
                            == json!("v-42")
                    })
            })
            .unwrap_or_else(|| panic!("no stamp patch for {dep}: {calls:?}"));
        let (pargs, _) = &calls[patch_idx];
        assert!(
            pargs.iter().any(|t| t == "--type=merge")
                || pargs
                    .windows(2)
                    .any(|w| w[0] == "--type" && w[1] == "merge"),
            "{pargs:?}"
        );
        let status_idx = calls
            .iter()
            .position(|(a, _)| {
                has(a, "rollout")
                    && has(a, "status")
                    && a.iter().any(|t| t.contains(dep.as_str()))
                    && a.iter().any(|t| t.starts_with("--timeout"))
            })
            .unwrap_or_else(|| panic!("no rollout status for {dep}: {calls:?}"));
        assert!(status_idx > patch_idx);
    }
}

#[test]
fn apply_is_server_side_with_field_manager_and_stdin_json() {
    let fake = FakeKubectl::new(None);
    let objects = vec![
        render_secret_store(&store_spec()),
        render_service_account(&store_spec()),
    ];
    eso::apply(&fake, NS, &objects).unwrap();
    let applies: Vec<_> = fake
        .calls()
        .into_iter()
        .filter(|(a, _)| has(a, "apply"))
        .collect();
    assert!(!applies.is_empty());
    let mut seen = Vec::new();
    for (args, stdin) in applies {
        assert!(has(&args, "--server-side"), "{args:?}");
        assert!(has(&args, "--field-manager=curie-secrets"), "{args:?}");
        let body: Value = serde_json::from_slice(&stdin.expect("json on stdin")).expect("json");
        match body {
            Value::Array(items) => seen.extend(items),
            v if v.get("items").is_some() => seen.extend(v["items"].as_array().unwrap().clone()),
            v => seen.push(v),
        }
    }
    for object in &objects {
        assert!(seen.contains(object), "missing {object}");
    }
}
