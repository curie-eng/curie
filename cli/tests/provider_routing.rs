//! Provider routing for `curie apply` (ADR 0163 decisions 2, 3 and 5).
//!
//! Library level: the planner, the preflight, generation against an in-memory
//! fake `SecretsProvider`, the grouped ExternalSecret render, and the sync
//! driver against a scripted fake `Kubectl`. Nothing here shells out, so the
//! suite passes with no `aws` binary and no ESO.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use base64::Engine as _;
use serde_json::{json, Value};

use curie::installation::Installation;
use curie::ops::{up_commands, CommonOpts, GithubTokenPlan, UpOpts};
use curie::provider::eso::{synced_version, Kubectl, KubectlOutput, FORCE_SYNC_ANNOTATION};
use curie::provider::routing::{
    check_store, dropped_value_keys, dry_run_lines, generate, plan_routing, preflight,
    render_external_secrets, route_up_opts, sync, RouteReason, RoutingInputs, RoutingPlan,
    DEFAULT_LANGFUSE_PUBLIC_KEY, HISTORY_MAX, REFRESH_INTERVAL,
};
use curie::provider::{
    platform_inventory, InventoryEntry, ObjectMetadata, ObjectVersion, ProviderError, PutRequest,
    SecretMaterial, SecretsProvider, StoredObject,
};

const RELEASE: &str = "rel";
const NAMESPACE: &str = "ns-rel";
const REMOTE_PREFIX: &str = "curie/test/rel";
const STORE: &str = "rel-aws";

const MODEL_VALUE: &str = "sentinel-model-value-7f3a";
const GITHUB_VALUE: &str = "sentinel-github-value-7f3a";
const SLACK_BOT_VALUE: &str = "sentinel-slack-bot-value-7f3a";
const PG_VALUE: &str = "sentinel-postgres-value-7f3a";
const SALT_VALUE: &str = "sentinel-salt-value-7f3a";
const SEALING_VALUE: &str = "sentinel-sealing-value-7f3a";
const API_KEY_VALUE: &str = "sentinel-apikey-value-7f3a";
const VALKEY_VALUE: &str = "sentinel-valkey-value-7f3a";
const MAIL_VALUE: &str = "sentinel-mail-value-7f3a";
const PROJECT_KEY_VALUE: &str = "sentinel-project-key-value-7f3a";
const INSTALLATION_VALUE: &str = "sentinel-installation-value-7f3a";

const GENERATED: [&str; 12] = [
    "installation-id",
    "postgres-password",
    "valkey-password",
    "clickhouse-password",
    "object-store-secret-key",
    "langfuse-data-keys",
    "langfuse-nextauth-secret",
    "langfuse-init-project-secret-key",
    "langfuse-init-user-password",
    "otlp-auth-header",
    "github-webhook-secret",
    "sealing-private-key",
];

const DECLARED_BLOCK: &str = "credentials:\n  model: ROUTING_TEST_MODEL_KEY\n  github_token: ROUTING_TEST_GITHUB_TOKEN\ncomms:\n  slack:\n    app_token: ROUTING_TEST_APP_TOKEN\n    bot_token: ROUTING_TEST_BOT_TOKEN\n";

fn install(extra: &str) -> Installation {
    let raw = format!(
        "version: 1\ninstall:\n  namespace: {NAMESPACE}\n  release: {RELEASE}\nsecrets:\n  provider: aws\n  region: us-east-1\n  prefix: curie/test\n  role_arn: arn:aws:iam::000000000000:role/curie-sync\n{extra}"
    );
    Installation::parse(&raw).expect("test curie.yaml parses")
}

fn plan_with(
    cfg: &Installation,
    live: Option<&Value>,
    sm: Option<&BTreeMap<String, Vec<String>>>,
) -> Result<RoutingPlan> {
    plan_routing(&RoutingInputs { cfg, live, sm })
}

fn fresh_declared_plan() -> RoutingPlan {
    let cfg = install(DECLARED_BLOCK);
    plan_with(&cfg, None, None).expect("fresh declared plan")
}

fn inventory() -> BTreeMap<String, InventoryEntry> {
    platform_inventory()
        .expect("platform inventory")
        .into_iter()
        .map(|entry| (entry.logical_name.clone(), entry))
        .collect()
}

fn routed_names(plan: &RoutingPlan) -> BTreeSet<String> {
    plan.entries
        .iter()
        .map(|e| e.logical_name.clone())
        .collect()
}

fn reason_of(plan: &RoutingPlan, logical: &str) -> Option<RouteReason> {
    plan.entries
        .iter()
        .find(|e| e.logical_name == logical)
        .map(|e| e.reason)
}

/// Secrets Manager holding every routed entry with every routed key.
fn full_sm(plan: &RoutingPlan) -> BTreeMap<String, Vec<String>> {
    plan.entries
        .iter()
        .map(|e| (e.logical_name.clone(), e.keys.clone()))
        .collect()
}

fn err_text(error: &anyhow::Error) -> String {
    format!("{error:#} {error:?}")
}

// ------------------------------------------------------------------ planning

#[test]
fn store_name_is_release_scoped() {
    assert_eq!(curie::provider::reconcile::store_name("rel"), "rel-aws");
    assert_eq!(HISTORY_MAX, 3);
}

#[test]
fn fresh_provider_install_routes_generated_and_declared_entries_through_inventory_knobs() {
    let plan = fresh_declared_plan();
    let inventory = inventory();

    assert_eq!(plan.release, RELEASE);
    assert_eq!(plan.namespace, NAMESPACE);
    assert_eq!(plan.remote_prefix, REMOTE_PREFIX);
    assert!(plan.fresh, "no live release means fresh");
    assert_eq!(plan.langfuse_public_key, DEFAULT_LANGFUSE_PUBLIC_KEY);

    let mut expected: BTreeSet<String> = GENERATED.iter().map(|s| s.to_string()).collect();
    for declared in [
        "runner-model-credentials",
        "github-token",
        "slack-app-token",
        "slack-bot-token",
    ] {
        expected.insert(declared.to_string());
    }
    assert_eq!(routed_names(&plan), expected, "routed entries");

    for logical in GENERATED {
        assert_eq!(
            reason_of(&plan, logical),
            Some(RouteReason::Generated),
            "{logical}"
        );
    }
    for logical in [
        "runner-model-credentials",
        "github-token",
        "slack-app-token",
        "slack-bot-token",
    ] {
        assert_eq!(
            reason_of(&plan, logical),
            Some(RouteReason::Declared),
            "{logical}"
        );
    }

    for entry in &plan.entries {
        let row = &inventory[&entry.logical_name];
        assert_eq!(
            entry.remote_id,
            format!("{REMOTE_PREFIX}/{}", entry.logical_name)
        );
        assert_eq!(entry.target, row.target.replace("{release}", RELEASE));
        assert_eq!(entry.keys, row.keys, "{}", entry.logical_name);
    }

    // Knob sets derived from the inventory, so this test tracks it.
    let mut want: BTreeSet<(String, String)> = BTreeSet::new();
    let mut key_knobs: BTreeSet<String> = BTreeSet::new();
    for entry in &plan.entries {
        let row = &inventory[&entry.logical_name];
        let chart = row.chart.as_ref().expect("routed rows carry chart knobs");
        assert!(
            !chart.knobs.is_empty(),
            "{} has no knob",
            entry.logical_name
        );
        for knob in &chart.knobs {
            want.insert((knob.secret.clone(), entry.target.clone()));
            if let Some(key) = &knob.key {
                assert_eq!(entry.keys.len(), 1, "{}", entry.logical_name);
                want.insert((key.clone(), entry.keys[0].clone()));
                key_knobs.insert(key.clone());
            }
        }
    }
    want.insert((
        "agentSandbox.runner.fakeModel".to_string(),
        "false".to_string(),
    ));
    want.insert(("dispatcher.slack.appToken".to_string(), String::new()));
    want.insert(("dispatcher.slack.botToken".to_string(), String::new()));
    want.insert(("worker.slackApiBaseUrl".to_string(), String::new()));

    let got: BTreeSet<(String, String)> = plan.knob_sets.iter().cloned().collect();
    assert_eq!(got, want, "knob sets");

    // One value per path: no knob set twice to different values.
    let mut by_path: BTreeMap<&str, &str> = BTreeMap::new();
    for (path, value) in &plan.knob_sets {
        if let Some(previous) = by_path.insert(path, value) {
            assert_eq!(previous, value, "{path} set twice with different values");
        }
    }
    for (path, value) in &plan.knob_sets {
        if key_knobs.contains(path) {
            assert!(!value.is_empty(), "key knob {path} set empty");
        }
    }
    assert!(plan
        .knob_sets
        .iter()
        .any(|(k, v)| k == "installation.idExistingSecret" && v == "rel-curie-installation-id"));
}

#[test]
fn undeclared_externals_route_only_when_present_in_sm_or_live() {
    let bare = install("");
    let plan = plan_with(&bare, None, Some(&BTreeMap::new())).expect("bare plan");
    let names = routed_names(&plan);
    for absent in [
        "runner-model-credentials",
        "github-token",
        "slack-app-token",
        "slack-bot-token",
        "slack-signing-secret",
        "sealing-previous-private-key",
        "github-app-private-key",
        "mail-agentmail-api-key",
        "mail-egress-secret",
        "adapter-credentials",
        "grafana-admin",
        "otlp-headers",
        "image-pull",
        "api-ingress-tls",
    ] {
        assert!(!names.contains(absent), "{absent} must not be routed");
    }
    assert!(!plan
        .knob_sets
        .iter()
        .any(|(k, _)| k == "agentSandbox.runner.fakeModel"));
    assert!(!plan
        .knob_sets
        .iter()
        .any(|(k, _)| k.starts_with("dispatcher.slack")));

    let mut sm = BTreeMap::new();
    sm.insert(
        "sealing-previous-private-key".to_string(),
        vec!["sealingPreviousPrivateKey".to_string()],
    );
    sm.insert(
        "github-app-private-key".to_string(),
        vec!["githubAppPrivateKey".to_string()],
    );
    sm.insert(
        "slack-signing-secret".to_string(),
        vec!["slackSigningSecret".to_string()],
    );
    let plan = plan_with(&bare, None, Some(&sm)).expect("optional plan");
    assert_eq!(
        reason_of(&plan, "sealing-previous-private-key"),
        Some(RouteReason::Optional)
    );
    assert_eq!(
        reason_of(&plan, "github-app-private-key"),
        Some(RouteReason::Optional)
    );
    assert_eq!(
        reason_of(&plan, "slack-signing-secret"),
        None,
        "slack signing secret needs comms.slack"
    );

    let slack = install(
        "comms:\n  slack:\n    app_token: ROUTING_TEST_APP_TOKEN\n    bot_token: ROUTING_TEST_BOT_TOKEN\n",
    );
    let plan = plan_with(&slack, None, Some(&sm)).expect("slack plan");
    assert_eq!(
        reason_of(&plan, "slack-signing-secret"),
        Some(RouteReason::Optional)
    );
    let plan = plan_with(&slack, None, Some(&BTreeMap::new())).expect("slack plan, empty sm");
    assert_eq!(reason_of(&plan, "slack-signing-secret"), None);

    let live = json!({"api": {"githubToken": GITHUB_VALUE}});
    let plan = plan_with(&bare, Some(&live), Some(&BTreeMap::new())).expect("live token plan");
    assert_eq!(
        reason_of(&plan, "github-token"),
        Some(RouteReason::Optional)
    );
    assert!(!format!("{plan:?}").contains(GITHUB_VALUE));

    let live = json!({"api": {"githubToken": ""}});
    let plan = plan_with(&bare, Some(&live), Some(&BTreeMap::new())).expect("empty live token");
    assert_eq!(reason_of(&plan, "github-token"), None);
}

fn assert_refused(result: Result<RoutingPlan>, names: &str, value: Option<&str>) {
    let error = result.expect_err("must be refused");
    let text = err_text(&error);
    assert!(text.contains(names), "refusal must name {names}: {text}");
    if let Some(value) = value {
        assert!(!text.contains(value), "refusal leaked a value: {text}");
    }
}

#[test]
fn operator_sets_of_routed_knobs_or_dropped_values_are_refused() {
    let cfg = install("set:\n  postgres.existingSecret: operator-postgres\n");
    assert_refused(plan_with(&cfg, None, None), "postgres.existingSecret", None);

    let cfg = install(&format!("set:\n  postgres.auth.password: {PG_VALUE}\n"));
    assert_refused(
        plan_with(&cfg, None, None),
        "postgres.auth.password",
        Some(PG_VALUE),
    );
}

#[test]
fn a_deployed_or_credentialed_mail_adapter_is_refused() {
    let cfg = install("");
    let live = json!({"mailAdapter": {"deploy": true}});
    assert_refused(plan_with(&cfg, Some(&live), None), "mailAdapter", None);

    let live = json!({"mailAdapter": {"deploy": false, "agentmail": {"apiKey": MAIL_VALUE}}});
    assert_refused(
        plan_with(&cfg, Some(&live), None),
        "mailAdapter",
        Some(MAIL_VALUE),
    );
}

// ----------------------------------------------------------------- preflight

#[test]
fn preflight_refuses_a_missing_declared_entry_on_a_fresh_install() {
    let cfg = install("credentials:\n  model: ROUTING_TEST_MODEL_KEY\n");
    let plan = plan_with(&cfg, None, Some(&BTreeMap::new())).expect("plan");
    let error = preflight(&plan, &BTreeMap::new()).expect_err("declared entry missing");
    assert!(
        err_text(&error).contains("curie/test/rel/runner-model-credentials"),
        "{}",
        err_text(&error)
    );
}

#[test]
fn preflight_lists_missing_generated_entries_on_a_fresh_install() {
    let cfg = install("");
    let plan = plan_with(&cfg, None, Some(&BTreeMap::new())).expect("plan");
    let to_create = preflight(&plan, &BTreeMap::new()).expect("fresh generates");
    let got: BTreeSet<&str> = to_create.iter().map(String::as_str).collect();
    assert!(got.contains("installation-id"), "{to_create:?}");
    assert_eq!(got, GENERATED.iter().copied().collect(), "{to_create:?}");
}

#[test]
fn preflight_refuses_missing_generated_entries_on_an_existing_release() {
    let cfg = install("");
    let live = json!({});
    let plan = plan_with(&cfg, Some(&live), Some(&BTreeMap::new())).expect("plan");
    assert!(!plan.fresh, "a live release is not fresh");
    let error = preflight(&plan, &BTreeMap::new()).expect_err("existing release never generates");
    assert!(
        err_text(&error).contains("curie/test/rel/installation-id"),
        "{}",
        err_text(&error)
    );
}

#[test]
fn preflight_refuses_an_object_missing_a_routed_key_and_passes_when_complete() {
    let plan = fresh_declared_plan();
    let mut sm = full_sm(&plan);
    assert!(preflight(&plan, &sm).expect("complete").is_empty());

    sm.insert(
        "langfuse-data-keys".to_string(),
        vec!["langfuseSalt".to_string()],
    );
    let error = preflight(&plan, &sm).expect_err("missing key");
    let text = err_text(&error);
    assert!(text.contains("langfuseEncryptionKey"), "{text}");
    assert!(text.contains("curie/test/rel/langfuse-data-keys"), "{text}");
}

// ---------------------------------------------------------------- generation

#[derive(Default)]
struct FakeProvider {
    objects: Mutex<BTreeMap<String, String>>,
    puts: Mutex<Vec<(String, Option<String>, String)>>,
    fail_put: Option<String>,
}

impl FakeProvider {
    fn with(self, name: &str, body: Value) -> Self {
        self.objects
            .lock()
            .unwrap()
            .insert(name.to_string(), body.to_string());
        self
    }

    fn body(&self, name: &str) -> Value {
        let raw = self.objects.lock().unwrap()[name].clone();
        serde_json::from_str(&raw).expect("stored JSON object")
    }

    fn puts(&self) -> Vec<(String, Option<String>, String)> {
        self.puts.lock().unwrap().clone()
    }
}

fn keys_of(raw: &str) -> Vec<String> {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|v| v.as_object().map(|m| m.keys().cloned().collect()))
        .unwrap_or_default()
}

impl SecretsProvider for FakeProvider {
    fn put(&self, request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError> {
        let material = request.material.expose().to_string();
        self.puts.lock().unwrap().push((
            request.name.to_string(),
            request.expected_version.map(str::to_string),
            material.clone(),
        ));
        if self.fail_put.as_deref() == Some(request.name) {
            return Err(ProviderError::Unavailable {
                name: request.name.to_string(),
                status: 503,
            });
        }
        let mut objects = self.objects.lock().unwrap();
        if request.expected_version.is_none() && objects.contains_key(request.name) {
            return Err(ProviderError::Conflict {
                name: request.name.to_string(),
                expected_version: None,
                actual_version: Some("1".to_string()),
            });
        }
        objects.insert(request.name.to_string(), material);
        Ok(ObjectVersion {
            id: "1".to_string(),
        })
    }

    fn get(&self, name: &str, _version: Option<&str>) -> Result<StoredObject, ProviderError> {
        match self.objects.lock().unwrap().get(name) {
            Some(raw) => Ok(StoredObject {
                version: ObjectVersion {
                    id: "1".to_string(),
                },
                material: SecretMaterial::new(raw.clone()),
                key_names: keys_of(raw),
            }),
            None => Err(ProviderError::NotFound {
                name: name.to_string(),
            }),
        }
    }

    fn get_metadata(&self, name: &str) -> Result<ObjectMetadata, ProviderError> {
        match self.objects.lock().unwrap().get(name) {
            Some(raw) => Ok(ObjectMetadata {
                name: name.to_string(),
                version: ObjectVersion {
                    id: "1".to_string(),
                },
                tags: BTreeMap::new(),
                key_names: keys_of(raw),
            }),
            None => Err(ProviderError::NotFound {
                name: name.to_string(),
            }),
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name.starts_with(prefix))
            .map(|(name, raw)| ObjectMetadata {
                name: name.clone(),
                version: ObjectVersion {
                    id: "1".to_string(),
                },
                tags: BTreeMap::new(),
                key_names: keys_of(raw),
            })
            .collect())
    }

    fn tag(
        &self,
        _name: &str,
        _tags: &BTreeMap<String, String>,
        _expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        panic!("generation must never tag")
    }

    fn delete(
        &self,
        _name: &str,
        _expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        panic!("generation must never delete")
    }
}

fn is_hex(value: &str, len: usize) -> bool {
    value.len() == len && value.chars().all(|c| c.is_ascii_hexdigit())
}

fn basic(public: &str, secret: &str) -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{public}:{secret}"))
    )
}

fn bare_fresh_plan() -> RoutingPlan {
    plan_with(&install(""), None, Some(&BTreeMap::new())).expect("plan")
}

#[test]
fn generate_creates_each_listed_entry_once_with_exactly_its_keys() {
    let plan = bare_fresh_plan();
    let to_create: Vec<String> = GENERATED.iter().map(|s| s.to_string()).collect();
    let provider = FakeProvider::default();

    let created = generate(&provider, &plan, &to_create).expect("generate");
    let created: BTreeSet<String> = created.into_iter().collect();
    assert_eq!(created, to_create.iter().cloned().collect::<BTreeSet<_>>());

    let puts = provider.puts();
    assert_eq!(puts.len(), GENERATED.len(), "one put per entry");
    let put_names: BTreeSet<&str> = puts.iter().map(|(n, _, _)| n.as_str()).collect();
    assert_eq!(put_names.len(), puts.len(), "each entry put once");
    for (name, expected_version, _) in &puts {
        assert_eq!(*expected_version, None, "{name} must be create-only");
    }

    let inventory = inventory();
    for logical in GENERATED {
        let body = provider.body(logical);
        let keys: BTreeSet<String> = body.as_object().expect("object").keys().cloned().collect();
        let want: BTreeSet<String> = inventory[logical].keys.iter().cloned().collect();
        assert_eq!(keys, want, "{logical} keys");
        for key in &keys {
            assert!(
                body[key].as_str().is_some_and(|v| !v.is_empty()),
                "{logical}.{key} empty"
            );
        }
    }

    let id = provider.body("installation-id")["installationId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(is_hex(&id, 32), "installationId shape");
    let encryption = provider.body("langfuse-data-keys")["langfuseEncryptionKey"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(is_hex(&encryption, 64), "encryption key shape");

    let project = provider.body("langfuse-init-project-secret-key")["langfuseInitProjectSecretKey"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        provider.body("otlp-auth-header")["otlpAuthHeader"],
        json!(basic(DEFAULT_LANGFUSE_PUBLIC_KEY, &project))
    );

    let sealing = provider.body("sealing-private-key")["sealingPrivateKey"]
        .as_str()
        .unwrap()
        .to_string();
    curie::sealing::public_key_of(&sealing).expect("generated sealing key is valid");
}

#[test]
fn otlp_header_is_derived_from_the_stored_project_key() {
    let plan = bare_fresh_plan();
    let provider = FakeProvider::default().with(
        "langfuse-init-project-secret-key",
        json!({"langfuseInitProjectSecretKey": PROJECT_KEY_VALUE}),
    );
    let created = generate(&provider, &plan, &["otlp-auth-header".to_string()]).expect("generate");
    assert_eq!(created, vec!["otlp-auth-header".to_string()]);
    assert_eq!(
        provider.body("otlp-auth-header")["otlpAuthHeader"],
        json!(basic(DEFAULT_LANGFUSE_PUBLIC_KEY, PROJECT_KEY_VALUE))
    );
    assert_eq!(
        provider.body("langfuse-init-project-secret-key")["langfuseInitProjectSecretKey"],
        json!(PROJECT_KEY_VALUE),
        "the stored project key is never replaced"
    );
}

#[test]
fn generate_never_overwrites_an_existing_object() {
    let plan = bare_fresh_plan();
    let provider = FakeProvider::default().with(
        "installation-id",
        json!({"installationId": INSTALLATION_VALUE}),
    );
    let created = generate(
        &provider,
        &plan,
        &["installation-id".to_string(), "valkey-password".to_string()],
    )
    .expect("generate");
    assert_eq!(created, vec!["valkey-password".to_string()]);
    assert_eq!(
        provider.body("installation-id")["installationId"],
        json!(INSTALLATION_VALUE)
    );
}

#[test]
fn generation_errors_carry_no_generated_material() {
    let plan = bare_fresh_plan();
    let provider = FakeProvider {
        fail_put: Some("sealing-private-key".to_string()),
        ..FakeProvider::default()
    };
    let to_create: Vec<String> = GENERATED.iter().map(|s| s.to_string()).collect();
    let error = generate(&provider, &plan, &to_create).expect_err("put failure surfaces");
    let text = err_text(&error);
    let puts = provider.puts();
    assert!(
        puts.iter().any(|(n, _, _)| n == "sealing-private-key"),
        "the failing put was attempted"
    );
    for (_, _, material) in &puts {
        let body: Value = serde_json::from_str(material).expect("JSON material");
        for value in body.as_object().unwrap().values() {
            let value = value.as_str().unwrap();
            assert!(
                !text.contains(value),
                "error leaked generated material: {text}"
            );
        }
    }
}

// ------------------------------------------------------------------- render

#[test]
fn external_secrets_group_by_target() {
    let plan = fresh_declared_plan();
    let rendered = render_external_secrets(&plan, STORE);
    let targets: BTreeSet<String> = plan.entries.iter().map(|e| e.target.clone()).collect();
    assert_eq!(
        rendered.len(),
        targets.len(),
        "one ExternalSecret per target"
    );

    let names: BTreeSet<String> = rendered
        .iter()
        .map(|es| es["metadata"]["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(names, targets);

    for es in &rendered {
        assert_eq!(es["kind"], "ExternalSecret");
        assert_eq!(es["metadata"]["namespace"], json!(plan.namespace));
        assert_eq!(es["spec"]["target"]["creationPolicy"], "Owner");
        assert_eq!(
            es["spec"]["secretStoreRef"],
            json!({"name": STORE, "kind": "SecretStore"})
        );
        assert_eq!(es["spec"]["refreshInterval"], json!(REFRESH_INTERVAL));
        assert_eq!(REFRESH_INTERVAL, "1h");
    }

    let langfuse = rendered
        .iter()
        .find(|es| es["metadata"]["name"] == "rel-curie-langfuse")
        .expect("langfuse ExternalSecret");
    let data = langfuse["spec"]["data"].as_array().expect("data");
    assert_eq!(data.len(), 6, "{langfuse}");
    let salt = data
        .iter()
        .find(|item| item["secretKey"] == "langfuseSalt")
        .expect("salt item");
    assert_eq!(
        salt["remoteRef"]["key"],
        "curie/test/rel/langfuse-data-keys"
    );
    assert_eq!(salt["remoteRef"]["property"], "langfuseSalt");
}

#[test]
fn dry_run_lines_name_the_store_and_each_target() {
    let plan = fresh_declared_plan();
    let text = dry_run_lines(&plan, STORE).join("\n");
    assert!(text.contains(STORE), "{text}");
    for entry in &plan.entries {
        assert!(
            text.contains(&entry.target),
            "{} missing: {text}",
            entry.target
        );
    }
}

// ------------------------------------------------------------- fake kubectl

#[derive(Default)]
struct KState {
    calls: Vec<(Vec<String>, Option<Vec<u8>>)>,
    store_present: bool,
    secrets: BTreeMap<String, Value>,
    marks: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct FakeKubectl(Arc<Mutex<KState>>);

fn out(success: bool, stdout: &str, stderr: &str) -> Result<KubectlOutput> {
    Ok(KubectlOutput {
        success,
        stdout: stdout.to_string(),
        stderr: stderr.to_string(),
    })
}

fn has(args: &[String], token: &str) -> bool {
    args.iter().any(|a| a == token)
}

/// The first positional after `verb kind`, skipping flags and their values.
fn object_name(args: &[String], kind: &str) -> Option<String> {
    let position = args
        .iter()
        .position(|a| a == kind || a.starts_with(&format!("{kind}/")))?;
    if let Some(name) = args[position].split_once('/').map(|(_, n)| n.to_string()) {
        return Some(name);
    }
    args.get(position + 1)
        .filter(|a| !a.starts_with('-'))
        .cloned()
}

impl FakeKubectl {
    fn new(store_present: bool) -> Self {
        let fake = Self::default();
        fake.0.lock().unwrap().store_present = store_present;
        fake
    }

    fn with_secret(self, secret: Value) -> Self {
        let name = secret["metadata"]["name"].as_str().unwrap().to_string();
        self.0.lock().unwrap().secrets.insert(name, secret);
        self
    }

    fn calls(&self) -> Vec<(Vec<String>, Option<Vec<u8>>)> {
        self.0.lock().unwrap().calls.clone()
    }
}

impl Kubectl for FakeKubectl {
    fn run(&self, args: &[String], stdin: Option<&[u8]>) -> Result<KubectlOutput> {
        let mut s = self.0.lock().unwrap();
        s.calls.push((args.to_vec(), stdin.map(<[u8]>::to_vec)));
        let not_found = |kind: &str, name: &str| {
            format!("Error from server (NotFound): {kind} \"{name}\" not found")
        };
        if has(args, "apply") && has(args, "--server-side") {
            return out(true, "applied", "");
        }
        if has(args, "annotate") {
            let name = object_name(args, "externalsecret").expect("annotated name");
            let prefix = format!("{FORCE_SYNC_ANNOTATION}=");
            let mark = args
                .iter()
                .find_map(|a| a.strip_prefix(&prefix))
                .expect("force-sync mark")
                .to_string();
            s.marks.insert(name, mark);
            return out(true, "annotated", "");
        }
        if has(args, "get") {
            let ignore = has(args, "--ignore-not-found");
            let lower: Vec<String> = args.iter().map(|a| a.to_lowercase()).collect();
            if let Some(name) = object_name(&lower, "secretstore")
                .or_else(|| object_name(&lower, "secretstores"))
                .or_else(|| object_name(&lower, "secretstore.external-secrets.io"))
            {
                return if s.store_present {
                    let store = json!({
                        "apiVersion": "external-secrets.io/v1",
                        "kind": "SecretStore",
                        "metadata": {"name": name, "namespace": NAMESPACE},
                        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
                    });
                    out(true, &store.to_string(), "")
                } else if ignore {
                    out(true, "", "")
                } else {
                    out(
                        false,
                        "",
                        &not_found("secretstores.external-secrets.io", &name),
                    )
                };
            }
            if let Some(name) = object_name(&lower, "externalsecret") {
                let Some(mark) = s.marks.get(&name).cloned() else {
                    return out(
                        false,
                        "",
                        &not_found("externalsecrets.external-secrets.io", &name),
                    );
                };
                let metadata = json!({
                    "name": name,
                    "namespace": NAMESPACE,
                    "generation": 1,
                    "annotations": { FORCE_SYNC_ANNOTATION: mark },
                });
                let version = synced_version(&metadata);
                let body = json!({
                    "apiVersion": "external-secrets.io/v1",
                    "kind": "ExternalSecret",
                    "metadata": metadata,
                    "status": {
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "syncedResourceVersion": version,
                    },
                });
                return out(true, &body.to_string(), "");
            }
            if let Some(name) = object_name(&lower, "secret") {
                return match s.secrets.get(&name) {
                    Some(secret) => out(true, &secret.to_string(), ""),
                    None if ignore => out(true, "", ""),
                    None => out(false, "", &not_found("secrets", &name)),
                };
            }
        }
        out(false, "", &format!("unexpected kubectl call: {args:?}"))
    }
}

#[test]
fn check_store_refuses_a_missing_store_with_one_read() {
    let kubectl = FakeKubectl::new(false);
    let error = check_store(&kubectl, NAMESPACE, STORE).expect_err("store missing");
    let text = err_text(&error);
    assert!(text.contains(STORE), "{text}");
    assert!(text.contains(NAMESPACE), "{text}");
    assert_eq!(kubectl.calls().len(), 1, "{:?}", kubectl.calls());

    let kubectl = FakeKubectl::new(true);
    check_store(&kubectl, NAMESPACE, STORE).expect("store present");
}

#[test]
fn sync_refuses_a_foreign_target_secret_before_applying() {
    let plan = fresh_declared_plan();
    let kubectl = FakeKubectl::new(true).with_secret(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "rel-curie-postgres", "namespace": NAMESPACE},
        "data": {},
    }));
    let error = sync(
        &kubectl,
        &plan,
        STORE,
        Duration::from_secs(2),
        Duration::from_millis(1),
    )
    .expect_err("foreign secret");
    assert!(
        err_text(&error).contains("rel-curie-postgres"),
        "{}",
        err_text(&error)
    );
    assert!(
        !kubectl.calls().iter().any(|(a, _)| has(a, "apply")),
        "no apply after a refusal: {:?}",
        kubectl.calls()
    );
}

#[test]
fn sync_applies_one_list_then_force_syncs_each_external_secret() {
    let plan = fresh_declared_plan();
    // A target already owned by our ExternalSecret (a re-apply) is allowed.
    let kubectl = FakeKubectl::new(true).with_secret(json!({
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "rel-curie-postgres",
            "namespace": NAMESPACE,
            "ownerReferences": [{
                "apiVersion": "external-secrets.io/v1",
                "kind": "ExternalSecret",
                "name": "rel-curie-postgres",
                "uid": "uid-owned",
                "controller": true,
            }],
        },
        "data": {},
    }));
    sync(
        &kubectl,
        &plan,
        STORE,
        Duration::from_secs(5),
        Duration::from_millis(1),
    )
    .expect("sync");

    let calls = kubectl.calls();
    let applies: Vec<usize> = calls
        .iter()
        .enumerate()
        .filter(|(_, (a, _))| has(a, "apply"))
        .map(|(i, _)| i)
        .collect();
    assert_eq!(applies.len(), 1, "exactly one apply: {calls:?}");
    let (args, stdin) = &calls[applies[0]];
    assert!(has(args, "--server-side"), "{args:?}");
    assert!(has(args, "--field-manager=curie-secrets"), "{args:?}");
    let list: Value = serde_json::from_slice(stdin.as_ref().expect("stdin")).expect("JSON List");
    assert_eq!(list["kind"], "List");
    let targets: BTreeSet<String> = plan.entries.iter().map(|e| e.target.clone()).collect();
    assert_eq!(list["items"].as_array().unwrap().len(), targets.len());

    for target in &targets {
        let annotate = calls
            .iter()
            .position(|(a, _)| has(a, "annotate") && has(a, target))
            .unwrap_or_else(|| panic!("no force sync for {target}: {calls:?}"));
        assert!(annotate > applies[0], "{target} force-synced before apply");
        assert!(
            calls[annotate..]
                .iter()
                .any(|(a, _)| has(a, "get") && has(a, "externalsecret") && has(a, target)),
            "{target} readiness never read"
        );
    }
}

// ------------------------------------------------------------ up rewriting

fn up_literal() -> UpOpts {
    UpOpts {
        retained_mail_values: None,
        common: CommonOpts {
            namespace: NAMESPACE.to_string(),
            release: RELEASE.to_string(),
            dry_run: true,
        },
        chart: "charts/curie".to_string(),
        no_expose: true,
        set: vec![format!("valkey.password={VALKEY_VALUE}")],
        set_string: vec!["worker.label=blue".to_string()],
        allow_egress_host: vec![],
        resolved_egress_cidrs: vec![],
        allow_web_egress: vec![],
        fake_model: false,
        credentials: Some(MODEL_VALUE.to_string()),
        local_model: None,
        model: None,
        secrets: vec![
            ("postgres.auth.password".to_string(), PG_VALUE.to_string()),
            ("langfuse.salt".to_string(), SALT_VALUE.to_string()),
            ("sealing.privateKey".to_string(), SEALING_VALUE.to_string()),
            ("api.githubToken".to_string(), GITHUB_VALUE.to_string()),
            (
                "dispatcher.slack.botToken".to_string(),
                SLACK_BOT_VALUE.to_string(),
            ),
            ("api.apiKey".to_string(), API_KEY_VALUE.to_string()),
        ],
        github_token: GithubTokenPlan::Set(GITHUB_VALUE.to_string()),
        dev: false,
        adopt: false,
        history_max: None,
    }
}

fn display(up: &UpOpts) -> String {
    up_commands(up)
        .iter()
        .map(|c| c.display())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn route_up_opts_passes_names_instead_of_values() {
    let plan = fresh_declared_plan();
    let mut up = up_literal();
    assert!(
        !display(&up).contains("--history-max"),
        "provider absent adds no bound"
    );

    route_up_opts(&mut up, &plan);

    let dropped: BTreeSet<&str> = dropped_value_keys().iter().copied().collect();
    for key in [
        "postgres.auth.password",
        "langfuse.salt",
        "sealing.privateKey",
        "api.githubToken",
        "dispatcher.slack.botToken",
        "valkey.password",
    ] {
        assert!(dropped.contains(key), "{key} must be a dropped value key");
    }
    assert!(!dropped.contains("api.apiKey"), "api.apiKey is a throwaway");

    for (key, _) in &up.secrets {
        assert!(!dropped.contains(key.as_str()), "{key} survived in secrets");
    }
    for expression in up.set.iter().chain(&up.set_string) {
        let key = expression
            .split_once('=')
            .map_or(expression.as_str(), |(k, _)| k);
        if dropped.contains(key) {
            // Only the cleared Slack companions may name a dropped key, empty.
            assert!(
                expression.ends_with('='),
                "{expression} carries a value for a dropped key"
            );
        }
    }
    assert!(up
        .secrets
        .iter()
        .any(|(k, v)| k == "api.apiKey" && v == API_KEY_VALUE));
    assert_eq!(up.credentials, None);
    assert!(matches!(up.github_token, GithubTokenPlan::Untouched));
    assert_eq!(up.history_max, Some(HISTORY_MAX));
    for (key, value) in &plan.knob_sets {
        if key == "agentSandbox.runner.fakeModel" {
            continue; // typed lane, see fake_model_false_goes_through_the_typed_set_lane
        }
        let expression = format!("{key}={value}");
        assert!(
            up.set_string.contains(&expression),
            "{expression} missing from set_string: {:?}",
            up.set_string
        );
    }

    let rendered = display(&up);
    assert!(rendered.contains("--history-max 3"), "{rendered}");
    for value in [
        MODEL_VALUE,
        GITHUB_VALUE,
        SLACK_BOT_VALUE,
        PG_VALUE,
        SALT_VALUE,
        SEALING_VALUE,
        VALKEY_VALUE,
        API_KEY_VALUE,
    ] {
        assert!(!rendered.contains(value), "{value} leaked: {rendered}");
    }
}

// ------------------------------------------------------ review regressions

#[test]
fn a_comma_joined_second_assignment_in_set_is_refused_and_dropped() {
    // Helm reads `a=x,b=y` as two assignments, so the second key must be
    // checked too, not only the YAML mapping key.
    let smuggled = format!("ClusterIP,api.githubToken={GITHUB_VALUE}");
    let cfg = install(&format!("set:\n  ui.service.type: \"{smuggled}\"\n"));
    assert_refused(
        plan_with(&cfg, None, None),
        "api.githubToken",
        Some(GITHUB_VALUE),
    );

    let plan = fresh_declared_plan();
    let mut up = up_literal();
    let expression = format!("ui.service.type={smuggled}");
    up.set_string.push(expression.clone());
    up.set.push(expression.clone());
    route_up_opts(&mut up, &plan);
    assert!(
        !up.set_string.contains(&expression) && !up.set.contains(&expression),
        "a set expression whose second assignment is a dropped key survived"
    );
    assert!(!display(&up).contains(GITHUB_VALUE), "{}", display(&up));
}

/// Another apply wins the race for the project key: its `create` stores a
/// rival value and reports `Conflict`. `put` is never a legal generation path.
struct RacingProvider {
    inner: FakeProvider,
    rival: String,
}

impl SecretsProvider for RacingProvider {
    fn put(&self, _request: &PutRequest<'_>) -> Result<ObjectVersion, ProviderError> {
        panic!("generation must create atomically, never put")
    }

    fn create(
        &self,
        name: &str,
        material: &SecretMaterial,
    ) -> Result<ObjectVersion, ProviderError> {
        let mut objects = self.inner.objects.lock().unwrap();
        if name == "langfuse-init-project-secret-key" {
            objects.insert(
                name.to_string(),
                json!({"langfuseInitProjectSecretKey": self.rival}).to_string(),
            );
        }
        if objects.contains_key(name) {
            return Err(ProviderError::Conflict {
                name: name.to_string(),
                expected_version: None,
                actual_version: None,
            });
        }
        objects.insert(name.to_string(), material.expose().to_string());
        Ok(ObjectVersion {
            id: "1".to_string(),
        })
    }

    fn get(&self, name: &str, version: Option<&str>) -> Result<StoredObject, ProviderError> {
        self.inner.get(name, version)
    }

    fn get_metadata(&self, name: &str) -> Result<ObjectMetadata, ProviderError> {
        self.inner.get_metadata(name)
    }

    fn list(&self, prefix: &str) -> Result<Vec<ObjectMetadata>, ProviderError> {
        self.inner.list(prefix)
    }

    fn tag(
        &self,
        name: &str,
        tags: &BTreeMap<String, String>,
        expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        self.inner.tag(name, tags, expected_version)
    }

    fn delete(
        &self,
        name: &str,
        expected_version: Option<&str>,
    ) -> Result<ObjectVersion, ProviderError> {
        self.inner.delete(name, expected_version)
    }
}

#[test]
fn a_lost_create_race_keeps_the_rival_key_and_derives_the_header_from_it() {
    let plan = bare_fresh_plan();
    let provider = RacingProvider {
        inner: FakeProvider::default(),
        rival: PROJECT_KEY_VALUE.to_string(),
    };
    let created = generate(
        &provider,
        &plan,
        &[
            "otlp-auth-header".to_string(),
            "langfuse-init-project-secret-key".to_string(),
        ],
    )
    .expect("a lost race is not an error");
    assert_eq!(
        created,
        vec!["otlp-auth-header".to_string()],
        "the project key was created by the rival, not by this apply"
    );
    assert_eq!(
        provider.inner.body("langfuse-init-project-secret-key")["langfuseInitProjectSecretKey"],
        json!(PROJECT_KEY_VALUE),
        "the rival's project key is never overwritten"
    );
    assert_eq!(
        provider.inner.body("otlp-auth-header")["otlpAuthHeader"],
        json!(basic(DEFAULT_LANGFUSE_PUBLIC_KEY, PROJECT_KEY_VALUE)),
        "the OTLP header must match the stored project key"
    );
}

#[test]
fn generate_never_calls_put() {
    let plan = bare_fresh_plan();
    let provider = RacingProvider {
        inner: FakeProvider::default(),
        rival: PROJECT_KEY_VALUE.to_string(),
    };
    let to_create: Vec<String> = GENERATED.iter().map(|s| s.to_string()).collect();
    generate(&provider, &plan, &to_create).expect("generate through create only");
    for logical in GENERATED {
        provider.inner.body(logical);
    }
}

fn assert_preserved_reference_is_preflighted(live: Value, logical: &str) {
    let cfg = install("");
    let plan = plan_with(&cfg, Some(&live), Some(&BTreeMap::new())).expect("plan");
    assert_eq!(
        reason_of(&plan, logical),
        Some(RouteReason::Optional),
        "a live reference to this release's synced Secret keeps {logical} routed"
    );
    let mut sm = full_sm(&plan);
    sm.remove(logical);
    let error = preflight(&plan, &sm).expect_err("the missing source must be refused");
    let text = err_text(&error);
    assert!(
        text.contains(&format!("{REMOTE_PREFIX}/{logical}")),
        "{text}"
    );
}

#[test]
fn a_preserved_model_reference_is_routed_and_preflighted() {
    assert_preserved_reference_is_preflighted(
        json!({"agentSandbox": {"runner": {"credentialsExistingSecret": "rel-curie-runner-credentials"}}}),
        "runner-model-credentials",
    );
}

#[test]
fn a_preserved_slack_reference_is_routed_and_preflighted() {
    assert_preserved_reference_is_preflighted(
        json!({"dispatcher": {"slack": {"appTokenExistingSecret": "rel-curie-slack"}}}),
        "slack-app-token",
    );
}

#[test]
fn a_backslash_escaped_second_assignment_key_in_set_is_refused() {
    // Helm unescapes `\T` to `T`, so `api.github\Token` assigns api.githubToken.
    let smuggled = format!("ClusterIP,api.github\\\\Token={GITHUB_VALUE}");
    let cfg = install(&format!("set:\n  ui.service.type: \"{smuggled}\"\n"));
    let error = plan_with(&cfg, None, None).expect_err("escaped key must be refused");
    let text = err_text(&error);
    assert!(
        !text.contains(GITHUB_VALUE),
        "refusal leaked a value: {text}"
    );
}

#[test]
fn fake_model_false_goes_through_the_typed_set_lane() {
    // `--set-string` would render the STRING "false", which the chart's
    // `and fakeModel ...` reads as truthy, so the worker gets no credentials.
    let plan = fresh_declared_plan();
    let mut up = up_literal();
    route_up_opts(&mut up, &plan);
    let expression = "agentSandbox.runner.fakeModel=false".to_string();
    assert!(up.set.contains(&expression), "{:?}", up.set);
    assert!(!up.set_string.contains(&expression), "{:?}", up.set_string);

    let rendered = display(&up);
    assert!(
        rendered.contains("--set agentSandbox.runner.fakeModel=false"),
        "{rendered}"
    );
    assert!(
        !rendered.contains("--set-string agentSandbox.runner.fakeModel=false"),
        "{rendered}"
    );
}
