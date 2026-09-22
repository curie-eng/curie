//! Platform credential inventory and the render coverage check (ADR 0163).
//!
//! The inventory is the list every Secret reference the chart renders must
//! land in. These tests pin the checked-in data, the whole-inventory rules,
//! the ref extractor, the coverage matcher, the bundle merge, and the two
//! planted negative controls the CI step relies on.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use curie::connector_build::parse_connectors;
use curie::provider::render_check::{
    extract_refs, run_check, uncovered, CheckOptions, NameContext, RenderedRef,
};
use curie::provider::{
    bundle_entries, parse_inventory, platform_inventory, validate_inventory, ChartBinding,
    ChartKnob, InventoryClass, InventoryEntry, RotationOwner, Store, UpdatePolicy,
};
use serde_json::{json, Value};

const EMBEDDED_INVENTORY: &str = include_str!("../src/provider/platform-inventory.yaml");

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli has a parent")
        .to_path_buf()
}

fn entry(logical_name: &str, target: &str, keys: &[&str]) -> InventoryEntry {
    InventoryEntry {
        logical_name: logical_name.into(),
        class: InventoryClass::Stateful,
        target: target.into(),
        keys: keys.iter().map(|k| k.to_string()).collect(),
        consumers: vec!["api".into()],
        rotation_owner: RotationOwner::Sm,
        update_policy: UpdatePolicy::Replace,
        store: Store::Sm,
        rotated_keys: vec![],
        chart: None,
    }
}

fn entries_with_key<'a>(entries: &'a [InventoryEntry], key: &str) -> Vec<&'a InventoryEntry> {
    entries
        .iter()
        .filter(|e| e.keys.iter().any(|k| k == key))
        .collect()
}

fn err_text<T: std::fmt::Debug>(result: anyhow::Result<T>) -> String {
    format!("{:#}", result.expect_err("must be refused"))
}

// --------------------------------------------------------------------------
// The checked-in platform inventory
// --------------------------------------------------------------------------

#[test]
fn platform_inventory_loads_and_validates() {
    let entries = platform_inventory().expect("embedded inventory loads");
    assert!(entries.len() >= 30, "only {} entries", entries.len());
    validate_inventory(&entries).expect("embedded inventory validates");
    for e in &entries {
        assert!(matches!(
            e.class,
            InventoryClass::External | InventoryClass::Stateful | InventoryClass::Throwaway
        ));
    }
}

#[test]
fn values_bound_to_persisted_data_are_immutable_and_provider_held() {
    let entries = platform_inventory().expect("embedded inventory loads");
    for key in [
        "postgresPassword",
        "langfuseEncryptionKey",
        "langfuseSalt",
        "installationId",
        "sealingPrivateKey",
        "sealingPreviousPrivateKey",
    ] {
        let found = entries_with_key(&entries, key);
        assert!(!found.is_empty(), "{key} is not in the inventory");
        for e in found {
            assert_eq!(
                e.update_policy,
                UpdatePolicy::Immutable,
                "{key} in {}",
                e.logical_name
            );
            assert_eq!(e.store, Store::Sm, "{key} in {}", e.logical_name);
        }
    }
}

#[test]
fn minted_and_chart_generated_tokens_stay_in_cluster() {
    let entries = platform_inventory().expect("embedded inventory loads");
    for key in [
        "apiKey",
        "approvalChatAttesterSecret",
        "internalWorkerToken",
        "mailChannelToken",
        "GRAFANA_SERVICE_ACCOUNT_TOKEN",
    ] {
        let found = entries_with_key(&entries, key);
        assert!(
            found.iter().any(|e| e.store == Store::Cluster),
            "{key} has no store: cluster entry"
        );
    }
}

#[test]
fn every_chart_default_secret_key_is_listed() {
    let entries = platform_inventory().expect("embedded inventory loads");
    let keys = [
        "installationId",
        "postgresPassword",
        "valkeyPassword",
        "clickhousePassword",
        "rustfsSecretKey",
        "langfuseSalt",
        "langfuseEncryptionKey",
        "langfuseNextauthSecret",
        "langfuseInitProjectSecretKey",
        "langfuseInitUserPassword",
        "otlpAuthHeader",
        "agentCredentials",
        "adapterCredentials",
        "apiKey",
        "approvalChatAttesterSecret",
        "internalWorkerToken",
        "githubWebhookSecret",
        "githubToken",
        "githubAppPrivateKey",
        "sealingPrivateKey",
        "sealingPreviousPrivateKey",
        "slackAppToken",
        "slackBotToken",
        "slackSigningSecret",
        "mailAgentmailApiKey",
        "mailChannelToken",
        "mailEgressSecret",
    ];
    let missing: Vec<&str> = keys
        .iter()
        .copied()
        .filter(|k| entries_with_key(&entries, k).is_empty())
        .collect();
    assert!(missing.is_empty(), "unlisted chart keys: {missing:?}");
}

#[test]
fn eso_never_adopts_the_chart_default_secret() {
    // ESO writes new Secret names; syncing into the chart's own Secret would
    // fight Helm for ownership on every upgrade.
    let entries = platform_inventory().expect("embedded inventory loads");
    for e in &entries {
        assert!(
            !(e.store == Store::Sm && e.target == "{fullname}-secrets"),
            "{} targets the chart Secret with store sm",
            e.logical_name
        );
    }
}

// --------------------------------------------------------------------------
// Refusals
// --------------------------------------------------------------------------

fn doc(body: &str) -> String {
    format!("version: 1\nentries:\n{body}")
}

const ROW: &str = "  - logical_name: {name}
    class: external
    target: \"{target}\"
    keys: {keys}
    consumers: [api]
    rotation_owner: {owner}
    update_policy: replace
    store: {store}
{extra}";

fn row(name: &str, target: &str, keys: &str, owner: &str, store: &str, extra: &str) -> String {
    ROW.replace("{name}", name)
        .replace("{target}", target)
        .replace("{keys}", keys)
        .replace("{owner}", owner)
        .replace("{store}", store)
        .replace("{extra}", extra)
}

#[test]
fn rotated_key_outside_keys_is_refused() {
    let text = err_text(parse_inventory(&doc(&row(
        "ledger",
        "ledger-creds",
        "[clientId]",
        "workload:ledger",
        "sm",
        "    rotated_keys: [refreshToken]\n",
    ))));
    assert!(text.contains("refreshToken"), "{text}");
}

#[test]
fn rotated_keys_need_a_workload_owner() {
    let text = err_text(parse_inventory(&doc(&row(
        "ledger",
        "ledger-creds",
        "[clientId, refreshToken]",
        "sm",
        "sm",
        "    rotated_keys: [refreshToken]\n",
    ))));
    assert!(text.contains("refreshToken"), "{text}");
}

#[test]
fn a_minted_key_marked_eso_managed_is_refused() {
    // Negative control b, in process: ESO would revert every rotation.
    let text = err_text(parse_inventory(&doc(&row(
        "grafana-connector-token",
        "{release}-grafana-connector",
        "[GRAFANA_SERVICE_ACCOUNT_TOKEN]",
        "mint:grafana-connector",
        "sm",
        "",
    ))));
    assert!(text.contains("rotation-owned"), "{text}");
    assert!(text.contains("GRAFANA_SERVICE_ACCOUNT_TOKEN"), "{text}");
}

#[test]
fn a_workload_owner_with_no_rotated_keys_owns_every_key() {
    let text = err_text(parse_inventory(&doc(&row(
        "ledger",
        "ledger-creds",
        "[refreshToken]",
        "workload:ledger",
        "sm",
        "",
    ))));
    assert!(text.contains("rotation-owned"), "{text}");
    assert!(text.contains("refreshToken"), "{text}");
}

#[test]
fn a_workload_rotating_one_key_beside_a_static_key_validates() {
    let entries = parse_inventory(&doc(&row(
        "ledger",
        "ledger-creds",
        "[clientId, refreshToken]",
        "workload:ledger",
        "sm",
        "    rotated_keys: [refreshToken]\n",
    )))
    .expect("static sibling is ESO-managed, rotated key is not");
    assert_eq!(entries[0].rotated_keys, vec!["refreshToken".to_string()]);
}

#[test]
fn an_unknown_placeholder_is_refused() {
    let text = err_text(parse_inventory(&doc(&row(
        "db",
        "{bogus}-db",
        "[password]",
        "sm",
        "sm",
        "",
    ))));
    assert!(text.contains("bogus"), "{text}");
}

#[test]
fn a_target_key_pair_listed_twice_is_refused() {
    let body = format!(
        "{}{}",
        row("db-a", "shared-db", "[password]", "sm", "sm", ""),
        row("db-b", "shared-db", "[password]", "sm", "sm", "")
    );
    let text = err_text(parse_inventory(&doc(&body)));
    assert!(text.contains("password"), "{text}");
    let direct = err_text(validate_inventory(&[
        entry("db-a", "shared-db", &["password"]),
        entry("db-b", "shared-db", &["password"]),
    ]));
    assert!(direct.contains("password"), "{direct}");
}

#[test]
fn a_logical_name_listed_twice_is_refused() {
    let body = format!(
        "{}{}",
        row("db", "db-one", "[password]", "sm", "sm", ""),
        row("db", "db-two", "[password]", "sm", "sm", "")
    );
    let text = err_text(parse_inventory(&doc(&body)));
    assert!(text.contains("db"), "{text}");
    let direct = err_text(validate_inventory(&[
        entry("dup-name", "db-one", &["password"]),
        entry("dup-name", "db-two", &["password"]),
    ]));
    assert!(direct.contains("dup-name"), "{direct}");
}

// --------------------------------------------------------------------------
// Serde
// --------------------------------------------------------------------------

#[test]
fn new_fields_round_trip() {
    let mut e = entry("ledger", "ledger-creds", &["clientId", "refreshToken"]);
    e.rotation_owner = RotationOwner::Workload("ledger".into());
    e.rotated_keys = vec!["refreshToken".into()];
    e.chart = Some(ChartBinding {
        default_secret: Some("{fullname}-secrets".into()),
        knobs: vec![ChartKnob {
            secret: "ledger.existingSecret".into(),
            key: Some("ledger.existingSecretKey".into()),
        }],
    });
    let value = serde_json::to_value(&e).expect("serialize");
    assert_eq!(value["store"], "sm");
    assert_eq!(value["rotated_keys"], json!(["refreshToken"]));
    assert_eq!(value["chart"]["default_secret"], "{fullname}-secrets");
    assert_eq!(
        value["chart"]["knobs"][0]["secret"],
        "ledger.existingSecret"
    );
    assert_eq!(
        value["chart"]["knobs"][0]["key"],
        "ledger.existingSecretKey"
    );
    let back: InventoryEntry = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, e);
}

#[test]
fn empty_optional_fields_are_omitted() {
    let mut e = entry("db", "db", &["password"]);
    e.store = Store::Cluster;
    let value = serde_json::to_value(&e).expect("serialize");
    assert_eq!(value["store"], "cluster");
    let object = value.as_object().expect("object");
    assert!(!object.contains_key("rotated_keys"), "{value}");
    assert!(!object.contains_key("chart"), "{value}");
    let back: InventoryEntry = serde_json::from_value(value).expect("deserialize");
    assert_eq!(back, e);
}

#[test]
fn unknown_field_inside_chart_is_refused() {
    let mut value = serde_json::to_value(entry("db", "db", &["password"])).expect("serialize");
    value["chart"] = json!({"default_secret": "db", "knobz": []});
    assert!(serde_json::from_value::<InventoryEntry>(value).is_err());
}

// --------------------------------------------------------------------------
// extract_refs
// --------------------------------------------------------------------------

const MANIFEST: &str = r#"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: inv-curie-api
spec:
  template:
    spec:
      imagePullSecrets:
        - name: regcred
      containers:
        - name: api
          env:
            - name: PLAIN
              value: x
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: inv-curie-secrets
                  key: postgresPassword
          envFrom:
            - secretRef:
                name: bulk-env
      volumes:
        - name: picked
          secret:
            secretName: picked-files
            items:
              - key: tls.crt
                path: cert
              - key: tls.key
                path: key
        - name: whole
          secret:
            secretName: whole-files
        - name: proj
          projected:
            sources:
              - secret:
                  name: projected-src
                  items:
                    - key: token
                      path: token
        - name: csi
          csi:
            driver: secrets-store.csi.k8s.io
            nodePublishSecretRef:
              name: csi-creds
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: inv-curie-api
secrets:
  - name: sa-token
imagePullSecrets:
  - name: sa-pull
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: inv-curie-api
spec:
  tls:
    - hosts: [a.example.com]
      secretName: api-tls
---
apiVersion: v1
kind: Secret
metadata:
  name: inv-curie-secrets
data:
  apiKey: eA==
stringData:
  apiSalt: y
---
apiVersion: extensions.agents.x-k8s.io/v1alpha1
kind: SandboxTemplate
metadata:
  name: inv-curie-runner
spec:
  podTemplate:
    spec:
      containers:
        - name: runner
          env:
            - name: CRED
              valueFrom:
                secretKeyRef:
                  name: deep-secret
                  key: deepKey
"#;

fn triples(refs: &[RenderedRef]) -> Vec<(String, Option<String>, String)> {
    let mut out: Vec<_> = refs
        .iter()
        .map(|r| (r.name.clone(), r.key.clone(), r.object.clone()))
        .collect();
    out.sort();
    out.dedup();
    out
}

#[test]
fn extract_refs_walks_every_reference_shape() {
    let refs = extract_refs(MANIFEST).expect("parse manifest");
    let t = |n: &str, k: Option<&str>, o: &str| (n.to_string(), k.map(String::from), o.to_string());
    let dep = "Deployment/inv-curie-api";
    let mut want = vec![
        t("inv-curie-secrets", Some("postgresPassword"), dep),
        t("bulk-env", None, dep),
        t("picked-files", Some("tls.crt"), dep),
        t("picked-files", Some("tls.key"), dep),
        t("whole-files", None, dep),
        t("projected-src", Some("token"), dep),
        t("regcred", Some(".dockerconfigjson"), dep),
        t("csi-creds", None, dep),
        t("sa-token", None, "ServiceAccount/inv-curie-api"),
        t(
            "sa-pull",
            Some(".dockerconfigjson"),
            "ServiceAccount/inv-curie-api",
        ),
        t("api-tls", None, "Ingress/inv-curie-api"),
        t(
            "inv-curie-secrets",
            Some("apiKey"),
            "Secret/inv-curie-secrets",
        ),
        t(
            "inv-curie-secrets",
            Some("apiSalt"),
            "Secret/inv-curie-secrets",
        ),
        t(
            "deep-secret",
            Some("deepKey"),
            "SandboxTemplate/inv-curie-runner",
        ),
    ];
    want.sort();
    assert_eq!(triples(&refs), want);
}

// --------------------------------------------------------------------------
// uncovered
// --------------------------------------------------------------------------

fn ctx() -> NameContext {
    NameContext {
        release: "inv".into(),
        fullname: "inv-curie".into(),
    }
}

fn rref(name: &str, key: Option<&str>) -> RenderedRef {
    RenderedRef {
        name: name.into(),
        key: key.map(String::from),
        object: "Deployment/test".into(),
    }
}

fn postgres_entry() -> InventoryEntry {
    let mut e = entry(
        "postgres-password",
        "{release}-curie-postgres",
        &["postgresPassword"],
    );
    e.chart = Some(ChartBinding {
        default_secret: Some("{fullname}-secrets".into()),
        knobs: vec![ChartKnob {
            secret: "postgres.existingSecret".into(),
            key: None,
        }],
    });
    e
}

#[test]
fn default_secret_pattern_covers_the_chart_secret() {
    let refs = [rref("inv-curie-secrets", Some("postgresPassword"))];
    assert!(uncovered(&refs, &[postgres_entry()], &json!({}), &ctx()).is_empty());
}

#[test]
fn an_existing_secret_knob_covers_only_when_set() {
    let refs = [rref("acme-pg", Some("postgresPassword"))];
    let entries = [postgres_entry()];
    assert_eq!(uncovered(&refs, &entries, &json!({}), &ctx()).len(), 1);
    let values = json!({"postgres": {"existingSecret": "acme-pg"}});
    assert!(uncovered(&refs, &entries, &values, &ctx()).is_empty());
}

#[test]
fn a_key_knob_covers_a_renamed_byo_key() {
    let mut e = entry(
        "github-app-key",
        "{release}-curie-github-app",
        &["githubAppPrivateKey"],
    );
    e.chart = Some(ChartBinding {
        default_secret: Some("{fullname}-secrets".into()),
        knobs: vec![ChartKnob {
            secret: "api.githubAppExistingSecret".into(),
            key: Some("api.githubAppExistingSecretKey".into()),
        }],
    });
    let refs = [rref("acme-gh", Some("privateKey"))];
    let only_name = json!({"api": {"githubAppExistingSecret": "acme-gh"}});
    assert_eq!(uncovered(&refs, &[e.clone()], &only_name, &ctx()).len(), 1);
    let both = json!({"api": {
        "githubAppExistingSecret": "acme-gh",
        "githubAppExistingSecretKey": "privateKey"
    }});
    assert!(uncovered(&refs, &[e.clone()], &both, &ctx()).is_empty());

    // A BYO name equal to the target pattern must still honour the key knob:
    // the supplied Secret carries `privateKey`, not the inventory key.
    let collide = json!({"api": {
        "githubAppExistingSecret": "inv-curie-github-app",
        "githubAppExistingSecretKey": "privateKey"
    }});
    let stale = [rref("inv-curie-github-app", Some("githubAppPrivateKey"))];
    assert_eq!(uncovered(&stale, &[e.clone()], &collide, &ctx()).len(), 1);
    let right = [rref("inv-curie-github-app", Some("privateKey"))];
    assert!(uncovered(&right, &[e], &collide, &ctx()).is_empty());
}

#[test]
fn an_unlisted_ref_is_returned() {
    let refs = [
        rref("inv-curie-secrets", Some("postgresPassword")),
        rref("planted-unlisted", Some("plantedKey")),
    ];
    let out = uncovered(&refs, &[postgres_entry()], &json!({}), &ctx());
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].name, "planted-unlisted");
    assert_eq!(out[0].key.as_deref(), Some("plantedKey"));
}

#[test]
fn an_operator_placeholder_is_refused() {
    let text = err_text(parse_inventory(&doc(&row(
        "tls",
        "{operator}",
        "[tls.crt]",
        "sm",
        "sm",
        "",
    ))));
    assert!(text.contains("operator"), "{text}");
}

#[test]
fn the_agent_placeholder_matches_one_agent_name() {
    let agent = entry(
        "agent-connector",
        "{fullname}-agent-{agent}-connector-secrets",
        &["TOKEN"],
    );
    let refs = [rref(
        "inv-curie-agent-sre-bot-connector-secrets",
        Some("TOKEN"),
    )];
    assert!(uncovered(&refs, &[agent], &Value::Null, &ctx()).is_empty());
}

fn tls_entry() -> InventoryEntry {
    let mut e = entry(
        "api-ingress-tls",
        "{release}-curie-api-tls",
        &["tls.crt", "tls.key"],
    );
    e.store = Store::Cluster;
    e.chart = Some(ChartBinding {
        default_secret: None,
        knobs: vec![ChartKnob {
            secret: "api.ingress.tls.secretName".into(),
            key: None,
        }],
    });
    e
}

fn image_pull_entry() -> InventoryEntry {
    let mut e = entry(
        "image-pull",
        "{release}-curie-image-pull",
        &[".dockerconfigjson"],
    );
    e.chart = Some(ChartBinding {
        default_secret: None,
        knobs: vec![
            ChartKnob {
                secret: "api.imagePullSecrets".into(),
                key: None,
            },
            ChartKnob {
                secret: "worker.imagePullSecrets".into(),
                key: None,
            },
        ],
    });
    e
}

#[test]
fn a_knob_covers_only_the_name_it_sets() {
    let values = json!({"api": {"ingress": {"tls": {"secretName": "acme-tls"}}}});
    let entries = [tls_entry()];
    assert!(uncovered(&[rref("acme-tls", None)], &entries, &values, &ctx()).is_empty());
    assert!(uncovered(
        &[rref("acme-tls", Some("tls.key"))],
        &entries,
        &values,
        &ctx()
    )
    .is_empty());
    let out = uncovered(
        &[rref("other-tls", None), rref("other-tls", Some("tls.key"))],
        &entries,
        &values,
        &ctx(),
    );
    assert_eq!(out.len(), 2, "{out:?}");
    // Unset, the knob covers nothing.
    assert_eq!(
        uncovered(&[rref("acme-tls", None)], &entries, &json!({}), &ctx()).len(),
        1
    );
}

#[test]
fn a_list_knob_covers_each_listed_name() {
    let values = json!({
        "api": {"imagePullSecrets": [{"name": "reg-a"}, {"name": "reg-b"}]},
        "worker": {"imagePullSecrets": ["reg-c"]},
    });
    let entries = [image_pull_entry()];
    let refs = [
        rref("reg-a", Some(".dockerconfigjson")),
        rref("reg-b", Some(".dockerconfigjson")),
        rref("reg-c", Some(".dockerconfigjson")),
    ];
    assert!(uncovered(&refs, &entries, &values, &ctx()).is_empty());
    let out = uncovered(
        &[
            rref("reg-unlisted", Some(".dockerconfigjson")),
            rref("reg-a", Some("token")),
        ],
        &entries,
        &values,
        &ctx(),
    );
    assert_eq!(out.len(), 2, "{out:?}");
    assert!(uncovered(&refs, &entries, &json!({}), &ctx()).len() == 3);
}

#[test]
fn an_unlisted_whole_secret_ref_is_returned() {
    // envFrom and an unfiltered volume take the whole Secret; a key-less ref
    // is not a free pass past a keyed inventory.
    let values = json!({
        "api": {"imagePullSecrets": [{"name": "reg-a"}], "ingress": {"tls": {"secretName": "acme-tls"}}}
    });
    let entries = [postgres_entry(), tls_entry(), image_pull_entry()];
    let manifest = r#"
apiVersion: v1
kind: Pod
metadata:
  name: planted
spec:
  containers:
    - name: c
      envFrom:
        - secretRef:
            name: unlisted-env
  volumes:
    - name: whole
      secret:
        secretName: unlisted-volume
    - name: csi
      csi:
        driver: secrets-store.csi.k8s.io
        nodePublishSecretRef:
          name: unlisted-csi
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: planted
secrets:
  - name: unlisted-sa
imagePullSecrets:
  - name: unlisted-sa-pull
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: planted-pv
spec:
  csi:
    driver: ebs.csi.aws.com
    volumeHandle: vol-1
    nodeStageSecretRef:
      name: unlisted-pv-stage
      namespace: curie
    controllerPublishSecretRef:
      name: unlisted-pv-publish
      namespace: curie
    controllerExpandSecretRef:
      name: unlisted-pv-expand
      namespace: curie
    nodeExpandSecretRef:
      name: unlisted-pv-node-expand
      namespace: curie
"#;
    let refs = extract_refs(manifest).expect("parse manifest");
    let mut names: Vec<String> = uncovered(&refs, &entries, &values, &ctx())
        .into_iter()
        .map(|r| r.name)
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "unlisted-csi",
            "unlisted-env",
            "unlisted-pv-expand",
            "unlisted-pv-node-expand",
            "unlisted-pv-publish",
            "unlisted-pv-stage",
            "unlisted-sa",
            "unlisted-sa-pull",
            "unlisted-volume"
        ]
    );
}

#[test]
fn a_renamed_key_does_not_also_cover_the_inventory_key() {
    let mut e = entry(
        "github-app-key",
        "{release}-curie-github-app",
        &["githubAppPrivateKey"],
    );
    e.chart = Some(ChartBinding {
        default_secret: Some("{fullname}-secrets".into()),
        knobs: vec![ChartKnob {
            secret: "api.githubAppExistingSecret".into(),
            key: Some("api.githubAppExistingSecretKey".into()),
        }],
    });
    let values = json!({"api": {
        "githubAppExistingSecret": "byo-github-app",
        "githubAppExistingSecretKey": "privateKey"
    }});
    let out = uncovered(
        &[rref("byo-github-app", Some("githubAppPrivateKey"))],
        &[e],
        &values,
        &ctx(),
    );
    assert_eq!(out.len(), 1, "{out:?}");
}

#[test]
fn external_credentials_a_rebuild_cannot_regenerate_are_provider_held() {
    let entries = platform_inventory().expect("platform");
    for name in ["image-pull", "grafana-admin", "api-ingress-tls"] {
        let e = entries
            .iter()
            .find(|e| e.logical_name == name)
            .unwrap_or_else(|| panic!("{name} listed"));
        assert_eq!(e.store, Store::Sm, "{name}");
    }
    let tls = entries
        .iter()
        .find(|e| e.logical_name == "api-ingress-tls")
        .expect("tls listed");
    // A supplied certificate has no in-install issuer to recreate it.
    assert_eq!(tls.store, Store::Sm);
    assert_eq!(tls.rotation_owner, RotationOwner::Sm);
}

// --------------------------------------------------------------------------
// bundle_entries
// --------------------------------------------------------------------------

const BUNDLE: &str = "\
connectors:
  ledger:
    image: ghcr.io/example/ledger-mcp:1.0.0
    secrets: [LEDGER_CLIENT_ID, LEDGER_REFRESH_TOKEN]
    secret_rotation:
      LEDGER_REFRESH_TOKEN: workload
  docs:
    url: https://docs.example.com/mcp
    secrets: [DOCS_TOKEN]
";

#[test]
fn bundle_entries_carry_workload_rotation() {
    let decl = parse_connectors(BUNDLE).expect("bundle parses");
    let entries = bundle_entries(&decl, "fin").expect("bundle entries");

    let hosted: Vec<_> = entries
        .iter()
        .filter(|e| e.target == "{release}-fin-connector-secrets")
        .collect();
    assert_eq!(hosted.len(), 1, "{entries:?}");
    let hosted = hosted[0];
    assert!(hosted.keys.contains(&"LEDGER_REFRESH_TOKEN".to_string()));
    assert_eq!(
        hosted.rotation_owner,
        RotationOwner::Workload("ledger".into())
    );
    assert_eq!(
        hosted.rotated_keys,
        vec!["LEDGER_REFRESH_TOKEN".to_string()]
    );
    assert_eq!(hosted.store, Store::Sm);

    let sandbox_target = "{fullname}-agent-fin-connector-secrets";
    for e in &entries {
        if e.target != "{release}-fin-connector-secrets" {
            assert_eq!(e.target, sandbox_target, "{}", e.logical_name);
        }
    }

    let docs: Vec<_> = entries
        .iter()
        .filter(|e| e.keys.contains(&"DOCS_TOKEN".to_string()))
        .collect();
    assert!(!docs.is_empty());
    for e in docs {
        assert_eq!(e.rotation_owner, RotationOwner::Sm);
        assert!(e.rotated_keys.is_empty());
        assert_eq!(
            e.target, sandbox_target,
            "a url connector has no hosted Secret"
        );
    }

    let mut all = platform_inventory().expect("platform");
    all.extend(entries);
    validate_inventory(&all).expect("platform plus bundle validates");
}

fn shared_bundle(alpha_rotates: bool, beta_rotates: bool) -> String {
    let rotation = |on: bool| {
        if on {
            "    secret_rotation:\n      TOKEN: workload\n"
        } else {
            ""
        }
    };
    format!(
        "connectors:\n  alpha:\n    image: ghcr.io/example/alpha:1.0.0\n    secrets: [TOKEN]\n{}  \
         beta:\n    image: ghcr.io/example/beta:1.0.0\n    secrets: [TOKEN]\n{}",
        rotation(alpha_rotates),
        rotation(beta_rotates)
    )
}

fn hosted_token_entry(bundle: &str) -> InventoryEntry {
    let decl = parse_connectors(bundle).expect("bundle parses");
    let entries = bundle_entries(&decl, "fin").expect("bundle entries");
    let hosted: Vec<_> = entries
        .into_iter()
        .filter(|e| e.target == "{release}-fin-connector-secrets")
        .collect();
    assert_eq!(hosted.len(), 1, "{hosted:?}");
    hosted.into_iter().next().expect("one")
}

#[test]
fn a_shared_key_lists_every_consumer() {
    let e = hosted_token_entry(&shared_bundle(false, false));
    assert_eq!(e.keys, vec!["TOKEN".to_string()]);
    assert_eq!(e.consumers, vec!["alpha".to_string(), "beta".to_string()]);
    assert_eq!(e.rotation_owner, RotationOwner::Sm);
}

#[test]
fn a_shared_key_is_owned_by_its_rotator_in_either_order() {
    for (alpha, beta, owner) in [(true, false, "alpha"), (false, true, "beta")] {
        let e = hosted_token_entry(&shared_bundle(alpha, beta));
        assert_eq!(e.consumers, vec!["alpha".to_string(), "beta".to_string()]);
        assert_eq!(e.rotation_owner, RotationOwner::Workload(owner.into()));
        assert_eq!(e.rotated_keys, vec!["TOKEN".to_string()]);
    }
}

#[test]
fn two_rotators_on_one_key_are_refused_naming_both() {
    let decl = parse_connectors(&shared_bundle(true, true)).expect("bundle parses");
    let text = err_text(bundle_entries(&decl, "fin"));
    assert!(text.contains("alpha") && text.contains("beta"), "{text}");
}

#[test]
fn a_rotation_value_other_than_workload_is_refused_at_parse() {
    let bundle = "\
connectors:
  ledger:
    image: ghcr.io/example/ledger-mcp:1.0.0
    secrets: [TOKEN]
    secret_rotation:
      TOKEN: sm
";
    assert!(parse_connectors(bundle).is_err());
}

// --------------------------------------------------------------------------
// Helm-backed: the real chart and the planted controls
// --------------------------------------------------------------------------

fn helm_available() -> bool {
    let ok = Command::new("helm")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !ok {
        eprintln!("skipping: helm not on PATH");
    }
    ok
}

fn default_options() -> CheckOptions {
    let root = repo_root();
    CheckOptions {
        chart: root.join("charts/curie"),
        inventory: None,
        bundles: vec![("sre-bot".into(), root.join("examples/sre-bot"))],
        repo_root: root,
    }
}

/// Copies the chart and plants a Pod referencing a Secret no entry lists.
fn planted_chart(tmp: &Path) -> PathBuf {
    let chart = tmp.join("curie");
    let status = Command::new("cp")
        .arg("-r")
        .arg(repo_root().join("charts/curie"))
        .arg(&chart)
        .status()
        .expect("cp");
    assert!(status.success());
    fs::write(
        chart.join("templates/zz-planted-unlisted.yaml"),
        "apiVersion: v1
kind: Pod
metadata:
  name: planted
spec:
  containers:
    - name: planted
      image: busybox
      env:
        - name: PLANTED
          valueFrom:
            secretKeyRef:
              name: planted-unlisted
              key: plantedKey
",
    )
    .expect("write planted template");
    chart
}

#[test]
fn the_real_chart_is_fully_covered() {
    if !helm_available() {
        return;
    }
    let report = run_check(&default_options()).expect("check runs");
    let failing: Vec<_> = report
        .sets
        .iter()
        .filter(|s| !s.uncovered.is_empty())
        .map(|s| (s.name.clone(), triples(&s.uncovered)))
        .collect();
    assert!(report.passed(), "uncovered: {failing:?}");
    assert!(
        report.sets.len() >= 8,
        "only {} values sets",
        report.sets.len()
    );
    for set in &report.sets {
        assert!(set.refs_checked > 0, "{} checked no refs", set.name);
    }
}

#[test]
fn control_a_an_unlisted_secret_key_ref_fails() {
    if !helm_available() {
        return;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut opts = default_options();
    opts.chart = planted_chart(tmp.path());
    let report = run_check(&opts).expect("check runs");
    assert!(!report.passed());
    assert!(report.sets.iter().any(|s| s
        .uncovered
        .iter()
        .any(|r| r.name == "planted-unlisted" && r.key.as_deref() == Some("plantedKey"))));
}

/// The embedded inventory with the minted Grafana connector token flipped to
/// store: sm. Edited structurally so a reformat of the file cannot silently
/// turn this control into a no-op.
fn flipped_inventory() -> String {
    let mut doc: serde_json::Value =
        serde_norway::from_str(EMBEDDED_INVENTORY).expect("inventory is yaml");
    let mut flipped = 0;
    for e in doc["entries"].as_array_mut().expect("entries list") {
        let has_key = e["keys"]
            .as_array()
            .is_some_and(|ks| ks.iter().any(|k| k == "GRAFANA_SERVICE_ACCOUNT_TOKEN"));
        let minted = e["rotation_owner"]
            .as_str()
            .is_some_and(|o| o.starts_with("mint:"));
        if has_key && minted && e["store"] == "cluster" {
            e["store"] = json!("sm");
            flipped += 1;
        }
    }
    assert!(
        flipped > 0,
        "no minted store: cluster Grafana token entry to flip"
    );
    serde_norway::to_string(&doc).expect("yaml")
}

#[test]
fn control_b_a_rotation_owned_key_marked_eso_managed_fails() {
    if !helm_available() {
        return;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let path = tmp.path().join("inventory.yaml");
    fs::write(&path, flipped_inventory()).expect("write inventory");
    let mut opts = default_options();
    opts.inventory = Some(path);
    let text = err_text(run_check(&opts));
    assert!(text.contains("rotation-owned"), "{text}");
    assert!(text.contains("GRAFANA_SERVICE_ACCOUNT_TOKEN"), "{text}");
}

// --------------------------------------------------------------------------
// CLI: provider-absent regression and control a through the binary
// --------------------------------------------------------------------------

fn find_on_path(tool: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|p| {
        std::env::split_paths(&p)
            .map(|d| d.join(tool))
            .find(|c| c.is_file())
    })
}

#[test]
fn the_check_never_calls_aws_or_kubectl() {
    if !helm_available() {
        return;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let bin = tmp.path().join("bin");
    fs::create_dir(&bin).expect("bin dir");
    let marker = tmp.path().join("provider-called");
    std::os::unix::fs::symlink(find_on_path("helm").expect("helm path"), bin.join("helm"))
        .expect("link helm");
    // git is not a provider; linked so repo discovery is not what fails here.
    if let Some(git) = find_on_path("git") {
        std::os::unix::fs::symlink(git, bin.join("git")).expect("link git");
    }
    for stub in ["aws", "kubectl"] {
        let path = bin.join(stub);
        fs::write(
            &path,
            format!("#!/bin/sh\necho {stub} >> '{}'\nexit 1\n", marker.display()),
        )
        .expect("write stub");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).expect("chmod");
    }
    let out = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["dev", "secrets-inventory"])
        .current_dir(repo_root())
        .env("PATH", &bin)
        .output()
        .expect("run curie");
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!marker.exists(), "the check invoked a provider CLI");
}

#[test]
fn the_cli_fails_on_a_planted_chart() {
    if !helm_available() {
        return;
    }
    let tmp = tempfile::tempdir().expect("tempdir");
    let chart = planted_chart(tmp.path());
    let out = Command::new(env!("CARGO_BIN_EXE_curie"))
        .args(["dev", "secrets-inventory", "--chart"])
        .arg(&chart)
        .current_dir(repo_root())
        .output()
        .expect("run curie");
    assert!(!out.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(text.contains("planted-unlisted"), "{text}");
}
