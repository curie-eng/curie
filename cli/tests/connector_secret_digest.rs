//! #4717: a hosted connector Deployment rolls when the owned Secret keys it
//! consumes change, and only then.

use std::collections::BTreeMap;

use curie::connectors::{stamp_secret_digests, CONNECTOR_SECRET_DIGEST};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn digest_of(pairs: &[(&str, &str)]) -> String {
    let mut map = BTreeMap::new();
    for (key, value) in pairs {
        map.insert(*key, *value);
    }
    let encoded = serde_json::to_vec(&map).expect("canonical map");
    Sha256::digest(encoded)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn deployment(name: &str, spec: Value) -> Value {
    json!({
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {"name": name, "annotations": {"keep": "yes"}},
        "spec": spec
    })
}

fn env_ref(name: &str, secret: &str, key: &str) -> Value {
    json!({
        "name": name,
        "valueFrom": {"secretKeyRef": {"name": secret, "key": key}}
    })
}

fn file_volume(secret: &str, key: &str) -> Value {
    json!({
        "name": format!("file-{key}"),
        "secret": {
            "secretName": secret,
            "items": [{"key": key, "path": "value"}]
        }
    })
}

fn template(env: Vec<Value>, volumes: Vec<Value>) -> Value {
    json!({
        "replicas": 1,
        "template": {
            "metadata": {"labels": {"app": "connector"}, "annotations": {"existing": "kept"}},
            "spec": {"containers": [{"name": "server", "env": env}], "volumes": volumes}
        }
    })
}

fn fingerprint(manifest: &Value) -> Option<String> {
    manifest
        .pointer("/spec/template/metadata/annotations")
        .and_then(|annotations| annotations.get(CONNECTOR_SECRET_DIGEST))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn assert_same(left: &Value, right: &Value) {
    if left != right {
        panic!("manifest changed");
    }
}

#[test]
fn rotating_one_value_changes_only_its_connector_digest() {
    let owned = "rel-acme-bot-connector-secrets";
    let mut values = BTreeMap::from([
        ("ROTATE_TOKEN".into(), "sentinel-token-old".into()),
        ("ROTATE_SETTING".into(), "sentinel-setting-old".into()),
        ("ROTATE_FILE".into(), "sentinel-file-old".into()),
        ("OTHER_TOKEN".into(), "sentinel-other".into()),
        ("UNUSED".into(), "sentinel-unused".into()),
    ]);
    let rotate = deployment(
        "rotate",
        template(
            vec![
                env_ref("ROTATE_TOKEN", owned, "ROTATE_TOKEN"),
                env_ref("ROTATE_SETTING", owned, "ROTATE_SETTING"),
                env_ref("ROTATE_TOKEN", owned, "ROTATE_TOKEN"),
            ],
            vec![file_volume(owned, "ROTATE_FILE")],
        ),
    );
    let other = deployment(
        "other",
        template(vec![env_ref("OTHER_TOKEN", owned, "OTHER_TOKEN")], vec![]),
    );
    let foreign = deployment(
        "foreign",
        template(
            vec![env_ref("FOREIGN", "someone-else", "FOREIGN")],
            vec![file_volume("someone-else", "FOREIGN")],
        ),
    );
    let idle = deployment("idle", template(vec![], vec![]));
    let service = json!({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "svc"}});
    let mut manifests = vec![
        rotate.clone(),
        other.clone(),
        foreign.clone(),
        idle.clone(),
        service.clone(),
    ];
    stamp_secret_digests(&mut manifests, owned, &values);

    let rotate_old = fingerprint(&manifests[0]).expect("rotate stamped");
    let other_old = fingerprint(&manifests[1]).expect("other stamped");
    if rotate_old
        != digest_of(&[
            ("ROTATE_FILE", "sentinel-file-old"),
            ("ROTATE_SETTING", "sentinel-setting-old"),
            ("ROTATE_TOKEN", "sentinel-token-old"),
        ])
    {
        panic!("rotate fingerprint mismatch");
    }
    if other_old != digest_of(&[("OTHER_TOKEN", "sentinel-other")]) {
        panic!("other fingerprint mismatch");
    }
    if fingerprint(&manifests[2]).is_some() || fingerprint(&manifests[3]).is_some() {
        panic!("a deployment with no owned key was stamped");
    }
    assert_same(&manifests[2], &foreign);
    assert_same(&manifests[3], &idle);
    assert_same(&manifests[4], &service);
    if manifests[0]
        .pointer("/metadata/annotations/keep")
        .and_then(Value::as_str)
        != Some("yes")
        || manifests[0]
            .pointer("/spec/template/metadata/annotations/existing")
            .and_then(Value::as_str)
            != Some("kept")
        || manifests[0]
            .pointer("/spec/replicas")
            .and_then(Value::as_u64)
            != Some(1)
    {
        panic!("existing fields were not preserved");
    }

    let mut reordered = vec![rotate.clone()];
    let mut shuffled = BTreeMap::new();
    shuffled.insert("UNUSED".into(), values["UNUSED"].clone());
    shuffled.insert("ROTATE_FILE".into(), values["ROTATE_FILE"].clone());
    shuffled.insert("ROTATE_TOKEN".into(), values["ROTATE_TOKEN"].clone());
    shuffled.insert("ROTATE_SETTING".into(), values["ROTATE_SETTING"].clone());
    shuffled.insert("OTHER_TOKEN".into(), values["OTHER_TOKEN"].clone());
    stamp_secret_digests(&mut reordered, owned, &shuffled);
    if fingerprint(&reordered[0]).as_deref() != Some(rotate_old.as_str()) {
        panic!("reordered inputs changed the fingerprint");
    }

    for (key, sentinel) in [
        ("ROTATE_TOKEN", "sentinel-token-new"),
        ("ROTATE_SETTING", "sentinel-setting-new"),
        ("ROTATE_FILE", "sentinel-file-new"),
    ] {
        values.insert(key.into(), sentinel.into());
        let mut next = vec![rotate.clone(), other.clone(), idle.clone()];
        stamp_secret_digests(&mut next, owned, &values);
        if fingerprint(&next[0]).as_deref() == Some(rotate_old.as_str()) {
            panic!("rotating a consumed value left the consumer fingerprint unchanged");
        }
        if fingerprint(&next[1]).as_deref() != Some(other_old.as_str()) {
            panic!("rotating one connector changed another connector");
        }
        if fingerprint(&next[2]).is_some() {
            panic!("an idle deployment was stamped during rotation");
        }
        let rendered = serde_json::to_string(&next).expect("json");
        let human = format!("connector rotate: ready\n{key}: explicit deploy input\n{rendered}");
        // The apply document carries the fingerprint. Human lines must not.
        let human_only = format!("connector rotate: ready\n{key}: explicit deploy input");
        if human_only.contains(sentinel)
            || human_only.contains(CONNECTOR_SECRET_DIGEST)
            || human.contains("sentinel-token-old")
        {
            panic!("a credential or fingerprint reached human output");
        }
    }
}
