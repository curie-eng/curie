//! @spec ACTION-EXECUTOR-23, ACTION-EXECUTOR-16: the CLI bundle check mirrors the
//! API's reserved sealing key refusal.
//!
//! The API refuses `SNAPSHOT_SEALING_KEY` and `SNAPSHOT_SEALING_KEYS_RETAINED`
//! in every form but a `SecretRef` (`curie_internal.sealing_key`,
//! `curie_api.bundles.sealing_key_custody_issues`). Rust cannot import that, so
//! the CLI's mirror (`curie::sealing_key`) and the API both read
//! `tests/vectors/sealing-key-custody.json`: the names, the refusal wording, the
//! reference grammar, and the refusals each bundle earns with their locations.
//! The API half is `apps/api/tests/test_sealing_key_custody_vector.py`.

use std::path::Path;

use curie::exit::{classify, ExitClass};
use curie::sealing_key;
use serde_json::Value;

fn vector() -> Value {
    let raw = include_str!("../../tests/vectors/sealing-key-custody.json");
    serde_json::from_str(raw).expect("parse tests/vectors/sealing-key-custody.json")
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("a JSON array")
        .iter()
        .map(|item| item.as_str().expect("a JSON string").to_string())
        .collect()
}

/// Lay a vector bundle case out on disk the way the API reads it.
fn write_bundle(root: &Path, case: &Value) {
    let plugin_dir = root.join(".claude-plugin");
    std::fs::create_dir_all(&plugin_dir).expect("create .claude-plugin");
    let mut manifest = serde_json::json!({
        "name": "sealer",
        "version": "0.1.0",
        "description": "t",
    });
    if let Some(secrets) = case.get("plugin_secrets") {
        manifest["secrets"] = secrets.clone();
    }
    std::fs::write(
        plugin_dir.join("plugin.json"),
        serde_json::to_string_pretty(&manifest).expect("serialize plugin.json"),
    )
    .expect("write plugin.json");
    if let Some(connectors) = case.get("connectors_yaml").and_then(Value::as_str) {
        std::fs::write(root.join("connectors.yaml"), connectors).expect("write connectors.yaml");
    }
}

#[test]
fn reserved_names_match_the_api() {
    let vector = vector();
    let mut names: Vec<String> = sealing_key::SEALING_KEY_NAMES
        .iter()
        .map(|name| name.to_string())
        .collect();
    names.sort();
    assert_eq!(names, strings(&vector["names"]));
    assert_eq!(sealing_key::SEALING_KEY_NAME, "SNAPSHOT_SEALING_KEY");
    assert_eq!(
        sealing_key::SEALING_KEYS_RETAINED_NAME,
        "SNAPSHOT_SEALING_KEYS_RETAINED"
    );
    for name in strings(&vector["names"]) {
        assert!(
            sealing_key::is_sealing_key_name(&name),
            "{name} is reserved"
        );
    }
    for control in [
        "MY_SEAL_KEY",
        "SNAPSHOT_SEALING_KEYRING",
        "snapshot_sealing_key",
    ] {
        assert!(
            !sealing_key::is_sealing_key_name(control),
            "{control} is not a reserved name"
        );
    }
}

#[test]
fn refusal_wording_is_the_apis_verbatim() {
    let vector = vector();
    assert_eq!(
        sealing_key::SEALING_KEY_CUSTODY_REASON,
        vector["reason"].as_str().expect("reason")
    );
    for name in strings(&vector["names"]) {
        assert_eq!(
            sealing_key::custody_reason(&name),
            vector["reasons"][&name].as_str().expect("reasons[name]"),
            "the CLI's refusal for {name} drifted from the API's"
        );
    }
}

#[test]
fn references_match_the_apis_grammar() {
    let vector = vector();
    for case in vector["references"].as_array().expect("references") {
        let text = case["text"].as_str().expect("text");
        assert_eq!(
            sealing_key::sealing_key_references(text),
            strings(&case["names"]),
            "references in {text:?}"
        );
    }
}

#[test]
fn every_bundle_earns_exactly_the_apis_refusals() {
    let vector = vector();
    let bundles = vector["bundles"].as_array().expect("bundles");
    assert!(!bundles.is_empty());
    for case in bundles {
        let id = case["name"].as_str().expect("case name");
        let dir = tempfile::tempdir().expect("tempdir");
        write_bundle(dir.path(), case);

        let issues = sealing_key::custody_issues(dir.path())
            .unwrap_or_else(|err| panic!("{id}: the check failed to read the bundle: {err:#}"));
        let got: Vec<(String, String, String)> = issues
            .iter()
            .map(|issue| {
                (
                    issue.name.clone(),
                    issue.location.clone(),
                    issue.message.clone(),
                )
            })
            .collect();
        let want: Vec<(String, String, String)> = case["refused"]
            .as_array()
            .expect("refused")
            .iter()
            .map(|refusal| {
                let name = refusal["name"].as_str().expect("name").to_string();
                let message = vector["reasons"][&name]
                    .as_str()
                    .expect("reasons[name]")
                    .to_string();
                (
                    name,
                    refusal["location"].as_str().expect("location").to_string(),
                    message,
                )
            })
            .collect();
        assert_eq!(got, want, "{id}: the CLI check disagrees with the API");
    }
}

#[test]
fn the_refusal_is_a_usage_error_with_the_apis_reason_and_a_fix() {
    let vector = vector();
    for case in vector["bundles"].as_array().expect("bundles") {
        let id = case["name"].as_str().expect("case name");
        let dir = tempfile::tempdir().expect("tempdir");
        write_bundle(dir.path(), case);
        let refused = case["refused"].as_array().expect("refused");

        let result = sealing_key::refuse_sealing_key_custody(dir.path());
        if refused.is_empty() {
            assert!(
                result.is_ok(),
                "{id}: an accepted bundle was refused: {:#}",
                result.unwrap_err()
            );
            continue;
        }
        let err = result.expect_err(id);
        let (class, fix) = classify(&err);
        assert_eq!(
            class,
            ExitClass::Usage,
            "{id}: a bundle refusal is a usage error"
        );
        assert!(
            fix.as_deref().is_some_and(|fix| fix.contains("SecretRef")),
            "{id}: the fix must say how to declare the key: {fix:?}"
        );
        let rendered = format!("{err:#}");
        for refusal in refused {
            let name = refusal["name"].as_str().expect("name");
            let location = refusal["location"].as_str().expect("location");
            assert!(
                rendered.contains(vector["reasons"][name].as_str().expect("reason")),
                "{id}: the error must carry the API's reason for {name}: {rendered}"
            );
            assert!(
                rendered.contains(location),
                "{id}: the error must name where {name} is declared ({location}): {rendered}"
            );
        }
    }
}

#[test]
fn a_refusal_never_echoes_the_declared_value() {
    let vector = vector();
    let case = vector["bundles"]
        .as_array()
        .expect("bundles")
        .iter()
        .find(|case| case["name"] == "env.SNAPSHOT_SEALING_KEY")
        .expect("the env case");
    let dir = tempfile::tempdir().expect("tempdir");
    write_bundle(dir.path(), case);
    let err = sealing_key::refuse_sealing_key_custody(dir.path()).expect_err("refused");
    let rendered = format!("{err:#} {:?}", classify(&err).1);
    assert!(
        !rendered.contains("SEALVALUE-placeholder"),
        "the refusal echoes the key's value: {rendered}"
    );
}
