//! Tracker scope uniqueness in the CLI factory preflight (#4622, ADR 0197).
//!
//! `tests/vectors/forge-tracker-scopes.json` is shared with the chart renderer
//! and the API loader. Each case's `values` is the chart `factory:` block; the
//! preflight `curie::factory_intake::check_factory_document` reads it as the
//! effective values document `{"factory": values}` before any Helm apply. An
//! accepted case passes; a duplicate scope is refused naming both bindings and
//! the scope (tracker kind, host, trackerScopeId); an unknown tracker reference
//! is refused naming the binding and the tracker; a repeated tracker or binding
//! name is refused naming the section (`factory.trackers`, `factory.bindings`)
//! and the name; a trackerScopeId that is not a nonempty string or a whole
//! number below 2^53 is refused naming the binding.

use std::fs;
use std::path::PathBuf;

use curie::factory_intake::check_factory_document;
use serde_json::{json, Value};

fn vector() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../tests/vectors/forge-tracker-scopes.json");
    let text = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&text).expect("the tracker scope vector is JSON")
}

fn cases() -> Vec<Value> {
    vector()["cases"]
        .as_array()
        .expect("the vector has a cases list")
        .clone()
}

fn text(value: &Value) -> &str {
    value.as_str().expect("a vector string")
}

#[test]
fn every_vector_case_matches_its_outcome() {
    let cases = cases();
    assert!(cases.len() >= 12, "the vector lost cases");
    for case in &cases {
        let name = text(&case["name"]);
        let outcome = &case["outcome"];
        let result: Result<(), String> =
            check_factory_document(&json!({"factory": case["values"]}));
        if outcome["accepted"].as_bool() == Some(true) {
            assert!(
                result.is_ok(),
                "{name}: an accepted case was refused: {result:?}"
            );
            continue;
        }
        let message: String = match result {
            Ok(()) => panic!("{name}: a refused case was accepted"),
            Err(message) => message,
        };
        match text(&outcome["refused"]) {
            "duplicate_scope" => {
                let scope = &outcome["scope"];
                let mut needles: Vec<&str> = outcome["bindings"]
                    .as_array()
                    .expect("duplicate_scope names its bindings")
                    .iter()
                    .map(text)
                    .collect();
                needles.extend([
                    text(&scope["kind"]),
                    text(&scope["host"]),
                    text(&scope["trackerScopeId"]),
                ]);
                for needle in needles {
                    assert!(
                        message.contains(needle),
                        "{name}: refusal does not name {needle:?}: {message}"
                    );
                }
            }
            "unknown_tracker" => {
                for needle in [text(&outcome["binding"]), text(&outcome["tracker"])] {
                    assert!(
                        message.contains(needle),
                        "{name}: refusal does not name {needle:?}: {message}"
                    );
                }
            }
            "duplicate_name" => {
                let section = format!("factory.{}", text(&outcome["section"]));
                for needle in [section.as_str(), text(&outcome["name"])] {
                    assert!(
                        message.contains(needle),
                        "{name}: refusal does not name {needle:?}: {message}"
                    );
                }
            }
            "invalid_scope_id" => {
                let needle = format!(
                    "({}) trackerScopeId must be a nonempty string or a whole number",
                    text(&outcome["binding"])
                );
                assert!(
                    message.contains(&needle),
                    "{name}: refusal does not name {needle:?}: {message}"
                );
            }
            other => panic!("{name}: unknown outcome {other}"),
        }
    }
}

#[test]
fn a_document_without_factory_bindings_passes() {
    for document in [
        json!({}),
        json!({"factory": {}}),
        json!({"factory": {"trackers": [], "bindings": [], "codeHosts": []}}),
    ] {
        assert_eq!(check_factory_document(&document), Ok(()), "{document}");
    }
}
