//! The chart's published dev connector caller pair (#3552, #195).
//!
//! A Go template cannot derive an Ed25519 public key, so
//! `charts/curie/templates/_helpers.tpl` carries both halves as literals. A
//! public half that is not the seed's own would leave every caller proxy on a
//! dev install refusing every token the worker signs.

use curie::connector_caller::verify_key_of;

const HELPERS_TEMPLATE: &str = include_str!("../../charts/curie/templates/_helpers.tpl");

fn literal_defined_as(helper: &str) -> &'static str {
    let marker = format!("{{{{- define \"{helper}\" -}}}}");
    let start = HELPERS_TEMPLATE
        .find(&marker)
        .unwrap_or_else(|| panic!("_helpers.tpl defines no {helper}"))
        + marker.len();
    let rest = &HELPERS_TEMPLATE[start..];
    &rest[..rest.find("{{-").expect("the definition is closed")]
}

#[test]
fn the_published_dev_public_key_is_the_published_seeds_own() {
    let seed = literal_defined_as("curie.connectorCallerDevSigningKey");
    let public = literal_defined_as("curie.connectorCallerDevVerifyKey");
    assert_eq!(verify_key_of(seed).unwrap(), public);
}

#[test]
fn the_published_dev_seed_is_the_documented_one() {
    use base64::Engine as _;
    let seed = base64::engine::general_purpose::STANDARD
        .decode(literal_defined_as("curie.connectorCallerDevSigningKey"))
        .unwrap();
    assert_eq!(seed, b"curie-dev-connector-caller-seed!");
}
