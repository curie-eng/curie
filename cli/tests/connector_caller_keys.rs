// The connector caller key pair `cluster up` generates (ADR-0168 decision 7).
//
// The worker signs with PyNaCl and every hosted connector's proxy verifies
// with it, so the public half this CLI hands the API must be the one PyNaCl
// derives from the same seed. The frozen corpus both Python halves read,
// `tests/vectors/connector-caller-token.json`, pins that here too.

use std::path::Path;

use curie::connector_caller::{generate_keypair, verify_key_of};

fn corpus() -> serde_json::Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tests")
        .join("vectors")
        .join("connector-caller-token.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("the frozen vector"))
        .expect("valid json")
}

#[test]
fn every_frozen_seed_derives_its_frozen_public_key() {
    let corpus = corpus();
    let vectors = corpus["vectors"].as_array().expect("vectors");
    assert!(!vectors.is_empty());
    for vector in vectors {
        let seed = vector["seed"].as_str().expect("seed");
        let public = vector["public"].as_str().expect("public");
        assert_eq!(verify_key_of(seed).unwrap(), public, "{}", vector["name"]);
    }
}

#[test]
fn a_generated_pair_is_a_real_pair_and_never_repeats() {
    let first = generate_keypair().unwrap();
    let second = generate_keypair().unwrap();
    assert_eq!(verify_key_of(&first.signing_key).unwrap(), first.verify_key);
    assert_ne!(first.signing_key, second.signing_key);
}

#[test]
fn a_seed_that_is_not_32_bytes_is_refused() {
    assert!(verify_key_of("c2hvcnQ=").is_err());
    assert!(verify_key_of("not base64!").is_err());
}
