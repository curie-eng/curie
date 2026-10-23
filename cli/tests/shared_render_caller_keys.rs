// The caller public verify keys the shared application render delivers to its
// proxy (ADR 0239 identity contract 2, ADR 0194 parity).
//
// The frozen Python render validates the keys it takes and writes them,
// comma-joined, into the proxy container's CURIE_CALLER_PROXY_PUBLIC_KEYS;
// `curie::connector_caller::shared_caller_public_keys` mirrors that rule in
// Rust. Both read the corpus frozen in
// `tests/vectors/shared-render-caller-keys.json`, so the two halves give every
// key list the same env value or the same refusal. The accepted values are
// then shown to admit tokens from the frozen seeds in
// `tests/vectors/connector-caller-token.json` whose public keys they list,
// and to refuse a signer they do not list.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use curie::connector_caller::{
    generate_keypair, mint_caller_token, shared_caller_public_keys, verify_caller_token,
    verify_key_of, CallerClaims, CallerKeysRefusal, CallerRefusal, VerifyMode,
    MAX_SHARED_CALLER_PUBLIC_KEYS,
};
use serde_json::Value;

const INSTALLATION: &str = "opaque:installation/1";
const AGENT_ID: &str = "44444444-4444-4444-8444-444444444444";

fn vector_file(name: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tests")
        .join("vectors")
        .join(name);
    serde_json::from_str(&std::fs::read_to_string(path).expect("the frozen vector"))
        .expect("valid json")
}

fn corpus() -> Value {
    vector_file("shared-render-caller-keys.json")
}

fn vectors(corpus: &Value) -> &Vec<Value> {
    let list = corpus["vectors"].as_array().expect("vectors is a list");
    assert!(!list.is_empty(), "vectors is empty");
    list
}

fn name(vector: &Value) -> &str {
    vector["name"].as_str().expect("name")
}

fn keys_of(vector: &Value) -> Vec<&str> {
    vector["public_keys"]
        .as_array()
        .expect("public_keys is a list")
        .iter()
        .map(|key| key.as_str().expect("a key is a string"))
        .collect()
}

fn unix_now() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after the epoch");
    i64::try_from(elapsed.as_secs()).expect("fits i64")
}

fn shared_claims(exp: i64) -> CallerClaims {
    CallerClaims {
        installation: INSTALLATION.to_string(),
        agent_id: AGENT_ID.to_string(),
        agent: "acme-dev".to_string(),
        exp,
        run: None,
        work_item: None,
    }
}

#[test]
fn every_vector_gives_the_frozen_env_value_or_refusal() {
    let corpus = corpus();
    for vector in vectors(&corpus) {
        let keys = keys_of(vector);
        let decided = shared_caller_public_keys(&keys);
        match (vector.get("env"), vector.get("refusal")) {
            (Some(env), None) => {
                let env = env.as_str().expect("env is a string");
                assert_eq!(decided, Ok(env.to_string()), "{}", name(vector));
            }
            (None, Some(refusal)) => {
                let refusal = refusal.as_str().expect("refusal is a string");
                match decided {
                    Err(code) => {
                        assert_eq!(code.as_str(), refusal, "{}", name(vector));
                        assert_eq!(code.to_string(), refusal, "{}", name(vector));
                    }
                    Ok(value) => panic!("{} was accepted as {value:?}", name(vector)),
                }
            }
            _ => panic!("{} names exactly one of env and refusal", name(vector)),
        }
    }
}

#[test]
fn the_bound_and_precedence_match_the_frozen_corpus() {
    let corpus = corpus();
    assert_eq!(
        corpus["env"].as_str(),
        Some("CURIE_CALLER_PROXY_PUBLIC_KEYS")
    );
    assert_eq!(
        corpus["max_keys"].as_u64(),
        Some(u64::try_from(MAX_SHARED_CALLER_PUBLIC_KEYS).expect("fits u64"))
    );
    let precedence: Vec<&str> = corpus["precedence"]
        .as_array()
        .expect("precedence is a list")
        .iter()
        .map(|code| code.as_str().expect("a code is a string"))
        .collect();
    let ordered: Vec<&str> = CallerKeysRefusal::ALL
        .iter()
        .map(|code| code.as_str())
        .collect();
    assert_eq!(precedence, ordered);
    let refused: BTreeSet<&str> = vectors(&corpus)
        .iter()
        .filter_map(|vector| vector.get("refusal").and_then(Value::as_str))
        .collect();
    for code in CallerKeysRefusal::ALL {
        assert!(refused.contains(code.as_str()), "no vector refuses {code}");
    }
}

#[test]
fn every_accepted_value_admits_its_listed_seeds_and_refuses_an_unlisted_signer() {
    let corpus = corpus();
    let tokens = vector_file("connector-caller-token.json");
    let mut seeds: BTreeSet<(String, String)> = BTreeSet::new();
    for vector in tokens["vectors"].as_array().expect("token vectors") {
        let seed = vector["seed"].as_str().expect("seed").to_string();
        let public = vector["public"].as_str().expect("public").to_string();
        assert_eq!(verify_key_of(&seed).expect("a frozen seed"), public);
        seeds.insert((seed, public));
    }
    let now = unix_now();
    let claims = shared_claims(now + 900);
    let mode = VerifyMode::Shared {
        installation: INSTALLATION.to_string(),
    };
    let unlisted = generate_keypair().expect("a fresh key pair");
    let accepted: Vec<&Value> = vectors(&corpus)
        .iter()
        .filter(|vector| vector.get("env").is_some())
        .collect();
    assert!(!accepted.is_empty());
    for vector in accepted {
        let env = vector["env"].as_str().expect("env is a string");
        let keys: Vec<String> = env.split(',').map(str::to_string).collect();
        assert!(!keys.contains(&unlisted.verify_key), "{}", name(vector));
        let mut listed = 0;
        for (seed, public) in &seeds {
            if !keys.contains(public) {
                continue;
            }
            listed += 1;
            let token = mint_caller_token(seed, &claims).expect("mint");
            assert_eq!(
                verify_caller_token(&keys, Some(&token), &mode, now),
                Ok(claims.clone()),
                "{}",
                name(vector)
            );
        }
        assert!(listed > 0, "{} lists no frozen seed", name(vector));
        let token = mint_caller_token(&unlisted.signing_key, &claims).expect("mint");
        assert_eq!(
            verify_caller_token(&keys, Some(&token), &mode, now),
            Err(CallerRefusal::Invalid),
            "{}",
            name(vector)
        );
    }
}
