// The connector caller token, cct2 (ADR 0239 identity contract 2, ADR 0194 parity).
//
// The worker mints it with PyNaCl and the caller proxy verifies it in Python;
// `curie::connector_caller` mirrors both halves in Rust. All three read the
// corpus frozen in `tests/vectors/connector-caller-token.json`, which was
// generated with PyNaCl directly, so the Rust mirror is held to bytes it did
// not write: every positive vector minted byte for byte, every decision given
// the same refusal name and claims, and every mint refusal refused.

use std::collections::BTreeSet;
use std::path::Path;

use curie::connector_caller::{
    mint_caller_token, verify_caller_token, CallerClaims, CallerRefusal, VerifyMode,
    CALLER_TOKEN_PREFIX,
};
use serde_json::Value;

// The corpus cannot carry a lone surrogate (serde_json refuses one), so a mint
// refusal spells it with this placeholder. A Rust `String` cannot hold one.
const SURROGATE_PLACEHOLDER: &str = "<U+D800>";

fn corpus() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tests")
        .join("vectors")
        .join("connector-caller-token.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("the frozen vector"))
        .expect("valid json")
}

fn entries<'a>(corpus: &'a Value, key: &str) -> &'a Vec<Value> {
    let list = corpus[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} is a list"));
    assert!(!list.is_empty(), "{key} is empty");
    list
}

/// A frozen token, stored as its "."-separated segments, joined back; `None`
/// only for a JSON null (no header). Any other shape is a corpus error.
fn token_of(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Array(segments) => Some(
            segments
                .iter()
                .map(|s| {
                    s.as_str()
                        .unwrap_or_else(|| panic!("a token segment is not a string: {s}"))
                })
                .collect::<Vec<_>>()
                .join("."),
        ),
        other => panic!("a token is its segment list or null, not {other}"),
    }
}

fn name(entry: &Value) -> &str {
    entry["name"].as_str().expect("name")
}

/// The claims a JSON object names, or why the Rust types cannot hold them.
///
/// An `Err` is a refusal at the type boundary: a value that is not a string
/// where a `String` goes, a string holding a lone surrogate, or an `exp` that
/// is not an integer representable as `i64`.
fn claims_of(value: &Value) -> Result<CallerClaims, String> {
    let object = value.as_object().expect("claims is an object");
    let text = |key: &str| -> Result<Option<String>, String> {
        match object.get(key) {
            None => Ok(None),
            Some(Value::String(s)) if s.contains(SURROGATE_PLACEHOLDER) => {
                Err(format!("{key} holds a lone surrogate"))
            }
            Some(Value::String(s)) => Ok(Some(s.clone())),
            Some(other) => Err(format!("{key} is not a string: {other}")),
        }
    };
    let required = |key: &str| -> Result<String, String> {
        text(key)?.ok_or_else(|| format!("{key} is missing"))
    };
    let exp = match object.get("exp") {
        Some(number @ Value::Number(_)) => number
            .as_i64()
            .ok_or_else(|| format!("exp {number} does not fit i64"))?,
        other => return Err(format!("exp is not an integer: {other:?}")),
    };
    Ok(CallerClaims {
        installation: required("installation")?,
        agent_id: required("agent_id")?,
        agent: required("agent")?,
        exp,
        run: text("run")?,
        work_item: text("work_item")?,
    })
}

fn mode_of(value: &Value) -> VerifyMode {
    match value["kind"].as_str().expect("mode kind") {
        "dedicated" => VerifyMode::Dedicated {
            admits: value["admits"]
                .as_array()
                .expect("admits")
                .iter()
                .map(|a| a.as_str().expect("admits entry").to_owned())
                .collect(),
        },
        "shared" => VerifyMode::Shared {
            installation: value["installation"]
                .as_str()
                .expect("installation")
                .to_owned(),
        },
        other => panic!("unknown mode {other}"),
    }
}

fn keys_of(value: &Value) -> Vec<String> {
    match value {
        Value::Array(list) => list
            .iter()
            .map(|k| k.as_str().expect("public key").to_owned())
            .collect(),
        Value::String(one) => vec![one.clone()],
        other => panic!("public is not a key list: {other}"),
    }
}

#[test]
fn the_corpus_carries_only_known_keys_and_the_prefix_is_cct2() {
    let corpus = corpus();
    let keys: BTreeSet<&str> = corpus
        .as_object()
        .expect("object")
        .keys()
        .map(String::as_str)
        .collect();
    let expected: BTreeSet<&str> = [
        "comment",
        "prefix",
        "max_token_chars",
        "max_installation_bytes",
        "installation_forbidden",
        "vectors",
        "decisions",
        "mint_refusals",
    ]
    .into_iter()
    .collect();
    assert_eq!(keys, expected);
    assert_eq!(CALLER_TOKEN_PREFIX, "cct2");
    assert_eq!(corpus["prefix"].as_str(), Some(CALLER_TOKEN_PREFIX));
    assert_eq!(corpus["max_token_chars"].as_u64(), Some(4096));
    assert_eq!(corpus["max_installation_bytes"].as_u64(), Some(128));
}

#[test]
fn every_refusal_names_itself_as_the_python_verifier_does() {
    assert_eq!(CallerRefusal::Missing.as_str(), "missing");
    assert_eq!(CallerRefusal::Invalid.as_str(), "invalid");
    assert_eq!(CallerRefusal::Expired.as_str(), "expired");
    assert_eq!(CallerRefusal::NotAdmitted.as_str(), "not_admitted");
}

#[test]
fn the_rust_minter_reproduces_every_frozen_token_byte_for_byte() {
    let corpus = corpus();
    for vector in entries(&corpus, "vectors") {
        let claims =
            claims_of(&vector["claims"]).unwrap_or_else(|why| panic!("{}: {why}", name(vector)));
        let seed = vector["seed"].as_str().expect("seed");
        let minted = mint_caller_token(seed, &claims)
            .unwrap_or_else(|err| panic!("{}: {err:#}", name(vector)));
        assert_eq!(
            Some(minted.as_str()),
            token_of(&vector["minted"]).as_deref(),
            "{}",
            name(vector)
        );
        assert!(minted.starts_with("cct2."), "{}", name(vector));
    }
}

#[test]
fn surrounding_whitespace_in_a_mounted_seed_is_ignored() {
    let corpus = corpus();
    let vector = &entries(&corpus, "vectors")[0];
    let claims = claims_of(&vector["claims"]).expect("claims");
    let seed = format!("{}\n", vector["seed"].as_str().expect("seed"));
    assert_eq!(
        Some(mint_caller_token(&seed, &claims).expect("mint").as_str()),
        token_of(&vector["minted"]).as_deref()
    );
}

#[test]
fn the_rust_verifier_agrees_with_every_decision() {
    let corpus = corpus();
    let decisions = entries(&corpus, "decisions");
    let mut admitted = 0;
    let mut refused = 0;
    for entry in decisions {
        let label = name(entry);
        let keys = keys_of(&entry["public"]);
        let token = token_of(&entry["token"]);
        let mode = mode_of(&entry["mode"]);
        let now = entry["now"].as_i64().expect("now");
        let decided = verify_caller_token(&keys, token.as_deref(), &mode, now);
        match entry["refusal"].as_str() {
            None => {
                assert!(entry["refusal"].is_null(), "{label}");
                let expected =
                    claims_of(&entry["claims"]).unwrap_or_else(|why| panic!("{label}: {why}"));
                assert_eq!(decided, Ok(expected), "{label}");
                admitted += 1;
            }
            Some(refusal) => {
                let got = match decided {
                    Ok(claims) => panic!("{label}: admitted {claims:?}, expected {refusal}"),
                    Err(got) => got,
                };
                assert_eq!(got.as_str(), refusal, "{label}");
                refused += 1;
            }
        }
    }
    assert!(admitted > 0 && refused > 0);
    assert_eq!(admitted + refused, decisions.len());
}

#[test]
fn every_frozen_token_is_admitted_in_its_mode() {
    let corpus = corpus();
    for vector in entries(&corpus, "vectors") {
        let label = name(vector);
        let claims = claims_of(&vector["claims"]).expect("claims");
        let keys = keys_of(&vector["public"]);
        let minted = token_of(&vector["minted"]).unwrap_or_else(|| panic!("{label}: minted"));
        let token = Some(minted.as_str());
        let now = claims.exp - 1;
        let dedicated = VerifyMode::Dedicated {
            admits: vec![claims.agent.clone()],
        };
        assert_eq!(
            verify_caller_token(&keys, token, &dedicated, now),
            Ok(claims.clone()),
            "{label}"
        );
        let shared = VerifyMode::Shared {
            installation: claims.installation.clone(),
        };
        let expected = if claims.installation.is_empty() {
            // Blank is a dedicated standalone worker's installation.
            Err(CallerRefusal::Invalid)
        } else {
            Ok(claims.clone())
        };
        assert_eq!(
            verify_caller_token(&keys, token, &shared, now),
            expected,
            "{label}"
        );
    }
}

// The mint refusals the Rust types refuse before the minter is reached. Pinned
// by name so a type that silently widened (a lossy cast, a stringly exp) is
// caught rather than counted as refused.
const REFUSED_AT_THE_TYPE_BOUNDARY: &[&str] = &[
    "an_installation_that_is_not_a_string",
    "an_installation_with_a_lone_surrogate",
    "an_agent_id_that_is_not_a_string",
    "an_agent_that_is_not_a_string",
    "an_agent_with_a_lone_surrogate",
    "exp_two_to_the_63",
    "exp_two_to_the_64",
    "exp_two_to_the_64_plus_one",
    "exp_as_a_bool",
    "exp_as_a_float",
    "exp_as_a_string",
];

#[test]
fn the_rust_minter_refuses_every_mint_refusal() {
    let corpus = corpus();
    let refusals = entries(&corpus, "mint_refusals");
    let seed = entries(&corpus, "vectors")[0]["seed"]
        .as_str()
        .expect("seed")
        .to_owned();
    let mut at_the_boundary = BTreeSet::new();
    for refusal in refusals {
        let label = name(refusal);
        match claims_of(&refusal["claims"]) {
            Ok(claims) => {
                let refused = mint_caller_token(&seed, &claims);
                assert!(refused.is_err(), "{label}: minted {refused:?}");
                let message = format!("{:#}", refused.unwrap_err());
                assert!(
                    !message.contains(&seed),
                    "{label}: the refusal echoed the seed"
                );
            }
            Err(_why) => {
                at_the_boundary.insert(label.to_owned());
            }
        }
    }
    let expected: BTreeSet<String> = REFUSED_AT_THE_TYPE_BOUNDARY
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
    assert_eq!(at_the_boundary, expected);
    assert!(refusals.len() > expected.len());
}

#[test]
fn the_type_boundary_really_cannot_hold_those_values() {
    // A lone surrogate is not a Rust `char`, so no `String` carries one.
    assert!(char::from_u32(0xD800).is_none());
    assert!(char::from_u32(0xDC00).is_none());
    // 2**63 is one past `i64::MAX`, so a JSON integer that large never becomes an `exp`.
    assert!(i64::try_from(1u64 << 63).is_err());
    let big: Value = serde_json::from_str("9223372036854775808").expect("json");
    assert_eq!(big.as_i64(), None);
    let max: Value = serde_json::from_str("9223372036854775807").expect("json");
    assert_eq!(max.as_i64(), Some(i64::MAX));
}

#[test]
fn a_negative_exp_is_refused_by_the_rust_minter() {
    let corpus = corpus();
    let vector = &entries(&corpus, "vectors")[0];
    let mut claims = claims_of(&vector["claims"]).expect("claims");
    let seed = vector["seed"].as_str().expect("seed");
    for exp in [-1, i64::MIN] {
        claims.exp = exp;
        assert!(mint_caller_token(seed, &claims).is_err(), "exp {exp}");
    }
    claims.exp = 0;
    assert!(mint_caller_token(seed, &claims).is_ok());
    claims.exp = i64::MAX;
    assert!(mint_caller_token(seed, &claims).is_ok());
}

#[test]
fn a_seed_that_is_not_32_bytes_is_refused_by_the_minter() {
    let corpus = corpus();
    let claims = claims_of(&entries(&corpus, "vectors")[0]["claims"]).expect("claims");
    assert!(mint_caller_token("c2hvcnQ=", &claims).is_err());
    assert!(mint_caller_token("not base64!", &claims).is_err());
}

// ADR 0239: a nonblank installation is at most `max_installation_bytes` UTF-8
// bytes with no code point in `installation_forbidden` (general category Cc
// plus Unicode White_Space, written out as inclusive ranges). A Rust `String`
// holds no surrogate, so the corpus's ranges are the whole rule here.
const UNREADABLE_SEED: &str = "not base64!";

fn forbidden_ranges(corpus: &Value) -> Vec<(u32, u32)> {
    corpus["installation_forbidden"]
        .as_array()
        .expect("installation_forbidden is a list")
        .iter()
        .map(|pair| {
            let bounds = pair.as_array().expect("a [first, last] pair");
            assert_eq!(bounds.len(), 2, "{pair}");
            let bound = |at: usize| {
                u32::try_from(bounds[at].as_u64().expect("a code point")).expect("a code point")
            };
            (bound(0), bound(1))
        })
        .collect()
}

fn rule_accepts(corpus: &Value, value: &str) -> bool {
    let ranges = forbidden_ranges(corpus);
    let max = usize::try_from(corpus["max_installation_bytes"].as_u64().expect("max"))
        .expect("max fits usize");
    !value.is_empty()
        && value.len() <= max
        && value.chars().all(|c| {
            !ranges
                .iter()
                .any(|&(first, last)| (first..=last).contains(&u32::from(c)))
        })
}

fn claims_with(corpus: &Value, installation: String) -> CallerClaims {
    let mut claims = claims_of(&entries(corpus, "vectors")[0]["claims"]).expect("claims");
    claims.installation = installation;
    claims
}

#[test]
fn the_rust_minter_applies_the_installation_rule_to_every_char() {
    // Exhaustive over every `char`. The minter checks the claims before it
    // reads the seed, so under a seed that is not base64 an accepted
    // installation fails on the seed, exactly as the baseline claims do, and a
    // refused one fails first on the claim. That decides each of the 1.1M
    // chars without signing; the boundary test below signs for real.
    let corpus = corpus();
    let ranges = forbidden_ranges(&corpus);
    let mut claims = claims_with(&corpus, "acme".to_owned());
    let seed_refusal = format!(
        "{:#}",
        mint_caller_token(UNREADABLE_SEED, &claims).expect_err("an unreadable seed never mints")
    );
    let mut wrong = Vec::new();
    for c in (0..=0x10FFFF_u32).filter_map(char::from_u32) {
        claims.installation = c.to_string();
        let refused = mint_caller_token(UNREADABLE_SEED, &claims)
            .expect_err("an unreadable seed never mints");
        let accepted = format!("{refused:#}") == seed_refusal;
        let expected = !ranges
            .iter()
            .any(|&(first, last)| (first..=last).contains(&u32::from(c)));
        if accepted != expected {
            wrong.push(format!("U+{:04X} expected {expected}", u32::from(c)));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} chars disagree; first {:?}",
        wrong.len(),
        &wrong[..wrong.len().min(20)]
    );
}

#[test]
fn the_rust_minter_and_verifier_apply_the_installation_rule_at_its_boundaries() {
    let corpus = corpus();
    let vector = &entries(&corpus, "vectors")[0];
    let seed = vector["seed"].as_str().expect("seed");
    let keys = keys_of(&vector["public"]);
    let mut points: BTreeSet<u32> = [
        0x21, 0x7E, 0x1C89, 0x200B, 0xD7FF, 0xE000, 0xFEFF, 0xFFFD, 0x10000, 0x10FFFF,
    ]
    .into_iter()
    .collect();
    for (first, last) in forbidden_ranges(&corpus) {
        points.extend([first.saturating_sub(1), first, last, last + 1]);
    }
    let singles: Vec<char> = points.into_iter().filter_map(char::from_u32).collect();
    let mut values: Vec<String> = singles.iter().map(|c| c.to_string()).collect();
    values.extend(singles.iter().map(|c| format!("acme{c}install")));
    values.extend([
        "\u{e9}".repeat(64),
        format!("{}a", "\u{e9}".repeat(64)),
        format!("{}\u{e9}", "\u{20ac}".repeat(42)),
        "A".repeat(128),
        "A".repeat(129),
        "acme-\u{e9}".to_owned(),
        "acme-e\u{301}".to_owned(),
        "\u{1f680}".to_owned(),
    ]);
    for value in values {
        let expected = rule_accepts(&corpus, &value);
        let claims = claims_with(&corpus, value.clone());
        let minted = mint_caller_token(seed, &claims);
        assert_eq!(minted.is_ok(), expected, "mint {value:?}: {minted:?}");
        let Ok(token) = minted else { continue };
        let shared = VerifyMode::Shared {
            installation: value.clone(),
        };
        assert_eq!(
            verify_caller_token(&keys, Some(&token), &shared, claims.exp - 1),
            Ok(claims.clone()),
            "verify {value:?}"
        );
    }
}
