//! CLI half of the frozen remediation policy and codes vectors.
//!
//! @spec AUTOMATED-REMEDIATION-3 @spec AUTOMATED-REMEDIATION-26. The API validates a remediation policy and
//! the `remediation-policy` verbs mirror that validation, so a document the
//! API refuses is refused here first with the API's code and path
//! (`tests/vectors/remediation-policy.json`, read on the API side by
//! `apps/api/tests/test_remediation_policy_vector.py`). The policy verbs
//! render the API's closed policy refusal codes
//! (`policy_refusals` in `tests/vectors/remediation-codes.json`); the
//! nomination vocabularies there are rendered by the receipt verbs, which land
//! with a nomination read route (task 13), so they are not read here yet.
//!
//! The readers are `curie::remediation_policy::validate_policy_document`
//! (`&serde_json::Value` to `Result<(), PolicyRefusal>`, whose `code` and `path`
//! are the API's), `curie::remediation_policy::parse_policy_text` (a strict
//! parse whose failure is a `policy_document_invalid` refusal), and the closed
//! policy refusal list `curie::remediation::POLICY_REFUSALS`.

use std::collections::{BTreeMap, BTreeSet};

use curie::remediation;
use curie::remediation_policy::{parse_policy_text, validate_policy_document};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ValidCase {
    name: String,
    document: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvalidCase {
    name: String,
    document: Value,
    code: String,
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct InvalidText {
    name: String,
    text: String,
    code: String,
    path: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyVector {
    comment: String,
    refusal_codes: Vec<String>,
    valid: Vec<ValidCase>,
    invalid: Vec<InvalidCase>,
    invalid_texts: Vec<InvalidText>,
    numeric_texts: Vec<InvalidText>,
}

/// Every key is declared so an unknown key still fails; only `policy_refusals`
/// is compared until the receipt verbs land.
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CodesVector {
    comment: String,
    nomination_states: Vec<String>,
    nomination_refusals: Vec<String>,
    submission_refusals: Vec<String>,
    admission_checks: BTreeMap<String, Vec<String>>,
    approval_reasons: Vec<String>,
    approval_resolution_refusals: Vec<String>,
    verification_outcomes: Vec<String>,
    receipt_stages: Vec<String>,
    kinds: Vec<String>,
    authorities: Vec<String>,
    authority_kinds: Vec<String>,
    actor_kinds: Vec<String>,
    policy_refusals: Vec<String>,
}

fn policy_vector() -> PolicyVector {
    let vector: PolicyVector = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/vectors/remediation-policy.json"
    )))
    .expect("parse tests/vectors/remediation-policy.json (an unknown key fails here)");
    assert!(!vector.comment.is_empty());
    vector
}

fn codes_vector() -> CodesVector {
    let vector: CodesVector = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../tests/vectors/remediation-codes.json"
    )))
    .expect("parse tests/vectors/remediation-codes.json (an unknown key fails here)");
    assert!(!vector.comment.is_empty());
    vector
}

fn set(values: &[String]) -> BTreeSet<&str> {
    values.iter().map(String::as_str).collect()
}

fn set_of(values: &[&'static str]) -> BTreeSet<&'static str> {
    values.iter().copied().collect()
}

/// @spec AUTOMATED-REMEDIATION-2: a closed document within every bound passes.
#[test]
fn each_valid_policy_document_is_accepted() {
    for case in policy_vector().valid {
        if let Err(refusal) = validate_policy_document(&case.document) {
            panic!(
                "{}: refused {} at {}",
                case.name, refusal.code, refusal.path
            );
        }
    }
}

/// @spec AUTOMATED-REMEDIATION-3: the CLI refuses with the API's reason.
#[test]
fn each_invalid_policy_document_is_refused_with_the_api_code_and_path() {
    for case in policy_vector().invalid {
        let refusal = validate_policy_document(&case.document)
            .expect_err(&format!("{}: accepted an invalid document", case.name));
        assert_eq!(
            (refusal.code.as_str(), refusal.path.as_str()),
            (case.code.as_str(), case.path.as_str()),
            "{}",
            case.name
        );
    }
}

/// @spec AUTOMATED-REMEDIATION-2: a non-finite number never reaches the API.
#[test]
fn a_non_finite_policy_text_is_refused() {
    for case in policy_vector().invalid_texts {
        let refusal = parse_policy_text(&case.text)
            .expect_err(&format!("{}: parsed a non-finite number", case.name));
        assert_eq!(
            refusal.code, case.code,
            "{} (API path {})",
            case.name, case.path
        );
    }
}

/// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3: a number a
/// native JSON value can change (an integer outside the signed 64-bit range,
/// an exponent beyond a double) is refused with the API's code AND path, by a
/// strict parse followed by validation, as `curie <tier> remediation-policy
/// apply` reads a file.
#[test]
fn a_number_a_json_value_can_change_is_refused_at_its_path() {
    for case in policy_vector().numeric_texts {
        let refusal = parse_policy_text(&case.text)
            .and_then(|document| validate_policy_document(&document))
            .expect_err(&format!("{}: accepted", case.name));
        assert_eq!(
            (refusal.code.as_str(), refusal.path.as_str()),
            (case.code.as_str(), case.path.as_str()),
            "{}",
            case.name
        );
    }
}

/// @spec AUTOMATED-REMEDIATION-26: every document code is a policy refusal.
#[test]
fn the_document_codes_are_policy_refusals() {
    let policy = policy_vector();
    let codes = codes_vector();
    assert!(set(&policy.refusal_codes).is_subset(&set(&codes.policy_refusals)));
    assert_eq!(
        set_of(remediation::POLICY_REFUSALS),
        set(&codes.policy_refusals)
    );
}
