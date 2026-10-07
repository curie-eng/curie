//! The CLI mirror of the API's remediation policy document validator.
//!
//! @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3 @spec AUTOMATED-REMEDIATION-10
//! @spec AUTOMATED-REMEDIATION-24
//!
//! `apps/api/src/curie_api/remediation_policy_document.py` validates a policy
//! document; `curie <tier> remediation-policy apply` runs this mirror first so
//! a document the API would refuse is refused before any request, with the
//! API's code and path. The two cannot share code across Python and Rust, so
//! they are pinned to the frozen vector `tests/vectors/remediation-policy.json`
//! (read here by `cli/tests/remediation_vectors.rs` and on the API side by
//! `apps/api/tests/test_remediation_policy_vector.py`). The checks run in the
//! API's order, so the first refusal is the API's first refusal.
//!
//! Two deliberate differences, neither reachable by a document the API
//! accepts: the CLI's JSON objects iterate keys in sorted order rather than
//! document order, so when several keys of one `limits` or `arguments` object
//! are each invalid the CLI may name a different one first; and a non-finite
//! number is refused by [`parse_policy_text`], at parse time, because a parsed
//! [`Value`] cannot hold one.

use std::fmt;

use serde_json::{Map, Value};

/// AUTOMATED-REMEDIATION-10 defaults and ceilings. A policy may only tighten
/// them; the incident window may only be lengthened.
pub const PER_POLICY_PER_HOUR_CEILING: i128 = 3;
pub const PER_INCIDENT_PER_TARGET_CEILING: i128 = 1;
pub const INCIDENT_WINDOW_SECONDS_MINIMUM: i128 = 3600;
pub const APPROVAL_TTL_SECONDS_DEFAULT: i128 = 14400;
pub const APPROVAL_TTL_SECONDS_CEILING: i128 = 86400;
/// An upper bound so a window is a sane integer; not a policy ceiling.
const SECONDS_MAXIMUM: i128 = (1 << 31) - 1;

const KINDS: &[&str] = &["remediate", "prevent", "tune"];
const NEVER_AUTOMATIC_KINDS: &[&str] = &["prevent", "tune"];
const READS_REQUIRED_KINDS: &[&str] = &["remediate", "prevent"];
const REVERSIBILITIES: &[&str] = &["reversible", "idempotent"];
const COMPARATORS: &[&str] = &["eq", "ne", "lt", "le", "gt", "ge", "in", "absent"];
const ARGUMENT_TYPES: &[&str] = &["string", "integer", "number", "boolean"];
const RANGE_TYPES: &[&str] = &["integer", "number"];

const TOP_KEYS: &[&str] = &["route", "limits", "actions"];
const LIMIT_KEYS: &[&str] = &[
    "per_policy_per_hour",
    "per_incident_per_target",
    "per_action_per_hour",
    "incident_window_seconds",
    "approval_ttl_seconds",
];
const ACTION_KEYS: &[&str] = &[
    "name",
    "kind",
    "connector",
    "tool",
    "arguments",
    "target",
    "reversibility",
    "precondition",
    "verifier",
    "automatic",
    "qualification",
];
const ACTION_REQUIRED: &[&str] = &[
    "name",
    "kind",
    "connector",
    "tool",
    "arguments",
    "target",
    "reversibility",
    "automatic",
];
const ARGUMENT_KEYS: &[&str] = &["type", "allowed", "minimum", "maximum"];
const DELTA_KEYS: &[&str] = &["max_delta", "min_delta", "delta"];
const TARGET_KEYS: &[&str] = &["argument", "allowed"];
const READ_KEYS: &[&str] = &[
    "connector",
    "tool",
    "arguments",
    "pointer",
    "comparator",
    "value",
];
const VERIFIER_KEYS: &[&str] = &[
    "connector",
    "tool",
    "arguments",
    "pointer",
    "comparator",
    "value",
    "settle_seconds",
    "deadline_seconds",
    "interval_seconds",
    "consecutive",
];

const IN_LIST_MAXIMUM: usize = 16;
const ALLOWED_MAXIMUM: usize = 256;
const VERIFIER_INTERVAL_MINIMUM: i128 = 10;
const VERIFIER_DEADLINE_MAXIMUM: i128 = 3600;
const VERIFIER_SAMPLE_CAP: i128 = 60;

// @spec AUTOMATED-REMEDIATION-2
/// A named policy refusal: the API's `detail.code` and `detail.path` (empty
/// for the document root), plus a human reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyRefusal {
    pub code: String,
    pub path: String,
    pub message: String,
}

impl fmt::Display for PolicyRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            write!(f, "{}: the document {}", self.code, self.message)
        } else {
            write!(f, "{} at {}: {}", self.code, self.path, self.message)
        }
    }
}

impl std::error::Error for PolicyRefusal {}

type Checked<T> = Result<T, PolicyRefusal>;

fn refuse(code: &str, path: impl Into<String>, message: impl Into<String>) -> PolicyRefusal {
    PolicyRefusal {
        code: code.to_string(),
        path: path.into(),
        message: message.into(),
    }
}

fn invalid(path: impl Into<String>, message: impl Into<String>) -> PolicyRefusal {
    refuse("policy_document_invalid", path, message)
}

fn one_of(value: &Value, set: &[&str]) -> bool {
    value.as_str().is_some_and(|text| set.contains(&text))
}

/// An object whose keys are all in `allowed`; the first unknown key in
/// sorted order is named, as the API names it.
fn closed<'a>(value: &'a Value, path: &str, allowed: &[&str]) -> Checked<&'a Map<String, Value>> {
    let Some(object) = value.as_object() else {
        return Err(invalid(path, "must be an object"));
    };
    let mut unknown: Vec<&String> = object
        .keys()
        .filter(|key| !allowed.contains(&key.as_str()))
        .collect();
    unknown.sort();
    match unknown.first() {
        Some(key) => Err(refuse(
            "policy_unknown_key",
            format!("{path}/{key}"),
            "is not a policy key",
        )),
        None => Ok(object),
    }
}

/// Every `required` key is present; the first missing one in sorted order is
/// named.
fn require(object: &Map<String, Value>, path: &str, required: &[&str]) -> Checked<()> {
    let mut missing: Vec<&str> = required
        .iter()
        .copied()
        .filter(|key| !object.contains_key(*key))
        .collect();
    missing.sort_unstable();
    match missing.first() {
        Some(key) => Err(invalid(format!("{path}/{key}"), "is required")),
        None => Ok(()),
    }
}

/// A JSON integer (never a boolean, never a float such as `3.0`).
fn integer(value: &Value, path: &str) -> Checked<i128> {
    let number = value
        .as_number()
        .and_then(|n| n.as_i64().map(i128::from).or(n.as_u64().map(i128::from)));
    number.ok_or_else(|| invalid(path, "must be an integer"))
}

/// A finite JSON number (integer or float, never a boolean).
fn number(value: &Value, path: &str) -> Checked<f64> {
    value
        .as_number()
        .and_then(serde_json::Number::as_f64)
        .filter(|n| n.is_finite())
        .ok_or_else(|| invalid(path, "must be a finite number"))
}

fn is_identifier(text: &str) -> bool {
    let bytes = text.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'/' | b'-'))
}

fn identifier(value: &Value, path: &str) -> Checked<()> {
    match value.as_str() {
        Some(text) if is_identifier(text) => Ok(()),
        _ => Err(invalid(path, "must be a non-empty identifier")),
    }
}

/// `[a-z0-9][a-z0-9_-]{0,62}`.
fn is_action_name(text: &str) -> bool {
    let bytes = text.as_bytes();
    let lower_or_digit = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    !bytes.is_empty()
        && bytes.len() <= 63
        && lower_or_digit(&bytes[0])
        && bytes[1..]
            .iter()
            .all(|b| lower_or_digit(b) || matches!(b, b'_' | b'-'))
}

/// An RFC 6901 pointer: empty, or `/`-led segments where `~` escapes only
/// `0` or `1`.
fn is_pointer(text: &str) -> bool {
    if text.is_empty() {
        return true;
    }
    if !text.starts_with('/') {
        return false;
    }
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '~' && !matches!(chars.next(), Some('0' | '1')) {
            return false;
        }
    }
    true
}

/// A JSON scalar: anything but an array or an object (a parsed number is
/// always finite).
fn scalar(value: &Value) -> bool {
    !value.is_array() && !value.is_object()
}

/// Python's `==` over JSON scalars, which `value in list` uses: numbers
/// compare by value across int and float, and a boolean equals `1` or `0`.
fn python_eq(left: &Value, right: &Value) -> bool {
    fn numeric(value: &Value) -> Option<(Option<i128>, f64)> {
        match value {
            Value::Bool(flag) => Some((Some(i128::from(*flag)), f64::from(u8::from(*flag)))),
            Value::Number(n) => Some((
                n.as_i64().map(i128::from).or(n.as_u64().map(i128::from)),
                n.as_f64()?,
            )),
            _ => None,
        }
    }
    match (numeric(left), numeric(right)) {
        (Some((Some(a), _)), Some((Some(b), _))) => a == b,
        (Some((_, a)), Some((_, b))) => a == b,
        _ => left == right,
    }
}

fn bounded(value: i128, path: &str, minimum: i128, maximum: i128) -> Checked<i128> {
    if value < minimum || value > maximum {
        return Err(refuse(
            "policy_limit_out_of_bounds",
            path,
            format!("must be between {minimum} and {maximum}"),
        ));
    }
    Ok(value)
}

// @spec AUTOMATED-REMEDIATION-10
/// Limits may only tighten the defaults.
fn validate_limits(limits: &Value) -> Checked<()> {
    let path = "/limits";
    let limits = closed(limits, path, LIMIT_KEYS)?;
    let mut values = std::collections::BTreeMap::new();
    for (key, value) in limits {
        values.insert(key.as_str(), integer(value, &format!("{path}/{key}"))?);
    }
    let get = |key: &str, default: i128| values.get(key).copied().unwrap_or(default);
    let per_policy = bounded(
        get("per_policy_per_hour", PER_POLICY_PER_HOUR_CEILING),
        &format!("{path}/per_policy_per_hour"),
        1,
        PER_POLICY_PER_HOUR_CEILING,
    )?;
    bounded(
        get("per_incident_per_target", PER_INCIDENT_PER_TARGET_CEILING),
        &format!("{path}/per_incident_per_target"),
        1,
        PER_INCIDENT_PER_TARGET_CEILING,
    )?;
    if let Some(per_action) = values.get("per_action_per_hour") {
        bounded(
            *per_action,
            &format!("{path}/per_action_per_hour"),
            1,
            per_policy,
        )?;
    }
    bounded(
        get("incident_window_seconds", INCIDENT_WINDOW_SECONDS_MINIMUM),
        &format!("{path}/incident_window_seconds"),
        INCIDENT_WINDOW_SECONDS_MINIMUM,
        SECONDS_MAXIMUM,
    )?;
    bounded(
        get("approval_ttl_seconds", APPROVAL_TTL_SECONDS_DEFAULT),
        &format!("{path}/approval_ttl_seconds"),
        1,
        APPROVAL_TTL_SECONDS_CEILING,
    )?;
    Ok(())
}

fn allowed_list<'a>(value: &'a Value, path: &str) -> Checked<&'a Vec<Value>> {
    let list = value
        .as_array()
        .filter(|list| !list.is_empty() && list.len() <= ALLOWED_MAXIMUM)
        .ok_or_else(|| {
            invalid(
                path,
                format!("must be a non-empty list of at most {ALLOWED_MAXIMUM} literal values"),
            )
        })?;
    if !list.iter().all(|item| scalar(item) && !item.is_null()) {
        return Err(invalid(path, "must hold JSON scalars only"));
    }
    Ok(list)
}

// @spec AUTOMATED-REMEDIATION-2
/// One closed argument schema entry.
fn validate_argument(spec: &Value, path: &str) -> Checked<()> {
    if let Some(object) = spec.as_object() {
        let mut delta: Vec<&String> = object
            .keys()
            .filter(|key| DELTA_KEYS.contains(&key.as_str()))
            .collect();
        delta.sort();
        if let Some(key) = delta.first() {
            return Err(refuse(
                "delta_bound_unsupported",
                format!("{path}/{key}"),
                "magnitude is bounded by absolute ranges only",
            ));
        }
    }
    let spec = closed(spec, path, ARGUMENT_KEYS)?;
    require(spec, path, &["type"])?;
    let kind = &spec["type"];
    if !one_of(kind, ARGUMENT_TYPES) {
        return Err(invalid(
            format!("{path}/type"),
            "is not a known argument type",
        ));
    }
    let has_allowed = spec.contains_key("allowed");
    let has_range = spec.contains_key("minimum") || spec.contains_key("maximum");
    if has_allowed == has_range {
        return Err(invalid(
            path,
            "needs either an allowed set or a minimum and maximum",
        ));
    }
    if has_allowed {
        allowed_list(&spec["allowed"], &format!("{path}/allowed"))?;
        return Ok(());
    }
    if !one_of(kind, RANGE_TYPES) {
        return Err(invalid(path, "a range needs an integer or number type"));
    }
    require(spec, path, &["minimum", "maximum"])?;
    let above = if kind == "integer" {
        integer(&spec["minimum"], &format!("{path}/minimum"))?
            > integer(&spec["maximum"], &format!("{path}/maximum"))?
    } else {
        number(&spec["minimum"], &format!("{path}/minimum"))?
            > number(&spec["maximum"], &format!("{path}/maximum"))?
    };
    if above {
        return Err(invalid(path, "minimum is above maximum"));
    }
    Ok(())
}

// @spec AUTOMATED-REMEDIATION-2
/// A declared read (the AUTOMATED-REMEDIATION-17 shape).
fn validate_read(read: &Value, path: &str, verifier: bool) -> Checked<()> {
    let keys = if verifier { VERIFIER_KEYS } else { READ_KEYS };
    let read = closed(read, path, keys)?;
    require(read, path, &["connector", "tool", "pointer", "comparator"])?;
    identifier(&read["connector"], &format!("{path}/connector"))?;
    identifier(&read["tool"], &format!("{path}/tool"))?;
    if read
        .get("arguments")
        .is_some_and(|value| !value.is_object())
    {
        return Err(invalid(format!("{path}/arguments"), "must be an object"));
    }
    if !read["pointer"].as_str().is_some_and(is_pointer) {
        return Err(invalid(
            format!("{path}/pointer"),
            "must be an RFC 6901 pointer",
        ));
    }
    let comparator = &read["comparator"];
    if !one_of(comparator, COMPARATORS) {
        return Err(invalid(format!("{path}/comparator"), "is not a comparator"));
    }
    let value_path = format!("{path}/value");
    match (comparator.as_str(), read.get("value")) {
        (Some("absent"), Some(_)) => return Err(invalid(value_path, "absent takes no value")),
        (Some("absent"), None) => {}
        (_, None) => return Err(invalid(value_path, "is required")),
        (Some("in"), Some(value)) => {
            let list = value
                .as_array()
                .filter(|list| !list.is_empty() && list.len() <= IN_LIST_MAXIMUM);
            let Some(list) = list else {
                return Err(invalid(
                    value_path,
                    format!("in takes a list of 1 to {IN_LIST_MAXIMUM} scalars"),
                ));
            };
            if !list.iter().all(scalar) {
                return Err(invalid(value_path, "must hold scalars only"));
            }
        }
        (_, Some(value)) => {
            if !scalar(value) {
                return Err(invalid(value_path, "must be a JSON scalar"));
            }
        }
    }
    if verifier {
        validate_verifier_timing(read, path)?;
    }
    Ok(())
}

// @spec AUTOMATED-REMEDIATION-2
/// The AUTOMATED-REMEDIATION-17 verifier timing bounds.
fn validate_verifier_timing(read: &Map<String, Value>, path: &str) -> Checked<()> {
    require(
        read,
        path,
        &["settle_seconds", "deadline_seconds", "interval_seconds"],
    )?;
    let interval = integer(
        &read["interval_seconds"],
        &format!("{path}/interval_seconds"),
    )?;
    let settle = integer(&read["settle_seconds"], &format!("{path}/settle_seconds"))?;
    let deadline = integer(
        &read["deadline_seconds"],
        &format!("{path}/deadline_seconds"),
    )?;
    let consecutive = match read.get("consecutive") {
        Some(value) => integer(value, &format!("{path}/consecutive"))?,
        None => 1,
    };
    if interval < VERIFIER_INTERVAL_MINIMUM {
        return Err(invalid(
            format!("{path}/interval_seconds"),
            format!("must be at least {VERIFIER_INTERVAL_MINIMUM}"),
        ));
    }
    if settle < interval {
        return Err(invalid(
            format!("{path}/settle_seconds"),
            "must be at least the interval",
        ));
    }
    if deadline <= settle
        || deadline > VERIFIER_DEADLINE_MAXIMUM
        || deadline > VERIFIER_SAMPLE_CAP * interval
    {
        return Err(invalid(
            format!("{path}/deadline_seconds"),
            format!(
                "must exceed settle and be at most {VERIFIER_DEADLINE_MAXIMUM} and \
                 {VERIFIER_SAMPLE_CAP} intervals"
            ),
        ));
    }
    if consecutive < 1 {
        return Err(invalid(format!("{path}/consecutive"), "must be at least 1"));
    }
    Ok(())
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-24
/// One action; returns its name.
fn validate_action<'a>(action: &'a Value, path: &str) -> Checked<&'a str> {
    let action = closed(action, path, ACTION_KEYS)?;
    require(action, path, ACTION_REQUIRED)?;
    let Some(name) = action["name"].as_str().filter(|name| is_action_name(name)) else {
        return Err(invalid(
            format!("{path}/name"),
            "must match [a-z0-9][a-z0-9_-]{0,62}",
        ));
    };
    let kind = &action["kind"];
    if !one_of(kind, KINDS) {
        return Err(invalid(format!("{path}/kind"), "is not a known kind"));
    }
    let kind = kind.as_str().unwrap_or_default();
    identifier(&action["connector"], &format!("{path}/connector"))?;
    identifier(&action["tool"], &format!("{path}/tool"))?;
    if !one_of(&action["reversibility"], REVERSIBILITIES) {
        return Err(invalid(
            format!("{path}/reversibility"),
            "is not a known reversibility",
        ));
    }
    let Some(automatic) = action["automatic"].as_bool() else {
        return Err(invalid(format!("{path}/automatic"), "must be a boolean"));
    };

    let Some(arguments) = action["arguments"].as_object() else {
        return Err(invalid(format!("{path}/arguments"), "must be an object"));
    };
    for (key, spec) in arguments {
        validate_argument(spec, &format!("{path}/arguments/{key}"))?;
    }

    let target_path = format!("{path}/target");
    let target = closed(&action["target"], &target_path, TARGET_KEYS)?;
    require(target, &target_path, TARGET_KEYS)?;
    let Some(argument) = target["argument"]
        .as_str()
        .filter(|argument| arguments.contains_key(*argument))
    else {
        return Err(invalid(
            format!("{target_path}/argument"),
            "must name a declared argument",
        ));
    };
    let allowed_targets = allowed_list(&target["allowed"], &format!("{target_path}/allowed"))?;
    if let Some(argument_allowed) = arguments[argument].get("allowed").filter(|v| !v.is_null()) {
        let argument_allowed = argument_allowed.as_array().map_or(&[][..], Vec::as_slice);
        if allowed_targets
            .iter()
            .any(|value| !argument_allowed.iter().any(|item| python_eq(value, item)))
        {
            return Err(invalid(
                format!("{target_path}/allowed"),
                "must be within the target argument's allowed set",
            ));
        }
    }

    for read in ["precondition", "verifier"] {
        if let Some(value) = action.get(read).filter(|value| !value.is_null()) {
            validate_read(value, &format!("{path}/{read}"), read == "verifier")?;
        }
    }
    let declared = |read: &str| action.get(read).is_some_and(|value| !value.is_null());
    if READS_REQUIRED_KINDS.contains(&kind) && !(declared("precondition") && declared("verifier")) {
        return Err(refuse(
            "precondition_and_verifier_required",
            path,
            format!("a {kind} action declares both a precondition and a verifier"),
        ));
    }
    if automatic && NEVER_AUTOMATIC_KINDS.contains(&kind) {
        return Err(refuse(
            "kind_not_automatic",
            format!("{path}/automatic"),
            format!("a {kind} action is never automatic"),
        ));
    }

    match action.get("qualification") {
        None | Some(Value::Null) => {}
        Some(Value::String(reference)) if !reference.is_empty() => {}
        Some(_) => {
            return Err(invalid(
                format!("{path}/qualification"),
                "must be null or a reference",
            ))
        }
    }
    Ok(name)
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-24
/// Validate a whole policy document as the API's `validate_document` does,
/// returning the API's first refusal.
pub fn validate_policy_document(document: &Value) -> Result<(), PolicyRefusal> {
    let document = closed(document, "", TOP_KEYS)?;
    require(document, "", TOP_KEYS)?;
    if !document["route"]
        .as_str()
        .is_some_and(|route| !route.trim().is_empty())
    {
        return Err(invalid("/route", "must name an approval route"));
    }
    validate_limits(&document["limits"])?;
    let Some(actions) = document["actions"]
        .as_array()
        .filter(|actions| !actions.is_empty())
    else {
        return Err(invalid("/actions", "must be a non-empty list"));
    };
    let mut names = std::collections::BTreeSet::new();
    for (index, action) in actions.iter().enumerate() {
        let name = validate_action(action, &format!("/actions/{index}"))?;
        if !names.insert(name) {
            return Err(invalid(format!("/actions/{index}/name"), "is a duplicate"));
        }
    }
    Ok(())
}

// @spec AUTOMATED-REMEDIATION-2
/// Parse a policy file strictly. Standard JSON has no `NaN` or `Infinity`, so
/// a document carrying one (which the API's parser would accept and then
/// refuse) is refused here as `policy_document_invalid`, as is any other text
/// that is not JSON.
pub fn parse_policy_text(text: &str) -> Result<Value, PolicyRefusal> {
    serde_json::from_str(text).map_err(|error| {
        invalid(
            "",
            format!("is not JSON with finite numbers only ({error})"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn pointer_shapes() {
        assert!(is_pointer(""));
        assert!(is_pointer("/a/~0b/~1c"));
        assert!(!is_pointer("a"));
        assert!(!is_pointer("/a~2"));
        assert!(!is_pointer("/a~"));
    }

    #[test]
    fn python_equality_crosses_int_float_and_bool() {
        assert!(python_eq(&json!(1), &json!(1.0)));
        assert!(python_eq(&json!(true), &json!(1)));
        assert!(!python_eq(&json!("1"), &json!(1)));
        assert!(python_eq(&json!("a"), &json!("a")));
    }

    #[test]
    fn a_refusal_names_code_and_path() {
        let refusal = invalid("/route", "must name an approval route");
        assert_eq!(
            refusal.to_string(),
            "policy_document_invalid at /route: must name an approval route"
        );
    }
}
