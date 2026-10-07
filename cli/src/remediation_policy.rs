//! The CLI mirror of the API's remediation policy document validator.
//!
//! @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3 @spec AUTOMATED-REMEDIATION-10
//! @spec AUTOMATED-REMEDIATION-24 @spec AUTOMATED-REMEDIATION-25
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
//! Numbers are compared exactly, as Python compares them, and a number a
//! native JSON value would change (an integer outside the signed 64-bit
//! range, a non-finite number) is refused at its path by
//! [`parse_policy_text`], as the API's pre-check refuses it. One deliberate
//! difference, unreachable by a document the API accepts: the CLI's JSON
//! objects iterate keys in sorted order rather than document order, so when
//! several keys of one `limits` or `arguments` object are each invalid the
//! CLI may name a different one first.

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
const TUNE_ACTION_KEYS: &[&str] = &[
    "name",
    "kind",
    "connector",
    "tool",
    "rules",
    "change",
    "automatic",
    "qualification",
];
const TUNE_ACTION_REQUIRED: &[&str] = &[
    "name",
    "kind",
    "connector",
    "tool",
    "rules",
    "change",
    "automatic",
];
/// AUTOMATED-REMEDIATION-25: the closed set of tunable rule fields.
const TUNE_FIELDS: &[&str] = &["threshold", "for_duration", "group_by", "dedupe", "retire"];
const TUNE_RETIRE: &str = "retire";
const TUNE_RULE_KEYS: &[&str] = &["current", "evidence"];
const TUNE_READ_KEYS: &[&str] = &["connector", "tool", "arguments", "pointer"];
const TUNE_RETIRE_KEYS: &[&str] = &["duplicate_of"];
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

/// A JSON number held exactly: an integer as an integer, never rounded
/// through a double, as Python holds it.
#[derive(Clone, Copy, Debug)]
enum Num {
    Int(i128),
    Float(f64),
}

/// The exact number a value holds; a boolean is `1` or `0`, as in Python.
fn num_of(value: &Value) -> Option<Num> {
    match value {
        Value::Bool(flag) => Some(Num::Int(i128::from(*flag))),
        Value::Number(n) => match n.as_i64().map(i128::from).or(n.as_u64().map(i128::from)) {
            Some(int) => Some(Num::Int(int)),
            None => n.as_f64().map(Num::Float),
        },
        _ => None,
    }
}

/// Python's exact ordering of two numbers: an integer and a float compare by
/// their mathematical values, never through a rounded double.
fn num_cmp(left: Num, right: Num) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    fn int_vs_float(int: i128, float: f64) -> Option<Ordering> {
        if float.is_nan() {
            return None;
        }
        // Past +-2^126 the float is beyond every integer a document can hold.
        if float >= 2f64.powi(126) {
            return Some(Ordering::Less);
        }
        if float <= -(2f64.powi(126)) {
            return Some(Ordering::Greater);
        }
        let floor = float.floor();
        // `floor` is integral and in range, so the cast is exact.
        let whole = floor as i128;
        if floor == float {
            Some(int.cmp(&whole))
        } else if int <= whole {
            Some(Ordering::Less)
        } else {
            Some(Ordering::Greater)
        }
    }
    match (left, right) {
        (Num::Int(a), Num::Int(b)) => Some(a.cmp(&b)),
        (Num::Float(a), Num::Float(b)) => a.partial_cmp(&b),
        (Num::Int(a), Num::Float(b)) => int_vs_float(a, b),
        (Num::Float(a), Num::Int(b)) => int_vs_float(b, a).map(Ordering::reverse),
    }
}

/// A finite JSON number (integer or float, never a boolean).
fn number(value: &Value, path: &str) -> Checked<Num> {
    match (value, num_of(value)) {
        (Value::Number(_), Some(Num::Int(int))) => Ok(Num::Int(int)),
        (Value::Number(_), Some(Num::Float(float))) if float.is_finite() => Ok(Num::Float(float)),
        _ => Err(invalid(path, "must be a finite number")),
    }
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
/// compare exactly by value across int and float (never through a rounded
/// double), and a boolean equals `1` or `0`.
fn python_eq(left: &Value, right: &Value) -> bool {
    match (num_of(left), num_of(right)) {
        (Some(a), Some(b)) => num_cmp(a, b) == Some(std::cmp::Ordering::Equal),
        _ => left == right,
    }
}

/// Python's `str.strip()`: Unicode whitespace plus the information
/// separators U+001C to U+001F, which `str.isspace` also counts.
fn python_strip(text: &str) -> &str {
    text.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
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
        let minimum = number(&spec["minimum"], &format!("{path}/minimum"))?;
        let maximum = number(&spec["maximum"], &format!("{path}/maximum"))?;
        num_cmp(minimum, maximum) == Some(std::cmp::Ordering::Greater)
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

// @spec AUTOMATED-REMEDIATION-25
/// A tune action's declared read: no comparator, the value is shown, not
/// judged.
fn validate_tune_read(read: &Value, path: &str) -> Checked<()> {
    let read = closed(read, path, TUNE_READ_KEYS)?;
    require(read, path, &["connector", "tool", "pointer"])?;
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
    Ok(())
}

// @spec AUTOMATED-REMEDIATION-25
/// A map from a field or evidence name to a declared read.
fn validate_tune_reads(reads: &Value, path: &str) -> Checked<()> {
    let Some(reads) = reads.as_object() else {
        return Err(invalid(path, "must be an object"));
    };
    for (key, read) in reads {
        let read_path = format!("{path}/{key}");
        if !is_identifier(key) {
            return Err(invalid(read_path, "must be a non-empty identifier"));
        }
        validate_tune_read(read, &read_path)?;
    }
    Ok(())
}

// @spec AUTOMATED-REMEDIATION-24 @spec AUTOMATED-REMEDIATION-25
/// The AUTOMATED-REMEDIATION-25 shape, after the common name, kind,
/// connector and tool.
fn validate_tune(action: &Map<String, Value>, path: &str) -> Checked<()> {
    let Some(automatic) = action["automatic"].as_bool() else {
        return Err(invalid(format!("{path}/automatic"), "must be a boolean"));
    };

    let Some(rules) = action["rules"]
        .as_object()
        .filter(|rules| !rules.is_empty())
    else {
        return Err(invalid(
            format!("{path}/rules"),
            "must be a non-empty object",
        ));
    };
    for (rule, declared) in rules {
        let rule_path = format!("{path}/rules/{rule}");
        if !is_identifier(rule) {
            return Err(invalid(rule_path, "must be a non-empty identifier"));
        }
        let declared = closed(declared, &rule_path, TUNE_RULE_KEYS)?;
        for reads in TUNE_RULE_KEYS {
            if let Some(value) = declared.get(*reads) {
                validate_tune_reads(value, &format!("{rule_path}/{reads}"))?;
            }
        }
    }

    let change_path = format!("{path}/change");
    let Some(change) = action["change"]
        .as_object()
        .filter(|change| !change.is_empty())
    else {
        return Err(invalid(change_path, "must be a non-empty object"));
    };
    closed(&action["change"], &change_path, TUNE_FIELDS)?;
    for (field, spec) in change {
        let field_path = format!("{change_path}/{field}");
        if field != TUNE_RETIRE {
            validate_argument(spec, &field_path)?;
            continue;
        }
        let spec = closed(spec, &field_path, TUNE_RETIRE_KEYS)?;
        require(spec, &field_path, TUNE_RETIRE_KEYS)?;
        let Some(duplicate_of) = spec["duplicate_of"]
            .as_array()
            .filter(|list| !list.is_empty())
        else {
            return Err(invalid(
                format!("{field_path}/duplicate_of"),
                "must be a non-empty list of declared rules",
            ));
        };
        for (index, rule) in duplicate_of.iter().enumerate() {
            if !rule.as_str().is_some_and(|rule| rules.contains_key(rule)) {
                return Err(invalid(
                    format!("{field_path}/duplicate_of/{index}"),
                    "must name a rule the action declares",
                ));
            }
        }
    }

    for (rule, declared) in rules {
        let current = declared.get("current").and_then(Value::as_object);
        for field in current.into_iter().flat_map(Map::keys) {
            if !change.contains_key(field) {
                return Err(invalid(
                    format!("{path}/rules/{rule}/current/{field}"),
                    "must be a field the change declares",
                ));
            }
        }
    }

    if automatic {
        return Err(refuse(
            "kind_not_automatic",
            format!("{path}/automatic"),
            "a tune action is never automatic",
        ));
    }
    Ok(())
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-22
fn validate_qualification(action: &Map<String, Value>, path: &str) -> Checked<()> {
    match action.get("qualification") {
        None | Some(Value::Null) => Ok(()),
        Some(Value::String(reference)) if !reference.is_empty() => Ok(()),
        Some(_) => Err(invalid(
            format!("{path}/qualification"),
            "must be null or a reference",
        )),
    }
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-24 @spec AUTOMATED-REMEDIATION-25
/// One action; returns its name. The closed key set is the action kind's:
/// `tune` has its own, and any other or unknown kind the `remediate`/`prevent`
/// set.
fn validate_action<'a>(action: &'a Value, path: &str) -> Checked<&'a str> {
    let tune = action.get("kind").and_then(Value::as_str) == Some("tune");
    let (keys, required) = if tune {
        (TUNE_ACTION_KEYS, TUNE_ACTION_REQUIRED)
    } else {
        (ACTION_KEYS, ACTION_REQUIRED)
    };
    let action = closed(action, path, keys)?;
    require(action, path, required)?;
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
    if tune {
        validate_tune(action, path)?;
        validate_qualification(action, path)?;
        return Ok(name);
    }
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

    validate_qualification(action, path)?;
    Ok(name)
}

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-10 @spec AUTOMATED-REMEDIATION-24
// @spec AUTOMATED-REMEDIATION-25
/// Validate a whole policy document as the API's `validate_document` does,
/// returning the API's first refusal.
pub fn validate_policy_document(document: &Value) -> Result<(), PolicyRefusal> {
    let document = closed(document, "", TOP_KEYS)?;
    require(document, "", TOP_KEYS)?;
    if !document["route"]
        .as_str()
        .is_some_and(|route| !python_strip(route).is_empty())
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

// @spec AUTOMATED-REMEDIATION-2 @spec AUTOMATED-REMEDIATION-3
/// Parse a policy file as the API reads it, refusing a number a native JSON
/// value would change, at that number's path.
///
/// The API's parser keeps an integer of any size exact and reads `NaN`,
/// `Infinity` and an exponent beyond a double (`1e400`) as non-finite floats;
/// its validator then refuses each at its path before any other check. A
/// `serde_json::Value` would instead round a large integer to a double and
/// reject `1e400` with no path, so this reader parses the text itself and
/// refuses, at the number's pointer:
///
/// * an integer outside the signed 64-bit range: `policy_limit_out_of_bounds`
///   when it is a member of the top-level `limits` object (a well-formed number
///   outside the limit's bounds), `policy_document_invalid` anywhere else;
/// * a non-finite number (`NaN`, `Infinity`, `-Infinity`, or an exponent
///   beyond a double): `policy_document_invalid`.
///
/// The first such number in document order is the refusal, as the API's
/// pre-check walks the document. Any other text that is not JSON is
/// `policy_document_invalid` at the document root.
pub fn parse_policy_text(text: &str) -> Result<Value, PolicyRefusal> {
    let mut parser = Parser {
        bytes: text.as_bytes(),
        at: 0,
        first_number_refusal: None,
    };
    let parsed = parser
        .value("", Place::Root, 0)
        .and_then(|value| {
            parser.skip_whitespace();
            if parser.at == parser.bytes.len() {
                Ok(value)
            } else {
                Err(parser.syntax("trailing characters after the document"))
            }
        })
        .map_err(|message| invalid("", format!("is not a JSON document ({message})")))?;
    match parser.first_number_refusal {
        Some(refusal) => Err(refusal),
        None => Ok(parsed),
    }
}

/// Where a value sits, as far as the number checks care.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Place {
    Root,
    /// The top-level `limits` member.
    Limits,
    /// A direct member of the top-level `limits` object.
    LimitField,
    Other,
}

/// The deepest nesting accepted, as `serde_json` limits it.
const MAX_DEPTH: usize = 128;

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
    first_number_refusal: Option<PolicyRefusal>,
}

impl Parser<'_> {
    fn syntax(&self, what: &str) -> String {
        format!("{what} at byte {}", self.at)
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.bytes.get(self.at), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.at += 1;
        }
    }

    fn eat(&mut self, literal: &str) -> bool {
        if self.bytes[self.at..].starts_with(literal.as_bytes()) {
            self.at += literal.len();
            true
        } else {
            false
        }
    }

    fn refuse_number(&mut self, refusal: PolicyRefusal) {
        if self.first_number_refusal.is_none() {
            self.first_number_refusal = Some(refusal);
        }
    }

    fn non_finite(&mut self, path: &str) -> Value {
        self.refuse_number(invalid(
            if path.is_empty() { "/" } else { path },
            "must be a finite number",
        ));
        Value::Null
    }

    fn value(&mut self, path: &str, place: Place, depth: usize) -> Result<Value, String> {
        if depth > MAX_DEPTH {
            return Err(self.syntax("nesting too deep"));
        }
        self.skip_whitespace();
        match self.bytes.get(self.at) {
            None => Err(self.syntax("unexpected end of text")),
            Some(b'{') => self.object(path, place, depth),
            Some(b'[') => self.array(path, depth),
            Some(b'"') => self.string().map(Value::String),
            Some(b't') if self.eat("true") => Ok(Value::Bool(true)),
            Some(b'f') if self.eat("false") => Ok(Value::Bool(false)),
            Some(b'n') if self.eat("null") => Ok(Value::Null),
            Some(b'N') if self.eat("NaN") => Ok(self.non_finite(path)),
            Some(b'I') if self.eat("Infinity") => Ok(self.non_finite(path)),
            Some(b'-') if self.eat("-Infinity") => Ok(self.non_finite(path)),
            Some(b'-' | b'0'..=b'9') => self.number(path, place),
            Some(_) => Err(self.syntax("unexpected character")),
        }
    }

    fn object(&mut self, path: &str, place: Place, depth: usize) -> Result<Value, String> {
        self.at += 1;
        let mut map = Map::new();
        self.skip_whitespace();
        if self.eat("}") {
            return Ok(Value::Object(map));
        }
        loop {
            self.skip_whitespace();
            if self.bytes.get(self.at) != Some(&b'"') {
                return Err(self.syntax("expected a member name"));
            }
            let key = self.string()?;
            self.skip_whitespace();
            if !self.eat(":") {
                return Err(self.syntax("expected ':'"));
            }
            let child = match (place, key.as_str()) {
                (Place::Root, "limits") => Place::Limits,
                (Place::Limits, _) => Place::LimitField,
                _ => Place::Other,
            };
            let value = self.value(&format!("{path}/{key}"), child, depth + 1)?;
            map.insert(key, value);
            self.skip_whitespace();
            if self.eat(",") {
                continue;
            }
            if self.eat("}") {
                return Ok(Value::Object(map));
            }
            return Err(self.syntax("expected ',' or '}'"));
        }
    }

    fn array(&mut self, path: &str, depth: usize) -> Result<Value, String> {
        self.at += 1;
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.eat("]") {
            return Ok(Value::Array(items));
        }
        loop {
            let item = self.value(&format!("{path}/{}", items.len()), Place::Other, depth + 1)?;
            items.push(item);
            self.skip_whitespace();
            if self.eat(",") {
                continue;
            }
            if self.eat("]") {
                return Ok(Value::Array(items));
            }
            return Err(self.syntax("expected ',' or ']'"));
        }
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let digits = self
            .bytes
            .get(self.at..self.at + 4)
            .and_then(|digits| std::str::from_utf8(digits).ok())
            .filter(|digits| digits.bytes().all(|b| b.is_ascii_hexdigit()))
            .ok_or_else(|| self.syntax("bad \\u escape"))?;
        let code = u32::from_str_radix(digits, 16).map_err(|_| self.syntax("bad \\u escape"))?;
        self.at += 4;
        Ok(code)
    }

    fn string(&mut self) -> Result<String, String> {
        self.at += 1;
        let mut out = String::new();
        loop {
            let start = self.at;
            while let Some(&b) = self.bytes.get(self.at) {
                if b == b'"' || b == b'\\' || b < 0x20 {
                    break;
                }
                self.at += 1;
            }
            out.push_str(
                std::str::from_utf8(&self.bytes[start..self.at])
                    .map_err(|_| self.syntax("invalid UTF-8"))?,
            );
            match self.bytes.get(self.at) {
                None => return Err(self.syntax("unterminated string")),
                Some(b'"') => {
                    self.at += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.at += 1;
                    let escaped = self.bytes.get(self.at).copied();
                    self.at += 1;
                    match escaped {
                        Some(b'"') => out.push('"'),
                        Some(b'\\') => out.push('\\'),
                        Some(b'/') => out.push('/'),
                        Some(b'b') => out.push('\u{8}'),
                        Some(b'f') => out.push('\u{c}'),
                        Some(b'n') => out.push('\n'),
                        Some(b'r') => out.push('\r'),
                        Some(b't') => out.push('\t'),
                        Some(b'u') => {
                            let high = self.hex4()?;
                            let code = if (0xD800..0xDC00).contains(&high) {
                                if !self.eat("\\u") {
                                    return Err(self.syntax("unpaired surrogate"));
                                }
                                let low = self.hex4()?;
                                if !(0xDC00..0xE000).contains(&low) {
                                    return Err(self.syntax("unpaired surrogate"));
                                }
                                0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00)
                            } else {
                                high
                            };
                            out.push(
                                char::from_u32(code)
                                    .ok_or_else(|| self.syntax("unpaired surrogate"))?,
                            );
                        }
                        _ => return Err(self.syntax("bad escape")),
                    }
                }
                Some(_) => return Err(self.syntax("control character in a string")),
            }
        }
    }

    fn number(&mut self, path: &str, place: Place) -> Result<Value, String> {
        let start = self.at;
        self.eat("-");
        let digits = |parser: &mut Self| {
            let from = parser.at;
            while parser.bytes.get(parser.at).is_some_and(u8::is_ascii_digit) {
                parser.at += 1;
            }
            parser.at - from
        };
        match self.bytes.get(self.at) {
            Some(b'0') => self.at += 1,
            Some(b'1'..=b'9') => {
                digits(self);
            }
            _ => return Err(self.syntax("bad number")),
        }
        let mut integral = true;
        if self.eat(".") {
            integral = false;
            if digits(self) == 0 {
                return Err(self.syntax("bad number"));
            }
        }
        if matches!(self.bytes.get(self.at), Some(b'e' | b'E')) {
            integral = false;
            self.at += 1;
            if matches!(self.bytes.get(self.at), Some(b'+' | b'-')) {
                self.at += 1;
            }
            if digits(self) == 0 {
                return Err(self.syntax("bad number"));
            }
        }
        // ASCII digits and signs only, so this slice is valid UTF-8.
        let literal = std::str::from_utf8(&self.bytes[start..self.at]).unwrap_or_default();
        if integral {
            return Ok(match literal.parse::<i64>() {
                Ok(int) => Value::from(int),
                Err(_) => {
                    self.refuse_number(if place == Place::LimitField {
                        refuse(
                            "policy_limit_out_of_bounds",
                            path,
                            "is outside the signed 64-bit range",
                        )
                    } else {
                        invalid(path, "is outside the signed 64-bit range")
                    });
                    Value::Null
                }
            });
        }
        let float: f64 = literal.parse().map_err(|_| self.syntax("bad number"))?;
        Ok(match serde_json::Number::from_f64(float) {
            Some(number) => Value::Number(number),
            None => self.non_finite(path),
        })
    }
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
    fn the_reader_matches_serde_on_ordinary_json() {
        let text = r#"{"a": [1, -7, 2.5, 1e2, "x\u00e9\ud83d\ude00\n", true, null], "b": {}}"#;
        assert_eq!(
            parse_policy_text(text).expect("parses"),
            serde_json::from_str::<Value>(text).expect("serde parses")
        );
        for bad in [
            "",
            "{",
            "[1,]",
            "01",
            "1.",
            "\"\u{1}\"",
            "{} x",
            r#""\ud800""#,
        ] {
            assert_eq!(
                parse_policy_text(bad).expect_err(bad).code,
                "policy_document_invalid"
            );
        }
    }

    #[test]
    fn negative_zero_is_the_integer_python_reads() {
        assert_eq!(parse_policy_text("-0").expect("parses"), json!(0));
    }

    #[test]
    fn numbers_compare_exactly() {
        let big = 9_007_199_254_740_993_i64; // 2^53 + 1
        assert!(!python_eq(&json!(big), &json!(9_007_199_254_740_992.0)));
        assert!(python_eq(&json!(big - 1), &json!(9_007_199_254_740_992.0)));
        assert_eq!(
            num_cmp(Num::Int(2), Num::Float(1.5)),
            Some(std::cmp::Ordering::Greater)
        );
        assert_eq!(
            num_cmp(Num::Float(-1.5), Num::Int(-1)),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn route_whitespace_is_pythons() {
        assert_eq!(python_strip("\u{1c}\u{1d} \u{1e}\u{1f}"), "");
        assert_eq!(python_strip(" oncall\t"), "oncall");
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
