"""Generate Rust serde types for the ACI protocol from the Pydantic models.

The Rust CLI (task I1) speaks the ACI over HTTP and NDJSON, so it needs types
that match the frozen contract exactly. Rather than depend on a JSON-Schema-to-
Rust toolchain, this module introspects the same Pydantic models the schema is
built from and emits idiomatic serde structs and internally tagged enums. Output
is deterministic, so the compat gate regenerates and diffs it; a model change
that is not reflected in the committed Rust fails the build.

Run as ``python -m aci_protocol.rust_export`` to rewrite the committed crate.
"""

import types as _types
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel

from .events import (
    AttachmentAccessCapability,
    ChannelReadCapability,
    ErrorEvent,
    Event,
    Final,
    Interrupt,
    PublicationContext,
    RetryCondition,
    RetryReason,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolAccess,
    ToolNote,
)
from .service_config import (
    EVAL_CONSUMER_GROUP_DEFAULT,
    EVAL_STREAM_DEFAULT,
    RUNS_STREAM_DEFAULT,
    STREAM_PAYLOAD_FIELD,
    WORKER_GROUP_DEFAULT,
)
from .session import BootEnv, Budget, OtelConfig, SessionConfig
from .turn import Attachment, HookRunRef, PrincipalRef, QueuedTurn, ReplyHandle, TurnSource
from .version import PROTOCOL_VERSION, WIRE_VERSION_FIELD
from .wire import ApprovalRequest, EvalJob, EvalReport, GateKind

_NONE = type(None)
# uuid.UUID maps to String: Pydantic emits {"type":"string","format":"uuid"} and
# the TypeScript lane gets `string`, so String is the coherent Rust counterpart.
# The generator has no crate-import machinery, so the Rust lane does not get a
# typed Uuid today -- String is the honest MVP, and the Python side keeps the
# real uuid.UUID (downgrading it there would be a genuine loosening).
_SCALARS: dict[type, str] = {
    str: "String",
    int: "i64",
    float: "f64",
    bool: "bool",
    uuid.UUID: "String",
    datetime: "String",
}

# Multi-valued string literals map to a dedicated Rust enum. Only Event.type
# exists today; an unrecognized literal raises so the generator stays honest.
_EVENT_TYPE_ARGS = get_args(Event.model_fields["type"].annotation)

_ENUM_DERIVES = "#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]"
_STRUCT_DERIVES = "#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]"
# A struct with a required enum member has no honest Default, so it derives
# everything else. Such a struct is only referenced as an Option, whose default
# is None.
_NO_DEFAULT_STRUCT_DERIVES = _ENUM_DERIVES
_NO_DEFAULT_STRUCTS: tuple[type[BaseModel], ...] = (RetryCondition,)

# Field-level deserializers keyed by (model, field name). Each one is a guard
# from _RETRY_CONDITION_GUARD that refuses a present value the Python model
# also refuses, so a grammar the Rust types cannot express still holds on the
# Rust read path. Every mapped field is optional, so it also carries
# serde(default) and a missing key reads as None.
_FIELD_DESERIALIZERS: dict[tuple[type[BaseModel], str], str] = {
    (RetryCondition, "retry_at"): "deserialize_optional_utc_retry_at",
    (RetryCondition, "scope_key"): "deserialize_optional_scope_key",
}

# Rust keywords that a field name may collide with, requiring a raw identifier.
_RUST_KEYWORDS = {
    "type",
    "match",
    "move",
    "ref",
    "self",
    "impl",
    "fn",
    "use",
    "mod",
    "as",
    "let",
    "loop",
    "enum",
    "struct",
    "trait",
    "crate",
    "super",
    "in",
    "box",
    "dyn",
    "async",
    "await",
}


def _rust_field(name: str) -> str:
    return f"r#{name}" if name in _RUST_KEYWORDS else name


def crate_dir() -> Path:
    """The committed generated Rust crate directory inside this package."""

    return Path(__file__).resolve().parents[2] / "generated" / "rust"


def _pascal(token: str) -> str:
    return "".join(part.capitalize() for part in token.replace("-", "_").split("_"))


def _split_optional(annotation: Any) -> tuple[Any, bool]:
    origin = get_origin(annotation)
    if origin is Union or origin is _types.UnionType:
        args = [a for a in get_args(annotation) if a is not _NONE]
        if len(args) != 1:
            raise TypeError(f"only Optional[T] unions are supported, got {annotation!r}")
        return args[0], True
    return annotation, False


def _rust_type(annotation: Any) -> str:
    inner, optional = _split_optional(annotation)
    rust = _rust_bare_type(inner)
    return f"Option<{rust}>" if optional else rust


def _rust_bare_type(annotation: Any) -> str:
    if get_origin(annotation) is Annotated:
        # Annotated metadata (a schema pattern, say) carries no Rust type of its
        # own; the guard, if any, comes from _FIELD_DESERIALIZERS.
        return _rust_bare_type(get_args(annotation)[0])
    if annotation in _SCALARS:
        return _SCALARS[annotation]
    origin = get_origin(annotation)
    if origin is list:
        return f"Vec<{_rust_type(get_args(annotation)[0])}>"
    if origin is Literal:
        args = get_args(annotation)
        if args == _EVENT_TYPE_ARGS:
            return "EventType"
        if len(args) == 1 and isinstance(args[0], str):
            # A single-valued string literal maps to a plain Rust String. The
            # version field is no longer a Literal, so this branch is a defensive
            # fallback for any future single-valued literal field.
            return "String"
        raise TypeError(f"unexpected literal field {annotation!r}")
    if origin is dict:
        key, value = get_args(annotation)
        if key is not str or value is not Any:
            # Anything narrower than the fully open shape deserves a named type
            # on both sides rather than a free-form map, so it is not mapped
            # here: the schema would claim a structure the Rust would not hold.
            raise TypeError(f"only dict[str, Any] maps to a Rust map, got {annotation!r}")
        # The crate already depends on serde_json, and a Map preserves the
        # object-ness the JSON Schema declares -- a bare Value would also accept
        # a string or a number, widening the contract on the Rust side only.
        return "serde_json::Map<String, serde_json::Value>"
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation.__name__
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        return annotation.__name__
    raise TypeError(f"no Rust mapping for {annotation!r}")


def _string_enum(name: str, values: tuple[str, ...], default: str | None = None) -> str:
    # Derive Default (with a #[default] variant) only when a defaulted field
    # references this enum, so serde(default) on that field compiles.
    derives = _STRUCT_DERIVES if default is not None else _ENUM_DERIVES
    lines = [derives, f"pub enum {name} {{"]
    for value in values:
        lines.append(f'    #[serde(rename = "{value}")]')
        if value == default:
            lines.append("    #[default]")
        lines.append(f"    {_pascal(value)},")
    lines.append("}")
    return "\n".join(lines)


def _struct_fields(model: type[BaseModel], skip: set[str], public: bool) -> list[str]:
    out: list[str] = []
    prefix = "pub " if public else ""
    for field_name, field in model.model_fields.items():
        if field_name in skip:
            continue
        rust = _rust_type(field.annotation)
        _, nullable = _split_optional(field.annotation)
        deserializer = _FIELD_DESERIALIZERS.get((model, field_name))
        if deserializer is not None:
            if field.is_required():
                raise TypeError(f"field deserializers support optional fields only: {field_name}")
            out.append(f'    #[serde(default, deserialize_with = "{deserializer}")]')
        elif field_name == WIRE_VERSION_FIELD:
            # The version field is mandatory and compatibility-checked on decode,
            # matching the NDJSON decoder. Detected by name (WIRE_VERSION_FIELD),
            # not by type, so dropping the old Literal does not silently remove
            # the guard and let #[serde(default)] make version optional.
            out.append('    #[serde(deserialize_with = "require_compatible_protocol_version")]')
        elif field.is_required() and nullable:
            out.append('    #[serde(deserialize_with = "deserialize_required_nullable")]')
        elif not field.is_required():
            # Any other field with a Pydantic default is omittable on the wire,
            # so Rust accepts it missing too.
            out.append("    #[serde(default)]")
        out.append(f"    {prefix}{_rust_field(field_name)}: {rust},")
    return out


def _struct(model: type[BaseModel]) -> str:
    # No deny_unknown_fields: the reader path is deliberately tolerant of unknown
    # fields (strict producers, tolerant consumers). A Rust producer stays strict
    # by construction -- a struct cannot serialize a field it does not have -- so
    # dropping it only loosens the read path we mean to loosen.
    if model in (ChannelReadCapability, AttachmentAccessCapability):
        derives = "#[derive(Clone, PartialEq, Default, Serialize, Deserialize)]"
    elif model in _NO_DEFAULT_STRUCTS:
        derives = _NO_DEFAULT_STRUCT_DERIVES
    else:
        derives = _STRUCT_DERIVES
    lines = [derives, f"pub struct {model.__name__} {{"]
    lines.extend(_struct_fields(model, skip=set(), public=True))
    lines.append("}")
    return "\n".join(lines)


def _env_keys_module() -> str:
    """Emit the boot-env key constants so no lane retypes the literals.

    Sorted by ``BootEnv.env_keys()`` and emitted verbatim: an env key is already
    a valid SCREAMING_CASE Rust identifier, so it is deliberately NOT routed
    through ``_pascal``/``_RUST_KEYWORDS`` (both are for field names, and either
    would mangle the constant away from the key it names). Sorting is what keeps
    regeneration byte-identical, so the ``git diff --exit-code`` drift gate
    cannot flap on field-declaration order.
    """

    lines = [
        "/// Boot-env variable names, generated from aci_protocol.session.BootEnv.",
        "/// The env key is the contract; the Rust CLI and the chart render-assert",
        "/// pin against these instead of retyping the literals.",
        "pub mod env_keys {",
    ]
    for key in BootEnv.env_keys():
        lines.append(f'    pub const {key}: &str = "{key}";')
    lines.append("}")
    return "\n".join(lines)


def _tagged_enum(name: str, tag: str, variants: tuple[type[BaseModel], ...]) -> str:
    lines = [
        _ENUM_DERIVES,
        f'#[serde(tag = "{tag}")]',
        f"pub enum {name} {{",
    ]
    for model in variants:
        tag_value = get_args(model.model_fields[tag].annotation)[0]
        lines.append(f'    #[serde(rename = "{tag_value}")]')
        lines.append(f"    {model.__name__} {{")
        for field_line in _struct_fields(model, skip={tag}, public=False):
            lines.append(f"    {field_line}")
        lines.append("    },")
    lines.append("}")
    return "\n".join(lines)


_VERSION_GUARD = """fn parse_semver(value: &str) -> Option<(u64, u64)> {
    let mut parts = value.split('.');
    let major = parts.next()?.parse::<u64>().ok()?;
    let minor = parts.next()?.parse::<u64>().ok()?;
    let patch = parts.next()?;
    if patch.is_empty() || !patch.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if parts.next().is_some() {
        return None;
    }
    Some((major, minor))
}

fn is_compatible_protocol_version(wire: &str) -> bool {
    let (w_major, w_minor) = match parse_semver(wire) {
        Some(parsed) => parsed,
        None => return false,
    };
    let (b_major, b_minor) = match parse_semver(PROTOCOL_VERSION) {
        Some(parsed) => parsed,
        None => return false,
    };
    if b_major == 0 {
        w_major == 0 && w_minor == b_minor
    } else {
        w_major == b_major
    }
}

fn require_compatible_protocol_version<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    if !is_compatible_protocol_version(&value) {
        return Err(serde::de::Error::custom(format!(
            "unsupported protocol version {value:?}; this build speaks {PROTOCOL_VERSION:?}"
        )));
    }
    Ok(value)
}"""


_REQUIRED_NULLABLE_DESERIALIZER = """fn deserialize_required_nullable<'de, D, T>(
    deserializer: D,
) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}"""


# The Rust half of RetryCondition's wire grammar. Rust has no pattern type, so
# these hand-written guards back the field deserializers in _FIELD_DESERIALIZERS
# and refuse exactly what the Python validators refuse: the single RFC 3339 UTC
# spelling with a real Gregorian date and time, and the sha256 digest form.
# Their refusal messages name the rule and never quote the value.
_RETRY_CONDITION_GUARD = """fn retry_at_number(digits: &[u8]) -> Option<u32> {
    digits.iter().try_fold(0u32, |total, byte| {
        if byte.is_ascii_digit() {
            Some(total * 10 + u32::from(byte - b'0'))
        } else {
            None
        }
    })
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        _ => 0,
    }
}

/// Whether `value` is the one spelling the ACI accepts for
/// `RetryCondition.retry_at`: `YYYY-MM-DDTHH:MM:SS`, an optional dot with one
/// to six fraction digits, then `Z`, naming a real Gregorian date and time.
fn is_utc_retry_at(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() < 20
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
    {
        return false;
    }
    let fraction_ok = match &bytes[19..] {
        [b'Z'] => true,
        [b'.', digits @ .., b'Z'] => {
            (1..=6).contains(&digits.len()) && digits.iter().all(u8::is_ascii_digit)
        }
        _ => false,
    };
    if !fraction_ok {
        return false;
    }
    let (Some(year), Some(month), Some(day), Some(hour), Some(minute), Some(second)) = (
        retry_at_number(&bytes[0..4]),
        retry_at_number(&bytes[5..7]),
        retry_at_number(&bytes[8..10]),
        retry_at_number(&bytes[11..13]),
        retry_at_number(&bytes[14..16]),
        retry_at_number(&bytes[17..19]),
    ) else {
        return false;
    };
    year >= 1
        && (1..=12).contains(&month)
        && (1..=days_in_month(year, month)).contains(&day)
        && hour < 24
        && minute < 60
        && second < 60
}

/// Whether `value` is `sha256:` followed by exactly 64 lowercase hex digits,
/// the one spelling the ACI accepts for `RetryCondition.scope_key`.
fn is_retry_scope_key(value: &str) -> bool {
    match value.strip_prefix("sha256:") {
        Some(digest) => {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        }
        None => false,
    }
}

fn deserialize_optional_utc_retry_at<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Ok(value) = Option::<String>::deserialize(deserializer) else {
        return Err(serde::de::Error::custom("retry_at must be a string or null"));
    };
    match value {
        Some(text) if !is_utc_retry_at(&text) => Err(serde::de::Error::custom(
            "retry_at must be an RFC 3339 UTC timestamp spelled YYYY-MM-DDTHH:MM:SS[.ffffff]Z",
        )),
        other => Ok(other),
    }
}

fn deserialize_optional_scope_key<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let Ok(value) = Option::<String>::deserialize(deserializer) else {
        return Err(serde::de::Error::custom("scope_key must be a string or null"));
    };
    match value {
        Some(text) if !is_retry_scope_key(&text) => Err(serde::de::Error::custom(
            "scope_key must be sha256: followed by 64 lowercase hex digits",
        )),
        other => Ok(other),
    }
}"""


_CHANNEL_READ_DEBUG = """impl std::fmt::Debug for ChannelReadCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChannelReadCapability")
            .field("url", &self.url)
            .field("token", &"[REDACTED]")
            .finish()
    }
}"""


_ATTACHMENT_ACCESS_TESTS = """#[cfg(test)]
mod attachment_access_tests {
    use super::*;

    #[test]
    fn optional_attachment_context_roundtrips_without_debug_disclosure() {
        let mut frame = serde_json::json!({
            "kind": "event", "type": "message", "text": "Inspect the file",
            "user": "U0EXAMPLE1", "ts": "1.0"
        });
        let old: InboundMessage = serde_json::from_value(frame.clone()).unwrap();
        assert!(matches!(old, InboundMessage::Event { attachment_access: None, .. }));
        frame["attachment_access"] = serde_json::json!({
            "url": "https://api.example.com/attachment-access",
            "token": "attachment.example.signature-sentinel"
        });
        let current: InboundMessage = serde_json::from_value(frame.clone()).unwrap();
        let output = serde_json::to_value(&current).unwrap();
        assert_eq!(output["attachment_access"], frame["attachment_access"]);
        assert!(!format!("{current:?}").contains("signature-sentinel"));
        frame["attachment_access"] = serde_json::json!({"url": "https://api.example.com"});
        assert!(serde_json::from_value::<InboundMessage>(frame).is_err());
    }
}"""


# The cross-lane retry-condition vector tests. The vector lives at the repo root
# so the Python decoder, this crate and the generated TypeScript consumer read
# one file; the include path is relative to generated/rust/src/lib.rs.
_RETRY_CONDITION_TESTS = """#[cfg(test)]
mod retry_condition_tests {
    use super::*;

    // Frames in the shared vector omit `version`; each lane stamps its own
    // PROTOCOL_VERSION so the version gate stays the only version check.
    const VECTOR: &str =
        include_str!("../../../../../tests/vectors/aci-error-retry-condition.json");

    fn check_keys(
        value: &serde_json::Value,
        required: &[&str],
        optional: &[&str],
    ) -> Result<(), String> {
        let object = value
            .as_object()
            .ok_or("vector entries must be JSON objects")?;
        if let Some(key) = object
            .keys()
            .find(|key| !required.contains(&key.as_str()) && !optional.contains(&key.as_str()))
        {
            return Err(format!("unknown vector key {key}"));
        }
        if let Some(key) = required.iter().find(|key| !object.contains_key(**key)) {
            return Err(format!("missing vector key {key}"));
        }
        Ok(())
    }

    fn case_list<'a>(
        vector: &'a serde_json::Value,
        list: &str,
    ) -> Result<&'a [serde_json::Value], String> {
        match vector[list].as_array() {
            Some(cases) => Ok(cases.as_slice()),
            None => Err(format!("vector list {list} must be an array")),
        }
    }

    // Fails closed: a key one lane cannot see must not pass vacuously.
    fn checked(vector: &serde_json::Value) -> Result<(), String> {
        check_keys(vector, &["comment", "accept", "refuse"], &[])?;
        for case in case_list(vector, "accept")? {
            check_keys(
                case,
                &["name", "frame", "expected_retry_condition"],
                &["strict_construction"],
            )?;
        }
        for case in case_list(vector, "refuse")? {
            check_keys(case, &["name", "frame", "violation"], &[])?;
        }
        Ok(())
    }

    fn load(text: &str) -> Result<serde_json::Value, String> {
        let vector: serde_json::Value =
            serde_json::from_str(text).map_err(|error| error.to_string())?;
        checked(&vector)?;
        Ok(vector)
    }

    fn shared_vector() -> serde_json::Value {
        load(VECTOR).unwrap_or_else(|error| panic!("the shared vector is malformed: {error}"))
    }

    fn case_name(case: &serde_json::Value) -> &str {
        case["name"].as_str().expect("vector case names are strings")
    }

    fn stamped(case: &serde_json::Value) -> serde_json::Value {
        let mut frame = case["frame"].clone();
        frame["version"] = serde_json::Value::from(PROTOCOL_VERSION);
        frame
    }

    #[test]
    fn vector_loader_refuses_unknown_and_missing_keys() {
        assert!(load(VECTOR).is_ok());
        let valid = serde_json::json!({
            "comment": "",
            "accept": [{"name": "x", "frame": {}, "expected_retry_condition": null}],
            "refuse": [{"name": "y", "frame": {}, "violation": "type"}]
        });
        assert!(checked(&valid).is_ok());
        let mutations: [fn(&mut serde_json::Value); 5] = [
            |vector| vector["extra"] = serde_json::Value::from(1),
            |vector| vector["accept"][0]["extra"] = serde_json::Value::from(1),
            |vector| vector["refuse"][0]["extra"] = serde_json::Value::from(1),
            |vector| {
                vector["accept"][0]
                    .as_object_mut()
                    .expect("vector cases are JSON objects")
                    .remove("expected_retry_condition");
            },
            |vector| {
                vector
                    .as_object_mut()
                    .expect("the vector is a JSON object")
                    .remove("comment");
            },
        ];
        for mutate in mutations {
            let mut vector = valid.clone();
            mutate(&mut vector);
            assert!(checked(&vector).is_err(), "{vector}");
        }
    }

    #[test]
    fn accepted_frames_decode_to_the_expected_retry_condition() {
        let vector = shared_vector();
        let accepted = case_list(&vector, "accept").unwrap();
        assert!(!accepted.is_empty());
        for case in accepted {
            let name = case_name(case);
            if let Some(mode) = case.get("strict_construction") {
                assert_eq!(mode.as_str(), Some("refuse"), "{name}");
            }
            let event: OutboundEvent = serde_json::from_value(stamped(case))
                .unwrap_or_else(|error| panic!("{name} must decode: {error}"));
            let OutboundEvent::ErrorEvent {
                retry_condition, ..
            } = &event
            else {
                panic!("{name} must decode as an error event");
            };
            assert_eq!(
                serde_json::to_value(retry_condition).unwrap(),
                case["expected_retry_condition"],
                "{name}"
            );
            let encoded = serde_json::to_string(&event).unwrap();
            let decoded: OutboundEvent = serde_json::from_str(&encoded).unwrap();
            assert_eq!(event, decoded, "{name}");
        }
    }

    #[test]
    fn refused_frames_fail_to_decode_on_their_carrier() {
        let vector = shared_vector();
        let refused = case_list(&vector, "refuse").unwrap();
        assert!(!refused.is_empty());
        for case in refused {
            let name = case_name(case);
            assert!(
                matches!(case["violation"].as_str(), Some("type" | "grammar")),
                "{name}"
            );
            let error = serde_json::from_value::<OutboundEvent>(stamped(case))
                .expect_err(name)
                .to_string();
            for member in ["retry_at", "scope_key"] {
                let carrier = &case["frame"]["retry_condition"];
                if let Some(text) = carrier.get(member).and_then(serde_json::Value::as_str) {
                    assert!(!error.contains(text), "{name} must not quote the value");
                }
            }
            let mut without_carrier = stamped(case);
            without_carrier
                .as_object_mut()
                .expect("vector frames are JSON objects")
                .remove("retry_condition");
            assert!(
                serde_json::from_value::<OutboundEvent>(without_carrier).is_ok(),
                "{name} must fail only on its retry condition"
            );
        }
    }

    #[test]
    fn previous_error_reader_ignores_the_retry_condition() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct PreviousErrorEvent {
            // A previous reader gated its version exactly as the current one does.
            #[serde(deserialize_with = "require_compatible_protocol_version")]
            version: String,
            message: String,
            #[serde(default)]
            classification: Option<String>,
        }
        let vector = shared_vector();
        let complete = case_list(&vector, "accept")
            .unwrap()
            .iter()
            .find(|case| case["name"] == "complete_usage_reset")
            .expect("the vector carries a complete retry condition");
        let with_carrier = stamped(complete);
        assert!(with_carrier["retry_condition"].is_object());
        let mut without_carrier = with_carrier.clone();
        without_carrier
            .as_object_mut()
            .expect("vector frames are JSON objects")
            .remove("retry_condition");
        let before: PreviousErrorEvent = serde_json::from_value(without_carrier).unwrap();
        let after: PreviousErrorEvent = serde_json::from_value(with_carrier.clone()).unwrap();
        assert_eq!(before, after);
        let current: OutboundEvent = serde_json::from_value(with_carrier).unwrap();
        assert!(matches!(
            current,
            OutboundEvent::ErrorEvent {
                retry_condition: Some(_),
                ..
            }
        ));
    }
}"""


_TESTS = """#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_event_roundtrips() {
        let event = OutboundEvent::Final {
            version: PROTOCOL_VERSION.to_string(),
            text: "hi".to_string(),
            status: SessionStatus::Done,
            approval_summary: None,
            approval_route: None,
            approval_gate_kind: None,
            approval_granted_tool: None,
            approval_granted_arguments: None,
            approval_display: None,
            input_tokens: None,
            output_tokens: None,
            text_mode: None,
        };
        let encoded = serde_json::to_string(&event).unwrap();
        let decoded: OutboundEvent = serde_json::from_str(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn awaiting_approval_final_roundtrips() {
        let event = OutboundEvent::Final {
            version: PROTOCOL_VERSION.to_string(),
            text: "requesting sign-off".to_string(),
            status: SessionStatus::AwaitingApproval,
            approval_summary: Some("Give ACME a 20% discount".to_string()),
            approval_route: Some("managers".to_string()),
            approval_gate_kind: Some("policy".to_string()),
            approval_granted_tool: None,
            approval_granted_arguments: None,
            approval_display: None,
            input_tokens: None,
            output_tokens: None,
            text_mode: None,
        };
        let encoded = serde_json::to_string(&event).unwrap();
        let decoded: OutboundEvent = serde_json::from_str(&encoded).unwrap();
        assert_eq!(event, decoded);
    }

    #[test]
    fn inbound_event_roundtrips() {
        let message = InboundMessage::Event {
            r#type: EventType::Message,
            text: "hello".to_string(),
            user: "U1".to_string(),
            ts: "1.0".to_string(),
            session_id: None,
            history_ref: None,
            publication_context: None,
            // TOOL-ACCESS-1: the enum's wire spelling round-trips too.
            tool_access: Some(ToolAccess::ReadOnly),
            memory_token: None,
            channel_read: None,
            attachment_access: None,
        };
        let encoded = serde_json::to_string(&message).unwrap();
        assert!(encoded.contains(r#""tool_access":"read-only""#));
        // TOOL-ACCESS-2: an unknown value is refused, never read as None.
        let unknown = encoded.replace(r#""read-only""#, r#""read-mostly""#);
        assert!(serde_json::from_str::<InboundMessage>(&unknown).is_err());
        let decoded: InboundMessage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn channel_read_roundtrips_without_debug_disclosure() {
        let token = "channel.read.example.signature-sentinel";
        let capability = ChannelReadCapability {
            url: "https://api.example.com/channel-read".to_string(),
            token: token.to_string(),
        };
        for rendered in [format!("{:?}", capability), format!("{:#?}", capability)] {
            assert!(!rendered.contains(token));
            assert!(rendered.contains("[REDACTED]"));
        }
        let message = InboundMessage::Event {
            r#type: EventType::Message,
            text: "hello".to_string(),
            user: "U0EXAMPLE1".to_string(),
            ts: "1.0".to_string(),
            session_id: None,
            history_ref: None,
            publication_context: None,
            tool_access: None,
            memory_token: None,
            channel_read: Some(capability),
            attachment_access: None,
        };
        for rendered in [format!("{:?}", message), format!("{:#?}", message)] {
            assert!(!rendered.contains(token));
            assert!(rendered.contains("[REDACTED]"));
        }
        let encoded = serde_json::to_string(&message).unwrap();
        assert!(encoded.contains(token));
        let decoded: InboundMessage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn channel_read_requires_both_children() {
        for capability in [
            serde_json::json!({"url": "https://api.example.com/channel-read"}),
            serde_json::json!({"token": "channel.read.example.signature-sentinel"}),
            serde_json::json!({"url": null, "token": "example-token"}),
            serde_json::json!({"url": "https://api.example.com/channel-read", "token": null}),
        ] {
            let frame = serde_json::json!({
                "kind": "event", "type": "message", "text": "hello",
                "user": "U0EXAMPLE1", "ts": "1.0", "channel_read": capability,
            });
            assert!(serde_json::from_value::<InboundMessage>(frame).is_err());
        }
    }

    #[test]
    fn channel_read_remains_optional_for_existing_events() {
        let mut frame = serde_json::json!({
            "kind": "event", "type": "message", "text": "hello",
            "user": "U0EXAMPLE1", "ts": "1.0",
        });
        let omitted: InboundMessage = serde_json::from_value(frame.clone()).unwrap();
        assert!(matches!(omitted, InboundMessage::Event { channel_read: None, .. }));
        frame["channel_read"] = serde_json::Value::Null;
        let explicit_null: InboundMessage = serde_json::from_value(frame).unwrap();
        assert_eq!(omitted, explicit_null);
    }

    #[test]
    fn previous_event_reader_ignores_the_new_channel_read_object() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct PreviousEvent {
            kind: String,
            r#type: EventType,
            text: String,
            user: String,
            ts: String,
            #[serde(default)]
            session_id: Option<String>,
            #[serde(default)]
            history_ref: Option<String>,
            #[serde(default)]
            publication_context: Option<PublicationContext>,
            #[serde(default)]
            tool_access: Option<ToolAccess>,
            #[serde(default)]
            memory_token: Option<String>,
        }
        let mut frame = serde_json::json!({
            "kind": "event", "type": "message", "text": "hello",
            "user": "U0EXAMPLE1", "ts": "1.0", "session_id": "example-session",
            "history_ref": "https://api.example.com/history",
            "tool_access": "read-only",
        });
        let before: PreviousEvent = serde_json::from_value(frame.clone()).unwrap();
        frame["channel_read"] = serde_json::json!({
            "url": "https://api.example.com/channel-read",
            "token": "channel.read.example.signature-sentinel",
            "future_nested_field": true,
        });
        let after: PreviousEvent = serde_json::from_value(frame.clone()).unwrap();
        assert_eq!(before, after);
        let current: InboundMessage = serde_json::from_value(frame).unwrap();
        assert!(matches!(current, InboundMessage::Event { channel_read: Some(_), .. }));
    }

    #[test]
    fn reply_handle_accepts_explicit_null_placeholder() {
        let raw = r#"{"kind":"email","channel":"agent@example.test","placeholder":null,"endpoint":"https://adapter.example/hook","adapter":"agentmail_sandbox"}"#;
        let decoded: ReplyHandle = serde_json::from_str(raw).unwrap();
        assert_eq!(decoded.kind, "email");
        assert_eq!(decoded.channel, "agent@example.test");
        assert_eq!(decoded.placeholder, None);
        assert_eq!(decoded.endpoint.as_deref(), Some("https://adapter.example/hook"));
        assert_eq!(decoded.adapter.as_deref(), Some("agentmail_sandbox"));
    }

    #[test]
    fn reply_handle_rejects_omitted_placeholder() {
        let raw = r#"{"kind":"email","channel":"agent@example.test"}"#;
        let error = serde_json::from_str::<ReplyHandle>(raw).unwrap_err();
        assert!(error.to_string().contains("missing field `placeholder`"));
    }

"""

# The version-gate tests, kept as a template whose fixture versions are derived
# from PROTOCOL_VERSION at render time. Hardcoded literals here silently invert
# on every version bump -- a fixture written as "an incompatible minor" becomes
# the current version, and the test then asserts the opposite of its own name.
# The placeholders are substituted, not f-string interpolated, so the Rust stays
# readable (it is dense with braces).
_VERSION_TESTS = """    #[test]
    fn rejects_incompatible_version_event() {
        let raw = r#"{"type":"final","version":"@INCOMPATIBLE@","text":"x","status":"done"}"#;
        assert!(serde_json::from_str::<OutboundEvent>(raw).is_err());
    }

    #[test]
    fn rejects_incompatible_near_version() {
        let raw = r#"{"type":"final","version":"@INCOMPATIBLE_NEAR@","text":"x","status":"done"}"#;
        assert!(serde_json::from_str::<OutboundEvent>(raw).is_err());
    }

    #[test]
    fn accepts_compatible_patch() {
        let raw = r#"{"type":"final","version":"@COMPATIBLE_PATCH@","text":"x","status":"done"}"#;
        assert!(serde_json::from_str::<OutboundEvent>(raw).is_ok());
    }

    #[test]
    fn accepts_unknown_fields() {
        let raw = r#"{"type":"final","version":"@CURRENT@","text":"x","status":"done","extra":1}"#;
        assert!(serde_json::from_str::<OutboundEvent>(raw).is_ok());
    }
}
"""


def _version_tests() -> str:
    """Render the version-gate tests with fixtures derived from PROTOCOL_VERSION.

    Compatibility is same ``major.minor`` under 0.x (same ``major`` from 1.0 on),
    so the compatible fixture differs only in the patch component. The near-miss
    incompatible fixture follows the same split: under 0.x a minor bump is the
    breaking axis, so it is ``{major}.{minor + 1}.0``; from 1.0 on a minor bump
    is compatible and only a major bump breaks, so it is ``{major + 1}.0.0``.
    ``9.9.9`` stands in for a version from a wholly different line.
    Deterministic: every fixture is a pure function of PROTOCOL_VERSION.
    """

    major, minor, patch = (int(part) for part in PROTOCOL_VERSION.split("."))
    near_incompatible = f"{major}.{minor + 1}.0" if major == 0 else f"{major + 1}.0.0"
    return (
        _VERSION_TESTS.replace("@INCOMPATIBLE@", "9.9.9")
        .replace("@INCOMPATIBLE_NEAR@", near_incompatible)
        .replace("@COMPATIBLE_PATCH@", f"{major}.{minor}.{patch + 1}")
        .replace("@CURRENT@", PROTOCOL_VERSION)
    )


def render_rust() -> str:
    """Render the full generated lib.rs as a deterministic string."""

    blocks = [
        "// GENERATED by aci_protocol.rust_export. Do not edit by hand.",
        "// Regenerate with: python -m aci_protocol.rust_export",
        "#![allow(dead_code)]",
        "use serde::{Deserialize, Serialize};",
        f'pub const PROTOCOL_VERSION: &str = "{PROTOCOL_VERSION}";',
        # The shared transport literals (#492), derived from the Python constants
        # so the two cannot drift. Not wire models: they are not in the exported
        # JSON Schema and do not move the wire fingerprint.
        f'pub const RUNS_STREAM_DEFAULT: &str = "{RUNS_STREAM_DEFAULT}";',
        f'pub const WORKER_GROUP_DEFAULT: &str = "{WORKER_GROUP_DEFAULT}";',
        f'pub const EVAL_STREAM_DEFAULT: &str = "{EVAL_STREAM_DEFAULT}";',
        f'pub const EVAL_CONSUMER_GROUP_DEFAULT: &str = "{EVAL_CONSUMER_GROUP_DEFAULT}";',
        f'pub const STREAM_PAYLOAD_FIELD: &str = "{STREAM_PAYLOAD_FIELD}";',
        _VERSION_GUARD,
        _REQUIRED_NULLABLE_DESERIALIZER,
        _RETRY_CONDITION_GUARD,
        _string_enum(
            "SessionStatus",
            tuple(m.value for m in SessionStatus),
            default=SessionStatus.DONE.value,
        ),
        _string_enum("EventType", _EVENT_TYPE_ARGS),
        # No default variant: GateKind is only referenced as Option<GateKind>,
        # which is Default regardless of the enum's own derives.
        _string_enum("GateKind", tuple(m.value for m in GateKind)),
        # QueuedTurn.source carries a serde(default), so this enum needs a Default
        # variant for that attribute to compile. SLACK is the right one: it is the
        # same non-job value the Python model defaults to, so a pre-upgrade payload
        # decodes identically in both lanes.
        _string_enum(
            "TurnSource",
            tuple(m.value for m in TurnSource),
            default=TurnSource.SLACK.value,
        ),
        # No default variant: ToolAccess is only referenced as Option<ToolAccess>
        # (QueuedTurn.tool_access, Event.tool_access), whose default is None.
        _string_enum("ToolAccess", tuple(m.value for m in ToolAccess)),
        _struct(Budget),
        _struct(OtelConfig),
        _struct(SessionConfig),
        _struct(BootEnv),
        _env_keys_module(),
        _struct(ReplyHandle),
        _struct(Attachment),
        _struct(HookRunRef),
        _struct(PrincipalRef),
        _struct(QueuedTurn),
        _struct(EvalJob),
        _struct(EvalReport),
        _struct(ApprovalRequest),
        _struct(PublicationContext),
        _struct(ChannelReadCapability),
        _CHANNEL_READ_DEBUG,
        _struct(AttachmentAccessCapability),
        _CHANNEL_READ_DEBUG.replace("ChannelReadCapability", "AttachmentAccessCapability"),
        _tagged_enum("InboundMessage", "kind", (Event, Interrupt)),
        # No default variant: RetryReason is a required member of RetryCondition,
        # which is itself only referenced as Option<RetryCondition>.
        _string_enum("RetryReason", tuple(m.value for m in RetryReason)),
        _struct(RetryCondition),
        _tagged_enum(
            "OutboundEvent",
            "type",
            (TextDelta, ToolNote, Final, ErrorEvent, SideEffectFlag),
        ),
        _TESTS.rstrip("\n"),
        _ATTACHMENT_ACCESS_TESTS,
        _version_tests().rstrip("\n"),
        _RETRY_CONDITION_TESTS,
    ]
    return "\n\n".join(blocks) + "\n"


def write_rust() -> Path:
    """Write the generated lib.rs to the committed crate and return its path."""

    path = crate_dir() / "src" / "lib.rs"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_rust(), encoding="utf-8")
    return path


if __name__ == "__main__":
    written = write_rust()
    print(f"wrote {written}")
