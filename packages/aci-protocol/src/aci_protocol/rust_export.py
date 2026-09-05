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
from enum import Enum
from pathlib import Path
from typing import Any, Literal, Union, get_args, get_origin

from pydantic import BaseModel

from .events import (
    ErrorEvent,
    Event,
    Final,
    Interrupt,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
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
from .turn import QueuedTurn, ReplyHandle, TurnSource
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
}

# Multi-valued string literals map to a dedicated Rust enum. Only Event.type
# exists today; an unrecognized literal raises so the generator stays honest.
_EVENT_TYPE_ARGS = get_args(Event.model_fields["type"].annotation)

_ENUM_DERIVES = "#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]"
_STRUCT_DERIVES = "#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]"

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
        if field_name == WIRE_VERSION_FIELD:
            # The version field is mandatory and compatibility-checked on decode,
            # matching the NDJSON decoder. Detected by name (WIRE_VERSION_FIELD),
            # not by type, so dropping the old Literal does not silently remove
            # the guard and let #[serde(default)] make version optional.
            out.append('    #[serde(deserialize_with = "require_compatible_protocol_version")]')
        elif field_name == "adoption_credential":
            out.append(
                '    #[serde(default, deserialize_with = "deserialize_adoption_credential")]'
            )
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
    lines = [_STRUCT_DERIVES, f"pub struct {model.__name__} {{"]
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
    derives = (
        "#[derive(Clone, PartialEq, Serialize, Deserialize)]"
        if name == "InboundMessage"
        else _ENUM_DERIVES
    )
    lines = [
        derives,
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
    if name == "InboundMessage":
        lines.append("")
        lines.append(_INBOUND_DEBUG_IMPL.rstrip("\n"))
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


_ADOPTION_CREDENTIAL_DESERIALIZER = """fn deserialize_adoption_credential<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)?;
    match &value {
        None => Ok(None),
        Some(s) if s.trim().is_empty() || s.chars().count() > 4096 => {
            Err(serde::de::Error::custom("malformed adoption credential"))
        }
        Some(_) => Ok(value),
    }
}"""


_INBOUND_DEBUG_IMPL = """impl std::fmt::Debug for InboundMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Event {
                r#type,
                text,
                user,
                ts,
                session_id,
                history_ref,
                adoption_credential,
            } => f
                .debug_struct("Event")
                .field("type", r#type)
                .field("text", text)
                .field("user", user)
                .field("ts", ts)
                .field("session_id", session_id)
                .field("history_ref", history_ref)
                .field(
                    "adoption_credential",
                    &adoption_credential.as_ref().map(|_| "<redacted>"),
                )
                .finish(),
            Self::Interrupt { reason } => {
                f.debug_struct("Interrupt").field("reason", reason).finish()
            }
        }
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
            input_tokens: None,
            output_tokens: None,
            adoption_applied: None,
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
            input_tokens: None,
            output_tokens: None,
            adoption_applied: None,
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
            adoption_credential: None,
        };
        let encoded = serde_json::to_string(&message).unwrap();
        let decoded: InboundMessage = serde_json::from_str(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn inbound_event_debug_redacts_adoption_credential() {
        let message = InboundMessage::Event {
            r#type: EventType::Message,
            text: "hello".to_string(),
            user: "U1".to_string(),
            ts: "1.0".to_string(),
            session_id: None,
            history_ref: None,
            adoption_credential: Some("adoption-credential-fixture-PLACEHOLDER".to_string()),
        };
        let rendered = format!("{message:?}");
        assert!(!rendered.contains("adoption-credential-fixture-PLACEHOLDER"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn inbound_event_rejects_empty_adoption_credential() {
        let raw = concat!(
            r#"{"kind":"event","type":"message","text":"hi","user":"U1","ts":"1.0","#,
            r#""adoption_credential":""}"#
        );
        let error = serde_json::from_str::<InboundMessage>(raw).unwrap_err();
        assert!(error.to_string().contains("malformed adoption credential"));
        assert!(!error.to_string().contains("adoption-credential-fixture-PLACEHOLDER"));
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
        _ADOPTION_CREDENTIAL_DESERIALIZER,
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
        _struct(Budget),
        _struct(OtelConfig),
        _struct(SessionConfig),
        _struct(BootEnv),
        _env_keys_module(),
        _struct(ReplyHandle),
        _struct(QueuedTurn),
        _struct(EvalJob),
        _struct(EvalReport),
        _struct(ApprovalRequest),
        _tagged_enum("InboundMessage", "kind", (Event, Interrupt)),
        _tagged_enum(
            "OutboundEvent",
            "type",
            (TextDelta, ToolNote, Final, ErrorEvent, SideEffectFlag),
        ),
        _TESTS.rstrip("\n"),
        _version_tests().rstrip("\n"),
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
