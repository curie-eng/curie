"""The optional typed retry condition on the ACI error frame (#5425).

``tests/vectors/aci-error-retry-condition.json`` is the cross-lane contract.
This file reads it through the production decoder (``parse_ndjson_line``),
strict producer construction (``ErrorEvent.model_validate`` without a reader
context) and the NDJSON encoder (``to_ndjson_line``); the generated Rust crate
and the generated TypeScript consumer read the same file. The remaining tests
pin what the vector cannot express: Python object construction, existing and
older readers, the version gate and the exported schema.
"""

import hashlib
import json
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    ErrorEvent,
    ProtocolVersionError,
    RetryCondition,
    RetryReason,
    is_compatible,
    parse_ndjson_line,
    to_ndjson_line,
)
from aci_protocol.events import (
    READER_CONTEXT,
    RETRY_AT_PATTERN,
    RETRY_SCOPE_KEY_PATTERN,
    _OutboundBase,
)
from aci_protocol.schema_export import build_schema
from aci_protocol.ts_vector_export import (
    RetryConditionVectorError,
    load_retry_condition_vector,
    vector_path,
)
from pydantic import ValidationError

_VECTOR = load_retry_condition_vector()
_ACCEPT: list[dict[str, Any]] = _VECTOR["accept"]
_REFUSE: list[dict[str, Any]] = _VECTOR["refuse"]

_MAJOR, _MINOR, _PATCH = (int(part) for part in PROTOCOL_VERSION.split("."))
# Compatible: same major.minor under 0.x. Incompatible: the next breaking line
# and the next major, both derived so a version bump cannot invert them.
_OLDEST_COMPATIBLE = f"{_MAJOR}.{_MINOR}.0"
_NEXT_PATCH = f"{_MAJOR}.{_MINOR}.{_PATCH + 1}"
_NEAR_INCOMPATIBLE = f"{_MAJOR}.{_MINOR + 1}.0" if _MAJOR == 0 else f"{_MAJOR + 1}.0.0"
_INCOMPATIBLE = list(dict.fromkeys([_NEAR_INCOMPATIBLE, f"{_MAJOR + 1}.0.0"]))

_UTC_INSTANT = datetime(2026, 11, 14, 17, 0, tzinfo=UTC)


def _ids(cases: list[dict[str, Any]]) -> list[str]:
    return [case["name"] for case in cases]


def _case(name: str) -> dict[str, Any]:
    return next(case for case in _ACCEPT if case["name"] == name)


def _line(frame: dict[str, Any], version: str = PROTOCOL_VERSION) -> str:
    """Stamp a vector frame with a version; frames omit it on purpose."""

    return json.dumps({"version": version, **frame})


def _dumped(condition: RetryCondition | None) -> dict[str, Any] | None:
    return None if condition is None else condition.model_dump(mode="json")


def _string_members(carrier: object) -> list[str]:
    if not isinstance(carrier, dict):
        return []
    return [
        value
        for value in (carrier.get("retry_at"), carrier.get("scope_key"))
        if isinstance(value, str)
    ]


# --- 1. The shared vector fails closed ----------------------------------------

_MUTATIONS: dict[str, Callable[[dict[str, Any]], object]] = {
    "unknown_top_level_key": lambda document: document.update(extra=1),
    "missing_top_level_key": lambda document: document.pop("comment"),
    "unknown_accept_case_key": lambda document: document["accept"][0].update(extra=1),
    "missing_accept_case_key": lambda document: document["accept"][0].pop(
        "expected_retry_condition"
    ),
    "unknown_refuse_case_key": lambda document: document["refuse"][0].update(extra=1),
    "missing_refuse_case_key": lambda document: document["refuse"][0].pop("violation"),
    "versioned_frame": lambda document: document["refuse"][0]["frame"].update(
        version=PROTOCOL_VERSION
    ),
    "unexpected_strict_construction": lambda document: document["accept"][0].update(
        strict_construction="accept"
    ),
    "unexpected_violation": lambda document: document["refuse"][0].update(violation="semantic"),
}


@pytest.mark.parametrize("mutation", sorted(_MUTATIONS))
def test_vector_loader_fails_closed_on_an_unexpected_shape(mutation: str, tmp_path: Path) -> None:
    document = json.loads(vector_path().read_text(encoding="utf-8"))
    intact = tmp_path / "intact.json"
    intact.write_text(json.dumps(document), encoding="utf-8")
    assert load_retry_condition_vector(intact) == document

    _MUTATIONS[mutation](document)
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(RetryConditionVectorError):
        load_retry_condition_vector(broken)


def test_vector_covers_every_reason_both_violation_classes_and_example_digests() -> None:
    expected = [case["expected_retry_condition"] for case in _ACCEPT]
    present = [condition for condition in expected if condition is not None]
    reasons = {condition["reason"] for condition in present}
    assert reasons == {reason.value for reason in RetryReason}
    assert None in expected
    assert {case["violation"] for case in _REFUSE} == {"type", "grammar"}
    assert [case["name"] for case in _ACCEPT if "strict_construction" in case] == [
        "unknown_nested_key_ignored"
    ]
    # The comment's claim: every digest is the sha256 of an example text.
    examples = (b"example credential scope", b"example credential scope two")
    digests = {f"sha256:{hashlib.sha256(text).hexdigest()}" for text in examples}
    assert {condition["scope_key"] for condition in present if condition["scope_key"]} == digests


# --- 2. Every accept frame decodes, constructs and round trips ----------------


@pytest.mark.parametrize("case", _ACCEPT, ids=_ids(_ACCEPT))
def test_accepted_frame_decodes_constructs_and_round_trips(case: dict[str, Any]) -> None:
    frame = case["frame"]
    expected = case["expected_retry_condition"]

    decoded = parse_ndjson_line(_line(frame))
    assert isinstance(decoded, ErrorEvent)
    assert _dumped(decoded.retry_condition) == expected
    assert decoded.message == frame["message"]
    assert decoded.classification == frame.get("classification")

    line = to_ndjson_line(decoded)
    assert json.loads(line)["retry_condition"] == expected
    redecoded = parse_ndjson_line(line)
    assert redecoded == decoded
    assert to_ndjson_line(redecoded) == line

    if case.get("strict_construction") == "refuse":
        unexpected = set(frame["retry_condition"]) - set(RetryCondition.model_fields)
        assert unexpected
        with pytest.raises(ValidationError) as refused:
            ErrorEvent.model_validate(frame)
        assert all(name in str(refused.value) for name in unexpected)
    else:
        produced = ErrorEvent.model_validate(frame)
        assert _dumped(produced.retry_condition) == expected
        assert produced == decoded


# --- 3. Every refuse frame fails decode and strict construction ---------------


@pytest.mark.parametrize("case", _REFUSE, ids=_ids(_REFUSE))
def test_refused_frame_fails_decode_and_construction_on_its_carrier(
    case: dict[str, Any],
) -> None:
    frame = case["frame"]
    with pytest.raises(ValidationError) as decoded:
        parse_ndjson_line(_line(frame))
    with pytest.raises(ValidationError) as constructed:
        ErrorEvent.model_validate(frame)

    for refusal in (decoded.value, constructed.value):
        errors = refusal.errors()
        assert errors
        assert all("retry_condition" in error["loc"] for error in errors)
        for value in _string_members(frame["retry_condition"]):
            assert all(value not in error["msg"] for error in errors)

    without_carrier = {key: value for key, value in frame.items() if key != "retry_condition"}
    assert isinstance(parse_ndjson_line(_line(without_carrier)), ErrorEvent)


# --- 4. Python object construction --------------------------------------------


def test_utc_datetime_is_accepted() -> None:
    condition = RetryCondition(reason=RetryReason.USAGE_RESET, retry_at=_UTC_INSTANT)
    assert condition.retry_at == _UTC_INSTANT
    assert condition.retry_at is not None
    assert condition.retry_at.tzinfo is UTC


def test_zero_offset_tzinfo_is_normalized_to_utc_and_written_with_z() -> None:
    zone = timezone(timedelta(0), "Z0")
    assert zone is not UTC
    event = ErrorEvent(
        message="Example provider error.",
        retry_condition=RetryCondition(
            reason=RetryReason.USAGE_RESET,
            retry_at=datetime(2026, 11, 14, 17, 0, tzinfo=zone),
        ),
    )
    assert event.retry_condition is not None
    assert event.retry_condition.retry_at is not None
    assert event.retry_condition.retry_at.tzinfo is UTC
    assert event.retry_condition.retry_at == _UTC_INSTANT

    line = to_ndjson_line(event)
    assert json.loads(line)["retry_condition"]["retry_at"] == "2026-11-14T17:00:00Z"
    assert parse_ndjson_line(line) == event


@pytest.mark.parametrize(
    "value",
    [
        datetime(2026, 11, 14, 17, 0),
        datetime(2026, 11, 14, 19, 0, tzinfo=timezone(timedelta(hours=2))),
        date(2026, 11, 14),
        1794243600,
        1794243600.5,
        True,
        b"2026-11-14T17:00:00Z",
    ],
    ids=["naive", "plus_two_hours", "date", "int", "float", "bool", "bytes"],
)
def test_python_retry_at_refuses_anything_but_utc(value: object) -> None:
    with pytest.raises(ValidationError):
        RetryCondition.model_validate({"reason": "usage_reset", "retry_at": value})


# --- 5. Existing messages ------------------------------------------------------


@pytest.mark.parametrize(
    "version", [_OLDEST_COMPATIBLE, _NEXT_PATCH], ids=["oldest_compatible", "next_patch"]
)
def test_existing_error_frame_decodes_without_a_retry_condition(version: str) -> None:
    frame = _case("absent_carrier")["frame"]
    assert "retry_condition" not in frame

    decoded = parse_ndjson_line(_line(frame, version))

    assert isinstance(decoded, ErrorEvent)
    assert decoded.version == version
    assert decoded.retry_condition is None
    assert decoded.message == frame["message"]
    assert decoded.classification == frame["classification"]


# --- 6. A tolerant older consumer ---------------------------------------------


class _ErrorEvent_0_5_25(_OutboundBase):  # noqa: N801 - the version IS the name
    """The ``ErrorEvent`` shape as of PROTOCOL_VERSION 0.5.25, pinned.

    Re-declared rather than imported so this test cannot follow the change it
    measures. It inherits ``_OutboundBase`` and so carries the same reader
    policy the real 0.5.25 model carried.
    """

    type: Literal["error"] = "error"
    message: str
    classification: str | None = None


def test_previous_patch_reader_drops_a_complete_retry_condition() -> None:
    assert set(ErrorEvent.model_fields) - set(_ErrorEvent_0_5_25.model_fields) == {
        "retry_condition"
    }
    assert set(_ErrorEvent_0_5_25.model_fields) <= set(ErrorEvent.model_fields)
    assert is_compatible(PROTOCOL_VERSION, "0.5.25")
    assert is_compatible("0.5.25", PROTOCOL_VERSION)

    payload = json.loads(_line(_case("complete_usage_reset")["frame"]))
    current = parse_ndjson_line(json.dumps(payload))
    assert isinstance(current, ErrorEvent)
    assert current.retry_condition is not None

    previous = _ErrorEvent_0_5_25.model_validate(payload, context=READER_CONTEXT)
    assert previous.message == current.message
    assert previous.classification == current.classification
    assert "retry_condition" not in previous.model_dump()
    # The tolerance is the reader context's, not a loosened model.
    with pytest.raises(ValidationError, match="retry_condition"):
        _ErrorEvent_0_5_25.model_validate(payload)


# --- 7. The version gate is not weakened ---------------------------------------


@pytest.mark.parametrize("version", _INCOMPATIBLE)
def test_complete_carrier_at_an_incompatible_version_is_refused(version: str) -> None:
    line = _line(_case("complete_usage_reset")["frame"], version)
    with pytest.raises(ProtocolVersionError) as refused:
        parse_ndjson_line(line)
    assert repr(version) in str(refused.value)
    assert repr(PROTOCOL_VERSION) in str(refused.value)


@pytest.mark.parametrize("case", _REFUSE, ids=_ids(_REFUSE))
def test_malformed_carrier_at_an_incompatible_version_fails_the_gate_first(
    case: dict[str, Any],
) -> None:
    with pytest.raises(ProtocolVersionError):
        parse_ndjson_line(_line(case["frame"], _NEAR_INCOMPATIBLE))


# --- 8. The exported schema ----------------------------------------------------


def test_exported_schema_adds_only_an_optional_carrier_and_a_new_enum() -> None:
    definitions = build_schema()["$defs"]

    error = definitions["ErrorEvent"]
    assert error["required"] == ["message", "type", "version"]
    carrier = error["properties"]["retry_condition"]
    assert {"$ref": "#/$defs/RetryCondition"} in carrier["anyOf"]
    assert {"type": "null"} in carrier["anyOf"]
    assert carrier["default"] is None

    condition = definitions["RetryCondition"]
    assert condition["required"] == ["reason"]
    assert condition["properties"]["reason"]["$ref"] == "#/$defs/RetryReason"
    retry_at = next(
        branch
        for branch in condition["properties"]["retry_at"]["anyOf"]
        if branch.get("type") == "string"
    )
    assert retry_at == {"type": "string", "format": "date-time", "pattern": RETRY_AT_PATTERN}
    scope_key = next(
        branch
        for branch in condition["properties"]["scope_key"]["anyOf"]
        if branch.get("type") == "string"
    )
    assert scope_key == {"type": "string", "pattern": RETRY_SCOPE_KEY_PATTERN}

    assert definitions["RetryReason"]["enum"] == [
        "rate_limited",
        "usage_reset",
        "credit_exhausted",
        "credential_rejected",
        "transient_service",
    ]
    # No existing frozen enum gained a value.
    assert definitions["SessionStatus"]["enum"] == [
        "done",
        "idle-awaiting-input",
        "classified-failure",
        "awaiting-approval",
    ]
    assert definitions["ToolAccess"]["enum"] == ["read-only"]
