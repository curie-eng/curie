"""Adoption-credential wire contract (#2385, ADR-0116, ADR-0122).

This package defines delivery and admission semantics. It does not implement
runner token swap, bootstrap retirement, route recovery, or warm-pool replicas.
A schema field is not those behaviors.
"""

from __future__ import annotations

import json

import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    ErrorEvent,
    Event,
    Final,
    Interrupt,
    TextDelta,
    parse_inbound,
    parse_ndjson_line,
    to_inbound_json,
    to_ndjson_line,
)
from pydantic import ValidationError

# Distinctive fixture material, not a live credential. Tests assert this exact
# string never appears in repr or validation errors.
_CREDENTIAL = "adoption-credential-fixture-PLACEHOLDER"
_BASE = {
    "kind": "event",
    "type": "message",
    "text": "hi",
    "user": "U0EXAMPLE1",
    "ts": "1.0",
}


def _payload(**fields: object) -> dict[str, object]:
    body: dict[str, object] = dict(_BASE)
    body.update(fields)
    return body


def test_legacy_omission_is_compatible() -> None:
    event = parse_inbound(_payload())
    assert isinstance(event, Event)
    assert event.adoption_credential is None
    assert event.session_id is None
    assert event.history_ref is None


def test_explicit_null_is_compatible() -> None:
    event = parse_inbound(_payload(adoption_credential=None))
    assert isinstance(event, Event)
    assert event.adoption_credential is None


def test_well_formed_credential_roundtrips_on_the_wire() -> None:
    event = Event(
        type="message",
        text="hi",
        user="U0EXAMPLE1",
        ts="1.0",
        adoption_credential=_CREDENTIAL,
    )
    encoded = to_inbound_json(event)
    wire = json.loads(encoded)
    decoded = parse_inbound(encoded)

    assert wire["adoption_credential"] == _CREDENTIAL
    assert isinstance(decoded, Event)
    assert decoded.adoption_credential == _CREDENTIAL


@pytest.mark.parametrize(
    "value",
    (
        1,
        False,
        ["not-a-string"],
        {"not": "a string"},
        "",
        "   ",
        "\n",
        "\t",
        "x" * 4097,
    ),
)
def test_malformed_credential_is_rejected(value: object) -> None:
    with pytest.raises(ValidationError):
        Event.model_validate(_payload(adoption_credential=value))
    with pytest.raises(ValidationError):
        parse_inbound(_payload(adoption_credential=value))


def test_malformed_rejection_is_atomic_and_does_not_construct_a_partial_event() -> None:
    with pytest.raises(ValidationError):
        Event.model_validate(_payload(text="turn-text", adoption_credential=""))
    # A sibling Event without the field still constructs; the failed validate
    # must not have been a half-applied mutation of a shared model.
    ok = Event.model_validate(_payload(text="turn-text"))
    assert ok.text == "turn-text"
    assert ok.adoption_credential is None


def test_interrupt_rejects_adoption_credential() -> None:
    with pytest.raises(ValidationError):
        Interrupt.model_validate({"reason": "stop", "adoption_credential": _CREDENTIAL})


def test_credential_is_absent_from_repr_and_str() -> None:
    event = Event(
        type="message",
        text="hi",
        user="U0EXAMPLE1",
        ts="1.0",
        adoption_credential=_CREDENTIAL,
    )
    assert _CREDENTIAL not in repr(event)
    assert _CREDENTIAL not in str(event)


def test_malformed_credential_errors_do_not_echo_secret_material() -> None:
    secret = _CREDENTIAL + ("x" * 4097)
    with pytest.raises(ValidationError) as direct:
        Event.model_validate(_payload(adoption_credential=secret))
    with pytest.raises(ValidationError) as inbound:
        parse_inbound(_payload(adoption_credential=secret))

    for exc in (direct.value, inbound.value):
        rendered = f"invalid event frame: {exc}"
        assert secret not in str(exc)
        assert secret not in repr(exc)
        assert secret not in rendered
        assert secret not in json.dumps(exc.errors(), default=str)


def test_error_event_message_is_not_a_credential_channel() -> None:
    error = ErrorEvent(message="malformed adoption credential")
    assert _CREDENTIAL not in error.message
    assert "adoption_credential" not in error.model_dump(mode="json")


def test_adoption_applied_omission_and_null_are_compatible() -> None:
    omitted = parse_ndjson_line(
        json.dumps({"type": "final", "version": PROTOCOL_VERSION, "text": "ok", "status": "done"})
    )
    assert isinstance(omitted, Final)
    assert omitted.adoption_applied is None

    explicit_null = parse_ndjson_line(
        json.dumps(
            {
                "type": "final",
                "version": PROTOCOL_VERSION,
                "text": "ok",
                "status": "done",
                "adoption_applied": None,
            }
        )
    )
    assert isinstance(explicit_null, Final)
    assert explicit_null.adoption_applied is None


@pytest.mark.parametrize("applied", (True, False))
def test_adoption_applied_roundtrips(applied: bool) -> None:
    final = Final(text="ok", adoption_applied=applied)
    wire = json.loads(to_ndjson_line(final))
    decoded = parse_ndjson_line(json.dumps(wire))
    assert wire["adoption_applied"] is applied
    assert isinstance(decoded, Final)
    assert decoded.adoption_applied is applied


def test_missing_adoption_applied_is_not_successful_adoption() -> None:
    """An old tolerant consumer ignores adoption_credential and emits no ack.

    A successful final without adoption_applied is the pre-0.4.5 shape. The
    producer must treat that as "not adopted", not as bootstrap retirement.
    """

    legacy = TextDelta(text="working")
    assert legacy.adoption_applied is None
    final = Final(text="done")
    assert final.adoption_applied is None


def test_container_and_bytes_errors_do_not_echo_secret_material() -> None:
    raw = json.dumps([_payload(adoption_credential=_CREDENTIAL)])
    with pytest.raises(ValidationError) as list_exc:
        parse_inbound(raw)
    assert _CREDENTIAL not in str(list_exc.value)
    assert _CREDENTIAL not in f"invalid event frame: {list_exc.value}"

    json_raw = (
        b'{"type":"message","text":"hi","user":"U0EXAMPLE1","ts":"1.0",'
        + f'"adoption_credential":"{_CREDENTIAL}", bad}}'.encode()
    )
    with pytest.raises(ValidationError) as bytes_exc:
        Event.model_validate_json(json_raw)
    assert _CREDENTIAL not in str(bytes_exc.value)
    assert _CREDENTIAL not in f"invalid event frame: {bytes_exc.value}"


def test_unrelated_missing_field_is_not_reported_as_malformed_credential() -> None:
    with pytest.raises(ValidationError) as exc:
        Event.model_validate(
            {
                "kind": "event",
                "type": "message",
                "user": "U0EXAMPLE1",
                "ts": "1.0",
                "adoption_credential": _CREDENTIAL,
            }
        )
    rendered = str(exc.value)
    assert "malformed adoption credential" not in rendered
    assert _CREDENTIAL not in rendered


def test_protocol_version_is_the_compatible_patch() -> None:
    # 0.4.5 introduced this contract; later optional-field patches on the same
    # 0.4 line stay wire compatible with it.
    from aci_protocol import is_compatible

    assert is_compatible("0.4.5", PROTOCOL_VERSION)


def test_malformed_json_errors_do_not_echo_secret_material() -> None:
    raw = (
        '{"kind":"event","type":"message","text":"hi","user":"U0EXAMPLE1",'
        f'"ts":"1.0","adoption_credential":"{_CREDENTIAL}", bad}}'
    )
    with pytest.raises(Exception) as exc:
        parse_inbound(raw)
    rendered = f"invalid event frame: {exc.value}"
    assert _CREDENTIAL not in str(exc.value)
    assert _CREDENTIAL not in rendered

    json_raw = (
        '{"type":"message","text":"hi","user":"U0EXAMPLE1","ts":"1.0",'
        f'"adoption_credential":"{_CREDENTIAL}", bad}}'
    )
    with pytest.raises(ValidationError) as json_exc:
        Event.model_validate_json(json_raw)
    assert _CREDENTIAL not in str(json_exc.value)
    assert _CREDENTIAL not in f"invalid event frame: {json_exc.value}"


_ESCAPED_KEY = "adoption_credenti\\u0061l"  # JSON-escaped spelling of the field name


def _malformed_json_specimens() -> list[tuple[str, str | bytes | bytearray]]:
    # Raw JSON that never parses, so pydantic reports ``json_invalid`` and would
    # otherwise attach the WHOLE raw input. The escaped key defeats a literal
    # field-name scrub; the keyless specimen has no field name to match at all.
    escaped = (
        '{"kind":"event","type":"message","text":"hi","user":"U0EXAMPLE1",'
        f'"ts":"1.0","{_ESCAPED_KEY}":"{_CREDENTIAL}", bad}}'
    )
    keyless = f'{{"text":"{_CREDENTIAL}", bad}}'
    return [
        ("escaped-str", escaped),
        ("escaped-bytes", escaped.encode()),
        ("escaped-bytearray", bytearray(escaped.encode())),
        ("keyless-str", keyless),
        ("keyless-bytes", keyless.encode()),
        ("keyless-bytearray", bytearray(keyless.encode())),
    ]


def _assert_no_credential_in_exception_chain(error: BaseException) -> None:
    pending = [error]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        surfaces = [str(current), repr(current)]
        if isinstance(current, ValidationError):
            surfaces.extend((json.dumps(current.errors(), default=str), current.json()))
        assert all(_CREDENTIAL not in surface for surface in surfaces)
        # Follow both links even when traceback display suppresses the context.
        pending.extend(
            linked for linked in (current.__cause__, current.__context__) if linked is not None
        )
    assert error.__cause__ is None
    assert error.__context__ is None


@pytest.mark.parametrize(
    ("label", "raw"),
    _malformed_json_specimens(),
    ids=[label for label, _ in _malformed_json_specimens()],
)
def test_invalid_json_errors_redact_the_whole_raw_input(
    label: str, raw: str | bytes | bytearray
) -> None:
    with pytest.raises(ValidationError) as exc:
        Event.model_validate_json(raw)
    error = exc.value
    # ``str(exc)`` truncating a long input is not a protection; ``errors()`` is
    # what a structured 400 body or a log formatter serializes.
    surfaces = {
        "str": str(error),
        "repr": repr(error),
        "errors": json.dumps(error.errors(), default=str),
        "interpolated": f"invalid event frame: {error}",
    }
    leaked = [name for name, text in surfaces.items() if _CREDENTIAL in text]
    assert not leaked, f"{label}: credential material in {leaked}"
    # The diagnosis stays truthful: malformed JSON is reported as invalid JSON,
    # not misdiagnosed as a malformed credential, and no raw input survives.
    types = {err["type"] for err in error.errors()}
    assert types == {"json_invalid"}, types
    assert all(err["input"] == "<redacted>" for err in error.errors())
    _assert_no_credential_in_exception_chain(error)


@pytest.mark.parametrize("encoding", (str, bytes, bytearray))
def test_json_unrelated_error_keeps_diagnosis_without_credential_chain(
    encoding: type[str] | type[bytes] | type[bytearray],
) -> None:
    body = _payload(adoption_credential=_CREDENTIAL)
    del body["text"]
    raw = json.dumps(body)
    encoded = raw if encoding is str else encoding(raw.encode())
    with pytest.raises(ValidationError) as exc:
        Event.model_validate_json(encoded)
    assert [(err["type"], err["loc"]) for err in exc.value.errors()] == [("missing", ("text",))]
    assert "malformed adoption credential" not in str(exc.value)
    _assert_no_credential_in_exception_chain(exc.value)


def test_mapping_inputs_cannot_bypass_malformed_admission() -> None:
    from collections import UserDict

    payload = UserDict(_payload(adoption_credential=""))
    with pytest.raises(ValidationError):
        Event.model_validate(payload)


def test_string_true_is_not_an_adoption_ack() -> None:
    with pytest.raises(ValidationError):
        parse_ndjson_line(
            json.dumps(
                {
                    "type": "final",
                    "version": PROTOCOL_VERSION,
                    "text": "ok",
                    "status": "done",
                    "adoption_applied": "true",
                }
            )
        )
    with pytest.raises(ValidationError):
        parse_ndjson_line(
            json.dumps(
                {
                    "type": "final",
                    "version": PROTOCOL_VERSION,
                    "text": "ok",
                    "status": "done",
                    "adoption_applied": 1,
                }
            )
        )
