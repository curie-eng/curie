import json

import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    Event,
    Final,
    Interrupt,
    ProtocolVersionError,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolNote,
    dump_ndjson,
    parse_inbound,
    parse_ndjson,
    parse_ndjson_line,
    to_inbound_json,
    to_ndjson_line,
)
from pydantic import ValidationError


def test_ndjson_line_has_trailing_newline() -> None:
    line = to_ndjson_line(TextDelta(text="hi"))
    assert line.endswith("\n")
    assert line.count("\n") == 1


def test_ndjson_document_roundtrips_every_type() -> None:
    events = [
        TextDelta(text="a"),
        ToolNote(text="calling", tool="search"),
        SideEffectFlag(tool="send_email"),
        Final(text="done", status=SessionStatus.DONE),
    ]
    assert parse_ndjson(dump_ndjson(events)) == events


def test_parse_skips_blank_lines() -> None:
    doc = to_ndjson_line(TextDelta(text="a")) + "\n   \n" + to_ndjson_line(Final(text="b"))
    assert len(parse_ndjson(doc)) == 2


def test_unknown_version_is_rejected() -> None:
    bad = json.dumps({"type": "final", "version": "9.9.9", "text": "x", "status": "done"})
    with pytest.raises(ProtocolVersionError):
        parse_ndjson_line(bad)


def test_missing_version_is_rejected() -> None:
    bad = json.dumps({"type": "final", "text": "x", "status": "done"})
    with pytest.raises(ProtocolVersionError):
        parse_ndjson_line(bad)


def test_current_version_is_accepted() -> None:
    good = json.dumps({"type": "final", "version": PROTOCOL_VERSION, "text": "x", "status": "done"})
    assert parse_ndjson_line(good) == Final(text="x")


def test_inbound_roundtrip() -> None:
    for message in (
        Event(type="message", text="hi", user="U1", ts="1.0"),
        Interrupt(reason="stop"),
    ):
        assert parse_inbound(to_inbound_json(message)) == message


def test_legacy_inbound_event_without_session_context_defaults_to_none() -> None:
    raw = json.dumps(
        {
            "kind": "event",
            "type": "message",
            "text": "hi",
            "user": "U0EXAMPLE1",
            "ts": "1.0",
        }
    )

    message = parse_inbound(raw)

    assert isinstance(message, Event)
    assert message.session_id is None
    assert message.history_ref is None


def test_inbound_event_accepts_explicit_null_session_context() -> None:
    raw = json.dumps(
        {
            "kind": "event",
            "type": "message",
            "text": "hi",
            "user": "U0EXAMPLE1",
            "ts": "1.0",
            "session_id": None,
            "history_ref": None,
        }
    )

    message = parse_inbound(raw)

    assert isinstance(message, Event)
    assert message.session_id is None
    assert message.history_ref is None


def test_inbound_event_session_context_survives_json_roundtrip() -> None:
    message = Event(
        type="message",
        text="hi",
        user="U0EXAMPLE1",
        ts="1.0",
        session_id="session-example",
        history_ref="history-example",
    )

    encoded = to_inbound_json(message)
    wire = json.loads(encoded)
    decoded = parse_inbound(encoded)

    assert wire["session_id"] == "session-example"
    assert wire["history_ref"] == "history-example"
    assert isinstance(decoded, Event)
    assert decoded.session_id == "session-example"
    assert decoded.history_ref == "history-example"
    assert decoded.adoption_credential is None


def test_legacy_inbound_event_without_adoption_credential_defaults_to_none() -> None:
    raw = json.dumps(
        {
            "kind": "event",
            "type": "message",
            "text": "hi",
            "user": "U0EXAMPLE1",
            "ts": "1.0",
            "session_id": "session-example",
            "history_ref": "history-example",
        }
    )

    message = parse_inbound(raw)

    assert isinstance(message, Event)
    assert message.adoption_credential is None


def test_inbound_event_accepts_explicit_null_adoption_credential() -> None:
    raw = json.dumps(
        {
            "kind": "event",
            "type": "message",
            "text": "hi",
            "user": "U0EXAMPLE1",
            "ts": "1.0",
            "adoption_credential": None,
        }
    )

    message = parse_inbound(raw)

    assert isinstance(message, Event)
    assert message.adoption_credential is None


# --- Reader policy: tolerant consumers, strict producers ----------------------


def test_unknown_field_is_ignored_on_read() -> None:
    # AC: consumers accept and ignore unknown fields they do not model. The
    # decoder threads a reader context that loosens the strict model on read.
    line = json.dumps(
        {
            "type": "final",
            "version": PROTOCOL_VERSION,
            "text": "x",
            "status": "done",
            "future_field": 1,
        }
    )
    assert parse_ndjson_line(line) == Final(text="x")


def test_unknown_field_is_rejected_on_construction() -> None:
    # AC: producers reject unknown fields on construction. This is the guard that
    # stops a lazy extra="ignore" from silently satisfying the reader-side test.
    with pytest.raises(ValidationError):
        Final(text="x", future_field=1)  # type: ignore[call-arg]


def test_unknown_field_is_ignored_on_inbound_read() -> None:
    # Edge 7: the reader context must reach parse_inbound as well as
    # parse_ndjson_line, or the runner's inbound decode stays strict cross-process.
    raw = json.dumps(
        {
            "kind": "event",
            "type": "message",
            "text": "hi",
            "user": "U1",
            "ts": "1.0",
            "future_field": 1,
        }
    )
    msg = parse_inbound(raw)
    assert isinstance(msg, Event)
    assert msg.text == "hi"


# --- Version compatibility (semver, major.minor under 0.x) --------------------


def test_compatible_patch_version_is_accepted() -> None:
    # AC: a same-major-minor patch difference is not an error. Build speaks
    # 0.4.0; a 0.4.7 line decodes fine.
    line = json.dumps({"type": "final", "version": "0.4.7", "text": "x", "status": "done"})
    decoded = parse_ndjson_line(line)
    assert isinstance(decoded, Final)
    assert decoded.text == "x"


def test_incompatible_minor_is_rejected_naming_both_versions() -> None:
    # AC: rejects an incompatible version with a message naming both versions.
    # The assertion is on the message content, not just the exception type.
    line = json.dumps({"type": "final", "version": "0.3.0", "text": "x", "status": "done"})
    with pytest.raises(ProtocolVersionError) as excinfo:
        parse_ndjson_line(line)
    message = str(excinfo.value)
    assert "0.3.0" in message
    assert PROTOCOL_VERSION in message


def test_incompatible_major_is_rejected() -> None:
    line = json.dumps({"type": "final", "version": "1.0.0", "text": "x", "status": "done"})
    with pytest.raises(ProtocolVersionError):
        parse_ndjson_line(line)


@pytest.mark.parametrize(
    "bad_version",
    [
        "abc",
        "",
        "0.2",
        "0.2.0-rc.1",
        "0.2.0+build",
        # ASCII semver refinements (change 3): no leading zeros, no trailing
        # newline (the ``$`` anchor tolerates it in Python, ``fullmatch`` rejects
        # it), and no Unicode digits (``[0-9]`` not ``\d``). The generated Rust
        # parses ASCII-only, so accepting any of these would split the lanes.
        "0.02.0",
        "0.2.0\n",
        "０.２.０",  # fullwidth digits for 0.2.0
    ],
)
def test_malformed_version_is_rejected_via_the_decoder_contract(bad_version: str) -> None:
    # Edge 5: a malformed version must reject through the decoder's own error
    # contract (ProtocolVersionError), never escape as an IndexError/ValueError
    # out of a bare .split(".").
    line = json.dumps({"type": "final", "version": bad_version, "text": "x", "status": "done"})
    with pytest.raises(ProtocolVersionError):
        parse_ndjson_line(line)
