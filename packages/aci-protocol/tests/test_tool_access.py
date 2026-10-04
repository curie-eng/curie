"""The optional per-turn tool access on the queued turn and the ACI event.

A producer that must never act (a synthetic availability probe) sets
``tool_access="read-only"`` on the ``QueuedTurn`` it enqueues; the worker
forwards it on the ``Event`` it posts to the runner. These tests pin the wire
half only: the field constructs strictly, survives serialization and the
sanctioned consumer decodes, is absent (null) on every turn that does not set
it, and refuses a value it does not know rather than reading it as null.
"""

import json
from pathlib import Path

import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    TOOL_ACCESS_STATUS_FIELD,
    Event,
    QueuedTurn,
    ToolAccess,
    parse_inbound,
    parse_queued_turn,
    to_inbound_json,
)
from aci_protocol.schema_export import build_schema
from aci_protocol.turn import CLUSTER_MESSAGE_ADAPTER
from pydantic import ValidationError

_GOLDEN = Path(__file__).resolve().parents[1] / "schema" / "queued-turn.fixture.json"


def _relay_turn(**extra: object) -> dict[str, object]:
    """A disconnected cluster-message relay turn, the shape a canary enqueues."""

    return {
        "event_id": "EvSIM-acme-probe",
        "conversation_id": "eval:acme-probe-0001",
        "author": "U0EXAMPLE1",
        "text": "Reply with exactly: nonce-0001",
        "reply_handle": {
            "kind": "slack",
            "channel": "C0EXAMPLE1",
            "placeholder": None,
            "endpoint": None,
            "adapter": CLUSTER_MESSAGE_ADAPTER,
            "identity": "acme-bot",
        },
        "received_at": "2026-01-01T00:00:00Z",
        "source": "slack",
        "attachments": [],
        "hook_run": None,
        **extra,
    }


def _event(**extra: object) -> dict[str, object]:
    return {
        "kind": "event",
        "type": "message",
        "text": "Reply with exactly: nonce-0001",
        "user": "U0EXAMPLE1",
        "ts": "eval:acme-probe-0001",
        **extra,
    }


def test_the_one_tool_access_value_is_read_only() -> None:
    # @spec TOOL-ACCESS-1
    assert [member.value for member in ToolAccess] == ["read-only"]
    assert ToolAccess("read-only") is ToolAccess.READ_ONLY


def test_a_read_only_relay_turn_constructs_strictly_and_survives_the_worker_decode() -> None:
    # @spec TOOL-ACCESS-1 TOOL-ACCESS-6
    produced = QueuedTurn.model_validate(_relay_turn(tool_access="read-only"))
    assert produced.tool_access is ToolAccess.READ_ONLY

    wire = produced.model_dump_json()
    assert json.loads(wire)["tool_access"] == "read-only"

    consumed = parse_queued_turn(wire)
    assert consumed.tool_access is ToolAccess.READ_ONLY
    assert consumed.reply_handle is not None
    assert consumed.reply_handle.identity == "acme-bot"


def test_a_turn_that_does_not_set_tool_access_is_unrestricted() -> None:
    # @spec TOOL-ACCESS-1
    produced = QueuedTurn.model_validate(_relay_turn())
    assert produced.tool_access is None
    assert json.loads(produced.model_dump_json())["tool_access"] is None


def test_a_payload_written_before_the_field_existed_decodes_unrestricted() -> None:
    # @spec TOOL-ACCESS-1: a pre-upgrade producer omits the key entirely.
    pre_upgrade = json.loads(_GOLDEN.read_text())
    pre_upgrade.pop("tool_access", None)

    turn = parse_queued_turn(json.dumps(pre_upgrade))

    assert turn.tool_access is None


def test_the_committed_golden_fixture_carries_a_null_tool_access() -> None:
    # @spec TOOL-ACCESS-1: the cross-language golden is an ordinary turn, and the
    # Rust CLI re-serializes it byte-identically, so the key is present as null.
    golden = json.loads(_GOLDEN.read_text())
    assert "tool_access" in golden
    assert golden["tool_access"] is None
    assert parse_queued_turn(_GOLDEN.read_text()).tool_access is None


def test_an_unknown_queued_tool_access_is_refused_never_degraded() -> None:
    # @spec TOOL-ACCESS-2: even the tolerant consumer decode refuses it. Reading
    # it as null would run a restricted turn unrestricted.
    wire = json.dumps(_relay_turn(tool_access="read-mostly"))

    with pytest.raises(ValidationError):
        parse_queued_turn(wire)
    with pytest.raises(ValidationError):
        QueuedTurn.model_validate(_relay_turn(tool_access="read-mostly"))


def test_a_read_only_event_round_trips_through_the_inbound_codec() -> None:
    # @spec TOOL-ACCESS-1 TOOL-ACCESS-6
    produced = Event.model_validate(_event(tool_access="read-only"))
    assert produced.tool_access is ToolAccess.READ_ONLY

    wire = to_inbound_json(produced)
    assert json.loads(wire)["tool_access"] == "read-only"

    consumed = parse_inbound(wire)
    assert isinstance(consumed, Event)
    assert consumed.tool_access is ToolAccess.READ_ONLY


def test_an_event_without_tool_access_is_unrestricted() -> None:
    # @spec TOOL-ACCESS-1: an older worker's frame omits the key entirely.
    consumed = parse_inbound(json.dumps(_event()))
    assert isinstance(consumed, Event)
    assert consumed.tool_access is None


def test_an_unknown_event_tool_access_is_refused_never_degraded() -> None:
    # @spec TOOL-ACCESS-2: by the consumer decode and by a strict producer alike.
    with pytest.raises(ValidationError):
        parse_inbound(json.dumps(_event(tool_access="read-mostly")))
    with pytest.raises(ValidationError):
        Event.model_validate(_event(tool_access="read-mostly"))


@pytest.mark.parametrize("spelling", ["Read-Only", "READ-ONLY", "read_only", " read-only"])
def test_the_spelling_of_read_only_is_exact(spelling: str) -> None:
    # @spec TOOL-ACCESS-2: a near miss is an unknown value, not read-only.
    with pytest.raises(ValidationError):
        parse_inbound(json.dumps(_event(tool_access=spelling)))
    with pytest.raises(ValidationError):
        parse_queued_turn(json.dumps(_relay_turn(tool_access=spelling)))


def test_the_runner_status_advertisement_key_is_tool_access() -> None:
    # @spec TOOL-ACCESS-4: one constant both the runner and the worker import,
    # so the advertisement and the check cannot spell the key differently.
    assert TOOL_ACCESS_STATUS_FIELD == "tool_access"


def test_the_schema_declares_tool_access_optional_on_both_models() -> None:
    # @spec TOOL-ACCESS-1 TOOL-ACCESS-2: a new optional field is a patch.
    schema = build_schema()
    assert schema["protocolVersion"] == PROTOCOL_VERSION == "0.5.16"

    definitions = schema["$defs"]
    assert definitions["ToolAccess"]["enum"] == ["read-only"]
    for model in ("Event", "QueuedTurn"):
        definition = definitions[model]
        assert "tool_access" not in definition.get("required", [])
        field = definition["properties"]["tool_access"]
        assert {"$ref": "#/$defs/ToolAccess"} in field["anyOf"]
        assert {"type": "null"} in field["anyOf"]
        assert field["default"] is None
