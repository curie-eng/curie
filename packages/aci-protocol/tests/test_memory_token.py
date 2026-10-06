"""The optional per-turn memory credential on the ACI event.

The worker mints a short-lived write credential for each turn and carries it to
the runner on the ``Event`` it posts (``/v1/event``, ``/v1/steer``), so the
runner's memory tools can present it without the credential ever entering the
sandbox env. These tests pin the wire half only: the field is optional and null
by default, survives the inbound codec, stays out of ``repr``, and is declared in
the schema as an optional nullable string.
"""

import json

from aci_protocol import Event, parse_inbound, to_inbound_json
from aci_protocol.schema_export import build_schema

# Shaped like a sandbox token so the repr check is meaningful; it signs nothing.
_TOKEN = "sbx.eyJhZ2VudCI6ImFjbWUtYm90In0.not-a-real-signature"


def _event(**extra: object) -> dict[str, object]:
    return {
        "kind": "event",
        "type": "message",
        "text": "Remember that the standup moved to 10am",
        "user": "U0EXAMPLE1",
        "ts": "1720000000.000100",
        **extra,
    }


def test_event_memory_token_defaults_to_null() -> None:
    # @spec MEMORY-TOKEN-1: null means the turn carries no write credential.
    produced = Event.model_validate(_event())
    assert produced.memory_token is None

    dumped = produced.model_dump(mode="json")
    assert "memory_token" in dumped
    assert dumped["memory_token"] is None
    assert json.loads(to_inbound_json(produced))["memory_token"] is None


def test_event_memory_token_round_trips_through_parse_inbound() -> None:
    # @spec MEMORY-TOKEN-1 MEMORY-TOKEN-2
    produced = Event.model_validate(_event(memory_token=_TOKEN))
    assert produced.memory_token == _TOKEN

    wire = to_inbound_json(produced)
    assert json.loads(wire)["memory_token"] == _TOKEN

    consumed = parse_inbound(wire)
    assert isinstance(consumed, Event)
    assert consumed.memory_token == _TOKEN


def test_event_repr_hides_memory_token() -> None:
    # @spec MEMORY-TOKEN-3: repr(event) and a log's %r never show the credential.
    event = Event.model_validate(_event(memory_token=_TOKEN))

    assert _TOKEN not in repr(event)
    assert _TOKEN not in str(event)
    assert _TOKEN not in f"{event!r}"
    assert "memory_token" not in repr(event)
    # The value is still there for the runner to use.
    assert event.memory_token == _TOKEN


def test_schema_lists_memory_token_as_optional_nullable_string() -> None:
    # @spec MEMORY-TOKEN-1: a new optional field is a patch under 0.x.
    schema = build_schema()
    assert schema["protocolVersion"] == "0.5.17"

    event = schema["$defs"]["Event"]
    assert "memory_token" not in event.get("required", [])
    field = event["properties"]["memory_token"]
    assert {"type": "string"} in field["anyOf"]
    assert {"type": "null"} in field["anyOf"]
    assert len(field["anyOf"]) == 2
    assert field["default"] is None
