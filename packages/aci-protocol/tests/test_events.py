import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    ErrorEvent,
    Event,
    Final,
    InboundMessage,
    Interrupt,
    OutboundEvent,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolNote,
)
from pydantic import TypeAdapter, ValidationError

_OUTBOUND = TypeAdapter(OutboundEvent)
_INBOUND = TypeAdapter(InboundMessage)


def test_outbound_events_default_version_to_protocol_version() -> None:
    for event in (
        TextDelta(text="hi"),
        ToolNote(text="note"),
        Final(text="done"),
        ErrorEvent(message="boom"),
        SideEffectFlag(),
    ):
        assert event.version == PROTOCOL_VERSION


def test_final_defaults_to_done_status() -> None:
    assert Final(text="ok").status is SessionStatus.DONE


def test_unknown_session_status_is_rejected() -> None:
    # Decision 3: an unknown SessionStatus is a hard decode error, never
    # degraded to a fallback. Status is control-bearing (awaiting-approval drives
    # suspend-and-wait); silently defaulting a future value to "done" would
    # finalize a turn that is actually pending a human decision.
    with pytest.raises(ValidationError):
        _OUTBOUND.validate_python(
            {
                "type": "final",
                "version": PROTOCOL_VERSION,
                "text": "x",
                "status": "invented-future-status",
            }
        )


def test_outbound_union_discriminates_on_type() -> None:
    decoded = _OUTBOUND.validate_python(
        {"type": "tool_note", "version": PROTOCOL_VERSION, "text": "n", "tool": "search"}
    )
    assert isinstance(decoded, ToolNote)
    assert decoded.tool == "search"


def test_inbound_union_discriminates_on_kind() -> None:
    event = _INBOUND.validate_python(
        {"kind": "event", "type": "message", "text": "hi", "user": "U1", "ts": "1.0"}
    )
    interrupt = _INBOUND.validate_python({"kind": "interrupt", "reason": "stop"})
    assert isinstance(event, Event)
    assert isinstance(interrupt, Interrupt)


def test_event_type_is_constrained() -> None:
    with pytest.raises(ValidationError):
        Event(type="not_a_type", text="x", user="u", ts="1.0")  # type: ignore[arg-type]


def test_models_reject_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Final(text="ok", nonsense=1)  # type: ignore[call-arg]


def test_event_rejects_unknown_fields_on_direct_construction() -> None:
    with pytest.raises(ValidationError):
        Event(  # type: ignore[call-arg]
            type="message",
            text="hi",
            user="U0EXAMPLE1",
            ts="1.0",
            nonsense=1,
        )


@pytest.mark.parametrize("field", ("session_id", "history_ref", "adoption_credential"))
def test_interrupt_rejects_event_session_context_fields(field: str) -> None:
    with pytest.raises(ValidationError):
        Interrupt.model_validate({"reason": "stop", field: "context-example"})


@pytest.mark.parametrize("field", ("session_id", "history_ref"))
@pytest.mark.parametrize(
    "value",
    (
        1,
        False,
        ["not-a-string"],
        {"not": "a string"},
    ),
)
def test_event_session_context_rejects_non_string_non_null_values(
    field: str, value: object
) -> None:
    payload = {
        "type": "message",
        "text": "hi",
        "user": "U0EXAMPLE1",
        "ts": "1.0",
        field: value,
    }

    with pytest.raises(ValidationError):
        Event.model_validate(payload)


def test_session_status_wire_values() -> None:
    assert SessionStatus.IDLE_AWAITING_INPUT.value == "idle-awaiting-input"
    assert SessionStatus.CLASSIFIED_FAILURE.value == "classified-failure"


def test_awaiting_approval_wire_value_and_final_round_trip() -> None:
    # ADR-0010 (#244): the fourth status and the optional summary field on
    # final, round-tripped through the strict wire models.
    assert SessionStatus.AWAITING_APPROVAL.value == "awaiting-approval"

    final = Final(
        text="Requesting sign-off",
        status=SessionStatus.AWAITING_APPROVAL,
        approval_summary="Give ACME a 20% discount",
    )
    wire = final.model_dump(mode="json")
    assert wire["status"] == "awaiting-approval"
    assert wire["approval_summary"] == "Give ACME a 20% discount"
    assert Final.model_validate(wire) == final

    # Pre-existing payloads (no approval fields) still parse, defaulting the
    # summary to None -- the additive-change guarantee.
    legacy = Final.model_validate({"type": "final", "text": "ok", "status": "done"})
    assert legacy.approval_summary is None


# --- What a side-effecting call reports (ADR-0117) -----------------------------


def test_side_effect_flag_carries_the_call_its_arguments_and_its_result() -> None:
    """The frame is the only place the platform learns what a call did.

    Before ADR-0117 it carried a tool name and a constant ``detail`` string, so a
    consumer could know that something mutated and never what.
    """

    flag = SideEffectFlag(
        tool="scale_deployment",
        call_id="toolu_01",
        arguments={"name": "api", "replicas": 10},
        result={"ok": True, "prior": {"spec": {"replicas": 3}}},
        failed=False,
    )
    assert flag.call_id == "toolu_01"
    assert flag.arguments == {"name": "api", "replicas": 10}
    assert flag.result == {"ok": True, "prior": {"spec": {"replicas": 3}}}
    assert flag.failed is False


def test_side_effect_flag_fields_are_optional_for_an_older_producer() -> None:
    """ADR-0036's reader policy: an additive optional field costs readers nothing.

    A producer that predates ADR-0117 emits neither, and the frame it wrote still
    decodes -- which is what makes this a patch bump and not a minor.
    """

    decoded = _OUTBOUND.validate_python({"type": "side_effect_flag", "version": "0.4.1"})
    assert decoded.call_id is None
    assert decoded.arguments is None
    assert decoded.result is None
    assert decoded.failed is None


def test_two_frames_of_one_call_are_joinable_by_call_id() -> None:
    """One call, two frames, one record.

    The opening frame is emitted when the call is made, so the no-retry rule
    latches even if the turn dies mid-call; the closing frame carries what came
    back. Without a shared call id a consumer cannot join them, and a turn that
    calls the same tool twice would collapse into one record or three.
    """

    opened = SideEffectFlag(tool="scale_deployment", call_id="toolu_01", arguments={"replicas": 10})
    closed = SideEffectFlag(tool="scale_deployment", call_id="toolu_01", result={"ok": True})
    assert opened.call_id == closed.call_id
