import hashlib
import uuid
from datetime import datetime
from typing import Literal

import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    ErrorEvent,
    Event,
    Final,
    InboundMessage,
    Interrupt,
    OutboundEvent,
    PublicationContext,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolNote,
    parse_inbound,
    to_inbound_json,
)
from aci_protocol.events import READER_CONTEXT, _AciModel
from pydantic import TypeAdapter, ValidationError

_OUTBOUND = TypeAdapter(OutboundEvent)
_INBOUND = TypeAdapter(InboundMessage)


def _publication_context() -> dict[str, object]:
    return {
        "agent_id": "11111111-1111-1111-1111-111111111111",
        "deployment_id": "22222222-2222-2222-2222-222222222222",
        "work_item_id": "33333333-3333-3333-3333-333333333333",
        "execution_request_id": "44444444-4444-4444-4444-444444444444",
        "runtime_epoch": 7,
        "conversation_id": "conversation-example",
        "lineage_id": "55555555-5555-5555-5555-555555555555",
        "lineage_version": 3,
        "expected_head": "a" * 40,
        "queued_event_id": "event-example",
        "precheck_url": "https://api.example.com/publications/precheck",
        "capability": "ppc.example.signature",
        "observed_title": "  Example title  ",
        "observed_body_sha256": hashlib.sha256(b"  Example body\n").hexdigest(),
        "observed_at": "2026-09-25T12:34:56Z",
    }


def _event_fields() -> dict[str, object]:
    return {
        "kind": "event",
        "type": "message",
        "text": "continue",
        "user": "U0EXAMPLE1",
        "ts": "1.0",
    }


class _Event_0_5_1(_AciModel):
    kind: Literal["event"] = "event"
    type: Literal["message", "job", "eval_case"]
    text: str
    user: str
    ts: str
    session_id: str | None = None
    history_ref: str | None = None


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


def test_publication_context_is_optional_for_events_without_a_factory_lineage() -> None:
    old_event = parse_inbound(_event_fields())
    assert isinstance(old_event, Event)
    assert old_event.publication_context is None

    explicit_null = Event.model_validate({**_event_fields(), "publication_context": None})
    assert explicit_null.publication_context is None


def test_publication_context_round_trips_with_exact_observation_and_identity() -> None:
    original = Event.model_validate(
        {**_event_fields(), "publication_context": _publication_context()}
    )
    restored = parse_inbound(to_inbound_json(original))

    assert isinstance(restored, Event)
    assert restored == original
    assert isinstance(restored.publication_context, PublicationContext)
    assert restored.publication_context.agent_id == uuid.UUID(
        "11111111-1111-1111-1111-111111111111"
    )
    assert restored.publication_context.execution_request_id == uuid.UUID(
        "44444444-4444-4444-4444-444444444444"
    )
    assert restored.publication_context.lineage_id == uuid.UUID(
        "55555555-5555-5555-5555-555555555555"
    )
    assert restored.publication_context.runtime_epoch == 7
    assert restored.publication_context.lineage_version == 3
    assert restored.publication_context.expected_head == "a" * 40
    assert restored.publication_context.observed_title == "  Example title  "
    assert restored.publication_context.observed_body_sha256 == hashlib.sha256(
        b"  Example body\n"
    ).hexdigest()
    observed_at = datetime.fromisoformat(
        str(restored.publication_context.observed_at).replace("Z", "+00:00")
    )
    offset = observed_at.utcoffset()
    assert offset is not None
    assert offset.total_seconds() == 0


def test_publication_context_rejects_unknown_producer_field() -> None:
    context = {**_publication_context(), "future_authority": "unexpected"}

    with pytest.raises(ValidationError):
        Event.model_validate({**_event_fields(), "publication_context": context})


def test_publication_context_consumer_ignores_unknown_nested_field() -> None:
    context = {**_publication_context(), "future_observation": "ignored"}
    decoded = parse_inbound(
        {**_event_fields(), "publication_context": context, "future_event_field": 1}
    )

    assert isinstance(decoded, Event)
    assert isinstance(decoded.publication_context, PublicationContext)
    assert decoded.publication_context.model_dump() == PublicationContext.model_validate(
        _publication_context()
    ).model_dump()
    assert "future_observation" not in decoded.publication_context.model_dump()
    assert "future_event_field" not in decoded.model_dump()


def test_previous_patch_consumer_can_decode_event_and_drop_optional_context() -> None:
    current = Event.model_validate(
        {**_event_fields(), "publication_context": _publication_context()}
    )
    previous = _Event_0_5_1.model_validate(
        current.model_dump(mode="json"), context=READER_CONTEXT
    )

    assert previous.text == current.text
    assert previous.user == current.user
    assert "publication_context" not in previous.model_dump()


@pytest.mark.parametrize("missing", tuple(_publication_context()))
def test_present_publication_context_requires_every_authority_field(missing: str) -> None:
    context = _publication_context()
    del context[missing]

    with pytest.raises(ValidationError):
        PublicationContext.model_validate(context)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("agent_id", "not a UUID"),
        ("deployment_id", "not a UUID"),
        ("work_item_id", "not a UUID"),
        ("execution_request_id", "not a UUID"),
        ("lineage_id", "not a UUID"),
        ("runtime_epoch", 0),
        ("runtime_epoch", -1),
        ("runtime_epoch", "7"),
        ("lineage_version", 0),
        ("lineage_version", -1),
        ("lineage_version", "3"),
        ("conversation_id", ""),
        ("queued_event_id", ""),
        ("capability", ""),
        ("precheck_url", "not a URL"),
        ("observed_at", "2026-09-25T12:34:56"),
    ),
)
def test_publication_context_rejects_invalid_authority_values(
    field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        PublicationContext.model_validate({**_publication_context(), field: value})


@pytest.mark.parametrize(
    "invalid_head",
    ("", "a" * 39, "a" * 41, "A" * 40, "g" * 40),
)
def test_publication_context_requires_lowercase_git_head(invalid_head: str) -> None:
    with pytest.raises(ValidationError):
        PublicationContext.model_validate(
            {**_publication_context(), "expected_head": invalid_head}
        )


@pytest.mark.parametrize(
    "invalid_digest",
    ("", "a" * 63, "a" * 65, "A" * 64, "g" * 64),
)
def test_publication_context_requires_lowercase_sha256_body_digest(
    invalid_digest: str,
) -> None:
    with pytest.raises(ValidationError):
        PublicationContext.model_validate(
            {**_publication_context(), "observed_body_sha256": invalid_digest}
        )


def test_present_malformed_publication_context_does_not_become_absence() -> None:
    with pytest.raises(ValidationError):
        parse_inbound({**_event_fields(), "publication_context": {"capability": "invalid"}})


@pytest.mark.parametrize("field", ("session_id", "history_ref"))
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
    assert legacy.approval_display is None


def test_awaiting_approval_display_is_optional_and_round_trips() -> None:
    """#2565: a rendered human sentence rides beside the machine summary.

    Omitted on older producers (default None). Present, it round-trips and does
    not replace approval_summary.
    """

    final = Final(
        text="Requesting sign-off",
        status=SessionStatus.AWAITING_APPROVAL,
        approval_summary="Tool call awaiting approval: Bash {\"command\": \"ls\"}",
        approval_display="Run ls. Approve?",
    )
    wire = final.model_dump(mode="json")
    assert wire["approval_display"] == "Run ls. Approve?"
    assert wire["approval_summary"].startswith("Tool call awaiting approval:")
    assert Final.model_validate(wire) == final


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
