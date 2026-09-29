"""Reply wire 1.1: the outbound delivery identity and the progress payload.

ADR-0130 section 4 puts a platform-minted ``delivery_id`` on the neutral reply
wire behind a minor version increment, and ADR-0130 sections 3 and 5 keep
progress from ever being read as an answer or an approval. Every rule here is
proved through ``TypeAdapter(ReplyEvent)``, the decoder the Discord and mail
adapters run on each POST, and each refusal names its reason, because several of
these bodies were already refused before 1.1 existed, for the wrong one (an
unknown key or an unknown version).
"""

from __future__ import annotations

import json
import uuid
from typing import Any, get_args

import pytest
from channel_protocol import reply as reply_wire
from channel_protocol.reply import (
    REPLY_WIRE_VERSION,
    ReplyEvent,
    ReplyTarget,
    ReplyUpdate,
    TurnCompleted,
    TurnStatus,
)
from channel_protocol.schema_export import render_schema
from pydantic import TypeAdapter, ValidationError

_EVENTS: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)
_ID = "00000000-0000-4000-8000-000000000001"
# Carries hex letters, so its uppercase form differs from it.
_LETTERED_ID = "00000000-0000-4000-8000-0000000000ab"
_TARGET: dict[str, Any] = {
    "kind": "slack",
    "address": "C0EXAMPLE1",
    "conversation_id": "1720000000.000100",
    "reply_ref": "1720000000.000400",
}
_CARD: dict[str, Any] = {
    "kind": "card",
    "state": "testing",
    "summary": "Running the integration suite",
    "revision": 3,
    "terminal": False,
}
_MILESTONE: dict[str, Any] = {
    "kind": "milestone",
    "milestone": "evidence",
    "summary": "Found the failing migration in the deploy log",
    "ordinal": 1,
}


def _message(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"version": "1.0", "text": "Found the failing migration"}
    body.update(overrides)
    return body


def _update(**fields: Any) -> dict[str, Any]:
    return {"version": "1.1", "event": "reply.update", "target": _TARGET, **fields}


def _post(**fields: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "version": "1.1",
        "event": "reply.post",
        "target": _TARGET,
        "message": _message(),
        "requested_by": "U0EXAMPLE1",
    }
    body.update(fields)
    return body


def _decode(body: dict[str, Any]) -> ReplyEvent:
    return _EVENTS.validate_json(json.dumps(body))


def _refusal(body: dict[str, Any]) -> list[dict[str, Any]]:
    with pytest.raises(ValidationError) as caught:
        _decode(body)
    return [dict(error) for error in caught.value.errors()]


def _refused_by_rule(body: dict[str, Any], error_type: str, fragment: str = "") -> None:
    errors = _refusal(body)
    assert any(
        error["type"] == error_type and fragment in str(error["msg"]) for error in errors
    ), errors
    # An adapter that answers 422 with these errors (the Discord adapter does)
    # must be able to encode them.
    json.dumps(errors)


# --- versions -------------------------------------------------------------------


def test_the_wire_has_two_versions_and_1_0_stays_the_default() -> None:
    assert get_args(reply_wire.ReplyWireVersion) == ("1.0", "1.1")
    assert REPLY_WIRE_VERSION == "1.0"
    assert reply_wire.PROGRESS_REPLY_WIRE_VERSION == "1.1"


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(
            {"version": "1.1", "event": "turn.status", "target": _TARGET, "status": "working"},
            id="turn.status",
        ),
        pytest.param(
            {
                "version": "1.1",
                "event": "turn.completed",
                "target": _TARGET,
                "event_id": "chn-example-0001",
                "outcome": "delivered",
            },
            id="turn.completed",
        ),
    ],
)
def test_an_event_with_no_1_1_field_is_1_0_only(body: dict[str, Any]) -> None:
    errors = _refusal(body)
    assert [error["type"] for error in errors] == ["literal_error"]
    assert errors[0]["loc"][-1] == "version"


def test_the_schema_says_which_events_have_a_1_1_form() -> None:
    defs = json.loads(render_schema())["$defs"]
    for name in ("TurnStatus", "TurnCompleted"):
        assert defs[name]["properties"]["version"]["const"] == "1.0", name
    for name in ("ReplyUpdate", "ReplyPost"):
        assert defs[name]["properties"]["version"]["enum"] == ["1.0", "1.1"], name
        assert {"delivery_id", "progress"} <= set(defs[name]["properties"]), name
        assert "delivery_id" not in defs[name].get("required", []), name


# --- the version rule -----------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(_update(version="1.0", text="hi", delivery_id=_ID), id="update-delivery-id"),
        pytest.param(_post(version="1.0", delivery_id=_ID), id="post-delivery-id"),
        pytest.param(
            _update(version="1.0", delivery_id=_ID, progress=_CARD), id="update-progress"
        ),
        pytest.param(
            _post(version="1.0", delivery_id=_ID, progress=_MILESTONE), id="post-progress"
        ),
    ],
)
def test_a_1_0_body_carries_no_1_1_field(body: dict[str, Any]) -> None:
    _refused_by_rule(body, "reply_wire_version", "need reply wire 1.1")


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(_update(text="hi"), id="update"),
        pytest.param(_post(), id="post"),
    ],
)
def test_a_1_1_body_carries_a_delivery_id(body: dict[str, Any]) -> None:
    # Otherwise a producer could label an ordinary body 1.1 and a 1.0-built
    # adapter would refuse a body it could have read.
    _refused_by_rule(body, "reply_wire_version", "carries a delivery_id")


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(_update(progress=_CARD), id="card-update"),
        pytest.param(_post(progress=_CARD), id="card-post"),
        pytest.param(_post(progress=_MILESTONE), id="milestone-post"),
    ],
)
def test_progress_needs_a_delivery_id(body: dict[str, Any]) -> None:
    _refused_by_rule(body, "progress_delivery_id")


# --- the accepted 1.1 forms ------------------------------------------------------


def test_a_card_update_decodes_with_its_delivery_identity() -> None:
    event = _decode(_update(delivery_id=_ID, progress=_CARD))
    assert isinstance(event, ReplyUpdate)
    assert event.delivery_id == _ID
    assert event.progress is not None
    assert event.progress.state == "testing"
    assert event.progress.revision == 3
    assert event.text is None and event.message is None


def test_a_card_and_a_milestone_both_post() -> None:
    card = _decode(_post(delivery_id=_ID, progress=_CARD))
    milestone = _decode(_post(delivery_id=_ID, progress=_MILESTONE))
    assert type(card.progress).__name__ == "ProgressCard"  # type: ignore[union-attr]
    assert type(milestone.progress).__name__ == "ProgressMilestone"  # type: ignore[union-attr]


def test_an_existing_form_may_carry_a_delivery_identity_alone() -> None:
    # ADR-0130 section 4: optional for existing reply forms, mandatory for
    # progress. An approval card post or an answer edit can carry one.
    post = _decode(_post(delivery_id=_ID))
    update = _decode(_update(text="The answer is 42.", delivery_id=_ID))
    assert post.delivery_id == _ID  # type: ignore[union-attr]
    assert post.progress is None  # type: ignore[union-attr]
    assert update.delivery_id == _ID  # type: ignore[union-attr]


def test_an_update_cannot_carry_a_milestone() -> None:
    # A milestone is a new durable message; only the card is edited in place.
    errors = _refusal(_update(delivery_id=_ID, progress=_MILESTONE))
    assert any(
        error["type"] == "literal_error" and error["loc"][-2:] == ("progress", "kind")
        for error in errors
    ), errors


# --- progress is never an answer or an approval ---------------------------------


@pytest.mark.parametrize(
    "answer_field",
    [
        pytest.param({"text": "the answer"}, id="text"),
        pytest.param({"text": ""}, id="empty-text"),
        pytest.param({"message": _message()}, id="message"),
        pytest.param(
            {"message": _message(), "settled": {"requested_by": "U0EXAMPLE1"}},
            id="message-and-settled",
        ),
        pytest.param({"settled": {"requested_by": "U0EXAMPLE1"}}, id="settled"),
        pytest.param({"nav": {"label": "Home", "command": "hub"}}, id="nav"),
    ],
)
def test_a_progress_update_carries_no_answer_field(answer_field: dict[str, Any]) -> None:
    # A Slack reply.update carrying a message and no decision renders the
    # expired approval card, and the mail egress replaces its buffered answer
    # with an update's text: a progress edit must give neither path anything.
    _refused_by_rule(
        _update(delivery_id=_ID, progress=_CARD, **answer_field),
        "progress_not_an_answer",
    )


@pytest.mark.parametrize("progress", [_CARD, _MILESTONE], ids=["card", "milestone"])
def test_a_progress_post_is_never_actionable(progress: dict[str, Any]) -> None:
    confirm = {
        "kind": "confirm",
        "id": "00000000-0000-4000-8000-00000000a001",
        "prompt": "Deploy?",
        "confirm": {"label": "Approve", "value": "approve"},
        "cancel": {"label": "Reject", "value": "reject"},
    }
    _refused_by_rule(
        _post(delivery_id=_ID, progress=progress, message=_message(interaction=confirm)),
        "progress_not_actionable",
    )


def test_an_approval_card_without_progress_stays_actionable() -> None:
    # The interaction rule is scoped to progress: the approval card itself,
    # with or without a delivery identity, keeps its confirm intent.
    confirm = {
        "kind": "confirm",
        "id": "00000000-0000-4000-8000-00000000a001",
        "prompt": "Deploy?",
        "confirm": {"label": "Approve", "value": "approve"},
        "cancel": {"label": "Reject", "value": "reject"},
    }
    event = _decode(_post(delivery_id=_ID, message=_message(interaction=confirm)))
    assert event.message.interaction is not None  # type: ignore[union-attr]


# --- the delivery identity is a canonical UUID ----------------------------------


def test_a_freshly_minted_uuid_is_a_valid_delivery_id() -> None:
    minted = str(uuid.uuid4())
    assert _decode(_update(text="hi", delivery_id=minted)).delivery_id == minted  # type: ignore[union-attr]
    assert _decode(_update(text="hi", delivery_id=_LETTERED_ID)).delivery_id == _LETTERED_ID  # type: ignore[union-attr]


@pytest.mark.parametrize(
    "delivery_id",
    [
        pytest.param(_LETTERED_ID.upper(), id="uppercase"),
        pytest.param(_ID.replace("-", ""), id="no-hyphens"),
        pytest.param("{" + _ID + "}", id="braces"),
        pytest.param("urn:uuid:" + _ID, id="urn"),
        pytest.param(_ID[:-1], id="short"),
        pytest.param("", id="empty"),
        pytest.param("msg_01H0000000000000000000000", id="an-inbound-style-id"),
    ],
)
def test_a_delivery_id_must_be_canonical(delivery_id: str) -> None:
    errors = _refusal(_update(text="hi", delivery_id=delivery_id))
    assert [error["type"] for error in errors] == ["string_pattern_mismatch"]


# --- 1.0 serialization is unchanged ---------------------------------------------


def test_a_1_0_reply_built_the_way_the_worker_builds_it_serializes_as_before() -> None:
    # The corpus proves decode-then-encode; this is the other path, the one the
    # worker takes (construct, then model_dump_json) for the same body. The
    # expected bytes are the corpus's reply-update-text case, captured from the
    # 1.0-only package.
    event = ReplyUpdate(
        version=REPLY_WIRE_VERSION,
        event="reply.update",
        target=ReplyTarget(
            kind="email",
            address="agent@example.com",
            conversation_id="thr_example_1",
            reply_ref="msg_example_1",
        ),
        text="The answer is 42.",
    )
    assert event.model_dump_json() == (
        '{"version":"1.0","target":{"kind":"email","address":"agent@example.com",'
        '"conversation_id":"thr_example_1","reply_ref":"msg_example_1"},'
        '"event":"reply.update","text":"The answer is 42.","message":null,'
        '"settled":null,"nav":null}'
    )


@pytest.mark.parametrize(
    "body",
    [
        pytest.param(
            {"version": "1.0", "event": "reply.update", "target": _TARGET, "text": "hi"},
            id="update",
        ),
        pytest.param(_post(version="1.0"), id="post"),
    ],
)
def test_a_1_0_body_omits_the_1_1_keys_in_every_serialization(body: dict[str, Any]) -> None:
    event = _decode(body)
    assert not {"delivery_id", "progress"} & set(json.loads(event.model_dump_json()))
    assert not {"delivery_id", "progress"} & set(event.model_dump())
    assert not {"delivery_id", "progress"} & set(event.model_dump(mode="json"))


def test_a_1_1_body_serializes_its_1_1_keys() -> None:
    event = _decode(_update(delivery_id=_ID, progress=_CARD))
    emitted = json.loads(event.model_dump_json())
    assert emitted["delivery_id"] == _ID
    assert emitted["progress"] == _CARD


def test_status_and_completion_are_unchanged_1_0_events() -> None:
    status = TurnStatus(
        version=REPLY_WIRE_VERSION,
        event="turn.status",
        target=ReplyTarget(**_TARGET),
        status="working",
    )
    completed = TurnCompleted(
        version=REPLY_WIRE_VERSION,
        event="turn.completed",
        target=ReplyTarget(**_TARGET),
        event_id="chn-example-0001",
        outcome="delivered",
    )
    assert set(TurnStatus.model_fields) == {"version", "target", "event", "status"}
    assert set(TurnCompleted.model_fields) == {"version", "target", "event", "event_id", "outcome"}
    assert _decode(json.loads(status.model_dump_json())) == status
    assert _decode(json.loads(completed.model_dump_json())) == completed


def test_the_progress_models_are_exported_in_the_committed_schema() -> None:
    defs = json.loads(render_schema())["$defs"]
    for name in (
        "ProgressCommand",
        "ProgressCard",
        "ProgressMilestone",
        "ProgressState",
        "MilestoneClass",
    ):
        assert name in defs, f"{name} is missing from the exported channel-protocol schema"
    assert defs["ProgressCommand"]["additionalProperties"] is False
