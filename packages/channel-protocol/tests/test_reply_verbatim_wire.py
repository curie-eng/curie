"""ADR-0227: exact-answer updates use a closed, opt-in reply wire 1.2."""

import json

import pytest
from channel_protocol.reply import ReplyEvent, ReplyPost, ReplyUpdate
from pydantic import TypeAdapter, ValidationError

_EVENTS = TypeAdapter(ReplyEvent)
_ID = "00000000-0000-4000-8000-000000000001"
_TARGET = {
    "kind": "slack",
    "address": "C0EXAMPLE1",
    "conversation_id": "1720000000.000100",
    "reply_ref": "1720000000.000400",
}


def _update(**fields: object) -> dict[str, object]:
    return {"version": "1.2", "event": "reply.update", "target": _TARGET, **fields}


def _decode(body: dict[str, object]) -> ReplyEvent:
    return _EVENTS.validate_json(json.dumps(body))


def test_verbatim_update_round_trips_with_exact_text_and_version() -> None:
    text = '```curie-reply\n{"text":"leave this literal"}\n```\n🦊  '
    event = _decode(_update(text=text, text_mode="verbatim", delivery_id=_ID))
    assert isinstance(event, ReplyUpdate)
    assert event.version == "1.2"
    assert event.text == text
    assert event.text_mode == "verbatim"
    assert json.loads(event.model_dump_json()) == _update(
        text=text, text_mode="verbatim", delivery_id=_ID
    ) | {"message": None, "settled": None, "nav": None}


@pytest.mark.parametrize(
    "fields",
    [
        {"text": "ok", "text_mode": "verbatim"},
        {"text": "", "text_mode": "verbatim", "delivery_id": _ID},
        {"text_mode": "verbatim", "delivery_id": _ID},
        {"text": "ok", "delivery_id": _ID},
        {
            "text": "ok",
            "text_mode": "verbatim",
            "delivery_id": _ID,
            "nav": {"label": "Home", "command": "hub"},
        },
        {
            "text": "ok",
            "text_mode": "verbatim",
            "delivery_id": _ID,
            "message": {"version": "1.0", "text": "x"},
        },
        {
            "text": "ok",
            "text_mode": "verbatim",
            "delivery_id": _ID,
            "settled": {"requested_by": "U0EXAMPLE1"},
        },
        {
            "text": "ok",
            "text_mode": "verbatim",
            "delivery_id": _ID,
            "progress": {
                "kind": "card",
                "state": "testing",
                "summary": "x",
                "revision": 1,
                "terminal": False,
            },
        },
    ],
)
def test_verbatim_update_rejects_missing_or_competing_fields(fields: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _decode(_update(**fields))


@pytest.mark.parametrize("version", ["1.0", "1.1"])
def test_verbatim_mode_cannot_be_disguised_as_older_wire(version: str) -> None:
    with pytest.raises(ValidationError):
        _decode(_update(version=version, text="x", text_mode="verbatim", delivery_id=_ID))


def test_reply_post_never_accepts_wire_1_2() -> None:
    with pytest.raises(ValidationError):
        _decode(
            {
                "version": "1.2",
                "event": "reply.post",
                "target": _TARGET,
                "message": {"version": "1.0", "text": "x"},
                "requested_by": "U0EXAMPLE1",
                "delivery_id": _ID,
            }
        )


def test_old_wire_serialization_stays_byte_shape_compatible() -> None:
    for version, delivery_id in [("1.0", None), ("1.1", _ID)]:
        body = {"version": version, "event": "reply.update", "target": _TARGET, "text": "old"}
        if delivery_id is not None:
            body["delivery_id"] = delivery_id
        event = _decode(body)
        assert isinstance(event, ReplyUpdate)
        serialized = json.loads(event.model_dump_json())
        assert "text_mode" not in serialized
        assert "delivery_id" in serialized if delivery_id else "delivery_id" not in serialized
        assert serialized == body | {"message": None, "settled": None, "nav": None}


def test_reply_post_model_cannot_be_constructed_as_wire_1_2() -> None:
    with pytest.raises(ValidationError):
        ReplyPost(
            version="1.2",
            event="reply.post",
            target=_TARGET,
            message={"version": "1.0", "text": "x"},
            requested_by="U0EXAMPLE1",
        )  # type: ignore[arg-type]
