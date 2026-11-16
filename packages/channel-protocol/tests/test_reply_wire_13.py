"""Reply wire 1.3: a replace edit and a private post.

Both forms are optional about ``delivery_id``. A wrong 1.3 shape is
``replace_shape`` or ``audience_shape``, ahead of the 1.1 progress rules and
the 1.2 ``text_mode`` rule. A 1.3 field on an older body is ``reply_wire_version``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from channel_protocol.reply import ReplyEvent, ReplyPost, ReplyUpdate
from pydantic import TypeAdapter, ValidationError

_EVENTS: TypeAdapter[ReplyEvent] = TypeAdapter(ReplyEvent)
_ID = "00000000-0000-4000-8000-000000000001"
_TARGET: dict[str, Any] = {
    "kind": "slack",
    "address": "C0EXAMPLE1",
    "conversation_id": "1720000000.000100",
    "reply_ref": "1720000000.000400",
}
_MESSAGE: dict[str, Any] = {
    "version": "1.0",
    "text": "replacement body",
    "header": "Replacement heading",
}
_CARD: dict[str, Any] = {
    "kind": "card",
    "state": "testing",
    "summary": "Running the integration suite",
    "revision": 3,
    "terminal": False,
}
_CONFIRM: dict[str, Any] = {
    "kind": "confirm",
    "id": "00000000-0000-4000-8000-00000000a001",
    "prompt": "Deploy?",
    "confirm": {"label": "Approve", "value": "approve"},
    "cancel": {"label": "Reject", "value": "reject"},
    "allow_free_text": False,
}


def _replace(**fields: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "version": "1.3",
        "event": "reply.update",
        "target": _TARGET,
        "message": _MESSAGE,
        "message_mode": "replace",
    }
    body.update(fields)
    return body


def _audience(**fields: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "version": "1.3",
        "event": "reply.post",
        "target": {**_TARGET, "reply_ref": None},
        "message": {"version": "1.0", "text": "only you can see this"},
        "requested_by": "U0EXAMPLE1",
        "audience_user": "U0EXAMPLE1",
    }
    body.update(fields)
    return body


def _errors(body: dict[str, Any]) -> list[dict[str, Any]]:
    with pytest.raises(ValidationError) as caught:
        _EVENTS.validate_json(json.dumps(body))
    return [dict(error) for error in caught.value.errors()]


def test_wire_13_replace_and_audience_round_trip() -> None:
    replace = _EVENTS.validate_json(json.dumps(_replace()))
    audience = _EVENTS.validate_json(json.dumps(_audience()))
    assert isinstance(replace, ReplyUpdate)
    assert isinstance(audience, ReplyPost)
    assert replace.version == "1.3" and replace.message_mode == "replace"
    assert audience.version == "1.3" and audience.audience_user == "U0EXAMPLE1"
    assert json.loads(replace.model_dump_json())["message_mode"] == "replace"
    assert "delivery_id" not in json.loads(replace.model_dump_json())
    assert "audience_user" not in json.loads(replace.model_dump_json())
    again = _EVENTS.validate_json(audience.model_dump_json())
    assert again == audience
    assert replace == _EVENTS.validate_json(replace.model_dump_json())


def test_wire_13_delivery_id_may_be_present_and_absent_fields_are_omitted() -> None:
    event = _EVENTS.validate_json(json.dumps(_replace(delivery_id=_ID)))
    assert isinstance(event, ReplyUpdate)
    assert event.delivery_id == _ID
    bare = ReplyUpdate(
        version="1.0",
        event="reply.update",
        target=_TARGET,
        text="hi",
    )
    dumped = json.loads(bare.model_dump_json())
    assert "message_mode" not in dumped
    assert "audience_user" not in dumped


@pytest.mark.parametrize(
    ("body", "error_type"),
    [
        pytest.param(_replace(version="1.0"), "reply_wire_version", id="message-mode-1-0"),
        pytest.param(
            _replace(version="1.1", delivery_id=_ID), "reply_wire_version", id="message-mode-1-1"
        ),
        pytest.param(
            {
                "version": "1.2",
                "event": "reply.update",
                "target": _TARGET,
                "text": "exact",
                "text_mode": "verbatim",
                "delivery_id": _ID,
                "message_mode": "replace",
            },
            "reply_wire_version",
            id="message-mode-1-2",
        ),
        pytest.param(_audience(version="1.0"), "reply_wire_version", id="audience-1-0"),
        pytest.param(
            _audience(version="1.1", delivery_id=_ID), "reply_wire_version", id="audience-1-1"
        ),
        pytest.param(
            {key: value for key, value in _replace().items() if key != "message_mode"},
            "reply_wire_version",
            id="update-without-message-mode",
        ),
        pytest.param(
            {key: value for key, value in _audience().items() if key != "audience_user"},
            "reply_wire_version",
            id="post-without-audience",
        ),
        pytest.param(_replace(text="nope"), "replace_shape", id="replace-text"),
        pytest.param(
            _replace(settled={"requested_by": "U0EXAMPLE1"}), "replace_shape", id="replace-settled"
        ),
        pytest.param(
            _replace(nav={"label": "Home", "command": "hub"}), "replace_shape", id="replace-nav"
        ),
        pytest.param(_replace(progress=_CARD), "replace_shape", id="replace-progress"),
        pytest.param(_replace(text_mode="verbatim"), "replace_shape", id="replace-text-mode"),
        pytest.param(
            {key: value for key, value in _replace().items() if key != "message"},
            "replace_shape",
            id="replace-no-message",
        ),
        pytest.param(
            _replace(target={**_TARGET, "reply_ref": None}),
            "replace_shape",
            id="replace-null-ref",
        ),
        pytest.param(_audience(progress=_CARD), "audience_shape", id="audience-progress"),
        pytest.param(
            _audience(message={"version": "1.0", "text": "hi", "interaction": _CONFIRM}),
            "audience_shape",
            id="audience-interaction",
        ),
    ],
)
def test_wire_13_invalid_shape_names_its_error(body: dict[str, Any], error_type: str) -> None:
    errors = _errors(body)
    assert any(error["type"] == error_type for error in errors), errors
    json.dumps(errors)


@pytest.mark.parametrize(
    ("audience_user", "error_type"),
    [
        pytest.param("u0example", "string_pattern_mismatch", id="pattern"),
        pytest.param("", "string_too_short", id="empty"),
        pytest.param("A" * 65, "string_too_long", id="long"),
    ],
)
def test_wire_13_malformed_audience_user_fails_the_constraint(
    audience_user: str, error_type: str
) -> None:
    errors = _errors(_audience(audience_user=audience_user))
    assert any(
        error["type"] == error_type and error["loc"][-1] == "audience_user" for error in errors
    ), errors


def test_wire_13_older_forms_still_validate() -> None:
    update = _EVENTS.validate_json(
        json.dumps({"version": "1.0", "event": "reply.update", "target": _TARGET, "text": "hi"})
    )
    post = _EVENTS.validate_json(
        json.dumps(
            {
                "version": "1.1",
                "event": "reply.post",
                "target": _TARGET,
                "message": {"version": "1.0", "text": "Queued"},
                "requested_by": "U0EXAMPLE1",
                "delivery_id": _ID,
                "progress": {**_CARD, "state": "queued", "summary": "Waiting", "revision": 1},
            }
        )
    )
    assert isinstance(update, ReplyUpdate) and update.version == "1.0"
    assert isinstance(post, ReplyPost) and post.version == "1.1" and post.progress is not None
