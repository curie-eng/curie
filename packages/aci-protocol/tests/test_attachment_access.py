"""Optional scoped attachment transport; no runtime enforcement is implied."""

import json

import pytest
from aci_protocol import BootEnv, Budget, Event, SessionConfig, parse_inbound, to_inbound_json
from aci_protocol.events import READER_CONTEXT
from pydantic import ValidationError

_FRAME = {"type": "message", "text": "Inspect the attached file", "user": "U0EXAMPLE1", "ts": "1.0"}
_ACCESS = {
    "url": "https://api.example.com/attachment-access",
    "token": "attachment.example.signature",
}


def test_attachment_authority_roundtrips_without_debug_disclosure() -> None:
    produced = Event.model_validate({**_FRAME, "attachment_access": _ACCESS})
    consumed = parse_inbound(to_inbound_json(produced))
    assert isinstance(consumed, Event)
    assert consumed.attachment_access is not None
    assert consumed.attachment_access.url == "https://api.example.com/attachment-access"
    assert consumed.attachment_access.token == "attachment.example.signature"
    assert _ACCESS["token"] not in repr(consumed)
    assert _ACCESS["token"] not in repr(consumed.attachment_access)


def test_old_event_has_no_attachment_authority() -> None:
    consumed = parse_inbound(json.dumps({"kind": "event", **_FRAME}))
    assert isinstance(consumed, Event)
    assert consumed.attachment_access is None


@pytest.mark.parametrize("access", [None, _ACCESS])
def test_nullable_authority_is_explicit_on_the_wire(access: object) -> None:
    event = Event.model_validate({**_FRAME, "attachment_access": access})
    assert json.loads(to_inbound_json(event))["attachment_access"] == access


@pytest.mark.parametrize(
    "access",
    [
        {},
        {"url": _ACCESS["url"]},
        {"token": _ACCESS["token"]},
        {**_ACCESS, "token": ""},
        {**_ACCESS, "token": 1},
        {**_ACCESS, "url": "file:///etc/passwd"},
        {**_ACCESS, "url": "not-a-url"},
    ],
)
def test_malformed_attachment_authority_is_not_silently_dropped(access: object) -> None:
    with pytest.raises(ValidationError):
        parse_inbound(json.dumps({"kind": "event", **_FRAME, "attachment_access": access}))


def test_attachment_reader_tolerates_future_metadata_but_producer_does_not() -> None:
    frame = {**_FRAME, "attachment_access": {**_ACCESS, "future": True}}
    with pytest.raises(ValidationError):
        Event.model_validate(frame)
    consumed = Event.model_validate(frame, context=READER_CONTEXT)
    assert consumed.attachment_access is not None
    assert consumed.attachment_access.token == _ACCESS["token"]


def test_optional_attachment_tool_switch_roundtrips_without_authority() -> None:
    env = SessionConfig(
        plugin_dir="/plugins/acme",
        session_id="thread-example",
        sandbox_id="sandbox-example",
        budget=Budget(max_output_tokens_per_run=100, max_usd_per_day=1.0),
    ).to_env()
    boot = BootEnv.from_env({**env, "CURIE_ATTACHMENT_ACCESS_ENABLED": "1"})
    assert boot.attachment_access_enabled is True
    assert boot.to_env()["CURIE_ATTACHMENT_ACCESS_ENABLED"] == "1"
    assert BootEnv.from_env(env).attachment_access_enabled is None
    assert "CURIE_ATTACHMENT_ACCESS_ENABLED" in BootEnv.env_keys(producer="worker")
