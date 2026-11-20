"""ADR 0272: an approval card in the selected approver's DM, through the dispatcher.

The click, the note modal and its submission run through the actual Socket Mode
handler and the selected receiver's answer path (real Valkey, synthetic API edge).
Slack payloads and Web API responses below replay the provider's documented
shapes for an IM conversation (``channel.id`` is a ``D...`` id). Nothing here
qualifies an actual Slack installation or its ``im:history`` grant; ADR 0272
evidence item 2 keeps that as a live check.

@spec APPROVER-DM-2 APPROVER-DM-3
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from curie_channel_gateway.slack.approval_actions import (
    APPROVE_ACTION_ID,
    APPROVE_NOTE_ACTION_ID,
    NOTE_MODAL_CALLBACK_ID,
    _fetch_card_message,
)
from curie_channel_gateway.slack.config import DispatcherConfig
from curie_channel_gateway.slack.preflight import check_slack_channel_capabilities
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web.slack_response import SlackResponse

from .conftest import FakeSocketClient
from .test_http_receiver import SENDER, HttpRig, api
from .test_http_receiver_answers import PRINCIPAL_HEADER, _proof, _resolved
from .test_http_receiver_dispatch import APPROVAL, _finish_owned_listeners, _selected
from .test_preflight import CHANNEL_A, CHANNEL_C, _agent, _client, _config
from .test_preflight_identities import DEFAULT, _IdentityClient

DM = "D0EXAMPLE1"
CARD_TS = "1700000000.000200"
TEAM = "T1"

# The approval card as Slack stores it in the DM (the shape conversations.replies
# and a block_actions ``message`` both carry).
CARD_MESSAGE: dict[str, Any] = {
    "type": "message",
    "subtype": "bot_message",
    "text": "Approve the synthetic operation",
    "ts": CARD_TS,
    "bot_id": "B0EXAMPLE1",
    "app_id": "A1",
    "team": TEAM,
    "blocks": [
        {"type": "header", "block_id": "h", "text": {"type": "plain_text", "text": "Approval"}},
        {
            "type": "section",
            "block_id": "s",
            "text": {"type": "mrkdwn", "text": "Approve the synthetic operation"},
        },
        {
            "type": "context",
            "block_id": "c",
            "elements": [{"type": "mrkdwn", "text": "Requested by <@U0EXAMPLE9>"}],
        },
        {
            "type": "actions",
            "block_id": "a",
            "elements": [
                {
                    "type": "button",
                    "action_id": APPROVE_ACTION_ID,
                    "text": {"type": "plain_text", "text": "Approve"},
                    "style": "primary",
                    "value": APPROVAL,
                },
            ],
        },
    ],
}


def dm_click(envelope: str, *, action_id: str = APPROVE_ACTION_ID) -> SocketModeRequest:
    """A block_actions interaction from a button on a card in an IM conversation."""
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope,
        payload={
            "type": "block_actions",
            "user": {"id": SENDER, "username": "approver", "name": "approver", "team_id": TEAM},
            "api_app_id": "A1",
            "token": "verif",
            "container": {
                "type": "message",
                "message_ts": CARD_TS,
                "channel_id": DM,
                "is_ephemeral": False,
            },
            "trigger_id": f"1700000001.{envelope}.trigger",
            "team": {"id": TEAM, "domain": "example"},
            "enterprise": None,
            "is_enterprise_install": False,
            "channel": {"id": DM, "name": "directmessage"},
            "message": dict(CARD_MESSAGE),
            "state": {"values": {}},
            "response_url": "https://hooks.slack.com/actions/T1/1/EXAMPLE",
            "actions": [
                {
                    "type": "button",
                    "action_id": action_id,
                    "block_id": "a",
                    "text": {"type": "plain_text", "text": "Approve"},
                    "value": APPROVAL,
                    "style": "primary",
                    "action_ts": "1700000001.000300",
                }
            ],
        },
    )


def dm_submission(envelope: str, *, note: str = "Looks right") -> SocketModeRequest:
    """A view_submission of the note modal opened from the DM card."""
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope,
        payload={
            "type": "view_submission",
            "team": {"id": TEAM, "domain": "example"},
            "user": {"id": SENDER, "username": "approver", "name": "approver", "team_id": TEAM},
            "api_app_id": "A1",
            "token": "verif",
            "trigger_id": f"1700000002.{envelope}.trigger",
            "enterprise": None,
            "is_enterprise_install": False,
            "view": {
                "id": "V0EXAMPLE1",
                "team_id": TEAM,
                "type": "modal",
                "callback_id": NOTE_MODAL_CALLBACK_ID,
                "private_metadata": json.dumps(
                    {
                        "approval_id": APPROVAL,
                        "channel": DM,
                        "card_ts": CARD_TS,
                        "decision": "approved",
                    }
                ),
                "state": {
                    "values": {
                        "note": {"note-input": {"type": "plain_text_input", "value": note}}
                    }
                },
                "title": {"type": "plain_text", "text": "Approve request"},
                "blocks": [],
                "app_id": "A1",
                "bot_id": "B0EXAMPLE1",
            },
            "response_urls": [],
        },
    )


def replies_response(client: Any, data: dict[str, Any]) -> SlackResponse:
    """A conversations.replies answer, validated the way the SDK validates one: an
    ``ok: false`` body raises ``SlackApiError`` exactly as the real client does."""
    return SlackResponse(
        client=client,
        http_verb="POST",
        api_url="https://slack.com/api/conversations.replies",
        req_args={},
        data=data,
        headers={},
        status_code=200,
    ).validate()


REPLIES_OK = {
    "ok": True,
    "messages": [CARD_MESSAGE],
    "has_more": False,
    "response_metadata": {"next_cursor": ""},
}
# What Slack answers when the bot token lacks the IM history scope.
MISSING_SCOPE = {
    "ok": False,
    "error": "missing_scope",
    "needed": "im:history",
    "provided": "chat:write",
}


def _handle(app: Any, request: SocketModeRequest) -> FakeSocketClient:
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, request)
    finally:
        _finish_owned_listeners(app)
    return sock


def _attested(request: httpx.Request) -> dict[str, Any]:
    """Decode the chat attestation's claims independently of its minting code."""
    _prefix, segment, _signature = request.headers[PRINCIPAL_HEADER].split(".")
    return json.loads(base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4)))


def _approved_rig() -> HttpRig:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_resolved())

    return HttpRig(handler)


def test_dm_button_click_answers_with_the_dm_channel_and_stamps_the_dm_card(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec APPROVER-DM-3: the proof's interaction channel is the DM, unchanged path."""
    api()
    rig = _approved_rig()
    app, legacy = _selected(config, redis_client, rig)
    sock = _handle(app, dm_click("env-dm-click"))
    assert sock.acked_envelope_ids == ["env-dm-click"]
    assert legacy.calls == []
    assert len(rig.requests) == 1
    request = rig.requests[0]
    assert str(request.url).endswith(f"/approvals/{APPROVAL}/resolve")
    assert json.loads(request.content) == {"decision": "approved"}
    proof = _proof(request)
    assert proof.native_channel_id == DM
    assert proof.approval_id == APPROVAL
    assert proof.slack.delivery == "block_actions"
    assert proof.slack.user_id == SENDER and proof.slack.interaction_user_team_id == TEAM
    assert proof.body_sha256 == hashlib.sha256(request.content).hexdigest()
    claims = _attested(request)
    assert (claims["actor_channel"], claims["sub"], claims["approval_id"]) == (
        DM, SENDER, APPROVAL,
    )
    rig.web.chat_update.assert_called_once()
    assert rig.web.chat_update.call_args.kwargs["channel"] == DM
    assert rig.web.chat_update.call_args.kwargs["ts"] == CARD_TS
    rig.closed()


def test_dm_note_click_opens_the_modal_with_the_dm_channel_and_trigger(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec APPROVER-DM-3: the modal carries the DM channel for its submission."""
    api()
    rig = HttpRig()
    app, legacy = _selected(config, redis_client, rig)
    request = dm_click("env-dm-open", action_id=APPROVE_NOTE_ACTION_ID)
    sock = _handle(app, request)
    assert sock.acked_envelope_ids == ["env-dm-open"]
    rig.web.views_open.assert_called_once()
    kwargs = rig.web.views_open.call_args.kwargs
    assert kwargs["trigger_id"] == request.payload["trigger_id"]
    meta = json.loads(kwargs["view"]["private_metadata"])
    assert meta == {
        "approval_id": APPROVAL, "channel": DM, "card_ts": CARD_TS, "decision": "approved",
    }
    assert rig.requests == []
    assert legacy.calls == []


def test_dm_note_submission_answers_with_the_dm_channel_and_reads_the_card_back(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec APPROVER-DM-2/3: the submission resolves with the DM channel, then the
    card is re-read with conversations.replies in the DM and stamped there."""
    api()
    rig = _approved_rig()
    app, legacy = _selected(config, redis_client, rig)
    rig.web.conversations_replies = MagicMock(
        side_effect=lambda **_kwargs: replies_response(rig.web, REPLIES_OK)
    )
    sock = _handle(app, dm_submission("env-dm-note"))
    assert sock.acked_envelope_ids == ["env-dm-note"]
    assert legacy.calls == []
    assert len(rig.requests) == 1
    request = rig.requests[0]
    assert json.loads(request.content) == {"decision": "approved", "note": "Looks right"}
    proof = _proof(request)
    assert proof.native_channel_id == DM
    assert proof.slack.delivery == "view_submission"
    assert _attested(request)["actor_channel"] == DM
    rig.web.conversations_replies.assert_called_once_with(
        channel=DM, ts=CARD_TS, inclusive=True, limit=1
    )
    rig.web.chat_update.assert_called_once()
    assert rig.web.chat_update.call_args.kwargs["channel"] == DM
    assert rig.web.chat_update.call_args.kwargs["ts"] == CARD_TS
    rig.closed()


def test_fetch_card_message_reads_the_dm_card_with_conversations_replies() -> None:
    """@spec APPROVER-DM-2: the settlement read is conversations.replies (im:history)."""
    web = MagicMock()
    web.conversations_replies = MagicMock(
        side_effect=lambda **_kwargs: replies_response(web, REPLIES_OK)
    )
    message = _fetch_card_message(
        web, channel=DM, card_ts=CARD_TS, log=logging.getLogger("test-approver-dm")
    )
    web.conversations_replies.assert_called_once_with(
        channel=DM, ts=CARD_TS, inclusive=True, limit=1
    )
    assert message["ts"] == CARD_TS
    assert message["blocks"] == CARD_MESSAGE["blocks"]


def _missing_scope_warnings(caplog: Any) -> list[str]:
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno >= logging.WARNING and "missing_scope" in record.getMessage()
    ]


def test_missing_im_history_scope_is_reported_as_a_failed_settlement_read(
    caplog: Any,
) -> None:
    """@spec APPROVER-DM-2: a DM settlement read without im:history is a reported
    failure, never a silent empty read."""
    web = MagicMock()
    # The SDK itself turns the recorded body into its exception, as the real client does.
    with pytest.raises(SlackApiError):
        replies_response(web, MISSING_SCOPE)
    web.conversations_replies = MagicMock(
        side_effect=lambda **_kwargs: replies_response(web, MISSING_SCOPE)
    )
    caplog.set_level(logging.WARNING)
    message = _fetch_card_message(
        web, channel=DM, card_ts=CARD_TS, log=logging.getLogger("test-approver-dm")
    )
    assert message == {}
    assert _missing_scope_warnings(caplog), "missing_scope was not reported"


def test_missing_im_history_scope_after_a_dm_submission_reports_and_never_stamps(
    config: DispatcherConfig, redis_client: redis.Redis, caplog: Any,
) -> None:
    """@spec APPROVER-DM-2: the decision stands, the card is left unstamped, and the
    missing scope is named in the report."""
    api()
    rig = _approved_rig()
    app, _legacy = _selected(config, redis_client, rig)
    rig.web.conversations_replies = MagicMock(
        side_effect=lambda **_kwargs: replies_response(rig.web, MISSING_SCOPE)
    )
    caplog.set_level(logging.WARNING)
    _handle(app, dm_submission("env-dm-scope"))
    assert len(rig.requests) == 1
    rig.web.conversations_replies.assert_called_once_with(
        channel=DM, ts=CARD_TS, inclusive=True, limit=1
    )
    rig.web.chat_update.assert_not_called()
    assert _missing_scope_warnings(caplog), "missing_scope was not reported"
    rig.closed()


def test_preflight_treats_an_approver_dm_route_as_no_fixed_target() -> None:
    """@spec APPROVER-DM-1: the DM resolution names no channel to probe and must not
    be read as a malformed target that refuses the boot."""
    agents = [
        _agent(
            channels=[{"kind": "slack", "address": CHANNEL_A, "adapter": "default"}],
            approval_routes={
                "managers": {
                    "resolution": {"mode": "approver_dm", "identity": "default"},
                    "approver_ticket": {"connector": "example-ticket", "tool": "approve"},
                },
                "security": {"resolution": {"kind": "slack", "address": CHANNEL_C}},
            },
        )
    ]
    default_client = _IdentityClient(auth_response={"ok": True})
    admitted = check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-preflight-approver-dm"),
        identities=(DEFAULT,),
        web_clients={"default": default_client},
        api_client=_client(lambda _request: httpx.Response(200, json=agents)),
    )
    assert [identity.name for identity in admitted] == ["default"]
    assert sorted(default_client.channels) == sorted([CHANNEL_A, CHANNEL_C])
