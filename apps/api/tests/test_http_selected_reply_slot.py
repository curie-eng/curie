"""The selected receiver's reply slot through real ingress, Valkey and the actual reply adapter.

@spec HTTP-INVOCATION-EXECUTION-27.
Synthetic Slack and directory edges only; no actual Slack installation is qualified.
"""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import logging
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from aci_protocol import QueuedTurn
from aiohttp import web
from aiohttp.test_utils import TestServer
from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyUpdate
from curie_api.delivery import sha16
from curie_api.identity.http_transport import HttpIdentityClient
from curie_api.main import create_app
from curie_internal import identity_context as codec
from curie_internal.keyspace import CHANNEL_KEY_PREFIX
from curie_worker.kernel.routing import _target_for
from curie_worker.reply_sink import TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter
from fastapi.testclient import TestClient
from slack_sdk.web import WebClient

from apps.api.tests import test_http_email_receivers as email_receivers
from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

BOT = "U0EXAMPLEBOT"
SLACK_TOKEN = "xoxb-synthetic-reply-slot"
PARENT_TS = "1720000000.000100"
MESSAGE_TS = "1720000000.000200"
FIRST_SLOT = "1720000000.000300"
SECOND_SLOT = "1720000000.000400"
log = logging.getLogger("synthetic-selected-reply-slot")


def _orchestrator() -> Callable[..., Any]:
    """@spec HTTP-INVOCATION-EXECUTION-27: a missing owner fails in the test body."""
    handlers = importlib.import_module("curie_dispatcher.handlers")
    owner = getattr(handlers, "submit_selected_mention", None)
    assert callable(owner), "submit_selected_mention is not implemented"
    return owner  # type: ignore[no-any-return]


def _dispatcher_config() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-27: the placeholder text the legacy lane posts."""
    config = importlib.import_module("curie_dispatcher.config")
    return config.DispatcherConfig(slack_app_token="xapp-test", slack_bot_token="xoxb-test")


def _native(scope: dict[str, Any], *, text: str = "Synthetic reply slot") -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-27: one native mention, fixed native delivery id."""
    event = {
        "type": "app_mention", "channel": scope["address"], "user": membership.SENDER,
        "user_team": membership.ACCOUNT, "text": f"<@{BOT}> {text}",
        "ts": MESSAGE_TS, "thread_ts": PARENT_TS,
    }
    return {
        "type": "event_callback", "event_id": "EvSyntheticReplySlot",
        "team_id": membership.ACCOUNT,
        "authorizations": [{"team_id": membership.ACCOUNT, "user_id": BOT, "is_bot": True}],
        "is_ext_shared_channel": False, "event": event,
    }


class Rig:
    """@spec HTTP-INVOCATION-EXECUTION-27: actual ASGI route behind the receiver's own client."""

    def __init__(self, client: TestClient, scope: dict[str, Any], credential: str) -> None:
        self.client = client
        self.scope = scope
        self.credential = credential
        self.answers: list[httpx.Response] = []
        self.lose_first_response = False
        self.unreachable = False
        self.web = WebClient(token=SLACK_TOKEN)
        self.web.auth_test = MagicMock(return_value={  # type: ignore[method-assign]
            "ok": True, "team_id": membership.ACCOUNT, "user_id": BOT,
            "bot_id": "B0EXAMPLE1", "is_enterprise_install": False,
        })
        self.web.chat_postMessage = MagicMock(  # type: ignore[method-assign]
            side_effect=[{"ok": True, "ts": FIRST_SLOT}, {"ok": True, "ts": SECOND_SLOT}],
        )
        self.web.chat_delete = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        self.web.chat_update = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
        self.runtime = self._runtime()

    async def _forward(self, request: httpx.Request) -> httpx.Response:
        """@spec HTTP-INVOCATION-EXECUTION-27: no fake admission, receipt or queue producer."""
        if self.unreachable:
            raise httpx.ConnectError("synthetic unreachable core", request=request)
        response = self.client.post(
            request.url.path, content=request.content, headers=dict(request.headers),
        )
        answer = httpx.Response(
            response.status_code, content=response.content, headers=response.headers,
        )
        self.answers.append(answer)
        if self.lose_first_response and len(self.answers) == 1:
            raise httpx.ReadTimeout("synthetic lost response", request=request)
        return answer

    def _runtime(self) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-27."""
        receiver = importlib.import_module("curie_dispatcher.http_receiver")
        scope = self.scope
        return receiver.build_http_receiver_runtime(
            receiver.HttpReceiverScopeV1(
                schema_version=1, receiver_id=scope["receiver_id"], receiver_generation=1,
                channel_identity_id=scope["identity"], channel_identity_name=scope["adapter"],
                provider_installation_id=scope["installation"],
                namespace=scope["namespace"].model_dump(),
                installation_account=membership.ACCOUNT, bot_user_id=BOT,
            ),
            (receiver.HttpReceiverBindingV1(
                schema_version=1, native_channel_id=scope["address"],
                binding_id=scope["binding"], binding_generation=scope["generation"],
                channel_credential=self.credential,
            ),),
            receiver_key=scope["receiver_key"], web_client=self.web,
            api_base_url="https://synthetic-core.example.test",
            http_client_factory=lambda: httpx.AsyncClient(
                transport=httpx.MockTransport(self._forward),
            ),
        )

    def mention(self, body: dict[str, Any]) -> Any:
        """@spec HTTP-INVOCATION-EXECUTION-27: the selected listener's own submission owner."""
        return _orchestrator()(
            self.runtime, web_client=self.web, config=_dispatcher_config(), log=log,
            body=body, event=body["event"], slack_identity=self.scope["adapter"],
            bot_user_id=BOT,
        )

    def turns(self, valkey: redis.Redis, runs_stream: str) -> list[QueuedTurn]:
        """@spec HTTP-INVOCATION-EXECUTION-27."""
        return [
            QueuedTurn.model_validate_json(fields["payload"])
            for _id, fields in valkey.xrange(runs_stream)
        ]


@contextmanager
def _rig(
    valkey: redis.Redis, synthetic_provider: SimpleNamespace,
) -> Iterator[Rig]:
    """@spec HTTP-INVOCATION-EXECUTION-27: owned app, client and binding state."""
    scope = membership._scope(synthetic_provider.url)
    identity_client = HttpIdentityClient(tls_context=synthetic_provider.tls)
    try:
        with TestClient(create_app(
            http_identity_runtime=membership._runtime(membership._runtime_api(), scope),
            http_identity_client=identity_client,
        )) as client:
            yield Rig(client, scope, membership._credential(client, scope))
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


def _edit_placeholder(turn: QueuedTurn) -> list[tuple[str, dict[str, Any]]]:
    """@spec HTTP-INVOCATION-EXECUTION-27: the actual worker adapter against an owned Slack stub."""
    captured: list[tuple[str, dict[str, Any]]] = []
    assert turn.reply_handle is not None

    async def deliver() -> None:
        async def capture(request: web.Request) -> web.Response:
            assert request.headers["Authorization"] == "Bearer " + SLACK_TOKEN
            body = await request.json()
            captured.append((request.match_info["method"], body))
            return web.json_response({"ok": True, "ts": body.get("ts") or MESSAGE_TS})

        endpoint = web.Application()
        endpoint.router.add_post("/slack/api/{method}", capture)
        async with TestServer(endpoint) as server:
            sink = SlackReplyAdapter(
                "", identity_tokens={turn.reply_handle.adapter: SLACK_TOKEN},
                base_url=str(server.make_url("/slack/api/")),
            )
            try:
                await sink.emit(
                    ReplyUpdate(
                        version=REPLY_WIRE_VERSION, event="reply.update",
                        target=_target_for(turn), text="Synthetic answer in the slot",
                    ),
                    route=TargetRoute(adapter=turn.reply_handle.adapter),
                )
            finally:
                for sdk in sink._clients.values():
                    if sdk.session is not None and not sdk.session.closed:
                        await sdk.session.close()

    asyncio.run(deliver())
    return captured


def test_lost_response_then_identical_retry_keeps_the_accepted_slot_the_answer_edits(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-27: a retry of an accepted request keeps its own slot."""
    with _rig(valkey, synthetic_provider) as rig:
        rig.lose_first_response = True
        outcome = rig.mention(_native(rig.scope))
        assert [answer.status_code for answer in rig.answers] == [200, 200]
        assert rig.answers[0].json()["duplicate"] is False
        assert rig.answers[1].json()["duplicate"] is True
        assert rig.answers[1].json()["reply_ref"] == FIRST_SLOT
        assert outcome.result.status == "accepted" and outcome.result.duplicate is True
        assert outcome.result.reply_ref == FIRST_SLOT and outcome.placeholder == FIRST_SLOT
        rig.web.chat_postMessage.assert_called_once()
        rig.web.chat_delete.assert_not_called()
        (turn,) = rig.turns(valkey, runs_stream)
        assert turn.reply_handle is not None and turn.reply_handle.placeholder == FIRST_SLOT
        captured = _edit_placeholder(turn)
        assert [method for method, _body in captured] == ["chat.update"]
        assert captured[0][1]["ts"] == FIRST_SLOT
        assert captured[0][1]["channel"] == rig.scope["address"]


def test_redelivery_with_a_new_placeholder_deletes_only_the_new_one_and_never_conflicts(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-27: the slot is authenticated but outside the intent."""
    with _rig(valkey, synthetic_provider) as rig:
        first = rig.mention(_native(rig.scope))
        again = rig.mention(_native(rig.scope))
        assert [answer.status_code for answer in rig.answers] == [200, 200]
        assert "reply_ref" not in rig.answers[0].json()
        assert rig.answers[1].json()["reply_ref"] == FIRST_SLOT
        assert first.result.status == "accepted" and first.placeholder == FIRST_SLOT
        assert again.result.status == "accepted" and again.result.duplicate is True
        assert again.placeholder is None
        rig.web.chat_delete.assert_called_once_with(channel=rig.scope["address"], ts=SECOND_SLOT)
        (turn,) = rig.turns(valkey, runs_stream)
        assert turn.reply_handle is not None and turn.reply_handle.placeholder == FIRST_SLOT


def test_different_text_under_the_same_delivery_id_is_still_an_intent_conflict(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-27: immutable content stays inside the intent."""
    with _rig(valkey, synthetic_provider) as rig:
        rig.mention(_native(rig.scope))
        changed = rig.mention(_native(rig.scope, text="Different synthetic content"))
        assert [answer.status_code for answer in rig.answers] == [200, 409]
        assert rig.answers[1].json()["detail"] == "delivery_intent_conflict"
        assert changed.result.status == "refused" and changed.placeholder == SECOND_SLOT
        rig.web.chat_delete.assert_not_called()
        assert len(rig.turns(valkey, runs_stream)) == 1


@pytest.mark.parametrize("outcome", ["pending", "unavailable"])
def test_pending_and_unavailable_outcomes_keep_the_placeholder(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
    outcome: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-27: never delete a placeholder without proof."""
    with _rig(valkey, synthetic_provider) as rig:
        body = _native(rig.scope)
        if outcome == "pending":
            claim = (
                f"{CHANNEL_KEY_PREFIX}:delivery:{rig.scope['binding']}:"
                f"{sha16(body['event_id'])}"
            )
            valkey.set(claim, "pending:" + "d" * 32, ex=60)
        else:
            rig.unreachable = True
        result = rig.mention(body)
        assert result.result.status == outcome and result.placeholder == FIRST_SLOT
        if outcome == "pending":
            assert [answer.status_code for answer in rig.answers] == [202]
        rig.web.chat_delete.assert_not_called()
        rig.web.chat_update.assert_not_called()
        assert rig.turns(valkey, runs_stream) == []


def _submission(
    scope: dict[str, Any], reply_ref: str, *, slack: codec.ReceiverSlackEvidenceV1 | None = None,
) -> tuple[bytes, str]:
    """@spec HTTP-INVOCATION-EXECUTION-27: a receiver proof over a body carrying reply_ref."""
    body = json.dumps({
        "kind": "slack", "address": scope["address"], "adapter": scope["adapter"],
        "delivery_id": "synthetic-event-" + uuid.uuid4().hex, "conversation_id": PARENT_TS,
        "author": membership.SENDER, "text": "Synthetic slot carriage", "reply_ref": reply_ref,
        "attachments": [],
    }, separators=(",", ":")).encode()
    parsed = json.loads(body)
    proof = codec.ReceiverProofV1(
        schema_version=1, receiver_id=scope["receiver_id"], receiver_generation=1,
        channel_identity_id=scope["identity"], provider_installation_id=scope["installation"],
        binding_id=scope["binding"], binding_generation=scope["generation"],
        delivery_id=parsed["delivery_id"], native_channel_id=scope["address"],
        conversation_id=PARENT_TS, body_sha256=hashlib.sha256(body).hexdigest(),
        slack=slack if slack is not None else codec.ReceiverSlackEvidenceV1(
            delivery="app_mention", user_id=membership.SENDER, event_user_team=None,
            event_team=membership.ACCOUNT, interaction_user_team_id=None,
            is_ext_shared_channel=False, enterprise_ids=(),
        ),
    )
    ring = codec.build_identity_keyring(
        "receiver", ((scope["receiver_id"], 1, scope["receiver_key"]),),
    )
    return body, codec.sign_receiver_proof(proof, ring)


@pytest.mark.parametrize(("reply_ref", "placeholder"), [("123.456", "123.456"), ("", None)])
def test_reply_ref_is_queued_as_the_sealed_reply_placeholder(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
    reply_ref: str, placeholder: str | None,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-27: a nonempty slot is edited; an empty one replies fresh."""
    with _rig(valkey, synthetic_provider) as rig:
        body, proof = _submission(rig.scope, reply_ref)
        response = membership._post(rig.client, rig.credential, body, proof)
        assert response.status_code == 200, response.text
        assert "reply_ref" not in response.json()
        entries = valkey.xrange(runs_stream)
        assert len(entries) == 1
        assert set(entries[0][1]) == {"payload", "curie_identity_context_v1"}
        sealed = codec.open_invocation_context(
            entries[0][1]["curie_identity_context_v1"],
            codec.build_identity_keyring(
                "context", (("synthetic-context", 1, rig.scope["context_key"]),),
            ),
        )
        assert isinstance(sealed, codec.InvocationContextV1)
        assert sealed.payload_sha256 == hashlib.sha256(
            entries[0][1]["payload"].encode("utf-8"),
        ).hexdigest()
        turn = QueuedTurn.model_validate_json(entries[0][1]["payload"])
        assert turn.reply_handle is not None and turn.reply_handle.placeholder == placeholder
        if placeholder is not None:
            captured = _edit_placeholder(turn)
            assert [method for method, _body in captured] == ["chat.update"]
            assert captured[0][1]["ts"] == placeholder


@pytest.mark.parametrize("reply_ref", ["not-a-timestamp", "1.2.3", "1" * 21 + ".1"])
def test_malformed_reply_ref_refuses_without_delivery_state(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
    reply_ref: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-27: only a Slack message timestamp is a slot."""
    with _rig(valkey, synthetic_provider) as rig:
        body, proof = _submission(rig.scope, reply_ref)
        response = membership._post(rig.client, rig.credential, body, proof)
        assert response.status_code == 401, response.text
        assert valkey.xrange(runs_stream) == []


def _email_body(
    *, delivery_id: str, reply_ref: str, text: str = "Synthetic email request",
) -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-27/29: an email TurnIn whose adapter names a reply_ref."""
    value = json.loads(email_receivers._body(delivery_id=delivery_id, text=text))
    value["reply_ref"] = reply_ref
    return json.dumps(value, separators=(",", ":")).encode()


def test_email_turn_intent_binds_the_verified_email_delivery(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-27/29: an email turn has an intent but no reply slot.

    The email proof's immutable fields and the whole parsed body, reply_ref
    included, are its intent: the same message is a duplicate, any other body
    under its Message-ID is a conflict, and no placeholder is queued.
    """
    scope = email_receivers._email_scope(synthetic_provider.url)
    try:
        with email_receivers._client(synthetic_provider, scope) as client:
            token = email_receivers._email_credential(client, scope)
            delivery = "<synthetic-" + uuid.uuid4().hex + "@example.com>"
            body = _email_body(delivery_id=delivery, reply_ref="<parent@example.com>")
            accepted = email_receivers._post(
                client, token, body, email_receivers._proof(scope, body),
            )
            assert accepted.status_code == 200, accepted.text
            assert accepted.json()["duplicate"] is False
            entries = valkey.xrange(runs_stream)
            assert len(entries) == 1
            turn = QueuedTurn.model_validate_json(entries[0][1]["payload"])
            assert turn.reply_handle is not None and turn.reply_handle.kind == "email"
            assert turn.reply_handle.placeholder is None

            again = email_receivers._post(
                client, token, body, email_receivers._proof(scope, body),
            )
            assert again.status_code == 200, again.text
            assert again.json() == {
                "event_id": accepted.json()["event_id"],
                "stream_id": accepted.json()["stream_id"], "duplicate": True,
            }
            for changed in (
                _email_body(delivery_id=delivery, reply_ref="<other@example.com>"),
                _email_body(delivery_id=delivery, reply_ref="<parent@example.com>",
                            text="Synthetic changed request"),
            ):
                conflict = email_receivers._post(
                    client, token, changed, email_receivers._proof(scope, changed),
                )
                assert conflict.status_code == 409, conflict.text
                assert conflict.json() == {"detail": "delivery_intent_conflict"}
            assert valkey.xrange(runs_stream) == entries
    finally:
        email_receivers._cleanup(valkey, scope)


def test_direct_message_reply_ref_is_queued_as_the_sealed_reply_placeholder(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26/27: a DM's placeholder is its reply slot too."""
    direct = codec.ReceiverSlackEvidenceV1.model_validate({
        "delivery": "message", "user_id": membership.SENDER,
        "event_user_team": membership.ACCOUNT, "event_team": membership.ACCOUNT,
        "interaction_user_team_id": None, "is_ext_shared_channel": False,
        "enterprise_ids": (), "channel_type": "im",
    })
    with _rig(valkey, synthetic_provider) as rig:
        body, proof = _submission(rig.scope, FIRST_SLOT, slack=direct)
        response = membership._post(rig.client, rig.credential, body, proof)
        assert response.status_code == 200, response.text
        (entry,) = valkey.xrange(runs_stream)
        turn = QueuedTurn.model_validate_json(entry[1]["payload"])
        assert turn.reply_handle is not None and turn.reply_handle.placeholder == FIRST_SLOT
        captured = _edit_placeholder(turn)
        assert [method for method, _body in captured] == ["chat.update"]
        assert captured[0][1]["ts"] == FIRST_SLOT
