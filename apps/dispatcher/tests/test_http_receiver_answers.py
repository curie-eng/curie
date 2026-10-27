"""Selected approval answers through the actual Bolt listeners, real Valkey, synthetic API edge.

@spec HTTP-APPROVAL-CONTINUITY-7. These tests do not qualify an actual Slack installation,
receiver key custody or the API's verification; they pin the listener's submission.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import httpx
import redis
from curie_dispatcher.approval_actions import APPROVE_ACTION_ID, APPROVE_NOTE_ACTION_ID
from curie_dispatcher.config import DispatcherConfig
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from slack_bolt.adapter.socket_mode import SocketModeHandler

from .conftest import FakeSocketClient
from .test_approval_note_dialog import _note_submit
from .test_dispatch import _block_action_request
from .test_http_receiver import BASE, CHANNEL, KEY, RECEIVER, SENDER, HttpRig, api
from .test_http_receiver_dispatch import (
    APPROVAL,
    _finish_owned_listeners,
    _owned_keys,
    _selected,
)

PROOF_HEADER = "x-curie-approval-receiver-proof"
PRINCIPAL_HEADER = "x-curie-approval-principal"


def _click(envelope: str, *, action_id: str = APPROVE_ACTION_ID, team: str | None = "T1") -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-7: a block action carrying actual user.team_id."""
    request = _block_action_request(envelope, action_id=action_id, value=APPROVAL)
    user: dict[str, Any] = {"id": SENDER}
    if team is not None:
        user["team_id"] = team
    request.payload["user"] = user
    request.payload["channel"] = {"id": CHANNEL}
    return request


def _submission(envelope: str) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-7: a note dialog submission with actual user.team_id."""
    request = _note_submit(envelope, note="Synthetic note", user=SENDER)
    request.payload["user"] = {"id": SENDER, "team_id": "T1"}
    meta = json.loads(request.payload["view"]["private_metadata"])
    meta.update(approval_id=APPROVAL, channel=CHANNEL)
    request.payload["view"]["private_metadata"] = json.dumps(meta)
    return request


def _resolved() -> dict[str, Any]:
    """@spec HTTP-APPROVAL-CONTINUITY-7: the existing ApprovalOut fields the renderer reads."""
    return {
        "id": APPROVAL, "status": "approved", "resolved_by": SENDER,
        "resolved_at": "2026-10-27T00:00:00Z",
    }


def _handle(app: Any, request: Any) -> FakeSocketClient:
    """@spec HTTP-APPROVAL-CONTINUITY-7: the actual Socket Mode handler."""
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, request)
    finally:
        _finish_owned_listeners(app)
    return sock


def _proof(request: httpx.Request) -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-7: verify with an independently built receiver ring."""
    ring = codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))
    proof = wire.verify_approval_answer_proof(request.headers[PROOF_HEADER], ring)
    assert isinstance(proof, wire.ApprovalAnswerProofV1)
    return proof


def _no_queue_or_legacy(
    redis_client: redis.Redis, config: DispatcherConfig, legacy: Any,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: no legacy resolution, admission or queue write."""
    assert legacy.calls == []
    assert _owned_keys(redis_client, config) == set()
    assert redis_client.xlen(config.stream) == 0


def test_selected_click_acks_before_one_proved_submission_of_exact_bytes(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: ack first, fresh client, one attempt, no ambient auth."""
    api()
    acked_first: list[bool] = []
    sock_holder: list[FakeSocketClient] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        acked_first.append(bool(sock_holder and sock_holder[0].acked_envelope_ids))
        return httpx.Response(200, json=_resolved())

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    sock = FakeSocketClient()
    sock_holder.append(sock)
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, _click("env-answer"))
    finally:
        _finish_owned_listeners(app)
    assert sock.acked_envelope_ids == ["env-answer"]
    assert acked_first == [True]
    assert len(rig.requests) == 1
    request = rig.requests[0]
    assert request.method == "POST"
    assert str(request.url) == f"{BASE}/approvals/{APPROVAL}/resolve"
    assert json.loads(request.content) == {"decision": "approved"}
    for ambient in ("authorization", "cookie", "x-api-key"):
        assert ambient not in request.headers
    proof = _proof(request)
    assert proof.operation == "resolve" and proof.approval_id == APPROVAL
    assert proof.body_sha256 == hashlib.sha256(request.content).hexdigest()
    assert proof.attestation_sha256 == hashlib.sha256(
        request.headers[PRINCIPAL_HEADER].encode("ascii"),
    ).hexdigest()
    assert proof.native_channel_id == CHANNEL
    assert proof.slack.delivery == "block_actions"
    assert proof.slack.user_id == SENDER and proof.slack.interaction_user_team_id == "T1"
    rig.closed()
    _no_queue_or_legacy(redis_client, config, legacy)


def test_selected_note_submission_closes_the_dialog_before_submitting_the_note(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: decision and note serialized once, then proved."""
    api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_resolved())

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    sock = _handle(app, _submission("env-note"))
    assert sock.acked_envelope_ids == ["env-note"]
    assert not sock.ack_payloads.get("env-note")
    assert len(rig.requests) == 1
    request = rig.requests[0]
    assert json.loads(request.content) == {"decision": "approved", "note": "Synthetic note"}
    proof = _proof(request)
    assert proof.slack.delivery == "view_submission"
    assert proof.body_sha256 == hashlib.sha256(request.content).hexdigest()
    rig.closed()
    _no_queue_or_legacy(redis_client, config, legacy)


def test_selected_note_click_opens_the_dialog_without_a_legacy_ownership_probe(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: the note lane is the listener's; nothing resolves yet."""
    api()
    rig = HttpRig()
    app, legacy = _selected(config, redis_client, rig)
    sock = _handle(app, _click("env-open", action_id=APPROVE_NOTE_ACTION_ID))
    assert sock.acked_envelope_ids == ["env-open"]
    rig.web.views_open.assert_called_once()
    assert rig.requests == []
    _no_queue_or_legacy(redis_client, config, legacy)


def test_selected_unknown_outcome_never_falls_back_or_retries(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: an unknown outcome invokes no legacy resolution."""
    api()

    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("synthetic", request=request)

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    sock = _handle(app, _click("env-unknown"))
    assert sock.acked_envelope_ids == ["env-unknown"]
    assert len(rig.requests) == 1
    rig.web.chat_update.assert_not_called()
    rig.web.chat_postEphemeral.assert_called_once()
    rig.closed()
    _no_queue_or_legacy(redis_client, config, legacy)


def test_selected_click_without_interaction_team_refuses_before_http(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: no actual user.team_id, nothing is attested or sent."""
    api()
    rig = HttpRig()
    app, legacy = _selected(config, redis_client, rig)
    sock = _handle(app, _click("env-noteam", team=None))
    assert sock.acked_envelope_ids == ["env-noteam"]
    assert rig.requests == [] and rig.clients == []
    _no_queue_or_legacy(redis_client, config, legacy)
