"""Selected approval answers through the actual Bolt listeners, real Valkey, synthetic API edge.

@spec HTTP-APPROVAL-CONTINUITY-7. These tests do not qualify an actual Slack installation,
receiver key custody or the API's verification; they pin the listener's submission.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
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
    """@spec HTTP-APPROVAL-CONTINUITY-7: the ack is written while the answer is held, then one
    proved attempt of the exact bytes with no ambient auth."""
    api()
    release = threading.Event()
    arrived = threading.Event()
    acked_at_answer: list[list[str]] = []
    sock = FakeSocketClient()

    async def handler(_request: httpx.Request) -> httpx.Response:
        arrived.set()
        await asyncio.to_thread(release.wait, 3)
        # The test releases the answer only after it saw the envelope ack, so this sample
        # orders the ack before the answer completes.
        acked_at_answer.append(list(sock.acked_envelope_ids))
        return httpx.Response(200, json=_resolved())

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, _click("env-answer"))
        assert sock.acked_envelope_ids == ["env-answer"]
        assert arrived.wait(2), "selected answer never reached the synthetic API"
    finally:
        release.set()
        _finish_owned_listeners(app)
    assert acked_at_answer == [["env-answer"]]
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


_REFUSAL_TEXTS = (
    (403, "caller_not_allowed", "You are not allowed to answer this approval."),
    (403, "unsupported_approval", "This approval can't be answered here."),
    (403, "some_new_internal_code", "This click was refused and the platform gave no reason."),
    (
        503, "authority_unavailable",
        "Couldn't confirm your permission right now. Try again in a moment.",
    ),
    (410, "approval expired and can no longer be resolved",
     "This approval expired and can no longer be resolved."),
)


def _ephemeral_texts(rig: HttpRig) -> list[str]:
    """@spec HTTP-APPROVAL-CONTINUITY-7: every ephemeral text the user was shown."""
    return [call.kwargs["text"] for call in rig.web.chat_postEphemeral.call_args_list]


def test_selected_click_refusal_shows_plain_text_never_the_api_code(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: each fixed refusal code maps to one plain sentence."""
    api()
    for index, (status_code, code, expected) in enumerate(_REFUSAL_TEXTS):

        async def handler(
            _request: httpx.Request, status_code: int = status_code, code: str = code,
        ) -> httpx.Response:
            return httpx.Response(status_code, json={"detail": code})

        rig = HttpRig(handler)
        app, _legacy = _selected(config, redis_client, rig)
        _handle(app, _click(f"env-refused-{index}"))
        assert _ephemeral_texts(rig) == [expected]
        assert code not in expected or status_code == 410
        rig.closed()


def test_selected_submission_refusal_shows_plain_text_never_the_api_code(
    config: DispatcherConfig, redis_client: redis.Redis,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: the closed dialog's refusal is plain text too."""
    api()
    for index, (status_code, code, expected) in enumerate(_REFUSAL_TEXTS):

        async def handler(
            _request: httpx.Request, status_code: int = status_code, code: str = code,
        ) -> httpx.Response:
            return httpx.Response(status_code, json={"detail": code})

        rig = HttpRig(handler)
        app, _legacy = _selected(config, redis_client, rig)
        _handle(app, _submission(f"env-refused-note-{index}"))
        shown = _ephemeral_texts(rig)
        assert expected in shown
        assert all(code not in text for text in shown if status_code != 410)
        rig.closed()
