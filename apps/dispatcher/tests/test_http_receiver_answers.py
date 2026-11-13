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
import pytest
import redis
from curie_channel_gateway.slack.approval_actions import (
    APPROVE_ACTION_ID,
    APPROVE_NOTE_ACTION_ID,
    REJECT_ACTION_ID,
    REJECT_NOTE_ACTION_ID,
)
from curie_channel_gateway.slack.config import DispatcherConfig
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


def _submission(envelope: str, *, decision: str = "approved") -> Any:
    """@spec HTTP-APPROVAL-CONTINUITY-7: a note dialog submission with actual user.team_id."""
    request = _note_submit(envelope, note="Synthetic note", decision=decision, user=SENDER)
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


# #5515: the conditional guidance appended to 401/403 refusal feedback. A
# copied or forwarded card keeps its buttons, and those are the refusals its
# clicks produce; the sentence stays conditional because the API does not
# classify the refusal and the surface must not -- an unauthorized clicker in
# the right channel gets the same reason, and the sentence must stay true for
# them too. It names no person and echoes no API code.
_DM_GUIDANCE = (
    " If this card was copied or forwarded, the selected approver must answer "
    "the original card in their DM."
)

_REFUSAL_TEXTS = (
    (401, "missing or invalid approval principal",
     "Resolving failed; try again shortly." + _DM_GUIDANCE),
    (403, "caller_not_allowed",
     "You are not allowed to answer this approval." + _DM_GUIDANCE),
    (403, "unsupported_approval",
     "This approval can't be answered here." + _DM_GUIDANCE),
    (403, "some_new_internal_code",
     "This click was refused and the platform gave no reason." + _DM_GUIDANCE),
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


# #5515: Approve and Reject, each refused with 401 and 403. The rows carry the
# decision the click replays, the action id that selects the listener, the
# synthetic API's refusal shape, and the plain reason the guidance is
# appended to.
_REFUSED_CLICK_REPLAYS = (
    ("approved", APPROVE_ACTION_ID, 401, "missing or invalid approval principal",
     "Resolving failed; try again shortly."),
    ("rejected", REJECT_ACTION_ID, 401, "missing or invalid approval principal",
     "Resolving failed; try again shortly."),
    ("approved", APPROVE_ACTION_ID, 403, "caller_not_allowed",
     "You are not allowed to answer this approval."),
    ("rejected", REJECT_ACTION_ID, 403, "caller_not_allowed",
     "You are not allowed to answer this approval."),
)


@pytest.mark.parametrize(
    ("decision", "action_id", "status_code", "detail", "reason"), _REFUSED_CLICK_REPLAYS,
)
def test_selected_refused_click_replays_the_decision_and_guides_to_the_original_dm(
    config: DispatcherConfig, redis_client: redis.Redis, decision: str, action_id: str,
    status_code: int, detail: str, reason: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: a 401/403 refusal keeps its reason, plus
    the #5515 guidance for a copied card's clicker.

    Approve and Reject each replay exactly the decision they carry; the refusal
    ephemeral reaches the clicker's actual interaction channel, neither the
    original nor a copied card is stamped, and no ordinary turn is queued.
    """
    api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"detail": detail})

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    sock = _handle(app, _click("env-refused-click", action_id=action_id))
    assert sock.acked_envelope_ids == ["env-refused-click"]
    assert len(rig.requests) == 1
    request = rig.requests[0]
    assert str(request.url) == f"{BASE}/approvals/{APPROVAL}/resolve"
    assert json.loads(request.content) == {"decision": decision}
    proof = _proof(request)
    assert proof.operation == "resolve" and proof.approval_id == APPROVAL
    assert proof.slack.user_id == SENDER and proof.native_channel_id == CHANNEL
    rig.web.chat_update.assert_not_called()
    assert rig.web.chat_postEphemeral.call_count == 1
    assert rig.web.chat_postEphemeral.call_args.kwargs == {
        "channel": CHANNEL, "user": SENDER, "text": reason + _DM_GUIDANCE,
    }
    rig.closed()
    _no_queue_or_legacy(redis_client, config, legacy)


_REFUSED_SUBMISSION_REPLAYS = (
    ("approved", 401, "missing or invalid approval principal",
     "Resolving failed; try again shortly."),
    ("approved", 403, "caller_not_allowed",
     "You are not allowed to answer this approval."),
    ("rejected", 401, "missing or invalid approval principal",
     "Resolving failed; try again shortly."),
    ("rejected", 403, "caller_not_allowed",
     "You are not allowed to answer this approval."),
)


@pytest.mark.parametrize(
    ("decision", "status_code", "detail", "reason"), _REFUSED_SUBMISSION_REPLAYS,
)
def test_selected_refused_submission_replays_the_decision_and_guides_to_the_original_dm(
    config: DispatcherConfig, redis_client: redis.Redis, decision: str, status_code: int,
    detail: str, reason: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: the note dialog's refused submission keeps
    its reason, plus the #5515 guidance.

    Both note decisions replay what the dialog carried, the refusal ephemeral
    reaches the clicker behind the closed dialog, and no card is stamped.
    """
    api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"detail": detail})

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    sock = _handle(app, _submission("env-refused-submit", decision=decision))
    assert sock.acked_envelope_ids == ["env-refused-submit"]
    assert not sock.ack_payloads.get("env-refused-submit")
    assert len(rig.requests) == 1
    request = rig.requests[0]
    assert str(request.url) == f"{BASE}/approvals/{APPROVAL}/resolve"
    assert json.loads(request.content) == {"decision": decision, "note": "Synthetic note"}
    proof = _proof(request)
    assert proof.slack.delivery == "view_submission"
    assert proof.slack.user_id == SENDER
    rig.web.chat_update.assert_not_called()
    assert rig.web.chat_postEphemeral.call_count == 1
    assert rig.web.chat_postEphemeral.call_args.kwargs == {
        "channel": CHANNEL, "user": SENDER, "text": reason + _DM_GUIDANCE,
    }
    rig.closed()
    _no_queue_or_legacy(redis_client, config, legacy)


_REFUSED_DIALOG_REPLAYS = (
    ("approved", APPROVE_NOTE_ACTION_ID, 401, "missing or invalid approval principal",
     "Resolving failed; try again shortly."),
    ("approved", APPROVE_NOTE_ACTION_ID, 403, "caller_not_allowed",
     "You are not allowed to answer this approval."),
    ("rejected", REJECT_NOTE_ACTION_ID, 401, "missing or invalid approval principal",
     "Resolving failed; try again shortly."),
    ("rejected", REJECT_NOTE_ACTION_ID, 403, "caller_not_allowed",
     "You are not allowed to answer this approval."),
)


@pytest.mark.parametrize(
    ("decision", "action_id", "status_code", "detail", "reason"), _REFUSED_DIALOG_REPLAYS,
)
def test_selected_refused_dialog_open_failure_guides_to_the_original_dm(
    config: DispatcherConfig, redis_client: redis.Redis, decision: str, action_id: str,
    status_code: int, detail: str, reason: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: a dialog that cannot open still resolves, and
    a 401/403 refusal keeps its reason plus the #5515 guidance.

    Both note buttons are covered: the fall-forward resolves with the clicked
    decision and no note, and the only surface left for the refusal is the
    ephemeral.
    """
    api()

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, json={"detail": detail})

    rig = HttpRig(handler)
    app, legacy = _selected(config, redis_client, rig)
    rig.web.views_open.side_effect = RuntimeError("synthetic views.open failure")
    sock = _handle(app, _click("env-refused-dialog", action_id=action_id))
    assert sock.acked_envelope_ids == ["env-refused-dialog"]
    rig.web.views_open.assert_called_once()
    assert len(rig.requests) == 1
    request = rig.requests[0]
    assert str(request.url) == f"{BASE}/approvals/{APPROVAL}/resolve"
    assert json.loads(request.content) == {"decision": decision}
    proof = _proof(request)
    assert proof.slack.delivery == "block_actions"
    assert proof.slack.user_id == SENDER
    rig.web.chat_update.assert_not_called()
    assert rig.web.chat_postEphemeral.call_count == 1
    assert rig.web.chat_postEphemeral.call_args.kwargs == {
        "channel": CHANNEL, "user": SENDER, "text": reason + _DM_GUIDANCE,
    }
    rig.closed()
    _no_queue_or_legacy(redis_client, config, legacy)
