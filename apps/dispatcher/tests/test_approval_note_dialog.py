"""The optional-note dialog on an approval card (#1053).

Same discipline as ``test_approval_actions.py``: driven through Bolt's real
``SocketModeHandler``, with only the socket, the Web API client, and the
platform API faked. What is asserted here is the behavior the plain click path
cannot have:

- a click on a note-carrying card OPENS a dialog and resolves nothing;
- submitting it resolves WITH the typed note, so the reason reaches the record
  (and from there the requester, since ``build_resume_turn`` interpolates it);
- submitting it blank resolves with no note at all, rather than an empty string;
- cancelling leaves the record pending, which is stricter than the old
  click-is-the-decision behavior and must not regress;
- and the widened claim race renders its refusal INSIDE the view, because the
  loser is standing in an open modal where an ephemeral is invisible.

Plus the ack budget (#1077): the submit ack must reach the socket before any
Slack round trip, and the note must stay small enough that the card stamp
cannot bounce.
"""

import threading
import time
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import httpx
import redis
from curie_dispatcher.app import build_app, build_web_client
from curie_dispatcher.approval_actions import (
    _VERDICT_LINE_MAX,
    APPROVE_NOTE_ACTION_ID,
    NOTE_MODAL_CALLBACK_ID,
    REJECT_NOTE_ACTION_ID,
    ApprovalResolveClient,
    ResolveOutcome,
    _refusal_text,
)
from curie_dispatcher.config import DispatcherConfig
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.errors import SlackApiError
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient

from .conftest import FakeSocketClient, _authorize, _black_hole_api, deliver_until_acked

APPROVAL_ID = "9a1e8a10-0000-0000-0000-000000001053"
CARD_TS = "1700.0042"
CARD_CHANNEL = "C_MGRS"
_PLATFORM_API_KEY = "platform-api-test-key"
_CHAT_ATTESTER_SECRET = "dispatcher-attester-test-secret"

_CARD_MESSAGE: dict[str, Any] = {
    "ts": CARD_TS,
    "blocks": [
        {"type": "header", "text": {"type": "plain_text", "text": "Approval required"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "Discount for ACME"}},
        {"type": "actions", "elements": []},
    ],
}


class ScriptedResolver:
    """Stands in for the platform API, recording the note it was handed."""

    def __init__(self, outcome: ResolveOutcome) -> None:
        self.outcome = outcome
        self.calls: list[dict[str, str | None]] = []

    def resolve(
        self,
        approval_id: str,
        *,
        decision: str,
        attested_user: str,
        attested_channel: str,
        note: str | None = None,
    ) -> ResolveOutcome:
        self.calls.append(
            {
                "approval_id": approval_id,
                "decision": decision,
                "attested_user": attested_user,
                "attested_channel": attested_channel,
                "note": note,
            }
        )
        return self.outcome

    def exists(self, approval_id: str) -> bool | None:
        del approval_id
        return not (
            self.outcome.status_code == 404
            and self.outcome.detail.strip().casefold() == "approval not found"
        )


class _CapturingHttpResponse:
    status_code = 200
    text = ""

    def json(self) -> dict[str, str]:
        return {"status": "approved", "resolved_by": "U_MANAGER"}


class _CapturingHttpClient:
    """Records the real resolve client's request at its one HTTP seam."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def post(
        self, url: str, *, json: dict[str, Any], headers: dict[str, str]
    ) -> _CapturingHttpResponse:
        self.requests.append({"method": "POST", "url": url, "json": json, "headers": headers})
        return _CapturingHttpResponse()

    def get(self, url: str, *, headers: dict[str, str]) -> _CapturingHttpResponse:
        self.requests.append({"method": "GET", "url": url, "headers": headers})
        return _CapturingHttpResponse()


def _claims(token: str) -> dict[str, Any]:
    """Decode claims only; signature correctness is pinned in the click suite."""

    _prefix, payload, _signature = token.split(".")
    import base64
    import json

    return json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))


def _wire_resolver() -> tuple[ApprovalResolveClient, _CapturingHttpClient]:
    captured = _CapturingHttpClient()
    return (
        ApprovalResolveClient(
            api_base_url="https://api.example.test",
            api_key=_PLATFORM_API_KEY,
            approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
            client=captured,  # type: ignore[arg-type]
        ),
        captured,
    )


def _gated_history(gate: threading.Event) -> Callable[..., dict[str, Any]]:
    """A card read that blocks until the test releases ``gate``.

    How the ack-ordering tests turn "the ack did not wait on a Slack call" into a
    structural fact rather than a timing hope.
    """

    def _history(**_kwargs: Any) -> dict[str, Any]:
        gate.wait(5)
        return {"messages": [_CARD_MESSAGE]}

    return _history


def _build(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    resolver: ScriptedResolver,
    *,
    views_open_raises: bool = False,
    history_side_effect: Callable[..., Any] | BaseException | None = None,
) -> tuple[App, WebClient]:
    """Build the app under test.

    ``history_side_effect`` is handed straight to the ``conversations.replies``
    mock, so a test picks the failure mode it needs: a callable that blocks (see
    ``_gated_history``) for a Slack call that is SLOW, or an exception instance
    for one that FAILS. ``slack_sdk`` 3.43.0 raises ``SlackApiError`` from
    ``WebClient`` whenever Slack answers ``ok: false``
    (``slack_sdk.web.base_client.BaseClient.api_call`` ends in
    ``validate_slack_response``), so that is the exception a real
    ``conversations.replies`` failure arrives as.
    """

    web_client = WebClient(token="xoxb-test")
    web_client.chat_postMessage = MagicMock(return_value={"ts": "555.000"})  # type: ignore[method-assign]
    web_client.chat_update = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.chat_postEphemeral = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.conversations_replies = MagicMock(  # type: ignore[method-assign]
        side_effect=history_side_effect,
        return_value={"messages": [_CARD_MESSAGE]},
    )
    web_client.views_open = MagicMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("trigger_id expired") if views_open_raises else None,
        return_value={"ok": True},
    )
    app = build_app(
        config,
        web_client=web_client,
        redis_client=redis_client,
        authorize=_authorize,
        resolver=resolver,
    )
    return app, web_client


def _drain(app: App) -> None:
    app.listener_runner.listener_executor.shutdown(wait=True)


def _assert_ownership_miss_ephemeral(web_client: WebClient) -> None:
    web_client.chat_postEphemeral.assert_called_once()
    text = web_client.chat_postEphemeral.call_args.kwargs["text"]
    folded = text.casefold()
    assert "disconnect" in folded
    assert "do not retry from this side" in folded
    assert "try again" not in folded


def _note_click(envelope_id: str, *, action_id: str, user: str = "U_MANAGER") -> SocketModeRequest:
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "block_actions",
            "trigger_id": f"trig-{envelope_id}",
            "team": {"id": "T1"},
            "user": {"id": user},
            "api_app_id": "A1",
            "token": "verif",
            "container": {"type": "message", "message_ts": CARD_TS},
            "channel": {"id": CARD_CHANNEL},
            "message": _CARD_MESSAGE,
            "actions": [
                {
                    "type": "button",
                    "action_id": action_id,
                    "action_ts": "2.0",
                    "value": APPROVAL_ID,
                }
            ],
        },
    )


def _note_submit(
    envelope_id: str,
    *,
    note: str | None,
    decision: str = "approved",
    user: str = "U_MANAGER",
) -> SocketModeRequest:
    """A view_submission for the note dialog, carrying the same
    ``private_metadata`` the open path writes."""

    import json

    state_value: dict[str, Any] = {}
    if note is not None:
        state_value = {"note": {"note-input": {"type": "plain_text_input", "value": note}}}
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "view_submission",
            "team": {"id": "T1"},
            "user": {"id": user},
            "api_app_id": "A1",
            "token": "verif",
            "trigger_id": f"trig-{envelope_id}",
            "view": {
                "id": "V1",
                "type": "modal",
                "callback_id": NOTE_MODAL_CALLBACK_ID,
                "private_metadata": json.dumps(
                    {
                        "approval_id": APPROVAL_ID,
                        "channel": CARD_CHANNEL,
                        "card_ts": CARD_TS,
                        "decision": decision,
                    }
                ),
                "state": {"values": state_value},
                "title": {"type": "plain_text", "text": "Approve request"},
                "blocks": [],
            },
        },
    )


def test_a_note_click_opens_a_dialog_and_resolves_nothing(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(ResolveOutcome(status_code=200, resolved_by="U_MANAGER"))
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _note_click("env-n1", action_id=APPROVE_NOTE_ACTION_ID))
    assert sock.acked_envelope_ids == ["env-n1"]
    _drain(app)

    # The decision is NOT made on the click: the whole point of the dialog is
    # that the approver can still cancel.
    assert resolver.calls == []
    web_client.views_open.assert_called_once()
    view = web_client.views_open.call_args.kwargs["view"]
    assert view["callback_id"] == NOTE_MODAL_CALLBACK_ID
    # The note field is optional: a decision must never be blocked on typing one.
    assert view["blocks"][0]["optional"] is True
    # Everything the submit handler needs rides in private_metadata, since a
    # view_submission payload carries no channel and no message of its own.
    import json

    meta = json.loads(view["private_metadata"])
    assert meta == {
        "approval_id": APPROVAL_ID,
        "channel": CARD_CHANNEL,
        "card_ts": CARD_TS,
        "decision": "approved",
    }


def test_submitting_the_dialog_resolves_with_the_typed_note(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _note_submit("env-n2", note="  approved for Q3  "))
    _drain(app)

    assert resolver.calls == [
        {
            "approval_id": APPROVAL_ID,
            "decision": "approved",
            "attested_user": "U_MANAGER",
            "attested_channel": CARD_CHANNEL,
            "note": "approved for Q3",
        }
    ]
    # The note is stamped onto the card too: the approver channel is where the
    # next person looks, and a reason that only reached the requester leaves
    # that channel with a bare verdict.
    web_client.chat_update.assert_called_once()
    kwargs = web_client.chat_update.call_args.kwargs
    assert kwargs["channel"] == CARD_CHANNEL and kwargs["ts"] == CARD_TS
    assert "approved for Q3" in kwargs["text"]
    assert not any(b.get("type") == "actions" for b in kwargs["blocks"])


def test_two_releases_only_the_owner_resolves_a_note_submission(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """One fake Slack app, two dispatchers: only the owner acks the submit (#2248)."""

    non_owner = ScriptedResolver(ResolveOutcome(status_code=404, detail="approval not found"))
    owner = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    non_owner_app, non_owner_web = _build(config, redis_client, non_owner)
    owner_app, owner_web = _build(config, redis_client, owner)
    non_owner_socket = FakeSocketClient()
    owner_socket = FakeSocketClient()
    submit = _note_submit("env-shared-note", note="approved for Q3")

    acked_by = deliver_until_acked(
        [
            (
                SocketModeHandler(non_owner_app, app_token="xapp-test"),
                non_owner_socket,
                non_owner_app,
            ),
            (
                SocketModeHandler(owner_app, app_token="xapp-test"),
                owner_socket,
                owner_app,
            ),
        ],
        submit,
    )

    assert acked_by is owner_socket
    assert non_owner_socket.acked_envelope_ids == []
    assert owner_socket.acked_envelope_ids == ["env-shared-note"]
    assert owner_socket.ack_payload_for("env-shared-note") is None
    assert len(non_owner.calls) == 1
    assert len(owner.calls) == 1
    assert owner.calls[0]["note"] == "approved for Q3"
    non_owner_web.conversations_replies.assert_not_called()
    non_owner_web.chat_update.assert_not_called()
    _assert_ownership_miss_ephemeral(non_owner_web)
    owner_web.chat_update.assert_called_once()
    assert "approved for Q3" in owner_web.chat_update.call_args.kwargs["text"]
    owner_web.chat_postEphemeral.assert_not_called()


def test_two_releases_only_the_owner_opens_a_note_dialog(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The non-owner must not ack a note click, so the owner opens the dialog."""

    non_owner = ScriptedResolver(ResolveOutcome(status_code=404, detail="approval not found"))
    owner = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    non_owner_app, non_owner_web = _build(config, redis_client, non_owner)
    owner_app, owner_web = _build(config, redis_client, owner)
    non_owner_socket = FakeSocketClient()
    owner_socket = FakeSocketClient()
    click = _note_click("env-shared-note-open", action_id=APPROVE_NOTE_ACTION_ID)

    acked_by = deliver_until_acked(
        [
            (
                SocketModeHandler(non_owner_app, app_token="xapp-test"),
                non_owner_socket,
                non_owner_app,
            ),
            (
                SocketModeHandler(owner_app, app_token="xapp-test"),
                owner_socket,
                owner_app,
            ),
        ],
        click,
    )

    assert acked_by is owner_socket
    assert non_owner_socket.acked_envelope_ids == []
    assert owner_socket.acked_envelope_ids == ["env-shared-note-open"]
    assert non_owner.calls == []
    assert owner.calls == []
    non_owner_web.views_open.assert_not_called()
    _assert_ownership_miss_ephemeral(non_owner_web)
    owner_web.views_open.assert_called_once()
    owner_web.chat_postEphemeral.assert_not_called()


def test_an_unrelated_404_does_not_claim_cross_release_ownership() -> None:
    """A missing proxy route and a missing approval row are different failures."""

    assert (
        _refusal_text(ResolveOutcome(status_code=404, detail="Not Found"))
        == "Resolving failed; try again shortly."
    )


def test_an_approval_record_miss_asks_for_the_owning_release() -> None:
    """The exact API row miss is a durable misconfiguration, not a retry."""

    refusal = _refusal_text(ResolveOutcome(status_code=404, detail="approval not found"))
    folded = refusal.casefold()

    assert "nothing was changed" in folded
    assert "does not have this approval" in folded
    assert "try again" not in folded
    assert "disconnect" in folded
    assert "socket mode" in folded
    assert "do not retry from this side" in folded


def test_modal_submit_posts_the_note_with_a_chat_principal(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """A modal's private metadata may locate a card, never assert its actor.

    The Socket Mode envelope is the identity source. Even though the modal
    reconstructs its channel from private metadata, the outbound proof must
    bind the real authenticated user, that card channel, and the approval id.
    """

    resolver, captured = _wire_resolver()
    app, _ = _build(config, redis_client, resolver)  # type: ignore[arg-type]
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(),
        _note_submit(
            "env-principal-modal",
            note="approved for Q3",
            user="U_MODAL_APPROVER",
        ),
    )
    _drain(app)

    assert len(captured.requests) == 1
    request = captured.requests[0]
    assert request["json"] == {
        "decision": "approved",
        "note": "approved for Q3",
    }
    claims = _claims(request["headers"]["X-Curie-Approval-Principal"])
    expected_claims = {
        "approval_id": APPROVAL_ID,
        "sub": "U_MODAL_APPROVER",
        "actor_channel": CARD_CHANNEL,
        "kind": "chat",
        "scope": "approval.resolve",
    }
    assert {key: claims[key] for key in expected_claims} == expected_claims
    assert isinstance(claims["exp"], int) and claims["exp"] > 0


def test_failed_modal_open_falls_forward_with_the_same_chat_principal(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The note enrichment failure must not fall back to asserted identity."""

    resolver, captured = _wire_resolver()
    app, _ = _build(
        config,
        redis_client,
        resolver,  # type: ignore[arg-type]
        views_open_raises=True,
    )
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(),
        _note_click(
            "env-principal-fall-forward",
            action_id=APPROVE_NOTE_ACTION_ID,
            user="U_FALL_FORWARD_APPROVER",
        ),
    )
    _drain(app)

    assert [request.get("method", "POST") for request in captured.requests] == ["GET", "POST"]
    request = captured.requests[1]
    assert request["json"] == {"decision": "approved"}
    claims = _claims(request["headers"]["X-Curie-Approval-Principal"])
    expected_claims = {
        "approval_id": APPROVAL_ID,
        "sub": "U_FALL_FORWARD_APPROVER",
        "actor_channel": CARD_CHANNEL,
        "kind": "chat",
        "scope": "approval.resolve",
    }
    assert {key: claims[key] for key in expected_claims} == expected_claims
    assert isinstance(claims["exp"], int) and claims["exp"] > 0


def test_a_blank_note_resolves_with_no_note_at_all(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    # An empty string is not the same statement as leaving it blank: it would
    # persist as a resolution_note the resume turn interpolates as "Note: .".
    resolver = ScriptedResolver(ResolveOutcome(status_code=200, resolved_by="U_MANAGER"))
    app, _ = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_submit("env-n3", note="   "))
    _drain(app)

    assert len(resolver.calls) == 1
    assert resolver.calls[0]["note"] is None


def test_a_reject_dialog_carries_the_rejected_decision(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(ResolveOutcome(status_code=200, resolved_by="U_MANAGER"))
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_click("env-n4", action_id=REJECT_NOTE_ACTION_ID))
    _drain(app)

    import json

    meta = json.loads(web_client.views_open.call_args.kwargs["view"]["private_metadata"])
    assert meta["decision"] == "rejected"


def test_cancelling_the_dialog_leaves_the_record_pending(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """A cancel produces no view_submission at all, so nothing resolves.

    Driven as the real sequence -- open the dialog, then deliver nothing -- which
    is exactly what Slack sends when the approver hits Cancel. This is stricter
    than the pre-#1053 behavior (a click WAS the decision) and the strictness is
    the point, so it gets its own guard.
    """

    resolver = ScriptedResolver(ResolveOutcome(status_code=200, resolved_by="U_MANAGER"))
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_click("env-n5", action_id=APPROVE_NOTE_ACTION_ID))
    _drain(app)

    web_client.views_open.assert_called_once()
    assert resolver.calls == []
    web_client.chat_update.assert_not_called()


def test_a_claim_race_loser_is_told_inside_the_view(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The regression this change could most easily introduce.

    Resolution now happens at submit rather than at click, so two approvers can
    hold open dialogs. The compare-and-set still makes one win; the loser is
    inside a modal, where a chat.postEphemeral posts BEHIND the open view and is
    never seen. The refusal must therefore come back as the view's own ack.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=409, resolved_by="U_FIRST", detail="already resolved")
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _note_submit("env-n6", note="mine"))
    _drain(app)

    payload = sock.ack_payload_for("env-n6")
    assert payload is not None, "a refused submission must ack with a response_action"
    assert payload.get("response_action") == "errors"
    assert "U_FIRST" in next(iter(payload["errors"].values()))
    # And it must NOT fall back to the invisible surface.
    web_client.chat_postEphemeral.assert_not_called()


def test_a_refused_submission_does_not_stamp_the_card(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(
        ResolveOutcome(
            status_code=403,
            detail="operator approval principals require an explicit user list",
        )
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _note_submit("env-n7", note="please"))
    _drain(app)

    web_client.chat_update.assert_not_called()
    payload = sock.ack_payload_for("env-n7")
    assert payload is not None
    assert "explicit user list" in next(iter(payload["errors"].values()))


def test_a_failed_views_open_falls_forward_and_resolves(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """A trigger_id is valid for ~3s, so views.open can genuinely fail.

    The human already expressed the decision by clicking; refusing it because an
    OPTIONAL enrichment could not be collected would leave the record pending
    with no feedback, which is worse than losing the note. So it resolves, and
    says so.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(config, redis_client, resolver, views_open_raises=True)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_click("env-n8", action_id=APPROVE_NOTE_ACTION_ID))
    _drain(app)

    assert len(resolver.calls) == 1
    assert resolver.calls[0]["note"] is None
    web_client.chat_update.assert_called_once()
    # Nothing here is silent: the approver is told the note step was skipped.
    web_client.chat_postEphemeral.assert_called_once()
    assert "note" in web_client.chat_postEphemeral.call_args.kwargs["text"].lower()


def test_a_failed_views_open_reports_a_403_refusal_to_the_clicker(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """#1085: the fall-forward handled one failure but not two.

    When ``views.open`` fails AND the fall-forward resolve is then refused, the
    in-view error channel cannot fire (there is no view, that is why we are on
    this path) and the ephemeral used to be gated on a 200. The clicker got
    nothing at all, which for a non-approver is indistinguishable from the
    platform being down.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(
            status_code=403,
            detail="operator approval principals require an explicit user list",
        )
    )
    app, web_client = _build(config, redis_client, resolver, views_open_raises=True)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_click("env-n9", action_id=APPROVE_NOTE_ACTION_ID))
    _drain(app)

    assert len(resolver.calls) == 1, "the fall-forward must still attempt the resolve"
    web_client.chat_postEphemeral.assert_called_once()
    text = web_client.chat_postEphemeral.call_args.kwargs["text"]
    # The API's own reason, verbatim: the refusal classes stay distinguishable
    # (#453 AC5), so this must not read like a generic failure.
    assert "explicit user list" in text, text
    # A refusal is not a resolution: the card keeps its live buttons.
    web_client.chat_update.assert_not_called()


def test_a_failed_views_open_reports_an_expiry_to_the_clicker(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The 410 sibling of the case above, and the one where silence costs most:
    the clicker's only question is why nothing happened, and expiry is the
    answer."""

    resolver = ScriptedResolver(ResolveOutcome(status_code=410, detail="approval expired"))
    app, web_client = _build(config, redis_client, resolver, views_open_raises=True)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_click("env-n10", action_id=REJECT_NOTE_ACTION_ID))
    _drain(app)

    web_client.chat_postEphemeral.assert_called_once()
    text = web_client.chat_postEphemeral.call_args.kwargs["text"]
    assert "expired" in text.lower(), text
    web_client.chat_update.assert_not_called()


def test_the_fall_forward_ephemeral_matches_the_in_view_wording(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """One refusal, one wording, whichever surface it lands on.

    The two paths differ only in WHERE a refusal is rendered (an ephemeral when
    no view opened, the view's own ack when one did). If they worded it
    differently, the same refusal would read as two different problems.
    """

    outcome = ResolveOutcome(status_code=409, resolved_by="U_FIRST")
    resolver = ScriptedResolver(outcome)
    app, web_client = _build(config, redis_client, resolver, views_open_raises=True)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_click("env-n11", action_id=APPROVE_NOTE_ACTION_ID))
    _drain(app)

    web_client.chat_postEphemeral.assert_called_once()
    assert web_client.chat_postEphemeral.call_args.kwargs["text"] == _refusal_text(outcome)


def test_the_ack_lands_before_any_slack_call_on_the_submit_path(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The ack must not be waiting on a Slack round trip (#1077).

    Slack gives an interaction three seconds to be acknowledged
    (https://api.slack.com/interactivity/handling#acknowledgment_response),
    which slack_bolt 1.30.0 mirrors as ``ack_timeout: int = 3`` on
    ``slack_bolt.listener.custom_listener.CustomListener``. The ack itself is a
    two-thread affair: the listener body runs on Bolt's ``listener_executor`` and
    only sets ``ack.response``, while the dispatch thread polls for it
    (``slack_bolt.listener.thread_runner.ThreadListenerRunner.run``) and then
    writes the envelope response
    (``slack_bolt.adapter.socket_mode.internals.send_response``).

    So the proof is the assertion made WHILE ``conversations.history`` is still
    blocked: the ack reached the socket before that call returned, therefore it
    cannot have been waiting on it. A "who was recorded first" list would not
    prove this -- the listener thread starts the fetch the instant it acks, well
    before the dispatch thread's 10ms poll wakes up to send the response.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    gate = threading.Event()
    app, web_client = _build(
        config, redis_client, resolver, history_side_effect=_gated_history(gate)
    )
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    try:
        handler.handle(sock, _note_submit("env-slow", note="shipping it"))
        assert sock.acked_envelope_ids == ["env-slow"], (
            "the submit was not acked while the Slack call was still outstanding"
        )
    finally:
        # In a finally so a failed assertion cannot leave the listener parked.
        gate.set()
    _drain(app)

    # The fetch still happens; it just happens after the ack.
    web_client.conversations_replies.assert_called_once()


def test_a_refused_submission_acks_before_the_card_read_and_still_refreshes_it(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The claim-race loser's refusal is on time AND the stale card is refreshed.

    The refusal rides on the view ack, so it is subject to the same three second
    deadline as the happy path: a loser who is told late is told nothing, because
    Slack has already closed the interaction. And the 409 branch's card refresh
    is the piece most likely to be dropped when the pre-ack work is split off, so
    it gets asserted after the gate is released.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(
            status_code=409,
            resolved_by=None,
            detail="already resolved by U_FIRST (approved)",
        )
    )
    gate = threading.Event()
    app, web_client = _build(
        config, redis_client, resolver, history_side_effect=_gated_history(gate)
    )
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    try:
        handler.handle(sock, _note_submit("env-slow-409", note="mine"))
        payload = sock.ack_payload_for("env-slow-409")
        assert payload is not None, (
            "a refused submission must ack with a response_action before any Slack call"
        )
        assert payload.get("response_action") == "errors"
        assert next(iter(payload["errors"].values())) == "already resolved by U_FIRST (approved)"
    finally:
        gate.set()
    _drain(app)

    # _refresh_settled_card must survive the split: a settled record must stop
    # offering buttons.
    web_client.chat_update.assert_called_once()


def test_a_failing_card_read_still_acks_and_leaves_the_card_intact(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The other half of the post-ack hazard: a Slack call that FAILS (#1077).

    A raise from the post-ack half is not a logged traceback, it is a lost ack.
    slack_bolt 1.30.0 handles an exception out of a non-auto-ack listener by
    setting ``ack.response = None`` when the listener had already acked
    (``slack_bolt.listener.thread_runner.ThreadListenerRunner.run``, the
    ``run_ack_function_asynchronously`` branch), while the dispatch thread is
    still polling ``ack.response`` in its 10ms loop. Lose that race and the
    dispatch thread polls to its three second timeout and sends nothing at all,
    leaving the modal open on an interaction Slack considers unacknowledged.

    That executor also SWALLOWS the exception, so calling the listener and
    checking that nothing propagated proves nothing. The observation that has
    teeth is the socket's: the envelope was acked.

    #1073 changed what happens to the CARD here, and the change is the point.
    This used to assert the verdict was stamped from the empty message a failed
    read falls back to -- which is the wipe: it replaces the record of what was
    approved with one line. An unread card is now left alone. The resolution
    still stands (it happened before the ack), the ack still lands, and the only
    cost is that the buttons stay up until the next click reports the record as
    already resolved.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(
        config,
        redis_client,
        resolver,
        history_side_effect=SlackApiError(
            "channel_not_found", {"ok": False, "error": "channel_not_found"}
        ),
    )
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _note_submit("env-hist-fail", note="shipping it"))
    assert sock.acked_envelope_ids == ["env-hist-fail"], (
        "a failing post-ack Slack call ate the submit's ack"
    )
    _drain(app)

    web_client.conversations_replies.assert_called_once()
    # The card is NOT stamped from an unread original (#1073): destroying the
    # summary is a worse outcome than leaving a settled record looking clickable.
    web_client.chat_update.assert_not_called()
    # And the decision itself was never in doubt -- it landed before the ack.
    assert len(resolver.calls) == 1
    assert resolver.calls[0]["note"] == "shipping it"


def test_an_unreadable_card_is_left_intact_rather_than_wiped(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """#1073's core regression, as the empty READ rather than a failing one.

    ``conversations.history`` does not return thread replies, and the DEFAULT
    card is one: with no route bound the card posts into the requesting thread.
    So the read came back ``{"messages": []}`` for every unrouted approval, and
    the stamp then wrote a card rebuilt from nothing -- header, summary and
    "Requested by" all replaced by a single verdict line.

    Two things now prevent that, and this pins the second: the read moved to
    ``conversations.replies`` (which returns both card shapes), AND an unread
    card is not stamped at all. The guard matters on its own because the whole
    failure was invisible -- both assertions the shipped test made passed
    identically against the wiped output.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(config, redis_client, resolver)
    # The pre-#1073 symptom exactly: a read that succeeds and returns nothing.
    web_client.conversations_replies = MagicMock(return_value={"messages": []})  # type: ignore[method-assign]
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_submit("env-wipe", note="approved for Q3"))
    _drain(app)

    assert len(resolver.calls) == 1, "the decision must still land"
    web_client.chat_update.assert_not_called()


def test_the_card_read_asks_for_the_thread_not_the_channel_timeline(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The read must be ``conversations.replies`` keyed on the card's own ts.

    The unrouted card is a thread REPLY. ``conversations.history`` walks the
    channel timeline and never returns one, which is why it answered empty;
    ``conversations.replies`` accepts a reply's own ts as well as a parent's, so
    it reads both card shapes. Asserting the call SHAPE, not just the outcome,
    because a later refactor back onto ``history`` would pass every
    outcome-level assertion in this file against a stubbed client.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_submit("env-replies", note="ok"))
    _drain(app)

    web_client.conversations_replies.assert_called_once()
    kwargs = web_client.conversations_replies.call_args.kwargs
    assert kwargs["channel"] == CARD_CHANNEL
    assert kwargs["ts"] == CARD_TS, "the card's OWN ts, not a parent's"


def test_a_stamped_card_keeps_its_summary_in_blocks_and_in_the_fallback(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The positive half: a readable card keeps its body, and so does ``text``.

    ``text`` is what notifications, previews and screen readers show, so a stamp
    that kept the blocks but overwrote the fallback with the verdict alone still
    loses the summary from all three (#1073's third AC).
    """

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_submit("env-keep", note="approved for Q3"))
    _drain(app)

    kwargs = web_client.chat_update.call_args.kwargs
    rendered = list(kwargs["blocks"])
    assert rendered[0]["type"] == "header", "the header must survive the stamp"
    assert any("Discount for ACME" in (b.get("text") or {}).get("text", "") for b in rendered), (
        f"the summary block must survive the stamp, got {rendered}"
    )
    assert not any(b.get("type") == "actions" for b in rendered), "buttons must go"
    assert "Discount for ACME" in kwargs["text"], "the fallback must carry the summary"
    assert "approved for Q3" in kwargs["text"]


def test_a_claim_race_refresh_does_not_wipe_an_unreadable_card(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The 409 path runs the same rebuild, so it carries the same hazard
    (#1073's second AC). A stale-button refresh is worth strictly less than the
    card body it would cost."""

    resolver = ScriptedResolver(ResolveOutcome(status_code=409, resolved_by="U_FIRST"))
    app, web_client = _build(config, redis_client, resolver)
    web_client.conversations_replies = MagicMock(return_value={"messages": []})  # type: ignore[method-assign]
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_submit("env-race-wipe", note="mine"))
    _drain(app)

    web_client.chat_update.assert_not_called()


def test_a_conflict_from_another_approver_still_names_that_approver(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The loser of a claim race must be told WHO holds the decision.

    Grounded on the outcome ``ApprovalResolveClient`` really builds from the
    platform API's conflict (see the test below): ``resolved_by`` is None and the
    winning approver's id lives inside the detail string. So the id reaches the
    submitter only if the detail is rendered verbatim; a wording that dropped it
    would leave the loser of a race told nothing about who took the decision.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(
            status_code=409,
            resolved_by=None,
            detail="already resolved by U_FIRST (approved)",
        )
    )
    app, _ = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _note_submit("env-409-other", note="mine", user="U_MANAGER"))
    _drain(app)

    payload = sock.ack_payload_for("env-409-other")
    assert payload is not None
    assert next(iter(payload["errors"].values())) == "already resolved by U_FIRST (approved)"


def test_a_real_api_conflict_carries_no_resolver_id_and_names_the_approver_in_its_detail() -> None:
    """The conflict the platform API actually sends, parsed by the real client.

    ``curie_api.routers.approvals.resolve_approval`` raises its resolve-path
    conflict as ``HTTPException(status.HTTP_409_CONFLICT, f"already resolved by
    {current.resolved_by} ({current.status})")``, and FastAPI serializes an
    ``HTTPException`` as ``{"detail": <str>}`` and nothing else. There is no
    ``resolved_by`` key in the body, so ``ResolveOutcome.resolved_by`` is None on
    every real conflict and the winning approver's id survives only inside the
    detail text.

    Driven through ``ApprovalResolveClient.resolve`` itself, over an
    ``httpx.MockTransport`` injected via its existing ``client`` parameter, so
    nothing between the wire body and the outcome is faked. Every other test in
    this file hands a ``ResolveOutcome`` straight to the app, which is why they
    cannot see the gap between the shape the API sends and the shape the
    dispatcher assumes.
    """

    def _conflict(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith(f"/approvals/{APPROVAL_ID}/resolve")
        return httpx.Response(409, json={"detail": "already resolved by U_MANAGER (approved)"})

    client = ApprovalResolveClient(
        api_base_url="http://platform.invalid",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=httpx.Client(transport=httpx.MockTransport(_conflict)),
    )
    try:
        outcome = client.resolve(
            APPROVAL_ID,
            decision="approved",
            attested_user="U_MANAGER",
            attested_channel=CARD_CHANNEL,
            note="mine",
        )
    finally:
        client._client.close()

    assert outcome.status_code == 409
    assert outcome.resolved_by is None
    assert outcome.detail == "already resolved by U_MANAGER (approved)"
    # So the only thing there is to tell the approver is what the API said. A
    # wording composed from ``resolved_by`` would render "Already resolved by
    # None." here, which is why that field must not be trusted on a 409.
    assert _refusal_text(outcome) == "already resolved by U_MANAGER (approved)"


def test_a_discarded_outcome_does_not_read_the_card_at_all(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """Only the 200 and 409 renders use the fetched message, so every other
    outcome pays a Slack round trip for a message nobody reads -- and pays it by
    holding one of Bolt's five shared listener workers while it waits.

    The contrast is asserted on the paths that DO need it: the 200 case in
    ``test_the_ack_lands_before_any_slack_call_on_the_submit_path`` and the 409
    case in ``test_a_refused_submission_acks_before_the_card_read_and_still_refreshes_it``.
    """

    resolver = ScriptedResolver(
        ResolveOutcome(
            status_code=403,
            detail="operator approval principals require an explicit user list",
        )
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_submit("env-403-no-fetch", note="please"))
    _drain(app)

    web_client.conversations_replies.assert_not_called()


def test_the_web_client_gives_up_inside_the_ack_budget(config: DispatcherConfig) -> None:
    """slack_sdk 3.43.0 defaults ``timeout`` to 30 seconds
    (``slack_sdk.web.base_client.BaseClient.__init__``), an order of magnitude
    past Slack's three second interaction deadline
    (https://api.slack.com/interactivity/handling#acknowledgment_response).

    Asserted against the deadline rather than pinned to an exact number, so
    retuning the value does not red this test.
    """

    assert build_web_client(config).timeout < 3


def test_the_resolver_gives_up_inside_the_ack_budget() -> None:
    """The resolve call is the only network hop left inside the ack budget.

    Behavioral, against a real loopback listener that accepts the connection and
    never replies -- a local server, not a mocked dependency. Asserting the
    elapsed wall clock rather than a private timeout attribute means the test
    survives an internal rename and still fails if the budget is blown by some
    other means (a retry loop, a second hop).
    """

    with _black_hole_api() as url:
        client = ApprovalResolveClient(
            api_base_url=url,
            api_key=_PLATFORM_API_KEY,
            approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        )
        try:
            started = time.monotonic()
            outcome = client.resolve(
                APPROVAL_ID,
                decision="approved",
                attested_user="U_MANAGER",
                attested_channel=CARD_CHANNEL,
            )
            elapsed = time.monotonic() - started
        finally:
            # The client owns an httpx.Client (and its connection pool) and
            # exposes no close(), so releasing it here is the only deterministic
            # teardown; left to the garbage collector it leaks a socket into the
            # rest of the session. Reached through the attribute deliberately: if
            # that attribute is renamed this test should red rather than quietly
            # leak again.
            client._client.close()

    # The httpx.HTTPError path: the approver gets "try again shortly" inside the
    # still-open modal instead of a blown ack.
    assert outcome.status_code == 0
    assert elapsed < 3.0, f"the resolver held the ack budget for {elapsed:.1f}s"


def test_an_over_long_note_cannot_break_the_card_stamp(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """Slack caps a text object's ``text`` at 3000 characters
    (https://api.slack.com/reference/block-kit/composition-objects#text), so a
    long note concatenated onto the attribution produces a card edit real Slack
    rejects. ``chat_update`` is a MagicMock here and accepts anything, so the
    assertion that catches it is the LENGTH, not an exception.
    """

    note = "x" * 3000
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_submit("env-long", note=note))
    _drain(app)

    web_client.chat_update.assert_called_once()
    kwargs = web_client.chat_update.call_args.kwargs

    # The capped object is the CONTEXT BLOCK's text, which is what Slack
    # documents a limit for. Since #1073 the ``text=`` kwarg is the notification
    # fallback and deliberately carries the card summary as well, so asserting
    # the block cap there would be testing the wrong object.
    verdict = kwargs["blocks"][-1]["elements"][0]["text"]
    assert len(verdict) <= 2900
    # The note is what gets cut, never the attribution.
    assert verdict.startswith("Approved by <@U_MANAGER>")
    # A single U+2026, not three periods: the marker is shared with the worker's
    # ``_truncate`` in ``curie_worker.blocks``, so the same Slack card surface
    # does not end truncated text two ways depending on which service stamped it.
    assert verdict.endswith("…")

    # The fallback is bounded too, or a long body loses the edit entirely.
    text = kwargs["text"]
    assert len(text) <= 39000
    assert text.startswith("Approved by <@U_MANAGER>")
    # And it still carries the summary, which is the point of #1073's change.
    assert "Discount for ACME" in text
    assert not any(
        b.get("type") == "actions" for b in web_client.chat_update.call_args.kwargs["blocks"]
    )
    # Only the display is clamped: the durable record keeps the whole note, which
    # is what the requester's resume turn interpolates.
    assert resolver.calls[0]["note"] == note


def test_the_note_modal_declares_a_limit_below_the_card_clamp(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The human-facing guard and the structural backstop must not converge.

    Slack shows a counter and blocks submit at the input's ``max_length``. If it
    equalled the verdict-line clamp the truncation branch would be unreachable
    through the modal, so the two are asserted to be ordered rather than equal.
    """

    resolver = ScriptedResolver(ResolveOutcome(status_code=200, resolved_by="U_MANAGER"))
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _note_click("env-lim", action_id=APPROVE_NOTE_ACTION_ID))
    _drain(app)

    element = web_client.views_open.call_args.kwargs["view"]["blocks"][0]["element"]
    assert "max_length" in element, "the modal must declare the note limit to the human"
    assert element["max_length"] > 0
    assert element["max_length"] < _VERDICT_LINE_MAX
