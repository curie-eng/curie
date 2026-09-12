"""Click-to-resolve through the real Bolt Socket Mode handler, offline (#246).

Same discipline as test_dispatch.py: the envelope is driven through Bolt's real
``SocketModeHandler``; only the socket, the Web API client, and the platform
API (a scripted ``ApprovalResolveClient`` stand-in) are faked. Asserts the
acceptance behaviors: an authorized click resolves and stamps the card, a
non-approver gets the ephemeral rejection, a claim-race loser gets "already
resolved by X", and the ordinary-button catch-all never double-handles an
approval click.
"""

import ast
import base64
import hashlib
import hmac
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from curie_dispatcher.app import build_app
from curie_dispatcher.approval_actions import (
    _APPROVAL_ACTION_IDS,
    _DECISION_BY_ACTION_ID,
    APPROVE_ACTION_ID,
    APPROVE_NOTE_ACTION_ID,
    REJECT_ACTION_ID,
    ApprovalResolveClient,
    ResolveOutcome,
    settled_verdict_line,
)
from curie_dispatcher.approval_principal import mint_chat_principal
from curie_dispatcher.config import DispatcherConfig
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient

from .conftest import FakeSocketClient, _authorize, deliver_once, deliver_until_acked
from .test_approval_note_dialog import _note_click, _note_submit

APPROVAL_ID = "9a1e8a10-0000-0000-0000-000000000246"
_PLATFORM_API_KEY = "platform-api-test-key"
_CHAT_ATTESTER_SECRET = "dispatcher-attester-test-secret"
_RESOLVE_TRACE_ID = int("4123456789abcdef0123456789abcdef", 16)
_RESOLVE_SPAN_ID = int("4123456789abcdef", 16)
_RESOLVE_TRACEPARENT = "00-4123456789abcdef0123456789abcdef-4123456789abcdef-01"


@contextmanager
def _resolve_parent() -> Iterator[None]:
    parent = SpanContext(
        trace_id=_RESOLVE_TRACE_ID,
        span_id=_RESOLVE_SPAN_ID,
        is_remote=True,
        trace_flags=TraceFlags.SAMPLED,
        trace_state=TraceState(),
    )
    token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(parent)))
    try:
        yield
    finally:
        otel_context.detach(token)


def test_dispatcher_minter_matches_the_api_wire_vector() -> None:
    vector = json.loads(
        (Path(__file__).parents[3] / "tests/vectors/approval-principal.json").read_text()
    )
    claims = vector["claims"]
    token = mint_chat_principal(
        vector["secret"],
        subject=claims["sub"],
        actor_channel=claims["actor_channel"],
        approval_id=claims["approval_id"],
        now=vector["issued_at"],
    )
    assert token == vector["token"]


_CARD_MESSAGE = {
    "ts": "1700.0042",
    "thread_ts": "1700.0001",
    "blocks": [
        {"type": "header", "text": {"type": "plain_text", "text": "Approval required"}},
        {"type": "section", "text": {"type": "mrkdwn", "text": "Discount for ACME"}},
        {"type": "actions", "elements": []},
    ],
}

# Real API reason strings, copied verbatim (not imported -- the dispatcher
# does not depend on apps/api). Sources: apps/api/src/curie_api/authorizer.py
# (_PRINCIPAL_ELIGIBILITY_REASON) and apps/api/src/curie_api/slack_approvers.py (the
# channel non-membership reason and the group-lookup could-not-verify reason).
# The API side pins its own half of this contract in
# apps/api/tests/test_approvers_port.py and apps/api/tests/test_approvals.py.
_CHANNEL_NON_APPROVER_REASON = "you are not an approver: resolve this from the approval's channel"
_PRINCIPAL_ELIGIBILITY_REASON = (
    "operator approval principals can resolve only routes bound to an explicit user list"
)
_COULD_NOT_VERIFY_GROUP_REASON = (
    "could not verify approver group membership: this approval's route is "
    "bound to a Slack user group and the membership lookup failed"
)


class ScriptedResolver:
    """Stands in for the platform API: returns a scripted outcome per call."""

    def __init__(self, outcome: ResolveOutcome) -> None:
        self.outcome = outcome
        self.calls: list[dict[str, str]] = []

    def resolve(
        self,
        approval_id: str,
        *,
        decision: str,
        attested_user: str,
        attested_channel: str,
        note: str | None = None,
    ) -> ResolveOutcome:
        # `note` is recorded, not ignored: the dialog path's whole point is that
        # the approver's reason reaches the record, and a stand-in that dropped
        # it would let that regress silently (#1053).
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
        # Mirror the production ownership probe: only the exact API row-miss
        # is "not this release". Any other outcome means this release has a
        # row (or the probe failed open).
        del approval_id
        return not (
            self.outcome.status_code == 404
            and self.outcome.detail.strip().casefold() == "approval not found"
        )


def _build(
    config: DispatcherConfig, redis_client: redis.Redis, resolver: ScriptedResolver
) -> tuple[App, WebClient]:
    web_client = WebClient(token="xoxb-test")
    web_client.chat_postMessage = MagicMock(return_value={"ts": "555.000"})  # type: ignore[method-assign]
    web_client.chat_update = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.chat_postEphemeral = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
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


def _approval_click(
    envelope_id: str, *, action_id: str, user: str = "U_MANAGER"
) -> SocketModeRequest:
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
            "container": {"type": "message", "message_ts": _CARD_MESSAGE["ts"]},
            "channel": {"id": "C_MGRS"},
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


def _stub_dialog_web(web_client: WebClient) -> None:
    web_client.views_open = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.conversations_replies = MagicMock(  # type: ignore[method-assign]
        return_value={"messages": [_CARD_MESSAGE]},
    )


_ONESHOT_TWO_RELEASE_TESTS = (
    "test_two_releases_oneshot_non_owner_then_owner_resolves_an_immediate_action",
    "test_two_releases_oneshot_non_owner_then_owner_opens_a_note_dialog",
    "test_two_releases_oneshot_non_owner_then_owner_resolves_a_note_submission",
)


def test_authorized_click_resolves_and_stamps_the_card(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _approval_click("env-a1", action_id=APPROVE_ACTION_ID))
    assert sock.acked_envelope_ids == ["env-a1"]
    _drain(app)

    # The resolver receives values Slack authenticated, rather than body fields
    # the caller asserts. Its real HTTP implementation attests these values.
    assert resolver.calls == [
        {
            "approval_id": APPROVAL_ID,
            "decision": "approved",
            "attested_user": "U_MANAGER",
            "attested_channel": "C_MGRS",
            # The immediate pair carries no note by construction; only the
            # dialog pair can collect one (#1053).
            "note": None,
        }
    ]

    # The card was stamped in place: buttons gone, verdict context appended.
    web_client.chat_update.assert_called_once()
    kwargs = web_client.chat_update.call_args.kwargs
    assert kwargs["channel"] == "C_MGRS" and kwargs["ts"] == _CARD_MESSAGE["ts"]
    assert all(b["type"] != "actions" for b in kwargs["blocks"])
    assert "Approved by <@U_MANAGER>" in kwargs["text"]

    # No turn was enqueued and no placeholder posted: the catch-all skipped it.
    web_client.chat_postMessage.assert_not_called()
    web_client.chat_postEphemeral.assert_not_called()


def test_reject_button_resolves_with_rejected_decision(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="rejected")
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _approval_click("env-r1", action_id=REJECT_ACTION_ID))
    _drain(app)

    assert resolver.calls[0]["decision"] == "rejected"
    assert "Rejected by <@U_MANAGER>" in web_client.chat_update.call_args.kwargs["text"]


def test_two_releases_only_the_owner_resolves_an_immediate_action(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """One fake Slack app, two dispatchers: only the owner consumes the click (#2248).

    Slack delivers the same envelope to the non-owner first. That release must
    leave the envelope unacked (so Slack retries) and must not mutate the card
    or post an ephemeral. The retry reaches the owner, who acks and stamps.
    """

    non_owner = ScriptedResolver(ResolveOutcome(status_code=404, detail="approval not found"))
    owner = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    non_owner_app, non_owner_web = _build(config, redis_client, non_owner)
    owner_app, owner_web = _build(config, redis_client, owner)
    non_owner_socket = FakeSocketClient()
    owner_socket = FakeSocketClient()
    click = _approval_click("env-shared-immediate", action_id=APPROVE_ACTION_ID)

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
    assert owner_socket.acked_envelope_ids == ["env-shared-immediate"]
    assert len(non_owner.calls) == 1
    assert len(owner.calls) == 1
    non_owner_web.chat_update.assert_not_called()
    non_owner_web.chat_postMessage.assert_not_called()
    non_owner_web.chat_postEphemeral.assert_not_called()
    owner_web.chat_update.assert_called_once()
    assert "Approved by <@U_MANAGER>" in owner_web.chat_update.call_args.kwargs["text"]
    owner_web.chat_postEphemeral.assert_not_called()
    assert any(
        "may be owned by another Curie release" in record.getMessage() for record in caplog.records
    )


def test_two_releases_oneshot_non_owner_then_owner_resolves_an_immediate_action(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """One delivery to the non-owner, then one to the owner. No retry loop (#2307)."""

    non_owner = ScriptedResolver(ResolveOutcome(status_code=404, detail="approval not found"))
    owner = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    non_owner_app, non_owner_web = _build(config, redis_client, non_owner)
    owner_app, owner_web = _build(config, redis_client, owner)
    non_owner_socket = FakeSocketClient()
    owner_socket = FakeSocketClient()
    click = _approval_click("env-oneshot-immediate", action_id=APPROVE_ACTION_ID)
    non_owner_handler = SocketModeHandler(non_owner_app, app_token="xapp-test")
    owner_handler = SocketModeHandler(owner_app, app_token="xapp-test")

    deliver_once(non_owner_handler, non_owner_socket, non_owner_app, click)

    assert non_owner_socket.acked_envelope_ids == []
    non_owner_web.chat_update.assert_not_called()
    non_owner_web.chat_postMessage.assert_not_called()
    non_owner_web.chat_postEphemeral.assert_not_called()

    deliver_once(owner_handler, owner_socket, owner_app, click)

    assert owner_socket.acked_envelope_ids == ["env-oneshot-immediate"]
    assert len(non_owner.calls) == 1
    assert len(owner.calls) == 1
    owner_web.chat_update.assert_called_once()
    assert "Approved by <@U_MANAGER>" in owner_web.chat_update.call_args.kwargs["text"]
    owner_web.chat_postEphemeral.assert_not_called()


def test_two_releases_oneshot_non_owner_then_owner_opens_a_note_dialog(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """One-shot note-open: non-owner leaves the envelope, owner opens the dialog."""

    non_owner = ScriptedResolver(ResolveOutcome(status_code=404, detail="approval not found"))
    owner = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    non_owner_app, non_owner_web = _build(config, redis_client, non_owner)
    owner_app, owner_web = _build(config, redis_client, owner)
    _stub_dialog_web(non_owner_web)
    _stub_dialog_web(owner_web)
    non_owner_socket = FakeSocketClient()
    owner_socket = FakeSocketClient()
    click = _note_click("env-oneshot-note-open", action_id=APPROVE_NOTE_ACTION_ID)
    non_owner_handler = SocketModeHandler(non_owner_app, app_token="xapp-test")
    owner_handler = SocketModeHandler(owner_app, app_token="xapp-test")

    deliver_once(non_owner_handler, non_owner_socket, non_owner_app, click)

    assert non_owner_socket.acked_envelope_ids == []
    non_owner_web.chat_update.assert_not_called()
    non_owner_web.chat_postMessage.assert_not_called()
    non_owner_web.chat_postEphemeral.assert_not_called()
    non_owner_web.views_open.assert_not_called()
    assert non_owner.calls == []

    deliver_once(owner_handler, owner_socket, owner_app, click)

    assert owner_socket.acked_envelope_ids == ["env-oneshot-note-open"]
    assert owner.calls == []
    owner_web.views_open.assert_called_once()


def test_two_releases_oneshot_non_owner_then_owner_resolves_a_note_submission(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """One-shot note-submit: non-owner leaves the envelope, owner resolves."""

    non_owner = ScriptedResolver(ResolveOutcome(status_code=404, detail="approval not found"))
    owner = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    non_owner_app, non_owner_web = _build(config, redis_client, non_owner)
    owner_app, owner_web = _build(config, redis_client, owner)
    _stub_dialog_web(non_owner_web)
    _stub_dialog_web(owner_web)
    non_owner_socket = FakeSocketClient()
    owner_socket = FakeSocketClient()
    submit = _note_submit("env-oneshot-note-submit", note="approved for Q3")
    non_owner_handler = SocketModeHandler(non_owner_app, app_token="xapp-test")
    owner_handler = SocketModeHandler(owner_app, app_token="xapp-test")

    deliver_once(non_owner_handler, non_owner_socket, non_owner_app, submit)

    assert non_owner_socket.acked_envelope_ids == []
    non_owner_web.chat_update.assert_not_called()
    non_owner_web.chat_postMessage.assert_not_called()
    non_owner_web.chat_postEphemeral.assert_not_called()

    deliver_once(owner_handler, owner_socket, owner_app, submit)

    assert owner_socket.acked_envelope_ids == ["env-oneshot-note-submit"]
    assert len(non_owner.calls) == 1
    assert len(owner.calls) == 1
    assert owner.calls[0]["note"] == "approved for Q3"
    owner_web.chat_update.assert_called_once()
    assert "approved for Q3" in owner_web.chat_update.call_args.kwargs["text"]


def test_oneshot_two_release_tests_do_not_call_deliver_until_acked() -> None:
    """Retry-loop-only is not the #2307 proof: inspect the one-shot sources."""

    source = Path(__file__).read_text()
    tree = ast.parse(source)
    funcs = {node.name: node for node in tree.body if isinstance(node, ast.FunctionDef)}
    assert "test_two_releases_only_the_owner_resolves_an_immediate_action" in funcs
    for name in _ONESHOT_TWO_RELEASE_TESTS:
        node = funcs.get(name)
        assert node is not None, f"missing one-shot test {name}"
        func_src = ast.get_source_segment(source, node) or ""
        assert "deliver_until_acked" not in func_src, (
            f"{name} must not call deliver_until_acked; retry-loop-only is insufficient"
        )
        assert "deliver_once(" in func_src, f"{name} must deliver via deliver_once"


def test_a_proxy_404_still_consumes_the_envelope(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """A generic ingress 404 is not an ownership miss and must still be acked."""

    resolver = ScriptedResolver(ResolveOutcome(status_code=404, detail="Not Found"))
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()

    handler.handle(sock, _approval_click("env-proxy-404", action_id=APPROVE_ACTION_ID))
    _drain(app)

    assert sock.acked_envelope_ids == ["env-proxy-404"]
    web_client.chat_postEphemeral.assert_called_once()
    assert "try again shortly" in web_client.chat_postEphemeral.call_args.kwargs["text"]


def test_the_ack_lands_before_any_slack_call_on_the_immediate_path(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """Owned immediate clicks ack before chat_update (#2248, #1077)."""

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client = _build(config, redis_client, resolver)
    gate = threading.Event()

    def _gated_update(**_kwargs: Any) -> dict[str, bool]:
        gate.wait(5)
        return {"ok": True}

    web_client.chat_update = MagicMock(side_effect=_gated_update)  # type: ignore[method-assign]
    handler = SocketModeHandler(app, app_token="xapp-test")
    sock = FakeSocketClient()
    try:
        handler.handle(sock, _approval_click("env-slow-immediate", action_id=APPROVE_ACTION_ID))
        assert sock.acked_envelope_ids == ["env-slow-immediate"], (
            "the click was not acked while the Slack call was still outstanding"
        )
    finally:
        gate.set()
    _drain(app)
    web_client.chat_update.assert_called_once()


def test_non_approver_rejection_renders_the_api_reason(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """Strengthened from the original #246 test. The old assertion
    (``"not an approver" in kwargs["text"]``) also passes against the
    hardcoded fixed string. Asserting the SPECIFIC channel reason from
    ``outcome.detail`` is the only way to prove the rendering preserves the
    server's selected-set refusal instead of substituting a generic literal
    (#453 AC4).
    """

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=403, detail=_CHANNEL_NON_APPROVER_REASON)
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(),
        _approval_click("env-f1", action_id=APPROVE_ACTION_ID, user="U_OUTSIDER"),
    )
    _drain(app)

    web_client.chat_postEphemeral.assert_called_once()
    kwargs = web_client.chat_postEphemeral.call_args.kwargs
    assert kwargs["user"] == "U_OUTSIDER"
    # The specific reason, not just the generic "not an approver" phrase a
    # hardcoded string could also satisfy.
    assert "resolve this from the approval's channel" in kwargs["text"]
    # The card is untouched and no turn was enqueued.
    web_client.chat_update.assert_not_called()
    web_client.chat_postMessage.assert_not_called()


def test_principal_eligibility_rejection_is_distinguishable(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """A principal/set eligibility 403 must not read like non-membership."""

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=403, detail=_PRINCIPAL_ELIGIBILITY_REASON)
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(),
        _approval_click("env-f2", action_id=APPROVE_ACTION_ID, user="U_AUTHOR"),
    )
    _drain(app)

    kwargs = web_client.chat_postEphemeral.call_args.kwargs
    assert "explicit user list" in kwargs["text"]
    assert "not an approver" not in kwargs["text"]


def test_could_not_verify_is_not_worded_as_a_policy_denial(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """AC5: an infrastructure/config failure must not be rendered as a policy
    denial. A clicker reading this must not be told they lack permission."""

    resolver = ScriptedResolver(
        ResolveOutcome(status_code=403, detail=_COULD_NOT_VERIFY_GROUP_REASON)
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(),
        _approval_click("env-f3", action_id=APPROVE_ACTION_ID, user="U_OUTSIDER"),
    )
    _drain(app)

    kwargs = web_client.chat_postEphemeral.call_args.kwargs
    assert "could not verify" in kwargs["text"]
    assert "not an approver" not in kwargs["text"]


def test_the_three_refusal_classes_render_distinctly(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """Non-membership, principal eligibility, and could-not-verify are
    distinguishable to the clicker. A hardcoded fixed string can never
    satisfy this, since all three would collapse to one rendered string."""

    rendered: list[str] = []
    for i, detail in enumerate(
        (
            _CHANNEL_NON_APPROVER_REASON,
            _PRINCIPAL_ELIGIBILITY_REASON,
            _COULD_NOT_VERIFY_GROUP_REASON,
        )
    ):
        resolver = ScriptedResolver(ResolveOutcome(status_code=403, detail=detail))
        app, web_client = _build(config, redis_client, resolver)
        handler = SocketModeHandler(app, app_token="xapp-test")
        handler.handle(
            FakeSocketClient(),
            _approval_click(f"env-distinct-{i}", action_id=APPROVE_ACTION_ID, user="U_OUTSIDER"),
        )
        _drain(app)
        rendered.append(web_client.chat_postEphemeral.call_args.kwargs["text"])

    assert len(set(rendered)) == 3


def test_403_with_empty_detail_does_not_assert_policy(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """Edge case: when the API's detail is empty (an infrastructure failure
    with no body), the fallback must stay class-neutral rather than guessing
    at "you are not an approver" (AC5)."""

    resolver = ScriptedResolver(ResolveOutcome(status_code=403, detail=""))
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(),
        _approval_click("env-f5", action_id=APPROVE_ACTION_ID, user="U_OUTSIDER"),
    )
    _drain(app)

    kwargs = web_client.chat_postEphemeral.call_args.kwargs
    assert kwargs["text"]
    assert "not an approver" not in kwargs["text"]


def test_claim_race_loser_sees_already_resolved_by_winner(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(
        ResolveOutcome(
            status_code=409,
            detail="already resolved by U_FIRST (approved)",
            resolved_by=None,
        )
    )
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(),
        _approval_click("env-l1", action_id=APPROVE_ACTION_ID, user="U_SECOND"),
    )
    _drain(app)

    kwargs = web_client.chat_postEphemeral.call_args.kwargs
    assert kwargs["user"] == "U_SECOND"
    assert "already resolved by U_FIRST" in kwargs["text"]
    # The stale card is refreshed so it stops offering buttons.
    assert web_client.chat_update.call_count == 1
    assert all(b["type"] != "actions" for b in web_client.chat_update.call_args.kwargs["blocks"])


def test_expired_click_gets_ephemeral_expiry_notice(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    resolver = ScriptedResolver(ResolveOutcome(status_code=410, detail="expired"))
    app, web_client = _build(config, redis_client, resolver)
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(FakeSocketClient(), _approval_click("env-x1", action_id=APPROVE_ACTION_ID))
    _drain(app)

    assert "expired" in web_client.chat_postEphemeral.call_args.kwargs["text"]


class _FakeHttpResponse:
    """A non-JSON HTTP response body, as an intermediary (ingress/WAF) in
    front of the API might return instead of FastAPI's own JSON 403."""

    def __init__(self, *, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text

    def json(self) -> Any:
        raise ValueError("not json")


class _FakeHttpClient:
    """Stands in for httpx.Client: the only external boundary resolve() calls."""

    def __init__(self, response: _FakeHttpResponse) -> None:
        self._response = response

    def post(self, url: str, *, json: dict[str, Any], headers: dict[str, str]) -> _FakeHttpResponse:
        return self._response


class _CapturingHttpResponse:
    status_code = 200
    text = ""

    def json(self) -> dict[str, str]:
        return {"status": "approved", "resolved_by": "U_MANAGER"}


class _CapturingHttpClient:
    """The resolver's only outbound boundary, recorded without weakening it."""

    def __init__(self) -> None:
        self.requests: list[dict[str, Any]] = []

    def post(
        self, url: str, *, json: dict[str, Any], headers: dict[str, str]
    ) -> _CapturingHttpResponse:
        self.requests.append({"url": url, "json": json, "headers": headers})
        return _CapturingHttpResponse()


def _assert_chat_attestation(
    token: str,
    *,
    approval_id: str,
    user: str,
    channel: str,
) -> None:
    """Pin the attestation's wire codec without importing the API verifier.

    The dispatcher must prove the Slack-authenticated user, the actual channel,
    and this exact approval record. Importing the server's verifier here would
    let a shared bug make both sides pass, so this is an independent stdlib
    check of the compact HMAC contract.
    """

    prefix, encoded_claims, signature = token.split(".")
    assert prefix == "apr"
    signing_input = f"{prefix}.{encoded_claims}"
    expected = (
        base64.urlsafe_b64encode(
            hmac.new(
                _CHAT_ATTESTER_SECRET.encode(), signing_input.encode(), hashlib.sha256
            ).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    api_key_signature = (
        base64.urlsafe_b64encode(
            hmac.new(_PLATFORM_API_KEY.encode(), signing_input.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    assert hmac.compare_digest(signature, expected), (
        "the approval principal must be signed with the dedicated Slack attester credential"
    )
    assert not hmac.compare_digest(signature, api_key_signature), (
        "a platform-key-signed principal must not substitute for a chat attestation"
    )
    padded = encoded_claims + "=" * (-len(encoded_claims) % 4)
    claims = json.loads(base64.urlsafe_b64decode(padded))
    assert claims["kind"] == "chat"
    assert claims["scope"] == "approval.resolve"
    assert claims["sub"] == user
    assert claims["actor_channel"] == channel
    assert claims["approval_id"] == approval_id


def test_immediate_click_posts_only_decision_with_an_authenticated_chat_principal(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The simple card path must not retain either caller-asserted field.

    Socket Mode authenticates this user and delivers this channel. The HTTP
    request must carry that proof in ``X-Curie-Approval-Principal`` and no
    token/credential may escape into a Slack render or dispatcher log.
    """

    captured = _CapturingHttpClient()
    resolver = ApprovalResolveClient(
        api_base_url="https://api.example.test",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=captured,  # type: ignore[arg-type]
    )
    app, web_client = _build(config, redis_client, resolver)  # type: ignore[arg-type]
    handler = SocketModeHandler(app, app_token="xapp-test")

    handler.handle(
        FakeSocketClient(), _approval_click("env-principal-immediate", action_id=APPROVE_ACTION_ID)
    )
    _drain(app)

    assert len(captured.requests) == 1
    request = captured.requests[0]
    assert request["url"].endswith(f"/approvals/{APPROVAL_ID}/resolve")
    assert request["json"] == {"decision": "approved"}
    token = request["headers"]["X-Curie-Approval-Principal"]
    _assert_chat_attestation(token, approval_id=APPROVAL_ID, user="U_MANAGER", channel="C_MGRS")
    assert token not in caplog.text
    rendered = repr(
        (
            web_client.chat_postMessage.call_args_list,
            web_client.chat_update.call_args_list,
            web_client.chat_postEphemeral.call_args_list,
        )
    )
    assert token not in rendered


def test_resolve_request_carries_the_dispatcher_action_parent() -> None:
    """The Slack action trace is forwarded as HTTP metadata, never body authority."""

    captured = _CapturingHttpClient()
    resolver = ApprovalResolveClient(
        api_base_url="https://api.example.test",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=captured,  # type: ignore[arg-type]
    )

    with _resolve_parent():
        outcome = resolver.resolve(
            APPROVAL_ID,
            decision="approved",
            attested_user="U_MANAGER",
            attested_channel="C_MGRS",
        )

    assert outcome.status_code == 200
    assert len(captured.requests) == 1
    request = captured.requests[0]
    assert request["headers"]["traceparent"] == _RESOLVE_TRACEPARENT
    assert "traceparent" not in request["json"]


def test_non_json_403_body_is_not_captured_as_detail() -> None:
    """LOW-1 (security review of #453): a non-JSON 403 body must not be
    captured into ResolveOutcome.detail. FastAPI's own 403s are always JSON,
    but an intermediary (ingress/WAF) in front of the API can return a
    non-JSON body -- an HTML block page that may embed an internal hostname
    or request id. Before this PR the 403 branch showed a hardcoded string,
    so this raw text never reached the clicker; now the immediate render
    path shows ``outcome.detail`` verbatim (#453 AC4/AC5), so a non-JSON
    body reaching resolve() must not become a renderable reason.
    """

    raw_body = "<html>403 Forbidden - waf-node-7.internal</html>"
    fake_client = _FakeHttpClient(_FakeHttpResponse(status_code=403, text=raw_body))
    resolver = ApprovalResolveClient(
        api_base_url="https://api.internal",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=fake_client,  # type: ignore[arg-type]
    )

    outcome = resolver.resolve(
        APPROVAL_ID,
        decision="approved",
        attested_user="U_MANAGER",
        attested_channel="C_MGRS",
    )

    assert outcome.status_code == 403
    assert raw_body not in outcome.detail


_OWNERSHIP_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "approval-ownership.json"
)
_EXPECTED_OWNERSHIP_VECTOR_KEYS = frozenset({"comment", "not_found_detail"})


class _JsonHttpResponse:
    def __init__(self, *, status_code: int, body: dict[str, Any]) -> None:
        self.status_code = status_code
        self._body = body
        self.text = json.dumps(body)

    def json(self) -> dict[str, Any]:
        return self._body


class _JsonHttpClient:
    def __init__(self, response: _JsonHttpResponse) -> None:
        self._response = response
        self.urls: list[str] = []

    def get(self, url: str, *, headers: dict[str, str]) -> _JsonHttpResponse:
        del headers
        self.urls.append(url)
        return self._response

    def post(self, url: str, *, json: dict[str, Any], headers: dict[str, str]) -> _JsonHttpResponse:
        raise AssertionError(f"exists() must not POST ({url})")


def test_ownership_miss_detail_matches_the_frozen_vector() -> None:
    from curie_dispatcher.approval_actions import _APPROVAL_NOT_FOUND_DETAIL

    vector = json.loads(_OWNERSHIP_VECTOR.read_text(encoding="utf-8"))
    assert set(vector) == _EXPECTED_OWNERSHIP_VECTOR_KEYS
    assert vector["not_found_detail"] == _APPROVAL_NOT_FOUND_DETAIL


def test_exists_treats_approval_not_found_as_unowned() -> None:
    vector = json.loads(_OWNERSHIP_VECTOR.read_text(encoding="utf-8"))
    fake = _JsonHttpClient(
        _JsonHttpResponse(status_code=404, body={"detail": vector["not_found_detail"]})
    )
    client = ApprovalResolveClient(
        api_base_url="https://api.example.test",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=fake,  # type: ignore[arg-type]
    )

    assert client.exists(APPROVAL_ID) is False
    assert fake.urls == [f"https://api.example.test/approvals/{APPROVAL_ID}"]


def test_exists_treats_a_present_row_as_owned() -> None:
    fake = _JsonHttpClient(
        _JsonHttpResponse(status_code=200, body={"id": APPROVAL_ID, "status": "pending"})
    )
    client = ApprovalResolveClient(
        api_base_url="https://api.example.test",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=fake,  # type: ignore[arg-type]
    )

    assert client.exists(APPROVAL_ID) is True


def test_exists_treats_a_proxy_404_as_unknown() -> None:
    fake = _JsonHttpClient(_JsonHttpResponse(status_code=404, body={"detail": "Not Found"}))
    client = ApprovalResolveClient(
        api_base_url="https://api.example.test",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=fake,  # type: ignore[arg-type]
    )

    assert client.exists(APPROVAL_ID) is None


def test_exists_treats_a_probe_error_as_unknown() -> None:
    fake = _JsonHttpClient(_JsonHttpResponse(status_code=500, body={"detail": "boom"}))
    client = ApprovalResolveClient(
        api_base_url="https://api.example.test",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=fake,  # type: ignore[arg-type]
    )

    assert client.exists(APPROVAL_ID) is None


class _RaisingHttpClient:
    def get(self, url: str, *, headers: dict[str, str]) -> Any:
        raise httpx.ConnectError("refused")


def test_exists_treats_an_http_error_as_unknown() -> None:
    client = ApprovalResolveClient(
        api_base_url="https://api.example.test",
        api_key=_PLATFORM_API_KEY,
        approval_chat_attester_secret=_CHAT_ATTESTER_SECRET,
        client=_RaisingHttpClient(),  # type: ignore[arg-type]
    )

    assert client.exists(APPROVAL_ID) is None


_ACTION_ID_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "approval-action-ids.json"
)

# Every top-level key the vector may carry. Checked exactly, so an unrecognized
# key is a loud failure rather than an input this lane silently ignores: a key
# one lane cannot see would otherwise pass vacuously, which is the exact drift
# the gate exists to catch. The Rust lane rejects unknown fields the same way,
# via `#[serde(deny_unknown_fields)]` on its ActionIdVector.
_EXPECTED_VECTOR_KEYS = frozenset(
    {
        "comment",
        "approve_action_id_prefix",
    }
)


def test_action_ids_match_the_frozen_vector() -> None:
    """The Python half of the cross-language approval action-id gate (#1079).

    Pins the dispatcher's Approve action ids to the one literal the vector
    freezes, the Approve prefix; no id set is compared, since the prefix is the
    only value duplicated across Python and Rust. The Rust CLI reads the same
    file in its own lane (cli/src/chat.rs's test module), so a rename in one
    language without the other fails that language's test. The rule itself is
    not restated here: it lives in the vector file.
    """

    vector = json.loads(_ACTION_ID_VECTOR.read_text(encoding="utf-8"))

    keys = set(vector)
    assert keys == _EXPECTED_VECTOR_KEYS, (
        f"{_ACTION_ID_VECTOR} has unexpected keys "
        f"{sorted(keys - _EXPECTED_VECTOR_KEYS)} and is missing "
        f"{sorted(_EXPECTED_VECTOR_KEYS - keys)}. A new key is rejected on purpose: "
        "one a lane cannot see would pass vacuously. Teach the new key to "
        "_EXPECTED_VECTOR_KEYS here, to ActionIdVector in cli/src/chat.rs, and to "
        "both lanes' assertions."
    )

    prefix = vector["approve_action_id_prefix"]

    # Deliberately kept, not an orphan of the vector narrowing: the Approve
    # sweep below discovers variants by iterating _DECISION_BY_ACTION_ID, so an
    # id registered in only one of the two production registries would slip past
    # this gate entirely. Pin them to each other so they cannot drift.
    assert set(_DECISION_BY_ACTION_ID) == _APPROVAL_ACTION_IDS, (
        "_DECISION_BY_ACTION_ID and _APPROVAL_ACTION_IDS no longer register the "
        "same action ids "
        f"(only in the map: {sorted(set(_DECISION_BY_ACTION_ID) - _APPROVAL_ACTION_IDS)}; "
        f"only in the set: {sorted(_APPROVAL_ACTION_IDS - set(_DECISION_BY_ACTION_ID))}). "
        "An id missing from either registry is invisible to both "
        "is_approval_action and this gate, so register every id in both."
    )

    # Discovery, not lookup: every id the production map resolves to "approved" is
    # checked, so a third Approve variant added later is covered with no test edit.
    approved = sorted(
        action_id
        for action_id, decision in _DECISION_BY_ACTION_ID.items()
        if decision == "approved"
    )
    # A map emptied or reshaped must not let the loop below pass vacuously. Today
    # the plain id and the note variant (#1053) are both there.
    assert len(approved) >= 2, (
        f"found only {len(approved)} action ids mapped to 'approved' in "
        "_DECISION_BY_ACTION_ID; this gate is no longer pinning anything, so a "
        "card rendered with an unprefixed Approve id would go undetected by "
        "cli/src/chat.rs's body.contains(APPROVE_ACTION_ID_PREFIX) and `curie "
        "local message` would stop reporting an awaiting-approval turn."
    )
    for action_id in approved:
        assert action_id.startswith(prefix), (
            f"the Approve action id {action_id!r} does not start with the frozen "
            f"prefix {prefix!r}; a card rendered with that variant would go "
            "undetected by cli/src/chat.rs's "
            "body.contains(APPROVE_ACTION_ID_PREFIX) and `curie local message` "
            "would stop reporting an awaiting-approval turn."
        )

    # Equality here, startswith above: the asymmetry is the production contract.
    # The base Approve id IS the prefix; the variants only have to extend it.
    assert APPROVE_ACTION_ID == prefix, (
        f"APPROVE_ACTION_ID ({APPROVE_ACTION_ID}) no longer equals the frozen "
        f"prefix ({prefix}) that cli/src/chat.rs matches on; "
        "body.contains(APPROVE_ACTION_ID_PREFIX) would stop detecting a posted "
        "approval card and `curie local message` would stop reporting an "
        "awaiting-approval turn."
    )


# ─── #1074: a note is user text in a bot-authored mrkdwn block ────────────────

INJECTION = "<!channel> <@U_TARGET> *Approved by CFO*"


def test_a_note_cannot_broadcast_or_forge_attribution_on_the_card() -> None:
    """The verdict line renders as ``mrkdwn`` in a context block attributed to
    Curie, so an approver's note was INTERPRETED rather than shown: ``<!channel>``
    pinged the whole room and ``*text*`` forged emphasis in the platform's voice.

    An approver is authorized to decide one gated action. Broadcasting to a
    channel, and writing what looks like a second attribution line, are not part
    of that.
    """
    line = settled_verdict_line(decision="approved", resolver="U_MANAGER", note=INJECTION)

    # The control sequences are shown, not executed.
    assert "<!channel>" not in line, f"a note must not broadcast:\n{line}"
    assert "<@U_TARGET>" not in line, f"a note must not mention:\n{line}"
    assert "&lt;!channel&gt;" in line
    assert "&lt;@U_TARGET&gt;" in line

    # The genuine attribution -- the one the platform wrote -- is untouched, so
    # escaping the note did not escape the line around it.
    assert "<@U_MANAGER>" in line, f"the real resolver mention must survive:\n{line}"


def test_escaping_is_ampersand_first_so_entities_are_not_double_escaped() -> None:
    """`&` has to go first or `<` becomes `&amp;lt;` and the reader sees the
    escaping rather than the text."""
    line = settled_verdict_line(decision="approved", resolver="U1", note="a & b <c>")

    assert "a &amp; b &lt;c&gt;" in line
    assert "&amp;lt;" not in line, f"double-escaped:\n{line}"


def test_cosmetic_markdown_is_left_alone() -> None:
    """`*`, `_` and backticks are not escaped on purpose: they render inertly,
    and stripping them would mangle a note that legitimately quotes code."""
    line = settled_verdict_line(decision="rejected", resolver="U1", note="use `--force` carefully")

    assert "`--force`" in line


def test_a_plain_note_is_unchanged() -> None:
    """The common case pays nothing for the guard."""
    line = settled_verdict_line(decision="rejected", resolver="U1", note="discount exceeds policy")

    assert "Note: discount exceeds policy" in line
