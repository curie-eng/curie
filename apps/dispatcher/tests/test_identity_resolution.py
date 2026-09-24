"""The dispatcher's log-only principal lookup (#2910, ADR 0155 step 5).

For every inbound Slack payload the dispatcher asks the platform API which
principal the Slack user is (``POST /identity/resolve`` located by team id) and
LOGS the answer on the ``curie_dispatcher.identity`` logger. It changes nothing
else: no field on the queued turn, no drop, no delay. The #1053/#1077 rule holds
-- nothing may sit before the ack -- so the lookup is submitted to a background
executor and the listener, its ack and its effects never wait on it.

The API is the external dependency from the dispatcher's side, so it is faked at
the HTTP seam (``httpx.MockTransport``) for the client, and by an injected fake
client for the Bolt-driven tests. Bolt is driven through its real
``SocketModeHandler`` exactly as ``test_inbound_relevance.py`` does.

Slack payload shapes are cited to Slack's reference: Events API envelope
(docs.slack.dev/apis/events-api/#callback-field), block_actions
(docs.slack.dev/reference/interaction-payloads/block-actions-payload) and
view_submission (docs.slack.dev/reference/interaction-payloads/view-interactions-payload).
"""

import json
import logging
import socket
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from curie_dispatcher import identity as identity_module
from curie_dispatcher import run
from curie_dispatcher.app import build_app
from curie_dispatcher.approval_actions import APPROVE_ACTION_ID, ResolveOutcome
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identity import (
    IdentityResolveClient,
    build_identity_client,
    observe_principal,
    principal_observer_middleware,
    slack_subject,
)
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient

from .conftest import FakeSocketClient, OfflineIdentity, _authorize
from .test_approval_note_dialog import (
    _CARD_MESSAGE,
    CARD_CHANNEL,
    CARD_TS,
    ScriptedResolver,
    _note_submit,
)
from .test_dispatch import BOT_TS, _drain, _events_api_request
from .test_preflight import _set_run_env, _TestTelemetry

IDENTITY_LOGGER = "curie_dispatcher.identity"
API_BASE = "https://api.example.test"
API_KEY = "platform-api-test-key"
PRINCIPAL_ID = "5f0c3a1e-0000-4000-8000-000000002910"
INSTALLATION_ID = "00000000-0000-0000-0000-000000000101"
# How long a blocked fake resolver waits for release. A synchronous (pre-ack)
# lookup would stall ``handler.handle`` for this long and then report finished,
# which is exactly what the ack-ordering assertions catch.
BLOCK_SECONDS = 5.0

RESOLVED = {
    "status": "resolved",
    "principal_id": PRINCIPAL_ID,
    "reason": "linked",
    "provider_installation_id": INSTALLATION_ID,
}
UNRESOLVED = {
    "status": "unresolved",
    "principal_id": None,
    "reason": "installation_not_found",
    "provider_installation_id": None,
}


# ---------------------------------------------------------------------------
# Slack payload shapes
# ---------------------------------------------------------------------------


def _event_callback(event: dict[str, Any], *, team_id: str = "T1") -> dict[str, Any]:
    """The Events API outer envelope: ``team_id`` sits at the top level."""
    return {
        "token": "verif",
        "team_id": team_id,
        "api_app_id": "A1",
        "type": "event_callback",
        "event_id": "Ev01",
        "event_time": 1700000000,
        "event": event,
    }


def _app_mention_event(user: str = "U123", text: str = "<@U0BOT> hi") -> dict[str, Any]:
    return {
        "type": "app_mention",
        "user": user,
        "text": text,
        "ts": "1700.0001",
        "channel": "C123",
        "event_ts": "1700.0001",
    }


def _block_actions_body(user: str = "U_MANAGER") -> dict[str, Any]:
    return {
        "type": "block_actions",
        "team": {"id": "T1", "domain": "acme"},
        "user": {"id": user, "username": "manager", "name": "manager", "team_id": "T1"},
        "api_app_id": "A1",
        "token": "verif",
        "trigger_id": "trig-1",
        "container": {"type": "message", "message_ts": "1700.0042"},
        "channel": {"id": "C_MGRS", "name": "mgrs"},
        "actions": [{"type": "button", "action_id": "x", "action_ts": "2.0", "value": "v"}],
    }


def _view_submission_body(user: str = "U_MANAGER") -> dict[str, Any]:
    return {
        "type": "view_submission",
        "team": {"id": "T1", "domain": "acme"},
        "user": {"id": user, "username": "manager", "name": "manager", "team_id": "T1"},
        "api_app_id": "A1",
        "token": "verif",
        "trigger_id": "trig-2",
        "view": {"id": "V1", "type": "modal", "callback_id": "cb", "state": {"values": {}}},
    }


# ---------------------------------------------------------------------------
# slack_subject
# ---------------------------------------------------------------------------


def test_subject_of_an_event_callback() -> None:
    assert slack_subject(_event_callback(_app_mention_event())) == ("T1", "U123")


def test_subject_of_a_direct_message() -> None:
    event = {
        "type": "message",
        "channel_type": "im",
        "channel": "D1",
        "user": "U9",
        "text": "hello",
        "ts": "1800.0001",
    }
    assert slack_subject(_event_callback(event, team_id="T0TEAM")) == ("T0TEAM", "U9")


def test_subject_of_a_block_action() -> None:
    assert slack_subject(_block_actions_body()) == ("T1", "U_MANAGER")


def test_subject_of_a_view_submission() -> None:
    assert slack_subject(_view_submission_body()) == ("T1", "U_MANAGER")


def test_bot_message_without_user_has_no_subject() -> None:
    event = {
        "type": "message",
        "subtype": "bot_message",
        "bot_id": "B0OTHER",
        "text": "alert",
        "ts": "1800.0002",
        "channel": "C123",
    }
    assert slack_subject(_event_callback(event)) is None


def test_missing_team_has_no_subject() -> None:
    body = _event_callback(_app_mention_event())
    del body["team_id"]
    assert slack_subject(body) is None
    # An org-wide install delivers ``team: null`` on interaction payloads.
    org_wide = _block_actions_body()
    org_wide["team"] = None
    assert slack_subject(org_wide) is None
    no_user = _view_submission_body()
    del no_user["user"]
    assert slack_subject(no_user) is None


def test_subject_never_guesses_from_other_fields() -> None:
    """A payload with no event.user is not rescued by a user id found elsewhere."""
    event = {"type": "app_mention", "bot_id": "B1", "text": "<@U123>", "ts": "1", "channel": "C1"}
    body = _event_callback(event) | {"authorizations": [{"user_id": "U0BOT", "team_id": "T1"}]}
    assert slack_subject(body) is None


# ---------------------------------------------------------------------------
# IdentityResolveClient against the HTTP seam
# ---------------------------------------------------------------------------


def _mock_client(
    handler: Callable[[httpx.Request], httpx.Response],
) -> tuple[IdentityResolveClient, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def _record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)

    http = httpx.Client(transport=httpx.MockTransport(_record))
    return IdentityResolveClient(API_BASE, API_KEY, client=http), seen


def test_client_posts_the_slack_locator_with_the_api_key() -> None:
    client, seen = _mock_client(lambda _r: httpx.Response(200, json=RESOLVED))

    assert client.resolve_slack_user("T0TEAM", "U0LINKED") == RESOLVED

    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert str(request.url) == f"{API_BASE}/identity/resolve"
    assert request.headers["X-API-Key"] == API_KEY
    # The tenant is omitted (the route defaults it); nothing beyond the locator
    # and the subject leaves the dispatcher.
    assert json.loads(request.content) == {
        "provider": "slack",
        "external_account_id": "T0TEAM",
        "provider_subject": "U0LINKED",
    }


def test_client_returns_an_unresolved_answer_as_is() -> None:
    client, _seen = _mock_client(lambda _r: httpx.Response(200, json=UNRESOLVED))
    assert client.resolve_slack_user("T0OTHER", "U0LINKED") == UNRESOLVED


@pytest.mark.parametrize("status", [500, 401, 422, 404])
def test_client_returns_none_on_a_non_200(status: int) -> None:
    client, seen = _mock_client(lambda _r: httpx.Response(status, json={"detail": "nope"}))
    assert client.resolve_slack_user("T1", "U1") is None
    assert len(seen) == 1


def test_client_returns_none_on_a_connect_error() -> None:
    def _refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    client, _seen = _mock_client(_refuse)
    assert client.resolve_slack_user("T1", "U1") is None


def test_client_returns_none_on_a_timeout() -> None:
    def _slow(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    client, _seen = _mock_client(_slow)
    assert client.resolve_slack_user("T1", "U1") is None


def test_client_returns_none_when_the_transport_raises_a_non_httpx_error() -> None:
    """The never-raises contract covers any transport failure, not only ``httpx.HTTPError``."""

    def _explode(_request: httpx.Request) -> httpx.Response:
        raise RuntimeError("transport exploded")

    client, seen = _mock_client(_explode)
    assert client.resolve_slack_user("T1", "U1") is None
    assert len(seen) == 1


def test_client_returns_none_through_a_closed_http_client() -> None:
    """A closed ``httpx.Client`` raises ``RuntimeError``, not an ``HTTPError``."""
    http = httpx.Client(transport=httpx.MockTransport(lambda _r: httpx.Response(200, json={})))
    http.close()
    client = IdentityResolveClient(API_BASE, API_KEY, client=http)

    assert client.resolve_slack_user("T1", "U1") is None


def test_client_returns_none_on_a_non_json_200() -> None:
    client, _seen = _mock_client(lambda _r: httpx.Response(200, text="<html>proxy</html>"))
    assert client.resolve_slack_user("T1", "U1") is None


# ---------------------------------------------------------------------------
# observe_principal
# ---------------------------------------------------------------------------


class _Collector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def identity_records() -> Iterator[list[logging.LogRecord]]:
    """Records on the dispatcher's identity logger, from any thread."""
    logger = logging.getLogger(IDENTITY_LOGGER)
    collector = _Collector()
    saved_level = logger.level
    logger.setLevel(logging.DEBUG)
    logger.addHandler(collector)
    try:
        yield collector.records
    finally:
        logger.removeHandler(collector)
        logger.setLevel(saved_level)


def _test_logger() -> tuple[logging.Logger, list[logging.LogRecord]]:
    collector = _Collector()
    logger = logging.getLogger(f"curie_dispatcher.test.identity.{uuid.uuid4().hex}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(collector)
    return logger, collector.records


class _ScriptedIdentity:
    def __init__(self, answer: dict[str, Any] | None = None, exc: BaseException | None = None):
        self.answer = answer
        self.exc = exc
        self.calls: list[tuple[str, str]] = []

    def resolve_slack_user(self, team_id: str, user_id: str) -> dict[str, Any] | None:
        self.calls.append((team_id, user_id))
        if self.exc is not None:
            raise self.exc
        return self.answer


def _resolution_lines(records: list[logging.LogRecord]) -> list[logging.LogRecord]:
    return [r for r in records if "slack principal resolution" in r.getMessage()]


def test_observe_logs_a_resolved_principal() -> None:
    logger, records = _test_logger()
    client = _ScriptedIdentity(RESOLVED)

    observe_principal(_event_callback(_app_mention_event()), client, logger)

    assert client.calls == [("T1", "U123")]
    lines = _resolution_lines(records)
    assert len(lines) == 1
    record = lines[0]
    assert record.levelno == logging.INFO
    message = record.getMessage()
    for field in (
        "status=resolved",
        "reason=linked",
        f"principal_id={PRINCIPAL_ID}",
        "team=T1",
        "user=U123",
    ):
        assert field in message, message


def test_observe_logs_an_unresolved_answer_with_a_dash_for_the_principal() -> None:
    logger, records = _test_logger()

    observe_principal(_block_actions_body(), _ScriptedIdentity(UNRESOLVED), logger)

    lines = _resolution_lines(records)
    assert len(lines) == 1
    message = lines[0].getMessage()
    assert lines[0].levelno == logging.INFO
    for field in (
        "status=unresolved",
        "reason=installation_not_found",
        "principal_id=-",
        "team=T1",
        "user=U_MANAGER",
    ):
        assert field in message, message


def test_observe_warns_when_the_lookup_failed() -> None:
    logger, records = _test_logger()

    observe_principal(_event_callback(_app_mention_event()), _ScriptedIdentity(None), logger)

    assert [r.levelno for r in records] == [logging.WARNING]
    assert "status=resolved" not in records[0].getMessage()


def test_observe_never_raises_when_the_client_raises() -> None:
    logger, records = _test_logger()
    client = _ScriptedIdentity(exc=RuntimeError("boom"))

    observe_principal(_event_callback(_app_mention_event()), client, logger)

    assert client.calls == [("T1", "U123")]
    assert [r.levelno for r in records] == [logging.WARNING]


def test_observe_skips_a_payload_without_a_subject() -> None:
    logger, _records = _test_logger()
    client = _ScriptedIdentity(RESOLVED)
    event = {"type": "message", "bot_id": "B0", "text": "x", "ts": "1", "channel": "C1"}

    observe_principal(_event_callback(event), client, logger)

    assert client.calls == []


def test_observe_never_logs_message_text_or_the_api_key() -> None:
    logger, records = _test_logger()
    body = _event_callback(_app_mention_event(text="SECRET-MESSAGE-TEXT xoxb-CANARY"))

    observe_principal(body, _ScriptedIdentity(RESOLVED), logger)
    observe_principal(body, _ScriptedIdentity(None), logger)
    observe_principal(body, _ScriptedIdentity(exc=RuntimeError("boom")), logger)

    assert records
    for record in records:
        message = record.getMessage()
        assert "SECRET-MESSAGE-TEXT" not in message
        assert "xoxb-CANARY" not in message
        assert API_KEY not in message


# ---------------------------------------------------------------------------
# Through Bolt: the lookup never sits before the ack
# ---------------------------------------------------------------------------


class _BlockingIdentity:
    """A resolver that holds until the test releases it.

    ``entered`` proves the lookup was actually submitted; ``finished`` stays
    clear while it is held, so "ack sent and turn enqueued while not finished"
    is a structural fact, not a timing hope.
    """

    def __init__(self, answer: dict[str, Any] | None = None) -> None:
        self.answer = answer if answer is not None else UNRESOLVED
        self.gate = threading.Event()
        self.entered = threading.Event()
        self.finished = threading.Event()
        self.calls: list[tuple[str, str]] = []

    def resolve_slack_user(self, team_id: str, user_id: str) -> dict[str, Any] | None:
        self.calls.append((team_id, user_id))
        self.entered.set()
        self.gate.wait(BLOCK_SECONDS)
        self.finished.set()
        return self.answer


class _RaisingIdentity:
    def __init__(self) -> None:
        self.called = threading.Event()

    def resolve_slack_user(self, team_id: str, user_id: str) -> dict[str, Any] | None:
        self.called.set()
        raise RuntimeError(f"identity api exploded for {team_id}/{user_id}")


class _NoneIdentity:
    def __init__(self) -> None:
        self.called = threading.Event()

    def resolve_slack_user(self, team_id: str, user_id: str) -> dict[str, Any] | None:
        del team_id, user_id
        self.called.set()
        return None


def _web_client() -> WebClient:
    web_client = WebClient(token="xoxb-test")
    web_client.chat_postMessage = MagicMock(return_value={"ts": BOT_TS})  # type: ignore[method-assign]
    web_client.chat_update = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.chat_postEphemeral = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.conversations_replies = MagicMock(  # type: ignore[method-assign]
        return_value={"messages": [_CARD_MESSAGE]}
    )
    web_client.views_open = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    return web_client


def _build(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    identity_client: Any,
    *,
    resolver: ScriptedResolver | None = None,
) -> tuple[App, WebClient, list[BaseException]]:
    web_client = _web_client()
    app = build_app(
        config,
        web_client=web_client,
        redis_client=redis_client,
        authorize=_authorize,
        resolver=resolver
        or ScriptedResolver(
            ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
        ),
        identity_client=identity_client,
    )
    errors: list[BaseException] = []

    @app.error
    def _collect_error(error: Exception) -> None:
        errors.append(error)

    return app, web_client, errors


def _wait_for(predicate: Callable[[], bool], timeout: float = BLOCK_SECONDS) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def _approval_click(envelope_id: str) -> SocketModeRequest:
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload={
            "type": "block_actions",
            "trigger_id": f"trig-{envelope_id}",
            "team": {"id": "T1"},
            "user": {"id": "U_MANAGER"},
            "api_app_id": "A1",
            "token": "verif",
            "container": {"type": "message", "message_ts": CARD_TS},
            "channel": {"id": CARD_CHANNEL},
            "message": _CARD_MESSAGE,
            "actions": [
                {
                    "type": "button",
                    "action_id": APPROVE_ACTION_ID,
                    "action_ts": "2.0",
                    "value": "9a1e8a10-0000-0000-0000-000000002910",
                }
            ],
        },
    )


def test_app_mention_is_acked_and_enqueued_before_the_lookup_returns(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    identity_records: list[logging.LogRecord],
) -> None:
    identity = _BlockingIdentity()
    app, _web, errors = _build(config, redis_client, identity)
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(
            sock, _events_api_request("env-id-1", "Ev-id-1", _app_mention_event())
        )
        _drain(app)

        assert sock.acked_envelope_ids == ["env-id-1"]
        assert len(redis_client.xrange(config.stream)) == 1
        # The lookup was submitted and is still held: the ack and the enqueue
        # did not wait for it.
        assert identity.entered.wait(BLOCK_SECONDS)
        assert not identity.finished.is_set()
    finally:
        identity.gate.set()

    assert identity.finished.wait(BLOCK_SECONDS)
    assert _wait_for(lambda: bool(_resolution_lines(identity_records)))
    assert identity.calls == [("T1", "U123")]
    message = _resolution_lines(identity_records)[0].getMessage()
    assert "status=unresolved" in message and "reason=installation_not_found" in message
    assert "team=T1" in message and "user=U123" in message
    assert errors == []


def test_approval_click_is_acked_and_resolved_before_the_lookup_returns(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    identity_records: list[logging.LogRecord],
) -> None:
    identity = _BlockingIdentity(RESOLVED)
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client, errors = _build(config, redis_client, identity, resolver=resolver)
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(sock, _approval_click("env-id-2"))
        _drain(app)

        assert sock.acked_envelope_ids == ["env-id-2"]
        assert len(resolver.calls) == 1
        assert resolver.calls[0]["attested_user"] == "U_MANAGER"
        web_client.chat_update.assert_called_once()
        # An approval click is not a turn.
        assert redis_client.xrange(config.stream) == []
        assert identity.entered.wait(BLOCK_SECONDS)
        assert not identity.finished.is_set()
    finally:
        identity.gate.set()

    assert identity.finished.wait(BLOCK_SECONDS)
    assert _wait_for(lambda: bool(_resolution_lines(identity_records)))
    assert identity.calls == [("T1", "U_MANAGER")]
    message = _resolution_lines(identity_records)[0].getMessage()
    assert "status=resolved" in message and f"principal_id={PRINCIPAL_ID}" in message
    assert errors == []


def test_note_submission_is_acked_and_resolved_before_the_lookup_returns(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    identity_records: list[logging.LogRecord],
) -> None:
    identity = _BlockingIdentity()
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
    )
    app, web_client, errors = _build(config, redis_client, identity, resolver=resolver)
    sock = FakeSocketClient()
    try:
        SocketModeHandler(app, app_token="xapp-test").handle(
            sock, _note_submit("env-id-3", note="approved for Q3")
        )
        _drain(app)

        assert sock.acked_envelope_ids == ["env-id-3"]
        # A successful submit closes the modal: the ack carries no errors body.
        assert sock.ack_payload_for("env-id-3") is None
        assert len(resolver.calls) == 1
        assert resolver.calls[0]["note"] == "approved for Q3"
        web_client.chat_update.assert_called_once()
        assert identity.entered.wait(BLOCK_SECONDS)
        assert not identity.finished.is_set()
    finally:
        identity.gate.set()

    assert identity.finished.wait(BLOCK_SECONDS)
    assert _wait_for(lambda: bool(_resolution_lines(identity_records)))
    assert identity.calls == [("T1", "U_MANAGER")]
    assert errors == []


@pytest.mark.parametrize("identity_factory", [_RaisingIdentity, _NoneIdentity])
def test_a_failed_lookup_leaves_the_enqueue_unchanged(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    identity_records: list[logging.LogRecord],
    identity_factory: Callable[[], Any],
) -> None:
    identity = identity_factory()
    app, _web, errors = _build(config, redis_client, identity)
    sock = FakeSocketClient()

    SocketModeHandler(app, app_token="xapp-test").handle(
        sock, _events_api_request("env-id-4", "Ev-id-4", _app_mention_event(text="please answer"))
    )
    _drain(app)

    assert identity.called.wait(BLOCK_SECONDS)
    assert _wait_for(lambda: any(r.levelno == logging.WARNING for r in identity_records))
    assert sock.acked_envelope_ids == ["env-id-4"]
    entries = redis_client.xrange(config.stream)
    assert len(entries) == 1
    assert errors == []


def test_a_bot_authored_mention_is_not_looked_up(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    identity_records: list[logging.LogRecord],
) -> None:
    identity = _ScriptedIdentity(RESOLVED)
    app, _web, errors = _build(config, redis_client, identity)
    sock = FakeSocketClient()
    event = {"type": "app_mention", "bot_id": "B0OTHER", "text": "hi", "ts": "1", "channel": "C1"}

    SocketModeHandler(app, app_token="xapp-test").handle(
        sock, _events_api_request("env-id-5", "Ev-id-5", event)
    )
    _drain(app)
    # Give a (wrongly) submitted lookup the chance to run before asserting none did.
    time.sleep(0.2)

    assert sock.acked_envelope_ids == ["env-id-5"]
    assert identity.calls == []
    assert errors == []


# ---------------------------------------------------------------------------
# No test reaches a real API: the offline default (finding A)
# ---------------------------------------------------------------------------


def test_an_app_built_without_an_identity_client_makes_no_network_call(
    redis_client: redis.Redis,
    config: DispatcherConfig,
    offline_identity: list[OfflineIdentity],
) -> None:
    """The shared harness default: ``build_app`` with no ``identity_client``
    must not POST to the configured API. The API is a real listening loopback
    socket, so a lookup that escaped the conftest stub would show up as an
    accepted connection."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    listener.settimeout(0.5)
    try:
        api_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        cfg = config.model_copy(update={"api_base_url": api_url})
        app = build_app(
            cfg,
            web_client=_web_client(),
            redis_client=redis_client,
            authorize=_authorize,
            resolver=ScriptedResolver(
                ResolveOutcome(status_code=200, resolved_by="U_MANAGER", decision="approved")
            ),
        )
        sock = FakeSocketClient()
        SocketModeHandler(app, app_token="xapp-test").handle(
            sock, _events_api_request("env-id-guard", "Ev-id-guard", _app_mention_event())
        )
        _drain(app)
        assert sock.acked_envelope_ids == ["env-id-guard"]

        accepted = 0
        try:
            conn, _addr = listener.accept()
            conn.close()
            accepted += 1
        except TimeoutError:
            pass
        assert accepted == 0, "a dispatcher test sent a real identity lookup to the API"

        # The middleware got the offline stub, and the lookup ran against it.
        assert len(offline_identity) == 1
        assert _wait_for(lambda: offline_identity[0].calls == [("T1", "U123")])
    finally:
        listener.close()


# ---------------------------------------------------------------------------
# Lifecycle: one shared lookup pool, shut down with the dispatcher (finding B)
# ---------------------------------------------------------------------------

LOOKUP_THREAD_PREFIX = "identity-lookup"


def _lookup_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name.startswith(LOOKUP_THREAD_PREFIX)]


def _payload(user: str) -> dict[str, Any]:
    return _event_callback(_app_mention_event(user=user))


class _GatedIdentity:
    """Blocks the first ``blocked`` lookups on a gate; records every call."""

    def __init__(self, blocked: int) -> None:
        self.blocked = blocked
        self.gate = threading.Event()
        self.entered = threading.Semaphore(0)
        self.calls: list[tuple[str, str]] = []
        self._lock = threading.Lock()

    def resolve_slack_user(self, team_id: str, user_id: str) -> dict[str, Any] | None:
        with self._lock:
            self.calls.append((team_id, user_id))
            position = len(self.calls)
        if position <= self.blocked:
            self.entered.release()
            self.gate.wait(BLOCK_SECONDS)
        return UNRESOLVED


def test_apps_share_one_lookup_pool_rather_than_one_each(
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """Building an app must not add lookup threads: three apps, one payload
    each, still leave at most the one shared pool's workers alive."""
    for index in range(3):
        identity = _ScriptedIdentity(RESOLVED)
        app, _web, errors = _build(config, redis_client, identity)
        SocketModeHandler(app, app_token="xapp-test").handle(
            FakeSocketClient(),
            _events_api_request(f"env-pool-{index}", f"Ev-pool-{index}", _app_mention_event()),
        )
        _drain(app)
        assert _wait_for(lambda identity=identity: identity.calls == [("T1", "U123")])
        assert errors == []

    # Idle pool workers never exit, so a pool per app stays visible here.
    assert _wait_for(lambda: len(_lookup_threads()) <= 2, timeout=1.0), [
        t.name for t in _lookup_threads()
    ]


def test_shutdown_cancels_queued_lookups_and_closes_built_clients(
    config: DispatcherConfig,
) -> None:
    built = build_identity_client(config)
    workers = identity_module._MAX_WORKERS
    identity = _GatedIdentity(blocked=workers)
    middleware = principal_observer_middleware(identity)
    try:
        for index in range(workers):
            middleware(_payload(f"U_BLOCK{index}"), lambda: None)
        for _ in range(workers):
            assert identity.entered.acquire(timeout=BLOCK_SECONDS)
        # Every worker is held, so this one is queued behind them.
        middleware(_payload("U_QUEUED"), lambda: None)

        identity_module.shutdown_identity_lookups()
    finally:
        identity.gate.set()

    time.sleep(0.3)
    assert ("T1", "U_QUEUED") not in identity.calls
    assert len(identity.calls) == workers
    assert built._client.is_closed


def test_a_lookup_after_shutdown_still_runs_on_a_fresh_pool(
    identity_records: list[logging.LogRecord],
) -> None:
    identity_module.shutdown_identity_lookups()
    identity = _ScriptedIdentity(RESOLVED)
    middleware = principal_observer_middleware(identity)
    reached: list[bool] = []

    middleware(_payload("U_AFTER"), lambda: reached.append(True))

    assert reached == [True]
    assert _wait_for(lambda: identity.calls == [("T1", "U_AFTER")])
    assert _wait_for(lambda: bool(_resolution_lines(identity_records)))


def test_run_main_shuts_identity_lookups_down_after_the_supervisor_stops(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Driven through the real ``run.main()`` with the same boundaries replaced
    as ``test_preflight``'s boot-ordering test."""
    events: list[str] = []

    class RecordingSupervisor:
        def run(self) -> None:
            events.append("supervisor.run")

        def request_stop(self) -> None:
            events.append("supervisor.request_stop")

    class RecordingHeartbeat:
        def set(self) -> None:
            events.append("heartbeat.stop")

    def _shutdown() -> None:
        events.append("identity.shutdown")

    _set_run_env(monkeypatch)
    monkeypatch.setattr(run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry())
    monkeypatch.setattr(run, "check_api_reachable", lambda *a, **k: None)
    monkeypatch.setattr(run, "check_slack_channel_capabilities", lambda *a, **k: None)
    monkeypatch.setattr(run, "build_supervisor", lambda *a, **k: RecordingSupervisor())
    monkeypatch.setattr(run, "start_heartbeat", lambda *a, **k: RecordingHeartbeat())
    monkeypatch.setattr(run.signal, "signal", lambda *a, **k: None)
    # Whichever way run.py reaches it: a module attribute or a from-import.
    monkeypatch.setattr(identity_module, "shutdown_identity_lookups", _shutdown, raising=False)
    monkeypatch.setattr(run, "shutdown_identity_lookups", _shutdown, raising=False)

    run.main()

    assert "identity.shutdown" in events
    assert events.index("identity.shutdown") > events.index("supervisor.run")
