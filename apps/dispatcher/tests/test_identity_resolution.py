"""Log-only Slack principal resolution in the dispatcher (#2910, ADR 0198, ADR 0201).

The dispatcher pulls the sender evidence ADR 0201 maps out of each delivery,
asks the platform API's ``POST /identity/resolve`` who that is, and logs the
answer. Nothing is enforced yet (#2914), so the one behavioral rule is the
#1053/#1077 ack rule: the lookup is submitted to a background pool and the
envelope is acked and the turn enqueued without waiting on it.

The envelopes are Bolt-shaped and built from the anonymised capture
placeholders (``apps/api/tests/fixtures/slack_identity_capture.json``): team
``THOME0001``, user ``UALICE001``, bot user ``UBOT00001``. No real ids.

Faked: the socket, the Web API client, and the platform API (a real loopback
HTTP server, or an injected lookup client). Valkey is real, as everywhere.
"""

from __future__ import annotations

import json
import logging
import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import redis
from curie_dispatcher import identity as identity_module
from curie_dispatcher.admission import build_admission
from curie_dispatcher.app import build_app
from curie_dispatcher.approval_actions import (
    APPROVE_ACTION_ID,
    NOTE_MODAL_CALLBACK_ID,
    ResolveOutcome,
)
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials
from curie_dispatcher.identity import (
    IdentityResolveClient,
    observe_principal,
    shutdown_identity_lookups,
    slack_evidence,
)
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from slack_sdk.socket_mode.request import SocketModeRequest
from slack_sdk.web import WebClient

from .conftest import FakeSocketClient, IdentityClientBuilds, ScriptedResolver, _authorize

TEAM = "THOME0001"
USER = "UALICE001"
BOT_USER = "UBOT00001"
API_KEY = "platform-api-test-key"

APPROVAL_ID = "9a1e8a10-0000-0000-0000-000000002910"
CARD_TS = "1700.0042"
CARD_CHANNEL = "C_MGRS"
_CARD_MESSAGE: dict[str, Any] = {
    "ts": CARD_TS,
    "blocks": [
        {"type": "header", "text": {"type": "plain_text", "text": "Approval required"}},
        {"type": "actions", "elements": []},
    ],
}

_EVIDENCE_FIELDS = {
    "delivery": None,
    "user_id": None,
    "event_user_team": None,
    "event_team": None,
    "interaction_user_team_id": None,
    "is_ext_shared_channel": None,
    "enterprise_ids": [],
}


@pytest.fixture(autouse=True)
def _release_lookup_pool() -> Iterator[None]:
    """Every test leaves the shared lookup pool shut down, so none inherits
    another's queued or blocked lookups. Gates are released in each test's own
    ``finally`` before this runs."""

    yield
    shutdown_identity_lookups()


# --- envelope builders -------------------------------------------------------


def _home_authorizations() -> list[dict[str, Any]]:
    # The capture's shape: a non-Grid install records a null enterprise.
    return [
        {
            "team_id": TEAM,
            "enterprise_id": None,
            "is_enterprise_install": False,
            "is_bot": True,
            "user_id": BOT_USER,
        }
    ]


def _event_body(event: dict[str, Any], **envelope: Any) -> dict[str, Any]:
    """The Events API body Bolt hands middleware: the ``event_callback`` payload."""

    body: dict[str, Any] = {
        "type": "event_callback",
        "team_id": TEAM,
        "event_id": "Ev2910",
        "event_time": 1700000000,
        "is_ext_shared_channel": False,
        "authorizations": _home_authorizations(),
        "event": event,
    }
    body.update(envelope)
    return body


def _mention(**overrides: Any) -> dict[str, Any]:
    event: dict[str, Any] = {
        "type": "app_mention",
        "user": USER,
        "team": TEAM,
        "channel": "C2910",
        "text": f"<@{BOT_USER}> hello",
        "ts": "1700.0001",
    }
    event.update(overrides)
    return event


def _interaction_body(kind: str, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "type": kind,
        "user": {"id": USER, "team_id": TEAM},
        "team": {"id": TEAM},
        "enterprise": None,
        "is_enterprise_install": False,
        "api_app_id": "A2910",
        "token": "verif",
        "trigger_id": "trig-2910",
    }
    body.update(overrides)
    return body


def _norm(evidence: dict[str, Any]) -> dict[str, Any]:
    """The evidence with every SlackEvidence field filled with its default.

    The API model forbids extra keys, so a key outside the seven fields is a
    422 there; an omitted optional key is the same as its default.
    """

    assert set(evidence) <= set(_EVIDENCE_FIELDS), sorted(set(evidence) - set(_EVIDENCE_FIELDS))
    filled = {key: evidence.get(key, default) for key, default in _EVIDENCE_FIELDS.items()}
    filled["enterprise_ids"] = sorted(filled["enterprise_ids"] or [])
    return filled


# --- slack_evidence ------------------------------------------------------------


def test_an_app_mention_carries_its_event_team_and_the_non_shared_flag() -> None:
    evidence = slack_evidence(_event_body(_mention()))

    assert evidence is not None
    assert _norm(evidence) == {
        "delivery": "app_mention",
        "user_id": USER,
        "event_user_team": None,
        "event_team": TEAM,
        "interaction_user_team_id": None,
        # False exactly, not None: the API's mention path needs it to be False.
        "is_ext_shared_channel": False,
        # The null enterprise in authorizations is not an enterprise id.
        "enterprise_ids": [],
    }
    assert evidence["is_ext_shared_channel"] is False


@pytest.mark.parametrize("kind", ["block_actions", "view_submission"])
def test_an_interaction_carries_the_user_team_id(kind: str) -> None:
    body = _interaction_body(kind)
    if kind == "view_submission":
        body["view"] = {"id": "V1", "type": "modal", "callback_id": NOTE_MODAL_CALLBACK_ID}
    evidence = slack_evidence(body)

    assert evidence is not None
    normed = _norm(evidence)
    assert normed["delivery"] == kind
    assert normed["user_id"] == USER
    assert normed["interaction_user_team_id"] == TEAM
    assert normed["event_user_team"] is None
    assert normed["is_ext_shared_channel"] is None
    # `enterprise: null` is the non-Grid answer, not an id.
    assert normed["enterprise_ids"] == []


def test_a_direct_message_is_a_message_delivery() -> None:
    event = {
        "type": "message",
        "channel_type": "im",
        "user": USER,
        "team": TEAM,
        "channel": "D2910",
        "text": "hello",
        "ts": "1700.0002",
    }
    evidence = slack_evidence(_event_body(event))

    assert evidence is not None
    normed = _norm(evidence)
    assert normed["delivery"] == "message"
    assert normed["user_id"] == USER
    assert normed["event_team"] == TEAM
    assert normed["event_user_team"] is None
    assert normed["is_ext_shared_channel"] is False


def test_a_shared_channel_event_carries_the_senders_user_team() -> None:
    event = _mention(user_team="TPARTNER1", source_team="TPARTNER1")
    evidence = slack_evidence(_event_body(event, is_ext_shared_channel=True))

    assert evidence is not None
    assert _norm(evidence) == {
        "delivery": "app_mention",
        "user_id": USER,
        "event_user_team": "TPARTNER1",
        "event_team": TEAM,
        "interaction_user_team_id": None,
        "is_ext_shared_channel": True,
        "enterprise_ids": [],
    }
    # source_team is not one of the mapped fields (ADR 0201).
    assert "TPARTNER1" not in [v for k, v in evidence.items() if k != "event_user_team"]


def test_every_enterprise_id_in_an_event_envelope_is_collected() -> None:
    authorizations = _home_authorizations()
    authorizations[0]["enterprise_id"] = "E0AUTH001"
    body = _event_body(_mention(), enterprise_id="E0ENV0001", authorizations=authorizations)

    evidence = slack_evidence(body)

    assert evidence is not None
    assert _norm(evidence)["enterprise_ids"] == ["E0AUTH001", "E0ENV0001"]


def test_every_enterprise_id_in_an_interaction_is_collected() -> None:
    body = _interaction_body(
        "block_actions",
        enterprise={"id": "E0ENT0001", "name": "redacted"},
        team={"id": TEAM, "enterprise_id": "E0TEAM001"},
        user={"id": USER, "team_id": TEAM, "enterprise_id": "E0USER001"},
    )

    evidence = slack_evidence(body)

    assert evidence is not None
    assert _norm(evidence)["enterprise_ids"] == ["E0ENT0001", "E0TEAM001", "E0USER001"]


def _enterprise_install_event() -> dict[str, Any]:
    authorizations = _home_authorizations()
    authorizations[0]["is_enterprise_install"] = True
    return _event_body(_mention(), is_enterprise_install=True, authorizations=authorizations)


@pytest.mark.parametrize(
    "body",
    [
        _enterprise_install_event(),
        _interaction_body("block_actions", is_enterprise_install=True),
        _interaction_body("view_submission", is_enterprise_install=True),
    ],
    ids=["event", "block_actions", "view_submission"],
)
def test_an_enterprise_install_is_an_enterprise_signal(body: dict[str, Any]) -> None:
    """L3 (rev 4.3): ``is_enterprise_install: true`` puts a signal into
    enterprise_ids, so the API refuses even with a documented user.team_id."""

    evidence = slack_evidence(body)

    assert evidence is not None
    normed = _norm(evidence)
    assert normed["enterprise_ids"], normed
    assert all(isinstance(e, str) and e and len(e) <= 256 for e in normed["enterprise_ids"])
    if normed["delivery"] != "app_mention":
        # The documented team is still copied; the signal is what refuses.
        assert normed["interaction_user_team_id"] == TEAM


@pytest.mark.parametrize(
    "body",
    [
        _event_body(_mention(), context_enterprise_id="E0CTX0001", context_team_id=TEAM),
        _interaction_body("block_actions", context_enterprise_id="E0CTX0001"),
    ],
    ids=["event", "interaction"],
)
def test_a_context_enterprise_id_is_an_enterprise_signal(body: dict[str, Any]) -> None:
    evidence = slack_evidence(body)

    assert evidence is not None
    assert "E0CTX0001" in _norm(evidence)["enterprise_ids"]


@pytest.mark.parametrize(
    "body",
    [
        _event_body(_mention(), context_enterprise_id=None, context_team_id=TEAM),
        _interaction_body("block_actions", is_enterprise_install=False),
    ],
    ids=["event-null-context-enterprise", "interaction-not-enterprise-install"],
)
def test_a_non_grid_install_carries_no_enterprise_signal(body: dict[str, Any]) -> None:
    evidence = slack_evidence(body)

    assert evidence is not None
    assert _norm(evidence)["enterprise_ids"] == []


@pytest.mark.parametrize(
    "body",
    [
        _event_body({k: v for k, v in _mention().items() if k != "user"}),
        {k: v for k, v in _interaction_body("block_actions").items() if k != "user"},
        _interaction_body("view_submission", user={"team_id": TEAM}),
    ],
    ids=["event-without-user", "interaction-without-user", "interaction-user-without-id"],
)
def test_no_user_means_no_evidence(body: dict[str, Any]) -> None:
    assert slack_evidence(body) is None


# --- IdentityResolveClient ----------------------------------------------------


@dataclass
class _RecordingApi:
    url: str = ""
    status: int = 200
    payload: bytes = b""
    paths: list[str] = field(default_factory=list)
    bodies: list[Any] = field(default_factory=list)
    keys: list[str | None] = field(default_factory=list)


@contextmanager
def _recording_api() -> Iterator[_RecordingApi]:
    state = _RecordingApi()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length)
            state.paths.append(self.path)
            state.bodies.append(json.loads(raw or b"null"))
            state.keys.append(self.headers.get("X-API-Key"))
            self.send_response(state.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(state.payload)))
            self.end_headers()
            self.wfile.write(state.payload)

        def log_message(self, _format: str, *args: object) -> None:
            """Quiet."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address[:2]
    state.url = f"http://{host!s}:{port}"
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


_RESOLVED = {
    "status": "resolved",
    "reason": "linked",
    "principal_id": "0b0e0000-0000-0000-0000-000000002910",
    "channel_identity_id": "0c1d0000-0000-0000-0000-000000002910",
    "namespace_id": "0a5e0000-0000-0000-0000-000000002910",
}


def _evidence() -> dict[str, Any]:
    evidence = slack_evidence(_event_body(_mention()))
    assert evidence is not None
    return evidence


def test_the_client_posts_the_exact_body_with_the_platform_key() -> None:
    with _recording_api() as api:
        api.payload = json.dumps(_RESOLVED).encode()
        client = IdentityResolveClient(api_base_url=api.url, api_key=API_KEY)

        answer = client.resolve("ops-bot", _evidence())

    assert answer == _RESOLVED
    assert api.paths == ["/identity/resolve"]
    assert api.bodies == [
        {"provider": "slack", "channel_identity": "ops-bot", "slack": _evidence()}
    ]
    assert api.keys == [API_KEY]


def test_a_500_is_no_answer() -> None:
    with _recording_api() as api:
        api.status = 500
        api.payload = b'{"detail":"Internal Server Error"}'
        client = IdentityResolveClient(api_base_url=api.url, api_key=API_KEY)

        assert client.resolve("default", _evidence()) is None


def test_a_non_json_answer_is_no_answer() -> None:
    with _recording_api() as api:
        api.payload = b"<html>proxy error</html>"
        client = IdentityResolveClient(api_base_url=api.url, api_key=API_KEY)

        assert client.resolve("default", _evidence()) is None


def test_a_refused_connection_is_no_answer() -> None:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()  # nothing listens: the connect is refused
    client = IdentityResolveClient(api_base_url=f"http://127.0.0.1:{port}", api_key=API_KEY)

    assert client.resolve("default", _evidence()) is None


def test_any_exception_from_the_transport_is_no_answer() -> None:
    class Exploding:
        def post(self, *_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("not an httpx error")

    client = IdentityResolveClient(
        api_base_url="http://api.example.invalid",
        api_key=API_KEY,
        client=Exploding(),  # type: ignore[arg-type]
    )

    assert client.resolve("default", _evidence()) is None


# --- observe_principal --------------------------------------------------------


class _ScriptedLookup:
    def __init__(self, answer: dict[str, Any] | None = None, exc: Exception | None = None) -> None:
        self.answer = answer
        self.exc = exc
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def resolve(self, identity_name: str, evidence: dict[str, Any]) -> dict[str, Any] | None:
        self.calls.append((identity_name, evidence))
        if self.exc is not None:
            raise self.exc
        return self.answer


def test_a_resolution_is_logged_at_info_in_the_stated_format(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = logging.getLogger("test-identity-observe")
    lookup = _ScriptedLookup(_RESOLVED)

    with caplog.at_level(logging.INFO, logger=log.name):
        observe_principal(_event_body(_mention()), lookup, log, identity_name="ops-bot")

    assert lookup.calls == [("ops-bot", _evidence())]
    infos = [r.getMessage() for r in caplog.records if r.levelno == logging.INFO]
    assert infos == [
        "slack principal resolution status=resolved reason=linked "
        f"principal_id={_RESOLVED['principal_id']} identity=ops-bot "
        f"delivery=app_mention user={USER}"
    ]


def test_an_unresolved_answer_logs_a_dash_for_the_principal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    log = logging.getLogger("test-identity-observe-unresolved")
    answer = {**_RESOLVED, "status": "unresolved", "reason": "no_link", "principal_id": None}

    with caplog.at_level(logging.INFO, logger=log.name):
        observe_principal(
            _interaction_body("block_actions"),
            _ScriptedLookup(answer),
            log,
            identity_name="default",
        )

    assert [r.getMessage() for r in caplog.records] == [
        "slack principal resolution status=unresolved reason=no_link principal_id=- "
        f"identity=default delivery=block_actions user={USER}"
    ]
    assert caplog.records[0].levelno == logging.INFO


@pytest.mark.parametrize(
    "lookup",
    [_ScriptedLookup(None), _ScriptedLookup(exc=RuntimeError("boom"))],
    ids=["no-answer", "raises"],
)
def test_a_failed_lookup_is_one_warning_and_never_raises(
    lookup: _ScriptedLookup, caplog: pytest.LogCaptureFixture
) -> None:
    log = logging.getLogger("test-identity-observe-failed")

    with caplog.at_level(logging.INFO, logger=log.name):
        observe_principal(_event_body(_mention()), lookup, log, identity_name="default")

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert warnings[0].getMessage().startswith("slack principal lookup failed")
    assert not [r for r in caplog.records if r.levelno == logging.INFO]


class _Clock:
    def __init__(self, now: float) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


_clock_epoch = [1_000_000.0]


@pytest.fixture(autouse=True)
def _lookup_clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    """Each test starts an hour after the last on a fake monotonic clock, so a
    rate-limit window from one test never suppresses another's warning."""

    _clock_epoch[0] += 3600.0
    clock = _Clock(_clock_epoch[0])
    monkeypatch.setattr(identity_module, "_monotonic", clock, raising=False)
    return clock


def test_lookup_failure_warnings_are_rate_limited_with_a_suppressed_count(
    _lookup_clock: _Clock, caplog: pytest.LogCaptureFixture
) -> None:
    """L6 (rev 4.3): an API that always fails logs at most one lookup-failure
    WARNING per 60 s; the next one reports how many were suppressed."""

    log = logging.getLogger("test-identity-observe-ratelimit")
    failing = _ScriptedLookup(None)

    with caplog.at_level(logging.INFO, logger=log.name):
        for i in range(10):
            _lookup_clock.now += 5.0 if i else 0.0  # 45 s in all, inside one window
            observe_principal(_event_body(_mention()), failing, log, identity_name="default")
        first_window = [r for r in caplog.records if r.levelno == logging.WARNING]

        _lookup_clock.now += 61.0
        observe_principal(_event_body(_mention()), failing, log, identity_name="default")

    assert len(failing.calls) == 11, "a suppressed warning must not skip the lookup"
    assert len(first_window) == 1
    assert first_window[0].getMessage().startswith("slack principal lookup failed")
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 2
    assert "suppressed" in warnings[1]
    assert "9" in warnings[1]


def test_a_raising_lookup_shares_the_rate_limit(
    _lookup_clock: _Clock, caplog: pytest.LogCaptureFixture
) -> None:
    log = logging.getLogger("test-identity-observe-ratelimit-raise")
    raising = _ScriptedLookup(exc=RuntimeError("boom"))

    with caplog.at_level(logging.INFO, logger=log.name):
        for _ in range(5):
            _lookup_clock.now += 1.0
            observe_principal(_event_body(_mention()), raising, log, identity_name="default")

    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1


def test_success_info_lines_are_not_rate_limited(
    _lookup_clock: _Clock, caplog: pytest.LogCaptureFixture
) -> None:
    log = logging.getLogger("test-identity-observe-ratelimit-info")
    failing = _ScriptedLookup(None)
    ok = _ScriptedLookup(_RESOLVED)

    with caplog.at_level(logging.INFO, logger=log.name):
        for _ in range(5):
            _lookup_clock.now += 1.0
            observe_principal(_event_body(_mention()), failing, log, identity_name="default")
            observe_principal(_event_body(_mention()), ok, log, identity_name="default")

    infos = [r for r in caplog.records if r.levelno == logging.INFO]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(infos) == 5
    assert all(r.getMessage().startswith("slack principal resolution") for r in infos)
    assert len(warnings) == 1


# --- through the real SocketModeHandler -------------------------------------------


class _BlockedLookup:
    """A lookup client whose every call blocks until the test opens ``gate``."""

    def __init__(self) -> None:
        self.gate = threading.Event()
        self.entered = threading.Event()
        self.returned = threading.Event()
        self._lock = threading.Lock()
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def resolve(self, identity_name: str, evidence: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            self.calls.append((identity_name, evidence))
        self.entered.set()
        self.gate.wait(10)
        self.returned.set()
        return {**_RESOLVED, "status": "unresolved", "reason": "no_link", "principal_id": None}

    def started(self) -> int:
        with self._lock:
            return len(self.calls)


class _RecordingLookup(_ScriptedLookup):
    def __init__(self) -> None:
        super().__init__(_RESOLVED)
        self.called = threading.Event()

    def resolve(self, identity_name: str, evidence: dict[str, Any]) -> dict[str, Any] | None:
        answer = super().resolve(identity_name, evidence)
        self.called.set()
        return answer


def _web_client() -> WebClient:
    web_client = WebClient(token="xoxb-test")
    web_client.chat_postMessage = MagicMock(return_value={"ts": "555.000"})  # type: ignore[method-assign]
    web_client.chat_update = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.chat_postEphemeral = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    web_client.conversations_replies = MagicMock(  # type: ignore[method-assign]
        return_value={"messages": [_CARD_MESSAGE]}
    )
    web_client.views_open = MagicMock(return_value={"ok": True})  # type: ignore[method-assign]
    return web_client


def _app(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    lookup: Any,
    *,
    identity: SlackIdentityCredentials | None = None,
    resolver: ScriptedResolver | None = None,
) -> App:
    return build_app(
        config,
        web_client=_web_client(),
        redis_client=redis_client,
        identity=identity,
        authorize=_authorize,
        resolver=resolver,
        identity_client=lookup,
    )


def _drain(app: App) -> None:
    app.listener_runner.listener_executor.shutdown(wait=True)


def _mention_request(envelope_id: str, event_id: str) -> SocketModeRequest:
    payload = _event_body(_mention(ts=f"1700.{event_id[-4:]}"))
    payload["event_id"] = event_id
    return SocketModeRequest(type="events_api", envelope_id=envelope_id, payload=payload)


def _approval_click(envelope_id: str) -> SocketModeRequest:
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload=_interaction_body(
            "block_actions",
            trigger_id=f"trig-{envelope_id}",
            container={"type": "message", "message_ts": CARD_TS},
            channel={"id": CARD_CHANNEL},
            message=_CARD_MESSAGE,
            actions=[
                {
                    "type": "button",
                    "action_id": APPROVE_ACTION_ID,
                    "action_ts": "2.0",
                    "value": APPROVAL_ID,
                }
            ],
        ),
    )


def _note_submit(envelope_id: str) -> SocketModeRequest:
    return SocketModeRequest(
        type="interactive",
        envelope_id=envelope_id,
        payload=_interaction_body(
            "view_submission",
            trigger_id=f"trig-{envelope_id}",
            view={
                "id": "V1",
                "type": "modal",
                "callback_id": NOTE_MODAL_CALLBACK_ID,
                "private_metadata": json.dumps(
                    {
                        "approval_id": APPROVAL_ID,
                        "channel": CARD_CHANNEL,
                        "card_ts": CARD_TS,
                        "decision": "approved",
                    }
                ),
                "state": {
                    "values": {"note": {"note-input": {"type": "plain_text_input", "value": "ok"}}}
                },
                "title": {"type": "plain_text", "text": "Approve request"},
                "blocks": [],
            },
        ),
    )


def test_a_mention_is_acked_and_enqueued_before_the_lookup_returns(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    lookup = _BlockedLookup()
    try:
        app = _app(config, redis_client, lookup)
        sock = FakeSocketClient()

        SocketModeHandler(app, app_token="xapp-test").handle(
            sock, _mention_request("env-m", "Ev0001")
        )

        assert sock.acked_envelope_ids == ["env-m"]
        assert lookup.entered.wait(5), "the lookup was never submitted"
        _drain(app)
        assert redis_client.xlen(config.stream) == 1
        # Ack and enqueue both happened while the lookup is still blocked.
        assert not lookup.returned.is_set()
        assert lookup.calls == [("default", _evidence())]
    finally:
        lookup.gate.set()
    assert lookup.returned.wait(5)


def test_an_approval_click_is_acked_and_resolved_before_the_lookup_returns(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    lookup = _BlockedLookup()
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by=USER, decision="approved")
    )
    try:
        app = _app(config, redis_client, lookup, resolver=resolver)
        sock = FakeSocketClient()

        SocketModeHandler(app, app_token="xapp-test").handle(sock, _approval_click("env-a"))

        assert sock.acked_envelope_ids == ["env-a"]
        assert lookup.entered.wait(5), "the lookup was never submitted"
        _drain(app)
        assert [call["decision"] for call in resolver.calls] == ["approved"]
        assert not lookup.returned.is_set()
        ((name, evidence),) = lookup.calls
        assert name == "default"
        assert _norm(evidence)["delivery"] == "block_actions"
        assert _norm(evidence)["interaction_user_team_id"] == TEAM
    finally:
        lookup.gate.set()
    assert lookup.returned.wait(5)


def test_a_note_submission_is_acked_and_resolved_before_the_lookup_returns(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    lookup = _BlockedLookup()
    resolver = ScriptedResolver(
        ResolveOutcome(status_code=200, resolved_by=USER, decision="approved")
    )
    try:
        app = _app(config, redis_client, lookup, resolver=resolver)
        sock = FakeSocketClient()

        SocketModeHandler(app, app_token="xapp-test").handle(sock, _note_submit("env-v"))

        assert sock.acked_envelope_ids == ["env-v"]
        assert lookup.entered.wait(5), "the lookup was never submitted"
        _drain(app)
        assert [(c["decision"], c["note"]) for c in resolver.calls] == [("approved", "ok")]
        assert not lookup.returned.is_set()
        ((name, evidence),) = lookup.calls
        assert name == "default"
        assert _norm(evidence)["delivery"] == "view_submission"
    finally:
        lookup.gate.set()
    assert lookup.returned.wait(5)


def test_each_app_sends_its_own_identity_name(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    ops = SlackIdentityCredentials(name="ops-bot", app_token="xapp-ops", bot_token="xoxb-ops")
    default_lookup, ops_lookup = _RecordingLookup(), _RecordingLookup()
    default_app = _app(config, redis_client, default_lookup)
    ops_app = _app(config, redis_client, ops_lookup, identity=ops)

    SocketModeHandler(default_app, app_token="xapp-test").handle(
        FakeSocketClient(), _mention_request("env-d", "Ev0011")
    )
    SocketModeHandler(ops_app, app_token="xapp-ops").handle(
        FakeSocketClient(), _mention_request("env-o", "Ev0012")
    )

    assert default_lookup.called.wait(5) and ops_lookup.called.wait(5)
    _drain(default_app)
    _drain(ops_app)
    assert [name for name, _ in default_lookup.calls] == ["default"]
    assert [name for name, _ in ops_lookup.calls] == ["ops-bot"]


def _lookup_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name.startswith("identity-lookup")]


def test_the_pool_is_shared_bounded_cancellable_and_recreated_lazily(
    config: DispatcherConfig, redis_client: redis.Redis
) -> None:
    lookup = _BlockedLookup()
    apps = [
        _app(
            config,
            redis_client,
            lookup,
            identity=SlackIdentityCredentials(
                name=name, app_token=f"xapp-{name}", bot_token=f"xoxb-{name}"
            ),
        )
        for name in ("alpha", "beta", "gamma")
    ]
    try:
        for index, app in enumerate(apps):
            handler = SocketModeHandler(app, app_token="xapp-test")
            for n in range(2):
                handler.handle(
                    FakeSocketClient(), _mention_request(f"env-{index}-{n}", f"Ev01{index}{n}")
                )
        deadline = time.monotonic() + 5
        while lookup.started() < 2 and time.monotonic() < deadline:
            time.sleep(0.01)
        time.sleep(0.2)  # room for a third worker to appear, if there were one

        # Six lookups across three apps, and one pool of two workers.
        assert lookup.started() == 2
        assert 1 <= len(_lookup_threads()) <= 2

        shutdown = threading.Thread(target=shutdown_identity_lookups, daemon=True)
        shutdown.start()
        shutdown.join(0.5)
    finally:
        lookup.gate.set()
    shutdown.join(5)
    assert not shutdown.is_alive()
    for app in apps:
        _drain(app)

    # The four queued lookups were cancelled, not run after the gate opened.
    time.sleep(0.2)
    assert lookup.started() == 2
    deadline = time.monotonic() + 5
    while _lookup_threads() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert _lookup_threads() == []

    # The next delivery builds a fresh pool.
    later = _RecordingLookup()
    app = _app(config, redis_client, later)
    SocketModeHandler(app, app_token="xapp-test").handle(
        FakeSocketClient(), _mention_request("env-later", "Ev0199")
    )
    assert later.called.wait(5)
    _drain(app)
    assert [name for name, _ in later.calls] == ["default"]


def test_shutdown_closes_the_clients_it_built(
    config: DispatcherConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    built: list[httpx.Client] = []
    real_client = httpx.Client

    class RecordingHttpClient(real_client):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            built.append(self)

    with monkeypatch.context() as patch:
        patch.setattr(httpx, "Client", RecordingHttpClient)
        clients = [
            identity_module.build_identity_client(config),
            identity_module.build_identity_client(config),
        ]

    assert all(isinstance(c, IdentityResolveClient) for c in clients)
    assert built, "build_identity_client built no HTTP client"
    shutdown_identity_lookups()
    assert all(client.is_closed for client in built)


def test_an_app_built_without_a_lookup_client_never_dials_the_api(
    config: DispatcherConfig,
    redis_client: redis.Redis,
    offline_identity_client: IdentityClientBuilds,
) -> None:
    """The autouse guard: production wiring builds the client from config, and
    the suite's stub keeps that off the network. The app's API URL is a real
    loopback listener; admission keeps its own fake API, so any connection that
    lands on the listener came from the identity lookup."""

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    try:
        app_config = config.model_copy(
            update={"api_base_url": f"http://127.0.0.1:{listener.getsockname()[1]}"}
        )
        app = build_app(
            app_config,
            web_client=_web_client(),
            redis_client=redis_client,
            authorize=_authorize,
            admission=build_admission(config, redis_client),
        )
        sock = FakeSocketClient()
        SocketModeHandler(app, app_token="xapp-test").handle(
            sock, _mention_request("env-guard", "Ev0299")
        )
        _drain(app)
        time.sleep(0.3)

        assert sock.acked_envelope_ids == ["env-guard"]
        assert redis_client.xlen(config.stream) == 1
        assert offline_identity_client.configs == [app_config]
        listener.setblocking(False)
        with pytest.raises(BlockingIOError):
            listener.accept()
    finally:
        listener.close()
