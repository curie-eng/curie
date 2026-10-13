"""Shared fixtures. Stream/dedupe tests run against the REAL Valkey from the
compose stack (per repo test discipline: never mock Valkey). The Slack Web API
and socket transport are faked; `_black_hole_api` is not a fake but a real
loopback socket standing in for an endpoint that never answers.

The platform API is another service to the dispatcher, reached over HTTP, so
`admission_api` stands it in with a real loopback HTTP server for
`POST /channels/admission` (ADR 0175), the same boundary the preflight suite
uses. Its caller lists are plain sets: the dispatcher decides nothing about who
is listed, it only relays the API's answer, so the fake need only answer
consistently."""
import json
import logging
import socket
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
import redis
from curie_dispatcher.approval_actions import ResolveOutcome
from curie_dispatcher.config import DispatcherConfig
from curie_test_support.valkey import (
    VALKEY_HOST as _VALKEY_HOST,
)
from curie_test_support.valkey import (
    VALKEY_PORT as _VALKEY_PORT,
)
from curie_test_support.valkey import (
    VALKEY_PW as _VALKEY_PW,
)
from curie_test_support.valkey import (
    connect_or_skip,
)
from slack_bolt.authorization import AuthorizeResult
from slack_sdk.socket_mode.builtin import client as builtin_socket_mode_client


def _authorize(**_kwargs: Any) -> AuthorizeResult:
    """Shared authorization stub: Bolt's ``authorize`` callback resolved to a
    fixed bot identity, so Socket Mode tests skip the real auth.test call."""
    return AuthorizeResult(
        enterprise_id=None,
        team_id="T1",
        bot_token="xoxb-test",
        bot_id="B1",
        bot_user_id="U0BOT",
    )


def _set_run_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear ambient dispatcher config and install only public test values."""
    for name, field in DispatcherConfig.model_fields.items():
        alias = field.validation_alias
        monkeypatch.delenv(
            alias if isinstance(alias, str) else name.upper(), raising=False
        )
    monkeypatch.setenv(
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "dispatcher-attester-test-secret"
    )


def person_rooted_thread(**kwargs: Any) -> dict[str, Any]:
    """Slack's ``conversations.replies`` answer for a thread a person started.

    A threaded mention may look up its thread's root (spec
    slack-alert-followup-context), so a harness built on a real ``WebClient``
    answers that call here instead of reaching Slack. The parent message comes
    first (https://docs.slack.dev/reference/methods/conversations.replies/),
    and a person's root never belongs to the bot, so the turn is unchanged.
    """
    return {
        "ok": True,
        "messages": [{"ts": kwargs["ts"], "user": "U0PERSON", "text": "A person's root."}],
        "has_more": False,
    }


class _TestTelemetry:
    """A telemetry stand-in for ``run.main`` tests: ``shutdown`` is a no-op."""

    def shutdown(self) -> None:
        pass


class FakeSocketClient:
    """Captures the envelope acks Bolt sends back over the socket."""

    def __init__(self) -> None:
        self.logger = logging.getLogger("fake-socket")
        self.acked_envelope_ids: list[str] = []
        # The ack BODY, not just the id (#1053). A block_actions ack is empty,
        # but a view_submission ack is a channel in its own right: it is where a
        # refused submission's reason is rendered, since the approver is standing
        # in an open modal and an ephemeral would post behind it. A test cannot
        # assert that from the envelope id alone.
        self.ack_payloads: dict[str, Any] = {}

    def send_socket_mode_response(self, response: Any) -> None:
        self.acked_envelope_ids.append(response.envelope_id)
        self.ack_payloads[response.envelope_id] = getattr(response, "payload", None)

    def ack_payload_for(self, envelope_id: str) -> Any:
        """The body this envelope was acked with, or None."""

        return self.ack_payloads.get(envelope_id)


def deliver_once(
    handler: Any,
    sock: FakeSocketClient,
    app: Any,
    request: Any,
) -> None:
    """Handle exactly one Socket Mode envelope and return.

    It does not walk other connections and does not stop at the first ack.
    Owner-only proof is one delivery to the
    non-owner (no ack, no mutate) then one delivery to the owner, not a loop
    until someone acks (#2307).
    """

    handler.handle(sock, request)
    app.listener_runner.listener_executor.shutdown(wait=True)


@contextmanager
def _black_hole_api() -> Iterator[str]:
    """A real port that completes the TCP handshake and then never answers.

    Nothing accepts the connection; the kernel's listen backlog completes the
    handshake, so `connect` succeeds and the client is left reading from a
    socket no one writes to. A refused connection fails instantly and can
    never show a probe running past its deadline, so this is the fixture for
    the opposite case: a probe whose read phase is what has to give up,
    exactly the scenario an unbounded probe overshoots on. The preflight
    suite is one consumer of this.
    """
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    try:
        yield f"http://127.0.0.1:{sock.getsockname()[1]}"
    finally:
        sock.close()


@dataclass
class FakeAdmissionApi:
    """A loopback stand-in for the platform API's ``POST /channels/admission``.

    Attributes:
        url: the base URL to hand the dispatcher as ``api_base_url``.
        lists: ``(address, adapter) -> caller ids`` for every route that
            carries a list; a route absent here is open to everyone.
        down: when True every request answers 503, the API-outage case.
        predates: when True every request answers FastAPI's own route-miss
            404, the platform API from before ADR 0175 had the route.
        delay_s: how long each answer takes, for the single-flight tests.
        requests: every request body received, in order.
        headers: the ``X-API-Key`` each request carried, in order.
    """

    url: str
    lists: dict[tuple[str, str | None], set[str]] = dataclass_field(default_factory=dict)
    down: bool = False
    predates: bool = False
    delay_s: float = 0.0
    requests: list[dict[str, Any]] = dataclass_field(default_factory=list)
    headers: list[str | None] = dataclass_field(default_factory=list)


@contextmanager
def fake_admission_api() -> Iterator[FakeAdmissionApi]:
    """Serve a `FakeAdmissionApi` on a free loopback port until the block exits."""

    state = FakeAdmissionApi(url="")

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"{}")
            state.requests.append(body)
            state.headers.append(self.headers.get("X-API-Key"))
            if self.path != "/channels/admission":
                self.send_response(404)
                self.end_headers()
                return
            if state.delay_s:
                time.sleep(state.delay_s)
            if state.down:
                self.send_response(503)
                self.end_headers()
                return
            if state.predates:
                # Exactly what FastAPI answers for a path it has no route for.
                payload = b'{"detail":"Not Found"}'
                self.send_response(404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return
            listed = state.lists.get((body.get("address"), body.get("adapter")))
            install_restricted = bool(state.lists)
            if listed is None:
                answer = {
                    "allowed": True,
                    "restricted": False,
                    "install_restricted": install_restricted,
                }
            else:
                allowed = any(caller in listed for caller in body.get("callers", []))
                answer = {"allowed": allowed, "restricted": True, "install_restricted": True}
            payload = json.dumps(answer).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *args: object) -> None:
            """Keep the test server out of the process's terminal log."""

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


@pytest.fixture
def admission_api() -> Iterator[FakeAdmissionApi]:
    """A fresh fake platform API per test; every route open unless listed."""

    with fake_admission_api() as api:
        yield api


# Compose defaults and connection params come from the shared curie_test_support.valkey helper.


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def config(
    redis_client: redis.Redis, admission_api: FakeAdmissionApi
) -> Iterator[DispatcherConfig]:
    """A config with a per-test-unique stream and dedupe prefix so tests do not
    collide, cleaned up afterwards. Its platform API is the per-test
    `admission_api`, so the caller-list check (ADR 0175) has something to ask."""
    token = uuid.uuid4().hex
    cfg = DispatcherConfig(
        slack_app_token="xapp-test",
        slack_bot_token="xoxb-test",
        valkey_host=_VALKEY_HOST,
        valkey_port=_VALKEY_PORT,
        valkey_password=_VALKEY_PW,
        stream=f"test:curie:runs:{token}",
        dedupe_prefix=f"test:curie:dedupe:{token}:",
        dedupe_ttl_seconds=60,
        placeholder_text="Working on it.",
        # ADR-0106: this is deliberately distinct from the platform API key.
        # Socket Mode interactions become a chat principal only when the
        # dispatcher can sign an attestation with this dedicated credential.
        approval_chat_attester_secret="dispatcher-attester-test-secret",
        api_base_url=admission_api.url,
        admission_cache_prefix=f"test:curie:admission:{token}:",
        # Every threaded mention may consult the root-context cache, so every
        # test gets its own prefix: tests reuse the same example channel, bot
        # and timestamps, and must never read each other's cached roots.
        thread_context_cache_prefix=f"test:curie:thread-context:{token}:",
    )
    yield cfg
    keys = list(redis_client.scan_iter(f"test:curie:dedupe:{token}:*"))
    keys.extend(redis_client.scan_iter(f"test:curie:admission:{token}:*"))
    keys.extend(redis_client.scan_iter(f"test:curie:thread-context:{token}:*"))
    keys.append(cfg.stream)
    if keys:
        redis_client.delete(*keys)


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


class SlackSocketStandIn:
    """slack_sdk's builtin websocket ``Connection``, without the network.

    It opens at once and stays open until the SDK closes it or a test drops
    it, so ``SocketModeClient``'s own connect, refresh and reconnect code runs
    unchanged around it. The constructor takes the SDK's keyword arguments and
    ignores them.
    """

    def __init__(self, **_kwargs: Any) -> None:
        self.session_id = uuid.uuid4().hex
        self._open = False

    def connect(self) -> None:
        self._open = True

    def is_active(self) -> bool:
        return self._open

    def close(self) -> None:
        self._open = False

    def disconnect(self) -> None:
        self._open = False

    def check_state(self) -> None:
        return None

    def send(self, payload: str) -> None:
        del payload

    def run_until_completion(self, state: Any) -> None:
        while self._open and not state.terminated:
            time.sleep(0.01)


@pytest.fixture
def offline_socket_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake only the Socket Mode transport: the websocket, and the
    ``apps.connections.open`` call that issues its URL."""
    monkeypatch.setattr(builtin_socket_mode_client, "Connection", SlackSocketStandIn)
    monkeypatch.setattr(
        builtin_socket_mode_client.SocketModeClient,
        "issue_new_wss_url",
        lambda _self: "wss://wss.example.invalid/link",
    )


def deliver_frames(client: Any, *frames: dict[str, Any], timeout: float = 5.0) -> None:
    """Hand Slack frames to the SDK's own message queue, in order, and return
    once every message listener has run for the last one.

    A ``disconnect`` frame never reaches the listeners (the SDK reconnects on
    it instead), so the last frame must be one that does.
    """
    last = frames[-1]
    done = threading.Event()

    def _after_the_others(_client: Any, message: dict[str, Any], _raw: Any) -> None:
        if message == last:
            done.set()

    client.message_listeners.append(_after_the_others)
    try:
        for frame in frames:
            client.enqueue_message(json.dumps(frame))
        assert done.wait(timeout), f"the SDK did not deliver {last!r} within {timeout}s"
    finally:
        client.message_listeners.remove(_after_the_others)


@dataclass
class IdentityClientBuilds:
    """What the offline ``build_identity_client`` stub was asked to build."""

    configs: list[Any] = dataclass_field(default_factory=list)


@pytest.fixture(autouse=True)
def offline_identity_client(monkeypatch: pytest.MonkeyPatch) -> IdentityClientBuilds:
    """Keep every app's principal lookup (#2910) off the network.

    ``build_app``/``register_handlers`` build the production lookup client from
    config when a test injects none. Its base URL is a real loopback API in
    most suites (``admission_api``), which has no ``/identity/resolve`` route,
    so an unpatched lookup would add a stray POST and a WARNING to tests that
    assert on both. The stub records the call and returns None: no client, no
    lookup. ``raising=False`` keeps the suite importable before the seam exists.
    """

    builds = IdentityClientBuilds()

    def _offline_build(config: Any) -> None:
        builds.configs.append(config)
        return None

    monkeypatch.setattr(
        "curie_dispatcher.handlers.build_identity_client", _offline_build, raising=False
    )
    return builds


@dataclass
class InstallationReportStarts:
    """Every ``start_installation_reports`` call ``run.main`` made."""

    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = dataclass_field(default_factory=list)


@pytest.fixture(autouse=True)
def offline_installation_reports(monkeypatch: pytest.MonkeyPatch) -> InstallationReportStarts:
    """Keep ``run.main`` from starting the #3039 reporter threads.

    The reporter calls ``auth.test`` with each identity's real token and posts
    to the platform API; a ``run.main`` test fakes neither. The stub records
    the call and starts nothing.
    """

    starts = InstallationReportStarts()

    def _offline_start(*args: Any, **kwargs: Any) -> list[threading.Thread]:
        starts.calls.append((args, kwargs))
        return []

    monkeypatch.setattr(
        "curie_dispatcher.run.start_installation_reports", _offline_start, raising=False
    )
    return starts
