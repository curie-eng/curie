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
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
import redis
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


def deliver_until_acked(
    connections: list[tuple[Any, FakeSocketClient, Any]],
    request: Any,
) -> FakeSocketClient | None:
    """Deliver one Socket Mode envelope to each connection until one acks.

    Slack retries an unacked envelope on another connection of the same app
    (https://docs.slack.dev/apis/events-api/using-socket-mode/#using-multiple-connections).
    This is the fake-app stand-in for that retry: the same envelope_id, in
    order, stopping at the first ack.
    """

    for handler, sock, app in connections:
        handler.handle(sock, request)
        app.listener_runner.listener_executor.shutdown(wait=True)
        if request.envelope_id in sock.acked_envelope_ids:
            return sock
    return None


def deliver_once(
    handler: Any,
    sock: FakeSocketClient,
    app: Any,
    request: Any,
) -> None:
    """Handle exactly one Socket Mode envelope and return.

    Unlike ``deliver_until_acked``, this does not walk other connections and
    does not stop at the first ack. Owner-only proof is one delivery to the
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
        requests: every request body received, in order.
        headers: the ``X-API-Key`` each request carried, in order.
    """

    url: str
    lists: dict[tuple[str, str | None], set[str]] = dataclass_field(default_factory=dict)
    down: bool = False
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
            if state.down:
                self.send_response(503)
                self.end_headers()
                return
            listed = state.lists.get((body.get("address"), body.get("adapter")))
            if listed is None:
                answer = {"allowed": True, "restricted": False}
            else:
                allowed = any(caller in listed for caller in body.get("callers", []))
                answer = {"allowed": allowed, "restricted": True}
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
    )
    yield cfg
    keys = list(redis_client.scan_iter(f"test:curie:dedupe:{token}:*"))
    keys.append(cfg.stream)
    if keys:
        redis_client.delete(*keys)
