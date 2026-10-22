"""Per-identity Socket Mode delivery signals (#4960).

An open socket is not proof that events arrive, so each identity counts the
frames Slack hands its client (``curie.slack.socket.envelopes``) and logs
slack_sdk's session lifecycle on its own logger. Drives slack_sdk's real
``SocketModeClient`` over a stand-in websocket (``offline_socket_mode``) and
reads what a real ``MeterProvider`` exported through the manifest-validated
``record_metric`` path. Nothing here sends Slack.
"""

import logging
import threading
import time
from collections.abc import Callable, Iterator

import pytest
import redis
from curie_dispatcher import run
from curie_dispatcher.app import SocketModeConnection
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials, default_identity_credentials
from curie_dispatcher.preflight import PreflightedIdentity
from curie_telemetry import build_resource, configure_meter_provider
from curie_telemetry import metrics as telemetry_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from slack_bolt import App

from .conftest import _authorize, deliver_frames

_ENVELOPES = "curie.slack.socket.envelopes"

Counts = Callable[[], dict[tuple[str, str], float]]


@pytest.fixture
def envelopes(monkeypatch: pytest.MonkeyPatch) -> Iterator[Counts]:
    """Read the envelope counter as ``{(slack_identity, type): value}``."""
    monkeypatch.setattr(telemetry_metrics, "_provider", None)
    monkeypatch.setattr(telemetry_metrics, "_instruments", {})
    telemetry_metrics.reset_bounded_labels()
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-dispatcher",
            service_version="0.0.0-test",
            service_instance_id="acme-dispatcher-envelope-test",
            deployment_environment="test",
        ),
    )
    configure_meter_provider(provider)

    def read() -> dict[tuple[str, str], float]:
        assert provider.force_flush(timeout_millis=5000)
        data = reader.get_metrics_data()
        points: dict[tuple[str, str], float] = {}
        if data is None:
            return points
        for resource_metrics in data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if metric.name != _ENVELOPES:
                        continue
                    for point in metric.data.data_points:
                        attributes = dict(point.attributes or {})
                        assert set(attributes) == {"service.name", "slack_identity", "type"}
                        assert attributes["service.name"] == "curie-dispatcher"
                        key = (str(attributes["slack_identity"]), str(attributes["type"]))
                        points[key] = float(point.value)
        return points

    yield read
    provider.shutdown()
    telemetry_metrics.reset_bounded_labels()


def _app() -> App:
    return App(
        signing_secret="unused-in-socket-mode",
        authorize=_authorize,
        token_verification_enabled=False,
    )


def _run_in_background(conn: SocketModeConnection) -> threading.Thread:
    thread = threading.Thread(target=conn.run, name="test-socket-envelopes-run", daemon=True)
    thread.start()
    return thread


def _wait_connected(conn: SocketModeConnection, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not conn.is_connected() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert conn.is_connected()


def _envelope(kind: str, envelope_id: str) -> dict[str, object]:
    # Slack's Socket Mode envelope: https://docs.slack.dev/apis/events-api/using-socket-mode/
    return {"envelope_id": envelope_id, "type": kind, "payload": {}, "accepts_response_payload": False}


def test_every_frame_is_counted_by_identity_and_type_before_any_listener(
    offline_socket_mode: None,
    envelopes: Counts,
) -> None:
    """Counted where the SDK enqueues the raw frame, so the count says Slack sent it.

    An unknown type and a frame that is not JSON both land on ``other``; neither
    opens a series of its own.
    """
    conn = SocketModeConnection(
        _app(),
        app_token="xapp-test",
        logger=logging.getLogger("test-socket-envelopes"),
        identity_name="ops",
    )
    client = conn._handler.client
    thread = _run_in_background(conn)
    try:
        _wait_connected(conn)
        client.enqueue_message("not json at all")
        deliver_frames(
            client,
            {"type": "hello", "num_connections": 1},
            _envelope("events_api", "e1"),
            _envelope("events_api", "e2"),
            _envelope("interactive", "i1"),
            _envelope("slash_commands", "s1"),
            _envelope("brand_new_type", "n1"),
        )
    finally:
        conn.close()
        thread.join(timeout=5)

    assert envelopes() == {
        ("ops", "hello"): 1,
        ("ops", "events_api"): 2,
        ("ops", "interactive"): 1,
        ("ops", "slash_commands"): 1,
        ("ops", "other"): 2,
    }


def test_a_disconnect_frame_is_counted_although_no_listener_sees_it(
    offline_socket_mode: None,
    envelopes: Counts,
) -> None:
    """The refresh notice is what keeps a quiet identity's count moving."""
    conn = SocketModeConnection(
        _app(),
        app_token="xapp-test",
        logger=logging.getLogger("test-socket-envelopes-disconnect"),
        identity_name="ops",
    )
    client = conn._handler.client
    thread = _run_in_background(conn)
    try:
        _wait_connected(conn)
        deliver_frames(
            client,
            {"type": "disconnect", "reason": "refresh_requested"},
            {"type": "hello", "num_connections": 1},
        )
    finally:
        conn.close()
        thread.join(timeout=5)

    counts = envelopes()
    assert counts[("ops", "disconnect")] == 1
    assert counts[("ops", "hello")] == 1


def test_a_connection_without_a_name_counts_as_default(
    offline_socket_mode: None,
    envelopes: Counts,
) -> None:
    conn = SocketModeConnection(
        _app(),
        app_token="xapp-test",
        logger=logging.getLogger("test-socket-envelopes-default"),
    )
    client = conn._handler.client
    thread = _run_in_background(conn)
    try:
        _wait_connected(conn)
        deliver_frames(client, {"type": "hello", "num_connections": 1})
    finally:
        conn.close()
        thread.join(timeout=5)

    assert envelopes() == {("default", "hello"): 1}


def test_each_identity_built_by_run_counts_under_its_own_name(
    offline_socket_mode: None,
    envelopes: Counts,
    redis_client: redis.Redis,
) -> None:
    """``run`` names every connection, so each identity is its own series."""
    config = DispatcherConfig(
        slack_app_token="xapp-test",
        slack_bot_token="xoxb-test",
        approval_chat_attester_secret="dispatcher-attester-test-secret",
    )
    secondary = SlackIdentityCredentials(
        name="ops-bot", app_token="xapp-ops", bot_token="xoxb-ops"
    )
    connections = run.build_identity_connections(
        config,
        (
            PreflightedIdentity(default_identity_credentials(config), None),
            PreflightedIdentity(secondary, None),
        ),
        redis_client=redis_client,
        logger=logging.getLogger("curie_dispatcher"),
    )
    sockets = [connection.connect() for connection in connections]
    threads = [_run_in_background(conn) for conn in sockets]
    try:
        for conn in sockets:
            _wait_connected(conn, timeout=15.0)
        deliver_frames(sockets[0]._handler.client, {"type": "hello", "num_connections": 1})
        deliver_frames(
            sockets[1]._handler.client,
            _envelope("events_api", "e1"),
            {"type": "hello", "num_connections": 1},
        )
    finally:
        for conn in sockets:
            conn.close()
        for thread in threads:
            thread.join(timeout=15)

    assert envelopes() == {
        ("default", "hello"): 1,
        ("ops-bot", "events_api"): 1,
        ("ops-bot", "hello"): 1,
    }


class _Collector(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def test_session_lifecycle_logs_at_info_on_a_per_identity_child_logger(
    offline_socket_mode: None,
) -> None:
    """slack_sdk's lifecycle lines reach the service logger's handlers and name the identity.

    Before #4960 the SDK logged on Bolt's ``slack_bolt.App`` logger, which only
    the root WARNING backstop handles, so a reconnect was never seen. The
    service logger here is set to INFO as ``configure_service_logging`` sets it;
    DEBUG frame traces must stay off.
    """
    service = logging.getLogger("test-socket-lifecycle-service")
    service.setLevel(logging.INFO)
    service.propagate = False
    collector = _Collector()
    service.addHandler(collector)
    conn = SocketModeConnection(
        _app(),
        app_token="xapp-test",
        logger=service,
        identity_name="ops",
    )
    client = conn._handler.client
    assert client.logger.name == "test-socket-lifecycle-service.slack_socket.ops"
    thread = _run_in_background(conn)
    try:
        _wait_connected(conn)
        deliver_frames(client, {"type": "hello", "num_connections": 1})
    finally:
        conn.close()
        thread.join(timeout=5)
        service.removeHandler(collector)

    sdk_records = [r for r in collector.records if r.name.endswith(".slack_socket.ops")]
    established = [r for r in sdk_records if "A new session has been established" in r.getMessage()]
    assert established, [r.getMessage() for r in collector.records]
    assert all(r.levelno == logging.INFO for r in established)
    assert all(r.levelno >= logging.INFO for r in sdk_records)
