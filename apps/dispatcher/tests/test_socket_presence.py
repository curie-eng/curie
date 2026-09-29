"""The Slack socket gauge: configured identities against those holding a live socket.

Drives slack_sdk's real ``SocketModeClient`` over a stand-in websocket
(``offline_socket_mode``) and reads what a real ``MeterProvider`` exported through
the manifest-validated ``record_metric`` path. Nothing here sends Slack.
"""

import logging
import threading
import time
from collections.abc import Callable, Iterator

import pytest
from curie_dispatcher import run
from curie_dispatcher.app import SocketModeConnection
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.socket_presence import SocketPresence
from curie_telemetry import build_resource, configure_meter_provider
from curie_telemetry import metrics as telemetry_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from slack_bolt import App

from .conftest import _authorize

_GAUGE = "curie.slack.socket.identities"
_SAMPLE_S = 0.02

Gauge = Callable[[], dict[str, float]]


@pytest.fixture
def gauge(monkeypatch: pytest.MonkeyPatch) -> Iterator[Gauge]:
    """Read the identities gauge as ``{state: value}`` from a real exporter."""
    monkeypatch.setattr(telemetry_metrics, "_provider", None)
    monkeypatch.setattr(telemetry_metrics, "_instruments", {})
    reader = InMemoryMetricReader()
    provider = MeterProvider(
        metric_readers=[reader],
        resource=build_resource(
            "curie-dispatcher",
            service_version="0.0.0-test",
            service_instance_id="acme-dispatcher-presence-test",
            deployment_environment="test",
        ),
    )
    configure_meter_provider(provider)

    def read() -> dict[str, float]:
        assert provider.force_flush(timeout_millis=5000)
        data = reader.get_metrics_data()
        points: dict[str, float] = {}
        if data is None:
            return points
        for resource_metrics in data.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if metric.name != _GAUGE:
                        continue
                    for point in metric.data.data_points:
                        attributes = dict(point.attributes or {})
                        assert set(attributes) == {"service.name", "state"}, attributes
                        assert attributes["service.name"] == "curie-dispatcher"
                        points[str(attributes["state"])] = float(point.value)
        return points

    yield read
    provider.shutdown()


def _wait_for(gauge: Gauge, expected: dict[str, float], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    seen = gauge()
    while seen != expected and time.monotonic() < deadline:
        time.sleep(_SAMPLE_S)
        seen = gauge()
    assert seen == expected, f"gauge stayed at {seen!r}, expected {expected!r}"


def _app() -> App:
    return App(
        signing_secret="unused-in-socket-mode",
        authorize=_authorize,
        token_verification_enabled=False,
    )


def _run_in_background(conn: SocketModeConnection) -> threading.Thread:
    """``run`` blocks until ``close``, as it does under the supervisor."""
    thread = threading.Thread(target=conn.run, name="test-socket-mode-run", daemon=True)
    thread.start()
    return thread


def test_gauge_follows_the_socket_through_connect_drop_reconnect_and_close(
    offline_socket_mode: None,
    gauge: Gauge,
) -> None:
    """The gauge is sampled from the socket itself, not from events.

    slack_sdk's stale ping check closes a socket without calling any listener
    (``Connection.check_state`` calls ``disconnect``, slack_sdk 3.44.1), which is
    the dead link the heartbeat file cannot see. The drop below is that path.
    """

    presence = SocketPresence(configured=1, sample_interval_s=_SAMPLE_S)
    conn = SocketModeConnection(
        _app(),
        app_token="xapp-test",
        logger=logging.getLogger("test-socket-presence"),
        presence=presence,
    )
    client = conn._handler.client
    thread = _run_in_background(conn)
    try:
        _wait_for(gauge, {"configured": 1, "connected": 1})

        assert client.current_session is not None
        client.current_session.disconnect()
        _wait_for(gauge, {"configured": 1, "connected": 0})

        # What the SDK's session monitor calls once it sees the link is gone.
        client.connect_to_new_endpoint()
        _wait_for(gauge, {"configured": 1, "connected": 1})
    finally:
        conn.close()
        thread.join(timeout=5)

    assert not thread.is_alive()
    assert gauge() == {"configured": 1, "connected": 0}


def test_gauge_counts_a_configured_identity_whose_connect_failed(
    offline_socket_mode: None,
    monkeypatch: pytest.MonkeyPatch,
    gauge: Gauge,
) -> None:
    """An identity that never connects is configured and not connected."""

    def refuse(_self: object) -> str:
        raise ConnectionError("apps.connections.open unreachable")

    monkeypatch.setattr(
        "slack_sdk.socket_mode.builtin.client.SocketModeClient.issue_new_wss_url", refuse
    )
    presence = SocketPresence(configured=1, sample_interval_s=_SAMPLE_S)
    conn = SocketModeConnection(
        _app(),
        app_token="xapp-test",
        logger=logging.getLogger("test-socket-presence-refused"),
        presence=presence,
    )
    try:
        with pytest.raises(ConnectionError):
            conn.run()
        assert gauge() == {"configured": 1, "connected": 0}
    finally:
        conn.close()


def test_the_supervisors_connections_report_one_configured_identity(
    offline_socket_mode: None,
    gauge: Gauge,
) -> None:
    """The dispatcher serves one Slack app, and its connections report to the gauge."""

    config = DispatcherConfig(
        slack_app_token="xapp-test",
        slack_bot_token="xoxb-test",
        approval_chat_attester_secret="dispatcher-attester-test-secret",
    )
    supervisor = run.build_supervisor(config, logger=logging.getLogger("curie_dispatcher"))
    conn = supervisor._connect()
    assert isinstance(conn, SocketModeConnection)
    thread = _run_in_background(conn)
    try:
        _wait_for(gauge, {"configured": 1, "connected": 1}, timeout=15.0)
    finally:
        conn.close()
        thread.join(timeout=15)
    assert gauge() == {"configured": 1, "connected": 0}
