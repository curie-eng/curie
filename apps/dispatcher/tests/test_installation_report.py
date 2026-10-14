"""The #3039 installation reporter (ADR 0198 decision 4).

Each Slack identity's own ``auth.test`` answer is reported to the platform
API's ``POST /identity/slack-reports``, so the API can attach the identity to
its installation and record the non-Grid evidence the mention path needs. It
runs in the background, one thread per identity, independent of preflight
(which calls ``auth.test`` only when several identities are declared), and it
never blocks startup or Slack traffic.

Faked: the Slack Web client (its ``auth_test`` only) and the platform API (a
real loopback HTTP server).
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
from pathlib import Path
from typing import Any

import pytest
from curie_dispatcher.installation_report import (
    InstallationReportClient,
    auth_test_report,
    start_installation_reports,
)
from slack_sdk.web import WebClient
from slack_sdk.web.slack_response import SlackResponse

from .conftest import InstallationReportStarts, _set_run_env, _TestTelemetry

TEAM = "THOME0001"
# The observed auth.test of an ordinary non-Grid workspace (rev 4.3), anonymised.
AUTH_TEST_CAPTURE: dict[str, Any] = json.loads(
    (
        Path(__file__).resolve().parents[3]
        / "apps"
        / "api"
        / "tests"
        / "fixtures"
        / "slack_auth_test_capture.json"
    ).read_text()
)
API_KEY = "platform-api-test-key"
BOT_TOKEN = "xoxb-never-logged-2910"  # gitleaks:allow (fake; asserted never logged)
REPORTED = {
    "identity_id": "0c1d0000-0000-0000-0000-000000003039",
    "provider_installation_id": "0b1a0000-0000-0000-0000-000000003039",
    "namespace_id": "0a5e0000-0000-0000-0000-000000003039",
    "installation_mismatch": False,
}


def _auth_test(**data: Any) -> SlackResponse:
    """What ``WebClient.auth_test`` returns: a ``SlackResponse``."""

    return SlackResponse(
        client=None,
        http_verb="POST",
        api_url="https://slack.com/api/auth.test",
        req_args={},
        data=data,
        headers={},
        status_code=200,
    )


def _home_auth_test() -> SlackResponse:
    # The capture's non-Grid answer: enterprise_id present and null.
    return _auth_test(
        ok=True,
        url="https://example.invalid/",
        team="Home",
        user="curie",
        team_id=TEAM,
        user_id="UBOT00001",
        bot_id="BBOT00001",
        enterprise_id=None,
        is_enterprise_install=False,
    )


# --- auth_test_report -----------------------------------------------------------


def test_an_ok_answer_maps_to_the_report_body() -> None:
    assert auth_test_report("ops-bot", _home_auth_test()) == {
        "name": "ops-bot",
        "team_id": TEAM,
        "enterprise_id": None,
        "enterprise_id_present": True,
        "is_enterprise_install": False,
    }


def test_an_absent_enterprise_id_is_not_a_null_one() -> None:
    report = auth_test_report("default", _auth_test(ok=True, team_id=TEAM))

    assert report == {
        "name": "default",
        "team_id": TEAM,
        "enterprise_id": None,
        "enterprise_id_present": False,
        "is_enterprise_install": None,
    }


def test_a_grid_answer_keeps_its_enterprise_id() -> None:
    report = auth_test_report(
        "default",
        _auth_test(ok=True, team_id=TEAM, enterprise_id="E0GRID001", is_enterprise_install=True),
    )

    assert report is not None
    assert report["enterprise_id"] == "E0GRID001"
    assert report["enterprise_id_present"] is True
    assert report["is_enterprise_install"] is True


def test_the_captured_answer_maps_to_absent_enterprise_id() -> None:
    """Rev 4.3: the observed answer omits enterprise_id entirely."""

    assert "enterprise_id" not in AUTH_TEST_CAPTURE
    report = auth_test_report("default", _auth_test(**AUTH_TEST_CAPTURE))

    assert report is not None
    assert {
        k: report[k] for k in ("enterprise_id", "enterprise_id_present", "is_enterprise_install")
    } == {
        "enterprise_id": None,
        "enterprise_id_present": False,
        "is_enterprise_install": False,
    }
    assert report["team_id"] == AUTH_TEST_CAPTURE["team_id"]


MALFORMED_ENTERPRISE_IDS = [123, True, ["E0GRID001"], {"id": "E0GRID001"}]


@pytest.mark.parametrize(
    "value", MALFORMED_ENTERPRISE_IDS, ids=["number", "bool", "list", "object"]
)
def test_a_malformed_enterprise_id_is_no_report(value: Any) -> None:
    """A present enterprise_id that is neither a string nor null is malformed:
    not Grid, not non-Grid, so nothing is reported."""

    response = _auth_test(ok=True, team_id=TEAM, enterprise_id=value, is_enterprise_install=False)

    assert auth_test_report("default", response) is None


@pytest.mark.parametrize("value", ["false", 0, 1, "true", None])
def test_is_enterprise_install_is_kept_only_as_a_strict_bool(value: Any) -> None:
    report = auth_test_report(
        "default",
        _auth_test(ok=True, team_id=TEAM, enterprise_id=None, is_enterprise_install=value),
    )

    assert report is not None
    assert report["is_enterprise_install"] is None


@pytest.mark.parametrize(
    "response",
    [
        _auth_test(ok=False, error="invalid_auth"),
        _auth_test(error="invalid_auth"),
    ],
    ids=["not-ok", "ok-missing"],
)
def test_a_not_ok_answer_is_no_report(response: SlackResponse) -> None:
    assert auth_test_report("default", response) is None


# --- the loopback API ---------------------------------------------------------------


@dataclass
class _ReportsApi:
    url: str = ""
    # Answered in order; the last one repeats.
    statuses: list[int] = field(default_factory=lambda: [200])
    answer: dict[str, Any] = field(default_factory=lambda: dict(REPORTED))
    paths: list[str] = field(default_factory=list)
    bodies: list[Any] = field(default_factory=list)
    keys: list[str | None] = field(default_factory=list)
    received: threading.Event = field(default_factory=threading.Event)


@contextmanager
def _reports_api() -> Iterator[_ReportsApi]:
    state = _ReportsApi()

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - stdlib handler API
            length = int(self.headers.get("Content-Length") or 0)
            state.paths.append(self.path)
            state.bodies.append(json.loads(self.rfile.read(length) or b"null"))
            state.keys.append(self.headers.get("X-API-Key"))
            index = min(len(state.bodies), len(state.statuses)) - 1
            status = state.statuses[index]
            payload = (
                json.dumps(state.answer).encode()
                if status == 200
                else json.dumps({"detail": "nope"}).encode()
            )
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            state.received.set()

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


def _refused_url() -> str:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return f"http://127.0.0.1:{port}"


_PAYLOAD = {
    "name": "default",
    "team_id": TEAM,
    "enterprise_id": None,
    "enterprise_id_present": True,
    "is_enterprise_install": False,
}


# --- InstallationReportClient.report -----------------------------------------------


def test_a_report_posts_the_payload_with_the_platform_key() -> None:
    with _reports_api() as api:
        client = InstallationReportClient(api_base_url=api.url, api_key=API_KEY)

        assert client.report(dict(_PAYLOAD)) == "ok"

    assert api.paths == ["/identity/slack-reports"]
    assert api.bodies == [_PAYLOAD]
    assert api.keys == [API_KEY]


@pytest.mark.parametrize("status", [404, 500, 502, 503])
def test_a_404_or_5xx_report_is_retry(status: int) -> None:
    with _reports_api() as api:
        api.statuses = [status]
        client = InstallationReportClient(api_base_url=api.url, api_key=API_KEY)

        assert client.report(dict(_PAYLOAD)) == "retry"


@pytest.mark.parametrize("status", [401, 403, 422])
def test_a_401_403_or_422_report_is_permanent(status: int) -> None:
    with _reports_api() as api:
        api.statuses = [status]
        client = InstallationReportClient(api_base_url=api.url, api_key=API_KEY)

        assert client.report(dict(_PAYLOAD)) == "permanent"


def test_a_refused_connection_is_retry() -> None:
    client = InstallationReportClient(api_base_url=_refused_url(), api_key=API_KEY)

    assert client.report(dict(_PAYLOAD)) == "retry"


def test_any_transport_exception_is_retry() -> None:
    class Exploding:
        def post(self, *_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("not an httpx error")

    client = InstallationReportClient(
        api_base_url="http://api.example.invalid",
        api_key=API_KEY,
        client=Exploding(),  # type: ignore[arg-type]
    )

    assert client.report(dict(_PAYLOAD)) == "retry"


# --- start_installation_reports -----------------------------------------------------


class _FakeWebClient:
    """Just ``auth_test``: answers in order, the last one repeating. An
    exception instance in the script is raised instead of returned."""

    def __init__(self, *script: SlackResponse | Exception, token: str = BOT_TOKEN) -> None:
        self.token = token
        self.script = list(script) or [_home_auth_test()]
        self.calls = 0

    def auth_test(self, **_kwargs: Any) -> SlackResponse:
        index = min(self.calls, len(self.script) - 1)
        self.calls += 1
        answer = self.script[index]
        if isinstance(answer, Exception):
            raise answer
        return answer


def _start(
    api: _ReportsApi,
    identities: list[tuple[str, Any]],
    *,
    stop_event: threading.Event | None = None,
    initial_backoff_s: float = 0.01,
    max_backoff_s: float = 0.05,
) -> tuple[list[threading.Thread], threading.Event]:
    stop = stop_event or threading.Event()
    threads = start_installation_reports(
        identities,
        InstallationReportClient(api_base_url=api.url, api_key=API_KEY),
        stop_event=stop,
        initial_backoff_s=initial_backoff_s,
        max_backoff_s=max_backoff_s,
    )
    return threads, stop


def _join(threads: list[threading.Thread], timeout: float = 5.0) -> None:
    for thread in threads:
        thread.join(timeout)
    assert not [t for t in threads if t.is_alive()], "a reporter thread did not finish"


def test_a_single_identity_is_still_reported() -> None:
    """Preflight skips auth.test for one identity; the reporter does not."""

    web = _FakeWebClient()
    with _reports_api() as api:
        threads, stop = _start(api, [("default", web)])
        _join(threads)

    assert len(threads) == 1 and threads[0].daemon
    assert web.calls == 1
    assert api.paths == ["/identity/slack-reports"]
    assert api.bodies == [_PAYLOAD]
    assert api.keys == [API_KEY]
    stop.set()


def test_every_identity_reports_with_its_own_client() -> None:
    default_web = _FakeWebClient()
    ops_web = _FakeWebClient(
        _auth_test(ok=True, team_id="TOPS00001", enterprise_id=None, is_enterprise_install=False),
        token="xoxb-ops-never-logged",
    )
    with _reports_api() as api:
        threads, _ = _start(api, [("default", default_web), ("ops-bot", ops_web)])
        _join(threads)

    assert len(threads) == 2
    assert (default_web.calls, ops_web.calls) == (1, 1)
    assert sorted(api.bodies, key=lambda b: b["name"]) == [
        _PAYLOAD,
        {**_PAYLOAD, "name": "ops-bot", "team_id": "TOPS00001"},
    ]


def test_a_503_is_retried_until_the_report_lands() -> None:
    with _reports_api() as api:
        api.statuses = [503, 503, 200]
        threads, _ = _start(api, [("default", _FakeWebClient())])
        _join(threads)

    assert api.bodies == [_PAYLOAD, _PAYLOAD, _PAYLOAD]


def test_an_auth_test_failure_or_not_ok_is_retried_without_posting() -> None:
    web = _FakeWebClient(
        RuntimeError("slack unreachable"),
        _auth_test(ok=False, error="ratelimited"),
        _home_auth_test(),
    )
    with _reports_api() as api:
        threads, _ = _start(api, [("default", web)])
        _join(threads)

    assert web.calls == 3
    assert api.bodies == [_PAYLOAD]


class _RecordingStop(threading.Event):
    """A stop event that records every backoff wait and never sleeps."""

    def __init__(self) -> None:
        super().__init__()
        self.waits: list[float] = []

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout if timeout is not None else -1.0)
        return super().wait(0)


def test_a_404_retries_at_max_backoff_and_a_5xx_with_backoff() -> None:
    """A 404 (the identity is not declared yet) waits the longest backoff; a
    5xx backs off from the initial value."""

    with _reports_api() as api:
        api.statuses = [503, 404, 200]
        stop = _RecordingStop()
        threads, _ = _start(
            api,
            [("default", _FakeWebClient())],
            stop_event=stop,
            initial_backoff_s=0.01,
            max_backoff_s=0.5,
        )
        _join(threads)

    assert len(api.bodies) == 3
    assert len(stop.waits) == 2
    assert stop.waits[0] == pytest.approx(0.01)
    assert stop.waits[1] == pytest.approx(0.5)


@pytest.mark.parametrize("status", [401, 403, 422])
def test_a_permanent_answer_logs_one_error_and_stops_the_thread(
    status: int, caplog: pytest.LogCaptureFixture
) -> None:
    web = _FakeWebClient()
    with _reports_api() as api:
        api.statuses = [status]
        stop = threading.Event()
        try:
            with caplog.at_level(logging.INFO):
                # Never stopped from outside: the thread must end on its own.
                threads, _ = _start(api, [("default", web)], stop_event=stop)
                _join(threads, timeout=2.0)
            time.sleep(0.1)
            posts = len(api.bodies)
        finally:
            stop.set()

    assert posts == 1, "a permanent answer was retried"
    assert web.calls == 1
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "default" in errors[0].getMessage()
    assert str(status) in errors[0].getMessage()


@pytest.mark.parametrize(
    "value", MALFORMED_ENTERPRISE_IDS, ids=["number", "bool", "list", "object"]
)
def test_a_malformed_enterprise_id_is_permanent_and_posts_nothing(
    value: Any, caplog: pytest.LogCaptureFixture
) -> None:
    """Fix review r5 #2: an ok answer whose enterprise_id has the wrong type
    will not fix itself on retry, so the thread logs one ERROR (the shape,
    never the values) and stops instead of calling Slack every minute."""

    web = _FakeWebClient(
        _auth_test(ok=True, team_id=TEAM, enterprise_id=value, is_enterprise_install=False)
    )
    with _reports_api() as api:
        with caplog.at_level(logging.INFO):
            threads, stop = _start(api, [("default", web)])
            # Several backoff periods (0.01 s doubling to 0.05 s) pass here, so
            # a retrying thread would have called auth.test again.
            _join(threads, timeout=2)
            assert not any(t.is_alive() for t in threads), "the thread kept retrying"
            stop.set()

    assert web.calls == 1
    assert api.bodies == []
    errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1, errors
    assert "default" in errors[0]
    assert repr(value) not in errors[0]


def test_a_refused_api_is_retried() -> None:
    web = _FakeWebClient()
    stop = threading.Event()
    threads = start_installation_reports(
        [("default", web)],
        InstallationReportClient(api_base_url=_refused_url(), api_key=API_KEY),
        stop_event=stop,
        initial_backoff_s=0.01,
        max_backoff_s=0.02,
    )
    deadline = time.monotonic() + 5
    while web.calls < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    stop.set()
    _join(threads)

    assert web.calls >= 3


def test_setting_stop_ends_a_thread_waiting_out_its_backoff() -> None:
    with _reports_api() as api:
        api.statuses = [503]
        threads, stop = _start(
            api, [("default", _FakeWebClient())], initial_backoff_s=30.0, max_backoff_s=60.0
        )
        assert api.received.wait(5)
        started = time.monotonic()
        stop.set()
        _join(threads, timeout=2.0)

    assert time.monotonic() - started < 2.0
    assert len(api.bodies) == 1


def test_a_mismatch_is_a_warning(caplog: pytest.LogCaptureFixture) -> None:
    with _reports_api() as api:
        api.answer = {**REPORTED, "installation_mismatch": True}
        with caplog.at_level(logging.INFO):
            threads, _ = _start(api, [("ops-bot", _FakeWebClient())])
            _join(threads)

    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len([w for w in warnings if "installation_mismatch" in w]) == 1
    assert any("ops-bot" in w and TEAM in w for w in warnings if "installation_mismatch" in w)
    # A mismatch is an answer, not a failure: it is not retried.
    assert len(api.bodies) == 1


def test_the_token_is_never_logged(caplog: pytest.LogCaptureFixture) -> None:
    web = _FakeWebClient(
        RuntimeError("slack unreachable"),
        _auth_test(ok=False, error="invalid_auth"),
        _home_auth_test(),
    )
    with _reports_api() as api:
        api.statuses = [500, 200]
        with caplog.at_level(logging.DEBUG):
            threads, _ = _start(api, [("default", web)])
            _join(threads)

    assert len(api.bodies) == 2
    assert caplog.records, "the reporter logged nothing at all"
    for record in caplog.records:
        assert BOT_TOKEN not in record.getMessage()
    assert BOT_TOKEN not in json.dumps(api.bodies)


# --- run.main ----------------------------------------------------------------------


def test_run_main_starts_the_reports_after_the_connections_and_stops_them_on_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    offline_installation_reports: InstallationReportStarts,
) -> None:
    """Driven through the real ``run.main``; only the boot gates, the heartbeat,
    signals and the supervisor group's run are faked. One identity, so it also
    pins that the reporter does not depend on preflight's auth.test."""

    from curie_dispatcher import run

    _set_run_env(monkeypatch)
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-default")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-default")
    order: list[str] = []
    stop_seen: dict[str, bool] = {}

    real_build_connections = run.build_identity_connections

    def recording_build_connections(*args: Any, **kwargs: Any) -> Any:
        connections = real_build_connections(*args, **kwargs)
        order.append("connections")
        return connections

    def recording_start(*args: Any, **kwargs: Any) -> list[threading.Thread]:
        order.append("reports")
        offline_installation_reports.calls.append((args, kwargs))
        stop_seen["at_start"] = kwargs["stop_event"].is_set()
        return []

    class Group:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            pass

        def run(self) -> None:
            order.append("run")
            ((_args, kwargs),) = offline_installation_reports.calls
            stop_seen["while_running"] = kwargs["stop_event"].is_set()

        def request_stop(self) -> None:
            pass

    monkeypatch.setattr(run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry())
    monkeypatch.setattr(run, "check_api_reachable", lambda *a, **k: None)
    monkeypatch.setattr(
        run,
        "check_slack_channel_capabilities",
        lambda *a, identities, **k: tuple(run.PreflightedIdentity(i, None) for i in identities),
    )
    monkeypatch.setattr(run, "build_identity_connections", recording_build_connections)
    monkeypatch.setattr(run, "SupervisorGroup", Group)
    monkeypatch.setattr(run, "start_installation_reports", recording_start)
    monkeypatch.setattr(run, "start_heartbeat", lambda *a, **k: threading.Event())
    monkeypatch.setattr(run.signal, "signal", lambda *a, **k: None)

    run.main()

    assert order == ["connections", "reports", "run"]
    ((args, kwargs),) = offline_installation_reports.calls
    identities = list(args[0]) if args else list(kwargs["identities"])
    assert [name for name, _ in identities] == ["default"]
    ((_, web_client),) = identities
    assert isinstance(web_client, WebClient)
    assert web_client.token == "xoxb-default"
    assert stop_seen == {"at_start": False, "while_running": False}
    assert kwargs["stop_event"].is_set(), "shutdown did not stop the reporter"
