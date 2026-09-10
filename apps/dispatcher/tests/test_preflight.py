"""Boot-time platform-API and Slack capability gates (#442, #2205).

The dispatcher resolves Slack approval clicks by calling the platform API. When
it is wired to the wrong place the only symptom today is a warning at click time
and a dead-ended button (``ApprovalResolveClient.resolve`` catches the
``httpx.HTTPError`` and returns ``ResolveOutcome(status_code=0)``). This gate
turns that silent misconfiguration into a loud boot failure naming the URL it
could not reach.

The gate is bounded-retry-then-fail, not fail-immediately: a single probe at t=0
races the API's own startup, and in k8s pod start order is not ordered at all, so
fail-immediately would crash-loop a healthy stack. Test 3 is the guard on that
decision.

Focused cases fake the API at the ``client=`` seam with
``httpx.MockTransport``. The entrypoint regressions use a real loopback HTTP
server and let the preflight construct its production client. Slack is faked
only at its provider client seam. In both forms the retry loops, deadline
decisions, parsing, and URL construction are real. Slow-path tests use the
injected monotonic/sleep seam so aggregate budgets are exact and the suite does
not wait on wall-clock time.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.preflight import (
    ApiUnreachableError,
    SlackChannelPreflightError,
    check_api_reachable,
    check_slack_channel_capabilities,
)
from slack_sdk.errors import SlackApiError
from slack_sdk.web import WebClient
from slack_sdk.web.slack_response import SlackResponse

from .conftest import _black_hole_api

API_URL = "http://curie-api:8000"
CHANNEL_A = "C0EXAMPLE1"
CHANNEL_B = "C0EXAMPLE2"
CHANNEL_C = "C0EXAMPLE3"
MISSING_SCOPE_MESSAGE = (
    "Slack channel capability preflight failed: bot token is missing required "
    "scope channels:read. Add channels:read under OAuth & Permissions > Bot "
    "Token Scopes, then reinstall the app to the workspace."
)
MISSING_FILES_READ_MESSAGE = (
    "Slack channel capability preflight failed: bot token is missing required "
    "scope files:read. Add files:read under OAuth & Permissions > Bot Token "
    "Scopes, then reinstall the app to the workspace."
)
SLACK_TIMEOUT_MESSAGE = (
    "Slack channel capability preflight failed: could not attempt every "
    "configured destination within the bounded startup budget. Check Slack "
    "API availability and retry."
)
FILES_LIST_OK: dict[str, object] = {
    # Slack documents a successful `files.list` as `{"ok": true, "files": [...]}`
    # with `paging`; an empty array is a documented answer for a workspace that
    # holds no files, which is why the probe can never depend on one existing:
    # https://docs.slack.dev/reference/methods/files.list
    "ok": True,
    "files": [],
    "paging": {"count": 1, "total": 0, "page": 1, "pages": 0},
}


@contextmanager
def _loopback_health_server(
    statuses: Sequence[int],
    *,
    userinfo: str = "",
) -> Iterator[tuple[str, list[str]]]:
    """Run a real loopback HTTP endpoint with a scripted ``/health`` result.

    The entrypoint regressions below deliberately do not inject an httpx client:
    they drive ``run.main`` through ``DispatcherConfig`` and the preflight's
    production-owned client. Only the remote API's answers are scripted here.
    """
    if not statuses:
        raise ValueError("statuses must contain at least one response")

    requests: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            requests.append(self.path)
            status = statuses[min(len(requests) - 1, len(statuses) - 1)]
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"ok"}')

        def log_message(self, _format: str, *args: object) -> None:
            """Keep the test server out of the process's terminal log."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    authority = f"{userinfo}@" if userinfo else ""
    try:
        yield f"http://{authority}{host}:{port}", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def _configure_main_env(
    monkeypatch: pytest.MonkeyPatch,
    *,
    api_url: str,
    timeout_s: float,
) -> None:
    """Configure the real ``DispatcherConfig()`` used by ``run.main``."""
    for name, field in DispatcherConfig.model_fields.items():
        alias = field.validation_alias
        monkeypatch.delenv(
            alias if isinstance(alias, str) else name.upper(), raising=False
        )
    monkeypatch.setenv("CURIE_API_URL", api_url)
    monkeypatch.setenv("CURIE_API_PREFLIGHT_TIMEOUT_SECONDS", str(timeout_s))
    monkeypatch.setenv("CURIE_BACKOFF_INITIAL_SECONDS", "0.01")
    monkeypatch.setenv("CURIE_BACKOFF_MAX_SECONDS", "0.02")
    monkeypatch.setenv("CURIE_BACKOFF_MULTIPLIER", "2")
    monkeypatch.setenv("CURIE_API_KEY", "dispatcher-platform-test-key")
    monkeypatch.setenv(
        "CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "dispatcher-attester-test-secret"
    )
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-entrypoint-test")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-entrypoint-test")


def _config(**overrides: object) -> DispatcherConfig:
    """A config whose poll loop runs to its deadline in a fraction of a second.

    The backoff knobs are the real ``config.backoff_*`` settings the preflight
    reuses; shrinking them keeps the suite fast without patching time, so the
    loop exercised here is the shipped one.
    """
    defaults: dict[str, object] = {
        "api_base_url": API_URL,
        # This test is about the reachability probe, not principal issuance.
        # Supply the independent mandatory attester credential so a fail-closed
        # DispatcherConfig does not hide the probe behavior under test.
        "approval_chat_attester_secret": "dispatcher-attester-test-secret",
        "api_preflight_timeout_s": 0.2,
        "backoff_initial_seconds": 0.01,
        "backoff_max_seconds": 0.02,
        "backoff_multiplier": 2.0,
    }
    defaults.update(overrides)
    return DispatcherConfig(**defaults)  # type: ignore[arg-type]


def _client(handler) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _agent(
    *,
    channels: list[dict[str, str]],
    approval_routes: dict[str, dict[str, Any]] | None,
) -> dict[str, Any]:
    """A complete, public-placeholder ``AgentOut`` response fixture."""
    return {
        "id": "00000000-0000-0000-0000-000000000001",
        "name": "acme-bot",
        "channels": channels,
        "repo_full_name": None,
        "behavior_packs": None,
        "model": None,
        "thinking": None,
        "approval_required_tools": None,
        "approval_routes": approval_routes,
        "hook_partitions": None,
        "secrets": None,
        "memory": True,
        "created_at": "2026-01-01T00:00:00Z",
    }


class _RecordingSlackClient:
    """The only Slack fake: it records the real SDK method boundary."""

    def __init__(
        self,
        *,
        side_effect: Exception | None = None,
        list_side_effect: Exception | None = None,
        list_response: object | None = None,
        files_side_effect: Exception | None = None,
        files_response: object | None = None,
    ) -> None:
        self.channels: list[str] = []
        self.list_calls: list[dict[str, object]] = []
        self.files_calls: list[dict[str, object]] = []
        self.side_effect = side_effect
        self.list_side_effect = list_side_effect
        self.files_side_effect = files_side_effect
        self.files_response = (
            dict(FILES_LIST_OK) if files_response is None else files_response
        )
        self.list_response = (
            {
                "ok": True,
                "channels": [],
                "response_metadata": {"next_cursor": ""},
            }
            if list_response is None
            else list_response
        )

    def conversations_list(
        self,
        *,
        types: str,
        exclude_archived: bool,
        limit: int,
    ) -> object:
        self.list_calls.append(
            {
                "types": types,
                "exclude_archived": exclude_archived,
                "limit": limit,
            }
        )
        if self.list_side_effect is not None:
            raise self.list_side_effect
        return self.list_response

    def files_list(self, *, count: int, **unexpected: object) -> object:
        """The `files:read` probe boundary (#2567).

        `count` is declared required and keyword-only, and anything else is
        captured into `unexpected` and recorded, because the page-size argument
        this method takes is the load-bearing detail: `files.list` predates
        Slack's cursor pagination and pages with `count`/`page`, while
        `slack_sdk`'s `WebClient.files_list` types only `count` and would
        forward a `limit=` into its `**kwargs` -- sending an undocumented
        argument and quietly fetching the 100-file default page instead of one.
        https://docs.slack.dev/reference/methods/files.list
        """
        self.files_calls.append({"count": count, **unexpected})
        if self.files_side_effect is not None:
            raise self.files_side_effect
        return self.files_response

    def conversations_info(self, *, channel: str) -> Any:
        self.channels.append(channel)
        if self.side_effect is not None:
            raise self.side_effect
        return {"ok": True, "channel": {"id": channel}}


class _FakeClock:
    """Deterministic monotonic clock for bounded boot-preflight tests."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


class _AdvancingSlackClient(_RecordingSlackClient):
    """Successful Slack fake whose calls consume a deterministic budget."""

    def __init__(
        self,
        *,
        clock: _FakeClock,
        seconds_per_call: float,
        list_seconds: float = 0.0,
    ) -> None:
        super().__init__()
        self._clock = clock
        self._seconds_per_call = seconds_per_call
        self._list_seconds = list_seconds

    def conversations_list(
        self,
        *,
        types: str,
        exclude_archived: bool,
        limit: int,
    ) -> object:
        response = super().conversations_list(
            types=types, exclude_archived=exclude_archived, limit=limit
        )
        self._clock.sleep(self._list_seconds)
        return response

    def conversations_info(self, *, channel: str) -> dict[str, Any]:
        response = super().conversations_info(channel=channel)
        self._clock.sleep(self._seconds_per_call)
        return response


class _StaticSlackClient(_RecordingSlackClient):
    """Slack fake for malformed non-exception response shapes."""

    def __init__(self, response: object) -> None:
        super().__init__()
        self._response = response

    def conversations_info(self, *, channel: str) -> object:
        self.channels.append(channel)
        return self._response


def _refusing(request: httpx.Request) -> httpx.Response:
    """A fake handler that always refuses the connection."""
    raise httpx.ConnectError("connection refused", request=request)


def _ok(request: httpx.Request) -> httpx.Response:
    """A fake handler that always answers a healthy 200."""
    return httpx.Response(200, json={"status": "ok"})


def test_healthy_api_returns_and_logs_the_url(caplog: pytest.LogCaptureFixture) -> None:
    """A reachable API returns cleanly and records where it looked.

    The happy path. A gutted implementation (a bare `return`) also passes this
    one, which is why it is not the contract -- tests 2 and 3 are.
    """
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        return httpx.Response(200, json={"status": "ok"})

    logger = logging.getLogger("test-preflight-healthy")
    with caplog.at_level(logging.INFO, logger="test-preflight-healthy"):
        check_api_reachable(_config(), logger=logger, client=_client(handler))

    assert requested == [f"{API_URL}/health"]
    assert any(API_URL in record.getMessage() for record in caplog.records), (
        "the preflight logged nothing naming the API URL it reached; the "
        "resolved wiring must be visible in the boot logs"
    )


def test_unreachable_api_raises_naming_the_url() -> None:
    """AC2: an unreachable API fails loudly, naming the URL that was tried.

    This is the whole point of the gate. Deleting the implementation fails this.
    Naming the URL is the load-bearing part: "cannot reach the API" without the
    resolved address does not tell an operator that it is pointed at itself.
    """

    with pytest.raises(ApiUnreachableError) as excinfo:
        check_api_reachable(
            _config(api_preflight_timeout_s=0.05),
            logger=logging.getLogger("test-preflight"),
            client=_client(_refusing),
        )

    assert API_URL in str(excinfo.value), (
        f"the error must name the configured api_base_url; got {str(excinfo.value)!r}"
    )


def test_transient_failure_then_success_does_not_raise() -> None:
    """Bounded RETRY, not fail-immediately: a slow-starting API is not a failure.

    The crash-loop guard, and the test that pins the design decision. A naive
    single-probe implementation fails here: in compose and k8s the API is
    routinely not yet accepting connections when the dispatcher boots.
    """
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) < 3:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"status": "ok"})

    check_api_reachable(
        _config(), logger=logging.getLogger("test-preflight"), client=_client(handler)
    )

    assert len(attempts) == 3, (
        f"expected the preflight to keep polling until the API answered "
        f"(3 attempts); it made {len(attempts)}"
    )


def test_non_200_health_is_treated_as_unreachable() -> None:
    """A responding-but-unhealthy endpoint is not "reachable".

    Guards against ignoring status_code. A 404 here is the realistic case: the
    URL points at some other service that answers HTTP but is not the API.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    with pytest.raises(ApiUnreachableError):
        check_api_reachable(
            _config(api_preflight_timeout_s=0.05),
            logger=logging.getLogger("test-preflight"),
            client=_client(handler),
        )


def test_health_url_tolerates_a_trailing_slash() -> None:
    """A base URL with a trailing slash must not yield `//health`.

    ``ApprovalResolveClient`` already rstrips its base; the preflight builds its
    own URL and must do the same or a perfectly reasonable
    `http://curie-api:8000/` fails the gate on a correctly wired stack.
    """
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        return httpx.Response(200, json={"status": "ok"})

    check_api_reachable(
        _config(api_base_url=f"{API_URL}/"),
        logger=logging.getLogger("test-preflight"),
        client=_client(handler),
    )

    assert requested == ["/health"]


def test_the_loop_spends_its_whole_budget_with_production_backoff_ratios() -> None:
    """The gate must keep polling until its deadline and report what it spent.

    This uses the production 120-second chart budget and reconnect ratios on an
    injected clock. Without the preflight-specific poll ceiling, the loop can
    spend the end of its budget asleep, so an API becoming healthy late in the
    advertised readiness window is never probed.
    """
    config = _config(
        api_preflight_timeout_s=120.0,
        backoff_initial_seconds=1.0,
        backoff_max_seconds=30.0,
        backoff_multiplier=2.0,
    )
    clock = _FakeClock()
    request_started_at: list[float] = []

    def refusing(request: httpx.Request) -> httpx.Response:
        request_started_at.append(clock.monotonic())
        raise httpx.ConnectError("connection refused", request=request)

    with pytest.raises(ApiUnreachableError) as excinfo:
        check_api_reachable(
            config,
            logger=logging.getLogger("test-preflight"),
            client=_client(refusing),
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    last_probe_elapsed = request_started_at[-1]
    assert last_probe_elapsed >= config.api_preflight_timeout_s * 0.9, (
        f"the last probe started only {last_probe_elapsed:.1f}s into the "
        f"{config.api_preflight_timeout_s:.0f}s budget; the loop slept through "
        "the final readiness window instead of polling it"
    )
    assert clock.monotonic() == config.api_preflight_timeout_s, (
        f"the preflight gave up after {clock.monotonic():.1f}s of its 120s budget; the "
        f"delay must be clamped to the remaining time, not abandoned when it "
        f"would overshoot"
    )
    # The terminal message must never claim time the loop did not really spend.
    assert "after 120.0s" in str(excinfo.value), (
        f"the error must report the time actually elapsed, not the configured "
        f"deadline; got {str(excinfo.value)!r}"
    )


def test_the_loop_does_not_probe_past_its_deadline() -> None:
    """The upper bound: the gate must spend its budget and then stop.

    The sibling test above pins the lower bound (never abandon the budget early).
    This pins the other side, which that fix left open: the loop could still
    enter a probe with little or no budget left, and the probe's own timeout then
    ran past the deadline -- measured at 0.249s elapsed against a 0.2s deadline,
    with the error still claiming "after 0.2s". A boot gate that overshoots its
    configured deadline delays the CrashLoopBackOff signal it exists to produce.

    The injected client carries the 5.0s default the preflight builds for itself,
    so an unbounded probe against a black hole burns 5s of a 0.3s budget.
    """
    with _black_hole_api() as url:
        config = _config(api_base_url=url, api_preflight_timeout_s=0.3)
        start = time.monotonic()
        with pytest.raises(ApiUnreachableError) as excinfo:
            check_api_reachable(
                config,
                logger=logging.getLogger("test-preflight"),
                client=httpx.Client(timeout=5.0),
            )
        elapsed = time.monotonic() - start

    assert elapsed < 0.6, (
        f"the preflight took {elapsed:.3f}s against a 0.3s deadline; each probe "
        f"must be bounded by the time remaining, not by the probe timeout"
    )
    assert elapsed >= 0.27, (
        f"the preflight gave up after {elapsed:.3f}s of its 0.3s budget; bounding "
        f"the probe must not turn into abandoning the deadline early"
    )
    assert "after 0.3s" in str(excinfo.value), (
        f"the error must report the time actually elapsed; got {str(excinfo.value)!r}"
    )


def test_userinfo_in_the_base_url_is_kept_out_of_logs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A credential-bearing base URL must not write its password to the logs.

    AC2 requires naming the resolved URL, so stripping userinfo beats dropping
    the URL. `httpx` accepts `http://user:pass@host`, so a BYO `apiBaseUrl` would
    otherwise put `pass` in the pod logs and every shipper downstream.
    """

    logger = logging.getLogger("test-preflight-userinfo")
    with caplog.at_level(logging.INFO, logger="test-preflight-userinfo"):
        check_api_reachable(
            _config(api_base_url="http://user:hunter2@curie-api:8000"),
            logger=logger,
            client=_client(_ok),
        )

    logged = " ".join(record.getMessage() for record in caplog.records)
    assert "hunter2" not in logged and "user" not in logged, (
        f"the preflight logged the URL's userinfo: {logged!r}"
    )
    assert "curie-api:8000" in logged, (
        f"AC2 still requires the log to name the resolved URL; got {logged!r}"
    )


def test_userinfo_is_stripped_from_the_error_too() -> None:
    """The failure path names the URL as well, so it needs the same scrub."""

    with pytest.raises(ApiUnreachableError) as excinfo:
        check_api_reachable(
            _config(
                api_base_url="http://user:hunter2@curie-api:8000",
                api_preflight_timeout_s=0.05,
            ),
            logger=logging.getLogger("test-preflight"),
            client=_client(_refusing),
        )

    assert "hunter2" not in str(excinfo.value)
    assert "curie-api:8000" in str(excinfo.value)


def test_slack_preflight_discovers_all_destinations_deduplicates_and_logs_count(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Every display-safe Slack destination is checked exactly once.

    Slack documents ``channel`` as the required conversation ID argument and
    ``{\"ok\": true, \"channel\": {...}}`` as the success response:
    https://docs.slack.dev/reference/methods/conversations.info/
    """
    api_key = "fake-platform-key-sentinel"
    config = _config(api_key=api_key)
    agent = _agent(
        channels=[
            {"kind": "slack", "address": CHANNEL_A},
            {"kind": "mail", "address": "mail-room-example"},
        ],
        approval_routes={
            "security": {
                "resolution": {"kind": "slack", "address": CHANNEL_B},
                "notification": {"kind": "slack", "address": CHANNEL_C},
            },
            "operations": {
                # Repeat the ordinary binding to prove the call is de-duplicated
                # across AgentOut.channels and every approval route target.
                "resolution": {"kind": "slack", "address": CHANNEL_A},
                "notification": {
                    "kind": "mail",
                    "address": "mail-approval-example",
                },
            },
        },
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[agent])

    slack = _RecordingSlackClient()
    logger = logging.getLogger("test-slack-preflight-success")
    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            config,
            logger=logger,
            web_client=slack,
            api_client=_client(handler),
        )

    assert len(requests) == 1
    assert requests[0].method == "GET"
    assert str(requests[0].url) == f"{API_URL}/agents"
    assert requests[0].headers["X-API-Key"] == api_key
    assert len(slack.channels) == 3
    assert sorted(slack.channels) == sorted([CHANNEL_A, CHANNEL_B, CHANNEL_C])
    assert slack.list_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]

    messages = [record.getMessage() for record in caplog.records]
    assert messages == [
        "Slack channel capability preflight public-channel capability verified; "
        "attachment download capability verified; "
        "checked 3 configured destinations; unverified 0"
    ]
    logged = " ".join(messages)
    for private_value in (api_key, CHANNEL_A, CHANNEL_B, CHANNEL_C):
        assert private_value not in logged, (
            f"success logs must contain only an aggregate count, not {private_value!r}"
        )


def test_slack_preflight_empty_destination_set_still_checks_public_capability(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unbound Slack surface still proves the workspace-level public scope."""
    agent = _agent(
        channels=[{"kind": "mail", "address": "mail-room-example"}],
        approval_routes=None,
    )
    slack = _RecordingSlackClient()
    logger = logging.getLogger("test-slack-preflight-empty")
    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            _config(),
            logger=logger,
            web_client=slack,
            api_client=_client(lambda _request: httpx.Response(200, json=[agent])),
        )

    assert slack.list_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]
    assert slack.channels == []
    assert [record.getMessage() for record in caplog.records] == [
        "Slack channel capability preflight public-channel capability verified; "
        "attachment download capability verified; "
        "checked 0 configured destinations; unverified 0"
    ]


def test_discovery_ignores_malformed_non_slack_target_before_reading_address() -> None:
    """Only Slack targets make the Slack address field part of this gate."""
    agent = _agent(
        channels=[
            {"kind": "mail"},
            {"kind": "slack", "address": CHANNEL_A},
        ],
        approval_routes=None,
    )
    slack = _RecordingSlackClient()

    check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-non-slack-malformed-target"),
        web_client=slack,
        api_client=_client(lambda _request: httpx.Response(200, json=[agent])),
    )

    assert slack.list_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]
    assert slack.channels == [CHANNEL_A]


def test_discovery_refuses_malformed_slack_target_without_leaking(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A Slack target still requires a nonempty string destination address."""
    agent = _agent(
        channels=[
            {"kind": "slack", "private": "hostile-target-field-sentinel"},
        ],
        approval_routes=None,
    )
    slack = _RecordingSlackClient()
    logger = logging.getLogger("test-slack-malformed-target")

    with caplog.at_level(logging.INFO, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                _config(),
                logger=logger,
                web_client=slack,
                api_client=_client(
                    lambda _request: httpx.Response(200, json=[agent])
                ),
            )

    assert "returned an invalid response shape" in str(excinfo.value)
    assert slack.list_calls == []
    assert slack.channels == []
    emitted = " ".join(
        [str(excinfo.value), *(record.getMessage() for record in caplog.records)]
    )
    assert "hostile-target-field-sentinel" not in emitted


def test_slack_preflight_retries_transient_agent_discovery_then_succeeds() -> None:
    """Authenticated discovery retries transient 5xx and connection failures.

    ``GET /health`` can answer before the API's database-backed ``GET /agents``
    is ready. The latter therefore needs the same bounded startup tolerance.
    """
    api_key = "fake-platform-key-sentinel"
    attempts: list[httpx.Request] = []
    clock = _FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(request)
        if len(attempts) == 1:
            return httpx.Response(503, text="transient-body-sentinel")
        if len(attempts) == 2:
            raise httpx.ConnectError("transient-connect-sentinel", request=request)
        return httpx.Response(200, json=[])

    check_slack_channel_capabilities(
        _config(
            api_key=api_key,
            api_preflight_timeout_s=1.0,
            backoff_initial_seconds=0.1,
            backoff_max_seconds=0.2,
        ),
        logger=logging.getLogger("test-slack-preflight-discovery-retry"),
        web_client=_RecordingSlackClient(),
        api_client=_client(handler),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    assert len(attempts) == 3
    assert all(request.headers["X-API-Key"] == api_key for request in attempts)
    assert clock.now == pytest.approx(0.3)


def test_slack_preflight_api_discovery_failure_is_redaction_safe(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Exhausted discovery fails closed without relaying bodies or credentials."""
    api_key = "hostile-api-key-sentinel"
    raw_body = "hostile-api-response-body-sentinel"
    logger = logging.getLogger("test-slack-preflight-api-failure")
    clock = _FakeClock()

    with caplog.at_level(logging.INFO, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                _config(
                    api_key=api_key,
                    api_preflight_timeout_s=0.2,
                    backoff_initial_seconds=0.1,
                    backoff_max_seconds=0.1,
                ),
                logger=logger,
                web_client=_RecordingSlackClient(),
                api_client=_client(
                    lambda _request: httpx.Response(503, text=raw_body)
                ),
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

    emitted = " ".join(
        [str(excinfo.value), *(record.getMessage() for record in caplog.records)]
    )
    assert "Slack channel capability preflight failed" in emitted
    assert api_key not in emitted
    assert raw_body not in emitted


@pytest.mark.parametrize(
    ("provider_failure", "private_values"),
    [
        pytest.param(
            SlackApiError(
                "hostile-info-scope-sdk-message-sentinel",
                {
                    "ok": False,
                    "error": "missing_scope",
                    "needed": "channels:read",
                    "provided": "groups:read",
                    "detail": "hostile-info-scope-body-sentinel",
                },
            ),
            (
                "channels:read",
                "groups:read",
                "hostile-info-scope-sdk-message-sentinel",
                "hostile-info-scope-body-sentinel",
            ),
            id="destination-public-scope-is-not-global-proof",
        ),
        pytest.param(
            SlackApiError(
                "hostile-groups-sdk-message-sentinel",
                {
                    "ok": False,
                    "error": "missing_scope",
                    "needed": "groups:read",
                    "provided": "chat:write",
                    "detail": "hostile-groups-body-sentinel",
                },
            ),
            (
                "groups:read",
                "chat:write",
                "hostile-groups-sdk-message-sentinel",
                "hostile-groups-body-sentinel",
            ),
            id="private-channel-scope",
        ),
        pytest.param(
            SlackApiError(
                "hostile-stale-sdk-message-sentinel",
                {
                    "ok": False,
                    "error": "channel_not_found",
                    "detail": "hostile-stale-body-sentinel",
                },
            ),
            (
                "channel_not_found",
                "hostile-stale-sdk-message-sentinel",
                "hostile-stale-body-sentinel",
            ),
            id="stale-binding",
        ),
        pytest.param(
            SlackApiError(
                "hostile-rate-sdk-message-sentinel",
                {
                    "ok": False,
                    "error": "ratelimited",
                    "retry_after": "hostile-retry-after-sentinel",
                },
            ),
            (
                "ratelimited",
                "hostile-rate-sdk-message-sentinel",
                "hostile-retry-after-sentinel",
            ),
            id="rate-limit",
        ),
        pytest.param(
            RuntimeError("hostile-transport-error-sentinel"),
            ("hostile-transport-error-sentinel",),
            id="transport-error",
        ),
        pytest.param(
            SlackApiError(
                "hostile-sdk-message-sentinel",
                {
                    "ok": False,
                    "error": "hostile-sdk-error-sentinel",
                    "detail": "hostile-sdk-body-sentinel",
                },
            ),
            (
                "hostile-sdk-message-sentinel",
                "hostile-sdk-error-sentinel",
                "hostile-sdk-body-sentinel",
            ),
            id="generic-sdk-error",
        ),
    ],
)
def test_slack_preflight_nondefinitive_failures_warn_aggregately_and_continue(
    provider_failure: Exception,
    private_values: tuple[str, ...],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Private, stale, rate-limited, and transport outcomes are not boot-fatal.

    Slack documents ``missing_scope``, ``channel_not_found``, and
    ``ratelimited`` for ``conversations.info``. A missing-scope response that
    does not include public-channel ``channels:read`` remains nondefinitive:
    https://docs.slack.dev/reference/methods/conversations.info/#errors
    """
    logger = logging.getLogger("test-slack-preflight-nondefinitive-failure")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        check_slack_channel_capabilities(
            _config(slack_bot_token="hostile-bot-token-sentinel"),
            logger=logger,
            web_client=_RecordingSlackClient(side_effect=provider_failure),
            api_client=_client(
                lambda _request: httpx.Response(
                    200,
                    json=[
                        _agent(
                            channels=[{"kind": "slack", "address": CHANNEL_A}],
                            approval_routes=None,
                        )
                    ],
                )
            ),
        )

    messages = [record.getMessage() for record in caplog.records]
    assert messages == [
        "Slack channel capability preflight public-channel capability verified; "
        "attachment download capability verified; "
        "checked 0 configured destinations; unverified 1"
    ]
    logged = " ".join(messages)
    for private_value in (
        "hostile-bot-token-sentinel",
        CHANNEL_A,
        *private_values,
    ):
        assert private_value not in logged


def test_malformed_falsey_conversations_list_response_is_unverified_and_redacted(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A falsey provider mapping cannot be replaced by the fake's happy default."""

    class FalseySlackResponse(dict[str, object]):
        def __bool__(self) -> bool:
            return False

    provider_response = FalseySlackResponse(
        ok=True,
        channels="hostile-provider-channels-sentinel",
        detail="hostile-provider-body-sentinel",
    )
    slack = _RecordingSlackClient(list_response=provider_response)
    logger = logging.getLogger("test-slack-malformed-capability-response")
    agent = _agent(
        channels=[{"kind": "slack", "address": CHANNEL_A}],
        approval_routes=None,
    )

    with caplog.at_level(logging.WARNING, logger=logger.name):
        check_slack_channel_capabilities(
            _config(slack_bot_token="hostile-bot-token-sentinel"),
            logger=logger,
            web_client=slack,
            api_client=_client(lambda _request: httpx.Response(200, json=[agent])),
        )

    assert slack.list_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]
    assert slack.channels == [CHANNEL_A]
    messages = [record.getMessage() for record in caplog.records]
    assert messages == [
        "Slack channel capability preflight public-channel capability unverified; "
        "attachment download capability verified; "
        "checked 1 configured destinations; unverified 0"
    ]
    logged = " ".join(messages)
    for private_value in (
        "hostile-bot-token-sentinel",
        "hostile-provider-channels-sentinel",
        "hostile-provider-body-sentinel",
        CHANNEL_A,
    ):
        assert private_value not in logged


@pytest.mark.parametrize(
    ("provider_response", "private_values"),
    [
        pytest.param({"ok": False}, (), id="ok-false"),
        pytest.param(
            {
                "ok": True,
                "error": "hostile-missing-channel-error-sentinel",
                "detail": "hostile-missing-channel-body-sentinel",
            },
            (
                "hostile-missing-channel-error-sentinel",
                "hostile-missing-channel-body-sentinel",
            ),
            id="missing-channel",
        ),
        pytest.param(
            {
                "ok": True,
                "channel": "hostile-nonmapping-channel-sentinel",
                "detail": "hostile-nonmapping-body-sentinel",
            },
            (
                "hostile-nonmapping-channel-sentinel",
                "hostile-nonmapping-body-sentinel",
            ),
            id="nonmapping-channel",
        ),
    ],
)
def test_slack_preflight_malformed_success_is_nonfatal_unverified_and_redacted(
    provider_response: object,
    private_values: tuple[str, ...],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Malformed success shapes are ambiguous and cannot make startup terminal.

    Slack documents a successful ``conversations.info`` response as
    ``{"ok": true, "channel": {...}}``. Any other non-exception shape is
    unverified, and its provider-controlled fields stay out of logs:
    https://docs.slack.dev/reference/methods/conversations.info/
    """
    logger = logging.getLogger("test-slack-preflight-malformed-response")
    slack = _StaticSlackClient(provider_response)
    with caplog.at_level(logging.WARNING, logger=logger.name):
        check_slack_channel_capabilities(
            _config(slack_bot_token="hostile-bot-token-sentinel"),
            logger=logger,
            web_client=slack,
            api_client=_client(
                lambda _request: httpx.Response(
                    200,
                    json=[
                        _agent(
                            channels=[{"kind": "slack", "address": CHANNEL_A}],
                            approval_routes=None,
                        )
                    ],
                )
            ),
        )

    assert slack.channels == [CHANNEL_A]
    messages = [record.getMessage() for record in caplog.records]
    assert messages == [
        "Slack channel capability preflight public-channel capability verified; "
        "attachment download capability verified; "
        "checked 0 configured destinations; unverified 1"
    ]
    logged = " ".join(messages)
    for private_value in (
        "hostile-bot-token-sentinel",
        CHANNEL_A,
        *private_values,
    ):
        assert private_value not in logged


def test_slack_preflight_stops_at_aggregate_deadline_and_logs_safe_counts(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unattempted destinations make aggregate-budget exhaustion terminal."""
    clock = _FakeClock()
    slack = _AdvancingSlackClient(clock=clock, seconds_per_call=1.0)
    logger = logging.getLogger("test-slack-preflight-aggregate-budget")
    agent = _agent(
        channels=[
            {"kind": "slack", "address": CHANNEL_A},
            {"kind": "slack", "address": CHANNEL_B},
            {"kind": "slack", "address": CHANNEL_C},
        ],
        approval_routes=None,
    )

    with caplog.at_level(logging.WARNING, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                _config(
                    slack_bot_token="hostile-bot-token-sentinel",
                    api_preflight_timeout_s=2.0,
                ),
                logger=logger,
                web_client=slack,
                api_client=_client(
                    lambda _request: httpx.Response(
                        200,
                        json=[agent],
                        headers={"X-Provider-Detail": "hostile-provider-sentinel"},
                    )
                ),
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

    assert slack.channels == [CHANNEL_A, CHANNEL_B]
    assert clock.now == pytest.approx(2.0)
    assert str(excinfo.value) == SLACK_TIMEOUT_MESSAGE
    emitted = " ".join(
        [str(excinfo.value), *(record.getMessage() for record in caplog.records)]
    )
    for private_value in (
        "hostile-bot-token-sentinel",
        "hostile-provider-sentinel",
        CHANNEL_A,
        CHANNEL_B,
        CHANNEL_C,
    ):
        assert private_value not in emitted


def test_health_and_slack_each_receive_the_full_configured_budget(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Health has its own budget; discovery plus Slack gets a fresh budget."""
    clock = _FakeClock()
    config = _config(api_preflight_timeout_s=2.0)

    def health_handler(_request: httpx.Request) -> httpx.Response:
        clock.sleep(2.0)
        return httpx.Response(200, json={"status": "ok"})

    check_api_reachable(
        config,
        logger=logging.getLogger("test-separate-budget-health"),
        client=_client(health_handler),
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    )

    agent = _agent(
        channels=[
            {"kind": "slack", "address": CHANNEL_A},
            {"kind": "slack", "address": CHANNEL_B},
            {"kind": "slack", "address": CHANNEL_C},
        ],
        approval_routes=None,
    )

    def discovery_handler(_request: httpx.Request) -> httpx.Response:
        clock.sleep(1.0)
        return httpx.Response(200, json=[agent])

    slack = _AdvancingSlackClient(clock=clock, seconds_per_call=1.0)
    logger = logging.getLogger("test-separate-budget-slack")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                config,
                logger=logger,
                web_client=slack,
                api_client=_client(discovery_handler),
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

    # Health consumed its full 2s before the Slack preflight started. Discovery
    # plus metadata still received another full 2s, ending at t=4 rather than
    # inheriting health's exhausted deadline at t=2.
    assert clock.now == pytest.approx(4.0)
    assert slack.channels == [CHANNEL_A]
    assert str(excinfo.value) == SLACK_TIMEOUT_MESSAGE
    emitted = " ".join(
        [str(excinfo.value), *(record.getMessage() for record in caplog.records)]
    )
    for private_value in (CHANNEL_A, CHANNEL_B, CHANNEL_C):
        assert private_value not in emitted


def test_production_slack_clients_are_per_destination_bounded_and_nonretrying(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Production probes cannot hide an unattempted destination behind retries."""
    from curie_dispatcher import preflight

    clock = _FakeClock()
    constructor_calls: list[dict[str, Any]] = []
    attempted: list[str] = []
    capability_calls: list[dict[str, object]] = []
    call_durations = iter([1.2, 1.5, 1.0])

    class RecordingProductionClient:
        def conversations_list(
            self,
            *,
            types: str,
            exclude_archived: bool,
            limit: int,
        ) -> dict[str, Any]:
            capability_calls.append(
                {
                    "types": types,
                    "exclude_archived": exclude_archived,
                    "limit": limit,
                }
            )
            clock.sleep(next(call_durations))
            return {
                "ok": True,
                "channels": [],
                "response_metadata": {"next_cursor": ""},
            }

        def conversations_info(self, *, channel: str) -> dict[str, Any]:
            attempted.append(channel)
            clock.sleep(next(call_durations))
            return {"ok": True, "channel": {"id": channel}}

    def construct_web_client(**kwargs: Any) -> RecordingProductionClient:
        constructor_calls.append(kwargs)
        return RecordingProductionClient()

    monkeypatch.setattr(preflight, "WebClient", construct_web_client)
    agent = _agent(
        channels=[
            {"kind": "slack", "address": CHANNEL_A},
            {"kind": "slack", "address": CHANNEL_B},
            {"kind": "slack", "address": CHANNEL_C},
        ],
        approval_routes=None,
    )

    def discovery_handler(_request: httpx.Request) -> httpx.Response:
        clock.sleep(0.4)
        return httpx.Response(
            200,
            json=[agent],
            headers={"X-Provider-Detail": "hostile-provider-sentinel"},
        )

    logger = logging.getLogger("test-production-slack-client-budget")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                _config(
                    slack_bot_token="hostile-bot-token-sentinel",
                    api_preflight_timeout_s=4.0,
                ),
                logger=logger,
                api_client=_client(discovery_handler),
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

    assert capability_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]
    assert attempted == [CHANNEL_A, CHANNEL_B]
    assert len(constructor_calls) == 3
    assert [call["timeout"] for call in constructor_calls] == [2, 2, 1]
    assert all(isinstance(call["timeout"], int) for call in constructor_calls)
    assert all(call["retry_handlers"] == [] for call in constructor_calls)
    assert all(isinstance(call["logger"], logging.Logger) for call in constructor_calls)
    assert all(not call["logger"].propagate for call in constructor_calls)
    assert all(
        not call["logger"].isEnabledFor(logging.CRITICAL)
        for call in constructor_calls
    )
    assert all(
        any(isinstance(handler, logging.NullHandler) for handler in call["logger"].handlers)
        for call in constructor_calls
    )
    assert all(
        call["token"] == "hostile-bot-token-sentinel"
        for call in constructor_calls
    )
    assert str(excinfo.value) == SLACK_TIMEOUT_MESSAGE
    emitted = " ".join(
        [str(excinfo.value), *(record.getMessage() for record in caplog.records)]
    )
    for private_value in (
        "hostile-bot-token-sentinel",
        "hostile-provider-sentinel",
        CHANNEL_A,
        CHANNEL_B,
        CHANNEL_C,
    ):
        assert private_value not in emitted


def test_production_provider_error_uses_silent_sdk_logger_and_safe_aggregate(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """SDK diagnostics cannot bypass the preflight's redaction boundary."""
    from curie_dispatcher import preflight

    constructor_calls: list[dict[str, Any]] = []
    raw_message = "hostile-production-sdk-message-sentinel"
    raw_body = "hostile-production-sdk-body-sentinel"

    class FailingProductionClient:
        def __init__(self, logger: logging.Logger) -> None:
            self._logger = logger

        def conversations_list(
            self,
            *,
            types: str,
            exclude_archived: bool,
            limit: int,
        ) -> object:
            assert (types, exclude_archived, limit) == ("public_channel", True, 1)
            self._logger.debug("%s %s", CHANNEL_A, raw_body)
            self._logger.error("%s %s", raw_message, raw_body)
            raise SlackApiError(
                raw_message,
                {
                    "ok": False,
                    "error": "ratelimited",
                    "detail": raw_body,
                    "channel": CHANNEL_A,
                },
            )

        def conversations_info(self, *, channel: str) -> object:
            raise AssertionError(f"unexpected destination call for {channel}")

    def construct_web_client(**kwargs: Any) -> FailingProductionClient:
        constructor_calls.append(kwargs)
        return FailingProductionClient(kwargs["logger"])

    monkeypatch.setattr(preflight, "WebClient", construct_web_client)
    logger = logging.getLogger("test-production-provider-error")
    with caplog.at_level(logging.DEBUG):
        with caplog.at_level(logging.DEBUG, logger="slack_sdk.web.base_client"):
            check_slack_channel_capabilities(
                _config(slack_bot_token="hostile-bot-token-sentinel"),
                logger=logger,
                api_client=_client(lambda _request: httpx.Response(200, json=[])),
            )

    assert len(constructor_calls) == 1
    constructor = constructor_calls[0]
    assert constructor["retry_handlers"] == []
    assert isinstance(constructor["timeout"], int)
    silent_logger = constructor["logger"]
    assert isinstance(silent_logger, logging.Logger)
    assert silent_logger.propagate is False
    assert not silent_logger.isEnabledFor(logging.CRITICAL)
    assert any(
        isinstance(handler, logging.NullHandler)
        for handler in silent_logger.handlers
    )
    app_messages = [
        record.getMessage() for record in caplog.records if record.name == logger.name
    ]
    assert app_messages == [
        "Slack channel capability preflight public-channel capability unverified; "
        "attachment download capability unverified; "
        "checked 0 configured destinations; unverified 0"
    ]
    emitted = " ".join(record.getMessage() for record in caplog.records)
    for private_value in (
        "hostile-bot-token-sentinel",
        raw_message,
        raw_body,
        CHANNEL_A,
        "ratelimited",
    ):
        assert private_value not in emitted


@pytest.mark.parametrize(
    "error_code",
    [
        pytest.param("missing_scope", id="missing-scope"),
        pytest.param("invalid_types", id="types-rejected-by-granted-scopes"),
    ],
)
def test_slack_public_capability_scope_error_has_exact_redacted_recovery(
    error_code: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The workspace-level public-channel probe supplies precise recovery.

    Slack documents ``channels:read`` for public-channel metadata returned by
    ``conversations.list``. Its error table says ``missing_scope`` means the
    token lacks necessary scope and ``invalid_types`` may mean the requested
    type cannot be used with the token's granted permission scopes. Neither
    response needs a provider ``needed`` hint for this public-only probe:
    https://docs.slack.dev/reference/methods/conversations.list/#errors
    https://docs.slack.dev/reference/scopes/channels.read/
    """
    raw_message = "hostile-slack-sdk-message-sentinel"
    response = SlackResponse(
        client=None,
        http_verb="GET",
        api_url="https://slack.com/api/conversations.list",
        req_args={
            "params": {
                "types": "public_channel",
                "exclude_archived": True,
                "limit": 1,
            }
        },
        data={
            "ok": False,
            "error": error_code,
            "provided": "chat:write",
            "detail": "hostile-response-body-sentinel",
        },
        headers={"X-Slack-Req-Id": "hostile-request-id-sentinel"},
        status_code=200,
    )
    missing_scope = SlackApiError(raw_message, response)
    logger = logging.getLogger("test-slack-preflight-missing-scope")
    slack = _RecordingSlackClient(list_side_effect=missing_scope)

    with caplog.at_level(logging.INFO, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                _config(slack_bot_token="hostile-bot-token-sentinel"),
                logger=logger,
                web_client=slack,
                api_client=_client(
                    lambda _request: httpx.Response(
                        200,
                        json=[
                            _agent(
                                channels=[{"kind": "slack", "address": CHANNEL_A}],
                                approval_routes=None,
                            )
                        ],
                    )
                ),
            )

    assert str(excinfo.value) == MISSING_SCOPE_MESSAGE
    assert slack.list_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]
    assert slack.channels == []
    emitted = " ".join(
        [str(excinfo.value), *(record.getMessage() for record in caplog.records)]
    )
    for private_value in (
        "hostile-bot-token-sentinel",
        CHANNEL_A,
        raw_message,
        error_code,
        "chat:write",
        "hostile-response-body-sentinel",
        "hostile-request-id-sentinel",
    ):
        assert private_value not in emitted


def _one_slack_agent_api() -> httpx.Client:
    """The discovery seam answering with a single Slack-bound destination."""
    return _client(
        lambda _request: httpx.Response(
            200,
            json=[
                _agent(
                    channels=[{"kind": "slack", "address": CHANNEL_A}],
                    approval_routes=None,
                )
            ],
        )
    )


def test_files_read_recovery_message_is_the_shipped_constant() -> None:
    """Pin the recovery text to the module's own constant, not a paraphrase.

    The message is the whole product of this gate: an operator reads it out of
    a CrashLoopBackOff log and reinstalls. A test that only matched a substring
    would let the reinstall step be dropped, which is the one instruction a
    workspace with a stale bot-token grant actually needs.
    """
    from curie_dispatcher import preflight

    assert preflight._MISSING_FILES_READ_MESSAGE == MISSING_FILES_READ_MESSAGE
    # The two recoveries stay separate so each names exactly one scope; an
    # operator missing only files:read must not be handed a list to guess from.
    assert preflight._MISSING_CHANNELS_READ_MESSAGE == MISSING_SCOPE_MESSAGE
    assert MISSING_FILES_READ_MESSAGE != MISSING_SCOPE_MESSAGE
    assert "channels:read" not in MISSING_FILES_READ_MESSAGE


def test_slack_preflight_with_files_read_passes_and_logs_a_clean_summary(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AC: a token holding files:read boots, with no refusal and no warning.

    Slack documents `files.list` as requiring the `files:read` scope and
    answering `{"ok": true, "files": [...]}`; the probe names no file, so an
    empty array on a fresh workspace is a full pass:
    https://docs.slack.dev/reference/methods/files.list
    https://docs.slack.dev/reference/scopes/files.read/
    """
    slack = _RecordingSlackClient()
    logger = logging.getLogger("test-slack-preflight-files-read-verified")

    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            _config(),
            logger=logger,
            web_client=slack,
            api_client=_one_slack_agent_api(),
        )

    assert slack.files_calls == [{"count": 1}]
    assert slack.channels == [CHANNEL_A]
    records = [record for record in caplog.records if record.name == logger.name]
    assert [record.getMessage() for record in records] == [
        "Slack channel capability preflight public-channel capability verified; "
        "attachment download capability verified; "
        "checked 1 configured destinations; unverified 0"
    ]
    # INFO, not WARNING: a verified files:read probe must not raise the level of
    # the summary line on an otherwise clean stack, or the level stops meaning
    # anything for the nondefinitive case below.
    assert [record.levelno for record in records] == [logging.INFO]


def test_slack_preflight_missing_files_read_scope_has_exact_redacted_recovery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A definitive missing_scope on the files probe is boot-fatal and precise.

    Slack's `files.list` error table documents `missing_scope` for a token that
    lacks the necessary scope. It is unambiguous for this probe in a way it is
    not on `conversations.info`: the request carries no channel and no type
    filter, so `files:read` is the only scope it can be missing:
    https://docs.slack.dev/reference/methods/files.list/#errors
    """
    raw_message = "hostile-files-sdk-message-sentinel"
    response = SlackResponse(
        client=None,
        http_verb="GET",
        api_url="https://slack.com/api/files.list",
        req_args={"params": {"count": 1}},
        data={
            "ok": False,
            "error": "missing_scope",
            "provided": "chat:write",
            "detail": "hostile-files-response-body-sentinel",
        },
        headers={"X-Slack-Req-Id": "hostile-files-request-id-sentinel"},
        status_code=200,
    )
    slack = _RecordingSlackClient(
        files_side_effect=SlackApiError(raw_message, response)
    )
    logger = logging.getLogger("test-slack-preflight-files-missing-scope")

    with caplog.at_level(logging.INFO, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                _config(slack_bot_token="hostile-bot-token-sentinel"),
                logger=logger,
                web_client=slack,
                api_client=_one_slack_agent_api(),
            )

    assert str(excinfo.value) == MISSING_FILES_READ_MESSAGE
    # The refusal has to carry both halves an operator acts on: the scope name
    # and the fact that a reinstall is what refreshes an existing grant.
    assert "files:read" in str(excinfo.value)
    assert "reinstall the app to the workspace" in str(excinfo.value)
    # Definitive means terminal before the per-destination loop: a token that
    # cannot fetch an upload's bytes is not a stack worth wiring up.
    assert slack.files_calls == [{"count": 1}]
    assert slack.channels == []
    emitted = " ".join(
        [str(excinfo.value), *(record.getMessage() for record in caplog.records)]
    )
    for private_value in (
        "hostile-bot-token-sentinel",
        CHANNEL_A,
        raw_message,
        "missing_scope",
        "chat:write",
        "hostile-files-response-body-sentinel",
        "hostile-files-request-id-sentinel",
    ):
        assert private_value not in emitted


@pytest.mark.parametrize(
    ("files_side_effect", "files_response", "private_values"),
    [
        pytest.param(
            RuntimeError("hostile-files-transport-sentinel"),
            None,
            ("hostile-files-transport-sentinel",),
            id="transport-error",
        ),
        pytest.param(
            TimeoutError("hostile-files-timeout-sentinel"),
            None,
            ("hostile-files-timeout-sentinel",),
            id="timeout",
        ),
        pytest.param(
            SlackApiError(
                "hostile-files-rate-sdk-message-sentinel",
                {
                    "ok": False,
                    "error": "ratelimited",
                    "retry_after": "hostile-files-retry-after-sentinel",
                },
            ),
            None,
            (
                "hostile-files-rate-sdk-message-sentinel",
                "ratelimited",
                "hostile-files-retry-after-sentinel",
            ),
            id="unexpected-error-code",
        ),
        pytest.param(
            SlackApiError(
                "hostile-files-org-sdk-message-sentinel",
                {
                    "ok": False,
                    "error": "org_login_required",
                    "detail": "hostile-files-org-body-sentinel",
                },
            ),
            None,
            (
                "hostile-files-org-sdk-message-sentinel",
                "org_login_required",
                "hostile-files-org-body-sentinel",
            ),
            id="org-token-refusal",
        ),
        pytest.param(
            None,
            {
                "ok": True,
                "files": "hostile-files-nonarray-sentinel",
                "detail": "hostile-files-shape-body-sentinel",
            },
            (
                "hostile-files-nonarray-sentinel",
                "hostile-files-shape-body-sentinel",
            ),
            id="malformed-success-shape",
        ),
        pytest.param(None, {"ok": False}, (), id="ok-false"),
    ],
)
def test_nondefinitive_files_probe_warns_without_refusing_or_passing_silently(
    files_side_effect: Exception | None,
    files_response: object | None,
    private_values: tuple[str, ...],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Anything but missing_scope stays nondefinitive: warn, do not refuse.

    Read off the implementation rather than assumed: only `missing_scope`
    raises. Every other outcome sets the probe's status to `unverified`, which
    routes the summary line through `logger.warning` instead of `logger.info`
    *and* makes that line name the attachment capability as the degraded one.
    The destination counters stay unchanged, because they are about destinations
    and the destination was still probed.

    Warning rather than refusing is deliberate: refusing would let a rate limit
    or a transport blip crash-loop a stack whose other capabilities check out,
    which is exactly what the ambiguous per-destination outcomes are already
    forbidden from doing. But the raised level alone was not enough (#2567) --
    it said "one of these checks is off" while the text spoke only about the
    channels probe, so an operator debugging a dropped attachment found nothing
    about attachments in the line. The text now carries that answer, and this
    test pins the text, not just the level.

    Slack documents `ratelimited` and `org_login_required` among `files.list`
    errors; neither says anything definite about the token's scopes:
    https://docs.slack.dev/reference/methods/files.list/#errors
    """
    slack = _RecordingSlackClient(
        files_side_effect=files_side_effect,
        files_response=files_response,
    )
    logger = logging.getLogger("test-slack-preflight-files-nondefinitive")

    # No pytest.raises: a nondefinitive probe is not boot-fatal, so an
    # exception escaping here is the failure.
    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            _config(slack_bot_token="hostile-bot-token-sentinel"),
            logger=logger,
            web_client=slack,
            api_client=_one_slack_agent_api(),
        )

    records = [record for record in caplog.records if record.name == logger.name]
    # Not a silent pass: the one summary line is emitted at WARNING.
    assert [record.levelno for record in records] == [logging.WARNING]
    # ...and not a level-only signal either: the text names WHICH probe came
    # back degraded. The destination counters are unchanged -- they are about
    # destinations, and the destination was still probed rather than skipped.
    assert [record.getMessage() for record in records] == [
        "Slack channel capability preflight public-channel capability verified; "
        "attachment download capability unverified; "
        "checked 1 configured destinations; unverified 0"
    ]
    (summary,) = [record.getMessage() for record in records]
    assert "attachment download capability unverified" in summary, (
        "a degraded files:read probe must be NAMED in the summary text, not "
        "signalled only by the record's level (#2567)"
    )
    assert "attachment download capability verified" not in summary
    assert slack.channels == [CHANNEL_A]
    logged = " ".join(record.getMessage() for record in records)
    for private_value in (
        "hostile-bot-token-sentinel",
        CHANNEL_A,
        *private_values,
    ):
        assert private_value not in logged


def test_a_client_predating_the_files_probe_is_unverified_not_fatal(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A seam that has no `files.list` at all is nondefinitive, not a refusal.

    Every Slack fake in this suite predating #2567 has exactly this shape, and
    so does any out-of-tree caller holding the older `SlackChannelClient`
    protocol. The attribute lookup raises `AttributeError`, which the probe's
    broad `except Exception` must absorb into `unverified` -- the same treatment
    a transport fault gets -- rather than turning a stale double or an older
    caller into a boot failure. As with any other degraded files probe, the
    summary must SAY so rather than leaving the raised level to imply it
    (#2567): "attachment download capability unverified" is the only thing that
    distinguishes this boot from a clean one in an operator's log.
    """

    class _PreProbeSlackClient:
        """The pre-#2567 provider surface: no `files.list` on the object."""

        def __init__(self) -> None:
            self.channels: list[str] = []
            self.list_calls: list[dict[str, object]] = []

        def conversations_list(
            self,
            *,
            types: str,
            exclude_archived: bool,
            limit: int,
        ) -> object:
            self.list_calls.append(
                {
                    "types": types,
                    "exclude_archived": exclude_archived,
                    "limit": limit,
                }
            )
            return {"ok": True, "channels": []}

        def conversations_info(self, *, channel: str) -> object:
            self.channels.append(channel)
            return {"ok": True, "channel": {"id": channel}}

    slack = _PreProbeSlackClient()
    assert not hasattr(slack, "files_list")
    logger = logging.getLogger("test-slack-preflight-files-probe-absent")

    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            _config(),
            logger=logger,
            web_client=slack,
            api_client=_one_slack_agent_api(),
        )

    assert slack.channels == [CHANNEL_A]
    records = [record for record in caplog.records if record.name == logger.name]
    assert [record.getMessage() for record in records] == [
        "Slack channel capability preflight public-channel capability verified; "
        "attachment download capability unverified; "
        "checked 1 configured destinations; unverified 0"
    ]
    (summary,) = [record.getMessage() for record in records]
    assert "attachment download capability unverified" in summary, (
        "a seam with no files.list is a degraded probe, and the summary text "
        "must name it as one rather than only raising the level (#2567)"
    )
    assert "attachment download capability verified" not in summary
    assert [record.levelno for record in records] == [logging.WARNING]


def test_verified_and_degraded_files_probes_log_different_summary_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The differential guard: `files_status` must be VISIBLE in the summary.

    Every other assertion in this file pins one summary line at a time, so a
    change that dropped `files_status` back out of the format string would be
    one mechanical find-and-replace away from a green suite. This runs the same
    stack twice, varying ONLY the files probe's answer, and demands the two
    summary lines differ.

    That is the #2567 defect restated as a test. A message that stopped
    reporting the attachment capability -- or that formatted
    `capability_status` into both slots -- makes these two runs produce
    byte-identical text, distinguishable only by log level, and fails here.
    """

    def summary_record(
        slack: _RecordingSlackClient, logger_name: str
    ) -> logging.LogRecord:
        logger = logging.getLogger(logger_name)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=logger.name):
            check_slack_channel_capabilities(
                _config(),
                logger=logger,
                web_client=slack,
                api_client=_one_slack_agent_api(),
            )
        (record,) = [
            record for record in caplog.records if record.name == logger.name
        ]
        return record

    verified = summary_record(
        _RecordingSlackClient(),
        "test-files-probe-differential-verified",
    )
    # `ratelimited` is a documented, nondefinitive `files.list` error: it
    # degrades the probe without refusing boot, so both runs reach the summary.
    degraded = summary_record(
        _RecordingSlackClient(
            files_side_effect=SlackApiError(
                "hostile-files-differential-sdk-message-sentinel",
                {"ok": False, "error": "ratelimited"},
            )
        ),
        "test-files-probe-differential-degraded",
    )

    assert verified.getMessage() != degraded.getMessage(), (
        "a verified and a degraded files:read probe must produce different "
        "summary TEXT; identical text means the message has stopped reporting "
        "the attachment download capability (#2567)"
    )
    # The difference is exactly the attachment slot. The channels probe and both
    # destination counters were identical across the two runs, so substituting
    # that one status must turn one line into the other -- which also catches a
    # message that reported the files status into the public-channel slot.
    assert "attachment download capability verified" in verified.getMessage()
    assert "attachment download capability unverified" in degraded.getMessage()
    assert (
        verified.getMessage().replace(
            "attachment download capability verified",
            "attachment download capability unverified",
        )
        == degraded.getMessage()
    )
    # The level contract still holds, so this cannot be satisfied by text at the
    # cost of the signal the previous author established.
    assert (verified.levelno, degraded.levelno) == (logging.INFO, logging.WARNING)
    for private_value in (
        "hostile-files-differential-sdk-message-sentinel",
        "ratelimited",
        CHANNEL_A,
    ):
        assert private_value not in degraded.getMessage()


def test_unattempted_files_probe_refuses_rather_than_reading_as_a_pass() -> None:
    """A scope check the budget never reached is a refusal, not a pass.

    The distinction this pins: *nondefinitive* (the probe answered ambiguously)
    warns, while *unattempted* (the aggregate deadline expired before the probe
    could run) is terminal, per the module docstring. Without the deadline check
    between the two capability probes, an exhausted budget would fall through to
    the destination loop and an absent files:read check would read as clean.
    """
    clock = _FakeClock()
    slack = _AdvancingSlackClient(clock=clock, seconds_per_call=1.0, list_seconds=2.0)
    logger = logging.getLogger("test-slack-preflight-files-unattempted")

    with pytest.raises(SlackChannelPreflightError) as excinfo:
        check_slack_channel_capabilities(
            _config(api_preflight_timeout_s=2.0),
            logger=logger,
            web_client=slack,
            api_client=_one_slack_agent_api(),
            monotonic=clock.monotonic,
            sleep=clock.sleep,
        )

    assert str(excinfo.value) == SLACK_TIMEOUT_MESSAGE
    assert slack.list_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]
    assert slack.files_calls == []
    assert slack.channels == []


def test_production_files_probe_is_bounded_to_one_file_and_reuses_the_client(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The probe fetches ONE file, via `count`, on the capability client.

    This is the load-bearing argument-name test. `files.list` predates Slack's
    cursor pagination and pages with `count`/`page`, not `limit`, and
    `slack_sdk`'s `WebClient.files_list` types `count` as keyword-only while
    passing anything else through `**kwargs` straight into the request body. So
    a `limit=1` would not fail: it would send an argument the method does not
    document, `count` would fall back to Slack's default, and the boot gate
    would quietly pull a 100-file page on every dispatcher start. This asserts
    the recorded kwargs are exactly `{"count": 1}`, so reintroducing `limit`
    fails here whether it replaces `count` or rides alongside it:
    https://docs.slack.dev/reference/methods/files.list

    It runs at the production `WebClient` seam so the assertion is on the real
    call site, and pins that the files probe reuses the capability client rather
    than opening a second provider connection -- both are the same
    workspace-level question, already bounded by the same remaining budget.
    """
    from curie_dispatcher import preflight

    constructor_calls: list[dict[str, Any]] = []
    files_calls: list[dict[str, object]] = []
    capability_calls: list[dict[str, object]] = []

    class RecordingProductionClient:
        def conversations_list(
            self,
            *,
            types: str,
            exclude_archived: bool,
            limit: int,
        ) -> dict[str, Any]:
            capability_calls.append(
                {
                    "types": types,
                    "exclude_archived": exclude_archived,
                    "limit": limit,
                }
            )
            return {
                "ok": True,
                "channels": [],
                "response_metadata": {"next_cursor": ""},
            }

        def files_list(self, *, count: int, **unexpected: object) -> dict[str, Any]:
            files_calls.append({"count": count, **unexpected})
            return dict(FILES_LIST_OK)

        def conversations_info(self, *, channel: str) -> dict[str, Any]:
            raise AssertionError(f"unexpected destination call for {channel}")

    def construct_web_client(**kwargs: Any) -> RecordingProductionClient:
        constructor_calls.append(kwargs)
        return RecordingProductionClient()

    monkeypatch.setattr(preflight, "WebClient", construct_web_client)
    logger = logging.getLogger("test-production-files-probe-bounded")

    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            _config(slack_bot_token="hostile-bot-token-sentinel"),
            logger=logger,
            api_client=_client(lambda _request: httpx.Response(200, json=[])),
        )

    assert files_calls == [{"count": 1}]
    assert capability_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]
    # One provider client for both workspace-level scope questions: per-client
    # construction exists to stop one hung *destination* borrowing another's
    # share of the budget, not to scale with the number of scopes probed.
    assert len(constructor_calls) == 1
    assert constructor_calls[0]["retry_handlers"] == []
    assert isinstance(constructor_calls[0]["timeout"], int)
    records = [record for record in caplog.records if record.name == logger.name]
    assert [record.levelno for record in records] == [logging.INFO]
    assert "hostile-bot-token-sentinel" not in " ".join(
        record.getMessage() for record in records
    )


def test_files_probe_matches_the_slack_sdk_files_list_signature() -> None:
    """`count` is really the argument slack_sdk types; `limit` is really not.

    The fakes above are only as good as their fidelity to the SDK, so this
    checks the claim against the installed `slack_sdk` rather than against a
    comment. `count` is a declared keyword-only parameter of
    `WebClient.files_list`; `limit` is not declared at all and would be
    absorbed by the method's `**kwargs`, which is the silent-100-files failure
    the probe's `count=1` avoids.
    """
    import inspect

    parameters = inspect.signature(WebClient.files_list).parameters
    assert "count" in parameters
    assert parameters["count"].kind is inspect.Parameter.KEYWORD_ONLY
    assert "limit" not in parameters
    assert any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD
        for parameter in parameters.values()
    ), (
        "files_list no longer swallows unknown kwargs; the count-vs-limit trap "
        "this probe guards against may have changed shape"
    )


def test_new_files_probe_leaves_the_channels_read_gate_unchanged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Regression guard: adding the files probe did not move the first gate.

    A definitive `channels:read` refusal must still short-circuit before the
    files probe runs, and must still carry the channels:read recovery -- not the
    files:read one, and not both.
    """
    slack = _RecordingSlackClient(
        list_side_effect=SlackApiError(
            "hostile-channels-sdk-message-sentinel",
            {"ok": False, "error": "missing_scope", "needed": "channels:read"},
        )
    )
    logger = logging.getLogger("test-channels-read-gate-unchanged")

    with caplog.at_level(logging.INFO, logger=logger.name):
        with pytest.raises(SlackChannelPreflightError) as excinfo:
            check_slack_channel_capabilities(
                _config(),
                logger=logger,
                web_client=slack,
                api_client=_one_slack_agent_api(),
            )

    assert str(excinfo.value) == MISSING_SCOPE_MESSAGE
    assert "files:read" not in str(excinfo.value)
    assert slack.list_calls == [
        {"types": "public_channel", "exclude_archived": True, "limit": 1}
    ]
    assert slack.files_calls == []
    assert slack.channels == []


def test_channels_read_unverified_path_is_unchanged_by_a_verified_files_probe(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A nondefinitive channels:read outcome still only warns, files probe or not."""
    slack = _RecordingSlackClient(
        list_side_effect=SlackApiError(
            "hostile-channels-rate-sentinel",
            {"ok": False, "error": "ratelimited"},
        )
    )
    logger = logging.getLogger("test-channels-read-unverified-unchanged")

    with caplog.at_level(logging.INFO, logger=logger.name):
        check_slack_channel_capabilities(
            _config(),
            logger=logger,
            web_client=slack,
            api_client=_one_slack_agent_api(),
        )

    assert slack.files_calls == [{"count": 1}]
    records = [record for record in caplog.records if record.name == logger.name]
    assert [record.getMessage() for record in records] == [
        "Slack channel capability preflight public-channel capability unverified; "
        "attachment download capability verified; "
        "checked 1 configured destinations; unverified 0"
    ]
    assert [record.levelno for record in records] == [logging.WARNING]


def test_slack_manifest_declares_the_files_read_probe_scope() -> None:
    """The installable manifest must grant the scope the new boot gate probes.

    A gate that refuses on a scope the shipped manifest never asks for would
    fail every fresh install, so the manifest and the probe move together.
    """
    manifest = yaml.safe_load(
        (Path(__file__).parents[1] / "slack-app-manifest.yaml").read_text()
    )

    bot_scopes = manifest["oauth_config"]["scopes"]["bot"]
    assert "files:read" in bot_scopes
    assert "channels:read" in bot_scopes


def test_slack_manifest_declares_the_preflight_scope() -> None:
    """The installable manifest must grant what the boot gate exercises."""
    manifest = yaml.safe_load(
        (Path(__file__).parents[1] / "slack-app-manifest.yaml").read_text()
    )

    assert "channels:read" in manifest["oauth_config"]["scopes"]["bot"]


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
    def shutdown(self) -> None:
        pass


def test_run_main_gates_before_connecting_slack(monkeypatch: pytest.MonkeyPatch) -> None:
    """The ordering contract: the wiring gate precedes any Slack wiring.

    Driven through the real ``run.main()`` so it survives an internal rename of
    ``check_api_reachable``. ``build_supervisor`` -- the boundary that builds the
    Valkey client, the Slack Web client, and the Socket Mode connection factory --
    is replaced with a recorder; reaching it at all means the gate did not fire
    first. The API is a genuinely dead port, so nothing is stubbed on the path
    under test.
    """
    from curie_dispatcher import run

    reached_after_api: list[str] = []

    def _unexpected_slack_preflight(*args: object, **kwargs: object) -> None:
        reached_after_api.append("slack_preflight")
        raise AssertionError(
            "Slack capability preflight ran before API reachability passed"
        )

    def _recording_build_supervisor(*args: object, **kwargs: object) -> object:
        reached_after_api.append("build_supervisor")
        raise AssertionError(
            "build_supervisor was called with an unreachable API: the dispatcher "
            "wired up Slack before (or instead of) gating on the platform API"
        )

    monkeypatch.setattr(
        run, "check_slack_channel_capabilities", _unexpected_slack_preflight
    )
    monkeypatch.setattr(run, "build_supervisor", _recording_build_supervisor)
    _set_run_env(monkeypatch)
    # Port 1 is reserved and never listening: a real, immediate connection refusal.
    monkeypatch.setenv("CURIE_API_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("CURIE_API_PREFLIGHT_TIMEOUT_SECONDS", "0.2")
    monkeypatch.setenv("CURIE_BACKOFF_INITIAL_SECONDS", "0.01")
    monkeypatch.setenv("CURIE_BACKOFF_MAX_SECONDS", "0.02")

    with pytest.raises(SystemExit) as excinfo:
        run.main()

    assert excinfo.value.code not in (0, None), (
        f"the dispatcher must exit non-zero on an unreachable API so a "
        f"CrashLoopBackOff surfaces it; exited {excinfo.value.code!r}"
    )
    assert reached_after_api == []


def test_run_main_orders_api_then_slack_preflight_then_supervisor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful boot reaches Socket Mode only after both ordered gates."""
    from curie_dispatcher import run

    events: list[str] = []

    class RecordingSupervisor:
        def run(self) -> None:
            events.append("supervisor.run")

        def request_stop(self) -> None:
            events.append("supervisor.request_stop")

    class RecordingHeartbeat:
        def set(self) -> None:
            events.append("heartbeat.stop")

    def check_api(*args: object, **kwargs: object) -> None:
        assert "deadline" not in kwargs
        events.append("api")

    def check_slack(
        *args: object,
        **kwargs: object,
    ) -> None:
        assert "web_client" not in kwargs
        assert "deadline" not in kwargs
        events.append("slack")

    def build_supervisor(
        _config: DispatcherConfig,
        *,
        logger: logging.Logger,
    ) -> RecordingSupervisor:
        assert logger.name == "curie_dispatcher"
        events.append("supervisor")
        return RecordingSupervisor()

    _set_run_env(monkeypatch)
    monkeypatch.setattr(
        run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry()
    )
    monkeypatch.setattr(run, "check_api_reachable", check_api)
    monkeypatch.setattr(run, "check_slack_channel_capabilities", check_slack)
    monkeypatch.setattr(run, "build_supervisor", build_supervisor)
    monkeypatch.setattr(
        run, "start_heartbeat", lambda *a, **k: RecordingHeartbeat()
    )
    monkeypatch.setattr(run.signal, "signal", lambda *a, **k: None)

    run.main()

    assert events[:3] == ["api", "slack", "supervisor"]
    assert "supervisor.run" in events


def test_run_main_missing_scope_exits_before_supervisor_without_leaking(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The actionable Slack refusal is terminal before Socket Mode wiring."""
    from curie_dispatcher import run

    events: list[str] = []

    def check_api(*args: object, **kwargs: object) -> None:
        events.append("api")

    def refuse_slack(*args: object, **kwargs: object) -> None:
        assert "web_client" not in kwargs
        events.append("slack")
        raise SlackChannelPreflightError(MISSING_SCOPE_MESSAGE)

    def unexpected_supervisor(
        _config: DispatcherConfig,
        *,
        logger: logging.Logger,
    ) -> object:
        assert logger.name == "curie_dispatcher"
        events.append("supervisor")
        raise AssertionError("missing_scope reached supervisor/Socket Mode wiring")

    _set_run_env(monkeypatch)
    monkeypatch.setattr(
        run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry()
    )
    monkeypatch.setattr(run, "check_api_reachable", check_api)
    monkeypatch.setattr(run, "check_slack_channel_capabilities", refuse_slack)
    monkeypatch.setattr(run, "build_supervisor", unexpected_supervisor)

    with caplog.at_level(logging.ERROR, logger="curie_dispatcher"):
        with pytest.raises(SystemExit) as excinfo:
            run.main()

    assert excinfo.value.code not in (0, None)
    assert events == ["api", "slack"]
    assert [record.getMessage() for record in caplog.records] == [MISSING_SCOPE_MESSAGE]


def test_run_main_waits_for_delayed_api_then_starts_supervisor_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A normally slow API does not restart the dispatcher before Slack starts.

    This enters through the process entrypoint, reads a real
    ``DispatcherConfig`` from the environment, and lets the production preflight
    create its own HTTP client and execute its own retry loop. Slack, heartbeat,
    signals, and telemetry are process boundaries, so record them without opening
    external connections or changing this pytest process's signal handlers.
    """
    from curie_dispatcher import run

    events: list[str] = []
    health_requests: list[str] = []
    heartbeat_stop = threading.Event()

    class Telemetry:
        def shutdown(self) -> None:
            events.append("telemetry.shutdown")

    class Supervisor:
        def run(self) -> None:
            events.append("supervisor.run")

        def request_stop(self) -> None:
            events.append("supervisor.request_stop")

    def check_slack(*args: object, **kwargs: object) -> None:
        assert health_requests == ["/health", "/health", "/health"], (
            "Slack capability preflight started before delayed API readiness passed"
        )
        events.append("slack_preflight")

    def build_supervisor(
        config: DispatcherConfig, *, logger: logging.Logger
    ) -> Supervisor:
        assert type(config) is DispatcherConfig
        assert logger.name == "curie_dispatcher"
        assert health_requests == ["/health", "/health", "/health"], (
            "Slack wiring started before delayed API readiness passed"
        )
        assert events[-1] == "slack_preflight"
        events.append("build_supervisor")
        return Supervisor()

    def start_heartbeat(path: str, interval_s: float) -> threading.Event:
        assert path == "/tmp/curie-dispatcher.heartbeat"
        assert interval_s == 10.0
        events.append("heartbeat.start")
        return heartbeat_stop

    monkeypatch.setattr(
        run,
        "bootstrap_service_telemetry",
        lambda *args, **kwargs: Telemetry(),
    )
    monkeypatch.setattr(run, "check_slack_channel_capabilities", check_slack)
    monkeypatch.setattr(run, "build_supervisor", build_supervisor)
    monkeypatch.setattr(run, "start_heartbeat", start_heartbeat)
    monkeypatch.setattr(
        run.signal,
        "signal",
        lambda signum, handler: events.append(f"signal.{signum}"),
    )

    with _loopback_health_server([503, 503, 200]) as (api_url, requests):
        health_requests = requests
        _configure_main_env(monkeypatch, api_url=api_url, timeout_s=0.5)
        run.main()

    assert requests == ["/health", "/health", "/health"], (
        "the real preflight must retry delayed readiness to success before "
        f"starting Slack; observed {requests!r}"
    )
    assert events.count("slack_preflight") == 1
    assert events.count("build_supervisor") == 1
    assert events.count("supervisor.run") == 1
    assert events.index("supervisor.run") > events.index("build_supervisor")
    assert events.count("heartbeat.start") == 1
    assert heartbeat_stop.is_set(), "normal supervisor return must stop the heartbeat"
    assert events[-1] == "telemetry.shutdown", (
        f"telemetry must shut down after a clean supervisor exit; events={events!r}"
    )


def test_run_main_times_out_before_slack_with_actionable_sanitized_log(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A persistently unhealthy API exits once, bounded and actionable.

    This is the timeout-negative sibling of delayed success. Userinfo in the URL
    makes sanitization falsifiable, while a real loopback 503 keeps ``last error``
    deterministic and proves the process made multiple attempts before giving up.
    """
    from curie_dispatcher import run

    events: list[str] = []

    class Telemetry:
        def shutdown(self) -> None:
            events.append("telemetry.shutdown")

    def reached_slack_preflight(*args: object, **kwargs: object) -> None:
        raise AssertionError("Slack preflight ran after the terminal API timeout")

    def reached_supervisor(*args: object, **kwargs: object) -> object:
        events.append("build_supervisor")
        raise AssertionError("Slack was built after the terminal preflight timeout")

    def reached_heartbeat(*args: object, **kwargs: object) -> threading.Event:
        raise AssertionError("heartbeat started before the API preflight passed")

    monkeypatch.setattr(
        run,
        "bootstrap_service_telemetry",
        lambda *args, **kwargs: Telemetry(),
    )
    monkeypatch.setattr(
        run, "check_slack_channel_capabilities", reached_slack_preflight
    )
    monkeypatch.setattr(run, "build_supervisor", reached_supervisor)
    monkeypatch.setattr(run, "start_heartbeat", reached_heartbeat)

    with _loopback_health_server(
        [503], userinfo="operator:credential"
    ) as (api_url, requests):
        _configure_main_env(monkeypatch, api_url=api_url, timeout_s=0.3)
        safe_url = api_url.replace("operator:credential@", "")
        started = time.monotonic()
        with caplog.at_level(logging.ERROR, logger="curie_dispatcher"):
            with pytest.raises(SystemExit) as excinfo:
                run.main()
        elapsed = time.monotonic() - started

    assert excinfo.value.code not in (0, None)
    assert 0.27 <= elapsed < 0.8, (
        f"the 0.3s entrypoint deadline was not bounded; elapsed={elapsed:.3f}s"
    )
    assert len(requests) >= 2, (
        f"the timeout path did not retry before failing; requests={requests!r}"
    )
    assert events == ["telemetry.shutdown"], (
        "terminal preflight failure must happen before Slack/heartbeat and still "
        f"shut down telemetry; events={events!r}"
    )

    terminal = "\n".join(record.getMessage() for record in caplog.records)
    assert safe_url in terminal
    assert "operator" not in terminal and "credential" not in terminal
    assert re.search(r"after \d+\.\d+s", terminal), terminal
    assert re.search(r"\(\d+ attempts, last error: HTTP 503\)", terminal), terminal
    assert "check CURIE_API_URL" in terminal
