"""The test installation declaration in the dispatcher (ADR 0202 decision 1, #4133).

Two refusals live here. At boot, ``DispatcherConfig`` refuses a published
default secret while the declaration is on. In the Slack preflight, a driver
identity is kept to its own agent: the installation's authorized identity
(``default``, as ``auth.test`` reports it) may not be a driver, a sibling
identity's driver must name its agent, and that identity may be bound to no
other agent. The API is faked at the httpx seam and Slack at its provider
client seam, as in test_preflight.py; ``run.main`` cases use a real loopback
API and the production preflight.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
import pytest
from curie_dispatcher import preflight
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials
from curie_dispatcher.preflight import (
    SlackChannelPreflightError,
    check_slack_channel_capabilities,
)
from pydantic import ValidationError

from .conftest import _set_run_env, _TestTelemetry
from .test_preflight import CHANNEL_A, _agent, _client, _config
from .test_preflight_identities import _IdentityClient

DEFAULT = SlackIdentityCredentials(
    name="default", app_token="xapp-default", bot_token="xoxb-default"
)
TESTER = SlackIdentityCredentials(
    name="tester", app_token="xapp-tester", bot_token="xoxb-tester"
)
DEFAULT_IDS = {"ok": True, "bot_id": "B0DEFAULT", "user_id": "U0DEFAULT"}
TESTER_IDS = {"ok": True, "bot_id": "B0TESTER", "user_id": "U0TESTER"}

EXTERNAL_DRIVER = {"channel_id": CHANNEL_A, "bot_id": "B0EXTERNAL", "bot_user_id": "U0EXTERNAL"}
DEFAULT_AS_DRIVER = {"channel_id": CHANNEL_A, "bot_id": "B0DEFAULT", "bot_user_id": "U0DEFAULT"}
SIBLING_DRIVER = {
    "channel_id": CHANNEL_A,
    "bot_id": "B0TESTER",
    "bot_user_id": "U0TESTER",
    "agent": "acme-tester",
}


def _test_config(drivers: list[dict[str, str]], **overrides: object) -> DispatcherConfig:
    return _config(
        api_key="dispatcher-platform-test-key",
        test_installation_enabled=True,
        test_installation_drivers=json.dumps(drivers),
        **overrides,
    )


def _named_agent(name: str, *, adapter: str) -> dict[str, Any]:
    agent = _agent(
        channels=[{"kind": "slack", "address": CHANNEL_A, "adapter": adapter}],
        approval_routes=None,
    )
    agent["name"] = name
    return agent


def _api(*agents: dict[str, Any]) -> httpx.Client:
    return _client(lambda _request: httpx.Response(200, json=list(agents)))


# ---- boot: the declaration and the published default secrets ----


def test_the_declaration_is_off_by_default() -> None:
    config = _config()
    assert config.test_installation_enabled is False
    assert config.test_installation_drivers == ()


@pytest.mark.parametrize(
    "overrides, offender",
    [
        ({"api_key": "curie-dev-key"}, "CURIE_API_KEY"),
        (
            {"approval_chat_attester_secret": "curie-dev-approval-chat-attester"},
            "CURIE_APPROVAL_CHAT_ATTESTER_SECRET",
        ),
    ],
)
def test_on_refuses_a_published_default_secret(overrides: dict[str, str], offender: str) -> None:
    with pytest.raises(ValidationError) as exc:
        _config(
            test_installation_enabled=True,
            test_installation_drivers=json.dumps([EXTERNAL_DRIVER]),
            **{"api_key": "dispatcher-platform-test-key", **overrides},
        )
    assert "CURIE_TEST_INSTALLATION_ENABLED" in str(exc.value)
    assert offender in str(exc.value)


def test_off_keeps_the_published_api_key_bootable() -> None:
    # Control: the shipped default api key boots while the declaration is off.
    assert _config(api_key="curie-dev-key").api_key == "curie-dev-key"


@pytest.mark.parametrize(
    "drivers, fragment",
    [
        ([{k: v for k, v in EXTERNAL_DRIVER.items() if k != "channel_id"}], "channel_id"),
        ([{**EXTERNAL_DRIVER, "channel_id": ""}], "channel_id"),
        ([{**EXTERNAL_DRIVER, "channel_id": EXTERNAL_DRIVER["channel_id"] + "\n"}], "channel_id"),
        ([{**EXTERNAL_DRIVER, "bot_id": EXTERNAL_DRIVER["bot_id"] + "\n"}], "bot_id"),
        (
            [{**EXTERNAL_DRIVER, "bot_user_id": EXTERNAL_DRIVER["bot_user_id"] + "\n"}],
            "bot_user_id",
        ),
        ([{**EXTERNAL_DRIVER, "bot_id": "U0EXAMPLE1"}], "bot_id"),
        ([{**EXTERNAL_DRIVER, "bot_user_id": "B0EXAMPLE1"}], "bot_user_id"),
        ([{**EXTERNAL_DRIVER, "extra": "x"}], "extra"),
        ([{**EXTERNAL_DRIVER, "agent": None}], "agent"),
        (None, "list"),
    ],
)
def test_a_malformed_driver_refuses_boot(drivers: object, fragment: str) -> None:
    with pytest.raises(ValidationError) as exc:
        _test_config(drivers)  # type: ignore[arg-type]
    assert fragment in str(exc.value)


def test_run_main_refuses_boot_on_a_published_default_with_the_declaration_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from curie_dispatcher import run

    _set_run_env(monkeypatch)
    monkeypatch.setenv("CURIE_API_KEY", "curie-dev-key")
    monkeypatch.setenv("CURIE_TEST_INSTALLATION_ENABLED", "true")
    monkeypatch.setenv("CURIE_TEST_INSTALLATION_DRIVERS", json.dumps([EXTERNAL_DRIVER]))
    monkeypatch.setattr(run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry())

    def unexpected(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("the dispatcher reached its API preflight")

    monkeypatch.setattr(run, "check_api_reachable", unexpected)

    with pytest.raises(ValidationError) as exc:
        run.main()
    assert "CURIE_API_KEY" in str(exc.value)


# ---- preflight: a driver identity is kept to its own agent ----


def test_off_asks_a_lone_identity_nothing_new() -> None:
    client = _IdentityClient(auth_response=DEFAULT_IDS)

    admitted = check_slack_channel_capabilities(
        _config(),
        logger=logging.getLogger("test-installation-off"),
        identities=(DEFAULT,),
        web_client=client,
        api_client=_api(_named_agent("acme-bot", adapter="default")),
    )

    assert [identity.name for identity in admitted] == ["default"]
    assert client.auth_calls == 0


def test_on_admits_a_driver_from_another_installation() -> None:
    client = _IdentityClient(auth_response=DEFAULT_IDS)

    admitted = check_slack_channel_capabilities(
        _test_config([EXTERNAL_DRIVER]),
        logger=logging.getLogger("test-installation-external"),
        identities=(DEFAULT,),
        web_client=client,
        api_client=_api(_named_agent("acme-bot", adapter="default")),
    )

    assert [identity.name for identity in admitted] == ["default"]
    assert client.auth_calls == 1


@pytest.mark.parametrize(
    "driver",
    [
        DEFAULT_AS_DRIVER,
        {**DEFAULT_AS_DRIVER, "bot_id": "B0OTHER"},
        {**DEFAULT_AS_DRIVER, "bot_user_id": "U0OTHER"},
        {**DEFAULT_AS_DRIVER, "agent": "acme-bot"},
    ],
    ids=["both-ids", "user-id-only", "bot-id-only", "with-agent"],
)
def test_on_refuses_the_authorized_identity_as_a_driver(driver: dict[str, str]) -> None:
    with pytest.raises(SlackChannelPreflightError) as exc:
        check_slack_channel_capabilities(
            _test_config([EXTERNAL_DRIVER, driver]),
            logger=logging.getLogger("test-installation-authorized"),
            identities=(DEFAULT,),
            web_client=_IdentityClient(auth_response=DEFAULT_IDS),
            api_client=_api(_named_agent("acme-bot", adapter="default")),
        )
    message = str(exc.value)
    assert "testInstallation.drivers[1]" in message
    assert "authorized Slack identity" in message
    assert "B0DEFAULT" not in message and "U0DEFAULT" not in message


def _two_identities(
    drivers: list[dict[str, str]], *agents: dict[str, Any]
) -> tuple[Any, ...]:
    return check_slack_channel_capabilities(
        _test_config(drivers),
        logger=logging.getLogger("test-installation-sibling"),
        identities=(DEFAULT, TESTER),
        web_clients={
            "default": _IdentityClient(auth_response=DEFAULT_IDS),
            "tester": _IdentityClient(auth_response=TESTER_IDS),
        },
        api_client=_api(*agents),
    )


def test_on_admits_a_sibling_driver_bound_to_its_named_agent_alone() -> None:
    admitted = _two_identities(
        [SIBLING_DRIVER],
        _named_agent("acme-bot", adapter="default"),
        _named_agent("acme-tester", adapter="tester"),
    )
    assert [identity.name for identity in admitted] == ["default", "tester"]


def test_on_refuses_a_sibling_driver_that_names_no_agent() -> None:
    driver = {k: v for k, v in SIBLING_DRIVER.items() if k != "agent"}
    with pytest.raises(SlackChannelPreflightError) as exc:
        _two_identities(
            [driver],
            _named_agent("acme-bot", adapter="default"),
            _named_agent("acme-tester", adapter="tester"),
        )
    message = str(exc.value)
    assert "testInstallation.drivers[0]" in message
    assert "sibling Slack identity tester" in message
    assert "names no agent" in message


def test_on_refuses_a_sibling_driver_whose_identity_serves_another_agent() -> None:
    with pytest.raises(SlackChannelPreflightError) as exc:
        _two_identities(
            [SIBLING_DRIVER],
            _named_agent("acme-bot", adapter="tester"),
            _named_agent("acme-tester", adapter="tester"),
        )
    message = str(exc.value)
    assert "testInstallation.drivers[0]" in message
    assert "sibling Slack identity tester" in message
    assert "acme-tester" in message
    # Other agents are counted, not named.
    assert "acme-bot" not in message


def test_on_refuses_a_sibling_driver_whose_identity_serves_only_another_agent() -> None:
    with pytest.raises(SlackChannelPreflightError):
        _two_identities(
            [SIBLING_DRIVER],
            _named_agent("acme-bot", adapter="tester"),
        )


def test_on_refuses_when_an_identity_does_not_report_its_ids() -> None:
    # Fail closed: without auth.test the preflight cannot tell a sibling from
    # an outside bot, so a sibling's agent binding would go unchecked.
    with pytest.raises(SlackChannelPreflightError) as exc:
        check_slack_channel_capabilities(
            _test_config([EXTERNAL_DRIVER]),
            logger=logging.getLogger("test-installation-unverified"),
            identities=(DEFAULT, TESTER),
            web_clients={
                "default": _IdentityClient(auth_response=DEFAULT_IDS),
                "tester": _IdentityClient(auth_response={"ok": False, "error": "invalid_auth"}),
            },
            api_client=_api(_named_agent("acme-bot", adapter="default")),
        )
    assert "Slack identity tester" in str(exc.value)
    assert "auth.test" in str(exc.value)


# ---- run.main: the preflight refusal through the real entrypoint ----


@contextmanager
def _loopback_api(agents: list[dict[str, Any]]) -> Iterator[str]:
    """A real loopback platform API answering ``/health`` and ``/agents``."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib handler API
            body = b'{"status":"ok"}' if self.path == "/health" else json.dumps(agents).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, _format: str, *args: object) -> None:
            """Keep the test server out of the process's terminal log."""

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


@pytest.mark.parametrize("driver, refused", [(DEFAULT_AS_DRIVER, True), (EXTERNAL_DRIVER, False)])
def test_run_main_preflight_refuses_the_authorized_identity_as_a_driver(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    driver: dict[str, str],
    refused: bool,
) -> None:
    from curie_dispatcher import run

    events: list[str] = []

    class Supervisor:
        def run(self) -> None:
            events.append("supervisor.run")

        def request_stop(self) -> None:
            pass

    def build_supervisor(*_args: object, **_kwargs: object) -> Supervisor:
        events.append("build_supervisor")
        return Supervisor()

    monkeypatch.setattr(
        preflight, "WebClient", lambda **_kwargs: _IdentityClient(auth_response=DEFAULT_IDS)
    )
    monkeypatch.setattr(run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry())
    monkeypatch.setattr(run, "build_supervisor", build_supervisor)
    monkeypatch.setattr(run, "start_heartbeat", lambda *_a: threading.Event())
    monkeypatch.setattr(run.signal, "signal", lambda *_a: None)

    with _loopback_api([_named_agent("acme-bot", adapter="default")]) as api_url:
        _set_run_env(monkeypatch)
        monkeypatch.setenv("CURIE_API_URL", api_url)
        monkeypatch.setenv("CURIE_API_KEY", "dispatcher-platform-test-key")
        monkeypatch.setenv("CURIE_API_PREFLIGHT_TIMEOUT_SECONDS", "2")
        monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-entrypoint-test")
        monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-entrypoint-test")
        monkeypatch.setenv("CURIE_TEST_INSTALLATION_ENABLED", "true")
        monkeypatch.setenv("CURIE_TEST_INSTALLATION_DRIVERS", json.dumps([driver]))
        with caplog.at_level(logging.ERROR, logger="curie_dispatcher"):
            if refused:
                with pytest.raises(SystemExit) as excinfo:
                    run.main()
                assert excinfo.value.code == 1
            else:
                run.main()

    if refused:
        assert events == []
        assert any(
            "authorized Slack identity" in record.getMessage() for record in caplog.records
        )
    else:
        assert events == ["build_supervisor", "supervisor.run"]

