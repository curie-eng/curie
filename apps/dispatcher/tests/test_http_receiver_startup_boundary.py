"""Each selected HTTP receiver starts behind its own failure boundary.

@spec HTTP-INVOCATION-EXECUTION-25.
Every test drives ``run.main`` through the real ``build_supervisor``; Slack, Redis and the
websocket are recorded synthetic seams, so no real connection or key custody is qualified.
"""

from __future__ import annotations

import base64
import json
import logging
import threading
from collections import Counter
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from curie_dispatcher import http_receiver_initializer, run
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.identities import SlackIdentityCredentials
from curie_dispatcher.preflight import PreflightedIdentity
from slack_sdk.web import WebClient

from .conftest import _set_run_env, _TestTelemetry

ONE, TWO, FREE = "acme-one", "acme-two", "default"
MATERIAL = bytes(range(32))
CREDENTIAL = "chn.SyntheticBoundaryCredential"
SLACK_TEAM = {ONE: "T0EXAMPLE1", TWO: "T0EXAMPLE2"}
SLACK_BOT = {ONE: "U0EXAMPLEBOT1", TWO: "U0EXAMPLEBOT2"}
SLOT = {ONE: "1", TWO: "2"}


def _key() -> str:
    """@spec HTTP-INVOCATION-EXECUTION-25: unpadded canonical base64url key material."""
    return base64.urlsafe_b64encode(MATERIAL).rstrip(b"=").decode("ascii")


def _entry(name: str) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-25: one synthetic receiver registration."""
    slot = SLOT[name]
    return {
        "scope": {
            "schema_version": 1,
            "receiver_id": f"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa{slot}",
            "receiver_generation": 1,
            "channel_identity_id": f"11111111-1111-4111-8111-11111111111{slot}",
            "channel_identity_name": name,
            "provider_installation_id": f"22222222-2222-4222-8222-22222222222{slot}",
            "namespace": {
                "id": f"44444444-4444-4444-8444-44444444444{slot}", "authority": "",
                "kind": "slack_workspace", "key": SLACK_TEAM[name],
            },
            "installation_account": SLACK_TEAM[name], "bot_user_id": SLACK_BOT[name],
        },
        "bindings": [{
            "schema_version": 1, "native_channel_id": "C0EXAMPLEBOUND",
            "binding_id": f"33333333-3333-4333-8333-33333333333{slot}",
            "binding_generation": 0, "channel_credential": CREDENTIAL,
        }],
        "key": _key(),
    }


def _write(directory: Path, text: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: the mounted operator file."""
    path = directory / "receivers.json"
    if path.exists():
        path.chmod(0o600)
    path.write_text(text)
    path.chmod(0o440)


class _Rig:
    """Recorded Slack answers and sockets around the real startup path."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.monkeypatch = monkeypatch
        self.auth_calls: Counter[str] = Counter()
        self.auth_failure: dict[str, Exception] = {}
        self.auth_changes: dict[str, dict[str, Any]] = {}
        self.connected: list[str] = []
        self.admit: tuple[str, ...] = (ONE, TWO, FREE)

    def client(self, _config: DispatcherConfig, credentials: SlackIdentityCredentials) -> WebClient:
        """@spec HTTP-INVOCATION-EXECUTION-25: each identity's own synthetic auth.test."""
        name = credentials.name
        client = WebClient(token=credentials.bot_token)

        def auth_test(**_kwargs: Any) -> dict[str, Any]:
            self.auth_calls[name] += 1
            if name in self.auth_failure:
                raise self.auth_failure[name]
            return {
                "ok": True, "team_id": SLACK_TEAM.get(name, "T0EXAMPLE9"),
                "user_id": SLACK_BOT.get(name, "U0EXAMPLEBOT9"), "bot_id": "B0EXAMPLE1",
                "is_enterprise_install": False, **self.auth_changes.get(name, {}),
            }

        client.auth_test = auth_test  # type: ignore[method-assign]
        return client

    def run(self, directory: Path) -> dict[str, Any]:
        """@spec HTTP-INVOCATION-EXECUTION-25: ``main`` with the real ``build_supervisor``."""
        _set_run_env(self.monkeypatch)
        names = (FREE, ONE, TWO)
        envs = {
            name: (("SLACK_APP_TOKEN", "SLACK_BOT_TOKEN") if name == FREE else (
                f"CURIE_SLACK_APP_TOKEN__{index}", f"CURIE_SLACK_BOT_TOKEN__{index}"))
            for index, name in enumerate(names)
        }
        self.monkeypatch.setenv("CURIE_SLACK_IDENTITIES", json.dumps([
            {"name": name, "app_token_env": envs[name][0], "bot_token_env": envs[name][1],
             "signing_secret_env": None}
            for name in names
        ]))
        for name in names:
            self.monkeypatch.setenv(envs[name][0], f"xapp-{name}")
            self.monkeypatch.setenv(envs[name][1], f"xoxb-{name}")
        self.monkeypatch.setenv("CURIE_HTTP_RECEIVER_DIR", str(directory))
        outcome: dict[str, Any] = {}
        connected = self.connected

        class FakeConnection:
            def __init__(self, _app: Any, app_token: str, **_kwargs: Any) -> None:
                connected.append(app_token.removeprefix("xapp-"))

        def run_group(group: Any) -> None:
            for connection in group.identity_connections:
                connection.connect()
            outcome["ran"] = True

        patch = self.monkeypatch.setattr
        patch(run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry())
        patch(run, "check_api_reachable", lambda *a, **k: None)
        patch(run, "check_slack_channel_capabilities", lambda _c, *, logger, identities: tuple(
            PreflightedIdentity(item, None) for item in identities if item.name in self.admit
        ))
        patch(run, "build_web_client", self.client)
        patch(run, "build_redis", lambda _config: MagicMock())
        patch(run, "SocketModeConnection", FakeConnection)
        patch(run.SupervisorGroup, "run", run_group)
        patch(run, "start_installation_reports", lambda *a, **k: None)
        patch(run, "start_heartbeat", lambda *a, **k: threading.Event())
        patch(run.signal, "signal", lambda *a, **k: None)
        try:
            run.main()
        except SystemExit as exc:
            outcome["exit"] = exc.code
        return outcome


@pytest.fixture
def rig(monkeypatch: pytest.MonkeyPatch) -> _Rig:
    """@spec HTTP-INVOCATION-EXECUTION-25."""
    return _Rig(monkeypatch)


@pytest.fixture
def receivers(tmp_path: Path) -> Path:
    """@spec HTTP-INVOCATION-EXECUTION-25: two registered identities, one unregistered."""
    _write(tmp_path, json.dumps({"schema_version": 1, "receivers": [_entry(ONE), _entry(TWO)]}))
    return tmp_path


def _assert_only_first_excluded(
    rig: _Rig, outcome: dict[str, Any], caplog: pytest.LogCaptureFixture, stage: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: the failure costs only its own identity."""
    assert outcome == {"ran": True}
    assert sorted(rig.connected) == sorted([FREE, TWO])
    errors = [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR]
    named = [message for message in errors if ONE in message]
    assert len(named) == 1 and stage in named[0], errors
    rendered = caplog.text
    for secret in (CREDENTIAL, _key(), "xoxb-", "xapp-", "T0EXAMPLE", "U0EXAMPLE"):
        assert secret not in rendered


def test_auth_test_timeout_excludes_only_that_identity(
    rig: _Rig, receivers: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: a timeout at auth.test, with the real supervisor."""
    rig.auth_failure[ONE] = TimeoutError("synthetic slack timeout")
    with caplog.at_level(logging.ERROR):
        outcome = rig.run(receivers)
    _assert_only_first_excluded(rig, outcome, caplog, "auth_test")


def test_failed_comparison_excludes_only_that_identity(
    rig: _Rig, receivers: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: a registration auth.test disagrees with."""
    rig.auth_changes[ONE] = {"user_id": "U0EXAMPLEOTHER"}
    with caplog.at_level(logging.ERROR):
        outcome = rig.run(receivers)
    _assert_only_first_excluded(rig, outcome, caplog, "auth_test")


def test_unadmitted_registration_is_excluded_by_name(
    rig: _Rig, receivers: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: a registration whose identity was not admitted."""
    rig.admit = (TWO, FREE)
    with caplog.at_level(logging.ERROR):
        outcome = rig.run(receivers)
    _assert_only_first_excluded(rig, outcome, caplog, "registration")
    assert rig.auth_calls[ONE] == 0


def test_runtime_construction_failure_excludes_only_that_identity(
    rig: _Rig, receivers: Path, caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: a runtime that cannot be constructed."""
    real = http_receiver_initializer.build_http_receiver_runtime

    def build(scope: Any, *args: Any, **kwargs: Any) -> Any:
        if scope.channel_identity_name == ONE:
            raise ValueError("synthetic runtime failure")
        return real(scope, *args, **kwargs)

    monkeypatch.setattr(http_receiver_initializer, "build_http_receiver_runtime", build)
    with caplog.at_level(logging.ERROR):
        outcome = rig.run(receivers)
    _assert_only_first_excluded(rig, outcome, caplog, "runtime")


def test_app_construction_failure_excludes_only_that_identity(
    rig: _Rig, receivers: Path, caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: a failure while the identity's app is built."""
    real = run.build_app

    def build(config: DispatcherConfig, **kwargs: Any) -> Any:
        if kwargs["identity"].name == ONE:
            raise ValueError("synthetic app failure")
        return real(config, **kwargs)

    monkeypatch.setattr(run, "build_app", build)
    with caplog.at_level(logging.ERROR):
        outcome = rig.run(receivers)
    _assert_only_first_excluded(rig, outcome, caplog, "app")


def test_each_receiver_is_qualified_once_per_start(rig: _Rig, receivers: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: app construction does not repeat auth.test."""
    outcome = rig.run(receivers)
    assert outcome == {"ran": True}
    assert sorted(rig.connected) == sorted([FREE, ONE, TWO])
    assert rig.auth_calls == Counter({ONE: 1, TWO: 1})


def test_malformed_or_duplicate_file_still_stops_startup(
    rig: _Rig, tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-25: a defective file excludes nothing, it stops."""
    _write(tmp_path, "{not json")
    with caplog.at_level(logging.ERROR):
        assert rig.run(tmp_path) == {"exit": 1}
    assert rig.connected == [] and not rig.auth_calls
    assert "invalid_document" in caplog.text
    _write(tmp_path, json.dumps({"schema_version": 1, "receivers": [_entry(ONE), _entry(ONE)]}))
    assert rig.run(tmp_path) == {"exit": 1}
    assert rig.connected == [] and not rig.auth_calls
