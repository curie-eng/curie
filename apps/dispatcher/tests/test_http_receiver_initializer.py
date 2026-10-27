"""Dispatcher operator initializer for selected HTTP receivers.

@spec HTTP-INVOCATION-EXECUTION-19/20.
Slack is a recorded synthetic auth.test; no real connection or key custody is qualified.
"""

from __future__ import annotations

import base64
import importlib
import json
import logging
import threading
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.http_receiver import HttpReceiverRuntime
from curie_dispatcher.identities import SlackIdentityCredentials
from curie_dispatcher.preflight import PreflightedIdentity
from slack_sdk.web import WebClient

from .conftest import _set_run_env, _TestTelemetry

FILE = "receivers.json"
RECEIVER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
IDENTITY = "11111111-1111-4111-8111-111111111111"
INSTALLATION = "22222222-2222-4222-8222-222222222222"
BINDING = "33333333-3333-4333-8333-333333333333"
NAMESPACE = "44444444-4444-4444-8444-444444444444"
CREDENTIAL = "chn.SyntheticInitializerCredential"
MATERIAL = bytes(range(32))
NAME = "ops-bot"
BOT = "U0EXAMPLEBOT"


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-20: fail during execution, never collection."""
    name = "curie_dispatcher.http_receiver_initializer"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "dispatcher HTTP receiver initializer is not implemented"
        raise
    for symbol in (
        "HttpReceiverInitializationError",
        "load_http_receiver_registrations",
        "build_selected_receivers",
    ):
        assert hasattr(module, symbol), f"dispatcher HTTP receiver initializer missing {symbol}"
    return module


def key(material: bytes) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-20: unpadded canonical base64url."""
    return base64.urlsafe_b64encode(material).rstrip(b"=").decode("ascii")


def entry(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-19/20: one receiver's scope, bindings and key."""
    value: dict[str, Any] = {
        "scope": {
            "schema_version": 1, "receiver_id": RECEIVER, "receiver_generation": 1,
            "channel_identity_id": IDENTITY, "channel_identity_name": NAME,
            "provider_installation_id": INSTALLATION,
            "namespace": {
                "id": NAMESPACE, "authority": "", "kind": "slack_workspace", "key": "T0EXAMPLE1",
            },
            "installation_account": "T0EXAMPLE1", "bot_user_id": BOT,
        },
        "bindings": [{
            "schema_version": 1, "native_channel_id": "C0EXAMPLEBOUND", "binding_id": BINDING,
            "binding_generation": 0, "channel_credential": CREDENTIAL,
        }],
        "key": key(MATERIAL),
    }
    value.update(changes)
    return value


def write(directory: Path, value: Any, *, mode: int = 0o440) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: the mounted operator file."""
    path = directory / FILE
    if path.exists():
        path.chmod(0o600)
    path.write_text(value if isinstance(value, str) else json.dumps(value))
    path.chmod(mode)


def refused(module: Any, call: Any) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-20: fixed stage, never supplied material."""
    with pytest.raises(module.HttpReceiverInitializationError) as caught:
        call()
    error = caught.value
    assert error.__cause__ is None and error.__suppress_context__
    rendered = str(error) + repr(error)
    assert CREDENTIAL not in rendered and key(MATERIAL) not in rendered
    assert str(error) == "http receiver initialization refused: " + error.stage
    return str(error.stage)


def config(api_url: str = "https://core.example.test") -> DispatcherConfig:
    """@spec HTTP-INVOCATION-EXECUTION-20: the configured API base URL."""
    return DispatcherConfig(  # type: ignore[call-arg]
        approval_chat_attester_secret="dispatcher-attester-test-secret", api_base_url=api_url,
    )


def qualified_client(**changes: Any) -> WebClient:
    """@spec HTTP-INVOCATION-EXECUTION-19: synthetic ordinary-workspace auth.test."""
    client = WebClient(token="xoxb-synthetic-initializer")
    client.auth_test = MagicMock(return_value={  # type: ignore[method-assign]
        "ok": True, "team_id": "T0EXAMPLE1", "user_id": BOT, "bot_id": "B0EXAMPLE1",
        "is_enterprise_install": False, **changes,
    })
    return client


def admitted(*names: str) -> tuple[PreflightedIdentity, ...]:
    """@spec HTTP-INVOCATION-EXECUTION-20: identities that passed Slack preflight."""
    return tuple(
        PreflightedIdentity(
            SlackIdentityCredentials(name=name, app_token="xapp-" + name, bot_token="xoxb-" + name),
            None,
        )
        for name in names
    )


def test_loads_and_builds_only_the_selected_admitted_identity(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/20: one runtime per selected identity."""
    module = api()
    write(tmp_path, {"schema_version": 1, "receivers": [entry()]})
    registrations = module.load_http_receiver_registrations(str(tmp_path))
    assert len(registrations) == 1 and CREDENTIAL not in repr(registrations)
    built: list[str] = []

    def factory(_config: DispatcherConfig, identity: SlackIdentityCredentials) -> WebClient:
        """@spec HTTP-INVOCATION-EXECUTION-19: the identity's own ordinary Web client."""
        built.append(identity.name)
        return qualified_client()

    receivers = module.build_selected_receivers(
        config(), admitted("default", NAME), registrations, web_client_factory=factory,
    )
    assert built == [NAME]
    assert type(receivers) is tuple and len(receivers) == 1
    assert type(receivers[0]) is HttpReceiverRuntime and receivers[0].identity_name == NAME


def test_unadmitted_or_unqualified_selection_refuses_startup(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/20: a selection never falls back to legacy."""
    module = api()
    write(tmp_path, {"schema_version": 1, "receivers": [entry()]})
    registrations = module.load_http_receiver_registrations(str(tmp_path))
    assert refused(module, lambda: module.build_selected_receivers(
        config(), admitted("default"), registrations,
        web_client_factory=lambda *_: qualified_client(),
    )) == "invalid_runtime"
    assert refused(module, lambda: module.build_selected_receivers(
        config(), admitted(NAME), registrations,
        web_client_factory=lambda *_: qualified_client(team_id="T0EXAMPLE2"),
    )) == "invalid_runtime"
    assert refused(module, lambda: module.build_selected_receivers(
        config(), admitted(NAME), registrations,
        web_client_factory=lambda *_: qualified_client(enterprise_id="E0EXAMPLE1"),
    )) == "invalid_runtime"


def mutate(path: str, value: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20: replace or delete one nested member."""
    result: dict[str, Any] = {"schema_version": 1, "receivers": [entry()]}
    *parents, leaf = path.split(".")
    node: Any = result
    for part in parents:
        node = node[int(part)] if part.isdigit() else node[part]
    target: Any = int(leaf) if leaf.isdigit() else leaf
    if value is KeyError:
        del node[target]
    else:
        node[target] = value
    return result


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("schema_version", True),
        ("schema_version", KeyError),
        ("receivers", []),
        ("unexpected", 1),
        ("receivers.0.unexpected", 1),
        ("receivers.0.key", key(MATERIAL[:31])),
        ("receivers.0.key", KeyError),
        ("receivers.0.bindings", []),
        ("receivers.0.bindings.0.channel_credential", ""),
        ("receivers.0.bindings.0.binding_generation", True),
        ("receivers.0.bindings.0.unexpected", 1),
        ("receivers.0.scope.namespace.authority", "grid"),
        ("receivers.0.scope.unexpected", 1),
    ],
)
def test_partial_or_invalid_documents_refuse(tmp_path: Path, path: str, value: Any) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: strict fields, nothing defaulted or ignored."""
    module = api()
    write(tmp_path, mutate(path, value))
    assert refused(
        module, lambda: module.load_http_receiver_registrations(str(tmp_path)),
    ) == "invalid_document"


def test_duplicate_identity_unsafe_file_and_missing_directory_refuse(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: unique identities and protected custody."""
    module = api()
    load = lambda: module.load_http_receiver_registrations(str(tmp_path))  # noqa: E731
    write(tmp_path, {"schema_version": 1, "receivers": [entry(), entry()]})
    assert refused(module, load) == "invalid_document"
    write(tmp_path, {"schema_version": 1, "receivers": [entry()]}, mode=0o644)
    assert refused(module, load) == "unsafe_file"
    assert refused(
        module, lambda: module.load_http_receiver_registrations(str(tmp_path / "absent")),
    ) == "unconfigured_directory"


def _run_main(
    monkeypatch: pytest.MonkeyPatch, directory: Path | None, auth: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20: run main with recorded Slack/API seams."""
    from curie_dispatcher import run

    _set_run_env(monkeypatch)
    monkeypatch.setenv("SLACK_APP_TOKEN", "xapp-default")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-default")
    monkeypatch.setenv("CURIE_SLACK_IDENTITIES", json.dumps([
        {"name": "default", "app_token_env": "SLACK_APP_TOKEN",
         "bot_token_env": "SLACK_BOT_TOKEN", "signing_secret_env": None},
        {"name": NAME, "app_token_env": "CURIE_SLACK_APP_TOKEN__1",
         "bot_token_env": "CURIE_SLACK_BOT_TOKEN__1", "signing_secret_env": None},
    ]))
    monkeypatch.setenv("CURIE_SLACK_APP_TOKEN__1", "xapp-ops")
    monkeypatch.setenv("CURIE_SLACK_BOT_TOKEN__1", "xoxb-ops")
    if directory is not None:
        monkeypatch.setenv("CURIE_HTTP_RECEIVER_DIR", str(directory))
    seen: dict[str, Any] = {}

    class Group:
        def run(self) -> None:
            seen["ran"] = True

        def request_stop(self) -> None:
            pass

    def build_supervisor(_config: DispatcherConfig, **kwargs: Any) -> Group:
        seen["kwargs"] = kwargs
        return Group()

    monkeypatch.setattr(run, "bootstrap_service_telemetry", lambda *a, **k: _TestTelemetry())
    monkeypatch.setattr(run, "check_api_reachable", lambda *a, **k: None)
    monkeypatch.setattr(
        run, "check_slack_channel_capabilities",
        lambda _c, *, logger, identities: tuple(PreflightedIdentity(i, None) for i in identities),
    )
    monkeypatch.setattr(run, "build_supervisor", build_supervisor)
    monkeypatch.setattr(run, "build_web_client", lambda *_: qualified_client(**(auth or {})))
    monkeypatch.setattr(run, "start_heartbeat", lambda *a, **k: threading.Event())
    monkeypatch.setattr(run.signal, "signal", lambda *a, **k: None)
    try:
        run.main()
    except SystemExit as exc:
        seen["exit"] = exc.code
    return seen


def test_main_threads_initialized_receivers_into_the_supervisor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/20: selected identity reaches build_supervisor."""
    api()
    write(tmp_path, {"schema_version": 1, "receivers": [entry()]})
    seen = _run_main(monkeypatch, tmp_path)
    assert seen.get("ran") is True, seen
    receivers = seen["kwargs"]["http_receivers"]
    assert [receiver.identity_name for receiver in receivers] == [NAME]


def test_main_without_selection_keeps_the_legacy_supervisor_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: unconfigured startup passes no receiver keyword."""
    api()
    seen = _run_main(monkeypatch, None)
    assert seen.get("ran") is True
    assert set(seen["kwargs"]) == {"logger", "identities", "declared_count"}


def test_main_refuses_to_start_on_a_failed_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-19/20: no socket starts after a selected failure."""
    api()
    write(tmp_path, {"schema_version": 1, "receivers": [entry()]})
    with caplog.at_level(logging.ERROR):
        seen = _run_main(monkeypatch, tmp_path, auth={"user_id": "U0EXAMPLEOTHER"})
    assert seen == {"exit": 1}
    assert "http receiver initialization refused: invalid_runtime" in caplog.text
    assert CREDENTIAL not in caplog.text and key(MATERIAL) not in caplog.text
