"""The runner fails closed when it has no bearer token (#3821).

Before #3821 ``create_app`` installed the bearer middleware only when a token
was present, and nothing at boot checked that one was. A sandbox booted from a
misrendered template or a hand-written claim therefore served every control
route (``/v1/event``, ``/v1/steer``, ``/v1/status``, ...) to anything that could
reach its port.

These tests drive the process entrypoint, ``_serve``, with the real
``RunnerConfig.from_env`` over a real boot env and the real ``create_app``. Only
side-effect seams are replaced: the harness resolution, the boot fetches,
``build_runner`` (which returns a real fake-model ``SessionRunner``) and
``web.run_app`` (which records the app instead of binding a port). What the app
then enforces is read over HTTP, never off an internal field.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner import __main__ as boot
from curie_runner.config import RunnerTokenRequiredError
from curie_runner.fake import FakeModelSession
from curie_runner.history import ConversationReplay
from curie_runner.session import SessionRunner

_TOKEN_ENV = "CURIE_RUNNER_TOKEN"
_FLAG_ENV = "CURIE_RUNNER_ALLOW_TOKENLESS"

_BOOT_ENV = {
    "CURIE_PLUGIN_DIR": "/bundle",
    "CURIE_SESSION_ID": "session-3821",
    "CURIE_SANDBOX_ID": "sandbox-3821",
    "CURIE_BUDGET": '{"max_output_tokens_per_run": 1000, "max_usd_per_day": 5.0}',
    "CURIE_FAKE_MODEL": "1",
}

_FRAME = {"kind": "event", "type": "message", "text": "hi", "user": "U1", "ts": "1"}


@dataclass
class _Boot:
    """What the process entrypoint did, recorded at each side-effect seam."""

    harness_resolved: int = 0
    fetches_loaded: int = 0
    runners_built: int = 0
    apps: list[web.Application] = field(default_factory=list)


@pytest.fixture
def boot_seams(monkeypatch: pytest.MonkeyPatch) -> _Boot:
    for name, value in _BOOT_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv(_TOKEN_ENV, raising=False)
    monkeypatch.delenv(_FLAG_ENV, raising=False)

    record = _Boot()

    def _resolve_harness(_name: str) -> object:
        record.harness_resolved += 1
        return object()

    async def _load_boot_fetches(_config: object, _fake_model: bool, _sdk_env: object) -> Any:
        record.fetches_loaded += 1
        return boot._BootFetches(  # noqa: SLF001 -- the module's own boot record
            memory_store=object(),  # type: ignore[arg-type]
            memory_preamble=None,
            history_store=object(),  # type: ignore[arg-type]
            conversation_replay=ConversationReplay(),
            mcp_capability=None,
        )

    def _build_runner(_config: object, **_kwargs: Any) -> SessionRunner:
        record.runners_built += 1
        return SessionRunner(
            max_usd_per_day=None,
            held_secrets=frozenset(),
            session_factory=FakeModelSession,
            ceiling=0,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="t",
        )

    def _run_app(app: web.Application, **_kwargs: Any) -> None:
        record.apps.append(app)

    monkeypatch.setattr(boot, "_resolve_harness", _resolve_harness)
    monkeypatch.setattr(boot, "_load_boot_fetches", _load_boot_fetches)
    monkeypatch.setattr(boot, "build_runner", _build_runner)
    monkeypatch.setattr(web, "run_app", _run_app)
    return record


def _serve() -> None:
    boot._serve()  # noqa: SLF001 -- the process entrypoint is the subject


def _served_app(record: _Boot) -> web.Application:
    assert len(record.apps) == 1, "the runner did not reach run_app exactly once"
    return record.apps[0]


_Call = tuple[str, str, dict[str, str]]


def _http(app: web.Application, *calls: _Call) -> list[tuple[int, Any]]:
    """Send each (method, path, headers) to the app; one server, one loop.

    An aiohttp Application binds to the first loop that serves it, so every
    request against one recorded app goes through a single TestClient.
    """

    results: list[tuple[int, Any]] = []

    async def go() -> None:
        async with TestClient(TestServer(app)) as client:
            for method, path, headers in calls:
                kwargs: dict[str, Any] = {"headers": headers}
                if method == "POST":
                    kwargs["json"] = _FRAME
                response = await client.request(method, path, **kwargs)
                results.append((response.status, await response.json(content_type=None)))

    anyio.run(go)
    return results


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _tokenless_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        r for r in caplog.records if r.levelno == logging.WARNING and _FLAG_ENV in r.getMessage()
    ]


def _assert_refused_before_any_session_work(record: _Boot) -> None:
    # Refused before the harness, the credential, the boot fetches and the
    # session are touched, and the port is never bound.
    assert record.apps == []
    assert record.harness_resolved == 0
    assert record.fetches_loaded == 0
    assert record.runners_built == 0


# --- refusal ----------------------------------------------------------------


def test_no_token_and_no_flag_refuses_with_the_named_error(boot_seams: _Boot) -> None:
    with pytest.raises(RunnerTokenRequiredError) as excinfo:
        _serve()

    message = str(excinfo.value)
    assert _TOKEN_ENV in message
    assert _FLAG_ENV in message
    _assert_refused_before_any_session_work(boot_seams)


@pytest.mark.parametrize("blank", ["", "   ", "\t\n"], ids=["empty", "spaces", "tab-newline"])
def test_blank_token_counts_as_missing(
    boot_seams: _Boot, monkeypatch: pytest.MonkeyPatch, blank: str
) -> None:
    # Enforcing a whitespace bearer would accept a token nobody minted.
    monkeypatch.setenv(_TOKEN_ENV, blank)

    with pytest.raises(RunnerTokenRequiredError):
        _serve()

    _assert_refused_before_any_session_work(boot_seams)


@pytest.mark.parametrize("value", ["0", "false", "yes", "on", ""])
def test_only_one_and_true_enable_the_flag(
    boot_seams: _Boot, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    # Deliberately narrower than CURIE_FAKE_MODEL's 1/true/yes and than
    # BootEnv's "anything but 0": an opt-out of authentication is spelled one way.
    monkeypatch.setenv(_FLAG_ENV, value)

    with pytest.raises(RunnerTokenRequiredError):
        _serve()

    _assert_refused_before_any_session_work(boot_seams)


# --- the explicit dev flag ----------------------------------------------------


@pytest.mark.parametrize("value", ["1", "true", " TRUE "])
def test_the_flag_boots_a_tokenless_runner_and_warns(
    boot_seams: _Boot,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    value: str,
) -> None:
    monkeypatch.setenv(_FLAG_ENV, value)
    caplog.set_level(logging.WARNING, logger="curie_runner")

    _serve()

    app = _served_app(boot_seams)
    assert _tokenless_warnings(caplog), "a tokenless boot must warn and name the flag"
    health, steer = _http(app, ("GET", "/healthz", {}), ("POST", "/v1/steer", {}))
    assert health == (200, {"ok": True})
    # Pass-through at the HTTP layer: with no header the steer reaches its
    # handler, which answers that there is no turn to steer.
    status, body = steer
    assert status == 409, body
    assert "no active turn" in body["error"]


def test_a_whitespace_token_under_the_flag_serves_tokenless_not_with_whitespace(
    boot_seams: _Boot, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The process must hand create_app None, not "   ": a whitespace bearer
    # would install a middleware that 401s every caller the CLI sends.
    monkeypatch.setenv(_TOKEN_ENV, "   ")
    monkeypatch.setenv(_FLAG_ENV, "1")
    caplog.set_level(logging.WARNING, logger="curie_runner")

    _serve()

    app = _served_app(boot_seams)
    assert _tokenless_warnings(caplog)
    [(status, body)] = _http(app, ("POST", "/v1/steer", {}))
    assert status == 409, body
    assert "no active turn" in body["error"]


# --- a set token is always enforced --------------------------------------------


def test_a_set_token_is_enforced_even_with_the_flag(
    boot_seams: _Boot, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    token = "tok-3821-example"
    monkeypatch.setenv(_TOKEN_ENV, token)
    monkeypatch.setenv(_FLAG_ENV, "1")
    caplog.set_level(logging.WARNING, logger="curie_runner")

    _serve()

    app = _served_app(boot_seams)
    assert _tokenless_warnings(caplog) == [], "a runner holding a token must not claim tokenless"
    no_header, wrong, right = _http(
        app,
        ("POST", "/v1/event", {}),
        ("POST", "/v1/event", _bearer("not-the-token")),
        ("GET", "/v1/status", _bearer(token)),
    )
    assert no_header[0] == 401
    assert wrong[0] == 401
    status, body = right
    assert status == 200, body
    assert body["capacity_admission"] is True


def test_a_set_token_without_the_flag_boots_and_enforces(
    boot_seams: _Boot, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The production shape: a cluster claim carries a token and never the flag.
    token = "tok-3821-example"
    monkeypatch.setenv(_TOKEN_ENV, token)

    _serve()

    app = _served_app(boot_seams)
    no_header, right = _http(app, ("POST", "/v1/steer", {}), ("GET", "/v1/status", _bearer(token)))
    assert no_header[0] == 401
    assert right[0] == 200, right[1]


def test_liveness_needs_no_bearer_on_an_enforcing_runner(
    boot_seams: _Boot, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The chart readinessProbe hits /healthz with no header; an enforcing
    # runner must still answer it or no warm pod ever turns ready.
    monkeypatch.setenv(_TOKEN_ENV, "tok-3821-example")

    _serve()

    assert _http(_served_app(boot_seams), ("GET", "/healthz", {})) == [(200, {"ok": True})]
