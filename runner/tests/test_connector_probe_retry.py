"""Bounded retry of the boot MCP capability probe (#2945).

Kept in its own module on purpose: the fix-pin gate reverts the source change
and runs this test against the old runner, so every module-level import here
must already exist there. Symbols the fix introduces are resolved inside the
test body, making the pinned test fail at runtime rather than at collection.

The bug under test: a connector that is merely not up yet -- a rollout
window -- is answered as a capability failure after one dial. The probe must
retry with a bounded backoff before declaring the connector unavailable, and
the refusal must stay distinguishable from a genuine misconfiguration.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any

import anyio
import pytest
from curie_runner import mcp_tool_capability as capability_module
from curie_runner.mcp_tool_capability import (
    ConnectorCapabilityFailure,
    probe_mcp_tool_capability,
)

_SERVER = Path(__file__).parent / "fixtures" / "mcp_tool_capability_server.py"

_PROBE_RESULT = tuple[int, bool, frozenset[str], frozenset[str]]


def _derived() -> dict[str, dict[str, Any]]:
    return {"github": {"type": "http", "url": "http://127.0.0.1:9/mcp", "headers": {}}}


def test_probe_retries_a_transient_failure_before_declaring_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # AC1 (#2945): the boot probe is one dial. A rollout-window connection
    # reset must be retried with backoff, not answered as a capability failure.
    probe_once = getattr(capability_module, "_probe_server_once", None)
    assert probe_once is not None, "boot probe is a single unretried dial"

    calls: list[str] = []

    async def flaky(*_args: object, **_kwargs: object) -> _PROBE_RESULT:
        calls.append("dial")
        if len(calls) <= 2:
            raise OSError("connection reset")
        return 1, False, frozenset(), frozenset()

    monkeypatch.setattr(capability_module, "_probe_server_once", flaky)
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BACKOFF_SECONDS", 0.0)

    result = anyio.run(probe_mcp_tool_capability, None, _derived(), {})

    assert calls == ["dial", "dial", "dial"]
    assert result.connector_failures == ()
    assert result.complete


def test_probe_exhaustion_reports_real_attempt_count_in_the_refusal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # AC1/AC3 (#2945): exhaustion keeps the fail-closed exclusion, but the
    # refusal names the bounded attempt count so it stays distinguishable
    # from a deterministic credential misconfiguration.
    attempts_budget = getattr(capability_module, "_PROBE_ATTEMPTS", None)
    assert attempts_budget is not None, "boot probe has no bounded attempt count"
    calls: list[str] = []

    async def down(*_args: object, **_kwargs: object) -> _PROBE_RESULT:
        calls.append("dial")
        raise OSError("connection refused")

    monkeypatch.setattr(capability_module, "_probe_server_once", down)
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BACKOFF_SECONDS", 0.0)

    result = anyio.run(probe_mcp_tool_capability, None, _derived(), {})

    assert calls == ["dial"] * attempts_budget
    failures = result.connector_failures
    assert len(failures) == 1
    failure = failures[0]
    assert failure.reason == "probe_failed"
    assert failure.attempts == attempts_budget
    message = failure.caller_message()
    assert f"after {attempts_budget} attempts" in message
    # The three refusal kinds remain distinguishable in caller-visible text.
    misconfigured = ConnectorCapabilityFailure(
        connector="github", credential_names=(), reason="probe_misconfigured"
    ).caller_message()
    credential = ConnectorCapabilityFailure(
        connector="github",
        credential_names=("GITHUB_TOKEN",),
        reason="missing_credential",
    ).caller_message()
    assert len({message, misconfigured, credential}) == 3


def test_probe_retry_budget_bounds_the_boot_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # AC2 (#2945): the retry window is bounded by a wall-clock deadline, not
    # only by the attempt count. A budget already spent gives up after the
    # dial that failed into it, and the refusal reports the dials really made.
    assert getattr(capability_module, "_PROBE_RETRY_BUDGET_SECONDS", None) is not None, (
        "boot probe retry has no wall-clock budget"
    )
    calls: list[str] = []

    async def down(*_args: object, **_kwargs: object) -> _PROBE_RESULT:
        calls.append("dial")
        raise OSError("connection refused")

    monkeypatch.setattr(capability_module, "_probe_server_once", down)
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BUDGET_SECONDS", 0.0)

    result = anyio.run(probe_mcp_tool_capability, None, _derived(), {})

    assert calls == ["dial"]
    failures = result.connector_failures
    assert len(failures) == 1
    failure = failures[0]
    assert failure.reason == "probe_failed"
    assert failure.attempts == 1
    assert "after 1 attempt" not in failure.caller_message()


def test_probe_deadline_clamps_every_dials_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # AC2 (#2945): the deadline is enforced on the dial, not only the sleep.
    # A connector that hangs on every attempt must not get the full
    # per-attempt timeout each time: each dial is clamped to the budget
    # remaining, so a mixed fail-then-hang sequence cannot run past the
    # budget.
    timeouts: list[float] = []

    async def hang_then_fail(*_args: object, **kwargs: object) -> _PROBE_RESULT:
        timeouts.append(float(kwargs["timeout_seconds"]))
        await anyio.sleep(1.0)
        raise OSError("connection reset")

    monkeypatch.setattr(capability_module, "_probe_server_once", hang_then_fail)
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BUDGET_SECONDS", 3.0)

    result = anyio.run(probe_mcp_tool_capability, None, _derived(), {})

    assert timeouts == pytest.approx([3.0, 2.0, 1.0], abs=0.5)
    failures = result.connector_failures
    assert len(failures) == 1
    assert failures[0].reason == "probe_failed"
    assert failures[0].attempts == 3


def test_probe_deadline_stops_dialing_once_spent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # AC2 (#2945): once the budget is spent, no further dial starts, and the
    # refusal reports the dials actually made.
    timeouts: list[float] = []

    async def hang_then_fail(*_args: object, **kwargs: object) -> _PROBE_RESULT:
        timeouts.append(float(kwargs["timeout_seconds"]))
        await anyio.sleep(1.0)
        raise OSError("connection reset")

    monkeypatch.setattr(capability_module, "_probe_server_once", hang_then_fail)
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BUDGET_SECONDS", 2.0)

    result = anyio.run(probe_mcp_tool_capability, None, _derived(), {})

    assert timeouts == pytest.approx([2.0, 1.0], abs=0.5)
    failures = result.connector_failures
    assert len(failures) == 1
    failure = failures[0]
    assert failure.reason == "probe_failed"
    assert failure.attempts == 2
    assert "after 2 attempts" in failure.caller_message()


def test_probe_deadline_binds_a_real_hanging_connector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # AC2 (#2945): end to end against a real stdio server that never answers,
    # the clamped timeout -- not the full 15-second per-attempt timeout --
    # ends the dial, so the whole probe stays inside the patched budget
    # instead of stretching toward a full minute of retries.
    script = tmp_path / "hanging_mcp.py"
    script.write_text(
        "import runpy, time\n"
        "time.sleep(20)\n"
        f"runpy.run_path({str(_SERVER)!r}, run_name='__main__')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BUDGET_SECONDS", 3.0)
    derived = {"github": {"command": sys.executable, "args": [str(script)]}}

    started = time.monotonic()
    result = anyio.run(probe_mcp_tool_capability, None, derived, {})
    elapsed = time.monotonic() - started

    failures = result.connector_failures
    assert len(failures) == 1
    assert failures[0].reason == "probe_failed"
    assert failures[0].attempts == 1
    assert elapsed < 10


def test_probe_does_not_retry_a_deterministic_misconfiguration() -> None:
    # AC3 (#2945): a server that answers but serves a nonconforming tool
    # name is genuinely misconfigured; the probe dials it once, does not
    # retry, and the refusal carries its own distinguishable wording. This
    # runs the real fixture server, so the error crosses the transport task
    # group and arrives wrapped, exactly like a live misconfigured connector.
    derived = {
        "github": {
            "command": sys.executable,
            "args": [str(_SERVER)],
            "env": {"CURIE_TEST_TOOL_MODE": "invalid-name"},
        }
    }
    result = anyio.run(probe_mcp_tool_capability, None, derived, {})

    failures = result.connector_failures
    assert len(failures) == 1
    failure = failures[0]
    assert failure.reason == "probe_misconfigured"
    assert "misconfigured" in failure.caller_message()
    assert "capability probe failed" not in failure.caller_message()


def test_probe_retry_never_leaks_the_credential_value(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # AC5 (#2945): the retry path adds two new leak surfaces -- the
    # per-attempt retry log and the exhausted-failure exception the probe
    # warning formats. Both carry the error class, never its text, which can
    # hold a header value (#2634 discipline).
    planted = "ghp-not-a-real-token-PLACEHOLDER"
    calls: list[str] = []

    async def leaky(*_args: object, **_kwargs: object) -> _PROBE_RESULT:
        calls.append("dial")
        raise OSError(f"connection reset dialing with Bearer {planted}")

    monkeypatch.setattr(capability_module, "_probe_server_once", leaky)
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BACKOFF_SECONDS", 0.0)

    with caplog.at_level(logging.WARNING):
        result = anyio.run(probe_mcp_tool_capability, None, _derived(), {})

    assert calls, "the retry wrapper was not exercised"
    failures = result.connector_failures
    assert len(failures) == 1
    failure = failures[0]
    assert planted not in failure.caller_message()
    probe_failure = getattr(capability_module, "_ProbeFailure", None)
    assert probe_failure is not None, "exhausted retry carries no attempt-count failure"
    for record in caplog.records:
        assert planted not in record.getMessage()
    assert any("OSError" in record.getMessage() for record in caplog.records)


def test_probe_retry_sleep_is_cancellable(monkeypatch: pytest.MonkeyPatch) -> None:
    # AC2 (#2945): cancellation is a BaseException in both anyio backends and
    # must propagate out of the backoff sleep rather than becoming a retry.
    assert getattr(capability_module, "_probe_server_once", None) is not None, (
        "boot probe is a single unretried dial"
    )
    dials: list[str] = []

    async def down(*_args: object, **_kwargs: object) -> _PROBE_RESULT:
        dials.append("dial")
        raise OSError("connection reset")

    monkeypatch.setattr(capability_module, "_probe_server_once", down)
    # The autouse fixture zeroes the backoff; a long sleep is required to be
    # cancelled into. setattr in the body wins over the autouse fixture, and
    # the budget must not reject the long backoff before the sleep.
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BACKOFF_SECONDS", 30.0)
    monkeypatch.setattr(capability_module, "_PROBE_RETRY_BUDGET_SECONDS", 600.0)

    async def go() -> bool:
        with anyio.move_on_after(0.05) as scope:
            await capability_module._probe_server(
                _derived()["github"],
                tool_prefix="mcp__github__",
                plugin_dir=None,
                inherited_env={},
            )
        return scope.cancelled_caught

    cancelled = anyio.run(go)

    assert cancelled
    assert dials == ["dial"]


def test_reprobe_redial_stays_single_attempt_and_keeps_the_boot_count(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # AC2/AC4 (#2945): the turn-start re-dial keeps #2634's per-turn budget --
    # one dial, bounded by the session's recovery budget, never the boot
    # retry sequence -- and a connector still down keeps the boot probe's
    # attempt count and refusal text.
    reprobe = getattr(capability_module, "reprobe_connector_failures", None)
    assert reprobe is not None, "turn-start re-probe is missing (#2634)"
    assert getattr(capability_module, "_probe_server_once", None) is not None, (
        "boot probe is a single unretried dial"
    )

    calls: list[dict[str, Any]] = []

    async def down(*_args: object, **kwargs: object) -> _PROBE_RESULT:
        calls.append(dict(kwargs))
        raise OSError("connection reset")

    monkeypatch.setattr(capability_module, "_probe_server", down)
    boot = ConnectorCapabilityFailure(
        connector="github",
        credential_names=(),
        reason="probe_failed",
        attempts=3,
    )

    remaining = anyio.run(reprobe, (boot,), _derived(), {})

    assert len(calls) == 1
    assert calls[0].get("attempts") == 1
    assert remaining == (boot,)
    assert remaining[0].attempts == 3
    assert "after 3 attempts" in remaining[0].caller_message()


def test_reprobe_redial_logs_the_real_dial_error_class(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # AC5 (#2945): through the REAL wrapper at one attempt, the turn-time
    # log keeps the last dial's error class, not the wrapper's own type, and
    # never the exception text, which can carry a header value.
    reprobe = getattr(capability_module, "reprobe_connector_failures", None)
    assert reprobe is not None, "turn-start re-probe is missing (#2634)"
    assert getattr(capability_module, "_probe_server_once", None) is not None, (
        "boot probe is a single unretried dial"
    )

    planted = "ghp-not-a-real-token-PLACEHOLDER"
    dials: list[str] = []

    async def leaky(*_args: object, **_kwargs: object) -> _PROBE_RESULT:
        dials.append("dial")
        raise OSError(f"connection reset dialing with Bearer {planted}")

    monkeypatch.setattr(capability_module, "_probe_server_once", leaky)
    boot = ConnectorCapabilityFailure(
        connector="github",
        credential_names=(),
        reason="probe_failed",
        attempts=3,
    )

    with caplog.at_level(logging.WARNING):
        still = anyio.run(reprobe, (boot,), _derived(), {})

    assert dials == ["dial"]
    assert still == (boot,)
    assert any("error_class=OSError" in record.getMessage() for record in caplog.records)
    for record in caplog.records:
        assert planted not in record.getMessage()
        assert "_ProbeFailure" not in record.getMessage()
