"""Polling controls traverse strict direct and candidate MCP response parsing."""

from __future__ import annotations

import json
import subprocess
from typing import Any

import pytest

from examples.tests import test_sre_bot_observability_runtime as runtime


class Clock:
    def __init__(self) -> None:
        self.elapsed = 0.0

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.elapsed += seconds


@pytest.fixture
def stack() -> runtime.RuntimeStack:
    return runtime.RuntimeStack(
        suffix="example", front_network="front", back_network="back",
        invalid_network="invalid", tempo_probe_container="probe",
        candidate_runner_container="candidate", invalid_candidate_runner_container="invalid",
        tempo_container="tempo", tempo_url="http://example.com",
        tempo_envelope=runtime.TempoEnvelope(None, None), grafana_url="http://example.com",
        loki_url="http://example.com", collector_url="http://example.com",
        token="example-valid-token", invalid_token="example-invalid-token",
        containers=[], volumes=[], tempo_connector_image="example:tempo",
        candidate_runner_image="example:runner",
    )


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    value = Clock()
    monkeypatch.setattr(runtime.time, "monotonic", value.monotonic)
    monkeypatch.setattr(runtime.time, "sleep", value.sleep)
    return value


def _result(text: str, *, error: bool = False) -> dict[str, Any]:
    # MCP CallToolResult carries content and isError independently:
    # https://modelcontextprotocol.io/specification/2025-06-18/server/tools
    return {"content": [{"type": "text", "text": text}], "isError": error}


def _evidence(response: dict[str, Any]) -> dict[str, Any]:
    # This is the real CANDIDATE_RUNNER_PROBE output shape. Retain capability,
    # catalog and environment evidence rather than manufacturing only text.
    return {
        "catalog": {}, "environment_names": [],
        "capability": {"complete": True, "failures": [], "connector_failures": [],
                       "observed_tools": ["query"]},
        "calls": [response],
    }


def _replay(
    monkeypatch: pytest.MonkeyPatch, responses: list[dict[str, Any]], *, candidate: bool,
) -> list[float]:
    budgets: list[float] = []

    def docker(*arguments: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        budgets.append(kwargs["timeout"])
        response = responses[min(len(budgets) - 1, len(responses) - 1)]
        body = _evidence(response) if candidate else {"jsonrpc": "2.0", "id": 2,
                                                       "result": response}
        return subprocess.CompletedProcess(arguments, 0, stdout=json.dumps(body), stderr="")

    monkeypatch.setattr(runtime, "_docker", docker)
    return budgets


@pytest.mark.parametrize("candidate", [False, True])
def test_connector_poll_retries_query_error_then_waits_for_marker(
    monkeypatch: pytest.MonkeyPatch, stack: runtime.RuntimeStack, clock: Clock, candidate: bool,
) -> None:
    budgets = _replay(monkeypatch, [
        _result("query error: backend unavailable HTTP 503", error=True),
        _result('{"traces": []}'),
        _result('{"traces": [{"name": "example-marker"}]}'),
    ], candidate=candidate)
    poll = stack.eventually_candidate_call_text if candidate else stack.eventually_call_text

    result = poll("tempo", "query", {}, "example-marker", timeout=3)

    assert "example-marker" in result
    assert len(budgets) == 3
    assert budgets == [3, 2, 1]
    assert clock.elapsed == 2


@pytest.mark.parametrize("candidate", [False, True])
def test_connector_poll_retries_connection_refused_then_waits_for_marker(
    monkeypatch: pytest.MonkeyPatch, stack: runtime.RuntimeStack, clock: Clock,
    candidate: bool,
) -> None:
    # The shipped Tempo connector wraps httpx transport failures with this
    # prefix: examples/sre-bot/connectors/tempo/server.py::_proxy.
    budgets = _replay(monkeypatch, [
        _result("could not reach Grafana: [Errno 111] Connection refused", error=True),
        _result('{"traces": []}'),
        _result('{"traces": [{"name": "example-marker"}]}'),
    ], candidate=candidate)
    poll = stack.eventually_candidate_call_text if candidate else stack.eventually_call_text

    result = poll("tempo", "query", {}, "example-marker", timeout=3)

    assert "example-marker" in result
    assert len(budgets) == 3
    assert budgets == [3, 2, 1]
    assert clock.elapsed == 2


@pytest.mark.parametrize("candidate", [False, True])
def test_connector_poll_persistent_query_error_obeys_deadline(
    monkeypatch: pytest.MonkeyPatch, stack: runtime.RuntimeStack, clock: Clock, candidate: bool,
) -> None:
    budgets = _replay(monkeypatch, [
        _result("query error: backend unavailable HTTP 503", error=True),
    ], candidate=candidate)
    poll = stack.eventually_candidate_call_text if candidate else stack.eventually_call_text

    with pytest.raises(AssertionError):
        poll("tempo", "query", {}, "example-marker", timeout=2.5)

    assert len(budgets) == 3
    assert budgets == [2.5, 1.5, 0.5]
    assert clock.elapsed == 2.5


@pytest.mark.parametrize("candidate", [False, True])
@pytest.mark.parametrize("status", [401, 403])
def test_connector_poll_auth_error_does_not_enter_retry_loop(
    monkeypatch: pytest.MonkeyPatch, stack: runtime.RuntimeStack, clock: Clock,
    candidate: bool, status: int,
) -> None:
    budgets = _replay(monkeypatch, [
        _result(f"HTTP {status} authorization refused", error=True),
        _result("example-marker"),
    ], candidate=candidate)
    poll = stack.eventually_candidate_call_text if candidate else stack.eventually_call_text

    with pytest.raises(AssertionError):
        poll("tempo", "query", {}, "example-marker", timeout=3)

    assert len(budgets) == 1
    assert clock.elapsed == 0


@pytest.mark.parametrize("violation", ["token", "incomplete", "capability_failure"])
def test_candidate_poll_preserves_security_and_capability_guards(
    monkeypatch: pytest.MonkeyPatch, stack: runtime.RuntimeStack, clock: Clock, violation: str,
) -> None:
    evidence = _evidence(_result("example-marker"))
    if violation == "token":
        evidence["catalog"] = {"authorization": stack.token}
    elif violation == "incomplete":
        evidence["capability"]["complete"] = False
    else:
        evidence["capability"]["failures"] = ["missing credential"]
    calls: list[float] = []

    def docker(*arguments: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        calls.append(kwargs["timeout"])
        return subprocess.CompletedProcess(arguments, 0, stdout=json.dumps(evidence), stderr="")

    monkeypatch.setattr(runtime, "_docker", docker)

    with pytest.raises(AssertionError):
        stack.eventually_candidate_call_text("tempo", "query", {}, "example-marker", timeout=3)

    assert len(calls) == 1
    assert clock.elapsed == 0
