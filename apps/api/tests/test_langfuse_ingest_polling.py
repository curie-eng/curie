"""Focused controls for delayed exact trace reads after real export."""

from __future__ import annotations

from typing import Any, cast

import httpx
import pytest
from fastapi.testclient import TestClient

from apps.api.tests import test_langfuse_integration as integration


class Clock:
    def __init__(self) -> None:
        self.elapsed = 0.0

    def monotonic(self) -> float:
        return self.elapsed

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.elapsed += seconds


class TraceClient:
    def __init__(self, responses: list[tuple[int, dict[str, Any]]]) -> None:
        self.responses = responses
        self.reads: list[tuple[str, dict[str, str]]] = []

    def get(self, path: str, *, headers: dict[str, str], **kwargs: Any) -> httpx.Response:
        self.reads.append((path, headers))
        index = min(len(self.reads) - 1, len(self.responses) - 1)
        status, body = self.responses[index]
        return httpx.Response(
            status,
            json=body,
            request=httpx.Request("GET", f"http://example.com{path}"),
        )


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    value = Clock()
    monkeypatch.setattr(integration.time, "monotonic", value.monotonic)
    monkeypatch.setattr(integration.time, "sleep", value.sleep)
    return value


def test_exact_trace_poll_waits_for_missing_then_incomplete_then_correlated_approval(
    clock: Clock,
) -> None:
    complete = {
        "tree": [{
            "name": "curie.queue.enqueue",
            "children": [
                {"name": "agent.run", "children": []},
                {"name": "curie.reply.update", "children": []},
            ],
        }],
        "approval_decision": "approved",
    }
    reader = TraceClient([
        (404, {"detail": "trace not found"}),
        (200, {"tree": [{"name": "curie.queue.enqueue", "children": []}],
               "approval_decision": None}),
        (200, complete),
    ])
    trace_id = "a" * 32
    headers = {"X-API-Key": "example-key"}

    def correlated(body: dict[str, Any]) -> bool:
        return body["approval_decision"] == "approved" and {
            child["name"] for child in body["tree"][0]["children"]
        } == {"agent.run", "curie.reply.update"}

    result = integration._poll_trace(
        cast(TestClient, reader), headers, trace_id, correlated, timeout=3, interval=1,
    )

    assert result == complete
    assert reader.reads == [(f"/langfuse/traces/{trace_id}", headers)] * 3
    assert clock.elapsed == 2


def test_exact_trace_poll_permanent_absence_fails_at_deadline(clock: Clock) -> None:
    reader = TraceClient([(404, {"detail": "trace not found"})])

    with pytest.raises(AssertionError):
        integration._poll_trace(
            cast(TestClient, reader), {}, "a" * 32, lambda body: bool(body["tree"]),
            timeout=2.5, interval=1,
        )

    assert len(reader.reads) == 3
    assert clock.elapsed == 2.5


@pytest.mark.parametrize("status", [401, 403])
def test_exact_trace_poll_authorization_failure_is_immediate(
    clock: Clock, status: int,
) -> None:
    # The public API requires Basic Auth. Invalid authentication is 401:
    # https://api.reference.langfuse.com/api-reference/authentication
    reader = TraceClient([(status, {"detail": "authorization refused"})])

    with pytest.raises(httpx.HTTPStatusError) as error:
        integration._poll_trace(
            cast(TestClient, reader), {}, "a" * 32, lambda body: True,
            timeout=10, interval=1,
        )

    assert error.value.response.status_code == status
    assert len(reader.reads) == 1
    assert clock.elapsed == 0


@pytest.mark.parametrize("status", [502, 503, 504])
def test_exact_trace_poll_recovers_from_temporary_upstream_failure(
    clock: Clock, status: int,
) -> None:
    reader = TraceClient([(status, {"detail": "upstream not ready"}),
                          (200, {"tree": [{"name": "agent.run"}]})])

    result = integration._poll_trace(
        cast(TestClient, reader), {}, "a" * 32, lambda body: bool(body["tree"]),
        timeout=2, interval=1,
    )

    assert result["tree"] == [{"name": "agent.run"}]
    assert len(reader.reads) == 2
    assert clock.elapsed == 1
