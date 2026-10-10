"""The worker's client for the action ledger (ADR-0117).

The worker has no database of its own, so recording what a turn did to the world
is an HTTP call to the platform API, exactly as creating an approval is. This
covers the two calls one side-effecting tool call produces and the failure that
must not be swallowed.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from aci_protocol import SideEffectFlag
from curie_worker.actions import ActionBackendError, ActionClient

pytestmark = pytest.mark.anyio


def _client(handler: Any) -> tuple[httpx.AsyncClient, list[dict[str, Any]]]:
    seen: list[dict[str, Any]] = []

    def wrapped(request: httpx.Request) -> httpx.Response:
        seen.append(
            {
                "method": request.method,
                "path": request.url.path,
                "body": json.loads(request.content) if request.content else None,
                "api_key": request.headers.get("X-API-Key"),
            }
        )
        return handler(request)

    return httpx.AsyncClient(transport=httpx.MockTransport(wrapped)), seen


async def test_recording_a_call_sends_its_arguments_and_a_dedupe_key() -> None:
    client, seen = _client(lambda _r: httpx.Response(201, json={"id": "a1", "status": "pending"}))

    async with client:
        recorded = await ActionClient(api_base_url="http://api", api_key="k", client=client).record(
            SideEffectFlag(
                tool="scale_deployment",
                call_id="toolu_01",
                arguments={"replicas": 10},
                detail="non-idempotent tool executed",
            ),
            event_id="event-1",
            conversation_id="C1",
            agent_id=None,
        )

    assert recorded.id == "a1"
    assert seen[0]["method"] == "POST"
    assert seen[0]["path"] == "/actions"
    assert seen[0]["api_key"] == "k"
    # The event id AND the call id: one turn can call the same tool twice, and a
    # redelivery of that turn must adopt both rows rather than collapse them.
    assert seen[0]["body"]["dedupe_key"] == "event-1:toolu_01"
    assert seen[0]["body"]["arguments"] == {"replicas": 10}


async def test_a_redelivered_record_is_not_an_error() -> None:
    """200 is the API's idempotent replay; only a non-2xx is a failure."""

    client, _ = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "pending"}))

    async with client:
        recorded = await ActionClient(api_base_url="http://api", api_key="k", client=client).record(
            SideEffectFlag(tool="t", call_id="c", arguments={}),
            event_id="e",
            conversation_id="C1",
            agent_id=None,
        )

    assert recorded.id == "a1"


async def test_completing_a_call_forwards_the_prior_state_a_restore_replays() -> None:
    """`prior` and `target` come out of the CONNECTOR's reply, not the arguments.

    No function of `replicas=10` can produce the replica count from before the
    call. That is why the reply is the declaration (ADR-0117 decision 1).
    """

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(
                tool="scale_deployment",
                call_id="toolu_01",
                failed=False,
                result={
                    "ok": True,
                    "prior": {"spec": {"replicas": 3}},
                    "post": {"spec": {"replicas": 10}},
                    "target": {"kind": "Deployment", "name": "api"},
                },
                detail="non-idempotent tool completed",
            ),
        )

    assert seen[0]["path"] == "/actions/a1/complete"
    assert seen[0]["body"]["prior_state"] == {"spec": {"replicas": 3}}
    # What the call LEFT, which is what a conflict check compares the live
    # resource against. It cannot be derived from `replicas=10`: a PATCH's
    # result is not its request body.
    assert seen[0]["body"]["post_state"] == {"spec": {"replicas": 10}}
    assert seen[0]["body"]["target"] == {"kind": "Deployment", "name": "api"}
    assert seen[0]["body"]["failed"] is False


async def test_a_prose_reply_completes_with_nothing_to_restore() -> None:
    """The connector answered in a sentence, so the result is absent entirely."""

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(tool="restart", call_id="c", failed=False, detail="restarted"),
        )

    assert seen[0]["body"]["result"] is None
    assert seen[0]["body"]["prior_state"] is None
    assert seen[0]["body"]["post_state"] is None
    assert seen[0]["body"]["target"] is None


async def test_a_structured_reply_without_a_prior_is_still_not_undoable() -> None:
    """A connector may return JSON and still not report what it overwrote."""

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(tool="t", call_id="c", failed=False, result={"ok": True}),
        )

    assert seen[0]["body"]["result"] == {"ok": True}
    assert seen[0]["body"]["prior_state"] is None


async def test_a_redacted_snapshot_never_produces_an_undoable_action() -> None:
    """A scrubbed prior state is not a restore; it is a placeholder a restore would write.

    The runner redacts a held secret or a token-shaped value inside the result
    before the frame leaves the sandbox, and says so. Forwarding that snapshot
    would give the ledger a row that looks undoable and whose undo sets the
    resource's value to ``[REDACTED:...]`` (#1873). With neither state recorded,
    the row is not undoable and the receipt says so.
    """

    client, seen = _client(lambda _r: httpx.Response(200, json={"id": "a1", "status": "succeeded"}))
    result = {
        "summary": "rotated acme-api's token",
        "prior": {"env": [{"name": "API_TOKEN", "value": "[REDACTED:held_secret]"}]},
        "post": {"env": [{"name": "API_TOKEN", "value": "[REDACTED:held_secret]"}]},
        "target": {"kind": "Deployment", "name": "acme-api"},
    }

    async with client:
        await ActionClient(api_base_url="http://api", api_key="k", client=client).complete(
            "a1",
            SideEffectFlag(tool="set_env", call_id="c", failed=False, result=result, redacted=True),
        )

    body = seen[0]["body"]
    assert body["prior_state"] is None
    assert body["post_state"] is None
    # What the call acted on and what it said stay on the record: they are what a
    # person reads, and neither is replayed.
    assert body["target"] == {"kind": "Deployment", "name": "acme-api"}
    assert body["result"] == result
    assert body["failed"] is False


async def test_a_refused_write_is_raised_not_swallowed() -> None:
    """A record the platform failed to write is a hole in its account of a change.

    The same branch already fails the turn when the no-retry marker cannot be
    persisted; losing the record of WHAT changed is not the lesser failure.
    """

    client, _ = _client(lambda _r: httpx.Response(500, text="nope"))

    async with client:
        with pytest.raises(ActionBackendError):
            await ActionClient(api_base_url="http://api", api_key="k", client=client).record(
                SideEffectFlag(tool="t", call_id="c"),
                event_id="e",
                conversation_id="C1",
                agent_id=None,
                budget_s=0,
            )


@pytest.fixture
def retry_clock(monkeypatch):
    from curie_worker import api_retry

    class Clock:
        now = 0.0
        sleeps: list[float]

        def __init__(self) -> None:
            self.sleeps = []

        def __call__(self) -> float:
            return self.now

        async def sleep(self, delay: float) -> None:
            self.sleeps.append(delay)
            self.now += delay

    clock = Clock()
    monkeypatch.setattr(api_retry, "_clock", clock)
    monkeypatch.setattr(api_retry, "_sleep", clock.sleep)
    return clock


# The replay contract is defined by routers/actions.py::create_action and complete_action.
@pytest.mark.parametrize("operation", ["record", "complete"])
async def test_ledger_transient_writes_replay_the_same_payload(operation, retry_clock) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if len(seen) == 1:
            error = httpx.ConnectError if operation == "record" else httpx.ReadTimeout
            raise error("API restarting", request=request)
        if len(seen) == 2:
            return httpx.Response(503)
        return httpx.Response(
            201 if operation == "record" else 200, json={"id": "a1", "status": "succeeded"}
        )

    client, seen = _client(handler)
    async with client:
        actions = ActionClient(api_base_url="http://api", api_key="k", client=client)
        frame = SideEffectFlag(tool="deploy", call_id="call-1", arguments={"replicas": 2})
        if operation == "record":
            result = await actions.record(
                frame, event_id="event-1", conversation_id="thread", agent_id=None
            )
            assert result.id == "a1"
        else:
            await actions.complete("a1", frame)
    assert len(seen) == 3
    assert all(request["body"] == seen[0]["body"] for request in seen)
    if operation == "record":
        assert [request["body"]["dedupe_key"] for request in seen] == ["event-1:call-1"] * 3
    assert retry_clock.sleeps == [0.5, 1.0]


@pytest.mark.parametrize("status", [400, 404, 409, 422])
@pytest.mark.parametrize("operation", ["record", "complete"])
async def test_ledger_refusals_are_never_retried(status, operation, retry_clock) -> None:
    client, seen = _client(lambda _request: httpx.Response(status))
    async with client:
        actions = ActionClient(api_base_url="http://api", api_key="k", client=client)
        frame = SideEffectFlag(tool="deploy", call_id="call-1")
        with pytest.raises(ActionBackendError):
            if operation == "record":
                await actions.record(
                    frame, event_id="event", conversation_id="thread", agent_id=None
                )
            else:
                await actions.complete("a1", frame)
    assert len(seen) == 1
    assert retry_clock.sleeps == []


async def test_ledger_transport_failure_exhausts_only_the_requested_window(retry_clock) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("API still absent", request=request)

    client, seen = _client(handler)
    async with client:
        with pytest.raises(ActionBackendError):
            await ActionClient(api_base_url="http://api", api_key="k", client=client).record(
                SideEffectFlag(tool="deploy", call_id="call-1"),
                event_id="event",
                conversation_id="thread",
                agent_id=None,
                budget_s=3,
            )
    assert retry_clock.sleeps == [0.5, 1.0, 1.5]
    assert sum(retry_clock.sleeps) == 3
    assert retry_clock.now == 3
    assert len(seen) == 4


@pytest.mark.parametrize("operation", ["record", "complete"])
async def test_ledger_empty_timeout_message_retains_the_exception_type(
    operation, retry_clock
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("", request=request)

    client, seen = _client(handler)
    async with client:
        actions = ActionClient(api_base_url="http://api", api_key="k", client=client)
        frame = SideEffectFlag(tool="deploy", call_id="call-1")
        with pytest.raises(ActionBackendError, match="ReadTimeout") as caught:
            if operation == "record":
                await actions.record(
                    frame,
                    event_id="event",
                    conversation_id="thread",
                    agent_id=None,
                    budget_s=3,
                )
            else:
                await actions.complete("a1", frame, budget_s=3)

    assert isinstance(caught.value.__cause__, httpx.ReadTimeout)
    assert str(caught.value.__cause__) == ""
    assert retry_clock.sleeps == [0.5, 1.0, 1.5]
    assert len(seen) == 4
