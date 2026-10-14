"""The worker's client for the thread attachment ledger (ADR 0205 decision 2, #4079).

The ledger is API-owned thread state the worker reads on every boot and appends
to once a turn's files are installed. Both routes sit under ``/v1/internal`` and
take ONLY the internal worker credential, so the client must send
``X-Curie-Worker-Token`` and nothing that could be mistaken for a platform key.

Contract pinned here (the implementer follows these names):

``curie_worker.ledger_client``
    ``LedgerUnavailable(RuntimeError)``
        Raised by ``query`` on any transport error, non-200 answer or malformed
        body, and by ``append`` once its bounded retry is spent or on a 4xx.
    ``ThreadAttachmentRef`` (frozen dataclass)
        ``file_id: str, ordinal: int, name: str, disk_name: str,
        mime_type: str | None, size_bytes: int | None, sha256: str,
        route_kind: str, route_adapter: str | None, route_identity: str``.
        ``to_wire() -> dict`` returns exactly those ten keys;
        ``ThreadAttachmentRef.from_wire(dict)`` is its inverse.
    ``ThreadAttachmentLedgerClient(*, api_base_url, worker_token, client,
    append_attempts=3, retry_backoff_s=0.2)``
        ``client`` is an ``httpx.AsyncClient``. An empty ``worker_token`` is a
        ``ValueError`` at construction, as ``PublicationCredentialClient``
        refuses one.
        ``async query(*, agent_id: str, thread_key: str)
        -> tuple[ThreadAttachmentRef, ...]`` POSTs ``/v1/internal/
        thread-attachments/query`` and returns the refs in the API's order.
        ``async append(*, agent_id, thread_key, event_id, refs) -> int`` POSTs
        ``/append`` and returns ``appended``. Transport errors and 5xx are
        retried up to ``append_attempts`` requests in total; a 4xx (409 name
        conflict, 413 thread full, 422) is not retried. An empty ``refs`` makes
        no request and returns 0. Redirects are never followed.

Round 2 (#4141):

* ``ThreadAttachmentRef`` gains ``event_id: str | None = None``: the query
  answers each row with the event that recorded it, and ``from_wire`` reads it
  when present. ``to_wire`` never sends it (the append names the event once,
  and the API refuses an extra ref field with 422).
* ``LedgerNotDeployed(LedgerUnavailable)``: ``query`` answered 404, an API that
  does not have the routes yet. The kernel treats it as "no ledger".
* A refused append's ``LedgerUnavailable`` message names the API's error code
  (for example ``thread_attachment.name_mismatch``), so the WARNING says why.

Only the API is faked (``httpx.MockTransport``); nothing else is mocked.
"""

from __future__ import annotations

import asyncio
import importlib
import json
from typing import Any

import httpx
import pytest

API = "http://curie-api:8000"
TOKEN = "worker-token-value"
AGENT = "11111111-1111-4111-8111-111111111111"
THREAD = "slack:C1:1700000000.000100"
SHA = "a" * 64


@pytest.fixture
def ledger() -> Any:
    return importlib.import_module("curie_worker.ledger_client")


def _ref(module: Any, file_id: str = "F1", ordinal: int = 0, **overrides: Any) -> Any:
    values: dict[str, Any] = {
        "file_id": file_id,
        "ordinal": ordinal,
        "name": f"{file_id}.pdf",
        "disk_name": f"{file_id}.pdf",
        "mime_type": "application/pdf",
        "size_bytes": 1024,
        "sha256": SHA,
        "route_kind": "slack",
        "route_adapter": None,
        "route_identity": "default",
    }
    values.update(overrides)
    return module.ThreadAttachmentRef(**values)


def _wire(file_id: str = "F1", ordinal: int = 0) -> dict[str, Any]:
    return {
        "file_id": file_id,
        "ordinal": ordinal,
        "name": f"{file_id}.pdf",
        "disk_name": f"{file_id}.pdf",
        "mime_type": "application/pdf",
        "size_bytes": 1024,
        "sha256": SHA,
        "route_kind": "slack",
        "route_adapter": None,
        "route_identity": "default",
    }


def _client(module: Any, handler: Any, **kwargs: Any) -> tuple[Any, httpx.AsyncClient]:
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return (
        module.ThreadAttachmentLedgerClient(
            api_base_url=f"{API}/",
            worker_token=TOKEN,
            client=http,
            **kwargs,
        ),
        http,
    )


def test_a_ref_round_trips_through_exactly_the_ten_wire_fields(ledger: Any) -> None:
    ref = _ref(ledger, route_kind="email", route_adapter="agentmail", size_bytes=None)

    wire = ref.to_wire()

    assert set(wire) == {
        "file_id",
        "ordinal",
        "name",
        "disk_name",
        "mime_type",
        "size_bytes",
        "sha256",
        "route_kind",
        "route_adapter",
        "route_identity",
    }, "the ledger never records an endpoint, a URL or bytes"
    assert ledger.ThreadAttachmentRef.from_wire(wire) == ref


def test_query_sends_the_worker_token_and_returns_refs_in_api_order(ledger: Any) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"refs": [_wire("F2", 0), _wire("F1", 1)]})

    async def go() -> Any:
        client, http = _client(ledger, handler)
        async with http:
            return await client.query(agent_id=AGENT, thread_key=THREAD)

    refs = asyncio.run(go())

    assert [ref.file_id for ref in refs] == ["F2", "F1"]
    assert isinstance(refs[0], ledger.ThreadAttachmentRef)
    assert len(seen) == 1
    request = seen[0]
    assert request.method == "POST"
    assert str(request.url) == f"{API}/v1/internal/thread-attachments/query"
    assert request.headers["X-Curie-Worker-Token"] == TOKEN
    assert "X-API-Key" not in request.headers
    assert json.loads(request.content) == {"agent_id": AGENT, "thread_key": THREAD}


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(500, json={"detail": "boom"}),
        httpx.Response(401, json={"detail": "unauthorized"}),
        httpx.Response(307, headers={"Location": "http://elsewhere/"}),
        httpx.Response(200, json={"rows": []}),
        httpx.Response(200, content=b"not json"),
        httpx.Response(200, json={"refs": [{"file_id": "F1"}]}),
    ],
    ids=["5xx", "401", "redirect", "wrong-envelope", "not-json", "short-ref"],
)
def test_query_raises_ledger_unavailable_on_any_unusable_answer(
    ledger: Any, response: httpx.Response
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return response

    async def go() -> None:
        client, http = _client(ledger, handler)
        async with http:
            await client.query(agent_id=AGENT, thread_key=THREAD)

    with pytest.raises(ledger.LedgerUnavailable):
        asyncio.run(go())


def test_query_raises_ledger_unavailable_when_the_api_is_unreachable(ledger: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async def go() -> None:
        client, http = _client(ledger, handler)
        async with http:
            await client.query(agent_id=AGENT, thread_key=THREAD)

    with pytest.raises(ledger.LedgerUnavailable):
        asyncio.run(go())


def test_append_posts_the_event_and_wire_refs_and_returns_the_count(ledger: Any) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"appended": 2})

    async def go() -> int:
        client, http = _client(ledger, handler, retry_backoff_s=0.0)
        async with http:
            return await client.append(
                agent_id=AGENT,
                thread_key=THREAD,
                event_id="ev-1",
                refs=[_ref(ledger, "F1", 0), _ref(ledger, "F2", 1)],
            )

    assert asyncio.run(go()) == 2
    assert len(seen) == 1
    request = seen[0]
    assert str(request.url) == f"{API}/v1/internal/thread-attachments/append"
    assert request.headers["X-Curie-Worker-Token"] == TOKEN
    assert json.loads(request.content) == {
        "agent_id": AGENT,
        "thread_key": THREAD,
        "event_id": "ev-1",
        "refs": [_wire("F1", 0), _wire("F2", 1)],
    }


def test_append_of_nothing_makes_no_request(ledger: Any) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("an empty append reached the API")

    async def go() -> int:
        client, http = _client(ledger, handler)
        async with http:
            return await client.append(
                agent_id=AGENT, thread_key=THREAD, event_id="ev-1", refs=[]
            )

    assert asyncio.run(go()) == 0


def test_append_retries_a_transient_failure_within_its_bound(ledger: Any) -> None:
    answers = [httpx.Response(503), httpx.Response(200, json={"appended": 1})]
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return answers.pop(0)

    async def go() -> int:
        client, http = _client(ledger, handler, append_attempts=3, retry_backoff_s=0.0)
        async with http:
            return await client.append(
                agent_id=AGENT, thread_key=THREAD, event_id="ev-1", refs=[_ref(ledger)]
            )

    assert asyncio.run(go()) == 1
    assert len(calls) == 2


def test_append_gives_up_after_its_bound_with_ledger_unavailable(ledger: Any) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if len(calls) % 2:
            raise httpx.ReadTimeout("slow", request=request)
        return httpx.Response(502)

    async def go() -> None:
        client, http = _client(ledger, handler, append_attempts=3, retry_backoff_s=0.0)
        async with http:
            await client.append(
                agent_id=AGENT, thread_key=THREAD, event_id="ev-1", refs=[_ref(ledger)]
            )

    with pytest.raises(ledger.LedgerUnavailable):
        asyncio.run(go())
    assert len(calls) == 3, "the retry is bounded at append_attempts requests"


@pytest.mark.parametrize("status", [409, 413, 422])
def test_append_does_not_retry_a_refusal(ledger: Any, status: int) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(status, json={"detail": {"code": "refused"}})

    async def go() -> None:
        client, http = _client(ledger, handler, append_attempts=3, retry_backoff_s=0.0)
        async with http:
            await client.append(
                agent_id=AGENT, thread_key=THREAD, event_id="ev-1", refs=[_ref(ledger)]
            )

    with pytest.raises(ledger.LedgerUnavailable):
        asyncio.run(go())
    assert len(calls) == 1


def test_the_client_refuses_to_exist_without_the_worker_token(ledger: Any) -> None:
    async def go() -> None:
        async with httpx.AsyncClient() as http:
            ledger.ThreadAttachmentLedgerClient(
                api_base_url=API, worker_token="", client=http
            )

    with pytest.raises(ValueError):
        asyncio.run(go())


# --- round 2 (#4141) -------------------------------------------------------------


def test_query_rows_carry_the_event_that_recorded_them_and_append_never_sends_it(
    ledger: Any,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "refs": [
                    {**_wire("F1", 0), "event_id": "ev-1"},
                    {**_wire("F2", 0), "event_id": "ev-2"},
                ]
            },
        )

    async def go() -> Any:
        client, http = _client(ledger, handler)
        async with http:
            return await client.query(agent_id=AGENT, thread_key=THREAD)

    refs = asyncio.run(go())

    assert [(ref.event_id, ref.file_id) for ref in refs] == [("ev-1", "F1"), ("ev-2", "F2")]
    assert "event_id" not in refs[0].to_wire()
    assert _ref(ledger).event_id is None


def test_a_404_query_means_the_api_has_no_ledger_yet(ledger: Any) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "Not Found"})

    async def go() -> None:
        client, http = _client(ledger, handler)
        async with http:
            await client.query(agent_id=AGENT, thread_key=THREAD)

    with pytest.raises(ledger.LedgerNotDeployed) as raised:
        asyncio.run(go())
    assert isinstance(raised.value, ledger.LedgerUnavailable)


@pytest.mark.parametrize("status", [500, 401])
def test_other_query_failures_are_not_mistaken_for_a_missing_ledger(
    ledger: Any, status: int
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"detail": "nope"})

    async def go() -> None:
        client, http = _client(ledger, handler)
        async with http:
            await client.query(agent_id=AGENT, thread_key=THREAD)

    with pytest.raises(ledger.LedgerUnavailable) as raised:
        asyncio.run(go())
    assert not isinstance(raised.value, ledger.LedgerNotDeployed)


def test_a_name_mismatch_append_names_the_code_and_is_not_retried(ledger: Any) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(
            409, json={"detail": {"code": "thread_attachment.name_mismatch", "message": "x"}}
        )

    async def go() -> None:
        client, http = _client(ledger, handler, append_attempts=3, retry_backoff_s=0.0)
        async with http:
            await client.append(
                agent_id=AGENT, thread_key=THREAD, event_id="ev-1", refs=[_ref(ledger)]
            )

    with pytest.raises(ledger.LedgerUnavailable) as raised:
        asyncio.run(go())
    assert "thread_attachment.name_mismatch" in str(raised.value)
    assert len(calls) == 1


@pytest.mark.parametrize(
    "body",
    [{"detail": {"code": "thread_attachment.agent_not_found", "message": "no such agent"}}],
)
def test_a_ledger_404_is_a_ledger_failure_not_a_missing_ledger(
    ledger: Any, body: dict[str, Any]
) -> None:
    """Round 4 (#4141): only a 404 that is not a ledger error (the route itself
    is missing, FastAPI's default ``{"detail": "Not Found"}``) means the API has
    no ledger. A 404 the ledger answers with its own code is an ordinary
    failure with the ADR's semantics."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json=body)

    async def go() -> None:
        client, http = _client(ledger, handler)
        async with http:
            await client.query(agent_id=AGENT, thread_key=THREAD)

    with pytest.raises(ledger.LedgerUnavailable) as raised:
        asyncio.run(go())
    assert not isinstance(raised.value, ledger.LedgerNotDeployed)
