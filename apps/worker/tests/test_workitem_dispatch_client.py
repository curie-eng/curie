"""The acquire grant carries the WorkItem repository (#2992)."""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
from curie_worker.workitem_dispatch import WorkItemDispatchClient

REQUEST_ID = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
WORK_ITEM_ID = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


def _acquire(body: dict[str, object]) -> object:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    async def go() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example-token", client=http
            )
            return await client.acquire(REQUEST_ID, owner="worker-1", generation=1)

    return asyncio.run(go())


BASE = {
    "generation": 1,
    "work_item_id": str(WORK_ITEM_ID),
    "conversation_id": "work-item-thread",
    "wait_deadline": "2026-09-23T02:00:00+00:00",
}


def test_acquire_grant_carries_the_work_item_repository() -> None:
    grant = _acquire({**BASE, "repo_full_name": "acme-corp/widgets"})
    assert grant.repo_full_name == "acme-corp/widgets"  # type: ignore[attr-defined]


@pytest.mark.parametrize("body", [BASE, {**BASE, "repo_full_name": None}])
def test_acquire_from_an_api_without_the_field_still_grants(body: dict[str, object]) -> None:
    # A worker rolled out ahead of its API replica must not fail an acquisition
    # the API has already committed.
    grant = _acquire(body)
    assert grant.repo_full_name is None  # type: ignore[attr-defined]


# --- #3076 orphan recovery verbs -------------------------------------------

from curie_worker.workitem_dispatch import WorkItemConflict  # noqa: E402


def _run(handler, call):  # type: ignore[no-untyped-def]
    async def go() -> object:
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api.example", worker_token="example-token", client=http
            )
            return await call(client)

    return asyncio.run(go())


def test_runtime_owners_parses_the_running_list() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "requests": [
                    {"request_id": str(REQUEST_ID), "runtime_owner": "w-old", "runtime_epoch": 2}
                ]
            },
        )

    owners = _run(handler, lambda c: c.runtime_owners())
    assert seen[0].method == "GET"
    assert seen[0].url.path == "/v1/internal/work-items/runtime-owners"
    assert seen[0].headers["X-Curie-Worker-Token"] == "example-token"
    assert [(o.request_id, o.runtime_owner, o.runtime_epoch) for o in owners] == [  # type: ignore[attr-defined]
        (REQUEST_ID, "w-old", 2)
    ]


def test_declare_owner_lost_posts_owner_and_epoch() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, json={"status": "cancellation_requested", "terminal_cause": "owner_lost"}
        )

    _run(handler, lambda c: c.declare_owner_lost(REQUEST_ID, owner="w-old", runtime_epoch=2))
    assert seen[0].method == "POST"
    assert seen[0].url.path == f"/v1/internal/work-items/requests/{REQUEST_ID}/owner-lost"
    import json

    assert json.loads(seen[0].content) == {"owner": "w-old", "runtime_epoch": 2}


def test_declare_owner_lost_raises_the_conflict_code_on_409() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": {"code": "stale_owner"}})

    with pytest.raises(WorkItemConflict) as caught:
        _run(handler, lambda c: c.declare_owner_lost(REQUEST_ID, owner="w-old", runtime_epoch=2))
    assert caught.value.code == "stale_owner"


def test_runtime_owners_sends_the_after_cursor() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"requests": []})

    _run(handler, lambda c: c.runtime_owners())
    _run(handler, lambda c: c.runtime_owners(after=REQUEST_ID))
    assert "after" not in seen[0].url.params
    assert seen[1].url.params["after"] == str(REQUEST_ID)
