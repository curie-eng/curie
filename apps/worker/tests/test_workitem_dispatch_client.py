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
