"""A failed live Bitbucket Cloud recording still cleans up (#4401, decision 16.3).

The session runs against `BitbucketCloudFake` as if live. The recorded session
is capped by `RecordingTransport`; the cleanup after a failure goes through
`AllowanceTransport`, its own reserved allowance, so an exhausted budget does
not block it, and the branch is deleted even when the decline fails.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AsyncExitStack

import httpx
import pytest
from curie_api.forges.types import Credential
from forge_fakes.bitbucket_cloud import (
    FACTORY_TOKEN,
    REVIEWER,
    REVIEWER_TOKEN,
    BitbucketCloudFake,
)
from forge_fakes.bitbucket_cloud_record import (
    PLACEHOLDER_SLUG,
    PLACEHOLDER_WORKSPACE,
    Observed,
    Session,
    adapter_for,
    cleanup,
    drive,
    side_client,
)
from forge_fakes.bitbucket_cloud_replay import (
    CLEANUP_REQUESTS,
    AllowanceTransport,
    RecordingTransport,
)

BRANCH = "curie-record-20261012T000000Z"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def fake() -> Iterator[BitbucketCloudFake]:
    fake = BitbucketCloudFake()
    fake.add_user(REVIEWER, REVIEWER_TOKEN)
    yield fake


class FakeGit:
    """The git steps, as pushes to the fake."""

    def __init__(self, fake: BitbucketCloudFake) -> None:
        self.fake = fake

    async def seed_main(self) -> None:
        raise AssertionError("the fake's main already has a commit")

    async def push_branch(self, credential: Credential, branch: str) -> str:
        return self.fake.push(branch, "Recording")


class _ExhaustMidSession(httpx.AsyncBaseTransport):
    """The fake, with the recording's budget made to run out mid-session.

    The first build status the test side posts is the last request the budget
    allows, so the session fails with its pull request open and its branch
    pushed.
    """

    def __init__(self, fake: BitbucketCloudFake) -> None:
        self.inner = httpx.ASGITransport(app=fake.app)
        self.recording: RecordingTransport | None = None

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        status = request.method == "POST" and request.url.path.endswith("/statuses/build")
        if status and self.recording is not None:
            # Counting this exchange, the budget is spent.
            self.recording.limit = min(self.recording.limit, len(self.recording.exchanges) + 1)
        return await self.inner.handle_async_request(request)


async def _drive(
    fake: BitbucketCloudFake,
    recording: RecordingTransport,
    cleanup_transport: httpx.AsyncBaseTransport,
) -> None:
    async with AsyncExitStack() as stack:
        adapter_client = await stack.enter_async_context(
            httpx.AsyncClient(transport=recording, timeout=15.0)
        )
        api = await stack.enter_async_context(
            side_client(recording, FACTORY_TOKEN, api_url=fake.api_url)
        )
        reviewer_api = await stack.enter_async_context(
            side_client(recording, REVIEWER_TOKEN, api_url=fake.api_url)
        )
        cleanup_api = await stack.enter_async_context(
            side_client(cleanup_transport, FACTORY_TOKEN, api_url=fake.api_url)
        )
        session = Session(
            PLACEHOLDER_WORKSPACE,
            PLACEHOLDER_SLUG,
            adapter_for(adapter_client, FACTORY_TOKEN, api_url=fake.api_url),
            api,
            reviewer_api,
            FakeGit(fake),
            BRANCH,
            cleanup=cleanup_api,
        )
        await drive(session)


def _exhausting(fake: BitbucketCloudFake) -> RecordingTransport:
    inner = _ExhaustMidSession(fake)
    recording = RecordingTransport(inner)
    inner.recording = recording
    return recording


@pytest.mark.anyio
async def test_an_exhausted_budget_still_declines_and_deletes(fake: BitbucketCloudFake) -> None:
    recording = _exhausting(fake)
    allowance = AllowanceTransport(httpx.ASGITransport(app=fake.app))

    with pytest.raises(AssertionError, match="at most"):
        await _drive(fake, recording, allowance)

    (pull,) = fake.pulls.values()
    assert pull.state == "DECLINED"
    assert BRANCH not in fake.branches
    # The recorded session stopped at its budget; the cleanup came from the reserve.
    assert len(recording.exchanges) == recording.limit
    assert allowance.used == CLEANUP_REQUESTS


@pytest.mark.anyio
async def test_a_failed_decline_still_deletes_the_branch_and_surfaces(
    fake: BitbucketCloudFake,
) -> None:
    recording = _exhausting(fake)
    asked: list[str] = []
    inner = httpx.ASGITransport(app=fake.app)

    async def answer(request: httpx.Request) -> httpx.Response:
        asked.append(request.method)
        if request.url.path.endswith("/decline"):
            return httpx.Response(500, json={"error": {"message": "stub"}}, request=request)
        return await inner.handle_async_request(request)

    with pytest.raises(AssertionError, match="at most") as raised:
        await _drive(fake, recording, httpx.MockTransport(answer))

    assert asked == ["POST", "DELETE"]
    assert BRANCH not in fake.branches
    (pull,) = fake.pulls.values()
    assert pull.state == "OPEN"
    assert "the cleanup after it also failed: AssertionError" in (raised.value.__notes__ or [])


@pytest.mark.anyio
async def test_cleanup_raises_the_decline_failure_after_deleting_the_branch(
    fake: BitbucketCloudFake,
) -> None:
    head = fake.push(BRANCH, "Recording")
    async with fake.client() as client:
        adapter = adapter_for(client, FACTORY_TOKEN, api_url=fake.api_url)
        pull = await adapter.open_pull_request(
            fake.repository(), head_ref=BRANCH, base_ref="main", title="Recording", body=""
        )
    asked: list[tuple[str, str]] = []

    async def answer(request: httpx.Request) -> httpx.Response:
        asked.append((request.method, request.url.path.rsplit("/", 1)[-1]))
        if request.method == "POST":
            return httpx.Response(500, request=request)
        return httpx.Response(204, request=request)

    async with side_client(httpx.MockTransport(answer), FACTORY_TOKEN) as cleanup_api:
        session = Session(
            PLACEHOLDER_WORKSPACE,
            PLACEHOLDER_SLUG,
            adapter,
            cleanup_api,
            cleanup_api,
            FakeGit(fake),
            BRANCH,
            cleanup=cleanup_api,
        )
        seen = Observed(head=head, pull=pull)
        with pytest.raises(AssertionError, match="/decline answered 500"):
            await cleanup(cleanup_api, session, seen)

    assert asked == [("POST", "decline"), ("DELETE", BRANCH)]
    assert (seen.pull_declined, seen.branch_deleted) == (False, True)
