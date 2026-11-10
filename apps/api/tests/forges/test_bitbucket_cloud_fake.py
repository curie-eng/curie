"""The Bitbucket Cloud adapter's build status mapping against `BitbucketCloudFake` (#4401).

Bitbucket Cloud reports a stopped or cancelled build as ``STOPPED`` or
``CANCELLED``; both read as cancelled, so a cancelled head rolls up red instead
of waiting forever as pending (decision 9.2). ``INPROGRESS`` stays pending.
"""

from __future__ import annotations

from typing import Any

import pytest
from curie_api.forges.errors import Unavailable
from curie_api.forges.types import CheckSource, CheckState, RollupState
from forge_fakes.bitbucket_cloud import (
    API_URL,
    FACTORY_TOKEN,
    MAIN,
    SLUG,
    WORKSPACE,
    BitbucketCloudFake,
)
from forge_fakes.bitbucket_cloud_record import adapter_for


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def fake() -> BitbucketCloudFake:
    return BitbucketCloudFake()


@pytest.fixture
def adapter(fake: BitbucketCloudFake) -> Any:
    return adapter_for(fake.client(), FACTORY_TOKEN)


@pytest.mark.anyio
async def test_bitbucket_cloud_build_statuses_on_the_head(
    fake: BitbucketCloudFake, adapter: Any
) -> None:
    head = fake.push("curie/ci", "work on curie/ci")
    for key, word in [
        ("unit", "SUCCESSFUL"),
        ("lint", "FAILED"),
        ("e2e", "INPROGRESS"),
        ("docs", "CANCELLED"),
        ("scan", "STOPPED"),
    ]:
        fake.post_status(head, key, word)

    rollup = await adapter.observe_ci(fake.repository(), head)

    by_key = {c.key: c for c in rollup.checks}
    assert {k: c.state for k, c in by_key.items()} == {
        "status:unit": CheckState.SUCCESS,
        "status:lint": CheckState.FAILURE,
        "status:e2e": CheckState.PENDING,
        "status:docs": CheckState.CANCELLED,
        "status:scan": CheckState.CANCELLED,
    }
    assert by_key["status:docs"].reported_state == "CANCELLED"
    assert all(c.source is CheckSource.STATUS and c.head_sha == head for c in rollup.checks)


@pytest.mark.anyio
@pytest.mark.parametrize("word", ["CANCELLED", "STOPPED"])
async def test_bitbucket_cloud_a_cancelled_head_rolls_up_failing(
    word: str, fake: BitbucketCloudFake, adapter: Any
) -> None:
    head = fake.push("curie/cancelled", "work on curie/cancelled")
    fake.post_status(head, "unit", "SUCCESSFUL")
    fake.post_status(head, "e2e", word)

    rollup = await adapter.observe_ci(fake.repository(), head)

    assert rollup.state is RollupState.FAILURE


@pytest.mark.anyio
async def test_bitbucket_cloud_an_in_progress_head_stays_pending(
    fake: BitbucketCloudFake, adapter: Any
) -> None:
    head = fake.push("curie/running", "work on curie/running")
    fake.post_status(head, "unit", "SUCCESSFUL")
    fake.post_status(head, "e2e", "INPROGRESS")

    rollup = await adapter.observe_ci(fake.repository(), head)

    assert rollup.state is RollupState.PENDING
    assert {c.key: c.state for c in rollup.checks}["status:e2e"] is CheckState.PENDING


# Ancestry (#5094): one merge-base request answers it.


def _ancestry_graph(fake: BitbucketCloudFake) -> dict[str, str]:
    """A side branch merged back into the mainline, and a fork off the root."""

    root = fake.branches[MAIN]
    side = fake.add_commit((root,), "side")
    mainline = fake.add_commit((root,), "mainline")
    merge = fake.add_commit((mainline, side), "merge")
    fork = fake.add_commit((root,), "fork")
    return {"root": root, "side": side, "mainline": mainline, "merge": merge, "fork": fork}


def _merge_base_paths(fake: BitbucketCloudFake) -> list[str]:
    return [seen.path for seen in fake.requests if "/merge-base/" in seen.path]


@pytest.mark.anyio
async def test_bitbucket_cloud_is_ancestor_fake_replays_the_merge_base_commit(
    fake: BitbucketCloudFake,
) -> None:
    graph = _ancestry_graph(fake)
    orphan = fake.add_commit((), "orphan")
    base = f"{API_URL}/repositories/{WORKSPACE}/{SLUG}/merge-base"
    headers = {"Authorization": f"Bearer {FACTORY_TOKEN}"}

    async with fake.client() as client:
        found = await client.get(f"{base}/{graph['side']}..{graph['merge']}", headers=headers)
        forked = await client.get(f"{base}/{graph['fork']}..{graph['merge']}", headers=headers)
        unrelated = await client.get(f"{base}/{orphan}..{graph['merge']}", headers=headers)
        unknown = await client.get(f"{base}/{'f' * 40}..{graph['merge']}", headers=headers)

    assert found.status_code == 200
    assert found.json()["type"] == "commit"
    assert found.json()["hash"] == graph["side"]
    assert forked.json()["hash"] == graph["root"]
    assert (unrelated.status_code, unknown.status_code) == (404, 404)


@pytest.mark.anyio
async def test_bitbucket_cloud_is_ancestor_of_identical_shas_is_true_without_a_request(
    fake: BitbucketCloudFake, adapter: Any
) -> None:
    graph = _ancestry_graph(fake)
    fake.requests.clear()

    assert await adapter.is_ancestor(fake.repository(), graph["merge"], graph["merge"])
    assert _merge_base_paths(fake) == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("ancestor", "descendant", "expected"),
    [
        ("root", "mainline", True),
        ("side", "merge", True),
        ("root", "merge", True),
        ("merge", "root", False),
        ("fork", "merge", False),
    ],
    ids=("ahead", "merge-parent", "through-merge", "behind", "diverged"),
)
async def test_bitbucket_cloud_is_ancestor_compares_the_merge_base_with_the_ancestor(
    fake: BitbucketCloudFake, adapter: Any, ancestor: str, descendant: str, expected: bool
) -> None:
    graph = _ancestry_graph(fake)

    answer = await adapter.is_ancestor(fake.repository(), graph[ancestor], graph[descendant])

    assert answer is expected
    assert _merge_base_paths(fake) == [
        f"/2.0/repositories/{WORKSPACE}/{SLUG}/merge-base/{graph[ancestor]}..{graph[descendant]}"
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "body",
    [{}, {"type": "commit"}, {"type": "commit", "hash": None}, ["not", "an", "object"]],
    ids=("empty", "no-hash", "null-hash", "not-an-object"),
)
async def test_bitbucket_cloud_is_ancestor_on_a_malformed_merge_base_is_unavailable(
    fake: BitbucketCloudFake, adapter: Any, body: Any
) -> None:
    graph = _ancestry_graph(fake)
    fake.answer_next(200, body=body)

    with pytest.raises(Unavailable):
        await adapter.is_ancestor(fake.repository(), graph["root"], graph["merge"])


@pytest.mark.anyio
async def test_bitbucket_cloud_is_ancestor_on_a_404_is_unavailable_not_false(
    fake: BitbucketCloudFake, adapter: Any
) -> None:
    graph = _ancestry_graph(fake)
    orphan = fake.add_commit((), "orphan")

    with pytest.raises(Unavailable):
        await adapter.is_ancestor(fake.repository(), orphan, graph["merge"])
    with pytest.raises(Unavailable):
        await adapter.is_ancestor(fake.repository(), "f" * 40, graph["merge"])
