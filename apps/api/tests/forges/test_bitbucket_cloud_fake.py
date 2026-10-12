"""The Bitbucket Cloud adapter's build status mapping against `BitbucketCloudFake` (#4401).

Bitbucket Cloud reports a stopped or cancelled build as ``STOPPED`` or
``CANCELLED``; both read as cancelled, so a cancelled head rolls up red instead
of waiting forever as pending (decision 9.2). ``INPROGRESS`` stays pending.
"""

from __future__ import annotations

from typing import Any

import pytest
from curie_api.forges.types import CheckSource, CheckState, RollupState
from forge_fakes.bitbucket_cloud import FACTORY_TOKEN, BitbucketCloudFake
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
