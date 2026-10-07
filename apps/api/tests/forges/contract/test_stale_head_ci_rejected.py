"""Checks reported for an older head never count for the current one."""

from __future__ import annotations

import pytest
from curie_api.forges.types import CheckState, RollupState
from forge_fakes.contract_harness import AdapterHarness, open_pull


@pytest.mark.anyio
async def test_green_checks_on_an_old_head_are_not_returned_for_the_new_head(
    harness: AdapterHarness,
) -> None:
    pull = await open_pull(harness, "factory/stale")
    old = pull.head_sha
    harness.report_checks(old, {"unit": CheckState.SUCCESS})
    new = harness.push_head(pull.head_ref)

    rollup = await harness.code_host.observe_ci(harness.repository, new)
    assert rollup.head_sha == new
    assert rollup.state is RollupState.NONE
    assert rollup.checks == ()


@pytest.mark.anyio
async def test_checks_on_the_new_head_are_returned(harness: AdapterHarness) -> None:
    pull = await open_pull(harness, "factory/fresh")
    harness.report_checks(pull.head_sha, {"unit": CheckState.SUCCESS})
    new = harness.push_head(pull.head_ref)
    harness.report_checks(new, {"unit": CheckState.PENDING})

    rollup = await harness.code_host.observe_ci(harness.repository, new)
    assert rollup.state is RollupState.PENDING
    assert [(c.key, c.head_sha) for c in rollup.checks] == [("unit", new)]
