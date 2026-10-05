"""Red CI on a head is failure; a new green head is success."""

from __future__ import annotations

import pytest
from curie_api.forges.types import CheckState, RollupState
from forge_fakes.contract_harness import AdapterHarness, open_pull


@pytest.mark.anyio
async def test_a_failing_head_then_a_green_new_head(harness: AdapterHarness) -> None:
    pull = await open_pull(harness, "factory/red-then-green")
    red = pull.head_sha
    harness.report_checks(red, {"unit": CheckState.FAILURE, "lint": CheckState.SUCCESS})

    failing = await harness.code_host.observe_ci(harness.repository, red)
    assert failing.state is RollupState.FAILURE
    assert {c.key: c.state for c in failing.checks}["unit"] is CheckState.FAILURE

    green = harness.push_head(pull.head_ref)
    current = await harness.code_host.read_pull_request(pull.ref)
    assert current.head_sha == green != red
    harness.report_checks(green, {"unit": CheckState.SUCCESS, "lint": CheckState.SUCCESS})

    passing = await harness.code_host.observe_ci(harness.repository, green)
    assert passing.state is RollupState.SUCCESS
    assert all(check.head_sha == green for check in passing.checks)
    # The old head keeps its own verdict.
    assert (await harness.code_host.observe_ci(harness.repository, red)).state is (
        RollupState.FAILURE
    )


@pytest.mark.anyio
async def test_a_pending_check_beside_a_failure_is_still_failure(
    harness: AdapterHarness,
) -> None:
    pull = await open_pull(harness, "factory/mixed")
    harness.report_checks(pull.head_sha, {"unit": CheckState.FAILURE, "e2e": CheckState.PENDING})
    rollup = await harness.code_host.observe_ci(harness.repository, pull.head_sha)
    assert rollup.state is RollupState.FAILURE
