"""Each adapter's declaration holds: no-ops write nothing, unsupported operations
raise, and the caller's fallback answers in their place."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from curie_api.forges.authority import may_act_on_feedback
from curie_api.forges.capabilities import (
    OPTIONAL_OPERATIONS,
    Operation,
    Support,
    supports,
    validate_pairing,
)
from curie_api.forges.errors import Unsupported
from curie_api.forges.types import CheckState, RollupState
from forge_fakes.contract_harness import AdapterHarness, InMemoryHarness, open_pull

Call = Callable[[], Awaitable[Any]]


async def _optional_calls(harness: AdapterHarness) -> dict[Operation, Call]:
    issue = harness.seed_issue("Ticket", "Do the thing.")
    pull = await open_pull(harness, "factory/capabilities")
    harness.report_checks(pull.head_sha, {"unit": CheckState.FAILURE})
    tracker, code_host, repository = harness.tracker, harness.code_host, harness.repository
    return {
        Operation.LINK_PULL_REQUEST: lambda: tracker.link_pull_request(issue, pull),
        Operation.DEPENDENCIES: lambda: tracker.dependencies(issue),
        Operation.GROUP_MEMBERSHIP: lambda: tracker.in_group(harness.writer, "starters"),
        Operation.CI_DIAGNOSTICS: lambda: code_host.ci_diagnostics(repository, pull.head_sha),
        Operation.RERUN_FAILED: lambda: code_host.rerun_failed(repository, pull.head_sha),
        Operation.USER_CAN_WRITE: lambda: code_host.user_can_write(repository, harness.writer),
    }


def _declared(harness: AdapterHarness, operation: Operation) -> Support:
    if operation.value.startswith("tracker."):
        return harness.tracker.capabilities[operation]
    return harness.code_host.capabilities[operation]


def test_the_pair_meets_its_mandatory_set(harness: AdapterHarness) -> None:
    validate_pairing(harness.tracker, harness.code_host)


@pytest.mark.anyio
async def test_no_op_operations_write_nothing_and_unsupported_ones_raise(
    harness: AdapterHarness,
) -> None:
    calls = await _optional_calls(harness)
    assert set(calls) == OPTIONAL_OPERATIONS
    for operation, call in calls.items():
        support = _declared(harness, operation)
        before = harness.write_count()
        if support is Support.UNSUPPORTED:
            with pytest.raises(Unsupported) as refused:
                await call()
            assert refused.value.operation is operation
        elif support is Support.NOOP:
            result = await call()
            assert result in (None, (), 0)
        else:
            await call()
            continue
        assert harness.write_count() == before, operation


@pytest.mark.anyio
async def test_supported_rerun_reruns_the_failed_check(harness: AdapterHarness) -> None:
    if not supports(harness.code_host.capabilities, Operation.RERUN_FAILED):
        pytest.skip(f"{harness.name} declares no rerun")
    pull = await open_pull(harness, "factory/rerun")
    harness.report_checks(pull.head_sha, {"unit": CheckState.FAILURE})
    before = harness.write_count()

    assert await harness.code_host.rerun_failed(harness.repository, pull.head_sha) == 1
    assert harness.write_count() == before + 1
    rollup = await harness.code_host.observe_ci(harness.repository, pull.head_sha)
    assert rollup.state is RollupState.PENDING


@pytest.mark.anyio
async def test_write_permission_or_its_allowlist_fallback_decides_feedback(
    harness: AdapterHarness,
) -> None:
    harness.set_write_access(harness.writer, True)
    allowlist = harness.feedback_allowlist()
    code_host, repository = harness.code_host, harness.repository

    assert await may_act_on_feedback(code_host, repository, harness.writer, allowlist) is True
    assert await may_act_on_feedback(code_host, repository, harness.outsider, allowlist) is False
    if supports(code_host.capabilities, Operation.USER_CAN_WRITE):
        # The host decides alone: an allowlist entry does not grant authority.
        listed = allowlist | {harness.outsider.id}
        assert await may_act_on_feedback(code_host, repository, harness.outsider, listed) is False
    else:
        # The fallback reads the binding allowlist, not write access.
        assert await may_act_on_feedback(code_host, repository, harness.writer, frozenset()) is (
            False
        )


def test_the_registry_exercises_no_op_and_unsupported_declarations() -> None:
    # Without an adapter that declares them, the two branches above are vacuous.
    minimal = InMemoryHarness(minimal=True)
    declared = {_declared(minimal, operation) for operation in OPTIONAL_OPERATIONS}
    assert {Support.NOOP, Support.UNSUPPORTED} <= declared
