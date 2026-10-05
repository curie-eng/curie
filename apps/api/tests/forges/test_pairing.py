"""Pairing rules and capability declarations (ADR 0197 decisions 4 and 5)."""

from __future__ import annotations

import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    TRACKER_OPERATIONS,
    Operation,
    Support,
    mandatory_capabilities,
    validate_pairing,
)
from curie_api.forges.errors import InvalidPairing
from curie_api.forges.memory import InMemoryCodeHost, InMemoryTracker, all_supported


@pytest.mark.parametrize(
    ("tracker", "code_host"),
    [
        (types.GITHUB, types.GITLAB),
        (types.GITLAB, types.GITHUB),
        (types.GITHUB, types.BITBUCKET_CLOUD),
        (types.GITLAB, types.BITBUCKET_DC),
    ],
)
def test_a_native_tracker_is_refused_with_another_forge(tracker: str, code_host: str) -> None:
    with pytest.raises(InvalidPairing):
        mandatory_capabilities(tracker, code_host)


@pytest.mark.parametrize(
    ("tracker", "code_host"),
    [
        (types.GITHUB, types.GITHUB),
        (types.GITLAB, types.GITLAB),
        (types.JIRA_CLOUD, types.GITHUB),
        (types.JIRA_CLOUD, types.GITLAB),
        (types.JIRA_CLOUD, types.BITBUCKET_CLOUD),
        (types.JIRA_CLOUD, types.BITBUCKET_DC),
    ],
)
def test_allowed_pairings_are_accepted(tracker: str, code_host: str) -> None:
    required = mandatory_capabilities(tracker, code_host)
    assert Operation.POLL_MARKED in required.tracker
    assert Operation.OBSERVE_CI in required.code_host


@pytest.mark.parametrize(
    ("tracker", "code_host"),
    [
        (types.BITBUCKET_CLOUD, types.BITBUCKET_CLOUD),
        (types.JIRA_CLOUD, types.JIRA_CLOUD),
        ("svn", types.GITHUB),
    ],
)
def test_a_kind_on_the_wrong_side_is_refused(tracker: str, code_host: str) -> None:
    with pytest.raises(InvalidPairing):
        mandatory_capabilities(tracker, code_host)


def test_write_access_is_mandatory_only_on_a_native_pairing() -> None:
    assert Operation.USER_CAN_WRITE in mandatory_capabilities(types.GITHUB, types.GITHUB).code_host
    jira = mandatory_capabilities(types.JIRA_CLOUD, types.BITBUCKET_CLOUD)
    assert Operation.USER_CAN_WRITE not in jira.code_host
    assert Operation.LINK_PULL_REQUEST not in jira.tracker


def test_a_mandatory_operation_that_is_not_supported_refuses_the_pair() -> None:
    declared = all_supported(TRACKER_OPERATIONS)
    declared[Operation.READ_TICKET] = Support.UNSUPPORTED
    with pytest.raises(InvalidPairing, match="tracker.read_ticket"):
        validate_pairing(InMemoryTracker(capabilities=declared), InMemoryCodeHost())


def test_a_native_code_host_without_write_access_refuses_the_pair() -> None:
    declared = all_supported(CODE_HOST_OPERATIONS)
    declared[Operation.USER_CAN_WRITE] = Support.UNSUPPORTED
    host = InMemoryCodeHost(capabilities=declared)
    with pytest.raises(InvalidPairing, match="user_can_write"):
        validate_pairing(InMemoryTracker(), host)
    # The same code host is fine under a tracker-only kind.
    validate_pairing(InMemoryTracker(kind=types.MEMORY_TRACKER_ONLY), host)


def test_a_no_op_is_refused_where_doing_nothing_is_not_an_answer() -> None:
    declared = all_supported(CODE_HOST_OPERATIONS)
    declared[Operation.USER_CAN_WRITE] = Support.NOOP
    with pytest.raises(InvalidPairing, match="cannot be a no-op"):
        InMemoryCodeHost(capabilities=declared)
    declared[Operation.USER_CAN_WRITE] = Support.SUPPORTED
    declared[Operation.RERUN_FAILED] = Support.NOOP
    InMemoryCodeHost(capabilities=declared)


def test_an_undeclared_operation_is_refused() -> None:
    declared = all_supported(TRACKER_OPERATIONS)
    del declared[Operation.DEPENDENCIES]
    with pytest.raises(InvalidPairing, match="undeclared"):
        InMemoryTracker(capabilities=declared)


def test_the_default_in_memory_pair_is_valid() -> None:
    validate_pairing(InMemoryTracker(), InMemoryCodeHost())
