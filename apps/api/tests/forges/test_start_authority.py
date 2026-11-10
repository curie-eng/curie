"""Who may start a run, and whose review feedback is acted on, under a binding
(ADR 0197, "Authority" items 1 to 3)."""

from __future__ import annotations

import pytest
from curie_api.forges import types
from curie_api.forges.authority import (
    binding_may_act_on_feedback,
    feedback_allowlist,
    may_start_with,
)
from curie_api.forges.capabilities import (
    CODE_HOST_OPERATIONS,
    NOOP_ELIGIBLE,
    OPTIONAL_OPERATIONS,
    TRACKER_OPERATIONS,
    Operation,
    Support,
)
from curie_api.forges.config import (
    AccountAllowlist,
    BindingConfig,
    ForgesConfig,
    GroupAuthority,
    WriteCheck,
)
from curie_api.forges.memory import InMemoryCodeHost, InMemoryTracker, all_supported
from curie_api.forges.types import Actor
from forge_fakes import config_samples as samples

STARTER = Actor(id=samples.JIRA_STARTER, login="Ada Lovelace")
# Same display name as the starter, different account: a login never authorizes.
NAMESAKE = Actor(id="557058:cccccccc-0000-4000-8000-000000000009", login="Ada Lovelace")
REVIEWER = Actor(id="{reviewer-uuid-1}", login="rev")


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _tracker(*, groups: Support = Support.SUPPORTED) -> InMemoryTracker:
    capabilities = all_supported(TRACKER_OPERATIONS)
    capabilities[Operation.GROUP_MEMBERSHIP] = groups
    return InMemoryTracker(kind=types.MEMORY_TRACKER_ONLY, capabilities=capabilities)


def _jira_binding(**overrides: object) -> BindingConfig:
    return ForgesConfig.model_validate(samples.jira_document(**overrides)).bindings[0]


@pytest.mark.anyio
async def test_a_write_check_defers_to_the_trackers_own_check() -> None:
    tracker = InMemoryTracker()
    issue = tracker.seed_issue("Ticket", "Body")
    tracker.grant(STARTER)
    authority = ForgesConfig.model_validate(samples.github_document()).bindings[0].start_authority
    assert isinstance(authority, WriteCheck)

    assert await may_start_with(authority, issue, STARTER, tracker=tracker) is True
    assert await may_start_with(authority, issue, NAMESAKE, tracker=tracker) is False


@pytest.mark.anyio
async def test_an_allowlist_admits_by_account_id_only() -> None:
    tracker = _tracker()
    issue = tracker.seed_issue("Ticket", "Body")
    # The tracker's own check would admit the namesake; the binding does not ask it.
    tracker.grant(NAMESAKE)
    authority = _jira_binding().start_authority
    assert isinstance(authority, AccountAllowlist)

    assert await may_start_with(authority, issue, STARTER, tracker=tracker) is True
    assert await may_start_with(authority, issue, NAMESAKE, tracker=tracker) is False


@pytest.mark.anyio
async def test_a_group_admits_its_members_through_the_tracker() -> None:
    tracker = _tracker()
    issue = tracker.seed_issue("Ticket", "Body")
    tracker.add_to_group("starters-id", STARTER)
    tracker.add_to_group("other-id", NAMESAKE)
    authority = _jira_binding(
        start_authority={"type": "group", "group_id": "starters-id"}
    ).start_authority
    assert isinstance(authority, GroupAuthority)

    assert await may_start_with(authority, issue, STARTER, tracker=tracker) is True
    assert await may_start_with(authority, issue, NAMESAKE, tracker=tracker) is False


@pytest.mark.anyio
async def test_a_tracker_without_group_membership_refuses_a_group_start() -> None:
    tracker = _tracker(groups=Support.UNSUPPORTED)
    issue = tracker.seed_issue("Ticket", "Body")
    tracker.add_to_group("starters-id", STARTER)
    tracker.grant(STARTER)

    authority = GroupAuthority(group_id="starters-id")
    assert await may_start_with(authority, issue, STARTER, tracker=tracker) is False


def test_group_membership_is_optional_and_cannot_be_a_no_op() -> None:
    assert Operation.GROUP_MEMBERSHIP in OPTIONAL_OPERATIONS & TRACKER_OPERATIONS
    assert Operation.GROUP_MEMBERSHIP not in NOOP_ELIGIBLE


def _code_host(*, reports_write: bool) -> InMemoryCodeHost:
    capabilities = all_supported(CODE_HOST_OPERATIONS)
    if not reports_write:
        capabilities[Operation.USER_CAN_WRITE] = Support.UNSUPPORTED
    return InMemoryCodeHost(kind=types.BITBUCKET_CLOUD, capabilities=capabilities)


@pytest.mark.anyio
async def test_feedback_falls_back_to_the_bindings_allowlist_for_that_code_host() -> None:
    binding = _jira_binding(review_feedback_allowlist={"bitbucket": [REVIEWER.id]})
    web = binding.repo("web")
    assert web is not None
    code_host = _code_host(reports_write=False)
    repository = code_host.seed_repository("acme/web")

    assert await binding_may_act_on_feedback(binding, web, code_host, repository, REVIEWER)
    assert not await binding_may_act_on_feedback(binding, web, code_host, repository, STARTER)


@pytest.mark.anyio
async def test_another_code_hosts_allowlist_does_not_apply() -> None:
    binding = _jira_binding(review_feedback_allowlist={"gitlab": [REVIEWER.id]})
    web, infra = binding.repo("web"), binding.repo("infra")
    assert web is not None and infra is not None
    code_host = _code_host(reports_write=False)
    repository = code_host.seed_repository("acme/web")

    assert feedback_allowlist(binding, infra) == {REVIEWER.id}
    assert feedback_allowlist(binding, web) == frozenset()
    assert not await binding_may_act_on_feedback(binding, web, code_host, repository, REVIEWER)


@pytest.mark.anyio
async def test_a_code_host_that_reports_write_access_decides_alone() -> None:
    binding = _jira_binding(review_feedback_allowlist={"bitbucket": [REVIEWER.id]})
    web = binding.repo("web")
    assert web is not None
    code_host = _code_host(reports_write=True)
    repository = code_host.seed_repository("acme/web")
    code_host.grant(repository, STARTER)

    assert await binding_may_act_on_feedback(binding, web, code_host, repository, STARTER)
    assert not await binding_may_act_on_feedback(binding, web, code_host, repository, REVIEWER)


def test_a_repository_from_another_binding_is_refused() -> None:
    binding = _jira_binding()
    other = ForgesConfig.model_validate(samples.github_document()).bindings[0].repos[0]
    with pytest.raises(ValueError, match="is not on binding"):
        feedback_allowlist(binding, other)
