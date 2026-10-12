"""The Jira Cloud tracker builds from config and pairs with GitHub (#4358 AC 2).

Decisions 2, 13, 17 and 19: a ``static_token`` with a ``username`` is the only
credential; the capability declaration supports group membership and pull
request links, declares dependencies a no-op, and meets the tracker-only
pairing's mandatory set with the GitHub code host. Start authority is the
binding's allowlist or group, checked against the fake over HTTP.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from curie_api.forges import types
from curie_api.forges.capabilities import (
    MARKED_COMMENT_OPERATIONS,
    TRACKER_OPERATIONS,
    CapabilityTier,
    Operation,
    Support,
    tier,
    validate_pairing,
)
from curie_api.forges.config import ForgesConfig
from curie_api.forges.connections import BindingTrackerConnection
from curie_api.forges.errors import Unauthorized
from curie_api.forges.registry import build_registry
from forge_fakes.jira import (
    CODE_HOST_NAME,
    SITE_HOST,
    STARTERS,
    TOKEN,
    TRACKER_NAME,
    JiraFake,
    jira_document,
    jira_registry,
    jira_tracker,
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _expected_tracker_capabilities() -> dict[Operation, Support]:
    declared = dict.fromkeys(TRACKER_OPERATIONS, Support.SUPPORTED)
    declared[Operation.DEPENDENCIES] = Support.NOOP
    return declared


def test_jira_config_ticket_labels_is_a_mandatory_tracker_operation() -> None:
    assert Operation.TICKET_LABELS.value == "tracker.ticket_labels"
    assert Operation.TICKET_LABELS in TRACKER_OPERATIONS
    assert tier(Operation.TICKET_LABELS) is CapabilityTier.MANDATORY


@pytest.mark.anyio
async def test_jira_config_the_jira_tracker_pairs_with_the_github_code_host() -> None:
    registry = jira_registry()
    tracker = jira_tracker(JiraFake())
    async with httpx.AsyncClient() as client:
        code_host = registry.code_host(CODE_HOST_NAME).adapter(client)

        validate_pairing(tracker, code_host)

    assert (tracker.kind, tracker.host) == (types.JIRA_CLOUD, SITE_HOST)
    assert code_host.kind == types.GITHUB


def test_jira_config_capabilities_are_the_declared_set() -> None:
    tracker = jira_tracker(JiraFake())

    assert dict(tracker.capabilities) == _expected_tracker_capabilities()
    assert tracker.capabilities[Operation.GROUP_MEMBERSHIP] is Support.SUPPORTED
    assert tracker.capabilities[Operation.LINK_PULL_REQUEST] is Support.SUPPORTED
    assert tracker.capabilities[Operation.DEPENDENCIES] is Support.NOOP
    assert tracker.capabilities[Operation.TICKET_LABELS] is Support.SUPPORTED
    assert dict(tracker.marked_comments.capabilities) == dict.fromkeys(
        MARKED_COMMENT_OPERATIONS, Support.SUPPORTED
    )
    assert tracker.marked_comments.kind == types.JIRA_CLOUD
    assert tracker.marked_comments.host == SITE_HOST


def test_jira_config_a_static_token_entry_builds_a_binding_scoped_connection() -> None:
    """Liveness for the two refusals below: the decision 2 shape builds."""

    registry = jira_registry()
    connection = registry.binding_tracker(TRACKER_NAME)

    assert isinstance(connection, BindingTrackerConnection)
    assert registry.tracker(TRACKER_NAME) is connection
    assert connection.config.name == TRACKER_NAME


def test_jira_config_oauth_client_credentials_are_refused_with_the_decision_2_text() -> None:
    document = jira_document(
        credential={
            "type": "oauth_client_credentials",
            "client_id": "client-1",
            "client_secret": "oauth-secret-fixture",
        }
    )
    config = ForgesConfig.model_validate(document)

    with pytest.raises(ValueError) as refused:
        build_registry(config)

    assert str(refused.value) == "jira_cloud supports static_token credentials only"


def test_jira_config_a_static_token_without_a_username_is_refused_naming_the_entry() -> None:
    config = ForgesConfig.model_validate(jira_document(username=None))

    with pytest.raises(ValueError) as refused:
        build_registry(config)

    message = str(refused.value)
    assert TRACKER_NAME in message
    assert "username" in message
    assert TOKEN not in message


@pytest.mark.anyio
async def test_jira_config_dependencies_is_a_no_op_that_reads_nothing() -> None:
    fake = JiraFake()
    tracker = jira_tracker(fake)
    issue = fake.ref(fake.seed_issue("Ticket"))

    assert await tracker.dependencies(issue) == ()
    assert fake.requests == []


# Start authority (decision 13) ---------------------------------------------------


def _allowlist(*accounts: str) -> dict[str, Any]:
    return {"type": "allowlist", "accounts": list(accounts)}


@pytest.mark.anyio
async def test_jira_config_an_allowlisted_account_may_start_and_another_may_not() -> None:
    fake = JiraFake()
    starter = fake.add_user("712020:00000000-0000-4000-8000-0000000000c1", "Starter")
    stranger = fake.add_user("712020:00000000-0000-4000-8000-0000000000c2", "Stranger")
    tracker = jira_tracker(fake, start_authority=_allowlist(starter.account_id))
    issue = fake.ref(fake.seed_issue("Ticket"))

    assert await tracker.may_start(issue, starter.actor) is True
    assert await tracker.may_start(issue, stranger.actor) is False


@pytest.mark.anyio
async def test_jira_config_group_authority_reads_the_accounts_groups_by_group_id() -> None:
    fake = JiraFake()
    member = fake.add_user("712020:00000000-0000-4000-8000-0000000000c3", "Member")
    other = fake.add_user("712020:00000000-0000-4000-8000-0000000000c4", "Other")
    fake.set_group_member(STARTERS, member.account_id, True)
    fake.set_group_member("00000000-0000-4000-8000-0000000000f9", other.account_id, True)
    tracker = jira_tracker(fake)
    issue = fake.ref(fake.seed_issue("Ticket"))

    assert await tracker.may_start(issue, member.actor) is True
    assert await tracker.may_start(issue, other.actor) is False
    assert await tracker.in_group(member.actor, STARTERS) is True
    assert await tracker.in_group(other.actor, STARTERS) is False

    reads = fake.requests_of("user_groups")
    assert reads and all(r.method == "GET" for r in reads)
    assert {r.params["accountId"] for r in reads} == {member.account_id, other.account_id}


@pytest.mark.anyio
async def test_jira_config_a_group_read_the_bot_may_not_make_is_unauthorized() -> None:
    """Edge case 1: a 403 on the group read defers; it never admits."""

    fake = JiraFake()
    member = fake.add_user("712020:00000000-0000-4000-8000-0000000000c5", "Member")
    fake.set_group_member(STARTERS, member.account_id, True)
    tracker = jira_tracker(fake)
    issue = fake.ref(fake.seed_issue("Ticket"))
    fake.forbid_group_reads = True

    with pytest.raises(Unauthorized):
        await tracker.may_start(issue, member.actor)
    with pytest.raises(Unauthorized):
        await tracker.in_group(member.actor, STARTERS)
