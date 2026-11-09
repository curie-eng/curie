"""ADR 0272 plan review R1: a DM row is judged under its recorded DM identity.

@spec APPROVER-DM-3.

The requester's turn arrives through the binding's own bot (``scope["main"]``,
the invocation's reply identity) while the owning route sends its DM card
through a second bot identity on the same installation (``scope["other"]``).
The route also restricts approvers to one Slack user group. Each bot has its
own user-group client (ADR-0168 decision 5), installed here as a recording
fake on the API's real ``SlackApproverSetSelector``; everything else is the
real creation, registration, answer and approved-resume path against the
roster TLS directory, Postgres and Valkey. Synthetic, reserved example values
only.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from approval_answer_support import (
    NATIVE_A,
    NATIVE_B,
    actions,
    approval_row,
    published,
    turn_check,
)
from approval_answer_support import synthetic_provider as synthetic_provider
from approver_dm_support import (
    DM_CHANNEL,
    account,
    configured_dm,
    dm_answer,
    dm_row,
    no_effect,
    register_dm_card,
    selected,
    unauthenticated,
)
from approver_dm_support import dm_case as dm_case
from approver_ticket_support import APPROVER_A
from curie_api.slack_approvers import SlackApproverSetSelector
from curie_channel_gateway.group_models import UserGroupMembership

GROUP = "S0EXAMPLE1"
CONTINUE = ("continue", "current_authority")


class _Group:
    """One bot's user-group client: a fixed member set, every lookup recorded.

    @spec APPROVER-DM-3.
    """

    def __init__(self, *users: str) -> None:
        self.users = frozenset(users)
        self.calls: list[str] = []

    def _answer(self, group_id: str) -> UserGroupMembership:
        """@spec APPROVER-DM-3."""
        self.calls.append(group_id)
        return UserGroupMembership(
            group=group_id, users=self.users, fetched_at=datetime.now(UTC), cache_age_s=0.0,
        )

    async def members(self, group_id: str) -> UserGroupMembership:
        """@spec APPROVER-DM-3."""
        return self._answer(group_id)

    async def fresh_members(self, group_id: str) -> UserGroupMembership:
        """@spec APPROVER-DM-3: the uncached fetch HTTP-selected answers require."""
        return self._answer(group_id)


def _groups(case: SimpleNamespace, *, dm: _Group, reply: _Group) -> SimpleNamespace:
    """Install one client per bot identity; ``default`` admits nobody selected.

    @spec APPROVER-DM-3.
    """
    clients = SimpleNamespace(dm=dm, reply=reply, default=_Group(NATIVE_B))
    case.client.app.state.approver_sets = SlackApproverSetSelector(
        clients.default,
        identity_clients={
            case.scope["main"]["adapter"]: clients.reply,
            case.scope["other"]["adapter"]: clients.dm,
        },
    )
    return clients


def _registered_on_other(case: SimpleNamespace) -> dict[str, Any]:
    """A group-restricted DM route on the second bot, one DM row and its registered card.

    @spec APPROVER-DM-3.
    """
    scope = case.scope
    assert scope["other"]["adapter"] != scope["main"]["adapter"]
    configured_dm(case, identity=scope["other"]["adapter"], approvers={"group": GROUP})
    row, request = dm_row(
        case, [selected(case, APPROVER_A, account(scope["other"], NATIVE_A))], receiver="other",
    )
    assert row["reply_adapter"] == scope["main"]["adapter"], row
    return register_dm_card(case, row, request, receiver="other")


def test_group_membership_is_read_through_the_dm_identity_at_answer_and_at_resume(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-3: the DM bot's group client decides, never the reply bot's.

    The reply identity's group client would refuse approver A; the DM
    identity's admits. The selected approver's answer in the registered DM
    approves, the approved-resume current check continues, and both judgments
    asked only the DM identity's client.
    """
    case = dm_case
    row = _registered_on_other(case)
    clients = _groups(case, dm=_Group(NATIVE_A), reply=_Group(NATIVE_B))

    response = dm_answer(case, row["id"], NATIVE_A, channel=DM_CHANNEL, receiver="other")
    assert response.status_code == 200, response.text
    assert (response.json()["status"], response.json()["resolved_by"]) == ("approved", NATIVE_A)
    stored = approval_row(row["id"])
    assert (stored["status"], stored["resolved_by"]) == ("approved", NATIVE_A)
    assert clients.dm.calls and set(clients.dm.calls) == {GROUP}, clients.dm.calls
    assert clients.reply.calls == [] and clients.default.calls == []
    answered = len(clients.dm.calls)

    entry = published(case, row["id"])
    assert turn_check(case, entry, row["id"]) == CONTINUE
    assert len(clients.dm.calls) > answered, "the approved resume did not re-read the group"
    assert clients.reply.calls == [] and clients.default.calls == []


def test_a_dm_group_that_refuses_is_not_overridden_by_the_reply_identity(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-3: the reverse group verdicts refuse with no effect.

    The DM identity's group excludes approver A while the reply identity's
    includes them: the answer is refused, the row stays pending on its card
    and the reply identity's client is never asked. Once the DM group admits
    approver A, the same answer approves.
    """
    case = dm_case
    row = _registered_on_other(case)
    clients = _groups(case, dm=_Group(NATIVE_B), reply=_Group(NATIVE_A))

    response = dm_answer(case, row["id"], NATIVE_A, channel=DM_CHANNEL, receiver="other")
    assert response.status_code == 403, response.text
    no_effect(case, row["id"], card_channel=DM_CHANNEL)
    assert clients.dm.calls, "the DM identity's group was never read"
    assert clients.reply.calls == [] and clients.default.calls == []

    clients = _groups(case, dm=_Group(NATIVE_A), reply=_Group(NATIVE_A))
    response = dm_answer(case, row["id"], NATIVE_A, channel=DM_CHANNEL, receiver="other")
    assert response.status_code == 200, response.text
    assert clients.reply.calls == [] and clients.default.calls == []


def test_a_receiver_of_the_reply_identity_cannot_answer_the_dm_row(
    dm_case: SimpleNamespace,
) -> None:
    """@spec APPROVER-DM-3: the requester's own bot is not the DM card's bot.

    Every group client admits approver A, so only the receiving identity
    differs: the selected approver's interaction in the registered DM, as seen
    by the reply identity's receiver, is not proof for this card. The same
    interaction through the DM identity then approves exactly once.
    """
    case = dm_case
    row = _registered_on_other(case)
    clients = _groups(case, dm=_Group(NATIVE_A), reply=_Group(NATIVE_A))

    response = dm_answer(case, row["id"], NATIVE_A, channel=DM_CHANNEL, receiver="main")
    unauthenticated(case, row["id"], response, card_channel=DM_CHANNEL)
    assert clients.reply.calls == []

    response = dm_answer(case, row["id"], NATIVE_A, channel=DM_CHANNEL, receiver="other")
    assert response.status_code == 200, response.text
    resolved = actions(row["id"], "resolved")
    assert len(resolved) == 1 and resolved[0]["actor"] == NATIVE_A, resolved
    assert turn_check(case, published(case, row["id"]), row["id"]) == CONTINUE
