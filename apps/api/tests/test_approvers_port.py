"""The approver-set port is a port, not a rename (#420, ADR-0034).

Every other test drives the authorizer through Slack's sets, which cannot tell a
real seam from a hardcoded one. These drive it through a set and a selector that
Slack has nothing to do with, so the file importing no Slack module is the
assertion: the authorizer needs no provider at all to work, and if it ever
reaches for one again this stops compiling rather than quietly still passing.

The self-approval case is the one that matters. A fake set can be written that
happily reports the author as a member, which is exactly what a careless real
implementation would do. The authorizer must still refuse. That is the proof AC2
is structural -- a property of the authorizer that no set can opt out of -- rather
than a promise each implementation makes in a docstring and might forget.
"""

import asyncio
from collections.abc import Mapping
from typing import Any

from curie_api.approvers import ApproverSet, ApproverSetSelector, MembershipVerdict
from curie_api.authorizer import AuthzDecision, authorize_approval
from curie_api.models import Approval

_AUTHOR = "U0AUTHOR1"
_APPROVER = "U0APPROV1"
_OUTSIDER = "U0OTHER01"
_CARD_CHANNEL = "C0BROAD01"


class _EveryoneIsAMember:
    """A set that admits anyone, including the author. No provider behind it."""

    audit_name = "FakeApproverSet"

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        return MembershipVerdict(member=True, evidence={"kind": "fake", "actor": actor})


class _NobodyIsAMember:
    audit_name = "FakeApproverSet"

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        return MembershipVerdict(
            member=False, reason="you are not an approver: fake set", evidence=None
        )


class _CannotTell:
    """A set whose lookup yielded nothing: the port's undetermined case."""

    audit_name = "FakeApproverSet"

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        return MembershipVerdict(
            member=False,
            undetermined=True,
            reason="could not verify: fake set could not tell",
            evidence={"kind": "fake", "lookup_failed": True},
        )


class _FakeSelector:
    """An ``ApproverSetSelector`` that reads no binding and knows no provider."""

    def __init__(self, approver_set: ApproverSet) -> None:
        self._approver_set = approver_set

    def __call__(self, approval: Approval, binding: Mapping[str, Any] | None) -> ApproverSet:
        return self._approver_set


def _approval(*, author: str = _AUTHOR) -> Approval:
    return Approval(
        conversation_id="th-420",
        author=author,
        summary="Discount for ACME",
        reply_channel="C0REQ0001",
        reply_placeholder="p-1",
        dedupe_key="ev-420",
        route="managers",
        card_channel=_CARD_CHANNEL,
    )


def _authorize(approver_set: ApproverSet, actor: str) -> tuple[str, AuthzDecision]:
    """The resolve endpoint's exact shape: select a set, then authorize on it."""

    select: ApproverSetSelector = _FakeSelector(approver_set)
    approval = _approval()
    return asyncio.run(
        authorize_approval(
            approval,
            actor,
            _CARD_CHANNEL,
            approver_set=select(approval, None),
        )
    )


def test_a_member_of_any_set_is_allowed() -> None:
    name, decision = _authorize(_EveryoneIsAMember(), _APPROVER)
    assert name == "FakeApproverSet"
    assert decision.allowed
    assert decision.evidence == {"kind": "fake", "actor": _APPROVER}


def test_a_non_member_of_any_set_is_denied_with_the_sets_reason() -> None:
    _name, decision = _authorize(_NobodyIsAMember(), _APPROVER)
    assert not decision.allowed
    assert "not an approver" in decision.reason


def test_an_undetermined_set_fails_closed() -> None:
    """The fail-closed contract is owed to the PORT's undetermined verdict, not
    to a Slack exception the authorizer happens to recognize."""

    _name, decision = _authorize(_CannotTell(), _APPROVER)
    assert not decision.allowed
    assert "could not verify" in decision.reason
    assert decision.evidence == {"kind": "fake", "lookup_failed": True}


def test_the_author_is_denied_even_by_a_set_that_admits_them() -> None:
    """AC2 is structural. The set says the author is a member; the authorizer
    refuses anyway, because a set never gets a vote on self-approval."""

    admitting = _EveryoneIsAMember()
    assert asyncio.run(admitting.contains(_AUTHOR, _CARD_CHANNEL)).member

    _name, decision = _authorize(admitting, _AUTHOR)
    assert not decision.allowed
    assert "self-approval" in decision.reason
    # The set never ran, so its snapshot must not appear next to this denial.
    assert decision.evidence is None


def test_an_outsider_is_still_allowed_by_a_set_that_admits_everyone() -> None:
    """Guards the test above from passing vacuously: the admitting set really
    does allow a non-author, so the author's denial is the self-approval rule and
    not a broken fake."""

    _name, decision = _authorize(_EveryoneIsAMember(), _OUTSIDER)
    assert decision.allowed
