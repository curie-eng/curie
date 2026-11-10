"""The approver-set port is a port, not a rename (#420, ADR-0106).

Every other test drives the authorizer through Slack's sets, which cannot tell a
real seam from a hardcoded one. These drive it through a set and a selector that
Slack has nothing to do with, so the file importing no Slack module is the
assertion: the authorizer needs no provider at all to work, and if it ever
reaches for one again this stops compiling rather than quietly still passing.

Requester equality has no independent vote. A provider-free fake proves the
authorizer asks the selected set even for the requester: an admitting set allows
them, while a denying set refuses them with its own reason and evidence.
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
    operator_eligible = True
    console_eligible = True
    adapter_eligible = True
    test_driver_eligible = False
    chat_eligible = True
    ineligible_reason = None

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        return MembershipVerdict(member=True, evidence={"kind": "fake", "actor": actor})


class _NobodyIsAMember:
    audit_name = "FakeApproverSet"
    operator_eligible = True
    console_eligible = True
    adapter_eligible = True
    test_driver_eligible = False
    chat_eligible = True
    ineligible_reason = None

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        return MembershipVerdict(
            member=False, reason="you are not an approver: fake set", evidence=None
        )


class _CannotTell:
    """A set whose lookup yielded nothing: the port's undetermined case."""

    audit_name = "FakeApproverSet"
    operator_eligible = True
    console_eligible = True
    adapter_eligible = True
    test_driver_eligible = False
    chat_eligible = True
    ineligible_reason = None

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


def _authorize(
    approver_set: ApproverSet,
    actor: str,
    *,
    approval: Approval | None = None,
    principal_kind: Any = "chat",
    canonical_owner: bool = False,
) -> tuple[str | None, AuthzDecision]:
    """The resolve endpoint's exact shape: select a set, then authorize on it.

    The fake selector reads no binding, so none is passed as the route binding
    either (ADR 0271).
    """

    select: ApproverSetSelector = _FakeSelector(approver_set)
    approval = approval or _approval()
    return asyncio.run(
        authorize_approval(
            approval,
            actor,
            _CARD_CHANNEL,
            approver_set=select(approval, None),
            principal_kind=principal_kind,
            route_binding=None,
            canonical_owner=canonical_owner,
        )
    )


def test_a_member_of_any_set_is_allowed() -> None:
    name, decision = _authorize(_EveryoneIsAMember(), _APPROVER)
    assert name == "FakeApproverSet"
    assert decision.allowed
    assert decision.evidence == {"kind": "fake", "actor": _APPROVER}


def test_a_requester_denied_by_any_set_gets_the_sets_reason() -> None:
    _name, decision = _authorize(_NobodyIsAMember(), _AUTHOR)
    assert not decision.allowed
    assert "not an approver" in decision.reason


def test_an_undetermined_set_fails_closed() -> None:
    """The fail-closed contract is owed to the PORT's undetermined verdict, not
    to a Slack exception the authorizer happens to recognize."""

    _name, decision = _authorize(_CannotTell(), _APPROVER)
    assert not decision.allowed
    assert "could not verify" in decision.reason
    assert decision.evidence == {"kind": "fake", "lookup_failed": True}


def test_the_requester_is_allowed_by_a_set_that_admits_them() -> None:
    """ADR-0106 makes set membership the whole authorization boundary."""

    admitting = _EveryoneIsAMember()
    assert asyncio.run(admitting.contains(_AUTHOR, _CARD_CHANNEL)).member

    _name, decision = _authorize(admitting, _AUTHOR)
    assert decision.allowed
    assert decision.evidence == {"kind": "fake", "actor": _AUTHOR}


def test_an_outsider_is_still_allowed_by_a_set_that_admits_everyone() -> None:
    """The provider-free admitting set applies identically to another actor."""

    _name, decision = _authorize(_EveryoneIsAMember(), _OUTSIDER)
    assert decision.allowed


# --- ADR 0271 decision 5: a dynamic approval is answered only by the HTTP owner ---


def _dynamic() -> Approval:
    approval = _approval()
    approval.selection_mode = "dynamic"
    return approval


def test_an_operator_eligible_set_never_answers_a_dynamic_approval_for_an_operator() -> None:
    """@spec REQUEST-APPROVAL-5: operator refusal precedes every set-specific shortcut.

    This set is operator eligible and admits everyone, so a refusal can come
    only from the authorizer's canonical-route rule.
    """

    for canonical_owner in (False, True):
        _name, decision = _authorize(
            _EveryoneIsAMember(), _APPROVER, approval=_dynamic(), principal_kind="operator",
            canonical_owner=canonical_owner,
        )
        assert not decision.allowed
        assert decision.evidence is not None
        assert decision.evidence["kind"] == "canonical_route_operator_refused"
    _name, ordinary = _authorize(_EveryoneIsAMember(), _APPROVER, principal_kind="operator")
    assert ordinary.allowed


def test_only_the_http_answer_owner_reaches_the_set_on_a_dynamic_approval() -> None:
    """@spec REQUEST-APPROVAL-5: any other caller is refused before the set is asked."""

    for kind in ("chat", "console", "adapter"):
        _name, decision = _authorize(
            _EveryoneIsAMember(), _APPROVER, approval=_dynamic(), principal_kind=kind,
        )
        assert not decision.allowed, kind
        assert decision.evidence is not None
        assert decision.evidence["kind"] == "canonical_route_requires_http_answer"
    _name, owner = _authorize(
        _EveryoneIsAMember(), _APPROVER, approval=_dynamic(), canonical_owner=True,
    )
    assert owner.allowed
    assert owner.evidence == {"kind": "fake", "actor": _APPROVER}
    _name, refused = _authorize(
        _NobodyIsAMember(), _APPROVER, approval=_dynamic(), canonical_owner=True,
    )
    assert not refused.allowed
