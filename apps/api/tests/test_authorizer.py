"""Authorizer and approver-set unit tests (#246, #420).

The originals below pin the channel-membership behavior (#246), now driven
through ``authorize_approval`` on an unbound approval, which is the path
production takes to reach that set. Everything after the divider pins #420: the
explicit-user-list and user-group sets that unfuse "who may approve" from "where
the card posted", plus the authorizer that selects between them and owns every
rule that is not membership. Slack is the only external service here and it is
reached exclusively through a MockTransport-backed real client.

Self-approval is deliberately NOT tested at the set level: a set is never asked
about it (ADR-0034), so a set-level self-approval test would assert an invariant
that does not live there. It is pinned on the authorizer, once per set, below.
"""

import asyncio
from typing import Any

import httpx
from curie_api.approvers import ApproverSet, ExplicitUsers, MembershipVerdict
from curie_api.authorizer import AuthzDecision, authorize_approval
from curie_api.models import Approval
from curie_api.slack_approvers import (
    SlackApproverSetSelector,
    SlackChannelMembers,
    SlackUserGroupMembers,
)
from curie_api.slack_usergroups import SlackUserGroupClient
from curie_api.usergroups import GroupMembershipSource


def _approval(*, author: str = "U_AE", channel: str = "C_MGRS") -> Approval:
    return Approval(
        conversation_id="th-1",
        author=author,
        summary="Discount for ACME",
        reply_channel=channel,
        reply_placeholder="p-1",
        dedupe_key="ev-1",
    )


def _authorize(
    approval: Approval,
    actor: str,
    actor_channel: str | None,
    *,
    binding: Any = None,
    group_client: GroupMembershipSource | None = None,
) -> tuple[str, AuthzDecision]:
    """The resolve endpoint's exact shape: select a set from the binding, then
    authorize on it. Selection is real, so these stay end-to-end tests of the
    binding-to-decision path rather than of the authorizer in isolation."""

    select = SlackApproverSetSelector(group_client)
    return asyncio.run(
        authorize_approval(approval, actor, actor_channel, approver_set=select(approval, binding))
    )


def _decide(approval: Approval, actor: str, channel: str | None) -> AuthzDecision:
    """The unbound path: no binding, so the card channel is the approver set."""

    _name, decision = _authorize(approval, actor, channel, binding=None)
    return decision


def test_member_of_the_approval_channel_is_allowed() -> None:
    assert _decide(_approval(), "U_MANAGER", "C_MGRS").allowed


def test_wrong_or_missing_channel_is_denied() -> None:
    for channel in ("C_OTHER", None, ""):
        decision = _decide(_approval(), "U_MANAGER", channel)
        assert not decision.allowed
        assert "not an approver" in decision.reason


def test_self_approval_is_denied_even_from_the_right_channel() -> None:
    decision = _decide(_approval(author="U_AE"), "U_AE", "C_MGRS")
    assert not decision.allowed
    assert "self-approval" in decision.reason


# --- #420: the user-list + user-group sets, and the authorizer over them -------

_GROUP = "S0MGRS001"
_AUTHOR = "U0AUTHOR1"
_APPROVER = "U0APPROV1"
_LISTED = "U0LISTED1"
_OUTSIDER = "U0OTHER01"
_CARD_CHANNEL = "C0BROAD01"
_REQUEST_CHANNEL = "C0REQ0001"


def _bound_approval(*, author: str = _AUTHOR, card_channel: str = _CARD_CHANNEL) -> Approval:
    """An approval whose card a route binding placed in ``card_channel`` (#247).

    Distinct from ``_approval`` above: the #420 story is about a card sitting in
    a BROAD channel while authority lives elsewhere, so these tests need the
    card/request channel split the route binding creates.
    """

    return Approval(
        conversation_id="th-420",
        author=author,
        summary="Discount for ACME",
        reply_channel=_REQUEST_CHANNEL,
        reply_placeholder="p-1",
        dedupe_key="ev-420",
        route="managers",
        card_channel=card_channel,
    )


def _slack(members: list[str], calls: list[httpx.Request] | None = None) -> SlackUserGroupClient:
    """A real SlackUserGroupClient over a MockTransport.

    Slack is an external service, so it is the one thing faked; the client, the
    sets, and the authorizer under test are all real. ``calls`` records every
    request that reached the transport, which is how the no-I/O contracts below
    are proven rather than asserted by inspection.
    """

    def _handler(request: httpx.Request) -> httpx.Response:
        if calls is not None:
            calls.append(request)
        return httpx.Response(200, json={"ok": True, "users": members})

    return SlackUserGroupClient(
        httpx.AsyncClient(transport=httpx.MockTransport(_handler)),
        token="xoxb-test",
    )


def _contains(approver_set: ApproverSet, actor: str, channel: str | None) -> MembershipVerdict:
    return asyncio.run(approver_set.contains(actor, channel))


# --- ExplicitUsers ------------------------------------------------------------


def test_user_list_set_contains_a_listed_actor() -> None:
    """AC1: the literal allowlist is the authority; no channel, no I/O."""

    verdict = _contains(ExplicitUsers([_APPROVER, _LISTED]), _LISTED, _CARD_CHANNEL)
    assert verdict.member


def test_user_list_set_excludes_an_unlisted_actor() -> None:
    """AC1: not on the list means no authority, even standing in the card channel."""

    verdict = _contains(ExplicitUsers([_APPROVER]), _OUTSIDER, _CARD_CHANNEL)
    assert not verdict.member
    assert "not an approver" in verdict.reason


def test_user_list_authorizer_denies_the_author_even_when_listed() -> None:
    """AC2: self-approval is blocked independent of the membership check, so an
    author who is also on the allowlist still cannot resolve their own request.

    Pinned on the authorizer, not the set: the set reports the author as a member
    (asserted here), and the refusal is the authorizer's alone."""

    assert _contains(ExplicitUsers([_AUTHOR, _APPROVER]), _AUTHOR, _CARD_CHANNEL).member

    _name, decision = _authorize(
        _bound_approval(author=_AUTHOR),
        _AUTHOR,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"users": [_AUTHOR, _APPROVER]}},
    )
    assert not decision.allowed
    assert "self-approval" in decision.reason


def test_user_list_set_ignores_the_actor_channel() -> None:
    """AC1 (the unfusing): the allowlist decides and the click channel is not
    part of the verdict -- proven in BOTH directions, so neither an
    allow-everything nor a still-checking-the-channel implementation passes."""

    approver_set = ExplicitUsers([_LISTED])

    # Listed actor, deliberately wrong channel (and no channel at all): member.
    assert _contains(approver_set, _LISTED, "C0WRONG01").member
    assert _contains(approver_set, _LISTED, None).member

    # Unlisted actor standing in exactly the card channel: still not a member.
    in_card_channel = _contains(approver_set, _OUTSIDER, _CARD_CHANNEL)
    assert not in_card_channel.member
    assert "not an approver" in in_card_channel.reason


def test_user_list_evidence_names_the_list_and_the_verdict() -> None:
    """AC3: the decision carries the authority that counted, not just the actor."""

    approver_set = ExplicitUsers([_APPROVER, _LISTED])

    allowed = _contains(approver_set, _LISTED, _CARD_CHANNEL)
    assert allowed.evidence is not None
    assert allowed.evidence["kind"] == "user_list"
    assert allowed.evidence["actor_listed"] is True
    assert sorted(allowed.evidence["users"]) == sorted([_APPROVER, _LISTED])

    denied = _contains(approver_set, _OUTSIDER, _CARD_CHANNEL)
    assert denied.evidence is not None
    assert denied.evidence["kind"] == "user_list"
    assert denied.evidence["actor_listed"] is False


# --- SlackUserGroupMembers ----------------------------------------------------


def test_user_group_set_contains_a_member() -> None:
    """AC1: membership in the bound Slack user group is the authority."""

    approver_set = SlackUserGroupMembers(_GROUP, _slack([_APPROVER]))
    assert _contains(approver_set, _APPROVER, _CARD_CHANNEL).member


def test_user_group_set_excludes_a_non_member() -> None:
    """AC1: standing in the card channel is not authority under a group binding."""

    approver_set = SlackUserGroupMembers(_GROUP, _slack([_APPROVER]))
    verdict = _contains(approver_set, _OUTSIDER, _CARD_CHANNEL)
    assert not verdict.member
    assert "not an approver" in verdict.reason


def test_user_group_set_ignores_the_actor_channel() -> None:
    """AC1 (the unfusing proof, unit level): authority is independent of card
    location. Proven in both directions -- a genuine member is a member from a
    deliberately wrong channel and with no channel evidence at all, while a
    non-member standing in exactly the card channel is still excluded."""

    approver_set = SlackUserGroupMembers(_GROUP, _slack([_APPROVER]))

    assert _contains(approver_set, _APPROVER, "C0WRONG01").member
    assert _contains(approver_set, _APPROVER, None).member

    in_card_channel = _contains(approver_set, _OUTSIDER, _CARD_CHANNEL)
    assert not in_card_channel.member
    assert "not an approver" in in_card_channel.reason


def test_user_group_authorizer_denies_the_author_even_when_a_member() -> None:
    """AC2: an author who is in the approver group still cannot self-approve.

    Pinned on the authorizer: the set would report the author as a member, and
    the authorizer refuses before it is ever asked (proven by the empty
    ``calls`` -- the group is not even fetched)."""

    calls: list[httpx.Request] = []
    _name, decision = _authorize(
        _bound_approval(author=_AUTHOR),
        _AUTHOR,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": _GROUP}},
        group_client=_slack([_AUTHOR, _APPROVER], calls),
    )
    assert not decision.allowed
    assert "self-approval" in decision.reason
    assert calls == []


def test_user_group_set_is_undetermined_when_the_lookup_failed() -> None:
    """Fail closed: a failed lookup yields no member set, so the verdict is
    undetermined -- never a member, and never quietly an empty group whose 'not
    an approver' reason would mislead the clicker into thinking policy, rather
    than infrastructure, refused them. Undetermined for every actor, including
    the author: the set does not know what an author is."""

    def _boom(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="upstream boom")

    approver_set = SlackUserGroupMembers(
        _GROUP,
        SlackUserGroupClient(
            httpx.AsyncClient(transport=httpx.MockTransport(_boom)),
            token="xoxb-test",
        ),
    )
    for actor in (_APPROVER, _OUTSIDER, _AUTHOR):
        verdict = _contains(approver_set, actor, _CARD_CHANNEL)
        assert verdict.undetermined
        assert not verdict.member
        assert "could not verify" in verdict.reason
        assert verdict.evidence is not None
        assert verdict.evidence["kind"] == "user_group"
        assert verdict.evidence["group"] == _GROUP
        assert verdict.evidence["lookup_failed"] is True


def test_user_group_set_excludes_everyone_when_the_group_is_empty() -> None:
    """Edge case 5: a valid lookup returning zero members is NOT a lookup
    failure. Nobody is a member, so the actor is excluded as a non-approver."""

    approver_set = SlackUserGroupMembers(_GROUP, _slack([]))
    verdict = _contains(approver_set, _APPROVER, _CARD_CHANNEL)
    assert not verdict.member
    assert not verdict.undetermined
    assert "not an approver" in verdict.reason
    assert verdict.evidence is not None
    assert verdict.evidence.get("lookup_failed") is not True
    assert verdict.evidence["member_count"] == 0


def test_user_group_evidence_names_the_group_and_the_verdict() -> None:
    """AC3: the group ID, the actor's verdict, and the size of the group that
    proved it. The member list itself is deliberately not carried."""

    approver_set = SlackUserGroupMembers(_GROUP, _slack([_APPROVER, _LISTED]))

    allowed = _contains(approver_set, _APPROVER, _CARD_CHANNEL)
    assert allowed.evidence is not None
    assert allowed.evidence["kind"] == "user_group"
    assert allowed.evidence["group"] == _GROUP
    assert allowed.evidence["actor_in_group"] is True
    assert allowed.evidence["member_count"] == 2

    denied = _contains(approver_set, _OUTSIDER, _CARD_CHANNEL)
    assert denied.evidence is not None
    assert denied.evidence["actor_in_group"] is False
    assert denied.evidence["member_count"] == 2


# --- SlackChannelMembers ------------------------------------------------------


def test_channel_set_compares_the_click_channel_to_the_approvers_channel() -> None:
    """The set's whole logic: the click's channel IS the membership evidence, so
    it performs no lookup and can never be undetermined."""

    approver_set = SlackChannelMembers(_CARD_CHANNEL)

    inside = _contains(approver_set, _OUTSIDER, _CARD_CHANNEL)
    assert inside.member
    assert inside.evidence is not None
    assert inside.evidence["kind"] == "channel_membership"
    assert inside.evidence["approvers_channel"] == _CARD_CHANNEL
    assert inside.evidence["actor_channel"] == _CARD_CHANNEL

    outside = _contains(approver_set, _OUTSIDER, "C0WRONG01")
    assert not outside.member
    assert not outside.undetermined
    assert "not an approver" in outside.reason
    assert outside.evidence is not None
    assert outside.evidence["actor_channel"] == "C0WRONG01"


# --- the authorizer -----------------------------------------------------------


def test_authorizer_prefers_the_explicit_user_list_over_the_group() -> None:
    """AC1 precedence (issue-stated): ``users`` wins and ``group`` is ignored,
    so a group member who is not on the list is denied and Slack is never asked."""

    calls: list[httpx.Request] = []
    binding = {
        "channel": _CARD_CHANNEL,
        "approvers": {"group": _GROUP, "users": [_LISTED]},
    }
    client = _slack([_APPROVER], calls)

    name, group_member = _authorize(
        _bound_approval(),
        _APPROVER,
        _CARD_CHANNEL,
        binding=binding,
        group_client=client,
    )
    assert name == "ExplicitUserListAuthorizer"
    assert not group_member.allowed
    assert "not an approver" in group_member.reason

    _name, listed = _authorize(
        _bound_approval(),
        _LISTED,
        _CARD_CHANNEL,
        binding=binding,
        group_client=client,
    )
    assert listed.allowed
    assert calls == []


def test_authorizer_selects_the_user_group_set_when_only_a_group_is_bound() -> None:
    """AC1: a group-only binding resolves membership through Slack and decides
    on it -- the card channel is not consulted."""

    calls: list[httpx.Request] = []
    binding = {"channel": _CARD_CHANNEL, "approvers": {"group": _GROUP}}
    client = _slack([_APPROVER], calls)

    name, decision = _authorize(
        _bound_approval(),
        _APPROVER,
        _CARD_CHANNEL,
        binding=binding,
        group_client=client,
    )
    assert name == "UserGroupAuthorizer"
    assert decision.allowed
    assert decision.evidence is not None
    assert decision.evidence["group"] == _GROUP
    assert decision.evidence["actor_in_group"] is True
    assert len(calls) == 1

    _name, outsider = _authorize(
        _bound_approval(),
        _OUTSIDER,
        _CARD_CHANNEL,
        binding=binding,
        group_client=client,
    )
    assert not outsider.allowed
    assert "not an approver" in outsider.reason


def test_authorizer_falls_back_to_channel_membership_without_approvers() -> None:
    """AC4: a binding that declares no ``approvers`` keeps today's behavior --
    the card channel's members are the approvers, nobody else is."""

    binding = {"channel": _CARD_CHANNEL}

    name, member = _authorize(_bound_approval(), _OUTSIDER, _CARD_CHANNEL, binding=binding)
    assert name == "ChannelMembershipAuthorizer"
    assert member.allowed
    assert member.evidence is not None
    assert member.evidence["kind"] == "channel_membership"
    assert member.evidence["approvers_channel"] == _CARD_CHANNEL
    assert member.evidence["actor_channel"] == _CARD_CHANNEL

    _name, elsewhere = _authorize(_bound_approval(), _OUTSIDER, "C0WRONG01", binding=binding)
    assert not elsewhere.allowed
    assert "not an approver" in elsewhere.reason
    assert elsewhere.evidence is not None
    assert elsewhere.evidence["actor_channel"] == "C0WRONG01"


def test_authorizer_falls_back_to_channel_membership_without_a_binding() -> None:
    """AC4: no binding at all (an agentless or unbound-route approval) is the
    zero-setup path and must keep resolving against the card channel."""

    name, in_channel = _authorize(_bound_approval(), _OUTSIDER, _CARD_CHANNEL, binding=None)
    assert name == "ChannelMembershipAuthorizer"
    assert in_channel.allowed

    _name, elsewhere = _authorize(_bound_approval(), _OUTSIDER, "C0WRONG01", binding=None)
    assert not elsewhere.allowed
    assert "not an approver" in elsewhere.reason


def test_authorizer_denies_a_malformed_approvers_block_without_channel_fallback() -> None:
    """Fail closed (edge case 3): an ``approvers`` block that does not parse (a
    hand-edited JSONB row, a future writer bug) is a config error, not an
    absence of policy. The actor here stands in the card channel, so a fallback
    to channel membership would ALLOW them -- a config error must never widen
    the approver set."""

    for broken in ({"group": 123}, {"users": "U0LISTED1"}, {}, {"users": []}):
        name, decision = _authorize(
            _bound_approval(),
            _OUTSIDER,
            _CARD_CHANNEL,
            binding={"channel": _CARD_CHANNEL, "approvers": broken},
            group_client=_slack([_OUTSIDER]),
        )
        assert not decision.allowed, f"malformed approvers {broken!r} must not allow"
        assert name != "ChannelMembershipAuthorizer", (
            f"malformed approvers {broken!r} fell back to channel membership"
        )


def test_authorizer_fails_closed_on_a_malformed_stored_binding() -> None:
    """Fail closed: the whole stored binding value (not just its ``approvers``
    block) is corrupted to a non-object -- a hand-edited JSONB row, a future
    writer bug. crud passes it through raw rather than coercing it to None, so a
    route an operator bound to a group must NOT silently widen to card-channel
    membership. The actor stands in the card channel, where a fallback would
    allow them."""

    for broken in ("C0CARD001", ["U0LISTED1"], 123, True):
        name, decision = _authorize(
            _bound_approval(),
            _OUTSIDER,
            _CARD_CHANNEL,
            binding=broken,
            group_client=_slack([_OUTSIDER]),
        )
        assert not decision.allowed, f"malformed binding {broken!r} must not allow"
        assert name != "ChannelMembershipAuthorizer", (
            f"malformed binding {broken!r} fell back to channel membership"
        )
        assert name == "InvalidApproversSpec"


def test_self_approval_wins_over_a_malformed_block_but_the_audit_still_names_it() -> None:
    """Ordering: self-approval is refused before the set is asked, so an author
    self-clicking an unreadable block is told about the self-approval rather than
    the spec. Both deny, so nothing is widened.

    The audit row still records ``InvalidApproversSpec``, because the name comes
    from the selected set: an operator reading the trail can still see the block
    was unreadable, which is the fact the reason string no longer carries."""

    name, decision = _authorize(
        _bound_approval(author=_AUTHOR),
        _AUTHOR,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": 123}},
    )
    assert name == "InvalidApproversSpec"
    assert not decision.allowed
    assert "self-approval" in decision.reason


def test_authorizer_denies_self_approval_before_fetching_the_group() -> None:
    """AC2 + the pre-fetch guard's no-I/O property: the author is refused
    before any Slack call is made, so a self-attempt against a group-bound route
    spends no rate-limit budget and cannot be used to probe the group."""

    calls: list[httpx.Request] = []
    _name, decision = _authorize(
        _bound_approval(author=_AUTHOR),
        _AUTHOR,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": _GROUP}},
        group_client=_slack([_AUTHOR, _APPROVER], calls),
    )
    assert not decision.allowed
    assert "self-approval" in decision.reason
    assert calls == []


def test_authorizer_fails_closed_when_no_slack_client_is_configured() -> None:
    """Fail closed: a group binding with no bot token wired into the API cannot
    be verified. It denies with the could-not-verify reason; it does not degrade
    into channel membership (the actor below is in the card channel)."""

    name, decision = _authorize(
        _bound_approval(),
        _OUTSIDER,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": _GROUP}},
        group_client=None,
    )
    assert not decision.allowed
    assert "could not verify" in decision.reason
    assert name != "ChannelMembershipAuthorizer"


def test_authorizer_fails_closed_when_the_slack_lookup_errors() -> None:
    """Fail closed: Slack answering 500 denies and records the failure; it never
    falls back to the card channel (which would allow this actor)."""

    def _boom(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="upstream boom")

    client = SlackUserGroupClient(
        httpx.AsyncClient(transport=httpx.MockTransport(_boom)),
        token="xoxb-test",
    )
    name, decision = _authorize(
        _bound_approval(),
        _OUTSIDER,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": _GROUP}},
        group_client=client,
    )
    assert not decision.allowed
    assert "could not verify" in decision.reason
    assert name != "ChannelMembershipAuthorizer"
    assert decision.evidence is not None
    assert decision.evidence["kind"] == "user_group"
    assert decision.evidence["group"] == _GROUP
    assert decision.evidence["lookup_failed"] is True
    assert decision.evidence["error"]
