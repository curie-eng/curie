"""Authorizer and approver-set unit tests (#246, #420).

The originals below pin the channel-membership behavior (#246), now driven
through ``authorize_approval`` on an unbound approval, which is the path
production takes to reach that set. Everything after the divider pins #420: the
explicit-user-list and user-group sets that unfuse "who may approve" from "where
the card posted", plus the authorizer that selects between them. Under ADR-0106
the selected set is the authorization boundary even for the requester. Slack is
the only external service here and it is
reached exclusively through a MockTransport-backed real client.

The ADR-0123 (#1081) section pins the last piece: an ABSENT binding is not the
same fact as a binding that declares no approvers. These go through the same
``_authorize`` helper, so they stay binding-to-decision tests of the real
selector rather than assertions about a class in isolation -- which is also why
the new set is never imported here by name.
"""

import asyncio
from typing import Any

import httpx
from curie_api.approvers import ApproverSet, EmailApprovers, ExplicitUsers, MembershipVerdict
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
        # NOT NULL in the table; every real row names its asking kind, and a
        # non-Slack one selects an email or no-approver set instead (ADR-0177 amendment).
        reply_kind="slack",
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
    principal_kind: str = "chat",
) -> tuple[str, AuthzDecision]:
    """The resolve endpoint's exact shape: select a set from the binding, then
    authorize on it. Selection is real, so these stay end-to-end tests of the
    binding-to-decision path rather than of the authorizer in isolation."""

    select = SlackApproverSetSelector(group_client)
    return asyncio.run(
        authorize_approval(
            approval,
            actor,
            actor_channel,
            approver_set=select(approval, binding),
            principal_kind=principal_kind,
        )
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


def test_requester_in_the_approval_channel_is_allowed_with_channel_evidence() -> None:
    decision = _decide(_approval(author="U_AE"), "U_AE", "C_MGRS")
    assert decision.allowed
    assert decision.evidence is not None
    assert decision.evidence["kind"] == "channel_membership"
    assert decision.evidence["approvers_channel"] == "C_MGRS"
    assert decision.evidence["actor_channel"] == "C_MGRS"


# --- #420: the user-list + user-group sets, and the authorizer over them -------

_GROUP = "S0MGRS001"
_AUTHOR = "U0AUTHOR1"
_APPROVER = "U0APPROV1"
_LISTED = "U0LISTED1"
_OUTSIDER = "U0OTHER01"
_CARD_CHANNEL = "C0BROAD01"
_REQUEST_CHANNEL = "C0REQ0001"


def _bound_approval(
    *,
    author: str = _AUTHOR,
    card_channel: str = _CARD_CHANNEL,
    route: str = "managers",
) -> Approval:
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
        route=route,
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


def test_user_list_authorizer_allows_the_requester_when_listed() -> None:
    """ADR-0106: the selected list decides even when actor equals requester."""

    assert _contains(ExplicitUsers([_AUTHOR, _APPROVER]), _AUTHOR, _CARD_CHANNEL).member

    name, decision = _authorize(
        _bound_approval(author=_AUTHOR),
        _AUTHOR,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"users": [_AUTHOR, _APPROVER]}},
    )
    assert name == "ExplicitUserListAuthorizer"
    assert decision.allowed
    assert decision.evidence is not None
    assert decision.evidence["kind"] == "user_list"
    assert decision.evidence["actor_listed"] is True


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


def test_user_group_authorizer_allows_the_requester_when_a_member() -> None:
    """ADR-0106: requester membership is fetched and decides normally."""

    calls: list[httpx.Request] = []
    name, decision = _authorize(
        _bound_approval(author=_AUTHOR),
        _AUTHOR,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": _GROUP}},
        group_client=_slack([_AUTHOR, _APPROVER], calls),
    )
    assert name == "UserGroupAuthorizer"
    assert decision.allowed
    assert decision.evidence is not None
    assert decision.evidence["actor_in_group"] is True
    assert len(calls) == 1


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


# --- ADR-0123 (#1081): an absent binding is not "no approvers declared" -------
#
# The axis under test is `_bound_approval()` (route="managers") against
# `_approval()` (no route), each with `binding=None`. Same missing binding, two
# different answers, because the Decision splits on whether the approval NAMED
# a route. Everything below the divider that the #420 tests already cover
# (users-beats-group precedence, the group lookup, the malformed-block and
# malformed-binding fail-closed cases) must stay green untouched.


def test_authorizer_refuses_a_routed_approval_whose_binding_is_gone() -> None:
    """ADR-0123 (#1081): a pending approval that NAMED a route is resolvable
    only through that route's binding. With the binding absent there is no set
    to resolve and no channel fallback applies.

    This retargets the old ``..._without_a_binding`` test, whose "an agentless
    or unbound-route approval is the zero-setup path" claim ADR-0123 supersedes.
    Falling through to the card channel swaps a server-enforced approver set for
    a caller-asserted ``actor_channel`` check on an approval that is ALREADY
    pending, which is the whole escalation. The actor below stands IN the card
    channel, so the old behavior ALLOWED them: reverting the selector split
    flips this test to allowed and fails it, which is what makes it
    red-on-revert rather than vacuously green.

    A route bound to JSON ``null`` reaches the selector as ``binding=None`` too
    (crud returns a bare ``None`` for it), so that edge case is this case.
    """

    name, in_channel = _authorize(_bound_approval(), _OUTSIDER, _CARD_CHANNEL, binding=None)
    assert name == "UnboundRouteBinding"
    assert not in_channel.allowed
    assert "no longer bound" in in_channel.reason
    assert in_channel.evidence is not None
    assert in_channel.evidence["kind"] == "route_binding"
    assert in_channel.evidence["route"] == "managers"
    assert in_channel.evidence["binding_present"] is False

    # The set admits nobody, so the wrong channel is refused for the same
    # reason rather than the channel-membership one.
    elsewhere_name, elsewhere = _authorize(_bound_approval(), _OUTSIDER, "C0WRONG01", binding=None)
    assert elsewhere_name == "UnboundRouteBinding"
    assert not elsewhere.allowed
    assert "no longer bound" in elsewhere.reason


def test_authorizer_keeps_channel_membership_for_a_routeless_approval() -> None:
    """ADR-0123's third bullet: an approval that named NO route never had a
    narrower set to lose, so an absent binding is still the #420 AC4 zero-setup
    default. ``_decide`` at the top of this file already walks the decision;
    what is pinned here is the audit NAME, which is the half that moves
    silently if the selector splits on the binding alone instead of on
    ``approval.route and binding is None``."""

    name, decision = _authorize(_approval(), "U_MANAGER", "C_MGRS", binding=None)
    assert name == "ChannelMembershipAuthorizer"
    assert decision.allowed


def test_authorizer_treats_an_empty_binding_as_present_not_absent() -> None:
    """ADR-0123 edge case: a route bound to ``{}`` is BOUND. The operator
    declared no approvers; they did not remove the route. Only ``None`` is
    absence, so this keeps channel membership and the routed actor standing in
    the card channel is allowed.

    An implementation written as ``if approval.route and not binding`` instead
    of ``binding is None`` refuses here. This is the exact off-by-one, and this
    test is the only thing that catches it."""

    name, decision = _authorize(_bound_approval(), _OUTSIDER, _CARD_CHANNEL, binding={})
    assert name == "ChannelMembershipAuthorizer"
    assert decision.allowed


def test_an_empty_route_string_is_routeless_and_keeps_channel_membership() -> None:
    """Two files must agree on what "named a route" means, and they agree by
    TRUTHINESS, not by ``is not None``.

    ``crud.get_approval_route_binding`` returns early on ``not approval.route``,
    so an approval carrying ``route=""`` is already routeless there and yields a
    ``None`` binding. The selector's fail-closed branch must key on the same
    truthiness (``approval.route and binding is None``). An implementer writing
    ``approval.route is not None`` instead would refuse this approval in the
    selector while crud had already classified it as routeless -- a silent
    divergence between two files, in the fail-closed direction, that nothing
    else pins.
    """

    name, decision = _authorize(_bound_approval(route=""), _OUTSIDER, _CARD_CHANNEL, binding=None)
    assert name == "ChannelMembershipAuthorizer"
    assert decision.allowed


def test_an_unbound_route_and_an_unreadable_block_stay_distinct_in_the_audit() -> None:
    """Audit vocabulary (approvers.py's frozen ``audit_name`` strings): both
    undetermined sets refuse everyone, but an operator reading the append-only
    trail must be able to tell "the route you named is gone" from "the
    approvers block does not parse". ADR-0123 adds a NEW string and renames
    nothing, so the malformed case must NOT drift onto the new name."""

    gone, _gone_decision = _authorize(_bound_approval(), _OUTSIDER, _CARD_CHANNEL, binding=None)
    unreadable, decision = _authorize(
        _bound_approval(),
        _OUTSIDER,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": 123}},
    )
    assert gone == "UnboundRouteBinding"
    assert unreadable == "InvalidApproversSpec"
    assert not decision.allowed
    assert decision.evidence is not None
    assert decision.evidence["kind"] == "approvers_config"


def test_requester_with_a_missing_binding_is_denied_for_the_binding() -> None:
    """Requester equality cannot bypass or mask ADR-0123's missing-route set."""

    name, decision = _authorize(
        _bound_approval(author=_AUTHOR), _AUTHOR, _CARD_CHANNEL, binding=None
    )
    assert name == "UnboundRouteBinding"
    assert not decision.allowed
    assert "no longer bound" in decision.reason
    assert decision.evidence is not None
    assert decision.evidence["kind"] == "route_binding"
    assert decision.evidence["route"] == "managers"
    assert decision.evidence["binding_present"] is False


def test_operator_with_a_missing_binding_is_denied_for_the_binding() -> None:
    """Principal eligibility must not disguise an ADR-0123 config failure."""

    name, decision = _authorize(
        _bound_approval(),
        _OUTSIDER,
        None,
        binding=None,
        principal_kind="operator",
    )
    assert name == "UnboundRouteBinding"
    assert not decision.allowed
    assert "no longer bound" in decision.reason
    assert decision.evidence is not None
    assert decision.evidence == {
        "kind": "route_binding",
        "route": "managers",
        "binding_present": False,
    }


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


def test_requester_with_a_malformed_block_is_denied_for_the_config() -> None:
    """Requester equality cannot bypass or mask an unreadable approver set."""

    name, decision = _authorize(
        _bound_approval(author=_AUTHOR),
        _AUTHOR,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": 123}},
    )
    assert name == "InvalidApproversSpec"
    assert not decision.allowed
    assert "could not verify approvers" in decision.reason
    assert decision.evidence is not None
    assert decision.evidence["kind"] == "approvers_config"
    assert decision.evidence["error"]


def test_operator_with_a_malformed_block_is_denied_for_the_config() -> None:
    """An unreadable set denies before policy eligibility can mislabel it."""

    name, decision = _authorize(
        _bound_approval(),
        _OUTSIDER,
        None,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": 123}},
        principal_kind="operator",
    )
    assert name == "InvalidApproversSpec"
    assert not decision.allowed
    assert "could not verify approvers" in decision.reason
    assert decision.evidence is not None
    assert decision.evidence["kind"] == "approvers_config"
    assert decision.evidence["error"]


def test_requester_not_in_the_group_is_looked_up_then_denied() -> None:
    """The selected set runs for a requester and its negative verdict controls."""

    calls: list[httpx.Request] = []
    _name, decision = _authorize(
        _bound_approval(author=_AUTHOR),
        _AUTHOR,
        _CARD_CHANNEL,
        binding={"channel": _CARD_CHANNEL, "approvers": {"group": _GROUP}},
        group_client=_slack([_APPROVER], calls),
    )
    assert not decision.allowed
    assert "not an approver" in decision.reason
    assert decision.evidence is not None
    assert decision.evidence["actor_in_group"] is False
    assert decision.evidence["member_count"] == 1
    assert len(calls) == 1


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


# --- ADR-0177 amendment: approver emails for a card shown in an email thread ---

_INBOX = "bot@example.com"
_EMAIL_APPROVER = "approver@example.com"
_EMAIL_REQUESTER = "requester@example.com"
_SURFACE = {"mode": "requesting_surface"}


def _email_card(*, kind: str = "email", route: str | None = "confirm") -> Approval:
    """An approval asked in ``kind``'s thread whose card was shown there."""

    return Approval(
        conversation_id="th-mail",
        author=_EMAIL_REQUESTER,
        summary="Send the quote",
        reply_kind=kind,
        reply_channel=_INBOX,
        reply_placeholder=None,
        dedupe_key="ev-mail",
        route=route,
        card_channel=_INBOX,
    )


def _listing(*emails: str) -> dict[str, Any]:
    return {"resolution": _SURFACE, "approvers": {"emails": list(emails)}}


def test_the_email_list_admits_a_listed_sender_from_an_adapter_only() -> None:
    name, decision = _authorize(
        _email_card(),
        _EMAIL_APPROVER.upper(),
        None,
        binding=_listing(_EMAIL_APPROVER),
        principal_kind="adapter",
    )
    assert (name, decision.allowed) == ("EmailApproverList", True)
    for kind in ("operator", "console", "chat"):
        name, decision = _authorize(
            _email_card(),
            _EMAIL_APPROVER,
            _INBOX,
            binding=_listing(_EMAIL_APPROVER),
            principal_kind=kind,
        )
        assert (name, decision.allowed) == ("EmailApproverList", False), kind
        assert decision.evidence == {
            "kind": "principal_set_eligibility",
            "principal_kind": kind,
            "approver_set": "EmailApproverList",
        }


def test_the_email_list_refuses_the_requester_it_does_not_name() -> None:
    _name, decision = _authorize(
        _email_card(),
        _EMAIL_REQUESTER,
        None,
        binding=_listing(_EMAIL_APPROVER),
        principal_kind="adapter",
    )
    assert not decision.allowed
    assert decision.evidence is not None and decision.evidence["actor_listed"] is False


def test_an_empty_email_list_is_undetermined_not_a_verdict() -> None:
    """The schema refuses an empty list; a set built around it must still fail
    closed, and say the configuration stopped the sender."""

    verdict = asyncio.run(EmailApprovers([]).contains(_EMAIL_APPROVER, None))
    assert (verdict.member, verdict.undetermined) == (False, True)
    assert "lists no approver email addresses" in verdict.reason


def test_no_list_on_email_admits_nobody_not_the_requester() -> None:
    """ADR-0177 amendment A3: routeless, a route without approvers, and a route
    with only Slack approvers all admit nobody on an email card."""

    for route, binding in (
        (None, None),
        ("confirm", {"resolution": _SURFACE}),
        ("confirm", {"resolution": _SURFACE, "approvers": {"users": [_APPROVER]}}),
    ):
        name, decision = _authorize(
            _email_card(route=route),
            _EMAIL_REQUESTER,
            None,
            binding=binding,
            principal_kind="adapter",
        )
        assert (name, decision.allowed) == ("NoVerifiableApprovers", False), binding
        assert "its route lists none" in decision.reason


def test_an_email_list_is_never_read_on_another_non_slack_channel() -> None:
    """Only the mail adapter verifies an address. Another channel's adapter
    naming a listed address proves nothing about it."""

    name, decision = _authorize(
        _email_card(kind="webchat"),
        _EMAIL_APPROVER,
        None,
        binding=_listing(_EMAIL_APPROVER),
        principal_kind="adapter",
    )
    assert (name, decision.allowed) == ("NoVerifiableApprovers", False)
    assert "webchat has no approver list" in decision.reason


def test_test_driver_resolves_only_explicit_users() -> None:
    from curie_api.approvers import InvalidApprovers, NoVerifiableApprovers, UnboundRoute

    async def decide(approvers: ApproverSet) -> AuthzDecision:
        _, verdict = await authorize_approval(
            _approval(),
            _LISTED,
            _CARD_CHANNEL,
            approver_set=approvers,
            principal_kind="test_driver",
        )
        return verdict

    assert asyncio.run(decide(ExplicitUsers([_LISTED]))).allowed
    assert not asyncio.run(decide(ExplicitUsers([_OUTSIDER]))).allowed
    for approvers in [
        SlackChannelMembers(_CARD_CHANNEL),
        SlackUserGroupMembers(_GROUP, None),
        EmailApprovers(["approver@example.com"]),
        InvalidApprovers("invalid"),
        UnboundRoute("missing"),
        NoVerifiableApprovers("discord", slack_declared=False),
    ]:
        verdict = asyncio.run(decide(approvers))
        assert not verdict.allowed
        assert verdict.evidence is not None
        assert verdict.evidence["principal_kind"] == "test_driver"
