"""Slack's two approver sets, and the selector that reads a binding (#420, ADR-0034).

Both of Slack's ways of saying "who is authorized" live here: everyone in this
room, and everyone on this list Slack maintains. They look like different kinds
of thing -- one is where the card posted, the other is a real fetch -- but both
are Slack expressing an approver set, which is why they are two implementations
of one port rather than an authorizer each. ``approvers.ExplicitUsers`` is the
only set that owes Slack nothing.

Selection lives here too, and that is not an accident of layering. Reading a
binding means parsing ``ApprovalApprovers``, whose schema validates ``S...``
usergroup IDs and ``C...`` channel IDs: the binding format is Slack's shape, so
the code that reads it is Slack-aware and belongs on this side of the port. That
is what lets ``authorizer.py`` be pure policy with no Slack in it at all.

The evidence asymmetry is deliberate and worth stating plainly. ``SlackChannelMembers``
performs no lookup: the click's channel IS the proof, because Slack only renders
a card (and only accepts clicks on it) for members of the channel it posted in,
and the dispatcher relays that channel over its authenticated Socket Mode
connection. ``SlackUserGroupMembers`` has no such free evidence and must ask
Slack, so it is the only set here that can come back undetermined.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from .approvers import ApproverSet, ExplicitUsers, InvalidApprovers, MembershipVerdict
from .models import Approval
from .schemas import ApprovalApprovers
from .usergroups import GroupMembershipSource, UserGroupLookupError


class SlackChannelMembers:
    """The card channel's members are the approvers (#246): the zero-setup default.

    Membership is proven by the resolution attempt's channel -- ``card_channel``
    when a route binding placed the card (#247), else the requesting channel.
    Callers that are not the dispatcher (an operator's curl, the CLI) authenticate
    with the platform API key and assert the channel explicitly.
    """

    # Frozen to the pre-port class name; see the audit-vocabulary note in
    # authorizer.py.
    audit_name = "ChannelMembershipAuthorizer"

    def __init__(self, approvers_channel: str | None) -> None:
        self._approvers_channel = approvers_channel

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        evidence: dict[str, Any] = {
            "kind": "channel_membership",
            "approvers_channel": self._approvers_channel,
            "actor_channel": actor_channel,
        }
        if actor_channel != self._approvers_channel:
            return MembershipVerdict(
                member=False,
                reason="you are not an approver: resolve this from the approval's channel",
                evidence=evidence,
            )
        return MembershipVerdict(member=True, evidence=evidence)


class SlackUserGroupMembers:
    """A Slack user group's members are the approvers (#420).

    Owns its lookup: it holds the ``GroupMembershipSource`` port and fetches
    inside ``contains``, rather than being handed a member set the caller already
    resolved. ``source`` is None when no bot token is configured, which is a
    normal Slack-free deployment and not an error -- it simply cannot determine
    membership, so a route bound to a group fails closed.

    Like the user list, the click channel plays no part.
    """

    # Frozen to the pre-port class name; see the audit-vocabulary note in
    # authorizer.py.
    audit_name = "UserGroupAuthorizer"

    def __init__(self, group_id: str, source: GroupMembershipSource | None) -> None:
        self._group_id = group_id
        self._source = source

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        if self._source is None:
            return self._undetermined("no Slack bot token is configured for the API")
        try:
            membership = await self._source.members(self._group_id)
        except UserGroupLookupError as exc:
            # The class name, not the message: the message carries the group ID
            # and upstream text, and this lands in an append-only table.
            return self._undetermined(type(exc).__name__)
        in_group = actor in membership.users
        # The member list itself is deliberately not evidence: a 500-member group
        # would bloat an append-only table on every click. The group, the actor's
        # verdict, the size of the set that proved it, and the age of the fetch
        # are the snapshot.
        evidence: dict[str, Any] = {
            "kind": "user_group",
            "group": self._group_id,
            "actor_in_group": in_group,
            "member_count": len(membership.users),
            "fetched_at": membership.fetched_at.isoformat(),
            "cache_age_s": membership.cache_age_s,
        }
        if not in_group:
            return MembershipVerdict(
                member=False,
                reason=(
                    "you are not an approver: this approval's route is bound to "
                    "a Slack user group you are not a member of"
                ),
                evidence=evidence,
            )
        return MembershipVerdict(member=True, evidence=evidence)

    def _undetermined(self, error: str) -> MembershipVerdict:
        """No member set, so no verdict: the authorizer fails closed on this.

        Kept distinct from an empty group on purpose. Both refuse, but the
        clicker needs to know the lookup, not the rule, is what stopped them.
        """

        return MembershipVerdict(
            member=False,
            undetermined=True,
            reason=(
                "could not verify approver group membership: this approval's "
                "route is bound to a Slack user group and the membership "
                "lookup failed"
            ),
            evidence={
                "kind": "user_group",
                "group": self._group_id,
                "lookup_failed": True,
                "error": error,
            },
        )


class SlackApproverSetSelector:
    """Reads a route binding and picks the set it calls for (``ApproverSetSelector``).

    Holds the ``GroupMembershipSource`` so it can hand it to a user-group set;
    None when no bot token is configured, which is a normal Slack-free deployment
    (a route bound to a group then fails closed at resolve time).
    """

    def __init__(self, group_client: GroupMembershipSource | None) -> None:
        self._group_client = group_client

    def __call__(self, approval: Approval, binding: Any) -> ApproverSet:
        """Precedence, exactly as issue #420 states it: ``users`` wins over
        ``group``, which wins over channel membership. No I/O happens here."""

        approvers, spec_error = _parse_approvers(binding)
        if spec_error is not None:
            # A spec that does not parse is a config error, not an absence of
            # policy: falling back to channel membership here would widen the
            # approver set to everyone in the card's channel -- the opposite of
            # what the binding was trying to say.
            return InvalidApprovers(spec_error)
        if approvers is None:
            # No approvers declared: the card channel's members are the approvers,
            # exactly as before #420 (AC4).
            return SlackChannelMembers(approval.card_channel or approval.reply_channel)
        if approvers.users:
            return ExplicitUsers(approvers.users)
        group = approvers.group
        if group is None:
            # Unreachable via the schema, which rejects an approvers block
            # declaring neither. Written as a branch rather than an assert so it
            # stays a real refusal: an assert is stripped under `python -O`, and
            # the line below would then bind a set to a group named None. A block
            # the platform cannot make sense of denies, exactly as one that does
            # not parse does.
            return InvalidApprovers("approvers block declares neither users nor group")
        return SlackUserGroupMembers(group, self._group_client)


def _parse_approvers(
    binding: Any,
) -> tuple[ApprovalApprovers | None, str | None]:
    """Read the binding's approvers block: ``(spec, None)`` when one is declared
    and parses, ``(None, None)`` when none is declared, ``(None, error)`` when
    one is declared but does not parse.

    Re-validates the approvers block itself at read time, not the whole
    binding: malformed approvers content fails closed here rather than becoming
    an unenforceable binding. A typo'd sibling key (``approver`` for
    ``approvers``) is caught at write time instead, by the model's
    ``extra="forbid"``, which is sufficient because the API is the binding's
    only writer. Absent and null are treated identically -- bindings written
    before #420 have no key at all.

    A binding that is present but not a JSON object (a hand-edited JSONB row, a
    future writer bug) fails closed here, distinct from the absent None above: a
    corrupted binding must not silently widen a route an operator bound to a
    group down to card-channel membership.
    """

    if binding is None:
        return None, None
    if not isinstance(binding, Mapping):
        return None, "route binding is not a JSON object"
    spec = binding.get("approvers")
    if spec is None:
        return None, None
    try:
        return ApprovalApprovers.model_validate(spec), None
    except ValidationError as exc:
        return None, type(exc).__name__
