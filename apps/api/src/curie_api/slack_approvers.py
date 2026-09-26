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

Selection also decides the ABSENT-binding case (ADR-0123), and the set it
returns for it lives in ``approvers.py`` rather than here: "this route is not
bound" is a fact about ``agents.approval_routes``, not about Slack, so
``approvers.UnboundRoute`` is provider-neutral even though only a Slack-aware
selector can tell that case apart from a binding that declares no approvers.

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

import httpx
from aci_protocol.turn import DEFAULT_IDENTITY, slack_speaking_identity
from pydantic import ValidationError

from .approvers import (
    ApproverSet,
    ExplicitUsers,
    InvalidApprovers,
    MembershipVerdict,
    UnboundRoute,
)
from .config import Settings
from .identities import slack_bot_tokens
from .models import Approval
from .schemas import ApprovalApprovers
from .slack_usergroups import SlackUserGroupClient
from .usergroups import GroupMembershipSource, UserGroupLookupError

_NO_API_TOKEN = "no Slack bot token is configured for the API"


class SlackChannelMembers:
    """The card channel's members are the approvers (#246): the zero-setup default.

    Membership is proven by the resolution attempt's channel -- ``card_channel``
    when a route binding placed the card (#247), else the requesting channel.
    Only a dispatcher-attested chat principal carries that channel. Operator and
    console principals carry none and therefore cannot satisfy this set.
    """

    # Frozen to the pre-port class name; see the audit-vocabulary note in
    # authorizer.py.
    audit_name = "ChannelMembershipAuthorizer"
    operator_eligible = False
    console_eligible = False

    def __init__(self, approvers_channel: str | None) -> None:
        self._approvers_channel = approvers_channel

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        evidence: dict[str, Any] = {
            "kind": "channel_membership",
            "approvers_channel": self._approvers_channel,
            "actor_channel": actor_channel,
        }
        if (
            not actor_channel
            or not self._approvers_channel
            or actor_channel != self._approvers_channel
        ):
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
    operator_eligible = False
    # A subject-bound console session proves the actor identity needed for the
    # server-side Slack lookup; unlike channel membership, no channel evidence
    # is required. Operator tokens remain explicit-user-only per ADR-0106.
    console_eligible = True

    def __init__(
        self,
        group_id: str,
        source: GroupMembershipSource | None,
        *,
        missing_source_reason: str = _NO_API_TOKEN,
    ) -> None:
        self._group_id = group_id
        self._source = source
        self._missing_source_reason = missing_source_reason

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        if self._source is None:
            return self._undetermined(self._missing_source_reason)
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


def approval_token_identity(approval: Approval) -> str:
    """The Slack identity whose token resolves this approval's groups (ADR-0168 decision 5).

    The rule is ``aci_protocol.turn.slack_speaking_identity``, shared with the
    worker so the two cannot disagree about a route.
    """

    kind = approval.reply_kind or ""
    return slack_speaking_identity(kind, approval.reply_adapter, approval.reply_endpoint)


class SlackApproverSetSelector:
    """Reads a route binding and picks the set it calls for (``ApproverSetSelector``).

    Holds the ``GroupMembershipSource`` so it can hand it to a user-group set;
    None when no bot token is configured, which is a normal Slack-free deployment
    (a route bound to a group then fails closed at resolve time).
    ``identity_clients`` holds each named Slack identity's own source
    (ADR-0168 decision 5); ``group_client`` is ``default``'s.

    The no-approvers case is a three-way split, not a single fallback
    (ADR-0123): a binding present with no ``approvers`` block and a routeless
    approval both keep channel membership, while an approval that NAMED a route
    with no binding to read is refused outright.
    """

    def __init__(
        self,
        group_client: GroupMembershipSource | None,
        *,
        identity_clients: Mapping[str, GroupMembershipSource] | None = None,
    ) -> None:
        self._group_client = group_client
        self._identity_clients = dict(identity_clients or {})

    def __call__(self, approval: Approval, binding: Any) -> ApproverSet:
        """Precedence, exactly as issue #420 states it: ``users`` wins over
        ``group``, which wins over channel membership. No I/O happens here.

        Channel membership is not the universal fallback it once was: an
        approval that named a route whose binding is absent is refused outright
        rather than falling through to it (ADR-0123). That split keys on
        ``binding is None``, NOT on the parsed approvers block -- ``_parse_approvers``
        returns ``(None, None)`` for both "no binding at all" and "binding
        present, no approvers declared", and conflating those two is the defect.
        """

        approvers, spec_error = _parse_approvers(binding)
        if spec_error is not None:
            # A spec that does not parse is a config error, not an absence of
            # policy: falling back to channel membership here would widen the
            # approver set to everyone in the card's channel -- the opposite of
            # what the binding was trying to say.
            return InvalidApprovers(spec_error)
        if approvers is None:
            if approval.route and binding is None:
                # The approval NAMED a route and there is no binding left to
                # read (ADR-0123). Not the same fact as a binding that declares
                # no approvers: falling through would let whoever rewrote the
                # route map swap a server-enforced approver set for a
                # channel-membership check on an approval that is ALREADY
                # pending, which is the whole escalation.
                #
                # ``binding is None`` and not ``not binding``: a route bound to
                # ``{}`` is BOUND, the operator just declared nothing, and only
                # ``None`` is absence. The truthiness test on ``approval.route``
                # is deliberate too -- ``crud.get_approval_route_binding``
                # returns early on ``not approval.route``, so keying on
                # ``is not None`` here would refuse a ``route=""`` approval that
                # crud has already classified as routeless.
                return UnboundRoute(approval.route)
            # A binding that is present and declares no approvers, or an approval
            # that named no route at all: the card channel's members are the
            # approvers, exactly as before #420 (AC4).
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
        identity = approval_token_identity(approval)
        if identity == DEFAULT_IDENTITY:
            return SlackUserGroupMembers(group, self._group_client)
        return SlackUserGroupMembers(
            group,
            self._identity_clients.get(identity),
            missing_source_reason=f"{_NO_API_TOKEN} for identity {identity!r}",
        )


def build_approver_set_selector(
    http: httpx.AsyncClient,
    settings: Settings,
    *,
    environ: Mapping[str, str] | None = None,
) -> SlackApproverSetSelector:
    """The API's selector, with one user-group client per Slack identity.

    Each client keeps its own member cache, so one bot's answer never stands
    in for another's. A stock install builds the one client, or none, that it
    always built.
    """

    clients = {
        name: SlackUserGroupClient(http, token=token, ttl_s=settings.slack_usergroup_cache_ttl_s)
        for name, token in slack_bot_tokens(settings, environ=environ).items()
    }
    return SlackApproverSetSelector(
        clients.pop(DEFAULT_IDENTITY, None), identity_clients=clients
    )


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
    ``extra="forbid"``.

    That guard used to be described here as sufficient because the API was the
    binding's only writer. It no longer is: the CLI's ``--routes-from`` (#1057)
    is a second writer, and because it parsed the operator's file into a struct
    and re-serialized it, the operator's own bytes never reached this model at
    all -- an unknown key was dropped before the request was built, and the
    channel-only binding that survived widened the approver set to the whole
    card channel (#1072). Each writer now refuses unknown keys on its own input
    (``cli/src/api.rs``'s ``RouteBindingInput``); this model stays the
    authoritative gate for anything that does reach it. A future writer that
    re-serializes rather than forwarding raw bytes owes the same check.

    Absent and null are treated identically -- bindings written before #420
    have no key at all.

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
