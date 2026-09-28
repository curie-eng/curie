"""The approver-set port: who counts as an approver (#420, ADR-0034).

An ``ApproverSet`` answers whether an actor belongs to the set and which
principal kinds it can be evaluated for: a channel-less operator or console
credential, or an adapter principal carrying another channel's sender.  That
second fact prevents a terminal, a browser or an adapter manufacturing
provider membership evidence.  Requester equality is deliberately absent: ADR-0106
makes set membership the authorization boundary for every requester.

Two axes decide an approval today, and they are not symmetrical:

- A set may prove membership from what the caller already presented. Channel
  membership does this: the click's channel IS the evidence, so it performs no
  lookup. That is why ``contains`` takes ``actor_channel`` at all.
- A set may go and find out. A user group does this, and it can fail.

``MembershipVerdict.undetermined`` carries that second case. It is not
``member=False``: "you are not in the set" and "we could not find out" are
different facts, they deny for different reasons, and telling a clicker the
first when the second is true sends them arguing with policy over an outage.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from aci_protocol.turn import SLACK_KIND

from .models import Approval
from .schemas import REQUESTING_SURFACE_MODE

# The audit vocabulary is FROZEN, and each set pins its own ``audit_name`` to the
# class name it had before ADR-0034 turned it from an authorizer into a set. The
# strings therefore read oddly now ("...Authorizer" naming a set), and that is
# the trade: `approval_audit.authorizer` is append-only history, rows already on
# main carry these values, and renaming the vocabulary mid-stream would make
# every old row lie about what decided. New vocabulary needs a new column, not a
# redefinition of this one.


@dataclass(frozen=True)
class MembershipVerdict:
    """A set's answer about one actor.

    ``undetermined=True`` means the set could not establish membership at all;
    ``member`` is then meaningless and the authorizer fails closed. ``reason``
    is rendered to the clicker, so it is the set's job: only the set knows
    whether it refused on a list, a group, or a channel. ``evidence`` is the
    snapshot the audit row stores.
    """

    member: bool
    undetermined: bool = False
    reason: str = ""
    evidence: dict[str, Any] | None = None


class ApproverSet(Protocol):
    """The set of actors permitted to resolve one approval.

    ``audit_name`` is the string the audit row's ``authorizer`` column records.
    ``contains`` is async because a set may perform its own lookup; the ones that
    do not simply never await.
    """

    @property
    def audit_name(self) -> str: ...

    @property
    def operator_eligible(self) -> bool: ...

    @property
    def console_eligible(self) -> bool: ...

    @property
    def adapter_eligible(self) -> bool: ...

    @property
    def chat_eligible(self) -> bool: ...

    @property
    def ineligible_reason(self) -> str | None:
        """What to tell an ineligible principal, or None for the authorizer's
        per-kind default. A set whose eligibility is not about Slack evidence
        says so itself, rather than the authorizer learning the set exists."""
        ...

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict: ...


class ExplicitUsers:
    """A literal allowlist of user IDs (#420): the only provider-neutral set.

    Pure config, so it decides while every upstream is unreachable, and it can
    never report ``undetermined``. The click channel plays no part -- that is the
    whole point of unfusing authority from card location -- so a listed approver
    may resolve from anywhere and an unlisted one may not, however deep in the
    card's channel they are standing.
    """

    # Frozen; see the audit-vocabulary note above.
    audit_name = "ExplicitUserListAuthorizer"
    operator_eligible = True
    console_eligible = True
    # The list holds Slack user IDs (``ApprovalApprovers`` validates them), and
    # only the Slack dispatcher vouches for a Slack ID (ADR-0106). An adapter
    # principal authenticated some other channel's sender, so letting it name a
    # listed ID would let any adapter serving the binding approve as that
    # person (ADR-0177, "A separate finding").
    adapter_eligible = False
    chat_eligible = True
    ineligible_reason = None

    def __init__(self, users: Sequence[str]) -> None:
        self._users = tuple(users)

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        listed = actor in self._users
        evidence: dict[str, Any] = {
            "kind": "user_list",
            "users": list(self._users),
            "actor_listed": listed,
        }
        if not listed:
            return MembershipVerdict(
                member=False,
                reason=(
                    "you are not an approver: this approval's route is bound to "
                    "an explicit list of approvers"
                ),
                evidence=evidence,
            )
        return MembershipVerdict(member=True, evidence=evidence)


class InvalidApprovers:
    """A declared approvers block the platform cannot read: a set admitting nobody.

    Modelling this as a set rather than a special path is what keeps the
    authorizer free of one. An unreadable block is not the absence of policy, it
    is policy nothing can evaluate, so it can determine nothing and admits
    nobody. That is exactly ``undetermined``, and the authorizer's existing
    fail-closed rule then denies it without knowing this set exists.

    Failing closed here is the point: falling back to channel membership would
    widen the approver set to everyone in the card's channel, the opposite of
    what the binding was trying to say.
    """

    # Frozen; see the audit-vocabulary note above. Never was a class name, but it
    # ships in rows #420 wrote, so it is vocabulary all the same.
    audit_name = "InvalidApproversSpec"
    # This sentinel needs no channel or provider evidence to evaluate: every
    # principal receives the same undetermined, fail-closed verdict below.
    operator_eligible = True
    console_eligible = True
    adapter_eligible = True
    chat_eligible = True
    ineligible_reason = None

    def __init__(self, error: str) -> None:
        self._error = error

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        return MembershipVerdict(
            member=False,
            undetermined=True,
            reason=(
                "could not verify approvers: this approval's route declares an "
                "approvers block the platform cannot read"
            ),
            evidence={"kind": "approvers_config", "error": self._error},
        )


class UnboundRoute:
    """The approval named a route whose binding is gone: a set admitting nobody.

    A pending approval that named a route is resolvable only through that
    route's binding (ADR-0123). Once the binding is gone there is no set left to
    resolve and no fallback applies: falling through to the card channel would
    swap a server-enforced approver set for a different membership check on an
    approval that is ALREADY pending, which is exactly the escalation ADR-0123
    closes. An absent binding is therefore NOT the same fact
    as a binding present with no ``approvers`` block -- that one is still ADR-0034
    AC4's zero-setup default and still resolves through channel membership.

    Like ``InvalidApprovers``, this is modelled as a set rather than a special
    path: it reports ``undetermined`` and the authorizer's existing fail-closed
    rule denies it, so nothing in ``authorizer.py`` needs to know this set
    exists. ``undetermined`` and not ``member=False`` because the clicker is not
    outside a set that was evaluated -- the configuration stopped them, and
    telling them otherwise sends them arguing with a policy that is not there.
    """

    # NEW vocabulary added by ADR-0123, not a rename: see the audit-vocabulary
    # note above. The four strings that ship in rows already on main stay
    # byte-identical; an operator reading the append-only trail must be able to
    # tell "the route you named is gone" from "the approvers block does not
    # parse", so this refusal gets its own name rather than borrowing one.
    audit_name = "UnboundRouteBinding"
    # The missing binding itself is the answer, independent of principal kind
    # or channel evidence; let ``contains`` preserve that reason in the audit.
    operator_eligible = True
    console_eligible = True
    adapter_eligible = True
    chat_eligible = True
    ineligible_reason = None

    def __init__(self, route: str) -> None:
        self._route = route

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        # The reason names the CLASS of failure and the evidence carries the
        # route, following ``SlackUserGroupMembers._undetermined``: the reason is
        # echoed to whoever clicked, while the evidence lands on the audit row
        # where ADR-0123 requires the missing binding to be named.
        return MembershipVerdict(
            member=False,
            undetermined=True,
            reason=(
                "could not verify approvers: this approval's route is no longer "
                "bound, so the approvers it named cannot be resolved"
            ),
            evidence={
                "kind": "route_binding",
                "route": self._route,
                "binding_present": False,
            },
        )


def card_on_requesting_surface(approval: Approval, binding: Any) -> bool:
    """Whether this approval's card was shown in the conversation that asked.

    True for a routeless approval, and for a route whose resolution is
    ``{"mode": "requesting_surface"}`` (ADR-0177 decision 1). ``binding`` is the
    route binding read fresh, like every other authority fact here, so a route
    re-pointed to a fixed channel stops counting at once. The row must agree
    as well: ``card_channel`` is where the worker actually posted the card, and
    a card recorded anywhere but the asking address is not the asking
    surface's to answer, whatever the route says now. Both halves must hold, so
    neither a rewritten route map nor a stray row can move the answer.
    """

    if approval.card_channel is not None and approval.card_channel != approval.reply_channel:
        return False
    if not approval.route:
        return True
    if not isinstance(binding, Mapping):
        return False
    resolution = binding.get("resolution")
    return isinstance(resolution, Mapping) and dict(resolution) == {
        "mode": REQUESTING_SURFACE_MODE
    }


def answered_by_requester_only(approval: Approval, binding: Any) -> bool:
    """Whether this approval's card is on a non-Slack surface (ADR-0177 decision 3).

    Those cards are answered by the requester alone. A Slack card, wherever it
    was shown, keeps Slack's approver sets (decision 2).
    """

    return approval.reply_kind != SLACK_KIND and card_on_requesting_surface(approval, binding)


class RequesterOnly:
    """The person who asked is the only approver (ADR-0177 decision 3).

    The set for a card shown on a non-Slack channel, such as an email thread.
    There the only identity anyone verified is the sender the channel's adapter
    authenticated when the request came in, so that sender, carried back by the
    same adapter, is the one actor admitted. It is a confirmation step, not a
    second person's sign-off: ADR-0106 already lets an authorized requester
    confirm their own action.

    Only an ``adapter`` principal is eligible. An operator, a console session and
    a Slack click cannot prove they are the email sender who asked, so each is
    refused before membership is read, whatever subject it names. The router
    has already checked that the adapter serves the binding the card went to
    (``crud._approval_served``), so "the serving adapter's sender" is the
    conjunction of that check and this one.

    An interim until approvers are principals linked to every channel identity
    (ADR-0166, #2910); then approver lists apply on every channel.
    """

    # NEW vocabulary added by ADR-0177; see the audit-vocabulary note above.
    audit_name = "RequesterOnly"
    operator_eligible = False
    console_eligible = False
    adapter_eligible = True
    chat_eligible = False

    def __init__(self, author: str, surface_kind: str) -> None:
        self._author = author
        self._surface_kind = surface_kind
        self.ineligible_reason = (
            f"on {surface_kind}, only the person who asked may answer this approval, "
            "by replying in the conversation where it was asked"
        )

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        # Exact comparison: the adapter presents the sender exactly as it
        # presented the author at ingress, so any normalization belongs there,
        # once, and a near miss fails closed here.
        is_requester = actor == self._author
        evidence: dict[str, Any] = {
            "kind": "requester_only",
            "surface_kind": self._surface_kind,
            "actor_is_requester": is_requester,
        }
        if not is_requester:
            return MembershipVerdict(
                member=False,
                reason=(
                    f"you are not an approver: on {self._surface_kind}, only the "
                    "person who asked may answer this approval"
                ),
                evidence=evidence,
            )
        return MembershipVerdict(member=True, evidence=evidence)


class ApproverSetSelector(Protocol):
    """Pick the approver set an approval's route binding calls for.

    Performs no I/O: a set that needs a lookup does it in ``contains``. Never
    raises -- an unreadable block is an ``InvalidApprovers`` set, not an error,
    so every binding maps to a set and the authorizer has one code path.

    Implementations are provider-aware by nature: selection reads the binding
    schema, and that schema is the provider's shape. That is why they live on the
    provider's side of this port and not with the authorizer.

    ``binding`` is ``Any`` because it is a raw JSONB value read straight from the
    route map: an implementation narrows it itself and fails a non-object closed
    rather than trusting it to be a mapping.
    """

    def __call__(self, approval: Approval, binding: Any) -> ApproverSet: ...
