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

from curie_api.schemas.channels import EMAIL_KIND, SLACK_CHANNEL_ID

from .models import Approval

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
    def test_driver_eligible(self) -> bool: ...

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
    test_driver_eligible = True
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
    test_driver_eligible = False
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
    test_driver_eligible = False
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

    The record decides, not the current route: the card stays where it was
    shown when an operator re-points the route later (ADR-0177: an approval is
    answered where its card is shown). ``card_channel`` is where the worker
    posted the card, so a card recorded anywhere but the asking address is not
    the asking conversation's. A routeless approval's card always went there.

    A routed one needs its binding still readable (ADR-0123: a named route
    with no binding left admits nobody, through ``UnboundRoute``). And the
    record keeps the card's address but not its kind, so a non-Slack asking
    address shaped like a Slack channel ID cannot be told apart from a fixed
    Slack card at that same ID; that case is read as the Slack card, which no
    adapter may answer. Every other routed record whose card sits at the asking
    address was shown there: fixed targets are Slack channel IDs, so a
    non-Slack asking address can only hold a requesting_surface card.
    """

    if approval.card_channel is not None and approval.card_channel != approval.reply_channel:
        return False
    if not approval.route:
        return True
    if not isinstance(binding, Mapping) or not isinstance(binding.get("resolution"), Mapping):
        return False
    if approval.reply_kind != SLACK_KIND and SLACK_CHANNEL_ID.fullmatch(approval.reply_channel):
        return False
    return True


def shown_off_slack(approval: Approval, binding: Any) -> bool:
    """Whether this approval's card is in a non-Slack conversation (ADR-0177 and its amendment).

    None of Slack's approver sets can be proven there. An email card is
    answered only from the route's approver email list (``EmailApprovers``);
    any other non-Slack card has no list it can verify (``NoVerifiableApprovers``).
    A Slack card, wherever it was shown, keeps Slack's approver sets.
    """

    return approval.reply_kind != SLACK_KIND and card_on_requesting_surface(approval, binding)


class EmailApprovers:
    """A literal list of approver email addresses (ADR-0177 amendment).

    The set for a card shown in an email thread whose route lists ``emails``.
    A member is a sender the serving mail adapter verified at its inbound gate
    and carried back as the actor, whose address is on the list. The match is
    exact apart from case: both sides are compared lowercase, the form the
    schema stores and the mail adapter reports, so a near miss fails closed.

    Only an ``adapter`` principal is eligible. An operator, a console session and
    a Slack click cannot prove they hold an email address, so each is refused
    before membership is read, whatever address it names. The router has already
    checked that the adapter serves the thread's binding
    (``crud.approvals._approval_served``) and that the binding's ``allowed_callers``
    admit the sender (ADR 0175), so "a verified, listed sender of the serving
    adapter" is the conjunction of those checks and this one.

    Pure config, like ``ExplicitUsers``: it never looks anything up. An empty
    list admits nobody and says so as ``undetermined``: the schema refuses one,
    so an empty list here is a row written around it, and that is a
    configuration the platform cannot evaluate, not a verdict on the sender.
    """

    # NEW vocabulary added by the ADR-0177 amendment; see the audit-vocabulary note above.
    audit_name = "EmailApproverList"
    operator_eligible = False
    console_eligible = False
    adapter_eligible = True
    test_driver_eligible = False
    chat_eligible = False
    ineligible_reason = (
        "on email, only an address on this approval's approver list may answer, "
        "by replying in the thread where it was asked"
    )

    def __init__(self, emails: Sequence[str]) -> None:
        self._emails = tuple(email.lower() for email in emails)

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        if not self._emails:
            return MembershipVerdict(
                member=False,
                undetermined=True,
                reason=(
                    "could not verify approvers: this approval's route lists no "
                    "approver email addresses"
                ),
                evidence={"kind": "email_list", "emails": [], "actor_listed": False},
            )
        listed = actor.lower() in self._emails
        evidence: dict[str, Any] = {
            "kind": "email_list",
            "emails": list(self._emails),
            "actor_listed": listed,
        }
        if not listed:
            return MembershipVerdict(
                member=False,
                reason=(
                    "you are not an approver: this approval's route is bound to "
                    "an explicit list of approver email addresses"
                ),
                evidence=evidence,
            )
        return MembershipVerdict(member=True, evidence=evidence)


class NoVerifiableApprovers:
    """A non-Slack card with no approver list that channel can verify: admits nobody.

    ADR-0177 amendment A3 retires the requester-only default: an email card is
    answered only from the route's ``emails``, and no other non-Slack channel
    has a list yet. A routeless approval, a route declaring no ``emails``, a
    route declaring only Slack approvers, and any non-email channel all land
    here. The worker escalates these when they are raised, so this set meets
    only an approval pending from before that rule, or one whose route was
    rewritten while it pended.

    Like ``UnboundRoute``, the configuration is the answer, not the principal,
    so every kind is eligible and reads the same ``undetermined`` refusal, which
    the authorizer denies without knowing this set exists.
    """

    # NEW vocabulary added by the ADR-0177 amendment; see the audit-vocabulary note above.
    audit_name = "NoVerifiableApprovers"
    operator_eligible = True
    console_eligible = True
    adapter_eligible = True
    test_driver_eligible = False
    chat_eligible = True
    ineligible_reason = None

    def __init__(self, surface_kind: str, *, slack_declared: bool) -> None:
        self._surface_kind = surface_kind
        self._slack_declared = slack_declared

    async def contains(self, actor: str, actor_channel: str | None) -> MembershipVerdict:
        if self._surface_kind == EMAIL_KIND:
            reason = (
                "could not verify approvers: on email, only an address on the "
                "approval's approver list may answer, and its route lists none"
            )
        else:
            reason = (
                f"could not verify approvers: {self._surface_kind} has no approver "
                "list that can be verified"
            )
        return MembershipVerdict(
            member=False,
            undetermined=True,
            reason=reason,
            evidence={
                "kind": "no_verifiable_approvers",
                "surface_kind": self._surface_kind,
                "slack_approvers_declared": self._slack_declared,
            },
        )


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
