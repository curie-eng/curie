"""The approver-set port: who counts as an approver (#420, ADR-0034).

An ``ApproverSet`` answers exactly one question -- is this actor in the set --
and gets no vote on anything else. That narrowness is the point. Self-approval
(AC2) is the authorizer's rule, enforced once in ``authorize_approval`` before
any set is consulted, so a set cannot skip it, forget it, or be safe only by
accident. The predecessor of this port had three authorizers each promising in a
docstring to re-check self-approval themselves; a promise is not a mechanism.

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

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

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


class ApproverSetSelector(Protocol):
    """Pick the approver set an approval's route binding calls for.

    Performs no I/O: a set that needs a lookup does it in ``contains``, which is
    what lets the authorizer refuse a self-approval before anything is fetched.
    Never raises -- an unreadable block is an ``InvalidApprovers`` set, not an
    error, so every binding maps to a set and the authorizer has one code path.

    Implementations are provider-aware by nature: selection reads the binding
    schema, and that schema is the provider's shape. That is why they live on the
    provider's side of this port and not with the authorizer.

    ``binding`` is ``Any`` because it is a raw JSONB value read straight from the
    route map: an implementation narrows it itself and fails a non-object closed
    rather than trusting it to be a mapping.
    """

    def __call__(self, approval: Approval, binding: Any) -> ApproverSet: ...
