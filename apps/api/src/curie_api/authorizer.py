"""The approval authorizer: who may resolve a pending approval (#246, #420).

This is the seam docs/interfaces/approval/INTERFACE.md calls the black line: a
server-side decision, at resolution time, of whether a given actor is allowed
to resolve a given pending approval. It runs HERE, on the server that owns the
durable ``Approval`` record, so it cannot be spoofed from inside the agent's
sandbox -- the runner and the bundle never participate in the decision.

This module is pure policy. It knows nothing about chat providers, route
bindings, or how an approver set is chosen: it is handed an
``approvers.ApproverSet``, which answers only "is this actor in the set", and it
applies every rule that is not membership. There is ONE authorizer, and the
swappable part is the set behind it (ADR-0034).

Requester equality has no special meaning here. ADR-0106 makes the selected set
the authorization boundary, so an authenticated requester who belongs to it may
confirm their own action and an unlisted requester remains denied.

Channel-less principals have one additional eligibility rule. An ``operator``
or an ``adapter`` (ADR-0154) can be evaluated only by an explicit-user set: an
adapter vouches for a sender it authenticated at ingress, but it carries no
provider channel evidence, so a channel-membership or Slack group set cannot
judge that sender. A subject-bound ``console``
principal may also be checked by a server-side user-group lookup, but neither
kind may manufacture Slack channel evidence.

Fail closed: a set that could not determine membership (a lookup that failed, a
binding the platform cannot read) denies. That is the set reporting
``undetermined``, and it stays distinct from reporting that the actor is not a
member -- a config or infrastructure error must not be rendered to a clicker as
policy, and must never widen an approver set an operator narrowed.

Each decision carries the evidence that produced it, so the audit row records
the authority that counted rather than only the authenticated actor.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from .approvers import ApproverSet
from .models import Approval

PrincipalKind = Literal["chat", "console", "operator", "adapter"]


@dataclass(frozen=True)
class AuthzDecision:
    """The verdict, the human-readable reason rendered to the clicker, and the
    membership facts that decided it (#420), which the audit row stores so the
    trail records the authority that counted rather than only the actor."""

    allowed: bool
    reason: str = ""
    evidence: dict[str, Any] | None = None


async def authorize_approval(
    approval: Approval,
    actor: str,
    actor_channel: str | None,
    *,
    approver_set: ApproverSet,
    principal_kind: PrincipalKind,
) -> tuple[str, AuthzDecision]:
    """Decide whether ``actor`` may resolve ``approval``, and name what decided
    (for the audit row).

    ``approver_set`` is the set this approval's route binding calls for, already
    chosen by an ``approvers.ApproverSetSelector``. Every binding maps to some
    set, including one the platform could not read, so there is no second path
    through here.

    The order is the contract: a channel-less principal must first be eligible
    for the selected set, then every principal (including the requester) is
    checked for membership.
    """

    name = approver_set.audit_name

    if principal_kind in ("operator", "adapter") and not approver_set.operator_eligible:
        return name, AuthzDecision(
            allowed=False,
            reason=(
                f"{principal_kind} approval principals can resolve only routes "
                "bound to an explicit user list"
            ),
            evidence={
                "kind": "principal_set_eligibility",
                "principal_kind": principal_kind,
                "explicit_users_required": True,
            },
        )
    if principal_kind == "console" and not approver_set.console_eligible:
        return name, AuthzDecision(
            allowed=False,
            reason=(
                "console approval principals can resolve only routes bound to "
                "an explicit user list or verifiable user group"
            ),
            evidence={
                "kind": "principal_set_eligibility",
                "principal_kind": principal_kind,
                "channel_evidence_required": True,
            },
        )

    verdict = await approver_set.contains(actor, actor_channel)
    if verdict.undetermined or not verdict.member:
        # Both refuse, and the set says why: it is the only one that knows
        # whether it could not determine membership or the actor is genuinely
        # outside the set.
        return name, AuthzDecision(allowed=False, reason=verdict.reason, evidence=verdict.evidence)
    return name, AuthzDecision(allowed=True, evidence=dict(verdict.evidence or {}))
