"""Who may answer a canonical approval route (ADR 0271 decisions 5 and 6).

@spec REQUEST-APPROVAL-5 REQUEST-APPROVAL-6.

A canonical route is a dynamic approval (its row stores a selected set), or a
route binding whose ``separation`` enables requester exclusion; a separation
the platform cannot read counts too, so a corrupt row fails closed. Only the
HTTP answer composition (``identity/http_approval_answer.py::_Composition``)
holds the fresh canonical facts such a route needs, so this module is the one
policy owner it calls at the answer, the final commit, approved-resume
publication and the resume and grant current checks. Every other caller is
refused by ``authorizer.authorize_approval`` before any approver set is asked.

The checks are pure: the caller reads every fact (fresh identity facts, the
route binding, the configuration version) under its own reader or writer, and
this module performs no I/O. A refusal is ``SelectionRefused``: 403
``caller_not_allowed`` or 503 ``authority_unavailable`` at every boundary, with
a stable ``code`` and the audit ``evidence`` that decided it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from curie_internal import identity_approval as wire
from fastapi import HTTPException
from pydantic import ValidationError

from . import approver_tickets
from .agent_role_evaluation import role_admits
from .models import Approval
from .schemas.approvals import ApprovalSeparation
from .schemas.http_identity import ValidatedFacts

EVIDENCE_KIND = "approval_selection"

# Stable refusal codes (the audit ``code``); the public detail stays fixed.
ROUTE_MISSING = "selection_route_missing"
REGISTRATION_INVALID = "selection_registration_invalid"
REGISTRATION_CHANGED = "selection_registration_changed"
KEY_REVOKED = "selection_key_revoked"
SEPARATION_INVALID = "separation_policy_invalid"
SNAPSHOT_INVALID = "selection_snapshot_invalid"
ACTOR_NOT_CANONICAL = "selection_actor_not_canonical"
ACTOR_NOT_SELECTED = "selection_actor_not_selected"
ACTOR_OUTSIDE_CEILING = "selection_actor_outside_ceiling"
ACTOR_CHANGED = "selection_actor_changed"
REQUESTER_UNKNOWN = "separation_requester_unknown"
ACTOR_UNKNOWN = "separation_actor_unknown"
REQUESTER_EXCLUDED = "separation_requester_excluded"
EXEMPTION_UNAVAILABLE = "separation_exemption_unavailable"

_MAX_SELECTED = 16
_DETAILS: dict[int, str] = {403: "caller_not_allowed", 503: "authority_unavailable"}

# One canonical human: (tenant_id, issuer, subject).
Person = tuple[str, str, str]


class SelectionRefused(HTTPException):
    """A canonical-route refusal with its stable code and audit evidence.

    An ``HTTPException`` so every existing boundary mapping keeps working: 403
    reads ``caller_not_allowed`` and 503 ``authority_unavailable`` at the answer,
    resume and grant boundaries alike. @spec REQUEST-APPROVAL-5/6.
    """

    def __init__(self, status: Literal[403, 503], code: str, evidence: dict[str, Any]) -> None:
        super().__init__(status, _DETAILS[status], headers={"Cache-Control": "no-store"})
        self.code = code
        self.evidence: dict[str, Any] = {**evidence, "code": code}


@dataclass(frozen=True)
class RoutePolicy:
    """The current policy of one canonical route, read strictly.

    ``registration`` is the route's current selector registration (dynamic
    rows only); ``exempt_group`` is ``(issuer, id)``. @spec REQUEST-APPROVAL-5/6.
    """

    route: str | None
    dynamic: bool
    exclude_requester: bool
    exempt_group: tuple[str, str] | None
    registration: approver_tickets.StoredRegistration | None


def _separation(value: Any) -> ApprovalSeparation | None:
    """A stored separation parsed strictly, or None when it cannot be read.

    @spec REQUEST-APPROVAL-6.
    """
    try:
        return ApprovalSeparation.model_validate(value)
    except (ValidationError, ValueError, TypeError, RecursionError):
        return None


def _enforces_separation(binding: Mapping[Any, Any]) -> bool:
    """A ``separation`` key that does not read as an explicit opt-out.

    Only ``exclude_requester: false`` (which the schema refuses beside an
    exemption) disables it; anything unreadable enforces. @spec REQUEST-APPROVAL-6.
    """
    if "separation" not in binding:
        return False
    parsed = _separation(binding["separation"])
    return parsed is None or parsed.exclude_requester


def canonical_route(row: Approval, binding: Any) -> bool:
    """Whether only the HTTP answer composition may judge an answer to ``row``.

    True for a dynamic row, whatever its binding; true for a binding object
    carrying a ``separation`` key unless it parses as the explicit opt-out.
    Never raises. @spec REQUEST-APPROVAL-5/6.
    """
    if row.selection_mode == "dynamic":
        return True
    return isinstance(binding, Mapping) and _enforces_separation(binding)


def routes_require_http(routes: Any) -> bool:
    """Whether an agent's raw route map needs HTTP identity for every binding.

    A selector registration (``approver_ticket`` key, even malformed), an
    enforced or unreadable ``separation``, or a route value or map that is not
    an object all require it; an absent map does not. ADR 0259 decision 1 as
    amended by ADR 0271 decision 5. @spec REQUEST-APPROVAL-5/6.
    """
    if routes is None:
        return False
    if not isinstance(routes, Mapping):
        return True
    for binding in routes.values():
        if not isinstance(binding, Mapping):
            return True
        if "approver_ticket" in binding or _enforces_separation(binding):
            return True
    return False


def _refuse(
    code: str, evidence: dict[str, Any], status: Literal[403, 503] = 403,
) -> SelectionRefused:
    """@spec REQUEST-APPROVAL-5/6."""
    return SelectionRefused(status, code, evidence)


def route_policy(row: Approval, binding: Any, *, evidence: dict[str, Any]) -> RoutePolicy:
    """The route's current registration and separation, or a refusal.

    A dynamic row needs its route still bound, a readable registration for the
    same connector and tool, and the same key id it was created under; its
    stored effective separation applies, and an absent one excludes (omission
    never disables). A static route's separation must parse strictly.
    ``evidence`` gains the exclusion and exemption group. @spec REQUEST-APPROVAL-5/6.
    """
    route = row.route or None
    dynamic = row.selection_mode == "dynamic"
    registration: approver_tickets.StoredRegistration | None = None
    if dynamic:
        if binding is None or route is None:
            raise _refuse(ROUTE_MISSING, evidence)
        try:
            found = approver_tickets.stored_registrations({route: binding})
        except approver_tickets.ApproverTicketRefusal:
            raise _refuse(REGISTRATION_INVALID, evidence) from None
        if len(found) != 1:
            raise _refuse(REGISTRATION_INVALID, evidence)
        registration = next(iter(found.values()))
        if registration.pair != (row.selection_connector, row.selection_tool):
            raise _refuse(REGISTRATION_CHANGED, evidence)
        if registration.ticket.key_id != row.selection_key_id:
            raise _refuse(KEY_REVOKED, evidence)
    if not isinstance(binding, Mapping):
        raise _refuse(SEPARATION_INVALID, evidence)
    exclude, exempt = True, None
    if "separation" in binding:
        parsed = _separation(binding["separation"])
        if parsed is None:
            raise _refuse(SEPARATION_INVALID, evidence)
        exclude = parsed.exclude_requester
        group = parsed.exempt_group
        exempt = None if group is None else (group.issuer, group.id)
    elif not dynamic:
        raise _refuse(SEPARATION_INVALID, evidence)
    evidence["exclusion"] = exclude
    evidence["exempt_group"] = None if exempt is None else {"issuer": exempt[0], "id": exempt[1]}
    return RoutePolicy(route, dynamic, exclude, exempt, registration)


def _person(value: Any) -> Person | None:
    """One stored canonical person, read strictly. @spec REQUEST-APPROVAL-5."""
    if type(value) is not dict:
        return None
    fields = (value.get("tenant_id"), value.get("issuer"), value.get("subject"))
    if not all(type(item) is str and item for item in fields):
        return None
    tenant, issuer, subject = fields
    assert isinstance(tenant, str) and isinstance(issuer, str) and isinstance(subject, str)
    return tenant, issuer, subject


def _shown(person: Person | None) -> dict[str, str] | None:
    """@spec REQUEST-APPROVAL-5/6: a person as audit evidence."""
    if person is None:
        return None
    return {"tenant_id": person[0], "issuer": person[1], "subject": person[2]}


def _selected_set(value: Any) -> frozenset[Person] | None:
    """The stored selected approvers: a list of 1 to 16 canonical persons, else None.

    @spec REQUEST-APPROVAL-5.
    """
    if type(value) is not list or not 1 <= len(value) <= _MAX_SELECTED:
        return None
    people = [_person(item) for item in value]
    if any(item is None for item in people):
        return None
    return frozenset(item for item in people if item is not None)


def canonical_actor(
    facts: ValidatedFacts, *, tenant_id: str, principal_applied: bool,
) -> Person | None:
    """The fresh canonical person behind an actor: a resolved, active human, applied.

    Anything else (unresolved, inactive, no applied principal) has none.
    @spec REQUEST-APPROVAL-5/6.
    """
    identity = facts.identity
    if (
        facts.outcome != "resolved" or identity is None or not principal_applied
        or identity.kind != "human" or identity.account_status != "active"
    ):
        return None
    return tenant_id, identity.issuer, identity.subject


def check_selected_approver(
    row: Approval, policy: RoutePolicy, *, actor: Person | None, facts: ValidatedFacts,
    evidence: dict[str, Any],
) -> Person:
    """The actor is a stored selected person, still inside the current ceiling.

    The stored selection is parsed strictly at every answer. ``actor`` is the
    caller's ``canonical_actor`` from the same fresh ``facts``: it must be a
    resolved, active, applied human whose canonical identity is in that set;
    the current registration's ``ceiling.users``, when set, must name it, and
    its pinned role must match the actor's fresh facts. A raw surface-account
    entry can match that role only for a resolved actor, and never replaces the
    selected-set check. Returns the actor. @spec REQUEST-APPROVAL-5.
    """
    registration = policy.registration
    if not policy.dynamic or registration is None:
        raise _refuse(REGISTRATION_INVALID, evidence)
    selected = _selected_set(row.selected_approvers)
    if selected is None:
        raise _refuse(SNAPSHOT_INVALID, evidence)
    evidence["selected_count"] = len(selected)
    if actor is None:
        raise _refuse(ACTOR_NOT_CANONICAL, evidence)
    if actor not in selected:
        raise _refuse(ACTOR_NOT_SELECTED, evidence)
    users = registration.ticket.ceiling.users
    if users is not None and (actor[1], actor[2]) not in {
        (user.issuer, user.subject) for user in users
    }:
        raise _refuse(ACTOR_OUTSIDE_CEILING, evidence)
    if not role_admits(registration.pinned_role, facts):
        raise _refuse(ACTOR_OUTSIDE_CEILING, evidence)
    return actor


def check_separation(
    policy: RoutePolicy, *, requester: Person | None, actor: Person | None,
    facts: ValidatedFacts, decision: str, evidence: dict[str, Any],
) -> None:
    """Requester exclusion by canonical equality, with an optional current exemption.

    ``actor`` is the caller's ``canonical_actor`` from the same fresh ``facts``,
    which also carry its group membership. Equality compares tenant, issuer and
    subject, so two channel accounts of one person are equal. Exclusion off, a
    rejection, or distinct people permit (the equality is recorded). Equal
    people under exclusion are permitted only while the actor, who is then the
    requester, belongs to the exempt group by its fresh membership; membership
    the observation does not carry is unavailable, never exempt.
    @spec REQUEST-APPROVAL-6.
    """
    evidence["requester"] = _shown(requester)
    if requester is None:
        raise _refuse(REQUESTER_UNKNOWN, evidence)
    if actor is None:
        raise _refuse(ACTOR_UNKNOWN, evidence)
    equal = actor == requester
    evidence["equality"] = equal
    evidence["exemption_used"] = False
    if not policy.exclude_requester or decision == "rejected" or not equal:
        return
    if policy.exempt_group is not None:
        groups = facts.groups
        if groups is None:
            raise _refuse(EXEMPTION_UNAVAILABLE, evidence, 503)
        if policy.exempt_group in {(group.issuer, group.id) for group in groups}:
            evidence["exemption_used"] = True
            return
    raise _refuse(REQUESTER_EXCLUDED, evidence)


def _requester(row: Approval, saved: wire.ApprovalContextV1) -> Person | None:
    """The original requester: the stored one of a dynamic row, else the sealed invoker.

    A static route's requester is the sealed original invocation's canonical
    person, and only when a human principal was applied to it.
    @spec REQUEST-APPROVAL-6.
    """
    if row.selection_mode == "dynamic":
        return _person(row.selection_requester)
    canonical = saved.canonical
    if canonical is None or saved.principal is None:
        return None
    return saved.scope.tenant_id, canonical.issuer, canonical.subject


def check_answer(
    row: Approval, binding: Any, *, saved: wire.ApprovalContextV1, facts: ValidatedFacts,
    principal_applied: bool, decision: str, configuration_version: int | None,
    preserved: bool = False,
) -> dict[str, Any] | None:
    """The one canonical-route check of an approver subject; its audit evidence.

    None for a route that is not canonical. Otherwise, in order: the route's
    current policy; for a preserved answering actor (resume, grant and the
    approved-resume publication) the fresh canonical person must equal the one
    sealed with the answer; for a dynamic row the selected-set and ceiling
    check; then separation. Raises ``SelectionRefused`` carrying the evidence
    gathered so far. @spec REQUEST-APPROVAL-5 REQUEST-APPROVAL-6.
    """
    if not canonical_route(row, binding):
        return None
    tenant_id = saved.scope.tenant_id
    evidence: dict[str, Any] = {
        "kind": EVIDENCE_KIND,
        "mode": row.selection_mode,
        "route": row.route,
        "key_id": row.selection_key_id,
        "connector": row.selection_connector,
        "tool": row.selection_tool,
        "selected_count": None,
        "actor": None,
        "requester": None,
        "equality": None,
        "exclusion": None,
        "exempt_group": None,
        "exemption_used": False,
        "decision": decision,
        "configuration_version": configuration_version,
    }
    policy = route_policy(row, binding, evidence=evidence)
    actor = canonical_actor(facts, tenant_id=tenant_id, principal_applied=principal_applied)
    evidence["actor"] = _shown(actor)
    if preserved:
        sealed = saved.actor_canonical
        if actor is None or sealed is None or (actor[1], actor[2]) != (
            sealed.issuer, sealed.subject,
        ):
            raise _refuse(ACTOR_CHANGED, evidence)
    if policy.dynamic:
        check_selected_approver(row, policy, actor=actor, facts=facts, evidence=evidence)
    check_separation(
        policy, requester=_requester(row, saved), actor=actor, facts=facts,
        decision=decision, evidence=evidence,
    )
    return evidence
