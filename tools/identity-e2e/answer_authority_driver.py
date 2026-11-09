"""ADR 0271 answer and resume authority through the real HTTP answer path.

One file in two halves, as ``approver_ticket_driver.py`` is, so what the host
judges and what the pod sends cannot drift apart:

1. On the host, standard library only: the five people a run needs and the
   roster overrides that change them, the approver role filters, the route
   map the scenario writes, the email answer body and proof fields, the resume
   check bodies, and the judges that read an answer's HTTP status, the row and
   its audit rows. The ``approver-ticket-answer`` scenario in
   ``answer_authority_scenario.py`` uses them.
2. Inside the release's API pod, ``Target.pod_python`` runs
   :data:`POD_BOOTSTRAP`, which executes ``approver_ticket_driver.py`` and then
   this file (both sent on stdin) and prints :func:`dispatch`'s one JSON line.
   It creates approvals with the creation driver's ``_create``, answers them as
   the mail adapter does (an adapter principal minted with the API's own key,
   the sender as the actor, and an email answer proof signed with the receiver
   key the harness registered) or with an operator principal, and runs the
   resume current check and the grant check from the wake the API published,
   all over loopback HTTP with the API's own credentials. It also reads the
   worker's done markers, so the host can wait until the worker is quiescent.

Every product import is inside a function. Nothing either half returns carries
a key, token, proof, capsule, nonce or ticket: results are statuses, refusal
codes, ids, audit rows (scrubbed) and digests. The receiver key and the
selector seed travel to the pod on stdin only.
"""

from __future__ import annotations

import copy
import hashlib
import json
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

import approver_ticket_driver as tickets

SCENARIO = "approver-ticket-answer"
# ADR 0271 required evidence item 3 (decision 5), in run order.
ITEM_3_STEPS = (
    "answer-selected-approver-allowed",
    "answer-unselected-refused",
    "selection-fixed-after-source-change",
    "ceiling-member-revoked",
    "key-revoked",
    "registration-removed",
    "canonical-inactive",
    "migrated-role-no-legacy-block",
    "raw-account-not-selected",
    "ceiling-widened",
)
# ADR 0271 required evidence item 4 (decision 6), in run order.
ITEM_4_STEPS = (
    "separation-default-on-omission",
    "separation-requester-excluded-alias",
    "separation-requester-rejection",
    "separation-exemption-member",
    "separation-exemption-removed",
    "separation-exemption-unavailable",
    "separation-exempt-unselected",
    "separation-operator-opt-out",
    "separation-agent-owned-opt-out-refused",
    "static-exclusion-route",
    "operator-principal-refused",
)
STEP_IDS = ITEM_3_STEPS + ITEM_4_STEPS

# The people a run needs, by the part each plays.
REQUESTER = "requester"
APPROVER = "approver"
SECOND = "second"
OUTSIDER = "outsider"
EXEMPT = "exempt"
ROLES = (REQUESTER, APPROVER, SECOND, OUTSIDER, EXEMPT)
# The approver role the run installs: everyone but the outsider.
BASE_ROLE = (REQUESTER, APPROVER, SECOND, EXEMPT)

DYNAMIC = "dynamic"
STATIC = "static"
# Every approval the run creates, by the step that creates it: (name, kind,
# the people a dynamic approval's ticket selects). Each takes one requester
# turn, because an approval's dedupe key is its turn's event id.
APPROVALS: dict[str, tuple[tuple[str, str, tuple[str, ...]], ...]] = {
    "answer-selected-approver-allowed": (("x1", DYNAMIC, (APPROVER,)),),
    "answer-unselected-refused": (("x2", DYNAMIC, (APPROVER,)),),
    # Reuses x2, created before the source change.
    "selection-fixed-after-source-change": (),
    "ceiling-member-revoked": (("x3", DYNAMIC, (APPROVER,)),),
    "key-revoked": (("x4", DYNAMIC, (APPROVER,)),),
    "registration-removed": (("x5", DYNAMIC, (APPROVER,)),),
    "canonical-inactive": (("x6", DYNAMIC, (APPROVER,)),),
    "migrated-role-no-legacy-block": (("x7", DYNAMIC, (APPROVER,)),),
    "raw-account-not-selected": (("x8", DYNAMIC, (APPROVER,)),),
    "ceiling-widened": (("x9", DYNAMIC, (APPROVER,)), ("x10", DYNAMIC, (APPROVER,))),
    "separation-default-on-omission": (),
    "separation-requester-excluded-alias": (("y1", DYNAMIC, (REQUESTER, APPROVER)),),
    "separation-requester-rejection": (("y2", DYNAMIC, (REQUESTER, APPROVER)),),
    "separation-exemption-member": (("y3", DYNAMIC, (REQUESTER, APPROVER)),),
    "separation-exemption-removed": (("y4", DYNAMIC, (REQUESTER, APPROVER)),),
    "separation-exemption-unavailable": (("y5", DYNAMIC, (REQUESTER, APPROVER)),),
    "separation-exempt-unselected": (("y6", DYNAMIC, (REQUESTER, APPROVER)),),
    "separation-operator-opt-out": (("y7", DYNAMIC, (REQUESTER, APPROVER)),),
    "separation-agent-owned-opt-out-refused": (),
    "static-exclusion-route": (("z1", STATIC, ()),),
    "operator-principal-refused": (("w1", DYNAMIC, (APPROVER,)), ("z2", STATIC, ())),
}
TURN_COUNT = sum(len(made) for made in APPROVALS.values())

# A third route, without a selector, that enables requester exclusion.
STATIC_ROUTE = "ticket-static-exclusion"
OWNED_ROUTES = (*tickets.OWNED_ROUTES, STATIC_ROUTE)
EXEMPT_GROUP = "identity-e2e-approval-exempt"
CHANGED_GROUP = "identity-e2e-answer-source-changed"
# A canonical subject no roster entry has, so a role rewrite admits nobody new.
UNROSTERED_SUBJECT = "identity-e2e-unrostered"
# A synthetic Slack user id: the operator principal's subject and the static
# route's explicit user list.
OPERATOR_USER = "U0EXAMPLEOP"
APPROVAL_TTL_S = 900
ADAPTER_TOKEN_TTL_S = 300
NOTE = "identity-e2e approver-ticket-answer"
# A concurrent identity lifecycle change (the worker's own turn checks) can
# make one answer or check answer 503; it is retried this many times in all.
ATTEMPTS = 3

# Every lookup the API applies moves the tenant's lifecycle generation, and an
# approved answer's inline resume publication refuses when it moved between
# its capture and its writer, leaving the wake to the resume reconciler's 900 s
# grace. So the agent's worker must be quiescent before an answer: the run
# waits until the worker has finished every turn it sent, and after each answer
# that publishes a wake, until it has finished the resumed turn. Finished means
# the worker's done marker for the event id exists, which a refused turn also
# writes (it sends no ``turn.completed``).
RESOLVED_STATUSES = ("approved", "rejected")
SETTLE_POLL_S = 1.0
# One in-pod wait, inside ``Target.pod_python``'s 120 s timeout.
SETTLE_CALL_S = 90.0
# The run's 21 turns: 20 share one conversation, which the worker serves in order.
TURNS_SETTLE_S = 900.0
# One resumed turn, or one turn that reapplies a person's principal.
WAKE_SETTLE_S = 240.0

# Response details, routes and headers of the answer and resume boundaries.
CALLER_NOT_ALLOWED = "caller_not_allowed"
AUTHORITY_UNAVAILABLE = "authority_unavailable"
RESOLVE_PATH = "/approvals/{approval_id}/resolve"
OPERATOR_MINT_PATH = "/approvals/principals/operator"
TURN_CHECK_PATH = "/v1/internal/identity/turn-authority/check"
GRANT_CHECK_PATH = "/v1/internal/identity/approval-grant/check"
ADAPTER_PRINCIPAL_HEADER = "X-Curie-Adapter-Principal"
APPROVAL_ACTOR_HEADER = "X-Curie-Approval-Actor"
APPROVAL_PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
ANSWER_PROOF_HEADER = "X-Curie-Approval-Receiver-Proof"
WORKER_TOKEN_HEADER = "X-Curie-Worker-Token"
RESUME_STAGE = "claim_or_resume"
GRANT_STAGE = "prepare"
WAKE_PAGE = 500
WAKE_SCAN = 10_000

# Refusal codes and audit names, as the 0271b plan names them.
NOT_SELECTED = "selection_actor_not_selected"
OUTSIDE_CEILING = "selection_actor_outside_ceiling"
KEY_REVOKED = "selection_key_revoked"
REQUESTER_EXCLUDED = "separation_requester_excluded"
OPERATOR_REFUSED = "canonical_route_operator_refused"
SELECTION_KIND = "approval_selection"
SEPARATION_CHANGED = "approval_separation.changed"
# The members of that event's details the scenario judges and records.
SEPARATION_CHANGE_FIELDS = (
    "route",
    "old_exclude_requester",
    "new_exclude_requester",
    "old_exempt_group",
    "new_exempt_group",
    "old_configuration_version",
    "new_configuration_version",
)
RESOLVED = "resolved"
DENIED = "denied"
RESUME_REFUSED = "resume_refused"

# Audit and response members never copied into evidence.
_SECRET_NAMES = frozenset(
    {
        "token",
        "proof",
        "capsule",
        "context",
        "identity_context",
        "nonce",
        "selection_nonce",
        "ticket",
        "approval_ticket",
        "signature",
        "public_key",
        "seed",
        "seed_hex",
        "key",
        "granted_arguments",
        "payload",
    }
)
_TEXT_LIMIT = 300


class KEEP:
    """Sentinel: leave this part of the scenario's route state as it is."""


# --- people and the roster ------------------------------------------------------------------


def active_humans(roster: Mapping[str, Any]) -> list[str]:
    """Every active, resolved human address in a harness roster, sorted."""

    return sorted(
        str(address).lower()
        for address, entry in (roster.get("senders") or {}).items()
        if entry.get("sender_kind", "human") == "human"
        and entry.get("account_status", "active") == "active"
        and entry.get("outcome") in (None, "resolved")
    )


def choose_people(roster: Mapping[str, Any], requester: str, approver: str) -> dict[str, str]:
    """The run's five people by part; a roster without them raises ``ValueError``.

    The requester and the selected approver are named by the caller; the
    second approver, the outsider and the exempt person are the remaining
    active humans in address order.
    """

    active = active_humans(roster)
    requester, approver = requester.lower(), approver.lower()
    if requester == approver:
        raise ValueError("the requester (--sender) and approver (--second-sender) must differ")
    missing = [who for who in (requester, approver) if who not in active]
    if missing:
        raise ValueError(f"{missing} are not active verified humans in the roster `up` installed")
    others = [address for address in active if address not in (requester, approver)]
    if len(others) < 3:
        raise ValueError(
            "approver-ticket-answer needs five active verified humans in the roster (requester, "
            "selected approver, second approver, outsider, exempt person); the roster `up` "
            f"installed has {len(active)}: {active}. Rerun `up` with the default roster or a "
            "roster file that adds them."
        )
    return {
        REQUESTER: requester,
        APPROVER: approver,
        SECOND: others[0],
        OUTSIDER: others[1],
        EXEMPT: others[2],
    }


def entry_of(roster: Mapping[str, Any], address: str) -> dict[str, Any]:
    entry = (roster.get("senders") or {}).get(address.lower())
    if not isinstance(entry, Mapping):
        raise ValueError(f"{address} is not in the roster")
    return copy.deepcopy(dict(entry))


def alias_address(roster: Mapping[str, Any], address: str) -> str:
    """A second address for one person, unused in the roster."""

    local, _, domain = address.lower().partition("@")
    senders = roster.get("senders") or {}
    candidate, index = f"{local}-alias@{domain}", 1
    while candidate in senders:
        index += 1
        candidate = f"{local}-alias{index}@{domain}"
    return candidate


def with_groups(
    entry: Mapping[str, Any], *, add: Iterable[str] = (), remove: Iterable[str] = ()
) -> dict[str, Any]:
    changed = copy.deepcopy(dict(entry))
    gone = set(remove)
    groups = [group for group in changed.get("groups") or [] if group not in gone]
    for group in add:
        if group not in groups:
            groups.append(group)
    changed["groups"] = groups
    return changed


def deactivated(entry: Mapping[str, Any]) -> dict[str, Any]:
    changed = copy.deepcopy(dict(entry))
    changed.pop("outcome", None)
    changed["account_status"] = "disabled"
    return changed


def unavailable(entry: Mapping[str, Any]) -> dict[str, Any]:
    """A lookup the directory cannot answer (the recorded unavailable response)."""

    return {"subject": str(entry["subject"]), "sender_kind": "unknown", "outcome": "unavailable"}


def active_human(rows: Iterable[Mapping[str, Any]] | None) -> bool:
    """Whether a person's ``principals`` rows include an active human, as creation requires."""

    return any(row.get("type") == "human" and row.get("status") == "active" for row in rows or ())


# --- the approver role and the route map -----------------------------------------------------


def email_namespace(registration: Mapping[str, Any], address: str) -> dict[str, Any]:
    """The email receiver registration's domain descriptor an address falls in."""

    domain = address.lower().rpartition("@")[2]
    for item in registration.get("domains") or ():
        if item.get("kind") == "email_domain" and item.get("key") == domain:
            return {key: item[key] for key in ("id", "authority", "kind", "key")}
    raise ValueError(f"the receiver registration lists no domain for {address}")


def surface_account(namespace: Mapping[str, Any], address: str) -> dict[str, Any]:
    """An ADR 0246 raw surface-account role entry for one email address."""

    return {
        "kind": "surface_account",
        "provider": "email",
        "namespace": dict(namespace),
        "native_id": address.lower(),
    }


def role_filter(
    issuer: str, subjects: Iterable[str], *, accounts: Sequence[Mapping[str, Any]] = ()
) -> dict[str, Any]:
    """A restricted approver role: these canonical people, then these raw accounts."""

    entries: list[dict[str, Any]] = [
        {"kind": "issuer_subject", "issuer": issuer, "subject": subject}
        for subject in sorted(set(subjects))
    ]
    entries += [dict(account) for account in accounts]
    return {"entries": entries}


def base_routes(stored: Any) -> dict[str, Any]:
    """An agent's stored routes without the three this scenario owns."""

    if not isinstance(stored, Mapping):
        return {}
    return {
        str(name): copy.deepcopy(binding)
        for name, binding in stored.items()
        if name not in OWNED_ROUTES
    }


def ticket_registration(
    *,
    key_id: str,
    public_key: str,
    role_version: int,
    issuer: str,
    users: Sequence[str] | None = None,
) -> dict[str, Any]:
    """A route's ``approver_ticket`` write, pinning the current approver role version."""

    registration = tickets.registration(
        key_id=key_id, public_key=public_key, role_version=role_version
    )
    if users is not None:
        registration["ceiling"]["users"] = [
            {"issuer": issuer, "subject": subject} for subject in users
        ]
    return registration


def separation(
    exclude: bool, *, exempt_group: str | None = None, issuer: str | None = None
) -> dict[str, Any]:
    value: dict[str, Any] = {"exclude_requester": exclude}
    if exempt_group is not None:
        value["exempt_group"] = {"issuer": issuer, "id": exempt_group}
    return value


def static_binding(*, emails: Sequence[str], operators: Sequence[str]) -> dict[str, Any]:
    """A requesting-surface route without a selector that excludes the requester."""

    return {
        "resolution": {"mode": "requesting_surface"},
        "approvers": {"users": list(operators), "emails": [e.lower() for e in emails]},
        "separation": {"exclude_requester": True},
    }


def owned_routes(
    base: Mapping[str, Any],
    *,
    registration: Mapping[str, Any] | None = None,
    separation: Mapping[str, Any] | None = None,
    static: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """``base`` plus the owned routes; separation rides the registered route.

    Separation is written only next to a registration, and only when given:
    an omitted one is how the default-on-omission step writes it.
    """

    routes = base_routes(base)
    for name in tickets.OWNED_ROUTES:
        routes[name] = copy.deepcopy(tickets.REQUESTING_SURFACE)
    if registration is not None:
        routes[tickets.TICKET_ROUTE]["approver_ticket"] = copy.deepcopy(dict(registration))
        if separation is not None:
            routes[tickets.TICKET_ROUTE]["separation"] = copy.deepcopy(dict(separation))
    if static is not None:
        routes[STATIC_ROUTE] = copy.deepcopy(dict(static))
    return routes


def has_separation(routes: Any) -> bool:
    return isinstance(routes, Mapping) and any(
        isinstance(binding, Mapping) and binding.get("separation") is not None
        for binding in routes.values()
    )


def needs_version(stored: Any, proposed: Any) -> bool:
    """Whether a route write must name the configuration version (plan, route writes)."""

    return any(
        tickets.has_registration(routes) or has_separation(routes) for routes in (stored, proposed)
    )


def owned_summary(routes: Any) -> dict[str, Any]:
    """What the scenario's route state means, comparable across write and readback."""

    routes = routes if isinstance(routes, Mapping) else {}
    binding = routes.get(tickets.TICKET_ROUTE)
    binding = binding if isinstance(binding, Mapping) else {}
    ticket = binding.get("approver_ticket")
    ceiling = ticket.get("ceiling") if isinstance(ticket, Mapping) else None
    ceiling = ceiling if isinstance(ceiling, Mapping) else {}
    stored_separation = binding.get("separation")
    if isinstance(ticket, Mapping) and stored_separation is None:
        stored_separation = {"exclude_requester": True}
    return {
        "bound": sorted(name for name in OWNED_ROUTES if name in routes),
        "registration": None
        if not isinstance(ticket, Mapping)
        else {
            "key_id": ticket.get("key_id"),
            "public_key": ticket.get("public_key"),
            "users": ceiling.get("users"),
            "approver_role_version": ceiling.get("approver_role_version"),
        },
        "separation": stored_separation,
        "static": routes.get(STATIC_ROUTE),
        "base": base_routes(routes),
    }


# --- the email answer, judged by the product's wire model in the unit tests -----------------


def answer_body(decision: str, note: str | None = NOTE) -> bytes:
    """The exact ``ApprovalResolve`` bytes the mail adapter forwards and the proof covers."""

    return json.dumps({"decision": decision, "note": note}, separators=(",", ":")).encode()


def answer_proof_fields(
    *,
    receiver_id: str,
    receiver_generation: int,
    binding_id: str,
    approval_id: str,
    sender: str,
    body: bytes,
) -> dict[str, Any]:
    """An ``EmailApprovalAnswerProofV1`` over exactly ``body`` for one sender and approval."""

    return {
        "schema_version": 1,
        "purpose": "approval_answer",
        "operation": "resolve",
        "receiver_id": str(receiver_id),
        "receiver_generation": int(receiver_generation),
        "binding_id": str(binding_id),
        "approval_id": str(approval_id),
        "sender": sender.lower(),
        "body_sha256": hashlib.sha256(body).hexdigest(),
    }


# --- the resume checks ------------------------------------------------------------------------


def resume_event_id(approval_id: str) -> str:
    """The deterministic event id of an approval's resume wake."""

    return f"approval-{approval_id}-resolved"


def check_scope(scope: Any) -> dict[str, Any]:
    """A ``TurnAuthorityScopeV1`` from a sealed scope (an object or a mapping)."""

    read = scope.get if isinstance(scope, Mapping) else lambda name: getattr(scope, name)
    return {
        "tenant_id": str(read("tenant_id")),
        "agent_id": str(read("agent_id")),
        "binding_id": str(read("binding_id")),
        "binding_generation": int(read("binding_generation")),
    }


def turn_check_body(
    *,
    request_id: str,
    stage: str,
    context: str,
    payload: str,
    event_id: str,
    scope: Mapping[str, Any],
    approval_id: str,
) -> dict[str, Any]:
    """A ``TurnAuthorityRequestV1`` for one published wake, as the worker builds it."""

    return {
        "schema_version": 1,
        "request_id": request_id,
        "stage": stage,
        "context": context,
        "payload_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "event_id": event_id,
        "scope": dict(scope),
        "approval_id": approval_id,
    }


def grant_expectation(row: Mapping[str, Any]) -> dict[str, Any]:
    """An ``ApprovalGrantExpectationV1`` from the stored row, encoded as the worker does."""

    arguments = row.get("granted_arguments")
    if isinstance(arguments, str):
        arguments = json.loads(arguments)
    return {
        "approval_id": str(row["id"]),
        "agent_id": str(row["agent_id"]),
        "conversation_id": row["conversation_id"],
        "gate_kind": row["gate_kind"],
        "granted_tool": row.get("granted_tool"),
        "granted_arguments_json": None
        if arguments is None
        else json.dumps(
            arguments, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ),
    }


def grant_check_body(check: Mapping[str, Any], grant: Mapping[str, Any]) -> dict[str, Any]:
    """An ``ApprovalGrantAuthorityRequestV1``: a prepare-stage check and its grant."""

    return {"schema_version": 1, "check": dict(check), "grant": dict(grant)}


# --- stored rows, scrubbed ------------------------------------------------------------------------


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def selection_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    """An approval row's selection snapshot as evidence: subjects and digests, never the nonce."""

    approvers = _json_value(row.get("selected_approvers"))
    requester = _json_value(row.get("selection_requester"))
    snapshot = {
        "requester": requester,
        "approvers": approvers,
        "key_id": row.get("selection_key_id"),
        "connector": row.get("selection_connector"),
        "tool": row.get("selection_tool"),
        "arguments_sha256": row.get("selection_arguments_sha256"),
    }
    return {
        "id": row.get("id"),
        "status": row.get("status"),
        "route": row.get("route"),
        "gate_kind": row.get("gate_kind"),
        "granted_tool": row.get("granted_tool"),
        "selection_mode": row.get("selection_mode"),
        "selection_key_id": row.get("selection_key_id"),
        "selected": [item.get("subject") for item in approvers or [] if isinstance(item, Mapping)],
        "requester": requester.get("subject") if isinstance(requester, Mapping) else None,
        "selected_sha256": _digest(approvers),
        "snapshot_sha256": _digest(snapshot),
        "resumed": bool(row.get("resumed")),
    }


def scrub(value: Any) -> Any:
    """``value`` without any secret-bearing member, long strings cut short."""

    if isinstance(value, Mapping):
        return {
            str(key): scrub(item) for key, item in value.items() if str(key) not in _SECRET_NAMES
        }
    if isinstance(value, list):
        return [scrub(item) for item in value[:50]]
    if isinstance(value, str) and len(value) > _TEXT_LIMIT:
        return value[:_TEXT_LIMIT] + "..."
    return value


def audit_summary(row: Mapping[str, Any]) -> dict[str, Any]:
    """One approval audit row as evidence."""

    keep = (
        "id",
        "action",
        "actor",
        "principal_kind",
        "principal_subject",
        "decision",
        "authorizer",
        "authorized",
        "reason",
        "evidence",
        "created_at",
    )
    return {key: scrub(row.get(key)) for key in keep}


# --- judges -------------------------------------------------------------------------------


def selection_facts(row: Mapping[str, Any] | None) -> dict[str, Any]:
    """The selection evidence of an audit row: the evidence itself or its ``selection``."""

    evidence = (row or {}).get("evidence")
    if not isinstance(evidence, Mapping):
        return {}
    if evidence.get("kind") == SELECTION_KIND:
        return dict(evidence)
    nested = evidence.get("selection")
    return dict(nested) if isinstance(nested, Mapping) else {}


def _evidence(row: Mapping[str, Any] | None) -> Mapping[str, Any]:
    evidence = (row or {}).get("evidence")
    return evidence if isinstance(evidence, Mapping) else {}


def refusal_code(row: Mapping[str, Any] | None) -> Any:
    """The ``code`` a refusal audit row's evidence records, else None."""

    return _evidence(row).get("code")


def names_code(row: Mapping[str, Any] | None, code: str) -> bool:
    """Whether an audit row names ``code``, in its evidence or its reason."""

    if row is None:
        return False
    return code in json.dumps(row.get("evidence"), default=str) or code == row.get("reason")


def refusal_status(row: Mapping[str, Any] | None) -> Any:
    """The HTTP ``status`` a refusal audit row's evidence records, else None."""

    found = _evidence(row).get("status")
    return found if isinstance(found, int) and not isinstance(found, bool) else None


def newest(rows: Iterable[Mapping[str, Any]], action: str) -> dict[str, Any] | None:
    matching = [dict(row) for row in rows if row.get("action") == action]
    return matching[-1] if matching else None


def evidence_fact(row: Mapping[str, Any] | None, name: str) -> Any:
    """One member of an audit row's selection evidence, else None.

    The members judged are ``equality``, ``exclusion`` and ``exemption_used``,
    read from :func:`selection_facts` only, so a member the row does not carry
    reads None and fails a judge that wants true or false.
    """

    return selection_facts(row).get(name)


def judge_answer(
    result: Mapping[str, Any],
    *,
    http: int,
    row_after: str,
    action: str,
    code: str | None = None,
    status: int | None = None,
    principal_kind: str | None = None,
) -> list[str]:
    """Every way one answer differs from what it must be, as one line each."""

    problems = []
    if result.get("http") != http:
        problems.append(
            f"the answer was {result.get('http')} {result.get('detail')!r}, not {http}"
        )
    if result.get("status_after") != row_after:
        problems.append(f"the row is {result.get('status_after')!r} after it, not {row_after}")
    audit = list(result.get("audit") or [])
    row = newest(audit, action)
    if row is None:
        problems.append(
            f"no new {action} audit row (new rows: {[item.get('action') for item in audit]})"
        )
        return problems
    if row.get("authorized") is not (action == RESOLVED):
        problems.append(f"the {action} audit row reads authorized={row.get('authorized')!r}")
    if code is not None and not names_code(row, code):
        problems.append(
            f"the {action} audit row does not name {code} (code {refusal_code(row)!r}, "
            f"reason {row.get('reason')!r})"
        )
    if status is not None and refusal_status(row) != status:
        problems.append(
            f"the {action} audit row records status {refusal_status(row)!r}, not {status}"
        )
    if principal_kind is not None and row.get("principal_kind") != principal_kind:
        problems.append(
            f"the {action} audit row names principal kind {row.get('principal_kind')!r}"
        )
    return problems


RESUME_CHECKS = ("turn", "grant")


def judge_resume(result: Mapping[str, Any], *, decision: str) -> list[str]:
    """Both current checks of one published wake answer ``decision``, refusals audited.

    ``refuse`` needs ``caller_not_allowed`` and a new ``resume_refused`` row.
    ``unavailable`` (a lookup the directory cannot answer) needs
    ``authority_unavailable`` from both checks and one new ``resume_refused``
    row per check recording status 503 and ``authority_unavailable``: the
    composition audits every refusal on a canonical route, 503s included.
    """

    if not result.get("wake"):
        return ["the API published no resume wake for the approval"]
    problems = []
    reasons = {"refuse": CALLER_NOT_ALLOWED, "unavailable": AUTHORITY_UNAVAILABLE}
    for name in RESUME_CHECKS:
        answer = result.get(name) or {}
        if answer.get("http") != 200 or answer.get("decision") != decision:
            problems.append(
                f"the {name} check answered {answer.get('http')} "
                f"{answer.get('decision')!r}/{answer.get('reason')!r}, not {decision}"
            )
        elif decision in reasons and answer.get("reason") != reasons[decision]:
            problems.append(f"the {name} check answered {decision} for {answer.get('reason')!r}")
    audit = list(result.get("audit") or [])
    refused = [row for row in audit if row.get("action") == RESUME_REFUSED]
    if decision in reasons and not refused:
        problems.append(
            f"no new {RESUME_REFUSED} audit row (new rows: {[row.get('action') for row in audit]})"
        )
    if decision == "unavailable" and refused:
        if len(refused) < len(RESUME_CHECKS):
            problems.append(
                f"{len(refused)} new {RESUME_REFUSED} audit rows, not one per current check"
            )
        for row in refused:
            if refusal_status(row) != 503 or not names_code(row, AUTHORITY_UNAVAILABLE):
                problems.append(
                    f"a {RESUME_REFUSED} audit row records status {refusal_status(row)!r} "
                    f"and reason {row.get('reason')!r}, not 503 {AUTHORITY_UNAVAILABLE}"
                )
    return problems


def separation_change(details: Mapping[str, Any]) -> dict[str, Any]:
    """An ``approval_separation.changed`` event's route, old and new values and versions.

    The selector audit writes each as a flat member of the event's details; a
    member it does not carry reads None.
    """

    return {name: details.get(name) for name in SEPARATION_CHANGE_FIELDS}


def ticket_readback(routes: Any, route: str = tickets.TICKET_ROUTE) -> dict[str, Any]:
    """A route's read-back registration: key, ceiling users, pinned version and role subjects."""

    binding = routes.get(route) if isinstance(routes, Mapping) else None
    binding = binding if isinstance(binding, Mapping) else {}
    ticket = binding.get("approver_ticket")
    ceiling = (ticket or {}).get("ceiling") if isinstance(ticket, Mapping) else None
    ceiling = ceiling if isinstance(ceiling, Mapping) else {}
    pinned = _json_value(ceiling.get("approver_role"))
    entries = (
        ((pinned.get("filter") or {}).get("entries") or []) if isinstance(pinned, Mapping) else []
    )
    return {
        "bound": route in (routes or {}),
        "registered": isinstance(ticket, Mapping),
        "has_approvers_block": binding.get("approvers") is not None,
        "key_id": (ticket or {}).get("key_id") if isinstance(ticket, Mapping) else None,
        "users": [user.get("subject") for user in ceiling.get("users") or []],
        "approver_role_version": ceiling.get("approver_role_version"),
        "pinned_subjects": sorted(
            str(entry.get("subject")) for entry in entries if entry.get("kind") == "issuer_subject"
        ),
        "pinned_accounts": sum(1 for entry in entries if entry.get("kind") == "surface_account"),
        "pinned_legacy_state": pinned.get("legacy_state") if isinstance(pinned, Mapping) else None,
        "separation": binding.get("separation"),
    }


def transient(result: Mapping[str, Any]) -> bool:
    """An answer that met a concurrent identity change: 503 and the row unchanged."""

    return result.get("http") == 503 and result.get("status_after") == result.get(
        "status_before"
    )


def resume_transient(result: Mapping[str, Any]) -> bool:
    return any(
        (result.get(name) or {}).get("decision") == "unavailable" for name in RESUME_CHECKS
    )


def publishes_wake(result: Mapping[str, Any]) -> bool:
    """An answer that resolved the row, after which the API owes the session a wake."""

    return result.get("http") == 200 and result.get("status_after") in RESOLVED_STATUSES


# --- inside the API pod -------------------------------------------------------------------------


# The creation driver's, so ``dispatch`` reports either driver's refusal alike.
DriverRefusal = tickets.DriverRefusal
_settings = tickets._settings


def _runtime() -> Any:
    from curie_api.identity.http_initializer import configured_http_identity_runtime

    runtime = configured_http_identity_runtime(_settings())
    if runtime is None:
        raise DriverRefusal("the API has no HTTP identity runtime")
    return runtime


def _raw_call(
    method: str, path: str, raw: bytes | None, headers: Mapping[str, str]
) -> tuple[int, Any]:
    request = urllib.request.Request(tickets.API_LOOPBACK + path, data=raw, method=method)
    for key, value in headers.items():
        request.add_header(key, value)
    try:
        timeout = tickets.HTTP_TIMEOUT_S
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            status, body = response.status, response.read()
    except urllib.error.HTTPError as error:
        status, body = error.code, error.read()
    except OSError as error:
        return 0, {"detail": f"transport {type(error).__name__}"}
    try:
        return status, json.loads(body.decode("utf-8")) if body else None
    except ValueError:
        return status, None


def _json_call(
    method: str, path: str, body: Any, headers: Mapping[str, str]
) -> tuple[int, Any]:
    raw = None if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
    sent = dict(headers)
    if raw is not None:
        sent["Content-Type"] = "application/json"
    return _raw_call(method, path, raw, sent)


def _public(approval_id: str) -> dict[str, Any]:
    """The approval's status and its audit trail, through the platform API."""

    headers = {"X-API-Key": _settings().api_key}
    status, row = _json_call("GET", f"/approvals/{approval_id}", None, headers)
    audit_status, audit = _json_call("GET", f"/approvals/{approval_id}/audit", None, headers)
    if status != 200 or audit_status != 200 or not isinstance(audit, list):
        raise DriverRefusal(
            f"reading approval {approval_id} answered {status}, its audit {audit_status}"
        )
    return {"status": row.get("status") if isinstance(row, dict) else None, "audit": audit}


def _new_audit(before: Mapping[str, Any], after: Mapping[str, Any]) -> list[dict[str, Any]]:
    known = {row.get("id") for row in before["audit"]}
    return [audit_summary(row) for row in after["audit"] if row.get("id") not in known]


def _row_facts(ids: Sequence[str]) -> dict[str, Any]:
    from curie_api.db import SCHEMA
    from sqlalchemy import text

    async def body(session: Any) -> dict[str, Any]:
        found: dict[str, Any] = {}
        for approval_id in ids:
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT id::text AS id, status, route, gate_kind, granted_tool, "
                            "selection_mode, selection_key_id, selection_connector, "
                            "selection_tool, selection_requester, selected_approvers, "
                            "selection_arguments_sha256, (resumed_at IS NOT NULL) AS resumed "
                            f'FROM "{SCHEMA}".approvals WHERE id::text = :i'
                        ),
                        {"i": str(approval_id)},
                    )
                )
                .mappings()
                .first()
            )
            found[str(approval_id)] = None if row is None else selection_summary(dict(row))
        return found

    return dict(tickets._sql(body))


def _rows(params: Mapping[str, Any]) -> dict[str, Any]:
    return {"rows": _row_facts([str(item) for item in params["ids"]])}


def _create(params: Mapping[str, Any]) -> dict[str, Any]:
    """Each batch through the creation driver: one selected set per batch."""

    tickets.APPROVAL_TTL_S = int(params.get("ttl_s") or APPROVAL_TTL_S)
    reports: list[dict[str, Any]] = []
    for batch in params["batches"]:
        labels = tickets.turn_labels(batch["cases"])
        result = tickets._create(
            {
                "agent_id": params["agent_id"],
                "key": params["key"],
                "registered": params["registered"],
                "approvers": batch["approvers"],
                "nonces": batch["nonces"],
                "turns": {label: params["turns"][label] for label in labels},
                "cases": batch["cases"],
            }
        )
        reports.extend(result["cases"])
    created = [
        str(answer["id"]) for item in reports for answer in item["results"] if answer.get("id")
    ]
    return {"cases": reports, "rows": _row_facts(created)}


def _answer(params: Mapping[str, Any]) -> dict[str, Any]:
    """One answer: an email reply forwarded by the mail adapter, or an operator's."""

    from curie_api import adapter_principal
    from curie_internal import identity_approval as wire
    from curie_internal import identity_context as codec
    from curie_internal.sandbox_token import b64url_decode

    settings = _settings()
    approval_id = str(uuid.UUID(str(params["approval_id"])))
    before = _public(approval_id)
    body = answer_body(str(params["decision"]), params.get("note", NOTE))
    headers = {"Content-Type": "application/json"}
    operator = params.get("operator")
    if operator:
        status, minted = _json_call(
            "POST", OPERATOR_MINT_PATH, {"subject": str(operator)}, {"X-API-Key": settings.api_key}
        )
        token = minted.get("token") if isinstance(minted, dict) else None
        if status != 201 or not isinstance(token, str):
            raise DriverRefusal(f"minting the operator principal answered {status}")
        headers[APPROVAL_PRINCIPAL_HEADER] = token
    else:
        receiver = params["receiver"]
        sender = str(params["sender"]).lower()
        headers[ADAPTER_PRINCIPAL_HEADER] = adapter_principal.mint(
            settings.api_key,
            subject=str(params["adapter"]),
            bindings=[str(params["binding_id"])],
            exp=int(time.time()) + ADAPTER_TOKEN_TTL_S,
        )
        headers[APPROVAL_ACTOR_HEADER] = sender
        ring = codec.build_identity_keyring(
            "receiver",
            (
                (
                    str(receiver["receiver_id"]),
                    int(receiver["generation"]),
                    b64url_decode(str(receiver["key"])),
                ),
            ),
        )
        proof = wire.EmailApprovalAnswerProofV1.model_validate(
            answer_proof_fields(
                receiver_id=str(receiver["receiver_id"]),
                receiver_generation=int(receiver["generation"]),
                binding_id=str(params["binding_id"]),
                approval_id=approval_id,
                sender=sender,
                body=body,
            )
        )
        headers[ANSWER_PROOF_HEADER] = wire.sign_email_approval_answer_proof(proof, ring)
    status, payload = _raw_call(
        "POST", RESOLVE_PATH.format(approval_id=approval_id), body, headers
    )
    after = _public(approval_id)
    # The resolve route publishes the wake before it answers and marks the row
    # resumed once the append landed; a refused publication leaves it unmarked.
    facts = _row_facts([approval_id]).get(approval_id) or {}
    return {
        "http": status,
        "detail": None if 200 <= status < 300 else tickets.detail_code(payload),
        "approval_status": payload.get("status") if isinstance(payload, dict) else None,
        "status_before": before["status"],
        "status_after": after["status"],
        "audit": _new_audit(before, after),
        "resumed": bool(facts.get("resumed")),
    }


def _settled(params: Mapping[str, Any]) -> dict[str, Any]:
    """Which events the worker has finished: its done marker exists, for at most ``wait_s``.

    The worker writes the marker for every terminal outcome, a refused turn
    included; the API reads the same keyspace for terminal reviews.
    """

    import redis
    from curie_internal.keyspace import done_key

    settings = _settings()
    events = [str(event) for event in params["events"]]
    deadline = time.monotonic() + min(max(float(params.get("wait_s") or 0), 0.0), SETTLE_CALL_S)
    client = redis.Redis.from_url(settings.valkey_dsn(), decode_responses=True)
    try:
        while True:
            pipe = client.pipeline(transaction=False)
            for event in events:
                pipe.exists(done_key(settings.worker_key_prefix, event))
            marked = pipe.execute()
            pending = [event for event, found in zip(events, marked, strict=True) if not found]
            if not pending or time.monotonic() >= deadline:
                break
            time.sleep(SETTLE_POLL_S)
    finally:
        client.close()
    return {"pending": pending}


def _principals(params: Mapping[str, Any]) -> dict[str, Any]:
    """Each subject's canonical ``principals`` rows (type and status), in one read."""

    from curie_api.db import SCHEMA
    from sqlalchemy import text

    subjects = [str(subject) for subject in params["subjects"]]

    async def body(session: Any) -> list[dict[str, Any]]:
        result = await session.execute(
            text(
                f'SELECT idp_subject AS subject, type, status FROM "{SCHEMA}".principals '
                "WHERE tenant_id::text = :t AND idp_issuer = :i AND idp_subject = ANY(:s) "
                "ORDER BY idp_subject, id"
            ),
            {"t": str(params["tenant_id"]), "i": str(params["issuer"]), "s": subjects},
        )
        return [dict(row._mapping) for row in result]

    rows: dict[str, list[dict[str, Any]]] = {subject: [] for subject in subjects}
    for row in tickets._sql(body):
        rows.setdefault(str(row["subject"]), []).append(
            {"type": row.get("type"), "status": row.get("status")}
        )
    return {"rows": rows}


def _find_wake(client: Any, stream: str, event_id: str) -> tuple[str, str, str] | None:
    """The newest runs-stream entry carrying ``event_id``'s payload and a sealed context.

    Read with XREVRANGE, which consumes nothing.
    """

    last, read = "+", 0
    while read < WAKE_SCAN:
        entries = client.xrevrange(stream, max=last, min="-", count=WAKE_PAGE)
        for entry_id, fields in entries or []:
            if entry_id == last:
                continue
            read += 1
            payload, context = fields.get("payload"), fields.get(tickets.CONTEXT_FIELD)
            if not payload or not context:
                continue
            try:
                queued = json.loads(payload)
            except ValueError:
                continue
            if isinstance(queued, dict) and queued.get("event_id") == event_id:
                return str(entry_id), str(payload), str(context)
        if not entries or len(entries) < WAKE_PAGE:
            return None
        last = entries[-1][0]
    return None


def _grant_row(approval_id: str) -> dict[str, Any]:
    from curie_api.db import SCHEMA
    from sqlalchemy import text

    async def body(session: Any) -> Any:
        return (
            (
                await session.execute(
                    text(
                        "SELECT id::text AS id, agent_id::text AS agent_id, conversation_id, "
                        "gate_kind, granted_tool, granted_arguments "
                        f'FROM "{SCHEMA}".approvals WHERE id::text = :i'
                    ),
                    {"i": approval_id},
                )
            )
            .mappings()
            .first()
        )

    row = tickets._sql(body)
    if row is None:
        raise DriverRefusal(f"approval {approval_id} is gone")
    return dict(row)


def _decision(status: int, body: Any) -> dict[str, Any]:
    body = body if isinstance(body, dict) else {}
    return {
        "http": status,
        "decision": body.get("decision"),
        "reason": body.get("reason"),
        "detail": None if status == 200 else tickets.detail_code(body),
    }


def _resume(params: Mapping[str, Any]) -> dict[str, Any]:
    """The resume current check and the grant check of one approval's published wake."""

    import redis
    from curie_internal import identity_approval as wire

    settings = _settings()
    runtime = _runtime()
    approval_id = str(uuid.UUID(str(params["approval_id"])))
    event_id = resume_event_id(approval_id)
    client = redis.Redis.from_url(settings.valkey_dsn(), decode_responses=True)
    try:
        found = _find_wake(client, settings.runs_stream, event_id)
    finally:
        client.close()
    if found is None:
        return {"wake": False, "audit": []}
    stream_id, payload, context = found
    resume = wire.open_resume_context(context, runtime._context_keys)
    if not isinstance(resume, wire.ResumeContextV1):
        raise DriverRefusal("the published wake's resume context does not open with the API keys")
    scope = check_scope(resume.scope)
    worker = {WORKER_TOKEN_HEADER: settings.internal_worker_token}
    before = _public(approval_id)

    def check(stage: str) -> dict[str, Any]:
        return turn_check_body(
            request_id=str(uuid.uuid4()),
            stage=stage,
            context=context,
            payload=payload,
            event_id=event_id,
            scope=scope,
            approval_id=approval_id,
        )

    turn_status, turn = _json_call("POST", TURN_CHECK_PATH, check(RESUME_STAGE), worker)
    grant = grant_expectation(_grant_row(approval_id))
    grant_status, granted = _json_call(
        "POST", GRANT_CHECK_PATH, grant_check_body(check(GRANT_STAGE), grant), worker
    )
    after = _public(approval_id)
    facts = _row_facts([approval_id]).get(approval_id) or {}
    return {
        "wake": True,
        "stream_id": stream_id,
        "wake_status": resume.status,
        "turn": _decision(turn_status, turn),
        "grant": _decision(
            grant_status, granted.get("check") if isinstance(granted, dict) else None
        ),
        "audit": _new_audit(before, after),
        "resumed": bool(facts.get("resumed")),
    }


def _identity_audits(params: Mapping[str, Any]) -> dict[str, Any]:
    from curie_api.db import SCHEMA
    from sqlalchemy import text

    async def body(session: Any) -> list[dict[str, Any]]:
        result = await session.execute(
            text(
                "SELECT id::text AS id, event_type, actor, details, created_at "
                f'FROM "{SCHEMA}".identity_audit_events WHERE tenant_id::text = :t '
                "AND event_type = :e ORDER BY created_at, id"
            ),
            {"t": str(params["tenant_id"]), "e": str(params["event_type"])},
        )
        return [dict(row._mapping) for row in result]

    rows = tickets._sql(body)
    return {
        "events": [
            {
                **row,
                "details": scrub(_json_value(row.get("details"))),
                "created_at": str(row.get("created_at")),
            }
            for row in rows
        ]
    }


def _role_raw(params: Mapping[str, Any]) -> dict[str, Any]:
    """The approver role as stored (to restore it exactly) and as the creation driver reads it."""

    from curie_api import agent_role_policy

    tenant, agent = uuid.UUID(params["tenant_id"]), uuid.UUID(params["agent_id"])

    async def body(session: Any) -> Any:
        async with session.begin():
            found, policy, _ = await agent_role_policy.lock_policy_rows(
                session, tenant_id=tenant, agent_id=agent, require_active=False
            )
            if found is None:
                raise DriverRefusal("the agent does not exist")
            roles = None if policy is None else policy.role_configuration
            return copy.deepcopy((roles or {}).get("approver"))

    raw = tickets._sql(body)
    state = tickets._approver_role({"tenant_id": str(tenant), "agent_id": str(agent)})
    return {**state, "raw": raw}


def _role(params: Mapping[str, Any]) -> dict[str, Any]:
    """Read or set the approver role; ``migrate`` migrates it with ``filter`` once."""

    if not params.get("migrate"):
        return tickets._approver_role(params)
    from curie_api import agent_role_policy
    from curie_api.schemas.agent_access import MigrateRoleV1

    tenant, agent = str(params["tenant_id"]), str(params["agent_id"])
    state = tickets._approver_role({"tenant_id": tenant, "agent_id": agent})
    if state.get("legacy_state") == "migrated":
        return tickets._approver_role(
            {"tenant_id": tenant, "agent_id": agent, "filter": params["filter"]}
        )
    change = MigrateRoleV1.model_validate(
        {
            "schema_version": 1,
            "tenant_id": tenant,
            "agent_id": agent,
            "expected_version": state["version"],
            "role": "approver",
            "operation": "migrate_role",
            "filter": params["filter"],
            "raw_account_revocation": "separate_removal_required",
        }
    )

    async def body(session: Any) -> int:
        async with session.begin():
            result = await agent_role_policy.apply_role_change(session, change=change)
        return int(result.version)

    tickets._sql(body)
    after = tickets._approver_role({"tenant_id": tenant, "agent_id": agent})
    return {**after, "action": "migrated"}


def _role_restore(params: Mapping[str, Any]) -> dict[str, Any]:
    """Put the stored approver role back exactly, or remove it when there was none.

    The role-policy API can neither un-migrate a role nor delete one, so the
    row is written here under the role policy's lock order, with the version
    bump and the audit event a role change records.
    """

    from curie_api import agent_role_policy, identity_audit
    from sqlalchemy import func

    tenant, agent = uuid.UUID(params["tenant_id"]), uuid.UUID(params["agent_id"])
    wanted = params.get("raw")

    async def body(session: Any) -> dict[str, Any]:
        async with session.begin():
            found, policy, _ = await agent_role_policy.lock_policy_rows(
                session, tenant_id=tenant, agent_id=agent, require_active=False
            )
            if found is None or policy is None:
                raise DriverRefusal("the agent or its access policy row is gone")
            roles = dict(policy.role_configuration or {})
            current = roles.get("approver")
            if current == wanted:
                return {"action": "unchanged", "version": policy.version}
            if wanted is None:
                roles.pop("approver", None)
            else:
                roles["approver"] = copy.deepcopy(wanted)
            previous = policy.version
            policy.role_configuration = (
                None if not roles and params.get("configured") is False else roles
            )
            policy.version = previous + 1
            policy.updated_at = func.now()
            identity_audit.record(
                session,
                tenant_id=tenant,
                event_type="identity.agent_role_policy_changed",
                actor="identity-e2e",
                details={
                    "agent_id": str(agent),
                    "role": "approver",
                    "operation": "identity_e2e_restore_role",
                    "previous_version": previous,
                    "version": previous + 1,
                    "previous_legacy_state": (current or {}).get("legacy_state"),
                    "legacy_state": (wanted or {}).get("legacy_state"),
                },
            )
            await session.flush()
            return {"action": "restored", "version": previous + 1}

    result = tickets._sql(body)
    state = tickets._approver_role({"tenant_id": str(tenant), "agent_id": str(agent)})
    return {**result, "state": state}


_ACTIONS: dict[str, Callable[[Mapping[str, Any]], dict[str, Any]]] = {
    "create": _create,
    "rows": _rows,
    "answer": _answer,
    "resume": _resume,
    "settled": _settled,
    "principals": _principals,
    "identity_audits": _identity_audits,
    "role": _role,
    "role_raw": _role_raw,
    "role_restore": _role_restore,
}
# The creation driver's actions this scenario reuses as they are.
_DELEGATED = ("capture", "principal", "agent_routes", "approver_role", "count_agent")


def dispatch(params: Any) -> dict[str, Any]:
    """Run one action; any failure becomes ``{"error": ...}`` without echoing inputs."""

    action = params.get("action") if isinstance(params, Mapping) else None
    if action in _DELEGATED:
        return tickets.dispatch(params)
    handler = _ACTIONS.get(action) if isinstance(action, str) else None
    if handler is None:
        return {"error": f"unknown action {str(action)[:40]!r}"}
    try:
        return handler(params)
    except DriverRefusal as exc:
        return {"error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - product exception text may quote inputs.
        database = tickets._database_message(exc)
        code = getattr(exc, "code", None)
        if isinstance(code, str) and tickets._REFUSAL_CODE.fullmatch(code):
            database = database or code
        suffix = f": {database}" if database else ""
        return {"error": f"{action} failed with {type(exc).__name__}{suffix}"}


# This file's and the creation driver's text, sent to the pod on stdin. The
# bootstrap loads the creation driver as a module first, so this file's
# ``import approver_ticket_driver`` finds it, then runs this file the same way.
SOURCE = Path(__file__).read_text(encoding="utf-8") if "__file__" in globals() else ""
SOURCE_PARAM = "answer_driver_source"
TICKETS_PARAM = "ticket_driver_source"
POD_BOOTSTRAP = (
    "import json, sys, types\n"
    "P = json.load(sys.stdin)\n"
    "def load(name, source):\n"
    "    module = types.ModuleType(name)\n"
    "    sys.modules[name] = module\n"
    "    exec(compile(source, name + '.py', 'exec'), module.__dict__)\n"
    "    return module\n"
    f"load('approver_ticket_driver', P.pop({TICKETS_PARAM!r}))\n"
    f"D = load('answer_authority_driver', P.pop({SOURCE_PARAM!r}))\n"
    "print(json.dumps(D.dispatch(P), sort_keys=True, default=str))\n"
)


def pod_params(action: str, **params: Any) -> dict[str, Any]:
    """What ``Target.pod_python(POD_BOOTSTRAP, ...)`` sends on stdin for one action."""

    return {SOURCE_PARAM: SOURCE, TICKETS_PARAM: tickets.SOURCE, "action": action, **params}
