"""ADR 0271 per-request approver selection: registration, gate, lock and creation checks.

``verify_ticket`` is the frozen pure preparation check (INTERFACE.md). Everything
else here is the platform's creation-side wiring of decisions 1, 2 and 4:

1. Registration (decision 1): the closed registration shape lives on
   ``schemas.approvals.ApprovalRouteBinding``; ``prepare_selector_write`` pins
   the operator-approved approver role and builds the audit rows for one route
   map replacement, inside the caller's row-locked transaction.
2. Serving bundle gate (decision 2): ``check_bundle_gate`` refuses a bundle
   route map that does not send a registered connector tool to its route.
3. Route lock (decision 2): ``check_tool_route`` reads the lock key only from
   ``granted_tool`` and the stored registrations, never from ``gate_kind``.
4. Creation (decision 4): ``verify_creation`` is the pure ticket, scope and
   requester check; ``check_selected_approvers`` judges each selected person
   from canonical identity records inside the identity writer.

Every refusal here carries a stable code; the creation owners map it onto
their own response envelope.
"""

from __future__ import annotations

import hashlib
import uuid
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from aci_protocol import ApprovalRequest
from curie_internal import identity_context as codec
from curie_internal.approver_ticket import (
    ApprovalTicket,
    TicketError,
    check_execution_size,
    verify_preparation,
)
from nacl.signing import VerifyKey
from plugin_format import connector_tool_prefix
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from . import agent_role_policy
from .action_forward import canonical_arguments
from .crud.deployments import list_active_deployment_versions
from .models import Agent, AgentAccessPolicy, Approval, Principal
from .remediation_nominations import canonical_text
from .schemas.agent_access import IssuerSubjectSelectorV1, RoleConfigurationV1
from .schemas.approvals import ApprovalRouteBinding, ApproverTicketRegistration

# --- refusal codes ------------------------------------------------------------------

# Registration (the operator route write and the flag writer).
VERSION_CONFLICT = "approval_routes.version_conflict"
EXPECTED_VERSION_REQUIRED = "approval_routes.expected_version_required"
REQUIRES_REQUESTER_IDENTITY = "approver_ticket_requires_requester_identity"
CEILING_INVALID = "approver_ticket_ceiling_invalid"
STALE_ROLE_POLICY_VERSION = "stale_role_policy_version"
PAIR_CONFLICT = "approver_ticket_pair_conflict"
NO_SERVING_BUNDLE = "approver_ticket.no_serving_bundle"
SELECTOR_REGISTERED = "requester_identity_selector_registered"
BUNDLE_GATE_MISMATCH = "approver_ticket.bundle_gate_mismatch"

# Creation (both creation owners).
ROUTE_LOCKED = "approval_route_locked"
ROUTE_MISMATCH = "approval_ticket_route_mismatch"
TICKET_REQUIRED = "approval_ticket_required"
TICKET_UNREGISTERED = "approval_ticket_unregistered"
STATE_INVALID = "approver_ticket_state_invalid"
REQUIRES_HTTP_CREATION = "approval_ticket_requires_http_creation"
REPLAYED = "approval_ticket_replayed"
ARGUMENTS_UNSTABLE = "approval_ticket_arguments_unstable"
APPROVER_INACTIVE = "approval_ticket_approver_inactive"
OUTSIDE_CEILING = "approval_ticket_outside_ceiling"

TICKET_MEMBER = "approval_ticket"
_ROLE: TypeAdapter[Any] = TypeAdapter(RoleConfigurationV1)


@dataclass(frozen=True)
class ApprovalSelection:
    """The immutable selection snapshot a dynamic approval row stores (plan D10)."""

    requester: dict[str, Any]
    connector: str
    tool: str
    key_id: str
    nonce: str
    arguments_sha256: str
    approvers: list[dict[str, Any]]


class ApproverTicketRefusal(Exception):
    """A refusal with its HTTP status and stable code."""

    def __init__(self, status: int, code: str, message: str | None = None) -> None:
        super().__init__(message or code)
        self.status = status
        self.code = code


class BundleGateMismatch(Exception):
    """A bundle route map does not gate a registered connector tool on its route.

    ``code`` lets ``gitflow._rejected`` and the API 422 render one envelope from
    the exception alone, like ``deploy.ApprovalRoutesUnbound``.
    """

    code = BUNDLE_GATE_MISMATCH


def verify_ticket(
    *,
    public_key: VerifyKey,
    key_id: str,
    arguments: dict[str, Any],
    clock: Callable[[], int],
) -> ApprovalTicket:
    """Check signed preparation facts using the production argument canonicalizer.

    The only ticket source is arguments["approval_ticket"].

    Route locking, registered key lookup, original-requester comparison, current
    identity/ceiling checks and atomic nonce spending belong to creation owners.
    This result alone never authorizes creation or execution. Delayed execution
    must not call this preparation-expiry check after approval acceptance.
    """
    try:
        if type(arguments) is not dict:
            raise TypeError
        execution = {key: value for key, value in arguments.items() if key != "approval_ticket"}
        digest = hashlib.sha256(canonical_arguments(execution).encode("utf-8")).hexdigest()
    except (ValueError, TypeError, RecursionError):
        raise TicketError("invalid_arguments") from None
    token = arguments.get("approval_ticket")
    if type(token) is not str:
        raise TicketError("invalid_ticket")
    return verify_preparation(
        token, public_key=public_key, key_id=key_id, now=clock(), arguments_sha256=digest
    )


# --- shared readers -------------------------------------------------------------------


def _selector_bindings(routes: Any) -> Iterator[tuple[str, dict[str, Any]]]:
    """Every raw route binding that carries an ``approver_ticket`` key, even malformed."""

    if isinstance(routes, dict):
        for name, binding in routes.items():
            if isinstance(binding, dict) and "approver_ticket" in binding:
                yield str(name), binding


def has_selector(routes: Any) -> bool:
    """Whether any route of a raw map carries an ``approver_ticket`` key, even malformed."""

    return next(_selector_bindings(routes), None) is not None


def live_pair(granted_tool: object) -> tuple[str, str] | None:
    """``mcp__<connector>__<tool>`` split once, exactly as the worker mints grants.

    ``kernel/claim.py::_connector_tool_grant``: a plugin server (``mcp__plugin_``)
    is never a connector, and both halves must be nonempty; the upstream tool
    keeps any later ``__``.
    """

    if not isinstance(granted_tool, str):
        return None
    if not granted_tool.startswith("mcp__") or granted_tool.startswith("mcp__plugin_"):
        return None
    connector, separator, upstream = granted_tool.removeprefix("mcp__").partition("__")
    if not separator or not connector or not upstream:
        return None
    return connector, upstream


def gates_pair(route_map: dict[str, str] | None, route: str, connector: str, tool: str) -> bool:
    """Whether a bundle's effective route map sends the connector tool to ``route``."""

    return route_map is not None and route_map.get(connector_tool_prefix(connector) + tool) == route


def _raw_pairs(routes: Any) -> list[tuple[str, str | None, str | None]]:
    """Every ``(route, connector, tool)`` a raw map registers; unreadable halves are None."""

    pairs: list[tuple[str, str | None, str | None]] = []
    for name, binding in _selector_bindings(routes):
        ticket = binding["approver_ticket"]
        connector = ticket.get("connector") if isinstance(ticket, dict) else None
        tool = ticket.get("tool") if isinstance(ticket, dict) else None
        pairs.append(
            (
                name,
                connector if isinstance(connector, str) else None,
                tool if isinstance(tool, str) else None,
            )
        )
    return pairs


def check_bundle_gate(
    route_map: dict[str, str] | None,
    routes: Any,
    *,
    version_id: uuid.UUID | None = None,
) -> None:
    """Refuse a bundle whose effective route map does not gate every registered pair.

    ``route_map`` is ``bundles.declared_approval_route_map`` (last-declaration
    wins; ``None`` is poison). ``routes`` is the agent's raw ``approval_routes``
    (stored or proposed). A map with no registration is never refused here; an
    unreadable registration refuses, because its pair cannot be proven gated.
    """

    pairs = _raw_pairs(routes)
    if not pairs:
        return
    subject = "the bundle" if version_id is None else f"the bundle for version {version_id}"
    for route, connector, tool in pairs:
        if connector is None or tool is None:
            raise BundleGateMismatch(
                f"route {route!r} carries an approver_ticket registration whose connector "
                "and tool cannot be read, so no bundle can be proven to gate it; rewrite "
                "the registration"
            )
        if not gates_pair(route_map, route, connector, tool):
            live = connector_tool_prefix(connector) + tool
            raise BundleGateMismatch(
                f"{subject} does not gate {live!r} on approval route {route!r}, where this "
                "agent registers its approver_ticket selector; the serving bundle's last "
                "declaration for that tool must name that route"
            )


# --- registration (decision 1) ------------------------------------------------------------


def check_pair_conflicts(bindings: dict[str, ApprovalRouteBinding]) -> None:
    """One connector tool is registered on at most one route of a map."""

    owners: dict[tuple[str, str], str] = {}
    for name, binding in bindings.items():
        ticket = binding.approver_ticket
        if ticket is None:
            continue
        pair = (ticket.connector, ticket.tool)
        if pair in owners:
            raise ApproverTicketRefusal(
                422,
                PAIR_CONFLICT,
                f"connector {ticket.connector!r} tool {ticket.tool!r} is registered on both "
                f"{owners[pair]!r} and {name!r}; a pair belongs to exactly one route",
            )
        owners[pair] = name


async def _pin_role(session: AsyncSession, agent: Agent, named_version: int) -> str:
    """The approver role at ``named_version``, canonically rendered, or a refusal."""

    row = (
        await session.execute(
            select(AgentAccessPolicy, AgentAccessPolicy.role_configuration.is_(None))
            .where(AgentAccessPolicy.agent_id == agent.id)
            .execution_options(populate_existing=True)
        )
    ).one_or_none()
    invalid = ApproverTicketRefusal(
        422,
        CEILING_INVALID,
        "approver_ticket needs a readable, restricted approver role with at least one "
        "entry in the agent's access policy",
    )
    if row is None:
        raise invalid
    policy, sql_null = row
    if policy.version != named_version:
        raise ApproverTicketRefusal(
            409,
            STALE_ROLE_POLICY_VERSION,
            f"ceiling.approver_role_version {named_version} is not the agent's current "
            f"access policy version {policy.version}; read it again and name it",
        )
    try:
        snapshot = agent_role_policy.stored_snapshot(
            policy,
            sql_null,
            tenant_id=agent.tenant_id,
            agent_id=agent.id,
        )
    except agent_role_policy.RolePolicyRefusal:
        raise invalid from None
    approver = None if snapshot is None else snapshot.roles.approver
    if approver is None or approver.filter is None or not approver.filter.entries:
        raise invalid
    configuration = policy.role_configuration
    assert isinstance(configuration, dict)
    return canonical_text(configuration["approver"])


def _stored_ticket(routes: Any, name: str) -> dict[str, Any] | None:
    binding = routes.get(name) if isinstance(routes, dict) else None
    ticket = binding.get("approver_ticket") if isinstance(binding, dict) else None
    return ticket if isinstance(ticket, dict) else None


def _stored_exclusion(routes: Any, name: str) -> bool | None:
    binding = routes.get(name) if isinstance(routes, dict) else None
    separation = binding.get("separation") if isinstance(binding, dict) else None
    value = separation.get("exclude_requester") if isinstance(separation, dict) else None
    return value if isinstance(value, bool) else None


def _unchanged(ticket: ApproverTicketRegistration, stored: dict[str, Any] | None) -> bool:
    """The operator fields equal the stored registration, naming its pinned version."""

    if stored is None or not isinstance(stored.get("ceiling"), dict):
        return False
    ceiling = stored["ceiling"]
    users = None if ticket.ceiling.users is None else [u.model_dump() for u in ticket.ceiling.users]
    return (
        (ticket.connector, ticket.tool, ticket.key_id, ticket.public_key)
        == (
            stored.get("connector"),
            stored.get("tool"),
            stored.get("key_id"),
            stored.get("public_key"),
        )
        and users == ceiling.get("users")
        and ticket.ceiling.approver_role_version == ceiling.get("approver_role_version")
        and isinstance(ceiling.get("approver_role"), str)
    )


@dataclass(frozen=True)
class SelectorAudit:
    """One identity audit row a selector route write records."""

    event_type: str
    details: dict[str, Any]


def _role_version(ticket: dict[str, Any] | None) -> Any:
    ceiling = ticket.get("ceiling") if ticket is not None else None
    return ceiling.get("approver_role_version") if isinstance(ceiling, dict) else None


async def prepare_selector_write(
    session: AsyncSession,
    agent: Agent,
    proposed: dict[str, Any],
    *,
    new_version: int,
) -> tuple[dict[str, Any], list[SelectorAudit]]:
    """The stored map and audit rows for one selector route map replacement.

    Runs inside the caller's transaction, after the agent row is locked and its
    expected version compared. Judges each registration (decision 1), pins its
    ceiling, and stores the effective requester exclusion. A registration
    identical to the stored one on the same route keeps its pin and is not
    judged against the current policy version.
    """

    bindings = {name: ApprovalRouteBinding.model_validate(raw) for name, raw in proposed.items()}
    check_pair_conflicts(bindings)
    stored = agent.approval_routes
    if any(b.approver_ticket is not None for b in bindings.values()) and not (
        agent.connector_requester_identity
    ):
        raise ApproverTicketRefusal(
            422,
            REQUIRES_REQUESTER_IDENTITY,
            "registering an approver_ticket selector requires the agent's "
            "connector_requester_identity flag on",
        )
    written: dict[str, Any] = {}
    for name, binding in bindings.items():
        raw = binding.model_dump()
        ticket = binding.approver_ticket
        if ticket is not None:
            previous = _stored_ticket(stored, name)
            if _unchanged(ticket, previous):
                assert previous is not None
                pin = previous["ceiling"]["approver_role"]
            else:
                pin = await _pin_role(session, agent, ticket.ceiling.approver_role_version)
            if ticket.ceiling.approver_role is not None and ticket.ceiling.approver_role != pin:
                raise ApproverTicketRefusal(
                    422,
                    CEILING_INVALID,
                    "ceiling.approver_role is server-owned: omit it, or echo the pinned "
                    "value back unchanged",
                )
            raw["approver_ticket"]["ceiling"]["approver_role"] = pin
            exclude = True if binding.separation is None else binding.separation.exclude_requester
            raw["separation"] = {"exclude_requester": exclude}
        written[name] = raw
    held = set(_registered_names(stored))
    old_version = agent.connector_requester_identity_version
    audits: list[SelectorAudit] = []
    for name in sorted(held | set(_registered_names(written))):
        before, after = _stored_ticket(stored, name), _stored_ticket(written, name)
        if before is None and name in held:
            before = {}  # A stored registration too malformed to read is still replaced.
        old_exclude = _stored_exclusion(stored, name) if before is not None else None
        new_exclude = _stored_exclusion(written, name) if after is not None else None
        named = after if after is not None else before
        assert named is not None
        details = {
            "route": name,
            "key_id": named.get("key_id"),
            "connector": named.get("connector"),
            "tool": named.get("tool"),
            "old_key_id": None if before is None else before.get("key_id"),
            "old_configuration_version": old_version,
            "new_configuration_version": new_version,
            "old_exclude_requester": old_exclude,
            "new_exclude_requester": new_exclude,
            "old_approver_role_version": _role_version(before),
            "new_approver_role_version": _role_version(after),
        }
        if before is None:
            audits.append(SelectorAudit("approver_ticket.registered", details))
        elif after is None:
            audits.append(SelectorAudit("approver_ticket.removed", details))
        elif before != after:
            audits.append(SelectorAudit("approver_ticket.replaced", details))
        if after is not None and (
            (before is None and new_exclude is False)
            or (before is not None and old_exclude != new_exclude)
        ):
            audits.append(SelectorAudit("approval_separation.changed", details))
    return written, audits


def _registered_names(routes: Any) -> Iterable[str]:
    return [name for name, _ in _selector_bindings(routes)]


# --- route lock (decision 2) ---------------------------------------------------------------


@dataclass(frozen=True)
class StoredRegistration:
    """A stored registration read strictly, with its parsed pinned ceiling."""

    route: str
    ticket: ApproverTicketRegistration
    pinned_role: Any
    exclude_requester: bool

    @property
    def pair(self) -> tuple[str, str]:
        return self.ticket.connector, self.ticket.tool


def _parse_registration(route: str, raw: Any) -> StoredRegistration:
    """A stored registration, or ``approver_ticket_state_invalid`` (fail closed)."""

    try:
        binding = ApprovalRouteBinding.model_validate(raw)
        ticket = binding.approver_ticket
        if ticket is None or binding.separation is None or ticket.ceiling.approver_role is None:
            raise ValueError("incomplete stored registration")
        role = _ROLE.validate_json(ticket.ceiling.approver_role)
        if role.filter is None or not role.filter.entries:
            raise ValueError("unrestricted pinned role")
    except (ValidationError, ValueError, TypeError):
        raise ApproverTicketRefusal(409, STATE_INVALID) from None
    return StoredRegistration(route, ticket, role, binding.separation.exclude_requester)


def stored_registrations(routes: Any) -> dict[tuple[str, str], StoredRegistration]:
    """Every stored registration by pair; malformed or duplicated pairs fail closed."""

    found: dict[tuple[str, str], StoredRegistration] = {}
    for name, binding in _selector_bindings(routes):
        registration = _parse_registration(name, binding)
        if registration.pair in found:
            raise ApproverTicketRefusal(409, STATE_INVALID)
        found[registration.pair] = registration
    return found


def check_tool_route(agent: Agent | None, data: ApprovalRequest) -> StoredRegistration | None:
    """Lock a registered connector tool to its route, both ways (plan D7).

    The lock key is parsed only from ``granted_tool``; ``gate_kind`` never
    enables or bypasses it, and a routeless or empty route is not the owning
    route. Returns the registration for a dynamic request, None for an
    ordinary one.
    """

    registered = stored_registrations(None if agent is None else agent.approval_routes)
    pair = live_pair(data.granted_tool)
    owner = None if pair is None else registered.get(pair)
    if owner is not None and data.route != owner.route:
        raise ApproverTicketRefusal(409, ROUTE_LOCKED)
    arguments = data.granted_arguments
    here = next(
        (item for item in registered.values() if data.route and item.route == data.route),
        None,
    )
    if here is None:
        if isinstance(arguments, dict) and TICKET_MEMBER in arguments:
            raise ApproverTicketRefusal(409, TICKET_UNREGISTERED)
        return None
    if pair != here.pair:
        raise ApproverTicketRefusal(409, ROUTE_MISMATCH)
    if not isinstance(arguments, dict) or type(arguments.get(TICKET_MEMBER)) is not str:
        raise ApproverTicketRefusal(409, TICKET_REQUIRED)
    assert agent is not None
    if not agent.connector_requester_identity:
        raise ApproverTicketRefusal(409, REQUIRES_REQUESTER_IDENTITY)
    return here


# --- creation (decision 4) -----------------------------------------------------------------


def _ticket_refusal(code: str) -> ApproverTicketRefusal:
    return ApproverTicketRefusal(422, f"approval_ticket_{code}")


def verify_creation(
    registration: StoredRegistration,
    *,
    agent: Agent,
    data: ApprovalRequest,
    original: codec.InvocationContextV1,
    clock: Callable[[], int],
) -> ApprovalTicket:
    """The pure creation checks (plan D8), in order; returns the verified ticket."""

    arguments = data.granted_arguments
    assert isinstance(arguments, dict)
    connector, tool = registration.pair
    try:
        check_execution_size(arguments, agent=agent.name, connector=connector, tool=tool)
        ticket = verify_ticket(
            public_key=VerifyKey(registration.ticket.public_key_bytes()),
            key_id=registration.ticket.key_id,
            arguments=arguments,
            clock=clock,
        )
    except TicketError as error:
        raise _ticket_refusal(error.code) from None
    scope = original.scope
    if (
        ticket.tenant_id != str(agent.tenant_id)
        or ticket.tenant_id != scope.tenant_id
        or ticket.agent_id != str(agent.id)
        or (ticket.connector, ticket.tool) != (connector, tool)
        or ticket.route != data.route
    ):
        raise _ticket_refusal("scope_mismatch")
    canonical = original.canonical
    requester = ticket.requester
    if (
        original.principal is None
        or canonical is None
        or (requester.tenant_id, requester.issuer, requester.subject)
        != (scope.tenant_id, canonical.issuer, canonical.subject)
    ):
        raise _ticket_refusal("requester_mismatch")
    accounts: set[tuple[str, str, str, str]] = set()
    for approver in ticket.approvers:
        namespaces: set[tuple[str, str]] = set()
        for account in approver.recipient_accounts or ():
            place = (account.installation_id, account.namespace.id)
            full = (account.provider, *place, account.account_id)
            if place in namespaces or full in accounts:
                raise _ticket_refusal("recipient_duplicate")
            namespaces.add(place)
            accounts.add(full)
    return ticket


async def check_selected_approvers(
    session: AsyncSession,
    registration: StoredRegistration,
    ticket: ApprovalTicket,
    *,
    tenant_id: uuid.UUID,
) -> None:
    """Each selected person is an active human inside the pinned ceiling (plan D9).

    Decided from the identity owner's canonical ``principals`` records, never
    from ticket content, inside the caller's identity fence. Only exact
    canonical identity proves the ceiling at creation: a ``ceiling.users``
    entry and an issuer/subject entry of the pinned role. Group and surface
    account entries never match, because nothing can establish them for a
    person who is not present (resolution B).
    """

    users = registration.ticket.ceiling.users
    allowed = None if users is None else {(user.issuer, user.subject) for user in users}
    subjects = {
        (entry.issuer, entry.subject)
        for entry in registration.pinned_role.filter.entries
        if isinstance(entry, IssuerSubjectSelectorV1)
    }
    for approver in ticket.approvers:
        principal = await session.scalar(
            select(Principal)
            .where(
                Principal.tenant_id == tenant_id,
                Principal.idp_issuer == approver.issuer,
                Principal.idp_subject == approver.subject,
            )
            .execution_options(populate_existing=True)
        )
        if principal is None or principal.type != "human" or principal.status != "active":
            raise ApproverTicketRefusal(422, APPROVER_INACTIVE)
        person = (approver.issuer, approver.subject)
        if (allowed is not None and person not in allowed) or person not in subjects:
            raise ApproverTicketRefusal(422, OUTSIDE_CEILING)


async def nonce_spent(session: AsyncSession, agent_id: uuid.UUID, nonce: str) -> bool:
    """Whether this agent already stored an approval spending ``nonce``."""

    found = await session.scalar(
        select(Approval.id).where(Approval.agent_id == agent_id, Approval.selection_nonce == nonce)
    )
    return found is not None


def selection_for(registration: StoredRegistration, ticket: ApprovalTicket) -> ApprovalSelection:
    """The immutable selection snapshot a dynamic approval stores."""

    return ApprovalSelection(
        requester=ticket.requester.model_dump(),
        connector=registration.ticket.connector,
        tool=registration.ticket.tool,
        key_id=registration.ticket.key_id,
        nonce=ticket.nonce,
        arguments_sha256=ticket.arguments_sha256,
        approvers=[approver.model_dump(exclude_none=True) for approver in ticket.approvers],
    )


def arguments_stable(stored: Any, requested: Any) -> bool:
    """The stored arguments canonicalize to the requested bytes (plan D12)."""

    try:
        return canonical_arguments(stored) == canonical_arguments(requested)
    except (ValueError, TypeError, RecursionError):
        return False


def creation_evidence(
    registration: StoredRegistration,
    ticket: ApprovalTicket,
) -> dict[str, Any]:
    """The created audit entry's selection evidence; never the ticket text."""

    return {
        "kind": "approver_ticket_selection",
        "route": registration.route,
        "key_id": registration.ticket.key_id,
        "connector": registration.ticket.connector,
        "tool": registration.ticket.tool,
        "nonce_sha256": hashlib.sha256(ticket.nonce.encode("ascii")).hexdigest(),
        "arguments_sha256": ticket.arguments_sha256,
        "approver_count": len(ticket.approvers),
    }


async def serving_versions(
    session: AsyncSession,
    agent_id: uuid.UUID,
) -> list[tuple[str, str | None]]:
    """The active deployment versions and their bundle objects, in a stable order."""

    versions = await list_active_deployment_versions(session, agent_id)
    return sorted((str(version.id), version.bundle_ref) for version in versions)
