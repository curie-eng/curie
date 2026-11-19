"""Learned rules: list them, and let a person who can merge decide them (#5797).

The list route sits on the memory API's own authorization (platform key or a
live console session). Accept, reject and remove take a principal console
session only, never the platform key: a rule applies only after a person
decides it, and for a repository rule only a person whose linked code host
account can write to that repository right now.

The write check reads the code host with no database transaction open, by the
repository's immutable id; the label stored on the rule is passed to the
adapter only as the credential hint an id read needs and never decides
identity. The decision itself is one transaction under the identity reader
fence and then the agent row lock, which serializes the per-scope cap; under
the fence it re-reads the exact link the provider check used, so a link
revoked while the code host was being read refuses the decision.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Coroutine, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, Literal

import httpx
from curie_internal.memory_rules import (
    MAX_ACCEPTED_PER_SCOPE,
    MAX_SNAPSHOT_BYTES,
    InvalidRule,
    InvalidTransition,
    RuleEvent,
    RuleState,
    canonical_text,
    next_state,
    repository_scope_key,
)
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from pydantic import ValidationError
from sqlalchemy import Select, func, select, text, tuple_
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from starlette.exceptions import HTTPException as StarletteHTTPException

from .. import identity_fence
from ..approval_auth import enforce_console_cookie_origin
from ..auth import require_api_key, require_principal_session
from ..config import get_settings
from ..deps import SessionDep
from ..forges import types as forge_types
from ..forges.capabilities import Operation, supports
from ..forges.errors import ForgeError, NotFound, Unavailable
from ..forges.registry import forges_registry
from ..forges.types import Actor, RepositoryRef
from ..models import Agent, IdentityLink, IdentityNamespace, MemoryRule, MemoryRuleEvent, Principal
from ..schemas.memory_rules import (
    ListedRuleState,
    MemoryRuleDecider,
    MemoryRuleDecision,
    MemoryRuleDecisionOut,
    MemoryRuleOut,
    MemoryRuleScopeOut,
    MemoryRulesOut,
    RuleScopeKind,
)
from .personal_enrollments import FOREIGN_CREDENTIAL_HEADERS, UNAUTHENTICATED

NO_STORE = {"Cache-Control": "no-store"}
LISTED_STATES: tuple[ListedRuleState, ...] = ("proposed", "accepted", "rejected", "removed")
# The ``identity_namespaces`` kind holding code host accounts by host.
GITHUB_HOST_KIND = "github_host"
# Code host kinds whose accounts have an identity namespace to link. Every
# other kind is unverifiable until it gets one.
_LINKABLE_KINDS = frozenset({forge_types.GITHUB})
_DECIDE = (
    "UPDATE curie.memory_rules SET state = :to_state, version = version + 1, "
    "{by} = :principal, {at} = now(), updated_at = now() "
    "WHERE id = :rule AND version = :expected RETURNING id"
)
# One fixed statement per event; no request value reaches a column name.
_DECIDE_STATEMENTS: dict[str, str] = {
    "accept": _DECIDE.format(by="accepted_by", at="accepted_at"),
    "reject": _DECIDE.format(by="rejected_by", at="rejected_at"),
    "remove": _DECIDE.format(by="removed_by", at="removed_at"),
}
_REFUSALS: dict[str, tuple[int, str, str]] = {
    "invalid_request": (
        422,
        "The request is not a valid learned rule request.",
        "Send only the documented query parameters, and a body of exactly "
        '{"expected_version": <integer of at least 1>}.',
    ),
    "not_found": (
        404,
        "No such agent or learned rule is visible to this session.",
        "List the agent's rules with `curie cluster memory <agent> --rules` and use an id "
        "from that list.",
    ),
    "scope_not_supported": (
        403,
        "Rules in agent or binding scope cannot be decided through this route yet.",
        "Only repository scope rules can be accepted, rejected or removed here.",
    ),
    "write_access_required": (
        403,
        "Your linked code host account cannot write to this rule's repository.",
        "Ask a repository admin for write access, or ask someone who can merge there to "
        "decide the rule.",
    ),
    "write_access_unverifiable": (
        403,
        "Write access to this rule's repository cannot be verified now.",
        "Only a rule on a configured GitHub code host, keyed by the repository's numeric "
        "id, can be decided; a rule whose repository moved or was replaced cannot.",
    ),
    "code_host_unavailable": (
        503,
        "The code host could not be reached to check write access.",
        "Retry in a moment; nothing was changed.",
    ),
    "stale_version": (
        409,
        "The rule changed since you read it.",
        "Read the rule again and retry with its current version.",
    ),
    "invalid_transition": (
        409,
        "The rule's state does not allow this action.",
        "Accept or reject only a proposed rule, and remove only an accepted one.",
    ),
    "scope_full": (
        409,
        "This scope already holds the maximum number of accepted rules.",
        "Remove an accepted rule in this scope before accepting another.",
    ),
    "snapshot_full": (
        409,
        "Accepting this rule would make the scope's rules too long to deliver.",
        "Remove an accepted rule in this scope, or reject this one, before accepting it.",
    ),
    "invalid_rule": (
        409,
        "The stored rule text cannot be delivered as a learned rule.",
        "Reject this rule; it cannot be accepted.",
    ),
    "unavailable": (
        503,
        "The rule store could not be reached.",
        "Retry in a moment; nothing was changed.",
    ),
}
_NO_LINK_FIX = (
    "Link your code host account first: an operator links it with "
    "POST /identity/admin/{tenant_id}/code-host-links."
)


def _refusal(
    code: str, *, message: str | None = None, fix: str | None = None, **extra: int
) -> HTTPException:
    status, default_message, default_fix = _REFUSALS[code]
    detail: dict[str, Any] = {
        "code": f"memory_rules.{code}",
        "message": message or default_message,
        "fix": fix or default_fix,
        **extra,
    }
    return HTTPException(status, detail, headers=NO_STORE)


class MemoryRulesRoute(APIRoute):
    """Every answer, refusals included, is ``Cache-Control: no-store``; a
    malformed request is one coded 422."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def no_store(request: Request) -> Response:
            try:
                response = await handler(request)
            except (RequestValidationError, ValidationError):
                raise _refusal("invalid_request") from None
            except StarletteHTTPException as error:
                error.headers = {**(error.headers or {}), **NO_STORE}
                raise
            response.headers.update(NO_STORE)
            return response

        return no_store


async def _session_only(request: Request) -> None:
    if any(name in request.headers for name in (*FOREIGN_CREDENTIAL_HEADERS, "Authorization")):
        raise HTTPException(401, UNAUTHENTICATED, headers=NO_STORE)


def _same_origin(request: Request) -> None:
    enforce_console_cookie_origin(request)


DeciderDep = Annotated[Principal, Depends(require_principal_session)]
SameOriginDep = Annotated[None, Depends(_same_origin)]

router = APIRouter(
    prefix="/agents",
    tags=["memory"],
    route_class=MemoryRulesRoute,
    dependencies=[Depends(require_api_key)],
)
decision_router = APIRouter(
    prefix="/agents",
    tags=["memory"],
    route_class=MemoryRulesRoute,
    dependencies=[Depends(_session_only)],
)


# --- reading rules -------------------------------------------------------------------


def _decider(principal_id: uuid.UUID | None, name: str | None) -> MemoryRuleDecider | None:
    return (
        None
        if principal_id is None
        else MemoryRuleDecider(principal_id=principal_id, display_name=name)
    )


def _rule_query(
    agent_id: uuid.UUID,
) -> Select[tuple[MemoryRule, str | None, str | None, str | None]]:
    accepted = aliased(Principal)
    rejected = aliased(Principal)
    removed = aliased(Principal)
    return (
        select(MemoryRule, accepted.display_name, rejected.display_name, removed.display_name)
        .outerjoin(accepted, accepted.id == MemoryRule.accepted_by)
        .outerjoin(rejected, rejected.id == MemoryRule.rejected_by)
        .outerjoin(removed, removed.id == MemoryRule.removed_by)
        .where(MemoryRule.agent_id == agent_id, MemoryRule.state.in_(LISTED_STATES))
        .order_by(MemoryRule.scope_kind, MemoryRule.scope_key, MemoryRule.number)
        .execution_options(populate_existing=True)
    )


def _item(
    rule: MemoryRule, accepted: str | None, rejected: str | None, removed: str | None
) -> MemoryRuleOut:
    assert rule.number is not None  # every listed state carries a number
    return MemoryRuleOut(
        id=rule.id,
        number=rule.number,
        label=f"R{rule.number}",
        scope_kind=rule.scope_kind,  # type: ignore[arg-type]
        scope_key=rule.scope_key,
        scope_label=rule.scope_label,
        state=rule.state,  # type: ignore[arg-type]
        text=rule.text,
        reason=rule.reason,
        source_url=rule.source_url,
        proposed_by=rule.proposed_by,
        proposed_at=rule.proposed_at,
        accepted_by=_decider(rule.accepted_by, accepted),
        accepted_at=rule.accepted_at,
        rejected_by=_decider(rule.rejected_by, rejected),
        rejected_at=rule.rejected_at,
        removed_by=_decider(rule.removed_by, removed),
        removed_at=rule.removed_at,
        version=rule.version,
    )


async def _items(session: AsyncSession, query: Select[Any]) -> list[MemoryRuleOut]:
    return [_item(*row) for row in (await session.execute(query)).tuples().all()]


async def _in_force_counts(
    session: AsyncSession, agent_id: uuid.UUID, scopes: Sequence[tuple[str, str]]
) -> dict[tuple[str, str], int]:
    if not scopes:
        return {}
    rows = await session.execute(
        select(MemoryRule.scope_kind, MemoryRule.scope_key, func.count())
        .where(
            MemoryRule.agent_id == agent_id,
            MemoryRule.state == "accepted",
            tuple_(MemoryRule.scope_kind, MemoryRule.scope_key).in_(list(scopes)),
        )
        .group_by(MemoryRule.scope_kind, MemoryRule.scope_key)
    )
    return {(kind, key): int(count) for kind, key, count in rows.tuples().all()}


@router.get("/{agent_id}/memory/rules", response_model=MemoryRulesOut)
async def list_rules(
    agent_id: uuid.UUID,
    session: SessionDep,
    state: Annotated[ListedRuleState | None, Query()] = None,
    scope_kind: Annotated[RuleScopeKind | None, Query()] = None,
    scope_key: Annotated[str | None, Query(max_length=1024)] = None,
) -> MemoryRulesOut:
    """The agent's proposed, accepted, rejected and removed rules, by scope and number."""

    try:
        if await session.scalar(select(Agent.id).where(Agent.id == agent_id)) is None:
            raise _refusal("not_found")
        query = _rule_query(agent_id)
        if state is not None:
            query = query.where(MemoryRule.state == state)
        if scope_kind is not None:
            query = query.where(MemoryRule.scope_kind == scope_kind)
        if scope_key is not None:
            query = query.where(MemoryRule.scope_key == scope_key)
        rules = await _items(session, query)
        labels: dict[tuple[str, str], str] = {}
        for rule in rules:
            labels.setdefault((rule.scope_kind, rule.scope_key), rule.scope_label)
        counts = await _in_force_counts(session, agent_id, list(labels))
    except DBAPIError:
        await session.rollback()
        raise _refusal("unavailable") from None
    return MemoryRulesOut(
        rules=rules,
        scopes=[
            MemoryRuleScopeOut(
                scope_kind=kind,  # type: ignore[arg-type]
                scope_key=key,
                scope_label=label,
                in_force_count=counts.get((kind, key), 0),
            )
            for (kind, key), label in labels.items()
        ],
        in_force_limit=MAX_ACCEPTED_PER_SCOPE,
    )


# --- deciding rules ------------------------------------------------------------------


@dataclass(frozen=True)
class _Scope:
    """A repository scope parsed from its key, never from the stored label."""

    kind: str
    host: str
    project_id: str
    label: str


def _parse_scope(scope_key: str, label: str) -> _Scope | None:
    """``kind:host:project_id`` with a numeric immutable id, or None."""

    kind, _, rest = scope_key.partition(":")
    host, _, project_id = rest.rpartition(":")
    try:
        if repository_scope_key(kind, host, project_id) != scope_key:
            return None
    except InvalidRule:
        return None
    if kind not in _LINKABLE_KINDS:
        return None
    if not (project_id.isascii() and project_id.isdigit()):
        # The GitHub adapter's ``path:`` form names a repository by a mutable path.
        return None
    return _Scope(kind, host, project_id, label)


@dataclass(frozen=True)
class _Link:
    """One identity link and the code host account id it names."""

    id: uuid.UUID
    account_id: str


async def _linked_accounts(
    session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID, scope: _Scope
) -> list[_Link]:
    """The principal's active links in the namespace of the scope's host."""

    host = scope.host.lower()
    rows = await session.execute(
        select(IdentityLink.id, IdentityLink.provider_native_id)
        .join(IdentityNamespace, IdentityNamespace.id == IdentityLink.identity_namespace_id)
        .where(
            IdentityNamespace.tenant_id == tenant_id,
            IdentityNamespace.provider == scope.kind,
            IdentityNamespace.kind == GITHUB_HOST_KIND,
            IdentityNamespace.authority == host,
            IdentityNamespace.key == host,
            IdentityNamespace.status == "active",
            IdentityLink.tenant_id == tenant_id,
            IdentityLink.principal_id == principal_id,
            IdentityLink.revoked_at.is_(None),
        )
        .order_by(IdentityLink.id)
    )
    return [_Link(link_id, str(account)) for link_id, account in rows.tuples().all()]


async def _write_refusal(
    client: httpx.AsyncClient, scope: _Scope, account_id: str
) -> HTTPException | None:
    """Read the code host now: None when ``account_id`` can write to the
    scope's repository, else the refusal. No database transaction is open."""

    try:
        connection = forges_registry(get_settings()).code_host_for(scope.kind, scope.host)
    except Unavailable:
        return _refusal("write_access_unverifiable")
    try:
        try:
            hint = RepositoryRef(scope.kind, scope.host, scope.project_id, scope.label)
        except ValueError:
            return _refusal("write_access_unverifiable")
        code_host = connection.adapter(client, repository=hint)
        if not (
            supports(code_host.capabilities, Operation.ACCOUNT_LOGIN)
            and supports(code_host.capabilities, Operation.USER_CAN_WRITE)
        ):
            return _refusal("write_access_unverifiable")
        try:
            resolved = await code_host.resolve_repository(scope.project_id)
        except NotFound:
            # Gone, or the id now answers for another repository.
            return _refusal("write_access_unverifiable")
        if (resolved.kind, resolved.host, resolved.project_id) != (
            scope.kind,
            scope.host,
            scope.project_id,
        ):
            return _refusal("write_access_unverifiable")
        login = await code_host.account_login(resolved, account_id)
        if login is None:
            return _refusal("write_access_required")
        if not await code_host.user_can_write(resolved, Actor(account_id, login)):
            return _refusal("write_access_required")
    except (ForgeError, httpx.HTTPError):
        return _refusal("code_host_unavailable")
    return None


@dataclass(frozen=True)
class _Checked:
    """What the unlocked reads established, compared again under the lock."""

    tenant_id: uuid.UUID
    principal_id: uuid.UUID
    scope_kind: str
    scope_key: str
    scope: _Scope
    # The exact link whose account the code host said can write.
    link: _Link


async def _check(
    session: AsyncSession,
    client: httpx.AsyncClient,
    principal: Principal,
    agent_id: uuid.UUID,
    rule_id: uuid.UUID,
) -> _Checked:
    """Ownership, scope and write access, read before any lock is taken."""

    tenant_id, principal_id = principal.tenant_id, principal.id
    agent_tenant = await session.scalar(select(Agent.tenant_id).where(Agent.id == agent_id))
    if agent_tenant is None or agent_tenant != tenant_id:
        raise _refusal("not_found")
    found = (
        await session.execute(
            # Any state: one that allows no action is 409 invalid_transition, not 404.
            select(MemoryRule.scope_kind, MemoryRule.scope_key, MemoryRule.scope_label).where(
                MemoryRule.id == rule_id,
                MemoryRule.agent_id == agent_id,
                MemoryRule.tenant_id == tenant_id,
            )
        )
    ).one_or_none()
    if found is None:
        raise _refusal("not_found")
    scope_kind, scope_key, scope_label = found
    if scope_kind != "repository":
        raise _refusal("scope_not_supported")
    scope = _parse_scope(scope_key, scope_label)
    if scope is None:
        raise _refusal("write_access_unverifiable")
    links = await _linked_accounts(session, tenant_id, principal_id, scope)
    if not links:
        raise _refusal(
            "write_access_required",
            message="No code host account on this repository's host is linked to you.",
            fix=_NO_LINK_FIX,
        )
    if len(links) > 1:
        raise _refusal("write_access_unverifiable")
    (link,) = links
    # End the read transaction: no snapshot or lock is held across provider reads.
    await session.rollback()
    refusal = await _write_refusal(client, scope, link.account_id)
    if refusal is not None:
        raise refusal
    return _Checked(tenant_id, principal_id, scope_kind, scope_key, scope, link)


async def _snapshot_refusal(
    session: AsyncSession, agent_id: uuid.UUID, rule: MemoryRule
) -> HTTPException | None:
    """The cap and the snapshot byte bound of the rule's scope, with it accepted."""

    accepted = (
        (
            await session.execute(
                select(MemoryRule.number, MemoryRule.text)
                .where(
                    MemoryRule.agent_id == agent_id,
                    MemoryRule.scope_kind == rule.scope_kind,
                    MemoryRule.scope_key == rule.scope_key,
                    MemoryRule.state == "accepted",
                )
                .order_by(MemoryRule.accepted_at, MemoryRule.id)
            )
        )
        .tuples()
        .all()
    )
    if len(accepted) >= MAX_ACCEPTED_PER_SCOPE:
        return _refusal("scope_full", limit=MAX_ACCEPTED_PER_SCOPE)
    assert rule.number is not None
    numbered = [(number, body) for number, body in accepted if number is not None]
    try:
        document = canonical_text([*numbered, (rule.number, rule.text)])
    except InvalidRule:
        return _refusal("invalid_rule")
    if len(document.encode("utf-8")) > MAX_SNAPSHOT_BYTES:
        return _refusal("snapshot_full", limit_bytes=MAX_SNAPSHOT_BYTES)
    return None


async def _apply(
    session: AsyncSession,
    checked: _Checked,
    agent_id: uuid.UUID,
    rule_id: uuid.UUID,
    event: RuleEvent,
    expected_version: int,
) -> MemoryRuleOut:
    """One transaction under the identity reader fence and the agent row lock;
    raises a refusal on any check."""

    # The fence first, as every identity reader takes it before its row locks:
    # an identity writer holds it exclusively before its own, so the order
    # never inverts. Until commit no link can be revoked or changed.
    await identity_fence.lock_identity_reader(session)
    current = await _linked_accounts(
        session, checked.tenant_id, checked.principal_id, checked.scope
    )
    if current != [checked.link]:
        raise _refusal("write_access_required")
    agent_tenant = await session.scalar(
        select(Agent.tenant_id).where(Agent.id == agent_id).with_for_update()
    )
    rule = await session.scalar(
        select(MemoryRule)
        .where(
            MemoryRule.id == rule_id,
            MemoryRule.agent_id == agent_id,
            MemoryRule.tenant_id == checked.tenant_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if agent_tenant != checked.tenant_id or rule is None:
        raise _refusal("not_found")
    if (rule.scope_kind, rule.scope_key) != (checked.scope_kind, checked.scope_key):
        raise _refusal("write_access_unverifiable")
    if rule.version != expected_version:
        raise _refusal("stale_version", current_version=rule.version)
    from_state: RuleState = rule.state  # type: ignore[assignment]
    try:
        to_state = next_state(from_state, event)
    except InvalidTransition:
        raise _refusal("invalid_transition") from None
    if event == "accept":
        refusal = await _snapshot_refusal(session, agent_id, rule)
        if refusal is not None:
            raise refusal
    updated = await session.execute(
        text(_DECIDE_STATEMENTS[event]),
        {
            "to_state": to_state,
            "principal": checked.principal_id,
            "rule": rule_id,
            "expected": expected_version,
        },
    )
    if updated.scalar_one_or_none() is None:
        raise _refusal("stale_version", current_version=rule.version)
    session.add(
        MemoryRuleEvent(
            rule_id=rule_id,
            tenant_id=checked.tenant_id,
            agent_id=agent_id,
            event=event,
            from_state=from_state,
            to_state=to_state,
            actor_kind="principal",
            actor=str(checked.principal_id),
            principal_id=checked.principal_id,
        )
    )
    await session.flush()
    # Every state a decision reaches is a listed, numbered one.
    (item,) = await _items(session, _rule_query(agent_id).where(MemoryRule.id == rule_id))
    await session.commit()
    return item


async def _decide(
    request: Request,
    session: AsyncSession,
    principal: Principal,
    agent_id: uuid.UUID,
    rule_id: uuid.UUID,
    event: Literal["accept", "reject", "remove"],
    body: MemoryRuleDecision,
) -> MemoryRuleDecisionOut:
    client: httpx.AsyncClient = request.app.state.http_client
    try:
        checked = await _check(session, client, principal, agent_id, rule_id)
        item = await _apply(session, checked, agent_id, rule_id, event, body.expected_version)
    except HTTPException:
        await session.rollback()
        raise
    except DBAPIError:
        await session.rollback()
        raise _refusal("unavailable") from None
    return MemoryRuleDecisionOut(rule=item, applies_from="next_ticket")


@decision_router.post(
    "/{agent_id}/memory/rules/{rule_id}/accept", response_model=MemoryRuleDecisionOut
)
async def accept_rule(
    request: Request,
    agent_id: uuid.UUID,
    rule_id: uuid.UUID,
    body: MemoryRuleDecision,
    principal: DeciderDep,
    _origin: SameOriginDep,
    session: SessionDep,
) -> MemoryRuleDecisionOut:
    """Accept a proposed rule; it applies from the next ticket's admission."""

    return await _decide(request, session, principal, agent_id, rule_id, "accept", body)


@decision_router.post(
    "/{agent_id}/memory/rules/{rule_id}/reject", response_model=MemoryRuleDecisionOut
)
async def reject_rule(
    request: Request,
    agent_id: uuid.UUID,
    rule_id: uuid.UUID,
    body: MemoryRuleDecision,
    principal: DeciderDep,
    _origin: SameOriginDep,
    session: SessionDep,
) -> MemoryRuleDecisionOut:
    """Reject a proposed rule; it never applies."""

    return await _decide(request, session, principal, agent_id, rule_id, "reject", body)


@decision_router.post(
    "/{agent_id}/memory/rules/{rule_id}/remove", response_model=MemoryRuleDecisionOut
)
async def remove_rule(
    request: Request,
    agent_id: uuid.UUID,
    rule_id: uuid.UUID,
    body: MemoryRuleDecision,
    principal: DeciderDep,
    _origin: SameOriginDep,
    session: SessionDep,
) -> MemoryRuleDecisionOut:
    """Remove an accepted rule; the next ticket's admission no longer carries it."""

    return await _decide(request, session, principal, agent_id, rule_id, "remove", body)
