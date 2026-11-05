"""Shared seeding and assertions for the personal execution owner suites (#5376).

Imported by name: ``conftest.py`` puts this directory on ``sys.path`` and the
suite runs under ``--import-mode=importlib`` (see ``_migration_support``).

A personal WorkItem's owner tuple is ``(tenant_id, model_auth_mode,
owner_principal_id, enrollment_generation)``. It is written once by the trusted
internal admission route, snapshotted onto every ExecutionRequest, and only the
versioned revalidation operation moves its generation. These helpers seed the
tenants, principals and ``curie.personal_claude_enrollments`` rows those suites
need with raw SQL, read the durable tuple back through a fresh engine for every
assertion, and project it through a newly constructed worker resolver.

Everything seeded carries ``MARK`` so ``cleanup_owned`` removes exactly it.
Placeholder identifiers only.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn
from curie_api import workitem_dispatch
from curie_api.config import get_settings
from curie_api.db import create_sessionmaker
from curie_api.forges.types import GITHUB, RepositoryRef, TrackerIssueRef
from curie_api.workitem_dispatch import AcquireGrant, Admission, StartResult, TerminationClaim
from curie_api.workitems import lifecycle
from curie_api.workitems.lifecycle import ExecutionRequestSnapshot, WorkItemOutcome
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from forge_fakes import factory_wake, no_external_calls
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

MARK = "test-personal-execution-"
INTERNAL_PREFIX = "/v1/internal/work-items"
REPO = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
FOREIGN_ADDRESS = "C0EXAMPLE9"
WIRE_CONVERSATION = "1700000000.000100"
OBJECTIVE = "Implement the admitted work item"
REQUESTER = "U0REQUEST1"
OTHER_REQUESTER = "U0REQUEST2"
OWNER_NAME = "Acme Owner"
OTHER_NAME = "Acme Reviewer"
RUNTIME_OWNER = "curie-workers-a"
OTHER_RUNTIME_OWNER = "curie-workers-b"
CLAIM_NAME = "curie-thread-personal-claim"
SANDBOX_NAME = "sbx-curie-thread-personal-claim"
TERMINATION_OBSERVATION = (
    "claims=curie-thread-personal-claim sandboxes=sbx-curie-thread-personal-claim "
    "absent_at=2026-09-18T12:00:10+00:00 observer=curie-workers-b"
)
TOOL = "mcp__acme__deploy_preview"
APPROVAL_PRINCIPAL_HEADER = "X-Curie-Approval-Principal"
FORBIDDEN_FRAGMENTS = ("token", "secret", "grant", "credential")
# Mirrors conftest's ``_truncate``: everything that references an agent, so the
# owned tenants and principals can be removed afterwards.
_TRUNCATED = (
    "curie.execution_requests",
    "curie.work_items",
    "curie.deploy_notice_outbox",
    "curie.approvals",
    "curie.deployments",
    "curie.agent_versions",
    "curie.agents",
    "curie.console_sessions",
    "curie.github_review_deliveries",
    "curie.factory_poll_cursors",
    "curie.personal_claude_enrollments",
)
_TUPLE = "tenant_id, model_auth_mode, owner_principal_id, enrollment_generation"

OwnerTuple = tuple[UUID | None, str, UUID | None, int | None]


# --- database access ---------------------------------------------------------------


def with_session[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    """Run ``body`` in its own session on a freshly created engine.

    The session comes from the application's own sessionmaker, so it keeps
    loaded rows across a commit exactly as a production session does.
    """

    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with create_sessionmaker(engine)() as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


def rows(statement: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    """Run one statement on a fresh engine, commit, and return its rows."""

    async def body(session: AsyncSession) -> list[dict[str, Any]]:
        result = await session.execute(text(statement), dict(params or {}))
        found = [dict(row) for row in result.mappings().all()] if result.returns_rows else []
        await session.commit()
        return found

    return with_session(body)


# --- owned world -------------------------------------------------------------------


@dataclass(frozen=True)
class World:
    """One active tenant with an enrolled owner, and a second tenant with its own agent."""

    tenant_id: UUID
    other_tenant_id: UUID
    owner_id: UUID
    other_principal_id: UUID
    enrollment_id: UUID
    generation: int
    foreign_agent_id: UUID

    def owner(self, *, generation: int | None = None) -> dict[str, Any]:
        """The admission body's ``owner`` context for the enrolled owner."""

        return {
            "tenant_id": str(self.tenant_id),
            "principal_id": str(self.owner_id),
            "enrollment_generation": self.generation if generation is None else generation,
        }

    def personal(self, generation: int | None = None) -> OwnerTuple:
        return (
            self.tenant_id,
            "personal",
            self.owner_id,
            self.generation if generation is None else generation,
        )

    def service(self) -> OwnerTuple:
        return (self.tenant_id, "service", None, None)


async def _insert_tenant(session: AsyncSession) -> UUID:
    tenant_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.tenants (id, deployment_id, status) "
            "VALUES (:id, :deployment, 'active')"
        ),
        {"id": tenant_id, "deployment": f"{MARK}{tenant_id}"},
    )
    return tenant_id


async def _insert_principal(session: AsyncSession, tenant_id: UUID, *, display_name: str) -> UUID:
    principal_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status, "
            "display_name) VALUES (:id, :tenant, :subject, 'human', 'active', :name)"
        ),
        {
            "id": principal_id,
            "tenant": tenant_id,
            "subject": f"{MARK}{principal_id}",
            "name": display_name,
        },
    )
    return principal_id


async def _insert_enrollment(
    session: AsyncSession,
    tenant_id: UUID,
    principal_id: UUID,
    *,
    generation: int,
    active: bool = True,
) -> UUID:
    enrollment_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.personal_claude_enrollments "
            "(id, tenant_id, principal_id, generation, active, version) "
            "VALUES (:id, :tenant, :principal, :generation, :active, 1)"
        ),
        {
            "id": enrollment_id,
            "tenant": tenant_id,
            "principal": principal_id,
            "generation": generation,
            "active": active,
        },
    )
    return enrollment_id


async def _insert_agent(session: AsyncSession, tenant_id: UUID, *, address: str) -> UUID:
    agent_id = uuid.uuid4()
    await session.execute(
        text(
            "INSERT INTO curie.agents (id, tenant_id, name, repo_full_name) "
            "VALUES (:id, :tenant, :name, :repo)"
        ),
        {
            "id": agent_id,
            "tenant": tenant_id,
            "name": f"acme-personal-{agent_id.hex[:8]}",
            "repo": REPO,
        },
    )
    await session.execute(
        text(
            "INSERT INTO curie.agent_channels (id, agent_id, tenant_id, kind, address, adapter) "
            "VALUES (:id, :agent, :tenant, 'slack', :address, 'default')"
        ),
        {"id": uuid.uuid4(), "agent": agent_id, "tenant": tenant_id, "address": address},
    )
    return agent_id


def seed_world(*, generation: int = 1) -> World:
    """Seed the owned tenants, principals, enrollment and foreign agent in one commit."""

    async def body(session: AsyncSession) -> World:
        tenant_id = await _insert_tenant(session)
        other_tenant_id = await _insert_tenant(session)
        owner_id = await _insert_principal(session, tenant_id, display_name=OWNER_NAME)
        other_id = await _insert_principal(session, tenant_id, display_name=OTHER_NAME)
        enrollment_id = await _insert_enrollment(
            session, tenant_id, owner_id, generation=generation
        )
        foreign_agent_id = await _insert_agent(session, other_tenant_id, address=FOREIGN_ADDRESS)
        await session.commit()
        return World(
            tenant_id=tenant_id,
            other_tenant_id=other_tenant_id,
            owner_id=owner_id,
            other_principal_id=other_id,
            enrollment_id=enrollment_id,
            generation=generation,
            foreign_agent_id=foreign_agent_id,
        )

    return with_session(body)


def cleanup_owned() -> int:
    """Remove every owned tenant, principal and enrollment; return what survived.

    Agents, work items and approvals reference the owned tenants and principals,
    so the same tables conftest truncates go first.
    """

    async def body(session: AsyncSession) -> int:
        mark = {"mark": f"{MARK}%"}
        owned = "SELECT id FROM curie.tenants WHERE deployment_id LIKE :mark"
        await session.execute(text(f"TRUNCATE {', '.join(_TRUNCATED)} CASCADE"))
        await session.execute(
            text(f"DELETE FROM curie.personal_claude_enrollments WHERE tenant_id IN ({owned})"),
            mark,
        )
        await session.execute(
            text(f"DELETE FROM curie.principals WHERE tenant_id IN ({owned})"), mark
        )
        await session.execute(
            text("DELETE FROM curie.tenants WHERE deployment_id LIKE :mark"), mark
        )
        await session.commit()
        remaining = await session.scalar(
            text(
                "SELECT (SELECT count(*) FROM curie.tenants WHERE deployment_id LIKE :mark) "
                "+ (SELECT count(*) FROM curie.principals WHERE idp_subject LIKE :mark)"
            ),
            mark,
        )
        return int(remaining or 0)

    return with_session(body)


def insert_agent(tenant_id: UUID, *, address: str = ADDRESS) -> UUID:
    """An agent of ``tenant_id`` bound to one Slack route, written with raw SQL."""

    async def body(session: AsyncSession) -> UUID:
        agent_id = await _insert_agent(session, tenant_id, address=address)
        await session.commit()
        return agent_id

    return with_session(body)


def move_agent_to_tenant(agent_id: UUID, tenant_id: UUID) -> None:
    """Re-home an agent the API created, with every row the write path scopes."""

    async def body(session: AsyncSession) -> None:
        for table, column in (
            ("agents", "id"),
            ("agent_channels", "agent_id"),
            ("agent_versions", "agent_id"),
            ("deployments", "agent_id"),
        ):
            await session.execute(
                text(f"UPDATE curie.{table} SET tenant_id = :tenant WHERE {column} = :agent"),
                {"tenant": tenant_id, "agent": agent_id},
            )
        await session.commit()

    with_session(body)


# --- enrollment, standing in for #5377 ----------------------------------------------


def enrollment_row(world: World) -> dict[str, Any] | None:
    found = rows(
        "SELECT id, generation, active, version FROM curie.personal_claude_enrollments "
        "WHERE tenant_id = :tenant AND principal_id = :principal",
        {"tenant": world.tenant_id, "principal": world.owner_id},
    )
    return found[0] if found else None


def advance_enrollment(world: World) -> tuple[int, int]:
    """Replace the owner's subscription: the generation and CAS version advance."""

    [row] = rows(
        "UPDATE curie.personal_claude_enrollments "
        "SET generation = generation + 1, version = version + 1, updated_at = now() "
        "WHERE tenant_id = :tenant AND principal_id = :principal "
        "RETURNING generation, version",
        {"tenant": world.tenant_id, "principal": world.owner_id},
    )
    return int(row["generation"]), int(row["version"])


def set_enrollment_active(world: World, active: bool) -> int:
    [row] = rows(
        "UPDATE curie.personal_claude_enrollments "
        "SET active = :active, version = version + 1, updated_at = now() "
        "WHERE tenant_id = :tenant AND principal_id = :principal RETURNING version",
        {"active": active, "tenant": world.tenant_id, "principal": world.owner_id},
    )
    return int(row["version"])


def delete_enrollment(world: World) -> None:
    rows(
        "DELETE FROM curie.personal_claude_enrollments "
        "WHERE tenant_id = :tenant AND principal_id = :principal",
        {"tenant": world.tenant_id, "principal": world.owner_id},
    )


def insert_enrollment(world: World, *, generation: int) -> None:
    async def body(session: AsyncSession) -> None:
        await _insert_enrollment(session, world.tenant_id, world.owner_id, generation=generation)
        await session.commit()

    with_session(body)


# --- admission facts and the internal routes -----------------------------------------


@dataclass(frozen=True)
class Admitted:
    work_item_id: UUID
    request_id: UUID
    facts: Admission


def configure_api(monkeypatch: pytest.MonkeyPatch, *, worker_token: str) -> None:
    """The environment the internal work-item suites build ``create_app()`` under."""

    for name, value in {
        "INTERNAL_WORKER_TOKEN": worker_token,
        "RESUME_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
    }.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()


def facts(
    agent_id: UUID,
    *,
    issue_number: int,
    conversation: str = WIRE_CONVERSATION,
    address: str = ADDRESS,
    request_id: UUID | None = None,
    objective: str = OBJECTIVE,
    requester: str = REQUESTER,
) -> Admission:
    """Service-default admission facts, the shape every lifecycle builder passes."""

    return Admission(
        agent_id=agent_id,
        kind="slack",
        address=address,
        reply_conversation_id=conversation,
        issue=TrackerIssueRef(GITHUB, "github.com", "101", str(issue_number)),
        repository=RepositoryRef(GITHUB, "github.com", "101", REPO),
        code_host_installation_id=202,
        objective=objective,
        requester=requester,
        request_id=request_id or uuid.uuid4(),
    )


def facts_json(admission: Admission) -> dict[str, Any]:
    return {
        "agent_id": str(admission.agent_id),
        "kind": admission.kind,
        "address": admission.address,
        "reply_conversation_id": admission.reply_conversation_id,
        "tracker": {
            "kind": admission.issue.kind,
            "host": admission.issue.host,
            "scope_id": admission.issue.scope_id,
            "issue_id": admission.issue.issue_id,
        },
        "repository": {
            "code_host_kind": admission.repository.kind,
            "host": admission.repository.host,
            "project_id": admission.repository.project_id,
            "path": admission.repository.path,
        },
        "code_host_installation_id": admission.code_host_installation_id,
        "objective": admission.objective,
        "requester": admission.requester,
        "request_id": str(admission.request_id),
    }


def _admit(
    client: TestClient,
    headers: Mapping[str, str],
    admission: Admission,
    *,
    extra: Mapping[str, Any],
) -> Admitted:
    response = client.post(
        f"{INTERNAL_PREFIX}/admissions",
        json={**facts_json(admission), **extra},
        headers=dict(headers),
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["replayed"] is False, body
    assert body["request"]["id"] == str(admission.request_id), body
    return Admitted(
        work_item_id=UUID(body["work_item"]["id"]),
        request_id=admission.request_id,
        facts=admission,
    )


def admit_personal(
    client: TestClient,
    headers: Mapping[str, str],
    world: World,
    agent_id: UUID,
    *,
    issue_number: int,
    conversation: str = WIRE_CONVERSATION,
    requester: str = REQUESTER,
) -> Admitted:
    """The trusted owner-bearing admission: the only path that names an owner."""

    admission = facts(
        agent_id, issue_number=issue_number, conversation=conversation, requester=requester
    )
    return _admit(
        client,
        headers,
        admission,
        extra={"model_auth_mode": "personal", "owner": world.owner()},
    )


def admit_service(
    client: TestClient,
    headers: Mapping[str, str],
    agent_id: UUID,
    *,
    issue_number: int,
    conversation: str,
) -> Admitted:
    admission = facts(agent_id, issue_number=issue_number, conversation=conversation)
    return _admit(client, headers, admission, extra={})


def start_request(request_id: UUID, *, owner: str = RUNTIME_OWNER) -> int:
    """Acquire and start through the functions the internal routes call."""

    async def body(session: AsyncSession) -> int:
        generation = await session.scalar(
            text("SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id"),
            {"id": request_id},
        )
        assert isinstance(generation, int), request_id
        async with no_external_calls() as http_client:
            acquired = await workitem_dispatch.acquire(
                session,
                request_id,
                owner=owner,
                generation=generation,
                wake=await factory_wake(session, request_id),
                context=None,
                client=http_client,
            )
        assert isinstance(acquired, AcquireGrant), acquired
        started = await workitem_dispatch.start(
            session,
            request_id,
            owner=owner,
            generation=generation,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
            context=acquired.context,
        )
        assert isinstance(started, StartResult), started
        return started.runtime_epoch

    return with_session(body)


def finish_failed(request_id: UUID, runtime_epoch: int, *, cause: str = "deadline_halted") -> None:
    """End a running request through the worker's finish path."""

    async def body(session: AsyncSession) -> None:
        finished = await workitem_dispatch.finish(
            session,
            request_id,
            runtime_epoch=runtime_epoch,
            outcome="failed",
            cause=cause,
            detail=None,
            ci_fix_round=None,
        )
        assert isinstance(finished, WorkItemOutcome), finished
        assert finished.request is not None and finished.request.status == "failed"

    with_session(body)


def relabel_while_live(
    agent_id: UUID, *, issue_number: int, live_request_id: UUID, objective: str
) -> Admission:
    """Another requester relabels the issue while its run is live.

    The live request is asked to stop and the relabel is stored on the WorkItem
    until that request reaches a terminus.
    """

    relabel = facts(
        agent_id, issue_number=issue_number, requester=OTHER_REQUESTER, objective=objective
    )
    stopping = with_session(lambda session: workitem_dispatch.readmit(session, relabel))
    assert isinstance(stopping, WorkItemOutcome), stopping
    assert stopping.request is not None
    assert (stopping.request.id, stopping.request.status) == (
        live_request_id,
        "cancellation_requested",
    )
    return relabel


def record_termination(request_id: UUID, *, owner: str = RUNTIME_OWNER) -> ExecutionRequestSnapshot:
    """End a stopping request through the worker's termination chain.

    ``claim_termination``, then ``record_termination`` with an observation that
    the run's claim and sandbox are gone: the request becomes terminal carrying
    that observation, the evidence that its runtime stopped.
    """

    claimed = with_session(
        lambda session: workitem_dispatch.claim_termination(session, request_id, owner=owner)
    )
    assert isinstance(claimed, TerminationClaim), claimed
    runtime_epoch = claimed.runtime_epoch
    ended = with_session(
        lambda session: workitem_dispatch.record_termination(
            session,
            request_id,
            runtime_epoch=runtime_epoch,
            observation=TERMINATION_OBSERVATION,
        )
    )
    assert isinstance(ended, WorkItemOutcome), ended
    assert ended.request is not None and ended.request.id == request_id, ended
    assert ended.request.terminal_at is not None, ended
    assert ended.request.termination_observation == TERMINATION_OBSERVATION, ended
    return ended.request


def admit_pending_readmit(work_item_id: UUID) -> WorkItemOutcome:
    """Create the request a relabel stored while its predecessor was stopping."""

    async def body(session: AsyncSession) -> object:
        now = await lifecycle.database_now(session)
        return await lifecycle.admit_pending_readmit(
            session, work_item_id=work_item_id, wait_deadline=now + timedelta(hours=1)
        )

    admitted = with_session(body)
    assert isinstance(admitted, WorkItemOutcome), admitted
    assert admitted.request is not None, admitted
    return admitted


# --- the durable owner tuple ---------------------------------------------------------


def _tuple(row: Mapping[str, Any]) -> OwnerTuple:
    generation = row["enrollment_generation"]
    return (
        row["tenant_id"],
        row["model_auth_mode"],
        row["owner_principal_id"],
        None if generation is None else int(generation),
    )


def work_item_tuple(work_item_id: UUID) -> OwnerTuple:
    [row] = rows(f"SELECT {_TUPLE} FROM curie.work_items WHERE id = :id", {"id": work_item_id})
    return _tuple(row)


def request_tuples(work_item_id: UUID) -> dict[UUID, OwnerTuple]:
    found = rows(
        f"SELECT id, {_TUPLE} FROM curie.execution_requests "
        "WHERE work_item_id = :id ORDER BY sequence",
        {"id": work_item_id},
    )
    return {row["id"]: _tuple(row) for row in found}


def assert_owner_tuples(
    work_item_id: UUID,
    *,
    work_item: OwnerTuple,
    requests: Mapping[UUID, OwnerTuple],
) -> None:
    """The WorkItem and EVERY one of its requests carry exactly these tuples."""

    assert work_item_tuple(work_item_id) == work_item
    assert request_tuples(work_item_id) == dict(requests)


def request_column(request_id: UUID, column: str) -> Any:
    [row] = rows(
        f"SELECT {column} AS value FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return row["value"]


# --- the worker projection ------------------------------------------------------------


def resolve_execution_owners(request_ids: Iterable[UUID], *, tenant_id: UUID) -> dict[UUID, Any]:
    """Project each request through one newly constructed worker resolver."""

    from curie_worker.binding import BindingResolver
    from curie_worker.config import WorkerConfig

    async def go() -> dict[UUID, Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            resolver = BindingResolver(engine, WorkerConfig(db_schema="curie"), tenant_id=tenant_id)
            return {
                request_id: await resolver.resolve_execution_owner(request_id)
                for request_id in request_ids
            }
        finally:
            await engine.dispose()

    return asyncio.run(go())


def assert_resolver_projects(
    world: World,
    *,
    agent_id: UUID,
    work_item_id: UUID,
    generations: Mapping[UUID, int],
) -> None:
    """A reconstructed worker resolver sees the original owner on every request."""

    projected = resolve_execution_owners(generations, tenant_id=world.tenant_id)
    for request_id, generation in generations.items():
        owner = projected[request_id]
        assert owner is not None, request_id
        assert (
            owner.work_item_id,
            owner.request_id,
            owner.agent_id,
            owner.tenant_id,
            owner.model_auth_mode,
            owner.owner_principal_id,
            owner.enrollment_generation,
        ) == (
            work_item_id,
            request_id,
            agent_id,
            world.tenant_id,
            "personal",
            world.owner_id,
            generation,
        ), request_id


# --- operator API ------------------------------------------------------------------


def users_route(users: Iterable[str], *, address: str = ADDRESS) -> dict[str, Any]:
    """An explicit ``approvers.users`` route, the only kind an operator may resolve."""

    return {
        "resolution": {"kind": "slack", "address": address},
        "approvers": {"users": list(users)},
    }


def api_agent(
    client: TestClient,
    auth_headers: Mapping[str, str],
    *,
    tenant_id: UUID,
    address: str = ADDRESS,
    approval_routes: Mapping[str, Any] | None = None,
    deployment: bool = False,
) -> dict[str, Any]:
    """Create an agent (and optionally a workspace deployment) through the operator
    API, then re-home it into ``tenant_id``; the create route names no tenant."""

    payload: dict[str, Any] = {
        "name": f"acme-personal-{uuid.uuid4().hex[:8]}",
        "channel": {"kind": "slack", "address": address},
        "repo_full_name": REPO,
    }
    if approval_routes is not None:
        payload["approval_routes"] = dict(approval_routes)
    created = client.post("/agents", json=payload, headers=dict(auth_headers))
    assert created.status_code == 201, created.text
    agent_id = UUID(created.json()["id"])
    result: dict[str, Any] = {"agent_id": agent_id}
    if deployment:
        version = client.post(
            f"/agents/{agent_id}/versions",
            json={"version_label": "v1", "created_by": "operator"},
            headers=dict(auth_headers),
        )
        assert version.status_code == 201, version.text
        deployed = client.post(
            "/deployments",
            json={
                "agent_id": str(agent_id),
                "version_id": version.json()["id"],
                "environment": "dev",
                "workspace_enabled": True,
            },
            headers=dict(auth_headers),
        )
        assert deployed.status_code == 201, deployed.text
        result["deployment_id"] = deployed.json()["id"]
    move_agent_to_tenant(agent_id, tenant_id)
    return result


def operator_headers(
    client: TestClient, auth_headers: Mapping[str, str], subject: str
) -> dict[str, str]:
    """A subject-bound operator credential minted by the real platform-key route."""

    minted = client.post(
        "/approvals/principals/operator", json={"subject": subject}, headers=dict(auth_headers)
    )
    assert minted.status_code == 201, minted.text
    assert minted.json()["subject"] == subject
    return {APPROVAL_PRINCIPAL_HEADER: minted.json()["token"]}


def tool_approval(
    client: TestClient,
    auth_headers: Mapping[str, str],
    *,
    agent_id: UUID,
    route: str,
    conversation: str = WIRE_CONVERSATION,
    address: str = ADDRESS,
) -> dict[str, Any]:
    """A permission-gate approval raised on the WorkItem's own conversation."""

    created = client.post(
        "/approvals",
        json={
            "agent_id": str(agent_id),
            "conversation_id": conversation,
            "author": REQUESTER,
            "summary": f"Tool call awaiting approval: {TOOL}",
            "reply_kind": "slack",
            "reply_channel": address,
            "reply_placeholder": "1700000000.000001",
            "dedupe_key": uuid.uuid4().hex,
            "route": route,
            "card_channel": address,
            "gate_kind": "permission",
            "granted_tool": TOOL,
        },
        headers=dict(auth_headers),
    )
    assert created.status_code in (200, 201), created.text
    body = created.json()
    assert body["route"] == route, body
    assert body["status"] == "pending", body
    return dict(body)


def stream_turns(stream: str) -> list[QueuedTurn]:
    valkey = connect_or_skip(decode_responses=True)
    try:
        return [
            QueuedTurn.model_validate_json(fields[STREAM_PAYLOAD_FIELD])
            for _id, fields in valkey.xrange(stream)
        ]
    finally:
        valkey.close()


def credential_material(node: Any, path: str = "$") -> list[str]:
    """Every key or string value naming a token, secret, grant or credential."""

    found: list[str] = []
    if isinstance(node, dict):
        for key, child in node.items():
            here = f"{path}.{key}"
            if any(fragment in str(key).lower() for fragment in FORBIDDEN_FRAGMENTS):
                found.append(here)
            found.extend(credential_material(child, here))
    elif isinstance(node, list):
        for index, child in enumerate(node):
            found.extend(credential_material(child, f"{path}[{index}]"))
    elif isinstance(node, str) and any(
        fragment in node.lower() for fragment in FORBIDDEN_FRAGMENTS
    ):
        found.append(f"{path}={node}")
    return found
