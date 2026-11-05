"""Worker projection of a WorkItem's immutable owner tuple (#5376, AC2 worker half).

Real Postgres at head schema (never mocked). Rows are seeded by SQL: a
non-default tenant, an active principal, its personal enrollment, an agent, a
personal WorkItem with two ExecutionRequests (one snapshotting an older
enrollment generation) and a service WorkItem with one request. The worker only
READS the tuple: ``BindingResolver.resolve_execution_owner`` answers the facts
and nothing else, and ``boot_env`` is untouched by personal executions (#5378
owns enforcement and any credential delivery).

Everything inserted is removed in the ``finally`` of each test.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import pytest
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker.binding import (
    BindingResolver,
    ExecutionOwner,
    ResolvedDeployment,
)
from curie_worker.config import WorkerConfig
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")

_OWNER_FIELDS = {
    "work_item_id",
    "request_id",
    "agent_id",
    "tenant_id",
    "model_auth_mode",
    "owner_principal_id",
    "enrollment_generation",
}
_FORBIDDEN_FRAGMENTS = ("token", "secret", "grant", "credential", "key")

# Execution-request state shapes, as SQL value tails for (status, wait_deadline,
# started_at, execution_deadline, terminal_at, terminal_cause); both satisfy
# execution_requests_state_shape_ck (copied from the API readmit fixtures).
_COMPLETED_SHAPE = (
    "'completed', clock_timestamp() - interval '2 minutes', "
    "clock_timestamp() - interval '90 minutes', "
    "clock_timestamp() - interval '30 minutes', "
    "clock_timestamp() - interval '29 minutes', 'completed'"
)
_RUNNING_SHAPE = (
    "'running', clock_timestamp() - interval '2 minutes', "
    "clock_timestamp() - interval '5 minutes', "
    "clock_timestamp() + interval '55 minutes', NULL, NULL"
)


@dataclass
class Seeded:
    tenant_id: uuid.UUID
    other_tenant_id: uuid.UUID
    principal_id: uuid.UUID
    agent_id: uuid.UUID
    personal_item_id: uuid.UUID
    old_request_id: uuid.UUID
    current_request_id: uuid.UUID
    service_item_id: uuid.UUID
    service_request_id: uuid.UUID
    current_generation: int
    old_generation: int


def _resolver(engine: AsyncEngine, tenant_id: uuid.UUID) -> BindingResolver:
    return BindingResolver(engine, WorkerConfig(db_schema=_SCHEMA), tenant_id=tenant_id)


async def _insert_work_item(
    conn,
    *,
    item_id: uuid.UUID,
    agent_id: uuid.UUID,
    tenant_id: uuid.UUID,
    mode: str,
    owner: uuid.UUID | None,
    generation: int | None,
) -> None:
    await conn.execute(
        text(
            f"INSERT INTO {_SCHEMA}.work_items (id, code_host_kind, code_host_host, "
            "tracker_kind, tracker_host, tracker_scope_id, repository_project_id, "
            "tracker_issue_id, code_host_installation_id, agent_id, repository_path, "
            "conversation_id, version, base_branch, base_source, base_commit, "
            "tenant_id, model_auth_mode, owner_principal_id, enrollment_generation) "
            "VALUES (:id, 'github', 'github.com', 'github', 'github.com', '9001', "
            "'9001', :issue, 41, :agent, 'acme-corp/acme-bot', :conversation, 1, "
            "'main', 'default', :base, :tenant, :mode, :owner, :generation)"
        ),
        {
            "id": item_id,
            "issue": str(uuid.uuid4().int % 10**9),
            "agent": agent_id,
            "conversation": f"conv-{item_id.hex[:12]}",
            "base": "a" * 40,
            "tenant": tenant_id,
            "mode": mode,
            "owner": owner,
            "generation": generation,
        },
    )


async def _insert_request(
    conn,
    *,
    request_id: uuid.UUID,
    item_id: uuid.UUID,
    sequence: int,
    shape: str,
    tenant_id: uuid.UUID,
    mode: str,
    owner: uuid.UUID | None,
    generation: int | None,
) -> None:
    await conn.execute(
        text(
            f"INSERT INTO {_SCHEMA}.execution_requests (id, work_item_id, sequence, status, "
            "wait_deadline, started_at, execution_deadline, terminal_at, terminal_cause, "
            "execution_attempts, tenant_id, model_auth_mode, owner_principal_id, "
            "enrollment_generation) "
            f"VALUES (:id, :item, :sequence, {shape}, 1, :tenant, :mode, :owner, :generation)"
        ),
        {
            "id": request_id,
            "item": item_id,
            "sequence": sequence,
            "tenant": tenant_id,
            "mode": mode,
            "owner": owner,
            "generation": generation,
        },
    )


async def _seed(engine: AsyncEngine) -> Seeded:
    seeded = Seeded(
        tenant_id=uuid.uuid4(),
        other_tenant_id=uuid.uuid4(),
        principal_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        personal_item_id=uuid.uuid4(),
        old_request_id=uuid.uuid4(),
        current_request_id=uuid.uuid4(),
        service_item_id=uuid.uuid4(),
        service_request_id=uuid.uuid4(),
        current_generation=7,
        old_generation=6,
    )
    async with engine.begin() as conn:
        for tenant in (seeded.tenant_id, seeded.other_tenant_id):
            await conn.execute(
                text(
                    f"INSERT INTO {_SCHEMA}.tenants (id, deployment_id, status) "
                    "VALUES (:id, gen_random_uuid()::text, 'active')"
                ),
                {"id": tenant},
            )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.principals (id, tenant_id, idp_issuer, idp_subject, "
                "type, status) VALUES (:id, :tenant, 'https://idp.example', :subject, "
                "'human', 'active')"
            ),
            {"id": seeded.principal_id, "tenant": seeded.tenant_id, "subject": uuid.uuid4().hex},
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.personal_claude_enrollments "
                "(id, tenant_id, principal_id, generation, active, version) "
                "VALUES (:id, :tenant, :principal, :generation, true, 1)"
            ),
            {
                "id": uuid.uuid4(),
                "tenant": seeded.tenant_id,
                "principal": seeded.principal_id,
                "generation": seeded.current_generation,
            },
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agents (id, name, tenant_id) VALUES (:id, :name, :tenant)"
            ),
            {
                "id": seeded.agent_id,
                "name": f"personal-owner-{seeded.agent_id.hex[:8]}",
                "tenant": seeded.tenant_id,
            },
        )
        await _insert_work_item(
            conn,
            item_id=seeded.personal_item_id,
            agent_id=seeded.agent_id,
            tenant_id=seeded.tenant_id,
            mode="personal",
            owner=seeded.principal_id,
            generation=seeded.current_generation,
        )
        await _insert_request(
            conn,
            request_id=seeded.old_request_id,
            item_id=seeded.personal_item_id,
            sequence=1,
            shape=_COMPLETED_SHAPE,
            tenant_id=seeded.tenant_id,
            mode="personal",
            owner=seeded.principal_id,
            generation=seeded.old_generation,
        )
        await _insert_request(
            conn,
            request_id=seeded.current_request_id,
            item_id=seeded.personal_item_id,
            sequence=2,
            shape=_RUNNING_SHAPE,
            tenant_id=seeded.tenant_id,
            mode="personal",
            owner=seeded.principal_id,
            generation=seeded.current_generation,
        )
        await _insert_work_item(
            conn,
            item_id=seeded.service_item_id,
            agent_id=seeded.agent_id,
            tenant_id=seeded.tenant_id,
            mode="service",
            owner=None,
            generation=None,
        )
        await _insert_request(
            conn,
            request_id=seeded.service_request_id,
            item_id=seeded.service_item_id,
            sequence=1,
            shape=_RUNNING_SHAPE,
            tenant_id=seeded.tenant_id,
            mode="service",
            owner=None,
            generation=None,
        )
    return seeded


async def _cleanup(engine: AsyncEngine, seeded: Seeded) -> None:
    async with engine.begin() as conn:
        for item in (seeded.personal_item_id, seeded.service_item_id):
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.execution_requests WHERE work_item_id = :id"),
                {"id": item},
            )
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.work_items WHERE id = :id"), {"id": item}
            )
        await conn.execute(
            text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"), {"id": seeded.agent_id}
        )
        await conn.execute(
            text(f"DELETE FROM {_SCHEMA}.personal_claude_enrollments WHERE tenant_id = :id"),
            {"id": seeded.tenant_id},
        )
        await conn.execute(
            text(f"DELETE FROM {_SCHEMA}.principals WHERE id = :id"),
            {"id": seeded.principal_id},
        )
        for tenant in (seeded.tenant_id, seeded.other_tenant_id):
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.tenants WHERE id = :id"), {"id": tenant}
            )


def _with_seed(body: Callable[[AsyncEngine, Seeded], Awaitable[None]]) -> None:
    async def go() -> None:
        engine = create_async_engine(_DB_URL)
        seeded: Seeded | None = None
        try:
            await pg_connect_or_skip(engine)
            seeded = await _seed(engine)
            await body(engine, seeded)
        finally:
            if seeded is not None:
                await _cleanup(engine, seeded)
            await engine.dispose()

    asyncio.run(go())


def test_a_personal_request_projects_its_own_snapshot_of_the_owner_tuple() -> None:
    async def body(engine: AsyncEngine, seeded: Seeded) -> None:
        resolver = _resolver(engine, seeded.tenant_id)

        current = await resolver.resolve_execution_owner(seeded.current_request_id)
        old = await resolver.resolve_execution_owner(seeded.old_request_id)

        assert isinstance(current, ExecutionOwner)
        assert isinstance(old, ExecutionOwner)
        assert current.model_dump() == {
            "work_item_id": seeded.personal_item_id,
            "request_id": seeded.current_request_id,
            "agent_id": seeded.agent_id,
            "tenant_id": seeded.tenant_id,
            "model_auth_mode": "personal",
            "owner_principal_id": seeded.principal_id,
            "enrollment_generation": seeded.current_generation,
        }
        # The older request keeps the generation it snapshotted at insertion.
        assert old.request_id == seeded.old_request_id
        assert old.work_item_id == seeded.personal_item_id
        assert old.owner_principal_id == seeded.principal_id
        assert old.enrollment_generation == seeded.old_generation

    _with_seed(body)


def test_a_service_request_projects_service_mode_with_no_owner_or_generation() -> None:
    async def body(engine: AsyncEngine, seeded: Seeded) -> None:
        owner = await _resolver(engine, seeded.tenant_id).resolve_execution_owner(
            seeded.service_request_id
        )

        assert owner is not None
        assert owner.model_dump() == {
            "work_item_id": seeded.service_item_id,
            "request_id": seeded.service_request_id,
            "agent_id": seeded.agent_id,
            "tenant_id": seeded.tenant_id,
            "model_auth_mode": "service",
            "owner_principal_id": None,
            "enrollment_generation": None,
        }

    _with_seed(body)


def test_a_legacy_shaped_service_request_projects_with_no_tenant_snapshot() -> None:
    # Migration 0148 is an expand: an older application inside the schema
    # window still inserts WorkItems and requests that name none of the four
    # owner columns, so tenant_id stays NULL and the mode defaults to service.
    # Such a row must still project, as the API read already shows it.
    legacy_item_id, legacy_request_id = uuid.uuid4(), uuid.uuid4()

    async def body(engine: AsyncEngine, seeded: Seeded) -> None:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    f"INSERT INTO {_SCHEMA}.work_items (id, code_host_kind, code_host_host, "
                    "tracker_kind, tracker_host, tracker_scope_id, repository_project_id, "
                    "tracker_issue_id, code_host_installation_id, agent_id, repository_path, "
                    "conversation_id, version, base_branch, base_source, base_commit) "
                    "VALUES (:id, 'github', 'github.com', 'github', 'github.com', '9001', "
                    "'9001', :issue, 41, :agent, 'acme-corp/acme-bot', :conversation, 1, "
                    "'main', 'default', :base)"
                ),
                {
                    "id": legacy_item_id,
                    "issue": str(uuid.uuid4().int % 10**9),
                    "agent": seeded.agent_id,
                    "conversation": f"conv-{legacy_item_id.hex[:12]}",
                    "base": "a" * 40,
                },
            )
            await conn.execute(
                text(
                    f"INSERT INTO {_SCHEMA}.execution_requests (id, work_item_id, sequence, "
                    "status, wait_deadline, started_at, execution_deadline, terminal_at, "
                    "terminal_cause, execution_attempts) "
                    f"VALUES (:id, :item, 1, {_RUNNING_SHAPE}, 1)"
                ),
                {"id": legacy_request_id, "item": legacy_item_id},
            )
        try:
            owning = _resolver(engine, seeded.tenant_id)
            owner = await owning.resolve_execution_owner(legacy_request_id)

            assert owner is not None
            assert owner.model_dump() == {
                "work_item_id": legacy_item_id,
                "request_id": legacy_request_id,
                "agent_id": seeded.agent_id,
                "tenant_id": None,
                "model_auth_mode": "service",
                "owner_principal_id": None,
                "enrollment_generation": None,
            }
            # The tenant scope still comes from the agent, not the NULL snapshot.
            foreign = _resolver(engine, seeded.other_tenant_id)
            assert await foreign.resolve_execution_owner(legacy_request_id) is None
        finally:
            async with engine.begin() as conn:
                await conn.execute(
                    text(f"DELETE FROM {_SCHEMA}.execution_requests WHERE work_item_id = :id"),
                    {"id": legacy_item_id},
                )
                await conn.execute(
                    text(f"DELETE FROM {_SCHEMA}.work_items WHERE id = :id"),
                    {"id": legacy_item_id},
                )

    _with_seed(body)


def test_another_tenants_resolver_answers_none_exactly_like_an_unknown_request() -> None:
    async def body(engine: AsyncEngine, seeded: Seeded) -> None:
        foreign = _resolver(engine, seeded.other_tenant_id)
        owning = _resolver(engine, seeded.tenant_id)

        # Control: the owning tenant resolves the very same ids.
        assert await owning.resolve_execution_owner(seeded.current_request_id) is not None
        assert await owning.resolve_execution_owner(seeded.service_request_id) is not None

        assert await foreign.resolve_execution_owner(seeded.current_request_id) is None
        assert await foreign.resolve_execution_owner(seeded.old_request_id) is None
        assert await foreign.resolve_execution_owner(seeded.service_request_id) is None
        assert await owning.resolve_execution_owner(uuid.uuid4()) is None
        assert await foreign.resolve_execution_owner(uuid.uuid4()) is None

    _with_seed(body)


def test_the_projection_is_read_only() -> None:
    async def body(engine: AsyncEngine, seeded: Seeded) -> None:
        async def snapshot() -> list[tuple[object, ...]]:
            async with engine.connect() as conn:
                rows = await conn.execute(
                    text(
                        f"SELECT id, version, model_auth_mode, owner_principal_id, "
                        f"enrollment_generation FROM {_SCHEMA}.execution_requests "
                        "WHERE work_item_id IN (:a, :b) ORDER BY id"
                    ),
                    {"a": seeded.personal_item_id, "b": seeded.service_item_id},
                )
                return [tuple(row) for row in rows]

        before = await snapshot()
        resolver = _resolver(engine, seeded.tenant_id)
        for request_id in (
            seeded.old_request_id,
            seeded.current_request_id,
            seeded.service_request_id,
        ):
            await resolver.resolve_execution_owner(request_id)

        assert await snapshot() == before

    _with_seed(body)


def test_execution_owner_is_frozen_forbids_extras_and_carries_no_credential_field() -> None:
    assert set(ExecutionOwner.model_fields) == _OWNER_FIELDS
    for name in ExecutionOwner.model_fields:
        assert not any(fragment in name for fragment in _FORBIDDEN_FRAGMENTS), name

    owner = ExecutionOwner(
        work_item_id=uuid.uuid4(),
        request_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        model_auth_mode="personal",
        owner_principal_id=uuid.uuid4(),
        enrollment_generation=2,
    )
    with pytest.raises(ValidationError):
        owner.enrollment_generation = 3  # type: ignore[misc]
    with pytest.raises(ValidationError):
        ExecutionOwner(**owner.model_dump(), claude_token="x")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        ExecutionOwner(**{**owner.model_dump(), "model_auth_mode": "other"})


def _resolved(agent_id: uuid.UUID) -> ResolvedDeployment:
    return ResolvedDeployment(
        agent_name="test-agent",
        agent_id=agent_id,
        version_id=uuid.UUID("22222222-2222-4222-8222-222222222222"),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
    )


def test_boot_env_is_unchanged_by_the_existence_of_personal_executions() -> None:
    async def body(engine: AsyncEngine, seeded: Seeded) -> None:
        resolver = _resolver(engine, seeded.tenant_id)
        resolved = _resolved(seeded.agent_id)
        personal_env = resolver.boot_env(resolved, f"thread-{seeded.personal_item_id.hex[:8]}")
        service_env = resolver.boot_env(resolved, f"thread-{seeded.service_item_id.hex[:8]}")

        # The same deployment renders the same key set whichever WorkItem the
        # thread belongs to: no ACI boot field was added for the owner tuple.
        assert set(personal_env) == set(service_env)
        for key in personal_env:
            lowered = key.lower()
            for fragment in ("owner", "principal", "generation", "enrollment", "auth_mode"):
                assert fragment not in lowered, key
        everything = "\n".join([*personal_env, *personal_env.values()])
        assert str(seeded.principal_id) not in everything
        assert str(seeded.tenant_id) not in everything
        assert str(seeded.personal_item_id) not in everything
        assert str(seeded.current_request_id) not in everything

    _with_seed(body)
