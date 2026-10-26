"""Real PostgreSQL worker binding classification, independent of deployment.

@spec HTTP-INVOCATION-EXECUTION-9 HTTP-INVOCATION-EXECUTION-17.
No legacy identity projection supplies positive authority in these fixtures.
"""

from __future__ import annotations

import importlib
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.models import Agent, AgentChannel, TenantIdentityProvider
from curie_worker.config import WorkerConfig
from principal_adoption_support import ISSUER, selection
from role_policy_support import run, seed
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-17: absent behavior fails in test bodies."""
    module = importlib.import_module("curie_worker.binding")
    assert hasattr(module, "HttpIdentityBinding"), (
        "worker identity binding value is not implemented"
    )
    assert callable(getattr(module.BindingResolver, "identity_binding", None)), (
        "worker identity binding classification is not implemented"
    )
    return module


async def _seed_scope(
    factory: async_sessionmaker[AsyncSession],
    mode: str = "absent",
    *,
    address: str | None = None,
    adapter: str | None = None,
) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-17: authoritative committed, undeployed API rows."""
    tenant, agent = await seed(factory)
    address = address or "C0EXAMPLE1-" + uuid.uuid4().hex
    adapter = adapter or "synthetic-" + uuid.uuid4().hex[:12]
    async with factory() as session, session.begin():
        binding = AgentChannel(
            tenant_id=tenant,
            agent_id=agent,
            kind="slack",
            address=address,
            adapter=adapter,
            endpoint=None,
            generation=7,
        )
        session.add(binding)
        if mode != "absent":
            session.add(
                TenantIdentityProvider(
                    tenant_id=tenant,
                    issuer=ISSUER,
                    governed_domains=["example.test"],
                    managed_by="synthetic-directory",
                    version=1,
                    identity_epoch=0,
                    lifecycle_generation=1,
                )
            )
        await session.flush()
        if mode not in {"absent", "sql_null"}:
            value = {
                "http": selection().model_dump(mode="json"),
                "empty_object": {},
                "unknown": {"mode": "synthetic-future-mode"},
                "malformed_http": {"mode": "http", "credential_ref": False},
            }[mode]
            await session.execute(
                text(
                    "UPDATE curie.tenant_identity_providers "
                    "SET http_selection=CAST(:value AS jsonb) "
                    "WHERE tenant_id=:tenant"
                ),
                {"value": json.dumps(value), "tenant": tenant},
            )
        return {
            "tenant": tenant,
            "agent": agent,
            "binding": binding.id,
            "generation": 7,
            "address": address,
            "adapter": adapter,
        }


async def _resolver(
    factory: async_sessionmaker[AsyncSession],
    scope: dict[str, Any],
    *,
    schema: str | None = None,
) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-17: never depend on default tenant/schema or suite order."""
    from curie_worker.binding import BindingResolver

    async with factory() as session:
        engine = session.bind
    assert isinstance(engine, AsyncEngine)
    return BindingResolver(
        engine,
        WorkerConfig(db_schema=schema or get_settings().db_schema),
        tenant_id=scope["tenant"],
    )


def _assert_binding(module: Any, value: Any, scope: dict[str, Any], required: bool) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: classification is exact current provenance."""
    assert isinstance(value, module.HttpIdentityBinding)
    assert value.tenant_id == scope["tenant"] and isinstance(value.tenant_id, uuid.UUID)
    assert value.agent_id == scope["agent"] and isinstance(value.agent_id, uuid.UUID)
    assert value.binding_id == scope["binding"] and isinstance(value.binding_id, uuid.UUID)
    assert value.binding_generation == scope["generation"] and type(value.binding_generation) is int
    assert value.http_identity_required is required


@asynccontextmanager
async def _corrupt_clone(
    factory: async_sessionmaker[AsyncSession],
    scope: dict[str, Any],
    payload: str,
) -> AsyncIterator[str]:
    """@spec HTTP-INVOCATION-EXECUTION-17: corrupt only an owned clone, preserve base checks."""
    schema = "http_binding_" + uuid.uuid4().hex
    source = get_settings().db_schema
    async with factory() as session:
        database = await session.scalar(text("SELECT current_database()"))
        assert isinstance(database, str) and database.startswith("curie_test_")
        assert (
            await session.scalar(text("SELECT to_regnamespace(:schema)"), {"schema": schema})
            is None
        )
    created = False
    try:
        async with factory() as session, session.begin():
            created = True
            await session.execute(text(f"CREATE SCHEMA {schema}"))
            for table, column, identifier in (
                ("tenants", "id", scope["tenant"]),
                ("agents", "id", scope["agent"]),
                ("agent_channels", "id", scope["binding"]),
                ("tenant_identity_providers", "tenant_id", scope["tenant"]),
            ):
                await session.execute(
                    text(f"CREATE TABLE {schema}.{table} (LIKE {source}.{table} INCLUDING ALL)")
                )
                await session.execute(
                    text(
                        f"INSERT INTO {schema}.{table} SELECT * FROM {source}.{table} "
                        f"WHERE {column}=:id"
                    ),
                    {"id": identifier},
                )
            await session.execute(
                text(
                    f"ALTER TABLE {schema}.tenant_identity_providers DROP CONSTRAINT "
                    "tenant_identity_providers_http_object_ck"
                )
            )
            await session.execute(
                text(
                    f"UPDATE {schema}.tenant_identity_providers "
                    "SET http_selection=CAST(:value AS jsonb) WHERE tenant_id=:tenant"
                ),
                {"value": payload, "tenant": scope["tenant"]},
            )
        yield schema
    finally:
        if created:
            # The exact name is generated and recorded before creation; never sweep schemas.
            assert schema.startswith("http_binding_") and len(schema) == 45
            async with factory() as session, session.begin():
                await session.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
            async with factory() as session:
                assert (
                    await session.scalar(
                        text("SELECT to_regnamespace(:schema)"),
                        {"schema": schema},
                    )
                    is None
                )


def test_owned_fixture_has_explicit_tenant_and_no_active_deployment(clean_db: None) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: existing resolver control qualifies real fixtures."""

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        scope = await _seed_scope(factory, "http")
        resolver = await _resolver(factory, scope)
        assert await resolver.resolve("slack", scope["adapter"], scope["address"]) is None
        bound = await resolver.undeployed_binding("slack", scope["adapter"], scope["address"])
        assert bound is not None and bound.agent_id == scope["agent"]

    run(check)


@pytest.mark.parametrize(
    "mode,required",
    [
        ("absent", False),
        ("sql_null", False),
        ("http", True),
        ("empty_object", True),
        ("unknown", True),
        ("malformed_http", True),
    ],
)
def test_current_binding_classifies_sql_nullness_independently_of_deployment(
    clean_db: None,
    mode: str,
    required: bool,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: malformed non-null objects cannot select legacy."""
    module = api()

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        scope = await _seed_scope(factory, mode)
        resolver = await _resolver(factory, scope)
        assert await resolver.resolve("slack", scope["adapter"], scope["address"]) is None
        value = await resolver.identity_binding("slack", scope["adapter"], scope["address"])
        _assert_binding(module, value, scope, required)
        async with factory() as session, session.begin():
            await session.execute(
                text("UPDATE curie.agent_channels SET generation=generation+1 WHERE id=:id"),
                {"id": scope["binding"]},
            )
        scope["generation"] += 1
        current = await resolver.identity_binding("slack", scope["adapter"], scope["address"])
        _assert_binding(module, current, scope, required)

    run(check)


@pytest.mark.parametrize("payload", ["null", "false", "[]"])
def test_nonobject_json_selection_still_requires_context_in_owned_corrupt_clone(
    clean_db: None,
    payload: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: decoded None/false/empty is not SQL NULL."""
    module = api()

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        scope = await _seed_scope(factory, "sql_null")
        async with _corrupt_clone(factory, scope, payload) as schema:
            resolver = await _resolver(factory, scope, schema=schema)
            value = await resolver.identity_binding("slack", scope["adapter"], scope["address"])
            _assert_binding(module, value, scope, True)

    run(check)


def test_corrupt_clone_fixture_preserves_base_constraint_and_real_route(clean_db: None) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: independently qualify SQL-null/JSON-null control."""

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        scope = await _seed_scope(factory, "sql_null")
        async with _corrupt_clone(factory, scope, "null") as schema:
            resolver = await _resolver(factory, scope, schema=schema)
            bound = await resolver.undeployed_binding("slack", scope["adapter"], scope["address"])
            assert bound is not None and bound.agent_id == scope["agent"]
            async with factory() as session:
                cloned = (
                    await session.execute(
                        text(
                            f"SELECT http_selection IS NULL AS absent, "
                            "jsonb_typeof(http_selection) AS json_type "
                            f"FROM {schema}.tenant_identity_providers WHERE tenant_id=:id"
                        ),
                        {"id": scope["tenant"]},
                    )
                ).one()
                assert cloned.absent is False and cloned.json_type == "null"
                assert (
                    await session.scalar(
                        text(
                            "SELECT http_selection IS NULL FROM curie.tenant_identity_providers "
                            "WHERE tenant_id=:id"
                        ),
                        {"id": scope["tenant"]},
                    )
                    is True
                )
                assert (
                    await session.scalar(
                        text(
                            "SELECT convalidated FROM pg_constraint WHERE "
                            "conrelid='curie.tenant_identity_providers'::regclass AND "
                            "conname='tenant_identity_providers_http_object_ck'"
                        )
                    )
                    is True
                )

    run(check)


def test_route_adapter_kind_address_and_tenant_are_exact_and_unknown_is_none(
    clean_db: None,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: another tenant or adapter supplies no fallback."""
    module = api()

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        first = await _seed_scope(factory, "http")
        second = await _seed_scope(factory, "sql_null", address=first["address"])
        resolver = await _resolver(factory, first)
        other = await _resolver(factory, second)
        _assert_binding(
            module,
            await resolver.identity_binding("slack", first["adapter"], first["address"]),
            first,
            True,
        )
        _assert_binding(
            module,
            await other.identity_binding("slack", second["adapter"], second["address"]),
            second,
            False,
        )
        for kind, adapter, address in (
            ("email", first["adapter"], first["address"]),
            ("slack", first["adapter"].upper(), first["address"]),
            ("slack", first["adapter"], first["address"].lower()),
            ("slack", first["adapter"], "C0EXAMPLEUNKNOWN"),
            ("slack", second["adapter"], first["address"]),
        ):
            assert await resolver.identity_binding(kind, adapter, address) is None
        assert await other.identity_binding("slack", first["adapter"], first["address"]) is None
        # A binding whose tenant disagrees with its agent is never borrowed by either owner.
        async with factory() as session, session.begin():
            await session.execute(
                text("UPDATE curie.agent_channels SET tenant_id=:tenant WHERE id=:id"),
                {"tenant": second["tenant"], "id": first["binding"]},
            )
        assert await resolver.identity_binding("slack", first["adapter"], first["address"]) is None
        assert await other.identity_binding("slack", first["adapter"], first["address"]) is None

    run(check)


def test_slack_omitted_adapter_uses_existing_default_identity_owner(clean_db: None) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: preserve supported adapter-selection behavior."""
    module = api()

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        scope = await _seed_scope(factory, "http", adapter="default")
        resolver = await _resolver(factory, scope)
        _assert_binding(
            module, await resolver.identity_binding("slack", None, scope["address"]), scope, True
        )

    run(check)


def test_ambiguous_unqualified_non_slack_route_is_refused_without_choosing_agent(
    clean_db: None,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: every undeployed route participates in ambiguity."""
    module = api()

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        scope = await _seed_scope(factory, "http")
        address = "synthetic-" + uuid.uuid4().hex + "@example.test"
        async with factory() as session, session.begin():
            second = Agent(tenant_id=scope["tenant"], name="synthetic-second-" + uuid.uuid4().hex)
            session.add(second)
            await session.flush()
            for agent, adapter in (
                (scope["agent"], "synthetic-mail-one"),
                (second.id, "synthetic-mail-two"),
            ):
                session.add(
                    AgentChannel(
                        tenant_id=scope["tenant"],
                        agent_id=agent,
                        kind="email",
                        address=address,
                        adapter=adapter,
                        endpoint="https://mail.example.test/reply",
                        generation=3,
                    )
                )
        resolver = await _resolver(factory, scope)
        with pytest.raises(module.AmbiguousRoute):
            await resolver.identity_binding("email", None, address)
        narrowed = await resolver.identity_binding("email", "synthetic-mail-one", address)
        assert narrowed.agent_id == scope["agent"] and narrowed.http_identity_required is True

    run(check)


def test_database_failure_propagates_instead_of_becoming_absent_or_legacy(clean_db: None) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: unavailable classification cannot authorize fallback."""
    api()

    async def check(factory: async_sessionmaker[AsyncSession]) -> None:
        scope = await _seed_scope(factory, "http")
        resolver = await _resolver(factory, scope, schema="missing_binding_" + uuid.uuid4().hex)
        with pytest.raises(SQLAlchemyError):
            await resolver.identity_binding("slack", scope["adapter"], scope["address"])

    run(check)


@pytest.mark.parametrize("generation", [1, 2**63 - 1])
@pytest.mark.parametrize("required", [True, False])
def test_binding_value_is_frozen_with_actual_integer_and_boolean(
    generation: int, required: bool
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17."""
    module = api()
    value = module.HttpIdentityBinding(
        tenant_id=uuid.uuid4(),
        agent_id=uuid.uuid4(),
        binding_id=uuid.uuid4(),
        binding_generation=generation,
        http_identity_required=required,
    )
    assert type(value.binding_generation) is int and value.http_identity_required is required
    with pytest.raises((AttributeError, TypeError, ValueError)):
        value.http_identity_required = not required


@pytest.mark.parametrize(
    "field,value",
    [
        ("binding_generation", True),
        ("binding_generation", False),
        ("binding_generation", 0),
        ("binding_generation", 2**63),
        ("binding_generation", 1.0),
        ("binding_generation", "1"),
        ("http_identity_required", 1),
        ("http_identity_required", 0),
        ("http_identity_required", "false"),
        ("http_identity_required", None),
    ],
)
def test_binding_value_refuses_coerced_or_out_of_range_classification(
    field: str, value: Any
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-17: neither bool-to-int nor int-to-bool coercion."""
    module = api()
    fields = {
        "tenant_id": uuid.uuid4(),
        "agent_id": uuid.uuid4(),
        "binding_id": uuid.uuid4(),
        "binding_generation": 1,
        "http_identity_required": True,
        field: value,
    }
    with pytest.raises((TypeError, ValueError)):
        module.HttpIdentityBinding(**fields)
