"""Warm-pool version targets (#1492 D1): exact active winner, refs, lookup.

The winner a warm pool is built for must be the SAME deployment the cold path
boots (`binding._RESOLVE_SQL`) and the connector reconciler serves
(`connector_loop._TARGETS_SQL`). A bundleless winner is REPORTED and marked
unrealizable; it is never skipped so that a lower-ranked bundleful version can
be warmed in its place (#1216 rank-first-decide-second).

Postgres-backed tests skip when no database is reachable and say so; nothing
here touches Kubernetes or a Secret.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from aci_protocol import BootEnv
from curie_worker import binding, connector_loop
from curie_worker.sandbox.warm_pool_contract import CredentialGeneration
from curie_worker.sandbox.warm_pool_targets import (
    ACTIVE_WINNERS_SQL,
    ActiveWinner,
    LookupOutcome,
    ObservedGeneration,
    SlotAllocation,
    TargetError,
    VersionPoolRef,
    VersionPoolTarget,
    active_winner,
    active_winners,
    build_target,
    derive_ref,
    lookup,
    slot_secret_names,
)
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .test_warm_pool_contract import (
    BASELINE_CREDENTIAL,
    CONNECTOR_SECRET_NAME,
    GENERATION,
    OBSERVED_CREDENTIAL_REVISIONS,
    _config,
    _project,
    _resolved,
    _resolver,
)

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")

_ORDER_KEY = "(d.environment = 'prod') DESC, d.deployed_at DESC, d.id DESC"
SLOT = SlotAllocation("curie-warm-bootstrap", 4, 1)


# --- the winner query is the resolver's winner -------------------------------------


def test_winner_order_key_is_byte_identical_to_binding_and_connector_loop() -> None:
    def order_clause(sql: str) -> str:
        match = re.search(r"ORDER BY\s+(.*)$", sql.strip(), flags=re.S)
        assert match is not None
        return match.group(1).strip()

    assert order_clause(ACTIVE_WINNERS_SQL) == f"a.id, {_ORDER_KEY}"
    assert order_clause(binding._RESOLVE_SQL) == _ORDER_KEY
    assert order_clause(connector_loop._TARGETS_SQL) == f"a.id, {_ORDER_KEY}"


def test_winner_query_reports_bundle_identity_and_never_filters_on_it() -> None:
    assert "DISTINCT ON (a.id)" in ACTIVE_WINNERS_SQL
    assert "v.bundle_sha256 AS bundle_sha256" in ACTIVE_WINNERS_SQL
    assert "bundle_ref IS NOT NULL" not in ACTIVE_WINNERS_SQL
    assert "d.status = 'active'" in ACTIVE_WINNERS_SQL
    # Every column ResolvedDeployment reads from the resolver query is selected
    # here too, so the projection is computed from the same facts as boot_env.
    for column in (
        "model",
        "thinking",
        "approval_required_tools",
        "secrets",
        "memory",
        "max_usd_per_day",
    ):
        assert f"a.{column} AS {column}" in ACTIVE_WINNERS_SQL, column


# --- targets ----------------------------------------------------------------------


def _winner(**overrides: object) -> ActiveWinner:
    resolved = _resolved(**{k: v for k, v in overrides.items() if k != "bundle_sha256"})
    return ActiveWinner(
        resolved=resolved,
        bundle_sha256=overrides.get("bundle_sha256", "ab" * 32),  # type: ignore[arg-type]
        environment="prod",
    )


def _target(winner: ActiveWinner | None = None, **kw: object) -> VersionPoolTarget:
    winner = winner or _winner()
    kw.setdefault("credential_revisions", OBSERVED_CREDENTIAL_REVISIONS)
    return build_target(
        winner,
        _resolver(),
        namespace="curie",
        model_credential_ref=BASELINE_CREDENTIAL,
        connector_secret_name=CONNECTOR_SECRET_NAME,
        credential_generation=GENERATION,
        **kw,  # type: ignore[arg-type]
    )


def test_target_carries_identity_and_the_projection_generation() -> None:
    target = _target()
    assert target.realizable
    assert target.refusal is None
    assert target.capability_generation == _project().capability_generation
    assert (target.agent_id, target.version_id, target.deployment_id) == (
        str(_resolved().agent_id),
        str(_resolved().version_id),
        str(_resolved().deployment_id),
    )
    assert target.bundle_sha256 == "ab" * 32
    assert "sk-live" not in repr(target)


def test_bundleless_winner_is_reported_unrealizable_not_replaced() -> None:
    target = _target(_winner(bundle_ref=None, bundle_sha256=None))
    assert not target.realizable
    assert target.refusal == "bundleless-winner"
    assert target.version_id == str(_resolved().version_id)
    with pytest.raises(TargetError, match="bundleless"):
        derive_ref(target, prefix="curie", slot=SLOT)


def test_target_without_a_deployment_id_is_refused() -> None:
    with pytest.raises(TargetError, match="deployment"):
        _target(_winner(deployment_id=None))


def test_bundle_ref_without_sha_is_reported_unrealizable() -> None:
    target = _target(_winner(bundle_sha256=None))
    assert not target.realizable
    assert target.refusal == "bundle-sha256-missing"


def test_missing_credential_authority_cannot_name_a_realizable_pool() -> None:
    target = _target(credential_revisions=None)
    assert not target.realizable
    assert target.refusal == "credential-authority-unverified"
    with pytest.raises(TargetError, match="credential-authority-unverified"):
        derive_ref(target, prefix="curie", slot=SLOT)


# --- refs -------------------------------------------------------------------------


_DNS_LABEL = re.compile(r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")


def test_ref_names_are_dns_labels_distinct_from_helm_pools() -> None:
    ref = derive_ref(_target(), prefix="curie", slot=SLOT)
    names = {ref.template_name, ref.pool_name, ref.bootstrap_secret_name}
    assert len(names) == 3
    for name in names:
        assert _DNS_LABEL.match(name), name
        assert len(name) <= 63
        assert not name.endswith("-runner-pool"), name
        assert "-agent-" not in name, name
    assert ref.bootstrap_secret_key == BootEnv.env_key("runner_bootstrap_token")
    assert ref.credential_secret_keys == _project().credential_keys
    assert ref.capability_generation == _target().capability_generation
    assert ref.namespace == "curie"
    assert ref.version_id == _target().version_id
    assert ref.slot_index == 1


def test_secret_name_is_the_granted_slot_not_a_hash_family() -> None:
    """A static resourceNames grant must cover the Secret after a restart (ADR-0122 d.4)."""

    granted = slot_secret_names("curie-warm-bootstrap", 4)
    assert granted == (
        "curie-warm-bootstrap-0",
        "curie-warm-bootstrap-1",
        "curie-warm-bootstrap-2",
        "curie-warm-bootstrap-3",
    )
    a = derive_ref(_target(), prefix="curie", slot=SLOT)
    b = derive_ref(_target(_winner(model="claude-sonnet-5")), prefix="curie", slot=SLOT)
    assert a.bootstrap_secret_name in granted
    # Same slot, different generation: the NAME is the slot; identity is labels.
    assert a.bootstrap_secret_name == b.bootstrap_secret_name
    assert a.capability_generation != b.capability_generation
    assert a.template_name != b.template_name
    assert a.pool_name != b.pool_name


@pytest.mark.parametrize(
    ("prefix", "max_slots", "index"),
    [
        ("curie-warm-bootstrap", 4, 4),
        ("curie-warm-bootstrap", 4, -1),
        ("curie-warm-bootstrap", 0, 0),
        ("Curie-Warm", 4, 0),
        ("", 4, 0),
        ("curie-agent-x-warm", 4, 0),
    ],
)
def test_slot_outside_the_grant_is_refused(prefix: str, max_slots: int, index: int) -> None:
    with pytest.raises(TargetError):
        SlotAllocation(prefix, max_slots, index)


def test_ref_names_change_with_the_generation_and_version() -> None:
    a = derive_ref(_target(), prefix="curie", slot=SLOT)
    b = derive_ref(_target(_winner(model="claude-sonnet-5")), prefix="curie", slot=SLOT)
    c = derive_ref(
        _target(_winner(version_id=uuid.UUID("00000000-0000-4000-8000-0000000000a2"))),
        prefix="curie",
        slot=SLOT,
    )
    assert len({a.pool_name, b.pool_name, c.pool_name}) == 3
    assert len({a.template_name, b.template_name, c.template_name}) == 3


@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "Curie",
        "cu_rie",
        "-curie",
        "x" * 40,
        "curie-agent-x",
        "curie-runner-pool",
        "curie-runner",
    ],
)
def test_ref_refuses_bad_prefixes(prefix: str) -> None:
    with pytest.raises(TargetError):
        derive_ref(_target(), prefix=prefix, slot=SLOT)


def test_pool_name_reserves_controller_headroom_under_the_label_limit() -> None:
    longest_ok = "x" * 21
    ref = derive_ref(_target(), prefix=longest_ok, slot=SLOT)
    assert len(ref.pool_name) <= 63 - 12
    with pytest.raises(TargetError, match="pool"):
        derive_ref(_target(), prefix="x" * 22, slot=SLOT)


def test_slot_secret_names_are_structurally_disjoint_from_pool_objects() -> None:
    ref = derive_ref(_target(), prefix="curie", slot=SLOT)
    assert ref.bootstrap_secret_name[-1].isdigit()
    assert ref.template_name.endswith("-tpl") and ref.pool_name.endswith("-pool")
    for name in slot_secret_names("curie-vp-00000000-000000000000", 3):
        assert name not in {ref.template_name, ref.pool_name}


def test_active_winner_equality_and_repr_ignore_secret_values() -> None:
    a = _winner(secrets={"GH_TOKEN": "MATERIAL-A-91c2"})
    b = _winner(secrets={"GH_TOKEN": "MATERIAL-B-91c2"})
    assert a == b and hash(a) == hash(b)
    assert a != _winner(version_id=uuid.UUID("00000000-0000-4000-8000-0000000000a2"))
    assert "MATERIAL-A-91c2" not in repr(a) + str(a)


def test_ref_is_a_value_and_serializes_without_material() -> None:
    material = {
        "GH_TOKEN": "CONNECTOR-MATERIAL-4d1e",
        "OTHER_TOKEN": "CONNECTOR-MATERIAL-4d1f",
    }
    credential = "MODEL-CREDENTIAL-MATERIAL-4d20"
    winner = _winner(secrets=material)
    target = build_target(
        winner,
        _resolver(_config(credentials=credential)),
        namespace="curie",
        model_credential_ref=BASELINE_CREDENTIAL,
        connector_secret_name=CONNECTOR_SECRET_NAME,
        credential_generation=GENERATION,
        credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
    )
    ref = derive_ref(target, prefix="curie", slot=SLOT)
    again = VersionPoolRef(**ref.as_dict())  # type: ignore[arg-type]
    assert again == ref
    # A ref is names and env KEYS only; the same dict is what a log line may carry.
    assert set(ref.as_dict()) == {
        "namespace",
        "version_id",
        "capability_generation",
        "slot_index",
        "template_name",
        "pool_name",
        "bootstrap_secret_name",
        "bootstrap_secret_key",
        "credential_secret_keys",
    }
    rendered = json.dumps(ref.as_dict()) + repr(ref) + str(ref) + repr(target) + str(target)
    rendered += target.projection.canonical_json()
    for value in (*material.values(), credential):
        assert value not in rendered
    assert "GH_TOKEN" in target.projection.secret_refs


# --- lookup: current generation or cold, never another pool ------------------------


def test_lookup_matches_only_the_current_generation() -> None:
    target = _target()
    ref = derive_ref(target, prefix="curie", slot=SLOT)
    observed = ObservedGeneration(
        template_name=ref.template_name,
        version_id=target.version_id,
        capability_generation=target.capability_generation,
    )
    assert lookup(observed, target) == LookupOutcome.MATCH
    assert lookup(None, target) == LookupOutcome.ABSENT
    stale = ObservedGeneration(
        template_name=ref.template_name,
        version_id=target.version_id,
        capability_generation="0" * 64,
    )
    assert lookup(stale, target) == LookupOutcome.MISMATCH
    other_version = ObservedGeneration(
        template_name=ref.template_name,
        version_id="00000000-0000-4000-8000-0000000000a2",
        capability_generation=target.capability_generation,
    )
    assert lookup(other_version, target) == LookupOutcome.MISMATCH
    helm_generic = ObservedGeneration(
        template_name="curie-runner", version_id=None, capability_generation=None
    )
    assert lookup(helm_generic, target) == LookupOutcome.MISMATCH


def test_lookup_refuses_an_unrealizable_target() -> None:
    target = _target(_winner(bundle_ref=None, bundle_sha256=None))
    assert lookup(None, target) == LookupOutcome.MISMATCH


# --- real Postgres: the query agrees with the resolver ----------------------------


def _db_locator() -> str:
    """Host and port only: the URL may carry a password and skip reasons reach reports."""

    url = make_url(_DB_URL)
    return f"{url.host}:{url.port}"


async def _engine_or_skip() -> AsyncEngine:
    """A reachable, MIGRATED database, or a skip that says which half is missing.

    A reachable server without the ``curie`` schema (another task's fresh
    stack on the shared port) is not evidence about this query either way, so
    it skips with that reason rather than failing on ``UndefinedTableError``.
    Skip reasons name the host, port and exception TYPE only.
    """

    engine = create_async_engine(_DB_URL)
    try:
        async with engine.connect() as conn:
            table = (
                await conn.execute(text("SELECT to_regclass(:name)"), {"name": f"{_SCHEMA}.agents"})
            ).scalar_one()
    except (SQLAlchemyError, OSError) as exc:  # pragma: no cover - environment dependent
        await engine.dispose()
        pytest.skip(f"Postgres not reachable at {_db_locator()}: {type(exc).__name__}")
    if table is None:  # pragma: no cover - environment dependent
        await engine.dispose()
        pytest.skip(f"Postgres at {_db_locator()} has no migrated {_SCHEMA}.agents table")
    return engine


SEEDED_JSON = {
    "behavior_packs": {"greeting": {"enabled": True}},
    "approval_required_tools": ["Bash", "Write"],
    "approval_routes": {"ops": {"resolve": {"kind": "slack", "address": "C0EXAMPLE9"}}},
    "secrets": {"GH_TOKEN": "seeded-synthetic-value"},
}


async def _seed_agent(engine: AsyncEngine, *, name: str, address: str, memory: bool) -> uuid.UUID:
    """One agent with every JSONB column populated, so coercion parity is exercised."""

    agent_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agents (id, name, model, memory, behavior_packs, "
                "approval_required_tools, approval_routes, secrets) "
                "VALUES (:id, :name, 'claude-opus-5', :memory, CAST(:packs AS jsonb), "
                "CAST(:gates AS jsonb), CAST(:routes AS jsonb), CAST(:secrets AS jsonb))"
            ),
            {
                "id": agent_id,
                "name": name,
                "memory": memory,
                "packs": json.dumps(SEEDED_JSON["behavior_packs"]),
                "gates": json.dumps(SEEDED_JSON["approval_required_tools"]),
                "routes": json.dumps(SEEDED_JSON["approval_routes"]),
                "secrets": json.dumps(SEEDED_JSON["secrets"]),
            },
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agent_channels (id, agent_id, kind, address) "
                "VALUES (:id, :agent_id, 'slack', :address)"
            ),
            {"id": uuid.uuid4(), "agent_id": agent_id, "address": address},
        )
    return agent_id


async def _seed_deployment(
    engine: AsyncEngine,
    *,
    agent_id: uuid.UUID,
    environment: str,
    bundle_ref: str | None,
    bundle_sha256: str | None,
    deployed_seconds_ago: int,
) -> uuid.UUID:
    version_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agent_versions "
                "(id, agent_id, version_label, bundle_ref, bundle_sha256, created_by) "
                "VALUES (:id, :agent_id, :label, :ref, :sha, 'test')"
            ),
            {
                "id": version_id,
                "agent_id": agent_id,
                "label": f"v-{environment}",
                "ref": bundle_ref,
                "sha": bundle_sha256,
            },
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.deployments "
                "(id, agent_id, version_id, environment, status, deployed_at) "
                f"VALUES (:id, :agent_id, :version_id, CAST(:env AS {_SCHEMA}.environment), "
                "'active', :deployed_at)"
            ),
            {
                "id": uuid.uuid4(),
                "agent_id": agent_id,
                "version_id": version_id,
                "env": environment,
                "deployed_at": datetime.now(UTC) - timedelta(seconds=deployed_seconds_ago),
            },
        )
    return version_id


async def _cleanup(engine: AsyncEngine, agent_ids: list[uuid.UUID]) -> None:
    async with engine.begin() as conn:
        for agent_id in agent_ids:
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"), {"id": agent_id}
            )


def test_active_winner_query_agrees_with_the_resolver_on_real_postgres() -> None:
    """A bundleless prod winner outranks a newer bundleful dev version in BOTH queries."""

    async def go() -> None:
        engine = await _engine_or_skip()
        token = uuid.uuid4().hex[:8]
        agent_ids: list[uuid.UUID] = []
        try:
            agent_id = await _seed_agent(
                engine, name=f"warm-{token}", address=f"C-warm-{token}", memory=True
            )
            agent_ids.append(agent_id)
            prod_version = await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="prod",
                bundle_ref=None,
                bundle_sha256=None,
                deployed_seconds_ago=600,
            )
            await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="dev",
                bundle_ref=f"bundles/{token}/dev.tgz",
                bundle_sha256="cd" * 32,
                deployed_seconds_ago=1,
            )
            resolver = binding.BindingResolver(engine, _config(db_schema=_SCHEMA))
            resolved = await resolver.resolve("slack", f"C-warm-{token}")
            assert resolved is not None
            assert resolved.version_id == prod_version

            winner = await active_winner(engine, _SCHEMA, agent_id)
            assert winner is not None
            assert winner.resolved.version_id == prod_version
            assert winner.resolved.model_dump() == resolved.model_dump()
            assert winner.resolved.behavior_packs == SEEDED_JSON["behavior_packs"]
            assert winner.resolved.approval_required_tools == SEEDED_JSON["approval_required_tools"]
            assert winner.resolved.approval_routes == SEEDED_JSON["approval_routes"]
            assert winner.resolved.secrets == SEEDED_JSON["secrets"]
            assert winner.bundle_sha256 is None
            assert winner.environment == "prod"

            everyone = {w.resolved.agent_id: w for w in await active_winners(engine, _SCHEMA)}
            assert everyone[agent_id].resolved.version_id == prod_version

            target = build_target(
                winner,
                resolver,
                namespace="curie",
                model_credential_ref=BASELINE_CREDENTIAL,
                connector_secret_name=CONNECTOR_SECRET_NAME,
                credential_generation=CredentialGeneration.for_window(
                    str(agent_id), issued_at=1_800_000_000
                ),
                credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
            )
            assert not target.realizable
            assert target.refusal == "bundleless-winner"
        finally:
            await _cleanup(engine, agent_ids)
            await engine.dispose()

    asyncio.run(go())


def test_active_winner_query_returns_bundle_sha256_on_real_postgres() -> None:
    async def go() -> None:
        engine = await _engine_or_skip()
        token = uuid.uuid4().hex[:8]
        agent_ids: list[uuid.UUID] = []
        try:
            agent_id = await _seed_agent(
                engine, name=f"warm-{token}", address=f"C-warm-{token}", memory=True
            )
            agent_ids.append(agent_id)
            version = await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="prod",
                bundle_ref=f"bundles/{token}/prod.tgz",
                bundle_sha256="ab" * 32,
                deployed_seconds_ago=5,
            )
            winner = await active_winner(engine, _SCHEMA, agent_id)
            assert winner is not None
            assert winner.resolved.version_id == version
            assert winner.bundle_sha256 == "ab" * 32
            target = build_target(
                winner,
                binding.BindingResolver(engine, _config(db_schema=_SCHEMA)),
                namespace="curie",
                model_credential_ref=BASELINE_CREDENTIAL,
                connector_secret_name=CONNECTOR_SECRET_NAME,
                credential_generation=CredentialGeneration.for_window(
                    str(agent_id), issued_at=1_800_000_000
                ),
                credential_revisions=OBSERVED_CREDENTIAL_REVISIONS,
            )
            assert target.realizable
            assert target.bundle_sha256 == "ab" * 32
            assert derive_ref(target, prefix="curie", slot=SLOT).version_id == str(version)
            assert await active_winner(engine, _SCHEMA, uuid.uuid4()) is None
        finally:
            await _cleanup(engine, agent_ids)
            await engine.dispose()

    asyncio.run(go())
