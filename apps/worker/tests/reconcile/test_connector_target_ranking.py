"""Which version the connector reconciler picks, against the REAL Postgres (#1216).

`connector_loop._TARGETS_SQL` and `binding._RESOLVE_SQL` must choose the SAME
version for an agent: the connectors a thread gets have to belong to the version
its sandbox actually boots. That agreement is a property of two SQL statements,
so it is asserted against a real database rather than a fake -- a mock cannot
disagree about `DISTINCT ON`, an enum comparison, or a NULL.

The bug this file exists to pin: `_TARGETS_SQL` used to filter
`WHERE v.bundle_ref IS NOT NULL` BEFORE ranking. When the true winner was a
bundleless version its row vanished and the runner-up -- a version the sandbox
never boots -- was silently promoted, its connector objects applied, and nothing
ever converged them away. Rank first, decide second.

Seeding follows `apps/worker/tests/binding/test_resolver.py`'s idiom (its
`_seed_agent` / `_seed_deployment`, the per-test uuid token, the explicit
cleanup). The helpers are local copies rather than an import: this file needs a
NULL `bundle_ref` and an explicit `deployed_at`, neither of which the resolver
test has any use for, and widening a shared helper for a second caller's edge
case is how a fixture stops describing either.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from curie_worker import binding, connector_loop
from curie_worker.connector_agent import RenderedConnectors
from curie_worker.connector_loop import ConnectorReconcileLoop
from curie_worker.connector_reconcile import OWNER_LABEL
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from .test_connector_agent import FakeClient, live_copy, manifest

_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")

NS = "curie"


async def _seed_agent(engine: AsyncEngine, *, name: str, address: str) -> uuid.UUID:
    """One agent and its single channel binding (ADR-0096, #1459).

    The binding is seeded even though `_TARGETS_SQL` does not join
    `agent_channels`, because the pinning test drives `_RESOLVE_SQL` -- which
    does -- over the same rows. Two agents seeded differently would not be
    comparing the two queries at all.
    """
    agent_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(f"INSERT INTO {_SCHEMA}.agents (id, name) VALUES (:id, :name)"),
            {"id": agent_id, "name": name},
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agent_channels (id, agent_id, kind, address, adapter) "
                "VALUES (:id, :agent_id, 'slack', :address, 'default')"
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
    deployed_seconds_ago: int,
) -> uuid.UUID:
    """One version plus the active deployment pointing at it; returns the version id.

    `bundle_ref` is nullable on purpose: a version created before its bundle is
    stored is a state the real endpoints produce, and
    `apps/api/tests/test_bundle_ingestion_bounds.py` pins that deploying it is
    allowed. `deployed_seconds_ago` is explicit because the whole precedence
    question is prod-versus-newer, which a `now()` default cannot express.
    """
    version_id = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agent_versions "
                "(id, agent_id, version_label, bundle_ref, created_by) "
                "VALUES (:id, :agent_id, :label, :ref, 'test')"
            ),
            {
                "id": version_id,
                "agent_id": agent_id,
                "label": f"v-{environment}",
                "ref": bundle_ref,
            },
        )
        await conn.execute(
            text(
                f"INSERT INTO {_SCHEMA}.deployments "
                "(id, agent_id, version_id, environment, status, deployed_at) "
                f"VALUES (:id, :agent_id, :version_id, CAST(:env AS {_SCHEMA}.environment), "
                # The timestamp is computed in Python: `now() - :param` leaves
                # the parameter's type indeterminate, so Postgres infers
                # timestamptz and the subtraction yields an interval.
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
                text(f"DELETE FROM {_SCHEMA}.agent_channels WHERE agent_id = :id"),
                {"id": agent_id},
            )
            await conn.execute(
                text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"), {"id": agent_id}
            )


async def _engine_or_skip() -> AsyncEngine:
    engine = create_async_engine(_DB_URL)
    try:
        async with engine.connect():
            pass
    except SQLAlchemyError as exc:
        await engine.dispose()
        pytest.skip(f"Postgres not reachable at {_DB_URL}: {exc}")
    return engine


class RefusesForAgent:
    """A ManifestSource that refuses to render ANY version of one agent.

    Keyed on the agent, not on the bundleless version: refusing only that one
    version would let the #1216 bug through, because the bug does not render the
    in-force version -- it renders the RUNNER-UP. A pass sweeps every agent in
    the shared database, so the rest are answered with an empty render rather
    than a raise.
    """

    def __init__(self, agent_id: uuid.UUID) -> None:
        self._forbidden = str(agent_id)
        self.rendered_versions: list[str] = []

    def rendered(self, *, agent_id: str, version_id: str) -> RenderedConnectors:
        self.rendered_versions.append(version_id)
        if agent_id == self._forbidden:
            raise AssertionError(
                f"rendered version {version_id} for an agent whose in-force "
                "version has no bundle; the render endpoint 404s and the "
                "reconciler must not ask"
            )
        return RenderedConnectors()


def test_the_prod_winner_is_reported_even_when_it_has_no_bundle() -> None:
    """Rank first, decide second.

    Prod is bundleless, dev is bundled and deployed LATER, so the two orderings
    disagree and the seeding cannot accidentally pass. Reinstating
    filter-then-rank returns the DEV version here and this goes red.
    """

    async def go() -> None:
        engine = await _engine_or_skip()
        try:
            token = uuid.uuid4().hex[:8]
            agent_id = await _seed_agent(
                engine, name=f"rank-{token}", address=f"C-rank-{token}"
            )
            prod_version = await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="prod",
                bundle_ref=None,
                deployed_seconds_ago=600,
            )
            dev_version = await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="dev",
                bundle_ref=f"bundles/{token}.zip",
                deployed_seconds_ago=60,
            )
            try:
                loop = ConnectorReconcileLoop(
                    engine=engine,
                    source=RefusesForAgent(agent_id),
                    client=FakeClient(),
                    namespace=NS,
                    db_schema=_SCHEMA,
                )
                mine = [t for t in await loop.targets() if t.agent_id == agent_id]

                assert len(mine) == 1, "DISTINCT ON must collapse dev+prod to one winner"
                assert mine[0].version_id == prod_version, (
                    "the reconciler picked a version the sandbox will not boot; "
                    f"expected the prod winner {prod_version}, got {mine[0].version_id} "
                    f"(the dev version is {dev_version})"
                )
                assert mine[0].has_bundle is False
            finally:
                await _cleanup(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_a_pass_prunes_rather_than_reconciling_a_lower_precedence_version() -> None:
    """The behavioural AC, end to end through `one_pass`.

    Same seeded state, a real database, a source that fails the test if asked to
    render the in-force version, and a cluster already holding two objects this
    agent owns. The pass must render nothing for this agent and remove what it
    owns, because the version in force declares no connectors we can reach.
    """

    async def go() -> None:
        engine = await _engine_or_skip()
        try:
            token = uuid.uuid4().hex[:8]
            agent_name = f"prune-{token}"
            agent_id = await _seed_agent(
                engine, name=agent_name, address=f"C-prune-{token}"
            )
            prod_version = await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="prod",
                bundle_ref=None,
                deployed_seconds_ago=600,
            )
            dev_version = await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="dev",
                bundle_ref=f"bundles/{token}.zip",
                deployed_seconds_ago=60,
            )
            try:
                client = FakeClient(
                    [
                        live_copy(manifest("Service", f"svc-{token}"), agent=agent_name),
                        live_copy(
                            manifest("Deployment", f"dep-{token}"), agent=agent_name
                        ),
                    ]
                )
                source = RefusesForAgent(agent_id)
                loop = ConnectorReconcileLoop(
                    engine=engine,
                    source=source,
                    client=client,
                    namespace=NS,
                    db_schema=_SCHEMA,
                )

                await loop.one_pass()

                assert str(prod_version) not in source.rendered_versions
                assert str(dev_version) not in source.rendered_versions, (
                    "the runner-up was rendered: the pass is reconciling a "
                    "version this agent's sandbox does not boot"
                )
                assert client.applied == [], (
                    "objects were applied from a version no sandbox boots"
                )
                assert sorted(client.deleted) == [
                    ("Deployment", f"dep-{token}"),
                    ("Service", f"svc-{token}"),
                ]
            finally:
                await _cleanup(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_the_two_queries_agree_on_the_winner() -> None:
    """Pinning test (#1216 part 2), the shape `binding.py` already uses for
    `_PERMISSION_GATE_SUMMARY_PREFIX` and `_RESUME_EVENT_ID_RE`: a duplicated
    literal that two modules must keep in agreement gets a test whose only job
    is to fail when they diverge.

    Here the duplicated literal is the ORDER BY. Both deployments are bundled,
    so nothing but precedence can separate them, and dev is the newer one so
    prod-first and most-recent-first disagree.

    Inverting EITHER query's ORDER BY must turn this red. That is the whole
    point: `_TARGETS_SQL` has no other test that can see its ordering, which is
    how it drifted away from `_RESOLVE_SQL` unnoticed in the first place.

    This test pins only the ENVIRONMENT key -- the environment expression
    decides the winner here before `deployed_at` is ever consulted, so flipping
    the recency key alone leaves it green;
    `test_the_two_queries_agree_on_the_more_recent_of_two_prod_deployments` is
    the other half of the clause.
    """

    async def go() -> None:
        engine = await _engine_or_skip()
        try:
            token = uuid.uuid4().hex[:8]
            address = f"C-pin-{token}"
            agent_id = await _seed_agent(engine, name=f"pin-{token}", address=address)
            await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="prod",
                bundle_ref=f"bundles/{token}-prod.zip",
                deployed_seconds_ago=600,
            )
            await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="dev",
                bundle_ref=f"bundles/{token}-dev.zip",
                deployed_seconds_ago=60,
            )
            try:
                resolve_sql = text(binding._RESOLVE_SQL.format(schema=_SCHEMA))
                targets_sql = text(connector_loop._TARGETS_SQL.format(schema=_SCHEMA))
                async with engine.connect() as conn:
                    bound: Any = (
                        (
                            await conn.execute(
                                resolve_sql, {"kind": "slack", "address": address}
                            )
                        )
                        .mappings()
                        .first()
                    )
                    targets = (await conn.execute(targets_sql)).mappings().all()

                assert bound is not None
                mine = [row for row in targets if row["agent_id"] == agent_id]
                assert len(mine) == 1
                assert mine[0]["version_id"] == bound["version_id"], (
                    "the reconciler and the binding resolver disagree about which "
                    "version is in force; the connectors an agent gets would come "
                    "from a version its sandbox does not boot"
                )
            finally:
                await _cleanup(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_the_two_queries_agree_on_the_more_recent_of_two_prod_deployments() -> None:
    """The RECENCY half of AC 2, and the reason it needs its own test.

    Its sibling `test_the_two_queries_agree_on_the_winner` seeds prod against
    dev, so `(d.environment = 'prod') DESC` settles the winner before
    `d.deployed_at DESC` is read at all: flip the recency key there and both
    queries still answer prod, and the test stays green. Here BOTH deployments
    are prod and both are bundled, so the environment key ties and recency is
    the only thing left that can separate them -- flipping `d.deployed_at DESC`
    to `ASC` in either query turns this red. Neither ORDER BY key is defended
    without both tests.

    The two deployments also exercise the tie-break's reason for existing: with
    the environment expression tied, only `deployed_at` (and behind it the
    total-order key `d.id DESC`) keeps the two statements from disagreeing, and
    a disagreement now costs a destructive prune.
    """

    async def go() -> None:
        engine = await _engine_or_skip()
        try:
            token = uuid.uuid4().hex[:8]
            address = f"C-recency-{token}"
            agent_id = await _seed_agent(
                engine, name=f"recency-{token}", address=address
            )
            older_version = await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="prod",
                bundle_ref=f"bundles/{token}-older.zip",
                deployed_seconds_ago=600,
            )
            newer_version = await _seed_deployment(
                engine,
                agent_id=agent_id,
                environment="prod",
                bundle_ref=f"bundles/{token}-newer.zip",
                deployed_seconds_ago=60,
            )
            try:
                resolve_sql = text(binding._RESOLVE_SQL.format(schema=_SCHEMA))
                targets_sql = text(connector_loop._TARGETS_SQL.format(schema=_SCHEMA))
                async with engine.connect() as conn:
                    bound: Any = (
                        (
                            await conn.execute(
                                resolve_sql, {"kind": "slack", "address": address}
                            )
                        )
                        .mappings()
                        .first()
                    )
                    targets = (await conn.execute(targets_sql)).mappings().all()

                assert bound is not None
                mine = [row for row in targets if row["agent_id"] == agent_id]
                assert len(mine) == 1, "DISTINCT ON must collapse two prod rows to one"
                assert bound["version_id"] == newer_version, (
                    "the binding resolver picked the OLDER of two prod deployments; "
                    f"expected {newer_version}, got {bound['version_id']} "
                    f"(the older version is {older_version})"
                )
                assert mine[0]["version_id"] == newer_version, (
                    "the reconciler picked the OLDER of two prod deployments; its "
                    "connector objects would come from a version no sandbox boots; "
                    f"expected {newer_version}, got {mine[0]['version_id']}"
                )
                assert mine[0]["version_id"] == bound["version_id"]
            finally:
                await _cleanup(engine, [agent_id])
        finally:
            await engine.dispose()

    asyncio.run(go())


def test_the_seeded_objects_are_owned_by_the_agent_under_test() -> None:
    """Guard on the guard: the prune assertions above are only meaningful if the
    fake cluster reports those objects as this agent's. An owner label that did
    not match would make `list_owned` return nothing and the delete assertions
    would pass for the wrong reason.
    """

    agent = "guard-agent"
    obj = live_copy(manifest("Service", "svc"), agent=agent)
    assert obj["metadata"]["labels"][OWNER_LABEL] == agent
    assert FakeClient([obj]).list_owned(NS, agent) == [obj]
