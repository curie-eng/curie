"""Tool observation through the sandbox client and real Postgres boundaries."""

from __future__ import annotations

import asyncio
import copy
import json
import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Any

import pytest
from curie_worker.sandbox.k8s import KubernetesSandboxClient
from curie_worker.sandbox.tool_probe import (
    CATALOG,
    PROBE_PREFIX,
    ProbeSelection,
    digest,
    probe_script,
)

IMAGE = "registry.example.com/acme-runner@sha256:" + "a" * 64
POOL = "acme-selected-runner-pool"
SENTINEL = "DO_NOT_REPORT_observer_37e1"


def source_template() -> dict[str, Any]:
    return {
        "podTemplate": {
            "spec": {
                "securityContext": {"runAsUser": 1000},
                "serviceAccountName": "acme-runner",
                "initContainers": [
                    {"name": "bundle-fetch", "env": [{"name": "secret", "value": SENTINEL}]}
                ],
                "volumes": [{"name": "credential", "secret": {"secretName": SENTINEL}}],
                "containers": [
                    {
                        "name": "runner",
                        "image": IMAGE,
                        "env": [{"name": "ANTHROPIC_API_KEY", "value": SENTINEL}],
                        "envFrom": [{"secretRef": {"name": SENTINEL}}],
                        "volumeMounts": [{"name": "credential", "mountPath": "/credentials"}],
                        "readinessProbe": {"httpGet": {"path": "/health", "port": 8080}},
                        "securityContext": {"readOnlyRootFilesystem": True},
                    }
                ],
            }
        }
    }


class ClusterAPI:
    """Kubernetes custom-object REST responses, from the vendored CRD schemas."""

    def __init__(self) -> None:
        self.spec = source_template()
        self.objects: dict[tuple[str, str], dict[str, Any]] = {}
        self.created: list[tuple[str, dict[str, Any]]] = []

    def get_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str, **kwargs: Any
    ) -> Any:
        from kubernetes.client import ApiException

        if plural == "sandboxwarmpools" and name == POOL:
            return {"spec": {"sandboxTemplateRef": {"name": "acme-selected"}}}
        if plural == "sandboxtemplates" and name == "acme-selected":
            return {"spec": copy.deepcopy(self.spec)}
        if (plural, name) not in self.objects:
            raise ApiException(status=404)
        return copy.deepcopy(self.objects[plural, name])

    def create_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, body: Any, **kwargs: Any
    ) -> Any:
        body = copy.deepcopy(body)
        body["metadata"]["uid"] = "example-uid"
        self.created.append((plural, body))
        self.objects[plural, body["metadata"]["name"]] = body
        return body

    def patch_namespaced_custom_object(
        self,
        group: str,
        version: str,
        namespace: str,
        plural: str,
        name: str,
        body: Any,
        **kwargs: Any,
    ) -> None:
        self.objects[plural, name]["metadata"].update(body["metadata"])

    def delete_namespaced_custom_object(
        self, group: str, version: str, namespace: str, plural: str, name: str, **kwargs: Any
    ) -> None:
        self.objects.pop((plural, name), None)


def cluster_client() -> tuple[KubernetesSandboxClient, ClusterAPI]:
    client = KubernetesSandboxClient.__new__(KubernetesSandboxClient)
    api = ClusterAPI()
    client._api = api  # type: ignore[assignment]
    client._namespace = "acme"
    return client, api


def test_real_client_copies_selected_image_without_credentials_or_workspace() -> None:
    client, api = cluster_client()
    deadline = time.monotonic() + 120
    selection = client.inspect_tool_probe(POOL, deadline=deadline)
    claim = PROBE_PREFIX + "example"
    client.create_tool_probe(claim, selection, deadline=deadline)
    template = next(body for kind, body in api.created if kind == "sandboxtemplates")
    pod = template["spec"]["podTemplate"]["spec"]
    assert pod["containers"][0]["image"] == IMAGE
    assert SENTINEL not in json.dumps(api.created)
    assert "initContainers" not in pod and "volumes" not in pod
    runner = pod["containers"][0]
    assert "env" not in runner and "envFrom" not in runner
    assert runner["command"] == ["/bin/sh", "-c", probe_script()]
    assert pod["securityContext"]["runAsUser"] == 1000
    assert pod["activeDeadlineSeconds"] == 120
    assert [kind for kind, _ in api.created] == [
        "sandboxtemplates",
        "sandboxwarmpools",
        "sandboxclaims",
    ]
    assert client.delete_tool_probe(claim, deadline=deadline)
    assert ("sandboxclaims", claim) not in api.objects


def test_probe_only_looks_up_catalog_and_does_not_execute_its_binaries(tmp_path: Any) -> None:
    import subprocess

    marker = tmp_path / "executed"
    for name in CATALOG:
        binary = tmp_path / name
        binary.write_text(f"#!/bin/sh\nprintf executed > '{marker}'\n")
        binary.chmod(0o755)
    result = subprocess.run(
        ["/bin/sh", "-c", probe_script()],
        env={"PATH": str(tmp_path)},
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout.splitlines() == [f"{name}=1" for name in CATALOG]
    assert not marker.exists()
    (tmp_path / "jq").unlink()
    result = subprocess.run(
        ["/bin/sh", "-c", probe_script()],
        env={"PATH": str(tmp_path)},
        text=True,
        capture_output=True,
        check=True,
    )
    assert "jq=0" in result.stdout.splitlines()
    assert not marker.exists()


@pytest.mark.parametrize("output", ["", "jq=1\n", "\udcff", SENTINEL])
def test_malformed_output_never_becomes_an_observation(output: str) -> None:
    from curie_worker.sandbox.tool_probe import parse_probe

    with pytest.raises(ValueError):
        parse_probe(output)


@pytest.fixture(scope="session")
def observer_database() -> Any:
    import asyncpg
    from alembic import command
    from alembic.config import Config
    from curie_api.config import get_settings
    from sqlalchemy import make_url

    base = make_url(
        os.environ.get(
            "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
        )
    )
    name = "curie_observer_test_" + uuid.uuid4().hex

    async def administer(statement: str) -> None:
        connection = await asyncpg.connect(
            host=base.host,
            port=base.port,
            user=base.username,
            password=base.password,
            database="postgres",
        )
        try:
            await connection.execute(statement)
        finally:
            await connection.close()

    asyncio.run(administer(f'CREATE DATABASE "{name}"'))
    previous = os.environ.get("DATABASE_URL")
    url = base.set(database=name).render_as_string(hide_password=False)
    try:
        os.environ["DATABASE_URL"] = url
        get_settings.cache_clear()
        config = Config()
        config.set_main_option(
            "script_location", str(Path(__file__).resolve().parents[3] / "apps/api/alembic")
        )
        command.upgrade(config, "head")
        yield url
    finally:
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous
        get_settings.cache_clear()
        asyncio.run(administer(f'DROP DATABASE "{name}" WITH (FORCE)'))


@pytest.fixture
def observation_world(observer_database: str) -> Any:
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(observer_database, poolclass=NullPool)
    identifier = uuid.uuid4()
    repository = f"acme-corp/acme-{identifier.hex[:8]}"

    async def seed() -> None:
        async with engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO curie.agents (id,name) VALUES (:id,:name)"),
                {"id": identifier, "name": "acme-" + identifier.hex[:8]},
            )
            await conn.execute(
                text("""
                INSERT INTO curie.agent_channels (id,agent_id,kind,address)
                VALUES (:id,:agent,'github',:repo)
            """),
                {"id": uuid.uuid4(), "agent": identifier, "repo": repository},
            )
            await conn.execute(
                text("""
                INSERT INTO curie.factory_poll_cursors
                    (tracker_kind,tracker_host,tracker_scope_id,scope_path)
                VALUES ('github','github.com',:scope,:repo)
            """),
                {"scope": str(identifier.int), "repo": repository},
            )

    async def cleanup() -> None:
        async with engine.begin() as conn:
            await conn.execute(text("DELETE FROM curie.agents WHERE id=:id"), {"id": identifier})
            await conn.execute(
                text("DELETE FROM curie.factory_poll_cursors WHERE scope_path=:repo"),
                {"repo": repository},
            )
        await engine.dispose()

    asyncio.run(seed())
    try:
        yield NS(engine=engine, agent=identifier, repository=repository, scope=str(identifier.int))
    finally:
        asyncio.run(cleanup())


class ProbeClient(KubernetesSandboxClient):
    """External claim responses follow the recorded vendored SandboxClaim boundary."""

    def __init__(self) -> None:
        self.spec = source_template()
        self.claims: dict[str, Any] = {}
        self.created: list[str] = []
        self.released: list[str] = []
        self.failure: Exception | None = None
        self.before_probe: Any = None
        self.pool_present = True
        self.runtime_digest = "sha256:" + "a" * 64
        self.runtime_present = frozenset({"sh", "git"})

    def warm_pool_exists(self, pool: str) -> bool:
        return self.pool_present

    def inspect_tool_probe(self, pool: str, *, deadline: float) -> ProbeSelection:
        return ProbeSelection(
            pool, "acme-selected", copy.deepcopy(self.spec), digest([pool, self.spec])
        )

    def create_tool_probe(self, name: str, selection: ProbeSelection, *, deadline: float) -> None:
        from curie_worker.sandbox.types import ClaimView

        if self.before_probe is not None:
            self.before_probe()
        self.created.append(name)
        self.claims[name] = ClaimView(name, True, name, datetime.now(UTC), None, None, None)
        if self.failure is not None:
            raise self.failure

    def get_claim(self, name: str, *, request_timeout_seconds: float) -> Any:
        return self.claims.get(name)

    def read_tool_probe(self, sandbox: str, *, deadline: float) -> Any:
        from curie_worker.sandbox.tool_probe import ProbeResult

        return ProbeResult(self.runtime_present, self.runtime_digest, "linux/amd64", 1000)

    def delete_tool_probe(self, name: str, *, deadline: float) -> bool:
        self.claims.pop(name, None)
        self.released.append(name)
        return True


def observer(world: Any, client: ProbeClient) -> Any:
    from curie_worker.factory_tool_observer import FactoryToolObserver
    from curie_worker.sandbox import SandboxSubstrate, SubstrateConfig

    return FactoryToolObserver(
        engine=world.engine,
        substrate=SandboxSubstrate(
            client,
            None,
            SubstrateConfig(namespace="acme", warm_pool=POOL),  # type: ignore[arg-type]
        ),
        db_schema="curie",
    )


async def observation_rows(world: Any) -> list[Any]:
    from sqlalchemy import text

    async with world.engine.connect() as conn:
        return list(
            (
                await conn.execute(
                    text(
                        "SELECT * FROM curie.factory_fact_observations "
                        "WHERE agent_id=:id ORDER BY item"
                    ),
                    {"id": world.agent},
                )
            ).mappings()
        )


def test_tick_writes_every_name_then_refreshes_without_another_claim(
    observation_world: Any,
) -> None:
    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)
    now = datetime.now(UTC)

    async def check() -> None:
        assert await loop.tick(now=now) == 1
        rows = await observation_rows(world)
        assert len(rows) == len(CATALOG)
        assert {row["item"] for row in rows} == set(CATALOG)
        assert {row["item"] for row in rows if row["state"] == "present"} == {"sh", "git"}
        assert {row["state"] for row in rows} == {"present", "absent"}
        assert all(row["expires_at"] == now + timedelta(seconds=900) for row in rows)
        assert await loop.tick(now=now + timedelta(seconds=300)) == 1
        refreshed = await observation_rows(world)
        assert all(row["observed_at"] == now for row in refreshed)
        assert all(row["expires_at"] == now + timedelta(seconds=1200) for row in refreshed)
        assert len(client.created) == 1 and client.released == client.created
        assert not client.claims

    asyncio.run(check())


@pytest.mark.parametrize("change", ["image", "user", "catalog", "configuration"])
def test_changed_identity_deletes_rows_before_failed_probe(
    observation_world: Any,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    import curie_worker.factory_tool_observer as module
    from sqlalchemy import text

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)
    asyncio.run(loop.tick())
    if change == "image":
        client.spec["podTemplate"]["spec"]["containers"][0]["image"] += "-changed"
    elif change == "user":
        client.spec["podTemplate"]["spec"]["securityContext"]["runAsUser"] = 1001
    elif change == "catalog":
        monkeypatch.setattr(module, "CATALOG_VERSION", 2)
    else:

        async def rotate() -> None:
            async with world.engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.agent_channels SET generation=generation+1 WHERE agent_id=:id"
                    ),
                    {"id": world.agent},
                )

        asyncio.run(rotate())
    client.failure = TimeoutError(SENTINEL)

    def assert_deleted() -> None:
        # A separate connection observes the committed deletion before the external probe.
        async def read() -> None:
            assert await observation_rows(world) == []

        asyncio.run(read())

    client.before_probe = assert_deleted

    async def check() -> None:
        await loop.tick()
        rows = await observation_rows(world)
        assert len(rows) == len(CATALOG)
        assert all(row["state"] == "unknown" and row["reason"] == "unreadable" for row in rows)
        assert len(client.created) == 2 and client.created == client.released
        assert not client.claims

    asyncio.run(check())


@pytest.mark.parametrize("reason", ["missing_pool", "stale_base"])
def test_selection_refusals_replace_prior_results_with_unknown(
    observation_world: Any,
    reason: str,
    caplog: Any,
) -> None:
    from dataclasses import replace

    from curie_worker.repo_runner_pools import RepoRunnerPool

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)
    asyncio.run(loop.tick())
    entry = RepoRunnerPool.model_validate(
        {
            "pool": "acme-" + SENTINEL.lower().replace("_", "-"),
            "digests": ["sha256:" + c * 64 for c in "abc"],
            "base": "sha256:" + "d" * 64,
            "docker": False,
        }
    )
    loop._substrate._config = replace(
        loop._substrate._config,
        repo_runner_pools={world.repository: entry},
        platform_runner_digest=entry.base if reason == "missing_pool" else "sha256:" + "e" * 64,
    )
    client.pool_present = False

    async def check() -> None:
        await loop.tick()
        rows = await observation_rows(world)
        assert len(rows) == len(CATALOG)
        assert all(
            row["state"] == "unknown" and row["reason"] == "runner_unavailable" for row in rows
        )
        assert len(client.created) == 1
        assert SENTINEL not in str(rows) + caplog.text
        assert entry.pool not in str(rows) + caplog.text
        assert entry.base not in str(rows) + caplog.text

    asyncio.run(check())


def test_second_replica_cannot_probe_while_tick_lock_is_held(observation_world: Any) -> None:
    from curie_worker.factory_tool_observer import _LOCK
    from sqlalchemy import text

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)

    async def check() -> None:
        async with world.engine.connect() as holder:
            await holder.execute(text("SELECT pg_advisory_lock(:key)"), {"key": _LOCK})
            try:
                assert await loop.tick() == 0
                assert not client.created and not await observation_rows(world)
            finally:
                await holder.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _LOCK})
        assert await loop.tick() == 1
        assert len(client.created) == 1

    asyncio.run(check())


def test_one_failed_subject_does_not_prevent_other_subject_observations(
    observation_world: Any,
    caplog: Any,
) -> None:
    from curie_worker.factory_tool_observer import Subject

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)

    async def targets(conn: Any) -> list[Any]:
        return [
            Subject(world.agent, "acme", "other", world.repository, "first"),
            Subject(world.agent, "acme", world.scope, world.repository, "second"),
        ]

    loop.subjects = targets
    client.failure = RuntimeError(SENTINEL)

    def next_subject() -> None:
        if client.created:
            client.failure = None

    client.before_probe = next_subject

    async def check() -> None:
        assert await loop.tick() == 2
        rows = await observation_rows(world)
        first = [row for row in rows if row["scope"] == "other"]
        second = [row for row in rows if row["scope"] == world.scope]
        assert len(first) == len(second) == len(CATALOG)
        assert all(row["state"] == "unknown" and row["reason"] == "unreadable" for row in first)
        assert {row["state"] for row in second} == {"present", "absent"}
        assert client.created == client.released
        assert SENTINEL not in str(rows) + caplog.text
        assert IMAGE not in str(rows) + caplog.text
        assert POOL not in str(rows) + caplog.text

    asyncio.run(check())


def test_stopped_observer_expires_and_no_request_extends_the_rows(observation_world: Any) -> None:
    from curie_api.factory_facts_store import read_observation
    from sqlalchemy.ext.asyncio import async_sessionmaker

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)
    now = datetime.now(UTC)

    async def check() -> None:
        await loop.tick(now=now)
        async with async_sessionmaker(world.engine)() as session:
            for seconds, expected in [(899, "present"), (900, "unknown")]:
                fact = await read_observation(
                    session,
                    kind="tool",
                    agent_id=world.agent,
                    scope=world.scope,
                    item="git",
                    now=now + timedelta(seconds=seconds),
                )
                assert fact.state == expected
                assert fact.reason == (None if seconds == 899 else "observation_expired")
        assert len(client.created) == 1

    asyncio.run(check())


def test_mutable_image_identity_change_does_not_refresh_old_positive_rows(
    observation_world: Any,
) -> None:
    world = observation_world
    client = ProbeClient()
    client.spec["podTemplate"]["spec"]["containers"][0]["image"] = (
        "registry.example.com/acme-runner:latest"
    )
    loop = observer(world, client)
    now = datetime.now(UTC)

    async def check() -> None:
        await loop.tick(now=now)
        client.runtime_digest = "sha256:" + "b" * 64
        client.runtime_present = frozenset({"sh"})
        await loop.tick(now=now + timedelta(seconds=300))
        rows = await observation_rows(world)
        git = next(row for row in rows if row["item"] == "git")
        assert git["state"] != "present", "the prior image's positive row was refreshed"

    asyncio.run(check())
