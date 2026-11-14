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
    parse_probe,
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
        if plural == "sandboxtemplates":
            for key, value in list(self.objects.items()):
                if any(
                    owner["name"] == name for owner in value["metadata"].get("ownerReferences", [])
                ):
                    del self.objects[key]


class ProbeCoreAPI:
    # Kubernetes v1.31 Pod and ContainerStatus response fields:
    # https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.31/
    def __init__(self) -> None:
        self.runner_status = NS(name="runner", image_id=IMAGE, state=NS(terminated=NS(exit_code=0)))
        self.runner = NS(name="runner", security_context=NS(run_as_user=1001))
        self.pod = NS(
            status=NS(
                container_statuses=[
                    NS(name="sidecar", image_id="invalid", state=NS(terminated=NS(exit_code=1))),
                    self.runner_status,
                ]
            ),
            spec=NS(
                containers=[NS(name="sidecar"), self.runner],
                security_context=NS(run_as_user=1000),
                node_name="acme-node",
            ),
        )
        self.output = "platform_os=Linux\nplatform_arch=x86_64\n" + "".join(
            f"{name}={int(name in {'sh', 'git'})}\n" for name in CATALOG
        )

    def read_namespaced_pod(self, name: str, namespace: str, **kwargs: Any) -> Any:
        return self.pod

    def read_node(self, name: str, **kwargs: Any) -> Any:
        raise AssertionError("the observer must not read nodes")

    def read_namespaced_pod_log(
        self, name: str, namespace: str, *, container: str, **kwargs: Any
    ) -> str:
        assert container == "runner"
        return self.output


@pytest.mark.parametrize(
    "variant", ["container_user", "pod_user", "arm64", "containerd", "unscheduled"]
)
def test_real_client_reads_the_runner_container_result(variant: str) -> None:
    client, _ = cluster_client()
    core = ProbeCoreAPI()
    client._core_api = core  # type: ignore[assignment]
    if variant == "pod_user":
        core.runner.security_context = None
    elif variant == "arm64":
        core.output = core.output.replace("x86_64", "aarch64")
    elif variant == "containerd":
        core.runner_status.image_id = "containerd://sha256:" + "a" * 64
    elif variant == "unscheduled":
        core.pod.spec.node_name = None
    result = client.read_tool_probe("acme-probe", deadline=time.monotonic() + 5)
    assert result is not None
    assert result.present == frozenset({"sh", "git"})
    assert result.image_digest == "sha256:" + "a" * 64
    assert result.user_id == (1000 if variant == "pod_user" else 1001)
    assert result.platform == ("linux/arm64" if variant == "arm64" else "linux/amd64")


@pytest.mark.parametrize(
    "invalid", ["exit", "user", "platform", "os", "architecture", "image", "log"]
)
def test_real_client_rejects_unreadable_runner_results(invalid: str) -> None:
    client, _ = cluster_client()
    core = ProbeCoreAPI()
    client._core_api = core  # type: ignore[assignment]
    if invalid == "exit":
        core.runner_status.state.terminated.exit_code = 1
    elif invalid == "user":
        core.runner.security_context = None
        core.pod.spec.security_context = None
    elif invalid == "platform":
        core.output = "\n".join(core.output.splitlines()[2:]) + "\n"
    elif invalid == "os":
        core.output = core.output.replace("Linux", "Windows")
    elif invalid == "architecture":
        core.output = core.output.replace("x86_64", "riscv64")
    elif invalid == "image":
        core.runner_status.image_id = "registry.example.com/acme-runner:latest"
    else:
        core.output = "git=1\n"
    with pytest.raises(ValueError):
        client.read_tool_probe("acme-probe", deadline=time.monotonic() + 5)


def test_real_client_waits_for_a_running_probe() -> None:
    client, _ = cluster_client()
    core = ProbeCoreAPI()
    client._core_api = core  # type: ignore[assignment]
    core.runner_status.state.terminated = None
    assert client.read_tool_probe("acme-probe", deadline=time.monotonic() + 5) is None


def cluster_client() -> tuple[KubernetesSandboxClient, ClusterAPI]:
    client = KubernetesSandboxClient.__new__(KubernetesSandboxClient)
    api = ClusterAPI()
    client._api = api  # type: ignore[assignment]
    client._namespace = "acme"
    client._tool_probe_sandboxes = {}
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
    uname = tmp_path / "uname"
    uname.write_text(
        "#!/bin/sh\ncase $1 in -s) printf 'Linux\\n';; -m) printf 'x86_64\\n';; *) exit 1;; esac\n"
    )
    uname.chmod(0o755)
    result = subprocess.run(
        ["/bin/sh", "-c", probe_script()],
        env={"PATH": str(tmp_path)},
        text=True,
        capture_output=True,
        check=True,
    )
    assert result.stdout.splitlines() == [
        "platform_os=Linux",
        "platform_arch=x86_64",
        *[f"{name}=1" for name in CATALOG],
    ]
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
    uname.unlink()
    result = subprocess.run(
        ["/bin/sh", "-c", probe_script()],
        env={"PATH": str(tmp_path)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode != 0


@pytest.mark.parametrize(
    ("machine", "platform"), [("x86_64", "linux/amd64"), ("aarch64", "linux/arm64")]
)
def test_probe_parses_the_process_uname_platform(machine: str, platform: str) -> None:
    tools = "".join(f"{name}={int(name in {'sh', 'git'})}\n" for name in CATALOG)
    output = f"platform_os=Linux\nplatform_arch={machine}\n" + tools
    assert parse_probe(output) == (frozenset({"sh", "git"}), platform)


@pytest.mark.parametrize(
    "platform_lines",
    [
        "",
        "platform_os=Linux\n",
        "platform_os=\nplatform_arch=x86_64\n",
        "platform_os=Linux\nplatform_arch=\n",
        "platform_os=Darwin\nplatform_arch=x86_64\n",
        "platform_os=Linux\nplatform_arch=riscv64\n",
    ],
)
def test_probe_rejects_missing_or_unsupported_uname_platform(platform_lines: str) -> None:
    output = platform_lines + "".join(f"{name}=0\n" for name in CATALOG)
    with pytest.raises(ValueError):
        parse_probe(output)


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
        self.release_confirmed = True

    def warm_pool_exists(self, pool: str) -> bool:
        return self.pool_present and "-agent-" not in pool

    def inspect_tool_probe(self, pool: str, *, deadline: float) -> ProbeSelection:
        return ProbeSelection(
            pool, "acme-selected", copy.deepcopy(self.spec), digest([pool, self.spec]), frozenset()
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
        if not self.release_confirmed:
            return False
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
            SubstrateConfig(
                namespace="acme", warm_pool=POOL, platform_runner_digest="sha256:" + "a" * 64
            ),  # type: ignore[arg-type]
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


@pytest.mark.parametrize("secrets", [None, {}])
def test_tick_observes_agents_with_api_serialized_secrets(
    observation_world: Any, secrets: dict[str, str] | None
) -> None:
    from sqlalchemy import text

    world = observation_world
    client = ProbeClient()

    async def check() -> None:
        async with world.engine.begin() as conn:
            await conn.execute(
                text("UPDATE curie.agents SET secrets=CAST(:secrets AS jsonb) WHERE id=:id"),
                {"secrets": json.dumps(secrets), "id": world.agent},
            )
        assert await observer(world, client).tick() == 1
        rows = await observation_rows(world)
        assert len(rows) == len(CATALOG)
        assert {row["state"] for row in rows} == {"present", "absent"}
        assert len(client.created) == 1 and client.released == client.created

    asyncio.run(check())


def test_tick_preserves_expiry_until_a_fresh_probe(
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
        assert all(row["expires_at"] == now + timedelta(seconds=900) for row in refreshed)
        assert len(client.created) == 1 and client.released == client.created
        await loop.tick(now=now + timedelta(seconds=599))
        assert len(client.created) == 1

        def assert_previous_observations_stay_readable() -> None:
            async def read() -> None:
                previous = await observation_rows(world)
                assert len(previous) == len(CATALOG)
                assert {row["state"] for row in previous} == {"present", "absent"}
                assert all(row["expires_at"] == now + timedelta(seconds=900) for row in previous)

            asyncio.run(read())

        client.before_probe = assert_previous_observations_stay_readable
        await loop.tick(now=now + timedelta(seconds=600))
        reprobed = await observation_rows(world)
        assert all(row["observed_at"] == now + timedelta(seconds=600) for row in reprobed)
        assert all(row["expires_at"] == now + timedelta(seconds=1500) for row in reprobed)
        assert len(client.created) == 2 and client.released == client.created
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
        client.spec["podTemplate"]["spec"]["containers"][0]["image"] = (
            "registry.example.com/acme-other-runner@sha256:" + "a" * 64
        )
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
            Subject(world.agent, "acme", "other", world.repository, "first", ()),
            Subject(world.agent, "acme", world.scope, world.repository, "second", ()),
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
        assert all(row["state"] == "unknown" and row["reason"] == "unreadable" for row in rows)
        assert git["state"] == "unknown"
        assert client.created == []

    asyncio.run(check())


def test_runtime_digest_must_match_the_deployments_declared_digest(
    observation_world: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    world = observation_world
    client = ProbeClient()
    core = ProbeCoreAPI()
    core.runner_status.image_id = "registry.example.com/acme-runner@sha256:" + "b" * 64
    client._core_api = core  # type: ignore[assignment]
    client._namespace = "acme"
    monkeypatch.setattr(
        client, "read_tool_probe", KubernetesSandboxClient.read_tool_probe.__get__(client)
    )
    loop = observer(world, client)

    async def check() -> None:
        await loop.tick()
        rows = await observation_rows(world)
        assert len(rows) == len(CATALOG)
        assert all(row["state"] == "unknown" and row["reason"] == "unreadable" for row in rows)
        assert len(client.created) == 1 and client.created == client.released
        core.runner_status.image_id = IMAGE
        await loop.tick()
        assert {row["state"] for row in await observation_rows(world)} == {"present", "absent"}

    asyncio.run(check())


@pytest.mark.parametrize("invalid", ["missing", "os", "architecture"])
def test_unreadable_process_platform_writes_only_unknown_rows(
    observation_world: Any, monkeypatch: pytest.MonkeyPatch, invalid: str
) -> None:
    world = observation_world
    client = ProbeClient()
    core = ProbeCoreAPI()
    if invalid == "missing":
        core.output = "\n".join(core.output.splitlines()[2:]) + "\n"
    elif invalid == "os":
        core.output = core.output.replace("Linux", "Darwin")
    else:
        core.output = core.output.replace("x86_64", "riscv64")
    client._core_api = core  # type: ignore[assignment]
    client._namespace = "acme"
    monkeypatch.setattr(
        client, "read_tool_probe", KubernetesSandboxClient.read_tool_probe.__get__(client)
    )

    async def check() -> None:
        await observer(world, client).tick()
        rows = await observation_rows(world)
        assert len(rows) == len(CATALOG)
        assert all(row["state"] == "unknown" and row["reason"] == "unreadable" for row in rows)
        assert len(client.created) == 1 and client.created == client.released

    asyncio.run(check())


def test_platform_image_needs_the_worker_configuration_declaration(observation_world: Any) -> None:
    from dataclasses import replace

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)
    loop._substrate._config = replace(loop._substrate._config, platform_runner_digest=None)

    async def check() -> None:
        await loop.tick()
        rows = await observation_rows(world)
        assert all(row["state"] == "unknown" and row["reason"] == "unreadable" for row in rows)
        assert not client.created

    asyncio.run(check())


def test_cancellation_keeps_tick_lock_until_probe_cleanup(observation_world: Any) -> None:
    import threading

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)
    entered, release = threading.Event(), threading.Event()

    def hold_probe() -> None:
        entered.set()
        if not release.wait(5):
            raise TimeoutError("observation test did not release probe")

    client.before_probe = hold_probe

    async def check() -> None:
        running = asyncio.create_task(loop.tick())
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            running.cancel()
            assert await observer(world, ProbeClient()).tick() == 0
        finally:
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await running
        assert len(client.created) == 1 and client.created == client.released
        assert not client.claims

    asyncio.run(check())


def test_unconfirmed_release_prevents_another_probe(observation_world: Any) -> None:
    world = observation_world
    client = ProbeClient()
    client.release_confirmed = False
    loop = observer(world, client)

    async def check() -> None:
        await loop.tick()
        assert len(client.created) == 1
        await loop.tick()
        assert len(client.created) == 1
        assert all(row["state"] == "unknown" for row in await observation_rows(world))
        client.release_confirmed = True
        await loop.tick()
        assert len(client.created) == 2 and client.created == client.released
        assert not client.claims

    asyncio.run(check())


def test_docker_service_uses_the_same_repository_daemon_configuration(
    observation_world: Any,
) -> None:
    from dataclasses import replace

    from curie_worker.repo_runner_pools import RepoRunnerPool

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)
    daemon = json.loads(
        (Path(__file__).resolve().parents[3] / "tests/vectors/daemon-config.json").read_text()
    )["daemon"]

    async def check() -> None:
        for enabled in (True, False):
            entry = RepoRunnerPool.model_validate(
                {
                    "pool": "acme-repo-runner",
                    "digests": ["sha256:" + c * 64 for c in "abc"],
                    "base": "sha256:" + "a" * 64,
                    "docker": enabled,
                    **({"daemon": daemon} if enabled else {}),
                }
            )
            loop._substrate._config = replace(
                loop._substrate._config, repo_runner_pools={world.repository: entry}
            )
            await loop.tick()
            rows = await observation_rows(world)
            services = [row for row in rows if row["item"] == "service:docker"]
            assert len(services) == int(enabled)
            if enabled:
                assert services[0]["state"] == "configured"
            assert not client.claims

    asyncio.run(check())


def test_capacity_refusal_leaves_no_positive_observation(observation_world: Any) -> None:
    from curie_worker.sandbox.types import CapacityExhaustedError, QuotaRejection

    world = observation_world
    client = ProbeClient()
    client.failure = CapacityExhaustedError(
        QuotaRejection(
            quota_name="acme-quota",
            requested={"pods": "1"},
            used={"pods": "1"},
            hard={"pods": "1"},
        )
    )
    loop = observer(world, client)

    async def check() -> None:
        await loop.tick()
        assert await observation_rows(world) == []
        assert client.created == client.released
        client.failure = None
        await loop.tick()
        assert {row["state"] for row in await observation_rows(world)} == {"present", "absent"}

    asyncio.run(check())


def test_probe_deadline_leaves_cleanup_budget_and_unknown_rows(
    observation_world: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import curie_worker.sandbox.substrate as substrate

    world = observation_world
    client = ProbeClient()
    clock = [0.0]
    monkeypatch.setattr(substrate, "_probe_monotonic", lambda: clock[0])

    def never_complete(sandbox: str, *, deadline: float) -> None:
        assert deadline == 90
        clock[0] = 91

    client.read_tool_probe = never_complete
    loop = observer(world, client)

    async def check() -> None:
        await loop.tick()
        rows = await observation_rows(world)
        assert len(rows) == len(CATALOG)
        assert all(row["state"] == "unknown" and row["reason"] == "unreadable" for row in rows)
        assert len(client.created) == 1 and client.created == client.released
        assert not client.claims

    asyncio.run(check())


def test_observer_keeps_the_real_claims_connector_pool_refusal(
    observation_world: Any,
    caplog: Any,
) -> None:
    from dataclasses import replace

    from sqlalchemy import text

    world = observation_world
    client = ProbeClient()
    loop = observer(world, client)

    async def check() -> None:
        await loop.tick()
        async with world.engine.begin() as conn:
            await conn.execute(
                text("UPDATE curie.agents SET secrets=CAST(:secrets AS jsonb) WHERE id=:id"),
                {"secrets": json.dumps({"ACME_TOKEN": SENTINEL}), "id": world.agent},
            )
            name = await conn.scalar(
                text("SELECT name FROM curie.agents WHERE id=:id"), {"id": world.agent}
            )
        await loop.tick()
        rows = await observation_rows(world)
        assert all(
            row["state"] == "unknown" and row["reason"] == "runner_unavailable" for row in rows
        )
        assert len(client.created) == 1
        loop._substrate._config = replace(
            loop._substrate._config, connector_secret_pools=frozenset({name})
        )
        await loop.tick()
        assert {row["state"] for row in await observation_rows(world)} == {"present", "absent"}
        assert len(client.created) == 2
        assert SENTINEL not in str(await observation_rows(world)) + caplog.text

    asyncio.run(check())
