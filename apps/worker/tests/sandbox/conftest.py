"""Fixtures for the sandbox substrate tests.

Affinity tests run against the REAL Valkey from the compose stack (repo test
discipline: never mock Valkey). The Kubernetes control plane is an external
service, so substrate-logic tests use ``FakeSandboxClient`` (an in-memory model
of the agent-sandbox claim/pool behavior observed in PT-1/PT-D); the real
client is exercised by the env-gated k8scratch e2e in ``test_e2e_k8scratch.py``.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pytest
import redis
from curie_test_support.valkey import (
    VALKEY_HOST,
    VALKEY_PORT,
    VALKEY_PW,
    connect_or_skip,
)
from curie_worker.sandbox import (
    AffinityStore,
    ClaimView,
    QuotaRejection,
    SandboxView,
    SubstrateConfig,
)
from curie_worker.sandbox.docker import DockerError, DockerSandboxClient
from curie_worker.sandbox.types import SandboxTermination
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import NoBackoff
from redis.maint_notifications import MaintNotificationsConfig


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool:
    """Keep the cluster-only resilience scenario out of default collection."""

    return (
        collection_path.name == "test_e2e_resilience.py"
        and os.environ.get("CURIE_SANDBOX_E2E") != "1"
    )


class _FakeBundleStore:
    def __init__(self, data: bytes = b"") -> None:
        self._data = data
        self.requested: list[str] = []

    def get(self, key: str) -> bytes:
        self.requested.append(key)
        return self._data


class _RecordingDocker(DockerSandboxClient):
    """Model Docker resources and label filters at the CLI boundary."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.calls: list[list[str]] = []
        self.timeouts: list[float] = []
        self.outputs: dict[str, str] = {}
        self.copied_files: dict[str, dict[str, bytes]] = {}
        self.volumes: dict[str, dict[str, str]] = {}
        self.volume_created_at: dict[str, str] = {}
        self.volume_files: dict[str, dict[str, bytes]] = {}
        self.auto_created_volumes: list[str] = []
        self.containers: dict[str, dict[str, str]] = {}
        self.container_status: dict[str, str] = {}
        self.container_created_at: dict[str, str] = {}
        self.container_mounts: dict[str, dict[str, tuple[str, str]]] = {}
        self.started_containers: list[str] = []

    @property
    def runner_argv(self) -> list[str]:
        return next(argv for argv in reversed(self.calls) if argv[0] == "run")

    def _docker(
        self,
        args: list[str],
        *,
        request_timeout_seconds: float,
        check: bool = True,
    ) -> str:
        self.timeouts.append(request_timeout_seconds)
        self.calls.append(args)
        if args[:2] == ["volume", "create"]:
            if args[-1] not in self.volumes:
                self.volumes[args[-1]] = dict(
                    value.split("=", 1) for value in _flag_values(args, "--label")
                )
                self.volume_created_at[args[-1]] = datetime.now(UTC).isoformat()
                self.volume_files[args[-1]] = {}
            return args[-1]
        if args[:2] == ["volume", "ls"]:
            return "\n".join(
                name
                for name, labels in self.volumes.items()
                if _matches_label_filters(args, labels)
            )
        if args[:2] == ["volume", "inspect"]:
            name = args[-1]
            if name not in self.volumes:
                if check:
                    raise DockerError(f"no such volume: {name}")
                return ""
            return json.dumps(self.volumes[name]) + "\t" + self.volume_created_at.get(name, "")
        if args[:2] == ["volume", "rm"]:
            for name in args[2:]:
                if any(
                    name in {source for source, mode in mounts.values()}
                    for container, mounts in self.container_mounts.items()
                    if container in self.containers
                ):
                    if check:
                        raise DockerError(f"volume {name!r} is in use")
                    continue
                self.volumes.pop(name, None)
                self.volume_created_at.pop(name, None)
                self.volume_files.pop(name, None)
        if args[0] in ("run", "create"):
            name = _flag_values(args, "--name")[0]
            if name in self.containers:
                raise DockerError(f"container name {name!r} is already in use")
            mounts = _volume_mounts(args)
            # https://docs.docker.com/engine/storage/volumes/: named volumes
            # are created automatically when a container mounts a missing one.
            for source, mode in mounts.values():
                if source not in self.volumes:
                    self.volumes[source] = {}
                    self.volume_created_at[source] = datetime.now(UTC).isoformat()
                    self.volume_files[source] = {}
                    self.auto_created_volumes.append(source)
            self.containers[name] = dict(
                value.split("=", 1) for value in _flag_values(args, "--label")
            )
            self.container_status[name] = "running" if args[0] == "run" else "created"
            self.container_created_at[name] = datetime.now(UTC).isoformat()
            self.container_mounts[name] = mounts
            if args[0] == "run":
                self.started_containers.append(name)
        if args[0] == "start":
            name = args[-1]
            if name not in self.containers:
                raise DockerError(f"no such container: {name}")
            self.container_status[name] = "exited" if "-a" in args else "running"
            self.started_containers.append(name)
        if args[0] == "ps" and "ps" not in self.outputs:
            return "\n".join(
                name
                for name, labels in self.containers.items()
                if _matches_label_filters(args, labels)
                and ("-a" in args or "--all" in args or self.container_status[name] == "running")
            )
        if args[0] == "inspect" and "inspect" not in self.outputs:
            name = args[-1]
            if name not in self.containers:
                return ""
            return "\t".join(
                (
                    self.container_status[name],
                    json.dumps(self.containers[name]),
                    self.container_created_at[name],
                )
            )
        if args[0] == "rm":
            for name in args[1:]:
                self.containers.pop(name, None)
                self.container_status.pop(name, None)
                self.container_created_at.pop(name, None)
                self.container_mounts.pop(name, None)
        if args[0] == "cp":
            source = Path(args[-2])
            container, _, destination = args[-1].partition(":")
            if container not in self.containers:
                raise DockerError(f"no such container: {container}")
            if not source.is_dir():
                raise DockerError(f"copy source is missing: {source}")
            if source.is_dir():
                self.copied_files[args[-1]] = {
                    str(path.relative_to(source)): path.read_bytes()
                    for path in source.rglob("*")
                    if path.is_file()
                }
                volume, mode = self.container_mounts[container][destination]
                if volume not in self.volumes:
                    raise DockerError(f"no such volume: {volume}")
                self.volume_files[volume] = dict(self.copied_files[args[-1]])
        return self.outputs.get(args[0], "")


def _flag_values(argv: list[str], flag: str) -> list[str]:
    return [argv[i + 1] for i, a in enumerate(argv) if a == flag and i + 1 < len(argv)]


def _matches_label_filters(argv: list[str], labels: dict[str, str]) -> bool:
    selectors = [
        value.removeprefix("label=")
        for value in _flag_values(argv, "--filter")
        if value.startswith("label=")
    ]
    return all(
        labels.get(key) == value if sep else key in labels
        for selector in selectors
        for key, sep, value in [selector.partition("=")]
    )


def _volume_mounts(argv: list[str]) -> dict[str, tuple[str, str]]:
    mounts: dict[str, tuple[str, str]] = {}
    for value in _flag_values(argv, "-v") + _flag_values(argv, "--volume"):
        source, target, *options = value.split(":")
        assert source and not any(part in source for part in ("/", "\\")), value
        mounts[target] = (source, "ro" if "ro" in options else "rw")
    for value in _flag_values(argv, "--mount"):
        fields = dict(part.partition("=")[::2] for part in value.split(","))
        assert fields.get("type") == "volume", value
        source = fields.get("source", fields.get("src", ""))
        target = fields.get("target", fields.get("dst", fields.get("destination", "")))
        assert source and target, value
        assert not any(part in source for part in ("/", "\\")), value
        readonly = any(key in fields for key in ("readonly", "ro"))
        mounts[target] = (source, "ro" if readonly else "rw")
    return mounts


def _seed_source(client: _RecordingDocker, target: str) -> Path:
    volume, mode = _volume_mounts(client.runner_argv)[target]
    helper = next(
        argv
        for argv in client.calls
        if argv[0] == "create"
        and volume in {source for source, mode in _volume_mounts(argv).values()}
    )
    name = _flag_values(helper, "--name")[0]
    copied = next(
        argv for argv in client.calls if argv[0] == "cp" and argv[-1].startswith(f"{name}:")
    )
    return Path(copied[-2])


@pytest.fixture
def redis_client() -> Iterator[redis.Redis]:
    client = connect_or_skip(decode_responses=False)
    yield client
    client.close()


@pytest.fixture
def key_prefix(redis_client: redis.Redis) -> Iterator[str]:
    """Per-test-unique key prefix on the shared Valkey, cleaned up after."""

    prefix = f"test:curie:sandbox:{uuid.uuid4().hex}"
    yield prefix
    keys = list(redis_client.scan_iter(match=f"{prefix}:*"))
    if keys:
        redis_client.delete(*keys)


@pytest.fixture
def pressure_redis_factory() -> Callable[[], AsyncRedis]:
    def make_client() -> AsyncRedis:
        return AsyncRedis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PW or None,
            decode_responses=False,
            socket_timeout=1.0,
            socket_connect_timeout=1.0,
            retry=AsyncRetry(NoBackoff(), 0),
            driver_info=None,
            maint_notifications_config=MaintNotificationsConfig(enabled=False),
        )

    return make_client


@pytest.fixture
def affinity(
    redis_client: redis.Redis,
    pressure_redis_factory: Callable[[], AsyncRedis],
    key_prefix: str,
) -> Iterator[AffinityStore]:
    pressure_client = pressure_redis_factory()
    yield AffinityStore(
        redis_client,
        pressure_client=pressure_client,
        key_prefix=key_prefix,
    )
    asyncio.run(pressure_client.aclose())


@pytest.fixture
def config(key_prefix: str) -> SubstrateConfig:
    return SubstrateConfig(
        namespace="test-ns",
        warm_pool="test-pool",
        route_ttl_seconds=60,
        suspended_route_ttl_seconds=120,
        claim_timeout_seconds=2.0,
        poll_interval_seconds=0.005,
        key_prefix=key_prefix,
    )


@dataclass
class FakeClaim:
    name: str
    env: dict[str, str]
    labels: dict[str, str]
    sandbox_name: str
    pool: str = ""
    ready: bool = True
    # The claim's creation instant, tz-aware UTC, as the cluster would stamp it.
    # A test ages a claim by assigning a past instant here, which is how the
    # reaper's bind-window grace is driven with no sleeps and no wall-clock
    # dependence; None models an adapter that cannot report an age at all.
    created_at: datetime | None = None
    quota_rejection: QuotaRejection | None = None
    ready_reason: str | None = None
    ready_message: str | None = None
    # The executor claim's header set (ACTION-EXECUTOR-5); None is an ordinary claim.
    executor_secret_names: frozenset[str] | None = None


@dataclass
class FakeSandbox:
    name: str
    service_fqdn: str
    operating_mode: str = "Running"
    ready: bool = True


@dataclass
class FakeSandboxClient:
    """In-memory model of the agent-sandbox extensions behavior:

    a created claim binds a sandbox immediately (warm pool), the sandbox gets a
    headless-service FQDN, deleting the claim deletes its sandbox, and
    suspending flips operatingMode (the pod deletion itself is a cluster-side
    effect the substrate never reads back).
    """

    namespace: str = "test-ns"
    claims: dict[str, FakeClaim] = field(default_factory=dict)
    sandboxes: dict[str, FakeSandbox] = field(default_factory=dict)
    bind_ready: bool = True
    quota_rejection: QuotaRejection | None = None
    quota_headroom_results: list[bool | BaseException] = field(default_factory=list)
    quota_headroom_calls: list[tuple[QuotaRejection, float]] = field(default_factory=list)
    ready_reason: str | None = None
    ready_message: str | None = None
    # The scheduler message a pod read reports while no node has room, or
    # None for a pod that is scheduled (or unknown).
    unschedulable_message: str | None = None
    pod_reads: list[str] = field(default_factory=list)
    termination: SandboxTermination | None = None
    termination_reads: list[str] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    # Every claim-scoped template sweep the substrate asked for, as
    # ``(keep, created_before)``; ``template_reap_error`` makes the sweep raise.
    template_reaps: list[tuple[set[str], datetime]] = field(default_factory=list)
    template_reap_error: BaseException | None = None

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        runner_resources: dict[str, object] | None = None,
        agent_name: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
    ) -> None:
        sandbox_name = f"sbx-{name}"
        self.claims[name] = FakeClaim(
            name=name,
            env=dict(env or {}),
            labels={"curietech.ai/managed-by": "curie-sandbox-substrate", **(labels or {})},
            sandbox_name=sandbox_name,
            pool=pool,
            ready=self.bind_ready and self.quota_rejection is None,
            created_at=datetime.now(UTC),
            quota_rejection=self.quota_rejection,
            ready_reason=self.ready_reason,
            ready_message=self.ready_message,
            executor_secret_names=executor_secret_names,
        )
        self.sandboxes[sandbox_name] = FakeSandbox(
            name=sandbox_name,
            service_fqdn=f"{sandbox_name}.{self.namespace}.svc.cluster.local",
        )
        self.created.append(name)

    def get_claim(self, name: str, *, request_timeout_seconds: float) -> ClaimView | None:
        assert request_timeout_seconds > 0
        claim = self.claims.get(name)
        if claim is None:
            return None
        # The age every reaper test drives is read back through this view: a
        # test ages ``FakeClaim.created_at``, and the substrate sees it here.
        return ClaimView(
            name=claim.name,
            ready=claim.ready,
            sandbox_name=claim.sandbox_name if claim.ready else None,
            created_at=claim.created_at,
            quota_rejection=claim.quota_rejection,
            ready_reason=claim.ready_reason,
            ready_message=claim.ready_message,
        )

    def delete_claim(self, name: str, *, request_timeout_seconds: float) -> None:
        assert request_timeout_seconds > 0
        claim = self.claims.pop(name, None)
        if claim is not None:
            self.sandboxes.pop(claim.sandbox_name, None)
        self.deleted.append(name)

    def list_claims(self, *, label_selector: str) -> list[ClaimView]:
        key, _, value = label_selector.partition("=")
        views = []
        for claim in self.claims.values():
            if claim.labels.get(key) == value:
                view = self.get_claim(claim.name, request_timeout_seconds=1.0)
                assert view is not None
                views.append(view)
        return views

    def reap_claim_templates(self, *, keep: set[str], created_before: datetime) -> list[str]:
        self.template_reaps.append((set(keep), created_before))
        if self.template_reap_error is not None:
            raise self.template_reap_error
        return []

    def get_sandbox(self, name: str, *, request_timeout_seconds: float) -> SandboxView | None:
        assert request_timeout_seconds > 0
        sandbox = self.sandboxes.get(name)
        if sandbox is None:
            return None
        return SandboxView(
            name=sandbox.name,
            ready=sandbox.ready,
            service_fqdn=sandbox.service_fqdn,
            operating_mode=sandbox.operating_mode,
        )

    def quota_has_headroom(
        self,
        rejection: QuotaRejection,
        *,
        request_timeout_seconds: float,
    ) -> bool:
        assert 0 < request_timeout_seconds <= 1.0
        self.quota_headroom_calls.append((rejection, request_timeout_seconds))
        if not self.quota_headroom_results:
            return False
        result = self.quota_headroom_results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result

    def pod_unschedulable(self, name: str, *, request_timeout_seconds: float) -> str | None:
        assert request_timeout_seconds > 0
        self.pod_reads.append(name)
        return self.unschedulable_message

    def pod_termination(
        self, name: str, *, request_timeout_seconds: float, since: datetime
    ) -> SandboxTermination | None:
        assert request_timeout_seconds > 0
        assert since.tzinfo is not None
        self.termination_reads.append(name)
        return self.termination

    def set_sandbox_mode(self, name: str, mode: str) -> None:
        self.sandboxes[name].operating_mode = mode


@pytest.fixture
def fake_k8s() -> FakeSandboxClient:
    return FakeSandboxClient()


@dataclass
class PoolProbingFakeSandboxClient(FakeSandboxClient):
    """A ``FakeSandboxClient`` that also answers ``warm_pool_exists``.

    A separate class, because the substrate already probes for a derived
    per-agent pool whenever the client can answer: adding the probe to the base
    fake would move every existing agent-pool test onto the derived pool.
    ``warm_pools`` None means every pool exists; a set names the pools that do.
    ``warm_pool_error`` makes the probe raise, as an apiserver outage would.
    """

    warm_pools: frozenset[str] | None = None
    warm_pool_error: BaseException | None = None
    warm_pool_probes: list[str] = field(default_factory=list)

    def warm_pool_exists(self, name: str) -> bool:
        self.warm_pool_probes.append(name)
        if self.warm_pool_error is not None:
            raise self.warm_pool_error
        return self.warm_pools is None or name in self.warm_pools


@pytest.fixture
def probing_k8s() -> PoolProbingFakeSandboxClient:
    return PoolProbingFakeSandboxClient()
