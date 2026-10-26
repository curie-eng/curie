"""Fixtures for the sandbox substrate tests.

Affinity tests run against the REAL Valkey from the compose stack (repo test
discipline: never mock Valkey). The Kubernetes control plane is an external
service, so substrate-logic tests use ``FakeSandboxClient`` (an in-memory model
of the agent-sandbox claim/pool behavior observed in PT-1/PT-D); the real
client is exercised by the env-gated k8scratch e2e in ``test_e2e_k8scratch.py``.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import os
import socket
import ssl
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import pytest
import redis
from curie_test_support.valkey import (
    VALKEY_HOST,
    VALKEY_PORT,
    VALKEY_PW,
    connect_or_skip,
)
from curie_worker.sandbox import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    AffinityStore,
    ClaimView,
    QuotaRejection,
    SandboxView,
    SubstrateConfig,
)
from curie_worker.sandbox.claim_tokens import CLAIM_LABEL
from curie_worker.sandbox.docker import DockerError, DockerSandboxClient
from curie_worker.sandbox.types import ClaimTimeoutError, SandboxError, SandboxTermination
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.retry import Retry as AsyncRetry
from redis.backoff import NoBackoff
from redis.maint_notifications import MaintNotificationsConfig

if TYPE_CHECKING:
    from curie_worker.sandbox.types import (
        DaemonObjectKind,
        DaemonObjectView,
        ListableDaemonKind,
        ToolDaemonMount,
    )


def pytest_addoption(parser: pytest.Parser) -> None:
    """The live tool-daemon e2e's target (test_e2e_tool_daemon.py, ADR 0217).

    Read only when ``CURIE_SANDBOX_E2E=1``; every value comes from the caller.
    """

    group = parser.getgroup("curie-sandbox-e2e")
    for name in ("--context", "--namespace", "--daemon-namespace", "--release"):
        group.addoption(name, default=None)
    group.addoption("--candidate", default=None)
    group.addoption("--work-dir", default=None)
    group.addoption("--check-failure-cleanup", action="store_true", default=False)


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
        self.volume_modes: dict[str, dict[str, int]] = {}
        self.auto_created_volumes: list[str] = []
        self.copy_checks: list[Callable[[Path], None]] = []
        self.containers: dict[str, dict[str, str]] = {}
        self.container_ids: dict[str, str] = {}
        self.container_status: dict[str, str] = {}
        self.container_created_at: dict[str, str] = {}
        self.container_started_at: dict[str, str] = {}
        self.container_mounts: dict[str, dict[str, tuple[str, str]]] = {}
        self.started_containers: list[str] = []

    @property
    def runner_argv(self) -> list[str]:
        return next(
            argv
            for argv in reversed(self.calls)
            if argv[0] == "create"
            and "curietech.ai/component=claim-runner" in _flag_values(argv, "--label")
        )

    def _container_name(self, identifier: str) -> str:
        return next(
            (name for name, value in self.container_ids.items() if value == identifier),
            identifier,
        )

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
                self.volume_modes[args[-1]] = {}
            return args[-1]
        if args[:2] == ["volume", "ls"]:
            filters = _flag_values(args, "--filter")
            referenced = {
                source
                for container, mounts in self.container_mounts.items()
                if container in self.containers
                for source, mode in mounts.values()
            }
            return "\n".join(
                name
                for name, labels in self.volumes.items()
                if _matches_label_filters(args, labels)
                and ("dangling=true" not in filters or name not in referenced)
                and ("dangling=false" not in filters or name in referenced)
            )
        if args[:2] == ["volume", "inspect"]:
            name = args[-1]
            if name not in self.volumes:
                if check:
                    raise DockerError(f"no such volume: {name}")
                return ""
            template = _flag_values(args, "--format")[0]
            return (
                template.replace("{{json .Labels}}", json.dumps(self.volumes[name]))
                .replace("{{.CreatedAt}}", self.volume_created_at.get(name, ""))
                .replace("{{.Name}}", name)
            )
        if args[:2] == ["volume", "rm"]:
            for name in args[2:]:
                # https://docs.docker.com/engine/storage/volumes/: a volume
                # referenced by a container cannot be removed, even if stopped.
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
                self.volume_modes.pop(name, None)
        if args[0] in ("run", "create"):
            name = _flag_values(args, "--name")[0]
            if name in self.containers:
                raise DockerError(f"container name {name!r} is already in use")
            mounts = _volume_mounts(args)
            # https://docs.docker.com/engine/storage/volumes/: named volumes
            # are created automatically when a container mounts a missing one.
            for source, _mode in mounts.values():
                if source not in self.volumes:
                    self.volumes[source] = {}
                    self.volume_created_at[source] = datetime.now(UTC).isoformat()
                    self.volume_files[source] = {}
                    self.volume_modes[source] = {}
                    self.auto_created_volumes.append(source)
            self.containers[name] = dict(
                value.split("=", 1) for value in _flag_values(args, "--label")
            )
            self.container_ids[name] = uuid.uuid4().hex + uuid.uuid4().hex
            self.container_status[name] = "running" if args[0] == "run" else "created"
            self.container_created_at[name] = datetime.now(UTC).isoformat()
            self.container_started_at[name] = (
                self.container_created_at[name] if args[0] == "run" else "0001-01-01T00:00:00Z"
            )
            self.container_mounts[name] = mounts
            if args[0] == "run":
                self.started_containers.append(name)
            return self.container_ids[name]
        if args[0] == "start":
            name = self._container_name(args[-1])
            if name not in self.containers:
                raise DockerError(f"no such container: {name}")
            self.container_status[name] = "exited" if "-a" in args else "running"
            self.container_started_at[name] = datetime.now(UTC).isoformat()
            self.started_containers.append(name)
        if args[0] == "exec":
            name = self._container_name(args[1])
            if name not in self.containers or self.container_status[name] != "running":
                raise DockerError(f"container is not running: {name}")
        if args[0] == "ps" and "ps" not in self.outputs:
            filters = _flag_values(args, "--filter")
            template = _flag_values(args, "--format")[0]
            return "\n".join(
                self.container_ids[name] if template == "{{.ID}}" else name
                for name, labels in self.containers.items()
                if _matches_label_filters(args, labels)
                and ("-a" in args or "--all" in args or self.container_status[name] == "running")
                and all(
                    value.removeprefix("volume=")
                    in {source for source, mode in self.container_mounts.get(name, {}).values()}
                    for value in filters
                    if value.startswith("volume=")
                )
                and all(
                    self.container_status[name] == value.removeprefix("status=")
                    for value in filters
                    if value.startswith("status=")
                )
            )
        if args[0] == "inspect" and "inspect" not in self.outputs:
            name = self._container_name(args[-1])
            if name not in self.containers:
                return ""
            template = _flag_values(args, "--format")[0]
            return (
                template.replace("{{.State.Status}}", self.container_status[name])
                .replace("{{json .Config.Labels}}", json.dumps(self.containers[name]))
                .replace("{{.Created}}", self.container_created_at[name])
                .replace("{{.Id}}", self.container_ids[name])
                .replace("{{.State.StartedAt}}", self.container_started_at[name])
                .replace("{{json .NetworkSettings.Networks}}", "{}")
            )
        if args[0] == "rm":
            for identifier in args[1:]:
                name = self._container_name(identifier)
                self.containers.pop(name, None)
                self.container_status.pop(name, None)
                self.container_created_at.pop(name, None)
                self.container_started_at.pop(name, None)
                self.container_mounts.pop(name, None)
        if args[0] == "cp":
            source = Path(args[-2])
            container, _, destination = args[-1].partition(":")
            container = self._container_name(container)
            if container not in self.containers:
                raise DockerError(f"no such container: {container}")
            if not source.is_dir():
                raise DockerError(f"copy source is missing: {source}")
            for copy_check in self.copy_checks:
                copy_check(source)
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
                self.volume_modes[volume] = {
                    ".": source.stat().st_mode,
                    **{
                        str(path.relative_to(source)): path.stat().st_mode
                        for path in source.rglob("*")
                    },
                }
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
        and "curietech.ai/component=claim-runner" not in _flag_values(argv, "--label")
        and volume in {source for source, mode in _volume_mounts(argv).values()}
    )
    name = client.container_ids[_flag_values(helper, "--name")[0]]
    copied = next(
        argv for argv in client.calls if argv[0] == "cp" and argv[-1].startswith(f"{name}:")
    )
    return Path(copied[-2])


@pytest.fixture
def redis_client(request: pytest.FixtureRequest) -> redis.Redis:
    client: redis.Redis | None = None

    def cleanup() -> None:
        if client is not None:
            client.close()

    request.addfinalizer(cleanup)
    client = connect_or_skip(decode_responses=False)
    return client


@pytest.fixture
def key_prefix(redis_client: redis.Redis, request: pytest.FixtureRequest) -> str:
    """Per-test-unique key prefix on the shared Valkey, cleaned up after."""

    prefix = f"test:curie:sandbox:{uuid.uuid4().hex}"

    def cleanup() -> None:
        keys = list(redis_client.scan_iter(match=f"{prefix}:*"))
        if keys:
            redis_client.delete(*keys)
        assert not list(redis_client.scan_iter(match=f"{prefix}:*"))

    request.addfinalizer(cleanup)
    return prefix


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
    request: pytest.FixtureRequest,
) -> AffinityStore:
    pressure_client = pressure_redis_factory()
    request.addfinalizer(lambda: asyncio.run(pressure_client.aclose()))
    return AffinityStore(
        redis_client,
        pressure_client=pressure_client,
        key_prefix=key_prefix,
    )


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
    # What a Docker-enabled claim's runner was handed (ADR 0217), else None.
    tool_daemon: ToolDaemonMount | None = None
    # The absolute time.monotonic() deadline create_claim was bounded by.
    deadline: float | None = None


class Op(NamedTuple):
    """One recorded client call, in order (``FakeSandboxClient.ops``).

    ``verb`` is ``create_claim`` / ``delete_claim`` for claims, and ``create``,
    ``list``, ``delete``, ``get`` or ``stream`` for the daemon seam. ``target``
    is the object name, or the label selector for ``list``. ``detail`` is the
    uid a ``delete`` was fenced by, or the timeout a ``stream`` was bounded by.
    """

    verb: str
    namespace: str
    kind: str
    target: str
    detail: object = None


DAEMON_VERBS = frozenset({"create", "list", "delete", "get", "stream"})


@dataclass
class FakeDaemonObject:
    """One object the fake daemon namespace stores, as the API server would."""

    namespace: str
    kind: str
    name: str
    uid: str
    manifest: dict[str, Any]
    created_at: datetime | None
    # Pod only: ``status.phase``, and the Ready condition with no deletionTimestamp.
    phase: str | None = None
    ready: bool = True

    @property
    def labels(self) -> dict[str, str]:
        return dict((self.manifest.get("metadata") or {}).get("labels") or {})


def label_selector_matches(labels: Mapping[str, str], selector: str) -> bool:
    """Equality (``k=v``) and existence (``k``) terms, comma-joined, as kube does."""

    for term in filter(None, selector.split(",")):
        key, sep, value = term.partition("=")
        if sep:
            if labels.get(key) != value:
                return False
        elif key not in labels:
            return False
    return True


def _http_response(status: int, body: bytes, *, chunked: bool = False) -> bytes:
    reason = {200: "OK", 404: "Not Found", 503: "Service Unavailable"}[status]
    if not chunked:
        head = f"HTTP/1.1 {status} {reason}\r\nContent-Length: {len(body)}\r\n"
        return (head + "Content-Type: text/plain\r\nConnection: close\r\n\r\n").encode() + body
    middle = len(body) // 2
    chunks = b"".join(
        f"{len(part):x}\r\n".encode() + part + b"\r\n" for part in (body[:middle], body[middle:])
    )
    head = f"HTTP/1.1 {status} {reason}\r\nTransfer-Encoding: chunked\r\n"
    return (head + "Content-Type: application/json\r\nConnection: close\r\n\r\n").encode() + (
        chunks + b"0\r\n\r\n"
    )


def _serve_daemon_tls(
    raw: socket.socket, files: Mapping[str, str], *, daemon_id: str, name: str, refused: bool
) -> None:
    """Serve one connection the way dockerd ``--tlsverify`` does.

    Real TLS with the server files of the daemon's Secret, and the client must
    present a certificate chaining to that Secret's ``ca.pem`` (CERT_REQUIRED),
    so a claim's own credential is accepted and any other claim's is refused.
    """

    try:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        with tempfile.TemporaryDirectory() as tmp:
            cert = Path(tmp, "server-cert.pem")
            key = Path(tmp, "server-key.pem")
            cert.write_text(files["server-cert.pem"])
            key.write_text(files["server-key.pem"])
            context.load_cert_chain(cert, key)
        context.verify_mode = ssl.CERT_REQUIRED
        context.verify_flags |= ssl.VERIFY_X509_STRICT
        context.load_verify_locations(cadata=files["ca.pem"])
        raw.settimeout(10.0)
        with context.wrap_socket(raw, server_side=True) as tls:
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = tls.recv(4096)
                if not chunk:
                    return
                request += chunk
            path = request.split(b" ", 2)[1].decode()
            if refused:
                tls.sendall(_http_response(503, b"daemon is not serving"))
            elif path == "/_ping":
                tls.sendall(_http_response(200, b"OK"))
            elif path == "/info":
                info = json.dumps({"ID": daemon_id, "Name": name}).encode()
                tls.sendall(_http_response(200, info, chunked=True))
            else:
                tls.sendall(_http_response(404, b"page not found"))
            # Connection: close, as dockerd ends it: close_notify, then EOF.
            with contextlib.suppress(OSError, ValueError):
                tls.unwrap().close()
    except (OSError, ValueError, KeyError):
        # ssl.SSLError is an OSError: a refused client certificate ends here.
        pass
    finally:
        raw.close()


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

    # -- claim creation shape (ADR 0217 lifecycle tests) -----------------------
    # Every claim and daemon call, in order; ``daemon_calls`` is the daemon part.
    ops: list[Op] = field(default_factory=list)
    # How long a successful create_claim takes, so a test can spend the budget.
    claim_create_delay_seconds: float = 0.0
    # create_claim raises this before persisting anything.
    create_claim_error: BaseException | None = None
    # create_claim persists the claim, then raises this (an ambiguous create:
    # the API server stored it but the response was lost to a timeout).
    create_claim_persist_error: BaseException | None = None

    # -- tool daemon model (ADR 0217) ------------------------------------------
    # The daemon namespace's objects, keyed (namespace, kind, name).
    daemon_objects: dict[tuple[str, str, str], FakeDaemonObject] = field(default_factory=dict)
    # (verb, kind) -> raised by that daemon call, after it is recorded.
    daemon_failures: dict[tuple[str, str], BaseException] = field(default_factory=dict)
    # status.phase every newly created daemon Pod reports.
    daemon_phase: str = "Running"
    # The Ready condition new daemon Pods report (a terminating Pod reads as not ready).
    daemon_ready: bool = True
    # How many next streams are refused as dockerd still starting up would be.
    daemon_unready_streams: int = 0
    # Pods whose dockerd answers every request with 503.
    refused_pods: set[str] = field(default_factory=set)
    # Pod name -> raised by a port-forward stream to it.
    stream_failures: dict[str, BaseException] = field(default_factory=dict)
    # (kind, name) replaced by another actor right after every list sees it:
    # the stored object gets a new uid, so a uid-fenced delete must miss it.
    replace_after_list: set[tuple[str, str]] = field(default_factory=set)
    # How long each successful daemon create takes.
    daemon_op_delay_seconds: float = 0.0
    daemon_request_timeouts: list[float] = field(default_factory=list)

    @property
    def daemon_calls(self) -> list[Op]:
        return [op for op in self.ops if op.verb in DAEMON_VERBS]

    def daemon_objects_of(self, claim: str) -> dict[str, FakeDaemonObject]:
        """The stored daemon objects named for ``claim``, by kind."""

        return {obj.kind: obj for obj in self.daemon_objects.values() if obj.name == claim}

    def seed_daemon(
        self,
        namespace: str,
        claim: str,
        *,
        port: int,
        server_files: Mapping[str, str] | None = None,
        created_at: datetime | None = None,
        ageless: bool = False,
        labels: Mapping[str, str] | None = None,
        kinds: tuple[str, ...] = ("NetworkPolicy", "Service", "Pod", "Secret"),
    ) -> dict[str, FakeDaemonObject]:
        """Store a daemon object set as a worker that crashed (or another one) left it.

        ``created_at`` defaults to now; ``ageless`` stores objects whose age the
        API cannot report. ``labels`` replaces the default claim, managed-by and
        chart selector labels (instance ``t``). The Secret is owned by the Pod's uid.
        """

        object_labels = (
            dict(labels)
            if labels is not None
            else {
                CLAIM_LABEL: claim,
                MANAGED_BY_LABEL: MANAGED_BY_VALUE,
                "app.kubernetes.io/name": "curie",
                "app.kubernetes.io/instance": "t",
                "app.kubernetes.io/component": "docker-daemon",
            }
        )
        seeded: dict[str, FakeDaemonObject] = {}
        for kind in kinds:
            manifest: dict[str, Any] = {
                "apiVersion": "networking.k8s.io/v1" if kind == "NetworkPolicy" else "v1",
                "kind": kind,
                "metadata": {"name": claim, "namespace": namespace, "labels": dict(object_labels)},
            }
            if kind == "Pod":
                manifest["spec"] = {
                    "containers": [
                        {"name": "dockerd", "ports": [{"containerPort": port, "protocol": "TCP"}]}
                    ],
                    "volumes": [{"name": "certs", "secret": {"secretName": claim}}],
                }
            if kind == "Secret":
                manifest["type"] = "Opaque"
                manifest["stringData"] = dict(server_files or {})
                pod = seeded.get("Pod")
                if pod is not None:
                    manifest["metadata"]["ownerReferences"] = [
                        {"apiVersion": "v1", "kind": "Pod", "name": claim, "uid": pod.uid}
                    ]
            seeded[kind] = self.daemon_objects[(namespace, kind, claim)] = FakeDaemonObject(
                namespace=namespace,
                kind=kind,
                name=claim,
                uid=str(uuid.uuid4()),
                manifest=manifest,
                created_at=None if ageless else (created_at or datetime.now(UTC)),
                phase=self.daemon_phase if kind == "Pod" else None,
                ready=self.daemon_ready,
            )
        return seeded

    def _daemon_call(
        self,
        verb: str,
        namespace: str,
        kind: str,
        target: str,
        timeout: float,
        detail: object = None,
    ) -> None:
        assert timeout > 0, f"{verb} {kind} {target} was not bounded"
        self.daemon_request_timeouts.append(timeout)
        self.ops.append(Op(verb, namespace, kind, target, detail))
        error = self.daemon_failures.get((verb, kind))
        if error is not None:
            raise error

    def create_daemon_object(
        self,
        namespace: str,
        manifest: Mapping[str, Any],
        *,
        request_timeout_seconds: float,
    ) -> str:
        from curie_worker.sandbox.types import ClaimNameConflict

        kind = str(manifest["kind"])
        name = str(manifest["metadata"]["name"])
        assert kind in {"Pod", "Service", "NetworkPolicy", "Secret"}, kind
        assert manifest["metadata"].get("namespace") in (None, namespace)
        index = len(self.ops)
        self._daemon_call("create", namespace, kind, name, request_timeout_seconds)
        if self.daemon_op_delay_seconds:
            time.sleep(self.daemon_op_delay_seconds)
        if (namespace, kind, name) in self.daemon_objects:
            # The API server's 409 AlreadyExists.
            raise ClaimNameConflict(f"{kind} {name} already exists in {namespace}")
        uid = str(uuid.uuid4())
        self.daemon_objects[(namespace, kind, name)] = FakeDaemonObject(
            namespace=namespace,
            kind=kind,
            name=name,
            uid=uid,
            manifest=copy.deepcopy(dict(manifest)),
            created_at=datetime.now(UTC),
            phase=self.daemon_phase if kind == "Pod" else None,
            ready=self.daemon_ready,
        )
        # A successful create's op carries the uid the API server returned.
        self.ops[index] = self.ops[index]._replace(detail=uid)
        return uid

    def _daemon_view(self, obj: FakeDaemonObject) -> DaemonObjectView:
        from curie_worker.sandbox.types import DaemonObjectView

        return DaemonObjectView(
            kind=cast("DaemonObjectKind", obj.kind),
            name=obj.name,
            uid=obj.uid,
            claim=obj.labels.get(CLAIM_LABEL),
            created_at=obj.created_at,
            phase=obj.phase,
            ready=obj.kind == "Pod" and obj.ready and obj.phase == "Running",
        )

    def list_daemon_objects(
        self,
        namespace: str,
        kind: ListableDaemonKind,
        *,
        label_selector: str,
        request_timeout_seconds: float,
    ) -> list[DaemonObjectView]:
        # The worker Role may create and delete daemon Secrets, never list them.
        assert kind in {"Pod", "Service", "NetworkPolicy"}, kind
        self._daemon_call("list", namespace, kind, label_selector, request_timeout_seconds)
        found = [
            obj
            for (ns, stored_kind, _name), obj in self.daemon_objects.items()
            if ns == namespace
            and stored_kind == kind
            and label_selector_matches(obj.labels, label_selector)
        ]
        views = [self._daemon_view(obj) for obj in found]
        for obj in found:
            if (kind, obj.name) in self.replace_after_list:
                obj.uid = str(uuid.uuid4())
        return views

    def delete_daemon_object(
        self,
        namespace: str,
        kind: DaemonObjectKind,
        name: str,
        *,
        uid: str,
        request_timeout_seconds: float,
    ) -> None:
        assert uid, "every daemon delete is fenced by the uid it means"
        self._daemon_call("delete", namespace, kind, name, request_timeout_seconds, uid)
        stored = self.daemon_objects.get((namespace, kind, name))
        if stored is None or stored.uid != uid:
            # Absent, or replaced since it was observed: not ours, nothing to do.
            return
        del self.daemon_objects[(namespace, kind, name)]
        if kind == "Pod":
            # Garbage collection: Secrets owned by this Pod's uid go with it.
            for key, obj in list(self.daemon_objects.items()):
                refs = (obj.manifest.get("metadata") or {}).get("ownerReferences") or []
                if obj.namespace == namespace and any(ref.get("uid") == uid for ref in refs):
                    del self.daemon_objects[key]

    def get_daemon_pod(
        self, namespace: str, name: str, *, request_timeout_seconds: float
    ) -> DaemonObjectView | None:
        self._daemon_call("get", namespace, "Pod", name, request_timeout_seconds)
        stored = self.daemon_objects.get((namespace, "Pod", name))
        return None if stored is None else self._daemon_view(stored)

    def open_daemon_stream(
        self, namespace: str, pod: str, port: int, *, timeout_seconds: float
    ) -> AbstractContextManager[socket.socket]:
        self._daemon_call("stream", namespace, "Pod", pod, timeout_seconds, timeout_seconds)
        failure = self.stream_failures.get(pod)
        if failure is not None:
            raise failure
        if self.daemon_unready_streams > 0:
            self.daemon_unready_streams -= 1
            raise SandboxError("port-forward: connection refused (dockerd starting)")
        stored = self.daemon_objects.get((namespace, "Pod", pod))
        if stored is None:
            raise SandboxError(f"port-forward: pod {pod} not found")
        if stored.phase != "Running":
            raise SandboxError(f"port-forward: pod {pod} is {stored.phase}")
        spec = stored.manifest.get("spec") or {}
        ports = {
            entry.get("containerPort")
            for container in spec.get("containers") or []
            for entry in container.get("ports") or []
        }
        if port not in ports:
            raise SandboxError(f"port-forward: nothing listens on {pod}:{port}")
        secret_names = [
            str(volume["secret"]["secretName"])
            for volume in spec.get("volumes") or []
            if (volume.get("secret") or {}).get("secretName")
        ]
        secret = next(
            (
                self.daemon_objects[(namespace, "Secret", secret_name)]
                for secret_name in secret_names
                if (namespace, "Secret", secret_name) in self.daemon_objects
            ),
            None,
        )
        if secret is None:
            # The kubelet never starts a container whose Secret volume is missing.
            raise SandboxError(f"port-forward: pod {pod} has no certificate Secret mounted")
        client_end, server_end = socket.socketpair()
        threading.Thread(
            target=_serve_daemon_tls,
            args=(server_end, dict(secret.manifest.get("stringData") or {})),
            kwargs={
                "daemon_id": f"FAKE:{stored.uid}",
                "name": pod,
                "refused": pod in self.refused_pods,
            },
            daemon=True,
        ).start()
        return contextlib.closing(client_end)

    def create_claim(
        self,
        name: str,
        *,
        pool: str,
        deadline: float,
        env: dict[str, str] | None = None,
        labels: dict[str, str] | None = None,
        runner_resources: dict[str, object] | None = None,
        agent_name: str | None = None,
        executor_secret_names: frozenset[str] | None = None,
        tool_daemon: ToolDaemonMount | None = None,
    ) -> None:
        from curie_worker.sandbox.types import ClaimNameConflict

        self.ops.append(Op("create_claim", self.namespace, "SandboxClaim", name, deadline))
        from curie_worker.sandbox import substrate as substrate_module

        # The substrate's own clock, read at call time, so a test that patches
        # ``substrate.time`` with a virtual clock sees the same instant here.
        if substrate_module.time.monotonic() >= deadline:
            raise ClaimTimeoutError(f"claim {name}: the shared claim deadline already passed")
        if self.claim_create_delay_seconds:
            time.sleep(self.claim_create_delay_seconds)
        if self.create_claim_error is not None:
            raise self.create_claim_error
        if name in self.claims:
            # The API server's 409 AlreadyExists: the name belongs to another claim.
            raise ClaimNameConflict(f"claim {name} already exists")
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
            tool_daemon=tool_daemon,
            deadline=deadline,
        )
        self.sandboxes[sandbox_name] = FakeSandbox(
            name=sandbox_name,
            service_fqdn=f"{sandbox_name}.{self.namespace}.svc.cluster.local",
        )
        self.created.append(name)
        if self.create_claim_persist_error is not None:
            raise self.create_claim_persist_error

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
        self.ops.append(Op("delete_claim", self.namespace, "SandboxClaim", name))
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
