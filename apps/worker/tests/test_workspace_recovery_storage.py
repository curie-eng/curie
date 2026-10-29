"""Workspace recovery objects against an owned store, a real API and real Postgres.

Every service here is real and owned by this module: a RustFS container and
named volume started through the docker CLI, a Postgres database created and
migrated with alembic, and the Curie API from ``create_app()`` served by
uvicorn on a loopback port. The worker side talks to them with the production
``WorkspaceRecoveryClient`` over HTTP and boto3 over TCP.

The only injected faults are named where they are used: a TCP proxy in front
of the store (stall, mid-stream cut, outage), an httpx transport that loses
only the ``/complete`` pointer write, SQL updates that move a staging or
retention deadline into the past instead of waiting minutes or hours, and a
collector subprocess killed between deletion and finalize.

``DOCKER_HOST`` may name a remote engine whose published loopback ports are
mirrored locally, so nothing here bind-mounts a host path and readiness is
proven by polling the S3 endpoint rather than by inspecting the container.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import logging
import math
import os
import random
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import tarfile
import threading
import time
import traceback
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping, Sequence
from contextlib import ExitStack, asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, NoReturn
from urllib.parse import parse_qs, urlsplit, urlunsplit

import httpx
import pytest
import uvicorn
from botocore.exceptions import BotoCoreError, ClientError
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_runner.workspace_checkpoint import capture_workspace_checkpoint
from curie_worker.workspace import (
    CommandResult,
    PreparedWorkspace,
    SubprocessCommands,
    WorkspaceClaimCoordinator,
    WorkspaceCredential,
    WorkspaceLimits,
    WorkspaceObjectStore,
    WorkspacePreparer,
)
from curie_worker.workspace_recovery import WorkspaceRecoveryClient
from curie_worker.workspace_recovery_storage import (
    CLEANUP_BUDGET_SECONDS,
    MANIFEST_LIMIT_BYTES,
    PATCH_LIMIT_BYTES,
    RecoveryAuthority,
    RecoveryBaseSource,
    RecoveryCaptureRequest,
    RecoveryCaptureResult,
    RecoveryCheckpointReader,
    RecoveryCheckpointWriter,
    RecoveryCollectionPass,
    RecoveryCollector,
    RecoveryObjectStore,
    RecoveryReadLease,
    RecoveryReadRefused,
    RecoveryRepository,
    build_recovery_s3_client,
)
from sqlalchemy import make_url, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

# Mirrors apps/api/tests/conftest.py: the API names the dev object-store
# credential itself, and the background reconcilers stay off so their passes
# never race this module's SQL.
os.environ.setdefault("S3_ACCESS_KEY", "rustfs")
os.environ.setdefault("S3_SECRET_KEY", "rustfssecret")
os.environ.setdefault("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
os.environ.setdefault("CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED", "false")
get_settings.cache_clear()

TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
API_DIR = Path(__file__).resolve().parents[3] / "apps/api"
INTERRUPT_SCRIPT = Path(__file__).resolve().parent / "fixtures/recovery_collector_interrupt.py"
EXIT_BEFORE_FINALIZE = 17

RUSTFS_IMAGE = "rustfs/rustfs:1.0.0-beta.12"
RUSTFS_ACCESS_KEY = "rustfs"
RUSTFS_SECRET_KEY = "rustfssecret"
OWNED_LABEL = "curie.test=workspace-recovery-storage"
BUCKET = "curie-recovery-test"
REGION = "us-east-1"
READY_TIMEOUT_SECONDS = 90.0

REPOSITORY_PATH = "acme-corp/acme-bot"
CODE_HOST_ORIGIN = "https://github.com"
CLEAN_URL = f"{CODE_HOST_ORIGIN}/{REPOSITORY_PATH}.git"
GIT_CREDENTIAL = "Basic recovery-fixture-credential"
DEPLOYMENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
RUNTIME_EPOCH = 3
DISPATCH_GENERATION = 2
ARCHIVE_LIMIT = 268_435_456
INSTALLATION_CAP = 1 << 30
CANARY = "curie-recovery-canary-5f0c2e9a"
GIT_IDENTITY = (
    "-c",
    "user.name=Recovery Fixture",
    "-c",
    "user.email=recovery@example.com",
    "-c",
    "commit.gpgsign=false",
)
# Slack for scheduler and container jitter on top of a stated budget.
TIMING_SLACK_SECONDS = 4.0
EMPTY_PASS = RecoveryCollectionPass(claimed=0, deleted=0, finalized=0, conflicts=0, failed=0)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


# --- availability ---------------------------------------------------------------------------


def _unavailable(reason: str) -> NoReturn:
    """Skip a local loop; a CI run fails instead, so the suite never skips silently there."""

    if "CI" in os.environ:
        pytest.fail(reason)
    pytest.skip(reason)


def _require_docker() -> None:
    if shutil.which("docker") is None:
        _unavailable("the docker CLI is not installed")
    probe = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Version}}"],
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )
    if probe.returncode != 0:
        _unavailable(f"the docker engine is unreachable: {probe.stderr.strip()[-300:]}")


def _require_postgres() -> None:
    base = make_url(TEST_DATABASE_URL)

    async def probe() -> None:
        engine = create_async_engine(base.set(database="postgres"), poolclass=NullPool)
        try:
            async with asyncio.timeout(15):
                async with engine.connect():
                    pass
        finally:
            await engine.dispose()

    try:
        asyncio.run(probe())
    except (OSError, SQLAlchemyError, TimeoutError) as exc:
        _unavailable(f"Postgres is unreachable at {base.host}:{base.port}: {type(exc).__name__}")


# --- owned RustFS -----------------------------------------------------------------------------


@dataclass(frozen=True)
class _OwnedStore:
    container: str
    volume: str
    port: int

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"


def _docker(
    *args: str, check: bool = True, timeout: float = 180
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=False, timeout=timeout
    )
    if check and completed.returncode != 0:
        raise AssertionError(f"docker {args[0]} failed: {completed.stderr.strip()[-500:]}")
    return completed


def _owned_containers() -> list[str]:
    listed = _docker("ps", "-a", "--filter", f"label={OWNED_LABEL}", "--format", "{{.Names}}")
    return [line for line in listed.stdout.splitlines() if line.strip()]


def _owned_volumes() -> list[str]:
    listed = _docker("volume", "ls", "--filter", f"label={OWNED_LABEL}", "--format", "{{.Name}}")
    return [line for line in listed.stdout.splitlines() if line.strip()]


def _container_present(name: str) -> bool:
    inspected = _docker("container", "inspect", name, check=False).returncode == 0
    return inspected or name in _owned_containers()


def _volume_present(name: str) -> bool:
    inspected = _docker("volume", "inspect", name, check=False).returncode == 0
    return inspected or name in _owned_volumes()


def _s3_client(endpoint: str, *, timeout_seconds: float = 10.0) -> Any:
    return build_recovery_s3_client(
        endpoint_url=endpoint,
        access_key=RUSTFS_ACCESS_KEY,
        secret_key=RUSTFS_SECRET_KEY,
        region=REGION,
        timeout_seconds=timeout_seconds,
    )


def _wait_for_store(endpoint: str) -> Any:
    client = _s3_client(endpoint, timeout_seconds=5.0)
    deadline = time.monotonic() + READY_TIMEOUT_SECONDS
    last = "no attempt"
    while time.monotonic() < deadline:
        try:
            client.list_buckets()
            return client
        except (BotoCoreError, ClientError) as exc:
            last = type(exc).__name__
        time.sleep(1.0)
    raise AssertionError(f"owned RustFS never answered list_buckets ({last})")


@contextmanager
def _owned_rustfs() -> Iterator[_OwnedStore]:
    """Run one RustFS container on a named volume, then prove both are gone."""

    _require_docker()
    suffix = secrets.token_hex(6)
    container = f"curie-recovery-test-{suffix}"
    volume = f"curie-recovery-test-{suffix}"
    try:
        _docker("volume", "create", "--label", OWNED_LABEL, volume)
        port = 0
        for _attempt in range(8):
            candidate = random.randint(20000, 32000)
            started = _docker(
                "run",
                "-d",
                "--name",
                container,
                "--label",
                OWNED_LABEL,
                "--user",
                "0",
                "-e",
                "RUSTFS_VOLUMES=/data",
                "-e",
                "RUSTFS_ADDRESS=0.0.0.0:9000",
                "-e",
                f"RUSTFS_ACCESS_KEY={RUSTFS_ACCESS_KEY}",
                "-e",
                f"RUSTFS_SECRET_KEY={RUSTFS_SECRET_KEY}",
                "-e",
                "RUSTFS_OBS_LOG_DIRECTORY=/tmp/logs",
                "-v",
                f"{volume}:/data",
                "-p",
                f"127.0.0.1:{candidate}:9000",
                RUSTFS_IMAGE,
                check=False,
                timeout=600,
            )
            if started.returncode == 0:
                port = candidate
                break
            stderr = started.stderr.lower()
            _docker("rm", "-f", "-v", container, check=False)
            if not any(word in stderr for word in ("allocated", "in use", "bind")):
                raise AssertionError(f"docker run failed: {started.stderr.strip()[-500:]}")
        if not port:
            raise AssertionError("no free published port for the owned RustFS")
        store = _OwnedStore(container=container, volume=volume, port=port)
        client = _wait_for_store(store.endpoint)
        client.create_bucket(Bucket=BUCKET)
        yield store
    finally:
        _docker("rm", "-f", "-v", container, check=False)
        _docker("volume", "rm", "-f", volume, check=False)
        assert not _container_present(container), f"owned container {container} survived"
        assert not _volume_present(volume), f"owned volume {volume} survived"


# --- owned Postgres ---------------------------------------------------------------------------


def _admin(base: URL, statement: str, **params: Any) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(
            base.set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
        )
        try:
            async with engine.connect() as connection:
                result = await connection.execute(text(statement), params)
                return [dict(row._mapping) for row in result] if result.returns_rows else []
        finally:
            await engine.dispose()

    return asyncio.run(run())


@contextmanager
def _owned_database() -> Iterator[str]:
    """Create and migrate a throwaway database, then drop it and prove it is gone."""

    _require_postgres()
    base = make_url(TEST_DATABASE_URL)
    name = f"curie_recovery_{secrets.token_hex(8)}"
    url = base.set(database=name).render_as_string(hide_password=False)
    _admin(base, f'CREATE DATABASE "{name}"')
    try:
        migrated = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=API_DIR,
            env={**os.environ, "DATABASE_URL": url},
            capture_output=True,
            text=True,
            check=False,
            timeout=900,
        )
        if migrated.returncode != 0:
            raise AssertionError(f"alembic upgrade head failed: {migrated.stderr[-1500:]}")
        yield url
    finally:
        _admin(base, f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        remaining = _admin(base, "SELECT 1 AS present FROM pg_database WHERE datname = :n", n=name)
        assert remaining == [], f"owned database {name} survived"


# --- real API ---------------------------------------------------------------------------------


@contextmanager
def _settings_env(values: Mapping[str, str]) -> Iterator[None]:
    """Set API env, clear the settings cache, and restore both afterwards.

    The router reads ``get_settings()`` per request, so the served API sees the
    change on its next call.
    """

    saved = {name: os.environ.get(name) for name in values}
    os.environ.update(values)
    get_settings.cache_clear()
    try:
        yield
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        get_settings.cache_clear()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@contextmanager
def _served_api() -> Iterator[str]:
    port = _free_port()
    config = uvicorn.Config(
        create_app(), host="127.0.0.1", port=port, log_config=None, log_level="warning"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, name="recovery-test-api", daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + READY_TIMEOUT_SECONDS
        while not server.started:
            if not thread.is_alive():
                raise AssertionError(
                    "the API exited during startup; its lifespan needs the configured "
                    "Valkey and bundle store"
                )
            if time.monotonic() > deadline:
                raise AssertionError("the API did not start in time")
            time.sleep(0.1)
        while True:
            try:
                if httpx.get(f"{url}/health", timeout=5.0).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                raise AssertionError("the API never answered /health")
            time.sleep(0.2)
        yield url
    finally:
        server.should_exit = True
        thread.join(timeout=30)


# --- synthetic repository and real workspace claims -------------------------------------------


def _git(cwd: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-c", "core.hooksPath=/dev/null", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        env={**os.environ, "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0"},
        timeout=120,
    )
    if completed.returncode != 0:
        raise AssertionError(f"git {args[0]} failed: {completed.stderr.strip()[-500:]}")
    return completed.stdout


def _binary(size: int, seed: int) -> bytes:
    return random.Random(seed).randbytes(size)


def _build_origin(root: Path) -> tuple[Path, str]:
    """A bare origin whose main holds text, binary and executable files."""

    work = root / "source"
    files: dict[str, bytes] = {
        "README.md": b"# acme bot\n",
        "src/app.py": b"print('acme')\n",
        "docs/remove-me.txt": b"deleted by the agent\n",
        "bin/run.sh": b"#!/bin/sh\necho run\n",
        "bin/tool.sh": b"#!/bin/sh\necho tool\n",
        "assets/blob.bin": _binary(4096, 1),
    }
    for relative, content in files.items():
        path = work / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    os.chmod(work / "bin/tool.sh", 0o755)
    _git(work, "init", "--quiet", "--initial-branch=main")
    _git(work, "add", "-A")
    _git(work, *GIT_IDENTITY, "commit", "--quiet", "-m", "seed")
    origin = root / "origin.git"
    _git(root, "clone", "--quiet", "--bare", str(work), str(origin))
    return origin, _git(work, "rev-parse", "HEAD").strip()


class _LocalOriginCommands:
    """Real git through SubprocessCommands; only the clone URL points at a local origin."""

    def __init__(self, origin: Path) -> None:
        self._real = SubprocessCommands()
        self._origin = origin

    def require(self, executable: str) -> None:
        self._real.require(executable)

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> CommandResult:
        args = [str(part) for part in argv]
        if "clone" in args or "fetch" in args:
            args = [str(self._origin) if part == CLEAN_URL else part for part in args]
            env = {**(env or {}), "GIT_ALLOW_PROTOCOL": "file"}
        return self._real.run(args, cwd=cwd, env=env, timeout_seconds=timeout_seconds)


class _CredentialClient:
    def redeem(self, deployment_id: uuid.UUID, conversation_id: str) -> WorkspaceCredential:
        del deployment_id, conversation_id
        return WorkspaceCredential(
            repo_full_name=REPOSITORY_PATH,
            clone_url=CLEAN_URL,
            authorization_header=GIT_CREDENTIAL,
            origin=CODE_HOST_ORIGIN,
            header_form="authorization_basic",
            base_branch="main",
        )


class _RecordingSubstrate:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def claim(self, thread_key: str, *, env: dict[str, str] | None = None, **_: object) -> object:
        del env
        self.calls.append(("claim", thread_key))
        return object()

    def resume(self, thread_key: str, *, env: dict[str, str] | None = None, **_: object) -> object:
        del env
        self.calls.append(("resume", thread_key))
        return object()


def _limits() -> WorkspaceLimits:
    return WorkspaceLimits(
        clone_timeout_seconds=90,
        archive_timeout_seconds=30,
        upload_timeout_seconds=30,
        total_timeout_seconds=150,
        max_checkout_bytes=512 * 1024 * 1024,
        max_archive_bytes=ARCHIVE_LIMIT,
        max_members=4096,
        max_compression_ratio=20.0,
        reference_ttl_seconds=300,
        max_concurrent_clones=2,
    )


def _claim(
    origin: Path, client: Any, scratch: Path, thread_key: str
) -> tuple[WorkspaceClaimCoordinator, PreparedWorkspace]:
    """Prepare and claim a real workspace whose base lives under private/workspaces/."""

    preparer = WorkspacePreparer(
        credentials=_CredentialClient(),
        commands=_LocalOriginCommands(origin),
        objects=WorkspaceObjectStore(client=client, bucket=BUCKET, prefix="private/workspaces"),
        scratch_root=scratch / "clone-scratch",
        limits=_limits(),
    )
    coordinator = WorkspaceClaimCoordinator(preparer=preparer, substrate=_RecordingSubstrate())
    result = coordinator.claim_or_resume_with_handle(
        thread_key=thread_key, deployment_id=DEPLOYMENT_ID
    )
    return coordinator, result.prepared


def _extract(archive: bytes, destination: Path) -> Path:
    destination.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        tar.extractall(destination, filter="tar")
    return destination


def _edit_and_capture(base_archive: bytes, base_sha: str, root: Path) -> tuple[bytes, bytes]:
    """Edit an extracted claim checkout every way the checkpoint covers, then capture it."""

    checkout = _extract(base_archive, root / "checkout")
    (checkout / "README.md").write_text("# acme bot\nlocally committed edit\n")
    _git(checkout, "add", "README.md")
    _git(checkout, *GIT_IDENTITY, "commit", "--quiet", "-m", "local edit")
    (checkout / "src/app.py").write_text("print('acme recovered')\n")
    (checkout / "notes").mkdir()
    (checkout / "notes/untracked.txt").write_text(f"agent scratch notes {CANARY}\n")
    (checkout / "assets/added.bin").write_bytes(b"\x00\x01" + _binary(2048, 2))
    (checkout / "assets/blob.bin").write_bytes(_binary(4096, 3))
    os.chmod(checkout / "bin/run.sh", 0o755)
    (checkout / "docs/remove-me.txt").unlink()
    captured = capture_workspace_checkpoint(checkout, base_sha=base_sha, budget_seconds=30)
    return captured.patch, captured.manifest


# --- harness ----------------------------------------------------------------------------------


@dataclass
class _Harness:
    store: _OwnedStore
    client: Any
    api_url: str
    db_url: str
    worker_token: str
    origin: Path
    base_commit: str
    coordinator: WorkspaceClaimCoordinator
    thread_key: str
    base_bytes: bytes
    base_sha: str
    patch: bytes
    manifest: bytes
    sentinels: dict[str, bytes]

    def base_source(self) -> RecoveryBaseSource:
        source = RecoveryBaseSource.from_claim(self.coordinator, self.thread_key)
        assert source is not None
        assert source.sha256 == self.base_sha
        return source

    def good_digests(self) -> dict[str, str]:
        return {
            "base": self.base_sha,
            "patch": _sha(self.patch),
            "manifest": _sha(self.manifest),
        }


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _file_sha(path: Path) -> str:
    return _sha(path.read_bytes())


def _physical(key: str) -> str:
    return f"private/{key}"


def _exists(harness: _Harness, physical: str) -> bool:
    try:
        harness.client.head_object(Bucket=BUCKET, Key=physical)
    except ClientError as exc:
        if str(exc.response.get("Error", {}).get("Code")) in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    return True


def _plant_sentinels(client: Any, claim_object_key: str, base_bytes: bytes) -> dict[str, bytes]:
    """Objects no recovery cleanup or collection may touch."""

    planted = {
        # A bucket key outside the private tree.
        "public/unrelated/sentinel.txt": b"outside the private prefix\n",
        # A recovery-shaped key that no ledger row names.
        f"private/workspace-recovery/checkpoints/{uuid.uuid4()}/patch": b"no ledger row\n",
        # A second claim-tree object beside the live claim base below.
        "private/workspaces/sentinel/unrelated-claim.tar.gz": b"claim tree sentinel\n",
    }
    for key, body in planted.items():
        client.put_object(Bucket=BUCKET, Key=key, Body=body)
    # The module's live claim base under private/workspaces/.
    planted[f"private/workspaces/{claim_object_key}"] = base_bytes
    return planted


def _assert_sentinels(harness: _Harness) -> None:
    for key, body in harness.sentinels.items():
        stored = harness.client.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        assert stored == body, f"sentinel {key} changed"


@pytest.fixture(scope="module")
def harness(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_Harness]:
    _require_docker()
    _require_postgres()
    root = tmp_path_factory.mktemp("workspace-recovery-storage")
    with ExitStack() as stack:
        store = stack.enter_context(_owned_rustfs())
        db_url = stack.enter_context(_owned_database())
        stack.enter_context(
            _settings_env(
                {
                    "DATABASE_URL": db_url,
                    "CURIE_WORKSPACE_RECOVERY_ENABLED": "true",
                    "CURIE_WORKSPACE_RECOVERY_MAX_BYTES": str(INSTALLATION_CAP),
                    "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES": str(ARCHIVE_LIMIT),
                }
            )
        )
        api_url = stack.enter_context(_served_api())
        client = _s3_client(store.endpoint)
        origin, base_commit = _build_origin(root)
        thread_key = "1700000000.000100"
        coordinator, prepared = _claim(origin, client, root, thread_key)
        stack.callback(coordinator.release, thread_key)
        assert prepared.base_sha == base_commit
        base_bytes = b"".join(coordinator.preparer.objects.get_stream(prepared.object_key))
        assert _sha(base_bytes) == prepared.sha256
        patch, manifest = _edit_and_capture(base_bytes, base_commit, root / "module-edit")
        yield _Harness(
            store=store,
            client=client,
            api_url=api_url,
            db_url=db_url,
            worker_token=get_settings().internal_worker_token,
            origin=origin,
            base_commit=base_commit,
            coordinator=coordinator,
            thread_key=thread_key,
            base_bytes=base_bytes,
            base_sha=prepared.sha256,
            patch=patch,
            manifest=manifest,
            sentinels=_plant_sentinels(client, prepared.object_key, base_bytes),
        )


# --- per-test context -------------------------------------------------------------------------


@dataclass
class _Ctx:
    harness: _Harness
    tmp: Path
    engine: AsyncEngine
    http: httpx.AsyncClient
    ledger: WorkspaceRecoveryClient
    store: RecoveryObjectStore


@asynccontextmanager
async def _context(harness: _Harness, tmp: Path) -> AsyncIterator[_Ctx]:
    engine = create_async_engine(harness.db_url, poolclass=NullPool)
    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            yield _Ctx(
                harness=harness,
                tmp=tmp,
                engine=engine,
                http=http,
                ledger=_ledger(harness, http),
                store=RecoveryObjectStore(client=harness.client, bucket=BUCKET),
            )
    finally:
        await engine.dispose()


def _ledger(harness: _Harness, http: httpx.AsyncClient) -> WorkspaceRecoveryClient:
    return WorkspaceRecoveryClient(
        api_base_url=harness.api_url, worker_token=harness.worker_token, client=http
    )


async def _sql(ctx: _Ctx, statement: str, **params: Any) -> list[dict[str, Any]]:
    async with ctx.engine.begin() as connection:
        result = await connection.execute(text(statement), params)
        return [dict(row._mapping) for row in result] if result.returns_rows else []


@dataclass(frozen=True)
class _Source:
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    base_commit: str


async def _seed(ctx: _Ctx) -> _Source:
    """A WorkItem and its failed source request, as apps/api/tests/test_workspace_recovery.py."""

    agent_id, item_id, request_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    async with ctx.engine.begin() as connection:
        await connection.execute(
            text("INSERT INTO curie.agents (id, name) VALUES (:id, :name)"),
            {"id": agent_id, "name": f"acme-recovery-{uuid.uuid4().hex[:12]}"},
        )
        await connection.execute(
            text(
                "INSERT INTO curie.work_items "
                "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
                "code_host_kind, code_host_host, repository_project_id, repository_path, "
                "code_host_installation_id, agent_id, conversation_id, version, next_sequence, "
                "base_branch, base_source, base_commit) VALUES "
                "(:id, 'github', 'github.com', '101', :issue, 'github', 'github.com', '101', "
                ":path, 202, :agent, :conversation, 7, 2, 'main', 'default', :base)"
            ),
            {
                "id": item_id,
                "issue": str(uuid.uuid4().int),
                "path": REPOSITORY_PATH,
                "agent": agent_id,
                "conversation": f"slack:C0EXAMPLE1:{uuid.uuid4().int}.000100",
                "base": ctx.harness.base_commit,
            },
        )
        await connection.execute(
            text(
                "INSERT INTO curie.execution_requests "
                "(id, work_item_id, sequence, status, wait_deadline, started_at, "
                "execution_deadline, terminal_at, terminal_cause, version, runtime_owner, "
                "runtime_epoch, runtime_heartbeat_expires_at, dispatch_generation, "
                "execution_attempts) VALUES "
                "(:id, :item, 1, 'failed', clock_timestamp() + interval '1 hour', "
                "clock_timestamp() - interval '1 minute', "
                "clock_timestamp() + interval '1 hour', "
                "clock_timestamp() - interval '1 second', 'runner_failed', 4, "
                "'fixture-worker', :epoch, clock_timestamp() + interval '1 hour', "
                ":generation, 1)"
            ),
            {
                "id": request_id,
                "item": item_id,
                "epoch": RUNTIME_EPOCH,
                "generation": DISPATCH_GENERATION,
            },
        )
    return _Source(work_item_id=item_id, request_id=request_id, base_commit=ctx.harness.base_commit)


def _authority(source: _Source) -> RecoveryAuthority:
    return RecoveryAuthority(
        work_item_id=source.work_item_id,
        source_request_id=source.request_id,
        runtime_epoch=RUNTIME_EPOCH,
        dispatch_generation=DISPATCH_GENERATION,
    )


def _request(
    source: _Source, *, patch: bytes, manifest: bytes, base: RecoveryBaseSource | None
) -> RecoveryCaptureRequest:
    return RecoveryCaptureRequest(
        authority=_authority(source),
        idempotency_key=uuid.uuid4().hex,
        repository=RecoveryRepository(
            provider="github", host="github.com", project_id="101", path=REPOSITORY_PATH
        ),
        base_commit=source.base_commit,
        target_branch="main",
        patch=patch,
        manifest=manifest,
        base=base,
    )


def _writer(
    ctx: _Ctx,
    *,
    store: RecoveryObjectStore | None = None,
    ledger: WorkspaceRecoveryClient | None = None,
    base_limit: int = ARCHIVE_LIMIT,
    capture_budget: float = 60.0,
) -> RecoveryCheckpointWriter:
    scratch = ctx.tmp / "recovery-scratch"
    scratch.mkdir(exist_ok=True)
    return RecoveryCheckpointWriter(
        ledger=ledger or ctx.ledger,
        store=store or ctx.store,
        scratch_root=scratch,
        base_archive_limit_bytes=base_limit,
        capture_budget_seconds=capture_budget,
        cleanup_budget_seconds=CLEANUP_BUDGET_SECONDS,
    )


def _reader(ctx: _Ctx, *, ttl: int = 60) -> RecoveryCheckpointReader:
    return RecoveryCheckpointReader(
        ledger=ctx.ledger, store=ctx.store, http=ctx.http, reference_ttl_seconds=ttl
    )


def _collector(ctx: _Ctx, collector_id: str | None = None) -> RecoveryCollector:
    return RecoveryCollector(
        ledger=ctx.ledger,
        store=ctx.store,
        collector_id=collector_id or f"recovery-test-{uuid.uuid4().hex[:12]}",
    )


async def _last_good(ctx: _Ctx, source: _Source) -> uuid.UUID | None:
    rows = await _sql(
        ctx,
        "SELECT last_good_checkpoint_id FROM curie.workspace_recovery_groups "
        "WHERE work_item_id = :id",
        id=source.work_item_id,
    )
    return rows[0]["last_good_checkpoint_id"] if rows else None


async def _checkpoint_rows(ctx: _Ctx, source: _Source) -> list[dict[str, Any]]:
    return await _sql(
        ctx,
        "SELECT id, status FROM curie.workspace_recovery_checkpoints WHERE work_item_id = :id",
        id=source.work_item_id,
    )


async def _checkpoint_status(ctx: _Ctx, checkpoint_id: uuid.UUID) -> str | None:
    rows = await _sql(
        ctx,
        "SELECT status FROM curie.workspace_recovery_checkpoints WHERE id = :id",
        id=checkpoint_id,
    )
    return str(rows[0]["status"]) if rows else None


async def _object_rows(ctx: _Ctx, checkpoint_id: uuid.UUID) -> list[dict[str, Any]]:
    return await _sql(
        ctx,
        "SELECT kind, key, status, collector_id FROM curie.workspace_recovery_objects "
        "WHERE checkpoint_id = :id ORDER BY kind",
        id=checkpoint_id,
    )


async def _checkpoint_keys(ctx: _Ctx, checkpoint_id: uuid.UUID) -> dict[str, str]:
    return {str(row["kind"]): str(row["key"]) for row in await _object_rows(ctx, checkpoint_id)}


async def _base_key(ctx: _Ctx, source: _Source) -> str | None:
    rows = await _sql(
        ctx,
        "SELECT key FROM curie.workspace_recovery_objects "
        "WHERE work_item_id = :id AND kind = 'base'",
        id=source.work_item_id,
    )
    return str(rows[0]["key"]) if rows else None


async def _expire_staging(ctx: _Ctx) -> None:
    # Injected time: the 600 s staging window elapses now instead of in ten minutes.
    await _sql(
        ctx,
        "UPDATE curie.workspace_recovery_checkpoints "
        "SET staging_expires_at = clock_timestamp() - interval '1 second' "
        "WHERE status = 'staging'",
    )


async def _expire_checkpoint(
    ctx: _Ctx, checkpoint_id: uuid.UUID, *, delay: str = "-1 second"
) -> None:
    # Injected time: the 24 h retention of a complete checkpoint ends at the given offset.
    await _sql(
        ctx,
        "UPDATE curie.workspace_recovery_checkpoints "
        "SET expires_at = clock_timestamp() + CAST(:delay AS interval) WHERE id = :id",
        id=checkpoint_id,
        delay=delay,
    )


async def _good_checkpoint(
    ctx: _Ctx, source: _Source, *, base: RecoveryBaseSource | None
) -> uuid.UUID:
    """A complete checkpoint of the module's real patch and manifest."""

    result = await _writer(ctx).capture(
        _request(source, patch=ctx.harness.patch, manifest=ctx.harness.manifest, base=base)
    )
    assert result.status == "complete", result
    assert result.failure is None and result.refusal is None
    assert result.cleanup_complete is True
    assert result.checkpoint_id is not None
    assert await _last_good(ctx, source) == result.checkpoint_id
    return result.checkpoint_id


async def _read_all(
    ctx: _Ctx,
    reader: RecoveryCheckpointReader,
    lease: RecoveryReadLease,
    expected: Mapping[str, str],
) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    for kind, digest in expected.items():
        destination = ctx.tmp / f"read-{uuid.uuid4().hex[:8]}-{kind}"
        await reader.read(lease, kind, destination)
        assert _file_sha(destination) == digest, f"{kind} bytes differ"
        assert stat.S_IMODE(destination.stat().st_mode) == 0o600
        paths[kind] = destination
    return paths


async def _assert_readable(
    ctx: _Ctx, source: _Source, checkpoint_id: uuid.UUID, expected: Mapping[str, str]
) -> None:
    reader = _reader(ctx)
    lease = await reader.acquire(
        work_item_id=source.work_item_id,
        checkpoint_id=checkpoint_id,
        holder_id=f"verify-{uuid.uuid4().hex[:12]}",
    )
    try:
        assert set(lease.objects) == {"base", "patch", "manifest"}
        for path in (await _read_all(ctx, reader, lease, expected)).values():
            path.unlink()
    finally:
        await reader.release(lease)
    assert lease.released is True


async def _drain(ctx: _Ctx, collector: RecoveryCollector) -> list[RecoveryCollectionPass]:
    """Collect until nothing is claimable, then prove one more pass is a no-op."""

    passes: list[RecoveryCollectionPass] = []
    for _ in range(8):
        collected = await collector.collect_once()
        passes.append(collected)
        assert collected.failed == 0 and collected.conflicts == 0, collected
        assert collected.finalized == collected.claimed, collected
        if collected.claimed == 0:
            break
    else:
        raise AssertionError(f"collection never settled: {passes}")
    assert await collector.collect_once() == EMPTY_PASS
    return passes


def _assert_failed(
    result: RecoveryCaptureResult,
    failure: str,
    *,
    reserved: bool,
    cleanup_complete: bool = True,
    refusal: str | None = None,
) -> None:
    assert isinstance(result, RecoveryCaptureResult)
    assert result.status == "failed", result
    assert result.failure == failure, result
    assert result.refusal == refusal, result
    assert result.cleanup_complete is cleanup_complete, result
    assert (result.checkpoint_id is not None) is reserved, result


async def _settle_after_failure(
    ctx: _Ctx,
    *,
    previous: tuple[_Source, uuid.UUID],
    keys: Sequence[str] = (),
    failed_checkpoint: uuid.UUID | None = None,
    pending_before_collection: bool = False,
) -> None:
    """The user outcome of every failure: the last good checkpoint is still the answer."""

    source, last_good = previous
    harness = ctx.harness
    assert await _last_good(ctx, source) == last_good
    await _assert_readable(ctx, source, last_good, harness.good_digests())
    if not pending_before_collection:
        for key in keys:
            assert ctx.store.pending_uploads(key) == [], key
    _assert_sentinels(harness)

    await _expire_staging(ctx)
    await _drain(ctx, _collector(ctx))

    for key in keys:
        assert ctx.store.pending_uploads(key) == [], key
        assert not _exists(harness, _physical(key)), key
    if failed_checkpoint is not None:
        assert await _checkpoint_status(ctx, failed_checkpoint) is None
    assert await _last_good(ctx, source) == last_good
    await _assert_readable(ctx, source, last_good, harness.good_digests())
    _assert_sentinels(harness)


# --- injected faults --------------------------------------------------------------------------


def _hard_close(sock: socket.socket) -> None:
    with suppress(OSError):
        sock.shutdown(socket.SHUT_RDWR)
    with suppress(OSError):
        sock.close()


class _FaultProxy:
    """Threaded TCP proxy in front of the owned store: the only storage fault injector.

    Modes: ``pass`` forwards both directions; ``stall`` accepts a connection and
    never forwards or answers it; ``cut_after(n)`` forwards ``n`` more client
    bytes in total, then closes both ends of the connection carrying the last
    byte and returns to ``pass``. ``close()`` stops listening and shuts every
    socket so a boto thread blocked on a stalled read unblocks.
    """

    def __init__(self, upstream_port: int) -> None:
        self._upstream = ("127.0.0.1", upstream_port)
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(0.2)
        self.port = int(self._listener.getsockname()[1])
        self._lock = threading.Lock()
        self._mode = "pass"
        self._cut_remaining = 0
        self.cuts = 0
        self.accepted = 0
        self._sockets: set[socket.socket] = set()
        self._closed = threading.Event()
        self._threads: list[threading.Thread] = []
        self._spawn(self._accept_loop)

    @property
    def endpoint(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stall(self) -> None:
        with self._lock:
            self._mode = "stall"

    def cut_after(self, client_bytes: int) -> None:
        with self._lock:
            self._mode = "cut"
            self._cut_remaining = client_bytes

    def close(self) -> None:
        self._closed.set()
        with suppress(OSError):
            self._listener.close()
        with self._lock:
            sockets = list(self._sockets)
            self._sockets.clear()
        for sock in sockets:
            _hard_close(sock)
        for thread in self._threads:
            thread.join(timeout=5)

    def _spawn(self, target: Callable[..., None], *args: object) -> None:
        thread = threading.Thread(target=target, args=args, daemon=True)
        thread.start()
        self._threads.append(thread)

    def _track(self, sock: socket.socket) -> bool:
        with self._lock:
            if self._closed.is_set():
                return False
            self._sockets.add(sock)
            return True

    def _release(self, *socks: socket.socket) -> None:
        with self._lock:
            for sock in socks:
                self._sockets.discard(sock)
        for sock in socks:
            _hard_close(sock)

    def _accept_loop(self) -> None:
        while not self._closed.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            client.settimeout(None)
            if not self._track(client):
                _hard_close(client)
                return
            with self._lock:
                self.accepted += 1
                mode = self._mode
            if mode == "stall":
                continue  # held open, never read and never answered
            try:
                upstream = socket.create_connection(self._upstream, timeout=10)
            except OSError:
                self._release(client)
                continue
            upstream.settimeout(None)
            if not self._track(upstream):
                self._release(client, upstream)
                return
            self._spawn(self._pump_request, client, upstream)
            self._spawn(self._pump_response, upstream, client)

    def _pump_request(self, client: socket.socket, upstream: socket.socket) -> None:
        while True:
            try:
                data = client.recv(65536)
            except OSError:
                break
            if not data:
                break
            cut = False
            with self._lock:
                if self._mode == "cut":
                    if len(data) >= self._cut_remaining:
                        data = data[: self._cut_remaining]
                        self._cut_remaining = 0
                        self._mode = "pass"
                        self.cuts += 1
                        cut = True
                    else:
                        self._cut_remaining -= len(data)
            try:
                if data:
                    upstream.sendall(data)
            except OSError:
                break
            if cut:
                break
        self._release(client, upstream)

    def _pump_response(self, upstream: socket.socket, client: socket.socket) -> None:
        while True:
            try:
                data = upstream.recv(65536)
            except OSError:
                break
            if not data:
                break
            try:
                client.sendall(data)
            except OSError:
                break
        self._release(client, upstream)


def _proxied_store(proxy: _FaultProxy) -> RecoveryObjectStore:
    return RecoveryObjectStore(client=_s3_client(proxy.endpoint), bucket=BUCKET)


class _PointerWriteFault(httpx.AsyncBaseTransport):
    """Real HTTP to the API, except every ``/complete`` call is lost before it is sent."""

    def __init__(self) -> None:
        self._inner = httpx.AsyncHTTPTransport()
        self.complete_calls = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/workspace-recovery/complete"):
            self.complete_calls += 1
            # Injected fault: the API connection drops while the worker writes the
            # checkpoint pointer, so the ledger never records the completion.
            raise httpx.ConnectError("injected pointer-write fault", request=request)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


# --- AC1: recovery outlives the claim ---------------------------------------------------------


@pytest.mark.anyio
async def test_recovery_survives_claim_release(harness: _Harness, tmp_path: Path) -> None:
    async with _context(harness, tmp_path) as ctx:
        thread_key = "1700000000.000201"
        coordinator, prepared = _claim(harness.origin, harness.client, tmp_path, thread_key)
        base_archive = b"".join(coordinator.preparer.objects.get_stream(prepared.object_key))
        assert _sha(base_archive) == prepared.sha256
        edited = tmp_path / "edited"
        patch, manifest = _edit_and_capture(base_archive, prepared.base_sha, edited)
        base = RecoveryBaseSource.from_claim(coordinator, thread_key)
        assert base is not None and base.sha256 == prepared.sha256

        source = await _seed(ctx)
        result = await _writer(ctx).capture(
            _request(source, patch=patch, manifest=manifest, base=base)
        )
        assert result.status == "complete", result
        assert result.failure is None and result.cleanup_complete is True
        assert result.checkpoint_id is not None
        assert await _last_good(ctx, source) == result.checkpoint_id
        recovery_keys = await _checkpoint_keys(ctx, result.checkpoint_id)
        base_key = await _base_key(ctx, source)
        assert base_key is not None and base_key.startswith("workspace-recovery/retained/")
        recovery_keys["base"] = base_key

        # The claim ends: its base object and ownership ledger go, and the sandbox
        # checkout is gone with it.
        coordinator.release(thread_key)
        assert coordinator.current(thread_key) is None
        assert RecoveryBaseSource.from_claim(coordinator, thread_key) is None
        assert not _exists(harness, f"private/workspaces/{prepared.object_key}")
        ownership = hashlib.sha256(thread_key.encode()).hexdigest()
        assert not _exists(harness, f"private/workspaces/_ownership/{ownership}.json")
        shutil.rmtree(edited)
        for key in recovery_keys.values():
            assert _exists(harness, _physical(key)), key

        reader = _reader(ctx)
        lease = await reader.acquire(
            work_item_id=source.work_item_id,
            checkpoint_id=result.checkpoint_id,
            holder_id="restoring-worker",
        )
        try:
            assert {kind: ref.key for kind, ref in lease.objects.items()} == recovery_keys
            paths = await _read_all(
                ctx,
                reader,
                lease,
                {"base": prepared.sha256, "patch": _sha(patch), "manifest": _sha(manifest)},
            )
        finally:
            await reader.release(lease)

        restored = _extract(paths["base"].read_bytes(), tmp_path / "restored")
        _git(restored, "apply", "--binary", str(paths["patch"]))
        document = json.loads(paths["manifest"].read_bytes())
        assert document["base_sha"] == prepared.base_sha
        entries = {entry["path"]: entry for entry in document["entries"]}
        assert {
            "README.md",
            "src/app.py",
            "notes/untracked.txt",
            "assets/added.bin",
            "assets/blob.bin",
            "bin/run.sh",
            "docs/remove-me.txt",
        } <= set(entries)
        for path, entry in entries.items():
            target = restored / path
            if entry.get("deleted"):
                assert not target.exists() and not target.is_symlink(), path
                continue
            data = target.read_bytes()
            assert _sha(data) == entry["sha256"], path
            assert len(data) == entry["size"], path
            executable = bool(target.stat().st_mode & stat.S_IXUSR)
            assert executable is (entry["mode"] == "100755"), path
        assert entries["bin/run.sh"]["mode"] == "100755"
        assert entries["docs/remove-me.txt"].get("deleted") is True
        assert (restored / "bin/tool.sh").stat().st_mode & stat.S_IXUSR
        for key in recovery_keys.values():
            assert _exists(harness, _physical(key)), key
        _assert_sentinels(harness)


# --- AC2: store faults and cap boundaries -----------------------------------------------------


async def _case_storage_timeout(ctx: _Ctx) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    proxy = _FaultProxy(ctx.harness.store.port)
    try:
        # Injected fault: a store endpoint that accepts connections and never
        # answers, like a hung load balancer or a frozen storage node.
        proxy.stall()
        writer = _writer(ctx, store=_proxied_store(proxy), capture_budget=3.0)
        started = time.monotonic()
        result = await writer.capture(
            _request(source, patch=_binary(256 * 1024, 10), manifest=b"{}", base=None)
        )
        elapsed = time.monotonic() - started
        assert proxy.accepted >= 1
    finally:
        proxy.close()
    _assert_failed(result, "capture_timeout", reserved=True, cleanup_complete=False)
    assert elapsed <= 3.0 + CLEANUP_BUDGET_SECONDS + TIMING_SLACK_SECONDS, elapsed
    assert result.checkpoint_id is not None
    keys = await _checkpoint_keys(ctx, result.checkpoint_id)
    assert set(keys) == {"patch", "manifest"}
    await _settle_after_failure(
        ctx,
        previous=(source, previous),
        keys=list(keys.values()),
        failed_checkpoint=result.checkpoint_id,
    )


async def _case_multipart_cut(ctx: _Ctx) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    proxy = _FaultProxy(ctx.harness.store.port)
    try:
        # Injected fault: the connection resets in the middle of the patch part
        # upload; the store stays reachable for the abort that follows.
        proxy.cut_after(64 * 1024)
        result = await _writer(ctx, store=_proxied_store(proxy)).capture(
            _request(source, patch=_binary(512 * 1024, 11), manifest=b"{}", base=None)
        )
        assert proxy.cuts == 1
    finally:
        proxy.close()
    _assert_failed(result, "upload_failed", reserved=True)
    assert result.checkpoint_id is not None
    assert await _checkpoint_status(ctx, result.checkpoint_id) == "tombstoned"
    keys = await _checkpoint_keys(ctx, result.checkpoint_id)
    for key in keys.values():
        assert ctx.store.pending_uploads(key) == [], key
        assert not _exists(ctx.harness, _physical(key)), key
    await _settle_after_failure(
        ctx,
        previous=(source, previous),
        keys=list(keys.values()),
        failed_checkpoint=result.checkpoint_id,
    )


async def _case_incomplete_upload(ctx: _Ctx) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    patch, manifest = _binary(128 * 1024, 12), b"{}"
    reservation = await ctx.ledger.reserve(
        {
            "work_item_id": str(source.work_item_id),
            "source_request_id": str(source.request_id),
            "runtime_epoch": RUNTIME_EPOCH,
            "dispatch_generation": DISPATCH_GENERATION,
            "idempotency_key": uuid.uuid4().hex,
            "repository": {
                "provider": "github",
                "host": "github.com",
                "project_id": "101",
                "path": REPOSITORY_PATH,
            },
            "base_commit": source.base_commit,
            "target_branch": "main",
            "base_archive_limit_bytes": ARCHIVE_LIMIT,
            "objects": [
                {"kind": "patch", "size_bytes": len(patch), "sha256": _sha(patch)},
                {"kind": "manifest", "size_bytes": len(manifest), "sha256": _sha(manifest)},
            ],
        }
    )
    checkpoint_id = uuid.UUID(reservation["checkpoint_id"])
    keys = {obj["kind"]: obj["key"] for obj in reservation["objects"]}
    # Injected fault: the capturing worker process died mid-upload. Its multipart
    # upload was started and one part sent, and nothing ever completed or aborted it.
    upload = ctx.harness.client.create_multipart_upload(Bucket=BUCKET, Key=_physical(keys["patch"]))
    ctx.harness.client.upload_part(
        Bucket=BUCKET,
        Key=_physical(keys["patch"]),
        UploadId=upload["UploadId"],
        PartNumber=1,
        Body=patch[: 64 * 1024],
    )
    assert ctx.store.pending_uploads(keys["patch"]) == [upload["UploadId"]]
    assert await _checkpoint_status(ctx, checkpoint_id) == "staging"
    await _settle_after_failure(
        ctx,
        previous=(source, previous),
        keys=list(keys.values()),
        failed_checkpoint=checkpoint_id,
        pending_before_collection=True,
    )


async def _case_pointer_write(ctx: _Ctx) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    fault = _PointerWriteFault()
    async with httpx.AsyncClient(transport=fault, timeout=30.0) as faulty:
        result = await _writer(ctx, ledger=_ledger(ctx.harness, faulty)).capture(
            _request(source, patch=_binary(64 * 1024, 13), manifest=b"{}", base=None)
        )
    _assert_failed(result, "complete_failed", reserved=True)
    assert fault.complete_calls == 2  # the idempotent pointer write is retried once
    assert result.checkpoint_id is not None
    assert await _checkpoint_status(ctx, result.checkpoint_id) == "tombstoned"
    keys = await _checkpoint_keys(ctx, result.checkpoint_id)
    for key in keys.values():
        assert not _exists(ctx.harness, _physical(key)), key
    await _settle_after_failure(
        ctx,
        previous=(source, previous),
        keys=list(keys.values()),
        failed_checkpoint=result.checkpoint_id,
    )


async def _case_corrupt_base(ctx: _Ctx) -> None:
    other = await _seed(ctx)
    previous = await _good_checkpoint(ctx, other, base=ctx.harness.base_source())
    source = await _seed(ctx)
    corrupted = bytearray(ctx.harness.base_bytes)
    # Injected fault: the claim's base archive changed at rest after its digest was
    # recorded, so the bytes no longer match the trusted digest.
    corrupted[len(corrupted) // 2] ^= 0xFF
    candidate = RecoveryBaseSource(
        sha256=ctx.harness.base_sha, open=lambda: iter([bytes(corrupted)])
    )
    result = await _writer(ctx).capture(
        _request(source, patch=ctx.harness.patch, manifest=ctx.harness.manifest, base=candidate)
    )
    _assert_failed(result, "base_digest_mismatch", reserved=False)
    assert await _checkpoint_rows(ctx, source) == []
    assert await _base_key(ctx, source) is None
    retained = ctx.harness.client.list_objects_v2(
        Bucket=BUCKET, Prefix=f"private/workspace-recovery/retained/{source.work_item_id}/"
    )
    assert retained.get("KeyCount", 0) == 0
    await _settle_after_failure(ctx, previous=(other, previous))


async def _case_storage_outage(ctx: _Ctx) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    proxy = _FaultProxy(ctx.harness.store.port)
    store = _proxied_store(proxy)
    # Injected fault: the store endpoint is down; every connection is refused.
    proxy.close()
    result = await _writer(ctx, store=store).capture(
        _request(source, patch=_binary(64 * 1024, 14), manifest=b"{}", base=None)
    )
    _assert_failed(result, "upload_failed", reserved=True, cleanup_complete=False)
    assert result.checkpoint_id is not None
    assert await _checkpoint_status(ctx, result.checkpoint_id) == "tombstoned"
    keys = await _checkpoint_keys(ctx, result.checkpoint_id)
    await _settle_after_failure(
        ctx,
        previous=(source, previous),
        keys=list(keys.values()),
        failed_checkpoint=result.checkpoint_id,
    )


async def _accepted_second_capture(ctx: _Ctx, *, patch: bytes, manifest: bytes) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    result = await _writer(ctx).capture(_request(source, patch=patch, manifest=manifest, base=None))
    assert result.status == "complete", result
    assert result.checkpoint_id is not None and result.checkpoint_id != previous
    assert await _last_good(ctx, source) == result.checkpoint_id
    digests = {"base": ctx.harness.base_sha, "patch": _sha(patch), "manifest": _sha(manifest)}
    await _assert_readable(ctx, source, result.checkpoint_id, digests)
    await _drain(ctx, _collector(ctx))
    await _assert_readable(ctx, source, result.checkpoint_id, digests)
    await _assert_readable(ctx, source, previous, ctx.harness.good_digests())
    _assert_sentinels(ctx.harness)


async def _refused_second_capture(
    ctx: _Ctx, *, patch: bytes, manifest: bytes, failure: str
) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    result = await _writer(ctx).capture(_request(source, patch=patch, manifest=manifest, base=None))
    _assert_failed(result, failure, reserved=False)
    assert [row["id"] for row in await _checkpoint_rows(ctx, source)] == [previous]
    await _settle_after_failure(ctx, previous=(source, previous))


async def _case_patch_at_limit(ctx: _Ctx) -> None:
    await _accepted_second_capture(
        ctx, patch=_binary(PATCH_LIMIT_BYTES, 20), manifest=ctx.harness.manifest
    )


async def _case_patch_over_limit(ctx: _Ctx) -> None:
    await _refused_second_capture(
        ctx,
        patch=_binary(PATCH_LIMIT_BYTES + 1, 21),
        manifest=ctx.harness.manifest,
        failure="patch_too_large",
    )


async def _case_manifest_at_limit(ctx: _Ctx) -> None:
    await _accepted_second_capture(
        ctx, patch=ctx.harness.patch, manifest=_binary(MANIFEST_LIMIT_BYTES, 22)
    )


async def _case_manifest_over_limit(ctx: _Ctx) -> None:
    await _refused_second_capture(
        ctx,
        patch=ctx.harness.patch,
        manifest=_binary(MANIFEST_LIMIT_BYTES + 1, 23),
        failure="manifest_too_large",
    )


async def _case_base_at_limit(ctx: _Ctx) -> None:
    limit = len(ctx.harness.base_bytes)
    source = await _seed(ctx)
    with _settings_env({"CURIE_WORKSPACE_MAX_ARCHIVE_BYTES": str(limit)}):
        result = await _writer(ctx, base_limit=limit).capture(
            _request(
                source,
                patch=ctx.harness.patch,
                manifest=ctx.harness.manifest,
                base=ctx.harness.base_source(),
            )
        )
    assert result.status == "complete", result
    assert result.checkpoint_id is not None
    assert await _last_good(ctx, source) == result.checkpoint_id
    await _assert_readable(ctx, source, result.checkpoint_id, ctx.harness.good_digests())
    await _drain(ctx, _collector(ctx))
    await _assert_readable(ctx, source, result.checkpoint_id, ctx.harness.good_digests())
    _assert_sentinels(ctx.harness)


async def _case_base_over_limit(ctx: _Ctx) -> None:
    other = await _seed(ctx)
    previous = await _good_checkpoint(ctx, other, base=ctx.harness.base_source())
    limit = len(ctx.harness.base_bytes) - 1
    source = await _seed(ctx)
    with _settings_env({"CURIE_WORKSPACE_MAX_ARCHIVE_BYTES": str(limit)}):
        result = await _writer(ctx, base_limit=limit).capture(
            _request(
                source,
                patch=ctx.harness.patch,
                manifest=ctx.harness.manifest,
                base=ctx.harness.base_source(),
            )
        )
    _assert_failed(result, "base_too_large", reserved=False)
    assert await _checkpoint_rows(ctx, source) == []
    assert await _base_key(ctx, source) is None
    await _settle_after_failure(ctx, previous=(other, previous))


async def _installation_cap(ctx: _Ctx, declared: int, *, over: int) -> str:
    used = await _sql(
        ctx,
        "SELECT COALESCE(SUM(size_bytes), 0) AS used FROM curie.workspace_recovery_objects",
    )
    return str(int(used[0]["used"]) + declared - over)


async def _case_installation_exact_fit(ctx: _Ctx) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    patch, manifest = _binary(32 * 1024, 30), b'{"exact":"fit"}'
    cap = await _installation_cap(ctx, len(patch) + len(manifest), over=0)
    with _settings_env({"CURIE_WORKSPACE_RECOVERY_MAX_BYTES": cap}):
        result = await _writer(ctx).capture(
            _request(source, patch=patch, manifest=manifest, base=None)
        )
    assert result.status == "complete", result
    assert result.checkpoint_id is not None and result.checkpoint_id != previous
    digests = {"base": ctx.harness.base_sha, "patch": _sha(patch), "manifest": _sha(manifest)}
    await _assert_readable(ctx, source, result.checkpoint_id, digests)
    await _assert_readable(ctx, source, previous, ctx.harness.good_digests())
    _assert_sentinels(ctx.harness)


async def _case_installation_over_cap(ctx: _Ctx) -> None:
    source = await _seed(ctx)
    previous = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    patch, manifest = _binary(32 * 1024, 31), b'{"over":"cap"}'
    cap = await _installation_cap(ctx, len(patch) + len(manifest), over=1)
    with _settings_env({"CURIE_WORKSPACE_RECOVERY_MAX_BYTES": cap}):
        result = await _writer(ctx).capture(
            _request(source, patch=patch, manifest=manifest, base=None)
        )
    _assert_failed(
        result,
        "reserve_refused",
        reserved=False,
        refusal="workspace_recovery.installation_capacity",
    )
    outcome = await _sql(
        ctx,
        "SELECT workspace_recovery_outcome FROM curie.work_items WHERE id = :id",
        id=source.work_item_id,
    )
    assert outcome[0]["workspace_recovery_outcome"] == "installation_capacity"
    assert [row["id"] for row in await _checkpoint_rows(ctx, source)] == [previous]
    await _settle_after_failure(ctx, previous=(source, previous))


_FAULT_CASES: dict[str, Callable[[_Ctx], Awaitable[None]]] = {
    "storage_timeout": _case_storage_timeout,
    "multipart_cut": _case_multipart_cut,
    "incomplete_upload": _case_incomplete_upload,
    "pointer_write": _case_pointer_write,
    "corrupt_base": _case_corrupt_base,
    "storage_outage": _case_storage_outage,
    "patch_at_limit": _case_patch_at_limit,
    "patch_over_limit": _case_patch_over_limit,
    "manifest_at_limit": _case_manifest_at_limit,
    "manifest_over_limit": _case_manifest_over_limit,
    "base_at_limit": _case_base_at_limit,
    "base_over_limit": _case_base_over_limit,
    "installation_exact_fit": _case_installation_exact_fit,
    "installation_over_cap": _case_installation_over_cap,
}


@pytest.mark.anyio
@pytest.mark.parametrize("case", list(_FAULT_CASES))
async def test_recovery_store_faults(harness: _Harness, tmp_path: Path, case: str) -> None:
    async with _context(harness, tmp_path) as ctx:
        await _FAULT_CASES[case](ctx)


# --- AC2: collection races --------------------------------------------------------------------

_SELECTION_REFUSALS = {
    "workspace_recovery.checkpoint_unavailable",
    "workspace_recovery.checkpoint_identity",
    "workspace_recovery.checkpoint_incomplete",
}


async def _race_reader_before_tombstone(ctx: _Ctx, collector: RecoveryCollector) -> None:
    source = await _seed(ctx)
    checkpoint = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    keys = await _checkpoint_keys(ctx, checkpoint)
    base_key = await _base_key(ctx, source)
    assert base_key is not None
    reader = _reader(ctx)
    lease = await reader.acquire(
        work_item_id=source.work_item_id, checkpoint_id=checkpoint, holder_id="early-reader"
    )
    await _expire_checkpoint(ctx, checkpoint)
    pinned = await collector.collect_once()
    assert pinned.failed == 0 and pinned.conflicts == 0
    assert await _checkpoint_status(ctx, checkpoint) == "tombstoned"
    assert await _last_good(ctx, source) is None
    assert {row["status"] for row in await _object_rows(ctx, checkpoint)} == {"tombstoned"}
    for key in (*keys.values(), base_key):
        assert _exists(ctx.harness, _physical(key)), key
    for path in (await _read_all(ctx, reader, lease, ctx.harness.good_digests())).values():
        path.unlink()
    await reader.release(lease)
    await _drain(ctx, collector)
    for key in (*keys.values(), base_key):
        assert not _exists(ctx.harness, _physical(key)), key
    assert await _checkpoint_status(ctx, checkpoint) is None
    assert await _last_good(ctx, source) is None


async def _race_tombstone_before_reader(ctx: _Ctx, collector: RecoveryCollector) -> None:
    source = await _seed(ctx)
    checkpoint = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    await _expire_checkpoint(ctx, checkpoint)
    reader = _reader(ctx)
    with pytest.raises(RecoveryReadRefused) as expired:
        await reader.acquire(
            work_item_id=source.work_item_id, checkpoint_id=checkpoint, holder_id="late-reader"
        )
    assert expired.value.code == "workspace_recovery.checkpoint_unavailable"
    await _drain(ctx, collector)
    with pytest.raises(RecoveryReadRefused) as collected:
        await reader.acquire(
            work_item_id=source.work_item_id, checkpoint_id=checkpoint, holder_id="late-reader"
        )
    assert collected.value.code in _SELECTION_REFUSALS


async def _race_acquire_against_collection(
    ctx: _Ctx, collector: RecoveryCollector, offset: float
) -> str:
    source = await _seed(ctx)
    checkpoint = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    reader = _reader(ctx)
    # Injected time: retention ends a moment from now, so the reader and the
    # collector race across the expiry instant.
    await _expire_checkpoint(ctx, checkpoint, delay="300 milliseconds")
    await asyncio.sleep(offset)
    barrier = asyncio.Barrier(2)

    async def acquire() -> RecoveryReadLease | RecoveryReadRefused:
        await barrier.wait()
        try:
            return await reader.acquire(
                work_item_id=source.work_item_id, checkpoint_id=checkpoint, holder_id="racer"
            )
        except RecoveryReadRefused as refused:
            return refused

    async def collect() -> RecoveryCollectionPass:
        await barrier.wait()
        return await collector.collect_once()

    acquired, collected = await asyncio.gather(acquire(), collect())
    assert collected.failed == 0 and collected.conflicts == 0, collected
    if isinstance(acquired, RecoveryReadRefused):
        assert acquired.code in _SELECTION_REFUSALS, acquired.code
        outcome = "refused"
    else:
        # Whatever the collector did, a lease that was granted reads verified bytes.
        for path in (await _read_all(ctx, reader, acquired, ctx.harness.good_digests())).values():
            path.unlink()
        await reader.release(acquired)
        outcome = "leased"
    await _expire_checkpoint(ctx, checkpoint)
    await _drain(ctx, collector)
    return outcome


async def _race_interrupted_collector(ctx: _Ctx, collector: RecoveryCollector) -> None:
    source = await _seed(ctx)
    older = await _good_checkpoint(ctx, source, base=ctx.harness.base_source())
    newer = await _good_checkpoint(ctx, source, base=None)
    await _drain(ctx, collector)
    await _expire_checkpoint(ctx, older)
    keys = await _checkpoint_keys(ctx, older)
    collector_id = f"interrupted-{uuid.uuid4().hex[:12]}"
    completed = subprocess.run(
        [sys.executable, str(INTERRUPT_SCRIPT)],
        env={
            **os.environ,
            "RECOVERY_API_URL": ctx.harness.api_url,
            "RECOVERY_WORKER_TOKEN": ctx.harness.worker_token,
            "RECOVERY_S3_ENDPOINT": ctx.harness.store.endpoint,
            "RECOVERY_S3_ACCESS_KEY": RUSTFS_ACCESS_KEY,
            "RECOVERY_S3_SECRET_KEY": RUSTFS_SECRET_KEY,
            "RECOVERY_BUCKET": BUCKET,
            "RECOVERY_COLLECTOR_ID": collector_id,
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    assert completed.returncode == EXIT_BEFORE_FINALIZE, completed.stderr[-1500:]
    rows = await _object_rows(ctx, older)
    assert {row["status"] for row in rows} == {"claimed"}
    assert {row["collector_id"] for row in rows} == {collector_id}
    assert any(not _exists(ctx.harness, _physical(key)) for key in keys.values())

    restarted = _collector(ctx, collector_id)
    resumed = await restarted.collect_once()
    assert resumed == RecoveryCollectionPass(
        claimed=2, deleted=2, finalized=2, conflicts=0, failed=0
    )
    for key in keys.values():
        assert not _exists(ctx.harness, _physical(key)), key
    assert await _checkpoint_status(ctx, older) is None
    assert await restarted.collect_once() == EMPTY_PASS
    assert await _last_good(ctx, source) == newer
    await _assert_readable(ctx, source, newer, ctx.harness.good_digests())


@pytest.mark.anyio
async def test_recovery_gc_races(harness: _Harness, tmp_path: Path) -> None:
    async with _context(harness, tmp_path) as ctx:
        collector = _collector(ctx, "gc-race-collector")
        await _drain(ctx, collector)
        await _race_reader_before_tombstone(ctx, collector)
        await _race_tombstone_before_reader(ctx, collector)
        outcomes = [
            await _race_acquire_against_collection(ctx, collector, offset)
            for offset in (0.0, 0.1, 0.2, 0.3, 0.4, 0.5)
        ]
        assert len(outcomes) == 6 and set(outcomes) <= {"leased", "refused"}
        await _race_interrupted_collector(ctx, collector)
        _assert_sentinels(harness)


# --- AC3: private read capabilities -----------------------------------------------------------


def _expires(url: str) -> int:
    return int(parse_qs(urlsplit(url).query)["X-Amz-Expires"][0])


def _record_text(record: logging.LogRecord) -> str:
    parts = [record.getMessage()]
    if record.exc_info:
        parts.append("".join(traceback.format_exception(*record.exc_info)))
    if record.exc_text:
        parts.append(record.exc_text)
    return "\n".join(parts)


@pytest.mark.anyio
async def test_recovery_private_read_capabilities(
    harness: _Harness, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="curie_worker")
    assert CANARY.encode() in harness.patch
    async with _context(harness, tmp_path) as ctx:
        source = await _seed(ctx)
        checkpoint = await _good_checkpoint(ctx, source, base=harness.base_source())
        keys = await _checkpoint_keys(ctx, checkpoint)
        base_key = await _base_key(ctx, source)
        assert base_key is not None
        keys["base"] = base_key

        async with httpx.AsyncClient(timeout=15.0) as anonymous:
            for key in keys.values():
                anonymous_url = f"{harness.store.endpoint}/{BUCKET}/{_physical(key)}"
                response = await anonymous.get(anonymous_url)
                assert response.status_code == 403, key
            listing = await anonymous.get(
                f"{harness.store.endpoint}/{BUCKET}",
                params={"list-type": "2", "prefix": "private/"},
            )
            assert listing.status_code == 403

            reader = _reader(ctx, ttl=60)
            lease = await reader.acquire(
                work_item_id=source.work_item_id, checkpoint_id=checkpoint, holder_id="cap-holder"
            )
            assert lease.expires_at.tzinfo is not None
            again = await reader.acquire(
                work_item_id=source.work_item_id, checkpoint_id=checkpoint, holder_id="cap-holder"
            )
            assert (again.lease_id, again.expires_at) == (lease.lease_id, lease.expires_at)

            before = time.time()
            capability = reader.capability(lease, "patch")
            assert (
                1
                <= _expires(capability)
                <= min(60, math.floor(lease.expires_at.timestamp() - before))
            )
            granted = await anonymous.get(capability)
            assert granted.status_code == 200
            assert _sha(granted.content) == _sha(harness.patch)

            # The patch capability's signature replayed against the manifest key.
            parts = urlsplit(capability)
            forged = urlunsplit(parts._replace(path=f"/{BUCKET}/{_physical(keys['manifest'])}"))
            assert (await anonymous.get(forged)).status_code == 403
            with pytest.raises(RecoveryReadRefused) as unknown:
                reader.capability(lease, "credentials")
            assert unknown.value.code == "unknown_object"

            short = _reader(ctx, ttl=1)
            brief = short.capability(lease, "manifest")
            assert _expires(brief) == 1
            await asyncio.sleep(4.0)
            assert (await anonymous.get(brief)).status_code == 403

            # Injected time: the lease's remaining life shrinks to a few seconds.
            await _sql(
                ctx,
                "UPDATE curie.workspace_recovery_readers "
                "SET expires_at = clock_timestamp() + interval '6 seconds' WHERE id = :id",
                id=lease.lease_id,
            )
            bounded = await reader.acquire(
                work_item_id=source.work_item_id, checkpoint_id=checkpoint, holder_id="cap-holder"
            )
            assert bounded.lease_id == lease.lease_id
            assert bounded.expires_at < lease.expires_at
            before = time.time()
            remaining = math.floor(bounded.expires_at.timestamp() - before)
            capped = reader.capability(bounded, "base")
            assert 1 <= _expires(capped) <= remaining < 60
            assert (await anonymous.get(capped)).status_code == 200
            await asyncio.sleep(max(0.0, bounded.expires_at.timestamp() - time.time()) + 1.0)
            with pytest.raises(RecoveryReadRefused) as lapsed:
                reader.capability(bounded, "base")
            assert lapsed.value.code == "lease_expired"

            released = await reader.acquire(
                work_item_id=source.work_item_id, checkpoint_id=checkpoint, holder_id="releaser"
            )
            for path in (await _read_all(ctx, reader, released, harness.good_digests())).values():
                path.unlink()
            # Injected fault: the stored patch is corrupted at rest after completion.
            harness.client.put_object(
                Bucket=BUCKET, Key=_physical(keys["patch"]), Body=_binary(4096, 40)
            )
            destination = tmp_path / "corrupt-read"
            with pytest.raises(RecoveryReadRefused) as corrupt:
                await reader.read(released, "patch", destination)
            assert corrupt.value.code == "digest_mismatch"
            assert not destination.exists()
            await reader.release(released)
            assert released.released is True
            with pytest.raises(RecoveryReadRefused) as gone:
                reader.capability(released, "manifest")
            assert gone.value.code == "lease_released"
        _assert_sentinels(harness)

    worker_records = [record for record in caplog.records if record.name.startswith("curie_worker")]
    assert any(
        record.name == "curie_worker.workspace_recovery_storage" for record in worker_records
    )
    for record in worker_records:
        logged = _record_text(record)
        assert CANARY not in logged, record.name
        assert "X-Amz-Signature" not in logged, record.name
        assert "Signature=" not in logged, record.name


# --- AC4: owned services are removed ----------------------------------------------------------


class _InjectedFailure(RuntimeError):
    pass


def test_recovery_owned_services_removed() -> None:
    observed: list[_OwnedStore] = []
    with pytest.raises(_InjectedFailure):
        with _owned_rustfs() as store:
            observed.append(store)
            assert _container_present(store.container)
            assert _volume_present(store.volume)
            raise _InjectedFailure("a test body that fails inside the owned store")
    (store,) = observed
    assert not _container_present(store.container)
    assert not _volume_present(store.volume)
    assert store.container not in _owned_containers()
    assert store.volume not in _owned_volumes()

    names: list[str] = []
    with pytest.raises(_InjectedFailure):
        with _owned_database() as url:
            names.append(str(make_url(url).database))
            raise _InjectedFailure("a test body that fails inside the owned database")
    (name,) = names
    base = make_url(TEST_DATABASE_URL)
    assert _admin(base, "SELECT 1 AS present FROM pg_database WHERE datname = :n", n=name) == []
