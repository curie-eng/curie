"""A failed factory run's workspace is captured before its claim is released (#5143).

Boundary, and why it is not ``Kernel.process_event``. Opening a factory execute
turn through the kernel needs the factory binding resolver, the issue-read
capability mint, workspace credential redemption against a code host and the
approval and publication collaborators; none of those change here, and faking
them would put doubles on the path anyway. So these tests drive the nearest
boundary that passes through every changed component with nothing inside them
faked:

1. A real ``WorkItemRun`` over the real HTTP ``WorkItemDispatchClient``, against
   the real served API (``curie_api.main.create_app`` under uvicorn) on an owned,
   migrated Postgres database. The request is admitted, acquired and started
   through the real dispatch routes, and the run's own ``finish`` makes it
   ``failed`` through the real finish route.
2. The real kernel from the kernel harness (real Valkey ``ThreadLock``, real
   ``SandboxSubstrate`` whose only double is the Kubernetes ``SandboxClient``
   control plane, per apps/worker/CLAUDE.md) with the real
   ``WorkspaceClaimCoordinator`` over a local git origin and the owned RustFS
   bucket, and the real ``WorkspaceRecoveryCapture`` / ``RecoveryCheckpointWriter``
   / ``WorkspaceRecoveryClient`` / ``RecoveryObjectStore``.
3. The real runner: ``curie_runner.server.create_app`` serving a real
   ``SessionRunner`` on a loopback port over a git checkout extracted from the
   claim's real base archive, reached through the real ``RunnerClient``. Only the
   model behind it is scripted: a ``FakeModelSession`` turn that writes tracked,
   untracked, binary, executable and locally committed changes and then ends in
   an error result, so the runner answers a classified failure.
4. The ordinary failure is then routed exactly as ``kernel/completion.py``
   routes it, through ``Kernel._finish_or_settle(outcome="failed")``, and the
   claim is released exactly as the ``kernel/lifecycle.py`` delivery ``finally``
   releases it, ``run.close()`` then ``Kernel._release_work_item_sandbox``.

Injected faults are named where they are used and sit outside the changed code:
a pass-through observer around the substrate's ``release_if_claim``, a ``git``
wrapper first on PATH inside the runner checkpoint (the runner suite's own
seam), a TCP proxy or a dead endpoint in front of the store, an httpx transport
that loses one ledger route, SQL that moves a deadline instead of waiting it
out, and lease loss made deterministic by replacing this thread's Valkey lock
value with a successor token at a pass-through hook on the store upload, the
ledger call or the capture's return.

Every owned resource (database, RustFS container and volume, bucket, temp
directories, Valkey keys under the test's prefix) is removed and its absence
asserted. A locally unavailable service skips; under CI it fails.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import shlex
import shutil
import socket
import stat
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
from aci_protocol import Event, Final, SessionStatus
from aiohttp import web
from aiohttp.test_utils import TestServer
from botocore.exceptions import ClientError
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_api.config import get_settings
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.fake import FakeModelSession
from curie_runner.server import bind_status_attestation
from curie_runner.server import create_app as create_runner_app
from curie_runner.session import SessionRunner
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.binding import RUNNER_TOKEN_ENV, FactoryWake
from curie_worker.runner_client import RunnerClient
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemDispatchClient,
    WorkItemRun,
)
from curie_worker.workspace import (
    WorkspaceClaimCoordinator,
    WorkspaceObjectStore,
    WorkspacePreparer,
)
from curie_worker.workspace_recovery import WorkspaceRecoveryClient
from curie_worker.workspace_recovery_capture import WorkspaceRecoveryCapture
from curie_worker.workspace_recovery_storage import (
    CLEANUP_BUDGET_SECONDS,
    RecoveryAuthority,
    RecoveryBaseSource,
    RecoveryCaptureRequest,
    RecoveryCheckpointReader,
    RecoveryCheckpointWriter,
    RecoveryCollector,
    RecoveryObjectStore,
    RecoveryRepository,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool

from apps.worker.tests.test_workspace_recovery_storage import (
    ARCHIVE_LIMIT,
    BUCKET,
    DEPLOYMENT_ID,
    GIT_IDENTITY,
    INSTALLATION_CAP,
    REPOSITORY_PATH,
    TIMING_SLACK_SECONDS,
    _binary,
    _build_origin,
    _CredentialClient,
    _extract,
    _FaultProxy,
    _git,
    _limits,
    _LocalOriginCommands,
    _owned_database,
    _owned_rustfs,
    _OwnedStore,
    _require_docker,
    _require_postgres,
    _s3_client,
    _served_api,
    _settings_env,
)

RUNNER_TOKEN = "recovery-runner-token-claim"
SUCCESSOR_TOKEN = "successor-holds-this-thread"
CAPTURE_BUDGET_SECONDS = 60.0
REAL_GIT = shutil.which("git") or "git"
WORKER_HEADERS_KEY = "X-Curie-Worker-Token"
REPOSITORY = RecoveryRepository(
    provider="github", host="github.com", project_id="101", path=REPOSITORY_PATH
)


# --- module services: owned store, owned database, served API, local origin ----------


@dataclass(frozen=True)
class _Services:
    store: _OwnedStore
    s3: Any
    db_url: str
    api_url: str
    worker_token: str
    origin: Path
    base_commit: str


@pytest.fixture(scope="module")
def services(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_Services]:
    _require_docker()
    _require_postgres()
    root = tmp_path_factory.mktemp("work-item-recovery-services")
    with ExitStack() as stack:
        store = stack.enter_context(_owned_rustfs())
        db_url = stack.enter_context(_owned_database())
        api_env = {
            "DATABASE_URL": db_url,
            "CURIE_WORKSPACE_RECOVERY_ENABLED": "true",
            "CURIE_WORKSPACE_RECOVERY_MAX_BYTES": str(INSTALLATION_CAP),
            "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES": str(ARCHIVE_LIMIT),
            "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
            "RESUME_RECONCILER_ENABLED": "false",
            "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
            "APPROVAL_SWEEP_INTERVAL_S": "0",
            "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        }
        # The API's lifespan needs the same Valkey the kernel suite uses.
        for name, value in (
            ("VALKEY_HOST", VALKEY_HOST),
            ("VALKEY_PORT", str(VALKEY_PORT)),
            ("VALKEY_PASSWORD", VALKEY_PW),
        ):
            api_env.setdefault(name, os.environ.get(name, value))
        stack.enter_context(_settings_env(api_env))
        api_url = stack.enter_context(_served_api())
        origin, base_commit = _build_origin(root)
        yield _Services(
            store=store,
            s3=_s3_client(store.endpoint),
            db_url=db_url,
            api_url=api_url,
            worker_token=get_settings().internal_worker_token,
            origin=origin,
            base_commit=base_commit,
        )
    shutil.rmtree(root, ignore_errors=True)
    assert not root.exists(), f"owned service directory {root} survived"


def _assert_valkey_clean(sync_redis: redis.Redis, names: dict[str, str]) -> None:
    """Remove every key this test wrote under its unique prefixes, and prove it."""

    for pattern in (f"{names['prefix']}*", f"{names['sandbox_prefix']}*"):
        keys = list(sync_redis.scan_iter(match=pattern))
        if keys:
            sync_redis.delete(*keys)
        assert list(sync_redis.scan_iter(match=pattern)) == [], pattern


# --- small helpers -------------------------------------------------------------------------


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _tree(root: Path) -> dict[str, tuple[str, int, str]]:
    """sha256, size and git mode of every regular file outside ``.git``."""

    files: dict[str, tuple[str, int, str]] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        if Path(dirpath) == root:
            dirnames[:] = [name for name in dirnames if name != ".git"]
        for name in filenames:
            path = Path(dirpath) / name
            info = path.lstat()
            assert stat.S_ISREG(info.st_mode), path
            data = path.read_bytes()
            mode = "100755" if info.st_mode & stat.S_IXUSR else "100644"
            files[path.relative_to(root).as_posix()] = (_sha(data), len(data), mode)
    return files


def _closed_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _git_wrapper(bin_dir: Path, *, subcommand: str, action: str) -> str:
    """A ``git`` that runs ``action`` on ``subcommand`` and then execs real git.

    The same seam ``runner/tests/test_workspace_checkpoint.py`` uses: the runner
    capture runs git by argv name with the process PATH, so the wrapper sees the
    chosen call. Returns the PATH value that puts it first.
    """

    bin_dir.mkdir(parents=True)
    script = bin_dir / "git"
    script.write_text(
        "#!/bin/sh\n"
        'sub=""\n'
        "skip=0\n"
        'for arg in "$@"; do\n'
        '  if [ "$skip" = 1 ]; then skip=0; continue; fi\n'
        '  case "$arg" in\n'
        "    -c|-C|--git-dir|--work-tree|--namespace) skip=1 ;;\n"
        "    -*) ;;\n"
        '    *) sub="$arg"; break ;;\n'
        "  esac\n"
        "done\n"
        f'if [ "$sub" = {shlex.quote(subcommand)} ]; then\n'
        f"{action}\n"
        "fi\n"
        f'exec {shlex.quote(REAL_GIT)} "$@"\n'
    )
    script.chmod(0o755)
    return f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"


def _once(marker: Path, body: str) -> str:
    q = shlex.quote(str(marker))
    return f"  if [ ! -e {q} ]; then\n    : > {q}\n{body}  fi\n"


def _agent_edits(checkout: Path) -> None:
    """What the scripted failing task does to the checkout before it fails."""

    (checkout / "README.md").write_text("# acme bot\nlocally committed edit\n")
    _git(checkout, "add", "README.md")
    _git(checkout, *GIT_IDENTITY, "commit", "--quiet", "-m", "agent local commit")
    (checkout / "src/app.py").write_text("print('acme half-finished fix')\n")
    (checkout / "notes").mkdir(exist_ok=True)
    (checkout / "notes/untracked.txt").write_text("agent scratch notes\n")
    (checkout / "assets/added.bin").write_bytes(b"\x00\x01" + _binary(2048, 21))
    (checkout / "assets/blob.bin").write_bytes(_binary(4096, 22))
    os.chmod(checkout / "bin/run.sh", 0o755)
    (checkout / "docs/remove-me.txt").unlink()


def _oversized_edits(checkout: Path) -> None:
    _agent_edits(checkout)
    # Incompressible bytes: the binary patch exceeds the 900000-byte patch cap.
    (checkout / "assets/huge.bin").write_bytes(_binary(1_200_000, 23))


def _failing_turn(checkout: Path, edits: Callable[[Path], None]) -> Callable[[], list[Any]]:
    """The scripted model turn: it edits the workspace, then the turn fails."""

    def script() -> list[Any]:
        edits(checkout)
        return [
            AssistantMessage(
                content=[TextBlock(text="Started the fix, then the provider failed")],
                model="fake-model",
            ),
            ResultMessage(
                subtype="error_during_execution",
                duration_ms=1,
                duration_api_ms=1,
                is_error=True,
                num_turns=1,
                session_id="fake-session",
                result="provider turn ended",
                usage=None,
            ),
        ]

    return script


# --- pass-through observers and hooks ------------------------------------------------------


@dataclass
class _ReleaseCall:
    thread_key: str
    claim_name: str
    released: bool
    at_call: dict[str, Any]


class _ReleaseObserver:
    """Wraps the real ``release_if_claim``: snapshots recovery state, then calls through."""

    def __init__(self, stage: _Stage) -> None:
        self._stage = stage
        self._real = stage.h.substrate.release_if_claim
        self.calls: list[_ReleaseCall] = []
        self.after: Callable[[], None] | None = None
        stage.h.substrate.release_if_claim = self  # type: ignore[method-assign]

    def __call__(self, thread_key: str, claim_name: str) -> bool:
        at_call = self._stage.recovery_state_sync()
        released = self._real(thread_key, claim_name)
        self.calls.append(_ReleaseCall(thread_key, claim_name, released, at_call))
        hook, self.after = self.after, None
        if hook is not None:
            hook()
        return released


class _HookedStore(RecoveryObjectStore):
    """The real store; ``after_upload`` runs once after a named object kind lands."""

    def __init__(self, *, client: Any, bucket: str) -> None:
        super().__init__(client=client, bucket=bucket)
        self.after_upload: tuple[str, Callable[[str], None]] | None = None

    def upload_verified(self, key: str, *args: Any, **kwargs: Any) -> Any:
        result = super().upload_verified(key, *args, **kwargs)
        hook = self.after_upload
        if hook is not None and key.endswith(f"/{hook[0]}"):
            self.after_upload = None
            hook[1](key)
        return result


Hook = Callable[[dict[str, Any]], Awaitable[None]]


class _HookedLedger(WorkspaceRecoveryClient):
    """The real ledger client; records reserve payloads and runs one-shot hooks."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.reserves: list[dict[str, Any]] = []
        self.after_reserve: Hook | None = None
        self.after_complete: Hook | None = None

    async def reserve(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.reserves.append(dict(payload))
        result = await super().reserve(payload)
        hook, self.after_reserve = self.after_reserve, None
        if hook is not None:
            await hook(result)
        return result

    async def complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await super().complete(payload)
        hook, self.after_complete = self.after_complete, None
        if hook is not None:
            await hook(result)
        return result


class _LoseRoute(httpx.AsyncBaseTransport):
    """Real HTTP to the API, except every call to one ledger route is lost."""

    def __init__(self, suffix: str) -> None:
        self._inner = httpx.AsyncHTTPTransport()
        self._suffix = suffix
        self.lost = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(self._suffix):
            self.lost += 1
            raise httpx.ConnectError("injected ledger route loss", request=request)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


@web.middleware
async def _count_checkpoints(request: web.Request, handler: Any) -> web.StreamResponse:
    if request.path == "/v1/checkpoint":
        request.app["checkpoint_calls"].append(time.monotonic())
    return await handler(request)


# --- one stage: a WorkItem, its thread, its claim, its runner and its run ----------------


@dataclass(frozen=True)
class _Source:
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    runtime_epoch: int
    dispatch_generation: int

    def authority(self) -> RecoveryAuthority:
        return RecoveryAuthority(
            work_item_id=self.work_item_id,
            source_request_id=self.request_id,
            runtime_epoch=self.runtime_epoch,
            dispatch_generation=self.dispatch_generation,
        )


@dataclass
class _Prior:
    source: _Source
    checkpoint_id: uuid.UUID
    digests: dict[str, str]


@dataclass
class _Stage:
    services: _Services
    h: Any
    root: Path
    sync_redis: redis.Redis
    engine: AsyncEngine
    http: httpx.AsyncClient
    dispatch: WorkItemDispatchClient
    plain_ledger: WorkspaceRecoveryClient
    ledger: _HookedLedger
    real_store: RecoveryObjectStore
    store: _HookedStore
    coordinator: WorkspaceClaimCoordinator
    capture: WorkspaceRecoveryCapture | None
    runner: SessionRunner
    runner_app: web.Application
    runner_server: TestServer
    runner_client: RunnerClient
    checkout: Path
    thread_key: str
    address: str
    wire: str
    issue: str
    agent_id: uuid.UUID
    loop: asyncio.AbstractEventLoop
    handle: Any = None
    prepared: Any = None
    work_item_id: uuid.UUID | None = None
    run: WorkItemRun | None = None
    source: _Source | None = None
    prior: _Prior | None = None
    observer: _ReleaseObserver | None = None
    expected_tree: dict[str, tuple[str, int, str]] = field(default_factory=dict)
    local_head: str = ""
    request_after_finish: dict[str, Any] = field(default_factory=dict)
    release_seconds: float = 0.0
    runner_closed: bool = False
    # The scripted model's next turn, read by the runner's FakeModelSession.
    script: list[Callable[[], list[Any]]] = field(default_factory=lambda: [lambda: []])
    proxy: _FaultProxy | None = None

    # -- SQL ---------------------------------------------------------------------------

    async def sql(self, statement: str, **params: Any) -> list[dict[str, Any]]:
        async with self.engine.begin() as connection:
            result = await connection.execute(text(statement), params)
            return [dict(row._mapping) for row in result] if result.returns_rows else []

    async def recovery_state(self) -> dict[str, Any]:
        """Checkpoints, pointer, object presence and outcome of this stage's WorkItem."""

        engine = create_async_engine(self.services.db_url, poolclass=NullPool)
        try:
            async with engine.connect() as connection:
                checkpoints = {
                    row.id: (row.status, row.source_request_id)
                    for row in await connection.execute(
                        text(
                            "SELECT id, status, source_request_id "
                            "FROM curie.workspace_recovery_checkpoints WHERE work_item_id = :id"
                        ),
                        {"id": self.work_item_id},
                    )
                }
                pointer = (
                    await connection.execute(
                        text(
                            "SELECT last_good_checkpoint_id FROM curie.workspace_recovery_groups "
                            "WHERE work_item_id = :id"
                        ),
                        {"id": self.work_item_id},
                    )
                ).scalar_one_or_none()
                objects = {
                    row.key: (row.kind, row.status, row.checkpoint_id)
                    for row in await connection.execute(
                        text(
                            "SELECT key, kind, status, checkpoint_id "
                            "FROM curie.workspace_recovery_objects WHERE work_item_id = :id"
                        ),
                        {"id": self.work_item_id},
                    )
                }
                outcome = (
                    await connection.execute(
                        text(
                            "SELECT workspace_recovery_outcome FROM curie.work_items WHERE id = :id"
                        ),
                        {"id": self.work_item_id},
                    )
                ).scalar_one()
        finally:
            await engine.dispose()
        return {
            "checkpoints": checkpoints,
            "last_good": pointer,
            "objects": objects,
            "present": {key: self.exists(f"private/{key}") for key in objects},
            "outcome": outcome,
        }

    def recovery_state_sync(self) -> dict[str, Any]:
        # Called from the substrate's worker thread: a private loop and engine.
        return asyncio.run(self.recovery_state())

    async def request_row(self, request_id: uuid.UUID) -> dict[str, Any]:
        rows = await self.sql(
            "SELECT status, terminal_cause, terminal_at, runtime_epoch, dispatch_generation, "
            "runtime_heartbeat_expires_at FROM curie.execution_requests WHERE id = :id",
            id=request_id,
        )
        return rows[0]

    def exists(self, physical: str) -> bool:
        try:
            self.services.s3.head_object(Bucket=BUCKET, Key=physical)
        except ClientError as exc:
            if str(exc.response.get("Error", {}).get("Code")) in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise
        return True

    # -- admission and dispatch --------------------------------------------------------

    @property
    def wake(self) -> FactoryWake:
        return FactoryWake(
            kind="slack", address=self.address, adapter="default", conversation_id=self.wire
        )

    async def admit(self) -> uuid.UUID:
        """Admit one request for this stage's issue through the real admission route."""

        request_id = uuid.uuid4()
        response = await self.http.post(
            f"{self.services.api_url}/v1/internal/work-items/admissions",
            headers={WORKER_HEADERS_KEY: self.services.worker_token},
            json={
                "agent_id": str(self.agent_id),
                "kind": "slack",
                "address": self.address,
                "reply_conversation_id": self.wire,
                "tracker": {
                    "kind": "github",
                    "host": "github.com",
                    "scope_id": "101",
                    "issue_id": self.issue,
                },
                "repository": {
                    "code_host_kind": "github",
                    "host": "github.com",
                    "project_id": "101",
                    "path": REPOSITORY_PATH,
                },
                "code_host_installation_id": 202,
                "objective": "Fix the reported widget bug",
                "requester": "U0REQUEST1",
                "request_id": str(request_id),
            },
        )
        assert response.status_code == 200, response.text
        rows = await self.sql(
            "SELECT work_item_id FROM curie.execution_requests WHERE id = :id", id=request_id
        )
        if self.work_item_id is None:
            self.work_item_id = rows[0]["work_item_id"]
            # The trusted base the factory resolved at admission (ADR 0186): the
            # origin commit the claim checks out.
            await self.sql(
                "UPDATE curie.work_items SET base_commit = :base, base_branch = 'main', "
                "base_source = 'default' WHERE id = :id",
                base=self.services.base_commit,
                id=self.work_item_id,
            )
        assert rows[0]["work_item_id"] == self.work_item_id
        return request_id

    async def acquire(self, request_id: uuid.UUID, owner: str) -> WorkItemAcquireGrant:
        rows = await self.sql(
            "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
            id=request_id,
        )
        generation = int(rows[0]["dispatch_generation"])
        grant = await self.dispatch.acquire(
            request_id, owner=owner, generation=generation, wake=self.wake, context=None
        )
        # The recovery authority relies on the acquire generation being the
        # request's dispatch generation.
        assert grant.generation == generation
        return grant

    async def failed_request(self, owner: str, claim: str) -> _Source:
        """Admit, start and fail one request with no sandbox (a previous attempt)."""

        request_id = await self.admit()
        grant = await self.acquire(request_id, owner)
        started = await self.dispatch.start(
            request_id,
            owner=owner,
            generation=grant.generation,
            claim_name=claim,
            sandbox_name=f"sbx-{claim}",
            context=grant.context,
        )
        await self.dispatch.finish(
            request_id,
            runtime_epoch=started.runtime_epoch,
            outcome="failed",
            cause="runner_failed",
            detail=None,
            ci_fix_round=None,
        )
        assert self.work_item_id is not None
        return _Source(self.work_item_id, request_id, started.runtime_epoch, grant.generation)

    # -- the thread's claim and its runner ---------------------------------------------

    async def claim(self) -> Any:
        result = await asyncio.to_thread(
            self.coordinator.claim_or_resume_with_handle,
            thread_key=self.thread_key,
            deployment_id=DEPLOYMENT_ID,
            env={RUNNER_TOKEN_ENV: RUNNER_TOKEN},
        )
        assert result.handle.token == RUNNER_TOKEN
        assert result.prepared.base_sha == self.services.base_commit
        return result

    async def claim_thread(self) -> None:
        result = await self.claim()
        self.handle, self.prepared = result.handle, result.prepared
        archive = b"".join(self.coordinator.preparer.objects.get_stream(self.prepared.object_key))
        assert _sha(archive) == self.prepared.sha256
        # The runner's managed workspace is the real claim base, extracted.
        self.checkout.rmdir()
        _extract(archive, self.checkout)
        assert _git(self.checkout, "rev-parse", "HEAD").strip() == self.services.base_commit

    @property
    def checkpoint_calls(self) -> list[float]:
        return self.runner_app["checkpoint_calls"]

    async def stop_runner(self) -> None:
        if not self.runner_closed:
            self.runner_closed = True
            await self.runner_server.close()

    # -- the previous attempt's last-good checkpoint -----------------------------------

    async def seed_last_good(self) -> _Prior:
        """A previous request of the same WorkItem left a complete checkpoint."""

        source = await self.failed_request("previous-worker", "previous-claim")
        prior = self.checkout / "prior-attempt.txt"
        prior.write_text("what the previous attempt left behind\n")
        captured = await self.runner_client.checkpoint(
            self.handle.base_url,
            RUNNER_TOKEN,
            base_sha=self.services.base_commit,
            budget_seconds=30.0,
        )
        prior.unlink()
        scratch = self.root / "prior-scratch"
        writer = RecoveryCheckpointWriter(
            ledger=self.plain_ledger,
            store=self.real_store,
            scratch_root=scratch,
            base_archive_limit_bytes=ARCHIVE_LIMIT,
        )
        base = RecoveryBaseSource.from_claim(self.coordinator, self.thread_key)
        assert base is not None
        result = await writer.capture(
            RecoveryCaptureRequest(
                authority=source.authority(),
                idempotency_key=f"previous-{uuid.uuid4().hex}",
                repository=REPOSITORY,
                base_commit=self.services.base_commit,
                target_branch="main",
                patch=captured.patch,
                manifest=captured.manifest,
                base=base,
            )
        )
        assert result.status == "complete", result
        assert result.checkpoint_id is not None
        self.prior = _Prior(
            source=source,
            checkpoint_id=result.checkpoint_id,
            digests={
                "base": self.prepared.sha256,
                "patch": _sha(captured.patch),
                "manifest": _sha(captured.manifest),
            },
        )
        assert (await self.recovery_state())["last_good"] == result.checkpoint_id
        return self.prior

    async def assert_last_good_readable(self) -> None:
        """The previous complete checkpoint is still selected and reads byte for byte."""

        assert self.prior is not None
        state = await self.recovery_state()
        assert state["last_good"] == self.prior.checkpoint_id, state
        assert state["checkpoints"][self.prior.checkpoint_id][0] == "complete"
        await _read_checkpoint(
            self, self.prior.checkpoint_id, self.prior.digests, self.root / "verify-prior"
        )

    # -- the run under test ------------------------------------------------------------

    async def start_run(self) -> WorkItemRun:
        request_id = await self.admit()
        owner = self.h.config.consumer_name
        grant = await self.acquire(request_id, owner)
        run = WorkItemRun(
            client=self.dispatch,
            request_id=request_id,
            owner=owner,
            grant=grant,
            wake=self.wake,
            event_id=f"work-item-{request_id}-execute-{grant.generation}",
            thread_key=self.thread_key,
            on_stop=self.h.kernel._stop_owned_work_item,
            on_stale=self.h.kernel._abandon_stale_work_item,
        )
        await run.start(claim_name=self.handle.claim_name, sandbox_name=self.handle.sandbox_name)
        assert run.runtime_epoch is not None
        assert run.recovery_eligible is False
        self.run = run
        assert self.work_item_id is not None
        self.source = _Source(self.work_item_id, request_id, run.runtime_epoch, run.generation)
        return run

    async def fail_turn(self, edits: Callable[[Path], None] = _agent_edits) -> None:
        """One real runner turn whose scripted model edits the checkout and fails."""

        self.script[0] = _failing_turn(self.checkout, edits)
        stream = await self.runner_client.start_turn(
            self.handle.base_url,
            Event(type="message", text="Fix the reported widget bug", user="U0REQUEST1", ts="1"),
            RUNNER_TOKEN,
        )
        final: Final | None = None
        async with stream:
            async for frame in stream:
                if isinstance(frame, Final):
                    final = frame
        assert final is not None and final.status is SessionStatus.CLASSIFIED_FAILURE, final
        deadline = time.monotonic() + 10
        while self.runner.turn_active:
            assert time.monotonic() < deadline, "the runner never ended the failed turn"
            await asyncio.sleep(0.02)
        self.expected_tree = _tree(self.checkout)
        self.local_head = _git(self.checkout, "rev-parse", "HEAD").strip()

    async def finish_failed(self) -> None:
        """The ordinary failure, routed as kernel/completion.py routes it."""

        run = self.run
        assert run is not None
        await self.h.kernel._finish_or_settle(
            run, outcome="failed", cause="runner_failed", detail=None, ci_fix_round=None
        )
        assert run.finished
        self.request_after_finish = await self.request_row(run.request_id)
        assert self.request_after_finish["status"] == "failed"
        assert self.request_after_finish["terminal_cause"] == "runner_failed"

    async def release(self) -> None:
        """The delivery ``finally`` of kernel/lifecycle.py for a finished run."""

        run = self.run
        assert run is not None
        if self.observer is None:
            self.observer = _ReleaseObserver(self)
        started = time.monotonic()
        await run.close()
        await self.h.kernel._release_work_item_sandbox(run)
        self.release_seconds = time.monotonic() - started

    async def assert_request_unchanged(self) -> None:
        assert self.run is not None
        assert await self.request_row(self.run.request_id) == self.request_after_finish

    async def own_checkpoints(self) -> dict[uuid.UUID, str]:
        assert self.run is not None
        state = await self.recovery_state()
        return {
            cp: status
            for cp, (status, source) in state["checkpoints"].items()
            if source == self.run.request_id
        }

    # -- lease loss --------------------------------------------------------------------

    @property
    def lock_key(self) -> str:
        return self.h.config.lock_key(self.thread_key)

    def lose_lease(self, *, hold_ms: int = 60_000) -> None:
        """A successor takes this thread's lock: the predecessor's token is gone."""

        held = self.sync_redis.get(self.lock_key)
        assert held is not None and held != SUCCESSOR_TOKEN, (
            "the release seam was not holding the thread lock at this point"
        )
        self.sync_redis.set(self.lock_key, SUCCESSOR_TOKEN, px=hold_ms)

    def successor_unlocks(self) -> None:
        if self.sync_redis.get(self.lock_key) == SUCCESSOR_TOKEN:
            self.sync_redis.delete(self.lock_key)

    def on_loop(self, coroutine: Awaitable[Any]) -> Any:
        """Run a coroutine on the test's loop from a worker-thread hook."""

        return asyncio.run_coroutine_threadsafe(coroutine, self.loop).result(timeout=120)  # type: ignore[arg-type]

    async def admit_successor(
        self, *, claim: bool = False, fail_with_outcome: str | None = None
    ) -> tuple[uuid.UUID, Any]:
        """A successor request on the same WorkItem, optionally taking the thread."""

        handle = None
        if fail_with_outcome is not None:
            successor = await self.failed_request("successor-worker", "successor-claim")
            response = await self.plain_ledger.outcome(
                {
                    **_authority_payload(successor),
                    "reason": fail_with_outcome,
                }
            )
            assert isinstance(response, dict)
            request_id = successor.request_id
        else:
            request_id = await self.admit()
        if claim:
            # What ADR 0206 does next: the lost route goes, a successor claims fresh.
            assert self.h.substrate._affinity.delete_if_claim(
                self.thread_key, self.handle.claim_name
            )
            result = await self.claim()
            handle = result.handle
            assert handle.claim_name != self.handle.claim_name
        return request_id, handle

    async def assert_successor_intact(self, handle: Any) -> None:
        route = await asyncio.to_thread(self.h.substrate.lookup, self.thread_key)
        assert route is not None and route.claim_name == handle.claim_name
        assert handle.claim_name in self.h.fake_k8s.claims
        current = self.coordinator.current(self.thread_key)
        assert current is not None and current.object_key != self.prepared.object_key
        assert self.exists(f"private/workspaces/{current.object_key}")
        assert self.exists(f"private/workspaces/_ownership/{_ownership(self.thread_key)}.json")


def _ownership(thread_key: str) -> str:
    return hashlib.sha256(thread_key.encode()).hexdigest()


def _authority_payload(source: _Source) -> dict[str, Any]:
    return {
        "work_item_id": str(source.work_item_id),
        "source_request_id": str(source.request_id),
        "runtime_epoch": source.runtime_epoch,
        "dispatch_generation": source.dispatch_generation,
    }


async def _read_checkpoint(
    stage: _Stage, checkpoint_id: uuid.UUID, digests: dict[str, str], into: Path
) -> dict[str, Path]:
    """Lease and read every object of a checkpoint, verifying each digest."""

    into.mkdir(parents=True, exist_ok=True)
    transport = httpx.AsyncHTTPTransport()
    reader = RecoveryCheckpointReader(
        ledger=stage.plain_ledger,
        store=stage.real_store,
        transport=transport,
        reference_ttl_seconds=60,
    )
    paths: dict[str, Path] = {}
    try:
        assert stage.work_item_id is not None
        lease = await reader.acquire(
            work_item_id=stage.work_item_id,
            checkpoint_id=checkpoint_id,
            holder_id=f"verify-{uuid.uuid4().hex[:12]}",
        )
        try:
            assert set(lease.objects) == {"base", "patch", "manifest"}
            for kind in ("base", "patch", "manifest"):
                destination = into / f"{uuid.uuid4().hex[:8]}-{kind}"
                await reader.read(lease, kind, destination)
                assert _sha(destination.read_bytes()) == digests[kind], kind
                paths[kind] = destination
        finally:
            await reader.release(lease)
    finally:
        await transport.aclose()
    return paths


@contextlib.asynccontextmanager
async def _stage(
    services: _Services,
    make_harness: Any,
    sync_redis: redis.Redis,
    tmp_path: Path,
    *,
    recovery: bool = True,
    budget_seconds: float = CAPTURE_BUDGET_SECONDS,
    capture_store: Callable[[], Any] | None = None,
    ledger_transport: httpx.AsyncBaseTransport | None = None,
) -> AsyncIterator[_Stage]:
    root = tmp_path / f"stage-{uuid.uuid4().hex[:8]}"
    checkout = root / "workspace"
    checkout.mkdir(parents=True)
    engine = create_async_engine(services.db_url, poolclass=NullPool)
    script: list[Callable[[], list[Any]]] = [lambda: []]
    session = FakeModelSession(script_factory=lambda: script[0]())
    runner = bind_status_attestation(
        SessionRunner(
            max_usd_per_day=None,
            held_secrets=frozenset(),
            session_factory=lambda: session,
            ceiling=10_000,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="work-item-recovery",
        ),
        session_id="session-acme-recovery",
        sandbox_id="sandbox-acme-recovery",
        cwd=str(checkout),
    )
    runner_app = create_runner_app(runner, token=RUNNER_TOKEN)
    runner_app["checkpoint_calls"] = []
    runner_app.middlewares.append(_count_checkpoints)
    runner_server = TestServer(runner_app)
    await runner_server.start_server()
    assert runner_server.port is not None
    runner_client = RunnerClient(total_timeout_s=120.0)
    worker_token = services.worker_token
    exit_stack = contextlib.AsyncExitStack()
    try:
        http = await exit_stack.enter_async_context(httpx.AsyncClient(timeout=30.0))
        ledger_http = (
            await exit_stack.enter_async_context(
                httpx.AsyncClient(timeout=30.0, transport=ledger_transport)
            )
            if ledger_transport is not None
            else http
        )
        plain_ledger = WorkspaceRecoveryClient(
            api_base_url=services.api_url, worker_token=worker_token, client=http
        )
        ledger = _HookedLedger(
            api_base_url=services.api_url, worker_token=worker_token, client=ledger_http
        )
        real_store = RecoveryObjectStore(client=services.s3, bucket=BUCKET)
        store = _HookedStore(
            client=capture_store() if capture_store is not None else services.s3, bucket=BUCKET
        )
        built: dict[str, Any] = {}

        def workspace_factory(substrate: Any) -> WorkspaceClaimCoordinator:
            built["coordinator"] = WorkspaceClaimCoordinator(
                preparer=WorkspacePreparer(
                    credentials=_CredentialClient(),
                    commands=_LocalOriginCommands(services.origin),
                    objects=WorkspaceObjectStore(
                        client=services.s3, bucket=BUCKET, prefix="private/workspaces"
                    ),
                    scratch_root=root / "clone-scratch",
                    limits=_limits(),
                ),
                substrate=substrate,
            )
            return built["coordinator"]

        def recovery_factory(workspace: Any) -> WorkspaceRecoveryCapture:
            assert workspace is built["coordinator"]
            built["capture"] = WorkspaceRecoveryCapture(
                ledger=ledger,
                writer=RecoveryCheckpointWriter(
                    ledger=ledger,
                    store=store,
                    scratch_root=root / "recovery-scratch",
                    base_archive_limit_bytes=ARCHIVE_LIMIT,
                ),
                runner=runner_client,
                coordinator=workspace,
                budget_seconds=budget_seconds,
                cleanup_budget_seconds=CLEANUP_BUDGET_SECONDS,
            )
            return built["capture"]

        harness_kwargs: dict[str, Any] = {"workspace_factory": workspace_factory}
        if recovery:
            harness_kwargs["recovery_factory"] = recovery_factory
        async with make_harness(**harness_kwargs) as h:
            # Every sandbox this test claims dials the real runner above.
            h.fake_k8s.runner_ports.extend([runner_server.port] * 8)
            agent_id = uuid.uuid4()
            address = f"C0RECOV{uuid.uuid4().hex[:8].upper()}"
            stage = _Stage(
                services=services,
                h=h,
                root=root,
                sync_redis=sync_redis,
                engine=engine,
                http=http,
                dispatch=WorkItemDispatchClient(
                    api_base_url=services.api_url, worker_token=worker_token, client=http
                ),
                plain_ledger=plain_ledger,
                ledger=ledger,
                real_store=real_store,
                store=store,
                coordinator=built["coordinator"],
                capture=built.get("capture"),
                runner=runner,
                runner_app=runner_app,
                runner_server=runner_server,
                runner_client=runner_client,
                checkout=checkout,
                thread_key=f"recovery-{uuid.uuid4().hex}",
                address=address,
                wire=f"{uuid.uuid4().int % 10**10}.000100",
                issue=str(uuid.uuid4().int % 10**9),
                agent_id=agent_id,
                loop=asyncio.get_running_loop(),
                script=script,
            )
            await stage.sql(
                "INSERT INTO curie.agents (id, name, repo_full_name) VALUES (:id, :name, :repo)",
                id=agent_id,
                name=f"acme-recovery-{agent_id.hex[:10]}",
                repo=REPOSITORY_PATH,
            )
            await stage.sql(
                "INSERT INTO curie.agent_channels (id, agent_id, kind, address, adapter) "
                "VALUES (:id, :agent, 'slack', :address, 'default')",
                id=uuid.uuid4(),
                agent=agent_id,
                address=address,
            )
            await stage.claim_thread()
            try:
                yield stage
            finally:
                stage.successor_unlocks()
                if stage.run is not None:
                    await stage.run.close()
                # The writer's private scratch never outlives a capture.
                scratch = root / "recovery-scratch"
                assert not scratch.exists() or list(scratch.iterdir()) == []
    finally:
        await exit_stack.aclose()
        await runner_client.close()
        with contextlib.suppress(Exception):
            await runner_server.close()
        await engine.dispose()
        shutil.rmtree(root, ignore_errors=True)
        assert not root.exists(), f"owned stage directory {root} survived"


def _assert_release_bounded(stage: _Stage, budget: float = CAPTURE_BUDGET_SECONDS) -> None:
    ceiling = budget + CLEANUP_BUDGET_SECONDS + TIMING_SLACK_SECONDS
    assert stage.release_seconds <= ceiling, (stage.release_seconds, ceiling)


def _assert_released(stage: _Stage) -> None:
    """The run's own claim, route, workspace ownership and base object are gone."""

    assert stage.observer is not None
    assert [(call.claim_name, call.released) for call in stage.observer.calls] == [
        (stage.handle.claim_name, True)
    ]
    assert stage.handle.claim_name not in stage.h.fake_k8s.claims
    assert stage.coordinator.current(stage.thread_key) is None
    assert not stage.exists(f"private/workspaces/{stage.prepared.object_key}")
    assert not stage.exists(f"private/workspaces/_ownership/{_ownership(stage.thread_key)}.json")


async def _assert_no_recovery_state(stage: _Stage) -> None:
    state = await stage.recovery_state()
    assert state["checkpoints"] == {} and state["objects"] == {}, state
    assert state["last_good"] is None and state["outcome"] is None, state
    assert stage.checkpoint_calls == []


# --- AC1 -----------------------------------------------------------------------------------


def test_failed_run_capture_before_release(
    services: _Services,
    make_harness: Any,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, make_harness, sync_redis, tmp_path) as stage:
            run = await stage.start_run()
            await stage.fail_turn()
            assert stage.local_head != services.base_commit
            await stage.finish_failed()
            assert run.recovery_eligible is True
            await stage.release()
            _assert_release_bounded(stage)

            # At the moment of the actual claim release a complete checkpoint was
            # already the WorkItem's last good one, with every object in the store.
            assert stage.observer is not None
            assert len(stage.observer.calls) == 1
            call = stage.observer.calls[0]
            assert (call.thread_key, call.claim_name, call.released) == (
                stage.thread_key,
                stage.handle.claim_name,
                True,
            )
            at_release = call.at_call
            checkpoint_id = at_release["last_good"]
            assert checkpoint_id is not None
            assert at_release["checkpoints"][checkpoint_id] == ("complete", run.request_id)
            kinds = {kind for kind, _status, _cp in at_release["objects"].values()}
            assert kinds == {"base", "patch", "manifest"}
            assert all(at_release["present"].values()), at_release["present"]
            assert all(
                status == "complete" for _kind, status, _cp in at_release["objects"].values()
            )
            assert at_release["outcome"] == "complete"
            assert len(stage.checkpoint_calls) == 1

            # The checkpoint names this exact source, and the reserve carried the
            # aggregate budget still left, so the ledger deadline is inside 60 s.
            row = (
                await stage.sql(
                    "SELECT source_request_id, runtime_epoch, dispatch_generation, "
                    "capture_deadline, staging_expires_at, completed_at "
                    "FROM curie.workspace_recovery_checkpoints WHERE id = :id",
                    id=checkpoint_id,
                )
            )[0]
            request = await stage.request_row(run.request_id)
            assert row["source_request_id"] == run.request_id
            assert row["runtime_epoch"] == request["runtime_epoch"] == run.runtime_epoch
            assert row["dispatch_generation"] == request["dispatch_generation"]
            reserved_at = row["staging_expires_at"] - timedelta(seconds=600)
            budget = (row["capture_deadline"] - reserved_at).total_seconds()
            assert 0 < budget < CAPTURE_BUDGET_SECONDS, budget
            sent = [payload.get("capture_budget_seconds") for payload in stage.ledger.reserves]
            assert sent and all(
                isinstance(value, (int, float)) and 0 < value <= CAPTURE_BUDGET_SECONDS
                for value in sent
            ), sent
            assert row["completed_at"] < row["capture_deadline"]
            await stage.assert_request_unchanged()

            # Destroy the original runtime: the claim, its base archive and its
            # ownership ledger are released, the runner stops and the checkout goes.
            _assert_released(stage)
            assert await asyncio.to_thread(stage.h.substrate.lookup, stage.thread_key) is None
            await stage.stop_runner()
            shutil.rmtree(stage.checkout)
            assert not stage.checkout.exists()

            # Restore independently from the private checkpoint alone.
            digests = {
                kind: (
                    await stage.sql(
                        "SELECT sha256 FROM curie.workspace_recovery_objects WHERE key = :key",
                        key=key,
                    )
                )[0]["sha256"]
                for key, (kind, _status, _cp) in at_release["objects"].items()
            }
            assert digests["base"] == stage.prepared.sha256
            paths = await _read_checkpoint(stage, checkpoint_id, digests, stage.root / "restore")
            patch = paths["patch"].read_bytes()
            manifest = json.loads(paths["manifest"].read_bytes())
            # Relative to the trusted base, not HEAD: the local commit is in it.
            assert manifest["base_sha"] == services.base_commit != stage.local_head
            assert b"diff --git a/README.md b/README.md" in patch
            assert b"locally committed edit" in patch
            restored = _extract(paths["base"].read_bytes(), stage.root / "restored")
            assert _git(restored, "rev-parse", "HEAD").strip() == services.base_commit
            _git(restored, "apply", "--binary", str(paths["patch"]))
            assert _tree(restored) == stage.expected_tree
            assert not (restored / "docs/remove-me.txt").exists()
            assert stage.expected_tree["bin/run.sh"][2] == "100755"
            assert stage.expected_tree["bin/tool.sh"][2] == "100755"
            entries = {entry["path"]: entry for entry in manifest["entries"]}
            assert {
                "README.md",
                "src/app.py",
                "notes/untracked.txt",
                "assets/added.bin",
                "assets/blob.bin",
                "bin/run.sh",
                "docs/remove-me.txt",
            } == set(entries)
            for path, entry in entries.items():
                if entry.get("deleted"):
                    assert path not in stage.expected_tree
                    continue
                sha256, size, mode = stage.expected_tree[path]
                assert (entry["sha256"], entry["size"], entry["mode"]) == (sha256, size, mode)

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


# --- AC2: release races ---------------------------------------------------------------------


async def _race_late_predecessor(stage: _Stage) -> None:
    run = await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()
    # A successor is admitted and claims the thread before the late cleanup.
    _successor, handle = await stage.admit_successor(claim=True)
    await stage.release()

    assert run.recovery_eligible is True
    assert stage.checkpoint_calls == [], "a late predecessor captured the successor's runtime"
    assert stage.observer is not None
    assert [(c.claim_name, c.released) for c in stage.observer.calls] == [
        (stage.handle.claim_name, False)
    ]
    state = await stage.recovery_state()
    assert state["checkpoints"] == {} and state["objects"] == {}
    await stage.assert_successor_intact(handle)
    await stage.assert_request_unchanged()


async def _race_lease_lost_during_upload(stage: _Stage) -> None:
    await stage.seed_last_good()
    run = await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()
    successor: dict[str, Any] = {}

    def lose(_key: str) -> None:
        stage.lose_lease()
        successor["admitted"] = stage.on_loop(stage.admit_successor(claim=True))
        stage.successor_unlocks()

    stage.store.after_upload = ("patch", lose)
    await stage.release()
    _assert_release_bounded(stage)

    assert "admitted" in successor, "the capture never uploaded its patch"
    own = await stage.own_checkpoints()
    assert list(own.values()) == ["tombstoned"], own
    state = await stage.recovery_state()
    assert state["outcome"] == "lease_lost"
    for key, (_kind, _status, cp) in state["objects"].items():
        if cp in own:
            assert not state["present"][key], f"lost-lease object {key} was kept"
    await stage.assert_last_good_readable()
    # The predecessor reacquired and released only its own recorded claim.
    assert stage.observer is not None
    assert [(c.claim_name, c.released) for c in stage.observer.calls] == [
        (stage.handle.claim_name, False)
    ]
    await stage.assert_successor_intact(successor["admitted"][1])
    assert run.recovery_eligible is True
    await stage.assert_request_unchanged()


async def _race_successor_admitted_without_capture(stage: _Stage) -> None:
    await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()

    async def lose(_reservation: dict[str, Any]) -> None:
        stage.lose_lease()
        await stage.admit_successor()
        stage.successor_unlocks()

    stage.ledger.after_reserve = lose
    await stage.release()
    _assert_release_bounded(stage)

    own = await stage.own_checkpoints()
    assert list(own.values()) == ["tombstoned"], own
    state = await stage.recovery_state()
    # The successor never captured: the predecessor's own "reserved" is replaced.
    assert state["outcome"] == "lease_lost"
    assert state["last_good"] is None
    # Nobody took the thread's route, so the reacquired release frees the claim.
    _assert_released(stage)
    await stage.assert_request_unchanged()


async def _race_successor_newer_outcome(stage: _Stage) -> None:
    await stage.seed_last_good()
    await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()
    seen: dict[str, Any] = {}

    def lose(_key: str) -> None:
        stage.lose_lease()
        # The successor ran, failed and recorded its own, newer capture outcome.
        seen["successor"] = stage.on_loop(stage.admit_successor(fail_with_outcome="capture_failed"))
        seen["outcome"] = stage.on_loop(stage.recovery_state())["outcome"]
        stage.successor_unlocks()

    stage.store.after_upload = ("patch", lose)
    await stage.release()

    assert seen["outcome"] == "capture_failed"
    own = await stage.own_checkpoints()
    assert list(own.values()) == ["tombstoned"], own
    state = await stage.recovery_state()
    assert state["outcome"] == "capture_failed", "predecessor cleanup overwrote a newer outcome"
    await stage.assert_last_good_readable()
    _assert_released(stage)
    await stage.assert_request_unchanged()


async def _race_lease_lost_during_complete(stage: _Stage) -> None:
    await stage.seed_last_good()
    await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()

    async def lose(_completion: dict[str, Any]) -> None:
        stage.lose_lease()
        stage.successor_unlocks()

    stage.ledger.after_complete = lose
    await stage.release()
    _assert_release_bounded(stage)

    own = await stage.own_checkpoints()
    assert list(own.values()) == ["tombstoned"], own
    (retracted,) = own
    state = await stage.recovery_state()
    assert state["outcome"] == "lease_lost"
    await stage.assert_last_good_readable()
    # Retracted, not discarded: its objects stay for the collector's reader rule.
    assert stage.observer is not None and stage.observer.calls
    at_release = stage.observer.calls[0].at_call
    retracted_keys = [k for k, (_kind, _s, cp) in at_release["objects"].items() if cp == retracted]
    assert sorted(at_release["objects"][k][0] for k in retracted_keys) == ["manifest", "patch"]
    assert all(stage.exists(f"private/{key}") for key in retracted_keys)
    _assert_released(stage)
    await stage.assert_request_unchanged()


async def _race_lease_lost_after_fence(stage: _Stage) -> None:
    await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()
    assert stage.capture is not None
    real = stage.capture.capture
    seen: list[Any] = []

    async def capture_then_lose(*args: Any, **kwargs: Any) -> Any:
        result = await real(*args, **kwargs)
        seen.append(result)
        stage.lose_lease()
        stage.successor_unlocks()
        return result

    stage.capture.capture = capture_then_lose  # type: ignore[method-assign]
    await stage.release()
    _assert_release_bounded(stage)

    assert [result.status for result in seen] == ["complete"]
    own = await stage.own_checkpoints()
    assert list(own.values()) == ["complete"], own
    state = await stage.recovery_state()
    assert state["last_good"] in own and state["outcome"] == "complete"
    # Released once, through the reacquired plain path.
    _assert_released(stage)
    await stage.assert_request_unchanged()


async def _race_lease_lost_after_release(stage: _Stage) -> None:
    await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()
    stage.observer = _ReleaseObserver(stage)
    stage.observer.after = lambda: (stage.lose_lease(), stage.successor_unlocks())
    await stage.release()

    # hold() exit finds the lease lost after the release ran: it is not repeated.
    own = await stage.own_checkpoints()
    assert list(own.values()) == ["complete"], own
    _assert_released(stage)
    await stage.assert_request_unchanged()


async def _race_lock_unobtainable(stage: _Stage) -> None:
    await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()
    assert stage.capture is not None
    real = stage.capture.capture

    async def capture_then_lose(*args: Any, **kwargs: Any) -> Any:
        result = await real(*args, **kwargs)
        # The successor keeps the lock past every reacquire attempt.
        stage.lose_lease(hold_ms=120_000)
        return result

    stage.capture.capture = capture_then_lose  # type: ignore[method-assign]
    await stage.release()

    assert stage.sync_redis.get(stage.lock_key) == SUCCESSOR_TOKEN
    # Release deferred: no delete was attempted and nothing was removed.
    assert stage.observer is not None and stage.observer.calls == []
    assert stage.handle.claim_name in stage.h.fake_k8s.claims
    route = await asyncio.to_thread(stage.h.substrate.lookup, stage.thread_key)
    assert route is not None and route.claim_name == stage.handle.claim_name
    current = stage.coordinator.current(stage.thread_key)
    assert current is not None and current.object_key == stage.prepared.object_key
    assert stage.exists(f"private/workspaces/{stage.prepared.object_key}")
    assert stage.exists(f"private/workspaces/_ownership/{_ownership(stage.thread_key)}.json")
    own = await stage.own_checkpoints()
    assert list(own.values()) == ["complete"], own
    state = await stage.recovery_state()
    assert all(state["present"].values()), state["present"]
    await stage.assert_request_unchanged()
    # Owned-resource teardown: the successor lets go and the claim is released.
    stage.successor_unlocks()
    assert await asyncio.to_thread(stage.h.substrate.release, stage.thread_key)
    await asyncio.to_thread(stage.coordinator.release, stage.thread_key)


_RACES: dict[str, Callable[[_Stage], Awaitable[None]]] = {
    "late_predecessor_after_successor_claim": _race_late_predecessor,
    "lease_lost_during_upload_successor_takes_thread": _race_lease_lost_during_upload,
    "successor_admitted_without_capturing": _race_successor_admitted_without_capture,
    "successor_with_newer_outcome": _race_successor_newer_outcome,
    "lease_lost_during_complete_retracts": _race_lease_lost_during_complete,
    "lease_lost_after_fence_before_release": _race_lease_lost_after_fence,
    "lease_lost_after_release_not_repeated": _race_lease_lost_after_release,
    "lock_unobtainable_release_deferred": _race_lock_unobtainable,
}


@pytest.mark.parametrize("case", list(_RACES))
def test_recovery_release_races(
    services: _Services,
    make_harness: Any,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
    case: str,
) -> None:
    async def go() -> None:
        async with _stage(services, make_harness, sync_redis, tmp_path) as stage:
            await _RACES[case](stage)

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


# --- AC3: terminal faults and the paths that never capture ----------------------------------


async def _fault_disabled(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    run = await stage.start_run()
    await stage.fail_turn()
    await stage.finish_failed()
    await stage.release()
    assert run.recovery_eligible is True
    await _assert_no_recovery_state(stage)
    _assert_released(stage)
    await stage.assert_request_unchanged()


async def _cancel_work_item(stage: _Stage) -> None:
    version = (
        await stage.sql(
            "SELECT version FROM curie.work_items WHERE id = :id", id=stage.work_item_id
        )
    )[0]["version"]
    response = await stage.http.post(
        f"{stage.services.api_url}/v1/internal/work-items/{stage.work_item_id}/cancel",
        headers={WORKER_HEADERS_KEY: stage.services.worker_token},
        json={"expected_version": version},
    )
    assert response.status_code == 200, response.text


async def _stop_and_release(stage: _Stage) -> None:
    """What the heartbeat's ``cancellation_requested`` callback and the finally do."""

    run = stage.run
    assert run is not None
    stage.observer = _ReleaseObserver(stage)
    await stage.h.kernel._stop_owned_work_item(stage.thread_key, run)
    assert run.finished
    await stage.release()
    assert run.recovery_eligible is False


async def _fault_cancellation(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    run = await stage.start_run()
    await _cancel_work_item(stage)
    beat = await stage.dispatch.heartbeat(run.request_id, runtime_epoch=run.runtime_epoch or 0)
    assert beat.status == "cancellation_requested"
    await _stop_and_release(stage)
    await _assert_no_recovery_state(stage)
    request = await stage.request_row(run.request_id)
    assert request["status"] == "cancelled"
    assert stage.handle.claim_name not in stage.h.fake_k8s.claims


async def _fault_execution_deadline(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    from curie_api.workitems import lifecycle

    run = await stage.start_run()
    # Injected time: the execution deadline passes now instead of in hours.
    await stage.sql(
        "UPDATE curie.execution_requests SET execution_deadline = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=run.request_id,
    )
    versions = (
        await stage.sql(
            "SELECT w.version AS item, r.version AS request FROM curie.execution_requests r "
            "JOIN curie.work_items w ON w.id = r.work_item_id WHERE r.id = :id",
            id=run.request_id,
        )
    )[0]
    async with AsyncSession(stage.engine) as session:
        await lifecycle.request_execution_deadline_cancellation(
            session,
            work_item_id=stage.work_item_id,
            request_id=run.request_id,
            expected_work_item_version=versions["item"],
            expected_request_version=versions["request"],
        )
        await session.commit()
    requested = await stage.request_row(run.request_id)
    assert (requested["status"], requested["terminal_cause"]) == (
        "cancellation_requested",
        "execution_deadline",
    )
    await _stop_and_release(stage)
    await _assert_no_recovery_state(stage)
    request = await stage.request_row(run.request_id)
    assert request["terminal_cause"] == "execution_deadline"
    assert request["status"] != "failed"


async def _fault_owner_loss(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    run = await stage.start_run()
    await stage.fail_turn()
    # A peer proved this owner dead; its next heartbeat is refused as stale.
    await stage.dispatch.declare_owner_lost(
        run.request_id, owner=run.owner, runtime_epoch=run.runtime_epoch or 0
    )
    with pytest.raises(WorkItemConflict) as refused:
        await stage.dispatch.heartbeat(run.request_id, runtime_epoch=run.runtime_epoch or 0)
    assert refused.value.code == "stale_owner"
    stage.observer = _ReleaseObserver(stage)
    await stage.h.kernel._abandon_stale_work_item(stage.thread_key, run)
    assert run.abandoned and run.finished
    await stage.release()
    assert run.recovery_eligible is False
    await _assert_no_recovery_state(stage)
    # The API termination chain owns the lost request's teardown: nothing released.
    assert stage.observer.calls == []
    assert stage.handle.claim_name in stage.h.fake_k8s.claims
    await asyncio.to_thread(stage.h.substrate.release, stage.thread_key)
    await asyncio.to_thread(stage.coordinator.release, stage.thread_key)


async def _fault_cancelled_finish(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    run = await stage.start_run()
    await stage.fail_turn()
    await _cancel_work_item(stage)
    with pytest.raises(WorkItemConflict) as refused:
        await stage.h.kernel._finish_or_settle(
            run, outcome="failed", cause="runner_failed", detail=None, ci_fix_round=None
        )
    assert refused.value.code == "work_item_cancelled"
    # kernel/completion.py's handling of this refusal (#3208).
    run.finished = True
    assert run.recovery_eligible is False
    await stage.release()
    await _assert_no_recovery_state(stage)
    _assert_released(stage)


async def _failed_with_last_good(
    stage: _Stage, edits: Callable[[Path], None] = _agent_edits
) -> None:
    await stage.seed_last_good()
    await stage.start_run()
    await stage.fail_turn(edits)
    await stage.finish_failed()


async def _assert_fault_kept_the_failure(stage: _Stage, outcome: str | None) -> None:
    """The original failure and the last good checkpoint survive the fault."""

    await stage.assert_request_unchanged()
    await stage.assert_last_good_readable()
    if outcome is not None:
        assert (await stage.recovery_state())["outcome"] == outcome
    own = await stage.own_checkpoints()
    assert "complete" not in own.values(), own


async def _fault_torn_capture(stage: _Stage, monkeypatch: pytest.MonkeyPatch) -> None:
    await _failed_with_last_good(stage)
    marker = stage.root / "raced"
    tracked = stage.checkout / "src/app.py"
    writer = f"    /bin/sh -c 'printf raced >> \"$1\"' writer {shlex.quote(str(tracked))}\n"
    monkeypatch.setenv(
        "PATH",
        _git_wrapper(
            stage.root / "torn-bin", subcommand="write-tree", action=_once(marker, writer)
        ),
    )
    try:
        await stage.release()
    finally:
        monkeypatch.undo()
    assert marker.exists(), "the concurrent writer never ran during the checkpoint"
    assert len(stage.checkpoint_calls) == 1
    await _assert_fault_kept_the_failure(stage, "torn_capture")
    assert await stage.own_checkpoints() == {}
    _assert_released(stage)


async def _fault_patch_too_large(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    await _failed_with_last_good(stage, _oversized_edits)
    await stage.release()
    await _assert_fault_kept_the_failure(stage, "patch_too_large")
    assert await stage.own_checkpoints() == {}
    _assert_released(stage)


async def _fault_runner_outage(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    await _failed_with_last_good(stage)
    await stage.stop_runner()
    await stage.release()
    _assert_release_bounded(stage)
    await _assert_fault_kept_the_failure(stage, "runner_unavailable")
    assert await stage.own_checkpoints() == {}
    _assert_released(stage)


async def _fault_store_outage(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    await _failed_with_last_good(stage)
    await stage.release()
    _assert_release_bounded(stage)
    await _assert_fault_kept_the_failure(stage, "upload_failed")
    assert list((await stage.own_checkpoints()).values()) == ["tombstoned"]
    _assert_released(stage)


RUNNER_PHASE_BUDGET = 4.0


async def _fault_runner_timeout(stage: _Stage, monkeypatch: pytest.MonkeyPatch) -> None:
    await _failed_with_last_good(stage)
    marker = stage.root / "hung"
    monkeypatch.setenv(
        "PATH",
        _git_wrapper(
            stage.root / "hang-bin", subcommand="diff", action=_once(marker, "    sleep 30\n")
        ),
    )

    async def expire_heartbeat_while_hung() -> None:
        while not marker.exists():
            await asyncio.sleep(0.02)
        # Injected time: the run's heartbeat lapses while the runner phase hangs.
        assert stage.run is not None
        await stage.sql(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :id",
            id=stage.run.request_id,
        )

    expiring = asyncio.create_task(expire_heartbeat_while_hung())
    try:
        await stage.release()
    finally:
        monkeypatch.undo()
        expiring.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await expiring
    assert marker.exists()
    _assert_release_bounded(stage, RUNNER_PHASE_BUDGET)
    assert stage.run is not None
    recorded = (
        await stage.sql(
            "SELECT w.workspace_recovery_outcome AS outcome, "
            "w.workspace_recovery_outcome_at AS at, "
            "r.runtime_heartbeat_expires_at AS expired FROM curie.work_items w "
            "JOIN curie.execution_requests r ON r.work_item_id = w.id WHERE r.id = :id",
            id=stage.run.request_id,
        )
    )[0]
    # Recorded durably inside the cleanup allowance, after the heartbeat lapsed.
    assert recorded["outcome"] == "capture_timeout"
    assert recorded["at"] > recorded["expired"]
    assert await stage.own_checkpoints() == {}
    stage.request_after_finish["runtime_heartbeat_expires_at"] = recorded["expired"]
    await _assert_fault_kept_the_failure(stage, "capture_timeout")
    _assert_released(stage)


AGGREGATE_BUDGET = 6.0


async def _fault_missed_deadline_slow_store(
    stage: _Stage, _monkeypatch: pytest.MonkeyPatch
) -> None:
    await _failed_with_last_good(stage)
    assert stage.proxy is not None
    stage.proxy.stall()
    await stage.release()
    _assert_release_bounded(stage, AGGREGATE_BUDGET)
    await _assert_fault_kept_the_failure(stage, "capture_timeout")
    _assert_released(stage)


async def _fault_completion_past_deadline(stage: _Stage, monkeypatch: pytest.MonkeyPatch) -> None:
    await _failed_with_last_good(stage)
    slow = 3.0
    marker = stage.root / "slow"
    monkeypatch.setenv(
        "PATH",
        _git_wrapper(
            stage.root / "slow-bin", subcommand="diff", action=_once(marker, f"    sleep {slow}\n")
        ),
    )
    reserved: list[dict[str, Any]] = []

    async def deadline_passes(reservation: dict[str, Any]) -> None:
        reserved.append(reservation)
        # Injected time: the ledger's capture deadline passes before completion.
        await stage.sql(
            "UPDATE curie.workspace_recovery_checkpoints SET capture_deadline = "
            "clock_timestamp() - interval '1 second' WHERE id = :id",
            id=uuid.UUID(str(reservation["checkpoint_id"])),
        )

    stage.ledger.after_reserve = deadline_passes
    try:
        await stage.release()
    finally:
        monkeypatch.undo()
    assert marker.exists() and reserved
    # After a long runner phase the reserve carried only the budget left.
    sent = stage.ledger.reserves[-1]["capture_budget_seconds"]
    assert 0 < sent <= CAPTURE_BUDGET_SECONDS - slow + 1.0, sent
    await _assert_fault_kept_the_failure(stage, None)
    own = await stage.own_checkpoints()
    assert list(own.values()) == ["tombstoned"], own
    assert (await stage.recovery_state())["outcome"] not in {"complete", "reserved"}
    _assert_released(stage)


async def _fault_staging_collected(stage: _Stage, _monkeypatch: pytest.MonkeyPatch) -> None:
    await _failed_with_last_good(stage)
    await stage.release()
    # The upload failed and its tombstone was lost: the staging row remains.
    own = await stage.own_checkpoints()
    assert list(own.values()) == ["staging"], own
    await stage.assert_request_unchanged()
    await stage.assert_last_good_readable()
    _assert_released(stage)

    # Injected time: the 600 s staging window ends now instead of in ten minutes.
    await stage.sql(
        "UPDATE curie.workspace_recovery_checkpoints SET staging_expires_at = "
        "clock_timestamp() - interval '1 second' WHERE id = :id",
        id=next(iter(own)),
    )
    staged = [
        key
        for key, (_kind, _status, cp) in (await stage.recovery_state())["objects"].items()
        if cp in own
    ]
    collector = RecoveryCollector(
        ledger=stage.plain_ledger,
        store=stage.real_store,
        collector_id=f"recovery-test-{uuid.uuid4().hex[:12]}",
    )
    for _ in range(8):
        collected = await collector.collect_once()
        assert collected.failed == 0 and collected.conflicts == 0, collected
        if collected.claimed == 0:
            break
    else:
        raise AssertionError("collection never settled")
    state = await stage.recovery_state()
    assert not any(cp in own for cp in state["checkpoints"]), state
    assert all(key not in state["objects"] for key in staged)
    assert all(not stage.exists(f"private/{key}") for key in staged)
    await stage.assert_last_good_readable()
    await stage.assert_request_unchanged()


@dataclass(frozen=True)
class _Fault:
    run: Callable[[_Stage, pytest.MonkeyPatch], Awaitable[None]]
    recovery: bool = True
    budget: float = CAPTURE_BUDGET_SECONDS
    store: str = "real"
    lose_fail: bool = False


_FAULTS: dict[str, _Fault] = {
    "disabled_recovery": _Fault(_fault_disabled, recovery=False),
    "cancellation": _Fault(_fault_cancellation),
    "execution_deadline": _Fault(_fault_execution_deadline),
    "owner_loss": _Fault(_fault_owner_loss),
    "work_item_cancelled_finish": _Fault(_fault_cancelled_finish),
    "torn_capture": _Fault(_fault_torn_capture),
    "patch_too_large": _Fault(_fault_patch_too_large),
    "runner_unavailable": _Fault(_fault_runner_outage),
    "store_outage": _Fault(_fault_store_outage, store="dead"),
    "runner_phase_timeout": _Fault(_fault_runner_timeout, budget=RUNNER_PHASE_BUDGET),
    "missed_aggregate_deadline": _Fault(
        _fault_missed_deadline_slow_store, budget=AGGREGATE_BUDGET, store="proxy"
    ),
    "completion_past_deadline": _Fault(_fault_completion_past_deadline),
    "staging_collected_after_expiry": _Fault(
        _fault_staging_collected, store="dead", lose_fail=True
    ),
}


@pytest.mark.parametrize("case", list(_FAULTS))
def test_recovery_terminal_faults(
    services: _Services,
    make_harness: Any,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    fault = _FAULTS[case]
    proxy: _FaultProxy | None = None

    def capture_store() -> Any:
        nonlocal proxy
        if fault.store == "dead":
            return _s3_client(f"http://127.0.0.1:{_closed_port()}", timeout_seconds=2.0)
        if fault.store == "proxy":
            proxy = _FaultProxy(services.store.port)
            return _s3_client(proxy.endpoint, timeout_seconds=30.0)
        return services.s3

    async def go() -> None:
        lost = _LoseRoute("/workspace-recovery/fail") if fault.lose_fail else None
        try:
            async with _stage(
                services,
                make_harness,
                sync_redis,
                tmp_path,
                recovery=fault.recovery,
                budget_seconds=fault.budget,
                capture_store=capture_store,
                ledger_transport=lost,
            ) as stage:
                stage.proxy = proxy
                await fault.run(stage, monkeypatch)
                if lost is not None:
                    assert lost.lost >= 1
        finally:
            if proxy is not None:
                proxy.close()

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)
