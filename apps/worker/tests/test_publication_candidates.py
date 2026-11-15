"""Publication candidates are retained before validation and read back exactly (#5428).

Boundary. These tests drive the nearest boundary that passes through every
changed component with nothing inside it faked:

1. A real ``WorkItemRun`` over the real HTTP ``WorkItemDispatchClient``, against
   the real served API (``curie_api.main.create_app`` under uvicorn) on an owned,
   migrated Postgres database with publication candidates enabled. The request is
   admitted, acquired and started through the real dispatch routes.
2. The real kernel from the kernel harness (real Valkey ``ThreadLock``, real
   ``SandboxSubstrate`` whose only double is the Kubernetes ``SandboxClient``
   control plane, per apps/worker/CLAUDE.md) with the real
   ``WorkspaceClaimCoordinator`` over a local git origin and an owned bucket, and
   the real ``PublicationCandidateCapture`` / ``CandidateWriter`` /
   ``PublicationCandidateClient`` / ``CandidateObjectStore``. The kernel method
   under test is ``Kernel._retain_and_validate_publication_snapshot``, the helper
   the publication snapshot branch of ``kernel/attempt.py`` calls.
3. The real runner: ``curie_runner.server.create_app`` serving a real
   ``SessionRunner`` and the production ``workspace_snapshotter`` over a git
   checkout extracted from the claim's real base archive, reached through the real
   ``RunnerClient``. Only the model behind it is scripted (``FakeModelSession``).

Real services: Postgres from ``TEST_DATABASE_URL`` (an owned database is
created and migrated), Valkey from ``TEST_VALKEY_HOST``/``TEST_VALKEY_PORT``, and
an S3 endpoint from ``TEST_S3_ENDPOINT_URL`` (the dev stack's RustFS by default,
as the other S3 backed worker suites use) with ``S3_ACCESS_KEY``/``S3_SECRET_KEY``,
in which this module creates its own uniquely named bucket. A missing prerequisite skips
locally and fails under CI. Every owned resource (database, bucket, temp
directories, Valkey keys under the test prefix) is removed and its absence
asserted.

Injected faults are named where they are used and sit outside the changed code:
an httpx transport that loses one ``/complete`` response after the API committed
it, SQL that moves a terminal timestamp or a claim deadline instead of waiting it
out, a stored object overwritten in the bucket, a collector subprocess killed
between deletion and finalize, a store that cuts one base upload before any byte
lands together with a transport that loses the writer's ``/fail`` call, and a run
whose ``execution_deadline`` is moved close instead of waiting it out.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import dataclasses
import hashlib
import os
import secrets
import shutil
import subprocess
import sys
import uuid
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import ExitStack
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import redis
from aci_protocol import Event, Final, QueuedTurn, ReplyHandle
from aiohttp import web
from aiohttp.test_utils import TestServer
from botocore.exceptions import BotoCoreError, ClientError
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_api.config import get_settings
from curie_runner import RunTracer, SideEffectClassifier, workspace_snapshot
from curie_runner.fake import FakeModelSession
from curie_runner.server import bind_status_attestation
from curie_runner.server import create_app as create_runner_app
from curie_runner.session import SessionRunner
from curie_test_support.static_shell import wait_until
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker import run as worker_run
from curie_worker.binding import RUNNER_TOKEN_ENV, FactoryWake
from curie_worker.config import WorkerConfig
from curie_worker.publication_candidate_capture import (
    CandidateCaptureResult,
    PublicationCandidateCapture,
)
from curie_worker.publication_candidate_storage import (
    CandidateObjectStore,
    CandidateReader,
    CandidateReadRefused,
    CandidateWriter,
    build_candidate_collector,
)
from curie_worker.publication_candidates import PublicationCandidateClient
from curie_worker.publication_validation import validate_snapshot_against_base
from curie_worker.runner_client import RunnerClient, RunnerWorkspaceSnapshot
from curie_worker.sandbox import DockerSandboxClient
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemDispatchClient,
    WorkItemRun,
)
from curie_worker.workspace import (
    WorkspaceClaimCoordinator,
    WorkspaceObjectStore,
    WorkspacePreparationError,
    WorkspacePreparer,
)
from curie_worker.workspace_recovery_storage import (
    RecoveryCollectionPass,
    RecoveryCollector,
    build_recovery_s3_client,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from apps.worker.tests.kernel.conftest import kernel_harness
from apps.worker.tests.kernel.test_work_item_recovery import (
    _agent_edits,
    _assert_valkey_clean,
    _ownership,
    _tree,
)
from apps.worker.tests.test_workspace_recovery_storage import (
    ARCHIVE_LIMIT,
    CODE_HOST_ORIGIN,
    DEPLOYMENT_ID,
    REGION,
    REPOSITORY_PATH,
    _binary,
    _build_origin,
    _CredentialClient,
    _extract,
    _git,
    _limits,
    _LocalOriginCommands,
    _owned_database,
    _require_postgres,
    _served_api,
    _settings_env,
    _unavailable,
)

RUNNER_TOKEN = "candidate-runner-token-claim"
WORKER_HEADERS_KEY = "X-Curie-Worker-Token"
TITLE = "Fix the reported widget bug"
BODY = "Carries the agent's local commit and working tree changes."
INSTALLATION_CAP = 1 << 30
INTERRUPT_SCRIPT = Path(__file__).resolve().parent / "fixtures/candidate_collector_interrupt.py"
EXIT_BEFORE_FINALIZE = 17
# A path the agent changes; listing it as protected makes validation refuse.
PROTECTED_PATH = "src/app.py"


# --- availability -------------------------------------------------------------------------


def _require_valkey() -> None:
    client = redis.Redis(
        host=VALKEY_HOST,
        port=VALKEY_PORT,
        password=VALKEY_PW or None,
        socket_connect_timeout=5,
    )
    try:
        client.ping()
    except redis.RedisError as exc:
        _unavailable(f"Valkey is unreachable at {VALKEY_HOST}:{VALKEY_PORT}: {type(exc).__name__}")
    finally:
        client.close()


def _s3_client(endpoint: str) -> Any:
    return build_recovery_s3_client(
        endpoint_url=endpoint,
        access_key=os.environ.get("S3_ACCESS_KEY", ""),
        secret_key=os.environ.get("S3_SECRET_KEY", ""),
        region=REGION,
    )


def _missing(exc: ClientError) -> bool:
    code = str(exc.response.get("Error", {}).get("Code"))
    return code in {"404", "NoSuchKey", "NotFound", "NoSuchBucket"}


@contextlib.contextmanager
def _owned_bucket(client: Any) -> Iterator[str]:
    """Create a uniquely named bucket, then empty and delete it and prove it is gone."""

    bucket = f"curie-candidates-{secrets.token_hex(6)}"
    created: list[bool] = []

    def remove() -> None:
        if not created:
            # The create was refused: there is nothing owned to remove.
            return
        with contextlib.suppress(ClientError):
            uploads = client.list_multipart_uploads(Bucket=bucket).get("Uploads") or ()
            for upload in uploads:
                client.abort_multipart_upload(
                    Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"]
                )
        with contextlib.suppress(ClientError):
            paginator = client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket):
                for item in page.get("Contents") or ():
                    client.delete_object(Bucket=bucket, Key=item["Key"])
        with contextlib.suppress(ClientError):
            client.delete_bucket(Bucket=bucket)
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError as exc:
            assert _missing(exc), exc
        else:
            raise AssertionError(f"owned bucket {bucket} survived")

    # Cleanup is registered before the bucket exists, so a failed create still
    # proves nothing was left behind.
    with ExitStack() as stack:
        stack.callback(remove)
        try:
            client.create_bucket(Bucket=bucket)
        except (BotoCoreError, ClientError) as exc:
            _unavailable(f"the S3 endpoint refused an owned bucket: {type(exc).__name__}")
        created.append(True)
        yield bucket


# --- module services ------------------------------------------------------------------------


@dataclass(frozen=True)
class _Services:
    s3: Any
    endpoint: str
    bucket: str
    db_url: str
    api_url: str
    worker_token: str
    origin: Path
    base_commit: str
    api_env: dict[str, str]


@pytest.fixture(scope="module")
def services(tmp_path_factory: pytest.TempPathFactory) -> Iterator[_Services]:
    _require_postgres()
    _require_valkey()
    endpoint = os.environ.get("TEST_S3_ENDPOINT_URL", "http://127.0.0.1:29000")
    root = tmp_path_factory.mktemp("publication-candidate-services")
    with ExitStack() as stack:

        def remove_root() -> None:
            shutil.rmtree(root, ignore_errors=True)
            assert not root.exists(), f"owned service directory {root} survived"

        stack.callback(remove_root)
        s3 = _s3_client(endpoint)
        stack.callback(s3.close)
        bucket = stack.enter_context(_owned_bucket(s3))
        db_url = stack.enter_context(_owned_database())
        api_env = {
            "DATABASE_URL": db_url,
            "CURIE_PUBLICATION_CANDIDATES_ENABLED": "true",
            "CURIE_PUBLICATION_CANDIDATE_MAX_BYTES": str(INSTALLATION_CAP),
            "CURIE_WORKSPACE_MAX_ARCHIVE_BYTES": str(ARCHIVE_LIMIT),
            "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
            "RESUME_RECONCILER_ENABLED": "false",
            "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
            "APPROVAL_SWEEP_INTERVAL_S": "0",
            "DEAD_LETTER_WATCH_INTERVAL_S": "0",
            "S3_ENDPOINT_URL": endpoint,
        }
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
            s3=s3,
            endpoint=endpoint,
            bucket=bucket,
            db_url=db_url,
            api_url=api_url,
            worker_token=get_settings().internal_worker_token,
            origin=origin,
            base_commit=base_commit,
            api_env=api_env,
        )


@contextlib.contextmanager
def _variant_api(**overrides: str) -> Iterator[str]:
    """A second served API on the same database with ``overrides`` in its settings."""

    with _settings_env(overrides):
        with _served_api() as url:
            yield url


# --- helpers -------------------------------------------------------------------------------


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _qevent(event_id: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id="1700000000.000100",
        author="U0REQUEST1",
        text="Fix the reported widget bug",
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="p-1", endpoint=None),
        received_at="2026-11-15T00:00:00+00:00",
    )


def _successful_turn(checkout: Path) -> Callable[[], list[Any]]:
    """The scripted model turn: it edits the workspace every way, then finishes."""

    def script() -> list[Any]:
        _agent_edits(checkout)
        return [
            AssistantMessage(
                content=[TextBlock(text="Fixed the widget bug; ready to publish")],
                model="fake-model",
            ),
            ResultMessage(
                subtype="success",
                duration_ms=1,
                duration_api_ms=1,
                is_error=False,
                num_turns=1,
                session_id="fake-session",
                result="done",
                usage=None,
            ),
        ]

    return script


class _RecordingLedger(PublicationCandidateClient):
    """The real ledger client; records every reserve answer and complete call."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.reservations: list[dict[str, Any]] = []
        self.completes = 0

    async def reserve(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await super().reserve(payload)
        self.reservations.append(result)
        return result

    async def complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.completes += 1
        return await super().complete(payload)


class _DropCompleteResponse(httpx.AsyncBaseTransport):
    """Real HTTP; the first ``/complete`` reaches the API, then its response is lost."""

    def __init__(self) -> None:
        self._inner = httpx.AsyncHTTPTransport()
        self.dropped = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._inner.handle_async_request(request)
        if request.url.path.endswith("/complete") and self.dropped == 0:
            await response.aread()
            await response.aclose()
            self.dropped += 1
            raise httpx.ReadError("injected loss of the committed completion", request=request)
        return response

    async def aclose(self) -> None:
        await self._inner.aclose()


class _LoseFail(httpx.AsyncBaseTransport):
    """Real HTTP, except every ``/fail`` call is lost before it reaches the API."""

    def __init__(self) -> None:
        self._inner = httpx.AsyncHTTPTransport()
        self.lost = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/fail"):
            self.lost += 1
            raise httpx.ConnectError("injected loss of the withdrawal", request=request)
        return await self._inner.handle_async_request(request)

    async def aclose(self) -> None:
        await self._inner.aclose()


class _CutBaseUpload(CandidateObjectStore):
    """The real store; the first base object upload is cut before any byte lands."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.cut = 0

    def upload_verified(self, key: str, *args: Any, **kwargs: Any) -> None:
        if key.endswith("/base") and self.cut == 0:
            self.cut += 1
            raise ConnectionResetError("injected cut of the base upload")
        super().upload_verified(key, *args, **kwargs)


async def _drain(collector: RecoveryCollector, *, passes: int = 10) -> list[RecoveryCollectionPass]:
    results: list[RecoveryCollectionPass] = []
    for _ in range(passes):
        result = await collector.collect_once()
        results.append(result)
        if result.claimed == 0:
            return results
    raise AssertionError(f"collection never settled: {results}")


# --- one stage: a WorkItem, its thread, its claim, its runner and its run ------------------


@dataclass
class _Stage:
    services: _Services
    h: Any
    root: Path
    engine: AsyncEngine
    http: httpx.AsyncClient
    dispatch: WorkItemDispatchClient
    coordinator: WorkspaceClaimCoordinator
    runner: SessionRunner
    runner_server: TestServer
    runner_client: RunnerClient
    checkout: Path
    thread_key: str
    address: str
    wire: str
    issue: str
    agent_id: uuid.UUID
    script: list[Callable[[], list[Any]]]
    handle: Any = None
    prepared: Any = None
    work_item_id: uuid.UUID | None = None
    run: WorkItemRun | None = None
    expected_tree: dict[str, tuple[str, int, str]] = field(default_factory=dict)
    local_head: str = ""
    runner_closed: bool = False
    owned_clients: list[httpx.AsyncClient] = field(default_factory=list)

    async def sql(self, statement: str, **params: Any) -> list[dict[str, Any]]:
        async with self.engine.begin() as connection:
            result = await connection.execute(text(statement), params)
            return [dict(row._mapping) for row in result] if result.returns_rows else []

    def exists(self, physical: str) -> bool:
        try:
            self.services.s3.head_object(Bucket=self.services.bucket, Key=physical)
        except ClientError as exc:
            if _missing(exc):
                return False
            raise
        return True

    @property
    def wake(self) -> FactoryWake:
        return FactoryWake(
            kind="slack", address=self.address, adapter="default", conversation_id=self.wire
        )

    async def admit(self) -> uuid.UUID:
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
            # The trusted base the factory resolved at admission (ADR 0186).
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
        return await self.dispatch.acquire(
            request_id, owner=owner, generation=generation, wake=self.wake, context=None
        )

    async def claim_thread(self) -> None:
        result = await asyncio.to_thread(
            self.coordinator.claim_or_resume_with_handle,
            thread_key=self.thread_key,
            deployment_id=DEPLOYMENT_ID,
            env={RUNNER_TOKEN_ENV: RUNNER_TOKEN},
        )
        assert result.handle.token == RUNNER_TOKEN
        assert result.prepared.base_sha == self.services.base_commit
        self.handle, self.prepared = result.handle, result.prepared
        archive = b"".join(self.coordinator.preparer.objects.get_stream(self.prepared.object_key))
        assert _sha(archive) == self.prepared.sha256
        # The runner's managed workspace is the real claim base, extracted.
        self.checkout.rmdir()
        _extract(archive, self.checkout)
        assert _git(self.checkout, "rev-parse", "HEAD").strip() == self.services.base_commit

    async def acquired_run(self) -> WorkItemRun:
        """An acquired, not yet started run: it has no runtime epoch."""

        request_id = await self.admit()
        owner = self.h.config.consumer_name
        grant = await self.acquire(request_id, owner)
        return WorkItemRun(
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

    async def start_run(self) -> WorkItemRun:
        run = await self.acquired_run()
        await run.start(claim_name=self.handle.claim_name, sandbox_name=self.handle.sandbox_name)
        assert run.started and run.runtime_epoch is not None
        self.run = run
        return run

    async def agent_turn(self) -> None:
        """One real runner turn whose scripted model edits the checkout."""

        self.script[0] = _successful_turn(self.checkout)
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
        assert final is not None
        runner = self.runner
        await asyncio.to_thread(
            wait_until, lambda: not runner.turn_active, 10, "the runner never ended the turn"
        )
        self.expected_tree = _tree(self.checkout)
        self.local_head = _git(self.checkout, "rev-parse", "HEAD").strip()

    def direct_edits(self, seed: int = 31) -> None:
        """Edits made straight in the checkout, for cases that need no model turn."""

        (self.checkout / "src/app.py").write_text(f"print('acme fix {seed}')\n")
        (self.checkout / "notes").mkdir(exist_ok=True)
        (self.checkout / f"notes/edit-{seed}.bin").write_bytes(_binary(1024, seed))

    async def snapshot(self) -> RunnerWorkspaceSnapshot:
        return await self.runner_client.snapshot(
            self.handle.base_url, token=RUNNER_TOKEN, base_sha=self.prepared.base_sha
        )

    async def client(self, transport: httpx.AsyncBaseTransport | None = None) -> httpx.AsyncClient:
        http = httpx.AsyncClient(timeout=30.0, transport=transport)
        self.owned_clients.append(http)
        return http

    async def ledger(
        self, *, api_url: str | None = None, transport: httpx.AsyncBaseTransport | None = None
    ) -> _RecordingLedger:
        return _RecordingLedger(
            api_base_url=api_url or self.services.api_url,
            worker_token=self.services.worker_token,
            client=await self.client(transport),
        )

    def store(self) -> CandidateObjectStore:
        return CandidateObjectStore(client=self.services.s3, bucket=self.services.bucket)

    def capture(
        self,
        ledger: PublicationCandidateClient,
        *,
        budget_seconds: float = 60.0,
        store: CandidateObjectStore | None = None,
    ) -> PublicationCandidateCapture:
        return PublicationCandidateCapture(
            ledger=ledger,
            writer=CandidateWriter(
                ledger=ledger,
                store=store or self.store(),
                scratch_root=self.root / "candidate-scratch",
                base_archive_limit_bytes=ARCHIVE_LIMIT,
            ),
            coordinator=self.coordinator,
            budget_seconds=budget_seconds,
        )

    async def capture_once(
        self,
        snapshot: RunnerWorkspaceSnapshot,
        *,
        dedupe_key: str | None = None,
        ledger: PublicationCandidateClient | None = None,
        budget_seconds: float = 60.0,
        run: WorkItemRun | None = None,
        store: CandidateObjectStore | None = None,
    ) -> CandidateCaptureResult:
        run = run or self.run
        assert run is not None
        ledger = ledger or await self.ledger()
        return await self.capture(ledger, store=store).capture(
            run,
            snapshot,
            self.prepared,
            dedupe_key=dedupe_key or f"capture-{uuid.uuid4().hex}",
            budget_seconds=budget_seconds,
        )

    async def candidates(self) -> list[dict[str, Any]]:
        return await self.sql(
            "SELECT id, status, origin_request_id, idempotency_key "
            "FROM curie.publication_candidates WHERE work_item_id = :id",
            id=self.work_item_id,
        )

    async def objects(self) -> list[dict[str, Any]]:
        return await self.sql(
            "SELECT candidate_id, kind, key, size_bytes, sha256, status, collector_id "
            "FROM curie.publication_candidate_objects WHERE work_item_id = :id",
            id=self.work_item_id,
        )

    async def groups(self) -> list[dict[str, Any]]:
        return await self.sql(
            "SELECT work_item_id FROM curie.publication_candidate_groups WHERE work_item_id = :id",
            id=self.work_item_id,
        )

    async def outcome(self) -> str | None:
        rows = await self.sql(
            "SELECT code FROM curie.publication_candidate_outcomes WHERE work_item_id = :id",
            id=self.work_item_id,
        )
        return None if not rows else str(rows[0]["code"]).removeprefix("publication_candidates.")

    async def finish_and_expire(self) -> None:
        """Final disposition, then move it back two days so retention has lapsed."""

        run = self.run
        assert run is not None
        await self.h.kernel._finish_or_settle(
            run,
            outcome="failed",
            cause="runner_failed",
            detail=None,
            ci_fix_round=None,
            # Unproven safety earns no automatic successor, so this disposition is final.
            retry_safety="uncertain",
            retry_condition=None,
        )
        assert run.finished
        await self.sql(
            "UPDATE curie.execution_requests SET terminal_at = terminal_at - interval '2 days' "
            "WHERE id = :id",
            id=run.request_id,
        )

    async def release_claim(self) -> None:
        """The delivery ``finally`` of kernel/lifecycle.py for this run."""

        run = self.run
        assert run is not None
        await run.close()
        await self.h.kernel._release_work_item_sandbox(run)

    async def stop_runner(self) -> None:
        if not self.runner_closed:
            self.runner_closed = True
            await self.runner_server.close()


def _reader(stage: _Stage, transport: httpx.AsyncBaseTransport, ledger: Any) -> CandidateReader:
    return CandidateReader(
        ledger=ledger, store=stage.store(), transport=transport, reference_ttl_seconds=60
    )


async def _read_candidate(
    stage: _Stage, candidate_id: uuid.UUID, into: Path, kinds: tuple[str, ...] = ("base", "patch")
) -> tuple[dict[str, Path], dict[str, Any]]:
    """Lease and read a candidate's objects through fresh clients, verifying digests."""

    into.mkdir(parents=True, exist_ok=True)
    transport = httpx.AsyncHTTPTransport()
    async with httpx.AsyncClient(timeout=30.0) as http:
        ledger = PublicationCandidateClient(
            api_base_url=stage.services.api_url,
            worker_token=stage.services.worker_token,
            client=http,
        )
        reader = _reader(stage, transport, ledger)
        paths: dict[str, Path] = {}
        try:
            assert stage.work_item_id is not None
            lease = await reader.acquire(
                work_item_id=stage.work_item_id,
                candidate_id=candidate_id,
                holder_id=f"verify-{uuid.uuid4().hex[:12]}",
            )
            try:
                for kind in kinds:
                    destination = into / f"{uuid.uuid4().hex[:8]}-{kind}"
                    await reader.read(lease, kind, destination)
                    ref = lease.objects[kind]
                    assert _sha(destination.read_bytes()) == ref.sha256, kind
                    assert destination.stat().st_size == ref.size_bytes, kind
                    paths[kind] = destination
            finally:
                await reader.release(lease)
        finally:
            await transport.aclose()
    return paths, {kind: lease.objects[kind] for kind in lease.objects}


@contextlib.asynccontextmanager
async def _stage(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
    **config_overrides: object,
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
            trace_name="publication-candidates",
        ),
        session_id="session-acme-candidates",
        sandbox_id="sandbox-acme-candidates",
        cwd=str(checkout),
    )
    runner_app: web.Application = create_runner_app(
        runner,
        token=RUNNER_TOKEN,
        snapshotter=workspace_snapshot.workspace_snapshotter(
            checkout,
            trusted_origin=CODE_HOST_ORIGIN,
            repository_path=REPOSITORY_PATH,
            proposal=lambda: (TITLE, BODY),
        ),
    )
    runner_server = TestServer(runner_app)
    await runner_server.start_server()
    assert runner_server.port is not None
    runner_client = RunnerClient(total_timeout_s=120.0)
    stage: _Stage | None = None
    try:
        async with httpx.AsyncClient(timeout=30.0) as http:
            built: dict[str, WorkspaceClaimCoordinator] = {}

            def workspace_factory(substrate: Any) -> WorkspaceClaimCoordinator:
                built["coordinator"] = WorkspaceClaimCoordinator(
                    preparer=WorkspacePreparer(
                        credentials=_CredentialClient(),
                        commands=_LocalOriginCommands(services.origin),
                        objects=WorkspaceObjectStore(
                            client=services.s3,
                            bucket=services.bucket,
                            prefix="private/workspaces",
                        ),
                        scratch_root=root / "clone-scratch",
                        limits=_limits(),
                    ),
                    substrate=substrate,
                )
                return built["coordinator"]

            async with kernel_harness(
                names,
                sync_redis,
                workspace_factory=workspace_factory,
                workspace_scratch_root=str(root / "validation-scratch"),
                **config_overrides,
            ) as h:
                (root / "validation-scratch").mkdir()
                # Every sandbox this test claims dials the real runner above.
                h.fake_k8s.runner_ports.extend([runner_server.port] * 8)
                agent_id = uuid.uuid4()
                address = f"C0CAND{uuid.uuid4().hex[:8].upper()}"
                dispatch = WorkItemDispatchClient(
                    api_base_url=services.api_url, worker_token=services.worker_token, client=http
                )
                stage = _Stage(
                    services=services,
                    h=h,
                    root=root,
                    engine=engine,
                    http=http,
                    dispatch=dispatch,
                    coordinator=built["coordinator"],
                    runner=runner,
                    runner_server=runner_server,
                    runner_client=runner_client,
                    checkout=checkout,
                    thread_key=f"candidate-{uuid.uuid4().hex}",
                    address=address,
                    wire=f"{uuid.uuid4().int % 10**10}.000100",
                    issue=str(uuid.uuid4().int % 10**9),
                    agent_id=agent_id,
                    script=script,
                )
                h.kernel._work_items = dispatch
                await stage.sql(
                    "INSERT INTO curie.agents (id, name, repo_full_name) "
                    "VALUES (:id, :name, :repo)",
                    id=agent_id,
                    name=f"acme-candidates-{agent_id.hex[:10]}",
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
                    if stage.run is not None:
                        await stage.run.close()
                    for client in stage.owned_clients:
                        await client.aclose()
                    # The writer's private scratch never outlives a capture.
                    scratch = root / "candidate-scratch"
                    assert not scratch.exists() or list(scratch.iterdir()) == []
    finally:
        await runner_client.close()
        with contextlib.suppress(Exception):
            await runner_server.close()
        await engine.dispose()
        shutil.rmtree(root, ignore_errors=True)
        assert not root.exists(), f"owned stage directory {root} survived"


# --- AC1: retained before validation, read back exactly after the runtime is gone ---------


def test_candidate_retained_before_failed_validation_and_restored_exactly(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(
            services, names, sync_redis, tmp_path, publication_protected_paths=(PROTECTED_PATH,)
        ) as stage:
            run = await stage.start_run()
            await stage.agent_turn()
            assert stage.local_head != services.base_commit
            snapshot = await stage.snapshot()
            assert PROTECTED_PATH in snapshot.changed_paths
            assert snapshot.base_sha == services.base_commit

            # The real kernel, wired with the real capture behind a transport that
            # loses the first completion's response after the API committed it.
            dropper = _DropCompleteResponse()
            ledger = await stage.ledger(transport=dropper)
            stage.h.kernel._publication_candidates = stage.capture(ledger)
            qevent = _qevent(run.event_id)

            with pytest.raises(WorkspacePreparationError) as refused:
                await stage.h.kernel._retain_and_validate_publication_snapshot(
                    qevent, stage.thread_key, snapshot, run, 120.0
                )
            # Validation failed exactly as it does without a candidate.
            with pytest.raises(WorkspacePreparationError) as direct:
                validate_snapshot_against_base(
                    stage.coordinator,
                    thread_key=stage.thread_key,
                    snapshot=snapshot,
                    max_patch_bytes=stage.h.config.publication_patch_max_bytes,
                    scratch_root=Path(stage.h.config.workspace_scratch_root),
                    git_timeout_seconds=stage.h.config.publication_git_command_timeout_seconds,
                    protected_paths=stage.h.config.publication_protected_paths,
                    allow_dependency_additions=stage.h.config.publication_allow_dependency_additions,
                )
            assert str(refused.value) == str(direct.value)

            # The lost response was retried against the idempotent completion.
            assert dropper.dropped == 1
            assert ledger.completes == 2
            rows = await stage.candidates()
            assert len(rows) == 1, rows
            candidate = rows[0]
            assert candidate["status"] == "complete"
            assert candidate["origin_request_id"] == run.request_id
            assert candidate["idempotency_key"] == run.event_id
            assert [r["replayed"] for r in ledger.reservations][-1] is False

            # The same event again: one candidate for the identity, replayed.
            replay_ledger = await stage.ledger()
            stage.h.kernel._publication_candidates = stage.capture(replay_ledger)
            with pytest.raises(WorkspacePreparationError):
                await stage.h.kernel._retain_and_validate_publication_snapshot(
                    qevent, stage.thread_key, snapshot, run, 120.0
                )
            assert [r["replayed"] for r in replay_ledger.reservations] == [True]
            again = await stage.candidates()
            assert [(r["id"], r["status"]) for r in again] == [(candidate["id"], "complete")]

            # Destroy the original runtime: release the claim through the kernel's
            # own release path (its base archive and ownership go), stop the runner
            # and remove the checkout.
            claim_base = f"private/workspaces/{stage.prepared.object_key}"
            assert stage.exists(claim_base)
            await stage.release_claim()
            assert not stage.exists(claim_base)
            assert not stage.exists(
                f"private/workspaces/_ownership/{_ownership(stage.thread_key)}.json"
            )
            assert stage.coordinator.current(stage.thread_key) is None
            await stage.stop_runner()
            shutil.rmtree(stage.checkout)

            # Restore from the candidate alone, through fresh clients and objects.
            receipts = {row["kind"]: row for row in await stage.objects()}
            assert set(receipts) == {"base", "patch"}
            assert receipts["base"]["sha256"] == stage.prepared.sha256
            assert receipts["patch"]["sha256"] == _sha(snapshot.patch)
            paths, refs = await _read_candidate(stage, candidate["id"], stage.root / "restore")
            for kind in ("base", "patch"):
                assert refs[kind].sha256 == receipts[kind]["sha256"], kind
                assert refs[kind].size_bytes == receipts[kind]["size_bytes"], kind
            restored = _extract(paths["base"].read_bytes(), stage.root / "restored")
            assert _git(restored, "rev-parse", "HEAD").strip() == services.base_commit
            _git(restored, "apply", "--binary", str(paths["patch"]))
            assert _tree(restored) == stage.expected_tree
            assert not (restored / "docs/remove-me.txt").exists()
            assert stage.expected_tree["bin/run.sh"][2] == "100755"
            assert stage.expected_tree["bin/tool.sh"][2] == "100755"
            assert "notes/untracked.txt" in stage.expected_tree
            assert "assets/added.bin" in stage.expected_tree

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


# --- AC2: refusals, pinning and collection against the real store ------------------------


def test_corrupt_base_refuses_reads_and_other_work_items_still_capture(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            await stage.start_run()
            stage.direct_edits()
            result = await stage.capture_once(await stage.snapshot())
            assert result.status == "complete" and result.candidate_id is not None
            base = next(row for row in await stage.objects() if row["kind"] == "base")
            services.s3.put_object(
                Bucket=services.bucket, Key=f"private/{base['key']}", Body=b"corrupted base"
            )
            with pytest.raises(CandidateReadRefused) as refused:
                await _read_candidate(stage, result.candidate_id, stage.root / "corrupt")
            assert refused.value.code == "digest_mismatch"
            # The patch is untouched and still verifies.
            await _read_candidate(
                stage, result.candidate_id, stage.root / "patch-only", kinds=("patch",)
            )
        async with _stage(services, names, sync_redis, tmp_path) as other:
            await other.start_run()
            other.direct_edits(seed=32)
            fresh = await other.capture_once(await other.snapshot())
            assert fresh.status == "complete", fresh
            assert fresh.candidate_id is not None
            await _read_candidate(other, fresh.candidate_id, other.root / "fresh")

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


def test_reader_lease_pins_a_tombstoned_patch_until_released(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            await stage.start_run()
            stage.direct_edits()
            result = await stage.capture_once(await stage.snapshot())
            assert result.status == "complete" and result.candidate_id is not None
            collector = build_candidate_collector(
                ledger=await stage.ledger(),
                store=stage.store(),
                collector_id=f"pin-{uuid.uuid4().hex[:12]}",
            )
            await _drain(collector)
            ledger = await stage.ledger()
            transport = httpx.AsyncHTTPTransport()
            reader = _reader(stage, transport, ledger)
            try:
                assert stage.work_item_id is not None
                lease = await reader.acquire(
                    work_item_id=stage.work_item_id,
                    candidate_id=result.candidate_id,
                    holder_id="pinning-reader",
                )
                await stage.finish_and_expire()
                await _drain(collector)
                # Tombstoned, but the leased patch survives collection.
                rows = await stage.candidates()
                assert [row["status"] for row in rows] == ["tombstoned"], rows
                patch = next(row for row in await stage.objects() if row["kind"] == "patch")
                assert stage.exists(f"private/{patch['key']}")
                destination = stage.root / "pinned-patch"
                await reader.read(lease, "patch", destination)
                assert _sha(destination.read_bytes()) == patch["sha256"]
                await reader.release(lease)
            finally:
                await transport.aclose()
            # Released, the next passes remove the patch, the candidate, the base
            # and the group.
            await _drain(collector)
            assert await stage.candidates() == []
            assert await stage.objects() == []
            assert await stage.groups() == []
            assert not stage.exists(f"private/{patch['key']}")

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


def test_candidate_collector_killed_before_finalize(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            await stage.start_run()
            stage.direct_edits()
            result = await stage.capture_once(await stage.snapshot())
            assert result.status == "complete"
            settle = build_candidate_collector(
                ledger=await stage.ledger(),
                store=stage.store(),
                collector_id=f"settle-{uuid.uuid4().hex[:12]}",
            )
            # Nothing else in the installation is eligible before this one expires.
            await _drain(settle)
            await stage.finish_and_expire()
            keys = {row["key"] for row in await stage.objects()}
            collector_id = f"interrupted-{uuid.uuid4().hex[:12]}"
            completed = await asyncio.to_thread(
                subprocess.run,
                [sys.executable, str(INTERRUPT_SCRIPT)],
                env={
                    **os.environ,
                    "CANDIDATE_API_URL": services.api_url,
                    "CANDIDATE_WORKER_TOKEN": services.worker_token,
                    "CANDIDATE_S3_ENDPOINT": services.endpoint,
                    "CANDIDATE_S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY", ""),
                    "CANDIDATE_S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY", ""),
                    "CANDIDATE_BUCKET": services.bucket,
                    "CANDIDATE_COLLECTOR_ID": collector_id,
                },
                capture_output=True,
                text=True,
                check=False,
                timeout=180,
            )
            assert completed.returncode == EXIT_BEFORE_FINALIZE, completed.stderr[-1500:]
            claimed = [row for row in await stage.objects() if row["status"] == "claimed"]
            assert claimed, await stage.objects()
            assert {row["collector_id"] for row in claimed} == {collector_id}
            assert any(not stage.exists(f"private/{row['key']}") for row in claimed)
            assert await stage.candidates() != []

            # The killed collector's claim lapses; a fresh collector finishes.
            await stage.sql(
                "UPDATE curie.publication_candidate_objects "
                "SET claimed_until = clock_timestamp() - interval '1 second' "
                "WHERE work_item_id = :id AND status = 'claimed'",
                id=stage.work_item_id,
            )
            fresh = build_candidate_collector(
                ledger=await stage.ledger(),
                store=stage.store(),
                collector_id=f"fresh-{uuid.uuid4().hex[:12]}",
            )
            passes = await _drain(fresh)
            assert sum(item.finalized for item in passes) >= len(keys), passes
            assert all(item.failed == 0 for item in passes), passes
            assert await stage.candidates() == []
            assert await stage.objects() == []
            assert await stage.groups() == []
            for key in keys:
                assert not stage.exists(f"private/{key}"), key

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


def test_capacity_refusal_is_recorded_and_keeps_the_existing_candidate(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            await stage.start_run()
            stage.direct_edits()
            snapshot = await stage.snapshot()
            kept = await stage.capture_once(snapshot)
            assert kept.status == "complete" and kept.candidate_id is not None
            before = await stage.objects()
            with _variant_api(CURIE_PUBLICATION_CANDIDATE_MAX_BYTES="1") as tiny:
                refused = await stage.capture_once(
                    snapshot, ledger=await stage.ledger(api_url=tiny)
                )
            assert refused.status == "failed", refused
            assert refused.code == "installation_capacity"
            assert refused.candidate_id is None
            outcome = await stage.outcome()
            assert outcome == "installation_capacity", outcome
            assert [row["id"] for row in await stage.candidates()] == [kept.candidate_id]
            assert await stage.objects() == before
            await _read_candidate(stage, kept.candidate_id, stage.root / "kept")
            # Liveness: under the configured cap the same capture is admitted.
            admitted = await stage.capture_once(snapshot)
            assert admitted.status == "complete", admitted

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


def test_disabled_ledger_skips_capture_and_creates_nothing(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            await stage.start_run()
            stage.direct_edits()
            snapshot = await stage.snapshot()
            with _variant_api(CURIE_PUBLICATION_CANDIDATES_ENABLED="false") as disabled:
                result = await stage.capture_once(
                    snapshot, ledger=await stage.ledger(api_url=disabled)
                )
            assert result == CandidateCaptureResult("skipped", "disabled")
            assert await stage.candidates() == []
            assert await stage.objects() == []
            assert await stage.groups() == []
            assert await stage.outcome() is None
            # Liveness: the enabled ledger retains the same snapshot.
            enabled = await stage.capture_once(snapshot)
            assert enabled.status == "complete", enabled

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


def test_exhausted_budget_records_capture_timeout(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            await stage.start_run()
            stage.direct_edits()
            snapshot = await stage.snapshot()
            result = await stage.capture_once(snapshot, budget_seconds=0.0)
            assert result == CandidateCaptureResult("failed", "capture_timeout")
            assert await stage.outcome() == "capture_timeout"
            assert await stage.candidates() == []
            assert await stage.objects() == []
            # Liveness: with budget left the same capture completes.
            admitted = await stage.capture_once(snapshot)
            assert admitted.status == "complete", admitted

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


def test_recapture_after_a_cut_base_upload_uploads_the_base(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            await stage.start_run()
            stage.direct_edits()
            snapshot = await stage.snapshot()
            key = f"capture-{uuid.uuid4().hex}"

            # The with-base reserve commits, then the base upload is cut and the
            # writer's withdrawal is lost: the candidate stays staging with no
            # base bytes in the store.
            cut = _CutBaseUpload(client=services.s3, bucket=services.bucket)
            loser = _LoseFail()
            first = await stage.capture_once(
                snapshot,
                dedupe_key=key,
                ledger=await stage.ledger(transport=loser),
                store=cut,
            )
            assert first.status == "failed" and first.code == "upload_failed", first
            assert cut.cut == 1 and loser.lost == 1
            assert [row["status"] for row in await stage.candidates()] == ["staging"]
            base = next(row for row in await stage.objects() if row["kind"] == "base")
            assert not stage.exists(f"private/{base['key']}")

            # The same capture again is told to declare the base, then replays
            # with both receipts and uploads both.
            ledger = await stage.ledger()
            second = await stage.capture_once(snapshot, dedupe_key=key, ledger=ledger)
            assert second.status == "complete", second
            assert second.candidate_id == first.candidate_id
            assert [r["replayed"] for r in ledger.reservations] == [True]
            assert sorted(obj["kind"] for obj in ledger.reservations[0]["objects"]) == [
                "base",
                "patch",
            ]
            assert [row["status"] for row in await stage.candidates()] == ["complete"]
            receipts = {row["kind"]: row for row in await stage.objects()}
            assert receipts["base"]["sha256"] == stage.prepared.sha256
            stored = services.s3.get_object(
                Bucket=services.bucket, Key=f"private/{receipts['base']['key']}"
            )["Body"].read()
            assert _sha(stored) == receipts["base"]["sha256"]
            assert second.candidate_id is not None
            await _read_candidate(stage, second.candidate_id, stage.root / "recaptured")

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


# --- liveness of each capture refusal branch ------------------------------------------------


def test_capture_refusal_branches_and_their_valid_neighbours(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            run = await stage.start_run()
            stage.direct_edits()
            snapshot = await stage.snapshot()

            # No runtime epoch (the started-ness signal): skipped, nothing reaches
            # the ledger.
            unstarted = copy.copy(run)
            unstarted.runtime_epoch = None
            skipped = await stage.capture_once(snapshot, run=unstarted)
            assert skipped == CandidateCaptureResult("skipped", "no_runtime_epoch")
            # No patch: skipped quietly.
            empty = dataclasses.replace(snapshot, patch=b"", changed_paths=())
            assert await stage.capture_once(empty) == CandidateCaptureResult("skipped", "no_patch")
            assert await stage.outcome() is None

            # A runner anchor that is not the prepared base: recorded base_mismatch.
            moved = dataclasses.replace(snapshot, base_sha="0" * 40)
            assert await stage.capture_once(moved) == CandidateCaptureResult(
                "failed", "base_mismatch"
            )
            assert await stage.outcome() == "base_mismatch"
            # A snapshot of another repository: recorded base_mismatch.
            foreign = dataclasses.replace(snapshot, repo_full_name="acme-corp/other-bot")
            assert await stage.capture_once(foreign) == CandidateCaptureResult(
                "failed", "base_mismatch"
            )
            # An unbounded changed path list: recorded capture_failed.
            unbounded = dataclasses.replace(
                snapshot, changed_paths=tuple(f"notes/{i}.txt" for i in range(4097))
            )
            assert await stage.capture_once(unbounded) == CandidateCaptureResult(
                "failed", "capture_failed"
            )
            assert await stage.outcome() == "capture_failed"
            assert await stage.candidates() == []

            # Valid neighbours: the repository compares casefolded, and the real
            # snapshot is retained.
            cased = dataclasses.replace(snapshot, repo_full_name=REPOSITORY_PATH.upper())
            first = await stage.capture_once(cased)
            assert first.status == "complete", first
            second = await stage.capture_once(snapshot)
            assert second.status == "complete", second
            assert len(await stage.candidates()) == 2

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


# --- the kernel helper ----------------------------------------------------------------------


def test_helper_without_candidates_validates_exactly_as_before(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            run = await stage.start_run()
            stage.direct_edits()
            snapshot = await stage.snapshot()
            assert stage.h.kernel._publication_candidates is None
            qevent = _qevent(run.event_id)
            # A passing snapshot still passes.
            await stage.h.kernel._retain_and_validate_publication_snapshot(
                qevent, stage.thread_key, snapshot, run, 120.0
            )
            # A failing one still raises the same message as the validator.
            bad = dataclasses.replace(snapshot, base_sha="0" * 40)
            with pytest.raises(WorkspacePreparationError) as raised:
                await stage.h.kernel._retain_and_validate_publication_snapshot(
                    qevent, stage.thread_key, bad, run, 120.0
                )
            with pytest.raises(WorkspacePreparationError) as direct:
                validate_snapshot_against_base(
                    stage.coordinator,
                    thread_key=stage.thread_key,
                    snapshot=bad,
                    max_patch_bytes=stage.h.config.publication_patch_max_bytes,
                    scratch_root=Path(stage.h.config.workspace_scratch_root),
                    git_timeout_seconds=stage.h.config.publication_git_command_timeout_seconds,
                    protected_paths=stage.h.config.publication_protected_paths,
                    allow_dependency_additions=stage.h.config.publication_allow_dependency_additions,
                )
            assert str(raised.value) == str(direct.value)
            assert await stage.candidates() == []

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


def test_helper_captures_only_for_this_events_started_run_with_budget(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            run = await stage.start_run()
            stage.direct_edits()
            snapshot = await stage.snapshot()
            stage.h.kernel._publication_candidates = stage.capture(await stage.ledger())
            # Another event's turn, and a snapshot budget that cannot keep 30 s for
            # validation and still give the capture 5 s, capture nothing; validation
            # passes either way.
            await stage.h.kernel._retain_and_validate_publication_snapshot(
                _qevent("some-other-event"), stage.thread_key, snapshot, run, 120.0
            )
            await stage.h.kernel._retain_and_validate_publication_snapshot(
                _qevent(run.event_id), stage.thread_key, snapshot, run, 39.0
            )
            await stage.h.kernel._retain_and_validate_publication_snapshot(
                _qevent(run.event_id), stage.thread_key, snapshot, None, 120.0
            )
            assert await stage.candidates() == []
            # Liveness: this event's started run with budget is retained, and a
            # passing validation still passes.
            await stage.h.kernel._retain_and_validate_publication_snapshot(
                _qevent(run.event_id), stage.thread_key, snapshot, run, 120.0
            )
            rows = await stage.candidates()
            assert [(row["status"], row["idempotency_key"]) for row in rows] == [
                ("complete", run.event_id)
            ]

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


def test_helper_capture_is_bounded_by_the_execution_deadline(
    services: _Services,
    names: dict[str, str],
    sync_redis: redis.Redis,
    tmp_path: Path,
) -> None:
    async def go() -> None:
        async with _stage(services, names, sync_redis, tmp_path) as stage:
            run = await stage.start_run()
            assert run.execution_deadline is not None
            stage.direct_edits()
            snapshot = await stage.snapshot()
            stage.h.kernel._publication_candidates = stage.capture(await stage.ledger())
            qevent = _qevent(run.event_id)
            granted = run.execution_deadline
            try:
                # A large delivery budget, but 40 s to the execution deadline cannot
                # keep 30 s for validation and 5 s for a failure report and still
                # give the capture 5 s: nothing is captured, validation passes.
                run.execution_deadline = datetime.now(UTC) + timedelta(seconds=40)
                await stage.h.kernel._retain_and_validate_publication_snapshot(
                    qevent, stage.thread_key, snapshot, run, 600.0
                )
                assert await stage.candidates() == []
                assert await stage.outcome() is None
                # Liveness: the same with a far execution deadline captures.
                run.execution_deadline = datetime.now(UTC) + timedelta(hours=1)
                await stage.h.kernel._retain_and_validate_publication_snapshot(
                    qevent, stage.thread_key, snapshot, run, 600.0
                )
            finally:
                run.execution_deadline = granted
            rows = await stage.candidates()
            assert [(row["status"], row["idempotency_key"]) for row in rows] == [
                ("complete", run.event_id)
            ]

    asyncio.run(go())
    _assert_valkey_clean(sync_redis, names)


# --- production wiring ----------------------------------------------------------------------


@pytest.mark.parametrize("enabled", [False, True])
def test_build_wires_the_candidate_capture_and_collector_only_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
    sync_redis: redis.Redis,
    enabled: bool,
) -> None:
    assert "publication-candidates-gc" in worker_run._SUPERVISED_OPERATIONS
    endpoint = sync_redis.connection_pool.connection_kwargs
    config = WorkerConfig(
        fake_model=True,
        valkey_host=str(endpoint["host"]),
        valkey_port=int(endpoint["port"]),
        valkey_password=str(endpoint.get("password") or ""),
        valkey_db=int(endpoint.get("db", 0)),
        s3_access_key="PLACEHOLDER",
        s3_secret_key="PLACEHOLDER",
        workspace_enabled=True,
        internal_worker_token="candidate-wiring-token",
        publication_candidates_enabled=enabled,
    )
    monkeypatch.setattr(DockerSandboxClient, "ensure_image", lambda self: None)

    async def exercise() -> None:
        runtime = worker_run.build(config, {"CURIE_SANDBOX_SUBSTRATE": "docker"})
        kernel = runtime.consumer._kernel
        affinity_redis = kernel._substrate._affinity._redis
        collector = runtime.publication_candidates_gc
        try:
            assert isinstance(kernel._workspace, WorkspaceClaimCoordinator)
            if not enabled:
                assert collector is None
                assert kernel._publication_candidates is None
                return
            assert isinstance(kernel._publication_candidates, PublicationCandidateCapture)
            assert isinstance(collector, RecoveryCollector)
            store = collector._store
            assert isinstance(store, CandidateObjectStore)
            assert store._bucket == config.workspace_bucket
            assert store._key("publication-candidates/x") == "private/publication-candidates/x"
            with pytest.raises(ValueError):
                store._key("workspace-recovery/x")
            with pytest.raises(ValueError):
                store._key("publication-candidates/../workspaces/x")
            assert collector._collector_id == config.consumer_name
        finally:
            if collector is not None:
                collector._store._client.close()
            affinity_redis.close()
            await runtime.runner.close()
            await runtime.sink.aclose()
            await runtime.eval_http.aclose()
            await runtime.async_redis.aclose()
            await runtime.pressure_async_redis.aclose()
            await runtime.eval_redis.aclose()
            await runtime.engine.dispose()

    asyncio.run(exercise())
