"""Real worker, workspace, admission, HTTP dispatch and SQL wiring for #5155."""

from __future__ import annotations

import asyncio
import base64
import importlib.util
import json
import os
import random
import shutil
import socket
import threading
import uuid
from collections.abc import Iterator
from contextlib import AsyncExitStack
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import asyncpg
import boto3
import httpx
import pytest
import redis
import uvicorn
from aci_protocol import QueuedTurn, ReplyHandle, TurnSource
from alembic import command
from alembic.config import Config
from botocore.config import Config as S3Config
from curie_api import models, workitem_dispatch
from curie_api.config import get_settings
from curie_api.db import create_sessionmaker
from curie_api.main import create_app
from curie_api.workitems import lifecycle
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.approvals import ApprovalClient
from curie_worker.binding import BindingResolver, FactoryWake
from curie_worker.sandbox import SuspendedThreadError
from curie_worker.workitem_dispatch import WorkItemDispatchClient
from curie_worker.workspace import (
    SubprocessCommands,
    WorkspaceClaimCoordinator,
    WorkspaceCloneLimiter,
    WorkspaceCredential,
    WorkspaceLimits,
    WorkspaceObjectStore,
    WorkspacePreparationError,
    WorkspacePreparer,
    WorkspaceStageTimeout,
)
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine

from apps.worker.tests.kernel.conftest import kernel_harness

ROOT = Path(__file__).resolve().parents[3]
REPOSITORY = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
WORKER_TOKEN = "start-wait-example-worker-token"
QUOTA = "acme-start-wait-quota"
HARD = {
    "pods": "2",
    "requests.cpu": "500m",
    "requests.memory": "512Mi",
    "limits.cpu": "2",
    "limits.memory": "2Gi",
}

_spec = importlib.util.spec_from_file_location(
    "start_wait_github_stub", ROOT / "tools/github-stub/github_stub.py"
)
assert _spec is not None and _spec.loader is not None
_github = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_github)


class CodeHost(_github.GithubStub):
    """Record real Git HTTP requests and optionally refuse authentication."""

    denied = False

    def _git_request(self, handler: Any, path: str, query: str, body: bytes) -> None:
        self.git_requests.append(path)
        if self.denied:
            handler.send_response(401)
            handler.send_header("Content-Length", "0")
            handler.end_headers()
            return
        super()._git_request(handler, path, query, body)


class CredentialIssuer:
    """External credential issuer; preparation and Git remain production code."""

    def __init__(self, host: CodeHost, fault: str) -> None:
        self.host = host
        self.fault = fault
        self.redemptions = 0
        self.entered = threading.Event()
        self.gate: threading.Event | None = None

    def select(self, _deployment: uuid.UUID, _thread: str, _author: str, repo: str) -> str:
        return repo or REPOSITORY

    def redeem(self, _deployment: uuid.UUID, _thread: str) -> WorkspaceCredential:
        self.redemptions += 1
        self.entered.set()
        if self.gate is not None and not self.gate.wait(10):
            raise RuntimeError("credential issuer gate was not released")
        if self.fault == "lookalike":
            raise WorkspacePreparationError("archive", "exceeded the 120s budget")
        return WorkspaceCredential(
            repo_full_name=REPOSITORY,
            clone_url=self.host.clone_url,
            authorization_header=(
                "Basic " + base64.b64encode(b"x-access-token:example-installation-token").decode()
            ),
            origin=self.host.base_url,
            header_form="authorization_basic",
            ca_bundle_ref=str(self.host.ca_file),
            base_branch="missing-ref" if self.fault == "bad-ref" else "main",
        )


@pytest.fixture(scope="module")
def start_wait_database() -> Iterator[str]:
    previous = os.environ.get("DATABASE_URL")
    base = make_url(get_settings().database_url)
    database = f"curie_test_start_wait_{uuid.uuid4().hex}"

    async def admin(statement: str) -> None:
        connection = await asyncpg.connect(
            user=base.username,
            password=base.password,
            host=base.host,
            port=base.port,
            database="postgres",
        )
        try:
            await connection.execute(statement)
        finally:
            await connection.close()

    asyncio.run(admin(f'CREATE DATABASE "{database}"'))
    try:
        database_url = base.set(database=database).render_as_string(hide_password=False)
        os.environ["DATABASE_URL"] = database_url
        get_settings.cache_clear()
        config = Config()
        config.set_main_option("script_location", str(ROOT / "apps/api/alembic"))
        command.upgrade(config, "head")
        yield database_url
    finally:
        asyncio.run(admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous
        get_settings.cache_clear()


class StartWaitCase:
    def __init__(self, root: Path, fault: str, database_url: str) -> None:
        self.root = root
        self.fault = fault
        self.database_url = database_url
        self.now = datetime.now(UTC)
        self.agent_id, self.deployment_id, self.request_id = (uuid.uuid4() for _ in range(3))
        self.conversation = "1700000000.000100"
        self.thread = f"slack:{ADDRESS}:{self.conversation}"
        self.wake = FactoryWake(
            kind="slack", address=ADDRESS, adapter="default", conversation_id=self.conversation
        )
        token = uuid.uuid4().hex
        self.names = {
            "stream": f"test:curie:runs:{token}",
            "group": f"g-{token}",
            "prefix": f"test:curie:worker:{token}",
            "sandbox_prefix": f"test:curie:sandbox:{token}",
        }
        self.payloads: list[dict[str, Any]] = []
        self.uploads: list[str] = []
        self.bucket = f"start-wait-{token}"
        self.stack = AsyncExitStack()
        self.loop = asyncio.new_event_loop()

    def run(self, awaitable: Any) -> Any:
        return self.loop.run_until_complete(awaitable)

    async def db_now(self, _session: Any) -> datetime:
        return self.now

    async def setup(self) -> None:
        self.engine = create_async_engine(self.database_url)
        self.sessions = create_sessionmaker(self.engine)
        self.redis = redis.Redis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PW or None,
            decode_responses=True,
        )
        self.redis.ping()
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("TEST_S3_ENDPOINT_URL")
            or os.environ.get("S3_ENDPOINT_URL", "http://127.0.0.1:29000"),
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "rustfs"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "rustfssecret"),
            region_name="us-east-1",
            config=S3Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 0}),
        )
        self.s3.create_bucket(Bucket=self.bucket)

        def uploaded(params: dict[str, Any], **_: Any) -> None:
            key = str(params["Key"])
            if key.endswith(".tar.gz"):
                self.uploads.append(key)

        self.s3.meta.events.register("before-parameter-build.s3.PutObject", uploaded)
        self.s3.meta.events.register("before-parameter-build.s3.CreateMultipartUpload", uploaded)
        seed = ROOT / "tools/factory-e2e/fixtures/unitconv"
        if self.fault == "archive":
            seed = self.root / "seed"
            shutil.copytree(ROOT / "tools/factory-e2e/fixtures/unitconv", seed)
            (seed / "payload.bin").write_bytes(random.Random(5155).randbytes(32 * 1024 * 1024))
        recording = json.loads(
            (ROOT / "tools/github-stub/recordings/curie-pr-3400.json").read_text()
        )
        self.host = CodeHost(
            self.root / "github",
            recording,
            seed_tree=seed,
            issue_file=ROOT / "tools/factory-e2e/fixtures/unitconv-issue.md",
        )
        self.host.git_requests = []
        self.host.denied = self.fault == "auth"
        await asyncio.to_thread(self.host.start)
        self.issuer = CredentialIssuer(self.host, self.fault)
        self.objects = WorkspaceObjectStore(client=self.s3, bucket=self.bucket)
        self.limiter = WorkspaceCloneLimiter(max_concurrent=1)
        limits = WorkspaceLimits(max_compression_ratio=1000)
        if self.fault == "clone-slot":
            limits = replace(
                limits,
                clone_timeout_seconds=0.005,
                archive_timeout_seconds=0.005,
                upload_timeout_seconds=0.005,
                total_timeout_seconds=0.02,
            )
            slot = self.limiter.acquire("external-clone")
            self.stack.callback(slot.release)
        elif self.fault == "clone":
            limits = replace(limits, clone_timeout_seconds=1e-9)
        elif self.fault == "archive":
            limits = replace(limits, archive_timeout_seconds=0.1)
        elif self.fault == "upload":
            limits = replace(limits, upload_timeout_seconds=1e-9)
        elif self.fault == "archive-size":
            limits = replace(limits, max_archive_bytes=1)
        ticks = iter((0.0, float(limits.total_timeout_seconds + 1)))
        self.preparer = WorkspacePreparer(
            credentials=self.issuer,
            commands=SubprocessCommands(),
            objects=self.objects,
            scratch_root=self.root / "workspaces",
            limits=limits,
            limiter=self.limiter,
            clock=(lambda: next(ticks)) if self.fault == "total" else None,
        )
        async with self.sessions() as session:
            session.add(
                models.Agent(
                    id=self.agent_id,
                    name=f"acme-bot-{self.agent_id.hex[:8]}",
                    repo_full_name=REPOSITORY,
                )
            )
            await session.flush()
            version_id = uuid.uuid4()
            session.add(
                models.AgentChannel(
                    id=uuid.uuid4(),
                    agent_id=self.agent_id,
                    kind="slack",
                    address=ADDRESS,
                    adapter="default",
                )
            )
            session.add(
                models.AgentVersion(
                    id=version_id,
                    agent_id=self.agent_id,
                    version_label="test-start-wait",
                    bundle_ref="example-bundle",
                    created_by="example-test",
                )
            )
            await session.flush()
            session.add(
                models.Deployment(
                    id=self.deployment_id,
                    agent_id=self.agent_id,
                    version_id=version_id,
                    environment=models.Environment.dev,
                    workspace_enabled=True,
                )
            )
            await session.commit()
        async with self.sessions() as session:
            session.add(
                models.ThreadWorkspace(
                    agent_id=self.agent_id,
                    selected_by_deployment_id=self.deployment_id,
                    conversation_id=self.thread,
                    repo_full_name=REPOSITORY,
                    selected_by="U0REQUEST1",
                )
            )
            await session.commit()
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        self.stack.callback(listener.close)
        self.api_url = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(
            uvicorn.Config(
                create_app(),
                log_level="error",
                lifespan="on",
                proxy_headers=False,
            )
        )
        serving = asyncio.create_task(server.serve(sockets=[listener]))

        async def stop_api() -> None:
            server.should_exit = True
            await serving

        self.stack.push_async_callback(stop_api)
        async with asyncio.timeout(15):
            while not server.started:
                if serving.done():
                    await serving
                    raise RuntimeError("test API failed to start")
                await asyncio.sleep(0.01)

        async def observe(request: httpx.Request) -> None:
            if request.url.path.endswith("/defer"):
                self.payloads.append(json.loads(request.content))

        self.http = await self.stack.enter_async_context(
            httpx.AsyncClient(
                base_url=self.api_url,
                event_hooks={"request": [observe]},
            )
        )
        self.dispatch = WorkItemDispatchClient(
            api_base_url=self.api_url, worker_token=WORKER_TOKEN, client=self.http
        )

        def workspace(substrate: Any) -> WorkspaceClaimCoordinator:
            self.workspace = WorkspaceClaimCoordinator(
                preparer=self.preparer, substrate=substrate, suspended_error=SuspendedThreadError
            )
            return self.workspace

        admission = {
            "CURIE_SANDBOX_ADMISSION": json.dumps(
                {
                    "quota_name": QUOTA,
                    "priority_class": "acme-sandbox",
                    "ceiling": 0 if self.fault == "zero" else 2,
                    "hard": HARD,
                }
            )
        }
        self.h = await self.stack.enter_async_context(
            kernel_harness(
                self.names,
                self.redis,
                binding_factory=lambda config: BindingResolver(self.engine, config),
                workspace_factory=workspace,
                work_items=self.dispatch,
                publication_creator=ApprovalClient(
                    api_base_url=self.api_url,
                    api_key="example-api-key",
                    client=self.http,
                    worker_token=WORKER_TOKEN,
                    read_timeout_s=5,
                ),
                max_attempts=1,
                substrate_env={} if self.fault == "disabled" else admission,
                api_key="example-api-key",
                api_base_url=self.api_url,
                internal_worker_token=WORKER_TOKEN,
            )
        )
        response = await self.http.post(
            "/v1/internal/work-items/admissions",
            headers={"X-Curie-Worker-Token": WORKER_TOKEN},
            json={
                "agent_id": str(self.agent_id),
                "kind": "slack",
                "address": ADDRESS,
                "reply_conversation_id": self.conversation,
                "tracker": {
                    "kind": "github",
                    "host": "github.com",
                    "scope_id": "101",
                    "issue_id": "5155",
                },
                "repository": {
                    "code_host_kind": "github",
                    "host": "github.com",
                    "project_id": "101",
                    "path": REPOSITORY,
                },
                "code_host_installation_id": 202,
                "request_id": str(self.request_id),
                "objective": "Implement the admitted work item",
                "requester": "U0REQUEST1",
            },
        )
        assert response.status_code == 200, response.text
        self.initial = await self.state()

    async def state(self) -> dict[str, Any]:
        async with self.sessions() as session:
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT r.*, w.version AS work_item_version "
                            "FROM curie.execution_requests r JOIN curie.work_items w "
                            "ON w.id = r.work_item_id WHERE r.id = :id"
                        ),
                        {"id": self.request_id},
                    )
                )
                .mappings()
                .one()
            )
            return dict(row)

    async def publish(self) -> int:
        row = await self.state()
        self.now = max(self.now, row["dispatch_not_before"])
        async with self.sessions() as session:
            assert await workitem_dispatch.fence_published(
                session, self.request_id, row["dispatch_epoch"], row["dispatch_generation"]
            )
        return int(row["dispatch_generation"])

    def event(self, event_id: str, text_: str = "Implement the admitted work item") -> QueuedTurn:
        return QueuedTurn(
            event_id=event_id,
            conversation_id=self.conversation,
            author="U0REQUEST1",
            text=text_,
            reply_handle=ReplyHandle(
                kind="slack", channel=ADDRESS, adapter="default", placeholder=None
            ),
            received_at="2026-10-29T00:00:00+00:00",
            source=TurnSource.SLACK,
        )

    async def deliver(self) -> None:
        generation = await self.publish()
        await self.h.kernel.process_event(
            self.event(f"work-item-{self.request_id}-execute-{generation}")
        )

    def keys(self) -> list[str]:
        return sorted(self.objects.list_keys(""))

    def ledger(self) -> dict[str, Any]:
        return self.h.substrate._affinity.admission_entries("test-ns", QUOTA)

    async def close(self) -> None:
        issuer = getattr(self, "issuer", None)
        if issuer is not None and issuer.gate is not None:
            issuer.gate.set()
        h = getattr(self, "h", None)
        if h is not None:
            if h.runner.hold is not None:
                h.runner.hold.set()
            for thread in (self.thread, "blocking-one", "blocking-two", "competitor"):
                await asyncio.to_thread(h.substrate.release, thread)
            self.workspace.release(self.thread)
        await self.stack.aclose()
        host = getattr(self, "host", None)
        if host is not None:
            await asyncio.to_thread(host.close)
        s3 = getattr(self, "s3", None)
        if s3 is not None:
            for row in s3.list_objects_v2(Bucket=self.bucket).get("Contents", []):
                s3.delete_object(Bucket=self.bucket, Key=row["Key"])
            s3.delete_bucket(Bucket=self.bucket)
            s3.close()
        client = getattr(self, "redis", None)
        if client is not None:
            for pattern in (
                f"{self.names['prefix']}*",
                f"{self.names['sandbox_prefix']}*",
                self.names["stream"],
            ):
                keys = list(client.scan_iter(match=pattern))
                if keys:
                    client.delete(*keys)
                assert not list(client.scan_iter(match=pattern))
            client.close()
        engine = getattr(self, "engine", None)
        if engine is not None:
            async with self.sessions() as session:
                await session.execute(
                    text("DELETE FROM curie.agents WHERE id = :id"), {"id": self.agent_id}
                )
                await session.commit()
            await engine.dispose()


@pytest.fixture
def start_wait(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    start_wait_database: str,
) -> Iterator[StartWaitCase]:
    for executable in ("git", "openssl"):
        if shutil.which(executable) is None:
            raise RuntimeError(f"required executable unavailable: {executable}")
    for key, value in {
        "INTERNAL_WORKER_TOKEN": WORKER_TOKEN,
        "API_KEY": "example-api-key",
        "RESUME_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "CURIE_DEPLOY_NOTICE_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
        "S3_ACCESS_KEY": os.environ.get("S3_ACCESS_KEY", "rustfs"),
        "S3_SECRET_KEY": os.environ.get("S3_SECRET_KEY", "rustfssecret"),
    }.items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    case = StartWaitCase(tmp_path, getattr(request, "param", "normal"), start_wait_database)
    monkeypatch.setattr(workitem_dispatch, "database_now", case.db_now)
    monkeypatch.setattr(lifecycle, "database_now", case.db_now)
    try:
        case.run(case.setup())
        yield case
    finally:
        try:
            case.run(case.close())
        finally:
            case.loop.close()
            get_settings.cache_clear()


def assert_uncounted_wait(case: StartWaitCase, row: dict[str, Any]) -> None:
    assert row["status"] == "waiting"
    assert row["start_deferrals"] == row["capacity_deferrals"] == 0
    assert row["execution_attempts"] == 0
    assert row["started_at"] is None and row["execution_deadline"] is None
    assert row["wait_deadline"] == case.initial["wait_deadline"]
    assert row["acquire_owner"] is None and row["acquire_expires_at"] is None
    assert row["acquired_generation"] is None
    assert row["terminal_cause"] is None


def assert_timeout_payload(case: StartWaitCase, stage: str) -> None:
    assert len(case.payloads) == 1
    payload = case.payloads[0]
    assert payload["capacity"] is False
    assert not payload["reason"].startswith("not_started:")
    detail = payload["start_failure_detail"]
    assert isinstance(detail, str) and 0 < len(detail) <= 2000
    assert f"workspace {stage} failed:" in detail
    assert "example-installation-token" not in detail
    assert case.h.runner.opened == []


def timeout_lookalike() -> str:
    return str(WorkspaceStageTimeout("archive", 120))
