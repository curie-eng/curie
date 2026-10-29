"""Factory start waits cross preparation, admission, and kernel completion."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Callable, Iterator, Sequence
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from aci_protocol import Final, SessionStatus
from curie_api.config import get_settings
from curie_worker import workspace as workspace_module
from curie_worker.approvals import PublicationLineage
from curie_worker.binding import MAX_TURNS_ENV, FactoryContext, FactoryWake
from curie_worker.sandbox import SuspendedThreadError
from curie_worker.workitem_dispatch import WorkItemDispatchClient
from curie_worker.workspace import (
    WorkspaceClaimCoordinator,
    WorkspaceCredential,
    WorkspacePreparationError,
    WorkspacePreparer,
)
from fastapi.testclient import TestClient
from sqlalchemy import DateTime, literal
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql.visitors import replacement_traverse

from apps.api.tests.conftest import (
    _disposable_db as _api_disposable_db,
)
from apps.api.tests.conftest import (
    auth_headers as _api_auth_headers,
)
from apps.api.tests.conftest import (
    clean_db as _api_clean_db,
)
from apps.api.tests.conftest import (
    identity_fence_backend as _api_identity_fence_backend,
)
from apps.api.tests.conftest import (
    identity_fence_key as _api_identity_fence_key,
)
from apps.api.tests.conftest import (
    migrated as _api_migrated,
)
from apps.api.tests.test_workitem_dispatch import (
    INTERNAL_PREFIX,
    WORKER_HEADERS,
    WORKER_TOKEN,
    _facts,
    _facts_json,
    _request_row,
    _start_failure_detail,
)
from apps.api.tests.test_workitem_dispatch import (
    dispatch_client as _api_dispatch_client,
)
from apps.worker.tests.kernel.test_work_item_workspace import (
    DEPLOYMENT_ID,
    ISSUE_URL,
    WORK_ITEM_REPO,
    _Binding,
    _DeferRecordingWorkItems,
    _NoExistingPublication,
    _turn,
)
from apps.worker.tests.queue_fixtures import factory_conversation
from apps.worker.tests.test_workspace import _FakeCommands, _limits, _StreamingObjectStore

_disposable_db = _api_disposable_db
migrated = _api_migrated
clean_db = _api_clean_db
auth_headers = _api_auth_headers
identity_fence_key = _api_identity_fence_key
identity_fence_backend = _api_identity_fence_backend
dispatch_client = _api_dispatch_client

CLONE_URL = f"https://github.com/{WORK_ITEM_REPO}.git"
QUOTA = "acme-sandbox-quota"
HARD = {
    "pods": "2",
    "requests.cpu": "500m",
    "requests.memory": "512Mi",
    "limits.cpu": "2",
    "limits.memory": "2Gi",
}


class SqlClock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def before_execute(
        self,
        _connection: Connection,
        statement: ClauseElement,
        multiparams: Any,
        params: Any,
        _execution_options: Any,
    ) -> tuple[ClauseElement, Any, Any]:
        def replace_clock(element: ClauseElement) -> ClauseElement | None:
            if (
                isinstance(element, FunctionElement)
                and getattr(element, "name", None) == "clock_timestamp"
            ):
                return literal(self.now, type_=DateTime(timezone=True))
            return None

        return replacement_traverse(statement, {}, replace_clock), multiparams, params


class ApiBinding(_Binding):
    def __init__(self, agent_id: uuid.UUID) -> None:
        self.agent_id = agent_id

    def _deployment(self) -> Any:
        deployment = super()._deployment()
        deployment.agent_id = self.agent_id
        return deployment

    async def resolve_factory(self, context: FactoryContext) -> object:
        assert context.agent_id == self.agent_id
        return self._deployment()


class WorkerHttp:
    def __init__(self, client: TestClient) -> None:
        self.deferrals: list[tuple[dict[str, Any], dict[str, Any]]] = []

        def send(request: httpx.Request) -> httpx.Response:
            response = client.request(
                request.method,
                request.url.raw_path.decode("ascii"),
                headers=dict(request.headers),
                content=request.content,
            )
            if request.url.path.endswith("/defer"):
                assert response.status_code == 200, response.text
                self.deferrals.append((json.loads(request.content), response.json()))
            return httpx.Response(
                response.status_code, headers=response.headers, content=response.content
            )

        self.http = httpx.AsyncClient(transport=httpx.MockTransport(send))
        self.client = WorkItemDispatchClient(
            api_base_url="http://api.example.test", worker_token=WORKER_TOKEN, client=self.http
        )


def admitted(
    client: TestClient, auth_headers: dict[str, str]
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    created = client.post(
        "/agents",
        headers=auth_headers,
        json={
            "name": f"acme-bot-{uuid.uuid4().hex[:8]}",
            "repo_full_name": WORK_ITEM_REPO,
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
        },
    )
    assert created.status_code == 201, created.text
    agent_id = uuid.UUID(created.json()["id"])
    facts = _facts(
        agent_id,
        issue_number=5155,
        repository_path=WORK_ITEM_REPO,
        reply_conversation_id="1700000000.000001",
    )
    response = client.post(
        f"{INTERNAL_PREFIX}/admissions", headers=WORKER_HEADERS, json=_facts_json(facts)
    )
    assert response.status_code == 200, response.text
    return agent_id, facts.request_id, uuid.UUID(response.json()["work_item"]["id"])


async def request_state(request_id: uuid.UUID) -> tuple[Any, str | None]:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with AsyncSession(engine) as session:
            return (
                await _request_row(session, request_id),
                await _start_failure_detail(session, request_id),
            )
    finally:
        await engine.dispose()


def admission_env() -> dict[str, str]:
    return {
        "CURIE_SANDBOX_ADMISSION": json.dumps(
            {"quota_name": QUOTA, "priority_class": "acme-sandbox", "ceiling": 2, "hard": HARD}
        )
    }


class WorkspaceClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.archive = False

    def monotonic(self) -> float:
        value = self.value
        if self.archive:
            self.value += 31.0
        return value


class WorkspaceCredentials:
    def __init__(self, *, fault: str | None, clock: WorkspaceClock) -> None:
        self.fault = fault
        self.clock = clock
        self.redemptions = 0

    def select(
        self, _deployment_id: uuid.UUID, _thread_key: str, _author: str, repo: str | None
    ) -> str | None:
        return repo

    def redeem(self, _deployment_id: uuid.UUID, _thread_key: str) -> WorkspaceCredential:
        self.redemptions += 1
        if self.fault == "auth":
            raise WorkspacePreparationError(
                "credential-redemption", "repository authentication refused"
            )
        if self.fault == "total-before-clone":
            self.clock.value = 151.0
        return WorkspaceCredential(
            repo_full_name=WORK_ITEM_REPO,
            clone_url=CLONE_URL,
            authorization_header="Basic ZXhhbXBsZTpleGFtcGxl",
            origin="https://github.com",
            header_form="authorization_basic",
            base_commit="b" * 40 if self.fault in {"total-before-pin", "badref"} else None,
        )


class WorkspaceGit(_FakeCommands):
    def __init__(self, *, fault: str | None, clock: WorkspaceClock) -> None:
        super().__init__(clone_url=CLONE_URL)
        self.fault = fault
        self.clock = clock
        self.after_clone: Callable[[], None] | None = None

    def run(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        timeout_seconds: float,
    ) -> Any:
        if "clone" in argv and self.fault == "clone":
            self.events.append("clone")
            raise TimeoutError("the git process exceeded its deadline")
        if "fetch" in argv and self.fault == "badref":
            self.events.append("fetch-exact")
            raise WorkspacePreparationError("git", "the requested ref does not exist")
        result = super().run(argv, cwd=cwd, env=env, timeout_seconds=timeout_seconds)
        if "clone" in argv:
            if self.fault in {"total", "total-before-pin"}:
                self.clock.value = 151.0
            if self.after_clone is not None:
                self.after_clone()
        if "rev-parse" in argv and self.fault == "archive":
            self.clock.archive = True
        return result


class WorkspaceObjects(_StreamingObjectStore):
    def __init__(self, *, fault: str | None) -> None:
        super().__init__()
        self.fault = fault
        self.archive_puts = 0

    def put_stream(self, key: str, chunks: Any) -> None:
        if not key.startswith("_ownership/"):
            self.archive_puts += 1
            if self.fault == "upload":
                raise TimeoutError("the object store exceeded its deadline")
            if self.fault == "generic-timeout-text":
                raise WorkspacePreparationError("upload", "exceeded the 30s budget")
        super().put_stream(key, chunks)


class CountingWorkspacePreparer(WorkspacePreparer):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.archives = 0

    def archive_checkout(self, checkout: Path, *, archive_path: Path) -> Iterator[bytes]:
        self.archives += 1
        yield from super().archive_checkout(checkout, archive_path=archive_path)


class WorkspaceServices:
    def __init__(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, fault: str | None = None
    ) -> None:
        self.clock = WorkspaceClock()
        self.credentials = WorkspaceCredentials(fault=fault, clock=self.clock)
        self.git = WorkspaceGit(fault=fault, clock=self.clock)
        self.objects = WorkspaceObjects(fault=fault)
        limits = _limits(workspace_module)
        if fault == "clone-slot":
            limits = replace(
                limits,
                clone_timeout_seconds=0.02,
                archive_timeout_seconds=0.02,
                upload_timeout_seconds=0.02,
                total_timeout_seconds=0.1,
                max_concurrent_clones=1,
            )
        if fault == "oversize":
            limits = replace(limits, max_archive_bytes=1)
        if fault in {"archive", "total-before-clone", "total-before-pin"}:
            monkeypatch.setattr(
                workspace_module,
                "time",
                SimpleNamespace(monotonic=self.clock.monotonic, time=time.time),
            )
        self.preparer = CountingWorkspacePreparer(
            credentials=self.credentials,
            commands=self.git,
            objects=self.objects,
            scratch_root=tmp_path / "workspace",
            limits=limits,
            clock=lambda: self.clock.value,
        )

    def coordinator(self, substrate: Any) -> WorkspaceClaimCoordinator:
        return WorkspaceClaimCoordinator(
            preparer=self.preparer, substrate=substrate, suspended_error=SuspendedThreadError
        )

    def operations(self) -> tuple[int, int, int]:
        return self.git.events.count("clone"), self.preparer.archives, self.objects.archive_puts


async def deliver(harness: Any, request_id: uuid.UUID, generation: int = 1) -> None:
    await harness.kernel.process_event(
        _turn(f"work-item-{request_id}-execute-{generation}", f"Resolve {ISSUE_URL}")
    )


def assert_timeout_defer(work_items: _DeferRecordingWorkItems, stage: str) -> None:
    assert len(work_items.defers) == 1, work_items.calls
    _, deferred = work_items.defers[0]
    assert deferred["capacity"] is False
    assert deferred["reason"] == "workspace_stage_timeout"
    assert not str(deferred["reason"]).startswith("not_started:")
    detail = deferred["start_failure_detail"]
    assert isinstance(detail, str)
    assert detail.startswith(f"workspace {stage} failed: exceeded the ")
    assert len(detail) <= 300
    assert "start" not in work_items.calls
    assert work_items.finishes == []


@pytest.mark.parametrize("stage", ["clone-slot", "clone", "archive", "upload", "total"])
def test_workspace_timeout_is_uncounted_wait(
    make_harness: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str
) -> None:
    services = WorkspaceServices(tmp_path, monkeypatch, fault=stage)

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=services.coordinator, max_attempts=1
        ) as harness:
            work_items = _DeferRecordingWorkItems()
            harness.kernel._work_items = work_items
            slot = (
                services.preparer.limiter.acquire("occupied-slot")
                if stage == "clone-slot"
                else None
            )
            try:
                if slot is not None:
                    slot.__enter__()
                await deliver(harness, uuid.uuid4())
                assert_timeout_defer(work_items, stage)
                assert harness.runner.opened == []
                assert harness.fake_k8s.claims == {}
            finally:
                if slot is not None:
                    slot.__exit__(None, None, None)

    asyncio.run(exercise())


@pytest.mark.parametrize("exhaustion", ["total-before-clone", "total-before-pin"])
@pytest.mark.parametrize("checkout", ["default", "lineage", "lineage-base"])
def test_total_deadline_inside_clone_keeps_its_structural_timeout(
    make_harness: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    exhaustion: str,
    checkout: str,
) -> None:
    services = WorkspaceServices(tmp_path, monkeypatch, fault=exhaustion)

    class Lineage(_NoExistingPublication):
        async def get_publication_lineage(self, *_args: object) -> PublicationLineage | None:
            if checkout == "default":
                return None
            return PublicationLineage(
                id=uuid.uuid4(), deployment_id=DEPLOYMENT_ID, conversation_id="example-thread",
                repo_full_name=WORK_ITEM_REPO, base_sha="a" * 40, branch="curie/example",
                pr_number=None, pr_url=None,
                head_sha="a" * 40 if checkout == "lineage" else None,
                state="open", version=1, latest_revision=1, has_pending_revision=False,
                has_pending_outcome=False,
                visible_outcome_revision=1 if checkout == "lineage-base" else 0,
            )

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=services.coordinator,
            publication_creator=Lineage(), max_attempts=1,
        ) as harness:
            work_items = _DeferRecordingWorkItems()
            harness.kernel._work_items = work_items
            await deliver(harness, uuid.uuid4())
            assert_timeout_defer(work_items, "total")
            assert services.operations()[1:] == (0, 0)
            assert harness.runner.opened == []

    asyncio.run(exercise())


def test_identical_timeout_text_without_timeout_type_counts_as_start_failure(
    make_harness: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    services = WorkspaceServices(tmp_path, monkeypatch, fault="generic-timeout-text")

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=services.coordinator, max_attempts=1
        ) as harness:
            work_items = _DeferRecordingWorkItems()
            harness.kernel._work_items = work_items
            await deliver(harness, uuid.uuid4())
            assert len(work_items.defers) == 1
            _, deferred = work_items.defers[0]
            assert deferred["reason"] == "not_started:classified_failure"
            assert deferred["capacity"] is False
            assert deferred["start_failure_detail"] == (
                "workspace upload failed: exceeded the 30s budget"
            )
            assert harness.runner.opened == []

    asyncio.run(exercise())


def test_capacity_preflight_does_no_preparation_and_later_delivery_starts(
    make_harness: Any,
    dispatch_client: TestClient,
    auth_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    services = WorkspaceServices(tmp_path, monkeypatch)
    agent_id, request_id, _ = admitted(dispatch_client, auth_headers)
    wire = WorkerHttp(dispatch_client)

    async def exercise() -> None:
        async with wire.http, make_harness(
            binding=ApiBinding(agent_id),
            workspace_factory=services.coordinator,
            substrate_env=admission_env(),
            publication_creator=_NoExistingPublication(),
            max_attempts=1,
        ) as harness:
            harness.kernel._work_items = wire.client
            initial, _ = await request_state(request_id)
            first = await asyncio.to_thread(harness.substrate.claim, "occupied-one")
            await asyncio.to_thread(harness.substrate.claim, "occupied-two")
            before = harness.substrate._affinity.admission_entries("test-ns", QUOTA)
            with caplog.at_level(logging.WARNING):
                await deliver(harness, request_id)
            assert services.operations() == (0, 0, 0)
            assert services.credentials.redemptions == 0
            assert harness.runner.opened == []
            assert len(harness.fake_k8s.claims) == 2
            assert len(wire.deferrals) == 1
            sent, returned = wire.deferrals[0]
            assert sent["capacity"] is True
            assert sent["reason"] == "capacity"
            assert returned["terminal_cause"] is None
            waiting, _ = await request_state(request_id)
            assert waiting.status == "waiting"
            assert waiting.capacity_deferrals == 1
            assert (
                waiting.start_deferrals == waiting.execution_attempts == waiting.runtime_epoch == 0
            )
            assert waiting.wait_deadline == initial.wait_deadline
            assert waiting.started_at is waiting.execution_deadline is None
            assert harness.substrate._affinity.admission_entries("test-ns", QUOTA) == before
            assert any(
                record.getMessage().startswith("sandbox admission refused")
                and getattr(record, "evidence_type", None) == "admission"
                for record in caplog.records
            )
            await asyncio.to_thread(harness.substrate.release, first.thread_key)
            harness.runner.default_script = [Final(text="done", status=SessionStatus.DONE)]
            await deliver(harness, request_id, waiting.dispatch_generation)
            assert services.operations() == (1, 1, 1)
            assert len(harness.runner.opened) == 2
            assert len(wire.deferrals) == 1
            started, _ = await request_state(request_id)
            assert started.status == "failed"
            assert started.terminal_cause == "early_stop"
            assert started.execution_attempts == started.runtime_epoch == 1
            assert started.started_at is not None
            assert started.capacity_deferrals == 1
            assert started.start_deferrals == 0
            assert started.wait_deadline == initial.wait_deadline

    asyncio.run(exercise())


def test_live_quota_refusal_precedes_workspace_operations(
    make_harness: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    services = WorkspaceServices(tmp_path, monkeypatch)

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=services.coordinator,
            substrate_env=admission_env(), max_attempts=1,
        ) as harness:
            work_items = _DeferRecordingWorkItems()
            harness.kernel._work_items = work_items
            read_quota = harness.fake_k8s.read_quota

            def saturated_quota(name: str, *, request_timeout_seconds: float) -> Any:
                observed = read_quota(name, request_timeout_seconds=request_timeout_seconds)
                return replace(observed, status_used=HARD)

            monkeypatch.setattr(harness.fake_k8s, "read_quota", saturated_quota)
            with caplog.at_level(logging.WARNING):
                await deliver(harness, uuid.uuid4())
            assert services.operations() == (0, 0, 0)
            assert services.credentials.redemptions == 0
            assert harness.fake_k8s.claims == {}
            assert harness.substrate._affinity.admission_entries("test-ns", QUOTA) == {}
            assert work_items.defers[0][1]["capacity"] is True
            assert any(
                record.getMessage().startswith("sandbox admission refused")
                and getattr(record, "evidence_type", None) == "admission"
                for record in caplog.records
            )

    asyncio.run(exercise())


def test_existing_route_is_adopted_when_fresh_workspace_capacity_is_full(
    make_harness: Any,
    dispatch_client: TestClient,
    auth_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    services = WorkspaceServices(tmp_path, monkeypatch)
    agent_id, request_id, _ = admitted(dispatch_client, auth_headers)
    wire = WorkerHttp(dispatch_client)
    binding = ApiBinding(agent_id)

    async def exercise() -> None:
        async with wire.http, make_harness(
            binding=binding,
            workspace_factory=services.coordinator,
            substrate_env=admission_env(),
            publication_creator=_NoExistingPublication(),
            max_attempts=1,
        ) as harness:
            harness.kernel._work_items = wire.client
            wake = _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            conversation = factory_conversation(
                FactoryWake(
                    kind="slack", address=wake.reply_handle.channel, adapter="default",
                    conversation_id=wake.conversation_id,
                )
            )
            boot_env = binding.boot_env(None, conversation)
            boot_env[MAX_TURNS_ENV] = str(harness.config.work_item_max_turns)
            existing = await asyncio.to_thread(
                harness.substrate.claim,
                conversation,
                env=boot_env,
                caller_run=str(request_id),
                workspace_repo=WORK_ITEM_REPO,
                workspace_materialized_head="a" * 40,
                publication_visible_outcome_revision=0,
            )
            await asyncio.to_thread(harness.substrate.claim, "occupied-other")
            harness.runner.default_script = [Final(text="done", status=SessionStatus.DONE)]
            await deliver(harness, request_id)
            assert services.operations() == (0, 0, 0)
            assert wire.deferrals == []
            assert len(harness.runner.opened) == 2
            row, _ = await request_state(request_id)
            assert row.status == "failed"
            assert row.terminal_cause == "early_stop"
            assert row.runtime_epoch == row.execution_attempts == 1
            assert row.capacity_deferrals == row.start_deferrals == 0
            assert row.runtime_claim_name == existing.claim_name
            assert harness.fake_k8s.deleted_claims == [existing.claim_name]

    asyncio.run(exercise())


def test_capacity_lost_after_preflight_is_still_refused_at_claim(
    make_harness: Any,
    dispatch_client: TestClient,
    auth_headers: dict[str, str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    services = WorkspaceServices(tmp_path, monkeypatch)
    agent_id, request_id, _ = admitted(dispatch_client, auth_headers)
    wire = WorkerHttp(dispatch_client)

    async def exercise() -> None:
        async with wire.http, make_harness(
            binding=ApiBinding(agent_id), workspace_factory=services.coordinator,
            substrate_env=admission_env(), max_attempts=1,
        ) as harness:
            harness.kernel._work_items = wire.client
            initial, _ = await request_state(request_id)
            harness.substrate.claim("occupied-one")
            services.git.after_clone = lambda: harness.substrate.claim("capacity-winner")
            await deliver(harness, request_id)
            assert services.operations() == (1, 1, 1)
            assert len(harness.fake_k8s.claims) == 2
            assert len(wire.deferrals) == 1
            sent, returned = wire.deferrals[0]
            assert sent["capacity"] is True
            assert sent["reason"] == "capacity"
            assert returned["terminal_cause"] is None
            waiting, _ = await request_state(request_id)
            assert waiting.status == "waiting"
            assert waiting.capacity_deferrals == 1
            assert (
                waiting.start_deferrals == waiting.execution_attempts == waiting.runtime_epoch == 0
            )
            assert waiting.wait_deadline == initial.wait_deadline
            assert waiting.started_at is waiting.execution_deadline is None
            assert harness.runner.opened == []
            assert services.objects.objects == {}

    asyncio.run(exercise())
