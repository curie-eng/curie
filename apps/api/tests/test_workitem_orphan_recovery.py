"""A worker restart recovers its orphaned WorkItem run end to end (#3076).

The old worker acquires and starts a request through the real API with the
real worker client, then dies. The restarted process reuses its name and holds
nothing locally. Its real orphan sweeper, running on a tiny interval, declares
the run owner-lost. The real reconciler publishes the terminate wake onto the
real runs stream, and the restarted process's real kernel consumes it: it
claims termination, reads the stored claim and sandbox names, tears them down
through the substrate, and records the observation. Only the substrate (the
cluster) is faked. The runtime lease is a few seconds and the whole recovery
must finish inside it.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
import redis
import redis.asyncio as aredis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.workitem_reconciler import WorkItemReconciler
from curie_worker.config import WorkerConfig
from curie_worker.kernel import Kernel
from curie_worker.markers import Markers
from curie_worker.runner_client import RunnerClient
from curie_worker.threadlock import ThreadLock
from curie_worker.workitem_dispatch import TerminationObservation, WorkItemDispatchClient
from curie_worker.workitem_orphans import WorkItemOrphanSweeper
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

WORKER_TOKEN = "work-item-orphan-worker-token"
REPO = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
CLAIM_NAME = "curie-thread-orphan-claim"
SANDBOX_NAME = "sbx-curie-thread-orphan-claim"
RUNTIME_TTL_S = 5


@pytest.fixture
def api(
    clean_db: None, runs_stream: str, valkey: redis.Redis, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    # The test drives the reconciler pass itself.
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    monkeypatch.setenv("CURIE_WORK_ITEM_RUNTIME_TTL_SECONDS", str(RUNTIME_TTL_S))
    get_settings.cache_clear()
    with TestClient(create_app()) as client:
        yield client
    get_settings.cache_clear()


class _Cluster:
    """Fake substrate: the old incarnation's claim and sandbox still exist."""

    def __init__(self) -> None:
        self.claims = {CLAIM_NAME}
        self.sandboxes = {SANDBOX_NAME}
        self.terminated: list[dict[str, Any]] = []

    def lookup(self, _thread_key: str) -> None:
        # Nothing is routed in the restarted process.
        return None

    def terminate_thread(
        self,
        thread_key: str,
        *,
        claim_name: str | None,
        sandbox_name: str | None,
        observer: str = "",
    ) -> TerminationObservation:
        self.terminated.append(
            {
                "thread_key": thread_key,
                "claim_name": claim_name,
                "sandbox_name": sandbox_name,
                "observer": observer,
            }
        )
        self.claims.discard(claim_name or "")
        self.sandboxes.discard(sandbox_name or "")
        return TerminationObservation(
            claims=(claim_name or "",),
            sandboxes=(sandbox_name or "",),
            observed_at=datetime.now(UTC),
            observer=observer,
        )


class _NoSink:
    async def emit(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("a terminate wake posts no reply")


def _admit(api: TestClient, auth_headers: dict[str, str]) -> uuid.UUID:
    created = api.post(
        "/agents",
        json={
            "name": f"acme-bot-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": ADDRESS},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    )
    assert created.status_code == 201, created.text
    facts = {
        "agent_id": created.json()["id"],
        "kind": "slack",
        "address": ADDRESS,
        "reply_conversation_id": "1700000000.000100",
        "repo_full_name": REPO,
        "github_repository_id": 101,
        "github_issue_number": 3076,
        "github_installation_id": 202,
        "objective": "Recover the orphaned run",
        "requester": "U0REQUEST1",
        "request_id": str(uuid.uuid4()),
    }
    admitted = api.post(
        "/v1/internal/work-items/admissions",
        json=facts,
        headers={"X-Curie-Worker-Token": WORKER_TOKEN},
    )
    assert admitted.status_code == 200, admitted.text
    return uuid.UUID(admitted.json()["request"]["id"])


def _async_valkey() -> aredis.Redis:
    s = get_settings()
    return aredis.Redis(
        host=s.valkey_host,
        port=s.valkey_port,
        password=s.valkey_password or None,
        decode_responses=True,
    )


def _kernel(name: str, cluster: _Cluster, client: WorkItemDispatchClient, valkey: Any) -> Kernel:
    s = get_settings()
    prefix = f"test:orphan:{uuid.uuid4().hex}"
    config = WorkerConfig(
        valkey_host=s.valkey_host,
        valkey_port=s.valkey_port,
        valkey_password=s.valkey_password,
        stream=s.runs_stream,
        consumer_group=f"{prefix}:group",
        consumer_name=name,
        key_prefix=prefix,
        lock_ttl_ms=5000,
        lock_acquire_timeout_s=5.0,
    )
    lock = ThreadLock(valkey, ttl_ms=5000, acquire_timeout_s=5.0, poll_interval_s=0.01)
    return Kernel(
        substrate=cluster,  # type: ignore[arg-type]
        runner=RunnerClient(total_timeout_s=config.runner_total_timeout_s),
        sink=_NoSink(),  # type: ignore[arg-type]
        lock=lock,
        pressure_lock=ThreadLock(
            valkey, ttl_ms=5000, acquire_timeout_s=0.1, poll_interval_s=0.02, owner=None
        ),
        markers=Markers(valkey, config),
        config=config,
        work_items=client,
    )


async def _published_turns(valkey: Any) -> list[QueuedTurn]:
    entries = await valkey.xrange(get_settings().runs_stream)
    return [QueuedTurn.model_validate_json(fields[STREAM_PAYLOAD_FIELD]) for _id, fields in entries]


async def _recover(app: Any, request_id: uuid.UUID, old: str) -> dict[str, Any]:
    started_at = time.monotonic()
    engine = create_async_engine(get_settings().database_url)
    valkey = _async_valkey()
    cluster = _Cluster()
    kernel: Kernel | None = None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://api"
        ) as http:
            client = WorkItemDispatchClient(
                api_base_url="http://api", worker_token=WORKER_TOKEN, client=http
            )
            # The old incarnation acquires and starts, then dies.
            await client.acquire(request_id, owner=old, generation=1)
            await client.start(
                request_id,
                owner=old,
                generation=1,
                claim_name=CLAIM_NAME,
                sandbox_name=SANDBOX_NAME,
            )

            # The restarted process: same name, a fresh kernel holding nothing.
            kernel = _kernel(old, cluster, client, valkey)
            reconciler = WorkItemReconciler(
                async_sessionmaker(engine, expire_on_commit=False), valkey, get_settings()
            )
            await reconciler._publish_terminate_wakes()
            wakes_before = await _published_turns(valkey)

            liveness_calls: list[str] = []

            async def liveness(owner: str) -> bool:
                liveness_calls.append(owner)
                return False

            sweeper = WorkItemOrphanSweeper(
                client,
                liveness,
                self_name=old,
                locally_owned=kernel.owns_work_item,
                absence_proof_s=3600.0,
                interval_s=0.05,
            )
            shutdown = asyncio.Event()
            sweeping = asyncio.create_task(sweeper.run_forever(shutdown))
            try:
                deadline = time.monotonic() + RUNTIME_TTL_S
                while time.monotonic() < deadline:
                    view = await client.get_request(request_id)
                    if view.status == "cancellation_requested":
                        break
                    await asyncio.sleep(0.02)
            finally:
                shutdown.set()
                await asyncio.wait_for(sweeping, timeout=2.0)
            declared = await client.get_request(request_id)

            # The API side of the terminate chain: publish the wake to the stream.
            await reconciler._publish_terminate_wakes()
            wakes = await _published_turns(valkey)
            # The consumer's entry: the kernel processes the queued turn.
            for wake in wakes:
                await kernel.process_event(wake)
            final = await client.get_request(request_id)
    finally:
        if kernel is not None:
            await kernel._runner.close()
        await valkey.aclose()
        await engine.dispose()
    return {
        "elapsed": time.monotonic() - started_at,
        "wakes_before": wakes_before,
        "declared": declared,
        "wakes": wakes,
        "cluster": cluster,
        "final": final,
    }


async def _db(query: str, request_id: uuid.UUID) -> list[Any]:
    engine = create_async_engine(get_settings().database_url)
    try:
        async with AsyncSession(engine) as session:
            return list((await session.execute(text(query), {"id": request_id})).all())
    finally:
        await engine.dispose()


def test_restarted_worker_recovers_its_orphaned_run(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    old = f"w-old-{uuid.uuid4().hex[:8]}"
    request_id = _admit(api, auth_headers)
    # Run on the app's own loop: its engine pool is bound there.
    portal = api.portal
    assert portal is not None
    result = portal.call(_recover, api.app, request_id, old)

    # No terminate wake before the sweep: nothing but the sweeper triggers it.
    assert result["wakes_before"] == []
    assert result["declared"].status == "cancellation_requested"
    assert [w.event_id for w in result["wakes"]] == [f"work-item-{request_id}-terminate"]

    cluster: _Cluster = result["cluster"]
    assert len(cluster.terminated) == 1
    assert cluster.terminated[0]["claim_name"] == CLAIM_NAME
    assert cluster.terminated[0]["sandbox_name"] == SANDBOX_NAME
    assert cluster.terminated[0]["observer"] == old
    assert cluster.claims == set()
    assert cluster.sandboxes == set()

    assert result["final"].status == "failed"
    rows = asyncio.run(
        _db(
            "SELECT status, terminal_cause, termination_observation "
            "FROM curie.execution_requests WHERE id = :id",
            request_id,
        )
    )
    [(status, cause, observation)] = [tuple(r) for r in rows]
    assert (status, cause) == ("failed", "owner_lost")
    assert f"claims={CLAIM_NAME}" in observation
    assert f"sandboxes={SANDBOX_NAME}" in observation
    assert observation.endswith(f"observer={old}")
    notices = asyncio.run(
        _db(
            "SELECT terminal_cause FROM curie.factory_terminal_notices "
            "WHERE execution_request_id = :id",
            request_id,
        )
    )
    assert [r[0] for r in notices] == ["owner_lost"]
    # Recovery never waited for the runtime lease to lapse.
    assert result["elapsed"] < RUNTIME_TTL_S
