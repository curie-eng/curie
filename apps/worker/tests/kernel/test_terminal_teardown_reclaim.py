"""Teardown re-claims under the thread lock before it stops anything (#5377).

Plan R2: a termination claim on a terminal row leaves its runtime epoch alone,
so a claimant that stalled could otherwise record, interrupt or delete after a
peer finished the teardown and the owner resumed on a fresh runtime. Both
kernel teardown paths (the terminate wake and the owner's heartbeat stop)
therefore re-claim while holding the thread lock, immediately before the
interrupt and ``terminate_thread``; a refusal stops nothing, and the record
names the re-claim's epoch and request version.

Boundary: the real kernel, the real Valkey ``ThreadLock``, the real
``SandboxSubstrate`` over the suite's fake Kubernetes ``SandboxClient`` (per
apps/worker/CLAUDE.md), the real fake runner for interrupts, and the real
``WorkItemDispatchClient`` over httpx. Only the API is a double, as in
test_work_item_settling.py: ``TeardownApi`` serves the termination routes'
contract (owner lease, epoch, request-version compare and set). The API's own
refusals are proved against Postgres by the API suite.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from curie_worker.binding import FactoryWake
from curie_worker.kernel import routing
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemDispatchClient,
    WorkItemRun,
)

from apps.worker.tests.queue_fixtures import factory_context

from .test_work_item_workspace import (
    AGENT_ID,
    CHANNEL,
    _Binding,
    _NoExistingPublication,
    _turn,
    _Workspace,
)

OWNER = "test-consumer"  # the harness kernel's consumer name
PEER = "worker-b"
_TERMINAL = ("completed", "failed", "expired")


@dataclass
class _Row:
    status: str
    runtime_epoch: int
    version: int
    teardown_pending: bool
    claim_name: str | None
    sandbox_name: str | None
    runtime_owner: str | None = None
    lease_live: bool = False
    observation: str | None = None


@dataclass
class TeardownApi:
    """The worker-token termination routes, as the #5377 contract defines them.

    Claim: a ``cancellation_requested`` row, or a terminal row with teardown
    pending, is claimable when no other owner holds a live lease; the claim
    renews the lease and bumps the version, and bumps the epoch only for
    ``cancellation_requested``. Record: the epoch and the expected request
    version must both be current and the observation nonblank.
    """

    request_id: uuid.UUID
    row: _Row
    redis: Any
    lock_key: str
    runner: Any
    claims: list[dict[str, Any]] = field(default_factory=list)
    records: list[dict[str, Any]] = field(default_factory=list)
    read_gate: asyncio.Event | None = None
    read_entered: asyncio.Event = field(default_factory=asyncio.Event)

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        base = f"/v1/internal/work-items/requests/{self.request_id}"
        path = request.url.path
        if request.method == "GET" and path == base:
            self.read_entered.set()
            if self.read_gate is not None:
                await self.read_gate.wait()
            return httpx.Response(
                200,
                json={
                    "status": self.row.status,
                    "runtime_epoch": self.row.runtime_epoch,
                    "runtime_claim_name": self.row.claim_name,
                    "runtime_sandbox_name": self.row.sandbox_name,
                },
            )
        body = json.loads(request.content)
        if path == f"{base}/termination/claim":
            return await self._claim(body["owner"])
        if path == f"{base}/termination":
            return self._record(body)
        if path == f"{base}/start":
            return httpx.Response(
                200,
                json={
                    "runtime_epoch": self.row.runtime_epoch,
                    "execution_deadline": (datetime.now(UTC) + timedelta(hours=1)).isoformat(),
                    "remaining_s": 3600,
                    "heartbeat_interval_s": 3600,
                },
            )
        raise AssertionError(f"unexpected API request {request.method} {path}")

    async def _claim(self, owner: str) -> httpx.Response:
        row = self.row
        seen = {
            "owner": owner,
            "lock_held": bool(await self.redis.exists(self.lock_key)),
            "interrupts_before": self.runner.interrupts,
        }
        eligible = row.status == "cancellation_requested" or (
            row.status in _TERMINAL and row.teardown_pending
        )
        if not eligible:
            self.claims.append({**seen, "outcome": "not_running"})
            return httpx.Response(409, json={"detail": {"code": "not_running"}})
        if row.lease_live and row.runtime_owner not in (None, owner):
            self.claims.append({**seen, "outcome": "duplicate"})
            return httpx.Response(409, json={"detail": {"code": "duplicate"}})
        row.runtime_owner = owner
        row.lease_live = True
        row.version += 1
        if row.status == "cancellation_requested":
            row.runtime_epoch += 1
        self.claims.append({**seen, "outcome": "claimed", "version": row.version})
        return httpx.Response(
            200, json={"runtime_epoch": row.runtime_epoch, "request_version": row.version}
        )

    def _record(self, body: dict[str, Any]) -> httpx.Response:
        row = self.row
        if body["runtime_epoch"] != row.runtime_epoch:
            return httpx.Response(409, json={"detail": {"code": "stale_owner"}})
        if body["expected_request_version"] != row.version:
            return httpx.Response(409, json={"detail": {"code": "stale_version"}})
        if not body["observation"].strip():
            return httpx.Response(
                409, json={"detail": {"code": "termination_observation_required"}}
            )
        if row.status == "cancellation_requested":
            row.status = "cancelled"
        row.teardown_pending = False
        row.runtime_owner = None
        row.lease_live = False
        row.version += 1
        row.observation = body["observation"]
        self.records.append(body)
        return httpx.Response(200, json={})


def _harness(make_harness: Any) -> Any:
    return make_harness(
        binding=_Binding(),
        workspace_factory=_Workspace,
        publication_creator=_NoExistingPublication(),
    )


def _client(http: httpx.AsyncClient) -> WorkItemDispatchClient:
    return WorkItemDispatchClient(
        api_base_url="http://api.example", worker_token="example-worker-token", client=http
    )


async def _claim_thread(h: Any, thread_key: str) -> Any:
    """A runtime on the thread, claimed under its lock as a delivery claims it."""

    async with h.kernel._lock.hold(h.config.lock_key(thread_key)):
        return await asyncio.to_thread(h.substrate.claim, thread_key)


def test_delayed_duplicate_teardown_never_touches_a_resumed_successor(make_harness) -> None:
    """Claimant A stalls; B tears down and records; a successor runs; A is refused."""

    async def go() -> None:
        async with _harness(make_harness) as h:
            request_id = uuid.uuid4()
            terminate = _turn(f"work-item-{request_id}-terminate", "terminate")
            thread_key = routing._thread_key_for(terminate)
            old = await _claim_thread(h, thread_key)
            # A normally finished personal request whose generation was fenced:
            # terminal, teardown pending, so no claim moves its epoch.
            api = TeardownApi(
                request_id,
                _Row(
                    status="completed",
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=True,
                    claim_name=old.claim_name,
                    sandbox_name=old.sandbox_name,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            api.read_gate = asyncio.Event()
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                # A claims, then stalls reading the request's stored names.
                stalled = asyncio.create_task(h.kernel.process_event(terminate))
                await asyncio.wait_for(api.read_entered.wait(), timeout=5)
                assert [c["outcome"] for c in api.claims] == ["claimed"]

                # A's lease lapses on the API's clock; B takes the teardown over,
                # under the thread lock, as B's kernel would.
                api.row.lease_live = False
                peer = _client(http)
                taken = await peer.claim_termination(request_id, owner=PEER)
                assert taken.runtime_epoch == 3
                async with h.kernel._lock.hold(h.config.lock_key(thread_key)):
                    gone = await asyncio.to_thread(
                        h.substrate.terminate_thread,
                        thread_key,
                        claim_name=old.claim_name,
                        sandbox_name=old.sandbox_name,
                        observer=PEER,
                    )
                assert gone is not None
                await peer.record_termination(
                    request_id,
                    runtime_epoch=taken.runtime_epoch,
                    expected_request_version=taken.request_version,
                    observation=gone.render(),
                )
                assert api.row.teardown_pending is False

                # The owner revalidated and resumed: a fresh runtime now owns the thread.
                successor = await _claim_thread(h, thread_key)
                assert successor.claim_name != old.claim_name
                interrupts = h.runner.interrupts

                api.read_gate.set()
                await asyncio.wait_for(stalled, timeout=10)

            # A's re-claim, under the thread lock, was refused, so A stopped nothing.
            assert [(c["owner"], c["outcome"]) for c in api.claims] == [
                (OWNER, "claimed"),
                (PEER, "claimed"),
                (OWNER, "not_running"),
            ]
            assert api.claims[-1]["lock_held"] is True
            assert h.runner.interrupts == interrupts
            assert successor.claim_name in h.fake_k8s.claims
            assert successor.claim_name not in h.fake_k8s.deleted_claims
            assert await asyncio.to_thread(h.substrate.lookup, thread_key) == successor
            assert [r["expected_request_version"] for r in api.records] == [taken.request_version]

    asyncio.run(go())


@pytest.mark.parametrize(
    ("status", "recorded_epoch"),
    [("cancellation_requested", 5), ("failed", 3)],
    ids=["cancellation", "terminal"],
)
def test_terminate_records_with_the_epoch_and_version_of_its_reclaim(
    make_harness, status: str, recorded_epoch: int
) -> None:
    async def go() -> None:
        async with _harness(make_harness) as h:
            request_id = uuid.uuid4()
            terminate = _turn(f"work-item-{request_id}-terminate", "terminate")
            thread_key = routing._thread_key_for(terminate)
            old = await _claim_thread(h, thread_key)
            api = TeardownApi(
                request_id,
                _Row(
                    status=status,
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=status != "cancellation_requested",
                    claim_name=old.claim_name,
                    sandbox_name=old.sandbox_name,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                await h.kernel.process_event(terminate)

            # The gating claim runs unlocked; the re-claim holds the thread lock
            # and precedes the interrupt.
            assert [(c["outcome"], c["lock_held"]) for c in api.claims] == [
                ("claimed", False),
                ("claimed", True),
            ]
            assert [c["interrupts_before"] for c in api.claims] == [0, 0]
            assert h.runner.interrupts == 1
            [record] = api.records
            assert record["runtime_epoch"] == recorded_epoch
            assert record["expected_request_version"] == api.claims[-1]["version"] == 7
            assert old.claim_name in record["observation"]
            assert old.claim_name not in h.fake_k8s.claims

    asyncio.run(go())


async def _owned_run(h: Any, api: TeardownApi, http: httpx.AsyncClient, thread_key: str) -> Any:
    handle = await _claim_thread(h, thread_key)
    api.row.claim_name = handle.claim_name
    api.row.sandbox_name = handle.sandbox_name
    wake = FactoryWake(
        kind="slack", address=CHANNEL, adapter=None, conversation_id="1700000000.000001"
    )
    run = WorkItemRun(
        client=_client(http),
        request_id=api.request_id,
        owner=OWNER,
        grant=WorkItemAcquireGrant(
            generation=1,
            work_item_id=uuid.uuid4(),
            conversation_id="slack:example",
            wait_deadline=datetime.now(UTC) + timedelta(hours=1),
            repository_path="acme-corp/widgets",
            context=factory_context(AGENT_ID, wake),
        ),
        wake=wake,
        event_id=f"work-item-{api.request_id}-execute-1",
        thread_key=thread_key,
        on_stop=h.kernel._stop_owned_work_item,
        on_stale=h.kernel._abandon_stale_work_item,
    )
    await run.start(claim_name=handle.claim_name, sandbox_name=handle.sandbox_name)
    return run, handle


def test_owner_stop_whose_reclaim_is_refused_neither_interrupts_nor_deletes(
    make_harness,
) -> None:
    """The heartbeat saw cancellation, but a peer holds the teardown lease."""

    async def go() -> None:
        async with _harness(make_harness) as h:
            thread_key = f"teardown-{uuid.uuid4().hex}"
            api = TeardownApi(
                uuid.uuid4(),
                _Row(
                    status="cancellation_requested",
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=False,
                    claim_name=None,
                    sandbox_name=None,
                    runtime_owner=PEER,
                    lease_live=True,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                run, handle = await _owned_run(h, api, http, thread_key)
                try:
                    await h.kernel._stop_owned_work_item(thread_key, run)
                finally:
                    await run.close()

            assert [(c["owner"], c["outcome"], c["lock_held"]) for c in api.claims] == [
                (OWNER, "duplicate", True)
            ]
            assert h.runner.interrupts == 0
            assert handle.claim_name in h.fake_k8s.claims
            assert api.records == []
            assert run.finished is False

    asyncio.run(go())


def test_owner_stop_records_with_the_epoch_and_version_of_its_reclaim(make_harness) -> None:
    async def go() -> None:
        async with _harness(make_harness) as h:
            thread_key = f"teardown-{uuid.uuid4().hex}"
            api = TeardownApi(
                uuid.uuid4(),
                _Row(
                    status="cancellation_requested",
                    runtime_epoch=3,
                    version=5,
                    teardown_pending=False,
                    claim_name=None,
                    sandbox_name=None,
                    runtime_owner=OWNER,
                    lease_live=True,
                ),
                h.async_redis,
                h.config.lock_key(thread_key),
                h.runner,
            )
            async with httpx.AsyncClient(transport=httpx.MockTransport(api)) as http:
                h.kernel._work_items = _client(http)
                run, handle = await _owned_run(h, api, http, thread_key)
                try:
                    await h.kernel._stop_owned_work_item(thread_key, run)
                finally:
                    await run.close()

            assert [(c["owner"], c["outcome"], c["lock_held"]) for c in api.claims] == [
                (OWNER, "claimed", True)
            ]
            assert api.claims[0]["interrupts_before"] == 0
            assert h.runner.interrupts == 1
            [record] = api.records
            assert record["runtime_epoch"] == 4
            assert record["expected_request_version"] == 6
            assert handle.claim_name not in h.fake_k8s.claims
            assert api.row.status == "cancelled"
            assert run.finished is True

    asyncio.run(go())
