"""Approval resumes retain one-shot authority across durable admission waits."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import aiohttp
import asyncpg
import pytest
from aci_protocol import Event, Final, SessionStatus
from aiohttp import web
from aiohttp.test_utils import TestServer
from alembic import command
from alembic.config import Config
from claude_agent_sdk.types import PermissionResultAllow
from curie_api.config import get_settings
from curie_runner import __main__ as runner_main
from curie_runner.config import RunnerConfig
from curie_runner.fake import FakeModelSession
from curie_runner.server import create_app as create_runner_app
from curie_worker.binding import BindingResolver
from curie_worker.capacity_wait import (
    WAIT_GENERATION_FIELD,
    CapacityWaitRefused,
)
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore
from curie_worker.sandbox import QuotaRejection
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from ..binding.test_approval_grant import _seed_approval
from ..binding.test_resolver import _seed_agent, _seed_deployment
from .test_consumer import (
    _capacity_consumer,
    _inventory_timeout_client,
    _pending_local_entry,
    _qevent,
    _wait_key,
    _wait_until,
)
from .test_tool_access import RecordingApprovals
from .test_work_item_ci_continuation import (
    _Binding,
    _ci_turn,
    _PublicationApi,
    _WorkItems,
    _Workspace,
)


@pytest.fixture(scope="module")
def approval_wait_database() -> Iterator[str]:
    base = make_url(get_settings().database_url)
    database = f"curie_test_approval_wait_{uuid.uuid4().hex}"
    previous = os.environ.get("DATABASE_URL")

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
        url = base.set(database=database).render_as_string(hide_password=False)
        os.environ["DATABASE_URL"] = url
        get_settings.cache_clear()
        config = Config()
        config.set_main_option(
            "script_location", str(Path(__file__).resolve().parents[3] / "api/alembic")
        )
        command.upgrade(config, "head")
        yield url
    finally:
        asyncio.run(admin(f'ALTER DATABASE "{database}" WITH ALLOW_CONNECTIONS true'))
        asyncio.run(admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)'))
        if previous is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = previous
        get_settings.cache_clear()


@asynccontextmanager
async def _binding(database_url: str, *, status: str = "approved") -> AsyncIterator[Any]:
    engine = create_async_engine(database_url)
    channel = f"acme-channel-{uuid.uuid4().hex}"
    try:
        agent_id = await _seed_agent(
            engine,
            channel=channel,
            name=f"acme-bot-{uuid.uuid4().hex}",
            max_usd=1.0,
            max_tokens=10000,
            approval_tools=["Bash"],
        )
        await _seed_deployment(
            engine, agent_id=agent_id, environment="prod", bundle_ref="s3://acme-bundles/v1"
        )
        approval_id = uuid.uuid4()
        await _seed_approval(
            engine,
            approval_id=approval_id,
            agent_id=agent_id,
            status=status,
            summary="Tool call awaiting approval: Bash",
            gate_kind="permission",
            granted_tool="Bash",
            granted_arguments={"command": "echo hi"},
        )
        yield engine, channel, approval_id
    finally:
        await engine.dispose()


async def _database_available(database_url: str, available: bool) -> None:
    url = make_url(database_url)
    database = url.database
    assert database is not None and database.startswith("curie_test_approval_wait_")
    connection = await asyncpg.connect(
        user=url.username,
        password=url.password,
        host=url.host,
        port=url.port,
        database="postgres",
    )
    try:
        await connection.execute(
            f'ALTER DATABASE "{database}" WITH ALLOW_CONNECTIONS {str(available).lower()}'
        )
        if not available:
            await connection.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = $1 AND pid <> pg_backend_pid()",
                database,
            )
    finally:
        await connection.close()


class _ExecutingRunner:
    def __init__(self, api: Any, plugin_dir: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.api = api
        self.plugin_dir = plugin_dir
        self.server: TestServer | None = None
        self.runner: Any = None
        self.calls: list[str] = []
        self.sessions: list[FakeModelSession] = []
        self.claim_env: dict[str, str] = {}
        self.hold_final: asyncio.Event | None = None
        self.final_ready = asyncio.Event()
        owner = self

        class ExecutingSession(FakeModelSession):
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                real_permission = kwargs["can_use_tool"]

                async def can_use_tool(name: str, arguments: Any, context: Any) -> Any:
                    decision = await real_permission(name, arguments, context)
                    if isinstance(decision, PermissionResultAllow) and name == "Bash":
                        assert arguments == {"command": "echo hi"}
                        process = await asyncio.create_subprocess_exec(
                            "bash",
                            "-c",
                            arguments["command"],
                            stdout=asyncio.subprocess.PIPE,
                        )
                        stdout, _ = await process.communicate()
                        assert process.returncode == 0 and stdout == b"hi\n"
                        owner.calls.append(stdout.decode().strip())
                    return decision

                kwargs["can_use_tool"] = can_use_tool
                super().__init__(*args, **kwargs)
                owner.sessions.append(self)

        monkeypatch.setattr(runner_main, "FakeModelSession", ExecutingSession)
        self.app = web.Application()
        self.app.router.add_route("*", "/{path:.*}", self.handle)
        self.app.on_cleanup.append(self.cleanup)

    async def handle(self, request: web.Request) -> web.StreamResponse:
        if self.server is None:
            claims = [obj for (kind, _), obj in self.api.objects.items() if kind == "sandboxclaims"]
            assert len(claims) == 1
            self.claim_env = {
                entry["name"]: entry["value"]
                for entry in claims[0]["spec"]["env"]
                if "value" in entry
            }
            config = RunnerConfig.from_env(
                {
                    **self.claim_env,
                    "CURIE_PLUGIN_DIR": str(self.plugin_dir),
                    "CURIE_SESSION_ID": "acme-approval-session",
                    "CURIE_SANDBOX_ID": "acme-approval-sandbox",
                }
            )
            self.runner = runner_main.build_runner(config, fake_model=True)
            self.server = TestServer(create_runner_app(self.runner))
            await self.server.start_server()
        transport_headers = {"host", "connection", "transfer-encoding", "content-length"}
        async with aiohttp.ClientSession(auto_decompress=False) as client:
            async with client.request(
                request.method,
                self.server.make_url(request.path_qs),
                data=await request.read(),
                headers={
                    key: value
                    for key, value in request.headers.items()
                    if key.lower() not in transport_headers
                },
            ) as response:
                downstream = web.StreamResponse(
                    status=response.status,
                    headers={
                        key: value
                        for key, value in response.headers.items()
                        if key.lower() not in transport_headers
                    },
                )
                await downstream.prepare(request)
                async for chunk in response.content:
                    if (
                        self.hold_final is not None
                        and request.path == "/v1/event"
                        and json.loads(chunk).get("type") == "final"
                    ):
                        self.final_ready.set()
                        await self.hold_final.wait()
                    await downstream.write(chunk)
                await downstream.write_eof()
                return downstream

    async def cleanup(self, _app: web.Application) -> None:
        if self.server is not None:
            await self.server.close()


@pytest.fixture
def approval_plugin(tmp_path: Path) -> Path:
    if shutil.which("bash") is None:
        raise RuntimeError("Bash is required for approval tool execution")
    plugin_dir = tmp_path / "bundle"
    (plugin_dir / ".claude-plugin").mkdir(parents=True)
    (plugin_dir / ".claude-plugin/plugin.json").write_text(json.dumps({"name": "acme-bot"}))
    return plugin_dir


async def _handle(consumer: Any, entry: tuple[str, dict[str, str]]) -> None:
    await consumer._sem.acquire()
    await consumer._handle(*entry)


async def _pending(h: Any) -> list[dict[str, Any]]:
    return await h.async_redis.xpending_range(
        h.config.stream, h.config.consumer_group, "-", "+", 10
    )


async def _wake(h: Any, consumer: Any, event_id: str) -> tuple[str, dict[str, str]]:
    await h.async_redis.zadd(consumer._waits._due, {event_id: 0})
    assert await consumer._waits.wake_due() == 1
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group,
        h.config.consumer_name,
        {h.config.stream: ">"},
        count=1,
    )
    entry = rows[0][1][0]
    pending = await _pending(h)
    assert len(pending) == 1 and pending[0]["times_delivered"] == 1
    return entry


@pytest.mark.parametrize("failure", ["sdk-timeout", "quota-timeout"])
@pytest.mark.parametrize("placeholder", ["p-1", None], ids=["placeholder", "no-placeholder"])
def test_approval_admission_wait_recovers_on_fresh_delivery(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    failure,
    placeholder,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure=failure,
        restore=True,
        failures=1,
    )

    async def go() -> None:
        async with make_harness(
            sandbox_client=client,
            substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
            slack_no_edit_streaming=True,
        ) as h:
            h.runner.default_script = [Final(text="continued", status=SessionStatus.DONE)]
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _qevent(
                "continue",
                event_id="approval-acme-resolved",
                placeholder=placeholder,
            )
            entry = await _pending_local_entry(h, event)
            assert (await _pending(h))[0]["times_delivered"] == 1
            await _handle(consumer, entry)
            record = await consumer._waits.get(event.event_id)
            assert record is not None and record.state == "waiting"
            assert await _pending(h) == []
            assert api.created == [] and h.runner.opened == []
            assert not await h.kernel._markers.is_terminal(event.event_id)
            assert "queued" in h.sink.last_text
            replacement = _capacity_consumer(h)
            wake = await _wake(h, replacement, event.event_id)
            await _handle(replacement, wake)
            final = await replacement._waits.get(event.event_id)
            assert final.state == "done" and final.deadline_ms == record.deadline_ms
            assert max(api.delivery_counts) == 1
            assert h.runner.queried == ["continue"]
            assert len(h.sink.completions) == 1 and h.sink.last_text == "continued"
            assert await _pending(h) == []
            assert not await h.async_redis.exists(f"{h.config.stream}:dead")

    asyncio.run(go())


@pytest.mark.parametrize(
    "stacked", [None, False, True], ids=["no-fault-control", "admission", "stacked-faults"]
)
def test_approved_tool_runs_once_after_real_authority_and_capacity_recovery(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    approval_wait_database,
    approval_plugin,
    stacked,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="sdk-timeout",
        restore=True,
        failures=0 if stacked is None else 1,
    )
    executing = _ExecutingRunner(api, approval_plugin, monkeypatch)

    async def go() -> None:
        async with _binding(approval_wait_database) as (engine, channel, approval_id):
            async with make_harness(
                sandbox_client=client,
                substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
                binding_factory=lambda config: BindingResolver(engine, config),
                runner_app=executing.app,
                slack_no_edit_streaming=True,
                reclaim_min_idle_ms=0,
            ) as h:
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent(
                    "Continue the approved tool",
                    event_id=f"approval-{approval_id}-resolved",
                    channel=channel,
                )
                await _handle(consumer, await _pending_local_entry(h, event))
                replacement = _capacity_consumer(h)
                if stacked is not None:
                    parked = await consumer._waits.get(event.event_id)
                    assert parked is not None and parked.state == "waiting"
                    assert await _pending(h) == [] and executing.calls == []
                    assert api.created == []
                    if stacked:
                        await _database_available(approval_wait_database, False)
                        try:
                            wake = await _wake(h, consumer, event.event_id)
                            await _handle(consumer, wake)
                            pending = await _pending(h)
                            assert len(pending) == 1 and pending[0]["times_delivered"] == 1
                            assert pending[0]["message_id"] == wake[0]
                            assert executing.calls == [] and api.created == []
                            assert not await h.kernel._markers.is_terminal(event.event_id)
                        finally:
                            await _database_available(approval_wait_database, True)
                            await engine.dispose()
                        api.quota.status.used = dict(api.quota.status.hard)
                        assert await consumer._reclaim_once() == 1
                        await asyncio.gather(*list(consumer._inflight))
                        second = await consumer._waits.get(event.event_id)
                        assert second.state == "waiting" and second.deferrals == 2
                        assert second.deadline_ms == parked.deadline_ms
                        assert await _pending(h) == [] and executing.calls == []
                        assert max(api.delivery_counts) == 2
                        api.quota.status.used = dict.fromkeys(api.quota.status.hard, "0")
                    replacement = _capacity_consumer(h)
                    await _handle(replacement, await _wake(h, replacement, event.event_id))
                    final = await replacement._waits.get(event.event_id)
                    assert final.state == "done" and final.deadline_ms == parked.deadline_ms
                assert executing.calls == ["hi"]
                assert len(executing.sessions) == 1
                assert len(executing.sessions[0].queries) == 1
                assert executing.runner._approval_gate.grant_tool is None
                assert executing.claim_env["CURIE_APPROVAL_GRANT_TOOL"] == "Bash"
                assert json.loads(executing.claim_env["CURIE_APPROVAL_GRANT_ARGUMENTS"]) == {
                    "command": "echo hi"
                }
                assert await h.kernel._markers.is_terminal(event.event_id)
                assert len(h.sink.completions) == 1 and h.sink.completions[0].outcome == "delivered"
                assert await _pending(h) == []
                assert not await h.async_redis.exists(f"{h.config.stream}:dead")
                duplicate = await _pending_local_entry(h, event)
                await _handle(replacement, duplicate)
                assert executing.calls == ["hi"] and len(h.sink.completions) == 1
                assert await _pending(h) == []
                second_frames = [
                    frame
                    async for frame in executing.runner.run_turn(
                        Event(
                            type="message",
                            text="Try the tool again",
                            user="U0EXAMPLE1",
                            ts="1",
                        )
                    )
                ]
                assert json.loads(second_frames[-1])["status"] == SessionStatus.AWAITING_APPROVAL
                assert executing.calls == ["hi"]
                assert executing.runner.status == SessionStatus.AWAITING_APPROVAL

    asyncio.run(go())


@pytest.mark.parametrize("status", ["rejected", "expired"])
@pytest.mark.parametrize("wait", [False, True], ids=["no-wait-control", "capacity-wait"])
def test_unapproved_resume_capacity_wake_never_executes_tool(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    approval_wait_database,
    approval_plugin,
    status,
    wait,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="quota-timeout",
        restore=True,
        failures=1 if wait else 0,
    )
    executing = _ExecutingRunner(api, approval_plugin, monkeypatch)

    async def go() -> None:
        async with _binding(approval_wait_database, status=status) as (
            engine,
            channel,
            approval_id,
        ):
            approvals = RecordingApprovals()
            async with make_harness(
                sandbox_client=client,
                substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
                binding_factory=lambda config: BindingResolver(engine, config),
                runner_app=executing.app,
                approvals=approvals,
                slack_no_edit_streaming=True,
            ) as h:
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent(
                    "Continue", event_id=f"approval-{approval_id}-resolved", channel=channel
                )
                await _handle(consumer, await _pending_local_entry(h, event))
                if wait:
                    record = await consumer._waits.get(event.event_id)
                    assert record is not None and record.state == "waiting"
                    assert executing.calls == []
                    await _handle(consumer, await _wake(h, consumer, event.event_id))
                assert executing.calls == []
                assert "CURIE_APPROVAL_GRANT_TOOL" not in executing.claim_env
                assert len(approvals.requests) == 1
                assert executing.runner.status == SessionStatus.AWAITING_APPROVAL
                assert await _pending(h) == []
                assert not await h.async_redis.exists(f"{h.config.stream}:dead")

    asyncio.run(go())


async def _park(h: Any, consumer: Any, event: Any) -> Any:
    entry_id, fields = await _pending_local_entry(h, event)
    leases = DeliveryLeaseStore(h.async_redis, h.config)
    lease = await leases.acquire(
        h.config.stream,
        h.config.consumer_group,
        entry_id,
        consumer=h.config.consumer_name,
    )
    try:
        record = await consumer._waits.park(entry_id, fields, event.event_id, lease)
    finally:
        await leases.release(
            h.config.stream,
            h.config.consumer_group,
            entry_id,
            owner=lease.owner,
            resume_event_id=None,
        )
    return record


@pytest.mark.parametrize("state", ["waiting", "woken", "active"])
def test_duplicate_approval_resume_during_wait_is_redundant(
    make_harness,
    state,
) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _qevent("Continue", event_id="approval-acme-duplicate-resolved")
            record = await _park(h, consumer, event)
            await consumer._send_wait_notice(record)
            assert await _pending(h) == []
            wake = None
            wake_lease = None
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            if state != "waiting":
                wake = await _wake(h, consumer, event.event_id)
            if state == "active":
                wake_lease = await leases.acquire(
                    h.config.stream,
                    h.config.consumer_group,
                    wake[0],
                    consumer=h.config.consumer_name,
                )
                record = await consumer._waits.get(event.event_id)
                assert (
                    await consumer._waits.mark_active(
                        event.event_id, record.generation, wake_lease, "acme-live-epoch"
                    )
                    == "active"
                )
            try:
                before = await consumer._waits.get(event.event_id)
                duplicate = await _pending_local_entry(h, event)
                await _handle(consumer, duplicate)
                assert not any(row["message_id"] == duplicate[0] for row in await _pending(h))
                assert await consumer._waits.get(event.event_id) == before
                assert h.runner.opened == h.sink.completions == []
                assert not await h.async_redis.exists(f"{h.config.stream}:dead")
            finally:
                if wake_lease is not None:
                    await leases.release(
                        h.config.stream,
                        h.config.consumer_group,
                        wake[0],
                        owner=wake_lease.owner,
                        resume_event_id=None,
                    )
            if state == "waiting":
                h.runner.default_script = [Final(text="continued once", status=SessionStatus.DONE)]
                await _handle(consumer, await _wake(h, consumer, event.event_id))
                assert h.runner.queried == ["Continue"]
                assert len(h.sink.completions) == 1
                assert (await consumer._waits.get(event.event_id)).state == "done"
                assert await _pending(h) == []

    asyncio.run(go())


@pytest.mark.parametrize("lost", ["lease-token", "pel-owner"])
def test_approval_park_refuses_a_stale_delivery_fence(make_harness, lost) -> None:
    async def go() -> None:
        async with make_harness() as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _qevent("Continue", event_id="approval-acme-fenced-resolved")
            entry_id, fields = await _pending_local_entry(h, event)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            lease = await leases.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            try:
                if lost == "lease-token":
                    await h.async_redis.delete(
                        h.config.delivery_lease_key(
                            h.config.stream, h.config.consumer_group, entry_id
                        )
                    )
                else:
                    await h.async_redis.xclaim(
                        h.config.stream, h.config.consumer_group, "acme-other-worker", 0, [entry_id]
                    )
                with pytest.raises(CapacityWaitRefused):
                    await consumer._waits.park(entry_id, fields, event.event_id, lease)
                assert len(await _pending(h)) == 1
                assert await consumer._waits.get(event.event_id) is None
                assert h.runner.opened == h.sink.updates == []
                assert not await h.kernel._markers.is_terminal(event.event_id)
            finally:
                await leases.release(
                    h.config.stream,
                    h.config.consumer_group,
                    entry_id,
                    owner=lease.owner,
                    resume_event_id=None,
                )

    asyncio.run(go())


@pytest.mark.parametrize("below_card", [False, True], ids=["routed-card", "in-thread-card"])
@pytest.mark.parametrize("placeholder", ["p-1", None], ids=["placeholder", "no-placeholder"])
@pytest.mark.parametrize("expire", [False, True], ids=["complete", "expire"])
def test_approval_wait_notice_keeps_its_placement_across_reparks_and_terminal_reply(
    make_harness,
    below_card,
    placeholder,
    expire,
) -> None:
    async def go() -> None:
        async with make_harness(
            slack_no_edit_streaming=True,
            capacity_wait_budget_s=1.0 if expire else 60.0,
            claim_timeout_seconds=0.05,
        ) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            approval_id = "acme-placement"
            event = _qevent(
                "Continue",
                event_id=f"approval-{approval_id}-resolved",
                placeholder=placeholder,
            )
            if below_card:
                await h.card_store.remember_reply_below_card(approval_id)
            await h.card_store.remember_notice_ref(approval_id, "acme-pending-notice")
            first = await _park(h, consumer, event)
            await consumer._send_wait_notice(first)
            queued = await consumer._waits.get(event.event_id)
            assert queued.reply_ref is not None
            assert len(h.sink.text_posts) == int(below_card)
            expected_ref = (
                h.sink.text_posts[0][1] if below_card else placeholder or "acme-pending-notice"
            )
            assert queued.reply_ref == expected_ref
            assert len(h.sink.text_posts) == int(below_card)
            assert h.sink.updates[-1][1] == expected_ref
            assert (
                h.sink.updates[-1][2]
                == "Your request is queued while sandbox admission is pending."
            )
            assert not below_card or expected_ref not in {placeholder, "acme-pending-notice"}
            if expire:
                await _wait_until(lambda: time.time() * 1000 >= first.deadline_ms)
                replacement = _capacity_consumer(h)
                await _handle(replacement, await _wake(h, replacement, event.event_id))
                expired = await replacement._waits.get(event.event_id)
                assert expired.state == "expired" and expired.deadline_ms == first.deadline_ms
                assert h.runner.opened == []
                assert len(h.sink.completions) == 1
                assert h.sink.updates[-1][1] == expected_ref
                text = h.sink.updates[-1][2].lower()
                assert "approval decision" in text and "expired" in text
                assert "send it again" not in text and "send the request again" not in text
            else:
                h.fake_k8s.quota_rejection = QuotaRejection(
                    quota_name="acme-sandbox-quota",
                    requested={"pods": "1"},
                    used={"pods": "2"},
                    hard={"pods": "2"},
                )
                await _handle(consumer, await _wake(h, consumer, event.event_id))
                second = await consumer._waits.get(event.event_id)
                assert second.state == "waiting" and second.deferrals == 2
                assert second.deadline_ms == first.deadline_ms and second.reply_ref == expected_ref
                assert h.runner.opened == h.sink.completions == []
                h.fake_k8s.quota_rejection = None
                h.runner.default_script = [Final(text="continued", status=SessionStatus.DONE)]
                replacement = _capacity_consumer(h)
                await _handle(replacement, await _wake(h, replacement, event.event_id))
                final = await replacement._waits.get(event.event_id)
                assert final.state == "done" and final.reply_ref == expected_ref
                assert final.deadline_ms == first.deadline_ms
                assert h.runner.queried == ["Continue"] and len(h.sink.completions) == 1
                assert h.sink.updates[-1] == ("C1", expected_ref, "continued")
            assert len(h.sink.text_posts) == int(below_card)
            assert await _pending(h) == []
            assert not await h.async_redis.exists(f"{h.config.stream}:dead")

    asyncio.run(go())


def test_real_postgres_outage_keeps_approval_wake_pending_until_reclaim(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    approval_wait_database,
    approval_plugin,
    caplog,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="sdk-timeout",
        restore=True,
        failures=0,
    )
    executing = _ExecutingRunner(api, approval_plugin, monkeypatch)

    async def go() -> None:
        async with _binding(approval_wait_database) as (engine, channel, approval_id):
            async with make_harness(
                sandbox_client=client,
                substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
                binding_factory=lambda config: BindingResolver(engine, config),
                runner_app=executing.app,
                slack_no_edit_streaming=True,
                reclaim_min_idle_ms=0,
            ) as h:
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent(
                    "Continue",
                    event_id=f"approval-{approval_id}-resolved",
                    channel=channel,
                )
                record = await _park(h, consumer, event)
                await consumer._send_wait_notice(record)
                await _database_available(approval_wait_database, False)
                try:
                    wake = await _wake(h, consumer, event.event_id)
                    await _handle(consumer, wake)
                    pending = await _pending(h)
                    assert len(pending) == 1 and pending[0]["message_id"] == wake[0]
                    assert pending[0]["times_delivered"] == 1
                    assert (
                        "Current invocation authority unavailable; keeping pending" in caplog.text
                    )
                    assert executing.calls == [] and api.created == []
                    assert not await h.kernel._markers.is_terminal(event.event_id)
                finally:
                    await _database_available(approval_wait_database, True)
                    await engine.dispose()
                assert await consumer._reclaim_once() == 1
                await asyncio.gather(*list(consumer._inflight))
                assert executing.calls == ["hi"]
                assert executing.runner._approval_gate.grant_tool is None
                assert max(api.delivery_counts) == 2
                final = await consumer._waits.get(event.event_id)
                assert final.state == "done" and final.deadline_ms == record.deadline_ms
                assert len(h.sink.completions) == 1
                assert await _pending(h) == []
                assert not await h.async_redis.exists(f"{h.config.stream}:dead")

    asyncio.run(go())


@pytest.mark.parametrize("failure", ["sdk-timeout", "quota-timeout"])
def test_started_factory_approval_admission_outage_keeps_driving_loop(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    failure,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure=failure,
        restore=True,
        failures=2,
    )

    async def go() -> None:
        request_id = uuid.uuid4()
        work_items = _WorkItems(running=request_id)
        async with make_harness(
            sandbox_client=client,
            substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
            binding=_Binding(),
            workspace_factory=_Workspace,
            work_items=work_items,
            publication_creator=_PublicationApi(),
            max_attempts=1,
        ) as h:
            h.runner.default_script = [Final(text="No publication", status=SessionStatus.DONE)]
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _ci_turn(request_id).model_copy(
                update={
                    "event_id": f"approval-{uuid.uuid4()}-resolved",
                    "text": "[approval resolved] approved",
                }
            )
            await _handle(consumer, await _pending_local_entry(h, event))
            assert api.failures_left == 0
            assert len(h.runner.opened) == 1
            assert len(work_items.finishes) == 1
            assert work_items.finishes[0][1]["cause"] == "no_pull_request"
            assert "defer" not in work_items.calls and "start" not in work_items.calls
            assert await consumer._waits.get(event.event_id) is None
            assert await _pending(h) == [] and max(api.delivery_counts) == 1
            assert h.sink.updates == h.sink.text_posts == []
            assert await h.kernel._markers.is_terminal(event.event_id)
            assert not await h.async_redis.exists(f"{h.config.stream}:dead")

    asyncio.run(go())


async def _read_new_entry(h: Any) -> tuple[str, dict[str, str]]:
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group,
        h.config.consumer_name,
        {h.config.stream: ">"},
        count=1,
    )
    assert len(rows) == 1 and len(rows[0][1]) == 1
    return rows[0][1][0]


async def _exhaust_pending_wake(h: Any, consumer: Any, wake_id: str) -> Any:
    for expected_count in range(2, h.config.max_delivery + 1):
        assert await consumer._reclaim_once() == 1
        await asyncio.gather(*list(consumer._inflight))
        pending = await _pending(h)
        assert len(pending) == 1 and pending[0]["message_id"] == wake_id
        assert pending[0]["times_delivered"] == expected_count
        assert await h.async_redis.xlen(h.config.dead_letter_stream_name()) == 0
    assert await consumer._reclaim_once() == 0
    assert await _pending(h) == []
    dead = await h.async_redis.xrange(h.config.dead_letter_stream_name())
    assert len(dead) == 1
    assert dead[0][1]["dl_original_id"] == wake_id
    assert dead[0][1]["dl_delivery_count"] == str(h.config.max_delivery)
    return dead


@pytest.mark.parametrize("repark", [False, True], ids=["recover", "repark-then-recover"])
def test_raw_approval_copy_recovers_dead_lettered_ungranted_wake(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    approval_wait_database,
    approval_plugin,
    repark,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="sdk-timeout",
        restore=True,
        failures=1,
    )
    executing = _ExecutingRunner(api, approval_plugin, monkeypatch)

    async def go() -> None:
        async with _binding(approval_wait_database) as (engine, channel, approval_id):
            async with make_harness(
                sandbox_client=client,
                substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
                binding_factory=lambda config: BindingResolver(engine, config),
                runner_app=executing.app,
                slack_no_edit_streaming=True,
                reclaim_min_idle_ms=0,
                max_delivery=3,
            ) as h:
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent(
                    "Continue the approved tool",
                    event_id=f"approval-{approval_id}-resolved",
                    channel=channel,
                )
                await h.card_store.remember_reply_below_card(str(approval_id))
                await _handle(consumer, await _pending_local_entry(h, event))
                parked = await consumer._waits.get(event.event_id)
                assert parked is not None and parked.state == "waiting"
                assert parked.reply_ref is not None and parked.reply_ref != "p-1"
                await _database_available(approval_wait_database, False)
                try:
                    wake = await _wake(h, consumer, event.event_id)
                    await _handle(consumer, wake)
                    pending = await _pending(h)
                    assert len(pending) == 1 and pending[0]["times_delivered"] == 1
                    assert executing.calls == [] and api.created == []
                    dead = await _exhaust_pending_wake(h, consumer, wake[0])
                finally:
                    await _database_available(approval_wait_database, True)
                    await engine.dispose()
                orphan = await consumer._waits.get(event.event_id)
                assert orphan.state == "woken" and orphan.grant_epoch is None
                assert orphan.deadline_ms == parked.deadline_ms
                assert not await h.kernel._markers.is_terminal(event.event_id)
                before_length = await h.async_redis.xlen(h.config.stream)
                replacement = _capacity_consumer(h)
                if repark:
                    api.quota.status.used = dict(api.quota.status.hard)
                raw = await _pending_local_entry(h, event)
                assert WAIT_GENERATION_FIELD not in raw[1]
                await _handle(replacement, raw)
                recovered = await replacement._waits.get(event.event_id)
                assert recovered.generation > orphan.generation
                assert recovered.fields["payload"] == parked.fields["payload"]
                assert recovered.reply_ref == parked.reply_ref
                assert recovered.first_wait_ms == parked.first_wait_ms
                assert recovered.deadline_ms == parked.deadline_ms
                assert await h.async_redis.xlen(h.config.stream) == before_length + 1
                assert await _pending(h) == []
                if repark:
                    assert recovered.state == "waiting" and executing.calls == []
                    api.quota.status.used = dict.fromkeys(api.quota.status.hard, "0")
                    await _handle(replacement, await _wake(h, replacement, event.event_id))
                final = await replacement._waits.get(event.event_id)
                assert final.state == "done" and final.deadline_ms == parked.deadline_ms
                assert final.reply_ref == parked.reply_ref
                assert executing.calls == ["hi"] and len(executing.sessions[0].queries) == 1
                assert executing.runner._approval_gate.grant_tool is None
                assert len(h.sink.completions) == 1
                assert h.sink.updates[-1][1] == parked.reply_ref
                assert await _pending(h) == []
                assert await h.async_redis.xrange(h.config.dead_letter_stream_name()) == dead
                await _handle(replacement, await _pending_local_entry(h, event))
                assert executing.calls == ["hi"] and len(h.sink.completions) == 1
                assert await _pending(h) == []
                assert await h.async_redis.xrange(h.config.dead_letter_stream_name()) == dead

    asyncio.run(go())


def test_raw_approval_copy_preserves_the_only_unread_capacity_wake(make_harness) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            consumer = _capacity_consumer(h)
            await consumer.ensure_group()
            event = _qevent("Continue", event_id="approval-acme-unread-resolved")
            parked = await _park(h, consumer, event)
            await consumer._send_wait_notice(parked)
            raw = await _pending_local_entry(h, event)
            await h.async_redis.zadd(consumer._waits._due, {event.event_id: 0})
            assert await consumer._waits.wake_due() == 1
            before = await consumer._waits.get(event.event_id)
            wake_id = await h.async_redis.hget(
                consumer._waits._record(event.event_id), "wake_entry"
            )
            assert before.state == "woken" and wake_id is not None
            pending = await _pending(h)
            assert len(pending) == 1 and pending[0]["message_id"] == raw[0]
            assert not await h.async_redis.xpending_range(
                h.config.stream, h.config.consumer_group, wake_id, wake_id, 1
            )
            before_length = await h.async_redis.xlen(h.config.stream)
            await _handle(consumer, raw)
            assert await consumer._waits.get(event.event_id) == before
            assert await h.async_redis.xlen(h.config.stream) == before_length
            assert await _pending(h) == []
            assert h.runner.opened == h.sink.completions == []
            h.runner.default_script = [Final(text="continued once", status=SessionStatus.DONE)]
            wake = await _read_new_entry(h)
            assert wake[0] == wake_id
            await _handle(consumer, wake)
            assert h.runner.queried == ["Continue"] and len(h.sink.completions) == 1
            assert (await consumer._waits.get(event.event_id)).state == "done"
            assert await _pending(h) == []
            assert not await h.async_redis.exists(h.config.dead_letter_stream_name())

    asyncio.run(go())


@pytest.mark.parametrize("grant", ["epoch-only", "unknown", "ever-granted"])
def test_raw_approval_copy_preserves_unconfirmed_orphan_until_original_deadline(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    approval_wait_database,
    approval_plugin,
    grant,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="sdk-timeout",
        restore=True,
        failures=1,
    )
    executing = _ExecutingRunner(api, approval_plugin, monkeypatch)

    async def go() -> None:
        async with _binding(approval_wait_database) as (engine, channel, approval_id):
            async with make_harness(
                sandbox_client=client,
                substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
                binding_factory=lambda config: BindingResolver(engine, config),
                runner_app=executing.app,
                slack_no_edit_streaming=True,
                capacity_wait_budget_s=1.0,
                reclaim_min_idle_ms=0,
            ) as h:
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent(
                    "Continue",
                    event_id=f"approval-{approval_id}-resolved",
                    channel=channel,
                )
                await _handle(consumer, await _pending_local_entry(h, event))
                parked = await consumer._waits.get(event.event_id)
                assert parked is not None and parked.state == "waiting"
                wake = await _wake(h, consumer, event.event_id)
                woken = await consumer._waits.get(event.event_id)
                leases = DeliveryLeaseStore(h.async_redis, h.config)
                lease = await leases.acquire(
                    h.config.stream,
                    h.config.consumer_group,
                    wake[0],
                    consumer=h.config.consumer_name,
                )
                epoch = "acme-unconfirmed-epoch"
                try:
                    assert (
                        await consumer._waits.mark_active(
                            event.event_id, woken.generation, lease, epoch
                        )
                        == "active"
                    )
                    if grant == "ever-granted":
                        assert await consumer._waits.confirm_grant(
                            event.event_id, woken.generation, lease, epoch
                        )
                        epoch = "acme-unconfirmed-retry-epoch"
                        assert (
                            await consumer._waits.mark_active(
                                event.event_id, woken.generation, lease, epoch
                            )
                            == "active"
                        )
                    if grant in {"unknown", "ever-granted"}:
                        assert await consumer._waits.mark_grant_unknown(
                            event.event_id, woken.generation, epoch
                        )
                    assert (
                        await h.async_redis.xack(h.config.stream, h.config.consumer_group, wake[0])
                        == 1
                    )
                finally:
                    await leases.release(
                        h.config.stream,
                        h.config.consumer_group,
                        wake[0],
                        owner=lease.owner,
                        resume_event_id=None,
                    )
                orphan = await consumer._waits.get(event.event_id)
                evidence = await h.async_redis.hgetall(consumer._waits._record(event.event_id))
                assert orphan.state == "active" and orphan.grant_epoch == epoch
                assert await _pending(h) == [] and executing.calls == []
                replacement = _capacity_consumer(h)
                raw = await _pending_local_entry(h, event)
                await _handle(replacement, raw)
                adopted = await replacement._waits.get(event.event_id)
                assert adopted.generation > orphan.generation and adopted.state == "active"
                assert adopted.deadline_ms == parked.deadline_ms
                assert adopted.first_wait_ms == parked.first_wait_ms
                assert adopted.fields["payload"] == parked.fields["payload"]
                assert adopted.reply_ref == parked.reply_ref
                after = await h.async_redis.hgetall(replacement._waits._record(event.event_id))
                for field in ("grant_epoch", "grant_confirmed", "grant_unknown", "ever_granted"):
                    assert after.get(field) == evidence.get(field), field
                pending = await _pending(h)
                assert len(pending) == 1 and pending[0]["message_id"] == raw[0]
                assert pending[0]["times_delivered"] == 1
                assert not await h.kernel._markers.is_terminal(event.event_id)
                assert executing.calls == [] and executing.server is None
                assert await replacement._reclaim_once() == 1
                await asyncio.gather(*list(replacement._inflight))
                assert (
                    await replacement._waits.get(event.event_id)
                ).generation == adopted.generation
                pending = await _pending(h)
                assert len(pending) == 1 and pending[0]["times_delivered"] == 2
                assert executing.calls == [] and executing.server is None
                await _wait_until(lambda: time.time() * 1000 >= parked.deadline_ms)
                assert await replacement._reclaim_once() == 1
                await asyncio.gather(*list(replacement._inflight))
                final = await replacement._waits.get(event.event_id)
                expected_cause = {
                    "epoch-only": "capacity_wait_expired",
                    "unknown": "grant_unknown",
                    "ever-granted": "delivery_exhausted",
                }[grant]
                assert final.state == "expired" and final.cause == expected_cause
                assert final.deadline_ms == parked.deadline_ms
                assert final.grant_epoch == epoch
                assert executing.calls == [] and executing.server is None
                assert len(h.sink.completions) == 1
                assert await h.kernel._markers.is_terminal(event.event_id)
                assert await _pending(h) == []
                assert not await h.async_redis.exists(h.config.dead_letter_stream_name())
                await _handle(replacement, await _pending_local_entry(h, event))
                assert executing.calls == [] and len(h.sink.completions) == 1

    asyncio.run(go())


def test_raw_approval_copy_adopts_confirmed_orphan_without_repeating_executed_tool(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    approval_wait_database,
    approval_plugin,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="sdk-timeout",
        restore=True,
        failures=1,
    )
    executing = _ExecutingRunner(api, approval_plugin, monkeypatch)
    executing.hold_final = asyncio.Event()

    async def go() -> None:
        async with _binding(approval_wait_database) as (engine, channel, approval_id):
            async with make_harness(
                sandbox_client=client,
                substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
                binding_factory=lambda config: BindingResolver(engine, config),
                runner_app=executing.app,
                slack_no_edit_streaming=True,
            ) as h:
                consumer = _capacity_consumer(h)
                await consumer.ensure_group()
                event = _qevent(
                    "Continue",
                    event_id=f"approval-{approval_id}-resolved",
                    channel=channel,
                )
                await _handle(consumer, await _pending_local_entry(h, event))
                parked = await consumer._waits.get(event.event_id)
                assert parked is not None and parked.state == "waiting"
                wake = await _wake(h, consumer, event.event_id)
                owner = asyncio.create_task(_handle(consumer, wake))
                try:
                    await asyncio.wait_for(executing.final_ready.wait(), timeout=5)
                    await _wait_key(h.async_redis, h.config.side_effect_key(event.event_id))
                    active = await consumer._waits.get(event.event_id)
                    assert active.state == "active" and active.grant_confirmed
                    assert active.grant_epoch is not None
                    assert executing.calls == ["hi"]
                    assert executing.runner._approval_gate.grant_tool is None
                    assert not await h.kernel._markers.is_terminal(event.event_id)
                    owner.cancel()
                    await asyncio.gather(owner, return_exceptions=True)
                    assert (
                        await h.async_redis.xack(h.config.stream, h.config.consumer_group, wake[0])
                        == 1
                    )
                    executing.hold_final.set()
                    replacement = _capacity_consumer(h)
                    await _handle(replacement, await _pending_local_entry(h, event))
                    final = await replacement._waits.get(event.event_id)
                    assert final.generation > active.generation and final.state == "done"
                    assert final.deadline_ms == parked.deadline_ms
                    assert final.grant_epoch == active.grant_epoch and final.grant_confirmed
                    assert executing.calls == ["hi"]
                    assert len(executing.sessions) == 1
                    assert len(executing.sessions[0].queries) == 1
                    assert len(h.sink.completions) == 1
                    assert h.sink.completions[0].outcome == "escalated"
                    assert await h.kernel._markers.saw_side_effect(event.event_id)
                    assert await _pending(h) == []
                    assert not await h.async_redis.exists(h.config.dead_letter_stream_name())
                finally:
                    executing.hold_final.set()
                    if not owner.done():
                        owner.cancel()
                    await asyncio.gather(owner, return_exceptions=True)

    asyncio.run(go())


class _PausedResumeClaim:
    def __init__(self, delegate: DeliveryLeaseStore, entry_id: str) -> None:
        self.delegate = delegate
        self.entry_id = entry_id
        self.entered = asyncio.Event()
        self.proceed = asyncio.Event()
        self.claimed = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.delegate, name)

    async def claim_resume(self, lease: Any, event_id: str, *, consumer: str) -> bool:
        if lease.entry_id == self.entry_id:
            self.entered.set()
            await self.proceed.wait()
        granted = await self.delegate.claim_resume(lease, event_id, consumer=consumer)
        if lease.entry_id == self.entry_id:
            self.claimed = granted
        return granted


def test_approval_copy_checks_wait_after_winning_the_resume_claim(
    make_harness,
    sync_redis,
    names,
    monkeypatch,
    approval_wait_database,
    approval_plugin,
) -> None:
    client, api, admission = _inventory_timeout_client(
        sync_redis,
        names,
        monkeypatch,
        failure="sdk-timeout",
        restore=True,
        failures=1,
    )
    executing = _ExecutingRunner(api, approval_plugin, monkeypatch)

    async def go() -> None:
        async with _binding(approval_wait_database) as (engine, channel, approval_id):
            async with make_harness(
                sandbox_client=client,
                substrate_env={"CURIE_SANDBOX_ADMISSION": admission},
                binding_factory=lambda config: BindingResolver(engine, config),
                runner_app=executing.app,
                slack_no_edit_streaming=True,
            ) as h:
                original = _capacity_consumer(h)
                await original.ensure_group()
                event = _qevent(
                    "Continue",
                    event_id=f"approval-{approval_id}-resolved",
                    channel=channel,
                )
                first = await _pending_local_entry(h, event)
                raw = await _pending_local_entry(h, event)
                paused = _PausedResumeClaim(DeliveryLeaseStore(h.async_redis, h.config), raw[0])
                duplicate = Consumer(
                    redis=h.async_redis,
                    kernel=h.kernel,
                    config=h.config,
                    leases=paused,
                )
                task = asyncio.create_task(_handle(duplicate, raw))
                try:
                    await asyncio.wait_for(paused.entered.wait(), timeout=5)
                    await _handle(original, first)
                    parked = await original._waits.get(event.event_id)
                    assert parked is not None and parked.state == "waiting"
                    assert executing.calls == []
                    await h.async_redis.zadd(original._waits._due, {event.event_id: 0})
                    assert await original._waits.wake_due() == 1
                    before = await original._waits.get(event.event_id)
                    wake_id = await h.async_redis.hget(
                        original._waits._record(event.event_id), "wake_entry"
                    )
                    paused.proceed.set()
                    await asyncio.wait_for(task, timeout=5)
                    assert paused.claimed
                    assert executing.calls == [] and executing.server is None
                    assert await duplicate._waits.get(event.event_id) == before
                    assert await _pending(h) == []
                    assert not await h.kernel._markers.is_terminal(event.event_id)
                    wake = await _read_new_entry(h)
                    assert wake[0] == wake_id
                    assert wake[1][WAIT_GENERATION_FIELD] == str(before.generation)
                    await _handle(original, wake)
                    final = await original._waits.get(event.event_id)
                    assert final.state == "done" and final.deadline_ms == parked.deadline_ms
                    assert executing.calls == ["hi"]
                    assert len(h.sink.completions) == 1
                    assert await _pending(h) == []
                    assert not await h.async_redis.exists(h.config.dead_letter_stream_name())
                finally:
                    paused.proceed.set()
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    asyncio.run(go())
