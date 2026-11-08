"""ADR 0274 worker provenance and pause settlement against real SQL and Valkey."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from aci_protocol import Final, SessionStatus
from sqlalchemy import text

from .test_cron_loop import _entries, _later_pass, _seed, _trigger
from .test_hook_runs import _capture_fire_metrics
from .test_targetless_cron import _resolver_factory, _seed_deployments, _targetless


@pytest.mark.parametrize("source", ["schedule", "manual", "webhook"])
def test_stored_provenance_and_close_are_per_run(make_hook_run, monkeypatch, source):
    fires = _capture_fire_metrics(monkeypatch)

    async def go():
        async with make_hook_run(source=source) as run, make_hook_run() as other:
            recorder = run.recorder()
            state = await recorder.get(run.ref)
            assert state.source == source
            async with run.engine.connect() as conn:
                before = (await conn.execute(text("SELECT clock_timestamp()"))).scalar_one()
            assert await recorder.renew(run.ref, 900)
            assert await run.lease_expires_at() >= before + timedelta(seconds=900)
            assert await recorder.close(run.ref, "blocked", "hook_paused") is True
            assert await recorder.close(run.ref, "ran") is False
            assert (await run.state())[0] == "blocked"
            assert await run.reason() == "hook_paused"
            assert await other.state() == (None, None)
            assert fires == [
                {
                    "service.name": "curie-worker",
                    "outcome": "blocked",
                    "trigger": "webhook" if source == "webhook" else "cron",
                }
            ]

    asyncio.run(go())


@pytest.mark.parametrize("source", ["schedule", "manual", "webhook"])
def test_targetless_pause_uses_stored_provenance(make_hook_run, make_harness, source):
    async def go():
        async with (
            make_hook_run(source=source) as run,
            _seed_deployments(run.engine, run.agent_id, with_decoy=False),
            make_harness(
                hook_runs=run.recorder(), binding_factory=_resolver_factory(run.engine)
            ) as h,
        ):
            event = _targetless(run.ref)
            async with run.engine.begin() as conn:
                await conn.execute(
                    text(
                        "INSERT INTO curie.schedule_controls (agent_id, name, paused_at) "
                        "VALUES (:agent, :name, now())"
                    ),
                    {"agent": run.agent_id, "name": run.ref.name},
                )
            await h.kernel.process_event(event)
            expected = "blocked" if source == "webhook" else "deferred"
            assert (await run.state())[0] == expected
            assert await run.reason() == "hook_paused"
            assert h.runner.opened == []
            assert h.sink.events == []
            async with run.engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.schedule_controls SET paused_at = NULL, resume_from = now() "
                        "WHERE agent_id = :agent AND name = :name"
                    ),
                    {"agent": run.agent_id, "name": run.ref.name},
                )
            await h.kernel.process_event(
                event.model_copy(update={"event_id": event.event_id + "-stale"})
            )
            assert (await run.state())[0] == expected
            assert h.runner.opened == []
            assert h.sink.events == []

    asyncio.run(go())


@pytest.mark.parametrize("source", ["schedule", "manual", "webhook"])
def test_pause_at_runner_start_uses_stored_provenance(make_hook_run, make_harness, source):
    async def go():
        async with (
            make_hook_run(source=source) as run,
            _seed_deployments(run.engine, run.agent_id, with_decoy=False),
            make_harness(
                hook_runs=run.recorder(), binding_factory=_resolver_factory(run.engine)
            ) as h,
        ):
            entered, release = asyncio.Event(), asyncio.Event()
            original = h.kernel._claim_or_resume

            async def held(*args, **kwargs):
                entered.set()
                await release.wait()
                return await original(*args, **kwargs)

            h.kernel._claim_or_resume = held
            task = asyncio.create_task(h.kernel.process_event(_targetless(run.ref)))
            try:
                await asyncio.wait_for(entered.wait(), 5)
                async with run.engine.begin() as conn:
                    await conn.execute(
                        text(
                            "INSERT INTO curie.schedule_controls (agent_id, name, paused_at) "
                            "VALUES (:agent, :name, now())"
                        ),
                        {"agent": run.agent_id, "name": run.ref.name},
                    )
            finally:
                release.set()
            await task
            assert (await run.state())[0] == ("blocked" if source == "webhook" else "deferred")
            assert await run.reason() == "hook_paused"
            assert h.runner.opened == []
            assert h.sink.events == []

    asyncio.run(go())


def test_webhook_success_is_silent_and_stale_delivery_cannot_restart(make_hook_run, make_harness):
    async def go():
        async with (
            make_hook_run(source="webhook") as run,
            _seed_deployments(run.engine, run.agent_id, with_decoy=False),
            make_harness(
                hook_runs=run.recorder(), binding_factory=_resolver_factory(run.engine)
            ) as h,
        ):
            h.runner.default_script = [Final(text="done", status=SessionStatus.DONE)]
            event = _targetless(run.ref)
            await h.kernel.process_event(event)
            await h.kernel.process_event(
                event.model_copy(update={"event_id": event.event_id + "-stale"})
            )
            assert h.runner.opened == [event.text]
            assert h.sink.events == []
            assert (await run.state())[0] == "ran"

    asyncio.run(go())


@pytest.mark.parametrize("source", ["schedule", "manual", "webhook"])
def test_scheduler_only_reopens_cron_provenance(sync_redis, names, source):
    async def go():
        async with _seed() as seed:
            run_id = await seed.add_run(seed.slot, seed.slot, outcome="deferred")
            async with seed.engine.begin() as conn:
                await conn.execute(
                    text("UPDATE curie.hook_runs SET source = :source WHERE id = :id"),
                    {"source": source, "id": run_id},
                )
            await _later_pass(
                seed, names["stream"], _trigger(seed), seed.slot + timedelta(seconds=20)
            )
            rows = await seed.runs()
            assert len(rows) == 1
            assert rows[0].outcome == ("deferred" if source == "webhook" else None)
            assert len(_entries(sync_redis, names["stream"])) == (0 if source == "webhook" else 1)

    asyncio.run(go())
