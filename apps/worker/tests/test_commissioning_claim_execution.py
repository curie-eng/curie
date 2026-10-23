"""One-shot campaign dispatch through the real candidate ACL, @spec PROTECTED-COMMISSIONING-4."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.commissioning_acl import derive_commissioning_lane
from curie_protected_hooks.commissioning_worker import CampaignParked

from . import test_commissioning_worker_entry as entry_tests
from .test_commissioning_worker_entry import (
    BODY,
    GuardProbe,
    _deliveries,
    opened,
    run,
    started,
    until,
)

campaign = entry_tests.campaign
pworker_service = entry_tests.pworker_service


def pending(broker: Any, campaign: Any) -> int:
    lane = derive_commissioning_lane(campaign.permit)
    result = broker.command("XPENDING", lane.stream, lane.group)
    return int(result["pending"] if isinstance(result, dict) else result[0])


def test_same_name_replicas_execute_once_and_settle_terminal_claims(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    calls: list[str] = []

    async def execute(claim: Any) -> None:
        calls.append(claim.entry_id)
        await asyncio.sleep(0.15)

    async def scenario() -> str:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            forged = pworker_service.command("XADD", campaign.stream, "*", "payload", "bad")
            workers = [
                await opened(directory, guard=GuardProbe(campaign).guard, execute_claim=execute)
                for _ in range(2)
            ]
            tasks = [asyncio.create_task(w.run()) for w in workers]
            try:
                await until(lambda: pending(pworker_service, campaign) == 0 if calls else False)
                assert calls == [entry]
                assert any(w.execution_outcomes.get(entry) == "completed" for w in workers)
                assert forged
            finally:
                for w in workers:
                    w.request_stop()
                await asyncio.wait_for(asyncio.gather(*tasks), 10)
                for w in workers:
                    await w.close()
            restarted = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            task = asyncio.create_task(restarted.run())
            try:
                await asyncio.sleep(0.2)
                assert calls == [entry]
            finally:
                restarted.request_stop()
                await asyncio.wait_for(task, 10)
                await restarted.close()
            return entry

    entry = run(scenario())
    lane = derive_commissioning_lane(campaign.permit)
    assert pworker_service.command("GET", lane.prefix + ":claim-execution:" + entry) == b"consumed"
    assert pworker_service.command("TTL", lane.prefix + ":claim-execution:" + entry) == -1
    assert not pworker_service.command("EXISTS", lane.dead)


def test_failed_execution_is_not_replayed_after_restart(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    calls: list[str] = []

    async def execute(claim: Any) -> None:
        calls.append(claim.entry_id)
        raise RuntimeError("synthetic interrupted dispatch")

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            first = await opened(directory, guard=GuardProbe(campaign).guard, execute_claim=execute)
            await first._consumer.ensure_group()
            config = first.consumer_config
            rows = await first._consumer._redis.xreadgroup(
                config.consumer_group, config.consumer_name, {config.stream: ">"}
            )
            try:
                _, fields = rows[0][1][0]
                await first._consumer._dispatch(entry, dict(fields))
                await until(lambda: first.execution_outcomes.get(entry) == "uncertain")
                assert pending(pworker_service, campaign) == 1
            finally:
                await first.close()
            second = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            task = asyncio.create_task(second.run())
            try:
                await until(lambda: pending(pworker_service, campaign) == 0)
                assert second.execution_outcomes[entry] == "execution-refused"
                assert calls == [entry]
            finally:
                second.request_stop()
                await asyncio.wait_for(task, 10)
                await second.close()

    run(scenario())


def test_stale_parked_ids_are_pruned_without_charging_deliveries(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")

    async def scenario() -> None:
        async with started(campaign) as control:
            gone, _, _ = await campaign.enqueue(control, BODY, commit=False)
            retained, _, _ = await campaign.enqueue(control, BODY, commit=False)
            worker = await opened(directory, guard=GuardProbe(campaign).guard)
            consumer = worker._consumer
            try:
                await consumer.ensure_group()
                c = worker.consumer_config
                await consumer._redis.xreadgroup(c.consumer_group, c.consumer_name, {c.stream: ">"})
                await consumer._recheck_pending(parked_only=False)
                await until(lambda: worker.verdicts.get(retained) == CampaignParked(8))
                assert consumer._parked == {gone, retained}
                pworker_service.command("XACK", c.stream, c.consumer_group, gone)
                await consumer._recheck_pending(parked_only=True)
                assert consumer._parked == {retained}
                assert _deliveries(pworker_service, campaign, retained) == 1
            finally:
                await worker.close()

    run(scenario())


def test_execution_composition_rejects_a_renamed_consumer_before_open(tmp_path: Path) -> None:
    from curie_commissioning_worker.entry import CommissioningWorkerRefused

    async def execute(claim: Any) -> None:
        pytest.fail("must not execute")

    async def scenario() -> None:
        with pytest.raises(CommissioningWorkerRefused) as error:
            await opened(tmp_path / "absent", execute_claim=execute, consumer_name="renamed")
        assert error.value.step == 1

    run(scenario())


def _replica(directory: str, records: Any, entry: str, ready: Any, calls: Any) -> None:
    """Fresh worker clients and immutable tuple records in each OS process."""
    from types import SimpleNamespace

    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap
    from curie_protected_hooks.commissioning_records import parse_commissioning_permit

    campaign = SimpleNamespace(
        manifest=parse_manifest(records[0]), bootstrap=parse_commissioning_bootstrap(records[1]),
        permit=parse_commissioning_permit(records[2]),
    )
    async def execute(claim: Any) -> None:
        calls.put(claim.entry_id)
        await asyncio.sleep(0.2)

    async def scenario() -> None:
        worker = await opened(
            Path(directory), guard=GuardProbe(campaign).guard, execute_claim=execute
        )
        ready.wait(timeout=10)
        task = asyncio.create_task(worker.run())
        try:
            await until(lambda: entry in worker.verdicts or task.done())
            await asyncio.sleep(0.4)
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, 10)
            await worker.close()

    run(scenario())


def test_two_os_processes_racing_one_pending_claim_only_forward_once(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    import multiprocessing
    import queue

    directory = campaign.write_directory(tmp_path / "runtime")
    context = multiprocessing.get_context("spawn")
    ready, calls = context.Barrier(2), context.Queue()

    async def scenario() -> str:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            worker = await opened(directory, guard=GuardProbe(campaign).guard)
            try:
                await worker._consumer.ensure_group()
                c = worker.consumer_config
                await worker._consumer._redis.xreadgroup(
                    c.consumer_group, c.consumer_name, {c.stream: ">"}
                )
            finally:
                await worker.close()
            replicas = [
                context.Process(
                    target=_replica,
                    args=(str(directory), (
                        campaign.manifest.canonical_bytes, campaign.bootstrap.canonical_bytes,
                        campaign.permit.canonical_bytes,
                    ), entry, ready, calls),
                )
                for _ in range(2)
            ]
            try:
                for replica in replicas:
                    replica.start()
                for replica in replicas:
                    await asyncio.to_thread(replica.join, 15)
                    assert not replica.is_alive(), "owned replica must finish"
                    assert replica.exitcode == 0
                assert calls.get(timeout=2) == entry
                with pytest.raises(queue.Empty):
                    calls.get(timeout=0.1)
                assert pending(pworker_service, campaign) == 0
            finally:
                for replica in replicas:
                    if replica.is_alive():
                        replica.terminate()
                    replica.join(timeout=5)
            return entry

    try:
        run(scenario())
    finally:
        calls.close()
        calls.join_thread()


@pytest.mark.parametrize("value", ["consumed", "", "incomplete"])
def test_existing_execution_scalar_refuses_forwarding(
    pworker_service: Any, campaign: Any, tmp_path: Path, value: str
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")

    async def execute(claim: Any) -> None:
        pytest.fail("a consumed or corrupt scalar never authorizes dispatch")

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            lane = derive_commissioning_lane(campaign.permit)
            pworker_service.command("SET", lane.prefix + ":claim-execution:" + entry, value)
            worker = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            task = asyncio.create_task(worker.run())
            try:
                await until(lambda: worker.execution_outcomes.get(entry) == "execution-refused")
                await until(lambda: pending(pworker_service, campaign) == 0)
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, 10)
                await worker.close()

    run(scenario())


def test_foreign_pending_owner_refuses_boot_before_execution(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")

    async def execute(claim: Any) -> None:
        pytest.fail("foreign pending owner must refuse before forwarding")

    async def scenario() -> None:
        async with started(campaign) as control:
            await campaign.enqueue(control, BODY)
            prior = await opened(
                directory, guard=GuardProbe(campaign).guard, consumer_name="old-name"
            )
            try:
                await prior._consumer.ensure_group()
                c = prior.consumer_config
                await prior._consumer._redis.xreadgroup(
                    c.consumer_group, c.consumer_name, {c.stream: ">"}
                )
            finally:
                await prior.close()
            worker = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            try:
                with pytest.raises(RuntimeError, match="foreign pending owner"):
                    await asyncio.wait_for(worker.run(), 10)
                assert pending(pworker_service, campaign) == 1
                assert worker.execution_outcomes == {}
            finally:
                await worker.close()

    run(scenario())


def test_unconfirmed_ack_is_uncertain_and_cannot_reforward_a_redelivered_entry(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    lane = derive_commissioning_lane(campaign.permit)
    calls: list[str] = []

    async def execute(claim: Any) -> None:
        calls.append(claim.entry_id)
        # A real competing broker settlement removes the row before our ACK.
        assert pworker_service.command("XACK", lane.stream, lane.group, claim.entry_id) == 1

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            first = await opened(directory, guard=GuardProbe(campaign).guard, execute_claim=execute)
            task = asyncio.create_task(first.run())
            try:
                await until(lambda: first.execution_outcomes.get(entry) == "uncertain")
                assert first.execution_outcomes[entry] != "completed"
            finally:
                first.request_stop()
                await asyncio.wait_for(task, 10)
                await first.close()
            # Force the same retained stream id back through the actual reader.
            pworker_service.command("XGROUP", "SETID", lane.stream, lane.group, "0")
            second = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            task = asyncio.create_task(second.run())
            try:
                await until(lambda: second.execution_outcomes.get(entry) == "execution-refused")
                assert calls == [entry]
            finally:
                second.request_stop()
                await asyncio.wait_for(task, 10)
                await second.close()

    run(scenario())


@pytest.mark.parametrize("committed", [True, False])
def test_only_eligible_execution_reaches_the_shared_delivery_cap(
    pworker_service: Any, campaign: Any, tmp_path: Path, committed: bool
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")
    lane = derive_commissioning_lane(campaign.permit)

    async def execute(claim: Any) -> None:
        pytest.fail("exhausted or parked claims never forward")

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY, commit=committed)
            worker = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            await worker._consumer.ensure_group()
            c = worker.consumer_config
            await worker._consumer._redis.xreadgroup(
                c.consumer_group, c.consumer_name, {c.stream: ">"}
            )
            # The broker is the authoritative delivery budget, including past transfers.
            pworker_service.command(
                "XCLAIM", lane.stream, lane.group, c.consumer_name, "0", entry,
                "RETRYCOUNT", str(c.max_delivery),
            )
            task = asyncio.create_task(worker.run())
            try:
                if committed:
                    await until(lambda: pending(pworker_service, campaign) == 0)
                    rows = pworker_service.command("XRANGE", lane.dead, "-", "+")
                    assert len(rows) == 1
                    assert worker.execution_outcomes.get(entry) != "completed"
                else:
                    await until(lambda: worker.verdicts.get(entry) == CampaignParked(8))
                    await asyncio.sleep(0.15)
                    assert _deliveries(pworker_service, campaign, entry) == c.max_delivery
                    assert not pworker_service.command("EXISTS", lane.dead)
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, 10)
                await worker.close()

    run(scenario())


def test_dead_letter_fence_refusal_remains_recheckable_until_broker_settlement(
    pworker_service: Any, campaign: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_commissioning_worker.consumer import CommissioningConsumer

    directory = campaign.write_directory(tmp_path / "runtime")
    original = CommissioningConsumer._dead_letter
    injected = False

    async def lose_at_settlement(
        consumer: Any, entry_id: str, fields: Any, *, reason: str, delivery_count: int
    ) -> None:
        nonlocal injected
        if not injected:
            injected = True
            lease = consumer._held_leases[entry_id]
            c = consumer._config
            # Scheduling barrier only: inject a real external lease loss at the
            # settlement boundary, then wait for the unchanged heartbeat to see
            # it. The real graveyard decides whether settlement is authorized.
            assert pworker_service.command(
                "DEL", c.delivery_lease_key(c.stream, c.consumer_group, entry_id)
            ) == 1
            await asyncio.wait_for(lease.lost.wait(), 15)
        await original(consumer, entry_id, fields, reason=reason, delivery_count=delivery_count)

    monkeypatch.setattr(CommissioningConsumer, "_dead_letter", lose_at_settlement)

    async def execute(claim: Any) -> None:
        pytest.fail("an over-cap claim must never forward")

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            worker = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            consumer, c = worker._consumer, worker.consumer_config
            try:
                await consumer.ensure_group()
                rows = await consumer._redis.xreadgroup(
                    c.consumer_group, c.consumer_name, {c.stream: ">"}
                )
                pworker_service.command(
                    "XCLAIM", c.stream, c.consumer_group, c.consumer_name, "0", entry,
                    "RETRYCOUNT", str(c.max_delivery),
                )
                await consumer._dispatch(entry, dict(rows[0][1][0][1]))
                await until(lambda: not consumer._inflight, timeout=20)
                assert pending(pworker_service, campaign) == 1, "the stale owner cannot ACK"
                assert entry in consumer._unsettled, "a refused dead-letter is still pending work"
                await consumer._recheck_pending(parked_only=True)
                await until(lambda: pending(pworker_service, campaign) == 0)
                await consumer._recheck_pending(parked_only=True)
                assert entry not in consumer._unsettled
            finally:
                await asyncio.gather(*tuple(consumer._inflight), return_exceptions=True)
                await worker.close()

    run(scenario())


@pytest.mark.parametrize("stop_peer", [False, True])
def test_same_name_replica_liveness_does_not_cancel_another_process_delivery(
    pworker_service: Any, campaign: Any, tmp_path: Path, stop_peer: bool
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")

    async def scenario() -> None:
        began, finish = asyncio.Event(), asyncio.Event()
        calls: list[str] = []

        async def execute(claim: Any) -> None:
            calls.append(claim.entry_id)
            began.set()
            await finish.wait()

        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            first = await opened(directory, guard=GuardProbe(campaign).guard, execute_claim=execute)
            second = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            first_task = asyncio.create_task(first.run())
            second_task = None
            try:
                await asyncio.wait_for(began.wait(), 5)
                second_task = asyncio.create_task(second.run())
                await until(lambda: entry in second.verdicts or second_task.done())
                if stop_peer:
                    second.request_stop()
                    await asyncio.wait_for(second_task, 5)
                # Pass the actual first renewal cadence, not a shortened fake
                # clock. Publishing or cleaning up a peer must not replace the
                # live executor's independent process-liveness ownership.
                await asyncio.sleep(first._consumer._liveness_refresh_interval_s() + 0.5)
                assert not first_task.done(), "a same-name peer must not cancel the live delivery"
                if not stop_peer:
                    assert not second_task.done()
                assert calls == [entry]
                finish.set()
                await until(lambda: first.execution_outcomes.get(entry) == "completed")
                assert pending(pworker_service, campaign) == 0
            finally:
                finish.set()
                first.request_stop()
                second.request_stop()
                tasks = [first_task] + ([] if second_task is None else [second_task])
                await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 15)
                await first.close()
                await second.close()

    run(scenario())


def test_cancelled_delivery_joins_cooperative_executor_cleanup_before_releasing_lease(
    pworker_service: Any, campaign: Any, tmp_path: Path
) -> None:
    directory = campaign.write_directory(tmp_path / "runtime")

    async def scenario() -> None:
        began, cleaned = asyncio.Event(), asyncio.Event()

        async def execute(claim: Any) -> None:
            try:
                began.set()
                await asyncio.Future()
            finally:
                await asyncio.sleep(0.05)
                cleaned.set()

        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            worker = await opened(
                directory, guard=GuardProbe(campaign).guard, execute_claim=execute
            )
            consumer, c = worker._consumer, worker.consumer_config
            try:
                await consumer.ensure_group()
                rows = await consumer._redis.xreadgroup(
                    c.consumer_group, c.consumer_name, {c.stream: ">"}
                )
                await consumer._dispatch(entry, dict(rows[0][1][0][1]))
                await asyncio.wait_for(began.wait(), 5)
                handlers = tuple(consumer._inflight)
                assert len(handlers) == 1
                handlers[0].cancel()
                await asyncio.gather(*handlers, return_exceptions=True)
                assert cleaned.is_set(), "executor cleanup must finish before handler teardown"
                assert worker.execution_outcomes[entry] == "uncertain"
                assert pending(pworker_service, campaign) == 1
                assert not pworker_service.command(
                    "EXISTS", c.delivery_lease_key(c.stream, c.consumer_group, entry)
                )
            finally:
                await worker.close()

    run(scenario())
