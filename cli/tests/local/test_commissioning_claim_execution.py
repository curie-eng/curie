"""Protected claim recovery across process death and a naturally expired 45-second lease."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from curie_protected_hooks.commissioning_acl import derive_commissioning_lane

from apps.worker.tests import test_commissioning_worker_entry as entry_tests
from apps.worker.tests.test_commissioning_claim_execution import pending
from apps.worker.tests.test_commissioning_worker_entry import (
    BODY,
    GuardProbe,
    opened,
    run,
    started,
    until,
)

campaign = entry_tests.campaign
pworker_service = entry_tests.pworker_service


def _effect_replica(directory: str, records: Any, entry: str, effects: str, events: Any) -> None:
    """Real entry/consumer; synthetic guard facts do not qualify a protected runtime."""
    import os
    from types import SimpleNamespace

    from curie_protected_hooks.authority_records import parse_manifest
    from curie_protected_hooks.commissioning_bootstrap import parse_commissioning_bootstrap
    from curie_protected_hooks.commissioning_records import parse_commissioning_permit

    selected = SimpleNamespace(
        manifest=parse_manifest(records[0]), bootstrap=parse_commissioning_bootstrap(records[1]),
        permit=parse_commissioning_permit(records[2]),
    )

    async def execute(claim: Any) -> None:
        fd = os.open(effects, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.write(fd, (claim.entry_id + "\n").encode("ascii"))
            os.fsync(fd)
        finally:
            os.close(fd)
        events.put(("effect", claim.entry_id))
        await asyncio.Future()

    async def scenario() -> None:
        worker = await opened(
            Path(directory), guard=GuardProbe(selected).guard, execute_claim=execute
        )
        task = asyncio.create_task(worker.run())
        try:
            await until(lambda: entry in worker.execution_outcomes or task.done(), timeout=90)
            assert not task.done(), "worker must reach a settlement outcome without failing"
            events.put(("outcome", worker.execution_outcomes[entry]))
        finally:
            worker.request_stop()
            await asyncio.wait_for(task, 10)
            await worker.close()

    run(scenario())


def test_a_hard_killed_executor_is_not_replayed_by_a_fresh_os_process(
    pworker_service: Any, campaign: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec PROTECTED-COMMISSIONING-4: consume-before-forward survives process death."""
    import multiprocessing
    import queue

    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[3]))
    directory = campaign.write_directory(tmp_path / "runtime")
    effects = tmp_path / "effects.txt"
    context = multiprocessing.get_context("spawn")
    events = context.Queue()
    replicas: list[Any] = []

    async def scenario() -> None:
        async with started(campaign) as control:
            entry, _, _ = await campaign.enqueue(control, BODY)
            records = (
                campaign.manifest.canonical_bytes, campaign.bootstrap.canonical_bytes,
                campaign.permit.canonical_bytes,
            )

            def replica() -> Any:
                child = context.Process(
                    target=_effect_replica,
                    args=(str(directory), records, entry, str(effects), events),
                )
                replicas.append(child)
                child.start()
                return child

            first = replica()
            message = await asyncio.to_thread(events.get, True, 15)
            assert message == ("effect", entry)
            assert pending(pworker_service, campaign) == 1, "crash happens before settlement"
            first.kill()
            await asyncio.to_thread(first.join, 5)
            assert not first.is_alive() and first.exitcode != 0
            replacement = replica()
            # The real lease must expire naturally. No test changes its TTL,
            # deletes delivery state, seeds a private field or releases it.
            message = await asyncio.to_thread(events.get, True, 100)
            assert message == ("outcome", "execution-refused")
            await asyncio.to_thread(replacement.join, 15)
            assert not replacement.is_alive() and replacement.exitcode == 0
            assert effects.read_text().splitlines() == [entry]
            assert pending(pworker_service, campaign) == 0
            lane = derive_commissioning_lane(campaign.permit)
            assert pworker_service.command(
                "GET", lane.prefix + ":claim-execution:" + entry
            ) == b"consumed"
            assert pworker_service.command(
                "TTL", lane.prefix + ":claim-execution:" + entry
            ) == -1
            with pytest.raises(queue.Empty):
                events.get(timeout=0.1)

    try:
        run(scenario())
    finally:
        for child in replicas:
            if child.is_alive():
                child.kill()
            child.join(timeout=5)
            assert not child.is_alive(), "only owned experiment processes must be gone"
        events.close()
        events.join_thread()


