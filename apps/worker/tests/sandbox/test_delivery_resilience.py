"""Real stream recovery and side effects, below the cluster fault-injection tier.

The production Kernel, substrate, RunnerClient, SessionRunner and Valkey run
unchanged. The external model fixture executes one actual subprocess file append
between its SDK tool-use/result messages. Replacement constructs new worker and
runner objects, then consumes the abandoned PEL entry through real XAUTOCLAIM.
Kubernetes and Slack are test doubles. This does not claim a pod/process kill or
live-provider evidence; those require the disposable cluster scenario.
"""

from __future__ import annotations

import asyncio
import contextlib
import sys
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn, ReplyHandle
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock
from curie_dispatcher.queue import to_stream_fields
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_worker.consumer import Consumer
from curie_worker.delivery_lease import DeliveryLeaseStore

from apps.worker.tests.kernel.conftest import _ProcessEventSpy, kernel_harness


class ReceiptModel(FakeModelSession):
    """A scripted provider whose tool really appends a durable receipt."""

    def __init__(self, receipt: Path, fault: str | None) -> None:
        super().__init__()
        self.receipt = receipt
        self.fault = fault
        self.reached = asyncio.Event()

    async def receive_turn(self) -> AsyncIterator[Any]:
        yield AssistantMessage(content=[TextBlock(text="starting")], model="fake-model")
        if self.fault == "before-effect":
            self.reached.set()
            await asyncio.Event().wait()
        yield AssistantMessage(
            content=[ToolUseBlock(id="receipt", name="Bash", input={"command": "append receipt"})],
            model="fake-model",
        )
        # The receipt is an actual child-process side effect, not a claim count,
        # an emitted frame count or an in-memory call recorder.
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import os,sys; "
            "fd=os.open(sys.argv[1],os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600); "
            "os.write(fd,b'committed\\n'); os.fsync(fd); os.close(fd)",
            str(self.receipt),
        )
        assert await process.wait() == 0
        self.reached.set()
        if self.fault == "after-effect":
            await asyncio.Event().wait()
        yield ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1, is_error=False,
            num_turns=1, session_id="resilience-session", result="receipt committed",
        )


def _runner(model: ReceiptModel) -> SessionRunner:
    return SessionRunner(
        session_factory=lambda: model,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="delivery-resilience",
    )


@pytest.mark.parametrize("fault", ["before-effect", "after-effect"])
@pytest.mark.parametrize("fenced", [False, True], ids=["legacy-unfenced", "current-fenced"])
def test_reclaimed_delivery_commits_exactly_one_receipt(
    fault: str, fenced: bool, names: dict[str, str], sync_redis: redis.Redis, tmp_path: Path,
) -> None:
    async def run() -> None:
        receipt = tmp_path / "effect-receipts"
        event = QueuedTurn(
            event_id=uuid.uuid4().hex,
            conversation_id="resilience-thread",
            author="U0EXAMPLE1",
            text="commit one receipt",
            reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder="1.0"),
            received_at="2026-09-05T00:00:00+00:00",
        )
        first_model = ReceiptModel(receipt, fault)
        first_runner = _runner(first_model)
        await first_runner.start()
        async with kernel_harness(
            names, sync_redis, runner_app=create_app(first_runner), reclaim_min_idle_ms=0,
        ) as first:
            first_calls = _ProcessEventSpy(first.kernel)
            first_config = first.config.model_copy(update={"consumer_name": "departed-worker"})
            consumer = Consumer(
                redis=first.async_redis, kernel=first.kernel, config=first_config,
                leases=DeliveryLeaseStore(first.async_redis, first_config) if fenced else None,
            )
            await consumer.ensure_group()
            entry_id = await first.async_redis.xadd(first.config.stream, to_stream_fields(event))
            deliveries = await first.async_redis.xreadgroup(
                first.config.consumer_group, "departed-worker", {first.config.stream: ">"}, count=1,
            )
            assert deliveries[0][1][0][0] == entry_id
            if fenced:
                await consumer._dispatch(*deliveries[0][1][0])
                task = next(iter(consumer._inflight))
            else:
                task = asyncio.create_task(first.kernel.process_event(event))
            try:
                await asyncio.wait_for(first_model.reached.wait(), timeout=5)
                if fenced:
                    lease = first_calls.leases_for(event.event_id)[0]
                    assert lease is not None and lease.generation == 1
                if fault == "after-effect":
                    async with asyncio.timeout(5):
                        while not await first.async_redis.exists(
                            first.config.side_effect_key(event.event_id)
                        ):
                            await asyncio.sleep(0.01)
                    assert receipt.read_text() == "committed\n"
                else:
                    assert not receipt.exists()
                    assert not await first.async_redis.exists(
                        first.config.side_effect_key(event.event_id)
                    )
                assert not await first.async_redis.exists(first.config.done_key(event.event_id))
            finally:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        # All worker/kernel/runner clients are fresh. Only the real Valkey
        # route, pending entry and side-effect marker survive the replacement.
        second_model = ReceiptModel(receipt, None)
        second_runner = _runner(second_model)
        await second_runner.start()
        async with kernel_harness(
            names, sync_redis, runner_app=create_app(second_runner), reclaim_min_idle_ms=0,
        ) as second:
            second_calls = _ProcessEventSpy(second.kernel)
            second_config = second.config.model_copy(update={"consumer_name": "replacement-worker"})
            consumer = Consumer(
                redis=second.async_redis, kernel=second.kernel, config=second_config,
                leases=DeliveryLeaseStore(second.async_redis, second_config) if fenced else None,
            )
            await consumer.ensure_group()
            assert await consumer._reclaim_once() == 1
            await asyncio.wait_for(asyncio.gather(*list(consumer._inflight)), timeout=10)
            if fenced:
                assert second_calls.leases_for(event.event_id)[0].generation == 2
            assert receipt.read_text() == "committed\n"
            expected_queries = [] if fault == "after-effect" else [event.text]
            assert second_model.queries == expected_queries
            assert second.sink.completions[-1].outcome == (
                "escalated" if fault == "after-effect" else "delivered"
            )
            pending = await second.async_redis.xpending(
                second.config.stream, second.config.consumer_group
            )
            assert pending["pending"] == 0
            assert await second.async_redis.exists(second.config.done_key(event.event_id))

            # A second independently delivered duplicate cannot append again.
            duplicate_id = await second.async_redis.xadd(
                second.config.stream, to_stream_fields(event)
            )
            duplicate = await second.async_redis.xreadgroup(
                second.config.consumer_group,
                second_config.consumer_name,
                {second.config.stream: ">"},
                count=1,
            )
            assert duplicate[0][1][0][0] == duplicate_id
            await consumer._dispatch(*duplicate[0][1][0])
            await asyncio.wait_for(asyncio.gather(*list(consumer._inflight)), timeout=10)
            assert receipt.read_text() == "committed\n"
            assert second_model.queries == expected_queries

    asyncio.run(run())
