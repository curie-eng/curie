"""Protected completion refuses administrative and unproved terminal writes."""

import asyncio

import pytest
from curie_worker.protected_markers import ProtectedMarkers, ProtectedSettlementError


@pytest.mark.parametrize(
    "method",
    [
        "mark_done_without_completion",
        "drop_pending_member",
        "mark_done",
        "mark_completion_pending",
        "publish_successor",
        "settle_fenced_without_completion",
        "settle_fenced_and_publish",
    ],
)
def test_marker_only_and_discard_refuse_without_broker_access(method):
    markers = object.__new__(ProtectedMarkers)
    with pytest.raises(ProtectedSettlementError):
        asyncio.run(getattr(markers, method)("event-example"))


def test_unconfirmed_clear_never_accesses_broker():
    markers = object.__new__(ProtectedMarkers)
    markers._confirmed = {}
    assert asyncio.run(markers.clear_completion("event-example", generation="a" * 32)) is False


def test_unscoped_settlement_never_accesses_broker():
    markers = object.__new__(ProtectedMarkers)
    assert (
        asyncio.run(
            markers.settle_fenced(
                "event-example",
                object(),
                stream="curie:runs",
                group="protected-example",
                entry_id="1-0",
                owner="owner-example",
                generation=1,
                marker_value="1",
            )
        )
        is None
    )


def test_sweep_refuses_unbounded_batch_before_broker_access():
    from curie_worker.protected_completion import ProtectedReplySink, sweep_protected_completions

    markers = object.__new__(ProtectedMarkers)
    sink = object.__new__(ProtectedReplySink)
    sink._markers = markers
    with pytest.raises(ProtectedSettlementError):
        asyncio.run(sweep_protected_completions(markers, sink, limit=65))


def test_actual_kernel_calls_injected_protected_marker_and_refuses_unowned_terminal(caplog):
    import time

    from aci_protocol import QueuedTurn, ReplyHandle, TurnSource
    from curie_worker.config import WorkerConfig
    from curie_worker.delivery_lease import DeliveryBudget, DeliveryLease
    from curie_worker.kernel.core import Kernel
    from curie_worker.reply_sink import TargetRoute

    async def go():
        markers = object.__new__(ProtectedMarkers)
        kernel = Kernel(
            substrate=None,
            runner=None,
            sink=None,
            lock=None,
            pressure_lock=None,
            markers=markers,
            config=WorkerConfig(),
        )
        assert kernel._markers is markers
        lease = DeliveryLease(
            stream="curie:runs",
            group="group-example",
            entry_id="1-0",
            owner="owner-example",
            generation=1,
            budget=DeliveryBudget(60000, 0, time.monotonic()),
        )
        event = QueuedTurn(
            event_id="event-example",
            conversation_id="thread-example",
            author="user-example",
            text="synthetic",
            source=TurnSource.SLACK,
            received_at="2026-10-12T00:00:00+00:00",
            reply_handle=ReplyHandle(
                kind="slack", channel="C0EXAMPLE1", placeholder="message-example"
            ),
        )
        await kernel._complete(
            event, TargetRoute(), "delivered", telemetry_outcome="delivered", lease=lease
        )
        assert lease.lost.is_set()
        assert lease.owner not in caplog.text

    asyncio.run(go())


def test_caller_supplied_sink_cannot_grant_confirmed_clear_authority():
    from curie_worker.protected_completion import ProtectedReplySink

    markers = object.__new__(ProtectedMarkers)
    with pytest.raises(ProtectedSettlementError):
        ProtectedReplySink(markers, object())
