"""A 0.2.9 runner against a 0.5.0 worker fails LOUDLY.

T-B14 (round-2 finding 4). The ACI bump is a breaking minor (D1), so a runner
image left at ``0.2.9`` is not "mostly compatible" -- ``ndjson.py:61-68`` refuses
its frames. This is what makes cutover step 4 (delete every live sandbox) a
TESTED requirement rather than an assertion: ``substrate.py:80-93``'s ``claim()``
returns an already-running sandbox for a live route, so without that step the
worker is handed a 0.2.9 runner and must not paper over it.

The failure has to be loud in a specific way: the entry stays PENDING for
reclaim and the turn does NOT silently complete. A version mismatch that
completed the turn would tell the adapter to send an email for a turn that never
produced an answer.
"""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    Final,
    ProtocolVersionError,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    is_compatible,
    parse_ndjson_line,
)
from curie_dispatcher.queue import to_stream_fields
from curie_worker.consumer import Consumer

DONE = SessionStatus.DONE

# A frame stamped by a historical runner, pinned as a literal rather than
# generated, so this keeps testing the old contract hop after PROTOCOL_VERSION
# moves again.
FRAME_0_2_9 = (
    '{"version":"0.2.9","type":"final","text":"an answer from an old runner",'
    '"status":"done"}'
)


def _qevent(*, thread: str, event_id: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id=thread,
        author="U1",
        text="hi",
        reply_handle=ReplyHandle(
            kind="email",
            channel="agent@example.test",
            placeholder="msg_upstream",
            endpoint="https://adapter.example/hook",
            adapter="agentmail-sandbox",
        ),
        received_at="2026-07-05T00:00:00+00:00",
    )


def test_the_worker_build_rejects_historical_and_previous_deployed_minors() -> None:
    # The companion assertion (version.py:42-63). A breaking minor means the
    # range genuinely excludes both the previous deployed minor and the older
    # historical version. If either reads True again, the bump was classed wrong
    # and the cutover ordering rests on nothing.
    #
    # The MINOR is pinned, not the exact version. Compatibility under 0.x is
    # decided by major.minor, so a patch bump (a new optional field, which is the
    # only compatible change class) cannot move either exclusion below; pinning
    # the patch digit made this test fail for a change it does not measure, and a
    # test that has to be edited for reasons unrelated to its subject teaches
    # people to edit it without reading it. A minor bump still lands here, which
    # is exactly when someone should look.
    assert PROTOCOL_VERSION.startswith("0.5."), PROTOCOL_VERSION
    assert is_compatible("0.4.8", PROTOCOL_VERSION) is False
    assert is_compatible(PROTOCOL_VERSION, "0.4.8") is False
    assert is_compatible("0.3.0", PROTOCOL_VERSION) is False
    assert is_compatible(PROTOCOL_VERSION, "0.3.0") is False
    assert is_compatible("0.2.9", PROTOCOL_VERSION) is False
    assert is_compatible(PROTOCOL_VERSION, "0.2.9") is False


def test_a_0_2_9_frame_is_refused_by_the_decoder() -> None:
    with pytest.raises(ProtocolVersionError) as excinfo:
        parse_ndjson_line(FRAME_0_2_9)
    # Both versions are named, so an operator reading the log knows which half
    # to roll rather than which half to guess.
    message = str(excinfo.value)
    assert "0.2.9" in message
    assert PROTOCOL_VERSION in message


def test_an_old_runner_leaves_the_entry_pending_and_completes_nothing(
    make_harness,
) -> None:
    # The loud path, end to end. The stream drop cases (aiohttp.ClientError,
    # TimeoutError) are caught in ``_consume`` and classified as retryable; a
    # version refusal is deliberately NOT one of those, so it escapes to the
    # consumer, which leaves the entry pending (consumer.py:209-213).
    async def go() -> None:
        # ``max_delivery`` is raised well above the number of reclaims this
        # window can fit (the harness runs reclaim_interval_s at 0.05). The
        # property under test is that the entry STAYS pending across
        # redeliveries; at the shipped cap of 5 it is correctly dead-lettered as
        # max-delivery-exceeded (#505, ADR-0039) inside the window, and the
        # pending count then drops to 0 for a reason that has nothing to do with
        # the version gate. Raising the cap keeps the assertion measuring the
        # version refusal rather than the delivery cap. It does not weaken it:
        # an implementation that ACKED the 0.2.9 frame leaves zero pending at
        # any cap, so the test still fails on the defect it exists to catch.
        async with make_harness(shimmer=False, max_delivery=50) as h:
            # The fake runner serializes model instances, so an old-runner frame
            # is one built at the old version.
            h.runner.default_script = [
                TextDelta(text="streaming from an old runner", version="0.2.9"),
                Final(text="an answer", status=DONE, version="0.2.9"),
            ]
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            qe = _qevent(thread="tMix", event_id="mix-1")
            await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
            task = asyncio.create_task(consumer.run())

            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                if h.runner.opened:
                    break
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.3)
            consumer.request_stop()
            await task

            summary = await h.async_redis.xpending(
                h.config.stream, h.config.consumer_group
            )
            assert summary["pending"] == 1, "a version refusal must not ack the entry"
            assert h.sink.completions == [], (
                "a turn that never decoded must not tell the adapter it completed"
            )
            assert await h.async_redis.exists(h.config.done_key("mix-1")) == 0
            assert (
                await h.async_redis.smembers(h.config.completions_pending_key()) == set()
            )

    asyncio.run(go())


def test_a_current_runner_on_the_same_harness_completes_normally(make_harness) -> None:
    # The control. Without it, a harness that is broken for any other reason
    # would pass the test above for the wrong reason -- "nothing completed"
    # is trivially true when nothing runs at all.
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            h.runner.default_script = [Final(text="an answer", status=DONE)]
            ev = _qevent(thread="tMixOk", event_id=uuid.uuid4().hex)
            await h.kernel.process_event(ev)
            assert [c.outcome for c in h.sink.completions] == ["delivered"]
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())
