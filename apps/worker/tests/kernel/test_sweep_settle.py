"""The next sweep slice is published inside the fenced settle (#2878 revision 1, R3).

A continuation is queued by the SAME Valkey script that settles the slice that
asked for it, behind the same ADR-0131 fence, so a fenced-out owner publishes
nothing and a lost reply can neither duplicate nor lose the successor
(revision 2, F2). Real Valkey throughout; the kernel cases use the full harness.
"""

from __future__ import annotations

import asyncio
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import redis
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus
from aci_protocol.service_config import STREAM_PAYLOAD_FIELD
from channel_protocol.reply import REPLY_WIRE_VERSION, ReplyTarget, TurnCompleted
from curie_worker import markers as markers_module
from curie_worker.markers import CompletionRecord
from curie_worker.reply_sink import TargetRoute

# importlib import mode does not add the test root to sys.path.
sys.path.insert(0, str(Path(__file__).parent.parent))

from sweep_fixtures import (  # noqa: E402
    PROMPT,
    Delivery,
    bump_fence,
    cron_event,
    enqueue,
    ensure_group,
    notices,
    read_next,
    redeliver,
    reply_texts,
    run_outcome,
    run_slice_to_budget_cut,
    stream_turns,
    successors,
    sweep_case,
)


def _record(event_id: str) -> CompletionRecord:
    return CompletionRecord(
        event_id=event_id,
        event=TurnCompleted(
            version=REPLY_WIRE_VERSION,
            event="turn.completed",
            target=ReplyTarget(
                kind="slack", address="C1", conversation_id="hook-thread", reply_ref=None
            ),
            event_id=event_id,
            outcome="dropped",
        ),
        route=TargetRoute(),
        created_at=time.time(),
        done=False,
    )


async def _owned_entry(h: Any) -> Delivery:
    """One placeholder entry this consumer read and holds the lease on."""
    await ensure_group(h)
    predecessor = QueuedTurn(
        event_id=f"cron:{uuid.uuid4()}:sweep-hook:2026-09-22T03:00:00+00:00",
        conversation_id="hook-thread",
        author="cron:sweep-hook",
        text=PROMPT,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder=None),
        received_at="2026-09-22T03:00:01+00:00",
    )
    await h.async_redis.xadd(h.config.stream, {STREAM_PAYLOAD_FIELD: predecessor.model_dump_json()})
    delivery = await read_next(h)
    assert delivery is not None
    return delivery


def _successor_json(delivery: Delivery) -> str:
    return delivery.event.model_copy(
        update={"event_id": f"{delivery.event.event_id}:sweep:1:1:0"}
    ).model_dump_json()


async def _payloads(h: Any) -> list[str]:
    return [
        fields[STREAM_PAYLOAD_FIELD]
        for _id, fields in await h.async_redis.xrange(h.config.stream)
        if STREAM_PAYLOAD_FIELD in fields
    ]


def test_settle_fenced_and_publish_settles_and_publishes_once(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            owned = await _owned_entry(h)
            event_id = owned.event.event_id
            successor = _successor_json(owned)
            record_generation = uuid.uuid4().hex
            before = await _payloads(h)

            settled = await h.kernel._markers.settle_fenced_and_publish(
                event_id,
                _record(event_id),
                stream=owned.lease.stream,
                group=owned.lease.group,
                entry_id=owned.lease.entry_id,
                owner=owned.lease.owner,
                generation=owned.lease.generation,
                marker_value="1",
                successor_stream=h.config.stream,
                successor_payload=successor,
                record_generation=record_generation,
            )

            assert settled == record_generation
            assert await h.async_redis.exists(h.config.done_key(event_id))
            stored = await h.kernel._markers.read_completion(event_id)
            assert stored is not None
            assert stored.done_flag is True
            assert stored.generation == record_generation
            assert stored.record.event_id == event_id
            assert (
                await h.async_redis.hget(
                    h.config.completion_key(event_id), markers_module._GENERATION_FIELD
                )
                == record_generation
            )
            after = await _payloads(h)
            assert after[: len(before)] == before
            assert after[len(before) :] == [successor]
            assert await h.kernel._markers.is_terminal(event_id)

    asyncio.run(go())


def test_settle_fenced_and_publish_refused_by_a_lost_fence_writes_and_publishes_nothing(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness() as h:
            owned = await _owned_entry(h)
            event_id = owned.event.event_id
            before = await _payloads(h)
            lease = owned.lease
            base: dict[str, Any] = {
                "stream": lease.stream,
                "group": lease.group,
                "entry_id": lease.entry_id,
                "marker_value": "1",
                "successor_stream": h.config.stream,
                "successor_payload": _successor_json(owned),
            }

            wrong_owner = await h.kernel._markers.settle_fenced_and_publish(
                event_id,
                _record(event_id),
                owner="not-the-owner",
                generation=lease.generation,
                record_generation=uuid.uuid4().hex,
                **base,
            )
            wrong_generation = await h.kernel._markers.settle_fenced_and_publish(
                event_id,
                _record(event_id),
                owner=lease.owner,
                generation=lease.generation + 1,
                record_generation=uuid.uuid4().hex,
                **base,
            )

            assert wrong_owner is None
            assert wrong_generation is None
            assert not await h.async_redis.exists(h.config.done_key(event_id))
            assert not await h.async_redis.exists(h.config.completion_key(event_id))
            assert not await h.async_redis.sismember(h.config.completions_pending_key(), event_id)
            assert (
                await h.async_redis.hget(
                    h.config.completion_key(event_id), markers_module._GENERATION_FIELD
                )
                is None
            )
            assert await _payloads(h) == before

            # Liveness: the true owner and generation do settle.
            assert (
                await h.kernel._markers.settle_fenced_and_publish(
                    event_id,
                    _record(event_id),
                    owner=lease.owner,
                    generation=lease.generation,
                    record_generation="gen-ok",
                    **base,
                )
                == "gen-ok"
            )

    asyncio.run(go())


def test_settle_fenced_and_publish_is_idempotent_on_replay(make_harness) -> None:
    """Review round 1, A: the production client retries EVAL after a lost reply,
    so a replay with the same record generation publishes nothing new."""

    async def go() -> None:
        async with make_harness() as h:
            owned = await _owned_entry(h)
            event_id = owned.event.event_id
            successor = _successor_json(owned)
            before = await _payloads(h)
            lease = owned.lease
            args: dict[str, Any] = {
                "stream": lease.stream,
                "group": lease.group,
                "entry_id": lease.entry_id,
                "owner": lease.owner,
                "generation": lease.generation,
                "marker_value": "1",
                "successor_stream": h.config.stream,
                "successor_payload": successor,
                "record_generation": "gen-replayed",
            }

            first = await h.kernel._markers.settle_fenced_and_publish(
                event_id, _record(event_id), **args
            )
            replay = await h.kernel._markers.settle_fenced_and_publish(
                event_id, _record(event_id), **args
            )

            assert first == "gen-replayed"
            assert replay == "gen-replayed"
            assert (await _payloads(h))[len(before) :] == [successor]
            assert (
                await h.async_redis.hget(
                    h.config.completion_key(event_id), markers_module._GENERATION_FIELD
                )
                == "gen-replayed"
            )

    asyncio.run(go())


def test_settle_fenced_and_publish_writes_nothing_when_the_xadd_fails(make_harness) -> None:
    """Review round 1, A: the XADD runs before the record and the done marker, so a
    publication that errors leaves the slice unsettled rather than settled with no
    successor."""

    async def go() -> None:
        async with make_harness() as h:
            owned = await _owned_entry(h)
            event_id = owned.event.event_id
            lease = owned.lease
            wrong_type = f"{h.config.key_prefix}:not-a-stream"
            await h.async_redis.set(wrong_type, "a string, so XADD fails WRONGTYPE")
            try:
                result = await h.kernel._markers.settle_fenced_and_publish(
                    event_id,
                    _record(event_id),
                    stream=lease.stream,
                    group=lease.group,
                    entry_id=lease.entry_id,
                    owner=lease.owner,
                    generation=lease.generation,
                    marker_value="1",
                    successor_stream=wrong_type,
                    successor_payload=_successor_json(owned),
                    record_generation="gen-unwritten",
                )
            except redis.ResponseError:
                result = None
            finally:
                await h.async_redis.delete(wrong_type)

            assert result is None
            assert not await h.async_redis.exists(h.config.done_key(event_id))
            assert not await h.async_redis.exists(h.config.completion_key(event_id))
            assert not await h.async_redis.sismember(h.config.completions_pending_key(), event_id)
            assert not await h.kernel._markers.is_terminal(event_id)

    asyncio.run(go())


_PUBLISHED_KEY = "KEYS[7]"


def test_settle_fenced_and_publish_guards_match_settle_fenced() -> None:
    """Drift pin: the publishing script is the settle script plus a replay guard on
    the event's published marker (``KEYS[7]``), the XADD, and the one write of that
    marker, all after the two fence guards and before any write of the record."""
    plain = [line.strip() for line in markers_module._SETTLE_FENCED_LUA.strip().splitlines()]
    publishing = [
        line.strip() for line in markers_module._SETTLE_FENCED_AND_PUBLISH_LUA.strip().splitlines()
    ]
    guard_starts = [
        i for i, line in enumerate(publishing) if line.startswith("if ") and _PUBLISHED_KEY in line
    ]
    assert len(guard_starts) == 1, publishing
    guard = guard_starts[0]
    assert publishing[guard].endswith(" then")
    assert "KEYS[2]" not in publishing[guard], "the replay guard reads the clearable record"
    assert publishing[guard + 1 : guard + 3] == ["return 1", "end"]
    xadds = [i for i, line in enumerate(publishing) if "'XADD'" in line]
    assert len(xadds) == 1
    xadd = xadds[0]
    assert "KEYS[6]" in publishing[xadd]
    marker_writes = [
        i
        for i, line in enumerate(publishing)
        if _PUBLISHED_KEY in line and i != guard and line.startswith("redis.call('SET'")
    ]
    assert len(marker_writes) == 1, publishing
    marker_write = marker_writes[0]
    assert abs(marker_write - xadd) == 1, "the published marker is written beside the XADD"
    inserted = {guard, guard + 1, guard + 2, xadd, marker_write}

    assert [line for i, line in enumerate(publishing) if i not in inserted] == plain
    assert not any(_PUBLISHED_KEY in line for i, line in enumerate(publishing) if i not in inserted)
    # Both fence guards come first, before the replay guard and the publication.
    fences = [
        i
        for i, line in enumerate(publishing)
        if line.startswith("if redis.call(") and i not in inserted
    ]
    assert fences == [0, 1]
    assert guard > max(fences)
    assert min(xadd, marker_write) > guard + 2
    # The publication precedes every record write, so a failed XADD writes nothing.
    record_writes = [
        i
        for i, line in enumerate(publishing)
        if i not in inserted
        and any(f"redis.call('{verb}'" in line for verb in ("HDEL", "HSET", "SADD", "SET"))
    ]
    assert record_writes and max(xadd, marker_write) < min(record_writes)
    assert publishing[-1] == "return 1"


def test_replay_after_the_outbox_was_cleared_publishes_no_second_successor(
    make_harness,
) -> None:
    """Round-2 review: the sweeper clears the completion record once the outbox is
    delivered; a retried EVAL after that clear must still be recognised as a replay
    of the committed publication, not run as a fresh one."""

    async def go() -> None:
        async with make_harness() as h:
            owned = await _owned_entry(h)
            event_id = owned.event.event_id
            successor = _successor_json(owned)
            before = await _payloads(h)
            lease = owned.lease
            args: dict[str, Any] = {
                "stream": lease.stream,
                "group": lease.group,
                "entry_id": lease.entry_id,
                "owner": lease.owner,
                "generation": lease.generation,
                "marker_value": "1",
                "successor_stream": h.config.stream,
                "successor_payload": successor,
                "record_generation": "gen-cleared",
            }
            markers = h.kernel._markers

            assert (
                await markers.settle_fenced_and_publish(event_id, _record(event_id), **args)
                == "gen-cleared"
            )
            done_before = await h.async_redis.get(h.config.done_key(event_id))
            ttl_before = await h.async_redis.ttl(h.config.done_key(event_id))
            assert await markers.clear_completion(event_id, generation="gen-cleared")
            assert not await h.async_redis.exists(h.config.completion_key(event_id))

            replay = await markers.settle_fenced_and_publish(event_id, _record(event_id), **args)

            assert replay == "gen-cleared"
            assert (await _payloads(h))[len(before) :] == [successor]
            assert await h.async_redis.get(h.config.done_key(event_id)) == done_before
            assert await h.async_redis.ttl(h.config.done_key(event_id)) <= ttl_before

    asyncio.run(go())


def test_redelivered_predecessor_after_a_published_settle_is_dropped(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await enqueue(c.h, cron_event(c.run))

            async def save() -> None:
                c.checkpoint()

            await run_slice_to_budget_cut(c.h, first, at_cut=save)
            assert len(await successors(c.h)) == 1

            again = await redeliver(c.h, first)
            await c.h.kernel.process_event(again.event, lease=again.lease)

            assert len(await successors(c.h)) == 1
            assert c.h.runner.opened == [PROMPT]
            assert await run_outcome(c.run) is None
            assert reply_texts(c.h) == []

    asyncio.run(go())


def test_fence_lost_at_budget_cut_publishes_no_successor(make_hook_run, make_harness) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            first = await enqueue(c.h, cron_event(c.run))
            moved: list[bool] = []

            async def transfer(_request: Any) -> None:
                if not moved:
                    moved.append(True)
                    await bump_fence(c.h, first)

            async def save() -> None:
                c.checkpoint()
                c.api.on_request = transfer

            await run_slice_to_budget_cut(c.h, first, at_cut=save)

            assert moved, "the coverage read never happened"
            assert await successors(c.h) == []
            assert not await c.h.async_redis.exists(c.h.config.done_key(first.event.event_id))
            assert await run_outcome(c.run) is None
            assert notices(c.h) == []
            assert first.lease.lost.is_set()

    asyncio.run(go())


def test_leaseless_budget_cut_publishes_then_marks_done(make_harness) -> None:
    """A budget cut needs a lease, so production never takes this arm; a
    direct ``_complete`` caller holding no lease still gets XADD, then done."""

    async def go() -> None:
        async with make_harness() as h:
            predecessor = QueuedTurn(
                event_id=f"cron:{uuid.uuid4()}:sweep-hook:2026-09-22T03:00:00+00:00",
                conversation_id="hook-thread",
                author="cron:sweep-hook",
                text=PROMPT,
                reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder=None),
                received_at="2026-09-22T03:00:01+00:00",
            )
            successor = predecessor.model_copy(
                update={"event_id": f"{predecessor.event_id}:sweep:1:1:0"}
            )
            markers = h.kernel._markers
            original = markers.mark_done
            seen_at_mark_done: list[list[str]] = []

            async def mark_done(event_id: str, *, marker_value: Any) -> None:
                seen_at_mark_done.append([t.event_id for t in await stream_turns(h)])
                await original(event_id, marker_value=marker_value)

            markers.mark_done = mark_done

            await h.kernel._complete(
                predecessor,
                TargetRoute(),
                "dropped",
                telemetry_outcome="deadline_halted",
                lease=None,
                successor=successor,
            )

            assert [t.event_id for t in await stream_turns(h)] == [successor.event_id]
            assert seen_at_mark_done == [[successor.event_id]], "done before the XADD"
            assert await h.async_redis.exists(h.config.done_key(predecessor.event_id))

    asyncio.run(go())


def _lose_publish_reply(h: Any, *, committed: bool) -> list[bool]:
    """Make the first publishing settle lose its reply, after or before it runs."""
    script = markers_module._SETTLE_FENCED_AND_PUBLISH_LUA
    original = h.async_redis.eval
    lost: list[bool] = []

    async def eval_with_a_lost_reply(lua: str, numkeys: int, *args: Any) -> Any:
        if lua == script and not lost:
            lost.append(True)
            if committed:
                await original(lua, numkeys, *args)
            raise redis.ConnectionError("connection reset while reading the reply")
        return await original(lua, numkeys, *args)

    h.async_redis.eval = eval_with_a_lost_reply
    return lost


def test_committed_publication_with_a_lost_reply_continues_as_settled(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            lost = _lose_publish_reply(h, committed=True)
            first = await enqueue(h, cron_event(c.run))

            async def save() -> None:
                c.checkpoint()

            await run_slice_to_budget_cut(h, first, side_effect=True, at_cut=save)

            assert lost == [True]
            assert await h.async_redis.exists(h.config.done_key(first.event.event_id))
            assert [s.event_id for s in await successors(h)] == [
                f"{first.event.event_id}:sweep:1:1:0"
            ]
            assert await run_outcome(c.run) is None
            assert notices(h) == []

            c.checkpoint(covered=("slack", "github", "notes"), uncovered=())
            h.runner.hold = None
            h.runner.turn_scripts = [[Final(text="plan", status=SessionStatus.DONE)]]
            second = await read_next(h)
            assert second is not None
            await h.kernel.process_event(second.event, lease=second.lease)

            assert await run_outcome(c.run) == "ran"
            assert h.runner.opened == [PROMPT, PROMPT]
            assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(go())


def test_uncommitted_publication_with_a_lost_reply_redelivers_to_the_side_effect_stop(
    make_hook_run, make_harness
) -> None:
    async def go() -> None:
        async with sweep_case(make_hook_run, make_harness) as c:
            h = c.h
            lost = _lose_publish_reply(h, committed=False)
            first = await enqueue(h, cron_event(c.run))

            async def save() -> None:
                c.checkpoint()

            try:
                await run_slice_to_budget_cut(h, first, side_effect=True, at_cut=save)
            except redis.ConnectionError:
                pass
            else:
                raise AssertionError("an uncommitted publication was treated as settled")

            assert lost == [True]
            assert await run_outcome(c.run) is None, "closed a maybe-published slice"
            assert await successors(h) == []
            assert not await h.async_redis.exists(h.config.done_key(first.event.event_id))
            assert notices(h) == []

            again = await redeliver(h, first)
            assert again.lease.generation == 2
            await h.kernel.process_event(again.event, lease=again.lease)

            assert h.runner.opened == [PROMPT]
            assert await run_outcome(c.run) == "failed"
            texts = reply_texts(h)
            assert any("prior attempt started an action" in t for t in texts), texts
            assert [n.text for n in notices(h)] == [
                f'Scheduled sweep "{c.run.ref.name}" for 2026-09-21 stopped before it '
                "finished (run outcome: failed). Not covered: github, notes."
            ]
            assert texts[-1] == notices(h)[0].text
            assert await successors(h) == []

    asyncio.run(go())
