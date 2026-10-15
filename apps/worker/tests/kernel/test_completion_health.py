"""Completion-outbox health is a separate plane from run-queue lag (#2422)."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import pytest
from curie_worker.markers import Markers
from redis.exceptions import ResponseError

from .test_completion_outbox import _record
from .test_otel_runtime import _install, _metrics


def _health() -> tuple[Any, Any, Any]:
    """Import the observer at test time so reverse-pin collection still succeeds."""

    try:
        from curie_worker import completion_health as completion_health_module
        from curie_worker.completion_health import (
            observe_completion_outbox,
            snapshot_completion_outbox,
        )
    except ImportError:
        pytest.fail("curie_worker.completion_health is missing")
    return completion_health_module, observe_completion_outbox, snapshot_completion_outbox


async def _group_pending_lag(h) -> tuple[int, int]:
    try:
        await h.async_redis.xgroup_create(
            h.config.stream, h.config.consumer_group, id="0", mkstream=True
        )
    except ResponseError:
        pass
    pending = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
    lag = 0
    for group in await h.async_redis.xinfo_groups(h.config.stream):
        name = group.get("name")
        if isinstance(name, bytes):
            name = name.decode()
        if name == h.config.consumer_group:
            lag = int(group.get("lag") or 0)
    return int(pending["pending"]), lag


def test_empty_outbox_is_not_degraded_while_the_run_queue_is_drained(make_harness) -> None:
    async def go() -> None:
        _module, _observe, snapshot_completion_outbox = _health()
        async with make_harness(shimmer=False) as h:
            markers = Markers(h.async_redis, h.config)
            snap = await snapshot_completion_outbox(markers, h.async_redis, h.config)
            pending, lag = await _group_pending_lag(h)
            assert (pending, lag) == (0, 0)
            assert snap.count == 0
            assert snap.retry == 0
            assert snap.degraded is False
            assert snap.state == "empty"
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()

    asyncio.run(go())


def test_inflight_completion_inside_grace_is_not_delivery_degraded(make_harness) -> None:
    async def go() -> None:
        _module, _observe, snapshot_completion_outbox = _health()
        async with make_harness(shimmer=False, completion_sweep_grace_s=60.0) as h:
            markers = Markers(h.async_redis, h.config)
            record = _record("inflight-1", done=True, age_s=5.0)
            await markers.mark_completion_pending(record.event_id, record)
            snap = await snapshot_completion_outbox(markers, h.async_redis, h.config)
            pending, lag = await _group_pending_lag(h)
            assert (pending, lag) == (0, 0)
            assert snap.count == 1
            assert snap.inflight == 1
            assert snap.retry == 0
            assert snap.degraded is False
            assert snap.state == "inflight"
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                record.event_id
            }

    asyncio.run(go())


def test_owed_completion_with_drained_queue_is_delivery_degraded_and_metrics_change(
    make_harness,
    monkeypatch,
) -> None:
    async def go() -> None:
        completion_health_module, observe_completion_outbox, _snapshot = _health()
        probe = _install(monkeypatch)
        monkeypatch.setattr(completion_health_module, "record_metric", probe.record_metric)
        async with make_harness(shimmer=False, completion_sweep_grace_s=60.0) as h:
            markers = Markers(h.async_redis, h.config)
            empty = await observe_completion_outbox(markers, h.async_redis, h.config)
            assert empty.degraded is False
            empty_retry = [
                point.value
                for point in _metrics(probe, "curie.completion.outbox")
                if point.attributes.get("outcome") == "retry"
            ]

            record = _record("owed-1", done=True, age_s=120.0)
            generation = await markers.mark_completion_pending(record.event_id, record)
            snap = await observe_completion_outbox(markers, h.async_redis, h.config)
            pending, lag = await _group_pending_lag(h)
            assert (pending, lag) == (0, 0)
            assert snap.count == 1
            assert snap.retry == 1
            assert snap.degraded is True
            assert snap.state == "retry"
            assert snap.oldest_age_s >= 120.0
            payload = json.dumps(snap.to_json())
            assert "owed-1" not in payload
            assert "event_id" not in snap.to_json()
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                record.event_id
            }

            owed_retry = [
                point.value
                for point in _metrics(probe, "curie.completion.outbox")
                if point.attributes.get("outcome") == "retry"
            ]
            assert empty_retry[-1] == 0
            assert owed_retry[-1] == 1
            ages = [
                point.value
                for point in _metrics(probe, "curie.completion.outbox.age")
                if point.attributes.get("outcome") == "retry"
            ]
            assert ages[-1] >= 120.0
            for point in probe.metrics:
                assert "event_id" not in point.attributes
                assert "session" not in point.attributes
                assert "run" not in point.attributes

            await markers.clear_completion(record.event_id, generation=generation)
            restored = await observe_completion_outbox(markers, h.async_redis, h.config)
            assert restored.degraded is False
            assert restored.count == 0
            assert restored.state == "empty"
            restored_retry = [
                point.value
                for point in _metrics(probe, "curie.completion.outbox")
                if point.attributes.get("outcome") == "retry"
            ]
            assert restored_retry[-1] == 0

    asyncio.run(go())


def test_observer_does_not_clear_or_relabel_an_owed_completion(make_harness) -> None:
    async def go() -> None:
        _module, _observe, snapshot_completion_outbox = _health()
        async with make_harness(shimmer=False, completion_sweep_grace_s=1.0) as h:
            markers = Markers(h.async_redis, h.config)
            record = _record("keep-owed", done=True, age_s=30.0)
            generation = await markers.mark_completion_pending(record.event_id, record)
            before = await markers.read_completion(record.event_id)
            assert before is not None
            snap = await snapshot_completion_outbox(markers, h.async_redis, h.config)
            assert snap.degraded is True
            after = await markers.read_completion(record.event_id)
            assert after is not None
            assert after.generation == generation == before.generation
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                record.event_id
            }

    asyncio.run(go())


def test_stale_generation_clear_cannot_delete_a_newer_record(make_harness) -> None:
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            markers = Markers(h.async_redis, h.config)
            first = _record("gen-fence", done=True, age_s=90.0)
            stale = await markers.mark_completion_pending(first.event_id, first)
            newer = _record("gen-fence", done=True, age_s=90.0)
            current = await markers.mark_completion_pending(newer.event_id, newer)
            assert stale != current
            assert not await markers.clear_completion(first.event_id, generation=stale)
            stored = await markers.read_completion(first.event_id)
            assert stored is not None
            assert stored.generation == current
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                first.event_id
            }
            assert await markers.clear_completion(first.event_id, generation=current)
            assert await markers.read_completion(first.event_id) is None

    asyncio.run(go())


def test_json_status_reader_omits_run_and_session_identifiers(make_harness, caplog) -> None:
    async def go() -> None:
        _module, _observe, snapshot_completion_outbox = _health()
        async with make_harness(shimmer=False, completion_sweep_grace_s=1.0) as h:
            markers = Markers(h.async_redis, h.config)
            record = _record("secret-event", done=True, age_s=30.0)
            await markers.mark_completion_pending(record.event_id, record)
            snap = await snapshot_completion_outbox(markers, h.async_redis, h.config)
            rendered = json.dumps(snap.to_json())
            assert "secret-event" not in rendered
            assert "th-1" not in rendered

    with caplog.at_level(logging.INFO):
        asyncio.run(go())
    for record in caplog.records:
        assert "secret-event" not in record.getMessage()


def test_answer_namespace_is_counted_by_the_real_outbox_observer(make_harness) -> None:
    from channel_protocol.reply import PROGRESS_REPLY_WIRE_VERSION, ReplyUpdate

    async def go() -> None:
        _module, _observe, snapshot = _health()
        async with make_harness(shimmer=False, completion_sweep_grace_s=0.0) as h:
            record = _record("owed-answer-health", age_s=120)
            record.answer = ReplyUpdate(
                version=PROGRESS_REPLY_WIRE_VERSION, event="reply.update",
                target=record.event.target, text="synthetic answer",
                delivery_id="00000000-0000-4000-8000-000000000075",
            )
            await h.kernel._markers.mark_completion_pending(record.event_id, record)
            result = await snapshot(h.kernel._markers, h.async_redis, h.config)
            assert result.count == result.retry == 1
            assert result.degraded and result.state == "retry"
            assert result.oldest_age_s >= 120
    asyncio.run(go())
