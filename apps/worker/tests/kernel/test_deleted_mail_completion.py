"""Deleted provider threads settle the actual completion outbox, not only the PEL."""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import threading
from contextlib import contextmanager
from pathlib import Path

import pytest
from aci_protocol import Final, SessionStatus
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.config import MailAdapterConfig
from curie_mail_adapter.egress import make_server
from curie_worker.consumer import Consumer
from curie_worker.markers import Markers
from curie_worker.reply_sink import HttpReplyAdapter

from .test_completion_outbox import (
    _dispatch_and_settle,
    _qevent,
    _read_one,
    _record,
)

# Reuse the package's documented HTTP provider simulator. Nothing in the
# adapter, sink, kernel, or Valkey is mocked.
_SUPPORT_PATH = Path(__file__).resolve().parents[3] / "mail-adapter/tests/_support.py"
_spec = importlib.util.spec_from_file_location("mail_completion_support", _SUPPORT_PATH)
assert _spec and _spec.loader
support = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(support)


@contextmanager
def mail_runtime(tmp_path, *, provider_base_url: str | None):
    mail = support.MailState()
    ingress = support.IngressState()
    provider = support.serve(support.MailHandler, mail)
    api = support.serve(support.IngressHandler, ingress)
    adapter = MailAdapter(
        MailAdapterConfig(
            agentmail_api_key=support.AGENTMAIL_API_KEY,
            agentmail_inbox=support.INBOX,
            agentmail_base_url=f"http://127.0.0.1:{provider.server_port}/v0",
            api_base_url=f"http://127.0.0.1:{api.server_port}",
            channel_token=support.CHANNEL_TOKEN,
            egress_secret=support.EGRESS_SECRET,
            ingress_enabled=True,
            allowed_senders=(support.ALLOWED_SENDER,),
            state_path=str(tmp_path / "mail.sqlite3"),
        )
    )
    egress = None
    try:
        mail.add_inbound("msg_upstream", "th-1")
        adapter.poll_once()
        assert len(ingress.requests) == 1
        if provider_base_url is not None:
            adapter.close()
            adapter = MailAdapter(
                adapter.config.model_copy(update={"agentmail_base_url": provider_base_url})
            )
        egress = make_server(adapter, 0)
        threading.Thread(target=egress.serve_forever, daemon=True).start()
        yield mail, f"http://127.0.0.1:{egress.server_port}/"
    finally:
        for server in (egress, api, provider):
            if server is None:
                continue
            server.shutdown()
            server.server_close()
        adapter.close()


@pytest.mark.parametrize("deleted", [True, False])
def test_admitted_mail_completion_dead_letters_deleted_thread_only(
    make_harness,
    tmp_path,
    caplog: pytest.LogCaptureFixture,
    deleted: bool,
) -> None:
    # AgentMail Get Thread 404 semantics, also observed in #2376:
    # https://docs.agentmail.to/api-reference/inboxes/threads/get
    async def go() -> None:
        with mail_runtime(tmp_path, provider_base_url=None) as (mail, endpoint):
            sink = HttpReplyAdapter({"acme-mail": support.EGRESS_SECRET})
            try:
                async with make_harness(
                    shimmer=False,
                    completion_sweep_grace_s=0.0,
                    sink=sink,
                ) as h:
                    consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
                    await consumer.ensure_group()
                    qe = _qevent(event_id="deleted-completion").model_copy(
                        update={
                            "reply_handle": _qevent().reply_handle.model_copy(
                                update={
                                    "channel": support.INBOX,
                                    "endpoint": endpoint,
                                    "adapter": "acme-mail",
                                }
                            ),
                        }
                    )
                    h.runner.default_script = [Final(text="answer", status=SessionStatus.DONE)]
                    if deleted:
                        mail.deleted_threads.add("th-1")
                    else:
                        mail.fail_next_thread = 500
                    from curie_dispatcher.queue import to_stream_fields

                    await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
                    entry, fields = await _read_one(h, h.config.consumer_name)
                    await _dispatch_and_settle(consumer, entry, fields)
                    assert (await h.async_redis.xpending(h.config.stream, h.config.consumer_group))[
                        "pending"
                    ] == 0
                    pending = await h.async_redis.smembers(h.config.completions_pending_key())
                    rows = await h.async_redis.xrange(h.config.dead_letter_stream_name())
                    if deleted:
                        assert pending == set()
                        assert len(rows) == 1
                        assert rows[0][1]["dl_reason"] == "thread deleted at provider"
                        assert rows[0][1]["event_id"] == qe.event_id
                        assert rows[0][1]["dl_delivery_count"] == "1"
                    else:
                        assert pending == {qe.event_id}
                        assert rows == []
                    await h.kernel.sweep_pending_completions()
                    await h.kernel.sweep_pending_completions()
                    assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()
                    assert mail.thread_calls == (1 if deleted else 2)
                    assert len(mail.replies) == (0 if deleted else 1)
                    assert len(await h.async_redis.xrange(h.config.dead_letter_stream_name())) == (
                        1 if deleted else 0
                    )
            finally:
                await sink.aclose()

    with caplog.at_level(logging.WARNING):
        asyncio.run(go())
    lines = [
        r.getMessage()
        for r in caplog.records
        if r.name == "curie_mail_adapter.adapter" and "thread deleted at provider" in r.getMessage()
    ]
    assert len(lines) == (1 if deleted else 0)


@pytest.mark.parametrize("refusal_stage", ["thread", "reply"])
def test_refused_provider_connection_persists_completion_cause(
    make_harness, tmp_path, refusal_stage: str
) -> None:
    # #2824 and #2731 observed ECONNREFUSED from provider egress in the cluster.
    async def go() -> None:
        provider = (
            support.refused_agentmail_connection()
            if refusal_stage == "thread"
            else support.refused_agentmail_reply()
        )
        with provider as provider_url:
            with mail_runtime(tmp_path, provider_base_url=provider_url) as (mail, endpoint):
                sink = HttpReplyAdapter({"acme-mail": support.EGRESS_SECRET})
                try:
                    async with make_harness(
                        shimmer=False,
                        completion_sweep_grace_s=0.0,
                        sink=sink,
                    ) as h:
                        consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
                        await consumer.ensure_group()
                        qe = _qevent(event_id=f"refused-{refusal_stage}").model_copy(
                            update={
                                "reply_handle": _qevent().reply_handle.model_copy(
                                    update={
                                        "channel": support.INBOX,
                                        "endpoint": endpoint,
                                        "adapter": "acme-mail",
                                    }
                                ),
                            }
                        )
                        h.runner.default_script = [Final(text="answer", status=SessionStatus.DONE)]
                        from curie_dispatcher.queue import to_stream_fields

                        await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
                        entry, fields = await _read_one(h, h.config.consumer_name)
                        await _dispatch_and_settle(consumer, entry, fields)

                        assert (
                            await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
                        )["pending"] == 0
                        assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                            qe.event_id
                        }
                        assert await h.async_redis.hget(
                            h.config.completion_key(qe.event_id), "cause"
                        ) == "provider egress refused"
                        assert await Markers(h.async_redis, h.config).read_completion(qe.event_id)
                        assert await h.async_redis.xrange(h.config.dead_letter_stream_name()) == []
                        assert mail.replies == []
                finally:
                    await sink.aclose()

    asyncio.run(go())


def test_dead_letter_completion_refuses_stale_generation(make_harness) -> None:
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            markers = Markers(h.async_redis, h.config)
            old = _record("stale-deleted", done=True)
            stale = await markers.mark_completion_pending(old.event_id, old)
            current = await markers.mark_completion_pending(old.event_id, old)
            assert not await markers.dead_letter_completion(
                old, generation=stale, reason="thread deleted at provider"
            )
            assert await h.async_redis.xrange(h.config.dead_letter_stream_name()) == []
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                old.event_id
            }
            assert await markers.dead_letter_completion(
                old, generation=current, reason="thread deleted at provider"
            )
            assert not await markers.dead_letter_completion(
                old, generation=current, reason="thread deleted at provider"
            )
            assert len(await h.async_redis.xrange(h.config.dead_letter_stream_name())) == 1
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == set()

    asyncio.run(go())


def test_egress_refusal_cause_refuses_stale_generation(make_harness) -> None:
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            markers = Markers(h.async_redis, h.config)
            record = _record("stale-refusal", done=True)
            stale = await markers.mark_completion_pending(record.event_id, record)
            current = await markers.mark_completion_pending(record.event_id, record)
            assert not await markers.note_provider_egress_refusal(
                record.event_id, generation=stale
            )
            assert (
                await h.async_redis.hget(h.config.completion_key(record.event_id), "cause")
                is None
            )
            assert await markers.note_provider_egress_refusal(
                record.event_id, generation=current
            )
            stored = await markers.read_completion(record.event_id)
            assert stored is not None
            assert stored.cause == "provider egress refused"

    asyncio.run(go())


def test_fenced_settle_clears_cause_only_for_current_owner(make_harness) -> None:
    from curie_dispatcher.queue import to_stream_fields
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            markers = Markers(h.async_redis, h.config)
            leases = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
            await consumer.ensure_group()

            event_id = "fenced-refusal"
            await h.async_redis.xadd(
                h.config.stream, to_stream_fields(_qevent(event_id=event_id))
            )
            entry_id, _fields = await _read_one(h, h.config.consumer_name)
            stale = await leases.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            assert await leases.release(
                h.config.stream, h.config.consumer_group, entry_id, owner=stale.owner
            )
            current = await leases.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )

            first_generation = await markers.mark_completion_pending(
                event_id, _record(event_id, done=False)
            )
            assert await markers.note_provider_egress_refusal(
                event_id, generation=first_generation
            )
            replacement = _record(event_id, done=True)
            assert await markers.settle_fenced(
                event_id,
                replacement,
                stream=h.config.stream,
                group=h.config.consumer_group,
                entry_id=entry_id,
                owner=stale.owner,
                generation=stale.generation,
            ) is None
            stored = await markers.read_completion(event_id)
            assert stored is not None
            assert stored.generation == first_generation
            assert stored.cause == "provider egress refused"
            assert not stored.done_flag
            assert not await h.async_redis.exists(h.config.done_key(event_id))

            new_generation = await markers.settle_fenced(
                event_id,
                replacement,
                stream=h.config.stream,
                group=h.config.consumer_group,
                entry_id=entry_id,
                owner=current.owner,
                generation=current.generation,
            )
            assert new_generation is not None and new_generation != first_generation
            stored = await markers.read_completion(event_id)
            assert stored is not None
            assert stored.generation == new_generation
            assert stored.cause is None
            assert stored.done_flag
            assert await h.async_redis.exists(h.config.done_key(event_id))

    asyncio.run(go())


def test_generic_retry_failure_clears_refusal_cause_without_clearing_completion(
    make_harness,
) -> None:
    from curie_dispatcher.queue import to_stream_fields

    async def go() -> None:
        completion_calls = 0

        async def adapter_reply(request: web.Request) -> web.Response:
            nonlocal completion_calls
            event = await request.json()
            if event["event"] != "turn.completed":
                return web.json_response({"ref": None})
            completion_calls += 1
            if completion_calls == 1:
                return web.json_response({"detail": "provider egress refused"}, status=424)
            return web.json_response({"detail": "adapter error"}, status=502)

        app = web.Application()
        app.router.add_post("/adapter", adapter_reply)
        server = TestServer(app)
        await server.start_server()
        sink = HttpReplyAdapter({"acme-mail": support.EGRESS_SECRET})
        try:
            async with make_harness(
                shimmer=False,
                completion_sweep_grace_s=0.0,
                sink=sink,
            ) as h:
                consumer = Consumer(redis=h.async_redis, kernel=h.kernel, config=h.config)
                await consumer.ensure_group()
                qe = _qevent(event_id="refusal-then-fault").model_copy(
                    update={
                        "reply_handle": _qevent().reply_handle.model_copy(
                            update={
                                "endpoint": f"http://127.0.0.1:{server.port}/adapter",
                                "adapter": "acme-mail",
                            }
                        ),
                    }
                )
                h.runner.default_script = [Final(text="answer", status=SessionStatus.DONE)]
                await h.async_redis.xadd(h.config.stream, to_stream_fields(qe))
                entry, fields = await _read_one(h, h.config.consumer_name)
                await _dispatch_and_settle(consumer, entry, fields)

                markers = Markers(h.async_redis, h.config)
                before = await markers.read_completion(qe.event_id)
                assert before is not None
                assert before.cause == "provider egress refused"
                assert completion_calls == 1

                await h.kernel.sweep_pending_completions()

                after = await markers.read_completion(qe.event_id)
                assert after is not None
                assert after.generation == before.generation
                assert after.cause is None
                assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                    qe.event_id
                }
                assert await h.async_redis.xrange(h.config.dead_letter_stream_name()) == []
                assert completion_calls == 2
        finally:
            await sink.aclose()
            await server.close()

    asyncio.run(go())


def test_graveyard_write_failure_keeps_completion_owed(make_harness) -> None:
    from redis.exceptions import ResponseError

    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            markers = Markers(h.async_redis, h.config)
            record = _record("failed-graveyard", done=True)
            generation = await markers.mark_completion_pending(record.event_id, record)
            await h.async_redis.set(h.config.dead_letter_stream_name(), "wrong-type")
            with pytest.raises(ResponseError, match="WRONGTYPE"):
                await markers.dead_letter_completion(
                    record,
                    generation=generation,
                    reason="thread deleted at provider",
                )
            assert await h.async_redis.smembers(h.config.completions_pending_key()) == {
                record.event_id
            }
            assert await markers.read_completion(record.event_id) is not None
            await h.async_redis.delete(h.config.dead_letter_stream_name())
            assert await markers.dead_letter_completion(
                record,
                generation=generation,
                reason="thread deleted at provider",
            )

    asyncio.run(go())
