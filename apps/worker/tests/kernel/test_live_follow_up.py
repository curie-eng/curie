"""Route an ordinary Slack follow-up through the real consumer and runner."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_internal.turn_queue import to_stream_fields
from curie_runner.fake import FakeModelSession
from curie_runner.otel import RunTracer
from curie_runner.server import create_app
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from curie_runner.usage_report import UsageReporter
from curie_worker.consumer import Consumer, refuse_unconfigured_reminder
from curie_worker.delivery_lease import DeliveryLeaseStore

sys.path.insert(0, str(Path(__file__).parent.parent))
from queue_fixtures import qevent  # noqa: E402

_FIRST = "Start the summary"
_FOLLOW_UP = "Include the timeline"
_FIRST_ANSWER = "Here is the summary."
_FOLLOW_UP_ANSWER = "Here is the timeline."
_FOLDED = "Folded into the in-progress reply above."
_SENT = "Your follow-up was sent to the session."


@pytest.mark.parametrize("ordering", ["model-active", "result-received", "result-during-steer"])
def test_live_follow_up_is_answered(make_harness, thread_reset_keys, ordering: str) -> None:
    async def scenario() -> None:
        model_active = asyncio.Event()
        finish_model = asyncio.Event()
        reporting_result = asyncio.Event()
        finish_report = asyncio.Event()
        sending_steer = asyncio.Event()
        finish_sending_steer = asyncio.Event()
        steer_answered = asyncio.Event()
        steer_statuses: list[int] = []
        model_answers: list[str] = []

        class ControlledModel(FakeModelSession):
            async def query(self, text: str) -> None:
                if ordering == "result-during-steer" and _FOLLOW_UP in text:
                    sending_steer.set()
                    await finish_sending_steer.wait()
                await super().query(text)

            async def receive_turn(self) -> AsyncIterator[AssistantMessage | ResultMessage]:
                model_active.set()
                await finish_model.wait()
                answer = _FOLLOW_UP_ANSWER if _FOLLOW_UP in self.queries[-1] else _FIRST_ANSWER
                model_answers.append(answer)
                yield AssistantMessage(content=[TextBlock(text=answer)], model="example-model")
                yield ResultMessage(
                    subtype="success",
                    duration_ms=1,
                    duration_api_ms=1,
                    is_error=False,
                    num_turns=1,
                    session_id="example-session",
                    result=answer,
                    usage={"input_tokens": 10, "output_tokens": 5},
                )

        async def report_usage(request: web.Request) -> web.Response:
            await request.json()
            reporting_result.set()
            await finish_report.wait()
            return web.json_response({"ok": True})

        @web.middleware
        async def observe_steer(
            request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
        ) -> web.StreamResponse:
            response = await handler(request)
            if request.path == "/v1/steer" and model_active.is_set():
                steer_statuses.append(response.status)
                steer_answered.set()
            return response

        model = ControlledModel(lambda: [])
        usage_app = web.Application()
        usage_app.router.add_post("/usage", report_usage)
        async with TestServer(usage_app) as usage:
            runner = SessionRunner(
                session_factory=lambda: model,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="live-follow-up",
                ceiling=0,
                max_usd_per_day=None,
                held_secrets=frozenset(),
                usage_reporter=UsageReporter(str(usage.make_url("/usage")), "example-token"),
            )
            runner_app = create_app(runner)
            runner_app.middlewares.append(observe_steer)
            async with make_harness(runner_app=runner_app, shimmer=False) as h:
                first = qevent(_FIRST, channel="C0EXAMPLE1", placeholder="p-first")
                follow_up = qevent(_FOLLOW_UP, channel="C0EXAMPLE1", placeholder="p-follow-up")
                consumer = Consumer(
                    redis=h.async_redis,
                    kernel=h.kernel,
                    config=h.config,
                    leases=DeliveryLeaseStore(h.async_redis, h.config),
                    reminders=refuse_unconfigured_reminder,
                )
                await consumer.ensure_group()
                consuming = asyncio.create_task(consumer.run())
                try:
                    async with asyncio.timeout(8):
                        await h.async_redis.xadd(h.config.stream, to_stream_fields(first))
                        await model_active.wait()
                        if ordering == "result-received":
                            finish_model.set()
                            await reporting_result.wait()
                        await h.async_redis.xadd(h.config.stream, to_stream_fields(follow_up))
                        if ordering == "result-during-steer":
                            await sending_steer.wait()
                            finish_model.set()
                            await reporting_result.wait()
                            finish_sending_steer.set()
                        await steer_answered.wait()
                        finish_model.set()
                        finish_report.set()
                        while True:
                            pending = await h.async_redis.xpending(
                                h.config.stream, h.config.consumer_group
                            )
                            if (
                                not pending["pending"]
                                and await h.async_redis.exists(
                                    h.config.done_key(first.event_id),
                                    h.config.done_key(follow_up.event_id),
                                )
                                == 2
                            ):
                                break
                            await asyncio.sleep(0)
                finally:
                    finish_model.set()
                    finish_report.set()
                    finish_sending_steer.set()
                    consumer.request_stop()
                    await asyncio.wait_for(consuming, 10)

                # Both deliveries settled and the follow-up was submitted once.
                assert len(model.queries) == 2
                assert _FIRST in model.queries[0]
                assert _FOLLOW_UP in model.queries[1]
                updates = [(ref, text) for _, ref, text in h.sink.updates]
                if ordering == "result-received":
                    assert updates.count(("p-follow-up", _FOLLOW_UP_ANSWER)) == 1, updates
                    assert updates.count(("p-first", _FIRST_ANSWER)) == 1, updates
                    assert ("p-follow-up", _FOLDED) not in updates
                    assert ("p-follow-up", _SENT) not in updates
                    assert model_answers == [_FIRST_ANSWER, _FOLLOW_UP_ANSWER]
                    assert steer_statuses[0] == 409
                    assert 200 not in steer_statuses
                elif ordering == "result-during-steer":
                    assert updates.count(("p-follow-up", _SENT)) == 1, updates
                    assert updates.count(("p-first", _FIRST_ANSWER)) == 1, updates
                    assert ("p-follow-up", _FOLDED) not in updates
                    assert model_answers == [_FIRST_ANSWER]
                    assert steer_statuses == [200]
                else:
                    assert updates.count(("p-first", _FOLLOW_UP_ANSWER)) == 1, updates
                    assert ("p-follow-up", _FOLLOW_UP_ANSWER) not in updates
                    assert model_answers == [_FOLLOW_UP_ANSWER]
                    assert steer_statuses == [200]

    asyncio.run(scenario())
