"""The runner-unreachable window at turn start (#4733).

A thread's sandbox pod recreated between turns leaves a retained route that
refuses connections until the replacement runner listens. A connect-phase
turn-start failure therefore retries for one window per delivery, the sandbox
claim timeout long, without consuming ``max_attempts``. Every connect failure
here is real: a loopback port with no listener, never a patched exception.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import AsyncExitStack, contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock
from uuid import uuid4

from aci_protocol import Attachment, Final, SessionStatus, TurnSource
from aiohttp import web
from channel_protocol.reply import ReplyUpdate
from curie_worker.kernel import clock as kernel_clock
from curie_worker.kernel import routing as kernel_routing
from curie_worker.kernel.failures import failure_class_from_reply

sys.path.insert(0, str(Path(__file__).parent.parent))
from queue_fixtures import qevent  # noqa: E402

_WIDE_WINDOW_S = 3.0
_NOT_WAITED_S = 1.0


class _Records(logging.Handler):
    """Every WARNING the worker logs, stamped on the monotonic clock.

    A test awaits a predicate over the records instead of sleeping: each record
    re-checks the waiters and wakes the ones it satisfies.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        super().__init__(logging.WARNING)
        self._loop = loop
        self.records: list[tuple[float, str]] = []
        self._waiters: list[tuple[Callable[[], bool], asyncio.Event]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append((time.monotonic(), record.getMessage()))
        for predicate, event in list(self._waiters):
            if predicate():
                self._loop.call_soon_threadsafe(event.set)

    async def wait(self, predicate: Callable[[], bool], what: str, timeout: float = 5.0) -> None:
        if predicate():
            return
        entry = (predicate, asyncio.Event())
        self._waiters.append(entry)
        try:
            await asyncio.wait_for(entry[1].wait(), timeout)
        except TimeoutError:
            raise AssertionError(f"timed out after {timeout}s waiting for: {what}") from None
        finally:
            self._waiters.remove(entry)

    def failures(self, event_id: str) -> list[float]:
        return [at for at, _message in self.failure_records(event_id)]

    def failure_records(self, event_id: str) -> list[tuple[float, str]]:
        prefix = f"turn start failed for {event_id}:"
        return [(at, message) for at, message in self.records if message.startswith(prefix)]

    def assert_connect_failures(self, event_id: str) -> None:
        # Every failure was a refused connect, never a dropped connection.
        messages = [message for _at, message in self.failure_records(event_id)]
        assert messages, f"no turn start failure for {event_id}"
        assert all("ClientConnectorError" in message for message in messages), messages

    def windows(self, event_id: str) -> list[tuple[float, str]]:
        prefix = f"runner unreachable at turn start for event {event_id}"
        return [(at, message) for at, message in self.records if message.startswith(prefix)]


@contextmanager
def _recording() -> Iterator[_Records]:
    records = _Records(asyncio.get_running_loop())
    root = logging.getLogger()
    root.addHandler(records)
    try:
        yield records
    finally:
        root.removeHandler(records)


class _ReopenableRunner:
    """A fake runner on a fixed loopback port whose listener can close and reopen.

    Every response closes its connection, so each request dials anew and a
    closed listener is a refused connect, as with a recreated pod's address.
    """

    def __init__(self, make_harness: Any) -> None:
        self.runner = make_harness.__globals__["FakeRunner"]()
        self.port: int = make_harness.__globals__["_closed_port"]()
        # When set, the next status read closes the listener after answering.
        self.close_after_status = False
        self.runner.app.on_response_prepare.append(self._prepare)
        self._web = web.AppRunner(self.runner.app, shutdown_timeout=1.0)

    async def _prepare(self, request: web.Request, response: web.StreamResponse) -> None:
        # The signal fires after aiohttp built the keep-alive headers and
        # before it writes them, so the header is set here as well as the
        # server-side close: the client must not pool this connection.
        response.force_close()
        response.headers["Connection"] = "close"
        if self.close_after_status and request.path in {"/status", "/v1/status"}:
            self.close_after_status = False
            await self.close()

    async def start(self) -> None:
        await self._web.setup()
        await self.open()

    async def open(self) -> None:
        await web.TCPSite(self._web, "127.0.0.1", self.port).start()

    async def close(self) -> None:
        for site in list(self._web.sites):
            await site.stop()

    async def cleanup(self) -> None:
        await self._web.cleanup()


def _route_sandboxes_to(h: Any, port: int) -> None:
    # Every sandbox this test claims dials ``port``. Repeated because the pool
    # refuses to hand out more ports than it holds.
    h.fake_k8s.runner_ports = [port] * 16


def _completion(h: Any, event_id: str) -> list[str]:
    return [c.outcome for c in h.sink.completions if c.event_id == event_id]


def _counted_attempts(text: str | None) -> int:
    assert text is not None
    found = re.search(r"\(runner-error\) after (\d+) attempt\(s\)", text)
    assert found is not None, text
    return int(found.group(1))


def test_eager_handoff_attempt_opens_no_window(make_harness, monkeypatch) -> None:
    from .test_attachment_claim import _FakeAttachmentLane, _HistoryBinding, _WorkspaceProbe

    max_attempts = 3
    window_attempts = 5

    async def go() -> None:
        binding = _HistoryBinding(uuid4(), workspace_enabled=False)
        async with (
            make_harness(
                binding=binding,
                attachment_enabled=True,
                attachment_mode="eager",
                max_attempts=max_attempts,
                claim_timeout_seconds=float(window_attempts),
            ) as h,
            AsyncExitStack() as stack,
        ):
            h.kernel._attachments = _FakeAttachmentLane()
            h.kernel._workspace = _WorkspaceProbe(h.substrate, selected_repo=None)
            runners = []
            for _ in range(1 + window_attempts + max_attempts):
                runner = _ReopenableRunner(make_harness)
                stack.push_async_callback(runner.cleanup)
                await runner.start()
                runners.append(runner)
            h.fake_k8s.runner_ports = [runner.port for runner in runners]
            for runner in runners[1:]:
                await runner.close()

            thread = "tEagerHandoff"
            await h.kernel.process_event(qevent("first", thread=thread))
            assert runners[0].runner.opened == ["first"]

            attempts = 0
            emit = h.sink.emit

            async def emit_with_runner_boot(reply, **kwargs):
                nonlocal attempts
                if isinstance(reply, ReplyUpdate) and reply.text == h.config.booting_text:
                    # The replacement starts listening before it becomes the next
                    # attempt's retained runner. The next replacement is still closed.
                    if attempts:
                        await runners[attempts].open()
                    attempts += 1
                return await emit(reply, **kwargs)

            monkeypatch.setattr(h.sink, "emit", emit_with_runner_boot)
            # Advance one second per attempt so the old loop can make five
            # window attempts without depending on CI wall time.
            monkeypatch.setattr(
                kernel_clock, "time", SimpleNamespace(monotonic=lambda: attempts, time=time.time)
            )
            handoff = Mock(wraps=h.substrate.handoff)
            monkeypatch.setattr(h.substrate, "handoff", handoff)
            event = qevent(
                "read the file",
                thread=thread,
                attachments=[Attachment(id="F2", name="report.csv", mime_type="text/csv")],
            )
            with _recording() as records:
                await h.kernel.process_event(event)

            records.assert_connect_failures(event.event_id)
            assert handoff.call_count == max_attempts
            assert len(records.failures(event.event_id)) == max_attempts
            assert records.windows(event.event_id) == []
            for runner in runners[1:max_attempts]:
                assert len(runner.runner.status_headers) == 2
                assert all(
                    headers.get("Authorization") == "Bearer workspace-test-token"
                    for headers in runner.runner.status_headers
                )
            assert _completion(h, event.event_id) == ["escalated"]
            assert failure_class_from_reply(h.sink.last_text) == "runner-error"
            assert _counted_attempts(h.sink.last_text) == max_attempts

    asyncio.run(go())


def test_runner_reachable_inside_window_is_answered_in_same_delivery(make_harness) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=3, claim_timeout_seconds=1.5) as h:
            runner = _ReopenableRunner(make_harness)
            await runner.start()
            try:
                _route_sandboxes_to(h, runner.port)
                with _recording() as records:
                    await h.kernel.process_event(qevent("first", thread="tReopen"))
                    assert runner.runner.opened == ["first"]

                    # The pod is recreated: the retained route now refuses.
                    await runner.close()
                    runner.runner.default_script = [
                        Final(text="answered", status=SessionStatus.DONE)
                    ]
                    second = qevent("second", thread="tReopen")
                    task = asyncio.create_task(h.kernel.process_event(second))
                    await records.wait(
                        lambda: len(records.failures(second.event_id)) > h.config.max_attempts,
                        "more than max_attempts refused attempts",
                    )
                    assert not task.done()
                    await runner.open()
                    await asyncio.wait_for(task, 5.0)

                    assert len(records.failures(second.event_id)) > h.config.max_attempts
                    records.assert_connect_failures(second.event_id)
                    assert len(records.windows(second.event_id)) == 1
                    assert runner.runner.opened == ["first", "second"]
                    assert h.sink.last_text == "answered"
                    assert _completion(h, second.event_id) == ["delivered"]
            finally:
                await runner.cleanup()

    asyncio.run(go())


def test_never_reachable_runner_escalates_once_after_window(make_harness) -> None:
    window_s = 0.5

    async def go() -> None:
        async with make_harness(
            max_attempts=3,
            claim_timeout_seconds=window_s,
            retry_backoff_base_s=0.1,
            retry_backoff_max_s=0.2,
        ) as h:
            _route_sandboxes_to(h, make_harness.__globals__["_closed_port"]())
            event = qevent("never", thread="tNeverReachable")
            with _recording() as records:
                started = time.monotonic()
                await h.kernel.process_event(event)
                elapsed = time.monotonic() - started

            windows = records.windows(event.event_id)
            assert len(windows) == 1
            opened_at, message = windows[0]
            assert f"{window_s:.1f}s" in message
            failures = records.failures(event.event_id)
            records.assert_connect_failures(event.event_id)
            after_window = [at for at in failures if at >= opened_at + window_s]
            assert len(after_window) == h.config.max_attempts, failures
            assert elapsed >= window_s
            assert _completion(h, event.event_id) == ["escalated"]
            assert _counted_attempts(h.sink.last_text) == len(failures)

    asyncio.run(go())


def test_window_wait_holds_no_thread_lock_and_second_delivery_proceeds(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            max_attempts=3,
            claim_timeout_seconds=1.5,
            retry_backoff_base_s=0.2,
            retry_backoff_max_s=0.2,
        ) as h:
            runner = _ReopenableRunner(make_harness)
            await runner.start()
            try:
                _route_sandboxes_to(h, runner.port)
                with _recording() as records:
                    await h.kernel.process_event(qevent("first", thread="tNoLock"))
                    assert runner.runner.opened == ["first"]
                    await runner.close()

                    waiting = qevent("waiting", thread="tNoLock")
                    lock_key = h.config.lock_key(kernel_routing._thread_key_for(waiting))
                    first = asyncio.create_task(h.kernel.process_event(waiting))
                    for count in (1, 2):
                        await records.wait(
                            lambda n=count: len(records.failures(waiting.event_id)) >= n,
                            f"refused attempt {count} of the waiting delivery",
                        )
                        assert len(records.windows(waiting.event_id)) == 1
                        assert await h.async_redis.exists(lock_key) == 0

                    follow_up = qevent("follow-up", thread="tNoLock")
                    second = asyncio.create_task(h.kernel.process_event(follow_up))
                    await records.wait(
                        lambda: len(records.failures(follow_up.event_id)) >= 1,
                        "the second delivery's own attempt",
                    )
                    assert not first.done(), "the second delivery waited for the window"

                    await runner.open()
                    await asyncio.wait_for(asyncio.gather(first, second), 5.0)

                    records.assert_connect_failures(waiting.event_id)
                    records.assert_connect_failures(follow_up.event_id)
                    # Once the runner listens again one delivery opens a turn
                    # and the other either opens its own or steers into it;
                    # which one wins is a race. The kernel guarantees each
                    # settles exactly once and neither escalates.
                    for event in (waiting, follow_up):
                        outcomes = _completion(h, event.event_id)
                        assert len(outcomes) == 1, outcomes
                        assert outcomes != ["escalated"], outcomes
            finally:
                await runner.cleanup()

    asyncio.run(go())


def test_runner_5xx_escalates_after_max_attempts_with_no_window(make_harness) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=3, claim_timeout_seconds=_WIDE_WINDOW_S) as h:
            h.runner.event_fail_times = 100
            event = qevent("five hundred", thread="t5xx")
            with _recording() as records:
                started = time.monotonic()
                await h.kernel.process_event(event)
                elapsed = time.monotonic() - started

            assert len(h.runner.opened) == h.config.max_attempts
            assert len(records.failures(event.event_id)) == h.config.max_attempts
            assert records.windows(event.event_id) == []
            assert elapsed < _NOT_WAITED_S
            assert _completion(h, event.event_id) == ["escalated"]
            assert _counted_attempts(h.sink.last_text) == h.config.max_attempts

    asyncio.run(go())


def test_cron_delivery_with_unreachable_runner_opens_no_window(make_harness, make_hook_run) -> None:
    async def go() -> None:
        async with (
            make_hook_run() as run,
            make_harness(
                hook_runs=run.recorder(), max_attempts=3, claim_timeout_seconds=_WIDE_WINDOW_S
            ) as h,
        ):
            runner = _ReopenableRunner(make_harness)
            await runner.start()
            try:
                _route_sandboxes_to(h, runner.port)
                # The liveness read answers idle, then the runner stops
                # listening, so the turn start itself is a refused connect.
                runner.close_after_status = True
                event = qevent(
                    "digest",
                    thread="tCronUnreachable",
                    placeholder=None,
                    source=TurnSource.CRON,
                    hook_run=run.ref,
                )
                with _recording() as records:
                    started = time.monotonic()
                    await h.kernel.process_event(event)
                    elapsed = time.monotonic() - started

                assert len(records.failures(event.event_id)) == 1
                records.assert_connect_failures(event.event_id)
                assert records.windows(event.event_id) == []
                assert runner.runner.opened == []
                assert elapsed < _NOT_WAITED_S
                assert _completion(h, event.event_id) == ["escalated"]
            finally:
                await runner.cleanup()

    asyncio.run(go())


def test_persisted_side_effect_flag_escalates_without_waiting(make_harness) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=3, claim_timeout_seconds=_WIDE_WINDOW_S) as h:
            _route_sandboxes_to(h, make_harness.__globals__["_closed_port"]())
            event = qevent("acted already", thread="tSideEffect")
            await h.kernel._markers.mark_side_effect(event.event_id)
            with _recording() as records:
                started = time.monotonic()
                await h.kernel.process_event(event)
                elapsed = time.monotonic() - started

            assert records.windows(event.event_id) == []
            assert elapsed < _NOT_WAITED_S
            assert _completion(h, event.event_id) == ["escalated"]
            reply = h.sink.last_text
            assert reply is not None
            assert failure_class_from_reply(reply) == "prior-side-effect"

    asyncio.run(go())
