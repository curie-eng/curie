"""Regression guard for the RunnerClient turn-stream release contract that the
kernel's _consume relies on (verify-f1 coverage gap 1): the aiohttp response must
be released on every exit path -- normal completion and an exception mid-stream --
so a turn never leaks a connection. We spy on the response's release() because it
is what TurnStream.close (called from __aexit__) invokes."""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import logging
import textwrap
import tracemalloc
from typing import Any

import pytest
from aci_protocol import Event, Final, SessionStatus, SideEffectFlag, TextDelta
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, ToolUseBlock
from curie_runner import RunTracer, SideEffectClassifier, create_app
from curie_runner import server as runner_server
from curie_runner import session as runner_session_module
from curie_runner.fake import FakeModelSession
from curie_runner.session import SessionRunner
from curie_telemetry.tracing import configure_tracer_provider
from curie_worker import runner_client as runner_client_module
from curie_worker.runner_client import RunnerClient, RunnerError, RunnerStreamTimeout
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

DONE = SessionStatus.DONE


def _event() -> Event:
    return Event(type="message", text="hi", user="U", ts="1")


def _spy_release(turn: Any) -> dict[str, int]:
    calls = {"n": 0}
    real = turn._response.release

    def spy() -> Any:
        calls["n"] += 1
        return real()

    turn._response.release = spy
    return calls


def test_turn_stream_released_on_normal_completion(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [TextDelta(text="x"), Final(text="done", status=DONE)]
            handle = await asyncio.to_thread(h.substrate.claim, "tS")
            client = RunnerClient(total_timeout_s=30.0)
            try:
                turn = await client.start_turn(handle.base_url, _event())
                calls = _spy_release(turn)
                async with turn:
                    async for _frame in turn:
                        pass
                assert calls["n"] >= 1  # released on normal exit
            finally:
                await client.close()

    asyncio.run(go())


def test_turn_stream_released_when_consumer_raises(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # A hanging turn: the body is not fully read, so aiohttp will not
            # auto-release on EOF -- only TurnStream.__aexit__ can release it.
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="x")]
            h.runner.tail = [Final(text="done", status=DONE)]
            handle = await asyncio.to_thread(h.substrate.claim, "tSraise")
            client = RunnerClient(total_timeout_s=30.0)
            try:
                turn = await client.start_turn(handle.base_url, _event())
                calls = _spy_release(turn)
                try:
                    async with turn:
                        raise RuntimeError("consumer blew up mid-stream")
                except RuntimeError:
                    pass
                assert calls["n"] >= 1  # released on the error path too
            finally:
                hold.set()
                await client.close()

    asyncio.run(go())


# --- Per-call Authorization header (issue #63) --------------------------------
# Against a REAL local aiohttp server that records each request's headers, so the
# assertion is on the actual bytes on the wire, not a mock of the client.


class _HeaderRecordingRunner:
    """Records the request headers seen on each ACI route."""

    def __init__(self, *, status_body: dict[str, object] | None = None) -> None:
        self.app = web.Application()
        self.app.add_routes(
            [
                web.post("/v1/event", self._event),
                web.post("/v1/steer", self._steer),
                web.post("/v1/interrupt", self._interrupt),
                web.get("/v1/status", self._status),
                web.get("/status", self._status),
            ]
        )
        self.headers: dict[str, dict[str, str]] = {}
        self.status_body = status_body or {
            "status": "idle-awaiting-input",
            "turn_active": False,
            "history_durable": True,
        }

    async def _event(self, request: web.Request) -> web.StreamResponse:
        self.headers["event"] = dict(request.headers)
        resp = web.StreamResponse(status=200, headers={"Content-Type": "application/x-ndjson"})
        await resp.prepare(request)
        await resp.write((Final(text="ok", status=DONE).model_dump_json() + "\n").encode("utf-8"))
        await resp.write_eof()
        return resp

    async def _steer(self, request: web.Request) -> web.Response:
        self.headers["steer"] = dict(request.headers)
        return web.json_response({"ok": True})

    async def _interrupt(self, request: web.Request) -> web.Response:
        self.headers["interrupt"] = dict(request.headers)
        return web.json_response({"ok": True})

    async def _status(self, request: web.Request) -> web.Response:
        self.headers[request.path] = dict(request.headers)
        return web.json_response(self.status_body)


async def _drain(turn: Any) -> None:
    async with turn:
        async for _frame in turn:
            pass


def test_runner_client_sends_bearer_token_on_every_call() -> None:
    async def go() -> None:
        runner = _HeaderRecordingRunner()
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0)
        try:
            turn = await client.start_turn(base_url, _event(), token="tok-1")
            await _drain(turn)
            await client.steer(base_url, _event(), token="tok-1")
            await client.interrupt(base_url, "stop", token="tok-1")
            await client.status(base_url, token="tok-1")

            assert runner.headers["event"].get("Authorization") == "Bearer tok-1"
            assert runner.headers["steer"].get("Authorization") == "Bearer tok-1"
            assert runner.headers["interrupt"].get("Authorization") == "Bearer tok-1"
            assert runner.headers["/v1/status"].get("Authorization") == "Bearer tok-1"
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


def test_runner_client_preserves_authenticated_boot_attestation() -> None:
    async def go() -> None:
        attested = {
            "status": "idle-awaiting-input",
            "ready": True,
            "turn_active": False,
            "history_durable": True,
            "session_id": "session-acme-workspace",
            "sandbox_id": "sandbox-acme-workspace",
            "managed_workspace": True,
            "cwd": "/workspace",
        }
        runner = _HeaderRecordingRunner(status_body=attested)
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0)
        try:
            status = await client.status(base_url, token="tok-1")

            assert status == attested
            assert runner.headers["/v1/status"].get("Authorization") == "Bearer tok-1"
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


def test_runner_client_omits_authorization_without_token() -> None:
    async def go() -> None:
        for token in (None, ""):
            runner = _HeaderRecordingRunner()
            server = TestServer(runner.app)
            await server.start_server()
            base_url = f"http://127.0.0.1:{server.port}"
            client = RunnerClient(total_timeout_s=30.0)
            try:
                turn = await client.start_turn(base_url, _event(), token=token)
                await _drain(turn)
                await client.steer(base_url, _event(), token=token)
                await client.interrupt(base_url, "stop", token=token)
                await client.status(base_url, token=token)

                assert "Authorization" not in runner.headers["event"]
                assert "Authorization" not in runner.headers["steer"]
                assert "Authorization" not in runner.headers["interrupt"]
                assert "Authorization" not in runner.headers["/status"]
            finally:
                await client.close()
                await server.close()

    asyncio.run(go())


# --- The interrupt RPC gets its own bound, separate from the streaming ---------
# budget (#742, a follow-up to #739 which bounded only one call site above this
# layer). Against a REAL local server whose /v1/interrupt accepts the
# connection and then answers nothing -- the wedged-runner shape -- so the
# assertion is on the actual client behavior, not a mock of it.


class _HangingInterruptRunner:
    """A runner whose ``/v1/interrupt`` accepts the connection and never
    answers, modelling the wedged runner #742 is about."""

    def __init__(self) -> None:
        self.app = web.Application()
        self.app.add_routes([web.post("/v1/interrupt", self._interrupt)])
        self.hang = asyncio.Event()  # never set by the test: the handler never returns

    async def _interrupt(self, request: web.Request) -> web.Response:
        await self.hang.wait()
        return web.json_response({"ok": True})


def test_interrupt_is_bounded_by_its_own_timeout_not_the_streaming_budget() -> None:
    """The interrupt call must time out at RunnerClient's own
    ``interrupt_timeout_s``, not the session's streaming ``total_timeout_s`` --
    deliberately configured huge here so the test would hang for a long time
    (rather than pass by accident) if the interrupt call fell back to
    inheriting it."""

    async def go() -> None:
        runner = _HangingInterruptRunner()
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0, interrupt_timeout_s=0.2)
        try:
            loop = asyncio.get_event_loop()
            started = loop.time()
            with pytest.raises(TimeoutError):
                await client.interrupt(base_url, "stop")
            elapsed = loop.time() - started
            assert elapsed < 5.0  # nowhere near the 30s streaming budget
        finally:
            runner.hang.set()
            await client.close()
            await server.close()

    asyncio.run(go())


def test_snapshot_refuses_an_oversized_body_before_json_decoding() -> None:
    async def go() -> None:
        app = web.Application()

        async def oversized(_request: web.Request) -> web.Response:
            return web.Response(
                body=b'{"patch_base64":"' + (b"A" * 140_000) + b'"}',
                content_type="application/json",
            )

        app.add_routes([web.post("/v1/snapshot", oversized)])
        server = TestServer(app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0, snapshot_patch_max_bytes=16)
        try:
            with pytest.raises(RunnerError, match="invalid bounded payload"):
                await client.snapshot(
                    f"http://127.0.0.1:{server.port}", token="runner-token"
                )
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


# --- Successful Final must not RST the runner before write_eof (issue #1958) --
# Kernel._consume stops applying frames at Final, then TurnStream.__aexit__
# releases the aiohttp response. Against the real runner HTTP stream that
# races server._event's write_eof (after aclosing teardown) and logs
# ClientConnectionResetError on a completed turn. Drain the rest of the body
# before release so write_eof still sees an open transport.


def test_kernel_final_break_closes_real_runner_stream_without_write_eof_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler_errors: list[BaseException] = []
    write_eof_errors: list[BaseException] = []
    real_write_eof = web.StreamResponse.write_eof
    original_event = runner_server._event

    async def spy_write_eof(self: web.StreamResponse, data: bytes = b"") -> None:
        try:
            await real_write_eof(self, data)
        except BaseException as exc:
            write_eof_errors.append(exc)
            raise

    monkeypatch.setattr(web.StreamResponse, "write_eof", spy_write_eof)

    async def go() -> None:
        handler_done = asyncio.Event()

        async def wrapped_event(request: web.Request) -> web.StreamResponse:
            try:
                return await original_event(request)
            except BaseException as exc:
                handler_errors.append(exc)
                raise
            finally:
                handler_done.set()

        monkeypatch.setattr(runner_server, "_event", wrapped_event)
        fake = FakeModelSession()
        runner = SessionRunner(
            session_factory=lambda: fake,
            ceiling=0,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="t",
        )
        original_record = runner._record_turn

        async def delayed_record(*args: Any, **kwargs: Any) -> Any:
            # Production posts the transcript after yielding Final and before
            # the generator ends. That is the window _consume uses to break
            # and release, which then cancels _event before write_eof.
            await asyncio.sleep(0.05)
            return await original_record(*args, **kwargs)

        runner._record_turn = delayed_record  # type: ignore[method-assign]
        await runner.start()
        server = TestServer(create_app(runner))
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0)
        saw_final = False
        try:
            turn = await client.start_turn(base_url, _event())
            release_calls = _spy_release(turn)
            async with turn:
                async for frame in turn:
                    if isinstance(frame, Final):
                        assert frame.status == DONE
                        saw_final = True
                        break
            await asyncio.wait_for(handler_done.wait(), timeout=5.0)
            assert saw_final
            assert handler_errors == [], (
                f"successful Final produced a runner handler error: {handler_errors!r}"
            )
            assert write_eof_errors == [], (
                "successful Final closed the runner transport before write_eof: "
                f"{write_eof_errors!r}"
            )
            assert release_calls["n"] >= 1
        finally:
            await client.close()
            await server.close()
            await runner.close()

    asyncio.run(go())


class _PostFinalRunner:
    """Real HTTP peer with controllable behavior after a valid Final."""

    def __init__(self, *, stall: bool = False, tail_bytes: int = 0) -> None:
        self.app = web.Application()
        self.app.add_routes([web.post("/v1/event", self._event)])
        self.stall = stall
        self.tail_bytes = tail_bytes
        self.after_final = asyncio.Event()
        self.unblock = asyncio.Event()
        self.handler_done = asyncio.Event()
        self.handler_errors: list[BaseException] = []

    async def _event(self, request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200, headers={"Content-Type": "application/x-ndjson"}
        )
        await response.prepare(request)
        try:
            await response.write(
                (Final(text="done", status=DONE).model_dump_json() + "\n").encode()
            )
            self.after_final.set()
            if self.stall:
                await self.unblock.wait()
            chunk = b"x" * (64 * 1024)
            remaining = self.tail_bytes
            while remaining:
                size = min(remaining, len(chunk))
                await response.write(chunk[:size])
                remaining -= size
            await response.write_eof()
        except asyncio.CancelledError:
            raise
        except (ConnectionError, OSError) as exc:
            # Expected only when a timeout/cancellation test deliberately
            # releases the client response before unblocking this handler.
            self.handler_errors.append(exc)
        finally:
            self.handler_done.set()
        return response


async def _break_after_final(turn: Any, final_seen: asyncio.Event | None = None) -> None:
    async with turn:
        async for frame in turn:
            if isinstance(frame, Final):
                if final_seen is not None:
                    final_seen.set()
                break


def test_post_final_stall_has_a_short_cleanup_bound_and_releases_response() -> None:
    async def go() -> None:
        runner = _PostFinalRunner(stall=True)
        server = TestServer(runner.app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        turn = await client.start_turn(f"http://127.0.0.1:{server.port}", _event())
        release_calls = _spy_release(turn)
        loop = asyncio.get_running_loop()
        started = loop.time()
        try:
            await asyncio.wait_for(_break_after_final(turn), timeout=2.0)
            assert loop.time() - started < 2.0
            assert release_calls["n"] >= 1
        finally:
            runner.unblock.set()
            await client.close()
            await server.close()

    asyncio.run(go())


def test_cancellation_during_post_final_drain_propagates_and_releases_response() -> None:
    async def go() -> None:
        runner = _PostFinalRunner(stall=True)
        server = TestServer(runner.app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        turn = await client.start_turn(f"http://127.0.0.1:{server.port}", _event())
        release_calls = _spy_release(turn)
        final_seen = asyncio.Event()
        task = asyncio.create_task(_break_after_final(turn, final_seen))
        try:
            await asyncio.wait_for(final_seen.wait(), timeout=1.0)
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert release_calls["n"] >= 1
        finally:
            runner.unblock.set()
            if not task.done():
                task.cancel()
            await client.close()
            await server.close()

    asyncio.run(go())


def test_unexpected_post_final_cleanup_error_is_ignored_and_releases_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def go() -> None:
        runner = _PostFinalRunner(stall=True)
        server = TestServer(runner.app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        turn = await client.start_turn(f"http://127.0.0.1:{server.port}", _event())
        release_calls = _spy_release(turn)

        async def cleanup_boom(_reader: Any, _size: int) -> bytes:
            raise RuntimeError("unexpected cleanup failure")

        try:
            monkeypatch.setattr(type(turn._response.content), "read", cleanup_boom)
            await _break_after_final(turn)
            assert release_calls["n"] >= 1
        finally:
            runner.unblock.set()
            await client.close()
            await server.close()

    asyncio.run(go())


def test_large_post_final_tail_is_discarded_without_aggregation() -> None:
    async def go() -> None:
        runner = _PostFinalRunner(tail_bytes=32 * 1024 * 1024)
        server = TestServer(runner.app)
        await server.start_server()
        client = RunnerClient(total_timeout_s=30.0)
        turn = await client.start_turn(f"http://127.0.0.1:{server.port}", _event())
        release_calls = _spy_release(turn)
        tracemalloc.start()
        tracemalloc.reset_peak()
        try:
            baseline, _ = tracemalloc.get_traced_memory()
            await asyncio.wait_for(_break_after_final(turn), timeout=5.0)
            _current, peak = tracemalloc.get_traced_memory()
            assert peak - baseline < 8 * 1024 * 1024
            assert release_calls["n"] >= 1
            await asyncio.wait_for(runner.handler_done.wait(), timeout=1.0)
            assert runner.handler_errors == []
        finally:
            tracemalloc.stop()
            await client.close()
            await server.close()

    asyncio.run(go())


# --- Per-request timeout from the remaining delivery budget (ADR-0131, #1971) -
#
# ``runner_total_timeout_s`` stops being an independent clock and becomes a
# per-request CEILING inside the delivery's one overall deadline. Each
# budget-consuming RPC takes an optional ``remaining_s``; the effective timeout
# is ``min(runner_total_timeout_s, remaining_s)``, and ``remaining_s=None`` keeps
# the session default so every pre-existing caller is behaviourally unchanged.
#
# Asserted against a REAL local server that accepts the connection and then never
# answers -- the shape a budget must actually bound -- so these measure client
# behavior rather than a mock of it.


class _HangingEventRunner:
    """A runner whose ``/v1/event`` and ``/v1/interrupt`` accept the connection
    and never answer."""

    def __init__(self) -> None:
        self.app = web.Application()
        self.app.add_routes(
            [
                web.post("/v1/event", self._hang),
                web.post("/v1/interrupt", self._hang),
            ]
        )
        self.hang = asyncio.Event()  # set only in teardown

    async def _hang(self, _request: web.Request) -> web.Response:
        await self.hang.wait()
        return web.json_response({"ok": True})


def test_start_turn_uses_the_remaining_budget_when_it_is_shorter() -> None:
    """A delivery with 0.3s of budget left must not hand the runner the full 30s
    session budget. Reverting the per-request override makes this call wait the
    whole streaming timeout, so the elapsed assertion is what goes red."""

    async def go() -> None:
        runner = _HangingEventRunner()
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        # 30s session default, deliberately huge relative to the budget below.
        client = RunnerClient(total_timeout_s=30.0)
        try:
            loop = asyncio.get_event_loop()
            started = loop.time()
            with pytest.raises(TimeoutError):
                await client.start_turn(base_url, _event(), remaining_s=0.3)
            assert loop.time() - started < 5.0
        finally:
            runner.hang.set()
            await client.close()
            await server.close()

    asyncio.run(go())


def test_start_turn_without_a_remaining_budget_uses_the_session_default(
    caplog,
) -> None:
    """``remaining_s=None`` must leave the session timeout in charge: that is the
    path every leaseless caller and every existing test takes, and it must stay
    byte-identical in behavior."""

    async def go() -> None:
        runner = _HangingEventRunner()
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=0.3)
        try:
            loop = asyncio.get_event_loop()
            started = loop.time()
            with caplog.at_level(logging.INFO, logger="curie_worker.runner_client"):
                with pytest.raises(TimeoutError):
                    await client.start_turn(base_url, _event(), remaining_s=None)
            assert loop.time() - started < 5.0
            budget_records = [
                record
                for record in caplog.records
                if record.name == "curie_worker.runner_client"
                and hasattr(record, "effective_request_timeout_s")
            ]
            assert budget_records == [], caplog.text
            assert not any(
                "remaining" in record.getMessage()
                and "effective" in record.getMessage()
                for record in caplog.records
                if record.name == "curie_worker.runner_client"
            ), caplog.text
        finally:
            runner.hang.set()
            await client.close()
            await server.close()

    asyncio.run(go())


def test_the_effective_timeout_is_the_min_of_the_budget_and_the_session_ceiling() -> None:
    """A remaining budget LARGER than the per-request ceiling must not raise the
    ceiling. Reverting ``min(...)`` to "the budget wins" would let a 30-minute
    delivery hand one runner request a 30-minute HTTP deadline."""

    async def go() -> None:
        runner = _HangingEventRunner()
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=0.3)
        try:
            loop = asyncio.get_event_loop()
            started = loop.time()
            with pytest.raises(TimeoutError):
                await client.start_turn(base_url, _event(), remaining_s=30.0)
            assert loop.time() - started < 5.0
        finally:
            runner.hang.set()
            await client.close()
            await server.close()

    asyncio.run(go())


def test_budgeted_request_logs_the_effective_timeout_bound(caplog) -> None:
    """A real request records the configured ceiling, the unmodified delivery
    remainder, and the effective post-floor timeout handed to aiohttp."""

    async def go() -> None:
        runner = _HeaderRecordingRunner()
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0)
        try:
            with caplog.at_level(logging.INFO, logger="curie_worker.runner_client"):
                turn = await client.start_turn(base_url, _event(), remaining_s=5.0)
                await _drain(turn)

            budget_records = [
                record
                for record in caplog.records
                if record.name == "curie_worker.runner_client"
                and "runner request timeout bound" in record.getMessage()
            ]
            assert len(budget_records) == 1, caplog.text
            record = budget_records[0]
            assert record.levelno == logging.INFO
            message = record.getMessage()
            normalized_message = message.lower()
            assert "configured" in normalized_message
            assert "ceiling" in normalized_message
            assert "30.0" in message
            assert "remaining" in normalized_message
            assert "effective" in normalized_message
            assert message.count("5.0") >= 2
            # The numeric body is fully formatted before logging. OTLP log
            # exporters receive this same body rather than a format template
            # plus deferred arguments that lose their intended representation.
            assert record.args == ()
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


def test_budgeted_status_does_not_log_the_turn_timeout_bound(caplog) -> None:
    """Budget propagation still bounds control RPCs, but the effective turn
    timeout record belongs only to the request that opens the streamed turn.
    Polling status must not emit that operator-facing message.
    """

    async def go() -> None:
        app = web.Application()

        async def status_handler(_request: web.Request) -> web.Response:
            return web.json_response({"turn_active": False})

        app.add_routes([web.get("/status", status_handler)])
        server = TestServer(app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0)
        try:
            caplog.clear()
            with caplog.at_level(logging.INFO, logger="curie_worker.runner_client"):
                status = await client.status(base_url, remaining_s=5.0)

            assert status["turn_active"] is False
            records = [
                record
                for record in caplog.records
                if record.name == "curie_worker.runner_client"
            ]
            assert not any(
                record.levelno == logging.INFO
                and "runner request timeout bound" in record.getMessage()
                for record in records
            ), caplog.text
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


def test_budgeted_status_uses_the_remaining_delivery_timeout() -> None:
    """A status read still consumes the delivery deadline even though it does
    not emit the turn-start timeout record. A short remainder must bound the
    request instead of inheriting the much larger configured ceiling.
    """

    async def go() -> None:
        hold = asyncio.Event()
        app = web.Application()

        async def hanging_status(_request: web.Request) -> web.Response:
            await hold.wait()
            return web.json_response({"turn_active": False})

        app.add_routes([web.get("/status", hanging_status)])
        server = TestServer(app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0)
        try:
            loop = asyncio.get_event_loop()
            started = loop.time()
            with pytest.raises(TimeoutError):
                await client.status(base_url, remaining_s=0.2)
            assert loop.time() - started < 5.0
        finally:
            hold.set()
            await client.close()
            await server.close()

    asyncio.run(go())


def test_a_remaining_budget_does_not_break_a_responsive_turn() -> None:
    """The positive control for the three timeout tests above: with a budget in
    hand and a runner that answers, the turn still opens and streams. Without it
    they would all pass against a client whose every request now fails."""

    async def go() -> None:
        runner = _HeaderRecordingRunner()
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0)
        try:
            turn = await client.start_turn(base_url, _event(), remaining_s=5.0)
            await _drain(turn)
            assert "event" in runner.headers
            assert await client.steer(base_url, _event(), remaining_s=5.0) is True
        finally:
            await client.close()
            await server.close()

    asyncio.run(go())


def test_interrupt_takes_no_remaining_budget_while_the_other_rpcs_do() -> None:
    """A structural guard against a future "simplification" that folds interrupt
    into the budget path. ``/v1/interrupt`` is the fail-closed path a lost lease
    fires: deriving its timeout from a budget that may already be exhausted would
    make the fence unable to stop the runner it just fenced."""
    for name in ("start_turn", "steer", "status", "snapshot", "reset"):
        parameters = inspect.signature(getattr(RunnerClient, name)).parameters
        assert "remaining_s" in parameters, f"{name} must accept a remaining budget"

    assert "remaining_s" not in inspect.signature(RunnerClient.interrupt).parameters, (
        "interrupt must never take a remaining budget: it is the fail-closed "
        "control path and keeps its own independent timeout"
    )

    source = textwrap.dedent(inspect.getsource(RunnerClient))
    tree = ast.parse(source)
    timeout_posts = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        call_source = ast.get_source_segment(source, node) or ""
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "post"
            and "/v1/timeout" in call_source
        ):
            timeout_posts.append(node)

    assert len(timeout_posts) == 1, "RunnerClient must own one /v1/timeout POST"
    timeout_keywords = {
        keyword.arg: ast.unparse(keyword.value)
        for keyword in timeout_posts[0].keywords
        if keyword.arg is not None
    }
    assert timeout_keywords.get("timeout") == "self._interrupt_timeout", (
        "/v1/timeout must use the independent control-plane cap, never the "
        "expired stream/delivery budget"
    )


def test_interrupt_keeps_its_own_timeout_under_a_huge_streaming_budget() -> None:
    """The behavioral half of the guard above. With a 30s session budget and a
    wedged runner, the interrupt must still return at its own 0.2s bound."""

    async def go() -> None:
        runner = _HangingEventRunner()
        server = TestServer(runner.app)
        await server.start_server()
        base_url = f"http://127.0.0.1:{server.port}"
        client = RunnerClient(total_timeout_s=30.0, interrupt_timeout_s=0.2)
        try:
            loop = asyncio.get_event_loop()
            started = loop.time()
            with pytest.raises(TimeoutError):
                await client.interrupt(base_url, "delivery lease lost")
            assert loop.time() - started < 5.0
        finally:
            runner.hang.set()
            await client.close()
            await server.close()

    asyncio.run(go())


# --- The streaming boundary owns its own timeout terminal record (#2011) ------
# ``start_turn``'s ``_rpc`` span has already closed by the time the NDJSON body
# is streamed, so an expiring total/sock_read budget used to leave NO record at
# this boundary at all, and handed the kernel a bare ``TimeoutError`` whose
# ``str()`` is the empty string.


def test_stream_timeout_raises_a_named_timeout_and_logs_the_expired_budget(
    make_harness, caplog
) -> None:
    """#2011: iterating a turn whose runner hangs past the client's budget must
    raise a ``RunnerStreamTimeout`` -- still a ``TimeoutError``, so every
    existing ``except TimeoutError`` keeps catching it -- whose message names the
    normalized exception class and the budget that expired, and must emit a
    correlated WARNING on the client's own logger. Today the raised exception is
    a bare ``TimeoutError`` that stringifies to "" and nothing is logged here."""

    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()  # never set: the response hangs after a prefix
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="x")]
            handle = await asyncio.to_thread(h.substrate.claim, "tStreamTimeout")
            client = RunnerClient(total_timeout_s=5.0)
            try:
                with caplog.at_level(logging.WARNING, logger="curie_worker.runner_client"):
                    turn = await client.start_turn(
                        handle.base_url, _event(), remaining_s=0.2
                    )
                    with pytest.raises(TimeoutError) as excinfo:
                        async with turn:
                            async for _frame in turn:
                                pass

                exc = excinfo.value
                assert isinstance(exc, RunnerStreamTimeout)
                assert isinstance(exc, TimeoutError)  # existing handlers still catch it
                assert str(exc).strip(), "a stream timeout must not stringify to nothing"
                assert "Timeout" in str(exc)  # the normalized underlying class
                # The delivery had only 0.2s left, so that effective request
                # bound -- not the configured 5s ceiling -- is what expired.
                assert "0.2" in str(exc)
                assert "5.0s" not in str(exc)

                warnings = [
                    record.getMessage()
                    for record in caplog.records
                    if record.name == "curie_worker.runner_client"
                    and record.levelno >= logging.WARNING
                ]
                assert warnings, caplog.text
                assert any(
                    "Timeout" in message and "0.2" in message for message in warnings
                ), warnings
                assert all("5.0s" not in message for message in warnings)
            finally:
                hold.set()
                await client.close()

    asyncio.run(go())


_PRIVATE_EVENT_TEXT = "private-timeout-event-body-PLACEHOLDER"
_PRIVATE_TOOL_CALL = "private-timeout-tool-call-PLACEHOLDER"
_PRIVATE_TOOL_ARGUMENT = "private-timeout-tool-argument-PLACEHOLDER"
_PRIVATE_POST_TIMEOUT_TEXT = "private-post-timeout-body-PLACEHOLDER"
_RUNNER_TOKEN = "runner-token-PLACEHOLDER"
_TURN_EPOCH_HEADER = "X-Curie-Turn-Epoch"


class _TimeoutBoundaryFake(FakeModelSession):
    """Stall after a side-effect prefix, with two real interrupt shapes."""

    def __init__(self, *, release_on_interrupt: bool) -> None:
        script = [
            AssistantMessage(
                content=[
                    ToolUseBlock(
                        id=_PRIVATE_TOOL_CALL,
                        name="Bash",
                        input={"command": _PRIVATE_TOOL_ARGUMENT},
                    )
                ],
                model="fake-model",
            ),
            AssistantMessage(
                content=[TextBlock(text=_PRIVATE_POST_TIMEOUT_TEXT)],
                model="fake-model",
            ),
        ]
        super().__init__(
            lambda: script,
            truncate_on_interrupt=release_on_interrupt,
        )
        self._release_on_interrupt = release_on_interrupt
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.notified = asyncio.Event()
        self.post_timeout_emitted = asyncio.Event()

    async def receive_turn(self):
        index = 0
        async for message in super().receive_turn():
            if index:
                self.post_timeout_emitted.set()
            yield message
            if index == 0:
                self.entered.set()
                await self.release.wait()
            index += 1

    async def interrupt(self) -> None:
        await super().interrupt()
        self.notified.set()
        if self._release_on_interrupt:
            self.release.set()

    async def close(self) -> None:
        self.release.set()
        await super().close()


def _span_material(spans: list[ReadableSpan]) -> str:
    return repr(
        [
            (
                span.name,
                dict(span.attributes or {}),
                [(event.name, dict(event.attributes or {})) for event in span.events],
                span.status,
            )
            for span in spans
        ]
    )


async def _assert_real_timeout_boundary(
    monkeypatch: pytest.MonkeyPatch,
    *,
    release_on_interrupt: bool,
) -> list[tuple[str, dict[str, str]]]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    fake = _TimeoutBoundaryFake(release_on_interrupt=release_on_interrupt)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(provider),
        classifier=SideEffectClassifier(),
        trace_name="curie-run:test",
        model="fake-model",
    )
    metrics: list[tuple[str, dict[str, str]]] = []

    def capture_metric(
        name: str,
        _value: float = 1,
        *,
        attributes: dict[str, str],
    ) -> None:
        metrics.append((name, dict(attributes)))

    monkeypatch.setattr(runner_session_module, "record_metric", capture_metric)
    worker_record_metric = runner_client_module.record_metric

    def capture_worker_metric(
        name: str,
        value: float = 1,
        *,
        attributes: dict[str, str],
    ) -> None:
        # Keep the real catalog validator in the path. A capture-only double
        # would hide the exact closed-domain failure this boundary regressed.
        worker_record_metric(name, value, attributes=attributes)
        metrics.append((name, dict(attributes)))

    monkeypatch.setattr(runner_client_module, "record_metric", capture_worker_metric)
    original_event = runner_server._event
    handler_done = asyncio.Event()
    handler_errors: list[BaseException] = []

    async def wrapped_event(request: web.Request) -> web.StreamResponse:
        try:
            return await original_event(request)
        except BaseException as exc:
            handler_errors.append(exc)
            raise
        finally:
            handler_done.set()

    monkeypatch.setattr(runner_server, "_event", wrapped_event)
    await runner.start()
    server = TestServer(create_app(runner, token=_RUNNER_TOKEN))
    await server.start_server()
    client = RunnerClient(total_timeout_s=0.25, interrupt_timeout_s=2.0)
    frames: list[Any] = []
    epoch = ""
    parent_trace_id = 0
    configure_tracer_provider(provider)
    try:
        tracer = provider.get_tracer("timeout-boundary-test")
        with tracer.start_as_current_span("worker.timeout.parent") as parent:
            parent_trace_id = parent.get_span_context().trace_id
            turn = await client.start_turn(
                f"http://127.0.0.1:{server.port}",
                Event(
                    type="message",
                    text=_PRIVATE_EVENT_TEXT,
                    user="U0EXAMPLE1",
                    ts="1",
                ),
                token=_RUNNER_TOKEN,
                remaining_s=0.25,
            )
            epoch = turn._response.headers[_TURN_EPOCH_HEADER]
            with pytest.raises(RunnerStreamTimeout):
                async with turn:
                    async for frame in turn:
                        frames.append(frame)

        await asyncio.wait_for(fake.notified.wait(), timeout=2.0)
        if not release_on_interrupt:
            # The control response has returned and TurnStream has released the
            # body. Only now let the SDK emit a non-terminal line: response.write
            # must fail and aclosing must inject GeneratorExit (or cancellation)
            # while run_turn is suspended at its yield.
            fake.release.set()
            await asyncio.wait_for(fake.post_timeout_emitted.wait(), timeout=2.0)
        await asyncio.wait_for(handler_done.wait(), timeout=2.0)
    finally:
        fake.release.set()
        await client.close()
        await server.close()
        configure_tracer_provider(None)

    assert any(isinstance(frame, SideEffectFlag) for frame in frames)
    spans = list(exporter.get_finished_spans())
    roots = [span for span in spans if span.name == "agent.run"]
    assert len(roots) == 1
    root = roots[0]
    assert root.attributes["curie.terminal.cause"] == "runner_timeout"
    assert root.attributes["curie.terminal.status"] == "failed"
    assert root.status.status_code is StatusCode.ERROR
    assert root.context is not None and root.context.trace_id == parent_trace_id
    assert root.parent is not None
    parent_rpc = next(
        span
        for span in spans
        if span.context is not None and span.context.span_id == root.parent.span_id
    )
    assert parent_rpc.name == "curie.runner.rpc"
    assert parent_rpc.attributes["curie.operation"] == "event"
    for phase in (
        span
        for span in spans
        if span.name in {"llm.generation", "execute_tool"}
    ):
        assert phase.end_time is not None
        assert "curie.phase.end_kind" in phase.attributes
    tool = next(span for span in spans if span.name == "execute_tool")
    assert tool.attributes["curie.phase.end_kind"] == "terminal_inferred"
    assert tool.attributes["curie.tool.outcome"] == "cancelled"
    assert tool.status.status_code is StatusCode.ERROR

    completed = [
        attributes
        for name, attributes in metrics
        if name == "curie.turn.completed"
    ]
    assert completed == [
        {
            "service.name": "curie-runner",
            "source": "runner",
            "outcome": "classified_failure",
        }
    ]
    material = _span_material(spans)
    for private_value in (
        epoch,
        _RUNNER_TOKEN,
        _PRIVATE_EVENT_TEXT,
        _PRIVATE_TOOL_CALL,
        _PRIVATE_TOOL_ARGUMENT,
        _PRIVATE_POST_TIMEOUT_TEXT,
    ):
        assert private_value not in material
    if not release_on_interrupt:
        assert handler_errors, "released-body post-timeout write must fail"
    assert fake.interrupts == 1, "an ACKed timeout stop must not be sent twice"
    return metrics


def test_real_http_timeout_unblocks_live_consumer_with_first_failure_terminal(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    metrics = asyncio.run(
        _assert_real_timeout_boundary(monkeypatch, release_on_interrupt=True)
    )
    timeout_rpc_attributes = {
        "service.name": "curie-worker",
        "operation": "timeout",
        "role": "client",
        "outcome": "success",
    }
    assert (
        "curie.runner.rpc.request.duration",
        timeout_rpc_attributes,
    ) in metrics
    assert ("curie.runner.rpc.result", timeout_rpc_attributes) in metrics
    assert not any(
        "runner timeout terminal notification failed" in record.getMessage()
        for record in caplog.records
    )


def test_real_http_timeout_terminalizes_before_released_body_write_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    asyncio.run(
        _assert_real_timeout_boundary(monkeypatch, release_on_interrupt=False)
    )


class _ProductionTimeoutPostureSession:
    """Ordered wire double for aiohttp's production non-cancelling posture."""

    def __init__(self) -> None:
        self.wire: list[tuple[str, str | int]] = []
        self.queries = 0
        self.interrupts = 0
        self.first_prefix_emitted = asyncio.Event()
        self.end_first_receive = asyncio.Event()
        self.interrupt_entered = asyncio.Event()
        self.release_ack = asyncio.Event()
        self.interrupt_returned = asyncio.Event()
        self.interrupt_cancelled = asyncio.Event()
        self.second_query_started = asyncio.Event()

    async def connect(self) -> None: ...

    async def query(self, text: str) -> None:
        self.queries += 1
        self.wire.append(("query", text))
        if self.queries == 2:
            self.second_query_started.set()

    async def receive_turn(self):
        if self.queries == 1:
            yield AssistantMessage(
                content=[
                    ToolUseBlock(
                        id="production-timeout-call-PLACEHOLDER",
                        name="Read",
                        input={"path": "production-timeout-path-PLACEHOLDER"},
                    )
                ],
                model="fake-model",
            )
            self.first_prefix_emitted.set()
            await self.end_first_receive.wait()
            # Keep the first turn non-terminal after the ACK. Its attempted
            # write to the released response drives the GeneratorExit cleanup.
            yield AssistantMessage(
                content=[TextBlock(text="post-timeout-PLACEHOLDER")],
                model="fake-model",
            )
            return
        yield ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="sdk-session-PLACEHOLDER",
            result="healthy",
        )

    async def interrupt(self) -> None:
        self.interrupts += 1
        self.wire.append(("interrupt", self.interrupts))
        self.interrupt_entered.set()
        try:
            await self.release_ack.wait()
        except asyncio.CancelledError:
            self.interrupt_cancelled.set()
            raise
        self.end_first_receive.set()
        self.interrupt_returned.set()

    async def close(self) -> None:
        self.release_ack.set()
        self.end_first_receive.set()


def test_production_http_timeout_handler_holds_next_query_until_ack(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def go() -> None:
        session = _ProductionTimeoutPostureSession()
        runner = SessionRunner(
            session_factory=lambda: session,
            ceiling=0,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="t",
        )
        await runner.start()
        app_runner = web.AppRunner(
            create_app(runner, token=_RUNNER_TOKEN), handler_cancellation=False
        )
        await app_runner.setup()
        site = web.TCPSite(app_runner, "127.0.0.1", 0)
        await site.start()
        assert site._server is not None  # noqa: SLF001 - ephemeral bound port
        sockets = site._server.sockets  # noqa: SLF001 - aiohttp exposes no port API
        assert sockets is not None
        port = sockets[0].getsockname()[1]
        base_url = f"http://127.0.0.1:{port}"
        client = RunnerClient(total_timeout_s=2.0, interrupt_timeout_s=0.05)
        second_frames: list[Any] = []
        second_stream_opened = asyncio.Event()
        second_task: asyncio.Task[None] | None = None

        async def consume_second() -> None:
            turn = await client.start_turn(
                base_url,
                Event(
                    type="message",
                    text="second",
                    user="U0EXAMPLE1",
                    ts="2",
                ),
                token=_RUNNER_TOKEN,
                remaining_s=2.0,
            )
            second_stream_opened.set()
            async with turn:
                async for frame in turn:
                    second_frames.append(frame)

        try:
            first = await client.start_turn(
                base_url,
                Event(
                    type="message",
                    text="first",
                    user="U0EXAMPLE1",
                    ts="1",
                ),
                token=_RUNNER_TOKEN,
                remaining_s=0.05,
            )
            with caplog.at_level(logging.WARNING, logger="curie_worker.runner_client"):
                with pytest.raises(RunnerStreamTimeout):
                    async with first:
                        async for _frame in first:
                            pass

            await asyncio.wait_for(session.interrupt_entered.wait(), timeout=1.0)
            second_task = asyncio.create_task(consume_second())
            await asyncio.wait_for(second_stream_opened.wait(), timeout=1.0)
            assert not session.interrupt_cancelled.is_set()
            assert not session.second_query_started.is_set()

            session.release_ack.set()
            await asyncio.wait_for(session.interrupt_returned.wait(), timeout=1.0)
            await asyncio.wait_for(session.second_query_started.wait(), timeout=1.0)
            await asyncio.wait_for(second_task, timeout=1.0)

            assert session.interrupts == 1
            assert session.wire == [
                ("query", "first"),
                ("interrupt", 1),
                ("query", "second"),
            ]
            assert isinstance(second_frames[-1], Final)
            assert second_frames[-1].status is SessionStatus.DONE
            assert any(
                "runner timeout terminal notification failed"
                in record.getMessage()
                for record in caplog.records
            )
        finally:
            session.release_ack.set()
            if second_task is not None and not second_task.done():
                second_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await second_task
            await client.close()
            await app_runner.cleanup()

    asyncio.run(go())
