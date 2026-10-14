"""Source-only capture and HTTP boundary checks, @spec AUTOMATED-REMEDIATION-6.

Synthetic proof objects carry no runtime qualification. In-process HTTP peers
exercise the actual RunnerClient and submitter, not the remediation API.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import json
import logging
from pathlib import Path

import aiohttp
import pytest
from aci_protocol import Event, Final, TextDelta, ToolNote
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.protected_binding import _CURRENT_DELIVERY, protected_delivery_scope
from curie_worker.runner_client import RunnerClient, TurnStream

from .test_protected_history import dispatchable

BLOCK = '```curie-remediation\n{"version":1,"nominations":[]}\n```\n'
OUTPUT = "Public introduction\n" + BLOCK + "Public tail"
PUBLIC = "Public introduction\nPublic tail"
EVENT = Event(type="message", text="Synthetic input", user="hook/example", ts="1")
VECTORS = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/remediation-nomination.json").read_text()
)["cases"]


def product():
    name = "curie_worker.protected_nomination"
    assert importlib.util.find_spec(name) is not None, "nomination runner composition is absent"
    return importlib.import_module(name)


class RecordingSubmitter:
    def __init__(self):
        self.submissions = []

    async def submit(self, *, event_id, block):
        self.submissions.append((event_id, block))


class SyntheticStream:
    """Runner boundary for failure/cancellation and context delegation only."""

    turn_epoch = "synthetic-epoch"

    def __init__(self, frames, failure=None):
        self.frames = frames
        self.failure = failure
        self.entered = 0
        self.exits = []
        self.closed = False

    async def __aiter__(self):
        for frame in self.frames:
            yield frame
        if self.failure is not None:
            raise self.failure

    async def __aenter__(self):
        self.entered += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.exits.append(exc_type)
        self.close()

    def close(self):
        self.closed = True


class GuardBoundary:
    """Synthetic guard only proves delegation/order, never real proof checks."""

    def __init__(self, stream, refusal=False):
        self.stream = stream
        self.refusal = refusal
        self.starts = []

    async def start_turn(self, base_url, event, token=None, **kwargs):
        self.starts.append((base_url, event, token, kwargs))
        if self.refusal:
            raise ValueError("synthetic guard refusal")
        return self.stream


async def drain(turn):
    async with turn:
        return [frame async for frame in turn]


@pytest.mark.parametrize("case", VECTORS, ids=lambda case: case["name"])
def test_shared_vector_filters_stream_and_final_once_without_mutating_original(case):
    """Missing/split/malformed fences and wrong statuses must keep API authority."""
    module = product()

    async def go():
        original = Final(text=case["output"], status=case["status"], input_tokens=7)
        raw = SyntheticStream([
            *(TextDelta(text=chunk) for chunk in case.get("deltas", [case["output"]])),
            ToolNote(text="Synthetic tool note"), original,
        ])
        submitter = RecordingSubmitter()
        runner = module.NominationCapturingRunner(GuardBoundary(raw), submitter)
        async with protected_delivery_scope(dispatchable(event="event/capture")):
            turn = await runner.start_turn("https://runner.example.com", EVENT)
        frames = await drain(turn)
        assert isinstance(turn, TurnStream)
        assert turn.turn_epoch == raw.turn_epoch
        assert "".join(f.text for f in frames if isinstance(f, TextDelta)) == case["reply"]
        assert frames[-1].text == case["reply"]
        assert frames[-1].input_tokens == 7
        assert original.text == case["output"]  # Runner transcript input remains raw.
        assert submitter.submissions == (
            [("event/capture", case["submitted"])] if case["submitted"] is not None else []
        )
        assert [f.text for f in frames if isinstance(f, ToolNote)] == ["Synthetic tool note"]
        assert raw.entered == 1 and raw.exits == [None] and raw.closed
        assert [f async for f in turn] == []
        assert len(submitter.submissions) <= 1

    asyncio.run(go())


def test_context_is_snapshotted_before_guard_await_and_final_is_authoritative():
    """A guard await cannot substitute another delivery or delta-only candidate."""
    module = product()

    class ContextChangingGuard(GuardBoundary):
        async def start_turn(self, *args, **kwargs):
            _CURRENT_DELIVERY.set(dispatchable(event="event/other"))
            return await super().start_turn(*args, **kwargs)

    async def go():
        submitter = RecordingSubmitter()
        raw = SyntheticStream([TextDelta(text=BLOCK), Final(text=OUTPUT)])
        guard = ContextChangingGuard(raw)
        runner = module.NominationCapturingRunner(guard, submitter)
        async with protected_delivery_scope(dispatchable(event="event/original")):
            turn = await runner.start_turn(
                "https://runner.example.com", EVENT, "synthetic-sandbox-token",
                remaining_s=3, capacity_admission=True, progress={"synthetic": "progress"},
            )
        assert (await drain(turn))[-1].text == PUBLIC
        assert submitter.submissions == [("event/original", BLOCK)]
        assert guard.starts == [(
            "https://runner.example.com", EVENT, "synthetic-sandbox-token",
            {"remaining_s": 3, "capacity_admission": True, "progress": {"synthetic": "progress"}},
        )]
        raw = SyntheticStream([TextDelta(text=BLOCK), Final(text="Plain final")])
        turn = await module.NominationCapturingRunner(GuardBoundary(raw), submitter).start_turn(
            "https://runner.example.com", EVENT
        )
        assert (await drain(turn))[-1].text == "Plain final"
        assert submitter.submissions == [("event/original", BLOCK)]

    asyncio.run(go())


@pytest.mark.parametrize("context", [None, "event/model-supplied", object()])
def test_unbound_or_wrong_context_filters_but_never_submits(context, caplog):
    module = product()

    async def go():
        token = _CURRENT_DELIVERY.set(context)
        try:
            submitter = RecordingSubmitter()
            raw = SyntheticStream([TextDelta(text=OUTPUT), Final(text=OUTPUT)])
            turn = await module.NominationCapturingRunner(GuardBoundary(raw), submitter).start_turn(
                "https://runner.example.com", EVENT
            )
            assert (await drain(turn))[-1].text == PUBLIC
            assert submitter.submissions == []
        finally:
            _CURRENT_DELIVERY.reset(token)

    with caplog.at_level(logging.WARNING):
        asyncio.run(go())
    assert [record.message for record in caplog.records] == ["nomination_unattributed"]


@pytest.mark.parametrize("failure", [None, ValueError("stream broken"), asyncio.CancelledError()])
def test_incomplete_errored_or_cancelled_stream_cannot_submit(failure):
    module = product()

    async def go():
        raw = SyntheticStream([TextDelta(text=OUTPUT)], failure)
        submitter = RecordingSubmitter()
        async with protected_delivery_scope(dispatchable()):
            turn = await module.NominationCapturingRunner(GuardBoundary(raw), submitter).start_turn(
                "https://runner.example.com", EVENT
            )
        if failure is None:
            await drain(turn)
        else:
            with pytest.raises(type(failure)):
                await drain(turn)
        assert submitter.submissions == []
        assert raw.exits == [None if failure is None else type(failure)] and raw.closed

    asyncio.run(go())


def test_guard_refusal_never_returns_capture_or_submits():
    module = product()

    async def go():
        raw = SyntheticStream([Final(text=OUTPUT)])
        submitter = RecordingSubmitter()
        runner = module.NominationCapturingRunner(GuardBoundary(raw, refusal=True), submitter)
        with pytest.raises(ValueError, match="synthetic guard refusal"):
            await runner.start_turn("https://runner.example.com", EVENT)
        assert raw.entered == 0 and submitter.submissions == []

    asyncio.run(go())


@pytest.mark.parametrize("shutdown", ["close", "exit", "mid_iteration"])
def test_closed_stream_cannot_submit_buffered_final(shutdown):
    """Explicit cleanup or abandoned consumption cannot revive a buffered turn."""
    module = product()

    async def go():
        submitter = RecordingSubmitter()
        raw = SyntheticStream([TextDelta(text="public\n"), Final(text=OUTPUT)])
        async with protected_delivery_scope(dispatchable()):
            turn = await module.NominationCapturingRunner(GuardBoundary(raw), submitter).start_turn(
                "https://runner.example.com", EVENT
            )
        if shutdown == "close":
            turn.close()
        elif shutdown == "exit":
            async with turn:
                pass
        else:
            iterator = turn.__aiter__()
            assert (await anext(iterator)).text == "public\n"
            turn.close()
            assert [frame async for frame in iterator] == []
        assert [frame async for frame in turn] == []
        assert raw.closed and submitter.submissions == []

    asyncio.run(go())


def test_actual_runner_stream_and_http_submitter_use_separate_credentials():
    """Real local HTTP peers prove NDJSON capture and the closed worker request."""
    module = product()

    async def go():
        transcript = []
        requests = []

        async def event(request):
            assert request.headers["Authorization"] == "Bearer synthetic-sandbox-token"
            assert "X-Curie-Worker-Token" not in request.headers
            transcript.append(OUTPUT)
            frames = [TextDelta(text=OUTPUT[:23]), TextDelta(text=OUTPUT[23:]), Final(text=OUTPUT)]
            return web.Response(
                text="".join(f.model_dump_json() + "\n" for f in frames),
                headers={"X-Curie-Turn-Epoch": "e" * 32},
            )

        async def nomination(request):
            requests.append((dict(request.headers), await request.json()))
            return web.json_response({"event_id": "event/http", "nomination_ids": []})

        app = web.Application()
        app.add_routes([web.post("/v1/event", event),
                        web.post("/v1/internal/remediation/nominations", nomination)])
        async with TestServer(app) as server:
            base = str(server.make_url("")).rstrip("/")
            submitter = module.HttpNominationSubmitter(base, "synthetic-worker-token")
            async with RunnerClient() as client:
                runner = module.NominationCapturingRunner(client, submitter)
                for _ in range(2):  # Retry uses original event identity, API owns idempotency.
                    async with protected_delivery_scope(dispatchable(event="event/http")):
                        turn = await runner.start_turn(base, EVENT, "synthetic-sandbox-token")
                    frames = await drain(turn)
                    assert frames[-1].text == PUBLIC and turn.turn_epoch == "e" * 32
            assert transcript == [OUTPUT, OUTPUT]
            assert len(requests) == 2
            for headers, body in requests:
                assert headers["X-Curie-Worker-Token"] == "synthetic-worker-token"
                assert "Authorization" not in headers
                assert body == {"event_id": "event/http", "block": BLOCK}
            assert "synthetic-worker-token" not in repr(submitter)

    asyncio.run(go())


@pytest.mark.parametrize("status", [302, 401, 409, 503])
def test_submission_refusal_does_not_follow_redirect_or_echo_sensitive_body(status):
    module = product()

    async def go():
        redirected = []

        async def refuse(request):
            return web.Response(status=status, text="synthetic-worker-token " + BLOCK,
                                headers={"Location": "/forbidden"})

        async def forbidden(request):
            redirected.append(request.path)
            return web.Response()

        app = web.Application()
        app.add_routes([web.post("/v1/internal/remediation/nominations", refuse),
                        web.route("*", "/forbidden", forbidden)])
        async with TestServer(app) as server:
            submitter = module.HttpNominationSubmitter(
                str(server.make_url("")).rstrip("/"), "synthetic-worker-token"
            )
            with pytest.raises(module.NominationSubmissionError) as error:
                await submitter.submit(event_id="event/http", block=BLOCK)
            assert str(error.value) == "nomination submission failed"
            assert error.value.__context__ is None and error.value.__cause__ is None
            assert redirected == []

    asyncio.run(go())


def test_submission_timeout_is_bounded_and_redacted():
    module = product()

    async def go():
        async def stall(request):
            await asyncio.sleep(0.2)
            return web.Response(text="synthetic-worker-token")

        app = web.Application()
        app.add_routes([web.post("/v1/internal/remediation/nominations", stall)])
        async with TestServer(app) as server:
            submitter = module.HttpNominationSubmitter(
                str(server.make_url("")).rstrip("/"), "synthetic-worker-token", timeout_s=0.01
            )
            with pytest.raises(module.NominationSubmissionError) as error:
                await submitter.submit(event_id="event/http", block=BLOCK)
            assert str(error.value) == "nomination submission failed"
            assert error.value.__context__ is None and error.value.__cause__ is None

    asyncio.run(go())


@pytest.mark.parametrize("operation", ["submit", "configuration"])
def test_submitter_detaches_entire_exception_graph_from_active_caller_error(operation, monkeypatch):
    """Nested caller handling must not attach a secret-bearing ambient cause."""
    module = product()

    def unavailable_session(**kwargs):
        raise aiohttp.ClientError("synthetic-inner-credential")

    monkeypatch.setattr(aiohttp, "ClientSession", unavailable_session)

    async def go():
        submitter = module.HttpNominationSubmitter(
            "https://api.example.com", "synthetic-worker-token"
        )
        try:
            raise ValueError("synthetic-outer-credential")
        except ValueError:
            expected = (
                "nomination submission failed" if operation == "submit"
                else "nomination submitter configuration is invalid"
            )
            exception_type = (
                module.NominationSubmissionError if operation == "submit" else ValueError
            )
            with pytest.raises(exception_type) as failure:
                if operation == "submit":
                    await submitter.submit(event_id="event/offline", block=BLOCK)
                else:
                    module.HttpNominationSubmitter(
                        "https://api.example.com", "synthetic-worker-token", timeout_s=0
                    )
            nodes = []
            pending = [failure.value]
            while pending:
                node = pending.pop()
                if any(node is seen for seen in nodes):
                    continue
                nodes.append(node)
                pending.extend(
                    linked for linked in (node.__context__, node.__cause__) if linked is not None
                )
            assert [str(node) for node in nodes] == [expected]
            assert failure.value.__context__ is None and failure.value.__cause__ is None

    asyncio.run(go())


@pytest.mark.parametrize("timeout", [0, -1, 11, float("nan"), float("inf")])
def test_submitter_rejects_unbounded_timeout(timeout):
    with pytest.raises(ValueError, match="nomination submitter configuration is invalid"):
        product().HttpNominationSubmitter(
            "https://api.example.com", "synthetic-worker-token", timeout_s=timeout
        )
