"""Synthetic caller/worker boundaries for opt-in final-answer publication."""

from __future__ import annotations

import asyncio

import pytest
from aci_protocol import Event, SessionStatus
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_worker.kernel.delivery import _StreamAccumulator
from curie_worker.runner_client import RunnerClient, RunnerError


@pytest.mark.parametrize("advertised,expected", [(None, False), ("1", True)])
def test_final_answer_mode_is_bound_to_event_response(advertised, expected):
    async def run():
        received = []

        async def event(request):
            received.append(request.headers.get("X-Curie-Final-Answer"))
            headers = {} if advertised is None else {"X-Curie-Final-Answer": advertised}
            return web.Response(text="", headers=headers)

        app = web.Application()
        app.router.add_post("/v1/event", event)
        async with TestServer(app) as server:
            client = RunnerClient(total_timeout_s=5)
            try:
                turn = await client.start_turn(
                    str(server.make_url("")).rstrip("/"),
                    Event(type="message", text="hello", user="U0EXAMPLE", ts="1"),
                )
                try:
                    assert turn.verbatim_answer is expected
                    assert received == ["1"]
                finally:
                    turn.close()
            finally:
                await client.close()

    asyncio.run(run())


@pytest.mark.parametrize("unsupported", ["2", "", "true", "1, 2"])
def test_unknown_final_answer_response_mode_is_refused(unsupported):
    async def run():
        async def event(request):
            return web.Response(text="", headers={"X-Curie-Final-Answer": unsupported})

        app = web.Application()
        app.router.add_post("/v1/event", event)
        async with TestServer(app) as server:
            client = RunnerClient(total_timeout_s=5)
            try:
                with pytest.raises(RunnerError, match="final-answer"):
                    await client.start_turn(
                        str(server.make_url("")).rstrip("/"),
                        Event(type="message", text="hello", user="U0EXAMPLE", ts="1"),
                    )
            finally:
                await client.close()

    asyncio.run(run())


def test_validated_done_answer_excludes_all_platform_text_decorations():
    acc = _StreamAccumulator(
        final_text="Approved exactly.\n",
        status=SessionStatus.DONE,
        receipt_mode="all",
        workspace_inferred_repo="acme-corp/acme-bot",
        verbatim_answer=True,
        final_text_mode="verbatim",
    )
    acc.receipt_rows = [
        {"tool": "Edit", "arguments": {"file_path": "example.py"}, "status": "succeeded"}
    ]
    assert acc.delivers_verbatim
    assert acc.rendered_with_receipt() == "Approved exactly.\n"


def test_unvalidated_done_answer_of_a_constrained_runner_keeps_rendering():
    """A workflow-scoped bundle's ordinary lookup is not an approved answer."""
    acc = _StreamAccumulator(
        final_text="Here is the template.",
        status=SessionStatus.DONE,
        workspace_inferred_repo="acme-corp/acme-bot",
        verbatim_answer=True,
    )
    assert not acc.delivers_verbatim
    assert acc.rendered_with_receipt() != "Here is the template."
    assert acc.rendered_with_receipt().startswith("Here is the template.")


def test_unnegotiated_verbatim_final_fails_closed(make_harness):
    from aci_protocol import Final
    from curie_worker.kernel import attempt

    async def run():
        acc = _StreamAccumulator()
        await attempt._apply_frame(
            None,  # type: ignore[arg-type]
            Final(text="approved", text_mode="verbatim"),
            acc,
            None,  # type: ignore[arg-type]
            None,  # type: ignore[arg-type]
            None,
        )
        assert acc.status is SessionStatus.CLASSIFIED_FAILURE
        assert acc.classification == "final-answer-validation"
        assert acc.final_text != "approved"
        assert not acc.delivers_verbatim

    asyncio.run(run())


def test_declared_idle_outcome_keeps_ordinary_lifecycle_rendering():
    acc = _StreamAccumulator(
        final_text="run interrupted",
        status=SessionStatus.IDLE_AWAITING_INPUT,
        workspace_inferred_repo="acme-corp/acme-bot",
        verbatim_answer=True,
    )
    assert acc.rendered_with_receipt().startswith("run interrupted")


@pytest.mark.parametrize("flow", ["validated", "lookup"])
def test_real_runner_gate_and_worker_replay_preserve_approved_text(make_harness, flow):
    """Synthetic SDK → actual HTTP → kernel → real Valkey → replayed wire.

    ``lookup`` is the same workflow-scoped bundle answering without its
    declared trigger: delivered, but through ordinary rendering.
    """
    import hashlib
    import json

    from claude_agent_sdk import (
        AssistantMessage,
        TextBlock,
        ToolResultBlock,
        ToolUseBlock,
        UserMessage,
    )
    from curie_runner import RunTracer, SideEffectClassifier, create_app
    from curie_runner.fake import FakeModelSession, _result
    from curie_runner.final_answer import FinalAnswerPolicy
    from curie_runner.session import SessionRunner
    from curie_worker.reply_sink import TargetRoute

    async def run():
        tool = "mcp__answer_checks__finalize_answer"
        draft = "synthetic draft"
        approved = "  ```curie-reply\nnot JSON 한글\n```\n"
        script = [
            AssistantMessage(content=[TextBlock(text="internal preamble")], model="synthetic"),
            AssistantMessage(
                content=[ToolUseBlock(id="check", name=tool, input={"draft": draft})],
                model="synthetic",
            ),
            UserMessage(
                content=[
                    ToolResultBlock(
                        tool_use_id="check",
                        content=json.dumps(
                            {
                                "version": 1,
                                "draftSha256": hashlib.sha256(draft.encode()).hexdigest(),
                                "text": approved,
                            }
                        ),
                        is_error=False,
                    )
                ]
            ),
            AssistantMessage(content=[TextBlock(text=approved)], model="synthetic"),
            _result(text=approved),
        ]
        trigger = "mcp__deck_review__inspect_deck"
        if flow == "validated":
            script.insert(
                0,
                AssistantMessage(
                    content=[ToolUseBlock(id="inspect", name=trigger, input={})],
                    model="synthetic",
                ),
            )
            script.insert(
                1,
                UserMessage(
                    content=[ToolResultBlock(tool_use_id="inspect", content="ok", is_error=False)]
                ),
            )
        else:
            approved = "Here is the *template*."
            script = [_result(text=approved)]
        fake = FakeModelSession(lambda: script)
        runner = SessionRunner(
            session_factory=lambda: fake,
            ceiling=0,
            max_usd_per_day=None,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(readonly_tools={tool}),
            trace_name="synthetic-final-answer",
            held_secrets=frozenset(),
            final_answer=FinalAnswerPolicy(tool=tool, triggers=frozenset({trigger})),
        )
        await runner.start()
        try:
            async with (
                TestServer(create_app(runner)) as server,
                make_harness(shimmer=False, turn_receipt="all", completion_sweep_grace_s=0) as h,
            ):
                # The fixture claims only its own substrate handle. Its HTTP
                # caller opens the actual configured runner below.
                from aci_protocol import QueuedTurn, ReplyHandle, TurnSource
                from curie_worker.sandbox.types import SandboxHandle

                qe = QueuedTurn(
                    event_id="synthetic-validated-replay",
                    conversation_id="th-example",
                    author="U0EXAMPLE",
                    text="question",
                    reply_handle=ReplyHandle(
                        kind="slack", channel="C0EXAMPLE1", placeholder="p-example"
                    ),
                    received_at="2026-01-01T00:00:00Z",
                    source=TurnSource.SLACK,
                )
                handle = SandboxHandle(
                    thread_key=qe.conversation_id,
                    claim_name="example-claim",
                    sandbox_name="example-runner",
                    namespace="example",
                    service_fqdn="127.0.0.1",
                    port=server.port,
                    session_id="example-session",
                )
                client = RunnerClient(total_timeout_s=5)
                try:
                    turn = await client.start_turn(
                        str(server.make_url("")).rstrip("/"),
                        Event(type="message", text="question", user="U0EXAMPLE", ts="1"),
                    )
                    outcome = await h.kernel._consume(
                        qe,
                        TargetRoute(),
                        turn,
                        handle=handle,
                        workspace_inferred_repo="acme-corp/acme-bot",
                    )
                finally:
                    await client.close()
                if flow == "lookup":
                    assert outcome.terminal_ok and outcome.text.startswith(approved)
                    assert outcome.text != approved  # receipt/notice rendering applies
                    assert outcome.answer is not None and outcome.answer.text_mode is None
                    return
                assert outcome.terminal_ok and outcome.text == approved
                assert outcome.answer is not None and outcome.answer.version == "1.2"
                assert outcome.answer.text_mode == "verbatim" and outcome.answer.nav is None
                assert h.sink.updates == []
                h.sink.fail_events = {"reply.update"}
                await h.kernel._complete(
                    qe, TargetRoute(), "delivered", telemetry_outcome="done", turn=outcome
                )
                stored = await h.kernel._markers.read_completion(qe.event_id)
                assert (
                    stored is not None
                    and stored.record.answer == outcome.answer
                    and stored.generation.startswith("answer-v2:")
                )
                h.sink.fail_events.clear()
                await h.kernel.sweep_pending_completions()
                assert h.sink.last_text == approved
                assert await h.kernel._markers.read_completion(qe.event_id) is None
                # A successful outbox replay needs no second SDK query.
                assert len(fake.queries) == 1
        finally:
            await runner.close()

    asyncio.run(run())


def test_duplicate_final_answer_headers_are_refused():
    async def run():
        async def event(request):
            return web.Response(
                text="", headers=[("X-Curie-Final-Answer", "1"), ("X-Curie-Final-Answer", "2")]
            )

        app = web.Application()
        app.router.add_post("/v1/event", event)
        async with TestServer(app) as server:
            client = RunnerClient(total_timeout_s=5)
            try:
                with pytest.raises(RunnerError, match="final-answer"):
                    await client.start_turn(
                        str(server.make_url("")).rstrip("/"),
                        Event(type="message", text="hello", user="U0EXAMPLE", ts="1"),
                    )
            finally:
                await client.close()

    asyncio.run(run())


def test_validation_classification_remains_terminal():
    from curie_worker.kernel import constants, failures

    assert failures.map_error_classification("final-answer-validation") == "final-answer-validation"
    assert "final-answer-validation" not in constants.RETRYABLE_CLASSIFICATIONS


def test_direct_verbatim_finalization_adopts_created_ref(make_harness):
    from channel_protocol.reply import ReplyTarget, ReplyUpdate
    from curie_worker.kernel.delivery import _ThrottledReply
    from curie_worker.reply_sink import TargetRoute

    async def run():
        async with make_harness(shimmer=False) as h:
            adopted = []
            reply = _ThrottledReply(
                h.sink,
                target=ReplyTarget(
                    kind="slack", address="C0EXAMPLE1", conversation_id="th-example", reply_ref=None
                ),
                route=TargetRoute(),
                min_interval_s=0,
                delivery_id="00000000-0000-4000-8000-000000000084",
                on_ref=adopted.append,
            )
            approved = " ```curie-reply\nnot JSON\n```\n"
            await reply.finalize(approved, verbatim=True)
            update = next(event for event, _, _ in h.sink.events if isinstance(event, ReplyUpdate))
            assert (
                update.version == "1.2"
                and update.text_mode == "verbatim"
                and update.text == approved
            )
            assert adopted and reply._target.reply_ref == adopted[0]

    asyncio.run(run())
