"""SessionRunner: turn streaming, interrupt reclassification, rehydrate options."""

import logging

import anyio
from aci_protocol import Event, Interrupt, SessionStatus, parse_ndjson
from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock
from curie_runner import RunTracer, SideEffectClassifier, build_options
from curie_runner.fake import FakeModelSession, default_turn
from curie_runner.session import SessionRunner


def _runner(
    script_factory=default_turn, ceiling: int = 0
) -> tuple[SessionRunner, FakeModelSession]:
    fake = FakeModelSession(script_factory)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=ceiling,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    return runner, fake


def _drain(runner: SessionRunner, frame) -> list:
    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        async for line in runner.run_inbound(frame):
            lines.append(line)

    anyio.run(go)
    return parse_ndjson("".join(lines))


def test_happy_turn_stream_shape() -> None:
    runner, fake = _runner()
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))
    types = [e.type for e in events]
    assert types[0] == "text_delta"
    assert "tool_note" in types
    assert "side_effect_flag" in types
    assert types[-1] == "final"
    assert events[-1].status == SessionStatus.DONE
    assert fake.queries == ["go"]  # the event text was pushed into the session
    assert runner.status == SessionStatus.DONE


def test_turn_lifecycle_logged(caplog) -> None:
    runner, _ = _runner()
    event = Event(type="message", text="go", user="U-log", ts="1")

    with caplog.at_level(logging.INFO, logger="curie_runner.session"):
        events = _drain(runner, event)

    messages = [record.getMessage() for record in caplog.records]
    assert events[-1].status == SessionStatus.DONE
    assert any("turn start" in message and "user=U-log" in message for message in messages)
    assert any("turn end" in message and "status=done" in message for message in messages)
    assert any("tool call" in message and "tool=Bash" in message for message in messages)


def test_bare_interrupt_yields_idle_final() -> None:
    runner, _ = _runner()
    events = _drain(runner, Interrupt(reason="stop"))
    assert [e.type for e in events] == ["final"]
    assert events[0].status == SessionStatus.IDLE_AWAITING_INPUT


def test_midturn_interrupt_reclassifies_final_to_idle() -> None:
    # Interrupt delivered while the turn is live: the fake truncates its replay
    # (as the SDK's native interrupt would), the turn ends without a model result,
    # and the fallback final is idle-awaiting-input rather than done.
    runner, fake = _runner()  # default_turn: several messages before the result

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        lines.append(await gen.__anext__())  # consume the first outbound event
        assert runner.turn_active
        await runner.interrupt("user stop")  # side-channel interrupt mid-turn
        async for line in gen:
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.IDLE_AWAITING_INPUT
    assert fake.interrupts >= 1


def test_interrupt_reclassifies_error_result_to_idle() -> None:
    # The other real interrupt shape: the SDK still delivers a terminal *error*
    # result after the interrupt. An intentional stop must read as idle, not a
    # classified failure.
    script = [
        AssistantMessage(content=[TextBlock(text="working")], model="m"),
        ResultMessage(
            subtype="error_during_execution", duration_ms=1, duration_api_ms=1,
            is_error=True, num_turns=1, session_id="s", result="aborted",
        ),
    ]
    fake = FakeModelSession(lambda: script, truncate_on_interrupt=False)
    runner = SessionRunner(
        session_factory=lambda: fake, ceiling=0, tracer=RunTracer(None),
        classifier=SideEffectClassifier(), trace_name="t",
    )

    lines: list[str] = []

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        lines.append(await gen.__anext__())
        await runner.interrupt("user stop")
        async for line in gen:
            lines.append(line)

    anyio.run(go)
    events = parse_ndjson("".join(lines))
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.IDLE_AWAITING_INPUT


def test_sdk_exception_still_terminates_in_final() -> None:
    # If the model session raises mid-turn, the ACI stream must still end in a
    # classified-failure final (never a truncated, final-less stream).
    class RaisingSession:
        async def connect(self) -> None: ...
        async def query(self, text: str) -> None:
            raise RuntimeError("cli disconnected")
        async def receive_turn(self):  # pragma: no cover - never reached
            if False:
                yield None
        async def interrupt(self) -> None: ...
        async def close(self) -> None: ...

    runner = SessionRunner(
        session_factory=RaisingSession, ceiling=0, tracer=RunTracer(None),
        classifier=SideEffectClassifier(), trace_name="t",
    )
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))
    assert [e.type for e in events] == ["error", "final"]
    assert events[0].classification == "runner-error"
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE


def test_sdk_exception_logs_turn_failure(caplog) -> None:
    class RaisingSession:
        async def connect(self) -> None: ...

        async def query(self, text: str) -> None:
            raise RuntimeError("authentication_failed")

        async def receive_turn(self):  # pragma: no cover - never reached
            if False:
                yield None

        async def interrupt(self) -> None: ...

        async def close(self) -> None: ...

    runner = SessionRunner(
        session_factory=RaisingSession,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )
    with caplog.at_level(logging.ERROR, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    messages = [record.getMessage() for record in caplog.records]
    assert [e.type for e in events] == ["error", "final"]
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert any(
        record.levelno == logging.ERROR
        and "turn failed" in record.getMessage()
        and "RuntimeError" in record.getMessage()
        for record in caplog.records
    )
    assert any("authentication_failed" in message for message in messages)


def test_auth_rejection_fails_fast_not_retried(caplog) -> None:
    # A rejected model credential (provider 401/403 -> AssistantMessage.error
    # "authentication_failed") must surface a DISTINCT, immediate classified
    # failure -- not a generic error streamed while the SDK/CLI retries with
    # backoff to the ~2min wall. The script places messages AFTER the auth error
    # that must never be consumed (proving the turn aborted at the rejection and
    # did not keep driving the session).
    sentinel = "SHOULD-NOT-APPEAR-AFTER-AUTH-FAIL"
    script = [
        AssistantMessage(content=[], model="m", error="authentication_failed"),
        AssistantMessage(content=[TextBlock(text=sentinel)], model="m"),
        ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1,
            is_error=False, num_turns=1, session_id="s", result=sentinel,
        ),
    ]
    runner, fake = _runner(lambda: script)

    with caplog.at_level(logging.ERROR, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    # Distinct, terminal, credential-rejected classification (not "runner-error",
    # not "budget-exceeded", not the raw SDK "authentication_failed").
    assert [e.type for e in events] == ["error", "final"]
    assert events[0].classification == "model-credential-rejected"
    assert "CURIE_CREDENTIALS" in events[0].message
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE
    # Fast-fail: aborted at the rejection, never consuming later messages, and
    # interrupted the live session so the CLI stops retrying.
    assert all(sentinel not in getattr(e, "text", "") for e in events)
    assert fake.interrupts >= 1
    assert any(
        record.levelno == logging.ERROR and "auth failure" in record.getMessage()
        for record in caplog.records
    )


def test_auth_fast_fail_survives_a_wedged_interrupt(caplog) -> None:
    # Hardening for the fast-fail: if interrupt() itself RAISES (a wedged
    # transport -- the very state a bad credential can cause), the exception must
    # NOT propagate to the generic *retryable* runner-error handler. The turn must
    # still surface the terminal model-credential-rejected classification so the
    # auth failure is never retried back into the ~2min hang.
    class WedgedInterruptSession(FakeModelSession):
        async def interrupt(self) -> None:
            self.interrupts += 1
            raise RuntimeError("transport wedged")

    script = [AssistantMessage(content=[], model="m", error="authentication_failed")]
    fake = WedgedInterruptSession(lambda: script)
    runner = SessionRunner(
        session_factory=lambda: fake,
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="t",
    )

    with caplog.at_level(logging.ERROR, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    # Still the terminal credential-rejected classification -- NOT the retryable
    # generic "runner-error", even though interrupt() raised.
    assert [e.type for e in events] == ["error", "final"]
    assert events[0].classification == "model-credential-rejected"
    assert "runner-error" not in [getattr(e, "classification", None) for e in events]
    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert runner.status == SessionStatus.CLASSIFIED_FAILURE
    assert fake.interrupts >= 1  # the interrupt was attempted (and swallowed)


def test_transient_model_error_is_not_fast_failed() -> None:
    # A transient AssistantMessage.error (e.g. a hard rate-limit) is NOT a
    # credential rejection: it must flow through translation unchanged and reach
    # the model's own terminal result, so genuine retry/backoff is preserved.
    script = [
        AssistantMessage(content=[], model="m", error="rate_limit"),
        ResultMessage(
            subtype="success", duration_ms=1, duration_api_ms=1,
            is_error=False, num_turns=1, session_id="s", result="recovered",
        ),
    ]
    runner, fake = _runner(lambda: script)
    events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    classifications = [getattr(e, "classification", None) for e in events]
    assert "model-credential-rejected" not in classifications
    assert "rate_limit" in classifications  # translated, non-terminal
    assert events[-1].status == SessionStatus.DONE  # reached the model's result
    assert fake.interrupts == 0  # not aborted


def test_budget_halt_logged(caplog) -> None:
    script = [
        AssistantMessage(
            content=[TextBlock(text="thinking hard")],
            model="fake",
            usage={"output_tokens": 500},
        ),
        ResultMessage(
            subtype="success",
            duration_ms=1,
            duration_api_ms=1,
            is_error=False,
            num_turns=1,
            session_id="s",
            result="done",
            usage={"output_tokens": 500},
        ),
    ]
    runner, _ = _runner(lambda: script, ceiling=10)

    with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert any(
        record.levelno == logging.WARNING and "budget halt" in record.getMessage()
        for record in caplog.records
    )


def test_error_result_body_not_logged(caplog) -> None:
    # An error *result* turn builds ErrorEvent(message=result); the "model error"
    # ERROR record must log only the structural classification, never the result body
    # (which is the model output / Final.text). No prior interrupt, so the turn is
    # a plain classified failure and translate takes the ErrorEvent(message=text)
    # branch.
    sentinel = "SENTINEL-RESULT-BODY-7c2e"
    script = [
        AssistantMessage(content=[TextBlock(text="working")], model="m"),
        ResultMessage(
            subtype="error_during_execution", duration_ms=1, duration_api_ms=1,
            is_error=True, num_turns=1, session_id="s", result=sentinel,
        ),
    ]
    runner, _ = _runner(lambda: script)

    with caplog.at_level(logging.WARNING, logger="curie_runner.session"):
        events = _drain(runner, Event(type="message", text="go", user="U", ts="1"))

    assert events[-1].status == SessionStatus.CLASSIFIED_FAILURE
    assert any(
        record.levelno == logging.ERROR and "model error" in record.getMessage()
        for record in caplog.records
    )
    assert all(sentinel not in record.getMessage() for record in caplog.records)


def test_turn_logging_does_not_include_message_body(caplog) -> None:
    runner, _ = _runner()
    sentinel = "SENTINEL-SECRET-BODY-9f3a"

    with caplog.at_level(logging.INFO, logger="curie_runner.session"):
        _drain(runner, Event(type="message", text=sentinel, user="U", ts="1"))

    assert all(sentinel not in record.getMessage() for record in caplog.records)


def test_steer_rejected_once_final_is_produced() -> None:
    # Finish-race guard: the moment the terminal final is produced, the turn no
    # longer accepts steers -- even though the generator has not fully closed.
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        async for line in gen:
            if parse_ndjson(line)[0].type == "final":
                assert runner.turn_active is False
                assert await runner.steer("too late") is False

    anyio.run(go)
    assert fake.queries == ["go"]  # the late steer never reached the session


def test_abandoned_stream_interrupts_the_sdk() -> None:
    # Consumer disconnect mid-turn (GeneratorExit via aclose) must stop the SDK so
    # it does not keep running tools past the released turn.
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="go", user="U", ts="1"))
        await gen.__anext__()  # turn live, mid-run
        assert runner.turn_active
        await gen.aclose()  # consumer walks away before the terminal final
        assert runner.turn_active is False

    anyio.run(go)
    assert fake.interrupts >= 1


def test_cross_task_abandon_does_not_wedge_turn_lock() -> None:
    # Client-disconnect mid-stream (#679): the server drives run_turn and, when
    # response.write() raises on disconnect, the suspended generator is finalized
    # by the asyncgen GC on a DIFFERENT task than the one that opened it. The turn
    # lock must survive that cross-task teardown. An anyio.Lock released off-owner
    # raises "current task is not holding this lock" AND leaves itself held,
    # wedging every future turn forever; a Semaphore's owner-agnostic release does
    # not. This closes the generator from a child task, then asserts a fresh turn
    # still acquires the lock and runs to a terminal final.
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="first", user="U", ts="1"))
        await gen.__anext__()  # turn live; the lock is held by THIS task
        assert runner.turn_active

        # Finalize the abandoned generator on a different task, mimicking the
        # asyncgen GC finalizer running off the driving frame. With the old
        # anyio.Lock this raises and wedges the lock; the task group would then
        # propagate the RuntimeError and fail the test here.
        async with anyio.create_task_group() as tg:
            tg.start_soon(gen.aclose)
        assert runner.turn_active is False

        # The discriminator: a subsequent turn must acquire the lock and finish.
        # fail_after turns a permanent wedge into a failure instead of a hang.
        lines: list[str] = []
        with anyio.fail_after(5):
            async for line in runner.run_turn(
                Event(type="message", text="second", user="U", ts="2")
            ):
                lines.append(line)

        events = parse_ndjson("".join(lines))
        assert events[-1].type == "final"
        assert events[-1].status == SessionStatus.DONE

    anyio.run(go)
    assert fake.queries[-1] == "second"  # the second turn actually ran


def test_build_options_carries_resume_ref() -> None:
    # Rehydrate-from-history (ADR-0003): a history ref becomes the SDK resume id.
    options = build_options(
        plugins=[],
        model="claude-opus-4-8",
        system_prompt=None,
        max_turns=20,
        max_budget_usd=5.0,
        resume="s3://history/thread-42",
    )
    assert options.resume == "s3://history/thread-42"
    assert options.max_budget_usd == 5.0
    assert options.permission_mode == "bypassPermissions"


def test_build_options_no_history_ref_is_none() -> None:
    options = build_options(
        plugins=[], model=None, system_prompt=None, max_turns=20,
        max_budget_usd=1.0, resume=None,
    )
    assert options.resume is None
    assert options.task_budget is None


def test_build_options_carries_task_budget_hint() -> None:
    # The ACI task_budget_hint (soft pacing) becomes the SDK task_budget.
    options = build_options(
        plugins=[], model=None, system_prompt=None, max_turns=20,
        max_budget_usd=1.0, resume=None, task_budget_hint=64000,
    )
    assert options.task_budget == {"total": 64000}


def test_steer_reaches_live_session() -> None:
    # A steer injects into the live turn: its text lands on the session as a
    # second query while the turn's stream is still open.
    runner, fake = _runner()

    async def go() -> None:
        await runner.start()
        gen = runner.run_turn(Event(type="message", text="first", user="U", ts="1"))
        await gen.__anext__()  # turn is now live
        assert await runner.steer("steered follow-up") is True
        async for _ in gen:
            pass

    anyio.run(go)
    assert fake.queries == ["first", "steered follow-up"]
