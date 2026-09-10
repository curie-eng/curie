"""Kernel rule tests: routing, steer, finish-race, interrupt, side-effect/retry.

Each rule is provoked against a real Valkey, the real G1 substrate, and a
scriptable in-process fake runner; only Slack and the model are faked.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import (
    ErrorEvent,
    Final,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    SideEffectFlag,
    TextDelta,
    ToolNote,
    TurnSource,
)
from channel_protocol.reply import ReplyAck, ReplyEvent, ReplyTarget
from curie_worker import kernel as kernel_module
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.kernel import ThreadBusyError
from curie_worker.reply_sink import TargetRoute
from curie_worker.runner_client import RunnerError, TurnStream
from curie_worker.sandbox import QuotaRejection
from curie_worker.workspace import (
    WorkspacePreparationError,
    WorkspaceSelectionRefused,
)

DONE = SessionStatus.DONE
IDLE = SessionStatus.IDLE_AWAITING_INPUT
FAIL = SessionStatus.CLASSIFIED_FAILURE


def _qevent(
    text: str,
    *,
    thread: str = "th-1",
    event_id: str | None = None,
    placeholder: str | None = "p-1",
    endpoint: str | None = None,
    adapter: str | None = None,
    source: TurnSource = TurnSource.SLACK,
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or uuid.uuid4().hex,
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(
            kind="slack",
            channel="C1",
            placeholder=placeholder,
            endpoint=endpoint,
            adapter=adapter,
        ),
        received_at="2026-07-05T00:00:00+00:00",
        source=source,
    )


def _thread_key(thread: str) -> str:
    return f"slack:C1:{thread}"


async def _wait_until(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


def _spy_next_turn_release(kernel: Any) -> dict[str, int]:
    calls = {"n": 0}
    real_start_turn = kernel._runner.start_turn

    async def start_turn(*args: Any, **kwargs: Any) -> TurnStream:
        turn = await real_start_turn(*args, **kwargs)
        real_release = turn._response.release

        def release() -> Any:
            calls["n"] += 1
            return real_release()

        turn._response.release = release
        return turn

    kernel._runner.start_turn = start_turn
    return calls


def test_new_turn_streams_to_slack_and_acks(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                TextDelta(text="Hello "),
                TextDelta(text="world"),
                Final(text="Hello world", status=DONE),
            ]
            ev = _qevent("hi")
            await h.kernel.process_event(ev)

            assert h.runner.opened == ["hi"]
            assert h.sink.last_text == "Hello world"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_post_final_stall_does_not_reclassify_or_retry_success(make_harness) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=3) as h:
            hold = asyncio.Event()
            h.runner.default_script = [Final(text="answer", status=DONE)]
            h.runner.hold = hold
            event = _qevent("hi")
            try:
                await asyncio.wait_for(h.kernel.process_event(event), timeout=2.0)
                assert h.runner.opened == ["hi"]
                assert h.sink.last_text == "answer"
                assert await h.async_redis.exists(h.config.done_key(event.event_id))
            finally:
                hold.set()

    asyncio.run(go())


def test_cancellation_while_route_lock_exits_releases_open_runner_response(
    make_harness,
) -> None:
    class BlockingExitLock:
        def __init__(self, inner: object) -> None:
            self._inner = inner
            self.exit_started = asyncio.Event()
            self.unblock = asyncio.Event()

        @asynccontextmanager
        async def hold(self, key: str) -> AsyncIterator[object]:
            async with self._inner.hold(key) as lease:  # type: ignore[attr-defined]
                yield lease
                self.exit_started.set()
                await self.unblock.wait()

    async def go() -> None:
        async with make_harness() as h:
            runner_hold = asyncio.Event()
            h.runner.hold = runner_hold
            h.runner.default_script = [Final(text="answer", status=DONE)]
            lock = BlockingExitLock(h.kernel._lock)
            h.kernel._lock = lock  # type: ignore[assignment]
            release_calls = _spy_next_turn_release(h.kernel)
            task = asyncio.create_task(h.kernel.process_event(_qevent("hi", thread="tCancel")))
            try:
                await asyncio.wait_for(lock.exit_started.wait(), timeout=1.0)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert release_calls["n"] >= 1
            finally:
                lock.unblock.set()
                runner_hold.set()
                if not task.done():
                    task.cancel()
                await _wait_until(lambda: not h.runner.turn_active)

    asyncio.run(go())


def test_cancellation_during_registered_kill_recheck_releases_runner_response(
    make_harness,
) -> None:
    class BlockingSecondKillCheck:
        def __init__(self) -> None:
            self.calls = 0
            self.entered = asyncio.Event()
            self.unblock = asyncio.Event()

        async def is_killed(self, _agent_id: uuid.UUID) -> bool:
            self.calls += 1
            if self.calls == 1:
                return False
            self.entered.set()
            await self.unblock.wait()
            return False

    async def go() -> None:
        agent_id = uuid.uuid4()
        binding = _TokenBinding("", agent_id)
        async with make_harness(binding=binding) as h:
            runner_hold = asyncio.Event()
            h.runner.hold = runner_hold
            h.runner.default_script = [Final(text="answer", status=DONE)]
            killswitch = BlockingSecondKillCheck()
            h.kernel.attach_killswitch(killswitch)  # type: ignore[arg-type]
            release_calls = _spy_next_turn_release(h.kernel)
            task = asyncio.create_task(h.kernel.process_event(_qevent("hi", thread="tRegistered")))
            try:
                await asyncio.wait_for(killswitch.entered.wait(), timeout=1.0)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert release_calls["n"] >= 1
            finally:
                killswitch.unblock.set()
                runner_hold.set()
                if not task.done():
                    task.cancel()
                await _wait_until(lambda: not h.runner.turn_active)

    asyncio.run(go())


def test_cancellation_during_deferred_job_boot_reply_releases_runner_response(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness() as h:
            runner_hold = asyncio.Event()
            h.runner.hold = runner_hold
            h.runner.default_script = [Final(text="answer", status=DONE)]
            reply_started = asyncio.Event()
            reply_unblock = asyncio.Event()

            async def blocking_reply(*_args: Any, **_kwargs: Any) -> None:
                reply_started.set()
                await reply_unblock.wait()

            h.kernel._reply_for = blocking_reply  # type: ignore[method-assign]
            release_calls = _spy_next_turn_release(h.kernel)
            task = asyncio.create_task(
                h.kernel.process_event(
                    _qevent(
                        "digest",
                        thread="tDeferred",
                        placeholder=None,
                        source=TurnSource.CRON,
                    )
                )
            )
            try:
                await asyncio.wait_for(reply_started.wait(), timeout=1.0)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert release_calls["n"] >= 1
            finally:
                reply_unblock.set()
                runner_hold.set()
                if not task.done():
                    task.cancel()
                await _wait_until(lambda: not h.runner.turn_active)

    asyncio.run(go())


def test_conflicting_runtime_repo_is_terminal_before_claim_or_model(
    make_harness,
) -> None:
    class WorkspaceResolved(_FakeResolved):
        def __init__(self) -> None:
            super().__init__(uuid.uuid4())
            self.deployment_id = uuid.uuid4()
            self.workspace_enabled = True

    class WorkspaceBinding:
        async def resolve(self, _kind: str, _channel: str) -> WorkspaceResolved:
            return WorkspaceResolved()

        def boot_env(
            self,
            _resolved: object,
            _thread_key: str,
            *,
            kind: str | None = None,
            address: str | None = None,
        ) -> dict[str, str]:
            return {}

        def packs_for(self, _resolved: object) -> BehaviorPacks:
            return BehaviorPacks()

    async def go() -> None:
        async with make_harness(binding=WorkspaceBinding()) as h:
            class WorkspaceProbe:
                selected: str | None = None
                claims = 0

                def select_repository(
                    self,
                    *,
                    thread_key: str,
                    deployment_id: uuid.UUID,
                    author: str,
                    repo_full_name: str | None,
                ) -> str:
                    assert thread_key == _thread_key("tRepo")
                    assert author == "U1"
                    if self.selected is None:
                        assert repo_full_name is not None
                        self.selected = repo_full_name
                    elif repo_full_name is not None and repo_full_name != self.selected:
                        raise WorkspaceSelectionRefused(
                            "This thread is already bound to a different repository."
                        )
                    return self.selected

                def claim_or_resume_with_handle(self, **kwargs: object) -> object:
                    self.claims += 1
                    return SimpleNamespace(
                        handle=h.substrate.claim("tRepo", env={}), prepared=None
                    )

                def touch(self, thread_key: str, *, ttl_seconds: int) -> bool:
                    return True

            probe = WorkspaceProbe()
            h.kernel._workspace = probe  # type: ignore[assignment]
            await h.kernel.process_event(
                _qevent(
                    "Change https://github.com/acme-corp/acme-bot",
                    thread="tRepo",
                )
            )
            await h.kernel.process_event(
                _qevent(
                    "Switch to https://github.com/acme-corp/acme-api",
                    thread="tRepo",
                )
            )

            assert probe.claims == 1
            assert h.runner.opened == [
                "Change https://github.com/acme-corp/acme-bot"
            ]
            assert h.sink.last_text == (
                "This thread is already bound to a different repository."
            )

    asyncio.run(go())


def test_workspace_selection_precedes_fresh_thread_greeting(make_harness) -> None:
    class WorkspaceResolved(_FakeResolved):
        def __init__(self) -> None:
            super().__init__(uuid.uuid4())
            self.deployment_id = uuid.uuid4()
            self.workspace_enabled = True

    class WorkspaceBinding:
        async def resolve(self, _kind: str, _channel: str) -> WorkspaceResolved:
            return WorkspaceResolved()

        def boot_env(
            self,
            _resolved: object,
            _thread_key: str,
            *,
            kind: str | None = None,
            address: str | None = None,
        ) -> dict[str, str]:
            return {}

        def packs_for(self, _resolved: object) -> BehaviorPacks:
            return BehaviorPacks.from_config(
                {
                    "greeting": {
                        "enabled": True,
                        "phrases": ["hi"],
                        "reply": "Hello from the greeting pack.",
                    }
                }
            )

    async def go() -> None:
        async with make_harness(binding=WorkspaceBinding()) as h:
            class WorkspaceProbe:
                def select_repository(self, **kwargs: object) -> str:
                    assert kwargs["repo_full_name"] is None
                    raise WorkspaceSelectionRefused(
                        "Name an allowed GitHub repository in the opening message."
                    )

            h.kernel._workspace = WorkspaceProbe()  # type: ignore[assignment]
            await h.kernel.process_event(_qevent("hi", thread="tGreetingRepo"))

            assert h.sink.last_text == (
                "Name an allowed GitHub repository in the opening message."
            )
            assert h.runner.opened == []
            assert h.fake_k8s.claim_envs == []

    asyncio.run(go())


def test_a_selection_refusal_is_logged_so_an_operator_can_find_it(
    make_harness, caplog
) -> None:
    """The refusal ends the turn; without a log line it ends it invisibly (#2004).

    `WorkspaceSelectionRefused` subclasses `WorkspacePreparationError`, so its
    narrower `except` runs first and the turn never reaches the sibling branch
    that logs "turn start failed". Every other turn-ending branch in that handler
    logs; this one did not.

    The reply covers the Slack case, where the person who asked reads the
    refusal. It covers nothing when the turn came from a hook: there is no
    placeholder to edit and no one watching, so an acknowledged entry with no
    sandbox and no log is indistinguishable from an idle bot -- which is exactly
    how this was found, and the whole of the reported defect.

    Asserts the agent is named, because "no line naming the agent" is what made
    the live install unsearchable.
    """

    class WorkspaceResolved(_FakeResolved):
        def __init__(self) -> None:
            super().__init__(uuid.uuid4())
            self.deployment_id = uuid.uuid4()
            self.workspace_enabled = True

    class WorkspaceBinding:
        async def resolve(self, _kind: str, _channel: str) -> WorkspaceResolved:
            return WorkspaceResolved()

        def boot_env(
            self,
            _resolved: object,
            _thread_key: str,
            *,
            kind: str | None = None,
            address: str | None = None,
        ) -> dict[str, str]:
            return {}

        def packs_for(self, _resolved: object) -> BehaviorPacks:
            return BehaviorPacks.from_config({})

    async def go() -> None:
        async with make_harness(binding=WorkspaceBinding()) as h:
            class WorkspaceProbe:
                def select_repository(self, **kwargs: object) -> str:
                    raise WorkspaceSelectionRefused(
                        "Start the thread by naming one allowed root GitHub "
                        "repository URL."
                    )

            h.kernel._workspace = WorkspaceProbe()  # type: ignore[assignment]
            with caplog.at_level(logging.INFO, logger="curie_worker.kernel"):
                await h.kernel.process_event(_qevent("hi", thread="tRefusalLog"))

            assert h.runner.opened == []
            logged = "\n".join(record.getMessage() for record in caplog.records)
            assert "test-agent" in logged, (
                "the refusal log must name the agent; an operator searching for "
                f"a silent bot has only that to search on. Got: {logged!r}"
            )
            assert "naming one allowed root GitHub repository" in logged, (
                f"the refusal log must carry the reason. Got: {logged!r}"
            )

    asyncio.run(go())


# --- A workspace PREPARATION failure must never be anonymous (#2004) ----------
# The refusal half of this ticket is already covered above, by the INFO line the
# refusal branch emits. These pin the other half: a clone, an upload, or a
# binding carrying no deployment id used to be swallowed by the broad
# start-failure clause, which names an event id and an anonymous repr -- so a
# workspace-enabled turn acked, created no sandbox, and left the operator with
# nothing to search on. They assert the log line, not the reply; the reply was
# never the missing half.


def _workspace_binding(deployment_id: uuid.UUID | None) -> object:
    """A binding for a workspace-enabled deployment with a FIXED deployment id.

    Fixed on purpose: the id is what an operator greps for, so the tests assert
    the exact value reaches the log rather than that some uuid did. ``None`` is
    admitted because it is a real resolved shape -- deployment_id is optional on
    the binding row -- and the misconfiguration it produces is its own test."""

    class WorkspaceResolved(_FakeResolved):
        def __init__(self) -> None:
            super().__init__(uuid.uuid4())
            self.deployment_id = deployment_id
            self.workspace_enabled = True

    class WorkspaceBinding:
        async def resolve(self, _kind: str, _channel: str) -> WorkspaceResolved:
            return WorkspaceResolved()

        def boot_env(
            self,
            _resolved: object,
            _thread_key: str,
            *,
            kind: str | None = None,
            address: str | None = None,
        ) -> dict[str, str]:
            return {}

        def packs_for(self, _resolved: object) -> BehaviorPacks:
            return BehaviorPacks()

    return WorkspaceBinding()


def _workspace_start_failures(caplog: Any) -> list[str]:
    return [r.getMessage() for r in caplog.records if "workspace start failed" in r.getMessage()]


def _workspace_start_failure_records(caplog: Any) -> list[logging.LogRecord]:
    # Companion to _workspace_start_failures: keeps the record itself so a
    # test can assert on level, not just message text.
    return [r for r in caplog.records if "workspace start failed" in r.getMessage()]


def test_workspace_preparation_failure_escalates_by_its_own_name(make_harness, caplog) -> None:
    """#2004: a workspace that cannot be prepared fails LOUDLY and by name.

    Selection succeeds, then the clone fails -- so this is a fault, not the
    deliberate refusal the branch above handles. Pre-fix it fell into the broad
    start-failure clause: the only trace was ``turn start failed for <event id>``
    with an anonymous repr -- naming neither the agent, the deployment, the
    repository, nor the stage -- and it escalated as the generic
    ``runner-error``, indistinguishable from a runner 5xx. This pins both halves:
    the operator line, and the classification the user-visible escalation
    carries. Retry behavior is unchanged, which the escalation-after-3-attempts
    shape (and the real, unprobed ``retry_class`` metric this path emits) still
    proves."""

    caplog.set_level(logging.WARNING, logger="curie_worker.kernel")
    deployment_id = uuid.uuid4()

    async def go() -> None:
        async with make_harness(binding=_workspace_binding(deployment_id), max_attempts=3) as h:
            class WorkspaceProbe:
                def select_repository(
                    self,
                    *,
                    thread_key: str,
                    deployment_id: uuid.UUID,
                    author: str,
                    repo_full_name: str | None,
                ) -> str:
                    assert repo_full_name is not None
                    return repo_full_name

                def claim_or_resume_with_handle(self, **kwargs: object) -> object:
                    raise WorkspacePreparationError(
                        "clone", "git clone exited 128: repository not found"
                    )

                def touch(self, thread_key: str, *, ttl_seconds: int) -> bool:
                    return True

            h.kernel._workspace = WorkspaceProbe()  # type: ignore[assignment]
            await h.kernel.process_event(
                _qevent("Fix https://github.com/acme-corp/acme-bot", thread="tWorkspaceClone")
            )

            # The turn was never accepted: nothing was claimed and no turn opened.
            assert h.fake_k8s.claim_envs == []
            assert h.runner.opened == []

            # Visible terminal failure, under the workspace's own name. Pre-fix
            # this said "runner-error" and pointed operators at the runner.
            assert h.sink.last_text is not None
            assert "workspace-error" in h.sink.last_text, h.sink.last_text
            assert "human" in h.sink.last_text.lower()

            failures = _workspace_start_failures(caplog)
            assert failures, f"the preparation failure was unnamed: {caplog.text!r}"
            message = failures[-1]
            assert "agent=test-agent" in message, message
            assert f"deployment={deployment_id}" in message, message
            assert "acme-corp/acme-bot" in message, message
            assert "stage=clone" in message, message
            assert "repository not found" in message, message

    asyncio.run(go())


def test_workspace_enabled_binding_without_deployment_id_is_named(make_harness, caplog) -> None:
    """#2004: the binding-stage misconfiguration logs before it raises.

    ``deployment_id`` is optional on a resolved binding, so a workspace-enabled
    deployment carrying none is reachable config drift. It is raised outside
    ``_attempt``'s handlers, so it never reached the visibility helper and the
    consumer saw only an anonymous processing exception. The raise is deliberate
    and unchanged -- leaving the entry pending is the right terminal answer for a
    config error -- so it is asserted here, not softened."""

    caplog.set_level(logging.WARNING, logger="curie_worker.kernel")

    async def go() -> None:
        async with make_harness(binding=_workspace_binding(None)) as h:
            with pytest.raises(WorkspacePreparationError):
                await h.kernel.process_event(_qevent("do the thing", thread="tNoDeploymentId"))

            assert h.fake_k8s.claim_envs == []
            assert h.runner.opened == []

            failures = _workspace_start_failures(caplog)
            assert failures, f"the binding misconfiguration was unnamed: {caplog.text!r}"
            message = failures[-1]
            assert "agent=test-agent" in message, message
            assert "stage=binding" in message, message
            assert "has no deployment id" in message, message
            # The whole point of this failure: no deployment id ever reached
            # the log call, and no repository was selected either.
            assert "deployment=<unknown>" in message, message
            assert "repo=<none named>" in message, message
            record = _workspace_start_failure_records(caplog)[-1]
            assert record.levelno == logging.WARNING, record

    asyncio.run(go())


def test_binding_failure_on_an_ambiguous_message_logs_without_re_raising(
    make_harness, caplog
) -> None:
    """#2004: the visibility helper must not raise the refusal it trips over.

    The helper reparses the turn text to name the repository, and
    ``parse_github_repo_fact`` itself RAISES ``WorkspaceSelectionRefused`` on a
    message naming two repositories. That refusal is incidental -- the failure
    being reported is the binding one -- so a naive helper would replace the
    caller's fault with it, and the missing-deployment-id raise below would reach
    the consumer as a refusal instead.

    This is the one reachable path where the guard still bites: the
    ``deployment_id is None`` raise in ``_process_event`` calls the helper with
    the raw turn text, before any selection is attempted. It pins that the
    ambiguity is reported as itself (neither repository can be named without
    lying about which one won) and that the binding failure is still what
    escapes."""

    caplog.set_level(logging.WARNING, logger="curie_worker.kernel")

    async def go() -> None:
        async with make_harness(binding=_workspace_binding(None)) as h:
            with pytest.raises(WorkspacePreparationError) as raised:
                await h.kernel.process_event(
                    _qevent(
                        "Port https://github.com/acme-corp/acme-bot to "
                        "https://github.com/acme-corp/acme-api",
                        thread="tAmbiguousRepo",
                    )
                )

            # The helper swallowed the reparse's refusal instead of letting it
            # stand in for the fault the caller is reporting.
            assert not isinstance(raised.value, WorkspaceSelectionRefused), raised.value
            assert raised.value.stage == "binding", raised.value

            assert h.fake_k8s.claim_envs == []
            assert h.runner.opened == []

            failures = _workspace_start_failures(caplog)
            assert failures, f"the binding misconfiguration was unnamed: {caplog.text!r}"
            message = failures[-1]
            assert "agent=test-agent" in message, message
            assert "repo=<ambiguous>" in message, message
            assert "stage=binding" in message, message
            record = _workspace_start_failure_records(caplog)[-1]
            assert record.levelno == logging.WARNING, record

    asyncio.run(go())



def test_tool_notes_are_consumed_without_reaching_user_facing_updates(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=0.0) as h:
            h.runner.default_script = [
                TextDelta(text="Answer so far"),
                ToolNote(text="searching...", tool="WebSearch"),
                TextDelta(text=" and more"),
                ToolNote(text="opening result", tool="WebSearch"),
                Final(text="Final answer", status=DONE),
            ]
            event = _qevent("research this")

            await h.kernel.process_event(event)

            texts = [text for _, _, text in h.sink.updates]
            assert "Answer so far" in texts
            assert "Answer so far and more" in texts
            assert h.sink.last_text == "Final answer"
            assert all("WebSearch" not in text for text in texts)
            assert all("searching..." not in text for text in texts)
            assert all("opening result" not in text for text in texts)

    asyncio.run(go())


def test_tool_notes_with_empty_or_absent_names_remain_internal(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=0.0) as h:
            h.runner.default_script = [
                TextDelta(text="Answer so far"),
                ToolNote(text="empty name", tool=""),
                ToolNote(text="unnamed", tool=None),
                Final(text="Final answer", status=DONE),
            ]

            await h.kernel.process_event(_qevent("research this"))

            texts = [text for _, _, text in h.sink.updates]
            assert "Answer so far" in texts
            assert h.sink.last_text == "Final answer"
            assert all("empty name" not in text for text in texts)
            assert all("unnamed" not in text for text in texts)

    asyncio.run(go())


def test_tool_notes_never_attempt_a_user_facing_sink_delivery(make_harness) -> None:
    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=60.0) as h:
            h.runner.default_script = [
                TextDelta(text="Partial answer"),
                ToolNote(text="running command", tool="Bash"),
                ToolNote(text="reading output", tool="Bash"),
                Final(text="Completed answer", status=DONE),
            ]
            original_emit = h.sink.emit
            leaked_attempts: list[str] = []

            async def reject_tool_note_emit(
                reply_event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                text = getattr(reply_event, "text", None)
                if isinstance(text, str) and (
                    "running command" in text or "reading output" in text
                ):
                    leaked_attempts.append(text)
                    raise RuntimeError("tool note reached the user-facing sink")
                return await original_emit(
                    reply_event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = reject_tool_note_emit  # type: ignore[method-assign]
            event = _qevent("run it")

            await h.kernel.process_event(event)

            assert leaked_attempts == []
            assert h.sink.last_text == "Completed answer"
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_final_response_is_not_filtered_when_it_matches_tool_note_formatting(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=60.0) as h:
            final_text = "Partial answer\n  -> [Bash] running command"
            h.runner.default_script = [
                TextDelta(text="Partial answer"),
                ToolNote(text="running command", tool="Bash"),
                Final(text=final_text, status=DONE),
            ]
            event = _qevent("run it")

            await h.kernel.process_event(event)

            assert h.sink.last_text == final_text
            assert [text for _, _, text in h.sink.updates].count(final_text) == 1
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_placeholderless_turn_does_not_create_a_message_from_a_tool_note(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                ToolNote(text="running command", tool="Bash"),
                Final(text="Completed answer", status=DONE),
            ]
            event = _qevent("run it", placeholder=None)

            await h.kernel.process_event(event)

            texts = [text for _, _, text in h.sink.updates]
            assert h.sink.last_text == "Completed answer"
            assert all("running command" not in text for text in texts)
            assert all("Bash" not in text for text in texts)
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_null_placeholder_turn_runs_and_posts_its_own_reply(make_harness) -> None:
    """ADR-0079: a turn with nothing to edit creates its own message.

    This asserts the reverse of what the kernel used to do. Until this change it
    raised on a null placeholder before touching the runner or the sink, which
    left the frozen contract (``placeholder: str | None``) and the runtime
    disagreeing about what the wire permitted.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="digest ready", status=DONE)]
            event = _qevent("run the digest", placeholder=None)

            await h.kernel.process_event(event)

            assert h.runner.opened == ["run the digest"]
            # It POSTED rather than edited: the delivery minted a ref of its own.
            assert [text for _, _, text in h.sink.text_posts]
            assert h.sink.last_text == "digest ready"
            assert await h.async_redis.exists(h.config.done_key(event.event_id))

    asyncio.run(go())


def test_placeholder_less_turn_posts_once_then_edits_that_message(make_harness) -> None:
    """The minted ref is adopted, so a streamed job does not spam the channel.

    Without adoption every throttled delta would create another message, which is
    the failure a null placeholder would otherwise produce on a chatty turn.
    """

    async def go() -> None:
        async with make_harness(slack_edit_min_interval_s=0.0) as h:
            h.runner.default_script = [
                TextDelta(text="one "),
                TextDelta(text="two "),
                TextDelta(text="three"),
                Final(text="one two three", status=DONE),
            ]

            await h.kernel.process_event(_qevent("go", placeholder=None))

            # Exactly one message was created for the whole turn...
            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            minted = h.sink.text_posts[0][1]
            # ...and every later delivery addressed that same message.
            refs = {ref for _, ref, _ in h.sink.updates}
            assert refs == {minted}, refs
            assert len(h.sink.updates) > 1, "the turn only delivered once; nothing was edited"

    asyncio.run(go())


def test_a_job_never_steers_a_live_session(make_harness) -> None:
    """ADR-0079: jobs are outputs, not steering inputs.

    A person's follow-up on a busy thread steers. A job on the same thread must
    not, because folding a scheduled digest into someone's conversation changes
    what that person's turn says.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = True
            event = _qevent(
                "nightly digest", placeholder=None, source=TurnSource.CRON
            )

            for _ in range(5):
                with pytest.raises(ThreadBusyError):
                    await h.kernel.process_event(event)

            assert h.sink.text_posts == [], "a deferred job left a booting notice"
            assert h.runner.steers == [], "a job steered a live session"
            assert h.runner.opened == [], "a job opened a turn beside a live one"

    asyncio.run(go())


def test_a_job_runs_normally_when_the_thread_is_idle(make_harness) -> None:
    """The deferral is conditional. An idle thread runs the job immediately."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = False
            h.runner.default_script = [Final(text="digest", status=DONE)]

            await h.kernel.process_event(
                _qevent("nightly digest", placeholder=None, source=TurnSource.CRON)
            )

            assert h.runner.opened == ["nightly digest"]
            assert h.runner.steers == []

    asyncio.run(go())


def test_an_unreadable_session_defers_the_job(make_harness) -> None:
    """The liveness read fails CLOSED.

    A runner that cannot answer is not evidence of an idle thread. Reading the
    failure as idle would open a turn beside one that may already be running,
    which breaks the kernel's one-live-turn-per-thread invariant -- so the job
    defers instead. Added because a mutation flipping this to fail-open left the
    suite green.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = False
            h.runner.status_fails = True

            with pytest.raises(ThreadBusyError):
                await h.kernel.process_event(
                    _qevent("digest", placeholder=None, source=TurnSource.CRON)
                )

            assert h.runner.opened == [], "an unreadable session let a job open a turn"

    asyncio.run(go())


def test_a_status_without_turn_active_defers_the_job(make_harness) -> None:
    """A 200 that omits the field is as unreadable as a 500.

    Separate from the 500 case on purpose: a runner answering successfully with a
    shape we cannot interpret is the likelier real-world drift, and reading a
    missing field as False would silently treat every such runner as idle.
    """

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = False
            h.runner.status_malformed = True

            with pytest.raises(ThreadBusyError):
                await h.kernel.process_event(
                    _qevent("digest", placeholder=None, source=TurnSource.CRON)
                )

            assert h.runner.opened == []

    asyncio.run(go())


def test_streamer_adopts_its_own_minted_ref(make_harness) -> None:
    """A streamer that posts its first delta edits that message thereafter.

    Driven directly rather than through ``process_event`` because the booting
    notice normally mints the ref first, which hides this path: it is reached
    when that delivery failed, and a mutation removing the adoption left the
    end-to-end test green. One message, then edits, is the property.
    """

    async def go() -> None:
        async with make_harness() as h:
            reply = kernel_module._ThrottledReply(
                h.sink,
                target=ReplyTarget(
                    kind="slack", address="C1", conversation_id="th-1", reply_ref=None
                ),
                route=TargetRoute(),
                min_interval_s=0.0,
            )
            await reply.stream("one")
            await reply.stream("one two")
            await reply.finalize("one two three")

            assert len(h.sink.text_posts) == 1, h.sink.text_posts
            minted = h.sink.text_posts[0][1]
            assert {ref for _, ref, _ in h.sink.updates} == {minted}
            assert len(h.sink.updates) == 3

    asyncio.run(go())


def test_a_person_still_steers_a_live_session(make_harness) -> None:
    """The guard is scoped to jobs and leaves the conversational path alone."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_active = True

            await h.kernel.process_event(_qevent("actually, make it shorter"))

            assert h.runner.steers == ["actually, make it shorter"]

    asyncio.run(go())


def test_shimmer_clears_status_when_the_turn_ends(make_harness) -> None:
    # With shimmer on, the kernel clears the assistant-thread status it
    # dispatcher set, on the turn's terminal exit (a plain success here).
    async def go() -> None:
        async with make_harness(shimmer=True) as h:
            h.runner.default_script = [Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tS"))
            assert ("C1", "tS") in h.sink.status_clears

    asyncio.run(go())


def test_no_status_clear_when_shimmer_is_off(make_harness) -> None:
    # With shimmer OFF the kernel never touches the assistant status. Pinned
    # explicitly: shimmer now defaults ON (#1182), so leaning on the default here
    # would silently stop exercising the off path.
    async def go() -> None:
        async with make_harness(shimmer=False) as h:
            h.runner.default_script = [Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("hi"))
            assert h.sink.status_clears == []

    asyncio.run(go())


def test_status_is_cleared_by_default(make_harness) -> None:
    # The mirror of the test above, and the reason the default flipped (#1182):
    # the worker shimmers by default, and editing the placeholder does not
    # auto-clear a Slack status, so the worker must clear it on the way out or
    # the caption lingers until Slack's own timeout.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="done", status=DONE)]
            await h.kernel.process_event(_qevent("hi"))
            assert h.sink.status_clears, "the shipped default must clear the caption"

    asyncio.run(go())


def test_followup_steers_the_live_turn(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            e1 = _qevent("first", thread="tA")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            # A follow-up on the same thread steers the live turn, not a new one.
            await h.kernel.process_event(_qevent("second", thread="tA"))
            assert h.runner.steers == ["second"]
            assert h.runner.opened == ["first"]

            hold.set()
            await t1

    asyncio.run(go())


def test_finish_race_falls_back_to_a_fresh_turn(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # First turn completes; the sandbox stays live but idle (no turn).
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tB"))

            # Follow-up: the steer hits 409 (no active turn) and the kernel opens
            # a fresh turn on the same idle sandbox.
            h.runner.default_script = [Final(text="two", status=DONE)]
            await h.kernel.process_event(_qevent("second", thread="tB"))

            assert h.runner.steers == []  # steer returned 409, not delivered
            assert h.runner.opened == ["first", "second"]
            assert h.sink.last_text == "two"

    asyncio.run(go())


def test_drop_mid_run_retries_then_succeeds(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # Attempt 1 streams a delta then the stream ends with no final (a
            # mid-run drop). Attempt 2 completes.
            h.runner.turn_scripts = [
                [TextDelta(text="partial")],
                [TextDelta(text="full"), Final(text="full done", status=DONE)],
            ]
            ev = _qevent("go")
            await h.kernel.process_event(ev)

            assert h.runner.opened == ["go", "go"]  # retried
            assert h.sink.last_text == "full done"

    asyncio.run(go())


def test_side_effect_failure_escalates_without_retry(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # A normally-retryable classification (runner-error) must NOT retry
            # once a side effect has executed.
            h.runner.default_script = [
                SideEffectFlag(tool="deploy"),
                ErrorEvent(message="boom", classification="runner-error"),
                Final(text="failed", status=FAIL),
            ]
            ev = _qevent("do it")
            await h.kernel.process_event(ev)

            assert h.runner.opened == ["do it"]  # exactly one attempt, no retry
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()
            assert await h.async_redis.exists(h.config.side_effect_key(ev.event_id))
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_rate_limit_retries_then_succeeds(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.turn_scripts = [
                [
                    ErrorEvent(message="rl", classification="rate-limit"),
                    Final(text="f", status=FAIL),
                ],
                [Final(text="recovered", status=DONE)],
            ]
            await h.kernel.process_event(_qevent("go"))

            assert h.runner.opened == ["go", "go"]
            assert h.sink.last_text == "recovered"

    asyncio.run(go())


def test_turn_start_failure_is_retryable_not_a_stall(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # The first /v1/event returns 500 (transient runner error / not ready).
            # This must be turned into a bounded retry, not escape and leave the
            # entry pending for the long reclaim window.
            h.runner.event_fail_times = 1
            h.runner.default_script = [Final(text="recovered", status=DONE)]

            await h.kernel.process_event(_qevent("go"))

            assert h.runner.opened == ["go", "go"]  # failed start, then retried
            assert h.sink.last_text == "recovered"

    asyncio.run(go())


def test_budget_exceeded_escalates_without_retry(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                ErrorEvent(message="over budget", classification="budget-exceeded"),
                Final(text="f", status=FAIL),
            ]
            await h.kernel.process_event(_qevent("go"))

            assert h.runner.opened == ["go"]  # budget-exceeded is not retryable
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()

    asyncio.run(go())


def test_connector_capability_failed_done_posts_diagnosis_not_escalate(
    make_harness,
) -> None:
    # #2519: the runner short-circuits with ErrorEvent + DONE so the diagnosis
    # reaches the message caller without a kernel.py change. Classified-failure
    # would be overwritten by the escalate copy. This pins the DONE fork.
    async def go() -> None:
        async with make_harness() as h:
            diagnosis = (
                "declared connector 'github' failed MCP capability probe: "
                "credential GITHUB_TOKEN expanded empty. Connector tools are unavailable."
            )
            h.runner.default_script = [
                ErrorEvent(
                    message=diagnosis, classification="connector-capability-failed"
                ),
                Final(text=diagnosis, status=DONE),
            ]
            await h.kernel.process_event(_qevent("go"))

            assert h.runner.opened == ["go"]
            assert h.sink.last_text == diagnosis
            assert "Flagging for a human" not in diagnosis
            assert "human" not in (h.sink.last_text or "").lower()

    asyncio.run(go())


def test_retries_are_bounded_then_escalate(make_harness) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=3) as h:
            # rate-limit every attempt -> retried up to max_attempts, then escalate.
            h.runner.default_script = [
                ErrorEvent(message="rl", classification="rate-limit"),
                Final(text="f", status=FAIL),
            ]
            await h.kernel.process_event(_qevent("go"))

            assert len(h.runner.opened) == 3
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()

    asyncio.run(go())


@pytest.mark.parametrize("slack_no_edit_streaming", [False, True])
def test_quota_capacity_is_terminal_without_retry_or_runner_turn(
    make_harness, slack_no_edit_streaming: bool
) -> None:
    async def go() -> None:
        async with make_harness(
            max_attempts=3,
            slack_no_edit_streaming=slack_no_edit_streaming,
            claim_timeout_seconds=0.05,
        ) as h:
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                resource="limits.cpu",
                requested="2",
                used="7",
                hard="8",
            )
            endpoint = "http://127.0.0.1:43199"
            ev = _qevent("go", endpoint=endpoint)

            await h.kernel.process_event(ev)

            expected = (
                "This agent is at capacity right now. It frees up when another "
                "conversation finishes, so please try again shortly."
            )
            expected_updates = [("C1", "p-1", expected)]
            if not slack_no_edit_streaming:
                expected_updates.insert(0, ("C1", "p-1", h.config.booting_text))
            assert h.sink.updates == expected_updates
            assert h.sink.update_endpoints == [endpoint] * len(expected_updates)
            assert len(h.fake_k8s.claim_envs) == 1
            assert h.runner.opened == []
            assert h.kernel._order_locks == {}
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_quota_refusal_keeps_operator_accounting_out_of_the_reply(
    make_harness, caplog
) -> None:
    """The quota's identity and numbers go to the log, never to the person.

    #2434: the reply body interpolated `quota_name`, `resource`, `requested`,
    `used` and `hard` verbatim, and on 2026-09-06 that reached a customer Slack
    channel three times. Those five fields are operator data -- they say nothing
    to whoever asked the question, and they disclose cluster capacity to anyone
    who can talk to the bot.

    Asserted as absence of the VALUES rather than equality with the new
    sentence, so rewording the refusal cannot silently reintroduce them. The
    second half pins the other side of the boundary: the operator loses nothing,
    so there is no pressure to put any of it back in the reply.
    """

    async def go() -> None:
        async with make_harness(max_attempts=3, claim_timeout_seconds=0.05) as h:
            # Multi-digit and distinctive on purpose. The reply assertions below
            # are substring checks, so a single-digit "2" would collide with any
            # unrelated number a future refusal might legitimately carry (a wait
            # time, say) and fail for the wrong reason.
            rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                resource="limits.cpu",
                requested="11",
                used="47",
                hard="53",
            )
            h.fake_k8s.quota_rejection = rejection

            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                await h.kernel.process_event(_qevent("go"))

            # Read off the rejection rather than retyping literals, so the two
            # halves of the boundary cannot drift apart.
            reply = h.sink.updates[-1][2]
            for leaked in (
                rejection.quota_name,
                rejection.resource,
                rejection.requested,
                rejection.used,
                rejection.hard,
                "ResourceQuota",
            ):
                assert leaked not in reply, (
                    f"the capacity refusal must not carry {leaked!r}: it is operator "
                    f"data and this text is read by a customer. Got: {reply!r}"
                )
            assert "quota" not in reply.lower(), (
                f"the capacity refusal must not name the quota mechanism at all. "
                f"Got: {reply!r}"
            )

            logged = "\n".join(record.getMessage() for record in caplog.records)
            for kept in (
                f"quota={rejection.quota_name}",
                f"resource={rejection.resource}",
                f"requested={rejection.requested}",
                f"used={rejection.used}",
                f"hard={rejection.hard}",
            ):
                assert kept in logged, (
                    f"the operator log must still carry {kept!r}: taking it out of "
                    f"the reply must not cost the operator the diagnosis. "
                    f"Got: {logged!r}"
                )

    asyncio.run(go())


def test_approval_resume_capacity_retries_then_escalates(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            max_attempts=3,
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.05,
        ) as h:
            thread = "t-approval-capacity"
            await asyncio.to_thread(h.substrate.claim, thread)
            await asyncio.to_thread(h.substrate.suspend, thread, history_ref="history-1")
            h.fake_k8s.claim_envs.clear()
            h.fake_k8s.quota_rejection = QuotaRejection(
                quota_name="curie-sandbox-quota",
                resource="limits.cpu",
                requested="1",
                used="8",
                hard="8",
            )
            endpoint = "http://127.0.0.1:43199"
            ev = _qevent(
                "approved continuation",
                thread=thread,
                event_id="approval-example-resolved",
                endpoint=endpoint,
            )

            await h.kernel.process_event(ev)

            assert len(h.fake_k8s.claim_envs) == 3
            assert h.runner.opened == []
            assert h.sink.updates == [
                (
                    "C1",
                    "p-1",
                    "The run failed (runner-error) after 3 attempt(s). Flagging for a human.",
                )
            ]
            assert h.sink.update_endpoints == [endpoint]
            assert h.kernel._order_locks == {}
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_claim_timeout_without_quota_retries_then_escalates(make_harness) -> None:
    async def go() -> None:
        async with make_harness(
            max_attempts=3,
            slack_no_edit_streaming=True,
            claim_timeout_seconds=0.02,
        ) as h:
            h.fake_k8s.bind_ready = False
            ev = _qevent("go")

            await h.kernel.process_event(ev)

            assert len(h.fake_k8s.claim_envs) == 3
            assert h.runner.opened == []
            assert h.sink.updates == [
                (
                    "C1",
                    "p-1",
                    "The run failed (runner-error) after 3 attempt(s). Flagging for a human.",
                )
            ]
            assert "sandbox capacity" not in h.sink.updates[0][2].lower()
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_interrupt_hard_stops_the_live_turn(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="thinking")]
            h.runner.tail = [Final(text="stopped", status=IDLE)]

            e1 = _qevent("start", thread="tI")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            signalled = await h.kernel.interrupt_thread(_thread_key("tI"), "user stop")
            assert signalled is True
            assert h.runner.interrupts == 1

            await t1
            assert h.sink.last_text == "stopped"

    asyncio.run(go())


def test_interrupt_agent_signals_other_threads_past_a_wedged_runner(
    make_harness, monkeypatch
) -> None:
    """#742: interrupt_agent fans out over an agent's live threads. A single
    wedged runner -- one that accepts the interrupt call and then never
    answers -- must not cost the agent's other threads up to
    `RunnerClient.interrupt`'s own request budget: the kill switch is "the one
    control that is supposed to work when things are broken." Each thread's
    interrupt is individually bounded and the fan-out runs concurrently, so a
    permanently-wedged thread times out (logged, not raised) while the other
    threads are still signalled well inside the test's own generous ceiling.

    The wedge is injected at the runner-client seam with an event that is never
    set, the same deterministic technique #739's release_thread test uses,
    rather than racing real timing against a live HTTP hang."""

    async def go() -> None:
        async with make_harness() as h:
            agent_id = uuid.uuid4()
            h.runner.default_script = [Final(text="hi", status=DONE)]
            threads = ("tKillA", "tKillB", "tKillC")
            for thread in threads:
                await h.kernel.process_event(_qevent("hi", thread=thread))
            h.kernel._active_by_agent[agent_id] = {_thread_key(t) for t in threads}

            monkeypatch.setattr(kernel_module, "_KILL_INTERRUPT_TIMEOUT_S", 0.2)

            wedged = asyncio.Event()  # never set: the first thread's runner hangs forever
            attempted: list[str] = []

            async def maybe_wedge(base_url: str, reason: str, token: str | None = None) -> None:
                attempted.append(reason)
                if len(attempted) == 1:
                    await wedged.wait()

            monkeypatch.setattr(h.kernel._runner, "interrupt", maybe_wedge)

            try:
                signalled = await asyncio.wait_for(h.kernel.interrupt_agent(agent_id), timeout=2.0)
            finally:
                wedged.set()

            assert len(attempted) == 3  # every thread was attempted, none blocked the rest
            assert signalled == 2  # the wedged thread times out; the other two still land

    asyncio.run(go())


def test_duplicate_event_is_idempotent(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="one", status=DONE)]
            ev = _qevent("hi", event_id="dup-1")
            await h.kernel.process_event(ev)
            await h.kernel.process_event(ev)  # same event id

            assert h.runner.opened == ["hi"]  # processed exactly once

    asyncio.run(go())


def test_ordering_preserved_under_concurrent_sends(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="w")]
            h.runner.tail = [Final(text="done", status=DONE)]

            # Both events for the same thread are dispatched concurrently, with no
            # pre-sequencing: the FIFO in-process lock must make the first-created
            # event open the turn and the second steer into it. Without that lock
            # the order (and whether a second turn is forked) would be a race, so
            # this asserts the ordering guarantee, not just that steering works.
            e1 = _qevent("first", thread="tO", event_id="o1")
            e2 = _qevent("second", thread="tO", event_id="o2")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            t2 = asyncio.create_task(h.kernel.process_event(e2))
            await _wait_until(lambda: h.runner.turn_active and bool(h.runner.steers))

            assert h.runner.opened == ["first"]  # exactly one turn, the first event
            assert h.runner.steers == ["second"]  # the second folded in as a steer

            hold.set()
            await asyncio.gather(t1, t2)

    asyncio.run(go())


def test_prior_side_effect_marker_escalates_without_running(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            ev = _qevent("retry me", event_id="se-1")
            # A prior attempt executed a side effect then the worker crashed: the
            # marker is set but the event never reached done. It must escalate,
            # never re-run the non-idempotent action.
            await h.async_redis.set(h.config.side_effect_key(ev.event_id), "1")

            await h.kernel.process_event(ev)

            assert h.runner.opened == []  # no turn was ever opened
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_suspended_thread_is_resumed_not_forked(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tR"))

            # Suspend the thread (records a rehydrate ref on the route).
            await asyncio.to_thread(h.substrate.suspend, _thread_key("tR"), history_ref="hist-1")

            # A new event on a suspended thread must resume (carry the history)
            # rather than silently fork a fresh, history-less session.
            h.runner.default_script = [Final(text="resumed", status=DONE)]
            await h.kernel.process_event(_qevent("second", thread="tR"))

            assert h.runner.opened == ["first", "second"]
            assert h.sink.last_text == "resumed"

    asyncio.run(go())


async def _route_key(async_redis, thread: str) -> str:
    keys = [k async for k in async_redis.scan_iter(match=f"*:route:{thread}")]
    assert len(keys) == 1, f"expected one route key for {thread}, found {keys}"
    return keys[0]


def test_live_route_reuse_refreshes_ttl(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            # First event creates a live route with the substrate's route TTL.
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tTTL"))

            route_key = await _route_key(h.async_redis, _thread_key("tTTL"))
            # Simulate time passing by dropping the TTL low.
            await h.async_redis.expire(route_key, 5)
            assert await h.async_redis.ttl(route_key) <= 5

            # A second event reuses the live route; routing through claim() must
            # refresh the TTL (a regression to lookup() would leave it at ~5 and
            # let the reaper delete a busy thread's sandbox).
            h.runner.default_script = [Final(text="two", status=DONE)]
            await h.kernel.process_event(_qevent("second", thread="tTTL"))
            assert await h.async_redis.ttl(route_key) > 5

    asyncio.run(go())


def test_steered_followup_placeholder_is_retired(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="w")]
            h.runner.tail = [Final(text="done", status=DONE)]

            e1 = _qevent("first", thread="tPH", placeholder="ph-1")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            # The follow-up carries its own placeholder; once steered, that
            # placeholder must be retired (not left stuck on "working").
            e2 = _qevent("second", thread="tPH", placeholder="ph-2")
            await h.kernel.process_event(e2)

            folded = [u for u in h.sink.updates if u[1] == "ph-2"]
            assert folded, "the steered follow-up's placeholder was never updated"
            assert "folded" in folded[-1][2].lower()

            hold.set()
            await t1

    asyncio.run(go())


def test_order_lock_map_evicts_after_processing(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="ok", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tEV"))
            # Ref-counted eviction: no per-thread lock entry lingers once the last
            # holder releases (a regression would leak one entry per thread seen).
            assert h.kernel._order_locks == {}

    asyncio.run(go())


# --- Per-sandbox runner token delivery, end-to-end (issue #63) ----------------


class _FakeResolved:
    """The minimal resolved deployment the kernel reads (agent_id plus the
    binding row's egress pair; shimmer off, so packs are never sampled)."""

    def __init__(self, agent_id: uuid.UUID) -> None:
        self.agent_id = agent_id
        self.agent_name = "test-agent"
        # Unset on this binding, so the turn keeps the route the server minted
        # onto its reply handle (ADR-0096 EB-B2).
        self.endpoint: str | None = None
        self.adapter: str | None = None


class _TokenBinding:
    """A binding whose boot_env injects a known runner token into the claim env,
    so the test can assert the exact value the worker delivers as the Bearer
    header. The claim-time minting itself is covered by the binding unit tests;
    this proves the claim->handle->kernel->runner delivery path."""

    def __init__(self, token: str, agent_id: uuid.UUID) -> None:
        self._token = token
        self._agent_id = agent_id

    async def resolve(self, _kind: str, _channel: str) -> _FakeResolved:
        return _FakeResolved(self._agent_id)

    def boot_env(
        self,
        _resolved: object,
        _thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
    ) -> dict[str, str]:
        return {"CURIE_RUNNER_TOKEN": self._token}

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()


def test_reply_handle_adapter_survives_a_binding_without_an_adapter(
    make_harness,
) -> None:
    """The existing kernel copy keeps the built-in route after binding.

    ``curie cluster message`` still binds as Slack. Its deployment row normally
    has no adapter, so the queue handle's reserved adapter must reach every sink
    event instead of being erased during resolution. This is verify-only for
    #2096: production kernel behavior already has the required fallback.
    """

    async def go() -> None:
        binding = _TokenBinding("tok-route", uuid.uuid4())
        async with make_harness(binding=binding) as h:
            h.runner.default_script = [Final(text="done", status=DONE)]
            await h.kernel.process_event(
                _qevent(
                    "hi",
                    thread="tClusterMessageAdapter",
                    placeholder="123e4567-e89b-42d3-a456-426614174000",
                    adapter="curie-cluster-message",
                )
            )

            routes = h.sink.routes_for("reply.update")
            assert routes, "the completed turn emitted no reply update"
            assert set(routes) == {
                TargetRoute(endpoint=None, adapter="curie-cluster-message")
            }

    asyncio.run(go())


def test_kernel_delivers_claim_token_as_bearer_header(make_harness) -> None:
    async def go() -> None:
        binding = _TokenBinding("tok-24", uuid.uuid4())
        async with make_harness(binding=binding) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="w")]
            h.runner.tail = [Final(text="done", status=DONE)]

            e1 = _qevent("first", thread="tTok")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            # Event path: the opening /v1/event carried the claim-minted token.
            assert h.runner.event_headers
            assert h.runner.event_headers[-1].get("Authorization") == "Bearer tok-24"

            # Steer path: a follow-up folded into the live turn carries it too.
            await h.kernel.process_event(_qevent("second", thread="tTok"))
            assert h.runner.steer_headers
            assert h.runner.steer_headers[-1].get("Authorization") == "Bearer tok-24"

            # Interrupt path: the explicit hard stop carries it as well.
            await h.kernel.interrupt_thread(_thread_key("tTok"), "user stop")
            assert h.runner.interrupt_headers
            assert h.runner.interrupt_headers[-1].get("Authorization") == "Bearer tok-24"

            hold.set()
            await t1

    asyncio.run(go())


# --- #31: no-edit streaming mode ----------------------------------------------

_MULTI_DELTA = [
    TextDelta(text="a"),
    ToolNote(text="checking", tool="ExampleTool"),
    TextDelta(text="b"),
    TextDelta(text="c"),
    Final(text="abc final", status=DONE),
]


def test_no_edit_streaming_edits_placeholder_once(make_harness) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            # Text and tool frames arrive, but no edit mode updates only the final.
            h.runner.default_script = list(_MULTI_DELTA)
            await h.kernel.process_event(_qevent("go"))

            assert len(h.sink.updates) == 1
            assert h.sink.last_text == "abc final"

    asyncio.run(go())


def test_no_edit_streaming_suppresses_tool_context_and_finalizes_once(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(slack_no_edit_streaming=True) as h:
            h.runner.default_script = [
                TextDelta(text="answer in progress"),
                ToolNote(text="checking", tool="ExampleTool"),
                Final(text="final answer", status=DONE),
            ]

            await h.kernel.process_event(_qevent("go"))

            assert h.sink.updates == [("C1", "p-1", "final answer")]

    asyncio.run(go())


def test_default_streaming_edits_more_than_once(make_harness) -> None:
    async def go() -> None:
        # Deletion-test guard: with no-edit OFF (default; conftest sets
        # slack_edit_min_interval_s=0.0) the SAME multi-delta script produces
        # more than one edit, proving the flag actually changes behavior.
        async with make_harness() as h:
            h.runner.default_script = list(_MULTI_DELTA)
            await h.kernel.process_event(_qevent("go"))

            assert len(h.sink.updates) > 1
            assert h.sink.last_text == "abc final"

    asyncio.run(go())


def test_booting_state_edits_placeholder_before_answer(make_harness) -> None:
    # A fresh-claim turn edits the placeholder to the booting caption at the very
    # start of the attempt, before the sandbox-claim wait, so the "booting a
    # runner" state is visible ahead of the streamed answer on the same message.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                TextDelta(text="Hello "),
                TextDelta(text="world"),
                Final(text="Hello world", status=DONE),
            ]
            ev = _qevent("hi", thread="tBOOT", placeholder="ph-boot")
            await h.kernel.process_event(ev)

            booting = h.config.booting_text
            on_ph = [
                (i, u)
                for i, u in enumerate(h.sink.updates)
                if u[0] == ev.reply_handle.channel and u[1] == ev.reply_handle.placeholder
            ]
            booting_idxs = [i for i, u in on_ph if u[2] == booting]
            answer_idxs = [i for i, u in on_ph if u[2] != booting]
            assert booting_idxs, "the booting caption was never edited onto the placeholder"
            assert answer_idxs, "no streamed-answer update landed on the placeholder"
            assert min(booting_idxs) < min(answer_idxs), (
                "the booting caption must precede the first streamed-answer update"
            )

    asyncio.run(go())


def test_reply_endpoint_is_threaded_to_the_sink(make_harness) -> None:
    # Issue #19: a turn carrying a per-turn reply endpoint must route every sink
    # edit for that turn through that endpoint (not the worker default), so a
    # no-Slack CLI stub and a real workspace can coexist on one worker.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [
                TextDelta(text="working "),
                Final(text="done", status=DONE),
            ]
            await h.kernel.process_event(
                _qevent("hi", thread="tEP", endpoint="http://stub:8155/api/")
            )

            assert h.sink.last_text == "done"
            # Every recorded update for this turn carried the per-turn endpoint.
            assert h.sink.update_endpoints, "no sink update recorded"
            assert set(h.sink.update_endpoints) == {"http://stub:8155/api/"}

    asyncio.run(go())


def test_reply_endpoint_defaults_to_none_for_the_worker_default(make_harness) -> None:
    # A turn with no per-turn endpoint threads None, so the sink uses its worker
    # default (the pre-#19 behavior is preserved for real-Slack ingress).
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="ok", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tEPNONE"))
            assert set(h.sink.update_endpoints) == {None}

    asyncio.run(go())


def test_booting_update_failure_never_fails_the_turn(make_harness) -> None:
    # The booting edit is best-effort: if the Slack update for the booting caption
    # raises, the turn still runs to its normal terminal answer. Inject a failure
    # on the first booting-caption update and prove both that it fired and that the
    # turn completed anyway.
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="all good", status=DONE)]

            booting = h.config.booting_text
            original_emit = h.sink.emit
            fired = {"n": 0}

            async def flaky_emit(
                event: ReplyEvent,
                *,
                route: TargetRoute,
                best_effort_unreachable: bool = False,
            ) -> ReplyAck:
                text = getattr(event, "text", None)
                if text == booting and fired["n"] == 0:
                    fired["n"] += 1
                    raise RuntimeError("injected Slack failure on booting update")
                return await original_emit(
                    event,
                    route=route,
                    best_effort_unreachable=best_effort_unreachable,
                )

            h.sink.emit = flaky_emit  # type: ignore[method-assign]

            ev = _qevent("hi", thread="tBOOTFAIL", placeholder="ph-boot-fail")
            await h.kernel.process_event(ev)

            assert fired["n"] > 0, "the booting update was never attempted"
            assert h.sink.last_text == "all good"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_release_thread_force_releases_a_live_route(make_harness) -> None:
    """#713: an operator can force-release a thread's sandbox even though it
    has a live (not suspended, not dead) route -- the whole point is to evict
    a sandbox that is up and answering but running stale env, not just one
    that already died on its own (that path -- claim()'s stale-sandbox
    eviction -- already existed)."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tRelease"))
            assert h.substrate.lookup(_thread_key("tRelease")) is not None  # the route is live

            released = await h.kernel.release_thread(_thread_key("tRelease"))
            assert released is True
            assert h.substrate.lookup(_thread_key("tRelease")) is None  # gone: next claim is fresh

    asyncio.run(go())


def test_workspace_reaper_holds_the_route_lock_during_exact_ledger_recheck(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(
            lock_ttl_ms=90,
            lock_acquire_timeout_s=1.0,
            lock_poll_interval_s=0.01,
        ) as h:
            thread = "tWorkspaceReap"
            entered = threading.Event()
            gate = threading.Event()

            class GatedWorkspace:
                def enumerate_expired(self) -> list[str]:
                    return [thread]

                def begin_expired_reap(self, thread_key: str) -> object:
                    assert thread_key == thread
                    entered.set()
                    gate.wait(timeout=5.0)
                    return object()

                def finish_expired_reap(self, candidate: object) -> bool:
                    return True

            h.kernel._workspace = GatedWorkspace()  # type: ignore[assignment]
            reaping = asyncio.create_task(h.kernel.reap_orphans())
            await _wait_until(entered.is_set)

            contender = asyncio.create_task(h.kernel._lock.acquire(h.config.lock_key(thread)))
            try:
                # Object cleanup outlives the original lease. Renewal must keep
                # the competing claimant fenced for the full critical section.
                await asyncio.sleep(0.25)
                assert not contender.done(), "reaper mutated the ledger outside the route lock"
            finally:
                gate.set()
            await reaping
            token = await contender
            await h.kernel._lock.release(h.config.lock_key(thread), token)

    asyncio.run(go())


def test_release_thread_interrupts_a_live_turn_first(make_harness) -> None:
    """Releasing a thread mid-turn interrupts it first rather than yanking the
    claim out from under a running turn silently."""

    async def go() -> None:
        async with make_harness() as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="thinking")]
            h.runner.tail = [Final(text="stopped", status=IDLE)]

            e1 = _qevent("start", thread="tReleaseMidTurn")
            t1 = asyncio.create_task(h.kernel.process_event(e1))
            await _wait_until(lambda: h.runner.turn_active)

            released = await h.kernel.release_thread(_thread_key("tReleaseMidTurn"))
            assert released is True
            assert h.runner.interrupts == 1  # interrupted, not silently abandoned

            hold.set()
            await t1

    asyncio.run(go())


def test_release_thread_releases_when_the_runner_never_answers_the_interrupt(
    make_harness, monkeypatch
) -> None:
    """#739: a WEDGED runner accepts the TCP connect and then never answers
    ``/v1/interrupt``. The interrupt is a courtesy, not a precondition, so the
    release must not be hostage to it: the sandbox is still released and the
    route-existed answer still comes back, bounded to a few seconds rather than
    the runner client's own 600s request timeout. Without the bound the operator
    reset is lost entirely (the substrate release line is never reached) and the
    maintenance tick that drove it stalls for the whole window.

    The hang is injected at the runner-client seam (the external HTTP call) with
    an event that is never set, so the wedge is deterministic rather than timing
    dependent. The generous 10s ceiling below only has to prove the call is
    bounded to seconds, not to pin the exact constant."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tWedged"))
            assert h.substrate.lookup(_thread_key("tWedged")) is not None  # the route is live

            monkeypatch.setattr(kernel_module, "_RESET_INTERRUPT_TIMEOUT_S", 0.2)

            wedged = asyncio.Event()  # never set: the runner answers nothing, ever

            async def never_answers(base_url: str, reason: str, token: str | None = None) -> None:
                await wedged.wait()

            monkeypatch.setattr(h.kernel._runner, "interrupt", never_answers)

            released = await asyncio.wait_for(
                h.kernel.release_thread(_thread_key("tWedged")), timeout=2.0
            )

            assert released is True
            # released despite the wedged runner
            assert h.substrate.lookup(_thread_key("tWedged")) is None

    asyncio.run(go())


def test_release_thread_releases_when_the_interrupt_raises(make_harness, monkeypatch) -> None:
    """#739, the other half of the wedged-runner shape: the runner answers, but
    with a transport error or a non-200. The release is an operator's explicit
    "give me a fresh sandbox", so a failed courtesy interrupt is logged and
    swallowed rather than aborting the release and stranding the stale sandbox."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            await h.kernel.process_event(_qevent("hi", thread="tInterruptBoom"))
            assert h.substrate.lookup(_thread_key("tInterruptBoom")) is not None

            async def boom(base_url: str, reason: str, token: str | None = None) -> None:
                raise RunnerError("/v1/interrupt -> 500: runner is wedged")

            monkeypatch.setattr(h.kernel._runner, "interrupt", boom)

            released = await h.kernel.release_thread(_thread_key("tInterruptBoom"))

            assert released is True
            assert h.substrate.lookup(_thread_key("tInterruptBoom")) is None

    asyncio.run(go())


def test_release_thread_with_no_route_is_a_noop(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            released = await h.kernel.release_thread("never-seen-thread")
            assert released is False

    asyncio.run(go())


def test_release_serializes_against_a_concurrent_turn_start(make_harness) -> None:
    """#734: the release runs under the same per-thread route lock the turn path
    holds around `_route_and_start`, so a reset and a message arriving for the
    same thread cannot interleave. Without the lock the message could
    `claim()`-adopt the sandbox the reset is tearing down and open a turn on it,
    which the release then yanks mid-run.

    The interleaving is forced deterministically rather than raced: the release
    is gated open (via a threading.Event, because the substrate release runs on
    `asyncio.to_thread`) so it sits IN the critical section, holding the route
    lock, while a new turn for the same thread tries to start. That turn must
    block on the lock -- proven by its `/v1/event` never firing while the
    release is parked -- and, once the release drops the route and frees the
    lock, must cold-create a FRESH sandbox (a new claim) instead of the released
    one, and complete cleanly rather than failing on a torn-down sandbox."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="ok", status=DONE)]

            # Establish a live route with a concrete, idle sandbox.
            await h.kernel.process_event(_qevent("first", thread="tRace"))
            old = h.substrate.lookup(_thread_key("tRace"))
            assert old is not None
            old_claim = old.claim_name

            # Gate the substrate release so it parks inside the critical section
            # (route lock held) until the test lets it proceed.
            real_release = h.substrate.release
            release_entered = threading.Event()
            release_gate = threading.Event()

            def gated_release(thread_key: str) -> bool:
                release_entered.set()
                release_gate.wait(timeout=5.0)
                return real_release(thread_key)

            h.substrate.release = gated_release  # type: ignore[method-assign]

            reset = asyncio.create_task(h.kernel.release_thread(_thread_key("tRace")))
            await _wait_until(release_entered.is_set)  # release now holds the lock

            # A new message for the same thread races the reset. It must block on
            # the route lock the release holds, not adopt the doomed sandbox.
            turn = asyncio.create_task(h.kernel.process_event(_qevent("second", thread="tRace")))
            await asyncio.sleep(0.2)
            assert h.runner.opened == ["first"], "turn started while the reset held the lock"

            # Let the release finish: it drops the route and frees the lock, so
            # the waiting turn now cold-creates a fresh sandbox.
            release_gate.set()
            assert await reset is True
            await turn

            assert h.runner.opened == ["first", "second"]  # the turn did run
            fresh = h.substrate.lookup(_thread_key("tRace"))
            assert fresh is not None
            assert fresh.claim_name != old_claim  # a fresh sandbox, not the released one
            assert old_claim not in h.fake_k8s.claims  # the released claim is gone

    asyncio.run(go())


def test_claim_latency_is_logged(make_harness, caplog) -> None:
    """#718: the claim wait (cold sandbox boot vs. an adopted warm one) is
    logged separately from the model turn's own duration -- the runner's own
    per-turn logging starts only once its process is already up, so it has no
    visibility into how long the worker waited to get it there. Both a fresh
    claim and a steer onto a live turn go through the same timed call, so both
    are covered by one assertion on the log line's presence and shape."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="hi", status=DONE)]
            with caplog.at_level(logging.INFO, logger="curie_worker.kernel"):
                await h.kernel.process_event(_qevent("hi", thread="tLatency"))

            matches = [
                r.getMessage()
                for r in caplog.records
                if f"claim latency for {_thread_key('tLatency')}" in r.getMessage()
            ]
            assert matches, caplog.text
            # "claim latency for tLatency: <N> ms" -- non-negative integer duration.
            ms = int(matches[0].rsplit(":", 1)[1].strip().split()[0])
            assert ms >= 0

    asyncio.run(go())


def test_lock_acquire_timeout_is_a_retryable_turn_start_failure(make_harness) -> None:
    """#849, the sibling of the runner-500 turn-start failure above: the turn
    never starts because the per-thread route lock cannot be taken in time.

    The contention is real -- another holder squats the exact Valkey lock key,
    so `acquire` polls to its deadline -- and the failure must come back as the
    same retryable outcome the other transient turn-start failures do, not
    escape `_attempt` to the consumer. The order lock must be released too, or
    the next same-thread event would never route."""

    async def go() -> None:
        async with make_harness(lock_acquire_timeout_s=0.2) as h:
            thread = "tLockTimeout"
            # A foreign holder of the route lock, outliving the acquire deadline.
            await h.async_redis.set(
                h.config.lock_key(_thread_key(thread)), "another-worker", nx=True, px=60000
            )

            released: list[bool] = []

            def release_order() -> None:
                released.append(True)

            qe = _qevent("go", thread=thread)
            outcome = await h.kernel._attempt(qe, TargetRoute(), release_order)

            assert outcome.terminal_ok is False
            assert outcome.classification == "runner-error"  # retryable
            assert h.runner.opened == []  # the turn was never started
            assert released, "the order lock was not released on the failed start"

    asyncio.run(go())


def test_lock_acquire_timeout_retries_in_process(make_harness) -> None:
    """#849: a route-lock acquire timeout is retried inside `process_event`,
    within `max_attempts`, instead of escaping to the consumer and leaving the
    stream entry pending for the whole reclaim window.

    Attempt 1 finds the lock squatted by a foreign holder and times out without
    ever reaching the runner; the squatter goes away once attempt 2 has begun,
    so attempt 2 opens the turn and completes. `opened == ["go"]` is what
    separates this from the runner-500 shape: the retry was caused by the lock,
    not by a failed turn start at the runner."""

    async def go() -> None:
        async with make_harness(lock_acquire_timeout_s=0.2, max_attempts=3) as h:
            thread = "tLockRetry"
            lock_key = h.config.lock_key(thread)
            await h.async_redis.set(lock_key, "another-worker", nx=True, px=60000)
            h.runner.default_script = [Final(text="recovered", status=DONE)]

            async def unsquat() -> None:
                # Each attempt opens with a "booting" edit before it touches the
                # lock, so a second one means attempt 1 already gave up.
                await _wait_until(lambda: len(h.sink.updates) >= 2)
                await h.async_redis.delete(lock_key)

            freeing = asyncio.create_task(unsquat())
            ev = _qevent("go", thread=thread)
            await h.kernel.process_event(ev)
            await freeing

            assert h.runner.opened == ["go"]  # attempt 1 never reached the runner
            assert h.sink.last_text == "recovered"
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


# --- ADR-0131 (#1971): steering across a long turn, and the reclaim preflight --
#
# Time is compressed by CONFIGURING short lease clocks, never by patching a
# clock. Every ratio the WorkerConfig validators enforce is preserved: TTL (1.0)
# >= 3 * heartbeat (0.3), the harness's reclaim interval (0.05) < TTL, and the
# runner ceiling (30) <= the budget (60, its configurable floor). The Valkey
# server TIME read behind the deadline is deliberately never stubbed.

_LEASE_TTL_S = 1.0

_LEASE_KNOBS: dict[str, object] = {
    "delivery_budget_s": 60.0,
    "delivery_lease_ttl_s": _LEASE_TTL_S,
    "delivery_lease_heartbeat_s": 0.3,
    "runner_total_timeout_s": 30.0,
}


async def _leased_entry(h: Any, store: Any, *, event_id: str, generation: int) -> Any:
    """A lease on a real PEL row, advanced to ``generation`` by re-acquisition.

    Acquire/release/acquire is what a change of authority looks like to the
    state hash, so this produces a genuine ``generation > 1`` lease -- the exact
    and only signal the reclaim preflight keys on (a distributed-state fact, not
    a sniff of the message text: kernel rule 3 stands).
    """
    from curie_dispatcher.queue import to_stream_fields

    await h.async_redis.xadd(
        h.config.stream, to_stream_fields(_qevent("reclaimed", event_id=event_id))
    )
    rows = await h.async_redis.xreadgroup(
        h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
    )
    entry_id = rows[0][1][0][0]
    lease = None
    for _ in range(generation):
        if lease is not None:
            await store.release(
                h.config.stream, h.config.consumer_group, entry_id, owner=lease.owner
            )
        lease = await store.acquire(
            h.config.stream,
            h.config.consumer_group,
            entry_id,
            consumer=h.config.consumer_name,
        )
    assert lease is not None and lease.generation == generation
    return lease


def test_a_long_turn_keeps_accepting_steers_and_a_finished_one_opens_a_new_turn(
    make_harness,
) -> None:
    """R7: continued steering, before and after the old 600s boundary.

    The boundary is compressed by CONFIGURING short lease clocks: the second
    steer lands after ~3 lease TTLs and ~10 heartbeat periods, which is the same
    place on the lease timeline that a real 600s+ turn occupies at production
    knobs. The old flat 600s HTTP deadline is what used to cut a turn like this
    off mid-flight; the point of the assertion is that a turn living well past
    its lease TTL still accepts steers, has burned no deliveries, and holds a
    live lease throughout.

    Red on revert of C8's remaining-budget plumbing if it breaks steering (a
    zero or negative per-request budget derived for ``steer``), and on any
    regression of kernel rules 1 and 2: a follow-up on a live thread is a STEER,
    a follow-up after the turn ends opens a NEW turn via the 409 finish-race
    fallback, never a retried steer.

    ``h.runner.opened`` is the negative control on the steering half: a steer
    that silently became a second turn shows up there.
    """
    from curie_dispatcher.queue import to_stream_fields
    from curie_worker.consumer import Consumer

    # Imported inside the test on purpose: ``delivery_lease`` does not exist
    # until this ticket lands, and a module-level import would fail COLLECTION
    # for this whole file, turning every unrelated test in it red.
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS, reclaim_min_idle_ms=900000) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=DONE)]

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(_qevent("first", thread="steer-1", event_id="steer-1")),
            )
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
            )
            entry_id, fields = rows[0][1][0]
            before = (
                await h.async_redis.xpending_range(
                    h.config.stream,
                    h.config.consumer_group,
                    min=entry_id,
                    max=entry_id,
                    count=1,
                )
            )[0]["times_delivered"]
            await consumer._dispatch(entry_id, dict(fields))
            await _wait_until(lambda: h.runner.turn_active)

            # Early in the turn: a same-thread follow-up steers.
            await h.kernel.process_event(_qevent("second", thread="steer-1"))
            assert h.runner.steers == ["second"]
            assert h.runner.opened == ["first"]

            # ...and well past the compressed boundary it still steers, on a
            # lease that is still live and has burned no deliveries.
            await asyncio.sleep(3 * _LEASE_TTL_S)
            assert await store.is_live(h.config.stream, h.config.consumer_group, entry_id)
            await h.kernel.process_event(_qevent("third", thread="steer-1"))
            assert h.runner.steers == ["second", "third"]
            assert h.runner.opened == ["first"], "a steer opened a second turn"
            after = (
                await h.async_redis.xpending_range(
                    h.config.stream,
                    h.config.consumer_group,
                    min=entry_id,
                    max=entry_id,
                    count=1,
                )
            )[0]["times_delivered"]
            assert int(after) == int(before)

            hold.set()
            await asyncio.gather(*list(consumer._inflight), return_exceptions=True)

            # After completion the same thread's follow-up opens a NEW turn: the
            # steer hits 409 (no active turn) and the kernel falls back rather
            # than retrying the steer (kernel rule 2).
            h.runner.hold = None
            h.runner.default_script = [Final(text="fresh", status=DONE)]
            await h.kernel.process_event(_qevent("fourth", thread="steer-1"))
            assert h.runner.steers == ["second", "third"], "the 409 steer was retried"
            assert h.runner.opened == ["first", "fourth"]
            assert h.sink.last_text == "fresh"

    asyncio.run(go())


def test_a_reclaimed_delivery_with_a_side_effect_marker_never_runs_the_runner_again(
    make_harness,
) -> None:
    """Reclaim preflight rule 1 (ADR-0131, plan C10): a side-effect marker
    forbids replay and settles to human escalation, with ZERO second runner
    execution.

    The check already exists (kernel rule 4) and needs no new code; what this
    test pins is the ORDERING -- it must run BEFORE the transferred-delivery
    preflight, so a preflight that interrupted, waited and then rehydrated could
    never re-execute a half-done non-idempotent action.

    Red if the preflight is inserted ahead of the side-effect check, or if the
    marker check is made conditional on the lease being absent.

    The negative control is the second event: a marker-free delivery at the same
    generation DOES run, so "zero executions" above is the marker and not a
    preflight that refuses everything.
    """
    # Imported inside the test on purpose: ``delivery_lease`` does not exist
    # until this ticket lands, and a module-level import would fail COLLECTION
    # for this whole file, turning every unrelated test in it red.
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            await h.async_redis.xgroup_create(
                h.config.stream, h.config.consumer_group, id="0", mkstream=True
            )
            h.runner.default_script = [Final(text="ok", status=DONE)]

            lease = await _leased_entry(h, store, event_id="pre-se", generation=2)
            ev = _qevent("retry me", thread="pre-se", event_id="pre-se")
            await h.async_redis.set(h.config.side_effect_key(ev.event_id), "1")

            await h.kernel.process_event(ev, lease=lease)

            assert h.runner.opened == [], "a reclaimed delivery re-ran a side-effecting turn"
            assert h.runner.interrupts == 0
            assert h.sink.last_text is not None and "human" in h.sink.last_text.lower()
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

            # NEGATIVE CONTROL: same generation, no marker -> the turn runs.
            clean_lease = await _leased_entry(h, store, event_id="pre-clean", generation=2)
            clean = _qevent("run me", thread="pre-clean", event_id="pre-clean")
            await h.kernel.process_event(clean, lease=clean_lease)
            assert h.runner.opened == ["run me"]

    asyncio.run(go())


def test_a_reclaimed_delivery_interrupts_a_still_active_retained_runner(
    make_harness,
) -> None:
    """Reclaim preflight rule 2: a runner that still reports an active turn is
    interrupted and must become idle (or disappear) before the retry.

    A replacement must not run beside a possibly-live turn on a sandbox the
    previous owner was working. The interrupt goes through the EXISTING bounded
    control path (``Kernel.interrupt_thread``); no second mechanism is added.
    Note the deliberate divergence from the ordinary route: ``_route_and_start``
    would STEER into a retained live turn, which is wrong for a redelivery of the
    same event -- it is a retry, not a follow-up.

    Red on removing the preflight: the delivery would be steered into (or opened
    beside) the previous owner's turn with no interrupt at all.

    The negative control is the second half: an IDLE retained runner at the same
    generation is not interrupted, so the interrupt above is the liveness read
    and not an unconditional one on every reclaim.
    """
    # Imported inside the test on purpose: ``delivery_lease`` does not exist
    # until this ticket lands, and a module-level import would fail COLLECTION
    # for this whole file, turning every unrelated test in it red.
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            await h.async_redis.xgroup_create(
                h.config.stream, h.config.consumer_group, id="0", mkstream=True
            )

            # A first, ordinary turn so the thread has a retained sandbox.
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="pre-live"))
            assert h.runner.opened == ["first"]

            # The retained runner reports a live turn (the previous owner's).
            h.runner.turn_active = True

            async def idle_on_interrupt() -> None:
                await _wait_until(lambda: h.runner.interrupts >= 1, timeout=10.0)
                h.runner.turn_active = False

            watcher = asyncio.create_task(idle_on_interrupt())
            lease = await _leased_entry(h, store, event_id="pre-live-2", generation=2)
            h.runner.default_script = [Final(text="two", status=DONE)]
            await h.kernel.process_event(
                _qevent("second", thread="pre-live", event_id="pre-live-2"), lease=lease
            )
            await watcher

            assert h.runner.interrupts >= 1, "the preflight never interrupted the live turn"
            assert h.runner.opened == ["first", "second"], (
                "the reclaimed delivery did not retry once the runner went idle"
            )

            # NEGATIVE CONTROL: an idle retained runner is not interrupted.
            interrupts_before = h.runner.interrupts
            idle_lease = await _leased_entry(h, store, event_id="pre-idle", generation=2)
            h.runner.default_script = [Final(text="three", status=DONE)]
            await h.kernel.process_event(
                _qevent("third", thread="pre-live", event_id="pre-idle"), lease=idle_lease
            )
            assert h.runner.interrupts == interrupts_before, (
                "an idle runner was interrupted: the preflight is unconditional"
            )
            assert h.runner.opened == ["first", "second", "third"]

    asyncio.run(go())


def test_an_unreadable_runner_fails_closed_and_leaves_a_reclaimed_delivery_pending(
    make_harness,
) -> None:
    """Reclaim preflight rule 3: an unreadable runner FAILS CLOSED.

    ``_turn_active`` already reports busy on an unreadable answer, so the bounded
    poll can never clear and the preflight must raise -- leaving the stream entry
    PENDING rather than running a replacement beside a turn whose liveness nobody
    can read. Asserted at the consumer, because "left pending" is the observable
    contract and does not depend on which exception type the preflight raises.

    Red on making the preflight fail OPEN (proceeding when liveness is
    unreadable), which is the failure mode a "just retry it" simplification
    reaches for.

    The negative control is the second delivery, with the status endpoint
    healthy again: the same reclaimed entry then runs and acks.
    """
    from curie_dispatcher.queue import to_stream_fields
    from curie_worker.consumer import Consumer

    # Imported inside the test on purpose: ``delivery_lease`` does not exist
    # until this ticket lands, and a module-level import would fail COLLECTION
    # for this whole file, turning every unrelated test in it red.
    from curie_worker.delivery_lease import DeliveryLeaseStore

    async def go() -> None:
        async with make_harness(**_LEASE_KNOBS, reclaim_min_idle_ms=900000) as h:
            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis, kernel=h.kernel, config=h.config, leases=store
            )
            await consumer.ensure_group()

            # A first turn so the thread retains a sandbox to be read.
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="pre-blind"))

            await h.async_redis.xadd(
                h.config.stream,
                to_stream_fields(
                    _qevent("second", thread="pre-blind", event_id="pre-blind-2")
                ),
            )
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
            )
            entry_id, fields = rows[0][1][0]
            # Take and drop a lease so the consumer's own acquisition is a
            # TRANSFER (generation 2) and the preflight applies.
            first = await store.acquire(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
                consumer=h.config.consumer_name,
            )
            await store.release(
                h.config.stream, h.config.consumer_group, entry_id, owner=first.owner
            )

            h.runner.status_fails = True
            await consumer._dispatch(entry_id, dict(fields))
            await asyncio.gather(*list(consumer._inflight), return_exceptions=True)

            assert h.runner.opened == ["first"], (
                "a replacement ran beside a runner whose liveness could not be read"
            )
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 1, "an unreadable runner did not leave the entry pending"

            # NEGATIVE CONTROL: the status endpoint recovers and the same
            # reclaimed entry runs to an ACK.
            h.runner.status_fails = False
            h.runner.turn_active = False
            h.runner.default_script = [Final(text="two", status=DONE)]
            await consumer._dispatch(entry_id, dict(fields))
            await asyncio.gather(*list(consumer._inflight), return_exceptions=True)

            assert h.runner.opened == ["first", "second"]
            summary = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert summary["pending"] == 0

    asyncio.run(go())


# --- The streaming budget expiring is its OWN failure, not a generic one (#2011)
# When aiohttp's total/sock_read budget expires mid-stream the kernel used to
# see a BARE ``TimeoutError`` whose ``str()`` is the empty string: the operator
# log read "turn stream dropped for <id>: " with nothing after the colon, and
# the outcome collapsed into the same "runner-error" a killed sandbox or a
# broken socket produces. A timeout is a distinct, actionable condition (the
# model ran past the budget) and must classify and log as one.


def test_stream_timeout_classifies_as_runner_timeout_with_a_named_reason(
    make_harness, caplog
) -> None:
    """#2011: a mid-stream timeout is classified ``runner-timeout`` and logged
    with a NON-EMPTY reason.

    The runner streams a side-effect frame and then hangs without a ``Final``,
    so the client's short total budget expires while the kernel is iterating.
    Today the outcome comes back as the generic ``runner-error`` and the warning
    ends at a bare colon, which is exactly the pair this pins."""

    async def go() -> None:
        async with make_harness(runner_total_timeout_s=0.5) as h:
            hold = asyncio.Event()  # never set: the response hangs open
            h.runner.hold = hold
            h.runner.default_script = [SideEffectFlag(tool="deploy")]
            released: list[bool] = []

            def release_order() -> None:
                released.append(True)

            qe = _qevent("go", thread="tStreamTimeout")
            try:
                with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                    outcome = await h.kernel._attempt(qe, TargetRoute(), release_order)
            finally:
                hold.set()  # let the fake runner's handler unwind

            assert outcome.terminal_ok is False
            assert outcome.classification == "runner-timeout"
            assert outcome.saw_side_effect is True
            assert released, "the order lock was not released on the timed-out turn"

            dropped = [
                r.getMessage()
                for r in caplog.records
                if "turn stream dropped" in r.getMessage()
            ]
            assert dropped, caplog.text
            message = dropped[-1]
            # "turn stream dropped for <event_id>: <reason>" -- the reason is what
            # #2011 lost. A bare TimeoutError stringifies to "", so today this is
            # the empty tail the operator sees.
            reason = message.rsplit(":", 1)[1].strip()
            assert reason, f"the drop reason is empty: {message!r}"
            assert "Timeout" in message, message

    asyncio.run(go())


def test_stream_timeout_after_a_side_effect_escalates_without_retry(make_harness) -> None:
    """#2011 x rule 4: a timeout that arrives after a side-effect frame is
    escalated to a human, never retried, and the escalation names the new
    ``runner-timeout`` classification rather than the generic runner-error.

    Same shape as test_side_effect_failure_escalates_without_retry, but the
    failure is the streaming budget expiring instead of an ErrorEvent."""

    async def go() -> None:
        async with make_harness(runner_total_timeout_s=0.5, max_attempts=3) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [SideEffectFlag(tool="deploy")]
            ev = _qevent("do it", thread="tTimeoutSideEffect")
            try:
                await h.kernel.process_event(ev)
            finally:
                hold.set()

            assert h.runner.opened == ["do it"]  # exactly one attempt, no retry
            assert h.sink.last_text is not None
            assert "human" in h.sink.last_text.lower()
            assert "runner-timeout" in h.sink.last_text
            assert await h.async_redis.exists(h.config.side_effect_key(ev.event_id))
            assert await h.async_redis.exists(h.config.done_key(ev.event_id))

    asyncio.run(go())


def test_stream_timeout_without_a_side_effect_still_retries(make_harness, monkeypatch) -> None:
    """#2011, the negative path: naming the timeout must not change retry
    semantics. A flag-clean timeout is still transient, so ``runner-timeout``
    belongs in RETRYABLE_CLASSIFICATIONS and in the ``retry_class`` metric
    allowlist -- ``record_metric`` RAISES on an out-of-domain attribute value,
    so the real (un-probed) telemetry here is what catches a missing entry.

    Attempt 1 streams a delta and then hangs until the client's budget expires;
    the hold is released while the kernel backs off, so attempt 2 finds an idle
    runner (a 409 on the steer probe) and opens a fresh turn that completes.

    Two independent things make the runner idle again before attempt 2 probes
    it -- the client's disconnect cancels the hanging handler, and the releaser
    below sets ``hold`` just past the budget -- and ``FakeRunner._event`` now
    clears ``turn_active`` on both paths. That matters because /v1/steer answers
    200 while a turn is still marked live: a runner left wrongly busy would have
    the retry folded into the dead turn as a STEER instead of opening a second
    one, which is what ``steers == []`` below guards."""

    real_record_metric = kernel_module.record_metric
    recorded: list[tuple[str, dict[str, str]]] = []

    def spy(name: str, value: float = 1, *, attributes: dict[str, str] | None = None) -> None:
        recorded.append((name, dict(attributes or {})))
        # Delegate to the real recorder so the metric allowlist still validates.
        real_record_metric(name, value, attributes=attributes)

    monkeypatch.setattr(kernel_module, "record_metric", spy)

    async def go() -> None:
        async with make_harness(
            runner_total_timeout_s=0.5,
            max_attempts=3,
            retry_backoff_base_s=0.5,
            retry_backoff_max_s=0.6,
        ) as h:
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.turn_scripts = [
                [TextDelta(text="partial")],
                [Final(text="recovered", status=DONE)],
            ]

            async def release_after_the_budget_expires() -> None:
                # The turn is open; the client gives up 0.5s later and the kernel
                # then backs off 0.5s before attempt 2 probes the runner. Release
                # just past the client's budget so a handler that was not already
                # cancelled by the disconnect unwinds inside that window; either
                # way the probe finds an idle session (409 on the steer) and a
                # NEW turn is opened.
                await _wait_until(lambda: len(h.runner.opened) >= 1)
                await asyncio.sleep(0.6)
                hold.set()

            releasing = asyncio.create_task(release_after_the_budget_expires())
            ev = _qevent("go", thread="tTimeoutRetry")
            try:
                await h.kernel.process_event(ev)
            finally:
                hold.set()
                await releasing

            # Asserted FIRST: this is the #2011 property. Leaving it behind the
            # shape assertions would let a harness-timing slip surface instead of
            # the classification the test exists to pin.
            retries = [attrs for name, attrs in recorded if name == "curie.queue.retry"]
            assert retries, recorded
            assert retries[-1]["retry_class"] == "runner-timeout"

            assert h.runner.opened == ["go", "go"]  # timed out, then retried
            assert h.runner.steers == []  # the retry opened a turn, it did not steer
            assert h.sink.last_text == "recovered"
            assert not await h.async_redis.exists(h.config.side_effect_key(ev.event_id))

    asyncio.run(go())


def test_reply_delivery_timeout_is_not_a_runner_timeout(make_harness, caplog, monkeypatch) -> None:
    """#2011, the boundary: only a RUNNER timeout may claim ``runner-timeout``.

    The `except (aiohttp.ClientError, TimeoutError)` clause in `_consume` spans
    frame application, and delivering a reply has its own HTTP budget
    (`HttpReplyAdapter` builds a 30s `ClientTimeout`). So a stalled reply
    endpoint raises a bare `TimeoutError` while the runner was answering
    perfectly well -- classifying that as ``runner-timeout`` would point an
    operator at the model budget for a delivery fault. It must stay
    ``runner-error``, while still logging a NON-EMPTY reason like every other
    exception this clause catches."""

    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [TextDelta(text="partial")]

            async def stalled_delivery(*_args: object, **_kwargs: object) -> None:
                # Not a RunnerStreamTimeout: this is what aiohttp raises when the
                # reply endpoint stops answering mid-turn.
                raise TimeoutError()

            monkeypatch.setattr(h.kernel, "_apply_frame", stalled_delivery)

            qe = _qevent("go", thread="tReplyTimeout")
            with caplog.at_level(logging.WARNING, logger="curie_worker.kernel"):
                outcome = await h.kernel._attempt(qe, TargetRoute(), lambda: None)

            assert outcome.terminal_ok is False
            assert outcome.classification == "runner-error"

            dropped = [
                r.getMessage()
                for r in caplog.records
                if "turn stream dropped" in r.getMessage()
            ]
            assert dropped, caplog.text
            message = dropped[-1]
            reason = message.rsplit(":", 1)[1].strip()
            assert reason, f"the drop reason is empty: {message!r}"

    asyncio.run(go())
