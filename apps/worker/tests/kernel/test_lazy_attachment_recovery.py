"""Late attachment settlement preserves kernel finish races and continuations."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from aci_protocol import TextDelta
from curie_worker.kernel.routing import _thread_key_for

from .test_lazy_attachment_routing import Control
from .test_thread_attachment_rebuild import _Binding, _event, _thread_key
from .test_work_item_early_stop import (
    ISSUE_PROMPT,
    ZERO_WORK_TURN,
    _done,
    _PublicationApi,
    _turn,
    _WorkItems,
    _Workspace,
)
from .test_work_item_early_stop import (
    _Binding as FactoryBinding,
)


@pytest.mark.parametrize("authority", ["missing", "renew-refused"])
def test_lazy_finish_race_opens_fresh_turn_after_opener_settlement(make_harness, authority):
    async def run():
        binding = _Binding()
        binding.resolved.deployment_id = uuid.uuid4()
        async with make_harness(
            binding=binding,
            attachment_enabled=True,
            attachment_mode="lazy",
            internal_worker_token="worker",
            max_attempts=1,
        ) as h:
            control = Control()
            h.kernel._attachment_access = control
            h.runner.attachment_access_enforced = True
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [_done("done")]
            opener = asyncio.create_task(
                h.kernel.process_event(_event("first", thread="finish", event_id="opener"))
            )
            try:
                async with asyncio.timeout(3):
                    while not h.runner.turn_active:
                        await asyncio.sleep(0.01)
                live = await h.kernel._markers.read_attachment_access_turn(_thread_key("finish"))
                assert live is not None
                original_status = h.kernel._runner.status
                tripped = False

                async def status_then_finish(*args, **kwargs):
                    nonlocal tripped
                    snapshot = await original_status(*args, **kwargs)
                    if not tripped and snapshot.get("turn_active") is True:
                        tripped = True
                        hold.set()
                        await opener
                        if authority == "renew-refused":
                            await h.kernel._markers.record_attachment_access_turn(
                                _thread_key("finish"), live, 60
                            )
                    return snapshot

                h.kernel._runner.status = status_then_finish
                if authority == "renew-refused":

                    async def refused(**body):
                        control.calls.append(("renew", body))
                        return False

                    control.renew = refused
                await h.kernel.process_event(
                    _event("second", thread="finish", event_id="successor")
                )
                assert tripped
                assert h.runner.opened == ["first", "second"]
                assert h.runner.steers == []
                contexts = [body for op, body in control.calls if op == "context"]
                assert contexts[-1]["event_id"] == "successor"
                assert contexts[-1]["owner"] != contexts[0]["owner"]
            finally:
                hold.set()
                await opener

    asyncio.run(run())


@pytest.mark.parametrize("authority", ["missing", "renew-refused"])
def test_lazy_missing_authority_on_active_turn_does_not_open_or_steer(make_harness, authority):
    async def run():
        binding = _Binding()
        binding.resolved.deployment_id = uuid.uuid4()
        async with make_harness(
            binding=binding,
            attachment_enabled=True,
            attachment_mode="lazy",
            internal_worker_token="worker",
            max_attempts=1,
        ) as h:
            control = Control()
            h.kernel._attachment_access = control
            h.runner.attachment_access_enforced = True
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [_done("done")]
            opener = asyncio.create_task(
                h.kernel.process_event(_event("first", thread="active", event_id="opener"))
            )
            try:
                async with asyncio.timeout(3):
                    while not h.runner.turn_active:
                        await asyncio.sleep(0.01)
                live = await h.kernel._markers.read_attachment_access_turn(_thread_key("active"))
                assert live is not None
                if authority == "missing":
                    await h.kernel._markers.take_attachment_access_turn(
                        _thread_key("active"), live["owner"]
                    )
                else:

                    async def refused(**body):
                        return False

                    control.renew = refused
                await h.kernel.process_event(_event("second", thread="active", event_id="refused"))
                assert h.runner.turn_active
                assert h.runner.opened == ["first"]
                assert h.runner.steers == []
                assert len([body for op, body in control.calls if op == "context"]) == 1
            finally:
                hold.set()
                await opener

    asyncio.run(run())


def test_lazy_factory_continuation_carries_same_live_opener_and_settles_it(make_harness):
    async def run():
        async with make_harness(
            binding=FactoryBinding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
            attachment_enabled=True,
            attachment_mode="lazy",
            internal_worker_token="worker",
            max_attempts=1,
        ) as h:
            control = Control()
            h.kernel._attachment_access = control
            h.kernel._work_items = _WorkItems()
            h.runner.attachment_access_enforced = True
            h.runner.turn_scripts = [list(ZERO_WORK_TURN), [_done("Cannot complete this request.")]]
            event = _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            await h.kernel.process_event(event)
            assert len(h.runner.event_bodies) == 2
            assert all(body.get("attachment_access") is not None for body in h.runner.event_bodies)
            contexts = [body for op, body in control.calls if op == "context"]
            assert len(contexts) == 2
            assert contexts[0]["owner"] == contexts[1]["owner"]
            assert contexts[0]["event_id"] == contexts[1]["event_id"] == event.event_id
            revokes = [body for op, body in control.calls if op == "revoke"]
            assert revokes and all(body["owner"] == contexts[0]["owner"] for body in revokes)
            assert (
                await h.kernel._markers.read_attachment_access_turn(_thread_key_for(event)) is None
            )
            assert h.kernel._work_items.finishes[0]["cause"] == "early_stop"

    asyncio.run(run())


def test_lazy_factory_continuation_refuses_displaced_authority_before_model(make_harness):
    from curie_worker.attachment_access_client import AttachmentAccessUnavailable

    async def run():
        async with make_harness(
            binding=FactoryBinding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
            attachment_enabled=True,
            attachment_mode="lazy",
            internal_worker_token="worker",
            max_attempts=1,
        ) as h:
            control = Control()
            h.kernel._attachment_access = control
            items = _WorkItems()
            h.kernel._work_items = items
            h.runner.attachment_access_enforced = True
            h.runner.turn_scripts = [list(ZERO_WORK_TURN), [_done("must not run")]]
            original_context = control.context
            calls = 0

            async def context(**body):
                nonlocal calls
                calls += 1
                if calls > 1:
                    raise AttachmentAccessUnavailable("the opener was replaced")
                return await original_context(**body)

            control.context = context
            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))
            assert calls == 2
            assert len(h.runner.opened) == 1
            assert len(items.finishes) == 1
            assert items.finishes[0]["cause"] in {"runner_failed", "runner_escalated"}
            assert any(op == "revoke" for op, _ in control.calls)

    asyncio.run(run())


def test_lazy_factory_continuation_cancellation_revokes_original_owner(make_harness):
    async def run():
        async with make_harness(
            binding=FactoryBinding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
            attachment_enabled=True,
            attachment_mode="lazy",
            internal_worker_token="worker",
            max_attempts=1,
        ) as h:
            from aci_protocol import TextDelta

            control = Control()
            h.kernel._attachment_access = control
            h.kernel._work_items = _WorkItems()
            h.runner.attachment_access_enforced = True
            h.runner.turn_scripts = [list(ZERO_WORK_TURN), [TextDelta(text="working")]]
            original_context = control.context
            calls = 0
            hold = asyncio.Event()

            async def context(**body):
                nonlocal calls
                calls += 1
                if calls == 2:
                    h.runner.hold = hold
                return await original_context(**body)

            control.context = context
            event = _turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT)
            task = asyncio.create_task(h.kernel.process_event(event))
            try:
                async with asyncio.timeout(3):
                    while len(h.runner.event_bodies) < 2 or not h.runner.turn_active:
                        await asyncio.sleep(0.01)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                contexts = [body for op, body in control.calls if op == "context"]
                revokes = [body for op, body in control.calls if op == "revoke"]
                assert contexts[0]["owner"] == contexts[1]["owner"]
                assert revokes and all(body["owner"] == contexts[0]["owner"] for body in revokes)
                assert (
                    await h.kernel._markers.read_attachment_access_turn(_thread_key_for(event))
                    is None
                )
            finally:
                hold.set()
                await asyncio.gather(task, return_exceptions=True)

    asyncio.run(run())
