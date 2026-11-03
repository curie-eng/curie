"""Lazy references steer the existing logical turn without a file handoff."""

from __future__ import annotations

import asyncio
import time
import uuid

from aci_protocol import Attachment, Final, SessionStatus, TextDelta
from curie_worker.attachment_access_client import AttachmentAccessMint

from .test_attachment_claim import _WorkspaceProbe
from .test_thread_attachment_rebuild import _Binding, _event, _thread_key, _wire


class Control:
    def __init__(self) -> None:
        self.calls = []

    async def register(self, **body):
        self.calls.append(("register", body))

    async def context(self, **body):
        self.calls.append(("context", body))
        return AttachmentAccessMint("opaque", int(time.time()) + 600, 90)

    async def renew(self, **body):
        self.calls.append(("renew", body))
        return True

    async def revoke(self, **body):
        self.calls.append(("revoke", body))
        return True


def test_lazy_file_steers_same_opener_without_revoking_or_replacing_workspace(make_harness) -> None:
    async def run():
        binding = _Binding()
        binding.resolved.deployment_id = uuid.uuid4()
        async with make_harness(
            binding=binding,
            attachment_enabled=True,
            attachment_mode="lazy",
            internal_worker_token="worker",
            max_attempts=1,
            workspace_factory=lambda substrate: _WorkspaceProbe(
                substrate, selected_repo="acme-corp/acme-bot"
            ),
        ) as h:
            lane, ledger = _wire(h)
            control = Control()
            h.kernel._attachment_access = control
            h.runner.attachment_access_enforced = True
            hold = asyncio.Event()
            h.runner.hold = hold
            h.runner.default_script = [TextDelta(text="working")]
            h.runner.tail = [Final(text="done", status=SessionStatus.DONE)]
            opener = asyncio.create_task(
                h.kernel.process_event(_event("first", thread="lazy-steer", event_id="opener"))
            )
            try:
                async with asyncio.timeout(3):
                    while not h.runner.turn_active:
                        await asyncio.sleep(0.01)
                original = h.substrate.lookup(_thread_key("lazy-steer"))
                assert original is not None
                await h.kernel.process_event(
                    _event(
                        "inspect file",
                        thread="lazy-steer",
                        event_id="followup",
                        attachments=[Attachment(id="F1", name="data.csv", size_bytes=400_000_000)],
                    )
                )
                assert h.runner.opened == ["first"]
                assert h.runner.steers == ["inspect file"]
                contexts = [body for op, body in control.calls if op == "context"]
                assert len(contexts) == 2
                assert contexts[0]["owner"] == contexts[1]["owner"]
                assert contexts[0]["event_id"] == contexts[1]["event_id"] == "opener"
                renewal = next(body for op, body in control.calls if op == "renew")
                assert renewal["event_id"] == "opener"
                assert renewal["current_event_id"] == "followup"
                assert not any(op == "revoke" for op, _ in control.calls)
                assert lane.prepare_calls == [] and ledger.query_calls == []
                current = h.substrate.lookup(_thread_key("lazy-steer"))
                assert current.claim_name == original.claim_name
                assert current.workspace_repo == "acme-corp/acme-bot"
            finally:
                hold.set()
                await opener
            revokes = [body for op, body in control.calls if op == "revoke"]
            assert len(revokes) == 1 and revokes[0]["event_id"] == "opener"

    asyncio.run(run())


def test_cancelled_context_reply_still_revokes_the_attempt_owner(make_harness) -> None:
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
            mint_started = asyncio.Event()

            async def lost_reply(**body):
                control.calls.append(("context", body))
                mint_started.set()
                await asyncio.Event().wait()

            control.context = lost_reply
            task = asyncio.create_task(
                h.kernel.process_event(
                    _event("first", thread="cancel-mint", event_id="cancelled-opener")
                )
            )
            await asyncio.wait_for(mint_started.wait(), 3)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("cancellation was swallowed")
            opened = next(body for op, body in control.calls if op == "context")
            revoked = next(body for op, body in control.calls if op == "revoke")
            assert revoked["owner"] == opened["owner"]
            assert revoked["event_id"] == "cancelled-opener"
            assert h.runner.opened == []

    asyncio.run(run())
