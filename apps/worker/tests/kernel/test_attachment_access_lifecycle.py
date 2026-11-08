"""Logical-turn attachment authority uses the live route, never a thread-wide grant."""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace

import pytest
from aci_protocol import Event
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.attachment_access_client import AttachmentAccessMint, AttachmentAccessUnavailable
from curie_worker.config import WorkerConfig
from curie_worker.kernel import attachment_access, constants
from curie_worker.markers import Markers
from curie_worker.sandbox.types import SandboxHandle
from redis.asyncio import Redis


class Control:
    def __init__(self):
        self.calls = []

    async def context(self, **body):
        self.calls.append(("context", body))
        return AttachmentAccessMint("opaque", 9999999999, 90)

    async def renew(self, **body):
        self.calls.append(("renew", body))
        return True

    async def revoke(self, **body):
        self.calls.append(("revoke", body))
        return True


def test_new_turn_uses_actual_route_and_end_revokes_only_own_owner(monkeypatch) -> None:
    monkeypatch.setattr(attachment_access, "_LEASE_RENEW_S", 0.01)

    async def run():
        control = Control()
        redis = Redis(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PW or None,
            decode_responses=True,
        )
        await redis.ping()
        config = WorkerConfig(key_prefix="attachment-test-" + uuid.uuid4().hex)
        markers = Markers(redis, config)

        async def status(*args, **kwargs):
            return {"attachment_access": True}

        kernel = SimpleNamespace(
            _attachment_access=control,
            _markers=markers,
            _runner=SimpleNamespace(status=status, turn_deadline_s=lambda _: 120),
            _config=SimpleNamespace(runner_facing_api_base_url="http://api/v1"),
        )
        record = attachment_access.AttemptAttachmentAccess()
        carry = attachment_access.AttachmentAccessTurn(
            str(uuid.uuid4()), str(uuid.uuid4()), "canonical/thread", "opener"
        )
        reset = constants._ATTACHMENT_ACCESS_TURN.set(carry)
        reset_record = constants._ATTACHMENT_ACCESS_ATTEMPT.set(record)
        handle = SandboxHandle(
            thread_key="canonical/thread",
            namespace="test",
            port=8080,
            claim_name="claim",
            sandbox_name="actual-sandbox",
            service_fqdn="runner",
            session_id="actual-session",
            generation=7,
        )
        try:
            result = await attachment_access.with_attachment_access(
                kernel, Event(type="message", text="read", user="user", ts="1"), handle, 100
            )
            assert result.attachment_access.token == "opaque"
            payload = control.calls[0][1]
            assert (
                payload["thread_key"],
                payload["session_id"],
                payload["sandbox_name"],
                payload["route_generation"],
            ) == ("canonical/thread", "actual-session", "actual-sandbox", 7)
            async with asyncio.timeout(1):
                while not any(op == "renew" for op, _ in control.calls):
                    await asyncio.sleep(0.01)
            await markers.record_attachment_access_turn(
                carry.thread_key, {"owner": "successor"}, 60
            )
            await attachment_access.close_attachment_access(kernel, record)
            assert (await markers.read_attachment_access_turn(carry.thread_key))[
                "owner"
            ] == "successor"
            assert control.calls[-1][1]["owner"] == record.owner
            calls_at_close = len(control.calls)
            await asyncio.sleep(0.03)
            assert len(control.calls) == calls_at_close
        finally:
            constants._ATTACHMENT_ACCESS_TURN.reset(reset)
            constants._ATTACHMENT_ACCESS_ATTEMPT.reset(reset_record)
            await record.stop()
            await redis.delete(config.attachment_access_turn_key(carry.thread_key))
            await redis.aclose()

    asyncio.run(run())


@pytest.mark.parametrize("status", [{}, {"attachment_access": 1}, {"attachment_access": False}])
def test_mixed_runner_refuses_before_mint(status) -> None:
    async def run():
        async def read(*args, **kwargs):
            return status

        kernel = SimpleNamespace(_attachment_access=Control(), _runner=SimpleNamespace(status=read))
        reset = constants._ATTACHMENT_ACCESS_TURN.set(
            attachment_access.AttachmentAccessTurn("a", "d", "t", "e")
        )
        try:
            with pytest.raises(AttachmentAccessUnavailable, match="compatible runner"):
                await attachment_access.with_attachment_access(
                    kernel,
                    Event(type="message", text="read", user="user", ts="1"),
                    SimpleNamespace(base_url="http://runner", token=""),
                    100,
                )
            assert kernel._attachment_access.calls == []
        finally:
            constants._ATTACHMENT_ACCESS_TURN.reset(reset)

    asyncio.run(run())
