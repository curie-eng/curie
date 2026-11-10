"""Loop bridge refuses wrong-thread calls and missing original budget."""

import asyncio

import pytest
from curie_worker.protected_launch_client import LaunchClientError
from curie_worker.protected_substrate import ProtectedSandboxSubstrate, protected_claim_scope


def test_sync_claim_never_blocks_owner_loop():
    async def go():
        substrate = object.__new__(ProtectedSandboxSubstrate)
        substrate._loop = asyncio.get_running_loop()
        with pytest.raises(LaunchClientError):
            substrate.claim("thread")
        async with protected_claim_scope(asyncio.get_running_loop().time() + 1):
            with pytest.raises(LaunchClientError):
                substrate.claim("thread")

    asyncio.run(go())


def test_sync_bridge_has_no_ambient_budget():
    async def go():
        substrate = object.__new__(ProtectedSandboxSubstrate)
        substrate._loop = asyncio.get_running_loop()
        with pytest.raises(LaunchClientError):
            await asyncio.to_thread(substrate.claim, "thread")

    asyncio.run(go())


def test_duplicate_cold_claim_preserves_existing_reservation():
    from curie_worker.kernel.routing import _thread_key_for
    from curie_worker.protected_binding import protected_delivery_scope

    from apps.worker.tests.test_protected_history import dispatchable

    async def go():
        delivery = dispatchable()
        thread = _thread_key_for(delivery.turn)
        substrate = object.__new__(ProtectedSandboxSubstrate)
        substrate._handles = {}
        substrate._claims = {thread}
        async with protected_delivery_scope(delivery):
            with pytest.raises(LaunchClientError):
                await substrate.claim_async(thread, remaining_s=1)
        assert substrate._claims == {thread}

    asyncio.run(go())
