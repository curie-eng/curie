"""Live entry proof and context on owned TLS Valkey, @spec PROTECTED-HOOK-LANE-6/7.

This is the admission gate only. It does not start a Consumer, Kernel or runner,
settle a delivery, or qualify a protected runtime.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import sys
from pathlib import Path

import pytest
from curie_protected_hooks.worker_verification import Dispatchable, Parked, Refused
from curie_worker.protected_binding import _CURRENT_DELIVERY

# Pytest importlib mode does not place the repository root on sys.path.
sys.path.insert(0, str(Path(__file__).parents[3]))
proof = importlib.import_module("packages.protected-hooks.tests.test_worker_verification")
pworker_service = proof.pworker_service
owned_worker = proof.owned_worker


def gate_type():
    assert importlib.util.find_spec("curie_worker.protected_lane") is not None, (
        "protected entry gate is absent"
    )
    return importlib.import_module("curie_worker.protected_lane").ProtectedEntryGate


async def pending(client, fields):
    """Read an admitted row through the actual worker principal."""
    runs = client.lane().runs
    await runs.xgroup_create(proof.wb.STREAM, proof.wb.GROUP, id="0")
    rows = await runs.xreadgroup(proof.wb.GROUP, proof.wb.CONSUMER, {proof.wb.STREAM: ">"}, count=1)
    entry_id, actual = rows[0][1][0]
    assert actual == fields
    return entry_id


async def pel(client, entry_id):
    rows = await client.lane().runs.xpending_range(
        proof.wb.STREAM, proof.wb.GROUP, min=entry_id, max=entry_id, count=1
    )
    assert len(rows) == 1
    return rows[0]["consumer"], rows[0]["times_delivered"]


def test_preparing_proof_never_authorizes_context_or_changes_pending_budget(owned_worker):
    async def go():
        digest, fields, _ = proof.admitted(owned_worker.broker)
        owned_worker.broker.command("DEL", proof.PREFIX + "commit:" + digest)
        async with proof.connected(proof.verification(), owned_worker) as client:
            entry_id = await pending(client, fields)
            before = await pel(client, entry_id)
            gate = gate_type()(client, trusted_manifest=owned_worker.broker.manifest())
            assert not await gate.settlement_or_transfer_eligible(entry_id, None)
            async with gate.delivery_scope(fields) as verdict:
                assert type(verdict) is Parked
                assert _CURRENT_DELIVERY.get() is None
            assert await pel(client, entry_id) == before
            assert before == (proof.wb.CONSUMER, 1)
            assert await client.lane().runs.xrange(proof.wb.DEAD_LETTER) == []

    asyncio.run(go())


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
def test_dispatch_scope_clears_after_downstream_failure_and_leaves_settlement_to_consumer(
    owned_worker,
    failure,
):
    async def go():
        _, fields, _ = proof.admitted(owned_worker.broker)
        async with proof.connected(proof.verification(), owned_worker) as client:
            entry_id = await pending(client, fields)
            before = await pel(client, entry_id)
            gate = gate_type()(client, trusted_manifest=owned_worker.broker.manifest())
            assert await gate.settlement_or_transfer_eligible(entry_id, None)
            with pytest.raises(failure, match="downstream failure"):
                async with gate.delivery_scope(fields) as verdict:
                    assert type(verdict) is Dispatchable
                    assert _CURRENT_DELIVERY.get() is verdict
                    assert verdict.turn.event_id == "event/example"
                    raise failure("downstream failure")
            assert _CURRENT_DELIVERY.get() is None
            assert await pel(client, entry_id) == before

    asyncio.run(go())


def test_dispatch_reverifies_proof_changed_after_transfer_eligibility(owned_worker):
    async def go():
        digest, fields, _ = proof.admitted(owned_worker.broker)
        async with proof.connected(proof.verification(), owned_worker) as client:
            entry_id = await pending(client, fields)
            gate = gate_type()(client, trusted_manifest=owned_worker.broker.manifest())
            assert await gate.settlement_or_transfer_eligible(entry_id, fields)
            owned_worker.broker.command("DEL", proof.PREFIX + "commit:" + digest)
            before = await pel(client, entry_id)
            async with gate.delivery_scope(fields) as verdict:
                assert type(verdict) is Parked
                assert _CURRENT_DELIVERY.get() is None
            assert not await gate.settlement_or_transfer_eligible(entry_id, fields)
            assert await pel(client, entry_id) == before

    asyncio.run(go())


@pytest.mark.parametrize("mutation", ["ordinary", "extra-field", "binding-gone"])
def test_refusal_allows_only_terminal_selection_and_never_execution_context(owned_worker, mutation):
    async def go():
        _, fields, _ = proof.admitted(owned_worker.broker)
        async with proof.connected(proof.verification(), owned_worker) as client:
            entry_id = await pending(client, fields)
            before = await pel(client, entry_id)
            gate = gate_type()(client, trusted_manifest=owned_worker.broker.manifest())
            malformed = dict(fields)
            if mutation == "ordinary":
                malformed.pop("protected_envelope")
            elif mutation == "extra-field":
                malformed["opaque-extra"] = "untrusted"
            else:
                owned_worker.broker.command("DEL", proof.PREFIX + "binding:event/example")
            assert await gate.settlement_or_transfer_eligible(entry_id, malformed)
            async with gate.delivery_scope(malformed) as verdict:
                assert type(verdict) is Refused
                assert _CURRENT_DELIVERY.get() is None
            assert await pel(client, entry_id) == before
            assert await client.lane().runs.xrange(proof.wb.DEAD_LETTER) == []

    asyncio.run(go())


def test_refusal_masks_outer_context_then_restores_it(owned_worker):
    async def go():
        _, fields, _ = proof.admitted(owned_worker.broker)
        async with proof.connected(proof.verification(), owned_worker) as client:
            gate = gate_type()(client, trusted_manifest=owned_worker.broker.manifest())
            async with gate.delivery_scope(fields) as outer:
                assert type(outer) is Dispatchable
                async with gate.delivery_scope({"payload": fields["payload"]}) as refused:
                    assert type(refused) is Refused
                    assert _CURRENT_DELIVERY.get() is None
                assert _CURRENT_DELIVERY.get() is outer
            assert _CURRENT_DELIVERY.get() is None

    asyncio.run(go())
