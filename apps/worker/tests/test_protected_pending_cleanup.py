"""Known pending cleanup is issuer evidence, never readiness or no-model-effect."""

from __future__ import annotations

import asyncio
import hashlib
import importlib

import pytest
from curie_protected_hooks.worker_verification import Dispatchable
from curie_worker.protected_launch_records import (
    LaunchPendingSuccess,
    LaunchRequest,
    canonical_json,
)
from curie_worker.protected_retirement import OwnedRetirementProof

from .test_protected_retirement_server import actual_pair, wb

pworker_service = wb.pworker_service
owned_worker = wb.owned_worker


def pending_type():
    module = importlib.import_module("curie_worker.protected_retirement")
    assert hasattr(module, "OwnedPendingCleanupProof"), "pending cleanup evidence is absent"
    return module.OwnedPendingCleanupProof


def material():
    launch = "11111111-1111-4111-8111-111111111111"
    pending = {
        "schema_version": 1,
        "launch_id": launch,
        "receipt_revision": "1",
        "state": "preparing",
        "receipt": None,
    }
    return {
        "schema_version": 1,
        "event_id": "event/example",
        "execution_session_key": "protected:33333333-3333-4333-8333-333333333333:1:" + "a" * 64,
        "manifest_digest": "b" * 64,
        "worker_principal_id": "worker/example",
        "issuer_spki_sha256": "c" * 64,
        "request_id": "44444444-4444-4444-8444-444444444444",
        "request_sha256": "d" * 64,
        "pending_response_identity": pending,
        "pending_response_sha256": hashlib.sha256(canonical_json(pending)).hexdigest(),
        "retire_response": {
            "schema_version": 1,
            "launch_id": launch,
            "operation_id": "22222222-2222-4222-8222-222222222222",
            "state": "retired",
        },
    }


def test_pending_cleanup_closed_codec_is_distinct_and_never_ready_receipt():
    proof = pending_type()(canonical_json(material()))
    assert type(proof) is not OwnedRetirementProof
    assert "issued_receipt_sha256" not in proof.as_dict()
    assert proof.reference == "launch/11111111-1111-4111-8111-111111111111"
    assert "event/example" not in repr(proof)
    with pytest.raises(ValueError):
        OwnedRetirementProof(proof.raw)
    with pytest.raises((AttributeError, TypeError)):
        proof.raw = b"copy"


@pytest.mark.parametrize("mutation", ["digest", "launch", "pending", "extra", "ready"])
def test_pending_cleanup_codec_refuses_mixed_or_incomplete_evidence(mutation):
    value = material()
    if mutation == "digest":
        value["pending_response_sha256"] = "e" * 64
    elif mutation == "launch":
        value["retire_response"]["launch_id"] = "55555555-5555-4555-8555-555555555555"
    elif mutation == "pending":
        value["retire_response"]["state"] = "retiring"
    elif mutation == "extra":
        value["control_capability"] = "SYNTHETIC-PRIVATE-MARKER"
    else:
        value["pending_response_identity"]["state"] = "idle"
    with pytest.raises(ValueError):
        pending_type()(canonical_json(value))


@pytest.mark.parametrize("mode", ["cleaned", "incomplete", "no-container", "sibling", "unknown"])
def test_actual_registered_pending_cleanup_proof_and_refusal(owned_worker, tmp_path, mode):
    async def run():
        async with actual_pair(owned_worker, tmp_path) as (
            server,
            client,
            external,
            delivery,
            _lease,
            request,
        ):
            external.fault_mode = "delayed-ready"
            original = LaunchRequest(canonical_json(request))
            claim = asyncio.create_task(client.create(original, delivery=delivery, remaining_s=10))
            try:
                for _ in range(60):
                    if client._owned and external.pod is not None:
                        owned = next(iter(client._owned.values()))
                        row = server._registry.get("worker/example", owned.capability.launch_id)
                        if row.objects.pod_uid is not None:
                            break
                    await asyncio.sleep(0.05)
                assert client._owned and external.pod is not None
                owned = next(iter(client._owned.values()))
                assert type(owned.observation.response) is LaunchPendingSuccess
                pending = owned.observation.response.as_dict()
                assert pending["receipt"] is None
                identity = {k: v for k, v in pending.items() if k != "control_capability"}
                if mode == "incomplete":
                    external.fault_mode = "no-termination"
                elif mode == "no-container":
                    external.pod["status"]["containerStatuses"] = []
                claim.cancel()
                result = await asyncio.gather(claim, return_exceptions=True)
                assert isinstance(result[0], asyncio.CancelledError)
                proof = await client.retire_verified(owned.capability)
                row = server._registry.get("worker/example", owned.capability.launch_id)
                if mode in {"incomplete", "no-container"}:
                    assert proof is None and row.state == "retiring"
                    assert row.termination is None
                    assert client.owned_retirement_for_delivery(delivery) is None
                    return
                assert proof is not None, "actual200 known pending cleanup lost its evidence"
                assert type(proof) is pending_type()
                assert row.state == "retired" and row.retirement_outcome == "cleanup-recorded"
                assert row.termination is not None
                value = proof.as_dict()
                assert value["request_id"] == original.as_dict()["request_id"]
                assert value["request_sha256"] == original.digest
                assert value["pending_response_identity"] == identity
                assert (
                    value["pending_response_sha256"]
                    == hashlib.sha256(canonical_json(identity)).hexdigest()
                )
                assert value["retire_response"]["launch_id"] == owned.capability.launch_id
                assert pending["control_capability"] not in proof.raw.decode()
                assert "source_fields" not in proof.raw.decode()
                assert client.is_owned_retirement(proof, delivery=delivery)
                assert not client.is_owned_retirement(pending_type()(proof.raw), delivery=delivery)
                copied = Dispatchable(
                    delivery.turn, delivery.envelope, delivery.intent, delivery.source_fields
                )
                assert not client.is_owned_retirement(proof, delivery=copied)
                assert client.owned_retirement_for_delivery(delivery) is proof
                if mode in {"sibling", "unknown"}:
                    second = dict(request, request_id="66666666-6666-4666-8666-666666666666")
                    external.pause_create_response = mode == "unknown"
                    other = asyncio.create_task(
                        client.create(
                            LaunchRequest(canonical_json(second)), delivery=delivery, remaining_s=10
                        )
                    )
                    try:
                        if mode == "unknown":
                            await asyncio.wait_for(external.create_response_ready.wait(), 3)
                        else:
                            for _ in range(60):
                                if len(client._owned) == 2:
                                    break
                                await asyncio.sleep(0.05)
                            assert len(client._owned) == 2
                        assert not client.is_owned_retirement(proof, delivery=delivery)
                        assert client.owned_retirement_for_delivery(delivery) is None
                        other.cancel()
                        await asyncio.gather(other, return_exceptions=True)
                        if mode == "unknown":
                            assert not client.is_owned_retirement(proof, delivery=delivery)
                            assert client.owned_retirement_for_delivery(delivery) is None
                        else:
                            latest = client.owned_retirement_for_delivery(delivery)
                            assert latest is not None and latest is not proof
                            assert not client.is_owned_retirement(proof, delivery=delivery)
                            assert client.is_owned_retirement(latest, delivery=delivery)
                    finally:
                        external.release_create_response.set()
                        other.cancel()
                        await asyncio.gather(other, return_exceptions=True)
            finally:
                claim.cancel()
                await asyncio.gather(claim, return_exceptions=True)

    asyncio.run(run())


def test_pending_cleanup_crosscheck_detaches_ambient_failure_graph():
    value = material()
    value["pending_response_sha256"] = "e" * 64
    try:
        raise RuntimeError("SYNTHETIC-PRIVATE-MARKER")
    except RuntimeError:
        with pytest.raises(ValueError) as caught:
            pending_type()(canonical_json(value))
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
