"""Parsed retirement data alone never establishes client-owned cleanup proof."""

import importlib
import importlib.util
import json

import pytest
from curie_worker.protected_launch_client import ProtectedLaunchClient
from curie_worker.protected_substrate import ProtectedSandboxSubstrate

LAUNCH = "11111111-1111-4111-8111-111111111111"
RUNTIME = "33333333-3333-4333-8333-333333333333"


def material():
    return {
        "schema_version": 1,
        "event_id": "event/example",
        "execution_session_key": f"protected:{RUNTIME}:1:{'a' * 64}",
        "manifest_digest": "b" * 64,
        "worker_principal_id": "worker/example",
        "issuer_spki_sha256": "c" * 64,
        "issued_receipt_sha256": "d" * 64,
        "retire_response": {
            "schema_version": 1, "launch_id": LAUNCH,
            "operation_id": "22222222-2222-4222-8222-222222222222", "state": "retired",
        },
    }


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def api():
    name = "curie_worker.protected_retirement"
    assert importlib.util.find_spec(name) is not None, "owned retirement codec missing"
    return importlib.import_module(name)


def test_structural_retirement_is_immutable_and_not_client_owned():
    proof = api().OwnedRetirementProof(encode(material()))
    assert proof.reference == "launch/" + LAUNCH
    assert len(proof.digest) == 64
    client = object.__new__(ProtectedLaunchClient)
    client._owned = {}
    assert not client.is_owned_retirement(proof, delivery=None)
    assert proof.event_id == "event/example"
    # Frozen slots subclasses reject unknown/property assignment with TypeError
    # on this Python build; either exception still requires immutability.
    with pytest.raises((AttributeError, TypeError)):
        proof.raw = b"replacement"
    assert proof.raw == encode(material())
    assert LAUNCH not in repr(proof)


def test_actual_cleanup_entry_points_are_available():
    assert callable(getattr(ProtectedLaunchClient, "retire_verified", None))
    assert callable(getattr(ProtectedSandboxSubstrate, "retire_delivery", None))


def test_absent_private_launch_cannot_restore_retirement():
    client = object.__new__(ProtectedLaunchClient)
    client._owned = {}
    method = getattr(client, "owned_retirement_for_delivery", None)
    assert callable(method), "cleanup evidence must survive handle removal"
    assert method(None) is None


@pytest.mark.parametrize("mutation", ["pending", "extra", "version", "digest"])
def test_incomplete_or_open_retirement_material_refuses(mutation):
    value = material()
    if mutation == "pending":
        value["retire_response"]["state"] = "retiring"
    elif mutation == "extra":
        value["credential"] = "SYNTHETIC-PRIVATE-MARKER"
    elif mutation == "version":
        value["schema_version"] = True
    else:
        value["issued_receipt_sha256"] = "invalid"
    with pytest.raises(ValueError) as caught:
        api().OwnedRetirementProof(encode(value))
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None
