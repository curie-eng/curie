"""Exact retained-state joins, never causal qualification, @spec COMMISSIONING-SERIES-3."""

from __future__ import annotations

import copy
import importlib
import importlib.util
import json
from typing import Any

import pytest
from curie_protected_hooks.commissioning_admissions import campaign_binding
from curie_protected_hooks.commissioning_campaign_records import (
    parse_campaign_admission,
    parse_campaign_envelope,
    parse_campaign_launch_request,
)

from .test_commissioning_bootstrap import encoded
from .test_commissioning_retained import records

DELIVERY = "a0000000-0000-4000-8000-000000000001"
LAUNCH_RESERVATION = "a0000000-0000-4000-8000-000000000002"
LAUNCH = "b0000000-0000-4000-8000-000000000001"
OTHER = "99999999-9999-4999-8999-999999999999"


def module() -> Any:
    name = "curie_protected_hooks.commissioning_evidence_records"
    assert importlib.util.find_spec(name) is not None, "retained correlation not implemented"
    return importlib.import_module(name)


def state_fixture(permit=None):
    p = permit or records()[0]
    pv = p.as_dict()
    c = campaign_binding(p)
    d = {
        "delivery_id": "c0000000-0000-4000-8000-000000000001",
        "attempt_id": OTHER,
        "reservation_id": DELIVERY,
        "request_digest": "a" * 64,
        "payload_digest": "b" * 64,
        "payload_bytes": "1",
        "source": pv["sources"][0],
        "target_ref": pv["targets"][0],
    }
    e = {
        "schema_version": 1,
        "record_type": "commissioning_envelope",
        "campaign": c,
        "delivery": d,
        "tool_access": "read-only",
    }
    envelope = parse_campaign_envelope(encoded(e))
    a = {
        "schema_version": 1,
        "record_type": "commissioning_admission",
        "campaign": c,
        "delivery": d,
        "envelope_digest": envelope.digest,
        "stream_entry_id": "1700000000000-0",
        "accepted_at_ms": str(int(pv["issued_at_ms"]) + 1),
    }
    admission = parse_campaign_admission(encoded(a))
    request = {
        "schema_version": 1,
        "record_type": "commissioning_launch_request",
        "campaign": c,
        "delivery": d,
        "admission_digest": admission.digest,
        "launch_id": LAUNCH,
        "launch_reservation_id": LAUNCH_RESERVATION,
        "projection_digest": "c" * 64,
    }
    request_digest = parse_campaign_launch_request(encoded(request)).digest
    o = {
        "schema_version": 1,
        "record_type": "commissioning_launch_ownership",
        "campaign": c,
        "delivery": d,
        "launch_id": LAUNCH,
        "launch_request_digest": request_digest,
        "projection_digest": "c" * 64,
        "state": "creating",
        "revision": "1",
        "workload": {
            "kind": "unbound",
            "namespace": "example-ns",
            "pod_name": "example-runner",
            "create_operation_id": "d0000000-0000-4000-8000-000000000001",
        },
    }
    zero = {key: "0" for key in pv["limits"]}
    j = {
        "record_type": "commissioning_journal",
        "permit_digest": p.digest,
        "broker_run_id": pv["broker_identity"]["run_id"],
        "state": "running",
        "generation": "2",
        "counters": zero | {"deliveries": "1", "launches": "1"},
        "exhausted": "",
    }
    r1 = {
        "record_type": "commissioning_reservation",
        "permit_digest": p.digest,
        "operation_id": DELIVERY,
        "kind": "delivery",
        "request_digest": d["request_digest"],
        "charge": zero | {"deliveries": "1"},
        "status": "dispatched",
        "claim": "",
        "exhausts": "0",
    }
    r2 = {
        "record_type": "commissioning_reservation",
        "permit_digest": p.digest,
        "operation_id": LAUNCH_RESERVATION,
        "kind": "launch",
        "request_digest": request_digest,
        "charge": zero | {"launches": "1"},
        "status": "dispatched",
        "claim": LAUNCH,
        "exhausts": "0",
    }
    return {
        "permit": p,
        "inventory": {
            "reservation_ids": (DELIVERY, LAUNCH_RESERVATION),
            "launch_ids": (LAUNCH,),
            "admission_ids": (DELIVERY,),
        },
        "values": [copy.deepcopy(value) for value in (p.as_dict(), j, [LAUNCH], r1, r2, o, a)],
    }


def values(fixture):
    return tuple(None if value is None else encoded(value) for value in fixture["values"])


def decoded(fixture):
    api = module()
    inventory = api.EvidenceInventory(**fixture["inventory"])
    return api.decode_retained_state(fixture["permit"], inventory, values(fixture))


def test_complete_state_has_exact_derived_keys_and_full_record_bindings():
    api = module()
    f = state_fixture()
    p = f["permit"]
    pv = p.as_dict()
    suffix = pv["campaign_id"] + ":" + pv["campaign_generation"]
    inventory = api.EvidenceInventory(**f["inventory"])
    expected = (
        f"protected:commissioning:permit:{suffix}",
        f"protected:commissioning:journal:{suffix}",
        f"protected:commissioning:ownership:{suffix}",
        f"protected:commissioning:reservation:{suffix}:{DELIVERY}",
        f"protected:commissioning:reservation:{suffix}:{LAUNCH_RESERVATION}",
        f"protected:commissioning:ownership:{suffix}:{LAUNCH}",
        f"protected:commissioning:admission:{suffix}:{DELIVERY}",
    )
    assert api.retained_keys(p, inventory) == expected
    result = decoded(f)
    assert result.permit.canonical_bytes == p.canonical_bytes
    assert result.journal.as_dict() == f["values"][1]
    assert len(result.reservations) == 2 and result.ownership[0].as_dict() == f["values"][5]
    assert result.admissions[0].as_dict() == f["values"][6]
    assert p.digest not in repr(result)


def test_empty_prepared_baseline_allows_only_an_absent_or_empty_index():
    api = module()
    f = state_fixture()
    j = f["values"][1]
    j.update(state="prepared", generation="1", counters={k: "0" for k in j["counters"]})
    f["values"] = [f["permit"].as_dict(), j, None]
    f["inventory"] = {}
    result = decoded(f)
    assert result.reservations == result.ownership == result.admissions == ()
    f["values"][2] = []
    assert decoded(f).journal.as_dict() == j
    f["values"][2] = [LAUNCH]
    with pytest.raises(api.RetainedEvidenceInvalid):
        decoded(f)


@pytest.mark.parametrize("index", range(7))
def test_missing_required_content_never_becomes_an_empty_success(index):
    api = module()
    f = state_fixture()
    f["values"][index] = None
    with pytest.raises(api.RetainedEvidenceInvalid):
        decoded(f)


@pytest.mark.parametrize("index", [5, 6])
@pytest.mark.parametrize(
    "field",
    [
        "campaign_id",
        "campaign_generation",
        "permit_digest",
        "bootstrap_digest",
        "manifest_digest",
        "runtime_id",
        "runtime_generation",
        "broker_run_id",
    ],
)
def test_every_common_campaign_binding_is_checked(index, field):
    api = module()
    f = state_fixture()
    f["values"][index]["campaign"][field] = (
        "8"
        if field.endswith("generation")
        else (
            "e" * 40
            if field == "broker_run_id"
            else ("f" * 64 if field.endswith("digest") else OTHER)
        )
    )
    with pytest.raises(api.RetainedEvidenceInvalid):
        decoded(f)


@pytest.mark.parametrize("bad", [[LAUNCH, LAUNCH], [OTHER], {}, [1]])
def test_index_is_not_an_unchecked_roster(bad):
    api = module()
    f = state_fixture()
    f["values"][2] = bad
    with pytest.raises(api.RetainedEvidenceInvalid):
        decoded(f)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda f: f["values"][1]["counters"].update(deliveries="2"),
        lambda f: f["values"][3].update(status="reserved"),
        lambda f: f["values"][4].update(claim=OTHER),
        lambda f: f["values"][4].update(request_digest="f" * 64),
        lambda f: f["values"][4].update(exhausts="1"),
        lambda f: f["values"][5]["delivery"].update(attempt_id=LAUNCH),
        lambda f: f["values"][5].update(projection_digest="f" * 64),
        lambda f: f["values"][6].update(envelope_digest="f" * 64),
        lambda f: f["values"][6]["delivery"].update(reservation_id=OTHER),
        lambda f: f["values"][6]["delivery"].update(request_digest="f" * 64),
    ],
)
def test_charges_admission_and_original_launch_cannot_be_cross_linked(mutation):
    api = module()
    f = state_fixture()
    mutation(f)
    with pytest.raises(api.RetainedEvidenceInvalid):
        decoded(f)


@pytest.mark.parametrize(
    "inventory",
    [
        {"reservation_ids": [DELIVERY]},
        {"reservation_ids": (DELIVERY, DELIVERY)},
        {"reservation_ids": (LAUNCH_RESERVATION, DELIVERY)},
        {"reservation_ids": ("../foreign",)},
        {"admission_ids": (DELIVERY,)},
        {"launch_ids": (False,)},
        {"reservation_ids": tuple(f"eeeeeeee-eeee-4eee-8eee-{i:012x}" for i in range(257))},
    ],
)
def test_inventory_is_canonical_bounded_and_not_an_arbitrary_key_source(inventory):
    api = module()
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.EvidenceInventory(**inventory)


def test_noncanonical_permit_and_extra_record_count_refuse():
    api = module()
    f = state_fixture()
    inventory = api.EvidenceInventory(**f["inventory"])
    raw = list(values(f))
    raw[0] = json.dumps(f["permit"].as_dict(), indent=2).encode()
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.decode_retained_state(f["permit"], inventory, tuple(raw))
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.decode_retained_state(f["permit"], inventory, values(f) + (b"{}",))
