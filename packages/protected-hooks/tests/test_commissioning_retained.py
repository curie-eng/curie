"""Closed retained state and actual Lua writer interoperability, @spec COMMISSIONING-SERIES-3."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
from pathlib import Path
from typing import Any

import pytest

from . import worker_broker as wb
from .commissioning_journal_broker import Campaign, encoded, principals, run
from .test_commissioning_records import permit_value
from .test_commissioning_reservations import Running

pworker_service = wb.pworker_service
__all__ = ["principals"]
VECTORS = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/commissioning-retained.json").read_text()
)["cases"]


def module() -> Any:
    name = "curie_protected_hooks.commissioning_retained"
    assert importlib.util.find_spec(name) is not None, "retained record decoders not implemented"
    return importlib.import_module(name)


def records(vector=None):
    from curie_protected_hooks.commissioning_records import parse_commissioning_permit

    vector = copy.deepcopy(vector or VECTORS[0])
    p = parse_commissioning_permit(encoded(permit_value() | {"limits": vector["limits"]}))
    j = {
        "record_type": "commissioning_journal",
        "permit_digest": p.digest,
        "broker_run_id": p.as_dict()["broker_identity"]["run_id"],
        **vector["expected_journal"],
    }
    r = {
        "record_type": "commissioning_reservation",
        "permit_digest": p.digest,
        "operation_id": vector["operation_id"],
        **vector["expected_reservation"],
    }
    return p, j, r


@pytest.mark.parametrize("vector", VECTORS, ids=lambda v: v["name"])
def test_closed_records_preserve_canonical_values_without_granting_authority(vector):
    api = module()
    p, j, r = records(vector)
    for value, parser, args in (
        (j, api.parse_retained_journal, ()),
        (r, api.parse_retained_reservation, (vector["operation_id"],)),
    ):
        parsed = parser(json.dumps(value, indent=2).encode(), p, *args)
        assert parsed.as_dict() == value
        assert parsed.canonical_bytes == encoded(value)
        assert parsed.digest == hashlib.sha256(encoded(value)).hexdigest()
        parsed.as_dict()["permit_digest"] = "changed"
        assert parsed.as_dict() == value
        assert p.digest not in repr(parsed)


@pytest.mark.parametrize(
    "kind,path",
    [
        ("journal", ()),
        ("journal", ("counters",)),
        ("reservation", ()),
        ("reservation", ("charge",)),
    ],
)
def test_each_retained_object_refuses_extra_and_missing_members(kind, path):
    api = module()
    p, j, r = records()
    base = j if kind == "journal" else r
    parser = api.parse_retained_journal if kind == "journal" else api.parse_retained_reservation
    args = () if kind == "journal" else (r["operation_id"],)
    target = base[path[0]] if path else base
    for name in ("unknown-member", *target):
        value = copy.deepcopy(base)
        current = value[path[0]] if path else value
        if name == "unknown-member":
            current[name] = "private-marker"
        else:
            del current[name]
        with pytest.raises(api.RetainedEvidenceInvalid) as error:
            parser(encoded(value), p, *args)
        assert "private-marker" not in str(error.value)


@pytest.mark.parametrize("bad", ["01", "-1", True, 1, 1.5, "9223372036854775808"])
def test_counts_refuse_coercion_noncanonical_or_unbounded_input(bad):
    api = module()
    p, j, r = records()
    j["counters"]["deliveries"] = bad
    r["charge"]["deliveries"] = bad
    for parser, value, args in (
        (api.parse_retained_journal, j, ()),
        (api.parse_retained_reservation, r, (r["operation_id"],)),
    ):
        with pytest.raises(api.RetainedEvidenceInvalid):
            parser(encoded(value), p, *args)


@pytest.mark.parametrize(
    "change",
    [
        {"permit_digest": "c" * 64},
        {"broker_run_id": "d" * 40},
        {"generation": "0"},
        {"state": "accepted"},
        {"state": []},
        {"exhausted": "missing-counter"},
        {"exhausted": "deliveries"},
    ],
)
def test_journal_rejects_foreign_or_inconsistent_bindings(change):
    api = module()
    p, j, _ = records()
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.parse_retained_journal(encoded(j | change), p)


@pytest.mark.parametrize(
    "change",
    [
        {"permit_digest": "c" * 64},
        {"operation_id": "99999999-9999-4999-8999-999999999999"},
        {"kind": "other"},
        {"kind": []},
        {"status": "accepted"},
        {"exhausts": True},
        {"claim": "not-a-uuid"},
        {"claim": "99999999-9999-4999-8999-999999999999"},
    ],
)
def test_reservation_rejects_cross_operation_or_invalid_state(change):
    api = module()
    p, _, r = records()
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.parse_retained_reservation(encoded(r | change), p, r["operation_id"])


def test_required_kind_counter_and_permit_ceilings_are_checked():
    api = module()
    p, j, r = records()
    j["counters"]["deliveries"] = "6"
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.parse_retained_journal(encoded(j), p)
    r["charge"]["deliveries"] = "0"
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.parse_retained_reservation(encoded(r), p, r["operation_id"])
    r["charge"]["deliveries"] = "6"
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.parse_retained_reservation(encoded(r), p, r["operation_id"])


@pytest.mark.parametrize(
    "raw", [b"null", b"[]", b"\xff", b" " * 16385, b"[" * 10000 + b"]" * 10000]
)
def test_malformed_or_oversized_required_bytes_refuse(raw):
    api = module()
    p, _, r = records()
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.parse_retained_journal(raw, p)
    with pytest.raises(api.RetainedEvidenceInvalid):
        api.parse_retained_reservation(raw, p, r["operation_id"])


def test_duplicate_required_member_is_not_normalized():
    api = module()
    p, j, r = records()
    for value, parser, args in (
        (j, api.parse_retained_journal, ()),
        (r, api.parse_retained_reservation, (r["operation_id"],)),
    ):
        raw = encoded(value)[:-1] + b',"permit_digest":"' + p.digest.encode() + b'"}'
        with pytest.raises(api.RetainedEvidenceInvalid):
            parser(raw, p, *args)


@pytest.mark.parametrize("vector", VECTORS, ids=lambda v: v["name"])
def test_actual_product_writers_and_reader_share_the_same_vector(
    pworker_service, principals, vector
):
    api = module()
    from curie_protected_hooks.commissioning_reservations import Charge

    campaign = Campaign(pworker_service, limits=vector["limits"])

    async def scenario():
        async with Running(principals, campaign) as reservations:
            await reservations.reserve(
                vector["operation_id"],
                vector["kind"],
                vector["request_digest"],
                Charge(**{k: int(v) for k, v in vector["charge"].items()}),
            )
            await reservations.dispatch(vector["operation_id"])
            j = api.parse_retained_journal(
                pworker_service.command("GET", campaign.journal_key), campaign.permit
            ).as_dict()
            r = api.parse_retained_reservation(
                pworker_service.command(
                    "GET", reservations.reservation_key(vector["operation_id"])
                ),
                campaign.permit,
                vector["operation_id"],
            ).as_dict()
            assert j == {
                "record_type": "commissioning_journal",
                "permit_digest": campaign.permit.digest,
                "broker_run_id": campaign.value["broker_identity"]["run_id"],
                **vector["expected_journal"],
            }
            assert r == {
                "record_type": "commissioning_reservation",
                "permit_digest": campaign.permit.digest,
                "operation_id": vector["operation_id"],
                **vector["expected_reservation"],
            }

    run(scenario())
