"""Inert complete retained-state joins, @spec COMMISSIONING-SERIES-3.

A caller supplies its independently retained operation inventory. Consistent
control-written state is not a causal witness, complete session registry,
cleanup verdict or authority to dispatch an operation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .authority_records import _pairs, _reject_number, _scalar
from .commissioning_acl import ADMISSION, JOURNAL, OWNERSHIP, PERMIT, RESERVATION
from .commissioning_admissions import campaign_binding
from .commissioning_campaign_records import (
    CampaignAdmission,
    CampaignLaunchOwnership,
    parse_campaign_admission,
    parse_campaign_envelope,
    parse_campaign_launch_ownership,
    parse_campaign_launch_request,
)
from .commissioning_records import CommissioningPermit
from .commissioning_retained import (
    COUNTERS,
    RetainedEvidenceInvalid,
    RetainedJournal,
    RetainedReservation,
    _encode,
    _require,
    parse_retained_journal,
    parse_retained_reservation,
)

__all__ = [
    "EvidenceInventory",
    "RetainedCampaignState",
    "RetainedEvidenceInvalid",
    "decode_retained_state",
    "retained_keys",
]
_ERRORS = (ValueError, TypeError, OverflowError, RecursionError)


def _ids(value: tuple[str, ...]) -> None:
    _require(type(value) is tuple and len(value) <= 256)
    for item in value:
        _scalar(item, "uuid")
    _require(tuple(sorted(set(value))) == value)


@dataclass(frozen=True, slots=True, repr=False)
class EvidenceInventory:
    """Known IDs, never paths or a completion assertion, @spec COMMISSIONING-SERIES-3."""

    reservation_ids: tuple[str, ...] = ()
    launch_ids: tuple[str, ...] = ()
    admission_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        try:
            for value in (self.reservation_ids, self.launch_ids, self.admission_ids):
                _ids(value)
            _require(set(self.admission_ids) <= set(self.reservation_ids))
        except _ERRORS:
            raise RetainedEvidenceInvalid() from None

    def __repr__(self) -> str:
        return "EvidenceInventory()"


def retained_keys(permit: CommissioningPermit, inventory: EvidenceInventory) -> tuple[str, ...]:
    """Derive a closed key inventory from original C, @spec COMMISSIONING-SERIES-3."""
    try:
        _require(type(permit) is CommissioningPermit and type(inventory) is EvidenceInventory)
        inventory.__post_init__()
        p = permit.as_dict()
        suffix = p["campaign_id"] + ":" + p["campaign_generation"]
        return (
            PERMIT + suffix,
            JOURNAL + suffix,
            OWNERSHIP + suffix,
            *(RESERVATION + suffix + ":" + i for i in inventory.reservation_ids),
            *(OWNERSHIP + suffix + ":" + i for i in inventory.launch_ids),
            *(ADMISSION + suffix + ":" + i for i in inventory.admission_ids),
        )
    except _ERRORS:
        raise RetainedEvidenceInvalid() from None


@dataclass(frozen=True, slots=True, repr=False)
class RetainedCampaignState:
    """Correlated facts, never a dispatch ticket, @spec COMMISSIONING-SERIES-3."""

    permit: CommissioningPermit
    journal: RetainedJournal
    reservations: tuple[RetainedReservation, ...]
    ownership: tuple[CampaignLaunchOwnership, ...]
    admissions: tuple[CampaignAdmission, ...]

    def __repr__(self) -> str:
        return "RetainedCampaignState()"


def _index(raw: bytes | None, expected: tuple[str, ...]) -> None:
    if raw is None:
        _require(not expected)
        return
    _require(type(raw) is bytes and len(raw) <= 16384)
    value = json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_float=_reject_number,
        parse_constant=_reject_number,
    )
    _require(type(value) is list and len(value) <= 256)
    for item in value:
        _scalar(item, "uuid")
    _require(len(set(value)) == len(value) and tuple(sorted(value)) == expected)


def _scope(delivery: dict[str, Any], p: dict[str, Any]) -> None:
    _require(delivery["source"] in p["sources"] and delivery["target_ref"] in p["targets"])


def _admissions(
    permit: CommissioningPermit,
    inventory: EvidenceInventory,
    raw: tuple[bytes | None, ...],
    reservations: dict[str, dict[str, Any]],
) -> tuple[CampaignAdmission, ...]:
    p = permit.as_dict()
    c = campaign_binding(permit)
    result = []
    for operation, value in zip(inventory.admission_ids, raw, strict=True):
        record = parse_campaign_admission(value)  # type: ignore[arg-type]
        a = record.as_dict()
        d = a["delivery"]
        r = reservations[operation]
        _require(a["campaign"] == c and d["reservation_id"] == operation)
        _scope(d, p)
        _require(
            r["kind"] == "delivery"
            and r["status"] == "dispatched"
            and r["request_digest"] == d["request_digest"]
        )
        _require(int(p["issued_at_ms"]) <= int(a["accepted_at_ms"]) < int(p["expires_at_ms"]))
        envelope = parse_campaign_envelope(
            _encode(
                {
                    "schema_version": 1,
                    "record_type": "commissioning_envelope",
                    "campaign": c,
                    "delivery": d,
                    "tool_access": "read-only",
                }
            )
        )
        _require(envelope.digest == a["envelope_digest"])
        result.append(record)
    return tuple(result)


def _ownership(
    permit: CommissioningPermit,
    inventory: EvidenceInventory,
    raw: tuple[bytes | None, ...],
    reservations: dict[str, dict[str, Any]],
    admissions: tuple[CampaignAdmission, ...],
) -> tuple[CampaignLaunchOwnership, ...]:
    p = permit.as_dict()
    c = campaign_binding(permit)
    admitted = {a.delivery["reservation_id"]: a for a in admissions}
    claimed: dict[str, dict[str, Any]] = {}
    for r in reservations.values():
        if r["claim"]:
            _require(r["claim"] in inventory.launch_ids and r["claim"] not in claimed)
            claimed[r["claim"]] = r
    _require(set(claimed) == set(inventory.launch_ids))
    result = []
    for launch, value in zip(inventory.launch_ids, raw, strict=True):
        record = parse_campaign_launch_ownership(value)  # type: ignore[arg-type]
        o = record.as_dict()
        d = o["delivery"]
        r = claimed[launch]
        _require(o["campaign"] == c and o["launch_id"] == launch)
        _scope(d, p)
        _require(d["reservation_id"] in admitted)
        a = admitted[d["reservation_id"]]
        _require(a.delivery == d and r["request_digest"] == o["launch_request_digest"])
        request = parse_campaign_launch_request(
            _encode(
                {
                    "schema_version": 1,
                    "record_type": "commissioning_launch_request",
                    "campaign": c,
                    "delivery": d,
                    "admission_digest": a.digest,
                    "launch_id": launch,
                    "launch_reservation_id": r["operation_id"],
                    "projection_digest": o["projection_digest"],
                }
            )
        )
        _require(request.digest == o["launch_request_digest"])
        result.append(record)
    return tuple(result)


def decode_retained_state(
    permit: CommissioningPermit, inventory: EvidenceInventory, values: tuple[bytes | None, ...]
) -> RetainedCampaignState:
    """Check bytes without a causal observation, @spec COMMISSIONING-SERIES-3."""
    try:
        keys = retained_keys(permit, inventory)
        _require(type(values) is tuple and len(values) == len(keys))
        _require(values[0] == permit.canonical_bytes)
        _require(all(type(v) is bytes and len(v) <= 16384 for i, v in enumerate(values) if i != 2))
        _index(values[2], inventory.launch_ids)
        j = parse_retained_journal(values[1], permit)  # type: ignore[arg-type]
        n = len(inventory.reservation_ids)
        m = len(inventory.launch_ids)
        reservations = tuple(
            parse_retained_reservation(raw, permit, operation)  # type: ignore[arg-type]
            for operation, raw in zip(inventory.reservation_ids, values[3 : 3 + n], strict=True)
        )
        r = {row.as_dict()["operation_id"]: row.as_dict() for row in reservations}
        jv = j.as_dict()
        for counter in COUNTERS:
            _require(
                sum(int(row["charge"][counter]) for row in r.values())
                == int(jv["counters"][counter])
            )
        exhausted = [row for row in r.values() if row["exhausts"] == "1"]
        _require(len(exhausted) == (1 if jv["exhausted"] else 0))
        if exhausted:
            _require(int(exhausted[0]["charge"][jv["exhausted"]]) > 0)
        a = _admissions(permit, inventory, values[3 + n + m :], r)
        o = _ownership(permit, inventory, values[3 + n : 3 + n + m], r, a)
        return RetainedCampaignState(permit, j, reservations, o, a)
    except _ERRORS:
        raise RetainedEvidenceInvalid() from None
