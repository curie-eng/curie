"""Pure typed claim links, never live evidence, @spec COMMISSIONING-SERIES-4.

These checks establish only internal reference consistency. They do not resolve
the complete evidence graph, authenticate custody/producers, observe original
resources, establish session membership/joins, settle a trial or grant acceptance.
"""

from __future__ import annotations

from typing import Any

from .commissioning_encoding import _REFUSALS, _require
from .commissioning_run_records import TRIAL_CASES, RunPlan
from .commissioning_trial_evidence_records import (
    CaseEvidence,
    ProducerBinding,
    SessionBinding,
    SessionRoster,
    TrialCleanup,
)
from .commissioning_trial_records import (
    FINAL_CASES,
    CaseReceipt,
    TrialResult,
    TrialStart,
    _refuse,
)

__all__ = [
    "validate_case_receipt_references",
    "validate_session_roster_references",
    "validate_trial_receipt_references",
]


def _start(plan: RunPlan, start: TrialStart) -> tuple[dict[str, Any], dict[str, Any]]:
    _require(type(plan) is RunPlan and type(start) is TrialStart)
    p, s = plan.as_dict(), start.as_dict()
    _require(s["run_id"] == p["run_id"] and s["plan_digest"] == plan.digest)
    _require(s["campaign"]["bootstrap_digest"] == p["static_binding"]["bootstrap"])
    scheduled = p["trials"][int(s["ordinal"]) - 1]
    _require(s["charged_limits"] == scheduled["limits"])
    return p, s


def _receipt(start: TrialStart, receipt: CaseReceipt) -> dict[str, Any]:
    _require(type(receipt) is CaseReceipt)
    s, r = start.as_dict(), receipt.as_dict()
    _require(r["start_digest"] == start.digest)
    for key in ("run_id", "ordinal", "tuple_digest"):
        _require(r[key] == s[key])
    cases = FINAL_CASES if s["ordinal"] == "10" else (TRIAL_CASES[int(s["ordinal"]) - 1],)
    _require(r["case"] in cases)
    return r


def validate_case_receipt_references(
    plan: RunPlan,
    start: TrialStart,
    receipt: CaseReceipt,
    producer: ProducerBinding,
    evidence: tuple[CaseEvidence, ...],
) -> None:
    """Compare typed receipt/producer/evidence claims, @spec COMMISSIONING-SERIES-4.

    A successful return authenticates nothing. Subject/expected/observed digest
    targets, actual producer qualification and the full C still require their
    independent decoders, custody and fresh observations.
    """
    try:
        p, _ = _start(plan, start)
        r = _receipt(start, receipt)
        _require(type(producer) is ProducerBinding)
        _require(type(evidence) is tuple and 1 <= len(evidence) <= 256)
        _require(all(type(item) is CaseEvidence for item in evidence))
        _require(r["producer"] == producer.digest)
        _require(r["evidence_digests"] == [item.digest for item in evidence])
        b = producer.as_dict()
        _require(b["case"] == r["case"])
        for key in ("image_set", "control_profile", "projection", "execution_config"):
            _require(b[key + "_digest"] == p["static_binding"][key])
        seen: set[tuple[str, str]] = set()
        for item in evidence:
            e = item.as_dict()
            for key in ("run_id", "ordinal", "case", "challenge", "tuple_digest"):
                _require(e[key] == r[key])
            for row in e["checks"]:
                identity = row["subcase"], row["subject_digest"]
                _require(row["subcase"] in b["subcases"] and identity not in seen)
                seen.add(identity)
        _require({subcase for subcase, _ in seen} == set(b["subcases"]))
    except _REFUSALS:
        _refuse()


def validate_trial_receipt_references(
    plan: RunPlan,
    start: TrialStart,
    result: TrialResult,
    receipts: tuple[CaseReceipt, ...],
) -> None:
    """Check ordered claimed observations only, @spec COMMISSIONING-SERIES-4.

    This does not authenticate terminal_state, resolve cleanup/transition or
    validate the receipts' producer/evidence targets. No trial is settled here.
    """
    try:
        _, s = _start(plan, start)
        _require(type(result) is TrialResult)
        _require(type(receipts) is tuple and 1 <= len(receipts) <= 256)
        _require(all(type(item) is CaseReceipt for item in receipts))
        value = result.as_dict()
        _require(value["start_digest"] == start.digest)
        for key in ("run_id", "ordinal"):
            _require(value[key] == s[key])
        _require(value["observations"] == [item.digest for item in receipts])
        cases = [_receipt(start, item)["case"] for item in receipts]
        expected = FINAL_CASES if s["ordinal"] == "10" else (TRIAL_CASES[int(s["ordinal"]) - 1],)
        _require(cases == list(expected))
        # SERIES-5 gives each final case its own challenge. The start's
        # challenge cannot stand in for all independent case challenges.
        _require(len({item.as_dict()["challenge"] for item in receipts}) == len(receipts))
    except _REFUSALS:
        _refuse()


def validate_session_roster_references(
    roster: SessionRoster,
    bindings: tuple[SessionBinding, ...],
    cleanup: TrialCleanup,
) -> None:
    """Compare exact session claim links only, @spec COMMISSIONING-SERIES-4.

    No ownership entry, prior intent, actual process/transport, registry
    completeness or genuine join is observed by this pure comparison.
    """
    try:
        _require(type(roster) is SessionRoster and type(cleanup) is TrialCleanup)
        _require(type(bindings) is tuple and 1 <= len(bindings) <= 256)
        _require(all(type(item) is SessionBinding for item in bindings))
        r, c = roster.as_dict(), cleanup.as_dict()
        _require(c["session_roster_digest"] == roster.digest)
        for key in ("run_id", "ordinal"):
            _require(c[key] == r[key])
        by_digest = {item.digest: item.as_dict() for item in bindings}
        _require(len(by_digest) == len(bindings))
        _require(set(by_digest) == {row["binding_digest"] for row in r["sessions"]})
        expected: dict[str, str] = {}
        for row in r["sessions"]:
            b = by_digest[row["binding_digest"]]
            for key in ("run_id", "ordinal"):
                _require(b[key] == r[key])
            for key in ("intent_id", "kind", "owner_digest", "launch_id"):
                _require(b[key] == row[key])
            expected[row["intent_id"]] = row["binding_digest"]
        _require(len(c["session_checks"]) == len(expected))
        _require(
            {row["intent_id"]: row["binding_digest"] for row in c["session_checks"]} == expected
        )
    except _REFUSALS:
        _refuse()
