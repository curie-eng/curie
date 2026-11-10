"""Typed claim links supply no live proof, @spec COMMISSIONING-SERIES-4."""

from __future__ import annotations

import copy
import importlib
from typing import Any

import pytest
from curie_protected_hooks.commissioning_encoding import CommissioningRecordInvalid
from curie_protected_hooks.commissioning_run_records import parse_run_plan
from curie_protected_hooks.commissioning_trial_evidence_records import (
    parse_case_evidence,
    parse_producer_binding,
    parse_session_binding,
    parse_session_roster,
    parse_trial_cleanup,
)
from curie_protected_hooks.commissioning_trial_records import (
    FINAL_CASES,
    parse_case_receipt,
    parse_trial_result,
    parse_trial_start,
)

from .test_commissioning_run_records import plan_value
from .test_commissioning_trial_evidence_records import values as target_values
from .test_commissioning_trial_records import broker, digest, encoded, uuid
from .test_commissioning_trial_records import values as trial_values


def api() -> Any:
    return importlib.import_module("curie_protected_hooks.commissioning_trial_links")


def claims(*, ordinal: int = 1) -> dict[str, Any]:
    plan = plan_value()
    start, receipt, result = (
        trial_values()[name] for name in ("trial_start", "case_receipt", "trial_result")
    )
    case = plan["trials"][ordinal - 1]["case"] if ordinal != 10 else FINAL_CASES[0]
    for value in (start, receipt, result):
        value.update(run_id=plan["run_id"], ordinal=str(ordinal))
    start["charged_limits"] = plan["trials"][ordinal - 1]["limits"].copy()
    start["campaign"]["bootstrap_digest"] = plan["static_binding"]["bootstrap"]
    receipt["case"] = case
    producer = target_values()["producer_binding"]
    producer["case"] = case
    for field in ("image_set", "control_profile", "projection", "execution_config"):
        producer[field + "_digest"] = plan["static_binding"][field]
    evidence = []
    for subcase in producer["subcases"]:
        item = target_values()["case_evidence"]
        item.update(
            run_id=plan["run_id"],
            ordinal=str(ordinal),
            case=case,
            challenge=start["challenge"],
            tuple_digest=start["tuple_digest"],
        )
        item["checks"][0]["subcase"] = subcase
        evidence.append(item)
    return dict(
        plan=plan, start=start, receipt=receipt, result=result, producer=producer, evidence=evidence
    )


def materialize(value: dict[str, Any]) -> tuple[Any, ...]:
    plan = parse_run_plan(encoded(value["plan"]))
    start = parse_trial_start(encoded(value["start"] | {"plan_digest": plan.digest}))
    producer = parse_producer_binding(encoded(value["producer"]))
    evidence = tuple(parse_case_evidence(encoded(item)) for item in value["evidence"])
    receipt = parse_case_receipt(
        encoded(
            value["receipt"]
            | {
                "start_digest": start.digest,
                "producer": producer.digest,
                "evidence_digests": [item.digest for item in evidence],
            }
        )
    )
    result = parse_trial_result(
        encoded(
            value["result"]
            | {
                "start_digest": start.digest,
                "observations": [receipt.digest],
            }
        )
    )
    return plan, start, receipt, producer, evidence, result


def check_case(value: dict[str, Any]) -> None:
    plan, start, receipt, producer, evidence, _ = materialize(value)
    api().validate_case_receipt_references(plan, start, receipt, producer, evidence)


def test_split_evidence_links_are_pure_claim_checks_without_a_verdict() -> None:
    value = claims()
    before = copy.deepcopy(value)
    assert check_case(value) is None
    assert value == before
    plan, start, receipt, _, _, result = materialize(value)
    assert api().validate_trial_receipt_references(plan, start, result, (receipt,)) is None


@pytest.mark.parametrize(
    "target,field,replacement",
    [
        ("start", "run_id", uuid(90)),
        ("start", "charged_limits", {name: "2" for name in plan_value()["aggregate_limits"]}),
        ("receipt", "run_id", uuid(90)),
        ("receipt", "ordinal", "2"),
        ("receipt", "challenge", uuid(90)),
        ("receipt", "tuple_digest", digest(90)),
        ("receipt", "case", "clean-session"),
        ("producer", "case", "clean-session"),
        *[
            ("producer", name + "_digest", digest(90))
            for name in ("image_set", "control_profile", "projection", "execution_config")
        ],
    ],
)
def test_rehashed_outer_records_do_not_repair_stale_claims(target, field, replacement) -> None:
    value = claims()
    value[target][field] = replacement
    with pytest.raises(CommissioningRecordInvalid):
        check_case(value)


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("run_id", uuid(90)),
        ("ordinal", "2"),
        ("case", "clean-session"),
        ("challenge", uuid(90)),
        ("tuple_digest", digest(90)),
    ],
)
def test_rehashed_evidence_cannot_cross_context(field, replacement) -> None:
    value = claims()
    value["evidence"][0][field] = replacement
    with pytest.raises(CommissioningRecordInvalid):
        check_case(value)


@pytest.mark.parametrize("attack", ["missing-subcase", "extra-subcase", "repeated-subject"])
def test_subcase_inventory_cannot_be_repaired_by_splitting_wrappers(attack) -> None:
    value = claims()
    if attack == "missing-subcase":
        value["evidence"].pop()
    elif attack == "extra-subcase":
        value["evidence"][0]["checks"][0]["subcase"] = "undeclared"
    else:
        extra = copy.deepcopy(value["evidence"][0])
        extra["checks"][0]["observed_digest"] = digest(91)
        value["evidence"].append(extra)
    with pytest.raises(CommissioningRecordInvalid):
        check_case(value)


def test_one_subcase_may_cover_distinct_original_subjects() -> None:
    value = claims()
    extra = copy.deepcopy(value["evidence"][0])
    extra["checks"][0]["subject_digest"] = digest(91)
    value["evidence"].append(extra)
    check_case(value)


def test_each_final_case_may_have_its_own_challenge() -> None:
    plan, start, result, receipts = final_receipts()
    receipts = tuple(
        parse_case_receipt(encoded(item.as_dict() | {"challenge": uuid(200 + i)}))
        for i, item in enumerate(receipts)
    )
    result = parse_trial_result(
        encoded(
            result.as_dict()
            | {
                "observations": [item.digest for item in receipts],
            }
        )
    )
    assert api().validate_trial_receipt_references(plan, start, result, receipts) is None


def test_rehashed_start_cannot_replace_the_sealed_bootstrap() -> None:
    value = claims()
    value["start"]["campaign"]["bootstrap_digest"] = digest(90)
    with pytest.raises(CommissioningRecordInvalid):
        check_case(value)


def test_final_receipts_cannot_reuse_one_challenge_for_every_case() -> None:
    plan, start, result, receipts = final_receipts()
    receipts = tuple(
        parse_case_receipt(
            encoded(
                item.as_dict()
                | {
                    "challenge": start.as_dict()["challenge"],
                }
            )
        )
        for item in receipts
    )
    result = parse_trial_result(
        encoded(
            result.as_dict()
            | {
                "observations": [item.digest for item in receipts],
            }
        )
    )
    with pytest.raises(CommissioningRecordInvalid):
        api().validate_trial_receipt_references(plan, start, result, receipts)


@pytest.mark.parametrize("slot", range(5))
def test_wrong_record_type_refuses_without_echoing_input(slot) -> None:
    args = list(materialize(claims())[:5])
    args[slot] = args[0] if slot else args[1]
    with pytest.raises(CommissioningRecordInvalid) as caught:
        api().validate_case_receipt_references(*args)
    assert str(caught.value) == "Invalid protected commissioning record"
    assert caught.value.__context__ is None


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("run_id", uuid(90)),
        ("ordinal", "2"),
        ("start_digest", digest(90)),
        ("observations", [digest(90)]),
    ],
)
def test_trial_result_links_are_not_just_hash_shaped(field, replacement) -> None:
    plan, start, receipt, _, _, result = materialize(claims())
    changed = parse_trial_result(encoded(result.as_dict() | {field: replacement}))
    with pytest.raises(CommissioningRecordInvalid):
        api().validate_trial_receipt_references(plan, start, changed, (receipt,))


def final_receipts() -> tuple[Any, ...]:
    plan, start, receipt, _, _, result = materialize(claims(ordinal=10))
    receipts = tuple(
        parse_case_receipt(encoded(receipt.as_dict() | {"case": case, "challenge": uuid(200 + i)}))
        for i, case in enumerate(FINAL_CASES)
    )
    result = parse_trial_result(
        encoded(
            result.as_dict()
            | {
                "observations": [item.digest for item in receipts],
            }
        )
    )
    return plan, start, result, receipts


def test_final_receipts_are_the_complete_fresh_ordered_inventory() -> None:
    assert api().validate_trial_receipt_references(*final_receipts()) is None


@pytest.mark.parametrize("attack", ["missing", "reordered", "extra", "historical", "stale"])
def test_final_result_does_not_accept_a_different_receipt_set(attack) -> None:
    plan, start, result, receipts = final_receipts()
    rows = list(receipts)
    if attack == "missing":
        rows.pop()
    elif attack == "reordered":
        rows[0], rows[1] = rows[1], rows[0]
    elif attack == "extra":
        rows.append(parse_case_receipt(encoded(rows[0].as_dict() | {"case": "final"})))
    else:
        rows[0] = parse_case_receipt(
            encoded(
                rows[0].as_dict()
                | {
                    "case" if attack == "historical" else "ordinal": "exhaust-deliveries"
                    if attack == "historical"
                    else "1",
                }
            )
        )
    # Rehash the result too: matching digests alone must not fix the inventory.
    result = parse_trial_result(
        encoded(
            result.as_dict()
            | {
                "observations": [item.digest for item in rows],
            }
        )
    )
    with pytest.raises(CommissioningRecordInvalid):
        api().validate_trial_receipt_references(plan, start, result, tuple(rows))


def session_claims() -> tuple[Any, ...]:
    seed = target_values()
    bindings = []
    for number, kind in enumerate(("broker", "runner", "sdk"), 20):
        value = copy.deepcopy(seed["session_binding"])
        value.update(kind=kind, intent_id=uuid(number))
        if kind == "broker":
            value["launch_id"] = None
            value["identity"] = {
                "client_id": "42",
                "broker_identity": broker(),
                "credential": {"id": "acme-observer", "generation": "1"},
            }
        elif kind == "runner":
            process = value["identity"]["process"]
            value["identity"] = {"container_id": process["container_id"], "process": process}
        bindings.append(parse_session_binding(encoded(value)))
    rows = []
    for binding in bindings:
        value = binding.as_dict()
        rows.append(
            {key: value[key] for key in ("intent_id", "kind", "owner_digest", "launch_id")}
            | {"binding_digest": binding.digest}
        )
    roster = parse_session_roster(encoded(seed["session_roster"] | {"sessions": rows}))
    checks = [
        {
            "intent_id": row["intent_id"],
            "binding_digest": row["binding_digest"],
            "join_observed": True,
            "transport_closed": True,
        }
        for row in rows
    ]
    cleanup = parse_trial_cleanup(
        encoded(
            seed["trial_cleanup"]
            | {
                "session_roster_digest": roster.digest,
                "session_checks": sorted(checks, key=lambda row: row["intent_id"]),
            }
        )
    )
    return roster, tuple(bindings), cleanup


def test_session_links_compare_claims_of_all_three_kinds_without_observing_joins() -> None:
    roster, bindings, cleanup = session_claims()
    assert api().validate_session_roster_references(roster, bindings, cleanup) is None
    # The supplied record pool has no traversal order; the roster does.
    assert api().validate_session_roster_references(roster, bindings[::-1], cleanup) is None


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("run_id", uuid(90)),
        ("ordinal", "2"),
        ("intent_id", uuid(90)),
        ("owner_digest", digest(90)),
        ("launch_id", uuid(90)),
    ],
)
def test_rehashed_binding_must_agree_with_its_roster_row(field, replacement) -> None:
    roster, bindings, cleanup = session_claims()
    rows = roster.as_dict()["sessions"]
    binding = parse_session_binding(encoded(bindings[-1].as_dict() | {field: replacement}))
    rows[-1]["binding_digest"] = binding.digest
    roster = parse_session_roster(encoded(roster.as_dict() | {"sessions": rows}))
    checks = cleanup.as_dict()["session_checks"]
    checks[-1]["binding_digest"] = binding.digest
    cleanup = parse_trial_cleanup(
        encoded(
            cleanup.as_dict()
            | {
                "session_roster_digest": roster.digest,
                "session_checks": checks,
            }
        )
    )
    with pytest.raises(CommissioningRecordInvalid):
        api().validate_session_roster_references(roster, (*bindings[:-1], binding), cleanup)


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("run_id", uuid(90)),
        ("ordinal", "2"),
        ("session_roster_digest", digest(90)),
    ],
)
def test_cleanup_cannot_name_a_different_trial_or_roster(field, replacement) -> None:
    roster, bindings, cleanup = session_claims()
    cleanup = parse_trial_cleanup(encoded(cleanup.as_dict() | {field: replacement}))
    with pytest.raises(CommissioningRecordInvalid):
        api().validate_session_roster_references(roster, bindings, cleanup)


@pytest.mark.parametrize(
    "attack",
    [
        "missing-binding",
        "extra-binding",
        "duplicate-binding",
        "missing-check",
        "stale-check",
        "extra-check",
        "kind",
    ],
)
def test_session_link_inventory_requires_exact_references(attack) -> None:
    roster, bindings, cleanup = session_claims()
    if attack == "missing-binding":
        bindings = bindings[:-1]
    elif attack == "extra-binding":
        extra = parse_session_binding(encoded(bindings[-1].as_dict() | {"intent_id": uuid(90)}))
        bindings = (*bindings, extra)
    elif attack == "duplicate-binding":
        bindings = (*bindings, bindings[-1])
    elif attack == "kind":
        rows = roster.as_dict()["sessions"]
        rows[-1]["kind"] = "runner"
        rows.sort(key=lambda row: (row["kind"], row["intent_id"]))
        roster = parse_session_roster(encoded(roster.as_dict() | {"sessions": rows}))
        cleanup = parse_trial_cleanup(
            encoded(
                cleanup.as_dict()
                | {
                    "session_roster_digest": roster.digest,
                }
            )
        )
    else:
        checks = cleanup.as_dict()["session_checks"]
        if attack == "missing-check":
            checks.pop()
        elif attack == "stale-check":
            checks[-1]["binding_digest"] = digest(90)
        else:
            checks.append(checks[-1] | {"intent_id": uuid(90)})
        cleanup = parse_trial_cleanup(encoded(cleanup.as_dict() | {"session_checks": checks}))
    with pytest.raises(CommissioningRecordInvalid):
        api().validate_session_roster_references(roster, bindings, cleanup)


@pytest.mark.parametrize("slot", range(3))
def test_session_link_wrong_type_is_a_redacted_refusal(slot) -> None:
    args = list(session_claims())
    args[slot] = args[0] if slot else args[2]
    with pytest.raises(CommissioningRecordInvalid) as caught:
        api().validate_session_roster_references(*args)
    assert caught.value.__context__ is None
