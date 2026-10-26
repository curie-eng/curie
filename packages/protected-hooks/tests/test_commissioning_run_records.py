"""Closed inert run records, @spec COMMISSIONING-SERIES-4."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
from typing import Any

import pytest

from .test_commissioning_bootstrap import at, encoded, object_paths

CASES = (
    "exhaust-deliveries",
    "exhaust-launches",
    "exhaust-model-calls",
    "exhaust-output-tokens",
    "exhaust-spend",
    "live-session-expiry",
    "control-restart",
    "interrupted-closing",
    "broker-restart",
    "final",
)
COUNTERS = ("deliveries", "launches", "model_calls", "output_tokens", "spend_microusd")


def intent_value() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "record_type": "commissioning_run_intent",
        "run_id": "11111111-1111-4111-8111-111111111111",
        "installation_id": "22222222-2222-4222-8222-222222222222",
        "recipe_digest": "a" * 64,
        "trials": [
            {
                "ordinal": str(i),
                "case": case,
                "limits": {key: "1" for key in COUNTERS},
                "max_duration_ms": "1000",
            }
            for i, case in enumerate(CASES, 1)
        ],
        "aggregate_limits": {key: "10" for key in COUNTERS},
        "max_elapsed_ms": "10000",
    }


def plan_value() -> dict[str, Any]:
    intent = intent_value()
    value = {key: copy.deepcopy(val) for key, val in intent.items() if key != "recipe_digest"}
    return value | {
        "record_type": "commissioning_run_plan",
        "intent_digest": hashlib.sha256(encoded(intent)).hexdigest(),
        "static_binding": {
            key: "b" * 64
            for key in (
                "bootstrap",
                "guard",
                "execution_config",
                "control_profile",
                "projection",
                "image_set",
                "sdk_profile",
                "network_identity",
                "ownership_anchors",
            )
        },
    }


def event_value() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "record_type": "commissioning_run_event",
        "run_id": intent_value()["run_id"],
        "revision": "1",
        "previous_digest": "0" * 64,
        "kind": "prepared",
        "ordinal": "0",
        "subject_digest": "b" * 64,
        "elapsed_ms": "0",
    }


def module() -> Any:
    name = "curie_protected_hooks.commissioning_run_records"
    assert importlib.util.find_spec(name) is not None, "run custody records not implemented"
    return importlib.import_module(name)


@pytest.mark.parametrize(
    "factory,parser",
    [
        (intent_value, "parse_run_intent"),
        (plan_value, "parse_run_plan"),
        (event_value, "parse_run_event"),
    ],
)
def test_each_record_is_canonical_immutable_and_has_an_independent_digest(factory, parser) -> None:
    api = module()
    value = factory()
    record = getattr(api, parser)(encoded(value))
    assert record.canonical_bytes == encoded(value)
    assert record.digest == hashlib.sha256(encoded(value)).hexdigest()
    assert record.as_dict() == value
    record.as_dict()["run_id"] = "changed"
    assert record.as_dict() == value
    assert "11111111" not in repr(record)


@pytest.mark.parametrize("path", object_paths(intent_value()))
def test_intent_is_closed_at_every_nested_object(path: tuple[Any, ...]) -> None:
    api = module()
    value = intent_value()
    at(value, path)["unexpected"] = "untrusted-private-marker"
    with pytest.raises(api.CommissioningRecordInvalid) as error:
        api.parse_run_intent(encoded(value))
    assert "untrusted-private-marker" not in str(error.value)
    for key in at(intent_value(), path):
        value = intent_value()
        del at(value, path)[key]
        with pytest.raises(api.CommissioningRecordInvalid):
            api.parse_run_intent(encoded(value))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(schema_version=True),
        lambda v: v.update(schema_version=2),
        lambda v: v.update(record_type="commissioning_permit"),
        lambda v: v["trials"].pop(),
        lambda v: v["trials"].reverse(),
        lambda v: v["trials"][0].update(case="different-stop"),
        lambda v: v["trials"][0].update(ordinal="0"),
        lambda v: v["trials"][0]["limits"].update(deliveries="0"),
        lambda v: v["trials"][0]["limits"].update(deliveries=1),
        lambda v: v["aggregate_limits"].update(model_calls="9"),
        lambda v: v.update(max_elapsed_ms="0"),
    ],
)
def test_intent_refuses_changed_inventory_or_unfunded_envelopes(mutation) -> None:
    api = module()
    value = intent_value()
    mutation(value)
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_run_intent(encoded(value))


def test_plan_seals_the_existing_intent_and_cannot_change_its_bounds() -> None:
    api = module()
    intent = api.parse_run_intent(encoded(intent_value()))
    assert api.validate_run_plan(intent, api.parse_run_plan(encoded(plan_value()))) is None
    for field in ("run_id", "installation_id", "intent_digest", "max_elapsed_ms"):
        value = plan_value()
        value[field] = {
            "run_id": "33333333-3333-4333-8333-333333333333",
            "installation_id": "33333333-3333-4333-8333-333333333333",
            "intent_digest": "c" * 64,
            "max_elapsed_ms": "9999",
        }[field]
        with pytest.raises(api.CommissioningRecordInvalid):
            api.validate_run_plan(intent, api.parse_run_plan(encoded(value)))
    value = plan_value()
    value["aggregate_limits"]["launches"] = "11"
    with pytest.raises(api.CommissioningRecordInvalid):
        api.validate_run_plan(intent, api.parse_run_plan(encoded(value)))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda v: v.update(kind="accepted"),
        lambda v: v.update(kind="charged"),
        lambda v: v.update(ordinal="00"),
        lambda v: v.update(ordinal=0),
        lambda v: v.update(previous_digest="a" * 64),
        lambda v: v.update(revision="0"),
        lambda v: v.update(elapsed_ms=-1),
        lambda v: v.update(elapsed_ms="-1"),
    ],
)
def test_event_refuses_phase_substitution_or_invalid_scalar(mutation) -> None:
    api = module()
    value = event_value()
    mutation(value)
    with pytest.raises(api.CommissioningRecordInvalid):
        api.parse_run_event(encoded(value))


def test_charged_event_has_positive_trial_ordinal_and_retained_predecessor() -> None:
    api = module()
    value = event_value() | {
        "kind": "charged",
        "ordinal": "1",
        "revision": "3",
        "previous_digest": "c" * 64,
    }
    assert api.parse_run_event(encoded(value)).as_dict() == value
    for changes in ({"ordinal": "11"}, {"previous_digest": "0" * 64}, {"ordinal": "0"}):
        with pytest.raises(api.CommissioningRecordInvalid):
            api.parse_run_event(encoded(value | changes))


def test_duplicate_and_oversized_records_are_not_normalized() -> None:
    api = module()
    raw = encoded(intent_value())[:-1] + b',"recipe_digest":"' + b"c" * 64 + b'"}'
    for value in (raw, b" " * 16385, b"\xff", b"null"):
        with pytest.raises(api.CommissioningRecordInvalid):
            api.parse_run_intent(value)
