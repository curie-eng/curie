"""Closed operator custody records; no verdicts, @spec COMMISSIONING-SERIES-8."""

from __future__ import annotations

import copy
import importlib
import importlib.util
import json
from typing import Any

import pytest

from .test_commissioning_bootstrap import encoded

ROLES = (
    "administrator",
    "evidence-observer",
    "acl-observer",
    "acceptance-publisher",
    "qualification-publisher",
    "activation-publisher",
)


def api() -> Any:
    name = "curie_protected_hooks.commissioning_operator_records"
    assert importlib.util.find_spec(name) is not None, "versioned operator custody not implemented"
    return importlib.import_module(name)


def recipe_value() -> dict[str, Any]:
    return {
        "schema_version": 2,
        "record_type": "commissioning_operator_recipe",
        "provisioning_recipe_digest": "a" * 64,
        "roles": [
            {
                "role": role,
                "credential": {"id": "example-" + role, "generation": "1"},
                "max_connections": "2",
            }
            for role in sorted(ROLES)
        ],
    }


def event_value() -> dict[str, Any]:
    return {
        "schema_version": 2,
        "record_type": "commissioning_operator_event",
        "run_id": "11111111-1111-4111-8111-111111111111",
        "revision": "1",
        "previous_digest": "0" * 64,
        "elapsed_ms": "0",
        "kind": "intended",
        "intent_id": "22222222-2222-4222-8222-222222222222",
        "role": "evidence-observer",
        "phase": "setup",
        "ordinal": "0",
        "binding_digest": None,
        "identity": None,
    }


def test_closed_v2_recipe_is_immutable_and_canonical() -> None:
    module = api()
    value = recipe_value()
    record = module.parse_operator_recipe(encoded(value))
    assert record.as_dict() == value
    changed = record.as_dict()
    changed["roles"].clear()
    assert record.as_dict() == value
    assert json.loads(record.canonical_bytes) == value
    assert "example-" not in repr(record)


@pytest.mark.parametrize(
    "attack",
    ["version", "unknown", "missing-role", "duplicate", "order", "unbounded", "shared-credential"],
)
def test_recipe_refuses_ambiguous_or_unbounded_inventory(attack: str) -> None:
    module = api()
    value = recipe_value()
    if attack == "version":
        value["schema_version"] = 1
    elif attack == "unknown":
        value["roles"][0]["phase"] = "trial"
    elif attack == "missing-role":
        value["roles"].pop()
    elif attack == "duplicate":
        value["roles"][1] = copy.deepcopy(value["roles"][0])
    elif attack == "order":
        value["roles"].reverse()
    elif attack == "unbounded":
        value["roles"][0]["max_connections"] = "0"
    else:
        value["roles"][1]["credential"] = copy.deepcopy(value["roles"][0]["credential"])
    with pytest.raises(module.CommissioningRecordInvalid):
        module.parse_operator_recipe(encoded(value))


def test_intent_is_not_a_bound_identity_or_a_close_witness() -> None:
    module = api()
    value = event_value()
    assert module.parse_operator_event(encoded(value)).as_dict() == value
    for kind in ("bound", "closed"):
        value["kind"] = kind
        with pytest.raises(module.CommissioningRecordInvalid):
            module.parse_operator_event(encoded(value))


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 1),
        ("kind", "accepted"),
        ("phase", "anything"),
        ("role", "candidate"),
        ("ordinal", "11"),
        ("elapsed_ms", True),
    ],
)
def test_event_refuses_unknown_authority_or_wrong_scalars(field: str, value: Any) -> None:
    module = api()
    row = event_value()
    row[field] = value
    with pytest.raises(module.CommissioningRecordInvalid):
        module.parse_operator_event(encoded(row))


def test_no_transition_after_the_final_trial() -> None:
    module = api()
    value = event_value()
    value.update(phase="transition", ordinal="10")
    with pytest.raises(module.CommissioningRecordInvalid):
        module.parse_operator_event(encoded(value))
