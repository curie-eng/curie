"""Cross-family executable parser boundaries, @spec COMMISSIONING-SERIES-4."""

from __future__ import annotations

import copy
import importlib
from typing import Any

import pytest
from curie_protected_hooks.authority_records import (
    parse_manifest,
    parse_qualification,
    parse_readiness,
)
from curie_protected_hooks.commissioning_encoding import CommissioningRecordInvalid
from curie_protected_hooks.commissioning_run_records import (
    parse_run_event,
    parse_run_intent,
    parse_run_plan,
)

from .test_commissioning_run_records import event_value, intent_value, plan_value
from .test_commissioning_trial_evidence_records import values as target_values
from .test_commissioning_trial_records import at, encoded, paths
from .test_commissioning_trial_records import values as trial_values

CASES = [("commissioning_trial_records", k, v) for k, v in trial_values().items()] + [
    ("commissioning_trial_evidence_records", k, v) for k, v in target_values().items()
]


def parser(module: str, kind: str) -> Any:
    return getattr(importlib.import_module("curie_protected_hooks." + module), "parse_" + kind)


def leaves(value: Any, prefix: tuple[Any, ...] = ()):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from leaves(child, prefix + (key,))
    elif isinstance(value, list):
        for key, child in enumerate(value):
            yield from leaves(child, prefix + (key,))
    else:
        yield prefix, value


@pytest.mark.parametrize("module,kind,value", CASES, ids=[c[1] for c in CASES])
def test_all_fourteen_families_refuse_each_other_and_existing_authority_formats(
    module, kind, value
):
    selected = parser(module, kind)
    for other_module, other_kind, other in CASES:
        if (other_module, other_kind) != (module, kind):
            with pytest.raises(CommissioningRecordInvalid):
                selected(encoded(other))
    for old in (intent_value(), plan_value(), event_value()):
        with pytest.raises(CommissioningRecordInvalid):
            selected(encoded(old))
    for old_parser in (
        parse_run_event,
        parse_run_intent,
        parse_run_plan,
        parse_manifest,
        parse_qualification,
        parse_readiness,
    ):
        with pytest.raises(ValueError):
            old_parser(encoded(value))


@pytest.mark.parametrize("module,kind,value", CASES, ids=[c[1] for c in CASES])
def test_duplicate_members_at_every_object_depth_refuse(module, kind, value):
    selected = parser(module, kind)
    for _, row in paths(value):
        key = next(iter(row))
        original = encoded(row)
        duplicate = original[:-1] + b"," + encoded(key) + b":" + encoded(row[key]) + b"}"
        payload = encoded(value).replace(original, duplicate, 1)
        with pytest.raises(CommissioningRecordInvalid):
            selected(payload)


@pytest.mark.parametrize("module,kind,value", CASES, ids=[c[1] for c in CASES])
def test_scalar_shapes_never_coerce_at_any_depth(module, kind, value):
    selected = parser(module, kind)
    for path, original in leaves(value):
        changed = copy.deepcopy(value)
        at(changed, path[:-1])[path[-1]] = 1 if type(original) is bool else True
        with pytest.raises(CommissioningRecordInvalid):
            selected(encoded(changed))


@pytest.mark.parametrize("module,kind,value", CASES, ids=[c[1] for c in CASES])
def test_malformed_input_errors_detach_raw_decoder_context(module, kind, value):
    with pytest.raises(CommissioningRecordInvalid) as caught:
        parser(module, kind)(b'{"invalid-private-input":')
    assert str(caught.value) == "Invalid protected commissioning record"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
