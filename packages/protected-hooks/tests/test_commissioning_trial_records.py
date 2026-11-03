"""Inert typed trial evidence codecs, @spec COMMISSIONING-SERIES-4."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
from typing import Any

import pytest


def encoded(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def uuid(number: int) -> str:
    return f"00000000-0000-4000-8000-{number:012x}"


def digest(number: int) -> str:
    return f"{number:064x}"


def campaign() -> dict[str, Any]:
    return {
        "campaign_id": uuid(2),
        "campaign_generation": "1",
        "permit_digest": digest(1),
        "bootstrap_digest": digest(2),
        "manifest_digest": digest(3),
        "runtime_id": uuid(3),
        "runtime_generation": "1",
        "broker_run_id": "a" * 40,
    }


def broker() -> dict[str, Any]:
    return {
        "instance_id": uuid(4),
        "endpoint": {"host": "broker.example.com", "port": 6379},
        "tls_server_name": "broker.example.com",
        "tls_spki_sha256": digest(4),
        "run_id": "a" * 40,
        "database": 0,
    }


def values() -> dict[str, dict[str, Any]]:
    common = {"schema_version": 1, "run_id": uuid(1), "ordinal": "1"}
    return {
        "trial_start": common
        | {
            "record_type": "commissioning_trial_start",
            "plan_digest": digest(5),
            "campaign": campaign(),
            "tuple_digest": digest(6),
            "challenge": uuid(5),
            "ownership_digest": digest(7),
            "charged_limits": {
                name: "1"
                for name in (
                    "deliveries",
                    "launches",
                    "model_calls",
                    "output_tokens",
                    "spend_microusd",
                )
            },
        },
        "trial_result": common
        | {
            "record_type": "commissioning_trial_result",
            "start_digest": digest(8),
            "terminal_state": "failed",
            "observations": [digest(9)],
            "cleanup_digest": digest(10),
            "transition_digest": digest(11),
        },
        "case_receipt": common
        | {
            "record_type": "commissioning_case_receipt",
            "start_digest": digest(8),
            "case": "exhaust-deliveries",
            "challenge": uuid(5),
            "tuple_digest": digest(6),
            "outcome": "pass",
            "model": "none",
            "producer": digest(12),
            "evidence_digests": [digest(13)],
        },
        "trial_transition": common
        | {
            "record_type": "commissioning_transition",
            "trial_tuple_digest": digest(6),
            "ownership_digest": digest(7),
            "broker_before": broker(),
            "broker_after": broker(),
            "derived_inputs_digest": digest(14),
            "acl_state_digest": digest(15),
        },
        "run_dossier": {
            "schema_version": 1,
            "record_type": "commissioning_run_dossier",
            "run_id": uuid(1),
            "plan_digest": digest(5),
            "event_head_digest": digest(16),
            "trial_result_digests": [digest(100 + i) for i in range(10)],
            "final_campaign": campaign(),
            "final_tuple_digest": digest(17),
            "final_observation_digests": [digest(18)],
        },
    }


def module() -> Any:
    name = "curie_protected_hooks.commissioning_trial_records"
    assert importlib.util.find_spec(name) is not None, "typed trial codecs not implemented"
    return importlib.import_module(name)


def parse(kind: str, value: Any) -> Any:
    return getattr(module(), "parse_" + kind)(encoded(value))


def paths(value: Any, prefix: tuple[Any, ...] = ()):
    if isinstance(value, dict):
        yield prefix, value
        for key, child in value.items():
            yield from paths(child, prefix + (key,))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from paths(child, prefix + (index,))


def at(value: Any, path: tuple[Any, ...]) -> Any:
    for key in path:
        value = value[key]
    return value


@pytest.mark.parametrize("kind", list(values()))
def test_closed_canonical_values_and_direct_construction_remain_inert(kind):
    original = values()[kind]
    parsed = parse(kind, original)
    assert parsed.canonical_bytes == encoded(original)
    assert parsed.digest == hashlib.sha256(encoded(original)).hexdigest()
    reordered = json.dumps(original, indent=2).encode()
    assert getattr(module(), "parse_" + kind)(reordered) == parsed
    assert type(parsed)(reordered) == parsed
    assert not hasattr(parsed, "accepted")
    assert not hasattr(parsed, "publish")
    for path, _ in paths(original):
        fresh = parsed.as_dict()
        at(fresh, path)["unexpected"] = "changed"
        assert parsed.as_dict() == original
    with pytest.raises((AttributeError, TypeError)):
        parsed._canonical_bytes = b"{}"
    assert uuid(1) not in repr(parsed)


@pytest.mark.parametrize("kind", list(values()))
def test_every_nested_member_is_required_and_closed(kind):
    original = values()[kind]
    for path, row in paths(original):
        for key in row:
            changed = copy.deepcopy(original)
            del at(changed, path)[key]
            with pytest.raises(module().CommissioningRecordInvalid):
                parse(kind, changed)
        changed = copy.deepcopy(original)
        at(changed, path)["unexpected"] = "untrusted"
        with pytest.raises(module().CommissioningRecordInvalid):
            parse(kind, changed)


@pytest.mark.parametrize("kind", list(values()))
def test_recursive_duplicate_numeric_and_oversize_inputs_refuse(kind):
    original = values()[kind]
    parser = getattr(module(), "parse_" + kind)
    raw = encoded(original)
    malformed = [
        raw[:-1] + b',"run_id":"' + uuid(1).encode() + b'"}',
        raw.replace(b'"schema_version":1', b'"schema_version":true'),
        raw.replace(b'"schema_version":1', b'"schema_version":1.0'),
        raw.replace(b'"schema_version":1', b'"schema_version":1e0'),
        raw.replace(b'"schema_version":1', b'"schema_version":NaN'),
        raw + b" " * 16385,
        b"[" * 1500 + b"]" * 1500,
        b"\xff",
        b"{}",
        b"[]",
        b"null",
        b"",
    ]
    if "campaign" in original:
        malformed.append(
            raw.replace(
                b'"campaign_generation":"1"', b'"campaign_generation":"1","campaign_generation":"1"'
            )
        )
    for payload in malformed:
        with pytest.raises(module().CommissioningRecordInvalid):
            parser(payload)


@pytest.mark.parametrize("kind", list(values()))
def test_cross_kind_substitution_refuses(kind):
    for other, value in values().items():
        if other != kind:
            with pytest.raises(module().CommissioningRecordInvalid):
                parse(kind, value)


@pytest.mark.parametrize(
    "kind", ["trial_start", "trial_result", "case_receipt", "trial_transition"]
)
@pytest.mark.parametrize("ordinal", ["0", "11", "01", 1, True, "-1"])
def test_trial_ordinals_are_the_exact_ten_trial_domain(kind, ordinal):
    value = values()[kind]
    value["ordinal"] = ordinal
    with pytest.raises(module().CommissioningRecordInvalid):
        parse(kind, value)


@pytest.mark.parametrize(
    "kind,field",
    [
        ("trial_result", "observations"),
        ("case_receipt", "evidence_digests"),
        ("run_dossier", "final_observation_digests"),
    ],
)
def test_digest_arrays_are_bounded_distinct_and_preserve_observation_order(kind, field):
    value = values()[kind]
    for invalid in (
        [],
        [digest(1), digest(1)],
        [digest(i) for i in range(257)],
        ["z" * 64],
        [True],
    ):
        value[field] = invalid
        with pytest.raises(module().CommissioningRecordInvalid):
            parse(kind, value)
    value[field] = [digest(9), digest(2)]
    assert parse(kind, value).as_dict()[field] == value[field]
    value[field] = [digest(i) for i in range(256)]
    # A bounded array may still exceed the independent 16 KiB record limit.
    with pytest.raises(module().CommissioningRecordInvalid):
        parse(kind, value)


def test_dossier_requires_exactly_ten_distinct_ordered_results():
    value = values()["run_dossier"]
    for count in (0, 1, 9, 11, 256):
        value["trial_result_digests"] = [digest(100 + i) for i in range(count)]
        with pytest.raises(module().CommissioningRecordInvalid):
            parse("run_dossier", value)
    value["trial_result_digests"] = [digest(100)] * 10
    with pytest.raises(module().CommissioningRecordInvalid):
        parse("run_dossier", value)


@pytest.mark.parametrize(
    "kind,field,invalid",
    [
        ("trial_result", "terminal_state", "running"),
        ("case_receipt", "outcome", "fail"),
        ("case_receipt", "outcome", True),
        ("case_receipt", "model", "fake"),
        ("case_receipt", "case", "invented-case"),
    ],
)
def test_no_abort_report_fake_model_or_unknown_case_enters_claimed_receipt(kind, field, invalid):
    value = values()[kind]
    value[field] = invalid
    with pytest.raises(module().CommissioningRecordInvalid):
        parse(kind, value)


@pytest.mark.parametrize("state", ["closed", "failed"])
def test_both_terminal_journal_labels_are_inert_values(state):
    value = values()["trial_result"]
    value["terminal_state"] = state
    assert parse("trial_result", value).as_dict() == value


@pytest.mark.parametrize("model", ["real", "none"])
def test_model_label_is_an_inert_claim_not_a_model_invocation(model):
    value = values()["case_receipt"]
    value["model"] = model
    assert parse("case_receipt", value).as_dict() == value


def test_dossier_preserves_predeclared_result_order_without_hash_sorting_or_verdict():
    value = values()["run_dossier"]
    value["trial_result_digests"].reverse()
    result = parse("run_dossier", value)
    assert result.as_dict() == value
    assert not hasattr(result, "accepted")
