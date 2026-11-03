"""Closed inert digest targets, @spec COMMISSIONING-SERIES-4."""

from __future__ import annotations

import copy
import hashlib
import importlib
import importlib.util
import json
from typing import Any

import pytest

from .test_commissioning_trial_records import at, broker, campaign, digest, encoded, paths, uuid

ROLES = (
    "candidate_enqueue",
    "candidate_worker",
    "control",
    "control_reader",
    "enqueue",
    "evidence_observer",
    "issuer",
    "observer",
    "qualification_publisher",
    "source_writer",
    "verifier",
    "worker",
)


def values() -> dict[str, dict[str, Any]]:
    common = {"schema_version": 1, "run_id": uuid(1), "ordinal": "1"}
    session = {
        "intent_id": uuid(5),
        "kind": "sdk",
        "owner_digest": digest(1),
        "launch_id": uuid(6),
    }
    return {
        "image_set": {
            "schema_version": 1,
            "record_type": "commissioning_image_set",
            "images": [
                {
                    "role": "runner",
                    "image_digest": "sha256:" + digest(1),
                    "platform": "linux/amd64",
                    "image_id": "containerd://sha256:" + digest(2),
                }
            ],
        },
        "ownership_snapshot": {
            "schema_version": 1,
            "record_type": "commissioning_ownership_snapshot",
            "owner": "curie-commission-000000000001",
            "entries": [
                {
                    "kind": "kubernetes",
                    "key": "acme-runtime/pods/acme-probe",
                    "state": "created",
                    "identity": {"uid": uuid(7)},
                }
            ],
        },
        "trial_cleanup": common
        | {
            "record_type": "commissioning_cleanup",
            "ownership_digest": digest(2),
            "session_roster_digest": digest(3),
            "resource_checks": [
                {"subject": digest(1), "container_terminated": True, "absent": True}
            ],
            "session_checks": [
                {
                    "intent_id": uuid(5),
                    "binding_digest": digest(4),
                    "join_observed": True,
                    "transport_closed": True,
                }
            ],
        },
        "session_roster": common
        | {
            "record_type": "commissioning_session_roster",
            "sessions": [session | {"binding_digest": digest(4)}],
        },
        "session_binding": common
        | {
            "record_type": "commissioning_session_binding",
            **session,
            "identity": {
                "session_id": "sdk-session-1",
                "process": {
                    "container_id": "containerd://" + digest(10),
                    "pid": "42",
                    "start_ticks": "900",
                },
            },
        },
        "derived_inputs": {
            "schema_version": 1,
            "record_type": "commissioning_derived_inputs",
            "manifest_digest": digest(1),
            "measurement_digest": digest(2),
            "selection_digest": digest(3),
            "runtime_inputs": [{"role": "bootstrap", "content_digest": digest(4)}],
        },
        "acl_snapshot": {
            "schema_version": 1,
            "record_type": "commissioning_acl_snapshot",
            "bootstrap_digest": digest(1),
            "guard_digest": digest(2),
            "campaign": campaign(),
            "principals": [
                {
                    "role": role,
                    "credential": {"id": "acme-" + role, "generation": "1"},
                    "rules_digest": digest(10 + i),
                    "password_set_digest": digest(30 + i),
                    "enabled": role not in {"enqueue", "worker", "verifier"},
                }
                for i, role in enumerate(ROLES)
            ],
        },
        "producer_binding": {
            "schema_version": 1,
            "record_type": "commissioning_producer_binding",
            "case": "clean-session",
            "image_set_digest": digest(1),
            "control_profile_digest": digest(2),
            "projection_digest": digest(3),
            "execution_config_digest": digest(4),
            "subcases": ["original-session", "scratch-isolation"],
        },
        "case_evidence": common
        | {
            "record_type": "commissioning_case_evidence",
            "case": "clean-session",
            "challenge": uuid(10),
            "tuple_digest": digest(5),
            "checks": [
                {
                    "subcase": "scratch-isolation",
                    "subject_digest": digest(1),
                    "expected_digest": digest(2),
                    "observed_digest": digest(3),
                    "outcome": "pass",
                }
            ],
        },
    }


def module() -> Any:
    name = "curie_protected_hooks.commissioning_trial_evidence_records"
    assert importlib.util.find_spec(name) is not None, "typed digest targets not implemented"
    return importlib.import_module(name)


def parse(kind: str, value: Any) -> Any:
    return getattr(module(), "parse_" + kind)(encoded(value))


@pytest.mark.parametrize("kind", list(values()))
def test_target_canonical_bytes_and_nested_immutability_supply_no_proof(kind):
    value = values()[kind]
    result = parse(kind, value)
    assert result.canonical_bytes == encoded(value)
    assert result.digest == hashlib.sha256(encoded(value)).hexdigest()
    assert type(result)(json.dumps(value, indent=2).encode()) == result
    for path, _ in paths(value):
        fresh = result.as_dict()
        at(fresh, path).clear()
        assert result.as_dict() == value
    assert not hasattr(result, "accepted") and not hasattr(result, "publish")
    with pytest.raises((AttributeError, TypeError)):
        result.extra = "authority"


@pytest.mark.parametrize("kind", list(values()))
def test_every_target_member_is_required_and_unknown_members_refuse(kind):
    value = values()[kind]
    for path, row in paths(value):
        for key in row:
            changed = copy.deepcopy(value)
            del at(changed, path)[key]
            # OwnershipLedger identities deliberately have its original open
            # nonempty string-map grammar; keys inside it are not prescribed.
            if path and path[-1] == "identity" and kind == "ownership_snapshot" and len(row) > 1:
                continue
            with pytest.raises(module().CommissioningRecordInvalid):
                parse(kind, changed)
        if path and path[-1] == "identity" and kind == "ownership_snapshot":
            continue
        changed = copy.deepcopy(value)
        at(changed, path)["unexpected"] = "untrusted"
        with pytest.raises(module().CommissioningRecordInvalid):
            parse(kind, changed)


@pytest.mark.parametrize("kind", list(values()))
def test_digest_target_cross_kind_substitution_and_recursive_duplicates_refuse(kind):
    for other, value in values().items():
        if other != kind:
            with pytest.raises(module().CommissioningRecordInvalid):
                parse(kind, value)
    raw = encoded(values()[kind])
    for invalid in (
        raw[:-1] + b',"schema_version":1}',
        raw.replace(b'"schema_version":1', b'"schema_version":true'),
        raw.replace(b'"schema_version":1', b'"schema_version":1e0'),
        raw + b" " * 16385,
        b"\xff",
        b"[]",
        b"null",
    ):
        with pytest.raises(module().CommissioningRecordInvalid):
            getattr(module(), "parse_" + kind)(invalid)


@pytest.mark.parametrize(
    "kind,field",
    [
        ("image_set", "images"),
        ("ownership_snapshot", "entries"),
        ("trial_cleanup", "resource_checks"),
        ("trial_cleanup", "session_checks"),
        ("session_roster", "sessions"),
        ("derived_inputs", "runtime_inputs"),
        ("acl_snapshot", "principals"),
        ("producer_binding", "subcases"),
        ("case_evidence", "checks"),
    ],
)
def test_sorted_rows_and_nonempty_finite_inventories_are_closed(kind, field):
    value = values()[kind]
    for rows in ([value[field][0]] * 2, [value[field][0]] * 257):
        value[field] = rows
        with pytest.raises(module().CommissioningRecordInvalid):
            parse(kind, value)
    value[field] = []
    if kind == "ownership_snapshot":
        assert parse(kind, value).as_dict()[field] == []
    else:
        with pytest.raises(module().CommissioningRecordInvalid):
            parse(kind, value)


@pytest.mark.parametrize(
    "field", ["absent", "container_terminated", "join_observed", "transport_closed"]
)
@pytest.mark.parametrize("bad", [False, 0, 1, "true", [], {}])
def test_passing_cleanup_never_coerces_missing_join_or_absence(field, bad):
    value = values()["trial_cleanup"]
    target = "resource_checks" if field in {"absent", "container_terminated"} else "session_checks"
    value[target][0][field] = bad
    with pytest.raises(module().CommissioningRecordInvalid):
        parse("trial_cleanup", value)


def test_nonprocess_cleanup_uses_null_only_for_container_termination():
    value = values()["trial_cleanup"]
    value["resource_checks"][0]["container_terminated"] = None
    assert parse("trial_cleanup", value).as_dict() == value
    value["resource_checks"][0]["absent"] = None
    with pytest.raises(module().CommissioningRecordInvalid):
        parse("trial_cleanup", value)


@pytest.mark.parametrize("kind", ["sdk", "runner", "broker"])
def test_session_identity_is_tagged_and_original_container_is_consistent(kind):
    value = values()["session_binding"]
    process = value["identity"]["process"]
    identities = {
        "sdk": {"session_id": "sdk-session-1", "process": process},
        "runner": {"container_id": process["container_id"], "process": process},
        "broker": {
            "client_id": "7",
            "broker_identity": broker(),
            "credential": {"id": "acme-control", "generation": "1"},
        },
    }
    value["kind"] = kind
    value["identity"] = identities[kind]
    if kind == "broker":
        value["launch_id"] = None
    assert parse("session_binding", value).as_dict() == value
    for other, identity in identities.items():
        if other != kind:
            value["identity"] = identity
            with pytest.raises(module().CommissioningRecordInvalid):
                parse("session_binding", value)
    value["identity"] = identities[kind]
    if kind == "runner":
        value["identity"]["container_id"] = "different-container"
        with pytest.raises(module().CommissioningRecordInvalid):
            parse("session_binding", value)
    if kind != "broker":
        value["launch_id"] = None
        with pytest.raises(module().CommissioningRecordInvalid):
            parse("session_binding", value)


@pytest.mark.parametrize("kind", ["session_roster", "session_binding"])
def test_unbound_or_unknown_sessions_never_form_a_bound_roster(kind):
    value = values()[kind]
    row = value["sessions"][0] if kind == "session_roster" else value
    row["kind"] = "operator"
    with pytest.raises(module().CommissioningRecordInvalid):
        parse(kind, value)


def test_acl_snapshot_covers_exact_g_v2_roles_with_distinct_credentials_and_strict_flags():
    value = values()["acl_snapshot"]
    for change in ("missing", "extra", "duplicate-credential", "bool-coercion"):
        changed = copy.deepcopy(value)
        if change == "missing":
            changed["principals"].pop()
        elif change == "extra":
            row = copy.deepcopy(changed["principals"][-1])
            row["role"] = "z-extra"
            changed["principals"].append(row)
        elif change == "duplicate-credential":
            changed["principals"][1]["credential"] = changed["principals"][0]["credential"]
        else:
            changed["principals"][0]["enabled"] = 1
        with pytest.raises(module().CommissioningRecordInvalid):
            parse("acl_snapshot", changed)


def test_case_predicate_may_require_different_digests_but_no_failed_check():
    value = values()["case_evidence"]
    assert parse("case_evidence", value).as_dict() == value
    value["checks"][0]["outcome"] = "fail"
    with pytest.raises(module().CommissioningRecordInvalid):
        parse("case_evidence", value)


@pytest.mark.parametrize(
    "field,invalid",
    [
        ("platform", "linux"),
        ("platform", "linux//v8"),
        ("platform", "LINUX/amd64"),
        ("image_digest", "example:latest"),
        ("image_id", "contains whitespace"),
    ],
)
def test_image_syntax_preserves_existing_artifact_platform_contract(field, invalid):
    value = values()["image_set"]
    value["images"][0][field] = invalid
    with pytest.raises(module().CommissioningRecordInvalid):
        parse("image_set", value)


def test_image_id_uses_existing_artifact_printable_identity_not_opaque_ref():
    value = values()["image_set"]
    value["images"][0]["image_id"] = "runtime-image=" + "a" * 1000
    assert parse("image_set", value).as_dict() == value
    value["images"][0]["image_id"] = "a" * 2049
    with pytest.raises(module().CommissioningRecordInvalid):
        parse("image_set", value)


@pytest.mark.parametrize("kind", list(values()))
def test_nonbytes_direct_construction_and_parser_inputs_refuse(kind):
    parser = getattr(module(), "parse_" + kind)
    record_type = type(parse(kind, values()[kind]))
    for invalid in (
        None,
        encoded(values()[kind]).decode(),
        bytearray(encoded(values()[kind])),
        True,
    ):
        with pytest.raises(module().CommissioningRecordInvalid):
            parser(invalid)
        with pytest.raises(module().CommissioningRecordInvalid):
            record_type(invalid)


def test_each_image_role_retains_distinct_observed_platforms_in_sorted_order():
    value = values()["image_set"]
    second = copy.deepcopy(value["images"][0])
    second.update(platform="linux/arm64/v8", image_id="runtime-image=" + digest(9))
    value["images"].append(second)
    assert parse("image_set", value).as_dict() == value
    value["images"].reverse()
    with pytest.raises(module().CommissioningRecordInvalid):
        parse("image_set", value)


@pytest.mark.parametrize(
    "kind,field,key,new_key",
    [
        ("ownership_snapshot", "entries", "key", "z-owned-resource"),
        ("trial_cleanup", "resource_checks", "subject", digest(2)),
        ("trial_cleanup", "session_checks", "intent_id", uuid(6)),
        ("session_roster", "sessions", "intent_id", uuid(6)),
        ("derived_inputs", "runtime_inputs", "role", "z-input"),
        ("case_evidence", "checks", "subject_digest", digest(2)),
    ],
)
def test_distinct_sorted_rows_pass_but_reverse_order_refuses(kind, field, key, new_key):
    value = values()[kind]
    second = copy.deepcopy(value[field][0])
    second[key] = new_key
    value[field].append(second)
    assert parse(kind, value).as_dict() == value
    value[field].reverse()
    with pytest.raises(module().CommissioningRecordInvalid):
        parse(kind, value)


@pytest.mark.parametrize(
    "kind,field", [("acl_snapshot", "principals"), ("producer_binding", "subcases")]
)
def test_complete_existing_inventory_still_requires_sorted_order(kind, field):
    value = values()[kind]
    assert parse(kind, value).as_dict() == value
    value[field].reverse()
    with pytest.raises(module().CommissioningRecordInvalid):
        parse(kind, value)
