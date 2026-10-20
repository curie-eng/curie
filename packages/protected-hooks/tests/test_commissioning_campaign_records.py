"""Closed commissioning campaign records, @spec PROTECTED-COMMISSIONING-7.

Decoded records are inert: they authenticate nothing and authorize no work.
"""

import copy
import hashlib
import itertools
import json
from typing import Any

import pytest
from curie_protected_hooks import commissioning_campaign_records as r
from curie_protected_hooks.admission_records import (
    parse_envelope,
    parse_intent,
    parse_receipt,
    parse_selection,
    parse_state,
)
from curie_protected_hooks.artifact_measurement import parse_artifact_measurement
from curie_protected_hooks.authority_records import (
    parse_manifest,
    parse_qualification,
    parse_readiness,
)
from curie_protected_hooks.worker_artifact_selection import SelectedArtifact

from .test_admission_records import examples as admission_examples

CAMPAIGN_ID = "11111111-1111-4111-8111-111111111111"
RUNTIME_ID = "22222222-2222-4222-8222-222222222222"
UUID_A = "33333333-3333-4333-8333-333333333333"
UUID_B = "44444444-4444-4444-8444-444444444444"
UUID_C = "55555555-5555-4555-8555-555555555555"
POD_UID = "66666666-6666-4666-8666-666666666666"
QUALIFICATION_ID = "77777777-7777-4777-8777-777777777777"
GUARD = dict(control_id=UUID_C, revision="3", config_sha256="9" * 64)

C = dict(
    campaign_id=CAMPAIGN_ID,
    campaign_generation="2",
    permit_digest="a" * 64,
    bootstrap_digest="b" * 64,
    manifest_digest="c" * 64,
    runtime_id=RUNTIME_ID,
    runtime_generation="1",
    broker_run_id="d" * 40,
)
D = dict(
    delivery_id=UUID_A,
    attempt_id=UUID_B,
    reservation_id=UUID_C,
    request_digest="e" * 64,
    payload_digest="f" * 64,
    payload_bytes="512",
    source=dict(source_id=UUID_A, generation="1", policy_fingerprint="1" * 64),
    target_ref="fixture:read-only/deployment",
)
UNBOUND = dict(
    kind="unbound", namespace="campaign-1", pod_name="runner-1", create_operation_id=UUID_B
)
BOUND = UNBOUND | dict(kind="bound", pod_uid=POD_UID)

KINDS = (
    "commissioning_envelope",
    "commissioning_admission",
    "commissioning_launch_request",
    "commissioning_launch_ownership",
    "commissioning_acceptance",
)
PARSERS = {
    "commissioning_envelope": r.parse_campaign_envelope,
    "commissioning_admission": r.parse_campaign_admission,
    "commissioning_launch_request": r.parse_campaign_launch_request,
    "commissioning_launch_ownership": r.parse_campaign_launch_ownership,
    "commissioning_acceptance": r.parse_commissioning_acceptance,
}
TYPES = {
    "commissioning_envelope": r.CampaignEnvelope,
    "commissioning_admission": r.CampaignAdmission,
    "commissioning_launch_request": r.CampaignLaunchRequest,
    "commissioning_launch_ownership": r.CampaignLaunchOwnership,
    "commissioning_acceptance": r.CommissioningAcceptance,
}

# Pinned sha256 of each example's canonical bytes; a grammar change moves these.
GOLDEN = {
    "CampaignEnvelope": "b7cb210623eb7e04380d7e20e33f407c411c5a9a713bf3f7fac10f3cba8785fc",
    "CampaignAdmission": "44c41e8cd02d3805d263e14295717babed1ab5e586047a783657894f3f5552b3",
    "CampaignLaunchRequest": "62a97528d100bf5c5c54bdc2a5769df98f96ac1bf3f126b946f4cddad1a4dc6d",
    "CampaignLaunchOwnership": "62df520045ade1fb2b1aeaf345f7ff1119897ebfebaed05ec4c0743c0b63f1c6",
    "CommissioningAcceptance": "53f8dbf5cb8a10c999295167f8b2c7ff0ea2be5ac81d470085d3213c04d189f0",
}


def value(kind: str = "commissioning_envelope", **changes: Any) -> dict[str, Any]:
    """A fresh valid example of one record kind."""
    base: dict[str, Any] = dict(schema_version=1, record_type=kind, campaign=C)
    if kind != "commissioning_acceptance":
        base["delivery"] = D
    extras: dict[str, dict[str, Any]] = {
        "commissioning_envelope": dict(tool_access="read-only"),
        "commissioning_admission": dict(
            envelope_digest="2" * 64,
            stream_entry_id="1700000000000-0",
            accepted_at_ms="1700000000001",
        ),
        "commissioning_launch_request": dict(
            admission_digest="3" * 64,
            launch_id=UUID_A,
            launch_reservation_id=UUID_B,
            projection_digest="4" * 64,
        ),
        "commissioning_launch_ownership": dict(
            launch_id=UUID_A,
            launch_request_digest="5" * 64,
            projection_digest="4" * 64,
            workload=UNBOUND,
            state="creating",
            revision="1",
        ),
        "commissioning_acceptance": dict(
            evidence_digest="6" * 64,
            qualification_id=QUALIFICATION_ID,
            qualification_generation="1",
            accepted_at_ms="1700000000002",
        ),
    }
    return copy.deepcopy(base | extras[kind] | changes)


def canonical(v: Any) -> bytes:
    return json.dumps(v, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def raw(kind: str = "commissioning_envelope", **changes: Any) -> bytes:
    return json.dumps(value(kind, **changes), indent=2).encode()


def refuses(kind: str, v: Any) -> None:
    data = v if type(v) is bytes else json.dumps(v).encode()
    with pytest.raises(r.CampaignRecordInvalid) as caught:
        PARSERS[kind](data)
    assert str(caught.value) == "Invalid commissioning campaign record"
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize("kind", KINDS)
def test_golden_canonical_bytes_and_identity(kind: str) -> None:
    record = PARSERS[kind](raw(kind))
    assert type(record) is TYPES[kind]
    assert record.canonical_bytes == canonical(value(kind))
    assert (
        record.digest
        == hashlib.sha256(canonical(value(kind))).hexdigest()
        == GOLDEN[TYPES[kind].__name__]
    )
    assert PARSERS[kind](record.canonical_bytes) == record
    assert repr(record) == TYPES[kind].__name__ + "()"
    assert not hasattr(record, "__dict__")


@pytest.mark.parametrize("kind", KINDS)
def test_nested_values_are_fresh_copies(kind: str) -> None:
    record = PARSERS[kind](raw(kind))
    first = record.as_dict()
    first["campaign"]["runtime_id"] = "changed"
    first.pop("record_type")
    assert record.as_dict() == value(kind)
    assert record.campaign == C
    record.campaign["runtime_id"] = "changed"
    assert record.campaign == C
    if kind != "commissioning_acceptance":
        record.delivery["source"]["generation"] = "9"
        assert record.delivery == D
    with pytest.raises(AttributeError):
        record._canonical_bytes = b"{}"  # type: ignore[misc]


@pytest.mark.parametrize("kind", KINDS)
def test_closed_member_sets_at_every_object(kind: str) -> None:
    v = value(kind)
    paths: list[tuple[str, ...]] = [(), ("campaign",)]
    if kind != "commissioning_acceptance":
        paths += [("delivery",), ("delivery", "source")]
    if kind == "commissioning_launch_ownership":
        paths.append(("workload",))
    for path in paths:
        target = v
        for step in path:
            target = target[step]
        for member in list(target):
            missing = copy.deepcopy(v)
            holder = missing
            for step in path:
                holder = holder[step]
            del holder[member]
            refuses(kind, missing)
        extra = copy.deepcopy(v)
        holder = extra
        for step in path:
            holder = holder[step]
        holder["unexpected"] = "1"
        refuses(kind, extra)


@pytest.mark.parametrize("kind", KINDS)
def test_cross_kind_and_unknown_record_types_refuse(kind: str) -> None:
    record = PARSERS[kind](raw(kind))
    for other in KINDS:
        if other != kind:
            refuses(other, record.canonical_bytes)
            refuses(other, value(kind, record_type=other))
    for record_type in (None, "", "commissioning_permit", "commissioning_launch_receipt", 1):
        refuses(kind, value(kind, record_type=record_type))
    with pytest.raises(r.CampaignRecordInvalid):
        TYPES[kind](b"{}")


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"[]",
        b"\xff",
        b'{"schema_version":1,"schema_version":1}',
        b"x" * 16385,
    ],
)
def test_encoding_refusals(kind: str, data: bytes) -> None:
    refuses(kind, data)


@pytest.mark.parametrize("kind", KINDS)
def test_lane2_scalar_refusals(kind: str) -> None:
    for version in (True, 2, "1", 1.0):
        refuses(kind, value(kind, schema_version=version))
    raw_bytes = canonical(value(kind))
    refuses(kind, raw_bytes.replace(b'"schema_version":1', b'"schema_version":1.0'))
    refuses(kind, raw_bytes.replace(b'"schema_version":1', b'"schema_version":NaN'))
    refuses(kind, b'{"campaign":' + canonical(C) + b"," + raw_bytes[1:])
    for member, bad in (
        ("campaign_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("campaign_id", "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"),
        ("campaign_generation", "0"),
        ("campaign_generation", "01"),
        ("permit_digest", "A" * 64),
        ("broker_run_id", "d" * 39),
        ("runtime_generation", 1),
    ):
        refuses(kind, value(kind, campaign=C | {member: bad}))


@pytest.mark.parametrize("kind", [k for k in KINDS if k != "commissioning_acceptance"])
def test_delivery_binding_bounds(kind: str) -> None:
    assert PARSERS[kind](raw(kind, delivery=D | {"payload_bytes": "32768"}))
    for bad in ("32769", "0", "01", "", 512, "9223372036854775808"):
        refuses(kind, value(kind, delivery=D | {"payload_bytes": bad}))
    for source in (
        D["source"] | {"generation": "0"},
        D["source"] | {"source_id": "x"},
        {"source_id": UUID_A, "generation": "1"},
        [D["source"]],
    ):
        refuses(kind, value(kind, delivery=D | {"source": source}))
    for target in ("", "-starts-with-dash", "x" * 257, None):
        refuses(kind, value(kind, delivery=D | {"target_ref": target}))


def test_envelope_tool_access_is_read_only() -> None:
    assert r.parse_campaign_envelope(raw()).tool_access == "read-only"
    for access in ("read-write", "Read-Only", "", None, ["read-only"], "operator"):
        refuses("commissioning_envelope", value(tool_access=access))


@pytest.mark.parametrize(
    "entry",
    ["0-0", "0-1", "1-0", "9007199254740991-9007199254740991", "1700000000000-12"],
)
def test_stream_entry_id_accepts(entry: str) -> None:
    record = r.parse_campaign_admission(raw("commissioning_admission", stream_entry_id=entry))
    assert record.stream_entry_id == entry


@pytest.mark.parametrize(
    "entry",
    [
        "01-0",
        "1-00",
        "9007199254740992-0",
        "0-9007199254740992",
        "1",
        "1-2-3",
        "1_0",
        "-1",
        "1-",
        " 1-0",
        "1-0\n",
        "１-0",
        "",
        10,
        None,
    ],
)
def test_stream_entry_id_refuses(entry: Any) -> None:
    refuses("commissioning_admission", value("commissioning_admission", stream_entry_id=entry))


@pytest.mark.parametrize("member", ["namespace", "pod_name"])
def test_workload_dns_labels(member: str) -> None:
    kind = "commissioning_launch_ownership"
    for good in ("a", "a" * 63, "0-a", "x1"):
        assert PARSERS[kind](raw(kind, workload=UNBOUND | {member: good}))
    for bad in ("", "a" * 64, "-a", "a-", "A", "a.b", "a_b", "é", None, 1):
        refuses(kind, value(kind, workload=UNBOUND | {member: bad}))


def test_workload_is_tagged_and_closed() -> None:
    kind = "commissioning_launch_ownership"
    refuses(kind, value(kind, workload=UNBOUND | {"pod_uid": POD_UID}))
    refuses(kind, value(kind, workload={k: v for k, v in BOUND.items() if k != "pod_uid"}))
    refuses(kind, value(kind, workload=UNBOUND | {"kind": "other"}))
    refuses(kind, value(kind, state="bound", workload=BOUND | {"pod_uid": "not-a-uid"}))
    refuses(kind, value(kind, workload=UNBOUND | {"create_operation_id": "x"}))
    refuses(kind, value(kind, workload=None))


def test_ownership_state_and_workload_consistency() -> None:
    kind = "commissioning_launch_ownership"
    for state in ("creating", "bound", "retiring", "retired", "uncertain"):
        workloads = {
            "creating": (UNBOUND,),
            "bound": (BOUND,),
            "retired": (BOUND,),
            "retiring": (UNBOUND, BOUND),
            "uncertain": (UNBOUND, BOUND),
        }[state]
        for workload in (UNBOUND, BOUND):
            if workload in workloads:
                assert r.parse_campaign_launch_ownership(raw(kind, state=state, workload=workload))
            else:
                refuses(kind, value(kind, state=state, workload=workload))
    for state in ("unknown", "Bound", "", None, "closed"):
        refuses(kind, value(kind, state=state))
    for revision in ("0", "01", 1):
        refuses(kind, value(kind, revision=revision))


def test_acceptance_is_not_a_qualification_link_for_campaign_id() -> None:
    kind = "commissioning_acceptance"
    refuses(kind, value(kind, qualification_id=CAMPAIGN_ID))
    refuses(kind, value(kind, qualification_id="x"))
    refuses(kind, value(kind, qualification_generation="0"))
    assert "delivery" not in r.parse_commissioning_acceptance(raw(kind)).as_dict()
    refuses(kind, value(kind, delivery=D))


@pytest.mark.parametrize("kind", KINDS)
def test_commissioning_bytes_refuse_every_protected_hooks_production_codec(kind: str) -> None:
    data = PARSERS[kind](raw(kind)).canonical_bytes
    for parse in (
        parse_envelope,
        parse_intent,
        parse_state,
        parse_receipt,
        parse_selection,
        parse_manifest,
        parse_qualification,
        parse_readiness,
        parse_artifact_measurement,
    ):
        with pytest.raises(ValueError):
            parse(data)
    with pytest.raises(ValueError):
        SelectedArtifact(PARSERS[kind](data), PARSERS[kind](data))  # type: ignore[arg-type]


def test_production_admission_bytes_refuse_every_commissioning_parser() -> None:
    productions = admission_examples()
    assert {"envelope", "intent", "receipt"} <= productions.keys()
    for production in productions.values():
        for kind in KINDS:
            refuses(kind, canonical(production))
            refuses(kind, production | {"record_type": kind})


# Ownership transitions: (current state, current bound?) x (proposed state, proposed bound?).
STATES = ("creating", "bound", "retiring", "retired", "uncertain")
ALLOWED = {
    ("creating", "bound"),
    ("creating", "retiring"),
    ("creating", "retired"),
    ("creating", "uncertain"),
    ("bound", "retiring"),
    ("bound", "uncertain"),
    ("retiring", "retired"),
    ("retiring", "uncertain"),
    ("uncertain", "bound"),
    ("uncertain", "retiring"),
    ("uncertain", "retired"),
}
SHAPES = {
    "creating": (False,),
    "bound": (True,),
    "retired": (True,),
    "retiring": (False, True),
    "uncertain": (False, True),
}


def ownership(state: str, bound: bool, revision: int, **changes: Any) -> r.CampaignLaunchOwnership:
    fields: dict[str, Any] = dict(
        state=state, workload=BOUND if bound else UNBOUND, revision=str(revision)
    )
    return r.parse_campaign_launch_ownership(
        raw("commissioning_launch_ownership", **(fields | changes))
    )


def evidence(**changes: Any) -> r.UnambiguousNotFound:
    fields: dict[str, Any] = dict(
        namespace=UNBOUND["namespace"],
        pod_name=UNBOUND["pod_name"],
        pod_uid=POD_UID,
        guard_identity=GUARD,
    )
    return r.UnambiguousNotFound(**(fields | changes))


@pytest.mark.parametrize(
    "current,proposed",
    [
        ((cs, cb), (ps, pb))
        for cs, ps in itertools.product(STATES, STATES)
        for cb in SHAPES[cs]
        for pb in SHAPES[ps]
    ],
)
def test_ownership_transition_table_is_exhaustive(
    current: tuple[str, bool], proposed: tuple[str, bool]
) -> None:
    before = ownership(current[0], current[1], 4)
    after = ownership(proposed[0], proposed[1], 5)
    expected = (current[0], proposed[0]) in ALLOWED and not (current[1] and not proposed[1])
    needs_evidence = proposed[0] == "retired" and not current[1]
    if expected and needs_evidence:
        with pytest.raises(r.CampaignRecordInvalid):
            r.next_ownership(before, after, trusted_guard_identity=GUARD)
        assert (
            r.next_ownership(before, after, not_found=evidence(), trusted_guard_identity=GUARD)
            is after
        )
    elif expected:
        assert r.next_ownership(before, after, trusted_guard_identity=GUARD) is after
    else:
        with pytest.raises(r.CampaignRecordInvalid):
            r.next_ownership(before, after, not_found=evidence(), trusted_guard_identity=GUARD)


def test_bound_uid_never_changes_or_unbinds() -> None:
    before = ownership("bound", True, 1)
    other = BOUND | {"pod_uid": UUID_C}
    with pytest.raises(r.CampaignRecordInvalid):
        r.next_ownership(
            before,
            ownership("retiring", True, 2, workload=other),
            trusted_guard_identity=GUARD,
        )
    with pytest.raises(r.CampaignRecordInvalid):
        r.next_ownership(before, ownership("retiring", False, 2), trusted_guard_identity=GUARD)
    retiring = ownership("retiring", True, 2)
    assert r.next_ownership(before, retiring, trusted_guard_identity=GUARD) is retiring


def test_transition_requires_same_launch_and_next_revision() -> None:
    before = ownership("creating", False, 1)
    for revision in (1, 3):
        with pytest.raises(r.CampaignRecordInvalid):
            r.next_ownership(
                before, ownership("uncertain", False, revision), trusted_guard_identity=GUARD
            )
    for changes in (
        dict(campaign=C | {"campaign_generation": "3"}),
        dict(delivery=D | {"attempt_id": UUID_C}),
        dict(launch_id=UUID_C),
        dict(launch_request_digest="7" * 64),
        dict(projection_digest="8" * 64),
        dict(workload=UNBOUND | {"pod_name": "runner-2"}),
        dict(workload=UNBOUND | {"namespace": "campaign-2"}),
        dict(workload=UNBOUND | {"create_operation_id": UUID_C}),
    ):
        with pytest.raises(r.CampaignRecordInvalid):
            r.next_ownership(
                before, ownership("uncertain", False, 2, **changes), trusted_guard_identity=GUARD
            )


def test_one_plain_not_found_never_retires_an_ambiguous_create() -> None:
    for state in ("creating", "retiring", "uncertain"):
        before = ownership(state, False, 1)
        after = ownership("retired", True, 2)
        for bad in (
            None,
            evidence(pod_name="runner-2"),
            evidence(namespace="campaign-2"),
            evidence(pod_uid=UUID_C),
            evidence(guard_identity=GUARD | {"revision": "4"}),
        ):
            with pytest.raises(r.CampaignRecordInvalid):
                r.next_ownership(before, after, not_found=bad, trusted_guard_identity=GUARD)
        assert r.next_ownership(before, after, not_found=evidence(), trusted_guard_identity=GUARD)
    with pytest.raises(r.CampaignRecordInvalid):
        r.next_ownership(
            ownership("creating", False, 1),
            ownership("retired", True, 2),
            not_found=evidence(),
            trusted_guard_identity=GUARD | {"config_sha256": "8" * 64},
        )


@pytest.mark.parametrize(
    "bad",
    [
        dict(namespace="A"),
        dict(pod_name=""),
        dict(pod_uid="x"),
        dict(guard_identity={"control_id": UUID_C}),
        dict(guard_identity=GUARD | {"revision": "0"}),
    ],
)
def test_not_found_evidence_is_closed(bad: dict[str, Any]) -> None:
    with pytest.raises(r.CampaignRecordInvalid):
        evidence(**bad)


def test_transition_refuses_non_records() -> None:
    record = ownership("creating", False, 1)
    for before, after in (
        (record.as_dict(), ownership("uncertain", False, 2)),
        (record, ownership("uncertain", False, 2).canonical_bytes),
        (r.parse_campaign_envelope(raw()), ownership("uncertain", False, 2)),
    ):
        with pytest.raises(r.CampaignRecordInvalid):
            r.next_ownership(before, after, trusted_guard_identity=GUARD)  # type: ignore[arg-type]


@pytest.mark.parametrize("state", STATES)
def test_only_retired_ownership_leaves_acceptance_unblocked(state: str) -> None:
    for bound in SHAPES[state]:
        assert r.blocks_acceptance(ownership(state, bound, 1)) is (state != "retired")
    for unknown in (None, b"", {}, r.parse_campaign_envelope(raw()), "retired"):
        assert r.blocks_acceptance(unknown) is True  # type: ignore[arg-type]
