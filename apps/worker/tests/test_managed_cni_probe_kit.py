"""Verdict rules and entry encoding of the managed CNI probe kit, ADR 0238.

@spec MANAGED-CNI-2 @spec MANAGED-CNI-3 @spec MANAGED-CNI-4. These tests prove
only the entry encoding, digest grouping and verdict rules. No cluster is
measured here and nothing here is qualification evidence.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json

import pytest
from curie_worker.managed_cni_probe_kit import (
    PRODUCER_CONSTANTS,
    PRODUCER_KIND,
    DenialEvidence,
    EgressEvidence,
    ObservationEntry,
    ProbeKitRefused,
    accepts,
    channel_digest,
    judge_denial,
    judge_egress,
    judge_guard_dry_run,
    judge_policy_sets,
    judge_reviews,
    producer_constants_digest,
    served_families,
)

DIGEST = "ab" * 32


def entry(**changes: object) -> ObservationEntry:
    base: dict[str, object] = {
        "case": 1,
        "sub_case": "non_vacuity",
        "family": "ipv4",
        "placement": "same_node",
        "source_kind": "foreign_namespace",
        "outcome": "pass",
        "reason": "denied",
        "evidence_digest": DIGEST,
    }
    base.update(changes)
    return ObservationEntry(**base)  # type: ignore[arg-type]


def clean_denial(**changes: object) -> DenialEvidence:
    base = DenialEvidence(
        sender_ready=True,
        listener_ready=True,
        on_requested_node=True,
        family_address=True,
        listener_deliveries=0,
        sockets_sampled=True,
        window_covered=True,
        overflow_moved=False,
        client_handshakes=0,
        client_replies=0,
        control_succeeded=True,
    )
    return dataclasses.replace(base, **changes)  # type: ignore[arg-type]


def test_constants_table_is_exactly_clause_two() -> None:
    assert PRODUCER_KIND == "managed-behavioral-v1"
    assert PRODUCER_CONSTANTS["denial_tcp_attempts"] == 5
    assert PRODUCER_CONSTANTS["denial_udp_attempts"] == 5
    assert PRODUCER_CONSTANTS["attempt_interval_ms"] == 200
    assert PRODUCER_CONSTANTS["connect_timeout_ms"] == 2000
    assert PRODUCER_CONSTANTS["listener_window_ms"] == 5000
    assert PRODUCER_CONSTANTS["pod_phase_timeout_ms"] == 120000
    assert PRODUCER_CONSTANTS["per_call_deadline_ms"] == 4000
    canonical = json.dumps(PRODUCER_CONSTANTS, sort_keys=True, separators=(",", ":"))
    assert producer_constants_digest() == hashlib.sha256(canonical.encode("ascii")).hexdigest()


def test_entry_document_has_exactly_the_spec_members() -> None:
    assert set(entry().document()) == {
        "case",
        "sub_case",
        "family",
        "placement",
        "source_kind",
        "outcome",
        "reason",
        "evidence_digest",
    }


@pytest.mark.parametrize(
    "changes",
    [
        {"case": 9},
        {"case": True},
        {"sub_case": "broker"},  # not a case 1 sub-case
        {"family": "ipv5"},
        {"placement": "elsewhere"},
        {"source_kind": "runner_policy"},  # not a case 1 source kind
        {"outcome": "skipped"},
        {"reason": "anything"},
        {"evidence_digest": "AB" * 32},
        {"case": 3, "sub_case": "spoof_live"},  # case 3 belongs to a later slice
    ],
)
def test_entry_refuses_values_outside_the_literals(changes: dict[str, object]) -> None:
    with pytest.raises(ProbeKitRefused):
        entry(**changes)


def test_non_address_entries_carry_family_and_placement_none() -> None:
    entry(
        case=2,
        sub_case="guard_refusal",
        family="none",
        placement="none",
        source_kind="none",
        reason="guard_refused",
    )
    with pytest.raises(ProbeKitRefused):
        entry(
            case=2,
            sub_case="guard_refusal",
            family="ipv4",
            placement="none",
            source_kind="none",
            reason="guard_refused",
        )


def test_acceptance_refuses_missing_duplicate_and_non_pass_entries() -> None:
    a = entry()
    b = entry(source_kind="protected_unselected")
    required = {a.key, b.key}
    assert accepts((a, b), required)
    assert not accepts((a,), required)
    assert not accepts((a, a, b), required)
    assert not accepts(
        (
            a,
            entry(
                source_kind="protected_unselected", outcome="unavailable", reason="control_failed"
            ),
        ),
        required,
    )
    assert not accepts(
        (a, b, entry(placement="cross_node", outcome="fail", reason="enforcement_absent")), required
    )
    assert not accepts((), set())


def test_channel_digest_groups_cases_as_the_spec_table() -> None:
    one = entry()
    two = entry(case=2, sub_case="broker", source_kind="release_namespace")
    network = channel_digest((one, two), "network")
    hostile = channel_digest((one, two), "hostile")
    assert network == channel_digest((one,), "network")
    assert hostile == channel_digest((two,), "hostile")
    assert channel_digest((two, one), "network") == network  # order independent
    with pytest.raises(ProbeKitRefused):
        channel_digest((one,), "cni")


def test_denial_passes_only_on_clean_evidence() -> None:
    assert judge_denial(clean_denial()) == ("pass", "denied")


@pytest.mark.parametrize(
    "changes",
    [{"listener_deliveries": 1}, {"client_handshakes": 1}, {"client_replies": 1}],
)
def test_any_delivery_fails_even_without_a_control(changes: dict[str, object]) -> None:
    evidence = clean_denial(control_succeeded=False, **changes)
    assert judge_denial(evidence) == ("fail", "delivered")
    assert judge_denial(evidence, delivery_reason="enforcement_absent") == (
        "fail",
        "enforcement_absent",
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"control_succeeded": False}, "control_failed"),
        ({"overflow_moved": True}, "listener_overflow"),
        ({"sender_ready": False}, "phase_timeout"),
        ({"listener_ready": False}, "phase_timeout"),
        ({"family_address": False}, "no_family_address"),
        ({"on_requested_node": False}, "wrong_node"),
        ({"sockets_sampled": False}, "listener_incomplete"),
        ({"window_covered": False}, "listener_incomplete"),
    ],
)
def test_denial_without_delivery_is_unavailable_never_pass(
    changes: dict[str, object], reason: str
) -> None:
    assert judge_denial(clean_denial(**changes)) == ("unavailable", reason)


def test_egress_is_judged_at_the_client() -> None:
    clean = EgressEvidence(
        sender_ready=True,
        on_requested_node=True,
        family_address=True,
        handshakes=0,
        replies=0,
        unattributed=0,
        control_succeeded=True,
        destinations_reached=True,
    )
    assert judge_egress(clean) == ("pass", "denied")
    # A destination an unrestricted pod cannot reach either proves no denial.
    assert judge_egress(dataclasses.replace(clean, destinations_reached=False)) == (
        "unavailable",
        "destination_unreached",
    )
    assert judge_egress(dataclasses.replace(clean, destinations_reached=False, handshakes=1)) == (
        "fail",
        "reply_received",
    )
    assert judge_egress(dataclasses.replace(clean, replies=1)) == ("fail", "reply_received")
    assert judge_egress(dataclasses.replace(clean, handshakes=1, control_succeeded=False)) == (
        "fail",
        "reply_received",
    )
    assert judge_egress(dataclasses.replace(clean, unattributed=1)) == (
        "unavailable",
        "unreachable_unattributed",
    )
    assert judge_egress(dataclasses.replace(clean, control_succeeded=False)) == (
        "unavailable",
        "control_failed",
    )


def test_reviews_pass_only_when_every_review_is_denied() -> None:
    assert judge_reviews([False, False]) == ("pass", "reviews_denied")
    assert judge_reviews([False, True]) == ("fail", "review_allowed")
    assert judge_reviews([False, None]) == ("unavailable", "review_unavailable")
    assert judge_reviews([True, None]) == ("fail", "review_allowed")
    assert judge_reviews([]) == ("unavailable", "review_unavailable")


GUARDS = ("curie-protected-guard.curietech.ai",)


def test_guard_refusal_must_name_the_guard() -> None:
    webhook = 'admission webhook "curie-protected-guard.curietech.ai" denied the request: no'
    policy = (
        "pods \"x\" is forbidden: ValidatingAdmissionPolicy 'curie-protected-guard.curietech.ai'"
        " with binding 'b' denied request: no"
    )
    assert judge_guard_dry_run(400, webhook, GUARDS) == ("pass", "guard_refused")
    assert judge_guard_dry_run(403, policy, GUARDS) == ("pass", "guard_refused")
    assert judge_guard_dry_run(201, "", GUARDS) == ("fail", "guard_admitted")
    rbac = 'pods is forbidden: User "x" cannot create resource "pods" in API group ""'
    assert judge_guard_dry_run(403, rbac, GUARDS) == ("fail", "refused_outside_guard")
    other = 'admission webhook "someone-else.example" denied the request: no'
    assert judge_guard_dry_run(400, other, GUARDS) == ("fail", "refused_outside_guard")
    assert judge_guard_dry_run(None, "", GUARDS) == ("unavailable", "review_unavailable")
    assert judge_guard_dry_run(500, webhook, GUARDS) == ("unavailable", "review_unavailable")
    with pytest.raises(ProbeKitRefused):
        judge_guard_dry_run(400, webhook, ())


@pytest.mark.parametrize(
    "message",
    [
        # Another webhook whose custom text quotes the guard's own denial.
        'admission webhook "someone-else.example" denied the request: admission webhook '
        '"curie-protected-guard.curietech.ai" denied the request: no',
        "pods \"x\" is forbidden: ValidatingAdmissionPolicy 'other' with binding 'b' denied "
        "request: ValidatingAdmissionPolicy 'curie-protected-guard.curietech.ai' with binding",
        # A guard name that only starts with the configured one.
        'admission webhook "curie-protected-guard.curietech.ai.evil" denied the request: no',
    ],
)
def test_a_quoted_guard_name_is_not_the_guard(message: str) -> None:
    assert judge_guard_dry_run(400, message, GUARDS) == ("fail", "refused_outside_guard")


def test_the_api_server_prefix_before_the_guard_is_accepted() -> None:
    message = (
        'pods "x" is forbidden: admission webhook "curie-protected-guard.curietech.ai" '
        "denied the request: not a probe template"
    )
    assert judge_guard_dry_run(403, message, GUARDS) == ("pass", "guard_refused")


def test_policy_set_must_not_change_between_snapshots() -> None:
    first = frozenset({("networking.k8s.io", "p", "uid-1", "7")})
    assert judge_policy_sets([first, first]) == ("pass", "policy_set_unchanged")
    changed = frozenset({("networking.k8s.io", "p", "uid-1", "8")})
    assert judge_policy_sets([first, changed]) == ("fail", "policy_set_changed")
    assert judge_policy_sets([first, None]) == ("unavailable", "review_unavailable")
    assert judge_policy_sets([first]) == ("unavailable", "review_unavailable")


def test_served_families_is_the_union_of_the_three_sources() -> None:
    assert served_families(["10.0.0.4"], ["IPv4"], ["192.168.49.2"]) == ("ipv4",)
    assert served_families(["10.0.0.4"], ["IPv4"], ["192.168.49.2", "fd00::2"]) == (
        "ipv4",
        "ipv6",
    )
    assert served_families([], ["IPv6"], []) == ("ipv6",)
    with pytest.raises(ProbeKitRefused):
        served_families(["not-an-address"], [], [])
    with pytest.raises(ProbeKitRefused):
        served_families([], ["IPv5"], [])
