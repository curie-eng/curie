"""The live-evidence privacy guard and the redacted projections (ADR 0271 review fix).

Covers ``require_private_evidence`` (the structural guard both new scenarios end
with), ``redacted_turns`` and ``principal_rows_summary`` (what ``send_turns``
records instead of raw capsule facts and principals rows). Synthetic values only.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import identity_scenarios as scenarios
import identity_scenarios_attestation as att
import pytest
from identity_scenarios import StepFailed

SEG = "A1b2C3d4E5f6G7h8_-9z"  # 20 base64url characters
ENV = f"{SEG}.{SEG}"
ALICE = "identity-e2e-alice"
BOB = "identity-e2e-bob"
ISSUER = "https://idp.identity-e2e.example"
TENANT = "tenant-0001"


def principal(subject: str = ALICE) -> dict[str, str]:
    return {"tenant_id": TENANT, "issuer": ISSUER, "subject": subject}


def fingerprint(canonical: dict[str, Any]) -> str:
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def raw_turns() -> dict[str, dict[str, Any]]:
    return {
        "approver": {
            "event_id": "evt-1",
            "stream_id": "str-1",
            "sender": "alice@identity-e2e.example",
            "capsule_facts": {
                "canonical": principal(ALICE),
                "principal_present": True,
                "scope": {"agent_id": "agent-aaaa", "tenant_id": TENANT},
                "extra": {"issuer": ISSUER, "subject": ALICE},
            },
        },
        "requester": {
            "event_id": "evt-2",
            "stream_id": "str-2",
            "sender": "bob@identity-e2e.example",
            "capsule_facts": {
                "canonical": principal(BOB),
                "principal_present": True,
                "scope": {"agent_id": "agent-aaaa"},
            },
        },
    }


def fails(data: Any, *, secrets: tuple[str, ...] = ()) -> str:
    with pytest.raises(StepFailed) as caught:
        att.require_private_evidence(data, secrets=secrets)
    return str(caught.value)


# --- require_private_evidence: passing -------------------------------------------------


def test_realistic_redacted_evidence_passes_and_counts_string_leaves() -> None:
    data = {
        "scenario": "grant-attestation",
        "steps": [
            {"name": "a", "observed": {"grant": "1111222233334444", "code": "refused"}},
            {"name": "b", "observed": {"count": 3, "ok": True, "none": None, "at": 1.5}},
        ],
        "registration": {"issuer": "present"},
        "sender": "alice@identity-e2e.example",
        "tuple": ("x", "y"),
    }
    assert att.require_private_evidence(data) >= 9


def test_near_miss_envelopes_pass() -> None:
    short = "A1b2C3d4E5f6G7h"  # 15 characters
    for text in (
        f"cct.{short}.{short}",
        f"cct.{SEG}",
        f"cat1 {ENV}",
        f"xcct{SEG}.{SEG}",
        "the cct. prefix alone",
    ):
        att.require_private_evidence({"note": text})


def test_short_secret_is_ignored() -> None:
    att.require_private_evidence({"note": "contains abcdefg here"}, secrets=("abcdefg",))


# --- require_private_evidence: failing -------------------------------------------------


def test_principal_shaped_mapping_nested_in_a_list_names_its_path() -> None:
    data = {
        "steps": [{}, {}, {"observed": {"turns": {"requester": {"capsule_facts": principal()}}}}]
    }
    message = fails(data)
    assert "steps[2].observed.turns.requester.capsule_facts" in message
    assert ALICE not in message
    assert ISSUER not in message


def test_principal_shape_needs_both_keys() -> None:
    att.require_private_evidence({"a": {"issuer": ISSUER}, "b": {"subject": ALICE}})
    assert "x[0]" in fails([{"x": [{"issuer": "i", "subject": "s"}]}])


@pytest.mark.parametrize("prefix", ["cct", "ccg", "ccr", "cat1"])
def test_envelope_inside_a_sentence_fails_without_echoing_it(prefix: str) -> None:
    value = f"{prefix}.{ENV}"
    data = {"steps": [{"detail": f"the proxy refused {value} on retry"}]}
    message = fails(data)
    assert "steps[0].detail" in message
    assert value not in message
    assert SEG not in message


def test_secret_in_a_failure_message_fails_without_echoing_it() -> None:
    data = {"steps": [{"failure": f"{BOB} was not the approver"}]}
    message = fails(data, secrets=(ALICE, BOB))
    assert "steps[0].failure" in message
    assert BOB not in message


def test_mapping_key_equal_to_a_secret_fails() -> None:
    message = fails({"by": {ALICE: 1}}, secrets=(ALICE,))
    assert "by" in message
    assert ALICE not in message


def test_mapping_key_that_is_an_envelope_fails() -> None:
    message = fails({"by": {f"ccg.{ENV}": 1}})
    assert SEG not in message


def test_secret_in_a_tuple_leaf_fails() -> None:
    message = fails({"seen": ("ok", f"x-{ALICE}-y")}, secrets=(ALICE,))
    assert "seen" in message
    assert ALICE not in message


# --- redacted_turns ------------------------------------------------------------------


def test_redacted_turns_projects_only_identifiers_and_a_fingerprint() -> None:
    turns = raw_turns()
    result = scenarios.redacted_turns(turns)
    assert set(result) == {"approver", "requester"}
    approver = result["approver"]
    assert set(approver) == {"event_id", "stream_id", "sender", "capsule"}
    assert approver["event_id"] == "evt-1"
    assert approver["stream_id"] == "str-1"
    assert approver["sender"] == "alice@identity-e2e.example"
    assert approver["capsule"] == {
        "principal_present": True,
        "canonical_fingerprint": fingerprint(principal(ALICE)),
        "scope_agent_id": "agent-aaaa",
    }
    assert result["requester"]["capsule"]["canonical_fingerprint"] == fingerprint(principal(BOB))
    assert (
        result["requester"]["capsule"]["canonical_fingerprint"]
        != (approver["capsule"]["canonical_fingerprint"])
    )


def test_redacted_turns_passes_the_guard_while_the_input_does_not() -> None:
    turns = raw_turns()
    result = scenarios.redacted_turns(turns)
    assert att.require_private_evidence({"turns": result}, secrets=(ALICE, BOB, ISSUER)) > 0
    message = fails({"turns": turns}, secrets=(ALICE, BOB))
    assert "turns" in message


def test_redacted_turns_does_not_mutate_its_input() -> None:
    turns = raw_turns()
    scenarios.redacted_turns(turns)
    assert turns == raw_turns()


# --- principal_rows_summary ----------------------------------------------------------


def test_principal_rows_summary_counts_and_types_only() -> None:
    rows = [
        {"id": "row-1", "type": "human", "status": "active", "issuer": ISSUER, "subject": ALICE},
        {"id": "row-2", "type": "agent", "status": "revoked", "issuer": ISSUER, "subject": BOB},
        {"id": "row-3", "type": "human", "status": "disabled", "issuer": ISSUER, "subject": BOB},
    ]
    summary = scenarios.principal_rows_summary(rows)
    assert summary == {"rows": 3, "active_human": True, "types": ["agent", "human"]}
    assert att.require_private_evidence(summary, secrets=(ALICE, BOB, "row-1")) > 0
    flat = json.dumps(summary)
    for leaked in (ALICE, BOB, ISSUER, "row-1"):
        assert leaked not in flat


def test_principal_rows_summary_needs_human_and_active_on_the_same_row() -> None:
    rows = [
        {"type": "human", "status": "disabled"},
        {"type": "agent", "status": "active"},
    ]
    assert scenarios.principal_rows_summary(rows)["active_human"] is False


def test_principal_rows_summary_of_no_rows() -> None:
    assert scenarios.principal_rows_summary([]) == {
        "rows": 0,
        "active_human": False,
        "types": [],
    }
