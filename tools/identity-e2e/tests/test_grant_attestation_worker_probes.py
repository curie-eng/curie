"""The worker-grant probes (G4 to G8) of ``grant-attestation``, judged on recorded shapes.

G4 to G8 are probes sent from the caller-proxy container with the resumed
sandbox's boot credentials: the resume claim's caller token and the
worker-minted grant. Covers ``worker_grant_cases`` and ``judge_probe``'s
``forwarded`` kind and its supplied-grant ``attested`` handling. Synthetic ids.
"""

from __future__ import annotations

from typing import Any

import identity_e2e as harness
import identity_scenarios_attestation as att
import pytest
from identity_scenarios import StepFailed

CONNECTOR = "human-restricted"
SINCE = 1_800_000_000.0
IN = SINCE + 5.0
REQUESTER = "a1b2c3d4e5f60718"
OTHER_REQUESTER = "0f0e0d0c0b0a0908"
WORKER_GRANT = "1111222233334444"
OTHER_GRANT = "5555666677778888"
NONCE = "n0nce42"
APPROVED = {"note": "approved call by the approver"}
OTHER = {"note": "some other call"}
ATTESTED = "execute_attested"
EFFECT = "record_effect"


def cases() -> dict[str, dict[str, Any]]:
    return {str(c["id"]): c for c in att.worker_grant_cases(NONCE, APPROVED, OTHER)}


# --- worker_grant_cases -------------------------------------------------------------


def test_cases_are_g4_to_g8_in_order() -> None:
    built = att.worker_grant_cases(NONCE, APPROVED, OTHER)
    assert [c["id"] for c in built] == ["G4", "G5", "G6", "G7", "G8"]


@pytest.mark.parametrize("case_id", ["G4", "G6"])
def test_unrelated_calls_forward_with_the_worker_grant(case_id: str) -> None:
    case = cases()[case_id]
    assert case["tool"] == EFFECT
    assert case["arguments"] == {"note": f"grant-attestation {case_id} {NONCE}"}
    assert case["supplied_grant"] == "worker"
    assert case["expect"] == {"kind": "forwarded", "outcome": "ok"}


def test_g5_spends_the_worker_grant_on_exactly_the_approved_arguments() -> None:
    case = cases()["G5"]
    assert case["tool"] == ATTESTED
    assert case["arguments"] == APPROVED
    assert case["supplied_grant"] == "worker"
    assert case["expect"] == {"kind": "attested", "outcome": "ok"}


def test_g7_replays_g5_and_is_refused_not_spent() -> None:
    case = cases()["G7"]
    assert case["replay_of"] == "G5"
    assert case["expect"] == {"kind": "refused", "reason": "not_spent"}


def test_g8_sends_other_arguments_with_the_worker_grant() -> None:
    assert APPROVED != OTHER
    case = cases()["G8"]
    assert case["tool"] == ATTESTED
    assert case["arguments"] == OTHER
    assert case["supplied_grant"] == "worker"
    assert case["expect"] == {"kind": "unattested", "outcome": "attestation_missing"}


def test_no_case_carries_or_mints_a_grant() -> None:
    for case in att.worker_grant_cases(NONCE, APPROVED, OTHER):
        assert not case.get("grant")
        assert "grant_token" not in case


# --- judge_probe: forwarded ---------------------------------------------------------


def decision(
    outcome: str = "admitted",
    *,
    attested: str | None = None,
    grant: str | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "caller": "ie2e-on",
        "outcome": outcome,
        "method": "POST",
        "path": "/mcp",
        "principal": "present" if outcome == "admitted" else None,
        "connector": CONNECTOR,
        "at_epoch": IN,
    }
    if attested is not None:
        entry["attested"] = attested
    if grant is not None:
        entry["grant"] = grant
    return entry


def effect(
    tool: str = EFFECT,
    outcome: str = "ok",
    *,
    requester: str = REQUESTER,
    grant: str | None = "-",
) -> dict[str, Any]:
    return {
        "at_epoch": IN,
        "tool": tool,
        "outcome": outcome,
        "requester": requester,
        "grant": grant,
        "connector": CONNECTOR,
        "pod": "ie2e-on-human-restricted-abc",
    }


def forwarded_case() -> dict[str, Any]:
    return {
        "id": "G4",
        "tool": EFFECT,
        "arguments": {"note": f"grant-attestation G4 {NONCE}"},
        "supplied_grant": "worker",
        "expect": {"kind": "forwarded", "outcome": "ok"},
    }


def forwarded_answer(**overrides: Any) -> dict[str, Any]:
    answer: dict[str, Any] = {
        "id": "G4",
        "status": 200,
        "is_error": False,
        "text": f"effect=3 requester={REQUESTER}",
        "refusal": None,
        "dropped": False,
        "grant_fingerprint": WORKER_GRANT,
    }
    answer.update(overrides)
    return answer


def judge_forwarded(
    answer: dict[str, Any] | None = None,
    decisions: list[dict[str, Any]] | None = None,
    effects: list[dict[str, Any]] | None = None,
    new_spent: list[str] | None = None,
    requester: str = REQUESTER,
) -> dict[str, Any]:
    return att.judge_probe(
        forwarded_case(),
        forwarded_answer() if answer is None else answer,
        [decision()] if decisions is None else decisions,
        [effect()] if effects is None else effects,
        new_spent=[] if new_spent is None else new_spent,
        requester=requester,
    )


def test_forwarded_passes_on_the_recorded_shape() -> None:
    judged = judge_forwarded()
    assert judged["grant_fingerprint"] == WORKER_GRANT


def test_forwarded_passes_when_other_grants_were_spent_elsewhere() -> None:
    judge_forwarded(new_spent=[OTHER_GRANT])


@pytest.mark.parametrize(
    ("label", "kwargs"),
    [
        ("attested decision", {"decisions": [decision(), decision(attested="present")]}),
        ("grant refusal", {"decisions": [decision("grant_required", grant="not_spent")]}),
        ("no connector line", {"effects": []}),
        ("two connector lines", {"effects": [effect(), effect()]}),
        ("another tool's line", {"effects": [effect(ATTESTED)]}),
        ("a refused line", {"effects": [effect(EFFECT, "attestation_missing")]}),
        ("a connector grant", {"effects": [effect(grant=WORKER_GRANT)]}),
        ("wrong requester", {"effects": [effect(requester=OTHER_REQUESTER)]}),
        ("the grant spent", {"new_spent": [WORKER_GRANT]}),
        (
            "an error result",
            {"answer": forwarded_answer(is_error=True, text="refused attestation_missing")},
        ),
        ("a non-effect text", {"answer": forwarded_answer(text="something else")}),
        ("a 403", {"answer": forwarded_answer(status=403, is_error=None, text=None)}),
    ],
    ids=lambda value: value if isinstance(value, str) else "case",
)
def test_forwarded_fails_each_broken_condition(label: str, kwargs: dict[str, Any]) -> None:
    with pytest.raises(StepFailed):
        judge_forwarded(**kwargs)


# --- judge_probe: attested with a supplied grant ------------------------------------


def attested_case() -> dict[str, Any]:
    return {
        "id": "G5",
        "tool": ATTESTED,
        "arguments": APPROVED,
        "supplied_grant": "worker",
        "expect": {"kind": "attested", "outcome": "ok"},
    }


def attested_answer() -> dict[str, Any]:
    return {
        "id": "G5",
        "status": 200,
        "is_error": False,
        "text": f"effect=4 requester={REQUESTER} grant={WORKER_GRANT}",
        "refusal": None,
        "dropped": False,
        "grant_fingerprint": WORKER_GRANT,
    }


def judge_attested(new_spent: list[str]) -> dict[str, Any]:
    return att.judge_probe(
        attested_case(),
        attested_answer(),
        [decision(), decision(attested="present")],
        [effect(ATTESTED, grant=WORKER_GRANT)],
        new_spent=new_spent,
        requester=REQUESTER,
    )


def test_attested_passes_when_the_supplied_grant_is_the_one_spent() -> None:
    assert judge_attested([WORKER_GRANT])["grant_fingerprint"] == WORKER_GRANT


def test_attested_fails_when_a_different_fingerprint_was_spent() -> None:
    with pytest.raises(StepFailed):
        judge_attested([OTHER_GRANT])


def test_attested_fails_when_nothing_was_spent() -> None:
    with pytest.raises(StepFailed):
        judge_attested([])


def test_unknown_kind_is_still_a_harness_error() -> None:
    case = {**forwarded_case(), "expect": {"kind": "bogus"}}
    with pytest.raises(harness.HarnessError):
        att.judge_probe(case, forwarded_answer(), [], [], new_spent=[], requester=REQUESTER)
