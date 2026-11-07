"""The requester-handoff step checks, judged on recorded shapes.

Covers the pure helpers the ``requester-handoff`` scenario uses: claim
replacement, principal match, reply fingerprint and the deferral evidence.
"""

from __future__ import annotations

import json
from typing import Any

import identity_scenarios
import pytest

FP_A = "aaaa1111bbbb2222"
FP_B = "cccc3333dddd4444"
SENT_AT = 1_000_000.0
PRINCIPAL_A = {"kind": "slack", "id": "U0AAAA"}
PRINCIPAL_B = {"kind": "slack", "id": "U0BBBB"}


def observed(
    claim: str, created: float, principal: Any, fingerprint: str
) -> dict[str, Any]:
    return {
        "claim": claim,
        "claim_created_epoch": created,
        "claims": {"agent": "ie2e-on", "principal": principal},
        "caller_fingerprint": fingerprint,
    }


FIRST = observed("claim-one", SENT_AT - 60.0, PRINCIPAL_A, FP_A)
SECOND = observed("claim-two", SENT_AT + 3.0, PRINCIPAL_B, FP_B)


def reply(name: str, caller: str) -> str:
    return f"connector-call {name}: ok tools=1 caller={caller}"


# --- require_replaced ---------------------------------------------------------------


def test_replaced_passes_and_is_json_serialisable() -> None:
    summary = identity_scenarios.require_replaced(FIRST, SECOND, sent_at=SENT_AT)
    assert isinstance(summary, dict)
    json.dumps(summary)


def test_replaced_summary_carries_no_token() -> None:
    first = dict(FIRST, token="tok-secret-one")
    second = dict(SECOND, token="tok-secret-two")
    summary = identity_scenarios.require_replaced(first, second, sent_at=SENT_AT)
    dumped = json.dumps(summary)
    assert "tok-secret-one" not in dumped
    assert "tok-secret-two" not in dumped


def test_replaced_fails_when_the_same_claim_served_both() -> None:
    same = dict(SECOND, claim=FIRST["claim"])
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_replaced(FIRST, same, sent_at=SENT_AT)


def test_replaced_fails_when_the_new_claim_predates_the_send() -> None:
    old = dict(SECOND, claim_created_epoch=SENT_AT - 30.0)
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_replaced(FIRST, old, sent_at=SENT_AT)


def test_replaced_allows_two_seconds_of_clock_skew() -> None:
    skewed = dict(SECOND, claim_created_epoch=SENT_AT - 2.0)
    identity_scenarios.require_replaced(FIRST, skewed, sent_at=SENT_AT)


def test_replaced_rejects_just_past_the_skew_window() -> None:
    late = dict(SECOND, claim_created_epoch=SENT_AT - 2.5)
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_replaced(FIRST, late, sent_at=SENT_AT)


def test_replaced_fails_when_the_caller_fingerprint_is_unchanged() -> None:
    stale = dict(SECOND, caller_fingerprint=FP_A)
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_replaced(FIRST, stale, sent_at=SENT_AT)


# --- require_principal --------------------------------------------------------------


def test_principal_passes_on_a_match() -> None:
    identity_scenarios.require_principal(SECOND, PRINCIPAL_B)


def test_principal_fails_on_the_other_human() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_principal(SECOND, PRINCIPAL_A)


def test_principal_fails_when_the_claim_has_no_principal() -> None:
    bare = {"claims": {"agent": "ie2e-on"}}
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_principal(bare, PRINCIPAL_B)


# --- require_reply_fingerprint ------------------------------------------------------


def test_reply_fingerprint_passes_when_every_ok_line_carries_it() -> None:
    texts = [reply("netpol-probe", FP_B), "unrelated text"]
    identity_scenarios.require_reply_fingerprint(texts, FP_B)


def test_reply_fingerprint_fails_when_b_reply_carries_a_fingerprint() -> None:
    # The reviewer worry: B was served with A's token.
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_reply_fingerprint([reply("netpol-probe", FP_A)], FP_B)


def test_reply_fingerprint_fails_if_any_ok_line_differs() -> None:
    texts = [reply("netpol-probe", FP_B), reply("second", FP_A)]
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_reply_fingerprint(texts, FP_B)


def test_reply_fingerprint_fails_with_no_ok_line() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_reply_fingerprint(["connector-call: none"], FP_B)
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_reply_fingerprint([], FP_B)


def test_reply_fingerprint_ignores_failed_lines() -> None:
    texts = [reply("netpol-probe", FP_B), "connector-call second: failed invalid"]
    identity_scenarios.require_reply_fingerprint(texts, FP_B)


# --- require_deferred ---------------------------------------------------------------

PENDING = "requester handoff pending for event evt-1"
A_REPLY_AT = SENT_AT + 8.0


def deferred(messages: list[str], b_claim_created: float = A_REPLY_AT + 40.0) -> dict[str, Any]:
    return identity_scenarios.require_deferred(
        messages, a_reply_at=A_REPLY_AT, b_claim_created=b_claim_created
    )


def test_deferred_passes_and_is_json_serialisable() -> None:
    summary = deferred(["starting", PENDING, "all done"])
    assert isinstance(summary, dict)
    json.dumps(summary)


def test_deferred_fails_as_not_exercised_on_the_idle_path() -> None:
    with pytest.raises(identity_scenarios.StepFailed) as raised:
        deferred(["starting", "all done"])
    assert "deferral not exercised" in str(raised.value)


def test_deferred_fails_with_no_messages_at_all() -> None:
    with pytest.raises(identity_scenarios.StepFailed) as raised:
        deferred([])
    assert "deferral not exercised" in str(raised.value)


def test_deferred_fails_when_the_turn_was_interrupted_before_retry() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        deferred([PENDING, "interrupting before retry", "all done"])


def test_deferred_fails_when_b_claim_predates_a_reply() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        deferred([PENDING], b_claim_created=A_REPLY_AT - 10.0)


def test_deferred_allows_two_seconds_of_skew_on_b_claim() -> None:
    deferred([PENDING], b_claim_created=A_REPLY_AT - 2.0)
    with pytest.raises(identity_scenarios.StepFailed):
        deferred([PENDING], b_claim_created=A_REPLY_AT - 2.5)
