"""The grant-attestation step checks, judged on recorded shapes.

Covers the pure checks the ``grant-attestation`` scenario (ADR 0271, S7) judges
its steps with: the gated-tool list, the unattested and attested effect
requirements, the proxy refusal requirement, the spend-key difference, and the
fake model's dial line. Synthetic ids only.
"""

from __future__ import annotations

from typing import Any

import identity_e2e as harness
import identity_scenarios_attestation as att
import pytest
from identity_scenarios import StepFailed

AGENT = "ie2e-on"
CONNECTOR = "human-restricted"
SINCE = 1_800_000_000.0
IN = SINCE + 5.0
BEFORE = SINCE - 600.0
REQUESTER = "a1b2c3d4e5f60718"
OTHER_REQUESTER = "0f0e0d0c0b0a0908"
GRANT = "1111222233334444"
OTHER_GRANT = "5555666677778888"
ATTESTED = "execute_attested"
APPROVED = "execute_approved"
EFFECT = "record_effect"


def decision(
    outcome: str = "admitted",
    *,
    at: float = IN,
    attested: str | None = None,
    grant: str | None = None,
    caller: str = AGENT,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "caller": caller,
        "outcome": outcome,
        "method": "POST",
        "path": "/mcp",
        "principal": "present" if outcome == "admitted" else None,
        "connector": CONNECTOR,
        "at_epoch": at,
    }
    if attested is not None:
        entry["attested"] = attested
    if grant is not None:
        entry["grant"] = grant
    return entry


def effect(
    tool: str = ATTESTED,
    outcome: str = "ok",
    *,
    requester: str = REQUESTER,
    grant: str | None = "-",
    at: float = IN,
) -> dict[str, Any]:
    return {
        "at_epoch": at,
        "tool": tool,
        "outcome": outcome,
        "requester": requester,
        "grant": grant,
        "connector": CONNECTOR,
        "pod": "ie2e-on-human-restricted-abc",
    }


# --- gated_tools_from_env / require_ungated -----------------------------------------


def test_gated_tools_absent_or_empty_means_none() -> None:
    assert att.gated_tools_from_env(None) == []
    assert att.gated_tools_from_env("") == []


def test_gated_tools_parses_the_json_list() -> None:
    value = '["human-restricted/delete_*","mcp__other__write"]'
    assert att.gated_tools_from_env(value) == ["human-restricted/delete_*", "mcp__other__write"]


@pytest.mark.parametrize("value", ["not json", '{"a":1}', "[1,2]", '["ok",null]', '"x"'])
def test_gated_tools_malformed_raises(value: str) -> None:
    with pytest.raises(harness.HarnessError):
        att.gated_tools_from_env(value)


def test_ungated_passes_when_nothing_matches() -> None:
    att.require_ungated(["human-restricted/delete_*", "other/*"], CONNECTOR, [ATTESTED, APPROVED])
    att.require_ungated([], CONNECTOR, [ATTESTED, APPROVED])


@pytest.mark.parametrize(
    "pattern",
    [
        "human-restricted/execute_attested",
        "human-restricted/execute_*",
        "human-restricted/*",
        "mcp__human-restricted__execute_attested",
        "mcp__human-restricted__*",
        "*",
    ],
)
def test_ungated_fails_naming_the_matching_pattern(pattern: str) -> None:
    with pytest.raises(StepFailed, match="execute_attested|" + pattern.replace("*", r"\*")):
        att.require_ungated(["unrelated/x", pattern], CONNECTOR, [ATTESTED, APPROVED])
    with pytest.raises(StepFailed) as raised:
        att.require_ungated([pattern], CONNECTOR, [ATTESTED])
    assert pattern in str(raised.value)


def test_ungated_match_is_case_sensitive() -> None:
    att.require_ungated(["Human-Restricted/EXECUTE_*"], CONNECTOR, [ATTESTED])


def test_ungated_catches_a_pattern_matching_only_one_tool() -> None:
    with pytest.raises(StepFailed):
        att.require_ungated(["human-restricted/execute_approved"], CONNECTOR, [ATTESTED, APPROVED])


# --- require_unattested -------------------------------------------------------------


def unattested(**overrides: Any) -> dict[str, Any]:
    args: dict[str, Any] = {"tool": EFFECT, "outcome": "ok", "since": SINCE}
    args.update(overrides)
    return args


def test_unattested_passes_on_a_plain_forward() -> None:
    result = att.require_unattested([decision()], [effect(EFFECT)], **unattested())
    assert result is None or isinstance(result, dict)


def test_unattested_passes_for_a_connector_refusal() -> None:
    att.require_unattested(
        [decision()],
        [effect(ATTESTED, "attestation_missing")],
        **unattested(tool=ATTESTED, outcome="attestation_missing"),
    )


def test_unattested_ignores_entries_before_the_window() -> None:
    att.require_unattested(
        [decision(attested="present", at=BEFORE), decision()],
        [effect(EFFECT, grant=GRANT, at=BEFORE), effect(EFFECT)],
        **unattested(),
    )


def test_unattested_fails_when_a_decision_is_attested() -> None:
    with pytest.raises(StepFailed, match="attested"):
        att.require_unattested(
            [decision(), decision(attested="present")], [effect(EFFECT)], **unattested()
        )


def test_unattested_fails_on_a_grant_refusal() -> None:
    refusal = decision("grant_required", grant="not_spent")
    with pytest.raises(StepFailed, match="grant"):
        att.require_unattested([decision(), refusal], [effect(EFFECT)], **unattested())


def test_unattested_fails_with_no_connector_line() -> None:
    with pytest.raises(StepFailed):
        att.require_unattested([decision()], [], **unattested())


def test_unattested_fails_with_two_connector_lines() -> None:
    with pytest.raises(StepFailed):
        att.require_unattested(
            [decision()], [effect(EFFECT), effect(EFFECT, at=IN + 1)], **unattested()
        )


def test_unattested_fails_when_the_line_is_for_another_tool() -> None:
    with pytest.raises(StepFailed):
        att.require_unattested([decision()], [effect(ATTESTED)], **unattested())


def test_unattested_fails_when_the_outcome_differs() -> None:
    with pytest.raises(StepFailed, match="attestation_missing|ok"):
        att.require_unattested(
            [decision()], [effect(EFFECT, "attestation_missing")], **unattested(outcome="ok")
        )


def test_unattested_fails_when_the_connector_saw_a_grant() -> None:
    with pytest.raises(StepFailed, match="grant"):
        att.require_unattested([decision()], [effect(EFFECT, grant=GRANT)], **unattested())


def test_unattested_fails_when_a_pre_change_line_has_no_grant_field() -> None:
    with pytest.raises(StepFailed):
        att.require_unattested([decision()], [effect(EFFECT, grant=None)], **unattested())


# --- require_attested_effect --------------------------------------------------------


def attested_args(**overrides: Any) -> dict[str, Any]:
    args: dict[str, Any] = {
        "tool": ATTESTED,
        "requester": REQUESTER,
        "spent": [GRANT],
    }
    args.update(overrides)
    return args


def test_attested_effect_passes_on_the_recorded_shape() -> None:
    att.require_attested_effect(
        [decision(), decision(attested="present")],
        [effect(ATTESTED, grant=GRANT)],
        **attested_args(),
    )


def test_attested_effect_fails_with_no_attested_decision() -> None:
    with pytest.raises(StepFailed, match="attested"):
        att.require_attested_effect(
            [decision()], [effect(ATTESTED, grant=GRANT)], **attested_args()
        )


def test_attested_effect_fails_with_two_attested_decisions() -> None:
    with pytest.raises(StepFailed, match="attested"):
        att.require_attested_effect(
            [decision(attested="present"), decision(attested="present", at=IN + 1)],
            [effect(ATTESTED, grant=GRANT)],
            **attested_args(),
        )


def test_attested_effect_fails_with_no_connector_line() -> None:
    with pytest.raises(StepFailed):
        att.require_attested_effect([decision(attested="present")], [], **attested_args())


def test_attested_effect_fails_with_two_connector_lines() -> None:
    lines = [effect(ATTESTED, grant=GRANT), effect(ATTESTED, grant=GRANT, at=IN + 1)]
    with pytest.raises(StepFailed):
        att.require_attested_effect([decision(attested="present")], lines, **attested_args())


def test_attested_effect_fails_when_the_outcome_is_a_refusal() -> None:
    with pytest.raises(StepFailed, match="ok|attestation"):
        att.require_attested_effect(
            [decision(attested="present")],
            [effect(ATTESTED, "grant_mismatch", grant=GRANT)],
            **attested_args(),
        )


def test_attested_effect_fails_for_another_requester() -> None:
    with pytest.raises(StepFailed, match="requester"):
        att.require_attested_effect(
            [decision(attested="present")],
            [effect(ATTESTED, requester=OTHER_REQUESTER, grant=GRANT)],
            **attested_args(),
        )


def test_attested_effect_fails_for_a_dash_requester() -> None:
    with pytest.raises(StepFailed, match="requester"):
        att.require_attested_effect(
            [decision(attested="present")],
            [effect(ATTESTED, requester="-", grant=GRANT)],
            **attested_args(),
        )


def test_attested_effect_fails_for_a_dash_grant() -> None:
    with pytest.raises(StepFailed, match="grant"):
        att.require_attested_effect(
            [decision(attested="present")], [effect(ATTESTED, grant="-")], **attested_args()
        )


def test_attested_effect_fails_for_a_grant_other_than_the_spent_one() -> None:
    with pytest.raises(StepFailed, match="grant"):
        att.require_attested_effect(
            [decision(attested="present")],
            [effect(ATTESTED, grant=OTHER_GRANT)],
            **attested_args(),
        )


@pytest.mark.parametrize("spent", [[], [GRANT, OTHER_GRANT]], ids=["none", "two"])
def test_attested_effect_requires_exactly_one_spend_fingerprint(spent: list[str]) -> None:
    with pytest.raises(StepFailed, match="spen"):
        att.require_attested_effect(
            [decision(attested="present")],
            [effect(ATTESTED, grant=GRANT)],
            **attested_args(spent=spent),
        )


def test_attested_effect_fails_for_another_tool() -> None:
    with pytest.raises(StepFailed):
        att.require_attested_effect(
            [decision(attested="present")],
            [effect(EFFECT, grant=GRANT)],
            **attested_args(),
        )


# --- require_proxy_refused ----------------------------------------------------------


def refused(**overrides: Any) -> dict[str, Any]:
    args: dict[str, Any] = {"reason": "not_spent"}
    args.update(overrides)
    return args


def test_proxy_refused_passes_on_the_recorded_shape() -> None:
    att.require_proxy_refused([decision("grant_required", grant="not_spent")], [], **refused())


def test_proxy_refused_ignores_admitted_decisions_beside_it() -> None:
    att.require_proxy_refused(
        [decision(), decision("grant_required", grant="arguments_mismatch")],
        [],
        **refused(reason="arguments_mismatch"),
    )


def test_proxy_refused_fails_with_no_refusal() -> None:
    with pytest.raises(StepFailed, match="grant_required"):
        att.require_proxy_refused([decision()], [], **refused())


def test_proxy_refused_fails_with_two_refusals() -> None:
    twice = [
        decision("grant_required", grant="not_spent"),
        decision("grant_required", grant="not_spent", at=IN + 1),
    ]
    with pytest.raises(StepFailed, match="grant_required"):
        att.require_proxy_refused(twice, [], **refused())


def test_proxy_refused_fails_on_another_sub_reason() -> None:
    with pytest.raises(StepFailed, match="not_spent|grant_expired"):
        att.require_proxy_refused(
            [decision("grant_required", grant="grant_expired")], [], **refused()
        )


def test_proxy_refused_fails_on_a_refusal_without_a_sub_reason() -> None:
    with pytest.raises(StepFailed):
        att.require_proxy_refused([decision("grant_required")], [], **refused())


def test_proxy_refused_fails_on_a_different_refusal_outcome() -> None:
    with pytest.raises(StepFailed, match="grant_required"):
        att.require_proxy_refused([decision("invalid", grant="not_spent")], [], **refused())


def test_proxy_refused_fails_when_the_connector_logged_a_line() -> None:
    with pytest.raises(StepFailed, match="connector|effect"):
        att.require_proxy_refused(
            [decision("grant_required", grant="not_spent")],
            [effect(ATTESTED, grant=GRANT)],
            **refused(),
        )


# --- spent_since / require_no_spend -------------------------------------------------


def test_spent_since_is_the_sorted_new_fingerprints() -> None:
    assert att.spent_since([OTHER_GRANT], [OTHER_GRANT, GRANT, "0000000000000001"]) == [
        "0000000000000001",
        GRANT,
    ]


def test_spent_since_is_empty_when_nothing_changed() -> None:
    assert att.spent_since([GRANT], [GRANT]) == []
    assert att.spent_since([], []) == []


def test_spent_since_ignores_removed_keys() -> None:
    assert att.spent_since([GRANT, OTHER_GRANT], [OTHER_GRANT]) == []


def test_spent_since_does_not_double_count_repeats() -> None:
    assert att.spent_since([], [GRANT, GRANT]) == [GRANT]


def test_no_spend_passes_when_nothing_appeared() -> None:
    att.require_no_spend([GRANT], [GRANT])
    att.require_no_spend([GRANT, OTHER_GRANT], [GRANT])


def test_no_spend_fails_naming_the_new_key() -> None:
    with pytest.raises(StepFailed, match=OTHER_GRANT):
        att.require_no_spend([GRANT], [GRANT, OTHER_GRANT])


# --- fake_dial ----------------------------------------------------------------------


CALLER = "9988776655443322"


def test_fake_dial_parses_an_ok_line() -> None:
    texts = [
        "working on it",
        f"connector-tool {CONNECTOR}/{ATTESTED}: ok applied note caller={CALLER}",
    ]
    dial = att.fake_dial(texts, CONNECTOR, ATTESTED)
    assert dial["outcome"] == "ok"
    assert dial["text"] == "applied note"
    assert dial["code"] is None
    assert dial["caller"] == CALLER


def test_fake_dial_parses_a_connector_refusal() -> None:
    line = f"connector-tool {CONNECTOR}/{ATTESTED}: refused attestation_missing caller={CALLER}"
    dial = att.fake_dial([line], CONNECTOR, ATTESTED)
    assert dial["outcome"] == "refused"
    assert dial["code"] == "attestation_missing"
    assert dial["caller"] == CALLER


def test_fake_dial_parses_a_proxy_refusal() -> None:
    line = f"connector-tool {CONNECTOR}/{ATTESTED}: failed grant_required caller={CALLER}"
    dial = att.fake_dial([line], CONNECTOR, ATTESTED)
    assert dial["outcome"] == "failed"
    assert dial["code"] == "grant_required"


def test_fake_dial_picks_the_requested_tool_among_others() -> None:
    texts = [
        f"connector-tool {CONNECTOR}/{EFFECT}: ok caller={CALLER}",
        f"connector-tool {CONNECTOR}/{ATTESTED}: refused attestation_missing caller={CALLER}",
    ]
    assert att.fake_dial(texts, CONNECTOR, ATTESTED)["code"] == "attestation_missing"
    assert att.fake_dial(texts, CONNECTOR, EFFECT)["outcome"] == "ok"


def test_fake_dial_fails_with_no_line_for_the_tool() -> None:
    line = f"connector-tool {CONNECTOR}/{EFFECT}: ok caller={CALLER}"
    with pytest.raises(StepFailed, match=ATTESTED):
        att.fake_dial([line, "other text"], CONNECTOR, ATTESTED)
    with pytest.raises(StepFailed):
        att.fake_dial([], CONNECTOR, ATTESTED)


def test_fake_dial_fails_with_two_lines() -> None:
    line = f"connector-tool {CONNECTOR}/{ATTESTED}: ok x caller={CALLER}"
    with pytest.raises(StepFailed):
        att.fake_dial([line, line], CONNECTOR, ATTESTED)
    with pytest.raises(StepFailed):
        att.fake_dial([line + "\n" + line], CONNECTOR, ATTESTED)


def test_fake_dial_ignores_a_line_for_another_connector() -> None:
    line = f"connector-tool other/{ATTESTED}: ok caller={CALLER}"
    with pytest.raises(StepFailed):
        att.fake_dial([line], CONNECTOR, ATTESTED)
