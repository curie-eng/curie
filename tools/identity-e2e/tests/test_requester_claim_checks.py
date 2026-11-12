"""The requester-claim step checks, judged on recorded shapes.

Covers the proxy decision-line parser, the delivered-turn requirement, the
hosted-connector admission requirement, and the one step function steps 1 and
2 share, driven by a fake harness that returns recorded observations.
"""

from __future__ import annotations

from typing import Any

import identity_e2e as harness
import identity_scenarios
import pytest

AGENT = "ie2e-on"
OTHER = "ie2e-off"
PRINCIPAL = "alice@example.test"
OK_LINE = "connector-call netpol-probe: ok tools=1"


# --- parse_proxy_decision -----------------------------------------------------------


def test_parse_admitted_with_principal_present() -> None:
    line = (
        "INFO:curie_connector_proxy.server:caller=ie2e-on outcome=admitted "
        "method=POST path='/mcp' principal=present"
    )
    assert harness.parse_proxy_decision(line) == {
        "caller": "ie2e-on",
        "outcome": "admitted",
        "method": "POST",
        "path": "/mcp",
        "principal": "present",
    }


def test_parse_admitted_with_principal_absent() -> None:
    line = (
        "INFO:curie_connector_proxy.server:caller=ie2e-off outcome=admitted "
        "method=POST path='/mcp' principal=absent"
    )
    parsed = harness.parse_proxy_decision(line)
    assert parsed is not None
    assert parsed["caller"] == "ie2e-off"
    assert parsed["outcome"] == "admitted"
    assert parsed["principal"] == "absent"


def test_parse_refusal_has_no_principal() -> None:
    line = (
        "INFO:curie_connector_proxy.server:caller=ie2e-on outcome=invalid "
        "method=POST path='/mcp'"
    )
    assert harness.parse_proxy_decision(line) == {
        "caller": "ie2e-on",
        "outcome": "invalid",
        "method": "POST",
        "path": "/mcp",
        "principal": None,
    }


def test_parse_accepts_a_kubectl_timestamp_prefix() -> None:
    line = (
        "2026-11-07T12:34:56.123456789Z INFO:curie_connector_proxy.server:"
        "caller=ie2e-on outcome=admitted method=POST path='/mcp' principal=present"
    )
    parsed = harness.parse_proxy_decision(line)
    assert parsed is not None
    assert parsed["caller"] == "ie2e-on"
    assert parsed["outcome"] == "admitted"
    assert parsed["principal"] == "present"


def test_parse_route_line_keeps_the_fields() -> None:
    line = (
        "INFO:curie_connector_proxy.server:caller=ie2e-on outcome=admitted "
        "method=GET path='/a/b' route='/x'"
    )
    parsed = harness.parse_proxy_decision(line)
    assert parsed is not None
    assert parsed["caller"] == "ie2e-on"
    assert parsed["outcome"] == "admitted"
    assert parsed["method"] == "GET"
    assert parsed["path"] == "/a/b"
    assert parsed["principal"] is None


@pytest.mark.parametrize(
    "line",
    [
        "",
        "INFO:curie_connector_proxy.server:listening on 0.0.0.0:8080",
        "2026-11-07T12:34:56Z WARNING:aiohttp.access:something else entirely",
        "plain text",
    ],
)
def test_parse_non_decision_line_is_none(line: str) -> None:
    assert harness.parse_proxy_decision(line) is None


# --- require_delivered --------------------------------------------------------------


def test_require_delivered_rejects_the_reviewers_recorded_turn() -> None:
    turn = {"completed": False, "outcome": None, "texts": []}
    with pytest.raises(identity_scenarios.StepFailed) as raised:
        identity_scenarios.require_delivered(turn)
    assert "completed" in str(raised.value)


def test_require_delivered_rejects_completed_but_not_delivered() -> None:
    turn = {"completed": True, "outcome": "failed", "texts": [OK_LINE]}
    with pytest.raises(identity_scenarios.StepFailed) as raised:
        identity_scenarios.require_delivered(turn)
    assert "failed" in str(raised.value)


def test_require_delivered_rejects_delivered_without_text() -> None:
    turn = {"completed": True, "outcome": "delivered", "texts": []}
    with pytest.raises(identity_scenarios.StepFailed) as raised:
        identity_scenarios.require_delivered(turn)
    assert "delivered" in str(raised.value)


def test_require_delivered_accepts_a_delivered_turn_with_text() -> None:
    identity_scenarios.require_delivered(
        {"completed": True, "outcome": "delivered", "texts": [OK_LINE]}
    )


# --- require_connector_admitted -----------------------------------------------------


def decision(
    caller: str = AGENT, outcome: str = "admitted", principal: str | None = "present"
) -> dict[str, Any]:
    return {
        "caller": caller,
        "outcome": outcome,
        "method": "POST",
        "path": "/mcp",
        "principal": principal,
    }


def admitted(texts: list[str], decisions: list[dict[str, Any]], principal: bool = True) -> Any:
    return identity_scenarios.require_connector_admitted(
        texts, decisions, agent=AGENT, connectors=["netpol-probe"], principal=principal
    )


def test_admitted_passes_with_principal_present() -> None:
    summary = admitted([OK_LINE], [decision()], principal=True)
    assert isinstance(summary, dict)
    import json

    json.dumps(summary)


def test_admitted_passes_with_principal_absent() -> None:
    summary = admitted([OK_LINE], [decision(principal="absent")], principal=False)
    assert isinstance(summary, dict)
    import json

    json.dumps(summary)


def test_admitted_requires_an_ok_line_for_every_connector() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        identity_scenarios.require_connector_admitted(
            [OK_LINE],
            [decision()],
            agent=AGENT,
            connectors=["netpol-probe", "second"],
            principal=True,
        )


def test_admitted_rejects_a_missing_ok_line() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        admitted(["connector-call: none"], [decision()])


def test_admitted_rejects_a_failed_invalid_line() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        admitted(["connector-call netpol-probe: failed invalid"], [decision()])


def test_admitted_rejects_no_decision_for_the_agent() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        admitted([OK_LINE], [])


def test_admitted_rejects_a_decision_for_another_agent_only() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        admitted([OK_LINE], [decision(caller=OTHER)])


def test_admitted_rejects_a_refusal_for_the_agent() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        admitted([OK_LINE], [decision(), decision(outcome="invalid", principal=None)])


def test_admitted_ignores_a_refusal_for_another_agent() -> None:
    admitted([OK_LINE], [decision(), decision(caller=OTHER, outcome="invalid", principal=None)])


def test_admitted_rejects_principal_absent_when_present_is_required() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        admitted([OK_LINE], [decision(principal="absent")], principal=True)


def test_admitted_rejects_principal_present_when_absent_is_required() -> None:
    with pytest.raises(identity_scenarios.StepFailed):
        admitted([OK_LINE], [decision(principal="present")], principal=False)


def test_admitted_rejects_mixed_present_and_absent() -> None:
    mixed = [decision(principal="present"), decision(principal="absent")]
    with pytest.raises(identity_scenarios.StepFailed):
        admitted([OK_LINE], mixed, principal=True)
    with pytest.raises(identity_scenarios.StepFailed):
        admitted([OK_LINE], mixed, principal=False)


# --- human_turn_step ----------------------------------------------------------------


class FakeHarness:
    """The four calls the step makes, returning recorded shapes."""

    def __init__(
        self,
        *,
        claims: dict[str, Any],
        turn: dict[str, Any],
        decisions: list[dict[str, Any]],
        connectors: list[str] | None = None,
    ) -> None:
        self.claims = claims
        self.turn = turn
        self.decisions = decisions
        self.connector_names = connectors if connectors is not None else ["netpol-probe"]

    def send(self, agent: str, sender: str, text: str) -> dict[str, Any]:
        return {"http_status": 200, "conversation_id": "conv-1"}

    def observe(self, agent: str, *, wait: Any) -> dict[str, Any]:
        return {"caller_token_present": True, "claims": self.claims, "turn": self.turn}

    def connectors(self, agent: str) -> list[str]:
        return list(self.connector_names)

    def proxy_decisions(self, agent: str, since: Any) -> list[dict[str, Any]]:
        return list(self.decisions)


DELIVERED = {"completed": True, "outcome": "delivered", "texts": [OK_LINE]}
CLAIMS_WITH = {"agent": AGENT, "principal": PRINCIPAL}
CLAIMS_WITHOUT = {"agent": AGENT}


def run_step(fake: FakeHarness, expected_principal: str | None) -> dict[str, Any]:
    evidence = identity_scenarios.Evidence("requester-claim")
    identity_scenarios.human_turn_step(
        fake,
        evidence,
        step_id="1",
        title="a human turn",
        agent=AGENT,
        sender=PRINCIPAL,
        text="[fake:connector-call] hello",
        expected_principal=expected_principal,
        wait=30,
    )
    return evidence.finish()


def test_step_fails_on_an_incomplete_turn_after_the_claim_exists() -> None:
    fake = FakeHarness(
        claims=CLAIMS_WITH,
        turn={"completed": False, "outcome": None, "texts": []},
        decisions=[decision()],
    )
    result = run_step(fake, PRINCIPAL)
    assert result["passed"] is False
    entry = result["steps"][0]
    assert entry["passed"] is False
    assert "completed" in entry["failure"] or "turn" in entry["failure"]


def test_step_passes_on_a_delivered_turn_with_an_admitted_principal() -> None:
    fake = FakeHarness(claims=CLAIMS_WITH, turn=DELIVERED, decisions=[decision()])
    result = run_step(fake, PRINCIPAL)
    assert result["steps"][0]["passed"] is True
    assert result["passed"] is True


def test_step_fails_when_the_proxy_decision_is_a_refusal() -> None:
    fake = FakeHarness(
        claims=CLAIMS_WITH,
        turn=DELIVERED,
        decisions=[decision(outcome="invalid", principal=None)],
    )
    result = run_step(fake, PRINCIPAL)
    assert result["steps"][0]["passed"] is False
    assert result["passed"] is False


def test_step_fails_when_the_decision_lacks_the_expected_principal() -> None:
    fake = FakeHarness(
        claims=CLAIMS_WITH, turn=DELIVERED, decisions=[decision(principal="absent")]
    )
    result = run_step(fake, PRINCIPAL)
    assert result["steps"][0]["passed"] is False
    assert result["passed"] is False


def test_control_agent_passes_with_principal_absent() -> None:
    fake = FakeHarness(
        claims=CLAIMS_WITHOUT, turn=DELIVERED, decisions=[decision(principal="absent")]
    )
    result = run_step(fake, None)
    assert result["steps"][0]["passed"] is True
    assert result["passed"] is True


def test_control_agent_fails_with_principal_present() -> None:
    fake = FakeHarness(
        claims=CLAIMS_WITHOUT, turn=DELIVERED, decisions=[decision(principal="present")]
    )
    result = run_step(fake, None)
    assert result["steps"][0]["passed"] is False
    assert result["passed"] is False


# --- the proxy's real log format ----------------------------------------------------

REAL_PROXY_LINE = (
    "INFO curie_connector_proxy caller=ie2e-on outcome=admitted method=POST "
    "path='/mcp' principal=present"
)


@pytest.mark.parametrize(
    "prefix", ["", "2026-11-07T12:34:56.123456789Z "], ids=["bare", "kubectl-timestamp"]
)
def test_parse_the_proxys_real_log_format(prefix: str) -> None:
    assert harness.parse_proxy_decision(prefix + REAL_PROXY_LINE) == {
        "caller": "ie2e-on",
        "outcome": "admitted",
        "method": "POST",
        "path": "/mcp",
        "principal": "present",
    }
