"""The requester-expiry and requester-effect step checks, judged on recorded shapes.

The worker lines and proxy decisions below are copied from a live
``requester-expiry`` run on a throwaway kind cluster (thread keys shortened).
"""

from __future__ import annotations

from typing import Any

import identity_scenarios
import identity_scenarios_requester
import pytest

PRINCIPAL = {
    "issuer": "https://identity-e2e.example.test/directory",
    "subject": "identity-e2e-alice",
    "tenant_id": "00000000-0000-0000-0000-000000000001",
}
FP = "79cc3cf3783702d4"


def bounded(bound: int, configured: int) -> str:
    return (
        f"requester turn on thread email:identity-e2e:ie2e-on:<c3> bounded to {bound}s "
        f"by the requester token window (configured bound {configured}s)"
    )


# --- require_bounded ----------------------------------------------------------------


def test_a_bound_printed_equal_to_its_configured_bound_passes() -> None:
    # The worker logs only when the configured bound exceeds the cap, and prints
    # both rounded: the second line of a turn (bounded again right before the
    # runner start) shows them equal when they differ by under a second.
    seen = identity_scenarios_requester.require_bounded(
        [bounded(3437, 10799), bounded(3437, 3437), bounded(3438, 10798)]
    )
    assert seen


@pytest.mark.parametrize(
    ("bound", "configured"),
    [(3438, 3437), (3600, 10800), (3700, 10800)],
    ids=["past-configured", "token-lifetime", "past-token"],
)
def test_a_bound_past_its_configured_bound_or_the_token_fails(bound: int, configured: int) -> None:
    with pytest.raises(identity_scenarios.StepFailed, match="a bound reaches past"):
        identity_scenarios_requester.require_bounded([bounded(bound, configured)])


def test_no_bounded_line_fails() -> None:
    with pytest.raises(identity_scenarios.StepFailed, match="logged no `bounded to"):
        identity_scenarios_requester.require_bounded(["requester handoff pending"])


# --- require_connector_turn after a harness replay -----------------------------------

REPLAY = {
    "at_epoch": 1794136320.212657,
    "caller": "ie2e-on",
    "connector": "human-restricted",
    "method": "POST",
    "outcome": "expired",
    "path": "/mcp",
    "principal": None,
}
TURN_DECISIONS = [
    {
        "at_epoch": 1794136329.925888 + index / 100,
        "caller": "ie2e-on",
        "connector": connector,
        "method": "POST",
        "outcome": "admitted",
        "path": "/mcp",
        "principal": "present",
    }
    for index, connector in enumerate(["human-restricted", "netpol-probe"])
]
SENT_AT = 1794136321.964
OBSERVED = {
    "caller_token_present": True,
    "claims": {"agent": "ie2e-on", "principal": PRINCIPAL},
    "caller_fingerprint": FP,
}
TURN = {
    "completed": True,
    "outcome": "delivered",
    "texts": [
        f"connector-call human-restricted: ok tools=1 caller={FP}\n"
        f"connector-call netpol-probe: ok tools=52 caller={FP}"
    ],
}


class RecordedHarness:
    """The two reads the turn judgment makes, answered from the recorded run."""

    def __init__(self, decisions: list[dict[str, Any]]) -> None:
        self.decisions = decisions

    def connectors(self, agent: str) -> list[str]:
        return ["human-restricted", "netpol-probe"]

    def proxy_decisions(self, agent: str, since: float) -> list[dict[str, Any]]:
        return [item for item in self.decisions if item["at_epoch"] >= since]


def judge(after: float | None) -> dict[str, Any]:
    seen: dict[str, Any] = {}
    identity_scenarios.require_connector_turn(
        RecordedHarness([REPLAY, *TURN_DECISIONS]),  # type: ignore[arg-type]
        seen,
        agent="ie2e-on",
        observed=OBSERVED,
        turn=TURN,
        since=SENT_AT,
        expected_principal=PRINCIPAL,
        after=after,
    )
    return seen


def test_the_replay_inside_the_skew_margin_is_read_as_a_refusal_without_after() -> None:
    with pytest.raises(identity_scenarios.StepFailed, match="the caller proxy refused ie2e-on"):
        judge(after=None)


def test_decisions_after_the_replay_judge_the_turn_alone() -> None:
    seen = judge(after=REPLAY["at_epoch"])
    assert [item["outcome"] for item in seen["decisions"]] == ["admitted", "admitted"]


def test_a_real_refusal_after_the_replay_still_fails() -> None:
    refused = dict(TURN_DECISIONS[0], at_epoch=SENT_AT + 1.0, outcome="expired")
    seen: dict[str, Any] = {}
    with pytest.raises(identity_scenarios.StepFailed, match="the caller proxy refused ie2e-on"):
        identity_scenarios.require_connector_turn(
            RecordedHarness([REPLAY, refused, *TURN_DECISIONS]),  # type: ignore[arg-type]
            seen,
            agent="ie2e-on",
            observed=OBSERVED,
            turn=TURN,
            since=SENT_AT,
            expected_principal=PRINCIPAL,
            after=REPLAY["at_epoch"],
        )


# --- require_direct_refused (requester-effect E8) ----------------------------------------

DIRECT_REFUSED = {
    "status": 200,
    "is_error": True,
    "text": "refused missing_requester",
    "source_loopback": False,
}


def test_a_direct_call_refused_missing_requester_with_no_effect_passes() -> None:
    seen = identity_scenarios_requester.require_direct_refused(
        DIRECT_REFUSED, [{"outcome": "missing_requester", "requester": "-"}]
    )
    assert seen["logged"] == [("missing_requester", "-")]


@pytest.mark.parametrize(
    ("direct", "effects", "message"),
    [
        (
            dict(DIRECT_REFUSED, source_loopback=True),
            [{"outcome": "missing_requester", "requester": "-"}],
            "left from loopback",
        ),
        (
            dict(DIRECT_REFUSED, is_error=False, text="effect=1 requester=0123456789abcdef"),
            [{"outcome": "ok", "requester": "0123456789abcdef"}],
            "the direct call answered",
        ),
        (
            DIRECT_REFUSED,
            [
                {"outcome": "missing_requester", "requester": "-"},
                {"outcome": "ok", "requester": "0123456789abcdef"},
            ],
            "the connector logged",
        ),
        (DIRECT_REFUSED, [], "the connector logged"),
    ],
    ids=["loopback-source", "effect-applied", "extra-effect-line", "no-log-line"],
)
def test_a_direct_call_that_was_not_refused_cleanly_fails(
    direct: dict[str, Any], effects: list[dict[str, Any]], message: str
) -> None:
    with pytest.raises(identity_scenarios.StepFailed, match=message):
        identity_scenarios_requester.require_direct_refused(direct, effects)
