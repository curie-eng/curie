"""Scorer-seam tests: the swappable scorer above the eval port.

Two layers: the pure trajectory matcher (all five deterministic modes) and the
seam wired through ``EvalRunner`` end to end -- a non-``contains`` scorer scoring
the tool-call trajectory of a real (fake-runner) turn, then recorded through the
existing recorder's ``EvalRunResult`` shape unchanged.
"""

from __future__ import annotations

import asyncio

import pytest
from curie_worker.eval import (
    EvalCase,
    EvalRunner,
    EvalSuite,
    Grader,
    GraderKind,
    GraderScorer,
    Scorer,
    ScoreResult,
    TrajectoryMode,
    TrajectoryScorer,
    TrajectorySpec,
    match_trajectory,
)

CONTAINS = GraderKind.CONTAINS


def _case(cid: str = "c") -> EvalCase:
    return EvalCase(id=cid, input="q", grader=Grader(kind=CONTAINS, expected="x"))


# --- pure trajectory matcher ----------------------------------------------


@pytest.mark.parametrize(
    ("mode", "expected", "observed", "passes"),
    [
        # EXACT: order and multiplicity must match.
        (TrajectoryMode.EXACT, ["a", "b"], ["a", "b"], True),
        (TrajectoryMode.EXACT, ["a", "b"], ["a", "b", "c"], False),
        (TrajectoryMode.EXACT, ["a", "b"], ["b", "a"], False),
        # IN_ORDER: expected is a subsequence, extra calls between are fine.
        (TrajectoryMode.IN_ORDER, ["a", "c"], ["a", "b", "c"], True),
        (TrajectoryMode.IN_ORDER, ["a", "c"], ["c", "a"], False),
        # ANY_ORDER: multiset containment, order ignored.
        (TrajectoryMode.ANY_ORDER, ["a", "b"], ["b", "x", "a"], True),
        (TrajectoryMode.ANY_ORDER, ["a", "a"], ["a", "b"], False),
    ],
)
def test_trajectory_modes_membership(
    mode: TrajectoryMode, expected: list[str], observed: list[str], passes: bool
) -> None:
    spec = TrajectorySpec(expected=tuple(expected), mode=mode)
    assert match_trajectory(spec, observed).passed is passes


def test_precision_penalizes_extra_calls() -> None:
    spec = TrajectorySpec(expected=("a", "b"), mode=TrajectoryMode.PRECISION, threshold=1.0)
    # All observed calls are expected -> precision 1.0 -> pass.
    assert match_trajectory(spec, ["a", "b"]).passed is True
    # A hallucinated extra call drops precision below 1.0 -> fail at threshold 1.0.
    assert match_trajectory(spec, ["a", "b", "junk"]).passed is False
    # ...but a lower threshold tolerates it.
    lenient = TrajectorySpec(expected=("a", "b"), mode=TrajectoryMode.PRECISION, threshold=0.6)
    assert match_trajectory(lenient, ["a", "b", "junk"]).passed is True


def test_recall_penalizes_missing_calls() -> None:
    spec = TrajectorySpec(expected=("a", "b"), mode=TrajectoryMode.RECALL, threshold=1.0)
    assert match_trajectory(spec, ["a", "b"]).passed is True
    # Missing one expected tool -> recall 0.5 -> fail at 1.0, pass at 0.5.
    assert match_trajectory(spec, ["a"]).passed is False
    assert match_trajectory(
        TrajectorySpec(expected=("a", "b"), mode=TrajectoryMode.RECALL, threshold=0.5),
        ["a"],
    ).passed is True


def test_failed_match_carries_a_debuggable_detail() -> None:
    result = match_trajectory(TrajectorySpec(expected=("a",), mode=TrajectoryMode.EXACT), ["b"])
    assert result.passed is False
    assert result.detail is not None
    assert "expected=['a']" in result.detail and "observed=['b']" in result.detail


# --- the seam contract ------------------------------------------------------


def test_grader_scorer_matches_the_frozen_grader() -> None:
    scorer = GraderScorer()
    case = EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="Paris"))
    assert scorer.score(case, "The capital is Paris.", []).passed is True
    assert scorer.score(case, "The capital is Berlin.", []).passed is False


def test_grader_scorer_threads_the_trajectory_to_a_tool_called_grader() -> None:
    # The default scorer now passes the observed trajectory to the grader, so a
    # frozen tool_called grader is judged on what the turn did, not its text.
    scorer = GraderScorer()
    case = EvalCase(
        id="c",
        input="q",
        grader=Grader(kind=GraderKind.TOOL_CALLED, expected="DeterministicEngine"),
    )
    # Text is unrelated; only the trajectory decides.
    assert scorer.score(case, "unrelated text", ["Bash", "DeterministicEngine"]).passed is True
    assert scorer.score(case, "I used the DeterministicEngine", []).passed is False


def test_scorers_satisfy_the_protocol() -> None:
    # runtime_checkable Protocol: both shipped scorers are structural Scorers.
    assert isinstance(GraderScorer(), Scorer)
    assert isinstance(TrajectoryScorer(), Scorer)


def test_trajectory_scorer_fails_closed_without_a_spec() -> None:
    # Deny-by-default: a case with no spec (and no default) cannot pass by omission.
    result = TrajectoryScorer().score(_case("unknown"), "output", ["a", "b"])
    assert result.passed is False
    assert result.detail is not None and "no trajectory spec" in result.detail


def test_trajectory_scorer_uses_a_default_spec_when_present() -> None:
    scorer = TrajectoryScorer(
        default=TrajectorySpec(expected=("a",), mode=TrajectoryMode.ANY_ORDER)
    )
    assert scorer.score(_case(), "out", ["a", "b"]).passed is True
    assert scorer.score(_case(), "out", ["b"]).passed is False


# --- seam wired through EvalRunner end to end -------------------------------


def test_trajectory_scorer_scores_a_real_turn_through_the_runner(make_eval_harness) -> None:
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            # The fake runner emits these tool_note frames before its final; the
            # answer text is deliberately unrelated so ONLY the trajectory decides.
            fake.responses = {"do the thing": "whatever", "skip a step": "whatever"}
            fake.tool_calls = {
                "do the thing": ["search", "fetch", "summarize"],
                "skip a step": ["search", "summarize"],
            }
            suite = EvalSuite(
                name="trajectory",
                cases=[
                    # The grader would pass on any text (contains ""), so a green
                    # here proves the trajectory scorer -- not the grader -- decided.
                    EvalCase(id="full", input="do the thing", grader=_grader_any()),
                    EvalCase(id="partial", input="skip a step", grader=_grader_any()),
                ],
            )
            scorer = TrajectoryScorer(
                specs={
                    "full": TrajectorySpec(
                        expected=("search", "fetch", "summarize"), mode=TrajectoryMode.IN_ORDER
                    ),
                    "partial": TrajectorySpec(
                        expected=("search", "fetch", "summarize"), mode=TrajectoryMode.IN_ORDER
                    ),
                }
            )
            result = await EvalRunner(client, scorer=scorer).run(
                suite, base_url=base_url, version="v1"
            )

            by_id = {r.case_id: r for r in result.results}
            # "full" made all three calls in order -> pass; "partial" missed
            # "fetch" -> the in-order subsequence fails.
            assert by_id["full"].passed is True
            assert by_id["partial"].passed is False
            assert result.summary() == "1/2 passed"

    asyncio.run(go())


def test_default_runner_still_uses_the_grader(make_eval_harness) -> None:
    # No scorer passed -> GraderScorer default -> the tool-call trajectory is
    # ignored and the answer text decides, exactly as before this seam.
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "the answer is 4"}
            fake.tool_calls = {"q": ["irrelevant_tool"]}
            suite = EvalSuite(
                name="grader",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")
            assert result.results[0].passed is True

    asyncio.run(go())


def _grader_any() -> Grader:
    # A grader that matches any output (contains the empty string), used to prove
    # the trajectory scorer is what gates the case.
    return Grader(kind=CONTAINS, expected="")


def test_score_result_is_a_simple_verdict() -> None:
    r = ScoreResult(passed=True)
    assert r.passed is True and r.detail is None
