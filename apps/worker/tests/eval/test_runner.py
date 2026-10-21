"""EvalRunner tests: run eval_case turns through a fake runner and grade them."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

import pytest
from curie_worker.eval import (
    AggregationPolicy,
    EvalCase,
    EvalOutcome,
    EvalRunner,
    EvalSuite,
    ExpectedStatus,
    Grader,
    GraderKind,
    SampleConfig,
    ScoreResult,
    TrajectoryScorer,
)

CONTAINS = GraderKind.CONTAINS


def test_eval_stops_at_final_and_bounds_a_stalled_http_tail(make_eval_harness) -> None:
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "answer"}
            fake.post_final_stall_inputs = {"q"}
            suite = EvalSuite(
                name="post-final-stall",
                cases=[
                    EvalCase(
                        id="c",
                        input="q",
                        grader=Grader(kind=CONTAINS, expected="answer"),
                    )
                ],
            )
            release_calls: list[dict[str, int]] = []
            real_start_turn = client.start_turn

            async def start_turn(*args: Any, **kwargs: Any) -> Any:
                turn = await real_start_turn(*args, **kwargs)
                calls = {"n": 0}
                real_release = turn._response.release

                def release() -> Any:
                    calls["n"] += 1
                    return real_release()

                turn._response.release = release
                release_calls.append(calls)
                return turn

            client.start_turn = start_turn  # type: ignore[method-assign]
            task = asyncio.create_task(
                EvalRunner(client).run(suite, base_url=base_url, version="v1")
            )
            try:
                await asyncio.wait_for(fake.post_final_started.wait(), timeout=1.0)
                result = await asyncio.wait_for(task, timeout=2.0)
                assert result.summary() == "1/1 passed"
                assert result.results[0].output == "answer"
                assert release_calls == [{"n": 1}]
            finally:
                fake.post_final_unblock.set()
                if not task.done():
                    task.cancel()
                await asyncio.wait_for(fake.post_final_handler_done.wait(), timeout=1.0)

    asyncio.run(go())


def test_runs_and_grades_a_mixed_suite(make_eval_harness) -> None:
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"2+2": "the answer is 4", "capital of France": "Berlin"}
            suite = EvalSuite(
                name="basics",
                cases=[
                    EvalCase(id="math", input="2+2", grader=Grader(kind=CONTAINS, expected="4")),
                    EvalCase(
                        id="geo",
                        input="capital of France",
                        grader=Grader(kind=CONTAINS, expected="Paris"),
                    ),
                ],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v-abc")

            assert result.version == "v-abc"
            assert result.total == 2
            assert result.summary() == "1/2 passed"
            by_id = {r.case_id: r for r in result.results}
            assert by_id["math"].passed is True
            assert by_id["geo"].passed is False and by_id["geo"].output == "Berlin"
            # The case was delivered as an eval_case event, not a message.
            assert all(frame["type"] == "eval_case" for frame in fake.seen)

    asyncio.run(go())


def test_a_case_that_errors_is_failed_not_fatal(make_eval_harness) -> None:
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.fail_inputs = {"explode"}
            fake.responses = {"fine": "yes"}
            suite = EvalSuite(
                name="s",
                cases=[
                    EvalCase(
                        id="bad", input="explode", grader=Grader(kind=CONTAINS, expected="x")
                    ),
                    EvalCase(id="good", input="fine", grader=Grader(kind=CONTAINS, expected="yes")),
                ],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")

            by_id = {r.case_id: r for r in result.results}
            assert by_id["bad"].passed is False and by_id["bad"].error is not None
            assert by_id["good"].passed is True  # one bad case did not abort the suite
            assert result.summary() == "1/2 passed"

    asyncio.run(go())


def test_classified_failure_final_fails_even_if_text_matches(make_eval_harness) -> None:
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            # The runner ends in a classified failure but its text contains the
            # expected string. The case must still FAIL (a failed turn can never
            # turn a PR check green).
            fake.responses = {"q": "the answer is 4"}
            fake.classified_failure_inputs = {"q"}
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")

            case = result.results[0]
            assert case.passed is False
            assert case.error is not None  # the failure reason is recorded
            assert result.summary() == "0/1 passed"

    asyncio.run(go())


def test_cost_is_attributed_on_failing_real_model_turns(make_eval_harness) -> None:
    # #854: a real-model turn that ends in a classified failure -- or the wrong
    # terminal status -- still burned tokens on its Final, and those failing runs
    # are often the most expensive. Their cost must be attributed, not dropped as
    # unknown. A fake run stays None: it has no real spend to price.
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"boom": "burned then failed", "wrong": "answered early"}
            fake.usage = {"boom": (1_000_000, 0), "wrong": (1_000_000, 0)}  # 1M input tokens
            fake.classified_failure_inputs = {"boom"}  # -> classified-failure return
            fake.awaiting_approval_inputs = {"wrong"}  # ends != the default expected 'done'
            suite = EvalSuite(
                name="cost",
                cases=[
                    EvalCase(id="cf", input="boom", grader=Grader(kind=CONTAINS, expected="")),
                    EvalCase(id="ws", input="wrong", grader=Grader(kind=CONTAINS, expected="")),
                ],
            )

            real = await EvalRunner(client).run(
                suite, base_url=base_url, version="v1", model="claude-sonnet-5"
            )
            by_id = {r.case_id: r for r in real.results}
            # Both failed; both burned 1M input tokens @ $3/Mtok (claude-sonnet-5) = $3.00.
            assert by_id["cf"].outcome is EvalOutcome.FAIL
            assert by_id["cf"].cost_usd == pytest.approx(3.0)
            assert by_id["ws"].cost_usd == pytest.approx(3.0)

            # The identical failing turns on the fake tier attribute no cost.
            faked = await EvalRunner(client).run(
                suite, base_url=base_url, version="v1", model="claude-sonnet-5", fake=True
            )
            assert {r.cost_usd for r in faked.results} == {None}

    asyncio.run(go())


def test_idle_awaiting_input_final_fails_even_if_text_matches(make_eval_harness) -> None:
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            # The turn ends idle-awaiting-input (an incomplete turn), yet its text
            # contains the expected string. Grading only a Done turn, the case must
            # still FAIL -- an incomplete turn can never turn a promotion gate green,
            # matching the CLI's Done-gate.
            fake.responses = {"q": "the answer is 4"}
            fake.idle_inputs = {"q"}
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")

            case = result.results[0]
            assert case.passed is False
            assert case.error is not None  # the incomplete-turn reason is recorded
            assert result.summary() == "0/1 passed"

    asyncio.run(go())


def test_fresh_conversation_by_default_reds_a_case_that_only_passes_from_history(
    make_eval_harness,
) -> None:
    """The #550 regression: a case that could only pass by inheriting a prior
    case's history goes RED under the fresh-conversation default.

    The recall case answers with the joined conversation so far. If it ran in the
    seed case's conversation, its answer would contain the seed's secret and the
    grader would pass -- the exact false green #550 removes. With the per-case
    reset, the recall case starts fresh, its answer omits the secret, and it
    fails. Ordering is no longer load-bearing."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"remember secret 42": "ok, noted"}
            fake.recall_inputs = {"what is the secret?"}
            suite = EvalSuite(
                name="leak",
                cases=[
                    EvalCase(
                        id="seed",
                        input="remember secret 42",
                        grader=Grader(kind=CONTAINS, expected="ok"),
                    ),
                    EvalCase(
                        id="recall",
                        input="what is the secret?",
                        grader=Grader(kind=CONTAINS, expected="42"),
                    ),
                ],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")

            by_id = {r.case_id: r for r in result.results}
            assert by_id["seed"].passed is True
            # The recall case CANNOT see the seed's history, so it goes red.
            assert by_id["recall"].passed is False
            assert by_id["recall"].output == ""  # no prior turn leaked in
            assert result.summary() == "1/2 passed"
            # The runner was reset before every fresh-conversation case.
            assert fake.resets == 2

    asyncio.run(go())


def test_shared_history_opt_in_lets_a_case_inherit_prior_history(
    make_eval_harness,
) -> None:
    """The opt-in half of #550: a case marked ``shared_history`` skips the reset
    and deliberately inherits the prior case's conversation, so the same recall
    case that fails in isolation now passes."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"remember secret 42": "ok, noted"}
            fake.recall_inputs = {"what is the secret?"}
            suite = EvalSuite(
                name="chain",
                cases=[
                    EvalCase(
                        id="seed",
                        input="remember secret 42",
                        grader=Grader(kind=CONTAINS, expected="ok"),
                    ),
                    EvalCase(
                        id="recall",
                        input="what is the secret?",
                        grader=Grader(kind=CONTAINS, expected="42"),
                        shared_history=True,
                    ),
                ],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")

            by_id = {r.case_id: r for r in result.results}
            assert by_id["seed"].passed is True
            # recall inherited the seed turn, so its answer carries the secret.
            assert by_id["recall"].passed is True
            assert "remember secret 42" in by_id["recall"].output
            assert result.summary() == "2/2 passed"
            # Reset ran before the seed case but NOT before the shared_history one.
            assert fake.resets == 1

    asyncio.run(go())


def test_a_failed_reset_fails_the_case_rather_than_leaking_history(
    make_eval_harness,
) -> None:
    """If isolation cannot be established (the reset endpoint errors), the case is
    failed with an error rather than run against a possibly-leaked conversation --
    a false green is exactly what #550 removes."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "the answer is 4"}
            fake.fail_reset = True  # the runner cannot establish isolation
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")

            case = result.results[0]
            assert case.passed is False
            assert case.error is not None and "reset" in case.error
            assert result.summary() == "0/1 passed"

    asyncio.run(go())


def test_gate_blocked_turn_is_green_and_narrate_only_is_red(make_eval_harness) -> None:
    """The run-7 anti-correlation, encoded (issue #262): a case that asserts
    `awaiting-approval` with a match-anything grader scores PASS when the turn
    parked awaiting approval (the gate held) and FAIL when the turn merely
    completed (`done`, the agent narrated). Before this change the runner gated on
    `done`, so the gate-blocked turn was RED and the narrate-only turn was GREEN --
    scoring anti-correlated with safety."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            gated = EvalCase(
                id="gate",
                input="q",
                grader=Grader(kind=CONTAINS, expected=""),  # match anything
                expect_status=ExpectedStatus.AWAITING_APPROVAL,
            )

            # The gate held: the turn parked awaiting approval -> GREEN.
            fake.responses = {"q": "blocked the close"}
            fake.awaiting_approval_inputs = {"q"}
            held = await EvalRunner(client).run(
                EvalSuite(name="s", cases=[gated]), base_url=base_url, version="v1"
            )
            assert held.results[0].passed is True
            assert held.summary() == "1/1 passed"

            # The agent merely narrated and the turn completed (done) -> RED.
            fake.awaiting_approval_inputs = set()
            fake.responses = {"q": "I asked for approval"}
            narrated = await EvalRunner(client).run(
                EvalSuite(name="s", cases=[gated]), base_url=base_url, version="v1"
            )
            assert narrated.results[0].passed is False
            assert narrated.results[0].error is not None

            # Inverse guard: a default (done) case never passes on an
            # awaiting-approval final, so the default gate did not loosen.
            default_case = EvalCase(
                id="d", input="q", grader=Grader(kind=CONTAINS, expected="")
            )
            fake.awaiting_approval_inputs = {"q"}
            fake.responses = {"q": "blocked the close"}
            default_result = await EvalRunner(client).run(
                EvalSuite(name="s", cases=[default_case]), base_url=base_url, version="v1"
            )
            assert default_result.results[0].passed is False

    asyncio.run(go())


def test_all_passed_suite(make_eval_harness) -> None:
    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q1": "a1", "q2": "a2"}
            suite = EvalSuite(
                name="green",
                cases=[
                    EvalCase(id="1", input="q1", grader=Grader(kind=CONTAINS, expected="a1")),
                    EvalCase(id="2", input="q2", grader=Grader(kind=CONTAINS, expected="a2")),
                ],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")
            assert result.all_passed() is True
            assert result.summary() == "2/2 passed"

    asyncio.run(go())


def test_tool_called_grader_greens_on_the_tool_note_and_reds_without_it(
    make_eval_harness,
) -> None:
    """The #621 acceptance, end to end through the real stream path: a case
    asserting a tool was called GREENs when the runner emits that tool_note and
    REDs when it does not -- and a turn that calls no tool at all fails it, so
    the grader is falsifiable (a do-nothing agent cannot pass)."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            # Same input for all three cases; the fake emits a per-input tool_note
            # trajectory, so the grader is judged on what the turn actually did.
            fake.responses = {
                "run it": "I used the DeterministicEngine",
                "narrate": "I used the DeterministicEngine tool, promise",
                "idle": "nothing happened",
            }
            fake.tool_calls = {
                "run it": ["Bash", "DeterministicEngine"],  # tool really called
                "narrate": ["Bash"],  # only claims it in text
                # "idle" emits no tool_note at all
            }
            suite = EvalSuite(
                name="tool-calls",
                cases=[
                    EvalCase(
                        id="called",
                        input="run it",
                        grader=Grader(
                            kind=GraderKind.TOOL_CALLED, expected="DeterministicEngine"
                        ),
                    ),
                    EvalCase(
                        id="only-narrated",
                        input="narrate",
                        grader=Grader(
                            kind=GraderKind.TOOL_CALLED, expected="DeterministicEngine"
                        ),
                    ),
                    EvalCase(
                        id="did-nothing",
                        input="idle",
                        grader=Grader(
                            kind=GraderKind.TOOL_CALLED, expected="DeterministicEngine"
                        ),
                    ),
                ],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")
            by_id = {r.case_id: r for r in result.results}
            # GREEN only when the tool_note is on the wire.
            assert by_id["called"].passed is True
            # RED when the agent merely claims it in text (grader reads trajectory).
            assert by_id["only-narrated"].passed is False
            # RED when no tool ran at all -- the falsifiability floor.
            assert by_id["did-nothing"].passed is False
            assert result.summary() == "1/3 passed"

    asyncio.run(go())


class _SpyScorer:
    """A scorer that would pass ANYTHING, and counts the times it was consulted.

    The unconditional ``passed=True`` is the point: if the fake path ever reaches
    a grader, the case comes back PASS and ``calls`` is non-zero, so a test asserting
    "never graded" cannot go green by accident.
    """

    def __init__(self) -> None:
        self.calls = 0

    def score(self, case: EvalCase, output: str, trajectory: Sequence[str]) -> ScoreResult:
        self.calls += 1
        return ScoreResult(passed=True)


def _one_case_suite() -> EvalSuite:
    # A grader the fake's canned "all done" could never satisfy on content, so a
    # PLUMBING_OK outcome can only come from skipping grading, never from passing it.
    return EvalSuite(
        name="s",
        cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="deal-desk"))],
    )


def test_scorer_failure_detail_is_preserved_separately_from_runtime_error(
    make_eval_harness,
) -> None:
    """A scorer rejection explains the verdict without claiming the turn broke."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "all done"}
            result = await EvalRunner(client, scorer=TrajectoryScorer()).run(
                _one_case_suite(), base_url=base_url, version="v1"
            )

            case = result.results[0]
            assert case.outcome is EvalOutcome.FAIL
            assert case.detail == "no trajectory spec for case 'c'"
            assert case.error is None

    asyncio.run(go())


def test_a_fake_turn_is_never_graded_and_is_neither_pass_nor_fail(make_eval_harness) -> None:
    """The fake model is a plumbing fixture, not a subject under test: the only
    thing the fake tier asserts is that the turn completed. The grader must not run
    at all, and the outcome is a distinct non-graded PLUMBING_OK."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "all done"}
            spy = _SpyScorer()
            result = await EvalRunner(client, scorer=spy).run(
                _one_case_suite(), base_url=base_url, version="v1", fake=True
            )

            assert spy.calls == 0, "the grader must not run at all on a fake turn"
            case = result.results[0]
            assert case.outcome is EvalOutcome.PLUMBING_OK
            assert case.passed is None  # not a pass, not a fail
            assert case.error is None  # the turn completed; nothing broke
            assert case.output == "all done"  # the turn's text is still recorded

    asyncio.run(go())


def test_a_real_turn_is_still_graded_when_the_run_is_not_fake(make_eval_harness) -> None:
    """The falsifiability guard (#527/ADR-0022): PLUMBING_OK must not become a
    backdoor that stops grading a real run. Off the fake tier, the same off-topic
    answer against the same grader is a real, graded FAIL."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "all done"}
            result = await EvalRunner(client).run(
                _one_case_suite(), base_url=base_url, version="v1", fake=False
            )

            case = result.results[0]
            assert case.outcome is EvalOutcome.FAIL
            assert case.passed is False

    asyncio.run(go())


@pytest.mark.parametrize(
    "breakage", ["classified_failure_inputs", "idle_inputs", "fail_inputs"]
)
def test_a_fake_turn_that_does_not_complete_is_a_failure_not_plumbing_ok(
    make_eval_harness, breakage: str
) -> None:
    """Ordering is load-bearing: the classified-failure / non-DONE / transport-error
    gates run BEFORE the fake early return. A fake turn that did not complete means
    the plumbing is genuinely broken -- the one thing this tier must still catch --
    so it is a real FAIL, never the non-graded outcome."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "all done"}
            getattr(fake, breakage).add("q")
            spy = _SpyScorer()
            result = await EvalRunner(client, scorer=spy).run(
                _one_case_suite(), base_url=base_url, version="v1", fake=True
            )

            case = result.results[0]
            assert case.outcome is EvalOutcome.FAIL
            assert case.passed is False
            assert case.error is not None  # the breakage reason is recorded
            assert spy.calls == 0  # a broken turn is not graded either

    asyncio.run(go())


def test_majority_vote_greens_a_case_that_passes_two_of_three_samples(make_eval_harness) -> None:
    """#332: with n=3 majority, a case whose runner answers correctly 2 of 3 times
    is GREEN, and the runner is reset before every sample so the draws are
    independent (fresh conversation each time)."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            # 2 passing answers, 1 wrong -> 2/3 under majority.
            fake.output_sequence = {"q": ["the answer is 4", "still 4", "nope"]}
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client, samples=SampleConfig(n=3)).run(
                suite, base_url=base_url, version="v1"
            )

            case = result.results[0]
            assert case.passed is True
            assert result.summary() == "1/1 passed"
            # Reset ran before each of the 3 samples (independent fresh draws).
            assert fake.resets == 3

    asyncio.run(go())


def test_flaky_case_that_passes_one_of_three_is_red_with_variance(make_eval_harness) -> None:
    """A flaky case that passes only 1 of 3 samples is RED under majority and is
    reported with its variance (the pass count)."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.output_sequence = {"q": ["the answer is 4", "nope", "wrong"]}
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client, samples=SampleConfig(n=3)).run(
                suite, base_url=base_url, version="v1"
            )

            case = result.results[0]
            assert case.passed is False
            assert case.error is None
            assert case.variance is not None and "1/3 samples passed" in case.variance
            assert case.samples == 3
            assert case.passes == 1

    asyncio.run(go())


def test_pass_at_k_greens_a_case_that_passes_once(make_eval_harness) -> None:
    """Under pass@1 the same 1-of-3 flaky case is GREEN (passed at least once)."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.output_sequence = {"q": ["nope", "the answer is 4", "wrong"]}
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(
                client, samples=SampleConfig(n=3, policy=AggregationPolicy.PASS_AT_K, k=1)
            ).run(suite, base_url=base_url, version="v1")

            assert result.results[0].passed is True

    asyncio.run(go())


def test_default_single_sample_matches_pre_sampling_behavior(make_eval_harness) -> None:
    """The default n=1 runs each case exactly once (one reset, one turn) -- the
    backward-compatible no-op path."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "the answer is 4"}
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")

            assert result.results[0].passed is True
            assert fake.resets == 1  # one sample, one reset
            assert len(fake.seen) == 1  # one turn delivered

    asyncio.run(go())


def test_cost_usd_is_priced_from_real_usage_for_a_known_model(make_eval_harness) -> None:
    """#390: a graded turn with reported usage carries a dollar cost computed from
    real token usage x the model's price, so the matrix's per-model cost rollup is
    non-null after a real run. claude-opus-4-8 is $5/1M input, $25/1M output."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "the answer is 4"}
            fake.usage = {"q": (1_000_000, 200_000)}  # 1M input, 200k output
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client).run(
                suite, base_url=base_url, version="v1", model="claude-opus-4-8"
            )

            case = result.results[0]
            assert case.passed is True
            # 1M * $5/1M + 200k * $25/1M = 5.0 + 5.0 = 10.0
            assert case.cost_usd == pytest.approx(10.0)

    asyncio.run(go())


def test_cost_usd_is_none_for_an_unpriced_model(make_eval_harness) -> None:
    """An unknown/unpriced model leaves cost None rather than guessing, so the
    case drops out of the cost rollup instead of counting as free."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "the answer is 4"}
            fake.usage = {"q": (1_000_000, 200_000)}
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client).run(
                suite, base_url=base_url, version="v1", model="some-unpriced-model"
            )

            assert result.results[0].passed is True
            assert result.results[0].cost_usd is None

    asyncio.run(go())


def test_fake_model_run_leaves_cost_none_even_with_usage(make_eval_harness) -> None:
    """A fake-tier run is never graded and never priced (#390 AC): even when the
    fake reports usage, the turn returns PLUMBING_OK before cost is computed, so
    cost stays None (no pricing)."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "all done"}
            fake.usage = {"q": (1_000_000, 200_000)}
            result = await EvalRunner(client).run(
                _one_case_suite(), base_url=base_url, version="v1",
                model="claude-opus-4-8", fake=True,
            )

            case = result.results[0]
            assert case.outcome is EvalOutcome.PLUMBING_OK
            assert case.cost_usd is None

    asyncio.run(go())


def test_cost_usd_is_none_when_the_runner_reports_no_usage(make_eval_harness) -> None:
    """A priced model but no reported usage (an older runner, or a provider that
    reports none) leaves cost None rather than pricing it as zero."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            fake.responses = {"q": "the answer is 4"}  # no fake.usage entry
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="4"))],
            )
            result = await EvalRunner(client).run(
                suite, base_url=base_url, version="v1", model="claude-opus-4-8"
            )

            assert result.results[0].passed is True
            assert result.results[0].cost_usd is None

    asyncio.run(go())


def test_a_bare_timeout_error_records_a_nonempty_error(make_eval_harness) -> None:
    """Issue #813: aiohttp raises a bare ``asyncio.TimeoutError`` on total/sock_read
    expiry, and ``str(asyncio.TimeoutError())`` is empty. If ``_run_case`` recorded
    that empty string as the error, the API's completion check (ADR-0068) would
    read a timed-out turn as "completed" instead of the loud failure it must be.
    The case must still FAIL, and its error must be non-empty."""

    async def go() -> None:
        async with make_eval_harness() as (base_url, fake, client):
            async def _timeout(*args, **kwargs):
                # aiohttp raises a bare asyncio.TimeoutError (== builtin TimeoutError
                # since 3.11) on timeout; its str() is empty, which is the whole bug.
                raise TimeoutError

            client.start_turn = _timeout  # type: ignore[method-assign]
            suite = EvalSuite(
                name="s",
                cases=[EvalCase(id="c", input="q", grader=Grader(kind=CONTAINS, expected="x"))],
            )
            result = await EvalRunner(client).run(suite, base_url=base_url, version="v1")

            case = result.results[0]
            assert case.passed is False
            assert case.error  # must be non-empty, not just non-None
            assert case.error == "TimeoutError"
            assert result.summary() == "0/1 passed"

    asyncio.run(go())
