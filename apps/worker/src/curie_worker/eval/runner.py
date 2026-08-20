"""Run an eval suite against a runner and grade each case.

Each case is delivered as an ACI ``eval_case`` event (the frozen contract already
carries that event type) over the same HTTP channel the kernel uses, via the F1
``RunnerClient``. The case's answer is the runner's ``final`` text (or the
accumulated text deltas if no final arrives); the grader turns that into pass/fail.
A case whose turn errors is recorded as a failed case with the error, so one bad
case never aborts the suite.

Isolation note: cases run sequentially against one runner base_url, but each
case runs in a *fresh conversation* by default (#550). Before a case the runner
issues a ``reset`` (``POST /v1/reset``) that discards the runner's conversation,
so a case can never answer from an earlier case's history instead of actually
invoking its tools -- a false green for a side-effecting agent, and a silent
order-dependence in the suite. A case that sets ``shared_history=True`` opts out
of the reset and deliberately inherits the prior case's conversation (a
multi-turn scenario expressed as ordered cases). Strong *sandbox* isolation (a
fresh pod per case) remains the Job-orchestration layer's choice (the API fans
out eval Jobs per version @ sha); the fresh-conversation reset is the per-case
guarantee this in-process runner gives on one endpoint.
"""

from __future__ import annotations

import time

import aiohttp
from aci_protocol import ErrorEvent, Event, Final, SessionStatus, TextDelta, ToolNote

from ..runner_client import RunnerClient, RunnerError
from .models import EvalCase, EvalCaseResult, EvalOutcome, EvalRunResult, EvalSuite
from .pricing import cost_usd
from .sampling import SampleConfig, aggregate
from .scorer import GraderScorer, Scorer


class EvalRunner:
    """Executes eval suites against a runner endpoint and scores the answers.

    Scoring goes through the swappable :class:`~.scorer.Scorer` seam. The default
    is :class:`~.scorer.GraderScorer`, which applies the case's frozen grader to
    the answer text (behavior-preserving); pass a different scorer (e.g. a
    :class:`~.scorer.TrajectoryScorer` over the tool-call sequence) to score the
    same turns differently. The runner still owns the completion gate (a
    classified-failure or non-``done`` turn fails regardless of the scorer), so a
    scorer only ever judges a turn that actually completed. A ``fake=True`` run
    skips the scorer entirely (see :meth:`run`).
    """

    def __init__(
        self,
        runner: RunnerClient,
        *,
        scorer: Scorer | None = None,
        samples: SampleConfig | None = None,
    ) -> None:
        self._runner = runner
        self._scorer: Scorer = scorer if scorer is not None else GraderScorer()
        # Multi-sample policy (#332). Default n=1 is a no-op: each case runs once
        # and its single result is returned unchanged, so existing suites behave
        # exactly as before. n>1 runs the case n times and reduces the verdicts by
        # the configured policy (majority / pass@k) -- variance-aware grading
        # around the frozen case + scorer.
        self._samples: SampleConfig = samples if samples is not None else SampleConfig()

    async def run(
        self,
        suite: EvalSuite,
        *,
        base_url: str,
        version: str,
        token: str | None = None,
        model: str | None = None,
        fake: bool = False,
    ) -> EvalRunResult:
        """Run every case against ``base_url``.

        ``fake`` says the runner behind ``base_url`` is the fake model, whose
        canned replies no grader can meaningfully judge (ADR-0055). It is not a
        scoring mode: the scorer is not consulted at all on a completed fake turn.

        Each case is run :attr:`SampleConfig.n` times and the per-sample verdicts
        are reduced to one by the configured aggregation policy (#332). With the
        default ``n=1`` the case runs once and its result is returned unchanged.
        """
        results: list[EvalCaseResult] = []
        for case in suite.cases:
            samples = [
                await self._sample_case(case, base_url, token, model=model, fake=fake)
                for _ in range(self._samples.n)
            ]
            results.append(aggregate(case.id, samples, self._samples))
        return EvalRunResult(
            version=version, suite=suite.name, results=results, model=model
        )

    async def _sample_case(
        self,
        case: EvalCase,
        base_url: str,
        token: str | None,
        *,
        model: str | None,
        fake: bool,
    ) -> EvalCaseResult:
        """Run one sample of a case: establish isolation, then run and score it.

        Isolation is re-established before *every* sample (not once per case): two
        samples of the same fresh-conversation case must each start clean, or the
        second would inherit the first's turn and the samples would not be
        independent draws. A ``shared_history`` case still skips the reset and
        chains, matching its per-case semantics.
        """
        if not case.shared_history:
            isolation_error = await self._isolate(base_url, token)
            if isolation_error is not None:
                # Could not establish isolation: fail the sample rather than run
                # it against a possibly-leaked conversation (a false green is
                # exactly what #550 removes). One bad reset never aborts the
                # suite, matching the per-case error handling in _run_case.
                return EvalCaseResult(
                    case_id=case.id,
                    outcome=EvalOutcome.FAIL,
                    output="",
                    latency_ms=0.0,
                    error=isolation_error,
                )
        return await self._run_case(case, base_url, token, model=model, fake=fake)

    async def _isolate(self, base_url: str, token: str | None) -> str | None:
        """Reset the runner's conversation; return an error string on failure.

        None on success. A failed reset means the fresh-conversation guarantee
        could not be established, so the caller fails the case instead of running
        it against a leaked history.
        """
        try:
            await self._runner.reset(base_url, token)
        except (RunnerError, aiohttp.ClientError, TimeoutError) as exc:
            return f"failed to reset conversation for isolation: {exc}"
        return None

    async def _run_case(
        self,
        case: EvalCase,
        base_url: str,
        token: str | None = None,
        *,
        model: str | None = None,
        fake: bool = False,
    ) -> EvalCaseResult:
        start = time.monotonic()
        event = Event(type="eval_case", text=case.input, user="eval", ts="0")
        parts: list[str] = []
        trajectory: list[str] = []
        final_text: str | None = None
        final_status: SessionStatus | None = None
        input_tokens: int | None = None
        output_tokens: int | None = None
        error_detail: str | None = None
        try:
            turn = await self._runner.start_turn(base_url, event, token=token)
            async with turn:
                async for frame in turn:
                    if isinstance(frame, TextDelta):
                        parts.append(frame.text)
                    elif isinstance(frame, ToolNote):
                        # The tool-call trajectory the scorer seam can match on.
                        if frame.tool is not None:
                            trajectory.append(frame.tool)
                    elif isinstance(frame, Final):
                        final_text = frame.text
                        final_status = frame.status
                        # Token usage the runner stamped on the successful final
                        # (#390); priced into cost_usd below for a graded turn.
                        input_tokens = frame.input_tokens
                        output_tokens = frame.output_tokens
                    elif isinstance(frame, ErrorEvent):
                        error_detail = frame.classification or frame.message
        except (RunnerError, aiohttp.ClientError, TimeoutError) as exc:
            return EvalCaseResult(
                case_id=case.id,
                outcome=EvalOutcome.FAIL,
                output="",
                latency_ms=_elapsed_ms(start),
                # The route the turn actually took, kept rather than discarded so a
                # consumer can diff behavior and not just pass-rate.
                trajectory=tuple(trajectory),
                # A bare asyncio.TimeoutError stringifies to "", which would
                # otherwise read downstream as a completed turn; keep it non-empty.
                error=str(exc) or type(exc).__name__,
                # Attribute whatever usage the turn stamped before it failed
                # (#854); None on a fake run or when no Final was seen.
                cost_usd=None if fake else cost_usd(model, input_tokens, output_tokens),
            )

        output = final_text if final_text is not None else "".join(parts)
        # A runner-reported failure (budget/model/runner error) fails the case
        # regardless of the grader, so a classified-failure final whose text
        # happens to contain the expected string can never turn a PR check green.
        if final_status is SessionStatus.CLASSIFIED_FAILURE:
            return EvalCaseResult(
                case_id=case.id,
                outcome=EvalOutcome.FAIL,
                output=output,
                latency_ms=_elapsed_ms(start),
                # The route the turn actually took, kept rather than discarded so a
                # consumer can diff behavior and not just pass-rate.
                trajectory=tuple(trajectory),
                error=error_detail or "runner reported a classified failure",
                # A classified-failure turn (budget/model error) can burn heavy
                # input tokens before it fails -- attribute that cost (#854).
                cost_usd=None if fake else cost_usd(model, input_tokens, output_tokens),
            )
        # Gate on the case's expected terminal status. A case asserts its terminal
        # status (default ``done``, or ``awaiting-approval`` for a gate-blocked
        # case), so a turn that ends in any other status -- or never produced a
        # Final at all -- is not a passing answer even if its text matches the
        # grader. This mirrors the CLI's ``expect_status`` gate so the platform
        # promotion gate keeps skill/local/cluster parity. ``final_status`` may be
        # None and the two enums are distinct, so compare defensively by value.
        expected = case.expect_status
        if final_status is None or final_status.value != expected.value:
            return EvalCaseResult(
                case_id=case.id,
                outcome=EvalOutcome.FAIL,
                output=output,
                latency_ms=_elapsed_ms(start),
                # The route the turn actually took, kept rather than discarded so a
                # consumer can diff behavior and not just pass-rate.
                trajectory=tuple(trajectory),
                error=f"turn ended {final_status}, expected status {expected.value!r}",
                # A wrong-terminal-status turn still ran the model and stamped
                # usage on its Final -- attribute that cost too (#854).
                cost_usd=None if fake else cost_usd(model, input_tokens, output_tokens),
            )
        # The turn completed, which on the fake tier is the whole assertion: the
        # fake answers from a canned script, so grading its text measures the
        # script, not the agent. Returning before the scorer is what makes that
        # literal -- no grader runs at all -- and the non-graded outcome keeps the
        # row out of every pass-rate rather than fabricating one. This sits AFTER
        # the gates above deliberately: a fake turn that never completed means the
        # plumbing is genuinely broken, and catching that is this tier's only job.
        if fake:
            return EvalCaseResult(
                case_id=case.id,
                outcome=EvalOutcome.PLUMBING_OK,
                output=output,
                latency_ms=_elapsed_ms(start),
                # The route the turn actually took, kept rather than discarded so a
                # consumer can diff behavior and not just pass-rate.
                trajectory=tuple(trajectory),
                # No cost_usd on purpose: the fake tier has no real spend, so
                # pricing it would fabricate a cost (#854).
            )
        # The turn completed cleanly, so a negative verdict is the scorer saying
        # "no", not a runner/turn error -- keep `error` reserved for the latter
        # (exceptions, classified failures, incomplete turns handled above).
        verdict = self._scorer.score(case, output, trajectory)
        return EvalCaseResult(
            case_id=case.id,
            outcome=EvalOutcome.PASS if verdict.passed else EvalOutcome.FAIL,
            output=output,
            latency_ms=_elapsed_ms(start),
            # The route the turn actually took, kept rather than discarded so a
            # consumer can diff behavior and not just pass-rate.
            trajectory=tuple(trajectory),
            detail=verdict.detail,
            # Real token usage x model pricing (#390). None when the model is
            # unpriced or the runner reported no usage, so the matrix omits this
            # case from the model's cost rollup rather than counting it as free.
            cost_usd=cost_usd(model, input_tokens, output_tokens),
        )


def _elapsed_ms(start: float) -> float:
    return round((time.monotonic() - start) * 1000, 2)
