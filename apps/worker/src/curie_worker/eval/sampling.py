"""Multi-sample runs with variance-aware grading (#332).

A single run grades noise: an agent that passes a case 2 times out of 3 is graded
GREEN or RED depending on which run the CI happened to catch. This module is the
*aggregation layer* around the frozen case + scorer: a case is run ``n`` times and
the ``n`` per-sample verdicts are reduced to one, by **majority vote** or
**pass@k**. The eval-case schema and the scorer are untouched -- sampling is a
run-layer policy (like the trajectory-spec mapping), supplied above the port, not
a field on the frozen ``EvalCase``.

Backward compatible by construction: the default is ``n=1``, and the runner
short-circuits a single-sample run to the exact one-shot result it produced
before this module existed. Only ``n>1`` engages aggregation.

Aggregation operates on the per-sample :class:`~.models.EvalCaseResult` list the
runner already produces, so it is a pure, testable reduction with no runner or
network dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from .models import EvalCaseResult, EvalOutcome


class AggregationPolicy(StrEnum):
    """How ``n`` per-sample verdicts reduce to one.

    ``MAJORITY`` -- GREEN when a strict majority of the graded samples passed
    (ties fail, deny-by-default: a 1-of-2 split is not a pass). ``PASS_AT_K`` --
    GREEN when at least ``k`` of the graded samples passed (the eval ``pass@k``:
    with ``k=1`` a case is green if it passed even once, the loosest useful bar).
    """

    MAJORITY = "majority"
    PASS_AT_K = "pass_at_k"


@dataclass(frozen=True)
class SampleConfig:
    """How many times to run each case and how to reduce the verdicts.

    ``n=1`` (the default) is a no-op: the runner returns the single sample
    unchanged, so existing suites behave exactly as before. ``k`` is only read by
    :data:`AggregationPolicy.PASS_AT_K` and is clamped into ``1..n``.
    """

    n: int = 1
    policy: AggregationPolicy = AggregationPolicy.MAJORITY
    k: int = 1

    def __post_init__(self) -> None:
        if self.n < 1:
            raise ValueError(f"samples n must be >= 1, got {self.n}")
        if self.k < 1:
            raise ValueError(f"pass@k k must be >= 1, got {self.k}")

    @property
    def effective_k(self) -> int:
        """``k`` clamped to ``1..n`` -- a k larger than n can never be met."""
        return min(self.k, self.n)


def _representative(samples: list[EvalCaseResult], outcome: EvalOutcome) -> EvalCaseResult:
    """The first sample whose outcome matches the aggregate (fallback: first).

    Used to source the aggregated row's ``output`` so a GREEN aggregate shows a
    passing sample's answer and a RED one shows a failing sample's, rather than an
    arbitrary sample that disagrees with the verdict.
    """
    for sample in samples:
        if sample.outcome is outcome:
            return sample
    return samples[0]


def _summed_cost(samples: list[EvalCaseResult]) -> float | None:
    """Total cost across samples; None only when no sample reported a cost.

    Summing (not averaging) keeps the matrix's per-model rollup counting the real
    dollars ``n`` samples spent -- running a case 3x costs 3x, and the rollup
    should say so.
    """
    known = [s.cost_usd for s in samples if s.cost_usd is not None]
    return sum(known) if known else None


def aggregate(
    case_id: str, samples: list[EvalCaseResult], config: SampleConfig
) -> EvalCaseResult:
    """Reduce ``n`` per-sample results for one case to a single verdict.

    Never called with an empty ``samples`` (the runner always produces at least
    one). A single sample is returned unchanged so ``n=1`` is bit-for-bit the
    pre-#332 behavior. All-plumbing (the fake tier) stays non-graded: there is no
    verdict to vote on.
    """
    if len(samples) == 1:
        return samples[0]

    total_latency = round(sum(s.latency_ms for s in samples), 2)
    cost = _summed_cost(samples)

    # The fake tier is never graded, so an all-plumbing set has no verdict to
    # aggregate -- it stays PLUMBING_OK. (fake runs are per-run, so a set is
    # either all plumbing or none.)
    if all(s.outcome is EvalOutcome.PLUMBING_OK for s in samples):
        rep = samples[0]
        return EvalCaseResult(
            case_id=case_id,
            outcome=EvalOutcome.PLUMBING_OK,
            output=rep.output,
            latency_ms=total_latency,
            detail=rep.detail,
            cost_usd=cost,
            trajectory=rep.trajectory,
        )

    graded = [s for s in samples if s.outcome is not EvalOutcome.PLUMBING_OK]
    passes = sum(1 for s in graded if s.outcome is EvalOutcome.PASS)
    graded_count = len(graded)

    if config.policy is AggregationPolicy.PASS_AT_K:
        green = passes >= config.effective_k
        bar = f"pass@{config.effective_k}"
    else:  # MAJORITY
        green = passes * 2 > graded_count
        bar = "majority"

    outcome = EvalOutcome.PASS if green else EvalOutcome.FAIL
    variance = f"{passes}/{graded_count} samples passed ({bar})"
    representative = _representative(samples, outcome)
    # Variance rides `error` only on a RED aggregate, matching the runner's
    # convention that `error` explains why a case is not green; a flaky-but-green
    # case (e.g. 2/3 under majority) reports GREEN and leaves `error` clear.
    return EvalCaseResult(
        case_id=case_id,
        outcome=outcome,
        output=representative.output,
        latency_ms=total_latency,
        error=None if green else f"variance-aware grading failed: {variance}",
        detail=representative.detail,
        cost_usd=cost,
        # An aggregate of n samples has no single route: the samples may each have
        # taken a different one. Carrying the representative sample's trajectory
        # keeps the route consistent with the `output` and `detail` beside it,
        # rather than inventing a union that no turn actually walked.
        trajectory=representative.trajectory,
    )
