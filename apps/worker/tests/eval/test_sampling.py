"""Multi-sample aggregation: majority vote / pass@k over per-sample results (#332)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from curie_api.evals import _completed
from curie_worker.eval.models import EvalCaseResult, EvalOutcome
from curie_worker.eval.sampling import AggregationPolicy, SampleConfig, aggregate

_VECTOR_PATH = Path(__file__).resolve().parents[4] / "tests" / "vectors" / "eval-sampling.json"


def _result(
    outcome: EvalOutcome,
    *,
    output: str = "",
    latency: float = 1.0,
    cost: float | None = None,
    error: str | None = None,
) -> EvalCaseResult:
    return EvalCaseResult(
        case_id="c",
        outcome=outcome,
        output=output,
        latency_ms=latency,
        cost_usd=cost,
        error=error,
    )


def _samples(passes: int, fails: int) -> list[EvalCaseResult]:
    return (
        [_result(EvalOutcome.PASS, output="ok") for _ in range(passes)]
        + [_result(EvalOutcome.FAIL, output="no") for _ in range(fails)]
    )


def test_single_sample_is_returned_unchanged() -> None:
    # n=1 must be a bit-for-bit no-op: the exact pre-#332 result.
    one = _result(EvalOutcome.PASS, output="answer", latency=12.5, cost=0.5)
    assert aggregate("c", [one], SampleConfig(n=1)) is one


def test_majority_two_of_three_is_green() -> None:
    agg = aggregate("c", _samples(2, 1), SampleConfig(n=3, policy=AggregationPolicy.MAJORITY))
    assert agg.passed is True
    assert agg.error is None
    assert agg.output == "ok"  # representative is a passing sample


def test_majority_one_of_three_is_red_and_reports_variance() -> None:
    agg = aggregate("c", _samples(1, 2), SampleConfig(n=3, policy=AggregationPolicy.MAJORITY))
    assert agg.passed is False
    # #857: a graded FAIL that completed must not stuff variance into `error`.
    assert agg.error is None
    assert agg.variance == "1/3 samples passed (majority)"
    assert agg.samples == 3
    assert agg.passes == 1
    assert agg.policy == "majority"
    assert agg.output == "no"  # representative is a failing sample


def test_majority_tie_fails_deny_by_default() -> None:
    agg = aggregate("c", _samples(1, 1), SampleConfig(n=2, policy=AggregationPolicy.MAJORITY))
    assert agg.passed is False  # 1-of-2 is not a strict majority


def test_pass_at_k_one_of_three_is_green() -> None:
    agg = aggregate(
        "c", _samples(1, 2), SampleConfig(n=3, policy=AggregationPolicy.PASS_AT_K, k=1)
    )
    assert agg.passed is True


def test_pass_at_k_threshold_not_met_is_red() -> None:
    agg = aggregate(
        "c", _samples(1, 2), SampleConfig(n=3, policy=AggregationPolicy.PASS_AT_K, k=2)
    )
    assert agg.passed is False
    assert agg.error is None
    assert agg.variance is not None and "pass@2" in agg.variance


def test_summed_cost_across_samples() -> None:
    samples = [
        _result(EvalOutcome.PASS, cost=0.10),
        _result(EvalOutcome.PASS, cost=0.20),
        _result(EvalOutcome.FAIL, cost=None),
    ]
    agg = aggregate("c", samples, SampleConfig(n=3))
    assert agg.cost_usd == pytest.approx(0.30)
    assert agg.latency_ms == pytest.approx(3.0)


def test_cost_none_when_no_sample_reported_cost() -> None:
    agg = aggregate("c", _samples(2, 1), SampleConfig(n=3))
    assert agg.cost_usd is None


def test_all_plumbing_samples_stay_non_graded() -> None:
    samples = [_result(EvalOutcome.PLUMBING_OK, output="all done") for _ in range(3)]
    agg = aggregate("c", samples, SampleConfig(n=3))
    assert agg.outcome is EvalOutcome.PLUMBING_OK
    assert agg.passed is None
    assert agg.error is None


def test_config_rejects_nonpositive_n_and_k() -> None:
    with pytest.raises(ValueError):
        SampleConfig(n=0)
    with pytest.raises(ValueError):
        SampleConfig(k=0)


def test_effective_k_clamps_to_n() -> None:
    assert SampleConfig(n=3, k=5).effective_k == 3


def test_shared_eval_sampling_vectors() -> None:
    """Python and Rust must agree on every vector in tests/vectors/eval-sampling.json."""
    payload = json.loads(_VECTOR_PATH.read_text())
    for vector in payload["vectors"]:
        samples = [
            _result(
                EvalOutcome(sample["outcome"]),
                output=sample["output"],
                error=sample["error"],
            )
            for sample in vector["samples"]
        ]
        config = SampleConfig(
            n=vector["n"],
            policy=AggregationPolicy(vector["policy"]),
            k=vector["k"],
        )
        agg = aggregate("c", samples, config)
        assert agg.outcome is EvalOutcome(vector["aggregate_outcome"]), vector["name"]
        assert agg.error == vector["error"], vector["name"]
        assert agg.variance == vector["variance"], vector["name"]
        assert agg.output == vector["representative_output"], vector["name"]
        if vector["identity"]:
            assert agg is samples[0], vector["name"]
        else:
            assert agg.samples == vector["n"], vector["name"]
            assert agg.passes == vector["passes"], vector["name"]
            if vector["aggregate_outcome"] != "plumbing_ok":
                assert agg.policy == vector["policy"], vector["name"]


def test_graded_fail_aggregate_is_completed_through_api_completed() -> None:
    """#857: assert the invariant through `_completed`, not by reading `error`.
    A 1/3 majority FAIL whose samples completed must count as a completed turn."""
    agg = aggregate("identity", _samples(1, 2), SampleConfig(n=3))
    trace = {
        "metadata": {
            "outcome": agg.outcome.value,
            "error": agg.error,
        }
    }
    assert _completed(trace) is True


def test_all_incomplete_samples_keep_error() -> None:
    """When no sample completed, `error` stays set so ADR-0068 can still
    distinguish never-answered from a 0% graded fail."""
    samples = [
        _result(EvalOutcome.FAIL, error="runner reported a classified failure")
        for _ in range(3)
    ]
    agg = aggregate("c", samples, SampleConfig(n=3))
    assert agg.passed is False
    assert agg.error is not None and "0/3 samples passed" in agg.error
    assert agg.variance == "0/3 samples passed (majority)"


def test_sample_config_from_env_defaults_to_one_sample() -> None:
    from curie_worker.eval.run import sample_config_from_env

    config = sample_config_from_env({})
    assert config == SampleConfig(n=1, policy=AggregationPolicy.MAJORITY, k=1)


def test_sample_config_from_env_reads_curie_eval_keys() -> None:
    from curie_worker.eval.run import sample_config_from_env

    config = sample_config_from_env(
        {
            "CURIE_EVAL_SAMPLES": "3",
            "CURIE_EVAL_AGGREGATION": "pass_at_k",
            "CURIE_EVAL_PASS_AT_K": "2",
        }
    )
    assert config == SampleConfig(n=3, policy=AggregationPolicy.PASS_AT_K, k=2)
