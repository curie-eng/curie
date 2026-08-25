"""Eval Job entrypoint: run a suite against a runner, record, report pass/fail.

This is what an eval Job runs (the API fans out Jobs per version @ sha on a PR).
It loads a suite from a JSON file, runs it against a runner endpoint, optionally
records the per-case scores to Langfuse (for the eval matrix), prints the
``EvalRunResult`` as JSON (for the PR-check reporter to read), and exits non-zero
if any case failed so the Job / GitHub check reflects the result.

Env:
    CURIE_EVAL_SUITE        path to the suite JSON (EvalSuite shape)
    CURIE_EVAL_TARGET_URL   runner base_url to evaluate against
    CURIE_EVAL_VERSION      version/sha tag to key results by (default "local")
    LANGFUSE_HOST / LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY
                              if all set, per-case scores are recorded to Langfuse
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections.abc import Mapping
from pathlib import Path

import httpx
from curie_telemetry import bootstrap_service_telemetry

from .. import __version__
from ..runner_client import RunnerClient
from .models import EvalRunResult, EvalScorer, EvalSuite
from .recorder import LangfuseEvalRecorder
from .runner import EvalRunner
from .sampling import AggregationPolicy, SampleConfig
from .scorer import Scorer


def load_suite(path: str | Path) -> EvalSuite:
    return EvalSuite.model_validate_json(Path(path).read_text())


async def run_eval_suite(
    suite: EvalSuite,
    *,
    base_url: str,
    version: str,
    recorder: LangfuseEvalRecorder | None = None,
    token: str | None = None,
    model: str | None = None,
    fake: bool = False,
    samples: SampleConfig | None = None,
    scorer: Scorer | None = None,
    stream_id: str | None,
) -> EvalRunResult:
    """Run a suite against a runner endpoint and, if configured, record it.

    ``model`` is the model id the suite is being run under; it is threaded onto
    the ``EvalRunResult`` so the recorder can tag the model dimension and the
    eval matrix can slice pass-rate/cost by model. ``fake`` says that runner is
    the fake model, whose turns are never graded (ADR-0055). ``samples`` is the
    multi-sample / variance-aware-grading policy (#332); the default (``None`` ->
    ``n=1``) runs each case once, unchanged. ``scorer`` is selected by the run
    layer when an optional trajectory sidecar is present; otherwise the runner
    keeps its ordinary frozen grader scorer.
    """
    async with RunnerClient() as runner:
        result = await EvalRunner(runner, scorer=scorer, samples=samples).run(
            suite,
            base_url=base_url,
            version=version,
            token=token,
            model=model,
            fake=fake,
        )
    scorer_name = EvalScorer.TRAJECTORY if scorer is not None else EvalScorer.GRADER
    result = result.model_copy(update={"stream_id": stream_id, "scorer": scorer_name})
    if recorder is not None:
        await recorder.record(result)
    return result


def _sample_config_from_env(env: Mapping[str, str]) -> SampleConfig:
    """Build the multi-sample policy from the eval Job's env (#332).

    ``CURIE_EVAL_SAMPLES`` (default 1) sets N; ``CURIE_EVAL_AGGREGATION``
    (``majority`` | ``pass_at_k``, default ``majority``) the policy; and
    ``CURIE_EVAL_PASS_AT_K`` (default 1) the pass@k threshold. Absent/unset
    keys yield the backward-compatible ``n=1`` default.
    """
    n = int(env.get("CURIE_EVAL_SAMPLES", "1"))
    policy = AggregationPolicy(env.get("CURIE_EVAL_AGGREGATION", AggregationPolicy.MAJORITY))
    k = int(env.get("CURIE_EVAL_PASS_AT_K", "1"))
    return SampleConfig(n=n, policy=policy, k=k)


async def _main_async(env: Mapping[str, str]) -> int:
    suite = load_suite(env["CURIE_EVAL_SUITE"])
    base_url = env["CURIE_EVAL_TARGET_URL"]
    version = env.get("CURIE_EVAL_VERSION", "local")
    # The model dimension: the model this eval Job's runner is configured with
    # (the same CURIE_MODEL the runner authenticates from). Empty/unset means
    # "model unknown", recorded as no model tag.
    model = env.get("CURIE_MODEL") or None
    samples = _sample_config_from_env(env)

    lf_keys = ("LANGFUSE_HOST", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY")
    if all(k in env for k in lf_keys):
        async with httpx.AsyncClient(timeout=30.0) as client:
            recorder = LangfuseEvalRecorder(
                base_url=env["LANGFUSE_HOST"],
                public_key=env["LANGFUSE_PUBLIC_KEY"],
                secret_key=env["LANGFUSE_SECRET_KEY"],
                client=client,
            )
            result = await run_eval_suite(
                suite,
                base_url=base_url,
                version=version,
                recorder=recorder,
                model=model,
                samples=samples,
                stream_id=None,
            )
    else:
        result = await run_eval_suite(
            suite,
            base_url=base_url,
            version=version,
            model=model,
            samples=samples,
            stream_id=None,
        )

    print(result.model_dump_json())
    # The exit code answers "did anything break", not "did anything pass": a run
    # whose cases were never graded (the fake tier) broke nothing, so failing the
    # Job on it would report a failure that did not happen. A real FAIL -- including
    # a fake turn that never completed -- is still non-zero.
    return 0 if result.completed_without_failure() else 1


def main(env: Mapping[str, str] | None = None) -> None:
    resolved = env if env is not None else os.environ
    telemetry = bootstrap_service_telemetry(
        "curie-worker",
        service_version=__version__,
        logger=logging.getLogger("curie_worker"),
        environ=resolved,
    )
    try:
        exit_code = asyncio.run(_main_async(resolved))
    finally:
        telemetry.shutdown()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
