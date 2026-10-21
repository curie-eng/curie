"""LangfuseEvalRecorder against the REAL compose Langfuse (never mocked).

Records a run's per-case results and reads them back through Langfuse's public
API, filtered by the run's unique version tag (so the read-back sees only this
run's traces). Langfuse v3 ingestion is asynchronous (queued -> ClickHouse), so
the read-back polls with a budget rather than expecting immediate consistency.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

import httpx
import pytest
from curie_worker.eval import (
    EvalCaseResult,
    EvalOutcome,
    EvalRunResult,
    LangfuseEvalRecorder,
)
from curie_worker.eval.recorder import SCORE_NAME

_LF_HOST = os.environ.get("TEST_LANGFUSE_HOST", "http://localhost:23000")
_LF_PK = os.environ.get("TEST_LANGFUSE_PUBLIC_KEY", "pk-lf-curie-dev")
_LF_SK = os.environ.get("TEST_LANGFUSE_SECRET_KEY", "sk-lf-curie-dev")

# One bound for every wait on Langfuse's asynchronous ingestion, shared so the
# trace poll and the score poll cannot drift apart: they wait on the same path.
_INGEST_POLL_ATTEMPTS = 40


async def _traces_for_version(client: httpx.AsyncClient, version: str) -> list[dict[str, Any]]:
    resp = await client.get(
        f"{_LF_HOST}/api/public/traces",
        params={"tags": f"version:{version}"},
        auth=(_LF_PK, _LF_SK),
    )
    if resp.status_code != 200:
        return []
    data: list[dict[str, Any]] = resp.json().get("data", [])
    return data


async def _score_for_trace(client: httpx.AsyncClient, trace_id: str) -> float | None:
    """One immediate read. ``None`` means "not there YET" as often as "not there".

    Two distinct not-yet states collapse into the same ``None``: the trace itself
    may not be queryable (non-200), or it may be queryable while its score has
    not ingested. Both are why callers must poll rather than read once.
    """
    resp = await client.get(f"{_LF_HOST}/api/public/traces/{trace_id}", auth=(_LF_PK, _LF_SK))
    if resp.status_code != 200:
        return None
    for score in resp.json().get("scores") or []:
        if score.get("name") == SCORE_NAME:
            return float(score["value"])
    return None


async def _score_for_trace_eventually(
    client: httpx.AsyncClient, trace_id: str, *, attempts: int = _INGEST_POLL_ATTEMPTS
) -> float | None:
    """Poll one trace's ``eval_pass`` score until it ingests (#1165).

    Traces and scores ingest through Langfuse SEPARATELY, so a trace can be
    queryable before its score is. The test that reads these already polled for
    the traces and then read the score once, which fails as ``assert None == 1.0``
    when the two land far enough apart. Observed on CI run 30594997403; a re-run
    of the same commit was green, which is exactly why the source and not the
    frequency is the thing to fix.

    ``is not None`` and never truthiness: a FAILING case scores a legitimate
    ``0.0``, so ``if score:`` would poll it to the bound every run and then
    report the correct value as a timeout.

    Args:
        client: an authenticated httpx client.
        trace_id: the trace whose score to wait for.
        attempts: one-second attempts before giving up. Defaults to the same
            bound the trace poll uses, since the two wait on one ingestion path.

    Returns:
        The score, or ``None`` when it never materialized.
    """
    for _ in range(attempts):
        score = await _score_for_trace(client, trace_id)
        if score is not None:
            return score
        await asyncio.sleep(1)
    return None


def test_empty_run_skips_the_ingestion_post() -> None:
    # A run with no case results must not POST a hollow ingestion batch; it
    # returns [] before touching the network.
    async def go() -> None:
        posts: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            posts.append(request)
            return httpx.Response(200, json={})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            recorder = LangfuseEvalRecorder(
                base_url="http://langfuse.invalid",
                public_key="pk",
                secret_key="sk",
                client=client,
            )
            run = EvalRunResult(version="v-empty", suite="empty-suite", results=[])
            assert await recorder.record(run) == []
        assert posts == []  # no /api/public/ingestion call was made

    asyncio.run(go())


def test_model_dimension_is_tagged_and_in_metadata() -> None:
    # The model dimension (issue #255): a run with a resolved model tags each
    # trace with model:<name> and carries model + per-case cost_usd in metadata,
    # so the matrix can slice pass-rate/cost by model. A run with model=None
    # records neither tag nor a non-null model field.
    async def go() -> None:
        captured: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            import json

            captured.append(json.loads(request.content))
            return httpx.Response(200, json={})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            recorder = LangfuseEvalRecorder(
                base_url="http://langfuse.invalid",
                public_key="pk",
                secret_key="sk",
                client=client,
            )
            run = EvalRunResult(
                version="v1",
                suite="s",
                model="claude-opus-4-8",
                results=[
                    EvalCaseResult(
                        case_id="c1",
                        outcome=EvalOutcome.PASS,
                        output="ok",
                        latency_ms=1.0,
                        cost_usd=0.0021,
                    )
                ],
            )
            await recorder.record(run)

        batch = captured[0]["batch"]
        trace = next(e for e in batch if e["type"] == "trace-create")["body"]
        assert "model:claude-opus-4-8" in trace["tags"]
        assert trace["metadata"]["model"] == "claude-opus-4-8"
        assert trace["metadata"]["cost_usd"] == 0.0021

    asyncio.run(go())


def test_model_none_records_no_model_tag() -> None:
    async def go() -> None:
        captured: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            import json

            captured.append(json.loads(request.content))
            return httpx.Response(200, json={})

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            recorder = LangfuseEvalRecorder(
                base_url="http://langfuse.invalid",
                public_key="pk",
                secret_key="sk",
                client=client,
            )
            run = EvalRunResult(
                version="v1",
                suite="s",
                results=[
                    EvalCaseResult(
                        case_id="c1", outcome=EvalOutcome.PASS, output="ok", latency_ms=1.0
                    )
                ],
            )
            await recorder.record(run)

        trace = next(
            e for e in captured[0]["batch"] if e["type"] == "trace-create"
        )["body"]
        assert not any(t.startswith("model:") for t in trace["tags"])
        assert trace["metadata"]["model"] is None

    asyncio.run(go())


async def _capture_batch(run: EvalRunResult) -> list[dict[str, Any]]:
    """Record ``run`` through a mocked ingestion endpoint and return the batch."""
    captured: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        import json

        captured.append(json.loads(request.content))
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        recorder = LangfuseEvalRecorder(
            base_url="http://langfuse.invalid",
            public_key="pk",
            secret_key="sk",
            client=client,
        )
        await recorder.record(run)
    batch: list[dict[str, Any]] = captured[0]["batch"]
    return batch


def _trace_by_case(batch: list[dict[str, Any]], case_id: str) -> dict[str, Any]:
    return next(
        e["body"]
        for e in batch
        if e["type"] == "trace-create" and e["body"]["metadata"]["case_id"] == case_id
    )


def test_a_plumbing_row_records_no_eval_pass_score() -> None:
    """A non-graded run has no grade, so it gets no ``eval_pass`` score: 0.0 would
    read as a fail that never happened and 1.0 is the false green this change
    exists to kill. Omission is the only honest Langfuse shape. The FAIL row beside
    it proves the suppression is per-row, not a blanket early return."""

    async def go() -> None:
        run = EvalRunResult(
            version="v1",
            suite="s",
            model=None,
            results=[
                EvalCaseResult(
                    case_id="plumb",
                    outcome=EvalOutcome.PLUMBING_OK,
                    output="all done",
                    latency_ms=1.0,
                ),
                EvalCaseResult(
                    case_id="broke",
                    outcome=EvalOutcome.FAIL,
                    output="",
                    latency_ms=1.0,
                    error="turn did not complete",
                ),
            ],
        )
        batch = await _capture_batch(run)

        # Both cases are traced...
        assert len([e for e in batch if e["type"] == "trace-create"]) == 2
        # ...but only the graded (failed) one carries a score.
        scores = [e for e in batch if e["type"] == "score-create"]
        assert len(scores) == 1
        assert scores[0]["body"]["name"] == SCORE_NAME
        assert scores[0]["body"]["value"] == 0.0
        broke_trace_id = _trace_by_case(batch, "broke")["id"]
        assert scores[0]["body"]["traceId"] == broke_trace_id

        plumb = _trace_by_case(batch, "plumb")
        assert plumb["metadata"]["outcome"] == "plumbing_ok"
        # `passed` stays in metadata so an unmigrated reader stays fail-safe: null
        # renders as missing, never as a fabricated pass or fail.
        assert plumb["metadata"]["passed"] is None
        assert "plumbing" in plumb["tags"]
        # A fake run is unlabelled (#606, main's 9a5dc1b): the fake model is never a
        # real model, so no `model:` tag is emitted and the row lands in the
        # matrix's unlabelled column rather than fabricating a model dimension.
        assert not any(t.startswith("model:") for t in plumb["tags"])

    asyncio.run(go())


def test_a_graded_run_scores_every_case_and_is_not_tagged_plumbing() -> None:
    """The real-model half is untouched: a graded run still emits its eval_pass
    score per case and carries no plumbing tag, so the tag means what it says."""

    async def go() -> None:
        run = EvalRunResult(
            version="v1",
            suite="s",
            model="claude-opus-4-8",
            results=[
                EvalCaseResult(
                    case_id="c1", outcome=EvalOutcome.PASS, output="ok", latency_ms=1.0
                ),
                EvalCaseResult(
                    case_id="c2", outcome=EvalOutcome.FAIL, output="no", latency_ms=1.0
                ),
            ],
        )
        batch = await _capture_batch(run)

        scores = {
            e["body"]["traceId"]: e["body"]["value"]
            for e in batch
            if e["type"] == "score-create"
        }
        assert scores[_trace_by_case(batch, "c1")["id"]] == 1.0
        assert scores[_trace_by_case(batch, "c2")["id"]] == 0.0
        for case_id, expected in (("c1", "pass"), ("c2", "fail")):
            trace = _trace_by_case(batch, case_id)
            assert trace["metadata"]["outcome"] == expected
            assert "plumbing" not in trace["tags"]

    asyncio.run(go())


def test_scorer_detail_is_recorded_separately_from_runtime_error() -> None:
    async def go() -> None:
        run = EvalRunResult(
            version="v1",
            suite="trajectory",
            results=[
                EvalCaseResult(
                    case_id="missing",
                    outcome=EvalOutcome.FAIL,
                    output="all done",
                    latency_ms=1.0,
                    detail="no trajectory spec for case 'missing'",
                )
            ],
        )

        batch = await _capture_batch(run)
        metadata = _trace_by_case(batch, "missing")["metadata"]
        assert metadata["detail"] == "no trajectory spec for case 'missing'"
        assert metadata["error"] is None

    asyncio.run(go())


def test_every_trace_records_the_authoritative_run_case_count() -> None:
    async def go() -> None:
        run = EvalRunResult(
            version="v1",
            suite="trajectory",
            stream_id="2000-0",
            results=[
                EvalCaseResult(
                    case_id="ordered",
                    outcome=EvalOutcome.PASS,
                    output="all done",
                    latency_ms=1.0,
                ),
                EvalCaseResult(
                    case_id="missing",
                    outcome=EvalOutcome.FAIL,
                    output="all done",
                    latency_ms=1.0,
                    detail="no trajectory spec for case 'missing'",
                ),
            ],
        )

        batch = await _capture_batch(run)
        traces = [event["body"] for event in batch if event["type"] == "trace-create"]
        assert len(traces) == 2
        assert {trace["metadata"]["case_count"] for trace in traces} == {2}

    asyncio.run(go())


def test_records_per_case_results_and_reads_them_back() -> None:
    async def go() -> None:
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                (await client.get(f"{_LF_HOST}/api/public/health")).raise_for_status()
            except httpx.HTTPError as exc:
                pytest.fail(f"Langfuse not reachable at {_LF_HOST}: {exc}")

            recorder = LangfuseEvalRecorder(
                base_url=_LF_HOST, public_key=_LF_PK, secret_key=_LF_SK, client=client
            )
            version = f"v-{uuid.uuid4().hex[:8]}"
            run = EvalRunResult(
                version=version,
                suite="recorder-test",
                results=[
                    EvalCaseResult(
                        case_id="pass-case", outcome=EvalOutcome.PASS, output="4", latency_ms=1.0
                    ),
                    EvalCaseResult(
                        case_id="fail-case", outcome=EvalOutcome.FAIL, output="x", latency_ms=1.0
                    ),
                ],
            )
            await recorder.record(run)

            # Poll for the async-ingested traces (keyed by the unique version tag).
            traces: list[dict[str, Any]] = []
            for _ in range(_INGEST_POLL_ATTEMPTS):
                traces = await _traces_for_version(client, version)
                if len(traces) >= 2:
                    break
                await asyncio.sleep(1)
            assert len(traces) == 2, f"eval traces did not materialize for {version}: {traces}"

            # Read off the polled traces, not re-fetched: metadata travels in the
            # trace body, so anything the poll above returned already carries it.
            # This needs no wait of its own (#1165's second done-when).
            passed_by_name = {t["name"]: (t.get("metadata") or {}).get("passed") for t in traces}
            assert passed_by_name == {
                "eval:recorder-test:pass-case": True,
                "eval:recorder-test:fail-case": False,
            }

            # Each trace carries its eval_pass score matching pass/fail. Polled,
            # because a materialized trace does NOT imply a materialized score
            # (#1165): scores ingest on their own path and can land later.
            for trace in traces:
                expected = 1.0 if (trace.get("metadata") or {}).get("passed") else 0.0
                actual = await _score_for_trace_eventually(client, trace["id"])
                assert actual is not None, (
                    f"score {SCORE_NAME!r} never materialized for trace {trace['id']} "
                    f"({trace['name']}) after {_INGEST_POLL_ATTEMPTS}s; the trace itself "
                    "was already queryable, so this is score ingestion lagging"
                )
                assert actual == expected, (
                    f"trace {trace['id']} ({trace['name']}) scored {actual}, expected {expected}"
                )

    asyncio.run(go())
