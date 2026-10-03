"""Eval matrix against REAL Langfuse, seeded with K1's own recorder.

Records eval results for two versions of a unique suite via
LangfuseEvalRecorder, then reads /evals/matrix back and asserts the grid values.
Ingestion is eventually consistent, so it polls. Skips when the dev stack is not
reachable.
"""

import asyncio
import secrets
import time
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_worker.eval.models import (
    EvalCaseResult,
    EvalOutcome,
    EvalRunResult,
    EvalScorer,
)
from curie_worker.eval.recorder import LangfuseEvalRecorder


def _stack_up() -> bool:
    try:
        httpx.get(
            f"{get_settings().langfuse_host}/api/public/health", timeout=2.0
        ).raise_for_status()
    except Exception:  # noqa: BLE001 - existing broad catch retained
        return False
    return True


pytestmark = pytest.mark.skipif(not _stack_up(), reason="dev stack not reachable")


def _outcome(passed: bool) -> EvalOutcome:
    return EvalOutcome.PASS if passed else EvalOutcome.FAIL


def _result(version: str, suite: str, c1: bool, c2: bool) -> EvalRunResult:
    return EvalRunResult(
        version=version,
        suite=suite,
        results=[
            EvalCaseResult(case_id="c1", outcome=_outcome(c1), output="o", latency_ms=1.0),
            EvalCaseResult(case_id="c2", outcome=_outcome(c2), output="o", latency_ms=1.0),
        ],
    )


async def _seed(suite: str, sha_a: str, sha_b: str) -> None:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=30.0) as http:
        recorder = LangfuseEvalRecorder(
            base_url=settings.langfuse_host,
            public_key=settings.langfuse_public_key,
            secret_key=settings.langfuse_secret_key,
            client=http,
        )
        await recorder.record(_result(sha_a, suite, c1=True, c2=False))
        await recorder.record(_result(sha_b, suite, c1=True, c2=True))


async def _seed_same_sha_runs(suite: str, sha: str) -> None:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=30.0) as http:
        recorder = LangfuseEvalRecorder(
            base_url=settings.langfuse_host,
            public_key=settings.langfuse_public_key,
            secret_key=settings.langfuse_secret_key,
            client=http,
        )
        await recorder.record(
            EvalRunResult(
                version=sha,
                suite=suite,
                stream_id="1000-0",
                scorer=EvalScorer.GRADER,
                results=[
                    EvalCaseResult(
                        case_id="ordered",
                        outcome=EvalOutcome.PASS,
                        output="old run",
                        latency_ms=1.0,
                    )
                ],
            )
        )
        await recorder.record(
            EvalRunResult(
                version=sha,
                suite=suite,
                stream_id="2000-0",
                scorer=EvalScorer.TRAJECTORY,
                results=[
                    EvalCaseResult(
                        case_id="ordered",
                        outcome=EvalOutcome.FAIL,
                        output="new run",
                        latency_ms=1.0,
                        detail="mode=in_order expected=['Read', 'Bash'] observed=['Bash']",
                    )
                ],
            )
        )


def test_matrix_reflects_seeded_multi_version_scores(
    client: Any, auth_headers: dict[str, str]
) -> None:
    suite = f"k1matrix-{secrets.token_hex(4)}"
    sha_a, sha_b = "sha" + secrets.token_hex(3), "sha" + secrets.token_hex(3)
    asyncio.run(_seed(suite, sha_a, sha_b))

    deadline = time.time() + 60
    cells: dict[tuple[str, str], str] = {}
    while time.time() < deadline:
        resp = client.get(
            "/evals/matrix", params={"suite": suite}, headers=auth_headers
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        cells = {
            (row["case_id"], cell["version"]): cell["status"]
            for row in body["rows"]
            for cell in row["cells"]
        }
        ready = {
            (c, v) for c in ("c1", "c2") for v in (sha_a, sha_b)
        } <= set(cells)
        if ready and all(cells[k] != "missing" for k in cells):
            break
        time.sleep(2)

    assert cells.get(("c1", sha_a)) == "pass"
    assert cells.get(("c2", sha_a)) == "fail"
    assert cells.get(("c1", sha_b)) == "pass"
    assert cells.get(("c2", sha_b)) == "pass"


def test_matrix_stream_id_filters_to_one_authoritative_run(
    client: Any, auth_headers: dict[str, str]
) -> None:
    suite = f"stream-matrix-{secrets.token_hex(4)}"
    sha = "sha" + secrets.token_hex(3)
    asyncio.run(_seed_same_sha_runs(suite, sha))

    deadline = time.time() + 60
    unfiltered: dict[str, Any] = {}
    filtered: dict[str, Any] = {}
    while time.time() < deadline:
        unfiltered_response = client.get(
            "/evals/matrix", params={"suite": suite}, headers=auth_headers
        )
        filtered_response = client.get(
            "/evals/matrix",
            params={"suite": suite, "stream_id": "1000-0"},
            headers=auth_headers,
        )
        assert unfiltered_response.status_code == 200, unfiltered_response.text
        assert filtered_response.status_code == 200, filtered_response.text
        unfiltered = unfiltered_response.json()
        filtered = filtered_response.json()
        if unfiltered["rows"] and filtered["rows"]:
            unfiltered_streams = {
                cell["stream_id"]
                for row in unfiltered["rows"]
                for cell in row["cells"]
            }
            filtered_streams = {
                cell["stream_id"]
                for row in filtered["rows"]
                for cell in row["cells"]
            }
            if unfiltered_streams == {"2000-0"} and filtered_streams == {"1000-0"}:
                break
        time.sleep(2)

    unfiltered_cells = unfiltered["rows"][0]["cells"]
    assert len(unfiltered_cells) == 1
    assert unfiltered_cells[0]["stream_id"] == "2000-0"
    assert unfiltered_cells[0]["status"] == "fail"
    assert unfiltered_cells[0]["case_count"] == 1

    filtered_cells = filtered["rows"][0]["cells"]
    assert len(filtered_cells) == 1
    assert filtered_cells[0]["stream_id"] == "1000-0"
    assert filtered_cells[0]["status"] == "pass"
    assert filtered_cells[0]["case_count"] == 1


def test_matrix_requires_api_key(client: Any) -> None:
    assert client.get("/evals/matrix", params={"suite": "x"}).status_code == 401
