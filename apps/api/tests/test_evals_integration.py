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
from curie_worker.eval.models import EvalCaseResult, EvalOutcome, EvalRunResult
from curie_worker.eval.recorder import LangfuseEvalRecorder


def _stack_up() -> bool:
    try:
        httpx.get(
            f"{get_settings().langfuse_host}/api/public/health", timeout=2.0
        ).raise_for_status()
    except Exception:
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


def test_matrix_reflects_seeded_multi_version_scores(
    client: Any, auth_headers: dict[str, str]
) -> None:
    suite = f"k1matrix-{secrets.token_hex(4)}"
    sha_a, sha_b = "sha" + secrets.token_hex(3), "sha" + secrets.token_hex(3)
    asyncio.run(_seed(suite, sha_a, sha_b))

    deadline = time.time() + 60
    cells: dict[tuple[str, str], str] = {}
    while time.time() < deadline:
        resp = client.get("/evals/matrix", params={"suite": suite}, headers=auth_headers)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        cells = {
            (row["case_id"], cell["version"]): cell["status"]
            for row in body["rows"]
            for cell in row["cells"]
        }
        ready = {(c, v) for c in ("c1", "c2") for v in (sha_a, sha_b)} <= set(cells)
        if ready and all(cells[k] != "missing" for k in cells):
            break
        time.sleep(2)

    assert cells.get(("c1", sha_a)) == "pass"
    assert cells.get(("c2", sha_a)) == "fail"
    assert cells.get(("c1", sha_b)) == "pass"
    assert cells.get(("c2", sha_b)) == "pass"


def test_matrix_requires_api_key(client: Any) -> None:
    assert client.get("/evals/matrix", params={"suite": "x"}).status_code == 401
