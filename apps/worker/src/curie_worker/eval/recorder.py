"""Record eval results to Langfuse so the matrix endpoint can read the grid.

Per the architecture (detailed-architecture section 4), eval Jobs write scores to
Langfuse and the eval matrix reads the grid back keyed by version tag. This
recorder exports one root OTLP span per case with output, metadata and the
version and suite tags used by the eval matrix. Each graded case also gets an
``eval_pass`` numeric score (1.0 / 0.0) through the batch ingestion API.
Plumbing cases (ADR-0055) record only the span.

Langfuse ingests both writes asynchronously, so a read-back is eventually consistent.
"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from time import time_ns
from typing import Any

import httpx

from .models import EvalCaseResult, EvalOutcome, EvalRunResult

SCORE_NAME = "eval_pass"


class IngestionError(Exception):
    """Langfuse accepted the ingestion request but rejected one or more events."""


class LangfuseEvalRecorder:
    """Writes completed eval traces and scores to the configured Langfuse project."""

    def __init__(
        self,
        *,
        base_url: str,
        public_key: str,
        secret_key: str,
        client: httpx.AsyncClient,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._auth = (public_key, secret_key)
        self._client = client

    async def record(self, run: EvalRunResult) -> list[str]:
        """Ingest the case results and return their Langfuse trace IDs."""
        if not run.results:
            # No cases: don't POST a hollow ingestion batch (an empty trace shell
            # in Langfuse). Nothing to record, so return before touching the API.
            return []
        timestamp_ns = time_ns()
        now = datetime.fromtimestamp(timestamp_ns / 1_000_000_000, UTC).isoformat()
        scores: list[dict[str, Any]] = []
        spans: list[dict[str, Any]] = []
        trace_ids: list[str] = []
        for result in run.results:
            trace_id = uuid.uuid4().hex
            trace_ids.append(trace_id)
            spans.append(self._trace_span(trace_id, run, result, str(timestamp_ns)))
            if result.outcome is not EvalOutcome.PLUMBING_OK:
                scores.append(self._score_event(trace_id, result, now))

        # Langfuse accepts a score before its trace exists. Submit scores first
        # so a score error does not leave a published trace missing its grade.
        if scores:
            await self._post_scores(scores)
        resp = await self._client.post(
            f"{self._base}/api/public/otel/v1/traces",
            json={"resourceSpans": [{"scopeSpans": [{"spans": spans}]}]},
            headers={"x-langfuse-ingestion-version": "4"},
            auth=self._auth,
        )
        resp.raise_for_status()
        # OTLP can report a partial rejection in an HTTP 200 response.
        partial = resp.json().get("partialSuccess") if resp.content else None
        if partial and int(partial.get("rejectedSpans", 0)):
            raise IngestionError("Langfuse rejected one or more eval observations")
        return trace_ids

    async def _post_scores(self, scores: list[dict[str, Any]]) -> None:
        resp = await self._client.post(
            f"{self._base}/api/public/ingestion",
            json={"batch": scores},
            auth=self._auth,
        )
        resp.raise_for_status()
        errors = resp.json().get("errors") or []
        if errors:
            raise IngestionError(f"Langfuse rejected {len(errors)} eval event(s): {errors}")

    def _trace_span(
        self, trace_id: str, run: EvalRunResult, result: EvalCaseResult, timestamp_ns: str
    ) -> dict[str, Any]:
        # The model dimension is a tag *and* a metadata field, mirroring the
        # version/suite convention: the tag lets the matrix reader filter/slice by
        # model, the metadata field is what build_matrix reads straight off the
        # trace. A run with no resolved model (target_url shortcut) records no
        # model: tag, so its cases land in the matrix's unlabelled column.
        tags = ["eval", f"version:{run.version}", f"suite:{run.suite}"]
        if run.model:
            tags.append(f"model:{run.model}")
        # A non-graded row is tagged so the grid can filter plumbing runs out of a
        # comparison without parsing metadata; the tag means exactly "no grader
        # judged this", never "this failed".
        if result.outcome is EvalOutcome.PLUMBING_OK:
            tags.append("plumbing")
        name = f"eval:{run.suite}:{result.case_id}"
        metadata = {
            "version": run.version,
            "suite": run.suite,
            "model": run.model,
            "stream_id": run.stream_id,
            "scorer": run.scorer.value,
            "case_count": run.total,
            "case_id": result.case_id,
            "outcome": result.outcome.value,
            # Legacy readers use passed; null keeps plumbing cases ungraded.
            "passed": result.passed,
            "latency_ms": result.latency_ms,
            "cost_usd": result.cost_usd,
            "detail": result.detail,
            "error": result.error,
            "samples": result.samples,
            "passes": result.passes,
            "policy": result.policy,
            "variance": result.variance,
        }
        output = json.dumps(result.output)
        # v3 trace reads use trace.output; v4 reads the root observation's output.
        # Keep metadata typed via the documented JSON object attribute.
        # https://langfuse.com/integrations/native/opentelemetry/migration-to-v4
        attributes = [
            {"key": key, "value": {"stringValue": value}}
            for key, value in (
                ("langfuse.trace.name", name),
                ("langfuse.trace.metadata", json.dumps(metadata)),
                ("langfuse.trace.output", output),
                ("langfuse.observation.output", output),
            )
        ]
        attributes.append(
            {
                "key": "langfuse.trace.tags",
                "value": {"arrayValue": {"values": [{"stringValue": tag} for tag in tags]}},
            }
        )
        return {
            "traceId": trace_id,
            "spanId": trace_id[:16],
            "name": name,
            "startTimeUnixNano": timestamp_ns,
            "endTimeUnixNano": timestamp_ns,
            "attributes": attributes,
        }

    def _score_event(self, trace_id: str, result: EvalCaseResult, now: str) -> dict[str, Any]:
        return {
            "id": uuid.uuid4().hex,
            "type": "score-create",
            "timestamp": now,
            "body": {
                "id": uuid.uuid4().hex,
                "traceId": trace_id,
                "name": SCORE_NAME,
                "value": 1.0 if result.passed else 0.0,
                "dataType": "NUMERIC",
            },
        }
