"""Langfuse read proxy powering the Runs view."""

import re
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, status

from ..auth import require_api_key
from ..deps import LangfuseDep
from ..evalcase import trace_to_eval_case
from ..langfuse import build_tree, hoist_approval_decision, hoist_sandbox_id
from ..metrics import agent_trace_filter
from ..schemas import EvalCaseOut, TraceTree

router = APIRouter(
    prefix="/langfuse",
    tags=["langfuse"],
    dependencies=[Depends(require_api_key)],
)

# Trace ids reach Langfuse's URL path (`/api/public/traces/{id}`), so restrict
# them to the safe id charset (OTel hex, UUIDs, cuids) — no `/`, `.`, or `..`
# that could traverse into other upstream paths.
_TRACE_ID_RE = re.compile(r"\A[A-Za-z0-9_-]{1,128}\Z")

# Observations can land before the trace row is readable. A 404 from the
# trace GET is the same not-ready condition as an empty observation list.
_TRACE_NOT_READY = "trace has no observations yet"


async def _trace_or_not_ready(lf: LangfuseDep, trace_id: str) -> dict[str, Any]:
    try:
        return await lf.get_trace(trace_id)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != status.HTTP_404_NOT_FOUND:
            raise
        raise HTTPException(status.HTTP_404_NOT_FOUND, _TRACE_NOT_READY) from exc


@router.get("/traces")
async def list_traces(
    lf: LangfuseDep, limit: int = 20, agent_id: str | None = None
) -> list[dict[str, object]]:
    # With agent_id, filter to that agent's runs by the trace-name token
    # (agent-<id>); without it, list all recent traces.
    name_contains = agent_trace_filter(agent_id) if agent_id else None
    return await lf.list_traces(limit, name_contains)


@router.get("/traces/{trace_id}", response_model=TraceTree)
async def get_trace(trace_id: str, lf: LangfuseDep) -> TraceTree:
    if not _TRACE_ID_RE.match(trace_id):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, "invalid trace id"
        )
    observations = await lf.get_observations(trace_id)
    if not observations:
        raise HTTPException(status.HTTP_404_NOT_FOUND, _TRACE_NOT_READY)
    trace = await _trace_or_not_ready(lf, trace_id)
    return TraceTree(
        trace=trace,
        tree=build_tree(observations),
        sandbox_id=hoist_sandbox_id(trace, observations),
        approval_decision=hoist_approval_decision(trace, observations),
    )


@router.post("/traces/{trace_id}/eval-case", response_model=EvalCaseOut)
async def promote_trace_to_eval_case(trace_id: str, lf: LangfuseDep) -> EvalCaseOut:
    """Promote a real trace into an anonymized, runnable eval case (#259).

    Reads the trace + its observations, extracts the conversation input/output,
    anonymizes them (emails, Slack ids, phone numbers, credential-shaped tokens),
    and emits an EvalCase conforming to the frozen eval-case format. 404s when the
    trace has no observations yet, matching the read path.
    """
    if not _TRACE_ID_RE.match(trace_id):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "invalid trace id")
    observations = await lf.get_observations(trace_id)
    if not observations:
        raise HTTPException(status.HTTP_404_NOT_FOUND, _TRACE_NOT_READY)
    trace = await _trace_or_not_ready(lf, trace_id)
    return trace_to_eval_case(trace_id, trace, observations)
