"""Eval endpoints (K1): the matrix grid and the PR-check report callback."""

from aci_protocol import EvalJob
from fastapi import APIRouter, Depends, HTTPException, status

from .. import crud
from ..auth import require_api_key
from ..config import get_settings
from ..deps import EvalQueueDep, GitHubReporterDep, LangfuseDep, SessionDep
from ..evalqueue import now_iso
from ..evals import build_matrix
from ..github_checks import GitHubReportError
from ..models import Environment
from ..schemas import (
    EvalMatrix,
    EvalReportResult,
    EvalTriggerRequest,
    EvalTriggerResult,
)
from ..wirebody import EvalReportBody

router = APIRouter(prefix="/evals", tags=["evals"], dependencies=[Depends(require_api_key)])


@router.post("/trigger", response_model=EvalTriggerResult)
async def trigger_eval(
    body: EvalTriggerRequest, session: SessionDep, queue: EvalQueueDep
) -> EvalTriggerResult:
    # On-demand sibling of the git-push eval fan-out (gitflow.process_push):
    # enqueue the SAME EvalJob onto curie:evals, minus the push-only
    # gate. This does NOT parse the eval-case format (worker/CLI concern); it
    # only resolves and emits agent_id/version_id/sha/suite/bundle_ref.
    agent = await crud.get_agent(session, body.agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")

    if body.version_id is not None:
        version = await crud.get_version(session, body.version_id)
        if version is None or version.agent_id != agent.id:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "version not found for this agent")
        sha = version.commit_sha
    else:
        deployment = await crud.get_active_deployment(session, agent.id, Environment.dev)
        if deployment is None:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                "agent has no active dev deployment to evaluate",
            )
        version = await crud.get_version(session, deployment.version_id)
        if version is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "deployed version not found")
        sha = version.commit_sha or deployment.commit_sha

    if sha is None:
        # A version with no commit sha cannot be keyed in eval traces; this is a
        # caller/data problem, surfaced as a clean 4xx rather than a 500 on the
        # required-string EvalJob.sha field.
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "resolved version has no commit sha; cannot run eval",
        )

    if version.bundle_ref is None:
        # The worker reads eval cases from the built bundle, so a version whose
        # bundle was never built has nothing to evaluate. Reject up front rather
        # than enqueuing an unrunnable job (mirrors the missing-sha guard above).
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            f"version {version.id} has no built bundle; nothing to evaluate",
        )

    suite = body.suite or get_settings().eval_default_suite
    stream_id = await queue.enqueue(
        EvalJob(
            agent_id=agent.id,
            version_id=version.id,
            sha=sha,
            suite=suite,
            bundle_ref=version.bundle_ref,
            target_url=body.target_url,
            model=body.model,
            requested_at=now_iso(),
        )
    )
    return EvalTriggerResult(
        stream_id=stream_id,
        agent_id=agent.id,
        version_id=version.id,
        sha=sha,
        suite=suite,
        bundle_ref=version.bundle_ref,
        model=body.model,
    )


@router.get("/matrix", response_model=EvalMatrix)
async def eval_matrix(lf: LangfuseDep, suite: str, versions: int = 5) -> EvalMatrix:
    # Filtered by SUITE, the real dimension on eval traces. There is
    # deliberately no agent filter: eval traces are tagged by suite + version,
    # not agent, so an agent param would be dead. Do not re-add one.
    traces = await lf.list_traces_by_tags(["eval", f"suite:{suite}"])
    return build_matrix(traces, suite, versions)


@router.post("/report", response_model=EvalReportResult)
async def report_eval(report: EvalReportBody, reporter: GitHubReporterDep) -> EvalReportResult:
    # The eval Job (or the fan-out consumer) posts its rollup here on completion;
    # the API turns it into a GitHub commit status. Posting twice is harmless
    # (GitHub statuses are last-write-wins per context), so no dedupe.
    try:
        state = await reporter.report_eval(
            report.repo_full_name,
            report.sha,
            report.passed_count,
            report.total,
            report.target_url,
        )
    except GitHubReportError as exc:
        # A GitHub rejection is a caller/input problem (unknown repo or commit,
        # bad token), not an API server fault: surface it as a 4xx with the
        # upstream detail instead of a bare 500. A GitHub 404 means the repo or
        # commit does not exist; other 4xx rejections map to 422; only a genuine
        # GitHub server fault (5xx) stays a 5xx (502 Bad Gateway).
        if exc.status_code == status.HTTP_404_NOT_FOUND:
            raise HTTPException(
                status.HTTP_404_NOT_FOUND,
                f"repo or commit not found on GitHub: {report.repo_full_name}@{report.sha}",
            ) from exc
        if 400 <= exc.status_code < 500:
            raise HTTPException(
                status.HTTP_422_UNPROCESSABLE_ENTITY,
                f"GitHub rejected the commit-status post ({exc.status_code}): {exc.detail}",
            ) from exc
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            f"GitHub commit-status API error ({exc.status_code})",
        ) from exc
    return EvalReportResult(state=state, sha=report.sha)
