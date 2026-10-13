"""GitLab CI behind the code host port (#4399 decisions 18 and 19).

Checks are read on exactly one head commit. The pipelines counted are those
with ``sha=<head>``, plus the pipelines of the merge requests that contain the
head (``GET /repository/commits/<head>/merge_requests``) whose sha is the head,
or that run on ``refs/merge-requests/<iid>/merge`` on a merge commit whose
parents include the head (a merged results pipeline). Anything else is left to
`CiRollup.on_head` to drop, so a green run on another commit never passes this
one.

Jobs (``include_retried=false``) are native ``RUN`` checks keyed
`check_run_key` of their name, newest per name winning. A commit status no job
accounts for was posted by an external system and is a ``STATUS`` check keyed
`status_key` of its name, newest per name winning. A GitLab state word the
decision does not name waits as PENDING, never passes.
"""

from __future__ import annotations

import logging
from collections.abc import Collection, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

from curie_telemetry.redact import redact_text

from curie_api.forges.errors import ForgeError, NotFound, Unavailable
from curie_api.forges.gitlab.transport import GitLabTransport, encode_segment, parse_time
from curie_api.forges.types import (
    CheckSource,
    CheckState,
    CiDiagnostic,
    CiRollup,
    NormalizedCheck,
    RerunAttempt,
    RerunJob,
    RerunObserver,
    RerunOutcome,
    RerunRecord,
    check_run_key,
    status_key,
)

logger = logging.getLogger(__name__)

# Decision 18. ``failed`` with ``allow_failure`` is NEUTRAL (see `check_state`).
STATES: Mapping[str, CheckState] = {
    "success": CheckState.SUCCESS,
    "failed": CheckState.FAILURE,
    "canceled": CheckState.CANCELLED,
    "skipped": CheckState.SKIPPED,
    "manual": CheckState.SKIPPED,
    "created": CheckState.PENDING,
    "waiting_for_resource": CheckState.PENDING,
    "preparing": CheckState.PENDING,
    "pending": CheckState.PENDING,
    "running": CheckState.PENDING,
    "scheduled": CheckState.PENDING,
}
# Decision 19: the excerpt is the last 200 lines of the job's trace.
TRACE_LINES = 200
EXCERPT_MAX_CHARS = 6_000
_EPOCH = datetime.fromtimestamp(0, UTC)
_FAILING = frozenset({CheckState.FAILURE, CheckState.CANCELLED})
# A definitive refusal of a retry; asking again will not change it.
_REFUSED = frozenset({400, 401, 403, 404, 405, 409, 422})


def check_state(word: str, *, allow_failure: bool) -> CheckState:
    if word == "failed" and allow_failure:
        return CheckState.NEUTRAL
    state = STATES.get(word)
    if state is None:
        logger.warning("GitLab CI state %r read as pending", word)
        return CheckState.PENDING
    return state


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _newest(items: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """The newest item per name, by creation time then id."""

    newest: dict[str, tuple[tuple[datetime, int], dict[str, Any]]] = {}
    for item in items:
        name = item.get("name")
        if not isinstance(name, str) or type(item.get("id")) is not int:
            continue
        created = parse_time(item.get("created_at"))
        key = (created or _EPOCH, item["id"])
        if name not in newest or key > newest[name][0]:
            newest[name] = (key, item)
    return {name: item for name, (_, item) in newest.items()}


class GitLabCi:
    """CI reads and reruns on one GitLab project."""

    def __init__(self, transport: GitLabTransport, *, project_id: str) -> None:
        self._transport = transport
        self._project = f"/projects/{project_id}"

    async def _merge_result_counts(self, sha: str, head_sha: str) -> bool:
        """A merged results pipeline counts only when its merge commit's parents
        include the head; an unreadable commit counts for nothing."""

        try:
            commit = await self._transport.get_object(
                f"{self._project}/repository/commits/{encode_segment(sha)}"
            )
        except NotFound:
            return False
        parents = commit.get("parent_ids")
        return isinstance(parents, list) and head_sha in parents

    async def _pipelines(self, head_sha: str) -> list[dict[str, Any]]:
        counted: dict[int, dict[str, Any]] = {}
        for pipeline in await self._transport.list_all(
            f"{self._project}/pipelines", {"sha": head_sha}
        ):
            if pipeline.get("sha") == head_sha and type(pipeline.get("id")) is int:
                counted[pipeline["id"]] = pipeline
        merge_requests = await self._transport.list_all(
            f"{self._project}/repository/commits/{encode_segment(head_sha)}/merge_requests"
        )
        for merge_request in merge_requests:
            iid = merge_request.get("iid")
            if type(iid) is not int:
                continue
            merge_ref = f"refs/merge-requests/{iid}/merge"
            for pipeline in await self._transport.list_all(
                f"{self._project}/merge_requests/{iid}/pipelines"
            ):
                pid, sha = pipeline.get("id"), pipeline.get("sha")
                if type(pid) is not int or pid in counted or not isinstance(sha, str):
                    continue
                if sha == head_sha or (
                    pipeline.get("ref") == merge_ref
                    and await self._merge_result_counts(sha, head_sha)
                ):
                    counted[pid] = pipeline
        return list(counted.values())

    async def observe(self, head_sha: str) -> CiRollup:
        """Decision 18: the normalized checks on exactly ``head_sha``."""

        jobs: list[dict[str, Any]] = []
        job_ids: set[int] = set()
        job_names: set[tuple[Any, str]] = set()
        for pipeline in await self._pipelines(head_sha):
            listed = await self._transport.list_all(
                f"{self._project}/pipelines/{pipeline['id']}/jobs", {"include_retried": "false"}
            )
            for job in listed:
                if type(job.get("id")) is not int or not isinstance(job.get("name"), str):
                    continue
                jobs.append(job)
                job_ids.add(job["id"])
                job_names.add((pipeline["id"], job["name"]))
        checks = [_job_check(job, head_sha) for job in _newest(jobs).values()]
        statuses = await self._transport.list_all(
            f"{self._project}/repository/commits/{encode_segment(head_sha)}/statuses",
            {"all": "true"},
        )
        external = [
            status
            for status in statuses
            if status.get("id") not in job_ids
            and (status.get("pipeline_id"), status.get("name")) not in job_names
        ]
        checks.extend(_status_check(status, head_sha) for status in _newest(external).values())
        return CiRollup.on_head(head_sha, tuple(checks))

    async def _trace(self, job_id: str) -> str | None:
        """The redacted last `TRACE_LINES` lines of a job's log, or None when it
        cannot be read now."""

        try:
            text = await self._transport.get_text(f"{self._project}/jobs/{job_id}/trace")
        except ForgeError:
            return None
        return redact_text("\n".join(text.splitlines()[-TRACE_LINES:]))

    async def diagnostics(self, rollup: CiRollup) -> tuple[CiDiagnostic, ...]:
        """Decision 19: each failing job's trace excerpt."""

        found = []
        for check in rollup.checks:
            if check.source is not CheckSource.RUN or check.state not in _FAILING:
                continue
            if check.check_id is None or not check.check_id.isdigit():
                continue
            log = await self._trace(check.check_id)
            found.append(
                CiDiagnostic(
                    check_key=check.key,
                    excerpt=(log or "")[-EXCERPT_MAX_CHARS:],
                    check_id=check.check_id,
                    title=check.name,
                    log=log,
                    log_unavailable=log is None,
                )
            )
        return tuple(found)

    async def rerun(
        self,
        jobs: Sequence[RerunJob],
        *,
        settled: Collection[str],
        on_attempt: RerunObserver | None,
    ) -> RerunRecord:
        """Decision 19: ``POST /jobs/:id/retry``, one unit per job id. A 2xx is
        accepted; a definitive 4xx refused; a rate limit, 5xx or transport
        failure is retried later and ends the call, as does a timeout after the
        request was sent (unconfirmed)."""

        resolved = tuple(replace(job, unit=job.unit or job.check_id) for job in jobs)
        record = RerunRecord(resolved)
        if not resolved:
            return record
        try:
            await self._transport.ensure_ready()
        except Unavailable as unavailable:
            return replace(record, stopped=RerunOutcome.RETRY, reason=str(unavailable))
        units = dict.fromkeys(job.unit for job in resolved if job.unit is not None)
        for unit in units:
            if unit in settled:
                continue
            outcome, reason = await self._retry(unit)
            record = replace(
                record, attempts=(*record.attempts, RerunAttempt(unit, outcome, reason))
            )
            if on_attempt is not None and not await on_attempt(record):
                break
            if outcome in {RerunOutcome.RETRY, RerunOutcome.UNCONFIRMED}:
                break
        return record

    async def _retry(self, unit: str) -> tuple[RerunOutcome, str | None]:
        if not unit.isdigit():
            return RerunOutcome.REFUSED, "rerun_rejected"
        try:
            response = await self._transport.request(
                "POST", f"{self._project}/jobs/{unit}/retry", allow=_REFUSED
            )
        except Unavailable as unavailable:
            reason = str(unavailable)
            if reason == "timeout":
                return RerunOutcome.UNCONFIRMED, reason
            return RerunOutcome.RETRY, reason
        if response.status_code in _REFUSED:
            return RerunOutcome.REFUSED, f"http_{response.status_code}"
        return RerunOutcome.ACCEPTED, None


def _job_check(job: Mapping[str, Any], head_sha: str) -> NormalizedCheck:
    word = _text(job.get("status"))
    name = job["name"]
    return NormalizedCheck(
        key=check_run_key(name),
        state=check_state(word, allow_failure=job.get("allow_failure") is True),
        head_sha=head_sha,
        name=name,
        url=_text(job.get("web_url")) or None,
        source=CheckSource.RUN,
        reported_state=word,
        started_at=parse_time(job.get("started_at")) or parse_time(job.get("created_at")),
        check_id=str(job["id"]),
        native=True,
    )


def _status_check(status: Mapping[str, Any], head_sha: str) -> NormalizedCheck:
    word = _text(status.get("status"))
    name = status["name"]
    return NormalizedCheck(
        key=status_key(name),
        state=check_state(word, allow_failure=status.get("allow_failure") is True),
        head_sha=head_sha,
        name=name,
        url=_text(status.get("target_url")) or None,
        source=CheckSource.STATUS,
        reported_state=word,
        started_at=parse_time(status.get("started_at")) or parse_time(status.get("created_at")),
        check_id=str(status["id"]),
        native=False,
    )
