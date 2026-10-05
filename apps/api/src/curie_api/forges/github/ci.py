"""Observe CI on a published pull request head through GitHub's Checks and Statuses APIs."""

from __future__ import annotations

import asyncio
import re
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlsplit

import anyio
import httpx
from curie_telemetry.redact import redact_text

from curie_api.config import Settings
from curie_api.github_app import GitHubAppError, GitHubInstallationRefused, credentials_for
from curie_api.repo_full_name import repo_url_path
from curie_api.schemas.workitems import WorkItemCiOut

CHECK_RUNS_PAGE = 100
# One overall bound on a live CI observation: credential acquisition (lock,
# discovery, token mint) and the check-runs request together.
CI_OBSERVATION_DEADLINE_SECONDS = 10.0
# How many credential mints a live CI observation may have in flight across all
# callers. A caller abandoned at the deadline cannot cancel the mint thread, so
# the slot is released by the thread itself when the mint actually finishes;
# until then further callers are refused ``observation_busy`` without spawning
# work, which is what keeps repeated timeouts from accumulating threads and
# lock contention.
CI_CREDENTIAL_SLOTS = 4
# One overall bound on the CI gate's detail observation (#3097): the credential
# mint, check runs, commit statuses and failing-run annotations together.
CI_DETAIL_DEADLINE_SECONDS = 20.0
# Annotations are read for at most this many failing check runs per observation.
CI_DETAIL_ANNOTATED_RUNS = 5
CI_DETAIL_LOGGED_JOBS = 5
CI_JOB_LOG_TAIL_BYTES = 64 * 1024
CI_JOB_LOG_MAX_DECODED_BYTES = 8 * 1024 * 1024
CI_JOB_LOG_MAX_CHARS = 6_000
CI_JOB_LOG_MAX_LINES = 80
CI_JOB_LOG_TIMEOUT_SECONDS = 5.0
# Bound on reading the PR base branch head's checks for pre-existing failures (#4105).
# The base read runs last and only spends what the log deadline leaves.
CI_BASE_READ_TIMEOUT_SECONDS = 5.0
_CI_LOG_HOST = "pipelines.actions.githubusercontent.com"
# GitHub's job log redirect has also been observed on Azure Blob storage.
_CI_AZURE_LOG_HOST = re.compile(r"productionresults[a-z0-9]+\.blob\.core\.windows\.net")
_CI_CREDENTIAL_GUARD = threading.BoundedSemaphore(CI_CREDENTIAL_SLOTS)


_FAILING_CONCLUSIONS = frozenset(
    {"failure", "timed_out", "cancelled", "action_required", "startup_failure"}
)
_PASSING_CONCLUSIONS = frozenset({"success", "neutral", "skipped"})
SHA_RE = re.compile(r"[0-9a-fA-F]{7,64}")
_COMMIT_SHA_RE = re.compile(r"[0-9a-fA-F]{40}")

CiObservation = WorkItemCiOut


def _unavailable(reason: str, head_sha: str | None = None) -> CiObservation:
    return CiObservation(
        state="unavailable",
        reason=reason,
        head_sha=head_sha,
        observed_at=datetime.now(UTC),
    )


_STATUS_REASONS = {
    401: "github_unauthorized",
    403: "github_forbidden",
    404: "github_not_found",
    429: "github_rate_limited",
}


def _verdict(payload: Any) -> str:
    """A CI state, or an ``unavailable`` reason code."""

    if not isinstance(payload, dict):
        return "malformed_response"
    total = payload.get("total_count")
    runs = payload.get("check_runs")
    if not isinstance(total, int) or isinstance(total, bool) or not isinstance(runs, list):
        return "malformed_response"
    if total > CHECK_RUNS_PAGE or total > len(runs):
        return "too_many_check_runs"
    if not runs:
        return "none"
    failing = pending = False
    for run in runs:
        if not isinstance(run, dict):
            return "malformed_response"
        status, conclusion = run.get("status"), run.get("conclusion")
        if status != "completed":
            pending = True
        elif conclusion in _FAILING_CONCLUSIONS:
            failing = True
        elif conclusion == "stale":
            pending = True
        elif conclusion not in _PASSING_CONCLUSIONS:
            return "malformed_response"
    if failing:
        return "failing"
    return "pending" if pending else "passing"


async def observe_ci(
    lineage: Any, work_item: Any, settings: Settings, client: httpx.AsyncClient
) -> CiObservation:
    """Observe CI for the lineage's published head, live and never persisted.

    The installation token lives only in a local here; every failure maps to a
    fixed reason code, and no response text, header or URL is ever echoed. The
    whole observation is bounded by ``CI_OBSERVATION_DEADLINE_SECONDS``; on
    expiry the caller is released with ``unavailable``/``timeout`` (a blocked
    credential thread is abandoned, not awaited).
    """

    head_sha = getattr(lineage, "head_sha", None) if lineage is not None else None
    try:
        return await asyncio.wait_for(
            _observe_ci(lineage, work_item, settings, client),
            timeout=CI_OBSERVATION_DEADLINE_SECONDS,
        )
    except TimeoutError:
        return _unavailable(
            "timeout",
            head_sha if isinstance(head_sha, str) and SHA_RE.fullmatch(head_sha) else None,
        )


class _CredentialPermit:
    """One credential slot, released exactly once by whoever claims it.

    Two parties can end up responsible for the same slot: the worker thread,
    which releases when its mint finishes, and the caller, whose deadline may
    cancel ``run_sync`` before the worker body ever runs (thread capacity
    exhausted), leaving no worker ``finally`` to run at all. ``claim`` makes the
    ownership decision atomic, so the slot is released on every path and never
    twice -- the guard is a ``BoundedSemaphore`` and an over-release raises.
    """

    __slots__ = ("_claimed", "_lock")

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._claimed = False

    def claim(self) -> bool:
        """True exactly once, for the party that owes the release."""

        with self._lock:
            if self._claimed:
                return False
            self._claimed = True
            return True


def _mint_and_release(
    permit: _CredentialPermit,
    resolver: Any,
    repo_full_name: str,
    installation_id: int | None,
) -> tuple[int, str]:
    """Mint an installation token, returning the bound slot when it finishes."""

    owns = permit.claim()
    try:
        minted: tuple[int, str] = resolver.fresh_installation_token(
            repo_full_name, installation_id
        )
        return minted
    finally:
        if owns:
            _CI_CREDENTIAL_GUARD.release()


async def mint_ci_token(
    lineage: Any, work_item: Any, settings: Settings, head_sha: str
) -> tuple[str | None, CiObservation | None]:
    """Mint a CI read token through the bounded credential slots.

    Returns ``(token, None)`` or ``(None, unavailable)``. The token lives only
    in the caller's local; every failure maps to a fixed reason code.
    """

    return await mint_repository_token(
        settings,
        lineage.repo_full_name,
        lineage.github_installation_id or work_item.github_installation_id,
        head_sha,
    )


async def mint_repository_token(
    settings: Settings, repo_full_name: str, installation_id: int | None, head_sha: str | None
) -> tuple[str | None, CiObservation | None]:
    """`mint_ci_token` for a repository named directly; ``None`` rediscovers
    the installation."""

    resolver = credentials_for(settings)
    if not resolver.app_configured:
        return None, _unavailable("app_not_configured", head_sha)
    if not _CI_CREDENTIAL_GUARD.acquire(blocking=False):
        # Every slot is held by a mint that has not finished; refuse now rather
        # than pile another thread onto the repository lock.
        return None, _unavailable("observation_busy", head_sha)
    permit = _CredentialPermit()
    try:
        # abandon_on_cancel: the overall deadline must release the caller even
        # while the credential thread is blocked on the repository lock. A
        # started mint releases the slot itself when its own work ends, so an
        # abandoned mint still holds it until then; if the deadline fires before
        # the worker body ever runs, the caller releases it instead.
        _, token = await anyio.to_thread.run_sync(
            _mint_and_release,
            permit,
            resolver,
            repo_full_name,
            installation_id,
            abandon_on_cancel=True,
        )
    except GitHubInstallationRefused:
        return None, _unavailable("installation_refused", head_sha)
    except (GitHubAppError, ValueError):
        return None, _unavailable("github_error", head_sha)
    finally:
        if permit.claim():
            _CI_CREDENTIAL_GUARD.release()
    return token, None


async def _observe_ci(
    lineage: Any, work_item: Any, settings: Settings, client: httpx.AsyncClient
) -> CiObservation:
    if lineage is None or lineage.pr_number is None:
        return CiObservation(state="not_applicable", reason="no_pull_request")
    head_sha = lineage.head_sha
    if not isinstance(head_sha, str) or not SHA_RE.fullmatch(head_sha):
        return _unavailable("no_head_sha")
    token, refused = await mint_ci_token(lineage, work_item, settings, head_sha)
    if refused is not None:
        return refused
    assert token is not None
    try:
        url = (
            f"{settings.github_api_url.rstrip('/')}/repos/"
            f"{repo_url_path(lineage.repo_full_name)}/commits/{head_sha}/check-runs"
        )
    except ValueError:
        return _unavailable("github_error", head_sha)
    try:
        response = await client.get(
            url,
            params={"per_page": CHECK_RUNS_PAGE},
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=settings.github_app_timeout_seconds,
            follow_redirects=False,
        )
    except httpx.TimeoutException:
        return _unavailable("timeout", head_sha)
    except httpx.HTTPError:
        return _unavailable("github_error", head_sha)
    finally:
        del token
    if response.status_code in _STATUS_REASONS:
        return _unavailable(_STATUS_REASONS[response.status_code], head_sha)
    if response.status_code != 200:
        return _unavailable("github_error", head_sha)
    try:
        payload = response.json()
    except ValueError:
        return _unavailable("malformed_response", head_sha)
    verdict = _verdict(payload)
    if verdict not in ("passing", "failing", "pending", "none"):
        return _unavailable(verdict, head_sha)
    return CiObservation(
        state=verdict,  # type: ignore[arg-type]
        reason=None,
        head_sha=head_sha,
        observed_at=datetime.now(UTC),
    )


@dataclass(frozen=True)
class CiDetail:
    """The CI gate's view of a published head (#3097), never persisted.

    ``state`` is ``observed`` with the raw check runs, commit statuses and
    failing-run annotations, or ``unavailable`` with a fixed ``reason`` code
    that never carries a response body, header, URL or token.
    """

    state: str
    reason: str | None
    head_sha: str | None
    check_runs: list[dict[str, Any]] = field(default_factory=list)
    statuses: list[dict[str, Any]] = field(default_factory=list)
    annotations: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    job_logs: dict[int, str] = field(default_factory=dict)
    job_log_unavailable: set[int] = field(default_factory=set)
    # The commit the PR's base branch points to at this observation, with its
    # check runs and commit statuses (#4105). All None when not read or
    # unreadable; set all or none.
    base_sha: str | None = None
    base_check_runs: list[dict[str, Any]] | None = None
    base_statuses: list[dict[str, Any]] | None = None


def _detail_unavailable(reason: str, head_sha: str | None) -> CiDetail:
    return CiDetail(state="unavailable", reason=reason, head_sha=head_sha)


def _check_runs_reason(payload: Any) -> str | None:
    """None when the check-runs page is complete and well formed, else a reason."""

    verdict = _verdict(payload)
    if verdict in ("passing", "failing", "pending", "none"):
        return None
    return verdict


def _statuses_list(payload: Any) -> list[dict[str, Any]] | None:
    """The commit statuses list, or None when the payload is malformed.

    Only the statuses list counts: the combined ``state`` reads pending when no
    status exists at all.
    """

    statuses = payload.get("statuses") if isinstance(payload, dict) else None
    if not isinstance(statuses, list) or not all(
        isinstance(item, dict) and isinstance(item.get("state"), str) for item in statuses
    ):
        return None
    return list(statuses)


def _has_failure(check_runs: list[dict[str, Any]], statuses: list[dict[str, Any]]) -> bool:
    return any(
        run.get("status") == "completed" and run.get("conclusion") in _FAILING_CONCLUSIONS
        for run in check_runs
    ) or any(item.get("state") in ("error", "failure") for item in statuses)


async def _read_base_checks(
    get: Callable[[str, dict[str, Any]], Awaitable[tuple[Any, str | None]]],
    base_ref: str,
    log_deadline: float,
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]] | None:
    """The base branch's current head, with its check runs and statuses (#4105).

    Returns None on any failure: an unreadable base only means every
    head failure counts as caused by the change, never an unavailable head. It
    runs after the annotation and job log reads and only spends what is left of
    ``log_deadline``, so a slow base never delays the head observation.
    """

    loop = asyncio.get_running_loop()
    budget = max(0.0, min(CI_BASE_READ_TIMEOUT_SECONDS, log_deadline - loop.time()))
    try:
        async with asyncio.timeout(budget):
            # https://docs.github.com/en/rest/branches/branches#get-a-branch
            branch, reason = await get(f"/branches/{quote(base_ref, safe='/')}", {})
            commit = branch.get("commit") if reason is None and isinstance(branch, dict) else None
            sha = commit.get("sha") if isinstance(commit, dict) else None
            if not isinstance(sha, str) or not _COMMIT_SHA_RE.fullmatch(sha):
                return None
            runs_payload, reason = await get(
                f"/commits/{sha}/check-runs",
                {"per_page": CHECK_RUNS_PAGE, "filter": "latest"},
            )
            if reason is not None or _check_runs_reason(runs_payload) is not None:
                return None
            status_payload, reason = await get(
                f"/commits/{sha}/status", {"per_page": CHECK_RUNS_PAGE}
            )
            statuses = _statuses_list(status_payload) if reason is None else None
            if statuses is None:
                return None
            return sha, list(runs_payload["check_runs"]), statuses
    except Exception:  # noqa: BLE001
        # TimeoutError included: the base read is advisory and never fails the head.
        return None


def _signed_job_log_url(location: str | None) -> httpx.URL | None:
    """Accept only observed GitHub Actions log storage hosts."""

    if not location or len(location) > 4096:
        return None
    try:
        parts = urlsplit(location)
        url = httpx.URL(location)
    except (ValueError, httpx.InvalidURL):
        return None
    if (
        parts.scheme != "https"
        or "@" in parts.netloc
        or parts.fragment
        or not (
            url.host == _CI_LOG_HOST
            or (url.host is not None and _CI_AZURE_LOG_HOST.fullmatch(url.host))
        )
        or url.port not in (None, 443)
    ):
        return None
    return url


async def _fetch_job_log(
    client: httpx.AsyncClient,
    base: str,
    job_id: int,
    headers: dict[str, str],
    timeout_seconds: float,
) -> str | None:
    """Read one bounded log; any provider or download error is optional."""

    try:
        async with asyncio.timeout(timeout_seconds):
            response = await client.get(
                f"{base}/actions/jobs/{job_id}/logs",
                headers=headers,
                timeout=timeout_seconds,
                auth=None,
                follow_redirects=False,
            )
            if response.status_code != 302:
                return None
            url = _signed_job_log_url(response.headers.get("Location"))
            if url is None:
                return None
            # build_request inherits client defaults. Strip credentials before
            # sending to the signed URL, and disable client level auth as well.
            request = client.build_request("GET", url)
            for name in ("authorization", "proxy-authorization", "cookie", "x-github-api-version"):
                request.headers.pop(name, None)
            download = await client.send(
                request, stream=True, auth=None, follow_redirects=False
            )
            try:
                if download.status_code != 200:
                    return None
                tail = bytearray()
                consumed = 0
                async for chunk in download.aiter_bytes(chunk_size=8192):
                    consumed += len(chunk)
                    if consumed > CI_JOB_LOG_MAX_DECODED_BYTES:
                        return None
                    tail.extend(chunk)
                    if len(tail) > CI_JOB_LOG_TAIL_BYTES:
                        del tail[:-CI_JOB_LOG_TAIL_BYTES]
            finally:
                await download.aclose()
            redacted = redact_text(tail.decode("utf-8", errors="replace"))
            return "\n".join(redacted.splitlines()[-CI_JOB_LOG_MAX_LINES:])[-CI_JOB_LOG_MAX_CHARS:]
    except (TimeoutError, httpx.HTTPError, ValueError):
        return None


async def observe_ci_detail(
    lineage: Any, work_item: Any, settings: Settings, client: httpx.AsyncClient
) -> CiDetail:
    """Observe check runs, commit statuses and failing annotations for the head.

    Shares ``observe_ci``'s bounded credential mint (``mint_ci_token``). The
    whole observation is bounded by ``CI_DETAIL_DEADLINE_SECONDS``; on expiry
    the caller is released with ``unavailable``/``timeout``.
    """

    raw = getattr(lineage, "head_sha", None) if lineage is not None else None
    head_sha = raw if isinstance(raw, str) and SHA_RE.fullmatch(raw) else None
    log_deadline = asyncio.get_running_loop().time() + CI_DETAIL_DEADLINE_SECONDS - 0.5
    try:
        return await asyncio.wait_for(
            _observe_ci_detail(lineage, work_item, settings, client, log_deadline),
            timeout=CI_DETAIL_DEADLINE_SECONDS,
        )
    except TimeoutError:
        return _detail_unavailable("timeout", head_sha)


async def _observe_ci_detail(
    lineage: Any,
    work_item: Any,
    settings: Settings,
    client: httpx.AsyncClient,
    log_deadline: float,
) -> CiDetail:
    head_sha = getattr(lineage, "head_sha", None) if lineage is not None else None
    if not isinstance(head_sha, str) or not SHA_RE.fullmatch(head_sha):
        return _detail_unavailable("no_head_sha", None)
    return await read_ci_detail(
        settings,
        client,
        repo_full_name=lineage.repo_full_name,
        installation_id=lineage.github_installation_id or work_item.github_installation_id,
        head_sha=head_sha,
        log_deadline=log_deadline,
        diagnostics=True,
        base_ref=getattr(lineage, "base_ref", None),
    )


async def read_ci_detail(
    settings: Settings,
    client: httpx.AsyncClient,
    *,
    repo_full_name: str,
    installation_id: int | None,
    head_sha: str,
    log_deadline: float | None,
    diagnostics: bool,
    base_ref: str | None,
) -> CiDetail:
    """Check runs and commit statuses on ``head_sha``; with ``diagnostics``, also
    the failing runs' annotations and Actions job logs. With ``base_ref``, a
    failing head also reads the base branch head's checks (#4105).

    Unbounded: the caller owns the deadline. ``log_deadline`` is the loop time
    by which job log downloads must end.
    """

    if not SHA_RE.fullmatch(head_sha):
        return _detail_unavailable("no_head_sha", None)
    token, refused = await mint_repository_token(
        settings, repo_full_name, installation_id, head_sha
    )
    if refused is not None:
        return _detail_unavailable(refused.reason or "github_error", head_sha)
    assert token is not None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        try:
            base = f"{settings.github_api_url.rstrip('/')}/repos/{repo_url_path(repo_full_name)}"
        except ValueError:
            return _detail_unavailable("github_error", head_sha)

        async def get(path: str, params: dict[str, Any]) -> tuple[Any, str | None]:
            try:
                response = await client.get(
                    f"{base}{path}",
                    params=params,
                    headers=headers,
                    timeout=settings.github_app_timeout_seconds,
                    auth=None,
                    follow_redirects=False,
                )
            except httpx.TimeoutException:
                return None, "timeout"
            except httpx.HTTPError:
                return None, "github_error"
            if response.status_code in _STATUS_REASONS:
                return None, _STATUS_REASONS[response.status_code]
            if response.status_code != 200:
                return None, "github_error"
            try:
                return response.json(), None
            except ValueError:
                return None, "malformed_response"

        runs_payload, reason = await get(
            f"/commits/{head_sha}/check-runs",
            {"per_page": CHECK_RUNS_PAGE, "filter": "latest"},
        )
        if reason is None:
            reason = _check_runs_reason(runs_payload)
        if reason is not None:
            return _detail_unavailable(reason, head_sha)
        status_payload, reason = await get(
            f"/commits/{head_sha}/status", {"per_page": CHECK_RUNS_PAGE}
        )
        if reason is not None:
            return _detail_unavailable(reason, head_sha)
        statuses = _statuses_list(status_payload)
        if statuses is None:
            return _detail_unavailable("malformed_response", head_sha)
        check_runs: list[dict[str, Any]] = list(runs_payload["check_runs"])
        annotations: dict[int, list[dict[str, Any]]] = {}
        job_logs: dict[int, str] = {}
        job_log_unavailable: set[int] = set()
        if not diagnostics:
            return CiDetail(
                state="observed",
                reason=None,
                head_sha=head_sha,
                check_runs=check_runs,
                statuses=list(statuses),
            )
        failing_ids = [
            run["id"]
            for run in check_runs
            if run.get("status") == "completed"
            and run.get("conclusion") in _FAILING_CONCLUSIONS
            and isinstance(run.get("id"), int)
            and not isinstance(run.get("id"), bool)
        ]
        for run_id in failing_ids[:CI_DETAIL_ANNOTATED_RUNS]:
            payload, reason = await get(
                f"/check-runs/{run_id}/annotations", {"per_page": CHECK_RUNS_PAGE}
            )
            # Annotations only enrich the failure report; an unreadable page
            # never changes the verdict.
            if reason is None and isinstance(payload, list):
                annotations[run_id] = [item for item in payload if isinstance(item, dict)]
        action_ids = list(
            dict.fromkeys(
                run["id"]
                for run in check_runs
                if run.get("status") == "completed"
                and run.get("conclusion") in _FAILING_CONCLUSIONS
                and isinstance(run.get("id"), int)
                and not isinstance(run.get("id"), bool)
                and isinstance(run.get("app"), dict)
                and run["app"].get("slug") == "github-actions"
            )
        )
        job_log_unavailable.update(action_ids[CI_DETAIL_LOGGED_JOBS:])
        loop = asyncio.get_running_loop()
        deadline = (
            log_deadline if log_deadline is not None else loop.time() + CI_JOB_LOG_TIMEOUT_SECONDS
        )
        for job_id in action_ids[:CI_DETAIL_LOGGED_JOBS]:
            time_left = max(0.0, deadline - loop.time())
            log = await _fetch_job_log(
                client,
                base,
                job_id,
                headers,
                min(CI_JOB_LOG_TIMEOUT_SECONDS, time_left),
            )
            if log:
                job_logs[job_id] = log
            else:
                job_log_unavailable.add(job_id)
        # Last, so it only spends what the log deadline leaves (#4105).
        base_read: tuple[str, list[dict[str, Any]], list[dict[str, Any]]] | None = None
        if (
            isinstance(base_ref, str)
            and base_ref
            and log_deadline is not None
            and _has_failure(check_runs, statuses)
        ):
            base_read = await _read_base_checks(get, base_ref, log_deadline)
    finally:
        del token
        headers.clear()
    return CiDetail(
        state="observed",
        reason=None,
        head_sha=head_sha,
        check_runs=check_runs,
        statuses=statuses,
        annotations=annotations,
        job_logs=job_logs,
        job_log_unavailable=job_log_unavailable,
        base_sha=base_read[0] if base_read is not None else None,
        base_check_runs=base_read[1] if base_read is not None else None,
        base_statuses=base_read[2] if base_read is not None else None,
    )


# --- Actions reruns (#3741) --------------------------------------------------------


@dataclass(frozen=True)
class ActionsRerun:
    """One GitHub answer for a rerun request.

    ``retry`` is a transport or rate-limit failure and is not the one allowed
    attempt. ``refused`` is a definitive client response. The response body is
    never copied.
    """

    outcome: str
    reason: str | None = None
    run_id: int | None = None


RERUN_REFUSED = {
    401: "github_unauthorized",
    403: "github_forbidden",
    404: "github_not_found",
    422: "rerun_rejected",
}
ACTIONS_RUN_ID = re.compile(r"/actions/runs/([1-9][0-9]{0,18})(?:/|$)")


def run_id_from_details(details_url: Any) -> int | None:
    if not isinstance(details_url, str):
        return None
    matched = ACTIONS_RUN_ID.search(details_url)
    if matched is None:
        return None
    return int(matched.group(1))


def rerun_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


async def github_send(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    headers: dict[str, str],
    timeout: float,
) -> httpx.Response | ActionsRerun:
    try:
        # build_request inherits client auth. send(..., auth=None) strips it so
        # the installation token is the only credential on the wire.
        request = client.build_request(method, url, headers=headers, timeout=timeout)
        return await client.send(request, auth=None, follow_redirects=False)
    except httpx.TimeoutException:
        # The server may already have accepted the request.
        return ActionsRerun("unconfirmed", "timeout")
    except httpx.HTTPError:
        return ActionsRerun("unconfirmed", "github_error")


def status_rerun(status_code: int, *, ok: int) -> ActionsRerun | None:
    if status_code == ok:
        return None
    if status_code == 429 or status_code >= 500:
        reason = "github_rate_limited" if status_code == 429 else "github_error"
        return ActionsRerun("retry", reason)
    return ActionsRerun("refused", RERUN_REFUSED.get(status_code, "rerun_rejected"))


async def lookup_run_id(
    client: httpx.AsyncClient,
    base: str,
    headers: dict[str, str],
    timeout: float,
    job_id: int,
) -> ActionsRerun:
    """Read ``run_id`` when the check run did not carry an Actions URL.

    ``GET /repos/{owner}/{repo}/actions/jobs/{job_id}`` returns the workflow
    run id. Only that integer is kept.
    https://docs.github.com/en/rest/actions/workflow-jobs#get-a-job-for-a-workflow-run
    """

    sent = await github_send(client, "GET", f"{base}/actions/jobs/{job_id}", headers, timeout)
    if isinstance(sent, ActionsRerun):
        return sent
    refused = status_rerun(sent.status_code, ok=200)
    if refused is not None:
        return refused
    try:
        payload = sent.json()
    except ValueError:
        return ActionsRerun("refused", "rerun_rejected")
    run_id = payload.get("run_id") if isinstance(payload, dict) else None
    if not isinstance(run_id, int) or isinstance(run_id, bool) or run_id < 1:
        return ActionsRerun("refused", "rerun_rejected")
    return ActionsRerun("requested", run_id=run_id)


async def post_failed_run(
    client: httpx.AsyncClient,
    base: str,
    headers: dict[str, str],
    timeout: float,
    run_id: int,
) -> ActionsRerun:
    """Ask GitHub to rerun every failed job in one workflow run.

    ``POST /repos/{owner}/{repo}/actions/runs/{run_id}/rerun-failed-jobs``
    answers 201 Created. A missing Actions permission is 403. The body is
    never read.
    https://docs.github.com/en/rest/actions/workflow-runs#re-run-failed-jobs-from-a-workflow-run
    """

    sent = await github_send(
        client,
        "POST",
        f"{base}/actions/runs/{run_id}/rerun-failed-jobs",
        headers,
        timeout,
    )
    if isinstance(sent, ActionsRerun):
        return sent
    refused = status_rerun(sent.status_code, ok=201)
    if refused is not None:
        return refused
    return ActionsRerun("requested", run_id=run_id)
