"""The GitHub CodeHost adapter (ADR 0197, "Two ports" item 2).

It wraps the GitHub reads and writes the factory already makes: the App
installation token flow (`curie_api.github_app`), the CI read in
`curie_api.forges.github.ci`, and the review feedback reads the poller and the
truth verifier make. Every operation, the optional ones included, is
supported.

Failures surface only as the classes in `curie_api.forges.errors`, each
carrying a fixed reason code (``str(error)``) and never a response body,
header, URL or token. Addressing is by ``RepositoryRef.path`` (GitHub's
``owner/name``), as every GitHub call made before the port did.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlsplit

import httpx
from curie_telemetry.redact import redact_text
from starlette.concurrency import run_in_threadpool

from curie_api.config import Settings
from curie_api.forges import types
from curie_api.forges.capabilities import CODE_HOST_OPERATIONS, Operation, Support
from curie_api.forges.errors import Ambiguous, ForgeError, NotFound, Unauthorized, Unavailable
from curie_api.forges.github import ci
from curie_api.forges.ports import MarkedComments
from curie_api.forges.types import (
    Actor,
    CheckSource,
    CheckState,
    CiAnnotation,
    CiDiagnostic,
    CiReport,
    CiRollup,
    Commit,
    Credential,
    CredentialExpiry,
    CredentialHeader,
    CredentialScope,
    FeedbackKind,
    FeedbackPage,
    NormalizedCheck,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
    RerunAttempt,
    RerunJob,
    RerunObserver,
    RerunOutcome,
    RerunRecord,
    ReviewFeedback,
    check_run_key,
    status_key,
)
from curie_api.github_app import GitHubAppError
from curie_api.repo_full_name import repo_url_path
from curie_api.repository_auth import resolve_repository_credential

# A repository referenced by its GitHub path only, before a stored row carried
# its immutable id (see `curie_api.forges.hosts.repository_ref`).
PATH_ID_PREFIX = "path:"
# The app that reports GitHub Actions check runs; only its runs can be rerun.
ACTIONS_APP_SLUG = "github-actions"
_TRANSIENT_MINT_REASONS = frozenset(
    {"timeout", "observation_busy", "github_rate_limited", "github_error"}
)
_SHA = re.compile(r"[0-9a-f]{40,64}")
_PER_PAGE = 100
_MAX_PAGES = 50
_EXCERPT_MAX = 6000
_FEEDBACK_EVENTS = {
    FeedbackKind.COMMENT: "issues/comments",
    FeedbackKind.REVIEW_COMMENT: "pulls/comments",
}
_PERMANENT_CI_REASONS = {
    "app_not_configured": Unauthorized,
    "installation_refused": Unauthorized,
    "github_unauthorized": Unauthorized,
    "github_forbidden": Unauthorized,
    "github_not_found": NotFound,
}


def _ci_error(reason: str) -> ForgeError:
    return _PERMANENT_CI_REASONS.get(reason, Unavailable)(reason)


def _run_state(run: Mapping[str, Any]) -> CheckState:
    if run.get("status") != "completed":
        return CheckState.PENDING
    conclusion = run.get("conclusion")
    if conclusion == "success":
        return CheckState.SUCCESS
    if conclusion == "neutral":
        return CheckState.NEUTRAL
    if conclusion == "skipped":
        return CheckState.SKIPPED
    if conclusion == "cancelled":
        return CheckState.CANCELLED
    if conclusion in {"failure", "timed_out", "action_required", "startup_failure"}:
        return CheckState.FAILURE
    # ``stale`` and anything unrecognised wait for a fresh conclusion.
    return CheckState.PENDING


def _status_state(item: Mapping[str, Any]) -> CheckState:
    state = item.get("state")
    if state == "success":
        return CheckState.SUCCESS
    if state in {"error", "failure"}:
        return CheckState.FAILURE
    return CheckState.PENDING


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _check_id(item: Mapping[str, Any]) -> str | None:
    value = item.get("id")
    return str(value) if type(value) is int and value > 0 else None


def base_rollup(detail: ci.CiDetail) -> CiRollup | None:
    """The checks read on the base branch head (#4105), or None when not read."""

    if detail.base_sha is None or detail.base_check_runs is None or detail.base_statuses is None:
        return None
    base = replace(detail, check_runs=detail.base_check_runs, statuses=detail.base_statuses)
    return CiRollup.on_head(detail.base_sha, normalize_checks(base, detail.base_sha))


def normalize_checks(detail: ci.CiDetail, head_sha: str) -> tuple[NormalizedCheck, ...]:
    """Check runs keyed by name and commit statuses keyed ``status:<context>``.

    A run's reported state is its status until it completes, then its
    conclusion; a status's is its state. Only a GitHub Actions run is native.
    """

    checks: list[NormalizedCheck] = []
    for run in detail.check_runs:
        name = _text(run.get("name"))
        reported = run.get("head_sha")
        url = run.get("details_url") or run.get("html_url")
        status = _text(run.get("status"))
        app = run.get("app")
        checks.append(
            NormalizedCheck(
                key=check_run_key(name),
                state=_run_state(run),
                head_sha=reported if isinstance(reported, str) and reported else head_sha,
                name=name,
                url=url if isinstance(url, str) else None,
                source=CheckSource.RUN,
                reported_state=status if status != "completed" else _text(run.get("conclusion")),
                started_at=_since(run.get("started_at")),
                check_id=_check_id(run),
                native=isinstance(app, dict) and app.get("slug") == ACTIONS_APP_SLUG,
            )
        )
    for item in detail.statuses:
        context = _text(item.get("context"))
        url = item.get("target_url")
        checks.append(
            NormalizedCheck(
                key=status_key(context),
                state=_status_state(item),
                head_sha=head_sha,
                name=context,
                url=url if isinstance(url, str) else None,
                source=CheckSource.STATUS,
                reported_state=_text(item.get("state")),
                started_at=_since(item.get("created_at")),
                check_id=_check_id(item),
            )
        )
    return tuple(checks)


def _redacted(value: Any) -> str | None:
    return redact_text(value) if isinstance(value, str) else None


def diagnostics_of(detail: ci.CiDetail) -> tuple[CiDiagnostic, ...]:
    """What each failing check said: output, annotations and the job log tail.

    Every failing check run and commit status gets one, so a caller can tell
    a check that said nothing from one whose log could not be read.
    """

    found: list[CiDiagnostic] = []
    for run in detail.check_runs:
        if _run_state(run) not in {CheckState.FAILURE, CheckState.CANCELLED}:
            continue
        raw_output = run.get("output")
        output: dict[str, Any] = raw_output if isinstance(raw_output, dict) else {}
        parts = [_text(output.get("title")), _text(output.get("summary"))]
        annotations: list[CiAnnotation] = []
        log: str | None = None
        run_id = run.get("id")
        if isinstance(run_id, int) and not isinstance(run_id, bool):
            for note in detail.annotations.get(run_id, []):
                line = note.get("start_line")
                annotations.append(
                    CiAnnotation(
                        path=_redacted(note.get("path")),
                        line=line if isinstance(line, int) else None,
                        message=_redacted(note.get("message")),
                    )
                )
                where = _text(note.get("path")) + (f":{line}" if isinstance(line, int) else "")
                parts.append(f"{where}: {_text(note.get('message'))}")
            raw_log = detail.job_logs.get(run_id)
            if isinstance(raw_log, str):
                log = redact_text(raw_log)
                parts.append(raw_log)
        found.append(
            CiDiagnostic(
                check_key=check_run_key(_text(run.get("name"))),
                excerpt=redact_text("\n".join(part for part in parts if part))[-_EXCERPT_MAX:],
                check_id=_check_id(run),
                title=_redacted(output.get("title")),
                summary=_redacted(output.get("summary")),
                annotations=tuple(annotations),
                log=log,
                log_unavailable=log is None and run_id in detail.job_log_unavailable,
            )
        )
    for item in detail.statuses:
        if _status_state(item) is not CheckState.FAILURE:
            continue
        description = _redacted(item.get("description"))
        found.append(
            CiDiagnostic(
                check_key=status_key(_text(item.get("context"))),
                excerpt=(description or "")[-_EXCERPT_MAX:],
                check_id=_check_id(item),
                summary=description,
            )
        )
    return tuple(found)


def _since(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


def _timestamp(value: Any) -> datetime | None:
    """A GitHub timestamp as an aware UTC time, or None when it is not one."""

    return _since(value) if isinstance(value, str) else None


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _human(item: Mapping[str, Any]) -> Actor | None:
    """The human author of a comment or review; None for an App or a bot."""

    if item.get("performed_via_github_app") is not None:
        return None
    user = item.get("user")
    if not isinstance(user, dict) or user.get("type") == "Bot":
        return None
    user_id, login = user.get("id"), user.get("login")
    if type(user_id) is not int or user_id <= 0 or not isinstance(login, str) or not login:
        return None
    return Actor(id=str(user_id), login=login)


class GitHubCodeHost:
    """CodeHost over the GitHub REST API with today's App or token credential."""

    kind = types.GITHUB

    def __init__(
        self,
        settings: Settings,
        client: httpx.AsyncClient,
        *,
        marked_comments: MarkedComments | None = None,
        repository_paths: Mapping[str, str] | None = None,
        page_size: int = _PER_PAGE,
    ) -> None:
        if page_size < 1:
            raise ValueError("page_size must be positive")
        self._settings = settings
        self._client = client
        self._api = settings.github_api_url.rstrip("/")
        self._html = settings.github_html_base.rstrip("/")
        self.host = urlsplit(self._html).hostname or "github.com"
        self.capabilities: Mapping[Operation, Support] = dict.fromkeys(
            CODE_HOST_OPERATIONS, Support.SUPPORTED
        )
        self._marked_comments = marked_comments
        # Immutable id -> configured path, so a numeric id resolves with a
        # token minted for its installation (commit 10 feeds this from config).
        self._paths = dict(repository_paths or {})
        self._page_size = page_size

    @property
    def marked_comments(self) -> MarkedComments:
        if self._marked_comments is None:
            raise RuntimeError("GitHubCodeHost was built without its marked comments")
        return self._marked_comments

    # Transport -------------------------------------------------------------

    def _repo(self, path: str) -> str:
        try:
            return f"/repos/{repo_url_path(path)}"
        except ValueError:
            raise NotFound("repository_path_invalid") from None

    async def _token(self, path: str) -> str:
        """The ``Authorization`` value for REST reads and writes on ``path``.

        It is the git credential's own header (an App installation token, else
        the operator token, as ``x-access-token`` Basic auth), the form every
        pull request, branch and commit call carried before the port.
        """

        try:
            _, header = await run_in_threadpool(resolve_repository_credential, path, self._settings)
        except (GitHubAppError, ValueError):
            raise Unauthorized("credential_unresolved") from None
        return header

    async def _send(
        self,
        method: str,
        path: str,
        *,
        token: str,
        params: Mapping[str, Any] | None = None,
        body: Mapping[str, Any] | None = None,
        ok: frozenset[int] = frozenset({200}),
    ) -> httpx.Response:
        try:
            response = await self._client.request(
                method,
                f"{self._api}{path}",
                params=dict(params) if params else None,
                json=dict(body) if body is not None else None,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": token,
                    "X-GitHub-Api-Version": "2022-11-28",
                },
                timeout=self._settings.github_app_timeout_seconds,
                follow_redirects=False,
            )
        except httpx.TimeoutException:
            raise Unavailable("timeout") from None
        except httpx.HTTPError:
            raise Unavailable("github_error") from None
        if response.status_code in ok:
            return response
        if response.status_code == 401:
            raise Unauthorized("github_unauthorized")
        if response.status_code == 403:
            # A rate-limited request is a 403 with an exhausted budget.
            if response.headers.get("x-ratelimit-remaining") == "0":
                raise Unavailable("github_rate_limited")
            raise Unauthorized("github_forbidden")
        if response.status_code == 404:
            raise NotFound("github_not_found")
        if response.status_code == 429:
            raise Unavailable("github_rate_limited")
        raise Unavailable("github_error")

    async def _json(self, method: str, path: str, *, token: str, **kwargs: Any) -> Any:
        response = await self._send(method, path, token=token, **kwargs)
        try:
            return response.json()
        except ValueError:
            raise Unavailable("malformed_response") from None

    async def _pages(
        self, path: str, *, token: str, params: Mapping[str, Any] | None = None
    ) -> list[Any]:
        """Every page of a listing, or `Unavailable` when one cannot be read."""

        items: list[Any] = []
        for page in range(1, _MAX_PAGES + 1):
            payload = await self._json(
                "GET",
                path,
                token=token,
                params={**(params or {}), "per_page": self._page_size, "page": page},
            )
            if not isinstance(payload, list):
                raise Unavailable("malformed_response")
            items.extend(payload)
            if len(payload) < self._page_size:
                return items
        raise Unavailable("listing_incomplete")

    # Repository and credential ---------------------------------------------

    async def resolve_repository(self, project_id: str) -> RepositoryRef:
        if project_id.startswith(PATH_ID_PREFIX):
            hint = project_id[len(PATH_ID_PREFIX) :]
            path = self._repo(hint)
        else:
            if not project_id.isdigit():
                raise NotFound("repository_id_invalid")
            hint = self._paths.get(project_id, "")
            if not hint:
                raise NotFound("repository_not_bound")
            path = f"/repositories/{project_id}"
        payload = await self._json("GET", path, token=await self._token(hint))
        if not isinstance(payload, dict):
            raise Unavailable("malformed_response")
        repository_id, full_name = payload.get("id"), payload.get("full_name")
        default_branch = payload.get("default_branch")
        if (
            type(repository_id) is not int
            or repository_id <= 0
            or not isinstance(full_name, str)
            or not isinstance(default_branch, str)
            or not default_branch
        ):
            raise Unavailable("malformed_response")
        if not project_id.startswith(PATH_ID_PREFIX) and str(repository_id) != project_id:
            raise NotFound("repository_mismatch")
        return RepositoryRef(
            self.kind, self.host, str(repository_id), full_name, default_branch=default_branch
        )

    async def credential(self, repository: RepositoryRef, scope: CredentialScope) -> Credential:
        """The App installation token (or the operator token) as git's Basic header.

        GitHub installation tokens are not scoped by clone or push, so both
        scopes carry the same token. Its lifetime is not reported here.
        """

        try:
            clone_url, header = await run_in_threadpool(
                resolve_repository_credential, repository.path, self._settings
            )
        except GitHubAppError:
            raise Unavailable("credential_unresolved") from None
        except ValueError:
            raise Unauthorized("credential_unresolved") from None
        kind, _, value = header.partition(" ")
        try:
            username, _, secret = base64.b64decode(value, validate=True).decode().partition(":")
        except (binascii.Error, UnicodeDecodeError):
            raise Unauthorized("credential_unresolved") from None
        if kind != "Basic" or not username or not secret:
            raise Unauthorized("credential_unresolved")
        # The origin is the host and base path git authenticates to, never the
        # repository URL: the clone URL is the origin plus the path.
        origin = clone_url.removesuffix(f"/{repository.path}.git")
        if origin == clone_url:
            raise Unauthorized("credential_unresolved")
        return Credential(
            origin=origin,
            header=CredentialHeader.AUTHORIZATION_BASIC,
            secret=secret,
            scope=scope,
            expiry=CredentialExpiry.UNKNOWN,
            username=username,
        )

    async def branch_head(self, repository: RepositoryRef, branch: str) -> str | None:
        path = f"{self._repo(repository.path)}/git/ref/heads/{quote(branch, safe='')}"
        try:
            payload = await self._json("GET", path, token=await self._token(repository.path))
        except NotFound:
            return None
        sha = payload.get("object", {}).get("sha") if isinstance(payload, dict) else None
        if not isinstance(sha, str) or _SHA.fullmatch(sha) is None:
            raise Unavailable("malformed_response")
        return sha

    async def read_commit(self, repository: RepositoryRef, sha: str) -> Commit:
        path = f"{self._repo(repository.path)}/git/commits/{quote(sha, safe='')}"
        payload = await self._json("GET", path, token=await self._token(repository.path))
        try:
            parents = tuple(str(parent["sha"]) for parent in payload["parents"])
            return Commit(sha=str(payload["sha"]), parents=parents, message=str(payload["message"]))
        except (KeyError, TypeError):
            raise Unavailable("malformed_response") from None

    # Pull requests ---------------------------------------------------------

    def _pull_request(
        self, repository: RepositoryRef, payload: Any, *, require_sides: bool
    ) -> PullRequest:
        """A pull request in ``repository``, from a payload GitHub returned.

        Its URL must be this repository's canonical pull URL. A side that names
        its repository must name this one, and with ``require_sides`` (a pull
        request found or opened by branch) both must: a fork's is never ours.
        """

        if not isinstance(payload, dict):
            raise Unavailable("malformed_response")
        number, url = payload.get("number"), payload.get("html_url")
        head, base = payload.get("head"), payload.get("base")
        if (
            type(number) is not int
            or number <= 0
            or not isinstance(url, str)
            or not isinstance(head, dict)
            or not isinstance(base, dict)
        ):
            raise Unavailable("malformed_response")
        expected = f"{self._html}/{repository.path}/pull/{number}"
        if url.casefold() != expected.casefold() or urlsplit(url).username is not None:
            raise Unavailable("pull_request_mismatch")
        for side in (head, base):
            repo = side.get("repo")
            if repo is None and not require_sides:
                continue
            name = repo.get("full_name") if isinstance(repo, dict) else None
            if not isinstance(name, str) or name.casefold() != repository.path.casefold():
                raise Unavailable("pull_request_mismatch")
        head_sha, head_ref, base_ref = head.get("sha"), head.get("ref"), base.get("ref")
        if (
            not isinstance(head_sha, str)
            or _SHA.fullmatch(head_sha.lower()) is None
            or not isinstance(head_ref, str)
            or not head_ref
            or not isinstance(base_ref, str)
        ):
            raise Unavailable("malformed_response")
        merged = payload.get("merged") is True or payload.get("merged_at") is not None
        raw_state = payload.get("state")
        if raw_state not in {"open", "closed"} or (merged and raw_state != "closed"):
            raise Unavailable("malformed_response")
        state = (
            PullRequestState.MERGED
            if merged
            else PullRequestState.CLOSED
            if raw_state == "closed"
            else PullRequestState.OPEN
        )
        title, body = payload.get("title"), payload.get("body")
        return PullRequest(
            ref=PullRequestRef(repository, str(number)),
            head_sha=head_sha.lower(),
            head_ref=head_ref,
            base_ref=base_ref,
            state=state,
            url=url,
            title=title if isinstance(title, str) else "",
            body=body if isinstance(body, str) else "",
            draft=payload.get("draft") is True,
            updated_at=_timestamp(payload.get("updated_at")),
        )

    async def find_pull_request(
        self, repository: RepositoryRef, *, head_ref: str
    ) -> PullRequest | None:
        owner = repository.path.split("/", 1)[0]
        rows = await self._json(
            "GET",
            f"{self._repo(repository.path)}/pulls",
            token=await self._token(repository.path),
            params={"state": "all", "head": f"{owner}:{head_ref}"},
        )
        if not isinstance(rows, list):
            raise Unavailable("malformed_response")
        if not rows:
            return None
        if len(rows) != 1:
            raise Ambiguous("multiple_pull_requests")
        return self._pull_request(repository, rows[0], require_sides=True)

    async def open_pull_request(
        self,
        repository: RepositoryRef,
        *,
        head_ref: str,
        base_ref: str,
        title: str,
        body: str,
        draft: bool = False,
    ) -> PullRequest:
        payload = await self._json(
            "POST",
            f"{self._repo(repository.path)}/pulls",
            token=await self._token(repository.path),
            body={
                "title": title,
                "head": head_ref,
                "base": base_ref,
                "body": body,
                **({"draft": True} if draft else {}),
            },
            ok=frozenset({201}),
        )
        return self._pull_request(repository, payload, require_sides=True)

    async def update_pull_request(
        self, pull_request: PullRequestRef, *, title: str | None, body: str | None
    ) -> PullRequest:
        changes = {
            name: value for name, value in (("title", title), ("body", body)) if value is not None
        }
        repository = pull_request.repository
        payload = await self._json(
            "PATCH",
            f"{self._repo(repository.path)}/pulls/{int(pull_request.number)}",
            token=await self._token(repository.path),
            body=changes,
        )
        return self._checked(pull_request, payload)

    def _checked(self, pull_request: PullRequestRef, payload: Any) -> PullRequest:
        pull = self._pull_request(pull_request.repository, payload, require_sides=False)
        if pull.ref.number != pull_request.number:
            raise Unavailable("pull_request_mismatch")
        return pull

    async def read_pull_request(self, pull_request: PullRequestRef) -> PullRequest:
        repository = pull_request.repository
        if not pull_request.number.isdigit():
            raise NotFound("pull_request_number_invalid")
        payload = await self._json(
            "GET",
            f"{self._repo(repository.path)}/pulls/{int(pull_request.number)}",
            token=await self._token(repository.path),
        )
        return self._checked(pull_request, payload)

    # CI ----------------------------------------------------------------------

    async def _detail(
        self,
        repository: RepositoryRef,
        head_sha: str,
        *,
        diagnostics: bool,
        base_ref: str | None = None,
    ) -> ci.CiDetail:
        deadline = (
            ci.CI_DETAIL_DEADLINE_SECONDS if diagnostics else ci.CI_OBSERVATION_DEADLINE_SECONDS
        )
        loop = asyncio.get_running_loop()
        try:
            detail = await asyncio.wait_for(
                ci.read_ci_detail(
                    self._settings,
                    self._client,
                    repo_full_name=repository.path,
                    installation_id=None,
                    head_sha=head_sha,
                    log_deadline=loop.time() + deadline - 0.5,
                    diagnostics=diagnostics,
                    base_ref=base_ref,
                ),
                timeout=deadline,
            )
        except TimeoutError:
            raise Unavailable("timeout") from None
        if detail.state != "observed" or detail.reason is not None:
            raise _ci_error(detail.reason or "github_error")
        return detail

    async def observe_ci(self, repository: RepositoryRef, head_sha: str) -> CiRollup:
        """Check runs and commit statuses on exactly ``head_sha``."""

        detail = await self._detail(repository, head_sha, diagnostics=False)
        return CiRollup.on_head(head_sha, normalize_checks(detail, head_sha))

    async def ci_diagnostics(
        self, repository: RepositoryRef, head_sha: str, *, base_ref: str | None
    ) -> CiReport:
        """Checks, annotations and job logs on ``head_sha``; for a failing head,
        also the checks on ``base_ref``'s current head (#4105).
        https://docs.github.com/en/rest/branches/branches#get-a-branch
        """

        detail = await self._detail(repository, head_sha, diagnostics=True, base_ref=base_ref)
        return CiReport(
            CiRollup.on_head(head_sha, normalize_checks(detail, head_sha)),
            diagnostics_of(detail),
            base_rollup(detail),
        )

    async def rerun_failed(
        self,
        repository: RepositoryRef,
        jobs: Sequence[RerunJob],
        *,
        settled: Collection[str] = (),
        on_attempt: RerunObserver | None = None,
    ) -> RerunRecord:
        """Rerun the failed jobs of each GitHub Actions workflow run they belong to (#3741).

        A job's workflow run is read from its Actions URL, else asked of
        ``GET /repos/{owner}/{repo}/actions/jobs/{job_id}``. Each workflow run
        not in ``settled`` is then asked once with ``POST
        /repos/{owner}/{repo}/actions/runs/{run_id}/rerun-failed-jobs``, under
        an installation token minted for the repository.
        https://docs.github.com/en/rest/actions/workflow-jobs#get-a-job-for-a-workflow-run
        https://docs.github.com/en/rest/actions/workflow-runs#re-run-failed-jobs-from-a-workflow-run
        """

        record = RerunRecord(tuple(jobs))
        if not jobs:
            return record
        base = f"{self._api}{self._repo(repository.path)}"
        token, refused = await ci.mint_repository_token(
            self._settings, repository.path, None, None
        )
        if refused is not None:
            reason = refused.reason or "github_error"
            outcome = (
                RerunOutcome.RETRY if reason in _TRANSIENT_MINT_REASONS else RerunOutcome.REFUSED
            )
            return replace(record, stopped=outcome, reason=reason)
        assert token is not None
        headers = ci.rerun_headers(token)
        del token
        timeout = self._settings.github_app_timeout_seconds
        try:
            resolved: list[RerunJob] = []
            for job in jobs:
                unit = job.unit
                if unit is None:
                    run_id = ci.run_id_from_details(job.url)
                    if run_id is None:
                        if not job.check_id.isdigit():
                            return replace(
                                record, stopped=RerunOutcome.REFUSED, reason="rerun_rejected"
                            )
                        looked = await ci.lookup_run_id(
                            self._client, base, headers, timeout, int(job.check_id)
                        )
                        if looked.outcome != "requested" or looked.run_id is None:
                            return replace(
                                record, stopped=RerunOutcome(looked.outcome), reason=looked.reason
                            )
                        run_id = looked.run_id
                    unit = str(run_id)
                resolved.append(replace(job, unit=unit))
            record = replace(record, jobs=tuple(resolved))
            units = [job.unit for job in resolved if job.unit is not None]
            for unit in dict.fromkeys(unit for unit in units if unit not in settled):
                posted = await ci.post_failed_run(self._client, base, headers, timeout, int(unit))
                outcome = (
                    RerunOutcome.ACCEPTED
                    if posted.outcome == "requested"
                    else RerunOutcome(posted.outcome)
                )
                attempt = RerunAttempt(unit, outcome, posted.reason)
                record = replace(record, attempts=(*record.attempts, attempt))
                if on_attempt is not None and not await on_attempt(record):
                    break
                if outcome in {RerunOutcome.RETRY, RerunOutcome.UNCONFIRMED}:
                    break
        finally:
            headers.clear()
        return record

    # Review feedback ---------------------------------------------------------

    async def list_review_feedback(
        self, pull_request: PullRequestRef, cursor: str | None
    ) -> FeedbackPage:
        """Human comments, review comments and reviews newer than ``cursor``.

        The cursor is the newest creation or submission time already listed.
        A listing that cannot be read to its end raises `Unavailable`.
        """

        since = _since(json.loads(cursor).get("since") if cursor else None)
        if cursor and since is None:
            raise ValueError("a GitHub feedback cursor carries an ISO-8601 'since'")
        pull = await self.read_pull_request(pull_request)
        repository = pull_request.repository
        token = await self._token(repository.path)
        repo = self._repo(repository.path)
        number = int(pull_request.number)
        params = {"since": _iso(since)} if since is not None else {}
        listed: list[tuple[datetime, ReviewFeedback]] = []

        def keep(item: Any, kind: FeedbackKind, stamp_key: str, head: str) -> None:
            if not isinstance(item, dict):
                return
            stamp, author, item_id = _since(item.get(stamp_key)), _human(item), item.get("id")
            if stamp is None or author is None or type(item_id) is not int:
                return
            if since is not None and stamp <= since:
                return
            thread = item.get("in_reply_to_id") or item_id
            listed.append(
                (
                    stamp,
                    ReviewFeedback(
                        id=str(item_id),
                        author=author,
                        body=_text(item.get("body")),
                        pull_request=pull_request,
                        head_sha=_text(item.get("commit_id")) or head,
                        kind=kind,
                        thread_id=str(thread) if kind is FeedbackKind.REVIEW_COMMENT else None,
                    ),
                )
            )

        issue_comments = await self._pages(
            f"{repo}/issues/{number}/comments", token=token, params=params
        )
        for comment in issue_comments:
            keep(comment, FeedbackKind.COMMENT, "created_at", pull.head_sha)
        review_comments = await self._pages(
            f"{repo}/pulls/{number}/comments", token=token, params=params
        )
        for comment in review_comments:
            keep(comment, FeedbackKind.REVIEW_COMMENT, "created_at", pull.head_sha)
        for review in await self._pages(f"{repo}/pulls/{number}/reviews", token=token):
            keep(review, FeedbackKind.REVIEW, "submitted_at", pull.head_sha)
        listed.sort(key=lambda pair: (pair[0], pair[1].id))
        newest = listed[-1][0] if listed else since
        next_cursor = json.dumps({"since": _iso(newest)}) if newest is not None else ""
        return FeedbackPage(tuple(item for _, item in listed), next_cursor)

    async def verify_feedback(self, feedback: ReviewFeedback) -> bool:
        pull_request = feedback.pull_request
        try:
            pull = await self.read_pull_request(pull_request)
        except NotFound:
            return False
        if pull.state is not PullRequestState.OPEN:
            return False
        repository = pull_request.repository
        repo = self._repo(repository.path)
        if not feedback.id.isdigit():
            return False
        if feedback.kind is FeedbackKind.REVIEW:
            path = f"{repo}/pulls/{int(pull_request.number)}/reviews/{int(feedback.id)}"
        else:
            path = f"{repo}/{_FEEDBACK_EVENTS[feedback.kind]}/{int(feedback.id)}"
        try:
            current = await self._json("GET", path, token=await self._token(repository.path))
        except NotFound:
            return False
        if not isinstance(current, dict) or _human(current) != feedback.author:
            return False
        if _text(current.get("body")) != feedback.body:
            return False
        target = {
            FeedbackKind.COMMENT: ("issue_url", f"{self._api}{repo}/issues/{pull_request.number}"),
            FeedbackKind.REVIEW_COMMENT: (
                "pull_request_url",
                f"{self._api}{repo}/pulls/{pull_request.number}",
            ),
        }.get(feedback.kind)
        return target is None or current.get(target[0]) == target[1]

    async def user_can_write(self, repository: RepositoryRef, actor: Actor) -> bool:
        """Write, maintain or admin for the immutable account id, read now.

        The legacy ``permission`` field folds maintain into write; the
        descriptive role name and any other value grant nothing.
        """

        path = (
            f"{self._repo(repository.path)}/collaborators/{quote(actor.login, safe='')}/permission"
        )
        try:
            payload = await self._json("GET", path, token=await self._token(repository.path))
        except NotFound:
            return False
        user = payload.get("user") if isinstance(payload, dict) else None
        if not isinstance(user, dict) or str(user.get("id")) != actor.id:
            return False
        return payload.get("permission") in ("write", "admin")
