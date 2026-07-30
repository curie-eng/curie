"""PR-check reporter (K1): turn an eval run into a GitHub commit status.

An eval run's rollup (passed_count / total, as EvalRunResult exposes) becomes a
GitHub commit status on the evaluated sha: success when every case passed, else
failure, with the familiar "34/36 passed" description. The GitHub client is
injectable so it can be mocked in tests.
"""

import logging
from typing import Literal

import httpx

logger = logging.getLogger(__name__)

State = Literal["success", "failure", "pending", "error"]


class GitHubReportError(Exception):
    """The GitHub statuses API rejected the commit-status post.

    Carries the upstream HTTP status so the caller can distinguish a client
    fault (unknown repo/commit, bad token) from a genuine GitHub server fault
    and map it to the right response, rather than letting every rejection bubble
    as an opaque 500.
    """

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def eval_state(passed_count: int, total: int) -> tuple[State, str]:
    """Map an eval rollup to a commit-status (state, description)."""

    state: State = "success" if total > 0 and passed_count == total else "failure"
    return state, f"{passed_count}/{total} passed"


class GitHubStatusReporter:
    """Posts commit statuses to the GitHub statuses API."""

    def __init__(
        self, client: httpx.AsyncClient, *, api_url: str, token: str, context: str
    ) -> None:
        self._client = client
        self._api_url = api_url.rstrip("/")
        self._token = token
        self._context = context

    async def report_eval(
        self,
        repo_full_name: str,
        sha: str,
        passed_count: int,
        total: int,
        target_url: str | None = None,
    ) -> State:
        state, description = eval_state(passed_count, total)
        if not self._token.strip():
            # No GitHub credential configured (local/dev, or a deploy without a
            # GitHub App). There is nothing to post a commit status to, so skip
            # the network call and return the computed state. Posting with an
            # empty token sends an "Authorization: Bearer " header that httpx
            # rejects (LocalProtocolError), which would 500 an otherwise
            # successful eval report.
            logger.info(
                "no GitHub token configured; skipping commit-status post for %s@%s (%s)",
                repo_full_name,
                sha,
                description,
            )
            return state
        payload: dict[str, str] = {
            "state": state,
            "context": self._context,
            "description": description,
        }
        if target_url:
            payload["target_url"] = target_url
        resp = await self._client.post(
            f"{self._api_url}/repos/{repo_full_name}/statuses/{sha}",
            json=payload,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise GitHubReportError(exc.response.status_code, exc.response.text.strip()) from exc
        return state
