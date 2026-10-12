"""The Bitbucket Cloud entry of the forge-neutral factory loop: BitbucketCloudDriver.

`run --scenario forge-loop --forge bitbucket_cloud` observes Bitbucket Cloud
only through it. Bitbucket has no issue tracker, so the ticket is a Jira Cloud
issue: ticket operations go to the Jira Cloud driver, built on first use. The
change is the pull request the WorkItem records, CI is the build statuses on
its head, the review is a top-level pull request comment, and the merge is the
pull request merge API, on a repository in a dedicated test Bitbucket
workspace.

Two repository access tokens, each in a file: the factory's, which the
driver reads with, and the reviewer's, the test actor that sets CI, reviews,
merges and resets. Every request goes through ``urllib.request.urlopen``.

Red CI is set deliberately, as the GitHub driver does: the test actor posts a
FAILED build status (LOOP_GATE_KEY) on the pull request's first head. Bitbucket
has no CI of its own to turn a later head green, so the first time `ci_state`
reads a later head that carries no status at all, it posts the SUCCESSFUL loop
gate there and reads again. The head marked red is never turned green.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import factory_e2e as fe

API_URL = "https://api.bitbucket.org/2.0"
WEB_URL = "https://bitbucket.org"
GIT_USER = "x-token-auth"
REPO_VAR = "CURIE_BITBUCKET_CLOUD_REPO"
TOKEN_FILE_VAR = "CURIE_BITBUCKET_CLOUD_TOKEN_FILE"
REVIEWER_TOKEN_FILE_VAR = "CURIE_BITBUCKET_CLOUD_REVIEWER_TOKEN_FILE"
# What the Jira Cloud driver reads; checked here so the run fails before
# anything is installed (#4401 decision 18.6).
JIRA_VARS = ("CURIE_JIRA_SITE", "CURIE_JIRA_TOKEN_FILE")
REQUIRED_VARS = (REPO_VAR, TOKEN_FILE_VAR, REVIEWER_TOKEN_FILE_VAR, *JIRA_VARS)

LOOP_GATE_KEY = "factory-e2e-loop-gate"
LOOP_GATE_NAME = "factory-e2e loop gate"
LOOP_GATE_DESCRIPTION = (
    "Loop gate: add a one line entry for this change to CHANGELOG.md and push it to this branch."
)
LOOP_GATE_GREEN = "Loop gate: the revision is green."
FACTORY_BRANCH_PREFIX = "curie/"
PR_STATES = ("OPEN", "MERGED", "DECLINED", "SUPERSEDED")
HTTP_TIMEOUT_SECONDS = 30
_FAILING = frozenset({"FAILED", "STOPPED", "CANCELLED"})


def cloud_ci_state(statuses: Sequence[Mapping[str, Any]]) -> str:
    """``failing``, ``pending``, ``passing`` or ``none`` for one head's build statuses. Pure."""

    if not statuses:
        return "none"
    states = [str(item.get("state")) for item in statuses]
    if any(state in _FAILING for state in states):
        return "failing"
    if any(state != "SUCCESSFUL" for state in states):
        return "pending"
    return "passing"


def _token(var: str) -> str:
    path = os.environ[var]
    try:
        token = Path(path).read_text().strip()
    except OSError as exc:
        raise fe.ConfigError(f"{var}: cannot read the token file: {exc.strerror}") from None
    if not token:
        raise fe.ConfigError(f"{var}: the token file is empty")
    return token


class BitbucketCloudDriver:
    """ForgeDriver (forges/base.py) for Bitbucket Cloud, test writes as the reviewer."""

    forge = "bitbucket_cloud"

    def __init__(self, p: fe.Preflight) -> None:
        missing = [name for name in REQUIRED_VARS if not os.environ.get(name, "").strip()]
        if missing:
            raise fe.ConfigError(
                "the bitbucket_cloud forge needs these environment variables set: "
                + ", ".join(missing)
            )
        repo = os.environ[REPO_VAR].strip().strip("/")
        workspace, _, slug = repo.partition("/")
        if not workspace or not slug or "/" in slug:
            raise fe.ConfigError(f"{REPO_VAR} must be <workspace>/<repo slug>")
        self.p = p
        self.repo = repo
        self.api = f"{API_URL}/repositories/{workspace}/{slug}"
        self.factory_token = _token(TOKEN_FILE_VAR)
        self.reviewer_token = _token(REVIEWER_TOKEN_FILE_VAR)
        self._jira: Any = None
        self._red_heads: set[str] = set()
        self._base_sha = ""

    # --- HTTP -----------------------------------------------------------

    def _call(
        self, method: str, url: str, body: Any = None, *, as_reviewer: bool = False
    ) -> tuple[int, Any]:
        """(status, JSON body) for one request; status 0 when nothing answered."""

        token = self.reviewer_token if as_reviewer else self.factory_token
        url = url if url.startswith("https://") else self.api + url
        request = urllib.request.Request(
            url,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
                status, data = int(response.status), response.read()
        except urllib.error.HTTPError as exc:
            status, data = exc.code, exc.read()
        except (urllib.error.URLError, OSError):
            return 0, None
        try:
            return status, json.loads(data) if data else None
        except ValueError:
            return status, None

    def _paged(self, path: str) -> list[dict[str, Any]] | None:
        """Every ``values`` item across ``next`` pages; None when a page is unreadable."""

        items: list[dict[str, Any]] = []
        url: str | None = path
        while url:
            status, page = self._call("GET", url)
            if status != 200 or not isinstance(page, dict):
                return None
            items.extend(item for item in page.get("values") or [] if isinstance(item, dict))
            url = page.get("next") if isinstance(page.get("next"), str) else None
        return items

    # --- tickets (Jira Cloud) -------------------------------------------

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
        if self._jira is None:
            self._jira = fe.FORGES["jira_cloud"].driver(self.p)
        return str(self._jira.open_and_label_ticket(title, body, since))

    def await_change(self, work_item_id: str) -> fe.ChangeRef | None:
        return fe.await_work_item_change(self.p, work_item_id)

    # --- the pull request -----------------------------------------------

    def _full_hash(self, sha: str) -> str | None:
        if len(sha) == 40:
            return sha
        status, commit = self._call("GET", f"/commit/{sha}")
        full = commit.get("hash") if status == 200 and isinstance(commit, dict) else None
        return full if isinstance(full, str) else None

    def change_head(self, change: fe.ChangeRef) -> tuple[str | None, int | None]:
        status, pr = self._call("GET", f"/pullrequests/{change.number}")
        if status != 200 or not isinstance(pr, dict):
            return None, None
        short = ((pr.get("source") or {}).get("commit") or {}).get("hash")
        head = self._full_hash(short) if isinstance(short, str) and short else None
        commits = self._paged(f"/pullrequests/{change.number}/commits?pagelen=50")
        return head, None if commits is None else len(commits)

    # --- CI ---------------------------------------------------------------

    def _post_gate(self, sha: str, state: str, description: str) -> tuple[int, Any]:
        return self._call(
            "POST",
            f"/commit/{sha}/statuses/build",
            {
                "key": LOOP_GATE_KEY,
                "state": state,
                "name": LOOP_GATE_NAME,
                "url": f"{WEB_URL}/{self.repo}",
                "description": description,
            },
            as_reviewer=True,
        )

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        status, body = self._post_gate(sha, "FAILED", LOOP_GATE_DESCRIPTION)
        if status not in (200, 201) or not isinstance(body, dict):
            raise fe.PreflightFailed(f"setting the red loop gate status failed (HTTP {status})")
        self._red_heads.add(sha)
        return {"sha": sha, "context": LOOP_GATE_KEY, "state": body.get("state")}

    def _statuses(self, sha: str) -> list[dict[str, Any]] | None:
        return self._paged(f"/commit/{sha}/statuses?pagelen=100")

    def ci_state(self, sha: str) -> dict[str, Any]:
        statuses = self._statuses(sha)
        if statuses == [] and self._red_heads and sha not in self._red_heads:
            # A revision after the red head: the test side turns it green.
            status, _ = self._post_gate(sha, "SUCCESSFUL", LOOP_GATE_GREEN)
            if status not in (200, 201):
                raise fe.PreflightFailed(
                    f"setting the green loop gate status failed (HTTP {status})"
                )
            statuses = self._statuses(sha)
        if statuses is None:
            return {"sha": sha, "state": "unreadable"}
        return {
            "sha": sha,
            "state": cloud_ci_state(statuses),
            "statuses": [{"key": s.get("key"), "state": s.get("state")} for s in statuses],
        }

    # --- review and merge --------------------------------------------------

    def post_review(self, change: fe.ChangeRef, text: str) -> dict[str, Any]:
        body = fe.revision_comment_text(text, self.p.config.mention)
        status, comment = self._call(
            "POST",
            f"/pullrequests/{change.number}/comments",
            {"content": {"raw": body}},
            as_reviewer=True,
        )
        if status != 201 or not isinstance(comment, dict) or not comment.get("id"):
            raise fe.PreflightFailed(f"posting the review comment failed (HTTP {status})")
        url = ((comment.get("links") or {}).get("html") or {}).get("href")
        return {"status_code": status, "comment_id": int(comment["id"]), "url": url}

    def merge(self, change: fe.ChangeRef) -> dict[str, Any]:
        """Merge as the reviewer; a 202 is an async merge, read back once."""

        status, body = self._call(
            "POST",
            f"/pullrequests/{change.number}/merge",
            {"type": "pullrequest", "close_source_branch": True},
            as_reviewer=True,
        )
        merged = isinstance(body, dict) and body.get("state") == "MERGED"
        if status == 202:
            merged = self.change_merged(change) is True
        commit = (body.get("merge_commit") or {}).get("hash") if isinstance(body, dict) else None
        return {"status_code": status, "merged": merged, "sha": commit}

    def change_merged(self, change: fe.ChangeRef) -> bool | None:
        status, pr = self._call("GET", f"/pullrequests/{change.number}")
        if status != 200 or not isinstance(pr, dict):
            return None
        return pr.get("state") == "MERGED"

    # --- the fixture repository --------------------------------------------

    def changes_opened(self) -> list[int]:
        query = "&".join(f"state={state}" for state in PR_STATES)
        listing = self._paged(f"/pullrequests?{query}&pagelen=50")
        if listing is None:
            raise fe.PreflightFailed("listing fixture pull requests failed")
        started = self.p.scenario_started
        numbers = []
        for item in listing:
            created = fe._parse_time(item.get("created_on"))
            if started is not None and (created is None or created < started):
                continue
            numbers.append(int(item["id"]))
        return sorted(numbers)

    def _default_branch(self) -> str:
        status, repo = self._call("GET", "")
        name = ((repo.get("mainbranch") or {}).get("name")) if isinstance(repo, dict) else None
        if status != 200 or not isinstance(name, str):
            raise fe.PreflightFailed(f"reading the fixture repository failed (HTTP {status})")
        return name

    def default_branch_head(self) -> str:
        name = self._default_branch()
        ref = urllib.parse.quote(name, safe="")
        status, branch = self._call("GET", f"/refs/branches/{ref}")
        sha = (branch.get("target") or {}).get("hash") if isinstance(branch, dict) else None
        if status != 200 or not isinstance(sha, str):
            raise fe.PreflightFailed(f"reading the fixture default branch failed (HTTP {status})")
        return sha

    def reset_fixture(self) -> dict[str, Any]:
        """Decline every open pull request and delete every factory branch."""

        open_prs = self._paged("/pullrequests?state=OPEN&pagelen=50")
        if open_prs is None:
            raise fe.PreflightFailed("listing open fixture pull requests failed")
        for pr in open_prs:
            status, _ = self._call("POST", f"/pullrequests/{pr['id']}/decline", as_reviewer=True)
            if status != 200:
                raise fe.PreflightFailed(f"declining a fixture pull request failed (HTTP {status})")
        branches = self._paged("/refs/branches?pagelen=100")
        if branches is None:
            raise fe.PreflightFailed("listing fixture branches failed")
        deleted = 0
        for branch in branches:
            name = str(branch.get("name") or "")
            if not name.startswith(FACTORY_BRANCH_PREFIX):
                continue
            ref = urllib.parse.quote(name, safe="")
            status, _ = self._call("DELETE", f"/refs/branches/{ref}", as_reviewer=True)
            if status != 204:
                raise fe.PreflightFailed(f"deleting a fixture branch failed (HTTP {status})")
            deleted += 1
        return {"declined": len(open_prs), "branches_deleted": deleted}

    def pin_fixture_base(self) -> None:
        """Remember the default branch head; on teardown, force-push it back if it moved.

        Bitbucket Cloud has no API that moves a branch, so the restore is a git
        push of the recorded commit as the reviewer.
        """

        if self._base_sha:
            return
        self._base_sha = self.default_branch_head()

        def restore() -> dict[str, Any]:
            if self.default_branch_head() != self._base_sha:
                self._git_restore(self._default_branch(), self._base_sha)
            return {"restored": self.default_branch_head() == self._base_sha}

        self.p.teardown.push("restore fixture default branch", restore)

    def _git_restore(self, branch: str, sha: str) -> None:
        basic = base64.b64encode(f"{GIT_USER}:{self.reviewer_token}".encode()).decode()
        # The credential travels in the environment, never in argv.
        env = {
            **os.environ,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "http.extraHeader",
            "GIT_CONFIG_VALUE_0": f"Authorization: Basic {basic}",
        }
        url = f"{WEB_URL}/{self.repo}.git"
        with tempfile.TemporaryDirectory(prefix="bitbucket-restore-") as work:
            for args in (
                ["init", "--quiet", "--bare"],
                ["fetch", "--quiet", url, f"refs/heads/{branch}"],
                ["push", "--quiet", "--force", url, f"{sha}:refs/heads/{branch}"],
            ):
                done = subprocess.run(
                    ["git", *args], cwd=work, env=env, capture_output=True, text=True
                )
                if done.returncode != 0:
                    raise fe.PreflightFailed(
                        f"git {args[0]} while restoring the fixture default branch failed "
                        f"(exit {done.returncode})"
                    )


fe.register_forge(
    fe.ForgeEntry(name="bitbucket_cloud", driver=BitbucketCloudDriver, installed=True)
)
