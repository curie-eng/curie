"""The Bitbucket Cloud entry of the forge-neutral factory loop: BitbucketCloudDriver.

`run --scenario forge-loop --forge bitbucket_cloud` observes Bitbucket Cloud
only through it. Bitbucket has no issue tracker, so the ticket is a Jira Cloud
issue: ticket operations and the "close Jira test issues" teardown go to the
Jira Cloud driver (forges/jira_cloud.py), built on first use. The
change is the pull request the WorkItem records, CI is the build statuses on
its head, the review is a top-level pull request comment, and the merge is the
pull request merge API, on a repository in a dedicated test Bitbucket
workspace.

Two repository access tokens, each in a file: the factory's, which the
driver reads with, and the reviewer's, the test actor that sets CI, reviews,
merges and resets. A repository access token cannot read its own account, so
the reviewer's Atlassian account id comes from
CURIE_BITBUCKET_CLOUD_REVIEWER_ACCOUNT_ID.

The entry's setup, ``install_bitbucket_cloud``, installs the factory with
`curie factory quickstart --tracker jira-cloud --code-host bitbucket-cloud`,
with the factory's token and the reviewer as the review account.

Every request goes through an opener that refuses every redirect, so a token
is only ever sent to the URL the driver named.

Red CI is set deliberately, as the GitHub driver does: the test actor posts a
FAILED build status (LOOP_GATE_KEY) on the pull request's first head.
Bitbucket has no CI of its own to turn a later head green, so `set_ci_red`
also starts a watcher that reads the pull request's head every few seconds
and, on the first head that is not the red one, posts the SUCCESSFUL loop gate
there once and stops (#4401 decision 18.1). The factory's CI gate reads that
status while the loop waits for the first request to end; `ci_state` only
reads. The head marked red is never turned green.

Bitbucket review comments reach the factory by polling, not by a webhook
(ADR 0197), so the review's delivery is proved the only way the platform
shows it: the WorkItem reads back a new request after the comment.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from email.message import Message
from pathlib import Path
from typing import IO, Any

import factory_e2e as fe

API_URL = "https://api.bitbucket.org/2.0"
WEB_URL = "https://bitbucket.org"
GIT_USER = "x-token-auth"
REPO_VAR = "CURIE_BITBUCKET_CLOUD_REPO"
TOKEN_FILE_VAR = "CURIE_BITBUCKET_CLOUD_TOKEN_FILE"
REVIEWER_TOKEN_FILE_VAR = "CURIE_BITBUCKET_CLOUD_REVIEWER_TOKEN_FILE"
REVIEWER_ACCOUNT_VAR = "CURIE_BITBUCKET_CLOUD_REVIEWER_ACCOUNT_ID"
# What the Jira Cloud driver reads (forges/jira_cloud.py); checked here so the
# run fails before anything is installed (#4401 decision 18.6).
JIRA_VARS = ("CURIE_FACTORY_JIRA_ENV_FILE",)
REQUIRED_VARS = (
    REPO_VAR,
    TOKEN_FILE_VAR,
    REVIEWER_TOKEN_FILE_VAR,
    REVIEWER_ACCOUNT_VAR,
    *JIRA_VARS,
)

LOOP_GATE_KEY = "factory-e2e-loop-gate"
# Bitbucket declares no CI diagnostics (ADR 0197 consequence 5), so the factory
# shows the agent a failing build's name, state and link only, never its
# description: the name carries the instruction the red gate asks for.
LOOP_GATE_NAME = "factory-e2e loop gate: add a one line CHANGELOG.md entry for this change"
LOOP_GATE_DESCRIPTION = (
    "Loop gate: add a one line entry for this change to CHANGELOG.md and push it to this branch."
)
LOOP_GATE_GREEN = "Loop gate: the revision is green."
FACTORY_BRANCH_PREFIX = "curie/"
PR_STATES = ("OPEN", "MERGED", "DECLINED", "SUPERSEDED")
HTTP_TIMEOUT_SECONDS = 30
# The green watcher reads the head this often, for at most as long as the loop
# waits for the first request to end.
WATCH_INTERVAL_SECONDS = 5.0
WATCH_LIFETIME_SECONDS = float(fe.EXECUTION_BOUND_SECONDS + fe.PUBLICATION_ALLOWANCE_SECONDS)
# Each watcher request is bounded by this. One iteration makes at most three
# (the pull request read, the short hash's commit read, made once per head, and
# the green post), so stopping waits longer than one whole iteration, and a watcher still running
# after that is a failure.
WATCH_HTTP_TIMEOUT_SECONDS = 10.0
WATCH_JOIN_SECONDS = WATCH_HTTP_TIMEOUT_SECONDS * 4
# How long an accepted (202) merge is read back before it counts as not merged.
MERGE_SETTLE_SECONDS = 60.0
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


class _RefuseRedirect(urllib.request.HTTPRedirectHandler):
    """Follow no redirect: the 3xx surfaces as an HTTPError, nothing is re-sent."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: IO[bytes],
        code: int,
        msg: str,
        headers: Message,
        newurl: str,
    ) -> None:
        return None


def _opener() -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(urllib.request.HTTPSHandler(), _RefuseRedirect())


def _token(var: str) -> str:
    token = fe._read_secret_file(Path(os.environ[var]))
    if token is None:
        raise fe.ConfigError(f"{var}: the token file is unreadable or empty")
    return token


class BitbucketCloudDriver:
    """ForgeDriver (forges/base.py) for Bitbucket Cloud, test writes as the reviewer."""

    forge = "bitbucket_cloud"
    pairing = "jira_cloud+bitbucket_cloud"

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
        self.factory_token_file = Path(os.environ[TOKEN_FILE_VAR])
        self.reviewer_account = os.environ[REVIEWER_ACCOUNT_VAR].strip()
        self.factory_token = _token(TOKEN_FILE_VAR)
        self.reviewer_token = _token(REVIEWER_TOKEN_FILE_VAR)
        self._opener = _opener()
        # Abbreviated hash -> full hash; a commit never changes.
        self._hashes: dict[str, str] = {}
        self._jira: Any = None
        self._change: fe.ChangeRef | None = None
        self._watch_stop: threading.Event | None = None
        self._watch_thread: threading.Thread | None = None
        self.green_heads: list[str] = []
        self._base_sha = ""

    # --- HTTP -----------------------------------------------------------

    def _call(
        self,
        method: str,
        url: str,
        body: Any = None,
        *,
        as_reviewer: bool = False,
        timeout: float = HTTP_TIMEOUT_SECONDS,
    ) -> tuple[int, Any]:
        """(status, JSON body) for one request; status 0 when nothing answered.

        A URL off the API origin, such as a ``next`` page link, is refused
        before it is requested, so the token never leaves Bitbucket.
        """

        url = url if urllib.parse.urlsplit(url).scheme else self.api + url
        origin, there = urllib.parse.urlsplit(API_URL), urllib.parse.urlsplit(url)
        if (there.scheme, there.netloc) != (origin.scheme, origin.netloc):
            raise fe.PreflightFailed(f"refused a request off {origin.scheme}://{origin.netloc}")
        token = self.reviewer_token if as_reviewer else self.factory_token
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        # Bitbucket Cloud answers a bodiless POST that declares a JSON body
        # with a bare 400, so only a request with a body names its type.
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            url,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers,
        )
        try:
            with self._opener.open(request, timeout=timeout) as response:
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

    def _tracker(self) -> Any:
        if self._jira is None:
            self._jira = fe.FORGES["jira_cloud"].driver(self.p)
        return self._jira

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
        # The repository holds only a README; the default ticket is about the
        # GitHub fixture's code.
        title, body = fe.readme_only_ticket(title, body)
        return str(self._tracker().open_and_label_ticket(title, body, since))

    def teardowns(self) -> tuple[tuple[str, Callable[[], Any]], ...]:
        # The Jira driver closes and unlabels every issue it opened.
        return tuple(self._tracker().teardowns())

    def tracker_evidence(self, change: fe.ChangeRef | None, work_item_id: str) -> dict[str, Any]:
        # The Jira tracker evidence is the jira_cloud+github entry's; this
        # pairing asserts the loop only.
        return {}

    def judge_tracker(self, evidence: Mapping[str, Any]) -> list[str]:
        return []

    def await_change(self, work_item_id: str) -> fe.ChangeRef | None:
        return fe.await_work_item_change(self.p, work_item_id)

    # --- the pull request -----------------------------------------------

    def _full_hash(self, sha: str, timeout: float = HTTP_TIMEOUT_SECONDS) -> str | None:
        if len(sha) == 40:
            return sha
        if sha in self._hashes:
            return self._hashes[sha]
        status, commit = self._call("GET", f"/commit/{sha}", timeout=timeout)
        full = commit.get("hash") if status == 200 and isinstance(commit, dict) else None
        if not isinstance(full, str):
            return None
        self._hashes[sha] = full
        return full

    def _short_head(
        self, change: fe.ChangeRef, timeout: float = HTTP_TIMEOUT_SECONDS
    ) -> str | None:
        """The pull request's head as Bitbucket Cloud lists it, abbreviated."""

        status, pr = self._call("GET", f"/pullrequests/{change.number}", timeout=timeout)
        if status != 200 or not isinstance(pr, dict):
            return None
        short = ((pr.get("source") or {}).get("commit") or {}).get("hash")
        return short if isinstance(short, str) and short else None

    def _head(self, change: fe.ChangeRef, timeout: float = HTTP_TIMEOUT_SECONDS) -> str | None:
        short = self._short_head(change, timeout)
        return self._full_hash(short, timeout) if short else None

    def change_head(self, change: fe.ChangeRef) -> tuple[str | None, int | None]:
        # The change the green watcher follows once its first head is set red.
        self._change = change
        head = self._head(change)
        if head is None:
            return None, None
        commits = self._paged(f"/pullrequests/{change.number}/commits?pagelen=50")
        return head, None if commits is None else len(commits)

    # --- CI ---------------------------------------------------------------

    def _post_gate(
        self, sha: str, state: str, description: str, timeout: float = HTTP_TIMEOUT_SECONDS
    ) -> tuple[int, Any]:
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
            timeout=timeout,
        )

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        """FAILED on ``sha``, then watch the change for the revision to turn green."""

        change = self._change
        if change is None:
            raise fe.PreflightFailed("set_ci_red needs the change: read its head first")
        status, body = self._post_gate(sha, "FAILED", LOOP_GATE_DESCRIPTION)
        if status not in (200, 201) or not isinstance(body, dict):
            raise fe.PreflightFailed(f"setting the red loop gate status failed (HTTP {status})")
        self._start_watch(change, sha)
        # The commit status vocabulary the loop judge reads (GitHub's).
        return {"sha": sha, "context": LOOP_GATE_KEY, "state": "failure"}

    def _start_watch(self, change: fe.ChangeRef, red: str) -> None:
        self._stop_watch()
        stop = threading.Event()
        interval = WATCH_INTERVAL_SECONDS
        deadline = time.monotonic() + WATCH_LIFETIME_SECONDS

        def watch() -> None:
            while not stop.wait(interval) and time.monotonic() < deadline:
                short = self._short_head(change, WATCH_HTTP_TIMEOUT_SECONDS)
                # Still the red head: no need to expand it.
                if not short or red.startswith(short):
                    continue
                head = self._full_hash(short, WATCH_HTTP_TIMEOUT_SECONDS)
                if not head or head == red:
                    continue
                # A stop that came during the read: write nothing after it.
                if stop.is_set():
                    return
                status, _ = self._post_gate(
                    head, "SUCCESSFUL", LOOP_GATE_GREEN, WATCH_HTTP_TIMEOUT_SECONDS
                )
                if status in (200, 201):
                    self.green_heads.append(head)
                    return

        thread = threading.Thread(target=watch, name="bitbucket-cloud-green-gate", daemon=True)
        self._watch_stop, self._watch_thread = stop, thread
        thread.start()
        self.p.teardown.push("stop the green loop gate watcher", self._stop_watch)

    def _stop_watch(self) -> dict[str, Any]:
        """Stop the green watcher and wait for it; fails while it is still running.

        A watcher still alive after the join may yet post green, so the run
        is not torn down. It stays recorded, and the next stop waits again.
        """

        stop, thread = self._watch_stop, self._watch_thread
        if stop is not None and thread is not None:
            stop.set()
            thread.join(timeout=WATCH_JOIN_SECONDS)
            if thread.is_alive():
                raise fe.PreflightFailed(
                    f"the green loop gate watcher still runs {WATCH_JOIN_SECONDS:g} s "
                    "after it was stopped; it may still post a green build"
                )
        self._watch_stop = self._watch_thread = None
        return {"green_heads": list(self.green_heads)}

    def _statuses(self, sha: str) -> list[dict[str, Any]] | None:
        return self._paged(f"/commit/{sha}/statuses?pagelen=100")

    def ci_state(self, sha: str) -> dict[str, Any]:
        statuses = self._statuses(sha)
        if statuses is None:
            return {"sha": sha, "state": "unreadable"}
        return {
            "sha": sha,
            "state": cloud_ci_state(statuses),
            "statuses": [{"key": s.get("key"), "state": s.get("state")} for s in statuses],
        }

    # --- review and merge --------------------------------------------------

    @staticmethod
    def _request_count(detail: Mapping[str, Any]) -> int:
        return len([r for r in detail.get("requests") or [] if isinstance(r, dict)])

    def post_review(self, change: fe.ChangeRef, text: str) -> dict[str, Any]:
        """Comment as the reviewer, then wait for the factory's poll to admit it.

        The delivery fields mirror a GitHub webhook delivery: the WorkItem
        read (HTTP 200) that shows a request beyond those it had before the
        comment, and that request's id.
        """

        work_item_id = str(self.p.evidence["work_item_id"])
        detail = self.p.work_item_detail(work_item_id)
        if detail is None:
            raise fe.PreflightFailed("reading the WorkItem before the review failed")
        before = self._request_count(detail)
        fe.load_forge_entries()
        mention = sys.modules["factory_e2e_forges.jira_cloud"].factory_mention(os.environ)
        body = fe.revision_comment_text(fe.readme_only_review(text), mention)
        status, comment = self._call(
            "POST",
            f"/pullrequests/{change.number}/comments",
            {"content": {"raw": body}},
            as_reviewer=True,
        )
        if status != 201 or not isinstance(comment, dict) or not comment.get("id"):
            raise fe.PreflightFailed(f"posting the review comment failed (HTTP {status})")
        url = ((comment.get("links") or {}).get("html") or {}).get("href")

        def admitted() -> dict[str, Any] | None:
            detail = self.p.work_item_detail(work_item_id)
            if detail is None or self._request_count(detail) <= before:
                return None
            return fe._latest_request(detail)

        request = fe._wait(
            f"the factory to admit review comment {comment['id']}",
            self.p.admission_timeout,
            admitted,
            5,
        )
        return {
            "status_code": status,
            "comment_id": int(comment["id"]),
            "url": url,
            "delivery_id": request.get("id"),
            "delivery_status_code": 200,
            "delivery_api_status": "factory_admitted",
        }

    def merge(self, change: fe.ChangeRef) -> dict[str, Any]:
        """Merge as the reviewer.

        A 202 is an accepted asynchronous merge: the pull request is read back
        until it reads MERGED, and ``status_code`` is that read's 200, with
        ``accepted`` recording the 202.
        """

        status, body = self._call(
            "POST",
            f"/pullrequests/{change.number}/merge",
            {"type": "pullrequest", "close_source_branch": True},
            as_reviewer=True,
        )
        result: dict[str, Any] = {}
        if status == 202:
            result["accepted"] = status
            deadline = time.monotonic() + MERGE_SETTLE_SECONDS
            while True:
                status, body = self._call("GET", f"/pullrequests/{change.number}")
                settled = isinstance(body, dict) and body.get("state") == "MERGED"
                if settled or time.monotonic() > deadline:
                    break
                time.sleep(2)
        merged = status == 200 and isinstance(body, dict) and body.get("state") == "MERGED"
        commit = (body.get("merge_commit") or {}).get("hash") if isinstance(body, dict) else None
        return {"status_code": status, "merged": merged, "sha": commit, **result}

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
        return self._branch_head(self._default_branch())

    def _branch_head(self, name: str) -> str:
        ref = urllib.parse.quote(name, safe="")
        status, branch = self._call("GET", f"/refs/branches/{ref}")
        sha = (branch.get("target") or {}).get("hash") if isinstance(branch, dict) else None
        if status != 200 or not isinstance(sha, str):
            raise fe.PreflightFailed(f"reading the fixture default branch failed (HTTP {status})")
        return sha

    def reset_fixture(self) -> dict[str, Any]:
        """Stop the green watcher, decline every open pull request, delete factory branches."""

        self._stop_watch()
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
        branch = self._default_branch()
        self._base_sha = self._branch_head(branch)

        def restore() -> dict[str, Any]:
            if self._branch_head(branch) == self._base_sha:
                return {"restored": True}
            self._git_restore(branch, self._base_sha)
            return {"restored": self._branch_head(branch) == self._base_sha}

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


def install_bitbucket_cloud(p: fe.Preflight) -> None:
    """Install the factory with the combined Jira and Bitbucket Cloud quickstart."""

    driver = p.loop_driver()
    if not isinstance(driver, BitbucketCloudDriver):
        raise fe.ConfigError("install_bitbucket_cloud needs the bitbucket_cloud forge driver")
    fe.load_forge_entries()
    sys.modules["factory_e2e_forges.jira_cloud"].install_jira_bitbucket(
        p,
        code_host="bitbucket-cloud",
        repo=driver.repo,
        token_file=driver.factory_token_file,
        review_account=driver.reviewer_account,
        secrets=[driver.factory_token, driver.reviewer_token],
    )
    p.evidence["bitbucket_cloud"] = {"repo": driver.repo}


fe.register_forge(
    fe.ForgeEntry(
        name="bitbucket_cloud",
        driver=BitbucketCloudDriver,
        installed=True,
        setup=install_bitbucket_cloud,
        github_app=False,
    )
)
