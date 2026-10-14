"""The Bitbucket Data Center entry of the forge-neutral factory loop: BitbucketDcDriver.

`run --scenario forge-loop --forge bitbucket_dc` observes a Bitbucket Data
Center through it, and the server is the throwaway fixture that
`curie dev forge-fixture bitbucket-dc up` starts. The driver reads that
fixture's `state.json` from CURIE_BITBUCKET_DC_FIXTURE: the base URL, the test
CA, the repository and the passwords of its seeded users. When the variable is
unset the run brings a fixture up on the kube context node's address and
removes it on teardown. Ticket operations and the "close Jira test issues"
teardown go to the Jira Cloud driver (forges/jira_cloud.py), built on first
use.

The entry's setup, ``install_bitbucket_dc``, installs the factory with `curie
factory quickstart --tracker jira-cloud --code-host bitbucket-dc` against the
fixture's advertised URL and CA, with the reviewer's user id as the review
account, then admits the fixture's Docker bridge subnets as the GitLab
fixture entry does.

The change is the pull request the WorkItem records, CI is the
repository-scoped build statuses on its head, the review is a top-level
comment by `reviewer`, and the admin merges, resets and restores. Reads use
the factory's repository access token. Every request goes through an opener
that trusts the fixture CA and refuses every redirect, so a credential is only
ever sent to the URL the driver named.

Red CI follows the Bitbucket Cloud driver: a FAILED loop gate build on the
pull request's first head, then a watcher that posts the SUCCESSFUL gate once
on the first later head it reads, while the loop waits for the first request
to end (#4401 decision 18.1). `ci_state` only reads. The head marked red is
never turned green. The review's delivery is proved as on Cloud: the WorkItem
reads back a new request after the comment.
"""

from __future__ import annotations

import base64
import json
import os
import ssl
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

FIXTURE_VAR = "CURIE_BITBUCKET_DC_FIXTURE"
FIXTURE_DOWN_STEP = "remove Bitbucket Data Center fixture"
FIXTURE_UP_TIMEOUT_SECONDS = 20 * 60
FIXTURE_DOWN_TIMEOUT_SECONDS = 10 * 60
STATE_SCHEMA = "curie.forge-fixture.bitbucket-dc/v1"
LOOP_GATE_KEY = "factory-e2e-loop-gate"
LOOP_GATE_NAME = "factory-e2e loop gate"
LOOP_GATE_DESCRIPTION = (
    "Loop gate: add a one line entry for this change to CHANGELOG.md and push it to this branch."
)
LOOP_GATE_GREEN = "Loop gate: the revision is green."
FACTORY_BRANCH_PREFIX = "curie/"
REVIEWER = "reviewer"
HTTP_TIMEOUT_SECONDS = 30
PAGE_LIMIT = 100
# The green watcher reads the head this often, for at most as long as the loop
# waits for the first request to end.
WATCH_INTERVAL_SECONDS = 5.0
WATCH_LIFETIME_SECONDS = float(fe.EXECUTION_BOUND_SECONDS + fe.PUBLICATION_ALLOWANCE_SECONDS)
# Each watcher request is bounded by this. One iteration makes at most two
# (the pull request read and the green post), so stopping waits longer than
# one whole iteration, and a watcher still running after that is a failure.
WATCH_HTTP_TIMEOUT_SECONDS = 10.0
WATCH_JOIN_SECONDS = WATCH_HTTP_TIMEOUT_SECONDS * 3
_FAILING = frozenset({"FAILED", "STOPPED", "CANCELLED"})


def dc_ci_state(builds: Sequence[Mapping[str, Any]]) -> str:
    """``failing``, ``pending``, ``passing`` or ``none`` for one head's builds. Pure.

    ``UNKNOWN`` and ``INPROGRESS`` are pending.
    """

    if not builds:
        return "none"
    states = [str(item.get("state")) for item in builds]
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


def _opener(context: ssl.SSLContext) -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=context), _RefuseRedirect()
    )


def _secret(path: str, what: str) -> str:
    value = fe._read_secret_file(Path(path))
    if value is None:
        raise fe.ConfigError(f"{FIXTURE_VAR}: the {what} file is unreadable or empty")
    return value


def _basic(user: str, password: str) -> str:
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()


class BitbucketDcDriver:
    """ForgeDriver (forges/base.py) for the Bitbucket Data Center fixture."""

    forge = "bitbucket_dc"
    pairing = "jira_cloud+bitbucket_dc"

    def __init__(self, p: fe.Preflight) -> None:
        path = os.environ.get(FIXTURE_VAR, "").strip()
        if not path:
            raise fe.ConfigError(
                f"the bitbucket_dc forge needs {FIXTURE_VAR} set to the fixture's state.json"
            )
        try:
            state = json.loads(Path(path).read_text())
        except (OSError, ValueError) as exc:
            raise fe.ConfigError(f"{FIXTURE_VAR}: cannot read the fixture state: {exc}") from None
        if not isinstance(state, dict) or state.get("schema") != STATE_SCHEMA:
            raise fe.ConfigError(f"{FIXTURE_VAR}: not a {STATE_SCHEMA} state file")
        try:
            self.base_url = str(state["base_url"]).rstrip("/")
            project, slug = str(state["project_key"]), str(state["repo_slug"])
            self.default_branch = str(state["default_branch"])
            admin = state["admin"]
            self.admin_user = str(admin["username"])
            admin_password_file = str(admin["password_file"])
            reviewer_password_file = str(state["users"][REVIEWER]["password_file"])
            token_file = str(state["token_file"])
            ca_file = str(state["ca_file"])
            self.advertised_url = str(state["advertised_url"]).rstrip("/")
            self.reviewer_id = str(state["users"][REVIEWER]["user_id"])
            self.repo_path = f"{project}/{slug}"
            self.compose_project = str(state["project"])
            self.port = int(state["port"])
        except (KeyError, TypeError) as exc:
            raise fe.ConfigError(f"{FIXTURE_VAR}: the fixture state lacks {exc}") from None
        self.p = p
        self.repo_web = f"{self.base_url}/projects/{project}/repos/{slug}"
        self.rest = f"{self.base_url}/rest/api/latest/projects/{project}/repos/{slug}"
        self.branch_utils = (
            f"{self.base_url}/rest/branch-utils/latest/projects/{project}/repos/{slug}"
        )
        self.clone_url = f"{self.base_url}/scm/{project.lower()}/{slug}.git"
        self.ca_file = ca_file
        try:
            self.context = ssl.create_default_context(cafile=ca_file)
        except (OSError, ssl.SSLError) as exc:
            raise fe.ConfigError(f"{FIXTURE_VAR}: cannot load the fixture CA: {exc}") from None
        self._opener = _opener(self.context)
        self.token_file = Path(token_file)
        self.token_auth = f"Bearer {_secret(token_file, 'factory token')}"
        self.admin_auth = _basic(self.admin_user, _secret(admin_password_file, "admin password"))
        self.reviewer_auth = _basic(REVIEWER, _secret(reviewer_password_file, "reviewer password"))
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
        auth: str | None = None,
        timeout: float = HTTP_TIMEOUT_SECONDS,
    ) -> tuple[int, Any]:
        """(status, JSON body) for one request; status 0 when nothing answered.

        A URL off the fixture's origin is refused before it is requested, so a
        credential never leaves the fixture.
        """

        url = url if urllib.parse.urlsplit(url).scheme else self.rest + url
        origin, there = urllib.parse.urlsplit(self.base_url), urllib.parse.urlsplit(url)
        if (there.scheme, there.netloc) != (origin.scheme, origin.netloc):
            raise fe.PreflightFailed(f"refused a request off {origin.scheme}://{origin.netloc}")
        request = urllib.request.Request(
            url,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "Authorization": auth or self.token_auth,
                "Accept": "application/json",
                "Content-Type": "application/json",
                # Data Center refuses a write without it when the XSRF check is on.
                "X-Atlassian-Token": "no-check",
            },
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

    def _paged(self, path: str, *, auth: str | None = None) -> list[dict[str, Any]] | None:
        """Every ``values`` item across ``isLastPage`` pages; None when a page is unreadable."""

        items: list[dict[str, Any]] = []
        start = 0
        separator = "&" if "?" in path else "?"
        while True:
            status, page = self._call(
                "GET", f"{path}{separator}start={start}&limit={PAGE_LIMIT}", auth=auth
            )
            if status != 200 or not isinstance(page, dict):
                return None
            items.extend(item for item in page.get("values") or [] if isinstance(item, dict))
            following = page.get("nextPageStart")
            if page.get("isLastPage", True) or not isinstance(following, int):
                return items
            start = following

    # --- tickets (Jira Cloud) -------------------------------------------

    def _tracker(self) -> Any:
        if self._jira is None:
            self._jira = fe.FORGES["jira_cloud"].driver(self.p)
        return self._jira

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
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

    def _head(self, change: fe.ChangeRef, timeout: float = HTTP_TIMEOUT_SECONDS) -> str | None:
        status, pr = self._call("GET", f"/pull-requests/{change.number}", timeout=timeout)
        if status != 200 or not isinstance(pr, dict):
            return None
        head = (pr.get("fromRef") or {}).get("latestCommit")
        return head if isinstance(head, str) and head else None

    def change_head(self, change: fe.ChangeRef) -> tuple[str | None, int | None]:
        # The change the green watcher follows once its first head is set red.
        self._change = change
        head = self._head(change)
        if head is None:
            return None, None
        commits = self._paged(f"/pull-requests/{change.number}/commits")
        return head, None if commits is None else len(commits)

    # --- CI ---------------------------------------------------------------

    def _post_gate(
        self, sha: str, state: str, description: str, timeout: float = HTTP_TIMEOUT_SECONDS
    ) -> tuple[int, Any]:
        return self._call(
            "POST",
            f"/commits/{sha}/builds",
            {
                "key": LOOP_GATE_KEY,
                "state": state,
                "name": LOOP_GATE_NAME,
                "url": self.repo_web,
                "description": description,
            },
            auth=self.admin_auth,
            timeout=timeout,
        )

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        """FAILED on ``sha``, then watch the change for the revision to turn green."""

        change = self._change
        if change is None:
            raise fe.PreflightFailed("set_ci_red needs the change: read its head first")
        status, _ = self._post_gate(sha, "FAILED", LOOP_GATE_DESCRIPTION)
        if status not in (200, 201, 204):
            raise fe.PreflightFailed(f"setting the red loop gate build failed (HTTP {status})")
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
                head = self._head(change, WATCH_HTTP_TIMEOUT_SECONDS)
                if not head or head == red:
                    continue
                # A stop that came during the read: write nothing after it.
                if stop.is_set():
                    return
                status, _ = self._post_gate(
                    head, "SUCCESSFUL", LOOP_GATE_GREEN, WATCH_HTTP_TIMEOUT_SECONDS
                )
                if status in (200, 201, 204):
                    self.green_heads.append(head)
                    return

        thread = threading.Thread(target=watch, name="bitbucket-dc-green-gate", daemon=True)
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

    def _builds(self, sha: str) -> list[dict[str, Any]] | None:
        """Each build on ``sha``, read one key at a time; None when any read fails.

        On 9.4 the repository-scoped builds read answers 400 without ``key``,
        so the keys come from the build-status listing, and each is then read
        from the repository-scoped API the adapter and Required Builds use.
        """

        listing = self._paged(f"{self.base_url}/rest/build-status/latest/commits/{sha}")
        if listing is None:
            return None
        builds: list[dict[str, Any]] = []
        for key in dict.fromkeys(str(item["key"]) for item in listing if item.get("key")):
            query = urllib.parse.quote(key, safe="")
            status, build = self._call("GET", f"/commits/{sha}/builds?key={query}")
            if status != 200 or not isinstance(build, dict):
                return None
            builds.append(build)
        return builds

    def ci_state(self, sha: str) -> dict[str, Any]:
        builds = self._builds(sha)
        if builds is None:
            return {"sha": sha, "state": "unreadable"}
        return {
            "sha": sha,
            "state": dc_ci_state(builds),
            "statuses": [{"key": b.get("key"), "state": b.get("state")} for b in builds],
        }

    # --- review and merge --------------------------------------------------

    @staticmethod
    def _request_count(detail: Mapping[str, Any]) -> int:
        return len([r for r in detail.get("requests") or [] if isinstance(r, dict)])

    def post_review(self, change: fe.ChangeRef, text: str) -> dict[str, Any]:
        """Comment as ``reviewer``, then wait for the factory's poll to admit it.

        The delivery fields mirror a GitHub webhook delivery: the WorkItem
        read (HTTP 200) that shows a request beyond those it had before the
        comment, and that request's id.
        """

        work_item_id = str(self.p.evidence["work_item_id"])
        detail = self.p.work_item_detail(work_item_id)
        if detail is None:
            raise fe.PreflightFailed("reading the WorkItem before the review failed")
        before = self._request_count(detail)
        body = fe.revision_comment_text(text, self.p.config.mention)
        status, comment = self._call(
            "POST",
            f"/pull-requests/{change.number}/comments",
            {"text": body},
            auth=self.reviewer_auth,
        )
        if status != 201 or not isinstance(comment, dict) or comment.get("id") is None:
            raise fe.PreflightFailed(f"posting the review comment failed (HTTP {status})")

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
            "delivery_id": request.get("id"),
            "delivery_status_code": 200,
            "delivery_api_status": "factory_admitted",
        }

    def merge(self, change: fe.ChangeRef) -> dict[str, Any]:
        """Merge as the admin at the version just read."""

        status, pr = self._call("GET", f"/pull-requests/{change.number}", auth=self.admin_auth)
        version = pr.get("version") if status == 200 and isinstance(pr, dict) else None
        if not isinstance(version, int):
            return {"status_code": status, "merged": False}
        status, body = self._call(
            "POST",
            f"/pull-requests/{change.number}/merge?version={version}",
            auth=self.admin_auth,
        )
        merged = isinstance(body, dict) and body.get("state") == "MERGED"
        return {"status_code": status, "merged": merged}

    def change_merged(self, change: fe.ChangeRef) -> bool | None:
        status, pr = self._call("GET", f"/pull-requests/{change.number}")
        if status != 200 or not isinstance(pr, dict):
            return None
        return pr.get("state") == "MERGED"

    # --- the fixture repository --------------------------------------------

    def changes_opened(self) -> list[int]:
        listing = self._paged("/pull-requests?state=ALL")
        if listing is None:
            raise fe.PreflightFailed("listing fixture pull requests failed")
        started = self.p.scenario_started
        numbers = []
        for item in listing:
            created = item.get("createdDate")
            if started is not None and (
                not isinstance(created, int) or created / 1000 < started.timestamp()
            ):
                continue
            numbers.append(int(item["id"]))
        return sorted(numbers)

    def default_branch_head(self) -> str:
        return self._branch_head(self.default_branch)

    def _branch_head(self, branch: str) -> str:
        ref = urllib.parse.quote(f"refs/heads/{branch}", safe="")
        status, page = self._call("GET", f"/commits?until={ref}&limit=1")
        values = page.get("values") if status == 200 and isinstance(page, dict) else None
        sha = values[0].get("id") if isinstance(values, list) and values else None
        if not isinstance(sha, str):
            raise fe.PreflightFailed(f"reading the fixture default branch failed (HTTP {status})")
        return sha

    def reset_fixture(self) -> dict[str, Any]:
        """Stop the green watcher; decline open pull requests, delete factory branches."""

        self._stop_watch()
        open_prs = self._paged("/pull-requests?state=OPEN", auth=self.admin_auth)
        if open_prs is None:
            raise fe.PreflightFailed("listing open fixture pull requests failed")
        for pr in open_prs:
            status, _ = self._call(
                "POST",
                f"/pull-requests/{pr['id']}/decline?version={pr.get('version', 0)}",
                auth=self.admin_auth,
            )
            if status != 200:
                raise fe.PreflightFailed(f"declining a fixture pull request failed (HTTP {status})")
        branches = self._paged("/branches", auth=self.admin_auth)
        if branches is None:
            raise fe.PreflightFailed("listing fixture branches failed")
        deleted = 0
        for branch in branches:
            name = str(branch.get("displayId") or "")
            if not name.startswith(FACTORY_BRANCH_PREFIX):
                continue
            status, _ = self._call(
                "DELETE",
                f"{self.branch_utils}/branches",
                {"name": f"refs/heads/{name}", "dryRun": False},
                auth=self.admin_auth,
            )
            if status != 204:
                raise fe.PreflightFailed(f"deleting a fixture branch failed (HTTP {status})")
            deleted += 1
        return {"declined": len(open_prs), "branches_deleted": deleted}

    def pin_fixture_base(self) -> None:
        """Remember the default branch head; on teardown, force-push it back if it moved."""

        if self._base_sha:
            return
        branch = self.default_branch
        self._base_sha = self._branch_head(branch)

        def restore() -> dict[str, Any]:
            if self._branch_head(branch) == self._base_sha:
                return {"restored": True}
            self._git_restore(branch, self._base_sha)
            return {"restored": self._branch_head(branch) == self._base_sha}

        self.p.teardown.push("restore fixture default branch", restore)

    def _git_restore(self, branch: str, sha: str) -> None:
        # The credential travels in the environment, never in argv.
        env = {
            **os.environ,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_CONFIG_COUNT": "2",
            "GIT_CONFIG_KEY_0": "http.extraHeader",
            "GIT_CONFIG_VALUE_0": f"Authorization: {self.admin_auth}",
            "GIT_CONFIG_KEY_1": "http.sslCAInfo",
            "GIT_CONFIG_VALUE_1": self.ca_file,
        }
        with tempfile.TemporaryDirectory(prefix="bitbucket-dc-restore-") as work:
            for args in (
                ["init", "--quiet", "--bare"],
                ["fetch", "--quiet", self.clone_url, f"refs/heads/{branch}"],
                ["push", "--quiet", "--force", self.clone_url, f"{sha}:refs/heads/{branch}"],
            ):
                done = subprocess.run(
                    ["git", *args], cwd=work, env=env, capture_output=True, text=True
                )
                if done.returncode != 0:
                    raise fe.PreflightFailed(
                        f"git {args[0]} while restoring the fixture default branch failed "
                        f"(exit {done.returncode})"
                    )


# --- the entry's setup: the fixture and the quickstart ---------------------------------


def _gitlab() -> Any:
    """The gitlab forge entry module, whose fixture helpers this entry shares."""

    fe.load_forge_entries()
    return sys.modules["factory_e2e_forges.gitlab"]


def bring_up_fixture(p: fe.Preflight) -> Path:
    """Start a fresh fixture, push its removal, and return its state.json path."""

    gitlab = _gitlab()
    host = gitlab.node_reachable_host(p)
    # Outside the repository and the run's workdir: the fixture refuses a state
    # directory inside the repository, since it holds credentials.
    state_dir = Path(tempfile.mkdtemp(prefix="curie-bitbucket-dc-fixture-"))
    script = p.repo_root.joinpath(*gitlab.FIXTURE_SCRIPT)

    def remove() -> dict[str, Any]:
        if state_dir.exists():
            result = subprocess.run(
                ["bash", str(script), "bitbucket-dc", "down", "--state-dir", str(state_dir)],
                capture_output=True,
                text=True,
                check=False,
                timeout=FIXTURE_DOWN_TIMEOUT_SECONDS,
            )
            if result.returncode != 0:
                tail = (result.stderr or result.stdout).strip()[-1500:]
                raise fe.PreflightFailed(f"forge-fixture bitbucket-dc down failed: {tail}")
        if state_dir.exists():
            raise fe.PreflightFailed(f"the Bitbucket DC fixture state dir {state_dir} remains")
        return {"state_dir": str(state_dir), "removed": True}

    p.teardown.push(FIXTURE_DOWN_STEP, remove)
    p.evidence["bitbucket_dc_fixture"] = {"state_dir": str(state_dir), "advertise_host": host}
    fe.log(f"forge-fixture bitbucket-dc up --advertise-host {host} (about 5 to 10 minutes)")
    result = subprocess.run(
        [
            "bash",
            str(script),
            "bitbucket-dc",
            "up",
            "--state-dir",
            str(state_dir),
            "--advertise-host",
            host,
        ],
        stdout=subprocess.PIPE,
        text=True,
        check=False,
        timeout=FIXTURE_UP_TIMEOUT_SECONDS,
    )
    state_path = state_dir / "state.json"
    if result.returncode != 0 or not state_path.is_file():
        raise fe.PreflightFailed(f"forge-fixture bitbucket-dc up exited {result.returncode}")
    p.step("Bitbucket DC fixture up", advertise_host=host, state_dir=str(state_dir))
    return state_path


def _bridge_subnets(project: str) -> list[str]:
    """The IPv4 subnets of the fixture's compose network."""

    out = fe.run(
        ["docker", "network", "inspect", f"{project}_default", "--format", "{{json .IPAM.Config}}"]
    )
    subnets: list[str] = []
    for entry in json.loads(out or "null") or []:
        subnet = entry.get("Subnet") if isinstance(entry, dict) else None
        if isinstance(subnet, str) and ":" not in subnet:
            subnets.append(subnet)
    return subnets


def install_bitbucket_dc(p: fe.Preflight) -> None:
    """Install the factory on the fixture with the combined Jira quickstart."""

    driver = p.loop_driver()
    if not isinstance(driver, BitbucketDcDriver):
        raise fe.ConfigError("install_bitbucket_dc needs the bitbucket_dc forge driver")
    fe.load_forge_entries()
    jira = sys.modules["factory_e2e_forges.jira_cloud"]
    jira.install_jira_bitbucket(
        p,
        code_host="bitbucket-dc",
        repo=driver.repo_path,
        token_file=driver.token_file,
        review_account=driver.reviewer_id,
        forge_flags=["--forge-url", driver.advertised_url, "--ca-file", driver.ca_file],
        secrets=[driver.token_auth.removeprefix("Bearer ")],
    )
    p.evidence["bitbucket_dc"] = {"repo": driver.repo_path}
    _gitlab().allow_fixture_bridge(
        p, _bridge_subnets(driver.compose_project), driver.port, kind="bitbucket_dc"
    )


def _driver(p: fe.Preflight) -> BitbucketDcDriver:
    if not os.environ.get(FIXTURE_VAR):
        # Read by BitbucketDcDriver; this run owns the fixture it names.
        os.environ[FIXTURE_VAR] = str(bring_up_fixture(p))
    return BitbucketDcDriver(p)


fe.register_forge(
    fe.ForgeEntry(
        name="bitbucket_dc",
        driver=_driver,
        installed=True,
        setup=install_bitbucket_dc,
        github_app=False,
    )
)
