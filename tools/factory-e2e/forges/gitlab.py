"""The GitLab entry of the forge-neutral factory loop: GitLabDriver (#4399).

`run --scenario forge-loop --forge gitlab --forge-target <fixture|saas>`
drives the `gitlab+gitlab@<target>` acceptance entries. GitLab is both the
tracker and the code host, so the release is installed without a GitHub App
(``install_values(..., forge="gitlab")``) and this entry's setup,
``install_gitlab``, binds the factory agent with `curie factory quickstart
--code-host gitlab --tracker gitlab`.

Target ``fixture`` is the GitLab CE fixture of tools/forge-fixtures/gitlab
(its README documents ``state.json``). When CURIE_GITLAB_FIXTURE names a
fixture's ``state.json`` the run uses that fixture and leaves it up. When it
is unset, the run brings a fresh fixture up with `forge-fixture.sh gitlab up
--advertise-host <host>`, where the host is the InternalIP of the kube
context's node that runs the Docker daemon, so the cluster reaches the
fixture at the same address its certificate names. Its teardown step, "remove
GitLab fixture", runs `forge-fixture.sh gitlab down` last and checks the
state directory is gone. Every request carries the root token in
``PRIVATE-TOKEN``; the driver acts as ``developer`` (ticket, red status, diff
comment) and ``reporter`` (the probe label) through ``Sudo``, and merges and
resets as root. TLS verifies with the fixture CA only.

Target ``saas`` is a dedicated gitlab.com test project read from SAAS_VARS:
its path, the factory's Developer token file (what the quickstart installs)
and the human Owner token file the driver labels, reviews, sets CI and merges
with. A missing variable fails before any request. gitlab.com Free gives the
test group no Reporter token, so there is no reporter probe.

Red CI is one failing external commit status (LOOP_GATE_NAME) the test actor
posts on the merge request's first head; a later head carries no such status,
so the project's own pipeline decides green (a job that also fails on the
default branch is pre-existing and does not fail the factory gate). A token is
read from its file for each request and goes only into that request's
``PRIVATE-TOKEN`` header: never into argv, evidence, logs or an error.
"""

from __future__ import annotations

import json
import os
import re
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from email.message import Message
from pathlib import Path
from typing import IO, Any

import factory_e2e as fe

TARGETS = ("fixture", "saas")
SAAS_VARS = (
    "CURIE_FACTORY_GITLAB_PROJECT",
    "CURIE_FACTORY_GITLAB_FACTORY_TOKEN_FILE",
    "CURIE_FACTORY_GITLAB_HUMAN_TOKEN_FILE",
)
FIXTURE_VAR = "CURIE_GITLAB_FIXTURE"
SAAS_API_URL = "https://gitlab.com/api/v4"
LOOP_GATE_NAME = "factory-e2e/loop-gate"
# A commit status description is shown beside the status; keep it one line.
LOOP_GATE_DESCRIPTION = (
    "Loop gate: add a one line entry for this change to CHANGELOG.md and push it to this branch."
)
FIXTURE_DOWN_STEP = "remove GitLab fixture"
FIXTURE_SCRIPT = ("cli", "scripts", "forge-fixture.sh")
# `up` gives up on its own after 15 minutes; this only bounds a hung client.
FIXTURE_UP_TIMEOUT_SECONDS = 20 * 60
FIXTURE_DOWN_TIMEOUT_SECONDS = 10 * 60
REQUEST_TIMEOUT_SECONDS = 30
PAGE_SIZE = 100
MAX_PAGES = 100
RATE_LIMIT_RETRIES = 5
# The tracker label `curie factory quickstart --tracker gitlab` writes.
QUICKSTART_LABEL = "curie-factory"
STATE_LABEL_PREFIX = "curie-factory:"
TRACKER_POLL_SECONDS = 45
REPORTER_PROBE_SECONDS = 3 * TRACKER_POLL_SECONDS
ADMISSION_POLL_SECONDS = 5
MERGE_ATTEMPTS = 12
MERGE_RETRY_SECONDS = 10
# GitLab answers these while it is still computing mergeability.
_MERGE_RETRY_STATUSES = frozenset({405, 406, 409, 422})
REPORTER_PROBE_TITLE = "factory-e2e: a Reporter's label starts nothing"
REPORTER_PROBE_BODY = "A Reporter labels this issue. No WorkItem may be admitted for it."
# The fixture's widgets project: add() in widget.sh subtracts, so test.sh fails.
FIXTURE_ISSUE = (
    "Fix add() in widget.sh",
    "add() in widget.sh subtracts its arguments instead of adding them, so `sh test.sh` "
    "fails. Make add() return the sum of its two arguments, run `sh test.sh` until it "
    "prints ok, and open the merge request.",
)
FIXTURE_REVIEW = (
    "Please make one follow-up change on this merge request: add a check to test.sh that "
    "`add 10 -4` prints 6. Push it to this same branch."
)

_FAILING = frozenset({"failed", "canceled"})
_PENDING = frozenset(
    {
        "created",
        "waiting_for_resource",
        "preparing",
        "pending",
        "running",
        "scheduled",
        "canceling",
    }
)
_HUNK = re.compile(r"^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@")


# --- pure helpers -------------------------------------------------------------------------


def missing_saas_env(environ: Mapping[str, str]) -> str | None:
    """The first of SAAS_VARS that is unset or blank, or None. Pure."""

    for name in SAAS_VARS:
        if not (environ.get(name) or "").strip():
            return name
    return None


def gitlab_ci_state(statuses: Sequence[Mapping[str, Any]]) -> str:
    """``none``, ``failing``, ``pending`` or ``passing`` for one head's commit statuses. Pure.

    ``failed`` with ``allow_failure`` does not fail; ``skipped`` and ``manual``
    neither fail nor hold it pending; a failure wins over anything pending.
    """

    if not statuses:
        return "none"
    if any(item.get("status") in _FAILING and not item.get("allow_failure") for item in statuses):
        return "failing"
    if any(item.get("status") in _PENDING for item in statuses):
        return "pending"
    return "passing"


def label_request(project_id: int, iid: int, label: str) -> tuple[str, str, dict[str, Any]]:
    """Add ``label`` to issue ``iid``, leaving every other label. Pure."""

    return "PUT", f"/projects/{project_id}/issues/{iid}", {"add_labels": label}


def status_request(project_id: int, sha: str, state: str) -> tuple[str, str, dict[str, Any]]:
    """Set the loop gate external commit status on ``sha``. Pure."""

    body = {"state": state, "name": LOOP_GATE_NAME, "description": LOOP_GATE_DESCRIPTION}
    return "POST", f"/projects/{project_id}/statuses/{sha}", body


def diff_comment_request(
    project_id: int,
    mr_iid: int,
    body: str,
    *,
    diff_refs: Mapping[str, Any],
    path: str,
    line: int,
    old_line: int | None = None,
) -> tuple[str, str, dict[str, Any]]:
    """A diff note on ``path`` at new ``line``, positioned on the MR's diff refs. Pure.

    ``old_line`` is set for an unchanged line, which GitLab positions by both.
    """

    position: dict[str, Any] = {
        "position_type": "text",
        "base_sha": diff_refs.get("base_sha"),
        "start_sha": diff_refs.get("start_sha"),
        "head_sha": diff_refs.get("head_sha"),
        "old_path": path,
        "new_path": path,
        "new_line": line,
    }
    if old_line is not None:
        position["old_line"] = old_line
    return (
        "POST",
        f"/projects/{project_id}/merge_requests/{mr_iid}/discussions",
        {"body": body, "position": position},
    )


def merge_request(project_id: int, mr_iid: int) -> tuple[str, str, dict[str, Any]]:
    """Merge the merge request, keeping its source branch. Pure."""

    return (
        "PUT",
        f"/projects/{project_id}/merge_requests/{mr_iid}/merge",
        {"should_remove_source_branch": False},
    )


def diff_line(diff: str) -> tuple[int, int | None] | None:
    """(new line, old line or None) of the first added line, else the first kept line. Pure."""

    new = old = 0
    kept: tuple[int, int | None] | None = None
    for raw in diff.splitlines():
        hunk = _HUNK.match(raw)
        if hunk:
            old, new = int(hunk.group(1)), int(hunk.group(2))
            continue
        if not new:
            continue
        if raw.startswith("+"):
            return new, None
        if raw.startswith("-"):
            old += 1
            continue
        if raw.startswith("\\"):
            continue
        if kept is None:
            kept = (new, old)
        new += 1
        old += 1
    return kept


def closes_issue(description: str | None, iid: int) -> bool:
    """Whether a merge request description closes issue ``iid``. Pure."""

    return bool(re.search(rf"(?im)\bcloses #{iid}\b", description or ""))


# --- the fixture: bring up, read, tear down ---------------------------------------------


def _script(p: fe.Preflight) -> Path:
    return p.repo_root.joinpath(*FIXTURE_SCRIPT)


def node_reachable_host(p: fe.Preflight) -> str:
    """The InternalIP of the kube context's node that runs the Docker daemon.

    The fixture's published port is open on the daemon host, so a node that
    is that host reaches it at its own InternalIP, as every other node does.
    """

    daemon = fe.run(["docker", "info", "--format", "{{.Name}}"]).strip()
    try:
        listing = json.loads(p.kubectl("get", "nodes", "-o", "json"))
    except ValueError as exc:
        raise fe.PreflightFailed("kubectl get nodes did not print JSON") from exc
    names: list[str] = []
    for node in listing.get("items") or []:
        if not isinstance(node, dict):
            continue
        name = str((node.get("metadata") or {}).get("name") or "")
        addresses = [a for a in (node.get("status") or {}).get("addresses") or [] if a]
        hostnames = {str(a.get("address")) for a in addresses if a.get("type") == "Hostname"}
        names.append(name)
        if daemon and daemon in {name, *hostnames}:
            for address in addresses:
                if address.get("type") == "InternalIP" and address.get("address"):
                    return str(address["address"])
            raise fe.PreflightFailed(f"node {name} has no InternalIP")
    raise fe.PreflightFailed(
        f"the Docker daemon host {daemon!r} is not a node of context "
        f"{p.config.kube_context} (nodes: {', '.join(names) or 'none'}); the GitLab fixture "
        "must run on a host the cluster's nodes reach"
    )


def bring_up_fixture(p: fe.Preflight) -> Path:
    """Start a fresh fixture, push its removal, and return its state.json path."""

    host = node_reachable_host(p)
    # Outside the repository and outside the run's workdir, which main deletes:
    # the acceptance check reads this directory's absence as the fixture gone.
    state_dir = Path(tempfile.mkdtemp(prefix="curie-gitlab-fixture-"))
    script = _script(p)

    def remove() -> dict[str, Any]:
        if state_dir.exists():
            result = subprocess.run(
                ["bash", str(script), "gitlab", "down", "--state-dir", str(state_dir)],
                capture_output=True,
                text=True,
                check=False,
                timeout=FIXTURE_DOWN_TIMEOUT_SECONDS,
            )
            if result.returncode != 0:
                tail = (result.stderr or result.stdout).strip()[-1500:]
                raise fe.PreflightFailed(f"forge-fixture gitlab down failed: {tail}")
        if state_dir.exists():
            raise fe.PreflightFailed(f"the GitLab fixture state dir {state_dir} remains")
        return {"state_dir": str(state_dir), "removed": True}

    p.teardown.push(FIXTURE_DOWN_STEP, remove)
    p.evidence["gitlab_fixture"] = {"state_dir": str(state_dir), "advertise_host": host}
    fe.log(f"forge-fixture gitlab up --advertise-host {host} (about 3 to 7 minutes)")
    result = subprocess.run(
        [
            "bash",
            str(script),
            "gitlab",
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
        raise fe.PreflightFailed(f"forge-fixture gitlab up exited {result.returncode}")
    p.step("GitLab fixture up", advertise_host=host, state_dir=str(state_dir))
    return state_path


def _load_state(path: Path) -> dict[str, Any]:
    try:
        state = json.loads(path.read_text())
    except OSError as exc:
        raise fe.ConfigError(f"{FIXTURE_VAR} names an unreadable file: {exc.strerror}") from None
    except ValueError:
        raise fe.ConfigError(f"{FIXTURE_VAR} names a file that is not JSON") from None
    if not isinstance(state, dict):
        raise fe.ConfigError(f"{FIXTURE_VAR} names a file that is not a fixture state")
    for key in ("local_api_url", "base_url", "ca_path", "root_token_file", "projects", "users"):
        if not state.get(key):
            raise fe.ConfigError(f"the fixture state.json has no {key}")
    return state


def _read_token(path: Path, what: str) -> str:
    try:
        token = path.read_text().strip()
    except OSError as exc:
        raise fe.ConfigError(f"the GitLab {what} is unreadable: {exc.strerror}") from None
    if not token:
        raise fe.ConfigError(f"the GitLab {what} is empty")
    return token


# --- the driver -------------------------------------------------------------------------


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


class GitLabDriver:
    """ForgeDriver (forges/base.py) for GitLab as tracker and code host."""

    forge = "gitlab"

    def __init__(self, p: fe.Preflight, *, target: str) -> None:
        if target not in TARGETS:
            raise fe.ConfigError(
                f"--forge-target must be one of {', '.join(TARGETS)} for the gitlab forge"
            )
        self.p = p
        self.target = target
        self.pairing = f"gitlab+gitlab@{target}"
        self._opener: urllib.request.OpenerDirector | None = None
        self._pid: int | None = None
        self._default_branch: str | None = None
        self._factory_username: str | None = None
        self._base_sha = ""
        self._restore_pushed = False
        # The run's ticket and review discussion, once they exist.
        self.iid: int | None = None
        self.discussion_id: str | None = None
        self.created: list[int] = []
        if target == "fixture":
            named = os.environ.get(FIXTURE_VAR)
            if not named:
                raise fe.ConfigError(f"{FIXTURE_VAR} must name the GitLab fixture's state.json")
            state_path = Path(named)
            self.state = _load_state(state_path)
            self.api_base = str(self.state["local_api_url"]).rstrip("/")
            widgets = (self.state.get("projects") or {}).get("widgets") or {}
            self._pid = int(widgets["id"])
            self.project_path = str(widgets["path"])
            self._default_branch = str(widgets.get("default_branch") or "main")
            service = (self.state.get("users") or {}).get("service_account") or {}
            self._factory_username = str(service.get("username") or "") or None
            recorded = dict(p.evidence.get("gitlab_fixture") or {})
            recorded["state_dir"] = str(state_path.parent)
            p.evidence["gitlab_fixture"] = recorded
        else:
            missing = missing_saas_env(os.environ)
            if missing:
                raise fe.ConfigError(f"the gitlab saas target needs {missing}")
            self.state = {}
            self.api_base = SAAS_API_URL
            self.project_path = os.environ[SAAS_VARS[0]].strip()
            self.factory_token_file = Path(os.environ[SAAS_VARS[1]].strip())
            self.human_token_file = Path(os.environ[SAAS_VARS[2]].strip())
        p.evidence["gitlab_project"] = self.project_path

    def __repr__(self) -> str:
        return f"GitLabDriver(target={self.target}, project={self.project_path})"

    # -- plumbing --

    def _context(self) -> ssl.SSLContext:
        if self.target == "fixture":
            context = ssl.create_default_context(cafile=str(self.state["ca_path"]))
            context.verify_mode = ssl.CERT_REQUIRED
            return context
        return ssl.create_default_context()

    def _open(self, request: urllib.request.Request) -> Any:
        # Every request carries PRIVATE-TOKEN (and Sudo for a fixture actor),
        # which the default redirect handler would copy to the Location.
        if self._opener is None:
            self._opener = urllib.request.build_opener(
                urllib.request.HTTPSHandler(context=self._context()), _RefuseRedirect()
            )
        return self._opener.open(request, timeout=REQUEST_TIMEOUT_SECONDS)

    def _headers(self, actor: str) -> dict[str, str]:
        if self.target == "fixture":
            if actor not in ("root", "developer", "reporter"):
                raise ValueError(f"no fixture actor {actor}")
            token = _read_token(Path(str(self.state["root_token_file"])), "root token file")
            headers = {"PRIVATE-TOKEN": token}
            if actor != "root":
                headers["Sudo"] = actor
            return headers
        if actor == "factory":
            return {"PRIVATE-TOKEN": _read_token(self.factory_token_file, "factory token file")}
        if actor != "human":
            raise ValueError(f"no saas actor {actor}")
        return {"PRIVATE-TOKEN": _read_token(self.human_token_file, "human token file")}

    def _call(
        self, actor: str, method: str, path: str, body: Any = None
    ) -> tuple[int, Any, Mapping[str, str]]:
        """One REST v4 call; (status, parsed body, headers). Never raises on HTTP status."""

        data = None if body is None else json.dumps(body).encode()
        for attempt in range(RATE_LIMIT_RETRIES + 1):
            request = urllib.request.Request(self.api_base + path, data=data, method=method)
            for key, value in self._headers(actor).items():
                request.add_header(key, value)
            request.add_header("Accept", "application/json")
            if data is not None:
                request.add_header("Content-Type", "application/json")
            headers: Mapping[str, str]
            try:
                with self._open(request) as response:
                    raw = response.read()
                    status = int(response.status)
                    headers = {k.lower(): v for k, v in response.headers.items()}
            except urllib.error.HTTPError as exc:
                raw = exc.read()
                status = int(exc.code)
                headers = {k.lower(): v for k, v in (exc.headers or {}).items()}
            except (urllib.error.URLError, OSError) as exc:
                reason = getattr(exc, "reason", exc)
                raise fe.PreflightFailed(
                    f"GitLab {method} {path.split('?', 1)[0]} failed: {type(reason).__name__}"
                ) from None
            if status == 429 and attempt < RATE_LIMIT_RETRIES:
                wait = headers.get("retry-after") or "60"
                time.sleep(float(wait) if wait.isdigit() else 60)
                continue
            break
        if not raw:
            return status, None, headers
        try:
            return status, json.loads(raw), headers
        except ValueError:
            return status, raw.decode(errors="replace"), headers

    def _json(self, actor: str, method: str, path: str, body: Any = None) -> tuple[int, Any]:
        status, parsed, _headers = self._call(actor, method, path, body)
        return status, parsed

    def _send(self, actor: str, request: tuple[str, str, Any]) -> tuple[int, Any]:
        method, path, body = request
        return self._json(actor, method, path, body)

    def _paged(self, actor: str, path: str) -> list[dict[str, Any]]:
        """Every item of a listing, following x-next-page; raises on a failed page."""

        items: list[dict[str, Any]] = []
        joiner = "&" if "?" in path else "?"
        page = "1"
        for _ in range(MAX_PAGES):
            status, body, headers = self._call(
                actor, "GET", f"{path}{joiner}per_page={PAGE_SIZE}&page={page}"
            )
            if status != 200 or not isinstance(body, list):
                raise fe.PreflightFailed(
                    f"listing GitLab {path.split('?', 1)[0]} failed (HTTP {status})"
                )
            items.extend(item for item in body if isinstance(item, dict))
            page = str(headers.get("x-next-page") or "").strip()
            if not page:
                return items
        raise fe.PreflightFailed(f"listing GitLab {path.split('?', 1)[0]} has too many pages")

    @property
    def labeler(self) -> str:
        """The test actor that opens, labels, reviews and sets CI."""

        return "developer" if self.target == "fixture" else "human"

    @property
    def admin(self) -> str:
        """The actor that merges and resets: root on the fixture, the Owner on gitlab.com."""

        return "root" if self.target == "fixture" else "human"

    def _resolve_project(self) -> None:
        status, project = self._json(
            "human", "GET", f"/projects/{urllib.parse.quote(self.project_path, safe='')}"
        )
        if status != 200 or not isinstance(project, dict) or not project.get("id"):
            raise fe.PreflightFailed(f"resolving the GitLab test project failed (HTTP {status})")
        self._pid = int(project["id"])
        self._default_branch = str(project.get("default_branch") or "main")

    @property
    def pid(self) -> int:
        """The project's numeric id; on gitlab.com its path is resolved once."""

        if self._pid is None:
            self._resolve_project()
        assert self._pid is not None
        return self._pid

    @property
    def default_branch(self) -> str:
        if self._default_branch is None:
            self._resolve_project()
        assert self._default_branch is not None
        return self._default_branch

    @property
    def factory_username(self) -> str:
        """The factory account's username: the fixture's service account, or GET /user."""

        if self._factory_username is None:
            status, user = self._json("factory", "GET", "/user")
            name = user.get("username") if status == 200 and isinstance(user, dict) else None
            if not isinstance(name, str) or not name:
                raise fe.PreflightFailed(f"reading the factory account failed (HTTP {status})")
            self._factory_username = name
        return self._factory_username

    def _work_items_for(self, iid: int) -> list[dict[str, Any]] | None:
        """WorkItems whose tracker issue is ``iid`` on this project; None when unreadable."""

        status, body = self.p.api("GET", "/work-items", headers={"X-API-Key": self.p.api_key})
        if status != 200 or not isinstance(body, dict):
            return None
        found = []
        for item in body.get("items") or []:
            tracker = item.get("tracker") if isinstance(item, dict) else None
            if (
                isinstance(tracker, dict)
                and tracker.get("kind") == "gitlab"
                and str(tracker.get("issue_id")) == str(iid)
                and str(tracker.get("scope_id")) == str(self.pid)
            ):
                found.append(item)
        return found

    def _create_issue(self, actor: str, title: str, body: str) -> int:
        status, issue = self._json(
            actor, "POST", f"/projects/{self.pid}/issues", {"title": title, "description": body}
        )
        if status != 201 or not isinstance(issue, dict) or not issue.get("iid"):
            raise fe.PreflightFailed(f"creating the GitLab test issue failed (HTTP {status})")
        iid = int(issue["iid"])
        self.created.append(iid)
        return iid

    def _label(self, actor: str, iid: int) -> None:
        status, _ = self._send(actor, label_request(self.pid, iid, self.p.config.label))
        if status != 200:
            raise fe.PreflightFailed(
                f"labelling GitLab issue #{iid} as the {actor} failed (HTTP {status})"
            )

    # -- the ticket --

    def open_and_label_ticket(self, title: str, body: str, since: float) -> str:
        if self.target == "fixture" and (title, body) == fe.DEFAULT_LOOP_ISSUE:
            # The default loop ticket is about the GitHub fixture's code.
            title, body = FIXTURE_ISSUE
        iid = self._create_issue(self.labeler, title, body)
        self.iid = iid
        self.p.evidence["gitlab_issue_iid"] = iid
        self._label(self.labeler, iid)
        self.p.step(f"GitLab issue opened and labelled by the {self.labeler}", iid=iid)

        def admitted() -> list[dict[str, Any]] | None:
            return self._work_items_for(iid) or None

        items = fe._wait(
            f"a WorkItem for GitLab issue #{iid}",
            self.p.admission_timeout,
            admitted,
            ADMISSION_POLL_SECONDS,
        )
        if len(items) != 1:
            raise fe.PreflightFailed(
                f"GitLab issue #{iid} admitted {len(items)} WorkItems, expected 1"
            )
        work_item_id = str(items[0]["id"])
        self.p.evidence["work_item_id"] = work_item_id
        self.p.step("WorkItem admitted", work_item_id=work_item_id, iid=iid)
        return work_item_id

    # -- the merge request --

    def await_change(self, work_item_id: str) -> fe.ChangeRef | None:
        return fe.await_work_item_change(self.p, work_item_id)

    def _merge_request(self, iid: int) -> dict[str, Any] | None:
        status, mr = self._json(self.admin, "GET", f"/projects/{self.pid}/merge_requests/{iid}")
        return mr if status == 200 and isinstance(mr, dict) else None

    def change_head(self, change: fe.ChangeRef) -> tuple[str | None, int | None]:
        mr = self._merge_request(change.number)
        sha = mr.get("sha") if mr is not None else None
        try:
            commits = self._paged(
                self.admin, f"/projects/{self.pid}/merge_requests/{change.number}/commits"
            )
        except fe.PreflightFailed:
            return (sha if isinstance(sha, str) else None), None
        return (sha if isinstance(sha, str) else None), len(commits)

    def set_ci_red(self, sha: str) -> dict[str, Any]:
        status, body = self._send(self.labeler, status_request(self.pid, sha, "failed"))
        if status not in (200, 201) or not isinstance(body, dict):
            raise fe.PreflightFailed(f"setting the red loop gate status failed (HTTP {status})")
        reported = body.get("status")
        return {
            "sha": sha,
            "context": LOOP_GATE_NAME,
            "state": "failure" if reported == "failed" else reported,
        }

    def ci_state(self, sha: str) -> dict[str, Any]:
        try:
            statuses = self._paged(
                self.admin, f"/projects/{self.pid}/repository/commits/{sha}/statuses"
            )
        except fe.PreflightFailed:
            return {"sha": sha, "state": "unreadable"}
        return {
            "sha": sha,
            "state": gitlab_ci_state(statuses),
            "statuses": [
                {
                    "name": s.get("name"),
                    "status": s.get("status"),
                    "allow_failure": s.get("allow_failure"),
                }
                for s in statuses
            ],
        }

    def _diff_position(self, iid: int) -> tuple[str, int, int | None]:
        diffs = self._paged(self.labeler, f"/projects/{self.pid}/merge_requests/{iid}/diffs")
        for item in diffs:
            if item.get("deleted_file") or not item.get("new_path"):
                continue
            found = diff_line(str(item.get("diff") or ""))
            if found is not None:
                return str(item["new_path"]), found[0], found[1]
        raise fe.PreflightFailed(f"merge request !{iid} has no line a diff comment can name")

    def _request_count(self, work_item_id: str) -> int | None:
        detail = self.p.work_item_detail(work_item_id)
        if detail is None:
            return None
        return len([r for r in detail.get("requests") or [] if isinstance(r, dict)])

    def post_review(self, change: fe.ChangeRef, text: str) -> dict[str, Any]:
        if self.target == "fixture" and text == fe.DEFAULT_LOOP_REVIEW_TEXT:
            # The default review names the GitHub fixture's code.
            text = FIXTURE_REVIEW
        work_item_id = str(self.p.evidence.get("work_item_id") or "")
        before = self._request_count(work_item_id) or 0
        mr = self._merge_request(change.number)
        refs = mr.get("diff_refs") if mr is not None else None
        if not isinstance(refs, dict):
            raise fe.PreflightFailed(f"reading merge request !{change.number} failed")
        path, line, old_line = self._diff_position(change.number)
        body = fe.revision_comment_text(text, self.factory_username)
        status, discussion = self._send(
            self.labeler,
            diff_comment_request(
                self.pid,
                change.number,
                body,
                diff_refs=refs,
                path=path,
                line=line,
                old_line=old_line,
            ),
        )
        if status != 201 or not isinstance(discussion, dict) or not discussion.get("id"):
            raise fe.PreflightFailed(f"posting the diff comment failed (HTTP {status})")
        self.discussion_id = str(discussion["id"])
        notes = discussion.get("notes") or [{}]
        result: dict[str, Any] = {
            "discussion_id": self.discussion_id,
            "note_id": notes[0].get("id") if isinstance(notes[0], dict) else None,
            "path": path,
            "line": line,
        }

        def new_request() -> bool:
            count = self._request_count(work_item_id)
            return count is not None and count > before

        try:
            fe._wait(
                "a new WorkItem request for the diff comment",
                self.p.admission_timeout,
                new_request,
                ADMISSION_POLL_SECONDS,
            )
        except fe.PreflightFailed:
            return {**result, "delivery_status_code": None, "delivery_api_status": "not_admitted"}
        return {**result, "delivery_status_code": 200, "delivery_api_status": "factory_admitted"}

    def merge(self, change: fe.ChangeRef) -> dict[str, Any]:
        """Merge as root (fixture) or the Owner (gitlab.com), retrying while GitLab checks."""

        status = 0
        body: Any = None
        for attempt in range(MERGE_ATTEMPTS):
            status, body = self._send(self.admin, merge_request(self.pid, change.number))
            if status not in _MERGE_RETRY_STATUSES or attempt == MERGE_ATTEMPTS - 1:
                break
            time.sleep(MERGE_RETRY_SECONDS)
        merged = status == 200 and isinstance(body, dict) and body.get("state") == "merged"
        sha = body.get("merge_commit_sha") if isinstance(body, dict) else None
        return {"status_code": status, "merged": merged, "sha": sha, "actor": self.admin}

    def change_merged(self, change: fe.ChangeRef) -> bool | None:
        mr = self._merge_request(change.number)
        if mr is None:
            return None
        return mr.get("state") == "merged"

    def changes_opened(self) -> list[int]:
        listing = self._paged(
            self.admin, f"/projects/{self.pid}/merge_requests?state=all&order_by=created_at"
        )
        started = self.p.scenario_started
        numbers = []
        for item in listing:
            created = fe._parse_time(item.get("created_at"))
            if started is not None and (created is None or created < started):
                continue
            numbers.append(int(item["iid"]))
        return sorted(numbers)

    # -- the fixture project --

    def default_branch_head(self) -> str:
        ref = urllib.parse.quote(self.default_branch, safe="")
        status, branch = self._json(
            self.admin, "GET", f"/projects/{self.pid}/repository/branches/{ref}"
        )
        sha = (branch.get("commit") or {}).get("id") if isinstance(branch, dict) else None
        if status != 200 or not isinstance(sha, str):
            raise fe.PreflightFailed(f"could not read the default branch head (HTTP {status})")
        return sha

    def reset_fixture(self) -> dict[str, Any]:
        """Close every open issue and merge request, and delete every other branch."""

        base = f"/projects/{self.pid}"
        closed_issues = closed_mrs = deleted = 0
        for issue in self._paged(self.admin, f"{base}/issues?state=opened"):
            status, _ = self._json(
                self.admin, "PUT", f"{base}/issues/{issue['iid']}", {"state_event": "close"}
            )
            if status != 200:
                raise fe.PreflightFailed(f"closing issue #{issue['iid']} failed (HTTP {status})")
            closed_issues += 1
        for mr in self._paged(self.admin, f"{base}/merge_requests?state=opened"):
            status, _ = self._json(
                self.admin, "PUT", f"{base}/merge_requests/{mr['iid']}", {"state_event": "close"}
            )
            if status != 200:
                raise fe.PreflightFailed(
                    f"closing merge request !{mr['iid']} failed (HTTP {status})"
                )
            closed_mrs += 1
        for branch in self._paged(self.admin, f"{base}/repository/branches"):
            name = str(branch.get("name") or "")
            if not name or name == self.default_branch or branch.get("protected"):
                continue
            ref = urllib.parse.quote(name, safe="")
            status, _ = self._json(self.admin, "DELETE", f"{base}/repository/branches/{ref}")
            if status != 204:
                raise fe.PreflightFailed(f"deleting a fixture branch failed (HTTP {status})")
            deleted += 1
        return {
            "issues_closed": closed_issues,
            "merge_requests_closed": closed_mrs,
            "branches_deleted": deleted,
        }

    def pin_fixture_base(self) -> None:
        """Remember the default branch head now and put it back on teardown."""

        if self._restore_pushed:
            return
        self._base_sha = self.default_branch_head()
        self.p.teardown.push("restore fixture default branch", self.restore_fixture_base)
        self._restore_pushed = True

    def restore_fixture_base(self) -> dict[str, Any]:
        """Point the default branch back at the pinned commit.

        GitLab has no ref update API, and a protected default branch cannot be
        deleted, so this parks the default on a temporary branch, recreates
        the branch at the pinned commit with its protection, and moves the
        default back.
        """

        head = self.default_branch_head()
        if head == self._base_sha:
            return {"restored": True, "moved": False}
        base = f"/projects/{self.pid}"
        name = self.default_branch
        ref = urllib.parse.quote(name, safe="")
        park = f"factory-e2e-park-{self.p.run_id}"

        def ok(status: int, expected: tuple[int, ...], what: str) -> None:
            if status not in expected:
                raise fe.PreflightFailed(f"restoring the default branch: {what} (HTTP {status})")

        status, _ = self._json(
            self.admin,
            "POST",
            f"{base}/repository/branches",
            {"branch": park, "ref": self._base_sha},
        )
        ok(status, (201,), "creating the parking branch")
        status, _ = self._json(self.admin, "PUT", base, {"default_branch": park})
        ok(status, (200,), "parking the default branch")
        failure: fe.PreflightFailed | None = None
        try:
            self._recreate_default(base, name, ref, ok)
        except fe.PreflightFailed as exc:
            failure = exc
        # Always move the default back off the parking branch: left parked, the
        # reset step cannot delete it (GitLab refuses to delete HEAD, HTTP 400).
        status, _ = self._json(self.admin, "PUT", base, {"default_branch": name})
        park_status = 0
        if status == 200:
            park_status, _ = self._json(
                self.admin,
                "DELETE",
                f"{base}/repository/branches/{urllib.parse.quote(park, safe='')}",
            )
        if failure is not None:
            raise failure
        ok(status, (200,), "moving the default branch back")
        ok(park_status, (204,), "deleting the parking branch")
        return {"restored": self.default_branch_head() == self._base_sha, "moved": True}

    def _recreate_default(
        self, base: str, name: str, ref: str, ok: Callable[[int, tuple[int, ...], str], None]
    ) -> None:
        """Recreate the default branch at the pinned commit with its protection.

        GitLab keeps a default branch's protection rule after a 204 to its
        DELETE, and protects the recreated branch again on its own, so posting
        the rule answers 409 "already exists". That is success when the rule
        GitLab kept grants what the original did (observed on the fixture).
        """

        status, protection = self._json(self.admin, "GET", f"{base}/protected_branches/{ref}")
        protected = status == 200 and isinstance(protection, dict)
        if protected:
            status, _ = self._json(self.admin, "DELETE", f"{base}/protected_branches/{ref}")
            ok(status, (204,), "unprotecting the default branch")
        status, _ = self._json(self.admin, "DELETE", f"{base}/repository/branches/{ref}")
        ok(status, (204,), "deleting the moved default branch")
        status, _ = self._json(
            self.admin,
            "POST",
            f"{base}/repository/branches",
            {"branch": name, "ref": self._base_sha},
        )
        ok(status, (201,), "recreating the default branch")
        if not protected:
            return
        assert isinstance(protection, dict)
        wanted = _protection_levels(protection)
        status, _ = self._json(
            self.admin,
            "POST",
            f"{base}/protected_branches",
            {
                "name": name,
                "push_access_level": wanted[0],
                "merge_access_level": wanted[1],
                "allow_force_push": wanted[2],
            },
        )
        if status == 409:
            status, kept = self._json(self.admin, "GET", f"{base}/protected_branches/{ref}")
            if status == 200 and isinstance(kept, dict) and _protection_levels(kept) == wanted:
                return
            raise fe.PreflightFailed(
                "restoring the default branch: the protection GitLab kept differs from the "
                f"original (HTTP {status})"
            )
        ok(status, (201,), "protecting the default branch again")

    def teardowns(self) -> tuple[tuple[str, Callable[[], Any]], ...]:
        # "reset fixture repository" closes every issue and merge request the run opened.
        return ()

    # -- tracker evidence --

    def _thread_replies(self, change: fe.ChangeRef | None) -> int | None:
        if change is None or self.discussion_id is None:
            return 0
        status, discussion = self._json(
            self.admin,
            "GET",
            f"/projects/{self.pid}/merge_requests/{change.number}/discussions/{self.discussion_id}",
        )
        if status != 200 or not isinstance(discussion, dict):
            return None
        factory = self.factory_username
        return sum(
            1
            for note in discussion.get("notes") or []
            if isinstance(note, dict)
            and not note.get("system")
            and (note.get("author") or {}).get("username") == factory
        )

    def _closes_issue(self, change: fe.ChangeRef | None) -> bool | None:
        if change is None or self.iid is None:
            return False
        mr = self._merge_request(change.number)
        if mr is None:
            return None
        return closes_issue(mr.get("description"), self.iid)

    def _reporter_probe(self) -> dict[str, Any]:
        """Open and label a second issue as reporter; nothing may be admitted or labelled."""

        iid = self._create_issue("reporter", REPORTER_PROBE_TITLE, REPORTER_PROBE_BODY)
        self._label("reporter", iid)
        self.p.step("GitLab issue labelled by the reporter", iid=iid)
        deadline = time.monotonic() + REPORTER_PROBE_SECONDS
        read = False
        admitted: bool | None = None
        while True:
            items = self._work_items_for(iid)
            if items:
                admitted = True
                break
            read = read or items is not None
            if time.monotonic() > deadline:
                admitted = False if read else None
                break
            time.sleep(fe.POLL_SECONDS)
        status, issue = self._json(self.admin, "GET", f"/projects/{self.pid}/issues/{iid}")
        labels = issue.get("labels") if status == 200 and isinstance(issue, dict) else None
        state_labels = (
            sorted(str(label) for label in labels if str(label).startswith(STATE_LABEL_PREFIX))
            if isinstance(labels, list)
            else None
        )
        return {
            "reporter_iid": iid,
            "reporter_label_admitted": admitted,
            "reporter_state_labels": state_labels,
        }

    def tracker_evidence(self, change: fe.ChangeRef | None, work_item_id: str) -> dict[str, Any]:
        evidence: dict[str, Any] = {
            "iid": self.iid,
            "closes_issue": self._closes_issue(change),
            "thread_replies": self._thread_replies(change),
        }
        if self.target == "fixture":
            evidence.update(self._reporter_probe())
        return evidence

    def judge_tracker(self, evidence: Mapping[str, Any]) -> list[str]:
        failures: list[str] = []
        if evidence.get("closes_issue") is not True:
            failures.append("the merge request description does not carry Closes #<iid>")
        replies = evidence.get("thread_replies")
        if replies != 1:
            failures.append(f"the review thread has {replies} factory replies, expected 1")
        if self.target == "fixture":
            admitted = evidence.get("reporter_label_admitted")
            if admitted is not False:
                failures.append(
                    f"a reporter's label admitted a WorkItem: {admitted!r}, expected False"
                )
            labels = evidence.get("reporter_state_labels")
            if labels != []:
                failures.append(f"a reporter's label set state labels {labels!r}, expected []")
        return failures


def _protection_levels(rule: Mapping[str, Any]) -> tuple[int, int, bool]:
    """A protected branch rule's push level, merge level and force push flag."""

    return (
        _max_level(rule.get("push_access_levels")),
        _max_level(rule.get("merge_access_levels")),
        bool(rule.get("allow_force_push")),
    )


def _max_level(levels: Any) -> int:
    """The highest access level a protected branch rule grants, Maintainer when unset."""

    found = [
        int(item["access_level"])
        for item in levels or []
        if isinstance(item, dict) and isinstance(item.get("access_level"), int)
    ]
    return max(found) if found else 40


# --- the entry's setup: the quickstart --------------------------------------------------


def install_gitlab(p: fe.Preflight) -> None:
    """Bind the factory agent to the GitLab project with `curie factory quickstart`.

    On the fixture the factory's token is the seeded service account's, the
    forge URL is the fixture's advertised base (the quickstart derives the
    egress entry for its host and port from it), and the fixture CA goes in
    with ``--ca-file``. On gitlab.com the forge URL is the quickstart's
    default and the token is the factory token file.
    """

    if p.config.label != QUICKSTART_LABEL:
        raise fe.ConfigError(
            f"the GitLab tracker label is {QUICKSTART_LABEL}; unset CURIE_FACTORY_LABEL for "
            "a gitlab run"
        )
    driver = p.loop_driver()
    if not isinstance(driver, GitLabDriver):
        raise fe.ConfigError("install_gitlab needs the gitlab forge driver")
    forge: list[str]
    if driver.target == "fixture":
        token_file = Path(str(driver.state["tokens"]["service_account"]["file"]))
        forge = [
            "--forge-url",
            str(driver.state["base_url"]),
            "--ca-file",
            str(driver.state["ca_path"]),
        ]
    else:
        token_file = driver.factory_token_file
        forge = []
    assert p.chart_dir is not None, "install() extracts the chart first"
    bundle = p.build_runner_layer(p.config.bundle_dir)
    argv = [
        p.config.curie_bin,
        "factory",
        "quickstart",
        "--context",
        p.config.kube_context,
        "--namespace",
        p.namespace,
        "--release",
        p.release,
        "--agent",
        fe.FACTORY_AGENT,
        "--plugin-dir",
        str(bundle),
        "--chart",
        str(p.chart_dir),
        "--repo",
        driver.project_path,
        "--execution-deadline",
        str(fe.EXECUTION_BOUND_SECONDS),
        "--code-host",
        "gitlab",
        "--tracker",
        "gitlab",
        "--token-file",
        str(token_file),
        *forge,
    ]
    env = {
        **os.environ,
        "KUBECONFIG": str(p._write_kubeconfig()),
        "CURIE_API_KEY": p.api_key,
        "CURIE_CONFIG_DIR": str(p._curie_config_dir()),
    }
    fe.log(f"curie factory quickstart --code-host gitlab --tracker gitlab ({driver.target})")
    result = subprocess.run(argv, capture_output=True, text=True, env=env, check=False)
    if result.returncode != 0:
        secrets = [
            p.api_key,
            p.config.model_api_key,
            p.worker_token,
            _read_token(token_file, "factory token file"),
        ]
        tail = fe._redact((result.stderr or result.stdout).strip(), secrets)[-1500:]
        raise fe.PreflightFailed(f"curie factory quickstart --code-host gitlab failed: {tail}")
    p.evidence["gitlab"] = {"project": driver.project_path, "target": driver.target}
    p.step("factory agent bound to the GitLab project", agent=fe.FACTORY_AGENT)
    if driver.target == "fixture":
        allow_fixture_bridge(p, driver.state.get("docker_subnets"), int(driver.state["port"]))


def fixture_bridge_egress(
    values: Mapping[str, Any], subnets: Sequence[str], port: int, kind: str = "gitlab"
) -> dict[str, Any]:
    """The factory values that also admit the fixture's Docker bridge subnets.

    ``kind`` is the forge kind whose entries get the subnets: ``gitlab``, or
    ``bitbucket_dc`` for the Bitbucket Data Center fixture, which is shaped
    the same way. The quickstart derives the egress entry from the advertised host.
    When the fixture runs on the Kubernetes node itself, Docker's DNAT rewrites
    a pod's connection to the container's bridge address before the
    NetworkPolicy decides, so the publication push is refused unless the
    bridge subnet on the fixture port is allowed too. Only the e2e fixture is
    shaped like this; a real GitLab is never co-located with a node's Docker.
    """

    factory = values.get("factory")
    if not isinstance(factory, Mapping):
        raise fe.PreflightFailed("the release has no factory values after the quickstart")
    extra = [{"cidr": s, "ports": [{"protocol": "TCP", "port": port}]} for s in subnets]
    out: dict[str, Any] = {}
    for key in ("codeHosts", "trackers"):
        entries = factory.get(key) or []
        updated = []
        for entry in entries:
            entry = dict(entry)
            if entry.get("kind") == kind:
                egress = list(entry.get("egress") or [])
                if not egress:
                    raise fe.PreflightFailed(f"factory.{key} {kind} entry has no egress")
                entry["egress"] = egress + [e for e in extra if e not in egress]
            updated.append(entry)
        out[key] = updated
    if not any(e.get("kind") == kind for e in out["codeHosts"]):
        raise fe.PreflightFailed(f"the quickstart wrote no {kind} code host")
    return {"factory": out}


def allow_fixture_bridge(p: fe.Preflight, subnets: Any, port: int, kind: str = "gitlab") -> None:
    """Admit the fixture's Docker bridge ``subnets`` on ``port`` for ``kind``'s entries."""

    if not isinstance(subnets, list) or not subnets:
        raise fe.ConfigError("the fixture state names no docker_subnets; bring it up again")
    assert p.chart_dir is not None, "install() extracts the chart first"
    context = p.config.kube_context
    raw = fe.run(
        ["helm", "--kube-context", context, "get", "values", p.release, "-n", p.namespace]
        + ["-o", "json"]
    )
    values = fixture_bridge_egress(json.loads(raw), subnets, port, kind)
    values_file = p.workdir / f"{kind}-fixture-egress.json"
    fd = os.open(str(values_file), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as handle:
        json.dump(values, handle)
    fe.run(
        fe.helm_upgrade_command(
            context=context,
            release=p.release,
            chart=str(p.chart_dir),
            namespace=p.namespace,
            values_file=str(values_file),
            timeout="20m",
        )
    )
    for resource in ("deployment", "statefulset"):
        for workload in p.kubectl("-n", p.namespace, "get", resource, "-o", "name").split():
            p.kubectl("-n", p.namespace, "rollout", "status", workload, "--timeout=15m")
    p.ensure_api()
    p.step(f"fixture bridge subnets allowed for the {kind} code host", subnets=len(subnets))


def _driver(p: fe.Preflight) -> GitLabDriver:
    target = p.evidence.get("forge_target")
    if target not in TARGETS:
        raise fe.ConfigError(
            f"the gitlab forge needs --forge-target, one of {', '.join(TARGETS)} (got {target!r})"
        )
    if target == "fixture" and not os.environ.get(FIXTURE_VAR):
        # Read by GitLabDriver and inherited by the quickstart; this run owns it.
        os.environ[FIXTURE_VAR] = str(bring_up_fixture(p))
    return GitLabDriver(p, target=target)


fe.register_forge(
    fe.ForgeEntry(
        name="gitlab",
        driver=_driver,
        installed=True,
        setup=install_gitlab,
        targets=TARGETS,
        github_app=False,
    )
)
