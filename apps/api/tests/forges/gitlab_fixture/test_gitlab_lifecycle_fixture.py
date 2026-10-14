"""AC 8 of #4399 against the live GitLab CE 19.4.1 fixture: the factory lifecycle.

The same ``run_*`` steps the offline suite runs over the fake
(``tests/forges/test_gitlab_lifecycle_offline.py``), with `LiveBackend` as the
install's GitLab: the API boots on a ``CURIE_FORGES`` that names the fixture's
widgets project and trusts the fixture CA, and every tracker poll, CI read,
rerun, merge request, note and reply goes over HTTPS to real GitLab.

People act through the root token with ``Sudo: <username>``. The factory's
identity is a token minted for the seeded service account per test (and revoked
afterwards), so the failure test can revoke it and the no-token checks look
for that test's secret only. The publication Job's push is a real ``git push``
with that token as a Basic header in the environment. CI is the fixture's shell
runner. ``main`` is red by seed, so the first push also adds a ``lifecycle``
job that ``main`` lacks and that fails on the branch (a failure ``main`` shares
is pre-existing to the gate, #4105); the fix push makes the pipeline green.
Every test uses a fresh trigger label, issues and branch, so the suite
reruns on the same fixture.

Select with ``CURIE_GITLAB_FIXTURE=<dir>/state.json uv run pytest apps/api/tests
-k gitlab_lifecycle``; without the variable these tests skip.
"""

from __future__ import annotations

import os
import ssl
import subprocess
import time
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx
import pytest
from curie_api.forges.types import Actor
from forge_fakes import gitlab_lifecycle as gl
from forge_fakes.gitlab import code_host_entry, tracker_entry
from forge_fakes.gitlab_lifecycle import GitLabInstall, MergeRequest, Note

from .conftest import BINDING_NAME, CODE_HOST_NAME, TRACKER_NAME, GitLabApi, encode, unique

pytestmark = pytest.mark.usefixtures("clean_db")

CI_TIMEOUT_S = 300.0
BUGGY = "add() { echo $(( $1 - $2 )); }"
FIXED = "add() { echo $(( $1 + $2 )); }"
CHANGED_FILE = "lifecycle.txt"
# The seeded ``main`` is red on purpose (its ``test`` job fails), and the gate
# judges a failure whose key also fails on the base head as pre-existing (#4105):
# no rerun, no fix round. So the factory's first push adds a job ``main`` does
# not have, failing while ``RED_MARK`` exists; the fix push removes the mark and
# fixes ``widget.sh``, so the whole pipeline goes green.
RED_JOB = "lifecycle"
RED_MARK = "lifecycle.red"
RED_JOB_YAML = f"""
{RED_JOB}:
  script:
    - test ! -e {RED_MARK}
"""
FINISHED = ("success", "failed", "canceled")


def _wait(what: str, probe: Callable[[], Any], *, timeout_s: float, every_s: float = 2.0) -> Any:
    deadline = time.monotonic() + timeout_s
    while True:
        found = probe()
        if found:
            return found
        if time.monotonic() >= deadline:
            pytest.fail(f"timed out after {timeout_s:.0f} s waiting for {what}")
        time.sleep(every_s)


class LiveBackend:
    """The live fixture's ``widgets`` project as the install's GitLab."""

    def __init__(self, state: Mapping[str, Any], work: Path) -> None:
        self.state = state
        self.root = GitLabApi(state, state["root_token_file"])
        widgets = state["projects"]["widgets"]
        self.project_id = int(widgets["id"])
        self.project_path = str(widgets["path"])
        self.api_url = str(state["local_api_url"])
        self.host = urlsplit(self.api_url).netloc
        self.origin = str(state["base_url"])
        self.ca_bundle = str(state["ca_path"])
        self.wait_budget_s = 900
        self.label = unique("factory")
        self.developer = self._actor("developer")
        self.reporter = self._actor("reporter")
        self.bot = self._actor("service_account")
        self._work = work
        self._clone: Path | None = None
        self._issues: list[int] = []
        self._branches: list[str] = []
        self._revoked = False
        # The factory's own token, minted for this test only.
        account = state["users"]["service_account"]
        group = state["group"]["id"]
        path = (
            f"/groups/{group}/service_accounts/{account['id']}/personal_access_tokens"
            if account.get("kind") == "group_service_account"
            else f"/users/{account['id']}/personal_access_tokens"
        )
        expires = (datetime.now(UTC) + timedelta(days=2)).date().isoformat()
        minted = self.root.call(
            "POST",
            path,
            json={
                "name": unique("curie-lifecycle"),
                "scopes": ["api", "write_repository"],
                "expires_at": expires,
            },
        )
        self.token: str = minted["token"]
        self._token_id = int(minted["id"])
        self._expires = f"{minted['expires_at']}T00:00:00Z"

    def __repr__(self) -> str:
        return f"LiveBackend(project={self.project_path!r}, label={self.label!r})"

    def _actor(self, who: str) -> Actor:
        user = self.state["users"][who]
        return Actor(str(user["id"]), str(user["username"]))

    @property
    def _project(self) -> str:
        return f"/projects/{self.project_id}"

    def forges_document(self) -> dict[str, Any]:
        return {
            "code_hosts": [
                code_host_entry(
                    name=CODE_HOST_NAME,
                    api_url=self.api_url,
                    host=self.host,
                    token=self.token,
                    expires_at=self._expires,
                )
            ],
            "trackers": [
                tracker_entry(
                    name=TRACKER_NAME,
                    api_url=self.api_url,
                    host=self.host,
                    token=self.token,
                    label=self.label,
                    mention=self.bot.login,
                    expires_at=self._expires,
                )
            ],
            "bindings": [
                {
                    "name": BINDING_NAME,
                    "tracker": TRACKER_NAME,
                    "tracker_scope_id": str(self.project_id),
                    "repos": [
                        {
                            "alias": BINDING_NAME,
                            "code_host": CODE_HOST_NAME,
                            "project_id": str(self.project_id),
                            "path": self.project_path,
                        }
                    ],
                }
            ],
        }

    def stop(self) -> None:
        # Best effort: leave the fixture tidy for the next run.
        for branch in self._branches:
            for mr in self._merge_requests(branch):
                if mr["state"] == "opened":
                    self.root.request(
                        "PUT",
                        f"{self._project}/merge_requests/{mr['iid']}",
                        json={"state_event": "close"},
                    )
        for iid in self._issues:
            self.root.request("PUT", f"{self._project}/issues/{iid}", json={"state_event": "close"})
        if not self._revoked:
            self.root.request("DELETE", f"/personal_access_tokens/{self._token_id}")

    def default_head(self) -> str:
        branch = self.root.call("GET", f"{self._project}/repository/branches/main")
        return str(branch["commit"]["id"])

    # Issues ------------------------------------------------------------------

    def add_issue(self, title: str, description: str = "", *, author: Actor | None = None) -> int:
        created = self.root.call(
            "POST",
            f"{self._project}/issues",
            sudo=author.login if author is not None else None,
            json={"title": f"{title} ({self.label})", "description": description},
        )
        iid = int(created["iid"])
        self._issues.append(iid)
        return iid

    def label_issue(self, iid: int, label: str, actor: Actor, *, add: bool = True) -> None:
        self.root.call(
            "PUT",
            f"{self._project}/issues/{iid}",
            sudo=actor.login,
            json={"add_labels" if add else "remove_labels": label},
        )

    def issue_note(self, iid: int, actor: Actor, body: str) -> int:
        created = self.root.call(
            "POST", f"{self._project}/issues/{iid}/notes", sudo=actor.login, json={"body": body}
        )
        return int(created["id"])

    def issue_labels(self, iid: int) -> set[str]:
        issue = self.root.call("GET", f"{self._project}/issues/{iid}")
        return set(issue["labels"])

    def _issue_notes(self, iid: int) -> list[dict[str, Any]]:
        notes: list[dict[str, Any]] = self.root.call(
            "GET",
            f"{self._project}/issues/{iid}/notes",
            params={"sort": "asc", "order_by": "created_at", "per_page": 100},
        )
        return notes

    def issue_notes(self, iid: int) -> list[Note]:
        return [
            Note(int(n["id"]), str(n["author"]["id"]), n["body"])
            for n in self._issue_notes(iid)
            if not n["system"]
        ]

    # Repository and merge requests ---------------------------------------------

    def _git(self, *args: str, cwd: Path | None = None) -> str:
        """git with the factory token as a Basic header and the fixture CA, both
        through the environment only: never argv, never a config file."""

        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        env.update(
            {
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_SSL_CAINFO": self.ca_bundle,
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": f"http.{self.state['local_base_url']}/.extraHeader",
                "GIT_CONFIG_VALUE_0": f"Authorization: {gl.basic_oauth2(self.token)}",
                "GIT_AUTHOR_NAME": "placeholder-user-1",
                "GIT_AUTHOR_EMAIL": "placeholder-user-1@example.invalid",
                "GIT_COMMITTER_NAME": "placeholder-user-1",
                "GIT_COMMITTER_EMAIL": "placeholder-user-1@example.invalid",
            }
        )
        done = subprocess.run(
            ["git", *args], cwd=cwd, env=env, capture_output=True, text=True, check=False
        )
        if done.returncode != 0:
            pytest.fail(f"git {args[0]} failed: {done.stderr.replace(self.token, '***')}")
        return done.stdout

    def push(self, branch: str, message: str, *, fix: bool = False) -> str:
        if self._clone is None:
            self._work.mkdir(parents=True, exist_ok=True)
            self._clone = self._work / "widgets"
            url = self.state["projects"]["widgets"]["http_url_to_repo"]
            self._git("clone", "--quiet", url, str(self._clone))
        clone = self._clone
        if branch not in self._branches:
            self._git("checkout", "--quiet", "-B", branch, "origin/main", cwd=clone)
            self._branches.append(branch)
        else:
            self._git("checkout", "--quiet", branch, cwd=clone)
        if fix:
            widget = (clone / "widget.sh").read_text()
            assert BUGGY in widget
            (clone / "widget.sh").write_text(widget.replace(BUGGY, FIXED))
            self._git("add", "widget.sh", cwd=clone)
            if (clone / RED_MARK).exists():
                self._git("rm", "--quiet", RED_MARK, cwd=clone)
        else:
            changed = clone / CHANGED_FILE
            first = not changed.exists()
            changed.write_text((changed.read_text() if not first else "") + f"{message}\n")
            self._git("add", CHANGED_FILE, cwd=clone)
            if first:
                ci = clone / ".gitlab-ci.yml"
                ci.write_text(ci.read_text() + RED_JOB_YAML)
                (clone / RED_MARK).write_text("red\n")
                self._git("add", ".gitlab-ci.yml", RED_MARK, cwd=clone)
        self._git("commit", "--quiet", "-m", message, cwd=clone)
        self._git("push", "--quiet", "origin", f"HEAD:refs/heads/{branch}", cwd=clone)
        return self._git("rev-parse", "HEAD", cwd=clone).strip()

    def _merge_requests(self, branch: str) -> list[dict[str, Any]]:
        found: list[dict[str, Any]] = self.root.call(
            "GET",
            f"{self._project}/merge_requests",
            params={"source_branch": branch, "state": "all", "per_page": 100},
        )
        return found

    def merge_request(self, branch: str) -> MergeRequest:
        (mr,) = self._merge_requests(branch)
        return MergeRequest(
            int(mr["iid"]), mr["source_branch"], mr["target_branch"], mr["description"] or ""
        )

    # CI ----------------------------------------------------------------------------

    def _latest_pipeline(self, sha: str) -> dict[str, Any] | None:
        pipelines = self.root.call(
            "GET", f"{self._project}/pipelines", params={"sha": sha, "per_page": 100}
        )
        return max(pipelines, key=lambda p: p["id"]) if pipelines else None

    def _settled(self, sha: str, *, newer_than: str | None = None) -> dict[str, Any] | None:
        """The finished pipeline on ``sha`` with its current (unretried) jobs by
        name, once its ``RED_JOB`` (when it has one) is not job ``newer_than``."""

        pipeline = self._latest_pipeline(sha)
        if pipeline is None or pipeline["status"] not in FINISHED:
            return None
        jobs = self.root.call("GET", f"{self._project}/pipelines/{pipeline['id']}/jobs")
        named = {j["name"]: j for j in sorted(jobs, key=lambda j: j["id"])}
        if any(j["status"] not in FINISHED for j in named.values()):
            return None
        red = named.get(RED_JOB)
        if newer_than is not None and (red is None or str(red["id"]) == newer_than):
            return None
        return {"status": pipeline["status"], "jobs": named}

    def ci_fails(self, head: str) -> str:
        settled = _wait(
            f"the pipeline on {head[:12]} to finish",
            lambda: self._settled(head),
            timeout_s=CI_TIMEOUT_S,
        )
        red = settled["jobs"][RED_JOB]
        assert (settled["status"], red["status"]) == ("failed", "failed"), settled
        return str(red["id"])

    def rerun_fails(self, head: str, job: str) -> None:
        # Only the gate retries: a newer RED_JOB on the head's pipeline is its rerun.
        settled = _wait(
            f"the gate's rerun of job {job} to finish",
            lambda: self._settled(head, newer_than=job),
            timeout_s=CI_TIMEOUT_S,
        )
        assert settled["jobs"][RED_JOB]["status"] == "failed", settled
        old = self.root.call("GET", f"{self._project}/jobs/{job}")
        assert (old["status"], old["name"]) == ("failed", RED_JOB)

    def ci_passes(self, head: str) -> None:
        settled = _wait(
            f"the pipeline on {head[:12]} to finish",
            lambda: self._settled(head),
            timeout_s=CI_TIMEOUT_S,
        )
        assert settled["status"] == "success", settled

    # Discussions -------------------------------------------------------------------

    def mr_discussion(self, iid: int, actor: Actor, body: str) -> str:
        mr_path = f"{self._project}/merge_requests/{iid}"

        def opened() -> dict[str, Any] | None:
            refs = self.root.call("GET", mr_path).get("diff_refs") or {}
            if not refs.get("head_sha"):
                return None
            answer = self.root.request(
                "POST",
                f"{mr_path}/discussions",
                sudo=actor.login,
                json={
                    "body": body,
                    "position": {
                        "position_type": "text",
                        "base_sha": refs["base_sha"],
                        "start_sha": refs["start_sha"],
                        "head_sha": refs["head_sha"],
                        "new_path": CHANGED_FILE,
                        "new_line": 1,
                    },
                },
            )
            return answer.json() if answer.status_code == 201 else None

        discussion = _wait("the diff discussion to open", opened, timeout_s=60)
        assert discussion["notes"][0]["type"] == "DiffNote", discussion["notes"][0]["type"]
        return str(discussion["id"])

    def thread_replies(self, iid: int, discussion: str) -> list[Note]:
        found = self.root.call(
            "GET", f"{self._project}/merge_requests/{iid}/discussions/{encode(discussion)}"
        )
        return [
            Note(int(n["id"]), str(n["author"]["id"]), n["body"])
            for n in found["notes"][1:]
            if not n["system"]
        ]

    # Token checks ------------------------------------------------------------------

    def published_texts(self) -> list[str]:
        texts: list[str] = []
        for iid in self._issues:
            texts.extend(n["body"] for n in self._issue_notes(iid))
        for branch in self._branches:
            for mr in self._merge_requests(branch):
                texts.extend([mr["title"], mr["description"] or ""])
                notes = self.root.call(
                    "GET",
                    f"{self._project}/merge_requests/{mr['iid']}/notes",
                    params={"per_page": 100},
                )
                texts.extend(n["body"] for n in notes)
        return texts

    def break_credential(self) -> None:
        self.root.call("DELETE", f"/personal_access_tokens/{self._token_id}")
        self._revoked = True

    def credential_was_refused(self) -> None:
        answer = httpx.get(
            f"{self.api_url}/version",
            headers={"PRIVATE-TOKEN": self.token},
            verify=ssl.create_default_context(cafile=self.ca_bundle),
            timeout=15.0,
        )
        assert answer.status_code == 401, answer.status_code


@pytest.fixture
def install(
    state: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[GitLabInstall]:
    backend = LiveBackend(state, tmp_path / "work")
    yield from gl.gitlab_install(monkeypatch, tmp_path, backend=backend)


@pytest.fixture
def two_surfaces(
    state: dict[str, Any], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Iterator[GitLabInstall]:
    backend = LiveBackend(state, tmp_path / "work")
    yield from gl.gitlab_install(
        monkeypatch, tmp_path, labels=(None, f"{backend.label}-fast"), backend=backend
    )


def test_gitlab_lifecycle_fixture_label_to_merge_request_ci_review_and_cancel(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    gl.run_label_to_merge_request_ci_review_and_cancel(install, caplog)


def test_gitlab_lifecycle_fixture_a_reporter_label_admits_nothing(
    install: GitLabInstall,
) -> None:
    gl.run_reporter_label_admits_nothing(install)


def test_gitlab_lifecycle_fixture_two_surfaces_admit_only_their_own_label(
    two_surfaces: GitLabInstall,
) -> None:
    gl.run_two_surfaces_admit_only_their_own_label(two_surfaces)


def test_gitlab_lifecycle_fixture_an_issue_mention_links_the_note(
    install: GitLabInstall,
) -> None:
    gl.run_issue_mention_links_the_note(install)


def test_gitlab_lifecycle_fixture_failures_log_and_answer_no_token(
    install: GitLabInstall, caplog: pytest.LogCaptureFixture
) -> None:
    gl.run_failures_log_and_answer_no_token(install, caplog)
