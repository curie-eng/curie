"""GitHub REST and webhook fakes shared by the factory intake suites.

Payload shapes follow GitHub's webhook catalog:
https://docs.github.com/en/webhooks/webhook-events-and-payloads
"""

from __future__ import annotations

import hashlib
import hmac
import itertools
import json
import re
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import unquote

import httpx
from curie_api.config import get_settings
from curie_api.factory_ci import CiView
from curie_api.forges.github.ci import CiDetail
from curie_api.forges.github.code_host import base_rollup, diagnostics_of, normalize_checks
from curie_api.github_app import GitHubInstallationRefused
from fastapi.testclient import TestClient

REPO = "acme-corp/acme-bot"

REPO_ID = 4401
INSTALLATION_ID = 5501
SENDER_ID = 6601
SENDER = "octocat"
LABEL = "factory"
MENTION = "curie"

BASE_SHA = "0" * 39 + "1"

_ENV = {
    "GITHUB_FACTORY_INGRESS_ENABLED": "true",
    "GITHUB_FACTORY_LABEL": LABEL,
    "GITHUB_FACTORY_MENTION": MENTION,
    "GITHUB_REVIEW_INGRESS_ENABLED": "false",
    "GITHUB_APP_ID": "51",
    "GITHUB_APP_PRIVATE_KEY": "example-private-key",
    "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
    "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
    "GITHUB_TOKEN": "",
    "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
    "RESUME_RECONCILER_ENABLED": "false",
    "APPROVAL_SWEEP_INTERVAL_S": "0",
    "DEAD_LETTER_WATCH_INTERVAL_S": "0",
}


class GitHubAPI:
    """In-process stand-in for the GitHub REST reads the intake verifier makes."""

    def __init__(self) -> None:
        self.issue_state = "open"
        self.labels = [LABEL]
        self.permission = "write"
        self.permission_requests: list[httpx.Request] = []
        self.permission_user_id = SENDER_ID
        self.comment_body: str | None = None
        self.comment_app: dict[str, Any] | None = None
        self.repository_id = REPO_ID
        self.issue_number = 0
        # @spec apps/api/README.md#factory-test-isolation
        # Timeline events retain identity inside this stand-in while independent
        # fixtures cannot address each other's request-derived Valkey keys.
        self.label_event_base = uuid.uuid4().int >> 80
        self.label_event_ids: dict[int, int] = {}

    def advance_label_event(self, number: int) -> None:
        """Record a newer labeled timeline event. A relabel webhook reads it."""

        current = self.label_event_ids.get(number, self.label_event_base + number)
        self.label_event_ids[number] = current + 1

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/repos/{REPO}":
            return httpx.Response(
                200,
                json={"id": self.repository_id, "full_name": REPO, "default_branch": "main"},
            )
        if path.startswith(f"/repos/{REPO}/git/ref/heads/"):
            # Admission resolves the base and reads its commit through the
            # code host (#3095).
            # https://docs.github.com/en/rest/git/refs#get-a-reference
            name = path.removeprefix(f"/repos/{REPO}/git/ref/heads/")
            return httpx.Response(
                200, json={"ref": f"refs/heads/{name}", "object": {"sha": BASE_SHA}}
            )
        if path.startswith(f"/repos/{REPO}/issues/comments/"):
            comment_id = int(path.rsplit("/", 1)[1])
            return httpx.Response(
                200,
                json={
                    "id": comment_id,
                    "body": self.comment_body,
                    "user": {"id": SENDER_ID, "login": SENDER, "type": "User"},
                    "performed_via_github_app": self.comment_app,
                    "issue_url": (
                        f"https://api.github.com/repos/{REPO}/issues/{self.issue_number}"
                    ),
                },
            )
        if path.startswith(f"/repos/{REPO}/issues/") and path.endswith("/events"):
            number = int(path.split("/")[-2])
            event_id = self.label_event_ids.get(number, self.label_event_base + number)
            return httpx.Response(
                200,
                json=[
                    {
                        "id": event_id,
                        "event": "labeled",
                        "label": {"name": LABEL},
                        "actor": {"id": SENDER_ID, "login": SENDER, "type": "User"},
                        "created_at": "2026-09-01T00:00:00Z",
                    }
                ],
            )
        if path.startswith(f"/repos/{REPO}/issues/"):
            number = int(path.rsplit("/", 1)[1])
            return httpx.Response(
                200,
                json={
                    "number": number,
                    "state": self.issue_state,
                    "labels": [{"name": name} for name in self.labels],
                },
            )
        if path == f"/repos/{REPO}/collaborators/{SENDER}/permission":
            self.permission_requests.append(request)
            return httpx.Response(
                200,
                json={
                    "permission": self.permission,
                    "user": {"id": self.permission_user_id, "login": SENDER},
                },
            )
        return httpx.Response(404, json={"message": "missing fixture"})


class _Credentials:
    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        if repo != REPO or installation_id != INSTALLATION_ID:
            raise GitHubInstallationRefused("installation was not rediscovered")
        return "fixture-installation-token"

    def token_for(self, repo: str) -> str:
        """The code host's repository credential (`curie_api.repository_auth`)."""

        if repo != REPO:
            raise GitHubInstallationRefused("installation was not rediscovered")
        return "fixture-installation-token"


def _signature(body: bytes) -> str:
    secret = get_settings().github_webhook_secret.encode()
    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def _post(
    client: TestClient,
    event: str,
    payload: dict[str, Any],
    *,
    delivery: str | None = None,
    signature: str | None = None,
) -> httpx.Response:
    body = json.dumps(payload).encode()
    return client.post(
        "/github/webhook",
        content=body,
        headers={
            "X-GitHub-Delivery": delivery or str(uuid.uuid4()),
            "X-GitHub-Event": event,
            "X-Hub-Signature-256": signature if signature is not None else _signature(body),
            "Content-Type": "application/json",
        },
    )


def _sender(
    sender_type: str = "User", login: str = SENDER, sender_id: int = SENDER_ID
) -> dict[str, Any]:
    return {"id": sender_id, "login": login, "type": sender_type}


def _issue_event(action: str, number: int, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "action": action,
        "installation": {"id": INSTALLATION_ID},
        "repository": {"id": REPO_ID, "full_name": REPO},
        "sender": _sender(),
        "issue": {
            "number": number,
            "state": "open",
            "body": "do not store this issue body",
        },
    }
    payload.update(extra)
    return payload


def ci_view(detail: CiDetail) -> CiView:
    """A GitHub checks and statuses read as the CI gate sees it through the code
    host (`GitHubCodeHost.ci_diagnostics`), so gate tests can state GitHub data."""

    if detail.state != "observed" or detail.reason is not None:
        return CiView(detail.head_sha, reason=detail.reason or "github_error")
    head = detail.head_sha or ""
    base = base_rollup(detail)
    return CiView(
        head,
        normalize_checks(detail, head),
        diagnostics_of(detail),
        base_failing=base.failing_keys if base is not None else frozenset(),
    )


def _comment_event(number: int, body: str, comment_id: int) -> dict[str, Any]:
    return _issue_event(
        "created",
        number,
        comment={
            "id": comment_id,
            "body": body,
            "user": _sender(),
            "performed_via_github_app": None,
        },
    )


# --- The GitHub repository fake the forge contract suite drives (ADR 0197).
#
# A stateful stand-in for one repository's issues and code: the issue, label,
# timeline, comment and permission reads the tracker adapter makes, and the
# branch, commit, pull request, check run, commit status, Actions rerun, review
# and review comment calls the code host adapter makes. Issues and pull
# requests share one number space, as on GitHub. Listings honor ``per_page``,
# so the harness pages at two; a listing read can be made to fail once; every
# write is counted.
# https://docs.github.com/en/rest/issues/events#list-issue-events-for-a-repository
# https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository
# https://docs.github.com/en/rest/git/refs#get-a-reference
# https://docs.github.com/en/rest/pulls/pulls#list-pull-requests
# https://docs.github.com/en/rest/pulls/comments#create-a-reply-for-a-review-comment
# https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
# https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference
# https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
# https://docs.github.com/en/rest/actions/workflow-runs#re-run-failed-jobs-from-a-workflow-run

CONTRACT_APP_ID = 51
CONTRACT_BOT = {"id": 1000, "login": "curie-factory", "type": "Bot"}
_LISTINGS = (
    re.compile(rf"^/repos/{REPO}/issues/events$"),
    re.compile(rf"^/repos/{REPO}/issues/comments$"),
    re.compile(rf"^/repos/{REPO}/issues/\d+/events$"),
    re.compile(rf"^/repos/{REPO}/issues/\d+/comments$"),
    re.compile(rf"^/repos/{REPO}/pulls/\d+/comments$"),
    re.compile(rf"^/repos/{REPO}/pulls/\d+/reviews$"),
)
_RUN_STATES = {
    "success": ("completed", "success"),
    "failure": ("completed", "failure"),
    "neutral": ("completed", "neutral"),
    "skipped": ("completed", "skipped"),
    "cancelled": ("completed", "cancelled"),
    "pending": ("in_progress", None),
}
_STATUS_STATES = {"success": "success", "failure": "failure", "cancelled": "error"}


def _stamp(second: int) -> str:
    return (datetime(2026, 9, 1, tzinfo=UTC) + timedelta(seconds=second)).isoformat().replace(
        "+00:00", "Z"
    )


class GitHubRepositoryFake:
    """In-process GitHub REST state for one repository's issues and code."""

    def __init__(self) -> None:
        self.issues: dict[int, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.comments: list[dict[str, Any]] = []
        self.review_comments: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
        self.permissions: dict[str, tuple[int, str]] = {}
        self.branches: dict[str, str] = {}
        self.commits: dict[str, dict[str, Any]] = {}
        self.pulls: dict[int, dict[str, Any]] = {}
        self.check_runs: dict[str, dict[str, dict[str, Any]]] = {}
        self.statuses: dict[str, dict[str, dict[str, Any]]] = {}
        self.default_branch = "main"
        self.writes = 0
        self._event_ids = itertools.count(70001)
        self._comment_ids = itertools.count(80001)
        self._check_ids = itertools.count(90001)
        self._workflow_runs = itertools.count(60001)
        self._seconds = itertools.count(1)
        self._shas = itertools.count(1)
        self._fail_countdown: int | None = None
        self.seed_repository()

    # Seeding, from outside the adapters -----------------------------------

    def seed_repository(self) -> None:
        """The repository with one commit on its default branch."""

        self.push_head(self.default_branch)

    def _number(self) -> int:
        return len(self.issues) + 1

    def seed_issue(self, title: str, body: str) -> int:
        number = self._number()
        self.issues[number] = {
            "number": number,
            "title": title,
            "body": body,
            "state": "open",
            "labels": [],
            "user": {"id": SENDER_ID, "login": SENDER, "type": "User"},
        }
        return number

    def label_event(self, number: int, kind: str, label: str, actor: dict[str, Any]) -> None:
        names = [entry["name"] for entry in self.issues[number]["labels"]]
        if kind == "labeled" and label not in names:
            names.append(label)
        if kind == "unlabeled" and label in names:
            names.remove(label)
        self.issues[number]["labels"] = [{"name": name} for name in names]
        self.events.append(
            {
                "id": next(self._event_ids),
                "event": kind,
                "label": {"name": label},
                "actor": actor,
                "performed_via_github_app": None,
                "issue": {"number": number},
                "created_at": "2026-09-01T00:00:00Z",
            }
        )

    def add_comment(
        self, number: int, user: dict[str, Any], body: str, *, app: bool
    ) -> dict[str, Any]:
        """A comment on the issue or pull request conversation ``number``."""

        comment = {
            "id": next(self._comment_ids),
            "body": body,
            "user": user,
            "performed_via_github_app": {"id": CONTRACT_APP_ID} if app else None,
            "issue_url": f"https://api.github.com/repos/{REPO}/issues/{number}",
            "html_url": f"https://github.com/{REPO}/issues/{number}#issuecomment",
            "created_at": _stamp(next(self._seconds)),
            "_number": number,
        }
        self.comments.append(comment)
        return comment

    def add_review_comment(
        self,
        number: int,
        user: dict[str, Any],
        body: str,
        *,
        app: bool,
        in_reply_to: int | None = None,
    ) -> dict[str, Any]:
        """A review comment on pull request ``number``, opening or joining a thread."""

        comment: dict[str, Any] = {
            "id": next(self._comment_ids),
            "body": body,
            "user": user,
            "performed_via_github_app": {"id": CONTRACT_APP_ID} if app else None,
            "pull_request_url": f"https://api.github.com/repos/{REPO}/pulls/{number}",
            "commit_id": self._pull_payload(number)["head"]["sha"],
            "created_at": _stamp(next(self._seconds)),
            "_number": number,
        }
        if in_reply_to is not None:
            comment["in_reply_to_id"] = in_reply_to
        self.review_comments.append(comment)
        return comment

    def add_review(self, number: int, user: dict[str, Any], body: str) -> dict[str, Any]:
        review = {
            "id": next(self._comment_ids),
            "body": body,
            "user": user,
            "state": "COMMENTED",
            "commit_id": self._pull_payload(number)["head"]["sha"],
            "submitted_at": _stamp(next(self._seconds)),
            "_number": number,
        }
        self.reviews.append(review)
        return review

    def push_head(self, branch: str) -> str:
        """A new commit on ``branch``; returns its sha."""

        sha = f"{next(self._shas):040x}"
        parent = self.branches.get(branch)
        self.commits[sha] = {
            "sha": sha,
            "message": f"change {sha[-4:]}",
            "parents": [{"sha": parent}] if parent else [],
        }
        self.branches[branch] = sha
        return sha

    def report_check(self, sha: str, key: str, state: str) -> None:
        """A GitHub Actions check run named ``key``, or a commit status for a
        ``status:<context>`` key, in a ``CheckState`` value."""

        if key.startswith("status:"):
            context = key.removeprefix("status:")
            self.statuses.setdefault(sha, {})[context] = {
                "id": next(self._check_ids),
                "context": context,
                "state": _STATUS_STATES.get(state, "pending"),
                "description": "checks failed" if state == "failure" else "",
                "target_url": f"https://ci.example/{sha[:12]}/{context}",
                "created_at": _stamp(next(self._seconds)),
            }
            return
        runs = self.check_runs.setdefault(sha, {})
        job_id = next(self._check_ids)
        workflow = next(
            (run["_workflow_run"] for run in runs.values()), next(self._workflow_runs)
        )
        status, conclusion = _RUN_STATES[state]
        runs[key] = {
            "id": job_id,
            "name": key,
            "head_sha": sha,
            "status": status,
            "conclusion": conclusion,
            "started_at": _stamp(next(self._seconds)),
            "details_url": f"https://github.com/{REPO}/actions/runs/{workflow}/job/{job_id}",
            "html_url": f"https://github.com/{REPO}/runs/{job_id}",
            "app": {"slug": "github-actions"},
            "output": {"title": "assertion failed" if conclusion == "failure" else None},
            "_workflow_run": workflow,
        }

    def set_pull_state(self, number: int, *, merged: bool) -> None:
        """Close pull request ``number``, merging it when ``merged``."""

        pull = self.pulls[number]
        pull["state"] = "closed"
        pull["merged_at"] = _stamp(next(self._seconds)) if merged else None

    def fail_next_page(self, *, after: int = 0) -> None:
        self._fail_countdown = after

    # Transport ------------------------------------------------------------

    def _listing_fails(self, path: str) -> bool:
        if self._fail_countdown is None or not any(rx.match(path) for rx in _LISTINGS):
            return False
        if self._fail_countdown == 0:
            self._fail_countdown = None
            return True
        self._fail_countdown -= 1
        return False

    @staticmethod
    def _visible(item: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in item.items() if not key.startswith("_")}

    def _page(self, request: httpx.Request, items: list[dict[str, Any]]) -> httpx.Response:
        per_page = int(request.url.params.get("per_page", "30"))
        page = int(request.url.params.get("page", "1"))
        visible = [self._visible(item) for item in items[(page - 1) * per_page : page * per_page]]
        return httpx.Response(200, json=visible)

    def _found(self, items: list[dict[str, Any]], item_id: str) -> httpx.Response:
        found = [item for item in items if item["id"] == int(item_id)]
        if not found:
            return httpx.Response(404, json={"message": "Not Found"})
        return httpx.Response(200, json=self._visible(found[0]))

    def _pull_payload(self, number: int) -> dict[str, Any]:
        pull = self.pulls[number]
        side = {"repo": {"full_name": REPO}}
        return {
            "number": number,
            "html_url": f"https://github.com/{REPO}/pull/{number}",
            "state": pull["state"],
            "merged_at": pull["merged_at"],
            "title": pull["title"],
            "body": pull["body"],
            "draft": pull["draft"],
            "head": {"ref": pull["head"], "sha": self.branches[pull["head"]], **side},
            "base": {"ref": pull["base"], **side},
        }

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if self._listing_fails(path):
            return httpx.Response(502, json={"message": "injected"})
        if request.method != "GET":
            return self._write(request, path)
        return self._read(request, path) or self._read_code(request, path)

    def _read(self, request: httpx.Request, path: str) -> httpx.Response | None:
        if path == f"/repos/{REPO}/issues/events":
            return self._page(request, sorted(self.events, key=lambda e: -e["id"]))
        if path == f"/repos/{REPO}/issues/comments":
            newest = request.url.params.get("direction") == "desc"
            ordered = sorted(self.comments, key=lambda c: c["id"], reverse=newest)
            return self._page(request, ordered)
        matched = re.fullmatch(rf"/repos/{REPO}/issues/comments/(\d+)", path)
        if matched:
            return self._found(self.comments, matched[1])
        matched = re.fullmatch(rf"/repos/{REPO}/issues/(\d+)(/events|/comments)?", path)
        if matched:
            number = int(matched[1])
            if number not in self.issues:
                return httpx.Response(404, json={"message": "Not Found"})
            if matched[2] == "/events":
                own = [e for e in self.events if e["issue"]["number"] == number]
                return self._page(request, own)
            if matched[2] == "/comments":
                return self._page(request, [c for c in self.comments if c["_number"] == number])
            return httpx.Response(200, json=self.issues[number])
        matched = re.fullmatch(rf"/repos/{REPO}/collaborators/([^/]+)/permission", path)
        if matched:
            user_id, permission = self.permissions.get(matched[1], (0, "none"))
            return httpx.Response(
                200, json={"permission": permission, "user": {"id": user_id, "login": matched[1]}}
            )
        return None

    def _read_code(self, request: httpx.Request, path: str) -> httpx.Response:
        repo = f"/repos/{REPO}"
        if path == repo:
            return httpx.Response(
                200,
                json={"id": REPO_ID, "full_name": REPO, "default_branch": self.default_branch},
            )
        if path.startswith(f"{repo}/git/ref/heads/"):
            sha = self.branches.get(path.removeprefix(f"{repo}/git/ref/heads/"))
            if sha is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json={"object": {"sha": sha, "type": "commit"}})
        matched = re.fullmatch(rf"{repo}/git/commits/([0-9a-f]+)", path)
        if matched:
            commit = self.commits.get(matched[1])
            if commit is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json=commit)
        if path == f"{repo}/pulls":
            owner, _, branch = request.url.params.get("head", "").partition(":")
            assert owner == REPO.split("/")[0] and request.url.params.get("state") == "all"
            found = [self._pull_payload(n) for n, p in self.pulls.items() if p["head"] == branch]
            return httpx.Response(200, json=found)
        matched = re.fullmatch(rf"{repo}/pulls/(\d+)(/comments|/reviews)?(?:/(\d+))?", path)
        if matched:
            number = int(matched[1])
            if number not in self.pulls:
                return httpx.Response(404, json={"message": "Not Found"})
            listed = {"/comments": self.review_comments, "/reviews": self.reviews}.get(
                matched[2] or "", []
            )
            own = [item for item in listed if item["_number"] == number]
            if matched[3] is not None:
                return self._found(own, matched[3])
            if matched[2] is not None:
                return self._page(request, own)
            return httpx.Response(200, json=self._pull_payload(number))
        matched = re.fullmatch(rf"{repo}/pulls/comments/(\d+)", path)
        if matched:
            return self._found(self.review_comments, matched[1])
        matched = re.fullmatch(rf"{repo}/commits/([0-9a-f]+)/(check-runs|status)", path)
        if matched:
            if matched[2] == "status":
                statuses = list(self.statuses.get(matched[1], {}).values())
                return httpx.Response(
                    200, json={"state": "pending", "statuses": statuses, "total_count": 0}
                )
            runs = [self._visible(run) for run in self.check_runs.get(matched[1], {}).values()]
            return httpx.Response(200, json={"total_count": len(runs), "check_runs": runs})
        if re.fullmatch(rf"{repo}/check-runs/\d+/annotations", path):
            return httpx.Response(200, json=[])
        # No job log is kept, so diagnostics report it unavailable.
        return httpx.Response(404, json={"message": "missing fixture"})

    def _write(self, request: httpx.Request, path: str) -> httpx.Response:
        self.writes += 1
        body = json.loads(request.content) if request.content else {}
        matched = re.fullmatch(rf"/repos/{REPO}/issues/(\d+)/comments", path)
        if request.method == "POST" and matched:
            comment = self.add_comment(int(matched[1]), CONTRACT_BOT, body["body"], app=True)
            return httpx.Response(201, json={"id": comment["id"]})
        matched = re.fullmatch(rf"/repos/{REPO}/(issues|pulls)/comments/(\d+)", path)
        if request.method == "PATCH" and matched:
            listed = self.comments if matched[1] == "issues" else self.review_comments
            for comment in listed:
                if comment["id"] == int(matched[2]):
                    comment["body"] = body["body"]
                    return httpx.Response(200, json={"id": comment["id"]})
            return httpx.Response(404, json={"message": "Not Found"})
        matched = re.fullmatch(rf"/repos/{REPO}/issues/(\d+)/labels(?:/(.+))?", path)
        if matched and int(matched[1]) in self.issues:
            issue = self.issues[int(matched[1])]
            names = [entry["name"] for entry in issue["labels"]]
            if request.method == "POST":
                names.extend(name for name in body["labels"] if name not in names)
            elif matched[2] is not None and unquote(matched[2]) in names:
                names.remove(unquote(matched[2]))
            else:
                return httpx.Response(404, json={"message": "Label does not exist"})
            issue["labels"] = [{"name": name} for name in names]
            return httpx.Response(200, json=issue["labels"])
        return self._write_code(request, path, body)

    def _write_code(
        self, request: httpx.Request, path: str, body: dict[str, Any]
    ) -> httpx.Response:
        repo = f"/repos/{REPO}"
        if request.method == "POST" and path == f"{repo}/pulls":
            if body["head"] not in self.branches or body["base"] not in self.branches:
                return httpx.Response(422, json={"message": "Validation Failed"})
            number = self._number()
            self.issues[number] = {"number": number, "state": "open", "pull_request": {}}
            self.pulls[number] = {
                "head": body["head"],
                "base": body["base"],
                "title": body["title"],
                "body": body["body"],
                "draft": body.get("draft", False),
                "state": "open",
                "merged_at": None,
            }
            return httpx.Response(201, json=self._pull_payload(number))
        matched = re.fullmatch(rf"{repo}/pulls/(\d+)", path)
        if request.method == "PATCH" and matched and int(matched[1]) in self.pulls:
            pull = self.pulls[int(matched[1])]
            pull.update({key: body[key] for key in ("title", "body") if key in body})
            return httpx.Response(200, json=self._pull_payload(int(matched[1])))
        matched = re.fullmatch(rf"{repo}/pulls/(\d+)/comments/(\d+)/replies", path)
        if request.method == "POST" and matched:
            reply = self.add_review_comment(
                int(matched[1]), CONTRACT_BOT, body["body"], app=True, in_reply_to=int(matched[2])
            )
            return httpx.Response(201, json={"id": reply["id"]})
        matched = re.fullmatch(rf"{repo}/actions/runs/(\d+)/rerun-failed-jobs", path)
        if request.method == "POST" and matched:
            for runs in self.check_runs.values():
                for run in runs.values():
                    if run["_workflow_run"] == int(matched[1]) and run["conclusion"] in {
                        "failure",
                        "cancelled",
                    }:
                        run.update(status="queued", conclusion=None)
            return httpx.Response(201)
        return httpx.Response(404, json={"message": "missing fixture"})
