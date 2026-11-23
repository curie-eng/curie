"""GitHub REST and webhook fakes shared by the factory intake suites.

Payload shapes follow GitHub's webhook catalog:
https://docs.github.com/en/webhooks/webhook-events-and-payloads

Combined status responses follow GitHub's REST documentation:
https://docs.github.com/rest/commits/statuses#get-the-combined-status-for-a-specific-reference
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import itertools
import json
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote, urlsplit

import httpx
from curie_api.config import get_settings
from curie_api.factory_ci import CiView
from curie_api.forges.github.ci import CiDetail
from curie_api.forges.github.code_host import (
    base_rollup,
    conflicted,
    diagnostics_of,
    normalize_checks,
)
from curie_api.github_app import GitHubInstallationRefused
from fastapi.testclient import TestClient

REPO = "acme-corp/acme-bot"

REPO_ID = 4401
INSTALLATION_ID = 5501
SENDER_ID = 6601
SENDER = "octocat"
LABEL = "factory"
MENTION = "curie"

LABEL_ACTOR_PERMISSIONS = ["write", "maintain", "admin", "read", "triage", "none"]
LABEL_ACTOR_REFUSALS = [
    ({"id": SENDER_ID, "login": SENDER, "type": "User"}, {"id": 51}, "actor_own_app"),
    ({"id": 42, "login": "fixture[bot]", "type": "Bot"}, None, "actor_bot"),
    ({"id": 42, "login": "fixture[bot]", "type": "Bot"}, {"id": 52}, "actor_bot"),
    ({"id": 42, "login": "ghost", "type": "User"}, None, "actor_unverifiable"),
    (None, None, "actor_unverifiable"),
    ({"id": SENDER_ID, "login": SENDER, "type": "User"}, "invalid", "actor_unverifiable"),
    ({"id": SENDER_ID, "login": SENDER, "type": "User"}, [], "actor_unverifiable"),
    ({"id": SENDER_ID, "login": SENDER, "type": "User"}, {}, "actor_unverifiable"),
    ({"login": "fixture[bot]", "type": "Bot"}, None, "actor_unverifiable"),
    ({"id": 42, "login": "ghost", "type": "Bot"}, None, "actor_unverifiable"),
    ({"id": True, "login": SENDER, "type": "User"}, None, "actor_unverifiable"),
    ({"id": SENDER_ID, "login": "", "type": "User"}, None, "actor_unverifiable"),
]

LABEL_ACTOR_REFUSAL_IDS = [
    f"label_{reason}-{index}" for index, (_, _, reason) in enumerate(LABEL_ACTOR_REFUSALS)
]

BASE_SHA = "0" * 39 + "1"

# The factory's GitHub App. A placeholder, never a real key: tests that mint
# for real pass their own generated PEM.
APP_ID = "51"
# The fixture App's slug (`GitHubCredentials.app_slug`); what an installation
# token writes is authored by its bot user, ``<slug>[bot]`` (#4629).
APP_SLUG = "curie-fixture"
APP_BOT_USER = {"id": 1001, "login": f"{APP_SLUG}[bot]", "type": "Bot"}
PRIVATE_KEY = "example-private-key"
GITHUB_API = "https://api.github.com"

# The settings every factory suite boots with that are not factory settings.
# The factory itself is configured only through `factory_env` (#4397).
_BASE_ENV = {
    "GITHUB_REVIEW_INGRESS_ENABLED": "false",
    "GITHUB_APP_ID": APP_ID,
    "GITHUB_APP_PRIVATE_KEY": PRIVATE_KEY,
    "GITHUB_WEBHOOK_SECRET": "example-factory-hmac-secret",
    "GITHUB_REPO_ALLOWLIST": '["acme-corp/*"]',
    "GITHUB_TOKEN": "",
    "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
    "RESUME_RECONCILER_ENABLED": "false",
    "APPROVAL_SWEEP_INTERVAL_S": "0",
    "DEAD_LETTER_WATCH_INTERVAL_S": "0",
}


def github_host(api_url: str) -> str:
    """The identity host of a GitHub API URL, as `forges.hosts.github_host` derives it."""

    netloc = urlsplit(api_url.rstrip("/")).netloc.lower()
    return "github.com" if netloc == "api.github.com" else netloc


def github_app_credential(
    *, app_id: str = APP_ID, private_key: str = PRIVATE_KEY
) -> dict[str, Any]:
    return {"type": "github_app", "app_id": app_id, "private_key": private_key}


def forges_document(
    *,
    api_url: str = GITHUB_API,
    label: str = LABEL,
    mention: str = MENTION,
    app_id: str = APP_ID,
    private_key: str = PRIVATE_KEY,
    poll_interval_s: float = 45.0,
    card_base_url: str = "",
    bindings: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """The in-code `ForgesConfig` of today's factory: one GitHub code host and
    one GitHub tracker on the same App, both named ``github``.

    This is the shape a test passes as ``Settings(forges=...)``; secrets sit
    inline because a dict passed in code goes straight to `ForgesConfig`.
    """

    host = github_host(api_url)
    credential = github_app_credential(app_id=app_id, private_key=private_key)
    return {
        "code_hosts": [
            {
                "name": "github",
                "kind": "github",
                "host": host,
                "api_url": api_url,
                "credential": dict(credential),
            }
        ],
        "trackers": [
            {
                "name": "github",
                "kind": "github",
                "host": host,
                "api_url": api_url,
                "credential": dict(credential),
                "label": label,
                "mention": mention,
                "poll_interval_s": poll_interval_s,
            }
        ],
        "bindings": list(bindings or []),
        "card_base_url": card_base_url,
    }


def github_repository_binding(
    *,
    repo: str = REPO,
    repo_id: int | str = REPO_ID,
    bases: list[str] | None = None,
    default_base: str | None = None,
    ci: dict[str, Any] | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    """A `bindings` entry carrying one GitHub repository's bases and CI policy.

    Replaces the old per-repository settings maps: ``bases``/``default_base``
    for the bases map, ``ci`` (``required`` and ``metadata_rerun_keys``) for the
    Python and metadata CI maps.
    """

    project_id = str(repo_id)
    entry: dict[str, Any] = {
        "alias": "repo",
        "code_host": "github",
        "project_id": project_id,
        "path": repo,
        "bases": list(bases or []),
        "default_base": default_base,
    }
    if ci is not None:
        entry["ci"] = ci
    return {
        "name": name or f"repo-{project_id}",
        "tracker": "github",
        "tracker_scope_id": project_id,
        "repos": [entry],
    }


_SECRET_FIELDS = {
    "private_key": "private_key_file",
    "client_secret": "client_secret_file",
    "token": "token_file",
}


def forges_env_json(document: dict[str, Any], key_dir: Path) -> str:
    """The ``CURIE_FORGES`` form of ``document``: every inline credential secret
    is written to a file under ``key_dir`` and replaced by its ``*_file`` path,
    the only form the env loader accepts."""

    rendered = json.loads(json.dumps(document))
    for section in ("code_hosts", "trackers"):
        for entry in rendered.get(section, []):
            credential = entry["credential"]
            for inline, field in _SECRET_FIELDS.items():
                if inline in credential:
                    directory = key_dir / f"{section}-{entry['name']}"
                    directory.mkdir(parents=True, exist_ok=True)
                    path = directory / field.removesuffix("_file")
                    path.write_text(credential.pop(inline))
                    credential[field] = str(path)
    return json.dumps(rendered)


def factory_env(
    key_dir: Path,
    *,
    intake: str = "poll",
    api_url: str = GITHUB_API,
    **forges: Any,
) -> dict[str, str]:
    """Every env var a factory suite boots with (#4397): the non-factory GitHub
    settings, ``FACTORY_ENABLED``, ``FACTORY_INTAKE`` and a ``CURIE_FORGES``
    whose App key is a file under ``key_dir``. ``GITHUB_API_URL`` is set to
    ``api_url`` so the code host's host equals the install's GitHub host."""

    return {
        **_BASE_ENV,
        "GITHUB_API_URL": api_url,
        "FACTORY_ENABLED": "true",
        "FACTORY_INTAKE": intake,
        "FACTORY_BASE_LABEL_SETTLE_S": "0",
        "CURIE_FORGES": forges_env_json(forges_document(api_url=api_url, **forges), key_dir),
    }


def fake_installation_tokens(monkeypatch: Any, credentials: Any = None) -> None:
    """Answer every App token request with ``credentials`` (the fixture
    installation token by default), wherever the `GitHubCredentials` came from:
    the factory code host entry or the non-factory settings resolver."""

    from curie_api.github_app import GitHubCredentials

    fake = credentials if credentials is not None else _Credentials()
    monkeypatch.setattr(
        GitHubCredentials,
        "token_for_verified_installation",
        lambda _self, repo, installation_id: fake.token_for_verified_installation(
            repo, installation_id
        ),
    )
    monkeypatch.setattr(GitHubCredentials, "token_for", lambda _self, repo: fake.token_for(repo))

    def fresh_installation_token(
        _self: Any, repo: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        own = getattr(fake, "fresh_installation_token", None)
        if own is not None:
            return own(repo, expected_installation_id)
        installation = (
            INSTALLATION_ID if expected_installation_id is None else (expected_installation_id)
        )
        return installation, fake.token_for_verified_installation(repo, installation)

    monkeypatch.setattr(GitHubCredentials, "fresh_installation_token", fresh_installation_token)
    monkeypatch.setattr(GitHubCredentials, "app_slug", lambda _self: APP_SLUG)


# The repository-wide listings a poll pass reads, answered empty when
# ``GitHubAPI.answer_listings`` is set.
_POLL_LISTINGS = frozenset(
    {f"/repos/{REPO}/issues", f"/repos/{REPO}/issues/comments", f"/repos/{REPO}/pulls/comments"}
)


@dataclass
class PlantedIssue:
    """One issue's own answers (#5380), in place of the shared ``issue_state``
    and ``labels``.

    GET an issue (https://docs.github.com/en/rest/issues/issues#get-an-issue)
    answers ``number``, ``state`` and ``labels``; every pull request is also an
    issue, and its issue payload carries a ``pull_request`` key. An issue the
    installation cannot see answers 404 with GitHub's ``message``. ``events``,
    when set, is the issue's event list
    (https://docs.github.com/en/rest/issues/events#list-issue-events).
    """

    state: str = "open"
    labels: tuple[str, ...] = ()
    pull_request: bool = False
    missing: bool = False
    events: list[dict[str, Any]] | None = None


def issue_event(event: str, *, event_id: int, label: str | None = None) -> dict[str, Any]:
    """One human event on an issue, in the list-issue-events shape
    (https://docs.github.com/en/rest/issues/events#list-issue-events)."""

    found: dict[str, Any] = {
        "id": event_id,
        "event": event,
        "actor": {"id": SENDER_ID, "login": SENDER, "type": "User"},
        "created_at": "2026-09-01T00:00:00Z",
    }
    if label is not None:
        found["label"] = {"name": label}
    return found


def listed_publication(number: int, *, pull: int, state: str = "open") -> dict[str, Any]:
    """A factory publication of issue ``number`` as the repository's pull request
    listing returns it (https://docs.github.com/en/rest/pulls/pulls#list-pull-requests):
    opened by the App's bot user from a publication branch of the repository
    itself, closing the issue."""

    repo = {"id": REPO_ID, "full_name": REPO}
    return {
        "number": pull,
        "state": state,
        "html_url": f"https://github.com/{REPO}/pull/{pull}",
        "node_id": f"PR_acme_{pull}",
        "user": dict(APP_BOT_USER),
        "body": f"Closes #{number}",
        "merged_at": None,
        "closed_at": None if state == "open" else "2026-09-02T00:00:00Z",
        "head": {"sha": BASE_SHA, "ref": f"curie/publication-{pull:032x}", "repo": dict(repo)},
        "base": {"ref": "main", "repo": dict(repo)},
    }


class GitHubAPI:
    """In-process stand-in for the GitHub REST reads the intake verifier makes."""

    def __init__(self) -> None:
        self.issue_state = "open"
        self.labels = [LABEL]
        self.permission = "write"
        self.permission_requests: list[httpx.Request] = []
        self.permission_user_id = SENDER_ID
        self.label_actor: Any = {"id": SENDER_ID, "login": SENDER, "type": "User"}
        self.label_app: Any = None
        self.permission_status = 200
        self.role_name = "write"
        self.comment_body: str | None = None
        self.comment_app: dict[str, Any] | None = None
        self.repository_id = REPO_ID
        self.issue_number = 0
        # @spec apps/api/README.md#factory-test-isolation
        # Timeline events retain identity inside this stand-in while independent
        # fixtures cannot address each other's request-derived Valkey keys.
        self.label_event_base = uuid.uuid4().int >> 80
        self.label_event_ids: dict[int, int] = {}
        # Issue number -> its comments, for the marked label refusal comment.
        self.issue_comments: dict[int, list[dict[str, Any]]] = {}
        self._comment_ids = itertools.count(9000)
        # Additive answers (#5380); each default leaves the answers above as
        # they were. Issues with their own answers, by number.
        self.planted_issues: dict[int, PlantedIssue] = {}
        # Branches GET a reference answers 404 for
        # (https://docs.github.com/en/rest/git/refs#get-a-reference).
        self.missing_branches: set[str] = set()
        # What the repository's pull request listing returns.
        self.listed_pulls: list[dict[str, Any]] = []
        # Answer the repository-wide listings a poll pass reads (open issues
        # by label, issue comments and review comments since a time) with one
        # empty page:
        # https://docs.github.com/en/rest/issues/issues#list-repository-issues,
        # https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository,
        # https://docs.github.com/en/rest/pulls/comments#list-review-comments-in-a-repository.
        self.answer_listings = False

    def advance_label_event(self, number: int) -> None:
        """Record a newer labeled timeline event. A relabel webhook reads it."""

        current = self.label_event_ids.get(number, self.label_event_base + number)
        self.label_event_ids[number] = current + 1

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/repos/{REPO}" or (
            request.method == "GET" and path == f"/repositories/{self.repository_id}"
        ):
            return httpx.Response(
                200,
                json={"id": self.repository_id, "full_name": REPO, "default_branch": "main"},
            )
        if self.answer_listings and request.method == "GET" and path in _POLL_LISTINGS:
            return httpx.Response(200, json=[])
        if path == f"/repos/{REPO}/pulls" and request.method == "GET":
            # The repository-wide listing rediscovery reads (#4629): no factory
            # publication exists in this stand-in unless one is listed.
            return httpx.Response(200, json=list(self.listed_pulls))
        if path.startswith(f"/repos/{REPO}/git/ref/heads/"):
            # Admission resolves the base and reads its commit through the
            # code host (#3095).
            # https://docs.github.com/en/rest/git/refs#get-a-reference
            name = path.removeprefix(f"/repos/{REPO}/git/ref/heads/")
            if name in self.missing_branches:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(
                200, json={"ref": f"refs/heads/{name}", "object": {"sha": BASE_SHA}}
            )
        if path.startswith(f"/repos/{REPO}/git/matching-refs/heads/"):
            # The code host reads this listing after a 404 on the reference;
            # GitHub answers 200 with an empty array when no branch matches
            # (https://docs.github.com/en/rest/git/refs#list-matching-references).
            name = path.removeprefix(f"/repos/{REPO}/git/matching-refs/heads/")
            if name in self.missing_branches:
                return httpx.Response(200, json=[])
            return httpx.Response(
                200,
                json=[{"ref": f"refs/heads/{name}", "object": {"sha": BASE_SHA, "type": "commit"}}],
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
        if path.startswith(f"/repos/{REPO}/issues/") and path.endswith("/comments"):
            # An issue's comments: the factory's marked refusal comment (#4361
            # decision 9) is listed, then posted as the App.
            number = int(path.split("/")[-2])
            listed = self.issue_comments.setdefault(number, [])
            if request.method == "POST":
                comment = {
                    "id": next(self._comment_ids),
                    "body": json.loads(request.content)["body"],
                    "user": {"id": 1, "login": "curie[bot]", "type": "Bot"},
                    "performed_via_github_app": {"id": int(APP_ID)},
                }
                listed.append(comment)
                return httpx.Response(201, json=comment)
            return httpx.Response(200, json=listed)
        if path.startswith(f"/repos/{REPO}/issues/") and path.endswith("/events"):
            number = int(path.split("/")[-2])
            planted = self.planted_issues.get(number)
            if planted is not None and planted.events is not None:
                return httpx.Response(200, json=list(planted.events))
            event_id = self.label_event_ids.get(number, self.label_event_base + number)
            return httpx.Response(
                200,
                json=[
                    {
                        "id": event_id,
                        "event": "labeled",
                        "label": {"name": LABEL},
                        "actor": self.label_actor,
                        "performed_via_github_app": self.label_app,
                        "created_at": "2026-09-01T00:00:00Z",
                    }
                ],
            )
        if path.startswith(f"/repos/{REPO}/issues/"):
            number = int(path.rsplit("/", 1)[1])
            planted = self.planted_issues.get(number)
            if planted is not None:
                return self._planted_issue(number, planted)
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
                self.permission_status,
                json={
                    "permission": self.permission,
                    "role_name": self.role_name,
                    "user": {"id": self.permission_user_id, "login": SENDER},
                },
            )
        return httpx.Response(404, json={"message": "missing fixture"})

    def _planted_issue(self, number: int, planted: PlantedIssue) -> httpx.Response:
        # https://docs.github.com/en/rest/issues/issues#get-an-issue
        if planted.missing:
            return httpx.Response(404, json={"message": "Not Found"})
        issue: dict[str, Any] = {
            "number": number,
            "state": planted.state,
            "labels": [{"name": name} for name in planted.labels],
        }
        if planted.pull_request:
            issue["pull_request"] = {"url": f"https://api.github.com/repos/{REPO}/pulls/{number}"}
        return httpx.Response(200, json=issue)


def github_time(moment: datetime) -> str:
    """A GitHub REST timestamp: ISO 8601 in UTC to the whole second, ``Z``
    suffixed, as every comment's ``created_at`` and ``updated_at`` are
    (https://docs.github.com/en/rest/issues/comments#get-an-issue-comment)."""

    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _at(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _since(value: str | None) -> datetime | None:
    return _at(value) if value else None


def _page_of(request: httpx.Request, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    params = request.url.params
    per_page = int(params.get("per_page", "30"))
    page = int(params.get("page", "1"))
    return items[(page - 1) * per_page : page * per_page]


class IssueCommentsAPI(GitHubAPI):
    """`GitHubAPI` that keeps issue comments the way GitHub does (#5787).

    Every default of `GitHubAPI` stays as it is; this subclass adds a comment
    store the factory's question write, the answer intake and a poll pass all
    read and write:

    1. One id sequence for every comment, increasing, so a later comment has a
       larger numeric ``id``, and ``created_at`` / ``updated_at`` in GitHub's
       whole-second ``Z`` form from a clock the test moves (``tick``).
    2. Create, list, read by id and edit, with each comment's ``user`` (whose
       ``type`` is ``User`` or ``Bot``), ``performed_via_github_app`` (the App a
       person or bot acted through, or null) and ``issue_url``:
       https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
       https://docs.github.com/en/rest/issues/comments#list-issue-comments
       https://docs.github.com/en/rest/issues/comments#get-an-issue-comment
       https://docs.github.com/en/rest/issues/comments#update-an-issue-comment
       A comment the installation token writes is authored by the App's bot
       user and carries ``performed_via_github_app`` with the App's id.
    3. The repository-wide listing a poll pass reads, filtered by ``since`` on
       ``updated_at`` (an edit only moves ``updated_at``) and sorted by
       creation, oldest first:
       https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository
    4. Permission answers per login, with the user's immutable id. GitHub's
       legacy ``permission`` field folds ``maintain`` into ``write`` and
       ``triage`` into ``read``; ``role_name`` keeps the role:
       https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
    5. ``store_then_fail``: the next comment create is stored, then answered
       with that status, as when GitHub's response is lost after the write.
    6. ``after_listing``: a step `handle_async` runs once, after the next
       repository-wide listing is answered and before the caller reads it, so
       a test can make something happen in the middle of a poll pass.

    Issue state and labels per issue come from `GitHubAPI.planted_issues`.
    The repository issue and review comment listings answer empty
    (``answer_listings``).
    """

    def __init__(self, *, api: str = GITHUB_API) -> None:
        super().__init__()
        self.api = api.rstrip("/")
        self.clock = datetime.now(UTC).replace(microsecond=0)
        self.stored: dict[int, dict[str, Any]] = {}
        self.collaborators: dict[str, tuple[int, str]] = {}
        self.store_then_fail: int | None = None
        self.after_listing: Callable[[], Awaitable[None]] | None = None
        self.answer_listings = True

    def tick(self, seconds: int = 1) -> None:
        self.clock += timedelta(seconds=seconds)

    def grant(self, user: dict[str, Any], role: str) -> None:
        """``user``'s role on the repository: admin, maintain, write, triage, read or none."""

        self.collaborators[user["login"]] = (user["id"], role)

    def add_comment(
        self,
        number: int,
        body: str,
        *,
        user: dict[str, Any],
        app: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """A comment created on issue ``number`` now, by ``user`` acting
        through ``app`` (None when not through an App)."""

        comment_id = next(self._comment_ids)
        stamp = github_time(self.clock)
        comment = {
            "id": comment_id,
            "body": body,
            "user": dict(user),
            "performed_via_github_app": None if app is None else dict(app),
            "issue_url": f"{self.api}/repos/{REPO}/issues/{number}",
            "html_url": f"https://github.com/{REPO}/issues/{number}#issuecomment-{comment_id}",
            "author_association": "NONE",
            "created_at": stamp,
            "updated_at": stamp,
        }
        self.stored[comment_id] = comment
        return comment

    def edit_comment(self, comment_id: int, body: str) -> dict[str, Any]:
        comment = self.stored[comment_id]
        comment["body"] = body
        comment["updated_at"] = github_time(self.clock)
        return comment

    def on_issue(self, number: int) -> list[dict[str, Any]]:
        url = f"{self.api}/repos/{REPO}/issues/{number}"
        return [
            self.stored[key] for key in sorted(self.stored) if self.stored[key]["issue_url"] == url
        ]

    async def handle_async(self, request: httpx.Request) -> httpx.Response:
        """`handle` for an async client, running ``after_listing`` once."""

        response = self.handle(request)
        step = self.after_listing
        if (
            step is not None
            and request.method == "GET"
            and request.url.path == f"/repos/{REPO}/issues/comments"
        ):
            self.after_listing = None
            await step()
        return response

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        root = re.escape(f"/repos/{REPO}")
        if path == f"/repos/{REPO}/issues/comments" and request.method == "GET":
            since = _since(request.url.params.get("since"))
            listed = sorted(
                (
                    comment
                    for comment in self.stored.values()
                    if since is None or _at(comment["updated_at"]) >= since
                ),
                key=lambda comment: (comment["created_at"], comment["id"]),
            )
            return httpx.Response(200, json=_page_of(request, listed))
        single = re.fullmatch(rf"{root}/issues/comments/(\d+)", path)
        if single is not None:
            found = self.stored.get(int(single.group(1)))
            if found is None:
                return httpx.Response(404, json={"message": "Not Found"})
            if request.method == "PATCH":
                found = self.edit_comment(found["id"], json.loads(request.content)["body"])
            return httpx.Response(200, json=found)
        thread = re.fullmatch(rf"{root}/issues/(\d+)/comments", path)
        if thread is not None:
            number = int(thread.group(1))
            if request.method == "POST":
                created = self.add_comment(
                    number,
                    json.loads(request.content)["body"],
                    user=APP_BOT_USER,
                    app={"id": int(APP_ID), "slug": APP_SLUG},
                )
                if self.store_then_fail is not None:
                    status, self.store_then_fail = self.store_then_fail, None
                    return httpx.Response(status, json={"message": "Server Error"})
                return httpx.Response(201, json=created)
            return httpx.Response(200, json=_page_of(request, self.on_issue(number)))
        permission = re.fullmatch(rf"{root}/collaborators/([^/]+)/permission", path)
        if permission is not None and unquote(permission.group(1)) in self.collaborators:
            login = unquote(permission.group(1))
            user_id, role = self.collaborators[login]
            legacy = {"maintain": "write", "triage": "read"}.get(role, role)
            self.permission_requests.append(request)
            return httpx.Response(
                200,
                json={
                    "permission": legacy,
                    "role_name": role,
                    "user": {"id": user_id, "login": login, "type": "User"},
                },
            )
        return super().handle(request)


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


@dataclass(frozen=True)
class WebhookDelivery:
    """One webhook delivery as GitHub sends it: the event name, the delivery
    headers and the exact body bytes ``X-Hub-Signature-256`` covers.

    https://docs.github.com/en/webhooks/webhook-events-and-payloads#delivery-headers
    https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
    """

    event: str
    headers: dict[str, str]
    body: bytes

    @property
    def payload(self) -> dict[str, Any]:
        decoded = json.loads(self.body)
        assert isinstance(decoded, dict)
        return decoded

    @property
    def delivery_id(self) -> str:
        return self.headers["X-GitHub-Delivery"]


def signed_delivery(
    event: str, payload: dict[str, Any], *, delivery_id: str | None = None
) -> WebhookDelivery:
    """``payload`` as a delivery of ``event``, signed with the configured
    webhook secret at this moment (a later secret change does not re-sign)."""

    body = json.dumps(payload).encode()
    headers = {
        "X-GitHub-Delivery": delivery_id or str(uuid.uuid4()),
        "X-GitHub-Event": event,
        "X-Hub-Signature-256": _signature(body),
        "Content-Type": "application/json",
    }
    return WebhookDelivery(event, headers, body)


def post_delivery(client: TestClient, delivery: WebhookDelivery) -> httpx.Response:
    """POST ``delivery`` to the webhook route unchanged: its own bytes and headers."""

    return client.post("/github/webhook", content=delivery.body, headers=dict(delivery.headers))


@dataclass(frozen=True)
class Posted:
    """What a person's action on `GitHubRepositoryFake` created, as the actor
    reads it back, and the webhook deliveries GitHub sends for it (none for a
    pending review)."""

    item: dict[str, Any]
    deliveries: tuple[WebhookDelivery, ...] = ()


def presented_token(request: httpx.Request) -> str | None:
    """The token an ``Authorization`` header carries: ``Bearer <token>``,
    ``token <token>`` or Basic ``x-access-token:<token>``."""

    scheme, _, credential = request.headers.get("Authorization", "").partition(" ")
    if scheme.lower() in {"bearer", "token"}:
        return credential.strip() or None
    if scheme.lower() == "basic":
        try:
            decoded = base64.b64decode(credential.strip()).decode()
        except (ValueError, UnicodeDecodeError):
            return None
        return decoded.partition(":")[2] or None
    return None


# REST review ``event`` -> the state a submitted review takes.
# https://docs.github.com/en/rest/pulls/reviews#submit-a-review-for-a-pull-request
REVIEW_EVENT_STATES = {
    "COMMENT": "COMMENTED",
    "REQUEST_CHANGES": "CHANGES_REQUESTED",
    "APPROVE": "APPROVED",
}


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


def ci_view(detail: CiDetail, pull: dict[str, Any] | None = None) -> CiView:
    """A GitHub checks and statuses read as the CI gate sees it through the code
    host (`GitHubCodeHost.ci_diagnostics`), so gate tests can state GitHub data.

    ``pull`` is the pull request payload the gate also reads, for mergeability.
    """

    if detail.state != "observed" or detail.reason is not None:
        return CiView(detail.head_sha, reason=detail.reason or "github_error")
    head = detail.head_sha or ""
    base = base_rollup(detail)
    return CiView(
        head,
        normalize_checks(detail, head),
        diagnostics_of(detail),
        base_failing=base.failing_keys if base is not None else frozenset(),
        conflicted=conflicted(pull) if pull is not None else None,
        merged=pull.get("merged") is True if pull is not None else None,
        closed_unmerged=(
            pull.get("state") == "closed" and pull.get("merged") is not True
            if pull is not None
            else None
        ),
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
# https://docs.github.com/en/rest/git/blobs#create-a-blob
# https://docs.github.com/en/rest/git/trees#create-a-tree
# https://docs.github.com/en/rest/git/commits#create-a-commit
# https://docs.github.com/en/rest/git/refs#create-a-reference
# https://docs.github.com/en/rest/issues/issues#list-repository-issues
# https://docs.github.com/en/rest/issues/issues#get-an-issue
#
# Issues follow the REST issue object: ``number``, ``title``, ``body``,
# ``state``, ``labels`` as objects with a ``name``, ``user`` with ``id``,
# ``login`` and ``type``, and ``html_url``. A pull request is an issue too: the
# repository issue listing returns it with a ``pull_request`` key, and its
# ``creator`` filter matches the opening account's login.
#
# Pull request reviews (#5794), as GitHub's REST and webhook documentation
# describes them:
# https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request
# https://docs.github.com/en/rest/pulls/reviews#submit-a-review-for-a-pull-request
# https://docs.github.com/en/rest/pulls/reviews#get-a-review-for-a-pull-request
# https://docs.github.com/en/rest/pulls/reviews#list-reviews-for-a-pull-request
# https://docs.github.com/en/rest/pulls/reviews#list-comments-for-a-pull-request-review
# https://docs.github.com/en/rest/pulls/reviews#dismiss-a-review-for-a-pull-request
# https://docs.github.com/en/rest/pulls/comments#list-review-comments-on-a-pull-request
# https://docs.github.com/en/rest/pulls/comments#list-review-comments-in-a-repository
# https://docs.github.com/en/rest/pulls/comments#get-a-review-comment-for-a-pull-request
# https://docs.github.com/en/rest/pulls/comments#update-a-review-comment-for-a-pull-request
# https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
# https://docs.github.com/en/rest/issues/comments#get-an-issue-comment
# https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
# https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request_review
# https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request_review_comment
# https://docs.github.com/en/webhooks/webhook-events-and-payloads#issue_comment
# https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries
#
#   1. Callers are told apart by the token they present (``Bearer``, ``token``
#      or Basic ``x-access-token:<token>``): a token from ``user_token`` is
#      that person; any other token is the App, acting as ``bot_user``.
#   2. Create a review with ``body``, an optional ``event`` (``COMMENT``,
#      ``REQUEST_CHANGES``, ``APPROVE``), an optional ``commit_id`` (the
#      pull's head when omitted) and ``comments`` of ``path``, ``line`` and
#      ``body``; 200 with the review. "To create a pending review for a pull
#      request, leave the event parameter blank"; a pending review's state is
#      ``PENDING`` and it does "not include the submitted_at property". A
#      comment without ``line`` is a file-level comment (``line`` null,
#      ``subject_type: "file"``). The docs make ``body`` mandatory for
#      ``COMMENT`` and ``REQUEST_CHANGES``; the HTTP handler waives that only
#      when the review carries inline comments, the body-less batch of inline
#      comments the factory has to handle (the live run, #5808, confirms it).
#      One pending review per person per pull request; a second is 422.
#   3. Submit with ``event`` (required; 422 without it or on a review that is
#      not pending): the state becomes ``COMMENTED``, ``CHANGES_REQUESTED`` or
#      ``APPROVED``, ``submitted_at`` is the fake clock's now, and each inline
#      comment's ``updated_at`` moves to it while ``created_at`` stays (the
#      bump #2794 observed live). A create call with ``event`` submits at once.
#   4. "Before you submit your review, your line comments are pending and only
#      visible to you" (https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/reviewing-proposed-changes-in-a-pull-request):
#      for anyone but the author a pending review and its comments answer 404
#      when read directly and are left out of every listing.
#   5. Review objects carry ``id``, ``node_id``, ``user``, ``body``, ``state``
#      (uppercase in REST), ``html_url`` (``#pullrequestreview-<id>``),
#      ``pull_request_url``, ``author_association``, ``_links``,
#      ``commit_id`` and, once submitted, ``submitted_at``. Review comments
#      carry the documented ``pull_request_review_id``, ``commit_id``,
#      ``original_commit_id``, ``path``, ``line``, ``original_line``, ``side``,
#      ``subject_type``, ``position`` (null: the fake keeps no diff),
#      ``user``, ``body``, ``created_at``, ``updated_at``, ``html_url``
#      (``#discussion_r<id>``), ``pull_request_url`` and
#      ``author_association``; no ``performed_via_github_app``.
#   6. The repository-wide comment listings honour ``since`` on
#      ``updated_at`` ("Only show results that were last updated after the
#      given time"); an item seeded by the older helpers, which carry no
#      ``updated_at``, is listed whatever ``since`` says, as before. Review
#      comments sort by ``created_at`` (``sort=updated`` by ``updated_at``),
#      ascending unless ``direction=desc``.
#   7. Each submission yields the signed deliveries GitHub sends: one
#      ``pull_request_review`` ``submitted`` and one
#      ``pull_request_review_comment`` ``created`` per inline comment. Webhook
#      ``review.state`` is lowercase (``commented``, ``changes_requested``,
#      ``approved``, ``dismissed``: the webhook payload schema the docs
#      render, also published as octokit/webhooks
#      ``payload-schemas/api.github.com/common/pull-request-review.schema.json``);
#      ``pull_request`` is the fake's own pull read. The signature is
#      ``X-Hub-Signature-256`` over the exact body bytes.
#
# The Git Data API shapes below (blobs, trees, parentless commits, and refs
# outside refs/heads) replay what was observed against api.github.com with a
# GitHub App installation token, 2026-11-03:
#   POST git/blobs {content, encoding: "base64"} -> 201 {sha, url}
#   POST git/trees {tree: [{path, mode: "100644", type: "blob", sha}]}
#       -> 201 {sha, url, tree: [...]}
#   POST git/commits {message, tree, parents: [], author, committer}
#       -> 201 {sha, tree: {sha}, parents: []}
#   POST git/refs {ref, sha} -> 201 {ref, object: {sha, type: "commit"}},
#       422 {"message": "Reference already exists"} when present
#   PATCH git/refs/<name> {sha, force} -> 200
#   DELETE git/refs/<name> -> 204, 422 {"message": "Reference does not exist"}
#       when absent
#   GET https://github.com/<repo>/raw/<commit sha>/<path> -> 200 image/png
# Object ids are content addressed exactly as git computes them, so replaying
# the same blobs, tree and dated commit yields the same SHAs.

CONTRACT_APP_ID = 51
CONTRACT_BOT = {"id": 1000, "login": "curie-factory", "type": "Bot"}
_LISTINGS = (
    re.compile(rf"^/repos/{REPO}/issues$"),
    re.compile(rf"^/repos/{REPO}/issues/events$"),
    re.compile(rf"^/repos/{REPO}/issues/comments$"),
    re.compile(rf"^/repos/{REPO}/issues/\d+/events$"),
    re.compile(rf"^/repos/{REPO}/issues/\d+/comments$"),
    re.compile(rf"^/repos/{REPO}/pulls/\d+/comments$"),
    re.compile(rf"^/repos/{REPO}/pulls/\d+/reviews$"),
    re.compile(rf"^/repos/{REPO}/pulls/\d+/reviews/\d+/comments$"),
    re.compile(rf"^/repos/{REPO}/pulls/comments$"),
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


def git_object_sha(kind: str, payload: bytes) -> str:
    """The git object id of ``payload`` stored as a ``kind`` object."""

    return hashlib.sha1(f"{kind} {len(payload)}\0".encode() + payload).hexdigest()


def git_blob_sha(content: bytes) -> str:
    """The git blob id of ``content``: sha1 of ``"blob <len>\\0"`` and the bytes."""

    return git_object_sha("blob", content)


def _git_tree_payload(entries: list[dict[str, str]]) -> bytes:
    # Git sorts tree entries by name, a directory as if its name ended in "/".
    def key(entry: dict[str, str]) -> bytes:
        name = entry["name"].encode()
        return name + b"/" if entry["type"] == "tree" else name

    payload = b""
    for entry in sorted(entries, key=key):
        mode = "40000" if entry["type"] == "tree" else entry["mode"]
        payload += f"{mode} {entry['name']}".encode() + b"\0" + bytes.fromhex(entry["sha"])
    return payload


def _git_signature(person: dict[str, Any]) -> str:
    stamp = datetime.fromisoformat(str(person["date"]).replace("Z", "+00:00"))
    offset = stamp.utcoffset() or timedelta(0)
    minutes = int(offset.total_seconds() // 60)
    sign = "-" if minutes < 0 else "+"
    zone = f"{sign}{abs(minutes) // 60:02d}{abs(minutes) % 60:02d}"
    return f"{person['name']} <{person['email']}> {int(stamp.timestamp())} {zone}"


def git_commit_sha(
    *,
    tree: str,
    parents: list[str],
    message: str,
    author: dict[str, Any],
    committer: dict[str, Any],
) -> str:
    """The git commit id of a commit object with these fields."""

    lines = [f"tree {tree}"]
    lines += [f"parent {parent}" for parent in parents]
    lines += [f"author {_git_signature(author)}", f"committer {_git_signature(committer)}"]
    return git_object_sha("commit", ("\n".join(lines) + "\n\n" + message).encode())


_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _stamp(second: int) -> str:
    return (
        (datetime(2026, 9, 1, tzinfo=UTC) + timedelta(seconds=second))
        .isoformat()
        .replace("+00:00", "Z")
    )


def _person() -> dict[str, Any]:
    return {"id": SENDER_ID, "login": SENDER, "type": "User"}


class GitHubRepositoryFake:
    """In-process GitHub REST state for one repository's issues and code."""

    def __init__(self) -> None:
        self.issues: dict[int, dict[str, Any]] = {}
        self.events: list[dict[str, Any]] = []
        self.comments: list[dict[str, Any]] = []
        self.review_comments: list[dict[str, Any]] = []
        self.reviews: list[dict[str, Any]] = []
        self.permissions: dict[str, tuple[int, str]] = {}
        # Requested account id -> the `GET /user/{account_id}` payload (#5414).
        # https://docs.github.com/en/rest/users/users#get-a-user-using-their-id
        self.accounts: dict[str, dict[str, Any]] = {}
        # The immutable id GitHub reports for this repository on both pull
        # request sides (``head.repo.id``, ``base.repo.id``).
        # https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request
        self.repository_id = REPO_ID
        self.visibility = "public"
        self.branches: dict[str, str] = {}
        self.commits: dict[str, dict[str, Any]] = {}
        self.pulls: dict[int, dict[str, Any]] = {}
        self.check_runs: dict[str, dict[str, dict[str, Any]]] = {}
        self.statuses: dict[str, dict[str, dict[str, Any]]] = {}
        self.default_branch = "main"
        self.writes = 0
        # Git Data API state: blob id -> bytes, tree id -> its entries
        # ({name, mode, type, sha}), and every ref outside refs/heads (the UI
        # evidence refs, ``refs/curie/ui-evidence/<key>``) -> commit id.
        self.blobs: dict[str, bytes] = {}
        self.trees: dict[str, list[dict[str, str]]] = {}
        self.evidence_refs: dict[str, str] = {}
        self.raw_requests: list[httpx.Request] = []
        self._write_faults: list[list[Any]] = []
        # Every request the fake answered, in order.
        self.http_requests: list[httpx.Request] = []
        # Who an issue or pull request the adapter's token opens is authored by.
        self.bot_user: dict[str, Any] = dict(CONTRACT_BOT)
        # Reads that answer an injected status: [pattern, status, count].
        self._read_faults: list[list[Any]] = []
        self.label_requests: list[httpx.Request] = []
        self._event_ids = itertools.count(70001)
        self._comment_ids = itertools.count(80001)
        self._check_ids = itertools.count(90001)
        self._workflow_runs = itertools.count(60001)
        self._seconds = itertools.count(1)
        self._shas = itertools.count(1)
        self._fail_countdown: int | None = None
        # The review API (#5794, header block above). URLs GitHub derives
        # from its API and web origins; the installation the App's webhook
        # deliveries name.
        self.api_url = GITHUB_API
        self.html_base = "https://github.com"
        self.installation_id = INSTALLATION_ID
        # The clock review, review comment and conversation comment times come
        # from, whole seconds in UTC, near the real now so a poll pass's
        # ``since`` (now minus its lookback) lists them; ``tick`` moves it.
        self.clock = datetime.now(UTC).replace(microsecond=0)
        # A person's token -> that person (``user_token``); every other token
        # is the App.
        self.user_tokens: dict[str, dict[str, Any]] = {}
        # Login -> ``author_association``; unset, a collaborator with a
        # permission entry is ``COLLABORATOR`` and anyone else ``NONE``.
        self.associations: dict[str, str] = {}
        # Every webhook delivery GitHub would have sent, oldest first, until
        # ``drain_deliveries`` takes them.
        self.deliveries: list[WebhookDelivery] = []
        self.seed_repository()

    # Seeding, from outside the adapters -----------------------------------

    def seed_repository(self) -> None:
        """The repository with one commit on its default branch."""

        self.push_head(self.default_branch)

    def _number(self) -> int:
        return len(self.issues) + 1

    def seed_issue(self, title: str, body: str, *, user: dict[str, Any] | None = None) -> int:
        """An issue opened from outside the adapters, by ``user`` (a person by default)."""

        number = self._number()
        self.issues[number] = {
            "number": number,
            "url": f"{self.api_url}/repos/{REPO}/issues/{number}",
            "title": title,
            "body": body,
            "state": "open",
            "labels": [],
            "user": dict(user) if user is not None else _person(),
            "html_url": f"https://github.com/{REPO}/issues/{number}",
            "performed_via_github_app": None,
        }
        return number

    def _pull_issue(self, number: int) -> dict[str, Any]:
        """The issue side of pull request ``number``, as the issue listing shows it."""

        return {
            "number": number,
            "url": f"{self.api_url}/repos/{REPO}/issues/{number}",
            "state": "open",
            "pull_request": {"url": f"https://api.github.com/repos/{REPO}/pulls/{number}"},
            "labels": [],
            "user": dict(self.bot_user),
            "html_url": f"https://github.com/{REPO}/pull/{number}",
        }

    def label_event(
        self,
        number: int,
        kind: str,
        label: str,
        actor: dict[str, Any],
        *,
        performed_via_github_app: dict[str, Any] | None = None,
    ) -> None:
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
                "performed_via_github_app": performed_via_github_app,
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

    # People and the review API (#5794) ----------------------------------

    def tick(self, seconds: int = 1) -> None:
        """Move the fake clock forward."""

        self.clock += timedelta(seconds=seconds)

    def _now(self) -> str:
        return github_time(self.clock)

    def user_token(self, user: dict[str, Any], *, association: str | None = None) -> str:
        """A token that identifies ``user`` (``{"id", "login", "type"}``) to the
        fake, as a person's own token does to GitHub. ``association`` is the
        ``author_association`` their reviews and comments carry."""

        token = f"user-token-{user['id']}"
        self.user_tokens[token] = dict(user)
        if association is not None:
            self.associations[user["login"]] = association
        return token

    def grant(self, user: dict[str, Any], role: str) -> None:
        """``user``'s role on the repository, as the permission read answers it.

        GitHub's legacy ``permission`` folds ``maintain`` into ``write`` and
        ``triage`` into ``read``:
        https://docs.github.com/en/rest/collaborators/collaborators#get-repository-permissions-for-a-user
        """

        legacy = {"maintain": "write", "triage": "read"}.get(role, role)
        self.permissions[user["login"]] = (int(user["id"]), legacy)

    def client_for(self, token: str) -> httpx.Client:
        """A client on this fake that presents ``token``, as GitHub's own
        clients do; the caller closes it."""

        return httpx.Client(
            transport=httpx.MockTransport(self.handle),
            base_url=self.api_url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )

    def drain_deliveries(self) -> list[WebhookDelivery]:
        """Every delivery not yet drained, oldest first."""

        drained, self.deliveries = self.deliveries, []
        return drained

    def _caller(self, request: httpx.Request) -> dict[str, Any]:
        return self.user_tokens.get(presented_token(request) or "") or self.bot_user

    @staticmethod
    def _sees(viewer: dict[str, Any], item: dict[str, Any]) -> bool:
        """A pending review and its comments are their author's alone."""

        author = item.get("_pending_author")
        return author is None or author == viewer["id"]

    def _association(self, user: dict[str, Any]) -> str:
        login = user["login"]
        if login in self.associations:
            return self.associations[login]
        return "COLLABORATOR" if login in self.permissions else "NONE"

    def _pull_url(self, number: int) -> str:
        return f"{self.api_url}/repos/{REPO}/pulls/{number}"

    def _review_of(self, number: int, review_id: int) -> dict[str, Any]:
        for review in self.reviews:
            if review["_number"] == number and review["id"] == review_id:
                return review
        raise KeyError(review_id)

    def comments_of_review(self, review_id: int) -> list[dict[str, Any]]:
        """The stored inline comments of review ``review_id``, oldest first."""

        return [c for c in self.review_comments if c.get("pull_request_review_id") == review_id]

    def create_review(
        self,
        number: int,
        user: dict[str, Any],
        *,
        body: str = "",
        event: str | None = None,
        comments: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
        commit_id: str | None = None,
    ) -> Posted:
        """A review by ``user`` on pull request ``number``, as the create-review
        call makes it: pending without ``event``, submitted at once with one.

        ``comments`` are ``{"path", "line", "body"}``; a comment without
        ``line`` is file-level. Unlike the HTTP handler this does not validate
        its input, so a test can also make what the API refuses (a body-less
        ``COMMENT`` review with no inline comments).
        https://docs.github.com/en/rest/pulls/reviews#create-a-review-for-a-pull-request
        """

        commit = commit_id or self._pull_payload(number)["head"]["sha"]
        review_id = next(self._comment_ids)
        stamp = self._now()
        url = f"{self.html_base}/{REPO}/pull/{number}#pullrequestreview-{review_id}"
        pull_url = self._pull_url(number)
        self.reviews.append(
            {
                "id": review_id,
                "node_id": f"PRR_fixture_{review_id}",
                "user": dict(user),
                "body": body,
                "state": "PENDING",
                "html_url": url,
                "pull_request_url": pull_url,
                "author_association": self._association(user),
                "_links": {"html": {"href": url}, "pull_request": {"href": pull_url}},
                "commit_id": commit,
                "_number": number,
                "_pending_author": user["id"],
            }
        )
        for spec in comments:
            self._add_review_comment(number, review_id, user, spec, commit, stamp)
        if event is None:
            return Posted(self._visible(self._review_of(number, review_id)))
        return self.submit_review(number, review_id, event)

    def _add_review_comment(
        self,
        number: int,
        review_id: int,
        user: dict[str, Any],
        spec: dict[str, Any],
        commit: str,
        stamp: str,
    ) -> None:
        # https://docs.github.com/en/rest/pulls/reviews#list-comments-for-a-pull-request-review
        comment_id = next(self._comment_ids)
        line = spec.get("line")
        url = f"{self.api_url}/repos/{REPO}/pulls/comments/{comment_id}"
        html = f"{self.html_base}/{REPO}/pull/{number}#discussion_r{comment_id}"
        pull_url = self._pull_url(number)
        self.review_comments.append(
            {
                "id": comment_id,
                "node_id": f"PRRC_fixture_{comment_id}",
                "url": url,
                "pull_request_review_id": review_id,
                "diff_hunk": "",
                "path": spec["path"],
                "position": None,
                "original_position": None,
                "commit_id": commit,
                "original_commit_id": commit,
                "user": dict(user),
                "body": spec["body"],
                "created_at": stamp,
                "updated_at": stamp,
                "html_url": html,
                "pull_request_url": pull_url,
                "author_association": self._association(user),
                "_links": {
                    "self": {"href": url},
                    "html": {"href": html},
                    "pull_request": {"href": pull_url},
                },
                "start_line": None,
                "original_start_line": None,
                "start_side": None,
                "line": line,
                "original_line": line,
                "side": "RIGHT",
                "subject_type": "file" if line is None else "line",
                "_number": number,
                "_pending_author": user["id"],
            }
        )

    def submit_review(
        self, number: int, review_id: int, event: str, *, body: str | None = None
    ) -> Posted:
        """Submit pending review ``review_id`` with ``event`` and return the
        review with the deliveries GitHub sends for it (also appended to
        ``deliveries``).

        https://docs.github.com/en/rest/pulls/reviews#submit-a-review-for-a-pull-request
        """

        review = self._review_of(number, review_id)
        if review["state"] != "PENDING":
            raise ValueError(f"review {review_id} is not pending")
        stamp = self._now()
        if body is not None:
            review["body"] = body
        review["state"] = REVIEW_EVENT_STATES[event]
        review["submitted_at"] = stamp
        del review["_pending_author"]
        comments = self.comments_of_review(review_id)
        for comment in comments:
            # Submission moves updated_at, never created_at (#2794).
            comment["updated_at"] = stamp
            comment.pop("_pending_author", None)
        deliveries = (
            self._review_delivery(review, "submitted"),
            *(
                signed_delivery(
                    "pull_request_review_comment",
                    self._webhook("created", number, review["user"], comment=self._visible(c)),
                )
                for c in comments
            ),
        )
        self.deliveries.extend(deliveries)
        return Posted(self._visible(review), deliveries)

    def dismiss_review(self, number: int, review_id: int) -> Posted:
        """Dismiss submitted review ``review_id``; GitHub sends
        ``pull_request_review`` with action ``dismissed``.

        https://docs.github.com/en/rest/pulls/reviews#dismiss-a-review-for-a-pull-request
        """

        review = self._review_of(number, review_id)
        if review["state"] == "PENDING":
            raise ValueError(f"review {review_id} is pending")
        review["state"] = "DISMISSED"
        delivery = self._review_delivery(review, "dismissed")
        self.deliveries.append(delivery)
        return Posted(self._visible(review), (delivery,))

    def _review_delivery(self, review: dict[str, Any], action: str) -> WebhookDelivery:
        # The webhook review object is the REST one with a lowercase state.
        webhook_review = {**self._visible(review), "state": review["state"].lower()}
        return signed_delivery(
            "pull_request_review",
            self._webhook(action, review["_number"], review["user"], review=webhook_review),
        )

    def _webhook(
        self, action: str, number: int, sender: dict[str, Any], **objects: Any
    ) -> dict[str, Any]:
        # https://docs.github.com/en/webhooks/webhook-events-and-payloads#pull_request_review
        return {
            "action": action,
            **objects,
            "pull_request": self._pull_payload(number),
            "repository": {"id": self.repository_id, "full_name": REPO},
            "installation": {"id": self.installation_id},
            "sender": dict(sender),
        }

    def add_conversation_comment(self, number: int, user: dict[str, Any], body: str) -> Posted:
        """A comment by ``user`` on the conversation of issue or pull request
        ``number`` in the documented shape, with its ``issue_comment``
        ``created`` delivery. `add_comment` keeps its older, smaller shape.

        https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
        https://docs.github.com/en/webhooks/webhook-events-and-payloads#issue_comment
        """

        comment_id = next(self._comment_ids)
        stamp = self._now()
        kind = "pull" if number in self.pulls else "issues"
        comment = {
            "id": comment_id,
            "node_id": f"IC_fixture_{comment_id}",
            "url": f"{self.api_url}/repos/{REPO}/issues/comments/{comment_id}",
            "html_url": f"{self.html_base}/{REPO}/{kind}/{number}#issuecomment-{comment_id}",
            "issue_url": f"{self.api_url}/repos/{REPO}/issues/{number}",
            "body": body,
            "user": dict(user),
            "performed_via_github_app": None,
            "author_association": self._association(user),
            "created_at": stamp,
            "updated_at": stamp,
            "_number": number,
        }
        self.comments.append(comment)
        issue = dict(self.issues[number])
        if number in self.pulls:
            issue["state"] = self.pulls[number]["state"]
        delivery = signed_delivery(
            "issue_comment",
            {
                "action": "created",
                "issue": issue,
                "comment": self._visible(comment),
                "repository": {"id": self.repository_id, "full_name": REPO},
                "installation": {"id": self.installation_id},
                "sender": dict(user),
            },
        )
        self.deliveries.append(delivery)
        return Posted(self._visible(comment), (delivery,))

    def edit_comment(self, comment_id: int, body: str) -> dict[str, Any]:
        """Edit a conversation or review comment: the body changes and
        ``updated_at`` moves to the fake clock's now.

        https://docs.github.com/en/rest/pulls/comments#update-a-review-comment-for-a-pull-request
        https://docs.github.com/en/rest/issues/comments#update-an-issue-comment
        """

        for comment in (*self.comments, *self.review_comments):
            if comment["id"] == comment_id:
                comment["body"] = body
                comment["updated_at"] = self._now()
                return self._visible(comment)
        raise KeyError(comment_id)

    def _review_refusal(
        self, number: int, user: dict[str, Any], body: dict[str, Any]
    ) -> str | None:
        """Why the create-review call refuses ``body`` (422), or None."""

        event, comments = body.get("event"), body.get("comments") or []
        if event is not None and event not in REVIEW_EVENT_STATES:
            return "Unprocessable Entity"
        if not isinstance(comments, list) or not all(
            isinstance(c, dict)
            and isinstance(c.get("path"), str)
            and c["path"]
            and isinstance(c.get("body"), str)
            and c["body"].strip()
            and (c.get("line") is None or (type(c["line"]) is int and c["line"] > 0))
            for c in comments
        ):
            return "Validation Failed"
        text = body.get("body") or ""
        if event in {"COMMENT", "REQUEST_CHANGES"} and not str(text).strip() and not comments:
            return "Validation Failed"
        if body.get("commit_id") is not None and body["commit_id"] not in self.commits:
            return "Validation Failed"
        if any(
            r["_number"] == number and r.get("_pending_author") == user["id"] for r in self.reviews
        ):
            return "User can only have one pending review per pull request"
        return None

    def _write_reviews(
        self, request: httpx.Request, path: str, body: dict[str, Any]
    ) -> httpx.Response | None:
        repo = f"/repos/{REPO}"
        caller = self._caller(request)
        matched = re.fullmatch(rf"{repo}/pulls/(\d+)/reviews", path)
        if request.method == "POST" and matched:
            number = int(matched[1])
            if number not in self.pulls:
                return httpx.Response(404, json={"message": "Not Found"})
            refusal = self._review_refusal(number, caller, body)
            if refusal is not None:
                return httpx.Response(422, json={"message": refusal})
            posted = self.create_review(
                number,
                caller,
                body=str(body.get("body") or ""),
                event=body.get("event"),
                comments=list(body.get("comments") or []),
                commit_id=body.get("commit_id"),
            )
            return httpx.Response(200, json=posted.item)
        matched = re.fullmatch(rf"{repo}/pulls/(\d+)/reviews/(\d+)/(events|dismissals)", path)
        if matched and request.method in {"POST", "PUT"}:
            number, review_id = int(matched[1]), int(matched[2])
            try:
                review = self._review_of(number, review_id)
            except KeyError:
                review = None
            if review is None or not self._sees(caller, review):
                return httpx.Response(404, json={"message": "Not Found"})
            if matched[3] == "dismissals":
                if review["state"] == "PENDING":
                    return httpx.Response(422, json={"message": "Can not dismiss a pending review"})
                return httpx.Response(200, json=self.dismiss_review(number, review_id).item)
            event = body.get("event")
            if event not in REVIEW_EVENT_STATES or review["state"] != "PENDING":
                return httpx.Response(422, json={"message": "Unprocessable Entity"})
            submitted = self.submit_review(number, review_id, event, body=body.get("body"))
            return httpx.Response(200, json=submitted.item)
        matched = re.fullmatch(rf"{repo}/issues/(\d+)/comments", path)
        if request.method == "POST" and matched and caller is not self.bot_user:
            number = int(matched[1])
            if number not in self.issues:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(
                201, json=self.add_conversation_comment(number, caller, body["body"]).item
            )
        return None

    def _listed_since(
        self, request: httpx.Request, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """``items`` filtered by ``since`` on ``updated_at`` (an item without
        one is always listed) and sorted as the review comment listing is."""

        params = request.url.params
        since = _since(params.get("since"))
        kept = [
            item
            for item in items
            if since is None or "updated_at" not in item or _at(item["updated_at"]) >= since
        ]
        key = "updated_at" if params.get("sort") == "updated" else "created_at"
        return sorted(
            kept,
            key=lambda item: (_at(item.get(key) or item["created_at"]), item["id"]),
            reverse=params.get("direction") == "desc",
        )

    def _read_reviews(self, request: httpx.Request, path: str) -> httpx.Response | None:
        """The review reads, each answered as the caller may see it."""

        repo = f"/repos/{REPO}"
        caller = self._caller(request)
        comments = [c for c in self.review_comments if self._sees(caller, c)]
        if path == f"{repo}/pulls/comments":
            return self._page(request, self._listed_since(request, comments))
        matched = re.fullmatch(rf"{repo}/pulls/comments/(\d+)", path)
        if matched:
            return self._found(comments, matched[1])
        matched = re.fullmatch(rf"{repo}/pulls/(\d+)/(comments|reviews)", path)
        if matched:
            number = int(matched[1])
            if number not in self.pulls:
                return httpx.Response(404, json={"message": "Not Found"})
            listed = comments if matched[2] == "comments" else self.reviews
            own = [i for i in listed if i["_number"] == number and self._sees(caller, i)]
            return self._page(request, own)
        matched = re.fullmatch(rf"{repo}/pulls/(\d+)/reviews/(\d+)(/comments)?", path)
        if matched:
            number, review_id = int(matched[1]), int(matched[2])
            found = [
                r
                for r in self.reviews
                if r["_number"] == number and r["id"] == review_id and self._sees(caller, r)
            ]
            if number not in self.pulls or not found:
                return httpx.Response(404, json={"message": "Not Found"})
            if matched[3] is None:
                return httpx.Response(200, json=self._visible(found[0]))
            return self._page(request, self.comments_of_review(review_id))
        return None

    def push_head(self, branch: str, *, tree: str | None = None, message: str | None = None) -> str:
        """A new commit on ``branch``; returns its sha.

        ``tree`` is the commit's tree id. Without it the commit payload carries
        no ``tree`` field at all, a shape readers must still accept.
        """

        sha = f"{next(self._shas):040x}"
        parent = self.branches.get(branch)
        self.seed_commit(
            sha,
            parents=[parent] if parent else [],
            message=message if message is not None else f"change {sha[-4:]}",
            tree=tree,
        )
        self.branches[branch] = sha
        return sha

    def seed_commit(
        self, sha: str, *, parents: list[str], message: str, tree: str | None = None
    ) -> None:
        """A commit with an exact id, as a push from outside the adapters lands
        it. ``tree`` None leaves the ``tree`` field out of the commit payload."""

        commit: dict[str, Any] = {
            "sha": sha,
            "message": message,
            "parents": [{"sha": parent} for parent in parents],
        }
        if tree is not None:
            commit["tree"] = {"sha": tree}
        self.commits[sha] = commit

    def seed_pull(self, head: str, base: str, *, title: str = "Change", body: str = "") -> int:
        """An open pull request from ``head`` into ``base``, opened from outside
        the adapters (the publication push Job's half); returns its number."""

        number = self._number()
        self.issues[number] = self._pull_issue(number)
        self.pulls[number] = {
            "head": head,
            "base": base,
            "title": title,
            "body": body,
            "draft": False,
            "state": "open",
            "merged_at": None,
        }
        return number

    def fail_writes(self, pattern: str, status: int, count: int = 1) -> None:
        """The next ``count`` writes whose ``"<METHOD> <path>"`` matches the
        regular expression ``pattern`` answer ``status`` without changing state.
        Each refused write still counts in ``writes``: it reached GitHub."""

        self._write_faults.append([re.compile(pattern), status, count])

    def fail_reads(self, pattern: str, status: int, count: int = 1) -> None:
        """The next ``count`` GET requests whose ``"GET <path>"`` matches
        ``pattern`` answer ``status`` (``{"message": "injected"}``)."""

        self._read_faults.append([re.compile(pattern), status, count])

    def raw(self, commit_sha: str, path: str) -> bytes | None:
        """The bytes ``path`` names in ``commit_sha``'s tree, or None."""

        commit = self.commits.get(commit_sha)
        tree = (commit or {}).get("tree", {}).get("sha")
        parts = path.split("/")
        for depth, name in enumerate(parts):
            entries = self.trees.get(tree or "")
            if entries is None:
                return None
            found = next((entry for entry in entries if entry["name"] == name), None)
            if found is None:
                return None
            if depth == len(parts) - 1:
                return self.blobs.get(found["sha"]) if found["type"] == "blob" else None
            if found["type"] != "tree":
                return None
            tree = found["sha"]
        return None

    def evidence_ref(self, key: str) -> str | None:
        """The commit ``refs/curie/ui-evidence/<key>`` points to, or None."""

        return self.evidence_refs.get(f"refs/curie/ui-evidence/{key}")

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
        workflow = next((run["_workflow_run"] for run in runs.values()), next(self._workflow_runs))
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
        # Underscored keys are the fake's own bookkeeping; ``_links`` is GitHub's.
        return {
            key: value for key, value in item.items() if not key.startswith("_") or key == "_links"
        }

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
        side = {"repo": {"id": self.repository_id, "full_name": REPO}}
        return {
            "number": number,
            "node_id": f"PR_fixture_{number}",
            "html_url": f"https://github.com/{REPO}/pull/{number}",
            "merged": pull["merged_at"] is not None,
            # The App's installation token opened it.
            "user": dict(APP_BOT_USER),
            "state": pull["state"],
            "merged_at": pull["merged_at"],
            "title": pull["title"],
            "body": pull["body"],
            "draft": pull["draft"],
            "head": {"ref": pull["head"], "sha": self.branches[pull["head"]], **side},
            "base": {"ref": pull["base"], **side},
        }

    def _history(self, sha: str) -> list[str]:
        """``sha`` and every commit reachable through any parent, nearest first."""

        seen, queue = [sha], [sha]
        while queue:
            for parent in self.commits.get(queue.pop(0), {}).get("parents", []):
                if parent["sha"] not in seen:
                    seen.append(parent["sha"])
                    queue.append(parent["sha"])
        return seen

    def _compare(self, base: str, head: str) -> httpx.Response:
        # https://docs.github.com/en/rest/commits/commits#compare-two-commits
        if base not in self.commits or head not in self.commits:
            return httpx.Response(404, json={"message": "Not Found"})
        status = "diverged"
        if base == head:
            status = "identical"
        elif base in self._history(head):
            status = "ahead"
        elif head in self._history(base):
            status = "behind"
        return httpx.Response(200, json={"status": status})

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.http_requests.append(request)
        path = request.url.path
        if request.url.host == "github.com":
            return self._raw(request, path)
        if self._listing_fails(path):
            return httpx.Response(502, json={"message": "injected"})
        if request.method != "GET":
            return self._write(request, path)
        injected = self._injected_read(request, path)
        if injected is not None:
            return injected
        return (
            self._read(request, path)
            or self._read_reviews(request, path)
            or self._read_code(request, path)
        )

    def _injected_read(self, request: httpx.Request, path: str) -> httpx.Response | None:
        subject = f"GET {path}"
        for fault in self._read_faults:
            pattern, status, remaining = fault
            if remaining > 0 and pattern.search(subject):
                fault[2] = remaining - 1
                return httpx.Response(status, json={"message": "injected"})
        return None

    def _issue_listing(self, request: httpx.Request) -> httpx.Response:
        """List repository issues, pull requests included, oldest or newest first.

        https://docs.github.com/en/rest/issues/issues#list-repository-issues
        Honours ``creator`` (a login), ``state`` (open by default) and
        ``direction`` (desc by default) over ``sort=created``, then pages.
        """

        params = request.url.params
        state = params.get("state", "open")
        creator = params.get("creator")
        listed = [
            issue
            for _, issue in sorted(self.issues.items(), reverse=params.get("direction") != "asc")
            if (state == "all" or issue.get("state") == state)
            and (
                creator is None
                or issue.get("user", {}).get("login", "").casefold() == creator.casefold()
            )
        ]
        return self._page(request, listed)

    def _read(self, request: httpx.Request, path: str) -> httpx.Response | None:
        account = re.fullmatch(r"/user/(\d+)", path)
        if account:
            payload = self.accounts.get(account[1])
            if payload is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json=payload)
        if path == f"/repos/{REPO}/issues":
            return self._issue_listing(request)
        if path == f"/repos/{REPO}/issues/events":
            return self._page(request, sorted(self.events, key=lambda e: -e["id"]))
        if path == f"/repos/{REPO}/issues/comments":
            # ``since`` filters on ``updated_at``; a comment `add_comment` seeded
            # carries none and is always listed.
            # https://docs.github.com/en/rest/issues/comments#list-issue-comments-for-a-repository
            newest = request.url.params.get("direction") == "desc"
            since = _since(request.url.params.get("since"))
            ordered = sorted(
                (
                    c
                    for c in self.comments
                    if since is None or "updated_at" not in c or _at(c["updated_at"]) >= since
                ),
                key=lambda c: c["id"],
                reverse=newest,
            )
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
        if path in {repo, f"/repositories/{REPO_ID}"}:
            return httpx.Response(
                200,
                json={
                    "id": REPO_ID,
                    "full_name": REPO,
                    "visibility": self.visibility,
                    "default_branch": self.default_branch,
                },
            )
        if path.startswith(f"{repo}/git/ref/heads/"):
            sha = self.branches.get(path.removeprefix(f"{repo}/git/ref/heads/"))
            if sha is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json={"object": {"sha": sha, "type": "commit"}})
        if path.startswith(f"{repo}/git/matching-refs/heads/"):
            # https://docs.github.com/en/rest/git/refs#list-matching-references
            prefix = path.removeprefix(f"{repo}/git/matching-refs/heads/")
            return httpx.Response(
                200,
                json=[
                    {"ref": f"refs/heads/{branch}", "object": {"sha": sha, "type": "commit"}}
                    for branch, sha in self.branches.items()
                    if branch.startswith(prefix)
                ],
            )
        matched = re.fullmatch(rf"{repo}/git/commits/([0-9a-f]+)", path)
        if matched:
            commit = self.commits.get(matched[1])
            if commit is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json=commit)
        matched = re.fullmatch(rf"{repo}/git/refs?/(.+)", path)
        if matched and not matched[1].startswith("heads/"):
            # https://docs.github.com/en/rest/git/refs#get-a-reference
            ref = f"refs/{unquote(matched[1])}"
            sha = self.evidence_refs.get(ref)
            if sha is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json=self._ref_payload(ref, sha))
        matched = re.fullmatch(rf"{repo}/compare/([0-9a-f]+)\.\.\.([0-9a-f]+)", path)
        if matched:
            return self._compare(matched[1], matched[2])
        if path == f"{repo}/pulls":
            assert request.url.params.get("state") == "all"
            if "head" not in request.url.params:
                # Every pull request, one page (rediscovery's listing, #4629).
                return httpx.Response(200, json=[self._pull_payload(n) for n in self.pulls])
            owner, _, branch = request.url.params.get("head", "").partition(":")
            assert owner == REPO.split("/")[0]
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
                    200,
                    json={"state": "pending", "statuses": statuses, "total_count": len(statuses)},
                )
            runs = [self._visible(run) for run in self.check_runs.get(matched[1], {}).values()]
            return httpx.Response(200, json={"total_count": len(runs), "check_runs": runs})
        if re.fullmatch(rf"{repo}/check-runs/\d+/annotations", path):
            return httpx.Response(200, json=[])
        if re.fullmatch(rf"{repo}/rules/branches/.+", path):
            # HTTP 404 contributes no ruleset names.
            return httpx.Response(404, json={"message": "Not Found"})
        matched = re.fullmatch(rf"{repo}/branches/(.+)", path)
        if matched:
            name = matched[1]
            sha = self.branches.get(name)
            body: dict[str, Any] = {"name": name}
            if sha is not None:
                body["commit"] = {"sha": sha}
            return httpx.Response(200, json=body)
        # No job log is kept, so diagnostics report it unavailable.
        return httpx.Response(404, json={"message": "missing fixture"})

    def _raw(self, request: httpx.Request, path: str) -> httpx.Response:
        # GET https://github.com/<owner>/<repo>/raw/<commit sha>/<path>
        self.raw_requests.append(request)
        matched = re.fullmatch(rf"/{REPO}/raw/([0-9a-f]{{40}})/(.+)", path)
        content = None
        if request.method == "GET" and matched:
            content = self.raw(matched[1], unquote(matched[2]))
        if content is None:
            return httpx.Response(404, text="Not Found")
        kind = "image/png" if content.startswith(_PNG_SIGNATURE) else "application/octet-stream"
        return httpx.Response(200, content=content, headers={"Content-Type": kind})

    def _injected_failure(self, request: httpx.Request, path: str) -> httpx.Response | None:
        subject = f"{request.method} {path}"
        for fault in self._write_faults:
            pattern, status, remaining = fault
            if remaining > 0 and pattern.search(subject):
                fault[2] = remaining - 1
                return httpx.Response(status, json={"message": "injected"})
        return None

    def _write(self, request: httpx.Request, path: str) -> httpx.Response:
        self.writes += 1
        injected = self._injected_failure(request, path)
        if injected is not None:
            return injected
        body = json.loads(request.content) if request.content else {}
        reviewed = self._write_reviews(request, path, body)
        if reviewed is not None:
            return reviewed
        matched = re.fullmatch(rf"/repos/{REPO}/issues/(\d+)/comments", path)
        if request.method == "POST" and matched:
            comment = self.add_comment(int(matched[1]), CONTRACT_BOT, body["body"], app=True)
            return httpx.Response(201, json={"id": comment["id"]})
        matched = re.fullmatch(rf"/repos/{REPO}/(issues|pulls)/comments/(\d+)", path)
        if request.method == "PATCH" and matched:
            listed = self.comments if matched[1] == "issues" else self.review_comments
            caller = self._caller(request)
            for comment in listed:
                if comment["id"] == int(matched[2]) and self._sees(caller, comment):
                    return httpx.Response(200, json=self.edit_comment(comment["id"], body["body"]))
            return httpx.Response(404, json={"message": "Not Found"})
        matched = re.fullmatch(rf"/repos/{REPO}/issues/(\d+)/labels(?:/(.+))?", path)
        if matched and int(matched[1]) in self.issues:
            issue = self.issues[int(matched[1])]
            names = [entry["name"] for entry in issue["labels"]]
            if request.method == "POST":
                # GitHub labels also address PR numbers through the issues API:
                # https://docs.github.com/en/rest/issues/labels?apiVersion=2022-11-28#add-labels-to-an-issue
                self.label_requests.append(request)
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
            self.issues[number] = self._pull_issue(number)
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
        evidence = self._write_git_data(request, path, body)
        if evidence is not None:
            return evidence
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

    # Git Data API ---------------------------------------------------------

    @staticmethod
    def _ref_payload(ref: str, sha: str) -> dict[str, Any]:
        return {
            "ref": ref,
            "url": f"{GITHUB_API}/repos/{REPO}/git/{ref}",
            "object": {
                "sha": sha,
                "type": "commit",
                "url": f"{GITHUB_API}/repos/{REPO}/git/commits/{sha}",
            },
        }

    def _store_tree(self, entries: list[dict[str, str]]) -> str:
        sha = git_object_sha("tree", _git_tree_payload(entries))
        self.trees[sha] = entries
        return sha

    def _build_tree(self, blobs: dict[str, str]) -> str:
        """Nested git trees for ``path -> blob id``, as GitHub builds them from
        slash separated paths; returns the root tree id."""

        files: dict[str, str] = {}
        directories: dict[str, dict[str, str]] = {}
        for path, sha in blobs.items():
            head, _, rest = path.partition("/")
            if rest:
                directories.setdefault(head, {})[rest] = sha
            else:
                files[head] = sha
        entries = [
            {"name": name, "mode": "100644", "type": "blob", "sha": sha}
            for name, sha in files.items()
        ]
        entries += [
            {"name": name, "mode": "040000", "type": "tree", "sha": self._build_tree(inner)}
            for name, inner in directories.items()
        ]
        return self._store_tree(entries)

    def _write_git_data(
        self, request: httpx.Request, path: str, body: dict[str, Any]
    ) -> httpx.Response | None:
        repo = f"/repos/{REPO}"
        api = f"{GITHUB_API}{repo}"
        invalid = httpx.Response(422, json={"message": "Validation Failed"})
        if request.method == "POST" and path == f"{repo}/git/blobs":
            encoding = body.get("encoding", "utf-8")
            if encoding == "base64":
                content = base64.b64decode(body["content"])
            elif encoding == "utf-8":
                content = str(body["content"]).encode()
            else:
                return invalid
            sha = git_blob_sha(content)
            self.blobs[sha] = content
            return httpx.Response(201, json={"sha": sha, "url": f"{api}/git/blobs/{sha}"})
        if request.method == "POST" and path == f"{repo}/git/trees":
            blobs: dict[str, str] = {}
            for entry in body.get("tree", []):
                if (
                    entry.get("type") != "blob"
                    or entry.get("mode") != "100644"
                    or entry.get("sha") not in self.blobs
                ):
                    return invalid
                blobs[str(entry["path"])] = str(entry["sha"])
            if not blobs or "base_tree" in body:
                return invalid
            sha = self._build_tree(blobs)
            listed = [
                {
                    "path": entry["name"],
                    "mode": entry["mode"],
                    "type": entry["type"],
                    "sha": entry["sha"],
                    "url": f"{api}/git/{entry['type']}s/{entry['sha']}",
                }
                for entry in self.trees[sha]
            ]
            payload = {"sha": sha, "url": f"{api}/git/trees/{sha}", "tree": listed}
            return httpx.Response(201, json={**payload, "truncated": False})
        if request.method == "POST" and path == f"{repo}/git/commits":
            tree, parents = body.get("tree"), list(body.get("parents", []))
            if tree not in self.trees or any(parent not in self.commits for parent in parents):
                return invalid
            when = _stamp(0)
            author = dict(body.get("author") or {"name": APP_SLUG, "email": "", "date": when})
            committer = dict(body.get("committer") or author)
            sha = git_commit_sha(
                tree=tree,
                parents=parents,
                message=str(body["message"]),
                author=author,
                committer=committer,
            )
            self.seed_commit(sha, parents=parents, message=str(body["message"]), tree=tree)
            return httpx.Response(
                201,
                json={
                    "sha": sha,
                    "url": f"{api}/git/commits/{sha}",
                    "html_url": f"https://github.com/{REPO}/commit/{sha}",
                    "message": body["message"],
                    "author": author,
                    "committer": committer,
                    "tree": {"sha": tree, "url": f"{api}/git/trees/{tree}"},
                    "parents": [
                        {"sha": parent, "url": f"{api}/git/commits/{parent}"} for parent in parents
                    ],
                },
            )
        if request.method == "POST" and path == f"{repo}/git/refs":
            ref, sha = str(body.get("ref", "")), body.get("sha")
            if not ref.startswith("refs/") or ref.count("/") < 2 or sha not in self.commits:
                return invalid
            if ref.startswith("refs/heads/"):
                if ref.removeprefix("refs/heads/") in self.branches:
                    return httpx.Response(422, json={"message": "Reference already exists"})
                self.branches[ref.removeprefix("refs/heads/")] = sha
            else:
                if ref in self.evidence_refs:
                    return httpx.Response(422, json={"message": "Reference already exists"})
                self.evidence_refs[ref] = sha
            return httpx.Response(201, json=self._ref_payload(ref, sha))
        matched = re.fullmatch(rf"{repo}/git/refs/(.+)", path)
        if matched and request.method in {"PATCH", "DELETE"}:
            ref = f"refs/{unquote(matched[1])}"
            branch = ref.removeprefix("refs/heads/") if ref.startswith("refs/heads/") else None
            present = branch in self.branches if branch is not None else ref in self.evidence_refs
            if not present:
                return httpx.Response(422, json={"message": "Reference does not exist"})
            if request.method == "DELETE":
                if branch is not None:
                    del self.branches[branch]
                else:
                    del self.evidence_refs[ref]
                return httpx.Response(204)
            sha = body.get("sha")
            if sha not in self.commits:
                return invalid
            if branch is not None:
                self.branches[branch] = sha
            else:
                self.evidence_refs[ref] = sha
            return httpx.Response(200, json=self._ref_payload(ref, sha))
        return None


def evidence_raw_url(commit_sha: str, path: str) -> str:
    """The raw URL GitHub serves ``path`` at ``commit_sha`` under."""

    return f"https://github.com/{REPO}/raw/{commit_sha}/{quote(path)}"
