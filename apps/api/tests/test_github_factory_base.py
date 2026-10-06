"""A factory ticket declares its base branch and keeps it (#3095, ADR 0186).

GitHub REST shapes follow:
https://docs.github.com/en/rest/git/refs#get-a-reference
https://docs.github.com/en/rest/issues/events
https://docs.github.com/en/rest/issues/comments#list-issue-comments
https://docs.github.com/en/rest/issues/comments#create-an-issue-comment
https://docs.github.com/en/rest/issues/comments#update-an-issue-comment

Machine fixtures drive the events. They are not human-authored GitHub proof.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import Settings, get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from forge_fakes.github import (
    _ENV,
    INSTALLATION_ID,
    LABEL,
    REPO,
    REPO_ID,
    GitHubAPI,
    _Credentials,
    _issue_event,
    _post,
)
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_github_factory_ingress import _code, _rows

MARKER = "<!-- curie-factory-base-refusal -->"
MAIN_SHA = "1" * 40
NEXT_SHA = "2" * 40
TRAIN = {REPO: {"bases": ["main", "next"], "default_base": "main"}}
_ISSUES = itertools.count(31000)


class BaseGitHubAPI(GitHubAPI):
    """Adds the repository default branch, branch reads and issue comments."""

    def __init__(self) -> None:
        super().__init__()
        self.default_branch = "main"
        self.branches: dict[str, str] = {"main": MAIN_SHA, "next": NEXT_SHA}
        self.branch_requests: list[str] = []
        self.branch_outage = False
        self.issue_comments: dict[int, list[dict[str, Any]]] = {}
        self.comment_writes: list[tuple[str, str, str]] = []
        self._next_comment = itertools.count(880000)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/repos/{REPO}":
            return httpx.Response(
                200,
                json={
                    "id": self.repository_id,
                    "full_name": REPO,
                    "default_branch": self.default_branch,
                },
            )
        branch_prefix = f"/repos/{REPO}/git/ref/heads/"
        if path.startswith(branch_prefix):
            name = path[len(branch_prefix) :]
            self.branch_requests.append(name)
            if self.branch_outage:
                return httpx.Response(502, json={"message": "Bad Gateway"})
            sha = self.branches.get(name)
            if sha is None:
                return httpx.Response(404, json={"message": "Not Found"})
            return httpx.Response(200, json={"ref": f"refs/heads/{name}", "object": {"sha": sha}})
        parts = path.split("/")
        # /repos/{owner}/{name}/issues/{number}/comments
        if len(parts) == 7 and parts[4] == "issues" and parts[6] == "comments":
            number = int(parts[5])
            listed = self.issue_comments.setdefault(number, [])
            if request.method == "GET":
                per_page = int(request.url.params.get("per_page", "30"))
                page = int(request.url.params.get("page", "1"))
                return httpx.Response(200, json=listed[(page - 1) * per_page : page * per_page])
            if request.method == "POST":
                body = json.loads(request.content)["body"]
                comment = {
                    "id": next(self._next_comment),
                    "body": body,
                    "user": {"id": 99, "login": "curie[bot]", "type": "Bot"},
                    "performed_via_github_app": {"id": 51, "slug": "curie"},
                }
                listed.append(comment)
                self.comment_writes.append(("POST", path, body))
                return httpx.Response(201, json=comment)
        if request.method == "PATCH" and path.startswith(f"/repos/{REPO}/issues/comments/"):
            comment_id = int(path.rsplit("/", 1)[1])
            body = json.loads(request.content)["body"]
            for listed in self.issue_comments.values():
                for comment in listed:
                    if comment["id"] == comment_id:
                        comment["body"] = body
                        self.comment_writes.append(("PATCH", path, body))
                        return httpx.Response(200, json=comment)
            return httpx.Response(404, json={"message": "Not Found"})
        return super().handle(request)

    def posts(self) -> list[str]:
        return [body for method, _path, body in self.comment_writes if method == "POST"]

    def patches(self) -> list[str]:
        return [body for method, _path, body in self.comment_writes if method == "PATCH"]


def _configure(monkeypatch: pytest.MonkeyPatch, bases: dict[str, Any] | None) -> None:
    if bases is None:
        monkeypatch.delenv("GITHUB_FACTORY_BASES", raising=False)
    else:
        monkeypatch.setenv("GITHUB_FACTORY_BASES", json.dumps(bases))
    get_settings.cache_clear()


@pytest.fixture
def base_app(monkeypatch: pytest.MonkeyPatch, clean_db: None) -> Any:
    for key, value in _ENV.items():
        monkeypatch.setenv(key, value)
    _configure(monkeypatch, TRAIN)
    for module in ("github_factory", "repository_auth"):
        monkeypatch.setattr(f"curie_api.{module}.credentials_for", lambda _settings: _Credentials())
    api = BaseGitHubAPI()
    with TestClient(create_app()) as client:
        external = httpx.AsyncClient(transport=httpx.MockTransport(api.handle))
        client.app.state.http_client = external
        created = client.post(
            "/agents",
            headers={"X-API-Key": get_settings().api_key},
            json={
                "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
                "repo_full_name": REPO,
                "channel": {"kind": "github", "address": REPO},
            },
        )
        assert created.status_code == 201, created.text
        yield client, api
        client.portal.call(external.aclose)
    get_settings.cache_clear()


def _work_item(number: int) -> dict[str, Any] | None:
    rows = _rows(
        "SELECT id, base_branch, base_source, base_commit, base_label_ignored, "
        "publication_lineage_id FROM curie.work_items "
        "WHERE tracker_scope_id = :repo AND tracker_issue_id = :number",
        {"repo": str(REPO_ID), "number": str(number)},
    )
    return rows[0] if rows else None


def _label(client: TestClient, api: BaseGitHubAPI, number: int, *labels: str) -> httpx.Response:
    api.issue_number = number
    api.labels = [LABEL, *labels]
    api.advance_label_event(number)
    return _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))


def _execute(statement: str, params: dict[str, Any]) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(go())


def _open_lineage(work_item_id: uuid.UUID, *, status: str = "open") -> None:
    """Attach a thread publication lineage (an open factory PR) to the WorkItem."""

    item = _rows(
        "SELECT agent_id, conversation_id, repository_path FROM curie.work_items WHERE id = :id",
        {"id": work_item_id},
    )[0]
    version_id, deployment_id, lineage_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    _execute(
        "INSERT INTO curie.agent_versions (id, agent_id, version_label, created_by) "
        "VALUES (:id, :agent, 'v1', 'fixture')",
        {"id": version_id, "agent": item["agent_id"]},
    )
    _execute(
        "INSERT INTO curie.deployments (id, agent_id, version_id, environment, status) "
        "VALUES (:id, :agent, :version, CAST('dev' AS curie.environment), 'active')",
        {"id": deployment_id, "agent": item["agent_id"], "version": version_id},
    )
    _execute(
        "INSERT INTO curie.thread_publication_lineages "
        "(id, agent_id, deployment_id, conversation_id, repo_full_name, base_sha, branch, "
        "pr_number, pr_url, head_sha, status, version, latest_revision) VALUES "
        "(:id, :agent, :deployment, :conversation, :repo, :base, :branch, 77, :url, :head, "
        ":status, 1, 1)",
        {
            "id": lineage_id,
            "agent": item["agent_id"],
            "deployment": deployment_id,
            "conversation": item["conversation_id"],
            "repo": item["repository_path"],
            "base": NEXT_SHA,
            "branch": f"curie/publication-{lineage_id.hex}",
            "url": f"https://github.com/{REPO}/pull/77",
            "head": "3" * 40,
            "status": status,
        },
    )
    _execute(
        "UPDATE curie.work_items SET publication_lineage_id = :lineage, version = version + 1 "
        "WHERE id = :id",
        {"lineage": lineage_id, "id": work_item_id},
    )


# --- Declaration: the deployment setting ------------------------------------------


def _bases_entry(settings: Settings, repo: str) -> tuple[list[str], str | None]:
    entry = settings.github_factory_bases[repo]
    if isinstance(entry, dict):
        return list(entry["bases"]), entry.get("default_base")
    return list(entry.bases), entry.default_base


def test_factory_bases_default_to_empty_so_every_repo_uses_its_default_branch() -> None:
    settings = Settings(_env_file=None)

    assert not settings.github_factory_bases


def test_factory_bases_parse_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "GITHUB_FACTORY_BASES",
        json.dumps({"curie-eng/curie": {"bases": ["main", "next"], "default_base": "main"}}),
    )

    settings = Settings(_env_file=None)

    assert _bases_entry(settings, "curie-eng/curie") == (["main", "next"], "main")


def test_factory_bases_default_base_is_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "GITHUB_FACTORY_BASES", json.dumps({"acme-corp/acme-bot": {"bases": ["main"]}})
    )

    settings = Settings(_env_file=None)

    assert _bases_entry(settings, "acme-corp/acme-bot") == (["main"], None)


@pytest.mark.parametrize(
    "value",
    [
        {"acme-corp/acme-bot": {"bases": ["main", "next"], "default_base": "release-1"}},
        {"acme-corp/acme-bot": {"bases": [], "default_base": "main"}},
        {"acme-corp/acme-bot": {"bases": []}},
        {"not-a-repository": {"bases": ["main"]}},
        {"acme-corp/*": {"bases": ["main"]}},
        {"acme-corp/acme-bot": {"bases": ["main", "main"]}},
        {"acme-corp/acme-bot": {"bases": ["has space"]}},
        {"acme-corp/acme-bot": {"bases": ["-oops"]}},
        {"acme-corp/acme-bot": {"bases": ["a..b"]}},
        {"acme-corp/acme-bot": {"bases": [""]}},
        {"acme-corp/acme-bot": {"default_base": "main"}},
    ],
)
def test_invalid_factory_bases_refuse_boot(
    monkeypatch: pytest.MonkeyPatch, value: dict[str, Any]
) -> None:
    monkeypatch.setenv("GITHUB_FACTORY_BASES", json.dumps(value))

    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None)

    assert "GITHUB_FACTORY_BASES" in str(exc.value)


# --- Precedence ----------------------------------------------------------------------


def test_no_base_label_admits_on_the_deployment_default(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)

    response = _label(client, api, number)

    assert response.json()["status"] == "factory_admitted", response.text
    item = _work_item(number)
    assert item is not None
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
        "main",
        "default",
        MAIN_SHA,
    )
    assert item["base_label_ignored"] is None
    assert api.branch_requests == ["main"]
    assert api.posts() == []


def test_one_base_label_admits_on_that_branch_with_its_commit(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)

    response = _label(client, api, number, "base:next", "bug")

    assert response.json()["status"] == "factory_admitted", response.text
    item = _work_item(number)
    assert item is not None
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
        "next",
        "label",
        NEXT_SHA,
    )
    assert api.branch_requests == ["next"]


def test_two_distinct_base_labels_are_refused_without_a_work_item(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)

    response = _label(client, api, number, "base:next", "base:main")

    assert response.status_code == 200, response.text
    assert _code(response) == "base_conflict"
    assert _work_item(number) is None
    assert api.branch_requests == []
    (body,) = api.posts()
    assert MARKER in body


def test_a_base_label_outside_the_allowed_bases_is_refused(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    api.branches["release-1"] = "4" * 40

    response = _label(client, api, number, "base:release-1")

    assert _code(response) == "base_not_allowed"
    assert _work_item(number) is None
    assert api.branch_requests == []
    (body,) = api.posts()
    assert MARKER in body
    assert "base `release-1` is not an allowed base for this deployment" in body


def test_an_empty_base_label_is_not_an_allowed_base(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)

    response = _label(client, api, number, "base:")

    assert _code(response) == "base_not_allowed"
    assert _work_item(number) is None


def test_a_repository_absent_from_the_map_only_allows_its_default_branch(
    base_app: tuple[TestClient, BaseGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = base_app
    _configure(monkeypatch, {"acme-corp/other": {"bases": ["main", "next"]}})
    default_number = next(_ISSUES)
    labelled_number = next(_ISSUES)

    admitted = _label(client, api, default_number)
    refused = _label(client, api, labelled_number, "base:next")

    assert admitted.json()["status"] == "factory_admitted", admitted.text
    item = _work_item(default_number)
    assert item is not None
    assert (item["base_branch"], item["base_source"]) == ("main", "default")
    assert _code(refused) == "base_not_allowed"
    assert _work_item(labelled_number) is None


def test_an_unset_default_base_uses_the_repository_default_branch(
    base_app: tuple[TestClient, BaseGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    client, api = base_app
    _configure(monkeypatch, {REPO: {"bases": ["main", "next"]}})
    api.default_branch = "next"
    number = next(_ISSUES)

    response = _label(client, api, number)

    assert response.json()["status"] == "factory_admitted", response.text
    item = _work_item(number)
    assert item is not None
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (
        "next",
        "default",
        NEXT_SHA,
    )


def test_a_deployment_default_outside_the_allowed_bases_is_refused(
    base_app: tuple[TestClient, BaseGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no default_base, the repository default branch must itself be allowed."""

    client, api = base_app
    _configure(monkeypatch, {REPO: {"bases": ["next"]}})
    number = next(_ISSUES)

    response = _label(client, api, number)

    assert _code(response) == "base_not_allowed"
    assert _work_item(number) is None
    assert api.branch_requests == []
    (body,) = api.posts()
    assert MARKER in body
    assert "`main`" in body


# --- Missing base ----------------------------------------------------------------------


def test_a_missing_base_branch_is_refused_once_and_not_substituted(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    del api.branches["next"]
    number = next(_ISSUES)

    first = _label(client, api, number, "base:next")
    second = _label(client, api, number, "base:next")

    assert _code(first) == "base_missing"
    assert _code(second) == "base_missing"
    assert _work_item(number) is None
    assert _rows(
        "SELECT r.id FROM curie.execution_requests r JOIN curie.work_items w "
        "ON w.id = r.work_item_id WHERE w.tracker_issue_id = :number",
        {"number": str(number)},
    ) == []
    # No fallback to the default: main is never read.
    assert "main" not in api.branch_requests
    (body,) = api.posts()
    assert MARKER in body
    assert "base `next` does not exist in the repository" in body
    assert api.patches() == []
    assert len(api.issue_comments[number]) == 1


def test_a_changed_refusal_reason_edits_the_one_refusal_comment(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    del api.branches["next"]
    number = next(_ISSUES)

    assert _code(_label(client, api, number, "base:next")) == "base_missing"
    assert _code(_label(client, api, number, "base:release-1")) == "base_not_allowed"

    assert len(api.posts()) == 1
    (patched,) = api.patches()
    assert MARKER in patched
    assert "release-1" in patched
    assert len(api.issue_comments[number]) == 1


def test_a_fixed_label_admits_after_a_refusal(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    del api.branches["next"]
    number = next(_ISSUES)
    assert _code(_label(client, api, number, "base:next")) == "base_missing"

    api.branches["next"] = NEXT_SHA
    response = _label(client, api, number, "base:next")

    assert response.json()["status"] == "factory_admitted", response.text
    item = _work_item(number)
    assert item is not None
    assert (item["base_branch"], item["base_commit"]) == ("next", NEXT_SHA)


def test_a_branch_lookup_outage_is_retryable_not_a_refusal(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    api.branch_outage = True

    response = _label(client, api, number, "base:next")

    assert response.status_code == 503, response.text
    assert _work_item(number) is None
    assert api.posts() == []


# --- Frozen base -----------------------------------------------------------------------


def test_relabel_with_an_open_pr_keeps_the_recorded_base_without_a_branch_read(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    assert _label(client, api, number, "base:next").json()["status"] == "factory_admitted"
    item = _work_item(number)
    assert item is not None
    _open_lineage(item["id"])
    api.branch_requests.clear()
    # Even an unreadable branch must not matter: the recorded base is kept.
    del api.branches["main"]

    _label(client, api, number, "base:main")

    after = _work_item(number)
    assert after is not None
    assert (after["base_branch"], after["base_source"], after["base_commit"]) == (
        "next",
        "label",
        NEXT_SHA,
    )
    assert after["base_label_ignored"] == "main"
    assert api.branch_requests == []
    assert api.posts() == []


def test_a_base_label_change_event_records_the_ignored_label(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    assert _label(client, api, number, "base:next").json()["status"] == "factory_admitted"
    item = _work_item(number)
    assert item is not None
    _open_lineage(item["id"])
    api.branch_requests.clear()

    api.labels = [LABEL, "base:main"]
    changed = _post(client, "issues", _issue_event("labeled", number, label={"name": "base:main"}))

    assert changed.status_code == 200, changed.text
    assert _code(changed) == "base_label_recorded"
    after = _work_item(number)
    assert after is not None
    assert after["base_branch"] == "next"
    assert after["base_label_ignored"] == "main"
    assert api.branch_requests == []

    api.labels = [LABEL, "base:next"]
    restored = _post(
        client, "issues", _issue_event("unlabeled", number, label={"name": "base:main"})
    )

    assert _code(restored) == "base_label_recorded"
    after = _work_item(number)
    assert after is not None
    assert after["base_label_ignored"] is None


def test_a_base_label_event_without_a_work_item_is_ignored(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    api.issue_number = number
    api.labels = ["base:next"]

    response = _post(client, "issues", _issue_event("labeled", number, label={"name": "base:next"}))

    assert response.status_code == 200, response.text
    assert _code(response) == "work_item_absent"
    assert _work_item(number) is None
    assert api.branch_requests == []


def test_fresh_admission_without_an_open_pr_resolves_the_base_again(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    assert _label(client, api, number, "base:next").json()["status"] == "factory_admitted"
    item = _work_item(number)
    assert item is not None
    _open_lineage(item["id"], status="closed")
    api.branch_requests.clear()

    _label(client, api, number)

    after = _work_item(number)
    assert after is not None
    assert (after["base_branch"], after["base_source"], after["base_commit"]) == (
        "main",
        "default",
        MAIN_SHA,
    )
    assert after["base_label_ignored"] is None
    assert api.branch_requests == ["main"]


# --- Label reconcile poll ---------------------------------------------------------------


class _ReconcileCredentials(_Credentials):
    def fresh_installation_token(
        self, repo: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        return INSTALLATION_ID, self.token_for_verified_installation(repo, INSTALLATION_ID)


class _ReconcileGitHubAPI(BaseGitHubAPI):
    def __init__(self) -> None:
        super().__init__()
        self.labelled: dict[int, datetime] = {}

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == f"/repos/{REPO}/issues":
            issues = [
                {"number": n, "state": "open", "labels": [{"name": name} for name in self.labels]}
                for n in self.labelled
            ]
            return httpx.Response(200, json=issues)
        if path.startswith(f"/repos/{REPO}/issues/") and path.endswith("/events"):
            number = int(path.split("/")[-2])
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 900000 + number,
                        "event": "labeled",
                        "label": {"name": LABEL},
                        "actor": {"id": 6601, "login": "octocat", "type": "User"},
                        "performed_via_github_app": None,
                        "created_at": self.labelled[number]
                        .isoformat()
                        .replace("+00:00", "Z"),
                    }
                ],
            )
        return super().handle(request)


def test_reconcile_refuses_a_missing_base_with_one_comment_across_passes(
    base_app: tuple[TestClient, BaseGitHubAPI], monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_api.factory_label_reconcile import reconcile_missed_labels
    from sqlalchemy.ext.asyncio import async_sessionmaker

    for module in ("github_factory", "factory_label_reconcile", "repository_auth"):
        monkeypatch.setattr(
            f"curie_api.{module}.credentials_for", lambda _s: _ReconcileCredentials()
        )
    api = _ReconcileGitHubAPI()
    del api.branches["next"]
    number = next(_ISSUES)
    api.issue_number = number
    api.labels = [LABEL, "base:next"]
    api.labelled[number] = datetime.now(UTC) - timedelta(minutes=30)

    def reconcile() -> int:
        async def go() -> int:
            engine = create_async_engine(get_settings().database_url)
            http = httpx.AsyncClient(transport=httpx.MockTransport(api.handle))
            try:
                return await reconcile_missed_labels(
                    async_sessionmaker(engine, expire_on_commit=False),
                    get_settings(),
                    http,
                    now=datetime.now(UTC),
                )
            finally:
                await http.aclose()
                await engine.dispose()

        return asyncio.run(go())

    assert reconcile() == 0
    assert reconcile() == 0

    assert _work_item(number) is None
    (body,) = api.posts()
    assert MARKER in body
    assert api.patches() == []


def test_a_human_comment_carrying_the_refusal_marker_is_not_adopted(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    del api.branches["next"]
    number = next(_ISSUES)
    human = {
        "id": 770001,
        "body": f"Quoting the bot: {MARKER} please fix",
        "user": {"id": 6601, "login": "octocat", "type": "User"},
        "performed_via_github_app": None,
    }
    api.issue_comments[number] = [dict(human)]

    response = _label(client, api, number, "base:next")

    assert _code(response) == "base_missing"
    assert api.patches() == []
    (body,) = api.posts()
    assert MARKER in body
    assert "base `next` does not exist in the repository" in body
    assert api.issue_comments[number][0] == human


def test_another_apps_comment_carrying_the_refusal_marker_is_not_adopted(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    assert get_settings().github_app_id == "51"
    del api.branches["next"]
    number = next(_ISSUES)
    foreign = {
        "id": 770002,
        "body": f"{MARKER} posted by some other integration",
        "user": {"id": 4242, "login": "other-app[bot]", "type": "Bot"},
        "performed_via_github_app": {"id": 999, "slug": "other-app"},
    }
    api.issue_comments[number] = [dict(foreign)]

    response = _label(client, api, number, "base:next")

    assert _code(response) == "base_missing"
    assert api.patches() == []
    (body,) = api.posts()
    assert MARKER in body
    assert api.issue_comments[number][0] == foreign
    # Curie's own comment (configured app id 51) is adopted on the next pass.
    assert _code(_label(client, api, number, "base:next")) == "base_missing"
    assert len(api.posts()) == 1
