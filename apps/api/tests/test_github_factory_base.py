"""Factory labels select a request base at acquisition (ADR 0253).

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
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import Settings, get_settings
from curie_api.factory_base import bases_for
from curie_api.forges.config import ForgesConfig
from curie_api.forges.registry import ForgeRegistry, forges_registry
from curie_api.main import create_app
from fastapi.testclient import TestClient
from forge_fakes.github import (
    LABEL,
    REPO,
    REPO_ID,
    GitHubAPI,
    _Credentials,
    _issue_event,
    _post,
    factory_env,
    fake_installation_tokens,
    forges_document,
    forges_env_json,
    github_repository_binding,
)
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
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
        if path == f"/repos/{REPO}" or (
            request.method == "GET" and path == f"/repositories/{self.repository_id}"
        ):
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


def _bindings(bases: dict[str, Any]) -> list[dict[str, Any]]:
    """One GitHub repository binding per ``{repo: {"bases", "default_base"}}`` entry:
    the bases a repository allows live on its binding (#4397)."""

    return [
        github_repository_binding(
            repo=repo,
            repo_id=REPO_ID if repo == REPO else 9100 + index,
            bases=entry.get("bases"),
            default_base=entry.get("default_base"),
        )
        for index, (repo, entry) in enumerate(bases.items())
    ]


def _configure(
    monkeypatch: pytest.MonkeyPatch,
    key_dir: Path,
    bases: dict[str, Any],
    client: TestClient | None = None,
) -> None:
    """Set the forge config to ``bases``; a running app gets the new registry too."""

    for key, value in factory_env(key_dir, bindings=_bindings(bases)).items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    if client is not None:
        client.app.state.forges = forges_registry(get_settings())


@pytest.fixture
def base_app(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None) -> Any:
    _configure(monkeypatch, tmp_path, TRAIN)
    monkeypatch.setenv("FACTORY_BASE_LABEL_SETTLE_S", "0")
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", "factory-base-worker")
    monkeypatch.setenv("CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS", "600")
    get_settings.cache_clear()
    fake_installation_tokens(monkeypatch, _Credentials())
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


def _bases_entry(registry: ForgeRegistry, repo: str) -> tuple[list[str], str | None]:
    # The GitHub path passes the repository's GitHub binding policy (#4358 plan D15).
    entry = bases_for(registry.github_repository_policy(repo))
    assert entry is not None
    return list(entry.bases), entry.default_base


def _forges_env(monkeypatch: pytest.MonkeyPatch, key_dir: Path, bases: dict[str, Any]) -> None:
    monkeypatch.setenv(
        "CURIE_FORGES", forges_env_json(forges_document(bindings=_bindings(bases)), key_dir)
    )


def test_factory_bases_default_to_empty_so_every_repo_uses_its_default_branch() -> None:
    settings = Settings(_env_file=None)

    assert not settings.forges.bindings
    assert bases_for(forges_registry(settings).github_repository_policy(REPO)) is None


def test_factory_bases_parse_from_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _forges_env(
        monkeypatch,
        tmp_path,
        {"curie-eng/curie": {"bases": ["main", "next"], "default_base": "main"}},
    )

    settings = Settings(_env_file=None)

    assert _bases_entry(forges_registry(settings), "curie-eng/curie") == (
        ["main", "next"],
        "main",
    )


def test_factory_bases_default_base_is_optional(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _forges_env(monkeypatch, tmp_path, {"acme-corp/acme-bot": {"bases": ["main"]}})

    settings = Settings(_env_file=None)

    assert _bases_entry(forges_registry(settings), "acme-corp/acme-bot") == (["main"], None)


@pytest.mark.parametrize(
    "value",
    [
        {"acme-corp/acme-bot": {"bases": ["main", "next"], "default_base": "release-1"}},
        {"not-a-repository": {"bases": ["main"]}},
        {"acme-corp/*": {"bases": ["main"]}},
        {"acme-corp/acme-bot": {"bases": ["main", "main"]}},
        {"acme-corp/acme-bot": {"bases": ["has space"]}},
        {"acme-corp/acme-bot": {"bases": ["-oops"]}},
        {"acme-corp/acme-bot": {"bases": ["a..b"]}},
        {"acme-corp/acme-bot": {"bases": [""]}},
        {"acme-corp/acme-bot": {"bases": [], "default_base": "main"}},
        {"acme-corp/acme-bot": {"default_base": "main"}},
    ],
)
def test_invalid_factory_bases_refuse_boot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, value: dict[str, Any]
) -> None:
    with pytest.raises(ValidationError):
        ForgesConfig.model_validate(forges_document(bindings=_bindings(value)))

    _forges_env(monkeypatch, tmp_path, value)

    with pytest.raises(ValidationError) as exc:
        Settings(_env_file=None)

    assert "CURIE_FORGES" in str(exc.value)


WORKER = {"X-Curie-Worker-Token": "factory-base-worker"}


def _execution(number: int) -> dict[str, Any]:
    return _rows(
        "SELECT r.* FROM curie.execution_requests r JOIN curie.work_items w "
        "ON w.id = r.work_item_id WHERE w.tracker_issue_id = :number "
        "ORDER BY r.sequence DESC LIMIT 1",
        {"number": str(number)},
    )[0]


def _acquire_body(number: int, *, owner: str = "factory-base-owner") -> dict[str, Any]:
    from forge_fakes import factory_wake

    row = _execution(number)

    async def wake() -> dict[str, Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return (await factory_wake(session, row["id"])).model_dump(mode="json")
        finally:
            await engine.dispose()

    return {
        "owner": owner,
        "generation": row["dispatch_generation"],
        "wake": asyncio.run(wake()),
        "context": None,
    }


def _acquire(client: Any, number: int, *, owner: str = "factory-base-owner") -> Any:
    row = _execution(number)
    return client.post(
        f"/v1/internal/work-items/requests/{row['id']}/acquire",
        headers=WORKER,
        json=_acquire_body(number, owner=owner),
    )


def _admit_and_claim(client: Any, api: BaseGitHubAPI, number: int, *labels: str) -> Any:
    response = _label(client, api, number, *labels)
    assert response.json()["status"] == "factory_admitted", response.text
    assert api.branch_requests == []
    return _acquire(client, number)


@pytest.mark.parametrize(
    ("labels", "branch", "source", "sha"),
    [((), "main", "default", MAIN_SHA), (("base:next", "bug"), "next", "label", NEXT_SHA)],
)
def test_first_acquisition_pins_the_current_selection(
    base_app: tuple[TestClient, BaseGitHubAPI],
    labels: tuple[str, ...],
    branch: str,
    source: str,
    sha: str,
) -> None:
    client, api = base_app
    number = next(_ISSUES)

    response = _admit_and_claim(client, api, number, *labels)

    assert response.status_code == 200, response.text
    item = _work_item(number)
    assert item is not None
    assert (item["base_branch"], item["base_source"], item["base_commit"]) == (branch, source, sha)
    assert api.branch_requests == [branch]
    assert api.posts() == []


@pytest.mark.parametrize(
    ("labels", "reason", "read_branch"),
    [
        (("base:next", "base:main"), "base_conflict", None),
        (("base:release-1",), "base_not_allowed", None),
        (("base:",), "base_not_allowed", None),
        (("base:next",), "base_missing", "next"),
    ],
)
def test_invalid_current_selection_refuses_acquisition_without_fallback(
    base_app: tuple[TestClient, BaseGitHubAPI],
    labels: tuple[str, ...],
    reason: str,
    read_branch: str | None,
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    del api.branches["next"]

    first = _admit_and_claim(client, api, number, *labels)
    second = _acquire(client, number)

    assert first.status_code == second.status_code == 409
    item = _work_item(number)
    assert item is not None
    assert (item["base_branch"], item["base_commit"]) == (None, None)
    request = _execution(number)
    assert request["status"] == "waiting"
    assert request["acquire_owner"] is None
    assert request["start_deferrals"] == 0
    assert reason in request["last_deferral_reason"]
    assert (request["dispatch_not_before"] - request["updated_at"]).total_seconds() >= 60
    assert "main" not in api.branch_requests
    assert set(api.branch_requests) == ({read_branch} if read_branch else set())
    (body,) = api.posts()
    assert MARKER in body
    assert api.patches() == []
    assert len(api.issue_comments[number]) == 1


@pytest.mark.parametrize(
    ("policy", "default", "labels", "expected"),
    [
        ({REPO: {"bases": ["main", "next"]}}, "next", (), ("next", "default")),
        ({REPO: {"bases": []}}, "next", (), ("next", "default")),
        ({REPO: {"bases": ["next"]}}, "main", (), None),
        ({"acme-corp/other": {"bases": ["main", "next"]}}, "main", ("base:next",), None),
    ],
)
def test_claim_uses_only_the_bound_repository_default_policy(
    base_app: tuple[TestClient, BaseGitHubAPI],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    policy: dict[str, Any],
    default: str,
    labels: tuple[str, ...],
    expected: tuple[str, str] | None,
) -> None:
    client, api = base_app
    _configure(monkeypatch, tmp_path, policy, client)
    api.default_branch = default
    number = next(_ISSUES)

    result = _admit_and_claim(client, api, number, *labels)

    item = _work_item(number)
    assert item is not None
    if expected is None:
        assert result.status_code == 409, result.text
        assert item["base_commit"] is None
        assert api.branch_requests == []
        (body,) = api.posts()
        assert MARKER in body
    else:
        assert result.status_code == 200, result.text
        assert (item["base_branch"], item["base_source"]) == expected


@pytest.mark.parametrize("repair", ["branch_created", "label_corrected"])
def test_a_refused_request_can_claim_after_the_selection_is_repaired(
    base_app: tuple[TestClient, BaseGitHubAPI], repair: str
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    del api.branches["next"]
    first = _admit_and_claim(client, api, number, "base:next")
    assert first.status_code == 409, first.text
    request_id = _execution(number)["id"]
    if repair == "branch_created":
        api.branches["next"] = NEXT_SHA
        branch, sha = "next", NEXT_SHA
    else:
        api.labels = [LABEL, "base:main"]
        branch, sha = "main", MAIN_SHA
    _execute(
        "UPDATE curie.execution_requests SET dispatch_not_before = clock_timestamp(), "
        "published_generation = dispatch_generation WHERE id = :id",
        {"id": request_id},
    )

    fixed = _acquire(client, number)

    assert fixed.status_code == 200, fixed.text
    assert _execution(number)["id"] == request_id
    item = _work_item(number)
    assert item is not None
    assert (item["base_branch"], item["base_commit"]) == (branch, sha)
    assert len(api.posts()) == 1


def test_a_changed_refusal_reason_edits_the_one_refusal_comment(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    del api.branches["next"]
    assert _admit_and_claim(client, api, number, "base:next").status_code == 409
    api.labels = [LABEL, "base:release-1"]
    _execute(
        "UPDATE curie.execution_requests SET dispatch_not_before = clock_timestamp(), "
        "published_generation = dispatch_generation WHERE id = :id",
        {"id": _execution(number)["id"]},
    )

    assert _acquire(client, number).status_code == 409

    assert len(api.posts()) == 1
    (patched,) = api.patches()
    assert MARKER in patched
    assert "release-1" in patched
    assert len(api.issue_comments[number]) == 1


def test_a_branch_lookup_outage_stays_nonterminal_and_recovers(
    base_app: tuple[TestClient, BaseGitHubAPI],
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    api.branch_outage = True
    response = _admit_and_claim(client, api, number, "base:next")

    assert response.status_code == 409, response.text
    assert _execution(number)["status"] == "waiting"
    assert _execution(number)["start_deferrals"] == 0
    request = _execution(number)
    assert 0 < (request["dispatch_not_before"] - request["updated_at"]).total_seconds() <= 5
    assert api.posts() == []
    api.branch_outage = False
    _execute(
        "UPDATE curie.execution_requests SET dispatch_not_before = clock_timestamp(), "
        "published_generation = dispatch_generation WHERE id = :id",
        {"id": _execution(number)["id"]},
    )

    healthy = _acquire(client, number)

    assert healthy.status_code == 200, healthy.text
    assert _work_item(number)["base_commit"] == NEXT_SHA


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


@pytest.mark.parametrize("author", ["human", "foreign_app"])
def test_a_foreign_refusal_marker_is_not_adopted(
    base_app: tuple[TestClient, BaseGitHubAPI], author: str
) -> None:
    client, api = base_app
    number = next(_ISSUES)
    del api.branches["next"]
    human = author == "human"
    foreign = {
        "id": 770001,
        "body": f"Quoting the bot: {MARKER} please fix",
        "user": {"id": 6601, "login": "octocat", "type": "User"}
        if human
        else {"id": 4242, "login": "other-app[bot]", "type": "Bot"},
        "performed_via_github_app": None if human else {"id": 999, "slug": "other-app"},
    }
    api.issue_comments[number] = [dict(foreign)]

    assert _admit_and_claim(client, api, number, "base:next").status_code == 409
    assert _acquire(client, number).status_code == 409

    assert api.patches() == []
    (body,) = api.posts()
    assert MARKER in body
    assert "base `next` does not exist in the repository" in body
    assert api.issue_comments[number][0] == foreign


# --- The policy comes from the chosen binding (#4358 plan D15, r1 finding 4) -------


def _policy(**fields: Any) -> Any:
    from curie_api.forges.config import RepositoryBindingConfig

    return RepositoryBindingConfig.model_validate(
        {"alias": "repo", "code_host": "github", "project_id": str(REPO_ID), "path": REPO, **fields}
    )


def test_bases_for_reads_only_the_policy_it_is_given() -> None:
    assert bases_for(None) is None
    assert bases_for(_policy()) is None
    listed = _policy(bases=["main", "next"], default_base="main")
    assert bases_for(listed) is listed


@pytest.mark.parametrize(
    ("policy", "labels", "expected"),
    [
        # The selected binding allows `next`: the label picks it.
        ({"bases": ["main", "next"]}, {"base:next"}, "next"),
        # The selected binding lists no bases: only the default branch, so a
        # `base:` label another binding would allow is refused.
        ({}, {"base:next"}, None),
        ({"bases": ["main", "next"], "default_base": "next"}, set(), "next"),
    ],
)
def test_resolve_base_takes_the_policy_in_place_of_a_repository_lookup(
    policy: dict[str, Any], labels: set[str], expected: str | None
) -> None:
    from curie_api.factory_base import BaseRefusal, ResolvedBase, resolve_base
    from curie_api.forges import types
    from curie_api.forges.memory import InMemoryCodeHost

    code_host = InMemoryCodeHost(kind=types.GITHUB, host="github.com")
    repository = code_host.seed_repository(REPO, project_id=str(REPO_ID))
    code_host.push(repository, "main", "initial commit")
    code_host.push(repository, "next", "next commit")

    async def go() -> Any:
        return await resolve_base(
            policy=_policy(**policy),
            labels=labels,
            default_branch="main",
            code_host=code_host,
            repository=repository,
        )

    result = asyncio.run(go())
    if expected is None:
        assert isinstance(result, BaseRefusal), result
    else:
        assert isinstance(result, ResolvedBase), result
        assert result.branch == expected
