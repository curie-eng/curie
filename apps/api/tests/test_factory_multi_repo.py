"""One factory agent serves every GitHub repository bound to it (#4346).

A GitHub channel binding (kind ``github``, address ``owner/name``) is what makes
its agent serve that repository. The agent's own ``repo_full_name`` (its
git-flow repository) plays no part in factory routing: admission, cancellation,
review feedback, polling and the missed-label backstop all follow the bindings.
Two bindings on one repository, on one agent or across agents, still refuse.

Every agent here deliberately has a ``repo_full_name`` that NO binding names,
so a test passes only when routing reads the binding alone.

Payload shapes follow GitHub's webhook catalog:
https://docs.github.com/en/webhooks/webhook-events-and-payloads
REST reads follow the shapes the shared fakes already record and cite
(``forge_fakes.github``, ``test_github_factory_review.ReviewGitHubAPI``,
``test_factory_poll_intake.PollGitHub``). The second repository reuses those
exact shapes: ``_Forge`` routes a ``/repos/<owner>/<name>`` request to that
repository's own fake instance, rewritten to the path the fake knows, and
renames the repository's full name and id in what comes back. No new GitHub
response shape is introduced.

Machine fixtures drive the events. They are not human-authored GitHub proof.
"""

from __future__ import annotations

import asyncio
import sys
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.factory_label_reconcile import bound_repositories, reconcile_missed_labels
from curie_api.factory_poll_intake import poll_once
from curie_api.github_app import GitHubInstallationRefused
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes.github import (  # noqa: E402
    INSTALLATION_ID,
    LABEL,
    REPO,
    REPO_ID,
    _issue_event,
    _post,
    factory_env,
    fake_installation_tokens,
)
from test_factory_poll_intake import PollGitHub  # noqa: E402
from test_github_factory_ingress import _code, _rows  # noqa: E402
from test_github_factory_review import (  # noqa: E402
    BASE_REF,
    EVENTS,
    FRAGMENTS,
    HEAD,
    ReviewGitHubAPI,
    _branch,
    _complete,
    _feedback_event,
    _mention,
    _pr_url,
)

# The first bound repository is the shared fake's. The second is served by its
# own fake instance through ``_Forge``. The agent's git-flow repository is a
# third that neither binding names.
SECOND_REPO = "acme-corp/widget-svc"
SECOND_REPO_ID = 4402
AGENT_REPO = "acme-corp/factory-home"
_REPO_IDS = {REPO: REPO_ID, SECOND_REPO: SECOND_REPO_ID}
_BOUND = (REPO, SECOND_REPO)

_ISSUE_NUMBERS = iter(range(9900, 10900))
_PULL_NUMBERS = iter(range(510, 1510))


class _RepoFake(Protocol):
    def handle(self, request: httpx.Request) -> httpx.Response: ...


def _rename(value: Any, repo: str) -> Any:
    """A REST body or webhook payload of the shared fake, as ``repo`` serves it.

    Every string naming the shared fake's repository names ``repo`` instead, and
    a repository object (``{"id": ..., "full_name": ...}``) carries ``repo``'s id.
    """

    if repo == REPO:
        return value
    if isinstance(value, str):
        return value.replace(REPO, repo)
    if isinstance(value, list):
        return [_rename(item, repo) for item in value]
    if isinstance(value, dict):
        renamed = {key: _rename(item, repo) for key, item in value.items()}
        if renamed.get("full_name") == repo and renamed.get("id") == REPO_ID:
            renamed["id"] = _REPO_IDS[repo]
        return renamed
    return value


@dataclass
class _Forge:
    """Two GitHub repositories behind one transport, each with its own fake.

    ``seen`` records every request path as the API sent it, so a test can tell
    which repositories GitHub was asked about.
    """

    fakes: dict[str, _RepoFake]
    seen: list[str] = field(default_factory=list)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.seen.append(path)
        for repo, fake in self.fakes.items():
            root = f"/repos/{repo}"
            if path != root and not path.startswith(f"{root}/"):
                continue
            if repo == REPO:
                return fake.handle(request)
            inner = httpx.Request(
                request.method,
                request.url.copy_with(path=f"/repos/{REPO}{path[len(root) :]}"),
                headers=request.headers,
                content=request.content,
            )
            answered = fake.handle(inner)
            headers = {k: v for k, v in answered.headers.items() if k.lower() == "etag"}
            if not answered.content:
                return httpx.Response(answered.status_code, headers=headers)
            return httpx.Response(
                answered.status_code, json=_rename(answered.json(), repo), headers=headers
            )
        return httpx.Response(404, json={"message": "missing fixture"})

    def asked_about(self, repo: str) -> list[str]:
        root = f"/repos/{repo}"
        return [path for path in self.seen if path == root or path.startswith(f"{root}/")]


class _TwoRepoCredentials:
    """The App installation token for both bound repositories, one installation."""

    def fresh_installation_token(
        self, repo: str, expected_installation_id: int | None = None
    ) -> tuple[int, str]:
        if repo not in _REPO_IDS or expected_installation_id not in (None, INSTALLATION_ID):
            raise GitHubInstallationRefused("installation was not rediscovered")
        return INSTALLATION_ID, "fixture-installation-token"

    def token_for_verified_installation(self, repo: str, installation_id: int) -> str:
        return self.fresh_installation_token(repo, installation_id)[1]

    def token_for(self, repo: str) -> str:
        return self.fresh_installation_token(repo)[1]


def _patch_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    credentials = _TwoRepoCredentials()
    fake_installation_tokens(monkeypatch, credentials)


@pytest.fixture
def forge_app(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, clean_db: None
) -> Iterator[tuple[TestClient, _Forge]]:
    for key, value in factory_env(tmp_path).items():
        monkeypatch.setenv(key, value)
    get_settings.cache_clear()
    _patch_credentials(monkeypatch)
    second = ReviewGitHubAPI()
    second.repository_id = SECOND_REPO_ID
    forge = _Forge({REPO: ReviewGitHubAPI(), SECOND_REPO: second})
    try:
        with TestClient(create_app()) as client:
            external = httpx.AsyncClient(transport=httpx.MockTransport(forge.handle))
            client.app.state.http_client = external
            try:
                yield client, forge
            finally:
                client.portal.call(external.aclose)
    finally:
        get_settings.cache_clear()


def _headers() -> dict[str, str]:
    return {"X-API-Key": get_settings().api_key}


def _create_agent(client: TestClient, repo_full_name: str, channel: dict[str, Any]) -> str:
    created = client.post(
        "/agents",
        headers=_headers(),
        json={
            "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
            "repo_full_name": repo_full_name,
            "channel": channel,
        },
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])


def _bind(client: TestClient, agent_id: str, channel: dict[str, Any]) -> None:
    """Add one more binding through the route ``curie cluster surfaces --add`` calls."""

    added = client.post(f"/agents/{agent_id}/channels", headers=_headers(), json=channel)
    assert added.status_code == 201, added.text


def _multi_repo_agent(client: TestClient) -> str:
    """One agent bound to both repositories; its own repository is neither."""

    agent_id = _create_agent(client, AGENT_REPO, {"kind": "github", "address": REPO})
    _bind(client, agent_id, {"kind": "github", "address": SECOND_REPO})
    bound = _rows(
        "SELECT address FROM curie.agent_channels WHERE agent_id = :agent AND kind = 'github'",
        {"agent": uuid.UUID(agent_id)},
    )
    assert sorted(row["address"] for row in bound) == sorted(_BOUND)
    return agent_id


def _work(repo: str, number: int | None = None) -> list[dict[str, Any]]:
    """Each WorkItem of ``repo`` (or one of its issues) with its requests, in order."""

    statement = (
        "SELECT w.id AS work_item_id, w.agent_id, w.repository_path, "
        "w.repository_project_id, w.tracker_issue_id, w.cancelled_at, "
        "r.id, r.sequence, r.status, r.terminal_cause, r.objective, r.reply_address "
        "FROM curie.work_items w "
        "LEFT JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "WHERE w.tracker_scope_id = :scope"
    )
    params: dict[str, Any] = {"scope": str(_REPO_IDS[repo])}
    if number is not None:
        statement += " AND w.tracker_issue_id = :number"
        params["number"] = str(number)
    return _rows(statement + " ORDER BY w.tracker_issue_id, r.sequence", params)


def _issue_payload(repo: str, action: str, number: int) -> dict[str, Any]:
    payload = _issue_event(action, number, label={"name": LABEL})
    renamed: dict[str, Any] = _rename(payload, repo)
    return renamed


def _deliver_label(client: TestClient, forge: _Forge, repo: str) -> tuple[int, httpx.Response]:
    number = next(_ISSUE_NUMBERS)
    fake = forge.fakes[repo]
    assert isinstance(fake, ReviewGitHubAPI)
    fake.issue_number = number
    fake.labels = [LABEL]
    return number, _post(client, "issues", _issue_payload(repo, "labeled", number))


def _admit(client: TestClient, forge: _Forge, repo: str) -> tuple[int, dict[str, Any]]:
    number, response = _deliver_label(client, forge, repo)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted", (repo, response.text)
    rows = _work(repo, number)
    assert len(rows) == 1, rows
    return number, rows[0]


def test_multi_repo_agent_admits_a_labelled_issue_in_each_bound_repository(
    forge_app: tuple[TestClient, _Forge],
) -> None:
    """Fails on the old join: neither repository is the agent's repo_full_name."""

    client, forge = forge_app
    agent_id = _multi_repo_agent(client)

    admitted = {repo: _admit(client, forge, repo) for repo in _BOUND}

    for repo, (number, row) in admitted.items():
        assert str(row["agent_id"]) == agent_id
        assert row["repository_path"] == repo
        assert row["repository_project_id"] == str(_REPO_IDS[repo])
        assert row["tracker_issue_id"] == str(number)
        assert row["status"] == "waiting"
        assert row["objective"] == f"https://github.com/{repo}/issues/{number}"
        assert row["reply_address"] == repo
    assert admitted[REPO][1]["work_item_id"] != admitted[SECOND_REPO][1]["work_item_id"]
    everything = _rows("SELECT agent_id, repository_path FROM curie.work_items")
    assert sorted(row["repository_path"] for row in everything) == sorted(_BOUND)
    assert {str(row["agent_id"]) for row in everything} == {agent_id}
    assert (
        _rows("SELECT id FROM curie.work_items WHERE repository_path = :repo", {"repo": AGENT_REPO})
        == []
    )


def test_multi_repo_label_removal_cancels_work_in_each_repository(
    forge_app: tuple[TestClient, _Forge],
) -> None:
    """Cancellation already routes by the WorkItem; it fails on the old join only
    because admission in a repository other than the agent's is its precondition.
    Removing the label in one repository leaves the other repository's work alone.
    """

    client, forge = forge_app
    _multi_repo_agent(client)
    numbers = {repo: _admit(client, forge, repo)[0] for repo in _BOUND}

    for done, repo in enumerate(_BOUND, start=1):
        fake = forge.fakes[repo]
        assert isinstance(fake, ReviewGitHubAPI)
        fake.labels = []
        removed = _post(client, "issues", _issue_payload(repo, "unlabeled", numbers[repo]))

        assert removed.status_code == 200, removed.text
        assert removed.json()["status"] == "factory_cancelled", removed.text
        (row,) = _work(repo, numbers[repo])
        assert row["status"] == "cancelled"
        assert row["terminal_cause"] == "issue_cancelled"
        assert row["cancelled_at"] is not None
        for later in _BOUND[done:]:
            (untouched,) = _work(later, numbers[later])
            assert untouched["status"] == "waiting", later
            assert untouched["cancelled_at"] is None, later


def test_multi_repo_same_issue_number_in_each_repository_stays_separate(
    forge_app: tuple[TestClient, _Forge],
) -> None:
    """Issue numbers are per repository, so one number in two bound repositories
    is two WorkItems on the one agent, and removing the label in one repository
    cancels only that repository's WorkItem.
    """

    client, forge = forge_app
    agent_id = _multi_repo_agent(client)
    number = next(_ISSUE_NUMBERS)
    fakes: dict[str, ReviewGitHubAPI] = {}
    for repo in _BOUND:
        fake = forge.fakes[repo]
        assert isinstance(fake, ReviewGitHubAPI)
        fake.issue_number = number
        fake.labels = [LABEL]
        fakes[repo] = fake
    assert fakes[REPO] is not fakes[SECOND_REPO]

    admitted: dict[str, dict[str, Any]] = {}
    for repo in _BOUND:
        response = _post(client, "issues", _issue_payload(repo, "labeled", number))
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "factory_admitted", (repo, response.text)
        (row,) = _work(repo, number)
        admitted[repo] = row

    for repo, row in admitted.items():
        assert str(row["agent_id"]) == agent_id
        assert row["repository_path"] == repo
        assert row["repository_project_id"] == str(_REPO_IDS[repo])
        assert row["tracker_issue_id"] == str(number)
        assert row["status"] == "waiting"
        assert row["objective"] == f"https://github.com/{repo}/issues/{number}"
    assert admitted[REPO]["work_item_id"] != admitted[SECOND_REPO]["work_item_id"]
    everything = _rows("SELECT agent_id, repository_path, tracker_issue_id FROM curie.work_items")
    assert sorted((row["repository_path"], row["tracker_issue_id"]) for row in everything) == (
        sorted((repo, str(number)) for repo in _BOUND)
    )
    assert {str(row["agent_id"]) for row in everything} == {agent_id}

    # Only the second repository loses its label; the first fake still has it.
    fakes[SECOND_REPO].labels = []
    assert fakes[REPO].labels == [LABEL]
    removed = _post(client, "issues", _issue_payload(SECOND_REPO, "unlabeled", number))

    assert removed.status_code == 200, removed.text
    assert removed.json()["status"] == "factory_cancelled", removed.text
    (cancelled,) = _work(SECOND_REPO, number)
    assert cancelled["work_item_id"] == admitted[SECOND_REPO]["work_item_id"]
    assert cancelled["status"] == "cancelled"
    assert cancelled["terminal_cause"] == "issue_cancelled"
    assert cancelled["cancelled_at"] is not None
    (untouched,) = _work(REPO, number)
    assert untouched["work_item_id"] == admitted[REPO]["work_item_id"]
    assert untouched["status"] == "waiting"
    assert untouched["cancelled_at"] is None
    assert untouched["terminal_cause"] is None


def test_multi_repo_stale_repository_id_is_refused_and_the_other_repository_still_admits(
    forge_app: tuple[TestClient, _Forge],
) -> None:
    """A notice whose repository id is not the one GitHub returns at that name
    (a repository replaced at the same name) is refused with repository_mismatch.
    The other bound repository still admits.
    """

    client, forge = forge_app
    _multi_repo_agent(client)
    number = next(_ISSUE_NUMBERS)
    fake = forge.fakes[SECOND_REPO]
    assert isinstance(fake, ReviewGitHubAPI)
    fake.issue_number = number
    fake.labels = [LABEL]
    payload = _issue_payload(SECOND_REPO, "labeled", number)
    assert payload["repository"]["id"] == SECOND_REPO_ID
    payload["repository"]["id"] = SECOND_REPO_ID + 1000  # GitHub still returns SECOND_REPO_ID

    stale = _post(client, "issues", payload)

    assert stale.status_code == 200, stale.text
    assert stale.json()["status"] == "factory_ignored", stale.text
    assert _code(stale) == "repository_mismatch"
    assert (
        _rows(
            "SELECT id FROM curie.work_items WHERE repository_path = :repo", {"repo": SECOND_REPO}
        )
        == []
    )

    _admit(client, forge, REPO)
    assert len(_rows("SELECT id FROM curie.work_items")) == 1


def _own_pull_request_in(repo: str, work_item_id: uuid.UUID, pr: int) -> None:
    """Seed the lineage a factory publication leaves in ``repo``.

    Mirrors ``test_github_factory_review._own_pull_request`` with the repository
    named, since that helper is fixed to the shared fake's repository.
    """

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                item = (
                    (
                        await conn.execute(
                            text(
                                "SELECT agent_id, conversation_id "
                                "FROM curie.work_items WHERE id = :id"
                            ),
                            {"id": work_item_id},
                        )
                    )
                    .mappings()
                    .one()
                )
                version_id, deployment_id, lineage_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
                await conn.execute(
                    text(
                        "INSERT INTO curie.agent_versions "
                        "(id, agent_id, version_label, created_by) "
                        "VALUES (:id, :agent, :label, 'fixture')"
                    ),
                    {"id": version_id, "agent": item["agent_id"], "label": f"v{pr}"},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.deployments "
                        "(id, agent_id, version_id, environment, status) VALUES "
                        "(:id, :agent, :version, CAST('dev' AS curie.environment), 'active')"
                    ),
                    {"id": deployment_id, "agent": item["agent_id"], "version": version_id},
                )
                await conn.execute(
                    text(
                        "INSERT INTO curie.thread_publication_lineages "
                        "(id, agent_id, deployment_id, conversation_id, repo_full_name, "
                        "base_sha, branch, pr_number, pr_url, head_sha, status, version, "
                        "latest_revision, code_host_kind, code_host_host, "
                        "repository_project_id, code_host_installation_id, "
                        "code_host_pr_id, base_ref) VALUES "
                        "(:id, :agent, :deployment, :conversation, :repo, :base, :branch, "
                        ":pr, :url, :head, 'open', 1, 1, 'github', 'github.com', :repo_id, "
                        ":installation, :node, :base_ref)"
                    ),
                    {
                        "id": lineage_id,
                        "agent": item["agent_id"],
                        "deployment": deployment_id,
                        "conversation": item["conversation_id"],
                        "repo": repo,
                        "base": "0123456789abcdef0123456789abcdef01234567",
                        "branch": _branch(pr),
                        "pr": pr,
                        "url": _rename(_pr_url(pr), repo),
                        "head": HEAD,
                        "repo_id": str(_REPO_IDS[repo]),
                        "installation": INSTALLATION_ID,
                        "node": f"PR_acme_{pr}",
                        "base_ref": BASE_REF,
                    },
                )
                await conn.execute(
                    text(
                        "UPDATE curie.work_items SET publication_lineage_id = :lineage, "
                        "version = version + 1 WHERE id = :id"
                    ),
                    {"lineage": lineage_id, "id": work_item_id},
                )
        finally:
            await engine.dispose()

    asyncio.run(go())


@pytest.mark.parametrize("event", EVENTS)
def test_multi_repo_review_feedback_routes_for_each_repository(
    forge_app: tuple[TestClient, _Forge], event: str
) -> None:
    """Review feedback on a factory PR in either bound repository is the next
    request on that repository's WorkItem, on the same agent.

    Review routing reads the publication lineage, not the agent's repository, so
    on the old join this fails at its precondition: the labelled issue is never
    admitted because neither repository is the agent's repo_full_name.
    """

    client, forge = forge_app
    agent_id = _multi_repo_agent(client)

    for repo in _BOUND:
        fake = forge.fakes[repo]
        assert isinstance(fake, ReviewGitHubAPI)
        number, first = _admit(client, forge, repo)
        pr = next(_PULL_NUMBERS)
        fake.open_pull(pr)
        _own_pull_request_in(repo, first["work_item_id"], pr)
        _complete(first["id"])
        payload, fid = _feedback_event(fake, event, pr, _mention())

        response = _post(client, event, _rename(payload, repo))

        assert response.status_code == 200, response.text
        assert response.json()["status"] == "factory_admitted", (repo, response.text)
        rows = _work(repo, number)
        assert [row["sequence"] for row in rows] == [1, 2]
        revision = rows[1]
        assert revision["work_item_id"] == first["work_item_id"]
        assert str(revision["agent_id"]) == agent_id
        assert revision["status"] == "waiting"
        assert revision["reply_address"] == repo
        assert revision["objective"].splitlines()[0] == (
            f"https://github.com/{repo}/pull/{pr}#{FRAGMENTS[event].format(id=fid)}"
        )


def _route(adapter: str, trigger_label: str | None = None) -> dict[str, Any]:
    channel: dict[str, Any] = {
        "kind": "github",
        "address": REPO,
        "adapter": adapter,
        "endpoint": f"http://{adapter}.test/reply",
    }
    if trigger_label is not None:
        channel["trigger_label"] = trigger_label
    return channel


@pytest.mark.parametrize("layout", ["across_agents", "one_agent"])
def test_multi_repo_label_routing_same_effective_label_on_one_repository_refuses(
    forge_app: tuple[TestClient, _Forge], layout: str
) -> None:
    """#4346 decision 4 as #4361 amends it: two bindings on one repository with
    the same effective label are refused, on one agent or across agents, even
    under distinct adapters. The refusal moved from the delivery (binding_missing)
    to the write (409), so the one binding left admits the label alone.

    The runtime twin, two bindings that come to share a label when the tracker
    label changes, is test_factory_label_routing's
    test_label_routing_install_label_changed_to_collide_admits_nothing.
    """

    client, forge = forge_app
    first = _create_agent(client, "acme-corp/factory-home-a", _route("factory-a"))
    if layout == "across_agents":
        refused = client.post(
            "/agents",
            headers=_headers(),
            json={
                "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
                "repo_full_name": "acme-corp/factory-home-b",
                "channel": _route("factory-b"),
            },
        )
    else:
        refused = client.post(
            f"/agents/{first}/channels", headers=_headers(), json=_route("factory-b")
        )

    assert refused.status_code == 409, refused.text
    bound = _rows(
        "SELECT agent_id FROM curie.agent_channels WHERE kind = 'github' AND address = :repo",
        {"repo": REPO},
    )
    assert [str(row["agent_id"]) for row in bound] == [first]

    number, _first_number_row = _admit(client, forge, REPO)
    assert str(_work(REPO, number)[0]["agent_id"]) == first


def test_multi_repo_label_routing_different_labels_on_one_repository_admit(
    forge_app: tuple[TestClient, _Forge],
) -> None:
    """The other half of the amended rule: two agents on one repository under
    different labels both bind, and the install label admits on its binding
    instead of being refused binding_missing for the binding count."""

    client, forge = forge_app
    full = _create_agent(client, "acme-corp/factory-home-a", {"kind": "github", "address": REPO})
    fast = _create_agent(
        client,
        "acme-corp/factory-home-b",
        {"kind": "github", "address": REPO, "trigger_label": f"{LABEL}-fast"},
    )
    bound = _rows(
        "SELECT agent_id FROM curie.agent_channels WHERE kind = 'github' AND address = :repo",
        {"repo": REPO},
    )
    assert sorted(str(row["agent_id"]) for row in bound) == sorted([full, fast])

    number, row = _admit(client, forge, REPO)

    assert str(row["agent_id"]) == full
    assert row["status"] == "waiting"
    assert _rows("SELECT agent_id FROM curie.work_items") == [{"agent_id": uuid.UUID(full)}]
    assert number > 0


def test_multi_repo_bound_repositories_lists_every_binding_address(
    forge_app: tuple[TestClient, _Forge],
) -> None:
    """Fails on the old join, which lists only an address equal to its agent's
    repo_full_name, and here there is none."""

    client, _forge = forge_app
    _multi_repo_agent(client)

    async def go() -> list[str]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await bound_repositories(session)
        finally:
            await engine.dispose()

    listed = asyncio.run(go())

    assert sorted(listed) == sorted(_BOUND)
    assert AGENT_REPO not in listed


def _drive(intake_pass: str, forge: _Forge) -> None:
    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        client = httpx.AsyncClient(transport=httpx.MockTransport(forge.handle))
        try:
            if intake_pass == "poll":
                await poll_once(sessions, get_settings(), client)
            else:
                await reconcile_missed_labels(
                    sessions, get_settings(), client, now=datetime.now(UTC)
                )
        finally:
            await client.aclose()
            await engine.dispose()

    asyncio.run(go())


@pytest.mark.parametrize("intake_pass", ["poll", "reconcile"])
@pytest.mark.parametrize(
    ("allowlist", "walked"),
    [
        (f'["{REPO}"]', (REPO,)),
        ('["acme-corp/*"]', _BOUND),
    ],
    ids=["one_allowed", "owner_wildcard"],
)
def test_multi_repo_poller_and_reconciler_skip_a_bound_repository_outside_the_allowlist(
    forge_app: tuple[TestClient, _Forge],
    monkeypatch: pytest.MonkeyPatch,
    intake_pass: str,
    allowlist: str,
    walked: tuple[str, ...],
) -> None:
    """The poller and the missed-label backstop walk every bound repository the
    allowlist admits and never ask GitHub about one it does not.

    The recorded GitHub request paths are the observable for the skip; the
    admitted WorkItems are the observable for the walk. Fails on the old join:
    no binding names the agent's repo_full_name, so nothing was walked.
    """

    client, forge = forge_app
    _multi_repo_agent(client)
    polled = {repo: PollGitHub() for repo in _BOUND}
    forge.fakes = {repo: fake for repo, fake in polled.items()}
    # Older than the backstop's grace period, so both passes admit it.
    planted = {repo: fake.plant_issue(age=timedelta(minutes=30)) for repo, fake in polled.items()}
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", allowlist)
    get_settings.cache_clear()
    forge.seen.clear()

    _drive(intake_pass, forge)

    for repo in _BOUND:
        rows = _work(repo, planted[repo])
        if repo in walked:
            assert f"/repos/{repo}/issues" in forge.asked_about(repo), forge.seen
            assert len(rows) == 1, (repo, rows)
            assert rows[0]["repository_path"] == repo
            assert rows[0]["status"] == "waiting"
        else:
            assert forge.asked_about(repo) == [], forge.seen
            assert rows == []
