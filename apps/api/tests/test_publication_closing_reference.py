"""Every factory publication body carries its tracker's closing reference
(#4358 plan D7, AC9 "a GitHub pull request opens whose body carries the closing
reference").

`POST /v1/internal/publications` for a WorkItem request asks the WorkItem's
tracker for `closing_reference(issue, repository)` and appends it when the body
lacks it, beside the verification statements. A GitHub body the bundle already
wrote with `Closes #<n>` keeps its text; one that omitted it gains it (the named
delta). Every such body then ends with the canonical marker line naming the
issue by its immutable identity (`factory_rediscovery.issue_marker`, #4629).
A Jira WorkItem's body gains `Jira: <KEY> <url>`, the key read from the Jira
fake by issue id. A tracker that cannot answer refuses the
publication 503 retryable, never storing a body without the reference.

The factory request is admitted through the real GitHub label webhook (the
`admitted` fixture); the Jira case then records that WorkItem on a Jira issue
of the fixture site, whose tracker the forge config binds to the same GitHub
repository. Placeholder identifiers only.
"""

# ruff: noqa: F811  (fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from curie_api.config import get_settings
from curie_api.factory_comment_text import github_ask_line
from curie_api.factory_rediscovery import issue_marker
from curie_api.forges.registry import forges_registry
from curie_api.forges.types import TrackerIssueRef
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

from forge_fakes import jira as jira_fake  # noqa: E402
from forge_fakes.github import (  # noqa: E402
    forges_document,
    forges_env_json,
    github_repository_binding,
)
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: E402, F401
from test_publications import (  # noqa: E402, F401
    _factory_publication_body,
    _factory_publication_case,
    _post_factory_publication,
    _record_factory_verification,
    assert_single_ask_line,
)

JIRA_SUMMARY = "Widget parser drops the last field"

Case = tuple[TestClient, uuid.UUID, dict[str, Any]]


def _rewrite_stored_identity(statement: str, params: dict[str, Any]) -> None:
    """Rewrite a column the work item identity trigger guards, as a row an
    earlier admission wrote that way: triggers are off for this one transaction."""

    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text("SET LOCAL session_replication_role = replica"))
                await conn.execute(text(statement), params)
        finally:
            await engine.dispose()

    asyncio.run(run())


def _issue_number(payload: dict[str, Any]) -> int:
    return int(payload["reply_conversation_id"].removeprefix("issue-"))


def _marker(request_id: uuid.UUID) -> str:
    """The canonical marker line of the request's WorkItem issue."""

    (row,) = _rows(
        "SELECT w.tracker_kind, w.tracker_host, w.tracker_scope_id, w.tracker_issue_id "
        "FROM curie.work_items w JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "WHERE r.id = :request",
        {"request": request_id},
    )
    return issue_marker(
        TrackerIssueRef(
            row["tracker_kind"],
            row["tracker_host"],
            row["tracker_scope_id"],
            row["tracker_issue_id"],
        )
    )


def _passed(client: TestClient, request_id: uuid.UUID) -> None:
    # A passed in-sandbox check: no verification statement is appended, so the
    # closing reference is the only possible change to the body.
    recorded = _record_factory_verification(client, request_id, outcome="passed")
    assert recorded.status_code == 201, recorded.text


# --- GitHub -----------------------------------------------------------------------


def test_a_github_body_that_already_closes_the_issue_keeps_its_text(
    _factory_publication_case: Case,
) -> None:
    client, request_id, payload = _factory_publication_case
    _passed(client, request_id)
    body = f"Closes #{_issue_number(payload)}\n\nThe change updates the factory route.\n"
    payload = {**payload, "body": body}

    created = _post_factory_publication(client, payload)

    assert created.status_code == 201, created.text
    marked = (
        f"{body.rstrip()}\n\n{_marker(request_id)}\n\n{github_ask_line('curie', 'pull_request')}"
    )
    assert created.json()["body"] == marked
    assert _factory_publication_body(created.json()["id"]) == marked
    assert not assert_single_ask_line(marked, f"{body.rstrip()}\n\n{_marker(request_id)}")


def test_a_github_body_with_the_previous_ask_wording_ends_with_one_current_line(
    _factory_publication_case: Case,
) -> None:
    client, request_id, payload = _factory_publication_case
    _passed(client, request_id)
    old = "Ask the factory: mention @curie on this pull request."
    text = f"Closes #{_issue_number(payload)}\n\nThe change updates the factory route."
    payload = {**payload, "body": f"{text}\n\n{old}\n"}

    created = _post_factory_publication(client, payload)

    assert created.status_code == 201, created.text
    stored = _factory_publication_body(created.json()["id"])
    assert old not in stored
    assert not assert_single_ask_line(stored, f"{text}\n\n{_marker(request_id)}")


def test_a_github_body_without_the_closing_reference_gains_it_once(
    _factory_publication_case: Case,
) -> None:
    client, request_id, payload = _factory_publication_case
    _passed(client, request_id)
    reference = f"Closes #{_issue_number(payload)}"
    assert reference not in payload["body"]

    created = _post_factory_publication(client, payload)

    assert created.status_code == 201, created.text
    stored = _factory_publication_body(created.json()["id"])
    assert stored == (
        f"{payload['body']}\n\n{reference}\n\n{_marker(request_id)}\n\n"
        f"{github_ask_line('curie', 'pull_request')}"
    )
    assert stored.count(reference) == 1
    assert not assert_single_ask_line(
        stored, f"{payload['body']}\n\n{reference}\n\n{_marker(request_id)}"
    )


# --- Jira -------------------------------------------------------------------------


class _ByHost(httpx.AsyncBaseTransport):
    """The Jira site to the Jira fake, every other host to the case's GitHub stand-in."""

    def __init__(self, jira: jira_fake.JiraFake, default: httpx.AsyncBaseTransport) -> None:
        self._jira = httpx.ASGITransport(app=jira.app)
        self._default = default

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.url.host == jira_fake.SITE_HOST:
            return await self._jira.handle_async_request(request)
        return await self._default.handle_async_request(request)


@pytest.fixture
def jira_case(
    _factory_publication_case: Case,
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> Iterator[tuple[TestClient, uuid.UUID, dict[str, Any], jira_fake.JiraFake, str]]:
    """The case's WorkItem recorded on a Jira issue of the fixture site, whose
    binding names the same GitHub repository."""

    client, request_id, payload = _factory_publication_case
    _client, github, _comments = admitted
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue(JIRA_SUMMARY, "Parsing a three-field line yields two fields.")
    document = forges_document(api_url=get_settings().github_api_url)
    jira = jira_fake.jira_document()
    document["trackers"].extend(jira["trackers"])
    document["bindings"].extend(jira["bindings"])
    monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, tmp_path / "forges"))
    get_settings.cache_clear()
    client.app.state.forges = forges_registry(get_settings())
    _rewrite_stored_identity(
        "UPDATE curie.work_items SET tracker_kind = 'jira_cloud', tracker_host = :site, "
        "tracker_scope_id = :cloud, tracker_issue_id = :issue, tracker_display_key = :key "
        "WHERE id = (SELECT work_item_id FROM curie.execution_requests WHERE id = :request)",
        {
            "site": jira_fake.SITE_HOST,
            "cloud": jira_fake.CLOUD_ID,
            "issue": str(issue.id),
            "key": issue.key,
            "request": request_id,
        },
    )
    original = client.app.state.http_client
    routed = httpx.AsyncClient(transport=_ByHost(fake, httpx.MockTransport(github.handle)))
    client.app.state.http_client = routed
    try:
        yield client, request_id, payload, fake, issue.key
    finally:
        client.app.state.http_client = original
        client.portal.call(routed.aclose)
        get_settings.cache_clear()


def _install_gitlab(client: TestClient, monkeypatch: pytest.MonkeyPatch, key_dir: Path) -> None:
    """Add the GitLab tracker beside the install's GitHub entries."""

    from forge_fakes import gitlab as gitlab_fake

    document = forges_document(api_url=get_settings().github_api_url)
    half = gitlab_fake.forges_document()
    for section in ("code_hosts", "trackers", "bindings"):
        document[section] = [*document[section], *half[section]]
    monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, key_dir))
    get_settings.cache_clear()
    client.app.state.forges = forges_registry(get_settings())


def test_a_gitlab_publication_body_omits_the_ask_line(
    _factory_publication_case: Case,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from forge_fakes import gitlab as gitlab_fake

    client, request_id, payload = _factory_publication_case
    _install_gitlab(client, monkeypatch, tmp_path / "forges")
    _rewrite_stored_identity(
        "UPDATE curie.work_items SET tracker_kind = 'gitlab', tracker_host = :host, "
        "tracker_scope_id = :scope WHERE id = ("
        "SELECT work_item_id FROM curie.execution_requests WHERE id = :request)",
        {
            "host": gitlab_fake.HOST,
            "scope": str(gitlab_fake.PROJECT_ID),
            "request": request_id,
        },
    )
    _passed(client, request_id)

    created = _post_factory_publication(client, payload)

    assert created.status_code == 201, created.text
    stored = _factory_publication_body(created.json()["id"])
    assert stored == (
        f"{payload['body']}\n\nCloses #{_issue_number(payload)}\n\n{_marker(request_id)}"
    )
    assert "Ask the factory" not in stored


def test_a_bitbucket_publication_body_omits_the_ask_line(
    _factory_publication_case: Case,
    admitted: Any,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A Jira tracker whose binding publishes onto a Bitbucket repository.

    ``create_publication`` builds the body from that tracker. The publication
    target is the Bitbucket repository, and the body has no GitHub ask line.
    """

    from forge_fakes import config_samples as samples

    client, request_id, payload = _factory_publication_case
    _client, github, _comments = admitted
    fake = jira_fake.JiraFake()
    issue = fake.seed_issue(JIRA_SUMMARY, "Parsing a three-field line yields two fields.")
    document = jira_fake.jira_document()
    document["code_hosts"] = [
        {
            **samples.bitbucket_code_host(),
            "host": "bitbucket.org",
            "api_url": "https://api.bitbucket.org/2.0",
            "credential": samples.static_token(),
        }
    ]
    project_id = "{6f1c0000-0000-4000-8000-000000000003}"
    path = "acme/web"
    binding = document["bindings"][0]
    binding["repos"] = [
        {
            "alias": "web",
            "code_host": "bitbucket",
            "project_id": project_id,
            "path": path,
        }
    ]
    binding["default_repo"] = "web"
    monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, tmp_path / "forges"))
    get_settings.cache_clear()
    client.app.state.forges = forges_registry(get_settings())
    _rewrite_stored_identity(
        "UPDATE curie.work_items SET tracker_kind = 'jira_cloud', tracker_host = :site, "
        "tracker_scope_id = :cloud, tracker_issue_id = :issue, tracker_display_key = :key, "
        "code_host_kind = 'bitbucket_cloud', code_host_host = 'bitbucket.org', "
        "repository_project_id = :project, repository_path = :path, "
        "code_host_installation_id = NULL "
        "WHERE id = (SELECT work_item_id FROM curie.execution_requests WHERE id = :request)",
        {
            "site": jira_fake.SITE_HOST,
            "cloud": jira_fake.CLOUD_ID,
            "issue": str(issue.id),
            "key": issue.key,
            "project": project_id,
            "path": path,
            "request": request_id,
        },
    )
    original = client.app.state.http_client
    routed = httpx.AsyncClient(transport=_ByHost(fake, httpx.MockTransport(github.handle)))
    client.app.state.http_client = routed
    payload = {**payload, "repo_full_name": path}
    try:
        _passed(client, request_id)
        created = _post_factory_publication(client, payload)
    finally:
        client.app.state.http_client = original
        client.portal.call(routed.aclose)
        get_settings.cache_clear()

    assert created.status_code == 201, created.text
    assert created.json()["repo_full_name"] == path
    reference = f"Jira: {issue.key} {jira_fake.SITE}/browse/{issue.key}"
    stored = _factory_publication_body(created.json()["id"])
    assert stored == f"{payload['body']}\n\n{reference}\n\n{_marker(request_id)}"
    assert "Ask the factory" not in stored
    assert "Closes" not in stored
    (row,) = _rows(
        "SELECT code_host_kind, code_host_host, repository_project_id, repository_path "
        "FROM curie.work_items w JOIN curie.execution_requests r ON r.work_item_id = w.id "
        "WHERE r.id = :request",
        {"request": request_id},
    )
    assert (
        row["code_host_kind"],
        row["code_host_host"],
        row["repository_project_id"],
        row["repository_path"],
    ) == ("bitbucket_cloud", "bitbucket.org", project_id, path)


def test_a_jira_body_gains_the_issue_key_and_url(
    jira_case: tuple[TestClient, uuid.UUID, dict[str, Any], jira_fake.JiraFake, str],
) -> None:
    client, request_id, payload, fake, key = jira_case
    _passed(client, request_id)

    created = _post_factory_publication(client, payload)

    assert created.status_code == 201, created.text
    reference = f"Jira: {key} {jira_fake.SITE}/browse/{key}"
    stored = _factory_publication_body(created.json()["id"])
    assert stored == f"{payload['body']}\n\n{reference}\n\n{_marker(request_id)}"
    assert "Ask the factory" not in stored
    # The key is read by issue id, with the bot's credential; nothing is written.
    reads = fake.requests_of("issue")
    assert reads and all(r.method == "GET" for r in reads)
    assert fake.writes == 0
    # No GitHub closing keyword: Jira has none.
    assert "Closes" not in stored


def test_a_moved_jira_issue_is_referenced_by_its_current_key(
    jira_case: tuple[TestClient, uuid.UUID, dict[str, Any], jira_fake.JiraFake, str],
) -> None:
    client, request_id, payload, fake, key = jira_case
    _passed(client, request_id)
    issue = next(iter(fake.issues.values()))
    fake.move_issue(issue.id, "WEB-41")

    created = _post_factory_publication(client, payload)

    assert created.status_code == 201, created.text
    stored = _factory_publication_body(created.json()["id"])
    assert stored.endswith(
        f"\n\nJira: WEB-41 {jira_fake.SITE}/browse/WEB-41\n\n{_marker(request_id)}"
    )
    assert key not in stored


def test_a_tracker_that_cannot_answer_refuses_the_publication_retryably(
    jira_case: tuple[TestClient, uuid.UUID, dict[str, Any], jira_fake.JiraFake, str],
) -> None:
    client, request_id, payload, fake, key = jira_case
    _passed(client, request_id)
    fake.fail_next("issue", status=503)

    refused = _post_factory_publication(client, payload)

    assert refused.status_code == 503, refused.text
    assert refused.json()["detail"]["code"] == "publication.closing_reference_unavailable"
    assert jira_fake.TOKEN not in refused.text
    assert (
        _rows(
            "SELECT id FROM curie.publications WHERE execution_request_id = :request",
            {"request": request_id},
        )
        == []
    )

    # The refusal is retryable: the same publication succeeds once Jira answers.
    retried = _post_factory_publication(client, payload)
    assert retried.status_code == 201, retried.text
    assert _factory_publication_body(retried.json()["id"]).endswith(
        f"\n\nJira: {key} {jira_fake.SITE}/browse/{key}\n\n{_marker(request_id)}"
    )


@pytest.mark.parametrize("gone", ["binding", "tracker"])
def test_a_work_item_whose_tracker_binding_is_gone_is_refused_not_published_bare(
    jira_case: tuple[TestClient, uuid.UUID, dict[str, Any], jira_fake.JiraFake, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    gone: str,
) -> None:
    """r1 finding 8: the reference cannot be named, so nothing is stored; the
    two Jira tests above are the liveness case with the binding configured."""

    client, request_id, payload, fake, _key = jira_case
    _passed(client, request_id)
    document = forges_document(api_url=get_settings().github_api_url)
    if gone == "binding":
        document["trackers"].extend(jira_fake.jira_document()["trackers"])
    monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, tmp_path / "unbound"))
    get_settings.cache_clear()
    client.app.state.forges = forges_registry(get_settings())

    refused = _post_factory_publication(client, payload)

    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"]["code"] == "publication.closing_reference_unconfigured"
    assert jira_fake.TOKEN not in refused.text
    assert fake.requests == []
    assert (
        _rows(
            "SELECT id FROM curie.publications WHERE execution_request_id = :request",
            {"request": request_id},
        )
        == []
    )


REQUIRED_CI = {"key": "Unit tests", "paths": ["unitconv"], "pending_key_prefix": None}


@pytest.mark.parametrize("required_on", ["jira", "github"])
def test_the_python_ci_policy_is_the_jira_binding_s_not_another_binding_s(
    jira_case: tuple[TestClient, uuid.UUID, dict[str, Any], jira_fake.JiraFake, str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    required_on: str,
) -> None:
    """r1 finding 4: a repository two bindings share. A GitHub binding listed
    first carries one CI policy and the Jira binding that chose the repository
    another; the Jira WorkItem's publication is judged by the Jira binding's."""

    client, request_id, payload, _fake, key = jira_case
    _passed(client, request_id)
    github_binding = github_repository_binding(
        ci={"required": REQUIRED_CI} if required_on == "github" else None
    )
    jira = jira_fake.jira_document()
    if required_on == "jira":
        jira["bindings"][0]["repos"][0]["ci"] = {"required": REQUIRED_CI}
    document = forges_document(api_url=get_settings().github_api_url, bindings=[github_binding])
    document["trackers"].extend(jira["trackers"])
    document["bindings"].extend(jira["bindings"])
    monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, tmp_path / "shared"))
    get_settings.cache_clear()
    client.app.state.forges = forges_registry(get_settings())
    payload = {**payload, "changed_paths": ["scripts/convert.py"]}

    answered = _post_factory_publication(client, payload)

    if required_on == "jira":
        assert answered.status_code == 409, answered.text
        assert answered.json()["detail"]["code"] == "publication.required_python_ci_unselected"
    else:
        # Liveness: the GitHub binding's policy does not reach the Jira WorkItem.
        assert answered.status_code == 201, answered.text
        assert _factory_publication_body(answered.json()["id"]).endswith(
            f"Jira: {key} {jira_fake.SITE}/browse/{key}\n\n{_marker(request_id)}"
        )


def test_deleted_jira_issue_names_the_issue_on_publication_create(
    jira_case: tuple[TestClient, uuid.UUID, dict[str, Any], jira_fake.JiraFake, str],
) -> None:
    client, request_id, payload, fake, key = jira_case
    [before] = _rows(
        "SELECT w.cancelled_at FROM curie.work_items w JOIN curie.execution_requests r "
        "ON r.work_item_id = w.id WHERE r.id = :id",
        {"id": request_id},
    )
    assert before["cancelled_at"] is None
    _passed(client, request_id)
    fake.delete_issue(next(issue.id for issue in fake.issues.values() if issue.key == key))

    refused = _post_factory_publication(client, payload)

    assert refused.status_code == 409, refused.text
    assert refused.json()["detail"] == {
        "code": "publication.issue_deleted",
        "message": "the tracker issue no longer exists",
    }
    assert (
        _rows(
            "SELECT id FROM curie.publications WHERE execution_request_id = :id", {"id": request_id}
        )
        == []
    )
    [cancelled] = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    assert cancelled == {"status": "cancellation_requested", "terminal_cause": "issue_deleted"}
    finish = client.post(
        f"/v1/internal/work-items/requests/{request_id}/finish",
        headers={"X-Curie-Worker-Token": get_settings().internal_worker_token},
        json={
            "runtime_epoch": payload["work_item_runtime_epoch"],
            "outcome": "failed",
            "cause": "approval_create_failed",
            "retry_safety": "uncertain",
            "detail": None,
        },
    )
    assert finish.status_code == 409, finish.text
    assert finish.json()["detail"]["code"] == "work_item_cancelled"
    assert _rows(
        "SELECT terminal_cause FROM curie.execution_requests WHERE id = :id", {"id": request_id}
    ) == [{"terminal_cause": "issue_deleted"}]
