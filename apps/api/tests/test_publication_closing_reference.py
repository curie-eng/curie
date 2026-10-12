"""Every factory publication body carries its tracker's closing reference
(#4358 plan D7, AC9 "a GitHub pull request opens whose body carries the closing
reference").

`POST /v1/internal/publications` for a WorkItem request asks the WorkItem's
tracker for `closing_reference(issue, repository)` and appends it when the body
lacks it, beside the verification statements. A GitHub body the bundle already
wrote with `Closes #<n>` is stored byte for byte; one that omitted it gains it
(the named delta). A Jira WorkItem's body gains `Jira: <KEY> <url>`, the key
read from the Jira fake by issue id. A tracker that cannot answer refuses the
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
from curie_api.forges.registry import forges_registry
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


def _passed(client: TestClient, request_id: uuid.UUID) -> None:
    # A passed in-sandbox check: no verification statement is appended, so the
    # closing reference is the only possible change to the body.
    recorded = _record_factory_verification(client, request_id, outcome="passed")
    assert recorded.status_code == 201, recorded.text


# --- GitHub -----------------------------------------------------------------------


def test_a_github_body_that_already_closes_the_issue_is_stored_byte_for_byte(
    _factory_publication_case: Case,
) -> None:
    client, request_id, payload = _factory_publication_case
    _passed(client, request_id)
    body = f"Closes #{_issue_number(payload)}\n\nThe change updates the factory route.\n"
    payload = {**payload, "body": body}

    created = _post_factory_publication(client, payload)

    assert created.status_code == 201, created.text
    assert created.json()["body"] == body
    assert _factory_publication_body(created.json()["id"]) == body


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
    assert stored == f"{payload['body']}\n\n{reference}"
    assert stored.count(reference) == 1


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


def test_a_jira_body_gains_the_issue_key_and_url(
    jira_case: tuple[TestClient, uuid.UUID, dict[str, Any], jira_fake.JiraFake, str],
) -> None:
    client, request_id, payload, fake, key = jira_case
    _passed(client, request_id)

    created = _post_factory_publication(client, payload)

    assert created.status_code == 201, created.text
    reference = f"Jira: {key} {jira_fake.SITE}/browse/{key}"
    stored = _factory_publication_body(created.json()["id"])
    assert stored == f"{payload['body']}\n\n{reference}"
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
    assert stored.endswith(f"\n\nJira: WEB-41 {jira_fake.SITE}/browse/WEB-41")
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
        f"\n\nJira: {key} {jira_fake.SITE}/browse/{key}"
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
            f"Jira: {key} {jira_fake.SITE}/browse/{key}"
        )
