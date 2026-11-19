"""A saved run handoff reaches the marked GitHub and Jira status comments."""

from __future__ import annotations

import sys
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.factory_comment_text import marker_for, status_marker
from curie_api.factory_run_handoff import RunHandoff
from forge_fakes.github_comments import _rows, admitted, comments  # noqa: F401 (fixtures)
from forge_fakes.jira import JiraFake, flatten, jira_tracker
from forge_fakes.jira import fake_client as jira_client
from test_factory_merge_notice import _route_through_ports
from test_factory_progress import progress_token
from test_factory_status_comment import _admit, _marked
from test_factory_terminus import (  # noqa: F401 (fixtures)
    _finish_unpublished,
    _notices,
    _reconcile,
    _request,
    _start_running,
)

pytestmark = pytest.mark.usefixtures("clean_db")


@pytest.mark.parametrize("tracker", ["github", "jira"])
def test_authenticated_handoff_is_saved_and_delivered_in_one_marked_comment(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    tracker: str,
) -> None:
    client, github, sink = admitted
    number = 57680 if tracker == "github" else 57681
    jira: JiraFake | None = None
    issue = None
    if tracker == "jira":
        jira = JiraFake()
        ticket = jira.seed_issue("Anonymous fixture")
        issue = jira.ref(ticket)

        @asynccontextmanager
        async def open_jira() -> AsyncIterator[Any]:
            assert jira is not None
            async with jira_client(jira) as http:
                yield jira_tracker(jira, client=http)

        _route_through_ports(monkeypatch, open_jira, issue)

    request_id = _admit(client, github, sink, number)
    _reconcile()  # Create the marked live comment before the handoff arrives.
    _start_running(client, request_id)
    payload = {
        "next_owner": "human",
        "next_action": "Choose the parser and then retry",
        "human_decision": "Which parser should be used?",
        "findings": [
            {
                "summary": "Parser choice remains open",
                "owner": "human",
                "next_step": "Choose one parser",
                "source": "agent_reported",
            }
        ],
        "verification": [
            {
                "check": "Parser handles the fixture",
                "command": "uv run pytest tests/parser_fixture.py -q",
                "state": "not_run",
                "source": "agent_reported",
                "acceptance_criteria": ["AC1"],
            }
        ],
    }
    response = client.post(
        f"/v1/work-item-progress/{request_id}/handoff",
        headers={"X-API-Key": progress_token(request_id)},
        json=payload,
    )
    assert response.status_code == 201, response.text
    assert response.json() == {"recorded": True, "request_id": str(request_id)}
    (stored,) = _rows(
        "SELECT run_handoff FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )
    assert stored["run_handoff"] == RunHandoff.model_validate(payload).model_dump(mode="json")

    detail = "Could not complete: private unstructured final that must stay off the tracker."
    finished = _finish_unpublished(client, request_id, cause="no_pull_request", detail=detail)
    assert finished.status_code == 200, finished.text
    _reconcile()

    if jira is None:
        (comment,) = _marked(sink, request_id)
        body = comment["body"]
        assert sink.posts == 1
    else:
        assert issue is not None
        (comment,) = jira.own_comments(issue)
        body = flatten(comment.body)
        assert len(jira.requests_of("comment_create")) == 1
        assert jira.requests_of("comment_update")
    if jira is None:
        assert marker_for(request_id) in comment["body"]
    else:
        assert comment.properties["curie.factory"]["marker"] == status_marker(request_id)
    assert "No pull request was published" in body
    assert "Next owner:" in body and "human (agent reported)" in body
    assert "Choose the parser and then retry" in body
    assert "Which parser should be used?" in body
    assert "Parser choice remains open" in body
    assert "AC1" in body and "Parser handles the fixture" in body
    assert "not run" in body and "uv run pytest tests/parser" in body
    assert "Cause: no_pull_request" in body
    assert "private unstructured final" not in body
    assert _notices(request_id)[0]["finalized_at"] is not None

    late = client.post(
        f"/v1/work-item-progress/{request_id}/handoff",
        headers={"X-API-Key": progress_token(request_id)},
        json={"next_owner": "agent", "next_action": "replace the terminal summary"},
    )
    assert late.status_code == 409, late.text
    assert late.json()["code"] == "no_active_request"
    (unchanged,) = _rows(
        "SELECT run_handoff FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )
    assert unchanged["run_handoff"] == stored["run_handoff"]

    # A repeated pass must leave the same remote comment in place.
    _reconcile()
    if jira is None:
        assert len(_marked(sink, request_id)) == 1
        assert sink.posts == 1
    else:
        assert len(jira.own_comments(issue)) == 1
        assert len(jira.requests_of("comment_create")) == 1


def test_handoff_refuses_a_token_for_another_request(admitted: Any) -> None:  # noqa: F811
    client, github, _sink = admitted
    number = 57682
    request_id = _admit(client, github, _sink, number)
    _start_running(client, request_id)
    response = client.post(
        f"/v1/work-item-progress/{request_id}/handoff",
        headers={"X-API-Key": progress_token(uuid.uuid4())},
        json={"next_owner": "agent", "next_action": "retry"},
    )
    assert response.status_code == 401, response.text
    (stored,) = _rows(
        "SELECT run_handoff FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
        {"id": request_id},
    )
    assert stored["run_handoff"] is None
