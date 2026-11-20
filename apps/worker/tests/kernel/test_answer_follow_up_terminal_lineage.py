"""An answer's follow-up whose pull request merged before it started (#5786).

The follow-up an answer comment starts runs on the WorkItem's conversation.
Worker start reads that conversation's pull request lineage through the real
API before any sandbox starts (`kernel/claim.py`), the API refreshes it from
the code host and records the merge, and the worker refuses a turn on a
terminal pull request. The lineage is a database row; the request is admitted
through the real internal admission route and dispatched through the real
`WorkItemDispatchClient` and `ApprovalClient`. Only GitHub's answer to the
pull request read is faked, at the code host seam, in the shape
https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request maps to; the
answer receipt is written as the answer intake writes it.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from curie_api.factory_notices import ANSWER_PULL_REQUEST_ENDED_SENTENCE
from curie_api.factory_questions import ADMITTED
from curie_api.forges.types import PullRequest, PullRequestRef, PullRequestState
from curie_api.routers import publications
from sqlalchemy import text

from apps.worker.tests.start_wait_fixtures import (
    REPOSITORY,
    StartWaitCase,
    start_wait,
    start_wait_database,
)

__all__ = ["start_wait", "start_wait_database"]

PR_NUMBER = 7
PR_URL = f"https://github.com/{REPOSITORY}/pull/{PR_NUMBER}"
HEAD = "2" * 40


class _MergedOnGitHub:
    """The code host's read of the WorkItem's pull request: a person merged it."""

    def __init__(self, branch: str) -> None:
        self.branch = branch
        self.reads: list[PullRequestRef] = []

    async def read_pull_request(self, ref: PullRequestRef) -> PullRequest:
        self.reads.append(ref)
        return PullRequest(
            ref=ref,
            head_sha=HEAD,
            head_ref=self.branch,
            base_ref="main",
            state=PullRequestState.MERGED,
            url=PR_URL,
        )


async def _open_lineage(case: StartWaitCase) -> tuple[uuid.UUID, str]:
    """The WorkItem's pull request as the database last recorded it: open."""

    lineage_id = uuid.uuid4()
    branch = f"curie/answer-{lineage_id.hex[:12]}"
    async with case.sessions() as session:
        await session.execute(
            text(
                "INSERT INTO curie.thread_publication_lineages (id, agent_id, deployment_id, "
                "conversation_id, repo_full_name, base_sha, branch, pr_number, pr_url, "
                "head_sha, status) VALUES (:id, :agent, :deployment, :conversation, :repo, "
                ":base, :branch, :number, :url, :head, 'open')"
            ),
            {
                "id": lineage_id,
                "agent": case.agent_id,
                "deployment": case.deployment_id,
                "conversation": case.thread,
                "repo": REPOSITORY,
                "base": "1" * 40,
                "branch": branch,
                "number": PR_NUMBER,
                "url": PR_URL,
                "head": HEAD,
            },
        )
        await session.execute(
            text(
                "UPDATE curie.work_items SET publication_lineage_id = :lineage "
                "WHERE id = (SELECT work_item_id FROM curie.execution_requests WHERE id = :id)"
            ),
            {"lineage": lineage_id, "id": case.request_id},
        )
        await session.commit()
    return lineage_id, branch


async def _lineage_status(case: StartWaitCase, lineage_id: uuid.UUID) -> str:
    async with case.sessions() as session:
        status = await session.scalar(
            text("SELECT status FROM curie.thread_publication_lineages WHERE id = :id"),
            {"id": lineage_id},
        )
    return str(status)


async def _answer_started(case: StartWaitCase) -> None:
    """The receipt the answer intake writes for the comment that created the request."""

    async with case.sessions() as session:
        await session.execute(
            text(
                "INSERT INTO curie.factory_answer_receipts "
                "(work_item_id, comment_key, outcome, request_id) "
                "SELECT work_item_id, '9001', :outcome, id "
                "FROM curie.execution_requests WHERE id = :id"
            ),
            {"outcome": ADMITTED, "id": case.request_id},
        )
        await session.commit()


def _merged_on_github(
    case: StartWaitCase, monkeypatch: pytest.MonkeyPatch
) -> tuple[uuid.UUID, _MergedOnGitHub]:
    lineage_id, branch = case.run(_open_lineage(case))
    github = _MergedOnGitHub(branch)

    def code_host(*_args: Any, **_kwargs: Any) -> _MergedOnGitHub:
        return github

    monkeypatch.setattr(publications, "code_host_for", code_host)
    return lineage_id, github


def test_answer_follow_up_whose_pull_request_merged_ends_at_its_first_start(
    start_wait: StartWaitCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    case = start_wait
    lineage_id, github = _merged_on_github(case, monkeypatch)
    case.run(_answer_started(case))

    case.run(case.deliver())

    # Worker start read the lineage, and the API recorded the merge.
    assert [ref.number for ref in github.reads] == [str(PR_NUMBER)]
    assert case.run(_lineage_status(case, lineage_id)) == "merged"
    row = case.run(case.state())
    assert (row["status"], row["terminal_cause"]) == ("failed", "start_failed")
    assert row["last_start_failure_detail"] == ANSWER_PULL_REQUEST_ENDED_SENTENCE
    assert row["start_deferrals"] == 0 and row["started_at"] is None
    (payload,) = case.payloads
    assert payload["reason"].startswith("not_started:")
    assert case.h.runner.opened == []


def test_a_request_no_answer_started_still_defers_on_a_merged_pull_request(
    start_wait: StartWaitCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Liveness: only an answer's request ends; any other unstarted request
    keeps the start deferral it had."""

    case = start_wait
    lineage_id, _github = _merged_on_github(case, monkeypatch)

    case.run(case.deliver())

    assert case.run(_lineage_status(case, lineage_id)) == "merged"
    row = case.run(case.state())
    assert (row["status"], row["terminal_cause"]) == ("waiting", None)
    assert row["start_deferrals"] == 1
    assert case.h.runner.opened == []
