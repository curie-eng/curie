"""A merged pull request promotes its newest rule candidate; a closed one discards all
of them (#5796, ADR 0287 decision 5).

Candidates are stored through ``POST /work-items/rule-proposal`` by revisions
admitted from signed review feedback (the fixtures of
test_memory_rule_proposal). The terminal lineage state is written by its two
production writers: a lineage reconciler pass (``mark_publication_lineage_terminal``)
and the terminal branch of ``advance_publication_lineage``, the latter on the
work item outcome suite's stack, where the lineage is opened by a real
publication.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pytest
from curie_api.config import get_settings
from curie_api.crud import lineages as crud_lineages
from curie_api.forges.types import PullRequestRef, PullRequestState, ReplyTarget, RepositoryRef
from curie_api.memory_rule_lifecycle import settle_lineage_rule_candidates
from curie_api.models import ThreadPublicationLineage
from curie_api.publication_authority import VerifiedPublicationIdentity
from curie_api.schemas.publications import PublicationLineageAdvance
from curie_api.workitem_dispatch import Admission
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

sys.path.insert(0, str(Path(__file__).resolve().parent))

import test_workitem_outcomes as outcomes  # noqa: E402
from test_memory_rule_proposal import (  # noqa: E402
    REASON,
    ReviewGitHubAPI,
    Run,
    _complete,
    agent_of,
    finish_running,
    propose,
    pull_request,
    reconcile,
    revise,
    rules,  # noqa: F401  (fixture)
    stored_events,
    stored_rules,
    switch,
)
from test_workitem_outcomes import stack  # noqa: E402, F401  (fixture)

RULE_A = "Use the shared parse_timestamp helper for every time flag."
RULE_B = "Every time flag goes through parse_timestamp in cli/src/time.rs."
RULE_C = "Name test helpers after the behavior they set up."


def _propose(client: TestClient, run: Run, rule: str) -> None:
    response = propose(client, run.capability, {"rule": rule, "reason": REASON})
    assert response.status_code == 200, response.text


def _by_text() -> dict[str, dict[str, Any]]:
    return {rule["text"]: rule for rule in stored_rules()}


def _events_of(rule_id: uuid.UUID) -> list[tuple[str, str | None, str, str, str]]:
    return [
        (e["event"], e["from_state"], e["to_state"], e["actor_kind"], e["actor"])
        for e in stored_events()
        if e["rule_id"] == rule_id
    ]


def _two_candidates(client: TestClient, api: ReviewGitHubAPI) -> tuple[Run, Run]:
    """Two revisions of one pull request, each storing one candidate."""

    number, first, pr, lineage_id = pull_request(client, api)
    _complete(client, first["id"])
    switch(client, agent_of(first["work_item_id"]), True)
    older = revise(client, api, number=number, pr=pr, lineage_id=lineage_id)
    _propose(client, older, RULE_A)
    finish_running(older.request_id)
    newer = revise(
        client, api, number=number, pr=pr, lineage_id=lineage_id, event="pull_request_review"
    )
    _propose(client, newer, RULE_B)
    assert newer.work_item_id == older.work_item_id
    rows = _by_text()
    assert [rows[RULE_A]["state"], rows[RULE_B]["state"]] == ["candidate", "candidate"]
    assert rows[RULE_A]["source_lineage_id"] == rows[RULE_B]["source_lineage_id"] == lineage_id
    return older, newer


def _second_terminal_pass(lineage_id: uuid.UUID, state: str) -> None:
    """A terminal lineage seen again: a reconciler pass (which no longer
    selects it) and the settlement itself run once more over it."""

    assert reconcile({}).read == 0

    async def again() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                lineage = await session.get(ThreadPublicationLineage, lineage_id)
                assert lineage is not None and lineage.status == state
                await settle_lineage_rule_candidates(session, lineage_id, state)
                await session.commit()
        finally:
            await engine.dispose()

    asyncio.run(again())


def test_merge_promotes_the_newest_candidate_and_supersedes_the_older(
    rules: tuple[TestClient, ReviewGitHubAPI],
) -> None:
    client, api = rules
    older, newer = _two_candidates(client, api)

    result = reconcile({newer.pr: PullRequestState.MERGED})

    assert result.merged == 1
    rows = _by_text()
    promoted, superseded = rows[RULE_B], rows[RULE_A]
    assert (promoted["state"], promoted["number"], promoted["version"]) == ("proposed", 1, 2)
    assert (superseded["state"], superseded["number"], superseded["version"]) == (
        "superseded",
        None,
        2,
    )
    assert promoted["updated_at"] >= promoted["proposed_at"]
    agent = str(newer.agent_id)
    assert _events_of(promoted["id"]) == [
        ("propose", None, "candidate", "agent", agent),
        ("promote", "candidate", "proposed", "platform", "platform"),
    ]
    assert _events_of(superseded["id"]) == [
        ("propose", None, "candidate", "agent", agent),
        ("supersede", "candidate", "superseded", "platform", "platform"),
    ]

    # A second terminal pass changes no row.
    before = (stored_rules(), stored_events())
    _second_terminal_pass(newer.lineage_id, "merged")
    assert (stored_rules(), stored_events()) == before

    # A rule promoted from a second pull request in the same repository takes
    # the next number in that scope; the first pull request's rows remain.
    number, first, pr, lineage_id = pull_request(client, api)
    _complete(client, first["id"])
    other = revise(client, api, number=number, pr=pr, lineage_id=lineage_id)
    _propose(client, other, RULE_C)
    assert reconcile({pr: PullRequestState.MERGED}).merged == 1
    rows = _by_text()
    assert rows[RULE_C]["scope_key"] == promoted["scope_key"]
    assert (rows[RULE_C]["state"], rows[RULE_C]["number"]) == ("proposed", 2)
    assert rows[RULE_A] == superseded and rows[RULE_B] == promoted
    assert len(stored_events()) == 6


def test_close_discards_every_candidate(rules: tuple[TestClient, ReviewGitHubAPI]) -> None:
    client, api = rules
    older, newer = _two_candidates(client, api)

    result = reconcile({newer.pr: PullRequestState.CLOSED})

    assert result.closed == 1
    rows = _by_text()
    for rule_text in (RULE_A, RULE_B):
        assert (
            rows[rule_text]["state"],
            rows[rule_text]["number"],
            rows[rule_text]["version"],
        ) == (
            "discarded",
            None,
            2,
        )
        assert _events_of(rows[rule_text]["id"]) == [
            ("propose", None, "candidate", "agent", str(older.agent_id)),
            ("discard", "candidate", "discarded", "platform", "platform"),
        ]

    before = (stored_rules(), stored_events())
    _second_terminal_pass(newer.lineage_id, "closed")
    assert (stored_rules(), stored_events()) == before


# The terminal branch of advance_publication_lineage --------------------------------

REVIEWER = "github:4242:reviewer"


def _review_facts(agent_id: str, comment_id: int) -> Admission:
    """The facts ``admit_parsed_feedback`` builds for a pull request comment."""

    facts = outcomes._facts(agent_id, requester=REVIEWER)
    pull = PullRequestRef(
        RepositoryRef("github", "github.com", "101", outcomes.REPO), str(outcomes.PR_NUMBER)
    )
    return Admission(
        **{**vars(facts), "objective": f"{outcomes.PR_URL}#issuecomment-{comment_id}\nRevise"},
        reply_target=ReplyTarget.on_pull_request(pull),
        reply_url=f"{outcomes.PR_URL}#issuecomment-{comment_id}",
    )


def _promote_next(work_item_id: uuid.UUID) -> None:
    async def promote(session: AsyncSession) -> None:
        outcome = await outcomes.workitems.admit_next_revision(
            session,
            work_item_id=work_item_id,
            wait_deadline=datetime.now(UTC) + timedelta(minutes=10),
        )
        assert isinstance(outcome, outcomes.workitems.WorkItemOutcome), outcome
        assert outcome.request is not None and outcome.request.status == "waiting"

    outcomes.with_session(promote)


def _capability(stack: TestClient, request_id: uuid.UUID) -> str:
    minted = stack.post(
        "/v1/internal/work-items/issue-read/context",
        headers=outcomes.WORKER_HEADERS,
        json={"execution_request_id": str(request_id), "publication_recovery": False},
    )
    assert minted.status_code == 200, minted.text
    return str(minted.json()["capability"])


def _proposed_by_capability(stack: TestClient, request_id: uuid.UUID, rule: str) -> None:
    response = stack.post(
        "/work-items/rule-proposal",
        headers={"X-Curie-Issue-Read": _capability(stack, request_id)},
        json={"rule": rule, "reason": REASON},
    )
    assert response.status_code == 200, response.text


def _advance_terminal(
    stack: TestClient, publication_id: str, state: Literal["merged", "closed"]
) -> None:
    """The worker's terminal outcome for a revision publication: the pull
    request was found merged or closed. Called the way ``_open_pr`` calls the
    service: the route would verify the identity against a live GitHub App,
    which this PAT-only stack does not have."""

    lease_owner = "rule-lifecycle-worker"
    (leased,) = _rows_of(
        "UPDATE curie.publications SET lease_owner = :owner, "
        "lease_expires_at = now() + interval '1 minute', version = version + 1 "
        "WHERE id = :id RETURNING version, lineage_id",
        {"id": uuid.UUID(publication_id), "owner": lease_owner},
    )
    (lineage,) = _rows_of(
        "SELECT version, head_sha FROM curie.thread_publication_lineages WHERE id = :id",
        {"id": leased["lineage_id"]},
    )

    async def advance() -> None:
        async with stack.app.state.sessionmaker() as session:
            advanced = await crud_lineages.advance_publication_lineage(
                session,
                uuid.UUID(publication_id),
                PublicationLineageAdvance(
                    expected_version=lineage["version"],
                    expected_head_sha=lineage["head_sha"],
                    expected_publication_version=leased["version"],
                    lease_owner=lease_owner,
                    state=state,
                    pr_number=outcomes.PR_NUMBER,
                    pr_url=outcomes.PR_URL,
                    head_sha=lineage["head_sha"],
                    metadata_updated_at=None,
                ),
                identity=VerifiedPublicationIdentity(
                    host="github.com",
                    repository_id=101,
                    installation_id=202,
                    pr_node_id="PR_example_123",
                    base_ref="main",
                ),
                github_html_base="https://github.com",
                first_parent_proof=None,
            )
            assert advanced.status == state

    stack.portal.call(advance)


def _rows_of(statement: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    async def body(session: AsyncSession) -> list[dict[str, Any]]:
        result = await session.execute(text(statement), params)
        rows = [dict(row) for row in result.mappings()]
        await session.commit()
        return rows

    return outcomes.with_session(body)


@pytest.mark.parametrize("state", ["merged", "closed"])
def test_a_terminal_publication_advance_settles_the_lineage_candidates(
    stack: TestClient, auth_headers: dict[str, str], state: Literal["merged", "closed"]
) -> None:
    agent = outcomes._agent(stack, auth_headers)
    switched = stack.patch(
        f"/agents/{agent['agent_id']}",
        headers=auth_headers,
        json={"memory_rule_proposals": True},
    )
    assert switched.status_code == 200, switched.text
    active = outcomes._completed(stack, agent)
    publication = outcomes._publish(stack, agent["deployment_id"])
    outcomes._resolve(stack, auth_headers, publication["approval_id"])
    outcomes._open_pr(stack, publication["id"])

    # Two revisions from review feedback on the open pull request, each
    # admitted while the previous request runs, as feedback is.
    older = outcomes._admit_revision(_review_facts(agent["agent_id"], 1))
    outcomes._complete(active)
    _promote_next(active.work_item_id)
    outcomes._start(stack, older.request_id)
    _proposed_by_capability(stack, older.request_id, RULE_A)
    newer = outcomes._admit_revision(_review_facts(agent["agent_id"], 2))
    # The older revision ends without publishing. `finish` completes a request
    # only once it published (or with a no-change reply this PAT-only stack
    # cannot link), so its end is written the way the review feedback suite
    # writes it; settlement does not read the request's status.
    outcomes._execute(
        "UPDATE curie.execution_requests SET status = 'completed', "
        "terminal_at = clock_timestamp(), terminal_cause = 'completed', "
        "version = version + 1 WHERE id = :id AND status = 'running'",
        {"id": older.request_id},
    )
    _promote_next(active.work_item_id)
    outcomes._start(stack, newer.request_id)
    _proposed_by_capability(stack, newer.request_id, RULE_B)
    assert [rule["state"] for rule in stored_rules()] == ["candidate", "candidate"]

    # The newer revision publishes and finds its pull request terminal.
    outcomes._execute(
        "UPDATE curie.publications SET outcome_history_ready_at = now() WHERE id = :id",
        {"id": uuid.UUID(publication["id"])},
    )
    revision = outcomes._publish(stack, agent["deployment_id"], base_sha=outcomes.HEAD_SHA)
    assert revision["lineage_id"] == publication["lineage_id"]
    outcomes._resolve(stack, auth_headers, revision["approval_id"])
    _advance_terminal(stack, revision["id"], state)

    rows = _by_text()
    if state == "merged":
        assert (rows[RULE_B]["state"], rows[RULE_B]["number"]) == ("proposed", 1)
        assert (rows[RULE_A]["state"], rows[RULE_A]["number"]) == ("superseded", None)
        assert _events_of(rows[RULE_B]["id"])[-1][0] == "promote"
        assert _events_of(rows[RULE_A]["id"])[-1][0] == "supersede"
    else:
        assert [rows[RULE_A]["state"], rows[RULE_B]["state"]] == ["discarded", "discarded"]
        assert _events_of(rows[RULE_A]["id"])[-1][0] == "discard"
        assert _events_of(rows[RULE_B]["id"])[-1][0] == "discard"
    for rule_text in (RULE_A, RULE_B):
        assert rows[rule_text]["proposed_by"] == REVIEWER
        assert rows[rule_text]["source_lineage_id"] == uuid.UUID(publication["lineage_id"])
        assert len(_events_of(rows[rule_text]["id"])) == 2
