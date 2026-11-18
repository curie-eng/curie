"""The merge notice: a completed request's status comment and label once its
pull request merges (one-ticket ADR decision 15).

The real reconciler and lineage reconciler run. GitHub is the threaded GitHub
fake from `forge_fakes.github_comments`. The memory tracker and the Jira fake
take the port comment sync (`_sync_through_ports`) in place of the GitHub
install path, the way `test_factory_status_comment.py` drives its Jira leg;
each is the production tracker adapter writing to its own fake. The factory
never moves a ticket's workflow status: the Jira fake records every request
and none may touch a transitions path.
"""

from __future__ import annotations

import sys
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api import factory_notices
from curie_api.forges.memory import InMemoryTracker
from curie_api.forges.types import PullRequest, PullRequestRef, PullRequestState, ReplyTarget
from forge_fakes.github_comments import HEAD_A, admitted, comments  # noqa: F401  (fixtures)
from forge_fakes.jira import JiraFake, flatten, jira_tracker
from forge_fakes.jira import fake_client as jira_client
from test_factory_status_comment import (
    STATE_LABELS,
    _admit,
    _curie_labels,
    _finish_failed,
    _marked,
    _observe_lineage_state,
    _writes,
)
from test_factory_terminus import (  # noqa: F401  (fixtures)
    REPO,
    _attach_publication,
    _notices,
    _published_issue,
    _reconcile,
    _request,
    _start_running,
)

pytestmark = pytest.mark.usefixtures("clean_db")

PR = 77
PR_OPEN = "curie-factory:pr-open"
NEEDS_HUMAN = "curie-factory:needs-human"
LEGS = ["github", "memory", "jira"]


class _Ports:
    """The tracker and code host one port pass sees: the real tracker adapter
    for comments, labels and the one pull request link; the pull request is
    read back from its number."""

    def __init__(self, tracker: Any) -> None:
        self.tracker = tracker
        self.marked_comments = tracker.marked_comments

    @property
    def capabilities(self) -> Any:
        return self.tracker.capabilities

    async def read_pull_request(self, ref: PullRequestRef) -> PullRequest:
        return PullRequest(
            ref=ref,
            head_sha=HEAD_A,
            head_ref="curie/publication",
            base_ref="main",
            state=PullRequestState.OPEN,
            url=f"https://github.com/{REPO}/pull/{ref.number}",
        )

    async def link_pull_request(self, issue: Any, pull: PullRequest) -> None:
        await self.tracker.link_pull_request(issue, pull)


@dataclass
class _Leg:
    """One tracker under test, read through its own fake."""

    name: str
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    client: Any
    body: Callable[[], str]
    labels: Callable[[], set[str]]
    writes: Callable[[], int]
    pr: int = PR
    jira: JiraFake | None = None
    ticket: int = 0

    @property
    def url(self) -> str:
        return f"https://github.com/{REPO}/pull/{self.pr}"

    @property
    def merged_line(self) -> str:
        return f"Pull request merged: {self.url}"

    def merge(self) -> None:
        _observe_lineage_state(self.client, self.work_item_id, "merged")
        _reconcile()


def _route_through_ports(
    monkeypatch: pytest.MonkeyPatch,
    open_tracker: Callable[[], Any],
    issue: Any,
) -> None:
    """Send every status pass through the port sync against ``open_tracker``'s
    tracker, whose ticket is ``issue``."""

    async def sync_one(http: Any, settings: Any, claim: Any, _verified: Any) -> int:
        async with open_tracker() as tracker:
            with monkeypatch.context() as adapters:
                adapters.setattr(
                    factory_notices, "tracker_for_work_item", lambda *_a, **_k: tracker
                )
                adapters.setattr(
                    factory_notices, "code_host_for_repository", lambda *_a, **_k: _Ports(tracker)
                )
                return await factory_notices._sync_through_ports(
                    http,
                    settings,
                    replace(claim, tracker_issue=issue, target=replace(claim.target, issue=issue)),
                )

    monkeypatch.setattr(factory_notices, "_sync_one", sync_one)


def _completed(name: str, fixture: Any, monkeypatch: pytest.MonkeyPatch, number: int) -> _Leg:
    """A completed request with its pull request open on ``name``'s tracker."""

    client, github, sink = fixture
    if name == "github":
        sink.by_path = True
        number, pr, first = _published_issue(client, github, sink)
        request_id, work_item_id = first["id"], first["work_item_id"]
        return _Leg(
            name,
            request_id,
            work_item_id,
            client,
            lambda: _marked(sink, request_id)[0]["body"],
            lambda: _curie_labels(sink, number),
            lambda: len(_writes(sink)),
            pr=pr,
        )
    jira: JiraFake | None = None
    if name == "memory":
        memory = InMemoryTracker()
        issue = memory.seed_issue("Ticket", "The ticket body.")

        @asynccontextmanager
        async def open_memory() -> AsyncIterator[Any]:
            yield memory

        _route_through_ports(monkeypatch, open_memory, issue)

        def body() -> str:
            ((_author, text),) = memory.marked_comments.comments(ReplyTarget.on_issue(issue))
            return text

        def labels() -> set[str]:
            return set(memory.labels(issue)) & STATE_LABELS

        def writes() -> int:
            return memory.ledger.writes

    else:
        jira = JiraFake()
        ticket = jira.seed_issue("Ticket")
        issue = jira.ref(ticket)

        @asynccontextmanager
        async def open_jira() -> AsyncIterator[Any]:
            async with jira_client(jira) as http:
                yield jira_tracker(jira, client=http)

        _route_through_ports(monkeypatch, open_jira, issue)

        def body() -> str:
            (comment,) = jira.own_comments(issue)
            return flatten(comment.body)

        def labels() -> set[str]:
            return set(jira.issue(issue).labels) & STATE_LABELS

        def writes() -> int:
            return jira.writes

    request_id = _admit(client, github, sink, number)
    _start_running(client, request_id)
    work_item_id = _request(number)["work_item_id"]
    _reconcile()
    _attach_publication(work_item_id, status="succeeded", pr=PR)
    _reconcile()
    return _Leg(
        name,
        request_id,
        work_item_id,
        client,
        body,
        labels,
        writes,
        jira=jira,
        ticket=0 if jira is None else ticket.id,
    )


_NUMBERS = {"github": 0, "memory": 56901, "jira": 56902}


@pytest.mark.parametrize("name", LEGS)
def test_merged_completed_request_gets_merge_notice_and_no_state_label(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    leg = _completed(name, admitted, monkeypatch, _NUMBERS[name])
    before = leg.body()
    assert f"Completed: {leg.url}" in before
    assert "Pull request merged" not in before
    assert leg.labels() == {PR_OPEN}
    assert _request_status(leg) == "completed"

    leg.merge()

    after = leg.body()
    result = after.split(f"Completed: {leg.url}", 1)[1]
    assert leg.merged_line in result
    assert after.count(leg.merged_line) == 1
    assert leg.labels() == set()
    assert _notices(leg.request_id)[0]["finalized_at"] is not None

    # Idempotent: a later pass writes nothing and keeps the same text.
    writes = leg.writes()
    _reconcile()
    _reconcile()
    assert leg.body() == after
    assert leg.writes() == writes


def test_merge_notice_no_transition_on_the_jira_fake(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    leg = _completed("jira", admitted, monkeypatch, _NUMBERS["jira"])
    assert leg.jira is not None
    status = leg.jira.issue(leg.ticket).status

    leg.merge()

    assert leg.merged_line in leg.body()
    assert leg.labels() == set()
    # Liveness: the pass reached Jira, and none of it was a transition.
    assert leg.jira.requests
    assert [r for r in leg.jira.requests if "/transitions" in r.path] == []
    assert leg.jira.issue(leg.ticket).status == status


@pytest.mark.parametrize("name", LEGS)
def test_open_lineage_completed_request_unchanged(
    admitted: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    leg = _completed(name, admitted, monkeypatch, _NUMBERS[name])
    before, writes = leg.body(), leg.writes()

    _observe_lineage_state(leg.client, leg.work_item_id, "open")
    _reconcile()
    _reconcile()

    assert leg.body() == before
    assert "Pull request merged" not in before
    assert leg.labels() == {PR_OPEN}
    assert leg.writes() == writes


def test_closed_lineage_completed_request_unchanged(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    sink.by_path = True
    number, _pr, first = _published_issue(client, github, sink)
    before = _marked(sink, first["id"])[0]["body"]

    _observe_lineage_state(client, first["work_item_id"], "closed")
    _reconcile()

    assert "Pull request merged" not in _marked(sink, first["id"])[0]["body"]
    assert _curie_labels(sink, number) == {PR_OPEN}
    assert before.splitlines()[0] == _marked(sink, first["id"])[0]["body"].splitlines()[0]


def test_merged_failed_request_unchanged_from_base(
    admitted: Any,  # noqa: F811
) -> None:
    client, github, sink = admitted
    request_id = _admit(client, github, sink, 56903)
    epoch = _start_running(client, request_id)
    work_item_id = _request(56903)["work_item_id"]
    _attach_publication(work_item_id, status="succeeded", pr=PR)
    _finish_failed(client, request_id, epoch, "runner_escalated", "The fixture cannot be repaired.")
    _reconcile()
    assert _curie_labels(sink, 56903) == {NEEDS_HUMAN}

    _observe_lineage_state(client, work_item_id, "merged")
    _reconcile()

    body = _marked(sink, request_id)[0]["body"]
    assert f"Pull request: https://github.com/{REPO}/pull/{PR} (merged)" in body
    assert "Pull request merged:" not in body
    assert _curie_labels(sink, 56903) == set()


def _request_status(leg: _Leg) -> str:
    from test_factory_terminus import _rows

    return str(
        _rows(
            "SELECT status FROM curie.execution_requests WHERE id = :id",
            {"id": leg.request_id},
        )[0]["status"]
    )
