"""An answer's follow-up whose pull request merged before it started, end to
end (#5786).

A writer's plain answer is admitted through the signed GitHub webhook. The
reconciler publishes the answer request's execute wake to the runs stream, and
a real worker kernel processes that wake against this API: it acquires the
request through `WorkItemDispatchClient`, and worker start
(`kernel/claim.py`) reads the WorkItem's pull request lineage, a database row,
through `ApprovalClient`. The API refreshes the lineage from the code host and
records the merge, the worker refuses the turn on the terminal pull request
and reports it not started, and the next reconciler pass writes the ticket's
status comment and state labels. Only GitHub's answer to the pull request read
is faked, at the code host seam, in the shape
https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request maps to.
"""

# ruff: noqa: F811  (the shared fixtures are imported, then requested)

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import redis
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn
from channel_protocol.work_item_events import execute_event_id
from curie_api.config import get_settings
from curie_api.factory_notices import ANSWER_PULL_REQUEST_ENDED_SENTENCE
from curie_api.factory_questions import QUESTIONS_LABEL
from curie_api.forges.types import PullRequest, PullRequestRef, PullRequestState
from curie_api.routers import publications
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.approvals import ApprovalClient
from curie_worker.binding import BindingResolver
from curie_worker.sandbox import SuspendedThreadError
from curie_worker.workitem_dispatch import WorkItemDispatchClient
from curie_worker.workspace import WorkspaceClaimCoordinator, WorkspacePreparationError
from forge_fakes.github_comments import _rows, comments  # noqa: F401  (fixture)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from test_factory_question_ending import _round_row, _state_labels, _status_body
from test_factory_terminus import _reconcile
from test_github_answer_intake import (
    ANSWER,
    WRITER,
    World,
    _answer_id,
    _deliver,
    _outstanding,
    world,  # noqa: F401  (fixture)
)

from apps.worker.tests.kernel.conftest import kernel_harness

PR_NUMBER = 41
HEAD = "2" * 40
API = "http://api"


class _Credentials:
    """Workspace selection's repository authority: the WorkItem's repository.
    Worker start refuses before any clone, so nothing is ever redeemed."""

    def __init__(self, repo: str) -> None:
        self.repo = repo

    def select(self, _deployment: uuid.UUID, _thread: str, _author: str, _repo: str | None) -> str:
        return self.repo

    def redeem(self, _deployment: uuid.UUID, _thread: str) -> Any:
        raise AssertionError("worker start must refuse before a clone")


class _Preparer:
    """Workspace preparation, reached only after worker start accepted the
    lineage: it records that and stops the turn there."""

    def __init__(self, repo: str) -> None:
        self.credentials = _Credentials(repo)
        self.reached = False

    def _stop(self, *_args: Any, **_kwargs: Any) -> Any:
        self.reached = True
        raise WorkspacePreparationError("clone", "example clone stopped")

    prepare = prepare_lineage = _stop


class _PullRequestOnGitHub:
    """The code host's read of the WorkItem's pull request."""

    def __init__(self, state: PullRequestState, branch: str, url: str) -> None:
        self.state, self.branch, self.url = state, branch, url
        self.reads: list[PullRequestRef] = []

    async def read_pull_request(self, ref: PullRequestRef) -> PullRequest:
        self.reads.append(ref)
        return PullRequest(
            ref=ref,
            head_sha=HEAD,
            head_ref=self.branch,
            base_ref="main",
            state=self.state,
            url=self.url,
        )


def _open_pull_request(world: World, work_item_id: uuid.UUID) -> tuple[uuid.UUID, str, str]:
    """The WorkItem's deployment with workspaces on, and its pull request
    lineage as a publication leaves it: open."""

    async def go() -> tuple[uuid.UUID, str, str]:
        async with world.client.app.state.sessionmaker() as session:
            item = (
                await session.execute(
                    text(
                        "SELECT agent_id, conversation_id, repository_path, base_commit, "
                        "code_host_kind, code_host_host, repository_project_id, "
                        "code_host_installation_id FROM curie.work_items WHERE id = :id"
                    ),
                    {"id": work_item_id},
                )
            ).one()
            version_id, deployment_id, lineage_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
            branch = f"curie/publication-{lineage_id.hex}"
            url = f"{get_settings().github_html_base}/{item.repository_path}/pull/{PR_NUMBER}"
            await session.execute(
                text(
                    "INSERT INTO curie.agent_versions (id, agent_id, version_label, "
                    "bundle_ref, created_by) VALUES (:id, :agent, 'v1', 'example-bundle', "
                    "'fixture')"
                ),
                {"id": version_id, "agent": item.agent_id},
            )
            await session.execute(
                text(
                    "INSERT INTO curie.deployments (id, agent_id, version_id, environment, "
                    "status, workspace_enabled) VALUES (:id, :agent, :version, "
                    "CAST('dev' AS curie.environment), 'active', true)"
                ),
                {"id": deployment_id, "agent": item.agent_id, "version": version_id},
            )
            await session.execute(
                text(
                    "INSERT INTO curie.thread_publication_lineages (id, agent_id, "
                    "deployment_id, conversation_id, repo_full_name, code_host_kind, "
                    "code_host_host, "
                    "repository_project_id, code_host_installation_id, code_host_pr_id, "
                    "base_ref, base_sha, branch, pr_number, pr_url, head_sha, status) VALUES "
                    "(:id, :agent, :deployment, :conversation, :repo, :kind, :host, :project, "
                    ":installation, :pr_id, 'main', :base, :branch, :number, :url, :head, 'open')"
                ),
                {
                    "id": lineage_id,
                    "agent": item.agent_id,
                    "deployment": deployment_id,
                    "conversation": item.conversation_id,
                    "repo": item.repository_path,
                    "kind": item.code_host_kind,
                    "host": item.code_host_host,
                    "project": item.repository_project_id,
                    "installation": item.code_host_installation_id,
                    "pr_id": str(PR_NUMBER),
                    "base": item.base_commit or "1" * 40,
                    "branch": branch,
                    "number": PR_NUMBER,
                    "url": url,
                    "head": HEAD,
                },
            )
            # The thread's repository selection, as workspace selection
            # records it on the run that opened the pull request.
            await session.execute(
                text(
                    "INSERT INTO curie.thread_workspaces (id, agent_id, "
                    "selected_by_deployment_id, conversation_id, repo_full_name, selected_by) "
                    "VALUES (:id, :agent, :deployment, :conversation, :repo, 'factory')"
                ),
                {
                    "id": uuid.uuid4(),
                    "agent": item.agent_id,
                    "deployment": deployment_id,
                    "conversation": item.conversation_id,
                    "repo": item.repository_path,
                },
            )
            await session.execute(
                text(
                    "UPDATE curie.work_items SET publication_lineage_id = :lineage WHERE id = :id"
                ),
                {"lineage": lineage_id, "id": work_item_id},
            )
            await session.commit()
            return lineage_id, branch, url

    return cast("tuple[uuid.UUID, str, str]", world.client.portal.call(go))


def _request(request_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT status, terminal_cause, last_start_failure_detail "
        "FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )
    return dict(row)


def _lineage_status(world: World, lineage_id: uuid.UUID) -> str:
    async def go() -> Any:
        async with world.client.app.state.sessionmaker() as session:
            return await session.scalar(
                text("SELECT status FROM curie.thread_publication_lineages WHERE id = :id"),
                {"id": lineage_id},
            )

    return str(world.client.portal.call(go))


def _published_wake(request_id: uuid.UUID) -> QueuedTurn:
    """The execute wake the reconciler published for ``request_id``."""

    valkey = redis.Redis(
        host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None, decode_responses=True
    )
    try:
        settings = get_settings()
        turns = [
            QueuedTurn.model_validate_json(fields[STREAM_PAYLOAD_FIELD])
            for _id, fields in valkey.xrange(settings.runs_stream)
        ]
    finally:
        valkey.close()
    prefix = execute_event_id(request_id, 1).rsplit("-", 1)[0] + "-"
    (wake,) = [turn for turn in turns if turn.event_id.startswith(prefix)]
    return wake


def _worker_start(world: World, wake: QueuedTurn) -> tuple[list[Any], bool]:
    """A worker kernel processes the published wake against this API; returns
    the runner turns it opened and whether workspace preparation was reached."""

    settings = get_settings()
    token = uuid.uuid4().hex
    names = {
        "stream": f"test:curie:runs:{token}",
        "group": f"g-{token}",
        "prefix": f"test:curie:worker:{token}",
        "sandbox_prefix": f"test:curie:sandbox:{token}",
    }
    valkey = redis.Redis(
        host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None, decode_responses=True
    )

    preparer = _Preparer(_work_item_repo(world))

    async def go() -> list[Any]:
        engine = create_async_engine(settings.database_url)
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=world.client.app), base_url=API
            ) as http:
                dispatch = WorkItemDispatchClient(
                    api_base_url=API, worker_token=settings.internal_worker_token, client=http
                )
                approvals = ApprovalClient(
                    api_base_url=API,
                    api_key=settings.api_key,
                    client=http,
                    worker_token=settings.internal_worker_token,
                    read_timeout_s=5,
                )

                def workspace(substrate: Any) -> WorkspaceClaimCoordinator:
                    return WorkspaceClaimCoordinator(
                        preparer=cast(Any, preparer),
                        substrate=substrate,
                        suspended_error=SuspendedThreadError,
                    )

                async with kernel_harness(
                    names,
                    valkey,
                    binding_factory=lambda config: BindingResolver(engine, config),
                    workspace_factory=workspace,
                    work_items=dispatch,
                    publication_creator=approvals,
                    max_attempts=1,
                    api_key=settings.api_key,
                    api_base_url=API,
                    internal_worker_token=settings.internal_worker_token,
                ) as h:
                    await h.kernel.process_event(wake)
                    return list(h.runner.opened)
        finally:
            await engine.dispose()

    try:
        opened = cast("list[Any]", world.client.portal.call(go))
    finally:
        valkey.close()
    return opened, preparer.reached


def _work_item_repo(world: World) -> str:
    async def go() -> Any:
        async with world.client.app.state.sessionmaker() as session:
            return await session.scalar(
                text("SELECT repository_path FROM curie.work_items LIMIT 1")
            )

    return str(world.client.portal.call(go))


@pytest.mark.parametrize("pull_request", ["merged", "open"])
def test_answer_follow_up_whose_pull_request_merged_ends_at_worker_start(
    world: World, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, pull_request: str
) -> None:
    """02b: a writer answers the factory's question and the answer starts a
    follow-up; a person then merges the ticket's pull request before that
    follow-up starts. Worker start reads the lineage, the API records the
    merge, and the refused start ends the follow-up ``start_failed``: the
    ticket's status comment says the pull request ended before the factory
    could act on the answer, and its labels leave awaiting-answer.

    Counterexample rejected: the follow-up waiting out its start deferrals
    while the ticket keeps the awaiting-answer label. On a pull request that
    is still open (the liveness case) worker start goes on as before: no
    terminal outcome and no ticket change from this rule."""

    number = 6230 if pull_request == "merged" else 6231
    triage = _outstanding(world, number)
    _reconcile()
    assert QUESTIONS_LABEL in _state_labels(world.sink, number)
    lineage_id, branch, url = _open_pull_request(world, triage.work_item_id)

    comment = world.github.add_comment(number, ANSWER, user=WRITER)
    assert _deliver(world, "webhook", comment, number)["status"] == "factory_admitted"
    answered = _answer_id(triage, comment)
    state = PullRequestState.MERGED if pull_request == "merged" else PullRequestState.OPEN
    github = _PullRequestOnGitHub(state, branch, url)
    monkeypatch.setattr(publications, "code_host_for", lambda *_a, **_k: github)
    _reconcile()
    wake = _published_wake(answered)
    labels = _state_labels(world.sink, number)
    assert labels and QUESTIONS_LABEL not in labels

    opened, prepared = _worker_start(world, wake)

    assert [ref.number for ref in github.reads] == [str(PR_NUMBER)]
    assert opened == []
    request = _request(answered)
    if pull_request == "open":
        # Worker start accepted the open pull request and went on to prepare
        # the workspace; this rule ended nothing.
        assert prepared
        assert _lineage_status(world, lineage_id) == "open"
        assert request["status"] == "waiting", request
        assert request["last_start_failure_detail"] != ANSWER_PULL_REQUEST_ENDED_SENTENCE
        _reconcile()
        assert _state_labels(world.sink, number) == labels
        return
    assert not prepared
    assert _lineage_status(world, lineage_id) == "merged"
    assert (request["status"], request["terminal_cause"]) == ("failed", "start_failed")
    row = _round_row(triage.work_item_id)
    assert row is not None and row["state"] == "cleared"
    _reconcile()
    body = _status_body(world.sink, answered)
    assert f"Could not complete: {ANSWER_PULL_REQUEST_ENDED_SENTENCE}" in body
    assert "Cause: start_failed" in body
    assert _state_labels(world.sink, number) == set()
