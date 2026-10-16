"""The offline factory lifecycle on a Bitbucket code host, for both kinds (#4401, AC 10).

A ``jira_cloud`` InMemoryTracker pairs with a real Bitbucket adapter built by
the production registry from a production ``CURIE_FORGES`` document, over a
`Backend`: a stateful fake for each kind, and for Data Center also the live
fixture. The tracker is registered with monkeypatch in place of the
``jira_cloud`` registry entry (decision 15.2), so the test behaves the same
whether or not the Jira adapter (#4358) has merged.

The factory core still builds the install's GitHub adapter in a few seams
(`curie_api.forges.hosts`), as the ``jira_cloud+github`` pair in
``tests/forges/test_offline_lifecycle.py`` documents; they are replaced here.
The WorkItem's own code host (`forges.trackers.code_host_for_repository`, #4358
plan D14) is replaced by the adapter on the backend's client, in every module
that imported it. The CI gate, reconciler and status replies otherwise run on
the stored Bitbucket identity unpatched, and the status replies go through the
real port branch of ``factory_notices._sync_one`` (#4358 plan D6).

`run_lifecycle` then drives AC 10.1 to 10.7 through the real poll pass,
admission, reconciler, CI gate, reply sink and cancel path. Placeholder
identifiers only.
"""

from __future__ import annotations

import asyncio
import json
import sys
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import httpx
import pytest
import redis
from aci_protocol import STREAM_PAYLOAD_FIELD
from channel_protocol.work_item_events import execute_event_id
from curie_api import factory_base, factory_notices, workitem_dispatch
from curie_api.config import get_settings
from curie_api.factory_comment_text import marker_for
from curie_api.factory_notices import FINAL_MARKER
from curie_api.forges import registry as forge_registry
from curie_api.forges import trackers, types
from curie_api.forges.authority import (
    binding_may_act_on_feedback,
    feedback_actionable,
    feedback_allowlist,
    may_start_with,
)
from curie_api.forges.capabilities import (
    TRACKER_OPERATIONS,
    Operation,
    Support,
    validate_pairing,
)
from curie_api.forges.config import (
    AccountAllowlist,
    BindingConfig,
    RepositoryBindingConfig,
    TrackerConfig,
)
from curie_api.forges.identity import issue_lock_keys, notice_request_id, revision_request_id
from curie_api.forges.memory import InMemoryTracker, PageFaults, WriteLedger, all_supported
from curie_api.forges.ports import CodeHost, Tracker
from curie_api.forges.registry import forges_registry
from curie_api.forges.types import (
    Actor,
    Disposition,
    FeedbackKind,
    PullRequest,
    ReplyTarget,
    RepositoryRef,
    TrackerIssueRef,
)
from curie_api.github_factory_events import mentions_login
from curie_api.main import create_app
from curie_api.workitem_dispatch import Admission, DispatchConflict, acquire, start
from curie_api.workitems.lifecycle import WorkItemOutcome
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from forge_fakes import factory_wake
from forge_fakes.github import REPO, forges_document, forges_env_json
from forge_fakes.github_comments import _clear_ci_keys, _rows

JIRA_HOST = "example.atlassian.net"
JIRA_SCOPE = "10000"
JIRA_KEY = "PROJ-1"
JIRA_TRACKER_NAME = "jira"
BINDING_NAME = "bitbucket-factory"
LABEL = "factory"
MENTION = "curie"
# Jira accounts: the starter `bitbucket_binding` allowlists, and one no binding lists.
MARKING_USER = Actor("placeholder-jira-account-1", "jira-dev")
JIRA_OUTSIDER = Actor("placeholder-jira-account-unlisted", "jira-drive-by")
REVIEW_BODY = f"@{MENTION} please rename the helper before merge"
OUTSIDER_BODY = f"@{MENTION} delete the whole module"
BRANCH = "curie/factory-proj-1"
STATE_LABELS = frozenset(factory_notices.STATE_LABELS)

# What a Jira Cloud tracker declares (ADR 0197): its closing reference already
# links the pull request, it has no dependency read the factory uses, and group
# membership is not readable with the factory's credential.
JIRA_TRACKER_CAPABILITIES: Mapping[Operation, Support] = {
    **all_supported(TRACKER_OPERATIONS),
    Operation.LINK_PULL_REQUEST: Support.NOOP,
    Operation.DEPENDENCIES: Support.NOOP,
    Operation.GROUP_MEMBERSHIP: Support.UNSUPPORTED,
}


class Backend(Protocol):
    """One Bitbucket deployment the lifecycle drives: a fake or a live fixture.

    The adapter itself is always the production one, built by the registry
    from `forges_document`; a backend only seeds and observes from outside.
    """

    kind: str  # "bitbucket_cloud" | "bitbucket_dc"
    code_host_name: str  # the code host's config entry name, e.g. "bitbucket-cloud"

    def forges_document(self) -> dict[str, Any]:
        """The Bitbucket half of ``CURIE_FORGES``: one ``code_hosts`` entry, one
        Jira tracker entry and one binding whose start allowlist names one
        account (`jira_tracker_entry` and `bitbucket_binding` build the last
        two). The memory tracker takes its host, label and scope from them.
        Secrets inline; the pair writes them to files."""
        ...

    def adapter_client(self) -> httpx.AsyncClient:
        """The HTTP client the registry-built adapter talks through."""
        ...

    def repository(self) -> RepositoryRef:
        """The bound repository as `resolve_repository` must answer it."""
        ...

    def push(self, branch: str, message: str) -> str:
        """Push one commit onto ``branch`` (created from the default branch
        when absent); returns its full sha."""
        ...

    def post_status(self, sha: str, key: str, state: str) -> None:
        """Post a build status in the native state words (``FAILED``, ...)."""
        ...

    def comment_as(
        self, who: str, pr: str, body: str, *, inline: bool, parent: str | None = None
    ) -> str:
        """``who`` ("reviewer" or "outsider") comments on pull request ``pr``:
        inline on a changed line, or on the conversation, or as a reply to
        ``parent``. Returns the comment id."""
        ...

    def actor(self, who: str) -> Actor:
        """The code host account of "reviewer" or "outsider"."""
        ...

    def thread_replies(self, pr: str, root: str) -> list[tuple[str, str]]:
        """``(author id, raw text)`` of every live reply under ``root``."""
        ...


# --- shared configuration --------------------------------------------------------


def memory_jira_tracker(
    *,
    host: str = JIRA_HOST,
    scope_id: str = JIRA_SCOPE,
    label: str = LABEL,
    mention: str = MENTION,
    ledger: WriteLedger | None = None,
    faults: PageFaults | None = None,
) -> InMemoryTracker:
    """An InMemoryTracker that declares what the Jira Cloud tracker does."""

    return InMemoryTracker(
        kind=types.JIRA_CLOUD,
        host=host,
        scope_id=scope_id,
        label=label,
        mention=mention,
        capabilities=JIRA_TRACKER_CAPABILITIES,
        ledger=ledger,
        faults=faults,
    )


def jira_tracker_entry() -> dict[str, Any]:
    """The ``trackers`` entry the memory tracker stands in for."""

    return {
        "name": JIRA_TRACKER_NAME,
        "kind": "jira_cloud",
        "host": JIRA_HOST,
        "api_url": f"https://{JIRA_HOST}",
        "credential": {
            "type": "static_token",
            "username": "acme-bot@example.com",
            "token": "fixture-jira-token",
        },
        "label": LABEL,
        "mention": MENTION,
    }


def bitbucket_binding(
    code_host_name: str, repository: RepositoryRef, reviewer: Actor
) -> dict[str, Any]:
    """The Jira binding of one Bitbucket repository: starts by the Jira
    allowlist, review feedback acted on for ``reviewer`` only (decision 11)."""

    return {
        "name": BINDING_NAME,
        "tracker": JIRA_TRACKER_NAME,
        "tracker_scope_id": JIRA_SCOPE,
        "repos": [
            {
                "alias": "app",
                "code_host": code_host_name,
                "project_id": repository.project_id,
                "path": repository.path,
            }
        ],
        "default_repo": "app",
        "start_authority": {"type": "allowlist", "accounts": [MARKING_USER.id]},
        "review_feedback_allowlist": {code_host_name: [reviewer.id]},
    }


class _MemoryJiraConnection:
    """The registry's ``jira_cloud`` tracker connection, answering with the
    test's InMemoryTracker (decision 15.2)."""

    def __init__(self, config: TrackerConfig, tracker: InMemoryTracker) -> None:
        self.config = config
        self._tracker = tracker

    def for_binding(
        self, client: httpx.AsyncClient, binding: BindingConfig, *, label: str | None = None
    ) -> Tracker:
        return self._tracker

    def issue_url(self, issue: TrackerIssueRef) -> str:
        return self._tracker.issue_url(issue)


def install_memory_jira(monkeypatch: pytest.MonkeyPatch, tracker: InMemoryTracker) -> None:
    """Register ``tracker`` as the ``jira_cloud`` kind for this test only.

    Replaces the registry's private kind table entry, which monkeypatch
    restores; an installed Jira adapter is overridden the same way.
    """

    def factory(config: TrackerConfig, *, ca_bundle: str = "") -> _MemoryJiraConnection:
        return _MemoryJiraConnection(config, tracker)

    monkeypatch.setitem(
        forge_registry._KINDS,
        types.JIRA_CLOUD,
        forge_registry._Kind(tracker=factory, code_host=None, installed=True),
    )


# --- persisted state ---------------------------------------------------------------


def _work_items(issue: TrackerIssueRef) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
        "tracker_display_key, code_host_kind, code_host_host, repository_project_id, "
        "repository_path, base_branch, base_commit, cancelled_at, version "
        "FROM curie.work_items WHERE tracker_kind = :kind AND tracker_issue_id = :issue",
        {"kind": issue.kind, "issue": issue.issue_id},
    )


def _work_item(issue: TrackerIssueRef) -> dict[str, Any]:
    rows = _work_items(issue)
    assert len(rows) == 1, rows
    return rows[0]


def _requests(work_item_id: uuid.UUID) -> list[dict[str, Any]]:
    return _rows(
        "SELECT id, sequence, status, terminal_cause, objective, "
        "reply_target_kind, reply_target_pr_number, reply_target_url "
        "FROM curie.execution_requests WHERE work_item_id = :id ORDER BY sequence",
        {"id": work_item_id},
    )


def _status(request_id: uuid.UUID) -> tuple[str, str | None]:
    row = _rows(
        "SELECT status, terminal_cause FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]
    return row["status"], row["terminal_cause"]


def _lineage(work_item_id: uuid.UUID) -> dict[str, Any]:
    return _rows(
        "SELECT l.pr_number, l.pr_url, l.head_sha FROM curie.thread_publication_lineages l "
        "JOIN curie.work_items w ON w.publication_lineage_id = l.id WHERE w.id = :id",
        {"id": work_item_id},
    )[0]


def _turns() -> list[dict[str, Any]]:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        return [
            json.loads(fields[STREAM_PAYLOAD_FIELD.encode()])
            for _id, fields in valkey.xrange(get_settings().runs_stream)
        ]
    finally:
        valkey.close()


def _drop_stream() -> None:
    valkey = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    try:
        valkey.delete(get_settings().runs_stream)
    finally:
        valkey.close()


def _execute_wake(request_id: uuid.UUID) -> dict[str, Any]:
    generation = _rows(
        "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]["dispatch_generation"]
    wakes = [t for t in _turns() if t["event_id"] == execute_event_id(request_id, generation)]
    assert len(wakes) == 1, [t["event_id"] for t in _turns()]
    return wakes[0]


def _ci_turns(request_id: uuid.UUID) -> list[dict[str, Any]]:
    prefix = f"work-item-{request_id}-ci-"
    return [t for t in _turns() if t["event_id"].startswith(prefix)]


def _start(request_id: uuid.UUID) -> None:
    """A worker acquires the published generation and starts the runtime."""

    generation = _rows(
        "SELECT dispatch_generation FROM curie.execution_requests WHERE id = :id",
        {"id": request_id},
    )[0]["dispatch_generation"]

    async def go() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                acquired = await acquire(
                    session, request_id, owner="factory-owner", generation=generation,
                    wake=await factory_wake(session, request_id),
                    context=None,
                )
                assert not isinstance(acquired, DispatchConflict), acquired
                started = await start(
                    session,
                    request_id,
                    owner="factory-owner",
                    generation=generation,
                    claim_name="claim-factory",
                    sandbox_name="sbx-factory",
                    context=acquired.context,
                )
                assert not isinstance(started, DispatchConflict), started
        finally:
            await engine.dispose()

    asyncio.run(go())


def _agent_id() -> uuid.UUID:
    rows = _rows("SELECT id FROM curie.agents")
    assert len(rows) == 1, rows
    agent_id: uuid.UUID = rows[0]["id"]
    return agent_id


def _create_agent(client: TestClient, *, binding_name: str) -> None:
    created = client.post(
        "/agents",
        headers={"X-API-Key": get_settings().api_key},
        json={
            "name": f"acme-factory-{uuid.uuid4().hex[:8]}",
            "repo_full_name": REPO,
            "channel": {"kind": "jira", "address": binding_name},
        },
    )
    assert created.status_code == 201, created.text


# --- the pair ----------------------------------------------------------------------


@dataclass(frozen=True)
class Reply:
    """One status reply write through the port: a create or an edit."""

    request_id: uuid.UUID
    target: str
    comment_id: str
    body: str


@dataclass
class BitbucketPair:
    """The booted install, the registry-built adapter and the tracker."""

    backend: Backend
    tracker: InMemoryTracker
    code_host: CodeHost
    binding: BindingConfig
    repo: RepositoryBindingConfig
    repository: RepositoryRef
    issue: TrackerIssueRef
    # The one Jira account the binding's start allowlist names.
    starter: Actor
    replies: list[Reply] = field(default_factory=list)
    cursor: str | None = None
    feedback_cursor: str | None = None
    pull: PullRequest | None = None
    # The request whose status the real sink is writing now.
    _syncing: uuid.UUID | None = None

    def _patch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The GitHub-only seams named in the module docstring."""

        def host(*_args: Any, **_kwargs: Any) -> CodeHost:
            return self.code_host

        for module, seams in (
            ("curie_api.forges.hosts", ("code_host_for", "factory_code_host")),
            ("curie_api.repository_access", ("code_host_for",)),
            ("curie_api.github_factory", ("factory_code_host",)),
            ("curie_api.routers.publications", ("code_host_for",)),
            ("curie_api.routers.publication_code_host", ("publishing_code_host",)),
            ("curie_api.factory_rediscovery", ("publishing_code_host",)),
        ):
            for seam in seams:
                monkeypatch.setattr(f"{module}.{seam}", host)
        original = trackers.code_host_for_repository
        for name, module in list(sys.modules.items()):
            if module is not None and name.startswith("curie_api"):
                if getattr(module, "code_host_for_repository", None) is original:
                    monkeypatch.setattr(module, "code_host_for_repository", host)
        # Every status write either port makes, with the request it is for.
        for side in (self.tracker.marked_comments, self.code_host.marked_comments):
            monkeypatch.setattr(side, "upsert_marked", self._logged(side.upsert_marked))
        monkeypatch.setattr(factory_notices, "_sync_one", self._sync_one(factory_notices._sync_one))

    def _sync_one(self, original: Callable[..., Any]) -> Callable[..., Any]:
        """The real sink, which writes a Jira WorkItem's replies through the
        ports; remembers the request each write is for."""

        async def sync_one(client: httpx.AsyncClient, settings: Any, claim: Any) -> int:
            self._syncing = claim.row.execution_request_id
            try:
                return int(await original(client, settings, claim))
            finally:
                self._syncing = None

        return sync_one

    def _logged(self, upsert: Callable[..., Any]) -> Callable[..., Any]:
        async def upsert_marked(target: ReplyTarget, marker: str, body: str) -> Any:
            result = await upsert(target, marker, body)
            if result.written and self._syncing is not None:
                self.replies.append(Reply(self._syncing, target.kind, result.comment.id, body))
            return result

        return upsert_marked

    # Steps -----------------------------------------------------------------

    def writes(self, request_id: uuid.UUID) -> list[tuple[str, str, str]]:
        """``(create|edit, target kind, body)`` of one request's replies."""

        seen: set[str] = set()
        found: list[tuple[str, str, str]] = []
        for reply in self.replies:
            if reply.request_id != request_id:
                continue
            found.append(
                ("edit" if reply.comment_id in seen else "create", reply.target, reply.body)
            )
            seen.add(reply.comment_id)
        return found

    def state_labels(self) -> set[str]:
        return set(self.tracker.labels(self.issue)) & STATE_LABELS

    async def _dispatch(self, facts: Admission, *, revision: bool) -> uuid.UUID:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                classid, objid = issue_lock_keys(facts.issue)
                await session.execute(
                    text(
                        "SELECT pg_advisory_xact_lock("
                        "CAST(:classid AS integer), CAST(:objid AS integer))"
                    ),
                    {"classid": classid, "objid": objid},
                )
                admit = workitem_dispatch.admit_revision if revision else workitem_dispatch.readmit
                result = await admit(session, facts)
                await session.commit()
        finally:
            await engine.dispose()
        assert isinstance(result, WorkItemOutcome), result
        assert not result.replayed
        assert result.request is not None and result.request.id == facts.request_id
        return facts.request_id

    def label(self, actor: Actor) -> uuid.UUID | None:
        """``actor`` applies the label; the poll pass admits it when the
        binding's start authority lets ``actor`` start. Returns the request."""

        self.tracker.apply_label(self.issue, self.tracker.label, actor)
        agent_id = _agent_id()

        async def go() -> uuid.UUID | None:
            page = await self.tracker.poll_marked(self.cursor, running=())
            notices = [n for n in page.notices if n.disposition is Disposition.ADMIT]
            assert len(notices) == 1, page
            notice = notices[0]
            assert notice.issue == self.issue and notice.actor == actor
            assert await self.tracker.verify_current(notice)
            self.cursor = page.cursor
            marked_by = await self.tracker.marking_actor(self.issue, self.tracker.label)
            assert marked_by == actor
            if not await may_start_with(
                self.binding.start_authority, self.issue, marked_by, tracker=self.tracker
            ):
                return None
            base = await factory_base.resolve_base(
                policy=self.repo,
                labels=set(),
                default_branch=self.repository.default_branch,
                code_host=self.code_host,
                repository=self.repository,
            )
            assert isinstance(base, factory_base.ResolvedBase), base
            return await self._dispatch(
                Admission(
                    agent_id=agent_id,
                    kind="jira",
                    address=self.binding.name,
                    reply_conversation_id=f"issue-{self.issue.issue_id}",
                    issue=self.issue,
                    repository=self.repository,
                    code_host_installation_id=None,
                    objective=self.tracker.issue_url(self.issue),
                    requester=f"{types.JIRA_CLOUD}:{marked_by.id}:{marked_by.login}",
                    request_id=notice_request_id(notice),
                    base=base,
                ),
                revision=False,
            )

        return asyncio.run(go())

    def unlabel(self, actor: Actor, *, running: tuple[TrackerIssueRef, ...]) -> list[Any]:
        """``actor`` removes the label; returns the poll pass's cancellations."""

        self.tracker.remove_label(self.issue, self.tracker.label, actor)

        async def go() -> list[Any]:
            page = await self.tracker.poll_marked(self.cursor, running=running)
            self.cursor = page.cursor
            cancels = [n for n in page.notices if n.disposition is Disposition.CANCEL]
            for notice in cancels:
                assert await self.tracker.verify_current(notice)
            return cancels

        return asyncio.run(go())

    def open_pull_request(self) -> PullRequest:
        head = self.backend.push(BRANCH, "Fix the widget parser")

        async def go() -> PullRequest:
            assert await self.code_host.find_pull_request(self.repository, head_ref=BRANCH) is None
            closing = await self.tracker.closing_reference(self.issue, self.repository)
            pull = await self.code_host.open_pull_request(
                self.repository,
                head_ref=BRANCH,
                base_ref="main",
                title=f"{JIRA_KEY} Fix the widget parser",
                body=f"{closing}\n\nKeeps the last field of a line.",
            )
            assert pull.head_sha == head
            assert pull.body.startswith(closing)
            await self.tracker.link_pull_request(self.issue, pull)
            # The pull request reads back the same way, by head and by number.
            found = await self.code_host.find_pull_request(self.repository, head_ref=BRANCH)
            assert found is not None and found.ref == pull.ref
            current = await self.code_host.read_pull_request(pull.ref)
            assert current.body.startswith(closing) and current.head_sha == head
            return pull

        self.pull = asyncio.run(go())
        return self.pull

    def feedback(self) -> list[Any]:
        """The review feedback listed since the last read that asks for the bot."""

        assert self.pull is not None
        pull = self.pull

        async def go() -> list[Any]:
            page = await self.code_host.list_review_feedback(pull.ref, self.feedback_cursor)
            self.feedback_cursor = page.cursor
            return [item for item in page.items if mentions_login(item.body, MENTION)]

        return asyncio.run(go())

    def may_act(self, author: Actor) -> bool:
        return asyncio.run(
            binding_may_act_on_feedback(
                self.binding, self.repo, self.code_host, self.repository, author
            )
        )

    def admit_revision(self, feedback: Any) -> uuid.UUID:
        assert self.pull is not None
        pull = self.pull
        agent_id = _agent_id()

        async def go() -> uuid.UUID:
            allowlist = feedback_allowlist(self.binding, self.repo)
            assert await feedback_actionable(self.code_host, feedback, allowlist)
            assert feedback.thread_id is not None
            url = f"{pull.url}#comment-{feedback.id}"
            provenance = {
                "event": feedback.kind.value,
                "url": url,
                "sender": feedback.author.login,
                "body": feedback.body,
            }
            return await self._dispatch(
                Admission(
                    agent_id=agent_id,
                    kind="jira",
                    address=self.binding.name,
                    reply_conversation_id=f"issue-{self.issue.issue_id}",
                    issue=self.issue,
                    repository=self.repository,
                    code_host_installation_id=None,
                    objective=(
                        f"{url}\n\nReview feedback asked for another revision.\n"
                        + json.dumps(provenance)
                    ),
                    requester=f"{self.repository.kind}:{feedback.author.id}:{feedback.author.login}",
                    request_id=revision_request_id(self.repository, feedback.kind, feedback.id),
                    reply_target=ReplyTarget.on_thread(pull.ref, feedback.thread_id),
                    reply_url=url,
                ),
                revision=True,
            )

        return asyncio.run(go())

    def cancel(self, work_item_id: uuid.UUID) -> None:
        """Cancel through the dispatch cancel path, as a verified unlabel does."""

        version = _work_item(self.issue)["version"]

        async def go() -> None:
            engine = create_async_engine(get_settings().database_url)
            try:
                async with AsyncSession(engine, expire_on_commit=False) as session:
                    result = await workitem_dispatch.cancel(
                        session, work_item_id=work_item_id, expected_version=version
                    )
                    await session.commit()
            finally:
                await engine.dispose()
            assert isinstance(result, WorkItemOutcome), result

        asyncio.run(go())


def _document(half: dict[str, Any]) -> dict[str, Any]:
    """Today's GitHub entries, so the install boots, plus the backend's half."""

    document = forges_document()
    for section in ("code_hosts", "trackers", "bindings"):
        document[section] = [*document[section], *half[section]]
    return document


def bitbucket_pair(
    backend: Backend, monkeypatch: pytest.MonkeyPatch, key_dir: Path
) -> Iterator[BitbucketPair]:
    """Boot the install on ``backend`` and yield the pair; a fixture body."""

    half = backend.forges_document()
    (tracker_entry,) = half["trackers"]
    (binding_entry,) = half["bindings"]
    # The memory tracker stands in for the document's own Jira entry.
    tracker = memory_jira_tracker(
        host=tracker_entry["host"],
        scope_id=binding_entry["tracker_scope_id"],
        label=tracker_entry["label"],
        mention=tracker_entry["mention"],
    )
    install_memory_jira(monkeypatch, tracker)
    repository = backend.repository()
    for key, value in {
        "CURIE_WORK_ITEM_WAIT_BUDGET_SECONDS": "30",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        # No GitHub intake: the reconciler must not poll a GitHub tracker.
        "FACTORY_ENABLED": "false",
        "CURIE_FORGES": forges_env_json(_document(half), key_dir),
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        # Admission checks every repository against this list, whatever its host.
        "GITHUB_REPO_ALLOWLIST": json.dumps(["acme-corp/*", repository.path]),
        "GITHUB_TOKEN": "",
        "INTERNAL_WORKER_TOKEN": "factory-terminus-worker",
        "RUNS_STREAM": f"test:curie:bitbucket-lifecycle:{uuid.uuid4().hex}",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("GITHUB_API_URL", raising=False)
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as client:
            _create_agent(client, binding_name=binding_entry["name"])
            http = backend.adapter_client()
            try:
                registry = forges_registry(get_settings())
                code_host = registry.code_host(backend.code_host_name).adapter(http)
                binding = registry.binding(binding_entry["name"])
                (repo,) = binding.repos
                assert repo.code_host == backend.code_host_name
                validate_pairing(tracker, code_host)
                assert isinstance(binding.start_authority, AccountAllowlist)
                (starter_id,) = binding.start_authority.accounts
                assert JIRA_OUTSIDER.id not in binding.start_authority.accounts
                resolved = asyncio.run(code_host.resolve_repository(repository.project_id))
                assert resolved == repository
                assert (resolved.path, resolved.default_branch) == (
                    repository.path,
                    repository.default_branch,
                )
                issue = tracker.seed_issue(
                    "Widget parser drops the last field",
                    "Parsing a three-field line yields two fields.",
                    display_key=JIRA_KEY,
                )
                pair = BitbucketPair(
                    backend,
                    tracker,
                    code_host,
                    binding,
                    repo,
                    resolved,
                    issue,
                    starter=Actor(starter_id, "jira-starter"),
                )
                pair._patch(monkeypatch)
                yield pair
            finally:
                asyncio.run(http.aclose())
                _clear_ci_keys()
                _drop_stream()
    finally:
        get_settings.cache_clear()


# --- the lifecycle -----------------------------------------------------------------


def run_lifecycle(pair: BitbucketPair) -> None:
    """AC 10.1 to 10.7, in the order a run meets them."""

    # Imported here: these suites are siblings on sys.path, not packages.
    from test_factory_ci_wait import _attach_fix
    from test_factory_terminus import _attach_publication, _reconcile, _set_base_ref

    backend, repository = pair.backend, pair.repository
    reviewer, outsider = backend.actor("reviewer"), backend.actor("outsider")

    # 10.7 A label by an account the binding does not allow admits nothing.
    assert pair.label(JIRA_OUTSIDER) is None
    assert _work_items(pair.issue) == []
    pair.unlabel(JIRA_OUTSIDER, running=())
    _reconcile()
    assert _work_items(pair.issue) == []

    # 10.1 The allowlisted account's label admits a WorkItem on the bound repository.
    first = pair.label(pair.starter)
    assert first is not None
    item = _work_item(pair.issue)
    work_item_id = item["id"]
    assert (
        item["code_host_kind"],
        item["code_host_host"],
        item["repository_project_id"],
        item["repository_path"],
    ) == (backend.kind, repository.host, repository.project_id, repository.path)
    assert item["tracker_display_key"] == JIRA_KEY
    assert item["base_branch"] == "main" and item["base_commit"] is not None
    assert [r["id"] for r in _requests(work_item_id)] == [first]
    _reconcile()
    assert _execute_wake(first)["text"] == _requests(work_item_id)[0]["objective"]
    assert pair.state_labels() == {"curie-factory:queued"}
    _start(first)
    _reconcile()
    assert _status(first) == ("running", None)
    assert pair.state_labels() == {"curie-factory:running"}

    # 10.2 The pull request opens through the adapter with the closing reference.
    pull = pair.open_pull_request()
    pr = int(pull.ref.number)
    _attach_publication(work_item_id, status="succeeded", pr=pr, head_sha=pull.head_sha)
    _set_base_ref(work_item_id, "main")
    pr_url = _lineage(work_item_id)["pr_url"]

    # 10.3 A failed status on the head asks for a fix round; a successful one
    # on the fix head completes the request.
    backend.post_status(pull.head_sha, "unit-tests", "FAILED")
    backend.post_status(pull.head_sha, "lint", "SUCCESSFUL")
    _reconcile()
    assert _status(first) == ("running", None)
    fix_turns = _ci_turns(first)
    assert [t["event_id"] for t in fix_turns] == [f"work-item-{first}-ci-2"]
    lines = fix_turns[0]["text"].split("\n")
    assert lines[0] == pair.tracker.issue_url(pair.issue)
    assert lines[1] == (
        f"Curie wait_ci round 2 of 3: the checks on {pr_url} failed at {pull.head_sha}."
    )
    assert "unit-tests" in fix_turns[0]["text"].lower()
    fix_head = backend.push(BRANCH, "Fix the widget parser off-by-one")
    _attach_fix(
        work_item_id,
        first,
        revision=2,
        head_sha=fix_head,
        title="Fix the widget parser off-by-one",
        paths=["src/widget.txt"],
    )
    backend.post_status(fix_head, "unit-tests", "SUCCESSFUL")
    backend.post_status(fix_head, "lint", "SUCCESSFUL")
    _reconcile()
    assert _status(first) == ("completed", "completed")
    assert len(_ci_turns(first)) == 1
    first_writes = pair.writes(first)
    assert [op for op, _, _ in first_writes] == ["create"] + ["edit"] * (len(first_writes) - 1)
    assert {target for _, target, _ in first_writes} == {"issue"}
    assert FINAL_MARKER in first_writes[-1][2] and f"Completed: {pr_url}" in first_writes[-1][2]
    assert pair.state_labels() == {"curie-factory:pr-open"}

    # 10.4 An allowlisted reviewer's inline comment is a revision, answered by
    # exactly one reply in its thread.
    root = backend.comment_as("reviewer", str(pr), REVIEW_BODY, inline=True)
    (asked,) = pair.feedback()
    assert (asked.author, asked.body, asked.kind) == (
        reviewer,
        REVIEW_BODY,
        FeedbackKind.REVIEW_COMMENT,
    )
    assert asked.thread_id == root and asked.head_sha == fix_head
    assert pair.may_act(reviewer) is True
    revision = pair.admit_revision(asked)
    rows = _requests(work_item_id)
    assert [(r["id"], r["sequence"]) for r in rows] == [(first, 1), (revision, 2)]
    assert (rows[1]["reply_target_kind"], rows[1]["reply_target_pr_number"]) == (
        "review_thread",
        str(pr),
    )
    _reconcile()
    assert _execute_wake(revision)["text"] == rows[1]["objective"]
    _start(revision)
    _reconcile()
    assert _status(revision) == ("running", None)
    (reply,) = backend.thread_replies(str(pr), root)
    assert reply[0] not in {reviewer.id, outsider.id}
    assert marker_for(revision) in reply[1]
    assert {target for _, target, _ in pair.writes(revision)} == {"review_thread"}

    # 10.5 A comment by an account outside the allowlist is listed and acted on never.
    backend.comment_as("outsider", str(pr), OUTSIDER_BODY, inline=False)
    (drive_by,) = pair.feedback()
    assert (drive_by.author, drive_by.kind) == (outsider, FeedbackKind.COMMENT)
    assert pair.may_act(outsider) is False
    _reconcile()
    assert [r["id"] for r in _requests(work_item_id)] == [first, revision]
    assert len(backend.thread_replies(str(pr), root)) == 1

    # 10.6 Removing the label cancels the running revision.
    cancels = pair.unlabel(pair.starter, running=(pair.issue,))
    assert [n.issue for n in cancels] == [pair.issue]
    pair.cancel(work_item_id)
    assert _work_item(pair.issue)["cancelled_at"] is not None
    assert _status(revision) == ("cancellation_requested", "issue_cancelled")
    assert _status(first) == ("completed", "completed")
