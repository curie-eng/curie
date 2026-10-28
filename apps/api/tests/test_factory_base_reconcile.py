"""Durable base moves recover real external effects through the public service."""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import unquote, urlsplit

import httpx
import pytest
from curie_api.config import Settings, get_settings
from curie_api.forges.config import ForgesConfig
from curie_api.models import (
    Agent,
    AgentChannel,
    AgentVersion,
    Deployment,
    ExecutionRequest,
    ThreadPublicationLineage,
    WorkItem,
)
from forge_fakes import factory_wake
from forge_fakes.github import forges_env_json
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from apps.api.tests.test_factory_rebase import (
    BRANCH,
    COMMIT_TIME,
    SECRET,
    GitGraph,
    GitHttp,
    git,
    git_http,
    graph,  # noqa: F401
)

REPO = "acme-corp/acme-bot"
TRANSITION_ID = uuid.UUID("11111111-2222-4333-8444-555555555555")
CONVERSATION = "gitlab:acme-corp%2Facme-bot:issue-7"


def database[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def run() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with async_sessionmaker(engine, expire_on_commit=False)() as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(run())


@dataclass
class TransitionCase:
    graph: GitGraph
    remote: GitHttp
    settings: Settings
    agent_id: uuid.UUID
    work_item_id: uuid.UUID
    request_id: uuid.UUID
    lineage_id: uuid.UUID
    target: str = "main"
    labels: tuple[str, ...] = ("factory", "base:next")
    state: str = "opened"
    retarget_status: int = 200
    retargets: list[dict[str, Any]] = field(default_factory=list)
    notes: list[dict[str, Any]] = field(default_factory=list)
    before_push: list[tuple[dict[str, Any], str]] = field(default_factory=list)
    during_retarget: list[dict[str, Any]] = field(default_factory=list)
    missing_pr: bool = False
    pull_head_sha: str | None = None
    pause_read: asyncio.Event | None = None
    read_started: asyncio.Event | None = None

    @property
    def clone_url(self) -> str:
        return f"{self.remote.origin}/{REPO}.git"

    def credential(self) -> Any:
        return replace(self.graph.credential(self.clone_url), origin=self.remote.origin)

    def snapshot(self) -> Any:
        return self.graph.snapshot(repository=self.repository())

    def repository(self) -> Any:
        from curie_api.forges.types import RepositoryRef

        return RepositoryRef("gitlab", urlsplit(self.remote.origin).netloc, "41", REPO)

    def row(self) -> dict[str, Any]:
        async def read(session: AsyncSession) -> dict[str, Any]:
            result = await session.execute(
                text("SELECT * FROM curie.factory_base_transitions WHERE id = :id"),
                {"id": TRANSITION_ID},
            )
            return dict(result.mappings().one())

        return database(read)

    def update(self, **values: Any) -> None:
        async def write(session: AsyncSession) -> None:
            from curie_api.models import FactoryBaseTransition

            operation = await session.get(FactoryBaseTransition, TRANSITION_ID)
            assert operation is not None
            for key, value in values.items():
                setattr(operation, key, value)
            await session.commit()

        database(write)

    def identities(self) -> tuple[dict[str, Any], dict[str, Any], int]:
        async def read(session: AsyncSession) -> tuple[dict[str, Any], dict[str, Any], int]:
            item = await session.execute(
                text("SELECT * FROM curie.work_items WHERE id = :id"), {"id": self.work_item_id}
            )
            lineage = await session.execute(
                text("SELECT * FROM curie.thread_publication_lineages WHERE id = :id"),
                {"id": self.lineage_id},
            )
            publications = await session.scalar(text("SELECT count(*) FROM curie.publications"))
            return (
                dict(item.mappings().one()),
                dict(lineage.mappings().one()),
                int(publications or 0),
            )

        return database(read)

    def transcript(self) -> str:
        async def read(session: AsyncSession) -> str:
            values = await session.scalars(
                text(
                    "SELECT value FROM curie.thread_transcripts "
                    "WHERE agent_id = :agent AND thread_key = :thread"
                ),
                {"agent": self.agent_id, "thread": CONVERSATION},
            )
            return json.dumps(list(values), sort_keys=True)

        return database(read)

    def record_receive(self) -> None:
        self.before_push.append((self.row(), self.target))

    async def api(self, request: httpx.Request) -> httpx.Response:
        """The production GitLab adapter reads documented REST v4 shapes.

        https://docs.gitlab.com/api/projects/#get-a-single-project
        https://docs.gitlab.com/api/merge_requests/#get-single-mr
        https://docs.gitlab.com/api/merge_requests/#update-mr
        https://docs.gitlab.com/api/branches/#get-single-repository-branch
        https://docs.gitlab.com/api/notes/#list-project-issue-notes
        """

        assert request.headers.get("private-token") == SECRET
        path = unquote(request.url.path).removeprefix("/api/v4")
        if path == "/version":
            return httpx.Response(200, json={"version": "19.4.1"})
        if path == "/user":
            return httpx.Response(200, json={"id": 900, "username": "factory-bot"})
        if path == "/projects/41":
            return httpx.Response(
                200,
                json={
                    "id": 41,
                    "path_with_namespace": REPO,
                    "default_branch": "main",
                    "http_url_to_repo": self.clone_url,
                },
            )
        if path.startswith("/projects/41/repository/branches/"):
            branch = path.removeprefix("/projects/41/repository/branches/")
            head = git(self.graph.origin, "rev-parse", f"refs/heads/{branch}", check=False)
            return (
                httpx.Response(200, json={"name": branch, "commit": {"id": head}})
                if head
                else httpx.Response(404)
            )
        if path == "/projects/41/merge_requests/7":
            if self.read_started is not None:
                self.read_started.set()
            if self.pause_read is not None:
                await self.pause_read.wait()
            if self.missing_pr:
                return httpx.Response(404)
            if request.method == "PUT":
                if self.retarget_status != 200:
                    return httpx.Response(self.retarget_status, json={"message": "Unavailable"})
                operation = await self.async_row()
                self.during_retarget.append(operation)
                change = json.loads(request.content)
                self.retargets.append(change)
                self.target = change["target_branch"]
            return httpx.Response(
                200,
                json={
                    "id": 419,
                    "iid": 7,
                    "project_id": 41,
                    "source_project_id": 41,
                    "target_project_id": 41,
                    "source_branch": BRANCH,
                    "target_branch": self.target,
                    "sha": self.pull_head_sha or self.graph.head(),
                    "state": self.state,
                    "title": "Factory change",
                    "description": "Existing publication",
                    "draft": False,
                    "has_conflicts": False,
                    "web_url": f"{self.remote.origin}/{REPO}/-/merge_requests/7",
                    "updated_at": datetime.now(UTC).isoformat(),
                },
            )
        if path == "/projects/41/issues/7":
            return httpx.Response(
                200,
                json={
                    "id": 107,
                    "iid": 7,
                    "project_id": 41,
                    "state": "opened",
                    "labels": list(self.labels),
                },
            )
        if path == "/projects/41/issues/7/notes":
            if request.method == "POST":
                body = json.loads(request.content)["body"]
                self.notes.append(
                    {
                        "id": len(self.notes) + 1,
                        "body": body,
                        "author": {"id": 900, "username": "factory-bot"},
                        "system": False,
                    }
                )
                return httpx.Response(201, json=self.notes[-1])
            return httpx.Response(200, json=self.notes, headers={"x-next-page": ""})
        raise AssertionError(f"unexpected forge boundary: {request.method} {path}")

    async def async_row(self) -> dict[str, Any]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as connection:
                result = await connection.execute(
                    text("SELECT * FROM curie.factory_base_transitions WHERE id = :id"),
                    {"id": TRANSITION_ID},
                )
                return dict(result.mappings().one())
        finally:
            await engine.dispose()

    async def async_pass(self) -> None:
        from curie_api.factory_base_reconcile import reconcile_base_transitions

        engine = create_async_engine(get_settings().database_url)
        try:
            async with httpx.AsyncClient(transport=httpx.MockTransport(self.api)) as client:
                await reconcile_base_transitions(
                    async_sessionmaker(engine, expire_on_commit=False),
                    settings=self.settings,
                    client=client,
                )
        finally:
            await engine.dispose()

    def tick(self) -> None:
        self.update(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
        asyncio.run(self.async_pass())

    def finish(self) -> dict[str, Any]:
        for _ in range(16):
            self.tick()
            operation = self.row()
            if operation["phase"] in {"conflict", "refused"}:
                pytest.fail(f"base move refused: {operation['error_code']}")
            if operation["phase"] == "applied" and operation["outcome_ready_at"] is not None:
                return operation
        pytest.fail("healthy durable base move did not finish in sixteen passes")


@pytest.fixture
def transition_case(
    clean_db: None,
    graph: GitGraph,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[TransitionCase]:
    from curie_api.models import FactoryBaseTransition

    wait_expired = getattr(request, "param", None) == "wait_expired"
    alias = graph.root / "acme-corp"
    alias.mkdir()
    (alias / "acme-bot.git").symlink_to(graph.origin, target_is_directory=True)
    with git_http(graph) as remote:
        host = urlsplit(remote.origin).netloc
        entry = {
            "name": "gitlab",
            "kind": "gitlab",
            "host": host,
            "api_url": f"{remote.origin}/api/v4",
            "credential": {"type": "static_token", "token": SECRET},
        }
        document = {
            "code_hosts": [entry],
            "trackers": [{**entry, "label": "factory", "mention": "factory-bot"}],
            "bindings": [
                {
                    "name": "factory",
                    "tracker": "gitlab",
                    "tracker_scope_id": "41",
                    "repos": [
                        {
                            "alias": "app",
                            "code_host": "gitlab",
                            "project_id": "41",
                            "path": REPO,
                            "bases": ["main", "next"],
                            "default_base": "main",
                        }
                    ],
                }
            ],
        }
        settings = get_settings().model_copy(
            update={"forges": ForgesConfig.model_validate(document)}
        )
        monkeypatch.setenv("CURIE_FORGES", forges_env_json(document, graph.root))
        get_settings.cache_clear()
        case = TransitionCase(
            graph, remote, settings, uuid.uuid4(), uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        )

        async def seed(session: AsyncSession) -> None:
            agent = Agent(
                id=case.agent_id, name=f"acme-base-{uuid.uuid4().hex}", repo_full_name=REPO
            )
            session.add(agent)
            await session.flush()
            binding = AgentChannel(agent_id=agent.id, kind="gitlab", address=REPO)
            version = AgentVersion(agent_id=agent.id, version_label="v1", created_by="fixture")
            session.add_all([binding, version])
            await session.flush()
            deployment = Deployment(
                agent_id=agent.id, version_id=version.id, environment="dev", workspace_enabled=True
            )
            session.add(deployment)
            await session.flush()
            lineage = ThreadPublicationLineage(
                id=case.lineage_id,
                agent_id=agent.id,
                deployment_id=deployment.id,
                conversation_id=CONVERSATION,
                repo_full_name=REPO,
                branch=BRANCH,
                base_ref="main",
                base_sha=graph.old_base,
                head_sha=graph.source,
                pr_number=7,
                pr_url=f"{remote.origin}/{REPO}/-/merge_requests/7",
                code_host_kind="gitlab",
                code_host_host=host,
                repository_project_id="41",
                code_host_pr_id="419",
                latest_revision=2,
                binding_id=binding.id,
                binding_generation=binding.generation,
            )
            session.add(lineage)
            await session.flush()
            item = WorkItem(
                id=case.work_item_id,
                agent_id=agent.id,
                conversation_id=CONVERSATION,
                tracker_kind="gitlab",
                tracker_host=host,
                tracker_scope_id="41",
                tracker_issue_id="7",
                code_host_kind="gitlab",
                code_host_host=host,
                repository_project_id="41",
                repository_path=REPO,
                publication_lineage_id=lineage.id,
                trigger_label="factory",
                base_branch="main",
                base_source="default",
                base_commit=graph.old_base,
                next_sequence=2,
            )
            session.add(item)
            await session.flush()
            request = ExecutionRequest(
                id=case.request_id,
                work_item_id=item.id,
                sequence=1,
                status="waiting",
                wait_deadline=datetime.now(UTC)
                + (timedelta(seconds=-1) if wait_expired else timedelta(hours=1)),
                objective="Continue after the selected base changes",
                requester="gitlab:31:developer",
                reply_kind="gitlab",
                reply_address=REPO,
                reply_conversation_id="issue-7",
            )
            session.add(request)
            await session.flush()
            session.add(
                FactoryBaseTransition(
                    id=TRANSITION_ID,
                    work_item_id=item.id,
                    request_id=request.id,
                    lineage_id=lineage.id,
                    phase="pending",
                    version=1,
                    old_base_ref="main",
                    old_base_sha=graph.old_base,
                    old_head_sha=graph.source,
                    target_base_ref="next",
                    target_base_sha=graph.target,
                    base_source="label",
                    branch=BRANCH,
                    pull_request_number="7",
                    committer_name="Factory",
                    committer_email="factory@example.com",
                    committer_time=COMMIT_TIME,
                    changed_paths=["feature-one.txt", "feature-two.txt"],
                    next_attempt_at=datetime.now(UTC),
                )
            )
            await session.commit()

        database(seed)
        remote.before_receive = case.record_receive
        try:
            yield case
        finally:
            get_settings.cache_clear()
            graph.assert_clean()


def _prepare(case: TransitionCase) -> Any:
    from curie_api.factory_rebase import prepare_rebase

    prepared = asyncio.run(prepare_rebase(case.snapshot(), case.credential()))
    case.update(phase="prepared", rebased_head_sha=prepared.head_sha, prepared_at=datetime.now(UTC))
    return prepared


def _push(case: TransitionCase, prepared: Any, *, recorded: bool) -> None:
    from curie_api.factory_rebase import push_rebase

    asyncio.run(push_rebase(case.snapshot(), case.credential(), prepared))
    if recorded:
        case.update(phase="pushed", pushed_at=datetime.now(UTC))


def _retarget(case: TransitionCase, *, recorded: bool) -> None:
    case.target = "next"
    case.retargets.append({"target_branch": "next"})
    if recorded:
        case.update(phase="retargeted", retargeted_at=datetime.now(UTC))


def _reach_phase(case: TransitionCase, phase: str) -> dict[str, Any]:
    for _ in range(8):
        operation = case.row()
        if operation["phase"] == phase:
            return operation
        assert operation["phase"] not in {"conflict", "refused"}, operation["error_code"]
        case.tick()
    pytest.fail(f"healthy durable base move did not reach {phase}")


def test_durable_move_commits_each_candidate_before_its_remote_effect_and_keeps_history(
    transition_case: TransitionCase,
) -> None:
    case = transition_case
    before_item, before_lineage, before_publications = case.identities()

    completed = case.finish()

    assert completed["verification_head_sha"] == case.graph.head()
    assert completed["verification_head_sha"] != completed["rebased_head_sha"]
    assert case.remote.pushes == 2
    assert [(row["phase"], target) for row, target in case.before_push] == [
        ("prepared", "main"),
        ("ci_prepared", "next"),
    ]
    assert case.before_push[0][0]["rebased_head_sha"] == completed["rebased_head_sha"]
    assert case.before_push[1][0]["verification_head_sha"] == completed["verification_head_sha"]
    assert all(row["phase"] == "pushed" for row in case.during_retarget)
    assert case.before_push[1][0]["retargeted_at"] is not None
    assert case.retargets == [{"target_branch": "next"}]
    item, lineage, publications = case.identities()
    assert (
        item["base_branch"],
        item["base_source"],
        item["base_commit"],
        item["base_resolved_request_id"],
    ) == ("next", "label", case.graph.target, case.request_id)
    assert (lineage["base_ref"], lineage["base_sha"], lineage["head_sha"]) == (
        "next",
        case.graph.target,
        completed["verification_head_sha"],
    )
    for column in (
        "id",
        "branch",
        "pr_number",
        "pr_url",
        "code_host_kind",
        "code_host_host",
        "repository_project_id",
        "code_host_pr_id",
        "latest_revision",
    ):
        assert lineage[column] == before_lineage[column]
    assert item["publication_lineage_id"] == before_item["publication_lineage_id"]
    assert publications == before_publications == 0
    history = case.transcript()
    assert completed["verification_head_sha"] in history
    assert "next" in history
    case.tick()
    assert case.transcript() == history
    assert case.remote.pushes == 2
    assert case.retargets == [{"target_branch": "next"}]


@pytest.mark.parametrize(
    "crash_after", ["prepared", "rebase_push", "retarget", "ci_prepared", "ci_push"]
)
def test_restart_observes_prior_effect_without_repeating_a_remote_write(
    transition_case: TransitionCase,
    crash_after: str,
) -> None:
    from curie_api.factory_rebase import prepare_ci_marker, push_ci_marker

    case = transition_case
    prepared = _prepare(case)
    if crash_after != "prepared":
        _push(case, prepared, recorded=crash_after != "rebase_push")
    if crash_after in {"retarget", "ci_prepared", "ci_push"}:
        _retarget(case, recorded=crash_after != "retarget")
    if crash_after in {"ci_prepared", "ci_push"}:
        marker = asyncio.run(
            prepare_ci_marker(case.snapshot(), case.credential(), prepared.head_sha)
        )
        case.update(phase="ci_prepared", verification_head_sha=marker.head_sha)
        if crash_after == "ci_push":
            asyncio.run(
                push_ci_marker(case.snapshot(), case.credential(), marker, prepared.head_sha)
            )
    saved = case.row()

    applied = case.finish()

    assert applied["rebased_head_sha"] == saved["rebased_head_sha"] == prepared.head_sha
    if saved["verification_head_sha"] is not None:
        assert applied["verification_head_sha"] == saved["verification_head_sha"]
    assert case.graph.head() == applied["verification_head_sha"]
    assert case.remote.pushes == 2
    assert case.retargets == [{"target_branch": "next"}]
    assert case.target == "next"


@pytest.mark.parametrize("target", ["next", "main"], ids=["unchanged", "different"])
def test_observed_selection_in_applied_outcome_gap_preserves_transition_request(
    transition_case: TransitionCase, target: str
) -> None:
    from curie_api.factory_base_reconcile import observe_base_labels
    from curie_api.workitem_dispatch import (
        AcquireGrant,
        DispatchConflict,
        StartResult,
        acquire,
        finish,
        start,
    )

    case = transition_case
    for _ in range(16):
        case.tick()
        operation = case.row()
        assert operation["phase"] not in {"conflict", "refused"}, operation["error_code"]
        if operation["phase"] == "applied":
            break
    else:
        pytest.fail("healthy durable base move did not reach its applied outcome gap")
    assert operation["outcome_ready_at"] is None
    assert operation["request_id"] == case.request_id
    assert case.identities()[0]["base_selection_fingerprint"] is None
    case.labels = ("factory", f"base:{target}")

    async def observe(session: AsyncSession) -> None:
        item = await session.get(WorkItem, case.work_item_id)
        assert item is not None
        async with httpx.AsyncClient(transport=httpx.MockTransport(case.api)) as client:
            await observe_base_labels(
                session,
                item=item,
                labels=set(case.labels),
                default_branch="main",
                event_identity=None,
                settings=case.settings,
                client=client,
            )
        await session.commit()

    async def requests(session: AsyncSession) -> list[tuple[uuid.UUID, str]]:
        rows = await session.execute(
            text(
                "SELECT id, status FROM curie.execution_requests "
                "WHERE work_item_id = :id ORDER BY sequence"
            ),
            {"id": case.work_item_id},
        )
        return [(row.id, row.status) for row in rows]

    database(observe)
    item = case.identities()[0]
    assert database(requests) == [(case.request_id, "waiting")]
    assert case.row()["outcome_ready_at"] is None
    if target != "next":
        assert item["readmit_request_id"] is not None
        assert item["readmit_request_id"] != case.request_id
        assert item["readmit_base_branch"] == target
        return
    assert item["readmit_request_id"] is None
    assert item["readmit_objective"] is None

    case.tick()
    assert case.row()["outcome_ready_at"] is not None
    assert database(requests) == [(case.request_id, "waiting")]

    async def execute(session: AsyncSession) -> None:
        request = await session.get(ExecutionRequest, case.request_id)
        assert request is not None
        generation = request.dispatch_generation
        async with httpx.AsyncClient(transport=httpx.MockTransport(case.api)) as client:
            acquired = await acquire(
                session,
                case.request_id,
                owner="base-observation-worker",
                generation=generation,
                wake=await factory_wake(session, case.request_id),
                context=None,
                client=client,
            )
        assert isinstance(acquired, AcquireGrant), acquired
        started = await start(
            session,
            case.request_id,
            owner="base-observation-worker",
            generation=generation,
            claim_name="claim-base-observation",
            sandbox_name="sandbox-base-observation",
            context=acquired.context,
        )
        assert isinstance(started, StartResult), started
        result = await finish(
            session,
            case.request_id,
            runtime_epoch=started.runtime_epoch,
            outcome="failed",
            cause="no_pull_request",
            detail="No changes needed: repository verification passed on the new base.",
            ci_fix_round=None,
        )
        assert isinstance(result, DispatchConflict), result
        assert result.code == "publication_pending"

    database(execute)
    completed = case.row()
    assert completed["request_id"] == case.request_id
    assert completed["ci_ready_at"] is not None
    assert completed["ci_settled_at"] is None
    assert database(requests) == [(case.request_id, "running")]
    assert case.identities()[0]["readmit_request_id"] is None
    assert case.identities()[2] == 0
    assert case.graph.head() == completed["verification_head_sha"]
    assert case.remote.pushes == 2


def test_restart_before_history_acknowledgement_does_not_duplicate_the_outcome(
    transition_case: TransitionCase,
) -> None:
    case = transition_case
    completed = case.finish()
    history = case.transcript()
    case.update(outcome_ready_at=None)

    recovered = case.finish()

    assert recovered["verification_head_sha"] == completed["verification_head_sha"]
    assert recovered["outcome_ready_at"] is not None
    assert case.transcript() == history
    assert case.remote.pushes == 2
    assert case.retargets == [{"target_branch": "next"}]


def test_applied_base_outcome_survives_fresh_runner_replay_and_retry(
    transition_case: TransitionCase,
) -> None:
    from curie_api.models import ThreadTranscript
    from curie_runner.history import TurnRecord, _parse_records, build_conversation_replay
    from sqlalchemy import select

    case = transition_case
    completed = case.finish()
    marker = f"factory-base-transition:{TRANSITION_ID}"

    async def stored(session: AsyncSession) -> list[Any]:
        row = await session.scalar(
            select(ThreadTranscript).where(
                ThreadTranscript.agent_id == case.agent_id,
                ThreadTranscript.binding_scope.is_(None),
                ThreadTranscript.thread_key == CONVERSATION,
            )
        )
        assert row is not None
        assert isinstance(row.value, list)
        return row.value

    original = database(stored)
    assert sum(isinstance(entry, dict) and entry.get("id") == marker for entry in original) == 1
    records = _parse_records(original)
    assert len(records) == 1
    outcome = records[0]
    assert isinstance(outcome, TurnRecord)
    assert "next" in outcome.assistant
    assert completed["target_base_sha"] in outcome.assistant
    assert completed["verification_head_sha"] in outcome.assistant
    replay, summary = build_conversation_replay(records)
    assert replay.present
    assert summary is None
    assert json.dumps(outcome.assistant) in json.dumps(
        [message.to_dict() for message in replay.messages]
    )

    case.update(outcome_ready_at=None)
    recovered = case.finish()

    assert recovered["outcome_ready_at"] is not None
    assert recovered["verification_head_sha"] == completed["verification_head_sha"]
    retried = database(stored)
    assert retried == original
    assert sum(isinstance(entry, dict) and entry.get("id") == marker for entry in retried) == 1
    restored, restored_summary = build_conversation_replay(_parse_records(retried))
    assert restored_summary is None
    assert restored.messages == replay.messages


def test_retarget_outage_preserves_pushed_progress_until_a_healthy_retry(
    transition_case: TransitionCase,
) -> None:
    case = transition_case
    prepared = _prepare(case)
    _push(case, prepared, recorded=True)
    case.retarget_status = 503

    for _ in range(10):
        case.tick()
        row = case.row()
        assert row["phase"] == "pushed"
        assert row["rebased_head_sha"] == prepared.head_sha == case.graph.head()
        assert row["verification_head_sha"] is None
        assert case.remote.pushes == 1
        assert case.target == "main"
        assert case.identities()[1]["base_ref"] == "main"
        assert not case.retargets

    case.retarget_status = 200
    assert case.finish()["verification_head_sha"] == case.graph.head()
    assert case.remote.pushes == 2
    assert case.retargets == [{"target_branch": "next"}]


@pytest.mark.parametrize("phase", ["prepared", "ci_prepared"])
def test_push_readback_outage_preserves_the_landed_write_until_a_healthy_retry(
    transition_case: TransitionCase, phase: str
) -> None:
    case = transition_case
    operation = _reach_phase(case, phase)
    expected_head = (
        operation["rebased_head_sha"] if phase == "prepared" else operation["verification_head_sha"]
    )
    expected_pushes = 1 if phase == "prepared" else 2
    before_lineage = case.identities()[1]

    def fail_reads_on_receive() -> None:
        case.record_receive()
        case.remote.fail_reads = True

    case.remote.before_receive = fail_reads_on_receive
    try:
        case.tick()
    finally:
        case.remote.before_receive = case.record_receive
        case.remote.fail_reads = False

    current = case.row()
    assert current["phase"] not in {"conflict", "refused", "applied"}, current["error_code"]
    assert case.graph.head() == expected_head
    assert case.remote.pushes == expected_pushes
    assert case.identities()[1] == before_lineage

    applied = case.finish()

    assert case.graph.head() == applied["verification_head_sha"]
    assert case.identities()[1]["head_sha"] == applied["verification_head_sha"]
    assert case.identities()[1]["base_ref"] == "next"
    assert case.remote.pushes == 2
    assert case.retargets == [{"target_branch": "next"}]


@pytest.mark.parametrize("phase", ["pushed", "ci_prepared", "ci_pushed"])
def test_lagging_pull_head_retries_until_the_provider_observes_the_pushed_branch(
    transition_case: TransitionCase, phase: str
) -> None:
    case = transition_case
    operation = _reach_phase(case, phase)
    case.pull_head_sha = (
        operation["old_head_sha"] if phase == "pushed" else operation["rebased_head_sha"]
    )
    expected_head = (
        operation["rebased_head_sha"] if phase == "pushed" else operation["verification_head_sha"]
    )
    expected_pushes = 1 if phase == "pushed" else 2
    before_lineage = case.identities()[1]

    for _ in range(2):
        case.tick()
        current = case.row()
        assert current["phase"] not in {"conflict", "refused", "applied"}, current["error_code"]
        assert case.graph.head() == expected_head
        assert case.remote.pushes == expected_pushes
        assert case.identities()[1] == before_lineage

    case.pull_head_sha = None
    applied = case.finish()

    assert case.graph.head() == applied["verification_head_sha"]
    assert case.identities()[1]["head_sha"] == applied["verification_head_sha"]
    assert case.identities()[1]["base_ref"] == "next"
    assert case.remote.pushes == 2
    assert case.retargets == [{"target_branch": "next"}]


@pytest.mark.parametrize("phase", ["pushed", "ci_prepared", "ci_pushed"])
@pytest.mark.parametrize("drift", ["pull", "branch"])
def test_unrelated_heads_after_push_refuse_without_more_remote_writes(
    transition_case: TransitionCase, phase: str, drift: str
) -> None:
    case = transition_case
    operation = _reach_phase(case, phase)
    before_head = case.graph.head()
    before_pushes = case.remote.pushes
    before_retargets = list(case.retargets)
    before_lineage = case.identities()[1]
    if drift == "pull":
        case.pull_head_sha = case.graph.first
        expected_head = before_head
    else:
        case.pull_head_sha = before_head
        expected_head = case.graph.commit("human.txt", "new remote owner\n", "Concurrent edit")
        git(case.graph.writer, "push", "--force", "origin", BRANCH)

    case.tick()
    case.tick()

    assert case.row()["phase"] == "refused"
    assert case.row()["error_code"] == "head_changed"
    assert case.graph.head() == expected_head
    assert case.remote.pushes == before_pushes
    assert case.retargets == before_retargets
    assert case.identities()[1] == before_lineage
    assert case.row()["rebased_head_sha"] == operation["rebased_head_sha"]


def test_unrelated_remote_head_is_refused_without_rewriting_it(
    transition_case: TransitionCase,
) -> None:
    case = transition_case
    _prepare(case)
    changed = case.graph.commit("human.txt", "new remote owner\n", "Concurrent source edit")
    git(case.graph.writer, "push", "origin", BRANCH)

    for _ in range(3):
        case.tick()

    assert case.row()["phase"] == "refused"
    assert case.row()["error_code"] == "head_changed"
    assert case.graph.head() == changed
    assert case.target == "main"
    assert case.remote.pushes == 0
    assert not case.retargets


@pytest.mark.parametrize("state", ["closed", "merged", "missing"])
def test_non_open_pr_refuses_before_remote_mutation(
    transition_case: TransitionCase, state: str
) -> None:
    case = transition_case
    case.state = state
    case.missing_pr = state == "missing"

    for _ in range(3):
        case.tick()

    assert case.row()["phase"] == "refused"
    assert case.graph.head() == case.graph.source
    assert case.remote.pushes == 0
    assert case.target == "main"
    assert not case.retargets


def test_competing_reconcilers_share_one_live_lease(transition_case: TransitionCase) -> None:
    case = transition_case

    async def competing() -> None:
        case.read_started = asyncio.Event()
        case.pause_read = asyncio.Event()
        first = asyncio.create_task(case.async_pass())
        try:
            await asyncio.wait_for(case.read_started.wait(), timeout=5)
            await asyncio.wait_for(case.async_pass(), timeout=5)
        finally:
            case.pause_read.set()
            await asyncio.wait_for(first, timeout=20)
            case.pause_read = None
            case.read_started = None

    asyncio.run(competing())
    case.finish()
    assert case.remote.pushes == 2
    assert len(case.retargets) == 1


@pytest.mark.parametrize("fence", ["owner", "version", "expiry", "cancelled"])
def test_stale_inflight_preparation_cannot_commit_or_push(
    transition_case: TransitionCase, fence: str
) -> None:
    case = transition_case

    async def stale() -> None:
        case.read_started = asyncio.Event()
        case.pause_read = asyncio.Event()
        running = asyncio.create_task(case.async_pass())
        try:
            await asyncio.wait_for(case.read_started.wait(), timeout=5)
            engine = create_async_engine(get_settings().database_url)
            try:
                async with engine.begin() as connection:
                    changes = {
                        "owner": "owner = 'replacement-owner', version = version + 1",
                        "version": "version = version + 1",
                        "expiry": "lease_expires_at = clock_timestamp() - interval '1 second'",
                    }
                    if fence == "cancelled":
                        await connection.execute(
                            text(
                                "UPDATE curie.work_items SET cancelled_at = clock_timestamp(), "
                                "version = version + 1 WHERE id = :id"
                            ),
                            {"id": case.work_item_id},
                        )
                    else:
                        await connection.execute(
                            text(
                                "UPDATE curie.factory_base_transitions "
                                f"SET {changes[fence]} WHERE id = :id"
                            ),
                            {"id": TRANSITION_ID},
                        )
            finally:
                await engine.dispose()
        finally:
            case.pause_read.set()
            await asyncio.wait_for(running, timeout=20)
            case.pause_read = None
            case.read_started = None

    asyncio.run(stale())

    row = case.row()
    assert row["phase"] in {"pending", "refused"}
    assert row["rebased_head_sha"] is None
    assert row["verification_head_sha"] is None
    assert case.graph.head() == case.graph.source
    assert not case.retargets
    assert case.remote.pushes == 0


def test_relabel_during_a_leased_prepared_push_queues_the_successor_without_invalidating_progress(
    transition_case: TransitionCase,
) -> None:
    from curie_api.factory_base_reconcile import observe_base_labels

    case = transition_case
    prepared = _reach_phase(case, "prepared")
    observed: list[tuple[dict[str, Any], dict[str, Any], dict[str, Any]]] = []

    async def observe(session: AsyncSession) -> None:
        item = await session.get(WorkItem, case.work_item_id)
        assert item is not None
        async with httpx.AsyncClient(transport=httpx.MockTransport(case.api)) as client:
            await observe_base_labels(
                session,
                item=item,
                labels=set(case.labels),
                default_branch="main",
                event_identity=None,
                settings=case.settings,
                client=client,
            )
        await session.commit()

    def relabel_before_receive() -> None:
        case.record_receive()
        if len(case.before_push) != 1:
            return
        before = case.row()
        case.labels = ("factory", "base:main")
        database(observe)
        observed.append((before, case.row(), case.identities()[0]))

    case.remote.before_receive = relabel_before_receive
    try:
        case.tick()
    finally:
        case.remote.before_receive = case.record_receive

    assert len(observed) == 1
    before, after, item = observed[0]
    assert before["phase"] == "prepared"
    assert before["owner"] is not None
    assert before["lease_expires_at"] > datetime.now(UTC)
    assert after["phase"] == "prepared"
    assert after["owner"] == before["owner"]
    assert after["version"] == before["version"]
    assert item["readmit_request_id"] is not None
    assert item["readmit_request_id"] != case.request_id
    assert item["readmit_base_branch"] == "main"
    assert item["readmit_base_source"] == "label"
    assert case.graph.head() == prepared["rebased_head_sha"]
    successor = item["readmit_request_id"]

    applied = case.finish()

    assert applied["request_id"] == case.request_id
    assert applied["target_base_ref"] == "next"
    assert case.identities()[0]["readmit_request_id"] == successor
    assert case.identities()[1]["head_sha"] == applied["verification_head_sha"]
    assert case.remote.pushes == 2
    assert case.retargets == [{"target_branch": "next"}]


@pytest.mark.parametrize(
    ("stopped", "transition_case"),
    [("cancelled", None), ("wait_expired", "wait_expired")],
    indirect=["transition_case"],
    ids=["cancelled", "wait-expired"],
)
@pytest.mark.parametrize("landed", [False, True], ids=["unpushed", "push-unrecorded"])
def test_stopped_prepared_request_reconciles_only_a_landed_push(
    transition_case: TransitionCase, stopped: str, landed: bool
) -> None:
    case = transition_case
    prepared = _prepare(case)
    before_lineage = case.identities()[1]
    if landed:
        _push(case, prepared, recorded=False)

    async def stop(session: AsyncSession) -> None:
        if stopped == "cancelled":
            item = await session.get(WorkItem, case.work_item_id)
            assert item is not None
            item.cancelled_at = datetime.now(UTC)
            item.version += 1
        else:
            request = await session.get(ExecutionRequest, case.request_id)
            assert request is not None
            assert request.wait_deadline is not None
            assert request.wait_deadline < datetime.now(UTC)
        await session.commit()

    database(stop)
    assert case.row()["phase"] == "prepared"
    assert case.graph.head() == (prepared.head_sha if landed else case.graph.source)

    if not landed:
        case.tick()
        case.tick()
        assert case.row()["phase"] == "refused"
        assert case.row()["error_code"] == "request_not_waiting"
        assert case.graph.head() == case.graph.source
        assert case.identities()[1] == before_lineage
        assert case.target == "main"
        assert case.remote.pushes == 0
        assert not case.retargets
        return

    applied = case.finish()

    assert case.graph.head() == applied["verification_head_sha"]
    assert case.identities()[1]["head_sha"] == applied["verification_head_sha"]
    assert case.identities()[1]["base_ref"] == "next"
    assert case.identities()[0]["base_branch"] == "next"
    assert case.remote.pushes == 2
    assert case.retargets == [{"target_branch": "next"}]


def test_newer_queued_relabel_cannot_overwrite_a_pushed_operation(
    transition_case: TransitionCase,
) -> None:
    case = transition_case
    prepared = _prepare(case)
    _push(case, prepared, recorded=True)
    successor = uuid.uuid4()

    async def queue(session: AsyncSession) -> None:
        item = await session.get(WorkItem, case.work_item_id)
        assert item is not None
        item.readmit_request_id = successor
        item.readmit_requester = "gitlab:31:developer"
        item.readmit_objective = "Use the newer selected base"
        item.readmit_base_branch = "main"
        item.readmit_base_source = "label"
        item.readmit_base_commit = case.graph.old_base
        await session.commit()

    database(queue)
    before = case.row()
    applied = case.finish()

    for column in (
        "id",
        "request_id",
        "old_base_sha",
        "old_head_sha",
        "target_base_ref",
        "target_base_sha",
        "rebased_head_sha",
        "committer_time",
    ):
        assert applied[column] == before[column]
    assert case.target == "next"
    assert case.remote.pushes == 2
    item = case.identities()[0]
    assert item["readmit_request_id"] == successor


def test_conflict_is_a_hold_and_repeated_reconcile_passes_make_no_remote_write(
    transition_case: TransitionCase,
) -> None:
    case = transition_case
    source = case.graph.commit("shared.txt", "factory edit\n", "Conflicting factory edit")
    git(case.graph.writer, "push", "origin", BRANCH)
    git(case.graph.writer, "checkout", "next")
    target = case.graph.commit("shared.txt", "target edit\n", "Conflicting target edit")
    git(case.graph.writer, "push", "origin", "next")
    case.update(old_head_sha=source, target_base_sha=target)

    async def update_source(session: AsyncSession) -> None:
        lineage = await session.get(ThreadPublicationLineage, case.lineage_id)
        assert lineage is not None
        lineage.head_sha = source
        await session.commit()

    database(update_source)
    for _ in range(5):
        case.tick()

    assert case.row()["phase"] == "conflict"
    assert case.row()["error_code"] == "conflict"
    assert case.graph.head() == source
    assert case.remote.pushes == 0
    assert case.target == "main"
    assert not case.retargets
    assert len(case.notes) == 1
