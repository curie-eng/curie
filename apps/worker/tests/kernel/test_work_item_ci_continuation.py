"""A CI fix round is a continuation turn of the SAME factory request (#3097).

The API's CI gate enqueues ``work-item-{request_id}-ci-{round}`` on the runs
stream when the pull request's checks fail. The worker adopts the running
request the way an approval continuation does (``running_for_conversation``),
never acquires or starts a new one, and refuses the turn when the running
request is not the one the event names (a relabel replaced it) or when the
execution already ended. A fix turn that ends without publishing finishes as
``ci_fix_unpublished``; a fix publication carries the adopted request and epoch.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from aci_protocol import (
    Final,
    HookRunRef,
    PublicationContext,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    TurnSource,
)
from curie_worker import kernel as kernel_module
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.workitem_dispatch import (
    WORK_ITEM_CI_RE,
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemRunning,
    WorkItemStartGrant,
    parse_work_item_event_id,
)

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
WORK_ITEM_REPO = "acme-corp/widgets"
ISSUE_URL = f"https://github.com/{WORK_ITEM_REPO}/issues/123"
PR_URL = f"https://github.com/{WORK_ITEM_REPO}/pull/77"
HEAD = "a1" * 20
EPOCH = 7
PUBLISH_TOOL = "mcp__curie__publish_changes"


class _PublicationApi:
    async def get_publication_lineage(self, *_args: object) -> None:
        return None

    async def get_publication_precheck_context(
        self,
        *,
        deployment_id: uuid.UUID,
        work_item_id: uuid.UUID,
        execution_request_id: uuid.UUID,
        runtime_epoch: int,
        queued_event_id: str,
    ) -> PublicationContext:
        return PublicationContext(
            agent_id=AGENT_ID,
            deployment_id=deployment_id,
            work_item_id=work_item_id,
            execution_request_id=execution_request_id,
            runtime_epoch=runtime_epoch,
            conversation_id=f"work-item-{work_item_id}",
            lineage_id=uuid.uuid4(),
            lineage_version=1,
            expected_head=HEAD,
            queued_event_id=queued_event_id,
            precheck_url="https://api.example.com/publications/precheck",
            capability="ppc.example.signature",
            observed_title="Existing pull request",
            observed_body_sha256="b" * 64,
            observed_at=datetime.now(UTC),
        )


def _ci_text(round_: int = 2) -> str:
    return (
        f"{ISSUE_URL}\n"
        f"Curie wait_ci round {round_} of 3: the checks on {PR_URL} failed at {HEAD}.\n"
        "Fix what the failing checks show, run the diff review, then publish.\n"
        '{"check_runs": [{"name": "unit-tests", "conclusion": "failure"}]}'
    )


class _Binding:
    async def resolve(self, kind: str, channel: str) -> object:
        return SimpleNamespace(
            agent_id=AGENT_ID,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
            workspace_enabled=True,
            version_id=uuid.uuid4(),
            version_label="v1",
            bundle_ref=None,
            max_usd_per_day=None,
            max_output_tokens_per_run=None,
            endpoint=None,
            adapter=None,
            approval_routes=None,
        )

    def boot_env(self, _resolved: object, thread_key: str, **_: object) -> dict[str, str]:
        return {
            "CURIE_SESSION_ID": f"work-item-{thread_key}",
            "CURIE_RUNNER_TOKEN": "example-work-item-runner-token",
        }

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()


class _Workspace:
    def __init__(self, substrate: object) -> None:
        self.substrate = substrate
        self.selections: list[object] = []

    def select_repository(self, **kwargs: object) -> object:
        self.selections.append(kwargs["repo_full_name"])
        return kwargs["repo_full_name"] or WORK_ITEM_REPO

    def claim_or_resume_with_handle(self, **kwargs: object) -> object:
        handle = self.substrate.claim(  # type: ignore[attr-defined]
            str(kwargs["thread_key"]),
            env=kwargs.get("env"),
            agent_name=kwargs.get("agent_name"),
            workspace_repo=kwargs.get("repo_full_name"),
        )
        return SimpleNamespace(handle=handle, prepared=None)

    def touch(self, _thread_key: str, *, ttl_seconds: int) -> bool:
        return True

    def release(self, _thread_key: str) -> None:
        return None


class _WorkItems:
    """Dispatch double. ``running_for_conversation`` names the running request."""

    def __init__(self, running: uuid.UUID | None, *, ended: bool = False) -> None:
        self.running = running
        self.ended = ended
        self.calls: list[str] = []
        self.lookups: list[str] = []
        self.finishes: list[tuple[uuid.UUID, dict[str, object]]] = []

    async def running_for_conversation(self, conversation_id: str) -> WorkItemRunning | None:
        self.calls.append("running_for_conversation")
        self.lookups.append(conversation_id)
        if self.ended:
            raise WorkItemConflict("execution_ended")
        if self.running is None:
            return None
        return WorkItemRunning(
            work_item_id=self.running,
            request_id=self.running,
            runtime_epoch=EPOCH,
            execution_deadline=datetime.now(UTC) + timedelta(minutes=20),
        )

    async def acquire(
        self, request_id: uuid.UUID, *, owner: str, generation: int
    ) -> WorkItemAcquireGrant:
        self.calls.append("acquire")
        raise AssertionError("a CI continuation never acquires a request")

    async def start(self, request_id: uuid.UUID, **_: object) -> WorkItemStartGrant:
        self.calls.append("start")
        raise AssertionError("a CI continuation never starts a request")

    async def finish(self, request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("finish")
        self.finishes.append((request_id, kwargs))

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        async def record(*_args: object, **_kwargs: object) -> None:
            self.calls.append(name)

        return record


def _ci_turn(request_id: uuid.UUID, round_: int = 2, *, text: str | None = None) -> QueuedTurn:
    return QueuedTurn(
        event_id=f"work-item-{request_id}-ci-{round_}",
        conversation_id=f"work-item-{request_id}",
        author="github:6601:octocat",
        text=text if text is not None else _ci_text(round_),
        reply_handle=ReplyHandle(
            kind="github",
            channel=WORK_ITEM_REPO,
            placeholder=None,
            endpoint=None,
            adapter=None,
        ),
        received_at="2026-09-24T12:00:00+00:00",
        source=TurnSource.WEBHOOK,
    )


# --- event id namespace ------------------------------------------------------------


@pytest.mark.parametrize("round_", [2, 3])
def test_ci_event_ids_parse_as_the_ci_kind(round_: int) -> None:
    request_id = uuid.uuid4()
    parsed = parse_work_item_event_id(f"work-item-{request_id}-ci-{round_}")
    assert parsed is not None
    assert parsed.kind == "ci"
    assert parsed.request_id == request_id
    assert parsed.generation == round_
    assert WORK_ITEM_CI_RE.fullmatch(f"work-item-{request_id}-ci-{round_}") is not None


@pytest.mark.parametrize("suffix", ["ci-1", "ci-4", "ci-02", "ci-", "ci-2x", "ci", "cI-2"])
def test_out_of_range_or_malformed_ci_ids_do_not_parse(suffix: str) -> None:
    assert parse_work_item_event_id(f"work-item-{uuid.uuid4()}-{suffix}") is None


def test_ci_id_with_a_malformed_uuid_does_not_parse() -> None:
    assert parse_work_item_event_id("work-item-not-a-uuid-ci-2") is None


def test_execute_and_terminate_ids_still_parse() -> None:
    request_id = uuid.uuid4()
    execute = parse_work_item_event_id(f"work-item-{request_id}-execute-1")
    terminate = parse_work_item_event_id(f"work-item-{request_id}-terminate")
    assert execute is not None and execute.kind == "execute"
    assert terminate is not None and terminate.kind == "terminate"


def test_a_targetless_ci_id_is_refused() -> None:
    turn = QueuedTurn.model_construct(
        event_id=f"work-item-{uuid.uuid4()}-ci-2",
        conversation_id=f"cron-{uuid.uuid4().hex}",
        author="cron",
        text="check the build",
        reply_handle=None,
        received_at="2026-09-24T12:00:00+00:00",
        source=TurnSource.CRON,
        attachments=[],
        hook_run=HookRunRef(
            agent_id=str(AGENT_ID), name="nightly", slot_utc="2026-09-24T12:00:00+00:00"
        ),
    )
    with pytest.raises(ValueError, match="work-item or resume id"):
        kernel_module._check_targetless_shape(turn)


# --- adoption into the same request ---------------------------------------------------


def test_a_ci_turn_is_a_factory_work_item_turn(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
        ) as h:
            assert h.kernel._is_factory_work_item_turn(f"work-item-{uuid.uuid4()}-ci-2")

    asyncio.run(exercise())


def test_an_unpublished_fix_turn_finishes_the_same_request_as_ci_fix_unpublished(
    make_harness,
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
        ) as h:
            request_id = uuid.uuid4()
            work_items = _WorkItems(running=request_id)
            h.kernel._work_items = work_items
            h.runner.default_script = [
                TextDelta(text="Looked at the failure. "),
                Final(
                    text="Could not complete: the test needs a fixture.",
                    status=SessionStatus.DONE,
                ),
            ]

            await h.kernel.process_event(_ci_turn(request_id))

            assert "running_for_conversation" in work_items.calls
            assert "acquire" not in work_items.calls
            assert "start" not in work_items.calls
            assert h.runner.opened, "the fix turn must run"
            assert len(work_items.finishes) == 1
            finished_id, finish = work_items.finishes[0]
            assert finished_id == request_id
            assert finish["runtime_epoch"] == EPOCH
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "ci_fix_unpublished"
            assert finish["detail"] is None
            # A CI fix turn has its own bounded loop: no #3128 continuation.
            assert len(h.runner.opened) == 1

    asyncio.run(exercise())


def test_a_fix_publication_carries_the_adopted_request_and_epoch(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_worker.approvals import CreatedPublication
    from curie_worker.runner_client import RunnerWorkspaceSnapshot

    class PublicationApi(_PublicationApi):
        def __init__(self) -> None:
            self.creates: list[object] = []

        async def get_publication_lineage(self, *_args: object) -> None:
            return None

        async def create_publication(self, request: object) -> CreatedPublication:
            self.creates.append(request)
            return CreatedPublication(
                id="publication-ci-fix", approval_id="approval-ci-fix", status="pending"
            )

    async def exercise() -> None:
        publications = PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            request_id = uuid.uuid4()
            work_items = _WorkItems(running=request_id)
            h.kernel._work_items = work_items
            h.runner.default_script = [
                Final(
                    text="Ready to publish the fix",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Publish the CI fix",
                    approval_gate_kind="permission",
                    approval_granted_tool=PUBLISH_TOOL,
                )
            ]

            async def snapshot(*_args: object, **_kwargs: object) -> RunnerWorkspaceSnapshot:
                return RunnerWorkspaceSnapshot(
                    repo_full_name=WORK_ITEM_REPO,
                    base_sha=HEAD,
                    patch=b"diff --git a/src/widget.py b/src/widget.py\n",
                    changed_paths=("src/widget.py",),
                    contains_workflow_files=False,
                    publication_title="Fix the widget parser off-by-one",
                    publication_body="Fixes the failing unit-tests check.",
                )

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            monkeypatch.setattr(
                "curie_worker.kernel.validate_snapshot_against_base",
                lambda *_args, **_kwargs: None,
            )

            await h.kernel.process_event(_ci_turn(request_id))

            assert len(publications.creates) == 1
            created = publications.creates[0]
            assert created.work_item_request_id == request_id  # type: ignore[attr-defined]
            assert created.work_item_runtime_epoch == EPOCH  # type: ignore[attr-defined]
            assert "acquire" not in work_items.calls

    asyncio.run(exercise())


def test_a_continuation_for_a_replaced_request_runs_no_turn(make_harness) -> None:
    """A relabel made a NEW running request; the old request's CI round is stale."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
        ) as h:
            old_request, new_request = uuid.uuid4(), uuid.uuid4()
            work_items = _WorkItems(running=new_request)
            h.kernel._work_items = work_items
            h.runner.default_script = [Final(text="must not run", status=SessionStatus.DONE)]
            turn = _ci_turn(old_request)

            await h.kernel.process_event(turn)

            assert not h.runner.opened
            assert work_items.finishes == []
            assert "acquire" not in work_items.calls
            assert old_request not in h.kernel._work_item_runs
            assert new_request not in h.kernel._work_item_runs
            assert await h.kernel._markers.is_terminal(turn.event_id)

    asyncio.run(exercise())


@pytest.mark.parametrize("state", ["ended", "absent"])
def test_a_continuation_for_an_ended_execution_runs_no_turn(make_harness, state: str) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
        ) as h:
            request_id = uuid.uuid4()
            work_items = _WorkItems(running=None, ended=state == "ended")
            h.kernel._work_items = work_items
            h.runner.default_script = [Final(text="must not run", status=SessionStatus.DONE)]
            turn = _ci_turn(request_id)

            await h.kernel.process_event(turn)

            assert not h.runner.opened
            assert work_items.finishes == []
            assert "acquire" not in work_items.calls
            assert await h.kernel._markers.is_terminal(turn.event_id)

    asyncio.run(exercise())
