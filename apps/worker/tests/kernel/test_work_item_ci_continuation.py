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
import logging
import time
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

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
from channel_protocol import work_item_events
from curie_dispatcher.queue import to_stream_fields
from curie_worker import kernel as kernel_module
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.delivery_lease import DeliveryLease, DeliveryLeaseStore
from curie_worker.sandbox import MissingAgentPoolError, QuotaRejection
from curie_worker.workitem_dispatch import (
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
    async def resolve(self, kind: str, adapter: str | None, channel: str) -> object:
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
            caller_run=kwargs.get("caller_run"),
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

    async def issue_read_context(self, request_id: uuid.UUID) -> tuple[str, str]:
        return "acme widgets issue 7", f"wir.capability-for-{request_id}"

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


CI_ROUNDS = range(work_item_events.CI_FIRST_FIX_ROUND, work_item_events.CI_MAX_ROUNDS + 1)


@pytest.mark.parametrize("round_", CI_ROUNDS)
def test_ci_event_ids_parse_as_the_ci_kind(round_: int) -> None:
    request_id = uuid.uuid4()
    parsed = parse_work_item_event_id(f"work-item-{request_id}-ci-{round_}")
    assert parsed is not None
    assert parsed.kind == "ci"
    assert parsed.request_id == request_id
    assert parsed.generation == round_


def test_the_worker_parser_agrees_with_the_shared_parser() -> None:
    request_id = uuid.uuid4()
    event_ids = [
        work_item_events.execute_event_id(request_id, 1),
        work_item_events.execute_event_id(request_id, 12),
        work_item_events.terminate_event_id(request_id),
        *(work_item_events.ci_event_id(request_id, round_) for round_ in CI_ROUNDS),
    ]
    for event_id in event_ids:
        shared = work_item_events.parse_work_item_event_id(event_id)
        worker = parse_work_item_event_id(event_id)
        assert shared is not None and worker is not None
        assert worker.request_id == shared.request_id
        assert worker.kind == shared.kind
        assert worker.generation == shared.number


@pytest.mark.parametrize(
    "suffix",
    ["ci-1", f"ci-{work_item_events.CI_MAX_ROUNDS + 1}", "ci-02", "ci-", "ci-2x", "ci", "cI-2"],
)
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


async def _continuation_lease(h: Any, turn: QueuedTurn) -> DeliveryLease:
    """Give the turn real delivery authority on its pending Valkey entry."""
    await h.async_redis.xgroup_create(
        h.config.stream, h.config.consumer_group, id="0", mkstream=True
    )
    entry_id = await h.async_redis.xadd(h.config.stream, to_stream_fields(turn))
    await h.async_redis.xreadgroup(
        h.config.consumer_group, h.config.consumer_name, {h.config.stream: ">"}, count=1
    )
    return await DeliveryLeaseStore(h.async_redis, h.config).acquire(
        h.config.stream,
        h.config.consumer_group,
        entry_id,
        consumer=h.config.consumer_name,
    )


def _refuse_claims(
    h: Any,
    work_items: _WorkItems,
    monkeypatch: pytest.MonkeyPatch,
    *,
    refusals: int | None,
) -> list[str]:
    """Refuse at the fake Kubernetes seam, keeping the real substrate intact."""
    create_claim = h.fake_k8s.create_claim
    claims: list[str] = []

    def claim(name: str, **kwargs: Any) -> None:
        assert work_items.finishes == [], "capacity must not settle the execution"
        assert h.runner.opened == [], "no runner turn starts while claims are refused"
        claims.append(name)
        h.fake_k8s.quota_rejection = (
            QuotaRejection(
                quota_name="curie-sandbox-quota",
                requested={"limits.cpu": "1"},
                used={"limits.cpu": "2"},
                hard={"limits.cpu": "2"},
            )
            if refusals is None or len(claims) <= refusals
            else None
        )
        create_claim(name, **kwargs)

    monkeypatch.setattr(h.fake_k8s, "create_claim", claim)
    return claims


@pytest.mark.parametrize("continuation", ["ci", "approval"])
def test_factory_continuation_waits_past_max_attempts_then_runs(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    continuation: str,
) -> None:
    """#4275: four quota refusals must not consume the three runner attempts."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
            max_attempts=3,
            claim_timeout_seconds=0.05,
        ) as h:
            request_id = uuid.uuid4()
            work_items = _WorkItems(running=request_id)
            h.kernel._work_items = work_items
            turn = _ci_turn(request_id)
            if continuation == "approval":
                turn = turn.model_copy(
                    update={
                        "event_id": f"approval-{uuid.uuid4()}-resolved",
                        "text": "[approval resolved] approved",
                    }
                )
            claims = _refuse_claims(h, work_items, monkeypatch, refusals=4)
            backoff_counts: list[int] = []

            def no_backoff(count: int) -> float:
                backoff_counts.append(count)
                assert work_items.finishes == []
                assert h.sink.updates == [] and h.sink.text_posts == []
                assert h.kernel._order_locks == {}, "capacity waiting releases order"
                return 0.0

            monkeypatch.setattr(h.kernel, "_backoff", no_backoff)
            h.runner.default_script = [Final(text="No fix published.", status=SessionStatus.DONE)]
            with caplog.at_level(logging.INFO, logger="curie_worker.kernel"):
                await h.kernel.process_event(turn)

            assert len(claims) == 5
            assert len(h.runner.opened) == 1
            assert backoff_counts == [1, 2, 3, 4]
            assert h.sink.updates == [] and h.sink.text_posts == []
            assert len(work_items.finishes) == 1
            assert work_items.finishes[0][1]["cause"] == (
                "ci_fix_unpublished" if continuation == "ci" else "no_pull_request"
            )
            retries = [record for record in caplog.records if "capacity retry" in record.message]
            assert len(retries) == 4
            for count, record in enumerate(retries, start=1):
                assert record.levelno == logging.INFO
                assert turn.event_id in record.message
                assert f"refusal={count}" in record.message
                assert "backoff=" in record.message
            assert await h.kernel._markers.is_terminal(turn.event_id)

    asyncio.run(exercise())


@pytest.mark.parametrize("remaining_s", [5.0, 4.0])
def test_ci_capacity_wait_stops_at_delivery_budget(
    make_harness,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    remaining_s: float,
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
            max_attempts=3,
            claim_timeout_seconds=0.05,
        ) as h:
            request_id = uuid.uuid4()
            work_items = _WorkItems(running=request_id)
            h.kernel._work_items = work_items
            turn = _ci_turn(request_id)
            lease = await _continuation_lease(h, turn)
            claims = _refuse_claims(h, work_items, monkeypatch, refusals=None)
            backoff_counts: list[int] = []

            def advance_delivery(count: int) -> float:
                backoff_counts.append(count)
                if count == 4:
                    lease.budget = replace(
                        lease.budget,
                        deadline_ms=lease.budget.anchor_server_ms + int(remaining_s * 1000),
                        anchor_monotonic=time.monotonic(),
                    )
                return 0.0

            monkeypatch.setattr(h.kernel, "_backoff", advance_delivery)
            await h.kernel.process_event(turn, lease=lease)

            assert backoff_counts == [1, 2, 3, 4]
            assert len(claims) == 4
            assert h.runner.opened == []
            assert len(work_items.finishes) == 1
            assert work_items.finishes[0][1]["cause"] == "runner_escalated"
            assert h.sink.updates == [] and h.sink.text_posts == []
            assert await h.kernel._markers.is_terminal(turn.event_id)
            assert "exceeded its delivery deadline" in caplog.text

    asyncio.run(exercise())


def test_ci_capacity_wait_stops_at_execution_deadline(
    make_harness, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=_PublicationApi(),
            max_attempts=3,
            claim_timeout_seconds=0.05,
        ) as h:
            request_id = uuid.uuid4()
            work_items = _WorkItems(running=request_id)
            h.kernel._work_items = work_items
            turn = _ci_turn(request_id)
            lease = await _continuation_lease(h, turn)
            claims = _refuse_claims(h, work_items, monkeypatch, refusals=None)
            backoff_counts: list[int] = []

            def advance_execution(count: int) -> float:
                backoff_counts.append(count)
                if count == 4:
                    run = h.kernel._run_for_event(turn.event_id)
                    assert run is not None
                    run.execution_deadline = datetime.now(UTC) + timedelta(seconds=5)
                return 0.0

            monkeypatch.setattr(h.kernel, "_backoff", advance_execution)
            await h.kernel.process_event(turn, lease=lease)

            assert lease.remaining_s() > 5
            assert backoff_counts == [1, 2, 3, 4]
            assert len(claims) == 4
            assert h.runner.opened == []
            assert len(work_items.finishes) == 1
            assert work_items.finishes[0][1]["cause"] == "runner_escalated"
            assert h.sink.updates == [] and h.sink.text_posts == []
            assert await h.kernel._markers.is_terminal(turn.event_id)
            assert "exceeded its delivery deadline" in caplog.text

    asyncio.run(exercise())


def test_ci_fix_that_cannot_start_settles_runner_escalated(
    make_harness, monkeypatch: pytest.MonkeyPatch
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
            attempts: list[str] = []

            def missing_pool(name: str, **_kwargs: object) -> None:
                attempts.append(name)
                raise MissingAgentPoolError("acme-bot", "curie-agent-acme-bot-runner-pool")

            monkeypatch.setattr(h.fake_k8s, "create_claim", missing_pool)
            turn = _ci_turn(request_id)
            await h.kernel.process_event(turn)

            assert len(attempts) == 1
            assert h.runner.opened == []
            assert len(work_items.finishes) == 1
            assert work_items.finishes[0][1]["cause"] == "runner_escalated"
            assert work_items.finishes[0][1]["detail"] is None
            assert await h.kernel._markers.is_terminal(turn.event_id)

    asyncio.run(exercise())


@pytest.mark.parametrize("metadata_only", [False, True])
def test_a_fix_publication_carries_the_adopted_request_and_epoch(
    make_harness, monkeypatch: pytest.MonkeyPatch, metadata_only: bool
) -> None:
    from curie_worker.approvals import CreatedPublication
    from curie_worker.runner_client import RunnerWorkspaceSnapshot

    class PublicationApi(_PublicationApi):
        def __init__(self) -> None:
            self.creates: list[object] = []
            self.contexts: list[PublicationContext] = []

        async def get_publication_precheck_context(self, **kwargs: object) -> PublicationContext:
            context = await super().get_publication_precheck_context(**kwargs)
            self.contexts.append(context)
            return context

        async def get_publication_lineage(self, *_args: object) -> None:
            return None

        async def create_publication(
            self, request: object, *, budget_s: float = 120
        ) -> CreatedPublication:
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
                    patch=(
                        b"" if metadata_only else b"diff --git a/src/widget.py b/src/widget.py\n"
                    ),
                    changed_paths=() if metadata_only else ("src/widget.py",),
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
            if metadata_only:
                assert created.patch == b""  # type: ignore[attr-defined]
                assert created.observed_title == publications.contexts[0].observed_title  # type: ignore[attr-defined]
                assert created.observed_body_sha256 == publications.contexts[0].observed_body_sha256  # type: ignore[attr-defined]
                assert created.observed_lineage_id == publications.contexts[0].lineage_id  # type: ignore[attr-defined]
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
