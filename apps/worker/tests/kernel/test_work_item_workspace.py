"""A factory work-item execution checks out its WorkItem repository (#2992).

The objective of a factory execution is an issue URL. That URL is the
assignment, not a repository picker: the API bound the repository to the
WorkItem from the signed delivery, and the acquire grant carries it. An
ordinary chat message naming the same issue URL still selects nothing.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TextDelta, TurnSource
from channel_protocol.reply import ReplyAck, ReplyEvent
from curie_worker.approvals import ApprovalRequest, CreatedApproval
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import ReplySink, TargetRoute, build_reply_sink
from curie_worker.workitem_dispatch import WorkItemAcquireGrant, WorkItemStartGrant
from redis.exceptions import ResponseError

AGENT_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
DEPLOYMENT_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")
CHANNEL = "C0EXAMPLE1"
WORK_ITEM_REPO = "acme-corp/widgets"
ISSUE_URL = f"https://github.com/{WORK_ITEM_REPO}/issues/123"


class _Binding:
    async def resolve(self, kind: str, channel: str) -> object:
        return SimpleNamespace(
            agent_id=AGENT_ID,
            agent_name="acme-bot",
            deployment_id=DEPLOYMENT_ID,
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
        self.claimed: list[object] = []

    def select_repository(self, **kwargs: object) -> object:
        self.selections.append(kwargs["repo_full_name"])
        return kwargs["repo_full_name"]

    def claim_or_resume_with_handle(self, **kwargs: object) -> object:
        self.claimed.append(kwargs.get("repo_full_name"))
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
    """Dispatch double: the acquire grant carries the WorkItem repository."""

    def __init__(self) -> None:
        self.calls: list[str] = []
        self.after_finish: Callable[[], Awaitable[None]] | None = None

    async def acquire(
        self, request_id: uuid.UUID, *, owner: str, generation: int
    ) -> WorkItemAcquireGrant:
        self.calls.append("acquire")
        return WorkItemAcquireGrant(
            generation=generation,
            work_item_id=request_id,
            conversation_id=f"work-item-{request_id}",
            wait_deadline=(datetime.now(UTC) + timedelta(hours=1)).isoformat(),
            repo_full_name=WORK_ITEM_REPO,
        )

    async def start(self, request_id: uuid.UUID, **_: object) -> WorkItemStartGrant:
        self.calls.append("start")
        return WorkItemStartGrant(
            runtime_epoch=1,
            execution_deadline=datetime.now(UTC) + timedelta(hours=1),
            remaining_s=3600.0,
            heartbeat_interval_s=60.0,
        )

    async def finish(self, _request_id: uuid.UUID, **_: object) -> None:
        self.calls.append("finish")
        if self.after_finish is not None:
            await self.after_finish()

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        async def record(*_args: object, **_kwargs: object) -> None:
            self.calls.append(name)

        return record


class _Approvals:
    async def create(self, _request: ApprovalRequest) -> CreatedApproval:
        return CreatedApproval(id="appr-1", status="pending")


class _RecordingSink:
    def __init__(self, delegate: ReplySink) -> None:
        self.delegate = delegate
        self.events: list[str] = []

    async def emit(
        self,
        event: ReplyEvent,
        *,
        route: TargetRoute,
        best_effort_unreachable: bool = False,
    ) -> ReplyAck:
        self.events.append(event.event)
        return await self.delegate.emit(
            event,
            route=route,
            best_effort_unreachable=best_effort_unreachable,
        )


def _turn(
    event_id: str,
    text: str,
    *,
    kind: str = "slack",
    placeholder: str | None = None,
    conversation_id: str = "1700000000.000001",
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id=conversation_id,
        author="U0EXAMPLE1",
        text=text,
        reply_handle=ReplyHandle(
            kind=kind,
            channel=WORK_ITEM_REPO if kind == "github" else CHANNEL,
            placeholder=placeholder,
            endpoint=None,
            adapter=None,
        ),
        received_at="2026-09-23T01:00:00+00:00",
        source=TurnSource.SLACK,
    )


def test_work_item_execution_checks_out_the_work_item_repository(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [
                TextDelta(text="Working. "),
                Final(text="Working. Done.", status=SessionStatus.DONE),
            ]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert "acquire" in work_items.calls
            assert h.kernel._workspace.selections == [WORK_ITEM_REPO]
            assert h.kernel._workspace.claimed == [WORK_ITEM_REPO]
            assert h.sink.updates == []

    asyncio.run(exercise())


@pytest.mark.parametrize("completion_write_fails", [False, True])
def test_work_item_approval_resume_emits_no_requesting_turn_reply(
    make_harness, completion_write_fails: bool
) -> None:
    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            approvals=_Approvals(),
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            request_id = uuid.uuid4()
            h.runner.default_script = [
                Final(
                    text="Requesting approval.",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Run the requested publication",
                    approval_gate_kind="permission",
                    approval_granted_tool="Bash",
                )
            ]

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )
            assert h.sink.updates == []

            h.runner.default_script = [
                TextDelta(text="Resuming. "),
                Final(text="Resuming. Done.", status=SessionStatus.DONE),
            ]
            resumed = _turn(
                f"approval-{uuid.uuid4()}-resolved",
                "[approval resolved] approved",
                placeholder="approval-placeholder",
            )
            if completion_write_fails:
                async def corrupt_completion_after_finish() -> None:
                    await h.async_redis.set(
                        h.config.completion_key(resumed.event_id), "wrong-type"
                    )

                work_items.after_finish = corrupt_completion_after_finish
                with pytest.raises(ResponseError, match="WRONGTYPE"):
                    await h.kernel.process_event(resumed)
                await h.kernel.notify_turn_not_started(resumed)
            else:
                await h.kernel.process_event(resumed)

            assert h.sink.updates == []
            assert work_items.calls.count("start") == 1
            assert work_items.calls.count("finish") == 1

    asyncio.run(exercise())


def test_github_work_item_reaches_the_model_without_chat_replies(make_harness) -> None:
    async def exercise() -> None:
        sink = build_reply_sink(WorkerConfig())
        github = _RecordingSink(sink._adapters["github"])
        sink._adapters["github"] = github
        try:
            async with make_harness(
                binding=_Binding(),
                workspace_factory=_Workspace,
                sink=sink,
            ) as h:
                work_items = _WorkItems()
                h.kernel._work_items = work_items
                h.runner.default_script = [
                    TextDelta(text="Working. "),
                    Final(text="Working. Done.", status=SessionStatus.DONE),
                ]
                request_id = uuid.uuid4()

                await h.kernel.process_event(
                    _turn(
                        f"work-item-{request_id}-execute-1",
                        f"Resolve {ISSUE_URL}",
                        kind="github",
                    )
                )

                assert h.runner.opened
                assert "start" in work_items.calls
                assert "reply.update" not in github.events
        finally:
            await sink.aclose()

    asyncio.run(exercise())


def test_issue_url_in_ordinary_chat_selects_no_repository(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            h.runner.default_script = [Final(text="Noted.", status=SessionStatus.DONE)]

            await h.kernel.process_event(
                _turn(f"slack-{uuid.uuid4()}", f"Please look at {ISSUE_URL}")
            )

            assert h.kernel._workspace.selections == [None]

    asyncio.run(exercise())


class _BarrierWorkItems(_WorkItems):
    """Holds every acquire until all concurrent executions have acquired."""

    def __init__(self, parties: int) -> None:
        super().__init__()
        self.barrier = asyncio.Barrier(parties)
        self.started: list[uuid.UUID] = []

    async def acquire(
        self, request_id: uuid.UUID, *, owner: str, generation: int
    ) -> WorkItemAcquireGrant:
        grant = await super().acquire(request_id, owner=owner, generation=generation)
        await self.barrier.wait()
        return grant

    async def start(self, request_id: uuid.UUID, **kwargs: object) -> WorkItemStartGrant:
        self.started.append(request_id)
        return await super().start(request_id, **kwargs)


def test_concurrent_work_items_each_start_their_own_request(make_harness) -> None:
    """#3069: a turn never starts under another execution's request."""

    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            request_ids = [uuid.uuid4() for _ in range(3)]
            work_items = _BarrierWorkItems(len(request_ids))
            h.kernel._work_items = work_items
            h.runner.default_script = [
                Final(text="Working. Done.", status=SessionStatus.DONE),
            ]

            await asyncio.gather(
                *(
                    h.kernel.process_event(
                        _turn(
                            f"work-item-{request_id}-execute-1",
                            f"Resolve {ISSUE_URL}",
                            conversation_id=f"1700000000.00000{index}",
                        )
                    )
                    for index, request_id in enumerate(request_ids, start=2)
                )
            )

            assert work_items.calls.count("acquire") == len(request_ids)
            assert sorted(work_items.started) == sorted(request_ids)
            assert work_items.calls.count("finish") == len(request_ids)

    asyncio.run(exercise())
