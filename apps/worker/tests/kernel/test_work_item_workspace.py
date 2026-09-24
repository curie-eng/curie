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
from aci_protocol import (
    ErrorEvent,
    Final,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    TurnSource,
)
from channel_protocol.reply import ReplyAck, ReplyEvent
from curie_worker.approvals import ApprovalRequest, CreatedApproval
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.config import WorkerConfig
from curie_worker.kernel import ThreadBusyError
from curie_worker.reply_sink import ReplySink, TargetRoute, build_reply_sink
from curie_worker.workitem_dispatch import (
    WorkItemAcquireGrant,
    WorkItemConflict,
    WorkItemStartGrant,
)
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
        if kwargs.get("replace_handle") is not None:
            handoff = self.substrate.handoff(  # type: ignore[attr-defined]
                str(kwargs["thread_key"]),
                expected=kwargs["replace_handle"],
                env=dict(kwargs.get("env") or {}),
                workspace_repo=kwargs.get("repo_full_name"),
                agent_name=kwargs.get("agent_name"),
            )
            return SimpleNamespace(handle=handoff, prepared=None)
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
        self.finishes: list[dict[str, object]] = []

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

    async def finish(self, _request_id: uuid.UUID, **kwargs: object) -> None:
        self.calls.append("finish")
        self.finishes.append(kwargs)
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


def test_credit_exhausted_escalation_finishes_with_its_cause_and_message(
    make_harness,
) -> None:
    """#3073: the issue names the real cause, not ``runner_escalated``."""

    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            message = "model error: unknown: API Error: 402 This request requires more credits"
            h.runner.default_script = [
                ErrorEvent(message=message, classification="model-credit-exhausted"),
                Final(text="", status=SessionStatus.CLASSIFIED_FAILURE),
            ]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert work_items.calls.count("finish") == 1
            finish = work_items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "model_credit_exhausted"
            assert finish["detail"] == message

    asyncio.run(exercise())


class _PublicationPendingWorkItems(_WorkItems):
    """The publication loop, not this finish, owns the WorkItem terminus."""

    async def finish(self, _request_id: uuid.UUID, **_: object) -> None:
        self.calls.append("finish")
        raise WorkItemConflict("publication_pending")


@pytest.mark.parametrize("publication_pending", [False, True])
def test_finished_work_item_deletes_its_sandbox_claim(
    make_harness, publication_pending: bool
) -> None:
    """A WorkItem run that settles terminally leaves no claim or route behind (#3075)."""

    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            work_items = _PublicationPendingWorkItems() if publication_pending else _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [
                TextDelta(text="Working. "),
                Final(text="Working. Done.", status=SessionStatus.DONE),
            ]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert "finish" in work_items.calls
            assert h.fake_k8s.deleted_claims
            assert h.fake_k8s.claims == {}

    asyncio.run(exercise())


def test_approval_hold_keeps_its_sandbox_claim(make_harness) -> None:
    """Awaiting approval is not a terminus: the resume turn still needs the route."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, approvals=_Approvals()
        ) as h:
            h.kernel._work_items = _WorkItems()
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
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert len(h.fake_k8s.claims) == 1
            assert h.fake_k8s.deleted_claims == []

    asyncio.run(exercise())


@pytest.mark.parametrize("path", ["owner_stop", "reconciler_terminate"])
def test_cancelled_work_item_deletes_its_suspended_sandbox_claim(
    make_harness, path: str
) -> None:
    """A run held for approval idles into a SUSPENDED route, then is cancelled.
    Both termination paths delete the suspended claim and drop the route, so
    the orphan reaper's routed-claim skip cannot strand it (#3075)."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, approvals=_Approvals()
        ) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [
                Final(
                    text="Requesting approval.",
                    status=SessionStatus.AWAITING_APPROVAL,
                    approval_summary="Run the requested publication",
                    approval_gate_kind="permission",
                    approval_granted_tool="Bash",
                )
            ]
            request_id = uuid.uuid4()
            execute = _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            await h.kernel.process_event(execute)
            [(thread_key, run)] = list(h.kernel._held_work_items.items())
            await asyncio.to_thread(h.substrate.suspend, thread_key, history_ref="hist-1")
            assert len(h.fake_k8s.claims) == 1

            if path == "owner_stop":
                await h.kernel._stop_owned_work_item(thread_key, run)
            else:

                async def get_request(_request_id: uuid.UUID) -> object:
                    return SimpleNamespace(
                        runtime_claim_name=run.claim_name,
                        runtime_sandbox_name=run.sandbox_name,
                    )

                work_items.get_request = get_request  # type: ignore[attr-defined]
                await h.kernel._terminate_work_item(execute, request_id)

            assert "record_termination" in work_items.calls
            assert h.fake_k8s.claims == {}
            assert h.substrate._affinity.get(thread_key) is None

    asyncio.run(exercise())


def test_work_item_boot_env_carries_the_configured_turn_budget(make_harness) -> None:
    """#3071: a factory execution runs with the worker's work-item turn budget,
    not the runner's short chat default."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, work_item_max_turns=5
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 1 and envs[0] is not None
            assert envs[0].get("CURIE_MAX_TURNS") == "5"

    asyncio.run(exercise())


def test_ordinary_chat_boot_env_carries_no_turn_budget(make_harness) -> None:
    """#3071: only a work-item delivery raises the turn budget; chat keeps the
    runner default by carrying no CURIE_MAX_TURNS at all."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, work_item_max_turns=5
        ) as h:
            h.runner.default_script = [Final(text="Noted.", status=SessionStatus.DONE)]

            await h.kernel.process_event(
                _turn(f"slack-{uuid.uuid4()}", f"Please look at {ISSUE_URL}")
            )

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 1 and envs[0] is not None
            assert "CURIE_MAX_TURNS" not in envs[0]

    asyncio.run(exercise())


def test_work_item_replaces_a_chat_sandbox_booted_without_its_turn_budget(
    make_harness,
) -> None:
    """#3071: CURIE_MAX_TURNS binds at boot, so a work item on a thread whose
    live sandbox was claimed by chat gets a fresh runner carrying its budget."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, work_item_max_turns=5
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]

            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))
            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 2
            assert "CURIE_MAX_TURNS" not in (envs[0] or {})
            assert (envs[1] or {}).get("CURIE_MAX_TURNS") == "5"

    asyncio.run(exercise())


def test_chat_replaces_a_work_item_sandbox_booted_with_the_factory_budget(
    make_harness,
) -> None:
    """#3071: an ordinary turn after a work item on the same thread must not
    inherit the factory turn budget from the reused sandbox."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, work_item_max_turns=5
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]

            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )
            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 2
            assert (envs[0] or {}).get("CURIE_MAX_TURNS") == "5"
            assert "CURIE_MAX_TURNS" not in (envs[1] or {})

    asyncio.run(exercise())


def _fail_settled_release(h: object) -> None:
    """Make the settled work item's best-effort sandbox release fail (#3075).

    A settled work item normally deletes its claim, so the only way its live,
    factory-budget sandbox survives to meet the next delivery on the thread is
    a failed release, which the kernel logs and tolerates. That surviving
    route is exactly what the #3071 turn budget fence guards.
    """

    def release(_thread_key: str) -> bool:
        raise RuntimeError("control plane unavailable")

    h.substrate.release = release  # type: ignore[attr-defined]


def test_consecutive_work_items_with_the_same_budget_adopt_the_sandbox(
    make_harness,
) -> None:
    """#3071: a matching turn budget is no reason to replace a live runner.
    The first work item's release fails, so its live sandbox survives, and the
    next work item with the same budget adopts it instead of claiming again."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, work_item_max_turns=5
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            _fail_settled_release(h)

            for _ in range(2):
                await h.kernel.process_event(
                    _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
                )

            envs = h.fake_k8s.claim_envs
            assert len(envs) == 1
            assert (envs[0] or {}).get("CURIE_MAX_TURNS") == "5"
            assert len(h.runner.opened) == 2

    asyncio.run(exercise())


def test_chat_steers_a_live_work_item_turn_instead_of_replacing_it(make_harness) -> None:
    """#3071: the turn budget fence applies only to a new turn. A message that
    arrives while the work item turn is live steers it (one live session)."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, work_item_max_turns=5
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            _fail_settled_release(h)

            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )
            assert len(h.fake_k8s.claims) == 1
            h.runner.turn_active = True
            await h.kernel.process_event(_turn(f"slack-{uuid.uuid4()}", "hello there"))

            assert h.runner.steers == ["hello there"]
            assert len(h.fake_k8s.claim_envs) == 1

    asyncio.run(exercise())


def test_chat_never_opens_a_turn_on_a_runner_with_the_factory_budget(
    make_harness,
) -> None:
    """#3071 finish race: liveness said busy, but the work item turn ended
    before the steer (409). The chat turn must not open on the old runner with
    the factory budget; it is retried and the retry replaces the runner."""

    async def exercise() -> None:
        async with make_harness(
            binding=_Binding(), workspace_factory=_Workspace, work_item_max_turns=5
        ) as h:
            h.kernel._work_items = _WorkItems()
            h.runner.default_script = [Final(text="Done.", status=SessionStatus.DONE)]
            _fail_settled_release(h)

            await h.kernel.process_event(
                _turn(f"work-item-{uuid.uuid4()}-execute-1", f"Resolve {ISSUE_URL}")
            )
            assert len(h.fake_k8s.claims) == 1
            opened_before = list(h.runner.opened)

            async def active_then_finished(*_args: object, **_kwargs: object) -> bool:
                h.runner.turn_active = False  # the turn ends before the steer lands
                return True

            h.kernel._turn_active = active_then_finished  # type: ignore[method-assign]
            chat = _turn(f"slack-{uuid.uuid4()}", "hello there")
            with pytest.raises(ThreadBusyError):
                await h.kernel.process_event(chat)

            assert h.runner.opened == opened_before
            assert len(h.fake_k8s.claim_envs) == 1

            del h.kernel._turn_active
            await h.kernel.process_event(chat)
            envs = h.fake_k8s.claim_envs
            assert len(envs) == 2
            assert "CURIE_MAX_TURNS" not in (envs[1] or {})

    asyncio.run(exercise())
