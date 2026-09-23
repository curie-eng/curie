"""A factory work-item execution checks out its WorkItem repository (#2992).

The objective of a factory execution is an issue URL. That URL is the
assignment, not a repository picker: the API bound the repository to the
WorkItem from the signed delivery, and the acquire grant carries it. An
ordinary chat message naming the same issue URL still selects nothing.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from aci_protocol import Final, QueuedTurn, ReplyHandle, SessionStatus, TurnSource
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.workitem_dispatch import WorkItemAcquireGrant, WorkItemStartGrant

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

    def __getattr__(self, name: str):  # type: ignore[no-untyped-def]
        async def record(*_args: object, **_kwargs: object) -> None:
            self.calls.append(name)

        return record


def _turn(event_id: str, text: str) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id,
        conversation_id="1700000000.000001",
        author="U0EXAMPLE1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel=CHANNEL, placeholder=None),
        received_at="2026-09-23T01:00:00+00:00",
        source=TurnSource.SLACK,
    )


def test_work_item_execution_checks_out_the_work_item_repository(make_harness) -> None:
    async def exercise() -> None:
        async with make_harness(binding=_Binding(), workspace_factory=_Workspace) as h:
            work_items = _WorkItems()
            h.kernel._work_items = work_items
            h.runner.default_script = [Final(text="Working.", status=SessionStatus.DONE)]
            request_id = uuid.uuid4()

            await h.kernel.process_event(
                _turn(f"work-item-{request_id}-execute-1", f"Resolve {ISSUE_URL}")
            )

            assert "acquire" in work_items.calls
            assert h.kernel._workspace.selections == [WORK_ITEM_REPO]
            assert h.kernel._workspace.claimed == [WORK_ITEM_REPO]

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
