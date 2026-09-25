"""Where the attachment lane meets the kernel's claim and reap paths (#2567, S3).

Two joins, both against the real Valkey and the real G1 substrate (only Slack
and the model are faked, as everywhere in this suite):

1. **Claim.** A turn carrying ``QueuedTurn.attachments`` must have them resolved
   into a signed reference BEFORE the sandbox is claimed, and that reference must
   arrive in the claim env. A turn carrying none must produce the claim it
   produces today, without the lane being consulted at all -- that is the
   common case and the regression this file guards hardest.

2. **Reap.** The attachment retention ledger is a SIBLING of the workspace one
   and is swept from the SAME ``reap_orphans`` tick, reusing the per-thread lock
   fence. No second scheduler, and no overloading the thread-ownership authority
   with a foreign object class. ``test_kernel.py``'s
   ``test_workspace_reaper_holds_the_route_lock_during_exact_ledger_recheck``
   is the shape the third test here mirrors, because the reason is identical:
   the object deletes between ``begin`` and ``finish`` can outlive the original
   lease, so the lock must be renewed across the whole critical section.

The lane itself is a test double here on purpose. Its own behavior -- the bounded
download, the digest, the refusal, the prefixes -- is pinned in
``tests/test_attachments.py`` and ``tests/test_attachment_retention.py`` against
a conforming object-store port. What is under test in this file is only the
kernel's wiring: what it calls, when, and what it puts in the claim env.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
import time
import uuid
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import pytest
from aci_protocol import (
    Attachment,
    Final,
    QueuedTurn,
    ReplyHandle,
    SessionStatus,
    TextDelta,
    TurnSource,
)
from curie_worker.attachments import AttachmentResolutionError
from curie_worker.behaviorpacks import BehaviorPacks
from curie_worker.kernel import Kernel
from curie_worker.sandbox import SuspendedThreadError
from curie_worker.workspace import WORKSPACE_REF_ENV, WORKSPACE_SHA256_ENV

DONE = SessionStatus.DONE
ATTACHMENTS_REF_ENV = "CURIE_ATTACHMENTS_REF"
REF_VALUE = "opaque-presigned-attachment-reference"
WORKSPACE_REF_VALUE = "opaque-presigned-workspace-reference"
ACTIVE_FILE_REPLY = (
    "I cannot add a file while the current reply is still running. "
    "Please send the whole message again after that reply finishes. "
    "The text of this message was not processed."
)
UNSAFE_FILE_REPLY = (
    "I could not safely add a file to this existing thread. "
    "Please start a new thread with the file attached. "
    "The text of this message was not processed."
)
CHANGED_FILE_REPLY = (
    "I could not add the file because the thread changed while I was fetching it. "
    "Please send the whole message again. "
    "The text of this message was not processed."
)
WORKSPACE_FILE_REPLY = (
    "I can't add a file to this thread because its repository workspace is already open. "
    "Please start a new thread with the file attached. "
    "The text of this message was not processed."
)
REPOSITORY_FILE_REPLY = (
    "I cannot add a file to an existing thread when a repository is selected for it. "
    "Please start a new thread with the file attached. "
    "The text of this message was not processed."
)
WORKSPACES_OFF_REPLY = (
    "Repository workspaces are turned off on this installation, so no repository "
    "was attached and no work started. An operator can turn them on with "
    "agentSandbox.runner.workspace.enabled in the chart values "
    "(CURIE_WORKSPACE_ENABLED on the worker)."
)


def _qevent(
    text: str,
    *,
    thread: str = "th-att",
    attachments: Sequence[Attachment] = (),
    event_id: str | None = None,
    placeholder: str = "p-1",
) -> QueuedTurn:
    return QueuedTurn(
        event_id=event_id or f"ev-{thread}-{len(text)}-{len(attachments)}",
        conversation_id=thread,
        author="U1",
        text=text,
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder=placeholder),
        received_at="2026-07-05T00:00:00+00:00",
        source=TurnSource.SLACK,
        attachments=list(attachments),
    )


def _thread_key(thread: str) -> str:
    return f"slack:C1:{thread}"


async def _wait_until(pred: Callable[[], bool], what: str, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {what}")


class _FakeAttachmentLane:
    """The kernel-facing surface of ``AttachmentCoordinator``, recorded.

    ``resolve`` returns an object with the one method the claim path needs
    (``claim_env``), so a kernel that resolves but forgets to merge the result
    into the boot env fails the claim-env assertion rather than passing on the
    strength of the call alone.
    """

    def __init__(
        self,
        *,
        ref_value: str = REF_VALUE,
        error: AttachmentResolutionError | None = None,
        resolve_entered: threading.Event | None = None,
        resolve_gate: threading.Event | None = None,
    ) -> None:
        self.resolve_calls: list[dict[str, Any]] = []
        self.discard_calls: list[dict[str, Any]] = []
        self._ref_value = ref_value
        self._error = error
        self._resolve_entered = resolve_entered
        self._resolve_gate = resolve_gate
        self.prepared = _Prepared(
            {ATTACHMENTS_REF_ENV: self._ref_value},
            object_keys=("attachments/test/prepared.bin",),
        )

    def resolve(
        self,
        *,
        thread_key: str,
        agent_id: str | None = None,
        attachments: Sequence[Attachment],
        **extra: Any,
    ) -> Any:
        self.resolve_calls.append(
            {
                "thread_key": thread_key,
                "agent_id": agent_id,
                "names": [ref.name for ref in attachments],
                "ids": [ref.id for ref in attachments],
                "extra": extra,
            }
        )
        if self._resolve_entered is not None:
            self._resolve_entered.set()
        if self._resolve_gate is not None:
            assert self._resolve_gate.wait(timeout=5.0), "attachment resolve gate timed out"
        if self._error is not None:
            raise self._error
        return self.prepared

    def discard_prepared(self, *, thread_key: str, prepared: Any) -> None:
        """Record exact prepared cleanup through the production signature."""

        self.discard_calls.append({"thread_key": thread_key, "prepared": prepared})

    # The reap half. Unused by the claim tests; present because the kernel holds
    # ONE optional collaborator for this lane, exactly as it holds one for the
    # workspace lane.
    def enumerate_expired(self) -> list[str]:
        return []

    def begin_expired_reap(self, thread_key: str) -> object | None:
        return None

    def finish_expired_reap(self, candidate: object) -> bool:
        return True


class _Prepared:
    def __init__(
        self,
        env: dict[str, str],
        *,
        object_keys: tuple[str, ...] = (),
    ) -> None:
        self._env = env
        self.refs: tuple[Any, ...] = ()
        self.object_keys = object_keys

    def claim_env(self) -> dict[str, str]:
        return dict(self._env)


class _WorkspaceResolved:
    def __init__(self, deployment_id: uuid.UUID, *, workspace_enabled: bool = True) -> None:
        self.agent_id = uuid.uuid4()
        self.agent_name = "test-agent"
        self.deployment_id = deployment_id
        self.workspace_enabled = workspace_enabled
        self.endpoint: str | None = None
        self.adapter: str | None = None


class _WorkspaceBinding:
    def __init__(self, deployment_id: uuid.UUID, *, workspace_enabled: bool = True) -> None:
        self.resolved = _WorkspaceResolved(
            deployment_id,
            workspace_enabled=workspace_enabled,
        )

    async def resolve(self, _kind: str, _adapter: str | None, _channel: str) -> _WorkspaceResolved:
        return self.resolved

    def boot_env(
        self,
        _resolved: object,
        thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
    ) -> dict[str, str]:
        return {
            "CURIE_SESSION_ID": f"session:{thread_key}",
            "CURIE_RUNNER_TOKEN": "workspace-test-token",
        }

    def packs_for(self, _resolved: object) -> BehaviorPacks:
        return BehaviorPacks()


class _HistoryBinding(_WorkspaceBinding):
    def boot_env(
        self,
        resolved: object,
        thread_key: str,
        *,
        kind: str | None = None,
        address: str | None = None,
    ) -> dict[str, str]:
        return {
            **super().boot_env(
                resolved,
                thread_key,
                kind=kind,
                address=address,
            ),
            "CURIE_HISTORY_REF": f"history:{thread_key}",
        }


class _WorkspaceProbe:
    """Small coordinator double that preserves a visible ownership record."""

    def __init__(self, substrate: Any, *, selected_repo: str | None = "acme/example") -> None:
        self.substrate = substrate
        self.selected_repo = selected_repo
        self.ledger = b'{"base":"unchanged"}'
        self.select_calls: list[dict[str, Any]] = []
        self.claim_calls: list[dict[str, Any]] = []
        self.touch_calls: list[tuple[str, int]] = []
        self.forbid_retained_access = False

    def select_repository(self, **kwargs: Any) -> str | None:
        self.select_calls.append(dict(kwargs))
        return self.selected_repo

    def claim_or_resume_with_handle(self, **kwargs: Any) -> object:
        if self.forbid_retained_access:
            raise AssertionError("a retained workspace file turn prepared a new workspace")
        self.claim_calls.append(dict(kwargs))
        env = {
            **dict(kwargs.get("env") or {}),
            WORKSPACE_REF_ENV: WORKSPACE_REF_VALUE,
            WORKSPACE_SHA256_ENV: "a" * 64,
        }
        thread_key = str(kwargs["thread_key"])
        agent_name = kwargs.get("agent_name")
        # #2739: the coordinator forwards the kernel's fresh-only fence.
        fence = {"fresh_only": kwargs["fresh_only"]} if "fresh_only" in kwargs else {}
        try:
            handle = self.substrate.claim(
                thread_key,
                env=env,
                agent_name=agent_name,
                workspace_repo=kwargs.get("repo_full_name"),
                **fence,
            )
        except SuspendedThreadError:
            handle = self.substrate.resume(
                thread_key,
                env=env,
                agent_name=agent_name,
                workspace_repo=kwargs.get("repo_full_name"),
            )
        return SimpleNamespace(handle=handle, prepared=SimpleNamespace(claim_env=lambda: env))

    def touch(self, thread_key: str, *, ttl_seconds: int) -> bool:
        if self.forbid_retained_access:
            raise AssertionError("a retained workspace file turn touched workspace ownership")
        self.touch_calls.append((thread_key, ttl_seconds))
        return True

    def current(self, _thread_key: str) -> object:
        raise AssertionError("a retained workspace file turn read workspace ownership")

    def enumerate_expired(self) -> list[str]:
        return []

    def begin_expired_reap(self, _thread_key: str) -> None:
        return None

    def finish_expired_reap(self, _candidate: object) -> bool:
        return True


def _claim_env(h: Any) -> dict[str, str]:
    envs = [env for env in h.fake_k8s.claim_envs if env is not None]
    assert envs, "no sandbox was claimed"
    return envs[-1]


def _authenticate_route(h: Any, thread: str) -> None:
    thread_key = _thread_key(thread)
    record = h.substrate._affinity.get(thread_key)
    assert record is not None
    h.substrate._affinity.replace(
        thread_key,
        replace(record, handle=replace(record.handle, token="retained-test-token")),
        ttl_seconds=60,
    )


def test_a_turn_with_no_attachments_claims_exactly_the_env_it_claims_today(
    make_harness,
) -> None:
    """The common case, untouched: the lane is not even consulted.

    Asserted two ways so the guard cannot be satisfied by an empty-valued entry:
    the lane records no ``resolve`` call, and the claim env carries no
    attachment key at all -- not the key with an empty value, which an init
    container would read as "there is work here".

    The signature check ahead of it pins the wiring shape: the lane is an
    OPTIONAL injected collaborator named ``attachments``, exactly as the
    workspace lane is ``workspace``, so a deployment without it runs unchanged
    instead of a turn discovering a missing attribute. The tests below reach the
    field as ``_attachments`` to match ``_workspace``.
    """

    assert "attachments" in inspect.signature(Kernel.__init__).parameters, (
        "Kernel must take the attachment lane as an optional keyword collaborator, "
        "the way it takes `workspace`"
    )

    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.runner.default_script = [Final(text="ok", status=DONE)]

            await h.kernel.process_event(_qevent("plain question", thread="tNoFiles"))

            assert lane.resolve_calls == [], (
                "a turn with no attachments must not touch the attachment lane"
            )
            env = _claim_env(h)
            assert ATTACHMENTS_REF_ENV not in env
            assert h.sink.last_text == "ok"

    asyncio.run(go())


def test_a_resolved_attachment_reference_reaches_the_claim_env(make_harness) -> None:
    """Resolved before the claim, and carried on it.

    Order is the load-bearing part: the reference has to exist before
    ``substrate.claim`` runs, because the claim env is how it is delivered.
    Resolving after the claim would boot the sandbox and then have nowhere to
    put the capability.
    """

    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.runner.default_script = [Final(text="read it", status=DONE)]

            await h.kernel.process_event(
                _qevent(
                    "what does this say?",
                    thread="tOneFile",
                    attachments=[Attachment(id="F1", name="report.csv", mime_type="text/csv")],
                )
            )

            assert len(lane.resolve_calls) == 1, lane.resolve_calls
            call = lane.resolve_calls[0]
            assert call["thread_key"] == _thread_key("tOneFile")
            assert call["ids"] == ["F1"]
            assert call["names"] == ["report.csv"]
            assert _claim_env(h)[ATTACHMENTS_REF_ENV] == REF_VALUE

    asyncio.run(go())


def test_a_file_on_an_idle_retained_thread_replaces_the_claim_and_reaches_runner_two(
    make_harness,
    monkeypatch,
) -> None:
    """The live defect: claim env cannot be injected into an adopted sandbox."""

    async def go() -> None:
        binding = _HistoryBinding(uuid.uuid4(), workspace_enabled=False)
        async with make_harness(binding=binding, per_sandbox_runners=2) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate, selected_repo=None)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            for runner in h.runners.values():
                runner.default_script = [Final(text="ok", status=DONE)]

            await h.kernel.process_event(_qevent("first", thread="tRetained"))
            first_handle = h.substrate.lookup(_thread_key("tRetained"))
            assert first_handle is not None
            assert first_handle.token == "workspace-test-token"
            probe_budgets: list[float | None] = []
            real_status = h.kernel._runner.status  # noqa: SLF001

            async def record_status(*args: Any, **kwargs: Any) -> dict[str, Any]:
                probe_budgets.append(kwargs.get("remaining_s"))
                return await real_status(*args, **kwargs)

            monkeypatch.setattr(h.kernel._runner, "status", record_status)  # noqa: SLF001

            first_claim = next(iter(h.fake_k8s.claims))
            first_sandbox = h.fake_k8s.claims[first_claim].sandbox_name
            first_port = h.fake_k8s.assigned_ports[first_sandbox]
            first_runner = h.runners[first_port]
            assert first_runner.opened == ["first"]
            assert lane.resolve_calls == []

            await h.kernel.process_event(
                _qevent(
                    "read the file",
                    thread="tRetained",
                    attachments=[
                        Attachment(
                            id="F2",
                            name="thread-report.pptx",
                            mime_type=(
                                "application/vnd.openxmlformats-officedocument."
                                "presentationml.presentation"
                            ),
                        )
                    ],
                )
            )

            assert len(h.fake_k8s.claim_envs) == 2, h.fake_k8s.claim_envs
            assert h.fake_k8s.claim_envs[1] is not None
            assert h.fake_k8s.claim_envs[1][ATTACHMENTS_REF_ENV] == REF_VALUE
            assert first_claim not in h.fake_k8s.claims
            assert len(h.fake_k8s.claims) == 1

            second_claim = next(iter(h.fake_k8s.claims))
            second_sandbox = h.fake_k8s.claims[second_claim].sandbox_name
            second_port = h.fake_k8s.assigned_ports[second_sandbox]
            assert second_claim != first_claim
            assert second_port != first_port
            second_handle = h.substrate.lookup(_thread_key("tRetained"))
            assert second_handle is not None
            assert second_handle.generation == first_handle.generation + 1
            assert second_handle.token == "workspace-test-token"
            assert first_runner.opened == ["first"]
            assert h.runners[second_port].opened == ["read the file"]
            assert h.runner.opened == []
            assert len(lane.resolve_calls) == 1
            assert lane.resolve_calls[0]["ids"] == ["F2"]
            assert lane.discard_calls == []
            assert probe_budgets[:2] == [5.0, 5.0]

    asyncio.run(go())


def test_deployed_nonworkspace_thread_with_deployment_id_handoffs_the_file(
    make_harness,
) -> None:
    """A deployment id alone does not mean the retained route owns a workspace."""

    async def go() -> None:
        binding = _HistoryBinding(uuid.uuid4(), workspace_enabled=False)
        async with make_harness(binding=binding, per_sandbox_runners=2) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate, selected_repo=None)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            for runner in h.runners.values():
                runner.default_script = [Final(text="ok", status=DONE)]

            await h.kernel.process_event(_qevent("first", thread="tDeployedGeneric"))
            first = h.substrate.lookup(_thread_key("tDeployedGeneric"))
            assert first is not None
            assert first.workspace_repo is None
            assert first.session_id == "session:slack:C1:tDeployedGeneric"
            assert first.history_ref == "history:slack:C1:tDeployedGeneric"
            first_port = h.fake_k8s.assigned_ports[first.sandbox_name]

            await h.kernel.process_event(
                _qevent(
                    "read the file",
                    thread="tDeployedGeneric",
                    attachments=[Attachment(id="F2B", name="deployed.pptx")],
                )
            )

            second = h.substrate.lookup(_thread_key("tDeployedGeneric"))
            assert second is not None
            assert second.claim_name != first.claim_name
            assert second.workspace_repo is None
            assert second.generation == first.generation + 1
            assert second.session_id == first.session_id
            assert second.history_ref == first.history_ref
            assert h.fake_k8s.claim_envs[-1] is not None
            assert h.fake_k8s.claim_envs[-1][ATTACHMENTS_REF_ENV] == REF_VALUE
            assert h.fake_k8s.claim_envs[-1]["CURIE_SESSION_ID"] == first.session_id
            assert h.fake_k8s.claim_envs[-1]["CURIE_HISTORY_REF"] == first.history_ref
            second_port = h.fake_k8s.assigned_ports[second.sandbox_name]
            assert h.runners[first_port].opened == ["first"]
            assert h.runners[second_port].opened == ["read the file"]
            assert len(workspace.select_calls) == 2
            assert workspace.claim_calls == []
            assert lane.discard_calls == []

    asyncio.run(go())


def test_repository_named_on_a_generic_retained_file_turn_is_refused_before_resolve(
    make_harness,
) -> None:
    async def go() -> None:
        binding = _WorkspaceBinding(uuid.uuid4(), workspace_enabled=False)
        async with make_harness(binding=binding) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate, selected_repo=None)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            h.runner.default_script = [Final(text="ok", status=DONE)]

            await h.kernel.process_event(_qevent("first", thread="tNamedRepository"))
            before = h.substrate.lookup(_thread_key("tNamedRepository"))
            assert before is not None and before.workspace_repo is None
            file_event = _qevent(
                "Read https://github.com/acme-corp/acme-bot",
                thread="tNamedRepository",
                event_id="named-repository-file",
                placeholder="p-file",
                attachments=[Attachment(id="F2C", name="repository.txt")],
            )

            await h.kernel.process_event(file_event)

            assert h.substrate.lookup(_thread_key("tNamedRepository")) == before
            assert lane.resolve_calls == []
            assert lane.discard_calls == []
            assert h.runner.opened == ["first"]
            assert len(workspace.select_calls) == 2
            file_updates = [
                text for _channel, ref, text in h.sink.updates if ref == "p-file"
            ]
            assert file_updates[-1] == REPOSITORY_FILE_REPLY
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))

    asyncio.run(go())


def test_repository_named_with_workspace_coordinator_off_keeps_main_refusal(
    make_harness,
) -> None:
    async def go() -> None:
        binding = _WorkspaceBinding(uuid.uuid4(), workspace_enabled=False)
        async with make_harness(binding=binding) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            thread_key = _thread_key("tWorkspaceOff")
            retained = await asyncio.to_thread(
                h.substrate.claim,
                thread_key,
                env={
                    "CURIE_SESSION_ID": "session:slack:C1:tWorkspaceOff",
                    "CURIE_HISTORY_REF": "history:slack:C1:tWorkspaceOff",
                    "CURIE_RUNNER_TOKEN": "workspace-off-token",
                },
                agent_name="test-agent",
            )
            assert h.kernel._workspace is None  # noqa: SLF001
            file_event = _qevent(
                "Read https://github.com/acme-corp/acme-bot",
                thread="tWorkspaceOff",
                event_id="workspace-off-file",
                placeholder="p-file",
                attachments=[Attachment(id="F2D", name="workspace-off.txt")],
            )

            await h.kernel.process_event(file_event)

            after = h.substrate.lookup(thread_key)
            assert after == retained
            assert after is not None and after.generation == retained.generation
            assert lane.resolve_calls == []
            assert lane.discard_calls == []
            assert h.runner.opened == []
            file_updates = [
                text for _channel, ref, text in h.sink.updates if ref == "p-file"
            ]
            assert file_updates[-1] == WORKSPACES_OFF_REPLY
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))
            completions = [
                item for item in h.sink.completions if item.event_id == file_event.event_id
            ]
            assert len(completions) == 1
            assert completions[0].outcome == "delivered"

    asyncio.run(go())


def test_sticky_repository_on_generic_retained_route_refuses_before_resolve(
    make_harness,
) -> None:
    async def go() -> None:
        binding = _WorkspaceBinding(uuid.uuid4(), workspace_enabled=False)
        async with make_harness(binding=binding) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate, selected_repo="acme/example")
            h.kernel._workspace = workspace  # type: ignore[assignment]
            thread_key = _thread_key("tStickyRepository")
            retained = await asyncio.to_thread(
                h.substrate.claim,
                thread_key,
                env={
                    "CURIE_SESSION_ID": "session:slack:C1:tStickyRepository",
                    "CURIE_HISTORY_REF": "history:slack:C1:tStickyRepository",
                    "CURIE_RUNNER_TOKEN": "sticky-repository-token",
                },
                agent_name="test-agent",
            )
            assert retained.workspace_repo is None
            file_event = _qevent(
                "read this",
                thread="tStickyRepository",
                event_id="sticky-repository-file",
                placeholder="p-file",
                attachments=[Attachment(id="F2E", name="sticky.txt")],
            )

            await h.kernel.process_event(file_event)

            after = h.substrate.lookup(thread_key)
            assert after == retained
            assert after is not None and after.generation == retained.generation
            assert lane.resolve_calls == []
            assert lane.discard_calls == []
            assert h.runner.opened == []
            assert len(workspace.select_calls) == 1
            assert workspace.select_calls[0]["repo_full_name"] is None
            file_updates = [
                text for _channel, ref, text in h.sink.updates if ref == "p-file"
            ]
            assert file_updates[-1] == REPOSITORY_FILE_REPLY
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))

    asyncio.run(go())


@pytest.mark.parametrize(
    "runner_state",
    [
        "active",
        "unreadable",
        "unauthenticated",
        "nondurable",
        "unsafe-status",
        "malformed-status",
    ],
)
def test_a_file_on_a_nonidle_retained_thread_is_terminal_without_resolving(
    make_harness,
    runner_state: str,
    caplog,
) -> None:
    """A runner outside the safe handoff boundary refuses the whole file turn."""

    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            first_task: asyncio.Task[None] | None = None
            hold: asyncio.Event | None = None

            if runner_state == "active":
                hold = asyncio.Event()
                h.runner.hold = hold
                h.runner.default_script = [TextDelta(text="working")]
                h.runner.tail = [Final(text="one", status=DONE)]
                first_task = asyncio.create_task(
                    h.kernel.process_event(_qevent("first", thread="tNonidle"))
                )
                await _wait_until(
                    lambda: h.runner.turn_active,
                    "the retained runner to become active",
                )
                _authenticate_route(h, "tNonidle")
            else:
                h.runner.default_script = [Final(text="one", status=DONE)]
                await h.kernel.process_event(_qevent("first", thread="tNonidle"))
                if runner_state != "unauthenticated":
                    _authenticate_route(h, "tNonidle")
                if runner_state == "unreadable":
                    h.runner.status_fails = True
                elif runner_state == "nondurable":
                    h.runner.history_durable = False
                elif runner_state == "unsafe-status":
                    h.runner.session_status = SessionStatus.AWAITING_APPROVAL.value
                elif runner_state == "malformed-status":
                    h.runner.status_malformed = True

            file_event = _qevent(
                "read this too",
                thread="tNonidle",
                event_id=f"file-{runner_state}",
                placeholder="p-file",
                attachments=[Attachment(id="F3", name="followup.txt")],
            )
            try:
                with caplog.at_level("INFO", logger="curie_worker.kernel"):
                    await h.kernel.process_event(file_event)

                file_updates = [
                    text for _channel, ref, text in h.sink.updates if ref == "p-file"
                ]
                expected_reply = (
                    ACTIVE_FILE_REPLY if runner_state == "active" else UNSAFE_FILE_REPLY
                )
                assert file_updates[-1] == expected_reply
                expected_state = "active" if runner_state == "active" else "unsafe"
                assert f"state={expected_state}" in caplog.text
                assert lane.resolve_calls == []
                assert lane.discard_calls == []
                assert h.runner.steers == []
                assert h.runner.interrupts == 0
                assert len(h.fake_k8s.claim_envs) == 1
                assert await h.async_redis.exists(h.config.done_key(file_event.event_id))
                completions = [
                    item for item in h.sink.completions if item.event_id == file_event.event_id
                ]
                assert len(completions) == 1
                assert completions[0].outcome == "delivered"
            finally:
                h.runner.status_fails = False
                h.runner.history_durable = True
                h.runner.session_status = SessionStatus.IDLE_AWAITING_INPUT.value
                h.runner.status_malformed = False
                if hold is not None:
                    hold.set()
                if first_task is not None:
                    await first_task

    asyncio.run(go())


def test_a_route_created_during_resolve_discards_the_exact_prepared_set(
    make_harness,
) -> None:
    """A phase one route miss cannot replace a route created during fetch."""

    async def go() -> None:
        async with make_harness(per_sandbox_runners=2) as h:
            for runner in h.runners.values():
                runner.default_script = [Final(text="ok", status=DONE)]

            resolve_entered = threading.Event()
            resolve_gate = threading.Event()
            lane = _FakeAttachmentLane(
                resolve_entered=resolve_entered,
                resolve_gate=resolve_gate,
            )
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            file_event = _qevent(
                "read this",
                thread="tChanged",
                event_id="changed-handle-file",
                placeholder="p-file",
                attachments=[Attachment(id="F4", name="changed.txt")],
            )
            task = asyncio.create_task(h.kernel.process_event(file_event))
            try:
                await _wait_until(
                    resolve_entered.is_set,
                    "attachment resolution to start outside the route lock",
                )
                replacement = await asyncio.to_thread(
                    h.substrate.claim,
                    _thread_key("tChanged"),
                    env={"EXTERNAL_REPLACEMENT": "1"},
                )
            finally:
                resolve_gate.set()
            await task

            current = h.substrate.lookup(_thread_key("tChanged"))
            assert current == replacement
            assert lane.discard_calls == [
                {
                    "thread_key": _thread_key("tChanged"),
                    "prepared": lane.prepared,
                }
            ]
            assert all(runner.opened != ["read this"] for runner in h.runners.values())
            file_updates = [
                text for _channel, ref, text in h.sink.updates if ref == "p-file"
            ]
            assert file_updates[-1] == CHANGED_FILE_REPLY
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))

    asyncio.run(go())


def test_phase_two_lock_timeout_discards_the_exact_prepared_set(make_harness) -> None:
    """Prepared bytes do not survive a timeout while reacquiring the route lock."""

    async def go() -> None:
        async with make_harness(
            lock_acquire_timeout_s=0.05,
            lock_poll_interval_s=0.005,
            max_attempts=1,
        ) as h:
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tLockTimeout"))
            _authenticate_route(h, "tLockTimeout")

            resolve_entered = threading.Event()
            resolve_gate = threading.Event()
            lane = _FakeAttachmentLane(
                resolve_entered=resolve_entered,
                resolve_gate=resolve_gate,
            )
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            file_event = _qevent(
                "read this",
                thread="tLockTimeout",
                event_id="phase-two-timeout",
                attachments=[Attachment(id="F5", name="timeout.txt")],
            )
            task = asyncio.create_task(h.kernel.process_event(file_event))
            await _wait_until(
                resolve_entered.is_set,
                "attachment resolution to start outside the route lock",
            )
            token = await h.kernel._lock.acquire(  # noqa: SLF001
                h.config.lock_key(_thread_key("tLockTimeout"))
            )
            try:
                resolve_gate.set()
                await task
            finally:
                await h.kernel._lock.release(  # noqa: SLF001
                    h.config.lock_key(_thread_key("tLockTimeout")),
                    token,
                )

            assert lane.discard_calls == [
                {
                    "thread_key": _thread_key("tLockTimeout"),
                    "prepared": lane.prepared,
                }
            ]
            assert len(h.fake_k8s.claim_envs) == 1
            assert h.runner.opened == ["first"]
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))

    asyncio.run(go())


def test_second_handoff_probe_failure_discards_prepared_and_keeps_old_route(
    make_harness,
    monkeypatch,
) -> None:
    async def go() -> None:
        async with make_harness() as h:
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tSecondProbe"))
            _authenticate_route(h, "tSecondProbe")
            old = h.substrate.lookup(_thread_key("tSecondProbe"))
            assert old is not None

            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            probe_results = iter(("ready", "unsafe"))

            async def probe(*_args: Any, **_kwargs: Any) -> str:
                return next(probe_results)

            monkeypatch.setattr(h.kernel, "_cold_handoff_readiness", probe)
            file_event = _qevent(
                "read this",
                thread="tSecondProbe",
                event_id="second-probe-failure",
                placeholder="p-file",
                attachments=[Attachment(id="F5B", name="probe.txt")],
            )

            await h.kernel.process_event(file_event)

            assert h.substrate.lookup(_thread_key("tSecondProbe")) == old
            assert len(lane.resolve_calls) == 1
            assert lane.discard_calls == [
                {
                    "thread_key": _thread_key("tSecondProbe"),
                    "prepared": lane.prepared,
                }
            ]
            file_updates = [
                text for _channel, ref, text in h.sink.updates if ref == "p-file"
            ]
            assert file_updates[-1] == UNSAFE_FILE_REPLY

    asyncio.run(go())


def test_handoff_bind_failure_discards_prepared_and_keeps_old_route(
    make_harness,
) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=1) as h:
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tHandoffFailure"))
            _authenticate_route(h, "tHandoffFailure")
            old = h.substrate.lookup(_thread_key("tHandoffFailure"))
            assert old is not None
            old_claims = set(h.fake_k8s.claims)

            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.fake_k8s.bind_ready = False
            file_event = _qevent(
                "read this",
                thread="tHandoffFailure",
                event_id="handoff-bind-failure",
                attachments=[Attachment(id="F5C", name="bind.txt")],
            )

            await h.kernel.process_event(file_event)

            assert h.substrate.lookup(_thread_key("tHandoffFailure")) == old
            assert set(h.fake_k8s.claims) == old_claims
            assert len(h.fake_k8s.claim_envs) == 2
            assert len(lane.resolve_calls) == 1
            assert lane.discard_calls == [
                {
                    "thread_key": _thread_key("tHandoffFailure"),
                    "prepared": lane.prepared,
                }
            ]
            assert h.runner.opened == ["first"]
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))

    asyncio.run(go())


def test_cancellation_waits_for_a_successful_handoff_before_cleanup(
    make_harness,
    monkeypatch,
) -> None:
    async def go() -> None:
        async with make_harness(per_sandbox_runners=2) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            thread_key = _thread_key("tCancelledHandoff")
            old = await asyncio.to_thread(
                h.substrate.claim,
                thread_key,
                env={
                    "CURIE_SESSION_ID": "session:slack:C1:tCancelledHandoff",
                    "CURIE_HISTORY_REF": "history:slack:C1:tCancelledHandoff",
                    "CURIE_RUNNER_TOKEN": "retained-test-token",
                },
            )
            bind_entered = threading.Event()
            bind_gate = threading.Event()
            real_get_claim = h.fake_k8s.get_claim

            def pause_candidate_bind(
                claim_name: str, *, request_timeout_seconds: float
            ) -> Any:
                if claim_name != old.claim_name and not bind_gate.is_set():
                    bind_entered.set()
                    assert bind_gate.wait(timeout=5.0), "candidate bind gate timed out"
                return real_get_claim(
                    claim_name, request_timeout_seconds=request_timeout_seconds
                )

            monkeypatch.setattr(h.fake_k8s, "get_claim", pause_candidate_bind)
            qevent = _qevent(
                "read this",
                thread="tCancelledHandoff",
                attachments=[Attachment(id="F5D", name="cancelled.txt")],
            )
            route_task = asyncio.create_task(
                h.kernel._route_attachment_and_start(  # noqa: SLF001
                    qevent,
                    thread_key,
                    h.kernel._to_event(qevent),  # noqa: SLF001
                    {
                        "CURIE_SESSION_ID": old.session_id,
                        "CURIE_HISTORY_REF": old.history_ref,
                        "CURIE_RUNNER_TOKEN": "replacement-test-token",
                    },
                    None,
                    workspace_inference=SimpleNamespace(repo=None),
                )
            )
            try:
                await _wait_until(bind_entered.is_set, "candidate bind to pause")
                route_task.cancel()
                await asyncio.sleep(0)
                route_task.cancel()
            finally:
                bind_gate.set()

            with pytest.raises(asyncio.CancelledError):
                await route_task

            installed = h.substrate.lookup(thread_key)
            assert installed is not None
            assert installed.claim_name != old.claim_name
            assert installed.generation == old.generation + 1
            assert installed.claim_name in h.fake_k8s.claims
            assert old.claim_name not in h.fake_k8s.claims
            assert h.fake_k8s.claim_envs[-1] is not None
            assert h.fake_k8s.claim_envs[-1][ATTACHMENTS_REF_ENV] == REF_VALUE
            assert lane.discard_calls == []

    asyncio.run(go())


def test_cancellation_discards_after_a_definitively_failed_handoff(
    make_harness,
    monkeypatch,
) -> None:
    async def go() -> None:
        async with make_harness(claim_timeout_seconds=0.05) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            thread_key = _thread_key("tCancelledFailedHandoff")
            old = await asyncio.to_thread(
                h.substrate.claim,
                thread_key,
                env={
                    "CURIE_SESSION_ID": "session:slack:C1:tCancelledFailedHandoff",
                    "CURIE_HISTORY_REF": "history:slack:C1:tCancelledFailedHandoff",
                    "CURIE_RUNNER_TOKEN": "retained-test-token",
                },
            )
            h.fake_k8s.bind_ready = False
            bind_entered = threading.Event()
            bind_gate = threading.Event()
            real_get_claim = h.fake_k8s.get_claim

            def pause_candidate_bind(
                claim_name: str, *, request_timeout_seconds: float
            ) -> Any:
                if claim_name != old.claim_name and not bind_gate.is_set():
                    bind_entered.set()
                    assert bind_gate.wait(timeout=5.0), "candidate bind gate timed out"
                return real_get_claim(
                    claim_name, request_timeout_seconds=request_timeout_seconds
                )

            monkeypatch.setattr(h.fake_k8s, "get_claim", pause_candidate_bind)
            qevent = _qevent(
                "read this",
                thread="tCancelledFailedHandoff",
                attachments=[Attachment(id="F5E", name="failed.txt")],
            )
            loop = asyncio.get_running_loop()
            previous_exception_handler = loop.get_exception_handler()
            exception_contexts: list[dict[str, Any]] = []
            loop.set_exception_handler(
                lambda _loop, context: exception_contexts.append(context)
            )
            try:
                route_task = asyncio.create_task(
                    h.kernel._route_attachment_and_start(  # noqa: SLF001
                        qevent,
                        thread_key,
                        h.kernel._to_event(qevent),  # noqa: SLF001
                        {
                            "CURIE_SESSION_ID": old.session_id,
                            "CURIE_HISTORY_REF": old.history_ref,
                            "CURIE_RUNNER_TOKEN": "replacement-test-token",
                        },
                        None,
                        workspace_inference=SimpleNamespace(repo=None),
                    )
                )
                try:
                    await _wait_until(bind_entered.is_set, "candidate bind to pause")
                    route_task.cancel()
                finally:
                    bind_gate.set()

                with pytest.raises(asyncio.CancelledError):
                    await route_task
                await asyncio.sleep(0)
            finally:
                loop.set_exception_handler(previous_exception_handler)

            assert h.substrate.lookup(thread_key) == old
            assert set(h.fake_k8s.claims) == {old.claim_name}
            assert lane.discard_calls == [
                {
                    "thread_key": thread_key,
                    "prepared": lane.prepared,
                }
            ]
            assert exception_contexts == []

    asyncio.run(go())


def test_second_handoff_probe_uses_the_remaining_delivery_budget(
    make_harness,
    monkeypatch,
) -> None:
    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            old = await asyncio.to_thread(
                h.substrate.claim,
                _thread_key("tElapsedProbeBudget"),
                env={
                    "CURIE_SESSION_ID": "session:slack:C1:tElapsedProbeBudget",
                    "CURIE_HISTORY_REF": "history:slack:C1:tElapsedProbeBudget",
                    "CURIE_RUNNER_TOKEN": "retained-test-token",
                },
            )
            probe_budgets: list[float] = []

            async def probe(*_args: Any, **kwargs: Any) -> str:
                probe_budgets.append(kwargs["remaining_s"])
                if len(probe_budgets) == 1:
                    await asyncio.sleep(0.05)
                return "ready"

            monkeypatch.setattr(h.kernel, "_cold_handoff_readiness", probe)
            h.runner.default_script = [Final(text="ok", status=DONE)]
            qevent = _qevent(
                "read this",
                thread="tElapsedProbeBudget",
                attachments=[Attachment(id="F5D", name="budget.txt")],
            )

            routed = await h.kernel._route_attachment_and_start(  # noqa: SLF001
                qevent,
                _thread_key("tElapsedProbeBudget"),
                h.kernel._to_event(qevent),  # noqa: SLF001
                {
                    "CURIE_SESSION_ID": old.session_id,
                    "CURIE_HISTORY_REF": old.history_ref,
                    "CURIE_RUNNER_TOKEN": "replacement-test-token",
                },
                None,
                remaining_s=0.2,
                workspace_inference=SimpleNamespace(repo=None),
            )
            try:
                assert routed.turn is not None
                assert len(probe_budgets) == 2
                assert 0.0 < probe_budgets[1] < probe_budgets[0] <= 0.2
                assert probe_budgets[0] - probe_budgets[1] >= 0.04
            finally:
                if routed.turn is not None:
                    routed.turn.close()

    asyncio.run(go())


def test_claim_state_lookup_failure_preserves_the_original_start_error(
    make_harness,
    monkeypatch,
    caplog,
) -> None:
    async def go() -> None:
        async with make_harness(max_attempts=1) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.runner.event_fail_times = 1
            real_lookup = h.substrate.lookup
            lookup_calls = 0

            def fail_cleanup_lookup(thread_key: str) -> Any:
                nonlocal lookup_calls
                lookup_calls += 1
                if lookup_calls == 4:
                    raise RuntimeError("injected cleanup lookup failure")
                return real_lookup(thread_key)

            monkeypatch.setattr(h.substrate, "lookup", fail_cleanup_lookup)
            file_event = _qevent(
                "read this",
                thread="tLookupFailure",
                event_id="lookup-failure-file",
                attachments=[Attachment(id="F5C", name="lookup.txt")],
            )

            with caplog.at_level("WARNING", logger="curie_worker.kernel"):
                await h.kernel.process_event(file_event)

            assert lookup_calls >= 4
            assert "injected cleanup lookup failure" in caplog.text
            assert "turn start failed" in caplog.text
            assert lane.discard_calls == []
            assert h.substrate.lookup(_thread_key("tLookupFailure")) is not None

    asyncio.run(go())


def test_lock_exit_failure_after_turn_start_unregisters_the_run(
    make_harness,
    monkeypatch,
) -> None:
    async def go() -> None:
        binding = _WorkspaceBinding(uuid.uuid4(), workspace_enabled=False)
        async with make_harness(binding=binding, per_sandbox_runners=2) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate, selected_repo=None)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            for runner in h.runners.values():
                runner.default_script = [Final(text="ok", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tLockExit"))

            real_hold = h.kernel._lock.hold  # noqa: SLF001
            hold_calls = 0

            @asynccontextmanager
            async def fail_second_exit(key: str) -> AsyncIterator[None]:
                nonlocal hold_calls
                hold_calls += 1
                this_call = hold_calls
                async with real_hold(key):
                    yield
                if this_call == 2:
                    raise RuntimeError("injected route lock exit failure")

            monkeypatch.setattr(h.kernel._lock, "hold", fail_second_exit)  # noqa: SLF001
            file_event = _qevent(
                "read this",
                thread="tLockExit",
                event_id="lock-exit-file",
                attachments=[Attachment(id="F5D", name="lock.txt")],
            )

            with pytest.raises(RuntimeError, match="route lock exit failure"):
                await h.kernel.process_event(file_event)

            assert binding.resolved.agent_id not in h.kernel._active_by_agent  # noqa: SLF001

    asyncio.run(go())


def test_zero_probe_budget_leaves_the_retained_file_turn_retryable(make_harness) -> None:
    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            old = await asyncio.to_thread(
                h.substrate.claim,
                _thread_key("tZeroBudget"),
                env={"CURIE_RUNNER_TOKEN": "retained-test-token"},
            )
            qevent = _qevent(
                "read this",
                thread="tZeroBudget",
                attachments=[Attachment(id="F5E", name="budget.txt")],
            )

            with pytest.raises(TimeoutError, match="probe budget is exhausted"):
                await h.kernel._route_attachment_and_start(  # noqa: SLF001
                    qevent,
                    _thread_key("tZeroBudget"),
                    h.kernel._to_event(qevent),  # noqa: SLF001
                    {},
                    None,
                    remaining_s=0.0,
                    workspace_inference=SimpleNamespace(repo=None),
                )

            assert h.substrate.lookup(_thread_key("tZeroBudget")) == old
            assert lane.resolve_calls == []

    asyncio.run(go())


def test_attachment_resolution_failure_is_terminal_without_claim_or_retry(
    make_harness,
    caplog,
) -> None:
    """A deterministic unavailable upload is answered once and never dead lettered."""

    async def go() -> None:
        async with make_harness(max_attempts=3) as h:
            lane = _FakeAttachmentLane(
                error=AttachmentResolutionError(
                    "fetch",
                    "source refused the download",
                )
            )
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            file_event = _qevent(
                "read this",
                thread="tUnavailable",
                event_id="unavailable-file",
                attachments=[Attachment(id="F6", name="unavailable.txt")],
            )

            with caplog.at_level("WARNING", logger="curie_worker.kernel"):
                await h.kernel.process_event(file_event)

            assert len(lane.resolve_calls) == 1
            assert lane.discard_calls == []
            assert h.fake_k8s.claim_envs == []
            assert h.runner.opened == []
            assert h.sink.last_text is not None
            assert "file" in h.sink.last_text.lower()
            assert "source refused the download" not in h.sink.last_text
            assert "source refused the download" in caplog.text
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))
            completions = [
                item for item in h.sink.completions if item.event_id == file_event.event_id
            ]
            assert len(completions) == 1
            assert completions[0].outcome == "delivered"

    asyncio.run(go())


def test_approval_resume_with_a_file_carries_the_reference_to_the_new_claim(
    make_harness,
) -> None:
    """A suspended approval route remains a supported attachment path."""

    async def go() -> None:
        async with make_harness(per_sandbox_runners=2) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            for runner in h.runners.values():
                runner.default_script = [Final(text="continued", status=DONE)]

            thread_key = _thread_key("tApprovalFile")
            old = await asyncio.to_thread(h.substrate.claim, thread_key)
            await asyncio.to_thread(
                h.substrate.suspend,
                thread_key,
                history_ref="history:approval",
            )
            await h.kernel.process_event(
                _qevent(
                    "continue with this file",
                    thread="tApprovalFile",
                    event_id=(
                        "approval-00000000-0000-4000-8000-000000000001-resolved"
                    ),
                    attachments=[Attachment(id="F7", name="approved.txt")],
                )
            )

            assert old.claim_name not in h.fake_k8s.claims
            assert len(h.fake_k8s.claim_envs) == 2
            resumed_env = h.fake_k8s.claim_envs[-1]
            assert resumed_env is not None
            assert resumed_env[ATTACHMENTS_REF_ENV] == REF_VALUE
            assert resumed_env["CURIE_HISTORY_REF"] == "history:approval"
            new_claim = next(iter(h.fake_k8s.claims.values()))
            new_port = h.fake_k8s.assigned_ports[new_claim.sandbox_name]
            assert h.runners[new_port].opened == ["continue with this file"]
            assert len(lane.resolve_calls) == 1
            assert lane.discard_calls == []

    asyncio.run(go())


def test_disabled_lane_does_not_replace_a_retained_claim_for_a_file_turn(
    make_harness,
) -> None:
    """A deployment with attachments disabled keeps its existing text behavior."""

    async def go() -> None:
        async with make_harness(per_sandbox_runners=2) as h:
            assert h.kernel._attachments is None  # noqa: SLF001
            for runner in h.runners.values():
                runner.default_script = [Final(text="text only", status=DONE)]

            await h.kernel.process_event(_qevent("first", thread="tDisabledRetained"))
            first_claim = next(iter(h.fake_k8s.claims))
            first_sandbox = h.fake_k8s.claims[first_claim].sandbox_name
            first_port = h.fake_k8s.assigned_ports[first_sandbox]
            first_envs = list(h.fake_k8s.claim_envs)

            await h.kernel.process_event(
                _qevent(
                    "second",
                    thread="tDisabledRetained",
                    attachments=[Attachment(id="F8", name="ignored.txt")],
                )
            )

            assert set(h.fake_k8s.claims) == {first_claim}
            assert h.fake_k8s.claim_envs == first_envs
            assert h.runners[first_port].opened == ["first", "second"]
            assert sum(len(runner.opened) for runner in h.runners.values()) == 2

    asyncio.run(go())


def test_enabled_lane_does_not_replace_a_retained_claim_without_files(
    make_harness,
) -> None:
    """Attachment free followups never enter the two phase replacement path."""

    async def go() -> None:
        async with make_harness(per_sandbox_runners=2) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            for runner in h.runners.values():
                runner.default_script = [Final(text="plain", status=DONE)]

            await h.kernel.process_event(_qevent("first", thread="tNoFilesRetained"))
            first_claim = next(iter(h.fake_k8s.claims))
            first_sandbox = h.fake_k8s.claims[first_claim].sandbox_name
            first_port = h.fake_k8s.assigned_ports[first_sandbox]
            first_envs = list(h.fake_k8s.claim_envs)

            await h.kernel.process_event(_qevent("second", thread="tNoFilesRetained"))

            assert lane.resolve_calls == []
            assert lane.discard_calls == []
            assert set(h.fake_k8s.claims) == {first_claim}
            assert h.fake_k8s.claim_envs == first_envs
            assert h.runners[first_port].opened == ["first", "second"]
            assert sum(len(runner.opened) for runner in h.runners.values()) == 2

    asyncio.run(go())


def test_retained_workspace_file_turn_refuses_without_mutating_any_owned_state(
    make_harness,
    monkeypatch,
) -> None:
    """The v0.9.1 workspace boundary is visible, terminal, and mutation free."""

    async def go() -> None:
        from curie_dispatcher.queue import to_stream_fields
        from curie_worker.consumer import Consumer
        from curie_worker.delivery_lease import DeliveryLeaseStore

        deployment_id = uuid.uuid4()
        binding = _WorkspaceBinding(deployment_id)
        async with make_harness(binding=binding) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tWorkspaceRetained"))

            thread_key = _thread_key("tWorkspaceRetained")
            route_before = h.substrate._affinity.get(thread_key)  # noqa: SLF001
            assert route_before is not None
            route_bytes = route_before.to_json()
            claims_before = dict(h.fake_k8s.claims)
            envs_before = list(h.fake_k8s.claim_envs)
            ledger_before = workspace.ledger
            workspace.forbid_retained_access = True

            def forbidden_adopt(_thread_key: str) -> object:
                raise AssertionError("a retained workspace file turn called adopt")

            monkeypatch.setattr(h.substrate, "adopt", forbidden_adopt)

            store = DeliveryLeaseStore(h.async_redis, h.config)
            consumer = Consumer(
                redis=h.async_redis,
                kernel=h.kernel,
                config=h.config,
                leases=store,
            )
            await consumer.ensure_group()
            file_event = _qevent(
                "read this",
                thread="tWorkspaceRetained",
                event_id="workspace-retained-file",
                placeholder="p-file",
                attachments=[Attachment(id="F9", name="workspace.txt")],
            )
            await h.async_redis.xadd(h.config.stream, to_stream_fields(file_event))
            rows = await h.async_redis.xreadgroup(
                h.config.consumer_group,
                h.config.consumer_name,
                {h.config.stream: ">"},
                count=1,
            )
            entry_id, fields = rows[0][1][0]
            await consumer._dispatch(entry_id, dict(fields))
            await asyncio.gather(*list(consumer._inflight), return_exceptions=True)

            route_after = h.substrate._affinity.get(thread_key)  # noqa: SLF001
            assert route_after is not None and route_after.to_json() == route_bytes
            assert h.fake_k8s.claims == claims_before
            assert h.fake_k8s.claim_envs == envs_before
            assert workspace.ledger == ledger_before
            assert len(workspace.claim_calls) == 1
            assert lane.resolve_calls == []
            assert lane.discard_calls == []
            assert h.runner.opened == ["first"]
            file_updates = [
                text for _channel, ref, text in h.sink.updates if ref == "p-file"
            ]
            assert file_updates[-1] == WORKSPACE_FILE_REPLY
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))
            assert not await store.is_live(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
            )
            assert not await store.has_state(
                h.config.stream,
                h.config.consumer_group,
                entry_id,
            )
            pending = await h.async_redis.xpending(h.config.stream, h.config.consumer_group)
            assert pending["pending"] == 0

    asyncio.run(go())


def test_fresh_workspace_claim_carries_workspace_and_attachment_references(
    make_harness,
) -> None:
    async def go() -> None:
        binding = _WorkspaceBinding(uuid.uuid4())
        async with make_harness(binding=binding) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            h.runner.default_script = [Final(text="read", status=DONE)]

            await h.kernel.process_event(
                _qevent(
                    "read this",
                    thread="tWorkspaceFresh",
                    attachments=[Attachment(id="F10", name="fresh.txt")],
                )
            )

            env = _claim_env(h)
            assert env[ATTACHMENTS_REF_ENV] == REF_VALUE
            assert env[WORKSPACE_REF_ENV] == WORKSPACE_REF_VALUE
            assert env[WORKSPACE_SHA256_ENV] == "a" * 64
            assert len(workspace.claim_calls) == 1
            assert len(lane.resolve_calls) == 1
            assert lane.discard_calls == []
            assert h.runner.opened == ["read this"]

    asyncio.run(go())


def test_retained_live_workspace_text_turn_preserves_route_and_skips_attachment_lane(
    make_harness,
) -> None:
    async def go() -> None:
        binding = _WorkspaceBinding(uuid.uuid4())
        async with make_harness(binding=binding, per_sandbox_runners=2) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            for runner in h.runners.values():
                runner.default_script = [Final(text="ok", status=DONE)]

            await h.kernel.process_event(_qevent("first", thread="tWorkspaceText"))
            thread_key = _thread_key("tWorkspaceText")
            route_before = h.substrate._affinity.get(thread_key)  # noqa: SLF001
            assert route_before is not None
            route_bytes = route_before.to_json()
            claim_envs_before = list(h.fake_k8s.claim_envs)
            claim_names_before = set(h.fake_k8s.claims)
            first_port = h.fake_k8s.assigned_ports[route_before.handle.sandbox_name]

            await h.kernel.process_event(_qevent("second", thread="tWorkspaceText"))

            route_after = h.substrate._affinity.get(thread_key)  # noqa: SLF001
            assert route_after is not None
            assert route_after.to_json() == route_bytes
            assert set(h.fake_k8s.claims) == claim_names_before
            assert h.fake_k8s.claim_envs == claim_envs_before
            assert len(workspace.claim_calls) == 1
            assert workspace.touch_calls == [(thread_key, h.kernel._route_ttl_seconds)]  # noqa: SLF001
            assert lane.resolve_calls == []
            assert lane.discard_calls == []
            assert h.runners[first_port].opened == ["first", "second"]
            assert sum(len(runner.opened) for runner in h.runners.values()) == 2

    asyncio.run(go())


def test_suspended_workspace_resume_carries_workspace_and_attachment_references(
    make_harness,
) -> None:
    async def go() -> None:
        binding = _WorkspaceBinding(uuid.uuid4())
        async with make_harness(binding=binding) as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            workspace = _WorkspaceProbe(h.substrate)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            thread_key = _thread_key("tWorkspaceSuspended")
            old = await asyncio.to_thread(
                h.substrate.claim,
                thread_key,
                env={
                    WORKSPACE_REF_ENV: WORKSPACE_REF_VALUE,
                    WORKSPACE_SHA256_ENV: "a" * 64,
                },
            )
            await asyncio.to_thread(
                h.substrate.suspend,
                thread_key,
                history_ref="history:workspace",
            )
            h.fake_k8s.claim_envs.clear()
            h.runner.default_script = [Final(text="read", status=DONE)]

            await h.kernel.process_event(
                _qevent(
                    "read this",
                    thread="tWorkspaceSuspended",
                    attachments=[Attachment(id="F11", name="suspended.txt")],
                )
            )

            assert old.claim_name not in h.fake_k8s.claims
            assert len(h.fake_k8s.claim_envs) == 1
            env = _claim_env(h)
            assert env[ATTACHMENTS_REF_ENV] == REF_VALUE
            assert env[WORKSPACE_REF_ENV] == WORKSPACE_REF_VALUE
            assert env[WORKSPACE_SHA256_ENV] == "a" * 64
            assert env["CURIE_HISTORY_REF"] == "history:workspace"
            assert len(workspace.claim_calls) == 1
            assert len(lane.resolve_calls) == 1
            assert lane.discard_calls == []
            assert h.runner.opened == ["read this"]

    asyncio.run(go())


def test_a_turn_with_files_is_answered_normally_when_the_lane_is_switched_off(
    make_harness,
) -> None:
    """The shipped default (``worker.attachments.enabled: false``) end to end.

    Off is a real state, not an absence of tests: ``run.build`` wires no
    coordinator, so ``Kernel._attachments`` is None and a message that DOES
    carry files has to behave exactly as v0.8.8 behaves -- answered, text only,
    files ignored, no error and no attachment key on the claim. The failure this
    guards is an ``assert lane is not None`` or an unguarded attribute reaching
    the turn path: every uploaded file would then dead-letter the turn on a
    deployment that had deliberately switched the feature off.
    """

    async def go() -> None:
        async with make_harness() as h:
            assert h.kernel._attachments is None, (  # noqa: SLF001 -- the state IS the subject
                "the harness pre-wires a lane; this test is about its absence"
            )
            h.runner.default_script = [Final(text="answered without the file", status=DONE)]

            await h.kernel.process_event(
                _qevent(
                    "what does this say?",
                    thread="tLaneOff",
                    attachments=[
                        Attachment(id="F1", name="report.csv", mime_type="text/csv"),
                        Attachment(id="F2", name="report.csv", mime_type="text/csv"),
                    ],
                )
            )

            assert h.sink.last_text == "answered without the file", (
                "a turn carrying files errored or was never answered with the lane off"
            )
            # Read straight off the substrate rather than through _claim_env:
            # with the lane off the kernel never touches boot_env at all, so the
            # claim carries None -- not an empty dict, which is what a lane that
            # resolved to nothing produces. Both are attachment-free; only this
            # one proves the turn path was not entered.
            assert h.fake_k8s.claim_envs and all(
                env is None or ATTACHMENTS_REF_ENV not in env for env in h.fake_k8s.claim_envs
            ), (
                "the lane is off and the claim still carries an attachment "
                f"capability: {h.fake_k8s.claim_envs}"
            )

    asyncio.run(go())


def test_the_attachment_ledger_is_swept_from_the_same_reap_tick_under_the_route_lock(
    make_harness,
) -> None:
    """One tick, one lock fence, two ledgers.

    ``reap_orphans`` is the existing periodic tick; the attachment sweep joins it
    rather than arriving with a scheduler of its own. The lock assertion is the
    same one the workspace sweep carries: object deletion happens between
    ``begin`` and ``finish`` and can outlive the original lease, so a competing
    claimant on that thread must stay fenced for the whole critical section --
    otherwise a fresh resolve can land between the two halves and have its ledger
    deleted by the finish.
    """

    async def go() -> None:
        async with make_harness(
            lock_ttl_ms=90,
            lock_acquire_timeout_s=1.0,
            lock_poll_interval_s=0.01,
        ) as h:
            thread = _thread_key("tAttachmentReap")
            entered = threading.Event()
            gate = threading.Event()
            finished: list[object] = []

            class GatedAttachmentLane:
                def resolve(self, **_kwargs: Any) -> Any:
                    raise AssertionError("the reap tick must not resolve anything")

                def enumerate_expired(self) -> list[str]:
                    return [thread]

                def begin_expired_reap(self, thread_key: str) -> object:
                    assert thread_key == thread
                    entered.set()
                    gate.wait(timeout=5.0)
                    return object()

                def finish_expired_reap(self, candidate: object) -> bool:
                    finished.append(candidate)
                    return True

            h.kernel._attachments = GatedAttachmentLane()  # type: ignore[attr-defined]

            reaping = asyncio.create_task(h.kernel.reap_orphans())
            await _wait_until(
                entered.is_set,
                "reap_orphans to sweep the attachment ledger in the same tick",
            )

            contender = asyncio.create_task(h.kernel._lock.acquire(h.config.lock_key(thread)))
            try:
                await asyncio.sleep(0.25)
                assert not contender.done(), (
                    "the attachment sweep mutated its ledger outside the route lock"
                )
            finally:
                gate.set()
            await reaping
            assert len(finished) == 1, "the sweep never reached its fenced ledger delete"
            token = await contender
            await h.kernel._lock.release(h.config.lock_key(thread), token)

    asyncio.run(go())


# --- #2739: a retained runner that restarts across the attachment lookups ----


def _hide_sandboxes(h: Any, monkeypatch: Any) -> set[str]:
    """Docker-shaped non-liveness: ``get_sandbox`` reports these names gone.

    ``DockerSandboxClient.get_sandbox`` returns None for a ``restarting``
    container, so both attachment lookups read no route. Clearing the returned
    set models the container coming back ``running``.
    """

    hidden: set[str] = set()
    real_get_sandbox = h.fake_k8s.get_sandbox

    def get_sandbox(name: str, *, request_timeout_seconds: float) -> Any:
        if name in hidden:
            return None
        return real_get_sandbox(
            name, request_timeout_seconds=request_timeout_seconds
        )

    monkeypatch.setattr(h.fake_k8s, "get_sandbox", get_sandbox)
    return hidden


def _before_substrate_call(
    h: Any, monkeypatch: Any, method: str, action: Callable[[], None]
) -> list[int]:
    """Run ``action`` once, right before the kernel's next ``substrate.<method>``."""

    calls: list[int] = []
    real = getattr(h.substrate, method)

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        if not calls:
            action()
        calls.append(1)
        return real(*args, **kwargs)

    monkeypatch.setattr(h.substrate, method, wrapped)
    return calls


async def _refused_restart_race(
    h: Any,
    monkeypatch: Any,
    *,
    thread: str,
    shape: str,
) -> tuple[Any, Any, _FakeAttachmentLane, QueuedTurn]:
    """Drive one file turn through the non-live -> live race and return
    (old handle, old runner, lane, file event)."""

    for runner in h.runners.values():
        runner.default_script = [Final(text="ok", status=DONE)]
    await h.kernel.process_event(_qevent("first", thread=thread))
    _authenticate_route(h, thread)
    thread_key = _thread_key(thread)
    old = h.substrate.lookup(thread_key)
    assert old is not None
    old_runner = h.runners[h.fake_k8s.assigned_ports[old.sandbox_name]]
    assert old_runner.opened == ["first"]

    lane = _FakeAttachmentLane()
    h.kernel._attachments = lane  # type: ignore[attr-defined]

    if shape == "docker":
        hidden = _hide_sandboxes(h, monkeypatch)
        hidden.add(old.sandbox_name)
        revive = hidden.clear
    else:
        h.fake_k8s.set_sandbox_mode(old.sandbox_name, "Suspended")

        def revive() -> None:
            h.fake_k8s.set_sandbox_mode(old.sandbox_name, "Running")

    assert h.substrate.lookup(thread_key) is None
    claim_calls = _before_substrate_call(h, monkeypatch, "claim", revive)

    file_event = _qevent(
        "read this",
        thread=thread,
        event_id=f"restart-race-{shape}",
        placeholder="p-file",
        attachments=[Attachment(id="F2739", name="restart.txt")],
    )
    await h.kernel.process_event(file_event)
    assert claim_calls, "the race never reached substrate.claim"
    return old, old_runner, lane, file_event


async def _assert_refused_race(
    h: Any,
    *,
    thread: str,
    old: Any,
    old_runner: Any,
    lane: _FakeAttachmentLane,
    file_event: QueuedTurn,
) -> None:
    thread_key = _thread_key(thread)
    assert old_runner.opened == ["first"], "the file turn started on the pre-existing runner"
    assert all("read this" not in runner.opened for runner in h.runners.values())
    assert lane.discard_calls == [{"thread_key": thread_key, "prepared": lane.prepared}]
    file_updates = [text for _channel, ref, text in h.sink.updates if ref == "p-file"]
    assert file_updates and file_updates[-1] == CHANGED_FILE_REPLY
    assert await h.async_redis.exists(h.config.done_key(file_event.event_id))
    assert all(
        ATTACHMENTS_REF_ENV not in (env or {}) for env in h.fake_k8s.claim_envs
    ), "a claim env carried the refused attachment"
    assert len(h.fake_k8s.claim_envs) == 1
    assert h.substrate.lookup(thread_key) == old


def test_docker_restart_between_attachment_lookups_and_claim_refuses_the_file_turn(
    make_harness,
    monkeypatch,
) -> None:
    """#2739: a ``restarting`` container reads as no route, then is adopted.

    Both lookups saw no live handle, so no fenced handoff ran, and the ordinary
    claim reused the old runner (no attachment capability) and ignored the
    attachment env. The turn must instead be refused as a changed thread.
    """

    async def go() -> None:
        async with make_harness(per_sandbox_runners=2) as h:
            old, old_runner, lane, file_event = await _refused_restart_race(
                h, monkeypatch, thread="tDockerRestart", shape="docker"
            )
            await _assert_refused_race(
                h,
                thread="tDockerRestart",
                old=old,
                old_runner=old_runner,
                lane=lane,
                file_event=file_event,
            )

    asyncio.run(go())


def test_k8s_nonrunning_mode_between_attachment_lookups_and_claim_refuses_the_file_turn(
    make_harness,
    monkeypatch,
) -> None:
    """#2739: the K8s shape, an operatingMode that is not Running during both
    lookups and Running again at claim."""

    async def go() -> None:
        async with make_harness(per_sandbox_runners=2) as h:
            old, old_runner, lane, file_event = await _refused_restart_race(
                h, monkeypatch, thread="tK8sRestart", shape="k8s"
            )
            await _assert_refused_race(
                h,
                thread="tK8sRestart",
                old=old,
                old_runner=old_runner,
                lane=lane,
                file_event=file_event,
            )

    asyncio.run(go())


def test_redelivered_file_after_a_refused_restart_race_reaches_one_runner_once(
    make_harness,
    monkeypatch,
) -> None:
    """#2739 negative duplicate-consumption control.

    After the refusal, the whole message sent again finds the now-live idle
    route and goes through the fenced handoff: the attachment reaches exactly
    one new runner exactly once, and the old runner never sees the file text.
    """

    async def go() -> None:
        async with make_harness(per_sandbox_runners=2) as h:
            thread = "tRestartRetry"
            old, old_runner, lane, file_event = await _refused_restart_race(
                h, monkeypatch, thread=thread, shape="docker"
            )
            await _assert_refused_race(
                h,
                thread=thread,
                old=old,
                old_runner=old_runner,
                lane=lane,
                file_event=file_event,
            )
            monkeypatch.undo()
            h.kernel._attachments = lane  # type: ignore[attr-defined]

            retry = _qevent(
                "read this again",
                thread=thread,
                event_id="restart-race-retry",
                placeholder="p-retry",
                attachments=[Attachment(id="F2739", name="restart.txt")],
            )
            await h.kernel.process_event(retry)

            attachment_envs = [
                env
                for env in h.fake_k8s.claim_envs
                if env is not None and ATTACHMENTS_REF_ENV in env
            ]
            assert len(attachment_envs) == 1
            assert attachment_envs[0][ATTACHMENTS_REF_ENV] == REF_VALUE
            new = h.substrate.lookup(_thread_key(thread))
            assert new is not None and new.claim_name != old.claim_name
            new_runner = h.runners[h.fake_k8s.assigned_ports[new.sandbox_name]]
            assert new_runner is not old_runner
            assert new_runner.opened == ["read this again"]
            assert old_runner.opened == ["first"]
            assert len(lane.resolve_calls) == 2
            assert len(lane.discard_calls) == 1

    asyncio.run(go())


def test_file_turn_on_a_new_thread_still_claims_fresh_with_the_attachment(
    make_harness,
) -> None:
    """#2739 AC3: with no route at all the fence is not a refusal."""

    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.runner.default_script = [Final(text="read", status=DONE)]

            await h.kernel.process_event(
                _qevent(
                    "read this",
                    thread="tRestartFresh",
                    placeholder="p-fresh",
                    attachments=[Attachment(id="F2739N", name="new.txt")],
                )
            )

            assert len(h.fake_k8s.claim_envs) == 1
            assert _claim_env(h)[ATTACHMENTS_REF_ENV] == REF_VALUE
            assert h.runner.opened == ["read this"]
            assert lane.discard_calls == []
            assert h.substrate.lookup(_thread_key("tRestartFresh")) is not None

    asyncio.run(go())


def test_workspace_route_live_again_at_adopt_refuses_the_file_turn(
    make_harness,
    monkeypatch,
) -> None:
    """#2739 workspace sibling: ``_claim_or_resume`` must not adopt a route that
    both attachment lookups saw as non-live."""

    async def go() -> None:
        binding = _WorkspaceBinding(uuid.uuid4())
        async with make_harness(binding=binding) as h:
            workspace = _WorkspaceProbe(h.substrate)
            h.kernel._workspace = workspace  # type: ignore[assignment]
            h.runner.default_script = [Final(text="one", status=DONE)]
            await h.kernel.process_event(_qevent("first", thread="tWorkspaceRestart"))
            thread_key = _thread_key("tWorkspaceRestart")
            old = h.substrate.lookup(thread_key)
            assert old is not None and old.workspace_repo == "acme/example"
            assert len(workspace.claim_calls) == 1

            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            hidden = _hide_sandboxes(h, monkeypatch)
            hidden.add(old.sandbox_name)
            assert h.substrate.lookup(thread_key) is None
            adopt_calls = _before_substrate_call(h, monkeypatch, "adopt", hidden.clear)

            file_event = _qevent(
                "read this",
                thread="tWorkspaceRestart",
                event_id="workspace-restart-race",
                placeholder="p-file",
                attachments=[Attachment(id="F2739W", name="workspace.txt")],
            )
            await h.kernel.process_event(file_event)

            assert adopt_calls, "the race never reached substrate.adopt"
            assert h.runner.opened == ["first"]
            assert lane.discard_calls == [
                {"thread_key": thread_key, "prepared": lane.prepared}
            ]
            file_updates = [
                text for _channel, ref, text in h.sink.updates if ref == "p-file"
            ]
            assert file_updates and file_updates[-1] == CHANGED_FILE_REPLY
            assert await h.async_redis.exists(h.config.done_key(file_event.event_id))
            assert all(
                ATTACHMENTS_REF_ENV not in (env or {}) for env in h.fake_k8s.claim_envs
            )
            assert h.substrate.lookup(thread_key) == old

    asyncio.run(go())


def test_the_lane_is_asked_for_the_identity_the_turn_arrived_on(make_harness) -> None:
    """ADR-0168 decision 5: a file is fetched with the addressed bot's token."""

    def turn(thread: str, adapter: str | None) -> QueuedTurn:
        return QueuedTurn(
            event_id=f"ev-{thread}",
            conversation_id=thread,
            author="U1",
            text="what does this say?",
            reply_handle=ReplyHandle(
                kind="slack", channel="C1", placeholder="p-1", adapter=adapter
            ),
            received_at="2026-07-05T00:00:00+00:00",
            source=TurnSource.SLACK,
            attachments=[Attachment(id="F1", name="report.csv", mime_type="text/csv")],
        )

    async def go() -> None:
        async with make_harness() as h:
            lane = _FakeAttachmentLane()
            h.kernel._attachments = lane  # type: ignore[attr-defined]
            h.runner.default_script = [Final(text="read it", status=DONE)]

            await h.kernel.process_event(turn("tNamedIdentity", "ops-bot"))
            await h.kernel.process_event(turn("tStockIdentity", None))

            assert [call["extra"] for call in lane.resolve_calls] == [
                {"identity": "ops-bot"},
                {"identity": "default"},
            ]

    asyncio.run(go())
