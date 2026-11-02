"""The kernel requests the publication snapshot at the retained prepared base (#5230).

The worker, not the agent's checkout, names P: the kernel reads the
coordinator's retained base for the thread and passes it to the runner. A
thread with no retained base never reaches the runner. The harness is the
factory execute flow from ``test_work_item_early_stop``.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace

import pytest
from curie_worker.runner_client import RunnerWorkspaceSnapshot

from apps.worker.tests.kernel.test_work_item_early_stop import (
    ISSUE_PROMPT,
    PUBLISH_TOOL,
    WORK_ITEM_REPO,
    _Binding,
    _PublicationApi,
    _publish_final,
    _tool,
    _turn,
    _WorkItems,
    _Workspace,
)

RETAINED_BASE = "b2" * 20


def _snapshot(base_sha: str) -> RunnerWorkspaceSnapshot:
    return RunnerWorkspaceSnapshot(
        repo_full_name=WORK_ITEM_REPO,
        base_sha=base_sha,
        patch=b"diff --git a/src/widget.py b/src/widget.py\n",
        changed_paths=("src/widget.py",),
        contains_workflow_files=False,
        publication_title="Fix the widget parser",
        publication_body="Fixes the parser.",
    )


class _RetainedBaseWorkspace(_Workspace):
    def __init__(self, substrate: object) -> None:
        super().__init__(substrate)
        self.claimed_threads: list[str] = []
        self.current_reads: list[str] = []

    def claim_or_resume_with_handle(self, **kwargs: object) -> object:
        self.claimed_threads.append(str(kwargs["thread_key"]))
        return super().claim_or_resume_with_handle(**kwargs)

    def current(self, thread_key: str) -> object:
        self.current_reads.append(thread_key)
        return SimpleNamespace(repo_full_name=WORK_ITEM_REPO, base_sha=RETAINED_BASE)


class _NoRetainedBaseWorkspace(_Workspace):
    def current(self, _thread_key: str) -> object:
        return None


def test_the_kernel_requests_the_snapshot_at_the_retained_prepared_base(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_RetainedBaseWorkspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            workspace = h.kernel._workspace
            assert isinstance(workspace, _RetainedBaseWorkspace)
            requested: list[object] = []
            validated: list[RunnerWorkspaceSnapshot] = []

            async def snapshot(*_args: object, **kwargs: object) -> RunnerWorkspaceSnapshot:
                requested.append(kwargs.get("base_sha"))
                return _snapshot(str(kwargs.get("base_sha")))

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            monkeypatch.setattr(
                "curie_worker.kernel.attempt.validate_snapshot_against_base",
                lambda *_args, **kwargs: validated.append(kwargs["snapshot"]),
            )
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            assert requested == [RETAINED_BASE]
            assert [candidate.base_sha for candidate in validated] == [RETAINED_BASE]
            assert len(publications.creates) == 1
            assert "hold_for_approval" in items.calls
            # The base is the one retained for the thread this turn claimed.
            assert workspace.current_reads
            assert set(workspace.current_reads) == set(workspace.claimed_threads)

    asyncio.run(exercise())


def test_a_thread_without_a_retained_base_never_requests_a_snapshot(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_NoRetainedBaseWorkspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            calls = 0

            async def snapshot(*_args: object, **_kwargs: object) -> RunnerWorkspaceSnapshot:
                nonlocal calls
                calls += 1
                return _snapshot("a1" * 20)

            monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
            h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            assert calls == 0
            assert publications.creates == []
            assert "hold_for_approval" not in items.calls
            assert len(items.finishes) == 1
            finish = items.finishes[0]
            assert finish["outcome"] == "failed"
            assert finish["cause"] == "approval_create_failed"
            detail = finish["detail"]
            assert isinstance(detail, str)
            assert detail.startswith("publication snapshot failed: ")
            assert "thread has no retained sanitized base" in detail

    asyncio.run(exercise())
