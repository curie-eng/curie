"""A bundle-denied publication through the worker's completion path (ADR 0286, #5785).

The runner turn here is real: the real bundled Claude CLI, the runner's
session and approval wiring, and a local bundle whose hooks.json denies the
first ``publish_changes`` body and allows the corrected one
(``runner/tests/runner_publish_hook_deny_support.py``, the same turn the
runner's case (a) asserts on). Its frames go to the real kernel through the
conftest runner, and the title and body its gate holds are what the runner's
``/v1/snapshot`` returns, so the worker publishes from exactly what the runner
recorded.

The worker opens exactly one publication, carrying the corrected body, and
holds the request for approval. The body the hook refused reaches no
publication request. Red with the runner fix reverted: the runner records the
refused body on sight of the first call, so that is the body the worker opens.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import uuid
from pathlib import Path
from types import ModuleType

import pytest

from .test_work_item_early_stop import (
    ISSUE_PROMPT,
    WORK_ITEM_REPO,
    _Binding,
    _done,
    _PublicationApi,
    _turn,
    _WorkItems,
    _Workspace,
)

_RUNNER_TESTS = Path(__file__).resolve().parents[4] / "runner" / "tests"


_SUPPORT_MODULE = "_worker_handoff_runner_publish_hook_deny_support"


def _support(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    """Load the runner's turn helper under a private name, for this test only.

    ``monkeypatch.setitem`` takes the entry back out at teardown, pass or fail,
    and the private name never shadows the runner suite's own import.
    """

    spec = importlib.util.spec_from_file_location(
        _SUPPORT_MODULE, _RUNNER_TESTS / "runner_publish_hook_deny_support.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered before it runs: its dataclasses resolve their module by name.
    monkeypatch.setitem(sys.modules, _SUPPORT_MODULE, module)
    spec.loader.exec_module(module)
    return module


def _patch_runner_snapshot(
    h: object, monkeypatch: pytest.MonkeyPatch, *, title: str, body: str
) -> None:
    """The runner's /v1/snapshot answer: its gate's title and body."""

    from curie_worker.runner_client import RunnerWorkspaceSnapshot

    async def snapshot(*_args: object, **_kwargs: object) -> RunnerWorkspaceSnapshot:
        return RunnerWorkspaceSnapshot(
            repo_full_name=WORK_ITEM_REPO,
            base_sha="a1" * 20,
            patch=b"diff --git a/README.md b/README.md\n",
            changed_paths=("README.md",),
            contains_workflow_files=False,
            publication_title=title,
            publication_body=body,
        )

    monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)  # type: ignore[attr-defined]
    monkeypatch.setattr(
        "curie_worker.kernel.attempt.validate_snapshot_against_base",
        lambda *_args, **_kwargs: None,
    )


def test_the_corrected_publication_is_the_one_the_worker_opens(
    make_harness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One publication, the corrected body, and the request held for approval."""

    support = _support(monkeypatch)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    turn = support.run_publish_turn(
        tmp_path,
        monkeypatch,
        workspace=support.workspace_with_change(tmp_path),
        hook_command=support.scripted_hook(tmp_path),
        script=[support.REFUSED_BODY, support.CORRECTED_BODY],
    )
    gate = turn.gate

    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_runner_snapshot(
                h,
                monkeypatch,
                title=gate.publication_title or "",
                body=gate.publication_body or "",
            )
            h.runner.turn_scripts = [list(turn.events)]
            h.runner.default_script = [_done("default script must not run")]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            # One turn: the recorded publication parks the request, so the
            # #3128 continuation never re-prompts it to publish.
            assert len(h.runner.opened) == 1
            [request] = publications.creates
            payload = request.to_json()  # type: ignore[attr-defined]
            assert payload["title"] == support.TITLE
            assert payload["body"] == support.CORRECTED_BODY
            assert support.REFUSED_BODY not in json.dumps(payload)
            assert items.finishes == []
            assert "hold_for_approval" in items.calls

    asyncio.run(exercise())
    # The runner ran the turn the handoff is about: two calls, the first denied.
    assert [call for call, _ in turn.emitted] == [support.call_id(0), support.call_id(1)]
