"""A bundle-denied publication through the worker's completion path (ADR 0286, #5785).

``tests/vectors/publish-hook-deny-turn.json`` pins what the runner emits on
the real bundled CLI when a bundle PreToolUse hook denies the first
``publish_changes`` call and allows the corrected one
(runner/tests/test_publish_hook_deny_real_cli.py): the turn's frames and the
title and body the runner's gate holds, which ``/v1/snapshot`` returns. This
case replays both through the real kernel and its publication path: the
frames from the conftest runner, the gate's title and body from the snapshot.

The worker opens exactly one publication, carrying the corrected body, and
holds the request for approval. The body the hook refused reaches no
publication request.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from aci_protocol import OutboundEvent, parse_ndjson
from aci_protocol.version import PROTOCOL_VERSION

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

VECTOR = json.loads(
    (
        Path(__file__).resolve().parents[4] / "tests" / "vectors" / "publish-hook-deny-turn.json"
    ).read_text(encoding="utf-8")
)


def _frames(pinned: list[dict[str, Any]]) -> list[OutboundEvent]:
    lines = "".join(json.dumps({**frame, "version": PROTOCOL_VERSION}) + "\n" for frame in pinned)
    return list(parse_ndjson(lines))


def _patch_runner_snapshot(h: object, monkeypatch: pytest.MonkeyPatch) -> None:
    """The runner's /v1/snapshot answer: its gate's pinned title and body."""

    from curie_worker.runner_client import RunnerWorkspaceSnapshot

    async def snapshot(*_args: object, **_kwargs: object) -> RunnerWorkspaceSnapshot:
        return RunnerWorkspaceSnapshot(
            repo_full_name=WORK_ITEM_REPO,
            base_sha="a1" * 20,
            patch=b"diff --git a/README.md b/README.md\n",
            changed_paths=("README.md",),
            contains_workflow_files=False,
            publication_title=VECTOR["publication"]["title"],
            publication_body=VECTOR["publication"]["body"],
        )

    monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)  # type: ignore[attr-defined]
    monkeypatch.setattr(
        "curie_worker.kernel.attempt.validate_snapshot_against_base",
        lambda *_args, **_kwargs: None,
    )


def test_the_corrected_publication_is_the_one_the_worker_opens(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One publication, the corrected body, and the request held for approval."""

    async def exercise() -> None:
        publications = _PublicationApi()
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_Workspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _patch_runner_snapshot(h, monkeypatch)
            h.runner.turn_scripts = [_frames(VECTOR["runner_frames"])]
            h.runner.default_script = [_done("default script must not run")]

            await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))

            # One turn: the recorded publication parks the request, so the
            # #3128 continuation never re-prompts it to publish.
            assert len(h.runner.opened) == 1
            [request] = publications.creates
            payload = request.to_json()  # type: ignore[attr-defined]
            assert payload["title"] == VECTOR["publication"]["title"]
            assert payload["body"] == VECTOR["publication"]["body"]
            assert VECTOR["refused_body"] not in json.dumps(payload)
            assert items.finishes == []
            assert "hold_for_approval" in items.calls

    asyncio.run(exercise())
