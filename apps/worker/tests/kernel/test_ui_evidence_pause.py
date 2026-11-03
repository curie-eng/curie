"""The kernel's two UI evidence calls around the publication pause.

``capture_ui_evidence`` runs right after the publication snapshot validates, at
the retained prepared base, and ``relay_ui_evidence`` runs right after
``create_publication`` returns, with the created id. Neither may change the
pause: creation, the approval hold, the suspend and the reply must read exactly
as a run with no evidence. The harness is the factory execute flow of
``test_publication_snapshot_base``; only the ``RunnerClient`` evidence methods
are replaced (by stand-ins for the runner's two routes), so the real capture
and relay code runs inside the kernel.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from typing import Any

import pytest
from curie_worker.runner_client import RunnerWorkspaceSnapshot
from curie_worker.workspace import WorkspacePreparationError

from apps.worker.tests.kernel.test_publication_snapshot_base import (
    RETAINED_BASE,
    _RetainedBaseWorkspace,
    _snapshot,
)
from apps.worker.tests.kernel.test_work_item_early_stop import (
    ISSUE_PROMPT,
    PUBLISH_TOOL,
    _Binding,
    _PublicationApi,
    _publish_final,
    _tool,
    _turn,
    _WorkItems,
)
from runner.tests.test_ui_evidence import png

CANDIDATE_TREE = "c3" * 20
IMAGES = {0: png("before/home.png"), 1: png("after/home.png")}
HANG_BUDGET_S = 6.0


def _runner_index(base_sha: str, *, absent: bool) -> dict[str, Any]:
    """A ``/v1/ui-evidence/index`` 200 body (plan section 5.1)."""

    if absent:
        return {
            "version": 1,
            "manifest": "absent",
            "manifest_refusal": None,
            "status": None,
            "reason": None,
            "base_sha": base_sha,
            "manifest_base_sha": None,
            "manifest_head_sha": None,
            "candidate_dirty": None,
            "manifest_candidate_tree": None,
            "candidate_tree": None,
            "candidate_error": None,
            "head_tree": None,
            "images": [],
        }
    return {
        "version": 1,
        "manifest": "valid",
        "manifest_refusal": None,
        "status": "complete",
        "reason": None,
        "base_sha": base_sha,
        "manifest_base_sha": base_sha,
        "manifest_head_sha": None,
        "candidate_dirty": True,
        "manifest_candidate_tree": CANDIDATE_TREE,
        "candidate_tree": CANDIDATE_TREE,
        "candidate_error": None,
        "head_tree": None,
        "images": [
            {
                "ordinal": ordinal,
                "path": f"{state}/home.png",
                "label": "Home page",
                "state": state,
                "size": len(IMAGES[ordinal]),
                "sha256": hashlib.sha256(IMAGES[ordinal]).hexdigest(),
                "refusal": None,
                "bound": True,
            }
            for ordinal, state in ((0, "before"), (1, "after"))
        ],
    }


class _EvidenceRoutes:
    """Stands in for the runner's two evidence routes on the kernel's RunnerClient."""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.mode = "evidence"  # or "absent" or "hang"
        self.index_calls: list[dict[str, Any]] = []
        self.hung = asyncio.Event()

    async def index(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        self.events.append("evidence_index")
        self.index_calls.append({"args": args, **kwargs})
        return _runner_index(str(kwargs["base_sha"]), absent=self.mode == "absent")

    async def image(self, *_args: Any, **kwargs: Any) -> None:
        self.events.append("evidence_image")
        if self.mode == "hang":
            self.hung.set()
            await asyncio.sleep(3600)
        kwargs["sink"].write(IMAGES[int(kwargs["index"])])


class _EvidencePublicationApi(_PublicationApi):
    """The publication API double, plus the evidence upload the relay calls."""

    def __init__(self, events: list[str], *, upload_error: Exception | None = None) -> None:
        super().__init__()
        self.events = events
        self.upload_error = upload_error
        self.uploads: list[dict[str, Any]] = []

    async def create_publication(self, request: object, *, budget_s: float = 120) -> object:
        self.events.append("create")
        return await super().create_publication(request, budget_s=budget_s)

    async def upload_ui_evidence(
        self, publication_id: str, capture: Any, *, budget_s: float
    ) -> None:
        self.events.append("upload")
        files: dict[int, bytes] = {}
        for ordinal, handle in capture.files.items():
            handle.seek(0)
            files[ordinal] = handle.read()
        self.uploads.append(
            {
                "publication_id": publication_id,
                "index": capture.index,
                "files": files,
                "budget_s": budget_s,
            }
        )
        if self.upload_error is not None:
            raise self.upload_error


class _PlainPublicationApi(_PublicationApi):
    """A creator with no ``upload_ui_evidence``: every existing kernel fake."""

    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self.events = events

    async def create_publication(self, request: object, *, budget_s: float = 120) -> object:
        self.events.append("create")
        return await super().create_publication(request, budget_s=budget_s)


def _install(
    h: Any,
    monkeypatch: pytest.MonkeyPatch,
    events: list[str],
    *,
    validation_error: Exception | None = None,
) -> _EvidenceRoutes:
    routes = _EvidenceRoutes(events)

    async def snapshot(*_args: object, **kwargs: object) -> RunnerWorkspaceSnapshot:
        events.append("snapshot")
        return _snapshot(str(kwargs.get("base_sha")))

    def validate(*_args: object, **_kwargs: object) -> None:
        events.append("validate")
        if validation_error is not None:
            raise validation_error

    monkeypatch.setattr(h.kernel._runner, "snapshot", snapshot)
    monkeypatch.setattr(h.kernel._runner, "ui_evidence_index", routes.index)
    monkeypatch.setattr(h.kernel._runner, "ui_evidence_image", routes.image)
    monkeypatch.setattr("curie_worker.kernel.attempt.validate_snapshot_against_base", validate)
    return routes


def _creation_view(request: Any) -> tuple[object, ...]:
    return (
        request.repo_full_name,
        request.base_sha,
        request.patch,
        tuple(request.changed_paths),
        request.title,
        request.body,
        request.summary,
    )


async def _pause_once(h: Any, items: _WorkItems, publications: _PublicationApi) -> dict[str, Any]:
    """Drive one factory execute turn to the publication pause; what the world saw."""

    marks = {
        "calls": len(items.calls),
        "finishes": len(items.finishes),
        "creates": len(publications.creates),
        "updates": len(h.sink.updates),
        "posts": len(h.sink.posts),
        "events": len(h.sink.events),
        "deleted": len(h.fake_k8s.deleted_claims),
    }
    h.runner.turn_scripts = [[_tool(PUBLISH_TOOL), _publish_final()]]
    await h.kernel.process_event(_turn(f"work-item-{uuid.uuid4()}-execute-1", ISSUE_PROMPT))
    return {
        "calls": items.calls[marks["calls"] :],
        "finishes": [
            (finish.get("outcome"), finish.get("cause"))
            for finish in items.finishes[marks["finishes"] :]
        ],
        "creates": [_creation_view(r) for r in publications.creates[marks["creates"] :]],
        "updates": [text for *_, text in h.sink.updates[marks["updates"] :]],
        "posts": len(h.sink.posts) - marks["posts"],
        "events": [event.event for event, *_ in h.sink.events[marks["events"] :]],
        "suspended": len(h.fake_k8s.deleted_claims) - marks["deleted"],
    }


def _assert_paused(view: dict[str, Any]) -> None:
    assert len(view["creates"]) == 1
    assert view["creates"][0][1] == RETAINED_BASE
    assert "hold_for_approval" in view["calls"]
    assert ("failed", "approval_create_failed") not in view["finishes"]


def test_capture_follows_the_validated_snapshot_and_upload_follows_create(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        events: list[str] = []
        publications = _EvidencePublicationApi(events)
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_RetainedBaseWorkspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            routes = _install(h, monkeypatch, events)

            view = await _pause_once(h, items, publications)

            _assert_paused(view)
            assert events == [
                "snapshot",
                "validate",
                "evidence_index",
                "evidence_image",
                "evidence_image",
                "create",
                "upload",
            ]
            assert [call["base_sha"] for call in routes.index_calls] == [RETAINED_BASE]
            (upload,) = publications.uploads
            assert upload["publication_id"] == "publication-1"
            assert upload["files"] == IMAGES
            assert upload["index"]["base_sha"] == RETAINED_BASE
            assert [image["bound"] for image in upload["index"]["images"]] == [True, True]
            assert [image["refusal"] for image in upload["index"]["images"]] == [None, None]

    asyncio.run(exercise())


def test_no_capture_when_snapshot_validation_fails(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        events: list[str] = []
        publications = _EvidencePublicationApi(events)
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_RetainedBaseWorkspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            _install(
                h,
                monkeypatch,
                events,
                validation_error=WorkspacePreparationError(
                    "publication-validation", "the candidate does not apply to the base"
                ),
            )

            view = await _pause_once(h, items, publications)

            assert "evidence_index" not in events
            assert "evidence_image" not in events
            assert "upload" not in events
            assert view["creates"] == []
            assert publications.uploads == []
            assert "hold_for_approval" not in view["calls"]
            (finish,) = items.finishes
            assert finish["outcome"] == "failed"
            assert str(finish["detail"]).startswith("publication snapshot failed: ")

    asyncio.run(exercise())


def test_a_hanging_capture_is_cut_at_its_budget_and_the_pause_is_unchanged(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("curie_worker.ui_evidence.CAPTURE_BUDGET_S", HANG_BUDGET_S)

    async def exercise() -> None:
        events: list[str] = []
        publications = _EvidencePublicationApi(events)
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_RetainedBaseWorkspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            routes = _install(h, monkeypatch, events)

            routes.mode = "absent"
            baseline = await _pause_once(h, items, publications)

            routes.mode = "hang"
            started = time.monotonic()
            hung = await _pause_once(h, items, publications)
            elapsed = time.monotonic() - started

            assert routes.hung.is_set(), "the capture never reached the image fetch"
            assert elapsed < HANG_BUDGET_S + 20
            _assert_paused(baseline)
            assert hung == baseline
            # The cut capture still reports what happened, after the publication exists.
            assert events[-2:] == ["create", "upload"]
            upload = publications.uploads[-1]
            assert upload["publication_id"] == "publication-1"
            assert "capture_timeout" in upload["index"]["warnings"]
            assert [image["refusal"] for image in upload["index"]["images"]] == [
                "timeout",
                "timeout",
            ]
            assert upload["files"] == {}

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "error",
    [RuntimeError("evidence upload exploded"), TimeoutError(), OSError("disk gone")],
    ids=["runtime-error", "timeout", "os-error"],
)
def test_a_failing_upload_leaves_creation_suspend_and_reply_unchanged(
    make_harness, monkeypatch: pytest.MonkeyPatch, error: Exception
) -> None:
    async def exercise() -> None:
        events: list[str] = []
        publications = _EvidencePublicationApi(events)
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_RetainedBaseWorkspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            routes = _install(h, monkeypatch, events)

            routes.mode = "absent"
            baseline = await _pause_once(h, items, publications)
            assert publications.uploads == []

            routes.mode = "evidence"
            publications.upload_error = error
            failed = await _pause_once(h, items, publications)

            _assert_paused(baseline)
            assert len(publications.uploads) == 1
            assert publications.uploads[0]["publication_id"] == "publication-1"
            assert failed == baseline

    asyncio.run(exercise())


def test_a_creator_without_evidence_upload_is_untouched(
    make_harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        events: list[str] = []
        publications = _PlainPublicationApi(events)
        assert not hasattr(publications, "upload_ui_evidence")
        async with make_harness(
            binding=_Binding(),
            workspace_factory=_RetainedBaseWorkspace,
            publication_creator=publications,
        ) as h:
            items = _WorkItems()
            h.kernel._work_items = items
            routes = _install(h, monkeypatch, events)

            routes.mode = "absent"
            baseline = await _pause_once(h, items, publications)

            routes.mode = "evidence"
            del events[:]
            with_evidence = await _pause_once(h, items, publications)

            _assert_paused(baseline)
            assert with_evidence == baseline
            # The capture ran and its files were simply dropped after creation.
            assert events[:3] == ["snapshot", "validate", "evidence_index"]
            assert events[-1] == "create"

    asyncio.run(exercise())
