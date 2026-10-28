"""Applied base moves reach the normal factory finish, CI and ownership boundaries."""

# ruff: noqa: F811

from __future__ import annotations

import asyncio
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import get_settings
from forge_fakes.github import LABEL, REPO
from forge_fakes.github_comments import (  # noqa: F401
    HEAD_A,
    HEAD_B,
    HEAD_C,
    _rows,
    admitted,
    check_run,
    ci_entry,
    ci_failing,
    ci_green,
    ci_pending,
    comments,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from test_factory_base_lifecycle import _train
from test_factory_ci_wait import (
    _attach_fix,
    _attach_fix_async,
    _ci_turns,
    _execute,
    _listed_owner_ids,
    _owner_lost,
    _terminal,
)
from test_factory_progress import verification
from test_factory_terminus import _label, _reconcile, _request, _start_running
from test_github_factory_base import MAIN_SHA, NEXT_SHA, _open_lineage

pytestmark = pytest.mark.usefixtures("clean_db")

WORKER = {"X-Curie-Worker-Token": "factory-terminus-worker"}


def _applied_move(
    client: Any,
    github: Any,
    sink: Any,
    number: int,
    *,
    paths: tuple[str, ...] = ("src/widget.txt",),
) -> dict[str, Any]:
    from curie_api.models import (
        ExecutionRequest,
        FactoryBaseTransition,
        ThreadPublicationLineage,
        WorkItem,
    )

    github.labels = [LABEL]
    _label(client, github, number)
    row = _request(number)
    _open_lineage(row["work_item_id"])
    transition_id = uuid.uuid4()

    async def seed() -> str:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                item = await session.get(WorkItem, row["work_item_id"])
                request = await session.get(ExecutionRequest, row["id"])
                assert item is not None and request is not None
                lineage = await session.get(ThreadPublicationLineage, item.publication_lineage_id)
                assert lineage is not None
                now = await session.scalar(text("SELECT clock_timestamp()"))
                item.base_branch = "next"
                item.base_source = "label"
                item.base_commit = NEXT_SHA
                item.base_resolved_request_id = request.id
                lineage.base_ref = "next"
                lineage.base_sha = NEXT_SHA
                lineage.head_sha = HEAD_B
                lineage.code_host_kind = item.code_host_kind
                lineage.code_host_host = item.code_host_host
                lineage.repository_project_id = item.repository_project_id
                lineage.code_host_installation_id = item.code_host_installation_id
                lineage.code_host_pr_id = "P0EXAMPLE77"
                session.add(
                    FactoryBaseTransition(
                        id=transition_id,
                        work_item_id=item.id,
                        request_id=request.id,
                        lineage_id=lineage.id,
                        phase="applied",
                        version=1,
                        old_base_ref="main",
                        old_base_sha=MAIN_SHA,
                        old_head_sha=HEAD_A,
                        target_base_ref="next",
                        target_base_sha=NEXT_SHA,
                        base_source="label",
                        branch=lineage.branch,
                        pull_request_number="77",
                        committer_name="Curie",
                        committer_email="factory@example.com",
                        committer_time=now,
                        rebased_head_sha=HEAD_C,
                        verification_head_sha=HEAD_B,
                        changed_paths=list(paths),
                        ci_pushed_at=now,
                        applied_at=now,
                        outcome_ready_at=now,
                        ci_ready_at=None,
                        ci_settled_at=None,
                    )
                )
                await session.commit()
                return lineage.branch
        finally:
            await engine.dispose()

    branch = asyncio.run(seed())
    epoch = _start_running(client, row["id"])
    sink.branches["next"] = NEXT_SHA
    sink.pull_script = [
        {
            "mergeable": True,
            "mergeable_state": "clean",
            "merged": False,
            "head": {"ref": branch, "sha": HEAD_B},
            "base": {"ref": "next"},
        }
    ]
    sink.ci_scripts = {HEAD_C: [ci_green()], HEAD_B: [ci_pending()]}
    return {
        **row,
        "number": number,
        "transition_id": transition_id,
        "epoch": epoch,
        "branch": branch,
    }


@pytest.fixture
def moved(admitted: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    client, github, sink = admitted
    _train(monkeypatch, tmp_path, client)
    row = _applied_move(client, github, sink, 32101)
    return client, github, sink, row


def _finish(client: Any, row: dict[str, Any], *, round_: int | None = None) -> Any:
    return client.post(
        f"/v1/internal/work-items/requests/{row['id']}/finish",
        headers=WORKER,
        json={
            "runtime_epoch": row["epoch"],
            "outcome": "failed",
            "cause": "no_pull_request" if round_ is None else "ci_fix_unpublished",
            "detail": "No changes needed: repository verification passed on the new base.",
            "ci_fix_round": round_,
        },
    )


def _transition(row: dict[str, Any]) -> dict[str, Any]:
    return _rows(
        "SELECT * FROM curie.factory_base_transitions WHERE id = :id", {"id": row["transition_id"]}
    )[0]


def _no_publications(row: dict[str, Any]) -> None:
    assert (
        _rows(
            "SELECT id FROM curie.publications WHERE execution_request_id = :id", {"id": row["id"]}
        )
        == []
    )


def _handoff(client: Any, row: dict[str, Any]) -> None:
    response = _finish(client, row)
    assert response.status_code == 409, response.text
    assert "publication_pending" in response.text
    assert _transition(row)["ci_ready_at"] is not None
    assert _terminal(row["number"]) == ("running", None)
    _no_publications(row)


def _verified(client: Any, row: dict[str, Any], *, passed: bool = True) -> None:
    result = verification(
        client,
        row["id"],
        {
            "check": "unit_tests",
            "command": "uv run pytest apps/api/tests -q",
            "outcome": "passed" if passed else "failed",
            "exit_status": 0 if passed else 1,
            "missing_binaries": [],
            "blocked_services": [],
        },
    )
    assert result.status_code == 201, result.text


def test_applied_move_does_not_compete_with_the_initial_verification_turn(moved: Any) -> None:
    _client, _github, sink, row = moved
    sink.ci_scripts[HEAD_B] = [ci_failing()]

    _reconcile()

    assert _terminal(row["number"]) == ("running", None)
    assert _transition(row)["ci_ready_at"] is None
    assert sink.ci_observations == []
    assert _ci_turns(row["id"]) == []
    _no_publications(row)


@pytest.mark.parametrize("verdict", ["red", "pending", "green"])
def test_no_change_successor_settles_only_on_its_final_head(moved: Any, verdict: str) -> None:
    client, _github, sink, row = moved
    sink.ci_scripts[HEAD_B] = [
        {"red": ci_failing, "pending": ci_pending, "green": ci_green}[verdict]()
    ]
    _handoff(client, row)

    _reconcile()

    assert HEAD_B in sink.ci_observations
    assert HEAD_C not in sink.ci_observations
    _no_publications(row)
    if verdict == "green":
        assert _terminal(row["number"]) == ("completed", "completed")
        assert _transition(row)["ci_settled_at"] is not None
        assert _ci_turns(row["id"]) == []
    else:
        assert _terminal(row["number"]) == ("running", None)
        assert _transition(row)["ci_settled_at"] is None
        turns = _ci_turns(row["id"])
        assert len(turns) == (1 if verdict == "red" else 0)
        if turns:
            assert turns[0]["event_id"] == f"work-item-{row['id']}-ci-2"
            assert HEAD_B in turns[0]["text"]


def test_pending_base_move_ci_retains_ownership_after_runtime_heartbeat_lapses(moved: Any) -> None:
    client, _github, sink, row = moved
    _handoff(client, row)
    _execute(
        "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
        "clock_timestamp() - make_interval(secs => :elapsed) WHERE id = :id",
        {"id": row["id"], "elapsed": get_settings().work_item_runtime_ttl_seconds + 5},
    )

    _reconcile()
    _reconcile()

    assert _terminal(row["number"]) == ("running", None)
    assert str(row["id"]) not in _listed_owner_ids(client)
    refused = _owner_lost(client, row["id"], row["epoch"])
    assert refused.status_code == 409, refused.text
    requests = _rows(
        "SELECT id, owner_lost_retry FROM curie.execution_requests WHERE work_item_id = :id",
        {"id": row["work_item_id"]},
    )
    assert requests == [{"id": row["id"], "owner_lost_retry": False}]
    sink.ci_scripts[HEAD_B] = [ci_green()]

    _reconcile()

    assert _terminal(row["number"]) == ("completed", "completed")
    _no_publications(row)


@pytest.mark.parametrize("started_after_retarget", [False, True])
def test_intermediate_head_green_cannot_settle_even_when_its_check_starts_after_retarget(
    moved: Any, started_after_retarget: bool
) -> None:
    client, _github, sink, row = moved
    pushed_at = _transition(row)["ci_pushed_at"]
    started_at = pushed_at + timedelta(seconds=5 if started_after_retarget else -30)
    sink.ci_scripts[HEAD_C] = [
        ci_entry(
            check_run(
                "build",
                started_at=started_at.isoformat(),
            )
        )
    ]
    sink.ci_scripts[HEAD_B] = [ci_pending()]
    _handoff(client, row)

    _reconcile()

    assert _terminal(row["number"]) == ("running", None)
    assert sink.ci_observations == [HEAD_B]
    assert _transition(row)["ci_settled_at"] is None
    assert _ci_turns(row["id"]) == []
    sink.ci_scripts[HEAD_B] = [ci_green()]

    _reconcile()

    assert _terminal(row["number"]) == ("completed", "completed")
    assert HEAD_C not in sink.ci_observations
    _no_publications(row)


def test_unready_move_does_not_hide_a_genuinely_lost_runtime(moved: Any) -> None:
    client, _github, _sink, row = moved
    assert _transition(row)["ci_ready_at"] is None
    assert str(row["id"]) in _listed_owner_ids(client)

    declared = _owner_lost(client, row["id"], row["epoch"])

    assert declared.status_code == 200, declared.text
    assert _terminal(row["number"]) == ("cancellation_requested", "owner_lost")


def test_a_normal_request_with_no_ci_subject_remains_reclaimable(moved: Any) -> None:
    client, github, _sink, _row = moved
    github.labels = [LABEL]
    _label(client, github, 32103)
    normal = _request(32103)
    epoch = _start_running(client, normal["id"])
    assert str(normal["id"]) in _listed_owner_ids(client)

    declared = _owner_lost(client, normal["id"], epoch)

    assert declared.status_code == 200, declared.text
    assert _terminal(32103) == ("cancellation_requested", "owner_lost")


@pytest.mark.parametrize("evidence", ["missing", "failed", "fresh"])
def test_python_paths_in_the_rebase_keep_the_normal_preflight_gate(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, evidence: str
) -> None:
    client, github, sink = admitted
    _train(monkeypatch, tmp_path, client)
    row = _applied_move(client, github, sink, 32102, paths=("apps/api/src/example.py",))
    if evidence != "missing":
        _verified(client, row, passed=evidence == "fresh")
    sink.ci_scripts[HEAD_B] = [ci_green()]
    _handoff(client, row)

    _reconcile()

    if evidence == "fresh":
        assert _terminal(row["number"]) == ("completed", "completed")
    else:
        assert _terminal(row["number"]) == ("failed", "ci_unverified")
        (notice,) = _rows(
            "SELECT detail FROM curie.factory_terminal_notices WHERE execution_request_id = :id",
            {"id": row["id"]},
        )
        assert "python_preflight_" in notice["detail"]
    _no_publications(row)


def test_a_new_real_publication_takes_precedence_over_the_applied_move(moved: Any) -> None:
    client, _github, sink, row = moved
    _handoff(client, row)
    _attach_fix(
        row["work_item_id"],
        row["id"],
        revision=2,
        head_sha=HEAD_C,
        title="Repair after base change",
        paths=["src/widget.txt"],
        base_sha=NEXT_SHA,
    )
    sink.pull_script[0]["head"]["sha"] = HEAD_C
    sink.ci_scripts = {HEAD_B: [ci_failing()], HEAD_C: [ci_green()]}

    _reconcile()

    assert _terminal(row["number"]) == ("completed", "completed")
    assert sink.ci_observations == [HEAD_C]
    publications = _rows(
        "SELECT revision_number FROM curie.publications WHERE execution_request_id = :id",
        {"id": row["id"]},
    )
    assert publications == [{"revision_number": 2}]


def test_base_move_is_round_one_and_a_real_fix_advances_round_accounting(moved: Any) -> None:
    client, _github, sink, row = moved
    sink.ci_scripts[HEAD_B] = [ci_failing()]
    _handoff(client, row)
    _reconcile()
    assert [turn["event_id"] for turn in _ci_turns(row["id"])] == [f"work-item-{row['id']}-ci-2"]
    _attach_fix(
        row["work_item_id"],
        row["id"],
        revision=2,
        head_sha=HEAD_C,
        title="Repair after base change",
        paths=["src/widget.txt"],
        base_sha=NEXT_SHA,
    )
    handed_off = _finish(client, row, round_=2)
    assert handed_off.status_code == 409, handed_off.text
    assert "publication_pending" in handed_off.text
    sink.pull_script[0]["head"]["sha"] = HEAD_C
    sink.ci_scripts[HEAD_C] = [ci_failing()]

    _reconcile()

    assert [turn["event_id"] for turn in _ci_turns(row["id"])] == [
        f"work-item-{row['id']}-ci-2",
        f"work-item-{row['id']}-ci-3",
    ]
    unpublished = _finish(client, row, round_=3)
    assert unpublished.status_code == 200, unpublished.text
    assert _terminal(row["number"]) == ("failed", "ci_fix_unpublished")


@pytest.mark.parametrize("change", ["request_version", "transition_version", "head", "publication"])
def test_a_stale_final_head_observation_cannot_settle_over_current_authority(
    moved: Any, change: str
) -> None:
    client, _github, sink, row = moved
    _handoff(client, row)
    sink.ci_scripts[HEAD_B] = [ci_green()]

    async def race() -> None:
        entered, release = asyncio.Event(), asyncio.Event()
        sink.get_barrier = (f"/repos/{REPO}/pulls/77", asyncio.get_running_loop(), entered, release)
        observing = asyncio.create_task(asyncio.to_thread(_reconcile))
        try:
            await asyncio.wait_for(entered.wait(), 5)
            if change == "publication":
                await _attach_fix_async(
                    row["work_item_id"],
                    row["id"],
                    revision=2,
                    head_sha=HEAD_C,
                    title="A newer revision",
                    paths=["src/widget.txt"],
                    base_sha=NEXT_SHA,
                )
                sink.ci_scripts[HEAD_C] = [ci_pending()]
                sink.pull_script[0]["head"]["sha"] = HEAD_C
            else:
                statement, identifier = {
                    "request_version": (
                        "UPDATE curie.execution_requests SET version = version + 1 WHERE id = :id",
                        row["id"],
                    ),
                    "transition_version": (
                        "UPDATE curie.factory_base_transitions "
                        "SET version = version + 1 WHERE id = :id",
                        row["transition_id"],
                    ),
                    "head": (
                        "UPDATE curie.thread_publication_lineages SET head_sha = :head "
                        "WHERE id = (SELECT publication_lineage_id FROM curie.work_items "
                        "WHERE id = :id)",
                        row["work_item_id"],
                    ),
                }[change]
                engine = create_async_engine(get_settings().database_url)
                try:
                    async with engine.begin() as conn:
                        await conn.execute(text(statement), {"id": identifier, "head": HEAD_C})
                finally:
                    await engine.dispose()
        finally:
            release.set()
            sink.get_barrier = None
            await asyncio.wait_for(observing, 10)

    client.portal.call(race)

    assert _terminal(row["number"]) == ("running", None)
    assert _transition(row)["ci_settled_at"] is None
    assert _ci_turns(row["id"]) == []
    if change == "head":
        _execute(
            "UPDATE curie.thread_publication_lineages SET head_sha = :head "
            "WHERE id = (SELECT publication_lineage_id FROM curie.work_items WHERE id = :id)",
            {"head": HEAD_B, "id": row["work_item_id"]},
        )
    if change == "publication":
        sink.ci_scripts[HEAD_C] = [ci_green()]

    _reconcile()

    assert _terminal(row["number"]) == ("completed", "completed")
