"""Work item title and factory progress on the operator read surface (#4102).

Work items are admitted and started through the real dispatch service, then
the status comment row and phase reports are written as the reconciler and the
sandbox's ``report_progress`` would leave them. Every assertion reads through
``GET /work-items`` and ``GET /work-items/{id}`` and compares against
``factory_progress.phase_view`` called on the same rows, so the console sees
exactly what the GitHub status card shows.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterator
from types import SimpleNamespace
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.factory_progress import phase_view
from curie_api.forges.hosts import github_issue_ref, repository_ref
from curie_api.github_app import _RESOLVERS
from curie_api.main import create_app
from curie_api.models import (
    ExecutionRequest,
    ExecutionRequestPhaseReport,
    FactoryStatusComment,
)
from curie_api.workitem_dispatch import acquire, admit, finish, start
from curie_test_support.valkey import connect_or_skip
from fastapi.testclient import TestClient
from sqlalchemy import delete, event, select, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

REPO = "acme-corp/acme-bot"
ADDRESS = "C0EXAMPLE1"
WIRE_CONVERSATION = "1700000000.000100"
WORKER_TOKEN = "work-item-progress-worker-token"
OWNER = "curie-workers-SENTINELOWNER"
CLAIM_NAME = "curie-thread-SENTINELCLAIM"
SANDBOX_NAME = "sbx-curie-thread-SENTINELSANDBOX"
TITLE = "Add a temperature converter"

DECLARATION: dict[str, Any] = {
    "phases": [
        {"id": "read_issue", "label": "Read issue"},
        {"id": "pin_criteria", "label": "Pin acceptance criteria"},
        {"id": "plan", "label": "Plan"},
        {"id": "plan_review", "label": "Plan review"},
        {"id": "failing_test", "label": "Failing test"},
        {"id": "implement", "label": "Implement"},
        {"id": "review_diff", "label": "Review diff"},
        {"id": "publish", "label": "Publish PR"},
        {"id": "wait_ci", "label": "Wait for CI"},
    ],
    "loops": [
        {"start": "plan", "review": "plan_review", "cap": 3},
        {"start": "implement", "review": "review_diff", "cap": 3},
    ],
}
STAGED_DECLARATION: dict[str, Any] = {
    **DECLARATION,
    "reviewer_model": "anthropic/claude-opus-5.5",
    "stages": [
        {"id": "plan", "label": "Plan", "phases": ["read_issue", "pin_criteria", "plan"]},
        {"id": "plan_review", "label": "Plan review", "phases": ["plan_review"]},
        {"id": "implement", "label": "Implement", "phases": ["failing_test", "implement"]},
        {"id": "review_diff", "label": "Review diff", "phases": ["review_diff", "publish"]},
        {"id": "wait_ci", "label": "Wait for CI", "phases": ["wait_ci"]},
    ],
    "loops": [
        *DECLARATION["loops"],
        {"start": "implement", "review": "wait_ci", "cap": 3},
    ],
}
# The plan review kicked the plan back once: plan is current again, round 2.
KICKED_BACK: list[tuple[str, int | None, str | None]] = [
    ("read_issue", None, "Reading the issue"),
    ("pin_criteria", None, None),
    ("plan", 1, "Drafting the plan"),
    ("plan_review", 1, "Plan misses the Kelvin case"),
    ("plan", 2, None),
]


# --- plumbing ---------------------------------------------------------------


def with_session[T](body: Callable[[AsyncSession], Awaitable[T]]) -> T:
    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with AsyncSession(engine) as session:
                return await body(session)
        finally:
            await engine.dispose()

    return asyncio.run(go())


@pytest.fixture
def stack(clean_db: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    runs_stream = f"test:curie:progress-runs:{uuid.uuid4().hex}"
    monkeypatch.setenv("RUNS_STREAM", runs_stream)
    monkeypatch.setenv("INTERNAL_WORKER_TOKEN", WORKER_TOKEN)
    monkeypatch.setenv("RESUME_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("CURIE_WORK_ITEM_RECONCILER_ENABLED", "false")
    monkeypatch.setenv("APPROVAL_SWEEP_INTERVAL_S", "0")
    monkeypatch.setenv("DEAD_LETTER_WATCH_INTERVAL_S", "0")
    monkeypatch.setenv("GITHUB_TOKEN", "ghp_progress_operator")
    monkeypatch.setenv("GITHUB_REPO_ALLOWLIST", '["acme-corp/*"]')
    get_settings.cache_clear()
    _RESOLVERS.clear()
    with TestClient(create_app()) as client:
        yield client
    valkey = connect_or_skip(decode_responses=True)
    valkey.delete(runs_stream, f"{runs_stream}:dead")
    valkey.close()
    _RESOLVERS.clear()
    get_settings.cache_clear()


def _agent(client: TestClient, auth_headers: dict[str, str]) -> str:
    """Create agent + version + deployment through the operator API."""

    agent = client.post(
        "/agents",
        json={
            "name": f"factory-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": ADDRESS},
            "repo_full_name": REPO,
        },
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    agent_id = str(agent.json()["id"])
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "operator"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    deployment = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version.json()["id"],
            "environment": "dev",
            "workspace_enabled": True,
        },
        headers=auth_headers,
    )
    assert deployment.status_code == 201, deployment.text
    return agent_id


def _facts(agent_id: str, issue_number: int) -> SimpleNamespace:
    return SimpleNamespace(
        agent_id=uuid.UUID(agent_id),
        kind="slack",
        address=ADDRESS,
        reply_conversation_id=WIRE_CONVERSATION,
        issue=github_issue_ref(get_settings(), repository_id=101, issue_number=issue_number),
        repository=repository_ref(get_settings(), path=REPO, project_id=101),
        code_host_installation_id=202,
        objective="Implement the admitted work item",
        requester="U0REQUEST1",
        request_id=uuid.uuid4(),
    )


def _admit(facts: SimpleNamespace) -> uuid.UUID:
    async def body(session: AsyncSession) -> uuid.UUID:
        admitted = await admit(session, facts)
        assert getattr(admitted, "request", None) is not None, admitted
        return uuid.UUID(str(admitted.work_item.id))  # type: ignore[union-attr]

    return with_session(body)


def _start(request_id: uuid.UUID) -> int:
    async def body(session: AsyncSession) -> int:
        await acquire(session, request_id, owner=OWNER, generation=1)
        started = await start(
            session,
            request_id,
            owner=OWNER,
            generation=1,
            claim_name=CLAIM_NAME,
            sandbox_name=SANDBOX_NAME,
        )
        return int(started.runtime_epoch)  # type: ignore[union-attr]

    return with_session(body)


def _fail(request_id: uuid.UUID, epoch: int) -> None:
    async def body(session: AsyncSession) -> None:
        result = await finish(
            session,
            request_id,
            runtime_epoch=epoch,
            outcome="failed",
            cause="deadline_halted",
            detail=None,
        )
        assert getattr(result, "code", None) is None, result

    with_session(body)


def _running(agent_id: str, issue_number: int) -> SimpleNamespace:
    facts = _facts(agent_id, issue_number)
    work_item_id = _admit(facts)
    epoch = _start(facts.request_id)
    return SimpleNamespace(work_item_id=work_item_id, request_id=facts.request_id, epoch=epoch)


def _set_comment(
    request_id: uuid.UUID,
    *,
    subject_title: str | None = None,
    declaration: dict[str, Any] | None = None,
) -> None:
    async def body(session: AsyncSession) -> None:
        changed = await session.execute(
            update(FactoryStatusComment)
            .where(FactoryStatusComment.execution_request_id == request_id)
            .values(subject_title=subject_title, declaration=declaration)
        )
        assert changed.rowcount == 1  # type: ignore[attr-defined]
        await session.commit()

    with_session(body)


def _drop_comment(request_id: uuid.UUID) -> None:
    async def body(session: AsyncSession) -> None:
        await session.execute(
            delete(FactoryStatusComment).where(
                FactoryStatusComment.execution_request_id == request_id
            )
        )
        await session.commit()

    with_session(body)


def _report(request_id: uuid.UUID, entries: list[tuple[str, int | None, str | None]]) -> None:
    async def body(session: AsyncSession) -> None:
        for phase, loop_round, note in entries:
            session.add(
                ExecutionRequestPhaseReport(
                    execution_request_id=request_id,
                    phase=phase,
                    loop_round=loop_round,
                    note=note,
                )
            )
            # One flush per report so the identity order is the report order.
            await session.flush()
        await session.commit()

    with_session(body)


def _expected_progress(request_id: uuid.UUID) -> dict[str, Any]:
    """phase_view on the stored rows, exactly as the status card route calls it."""

    async def body(session: AsyncSession) -> dict[str, Any]:
        request = await session.get(ExecutionRequest, request_id)
        row = await session.get(FactoryStatusComment, request_id)
        assert request is not None and row is not None
        reports = list(
            (
                await session.scalars(
                    select(ExecutionRequestPhaseReport)
                    .where(ExecutionRequestPhaseReport.execution_request_id == request_id)
                    .order_by(ExecutionRequestPhaseReport.id)
                )
            ).all()
        )
        view = phase_view(
            row.declaration or {"phases": [], "loops": []},
            reports,
            request.status,
            request.terminal_cause,
        )
        slots = view.stages if view.staged else view.phases
        return {
            "current": view.current,
            "note": next((r.note for r in reversed(reports) if r.note), None),
            "stages": [
                {
                    "id": slot.id,
                    "label": slot.label,
                    "state": slot.state,
                    "round_label": slot.round_label,
                }
                for slot in slots
            ],
        }

    return with_session(body)


def _detail(
    client: TestClient, auth_headers: dict[str, str], work_item_id: uuid.UUID
) -> dict[str, Any]:
    response = client.get(f"/work-items/{work_item_id}", headers=auth_headers)
    assert response.status_code == 200, response.text
    return dict(response.json())


def _listed(
    client: TestClient, auth_headers: dict[str, str], work_item_id: uuid.UUID
) -> dict[str, Any]:
    response = client.get("/work-items", headers=auth_headers)
    assert response.status_code == 200, response.text
    matches = [i for i in response.json()["items"] if i["id"] == str(work_item_id)]
    assert len(matches) == 1, response.json()
    return dict(matches[0])


def _both(
    client: TestClient, auth_headers: dict[str, str], work_item_id: uuid.UUID
) -> list[dict[str, Any]]:
    return [
        _listed(client, auth_headers, work_item_id),
        _detail(client, auth_headers, work_item_id),
    ]


# --- title and progress -----------------------------------------------------


def test_work_item_progress_staged_kicked_back_loop_matches_phase_view(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    seeded = _running(_agent(stack, auth_headers), 4102)
    _set_comment(seeded.request_id, subject_title=TITLE, declaration=STAGED_DECLARATION)
    _report(seeded.request_id, KICKED_BACK)
    expected = _expected_progress(seeded.request_id)

    # Not vacuous: the kickback shows as a redo stage and a round badge.
    by_id = {stage["id"]: stage for stage in expected["stages"]}
    assert expected["current"] == "plan"
    assert by_id["plan"]["state"] == "current"
    assert by_id["plan_review"]["state"] == "redo"
    assert by_id["plan_review"]["round_label"] == "round 2 of 3"
    assert [stage["id"] for stage in expected["stages"]] == [
        "plan",
        "plan_review",
        "implement",
        "review_diff",
        "wait_ci",
    ]

    for body in _both(stack, auth_headers, seeded.work_item_id):
        assert body["title"] == TITLE
        assert body["progress"] == expected
        assert body["progress"]["note"] == "Plan misses the Kelvin case"


def test_work_item_progress_unstaged_declaration_is_one_stage_per_phase(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    seeded = _running(_agent(stack, auth_headers), 4103)
    _set_comment(seeded.request_id, subject_title=TITLE, declaration=DECLARATION)
    _report(
        seeded.request_id,
        [
            ("read_issue", None, "Reading"),
            ("plan", 1, None),
            ("plan_review", 1, None),
            ("plan", 2, "Second plan"),
        ],
    )
    expected = _expected_progress(seeded.request_id)

    assert [stage["id"] for stage in expected["stages"]] == [
        phase["id"] for phase in DECLARATION["phases"]
    ]
    assert {stage["id"]: stage["state"] for stage in expected["stages"]}["plan_review"] == "redo"
    for body in _both(stack, auth_headers, seeded.work_item_id):
        assert body["progress"] == expected
        assert len(body["progress"]["stages"]) == len(DECLARATION["phases"])


def test_work_item_progress_note_is_the_latest_non_empty_note(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    seeded = _running(_agent(stack, auth_headers), 4104)
    _set_comment(seeded.request_id, subject_title=TITLE, declaration=STAGED_DECLARATION)
    _report(
        seeded.request_id,
        [
            ("read_issue", None, "Reading the issue"),
            ("pin_criteria", None, "Pinned three criteria"),
            ("plan", 1, None),
        ],
    )

    for body in _both(stack, auth_headers, seeded.work_item_id):
        assert body["progress"]["note"] == "Pinned three criteria"
        assert body["progress"]["current"] == "plan"


def test_work_item_without_status_comment_has_null_title_and_progress(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    seeded = _running(_agent(stack, auth_headers), 4105)
    _drop_comment(seeded.request_id)

    for body in _both(stack, auth_headers, seeded.work_item_id):
        assert "title" in body and body["title"] is None
        assert "progress" in body and body["progress"] is None


def test_work_item_progress_null_when_latest_request_has_no_status_comment(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(stack, auth_headers)
    first = _running(agent_id, 4106)
    _set_comment(first.request_id, subject_title=TITLE, declaration=STAGED_DECLARATION)
    _report(first.request_id, KICKED_BACK)
    _fail(first.request_id, first.epoch)
    second = _running(agent_id, 4106)
    assert second.work_item_id == first.work_item_id
    _drop_comment(second.request_id)

    for body in _both(stack, auth_headers, first.work_item_id):
        assert [r["sequence"] for r in body["requests"]] == [1, 2]
        assert body["title"] == TITLE
        assert "progress" in body and body["progress"] is None


def test_work_item_title_is_the_newest_status_comment_title_verbatim(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(stack, auth_headers)
    first = _running(agent_id, 4107)
    _set_comment(first.request_id, subject_title="Old title")
    _fail(first.request_id, first.epoch)
    second = _running(agent_id, 4107)
    _set_comment(second.request_id, subject_title=TITLE)
    _fail(second.request_id, second.epoch)

    for body in _both(stack, auth_headers, first.work_item_id):
        # The newest row wins over an older titled one.
        assert body["title"] == TITLE

    third = _running(agent_id, 4107)
    assert third.work_item_id == first.work_item_id
    for body in _both(stack, auth_headers, first.work_item_id):
        # Decision 3: the newest row's subject_title, even before it is filled.
        assert body["title"] is None


def test_work_item_progress_with_null_declaration_is_empty_not_null(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    seeded = _running(_agent(stack, auth_headers), 4108)
    _report(seeded.request_id, [("plan", 1, "Planning before declaring")])
    expected = _expected_progress(seeded.request_id)
    assert expected["stages"] == []

    for body in _both(stack, auth_headers, seeded.work_item_id):
        assert body["title"] is None
        assert body["progress"] is not None
        assert body["progress"] == expected


# --- batching ---------------------------------------------------------------


def _list_statement_count(client: TestClient, auth_headers: dict[str, str]) -> int:
    engine = client.app.state.engine.sync_engine  # type: ignore[attr-defined]
    statements: list[str] = []

    def count(*args: Any) -> None:
        statements.append(str(args[2]))

    event.listen(engine, "before_cursor_execute", count)
    try:
        response = client.get("/work-items", headers=auth_headers)
    finally:
        event.remove(engine, "before_cursor_execute", count)
    assert response.status_code == 200, response.text
    for item in response.json()["items"]:
        assert item["progress"] is not None and item["progress"]["stages"]
        assert item["title"] == TITLE
    return len(statements)


def _seed_progress_items(agent_id: str, issue_numbers: range) -> None:
    for issue_number in issue_numbers:
        seeded = _running(agent_id, issue_number)
        _set_comment(seeded.request_id, subject_title=TITLE, declaration=STAGED_DECLARATION)
        _report(seeded.request_id, KICKED_BACK)


def test_work_item_list_statement_count_is_constant_in_item_count(
    stack: TestClient, auth_headers: dict[str, str]
) -> None:
    agent_id = _agent(stack, auth_headers)
    _seed_progress_items(agent_id, range(5000, 5001))
    one = _list_statement_count(stack, auth_headers)

    _seed_progress_items(agent_id, range(5001, 5020))
    response = stack.get("/work-items", headers=auth_headers)
    assert len(response.json()["items"]) == 20
    twenty = _list_statement_count(stack, auth_headers)

    assert one > 0
    assert twenty == one
