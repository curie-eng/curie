"""Tracker result proposals and their exactly-once delivery (ADR 0277 decision 4, #5585).

Criterion 3: the typed proposal against ``tests/vectors/tracker-result-proposal.json``.
Criterion 5: delivery on real Postgres. The source WorkItem is admitted through
a signed GitHub issues webhook against create_app() and made running through
`workitem_dispatch.acquire` and `start`, as test_factory_terminus does. The
tracker is the in-memory adapter keyed to that WorkItem's issue. Grants are
ORM inserts using the production `source_fingerprint` and `output_identity_for`;
only tests create grants in #5585. Time moves only by SQL on this file's rows.
Criterion 6: the storage boundary, by planted sentinels.
"""

# ruff: noqa: F811  (the shared ``admitted`` fixture is imported, then requested)

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from curie_api.config import Settings, get_settings
from curie_api.factory_tracker_result_delivery import (
    DeliveryOutcome,
    DeliveryState,
    RefusalReason,
    TrackerResultRefused,
    TrackerResultStorageError,
    deliver,
    expire_overdue,
    output_identity_for,
    result_marker,
    source_fingerprint,
    submit_result,
)
from curie_api.factory_tracker_results import (
    FindingsReason,
    PlatformFindings,
    ProposalInvalid,
    ResultProposal,
    parse_proposal,
    proposal_content,
    proposal_sha256,
)
from curie_api.forges.capabilities import TRACKER_OPERATIONS, Operation, Support
from curie_api.forges.memory import InMemoryTracker, all_supported
from curie_api.forges.types import ReplyTarget, TrackerIssueRef
from curie_api.models import (
    ExecutionRequest,
    FactoryTrackerResultArtifact,
    FactoryTrackerResultGrant,
    WorkItem,
)
from curie_api.workitem_dispatch import DispatchConflict, acquire, start
from forge_fakes import factory_wake
from forge_fakes.github import LABEL, REPO_ID, GitHubAPI, _issue_event, _post
from forge_fakes.github_comments import (  # noqa: F401  (fixtures)
    _rows,
    admitted,
    comments,
)
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

VECTOR = Path(__file__).resolve().parents[3] / "tests" / "vectors" / "tracker-result-proposal.json"
REVISION = "3f2a9c0d4b5e6f708192a3b4c5d6e7f8091a2b3c"


# Criterion 3: the proposal ------------------------------------------------------


def _vector() -> dict[str, Any]:
    data: dict[str, Any] = json.loads(VECTOR.read_text(encoding="utf-8"))
    assert set(data) == {"comment", "valid", "invalid"}
    return data


def _filled(case: dict[str, Any]) -> Any:
    proposal = json.loads(json.dumps(case["proposal"]))
    fill = case.get("fill")
    if fill is not None:
        (key,) = fill["path"]
        proposal[key] = fill["char"] * fill["length"]
    return proposal


def _independent_sha256(proposal: dict[str, Any]) -> str:
    present = {key: value for key, value in proposal.items() if value is not None}
    canonical = json.dumps(present, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def test_every_valid_vector_parses_with_its_digest() -> None:
    cases = _vector()["valid"]
    assert {case["proposal"]["kind"] for case in cases} == {"ticket", "findings"}
    for case in cases:
        proposal = parse_proposal(case["proposal"])
        assert proposal.kind == case["proposal"]["kind"], case["name"]
        assert proposal_sha256(proposal) == case["proposal_sha256"], case["name"]
        assert _independent_sha256(case["proposal"]) == case["proposal_sha256"], case["name"]
        # Key order does not move the digest.
        reordered = dict(reversed(list(case["proposal"].items())))
        assert proposal_sha256(parse_proposal(reordered)) == case["proposal_sha256"]


def test_every_invalid_vector_reports_exactly_its_issues() -> None:
    names = set()
    for case in _vector()["invalid"]:
        names.add(case["name"])
        with pytest.raises(ProposalInvalid) as refused:
            parse_proposal(_filled(case))
        got = sorted([issue.field, issue.code] for issue in refused.value.issues)
        assert got == sorted(case["issues"]), case["name"]
    # The cases criterion 3 names.
    assert {
        "submitted_budget_exhausted",
        "destination_repository",
        "destination_labels",
        "destination_agent",
        "destination_assignee",
        "destination_destination",
        "unknown_field",
        "oversized_body",
        "revision_39_hex",
        "revision_uppercase",
    } <= names


def test_a_refusal_never_carries_submitted_text() -> None:
    planted = "plantedvaluewombat"
    raw = {
        "kind": "findings",
        "reason": f"budget_exhausted {planted}",
        "tried": planted * 300,
        "missing": f"{planted}\x00",
        "revision": planted,
        f"{planted} key": planted,
        "destination": planted,
    }
    with pytest.raises(ProposalInvalid) as refused:
        parse_proposal(raw)
    rendered = repr(refused.value.issues) + str(refused.value) + repr(refused.value.args)
    assert planted not in rendered
    assert refused.value.__cause__ is None and refused.value.__context__ is None
    assert {issue.code for issue in refused.value.issues} == {
        "value",
        "length",
        "text",
        "revision",
        "unknown_field",
    }


def test_budget_exhausted_is_platform_only() -> None:
    platform = PlatformFindings(
        kind="findings", reason=FindingsReason.BUDGET_EXHAUSTED, tried="t", missing="m"
    )
    assert platform.model_dump(mode="json")["reason"] == "budget_exhausted"
    with pytest.raises(ProposalInvalid) as refused:
        parse_proposal(platform.model_dump(mode="json"))
    assert [(issue.field, issue.code) for issue in refused.value.issues] == [("reason", "value")]


def test_the_delivery_deadline_setting_is_a_positive_finite_number(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = "CURIE_FACTORY_TRACKER_RESULT_DELIVERY_DEADLINE_SECONDS"
    monkeypatch.delenv(name, raising=False)
    assert Settings().factory_tracker_result_delivery_deadline_seconds == 900
    for value in ("0", "-1", "inf", "86401"):
        monkeypatch.setenv(name, value)
        with pytest.raises(ValidationError):
            Settings()
    monkeypatch.setenv(name, "60")
    assert Settings().factory_tracker_result_delivery_deadline_seconds == 60


# Delivery setup -----------------------------------------------------------------


def _run[T](work: Callable[[async_sessionmaker[AsyncSession]], Awaitable[T]]) -> T:
    """Run service code on this thread's own loop and engine.

    A `BaseException` raised inside (a modelled process death) propagates
    after the engine is disposed.
    """

    async def go() -> T:
        engine = create_async_engine(get_settings().database_url)
        try:
            return await work(async_sessionmaker(engine, expire_on_commit=False))
        finally:
            await engine.dispose()

    return asyncio.run(go())


def _exec(statement: str, params: dict[str, Any] | None = None) -> None:
    async def work(maker: async_sessionmaker[AsyncSession]) -> None:
        async with maker() as session:
            await session.execute(text(statement), params or {})
            await session.commit()

    _run(work)


def _label(client: Any, github: GitHubAPI, number: int) -> None:
    github.issue_number = number
    github.advance_label_event(number)
    response = _post(client, "issues", _issue_event("labeled", number, label={"name": LABEL}))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "factory_admitted"


def _start_running(client: Any, request_id: uuid.UUID) -> int:
    async def go() -> int:
        async with client.app.state.sessionmaker() as session:
            acquired = await acquire(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                wake=await factory_wake(session, request_id),
                context=None,
                client=client.app.state.http_client,
            )
            assert not isinstance(acquired, DispatchConflict), acquired
            started = await start(
                session,
                request_id,
                owner="factory-owner",
                generation=1,
                claim_name=f"claim-{request_id.hex[:12]}",
                sandbox_name=f"sbx-{request_id.hex[:12]}",
                context=acquired.context,
            )
            assert not isinstance(started, DispatchConflict), started
            return int(started.runtime_epoch)

    return int(client.portal.call(go))


@dataclass
class _Source:
    request_id: uuid.UUID
    work_item_id: uuid.UUID
    epoch: int
    tracker: InMemoryTracker
    issue: TrackerIssueRef
    grant_id: uuid.UUID


def _grant(request_id: uuid.UUID, kinds: tuple[str, ...] = ("findings", "ticket")) -> uuid.UUID:
    async def work(maker: async_sessionmaker[AsyncSession]) -> uuid.UUID:
        async with maker() as session:
            request = await session.get(ExecutionRequest, request_id)
            assert request is not None
            work_item = await session.get(WorkItem, request.work_item_id)
            assert work_item is not None
            grant = FactoryTrackerResultGrant(
                id=uuid.uuid4(),
                work_item_id=work_item.id,
                execution_request_id=request.id,
                source_fingerprint=source_fingerprint(work_item, request),
                repository_id=work_item.repository_project_id,
                allowed_result_kinds=sorted(kinds),
                output_identity=output_identity_for(request.id),
                implementation_label="factory-implement" if "ticket" in kinds else None,
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
            session.add(grant)
            await session.commit()
            return grant.id

    return _run(work)


def _source(
    admitted: Any, number: int, *, kinds: tuple[str, ...] = ("findings", "ticket")
) -> _Source:
    client, github, _sink = admitted
    _label(client, github, number)
    (row,) = _rows(
        "SELECT r.id AS request_id, w.id AS work_item_id, w.tracker_kind, w.tracker_host, "
        "w.tracker_scope_id, w.tracker_issue_id FROM curie.execution_requests r "
        "JOIN curie.work_items w ON w.id = r.work_item_id "
        "WHERE w.tracker_scope_id = :scope AND w.tracker_issue_id = :number",
        {"scope": str(REPO_ID), "number": str(number)},
    )
    epoch = _start_running(client, row["request_id"])
    tracker = InMemoryTracker(
        kind=row["tracker_kind"], host=row["tracker_host"], scope_id=row["tracker_scope_id"]
    )
    issue = tracker.seed_issue(
        "Export drops a row", "Reported symptom.", issue_id=row["tracker_issue_id"]
    )
    return _Source(
        request_id=row["request_id"],
        work_item_id=row["work_item_id"],
        epoch=epoch,
        tracker=tracker,
        issue=issue,
        grant_id=_grant(row["request_id"], kinds),
    )


def _ticket(**over: Any) -> ResultProposal:
    raw: dict[str, Any] = {
        "kind": "ticket",
        "title": "Export drops the last row",
        "body": "## Problem\n\nThe export omits the final row.",
        "reproduction": {
            "command": "uv run pytest tests/test_export.py -q",
            "observed": "1 failed: expected 3 rows, got 2",
            "revision": REVISION,
        },
    }
    raw.update(over)
    return parse_proposal(raw)


def _findings(**over: Any) -> ResultProposal:
    raw: dict[str, Any] = {
        "kind": "findings",
        "reason": "not_reproduced",
        "tried": "Ran the export three times on the sample data.",
        "missing": "A dataset that shows the dropped row.",
        "revision": REVISION,
    }
    raw.update(over)
    return parse_proposal(raw)


def _proposal(kind: str) -> ResultProposal:
    return _ticket() if kind == "ticket" else _findings()


def _submit(
    src: _Source,
    proposal: ResultProposal,
    *,
    grant_id: uuid.UUID | None = None,
    request_id: uuid.UUID | None = None,
    epoch: int | None = None,
    tracker: InMemoryTracker | None = None,
    settings: Settings | None = None,
) -> uuid.UUID:
    async def work(maker: async_sessionmaker[AsyncSession]) -> uuid.UUID:
        async with maker() as session:
            return await submit_result(
                session,
                grant_id=grant_id or src.grant_id,
                execution_request_id=request_id or src.request_id,
                runtime_epoch=src.epoch if epoch is None else epoch,
                proposal=proposal,
                tracker=tracker or src.tracker,
                settings=settings or get_settings(),
            )

    return _run(work)


def _deliver(src: _Source, delivery_id: uuid.UUID, *, owner: str = "deliverer") -> DeliveryOutcome:
    async def work(maker: async_sessionmaker[AsyncSession]) -> DeliveryOutcome:
        return await deliver(
            maker, delivery_id, tracker=src.tracker, settings=get_settings(), owner=owner
        )

    return _run(work)


def _delivery(delivery_id: uuid.UUID) -> dict[str, Any]:
    (row,) = _rows(
        "SELECT state, attempts, last_error_code, failure_code, output_issue_id, "
        "attempt_owner, submitted_at, deadline_at "
        "FROM curie.factory_tracker_result_deliveries WHERE id = :id",
        {"id": delivery_id},
    )
    return dict(row)


def _count(table: str, delivery_id: uuid.UUID | None = None) -> int:
    where = "" if delivery_id is None else " WHERE delivery_id = :id"
    (row,) = _rows(
        f"SELECT count(*) AS n FROM curie.{table}{where}",
        {} if delivery_id is None else {"id": delivery_id},
    )
    return int(row["n"])


def _deliveries() -> int:
    (row,) = _rows("SELECT count(*) AS n FROM curie.factory_tracker_result_deliveries")
    return int(row["n"])


def _comments(src: _Source) -> list[str]:
    target = ReplyTarget.on_issue(src.issue)
    return [body for author, body in src.tracker.marked_comments.comments(target)]


def _own_issues(src: _Source) -> list[TrackerIssueRef]:
    return src.tracker.issues(author=src.tracker.identity)


def _age_lease(delivery_id: uuid.UUID) -> None:
    _exec(
        "UPDATE curie.factory_tracker_result_deliveries "
        "SET attempt_lease_expires_at = clock_timestamp() - interval '1 second' "
        "WHERE id = :id",
        {"id": delivery_id},
    )


def _age_deadline(delivery_id: uuid.UUID) -> None:
    _exec(
        "UPDATE curie.factory_tracker_result_deliveries "
        "SET submitted_at = clock_timestamp() - interval '2 seconds', "
        "deadline_at = clock_timestamp() - interval '1 second' WHERE id = :id",
        {"id": delivery_id},
    )


def _assert_delivered_once(src: _Source, delivery_id: uuid.UUID, kind: str) -> None:
    row = _delivery(delivery_id)
    assert (row["state"], row["failure_code"], row["attempt_owner"]) == ("delivered", None, None)
    assert _count("factory_tracker_result_receipts", delivery_id) == 1
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0
    comments = _comments(src)
    assert len(comments) == 1
    assert result_marker(src.request_id) in comments[0]
    (receipt,) = _rows(
        "SELECT result_kind, reason, revision, result_comment_id, output_issue_id, "
        "output_url, output_body_sha256 FROM curie.factory_tracker_result_receipts "
        "WHERE delivery_id = :id",
        {"id": delivery_id},
    )
    assert receipt["result_kind"] == kind
    assert receipt["revision"] == REVISION
    if kind == "ticket":
        (issue,) = _own_issues(src)
        assert len(src.tracker.sent_issue_bodies) == 1
        assert receipt["output_issue_id"] == issue.issue_id == row["output_issue_id"]
        assert receipt["output_url"] == src.tracker.issue_url(issue)
        assert (
            receipt["output_body_sha256"]
            == hashlib.sha256(src.tracker.issue_body(issue).encode()).hexdigest()
        )
        assert receipt["reason"] is None
        # The planted reproduction values and the link, nothing about wording.
        assert "uv run pytest tests/test_export.py -q" in comments[0]
        assert "1 failed: expected 3 rows, got 2" in comments[0]
        assert receipt["output_url"] in comments[0]
    else:
        assert _own_issues(src) == []
        assert receipt["output_issue_id"] is None
        assert receipt["reason"] == "not_reproduced"
        assert "Ran the export three times on the sample data." in comments[0]
        assert "A dataset that shows the dropped row." in comments[0]


# Criterion 5: exactly once ----------------------------------------------------------


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_duplicate_submission_delivers_once(admitted: Any, kind: str) -> None:
    src = _source(admitted, 9301)
    proposal = _proposal(kind)
    first = _submit(src, proposal)
    assert _submit(src, proposal) == first
    assert _deliveries() == 1
    assert src.tracker.ledger.writes == 0
    outcome = _deliver(src, first)
    assert (outcome.state, outcome.attempted) == (DeliveryState.DELIVERED, True)
    _assert_delivered_once(src, first, kind)
    writes = src.tracker.ledger.writes
    # Delivered: the same submission returns the same id and a second attempt writes nothing.
    assert _submit(src, proposal) == first
    again = _deliver(src, first)
    assert (again.state, again.attempted) == (DeliveryState.DELIVERED, False)
    assert src.tracker.ledger.writes == writes
    # Another proposal under the same grant is a conflict.
    other = _ticket(title="Another title") if kind == "ticket" else _findings(missing="Other.")
    with pytest.raises(TrackerResultRefused) as refused:
        _submit(src, other)
    assert refused.value.reason is RefusalReason.PROPOSAL_CONFLICT
    _assert_delivered_once(src, first, kind)


class _Restart(BaseException):
    """The process dies; not an `Exception`, so nothing in the service catches it."""


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_restart_between_the_issue_write_and_the_comment_write_delivers_once(
    admitted: Any, kind: str
) -> None:
    src = _source(admitted, 9302)
    delivery_id = _submit(src, _proposal(kind))
    src.tracker.write_faults.abort_before_next(Operation.UPSERT_MARKED, _Restart())
    with pytest.raises(_Restart):
        _deliver(src, delivery_id, owner="first-process")
    assert _comments(src) == []
    row = _delivery(delivery_id)
    assert (row["state"], row["attempt_owner"]) == ("owed", "first-process")
    if kind == "ticket":
        # The issue was written and recorded before the comment.
        (issue,) = _own_issues(src)
        assert row["output_issue_id"] == issue.issue_id
    # The dead attempt's lease still holds.
    held = _deliver(src, delivery_id, owner="second-process")
    assert (held.state, held.attempted) == (DeliveryState.OWED, False)
    _age_lease(delivery_id)
    outcome = _deliver(src, delivery_id, owner="second-process")
    assert outcome.state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, kind)
    assert _delivery(delivery_id)["attempts"] == 2


def test_a_lost_create_response_reconciles_to_the_one_issue(admitted: Any) -> None:
    src = _source(admitted, 9303)
    delivery_id = _submit(src, _ticket())
    src.tracker.write_faults.lose_next_response(Operation.CREATE_LINKED_ISSUE)
    lost = _deliver(src, delivery_id)
    assert (lost.state, lost.last_error_code) == (DeliveryState.OWED, "tracker_unavailable")
    assert len(_own_issues(src)) == 1
    assert _comments(src) == []
    assert _delivery(delivery_id)["output_issue_id"] is None
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    outcome = _deliver(src, delivery_id)
    assert outcome.state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "ticket")


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_lost_comment_response_delivers_one_comment(admitted: Any, kind: str) -> None:
    src = _source(admitted, 9304)
    delivery_id = _submit(src, _proposal(kind))
    src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    lost = _deliver(src, delivery_id)
    assert (lost.state, lost.last_error_code) == (DeliveryState.OWED, "tracker_unavailable")
    assert len(_comments(src)) == 1
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 1
    outcome = _deliver(src, delivery_id)
    assert outcome.state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, kind)


# Criterion 5: fences ----------------------------------------------------------------


_SUBMISSION_FENCES = [
    "source_fingerprint_changed",
    "stale_epoch",
    "lapsed_heartbeat",
    "cancelled",
    "grant_for_another_request",
    "grant_not_found",
    "grant_scope_mismatch",
    "grant_expired",
    "result_kind_not_allowed",
    "tracker_mismatch",
    "tracker_operation_unsupported",
    "closes_source",
]


@pytest.mark.parametrize("fence", _SUBMISSION_FENCES)
def test_a_failed_fence_refuses_submission_and_writes_nothing(admitted: Any, fence: str) -> None:
    kinds = ("findings",) if fence == "result_kind_not_allowed" else ("findings", "ticket")
    src = _source(admitted, 9311, kinds=kinds)
    params = {"work_item": src.work_item_id, "request": src.request_id, "grant": src.grant_id}
    bad: dict[str, Any] = {}
    (admitted_with,) = _rows(
        "SELECT trigger_label FROM curie.work_items WHERE id = :work_item", params
    )
    proposal = _ticket()
    other_tracker: InMemoryTracker | None = None
    expected = {
        "stale_epoch": RefusalReason.OWNER_NOT_LIVE,
        "lapsed_heartbeat": RefusalReason.OWNER_NOT_LIVE,
        "grant_for_another_request": RefusalReason.GRANT_REQUEST_MISMATCH,
    }.get(fence) or RefusalReason(fence)
    if fence == "source_fingerprint_changed":
        # A relabel to another trigger moves the admission facts.
        _exec(
            "UPDATE curie.work_items SET trigger_label = 'other-trigger' WHERE id = :work_item",
            params,
        )
    elif fence == "stale_epoch":
        bad["epoch"] = src.epoch + 1
    elif fence == "lapsed_heartbeat":
        _exec(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :request",
            params,
        )
    elif fence == "cancelled":
        _exec(
            "UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :work_item",
            params,
        )
    elif fence == "grant_for_another_request":
        other = _source(admitted, 9312)
        bad.update(request_id=other.request_id, epoch=other.epoch)
    elif fence == "grant_not_found":
        bad["grant_id"] = uuid.uuid4()
    elif fence == "grant_scope_mismatch":
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET repository_id = '999999' "
            "WHERE id = :grant",
            params,
        )
    elif fence == "grant_expired":
        _exec(
            "UPDATE curie.factory_tracker_result_grants "
            "SET expires_at = clock_timestamp() - interval '1 second' WHERE id = :grant",
            params,
        )
    elif fence == "result_kind_not_allowed":
        pass
    elif fence == "tracker_mismatch":
        other_tracker = InMemoryTracker(
            kind=src.tracker.kind, host="tracker.other.example", scope_id=src.tracker.scope_id
        )
        bad["tracker"] = other_tracker
    elif fence == "tracker_operation_unsupported":
        capabilities = all_supported(TRACKER_OPERATIONS)
        capabilities[Operation.CREATE_LINKED_ISSUE] = Support.UNSUPPORTED
        other_tracker = InMemoryTracker(
            kind=src.tracker.kind,
            host=src.tracker.host,
            scope_id=src.tracker.scope_id,
            capabilities=capabilities,
        )
        other_tracker.seed_issue("report", "symptom", issue_id=src.issue.issue_id)
        bad["tracker"] = other_tracker
    elif fence == "closes_source":
        bad["proposal"] = _ticket(body=f"Fixes #{src.issue.issue_id} for good.")
    with pytest.raises(TrackerResultRefused) as refused:
        _submit(src, bad.pop("proposal", proposal), **bad)
    assert refused.value.reason is expected
    assert _deliveries() == 0
    assert _count("factory_tracker_result_artifacts") == 0
    assert src.tracker.ledger.writes == 0
    if other_tracker is not None:
        assert other_tracker.ledger.writes == 0

    # Liveness control: the same setup with the fence satisfied delivers.
    if fence == "source_fingerprint_changed":
        _exec(
            "UPDATE curie.work_items SET trigger_label = :label WHERE id = :work_item",
            {**params, "label": admitted_with["trigger_label"]},
        )
    elif fence == "lapsed_heartbeat":
        _exec(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() + interval '60 seconds' WHERE id = :request",
            params,
        )
    elif fence == "cancelled":
        _exec("UPDATE curie.work_items SET cancelled_at = NULL WHERE id = :work_item", params)
    elif fence == "grant_scope_mismatch":
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET repository_id = :repo "
            "WHERE id = :grant",
            {**params, "repo": str(REPO_ID)},
        )
    elif fence == "grant_expired":
        _exec(
            "UPDATE curie.factory_tracker_result_grants "
            "SET expires_at = clock_timestamp() + interval '1 hour' WHERE id = :grant",
            params,
        )
    elif fence == "result_kind_not_allowed":
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET allowed_result_kinds = "
            "'[\"findings\", \"ticket\"]'::jsonb, implementation_label = 'factory-implement' "
            "WHERE id = :grant",
            params,
        )
    delivery_id = _submit(src, proposal)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "ticket")


_ATTEMPT_FENCES = [
    "none",
    "source_fingerprint_changed",
    "stale_epoch",
    "lapsed_heartbeat",
    "cancelled",
    "grant_for_another_request",
    "grant_scope_mismatch",
]


@pytest.mark.parametrize("fence", _ATTEMPT_FENCES)
def test_a_fence_failing_at_attempt_time_settles_failed_and_writes_nothing(
    admitted: Any, fence: str
) -> None:
    src = _source(admitted, 9321)
    delivery_id = _submit(src, _ticket())
    params = {"work_item": src.work_item_id, "request": src.request_id, "grant": src.grant_id}
    expected = fence
    if fence == "source_fingerprint_changed":
        _exec(
            "UPDATE curie.work_items SET trigger_label = 'other-trigger' WHERE id = :work_item",
            params,
        )
    elif fence == "stale_epoch":
        expected = "owner_not_live"
        _exec(
            "UPDATE curie.execution_requests SET runtime_epoch = runtime_epoch + 1 "
            "WHERE id = :request",
            params,
        )
    elif fence == "lapsed_heartbeat":
        expected = "owner_not_live"
        _exec(
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :request",
            params,
        )
    elif fence == "cancelled":
        _exec(
            "UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :work_item",
            params,
        )
    elif fence == "grant_for_another_request":
        expected = "grant_request_mismatch"
        other = _source(admitted, 9322)
        _exec(
            "DELETE FROM curie.factory_tracker_result_grants WHERE id = :other",
            {"other": other.grant_id},
        )
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET execution_request_id = :other "
            "WHERE id = :grant",
            {**params, "other": other.request_id},
        )
    elif fence == "grant_scope_mismatch":
        _exec(
            "UPDATE curie.factory_tracker_result_grants SET repository_id = '999999' "
            "WHERE id = :grant",
            params,
        )
    outcome = _deliver(src, delivery_id)
    if fence == "none":
        assert outcome.state is DeliveryState.DELIVERED
        _assert_delivered_once(src, delivery_id, "ticket")
        return
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, expected)
    assert src.tracker.ledger.writes == 0
    assert _own_issues(src) == [] and _comments(src) == []
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    # Failed is final.
    assert _deliver(src, delivery_id).attempted is False


def test_two_owned_issues_with_the_marker_fail_ambiguous_and_create_nothing(
    admitted: Any,
) -> None:
    src = _source(admitted, 9331)
    embedded = src.tracker.embed_linked_marker(output_identity_for(src.request_id))
    for _ in range(2):
        src.tracker.seed_issue("earlier", f"text\n\n{embedded}\n", author=src.tracker.identity)
    delivery_id = _submit(src, _ticket())
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, "output_ambiguous")
    assert len(_own_issues(src)) == 2
    assert src.tracker.ledger.writes == 0 and _comments(src) == []
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


# Criterion 5: read-back and deadline -----------------------------------------------------


def test_an_issue_read_back_mismatch_is_not_delivered_and_then_fails_at_the_deadline(
    admitted: Any,
) -> None:
    src = _source(admitted, 9341)
    delivery_id = _submit(src, _ticket())
    src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    assert _deliver(src, delivery_id).last_error_code == "tracker_unavailable"
    (issue,) = _own_issues(src)
    # A person edits the output issue; the marker stays its last line.
    src.tracker.edit_issue_body(issue, "edited by a person\n\n" + src.tracker.issue_body(issue))
    mismatch = _deliver(src, delivery_id)
    assert (mismatch.state, mismatch.last_error_code) == (DeliveryState.OWED, "readback_mismatch")
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert len(_own_issues(src)) == 1
    writes = src.tracker.ledger.writes
    _age_deadline(delivery_id)
    failed = _deliver(src, delivery_id)
    assert (failed.state, failed.failure_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
    )
    assert src.tracker.ledger.writes == writes
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


def test_a_comment_read_back_that_misses_is_not_delivered(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = _source(admitted, 9342)
    delivery_id = _submit(src, _findings())
    comments = src.tracker.marked_comments
    real_upsert, real_find = comments.upsert_marked, comments.find_marked
    upserted: list[bool] = []

    async def upsert(target: ReplyTarget, marker: str, body: str) -> Any:
        result = await real_upsert(target, marker, body)
        upserted.append(True)
        return result

    async def find(target: ReplyTarget, marker: str) -> Any:
        # The read-back after the write sees no comment.
        return None if upserted else await real_find(target, marker)

    monkeypatch.setattr(comments, "upsert_marked", upsert)
    monkeypatch.setattr(comments, "find_marked", find)
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.last_error_code) == (DeliveryState.OWED, "readback_mismatch")
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    monkeypatch.setattr(comments, "upsert_marked", real_upsert)
    monkeypatch.setattr(comments, "find_marked", real_find)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    _assert_delivered_once(src, delivery_id, "findings")


def test_past_the_deadline_a_delivery_is_recorded_failed(admitted: Any) -> None:
    attempted = _source(admitted, 9351)
    sixty = get_settings().model_copy(
        update={"factory_tracker_result_delivery_deadline_seconds": 60}
    )
    first = _submit(attempted, _ticket(), settings=sixty)
    row = _delivery(first)
    assert row["deadline_at"] - row["submitted_at"] == timedelta(seconds=60)
    _age_deadline(first)
    outcome = _deliver(attempted, first)
    assert (outcome.state, outcome.failure_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
    )
    assert attempted.tracker.ledger.writes == 0
    assert _count("factory_tracker_result_artifacts", first) == 0

    # The sweep settles an owed row nothing attempted; a row inside its deadline stays owed.
    unattempted = _source(admitted, 9352)
    second = _submit(unattempted, _findings())
    live = _source(admitted, 9353)
    third = _submit(live, _findings())
    _age_deadline(second)
    assert _run(lambda maker: expire_overdue(maker)) == 1
    assert (_delivery(second)["state"], _delivery(second)["failure_code"]) == (
        "failed",
        "delivery_deadline_exceeded",
    )
    assert _count("factory_tracker_result_artifacts", second) == 0
    assert _delivery(third)["state"] == "owed"
    assert _deliver(live, third).state is DeliveryState.DELIVERED


# Criterion 6: storage boundary -------------------------------------------------------


def _tables_holding(sentinels: list[str]) -> dict[str, list[str]]:
    """Every table in the curie schema whose rows, as text, hold a sentinel."""

    async def work(maker: async_sessionmaker[AsyncSession]) -> dict[str, list[str]]:
        async with maker() as session:
            tables = (
                await session.scalars(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = 'curie' AND table_type = 'BASE TABLE'"
                    )
                )
            ).all()
            assert "factory_tracker_result_deliveries" in tables
            params = {f"s{index}": sentinel for index, sentinel in enumerate(sentinels)}
            probes = ", ".join(
                f"coalesce(bool_or(position(:s{index} in t::text) > 0), false)"
                for index in range(len(sentinels))
            )
            found: dict[str, list[str]] = {}
            for table in tables:
                hits = (
                    await session.execute(text(f'SELECT {probes} FROM curie."{table}" t'), params)
                ).one()
                held = [sentinel for sentinel, hit in zip(sentinels, hits, strict=True) if hit]
                if held:
                    found[str(table)] = held
            return found

    return _run(work)


def test_no_row_and_no_log_line_holds_submitted_text(
    admitted: Any, caplog: pytest.LogCaptureFixture
) -> None:
    # The root logger at the production level (Settings.log_level); no logger is
    # excluded and none is forced lower than production runs it.
    caplog.set_level(get_settings().log_level.upper())
    planted = {
        field: f"planted{field}{uuid.uuid4().hex[:8]}"
        for field in ("title", "body", "command", "observed", "tried", "missing")
    }
    ticket_src = _source(admitted, 9361)
    findings_src = _source(admitted, 9362)
    ticket = _ticket(
        title=f"Title {planted['title']}",
        body=f"Body {planted['body']}",
        reproduction={
            "command": f"run {planted['command']}",
            "observed": f"saw {planted['observed']}",
            "revision": REVISION,
        },
    )
    findings = _findings(tried=f"tried {planted['tried']}", missing=f"lacks {planted['missing']}")
    delivered = _submit(ticket_src, ticket)
    failed = _submit(findings_src, findings)
    # Sealed: the artifact is the one place the text lives.
    assert set(_tables_holding(list(planted.values()))) == {"factory_tracker_result_artifacts"}

    assert _deliver(ticket_src, delivered).state is DeliveryState.DELIVERED
    findings_src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    assert _deliver(findings_src, failed).last_error_code == "tracker_unavailable"
    _age_deadline(failed)
    assert _deliver(findings_src, failed).failure_code == "delivery_deadline_exceeded"

    # The tracker holds the content; the rows and the logs do not.
    (issue,) = _own_issues(ticket_src)
    assert planted["body"] in ticket_src.tracker.issue_body(issue)
    assert planted["command"] in _comments(ticket_src)[0]
    assert planted["observed"] in _comments(ticket_src)[0]
    assert planted["tried"] in _comments(findings_src)[0]
    assert _count("factory_tracker_result_artifacts") == 0
    assert _tables_holding(list(planted.values())) == {}
    logged = caplog.text + "".join(record.getMessage() for record in caplog.records)
    assert any(record.name == "curie_api.factory_tracker_results" for record in caplog.records)
    for value in planted.values():
        assert value not in logged


def test_a_failed_artifact_write_raises_a_code_only_error(
    admitted: Any, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    caplog.set_level(get_settings().log_level.upper())
    planted = f"plantedtried{uuid.uuid4().hex[:8]}"
    src = _source(admitted, 9371)
    seam = "curie_api.factory_tracker_result_delivery.proposal_content"

    def unstorable(proposal: ResultProposal) -> dict[str, object]:
        # JSONB refuses NUL, so the artifact INSERT fails with its content bound.
        return {**proposal_content(proposal), "tried": f"{planted}\x00"}

    monkeypatch.setattr(seam, unstorable)
    with pytest.raises(TrackerResultStorageError) as failed:
        _submit(src, _findings(tried=f"tried {planted}"))
    error = failed.value
    assert error.code == "artifact_write_failed"
    assert error.__cause__ is None and error.__context__ is None
    assert planted not in f"{error}{error!r}{error.args!r}"
    assert _deliveries() == 0 and _count("factory_tracker_result_artifacts") == 0
    assert src.tracker.ledger.writes == 0
    assert planted not in caplog.text + "".join(r.getMessage() for r in caplog.records)
    # Liveness control: the same proposal stores and delivers without the fault.
    monkeypatch.setattr(seam, proposal_content)
    delivery_id = _submit(src, _findings())
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED


@pytest.mark.parametrize("kind", ["ticket", "findings"])
def test_a_valid_proposal_round_trips_through_the_stored_artifact(admitted: Any, kind: str) -> None:
    src = _source(admitted, 9372)
    body = "b" * 60_000
    proposal = _ticket(body=body) if kind == "ticket" else _findings(reason="already_fixed")
    delivery_id = _submit(src, proposal)

    async def stored(maker: async_sessionmaker[AsyncSession]) -> Any:
        async with maker() as session:
            artifact = await session.get(FactoryTrackerResultArtifact, delivery_id)
            assert artifact is not None
            return artifact.content

    content = _run(stored)
    # Strict mode reads the stored JSON strings back (``reason`` included).
    assert parse_proposal(content) == proposal
    assert proposal_sha256(parse_proposal(content)) == proposal_sha256(proposal)
    assert _deliver(src, delivery_id).state is DeliveryState.DELIVERED
    if kind == "ticket":
        (issue,) = _own_issues(src)
        assert body in src.tracker.issue_body(issue)
    else:
        assert "already_fixed" in _comments(src)[0]


# F1: rechecks immediately before each tracker write ------------------------------


async def _sql_now(statements: list[tuple[str, dict[str, Any]]]) -> None:
    """Run SQL from inside an attempt, between two tracker calls."""

    engine = create_async_engine(get_settings().database_url)
    try:
        async with engine.begin() as connection:
            for statement, params in statements:
                await connection.execute(text(statement), params)
    finally:
        await engine.dispose()


def _after(
    monkeypatch: pytest.MonkeyPatch,
    owner: object,
    name: str,
    hook: Callable[[], Awaitable[None]],
) -> None:
    """Run ``hook`` right after ``owner.name`` returns, once."""

    real = getattr(owner, name)
    ran: list[bool] = []

    async def wrapped(*args: Any, **kwargs: Any) -> Any:
        result = await real(*args, **kwargs)
        if not ran:
            ran.append(True)
            await hook()
        return result

    monkeypatch.setattr(owner, name, wrapped)


_AGE_DEADLINE = (
    "UPDATE curie.factory_tracker_result_deliveries "
    "SET submitted_at = clock_timestamp() - interval '2 seconds', "
    "deadline_at = clock_timestamp() - interval '1 second' WHERE id = :delivery"
)


def _crossing(
    condition: str, src: _Source, delivery_id: uuid.UUID, other: _Source | None
) -> tuple[list[tuple[str, dict[str, Any]]], str]:
    """The SQL that crosses ``condition`` and the failure code it must end in."""

    params: dict[str, Any] = {
        "work_item": src.work_item_id,
        "request": src.request_id,
        "grant": src.grant_id,
        "delivery": delivery_id,
    }
    if condition == "source_fingerprint_changed":
        sql = ["UPDATE curie.work_items SET trigger_label = 'other-trigger' WHERE id = :work_item"]
        return [(statement, params) for statement in sql], condition
    if condition == "stale_epoch":
        sql = [
            "UPDATE curie.execution_requests SET runtime_epoch = runtime_epoch + 1 "
            "WHERE id = :request"
        ]
        return [(statement, params) for statement in sql], "owner_not_live"
    if condition == "lapsed_heartbeat":
        sql = [
            "UPDATE curie.execution_requests SET runtime_heartbeat_expires_at = "
            "clock_timestamp() - interval '1 second' WHERE id = :request"
        ]
        return [(statement, params) for statement in sql], "owner_not_live"
    if condition == "cancelled":
        sql = ["UPDATE curie.work_items SET cancelled_at = clock_timestamp() WHERE id = :work_item"]
        return [(statement, params) for statement in sql], condition
    if condition == "grant_scope_mismatch":
        sql = [
            "UPDATE curie.factory_tracker_result_grants SET repository_id = '999999' "
            "WHERE id = :grant"
        ]
        return [(statement, params) for statement in sql], condition
    if condition == "grant_for_another_request":
        assert other is not None
        moved = {**params, "other_grant": other.grant_id, "other_request": other.request_id}
        return [
            ("DELETE FROM curie.factory_tracker_result_grants WHERE id = :other_grant", moved),
            (
                "UPDATE curie.factory_tracker_result_grants "
                "SET execution_request_id = :other_request WHERE id = :grant",
                moved,
            ),
        ], "grant_request_mismatch"
    assert condition == "deadline"
    return [(_AGE_DEADLINE, params)], "delivery_deadline_exceeded"


_BETWEEN_WRITES = [
    "source_fingerprint_changed",
    "stale_epoch",
    "lapsed_heartbeat",
    "cancelled",
    "grant_scope_mismatch",
    "grant_for_another_request",
    "deadline",
]


@pytest.mark.parametrize("condition", _BETWEEN_WRITES)
def test_a_condition_crossed_between_the_issue_and_the_comment_posts_no_comment(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, condition: str
) -> None:
    src = _source(admitted, 9381)
    other = _source(admitted, 9382) if condition == "grant_for_another_request" else None
    delivery_id = _submit(src, _ticket())
    statements, code = _crossing(condition, src, delivery_id, other)

    async def cross() -> None:
        await _sql_now(statements)

    _after(monkeypatch, src.tracker, "create_linked_issue", cross)
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, code)
    assert len(_own_issues(src)) == 1
    assert _comments(src) == []
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


@pytest.mark.parametrize("condition", ["deadline", "cancelled", "lapsed_heartbeat"])
def test_a_condition_crossed_after_the_comment_is_never_recorded_delivered(
    admitted: Any, monkeypatch: pytest.MonkeyPatch, condition: str
) -> None:
    src = _source(admitted, 9383)
    delivery_id = _submit(src, _findings())
    statements, code = _crossing(condition, src, delivery_id, None)

    async def cross() -> None:
        await _sql_now(statements)

    _after(monkeypatch, src.tracker.marked_comments, "upsert_marked", cross)
    outcome = _deliver(src, delivery_id)
    assert (outcome.state, outcome.failure_code) == (DeliveryState.FAILED, code)
    assert len(_comments(src)) == 1
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


def test_the_sweep_fails_an_attempt_in_flight_and_the_late_attempt_records_nothing(
    admitted: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = _source(admitted, 9384)
    delivery_id = _submit(src, _ticket())
    swept: list[int] = []

    async def sweep() -> None:
        await _sql_now([(_AGE_DEADLINE, {"delivery": delivery_id})])
        engine = create_async_engine(get_settings().database_url)
        try:
            swept.append(await expire_overdue(async_sessionmaker(engine, expire_on_commit=False)))
        finally:
            await engine.dispose()

    _after(monkeypatch, src.tracker, "create_linked_issue", sweep)
    outcome = _deliver(src, delivery_id)
    # The sweep settled the row although this attempt held its lease.
    assert swept == [1]
    assert (outcome.state, outcome.failure_code, outcome.last_error_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
        None,
    )
    row = _delivery(delivery_id)
    # The late attempt recorded nothing: no output, no lease, one attempt.
    assert (row["output_issue_id"], row["attempt_owner"], row["attempts"]) == (None, None, 1)
    assert _comments(src) == []
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0


def test_the_sweep_settles_an_overdue_row_whose_lease_is_still_held(admitted: Any) -> None:
    src = _source(admitted, 9385)
    delivery_id = _submit(src, _findings())
    src.tracker.write_faults.abort_before_next(Operation.UPSERT_MARKED, _Restart())
    with pytest.raises(_Restart):
        _deliver(src, delivery_id, owner="dead-process")
    assert _delivery(delivery_id)["attempt_owner"] == "dead-process"
    _age_deadline(delivery_id)
    assert _run(lambda maker: expire_overdue(maker)) == 1
    row = _delivery(delivery_id)
    assert (row["state"], row["failure_code"], row["attempt_owner"]) == (
        "failed",
        "delivery_deadline_exceeded",
        None,
    )
    assert _count("factory_tracker_result_artifacts", delivery_id) == 0
    assert _deliver(src, delivery_id).attempted is False
    assert _comments(src) == []


# F2: a recorded output is never recreated -------------------------------------------


def test_a_recorded_output_whose_marker_is_gone_is_never_recreated(admitted: Any) -> None:
    src = _source(admitted, 9391)
    delivery_id = _submit(src, _ticket())
    src.tracker.write_faults.lose_next_response(Operation.UPSERT_MARKED)
    assert _deliver(src, delivery_id).last_error_code == "tracker_unavailable"
    (issue,) = _own_issues(src)
    assert _delivery(delivery_id)["output_issue_id"] == issue.issue_id
    # A person removes the marker line from the output issue.
    src.tracker.edit_issue_body(issue, "rewritten by a person, marker removed")
    missing = _deliver(src, delivery_id)
    assert (missing.state, missing.last_error_code) == (DeliveryState.OWED, "output_missing")
    assert _own_issues(src) == [issue]
    assert len(src.tracker.sent_issue_bodies) == 1
    assert _count("factory_tracker_result_receipts", delivery_id) == 0
    _age_deadline(delivery_id)
    failed = _deliver(src, delivery_id)
    assert (failed.state, failed.failure_code) == (
        DeliveryState.FAILED,
        "delivery_deadline_exceeded",
    )
    assert _own_issues(src) == [issue]
