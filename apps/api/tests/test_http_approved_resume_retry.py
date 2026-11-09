"""Prompt approved-resume retries over the API, Postgres, Valkey and TLS directory."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from approval_answer_support import (
    NATIVE_A,
    answer_as,
    approval_row,
    configured,
    dynamic,
    no_wake,
    person,
    published,
    resume_refusals,
    runtime_for,
    turn_check,
)
from approval_answer_support import selection_case as selection_case
from approval_answer_support import synthetic_provider as synthetic_provider
from approver_ticket_support import APPROVER_A, APPROVER_B
from curie_api import resumereconciler
from curie_api.config import get_settings
from curie_api.identity import http_approval_answer as answers
from curie_api.models import ApprovalStatus
from curie_api.resumereconciler import ResumeReconciler
from curie_worker.config import WorkerConfig
from curie_worker.delivery_lease import DeliveryLeaseStore
from redis import asyncio as redis_async

from apps.api.tests.test_http_turn_authority import post_check, request_for

GRACE_SECONDS = 10_860


def publication_window(
    case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, *, conflicts: int = 0,
    change: Callable[[], None] | None = None,
) -> list[tuple[str, ...]]:
    """Inject a real concurrent authority observation after the lifecycle writer closes."""
    observe = answers._Composition.observe_and_apply
    attempts: list[tuple[str, ...]] = []

    async def observed(composition: Any) -> None:
        await observe(composition)
        if composition.row.status != ApprovalStatus.approved:
            return
        attempts.append(tuple(subject.lookup.request_id for subject in composition.subjects))
        if len(attempts) <= conflicts:
            response = await asyncio.to_thread(post_check, case.client, request_for(case))
            assert response.status_code == 200, response.text
            assert response.json()["decision"] == "continue", response.text
        if change is not None and len(attempts) == 1:
            await asyncio.to_thread(change)

    monkeypatch.setattr(answers._Composition, "observe_and_apply", observed)
    return attempts


def reconcile(case: SimpleNamespace) -> int:
    """Run the production reconciler on the app's own loop and undelivered record."""
    async def once() -> int:
        state = case.client.app.state
        reconciler = ResumeReconciler(
            state.sessionmaker, state.resume_queue, interval_seconds=30,
            grace_seconds=GRACE_SECONDS, batch_limit=10,
            identity_runtime=state.http_identity_runtime,
        )
        return await reconciler.reconcile_once()

    assert case.client.portal is not None
    return int(case.client.portal.call(once))


@pytest.mark.parametrize("conflicts", [0, 1, 2])
def test_lifecycle_churn_rechecks_fresh_authority_and_publishes_one_resume_promptly(
    selection_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, conflicts: int,
) -> None:
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    attempts = publication_window(case, monkeypatch, conflicts=conflicts)
    started = time.monotonic()
    response = answer_as(case, row, NATIVE_A)
    elapsed = time.monotonic() - started
    assert response.status_code == 200, response.text
    assert elapsed < 5, elapsed
    assert approval_row(row["id"])["resumed_at"] is not None
    assert len(attempts) == conflicts + 1
    request_ids = [request_id for attempt in attempts for request_id in attempt]
    assert len(set(request_ids)) == 2 * (conflicts + 1)
    assert len(resume_refusals(row["id"])) == conflicts
    entry = published(case, row["id"])
    assert turn_check(case, entry, row["id"]) == ("continue", "current_authority")
    assert reconcile(case) == 0
    assert published(case, row["id"]) == entry


def test_a_subject_revoked_after_a_conflict_still_refuses_on_fresh_authority(
    selection_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))

    def revoke() -> None:
        case.provider.roster[NATIVE_A] = person(APPROVER_A, status="revoked")

    attempts = publication_window(case, monkeypatch, conflicts=1, change=revoke)
    response = answer_as(case, row, NATIVE_A)
    assert response.status_code == 200, response.text
    assert approval_row(row["id"])["status"] == "approved"
    assert approval_row(row["id"])["resumed_at"] is None
    no_wake(case, row["id"])
    assert len(attempts) == 2
    refusals = resume_refusals(row["id"])
    assert len(refusals) == 2
    assert (refusals[-1]["reason"], refusals[-1]["evidence"]["status"]) == (
        "caller_not_allowed", 403,
    )


@pytest.mark.parametrize("cause", ["fingerprint", "route", "runtime"])
def test_a_real_authority_change_is_not_retried_and_keeps_its_refusal_audit(
    selection_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, cause: str,
) -> None:
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    state = case.client.app.state
    runtime = state.http_identity_runtime

    def change() -> None:
        if cause == "fingerprint":
            from approver_ticket_support import set_approver_role, subject_entry

            set_approver_role(case.scope, {"entries": [subject_entry(APPROVER_B)]})
        elif cause == "route":
            configured(case, users=[APPROVER_B])
        else:
            state.http_identity_runtime = runtime_for(case.scope)

    attempts = publication_window(case, monkeypatch, change=change)
    try:
        response = answer_as(case, row, NATIVE_A)
    finally:
        state.http_identity_runtime = runtime
    assert response.status_code == 200, response.text
    assert len(attempts) == 1
    assert approval_row(row["id"])["resumed_at"] is None
    no_wake(case, row["id"])
    refusals = resume_refusals(row["id"])
    assert len(refusals) == 1
    assert (refusals[0]["reason"], refusals[0]["evidence"]["status"]) == (
        "authority_unavailable", 503,
    )
    assert refusals[0]["evidence"]["stage"] == "recheck"


def test_continuous_churn_exhausts_three_attempts_and_leaves_one_grace_based_wake(
    selection_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    attempts = publication_window(case, monkeypatch, conflicts=3)
    response = answer_as(case, row, NATIVE_A)
    assert response.status_code == 200, response.text
    assert len(attempts) == 3
    assert len(resume_refusals(row["id"])) == 3
    assert approval_row(row["id"])["resumed_at"] is None
    no_wake(case, row["id"])
    assert case.client.app.state.resume_queue.undelivered_resumes() == frozenset()
    assert reconcile(case) == 0
    assert len(attempts) == 3
    monkeypatch.setattr(resumereconciler, "datetime", SimpleNamespace(
        now=lambda *_: datetime.now(UTC) + timedelta(seconds=GRACE_SECONDS + 1),
    ))
    assert reconcile(case) == 1
    assert len(attempts) == 4
    entry = published(case, row["id"])
    assert approval_row(row["id"])["resumed_at"] is not None
    assert reconcile(case) == 0
    assert published(case, row["id"]) == entry


@pytest.mark.parametrize("applied", [False, True])
def test_an_uncertain_append_never_retries_inline_and_4016_recovers_before_grace(
    selection_case: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, applied: bool,
) -> None:
    case = selection_case
    configured(case)
    row = dynamic(case, (APPROVER_A,))
    attempts = publication_window(case, monkeypatch)
    state = case.client.app.state
    evaluate = state.valkey.eval
    appends: list[str] = []

    async def uncertain(script: str, *args: Any, **kwargs: Any) -> Any:
        if "identity_resume_v1" in script:
            appends.append(script)
            if len(appends) == 1:
                if applied:
                    await evaluate(script, *args, **kwargs)
                raise redis.ConnectionError("synthetic append outcome unknown")
        return await evaluate(script, *args, **kwargs)

    monkeypatch.setattr(state.valkey, "eval", uncertain)
    response = answer_as(case, row, NATIVE_A)
    assert response.status_code == 200, response.text
    assert len(attempts) == len(appends) == 1
    assert approval_row(row["id"])["resumed_at"] is None
    if applied:
        published(case, row["id"])
    else:
        no_wake(case, row["id"])
    assert state.resume_queue.undelivered_resumes() == frozenset({row["id"]})
    assert reconcile(case) == 1
    assert len(attempts) == len(appends) == 2
    assert state.resume_queue.undelivered_resumes() == frozenset()
    assert approval_row(row["id"])["resumed_at"] is not None
    event_id = f"approval-{row['id']}-resolved"
    entries = [
        (entry_id, fields) for entry_id, fields in case.valkey.xrange(case.stream)
        if QueuedTurn.model_validate_json(fields["payload"]).event_id == event_id
    ]
    assert len(entries) == 1 + int(applied)
    assert one_resume_claim(case, event_id) == [True] + ([False] if applied else [])
    assert reconcile(case) == 0
    assert len(attempts) == len(appends) == 2
    assert [
        (entry_id, fields) for entry_id, fields in case.valkey.xrange(case.stream)
        if QueuedTurn.model_validate_json(fields["payload"]).event_id == event_id
    ] == entries


def one_resume_claim(case: SimpleNamespace, event_id: str) -> list[bool]:
    """Real delivery leases serialize #4016's redundant copy after a lost append reply."""
    async def claim() -> list[bool]:
        client = redis_async.from_url(get_settings().valkey_dsn(), decode_responses=True)
        config = WorkerConfig(key_prefix=f"test:resume-retry:{uuid.uuid4().hex}")
        store = DeliveryLeaseStore(client, config)
        group, consumer = config.key_prefix, "synthetic-consumer"
        leases = []
        granted = []
        await client.xgroup_create(case.stream, group, id="0-0")
        try:
            deliveries = await client.xreadgroup(group, consumer, {case.stream: ">"}, count=10)
            for _, entries in deliveries:
                for entry_id, fields in entries:
                    if QueuedTurn.model_validate_json(fields["payload"]).event_id != event_id:
                        continue
                    lease = await store.acquire(case.stream, group, entry_id, consumer=consumer)
                    leases.append(lease)
                    granted.append(await store.claim_resume(lease, event_id, consumer=consumer))
            return granted
        finally:
            for lease in leases:
                await store.release(
                    case.stream, group, lease.entry_id, owner=lease.owner,
                    resume_event_id=lease.resume_event_id,
                )
                await store.settle(case.stream, group, lease.entry_id)
            await client.xgroup_destroy(case.stream, group)
            await client.aclose()

    assert case.client.portal is not None
    return list(case.client.portal.call(claim))
