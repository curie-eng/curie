"""Prompt approved-resume retries over the API, Postgres, Valkey and TLS directory."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from approval_answer_support import selection_case as selection_case
from approval_answer_support import synthetic_provider as synthetic_provider
from curie_api.identity import http_approval_answer as answers
from curie_api.models import ApprovalStatus
from curie_api.resumereconciler import ResumeReconciler

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


REVOCATION_WINDOWS = [
    (moment, status, role)
    for moment in ("after_apply", "backoff", "reconciler")
    for status in ("disabled", "revoked")
    for role in ("invocation", "approver")
] + [("none", "active", "approver")]
