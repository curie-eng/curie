"""A committed selected answer publishes its resume through the core-owned path.

The actual resolve and recover routes, the production approved-resume authority,
ResumeQueue, PostgreSQL, Valkey and the synthetic TLS identity provider;
synthetic inputs only. @spec HTTP-APPROVAL-CONTINUITY-8/10/11.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.db import SCHEMA
from role_policy_support import run
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests.test_http_approval_answers import (
    SENDER,
    answer,
    sealed_actor,
)
from apps.api.tests.test_http_approval_answers import answer_case as answer_case
from apps.api.tests.test_http_approval_creation import creation_case as creation_case
from apps.api.tests.test_http_approval_legacy_writers import RECOVERY_ENV
from apps.api.tests.test_http_approval_resume import (
    CONTEXT_FIELD,
    resume_context,
)
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider


@pytest.fixture
def published_case(answer_case: SimpleNamespace) -> SimpleNamespace:
    """@spec HTTP-APPROVAL-CONTINUITY-11: a real selected row and a migrated approver role."""
    return answer_case


def resumes(case: SimpleNamespace, valkey: redis.Redis, stream: str) -> list[dict[str, Any]]:
    """@spec HTTP-APPROVAL-CONTINUITY-11: entries appended since the fixture began."""
    return [fields for _, fields in valkey.xrange(stream)[len(case.stream_before):]]


def owe(approval_id: Any) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-11: reopen the owed wake, as a dead-letter reopen does."""

    async def clear(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-11."""
        async with factory() as session, session.begin():
            await session.execute(
                text(f'UPDATE "{SCHEMA}".approvals SET resumed_at = NULL WHERE id = :id'),
                {"id": approval_id},
            )

    run(clear)


def test_a_recovered_answer_publishes_its_rejected_notification(
    published_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-10/11: recovery converges on the same publisher."""
    case = published_case
    body, headers = answer(case, operation="recover")
    headers["X-API-Key"] = get_settings().api_key
    os.environ[RECOVERY_ENV] = "true"
    get_settings.cache_clear()
    try:
        response = case.client.post(
            f"/approvals/{case.row['id']}/recover", content=body, headers=headers,
        )
    finally:
        os.environ.pop(RECOVERY_ENV, None)
        get_settings.cache_clear()
    assert response.status_code == 200, response.text
    assert response.json()["actor"] == SENDER
    row, _ = sealed_actor(case, "rejected")
    assert row["resumed_at"] is not None
    entries = resumes(case, valkey, runs_stream)
    assert len(entries) == 1
    assert resume_context(case, entries[0][CONTEXT_FIELD]).status == "rejected"


