"""Schedule claims use real Postgres and never replay missed intervals."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from _migration_support import sql_dicts, sql_rows
from curie_api.config import get_settings
from curie_api.crud.approvals import claim_due_reminders
from curie_api.sweeper import sweep_due_reminders
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def seed(*, expired: bool = False, status: str = "pending") -> uuid.UUID:
    approval_id = uuid.uuid4()
    sql_rows(
        "INSERT INTO curie.approvals (id, conversation_id, author, summary, reply_kind, "
        "reply_channel, dedupe_key, status, remind_every_s, next_remind_at, expires_at) "
        "VALUES (:id, 'schedule-test', 'U0EXAMPLE1', 'Approve work', 'slack', 'C0EXAMPLE1', "
        ":dedupe, :status, 60, now() - interval '2 days', "
        "CASE WHEN :expired THEN now() - interval '1 second' ELSE NULL END)",
        {"id": approval_id, "dedupe": str(approval_id), "status": status, "expired": expired},
    )
    return approval_id


def test_competing_sweepers_claim_each_due_value_once_and_restart_from_now(clean_db) -> None:
    approval_id = seed()
    before = datetime.now(UTC).replace(tzinfo=None)

    async def run():
        engine = create_async_engine(get_settings().database_url)
        try:
            sessions = async_sessionmaker(engine)
            async with sessions() as first, sessions() as second:
                claims = await asyncio.gather(
                    claim_due_reminders(first), claim_due_reminders(second)
                )
            events = [event for batch in claims for event in batch]
            assert len(events) == 1
            assert events[0].approval_id == str(approval_id)
            async with sessions() as restarted:
                assert await claim_due_reminders(restarted) == []
        finally:
            await engine.dispose()

    asyncio.run(run())
    row = sql_dicts("SELECT next_remind_at FROM curie.approvals WHERE id=:id", {"id": approval_id})[
        0
    ]
    assert before + timedelta(seconds=60) <= row["next_remind_at"]
    assert row["next_remind_at"] <= datetime.now(UTC).replace(tzinfo=None) + timedelta(seconds=60)


@pytest.mark.parametrize(
    "status,expired",
    [("approved", False), ("rejected", False), ("expired", False), ("pending", True)],
)
def test_terminal_and_expired_approvals_are_not_claimed(clean_db, status, expired) -> None:
    seed(expired=expired, status=status)

    async def run():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with async_sessionmaker(engine)() as session:
                assert await claim_due_reminders(session) == []
        finally:
            await engine.dispose()

    asyncio.run(run())


def test_failed_publish_loses_one_interval_then_a_later_due_interval_runs(clean_db) -> None:
    approval_id = seed()
    sent = []

    async def unavailable(event):
        raise ConnectionError("publication unavailable")

    async def record(event):
        sent.append(event)

    async def run(publish):
        engine = create_async_engine(get_settings().database_url)
        try:
            async with async_sessionmaker(engine)() as session:
                return await sweep_due_reminders(session, publish)
        finally:
            await engine.dispose()

    assert asyncio.run(run(unavailable)) == 0
    assert asyncio.run(run(record)) == 0
    sql_rows(
        "UPDATE curie.approvals SET next_remind_at=now()-interval '1 second' WHERE id=:id",
        {"id": approval_id},
    )
    assert asyncio.run(run(record)) == 1
    assert len(sent) == 1
    assert sent[0].approval_id == str(approval_id)
