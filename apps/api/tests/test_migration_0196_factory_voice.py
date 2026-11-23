"""Voice ingress on an older serving schema and lossless downgrade of an empty voice table."""

from __future__ import annotations

import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, sql_rows, stamped_revision
from alembic import command
from alembic.script import ScriptDirectory
from curie_api.factory_voice import expire_voice_drafts
from curie_api.factory_voice_contract import Confirmed, Pending, Refused
from factory_run_support import _run
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from test_factory_voice import draft, media, setup_voice

REVISION = "0196"


def parent_revision() -> str:
    revision = ScriptDirectory.from_config(alembic_config()).get_revision(REVISION)
    assert revision is not None and isinstance(revision.down_revision, str)
    return revision.down_revision


def test_older_schema_serves_without_processing_voice(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    below = parent_revision()
    isolated_migration_db.at(below)

    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, recognizer = await setup_voice(sessions)
        assert await service.submit(provider.source, media()) == Refused(reason="unavailable")
        assert not await service.process_pending("github", provider)
        assert await provider.decide(service, "0123456789ab.1", "confirm") == Refused(
            reason="unavailable"
        )
        async with sessions.begin() as session:
            assert await expire_voice_drafts(session) == 0
        assert (
            not provider.admissions
            and not provider.outcomes
            and provider.reads == recognizer.calls == 0
        )

    _run(go)
    config = alembic_config()
    command.upgrade(config, REVISION)
    assert stamped_revision() == REVISION
    assert sql_rows("SELECT count(*) FROM curie.factory_voice_drafts") == [(0,)]
    command.downgrade(config, below)
    assert stamped_revision() == below
    assert sql_rows("SELECT to_regclass('curie.factory_voice_drafts') IS NULL") == [(True,)]


@pytest.mark.parametrize("confirmed", [False, True])
def test_downgrade_preserves_pending_work_and_confirmed_receipts(
    isolated_migration_db: IsolatedMigrationDb, confirmed: bool
) -> None:
    isolated_migration_db.at(REVISION)

    async def go(sessions: async_sessionmaker[AsyncSession]) -> None:
        service, provider, _ = await setup_voice(sessions)
        if confirmed:
            first = await draft(service, provider)
            assert isinstance(await provider.decide(service, first.code, "confirm"), Confirmed)
        else:
            assert isinstance(await service.submit(provider.source, media()), Pending)

    _run(go)
    before = sql_rows("SELECT id, source_key, status, admission_id FROM curie.factory_voice_drafts")
    with pytest.raises(DBAPIError, match="factory_voice_drafts_present"):
        command.downgrade(alembic_config(), parent_revision())
    assert stamped_revision() == REVISION
    assert (
        sql_rows("SELECT id, source_key, status, admission_id FROM curie.factory_voice_drafts")
        == before
    )
