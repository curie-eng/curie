"""Directory health describes projections without expiring authorization."""

import asyncio
from datetime import UTC, datetime, timedelta

from curie_api import identity_audit
from curie_api.config import get_settings
from curie_api.routers.teams import directory_metrics
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from .test_team_authorization import seed


def test_metrics_report_age_and_failure_count_without_changing_membership(clean_db):
    async def check():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with async_sessionmaker(engine)() as session:
                person, team, _, _, _, _ = await seed(session)
                team.last_synced_at = datetime.now(UTC) - timedelta(hours=2)
                identity_audit.record(
                    session,
                    tenant_id=person.tenant_id,
                    event_type="identity.directory_sync_failed",
                    actor="example-directory",
                    team_id=team.id,
                )
                await session.flush()
                response = await directory_metrics(session, tenant_id=person.tenant_id)
                body = bytes(response.body).decode()
                assert (
                    f'curie_directory_sync_failed_attempts_total{{team_id="{team.id}"}} 1' in body
                )
                age = next(
                    float(line.rsplit(" ", 1)[1])
                    for line in body.splitlines()
                    if line.startswith(f'curie_directory_sync_age_seconds{{team_id="{team.id}"}}')
                )
                assert 7200 <= age < 7210
                assert team.epoch == 1 and team.member_count == 1
        finally:
            await engine.dispose()

    asyncio.run(check())
