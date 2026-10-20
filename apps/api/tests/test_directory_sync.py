"""Behavior of directory projection writes against real disposable Postgres.

Changing replacement to append, dropping an epoch/scope check, or allowing
sync errors to clear the projection must fail these tests.
"""

import asyncio
import uuid
from collections.abc import Awaitable, Callable

import pytest
from curie_api import directory_sync as sync
from curie_api.config import get_settings
from curie_api.models import Principal, PrincipalTeam, Team, Tenant
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

ISSUER = "https://idp.example.com"


async def seed(session: AsyncSession) -> tuple[sync.DirectoryScope, Team, list[Principal]]:
    tenant = Tenant(
        id=uuid.uuid4(),
        deployment_id=f"directory-test-{uuid.uuid4()}",
        status="active",
    )
    session.add(tenant)
    await session.flush()
    team = Team(
        tenant_id=tenant.id,
        source="idp_group",
        external_id="group-example",
        name="Example group",
        managed_by="example-adapter",
    )
    people = [
        Principal(tenant_id=tenant.id, idp_issuer=ISSUER, idp_subject=f"person-{i}", type="human")
        for i in range(3)
    ]
    session.add_all([team, *people])
    await session.flush()
    scope = sync.DirectoryScope(
        tenant_id=tenant.id,
        issuer=ISSUER,
        managed_by="example-adapter",
        team_ids=frozenset({team.id}),
    )
    return scope, team, people


def run(body: Callable[[AsyncSession], Awaitable[None]]) -> None:
    async def check() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                transaction = await conn.begin()
                try:
                    async with AsyncSession(bind=conn, expire_on_commit=False) as session:
                        await body(session)
                finally:
                    await transaction.rollback()
        finally:
            await engine.dispose()

    asyncio.run(check())


async def members(session: AsyncSession, team: Team) -> set[uuid.UUID]:
    return set(
        await session.scalars(
            select(PrincipalTeam.principal_id).where(PrincipalTeam.team_id == team.id)
        )
    )


def test_complete_snapshot_replaces_members_and_records_state(migrated: None) -> None:
    async def body(s: AsyncSession) -> None:
        scope, team, p = await seed(s)
        first = await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=0,
            principal_ids=[p[0].id, p[1].id],
            complete=True,
        )
        assert first.epoch == 1 and first.member_count == 2
        assert await members(s, team) == {p[0].id, p[1].id}
        second = await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=1,
            principal_ids=[p[2].id],
            complete=True,
        )
        assert await members(s, team) == {p[2].id}
        assert second.epoch == 2 and second.member_count == 1
        assert second.digest != first.digest
        assert team.last_synced_at is not None and team.last_error is None
        rows = list(await s.scalars(select(PrincipalTeam).where(PrincipalTeam.team_id == team.id)))
        assert rows[0].source == "idp_group" and rows[0].version == 2

    run(body)


def test_empty_complete_snapshot_removes_every_member(migrated: None) -> None:
    async def body(s: AsyncSession) -> None:
        scope, team, p = await seed(s)
        await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=0,
            principal_ids=[p[0].id],
            complete=True,
        )
        result = await sync.apply_snapshot(
            s, scope=scope, team_id=team.id, expected_epoch=1, principal_ids=[], complete=True
        )
        assert result.epoch == 2 and result.member_count == 0
        assert await members(s, team) == set()

    run(body)


@pytest.mark.parametrize(
    "bad",
    ["partial", "duplicate", "stale", "foreign", "issuer", "manager", "scope"],
)
def test_rejected_snapshot_preserves_last_projection(migrated: None, bad: str) -> None:
    async def body(s: AsyncSession) -> None:
        scope, team, p = await seed(s)
        old = await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=0,
            principal_ids=[p[0].id],
            complete=True,
        )
        ids = [p[1].id]
        epoch, complete = 1, True
        if bad == "partial":
            complete = False
        if bad == "duplicate":
            ids = [p[1].id, p[1].id]
        if bad == "stale":
            epoch = 0
        if bad == "foreign":
            other, _, q = await seed(s)
            assert other.tenant_id != scope.tenant_id
            ids = [q[0].id]
        if bad == "issuer":
            p[1].idp_issuer = "https://other.example.com"
            await s.flush()
        if bad == "manager":
            team.managed_by = "other-adapter"
            await s.flush()
        if bad == "scope":
            scope = sync.DirectoryScope(
                tenant_id=scope.tenant_id,
                issuer=ISSUER,
                managed_by=scope.managed_by,
                team_ids=frozenset({uuid.uuid4()}),
            )
        with pytest.raises(sync.DirectorySyncError):
            await sync.apply_snapshot(
                s,
                scope=scope,
                team_id=team.id,
                expected_epoch=epoch,
                principal_ids=ids,
                complete=complete,
            )
        assert await members(s, team) == {p[0].id}
        await s.refresh(team)
        assert (team.epoch, team.member_count, team.digest) == (old.epoch, 1, old.digest)

    run(body)


def test_failed_attempt_preserves_membership_and_success_time(migrated: None) -> None:
    async def body(s: AsyncSession) -> None:
        scope, team, p = await seed(s)
        await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=0,
            principal_ids=[p[0].id],
            complete=True,
        )
        succeeded = team.last_synced_at
        await sync.record_failure(
            s, scope=scope, team_id=team.id, expected_epoch=1, error="provider_unavailable"
        )
        assert team.last_error == "provider_unavailable"
        assert team.last_synced_at == succeeded and team.epoch == 1
        assert await members(s, team) == {p[0].id}
        with pytest.raises(sync.DirectorySyncError):
            await sync.record_failure(
                s, scope=scope, team_id=team.id, expected_epoch=0, error="provider_unavailable"
            )
        with pytest.raises(sync.DirectorySyncError):
            await sync.record_failure(
                s,
                scope=scope,
                team_id=team.id,
                expected_epoch=1,
                error="raw provider response or credential",
            )

    run(body)


def test_status_version_prevents_stale_reactivation(migrated: None) -> None:
    async def body(s: AsyncSession) -> None:
        scope, team, p = await seed(s)
        await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=0,
            principal_ids=[p[0].id],
            complete=True,
        )
        version = await sync.set_account_status(
            s, scope=scope, principal_id=p[0].id, expected_version=1, status="disabled"
        )
        assert version == 2 and p[0].status == "disabled"
        with pytest.raises(sync.DirectorySyncError):
            await sync.set_account_status(
                s, scope=scope, principal_id=p[0].id, expected_version=1, status="active"
            )
        await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=1,
            principal_ids=[p[0].id],
            complete=True,
        )
        await s.refresh(p[0])
        assert p[0].status == "disabled" and p[0].authorization_version == 2
        assert await members(s, team) == {p[0].id}
        version = await sync.set_account_status(
            s, scope=scope, principal_id=p[0].id, expected_version=2, status="active"
        )
        assert version == 3

    run(body)


def test_digest_is_independent_of_member_order(migrated: None) -> None:
    async def body(s: AsyncSession) -> None:
        scope, team, p = await seed(s)
        a = await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=0,
            principal_ids=[p[0].id, p[1].id],
            complete=True,
        )
        b = await sync.apply_snapshot(
            s,
            scope=scope,
            team_id=team.id,
            expected_epoch=1,
            principal_ids=[p[1].id, p[0].id],
            complete=True,
        )
        assert a.digest == b.digest

    run(body)


@pytest.mark.parametrize("change", ["tenant", "issuer", "service"])
def test_status_write_refuses_foreign_or_nonhuman_principals(
    migrated: None,
    change: str,
) -> None:
    async def body(s: AsyncSession) -> None:
        scope, _, people = await seed(s)
        person = people[0]
        if change == "tenant":
            _, _, foreign = await seed(s)
            person = foreign[0]
        elif change == "issuer":
            person.idp_issuer = "https://other.example.com"
        else:
            person.type = "service"
        await s.flush()
        with pytest.raises(sync.DirectorySyncError, match="principal_out_of_scope"):
            await sync.set_account_status(
                s,
                scope=scope,
                principal_id=person.id,
                expected_version=1,
                status="disabled",
            )
        await s.refresh(person)
        assert person.status == "active" and person.authorization_version == 1

    run(body)


def test_two_snapshot_writers_cannot_restore_a_removed_member(migrated: None) -> None:
    async def body() -> None:
        engine = create_async_engine(get_settings().database_url)
        tenant_id = None
        try:
            async with AsyncSession(engine, expire_on_commit=False) as session:
                scope, team, people = await seed(session)
                tenant_id = scope.tenant_id
                team_id = team.id
                ids = [p.id for p in people]
                await session.commit()

            async def writer(person: uuid.UUID) -> sync.SnapshotResult | str:
                async with AsyncSession(engine) as session:
                    try:
                        result = await sync.apply_snapshot(
                            session,
                            scope=scope,
                            team_id=team_id,
                            expected_epoch=0,
                            principal_ids=[person],
                            complete=True,
                        )
                        await session.commit()
                        return result
                    except sync.DirectoryConflict:
                        await session.rollback()
                        return "conflict"

            results = await asyncio.gather(writer(ids[0]), writer(ids[1]))
            assert results.count("conflict") == 1
            async with AsyncSession(engine) as session:
                current = await session.get(Team, team_id)
                assert current is not None and current.epoch == 1
                assert len(await members(session, current)) == 1
        finally:
            if tenant_id is not None:
                from sqlalchemy import delete

                async with engine.begin() as conn:
                    for model in (PrincipalTeam, Team, Principal, Tenant):
                        key = model.id if model is Tenant else model.tenant_id
                        await conn.execute(delete(model).where(key == tenant_id))
            await engine.dispose()

    asyncio.run(body())
