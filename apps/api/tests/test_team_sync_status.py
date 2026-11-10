"""Team directory sync state: columns, constraints and the status route (#4584).

ADR 0220 sections 5 and 6. A directory adapter replaces a team's whole
membership as a complete snapshot; the team records which snapshot it holds
(``epoch``, ``member_count``, ``digest``, ``last_synced_at``) and how the last
attempt went (``last_attempt_at``, ``last_error``). Operators read that state
to alert on a stalled adapter. It is reported, never enforced: nothing in this
change consults it for an authorization decision.

The DB-level tests run inside an outer transaction that is always rolled back,
with each expected failure in a SAVEPOINT. The route tests commit, so a local
fixture removes the rows this module created before and after each test.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.models import Team
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
DEFAULT_TENANT_UUID = uuid.UUID(DEFAULT_TENANT_ID)
# Rows this module creates carry this prefix so cleanup removes exactly them.
MARK = "team-sync-test-"
BASE = "/teams"

SYNC_FIELDS = (
    "epoch",
    "member_count",
    "digest",
    "last_synced_at",
    "last_attempt_at",
    "last_error",
    "managed_by",
)


def test_team_model_declares_sync_state() -> None:
    columns = Team.__table__.columns
    for name in SYNC_FIELDS:
        assert name in columns, name
    assert columns["epoch"].nullable is False
    assert columns["member_count"].nullable is False
    for name in ("digest", "last_synced_at", "last_attempt_at", "last_error", "managed_by"):
        assert columns[name].nullable is True, name


# --- DB-level constraints --------------------------------------------------


def _rolled_back(body: Callable[[AsyncConnection], Awaitable[Any]]) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                trans = await conn.begin()
                try:
                    return await body(conn)
                finally:
                    await trans.rollback()
        finally:
            await engine.dispose()

    return asyncio.run(run())


_INSERT = (
    "INSERT INTO curie.teams (id, tenant_id, source, external_id, name, epoch, "
    "member_count, digest, last_synced_at, last_attempt_at, last_error, managed_by) "
    "VALUES (:id, :tenant_id, :source, :external_id, :name, :epoch, :member_count, "
    ":digest, :last_synced_at, :last_attempt_at, :last_error, :managed_by)"
)


def _team(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "id": uuid.uuid4(),
        "tenant_id": DEFAULT_TENANT_UUID,
        "source": "idp_group",
        "external_id": f"00g{uuid.uuid4().hex[:10]}",
        "name": f"{MARK}{uuid.uuid4().hex[:6]}",
        "epoch": 0,
        "member_count": 0,
        "digest": None,
        "last_synced_at": None,
        "last_attempt_at": None,
        "last_error": None,
        "managed_by": None,
    }
    row.update(overrides)
    return row


def _synced(**overrides: Any) -> dict[str, Any]:
    now = datetime.now(UTC)
    base = {
        "epoch": 3,
        "member_count": 2,
        "digest": "9f2c1a",
        "last_synced_at": now,
        "last_attempt_at": now,
        "managed_by": "directory-adapter",
    }
    base.update(overrides)
    return _team(**base)


async def _expect_integrity_error(
    conn: AsyncConnection, params: dict[str, Any], *, constraint: str
) -> None:
    savepoint = await conn.begin_nested()
    try:
        with pytest.raises(IntegrityError) as exc_info:
            await conn.execute(text(_INSERT), params)
    finally:
        if savepoint.is_active:
            await savepoint.rollback()
    cause = exc_info.value.orig.__cause__
    assert getattr(cause, "constraint_name", None) == constraint, str(exc_info.value)


def test_unsynced_and_synced_rows_accepted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> None:
        await conn.execute(text(_INSERT), _team())
        await conn.execute(text(_INSERT), _synced())
        # A failed attempt on a never-synced team: an error, no snapshot.
        await conn.execute(
            text(_INSERT),
            _team(last_attempt_at=datetime.now(UTC), last_error="idp_unreachable"),
        )
        # An empty complete snapshot is a real sync, not an unsynced team.
        await conn.execute(text(_INSERT), _synced(member_count=0, digest="e3b0c4"))

    _rolled_back(body)


@pytest.mark.parametrize(
    ("row", "constraint"),
    [
        (lambda: _team(epoch=-1), "teams_epoch_ck"),
        (lambda: _synced(member_count=-1), "teams_member_count_ck"),
        # Synced means a snapshot was applied: epoch, digest and time together.
        (lambda: _synced(last_synced_at=None), "teams_sync_state_ck"),
        (lambda: _synced(digest=None), "teams_sync_state_ck"),
        (lambda: _team(digest="9f2c1a"), "teams_sync_state_ck"),
        (lambda: _team(last_synced_at=datetime.now(UTC)), "teams_sync_state_ck"),
        (lambda: _team(member_count=4), "teams_sync_state_ck"),
        # Only an IdP group is managed by a directory adapter.
        (
            lambda: _team(source="curie_managed", external_id=None, managed_by="adapter"),
            "teams_managed_by_ck",
        ),
        (lambda: _team(managed_by=""), "teams_managed_by_ck"),
        (lambda: _team(last_error=""), "teams_last_error_ck"),
        (lambda: _team(last_error="x" * 201), "teams_last_error_ck"),
    ],
)
def test_inconsistent_sync_state_rejected(
    migrated: None, row: Callable[[], dict[str, Any]], constraint: str
) -> None:
    async def body(conn: AsyncConnection) -> None:
        await _expect_integrity_error(conn, row(), constraint=constraint)

    _rolled_back(body)


# --- routes ----------------------------------------------------------------


def _sql(statement: str, params: dict[str, Any] | None = None) -> None:
    async def run() -> None:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(text(statement), params or {})
        finally:
            await engine.dispose()

    asyncio.run(run())


def _cleanup() -> None:
    _sql("DELETE FROM curie.teams WHERE name LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.tenants WHERE deployment_id LIKE :mark", {"mark": f"{MARK}%"})


@pytest.fixture
def api(migrated: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    _cleanup()
    monkeypatch.setenv("SLACK_BOT_TOKEN", "")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        get_settings.cache_clear()
        _cleanup()


def _insert(row: dict[str, Any]) -> str:
    _sql(_INSERT, row)
    return str(row["id"])


def _other_tenant() -> uuid.UUID:
    tenant_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"{MARK}{tenant_id}"},
    )
    return tenant_id


def _ours(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [row for row in rows if row["name"].startswith(MARK)]


def test_route_requires_platform_key(api: TestClient) -> None:
    assert api.get(BASE).status_code == 401
    assert api.get(BASE, headers={"X-API-Key": "wrong"}).status_code == 401


def test_lists_unsynced_synced_and_failing_teams(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    synced_at = datetime.now(UTC) - timedelta(minutes=5)
    unsynced = _insert(_team(name=f"{MARK}a-unsynced"))
    synced = _insert(
        _synced(
            name=f"{MARK}b-synced",
            last_synced_at=synced_at,
            last_attempt_at=synced_at,
        )
    )
    failing = _insert(
        _synced(
            name=f"{MARK}c-failing",
            last_synced_at=synced_at,
            last_attempt_at=datetime.now(UTC),
            last_error="idp_unreachable",
        )
    )

    response = api.get(BASE, headers=auth_headers)
    assert response.status_code == 200, response.text
    rows = _ours(response.json())
    assert [row["id"] for row in rows] == [unsynced, synced, failing]

    first, second, third = rows
    assert first["synced"] is False
    assert first["epoch"] == 0
    assert first["sync_age_seconds"] is None
    assert first["last_synced_at"] is None

    assert second["synced"] is True
    assert second["epoch"] == 3
    assert second["member_count"] == 2
    assert second["digest"] == "9f2c1a"
    assert second["managed_by"] == "directory-adapter"
    assert second["last_error"] is None
    assert 290 <= second["sync_age_seconds"] <= 600

    # A failing adapter keeps the last snapshot: still synced, error reported.
    assert third["synced"] is True
    assert third["last_error"] == "idp_unreachable"
    assert third["sync_age_seconds"] >= 290


def test_status_carries_no_member_list(api: TestClient, auth_headers: dict[str, str]) -> None:
    _insert(_synced(name=f"{MARK}members"))
    (row,) = _ours(api.get(BASE, headers=auth_headers).json())
    assert set(row) == {
        "id",
        "tenant_id",
        "source",
        "external_id",
        "name",
        "synced",
        "sync_age_seconds",
        *SYNC_FIELDS,
    }


def test_tenant_filter(api: TestClient, auth_headers: dict[str, str]) -> None:
    other = _other_tenant()
    mine = _insert(_team(name=f"{MARK}default"))
    theirs = _insert(_team(name=f"{MARK}other", tenant_id=other))

    default_rows = _ours(
        api.get(BASE, params={"tenant_id": DEFAULT_TENANT_ID}, headers=auth_headers).json()
    )
    assert [row["id"] for row in default_rows] == [mine]

    other_rows = _ours(api.get(BASE, params={"tenant_id": str(other)}, headers=auth_headers).json())
    assert [row["id"] for row in other_rows] == [theirs]
    assert other_rows[0]["tenant_id"] == str(other)
