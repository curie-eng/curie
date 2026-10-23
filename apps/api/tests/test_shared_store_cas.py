"""Shared connector store CAS, isolation and lock contracts (ADR 0239, AC8).

ADR 0239 "Custody and least privilege" 3 requires that each shared application
reads the store through its own non-superuser role: rows are derived from the
authenticated role, the role may lock its own application and binding rows
through the non-authority ``lock_guard`` column, and it can neither read nor
change another application's rows. "Static sources" 1, 4, 5 and 6 require CAS
reimport with ``source_stale`` checks, durable refresh intents fenced by intent
and epoch, explicit recovery, and a disable that waits for open resolver locks
and acknowledges only after commit. "Custody and least privilege" 6 requires
CAS reseal before a recipient retires.

Every database assertion here runs on a real asyncpg connection made with the
DSN the API sealed for the application, opened with the test's private key.
Concurrency is proven with separate connections and explicit ordering: a test
holds a lock, shows the competing statement or request is parked on it, then
releases it.
"""

from __future__ import annotations

import json
import secrets
import threading
import time
import uuid
from collections.abc import Iterator
from typing import Any

import asyncpg
import nacl.exceptions
import pytest
from _migration_support import render_url, sql_dicts
from _shared_connector_support import (
    APPLICATION_TABLES,
    BEGIN_SQL,
    COMMIT_SQL,
    MARK_RECOVERY_SQL,
    RESEAL_SQL,
    SHARED_FUNCTIONS,
    SHARED_TABLES,
    SLOT,
    STATEMENT_WAIT_S,
    PgSession,
    Recipient,
    SessionPool,
    SharedCleanup,
    SharedStore,
    World,
    application_row,
    assert_refused,
    assert_still_blocked,
    b64,
    binding_row,
    build_world,
    client_fingerprint,
    connect_as,
    expect_sqlstate,
    in_background,
    intent_row,
    quote_ident,
    random_installation,
    resolver_read_sql,
    secret_sentinel,
    shared_app,
    source_metadata,
    wait_for_lock_waiters,
)
from curie_api.config import get_settings
from sqlalchemy.engine import make_url


@pytest.fixture
def cleanup(migrated: None) -> Iterator[SharedCleanup]:
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def sessions(cleanup: SharedCleanup) -> Iterator[SessionPool]:
    pool = SessionPool()
    yield pool
    pool.close_all()


@pytest.fixture
def store(cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch) -> Iterator[SharedStore]:
    installation = cleanup.installation(random_installation())
    with shared_app(monkeypatch, installation) as client:
        yield SharedStore(client, installation, cleanup)
    monkeypatch.undo()
    get_settings.cache_clear()


@pytest.fixture
def world(store: SharedStore) -> World:
    return build_world(store)


def _credential(label: str = "ROTATED") -> bytes:
    return json.dumps({SLOT: secret_sentinel(label)}).encode()


def _began(
    session: PgSession, grant_id: str | None, owner: str = "resolver-a", lease: int = 60
) -> asyncpg.Record:
    row = session.fetchrow(BEGIN_SQL, grant_id, owner, lease)
    assert row is not None and row["outcome"] == "began", row
    return row


def _outcome(session: PgSession, query: str, *args: Any) -> str:
    row = session.fetchrow(query, *args)
    assert row is not None
    return str(row["outcome"])


def _ready(binding_id: str, epoch: int) -> None:
    intent = intent_row(binding_id)
    assert intent["state"] == "ready", intent
    assert intent["epoch"] == epoch, intent
    for column in ("intent_id", "owner", "lease_expires_at", "recovery_reason"):
        assert intent[column] is None, (column, intent)


def _own_ids(session: PgSession, table: str) -> set[str]:
    column = "id" if table == "shared_connector_applications" else "application_id"
    return {str(row[0]) for row in session.fetch(f"SELECT {column} FROM curie.{table}")}


# --- 1. reimport CAS ----------------------------------------------------------


def test_reimport_advances_the_revision_once_and_a_stale_writer_changes_nothing(
    world: World,
) -> None:
    replacement = _credential("REIMPORTED")
    advanced = world.reimport(1, replacement, resource_version="101")
    assert advanced.status_code == 200, advanced.text
    record = advanced.json()
    assert record["binding_id"] == world.binding_id
    assert record["credential_revision"] == 2
    assert record["source"] == world.source
    assert "ciphertext" not in record
    after, intent_after = binding_row(world.binding_id), intent_row(world.binding_id)
    assert world.recipient.open(after["ciphertext"]) == replacement

    stale = world.store.reimport(
        world.binding_id,
        1,
        world.recipient,
        _credential("STALE"),
        source_metadata(uid=world.source["uid"], resource_version="102"),
    )
    assert_refused(stale, 409, "shared_binding_revision_conflict")
    assert binding_row(world.binding_id) == after
    assert intent_row(world.binding_id) == intent_after
    assert world.store.binding(world.binding_id)["credential_revision"] == 2


def test_concurrent_reimports_with_one_expected_revision_have_exactly_one_winner(
    world: World, sessions: SessionPool
) -> None:
    holder = sessions.owner()
    holder.execute("BEGIN")
    holder.fetch(
        "SELECT id FROM curie.shared_connector_bindings WHERE id = $1 FOR UPDATE",
        world.binding_uuid,
    )
    writes = [_credential(f"WRITER{index}") for index in range(2)]
    source = source_metadata(uid=world.source["uid"], resource_version="200")
    pending = [
        in_background(
            lambda plaintext=plaintext: world.store.reimport(
                world.binding_id, 1, world.recipient, plaintext, source
            )
        )
        for plaintext in writes
    ]
    wait_for_lock_waiters(2)
    assert not any(future.done() for future in pending)
    holder.execute("COMMIT")

    responses = [future.result(timeout=STATEMENT_WAIT_S) for future in pending]
    assert sorted(response.status_code for response in responses) == [200, 409], [
        response.text for response in responses
    ]
    winner = next(index for index, response in enumerate(responses) if response.status_code == 200)
    assert_refused(responses[1 - winner], 409, "shared_binding_revision_conflict")
    row = binding_row(world.binding_id)
    assert row["credential_revision"] == 2
    assert world.recipient.open(row["ciphertext"]) == writes[winner]


# --- 2. role isolation -------------------------------------------------------


def test_an_application_role_sees_only_its_own_rows_in_every_shared_table(
    store: SharedStore, sessions: SessionPool
) -> None:
    mine, theirs = build_world(store), build_world(store)
    role = sessions.open(mine.dsn)
    assert role.fetchval("SELECT current_user") == mine.role_name
    both = [uuid.UUID(mine.application_id), uuid.UUID(theirs.application_id)]
    # The caller verify key tables are scoped to the installation, not the
    # application (0125); test_shared_connector_caller_keys.py covers them.
    for table in APPLICATION_TABLES:
        column = "id" if table == "shared_connector_applications" else "application_id"
        stored = {
            str(row[column])
            for row in sql_dicts(
                f"SELECT {column} FROM curie.{table} WHERE {column} = ANY(:ids)", {"ids": both}
            )
        }
        assert stored == {mine.application_id, theirs.application_id}, table
        assert _own_ids(role, table) == {mine.application_id}, table
    assert (
        role.fetch(
            "SELECT id FROM curie.shared_connector_bindings WHERE id = $1", theirs.binding_uuid
        )
        == []
    )
    assert {
        row["role_name"]
        for row in role.fetch("SELECT role_name FROM curie.shared_connector_database_roles")
    } == {mine.role_name}


def test_an_application_role_cannot_lock_update_or_refresh_another_applications_rows(
    store: SharedStore, sessions: SessionPool
) -> None:
    mine, theirs = build_world(store), build_world(store)
    role = sessions.open(mine.dsn)
    their_application = uuid.UUID(theirs.application_id)
    for lock in ("SHARE", "KEY SHARE"):
        assert (
            role.fetch(
                f"SELECT id FROM curie.shared_connector_bindings WHERE id = $1 FOR {lock}",
                theirs.binding_uuid,
            )
            == []
        )
        assert (
            role.fetch(
                f"SELECT id FROM curie.shared_connector_applications WHERE id = $1 FOR {lock}",
                their_application,
            )
            == []
        )
        assert role.fetch(resolver_read_sql(lock), theirs.binding_uuid) == []
    assert (
        role.execute(
            "UPDATE curie.shared_connector_bindings SET lock_guard = lock_guard WHERE id = $1",
            theirs.binding_uuid,
        )
        == "UPDATE 0"
    )
    assert (
        role.execute(
            "UPDATE curie.shared_connector_applications SET lock_guard = lock_guard WHERE id = $1",
            their_application,
        )
        == "UPDATE 0"
    )

    sealed = mine.recipient.seal(_credential())
    assert _outcome(role, BEGIN_SQL, theirs.grant_id, "intruder", 30) == "not_found"
    assert (
        _outcome(
            role, COMMIT_SQL, theirs.grant_id, uuid.uuid4(), 1, mine.recipient.recipient_id, sealed
        )
        == "not_found"
    )
    assert role.fetchval(MARK_RECOVERY_SQL, theirs.grant_id, uuid.uuid4(), 1) == "not_found"
    assert (
        role.fetchval(
            RESEAL_SQL,
            theirs.binding_uuid,
            1,
            theirs.recipient.recipient_id,
            mine.recipient.recipient_id,
            sealed,
        )
        == "not_found"
    )
    _ready(theirs.binding_id, 0)
    assert binding_row(theirs.binding_id)["credential_revision"] == 1

    assert (
        role.execute(
            "UPDATE curie.shared_connector_bindings SET lock_guard = lock_guard WHERE id = $1",
            mine.binding_uuid,
        )
        == "UPDATE 1"
    )
    _began(sessions.open(theirs.dsn), theirs.grant_id, "resolver-b")


def test_an_application_role_has_no_write_authority_and_cannot_borrow_another_identity(
    store: SharedStore, sessions: SessionPool
) -> None:
    mine, theirs = build_world(store), build_world(store)
    role = sessions.open(mine.dsn)
    for table in SHARED_TABLES:
        for statement in (
            f"INSERT INTO curie.{table} DEFAULT VALUES",
            f"DELETE FROM curie.{table}",
            f"TRUNCATE curie.{table}",
        ):
            expect_sqlstate(role, "42501", statement)
    expect_sqlstate(role, "42501", "SELECT id FROM curie.agents LIMIT 1")

    for statement in (
        f"SET curie.shared_application = '{theirs.application_id}'",
        f"SET curie.application_id = '{theirs.application_id}'",
        f"SELECT set_config('curie.shared_role', '{theirs.role_name}', false)",
    ):
        role.execute(statement)
        for table in APPLICATION_TABLES:
            assert _own_ids(role, table) == {mine.application_id}, (statement, table)
    assert _outcome(role, BEGIN_SQL, theirs.grant_id, "intruder", 30) == "not_found"
    expect_sqlstate(role, "42501", f"SET ROLE {quote_ident(theirs.role_name)}")
    expect_sqlstate(role, "42501", f"SET SESSION AUTHORIZATION {quote_ident(theirs.role_name)}")
    assert role.fetchval("SELECT current_user") == mine.role_name
    assert role.fetchval("SELECT session_user") == mine.role_name
    _ready(theirs.binding_id, 0)


# --- 3. lock_guard -----------------------------------------------------------


def test_lock_guard_lets_the_role_lock_its_own_application_and_binding_rows(
    world: World, sessions: SessionPool
) -> None:
    role = sessions.open(world.dsn)
    application = uuid.UUID(world.application_id)
    generation = world.store.application(world.application_id)["policy_generation"]
    for lock in ("SHARE", "KEY SHARE"):
        role.execute("BEGIN")
        assert [
            str(row["id"])
            for row in role.fetch(
                f"SELECT id FROM curie.shared_connector_applications WHERE id = $1 FOR {lock}",
                application,
            )
        ] == [world.application_id]
        assert [
            str(row["id"])
            for row in role.fetch(
                f"SELECT id FROM curie.shared_connector_bindings WHERE id = $1 FOR {lock}",
                world.binding_uuid,
            )
        ] == [world.binding_id]
        row = role.fetchrow(resolver_read_sql(lock), world.binding_uuid)
        assert row is not None
        assert dict(row) == {
            "application_enabled": True,
            "policy_generation": generation,
            "binding_enabled": True,
            "credential_revision": 1,
        }
        role.execute("COMMIT")
    assert (
        role.execute(
            "UPDATE curie.shared_connector_applications SET lock_guard = lock_guard WHERE id = $1",
            application,
        )
        == "UPDATE 1"
    )
    assert (
        role.execute(
            "UPDATE curie.shared_connector_bindings SET lock_guard = lock_guard WHERE id = $1",
            world.binding_uuid,
        )
        == "UPDATE 1"
    )


def test_the_application_role_cannot_update_any_authority_column(
    world: World, sessions: SessionPool
) -> None:
    role = sessions.open(world.dsn)
    application = uuid.UUID(world.application_id)
    application_sql = "SELECT * FROM curie.shared_connector_applications WHERE id = :id"
    before = binding_row(world.binding_id), sql_dicts(application_sql, {"id": application})
    for column in (
        "enabled",
        "application_id",
        "agent_id",
        "provider_account",
        "credential_revision",
        "ciphertext",
    ):
        expect_sqlstate(
            role,
            "42501",
            f"UPDATE curie.shared_connector_bindings SET {column} = {column} WHERE id = $1",
            world.binding_uuid,
        )
    for column in ("enabled", "policy", "policy_generation"):
        expect_sqlstate(
            role,
            "42501",
            f"UPDATE curie.shared_connector_applications SET {column} = {column} WHERE id = $1",
            application,
        )
    after = binding_row(world.binding_id), sql_dicts(application_sql, {"id": application})
    assert after == before


# --- 4. disable after acknowledgement ----------------------------------------


@pytest.mark.parametrize("lock", ["SHARE", "KEY SHARE"])
@pytest.mark.parametrize("target", ["binding", "application"])
def test_disable_waits_for_an_open_resolver_lock_and_later_resolves_refuse(
    world: World, sessions: SessionPool, target: str, lock: str
) -> None:
    resolver = sessions.open(world.dsn)
    resolver.execute("BEGIN")
    admitted = resolver.fetchrow(resolver_read_sql(lock), world.binding_uuid)
    assert admitted is not None
    assert admitted["application_enabled"] is True
    assert admitted["binding_enabled"] is True

    if target == "binding":
        path = f"/bindings/{world.binding_id}/disable"
        acknowledgement = {"binding_id": world.binding_id, "enabled": False, "acknowledged": True}
    else:
        path = f"/applications/{world.application_id}/disable"
        acknowledgement = {
            "application_id": world.application_id,
            "enabled": False,
            "acknowledged": True,
        }
    pending = in_background(lambda: world.store.operator("POST", path))
    assert_still_blocked(pending)
    resolver.execute("COMMIT")
    acknowledged = pending.result(timeout=STATEMENT_WAIT_S)
    assert acknowledged.status_code == 200, acknowledged.text
    assert acknowledged.json() == acknowledgement

    after = sessions.open(world.dsn)
    after.execute("BEGIN")
    refused = after.fetchrow(resolver_read_sql(lock), world.binding_uuid)
    after.execute("COMMIT")
    assert refused is not None
    assert refused["binding_enabled"] is (target != "binding")
    assert refused["application_enabled"] is (target != "application")
    assert _outcome(after, BEGIN_SQL, world.grant_id, "resolver-after-ack", 30) == "disabled"

    again = world.store.operator("POST", path)
    assert again.status_code == 200, again.text
    assert again.json() == acknowledgement


# --- 5. refresh intents -------------------------------------------------------


def test_refresh_begin_has_a_single_owner_and_commit_is_fenced_by_intent_and_epoch(
    world: World, sessions: SessionPool
) -> None:
    owner, other = sessions.open(world.dsn), sessions.open(world.dsn)
    began = _began(owner, world.grant_id, "resolver-a")
    assert began["epoch"] == 1
    assert isinstance(began["intent_id"], uuid.UUID)
    assert began["credential_revision"] == 1
    assert began["recipient_id"] == world.recipient.recipient_id
    assert world.recipient.open(began["ciphertext"]) == world.plaintext
    intent = intent_row(world.binding_id)
    assert (intent["state"], intent["owner"], intent["intent_id"]) == (
        "refreshing",
        "resolver-a",
        began["intent_id"],
    )

    busy = other.fetchrow(BEGIN_SQL, world.grant_id, "resolver-b", 60)
    assert busy is not None
    assert dict(busy) == {
        "outcome": "busy",
        "intent_id": None,
        "epoch": None,
        "credential_revision": None,
        "recipient_id": None,
        "ciphertext": None,
    }

    rotated = _credential()
    sealed = world.recipient.seal(rotated)
    recipient = world.recipient.recipient_id
    for intent_id, epoch in (
        (uuid.uuid4(), began["epoch"]),
        (began["intent_id"], began["epoch"] + 1),
        (began["intent_id"], began["epoch"] - 1),
    ):
        assert (
            _outcome(other, COMMIT_SQL, world.grant_id, intent_id, epoch, recipient, sealed)
            == "stale"
        )
    assert binding_row(world.binding_id)["credential_revision"] == 1

    committed = owner.fetchrow(
        COMMIT_SQL, world.grant_id, began["intent_id"], began["epoch"], recipient, sealed
    )
    assert committed is not None
    assert dict(committed) == {"outcome": "committed", "credential_revision": 2}
    row = binding_row(world.binding_id)
    assert row["credential_revision"] == 2
    assert world.recipient.open(row["ciphertext"]) == rotated
    _ready(world.binding_id, began["epoch"])
    assert (
        _outcome(
            owner, COMMIT_SQL, world.grant_id, began["intent_id"], began["epoch"], recipient, sealed
        )
        == "stale"
    )
    assert _outcome(owner, BEGIN_SQL, f"grant:{uuid.uuid4().hex}", "resolver-a", 60) == "not_found"
    record = world.store.binding(world.binding_id)
    assert record["credential_revision"] == 2
    assert record["refresh"] == {
        "grant_id": world.grant_id,
        "client_config_fingerprint": client_fingerprint(str(world.grant_id)),
        "state": "ready",
        "epoch": began["epoch"],
    }


def test_concurrent_refresh_begins_admit_exactly_one_owner(
    world: World, sessions: SessionPool
) -> None:
    epoch_before = intent_row(world.binding_id)["epoch"]
    holder = sessions.owner()
    holder.execute("BEGIN")
    holder.fetch(
        "SELECT grant_id FROM curie.shared_connector_refresh_intents "
        "WHERE binding_id = $1 FOR UPDATE",
        world.binding_uuid,
    )
    owners = [f"resolver-{index}" for index in range(3)]
    pending = [
        sessions.open(world.dsn).submit("fetchrow", BEGIN_SQL, world.grant_id, owner, 60)
        for owner in owners
    ]
    assert_still_blocked(pending[-1], waiters=len(owners))
    assert not any(future.done() for future in pending)
    holder.execute("COMMIT")

    rows = [future.result(timeout=STATEMENT_WAIT_S) for future in pending]
    outcomes = [row["outcome"] for row in rows]
    assert sorted(outcomes) == ["began", "busy", "busy"], outcomes
    winner = outcomes.index("began")
    for index, row in enumerate(rows):
        if index != winner:
            assert row["intent_id"] is None and row["ciphertext"] is None, dict(row)
    assert rows[winner]["epoch"] == epoch_before + 1
    intent = intent_row(world.binding_id)
    assert intent["state"] == "refreshing"
    assert intent["epoch"] == epoch_before + 1
    assert intent["intent_id"] == rows[winner]["intent_id"]
    assert intent["owner"] == owners[winner]


def test_mark_recovery_blocks_every_begin_and_fences_the_old_intent(
    world: World, sessions: SessionPool
) -> None:
    role = sessions.open(world.dsn)
    assert role.fetchval(MARK_RECOVERY_SQL, world.grant_id, uuid.uuid4(), 0) == "stale"
    began = _began(role, world.grant_id)
    assert role.fetchval(MARK_RECOVERY_SQL, world.grant_id, uuid.uuid4(), began["epoch"]) == "stale"
    assert (
        role.fetchval(MARK_RECOVERY_SQL, f"grant:{uuid.uuid4().hex}", began["intent_id"], 1)
        == "not_found"
    )
    assert (
        role.fetchval(MARK_RECOVERY_SQL, world.grant_id, began["intent_id"], began["epoch"])
        == "recovery_required"
    )
    intent = intent_row(world.binding_id)
    assert (intent["state"], intent["recovery_reason"]) == (
        "recovery_required",
        "uncertain_outcome",
    )

    for owner in ("resolver-a", "resolver-b"):
        assert _outcome(role, BEGIN_SQL, world.grant_id, owner, 60) == "recovery_required"
    late = _outcome(
        role,
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(_credential()),
    )
    assert late == "stale"
    row = binding_row(world.binding_id)
    assert row["credential_revision"] == 1
    assert world.recipient.open(row["ciphertext"]) == world.plaintext
    assert intent_row(world.binding_id)["state"] == "recovery_required"


def test_an_expired_lease_becomes_recovery_on_the_next_begin_and_ownership_never_moves(
    world: World, sessions: SessionPool
) -> None:
    original, other = sessions.open(world.dsn), sessions.open(world.dsn)
    began = _began(original, world.grant_id, "resolver-a", lease=1)
    time.sleep(1.5)
    converted = other.fetchrow(BEGIN_SQL, world.grant_id, "resolver-b", 60)
    assert converted is not None
    assert converted["outcome"] == "recovery_required"
    assert converted["intent_id"] is None and converted["ciphertext"] is None
    intent = intent_row(world.binding_id)
    assert (intent["state"], intent["recovery_reason"]) == ("recovery_required", "lease_expired")
    assert intent["owner"] != "resolver-b"

    late = _outcome(
        original,
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(_credential()),
    )
    assert late == "stale"
    row = binding_row(world.binding_id)
    assert row["credential_revision"] == 1
    assert world.recipient.open(row["ciphertext"]) == world.plaintext
    assert _outcome(other, BEGIN_SQL, world.grant_id, "resolver-b", 60) == "recovery_required"


def test_a_late_commit_with_no_competing_begin_is_accepted(
    world: World, sessions: SessionPool
) -> None:
    role = sessions.open(world.dsn)
    began = _began(role, world.grant_id, "resolver-a", lease=1)
    time.sleep(1.5)
    rotated = _credential()
    committed = role.fetchrow(
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(rotated),
    )
    assert committed is not None
    assert dict(committed) == {"outcome": "committed", "credential_revision": 2}
    assert world.recipient.open(binding_row(world.binding_id)["ciphertext"]) == rotated
    _ready(world.binding_id, began["epoch"])


def test_a_late_commit_that_takes_the_lock_first_commits_and_the_next_begin_advances(
    world: World, sessions: SessionPool
) -> None:
    committer, competitor = sessions.open(world.dsn), sessions.open(world.dsn)
    began = _began(committer, world.grant_id, "resolver-a", lease=1)
    time.sleep(1.5)
    rotated = _credential()
    committer.execute("BEGIN")
    committed = committer.fetchrow(
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(rotated),
    )
    assert committed is not None and committed["outcome"] == "committed"
    pending = competitor.submit("fetchrow", BEGIN_SQL, world.grant_id, "resolver-b", 60)
    assert_still_blocked(pending)
    committer.execute("COMMIT")

    following = pending.result(timeout=STATEMENT_WAIT_S)
    assert following["outcome"] == "began", following
    assert following["epoch"] == began["epoch"] + 1
    assert following["credential_revision"] == 2
    assert world.recipient.open(following["ciphertext"]) == rotated
    assert intent_row(world.binding_id)["owner"] == "resolver-b"


def test_an_expiring_begin_that_takes_the_lock_first_makes_the_late_commit_stale(
    world: World, sessions: SessionPool
) -> None:
    committer, competitor = sessions.open(world.dsn), sessions.open(world.dsn)
    began = _began(committer, world.grant_id, "resolver-a", lease=1)
    time.sleep(1.5)
    competitor.execute("BEGIN")
    assert _outcome(competitor, BEGIN_SQL, world.grant_id, "resolver-b", 60) == "recovery_required"
    pending = committer.submit(
        "fetchrow",
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(_credential()),
    )
    assert_still_blocked(pending)
    competitor.execute("COMMIT")

    late = pending.result(timeout=STATEMENT_WAIT_S)
    assert late["outcome"] == "stale", late
    intent = intent_row(world.binding_id)
    assert (intent["state"], intent["recovery_reason"]) == ("recovery_required", "lease_expired")
    row = binding_row(world.binding_id)
    assert row["credential_revision"] == 1
    assert world.recipient.open(row["ciphertext"]) == world.plaintext


@pytest.mark.parametrize("size", [48, 65537])
def test_an_out_of_bounds_commit_raises_and_changes_neither_ciphertext_nor_intent(
    world: World, sessions: SessionPool, size: int
) -> None:
    role = sessions.open(world.dsn)
    began = _began(role, world.grant_id)
    binding_before, intent_before = binding_row(world.binding_id), intent_row(world.binding_id)
    expect_sqlstate(
        role,
        "22023",
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        b"\x01" * size,
    )
    assert binding_row(world.binding_id) == binding_before
    assert intent_row(world.binding_id) == intent_before

    committed = _outcome(
        role,
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(_credential()),
    )
    assert committed == "committed"


def test_a_commit_sealed_to_an_unknown_recipient_changes_nothing(
    world: World, sessions: SessionPool
) -> None:
    role = sessions.open(world.dsn)
    began = _began(role, world.grant_id)
    stranger = Recipient.generate()
    binding_before, intent_before = binding_row(world.binding_id), intent_row(world.binding_id)
    unknown = _outcome(
        role,
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        stranger.recipient_id,
        stranger.seal(_credential()),
    )
    assert unknown == "recipient_unknown"
    assert binding_row(world.binding_id) == binding_before
    assert intent_row(world.binding_id) == intent_before
    committed = _outcome(
        role,
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(_credential()),
    )
    assert committed == "committed"


def test_explicit_reimport_resets_refresh_state_and_fences_the_pre_reset_intent(
    world: World, sessions: SessionPool
) -> None:
    role = sessions.open(world.dsn)
    began = _began(role, world.grant_id)
    replacement = _credential("REIMPORTED")
    reset = world.reimport(1, replacement, resource_version="101")
    assert reset.status_code == 200, reset.text
    assert reset.json()["refresh"] == {
        "grant_id": world.grant_id,
        "client_config_fingerprint": client_fingerprint(str(world.grant_id)),
        "state": "ready",
        "epoch": began["epoch"] + 1,
    }
    _ready(world.binding_id, began["epoch"] + 1)
    stale = _outcome(
        role,
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(_credential()),
    )
    assert stale == "stale"
    row = binding_row(world.binding_id)
    assert row["credential_revision"] == 2
    assert world.recipient.open(row["ciphertext"]) == replacement

    second = _began(role, world.grant_id)
    assert second["epoch"] == began["epoch"] + 2
    assert second["credential_revision"] == 2
    assert (
        role.fetchval(MARK_RECOVERY_SQL, world.grant_id, second["intent_id"], second["epoch"])
        == "recovery_required"
    )
    assert _outcome(role, BEGIN_SQL, world.grant_id, "resolver-a", 60) == "recovery_required"
    recovered = world.reimport(2, _credential("RECOVERED"), resource_version="102")
    assert recovered.status_code == 200, recovered.text
    _ready(world.binding_id, second["epoch"] + 1)
    assert _began(role, world.grant_id)["epoch"] == second["epoch"] + 2


def test_the_application_role_cannot_write_refresh_state_directly(
    world: World, sessions: SessionPool
) -> None:
    role = sessions.open(world.dsn)
    began = _began(role, world.grant_id)
    assert (
        role.fetchval(MARK_RECOVERY_SQL, world.grant_id, began["intent_id"], began["epoch"])
        == "recovery_required"
    )
    before = intent_row(world.binding_id)
    for statement in (
        "UPDATE curie.shared_connector_refresh_intents SET state = 'ready', recovery_reason = NULL",
        "UPDATE curie.shared_connector_refresh_intents SET epoch = epoch + 1",
        "DELETE FROM curie.shared_connector_refresh_intents",
    ):
        expect_sqlstate(role, "42501", statement)
    assert intent_row(world.binding_id) == before
    assert _outcome(role, BEGIN_SQL, world.grant_id, "resolver-a", 60) == "recovery_required"


def test_a_login_that_is_not_an_application_role_cannot_begin_a_refresh(
    world: World, sessions: SessionPool, cleanup: SharedCleanup
) -> None:
    owner = sessions.owner()
    refused = expect_sqlstate(owner, "42501", BEGIN_SQL, world.grant_id, "platform", 30)
    assert "shared_refresh_not_authorized" in str(refused)

    plain = cleanup.role(f"sca_plain_{secrets.token_hex(6)}")
    password = secrets.token_urlsafe(18)
    owner.execute(f"CREATE ROLE {quote_ident(plain)} LOGIN PASSWORD '{password}'")
    url = make_url(get_settings().database_url).set(
        drivername="postgresql", username=plain, password=password
    )
    stranger = sessions.open(render_url(url))
    expect_sqlstate(stranger, "42501", BEGIN_SQL, world.grant_id, "stranger", 30)
    owner.execute(f"GRANT USAGE ON SCHEMA curie TO {quote_ident(plain)}")
    owner.execute(
        "GRANT EXECUTE ON FUNCTION curie.shared_connector_refresh_begin(text, text, integer) "
        f"TO {quote_ident(plain)}"
    )
    refused = expect_sqlstate(stranger, "42501", BEGIN_SQL, world.grant_id, "stranger", 30)
    assert "shared_refresh_not_authorized" in str(refused)
    _ready(world.binding_id, 0)

    assert _began(sessions.open(world.dsn), world.grant_id)["epoch"] == 1


@pytest.mark.parametrize("target", ["binding", "application"])
def test_begin_answers_disabled_once_the_binding_or_application_is_disabled(
    world: World, sessions: SessionPool, target: str
) -> None:
    role = sessions.open(world.dsn)
    began = _began(role, world.grant_id)
    committed = _outcome(
        role,
        COMMIT_SQL,
        world.grant_id,
        began["intent_id"],
        began["epoch"],
        world.recipient.recipient_id,
        world.recipient.seal(_credential()),
    )
    assert committed == "committed"
    disabled = (
        world.store.disable_binding(world.binding_id)
        if target == "binding"
        else world.store.disable_application(world.application_id)
    )
    assert disabled.status_code == 200, disabled.text

    refused = role.fetchrow(BEGIN_SQL, world.grant_id, "resolver-a", 60)
    assert refused is not None
    assert dict(refused) == {
        "outcome": "disabled",
        "intent_id": None,
        "epoch": None,
        "credential_revision": None,
        "recipient_id": None,
        "ciphertext": None,
    }
    _ready(world.binding_id, began["epoch"])


def test_reimport_and_begin_on_the_same_binding_both_finish_without_deadlock(
    world: World, sessions: SessionPool
) -> None:
    holder = sessions.open(world.dsn)
    holder.execute("BEGIN")
    began = _began(holder, world.grant_id)
    pending = in_background(lambda: world.reimport(1, _credential("HELD"), resource_version="101"))
    assert_still_blocked(pending)
    holder.execute("COMMIT")
    reimported = pending.result(timeout=STATEMENT_WAIT_S)
    assert reimported.status_code == 200, reimported.text
    assert reimported.json()["refresh"]["state"] == "ready"
    assert reimported.json()["refresh"]["epoch"] == began["epoch"] + 1

    racer = sessions.open(world.dsn)
    revision = 2
    for round_number in range(5):
        start = threading.Barrier(2, timeout=STATEMENT_WAIT_S)
        source = source_metadata(uid=world.source["uid"], resource_version=str(200 + round_number))

        def reimport(
            expected: int = revision,
            source: dict[str, str] = source,
            start: threading.Barrier = start,
        ) -> Any:
            start.wait()
            return world.store.reimport(
                world.binding_id, expected, world.recipient, _credential("RACE"), source
            )

        racing = in_background(reimport)
        start.wait()
        outcome = _outcome(racer, BEGIN_SQL, world.grant_id, f"racer-{round_number}", 60)
        response = racing.result(timeout=STATEMENT_WAIT_S)
        assert response.status_code == 200, response.text
        assert outcome in {"began", "busy"}, outcome
        revision += 1
    assert binding_row(world.binding_id)["credential_revision"] == revision


# --- 6. source_stale ---------------------------------------------------------


def test_a_source_check_reports_staleness_without_credential_material(world: World) -> None:
    store = world.store
    uid, version = world.source["uid"], world.source["resource_version"]
    current = store.check(world.binding_id, uid, version)
    assert current.status_code == 200, current.text
    assert current.json() == {
        "binding_id": world.binding_id,
        "status": "current",
        "credential_revision": 1,
        "recorded_source": {"uid": uid, "resource_version": version},
    }
    for changed_uid, changed_version in (
        (uid, "101"),
        ("uid-replaced", version),
        ("uid-replaced", "101"),
    ):
        stale = store.check(world.binding_id, changed_uid, changed_version)
        assert stale.status_code == 200, stale.text
        assert stale.json() == {
            "binding_id": world.binding_id,
            "status": "source_stale",
            "credential_revision": 1,
            "recorded_source": {"uid": uid, "resource_version": version},
        }
    stored = binding_row(world.binding_id)
    assert stored["credential_revision"] == 1
    sentinel = json.loads(world.plaintext)[SLOT]
    for response in store.responses:
        assert sentinel not in response.text
        assert b64(stored["ciphertext"]) not in response.text

    reimported = world.reimport(1, _credential("REIMPORTED"), resource_version="101")
    assert reimported.status_code == 200, reimported.text
    caught_up = store.check(world.binding_id, uid, "101")
    assert caught_up.json() == {
        "binding_id": world.binding_id,
        "status": "current",
        "credential_revision": 2,
        "recorded_source": {"uid": uid, "resource_version": "101"},
    }
    assert store.check(world.binding_id, uid, version).json()["status"] == "source_stale"
    assert_refused(store.check(str(uuid.uuid4()), uid, version), 404, "shared_binding_not_found")


def _check_body(world: World, status: str, revision: int = 1) -> dict[str, Any]:
    """The unchanged check response for ``world``'s recorded source."""

    return {
        "binding_id": world.binding_id,
        "status": status,
        "credential_revision": revision,
        "recorded_source": {
            "uid": world.source["uid"],
            "resource_version": world.source["resource_version"],
        },
    }


def test_a_mismatched_check_persists_source_stale_for_every_later_check(world: World) -> None:
    """The resolver cannot see a request; only the stored flag refuses for it (R1-8)."""

    store = world.store
    uid, version = world.source["uid"], world.source["resource_version"]
    assert binding_row(world.binding_id)["source_stale"] is False

    mismatched = store.check(world.binding_id, uid, "101")
    assert mismatched.status_code == 200, mismatched.text
    assert mismatched.json() == _check_body(world, "source_stale")
    # The request has returned, so the flag was committed, not left in its session.
    assert binding_row(world.binding_id)["source_stale"] is True

    # A fresh request carrying the RECORDED source still reports the stored flag.
    matching = store.check(world.binding_id, uid, version)
    assert matching.status_code == 200, matching.text
    assert matching.json() == _check_body(world, "source_stale")
    assert binding_row(world.binding_id)["source_stale"] is True
    assert store.binding(world.binding_id)["credential_revision"] == 1


def test_reimport_clears_source_stale_and_a_never_checked_binding_reports_current(
    world: World,
) -> None:
    store = world.store
    uid, version = world.source["uid"], world.source["resource_version"]
    assert store.check(world.binding_id, uid, "101").json()["status"] == "source_stale"
    assert binding_row(world.binding_id)["source_stale"] is True

    # A reimport that loses the CAS changes nothing, the flag included.
    lost = store.reimport(
        world.binding_id,
        7,
        world.recipient,
        _credential("LOST"),
        source_metadata(uid=uid, resource_version="101"),
    )
    assert_refused(lost, 409, "shared_binding_revision_conflict")
    assert binding_row(world.binding_id)["source_stale"] is True

    reimported = world.reimport(1, _credential("REIMPORTED"), resource_version="101")
    assert reimported.status_code == 200, reimported.text
    assert binding_row(world.binding_id)["source_stale"] is False
    caught_up = store.check(world.binding_id, uid, "101")
    assert caught_up.status_code == 200, caught_up.text
    assert caught_up.json() == {
        "binding_id": world.binding_id,
        "status": "current",
        "credential_revision": 2,
        "recorded_source": {"uid": uid, "resource_version": "101"},
    }
    assert binding_row(world.binding_id)["source_stale"] is False

    # Liveness: a binding that was never checked stale is current, with the flag unset.
    untouched = build_world(store)
    assert binding_row(untouched.binding_id)["source_stale"] is False
    never = store.check(untouched.binding_id, untouched.source["uid"], version)
    assert never.status_code == 200, never.text
    assert never.json() == _check_body(untouched, "current")
    assert binding_row(untouched.binding_id)["source_stale"] is False


def test_a_matching_check_never_sets_source_stale_or_changes_the_row(world: World) -> None:
    uid, version = world.source["uid"], world.source["resource_version"]
    before = binding_row(world.binding_id)
    assert before["source_stale"] is False
    for _ in range(3):
        current = world.store.check(world.binding_id, uid, version)
        assert current.status_code == 200, current.text
        assert current.json() == _check_body(world, "current")
        assert binding_row(world.binding_id) == before


def test_connector_custody_cannot_hold_source_stale(
    store: SharedStore, sessions: SessionPool
) -> None:
    application_id = store.register_application(slots=(), custody="connector")["application_id"]
    store.register_recipient(application_id)
    created = store.post_binding(
        application_id, store.binding_body(store.create_agent(), None, None)
    )
    assert created.status_code == 201, created.text
    connector_id = created.json()["binding_id"]
    before = binding_row(connector_id)
    assert (before["custody"], before["source_stale"]) == ("connector", False)

    owner = sessions.owner()
    error = expect_sqlstate(
        owner,
        "23514",
        "UPDATE curie.shared_connector_bindings SET source_stale = true WHERE id = $1",
        uuid.UUID(connector_id),
    )
    assert error.constraint_name == "shared_connector_bindings_source_stale_ck", str(error)
    assert binding_row(connector_id) == before

    # Liveness: the same write is accepted for a platform binding.
    platform = build_world(store)
    flag = "UPDATE curie.shared_connector_bindings SET source_stale = $2 WHERE id = $1"
    assert owner.execute(flag, platform.binding_uuid, True) == "UPDATE 1"
    assert binding_row(platform.binding_id)["source_stale"] is True
    assert owner.execute(flag, platform.binding_uuid, False) == "UPDATE 1"
    assert binding_row(platform.binding_id)["source_stale"] is False


def test_the_application_role_cannot_update_source_stale(
    world: World, sessions: SessionPool
) -> None:
    assert world.store.check(world.binding_id, world.source["uid"], "101").json()["status"] == (
        "source_stale"
    )
    before = binding_row(world.binding_id)
    assert before["source_stale"] is True

    role = sessions.open(world.dsn)
    expect_sqlstate(
        role,
        "42501",
        "UPDATE curie.shared_connector_bindings SET source_stale = false WHERE id = $1",
        world.binding_uuid,
    )
    assert binding_row(world.binding_id) == before
    # Reading the column stays allowed: the resolver observes the flag.
    assert (
        role.fetchval(
            "SELECT source_stale FROM curie.shared_connector_bindings WHERE id = $1",
            world.binding_uuid,
        )
        is True
    )


# --- 7. reseal ---------------------------------------------------------------


def test_the_role_reseals_with_cas_and_the_old_recipient_retires_after_dsn_rotation(
    world: World, sessions: SessionPool
) -> None:
    store, old = world.store, world.recipient
    new = store.register_recipient(world.application_id)
    role = sessions.open(world.dsn)
    resealed = new.seal(world.plaintext)
    before = binding_row(world.binding_id)
    unknown_target = Recipient.generate().recipient_id
    for args, outcome in (
        ((world.binding_uuid, 2, old.recipient_id, new.recipient_id, resealed), "stale"),
        ((world.binding_uuid, 1, "f" * 64, new.recipient_id, resealed), "stale"),
        ((world.binding_uuid, 1, old.recipient_id, unknown_target, resealed), "recipient_unknown"),
        (
            (world.binding_uuid, 1, old.recipient_id, old.recipient_id, old.seal(world.plaintext)),
            "recipient_unknown",
        ),
        ((uuid.uuid4(), 1, old.recipient_id, new.recipient_id, resealed), "not_found"),
    ):
        assert role.fetchval(RESEAL_SQL, *args) == outcome, (args[1:4], outcome)
        assert binding_row(world.binding_id) == before
    expect_sqlstate(
        role,
        "22023",
        RESEAL_SQL,
        world.binding_uuid,
        1,
        old.recipient_id,
        new.recipient_id,
        b"\x01" * 48,
    )
    assert binding_row(world.binding_id) == before

    assert (
        role.fetchval(
            RESEAL_SQL, world.binding_uuid, 1, old.recipient_id, new.recipient_id, resealed
        )
        == "resealed"
    )
    after = binding_row(world.binding_id)
    assert after["credential_revision"] == 1
    assert after["recipient_id"] == new.recipient_id
    assert new.open(after["ciphertext"]) == world.plaintext
    with pytest.raises(nacl.exceptions.CryptoError):
        old.open(after["ciphertext"])
    assert store.binding(world.binding_id)["recipient_id"] == new.recipient_id

    assert_refused(
        store.retire(world.application_id, old.recipient_id), 409, "shared_recipient_referenced"
    )
    rotated = store.rotate(world.application_id, 1)
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["recipient_id"] == new.recipient_id
    assert rotated.json()["dsn_revision"] == 2
    assert connect_as(new.open(rotated.json()["sealed_dsn"]).decode()) == world.role_name

    retired = store.retire(world.application_id, old.recipient_id)
    assert retired.status_code == 200, retired.text
    assert retired.json()["application_id"] == world.application_id
    assert retired.json()["recipient_id"] == old.recipient_id
    assert retired.json()["retired_at"] is not None
    recipients = {
        item["recipient_id"]: item for item in store.application(world.application_id)["recipients"]
    }
    assert recipients[old.recipient_id]["retired_at"] is not None
    assert recipients[old.recipient_id]["current"] is False
    assert recipients[new.recipient_id]["retired_at"] is None
    assert recipients[new.recipient_id]["current"] is True


# --- 8. retirement race ------------------------------------------------------


def _resealed_to_a_second_recipient(
    store: SharedStore, sessions: SessionPool
) -> tuple[str, str, Recipient, Recipient, bytes, PgSession]:
    """A binding first sealed to ``first``, now resealed to the current ``second``.

    The DSN is sealed to ``second`` too, so ``first`` is live, not current and
    unreferenced: exactly the recipient a writer and a retire can race over.
    """

    application_id = store.register_application()["application_id"]
    first = store.register_recipient(application_id)
    agent_id = store.create_agent()
    plaintext = _credential("RACE")
    binding_id = store.import_binding(application_id, agent_id, first, plaintext)["binding_id"]
    second = store.register_recipient(application_id)
    record, dsn = store.provision_bundled(application_id, second)
    assert record["recipient_id"] == second.recipient_id
    role = sessions.open(dsn)
    assert (
        role.fetchval(
            RESEAL_SQL,
            uuid.UUID(binding_id),
            1,
            first.recipient_id,
            second.recipient_id,
            second.seal(plaintext),
        )
        == "resealed"
    )
    return application_id, binding_id, first, second, plaintext, role


def test_a_retire_waits_for_a_writer_holding_the_recipient_and_then_refuses(
    store: SharedStore, sessions: SessionPool
) -> None:
    application_id, binding_id, first, second, plaintext, role = _resealed_to_a_second_recipient(
        store, sessions
    )
    role.execute("BEGIN")
    assert (
        role.fetchval(
            RESEAL_SQL,
            uuid.UUID(binding_id),
            1,
            second.recipient_id,
            first.recipient_id,
            first.seal(plaintext),
        )
        == "resealed"
    )
    pending = in_background(lambda: store.retire(application_id, first.recipient_id))
    assert_still_blocked(pending)
    role.execute("COMMIT")
    assert_refused(pending.result(timeout=STATEMENT_WAIT_S), 409, "shared_recipient_referenced")
    recipients = {
        item["recipient_id"]: item for item in store.application(application_id)["recipients"]
    }
    assert recipients[first.recipient_id]["retired_at"] is None
    assert binding_row(binding_id)["recipient_id"] == first.recipient_id


def test_a_committed_retirement_makes_later_writers_refuse_the_recipient(
    store: SharedStore, sessions: SessionPool
) -> None:
    application_id, binding_id, first, second, plaintext, role = _resealed_to_a_second_recipient(
        store, sessions
    )
    retired = store.retire(application_id, first.recipient_id)
    assert retired.status_code == 200, retired.text
    before = binding_row(binding_id)

    assert (
        role.fetchval(
            RESEAL_SQL,
            uuid.UUID(binding_id),
            1,
            second.recipient_id,
            first.recipient_id,
            first.seal(plaintext),
        )
        == "recipient_unknown"
    )
    source = source_metadata(uid="uid-retired", resource_version="300")
    assert_refused(
        store.reimport(binding_id, 1, first, plaintext, source), 409, "shared_recipient_unknown"
    )
    assert_refused(
        store.post_binding(
            application_id,
            store.binding_body(store.create_agent(), first, plaintext, source=source),
        ),
        409,
        "shared_recipient_unknown",
    )
    assert_refused(
        store.worker(
            "POST",
            f"/applications/{application_id}/recipients",
            first.registration(application_id),
        ),
        409,
        "shared_recipient_retired",
    )
    assert binding_row(binding_id) == before

    accepted = store.reimport(binding_id, 1, second, plaintext, source)
    assert accepted.status_code == 200, accepted.text
    assert accepted.json()["credential_revision"] == 2


# --- 9. the store's trigger functions are not a back door --------------------

# The membership triggers are SECURITY DEFINER: they raise the policy generation
# of every application id in their transition tables as the function owner.
TRIGGER_FUNCTIONS_SQL = r"""
SELECT p.proname AS name,
       has_function_privilege(current_user, p.oid, 'EXECUTE') AS executable
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'curie'
  AND p.proname LIKE 'shared\_connector%'
  AND p.prorettype = 'trigger'::regtype
ORDER BY p.proname
"""
CAS_FUNCTIONS_SQL = """
SELECT p.proname AS name,
       has_function_privilege(current_user, p.oid, 'EXECUTE') AS executable
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'curie' AND p.proname = ANY($1::text[])
ORDER BY p.proname
"""
# event -> (operation, REFERENCING clause, statement that fires the trigger)
MEMBERSHIP_TRIGGER_SHAPES: dict[str, tuple[str, str, str]] = {
    "inserted": (
        "INSERT",
        "NEW TABLE AS inserted_rows",
        "INSERT INTO {table} VALUES ('{application}')",
    ),
    "updated": (
        "UPDATE",
        "OLD TABLE AS old_rows NEW TABLE AS new_rows",
        "UPDATE {table} SET application_id = application_id",
    ),
    "deleted": ("DELETE", "OLD TABLE AS deleted_rows", "DELETE FROM {table}"),
}


def test_an_application_role_cannot_reuse_the_store_trigger_functions(
    store: SharedStore, sessions: SessionPool
) -> None:
    """A role may not attach an owner-run trigger function to a table of its own.

    Liveness: test_member_aliases_narrow_the_policy_and_raise_the_generation_only_on_change
    and test_deleting_an_agent_removes_its_membership_and_binding_and_raises_the_generation
    (test_shared_connector_admin.py) show real membership writes still raise the generation,
    and the CAS functions asserted executable below are the ones the role does use.
    """

    mine, theirs = build_world(store), build_world(store)
    role = sessions.open(mine.dsn)
    their_application = uuid.UUID(theirs.application_id)
    generation = application_row(theirs.application_id)["policy_generation"]

    triggers = role.fetch(TRIGGER_FUNCTIONS_SQL)
    assert "shared_connector_memberships_inserted" in {row["name"] for row in triggers}
    assert [row["name"] for row in triggers if row["executable"]] == []
    cas = role.fetch(CAS_FUNCTIONS_SQL, list(SHARED_FUNCTIONS))
    assert {row["name"]: row["executable"] for row in cas} == dict.fromkeys(SHARED_FUNCTIONS, True)

    # A temporary table dies with this session, which the pool closes at teardown.
    table = f"hijack_{secrets.token_hex(4)}"
    role.execute(f"CREATE TEMP TABLE {table} (application_id uuid)")
    expect_sqlstate(
        role,
        "42501",
        f"CREATE TRIGGER {table}_bump AFTER INSERT ON {table} "
        "REFERENCING NEW TABLE AS inserted_rows FOR EACH STATEMENT "
        "EXECUTE FUNCTION curie.shared_connector_memberships_inserted()",
    )
    role.execute(f"INSERT INTO {table} VALUES ($1)", their_application)
    assert application_row(theirs.application_id)["policy_generation"] == generation


@pytest.mark.parametrize("event", list(MEMBERSHIP_TRIGGER_SHAPES))
def test_a_membership_trigger_function_refuses_to_run_for_any_other_table(
    store: SharedStore, sessions: SessionPool, event: str
) -> None:
    """Defence in depth: even the owner cannot point a membership trigger at another table.

    Liveness: test_member_aliases_narrow_the_policy_and_raise_the_generation_only_on_change
    and test_deleting_an_agent_removes_its_membership_and_binding_and_raises_the_generation
    (test_shared_connector_admin.py) show the same functions still fire for the real table.
    """

    application_id = store.register_application()["application_id"]
    generation = application_row(application_id)["policy_generation"]
    operation, referencing, statement = MEMBERSHIP_TRIGGER_SHAPES[event]

    # A temporary table dies with this session, which the pool closes at teardown.
    owner = sessions.owner()
    table = f"scope_{secrets.token_hex(4)}"
    owner.execute(f"CREATE TEMP TABLE {table} (application_id uuid)")
    owner.execute(f"INSERT INTO {table} VALUES ('{application_id}')")
    owner.execute(
        f"CREATE TRIGGER {table}_scope AFTER {operation} ON {table} "
        f"REFERENCING {referencing} FOR EACH STATEMENT "
        f"EXECUTE FUNCTION curie.shared_connector_memberships_{event}()"
    )
    error = expect_sqlstate(
        owner, "42501", statement.format(table=table, application=application_id)
    )
    assert str(error).startswith("shared_connector_trigger_scope"), str(error)
    assert application_row(application_id)["policy_generation"] == generation
