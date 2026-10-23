"""The API publishes the shared proxy's caller verify keys (ADR 0239, cp5, AC2).

Every API process, at startup and then every 60 s for its lifetime, upserts the
operator-configured current and previous caller verify keys (the same
``Settings.connector_proxy()`` source dedicated proxies render from) and a
heartbeat for its observed installation, in one transaction on database time.
The shared proxy accepts the keys seen within 300 s of the installation's
newest heartbeat, read through its own application role. So the accepted set
is the union of keys carried by API processes that are live within the window:
rolling updates, rollbacks and in-place rotations converge by republishing, a
retired key ages out once no live process carries it, and an API outage
freezes the last set instead of expiring it.

Every case goes through the real lifespan entrypoint: ``shared_app`` builds
``create_app()`` under a TestClient, so the startup tick of
``caller_keys.run_publisher`` runs. Aging is simulated by writing ``seen_at``
through the owner connection. The accepted set is read the way the proxy reads
it, ``curie_connector_proxy.shared.read_caller_keys`` over a pool opened as the
bundled application role with the DSN the API sealed. Nothing is faked.
"""

from __future__ import annotations

import asyncio
import base64
import secrets
import time
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any

import asyncpg
import nacl.signing
import pytest
from _migration_support import IsolatedMigrationDb, alembic_config, render_url, sql_dicts
from _shared_connector_support import (
    APPLICATION_TABLES,
    CALLER_KEY_TABLES,
    SHARED_FUNCTIONS,
    SessionPool,
    SharedCleanup,
    SharedStore,
    assert_refused,
    capture_debug_logs,
    captured_log_text,
    database_role_rows,
    expect_sqlstate,
    quote_ident,
    random_installation,
    regclass,
    role_exists,
    role_name_for,
    shared_app,
)
from alembic import command
from curie_api.config import get_settings
from curie_api.db import create_engine, create_sessionmaker
from curie_api.shared_connectors import caller_keys, database_roles
from curie_connector_proxy import shared
from curie_connector_proxy.resolver import privileges
from pydantic import ValidationError
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

KEYS = "shared_connector_caller_keys"
HEARTBEATS = "shared_connector_caller_key_heartbeats"
CURRENT_ENV = "CURIE_CONNECTOR_CALLER_PUBLIC_KEY"
PREVIOUS_ENV = "CURIE_CONNECTOR_CALLER_PREVIOUS_PUBLIC_KEY"
PROXY_IMAGE_ENV = "CURIE_CONNECTOR_PROXY_IMAGE"
PROXY_IMAGE = "ghcr.io/curie-eng/curie-worker:0.0.0"
SCHEMA_UNAVAILABLE = "shared_caller_keys_schema_unavailable"
# The proxy's window, in seconds, relative to the newest heartbeat.
WINDOW_S = 300
# Bounds every wait for a publisher tick, so a missing tick fails the test.
TICK_WAIT_S = 15.0
# What the managed-database documentation tells an operator to grant, besides
# SELECT on every shared table.
FUNCTION_SIGNATURES = (
    "shared_connector_refresh_begin(text, text, integer)",
    "shared_connector_refresh_commit(text, uuid, bigint, text, bytea)",
    "shared_connector_refresh_mark_recovery(text, uuid, bigint)",
    "shared_connector_reseal(uuid, bigint, text, text, bytea)",
)


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


# --- helpers ----------------------------------------------------------------


def _verify_key() -> str:
    """A fresh Ed25519 caller verify key, standard base64, as the chart carries it."""

    return base64.b64encode(bytes(nacl.signing.SigningKey.generate().verify_key)).decode()


def _keys_env(current: str | None, previous: str | None = None) -> dict[str, str]:
    """The API's caller verify key configuration; blank means none."""

    return {
        CURRENT_ENV: current or "",
        PREVIOUS_ENV: previous or "",
        PROXY_IMAGE_ENV: PROXY_IMAGE,
    }


def _state(installation: str) -> tuple[dict[str, datetime], datetime | None]:
    """The owner's view: each published key's ``seen_at``, and the heartbeat."""

    keys = {
        row["public_key"]: row["seen_at"]
        for row in sql_dicts(
            f"SELECT public_key, seen_at FROM curie.{KEYS} WHERE installation_id = :i",
            {"i": installation},
        )
    }
    beats = sql_dicts(
        f"SELECT seen_at FROM curie.{HEARTBEATS} WHERE installation_id = :i", {"i": installation}
    )
    assert len(beats) <= 1, beats
    return keys, (beats[0]["seen_at"] if beats else None)


def _wait[T](probe: Callable[[], T | None], what: str, timeout: float = TICK_WAIT_S) -> T:
    deadline = time.monotonic() + timeout
    while True:
        value = probe()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.05)


def _tick(installation: str, keys: set[str], after: datetime | None = None) -> datetime:
    """Wait for one publisher tick that carried exactly ``keys``; answer its heartbeat.

    A tick upserts its keys and the heartbeat in one transaction on database
    time, so every key it carried has ``seen_at`` equal to that heartbeat.
    """

    def probe() -> datetime | None:
        published, heartbeat = _state(installation)
        if heartbeat is None or (after is not None and heartbeat <= after):
            return None
        carried = {key for key, seen in published.items() if seen == heartbeat}
        return heartbeat if carried == keys else None

    return _wait(probe, f"a publisher tick for {installation} carrying {sorted(keys)}")


def _age(installation: str, key: str, seconds: int) -> None:
    """Move ``key`` to ``seconds`` before the installation's newest heartbeat."""

    sql_dicts(
        f"UPDATE curie.{KEYS} k SET seen_at = h.seen_at - make_interval(secs => :s) "
        f"FROM curie.{HEARTBEATS} h WHERE h.installation_id = k.installation_id "
        "AND k.installation_id = :i AND k.public_key = :k",
        {"i": installation, "k": key, "s": seconds},
    )


def _shift_everything(installation: str, seconds: int) -> None:
    """Every row of the installation, keys and heartbeat alike, ``seconds`` older."""

    for table in CALLER_KEY_TABLES:
        sql_dicts(
            f"UPDATE curie.{table} SET seen_at = seen_at - make_interval(secs => :s) "
            "WHERE installation_id = :i",
            {"i": installation, "s": seconds},
        )


def _read(dsn: str, installation: str) -> tuple[Any, ...]:
    """``read_caller_keys`` exactly as the proxy calls it, over the role's own pool."""

    async def run() -> tuple[Any, ...]:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        try:
            return tuple(await shared.read_caller_keys(pool, installation))
        finally:
            await pool.close()

    return asyncio.run(run())


def _accepted(dsn: str, installation: str) -> set[str]:
    return {base64.b64encode(bytes(key)).decode() for key in _read(dsn, installation)}


def _refused(dsn: str, installation: str) -> str:
    with pytest.raises(shared.SharedRefusal) as refused:
        _read(dsn, installation)
    return str(refused.value)


def _reader(store: SharedStore) -> tuple[str, str]:
    """A bundled application of ``store``'s installation; answer its id and opened DSN."""

    application_id = store.register_application()["application_id"]
    recipient = store.register_recipient(application_id)
    _, dsn = store.provision_bundled(application_id, recipient)
    return application_id, dsn


def _session_call(factory: Callable[[Any], Any]) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with sessions() as session:
                return await factory(session)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _publish_once() -> str:
    """One direct ``caller_keys.publish`` under the current environment's settings."""

    return str(_session_call(lambda session: caller_keys.publish(session, get_settings())))


# --- the live publisher window ----------------------------------------------


def test_first_publication_on_empty_table(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    current, previous = _verify_key(), _verify_key()
    assert _state(installation) == ({}, None)

    with shared_app(monkeypatch, installation, **_keys_env(current, previous)) as client:
        store = SharedStore(client, installation, cleanup)
        heartbeat = _tick(installation, {current, previous})
        _, dsn = _reader(store)
        assert _accepted(dsn, installation) == {current, previous}

    published, beat = _state(installation)
    assert set(published) == {current, previous}
    assert beat is not None and beat >= heartbeat
    # Negative: a key no process carries is never accepted.
    assert _verify_key() not in _accepted(dsn, installation)


def test_two_live_publishers_are_both_accepted(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    k1, k2 = _verify_key(), _verify_key()

    with shared_app(monkeypatch, installation, **_keys_env(k2)) as x:
        first = _tick(installation, {k2})
        _, dsn = _reader(SharedStore(x, installation, cleanup))
        # Y is mid-rotation: it carries k2 as current and k1 as previous.
        with shared_app(monkeypatch, installation, **_keys_env(k2, k1)):
            _tick(installation, {k1, k2}, after=first)
            assert _accepted(dsn, installation) == {k1, k2}
    assert set(_state(installation)[0]) == {k1, k2}


def test_stopped_publisher_ages_out(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    k1, k2 = _verify_key(), _verify_key()

    with shared_app(monkeypatch, installation, **_keys_env(k2)) as x:
        first = _tick(installation, {k2})
        _, dsn = _reader(SharedStore(x, installation, cleanup))
        with shared_app(monkeypatch, installation, **_keys_env(k1)):
            _tick(installation, {k1}, after=first)
        # Y has stopped; X stays live. Control: inside the window k1 still counts.
        _age(installation, k1, WINDOW_S - 1)
        assert _accepted(dsn, installation) == {k1, k2}
        _age(installation, k1, WINDOW_S + 1)
        assert _accepted(dsn, installation) == {k2}


def test_in_place_rotation(cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch) -> None:
    installation = cleanup.installation(random_installation())
    k1, k3 = _verify_key(), _verify_key()

    with shared_app(monkeypatch, installation, **_keys_env(k1)) as client:
        first = _tick(installation, {k1})
        _, dsn = _reader(SharedStore(client, installation, cleanup))
    assert _accepted(dsn, installation) == {k1}

    # The Secret is rotated in place and the API restarts carrying only k3.
    with shared_app(monkeypatch, installation, **_keys_env(k3)):
        _tick(installation, {k3}, after=first)
        assert k3 in _accepted(dsn, installation)
        _age(installation, k1, WINDOW_S + 1)
        assert _accepted(dsn, installation) == {k3}


def test_rollback_republishes_older_set(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    k1, k2 = _verify_key(), _verify_key()

    with shared_app(monkeypatch, installation, **_keys_env(k1)) as client:
        first = _tick(installation, {k1})
        _, dsn = _reader(SharedStore(client, installation, cleanup))
    with shared_app(monkeypatch, installation, **_keys_env(k2)):
        second = _tick(installation, {k2}, after=first)
        _age(installation, k1, WINDOW_S + 1)
        assert _accepted(dsn, installation) == {k2}

    # A rollback restarts the process with the older configuration.
    with shared_app(monkeypatch, installation, **_keys_env(k1)):
        _tick(installation, {k1}, after=second)
        assert _accepted(dsn, installation) == {k1, k2}


def test_blank_configuration_ages_every_key_out(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    k1 = _verify_key()

    with shared_app(monkeypatch, installation, **_keys_env(k1)) as client:
        first = _tick(installation, {k1})
        _, dsn = _reader(SharedStore(client, installation, cleanup))
    before, _ = _state(installation)

    with shared_app(monkeypatch, installation, **_keys_env(None)):
        # The heartbeat moves; no key row is written or touched.
        _tick(installation, set(), after=first)
        after, _ = _state(installation)
        assert after == before
        # Still inside the window: k1 stays accepted until it ages out.
        assert _accepted(dsn, installation) == {k1}
        _age(installation, k1, WINDOW_S + 1)
        assert _refused(dsn, installation) == shared.STORE_UNAVAILABLE


def test_api_outage_freezes_the_last_set(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    current, previous = _verify_key(), _verify_key()

    with shared_app(monkeypatch, installation, **_keys_env(current, previous)) as client:
        _tick(installation, {current, previous})
        _, dsn = _reader(SharedStore(client, installation, cleanup))
    # Every publisher is gone and a day of wall time passes: the window is
    # relative to the newest heartbeat, so nothing expires.
    _shift_everything(installation, 86_400)
    assert _accepted(dsn, installation) == {current, previous}
    # Secondary: the relative rule still applies inside the frozen set.
    _age(installation, previous, WINDOW_S + 1)
    assert _accepted(dsn, installation) == {current}


def test_malformed_config_refuses_boot_and_leaves_rows(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    k1 = _verify_key()
    with shared_app(monkeypatch, installation, **_keys_env(k1)):
        _tick(installation, {k1})
    before = _state(installation)

    malformed = _keys_env(k1)
    malformed[CURRENT_ENV] = "not-a-base64-key!"
    with pytest.raises(ValidationError, match="caller proxy is misconfigured"):
        with shared_app(monkeypatch, installation, **malformed):
            pass
    # shared_app leaves the malformed key in the environment, and every owner
    # read builds Settings, so put the valid key back before reading the rows.
    monkeypatch.setenv(CURRENT_ENV, k1)
    get_settings.cache_clear()
    assert _state(installation) == before

    # Liveness: the same boot with the valid key starts and publishes again.
    with shared_app(monkeypatch, installation, **_keys_env(k1)):
        _tick(installation, {k1}, after=before[1])


def test_unobserved_or_blank_installation_never_writes(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    unobserved = cleanup.installation(random_installation())
    control = cleanup.installation(random_installation())
    key = _verify_key()

    with shared_app(monkeypatch, unobserved, observed="false", **_keys_env(key)):
        assert _publish_once() == "skipped"
        # A live, observed process started after it has ticked; the unobserved
        # one, started first, wrote nothing.
        with shared_app(monkeypatch, control, **_keys_env(key)):
            _tick(control, {key})
    assert _state(unobserved) == ({}, None)

    (total,) = sql_dicts(f"SELECT count(*) AS n FROM curie.{HEARTBEATS}")
    with shared_app(monkeypatch, None, **_keys_env(key)):
        assert _publish_once() == "skipped"
    (after,) = sql_dicts(f"SELECT count(*) AS n FROM curie.{HEARTBEATS}")
    assert after == total

    # Liveness: the same direct call with an observed installation publishes.
    with shared_app(monkeypatch, unobserved, **_keys_env(key)):
        assert _publish_once() == "published"
    assert set(_state(unobserved)[0]) == {key}


def test_publisher_waits_for_schema(
    isolated_migration_db: IsolatedMigrationDb,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Objection 6: the serving window still admits 0124, so the API boots there."""

    isolated_migration_db.at("0124")
    installation = random_installation()
    key = _verify_key()
    with capture_debug_logs(caplog):
        with shared_app(monkeypatch, installation, **_keys_env(key)) as client:
            assert client.get("/health").status_code == 200
            _wait(
                lambda: SCHEMA_UNAVAILABLE in captured_log_text(caplog.records),
                "the publisher to report the missing tables",
            )
    for table in CALLER_KEY_TABLES:
        assert regclass(table) is None, table

    # The same publisher loop keeps retrying, and its next tick after the
    # migration publishes with no restart.
    caplog.clear()

    async def run() -> None:
        engine = create_engine()
        task = asyncio.create_task(
            caller_keys.run_publisher(create_sessionmaker(engine), get_settings(), interval_s=0.2)
        )
        try:
            deadline = time.monotonic() + TICK_WAIT_S
            while SCHEMA_UNAVAILABLE not in captured_log_text(caplog.records):
                assert not task.done(), "the publisher stopped"
                assert time.monotonic() < deadline, "no tick reported the missing tables"
                await asyncio.sleep(0.05)
            await asyncio.to_thread(command.upgrade, alembic_config(), "head")
            deadline = time.monotonic() + TICK_WAIT_S
            while not (await asyncio.to_thread(_state, installation))[0]:
                assert not task.done(), "the publisher stopped"
                assert time.monotonic() < deadline, "no tick published after the migration"
                await asyncio.sleep(0.05)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await engine.dispose()

    monkeypatch.setenv("CURIE_INSTALLATION_ID", installation)
    monkeypatch.setenv("CURIE_INSTALLATION_ID_OBSERVED", "true")
    for name, value in _keys_env(key).items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    try:
        with capture_debug_logs(caplog):
            asyncio.run(run())
    finally:
        get_settings.cache_clear()
    published, heartbeat = _state(installation)
    assert set(published) == {key}
    assert heartbeat is not None


def test_window_spans_five_intervals() -> None:
    assert caller_keys.CALLER_KEYS_PUBLISH_INTERVAL_S == 60
    assert shared.CALLER_KEY_WINDOW_S == WINDOW_S
    assert shared.CALLER_KEY_WINDOW_S == 5 * caller_keys.CALLER_KEYS_PUBLISH_INTERVAL_S


# --- the application role's view -------------------------------------------


def test_application_role_reads_only_its_installation(
    cleanup: SharedCleanup, sessions: SessionPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    other = cleanup.installation(random_installation())
    mine, theirs = _verify_key(), _verify_key()
    with shared_app(monkeypatch, installation, **_keys_env(mine)) as client:
        _tick(installation, {mine})
        _, dsn = _reader(SharedStore(client, installation, cleanup))

    owner = sessions.owner()
    owner.execute(
        f"INSERT INTO curie.{KEYS} (installation_id, public_key) VALUES ($1, $2)", other, theirs
    )
    owner.execute(f"INSERT INTO curie.{HEARTBEATS} (installation_id) VALUES ($1)", other)

    role = sessions.open(dsn)
    assert [row[0] for row in role.fetch(f"SELECT public_key FROM curie.{KEYS}")] == [mine]
    assert [row[0] for row in role.fetch(f"SELECT installation_id FROM curie.{HEARTBEATS}")] == [
        installation
    ]
    for table in CALLER_KEY_TABLES:
        expect_sqlstate(role, "42501", f"DELETE FROM curie.{table}")
        expect_sqlstate(role, "42501", f"UPDATE curie.{table} SET seen_at = now()")
        expect_sqlstate(role, "42501", f"TRUNCATE curie.{table}")
    expect_sqlstate(
        role,
        "42501",
        f"INSERT INTO curie.{KEYS} (installation_id, public_key) VALUES ($1, $2)",
        installation,
        _verify_key(),
    )
    expect_sqlstate(
        role, "42501", f"INSERT INTO curie.{HEARTBEATS} (installation_id) VALUES ($1)", other
    )
    role.close()

    # Through the proxy's read: its own installation, never the other one.
    assert _accepted(dsn, installation) == {mine}
    assert _refused(dsn, other) == shared.STORE_UNAVAILABLE


def _privileges(role: str, table: str) -> dict[str, Any]:
    (row,) = sql_dicts(
        "SELECT has_table_privilege(CAST(:role AS name), CAST(:table AS text), 'SELECT') "
        "AS can_select, has_table_privilege(CAST(:role AS name), CAST(:table AS text), "
        "'INSERT, UPDATE, DELETE, TRUNCATE') AS can_write",
        {"role": role, "table": f"curie.{table}"},
    )
    return row


def test_provisioned_role_has_select_on_both_new_tables(
    cleanup: SharedCleanup, sessions: SessionPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    installation = cleanup.installation(random_installation())
    with shared_app(monkeypatch, installation) as client:
        store = SharedStore(client, installation, cleanup)
        application_id, _ = _reader(store)
        role = store.application(application_id)["database"]["role_name"]
        for table in CALLER_KEY_TABLES:
            assert _privileges(role, table) == {"can_select": True, "can_write": False}, table
        assert _session_call(lambda session: database_roles._role_is_restricted(session, role))

        owner = sessions.owner()
        revision = 1
        for table in CALLER_KEY_TABLES:
            owner.execute(f"REVOKE SELECT ON curie.{table} FROM {quote_ident(role)}")
            assert not _session_call(
                lambda session: database_roles._role_is_restricted(session, role)
            )
            assert_refused(
                store.rotate(application_id, revision), 500, "shared_role_verification_failed"
            )
            owner.execute(f"GRANT SELECT ON curie.{table} TO {quote_ident(role)}")
            # Liveness: with the grant back the same rotation goes through.
            rotated = store.rotate(application_id, revision)
            assert rotated.status_code == 200, rotated.text
            assert rotated.json()["dsn_revision"] == revision + 1
            revision += 1


def test_managed_role_needs_select_on_both_new_tables(
    cleanup: SharedCleanup, sessions: SessionPool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Driver note 1: the resolver's pre-activation test requires both grants.

    An operator-managed role granted what the managed-database documentation
    lists starts; without SELECT on either caller verify key table it refuses
    with ``role_missing_select``.
    """

    installation = cleanup.installation(random_installation())
    role = cleanup.role(f"ops_shared_{secrets.token_hex(6)}")
    password = secrets.token_urlsafe(24)
    owner = sessions.owner()
    owner.execute(f"CREATE ROLE {quote_ident(role)} LOGIN NOINHERIT PASSWORD '{password}'")
    grantee = quote_ident(role)
    owner.execute(f"GRANT USAGE ON SCHEMA curie TO {grantee}")
    owner.execute(
        "GRANT SELECT ON TABLE "
        + ", ".join(f"curie.{table}" for table in APPLICATION_TABLES)
        + f" TO {grantee}"
    )
    owner.execute(
        "GRANT UPDATE (lock_guard) ON TABLE curie.shared_connector_applications, "
        f"curie.shared_connector_bindings TO {grantee}"
    )
    owner.execute(
        "GRANT EXECUTE ON FUNCTION "
        + ", ".join(f"curie.{signature}" for signature in FUNCTION_SIGNATURES)
        + f" TO {grantee}"
    )
    assert set(SHARED_FUNCTIONS) == {signature.split("(")[0] for signature in FUNCTION_SIGNATURES}
    with shared_app(monkeypatch, installation) as client:
        store = SharedStore(client, installation, cleanup)
        application_id = store.register_application(dsn_encoding="managed-raw")["application_id"]
        provisioned = store.provision(application_id, {"mode": "managed", "role_name": role})
        assert provisioned.status_code == 201, provisioned.text
    dsn = render_url(
        make_url(get_settings().database_url).set(
            drivername="postgresql", username=role, password=password
        )
    )

    async def verify() -> str:
        connection = await asyncpg.connect(dsn)
        try:
            return str(await privileges.verify_role(connection, application_id))
        finally:
            await connection.close()

    def refusal() -> str:
        with pytest.raises(privileges.PrivilegeRefusal) as refused:
            asyncio.run(verify())
        return str(refused.value)

    # Granted only the six 0123 tables, as before cp5: refused.
    assert refusal() == "role_missing_select"
    owner.execute(f"GRANT SELECT ON curie.{KEYS} TO {grantee}")
    assert refusal() == "role_missing_select"
    owner.execute(f"GRANT SELECT ON curie.{HEARTBEATS} TO {grantee}")
    # Liveness: with both grants the role starts.
    assert asyncio.run(verify()) == role
    owner.execute(f"REVOKE SELECT ON curie.{KEYS} FROM {grantee}")
    assert refusal() == "role_missing_select"


def test_provisioning_below_0125_answers_store_unavailable(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Driver note 2: a database inside the window but below 0125 is a named refusal.

    Provisioning grants, and rotation verifies, SELECT on the caller verify key
    tables; with the tables absent both answer 503 ``shared_store_unavailable``
    and change nothing, never an unhandled error.
    """

    isolated_migration_db.at("head")
    cleanup = SharedCleanup()
    installation = cleanup.installation(random_installation())
    try:
        with shared_app(monkeypatch, installation) as client:
            store = SharedStore(client, installation, cleanup)
            provisioned, _ = _reader(store)
            pending = store.register_application()["application_id"]
            store.register_recipient(pending)
        command.downgrade(alembic_config(), "0124")
        for table in CALLER_KEY_TABLES:
            assert regclass(table) is None, table
        pending_role = cleanup.role(role_name_for(installation, pending))

        with shared_app(monkeypatch, installation) as client:
            store = SharedStore(client, installation, cleanup)
            assert_refused(
                store.provision(pending, {"mode": "bundled"}), 503, "shared_store_unavailable"
            )
            assert not role_exists(pending_role)
            assert database_role_rows(pending) == []
            assert_refused(store.rotate(provisioned, 1), 503, "shared_store_unavailable")
            (row,) = database_role_rows(provisioned)
            assert row["dsn_revision"] == 1

        command.upgrade(alembic_config(), "head")
        with shared_app(monkeypatch, installation) as client:
            store = SharedStore(client, installation, cleanup)
            # Liveness: at 0125 the same provisioning succeeds.
            created = store.provision(pending, {"mode": "bundled"})
            assert created.status_code == 201, created.text
            assert role_exists(pending_role)
    finally:
        cleanup.run()
