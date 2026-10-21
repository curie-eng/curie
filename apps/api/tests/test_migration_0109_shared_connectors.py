"""Migration 0109 adds the shared connector store (ADR 0239, AC1).

Expand only, on 0108. The upgrade creates six tables in ``curie`` with row
level security enabled (not forced, so the owning API role is unaffected), the
four SECURITY DEFINER refresh and reseal functions with no PUBLIC execute, and
triggers that refuse changes to pinned columns even from the owner. The
downgrade revokes each bundled role's schema grant and drops the role after the
tables and functions that held its other grants, without ``DROP OWNED BY``
(plan decision 19), so it also runs as a non-superuser owner that created those
roles with CREATEROLE. Managed role names belong to the operator and survive.
"""

from __future__ import annotations

import asyncio
import json
import secrets
import uuid
from collections.abc import Iterator
from pathlib import Path

import asyncpg
import pytest
from _migration_support import (
    IsolatedMigrationDb,
    admin_execute,
    alembic_config,
    render_url,
    sql_dicts,
)
from _shared_connector_support import (
    OTHER_SERVER_IMAGE,
    SERVER_IMAGE,
    SHARED_FUNCTIONS,
    SHARED_TABLES,
    SLOT,
    PgSession,
    Recipient,
    SessionPool,
    SharedCleanup,
    SharedStore,
    application_spec,
    client_fingerprint,
    connect_as,
    quote_ident,
    random_installation,
    regclass,
    role_exists,
    role_name_for,
    shared_app,
    shared_function_names,
    shared_policy,
    source_fingerprint,
)
from alembic import command
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy.engine import make_url

REVISION_KINDS = Path(__file__).resolve().parents[1] / "src/curie_api/revision_kinds.json"
POLICIES = {
    "shared_connector_applications": {"SELECT", "UPDATE"},
    "shared_connector_bindings": {"SELECT", "UPDATE"},
    "shared_connector_recipients": {"SELECT"},
    "shared_connector_database_roles": {"SELECT"},
    "shared_connector_memberships": {"SELECT"},
    "shared_connector_refresh_intents": {"SELECT"},
}
NAMED_CONSTRAINTS = {
    "shared_connector_applications_pkey",
    "shared_connector_applications_custody_key",
    "shared_connector_applications_installation_key",
}


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


def _store_absent() -> None:
    for table in SHARED_TABLES:
        assert regclass(table) is None, table
    assert shared_function_names() == set()


def _store_present() -> None:
    for table in SHARED_TABLES:
        assert regclass(table) is not None, table
    assert set(SHARED_FUNCTIONS) <= shared_function_names()


def test_0109_is_an_expand_revision_on_0108() -> None:
    revision = ScriptDirectory.from_config(alembic_config()).get_revision("0109")
    assert revision is not None
    assert revision.down_revision == "0108"
    assert json.loads(REVISION_KINDS.read_text())["0109"] == "expand"


def test_upgrade_creates_the_store_with_row_level_security_policies_and_named_keys(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0108")
    _store_absent()
    command.upgrade(alembic_config(), "0109")
    _store_present()

    rows = sql_dicts(
        "SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'curie' AND c.relname = ANY(:tables)",
        {"tables": list(SHARED_TABLES)},
    )
    assert {
        row["relname"]: (row["relrowsecurity"], row["relforcerowsecurity"]) for row in rows
    } == {table: (True, False) for table in SHARED_TABLES}
    policies: dict[str, set[str]] = {}
    for row in sql_dicts(
        "SELECT tablename, cmd FROM pg_policies "
        "WHERE schemaname = 'curie' AND tablename = ANY(:tables)",
        {"tables": list(SHARED_TABLES)},
    ):
        policies.setdefault(row["tablename"], set()).add(row["cmd"])
    assert policies == POLICIES

    constraints = {
        row["conname"]
        for row in sql_dicts(
            "SELECT conname FROM pg_constraint WHERE conrelid = to_regclass(:table)",
            {"table": "curie.shared_connector_applications"},
        )
    }
    assert NAMED_CONSTRAINTS <= constraints
    (index,) = sql_dicts(
        "SELECT indexdef FROM pg_indexes WHERE schemaname = 'curie' AND indexname = :name",
        {"name": "shared_connector_bindings_enabled_agent_key"},
    )
    assert "UNIQUE" in index["indexdef"]
    assert "application_id" in index["indexdef"] and "agent_id" in index["indexdef"]
    assert "WHERE enabled" in index["indexdef"]


def test_store_functions_are_security_definer_with_a_pinned_search_path_and_no_public_execute(
    migrated: None,
) -> None:
    rows = sql_dicts(
        "SELECT p.proname, p.prosecdef, p.proconfig, EXISTS ("
        "  SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl"
        "  WHERE acl.grantee = 0 AND acl.privilege_type = 'EXECUTE'"
        ") AS public_execute "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
        "WHERE n.nspname = 'curie' AND p.proname = ANY(:names)",
        {"names": list(SHARED_FUNCTIONS)},
    )
    by_name = {row["proname"]: row for row in rows}
    assert set(by_name) == set(SHARED_FUNCTIONS)
    for name, row in by_name.items():
        assert row["prosecdef"] is True, name
        assert row["public_execute"] is False, name
        settings = [entry.replace(" ", "") for entry in row["proconfig"] or []]
        assert "search_path=pg_catalog,pg_temp" in settings, (name, row["proconfig"])


def test_owner_updates_to_pinned_columns_are_refused_and_allowed_changes_pass(
    sessions: SessionPool,
) -> None:
    owner = sessions.owner()
    owner.execute("BEGIN")
    try:
        _exercise_pinned_columns(owner)
    finally:
        owner.execute("ROLLBACK")


def _refused(owner: PgSession, statement: str, *args: object) -> asyncpg.PostgresError:
    owner.execute("SAVEPOINT pinned")
    try:
        owner.execute(statement, *args)
    except asyncpg.PostgresError as error:
        owner.execute("ROLLBACK TO SAVEPOINT pinned")
        return error
    owner.execute("ROLLBACK TO SAVEPOINT pinned")
    raise AssertionError(f"the owner was allowed to run {statement!r}")


def _exercise_pinned_columns(owner: PgSession) -> None:
    agents = [uuid.uuid4() for _ in range(3)]
    for agent_id in agents:
        owner.execute(
            "INSERT INTO curie.agents (id, name) VALUES ($1, $2)",
            agent_id,
            f"mig-0109-{agent_id.hex[:12]}",
        )
    application = uuid.uuid4()
    installation = random_installation()
    owner.execute(
        "INSERT INTO curie.shared_connector_applications (id, installation_id, spec, "
        "server_image, credential_slots, provider_origins, snapshot_seal, policy, pin_digest, "
        "custody) VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6::jsonb, NULL, $7::jsonb, $8, "
        "'platform')",
        application,
        installation,
        json.dumps(application_spec(installation, str(application))),
        SERVER_IMAGE,
        json.dumps([SLOT]),
        json.dumps(["https://api.example.com"]),
        json.dumps(shared_policy()),
        "0" * 64,
    )
    current, spare = Recipient.generate(), Recipient.generate()
    for recipient in (current, spare):
        owner.execute(
            "INSERT INTO curie.shared_connector_recipients "
            "(application_id, recipient_id, algorithm, public_key) "
            "VALUES ($1, $2, 'x25519-sealed-box', $3)",
            application,
            recipient.recipient_id,
            recipient.public_bytes,
        )
    held, free = uuid.uuid4(), uuid.uuid4()
    for binding_id, agent_id in ((held, agents[0]), (free, agents[1])):
        owner.execute(
            "INSERT INTO curie.shared_connector_bindings (id, application_id, agent_id, "
            "provider_account, custody, credential_revision, recipient_id, ciphertext, "
            "source_kind, source_namespace, source_name, source_uid, source_resource_version, "
            "source_fingerprint) VALUES ($1, $2, $3, $4, 'platform', 1, $5, $6, "
            "'kubernetes-secret', 'acme', 'acme-secret', 'uid-1', '100', $7)",
            binding_id,
            application,
            agent_id,
            f"acct-{binding_id.hex[:8]}",
            current.recipient_id,
            current.seal(b'{"access_token": "pinned"}'),
            source_fingerprint("pinned"),
        )
    owner.execute(
        "INSERT INTO curie.shared_connector_refresh_intents (installation_id, grant_id, "
        "application_id, binding_id, client_config_fingerprint) VALUES ($1, $2, $3, $4, $5)",
        installation,
        "grant:pinned",
        application,
        held,
        client_fingerprint("pinned"),
    )
    owner.execute(
        "INSERT INTO curie.shared_connector_database_roles (application_id, role_name, mode, "
        "status, sealed_dsn, dsn_recipient_id) VALUES ($1, $2, 'bundled', 'provisioned', $3, $4)",
        application,
        f"curie_sca_{secrets.token_hex(20)}",
        current.seal(b"postgresql://pinned"),
        current.recipient_id,
    )

    applications = "UPDATE curie.shared_connector_applications SET {} WHERE id = $1"
    for assignment, args in (
        ("id = $2", (uuid.uuid4(),)),
        ("installation_id = $2", ("another-installation",)),
        ("spec = '{}'::jsonb", ()),
        ("server_image = $2", (OTHER_SERVER_IMAGE,)),
        ("credential_slots = '[]'::jsonb", ()),
        ("provider_origins = '[]'::jsonb", ()),
        ('snapshot_seal = \'{"name": "acme-snapshot", "key": "seal"}\'::jsonb', ()),
        ("policy = '{}'::jsonb", ()),
        ("pin_digest = $2", ("f" * 64,)),
        ("custody = 'connector'", ()),
        ("created_at = created_at - interval '1 day'", ()),
    ):
        error = _refused(owner, applications.format(assignment), application, *args)
        assert error.sqlstate == "23514", (assignment, error.sqlstate, str(error))
        assert str(error).startswith("shared_connector_immutable"), (assignment, str(error))
    for allowed in (
        "lock_guard = lock_guard",
        "updated_at = now()",
        "policy_generation = policy_generation + 3",
        "enabled = false, disabled_at = now()",
    ):
        assert owner.execute(applications.format(allowed), application) == "UPDATE 1", allowed
    for refused in (
        "policy_generation = policy_generation - 1",
        "enabled = true, disabled_at = NULL",
    ):
        error = _refused(owner, applications.format(refused), application)
        assert error.sqlstate == "23514", (refused, str(error))
        assert str(error).startswith("shared_connector_immutable"), (refused, str(error))

    bindings = "UPDATE curie.shared_connector_bindings SET {} WHERE id = $1"
    for assignment, args in (
        ("id = $2", (uuid.uuid4(),)),
        ("agent_id = $2", (agents[2],)),
        ("provider_account = 'acct-renamed'", ()),
        ("created_at = created_at - interval '1 day'", ()),
    ):
        _refused(owner, bindings.format(assignment), free, *args)
    assert owner.execute(bindings.format("credential_revision = 3"), free) == "UPDATE 1"
    _refused(owner, bindings.format("credential_revision = 2"), free)
    assert owner.execute(bindings.format("recipient_id = $2"), free, spare.recipient_id) == (
        "UPDATE 1"
    )
    assert owner.execute(bindings.format("enabled = false, disabled_at = now()"), free) == (
        "UPDATE 1"
    )
    _refused(owner, bindings.format("enabled = true, disabled_at = NULL"), free)

    recipients = (
        "UPDATE curie.shared_connector_recipients SET {} "
        "WHERE application_id = $1 AND recipient_id = $2"
    )
    loose = Recipient.generate()
    owner.execute(
        "INSERT INTO curie.shared_connector_recipients "
        "(application_id, recipient_id, algorithm, public_key) "
        "VALUES ($1, $2, 'x25519-sealed-box', $3)",
        application,
        loose.recipient_id,
        loose.public_bytes,
    )
    for assignment, args in (
        ("public_key = $3", (Recipient.generate().public_bytes,)),
        ("recipient_id = $3", (Recipient.generate().recipient_id,)),
        ("created_at = created_at - interval '1 day'", ()),
    ):
        _refused(owner, recipients.format(assignment), application, loose.recipient_id, *args)
    assert (
        owner.execute(recipients.format("retired_at = now()"), application, loose.recipient_id)
        == "UPDATE 1"
    )
    for assignment in ("retired_at = NULL", "retired_at = retired_at - interval '1 day'"):
        _refused(owner, recipients.format(assignment), application, loose.recipient_id)
    _refused(owner, bindings.format("recipient_id = $2"), held, loose.recipient_id)
    _refused(
        owner,
        "UPDATE curie.shared_connector_database_roles SET dsn_recipient_id = $2 "
        "WHERE application_id = $1",
        application,
        loose.recipient_id,
    )

    roles = "UPDATE curie.shared_connector_database_roles SET {} WHERE application_id = $1"
    for assignment in (
        "role_name = 'curie_sca_renamed'",
        "created_at = created_at - interval '1 day'",
    ):
        _refused(owner, roles.format(assignment), application)
    assert owner.execute(roles.format("dsn_revision = 3, status = 'ready'"), application) == (
        "UPDATE 1"
    )
    _refused(owner, roles.format("dsn_revision = 2"), application)

    intents = "UPDATE curie.shared_connector_refresh_intents SET {} WHERE binding_id = $1"
    for assignment, args in (
        ("grant_id = 'grant:renamed'", ()),
        ("client_config_fingerprint = $2", (client_fingerprint("renamed"),)),
        ("binding_id = $2", (free,)),
        ("created_at = created_at - interval '1 day'", ()),
    ):
        _refused(owner, intents.format(assignment), held, *args)
    assert owner.execute(intents.format("epoch = epoch + 3"), held) == "UPDATE 1"
    _refused(owner, intents.format("epoch = epoch - 1"), held)

    generation = "SELECT policy_generation FROM curie.shared_connector_applications WHERE id = $1"
    before = owner.fetchval(generation, application)
    owner.execute(
        "INSERT INTO curie.shared_connector_memberships (application_id, agent_id, alias) "
        "VALUES ($1, $2, 'primary'), ($1, $2, 'secondary'), ($1, $3, 'primary')",
        application,
        agents[0],
        agents[1],
    )
    assert owner.fetchval(generation, application) == before + 1
    owner.execute(
        "UPDATE curie.shared_connector_memberships SET tools = '[\"issues.read\"]'::jsonb "
        "WHERE application_id = $1",
        application,
    )
    assert owner.fetchval(generation, application) == before + 2
    owner.execute(
        "DELETE FROM curie.shared_connector_memberships WHERE application_id = $1", application
    )
    assert owner.fetchval(generation, application) == before + 3


def test_downgrade_drops_the_store_and_its_bundled_roles_and_upgrade_restores_it(
    isolated_migration_db: IsolatedMigrationDb,
    cleanup: SharedCleanup,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    isolated_migration_db.at("head")
    installation = random_installation()
    operator_role = cleanup.role(f"ops_shared_{secrets.token_hex(6)}")
    sql_dicts(f"CREATE ROLE {quote_ident(operator_role)} NOLOGIN")
    with shared_app(monkeypatch, installation) as client:
        store = SharedStore(client, installation, cleanup)
        bundled = store.register_application()["application_id"]
        recipient = store.register_recipient(bundled)
        record, dsn = store.provision_bundled(bundled, recipient)
        managed = store.register_application(dsn_encoding="managed-raw")["application_id"]
        provisioned = store.provision(managed, {"mode": "managed", "role_name": operator_role})
        assert provisioned.status_code == 201, provisioned.text
    role = record["role_name"]
    assert role == role_name_for(installation, bundled)
    assert connect_as(dsn) == role

    config = alembic_config()
    try:
        command.downgrade(config, "0108")
        _store_absent()
        assert not role_exists(role)
        assert role_exists(operator_role)
        assert regclass("agents") is not None
    finally:
        command.upgrade(config, "head")
    _store_present()
    assert not role_exists(role)


def test_a_non_superuser_owner_upgrades_provisions_and_downgrades(
    cleanup: SharedCleanup, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Plan decision 19: the whole round trip runs as a CREATEROLE, CREATEDB owner."""

    base = make_url(get_settings().database_url)
    suffix = secrets.token_hex(6)
    owner = cleanup.role(f"sca_owner_{suffix}")
    database = cleanup.database(f"curie_test_sca_owner_{suffix}")
    password = secrets.token_urlsafe(24)
    asyncio.run(
        admin_execute(
            base,
            f"CREATE ROLE {quote_ident(owner)} LOGIN NOSUPERUSER CREATEROLE CREATEDB "
            f"PASSWORD '{password}'",
            f"CREATE DATABASE {quote_ident(database)} OWNER {quote_ident(owner)}",
        )
    )
    installation = random_installation()
    application_id = str(uuid.uuid4())
    bundled = cleanup.role(role_name_for(installation, application_id), database=database)

    owner_url = render_url(base.set(username=owner, password=password, database=database))
    monkeypatch.setenv("DATABASE_URL", owner_url)
    get_settings.cache_clear()
    try:
        config = alembic_config()
        command.upgrade(config, "head")
        (identity,) = sql_dicts(
            "SELECT current_user AS login, rolsuper, rolcreaterole, rolcreatedb "
            "FROM pg_roles WHERE rolname = current_user"
        )
        assert identity == {
            "login": owner,
            "rolsuper": False,
            "rolcreaterole": True,
            "rolcreatedb": True,
        }
        _store_present()

        with shared_app(monkeypatch, installation) as client:
            store = SharedStore(client, installation, cleanup)
            store.register_application(application_id)
            recipient = store.register_recipient(application_id)
            record, dsn = store.provision_bundled(application_id, recipient)
        assert record["role_name"] == bundled
        assert role_exists(bundled)
        assert connect_as(dsn) == bundled

        command.downgrade(config, "0108")
        _store_absent()
        assert not role_exists(bundled)

        command.upgrade(config, "head")
        _store_present()
    finally:
        monkeypatch.undo()
        get_settings.cache_clear()
