"""Migration 0124 persists shared connector source staleness (ADR 0239, AC4).

Expand only, on 0123. The upgrade adds ``source_stale boolean NOT NULL DEFAULT
false`` to ``curie.shared_connector_bindings`` with the check
``shared_connector_bindings_source_stale_ck`` (``NOT source_stale OR custody =
'platform'``), so connector custody can never hold the flag. Existing rows read
false. The application role gets no new grant. The downgrade drops the
constraint and the column.

0124 also replaces ``shared_connector_refresh_begin`` and
``shared_connector_refresh_mark_recovery`` so every transition into
``recovery_required`` takes the binding row ``FOR NO KEY UPDATE`` and so waits
for an in-flight resolve, which holds that binding ``FOR SHARE`` (review
F-pkg-r1). ``CREATE OR REPLACE`` keeps each function's owner and EXECUTE
grants; the downgrade restores the exact 0123 definitions.
"""

from __future__ import annotations

import concurrent.futures
import json
import secrets
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from _migration_support import (
    IsolatedMigrationDb,
    alembic_config,
    column_names,
    constraint_exists,
    render_url,
    sql_dicts,
    stamped_revision,
)
from _shared_connector_support import (
    BEGIN_SQL,
    LOCK_WAIT_S,
    MARK_RECOVERY_SQL,
    SERVER_IMAGE,
    SLOT,
    STATEMENT_WAIT_S,
    PgSession,
    Recipient,
    SessionPool,
    SharedCleanup,
    application_spec,
    binding_row,
    client_fingerprint,
    owner_connect_kwargs,
    quote_ident,
    random_installation,
    shared_policy,
    source_fingerprint,
)
from alembic import command
from alembic.script import ScriptDirectory
from curie_api.config import get_settings
from sqlalchemy.engine import make_url

REVISION_KINDS = Path(__file__).resolve().parents[1] / "src/curie_api/revision_kinds.json"
TABLE = "shared_connector_bindings"
CONSTRAINT = "shared_connector_bindings_source_stale_ck"
# The two functions 0124 replaces, and nothing else it may touch.
RECOVERY_FUNCTIONS = (
    "curie.shared_connector_refresh_begin(text,text,integer)",
    "curie.shared_connector_refresh_mark_recovery(text,uuid,bigint)",
)


def _seed_platform_binding() -> str:
    """One platform binding with its application, recipient and agent, as the owner."""

    owner = PgSession(**owner_connect_kwargs())
    try:
        agent_id, application, binding_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        installation = random_installation()
        owner.execute(
            "INSERT INTO curie.agents (id, name) VALUES ($1, $2)",
            agent_id,
            f"mig-0124-{agent_id.hex[:12]}",
        )
        owner.execute(
            "INSERT INTO curie.shared_connector_applications (id, installation_id, spec, "
            "server_image, credential_slots, provider_origins, snapshot_seal, policy, "
            "pin_digest, custody) VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6::jsonb, NULL, "
            "$7::jsonb, $8, 'platform')",
            application,
            installation,
            json.dumps(application_spec(installation, str(application))),
            SERVER_IMAGE,
            json.dumps([SLOT]),
            json.dumps(["https://api.example.com"]),
            json.dumps(shared_policy()),
            "0" * 64,
        )
        recipient = Recipient.generate()
        owner.execute(
            "INSERT INTO curie.shared_connector_recipients "
            "(application_id, recipient_id, algorithm, public_key) "
            "VALUES ($1, $2, 'x25519-sealed-box', $3)",
            application,
            recipient.recipient_id,
            recipient.public_bytes,
        )
        owner.execute(
            "INSERT INTO curie.shared_connector_bindings (id, application_id, agent_id, "
            "provider_account, custody, credential_revision, recipient_id, ciphertext, "
            "source_kind, source_namespace, source_name, source_uid, source_resource_version, "
            "source_fingerprint) VALUES ($1, $2, $3, 'acct-primary', 'platform', 1, $4, $5, "
            "'kubernetes-secret', 'acme', 'acme-secret', 'uid-1', '100', $6)",
            binding_id,
            application,
            agent_id,
            recipient.recipient_id,
            recipient.seal(b'{"access_token": "migrated"}'),
            source_fingerprint("migrated"),
        )
        return str(binding_id)
    finally:
        owner.close()


def _column() -> list[dict[str, Any]]:
    return sql_dicts(
        "SELECT data_type, is_nullable, column_default FROM information_schema.columns "
        "WHERE table_schema = 'curie' AND table_name = :table AND column_name = 'source_stale'",
        {"table": TABLE},
    )


def _without_flag(row: dict[str, Any]) -> dict[str, Any]:
    return {name: value for name, value in row.items() if name != "source_stale"}


def _flag_absent() -> None:
    assert "source_stale" not in column_names(TABLE)
    assert _column() == []
    assert not constraint_exists(CONSTRAINT)


def _flag_present() -> None:
    assert "source_stale" in column_names(TABLE)
    assert _column() == [{"data_type": "boolean", "is_nullable": "NO", "column_default": "false"}]
    assert constraint_exists(CONSTRAINT)
    (definition,) = sql_dicts(
        "SELECT pg_get_constraintdef(c.oid) AS definition, c.convalidated FROM pg_constraint c "
        "WHERE c.conrelid = to_regclass(:table) AND c.conname = :name",
        {"table": f"curie.{TABLE}", "name": CONSTRAINT},
    )
    assert definition["convalidated"] is True
    assert definition["definition"].startswith("CHECK (")
    assert "source_stale" in definition["definition"]
    assert "custody = 'platform'" in definition["definition"]


def test_0124_is_an_expand_revision_on_0123() -> None:
    revision = ScriptDirectory.from_config(alembic_config()).get_revision("0124")
    assert revision is not None
    assert revision.down_revision == "0123"
    assert json.loads(REVISION_KINDS.read_text())["0124"] == "expand"


def test_upgrade_adds_the_flag_false_for_existing_rows_and_the_downgrade_removes_it(
    isolated_migration_db: IsolatedMigrationDb,
) -> None:
    isolated_migration_db.at("0123")
    _flag_absent()
    binding_id = _seed_platform_binding()
    before = binding_row(binding_id)
    assert "source_stale" not in before
    config = alembic_config()

    command.upgrade(config, "0124")
    assert stamped_revision() == "0124"
    _flag_present()
    upgraded = binding_row(binding_id)
    assert upgraded["source_stale"] is False
    assert _without_flag(upgraded) == before

    # The flag is writable for a platform binding, and the downgrade discards it.
    sql_dicts(
        "UPDATE curie.shared_connector_bindings SET source_stale = true WHERE id = :id",
        {"id": uuid.UUID(binding_id)},
    )
    assert binding_row(binding_id)["source_stale"] is True

    command.downgrade(config, "0123")
    assert stamped_revision() == "0123"
    _flag_absent()
    assert binding_row(binding_id) == before

    command.upgrade(config, "0124")
    assert stamped_revision() == "0124"
    _flag_present()
    restored = binding_row(binding_id)
    assert restored["source_stale"] is False
    assert _without_flag(restored) == before


# --- transitions into recovery take the binding lock ---------------------------


@pytest.fixture
def cleanup(isolated_migration_db: IsolatedMigrationDb) -> Iterator[SharedCleanup]:
    # Runs before the isolated database is dropped, so the role's grants go first.
    tracker = SharedCleanup()
    yield tracker
    tracker.run()


@pytest.fixture
def sessions(cleanup: SharedCleanup) -> Iterator[SessionPool]:
    pool = SessionPool()
    yield pool
    pool.close_all()


def _seed_application_role(cleanup: SharedCleanup) -> tuple[str, str, list[str], list[str]]:
    """A bundled application role with two refresh grants, granted as the API grants it.

    Answers the role, its DSN, the two grant IDs and their binding IDs.
    """

    role = cleanup.role(f"curie_sca_{secrets.token_hex(20)}")
    password = secrets.token_urlsafe(24)
    owner = PgSession(**owner_connect_kwargs())
    try:
        owner.execute(f"CREATE ROLE {quote_ident(role)} LOGIN PASSWORD '{password}'")
        owner.execute(f"GRANT USAGE ON SCHEMA curie TO {quote_ident(role)}")
        for signature in RECOVERY_FUNCTIONS:
            owner.execute(f"GRANT EXECUTE ON FUNCTION {signature} TO {quote_ident(role)}")
        application, installation = uuid.uuid4(), random_installation()
        owner.execute(
            "INSERT INTO curie.shared_connector_applications (id, installation_id, spec, "
            "server_image, credential_slots, provider_origins, snapshot_seal, policy, "
            "pin_digest, custody) VALUES ($1, $2, $3::jsonb, $4, $5::jsonb, $6::jsonb, NULL, "
            "$7::jsonb, $8, 'platform')",
            application,
            installation,
            json.dumps(application_spec(installation, str(application))),
            SERVER_IMAGE,
            json.dumps([SLOT]),
            json.dumps(["https://api.example.com"]),
            json.dumps(shared_policy()),
            "0" * 64,
        )
        recipient = Recipient.generate()
        owner.execute(
            "INSERT INTO curie.shared_connector_recipients "
            "(application_id, recipient_id, algorithm, public_key) "
            "VALUES ($1, $2, 'x25519-sealed-box', $3)",
            application,
            recipient.recipient_id,
            recipient.public_bytes,
        )
        owner.execute(
            "INSERT INTO curie.shared_connector_database_roles (application_id, role_name, "
            "mode, status, sealed_dsn, dsn_recipient_id) "
            "VALUES ($1, $2, 'bundled', 'provisioned', $3, $4)",
            application,
            role,
            recipient.seal(b"postgresql://sealed"),
            recipient.recipient_id,
        )
        grants: list[str] = []
        bindings: list[str] = []
        for index in range(2):
            agent_id, binding_id = uuid.uuid4(), uuid.uuid4()
            owner.execute(
                "INSERT INTO curie.agents (id, name) VALUES ($1, $2)",
                agent_id,
                f"mig-0124-{agent_id.hex[:12]}",
            )
            owner.execute(
                "INSERT INTO curie.shared_connector_bindings (id, application_id, agent_id, "
                "provider_account, custody, credential_revision, recipient_id, ciphertext, "
                "source_kind, source_namespace, source_name, source_uid, "
                "source_resource_version, source_fingerprint) VALUES ($1, $2, $3, $4, "
                "'platform', 1, $5, $6, 'kubernetes-secret', 'acme', 'acme-secret', 'uid-1', "
                "'100', $7)",
                binding_id,
                application,
                agent_id,
                f"acct-{index}",
                recipient.recipient_id,
                recipient.seal(b'{"access_token": "refreshable"}'),
                source_fingerprint(f"refreshable-{index}"),
            )
            grant_id = f"grant:{uuid.uuid4().hex}"
            owner.execute(
                "INSERT INTO curie.shared_connector_refresh_intents (installation_id, grant_id, "
                "application_id, binding_id, client_config_fingerprint) "
                "VALUES ($1, $2, $3, $4, $5)",
                installation,
                grant_id,
                application,
                binding_id,
                client_fingerprint(grant_id),
            )
            grants.append(grant_id)
            bindings.append(str(binding_id))
    finally:
        owner.close()
    dsn = render_url(
        make_url(get_settings().database_url).set(
            drivername="postgresql", username=role, password=password
        )
    )
    return role, dsn, grants, bindings


def _recovery_functions(role: str) -> dict[str, dict[str, Any]]:
    """Each function's definition, owner, ACL and who may execute it."""

    snapshot: dict[str, dict[str, Any]] = {}
    for signature in RECOVERY_FUNCTIONS:
        (row,) = sql_dicts(
            "SELECT pg_get_functiondef(p.oid) AS definition, "
            "pg_get_userbyid(p.proowner) AS owner, p.proacl::text[] AS acl, EXISTS ("
            "  SELECT 1 FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) acl"
            "  WHERE acl.grantee = 0 AND acl.privilege_type = 'EXECUTE'"
            ") AS public_execute, "
            "has_function_privilege(CAST(:role AS name), p.oid, 'EXECUTE') AS role_execute "
            "FROM pg_proc p WHERE p.oid = to_regprocedure(CAST(:signature AS text))",
            {"role": role, "signature": signature},
        )
        snapshot[signature] = row
    return snapshot


def _blocked_by(waiter: int, holder: int) -> bool:
    (row,) = sql_dicts("SELECT pg_blocking_pids(:pid) AS blockers", {"pid": waiter})
    return holder in row["blockers"]


def _call_under_binding_share(
    sessions: SessionPool, dsn: str, binding_id: str, sql: str, *args: Any
) -> tuple[concurrent.futures.Future[Any], int, int, PgSession]:
    """Hold ``binding_id`` FOR SHARE as the owner (as a resolve does), then submit ``sql``."""

    holder = sessions.owner()
    holder_pid = int(holder.fetchval("SELECT pg_backend_pid()"))
    holder.execute("BEGIN")
    holder.fetch(
        "SELECT id FROM curie.shared_connector_bindings WHERE id = $1 FOR SHARE",
        uuid.UUID(binding_id),
    )
    caller = sessions.open(dsn)
    caller_pid = int(caller.fetchval("SELECT pg_backend_pid()"))
    method = "fetchrow" if sql == BEGIN_SQL else "fetchval"
    return caller.submit(method, sql, *args), caller_pid, holder_pid, holder


def _waits_for_the_share_lock(
    sessions: SessionPool, dsn: str, binding_id: str, name: str, sql: str, *args: Any
) -> Any:
    """``sql`` must wait on a binding FOR SHARE, then answer once it is released."""

    pending, caller_pid, holder_pid, holder = _call_under_binding_share(
        sessions, dsn, binding_id, sql, *args
    )
    try:
        deadline = time.monotonic() + LOCK_WAIT_S
        while not _blocked_by(caller_pid, holder_pid):
            if pending.done():
                answer = pending.exception() or pending.result()
                answer = answer["outcome"] if hasattr(answer, "keys") else answer
                raise AssertionError(
                    f"{name} completed while the binding was held FOR SHARE (answered {answer!r})"
                )
            assert time.monotonic() < deadline, f"{name} neither waited nor completed"
            time.sleep(0.05)
        time.sleep(0.5)
        assert not pending.done(), f"{name} completed while the binding was held FOR SHARE"
    finally:
        holder.execute("COMMIT")
    return pending.result(timeout=STATEMENT_WAIT_S)


def test_upgrade_makes_recovery_transitions_take_the_binding_lock_and_downgrade_restores_0123(
    isolated_migration_db: IsolatedMigrationDb,
    cleanup: SharedCleanup,
    sessions: SessionPool,
) -> None:
    isolated_migration_db.at("0123")
    role, dsn, grants, bindings = _seed_application_role(cleanup)
    cp1 = _recovery_functions(role)
    for signature, row in cp1.items():
        assert row["public_execute"] is False, signature
        assert row["role_execute"] is True, signature
    config = alembic_config()

    command.upgrade(config, "0124")
    assert stamped_revision() == "0124"
    replaced = _recovery_functions(role)
    for signature, row in replaced.items():
        before = cp1[signature]
        # CREATE OR REPLACE: the same owner and the same grants, PUBLIC still without EXECUTE.
        assert (row["owner"], row["acl"]) == (before["owner"], before["acl"]), signature
        assert row["public_execute"] is False, signature
        assert row["role_execute"] is True, signature

    # A binding held FOR SHARE (as an in-flight resolve holds it) holds off a
    # begin, and a mark of the refresh that begin started.
    began = _waits_for_the_share_lock(
        sessions, dsn, bindings[0], "refresh_begin", BEGIN_SQL, grants[0], "owner-one", 60
    )
    assert began is not None and began["outcome"] == "began", began
    marked = _waits_for_the_share_lock(
        sessions,
        dsn,
        bindings[0],
        "mark_recovery",
        MARK_RECOVERY_SQL,
        grants[0],
        began["intent_id"],
        began["epoch"],
    )
    assert marked == "recovery_required"
    for signature, row in replaced.items():
        assert row["definition"] != cp1[signature]["definition"], signature

    command.downgrade(config, "0123")
    assert stamped_revision() == "0123"
    # Exactly the cp1 definitions, owner and grants, as a fresh 0123 upgrade made them.
    assert _recovery_functions(role) == cp1
    # cp1 behavior is back: a binding FOR SHARE does not hold off a begin.
    pending, _, _, holder = _call_under_binding_share(
        sessions, dsn, bindings[1], BEGIN_SQL, grants[1], "owner-two", 60
    )
    try:
        restored = pending.result(timeout=LOCK_WAIT_S)
    finally:
        holder.execute("COMMIT")
    assert restored is not None and restored["outcome"] == "began", restored
