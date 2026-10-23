"""Each shared application's restricted Postgres role (ADR 0239 "Custody" 3 and 4).

Bundled mode: the API creates one non-superuser LOGIN role per application
through its own installation connection, grants it SELECT on the six shared
tables, UPDATE of ``lock_guard`` alone on application and binding rows, and
EXECUTE on the refresh and reseal functions, verifies all of that from the
catalog in the same transaction, then seals the role's DSN to the
application's current recipient before storing or returning it. Managed mode
records an operator-owned role and the spec's own DSN Secret reference; the API
never connects as that role or reads the Secret.

The password never appears in SQL: the role gets a SCRAM-SHA-256 verifier
computed here. The DDL that carries the verifier runs on the request's own
driver connection, inside its transaction, so SQLAlchemy's statement logging
never sees it, and a DDL failure answers a fixed code without the statement or
the server's message. The plaintext DSN exists only long enough to be sealed.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import uuid
from collections.abc import Sequence
from typing import Any
from urllib.parse import quote, urlencode

import asyncpg  # type: ignore[import-untyped]
from fastapi import status
from sqlalchemy import func, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..db import SCHEMA
from ..models import SharedConnectorApplication, SharedConnectorDatabaseRole
from ..schemas.shared_connectors import (
    SharedBundledDatabaseOut,
    SharedDatabaseProvision,
    SharedDatabaseStatusOut,
    SharedDatabaseStatusReport,
    SharedManagedDatabaseOut,
)
from . import sealing
from .store import (
    SharedRefusal,
    current_recipient,
    database_role,
    guarded_write,
    lock_application,
)

SHARED_TABLES = (
    "shared_connector_applications",
    "shared_connector_recipients",
    "shared_connector_database_roles",
    "shared_connector_memberships",
    "shared_connector_bindings",
    "shared_connector_refresh_intents",
)
# The only column an application role may update: a non-authority column that
# lets FOR SHARE and FOR KEY SHARE lock the role's own rows.
LOCK_GUARD_TABLES = ("shared_connector_applications", "shared_connector_bindings")
SHARED_FUNCTIONS = (
    "shared_connector_refresh_begin(text, text, integer)",
    "shared_connector_refresh_commit(text, uuid, bigint, text, bytea)",
    "shared_connector_refresh_mark_recovery(text, uuid, bigint)",
    "shared_connector_reseal(uuid, bigint, text, text, bytea)",
)
CONNECTION_LIMIT = 32
SCRAM_ITERATIONS = 4096
# The libpq SSL modes a DSN may carry (plan decision 18). asyncpg's ``ssl``
# query parameter is rewritten to ``sslmode``; every other parameter is dropped.
_SSL_MODES = frozenset({"disable", "allow", "prefer", "require", "verify-ca", "verify-full"})


def role_name(installation_id: str, application_id: uuid.UUID) -> str:
    """A bounded hash of the exact installation and application: 50 bytes."""

    identity = json.dumps([installation_id, str(application_id)], separators=(",", ":"))
    return "curie_sca_" + hashlib.sha256(identity.encode()).hexdigest()[:40]


def scram_verifier(password: str) -> str:
    """The SCRAM-SHA-256 verifier Postgres stores (RFC 5802, RFC 7677)."""

    salt = secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, SCRAM_ITERATIONS)
    client_key = hmac.new(salted, b"Client Key", hashlib.sha256).digest()
    stored_key = hashlib.sha256(client_key).digest()
    server_key = hmac.new(salted, b"Server Key", hashlib.sha256).digest()
    encoded = sealing.b64encode
    return (
        f"SCRAM-SHA-256${SCRAM_ITERATIONS}:{encoded(salt)}"
        f"${encoded(stored_key)}:{encoded(server_key)}"
    )


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _ssl_mode(query: Any) -> str | None:
    for name in ("sslmode", "ssl"):
        value = query.get(name)
        if isinstance(value, str) and value in _SSL_MODES:
            return value
    return None


def _role_dsn(role: str, password: str) -> str:
    """The role's DSN: the API's host, port and database, and nothing else of its URL."""

    url = make_url(get_settings().database_url)
    host = url.host or ""
    if ":" in host:
        host = f"[{host}]"
    port = "" if url.port is None else f":{url.port}"
    dsn = (
        f"postgresql://{quote(role, safe='')}:{quote(password, safe='')}"
        f"@{host}{port}/{quote(url.database or '', safe='')}"
    )
    mode = _ssl_mode(url.query)
    return dsn if mode is None else f"{dsn}?{urlencode({'sslmode': mode})}"


async def _require_role_admin(session: AsyncSession) -> None:
    """Refuse before any shared row is read when this connection cannot create roles."""

    row = (
        await session.execute(
            text("SELECT rolsuper, rolcreaterole FROM pg_roles WHERE rolname = current_user")
        )
    ).one_or_none()
    if row is None or not (row.rolsuper or row.rolcreaterole):
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_role_admin_required")


async def _run_ddl(session: AsyncSession, statements: Sequence[str]) -> None:
    """Run role DDL on the request transaction's own driver connection.

    Bypassing SQLAlchemy keeps the verifier out of its statement log and its
    error text. The transaction is already open (the role-admin check ran
    first), so a later refusal rolls the role back with everything else.
    """

    connection = await session.connection()
    raw = await connection.get_raw_connection()
    driver = raw.driver_connection
    if driver is None or not driver.is_in_transaction():
        raise RuntimeError("shared role DDL must run inside the request transaction")
    failure: str | None = None
    try:
        for statement in statements:
            await driver.execute(statement)
    except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
        failure = getattr(error, "sqlstate", None) or "unknown"
    if failure == "42501":
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_role_admin_required")
    if failure == "42710":
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_database_role_conflict")
    if failure is not None:
        raise SharedRefusal(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "shared_role_verification_failed"
        )


def _grants(role: str) -> list[str]:
    grantee = _identifier(role)
    schema = _identifier(SCHEMA)
    tables = ", ".join(f"{schema}.{_identifier(table)}" for table in SHARED_TABLES)
    guarded = ", ".join(f"{schema}.{_identifier(table)}" for table in LOCK_GUARD_TABLES)
    functions = ", ".join(f"{schema}.{signature}" for signature in SHARED_FUNCTIONS)
    return [
        f"GRANT USAGE ON SCHEMA {schema} TO {grantee}",
        f"GRANT SELECT ON TABLE {tables} TO {grantee}",
        f"GRANT UPDATE (lock_guard) ON TABLE {guarded} TO {grantee}",
        f"GRANT EXECUTE ON FUNCTION {functions} TO {grantee}",
    ]


async def _role_is_restricted(session: AsyncSession, role: str) -> bool:
    """Read the role's restrictions back from the catalog.

    A CREATEROLE creator that lacks a grant option gets a warning, not an
    error, from GRANT; this is what turns that into a refusal. PG16 makes the
    creator an ADMIN member of the new role, so the check is that the new role
    is a member of nothing, not that nobody is a member of it.
    """

    flags = (
        await session.execute(
            text(
                "SELECT r.rolsuper, r.rolcreatedb, r.rolcreaterole, r.rolinherit, "
                "r.rolreplication, r.rolbypassrls, r.rolcanlogin, r.rolconnlimit, "
                "EXISTS (SELECT 1 FROM pg_auth_members m WHERE m.member = r.oid) AS is_member "
                "FROM pg_roles r WHERE r.rolname = :role"
            ),
            {"role": role},
        )
    ).one_or_none()
    if (
        flags is None
        or not flags.rolcanlogin
        or flags.rolconnlimit != CONNECTION_LIMIT
        or any(
            (
                flags.rolsuper,
                flags.rolcreatedb,
                flags.rolcreaterole,
                flags.rolinherit,
                flags.rolreplication,
                flags.rolbypassrls,
                flags.is_member,
            )
        )
    ):
        return False

    scope = {"role": role, "schema": SCHEMA, "tables": list(SHARED_TABLES)}
    tables = (
        await session.execute(
            text(
                "SELECT c.relname::text AS relname, "
                "has_table_privilege(r.oid, c.oid, 'SELECT') AS can_select, "
                "has_table_privilege(r.oid, c.oid, "
                "'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') AS can_write, "
                "has_any_column_privilege(r.oid, c.oid, 'INSERT, REFERENCES') AS column_write "
                "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "CROSS JOIN pg_roles r "
                "WHERE r.rolname = :role AND n.nspname = :schema "
                "AND c.relname::text = ANY(CAST(:tables AS text[]))"
            ),
            scope,
        )
    ).all()
    if {row.relname for row in tables} != set(SHARED_TABLES) or any(
        not row.can_select or row.can_write or row.column_write for row in tables
    ):
        return False

    columns = (
        await session.execute(
            text(
                "SELECT c.relname::text AS relname, a.attname::text AS attname "
                "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace CROSS JOIN pg_roles r "
                "WHERE r.rolname = :role AND n.nspname = :schema "
                "AND c.relname::text = ANY(CAST(:tables AS text[])) "
                "AND a.attnum > 0 AND NOT a.attisdropped "
                "AND has_column_privilege(r.oid, c.oid, a.attnum, 'UPDATE')"
            ),
            scope,
        )
    ).all()
    if {(row.relname, row.attname) for row in columns} != {
        (table, "lock_guard") for table in LOCK_GUARD_TABLES
    }:
        return False

    executable = await session.scalar(
        text(
            "SELECT bool_and(has_function_privilege(r.oid, "
            "CAST(CAST(f AS regprocedure) AS oid), 'EXECUTE')) "
            "FROM pg_roles r CROSS JOIN unnest(CAST(:functions AS text[])) AS f "
            "WHERE r.rolname = :role"
        ),
        {
            "role": role,
            "functions": [f"{SCHEMA}.{signature}" for signature in SHARED_FUNCTIONS],
        },
    )
    return bool(executable)


async def _verify_role(session: AsyncSession, role: str) -> None:
    if not await _role_is_restricted(session, role):
        raise SharedRefusal(
            status.HTTP_500_INTERNAL_SERVER_ERROR, "shared_role_verification_failed"
        )


async def _provisionable(
    session: AsyncSession, application_id: uuid.UUID, dsn_encoding: str
) -> SharedConnectorApplication:
    # NO KEY UPDATE serializes two provisionings of one application, so the
    # second sees the first's row instead of racing it to CREATE ROLE.
    application = await lock_application(session, application_id, "no_key_update")
    if not application.enabled:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_application_disabled")
    if application.spec.get("dsn_encoding") != dsn_encoding:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_database_mode_mismatch")
    if await database_role(session, application_id) is not None:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_database_already_provisioned")
    return application


def _bundled_out(
    application_id: uuid.UUID, role: str, recipient_id: str, revision: int, sealed: bytes
) -> SharedBundledDatabaseOut:
    return SharedBundledDatabaseOut(
        application_id=application_id,
        mode="bundled",
        role_name=role,
        status="provisioned",
        recipient_id=recipient_id,
        dsn_revision=revision,
        sealed_dsn=sealing.b64encode(sealed),
    )


async def provision_database(
    session: AsyncSession, application_id: uuid.UUID, body: SharedDatabaseProvision
) -> SharedBundledDatabaseOut | SharedManagedDatabaseOut:
    if body.mode == "bundled":
        return await _provision_bundled(session, application_id)
    # The body validator requires a managed request to name its role.
    assert body.role_name is not None
    return await _provision_managed(session, application_id, body.role_name)


async def _provision_bundled(
    session: AsyncSession, application_id: uuid.UUID
) -> SharedBundledDatabaseOut:
    await _require_role_admin(session)
    application = await _provisionable(session, application_id, "bundled-sealed")
    recipient = await current_recipient(session, application_id, "share")
    if recipient is None:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_required")
    role = role_name(application.installation_id, application_id)
    password = secrets.token_urlsafe(32)
    await _run_ddl(
        session,
        [
            f"CREATE ROLE {_identifier(role)} WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE "
            f"NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT {CONNECTION_LIMIT} "
            f"PASSWORD {_literal(scram_verifier(password))}",
            *_grants(role),
        ],
    )
    await _verify_role(session, role)
    sealed = sealing.seal(bytes(recipient.public_key), _role_dsn(role, password).encode())
    await guarded_write(
        session,
        insert(SharedConnectorDatabaseRole).values(
            application_id=application_id,
            role_name=role,
            mode="bundled",
            status="provisioned",
            sealed_dsn=sealed,
            dsn_recipient_id=recipient.recipient_id,
            dsn_revision=1,
        ),
        unique_conflict=(status.HTTP_409_CONFLICT, "shared_database_already_provisioned"),
    )
    await session.commit()
    return _bundled_out(application_id, role, recipient.recipient_id, 1, sealed)


async def _provision_managed(
    session: AsyncSession, application_id: uuid.UUID, managed_role: str
) -> SharedManagedDatabaseOut:
    """Record an operator's role and the spec's DSN Secret; touch neither."""

    application = await _provisionable(session, application_id, "managed-raw")
    if await session.scalar(
        select(SharedConnectorDatabaseRole.application_id).where(
            SharedConnectorDatabaseRole.role_name == managed_role
        )
    ):
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_database_role_conflict")
    secret = application.spec["database_dsn"]
    await guarded_write(
        session,
        insert(SharedConnectorDatabaseRole).values(
            application_id=application_id,
            role_name=managed_role,
            mode="managed",
            status="pending",
            dsn_revision=1,
            dsn_secret_name=secret["name"],
            dsn_secret_key=secret["key"],
        ),
        unique_conflict=(status.HTTP_409_CONFLICT, "shared_database_role_conflict"),
    )
    await session.commit()
    return SharedManagedDatabaseOut.model_validate(
        {
            "application_id": application_id,
            "mode": "managed",
            "role_name": managed_role,
            "status": "pending",
            "dsn_secret": {"name": secret["name"], "key": secret["key"]},
        }
    )


async def rotate_database(
    session: AsyncSession, application_id: uuid.UUID, expected_dsn_revision: int
) -> SharedBundledDatabaseOut:
    """CAS on the DSN revision: a new verifier and a DSN sealed to the current recipient.

    Both the reseal path that frees an old recipient and the recovery for a
    lost provisioning response (plan decision 17). The old password stops
    authenticating when this commits.
    """

    await _require_role_admin(session)
    row = await database_role(session, application_id, "update")
    if row is None:
        raise SharedRefusal(status.HTTP_404_NOT_FOUND, "shared_database_not_provisioned")
    if row.mode != "bundled":
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_database_mode_mismatch")
    if row.dsn_revision != expected_dsn_revision:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_database_revision_conflict")
    recipient = await current_recipient(session, application_id, "share")
    if recipient is None:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_recipient_required")
    role = row.role_name
    # A role altered since provisioning gets no fresh DSN.
    await _verify_role(session, role)
    password = secrets.token_urlsafe(32)
    await _run_ddl(
        session,
        [f"ALTER ROLE {_identifier(role)} WITH PASSWORD {_literal(scram_verifier(password))}"],
    )
    sealed = sealing.seal(bytes(recipient.public_key), _role_dsn(role, password).encode())
    revision = (
        await guarded_write(
            session,
            update(SharedConnectorDatabaseRole)
            .where(
                SharedConnectorDatabaseRole.application_id == application_id,
                SharedConnectorDatabaseRole.dsn_revision == expected_dsn_revision,
            )
            .values(
                sealed_dsn=sealed,
                dsn_recipient_id=recipient.recipient_id,
                dsn_revision=SharedConnectorDatabaseRole.dsn_revision + 1,
                status="provisioned",
                status_reason=None,
                updated_at=func.now(),
            )
            .returning(SharedConnectorDatabaseRole.dsn_revision)
            .execution_options(synchronize_session=False),
        )
    ).scalar_one_or_none()
    if revision is None:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_database_revision_conflict")
    await session.commit()
    return _bundled_out(application_id, role, recipient.recipient_id, int(revision), sealed)


async def sealed_dsn_for_worker(
    session: AsyncSession, application_id: uuid.UUID
) -> tuple[bytes, int, str] | None:
    """The bundled sealed DSN, its revision and recipient, for the worker's Secret.

    The one read of ``sealed_dsn`` the API makes; every other load defers it
    with ``raiseload``. Only the worker-authenticated application render calls
    this, and the worker writes the box unopened into the DSN Secret.
    """

    row = (
        await session.execute(
            select(
                SharedConnectorDatabaseRole.sealed_dsn,
                SharedConnectorDatabaseRole.dsn_revision,
                SharedConnectorDatabaseRole.dsn_recipient_id,
            ).where(
                SharedConnectorDatabaseRole.application_id == application_id,
                SharedConnectorDatabaseRole.mode == "bundled",
            )
        )
    ).one_or_none()
    if row is None or row.sealed_dsn is None or row.dsn_recipient_id is None:
        return None
    return bytes(row.sealed_dsn), int(row.dsn_revision), str(row.dsn_recipient_id)


async def report_status(
    session: AsyncSession, application_id: uuid.UUID, body: SharedDatabaseStatusReport
) -> SharedDatabaseStatusOut:
    """The worker's readiness report, fenced by the DSN revision it activated."""

    row = await database_role(session, application_id, "update")
    if row is None:
        raise SharedRefusal(status.HTTP_404_NOT_FOUND, "shared_database_not_provisioned")
    if row.dsn_revision != body.dsn_revision:
        raise SharedRefusal(status.HTTP_409_CONFLICT, "shared_database_revision_conflict")
    await session.execute(
        update(SharedConnectorDatabaseRole)
        .where(SharedConnectorDatabaseRole.application_id == application_id)
        .values(status=body.status, status_reason=body.reason, updated_at=func.now())
        .execution_options(synchronize_session=False)
    )
    await session.commit()
    return SharedDatabaseStatusOut.model_validate(
        {
            "application_id": application_id,
            "mode": row.mode,
            "status": body.status,
            "status_reason": body.reason,
            "dsn_revision": row.dsn_revision,
        }
    )
