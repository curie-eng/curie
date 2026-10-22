"""The resolver tests its own database role before it activates (ADR 0239).

"Custody and least privilege" 3 and 4: every application has a distinct
non-superuser role with role-derived row access, SELECT on the six shared
tables, UPDATE of the non-authority ``lock_guard`` column alone on application
and binding rows, and EXECUTE on the four refresh and reseal functions. A
managed role is created by an operator, and a bundled one can be altered after
the API verified it, so the resolver reads its actual privileges back from the
catalog before it binds its socket and refuses to start on anything wider or
narrower.

This mirrors ``curie_api.shared_connectors.database_roles._role_is_restricted``
(the effective-grant model the API checks at provisioning) with one difference:
it names the first failed check as a code, because the operator needs to know
what to fix. The connection limit and the INHERIT flag are not checked here; a
managed role may differ there harmlessly, and membership itself is refused.

Catalog reads only, all as ``current_user``: ``pg_roles``, ``pg_auth_members``
and the ``has_*_privilege`` functions over ``pg_class`` and ``pg_attribute``.
"""

from __future__ import annotations

from typing import Any

SCHEMA = "curie"
SHARED_TABLES = (
    "shared_connector_applications",
    "shared_connector_recipients",
    "shared_connector_database_roles",
    "shared_connector_memberships",
    "shared_connector_bindings",
    "shared_connector_refresh_intents",
)
LOCK_GUARD_TABLES = ("shared_connector_applications", "shared_connector_bindings")
SHARED_FUNCTIONS = (
    "shared_connector_refresh_begin(text, text, integer)",
    "shared_connector_refresh_commit(text, uuid, bigint, text, bytea)",
    "shared_connector_refresh_mark_recovery(text, uuid, bigint)",
    "shared_connector_reseal(uuid, bigint, text, text, bytea)",
)

# The order the codes are checked in, which decides the code a role that fails
# several checks is refused with (the owner DSN fails the flags first).
_FLAG_CODES = (
    ("rolsuper", "role_superuser"),
    ("rolcreaterole", "role_createrole"),
    ("rolcreatedb", "role_createdb"),
    ("rolreplication", "role_replication"),
    ("rolbypassrls", "role_bypassrls"),
)

_ROLE_SQL = """
SELECT r.rolname::text AS rolname, r.rolsuper, r.rolcreaterole, r.rolcreatedb,
       r.rolreplication, r.rolbypassrls,
       EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member = r.oid) AS is_member,
       session_user::text = current_user::text AS same_session,
       has_schema_privilege(r.oid, $1, 'USAGE') AS schema_usage
FROM pg_catalog.pg_roles r
WHERE r.rolname = current_user
"""

_TABLES_SQL = """
SELECT c.relname::text AS relname,
       has_table_privilege(r.oid, c.oid, 'SELECT') AS can_select,
       has_table_privilege(r.oid, c.oid,
           'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') AS can_write,
       has_any_column_privilege(r.oid, c.oid, 'INSERT, REFERENCES') AS column_write
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN pg_catalog.pg_roles r
WHERE r.rolname = current_user AND n.nspname = $1 AND c.relname::text = ANY($2::text[])
"""

_UPDATE_COLUMNS_SQL = """
SELECT c.relname::text AS relname, a.attname::text AS attname
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN pg_catalog.pg_roles r
WHERE r.rolname = current_user AND n.nspname = $1 AND c.relname::text = ANY($2::text[])
  AND a.attnum > 0 AND NOT a.attisdropped
  AND has_column_privilege(r.oid, c.oid, a.attnum, 'UPDATE')
"""

# to_regprocedure answers NULL for a missing function, which reads as no EXECUTE.
_FUNCTIONS_SQL = """
SELECT coalesce(bool_and(coalesce(
           has_function_privilege(current_user, to_regprocedure(f)::oid, 'EXECUTE'), false)),
       false) AS can_execute
FROM unnest($1::text[]) AS f
"""

_MAPPING_SQL = """
SELECT d.application_id::text AS application_id
FROM curie.shared_connector_database_roles d
WHERE d.role_name = current_user::text
"""


class PrivilegeRefusal(Exception):
    """The role is wider or narrower than an application role. ``str`` is the code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code

    def __str__(self) -> str:
        return self.code


async def verify_role(connection: Any, application: str) -> str:
    """Refuse unless ``connection``'s role is exactly ``application``'s role; answer its name.

    ``connection`` is an asyncpg connection (the package ships no types).
    """

    role = await connection.fetchrow(_ROLE_SQL, SCHEMA)
    if role is None:
        raise PrivilegeRefusal("role_session_mismatch")
    for flag, code in _FLAG_CODES:
        if role[flag]:
            raise PrivilegeRefusal(code)
    if role["is_member"]:
        raise PrivilegeRefusal("role_inherits_membership")
    if not role["same_session"]:
        raise PrivilegeRefusal("role_session_mismatch")

    tables = await connection.fetch(_TABLES_SQL, SCHEMA, list(SHARED_TABLES))
    if (
        not role["schema_usage"]
        or {str(row["relname"]) for row in tables} != set(SHARED_TABLES)
        or not all(row["can_select"] for row in tables)
    ):
        raise PrivilegeRefusal("role_missing_select")

    mapped = await connection.fetch(_MAPPING_SQL)
    if [str(row["application_id"]) for row in mapped] != [application]:
        raise PrivilegeRefusal("role_application_mismatch")

    if any(row["can_write"] or row["column_write"] for row in tables):
        raise PrivilegeRefusal("role_write_authority")
    columns = await connection.fetch(_UPDATE_COLUMNS_SQL, SCHEMA, list(SHARED_TABLES))
    updatable = {(str(row["relname"]), str(row["attname"])) for row in columns}
    guarded = {(table, "lock_guard") for table in LOCK_GUARD_TABLES}
    if updatable - guarded:
        raise PrivilegeRefusal("role_write_authority")
    if guarded - updatable:
        raise PrivilegeRefusal("role_lock_guard_missing")

    executable = await connection.fetchval(
        _FUNCTIONS_SQL, [f"{SCHEMA}.{signature}" for signature in SHARED_FUNCTIONS]
    )
    if executable is not True:
        raise PrivilegeRefusal("role_missing_execute")
    return str(role["rolname"])
