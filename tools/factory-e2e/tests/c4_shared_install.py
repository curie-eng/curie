"""Clean-state rules for the shared kind install of the admitted agent cases.

One session install serves every case in a pytest process. A case may start
only when a snapshot of the install equals the baseline recorded after the
install became ready: nothing a previous case created survives, and nothing
the install owns is missing. This module is pure; the cluster reads live in
c4_admitted_kind.AdmittedKind.install_snapshot, and the SQL it builds runs
there through psql in the install's Postgres pod.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import Any

# Tables the install owns for its whole lifetime; they are not inventoried.
# apps/api/alembic/env.py pins the version table into the curie schema.
INSTALL_LIFETIME_TABLES = frozenset({"alembic_version"})
# Intake's per-scope poll positions. Every reset truncates them so the next
# case's intake starts from a fresh install's position, and intake rewrites
# its row within one poll interval, so their row count is not compared.
INTAKE_CURSOR_TABLES = frozenset({"factory_poll_cursors"})
# Truncated by the reset's existing statements rather than the generic sweep.
RESET_COVERED_TABLES = frozenset({"work_items", "agents", "tenants"})
TABLE_NAME = re.compile(r"[a-z_][a-z0-9_]*")
TABLE_ROWS_QUERY = (
    "SELECT coalesce(json_object_agg(t.table_name, (xpath('/row/c/text()', "
    "query_to_xml(format('SELECT count(*) AS c FROM %I.%I', t.table_schema, t.table_name), "
    "false, true, '')))[1]::text::bigint), '{}'::json) "
    "FROM information_schema.tables t "
    "WHERE t.table_schema = 'curie' AND t.table_type = 'BASE TABLE'"
)
# The reset's own deadlock_timeout. The API keeps running through a reset, and
# its reads can close a lock cycle with the TRUNCATE: the GitHub poll intake
# reads agent_channels and then work_items in one transaction, while the
# TRUNCATE locks work_items first and agent_channels later. Each backend
# checks for a cycle once, deadlock_timeout into a lock wait, and Postgres
# aborts the backend whose check finds it. The reset began waiting first, so
# at the shared 1 s default its check ran first and the reset was aborted.
# The API keeps that default, so the live transaction finds such a cycle and
# is aborted instead, and its loop repeats the pass. The reset waits on lock
# holders exactly as before. A cycle whose live members all ran their one
# check before it closed is broken by reset_cycle_breaker_sql instead; this
# timeout only bounds a reset whose breaker never ran.
RESET_DEADLOCK_TIMEOUT = "1min"
# The two-key advisory lock the reset holds for its whole transaction, so the
# breaker can find the reset's backend in pg_locks. The API's two-key
# advisory locks use other class ids.
RESET_LOCK_KEYS = (0x6334, 0x7273)
# How often the breaker looks for a cycle through the reset.
RESET_BREAKER_INTERVAL_SECONDS = 0.05
# The breaker's own bound, past the reset's 300 s psql command budget, for a
# breaker whose psql client was killed without the server noticing.
RESET_BREAKER_LIFETIME = "330 seconds"


class CaseIsolationError(RuntimeError):
    """The shared install is not in its verified clean baseline state."""


# The chart points Langfuse at the platform Valkey (REDIS_HOST, no key
# prefix), and its BullMQ queues use BullMQ's default "bull:" keyspace. Those
# keys belong to the trace backend, which writes them asynchronously while a
# finished case's traces are ingested: a queue's event stream first appears
# when a job is enqueued on it, which can land after a reset. They are not
# factory state, and deleting them would corrupt Langfuse's queues. Curie
# writes nothing under this prefix.
TRACE_BACKEND_KEY_PREFIX = "bull:"


def case_owned_streams(keys: set[str]) -> frozenset[str]:
    return frozenset(key for key in keys if not key.startswith(TRACE_BACKEND_KEY_PREFIX))


@dataclass(frozen=True)
class InstallSnapshot:
    agents: int
    work_items: int
    execution_requests: int
    tenants: frozenset[str]
    sandbox_claims: frozenset[str]
    publication_jobs: frozenset[str]
    # Per-publication companions. The persistent publication owner ConfigMap
    # owns them, so deleting a Job leaves them behind.
    publication_secrets: frozenset[str]
    publication_config_maps: frozenset[str]
    api_forges: Any
    worker_env: tuple[tuple[str, str], ...]
    worker_pods: int
    deployments: frozenset[str]
    network_policies: frozenset[str]
    runs_stream_length: int
    streams: frozenset[str]
    claimed_sandboxes: frozenset[str]
    terminating_pods: frozenset[str]
    # Exact row count of every inventoried table in the curie schema, sorted.
    table_rows: tuple[tuple[str, int], ...]
    # Valkey keys with no TTL, so state a case leaves behind cannot expire.
    persistent_keys: frozenset[str]


def inventoried_table_rows(counts: Mapping[str, Any]) -> tuple[tuple[str, int], ...]:
    """The table_rows field from one TABLE_ROWS_QUERY result."""

    return tuple(
        sorted(
            (str(table), int(count))
            for table, count in counts.items()
            if table not in INSTALL_LIFETIME_TABLES | INTAKE_CURSOR_TABLES
        )
    )


def transaction_sql(query: str) -> str:
    """The psql command text that runs ``query`` as one write transaction."""

    return "BEGIN; " + query + "; COMMIT;"


def reset_sql(table_rows: tuple[tuple[str, int], ...], tenants: frozenset[str]) -> str:
    """The reset's one write: empty every case table and every case tenant.

    Every table that references curie.agents (channels, versions,
    deployments and their dependants) or curie.work_items is truncated
    with them, which releases the case-created tenants those rows named.
    The cascade also empties tables whose nullable foreign keys reference
    that set, such as credential_redemption_audit_entries and
    github_review_deliveries; no case asserts on them. Every other
    table that was empty in the baseline is truncated too, so a row a
    case created anywhere else is removed. A table with baseline rows is
    not cleaned: the verification snapshot reports a changed count. The text
    runs inside transaction_sql, so its SET LOCAL and its advisory lock last
    for the reset alone. The advisory lock is how reset_cycle_breaker_sql,
    which runs beside it, finds the reset's backend.
    """

    kept = ", ".join(f"'{uuid.UUID(tenant)}'" for tenant in sorted(tenants))
    empty_tables = sorted(
        table for table, count in table_rows if count == 0 and table not in RESET_COVERED_TABLES
    )
    for table in empty_tables:
        if not TABLE_NAME.fullmatch(table):
            raise RuntimeError(f"unexpected curie table name {table!r}")
    # One statement lets Postgres order the foreign keys among the swept
    # tables. A baseline table the cascade reaches shows up as a changed
    # count in the verification snapshot and fails the case.
    swept = ["curie.work_items", "curie.agents"] + [
        f'curie."{table}"' for table in sorted(INTAKE_CURSOR_TABLES | set(empty_tables))
    ]
    return (
        f"SET LOCAL deadlock_timeout = '{RESET_DEADLOCK_TIMEOUT}'; "
        f"SELECT pg_advisory_xact_lock({RESET_LOCK_KEYS[0]}, {RESET_LOCK_KEYS[1]}); "
        "TRUNCATE " + ", ".join(swept) + " CASCADE; "
        "DELETE FROM curie.tenants" + (f" WHERE id NOT IN ({kept})" if kept else "")
    )


def reset_cycle_breaker_sql() -> str:
    """The statement that runs beside the reset and breaks its lock cycles.

    Postgres runs one deadlock check per lock wait, deadlock_timeout after
    the wait starts. A live transaction that began waiting on a table the
    reset already holds, while the reset was still waiting on some unrelated
    holder, checks and finds no cycle. When the reset later waits on a table
    that transaction holds, the cycle is complete, but only the reset's own
    check is still to run, so Postgres aborts the reset.

    The breaker looks for that cycle from the outside: every backend that
    directly blocks the reset and is itself blocked, through any chain of
    lock waits, by the reset. It cancels that backend's statement, which is
    the victim Postgres picks when the live side's check runs first, and the
    API's loops repeat a cancelled pass the same way. A backend that only
    holds a lock the reset waits for, without waiting on the reset, is left
    alone, and so is a reset in any other database on the server. The
    statement loops until its client is gone or its lifetime ends.
    """

    classid, objid = RESET_LOCK_KEYS
    return (
        "DO $c4_breaker$ DECLARE until timestamptz := clock_timestamp() + "
        f"interval '{RESET_BREAKER_LIFETIME}'; BEGIN "
        "WHILE clock_timestamp() < until LOOP "
        "PERFORM pg_cancel_backend(cycle.root) FROM ("
        "WITH RECURSIVE reset AS ("
        "SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND granted "
        "AND database = (SELECT oid FROM pg_database WHERE datname = current_database()) "
        f"AND classid = {classid} AND objid = {objid} AND objsubid = 2), "
        "waits(root, pid) AS ("
        "SELECT blocker, blocker FROM reset, unnest(pg_blocking_pids(reset.pid)) AS blocker "
        "UNION SELECT waits.root, blocker FROM waits, "
        "unnest(pg_blocking_pids(waits.pid)) AS blocker) "
        "SELECT DISTINCT waits.root FROM waits JOIN reset ON waits.pid = reset.pid"
        ") AS cycle; "
        f"PERFORM pg_sleep({RESET_BREAKER_INTERVAL_SECONDS}); "
        "END LOOP; END $c4_breaker$"
    )


_ZERO_FIELDS = frozenset(
    {"agents", "work_items", "execution_requests", "worker_pods", "runs_stream_length"}
)
_EMPTY_FIELDS = frozenset(
    {
        "sandbox_claims",
        "publication_jobs",
        "publication_secrets",
        "publication_config_maps",
        "claimed_sandboxes",
        "terminating_pods",
    }
)
_BASELINE_SET_FIELDS = frozenset(
    {"tenants", "deployments", "network_policies", "streams", "persistent_keys"}
)


def _names(values: frozenset[str] | set[str]) -> str:
    return ", ".join(sorted(values))


def _set_difference(field: str, baseline: frozenset[str], current: frozenset[str]) -> str | None:
    baseline, current = frozenset(baseline), frozenset(current)
    if baseline == current:
        return None
    parts = []
    if extra := current - baseline:
        parts.append(f"extra {_names(extra)}")
    if missing := baseline - current:
        parts.append(f"missing {_names(missing)}")
    return f"{field}: " + "; ".join(parts)


def _env_difference(
    baseline: tuple[tuple[str, str], ...], current: tuple[tuple[str, str], ...]
) -> str | None:
    if sorted(baseline) == sorted(current):
        return None
    before, after = dict(baseline), dict(current)
    # Only variable names are reported: values and valueFrom references can
    # name secrets.
    parts = []
    if added := after.keys() - before.keys():
        parts.append(f"added {_names(set(added))}")
    if removed := before.keys() - after.keys():
        parts.append(f"removed {_names(set(removed))}")
    if changed := {name for name in before.keys() & after.keys() if before[name] != after[name]}:
        parts.append(f"changed {_names(changed)}")
    if not parts:
        parts.append("entries differ")
    return "worker_env: " + "; ".join(parts)


def _table_rows_difference(
    baseline: tuple[tuple[str, int], ...], current: tuple[tuple[str, int], ...]
) -> str | None:
    before, after = dict(baseline), dict(current)
    parts = []
    for table in sorted(before.keys() | after.keys()):
        if table not in after:
            parts.append(f"{table} {before[table]} -> absent")
        elif table not in before:
            parts.append(f"{table} absent -> {after[table]}")
        elif before[table] != after[table]:
            parts.append(f"{table} {before[table]} -> {after[table]}")
    return "table_rows: " + ", ".join(parts) if parts else None


def leftover_state(baseline: InstallSnapshot, current: InstallSnapshot) -> list[str]:
    """One stable, secret-free message per leftover; [] means the install is clean."""

    messages = []
    for item in fields(InstallSnapshot):
        name = item.name
        before, after = getattr(baseline, name), getattr(current, name)
        message: str | None = None
        if name in _ZERO_FIELDS:
            message = f"{name}: {after} present, expected 0" if after else None
        elif name in _EMPTY_FIELDS:
            values = frozenset(after)
            if values:
                message = f"{name}: {len(values)} present ({_names(values)}), expected none"
        elif name in _BASELINE_SET_FIELDS:
            message = _set_difference(name, before, after)
        elif name == "worker_env":
            message = _env_difference(before, after)
        elif name == "table_rows":
            message = _table_rows_difference(before, after)
        elif after != before:
            # api_forges: CURIE_FORGES content is not reported.
            message = f"{name}: differs from the baseline"
        if message:
            messages.append(message)
    return messages
