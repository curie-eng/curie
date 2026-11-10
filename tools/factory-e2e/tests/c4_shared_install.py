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
    not cleaned: the verification snapshot reports a changed count.
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
        "TRUNCATE " + ", ".join(swept) + " CASCADE; "
        "DELETE FROM curie.tenants" + (f" WHERE id NOT IN ({kept})" if kept else "")
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
