"""Clean-state rules for the shared kind install of the admitted agent cases.

One session install serves every case in a pytest process. A case may start
only when a snapshot of the install equals the baseline recorded after the
install became ready: nothing a previous case created survives, and nothing
the install owns is missing. This module is pure; the cluster reads live in
c4_admitted_kind.AdmittedKind.install_snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any


class CaseIsolationError(RuntimeError):
    """The shared install is not in its verified clean baseline state."""


@dataclass(frozen=True)
class InstallSnapshot:
    agents: int
    work_items: int
    execution_requests: int
    tenants: frozenset[str]
    sandbox_claims: frozenset[str]
    publication_jobs: frozenset[str]
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


_ZERO_FIELDS = frozenset(
    {"agents", "work_items", "execution_requests", "worker_pods", "runs_stream_length"}
)
_EMPTY_FIELDS = frozenset(
    {"sandbox_claims", "publication_jobs", "claimed_sandboxes", "terminating_pods"}
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
