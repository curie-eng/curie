"""Operator-controlled workload to repository mapping for inbound hooks (#2572).

A signed delivery may name a workload. The operator map, not the model and not
a URL inside the untrusted payload, is what may select a coding target. Missing,
ambiguous, or unauthorized mappings visibly stop coding. This module is imported
by ``schemas.py`` and ``routers/hooks.py`` and must not import either back.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, Literal

from .hook_partition import HOOK_NAME, PARTITION_VALUE, resolve_pointer
from .workspace_policy import repository_is_allowed, valid_repository_name

MappingStatus = Literal[
    "unconfigured",
    "mapped",
    "missing",
    "ambiguous",
    "unauthorized",
    "wrong_binding",
]

_REVISION = re.compile(
    r"\A(?:[0-9a-f]{40}|[A-Za-z0-9][A-Za-z0-9._-]{0,62})\Z"
)


@dataclass(frozen=True)
class MappingOutcome:
    """The coding-target decision for one verified delivery."""

    status: MappingStatus
    repository: str | None = None
    revision: str | None = None
    reason: str = ""

    @property
    def selects_workspace(self) -> bool:
        return self.status == "mapped" and self.repository is not None


def validate_revision(value: str) -> str:
    """Refuse a revision that cannot be recorded as a deployed source identity."""

    if not _REVISION.fullmatch(value):
        raise ValueError(
            "revision must be a 40-character lowercase git SHA or 1-63 characters "
            "of letters, digits, dot, dash or underscore, beginning with a letter "
            "or a digit"
        )
    return value


def validate_workload_key(value: str) -> str:
    if not PARTITION_VALUE.fullmatch(value):
        raise ValueError(
            "source binding map keys must be 1-63 characters of letters, digits, "
            "dot, dash or underscore, beginning with a letter or a digit"
        )
    return value


def _label_name(pointer: str) -> str | None:
    if not pointer:
        return None
    token = pointer.rsplit("/", 1)[-1]
    return token or None


def _workloads_from_alerts(document: Any, label: str) -> list[str]:
    if not isinstance(document, dict):
        return []
    alerts = document.get("alerts")
    if not isinstance(alerts, list):
        return []
    found: list[str] = []
    seen: set[str] = set()
    for alert in alerts:
        if not isinstance(alert, dict):
            continue
        labels = alert.get("labels")
        if not isinstance(labels, dict):
            continue
        raw = labels.get(label)
        if not isinstance(raw, str) or not PARTITION_VALUE.fullmatch(raw):
            continue
        if raw not in seen:
            seen.add(raw)
            found.append(raw)
    return found


def resolve_source_binding(
    bindings: dict[str, Any] | None,
    hook: str,
    body: bytes,
    allowlist: tuple[str, ...],
) -> MappingOutcome:
    """Decide the coding target for one hook delivery.

    Runs after signature verification. An unsigned caller never learns whether
    a map exists. Unconfigured hooks keep today's generic hook text and select
    no workspace.
    """

    if not bindings:
        return MappingOutcome(status="unconfigured")
    if hook not in bindings:
        return MappingOutcome(
            status="wrong_binding",
            reason=(
                "Coding is stopped: this hook has no operator source binding. "
                "Record a workload-to-allowlisted-repository mapping for this "
                "hook. The model cannot guess a repository."
            ),
        )
    configured = bindings[hook]
    pointer = configured.get("workload_pointer") if isinstance(configured, dict) else None
    mapping = configured.get("map") if isinstance(configured, dict) else None
    if not isinstance(pointer, str) or not isinstance(mapping, dict):
        return MappingOutcome(
            status="missing",
            reason=(
                "Coding is stopped: the source binding for this hook is incomplete. "
                "Record a workload pointer and map. The model cannot guess a "
                "repository."
            ),
        )

    try:
        document: Any = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return MappingOutcome(
            status="missing",
            reason=(
                "Coding is stopped: this delivery is not JSON, so the workload "
                "pointer cannot resolve. The model cannot guess a repository."
            ),
        )

    workloads: list[str] = []
    try:
        value = resolve_pointer(document, pointer)
    except (ValueError, LookupError, TypeError):
        value = None
    if isinstance(value, str) and PARTITION_VALUE.fullmatch(value):
        workloads = [value]
    elif isinstance(value, list):
        workloads = [
            item
            for item in value
            if isinstance(item, str) and PARTITION_VALUE.fullmatch(item)
        ]

    label = _label_name(pointer)
    if not workloads and label is not None:
        workloads = _workloads_from_alerts(document, label)

    matched: list[tuple[str, str, str]] = []
    missing_keys: list[str] = []
    for workload in workloads:
        entry = mapping.get(workload)
        if not isinstance(entry, dict):
            missing_keys.append(workload)
            continue
        repository = entry.get("repository")
        revision = entry.get("revision")
        if not isinstance(repository, str) or not isinstance(revision, str):
            missing_keys.append(workload)
            continue
        matched.append((workload, repository, revision))

    if missing_keys:
        return MappingOutcome(
            status="missing",
            reason=(
                "Coding is stopped: no authorized workload-to-repository mapping "
                "matched this alert. Record one mapping from the workload to an "
                "allowlisted repository and deployed revision. The model cannot "
                "guess a repository."
            ),
        )
    unique_targets = {(item[1].casefold(), item[2]) for item in matched}
    if len(unique_targets) > 1:
        return MappingOutcome(
            status="ambiguous",
            reason=(
                "Coding is stopped: this alert names more than one mapped "
                "workload repository. Record a single authorized mapping or "
                "split the alert group. The model cannot guess a repository."
            ),
        )
    if not matched:
        return MappingOutcome(
            status="missing",
            reason=(
                "Coding is stopped: no authorized workload-to-repository mapping "
                "matched this alert. Record one mapping from the workload to an "
                "allowlisted repository and deployed revision. The model cannot "
                "guess a repository."
            ),
        )

    _workload, repository, revision = matched[0]
    if not valid_repository_name(repository) or not repository_is_allowed(
        repository, allowlist
    ):
        return MappingOutcome(
            status="unauthorized",
            reason=(
                "Coding is stopped: the mapped repository is not on "
                "api.githubRepoAllowlist. Record an allowlisted repository. The "
                "model cannot guess a repository."
            ),
        )
    return MappingOutcome(
        status="mapped",
        repository=repository,
        revision=revision,
        reason=(
            f"This delivery has an authorized source mapping: repository "
            f"{repository} at revision {revision}. Coding may use only this "
            "mapping. The model cannot choose another repository."
        ),
    )


def validate_source_binding_keys(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return value
    for name in value:
        if not HOOK_NAME.fullmatch(name):
            raise ValueError(
                f"source_bindings key {name!r} is not a hook name: 1-63 "
                "characters of lowercase letters, digits, dot, dash or "
                "underscore, beginning with a letter or a digit"
            )
    return value
