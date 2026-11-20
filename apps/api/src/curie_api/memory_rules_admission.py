"""Admission-only learned rule pinning and metadata projections (#5798).

A WorkItem pins the accepted rules of its repository scope once, at its first
admission, in the transaction that inserts it. Every execution request of that
WorkItem copies the pin's evidence state, so a rule accepted or removed while a
ticket runs changes only the next ticket.

The pin fails closed: a scope that holds more rules than one selection may carry,
or whose canonical text is over the snapshot bound, refuses the admission with
``memory_rules_unavailable`` and writes nothing. Truncating it would silently
drop a rule a person accepted.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Sequence
from typing import Any, Literal, cast

from curie_internal.memory_rules import (
    SelectedMemoryRules,
    repository_scope_key,
    select_memory_rules,
)
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from .factory_tracker_result_runtime import CONTRACT_IMPLEMENTATION, CompletionContractName
from .forges.types import RepositoryRef
from .models import WorkItem, WorkItemMemoryRuleSnapshot
from .schemas.workitems import MemoryRulesMetadata

MemoryRulesEvidence = Literal["none", "pending", "applied", "failed"]


class MemoryRulesUnavailable(RuntimeError):
    pass


def scope_key(repository: RepositoryRef) -> str:
    """The repository scope key the accept route stores on a rule."""

    return repository_scope_key(repository.kind, repository.host, repository.project_id)


async def selection_columns(
    session: AsyncSession, *, agent_id: uuid.UUID, repository: RepositoryRef
) -> tuple[dict[str, Any], SelectedMemoryRules | None]:
    """The WorkItem pin columns and the selection a snapshot records, if any."""

    try:
        selection = await select_memory_rules(
            session,
            agent_id=agent_id,
            scope_kind="repository",
            scope_key=scope_key(repository),
        )
    except ValueError:
        # ScopeOverfull, a canonical text over the snapshot bound (a pydantic
        # ValidationError), or a stored row that no longer validates.
        raise MemoryRulesUnavailable("memory_rules_unavailable") from None
    selected = selection if isinstance(selection, SelectedMemoryRules) else None
    return {
        "memory_rules_state": selection.kind,
        "memory_rules_count": selected.count if selected else 0,
        "memory_rules_sha256": selected.sha256 if selected else None,
        "memory_rules_format_version": selected.format_version if selected else None,
    }, selected


async def add_snapshot(
    session: AsyncSession, *, work_item_id: uuid.UUID, selection: SelectedMemoryRules | None
) -> None:
    if selection is not None:
        try:
            session.add(
                WorkItemMemoryRuleSnapshot(
                    work_item_id=work_item_id,
                    rule_numbers=list(selection.rule_numbers),
                    text=selection.text,
                    sha256=selection.sha256,
                )
            )
            await session.flush()
        except DBAPIError:
            raise MemoryRulesUnavailable("memory_rules_unavailable") from None


async def rule_numbers(
    session: AsyncSession, work_item_ids: Iterable[uuid.UUID]
) -> dict[uuid.UUID, list[int]]:
    """The pinned rule numbers of each WorkItem that has a snapshot, in one read."""

    ids = list(set(work_item_ids))
    if not ids:
        return {}
    rows = await session.execute(
        select(
            WorkItemMemoryRuleSnapshot.work_item_id, WorkItemMemoryRuleSnapshot.rule_numbers
        ).where(WorkItemMemoryRuleSnapshot.work_item_id.in_(ids))
    )
    return {row[0]: [int(number) for number in row[1]] for row in rows}


def metadata(item: WorkItem, numbers: Sequence[int] = ()) -> MemoryRulesMetadata:
    """Public pin metadata; never the rule text."""

    return MemoryRulesMetadata.model_validate(
        {
            "state": item.memory_rules_state,
            "count": item.memory_rules_count,
            "rule_numbers": list(numbers),
            "sha256": item.memory_rules_sha256,
            "format_version": item.memory_rules_format_version,
        }
    )


def request_selection_columns(
    item: WorkItem, *, completion_contract: CompletionContractName
) -> dict[str, Any]:
    """The request's evidence reference: pending only for an implementation request.

    The rules are delivered to the implementer only, so a request under any other
    completion contract (the separate triage path, a publication recovery) carries
    ``none`` and is served an absent envelope.
    """

    pending = (
        item.memory_rules_state == "selected" and completion_contract == CONTRACT_IMPLEMENTATION
    )
    return {"memory_rules_evidence": "pending" if pending else "none"}


def evidence(value: str) -> MemoryRulesEvidence:
    return cast(MemoryRulesEvidence, value)
