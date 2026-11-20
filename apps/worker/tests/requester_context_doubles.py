"""Requester context and learned rules halves for WorkItem API doubles.

ADR 0278 (#5592) for the requester context, #5798 for the learned rules.

Every started WorkItem request now reads its context envelope and relays the
runner's acknowledgment before its first turn. Doubles of the dispatch client
that do not test that path inherit this explicit absence: the envelope selects
nothing, and the relayed outcome becomes the evidence the API would record.
"""

from __future__ import annotations

import uuid
from typing import Any

TENANT_ID = uuid.UUID("00000000-0000-4000-8000-0000000000aa")


class AbsentMemoryRulesContext:
    """No pinned learned rules (#5798): the envelope selects nothing."""

    async def memory_rules_context(
        self, request_id: uuid.UUID, *, runtime_epoch: int
    ) -> dict[str, Any]:
        return {
            "envelope_version": 1,
            "tenant_id": str(TENANT_ID),
            "work_item_id": str(uuid.uuid5(TENANT_ID, str(request_id))),
            "execution_request_id": str(request_id),
            "execution_fence": runtime_epoch,
            "selection": {"kind": "absent", "reason": "no_rules"},
        }

    async def acknowledge_memory_rules_context(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
        acknowledgment: dict[str, Any] | None,
        reason: str | None,
    ) -> str:
        outcome = acknowledgment.get("outcome") if acknowledgment is not None else "failed"
        return str(outcome)


class AbsentRequesterContext(AbsentMemoryRulesContext):
    """Absent requester context; the learned rules envelope travels beside it,
    so every started request's double also answers that one absent."""

    async def requester_context(
        self, request_id: uuid.UUID, *, runtime_epoch: int
    ) -> dict[str, Any]:
        return {
            "envelope_version": 1,
            "tenant_id": str(TENANT_ID),
            "work_item_id": str(uuid.uuid5(TENANT_ID, str(request_id))),
            "execution_request_id": str(request_id),
            "execution_fence": runtime_epoch,
            "selection": {"kind": "absent", "reason": "historical"},
        }

    async def acknowledge_requester_context(
        self,
        request_id: uuid.UUID,
        *,
        runtime_epoch: int,
        acknowledgment: dict[str, Any] | None,
        reason: str | None,
    ) -> str:
        outcome = acknowledgment.get("outcome") if acknowledgment is not None else "failed"
        return str(outcome)
