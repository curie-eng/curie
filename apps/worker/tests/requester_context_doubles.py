"""Requester context halves for WorkItem API doubles (ADR 0278, #5592).

Every started WorkItem request now reads its context envelope and relays the
runner's acknowledgment before its first turn. Doubles of the dispatch client
that do not test that path inherit this explicit absence: the envelope selects
nothing, and the relayed outcome becomes the evidence the API would record.
"""

from __future__ import annotations

import uuid
from typing import Any

TENANT_ID = uuid.UUID("00000000-0000-4000-8000-0000000000aa")


class AbsentRequesterContext:
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
