"""Recording what a turn did to the world (ADR-0117).

The worker has no database of its own -- it persists an approval by POSTing to
the platform API, and it records an action the same way. So the two ACI frames of
one side-effecting call become two calls here: ``record`` when the call was made,
``complete`` when its result came back.

The connector's reply is the only declaration of reversibility (decision 1), and
this module is where that convention is read: ``prior`` and ``target`` out of the
tool's structured reply. Nothing else can supply them -- no function of
``scale_deployment(replicas=10)``'s arguments produces the replica count from
before the call, which is why snapshot-restore is not the safer mechanism but the
only one that knows the answer.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
from aci_protocol import SideEffectFlag

from .api_retry import DEFAULT_BUDGET_S, post_with_retry

logger = logging.getLogger(__name__)

# The keys a reporting connector answers with. Named here rather than inline
# because this pair IS the contract a connector author writes to, and a silent
# rename would turn every reversible tool in the fleet irreversible with nothing
# failing.
PRIOR_KEY = "prior"
POST_KEY = "post"
TARGET_KEY = "target"


class ActionBackendError(RuntimeError):
    """The ledger could not be written."""


@dataclass(frozen=True)
class RecordedAction:
    id: str
    status: str


class ActionRecorder(Protocol):
    """The kernel's whole view of the ledger: open a record, then close it."""

    async def record(
        self,
        frame: SideEffectFlag,
        *,
        event_id: str,
        conversation_id: str,
        agent_id: str | None,
        gate_approval_id: str | None = None,
        budget_s: float = DEFAULT_BUDGET_S,
    ) -> RecordedAction: ...

    async def complete(
        self, action_id: str, frame: SideEffectFlag, *, budget_s: float = DEFAULT_BUDGET_S
    ) -> dict[str, Any]: ...


def _snapshot(
    frame: SideEffectFlag,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, dict[str, Any] | None]:
    """What the call read, what it left, and what it acted on -- from its reply.

    ``prior`` is what a restore puts back and ``post`` is what a restore is
    checked against, so the two are not interchangeable: comparing the live
    resource to ``prior`` would refuse every undo that is actually safe and
    permit exactly the one that is not.

    A connector that answered in prose carries no structured result and lands
    here as all-None, which downstream means not undoable. So does one that
    returned JSON without reporting these. Neither is an error and neither is
    inferred: an inferred state is a guess a restore would act on.
    """

    result = frame.result
    if not isinstance(result, dict):
        return None, None, None
    if frame.redacted:
        # The runner replaced something inside this reply, so ``prior`` may hold
        # a placeholder where a value was, and a restore would write it (#1873).
        # Recording neither state keeps the row not undoable; ``target`` names
        # what was acted on and is never replayed, so it stays.
        target = result.get(TARGET_KEY)
        return None, None, target if isinstance(target, dict) else None

    def _obj(key: str) -> dict[str, Any] | None:
        value = result.get(key)
        return value if isinstance(value, dict) else None

    return _obj(PRIOR_KEY), _obj(POST_KEY), _obj(TARGET_KEY)


class ActionClient:
    """HTTP implementation against the platform API's /actions endpoint."""

    def __init__(
        self,
        *,
        api_base_url: str,
        api_key: str,
        client: httpx.AsyncClient,
    ) -> None:
        self._url = f"{api_base_url.rstrip('/')}/actions"
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._client = client

    async def record(
        self,
        frame: SideEffectFlag,
        *,
        event_id: str,
        conversation_id: str,
        agent_id: str | None,
        gate_approval_id: str | None = None,
        budget_s: float = DEFAULT_BUDGET_S,
    ) -> RecordedAction:
        """Open the record for a call that was just made.

        ``dedupe_key`` is the event id AND the call id. The event id alone would
        collapse a turn that called the same tool twice into one record; the call
        id alone would not survive a redelivery of that turn.
        """

        body = {
            "agent_id": agent_id,
            "conversation_id": conversation_id,
            "call_id": frame.call_id,
            "tool": frame.tool or "unknown",
            "arguments": frame.arguments,
            "detail": frame.detail,
            # What authorized the forward call, so an undo can require the same
            # and no more (ADR-0117 decision 3). None means nothing gated it.
            "gate_approval_id": gate_approval_id,
            "dedupe_key": f"{event_id}:{frame.call_id}",
        }
        payload = await self._post(self._url, body, "action record", budget_s=budget_s)
        return RecordedAction(id=str(payload["id"]), status=str(payload["status"]))

    async def complete(
        self, action_id: str, frame: SideEffectFlag, *, budget_s: float = DEFAULT_BUDGET_S
    ) -> dict[str, Any]:
        """Close the record with what came back, and return the row as stored.

        Returned rather than discarded because the receipt is rendered from what
        the LEDGER holds, not from what the worker sent: ``undoable`` is derived
        on the record, so reading it back is what keeps a receipt from claiming a
        reversibility the row does not have.
        """

        prior, post, target = _snapshot(frame)
        return await self._post(
            f"{self._url}/{action_id}/complete",
            {
                "failed": bool(frame.failed),
                "result": frame.result,
                "prior_state": prior,
                "post_state": post,
                "target": target,
                "detail": frame.detail,
            },
            "action complete",
            budget_s=budget_s,
        )

    async def _post(
        self, url: str, body: dict[str, Any], what: str, *, budget_s: float
    ) -> dict[str, Any]:
        try:
            response = await post_with_retry(
                self._client, url, json=body, headers=self._headers, budget_s=budget_s
            )
        except httpx.HTTPError as exc:
            raise ActionBackendError(f"{what} failed: {exc}") from exc
        # 201 is a fresh record; 200 is the idempotent replay of either call.
        if response.status_code not in (200, 201):
            raise ActionBackendError(
                f"{what} failed: HTTP {response.status_code}: {response.text}"
            )
        payload: dict[str, Any] = response.json()
        return payload
