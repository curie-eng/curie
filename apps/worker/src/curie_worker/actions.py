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

from .sealed_snapshot import carries_placeholder, is_post_version, is_sealed_envelope

logger = logging.getLogger(__name__)

# The keys a reporting connector answers with. Named here rather than inline
# because this pair IS the contract a connector author writes to, and a silent
# rename would turn every reversible tool in the fleet irreversible with nothing
# failing.
PRIOR_KEY = "prior"
POST_KEY = "post"
TARGET_KEY = "target"
# The version the call left (ACTION-EXECUTOR-9), recorded as ``post_version``.
VERSION_KEY = "version"


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
    ) -> RecordedAction: ...

    async def complete(self, action_id: str, frame: SideEffectFlag) -> dict[str, Any]: ...


@dataclass(frozen=True)
class Snapshot:
    """What the completion records out of one connector reply."""

    prior_state: dict[str, Any] | None
    post_state: dict[str, Any] | None
    target: dict[str, Any] | None
    post_version: str | None


_NOTHING = Snapshot(None, None, None, None)


def _snapshot(frame: SideEffectFlag) -> Snapshot:
    """What a restore needs, from the connector's reply. @spec ACTION-EXECUTOR-9.

    ``prior_state`` is recorded exactly when the frame is not ``redacted``,
    ``prior`` validates as a sealed envelope, ``version`` is a valid version and
    neither ``target`` nor ``version`` carries the redaction placeholder; then
    ``version`` is recorded as ``post_version``. Anything else records neither,
    so the row is not undoable (ACTION-EXECUTOR-11). A cleartext ``prior`` is
    history: it stays in ``result`` and is never restorable state.

    A ``redacted`` frame is the frozen consumer rule on ``SideEffectFlag``: the
    runner replaced or withheld something in ``result``, so nothing in it is
    replayed (#1873), even an envelope that crossed unaltered beside a scrubbed
    field (ACTION-EXECUTOR-10). The placeholder check is defense in depth for
    Curie's own scrubber.

    ``target`` and ``post`` stay on the record whenever they are objects: they
    are what a person reads, and neither alone makes a row undoable.
    """

    result = frame.result
    if not isinstance(result, dict):
        return _NOTHING

    def _obj(key: str) -> dict[str, Any] | None:
        value = result.get(key)
        return value if isinstance(value, dict) else None

    target = _obj(TARGET_KEY)
    if frame.redacted:
        return Snapshot(None, None, target, None)
    prior = result.get(PRIOR_KEY)
    version = result.get(VERSION_KEY)
    sealed = (
        is_sealed_envelope(prior)
        and is_post_version(version)
        and target is not None
        and not carries_placeholder(target)
    )
    if not sealed:
        return Snapshot(None, _obj(POST_KEY), target, None)
    return Snapshot(prior, _obj(POST_KEY), target, version)


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
        payload = await self._post(self._url, body, "action record")
        return RecordedAction(id=str(payload["id"]), status=str(payload["status"]))

    async def complete(
        self,
        action_id: str,
        frame: SideEffectFlag,
        *,
        connector: str | None = None,
        connector_digest: str | None = None,
    ) -> dict[str, Any]:
        """Close the record with what came back, and return the row as stored.

        Returned rather than discarded because the receipt is rendered from what
        the LEDGER holds, not from what the worker sent: ``undoable`` is derived
        on the record, so reading it back is what keeps a receipt from claiming a
        reversibility the row does not have.

        ``connector`` and ``connector_digest`` are the digest-attributing
        wrapper's verdict (``action_digest``, @spec ACTION-EXECUTOR-12). They
        travel as a pair or not at all; the API refuses half of one, and a
        refused completion would fail the turn, so half a pair is dropped here.
        """

        snapshot = _snapshot(frame)
        body: dict[str, Any] = {
            "failed": bool(frame.failed),
            "result": frame.result,
            "prior_state": snapshot.prior_state,
            "post_state": snapshot.post_state,
            "post_version": snapshot.post_version,
            "target": snapshot.target,
            "detail": frame.detail,
        }
        if connector is not None and connector_digest is not None:
            body["connector"] = connector
            body["connector_digest"] = connector_digest
        elif connector is not None or connector_digest is not None:
            logger.warning("action %s: half a connector attribution dropped", action_id)
        return await self._post(f"{self._url}/{action_id}/complete", body, "action complete")

    async def _post(self, url: str, body: dict[str, Any], what: str) -> dict[str, Any]:
        try:
            response = await self._client.post(url, json=body, headers=self._headers)
        except httpx.HTTPError as exc:
            raise ActionBackendError(f"{what} failed: {exc}") from exc
        # 201 is a fresh record; 200 is the idempotent replay of either call.
        if response.status_code not in (200, 201):
            raise ActionBackendError(
                f"{what} failed: HTTP {response.status_code}: {response.text}"
            )
        payload: dict[str, Any] = response.json()
        return payload
