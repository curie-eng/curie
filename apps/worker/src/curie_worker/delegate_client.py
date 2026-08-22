"""PROTOTYPE: the worker's write client for delegate calls (Draft ADR-0115).

Mirrors ``approvals.ApprovalClient``: the worker never writes to Postgres or
XADDs onto ``curie:runs`` directly for this feature -- it calls back into the
API over HTTP with the platform key, the same pattern every other durable
write the worker makes already uses. See
``docs/demo/ADR-0115-PROTOTYPE-NOTES.md`` for what this prototype cuts from
the full ADR.
"""

from __future__ import annotations

import logging

import httpx

logger = logging.getLogger(__name__)


class DelegateBackendError(Exception):
    """A delegate-call callback to the API failed."""


class DelegateClient:
    """HTTP implementation against the platform API's (prototype) delegate routes."""

    def __init__(self, *, api_base_url: str, api_key: str, client: httpx.AsyncClient) -> None:
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-API-Key": api_key} if api_key else {}
        self._client = client

    async def progress(self, target_agent_id: str, call_id: str, text: str) -> None:
        """Buffer the latest reply text. Best-effort: a lost intermediate update
        costs nothing as long as the FINAL one lands before ``complete``."""
        try:
            response = await self._client.patch(
                f"{self._base}/agents/{target_agent_id}/delegate/calls/{call_id}",
                json={"result_text": text},
                headers=self._headers,
            )
            if response.status_code >= 400:
                logger.warning(
                    "delegate progress update failed for %s: HTTP %s",
                    call_id,
                    response.status_code,
                )
        except httpx.HTTPError as exc:
            logger.warning("delegate progress update failed for %s: %s", call_id, exc)

    async def complete(self, target_agent_id: str, call_id: str, outcome: str) -> None:
        """Deliver the terminal outcome. Raises on failure so the worker's
        completion-outbox retry (kernel.py's ``_deliver_completion``) owns
        redelivery instead of this call silently losing the round trip."""
        try:
            response = await self._client.post(
                f"{self._base}/agents/{target_agent_id}/delegate/calls/{call_id}/complete",
                json={"outcome": outcome},
                headers=self._headers,
            )
        except httpx.HTTPError as exc:
            raise DelegateBackendError(f"delegate complete failed: {exc}") from exc
        if response.status_code >= 400:
            raise DelegateBackendError(
                f"delegate complete failed: HTTP {response.status_code}: {response.text}"
            )
