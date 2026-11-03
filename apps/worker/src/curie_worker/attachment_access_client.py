"""Bounded worker-only control-plane client for on-demand attachments."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import httpx

from .runner_client import RunnerError


class AttachmentAccessUnavailable(RunnerError):
    """Explicit lazy activation cannot proceed against an incompatible API."""


@dataclass(frozen=True)
class AttachmentAccessMint:
    token: str = field(repr=False)
    expires_at: int
    lease_seconds: int


class AttachmentAccessClient:
    def __init__(self, *, api_base_url: str, worker_token: str, client: httpx.AsyncClient) -> None:
        self._url = api_base_url.rstrip("/") + "/v1/internal/attachment-access"
        self._headers = {"X-Curie-Worker-Token": worker_token}
        self._http = client

    async def _post(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = await self._http.post(
                f"{self._url}/{operation}", json=payload, headers=self._headers, timeout=3.0
            )
            response.raise_for_status()
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError("response is not an object")
            return value
        except (httpx.HTTPError, ValueError) as exc:
            # Never include provider identifiers, response bodies or capabilities.
            raise AttachmentAccessUnavailable(
                "On-demand attachments require a compatible API and private worker broker; "
                f"attachment {operation} failed."
            ) from exc

    async def register(self, **payload: Any) -> None:
        value = await self._post("register", payload)
        if type(value.get("appended")) is not int:
            raise AttachmentAccessUnavailable("Attachment registration response was invalid.")

    async def context(self, **payload: Any) -> AttachmentAccessMint:
        value = await self._post("context", payload)
        if (
            not isinstance(value.get("token"), str)
            or not value["token"]
            or type(value.get("expires_at")) is not int
            or type(value.get("lease_seconds")) is not int
            or value["lease_seconds"] <= 0
        ):
            raise AttachmentAccessUnavailable("Attachment context response was invalid.")
        return AttachmentAccessMint(value["token"], value["expires_at"], value["lease_seconds"])

    async def renew(self, **payload: Any) -> bool:
        return (await self._post("renew", payload)).get("active") is True

    async def revoke(self, **payload: Any) -> bool:
        return (await self._post("revoke", payload)).get("active") is True
