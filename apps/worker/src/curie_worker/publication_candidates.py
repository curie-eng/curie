"""Authenticated publication candidate ledger client (#5428).

A publication candidate is a private, non-authorizing copy of a publication
snapshot and its trusted base, retained until 24 hours after the owning
request's final disposition. This client only moves metadata; the ledger owns
every object key and every retention decision. Nothing here grants publication.
"""

from typing import Any

import httpx


class PublicationCandidateClient:
    def __init__(self, *, api_base_url: str, worker_token: str, client: httpx.AsyncClient) -> None:
        if not worker_token:
            raise ValueError("publication candidates require internal worker auth")
        self._base = api_base_url.rstrip("/")
        self._headers = {"X-Curie-Worker-Token": worker_token}
        self._client = client

    async def _post(self, operation: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = await self._client.post(
            f"{self._base}/v1/internal/publication-candidates/{operation}",
            headers=self._headers,
            json=payload,
            timeout=10.0,
        )
        response.raise_for_status()
        result = response.json()
        if not isinstance(result, dict):
            raise ValueError("publication candidates returned malformed metadata")
        return result

    async def context(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("context", payload)

    async def outcome(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("outcome", payload)

    async def reserve(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("reserve", payload)

    async def complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("complete", payload)

    async def fail(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("fail", payload)

    async def acquire_reader(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("acquire-reader", payload)

    async def release_reader(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("release-reader", payload)

    async def claim_collection(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("claim-collection", payload)

    async def finalize_collection(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("finalize-collection", payload)
