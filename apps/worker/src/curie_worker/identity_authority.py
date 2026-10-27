"""Current core checks. @spec HTTP-INVOCATION-EXECUTION-8/16."""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Never
from urllib.parse import urlsplit

import httpx
from curie_internal.identity_authority import TurnAuthorityRequestV1, TurnAuthorityResponseV1


class TurnAuthorityUnavailable(RuntimeError):
    """@spec HTTP-INVOCATION-EXECUTION-16."""

    def __init__(self) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        super().__init__("identity_authority_unavailable")


def _pairs(entries: list[tuple[str, Any]]) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    result: dict[str, Any] = {}
    for key, value in entries:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _constant(value: str) -> Never:
    """@spec HTTP-INVOCATION-EXECUTION-16."""
    raise ValueError


class HttpTurnAuthorityClient:
    """@spec HTTP-INVOCATION-EXECUTION-8/16."""

    def __init__(
        self, *, api_base_url: str, worker_token: str, client: httpx.AsyncClient,
    ) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        try:
            if type(api_base_url) is not str or type(worker_token) is not str or not worker_token:
                raise ValueError
            parsed = urlsplit(api_base_url)
            if (
                parsed.scheme not in {"http", "https"} or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or not isinstance(client, httpx.AsyncClient)
            ):
                raise ValueError
            worker_token.encode("ascii")
            if any(char in worker_token for char in "\r\n\x00"):
                raise ValueError
        except (ValueError, TypeError, UnicodeError):
            raise ValueError("invalid_turn_authority_client") from None
        self._url = api_base_url.rstrip("/") + "/v1/internal/identity/turn-authority/check"
        self._token = worker_token
        self._client = client

    async def check(self, request: TurnAuthorityRequestV1) -> TurnAuthorityResponseV1:
        """@spec HTTP-INVOCATION-EXECUTION-16."""
        try:
            if type(request) is not TurnAuthorityRequestV1:
                raise ValueError
            captured = TurnAuthorityRequestV1.model_validate(request)
        except (ValueError, TypeError, AttributeError, RecursionError):
            raise ValueError("invalid_turn_authority_request") from None
        try:
            async with asyncio.timeout(5.0):
                outgoing = self._client.build_request(
                    "POST", self._url,
                    headers={"X-Curie-Worker-Token": self._token},
                    json=captured.model_dump(mode="json"), timeout=5.0,
                )
                for header in ("Authorization", "X-API-Key", "Cookie"):
                    outgoing.headers.pop(header, None)
                response = await self._client.send(
                    outgoing, stream=True, auth=None, follow_redirects=False,
                )
                try:
                    if response.status_code != 200:
                        raise ValueError
                    raw = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(raw) + len(chunk) > 65_536:
                            raise ValueError
                        raw.extend(chunk)
                    value = json.loads(
                        bytes(raw).decode("utf-8"), object_pairs_hook=_pairs,
                        parse_constant=_constant,
                    )
                    result = TurnAuthorityResponseV1.model_validate(value)
                    if (
                        result.request_id != captured.request_id or result.stage != captured.stage
                        or result.event_id != captured.event_id or result.scope != captured.scope
                        or result.context_sha256
                        != hashlib.sha256(captured.context.encode("ascii")).hexdigest()
                    ):
                        raise ValueError
                    return result
                finally:
                    async with asyncio.timeout(0.25):
                        await response.aclose()
        except Exception:  # noqa: BLE001 - fixed errors exclude transport/credential bytes.
            raise TurnAuthorityUnavailable() from None
