"""Current core checks. @spec HTTP-INVOCATION-EXECUTION-8/16."""

from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any, Never
from urllib.parse import urlsplit

import httpx
from curie_internal.identity_approval import (
    ApprovalGrantAuthorityRequestV1,
    ApprovalGrantAuthorityResponseV1,
)
from curie_internal.identity_authority import (
    BindingClassificationRequestV1,
    BindingClassificationResponseV1,
    TurnAuthorityRequestV1,
    TurnAuthorityResponseV1,
)


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


async def exchange_identity_json(
    client: httpx.AsyncClient, request: httpx.Request, *, budget_s: float,
) -> tuple[int, dict[str, Any]]:
    """Caller owns credentials/client; this owns the reply. @spec HTTP-APPROVAL-CONTINUITY-3/12."""
    deadline = asyncio.get_running_loop().time() + budget_s
    response: httpx.Response | None = None
    cancelled = False
    try:
        async with asyncio.timeout_at(deadline):
            response = await client.send(request, stream=True, auth=None, follow_redirects=False)
            raw = bytearray()
            async for chunk in response.aiter_bytes():
                if len(raw) + len(chunk) > 65_536:
                    raise ValueError
                raw.extend(chunk)
            value = json.loads(
                bytes(raw).decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant,
            )
            if type(value) is not dict:
                raise ValueError
            return response.status_code, value
    except asyncio.CancelledError:
        cancelled = True
        raise
    finally:
        if response is not None:
            try:
                remaining = max(0.0, deadline - asyncio.get_running_loop().time())
                async with asyncio.timeout(min(0.25, remaining)):
                    await response.aclose()
            except Exception:  # noqa: BLE001 - preserve caller cancellation across cleanup faults.
                if not cancelled:
                    raise


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
        self._grant_url = api_base_url.rstrip("/") + "/v1/internal/identity/approval-grant/check"
        self._classification_url = (
            api_base_url.rstrip("/") + "/v1/internal/identity/binding-classification/check"
        )
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

    async def check_grant(
        self, request: ApprovalGrantAuthorityRequestV1,
    ) -> ApprovalGrantAuthorityResponseV1:
        """@spec HTTP-APPROVAL-CONTINUITY-12."""
        try:
            if type(request) is not ApprovalGrantAuthorityRequestV1:
                raise ValueError
            captured = ApprovalGrantAuthorityRequestV1.model_validate(request)
            payload = captured.model_dump_json().encode("utf-8")
            if len(payload) > 65_536:
                raise ValueError
        except (ValueError, TypeError, AttributeError, RecursionError):
            raise ValueError("invalid_approval_grant_request") from None
        try:
            outgoing = httpx.Request(
                "POST", self._grant_url, content=payload,
                headers={"X-Curie-Worker-Token": self._token, "Content-Type": "application/json"},
                extensions={"timeout": httpx.Timeout(5.0).as_dict()},
            )
            status, body = await exchange_identity_json(self._client, outgoing, budget_s=5.0)
            if status != 200:
                raise ValueError
            result = ApprovalGrantAuthorityResponseV1.model_validate(body)
            expected = captured.check
            check = result.check
            grant_json = json.dumps(
                captured.grant.model_dump(mode="json"), ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            ).encode("utf-8")
            if (
                check.request_id != expected.request_id or check.stage != expected.stage
                or check.event_id != expected.event_id or check.scope != expected.scope
                or check.context_sha256
                != hashlib.sha256(expected.context.encode("ascii")).hexdigest()
                or result.grant_sha256 != hashlib.sha256(grant_json).hexdigest()
            ):
                raise ValueError
            return result
        except Exception:  # noqa: BLE001 - no transport, credential or effect bytes escape.
            raise TurnAuthorityUnavailable() from None

    async def classify_binding(
        self, request: BindingClassificationRequestV1,
    ) -> BindingClassificationResponseV1:
        """Whether the binding identity is registered. @spec HTTP-INVOCATION-EXECUTION-22."""
        try:
            if type(request) is not BindingClassificationRequestV1:
                raise ValueError
            captured = BindingClassificationRequestV1.model_validate(request)
            payload = captured.model_dump_json().encode("utf-8")
        except (ValueError, TypeError, AttributeError, RecursionError):
            raise ValueError("invalid_binding_classification_request") from None
        try:
            outgoing = httpx.Request(
                "POST", self._classification_url, content=payload,
                headers={"X-Curie-Worker-Token": self._token, "Content-Type": "application/json"},
                extensions={"timeout": httpx.Timeout(5.0).as_dict()},
            )
            status, body = await exchange_identity_json(self._client, outgoing, budget_s=5.0)
            if status != 200:
                raise ValueError
            result = BindingClassificationResponseV1.model_validate(body)
            if result.request_id != captured.request_id or result.scope != captured.scope:
                raise ValueError
            return result
        except Exception:  # noqa: BLE001 - no transport or credential bytes escape.
            raise TurnAuthorityUnavailable() from None
