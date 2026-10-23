"""Scoped attachment facade and worker-only catalog/broker control (ADR 0244)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
import time
import uuid
from collections.abc import Callable, Coroutine
from typing import Annotated, Any
from urllib.parse import urlsplit

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from redis.exceptions import RedisError
from starlette.responses import JSONResponse, StreamingResponse

from ..attachment_access import catalog
from ..attachment_access.authority import (
    LEASE_SECONDS,
    Authority,
    current_binding,
    validate,
    validate_route,
)
from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..models import Agent
from ..schemas.attachment_access import (
    Active,
    Capability,
    Completed,
    Completion,
    Context,
    FileDescriptor,
    Listing,
    Owner,
    Page,
    Registered,
    Registration,
    Renewal,
    Selection,
    Ticket,
    Transfer,
)

NO_STORE = {"Cache-Control": "no-store"}
WORKER_HEADER = "X-Curie-Worker-Token"
CHUNK = 1024 * 1024
RESERVE = """
for i=1,2 do redis.call('ZREMRANGEBYSCORE', KEYS[i], '-inf', ARGV[1]) end
if redis.call('ZCARD', KEYS[1]) >= 16 or redis.call('ZCARD', KEYS[2]) >= 2 then return 0 end
for i=1,2 do
 redis.call('ZADD', KEYS[i], ARGV[2], ARGV[3])
 if redis.call('TTL', KEYS[i]) < tonumber(ARGV[4]) then
  redis.call('EXPIRE', KEYS[i], ARGV[4])
 end
end
return 1
"""


class ControlRoute(APIRoute):
    """Limit control bodies before JSON parsing and never echo submitted inputs."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def guarded(request: Request) -> Response:
            chunks, size = [], 0
            async for chunk in request.stream():
                size += len(chunk)
                if size > CHUNK:
                    raise catalog.refused("control_body_too_large", 413)
                chunks.append(chunk)
            request._body = b"".join(chunks)
            try:
                response = await handler(request)
                response.headers.update(NO_STORE)
                return response
            except RequestValidationError:
                return JSONResponse(
                    {"detail": {"code": "attachment_access.request_invalid"}},
                    status_code=422,
                    headers=NO_STORE,
                )
            except RedisError:
                raise catalog.refused("authority_unavailable", 503) from None

        return guarded


def require_enabled() -> None:
    if not get_settings().attachment_access_enabled:
        raise catalog.refused("disabled", 503)


async def require_internal_access(
    _authenticated: Annotated[None, Depends(require_internal_worker_token)],
) -> None:
    require_enabled()


router = APIRouter(
    prefix="/attachment-access", tags=["attachment-access"], route_class=ControlRoute
)
internal_router = APIRouter(
    prefix="/v1/internal/attachment-access",
    tags=["internal-attachment-access"],
    route_class=ControlRoute,
    dependencies=[Depends(require_internal_access)],
)


def ledger(request: Request) -> Authority:
    return Authority(request.app.state.valkey)


def broker_url() -> str:
    url = get_settings().attachment_broker_url.rstrip("/")
    parts = urlsplit(url)
    if (
        parts.scheme not in {"http", "https"}
        or not parts.hostname
        or parts.username
        or parts.password
        or parts.query
        or parts.fragment
    ):
        raise catalog.refused("broker_unconfigured", 503)
    return url


async def require_access(
    request: Request,
    credential: Annotated[str | None, Header(alias="X-Curie-Attachment-Access")] = None,
) -> dict[str, Any]:
    claims = await ledger(request).read(credential)
    require_enabled()
    return claims


Claims = Annotated[dict[str, Any], Depends(require_access)]


@internal_router.post("/register", response_model=Registered)
async def register(data: Registration, session: SessionDep) -> Registered:
    if await session.get(Agent, data.agent_id) is None:
        raise catalog.refused("agent_not_found", 404)
    return Registered(appended=await catalog.register(session, data))


@internal_router.post("/context", response_model=Capability)
async def context(data: Context, request: Request, session: SessionDep) -> Capability:
    authority = ledger(request)
    await validate_route(authority.client, data)
    binding = await current_binding(session, data)
    try:
        async with httpx.AsyncClient(timeout=5, follow_redirects=False, trust_env=False) as client:
            async with client.stream(
                "POST",
                broker_url() + "/health",
                headers={WORKER_HEADER: get_settings().internal_worker_token},
            ) as response:
                payload = bytearray()
                async for part in response.aiter_bytes(4096):
                    payload.extend(part)
                    if len(payload) > 4096:
                        raise catalog.refused("broker_incompatible", 503)
                health = json.loads(payload)
        if response.status_code != 200 or health != {"attachment_access": 1}:
            raise catalog.refused("broker_incompatible", 503)
    except (httpx.HTTPError, ValueError):
        raise catalog.refused("broker_unavailable", 503) from None
    incarnation = await catalog.live_catalog(session, data.agent_id, data.thread_key)
    if incarnation is None:
        raise catalog.refused("catalog_unavailable")
    state = await authority.open(data, binding, catalog_id=str(incarnation.id))
    await validate(session, authority, state)
    return Capability(
        token=authority.credential(data), expires_at=state["exp"], lease_seconds=LEASE_SECONDS
    )


@internal_router.post("/renew", response_model=Active)
async def renew(data: Renewal, request: Request, session: SessionDep) -> Active:
    authority = ledger(request)
    try:
        state = await authority.read(authority.credential(data))
        await validate(session, authority, state)
    except HTTPException:
        return Active(active=False)
    return Active(active=await authority.renew(data, data.current_event_id))


@internal_router.post("/revoke", response_model=Active)
async def revoke(data: Owner, request: Request) -> Active:
    return Active(active=await ledger(request).revoke(data))


@router.post("/list", response_model=Page)
async def list_files(data: Listing, request: Request, session: SessionDep, claims: Claims) -> Page:
    scope = await validate(session, ledger(request), claims)
    rows = await catalog.live_rows(session, scope.agent_id, scope.thread_key)
    start = 0
    if data.cursor is not None:
        found = next((i for i, row in enumerate(rows) if str(row.id) == data.cursor), None)
        if found is None:
            raise catalog.refused("cursor_invalid", 422)
        start = found + 1
    page = rows[start : start + data.limit]
    return Page(
        files=[catalog.descriptor(row, claims["current_event_id"]) for row in page],
        next_cursor=str(page[-1].id) if page and start + len(page) < len(rows) else None,
    )


async def authorized_file(
    request: Request, session: Any, claims: dict[str, Any], ref_id: uuid.UUID
) -> Any:
    scope = await validate(session, ledger(request), claims)
    row = await catalog.selected(session, scope.agent_id, scope.thread_key, ref_id)
    binding = await current_binding(session, scope)
    # A catalog reference can never nominate a different binding or credential.
    identity = binding.adapter or "default"
    if (row.route_kind, row.route_identity) != (
        binding.kind,
        identity,
    ) or row.route_adapter not in {None, binding.adapter}:
        raise catalog.refused("reference_route_changed")
    return row


@router.post("/authorize", response_model=FileDescriptor)
async def authorize(
    data: Selection, request: Request, session: SessionDep, claims: Claims
) -> FileDescriptor:
    row = await authorized_file(request, session, claims, data.id)
    return catalog.descriptor(row, claims["current_event_id"])


async def transaction(request: Request, transfer_id: str) -> dict[str, Any]:
    authority = ledger(request)
    raw = await authority.client.get(authority.key("transfer", transfer_id))
    if raw is None:
        raise catalog.refused("transfer_unavailable", 404)
    result: dict[str, Any] = json.loads(raw)
    if result["deadline"] <= time.time():
        raise catalog.refused("transfer_expired")
    return result


@internal_router.post("/redeem")
async def redeem(data: Ticket, request: Request, session: SessionDep) -> dict[str, Any]:
    authority = ledger(request)
    transfer_id = await authority.client.getdel(authority.key("ticket", data.ticket))
    if transfer_id is None:
        raise catalog.refused("ticket_invalid", 401)
    if isinstance(transfer_id, bytes):
        transfer_id = transfer_id.decode()
    transfer = await transaction(request, transfer_id)
    claims = await authority.read_hash(transfer["token_hash"])
    row = await authorized_file(request, session, claims, uuid.UUID(transfer["id"]))
    binding = await current_binding(session, Context.model_validate(claims["request"]))
    if (
        str(binding.id) != claims["binding_id"]
        or binding.generation != claims["binding_generation"]
    ):
        raise catalog.refused("binding_revoked")
    return {
        "binding_id": str(binding.id),
        "binding_generation": binding.generation,
        "binding_address": binding.address,
        "transfer_id": transfer_id,
        "agent_id": str(row.agent_id),
        "file_id": row.file_id,
        "route_kind": row.route_kind,
        "route_adapter": row.route_adapter,
        "route_identity": row.route_identity,
        "deadline": transfer["deadline"],
    }


@internal_router.post("/complete", response_model=Completion)
async def complete(data: Completed, request: Request, session: SessionDep) -> Completion:
    authority = ledger(request)
    transfer = await transaction(request, data.transfer_id)
    claims = await authority.read_hash(transfer["token_hash"])
    row = await authorized_file(request, session, claims, uuid.UUID(transfer["id"]))
    await catalog.bind_integrity(
        session, row, version=transfer["version"], digest=data.sha256, size=data.size_bytes
    )
    # Recheck live authority after the integrity write, before committing.
    await validate(session, authority, claims)
    await session.commit()
    result = Completion(id=transfer["id"], sha256=data.sha256, size_bytes=data.size_bytes)
    await authority.client.set(
        authority.key("complete", data.transfer_id),
        result.model_dump_json(),
        ex=max(1, int(transfer["deadline"] - time.time())),
    )
    return result


@router.post("/completion", response_model=Completion)
async def completion(
    data: Transfer, request: Request, session: SessionDep, claims: Claims
) -> Completion:
    transfer = await transaction(request, data.transfer_id)
    if transfer["token_hash"] != claims["token_hash"]:
        raise catalog.refused("transfer_unavailable", 404)
    await authorized_file(request, session, claims, uuid.UUID(transfer["id"]))
    raw = await ledger(request).client.get(ledger(request).key("complete", data.transfer_id))
    if raw is None:
        raise catalog.refused("transfer_incomplete")
    return Completion.model_validate_json(raw)


@router.post("/fetch")
async def fetch(
    data: Selection, request: Request, session: SessionDep, claims: Claims
) -> StreamingResponse:
    row = await authorized_file(request, session, claims, data.id)
    authority = ledger(request)
    transfer_id, ticket = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    seconds = min(
        get_settings().attachment_transfer_timeout_seconds, int(claims["exp"] - time.time())
    )
    if seconds <= 0:
        raise catalog.refused("turn_inactive")
    deadline = time.time() + seconds
    keys = (
        authority.prefix + "transfers",
        authority.key("session-transfers", claims["request"]["session_id"]),
    )
    if not await authority.client.eval(
        RESERVE, 2, *keys, time.time(), deadline, transfer_id, seconds + 1
    ):
        raise catalog.refused("transfer_capacity", 429)
    transfer = {
        "id": str(row.id),
        "version": row.version,
        "token_hash": claims["token_hash"],
        "deadline": deadline,
    }
    await authority.client.set(
        authority.key("transfer", transfer_id), json.dumps(transfer), ex=seconds
    )
    await authority.client.set(authority.key("ticket", ticket), transfer_id, ex=min(30, seconds))
    await session.commit()

    async def body() -> Any:
        upstream_response: httpx.Response | None = None

        async def watch() -> None:
            while True:
                await asyncio.sleep(1)
                try:
                    async with request.app.state.sessionmaker() as current:
                        await authorized_file(request, current, claims, data.id)
                except Exception:
                    if upstream_response is not None:
                        await upstream_response.aclose()
                    raise

        monitor = asyncio.create_task(watch())
        try:
            async with (
                asyncio.timeout(max(0, deadline - time.time())),
                httpx.AsyncClient(
                    timeout=httpx.Timeout(30, connect=5), follow_redirects=False, trust_env=False
                ) as client,
            ):
                async with client.stream(
                    "POST",
                    broker_url() + "/fetch",
                    headers={WORKER_HEADER: get_settings().internal_worker_token},
                    json={"ticket": ticket},
                ) as upstream:
                    upstream_response = upstream
                    if upstream.status_code != 200:
                        raise catalog.refused("provider_unavailable", 502)
                    iterator = upstream.aiter_bytes(CHUNK)
                    while True:
                        read = asyncio.create_task(anext(iterator, None))
                        try:
                            done, _ = await asyncio.wait(
                                {read, monitor}, return_when=asyncio.FIRST_COMPLETED
                            )
                            if monitor in done:
                                await monitor
                            chunk = await read
                        finally:
                            if not read.done():
                                read.cancel()
                                with contextlib.suppress(asyncio.CancelledError):
                                    await read
                        if chunk is None:
                            break
                        yield chunk
        except (httpx.HTTPError, HTTPException, TimeoutError):
            # EOF never conveys success. The separately authorized completion
            # endpoint stays incomplete for any interrupted transfer.
            return
        finally:
            monitor.cancel()
            with contextlib.suppress(asyncio.CancelledError, HTTPException, RedisError):
                await monitor
            await authority.client.zrem(keys[0], transfer_id)
            await authority.client.zrem(keys[1], transfer_id)
            await authority.client.delete(authority.key("ticket", ticket))

    return StreamingResponse(
        body(),
        media_type="application/octet-stream",
        headers=NO_STORE | {"X-Curie-Transfer-ID": transfer_id},
    )
