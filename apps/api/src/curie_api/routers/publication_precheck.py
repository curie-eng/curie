"""The single read operation authorized by a publication precheck capability."""

import asyncio
import time
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from redis.asyncio import Redis
from redis.exceptions import RedisError

from ..config import get_settings
from ..deps import SessionDep
from ..publication_precheck_token import (
    PublicationPrecheckClaims,
    metadata_digest,
    verify_claims,
)
from ..publication_truth import (
    PRECHECK_TIMEOUT_SECONDS,
    PublicationPrecheckRefused,
    PublicationPrecheckUnavailable,
    PublicationReadAuthority,
    read_publication_authority,
    read_publication_metadata,
)
from ..schemas import PublicationPrecheck, PublicationPrecheckResult

router = APIRouter(prefix="/publications", tags=["publications"])

# A sliding window uses the shared store's clock and one atomic operation.
# Renewing a capability cannot reset a lineage's budget. Refused attempts at
# capacity add no entries, keeping memory bounded to twenty timestamps.
_CHARGE_ATTEMPT = """
local stamp = redis.call('TIME')
local now = tonumber(stamp[1]) * 1000000 + tonumber(stamp[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - 300000000)
if redis.call('ZCARD', KEYS[1]) >= 20 then
    return 0
end
redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('EXPIRE', KEYS[1], 300)
return 1
"""


def precheck_error(status_code: int, code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code,
        {"code": code, "message": message},
        headers={"Cache-Control": "no-store"},
    )


async def require_publication_precheck(
    credential: Annotated[str | None, Header(alias="X-Curie-Publication-Precheck")] = None,
) -> PublicationPrecheckClaims:
    claims = verify_claims(credential, get_settings().api_key) if credential else None
    if claims is None:
        raise precheck_error(401, "invalid_capability", "publication read credential is invalid")
    return claims


def require_claim_authority(
    claims: PublicationPrecheckClaims, authority: PublicationReadAuthority | None
) -> None:
    if (
        authority is None
        or claims.agent_id != authority.agent_id
        or claims.deployment_id != authority.deployment_id
        or claims.work_item_id != authority.work_item_id
        or claims.execution_request_id != authority.execution_request_id
        or claims.runtime_epoch != authority.runtime_epoch
        or claims.conversation_id != authority.conversation_id
        or claims.lineage_id != authority.lineage_id
        or claims.lineage_version != authority.lineage_version
        or claims.expected_head != authority.expected_head
        or claims.exp > authority.execution_deadline.timestamp()
        or claims.exp <= time.time()
    ):
        raise PublicationPrecheckRefused


async def charge_precheck_attempt(client: Redis, lineage_id: uuid.UUID) -> None:
    try:
        charged = await client.eval(
            _CHARGE_ATTEMPT, 1, f"publication:precheck:{lineage_id}", uuid.uuid4().hex
        )
    except RedisError:
        raise PublicationPrecheckUnavailable from None
    if charged == 0:
        raise precheck_error(
            429, "rate_limited", "publication comparison budget is exhausted; try again later"
        )
    if charged != 1:
        raise PublicationPrecheckUnavailable


@router.post("/precheck", response_model=PublicationPrecheckResult)
async def compare_publication_metadata(
    data: PublicationPrecheck,
    request: Request,
    response: Response,
    session: SessionDep,
    claims: Annotated[PublicationPrecheckClaims, Depends(require_publication_precheck)],
) -> PublicationPrecheckResult:
    response.headers["Cache-Control"] = "no-store"
    if (
        metadata_digest(data.observed_title) != claims.observed_title_sha256
        or data.observed_body_sha256 != claims.observed_body_sha256
        or data.observed_at != claims.observed_at
    ):
        raise precheck_error(409, "invalid_context", "publication observation does not match")
    try:
        async with asyncio.timeout(PRECHECK_TIMEOUT_SECONDS):
            authority = await read_publication_authority(
                session,
                deployment_id=claims.deployment_id,
                work_item_id=claims.work_item_id,
                execution_request_id=claims.execution_request_id,
                runtime_epoch=claims.runtime_epoch,
            )
            require_claim_authority(claims, authority)
            assert authority is not None
            await charge_precheck_attempt(request.app.state.valkey, authority.lineage_id)
            if authority.has_inflight_push:
                raise PublicationPrecheckUnavailable
            metadata = await read_publication_metadata(
                authority, settings=get_settings(), client=request.app.state.http_client
            )
            current = await read_publication_authority(
                session,
                deployment_id=claims.deployment_id,
                work_item_id=claims.work_item_id,
                execution_request_id=claims.execution_request_id,
                runtime_epoch=claims.runtime_epoch,
            )
            require_claim_authority(claims, current)
            assert current is not None
            if current.has_inflight_push:
                raise PublicationPrecheckUnavailable
            if current != authority:
                raise PublicationPrecheckRefused
    except PublicationPrecheckRefused:
        raise precheck_error(
            409, "invalid_context", "publication execution authority is no longer current"
        ) from None
    except (PublicationPrecheckUnavailable, TimeoutError):
        raise precheck_error(
            503, "precheck_unavailable", "current publication metadata could not be verified"
        ) from None
    if (
        metadata_digest(metadata.title) != claims.observed_title_sha256
        or metadata_digest(metadata.body) != claims.observed_body_sha256
    ):
        raise precheck_error(
            409, "stale_context", "pull request metadata changed after this turn was prepared"
        )
    return PublicationPrecheckResult(
        result=(
            "unchanged"
            if data.proposed_title == metadata.title and data.proposed_body == metadata.body
            else "metadata_changed"
        )
    )
