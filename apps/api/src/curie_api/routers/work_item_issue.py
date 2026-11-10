"""The single issue read authorized by a factory issue read capability (ADR 0187)."""

import time
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from redis.asyncio import Redis
from redis.exceptions import RedisError

from curie_api.schemas.workitems import (
    IssueReadContext,
    IssueReadContextMint,
    IssueReadResult,
)

from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..forges.errors import ForgeError
from ..forges.registry import forges_registry
from ..forges.trackers import display_reference, issue_link
from ..issue_read import (
    CAPABILITY_TTL_SECONDS,
    IssueReadAuthority,
    IssueReadRefused,
    IssueReadUnavailable,
    read_issue,
    read_issue_authority,
)
from ..issue_read_token import IssueReadClaims, mint, verify_claims

router = APIRouter(prefix="/work-items", tags=["work-items"])
internal_router = APIRouter(prefix="/v1/internal/work-items", tags=["internal-work-items"])

CAPABILITY_HEADER = "X-Curie-Issue-Read"


def issue_read_error(status_code: int, code: str, message: str) -> HTTPException:
    return HTTPException(
        status_code,
        {"code": code, "message": message},
        headers={"Cache-Control": "no-store"},
    )


async def require_issue_read(
    credential: Annotated[str | None, Header(alias=CAPABILITY_HEADER)] = None,
) -> IssueReadClaims:
    claims = verify_claims(credential, get_settings().api_key) if credential else None
    if claims is None:
        raise issue_read_error(401, "invalid_capability", "issue read credential is invalid")
    return claims


def _claims_match(claims: IssueReadClaims, authority: IssueReadAuthority) -> bool:
    issue = authority.issue
    return (
        claims.work_item_id == authority.work_item_id
        and claims.execution_request_id == authority.execution_request_id
        and claims.tracker_kind == issue.kind
        and claims.tracker_host == issue.host
        and claims.tracker_scope_id == issue.scope_id
        and claims.tracker_issue_id == issue.issue_id
    )


@internal_router.post(
    "/issue-read/context",
    response_model=IssueReadContext,
    dependencies=[Depends(require_internal_worker_token)],
)
async def mint_issue_read_context(
    data: IssueReadContextMint, response: Response, session: SessionDep
) -> IssueReadContext:
    response.headers["Cache-Control"] = "no-store"
    try:
        authority = await read_issue_authority(
            session, execution_request_id=data.execution_request_id, running=False
        )
    except IssueReadRefused:
        raise issue_read_error(
            409, "invalid_context", "the execution cannot read its issue"
        ) from None
    now = int(time.time())
    exp = now + CAPABILITY_TTL_SECONDS
    if authority.execution_deadline is not None:
        exp = min(exp, int(authority.execution_deadline.timestamp()))
    claims = IssueReadClaims(
        scope="work_item.issue_read",
        work_item_id=authority.work_item_id,
        execution_request_id=authority.execution_request_id,
        tracker_kind=authority.issue.kind,
        tracker_host=authority.issue.host,
        tracker_scope_id=authority.issue.scope_id,
        tracker_issue_id=authority.issue.issue_id,
        iat=now,
        exp=exp,
    )
    return IssueReadContext(
        work_item_id=claims.work_item_id,
        execution_request_id=claims.execution_request_id,
        issue=display_reference(authority.issue, authority.repository.path),
        capability=mint(get_settings().api_key, claims),
    )


# One execution's reads share a sliding window on the shared store's clock, so
# repeated or concurrent calls cannot drain the tracker's quota.
# Each read costs at most eleven provider requests. Refused attempts add nothing.
_ISSUE_READS_PER_WINDOW = 20
_CHARGE_ATTEMPT = """
local stamp = redis.call('TIME')
local now = tonumber(stamp[1]) * 1000000 + tonumber(stamp[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now - 300000000)
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then
    return 0
end
redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('EXPIRE', KEYS[1], 300)
return 1
"""


async def charge_issue_read_attempt(client: Redis, execution_request_id: uuid.UUID) -> None:
    try:
        charged = await client.eval(
            _CHARGE_ATTEMPT,
            1,
            f"work_item:issue_read:{execution_request_id}",
            uuid.uuid4().hex,
            _ISSUE_READS_PER_WINDOW,
        )
    except RedisError:
        raise IssueReadUnavailable from None
    if charged == 0:
        raise issue_read_error(
            429, "rate_limited", "issue read budget is exhausted; try again in a few minutes"
        )
    if charged != 1:
        raise IssueReadUnavailable


@router.post("/issue-read", response_model=IssueReadResult)
async def read_work_item_issue(
    request: Request,
    response: Response,
    session: SessionDep,
    claims: Annotated[IssueReadClaims, Depends(require_issue_read)],
) -> IssueReadResult:
    response.headers["Cache-Control"] = "no-store"
    # The capability names the one issue; a durable WorkItem holding another
    # refuses it before any read.
    settings = get_settings()
    try:
        authority = await read_issue_authority(
            session, execution_request_id=claims.execution_request_id, running=True
        )
        if not _claims_match(claims, authority):
            raise IssueReadRefused
        await charge_issue_read_attempt(request.app.state.valkey, claims.execution_request_id)
        ticket = await read_issue(
            authority, settings=settings, client=request.app.state.http_client
        )
        # Cancellation or the deadline may land during the tracker read.
        current = await read_issue_authority(
            session, execution_request_id=claims.execution_request_id, running=True
        )
        if not _claims_match(claims, current):
            raise IssueReadRefused
    except IssueReadRefused:
        raise issue_read_error(
            409, "invalid_context", "the execution no longer grants this issue read"
        ) from None
    except IssueReadUnavailable:
        raise _unavailable() from None
    # The database requires both timestamps for a running execution.
    assert current.started_at is not None
    assert current.execution_deadline is not None
    try:
        url = issue_link(forges_registry(settings), current.issue, current.repository)
    except ForgeError:
        raise _unavailable() from None
    return IssueReadResult(
        issue=display_reference(current.issue, current.repository.path),
        url=url,
        ticket=ticket,
        execution_deadline=current.execution_deadline,
        execution_seconds=int((current.execution_deadline - current.started_at).total_seconds()),
    )


def _unavailable() -> HTTPException:
    return issue_read_error(
        503, "issue_unavailable", "The tracker could not answer the issue read; retry shortly"
    )
