"""The single issue read authorized by a factory issue read capability (ADR 0187)."""

import time
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from redis.asyncio import Redis
from redis.exceptions import RedisError

from curie_api.schemas.workitems import (
    IssueReadComment,
    IssueReadContext,
    IssueReadContextMint,
    IssueReadRequest,
    IssueReadResult,
)

from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
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
    return (
        claims.work_item_id == authority.work_item_id
        and claims.execution_request_id == authority.execution_request_id
        and claims.github_repository_id == authority.github_repository_id
        and claims.issue_number == authority.issue_number
        and claims.repo_full_name.casefold() == authority.repo_full_name.casefold()
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
        repo_full_name=authority.repo_full_name,
        github_repository_id=authority.github_repository_id,
        issue_number=authority.issue_number,
        iat=now,
        exp=exp,
    )
    return IssueReadContext(
        work_item_id=claims.work_item_id,
        execution_request_id=claims.execution_request_id,
        repo_full_name=claims.repo_full_name,
        issue_number=claims.issue_number,
        capability=mint(get_settings().api_key, claims),
    )


# One execution's reads share a sliding window on the shared store's clock, so
# repeated or concurrent calls cannot drain the installation's GitHub quota.
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
    data: IssueReadRequest,
    request: Request,
    response: Response,
    session: SessionDep,
    claims: Annotated[IssueReadClaims, Depends(require_issue_read)],
) -> IssueReadResult:
    response.headers["Cache-Control"] = "no-store"
    # The capability names one issue. Refuse any other before touching state.
    if (
        data.repo_full_name.casefold() != claims.repo_full_name.casefold()
        or data.issue_number != claims.issue_number
    ):
        raise issue_read_error(
            403,
            "issue_not_granted",
            f"this execution may read only {claims.repo_full_name}#{claims.issue_number}",
        )
    try:
        authority = await read_issue_authority(
            session, execution_request_id=claims.execution_request_id, running=True
        )
        if not _claims_match(claims, authority):
            raise IssueReadRefused
        await charge_issue_read_attempt(request.app.state.valkey, claims.execution_request_id)
        content = await read_issue(
            authority, settings=get_settings(), client=request.app.state.http_client
        )
        # Cancellation or the deadline may land during the GitHub read.
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
        raise issue_read_error(
            503, "issue_unavailable", "GitHub could not answer the issue read; retry shortly"
        ) from None
    return IssueReadResult(
        repo_full_name=authority.repo_full_name,
        issue_number=authority.issue_number,
        title=content.title,
        body=content.body,
        state=content.state,
        author=content.author,
        comments=[
            IssueReadComment(author=c.author, created_at=c.created_at, body=c.body)
            for c in content.comments
        ],
        comments_truncated=content.comments_truncated,
    )
