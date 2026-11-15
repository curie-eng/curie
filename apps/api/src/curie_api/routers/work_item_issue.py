"""The single issue read authorized by a factory issue read capability (ADR 0187),
and its two tracker result siblings (ADR 0277, #5586).

The same ``wir`` capability authorizes ``/work-items/completion-contract`` (the
runner asks which contract its execution runs under; a tracker result request
reserves its one model session there) and ``/work-items/tracker-result`` (the
runner's ``submit_tracker_result`` tool). Each re-checks that the capability
names the durable WorkItem's issue before and after any tracker call.
"""

import asyncio
import logging
import time
import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from redis.asyncio import Redis
from redis.exceptions import RedisError

from curie_api.schemas.workitems import (
    CompletionContractOut,
    IssueReadContext,
    IssueReadContextMint,
    IssueReadResult,
    TrackerResultOut,
    TrackerResultSubmission,
)

from .. import factory_tracker_result_runtime as runtime
from .. import requester_instruction_context
from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..factory_tracker_result_delivery import (
    TrackerResultRefused,
    TrackerResultStorageError,
    deliver,
)
from ..factory_tracker_results import ProposalInvalid, parse_proposal
from ..forges.errors import ForgeError
from ..forges.registry import forges_registry
from ..forges.trackers import display_reference, issue_link
from ..forges.types import TrackerIssueRef
from ..issue_read import (
    CAPABILITY_TTL_SECONDS,
    IssueReadAuthority,
    IssueReadRefused,
    IssueReadUnavailable,
    read_issue,
    read_issue_authority,
)
from ..issue_read_token import IssueReadClaims, mint, verify_claims

logger = logging.getLogger(__name__)

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


async def charge_issue_read_attempt(
    client: Redis, execution_request_id: uuid.UUID, *, purpose: str = "issue_read"
) -> None:
    """Charge one attempt to the execution's ``purpose`` window (issue reads,
    or the tracker result routes, which keep their own)."""

    try:
        charged = await client.eval(
            _CHARGE_ATTEMPT,
            1,
            f"work_item:{purpose}:{execution_request_id}",
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


# Tracker result siblings (ADR 0277, #5586) -----------------------------------------

# One inline delivery attempt after a submission; the reconciler retries the rest.
_INLINE_DELIVERY_SECONDS = 45.0
_TRACKER_RESULT_BUDGET = "tracker_result"
_REFUSAL_MESSAGES = {
    runtime.INVALID_CONTEXT: "the execution no longer grants this call",
    runtime.SESSION_ALREADY_STARTED: "this execution's one model session already started",
    runtime.SESSION_NOT_STARTED: "this execution has not reserved its model session",
    runtime.NOT_TRACKER_RESULT: "this execution does not complete with a tracker result",
    runtime.RESULT_ALREADY_SUBMITTED: "this execution already submitted its result",
}


def _refused(code: str) -> HTTPException:
    return issue_read_error(
        409, code, _REFUSAL_MESSAGES.get(code, "the tracker result was refused")
    )


def _tracker_unavailable() -> HTTPException:
    return issue_read_error(
        503, "tracker_unavailable", "The tracker could not answer; retry shortly"
    )


async def _named_pair(session: SessionDep, claims: IssueReadClaims) -> runtime.RuntimePair:
    """The capability's request and WorkItem, refused unless the capability
    names exactly that WorkItem's issue. Read-only; the transaction ends here,
    and the returned rows are detached snapshots (no tracker call is made
    inside a transaction)."""

    pair = await runtime.load_pair(session, claims.execution_request_id)
    session.expunge_all()
    await session.rollback()
    claimed = TrackerIssueRef(
        claims.tracker_kind, claims.tracker_host, claims.tracker_scope_id, claims.tracker_issue_id
    )
    if (
        pair is None
        or pair.work_item.id != claims.work_item_id
        or not runtime.names_issue(pair, claimed)
    ):
        raise runtime.RuntimeRefused(runtime.INVALID_CONTEXT)
    return pair


@router.post("/completion-contract", response_model=CompletionContractOut)
async def read_completion_contract(
    request: Request,
    response: Response,
    session: SessionDep,
    claims: Annotated[IssueReadClaims, Depends(require_issue_read)],
) -> CompletionContractOut:
    """The execution's completion contract; for a tracker result request, the
    reservation of its one model session.

    The first call for a tracker result request reads the source issue's
    title and body, mints the request's grant and reserves the session; any
    later call is 409 ``session_already_started``, so a second runner starts
    no model. An implementation request reserves nothing and gets no grant.
    """

    response.headers["Cache-Control"] = "no-store"
    settings = get_settings()
    try:
        pair = await _named_pair(session, claims)
        view = await runtime.contract_precheck(session, pair)
        await session.rollback()
        if view is None:
            await charge_issue_read_attempt(
                request.app.state.valkey,
                claims.execution_request_id,
                purpose=_TRACKER_RESULT_BUDGET,
            )
            _tracker, digest = await runtime.read_source_sha256(
                settings, request.app.state.http_client, pair.work_item
            )
            # Cancellation or a re-route may land during the tracker read.
            await _named_pair(session, claims)
            view = await runtime.reserve_session(
                session,
                request_id=claims.execution_request_id,
                content_sha256=digest,
                settings=settings,
            )
    except runtime.RuntimeRefused as refused:
        raise _refused(refused.code) from None
    except (runtime.SourceUnavailable, IssueReadUnavailable):
        raise _tracker_unavailable() from None
    # ADR 0278 (#5592): the runner waits for the worker's envelope for this
    # execution, and checks it against this binding, before any model work.
    execution = await requester_instruction_context.execution_binding(
        session, pair.work_item, pair.request
    )
    await session.rollback()
    return CompletionContractOut(
        completion_contract=view.completion_contract,
        result_kinds=list(view.result_kinds),
        session_usd_allowance=view.session_usd_allowance,
        execution=execution,
    )


@router.post("/tracker-result", response_model=TrackerResultOut)
async def submit_tracker_result(
    data: TrackerResultSubmission,
    request: Request,
    response: Response,
    session: SessionDep,
    claims: Annotated[IssueReadClaims, Depends(require_issue_read)],
) -> TrackerResultOut:
    """Seal the execution's one tracker result and attempt its delivery once.

    Refuses (409, nothing written) an implementation request, a request
    whose session was never reserved, a second submission, a source issue
    whose title or body changed since the reservation, and every grant fence
    (`RefusalReason`); an invalid proposal is 422 with field paths and codes
    only. An accepted result is answered with its delivery state after one
    inline attempt; the WorkItem reconciler retries an owed one.
    """

    response.headers["Cache-Control"] = "no-store"
    settings = get_settings()
    try:
        pair = await _named_pair(session, claims)
        target = await runtime.submission_precheck(session, pair)
        await session.rollback()
        try:
            proposal = parse_proposal(data.proposal)
        except ProposalInvalid as invalid:
            raise HTTPException(
                422,
                {
                    "code": "proposal_invalid",
                    "issues": [
                        {"field": issue.field, "code": issue.code} for issue in invalid.issues
                    ],
                },
                headers={"Cache-Control": "no-store"},
            ) from None
        await charge_issue_read_attempt(
            request.app.state.valkey, claims.execution_request_id, purpose=_TRACKER_RESULT_BUDGET
        )
        tracker, digest = await runtime.read_source_sha256(
            settings, request.app.state.http_client, pair.work_item
        )
        if target.source_content_sha256 is None or digest != target.source_content_sha256:
            raise runtime.RuntimeRefused("source_fingerprint_changed")
        await _named_pair(session, claims)
        delivery_id = await runtime.seal_submission(
            session,
            request_id=claims.execution_request_id,
            target=target,
            proposal=proposal,
            tracker=tracker,
            settings=settings,
        )
    except runtime.RuntimeRefused as refused:
        raise _refused(refused.code) from None
    except TrackerResultRefused as refused:
        raise _refused(refused.reason.value) from None
    except TrackerResultStorageError:
        raise issue_read_error(
            503, "storage_unavailable", "The result could not be stored; retry shortly"
        ) from None
    except (runtime.SourceUnavailable, IssueReadUnavailable):
        raise _tracker_unavailable() from None
    try:
        async with asyncio.timeout(_INLINE_DELIVERY_SECONDS):
            await deliver(
                request.app.state.sessionmaker,
                delivery_id,
                tracker=tracker,
                settings=settings,
                owner=f"tracker-result-route:{uuid.uuid4()}",
            )
    except (TimeoutError, TrackerResultStorageError):
        # Sealed and owed: the reconciler's delivery pass retries it.
        logger.warning("tracker result inline delivery did not finish delivery=%s", delivery_id)
    answer = await runtime.submission_answer(session, delivery_id)
    return TrackerResultOut.model_validate(
        {
            "result_kind": answer.result_kind,
            "state": answer.state,
            "result_comment_url": answer.result_comment_url,
            "ticket_url": answer.ticket_url,
        }
    )
