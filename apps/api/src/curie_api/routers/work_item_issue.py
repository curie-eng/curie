"""The single issue read authorized by a factory issue read capability (ADR 0187),
the completion contract route, the question route (#5785) and the rule
proposal route (#5796, ADR 0287).

The same ``wir`` capability authorizes ``/work-items/completion-contract`` (the
runner asks which contract its execution runs under),
``/work-items/questions`` (the runner's ``ask_questions`` tool) and
``/work-items/rule-proposal`` (the runner's ``propose_rule`` tool). Each
re-checks that the capability names the durable WorkItem's issue before and
after any tracker call.
"""

import json
import logging
import math
import time
import uuid
from typing import Annotated, Final, NoReturn

from curie_internal.memory_rules import InvalidRule
from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response
from redis.asyncio import Redis
from redis.exceptions import RedisError
from sqlalchemy import select

from curie_api.schemas.workitems import (
    CompletionContractOut,
    IssueReadContext,
    IssueReadContextMint,
    IssueReadResult,
    QuestionsOut,
    QuestionsSubmission,
    RuleProposalOut,
    RuleProposalSubmission,
)

from .. import completion_contract as runtime
from .. import factory_questions as questions_core
from .. import memory_rule_lifecycle as rules_core
from .. import requester_instruction_context
from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
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
from ..models import ExecutionRequest
from ..wirebody import read_bounded_body

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/work-items", tags=["work-items"])
internal_router = APIRouter(prefix="/v1/internal/work-items", tags=["internal-work-items"])

CAPABILITY_HEADER = "X-Curie-Issue-Read"
# ``execution_requests.execution_kind`` of a publication recovery (#5429).
_PUBLICATION_RECOVERY: Final = "publication_recovery"


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
    if data.publication_recovery and (
        await session.scalar(
            select(ExecutionRequest.execution_kind).where(
                ExecutionRequest.id == data.execution_request_id
            )
        )
        != _PUBLICATION_RECOVERY
    ):
        # Only a publication recovery request's own execute delivery may
        # learn that its runner constructs no model session (#5429).
        raise issue_read_error(
            409, "invalid_context", "the execution is not a publication recovery"
        )
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
        publication_recovery=data.publication_recovery,
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
    """Charge one attempt to the execution's issue read window."""

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


# The completion contract route ---------------------------------------------------

_REFUSAL_MESSAGES = {
    runtime.INVALID_CONTEXT: "the execution no longer grants this call",
}


def _refused(code: str) -> HTTPException:
    return issue_read_error(409, code, _REFUSAL_MESSAGES.get(code, "the call was refused"))


def _contract_refused(code: str) -> HTTPException:
    """A completion contract refusal, which also offers no rule proposal.

    A WorkItem cancelled before the call is refused here (409
    ``invalid_context``), the fence that stops its runner before any model
    work; the body still answers ``rule_proposal`` false (ADR 0287, #5796).
    """

    return HTTPException(
        409,
        {
            "code": code,
            "message": _REFUSAL_MESSAGES.get(code, "the call was refused"),
            "rule_proposal": False,
        },
        headers={"Cache-Control": "no-store"},
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
    response: Response,
    session: SessionDep,
    claims: Annotated[IssueReadClaims, Depends(require_issue_read)],
) -> CompletionContractOut:
    """The execution's completion contract.

    An implementation request is answered with its ADR 0278 execution
    binding. A publication recovery request (#5429) passes the same fences and
    is answered ``publication_recovery`` with no execution binding, so its
    runner constructs no model session. A capability that does not name its
    WorkItem's issue, a cancelled WorkItem, and a request that can no longer
    run are 409 ``invalid_context``.
    """

    response.headers["Cache-Control"] = "no-store"
    try:
        pair = await _named_pair(session, claims)
        await runtime.contract_precheck(session, pair)
        await session.rollback()
    except runtime.RuntimeRefused as refused:
        # Every refusal also answers ``rule_proposal`` false (ADR 0287).
        raise _contract_refused(refused.code) from None
    if claims.publication_recovery:
        # Minted only for a publication recovery request's execute delivery,
        # whose kind cannot change. A CI fix or review turn on the same
        # request mints an ordinary capability and gets the ordinary
        # workflow contract below (ADR 0269 point 5).
        return CompletionContractOut(
            completion_contract=_PUBLICATION_RECOVERY,
            execution=None,
            rule_proposal=False,
        )
    # ADR 0278 (#5592): the runner waits for the worker's envelope for this
    # execution, and checks it against this binding, before any model work.
    execution = await requester_instruction_context.execution_binding(
        session, pair.work_item, pair.request
    )
    # ADR 0287 (#5796): the runner mounts ``propose_rule`` only on this offer.
    eligible = await rules_core.rule_proposal_eligibility(
        session, request_id=claims.execution_request_id, work_item_id=claims.work_item_id
    )
    await session.rollback()
    return CompletionContractOut(
        completion_contract="implementation",
        execution=execution,
        rule_proposal=eligible is not None,
    )


# The question route (#5785) ----------------------------------------------------------

_QUESTION_REFUSALS = {
    questions_core.INVALID_CONTEXT: "the execution no longer grants this call",
    questions_core.INVALID_QUESTIONS: "the questions are outside the tool's bounds",
    questions_core.NOT_IMPLEMENTATION_CONTRACT: "this execution does not run the "
    "implementation contract",
    questions_core.REQUEST_NOT_RUNNING: "this execution is not running",
    questions_core.WORK_ITEM_CANCELLED: "this work item is cancelled",
    questions_core.PULL_REQUEST_OPEN: "this work item already has an open pull request",
    questions_core.ALREADY_PUBLISHED: "this execution already published",
    questions_core.ALREADY_ASKED: "this execution already posted different questions",
    questions_core.QUESTION_ROUNDS_EXHAUSTED: "this ticket's question rounds are used",
}


def _question_refused(code: str) -> HTTPException:
    return issue_read_error(409, code, _QUESTION_REFUSALS.get(code, "the questions were refused"))


# Far above any input inside the bounds, even with every character escaped.
_QUESTIONS_BODY_MAX_BYTES: Final = 256 * 1024
# Documented, not bound: the handler reads the body itself, after the
# capability, so no body shape is answered with FastAPI's echoing 422.
_QUESTIONS_BODY = {
    key: value
    for key, value in QuestionsSubmission.model_json_schema(
        ref_template="#/components/schemas/{model}"
    ).items()
    if key != "$defs"
}


def _refuse_json_constant(name: str) -> NoReturn:
    raise ValueError(f"not a JSON value: {name}")


def _finite_float(literal: str) -> float:
    value = float(literal)
    if not math.isfinite(value):
        raise ValueError("not a finite number")
    return value


async def _questions_submission(request: Request) -> QuestionsSubmission:
    """The body as a submission, or 409 ``invalid_questions`` echoing nothing
    for anything that is not one JSON object within the size bound."""

    try:
        raw = await read_bounded_body(request, _QUESTIONS_BODY_MAX_BYTES, subject="questions body")
    except HTTPException:
        raise _question_refused(questions_core.INVALID_QUESTIONS) from None
    try:
        # NaN and the infinities are not JSON (RFC 8259), and a number too
        # large for a float (1e999) decodes to one; refuse them like any other
        # body outside the shape rather than carry them on.
        parsed = json.loads(raw, parse_constant=_refuse_json_constant, parse_float=_finite_float)
    except (ValueError, RecursionError):
        raise _question_refused(questions_core.INVALID_QUESTIONS) from None
    if not isinstance(parsed, dict):
        raise _question_refused(questions_core.INVALID_QUESTIONS)
    return QuestionsSubmission.model_validate(parsed)


@router.post(
    "/questions",
    response_model=QuestionsOut,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _QUESTIONS_BODY}},
        }
    },
)
async def post_questions(
    request: Request,
    response: Response,
    session: SessionDep,
    claims: Annotated[IssueReadClaims, Depends(require_issue_read)],
) -> QuestionsOut:
    """Post this execution's numbered questions on its own ticket (#5785).

    The platform composes the comment and writes it as one marked comment per
    asking request through the tracker's marked-comment write. The round is
    recorded ``posting`` and committed before the tracker write, and
    ``outstanding`` after it, with the answer fence read from the database
    clock then. A replay of the same body by the same request answers the
    first result with no second write. Refusals are 409 with a fixed code and
    nothing echoed; a tracker failure is 503 ``tracker_unavailable`` and
    leaves the round ``posting``, which settlement treats as not asked. The
    body is read only after the capability is verified, and any body that is
    not a JSON object inside the bounds is 409 ``invalid_questions``.
    """

    response.headers["Cache-Control"] = "no-store"
    data = await _questions_submission(request)
    if not questions_core.input_valid(data.summary, data.questions):
        raise _question_refused(questions_core.INVALID_QUESTIONS)
    try:
        await _named_pair(session, claims)
    except runtime.RuntimeRefused as refused:
        raise _question_refused(refused.code) from None
    try:
        begun = await questions_core.begin_round(
            session,
            request_id=claims.execution_request_id,
            summary=data.summary,
            questions=data.questions,
        )
        if isinstance(begun, questions_core.Posting):
            try:
                begun = await questions_core.post_and_record(
                    session,
                    get_settings(),
                    request.app.state.http_client,
                    request_id=claims.execution_request_id,
                    posting=begun,
                )
            except questions_core.TrackerUnavailable:
                raise _tracker_unavailable() from None
    except questions_core.QuestionsRefused as refused:
        raise _question_refused(refused.code) from None
    return QuestionsOut(round=begun.round, rounds_limit=questions_core.ROUND_LIMIT)


# The rule proposal route (#5796, ADR 0287) ---------------------------------------------

# Two fields of at most 240 bytes each, with room for every character escaped.
_RULE_PROPOSAL_BODY_MAX_BYTES: Final = 8 * 1024
_RULE_PROPOSAL_KEYS: Final = frozenset({"rule", "reason"})
# Documented, not bound: the handler reads the body itself, after the
# capability, so no body is answered with FastAPI's echoing 422.
_RULE_PROPOSAL_BODY = RuleProposalSubmission.model_json_schema(
    ref_template="#/components/schemas/{model}"
)
_RULE_REFUSALS = {
    rules_core.NOT_ELIGIBLE: (403, "this execution may not propose a rule"),
    rules_core.ALREADY_PROPOSED: (409, "this execution already proposed a rule"),
    "invalid_body": (422, "the body must be one JSON object with string rule and reason"),
    "invalid_text": (422, "the rule or reason must be one non-empty line"),
    "too_large": (422, "the rule or reason is over 240 bytes"),
    "reserved_text": (422, "the rule or reason holds reserved platform text"),
}
_RULE_UNAVAILABLE: Final = "rule_proposal_unavailable"


def _rule_refused(request_id: uuid.UUID, code: str, sizes: tuple[int, int]) -> HTTPException:
    # Only the request id, the outcome and byte counts: never the text (ADR 0287).
    logger.info(
        "rule proposal refused request=%s code=%s rule_bytes=%d reason_bytes=%d",
        request_id,
        code,
        *sizes,
    )
    status_code, message = _RULE_REFUSALS[code]
    return issue_read_error(status_code, code, message)


def _utf8_size(value: str) -> int:
    return len(value.encode("utf-8", errors="replace"))


async def _rule_proposal_submission(request: Request) -> tuple[str, str] | None:
    """The body's ``(rule, reason)``, or None for anything that is not one JSON
    object of exactly those two string fields within the size bound."""

    try:
        raw = await read_bounded_body(
            request, _RULE_PROPOSAL_BODY_MAX_BYTES, subject="rule proposal body"
        )
        parsed = json.loads(raw, parse_constant=_refuse_json_constant, parse_float=_finite_float)
    except (HTTPException, ValueError, RecursionError):
        return None
    if not isinstance(parsed, dict) or set(parsed) != _RULE_PROPOSAL_KEYS:
        return None
    rule, reason = parsed["rule"], parsed["reason"]
    if not isinstance(rule, str) or not isinstance(reason, str):
        return None
    return rule, reason


@router.post(
    "/rule-proposal",
    response_model=RuleProposalOut,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _RULE_PROPOSAL_BODY}},
        }
    },
)
async def post_rule_proposal(
    request: Request,
    response: Response,
    session: SessionDep,
    claims: Annotated[IssueReadClaims, Depends(require_issue_read)],
) -> RuleProposalOut:
    """Store this execution's one learned rule candidate (#5796, ADR 0287).

    The capability's request and WorkItem decide everything but the text: the
    agent, the repository scope, the source request, pull request lineage and
    URL, and the proposer (the request's verified requester). The candidate
    stays hidden until the pull request merges. A capability that does not
    name its WorkItem's issue is 409 ``invalid_context``; a request that is
    not an eligible revision is 403 ``rule_proposal_not_eligible``; a second
    proposal of the request is 409 ``rule_already_proposed``; a body that is
    not one JSON object of exactly ``rule`` and ``reason`` strings is 422
    ``invalid_body``; text outside the bounds is 422 ``invalid_text``,
    ``too_large`` or ``reserved_text``. No refusal and no log line carries the
    submitted text.
    """

    response.headers["Cache-Control"] = "no-store"
    try:
        await _named_pair(session, claims)
    except runtime.RuntimeRefused as refused:
        raise _refused(refused.code) from None
    request_id = claims.execution_request_id
    submitted = await _rule_proposal_submission(request)
    if submitted is None:
        raise _rule_refused(request_id, "invalid_body", (0, 0))
    rule, reason = submitted
    sizes = (_utf8_size(rule), _utf8_size(reason))
    # A request that is no longer the live execution (finished, or its lease
    # or deadline expired) is refused as not eligible; a database failure
    # while storing is 503 ``rule_proposal_unavailable``, to retry.
    try:
        await rules_core.propose_candidate(
            session,
            request_id=request_id,
            work_item_id=claims.work_item_id,
            rule=rule,
            reason=reason,
        )
    except rules_core.ProposalRefused as refused:
        raise _rule_refused(request_id, refused.code, sizes) from None
    except InvalidRule as invalid:
        raise _rule_refused(request_id, invalid.code, sizes) from None
    except rules_core.ProposalUnavailable:
        # The request id and the outcome only: the database error is dropped.
        logger.warning(
            "rule proposal unavailable request=%s code=%s", request_id, _RULE_UNAVAILABLE
        )
        raise issue_read_error(
            503, _RULE_UNAVAILABLE, "The rule proposal could not be stored; retry shortly"
        ) from None
    logger.info(
        "rule proposal stored request=%s code=candidate rule_bytes=%d reason_bytes=%d",
        request_id,
        *sizes,
    )
    return RuleProposalOut(status="candidate")
