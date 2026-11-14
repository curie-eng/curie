"""Tracker result requests at runtime: the completion contract and its session (ADR 0277, #5586).

The completion contract is selected once, at admission, from the trusted
binding's in-force deployed version, never from issue text or model output.
A version whose stored bundle carries a valid ``workflow/result.json`` has a
result policy (its declaration digest, kinds and implementation label) and
admits ``tracker_result`` requests; every other version admits
``implementation`` requests, exactly as before.

The first section (contract selection) is written by admission; the runtime
section below the marker line serves the runner-facing routes.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final, Literal

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .forges.capabilities import Operation, Support, supports
from .forges.connections import BindingTrackerConnection, RepositoryTrackerConnection
from .forges.registry import ForgeRegistry
from .models import AgentVersion, Deployment, Environment
from .tracker_result_declaration import ResultKind

logger = logging.getLogger(__name__)

CONTRACT_IMPLEMENTATION: Final = "implementation"
CONTRACT_TRACKER_RESULT: Final = "tracker_result"
DEFAULT_TRIAGE_SESSION_USD = 5.0

CompletionContractName = Literal["implementation", "tracker_result"]

# The admission refusal when the declared kinds need an operation the
# WorkItem's tracker cannot perform, or the stored policy cannot be read.
REFUSAL_UNSUPPORTED = "tracker_result_unsupported"
REFUSAL_POLICY_UNREADABLE = "tracker_result_policy_unreadable"

# The in-force version read is bounded: past it admission fails rather than
# waiting on a stalled database.
SELECT_DEADLINE_S = 10.0

_KINDS_FINDINGS_ONLY = (ResultKind.FINDINGS.value,)
_KINDS_WITH_TICKET = (ResultKind.FINDINGS.value, ResultKind.TICKET.value)


@dataclass(frozen=True)
class CompletionContract:
    """The contract an admitted request runs under.

    ``result_policy_digest``, ``result_kinds`` and ``implementation_label``
    describe the deployed policy for ``tracker_result`` and are empty for
    ``implementation``.
    """

    contract: CompletionContractName
    result_policy_digest: str | None = None
    result_kinds: tuple[str, ...] = ()
    implementation_label: str | None = None

    @property
    def is_tracker_result(self) -> bool:
        return self.contract == CONTRACT_TRACKER_RESULT


IMPLEMENTATION = CompletionContract(CONTRACT_IMPLEMENTATION)


@dataclass(frozen=True)
class ContractRefused:
    """Admission refuses: the binding cannot finish a tracker result here."""

    code: str


def _refuse_request(request: httpx.Request) -> httpx.Response:
    raise RuntimeError("a capability read sends no request")


def tracker_capabilities(
    registry: ForgeRegistry, tracker_kind: str, tracker_host: str
) -> Mapping[Operation, Support] | None:
    """What the configured tracker of ``tracker_kind`` on ``tracker_host`` declares.

    Read from the adapter the registry builds for it, the same object delivery
    checks (`factory_tracker_result_delivery`), built over a transport that
    refuses every request: constructing an adapter makes none. None when no
    configured tracker matches, or no binding scopes a binding scoped one.
    """

    entry = next(
        (
            tracker
            for tracker in registry.config.trackers
            if tracker.kind == tracker_kind and tracker.host == tracker_host
        ),
        None,
    )
    if entry is None:
        return None
    connection = registry.tracker(entry.name)
    client = httpx.AsyncClient(transport=httpx.MockTransport(_refuse_request))
    if isinstance(connection, RepositoryTrackerConnection):

        async def no_token() -> str:
            raise RuntimeError("a capability read mints no token")

        tracker = connection.for_repository(
            client, repo_full_name="capability/read", repository_id=0, token=no_token
        )
        return dict(tracker.capabilities)
    if isinstance(connection, BindingTrackerConnection):
        binding = next(
            (item for item in registry.config.bindings if item.tracker == entry.name), None
        )
        if binding is None:
            return None
        return dict(connection.for_binding(client, binding).capabilities)
    return None


def tracker_creates_linked_issues(
    registry: ForgeRegistry, tracker_kind: str, tracker_host: str
) -> bool:
    """Whether that tracker supports `Operation.CREATE_LINKED_ISSUE`.

    Fails closed: an unknown tracker, or any error reading its declaration,
    is False.
    """

    try:
        capabilities = tracker_capabilities(registry, tracker_kind, tracker_host)
    except Exception:  # noqa: BLE001 - an unreadable declaration is unsupported
        logger.warning("tracker capability read failed for %s on %s", tracker_kind, tracker_host)
        return False
    return capabilities is not None and supports(capabilities, Operation.CREATE_LINKED_ISSUE)


def _stored_policy(value: Any) -> tuple[tuple[str, ...], str | None] | None:
    """The stored ``result_policy`` as (sorted kinds, label), or None when malformed."""

    if not isinstance(value, dict) or set(value) != {"results", "implementation_label"}:
        return None
    results, label = value["results"], value["implementation_label"]
    if not isinstance(results, list) or tuple(results) not in (
        _KINDS_FINDINGS_ONLY,
        _KINDS_WITH_TICKET,
    ):
        return None
    if (label is None) != (tuple(results) == _KINDS_FINDINGS_ONLY):
        return None
    if label is not None and not isinstance(label, str):
        return None
    return tuple(results), label


async def in_force_version(session: AsyncSession, agent_id: uuid.UUID) -> AgentVersion | None:
    """The agent's in-force deployed version: prod outranks dev, then the newest
    active deployment (the ranking ``routers/hook_wake.py`` applies)."""

    found: AgentVersion | None = await session.scalar(
        select(AgentVersion)
        .join(
            Deployment,
            (Deployment.version_id == AgentVersion.id)
            & (Deployment.agent_id == AgentVersion.agent_id),
        )
        .where(Deployment.agent_id == agent_id, Deployment.status == "active")
        .order_by(
            (Deployment.environment == Environment.prod).desc(),
            Deployment.deployed_at.desc(),
            Deployment.id.desc(),
        )
        .limit(1)
    )
    return found


async def select_completion_contract(
    session: AsyncSession,
    *,
    agent_id: uuid.UUID,
    tracker_kind: str,
    tracker_host: str,
    registry: ForgeRegistry,
) -> CompletionContract | ContractRefused:
    """The contract a request admitted for ``agent_id`` on that tracker runs under.

    ``implementation`` when the in-force version declares no result policy
    (or the agent has no active deployment). ``tracker_result`` with the
    version's digest otherwise, unless the policy allows a ticket and the
    WorkItem's tracker cannot create a linked issue, or the stored policy
    cannot be read: then a `ContractRefused`, which admission treats as a
    label no binding serves. The read is bounded by ``SELECT_DEADLINE_S``; a
    database error or the deadline propagates, so nothing is admitted.
    """

    async with asyncio.timeout(SELECT_DEADLINE_S):
        version = await in_force_version(session, agent_id)
    if version is None or version.result_policy_digest is None:
        return IMPLEMENTATION
    policy = _stored_policy(version.result_policy)
    if policy is None:
        return ContractRefused(REFUSAL_POLICY_UNREADABLE)
    kinds, label = policy
    if ResultKind.TICKET.value in kinds and not tracker_creates_linked_issues(
        registry, tracker_kind, tracker_host
    ):
        return ContractRefused(REFUSAL_UNSUPPORTED)
    return CompletionContract(
        CONTRACT_TRACKER_RESULT,
        result_policy_digest=version.result_policy_digest,
        result_kinds=kinds,
        implementation_label=label,
    )


# --- runtime (S2b) ---
#
# The runner-facing half (ADR 0277 decisions 4, 5 and 8). The issue read
# capability authorizes two sibling routes (`routers/work_item_issue.py`):
# ``/work-items/completion-contract`` reserves the request's one model session
# and mints its grant (`reserve_session`), and ``/work-items/tracker-result``
# seals one submitted result (`seal_submission`) and delivers it through
# `curie_api.factory_tracker_result_delivery`. Imports sit here, below the
# admission section, so the two halves stay separately owned.

import hashlib  # noqa: E402
from datetime import datetime, timedelta  # noqa: E402

from sqlalchemy import func  # noqa: E402
from sqlalchemy.exc import IntegrityError  # noqa: E402

from .config import Settings  # noqa: E402
from .factory_tracker_result_delivery import (  # noqa: E402
    RefusalReason,
    TrackerResultRefused,
    output_identity_for,
    source_fingerprint,
    submit_result,
)
from .factory_tracker_results import ResultProposal  # noqa: E402
from .forges.errors import ForgeError  # noqa: E402
from .forges.ports import Tracker  # noqa: E402
from .forges.registry import forges_registry  # noqa: E402
from .forges.trackers import tracker_for_work_item  # noqa: E402
from .forges.types import GITHUB, SourceContent, TrackerIssueRef  # noqa: E402
from .github_app import GitHubAppError  # noqa: E402
from .models import (  # noqa: E402
    DEFAULT_EXECUTION_DEADLINE_SECONDS,
    Agent,
    ExecutionRequest,
    FactoryTrackerResultDelivery,
    FactoryTrackerResultGrant,
    FactoryTrackerResultReceipt,
    WorkItem,
)
from .tracker_result_declaration import canonical_json  # noqa: E402

# One source read (title and body) for the content fingerprint.
SOURCE_READ_TIMEOUT_SECONDS = 20.0
_PROVIDER_TIMEOUT_SECONDS = 5.0
# Statuses that may still become, or are, the request's one session.
_RESERVABLE_STATUSES = ("queued", "waiting", "running")

# Refusal codes the runner-facing routes answer with 409 (besides every
# `RefusalReason` value).
INVALID_CONTEXT = "invalid_context"
SESSION_ALREADY_STARTED = "session_already_started"
SESSION_NOT_STARTED = "session_not_started"
NOT_TRACKER_RESULT = "not_tracker_result"
RESULT_ALREADY_SUBMITTED = "result_already_submitted"


async def _database_now(session: AsyncSession) -> datetime:
    now = await session.scalar(select(func.clock_timestamp()))
    assert isinstance(now, datetime)
    return now


def source_content_sha256(content: SourceContent) -> str:
    """SHA-256 of ``canonical_json({"title": ..., "body": ...})``.

    The one content fingerprint helper: the reservation records it on the
    grant, a submission and every delivery attempt compare against it.
    """

    return hashlib.sha256(
        canonical_json({"title": content.title, "body": content.body})
    ).hexdigest()


class SourceUnavailable(RuntimeError):
    """The tracker could not answer the source read."""


def work_item_tracker(
    settings: Settings, client: httpx.AsyncClient, work_item: WorkItem
) -> Tracker:
    """The tracker the WorkItem's issue lives on, with a capped provider timeout.

    The one place the runner-facing routes and the delivery pass resolve it,
    so a GitHub issue is read and written only with a token for its recorded
    installation. Raises the forge errors of `tracker_for_work_item`.
    """

    return tracker_for_work_item(
        forges_registry(settings),
        client,
        work_item.tracker_issue,
        work_item.repository,
        work_item.code_host_installation_id,
        timeout_s=_PROVIDER_TIMEOUT_SECONDS,
    )


async def read_source_sha256(
    settings: Settings, client: httpx.AsyncClient, work_item: WorkItem
) -> tuple[Tracker, str]:
    """The WorkItem's tracker and the content digest of its issue now.

    Every failure, a configuration one included, is `SourceUnavailable`.
    """

    try:
        tracker = work_item_tracker(settings, client, work_item)
        async with asyncio.timeout(SOURCE_READ_TIMEOUT_SECONDS):
            content = await tracker.read_source_content(work_item.tracker_issue)
    except (GitHubAppError, ForgeError, ValueError, TimeoutError):
        raise SourceUnavailable from None
    return tracker, source_content_sha256(content)


class RuntimeRefused(Exception):
    """A runner-facing route refuses with ``code``; nothing was written."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"tracker result route refused: {code}")


@dataclass(frozen=True)
class ContractView:
    """What ``/work-items/completion-contract`` answers."""

    completion_contract: CompletionContractName
    result_kinds: tuple[str, ...]
    session_usd_allowance: float | None


_IMPLEMENTATION_VIEW = ContractView(CONTRACT_IMPLEMENTATION, (), None)


@dataclass(frozen=True)
class RuntimePair:
    work_item: WorkItem
    request: ExecutionRequest


async def load_pair(
    session: AsyncSession, request_id: uuid.UUID, *, lock: bool = False
) -> RuntimePair | None:
    """The request and its WorkItem; ``lock`` takes the request row lock."""

    query = (
        select(WorkItem, ExecutionRequest)
        .select_from(ExecutionRequest)
        .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
        .where(ExecutionRequest.id == request_id)
        .execution_options(populate_existing=True)
    )
    if lock:
        query = query.with_for_update(of=ExecutionRequest)
    row = (await session.execute(query)).one_or_none()
    if row is None:
        return None
    return RuntimePair(work_item=row[0], request=row[1])


def names_issue(pair: RuntimePair, issue: TrackerIssueRef) -> bool:
    """Whether ``issue`` (the capability's) is the durable WorkItem's issue,
    and the WorkItem can be read at all (a GitHub issue only with its
    recorded installation)."""

    item = pair.work_item
    if item.tracker_kind == GITHUB and item.code_host_installation_id is None:
        return False
    stored = item.tracker_issue
    return (stored.kind, stored.host, stored.scope_id, stored.issue_id) == (
        issue.kind,
        issue.host,
        issue.scope_id,
        issue.issue_id,
    )


def contract_of(request: ExecutionRequest) -> CompletionContractName:
    return (
        CONTRACT_TRACKER_RESULT
        if request.completion_contract == CONTRACT_TRACKER_RESULT
        else CONTRACT_IMPLEMENTATION
    )


def _reservable(pair: RuntimePair, now: datetime) -> bool:
    request = pair.request
    return (
        pair.work_item.cancelled_at is None
        and request.status in _RESERVABLE_STATUSES
        and (request.execution_deadline is None or request.execution_deadline > now)
    )


async def contract_precheck(session: AsyncSession, pair: RuntimePair) -> ContractView | None:
    """The implementation answer (no reservation, no grant), or None when the
    request is a tracker result request that still needs `reserve_session`.

    Refuses `SESSION_ALREADY_STARTED` when the session is already reserved,
    and `INVALID_CONTEXT` when the request can no longer run.
    """

    now = await _database_now(session)
    if not _reservable(pair, now):
        raise RuntimeRefused(INVALID_CONTEXT)
    if contract_of(pair.request) == CONTRACT_IMPLEMENTATION:
        return _IMPLEMENTATION_VIEW
    if pair.request.model_session_reserved_at is not None:
        raise RuntimeRefused(SESSION_ALREADY_STARTED)
    return None


async def reserve_session(
    session: AsyncSession,
    *,
    request_id: uuid.UUID,
    content_sha256: str,
    settings: Settings,
) -> ContractView:
    """Reserve the tracker result request's one model session and mint its grant.

    Under the request row lock, in one committed transaction: refuses
    `SESSION_ALREADY_STARTED` when the session is reserved (or a grant
    exists), `INVALID_CONTEXT` when the request can no longer run or its
    deployed policy cannot be read; otherwise inserts the grant (the content
    digest read before the lock, the frozen admission fingerprint, the
    version's kinds and implementation label, the request's output identity,
    and an expiry at the execution deadline plus the delivery deadline,
    provisional until the start transition sets it) and stamps
    ``model_session_reserved_at``. Every refusal rolls back.
    """

    pair = await load_pair(session, request_id, lock=True)
    try:
        if pair is None or contract_of(pair.request) != CONTRACT_TRACKER_RESULT:
            raise RuntimeRefused(INVALID_CONTEXT)
        work_item, request = pair.work_item, pair.request
        now = await _database_now(session)
        if not _reservable(pair, now):
            raise RuntimeRefused(INVALID_CONTEXT)
        granted = await session.scalar(
            select(FactoryTrackerResultGrant.id).where(
                FactoryTrackerResultGrant.execution_request_id == request.id
            )
        )
        if request.model_session_reserved_at is not None or granted is not None:
            raise RuntimeRefused(SESSION_ALREADY_STARTED)
        version = await session.scalar(
            select(AgentVersion)
            .where(
                AgentVersion.agent_id == work_item.agent_id,
                AgentVersion.result_policy_digest == request.result_policy_digest,
            )
            .order_by(AgentVersion.created_at.desc())
            .limit(1)
        )
        policy = None if version is None else _stored_policy(version.result_policy)
        agent = await session.get(Agent, work_item.agent_id)
        if policy is None or agent is None:
            raise RuntimeRefused(INVALID_CONTEXT)
    except RuntimeRefused:
        await session.rollback()
        raise
    kinds, label = policy
    delivery = timedelta(seconds=settings.factory_tracker_result_delivery_deadline_seconds)
    if request.execution_deadline is not None:
        expires_at = request.execution_deadline + delivery
    else:
        seconds = agent.execution_deadline_seconds or DEFAULT_EXECUTION_DEADLINE_SECONDS
        expires_at = now + timedelta(seconds=seconds) + delivery
    session.add(
        FactoryTrackerResultGrant(
            id=uuid.uuid4(),
            work_item_id=work_item.id,
            execution_request_id=request.id,
            source_fingerprint=source_fingerprint(work_item, request),
            source_content_sha256=content_sha256,
            repository_id=work_item.repository_project_id,
            allowed_result_kinds=sorted(kinds),
            output_identity=output_identity_for(request.id),
            implementation_label=label,
            expires_at=expires_at,
        )
    )
    request.model_session_reserved_at = now
    allowance = (
        float(agent.max_usd_per_day)
        if agent.max_usd_per_day is not None
        else DEFAULT_TRIAGE_SESSION_USD
    )
    try:
        await session.commit()
    except IntegrityError:
        # A concurrent reservation inserted the grant first.
        await session.rollback()
        raise RuntimeRefused(SESSION_ALREADY_STARTED) from None
    logger.info("tracker result session reserved request=%s kinds=%s", request_id, kinds)
    return ContractView(CONTRACT_TRACKER_RESULT, tuple(sorted(kinds)), allowance)


@dataclass(frozen=True)
class SubmissionTarget:
    """What a submission is checked against before any tracker read."""

    grant_id: uuid.UUID
    source_content_sha256: str | None


async def submission_precheck(session: AsyncSession, pair: RuntimePair) -> SubmissionTarget:
    """The cheap refusals, before any tracker read; read-only.

    `NOT_TRACKER_RESULT` for an implementation request, `SESSION_NOT_STARTED`
    before the reservation, ``grant_not_found`` without a grant, and
    `RESULT_ALREADY_SUBMITTED` when a delivery exists for the grant.
    """

    request = pair.request
    if contract_of(request) != CONTRACT_TRACKER_RESULT:
        raise RuntimeRefused(NOT_TRACKER_RESULT)
    if request.model_session_reserved_at is None:
        raise RuntimeRefused(SESSION_NOT_STARTED)
    grant = await session.scalar(
        select(FactoryTrackerResultGrant).where(
            FactoryTrackerResultGrant.execution_request_id == request.id
        )
    )
    if grant is None:
        raise RuntimeRefused("grant_not_found")
    submitted = await session.scalar(
        select(FactoryTrackerResultDelivery.id).where(
            FactoryTrackerResultDelivery.grant_id == grant.id
        )
    )
    if submitted is not None:
        raise RuntimeRefused(RESULT_ALREADY_SUBMITTED)
    return SubmissionTarget(grant_id=grant.id, source_content_sha256=grant.source_content_sha256)


async def seal_submission(
    session: AsyncSession,
    *,
    request_id: uuid.UUID,
    target: SubmissionTarget,
    proposal: ResultProposal,
    tracker: Tracker,
    settings: Settings,
) -> uuid.UUID:
    """Seal the first accepted submission for the request's grant.

    Takes the request row lock, then the grant row lock (the order the
    lifecycle takes them when it seals exhaustion findings), refuses
    `RESULT_ALREADY_SUBMITTED` when a delivery already exists, and holds both
    through `submit_result` under the request's current runtime epoch, so a
    concurrent submission or exhaustion seal sees this one. Raises
    `RuntimeRefused` or `TrackerResultRefused`; returns the delivery id.
    """

    pair = await load_pair(session, request_id, lock=True)
    if pair is None:
        await session.rollback()
        raise RuntimeRefused(INVALID_CONTEXT)
    grant = await session.scalar(
        select(FactoryTrackerResultGrant)
        .where(FactoryTrackerResultGrant.id == target.grant_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if grant is None or grant.execution_request_id != request_id:
        await session.rollback()
        raise TrackerResultRefused(RefusalReason.GRANT_NOT_FOUND)
    submitted = await session.scalar(
        select(FactoryTrackerResultDelivery.id).where(
            FactoryTrackerResultDelivery.grant_id == grant.id
        )
    )
    if submitted is not None:
        await session.rollback()
        raise RuntimeRefused(RESULT_ALREADY_SUBMITTED)
    return await submit_result(
        session,
        grant_id=grant.id,
        execution_request_id=request_id,
        runtime_epoch=pair.request.runtime_epoch,
        proposal=proposal,
        tracker=tracker,
        settings=settings,
    )


@dataclass(frozen=True)
class SubmissionAnswer:
    """What ``/work-items/tracker-result`` answers after its inline attempt."""

    result_kind: str
    state: str
    result_comment_url: str | None
    ticket_url: str | None


async def submission_answer(session: AsyncSession, delivery_id: uuid.UUID) -> SubmissionAnswer:
    """The delivery's state now, with its links once delivered."""

    delivery = await session.scalar(
        select(FactoryTrackerResultDelivery)
        .where(FactoryTrackerResultDelivery.id == delivery_id)
        .execution_options(populate_existing=True)
    )
    assert delivery is not None
    receipt = await session.scalar(
        select(FactoryTrackerResultReceipt).where(
            FactoryTrackerResultReceipt.delivery_id == delivery_id
        )
    )
    answer = SubmissionAnswer(
        result_kind=delivery.result_kind,
        state=delivery.state,
        result_comment_url=receipt.result_comment_url if receipt is not None else None,
        ticket_url=receipt.output_url if receipt is not None else None,
    )
    await session.rollback()
    return answer
