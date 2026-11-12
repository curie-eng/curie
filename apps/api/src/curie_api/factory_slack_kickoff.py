"""A Slack mention that links one issue starts a personal factory run (#5380, ADR 0268).

The worker hands over an ``app_mention`` at a channel root, or in a thread no
factory run maps, whose text links an issue, with the dispatcher's verified
mention receipt. This module decides, in order, and answers a named outcome at
the first check that fails:

1. The receipt names the same delivery, route and sender as the turn
   (``receipt_mismatch``).
2. A request this delivery already committed, with its kickoff thread at this
   root, answers ``admitted`` and ``replayed`` without judging current
   authority again: it was admitted under the authority valid then.
3. A version 2 receipt names an operator-declared test driver (ADR 0202) of
   this API's own test installation, by bot id, bot user and channel
   (``test_driver_refused``).
4. ``not_factory`` when the factory is off or no GitHub tracker or code host
   is configured. The worker then runs the mention as ordinary chat, and so
   it does for the two later ``not_factory`` answers.
5. The receiving Slack binding is the channel identity's own and the agent's
   only one at the channel, in the agent's tenant (``binding_missing``,
   ``binding_ambiguous``). An agent holding no GitHub surface is
   ``not_factory``.
6. The receipt's body links exactly one GitHub issue on the factory's code
   host (``link_missing``, ``link_multiple``, ``link_malformed``,
   ``link_host_unsupported``, ``link_pull_request``), and one of the agent's
   GitHub surfaces serves its repository, matched casefold (``not_factory``
   otherwise). The surface's address is the repository path from then on.
7. The sender resolves to an active principal in the binding's tenant
   (`identity.service.resolve_principal`, ``sender_unverified``).
8. The principal's personal enrollment header is active at a positive
   generation (``enrollment_inactive``). Read unlocked: admission validates
   it again under its own row locks.
9. No factory thread is mapped at this root (``thread_conflict``).

Then, outside any transaction, GitHub is read: the installation token, the
repository (its immutable id and default branch), the issue, the issue's
WorkItem and, for a new WorkItem, its base (`factory_base.resolve_base`,
``base_refused``) and the code host's own earlier publications for the issue
(`factory_rediscovery.rediscover`, ``publication_exists``; no tracker comment
is written). One transaction then holds the identity reader lock and the
issue lock, reads the issue from GitHub again (the decisive read: it exists,
``issue_inaccessible``; it is no pull request; it is open, ``issue_closed``),
resolves the sender again and re-reads the binding (``authority_changed``),
re-checks the root and the issue's thread, and admits through
`workitem_dispatch.admit` with ``model_auth_mode="personal"``, the sender as
owner at the generation read above, the resolved base, no trigger label, and
a request id derived from the delivery. A refusal there answers
``admission_refused`` with its code, except that an inactive or replaced
enrollment answers ``enrollment_inactive``. The same transaction inserts the
WorkItem's ``factory_slack_threads`` row with origin ``kickoff`` at this root
and reads it back; any difference rolls the whole admission back
(``thread_conflict``).

A code host that cannot answer now raises `CodeHostUnavailable` (503), so the
worker keeps the delivery pending. Logs carry codes and ids only, never the
body.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlsplit

import httpx
from curie_internal.issue_links import IssueLink, find_issue_links
from curie_internal.slack_mention_receipt import SlackMentionReceipt, kickoff_request_id
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from . import factory_rediscovery, identity_fence, workitem_dispatch
from .config import Settings
from .crud import channels as crud_channels
from .factory_base import BaseRefusal, ResolvedBase, bases_for, choose_base, resolve_base
from .factory_slack import CodeHostUnavailable
from .forges.errors import ForgeError, NotFound
from .forges.hosts import (
    factory_code_host,
    factory_code_host_entry,
    factory_credentials,
    factory_repository,
    factory_tracker,
    repository_ref,
    resolve_stored,
)
from .forges.registry import ForgeRegistry, forges_registry
from .forges.surfaces import SURFACE_KIND, resolve_surface
from .forges.trackers import issue_link, reply_route, work_item_policy
from .forges.types import GITHUB, RepositoryRef, TrackerIssueRef
from .github_app import GitHubAppError, GitHubInstallationRefused
from .github_factory import lock_issue, work_item_for
from .github_review_events import FeedbackUnavailable
from .identity.service import resolve_principal
from .identity.slack import SlackEvidence
from .models import (
    Agent,
    AgentChannel,
    ExecutionRequest,
    FactorySlackThread,
    PersonalClaudeEnrollment,
)
from .personal_execution import OwnerContext
from .schemas.workitems import (
    FactorySlackKickoffIn,
    FactorySlackKickoffOut,
    FactorySlackKickoffOutcome,
)
from .workitem_dispatch import Admission
from .workitems.lifecycle import WorkItemOutcome

logger = logging.getLogger(__name__)

# The bound on each issue read from GitHub, the decisive one under the issue
# lock included: an issue read never holds the lock longer than this.
ISSUE_READ_SECONDS = 10.0
# Admission refusals that mean the owner's enrollment changed after it was read.
_ENROLLMENT_CODES = frozenset({"enrollment_inactive", "enrollment_generation_mismatch"})
_ORIGIN = "kickoff"
# The agent surface kind that serves a GitHub repository's issues.
_GITHUB_SURFACE = SURFACE_KIND[GITHUB]


class _IssueNow(Protocol):
    """One issue as its tracker answers it now: its state, its label names, and
    whether the number is a pull request's."""

    @property
    def state(self) -> str | None: ...

    @property
    def labels(self) -> frozenset[str]: ...

    @property
    def pull_request(self) -> bool: ...


class _IssueReader(Protocol):
    """The factory tracker's issue read on one repository (#5380)."""

    async def issue_state(self, number: int) -> _IssueNow: ...


@dataclass(frozen=True)
class _Receiving:
    """The receiving Slack binding, its agent and tenant, and the agent's
    GitHub surface addresses, as read; compared again under the locks."""

    binding_id: uuid.UUID
    agent_id: uuid.UUID
    tenant_id: uuid.UUID
    surfaces: tuple[str, ...]


@dataclass(frozen=True)
class _Target:
    """What the reads outside the transaction established about the issue."""

    path: str
    number: int
    installation_id: int
    repository: RepositoryRef
    issue: TrackerIssueRef
    issue_url: str
    default_branch: str
    tracker: _IssueReader
    # The issue's WorkItem as read; None for a new one.
    existing: uuid.UUID | None
    # The base a new WorkItem starts from; None for an existing one.
    base: ResolvedBase | None


def _refused(
    outcome: FactorySlackKickoffOutcome, *, reason: str | None = None
) -> FactorySlackKickoffOut:
    return FactorySlackKickoffOut(outcome=outcome, reason=reason)


def _kickoff_receipt(
    data: FactorySlackKickoffIn,
) -> tuple[SlackMentionReceipt, SlackEvidence] | None:
    """The receipt and its sender evidence, when both name exactly this delivery."""

    try:
        receipt = SlackMentionReceipt.model_validate(data.receipt)
        evidence = SlackEvidence.model_validate(receipt.evidence)
    except ValidationError:
        return None
    if (
        receipt.delivery_id != data.delivery_id
        or receipt.channel_identity != data.channel_identity
        or receipt.channel != data.channel
        or receipt.root_ts != data.root_ts
        or receipt.user_id != data.author
        or evidence.user_id != receipt.user_id
        or not receipt.message_ts
    ):
        return None
    return receipt, evidence


def _driver_declared(settings: Settings, receipt: SlackMentionReceipt) -> bool:
    """A person's receipt, or a declared driver's on this API's test installation."""

    if receipt.version == 1:
        return True
    if not settings.test_installation_enabled:
        return False
    return any(
        driver.bot_id == receipt.driver_bot_id
        and driver.bot_user_id == receipt.user_id
        and driver.channel_id == receipt.channel
        for driver in settings.test_installation_drivers
    )


def _factory_serves(settings: Settings, registry: ForgeRegistry) -> bool:
    return (
        settings.factory_enabled
        and registry.github_tracker() is not None
        and registry.github_code_host() is not None
    )


async def _thread_at_root(
    session: AsyncSession, data: FactorySlackKickoffIn
) -> FactorySlackThread | None:
    found: FactorySlackThread | None = await session.scalar(
        select(FactorySlackThread)
        .where(
            FactorySlackThread.channel_identity == data.channel_identity,
            FactorySlackThread.channel == data.channel,
            FactorySlackThread.root_ts == data.root_ts,
        )
        .execution_options(populate_existing=True)
    )
    return found


async def _thread_of(session: AsyncSession, work_item_id: uuid.UUID) -> FactorySlackThread | None:
    found: FactorySlackThread | None = await session.scalar(
        select(FactorySlackThread)
        .where(FactorySlackThread.work_item_id == work_item_id)
        .execution_options(populate_existing=True)
    )
    return found


def _is_this_root(thread: FactorySlackThread, data: FactorySlackKickoffIn) -> bool:
    return (
        thread.origin == _ORIGIN
        and thread.channel_identity == data.channel_identity
        and thread.channel == data.channel
        and thread.root_ts == data.root_ts
    )


async def _replay(
    session: AsyncSession, data: FactorySlackKickoffIn, request_id: uuid.UUID
) -> FactorySlackKickoffOut | None:
    """The answer for this delivery's committed request, or None when there is none.

    The request id is this delivery's own, so a row with it is the admission an
    earlier attempt committed together with its kickoff thread at this root.
    """

    request = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.id == request_id)
        .execution_options(populate_existing=True)
    )
    if request is None:
        return None
    thread = await _thread_of(session, request.work_item_id)
    if thread is None or not _is_this_root(thread, data):
        return _refused("thread_conflict")
    return FactorySlackKickoffOut(
        outcome="admitted",
        request_id=str(request.id),
        request_status=request.status,
        work_item_id=str(request.work_item_id),
        issue_url=request.objective,
        replayed=True,
    )


async def _receiving(
    session: AsyncSession, data: FactorySlackKickoffIn
) -> _Receiving | FactorySlackKickoffOut:
    """The receiving binding, as progress delivery requires it: the channel
    identity's own route, and the agent's only identity-bearing Slack binding
    at the channel, in the agent's tenant."""

    try:
        binding = await crud_channels.binding_for_route(
            session, "slack", data.channel_identity, data.channel
        )
    except crud_channels.AmbiguousRoute:
        return _refused("binding_ambiguous")
    if binding is None or binding.adapter != data.channel_identity:
        return _refused("binding_missing")
    binding_id, agent_id, binding_tenant = binding.id, binding.agent_id, binding.tenant_id
    agent_tenant = await session.scalar(select(Agent.tenant_id).where(Agent.id == agent_id))
    if agent_tenant is None or agent_tenant != binding_tenant:
        return _refused("binding_missing")
    routes = list(
        await session.scalars(
            select(AgentChannel.id).where(
                AgentChannel.agent_id == agent_id,
                AgentChannel.kind == "slack",
                AgentChannel.address == data.channel,
                AgentChannel.adapter.is_not(None),
            )
        )
    )
    if routes != [binding_id]:
        return _refused("binding_ambiguous")
    surfaces = await session.scalars(
        select(AgentChannel.address)
        .where(AgentChannel.agent_id == agent_id, AgentChannel.kind == _GITHUB_SURFACE)
        .order_by(AgentChannel.address)
    )
    return _Receiving(binding_id, agent_id, agent_tenant, tuple(surfaces))


def _one_link(
    registry: ForgeRegistry, receipt: SlackMentionReceipt
) -> tuple[IssueLink, int] | FactorySlackKickoffOut:
    """The one issue the receipt's body links, with its number."""

    links = find_issue_links(receipt.body)
    if not links:
        return _refused("link_missing")
    if len(links) > 1:
        return _refused("link_multiple")
    (link,) = links
    if link.kind == "malformed" or link.number is None:
        return _refused("link_malformed")
    host = urlsplit(factory_code_host_entry(registry).html_base).netloc.lower()
    if link.host != host:
        return _refused("link_host_unsupported")
    if link.kind != "issue":
        return _refused("link_pull_request")
    return link, link.number


def _served_path(
    registry: ForgeRegistry, receiving: _Receiving, link: IssueLink
) -> str | FactorySlackKickoffOut:
    """The agent's GitHub surface address serving the linked repository, matched
    casefold; from here on the bound address is the repository path."""

    wanted = link.path.casefold()
    matches = sorted({address for address in receiving.surfaces if address.casefold() == wanted})
    if not matches:
        return _refused("not_factory")
    if len(matches) > 1:
        return _refused("binding_ambiguous")
    (path,) = matches
    if resolve_surface(registry, _GITHUB_SURFACE, path).tracker is None:
        return _refused("not_factory")
    return path


async def _principal(
    session: AsyncSession,
    data: FactorySlackKickoffIn,
    tenant_id: uuid.UUID,
    evidence: SlackEvidence,
) -> uuid.UUID | FactorySlackKickoffOut:
    resolution = await resolve_principal(
        session,
        tenant_id=tenant_id,
        provider="slack",
        channel_identity=data.channel_identity,
        slack=evidence,
    )
    if resolution.status != "resolved" or resolution.principal_id is None:
        return _refused("sender_unverified", reason=resolution.reason)
    return resolution.principal_id


async def _enrolled_generation(
    session: AsyncSession, tenant_id: uuid.UUID, principal_id: uuid.UUID
) -> int | None:
    """The enrollment header's generation when it is active and positive. Unlocked."""

    row = (
        await session.execute(
            select(PersonalClaudeEnrollment.generation, PersonalClaudeEnrollment.active).where(
                PersonalClaudeEnrollment.tenant_id == tenant_id,
                PersonalClaudeEnrollment.principal_id == principal_id,
            )
        )
    ).one_or_none()
    if row is None or not row.active or int(row.generation) < 1:
        return None
    return int(row.generation)


async def _issue_state(tracker: _IssueReader, number: int) -> _IssueNow | None:
    """The issue as GitHub answers it now, None when it does not exist for this
    installation. Bounded by `ISSUE_READ_SECONDS`."""

    try:
        async with asyncio.timeout(ISSUE_READ_SECONDS):
            return await tracker.issue_state(number)
    except NotFound:
        return None
    except TimeoutError:
        raise CodeHostUnavailable("issue_timeout") from None


def _open_issue(state: _IssueNow | None) -> _IssueNow | FactorySlackKickoffOut:
    """``state`` when it is an open issue, else the refusal."""

    if state is None:
        return _refused("issue_inaccessible")
    if state.pull_request:
        return _refused("link_pull_request")
    if state.state != "open":
        return _refused("issue_closed")
    return state


async def _read_target(
    session: AsyncSession,
    registry: ForgeRegistry,
    settings: Settings,
    client: httpx.AsyncClient,
    path: str,
    number: int,
) -> _Target | FactorySlackKickoffOut:
    """Every GitHub read before the transaction. The caller holds no lock."""

    try:
        installation_id, token = await run_in_threadpool(
            factory_credentials(registry).fresh_installation_token, path, None
        )
    except (GitHubInstallationRefused, ValueError):
        return _refused("issue_inaccessible")
    except GitHubAppError:
        raise CodeHostUnavailable("installation_unavailable") from None
    # The factory's code host, fenced to the installation just verified.
    code_host = factory_code_host(registry, client, installations={path: installation_id})
    try:
        current = await resolve_stored(
            code_host, repository_ref(settings, path=path, project_id=None)
        )
    except ForgeError:
        # A 404 can conceal a missing App permission, so no answer here proves
        # the repository is gone: the read is unavailable, not refused.
        raise CodeHostUnavailable("repository_unavailable") from None
    if current.path.casefold() != path.casefold() or not current.project_id.isdigit():
        return _refused("issue_inaccessible")
    default_branch = current.default_branch
    if not default_branch:
        raise CodeHostUnavailable("repository_unavailable")
    repository_id = int(current.project_id)

    async def minted() -> str:
        return token

    tracker_entry = factory_tracker(registry)
    tracker = tracker_entry.for_repository(
        client, repo_full_name=path, repository_id=repository_id, token=minted
    )
    issue = TrackerIssueRef(GITHUB, tracker_entry.config.host, str(repository_id), str(number))
    repository = factory_repository(registry, path=path, project_id=repository_id)
    state = _open_issue(await _issue_state(tracker, number))
    if isinstance(state, FactorySlackKickoffOut):
        return state
    item = await work_item_for(session, issue)
    existing = None if item is None else item.id
    thread = None if existing is None else await _thread_of(session, existing)
    await session.rollback()
    if thread is not None:
        return _refused("thread_conflict")
    base: ResolvedBase | None = None
    if existing is None:
        resolved = await resolve_base(
            policy=work_item_policy(registry, issue, repository),
            labels=set(state.labels),
            default_branch=default_branch,
            code_host=code_host,
            repository=repository,
        )
        if isinstance(resolved, BaseRefusal):
            return _refused("base_refused", reason=resolved.code)
        base = resolved
        found = await factory_rediscovery.rediscover(
            tracker,
            factory_rediscovery.publishing_code_host(settings, client, repository),
            issue,
            repository,
            event_at=None,
        )
        if found.verdict != "clear":
            return _refused("publication_exists")
    return _Target(
        path=path,
        number=number,
        installation_id=installation_id,
        repository=repository,
        issue=issue,
        issue_url=issue_link(registry, issue, repository),
        default_branch=default_branch,
        tracker=tracker,
        existing=existing,
        base=base,
    )


async def kickoff(
    session: AsyncSession,
    data: FactorySlackKickoffIn,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> FactorySlackKickoffOut:
    """Decide one issue-linking mention; admit a personal factory run when allowed.

    Raises `CodeHostUnavailable` when the code host cannot answer now.
    """

    try:
        decision = await _decide(session, data, settings=settings, client=client)
    finally:
        if session.in_transaction():
            await session.rollback()
    logger.info(
        "factory slack kickoff delivery %s: %s%s",
        data.delivery_id,
        decision.outcome,
        "" if decision.reason is None else f" ({decision.reason})",
    )
    return decision


async def _decide(
    session: AsyncSession,
    data: FactorySlackKickoffIn,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> FactorySlackKickoffOut:
    checked = _kickoff_receipt(data)
    if checked is None:
        return _refused("receipt_mismatch")
    receipt, evidence = checked
    request_id = kickoff_request_id(data.channel_identity, data.delivery_id)
    replayed = await _replay(session, data, request_id)
    if replayed is not None:
        return replayed
    if not _driver_declared(settings, receipt):
        return _refused("test_driver_refused")
    registry = forges_registry(settings)
    if not _factory_serves(settings, registry):
        return _refused("not_factory")
    receiving = await _receiving(session, data)
    if isinstance(receiving, FactorySlackKickoffOut):
        return receiving
    if not receiving.surfaces:
        return _refused("not_factory")
    linked = _one_link(registry, receipt)
    if isinstance(linked, FactorySlackKickoffOut):
        return linked
    link, number = linked
    path = _served_path(registry, receiving, link)
    if isinstance(path, FactorySlackKickoffOut):
        return path
    principal_id = await _principal(session, data, receiving.tenant_id, evidence)
    if isinstance(principal_id, FactorySlackKickoffOut):
        return principal_id
    generation = await _enrolled_generation(session, receiving.tenant_id, principal_id)
    if generation is None:
        return _refused("enrollment_inactive")
    if await _thread_at_root(session, data) is not None:
        return _refused("thread_conflict")
    # End the read transaction: no snapshot or lock is held across provider reads.
    await session.rollback()

    try:
        target = await _read_target(session, registry, settings, client, path, number)
        if isinstance(target, FactorySlackKickoffOut):
            return target
        return await _admit(
            session,
            registry,
            data,
            evidence,
            receiving,
            target,
            OwnerContext(receiving.tenant_id, principal_id, generation),
            request_id,
            requester=f"slack:{receipt.user_id}",
            settings=settings,
        )
    except FeedbackUnavailable as exc:
        raise CodeHostUnavailable(exc.code) from None
    except ForgeError as exc:
        raise CodeHostUnavailable(type(exc).__name__) from None
    except httpx.HTTPError:
        raise CodeHostUnavailable("transport") from None


async def _admit(
    session: AsyncSession,
    registry: ForgeRegistry,
    data: FactorySlackKickoffIn,
    evidence: SlackEvidence,
    receiving: _Receiving,
    target: _Target,
    owner: OwnerContext,
    request_id: uuid.UUID,
    *,
    requester: str,
    settings: Settings,
) -> FactorySlackKickoffOut:
    """Under the identity reader and issue locks, read the issue, re-check and admit.

    The enrollment is never locked here before admission locks the WorkItem:
    `workitem_dispatch.admit` validates the owner's tenant, principal and
    enrollment ``FOR SHARE`` in the order disconnect locks them
    (`personal_enrollments._lock_owner`), so a racing disconnect either
    commits first and this admission is refused, or waits and then fences the
    request this admission commits.
    """

    await identity_fence.lock_identity_reader(session)
    await lock_issue(session, target.issue)
    # The decisive read: a close handled while the reads above ran has been
    # stored by now, and none can be stored until this transaction ends.
    state = _open_issue(await _issue_state(target.tracker, target.number))
    if isinstance(state, FactorySlackKickoffOut):
        return state
    replayed = await _replay(session, data, request_id)
    if replayed is not None:
        return replayed
    current = await _principal(session, data, receiving.tenant_id, evidence)
    if current != owner.principal_id:
        return _refused("authority_changed")
    now = await _receiving(session, data)
    if (
        not isinstance(now, _Receiving)
        or (now.binding_id, now.agent_id, now.tenant_id)
        != (receiving.binding_id, receiving.agent_id, receiving.tenant_id)
        or target.path not in now.surfaces
    ):
        return _refused("authority_changed")
    if await _thread_at_root(session, data) is not None:
        return _refused("thread_conflict")
    item = await work_item_for(session, target.issue)
    if item is not None and await _thread_of(session, item.id) is not None:
        return _refused("thread_conflict")
    base = None if item is not None else target.base
    if item is None:
        if base is None:
            # The WorkItem read before the lock is gone; read everything again.
            raise CodeHostUnavailable("issue_changed")
        policy = work_item_policy(registry, target.issue, target.repository)
        choice = choose_base(set(state.labels), bases_for(policy), target.default_branch)
        if isinstance(choice, BaseRefusal):
            return _refused("base_refused", reason=choice.code)
        if (choice.branch, choice.source) != (base.branch, base.source):
            # The issue's base labels changed since the branch was read.
            raise CodeHostUnavailable("base_changed")
    kind, address, conversation = reply_route(registry, target.issue, target.path)
    facts = Admission(
        agent_id=receiving.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=conversation,
        issue=target.issue,
        repository=target.repository,
        code_host_installation_id=target.installation_id,
        objective=target.issue_url,
        requester=requester,
        request_id=request_id,
        base=base,
        trigger_label=None,
        model_auth_mode="personal",
        owner=owner,
    )
    # Dispatch commits inside a savepoint, so the identity reader and issue
    # locks stay held until the admission commits with its thread.
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as admission:
        result = await workitem_dispatch.admit(admission, facts)
        if (
            item is None
            and isinstance(result, WorkItemOutcome)
            and result.request is not None
            and result.request.id == request_id
        ):
            from .factory_base_reconcile import initialize_base_selection

            await initialize_base_selection(
                admission,
                item_id=result.work_item.id,
                labels=set(state.labels),
                default_branch=target.default_branch,
                settings=settings,
            )
    if not isinstance(result, WorkItemOutcome):
        await session.rollback()
        code = str(result.code)
        if code in _ENROLLMENT_CODES:
            return _refused("enrollment_inactive")
        return _refused("admission_refused", reason=code)
    request = result.request
    if request is None or request.id != request_id:
        # Admission answered with a request that is not this delivery's.
        await session.rollback()
        return _refused("admission_refused", reason="request_mismatch")
    work_item_id = result.work_item.id
    await session.execute(
        insert(FactorySlackThread)
        .values(
            work_item_id=work_item_id,
            agent_id=receiving.agent_id,
            channel_identity=data.channel_identity,
            channel=data.channel,
            root_ts=data.root_ts,
            first_sequence=request.sequence,
            origin=_ORIGIN,
        )
        .on_conflict_do_nothing()
    )
    by_item = await _thread_of(session, work_item_id)
    at_root = await _thread_at_root(session, data)
    if (
        by_item is None
        or at_root is None
        or by_item.work_item_id != at_root.work_item_id
        or not _is_this_root(by_item, data)
        or by_item.agent_id != receiving.agent_id
        or by_item.first_sequence != request.sequence
    ):
        await session.rollback()
        return _refused("thread_conflict")
    await session.commit()
    return FactorySlackKickoffOut(
        outcome="admitted",
        request_id=str(request.id),
        request_status=request.status,
        work_item_id=str(work_item_id),
        issue_url=target.issue_url,
        replayed=result.replayed,
    )
