"""A Slack mention in a factory run's root asks for one more revision of its pull request (#5414).

The worker hands over a threaded ``app_mention`` whose root it found in
``factory_slack_threads``, with the dispatcher's verified mention receipt. This
module decides, in order, and refuses with a named outcome at the first check
that fails:

1. The receipt names the same delivery, route and sender as the turn, and
   is a person's version 1 receipt for a thread reply: a root mention's
   receipt and a declared test driver's version 2 receipt, which kickoff
   reads (#5380), never steer.
2. The root is a factory run's root.
3. The sender resolves to an active principal in the receiving channel
   identity (`identity.service.resolve_principal`).
4. The WorkItem is not cancelled, its lineage is open, and the lineage has a
   pull request.
5. The configured code host is the lineage repository's own, by kind and
   host (``code_host_mismatch`` otherwise): for GitHub the registry's GitHub
   entry, whose host must equal the lineage's; for any other kind the entry
   that kind and host name.
6. The code host can name an account by its immutable id
   (`Operation.ACCOUNT_LOGIN`), and the principal holds exactly one active
   link in that host's ``github_host`` namespace.
7. Read now from the code host, outside any lock: the stored repository still
   has its immutable id (`forges.hosts.resolve_stored`) and the pull request
   read belongs to it (``repository_mismatch`` otherwise), the pull request is
   open on the lineage's branch, the account's current login, and the person's
   authority on the pull request (`forges.authority.may_act_on_feedback`).

Then one transaction holds the identity reader lock, repeats the sender and
link checks (any difference is ``authority_changed``), takes the issue lock,
re-reads the WorkItem and lineage, and admits a revision through
`workitem_dispatch.admit_revision` with the WorkItem's own tracker binding and
conversation, the pull request as the reply target, and a request id derived
from the delivery, so a redelivered mention admits once. The admission carries
the mode and owner the locked WorkItem calls for (#5378, ADR 0268): a personal
WorkItem names the sender as its owner at the sender's current enrollment
generation, so admission refuses anyone but its owner and a stale or inactive
enrollment; a service WorkItem carries the installation's mode and no owner,
as tracker ingress does, so a personal installation refuses it with
``personal_owner_required``. The revision copies the WorkItem's owner tuple,
never the sender's. A redelivery whose
facts no longer match the request an earlier attempt committed (the account's
login was renamed meanwhile, say) answers that committed request as admitted
and replayed, never a refusal: it was admitted under the authority valid then.
Logs carry codes and ids only, never the body.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any

import httpx
from curie_internal.slack_mention_receipt import SlackMentionReceipt
from pydantic import ValidationError
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from . import identity_fence, workitem_dispatch
from .config import Settings
from .factory_confirmed_steering import RefusedSteering
from .factory_follow_up import with_pull_request_follow_up_marker
from .forges import types
from .forges.authority import feedback_allowlist, may_act_on_feedback
from .forges.capabilities import Operation, supports
from .forges.errors import ForgeError, NotFound, Unavailable
from .forges.hosts import factory_code_host_entry, resolve_stored
from .forges.ports import CodeHost
from .forges.registry import ForgeRegistry, forges_registry
from .forges.trackers import binding_for_scope, reply_route
from .forges.types import (
    Actor,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
    TrackerIssueRef,
)
from .github_factory import lock_issue
from .identity.service import Resolution, resolve_principal
from .identity.slack import SlackEvidence
from .models import (
    DEFAULT_TENANT_ID,
    Agent,
    ChannelIdentity,
    ExecutionRequest,
    FactorySlackThread,
    IdentityLink,
    IdentityNamespace,
    PersonalClaudeEnrollment,
    Principal,
    ProviderInstallation,
    Tenant,
    ThreadPublicationLineage,
    WorkItem,
)
from .personal_execution import ModelAuthMode, OwnerContext, OwnerTuple
from .schemas.workitems import (
    FactorySlackSteerIn,
    FactorySlackSteerOut,
    FactorySlackSteerOutcome,
)
from .workitem_dispatch import Admission
from .workitems.lifecycle import WorkItemOutcome

logger = logging.getLogger(__name__)

# The ``identity_namespaces`` kind that holds code host accounts by host.
GITHUB_HOST_KIND = "github_host"
# `workitem_dispatch` refuses a longer objective.
MAX_OBJECTIVE = 65536


class CodeHostUnavailable(Exception):
    """The code host could not answer now; the delivery is retried."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class _Link:
    link_id: uuid.UUID
    account_id: str


@dataclass(frozen=True)
class _Run:
    """What the unlocked reads established, compared again under the locks."""

    work_item_id: uuid.UUID
    tracker_issue: TrackerIssueRef
    installation_id: int | None
    lineage_id: uuid.UUID
    repository: RepositoryRef
    branch: str
    pr_number: int
    pr_url: str


@dataclass(frozen=True, kw_only=True, repr=False)
class _VoiceSource:
    delivery_id: str
    channel_identity: str
    team_id: str
    api_app_id: str
    channel: str
    root_ts: str
    message_ts: str
    file_id: str
    content_sha256: str
    author: str
    evidence_json: str


@dataclass(frozen=True, kw_only=True)
class _VoiceRun:
    agent_id: uuid.UUID
    owner: OwnerTuple
    installation_mode: ModelAuthMode
    tracker_issue: TrackerIssueRef
    repository: RepositoryRef
    installation_id: int | None
    lineage_id: uuid.UUID | None
    branch: str | None
    pr_number: int | None
    pr_url: str | None
    implementation_request_id: uuid.UUID | None


@dataclass(frozen=True, kw_only=True, repr=False)
class SlackVoiceAuthorization:
    """Facts established before fetching a clip or locking its draft."""

    work_item_id: uuid.UUID
    principal_id: uuid.UUID
    source: _VoiceSource
    resolution: Resolution
    link: _Link
    login: str
    run: _VoiceRun


def steering_request_id(channel_identity: str, delivery_id: str) -> uuid.UUID:
    """The revision request one Slack delivery admits, the same on every redelivery."""

    return uuid.uuid5(
        uuid.NAMESPACE_URL, f"curie-slack://factory/steer/{channel_identity}/{delivery_id}"
    )


def objective(pr_url: str, branch: str, login: str, body: str) -> str:
    """The revision's objective: the pull request, the same-branch instruction, and
    the person's full reply as JSON, so quotes and newlines arrive intact."""

    reply = json.dumps(
        {
            "kind": "slack_mention",
            "pull_request": pr_url,
            "branch": branch,
            "sender": login,
            "body": body,
        },
        ensure_ascii=False,
    )
    return with_pull_request_follow_up_marker(
        f"{pr_url}\n\nA person mentioned this factory run in its Slack thread. Revise this "
        f"same pull request on its existing branch {branch}; do not open a new pull "
        f"request. Their full reply follows as JSON.\n{reply}"
    )


def _refused(
    outcome: FactorySlackSteerOutcome, *, reason: str | None = None
) -> FactorySlackSteerOut:
    return FactorySlackSteerOut(outcome=outcome, reason=reason)


def _receipt(data: FactorySlackSteerIn) -> tuple[SlackMentionReceipt, SlackEvidence] | None:
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
    ):
        return None
    # Cross-consumer controls (#5380): kickoff also reads root and declared
    # driver receipts; steering judges a person's thread reply only.
    if receipt.version != 1 or receipt.message_ts == receipt.root_ts:
        return None
    return receipt, evidence


def _voice_source(data: FactorySlackSteerIn, evidence: SlackEvidence) -> _VoiceSource | None:
    """Bind the adapter's verified file receipt to this delivery and its sender evidence."""

    receipt = data.receipt
    try:
        sender = SlackEvidence.model_validate(receipt.get("evidence"))
    except ValidationError:
        return None
    if (
        receipt.get("version") != 3
        or receipt.get("driver_bot_id") is not None
        or receipt.get("body") != ""
        or receipt.get("delivery_id") != data.delivery_id
        or receipt.get("channel_identity") != data.channel_identity
        or receipt.get("channel") != data.channel
        or receipt.get("root_ts") != data.root_ts
        or receipt.get("message_ts") == data.root_ts
        or receipt.get("user_id") != data.author
        or sender.user_id != data.author
        or sender.delivery != "app_mention"
        or sender != evidence
    ):
        return None
    fields = ("message_ts", "voice_file_id", "content_sha256", "api_app_id", "team_id")
    if any(not isinstance(receipt.get(key), str) or not receipt[key] for key in fields):
        return None
    return _VoiceSource(
        delivery_id=data.delivery_id,
        channel_identity=data.channel_identity,
        channel=data.channel,
        root_ts=data.root_ts,
        author=data.author,
        message_ts=receipt["message_ts"],
        file_id=receipt["voice_file_id"],
        content_sha256=receipt["content_sha256"],
        api_app_id=receipt["api_app_id"],
        team_id=receipt["team_id"],
        evidence_json=sender.model_dump_json(),
    )


async def _resolve(
    session: AsyncSession, data: FactorySlackSteerIn, evidence: SlackEvidence
) -> Resolution:
    return await resolve_principal(
        session,
        tenant_id=DEFAULT_TENANT_ID,
        provider="slack",
        channel_identity=data.channel_identity,
        slack=evidence,
    )


async def _links(
    session: AsyncSession, principal_id: uuid.UUID, repository: RepositoryRef
) -> list[_Link]:
    """The principal's active links in the namespace of ``repository``'s host."""

    host = repository.host.lower()
    rows = (
        await session.execute(
            select(IdentityLink.id, IdentityLink.provider_native_id)
            .join(IdentityNamespace, IdentityNamespace.id == IdentityLink.identity_namespace_id)
            .where(
                IdentityNamespace.tenant_id == DEFAULT_TENANT_ID,
                IdentityNamespace.provider == repository.kind,
                IdentityNamespace.kind == GITHUB_HOST_KIND,
                IdentityNamespace.authority == host,
                IdentityNamespace.key == host,
                IdentityNamespace.status == "active",
                IdentityLink.tenant_id == DEFAULT_TENANT_ID,
                IdentityLink.principal_id == principal_id,
                IdentityLink.revoked_at.is_(None),
            )
            .order_by(IdentityLink.id)
        )
    ).all()
    return [_Link(row[0], row[1]) for row in rows]


async def resolve_github_requester(
    session: AsyncSession, *, agent_id: uuid.UUID, tracker_host: str, account_id: int
) -> uuid.UUID | None:
    """Resolve only the verified label actor, using the trusted tracker host."""

    if type(account_id) is not int or account_id <= 0:
        return None
    await identity_fence.lock_identity_reader(session)
    host = tracker_host.lower()
    principal: uuid.UUID | None = await session.scalar(
        select(Principal.id)
        .join(Tenant, Tenant.id == Principal.tenant_id)
        .join(Agent, Agent.tenant_id == Tenant.id)
        .join(IdentityLink, IdentityLink.principal_id == Principal.id)
        .join(IdentityNamespace, IdentityNamespace.id == IdentityLink.identity_namespace_id)
        .where(
            Agent.id == agent_id,
            Tenant.status == "active",
            Principal.status == "active",
            IdentityLink.tenant_id == Tenant.id,
            IdentityLink.provider_native_id == str(account_id),
            IdentityLink.revoked_at.is_(None),
            IdentityNamespace.tenant_id == Tenant.id,
            IdentityNamespace.provider == "github",
            IdentityNamespace.kind == GITHUB_HOST_KIND,
            IdentityNamespace.authority == host,
            IdentityNamespace.key == host,
            IdentityNamespace.status == "active",
        )
    )
    return principal


def _canonical_host(host: str) -> str:
    return host.strip().lower()


def _code_host(
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    repository: RepositoryRef,
    installation_id: int | None,
) -> CodeHost | FactorySlackSteerOut:
    """The configured code host of ``repository``'s own kind and host, or the
    ``code_host_mismatch`` refusal when the registry configures none."""

    installations = None if installation_id is None else {repository.path: installation_id}
    try:
        if repository.kind == types.GITHUB:
            entry = factory_code_host_entry(registry)
            if _canonical_host(entry.config.host) != _canonical_host(repository.host):
                return _refused("code_host_mismatch")
            return entry.adapter(client, installations=installations)
        try:
            connection = registry.code_host_for(repository.kind, repository.host)
        except Unavailable:
            # The lookup's only refusal: no entry has this kind and host.
            return _refused("code_host_mismatch")
        return connection.adapter(client, installations=installations, repository=repository)
    except ForgeError as exc:
        raise CodeHostUnavailable(type(exc).__name__) from None


def _allowlist(
    registry: ForgeRegistry, issue: TrackerIssueRef, repository: RepositoryRef
) -> frozenset[str]:
    """The binding's fallback account ids for a code host that reports no write
    access; empty when no binding lists the repository."""

    binding = binding_for_scope(registry, issue)
    if binding is None:
        return frozenset()
    repo = registry.binding_repository_policy(binding, repository)
    if repo is None or repo not in binding.repos:
        return frozenset()
    return feedback_allowlist(binding, repo)


async def _linked_code_host(
    session: AsyncSession,
    registry: ForgeRegistry,
    client: httpx.AsyncClient,
    principal_id: uuid.UUID,
    run: _Run | _VoiceRun,
) -> tuple[CodeHost, _Link] | FactorySlackSteerOut:
    code_host = _code_host(registry, client, run.repository, run.installation_id)
    if isinstance(code_host, FactorySlackSteerOut):
        return code_host
    if not supports(code_host.capabilities, Operation.ACCOUNT_LOGIN):
        return _refused("code_host_identity_unsupported")
    links = await _links(session, principal_id, run.repository)
    if not links:
        return _refused("code_host_identity_missing")
    if len(links) != 1:
        return _refused("code_host_identity_ambiguous")
    return code_host, links[0]


async def _mapped_work_item_id(
    session: AsyncSession, data: FactorySlackSteerIn
) -> uuid.UUID | None:
    work_item_id: uuid.UUID | None = await session.scalar(
        select(FactorySlackThread.work_item_id).where(
            FactorySlackThread.channel_identity == data.channel_identity,
            FactorySlackThread.channel == data.channel,
            FactorySlackThread.root_ts == data.root_ts,
        )
    )
    return work_item_id


async def _read_run(
    session: AsyncSession, work_item_id: uuid.UUID
) -> _Run | FactorySlackSteerOut:
    item = await session.get(WorkItem, work_item_id)
    if item is None:
        return _refused("not_mapped")
    if item.cancelled_at is not None:
        return _refused("work_item_cancelled")
    lineage = (
        None
        if item.publication_lineage_id is None
        else await session.get(ThreadPublicationLineage, item.publication_lineage_id)
    )
    if lineage is None or lineage.status != "open":
        return _refused("lineage_closed")
    repository = lineage.repository
    if lineage.pr_number is None or lineage.pr_url is None or repository is None:
        return _refused("no_pull_request")
    return _Run(
        work_item_id=item.id,
        tracker_issue=item.tracker_issue,
        installation_id=item.code_host_installation_id,
        lineage_id=lineage.id,
        repository=repository,
        branch=lineage.branch,
        pr_number=lineage.pr_number,
        pr_url=lineage.pr_url,
    )


async def _voice_owner_refusal(
    session: AsyncSession, item: WorkItem, principal_id: uuid.UUID, installation_mode: ModelAuthMode
) -> RefusedSteering | None:
    if item.tenant_id is not None and item.tenant_id != DEFAULT_TENANT_ID:
        return RefusedSteering(reason="owner_tenant_mismatch")
    tenant_status = await session.scalar(
        select(Tenant.status)
        .join(Agent, Agent.tenant_id == Tenant.id)
        .where(Agent.id == item.agent_id, Tenant.id == DEFAULT_TENANT_ID)
    )
    if tenant_status is None:
        return RefusedSteering(reason="owner_tenant_mismatch")
    if tenant_status != "active":
        return RefusedSteering(reason="owner_tenant_inactive")
    if item.model_auth_mode != "personal":
        if installation_mode == "personal":
            return RefusedSteering(reason="personal_owner_required")
        return None
    if principal_id != item.owner_principal_id:
        return RefusedSteering(reason="owner_mismatch")
    enrollment = (
        await session.execute(
            select(PersonalClaudeEnrollment.active, PersonalClaudeEnrollment.generation).where(
                PersonalClaudeEnrollment.tenant_id == item.tenant_id,
                PersonalClaudeEnrollment.principal_id == principal_id,
            )
        )
    ).one_or_none()
    if enrollment is None or not enrollment.active:
        return RefusedSteering(reason="enrollment_inactive")
    if enrollment.generation != item.enrollment_generation:
        return RefusedSteering(reason="enrollment_generation_mismatch")
    return None


async def _read_voice_run(
    session: AsyncSession, item: WorkItem, installation_mode: ModelAuthMode
) -> _VoiceRun | RefusedSteering:
    lineage = None
    if item.publication_lineage_id is not None:
        lineage = await session.get(ThreadPublicationLineage, item.publication_lineage_id)
        if lineage is None or lineage.status != "open":
            return RefusedSteering(reason="lineage_closed")
        if lineage.repository is not None and lineage.repository != item.repository:
            return RefusedSteering(reason="repository_mismatch")
        if (lineage.pr_number is None) != (lineage.pr_url is None):
            return RefusedSteering(reason="no_pull_request")
        if lineage.pr_number is not None and lineage.repository is None:
            return RefusedSteering(reason="repository_mismatch")
    implementation_id = None
    if lineage is None or lineage.pr_number is None:
        implementation_id = await session.scalar(
            select(ExecutionRequest.id)
            .where(
                ExecutionRequest.work_item_id == item.id,
                ExecutionRequest.execution_kind == "implementation",
                or_(
                    and_(
                        ExecutionRequest.status == "waiting",
                        ExecutionRequest.wait_deadline > func.clock_timestamp(),
                    ),
                    and_(
                        ExecutionRequest.status == "running",
                        ExecutionRequest.execution_deadline > func.clock_timestamp(),
                    ),
                ),
            )
        )
        if implementation_id is None:
            return RefusedSteering(reason="no_pull_request")
    return _VoiceRun(
        agent_id=item.agent_id,
        owner=OwnerTuple.of(item),
        installation_mode=installation_mode,
        tracker_issue=item.tracker_issue,
        repository=item.repository,
        installation_id=item.code_host_installation_id,
        lineage_id=item.publication_lineage_id,
        branch=lineage.branch if lineage is not None else None,
        pr_number=lineage.pr_number if lineage is not None else None,
        pr_url=lineage.pr_url if lineage is not None else None,
        implementation_request_id=implementation_id,
    )


async def authorize_voice_source(
    session: AsyncSession,
    data: FactorySlackSteerIn,
    evidence: SlackEvidence,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> SlackVoiceAuthorization | RefusedSteering:
    """Check a verified original clip before media retrieval and again before draft locking."""

    source = _voice_source(data, evidence)
    if source is None:
        return RefusedSteering(reason="receipt_mismatch")
    async with AsyncSession(bind=session.bind) as reader:
        work_item_id = await _mapped_work_item_id(reader, data)
        if work_item_id is None:
            return RefusedSteering(reason="not_mapped")
        item = await reader.get(WorkItem, work_item_id)
        if item is None:
            return RefusedSteering(reason="not_mapped")
        if item.cancelled_at is not None:
            return RefusedSteering(reason="work_item_cancelled")
        resolution = await _resolve(reader, data, evidence)
        if resolution.status != "resolved" or resolution.principal_id is None:
            return RefusedSteering(reason="sender_unverified")
        principal_id = resolution.principal_id
        team = await reader.scalar(
            select(ProviderInstallation.external_account_id)
            .join(
                ChannelIdentity,
                ChannelIdentity.provider_installation_id == ProviderInstallation.id,
            )
            .where(ChannelIdentity.id == resolution.channel_identity_id)
        )
        if team != source.team_id:
            return RefusedSteering(reason="receipt_mismatch")
        mode = settings.factory_model_auth_mode
        owner_refusal = await _voice_owner_refusal(reader, item, principal_id, mode)
        if owner_refusal is not None:
            return owner_refusal
        run = await _read_voice_run(reader, item, mode)
        if isinstance(run, RefusedSteering):
            return run
        registry = forges_registry(settings)
        linked = await _linked_code_host(reader, registry, client, principal_id, run)
        if isinstance(linked, FactorySlackSteerOut):
            return RefusedSteering(reason=linked.outcome)
        code_host, link = linked
        allowlist = _allowlist(registry, run.tracker_issue, run.repository)
    refusal, login = await _provider_refusal(code_host, run, link.account_id, allowlist)
    if refusal is not None:
        return RefusedSteering(reason=refusal)
    assert login is not None
    return SlackVoiceAuthorization(
        work_item_id=work_item_id,
        principal_id=principal_id,
        source=source,
        resolution=resolution,
        link=link,
        login=login,
        run=run,
    )


async def _pull_refusal(
    code_host: CodeHost, repository: RepositoryRef, number: int, branch: str | None
) -> FactorySlackSteerOutcome | None:
    try:
        pull = await code_host.read_pull_request(PullRequestRef(repository, str(number)))
    except NotFound:
        return "pull_request_not_open"
    except Unavailable as exc:
        if exc.args == ("pull_request_mismatch",):
            return "repository_mismatch"
        raise
    if pull.state is not PullRequestState.OPEN or pull.ref.number != str(number):
        return "pull_request_not_open"
    if pull.head_ref != branch:
        return "pull_request_branch_mismatch"
    return None


async def _provider_refusal(
    code_host: CodeHost,
    run: _Run | _VoiceRun,
    account_id: str,
    allowlist: frozenset[str],
) -> tuple[FactorySlackSteerOutcome | None, str | None]:
    """Read the pull request and the person's authority now. Returns the refusal
    code, or None with the account's current login when every read agrees."""

    try:
        try:
            repository = await resolve_stored(code_host, run.repository)
        except NotFound:
            # A moved or replaced repository, or one that no longer exists.
            return "repository_mismatch", None
        if run.pr_number is not None:
            refusal = await _pull_refusal(code_host, repository, run.pr_number, run.branch)
            if refusal is not None:
                return refusal, None
        login = await code_host.account_login(repository, account_id)
        if login is None:
            return "code_host_identity_missing", None
        if not await may_act_on_feedback(
            code_host, repository, Actor(account_id, login), allowlist
        ):
            return "not_authorized", None
    except ForgeError as exc:
        raise CodeHostUnavailable(type(exc).__name__) from None
    except httpx.HTTPError:
        raise CodeHostUnavailable("transport") from None
    return None, login


async def steer(
    session: AsyncSession,
    data: FactorySlackSteerIn,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> FactorySlackSteerOut:
    """Decide one threaded mention in a factory root; admit a revision when allowed.

    Raises `CodeHostUnavailable` when the code host cannot answer now.
    """

    decision = await _decide(session, data, settings=settings, client=client)
    logger.info(
        "factory slack steering delivery %s: %s%s",
        data.delivery_id,
        decision.outcome,
        "" if decision.reason is None else f" ({decision.reason})",
    )
    return decision


async def _decide(
    session: AsyncSession,
    data: FactorySlackSteerIn,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> FactorySlackSteerOut:
    checked = _receipt(data)
    if checked is None:
        return _refused("receipt_mismatch")
    receipt, evidence = checked
    work_item_id = await _mapped_work_item_id(session, data)
    if work_item_id is None:
        return _refused("not_mapped")
    resolution = await _resolve(session, data, evidence)
    if resolution.status != "resolved" or resolution.principal_id is None:
        return _refused("sender_unverified", reason=resolution.reason)
    principal_id = resolution.principal_id
    run = await _read_run(session, work_item_id)
    if isinstance(run, FactorySlackSteerOut):
        return run
    registry = forges_registry(settings)
    linked = await _linked_code_host(session, registry, client, principal_id, run)
    if isinstance(linked, FactorySlackSteerOut):
        return linked
    code_host, link = linked
    allowlist = _allowlist(registry, run.tracker_issue, run.repository)
    # End the read transaction: no snapshot or lock is held across provider reads.
    await session.rollback()

    refusal, login = await _provider_refusal(code_host, run, link.account_id, allowlist)
    if refusal is not None:
        return _refused(refusal)
    assert login is not None

    try:
        return await _admit(
            session,
            registry,
            data,
            receipt,
            evidence,
            principal_id,
            link,
            run,
            login,
            installation_mode=settings.factory_model_auth_mode,
        )
    finally:
        if session.in_transaction():
            await session.rollback()


async def _owner_facts(
    session: AsyncSession,
    item: WorkItem,
    principal_id: uuid.UUID,
    installation_mode: ModelAuthMode,
) -> tuple[ModelAuthMode, OwnerContext | None]:
    """The mode and owner a steer of the locked ``item`` admits under (#5378).

    A personal WorkItem names the sender as its owner, in the tenant `_resolve`
    resolved the sender in, at the generation the sender is enrolled at now
    (a snapshot until admission locks and checks the enrollment). Admission
    refuses another principal whatever its code host access (``owner_mismatch``),
    an inactive or missing enrollment (``enrollment_inactive``; 0 is never a
    generation) and another tenant's WorkItem (``owner_tenant_mismatch``).

    A service WorkItem carries the installation's mode and never an owner, as
    tracker ingress does: a personal installation refuses it with
    ``personal_owner_required``. Either way the revision copies the WorkItem's
    own tuple.
    """

    if item.model_auth_mode != "personal":
        return installation_mode, None
    enrollment = select(PersonalClaudeEnrollment.generation).where(
        PersonalClaudeEnrollment.tenant_id == DEFAULT_TENANT_ID,
        PersonalClaudeEnrollment.principal_id == principal_id,
    )
    if item.owner_principal_id != principal_id or item.tenant_id != DEFAULT_TENANT_ID:
        enrollment = enrollment.with_for_update(read=True)
    enrolled = await session.scalar(enrollment)
    generation = 0 if enrolled is None else int(enrolled)
    return "personal", OwnerContext(DEFAULT_TENANT_ID, principal_id, generation)


async def _admit(
    session: AsyncSession,
    registry: ForgeRegistry,
    data: FactorySlackSteerIn,
    receipt: SlackMentionReceipt,
    evidence: SlackEvidence,
    principal_id: uuid.UUID,
    link: _Link,
    run: _Run,
    login: str,
    *,
    installation_mode: ModelAuthMode,
) -> FactorySlackSteerOut:
    """Under the identity reader and issue locks, re-check and admit in one transaction."""

    await identity_fence.lock_identity_reader(session)
    current = await _resolve(session, data, evidence)
    links = (
        await _links(session, principal_id, run.repository) if current.status == "resolved" else []
    )
    if current.principal_id != principal_id or links != [link]:
        return _refused("authority_changed")
    await lock_issue(session, run.tracker_issue)
    item = await session.scalar(
        select(WorkItem)
        .where(WorkItem.id == run.work_item_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if item is None:
        return _refused("not_mapped")
    if item.cancelled_at is not None:
        return _refused("work_item_cancelled")
    lineage = await session.scalar(
        select(ThreadPublicationLineage)
        .where(ThreadPublicationLineage.id == run.lineage_id)
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if lineage is None or item.publication_lineage_id != lineage.id or lineage.status != "open":
        return _refused("lineage_closed")
    if lineage.pr_number is None or lineage.pr_url is None:
        return _refused("no_pull_request")
    if lineage.pr_number != run.pr_number or lineage.repository != run.repository:
        return _refused("pull_request_not_open")
    if lineage.branch != run.branch:
        return _refused("pull_request_branch_mismatch")
    pr_url = lineage.pr_url
    text = objective(pr_url, run.branch, login, receipt.body)
    if len(text) > MAX_OBJECTIVE:
        return _refused("objective_too_large")
    kind, address, conversation = reply_route(registry, item.tracker_issue, item.repository_path)
    mode, owner = await _owner_facts(session, item, principal_id, installation_mode)
    facts = Admission(
        agent_id=item.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=conversation,
        issue=item.tracker_issue,
        repository=item.repository,
        code_host_installation_id=item.code_host_installation_id,
        objective=text,
        requester=f"{run.repository.kind}:{link.account_id}:{login}",
        request_id=steering_request_id(data.channel_identity, data.delivery_id),
        reply_target=ReplyTarget.on_pull_request(
            PullRequestRef(run.repository, str(run.pr_number))
        ),
        reply_url=pr_url,
        model_auth_mode=mode,
        owner=owner,
    )
    # Dispatch commits inside a savepoint, so the identity reader and issue
    # locks stay held until the admission commits with them.
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as admission:
        result = await workitem_dispatch.admit_for_verified_owner(
            admission,
            facts,
            work_item_id=item.id,
            principal_id=principal_id,
            admit=workitem_dispatch.admit_revision,
        )
        await admission.commit()
    if not isinstance(result, WorkItemOutcome) and result.code == "identity_mismatch":
        committed = await _committed_request(session, facts.request_id, run.work_item_id)
        if committed is not None:
            await session.commit()
            return committed.model_copy(update={"pull_request_url": pr_url})
    await session.commit()
    return _answer(result, facts.request_id, pr_url)


async def _committed_request(
    session: AsyncSession, request_id: uuid.UUID, work_item_id: uuid.UUID
) -> FactorySlackSteerOut | None:
    """The answer for a request an earlier attempt of this delivery committed.

    ``admit_revision`` refuses a replay whose facts differ from the committed
    snapshot. The request id is this delivery's own, so a row with it on this
    WorkItem is the revision that attempt admitted.
    """

    row = await session.scalar(
        select(ExecutionRequest)
        .where(ExecutionRequest.id == request_id, ExecutionRequest.work_item_id == work_item_id)
        .execution_options(populate_existing=True)
    )
    if row is None:
        return None
    return FactorySlackSteerOut(
        outcome="admitted",
        request_id=str(row.id),
        request_status=row.status,
        replayed=True,
    )


def _answer(result: Any, request_id: uuid.UUID, pr_url: str) -> FactorySlackSteerOut:
    if isinstance(result, WorkItemOutcome):
        request = result.request
        return FactorySlackSteerOut(
            outcome="admitted",
            request_id=str(request.id if request is not None else request_id),
            request_status=None if request is None else request.status,
            replayed=result.replayed,
            pull_request_url=pr_url,
        )
    return _refused("admission_refused", reason=str(result.code))
