"""Authenticate factory Slack guidance and bind it to its first eligible execution."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, cast, get_args

import httpx
from curie_internal.slack_mention_receipt import SlackMentionReceipt
from pydantic import ValidationError
from sqlalchemy import and_, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from . import factory_steering_ledger as ledger
from . import identity_fence, workitem_dispatch
from .config import Settings
from .factory_confirmed_steering import (
    AppliedSteering,
    ConfirmedText,
    QueuedSteering,
    RefusedSteering,
    SlackSteeringSource,
    SteeringDecision,
)
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
    FactorySlackPublicThread,
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
from .workitems.lifecycle import WorkItemOutcome, database_now
from .workspace_policy import repository_allowed

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


@dataclass(frozen=True, kw_only=True, repr=False)
class _Source:
    delivery_id: str
    channel_identity: str
    team_id: str | None
    api_app_id: str | None
    channel: str
    root_ts: str
    message_ts: str
    file_id: str | None
    content_sha256: str
    author: str
    evidence_json: str


@dataclass(frozen=True, kw_only=True)
class _Run:
    agent_id: uuid.UUID
    owner: OwnerTuple
    installation_mode: ModelAuthMode
    public_channel: str | None
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
    source: _Source
    resolution: Resolution
    link: _Link
    login: str
    run: _Run
    public_source: bool


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
        data.receipt.get("bot_user_id") is not None
        or receipt.delivery_id != data.delivery_id
        or receipt.channel_identity != data.channel_identity
        or receipt.channel != data.channel
        or receipt.root_ts != data.root_ts
        or receipt.user_id != data.author
        or evidence.user_id != receipt.user_id
    ):
        return None
    # Cross-consumer controls (#5380): kickoff also reads root and declared
    # driver receipts; steering judges a person's thread reply only.
    if (
        receipt.version != 1
        or receipt.message_ts == receipt.root_ts
        or evidence.delivery != "app_mention"
    ):
        return None
    return receipt, evidence


def _voice_source(data: FactorySlackSteerIn, evidence: SlackEvidence) -> _Source | None:
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
    if not (
        re.fullmatch(r"[0-9a-f]{64}", receipt["content_sha256"])
        and re.fullmatch(r"F[A-Z0-9]+", receipt["voice_file_id"])
        and re.fullmatch(r"A[A-Z0-9]+", receipt["api_app_id"])
        and re.fullmatch(r"T[A-Z0-9]+", receipt["team_id"])
        and re.fullmatch(r"[0-9]+\.[0-9]+", receipt["message_ts"])
    ):
        return None
    return _Source(
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
    session: AsyncSession, data: _Source, evidence: SlackEvidence
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
    run: _Run,
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


async def _mapping(session: AsyncSession, source: _Source) -> tuple[uuid.UUID, str] | None:
    row = (
        await session.execute(
            text("""
        SELECT work_item_id, destination_role FROM curie.factory_slack_mapped_roots
        WHERE channel_identity = :identity AND channel = :channel AND root_ts = :root
    """),
            {
                "identity": source.channel_identity,
                "channel": source.channel,
                "root": source.root_ts,
            },
        )
    ).one_or_none()
    return None if row is None else (row.work_item_id, row.destination_role)


async def _voice_owner_refusal(
    session: AsyncSession,
    item: WorkItem,
    principal_id: uuid.UUID,
    installation_mode: ModelAuthMode,
    *,
    voice: bool,
    lock_enrollment: bool = False,
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
    if voice and principal_id != item.owner_principal_id:
        return RefusedSteering(reason="owner_mismatch")
    query = select(PersonalClaudeEnrollment.active, PersonalClaudeEnrollment.generation).where(
        PersonalClaudeEnrollment.tenant_id == item.tenant_id,
        PersonalClaudeEnrollment.principal_id == principal_id,
    )
    if lock_enrollment:
        query = query.with_for_update(read=True)
    enrollment = (await session.execute(query)).one_or_none()
    if enrollment is None or not enrollment.active:
        return RefusedSteering(reason="enrollment_inactive")
    if principal_id != item.owner_principal_id:
        return RefusedSteering(reason="owner_mismatch")
    if enrollment.generation != item.enrollment_generation:
        return RefusedSteering(reason="enrollment_generation_mismatch")
    return None


async def _read_voice_run(
    session: AsyncSession,
    item: WorkItem,
    installation_mode: ModelAuthMode,
    *,
    voice: bool,
    locked: bool = False,
    deferred: bool = False,
) -> _Run | RefusedSteering:
    lineage = None
    if item.publication_lineage_id is not None:
        lineage = await session.get(
            ThreadPublicationLineage,
            item.publication_lineage_id,
            with_for_update={"read": True} if locked else None,
            populate_existing=locked,
        )
        if lineage is None or lineage.status != "open":
            return RefusedSteering(reason="lineage_closed")
        if lineage.repository is not None and lineage.repository != item.repository:
            return RefusedSteering(reason="repository_mismatch")
        if (lineage.pr_number is None) != (lineage.pr_url is None):
            return RefusedSteering(reason="no_pull_request")
        if lineage.pr_number is not None and (
            lineage.repository is None
            or lineage.code_host_installation_id != item.code_host_installation_id
        ):
            return RefusedSteering(reason="repository_mismatch")
    implementation_id = None
    destination = select(Agent.factory_public_slack_channel).where(Agent.id == item.agent_id)
    if locked:
        destination = destination.with_for_update(read=True)
    public_channel = await session.scalar(destination)
    if deferred and public_channel is None:
        return RefusedSteering(reason="authority_changed")
    if not public_channel and (voice or lineage is None or lineage.pr_number is None):
        return RefusedSteering(
            reason="lineage_closed" if lineage is None and not voice else "no_pull_request"
        )
    if lineage is None or lineage.pr_number is None:
        if await session.get(FactorySlackPublicThread, item.id) is None:
            return RefusedSteering(reason="no_pull_request")
        implementation_id = await session.scalar(
            select(ExecutionRequest.id).where(
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
    return _Run(
        agent_id=item.agent_id,
        owner=OwnerTuple.of(item),
        installation_mode=installation_mode,
        public_channel=public_channel,
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
    return await _authorize(session, source, settings=settings, client=client)


async def _receiving_team(session: AsyncSession, resolution: Resolution) -> str | None:
    team: str | None = await session.scalar(
        select(ProviderInstallation.external_account_id)
        .join(ChannelIdentity, ChannelIdentity.provider_installation_id == ProviderInstallation.id)
        .where(ChannelIdentity.id == resolution.channel_identity_id)
    )
    return team


async def _authorize(
    session: AsyncSession,
    source: _Source,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
    deferred: bool = False,
) -> SlackVoiceAuthorization | RefusedSteering:
    evidence = SlackEvidence.model_validate_json(source.evidence_json)
    async with AsyncSession(bind=session.bind) as reader:
        mapping = await _mapping(reader, source)
        if mapping is None:
            return RefusedSteering(reason="not_mapped")
        work_item_id, role = mapping
        item = await reader.get(WorkItem, work_item_id)
        if item is None:
            return RefusedSteering(reason="not_mapped")
        if item.cancelled_at is not None:
            return RefusedSteering(reason="work_item_cancelled")
        resolution = await _resolve(reader, source, evidence)
        if resolution.status != "resolved" or resolution.principal_id is None:
            return RefusedSteering(reason="sender_unverified")
        principal_id = resolution.principal_id
        if (
            source.team_id is not None
            and await _receiving_team(reader, resolution) != source.team_id
        ):
            return RefusedSteering(reason="receipt_mismatch")
        mode = settings.factory_model_auth_mode
        owner_refusal = await _voice_owner_refusal(
            reader, item, principal_id, mode, voice=source.file_id is not None
        )
        if owner_refusal is not None and source.file_id is not None:
            return owner_refusal
        run = await _read_voice_run(
            reader, item, mode, voice=source.file_id is not None, deferred=deferred
        )
        if isinstance(run, RefusedSteering):
            return run
        if run.public_channel and not repository_allowed(settings, run.repository):
            return RefusedSteering(reason="repository_mismatch")
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
    if owner_refusal is not None and not (
        owner_refusal.reason == "enrollment_generation_mismatch"
        and not deferred
        and source.file_id is None
        and run.pr_number is not None
    ):
        return owner_refusal
    return SlackVoiceAuthorization(
        work_item_id=work_item_id,
        principal_id=principal_id,
        source=source,
        resolution=resolution,
        link=link,
        login=login,
        run=run,
        public_source=role == "public",
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
    if pull.ref.repository != repository:
        return "repository_mismatch"
    if pull.state is not PullRequestState.OPEN or pull.ref.number != str(number):
        return "pull_request_not_open"
    if pull.head_ref != branch:
        return "pull_request_branch_mismatch"
    return None


async def _provider_refusal(
    code_host: CodeHost,
    run: _Run,
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


def _source_facts(authority: SlackVoiceAuthorization) -> dict[str, Any]:
    source = authority.source
    run = authority.run
    return {
        "message_ts": source.message_ts,
        "author": source.author,
        "file_id": source.file_id,
        "original_delivery_id": source.delivery_id,
        "team_id": source.team_id,
        "api_app_id": source.api_app_id,
        "evidence": json.loads(source.evidence_json),
        "principal_id": str(authority.principal_id),
        "identity_id": str(authority.resolution.channel_identity_id),
        "namespace_id": str(authority.resolution.namespace_id),
        "link_id": str(authority.link.link_id),
        "account_id": authority.link.account_id,
        "provider": run.repository.kind,
        "host": run.repository.host,
        "repository_id": run.repository.project_id,
        "publication": {
            "lineage_id": str(run.lineage_id) if run.lineage_id is not None else None,
            "branch": run.branch,
            "pr_number": run.pr_number,
        },
        "installation_id": run.installation_id,
        "owner": {
            key: str(value) if isinstance(value, uuid.UUID) else value
            for key, value in run.owner.columns().items()
        },
    }


def _same_facts(stored: dict[str, Any], current: dict[str, Any]) -> bool:
    # Prepublication guidance can gain a first branch and PR. Already bound coordinates cannot move.
    fixed = {key: value for key, value in stored.items() if key != "publication"}
    if fixed != {key: value for key, value in current.items() if key != "publication"}:
        return False
    return all(
        value is None or current["publication"][key] == value
        for key, value in stored["publication"].items()
    )


async def _lock_authority(
    session: AsyncSession,
    authority: SlackVoiceAuthorization,
    settings: Settings,
    confirmation_evidence: SlackEvidence | None = None,
    *,
    owner_resume: bool = False,
) -> WorkItem | RefusedSteering:
    source = authority.source
    await identity_fence.lock_identity_reader(session)
    await lock_issue(session, authority.run.tracker_issue)
    item = await session.get(
        WorkItem, authority.work_item_id, with_for_update=True, populate_existing=True
    )
    mapping = await _mapping(session, source)
    if item is None or mapping is None or mapping[0] != authority.work_item_id:
        return RefusedSteering(reason="target_mismatch")
    evidence = SlackEvidence.model_validate_json(source.evidence_json)
    current = await _resolve(session, source, evidence)
    if current != authority.resolution or (mapping[1] == "public") != authority.public_source:
        return RefusedSteering(reason="authority_changed")
    if (
        confirmation_evidence is not None
        and await _resolve(session, source, confirmation_evidence) != current
    ):
        return RefusedSteering(reason="authority_changed")
    if source.team_id is not None and await _receiving_team(session, current) != source.team_id:
        return RefusedSteering(reason="authority_changed")
    if await _links(session, authority.principal_id, item.repository) != [authority.link]:
        return RefusedSteering(reason="authority_changed")
    if item.cancelled_at is not None:
        return RefusedSteering(reason="authority_changed")
    mode = settings.factory_model_auth_mode
    refused = await _voice_owner_refusal(
        session,
        item,
        authority.principal_id,
        mode,
        voice=source.file_id is not None,
        lock_enrollment=not owner_resume,
    )
    if refused is not None and not (
        owner_resume and refused.reason == "enrollment_generation_mismatch"
    ):
        return RefusedSteering(reason="authority_changed")
    run = await _read_voice_run(session, item, mode, voice=source.file_id is not None, locked=True)
    if run != authority.run:
        return RefusedSteering(reason="authority_changed")
    return item


async def _revision(
    session: AsyncSession,
    item: WorkItem,
    authority: SlackVoiceAuthorization,
    settings: Settings,
    steering_id: uuid.UUID,
    body: str,
) -> RefusedSteering | None:
    run = authority.run
    assert run.pr_url is not None and run.pr_number is not None and run.branch is not None
    revision_objective = objective(run.pr_url, run.branch, authority.login, body)
    if len(revision_objective) > MAX_OBJECTIVE:
        return RefusedSteering(reason="objective_too_large")
    kind, address, conversation = reply_route(
        forges_registry(settings), item.tracker_issue, item.repository_path
    )
    mode, owner = await _owner_facts(
        session, item, authority.principal_id, settings.factory_model_auth_mode
    )
    facts = Admission(
        agent_id=item.agent_id,
        kind=kind,
        address=address,
        reply_conversation_id=conversation,
        issue=item.tracker_issue,
        repository=item.repository,
        code_host_installation_id=item.code_host_installation_id,
        objective=revision_objective,
        requester=f"{run.repository.kind}:{authority.link.account_id}:{authority.login}",
        request_id=steering_id,
        reply_target=ReplyTarget.on_pull_request(
            PullRequestRef(run.repository, str(run.pr_number))
        ),
        reply_url=run.pr_url,
        model_auth_mode=mode,
        owner=owner,
    )
    async with session.begin_nested() as admission:
        async with AsyncSession(
            bind=await session.connection(),
            join_transaction_mode="create_savepoint",
            expire_on_commit=False,
        ) as writer:
            result = await workitem_dispatch.admit_for_verified_owner(
                writer,
                facts,
                work_item_id=item.id,
                principal_id=authority.principal_id,
                admit=workitem_dispatch.admit_revision,
            )
            if isinstance(result, WorkItemOutcome):
                await writer.commit()
                return None
        await admission.rollback()
        return RefusedSteering(reason=str(result.code))


def _historical_match(row: ExecutionRequest, authority: SlackVoiceAuthorization, body: str) -> bool:
    """Old revisions stored the original immutable actor and the complete accepted objective."""
    prefix = f"{authority.run.repository.kind}:{authority.link.account_id}:"
    run = authority.run
    if (
        row.work_item_id != authority.work_item_id
        or not row.requester
        or not row.requester.startswith(prefix)
        or run.branch is None
        or run.pr_url is None
        or row.reply_target_pr_number != str(run.pr_number)
        or row.reply_target_url != run.pr_url
        or row.reply_target_kind != "pull_request"
    ):
        return False
    return row.objective == objective(run.pr_url, run.branch, row.requester[len(prefix) :], body)


async def _accept(
    session: AsyncSession,
    *,
    settings: Settings,
    authority: SlackVoiceAuthorization,
    steering_id: uuid.UUID,
    body: str,
    first_delivery_id: str,
    confirmation_evidence: SlackEvidence | None = None,
) -> SteeringDecision:
    owner_resume = authority.source.file_id is None and authority.run.pr_number is not None
    item = await _lock_authority(
        session, authority, settings, confirmation_evidence, owner_resume=owner_resume
    )
    if isinstance(item, RefusedSteering):
        return item
    if not body.strip():
        return RefusedSteering(reason="invalid_text")
    if len(body) > MAX_OBJECTIVE:
        return RefusedSteering(reason="objective_too_large")
    # A conflicting draft key on a different WorkItem does not share this WorkItem lock.
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"factory-steering:{steering_id}"},
    )
    existing = await ledger.get(session, steering_id)
    source = authority.source
    facts = _source_facts(authority)
    if existing is not None:
        if existing.work_item_id != item.id:
            return RefusedSteering(reason="target_mismatch")
        if (
            existing.source_identity != source.channel_identity
            or existing.source_channel != source.channel
            or existing.source_root_ts != source.root_ts
            or not _same_facts(existing.source_facts, facts)
            or existing.source_content_sha256 != source.content_sha256
            or existing.accepted_text != body
            or existing.accepted_text_sha256 != ledger.digest(body)
        ):
            return RefusedSteering(reason="source_changed")
        return existing.decision(replayed=True)
    historical = await session.get(ExecutionRequest, steering_id)
    if historical is not None and (
        source.file_id is not None or not _historical_match(historical, authority, body)
    ):
        return RefusedSteering(reason="source_changed")
    request_id = None
    if historical is not None:
        request_id = historical.id
    elif authority.run.pr_number is not None:
        refused = await _revision(session, item, authority, settings, steering_id, body)
        if refused is not None:
            return refused
        request_id = steering_id
    else:
        now = await database_now(session)
        unassigned = await ledger.pending(session, item.id)
        expired = [
            entry.steering_id
            for entry in unassigned
            if entry.created_at + timedelta(hours=24) <= now
        ]
        if expired:
            await ledger.refuse(session, item.id, expired, "guidance_expired")
        if await session.scalar(
            text(
                "SELECT count(*) >= 8 FROM curie.factory_steering_ledger "
                "WHERE work_item_id = :id AND state = 'queued'"
            ),
            {"id": item.id},
        ):
            return RefusedSteering(reason="guidance_queue_full")
    if not authority.run.public_channel and not authority.public_source and source.file_id is None:
        return QueuedSteering(
            steering_id=steering_id,
            execution_request_id=request_id,
            replayed=historical is not None,
        )
    if owner_resume and item.model_auth_mode == "personal" and historical is None:
        # The verified owner can resume at a newer generation in the admission savepoint.
        await session.refresh(item, ["enrollment_generation"])
        facts["owner"]["enrollment_generation"] = item.enrollment_generation
    await ledger.record(
        session,
        steering_id=steering_id,
        work_item_id=item.id,
        source_identity=source.channel_identity,
        source_channel=source.channel,
        source_root_ts=source.root_ts,
        source_facts=facts,
        first_delivery_id=first_delivery_id,
        source_content_sha256=source.content_sha256,
        accepted_text=body,
        execution_request_id=request_id,
    )
    return QueuedSteering(
        steering_id=steering_id, execution_request_id=request_id, replayed=historical is not None
    )


def _confirmation_evidence(
    command: ConfirmedText, authority: SlackVoiceAuthorization, confirmation: FactorySlackSteerIn
) -> SlackEvidence | None:
    source = authority.source
    expected = SlackSteeringSource(
        channel_identity=source.channel_identity,
        channel=source.channel,
        root_ts=source.root_ts,
        message_ts=source.message_ts,
        file_id=source.file_id or "",
        author=source.author,
        principal_id=authority.principal_id,
        confirmation_delivery_id=confirmation.delivery_id,
    )
    receipt = confirmation.receipt
    if (
        command.source != expected
        or command.source_content_sha256 != source.content_sha256
        or not source.file_id
        or not source.team_id
        or not source.api_app_id
        or confirmation.channel_identity != source.channel_identity
        or confirmation.channel != source.channel
        or confirmation.root_ts != source.root_ts
        or confirmation.author != source.author
        or confirmation.delivery_id == source.delivery_id
        or receipt.get("delivery_id") != confirmation.delivery_id
        or receipt.get("version") != 1
        or receipt.get("team_id") != source.team_id
        or receipt.get("api_app_id") != source.api_app_id
        or receipt.get("channel_identity") != source.channel_identity
        or receipt.get("channel") != source.channel
        or receipt.get("user_id") != source.author
        or receipt.get("root_ts") != source.root_ts
        or not isinstance(receipt.get("message_ts"), str)
        or not receipt["message_ts"]
        or receipt["message_ts"] in {source.root_ts, source.message_ts}
        or receipt.get("driver_bot_id") is not None
        or "voice_file_id" in receipt
        or "content_sha256" in receipt
    ):
        return None
    bot = receipt.get("bot_user_id")
    body = receipt.get("body")
    if (
        not isinstance(bot, str)
        or not re.fullmatch(r"[UW][A-Z0-9]+", bot)
        or not isinstance(body, str)
    ):
        return None
    if not re.fullmatch(
        rf"[ \t]*<@{re.escape(bot)}(?:\|[^>]+)?>[ \t]+"
        rf"confirm {command.draft_id.hex[:12]}\.[1-9][0-9]{{0,8}}",
        body,
    ):
        return None
    try:
        evidence = SlackEvidence.model_validate(receipt.get("evidence"))
    except ValidationError:
        return None
    return (
        evidence
        if evidence.delivery == "app_mention" and evidence.user_id == source.author
        else None
    )


async def admit_confirmed_text(
    session: AsyncSession,
    command: ConfirmedText,
    /,
    *,
    settings: Settings,
    authority: SlackVoiceAuthorization,
    confirmation: FactorySlackSteerIn,
) -> SteeringDecision:
    """Bind a freshly authenticated confirmation to the caller's locked draft and transaction."""
    if command.work_item_id != authority.work_item_id:
        return RefusedSteering(reason="target_mismatch")
    mapping = await _mapping(session, authority.source)
    if mapping is None or mapping[0] != command.work_item_id:
        return RefusedSteering(reason="target_mismatch")
    evidence = _confirmation_evidence(command, authority, confirmation)
    if evidence is None:
        return RefusedSteering(reason="source_changed")
    async with AsyncSession(
        bind=await session.connection(),
        join_transaction_mode="create_savepoint",
        expire_on_commit=False,
    ) as writer:
        result = await _accept(
            writer,
            settings=settings,
            authority=authority,
            steering_id=command.steering_id,
            body=command.text,
            first_delivery_id=confirmation.delivery_id,
            confirmation_evidence=evidence,
        )
        await writer.commit()
    return result


def _ordinary_answer(decision: SteeringDecision) -> FactorySlackSteerOut:
    if isinstance(decision, RefusedSteering):
        if decision.reason in get_args(FactorySlackSteerOutcome):
            return _refused(cast(FactorySlackSteerOutcome, decision.reason))
        return _refused("admission_refused", reason=decision.reason)
    return FactorySlackSteerOut(
        outcome="applied" if isinstance(decision, AppliedSteering) else "queued",
        request_id=str(decision.execution_request_id) if decision.execution_request_id else None,
        replayed=decision.replayed,
    )


async def steer(
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
    source = _Source(
        delivery_id=data.delivery_id,
        channel_identity=data.channel_identity,
        channel=data.channel,
        root_ts=data.root_ts,
        author=data.author,
        message_ts=receipt.message_ts,
        team_id=None,
        api_app_id=None,
        file_id=None,
        content_sha256=ledger.digest(receipt.body),
        evidence_json=evidence.model_dump_json(),
    )
    authority = await _authorize(session, source, settings=settings, client=client)
    if isinstance(authority, RefusedSteering):
        return _ordinary_answer(authority)
    decision = await _accept(
        session,
        settings=settings,
        authority=authority,
        steering_id=steering_request_id(data.channel_identity, data.delivery_id),
        body=receipt.body,
        first_delivery_id=data.delivery_id,
    )
    if (
        isinstance(decision, QueuedSteering)
        and not authority.run.public_channel
        and not authority.public_source
        and decision.execution_request_id is not None
    ):
        request = await session.get(ExecutionRequest, decision.execution_request_id)
        answer = FactorySlackSteerOut(
            outcome="admitted",
            request_id=str(decision.execution_request_id),
            request_status=request.status if request else None,
            pull_request_url=authority.run.pr_url,
            replayed=decision.replayed,
        )
    else:
        answer = _ordinary_answer(decision)
    await session.commit()
    return answer


async def redrive_pending(
    session: AsyncSession,
    entry: ledger.SteeringEntry,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> SteeringDecision:
    """Reread the original actor and current repository before locking and assigning a follow-up."""
    facts = entry.source_facts
    source = _Source(
        delivery_id=facts["original_delivery_id"],
        channel_identity=entry.source_identity,
        channel=entry.source_channel,
        root_ts=entry.source_root_ts,
        message_ts=facts["message_ts"],
        team_id=facts["team_id"],
        api_app_id=facts["api_app_id"],
        file_id=facts["file_id"],
        author=facts["author"],
        content_sha256=entry.source_content_sha256,
        evidence_json=json.dumps(facts["evidence"]),
    )
    authority = await _authorize(session, source, settings=settings, client=client, deferred=True)
    # Acquire or cancellation may settle the pending row during the provider read.
    await identity_fence.lock_identity_reader(session)
    tracker_item = await session.get(WorkItem, entry.work_item_id)
    if tracker_item is None:
        return RefusedSteering(reason="not_mapped")
    await lock_issue(session, tracker_item.tracker_issue)
    item = await session.get(
        WorkItem, entry.work_item_id, with_for_update=True, populate_existing=True
    )
    current = await ledger.get(session, entry.steering_id)
    if item is None or current is None:
        return RefusedSteering(reason="not_mapped")
    if current.state != "queued" or current.execution_request_id is not None:
        return current.decision(replayed=True)
    reason = None
    if item.cancelled_at is not None:
        reason = "work_item_cancelled"
    elif entry.created_at + timedelta(hours=24) <= await database_now(session):
        reason = "guidance_expired"
    elif item.readmit_request_id or await session.scalar(
        select(ExecutionRequest.id)
        .where(
            ExecutionRequest.work_item_id == item.id,
            ExecutionRequest.status.in_(("waiting", "queued", "running", "cancellation_requested")),
        )
        .limit(1)
    ):
        return current.decision(replayed=True)
    elif isinstance(authority, RefusedSteering):
        reason = authority.reason
    elif authority.work_item_id != item.id:
        reason = "target_mismatch"
    elif not _same_facts(facts, _source_facts(authority)):
        reason = "authority_changed"
    else:
        locked = await _lock_authority(session, authority, settings)
        if isinstance(locked, RefusedSteering):
            reason = locked.reason
        elif authority.run.pr_number is None:
            reason = "no_pull_request"
        else:
            refused = await _revision(
                session, locked, authority, settings, entry.steering_id, entry.accepted_text
            )
            if refused is None:
                await ledger.assign(
                    session,
                    item.id,
                    entry.steering_id,
                    [entry.steering_id],
                    objective_embedded=True,
                )
                return QueuedSteering(
                    steering_id=entry.steering_id,
                    execution_request_id=entry.steering_id,
                    replayed=False,
                )
            reason = refused.reason
    await ledger.refuse(session, item.id, [entry.steering_id], reason)
    return RefusedSteering(reason=reason)
