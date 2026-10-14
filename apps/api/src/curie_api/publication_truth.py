"""Read current factory publication facts without updating durable lineage."""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime

import httpx
from sqlalchemy import Select, SQLColumnExpression, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from starlette.concurrency import run_in_threadpool

from curie_api.crud import lineages as crud_lineages

from .config import Settings
from .forges.errors import ForgeError
from .forges.github.transport import github_headers, repository_identity_matches
from .forges.hosts import forge_code_host
from .forges.types import GITHUB, PullRequestRef, PullRequestState, RepositoryRef
from .github_app import GitHubAppError, GitHubAuth, GitHubCredentials
from .models import Deployment, ExecutionRequest, ThreadPublicationLineage, WorkItem
from .repo_full_name import repo_url_path

PRECHECK_TIMEOUT_SECONDS = 8.0
_PROVIDER_TIMEOUT_SECONDS = 2.0
_MAX_RESPONSE_BYTES = 1_048_576


class PublicationPrecheckRefused(RuntimeError):
    """The durable execution no longer grants this read authority."""


class PublicationPullRequestNotAdopted(PublicationPrecheckRefused):
    """An existing conversation PR cannot be continued by this execution."""


class PublicationPrecheckUnavailable(RuntimeError):
    """Current provider truth cannot be established."""


@dataclass(frozen=True)
class PublicationReadAuthority:
    agent_id: uuid.UUID
    deployment_id: uuid.UUID
    work_item_id: uuid.UUID
    execution_request_id: uuid.UUID
    runtime_epoch: int
    conversation_id: str
    lineage_id: uuid.UUID
    lineage_version: int
    expected_head: str
    execution_deadline: datetime
    repo_full_name: str
    pr_number: int
    pr_url: str
    branch: str
    # The lineage's verified code host identity (ADR 0197): its repository,
    # the GitHub App installation that verified it (None on a code host with
    # no installations), and the pull request's immutable id.
    repository: RepositoryRef
    installation_id: int | None
    pr_id: str
    base_ref: str
    has_inflight_push: bool


@dataclass(frozen=True)
class PublicationMetadata:
    title: str
    body: str
    observed_at: datetime


def conversation_pr_lineage_id(
    *,
    agent_id: uuid.UUID | SQLColumnExpression[uuid.UUID],
    conversation_id: str | SQLColumnExpression[str],
    repo_full_name: str | SQLColumnExpression[str],
) -> Select[tuple[uuid.UUID]]:
    """Select this conversation's open PR, otherwise its newest PR lineage."""

    lineage = aliased(ThreadPublicationLineage)
    return (
        select(lineage.id)
        .where(
            lineage.agent_id == agent_id,
            lineage.conversation_id == conversation_id,
            func.lower(lineage.repo_full_name) == func.lower(repo_full_name),
            lineage.pr_number.is_not(None),
        )
        .order_by(
            (lineage.status == "open").desc(),
            lineage.created_at.desc(),
            lineage.id.desc(),
        )
        .limit(1)
    )


async def read_publication_authority(
    session: AsyncSession,
    *,
    github_html_base: str,
    deployment_id: uuid.UUID,
    work_item_id: uuid.UUID,
    execution_request_id: uuid.UUID,
    runtime_epoch: int,
) -> PublicationReadAuthority | None:
    """Read current ownership and lease at a database clock value, without locks.

    Repeating this query after the provider read refreshes ORM identities and
    checks a new database clock value. No transaction time or stale identity
    map can preserve authority that expired while awaiting GitHub.
    """

    existing_pr = (
        conversation_pr_lineage_id(
            agent_id=WorkItem.agent_id,
            conversation_id=WorkItem.conversation_id,
            repo_full_name=WorkItem.repository_path,
        )
        .correlate(WorkItem)
        .scalar_subquery()
    )
    result = await session.execute(
        select(
            Deployment,
            WorkItem,
            ExecutionRequest,
            ThreadPublicationLineage,
            func.clock_timestamp(),
            existing_pr,
        )
        .select_from(ExecutionRequest)
        .join(WorkItem, WorkItem.id == ExecutionRequest.work_item_id)
        .outerjoin(
            ThreadPublicationLineage, ThreadPublicationLineage.id == WorkItem.publication_lineage_id
        )
        .join(Deployment, Deployment.id == deployment_id)
        .where(ExecutionRequest.id == execution_request_id, WorkItem.id == work_item_id)
        .execution_options(populate_existing=True)
    )
    row = result.one_or_none()
    if row is None:
        raise PublicationPrecheckRefused
    deployment, item, execution, lineage, now, existing_pr_id = row
    if (
        deployment.agent_id != item.agent_id
        or item.cancelled_at is not None
        or execution.status != "running"
        or execution.runtime_epoch != runtime_epoch
        or not execution.runtime_owner
        or execution.runtime_heartbeat_expires_at is None
        or execution.runtime_heartbeat_expires_at <= now
        or execution.execution_deadline is None
        or execution.execution_deadline <= now
    ):
        raise PublicationPrecheckRefused
    if lineage is not None and (
        lineage.agent_id != item.agent_id
        or item.conversation_id != lineage.conversation_id
        or item.repository_path.casefold() != lineage.repo_full_name.casefold()
        or lineage.status != "open"
    ):
        raise PublicationPrecheckRefused
    if lineage is None or lineage.pr_number is None:
        # An absent WorkItem link must not hide an existing conversation PR.
        # The correlated selector uses the same snapshot as the lease.
        if existing_pr_id is not None:
            raise PublicationPullRequestNotAdopted
        if lineage is None and item.publication_lineage_id is not None:
            raise PublicationPrecheckRefused
        return None
    # A GitHub pull request's link is checked here, before GitHub is read. Any
    # other code host names its pull requests itself, so its stored link is
    # compared with the one its read answers (`read_publication_metadata`).
    repository = lineage.repository
    github = lineage.code_host_kind == GITHUB
    if (
        lineage.pr_number <= 0
        or not lineage.pr_url
        or (
            github
            and lineage.pr_url
            != f"{github_html_base}/{lineage.repo_full_name}/pull/{lineage.pr_number}"
        )
        or lineage.head_sha is None
        or re.fullmatch(r"[0-9a-f]{40}", lineage.head_sha) is None
        or repository is None
        or (github and lineage.code_host_installation_id is None)
        or not lineage.code_host_pr_id
        or not lineage.base_ref
    ):
        raise PublicationPrecheckUnavailable
    if (
        lineage.repository != item.repository
        or item.code_host_installation_id != lineage.code_host_installation_id
    ):
        raise PublicationPrecheckRefused
    return PublicationReadAuthority(
        agent_id=item.agent_id,
        deployment_id=deployment.id,
        work_item_id=item.id,
        execution_request_id=execution.id,
        runtime_epoch=execution.runtime_epoch,
        conversation_id=lineage.conversation_id,
        lineage_id=lineage.id,
        lineage_version=lineage.version,
        expected_head=lineage.head_sha,
        execution_deadline=execution.execution_deadline,
        repo_full_name=lineage.repo_full_name,
        pr_number=lineage.pr_number,
        pr_url=lineage.pr_url,
        branch=lineage.branch,
        repository=repository,
        installation_id=lineage.code_host_installation_id,
        pr_id=lineage.code_host_pr_id,
        base_ref=lineage.base_ref,
        has_inflight_push=await crud_lineages.publication_lineage_has_inflight_push(
            session, lineage
        ),
    )


async def read_publication_metadata(
    authority: PublicationReadAuthority,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> PublicationMetadata:
    """One bounded PR read using only identities resolved from durable state.

    A GitHub pull request is read with a token minted for its verified
    installation; any other through the code host its repository names.
    """

    if authority.repository.kind == GITHUB:
        return await _github_metadata(authority, settings=settings, client=client)
    code_host = forge_code_host(settings, client, authority.repository)
    try:
        async with asyncio.timeout(PRECHECK_TIMEOUT_SECONDS):
            pull = await code_host.read_pull_request(
                PullRequestRef(authority.repository, str(authority.pr_number))
            )
    except (ForgeError, TimeoutError):
        raise PublicationPrecheckUnavailable from None
    if (
        pull.ref.number != str(authority.pr_number)
        or pull.ref.number != authority.pr_id
        or pull.url != authority.pr_url
        or pull.state is not PullRequestState.OPEN
        or pull.head_ref != authority.branch
        or pull.head_sha != authority.expected_head
        or pull.base_ref != authority.base_ref
        or not pull.title.strip()
        or len(pull.title) > 256
        or len(pull.body) > 65_536
    ):
        raise PublicationPrecheckUnavailable
    return PublicationMetadata(pull.title, pull.body, datetime.now(UTC))


async def _github_metadata(
    authority: PublicationReadAuthority,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> PublicationMetadata:
    """One bounded GitHub PR GET for the verified installation."""

    assert authority.installation_id is not None
    # This resolver has its own short request budget. Reusing the general
    # resolver would inherit its longer configured timeout and shared locks.
    auth = GitHubAuth.from_settings(settings)
    resolver = GitHubCredentials(
        auth=replace(auth, timeout_s=min(auth.timeout_s, _PROVIDER_TIMEOUT_SECONDS))
    )
    try:
        async with asyncio.timeout(PRECHECK_TIMEOUT_SECONDS):
            token = await run_in_threadpool(
                resolver.token_for_verified_installation,
                authority.repo_full_name,
                authority.installation_id,
            )
            path = f"/repos/{repo_url_path(authority.repo_full_name)}/pulls/{authority.pr_number}"
            async with client.stream(
                "GET",
                settings.github_api_url.rstrip("/") + path,
                headers=github_headers(token),
                timeout=_PROVIDER_TIMEOUT_SECONDS,
                follow_redirects=False,
            ) as response:
                if response.status_code != 200:
                    raise PublicationPrecheckUnavailable
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > _MAX_RESPONSE_BYTES:
                        raise PublicationPrecheckUnavailable
                payload = json.loads(body)
    except (GitHubAppError, httpx.HTTPError, ValueError, TimeoutError):
        raise PublicationPrecheckUnavailable from None
    if not isinstance(payload, dict):
        raise PublicationPrecheckUnavailable
    head, base = payload.get("head"), payload.get("base")
    title, description = payload.get("title"), payload.get("body")
    if (
        type(payload.get("number")) is not int
        or payload["number"] != authority.pr_number
        or payload.get("node_id") != authority.pr_id
        or payload.get("html_url")
        != f"{settings.github_html_base}/{authority.repo_full_name}/pull/{authority.pr_number}"
        or payload.get("state") != "open"
        or payload.get("merged") is not False
        or not isinstance(head, dict)
        or not isinstance(base, dict)
        or head.get("ref") != authority.branch
        or head.get("sha") != authority.expected_head
        or base.get("ref") != authority.base_ref
        or not isinstance(title, str)
        or not title.strip()
        or len(title) > 256
        or "body" not in payload
        or (description is not None and not isinstance(description, str))
        or (isinstance(description, str) and len(description) > 65_536)
    ):
        raise PublicationPrecheckUnavailable
    try:
        title.encode("utf-8")
        (description or "").encode("utf-8")
    except UnicodeError:
        raise PublicationPrecheckUnavailable from None
    for side in (head, base):
        if not repository_identity_matches(
            side.get("repo"),
            repository_id=int(authority.repository.project_id),
            repo_full_name=authority.repo_full_name,
        ):
            raise PublicationPrecheckUnavailable
    return PublicationMetadata(title, description or "", datetime.now(UTC))
