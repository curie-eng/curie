"""Read current factory publication facts without updating durable lineage."""

from __future__ import annotations

import asyncio
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from starlette.concurrency import run_in_threadpool

from . import crud
from .config import Settings
from .github_app import GitHubAppError, GitHubCredentials
from .github_review_truth import github_headers, repository_identity_matches
from .models import Deployment, ExecutionRequest, ThreadPublicationLineage, WorkItem
from .repo_full_name import repo_url_path

PRECHECK_TIMEOUT_SECONDS = 8.0
_PROVIDER_TIMEOUT_SECONDS = 2.0
_MAX_RESPONSE_BYTES = 1_048_576


class PublicationPrecheckRefused(RuntimeError):
    """The durable execution no longer grants this read authority."""


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
    branch: str
    repository_id: int
    installation_id: int
    pr_node_id: str
    base_ref: str
    has_inflight_push: bool


@dataclass(frozen=True)
class PublicationMetadata:
    title: str
    body: str
    observed_at: datetime


async def read_publication_authority(
    session: AsyncSession,
    *,
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

    other_lineage = aliased(ThreadPublicationLineage)
    existing_pr = (
        select(other_lineage.id)
        .where(
            other_lineage.agent_id == WorkItem.agent_id,
            other_lineage.conversation_id == WorkItem.conversation_id,
            func.lower(other_lineage.repo_full_name) == func.lower(WorkItem.repo_full_name),
            other_lineage.pr_number.is_not(None),
        )
        .correlate(WorkItem)
        .exists()
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
    deployment, item, execution, lineage, now, has_existing_pr = row
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
        or item.repo_full_name.casefold() != lineage.repo_full_name.casefold()
        or lineage.status != "open"
    ):
        raise PublicationPrecheckRefused
    if lineage is None or lineage.pr_number is None:
        # An absent WorkItem link must not hide an existing conversation PR.
        # The correlated existence check uses the same snapshot as the lease.
        if has_existing_pr or (lineage is None and item.publication_lineage_id is not None):
            raise PublicationPrecheckRefused
        return None
    if (
        lineage.pr_number <= 0
        or lineage.pr_url != f"https://github.com/{lineage.repo_full_name}/pull/{lineage.pr_number}"
        or lineage.head_sha is None
        or re.fullmatch(r"[0-9a-f]{40}", lineage.head_sha) is None
        or lineage.github_repository_id is None
        or lineage.github_installation_id is None
        or not lineage.github_pr_node_id
        or not lineage.base_ref
    ):
        raise PublicationPrecheckUnavailable
    if (
        item.github_repository_id != lineage.github_repository_id
        or item.github_installation_id != lineage.github_installation_id
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
        branch=lineage.branch,
        repository_id=lineage.github_repository_id,
        installation_id=lineage.github_installation_id,
        pr_node_id=lineage.github_pr_node_id,
        base_ref=lineage.base_ref,
        has_inflight_push=await crud.publication_lineage_has_inflight_push(session, lineage),
    )


async def read_publication_metadata(
    authority: PublicationReadAuthority,
    *,
    settings: Settings,
    client: httpx.AsyncClient,
) -> PublicationMetadata:
    """One bounded PR GET using only identities resolved from durable state."""

    # This resolver has its own short request budget. Reusing the general
    # resolver would inherit its longer configured timeout and shared locks.
    resolver = GitHubCredentials(
        settings=settings.model_copy(
            update={
                "github_app_timeout_seconds": min(
                    settings.github_app_timeout_seconds, _PROVIDER_TIMEOUT_SECONDS
                ),
            }
        )
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
        or payload.get("node_id") != authority.pr_node_id
        or payload.get("html_url")
        != f"https://github.com/{authority.repo_full_name}/pull/{authority.pr_number}"
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
            repository_id=authority.repository_id,
            repo_full_name=authority.repo_full_name,
        ):
            raise PublicationPrecheckUnavailable
    return PublicationMetadata(title, description or "", datetime.now(UTC))
