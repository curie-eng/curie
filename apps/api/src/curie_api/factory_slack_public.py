"""Reserve the public Slack destination while its new request still holds the item lock."""

import uuid

import httpx
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from .config import Settings
from .db import SCHEMA
from .forges.errors import ForgeError
from .forges.hosts import factory_code_host_entry
from .forges.registry import forges_registry
from .forges.types import CiRollup, PullRequestRef
from .models import FactorySlackPublicThread, ThreadPublicationLineage, WorkItem


async def reserve_public_root(
    session: AsyncSession, work_item_id: uuid.UUID, sequence: int
) -> None:
    await session.execute(
        text(f"SELECT {SCHEMA}.reserve_factory_slack_public_root(:item, :sequence, false)"),
        {"item": work_item_id, "sequence": sequence},
    )


async def public_publication(
    session: AsyncSession, work_item_id: uuid.UUID, *, settings: Settings, client: httpx.AsyncClient
) -> dict[str, str | int]:
    row = (
        await session.execute(
            select(WorkItem, ThreadPublicationLineage)
            .join(FactorySlackPublicThread, FactorySlackPublicThread.work_item_id == WorkItem.id)
            .join(
                ThreadPublicationLineage,
                ThreadPublicationLineage.id == WorkItem.publication_lineage_id,
            )
            .where(WorkItem.id == work_item_id)
        )
    ).one_or_none()
    if row is None:
        return {}
    item, lineage = row
    repository = item.repository
    installation = item.code_host_installation_id
    if (
        repository.kind != "github"
        or not repository.project_id.isdecimal()
        or installation is None
        or lineage.repository != repository
        or lineage.code_host_installation_id != installation
        or not lineage.pr_number
        or lineage.status != "open"
    ):
        return {}
    try:
        entry = factory_code_host_entry(forges_registry(settings))
        if (
            entry.config.host != repository.host
            or await entry.installation_for(repository) != installation
        ):
            return {}
        host = entry.adapter(
            client, installations={repository.path: installation}, repository=repository
        )
        resolved = await host.resolve_repository(repository.project_id)
        if (
            resolved != repository
            or resolved.path.casefold() != repository.path.casefold()
            or not resolved.is_public
        ):
            return {}
        ref = PullRequestRef(resolved, str(lineage.pr_number))
        pull = await host.read_pull_request(ref)
        if pull.ref != ref or pull.head_ref != lineage.branch or pull.state != "open":
            return {}
        facts: dict[str, str | int] = {
            "url": pull.url,
            "number": lineage.pr_number,
            "head_sha": pull.head_sha,
        }
        try:
            observed = await host.observe_ci(resolved, pull.head_sha)
        except ForgeError:
            return facts
        if observed.head_sha == pull.head_sha:
            facts["checks"] = CiRollup.on_head(pull.head_sha, observed.checks).state.value
        return facts
    except ForgeError:
        return {}
