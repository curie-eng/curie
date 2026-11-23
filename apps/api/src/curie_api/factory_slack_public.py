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
    bind = {"item": work_item_id, "sequence": sequence}
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(CAST(:item AS text), 194))"),
        {"item": str(work_item_id)},
    )
    await session.execute(
        text(f"""
        INSERT INTO {SCHEMA}.factory_slack_public_threads
            (work_item_id, agent_id, channel_identity, channel, first_sequence)
        SELECT w.id, w.agent_id, b.identity, a.factory_public_slack_channel, :sequence
          FROM {SCHEMA}.work_items w JOIN {SCHEMA}.agents a ON a.id = w.agent_id
          CROSS JOIN LATERAL (
            SELECT min(c.adapter) AS identity FROM {SCHEMA}.agent_channels c
             WHERE c.agent_id = a.id AND c.kind = 'slack'
               AND c.address = a.factory_public_slack_channel AND c.adapter IS NOT NULL
             HAVING count(*) = 1
          ) b
         WHERE w.id = :item AND a.factory_slack_since IS NOT NULL
        ON CONFLICT (work_item_id) DO NOTHING
    """),
        bind,
    )
    # A kickoff's placeholder is recorded after request allocation, in the same transaction.
    old = (
        await session.execute(
            text(f"""
        SELECT t.work_item_id
          FROM {SCHEMA}.factory_slack_threads t
          JOIN {SCHEMA}.factory_slack_public_threads p
            USING (work_item_id, channel_identity, channel)
         WHERE t.work_item_id = :item
           AND (p.root_ts IS NULL OR p.root_ts = t.root_ts)
         FOR UPDATE OF t, p
    """),
            bind,
        )
    ).first()
    if old is None:
        return
    await session.execute(
        text(f"""
        UPDATE {SCHEMA}.factory_slack_public_threads p
           SET root_ts = COALESCE(p.root_ts, t.root_ts),
               card_ts = COALESCE(p.card_ts, t.card_ts), origin = t.origin,
               requester_slack_user = t.requester_slack_user,
               card_upgrade_cutoff = t.card_upgrade_cutoff
          FROM {SCHEMA}.factory_slack_threads t
         WHERE p.work_item_id = :item AND t.work_item_id = p.work_item_id
    """),
        bind,
    )
    await session.execute(
        text(f"""
        INSERT INTO {SCHEMA}.factory_slack_public_posts
            (work_item_id, post_key, kind, sequence, rank, ordinal, payload, attempts,
             not_before, posted_at, posted_ref, failed_at, created_at)
        SELECT work_item_id, post_key, kind, sequence, rank, ordinal, '{{}}'::jsonb, attempts,
               not_before, posted_at, posted_ref, failed_at, created_at
          FROM {SCHEMA}.factory_slack_posts
         WHERE work_item_id = :item AND last_error IS DISTINCT FROM 'transferred_to_public'
        ON CONFLICT (work_item_id, post_key) DO NOTHING
    """),
        bind,
    )
    await session.execute(
        text(f"""
        UPDATE {SCHEMA}.factory_slack_posts SET failed_at = now(),
               last_error = 'transferred_to_public', payload = '{{}}'::jsonb,
               lease_owner = NULL, lease_expires_at = NULL
         WHERE work_item_id = :item AND posted_at IS NULL AND failed_at IS NULL
    """),
        bind,
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
