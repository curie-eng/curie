"""Publish git-flow outcomes for the worker's Slack egress lane (#1331).

The API selects recipients from stored agent bindings. The stream carries no
credential and no Git stderr: the worker chooses the configured bot token for
the server-selected Slack identity and renders stable outcome codes.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from curie_internal.keyspace import DEPLOY_NOTICE_DEDUPE_PREFIX
from curie_telemetry import record_metric
from redis.asyncio import Redis
from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.crud import agents as crud_agents
from curie_api.schemas.deployments import WebhookResult

from .config import Settings
from .gitflow import environment_for_ref
from .models import Agent, AgentChannel, Deployment, DeployNoticeOutbox, Environment

logger = logging.getLogger(__name__)

_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_DEDUP_TTL_SECONDS = 31 * 86400
_OUTBOX_BATCH = 100
_OUTBOX_RETENTION = timedelta(days=30)
# The per-repository notice bound (docs/operations.md): at most this many
# notices, one per post to one channel, per repository in any rolling window.
_REPO_NOTICE_LIMIT = 20
_REPO_NOTICE_WINDOW_MINUTES = 60
_PUBLISH_ONCE = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  return false
end
local stream_id = redis.call('XADD', KEYS[2], '*', 'payload', ARGV[2])
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
return stream_id
"""


async def _changes_the_active_version(session: AsyncSession, result: WebhookResult) -> bool:
    """Whether this deployment replaced a different version in its environment.

    Git-flow appends an active row per push, so a redelivery of the active push
    deploys the same version again. Nothing changed underneath the channel, and
    nothing is announced. A first deployment, or one without the ids to compare,
    is a change.
    """

    if result.deployment_id is None or result.version_id is None or result.environment is None:
        return True
    previous = await session.scalar(
        select(Deployment.version_id)
        .where(
            Deployment.agent_id == result.agent_id,
            Deployment.environment == result.environment,
            Deployment.status == "active",
            Deployment.id != result.deployment_id,
        )
        .order_by(Deployment.deployed_at.desc(), Deployment.id.desc())
        .limit(1)
    )
    return previous != result.version_id


def _count_suppressed(reason: str) -> None:
    record_metric(
        "curie.deploy_notice.suppressed",
        attributes={"service.name": "curie-api", "reason": reason},
    )


async def _prod_bound_routes(
    session: AsyncSession, routes: set[tuple[str, str]]
) -> set[tuple[str, str]]:
    """The ``(kind, address)`` channels any agent with a live prod deployment binds.

    Keyed without the adapter: one channel reached through another bot
    identity is still the prod audience.
    """

    rows = await session.execute(
        select(AgentChannel.kind, AgentChannel.address)
        .join(Deployment, Deployment.agent_id == AgentChannel.agent_id)
        .where(
            Deployment.environment == Environment.prod,
            Deployment.status == "active",
            tuple_(AgentChannel.kind, AgentChannel.address).in_(sorted(routes)),
        )
        .distinct()
    )
    return {(kind, address) for kind, address in rows.all()}


class DeployNoticeQueue:
    """One atomic dedupe-and-enqueue per Slack binding and push outcome."""

    def __init__(
        self,
        redis: Redis,
        stream: str,
        sessionmaker: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        self._redis = redis
        self._stream = stream
        self._sessionmaker = sessionmaker

    async def publish(
        self,
        session: AsyncSession,
        result: WebhookResult,
        payload: dict[str, object],
        settings: Settings,
    ) -> int:
        """@spec docs/operations.md#automatically-with-git-flow."""
        if result.status not in {"deployed", "promoted", "rejected"}:
            return 0
        repo = payload.get("repository")
        full_name = repo.get("full_name") if isinstance(repo, dict) else None
        ref = payload.get("ref")
        sha = result.commit_sha or payload.get("after")
        if (
            not isinstance(full_name, str)
            or not isinstance(ref, str)
            or not isinstance(sha, str)
            or _SHA.fullmatch(sha) is None
        ):
            return 0

        if result.status == "rejected" and result.agent_id is not None:
            # Rejected after the push resolved its target: that agent alone.
            # One repository builds several agents (ADR-0091), and a sibling's
            # channel can be a different audience.
            agent = await crud_agents.get_agent(session, result.agent_id)
            agents = [agent] if agent is not None else []
        elif result.status == "rejected":
            agents = await crud_agents.get_agents_by_repo(session, full_name)
            if not agents and any(
                error.get("code") == "git.repository_case_mismatch"
                for error in (result.errors or [])
            ):
                agents = await crud_agents.get_agents_by_repo_casefold(session, full_name)
        else:
            if result.agent_id is None:
                return 0
            if not await _changes_the_active_version(session, result):
                return 0
            agent = await crud_agents.get_agent(session, result.agent_id)
            agents = [agent] if agent is not None else []

        environment = result.environment or environment_for_ref(ref, settings)
        codes = sorted(
            {error.get("code", "") for error in (result.errors or []) if error.get("code")}
        )
        recipients: list[tuple[Agent, AgentChannel]] = []
        for agent in agents:
            # A clone may finish after an operator changed opt-in or channels.
            # The webhook shares process_push's expire_on_commit=False session,
            # so an ordinary get_agent can return its stale identity-map row.
            await session.refresh(agent, attribute_names=["deploy_notifications", "channels"])
            if result.status != "rejected" and not agent.deploy_notifications:
                continue
            recipients.extend(
                (agent, binding)
                for binding in agent.channels
                if binding.kind == "slack" and binding.adapter is not None
            )
        if result.status == "rejected" and result.agent_id is None:
            # Unmatched: it may belong to any agent the repository builds, so a
            # prod channel never hears it (docs/operations.md).
            prod = await _prod_bound_routes(
                session, {(binding.kind, binding.address) for _, binding in recipients}
            )
            recipients = [
                (agent, binding)
                for agent, binding in recipients
                if (binding.kind, binding.address) not in prod
            ]
            if not recipients:
                logger.warning(
                    "deploy notice withheld: no non-prod channel for an unmatched rejection "
                    "repo=%s sha=%s codes=%s",
                    full_name,
                    sha[:12],
                    ",".join(codes),
                )
                _count_suppressed("no_nonprod_recipient")
                return 0
        rows: list[dict[str, str]] = []
        repo_key = full_name.casefold()
        for agent, binding in recipients:
            notice: dict[str, Any] = {
                "address": binding.address,
                "identity": binding.adapter,
                "agent_name": agent.name,
                "status": result.status,
                "sha": sha,
                "environment": environment.value if environment is not None else None,
                "codes": codes,
            }
            encoded = json.dumps(notice, sort_keys=True, separators=(",", ":"))
            # A success notice is keyed by its deployment, so a rollback to
            # an announced sha is announced again; retries of the same
            # deployment keep one key. A rejection has no deployment row.
            deployment = result.deployment_id or ""
            identity = (
                f"{self._stream}\0{full_name}\0{ref}\0{sha}\0{agent.id}"
                f"\0{deployment}\0{encoded}"
            )
            digest = hashlib.sha256(identity.encode()).hexdigest()
            notice["notice_key"] = digest
            encoded = json.dumps(notice, sort_keys=True, separators=(",", ":"))
            rows.append(
                {"key": digest, "stream": self._stream, "repo": repo_key, "payload": encoded}
            )
        if not rows:
            return 0
        # One writer per repository at a time, so two concurrent pushes cannot
        # both pass the bound. The lock is transaction scoped.
        await session.execute(
            select(
                func.pg_advisory_xact_lock(
                    func.hashtextextended(
                        json.dumps(["curie:deploy-notice", self._stream, repo_key]), 0
                    )
                )
            )
        )
        recorded = set(
            (
                await session.scalars(
                    select(DeployNoticeOutbox.key).where(
                        DeployNoticeOutbox.key.in_([row["key"] for row in rows])
                    )
                )
            ).all()
        )
        new_rows = [row for row in rows if row["key"] not in recorded]
        if new_rows:
            recent = await session.scalar(
                select(func.count())
                .select_from(DeployNoticeOutbox)
                .where(
                    DeployNoticeOutbox.stream == self._stream,
                    DeployNoticeOutbox.repo == repo_key,
                    DeployNoticeOutbox.created_at
                    > datetime.now(UTC) - timedelta(minutes=_REPO_NOTICE_WINDOW_MINUTES),
                )
            )
            if (recent or 0) + len(new_rows) > _REPO_NOTICE_LIMIT:
                await session.commit()
                logger.warning(
                    "deploy notice withheld: repository notice bound reached "
                    "repo=%s sha=%s status=%s bound=%d per %d minutes",
                    full_name,
                    sha[:12],
                    result.status,
                    _REPO_NOTICE_LIMIT,
                    _REPO_NOTICE_WINDOW_MINUTES,
                )
                _count_suppressed("rate_limited")
                if not recorded:
                    return 0
                return await self.reconcile_once(session, keys=sorted(recorded))
            # Persist the recipient decision before touching Valkey. A process
            # or Valkey outage leaves the row for the independent reconciler.
            await session.execute(
                insert(DeployNoticeOutbox).values(new_rows).on_conflict_do_nothing(
                    index_elements=[DeployNoticeOutbox.key]
                )
            )
        await session.commit()
        return await self.reconcile_once(session, keys=[row["key"] for row in rows])

    async def reconcile_once(
        self, session: AsyncSession, *, keys: list[str] | None = None
    ) -> int:
        statement = (
            select(DeployNoticeOutbox)
            .where(
                DeployNoticeOutbox.stream == self._stream,
                DeployNoticeOutbox.enqueued_at.is_(None),
            )
            .order_by(DeployNoticeOutbox.created_at, DeployNoticeOutbox.key)
            .limit(_OUTBOX_BATCH)
            .with_for_update(skip_locked=True)
        )
        if keys is not None:
            statement = statement.where(DeployNoticeOutbox.key.in_(keys))
        published = 0
        async with session.begin():
            for row in await session.scalars(statement):
                row.attempts += 1
                try:
                    stream_id = await self._redis.eval(
                        _PUBLISH_ONCE,
                        2,
                        f"{DEPLOY_NOTICE_DEDUPE_PREFIX}{row.key}",
                        row.stream,
                        _DEDUP_TTL_SECONDS,
                        row.payload,
                    )
                except Exception:  # noqa: BLE001 - existing broad catch retained
                    logger.warning("deploy notice outbox enqueue deferred; pending row retained")
                    break
                row.enqueued_at = datetime.now(UTC)
                if stream_id:
                    published += 1
            if keys is None:
                cutoff = datetime.now(UTC) - _OUTBOX_RETENTION
                old_keys = select(DeployNoticeOutbox.key).where(
                    DeployNoticeOutbox.stream == self._stream,
                    DeployNoticeOutbox.enqueued_at < cutoff,
                ).limit(_OUTBOX_BATCH)
                await session.execute(
                    delete(DeployNoticeOutbox).where(DeployNoticeOutbox.key.in_(old_keys))
                )
        return published

    async def run_forever(self, interval_s: float) -> None:
        assert self._sessionmaker is not None
        while True:
            try:
                async with self._sessionmaker() as session:
                    await self.reconcile_once(session)
            except Exception:
                logger.exception("deploy notice outbox pass failed; pending rows retained")
            await asyncio.sleep(interval_s)
