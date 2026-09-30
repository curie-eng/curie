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

from redis.asyncio import Redis
from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from . import crud
from .config import Settings
from .gitflow import environment_for_ref
from .models import DeployNoticeOutbox
from .schemas import WebhookResult

logger = logging.getLogger(__name__)

_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_DEDUP_TTL_SECONDS = 86400
_OUTBOX_BATCH = 100
_OUTBOX_RETENTION = timedelta(days=30)
_PUBLISH_ONCE = """
if redis.call('EXISTS', KEYS[1]) == 1 then
  return false
end
local stream_id = redis.call('XADD', KEYS[2], '*', 'payload', ARGV[2])
redis.call('SET', KEYS[1], '1', 'EX', ARGV[1])
return stream_id
"""


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

        if result.status == "rejected":
            agents = await crud.get_agents_by_repo(session, full_name)
            if not agents and any(
                error.get("code") == "git.repository_case_mismatch"
                for error in (result.errors or [])
            ):
                agents = await crud.get_agents_by_repo_casefold(session, full_name)
        else:
            if result.agent_id is None:
                return 0
            agent = await crud.get_agent(session, result.agent_id)
            agents = [agent] if agent is not None else []

        environment = result.environment or environment_for_ref(ref, settings)
        codes = sorted(
            {error.get("code", "") for error in (result.errors or []) if error.get("code")}
        )
        rows: list[dict[str, str]] = []
        for agent in agents:
            # A clone may finish after an operator changed opt-in or channels.
            # The webhook shares process_push's expire_on_commit=False session,
            # so an ordinary get_agent can return its stale identity-map row.
            await session.refresh(agent, attribute_names=["deploy_notifications", "channels"])
            if result.status != "rejected" and not agent.deploy_notifications:
                continue
            for binding in agent.channels:
                if binding.kind != "slack" or binding.adapter is None:
                    continue
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
                identity = f"{self._stream}\0{full_name}\0{ref}\0{sha}\0{agent.id}\0{encoded}"
                digest = hashlib.sha256(identity.encode()).hexdigest()
                rows.append({"key": digest, "stream": self._stream, "payload": encoded})
        if not rows:
            return 0
        # Persist the recipient decision before touching Valkey. A process or
        # Valkey outage leaves the row for the independent API reconciler.
        await session.execute(
            insert(DeployNoticeOutbox).values(rows).on_conflict_do_nothing(
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
                        f"curie:deploy-notice:dedupe:{row.key}",
                        row.stream,
                        _DEDUP_TTL_SECONDS,
                        row.payload,
                    )
                except Exception:
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
