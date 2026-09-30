"""Publish git-flow outcomes for the worker's Slack egress lane (#1331).

The API selects recipients from stored agent bindings. The stream carries no
credential and no Git stderr: the worker chooses the configured bot token for
the server-selected Slack identity and renders stable outcome codes.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from . import crud
from .config import Settings
from .gitflow import environment_for_ref
from .schemas import WebhookResult

logger = logging.getLogger(__name__)

DEPLOY_NOTICE_STREAM = "curie:deploy-notices"
_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_DEDUP_TTL_SECONDS = 86400
_PUBLISH_ONCE = """
if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then
  return redis.call('XADD', KEYS[2], '*', 'payload', ARGV[2])
end
return false
"""


class DeployNoticeQueue:
    """One atomic dedupe-and-enqueue per Slack binding and push outcome."""

    def __init__(self, redis: Redis, stream: str = DEPLOY_NOTICE_STREAM) -> None:
        self._redis = redis
        self._stream = stream

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
        published = 0
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
                identity = f"{full_name}\0{ref}\0{sha}\0{agent.id}\0{encoded}"
                digest = hashlib.sha256(identity.encode()).hexdigest()
                key = f"curie:deploy-notice:dedupe:{digest}"
                stream_id = await self._redis.eval(
                    _PUBLISH_ONCE, 2, key, self._stream, _DEDUP_TTL_SECONDS, encoded
                )
                if stream_id:
                    published += 1
        return published
