"""Kill switch over Valkey (L1).

Implements the coordinator-fixed seam so the worker slice can consume it:
- flag: SET ``curie:kill:<agent_id>`` = "1" with no TTL (persists until resume)
- event: PUBLISH on ``curie:kill-events`` a JSON {agent_id, action, ts}

The worker subscribes to the channel and interrupts live turns for the agent;
this module only produces the signal.
"""

import json
import uuid
from datetime import UTC, datetime

import redis.asyncio as redis
from curie_internal.keyspace import KILL_CHANNEL, kill_key


class KillSwitch:
    def __init__(self, client: redis.Redis) -> None:
        self._client = client

    async def _publish(self, agent_id: uuid.UUID, action: str) -> None:
        event = {
            "agent_id": str(agent_id),
            "action": action,
            "ts": datetime.now(UTC).isoformat(),
        }
        await self._client.publish(KILL_CHANNEL, json.dumps(event))

    async def kill(self, agent_id: uuid.UUID) -> None:
        """Set the kill flag (no TTL) and publish a kill event. Idempotent."""

        await self._client.set(kill_key(agent_id), "1")
        await self._publish(agent_id, "kill")

    async def resume(self, agent_id: uuid.UUID) -> None:
        """Clear the kill flag and publish a resume event. Idempotent."""

        await self._client.delete(kill_key(agent_id))
        await self._publish(agent_id, "resume")

    async def is_killed(self, agent_id: uuid.UUID) -> bool:
        return bool(await self._client.exists(kill_key(agent_id)))
