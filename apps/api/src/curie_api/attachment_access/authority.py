"""Opaque capabilities backed by owner-fenced short live leases.

Only hashes of credentials and tickets enter the ledger. A steer keeps the
opener; revocation tombstones that opener so a delayed open cannot revive it.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from typing import Any

from aci_protocol.turn import route_identity
from channel_protocol import parse_scoped_conversation_id
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..config import get_settings
from ..models import Agent, AgentChannel, Deployment
from ..schemas.attachment_access import Context, Owner
from .catalog import live_catalog, refused

LEASE_SECONDS = 90
OPEN = """
if redis.call('EXISTS', KEYS[3]) == 1 then return 0 end
local old = redis.call('GET', KEYS[1])
if old and old ~= ARGV[1] then
  local prior = redis.call('GET', ARGV[5] .. old)
  if prior then
    local p = cjson.decode(prior)
    redis.call('SET', p.tombstone, '1', 'EX', 3600)
  end
end
redis.call('SET', KEYS[2], ARGV[2], 'EX', ARGV[3], 'NX')
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[4])
return 1
"""
RENEW = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
local raw = redis.call('GET', KEYS[2])
if not raw then return 0 end
if ARGV[3] ~= '' then
  local state = cjson.decode(raw)
  state.current_event_id = ARGV[3]
  redis.call('SET', KEYS[2], cjson.encode(state), 'KEEPTTL')
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
return 1
"""
REVOKE = """
redis.call('SET', KEYS[2], '1', 'EX', 3600)
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
"""


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


class Authority:
    def __init__(self, client: Redis) -> None:
        self.client = client
        self.prefix = get_settings().worker_key_prefix + ":attachment-access:"

    def key(self, purpose: str, value: str) -> str:
        return self.prefix + purpose + ":" + digest(value)

    def credential(self, owner: Owner) -> str:
        # Deterministic only for a trusted worker's unique opener, allowing
        # idempotent mint without persisting a replayable credential.
        raw = owner.model_dump_json(include={"agent_id", "thread_key", "event_id", "owner"})
        return (
            "ata."
            + hmac.new(
                get_settings().api_key.encode(),
                ("attachment.access:" + raw).encode(),
                hashlib.sha256,
            ).hexdigest()
        )

    def active_key(self, owner: Owner) -> str:
        return self.key("active", str(owner.agent_id) + ":" + owner.thread_key)

    def tombstone(self, owner: Owner) -> str:
        return self.key("ended", str(owner.agent_id) + ":" + owner.owner)

    async def open(
        self, data: Context, binding: AgentChannel, *, catalog_id: str
    ) -> dict[str, Any]:
        token = self.credential(data)
        token_hash = digest(token)
        held = await self.client.get(self.prefix + "context:" + token_hash)
        if held:
            old: dict[str, Any] = json.loads(held)
            previous = Context.model_validate(old["request"])
            if previous.model_dump(exclude={"ttl_s"}) != data.model_dump(exclude={"ttl_s"}):
                raise refused("owner_changed")
            if not await self.renew(data):
                raise refused("turn_inactive")
            return old
        state = {
            "request": data.model_dump(mode="json"),
            "exp": int(time.time()) + data.ttl_s,
            "catalog_id": catalog_id,
            "current_event_id": data.event_id,
            "binding_id": str(binding.id),
            "binding_generation": binding.generation,
            "token_hash": token_hash,
            "tombstone": self.tombstone(data),
        }
        ok = await self.client.eval(
            OPEN,
            3,
            self.active_key(data),
            self.prefix + "context:" + token_hash,
            self.tombstone(data),
            token_hash,
            json.dumps(state),
            data.ttl_s,
            min(LEASE_SECONDS, data.ttl_s),
            self.prefix + "context:",
        )
        if not ok:
            raise refused("turn_inactive")
        return state

    async def renew(self, data: Owner, current_event_id: str | None = None) -> bool:
        token_hash = digest(self.credential(data))
        raw = await self.client.get(self.prefix + "context:" + token_hash)
        if not raw:
            return False
        state = json.loads(raw)
        remaining = state["exp"] - int(time.time())
        if remaining <= 0:
            return False
        return bool(
            await self.client.eval(
                RENEW,
                2,
                self.active_key(data),
                self.prefix + "context:" + token_hash,
                token_hash,
                min(LEASE_SECONDS, remaining),
                current_event_id or "",
            )
        )

    async def revoke(self, data: Owner) -> bool:
        return bool(
            await self.client.eval(
                REVOKE,
                2,
                self.active_key(data),
                self.tombstone(data),
                digest(self.credential(data)),
            )
        )

    async def read_hash(self, token_hash: str) -> dict[str, Any]:
        raw = await self.client.get(self.prefix + "context:" + token_hash)
        if raw is None:
            raise refused("invalid_capability", 401)
        state: dict[str, Any] = json.loads(raw)
        data = Context.model_validate(state["request"])
        active = await self.client.get(self.active_key(data))
        if isinstance(active, bytes):
            active = active.decode()
        if active != token_hash or state["exp"] <= time.time():
            raise refused("turn_inactive")
        return state

    async def read(self, token: str | None) -> dict[str, Any]:
        if not token or not token.startswith("ata.") or len(token) != 68:
            raise refused("invalid_capability", 401)
        return await self.read_hash(digest(token))


async def current_binding(db: AsyncSession, data: Context | Any) -> AgentChannel:
    parsed = parse_scoped_conversation_id(data.thread_key)
    if parsed is None:
        raise refused("thread_invalid")
    agent = await db.get(Agent, data.agent_id, populate_existing=True)
    deployment = await db.get(Deployment, data.deployment_id, populate_existing=True)
    if (
        agent is None
        or agent.status != "active"
        or deployment is None
        or deployment.agent_id != data.agent_id
        or deployment.status != "active"
        or deployment.tenant_id != agent.tenant_id
    ):
        raise refused("deployment_inactive")
    rows = await db.scalars(
        select(AgentChannel)
        .where(
            AgentChannel.agent_id == data.agent_id,
            AgentChannel.kind == parsed.kind,
            AgentChannel.address == parsed.address,
            AgentChannel.tenant_id == agent.tenant_id,
        )
        .execution_options(populate_existing=True)
    )
    for row in rows:
        if route_identity(row.kind, row.adapter) == (parsed.identity or "default"):
            return row
    raise refused("binding_revoked")


async def validate_route(client: Redis, data: Context) -> None:
    key = get_settings().attachment_sandbox_key_prefix + ":route:" + data.thread_key
    raw = await client.get(key)
    if raw is None:
        raise refused("route_changed")
    try:
        route = json.loads(raw)
        if (
            route.get("state", "live"),
            route.get("session_id"),
            route.get("sandbox_name"),
            route.get("generation", 0),
            route.get("thread_key"),
        ) != ("live", data.session_id, data.sandbox_name, data.route_generation, data.thread_key):
            raise refused("route_changed")
    except (TypeError, ValueError):
        raise refused("route_changed") from None


async def validate(db: AsyncSession, authority: Authority, state: dict[str, Any]) -> Context:
    await authority.read_hash(state["token_hash"])
    data = Context.model_validate(state["request"])
    incarnation = await live_catalog(db, data.agent_id, data.thread_key)
    if incarnation is None or str(incarnation.id) != state["catalog_id"]:
        raise refused("catalog_reset")
    await validate_route(authority.client, data)
    binding = await current_binding(db, data)
    if str(binding.id) != state["binding_id"] or binding.generation != state["binding_generation"]:
        raise refused("binding_revoked")
    return data
