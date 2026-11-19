"""Coordinate installed HTTP requests using independently observed database locks."""

import asyncio
import json
import uuid

import asyncpg
import httpx
from curie_api.config import get_settings
from curie_protected_hooks.source_policy_records import target_intent_sha256


async def coordinate(spec):
    settings = get_settings()
    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    holder = await asyncpg.connect(dsn)
    probe = await asyncpg.connect(dsn)
    tasks = []
    agent, hook, action = spec["agent"], spec["hook"], spec["action"]
    hook_key = f"{agent}:{hook}"
    gate_key = f"hook-source:{agent}"

    async def waiting(key, count=1):
        async with asyncio.timeout(15):
            while True:
                observed = await probe.fetchval(
                    "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted "
                    "AND classid::bigint=((hashtextextended($1,0)>>32)&4294967295) "
                    "AND objid::bigint=(hashtextextended($1,0)&4294967295) AND objsubid=1",
                    key,
                )
                if observed >= count:
                    return

    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=30) as client:

        async def wake():
            response = await client.post(
                f"/hooks/{agent}/{hook}/wake", content=b"{}", headers=spec["headers"]
            )
            return {"status": response.status_code, "body": response.json()}

        async def control():
            path = (
                f"/agents/{agent}/hooks/{hook}/wake-secret/rotate"
                if action == "rotate"
                else f"/schedules/{agent}/{hook}/pause"
            )
            response = await client.post(path, headers={"X-Api-Key": settings.api_key})
            return {"status": response.status_code, "body": response.json()}

        try:
            async with holder.transaction():
                await holder.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                    gate_key if action == "source" else hook_key,
                )
                if action != "source":
                    tasks.append(asyncio.create_task(control()))
                    await waiting(hook_key)
                tasks.extend(asyncio.create_task(wake()) for _ in range(4))
                if action == "pause":
                    await waiting(hook_key, 2)
                else:
                    await waiting(gate_key)
                if action == "source":
                    operation = uuid.uuid4()
                    target = dict(
                        mode="ordinary",
                        tool_access=None,
                        runtime_id=None,
                        qualification_id=None,
                        bundle_digest=None,
                    )
                    await holder.execute(
                        "INSERT INTO curie.hook_source_operations "
                        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
                        "VALUES($1,$2,$3,$4,'committed',1)",
                        uuid.UUID(agent),
                        hook,
                        operation,
                        target_intent_sha256(target),
                    )
                    await holder.execute(
                        "INSERT INTO curie.hook_source_policies "
                        "(agent_id,hook,operation_id,generation,mode,legacy_generation) "
                        "VALUES($1,$2,$3,1,'ordinary',0)",
                        uuid.UUID(agent),
                        hook,
                        operation,
                    )
            responses = await asyncio.gather(*tasks)
            print(json.dumps(responses))
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await holder.close()
            await probe.close()
