"""Independent API writer ownership proof. @spec IDENTITY-WRITER-LIFETIME-V1-1 to -10."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
import pytest
from curie_api import identity_fence
from curie_api.crud import agents as agent_crud
from curie_api.main import create_app
from identity_writer_lifetime_support import ConnectionObserver, agents, headers, models, run

from .identity_fence_support import identity_fence_backend  # noqa: F401


def test_protected_handler_cannot_commit_after_its_lock_connection_is_invalidated(
    migrated: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """@spec IDENTITY-WRITER-LIFETIME-V1-4/10. A late independent commit is the regression."""

    async def body() -> None:
        """@spec IDENTITY-WRITER-LIFETIME-V1-4/10. Drive the real app and existing PATCH handler."""
        app = create_app()
        async with app.router.lifespan_context(app):
            factory = app.state.sessionmaker
            ids = await agents(factory)
            entered, release = asyncio.Event(), asyncio.Event()
            original = agent_crud.update_agent_model

            async def paused(session: Any, agent: Any, model: Any) -> Any:
                """@spec IDENTITY-WRITER-LIFETIME-V1-10. Delegate the actual product write."""
                if agent.id == ids[0]:
                    entered.set()
                    await release.wait()
                return await original(session, agent, model)

            with monkeypatch.context() as observed:
                observed.setattr(agent_crud, "update_agent_model", paused)
                with ConnectionObserver(app.state.engine, observed) as connections:
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
                        base_url="http://testserver",
                    ) as client:
                        first = asyncio.create_task(
                            client.patch(
                                f"/agents/{ids[0]}",
                                headers=headers(),
                                json={"model": "synthetic-late-first"},
                            )
                        )
                        try:
                            await asyncio.wait_for(entered.wait(), timeout=5)
                            owner = await connections.lock_owner()
                            await owner.invalidate()
                            second = await asyncio.wait_for(
                                client.patch(
                                    f"/agents/{ids[1]}",
                                    headers=headers(),
                                    json={"model": "synthetic-valid-second"},
                                ),
                                timeout=5,
                            )
                            assert second.status_code == 200, second.text
                            successor = await app.state.valkey.get(identity_fence.FENCE_KEY)
                            assert successor.startswith(b"stable:")
                            release.set()
                            first_response = await asyncio.wait_for(first, timeout=5)
                            # Response can already be an error; persisted state is authoritative.
                            persisted = await models(factory, ids)
                            assert persisted == ["synthetic-original", "synthetic-valid-second"], (
                                "late writer committed after its serialization connection was lost",
                                persisted,
                                first_response.status_code,
                            )
                            assert await app.state.valkey.get(identity_fence.FENCE_KEY) == successor
                        finally:
                            release.set()
                            if not first.done():
                                first.cancel()
                                await asyncio.gather(first, return_exceptions=True)

    run(body)
