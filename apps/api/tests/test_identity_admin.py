"""Read-only identity administration stays tenant scoped and bounded."""

import asyncio
import uuid

from curie_api.config import get_settings
from curie_api.models import Principal, Tenant
from curie_api.routers.identity_admin import identity_view
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


def test_people_view_never_returns_another_tenant(migrated):
    async def check():
        engine = create_async_engine(get_settings().database_url)
        try:
            async with async_sessionmaker(engine)() as session:
                one = Tenant(deployment_id=f"admin-one-{uuid.uuid4()}")
                two = Tenant(deployment_id=f"admin-two-{uuid.uuid4()}")
                session.add_all([one, two])
                await session.flush()
                session.add_all(
                    [
                        Principal(
                            tenant_id=one.id,
                            idp_issuer="https://idp.example.com",
                            type="human",
                            idp_subject="one",
                        ),
                        Principal(
                            tenant_id=two.id,
                            idp_issuer="https://idp.example.com",
                            type="human",
                            idp_subject="two",
                        ),
                    ]
                )
                await session.flush()
                result = await identity_view(one.id, "people", session, limit=1, after=None)
                assert len(result["items"]) == 1
                assert result["items"][0]["idp_subject"] == "one"
                assert result["next_cursor"] == result["items"][0]["id"]
        finally:
            await engine.dispose()

    asyncio.run(check())
