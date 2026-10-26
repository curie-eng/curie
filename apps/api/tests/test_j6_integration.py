"""The shipped API invalidates the real dispatcher's cached team decision."""

import asyncio
import uuid

import httpx
import redis
from curie_api.config import get_settings
from curie_api.models import (
    Agent,
    AgentAccessPolicy,
    AgentChannel,
    ChannelIdentity,
    IdentityAuditEvent,
    IdentityConnectToken,
    IdentityLink,
    IdentityNamespace,
    Principal,
    PrincipalTeam,
    ProviderInstallation,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from curie_dispatcher.admission import AdmissionClient, AdmissionGate
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from .test_access_log_redaction import namespaced_identity_api  # noqa: F401 - real app fixture
from .test_team_authorization import seed


def test_real_api_snapshot_and_status_revoke_cached_dispatcher_allow(namespaced_identity_api):  # noqa: F811
    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def setup():
        async with factory() as session:
            person, team, _, _, binding, evidence = await seed(session)
            await session.commit()
            return person.tenant_id, team.id, binding.address, evidence.model_dump()

    tenant, team, address, evidence = asyncio.run(setup())
    cache = redis.Redis.from_url(get_settings().valkey_dsn())
    prefix = f"curie:j6-test:{uuid.uuid4().hex}"
    try:
        with httpx.Client(
            base_url=namespaced_identity_api.url, timeout=10, trust_env=False
        ) as http:
            gate = AdmissionGate(
                AdmissionClient(
                    api_base_url=namespaced_identity_api.url,
                    api_key="access-log-test-key",
                    client=http
                ),
                ttl_s=300,
                stale_s=600,
                redis_client=cache,
                key_prefix=prefix,
            )

            def refusal():
                return gate.refusal(
                    address=address, adapter="default", callers=["U0EXAMPLE1"], slack=evidence
                )

            assert refusal() is None
            assert refusal() is None
            admin = {"X-API-Key": "access-log-test-key"}
            credential = http.post(
                "/directory/credentials",
                headers=admin,
                json={
                    "tenant_id": str(tenant),
                    "expected_config_version": 1,
                    "team_ids": [str(team)],
                    "namespace_ids": [],
                },
            )
            assert credential.status_code == 201, credential.text
            scoped = {"X-Curie-Directory-Credential": credential.json()["credential"]}
            empty = http.put(
                f"/directory/teams/{team}/snapshot",
                headers=scoped,
                json={"expected_epoch": 1, "complete": True, "people": []},
            )
            assert empty.status_code == 200, empty.text
            assert refusal() is not None
            restored = http.put(
                f"/directory/teams/{team}/snapshot",
                headers=scoped,
                json={"expected_epoch": 2, "complete": True, "people": [{"subject": "member"}]},
            )
            assert restored.status_code == 200, restored.text
            assert refusal() is None
            disabled = http.post(
                "/directory/people/status",
                headers=scoped,
                json={
                    "subject": "member",
                    "expected_version": 1,
                    "status": "disabled",
                },
            )
            assert disabled.status_code == 200, disabled.text
            assert refusal() is not None
            stale = http.post(
                "/directory/people/status",
                headers=scoped,
                json={"subject": "member", "expected_version": 1, "status": "active"},
            )
            assert stale.status_code == 409, stale.text
            assert refusal() is not None
            restored_status = http.post(
                "/directory/people/status",
                headers=scoped,
                json={"subject": "member", "expected_version": 2, "status": "active"},
            )
            assert restored_status.status_code == 200
            assert refusal() is None

            # A decision waiting behind a writer must read the binding AFTER
            # acquiring its reader lock, rather than retain a pre-lock allow.
            async def concurrent_list_removal():
                from concurrent.futures import ThreadPoolExecutor

                import redis.asyncio as async_redis
                from curie_api import identity_fence
                from sqlalchemy import select, text

                writer_cache = async_redis.from_url(get_settings().valkey_dsn())
                try:
                    with ThreadPoolExecutor(max_workers=1) as pool:
                        async with identity_fence.mutation(factory, writer_cache):
                            pending = pool.submit(
                                http.post,
                                "/channels/admission",
                                headers=admin,
                                json={
                                    "kind": "slack",
                                    "address": address,
                                    "adapter": "default",
                                    "callers": ["U0EXAMPLE1"],
                                    "slack": evidence,
                                },
                            )
                            async with factory() as session:
                                for _ in range(100):
                                    waiting = await session.scalar(
                                        text(
                                            "SELECT EXISTS (SELECT 1 FROM pg_locks "
                                            "WHERE locktype='advisory' AND objid=2250220 "
                                            "AND NOT granted)"
                                        )
                                    )
                                    if waiting:
                                        break
                                    await asyncio.sleep(0.02)
                                assert waiting, "admission did not wait behind the identity writer"
                                binding = await session.scalar(
                                    select(AgentChannel).where(AgentChannel.tenant_id == tenant)
                                )
                                binding.allowed_callers = ["U0EXAMPLE2"]
                                await session.commit()
                        response = await asyncio.to_thread(pending.result, timeout=5)
                        assert response.status_code == 200
                        assert not response.json()["allowed"]
                finally:
                    await writer_cache.aclose()

            asyncio.run(concurrent_list_removal())
    finally:
        keys = list(cache.scan_iter(prefix + "*"))
        if keys:
            cache.delete(*keys)
        cache.close()

        async def cleanup():
            async with factory() as session:
                for model in (
                    IdentityConnectToken,
                    IdentityAuditEvent,
                    IdentityLink,
                    PrincipalTeam,
                    AgentAccessPolicy,
                    AgentChannel,
                    ChannelIdentity,
                    ProviderInstallation,
                    IdentityNamespace,
                    Agent,
                    Team,
                    Principal,
                    TenantIdentityProvider,
                    Tenant,
                ):
                    key = model.id if model is Tenant else model.tenant_id
                    await session.execute(delete(model).where(key == tenant))
                await session.commit()
            await engine.dispose()

        asyncio.run(cleanup())


def test_incomplete_unauthenticated_body_cannot_block_other_admission(namespaced_identity_api):  # noqa: F811
    import socket
    from urllib.parse import urlsplit

    origin = urlsplit(namespaced_identity_api.url)
    with socket.create_connection((origin.hostname, origin.port), timeout=3) as slow:
        slow.sendall(
            b"POST /directory/teams HTTP/1.1\r\nHost: localhost\r\n"
            b"Content-Type: application/json\r\nContent-Length: 1000\r\n\r\n{"
        )
        with httpx.Client(base_url=namespaced_identity_api.url, timeout=2, trust_env=False) as http:
            response = http.post(
                "/channels/admission",
                headers={"X-API-Key": "access-log-test-key"},
                json={"kind": "slack", "address": "C0EXAMPLE9", "callers": ["U0EXAMPLE9"]},
            )
            assert response.status_code == 200
