"""On-demand attachment facade: real Postgres and Valkey, synthetic provider."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
import redis.asyncio as redis
from channel_protocol import scoped_conversation_id
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_api.models import Agent, AgentChannel, AgentVersion, Deployment, Environment
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

THREAD = scoped_conversation_id("slack", "C0EXAMPLE1", "1700000000.000100")
INTERNAL = "/v1/internal/attachment-access"


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
async def access(clean_db: None, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[Any]:
    monkeypatch.setenv("KEY_PREFIX", f"curie:worker:attachment-test-{uuid.uuid4().hex}")
    monkeypatch.setenv("ATTACHMENT_ACCESS_ENABLED", "true")
    monkeypatch.setenv("ATTACHMENT_BROKER_URL", "http://127.0.0.1:8093")
    get_settings.cache_clear()
    settings = get_settings()
    engine = create_async_engine(settings.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    valkey = redis.Redis(
        host=settings.valkey_host, port=settings.valkey_port, decode_responses=True
    )
    app = create_app()
    app.state.sessionmaker = sessions
    app.state.valkey = valkey
    async with sessions() as db:
        agent = Agent(name=f"attachment-{uuid.uuid4().hex}")
        db.add(agent)
        await db.flush()
        version = AgentVersion(agent_id=agent.id, version_label="test", created_by="test")
        db.add(version)
        db.add(
            AgentChannel(agent_id=agent.id, kind="slack", address="C0EXAMPLE1", adapter="default")
        )
        await db.flush()
        deployment = Deployment(
            agent_id=agent.id, version_id=version.id, environment=Environment.dev
        )
        db.add(deployment)
        await db.commit()
        aid, did = str(agent.id), str(deployment.id)
    route_key = f"curie:sandbox:route:{THREAD}"
    await valkey.set(
        route_key,
        json.dumps(
            {
                "state": "live",
                "session_id": "session-one",
                "sandbox_name": "acme-sandbox",
                "generation": 1,
                "thread_key": THREAD,
            }
        ),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield {
            "client": client,
            "aid": aid,
            "did": did,
            "sessions": sessions,
            "valkey": valkey,
            "app": app,
        }
    await valkey.delete(route_key)
    owned = [key async for key in valkey.scan_iter(settings.worker_key_prefix + ":*")]
    if owned:
        await valkey.delete(*owned)
    await valkey.aclose()
    await engine.dispose()
    get_settings.cache_clear()


def worker() -> dict[str, str]:
    return {"X-Curie-Worker-Token": get_settings().internal_worker_token}


def registration(aid: str) -> dict[str, Any]:
    return {
        "agent_id": aid,
        "thread_key": THREAD,
        "event_id": "event-one",
        "refs": [
            {
                "file_id": "FEXAMPLE1",
                "ordinal": 0,
                "name": "../report.pdf",
                "mime_type": "application/pdf",
                "size_bytes": 2**40,
                "route_kind": "slack",
                "route_adapter": None,
                "route_identity": "default",
            }
        ],
    }


@pytest.mark.anyio
async def test_registration_is_metadata_only_idempotent_and_worker_only(access: Any) -> None:
    client = access["client"]
    data = registration(access["aid"])
    denied = await client.post(INTERNAL + "/register", json=data)
    assert denied.status_code == 401
    first = await client.post(INTERNAL + "/register", json=data, headers=worker())
    assert first.status_code == 200, first.text
    assert first.json() == {"appended": 1}
    repeated = await client.post(INTERNAL + "/register", json=data, headers=worker())
    assert repeated.json() == {"appended": 0}


def context(access: Any, **change: Any) -> dict[str, Any]:
    body = {
        "agent_id": access["aid"],
        "deployment_id": access["did"],
        "thread_key": THREAD,
        "event_id": "event-one",
        "owner": "owner-one",
        "session_id": "session-one",
        "sandbox_name": "acme-sandbox",
        "route_generation": 1,
        "ttl_s": 600,
    }
    return body | change


@pytest.mark.anyio
async def test_context_refuses_missing_route_before_mint(access: Any) -> None:
    result = await access["client"].post(
        INTERNAL + "/context", headers=worker(), json=context(access, route_generation=2)
    )
    assert result.status_code == 409
    assert result.json()["detail"]["code"] == "attachment_access.route_changed"


@pytest.mark.anyio
async def test_no_public_authority_from_worker_or_platform_credential(access: Any) -> None:
    for headers in (
        worker(),
        {"X-API-Key": get_settings().api_key},
        {"X-Curie-Attachment-Access": "ata.forged"},
    ):
        result = await access["client"].post("/attachment-access/list", headers=headers, json={})
        assert result.status_code == 401


@pytest.mark.anyio
async def test_streamed_broker_commits_integrity_and_live_scope(
    access: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio
    import hashlib
    import socket

    import uvicorn
    from aiohttp import web
    from curie_worker.attachment_broker import AttachmentBroker
    from curie_worker.attachments import AgentRoute, ChannelPortFileClient

    body_reads: list[str] = []
    stopped = asyncio.Event()
    chunk = b"x" * (1024 * 1024)

    async def provider(req: web.Request) -> web.StreamResponse:
        assert req.headers["X-Curie-Adapter-Secret"] == "synthetic-adapter-key"
        body_reads.append(req.match_info["id"])
        response = web.StreamResponse()
        await response.prepare(req)
        try:
            for _ in range(257 if req.match_info["id"] == "FEXAMPLE1" else 200):
                await response.write(chunk)
                if req.match_info["id"] != "FEXAMPLE1":
                    await asyncio.sleep(0.05)
            await response.write_eof()
        except ConnectionError:
            stopped.set()
        return response

    provider_app = web.Application()
    provider_app.router.add_get("/attachments/{id}", provider)
    provider_runner = web.AppRunner(provider_app)
    await provider_runner.setup()
    provider_site = web.TCPSite(provider_runner, "127.0.0.1", 0)
    await provider_site.start()
    provider_port = provider_site._server.sockets[0].getsockname()[1]
    provider_url = f"http://127.0.0.1:{provider_port}"
    email_thread = scoped_conversation_id(
        "email", "agent@example.test", "thread-one", identity="acme-adapter"
    )
    async with access["sessions"]() as db:
        db.add(
            AgentChannel(
                agent_id=uuid.UUID(access["aid"]),
                kind="email",
                address="agent@example.test",
                adapter="acme-adapter",
                endpoint=provider_url,
            )
        )
        await db.commit()
    await access["valkey"].set(
        f"curie:sandbox:route:{email_thread}",
        json.dumps(
            {
                "state": "live",
                "session_id": "session-one",
                "sandbox_name": "acme-sandbox",
                "generation": 1,
                "thread_key": email_thread,
            }
        ),
    )
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    api_url = f"http://127.0.0.1:{sock.getsockname()[1]}"
    server = uvicorn.Server(uvicorn.Config(access["app"], lifespan="off", log_level="error"))
    server_task = asyncio.create_task(server.serve(sockets=[sock]))
    async with asyncio.timeout(10):
        while not server.started:
            await asyncio.sleep(0.01)

    async def routes(aid: uuid.UUID) -> list[AgentRoute]:
        assert str(aid) == access["aid"]
        return [AgentRoute(kind="email", adapter="acme-adapter", endpoint=provider_url)]

    broker = AttachmentBroker(
        internal_token=get_settings().internal_worker_token,
        routes_for_agent=routes,
        slack_files={},
        channel_files=ChannelPortFileClient(credentials={"acme-adapter": "synthetic-adapter-key"}),
        api_url=api_url,
    )
    broker_runner = web.AppRunner(broker.create_app(), handler_cancellation=True)
    await broker_runner.setup()
    broker_site = web.TCPSite(broker_runner, "127.0.0.1", 0)
    await broker_site.start()
    broker_port = broker_site._server.sockets[0].getsockname()[1]
    monkeypatch.setenv("ATTACHMENT_BROKER_URL", f"http://127.0.0.1:{broker_port}")
    get_settings.cache_clear()
    try:
        async with httpx.AsyncClient(base_url=api_url, timeout=20) as client:
            data = registration(access["aid"])
            data["thread_key"] = email_thread
            data["refs"][0].update(
                route_kind="email", route_adapter="acme-adapter", route_identity="acme-adapter"
            )
            data["refs"].append(dict(data["refs"][0], file_id="FEXAMPLE2", ordinal=1))
            response = await client.post(INTERNAL + "/register", headers=worker(), json=data)
            assert response.status_code == 200, response.text
            mint = context(access, thread_key=email_thread)
            response = await client.post(INTERNAL + "/context", headers=worker(), json=mint)
            assert response.status_code == 200, response.text
            token = response.json()["token"]
            headers = {"X-Curie-Attachment-Access": token}
            again = await client.post(INTERNAL + "/context", headers=worker(), json=mint)
            assert again.json()["token"] == token
            page = await client.post("/attachment-access/list", headers=headers, json={})
            assert page.status_code == 200, page.text
            file = page.json()["files"][0]
            assert file["size_bytes"] == 2**40 and file["sha256"] is None
            assert file["name"] == "report.pdf" and body_reads == []
            # Steer retains opener authority, then reopens that same context.
            owner = {key: mint[key] for key in ("agent_id", "thread_key", "event_id", "owner")}
            newer = dict(
                data, event_id="event-two", refs=[dict(data["refs"][0], file_id="FEXAMPLE3")]
            )
            assert (
                await client.post(INTERNAL + "/register", headers=worker(), json=newer)
            ).status_code == 200
            assert (
                await client.post(
                    INTERNAL + "/renew",
                    headers=worker(),
                    json=owner | {"current_event_id": "event-two"},
                )
            ).json()["active"]
            retained = await client.post(INTERNAL + "/context", headers=worker(), json=mint)
            assert retained.json()["token"] == token
            assert (await client.post(INTERNAL + "/renew", headers=worker(), json=owner)).json()[
                "active"
            ]
            current = await client.post("/attachment-access/list", headers=headers, json={})
            assert [row["current"] for row in current.json()["files"]] == [False, False, True]
            hashed = hashlib.sha256()
            total = 0
            async with client.stream(
                "POST", "/attachment-access/fetch", headers=headers, json={"id": file["id"]}
            ) as response:
                assert response.status_code == 200
                transfer_id = response.headers["X-Curie-Transfer-ID"]
                async for part in response.aiter_bytes(65536):
                    total += len(part)
                    hashed.update(part)
                    await asyncio.sleep(0)
            assert total == 257 * 1024 * 1024
            final = await client.post(
                "/attachment-access/completion", headers=headers, json={"transfer_id": transfer_id}
            )
            assert final.json() == {
                "id": file["id"],
                "sha256": hashed.hexdigest(),
                "size_bytes": total,
                "status": "complete",
            }, final.text
            assert body_reads == ["FEXAMPLE1"]
            second_id = page.json()["files"][1]["id"]
            async with client.stream(
                "POST", "/attachment-access/fetch", headers=headers, json={"id": second_id}
            ) as interrupted:
                canceled_id = interrupted.headers["X-Curie-Transfer-ID"]
                assert await anext(interrupted.aiter_bytes(65536))
            await asyncio.wait_for(stopped.wait(), timeout=2)
            incomplete = await client.post(
                "/attachment-access/completion", headers=headers, json={"transfer_id": canceled_id}
            )
            assert incomplete.status_code == 409
            assert incomplete.json()["detail"]["code"] == "attachment_access.transfer_incomplete"
            stopped.clear()
            owner = {key: mint[key] for key in ("agent_id", "thread_key", "event_id", "owner")}
            assert (await client.post(INTERNAL + "/renew", headers=worker(), json=owner)).json()[
                "active"
            ]
            async with client.stream(
                "POST", "/attachment-access/fetch", headers=headers, json={"id": second_id}
            ) as revoked:
                revoked_chunks = revoked.aiter_bytes(65536)
                assert await anext(revoked_chunks)
                assert (
                    await client.post(INTERNAL + "/revoke", headers=worker(), json=owner)
                ).json()["active"]
                await asyncio.wait_for(stopped.wait(), timeout=2)
                received = 65536
                try:
                    async for remaining in revoked_chunks:
                        received += len(remaining)
                except httpx.HTTPError:
                    pass
                assert received < 200 * 1024 * 1024
            denied = await client.post(
                "/attachment-access/authorize", headers=headers, json={"id": file["id"]}
            )
            assert denied.status_code == 409
            late = await client.post(INTERNAL + "/context", headers=worker(), json=mint)
            assert late.status_code == 409
            assert body_reads == ["FEXAMPLE1", "FEXAMPLE2", "FEXAMPLE2"]
    finally:
        await broker_runner.cleanup()
        server.should_exit = True
        await server_task
        sock.close()
        await provider_runner.cleanup()
        await access["valkey"].delete(f"curie:sandbox:route:{email_thread}")


@pytest.mark.anyio
async def test_integrity_compare_and_set_and_transcript_reset(access: Any) -> None:
    import hashlib

    from curie_api import thread_attachments
    from curie_api.attachment_access import catalog
    from curie_api.models import AttachmentReference
    from fastapi import HTTPException
    from sqlalchemy import select, update

    result = await access["client"].post(
        INTERNAL + "/register", json=registration(access["aid"]), headers=worker()
    )
    assert result.status_code == 200
    async with access["sessions"]() as db:
        row = await db.scalar(select(AttachmentReference))
        initial_version = row.version
        await db.execute(
            update(AttachmentReference)
            .where(AttachmentReference.id == row.id)
            .values(version=initial_version + 1)
        )
        await db.commit()
        with pytest.raises(HTTPException, match="integrity_conflict"):
            await catalog.bind_integrity(
                db, row, version=initial_version, digest=hashlib.sha256(b"one").hexdigest(), size=3
            )
        await db.rollback()
        row = await db.scalar(select(AttachmentReference))
        await catalog.bind_integrity(
            db, row, version=row.version, digest=hashlib.sha256(b"one").hexdigest(), size=3
        )
        await db.commit()
        # A concurrently completed identical fetch is accepted, changed bytes are not.
        await catalog.bind_integrity(
            db, row, version=initial_version, digest=hashlib.sha256(b"one").hexdigest(), size=3
        )
        with pytest.raises(HTTPException, match="integrity_conflict"):
            await catalog.bind_integrity(
                db, row, version=initial_version, digest=hashlib.sha256(b"two").hexdigest(), size=3
            )
        await db.rollback()
        await thread_attachments.delete_for(db, uuid.UUID(access["aid"]), None, [THREAD])
        await db.commit()
        assert await catalog.live_rows(db, uuid.UUID(access["aid"]), THREAD) == []


def test_catalog_names_fit_utf8_and_drop_controls() -> None:
    from curie_api.attachment_access.catalog import safe_leaf

    taken: set[str] = set()
    for name in (
        "../x\n\x7f\x85.txt",
        "é" * 200 + ".pdf",
        "é" * 200 + ".pdf",
        ".hidden",
        "..",
        "a\\b.txt",
    ):
        leaf = safe_leaf(name, taken)
        assert len(leaf.encode()) <= 255
        assert not leaf.startswith(".")
        assert not any(ord(c) < 32 or 127 <= ord(c) <= 159 for c in leaf)
        assert "/" not in leaf and "\\" not in leaf
    assert len(taken) == 6


async def direct_context(access: Any, **changes: Any) -> tuple[dict[str, str], dict[str, Any]]:
    from curie_api.attachment_access.authority import Authority, current_binding
    from curie_api.schemas.attachment_access import Context

    data = Context.model_validate(context(access, **changes))
    authority = Authority(access["valkey"])
    async with access["sessions"]() as db:
        binding = await current_binding(db, data)
        from curie_api.attachment_access.catalog import live_catalog

        incarnation = await live_catalog(db, data.agent_id, data.thread_key)
        state = await authority.open(data, binding, catalog_id=str(incarnation.id))
    return {"X-Curie-Attachment-Access": authority.credential(data)}, state


@pytest.mark.anyio
async def test_scope_pagination_successor_and_live_binding_fences(access: Any) -> None:
    from sqlalchemy import update

    data = registration(access["aid"])
    data["refs"] += [
        dict(data["refs"][0], file_id="FEXAMPLE2", ordinal=1),
        dict(data["refs"][0], file_id="FEXAMPLE3", ordinal=2),
    ]
    client = access["client"]
    assert (
        await client.post(INTERNAL + "/register", headers=worker(), json=data)
    ).status_code == 200
    headers, _ = await direct_context(access)
    first = await client.post("/attachment-access/list", headers=headers, json={"limit": 2})
    assert len(first.json()["files"]) == 2
    cursor = first.json()["next_cursor"]
    assert cursor is not None
    second = await client.post("/attachment-access/list", headers=headers, json={"cursor": cursor})
    assert len(second.json()["files"]) == 1 and second.json()["next_cursor"] is None
    wrong = await client.post(
        "/attachment-access/authorize", headers=headers, json={"id": str(uuid.uuid4())}
    )
    assert wrong.status_code == 404
    successor, _ = await direct_context(access, event_id="event-two", owner="owner-two")
    ended = {key: context(access)[key] for key in ("agent_id", "thread_key", "event_id", "owner")}
    assert not (await client.post(INTERNAL + "/revoke", headers=worker(), json=ended)).json()[
        "active"
    ]
    assert (
        await client.post("/attachment-access/list", headers=headers, json={})
    ).status_code == 409
    assert (
        await client.post("/attachment-access/list", headers=successor, json={})
    ).status_code == 200
    async with access["sessions"]() as db:
        await db.execute(
            update(AgentChannel)
            .where(AgentChannel.agent_id == uuid.UUID(access["aid"]))
            .values(generation=1)
        )
        await db.commit()
    assert (
        await client.post("/attachment-access/list", headers=successor, json={})
    ).status_code == 409


@pytest.mark.anyio
async def test_expired_lease_and_replaced_route_refuse_cached_authorize(access: Any) -> None:
    from curie_api.attachment_access.authority import Authority
    from curie_api.schemas.attachment_access import Owner

    client = access["client"]
    assert (
        await client.post(
            INTERNAL + "/register", headers=worker(), json=registration(access["aid"])
        )
    ).status_code == 200
    headers, _ = await direct_context(access)
    page = (await client.post("/attachment-access/list", headers=headers, json={})).json()
    ref = page["files"][0]["id"]
    await access["valkey"].set(
        f"curie:sandbox:route:{THREAD}",
        json.dumps(
            {
                "state": "live",
                "session_id": "replacement",
                "sandbox_name": "acme-sandbox",
                "generation": 2,
                "thread_key": THREAD,
            }
        ),
    )
    refused = await client.post("/attachment-access/authorize", headers=headers, json={"id": ref})
    assert refused.status_code == 409
    authority = Authority(access["valkey"])
    owner = Owner.model_validate(
        {key: context(access)[key] for key in ("agent_id", "thread_key", "event_id", "owner")}
    )
    await access["valkey"].delete(authority.active_key(owner))
    assert not (
        await client.post(INTERNAL + "/renew", headers=worker(), json=owner.model_dump(mode="json"))
    ).json()["active"]
    assert (
        await client.post("/attachment-access/list", headers=headers, json={})
    ).status_code == 409


@pytest.mark.anyio
async def test_registration_retires_expired_transcript_without_losing_new_refs(access: Any) -> None:
    from datetime import UTC, datetime, timedelta

    from curie_api.attachment_access import catalog
    from curie_api.models import ThreadTranscript

    async with access["sessions"]() as db:
        db.add(
            ThreadTranscript(
                agent_id=uuid.UUID(access["aid"]),
                binding_scope=None,
                thread_key=THREAD,
                value=[],
                expires_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )
        await db.commit()
    response = await access["client"].post(
        INTERNAL + "/register", headers=worker(), json=registration(access["aid"])
    )
    assert response.status_code == 200
    async with access["sessions"]() as db:
        assert len(await catalog.live_rows(db, uuid.UUID(access["aid"]), THREAD)) == 1


@pytest.mark.anyio
async def test_reset_then_register_new_catalog_cannot_revive_old_context(access: Any) -> None:
    from curie_api import thread_attachments

    client = access["client"]
    empty = registration(access["aid"]) | {"refs": []}
    assert (
        await client.post(INTERNAL + "/register", headers=worker(), json=empty)
    ).status_code == 200
    headers, _ = await direct_context(access)
    assert (
        await client.post("/attachment-access/list", headers=headers, json={})
    ).status_code == 200
    async with access["sessions"]() as db:
        await thread_attachments.delete_for(db, uuid.UUID(access["aid"]), None, [THREAD])
        await db.commit()
    assert (
        await client.post(
            INTERNAL + "/register", headers=worker(), json=registration(access["aid"])
        )
    ).status_code == 200
    result = await client.post("/attachment-access/list", headers=headers, json={})
    assert result.status_code == 409
    assert result.json()["detail"]["code"] == "attachment_access.catalog_reset"


@pytest.mark.anyio
async def test_steer_updates_current_message_without_replacing_authority(access: Any) -> None:
    client = access["client"]
    initial = registration(access["aid"])
    await client.post(INTERNAL + "/register", headers=worker(), json=initial)
    headers, _ = await direct_context(access)
    opener = {
        key: value
        for key, value in context(access).items()
        if key in {"agent_id", "thread_key", "event_id", "owner"}
    }
    newer = registration(access["aid"])
    newer["event_id"] = "event-two"
    newer["refs"][0]["file_id"] = "FEXAMPLE2"
    await client.post(INTERNAL + "/register", headers=worker(), json=newer)
    changed = await client.post(
        INTERNAL + "/renew", headers=worker(), json=opener | {"current_event_id": "event-two"}
    )
    assert changed.status_code == 200, changed.text
    assert changed.json() == {"active": True}
    await client.post(INTERNAL + "/renew", headers=worker(), json=opener)
    repeated, _ = await direct_context(access)
    assert repeated == headers
    page = await client.post("/attachment-access/list", headers=headers, json={})
    assert [row["current"] for row in page.json()["files"]] == [False, True]
    for row in page.json()["files"]:
        authorized = await client.post(
            "/attachment-access/authorize", headers=headers, json={"id": row["id"]}
        )
        assert authorized.json()["current"] == row["current"]


@pytest.mark.anyio
async def test_verified_legacy_catalog_import_preserves_identity_and_old_schema(
    access: Any,
) -> None:
    from datetime import UTC, datetime, timedelta

    from curie_api.models import ThreadAttachmentRef
    from sqlalchemy import select, text

    leaf = "a" * 251 + ".pdf"
    async with access["sessions"]() as db:
        old = ThreadAttachmentRef(
            agent_id=uuid.UUID(access["aid"]),
            binding_scope=None,
            thread_key=THREAD,
            event_id="legacy-event",
            file_id="FLEGACY1",
            ordinal=0,
            name=leaf,
            disk_name=leaf,
            mime_type="  ",
            size_bytes=7,
            sha256="a" * 64,
            route_kind="slack",
            route_adapter=None,
            route_identity="default",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
        db.add(old)
        await db.commit()
        old_id = old.id
    client = access["client"]
    empty = registration(access["aid"]) | {"refs": []}
    await client.post(INTERNAL + "/register", headers=worker(), json=empty)
    headers, _ = await direct_context(access)
    first = (await client.post("/attachment-access/list", headers=headers, json={})).json()
    await client.post(INTERNAL + "/register", headers=worker(), json=empty)
    second = (await client.post("/attachment-access/list", headers=headers, json={})).json()
    assert first == second
    assert first["files"][0] | {"id": "opaque"} == {
        "id": "opaque",
        "name": leaf,
        "mime_type": None,
        "size_bytes": 7,
        "sha256": "a" * 64,
        "current": False,
    }
    async with access["sessions"]() as db:
        assert await db.scalar(select(ThreadAttachmentRef.id)) == old_id
        nullable = await db.scalar(
            text(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_schema='curie' AND table_name='thread_attachment_refs' "
                "AND column_name='sha256'"
            )
        )
        assert nullable == "NO"
