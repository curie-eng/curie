"""Signed hook support probe, @spec PROTECTED-HOOK-SOURCE-9 (with SOURCE-2/4)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
import pytest
from _migration_support import IsolatedMigrationDb, sql_dicts
from curie_api import hook_signing, hook_source_signing
from curie_api.config import get_settings
from curie_api.main import create_app
from curie_protected_hooks.source_policy_sql import SourceGate
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

HOOK = "daily-summary"
AUTH_DETAIL = "missing or invalid signature"
GENERATION = 9
DTO_KEYS = {
    "requested_tool_access",
    "effective_tool_access",
    "source_generation",
    "runtime_id",
    "runtime_generation",
    "qualification_id",
    "supported",
    "reason",
}
BODIES: dict[str | None, bytes] = {
    None: b'{"tool_access":null}',
    "read-only": b'{"tool_access":"read-only"}',
}


@pytest.fixture
def support_db(
    isolated_migration_db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch
) -> Iterator[None]:
    """Real migrated Postgres and an owned runs stream, @spec PROTECTED-HOOK-SOURCE-9."""
    isolated_migration_db.at("head")
    for name, value in {
        "GITHUB_REVIEW_INGRESS_ENABLED": "false",
        "RESUME_RECONCILER_ENABLED": "false",
        "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
        "APPROVAL_SWEEP_INTERVAL_S": "0",
        "DEAD_LETTER_WATCH_INTERVAL_S": "0",
        "COMMIT_POLL_INTERVAL_S": "0",
        "OTEL_SDK_DISABLED": "true",
        "OTEL_EXPORTER_OTLP_ENDPOINT": "",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("RUNS_STREAM", "test:curie:source-support:" + uuid.uuid4().hex)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@asynccontextmanager
async def probe_app() -> AsyncIterator[Any]:
    """App lifespan (real Valkey), client and one fresh agent, @spec PROTECTED-HOOK-SOURCE-9."""
    owned_stream = get_settings().runs_stream
    app = create_app()
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/agents",
                headers={"X-API-Key": get_settings().api_key},
                json={
                    "name": "acme-support-" + uuid.uuid4().hex,
                    "channel": {
                        "kind": "email",
                        "address": "support@example.test",
                        "endpoint": "http://adapter.example.test",
                        "adapter": "mail",
                    },
                },
            )
            assert response.status_code == 201, response.text
            agent = response.json()["id"]
            try:
                yield app, client, agent
            finally:
                keys = [key async for key in app.state.valkey.scan_iter(match=f"*{agent}*")]
                if keys:
                    await app.state.valkey.delete(*keys)
                await app.state.valkey.delete(owned_stream)


def legacy_secret(agent: str, generation: int = 0) -> str:
    """Current shared agent key, @spec PROTECTED-HOOK-SOURCE-4."""
    return hook_signing.derive(get_settings().api_key, agent_id=agent, generation=generation)


def scoped_secret(agent: str, generation: int = GENERATION) -> str:
    """Current scoped source key, @spec PROTECTED-HOOK-SOURCE-4."""
    return hook_source_signing.derive(
        get_settings().api_key, agent_id=agent, hook=HOOK, generation=generation
    )


def support_headers(
    key: str,
    *,
    requested: str | None,
    body: bytes,
    delivery: str | None = "support-delivery",
) -> dict[str, str]:
    """Support-purpose signature; ``delivery=None`` signs "" and omits the header.

    @spec PROTECTED-HOOK-SOURCE-9.
    """
    stamp = str(int(time.time()))
    headers = {
        hook_signing.TIMESTAMP_HEADER: stamp,
        hook_signing.SIGNATURE_HEADER: hook_source_signing.sign_support(
            key,
            timestamp=stamp,
            delivery_id=delivery or "",
            hook=HOOK,
            tool_access=requested,
            body=body,
        ),
    }
    if delivery is not None:
        headers[hook_signing.DELIVERY_HEADER] = delivery
    return headers


def delivery_headers(
    key: str, *, requested: str | None, body: bytes, delivery: str = "support-delivery"
) -> dict[str, str]:
    """Ordinary delivery-v2 signature, @spec PROTECTED-HOOK-SOURCE-2/9."""
    stamp = str(int(time.time()))
    return {
        hook_signing.TIMESTAMP_HEADER: stamp,
        hook_signing.DELIVERY_HEADER: delivery,
        hook_signing.SIGNATURE_HEADER: hook_signing.sign(
            key,
            timestamp=stamp,
            delivery_id=delivery,
            hook=HOOK,
            tool_access=requested,
            body=body,
        ),
    }


def _intent(target: dict[str, Any]) -> str:
    """@spec PROTECTED-HOOK-SOURCE-2/10."""
    return hashlib.sha256(
        json.dumps(target, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def seed(agent: str, kind: str) -> dict[str, Any]:
    """Seed a source state exactly as the ingress suite does, @spec PROTECTED-HOOK-SOURCE-2/10.

    ``pending``/``historical`` leave attempt history without a policy row;
    ``ordinary`` is a tombstone row; ``protected`` carries runtime references.
    """
    protected = kind == "protected"
    target = dict(
        mode="protected" if protected else "ordinary",
        tool_access="read-only" if protected else None,
        runtime_id=str(uuid.uuid4()) if protected else None,
        qualification_id=str(uuid.uuid4()) if protected else None,
        bundle_digest="a" * 64 if protected else None,
    )
    operation = str(uuid.uuid4())
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
        "VALUES (:agent,:hook,:operation,:intent,:status,:generation)",
        dict(
            agent=agent,
            hook=HOOK,
            operation=operation,
            intent=_intent(target),
            status="pending" if kind == "pending" else "committed",
            generation=GENERATION,
        ),
    )
    if kind in ("protected", "ordinary"):
        sql_dicts(
            "INSERT INTO curie.hook_source_policies "
            "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
            "qualification_id,bundle_digest,legacy_generation) "
            "VALUES (:agent,:hook,:operation,:generation,:mode,:tool_access,:runtime_id,"
            ":qualification_id,:bundle_digest,0)",
            dict(agent=agent, hook=HOOK, operation=operation, generation=GENERATION, **target),
        )
    return target


def rotate_protected(agent: str, target: dict[str, Any], generation: int) -> None:
    """Consistent protected rotation to ``generation``, @spec PROTECTED-HOOK-SOURCE-2/4."""
    operation = str(uuid.uuid4())
    sql_dicts(
        "INSERT INTO curie.hook_source_operations "
        "(agent_id,hook,operation_id,intent_sha256,status,generation) "
        "VALUES (:agent,:hook,:operation,:intent,'committed',:generation)",
        dict(
            agent=agent,
            hook=HOOK,
            operation=operation,
            intent=_intent(target),
            generation=generation,
        ),
    )
    sql_dicts(
        "UPDATE curie.hook_source_policies SET generation=:generation, operation_id=:operation "
        "WHERE agent_id=:agent AND hook=:hook",
        dict(agent=agent, hook=HOOK, operation=operation, generation=generation),
    )


def _sql_state(agent: str) -> Any:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    params = {"agent": agent}
    return tuple(
        sql_dicts(statement, params)
        for statement in (
            "SELECT * FROM curie.hook_source_policies WHERE agent_id=:agent ORDER BY hook",
            "SELECT * FROM curie.hook_source_operations WHERE agent_id=:agent "
            "ORDER BY operation_id",
            "SELECT hook_generation FROM curie.agents WHERE id=:agent",
            "SELECT * FROM curie.hook_runs WHERE agent_id=:agent ORDER BY id",
            "SELECT * FROM curie.thread_workspaces WHERE agent_id=:agent",
        )
    )


async def valkey_effects(app: Any, agent: str) -> Any:
    """Claims, quota keys and stream entries, @spec PROTECTED-HOOK-SOURCE-9."""
    keys = sorted([key async for key in app.state.valkey.scan_iter(match=f"*{agent}*")])
    return keys, await app.state.valkey.xlen(get_settings().runs_stream)


async def effects(app: Any, agent: str) -> Any:
    """Every observable write the probe must not make, @spec PROTECTED-HOOK-SOURCE-9."""
    return await valkey_effects(app, agent), await asyncio.to_thread(_sql_state, agent)


async def wait_for_advisory(observer: Any) -> None:
    """Measured PG lock wait, @spec PROTECTED-HOOK-SOURCE-2."""
    async with asyncio.timeout(5):
        while True:
            async with observer.connect() as conn:
                waiting = await conn.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                        "WHERE datname=current_database() AND wait_event='advisory')"
                    )
                )
            if waiting:
                return
            await asyncio.sleep(0.01)


async def wait_for_gate_waiter(observer: Any, request: asyncio.Task[httpx.Response]) -> None:
    """Fail clearly if the probe returns instead of waiting, @spec PROTECTED-HOOK-SOURCE-2/9."""
    waiter = asyncio.create_task(wait_for_advisory(observer))
    done, _ = await asyncio.wait({request, waiter}, return_when=asyncio.FIRST_COMPLETED)
    if request in done:
        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        response = request.result()
        pytest.fail(f"probe did not wait on the gate: {response.status_code} {response.text}")
    await waiter


def expected(
    requested: str | None, effective: str | None, generation: str | None, reason: str
) -> dict[str, Any]:
    """The exact safe 503 DTO, @spec PROTECTED-HOOK-SOURCE-9."""
    return {
        "requested_tool_access": requested,
        "effective_tool_access": effective,
        "source_generation": generation,
        "runtime_id": None,
        "runtime_generation": None,
        "qualification_id": None,
        "supported": False,
        "reason": reason,
    }


def assert_uniform_401(response: httpx.Response) -> None:
    """Existing uniform refusal, never the DTO, @spec PROTECTED-HOOK-SOURCE-9."""
    assert response.status_code == 401, response.text
    assert response.json() == {"detail": AUTH_DETAIL}


async def post_support(
    client: httpx.AsyncClient, agent: str, body: bytes, headers: dict[str, str]
) -> httpx.Response:
    """@spec PROTECTED-HOOK-SOURCE-9."""
    return await client.post(
        f"/hooks/{agent}/{HOOK}/support",
        content=body,
        headers={"Content-Type": "application/json", **headers},
    )


# -- positive resolution table ------------------------------------------------


@pytest.mark.parametrize(
    "body,requested",
    [(b"{}", None), (BODIES[None], None), (BODIES["read-only"], "read-only")],
)
def test_never_configured_hook_reports_requested_policy_unconfigured(
    support_db: None, body: bytes, requested: str | None
) -> None:
    """No row, no history: legacy key, effective == requested, 503.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            before = await effects(app, agent)
            key = legacy_secret(agent)
            response = await post_support(
                client, agent, body, support_headers(key, requested=requested, body=body)
            )
            assert response.status_code == 503, response.text
            assert response.json() == expected(requested, requested, None, "source_unconfigured")
            assert key not in response.text
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("requested", [None, "read-only"])
def test_ordinary_tombstone_reports_row_generation_closed(
    support_db: None, requested: str | None
) -> None:
    """Ordinary tombstone row: legacy key, effective == requested, row generation, closed.

    A tombstone admits nothing at delivery ingress until broker confirmation of
    its ordinary publication is available, so it reports ``source_closed``.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            await asyncio.to_thread(seed, agent, "ordinary")
            before = await effects(app, agent)
            body = BODIES[requested]
            key = legacy_secret(agent)
            response = await post_support(
                client, agent, body, support_headers(key, requested=requested, body=body)
            )
            assert response.status_code == 503, response.text
            assert response.json() == expected(
                requested, requested, str(GENERATION), "source_closed"
            )
            assert key not in response.text
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("requested", [None, "read-only"])
@pytest.mark.parametrize("kind", ["pending", "historical"])
def test_attempt_history_without_row_reports_closed_read_only(
    support_db: None, kind: str, requested: str | None
) -> None:
    """History without a committed row: legacy key, effective read-only, closed.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            await asyncio.to_thread(seed, agent, kind)
            before = await effects(app, agent)
            body = BODIES[requested]
            key = legacy_secret(agent)
            response = await post_support(
                client, agent, body, support_headers(key, requested=requested, body=body)
            )
            assert response.status_code == 503, response.text
            assert response.json() == expected(requested, "read-only", None, "source_closed")
            assert key not in response.text
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("requested", [None, "read-only"])
def test_protected_row_reports_broker_unavailable_without_echoing_references(
    support_db: None, requested: str | None
) -> None:
    """Protected row: scoped key, read-only, row generation, broker_unavailable.

    The row's runtime, qualification and bundle references are writable
    configuration and never appear; neither does any secret.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-4.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9/4."""
        async with probe_app() as (app, client, agent):
            target = await asyncio.to_thread(seed, agent, "protected")
            before = await effects(app, agent)
            body = BODIES[requested]
            key = scoped_secret(agent)
            response = await post_support(
                client, agent, body, support_headers(key, requested=requested, body=body)
            )
            assert response.status_code == 503, response.text
            assert response.json() == expected(
                requested, "read-only", str(GENERATION), "broker_unavailable"
            )
            for leaked in (
                target["runtime_id"],
                target["qualification_id"],
                target["bundle_digest"],
                key,
                legacy_secret(agent),
                get_settings().api_key,
            ):
                assert leaked not in response.text
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("kind", ["never", "ordinary", "pending", "historical", "protected"])
def test_probe_writes_nothing_and_reserves_no_delivery_id(support_db: None, kind: str) -> None:
    """Repeated probes change no Valkey/SQL state and claim no delivery id.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            if kind != "never":
                await asyncio.to_thread(seed, agent, kind)
            key = scoped_secret(agent) if kind == "protected" else legacy_secret(agent)
            body = BODIES["read-only"]
            before = await effects(app, agent)
            responses = [
                await post_support(
                    client,
                    agent,
                    body,
                    support_headers(key, requested="read-only", body=body, delivery="shared-id"),
                )
                for _ in range(2)
            ]
            assert [r.status_code for r in responses] == [503, 503], responses[-1].text
            assert responses[0].json() == responses[1].json()
            assert set(responses[0].json()) == DTO_KEYS
            assert await effects(app, agent) == before
            if kind == "never":
                # The probe's delivery id was never claimed: a real delivery with
                # the same id is a first, non-duplicate enqueue.
                delivered = await client.post(
                    f"/hooks/{agent}/{HOOK}",
                    content=body,
                    headers=delivery_headers(
                        legacy_secret(agent), requested=None, body=body, delivery="shared-id"
                    ),
                )
                assert delivered.status_code == 200, delivered.text
                assert delivered.json()["duplicate"] is False
                assert await app.state.valkey.xlen(get_settings().runs_stream) == 1

    asyncio.run(asyncio.wait_for(scenario(), 20))


# -- authentication ------------------------------------------------------------


@pytest.mark.parametrize("protected", [False, True])
def test_delivery_signature_cannot_authenticate_support_probe(
    support_db: None, protected: bool
) -> None:
    """A captured delivery-v2 signature is refused on /support.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-4.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            if protected:
                await asyncio.to_thread(seed, agent, "protected")
            key = scoped_secret(agent) if protected else legacy_secret(agent)
            before = await effects(app, agent)
            body = BODIES["read-only"]
            response = await post_support(
                client, agent, body, delivery_headers(key, requested="read-only", body=body)
            )
            assert_uniform_401(response)
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("requested", [None, "read-only"])
def test_support_signature_cannot_enqueue_at_delivery_ingress(
    support_db: None, requested: str | None
) -> None:
    """A support-purpose signature is refused by POST /hooks/{agent}/{hook}.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            # Precondition: the route itself is live; only purpose separation refuses.
            body = BODIES[requested]
            probe = await post_support(
                client,
                agent,
                body,
                support_headers(legacy_secret(agent), requested=requested, body=body),
            )
            assert probe.status_code == 503, probe.text
            before = await effects(app, agent)
            response = await client.post(
                f"/hooks/{agent}/{HOOK}",
                content=body,
                params={"tool_access": requested} if requested else {},
                headers=support_headers(
                    legacy_secret(agent), requested=requested, body=body, delivery="other-id"
                ),
            )
            assert_uniform_401(response)
            assert await effects(app, agent) == before
            assert await app.state.valkey.xlen(get_settings().runs_stream) == 0

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("case", ["legacy_on_protected", "scoped_on_unconfigured", "unknown"])
def test_wrong_key_or_unknown_agent_is_uniform_401(support_db: None, case: str) -> None:
    """Only the current key for the current source state authenticates.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-4.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9/4."""
        async with probe_app() as (app, client, agent):
            target_agent = agent
            if case == "legacy_on_protected":
                await asyncio.to_thread(seed, agent, "protected")
                key = legacy_secret(agent)
            elif case == "scoped_on_unconfigured":
                key = scoped_secret(agent, generation=1)
            else:
                target_agent = str(uuid.uuid4())
                key = legacy_secret(target_agent)
            before = await effects(app, agent)
            body = BODIES[None]
            response = await post_support(
                client, target_agent, body, support_headers(key, requested=None, body=body)
            )
            assert_uniform_401(response)
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize(
    "signed,sent", [(None, "read-only"), ("read-only", None)], ids=["add", "remove"]
)
@pytest.mark.parametrize("protected", [False, True])
def test_changed_requested_policy_fails_authentication(
    support_db: None, protected: bool, signed: str | None, sent: str | None
) -> None:
    """The parsed body policy is signed context; changing it is a 401.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-4.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9/4."""
        async with probe_app() as (app, client, agent):
            if protected:
                await asyncio.to_thread(seed, agent, "protected")
            key = scoped_secret(agent) if protected else legacy_secret(agent)
            before = await effects(app, agent)
            # Sign over the body that carries `signed`, but send the one carrying `sent`.
            headers = support_headers(key, requested=signed, body=BODIES[signed])
            response = await post_support(client, agent, BODIES[sent], headers)
            assert_uniform_401(response)
            # Same policy, different raw bytes is also refused: no normalization.
            headers = support_headers(key, requested=sent, body=BODIES[sent])
            response = await post_support(client, agent, BODIES[sent] + b" ", headers)
            assert_uniform_401(response)
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


# -- request order ---------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        b'{"tool_access":null,"extra":1}',
        b'{"extra":1}',
        b"[]",
        b'"read-only"',
        b"null",
        b'{"tool_access":"write"}',
        b'{"tool_access":"READ-ONLY"}',
        b'{"tool_access":1}',
        b"not json",
        b"",
    ],
)
@pytest.mark.parametrize("unknown", [False, True])
def test_strict_input_is_422_before_any_read_or_effect(
    support_db: None, body: bytes, unknown: bool
) -> None:
    """Malformed HookSupportIn is the ordinary 422, before any database read.

    Even an unknown agent gets 422, since the parse precedes authentication.
    @spec PROTECTED-HOOK-SOURCE-9.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            target_agent = str(uuid.uuid4()) if unknown else agent
            before = await effects(app, agent)
            response = await post_support(
                client,
                target_agent,
                body,
                support_headers(legacy_secret(target_agent), requested=None, body=body),
            )
            assert response.status_code == 422, response.text
            assert not DTO_KEYS & set(response.json())
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


def test_invalid_hook_name_then_oversized_body_precede_parse(
    support_db: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Hook name (400) then the bounded raw body (413), as the delivery route.

    @spec PROTECTED-HOOK-SOURCE-9.
    """
    monkeypatch.setenv("HOOK_MAX_BODY_BYTES", "64")
    get_settings.cache_clear()

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            before = await effects(app, agent)
            bad_name = await client.post(
                f"/hooks/{agent}/Not_Valid/support",
                content=b"not json",
                headers={"Content-Type": "application/json"},
            )
            assert bad_name.status_code == 400, bad_name.text
            oversized = b'{"tool_access":null,"pad":"' + b"x" * 64 + b'"}'
            response = await post_support(
                client,
                agent,
                oversized,
                support_headers(legacy_secret(agent), requested=None, body=oversized),
            )
            assert response.status_code == 413, response.text
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("protected", [False, True])
def test_missing_delivery_id_is_400_only_after_valid_signature(
    support_db: None, protected: bool
) -> None:
    """A valid signature over "" without the header is 400; a bad one stays 401.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9."""
        async with probe_app() as (app, client, agent):
            if protected:
                await asyncio.to_thread(seed, agent, "protected")
            key = scoped_secret(agent) if protected else legacy_secret(agent)
            before = await effects(app, agent)
            body = BODIES[None]
            response = await post_support(
                client, agent, body, support_headers(key, requested=None, body=body, delivery=None)
            )
            assert response.status_code == 400, response.text
            assert hook_signing.DELIVERY_HEADER in response.json()["detail"]
            unsigned = await post_support(
                client,
                agent,
                body,
                support_headers("bad-secret", requested=None, body=body, delivery=None),
            )
            assert_uniform_401(unsigned)
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("unknown", [False, True])
def test_bad_or_unknown_authentication_finishes_without_gate(
    support_db: None, unknown: bool
) -> None:
    """Preauthentication refuses before acquiring the agent advisory lock.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9/2."""
        async with probe_app() as (app, client, agent):
            target = str(uuid.uuid4()) if unknown else agent
            body = BODIES[None]
            async with app.state.source_gate.hold(uuid.UUID(target)):
                async with asyncio.timeout(2):
                    response = await post_support(
                        client,
                        target,
                        body,
                        support_headers("bad-secret", requested=None, body=body),
                    )
                assert_uniform_401(response)
                assert app.state.source_gate.engine.pool.checkedout() == 1

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("protected", [False, True])
def test_waiting_probe_reauthenticates_after_rotation(support_db: None, protected: bool) -> None:
    """A key rotated while the probe waits on the gate fails reauthentication.

    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2 @spec PROTECTED-HOOK-SOURCE-4.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9/2/4."""
        async with probe_app() as (app, client, agent):
            target = await asyncio.to_thread(seed, agent, "protected") if protected else None
            body = BODIES[None]
            old = scoped_secret(agent) if protected else legacy_secret(agent)
            headers = support_headers(old, requested=None, body=body)
            observer = create_async_engine(get_settings().database_url, poolclass=NullPool)
            task = None
            try:
                async with app.state.source_gate.hold(uuid.UUID(agent)):
                    task = asyncio.create_task(post_support(client, agent, body, headers))
                    await wait_for_gate_waiter(observer, task)
                    assert app.state.engine.pool.checkedout() == 0
                    if target is not None:
                        await asyncio.to_thread(rotate_protected, agent, target, GENERATION + 1)
                    else:
                        async with observer.begin() as conn:
                            await conn.execute(
                                text("UPDATE curie.agents SET hook_generation=1 WHERE id=:agent"),
                                {"agent": uuid.UUID(agent)},
                            )
                    rotated = await effects(app, agent)
                response = await asyncio.wait_for(task, 5)
                assert_uniform_401(response)
                assert await effects(app, agent) == rotated
                fresh_key = (
                    scoped_secret(agent, GENERATION + 1) if protected else legacy_secret(agent, 1)
                )
                fresh = await post_support(
                    client,
                    agent,
                    body,
                    support_headers(fresh_key, requested=None, body=body, delivery="fresh-id"),
                )
                assert fresh.status_code == 503, fresh.text
                assert fresh.json() == (
                    expected(None, "read-only", str(GENERATION + 1), "broker_unavailable")
                    if protected
                    else expected(None, None, None, "source_unconfigured")
                )
                assert await effects(app, agent) == rotated
            finally:
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                await observer.dispose()

    asyncio.run(asyncio.wait_for(scenario(), 20))


@pytest.mark.parametrize("failure", ["gate_missing", "gate_database_unusable"])
def test_gate_or_database_failure_is_bare_authority_unavailable(
    support_db: None, failure: str
) -> None:
    """Preauthentication passes, then no gate-held resolution can be read: bare 503.

    The body is exactly ``{"detail": "authority_unavailable"}`` with none of the
    HookSupportOut keys, and the probe makes no Valkey or SQL effect.
    @spec PROTECTED-HOOK-SOURCE-9 @spec PROTECTED-HOOK-SOURCE-2.
    """

    async def scenario() -> None:
        """@spec PROTECTED-HOOK-SOURCE-9/2."""
        async with probe_app() as (app, client, agent):
            body = BODIES["read-only"]
            key = legacy_secret(agent)
            # Precondition: the same signed probe resolves while the gate is live.
            live = await post_support(
                client, agent, body, support_headers(key, requested="read-only", body=body)
            )
            assert live.status_code == 503, live.text
            assert live.json() == expected("read-only", "read-only", None, "source_unconfigured")
            before = await effects(app, agent)
            original = app.state.source_gate
            unusable = None
            try:
                if failure == "gate_missing":
                    app.state.source_gate = None
                else:
                    # A real gate whose database does not exist: every connect fails.
                    missing = make_url(get_settings().database_url).set(
                        database="curie_missing_" + uuid.uuid4().hex
                    )
                    unusable = create_async_engine(missing, poolclass=NullPool)
                    app.state.source_gate = SourceGate(unusable)
                response = await post_support(
                    client,
                    agent,
                    body,
                    support_headers(key, requested="read-only", body=body, delivery="lost-gate"),
                )
            finally:
                app.state.source_gate = original
                if unusable is not None:
                    await unusable.dispose()
            assert response.status_code == 503, response.text
            assert response.json() == {"detail": "authority_unavailable"}
            assert not DTO_KEYS & set(response.json())
            assert key not in response.text
            assert await effects(app, agent) == before

    asyncio.run(asyncio.wait_for(scenario(), 20))
