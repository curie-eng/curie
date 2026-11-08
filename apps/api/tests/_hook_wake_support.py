"""Shared support for the ADR 0274 declared webhook wake API tests.

The signer below is an independent reference written from the frozen contract
bytes in docs/interfaces/triggers/INTERFACE.md ("Declared webhook wake contract")
and pinned to tests/vectors/hook-wake-signing.json by test_hook_wake_signing.py.
It never imports ``curie_api.hook_wake_signing``, the implementation under test.

Everything else drives the real application: an isolated Postgres database per
test, Valkey, the runs stream and RustFS. Nothing inside the API is replaced;
only environment settings change. Helpers that settle or backdate a run by SQL
stand in for the worker or for elapsed time, both outside the API.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import io
import json
import math
import os
import secrets
import socket
import tarfile
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NoReturn
from urllib.parse import quote, urlsplit, urlunsplit

import asyncpg
import httpx
import pytest
import redis.asyncio as aioredis
from _migration_support import IsolatedMigrationDb
from aci_protocol import STREAM_PAYLOAD_FIELD, QueuedTurn
from curie_api.config import get_settings
from curie_api.db import create_sessionmaker
from curie_api.killswitch import kill_key
from curie_api.main import create_app
from curie_protected_hooks.source_policy_records import target_intent_sha256
from curie_protected_hooks.source_policy_sql import SourceGate
from redis.exceptions import RedisError
from sqlalchemy import URL, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

VECTORS = Path(__file__).resolve().parents[3] / "tests/vectors/hook-wake-signing.json"

WAKE = "queue-ready"
OTHER_WAKE = "another-queue"
CRON = "nightly-check"
PLAIN = "plain-hook"
UNDECLARED = "never-declared"
WAKE_PROMPT = "Read pending work through the connector tools and record each result."
OTHER_PROMPT = "Read the other queue through the connector tools and record each result."
CRON_PROMPT = "Run the scheduled check."
PLAIN_PROMPT = "Summarize the delivered payload."

TIMESTAMP_HEADER = "X-Curie-Timestamp"
DELIVERY_HEADER = "X-Curie-Delivery-Id"
SIGNATURE_HEADER = "X-Curie-Signature-256"

WAKE_PREFIX = b"curie.hook.wake.v1"
TOLERANCE_S = 300
SKEW_MARGIN_S = 30
DEFAULT_LEASE_S = 600.0
ACL_PASSWORD = "example-password"

COALESCED = {"status": "coalesced"}
OVERSIZE = {"detail": "hook body exceeds the maximum size"}
INVALID_SIGNATURE = {"detail": "invalid_wake_signature"}
QUERY_FORBIDDEN = {"detail": "wake_query_forbidden"}
BODY_INVALID = {"detail": "wake_body_invalid"}
NOT_DECLARED = {"detail": "wake_not_declared"}
DECLARATION_UNAVAILABLE = {"detail": "wake_declaration_unavailable"}
SOURCE_NOT_ORDINARY = {"detail": "wake_source_not_ordinary"}
AUTHORITY_UNAVAILABLE = {"detail": "authority_unavailable"}
HOOK_PAUSED = {"detail": "hook_paused"}
AGENT_KILLED = {"detail": "agent_killed"}
BUDGET_EXHAUSTED = {"detail": "budget_exhausted"}
DEDUPE_UNAVAILABLE = {"detail": "wake_dedupe_unavailable"}
ENQUEUE_UNAVAILABLE = {"detail": "wake_enqueue_unavailable"}
GENERATION_EXHAUSTED = {"detail": "wake_generation_exhausted"}
ADMIN_UNAUTHORIZED = {"detail": "missing or invalid API key"}

# Every hook-scoped configuration the contract refuses before any run mutation.
SOURCE_KINDS = ("protected", "tombstone", "pending", "remediation", "partition", "binding")

_ORDINARY = dict(
    mode="ordinary", tool_access=None, runtime_id=None, qualification_id=None, bundle_digest=None
)
_PROTECTED = dict(
    mode="protected",
    tool_access="read-only",
    runtime_id="33333333-3333-4333-8333-333333333333",
    qualification_id="44444444-4444-4444-8444-444444444444",
    bundle_digest="a" * 64,
)
_ENVIRONMENT = {
    "GITHUB_REVIEW_INGRESS_ENABLED": "false",
    "RESUME_RECONCILER_ENABLED": "false",
    "CURIE_WORK_ITEM_RECONCILER_ENABLED": "false",
    "APPROVAL_SWEEP_INTERVAL_S": "0",
    "DEAD_LETTER_WATCH_INTERVAL_S": "0",
    "COMMIT_POLL_INTERVAL_S": "0",
    "OTEL_SDK_DISABLED": "true",
    "OTEL_EXPORTER_OTLP_ENDPOINT": "",
}


# --- independent reference signer -------------------------------------------


def reference_key(api_key: str, agent_id: str, hook: str, generation: int) -> str:
    """The wake key per the contract: unpadded base64url HMAC-SHA256 of the label."""
    derivation = f"curie.hook.wake.key.v1:{agent_id}:{hook}:{generation}".encode()
    digest = hmac.digest(api_key.encode("utf-8"), derivation, "sha256")
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def reference_material(
    timestamp: str,
    delivery_id: str,
    agent_id: str,
    hook: str,
    body: bytes,
    *,
    prefix: bytes = WAKE_PREFIX,
) -> bytes:
    """The exact signed bytes: prefix, newline, framing, compact context and body."""
    context = json.dumps([agent_id, hook], ensure_ascii=True, separators=(",", ":")).encode("ascii")
    framing = f"{timestamp}.{delivery_id}.{len(context)}:".encode()
    return prefix + b"\n" + framing + context + body


def reference_signature(
    key: str,
    timestamp: str,
    delivery_id: str,
    agent_id: str,
    hook: str,
    body: bytes,
    *,
    prefix: bytes = WAKE_PREFIX,
) -> str:
    """The ``sha256=`` header value, keyed by the UTF-8 bytes of the derived key."""
    material = reference_material(timestamp, delivery_id, agent_id, hook, body, prefix=prefix)
    return "sha256=" + hmac.new(key.encode("utf-8"), material, hashlib.sha256).hexdigest()


def reference_scope(api_key: str) -> str:
    """The installation scope: HMAC over the scope label, never a raw key digest."""
    return hmac.new(
        api_key.encode("utf-8"), b"curie.hook.wake.scope.v1", hashlib.sha256
    ).hexdigest()


def reference_dedupe_key(
    api_key: str, agent_id: str, hook: str, generation: int, delivery_id: str
) -> str:
    """The contract's dedupe key namespace for one signed delivery."""
    scope = reference_scope(api_key)
    return f"curie:hook-wake:v1:{scope}:{agent_id}:{hook}:{generation}:{delivery_id}"


def minimum_reservation_s(timestamp: str, now: float) -> int:
    """The smallest TTL the contract allows at receipt time ``now``."""
    return max(1, math.floor(int(timestamp) + TOLERANCE_S - now) + 1)


def reference_ttl(timestamp: str, now: float) -> int:
    """The plan's TTL: the contract minimum plus the fixed clock-skew margin."""
    return minimum_reservation_s(timestamp, now) + SKEW_MARGIN_S


def fresh_delivery_id() -> str:
    return "wake-" + uuid.uuid4().hex


def unix_now() -> str:
    return str(int(time.time()))


def signed_headers(
    key: str,
    agent_id: str,
    hook: str,
    body: bytes = b"{}",
    *,
    timestamp: str | None = None,
    delivery_id: str | None = None,
) -> dict[str, str]:
    """Wake headers signed by the independent reference over exactly ``body``."""
    stamp = unix_now() if timestamp is None else timestamp
    delivery = fresh_delivery_id() if delivery_id is None else delivery_id
    return {
        TIMESTAMP_HEADER: stamp,
        DELIVERY_HEADER: delivery,
        SIGNATURE_HEADER: reference_signature(key, stamp, delivery, agent_id, hook, body),
    }


async def early_in_second() -> float:
    """Wait, without sleeping, until early in a wall-clock second.

    A boundary timestamp such as ``ceil(now) - 300`` stays inside the window only
    while the server's clock reads the same second, so the caller starts with
    most of a second in hand. Spins at most about one second.
    """
    async with asyncio.timeout(2):
        while True:
            now = time.time()
            if 0.01 <= now % 1 <= 0.4:
                return now
            await asyncio.sleep(0)


# --- environment, bundle and application ------------------------------------


def wake_environment(db: IsolatedMigrationDb, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Migrate the test's own database and give it a private runs stream."""
    db.at("head")
    stream = "test:hook-wake:" + uuid.uuid4().hex
    for name, value in {**_ENVIRONMENT, "RUNS_STREAM": stream, "CURIE_STREAM": stream}.items():
        monkeypatch.setenv(name, value)
    for name in ("CURIE_DELIVERY_BUDGET_S", "CURIE_HOOK_CLAIM_LEASE_S"):
        monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    yield stream
    get_settings.cache_clear()


def drive(scenario: Callable[[], Awaitable[None]], timeout: float = 25) -> None:
    asyncio.run(asyncio.wait_for(scenario(), timeout))


def archive(files: dict[str, bytes]) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as tar:
        for path, value in files.items():
            info = tarfile.TarInfo(path)
            info.size = len(value)
            tar.addfile(info, io.BytesIO(value))
    return output.getvalue()


def wake_triggers() -> list[dict[str, Any]]:
    """Two wakes, one cron and one prompt-bearing webhook that does not opt in."""
    return [
        dict(type="webhook", wake=True, name=WAKE, path="/inbound", prompt=WAKE_PROMPT),
        dict(type="webhook", wake=True, name=OTHER_WAKE, path="/inbound", prompt=OTHER_PROMPT),
        dict(type="cron", name=CRON, schedule="0 9 * * *", prompt=CRON_PROMPT),
        dict(type="webhook", name=PLAIN, path="/plain", prompt=PLAIN_PROMPT),
    ]


def wake_bundle(triggers: list[dict[str, Any]] | None = None) -> bytes:
    manifest = dict(
        name="wake-fixture",
        version="0.1.0",
        description="test",
        triggers=wake_triggers() if triggers is None else triggers,
    )
    return archive(
        {
            "wake-fixture/.claude-plugin/plugin.json": json.dumps(manifest).encode(),
            "wake-fixture/skills/wake-fixture/SKILL.md": (
                b"---\nname: wake-fixture\ndescription: test\n---\nRead the work queue.\n"
            ),
        }
    )


@dataclass
class WakeApp:
    """One real application, its client and the deployed agent a test drives."""

    app: Any
    client: httpx.AsyncClient
    headers: dict[str, str]
    agent_id: str
    agent_name: str
    version_id: str
    agents: list[str] = field(default_factory=list)
    versions: list[str] = field(default_factory=list)

    @property
    def api_key(self) -> str:
        return self.headers["X-API-Key"]

    @property
    def valkey(self) -> Any:
        return self.app.state.valkey

    def key(self, hook: str = WAKE, *, agent_id: str | None = None, generation: int = 1) -> str:
        return reference_key(self.api_key, agent_id or self.agent_id, hook, generation)

    async def add_agent(self, *, deploy: bool = True) -> tuple[str, str, str | None]:
        """Another agent on the same app; returns its id, name and version id."""
        return await _create_agent(
            self.client, self.headers, self.agents, self.versions, deploy=deploy
        )


async def _create_agent(
    client: httpx.AsyncClient,
    headers: dict[str, str],
    agents: list[str],
    versions: list[str],
    *,
    deploy: bool = True,
) -> tuple[str, str, str | None]:
    name = "wake-" + uuid.uuid4().hex
    response = await client.post(
        "/agents",
        headers=headers,
        json=dict(
            name=name,
            channel=dict(
                kind="email",
                address=f"{name}@example.test",
                endpoint="http://curie-mail-adapter:8080/",
                adapter="agentmail-sandbox",
            ),
        ),
    )
    assert response.status_code == 201, response.text
    agent_id = str(response.json()["id"])
    agents.append(agent_id)
    if not deploy:
        return agent_id, name, None
    response = await client.post(
        f"/agents/{agent_id}/versions",
        headers=headers,
        json=dict(version_label="v1", created_by="test"),
    )
    assert response.status_code == 201, response.text
    version_id = str(response.json()["id"])
    versions.append(version_id)
    response = await client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        headers=headers,
        files={"file": ("wake-fixture.tar.gz", wake_bundle())},
    )
    assert response.status_code == 201, response.text
    response = await client.post(
        "/deployments",
        headers=headers,
        json=dict(agent_id=agent_id, version_id=version_id, environment="dev"),
    )
    assert response.status_code == 201, response.text
    return agent_id, name, version_id


@asynccontextmanager
async def wake_app() -> AsyncIterator[WakeApp]:
    """A same-loop application with one deployed wake agent and owned cleanup."""
    app = create_app()
    headers = {"X-API-Key": get_settings().api_key}
    agents: list[str] = []
    versions: list[str] = []
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            try:
                agent_id, name, version_id = await _create_agent(client, headers, agents, versions)
                assert version_id is not None
                yield WakeApp(app, client, headers, agent_id, name, version_id, agents, versions)
            finally:
                await _cleanup(app, agents, versions)


async def _cleanup(app: Any, agents: list[str], versions: list[str]) -> None:
    valkey = app.state.valkey
    for agent in agents:
        await valkey.delete(kill_key(uuid.UUID(agent)))
        keys = [key async for key in valkey.scan_iter(match=_dedupe_pattern(agent))]
        if keys:
            await valkey.delete(*keys)
    await valkey.delete(get_settings().runs_stream)
    store = app.state.bundle_store
    for version in versions:
        key = await bundle_ref(app, version)
        if key:
            await asyncio.to_thread(store._client.delete_object, Bucket=store._bucket, Key=key)


def _dedupe_pattern(agent_id: str) -> str:
    return f"curie:hook-wake:v1:*:{agent_id}:*"


def missing_prerequisite(reason: str) -> NoReturn:
    """Fail in CI and skip elsewhere, for a test prerequisite this environment lacks.

    CI (``CI`` set to a true value, as GitHub Actions sets ``CI=true``) provides
    every prerequisite, so a missing one there is a broken environment that must
    not silently drop the proof. Outside CI the test is skipped with ``reason``.
    """
    if os.environ.get("CI", "").strip().lower() in ("1", "true", "yes", "on"):
        pytest.fail(f"missing test prerequisite in CI: {reason}", pytrace=False)
    pytest.skip(reason)


@asynccontextmanager
async def restricted_valkey(*denied: str) -> AsyncIterator[str]:
    """A fresh ACL user on the test Valkey with ``denied`` commands removed.

    Created with the suite's own connection and always deleted afterwards.
    Yields the DSN that authenticates as that user. When the test Valkey
    refuses ACL SETUSER, ``missing_prerequisite`` fails the test in CI and
    skips it with the reason elsewhere.
    """
    dsn = get_settings().valkey_dsn()
    admin = aioredis.from_url(dsn)
    user = "wake-test-" + uuid.uuid4().hex
    created = False
    try:
        try:
            await admin.execute_command(
                "ACL",
                "SETUSER",
                user,
                "reset",
                "on",
                ">" + ACL_PASSWORD,
                "~*",
                "&*",
                "+@all",
                *(f"-{command}" for command in denied),
            )
        except RedisError as exc:
            missing_prerequisite(f"the test Valkey refused ACL SETUSER: {type(exc).__name__}")
        created = True
        yield _dsn_with_user(dsn, user, ACL_PASSWORD)
    finally:
        try:
            if created:
                await admin.execute_command("ACL", "DELUSER", user)
        finally:
            await admin.aclose()


def _dsn_with_user(dsn: str, user: str, password: str) -> str:
    parts = urlsplit(dsn)
    host = parts.hostname or "localhost"
    if ":" in host:
        host = f"[{host}]"
    netloc = f"{quote(user, safe='')}:{quote(password, safe='')}@{host}"
    if parts.port is not None:
        netloc += f":{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def unreachable_valkey_dsn() -> str:
    """A Valkey URL whose port has nothing listening."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"redis://127.0.0.1:{port}/0"


@asynccontextmanager
async def side_client(valkey_url: str) -> AsyncIterator[httpx.AsyncClient]:
    """A second real app on the same database whose only difference is ``VALKEY_URL``.

    The variable is set before the app is created and stays set while the app
    serves, then is restored. The test reads Valkey state through the first
    app's own client, never through settings.

    Startup never connects to Valkey (the client connects on first use), so
    every caller, the unreachable URL included, expects the app to boot and the
    request to meet the failure. No startup failure is an expected outcome:
    any exception from startup propagates and fails the test instead of
    skipping it.
    """
    saved = os.environ.get("VALKEY_URL")
    os.environ["VALKEY_URL"] = valkey_url
    get_settings.cache_clear()
    try:
        async with AsyncExitStack() as stack:
            app = create_app()
            await stack.enter_async_context(app.router.lifespan_context(app))
            yield await stack.enter_async_context(
                httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")
            )
    finally:
        if saved is None:
            os.environ.pop("VALKEY_URL", None)
        else:
            os.environ["VALKEY_URL"] = saved
        get_settings.cache_clear()


@asynccontextmanager
async def unreachable_database(app: Any, *, sessions: bool, gate: bool) -> AsyncIterator[None]:
    """Point request sessions and/or the source gate at a database nothing serves.

    The engine targets 127.0.0.1 port 1, where nothing listens, so the asyncpg
    driver itself refuses while opening a connection; NullPool keeps nothing
    open. ``sessions`` swaps ``app.state.sessionmaker`` (every request session,
    so wake generation metadata), ``gate`` swaps ``app.state.source_gate``. The
    app's own engines are untouched, the originals always come back, and the
    dead engine is disposed.
    """
    dead = create_async_engine(
        URL.create(
            "postgresql+asyncpg",
            username="unreachable",
            password="unused",
            host="127.0.0.1",
            port=1,
            database="unreachable",
        ),
        poolclass=NullPool,
    )
    saved_sessions, saved_gate = app.state.sessionmaker, app.state.source_gate
    try:
        if sessions:
            app.state.sessionmaker = create_sessionmaker(dead)
        if gate:
            app.state.source_gate = SourceGate(dead)
        yield
    finally:
        app.state.sessionmaker = saved_sessions
        app.state.source_gate = saved_gate
        await dead.dispose()


@asynccontextmanager
async def connection_limited_database(app: Any) -> AsyncIterator[None]:
    """Serve request sessions as a fresh role whose every connection Postgres refuses.

    The role is created ``LOGIN ... CONNECTION LIMIT 0`` through the app's own
    engine, so the server answers each connection with "too many connections
    for role" (SQLSTATE 53300), which asyncpg raises from ``connect`` as a
    ``PostgresError``. A probe proves that before the swap. Only
    ``app.state.sessionmaker`` changes, to a NullPool engine on the app's
    database URL with only the credentials replaced; the source gate stays
    live. The original sessionmaker always comes back, the engine is disposed
    and the role is dropped.
    """
    role = "wake_limited_" + secrets.token_hex(8)
    password = secrets.token_hex(16)
    quoted_role = '"' + role.replace('"', '""') + '"'
    quoted_password = "'" + password.replace("'", "''") + "'"
    url = make_url(get_settings().database_url)
    limited = create_async_engine(url.set(username=role, password=password), poolclass=NullPool)
    engine: AsyncEngine = app.state.engine
    saved_sessions = app.state.sessionmaker
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    f"CREATE ROLE {quoted_role} LOGIN PASSWORD {quoted_password} CONNECTION LIMIT 0"
                )
            )
        await _assert_role_connections_refused(url, role, password)
        app.state.sessionmaker = create_sessionmaker(limited)
        yield
    finally:
        app.state.sessionmaker = saved_sessions
        try:
            await limited.dispose()
        finally:
            async with engine.begin() as conn:
                await conn.execute(text(f"DROP ROLE IF EXISTS {quoted_role}"))


async def _assert_role_connections_refused(url: URL, role: str, password: str) -> None:
    """The server itself refuses ``role``: asyncpg raises 53300 while connecting."""
    try:
        probe = await asyncpg.connect(
            user=role, password=password, host=url.host, port=url.port, database=url.database
        )
    except asyncpg.exceptions.TooManyConnectionsError as exc:
        assert exc.sqlstate == "53300", exc.sqlstate
        return
    await probe.close()
    raise AssertionError("a CONNECTION LIMIT 0 role was allowed to connect")


# --- requests ----------------------------------------------------------------


def wake_path(agent_id: str, hook: str) -> str:
    return f"/hooks/{quote(agent_id, safe='')}/{quote(hook, safe='')}/wake"


def secret_path(agent_id: str, hook: str = WAKE) -> str:
    return f"/agents/{quote(agent_id, safe='')}/hooks/{quote(hook, safe='')}/wake-secret"


def rotate_path(agent_id: str, hook: str = WAKE) -> str:
    return secret_path(agent_id, hook) + "/rotate"


async def wake(
    ctx: WakeApp,
    hook: str = WAKE,
    *,
    agent_id: str | None = None,
    key: str | None = None,
    body: bytes = b"{}",
    headers: dict[str, str] | None = None,
    timestamp: str | None = None,
    delivery_id: str | None = None,
    query: str = "",
    client: httpx.AsyncClient | None = None,
    extra_headers: dict[str, str] | None = None,
) -> httpx.Response:
    """POST one wake; signs with the reference signer unless ``headers`` is given."""
    agent = ctx.agent_id if agent_id is None else agent_id
    if headers is None:
        signing_key = ctx.key(hook, agent_id=agent) if key is None else key
        headers = signed_headers(
            signing_key, agent, hook, body, timestamp=timestamp, delivery_id=delivery_id
        )
    return await (client or ctx.client).post(
        wake_path(agent, hook) + query,
        content=body,
        headers={**headers, **(extra_headers or {})},
    )


async def control(
    ctx: WakeApp, name: str, action: str, *, agent: str | None = None
) -> httpx.Response:
    """The existing schedule pause or resume route."""
    return await ctx.client.post(
        f"/schedules/{agent or ctx.agent_id}/{quote(name, safe='')}/{action}",
        headers=ctx.headers,
    )


def assert_queued(response: httpx.Response) -> str:
    assert response.status_code == 202, response.text
    body = response.json()
    assert set(body) == {"status", "run_id"}, body
    assert body["status"] == "queued", body
    assert str(uuid.UUID(body["run_id"])) == body["run_id"], body
    return str(body["run_id"])


def assert_refused(response: httpx.Response, status: int, body: dict[str, Any]) -> None:
    assert response.status_code == status, response.text
    assert response.json() == body, response.text


def assert_no_store(response: httpx.Response) -> None:
    assert "no-store" in response.headers.get("cache-control", "").lower(), response.headers


# --- state snapshots ---------------------------------------------------------


async def hook_runs(app: Any, agent_id: str) -> list[dict[str, Any]]:
    """Every ``hook_runs`` column for one agent, oldest slot first."""
    async with app.state.engine.connect() as conn:
        result = await conn.execute(
            text("SELECT * FROM curie.hook_runs WHERE agent_id=:agent ORDER BY slot_utc, id"),
            dict(agent=uuid.UUID(agent_id)),
        )
        return [dict(row) for row in result.mappings()]


async def stream_entries(app: Any) -> list[Any]:
    return list(await app.state.valkey.xrange(get_settings().runs_stream))


async def snapshot(
    ctx: WakeApp, agent_id: str | None = None
) -> tuple[list[dict[str, Any]], list[Any]]:
    """Independent durable effects: the agent's runs and the whole runs stream."""
    return await hook_runs(ctx.app, agent_id or ctx.agent_id), await stream_entries(ctx.app)


def turns(entries: list[Any]) -> list[QueuedTurn]:
    field_name = STREAM_PAYLOAD_FIELD.encode()
    return [QueuedTurn.model_validate_json(fields[field_name]) for _id, fields in entries]


async def dedupe_keys(ctx: WakeApp, agent_id: str | None = None) -> list[bytes]:
    pattern = _dedupe_pattern(agent_id or ctx.agent_id)
    return sorted([key async for key in ctx.valkey.scan_iter(match=pattern)])


def dedupe_key_for(
    ctx: WakeApp, headers: dict[str, str], hook: str = WAKE, *, generation: int = 1
) -> str:
    return reference_dedupe_key(
        ctx.api_key, ctx.agent_id, hook, generation, headers[DELIVERY_HEADER]
    )


async def bundle_ref(app: Any, version_id: str) -> str | None:
    async with app.state.engine.connect() as conn:
        value = await conn.scalar(
            text("SELECT bundle_ref FROM curie.agent_versions WHERE id=:id"),
            dict(id=uuid.UUID(version_id)),
        )
    return None if value is None else str(value)


async def get_bundle_object(app: Any, version_id: str) -> bytes:
    key = await bundle_ref(app, version_id)
    assert key is not None
    return bytes(await app.state.bundle_store.get(key))


async def put_bundle_object(app: Any, version_id: str, data: bytes | None) -> None:
    """Replace the stored bundle object, or delete it when ``data`` is None."""
    key = await bundle_ref(app, version_id)
    assert key is not None
    store = app.state.bundle_store
    if data is None:
        await asyncio.to_thread(store._client.delete_object, Bucket=store._bucket, Key=key)
    else:
        await asyncio.to_thread(store._client.put_object, Bucket=store._bucket, Key=key, Body=data)


async def stored_generation(app: Any, agent_id: str, hook: str = WAKE) -> int | None:
    async with app.state.engine.connect() as conn:
        value = await conn.scalar(
            text("SELECT generation FROM curie.hook_wake_keys WHERE agent_id=:a AND hook=:h"),
            dict(a=uuid.UUID(agent_id), h=hook),
        )
    return None if value is None else int(value)


async def schedule_control(app: Any, agent_id: str, name: str) -> dict[str, Any] | None:
    async with app.state.engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    text(
                        "SELECT paused_at, resume_from, generation FROM curie.schedule_controls "
                        "WHERE agent_id=:a AND name=:n"
                    ),
                    dict(a=uuid.UUID(agent_id), n=name),
                )
            )
            .mappings()
            .first()
        )
    return None if row is None else dict(row)


# --- seeds (SQL below the API) ------------------------------------------------


async def seed_source(app: Any, agent: str, hook: str, kind: str) -> None:
    """Closed source authority: a protected or ordinary policy, or pending history."""
    target = _PROTECTED if kind == "protected" else _ORDINARY
    params = dict(
        agent=uuid.UUID(agent),
        hook=hook,
        op=uuid.uuid4(),
        intent=target_intent_sha256(target),
        status="pending" if kind == "pending" else "committed",
        **target,
    )
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO curie.hook_source_operations "
                "(agent_id,hook,operation_id,intent_sha256,status,generation) "
                "VALUES(:agent,:hook,:op,:intent,:status,1)"
            ),
            params,
        )
        if kind in ("protected", "tombstone"):
            await conn.execute(
                text(
                    "INSERT INTO curie.hook_source_policies "
                    "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
                    "qualification_id,bundle_digest,legacy_generation) "
                    "VALUES(:agent,:hook,:op,1,:mode,:tool_access,:runtime_id,"
                    ":qualification_id,:bundle_digest,0)"
                ),
                params,
            )


async def seed_remediation(app: Any, agent: str, hook: str) -> None:
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO curie.remediation_policies "
                "(agent_id,hook,generation,operation_id,armed,active) "
                "VALUES(:agent,:hook,1,:op,false,false)"
            ),
            dict(agent=uuid.UUID(agent), hook=hook, op=uuid.uuid4()),
        )


async def _merge_agent_json(app: Any, agent: str, column: str, hook: str, value: Any) -> None:
    assert column in ("hook_partitions", "source_bindings")
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text(
                # A new agent stores JSON null, not SQL NULL; merge into an object only.
                f"UPDATE curie.agents SET {column} = "
                f"(CASE WHEN jsonb_typeof({column}) = 'object' THEN {column} "
                "ELSE '{}'::jsonb END) || CAST(:value AS jsonb) WHERE id=:agent"
            ),
            dict(agent=uuid.UUID(agent), value=json.dumps({hook: value})),
        )


async def seed_partition(app: Any, agent: str, hook: str) -> None:
    await _merge_agent_json(app, agent, "hook_partitions", hook, {"pointer": "/item/id"})


async def seed_binding(app: Any, agent: str, hook: str) -> None:
    await _merge_agent_json(
        app, agent, "source_bindings", hook, {"workload_pointer": "/workload", "map": {}}
    )


async def seed_configuration(app: Any, agent: str, hook: str, kind: str) -> None:
    """Seed one refused source or hook-scoped configuration from SOURCE_KINDS."""
    if kind in ("protected", "tombstone", "pending"):
        await seed_source(app, agent, hook, kind)
    elif kind == "remediation":
        await seed_remediation(app, agent, hook)
    elif kind == "partition":
        await seed_partition(app, agent, hook)
    elif kind == "binding":
        await seed_binding(app, agent, hook)
    else:
        raise AssertionError(kind)


async def seed_generation(app: Any, agent: str, hook: str, generation: int) -> None:
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO curie.hook_wake_keys (agent_id,hook,generation) "
                "VALUES(:a,:h,:g) ON CONFLICT (agent_id,hook) DO UPDATE SET generation=:g"
            ),
            dict(a=uuid.UUID(agent), h=hook, g=generation),
        )


async def settle_run(app: Any, run_id: Any, outcome: str = "ran") -> None:
    """Stand-in for the worker settling a run (outside the API)."""
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE curie.hook_runs SET outcome=:o, ended_at=now() WHERE id=:id"),
            dict(o=outcome, id=uuid.UUID(str(run_id))),
        )


async def expire_lease(app: Any, run_id: Any) -> None:
    """Stand-in for elapsed time: the open claim's lease is now in the past."""
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE curie.hook_runs SET lease_expires_at = now() - interval '1 second' "
                "WHERE id=:id"
            ),
            dict(id=uuid.UUID(str(run_id))),
        )


async def insert_open_run(
    app: Any,
    agent: str,
    version: str,
    name: str,
    *,
    source: str,
    age_s: float = 0.0,
    lease_expires_in_s: float | None = None,
) -> uuid.UUID:
    """An open run written below the API, started ``age_s`` seconds ago."""
    run_id = uuid.uuid4()
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO curie.hook_runs "
                "(id,agent_id,name,slot_utc,version_id,source,started_at,lease_expires_at) "
                "VALUES (:id,:agent,:name,now() - make_interval(secs => :age),:version,"
                ":source,now() - make_interval(secs => :age),"
                "CASE WHEN CAST(:lease AS double precision) IS NULL THEN NULL "
                "ELSE now() + make_interval(secs => CAST(:lease AS double precision)) END)"
            ),
            dict(
                id=run_id,
                agent=uuid.UUID(agent),
                name=name,
                version=uuid.UUID(version),
                source=source,
                age=age_s,
                lease=lease_expires_in_s,
            ),
        )
    return run_id


async def spend_budget(app: Any, agent: str, *, spent: bool = True) -> None:
    """A spent configured limit (0 USD per day), or the default limit back."""
    async with app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE curie.agents SET max_usd_per_day=:usd WHERE id=:agent"),
            dict(usd=0.0 if spent else None, agent=uuid.UUID(agent)),
        )


async def set_killed(ctx: WakeApp, *, killed: bool = True, agent: str | None = None) -> None:
    flag = kill_key(uuid.UUID(agent or ctx.agent_id))
    if killed:
        await ctx.valkey.set(flag, "1")
    else:
        await ctx.valkey.delete(flag)


@asynccontextmanager
async def observer() -> AsyncIterator[AsyncEngine]:
    """A connection outside the app's pools, for lock and activity probes."""
    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    try:
        yield engine
    finally:
        await engine.dispose()


def hook_lock_key(agent_id: str, hook: str = WAKE) -> str:
    """The per agent and hook admission lock text shared with cron and manual fire."""
    return f"{agent_id}:{hook}"


def gate_lock_key(agent_id: str) -> str:
    return "hook-source:" + agent_id


async def lock_waiters(engine: AsyncEngine, key: str) -> int:
    """How many sessions of this database wait, ungranted, on advisory lock ``key``."""
    async with engine.connect() as conn:
        value = await conn.scalar(
            text(
                "SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid=l.database "
                "WHERE d.datname=current_database() AND l.locktype='advisory' "
                "AND NOT l.granted "
                "AND l.classid::bigint=((hashtextextended(:key,0)>>32)&4294967295) "
                "AND l.objid::bigint=(hashtextextended(:key,0)&4294967295) "
                "AND l.objsubid=1"
            ),
            {"key": key},
        )
    return int(value or 0)


async def wait_for_lock_waiters(engine: AsyncEngine, key: str, count: int = 1) -> None:
    """Return once at least ``count`` sessions wait on advisory lock ``key``."""
    async with asyncio.timeout(5):
        while await lock_waiters(engine, key) < count:
            await asyncio.sleep(0)


@asynccontextmanager
async def holding_hook_lock(
    engine: AsyncEngine, agent_id: str, hook: str = WAKE
) -> AsyncIterator[None]:
    """Hold the per agent and hook admission lock from outside the app."""
    async with engine.begin() as conn:
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": hook_lock_key(agent_id, hook)},
        )
        yield


async def finish(tasks: list[asyncio.Task[Any]]) -> None:
    """Cancel and drain any task a failed scenario left running."""
    for task in tasks:
        if not task.done():
            task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
