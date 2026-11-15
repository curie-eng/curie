"""The worker's provider probe against real Postgres and Valkey (#5426).

Every WorkItem and ExecutionRequest row goes through the real migrated schema,
and every ledger key lives in the real Valkey under a per-test prefix. Only the
OpenRouter HTTP endpoint is faked, with ``httpx.MockTransport`` replaying the
documented GET /api/v1/key shapes:
https://openrouter.ai/docs/api/api-reference/api-keys/get-current-api-key
The amounts in the replayed body are sentinels, and the key is a low-entropy
placeholder.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sys
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import httpx
import pytest
import redis
import redis.asyncio as aredis
from curie_internal.provider_gate_ledger import (
    claim_probe,
    lease_key,
    read_current,
    read_scope,
    record_probe,
    register_current,
)
from curie_internal.provider_scope import (
    AllowanceReading,
    ProviderScope,
    provider_scope,
)
from curie_test_support.postgres import pg_connect_or_skip
from curie_test_support.valkey import VALKEY_HOST, VALKEY_PORT, VALKEY_PW
from curie_worker.config import WorkerConfig
from curie_worker.provider_probe import ProviderProbeLoop, read_openrouter_allowance
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

# Low-entropy placeholders only.
# Every test runs under its own credentials (see ``_per_test_credentials``), so the
# scope keys its gates carry are its own on the shared database: the probe loop
# selects blocked gates across all tests, and a gate on another test's scope
# must never read for this one.
KEY = ""
OTHER_KEY = ""
ANT_KEY = ""
API = "https://openrouter.fixture.test/api/v1"
START = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)
_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")

# The documented 200 body of GET /api/v1/key, sanitized. Amounts are sentinels.
_KEY_BODY: dict[str, Any] = {
    "data": {
        "label": "sk-or-v1-000...000",
        "limit": 25,
        "limit_remaining": 12.5,
        "limit_reset": "monthly",
        "usage": 12.5,
        "is_free_tier": False,
    }
}
# The documented 401 body for a key the provider does not know.
_REJECTED_BODY: dict[str, Any] = {"error": {"message": "User not found.", "code": 401}}


def _digits(node_id: str, label: str) -> str:
    """Sixteen decimal digits derived deterministically from the test's node id."""

    return f"{int(hashlib.sha256(f'{node_id}:{label}'.encode()).hexdigest()[:13], 16):016d}"


@pytest.fixture(autouse=True)
def _per_test_credentials(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> None:
    node_id = request.node.nodeid
    module = sys.modules[__name__]
    monkeypatch.setattr(module, "KEY", "sk-or-v1-" + "0" * 48 + _digits(node_id, "own"))
    monkeypatch.setattr(module, "OTHER_KEY", "sk-or-v1-" + "0" * 48 + _digits(node_id, "other"))
    monkeypatch.setattr(module, "ANT_KEY", "sk-ant-api03-" + "0" * 48 + _digits(node_id, "ant"))


def _own_scope(credential: str | None = None) -> ProviderScope:
    scope = provider_scope({"CURIE_CREDENTIALS": credential or KEY})
    assert scope is not None
    return scope


def _ok() -> httpx.Response:
    return httpx.Response(200, json=_KEY_BODY)


class Script:
    """The fake OpenRouter: answers each request with the next scripted step."""

    def __init__(self, steps: list[object] | None = None) -> None:
        self.steps = list(steps or [])
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if not self.steps:
            raise AssertionError(f"unexpected request {request.method} {request.url.path}")
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        assert isinstance(step, httpx.Response)
        return step


@dataclass
class Clock:
    now: datetime = START

    def __call__(self) -> datetime:
        return self.now

    @property
    def ms(self) -> int:
        return int(self.now.timestamp() * 1000)


@dataclass
class Harness:
    engine: AsyncEngine
    redis: aredis.Redis
    prefix: str
    client: httpx.AsyncClient
    script: Script
    clock: Clock
    agent_id: uuid.UUID
    work_items: list[uuid.UUID] = field(default_factory=list)

    def loop(
        self,
        *,
        owner: str | None = None,
        credentials: str | None = None,
        **config: Any,
    ) -> ProviderProbeLoop:
        return ProviderProbeLoop(
            self.engine,
            WorkerConfig(
                db_schema=_SCHEMA,
                key_prefix=self.prefix,
                credentials=KEY if credentials is None else credentials,
                **config,
            ),
            self.redis,
            client=self.client,
            clock=self.clock,
            owner=owner,
            environ={"CURIE_OPENROUTER_API_URL": API},
        )

    async def gate(
        self,
        reason: str,
        scope_key: str | None,
        *,
        blocked_on: str = "provider_credit",
        retry_at: datetime | None = None,
        successor: str = "waiting",
        wait_deadline: timedelta = timedelta(hours=1),
    ) -> None:
        """One WorkItem with a failed predecessor and its retry successor."""

        item_id = uuid.uuid4()
        number = str(uuid.uuid4().int % 10**12 + 1)
        now = datetime.now(UTC)
        async with self.engine.begin() as connection:
            await connection.execute(
                text(
                    f"INSERT INTO {_SCHEMA}.work_items "
                    "(id, tracker_kind, tracker_host, tracker_scope_id, tracker_issue_id, "
                    "code_host_kind, code_host_host, repository_project_id, "
                    "code_host_installation_id, agent_id, repository_path, conversation_id, "
                    "version, next_sequence) VALUES "
                    "(:id, 'github', 'github.com', :number, :number, 'github', 'github.com', "
                    "'101', 202, :agent, 'acme-corp/acme-bot', :conversation, 1, 3)"
                ),
                {
                    "id": item_id,
                    "number": number,
                    "agent": self.agent_id,
                    "conversation": f"probe-{item_id.hex[:12]}",
                },
            )
            self.work_items.append(item_id)
            predecessor = uuid.uuid4()
            await _insert_request(
                connection,
                id=predecessor,
                work_item_id=item_id,
                sequence=1,
                status="failed",
                wait_deadline=now + timedelta(hours=1),
                started_at=now - timedelta(minutes=10),
                execution_deadline=now + timedelta(minutes=50),
                execution_attempts=1,
                terminal_at=now - timedelta(minutes=1),
                terminal_cause="turn_failed",
                retry_safety="no_effects",
                retry_condition_reason=reason,
                retry_condition_retry_at=retry_at,
                retry_condition_scope_key=scope_key,
            )
            columns: dict[str, Any] = {
                "id": uuid.uuid4(),
                "work_item_id": item_id,
                "sequence": 2,
                "status": successor,
                "wait_deadline": now + wait_deadline,
                "retry_of_request_id": predecessor,
                "retry_origin_request_id": predecessor,
                "blocked_on": blocked_on,
            }
            if successor == "running":
                # A started request carries no block.
                columns.update(
                    blocked_on=None,
                    started_at=now,
                    execution_deadline=now + timedelta(hours=1),
                    execution_attempts=1,
                )
            elif successor == "expired":
                columns.update(terminal_at=now, terminal_cause="retry_condition_expired")
            await _insert_request(connection, **columns)

    async def observation(self, scope: ProviderScope | None = None) -> Any:
        scope = scope or _own_scope()
        return await read_scope(self.redis, self.prefix, scope.provider, scope.scope_key)

    @property
    def gets(self) -> list[httpx.Request]:
        return self.script.requests


async def _insert_request(connection: Any, **values: Any) -> None:
    names = ", ".join(values)
    binds = ", ".join(f":{name}" for name in values)
    await connection.execute(
        text(f"INSERT INTO {_SCHEMA}.execution_requests ({names}) VALUES ({binds})"), values
    )


@asynccontextmanager
async def harness(prefix: str, steps: list[object] | None = None) -> AsyncIterator[Harness]:
    engine = create_async_engine(_DB_URL, poolclass=NullPool)
    client_redis = aredis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PW or None)
    script = Script(steps)
    agent_id = uuid.uuid4()
    built: Harness | None = None
    try:
        await pg_connect_or_skip(engine)
        async with engine.begin() as connection:
            await connection.execute(
                text(f"INSERT INTO {_SCHEMA}.agents (id, name) VALUES (:id, :name)"),
                {"id": agent_id, "name": f"probe-{agent_id.hex[:8]}"},
            )
        # follow_redirects on the client proves the read's own refusal wins.
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(script), follow_redirects=True
        ) as client:
            built = Harness(engine, client_redis, prefix, client, script, Clock(), agent_id)
            yield built
    finally:
        async with engine.begin() as connection:
            for item_id in built.work_items if built else []:
                await connection.execute(
                    text(f"DELETE FROM {_SCHEMA}.work_items WHERE id = :id"), {"id": item_id}
                )
            await connection.execute(
                text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"), {"id": agent_id}
            )
        await client_redis.aclose()
        await engine.dispose()


def _run(names: dict[str, str], body: Callable[[Harness], Any], steps: list[object]) -> None:
    async def go() -> None:
        async with harness(names["prefix"], steps) as h:
            await body(h)

    asyncio.run(go())


def _text(value: object) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _assert_bearer(request: httpx.Request) -> None:
    assert request.method == "GET"
    assert str(request.url) == f"{API}/key"
    assert request.headers["authorization"] == f"Bearer {KEY}"


def test_no_provider_gate_registers_the_scope_and_reads_nothing(
    names: dict[str, str], sync_redis: redis.Redis
) -> None:
    async def body(h: Harness) -> None:
        await h.loop().tick()
        scope = _own_scope()
        assert h.gets == []
        assert await read_current(h.redis, h.prefix, "openrouter") == scope.scope_key
        observation = await h.observation()
        assert observation is not None
        assert observation.current_at_ms == h.clock.ms
        assert observation.probed_at_ms is None

    _run(names, body, [])


def test_a_credit_gate_on_the_own_scope_reads_once_and_records(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        await h.gate("credit_exhausted", _own_scope().scope_key)
        await h.loop().tick()
        assert len(h.gets) == 1
        _assert_bearer(h.gets[0])
        observation = await h.observation()
        assert observation.outcome == "authenticated"
        assert observation.limit == Decimal("25")
        assert observation.limit_remaining == Decimal("12.5")
        assert observation.probed_at_ms == h.clock.ms

    _run(names, body, [_ok()])


def test_a_usage_gate_without_a_time_on_the_own_scope_reads_once(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        await h.gate("usage_reset", _own_scope().scope_key, blocked_on="provider_usage_reset")
        await h.loop().tick()
        assert len(h.gets) == 1

    _run(names, body, [_ok()])


def test_two_same_scope_gates_and_two_concurrent_loops_read_once(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        own = _own_scope().scope_key
        await h.gate("credit_exhausted", own)
        await h.gate("usage_reset", own, blocked_on="provider_credit")
        await h.loop(owner="probe-one").tick()
        assert len(h.gets) == 1
        # A new probe round: both loops tick at once, one reads.
        h.clock.now += timedelta(seconds=31)
        await asyncio.gather(h.loop(owner="probe-one").tick(), h.loop(owner="probe-two").tick())
        assert len(h.gets) == 2

    _run(names, body, [_ok(), _ok(), _ok()])


def test_a_peer_lease_blocks_the_read_until_it_lapses(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        scope = _own_scope()
        await h.gate("credit_exhausted", scope.scope_key)
        peer = await claim_probe(h.redis, h.prefix, scope.provider, scope.scope_key, "peer")
        assert peer is not None
        await h.loop().tick()
        assert h.gets == []

        lease = lease_key(h.prefix, scope.provider, scope.scope_key)
        assert await h.redis.pexpire(lease, 1)
        for _ in range(100_000):
            if not await h.redis.exists(lease):
                break
        else:
            raise AssertionError("the lapsed lease never left Valkey")
        await h.loop().tick()
        assert len(h.gets) == 1
        recorded = await h.observation()
        assert recorded.outcome == "authenticated"

        # The lapsed peer's late reading is refused and changes nothing.
        late = AllowanceReading(outcome="rejected", limit_remaining=None, limit=None)
        assert not await record_probe(
            h.redis, h.prefix, scope.provider, scope.scope_key, peer, h.clock.ms + 5, late
        )
        assert await h.observation() == recorded

    _run(names, body, [_ok()])


def test_a_recent_probe_is_not_repeated_within_thirty_seconds(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        await h.gate("credit_exhausted", _own_scope().scope_key)
        loop = h.loop()
        await loop.tick()
        assert len(h.gets) == 1
        h.clock.now += timedelta(seconds=10)
        await loop.tick()
        h.clock.now += timedelta(seconds=19)
        await loop.tick()
        assert len(h.gets) == 1
        h.clock.now = START + timedelta(seconds=31)
        await loop.tick()
        assert len(h.gets) == 2
        assert (await h.observation()).probed_at_ms == h.clock.ms

    _run(names, body, [_ok(), _ok()])


def test_a_credential_gate_on_another_scope_of_the_provider_probes_the_own_scope(
    names: dict[str, str],
) -> None:
    async def body(h: Harness) -> None:
        failed = _own_scope(OTHER_KEY)
        await register_current(h.redis, h.prefix, failed, h.clock.ms)
        await h.gate("credential_rejected", failed.scope_key, blocked_on="provider_credential")
        await h.loop().tick()
        assert len(h.gets) == 1
        _assert_bearer(h.gets[0])
        assert (await h.observation()).outcome == "authenticated"
        assert (await h.observation(failed)).probed_at_ms is None

    _run(names, body, [_ok()])


def test_a_credential_gate_on_an_unregistered_scope_reads_nothing(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        await h.gate(
            "credential_rejected", _own_scope(OTHER_KEY).scope_key, blocked_on="provider_credential"
        )
        await h.loop().tick()
        assert h.gets == []

    _run(names, body, [])


def test_a_credential_gate_on_the_own_scope_reads_nothing(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        await h.gate(
            "credential_rejected", _own_scope().scope_key, blocked_on="provider_credential"
        )
        await h.loop().tick()
        assert h.gets == []

    _run(names, body, [])


def test_gates_outside_the_probe_selection_read_nothing(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        own = _own_scope().scope_key
        await h.gate(
            "usage_reset",
            own,
            blocked_on="provider_usage_reset",
            retry_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        await h.gate("credit_exhausted", own, blocked_on="provider_rate_limit")
        await h.gate("credit_exhausted", own, successor="running")
        await h.gate("credit_exhausted", own, successor="expired")
        await h.gate("credit_exhausted", own, wait_deadline=-timedelta(minutes=1))
        await h.gate("rate_limited", own, blocked_on="provider_credit")
        await h.gate("credit_exhausted", None)
        await h.loop().tick()
        assert h.gets == []

    _run(names, body, [])


def test_an_anthropic_scope_registers_and_never_reads(names: dict[str, str]) -> None:
    async def body(h: Harness) -> None:
        scope = _own_scope(ANT_KEY)
        assert scope.provider == "anthropic"
        await h.gate("credit_exhausted", scope.scope_key)
        await h.loop(credentials=ANT_KEY).tick()
        assert h.gets == []
        assert await read_current(h.redis, h.prefix, "anthropic") == scope.scope_key
        assert (await h.observation(scope)).probed_at_ms is None

    _run(names, body, [])


@pytest.mark.parametrize(
    ("credentials", "config"),
    [
        pytest.param(None, {"factory_model_auth_mode": "personal"}, id="personal-mode"),
        pytest.param("", {}, id="no-credential"),
    ],
)
def test_no_scope_writes_nothing_to_valkey(
    names: dict[str, str],
    sync_redis: redis.Redis,
    credentials: str | None,
    config: dict[str, Any],
) -> None:
    async def body(h: Harness) -> None:
        await h.gate("credit_exhausted", _own_scope().scope_key)
        await h.loop(credentials=credentials, **config).tick()
        assert h.gets == []

    _run(names, body, [])
    assert list(sync_redis.scan_iter(match=f"{names['prefix']}*")) == []


def test_rejected_unreadable_and_redirected_reads_are_recorded_as_such(
    names: dict[str, str],
) -> None:
    redirect = httpx.Response(302, headers={"Location": "https://elsewhere.fixture.test/key"})
    steps: list[object] = [
        httpx.Response(401, json=_REJECTED_BODY),
        httpx.ConnectError("refused"),
        redirect,
        httpx.Response(200, text="not json"),
    ]

    async def body(h: Harness) -> None:
        await h.gate("credit_exhausted", _own_scope().scope_key)
        loop = h.loop()
        outcomes = []
        for _ in range(4):
            await loop.tick()
            outcomes.append((await h.observation()).outcome)
            h.clock.now += timedelta(seconds=31)
        assert outcomes == ["rejected", "unreadable", "unreadable", "unreadable"]
        # The redirect was not followed: every request went to the key read.
        assert len(h.gets) == 4
        assert all(str(request.url) == f"{API}/key" for request in h.gets)

    _run(names, body, steps)


def test_read_openrouter_allowance_parses_the_documented_body() -> None:
    script = Script([_ok()])

    async def go() -> AllowanceReading:
        async with httpx.AsyncClient(transport=httpx.MockTransport(script)) as client:
            return await read_openrouter_allowance(KEY, api_url=API, client=client)

    reading = asyncio.run(go())
    assert reading == AllowanceReading(
        outcome="authenticated", limit_remaining=Decimal("12.5"), limit=Decimal("25")
    )
    assert KEY not in repr(reading)


def test_the_key_never_reaches_logs_or_valkey(
    names: dict[str, str], sync_redis: redis.Redis, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)

    async def body(h: Harness) -> None:
        await h.gate("credit_exhausted", _own_scope().scope_key)
        loop = h.loop()
        await loop.tick()
        h.clock.now += timedelta(seconds=31)
        await loop.tick()
        assert len(h.gets) == 2

    _run(names, body, [_ok(), httpx.Response(401, json=_REJECTED_BODY)])
    assert "provider probe read provider=openrouter outcome=authenticated" in caplog.text
    assert "provider probe read provider=openrouter outcome=rejected" in caplog.text
    keys = list(sync_redis.scan_iter(match=f"{names['prefix']}*"))
    assert keys
    values: list[str] = []
    for key in keys:
        kind = _text(sync_redis.type(key))
        if kind == "string":
            values.append(_text(sync_redis.get(key)))
        elif kind == "hash":
            values.extend(
                f"{_text(name)}={_text(value)}" for name, value in sync_redis.hgetall(key).items()
            )
        else:
            raise AssertionError(f"unexpected ledger key type {kind}")
    blob = caplog.text + "\n".join(keys) + "\n".join(values)
    assert KEY not in blob
    assert KEY.removeprefix("sk-or-v1-") not in blob
    assert "12.5" not in caplog.text
