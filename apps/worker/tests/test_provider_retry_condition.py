"""The worker's provider probe against real Postgres and Valkey (#5426).

Every WorkItem and ExecutionRequest row goes through the real migrated schema,
and every ledger key lives in the real Valkey under a per-test prefix. Only the
OpenRouter HTTP endpoint is faked, with ``httpx.MockTransport`` replaying the
documented GET /api/v1/key shapes:
https://openrouter.ai/docs/api/api-reference/api-keys/get-current-api-key
The amounts in the replayed body are sentinels, and the key is a low-entropy
placeholder.

The end to end tests at the bottom (AC1) also run the real runner app over a
scripted model session, the real worker consumer and finish, the real API and
real reconciler passes, through ``FactoryRetryCase``.
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
from typing import Any, cast

import httpx
import pytest
import redis
import redis.asyncio as aredis
from claude_agent_sdk import AssistantMessage, RateLimitEvent, ResultMessage, TextBlock
from claude_agent_sdk.types import AssistantMessageError, RateLimitInfo
from curie_api.config import get_settings
from curie_api.workitem_reconciler import WorkItemReconciler
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
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from apps.worker.tests.factory_retry_fixtures import (
    Call,
    FactoryRetryCase,
    Item,
    RealRunner,
    factory_retry,
    factory_retry_database,
)

__all__ = ["factory_retry", "factory_retry_database"]

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


# -- AC1 end to end: runner, NDJSON, worker finish, API settlement, reconciler -------
#
# Each test below serves the real runner app (the production SessionRunner over a
# FakeModelSession replaying the scripted SDK messages) for every sandbox the case
# claims, so the condition is built by the real translator and session, crosses the
# real NDJSON wire to the real worker consumer and finish, and is settled by the real
# API into SQL. Every release pass runs a WorkItemReconciler constructed for that pass,
# standing for an API restart, on the worker's key prefix. The probe is the real
# ProviderProbeLoop; only OpenRouter GET /api/v1/key is faked, at the HTTP edge.

# Observed 2026-10-29 (#5163, fixture ``_OPENROUTER_WORKSPACE_BUDGET_403`` in
# runner/tests/test_translate.py and runner/tests/test_provider_retry_condition.py):
# an OpenRouter workspace daily budget refusal is HTTP 403, reported by the SDK as
# error="authentication_failed". ``{reset}`` replaces the observed reset clause.
_WORKSPACE_BUDGET_403 = (
    'API Error: 403 {{"error":{{"message":"Workspace daily budget of $20.00 exceeded. '
    'Contact your org admin. $20.08 of $20.00 used{reset}",'
    '"code":403}}}}'
)
# Observed 2026-10-05 on a staging install (#4104, fixture ``_OPENROUTER_KEY_LIMIT_403``
# in runner/tests/test_translate.py): an OpenRouter key at its own spend limit,
# reported as error="authentication_failed".
_KEY_LIMIT_403 = (
    'API Error: 403 {"error":{"message":"Key limit exceeded (total limit). Manage it '
    'using https://openrouter.ai/workspaces/example/keys/example","code":403}}'
)
# OpenRouter documents HTTP 401 for invalid credentials, with an
# {"error":{"code","message"}} body:
# https://openrouter.ai/docs/api/reference/errors-and-debugging
# The 'API Error: <status>' prefix is the CLI format observed for the 402 and 403
# fixtures (runner/tests/test_translate.py).
_OPENROUTER_401 = 'API Error: 401 {"error":{"message":"User not found.","code":401}}'
# A 403 with no credit, budget or documented 401 text (runner/tests/test_translate.py
# pins it as not credit exhausted).
_GENERIC_403 = 'API Error: 403 {"error":{"message":"invalid api key","code":403}}'
_RESET_GRACE = timedelta(seconds=60)
_CLEAN_TURN_FINISH = {"cause": "model_unreachable", "retry_safety": "no_effects"}


def _budget_403(reset_clause: str) -> str:
    return _WORKSPACE_BUDGET_403.format(reset=reset_clause)


def _rfc3339(moment: datetime, designator: str = "Z") -> str:
    return f"{moment:%Y-%m-%dT%H:%M:%S}.000{designator}"


def _failed_result(text_: str) -> ResultMessage:
    return ResultMessage(
        subtype="error_during_execution",
        duration_ms=1,
        duration_api_ms=1,
        is_error=True,
        num_turns=1,
        session_id="fake-session",
        result=text_,
        usage=None,
    )


def _errored_turn(error: str, provider_text: str) -> Callable[[], list[Any]]:
    """A model turn whose assistant message carries the SDK error and provider text."""

    def script() -> list[Any]:
        return [
            AssistantMessage(
                content=[TextBlock(text=provider_text)],
                model="fake-model",
                error=cast("AssistantMessageError", error),
            ),
            _failed_result(provider_text),
        ]

    return script


def _rate_limited_turn(resets_at: int | None) -> Callable[[], list[Any]]:
    """A model turn the SDK ends with a rejected rate limit event.

    Provenance: the typed SDK field ``claude_agent_sdk.types.RateLimitInfo.resets_at``.
    """

    def script() -> list[Any]:
        return [
            RateLimitEvent(
                rate_limit_info=RateLimitInfo(status="rejected", resets_at=resets_at),
                uuid="fake-rate-limit",
                session_id="fake-session",
            ),
            _failed_result("model rate limit reached"),
        ]

    return script


def _key_body(limit: float, remaining: float) -> httpx.Response:
    """The documented GET /api/v1/key 200 shape with sentinel amounts."""

    return httpx.Response(
        200, json={"data": {**_KEY_BODY["data"], "limit": limit, "limit_remaining": remaining}}
    )


def _turns(runners: list[RealRunner]) -> int:
    return sum(runner.turns for runner in runners)


@dataclass
class EndToEnd:
    """One FactoryRetryCase, the fake OpenRouter edge and the passes the tests run."""

    case: FactoryRetryCase
    client: httpx.AsyncClient
    script: Script

    @property
    def prefix(self) -> str:
        return self.case.names["prefix"]

    @property
    def valkey(self) -> aredis.Redis:
        valkey = self.case.reconciler_valkey
        assert valkey is not None
        return valkey

    async def db_now(self) -> datetime:
        (row,) = await self.case.rows("SELECT clock_timestamp() AS now")
        now: datetime = row["now"]
        return now

    async def reconcile(self, passes: int = 1) -> None:
        """Real reconciler passes, each by a reconciler constructed for it (a restart)."""

        for _ in range(passes):
            reconciler = WorkItemReconciler(
                async_sessionmaker(self.case.engine, expire_on_commit=False),
                self.valkey,
                get_settings().model_copy(update={"worker_key_prefix": self.prefix}),
            )
            await reconciler.run_once()
            await self.case.settle()

    async def probe(self, credential: str, *, offset: timedelta) -> None:
        """One real probe loop tick, its clock ``offset`` after database time.

        Moving the injected clock stands for the probe interval passing between
        ticks; the ledger is stamped from this clock.
        """

        clock = Clock(await self.db_now() + offset)
        loop = ProviderProbeLoop(
            self.case.engine,
            WorkerConfig(db_schema="curie", key_prefix=self.prefix, credentials=credential),
            self.valkey,
            client=self.client,
            clock=clock,
            environ={"CURIE_OPENROUTER_API_URL": API},
        )
        await loop.tick()

    async def fail(self, item: Item) -> tuple[Call, dict[str, Any], dict[str, Any]]:
        """Dispatch ``item``'s request; its finish, its stored row and its successor."""

        case = self.case
        await case.publish_wake(item.request_id)
        finish = await case.finished(item.request_id)
        await case.settle()
        predecessor = await case.row(item.request_id)
        assert predecessor["status"] == "failed", predecessor
        successor = await case.successor(item.request_id)
        assert successor is not None and successor["status"] == "waiting", successor
        return finish, predecessor, successor

    async def assert_asleep(self, request_id: uuid.UUID) -> None:
        """The request is waiting, was never woken and was never acquired."""

        row = await self.case.row(request_id)
        assert (row["status"], row["published_generation"], row["started_at"]) == (
            "waiting",
            None,
            None,
        ), row
        assert self.case.wakes_for(request_id) == []
        assert self.case.calls_to("acquire", request_id) == []

    async def assert_ran_once(self, request_id: uuid.UUID) -> None:
        """The released request started exactly one runner turn and finished it."""

        finish = await self.case.finished(request_id)
        assert {key: finish.body[key] for key in _CLEAN_TURN_FINISH} == _CLEAN_TURN_FINISH
        await self.case.settle()
        assert [call.status for call in self.case.calls_to("start", request_id)] == [200]
        assert (await self.case.row(request_id))["execution_attempts"] == 1


def _end_to_end(case: FactoryRetryCase, body: Callable[[EndToEnd], Any]) -> None:
    async def go() -> None:
        script = Script()
        async with httpx.AsyncClient(transport=httpx.MockTransport(script)) as client:
            await body(EndToEnd(case, client, script))

    case.run(go())


def _assert_bearer_of(request: httpx.Request, credential: str) -> None:
    assert request.method == "GET"
    assert str(request.url) == f"{API}/key"
    assert request.headers["authorization"] == f"Bearer {credential}"


def test_documented_daily_reset_waits_until_the_reset_plus_sixty_seconds(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def body(e2e: EndToEnd) -> None:
        reset = (await e2e.db_now() + timedelta(minutes=5)).replace(microsecond=0)
        refusal = _budget_403(f"; resets {_rfc3339(reset)}.")
        failing = await case.real_runners(
            1,
            script=_errored_turn("authentication_failed", refusal),
            provider_scope=_own_scope(),
        )
        later = await case.real_runners(2)

        finish, predecessor, successor = await e2e.fail(case.item)
        assert finish.body["cause"] == "model_credit_exhausted"
        assert finish.body["retry_safety"] == "no_effects"
        assert finish.body["retry_condition"]["reason"] == "usage_reset"
        # The budget is workspace scoped, so the condition names no key scope.
        assert (
            predecessor["retry_condition_reason"],
            predecessor["retry_condition_retry_at"],
            predecessor["retry_condition_scope_key"],
        ) == ("usage_reset", reset, None)
        assert successor["blocked_on"] is None
        assert successor["dispatch_not_before"] == reset + _RESET_GRACE
        assert await case.used(case.item.work_item_id) == 1

        await e2e.reconcile(passes=2)
        await e2e.assert_asleep(successor["id"])
        assert _turns(failing) == 1 and _turns(later) == 0

        # Stand for the reset plus its grace passing: only the waiting successor's
        # dispatch_not_before is rewritten into the past.
        await case.make_due(successor["id"])
        await e2e.reconcile()
        await e2e.assert_ran_once(successor["id"])
        assert [runner.turns for runner in later] == [1, 0]
        assert _turns(failing) == 1

    _end_to_end(case, body)


def test_ordinary_throttle_backs_off_until_the_typed_reset(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def body(e2e: EndToEnd) -> None:
        scope = _own_scope()
        resets_at = int((await e2e.db_now() + timedelta(minutes=5)).timestamp())
        failing = await case.real_runners(
            1, script=_rate_limited_turn(resets_at), provider_scope=scope
        )
        later = await case.real_runners(2)

        finish, predecessor, successor = await e2e.fail(case.item)
        assert finish.body["cause"] == "model_rate_limited"
        assert finish.body["retry_safety"] == "no_effects"
        retry_at = datetime.fromtimestamp(resets_at, tz=UTC)
        assert (
            predecessor["retry_condition_reason"],
            predecessor["retry_condition_retry_at"],
            predecessor["retry_condition_scope_key"],
        ) == ("rate_limited", retry_at, scope.scope_key)
        # A backoff, not a block, due at the provider's reset plus the grace.
        assert successor["blocked_on"] is None
        assert successor["dispatch_not_before"] == retry_at + _RESET_GRACE

        await e2e.reconcile(passes=2)
        await e2e.assert_asleep(successor["id"])
        assert _turns(failing) == 1 and _turns(later) == 0

        # Stand for the reset passing: only the successor's dispatch_not_before moves.
        await case.make_due(successor["id"])
        await e2e.reconcile()
        await e2e.assert_ran_once(successor["id"])
        assert [runner.turns for runner in later] == [1, 0]

    _end_to_end(case, body)


def test_ordinary_throttle_without_a_reset_time_stays_blocked_until_it_expires(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def body(e2e: EndToEnd) -> None:
        scope = _own_scope()
        # The SDK leaves resets_at unset when the provider names no reset.
        failing = await case.real_runners(1, script=_rate_limited_turn(None), provider_scope=scope)
        later = await case.real_runners(2)

        finish, predecessor, successor = await e2e.fail(case.item)
        assert finish.body["cause"] == "model_rate_limited"
        assert (
            predecessor["retry_condition_reason"],
            predecessor["retry_condition_retry_at"],
        ) == ("rate_limited", None)
        # No guessed timer: blocked until the waiting deadline.
        assert successor["blocked_on"] == "provider_rate_limit"
        assert successor["dispatch_not_before"] == successor["wait_deadline"]

        await e2e.reconcile(passes=2)
        await e2e.assert_asleep(successor["id"])
        assert _turns(failing) == 1 and _turns(later) == 0

    _end_to_end(case, body)


def test_invalid_credential_blocks_until_a_replaced_credential_authenticates(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def body(e2e: EndToEnd) -> None:
        failed_scope = _own_scope()
        failing = await case.real_runners(
            1,
            script=_errored_turn("authentication_failed", _OPENROUTER_401),
            provider_scope=failed_scope,
        )
        later = await case.real_runners(2)
        item = case.item

        finish, predecessor, successor = await e2e.fail(item)
        # The worker reports the runner's model-credential-rejected classification.
        assert finish.body["cause"] == "model_credential_rejected"
        assert finish.body["retry_safety"] == "no_effects"
        assert (
            predecessor["retry_condition_reason"],
            predecessor["retry_condition_retry_at"],
            predecessor["retry_condition_scope_key"],
        ) == ("credential_rejected", None, failed_scope.scope_key)
        assert successor["blocked_on"] == "provider_credential"
        assert await case.used(item.work_item_id) == 1

        await e2e.reconcile(passes=3)
        # The worker still runs with the rejected credential: nothing is read.
        await e2e.probe(KEY, offset=timedelta(seconds=1))
        assert e2e.script.requests == []
        await e2e.reconcile(passes=2)
        await e2e.assert_asleep(successor["id"])
        assert _turns(failing) == 1 and _turns(later) == 0

        # The credential is replaced: the probe reads the new key once and it
        # authenticates. The successor is released by a fresh reconciler.
        e2e.script.steps.append(_ok())
        await e2e.probe(OTHER_KEY, offset=timedelta(seconds=31))
        assert len(e2e.script.requests) == 1
        _assert_bearer_of(e2e.script.requests[0], OTHER_KEY)
        await e2e.reconcile()
        await e2e.assert_ran_once(successor["id"])
        assert [runner.turns for runner in later] == [1, 0]
        assert _turns(failing) == 1
        # The release spent no allowance: the released turn's own transient failure
        # took the second and last automatic slot, which is only possible when the
        # count still stood at one after the release.
        assert await case.used(item.work_item_id) == 2
        third = await case.successor(successor["id"])
        assert third is not None and third["status"] == "waiting", third

    _end_to_end(case, body)


def test_depleted_credit_releases_only_on_a_raised_limit(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def body(e2e: EndToEnd) -> None:
        scope = _own_scope()
        failing = await case.real_runners(
            1,
            script=_errored_turn("authentication_failed", _KEY_LIMIT_403),
            provider_scope=scope,
        )
        later = await case.real_runners(2)

        finish, predecessor, successor = await e2e.fail(case.item)
        assert finish.body["cause"] == "model_credit_exhausted"
        assert (
            predecessor["retry_condition_reason"],
            predecessor["retry_condition_retry_at"],
            predecessor["retry_condition_scope_key"],
        ) == ("credit_exhausted", None, scope.scope_key)
        assert successor["blocked_on"] == "provider_credit"

        # The same limit with a rising remainder (a usage fall or a period
        # rollover) is not added allowance. Each tick's clock is 30 s past the
        # last, the probe interval.
        e2e.script.steps.extend([_key_body(25, 0.5), _key_body(25, 12.5)])
        await e2e.probe(KEY, offset=timedelta(seconds=1))
        await e2e.reconcile(passes=2)
        await e2e.probe(KEY, offset=timedelta(seconds=32))
        await e2e.reconcile(passes=2)
        assert len(e2e.script.requests) == 2
        await e2e.assert_asleep(successor["id"])
        assert _turns(failing) == 1 and _turns(later) == 0

        # A strictly greater limit with something left under it releases.
        e2e.script.steps.append(_key_body(50, 25))
        await e2e.probe(KEY, offset=timedelta(seconds=63))
        assert len(e2e.script.requests) == 3
        for request in e2e.script.requests:
            _assert_bearer_of(request, KEY)
        await e2e.reconcile()
        await e2e.assert_ran_once(successor["id"])
        assert [runner.turns for runner in later] == [1, 0]

    _end_to_end(case, body)


def test_generic_403_and_unscoped_rejections_stay_blocked_while_a_valid_control_releases(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def body(e2e: EndToEnd) -> None:
        scope = _own_scope()
        generic = await case.real_runners(
            1, script=_errored_turn("authentication_failed", _GENERIC_403), provider_scope=scope
        )
        # A runner that could not name its scope (scope None).
        unscoped = await case.real_runners(
            1, script=_errored_turn("authentication_failed", _OPENROUTER_401)
        )
        control = await case.real_runners(
            1, script=_errored_turn("authentication_failed", _OPENROUTER_401), provider_scope=scope
        )
        later = await case.real_runners(2)

        # Each WorkItem is admitted only once the one before it settled, so the
        # pooled runners are claimed in this order.
        finish, predecessor, generic_successor = await e2e.fail(case.item)
        # Classification unchanged and no condition on the wire.
        assert finish.body["cause"] == "model_credential_rejected"
        assert finish.body["retry_condition"] is None
        assert predecessor["retry_condition_reason"] is None
        assert predecessor["retry_condition_scope_key"] is None
        assert generic_successor["blocked_on"] == "provider_credential"

        finish, predecessor, unscoped_successor = await e2e.fail(await case.admit())
        assert finish.body["cause"] == "model_credential_rejected"
        assert (
            predecessor["retry_condition_reason"],
            predecessor["retry_condition_scope_key"],
        ) == ("credential_rejected", None)
        assert unscoped_successor["blocked_on"] == "provider_credential"

        finish, predecessor, control_successor = await e2e.fail(await case.admit())
        assert (
            predecessor["retry_condition_reason"],
            predecessor["retry_condition_scope_key"],
        ) == ("credential_rejected", scope.scope_key)
        assert control_successor["blocked_on"] == "provider_credential"

        # The worker registers the failed scope, then runs with a replaced
        # credential that authenticates.
        await e2e.probe(KEY, offset=timedelta(seconds=1))
        e2e.script.steps.append(_ok())
        await e2e.probe(OTHER_KEY, offset=timedelta(seconds=31))
        assert len(e2e.script.requests) == 1
        _assert_bearer_of(e2e.script.requests[0], OTHER_KEY)
        await e2e.reconcile(passes=3)

        await e2e.assert_ran_once(control_successor["id"])
        for blocked in (generic_successor, unscoped_successor):
            await e2e.assert_asleep(blocked["id"])
            assert (await case.row(blocked["id"]))["blocked_on"] == "provider_credential"
        assert [_turns(generic), _turns(unscoped), _turns(control)] == [1, 1, 1]
        assert [runner.turns for runner in later] == [1, 0]

    _end_to_end(case, body)


def test_daily_budget_without_a_utc_reset_stays_blocked_while_a_valid_control_releases(
    factory_retry: FactoryRetryCase,
) -> None:
    case = factory_retry

    async def body(e2e: EndToEnd) -> None:
        scope = _own_scope()
        reset = (await e2e.db_now() + timedelta(minutes=5)).replace(microsecond=0)
        offset = await case.real_runners(
            1,
            script=_errored_turn(
                "authentication_failed", _budget_403(f"; resets {_rfc3339(reset, '+00:00')}.")
            ),
            provider_scope=scope,
        )
        missing = await case.real_runners(
            1,
            script=_errored_turn("authentication_failed", _budget_403(".")),
            provider_scope=scope,
        )
        control = await case.real_runners(
            1, script=_errored_turn("authentication_failed", _KEY_LIMIT_403), provider_scope=scope
        )
        later = await case.real_runners(2)

        # Each WorkItem is admitted only once the one before it settled, so the
        # pooled runners are claimed in this order.
        waiting: list[dict[str, Any]] = []
        for first in (True, False):
            finish, predecessor, successor = await e2e.fail(
                case.item if first else await case.admit()
            )
            assert finish.body["cause"] == "model_credit_exhausted"
            assert (
                predecessor["retry_condition_reason"],
                predecessor["retry_condition_retry_at"],
                predecessor["retry_condition_scope_key"],
            ) == ("usage_reset", None, None)
            assert successor["blocked_on"] == "provider_credit"
            waiting.append(successor)
        _finish, predecessor, control_successor = await e2e.fail(await case.admit())
        assert (
            predecessor["retry_condition_reason"],
            predecessor["retry_condition_scope_key"],
        ) == ("credit_exhausted", scope.scope_key)
        assert control_successor["blocked_on"] == "provider_credit"

        # A baseline read, then a strictly greater limit with something left.
        e2e.script.steps.extend([_key_body(25, 0.5), _key_body(50, 25)])
        await e2e.probe(KEY, offset=timedelta(seconds=1))
        await e2e.reconcile(passes=2)
        for successor in (*waiting, control_successor):
            await e2e.assert_asleep(successor["id"])
        await e2e.probe(KEY, offset=timedelta(seconds=32))
        assert len(e2e.script.requests) == 2
        await e2e.reconcile(passes=3)

        await e2e.assert_ran_once(control_successor["id"])
        for successor in waiting:
            await e2e.assert_asleep(successor["id"])
            assert (await case.row(successor["id"]))["blocked_on"] == "provider_credit"
        assert [_turns(offset), _turns(missing), _turns(control)] == [1, 1, 1]
        assert [runner.turns for runner in later] == [1, 0]

    _end_to_end(case, body)
