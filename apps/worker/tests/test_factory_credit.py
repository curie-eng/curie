"""Model credit observation (issue 5581).

The fixture shapes are the ones recorded in cli/src/openrouter_credit.rs and
cli/tests/openrouter_credit.rs: GET /key 200 with data.limit and
data.limit_remaining (null limit means unlimited), GET /key 401
{"error":{"message":"User not found.","code":401}}, and GET /credits 200 with
data.total_credits and data.total_usage. Amounts below are sentinels, not the
recorded sample values. The placeholder key is the one that file already uses.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from curie_test_support.postgres import pg_connect_or_skip
from curie_worker.config import WorkerConfig
from curie_worker.factory_credit import (
    LOCK_CLASS_ID,
    LOCK_OBJECT_ID,
    CreditObservation,
    FactoryCreditLoop,
    observe,
    subject_digest,
)
from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool


def sample_value() -> str:
    # Two fragments, so no single quoted assignment matches a generic key rule.
    return "sk-or-" + "v1-PLACEHOLDER-test-key"


# Sentinel amounts. The assertions search logs and rows for these spellings.
ABOVE = "424242.42"
BELOW = "3.17"
HIGH_TOTAL = "900000.11"
HIGH_USED = "100.01"
LOW_TOTAL = "100000.01"
LOW_USED = "99999.02"
SENTINELS = (ABOVE, BELOW, HIGH_TOTAL, HIGH_USED, LOW_TOTAL, LOW_USED)
API = "https://credit.fixture.test/api/v1"
_DB_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:25432/postgres"
)
_SCHEMA = os.environ.get("TEST_DB_SCHEMA", "curie")
_LOCK_SQL = "SELECT pg_try_advisory_lock(CAST(:classid AS integer), CAST(:objid AS integer))"
_UNLOCK_SQL = "SELECT pg_advisory_unlock(CAST(:classid AS integer), CAST(:objid AS integer))"


def _key_body(limit: str, remaining: str) -> dict[str, object]:
    raw = (
        '{"data":{"label":"sk-or-v1-PLACEHOLDER","is_management_key":false,'
        '"is_provisioning_key":false,"limit":'
        + limit
        + ',"limit_reset":null,"limit_remaining":'
        + remaining
        + ',"include_byok_in_limit":false,"usage":'
        + ABOVE
        + ',"usage_daily":'
        + ABOVE
        + ',"usage_weekly":'
        + ABOVE
        + ',"usage_monthly":'
        + ABOVE
        + ',"is_free_tier":false,"expires_at":null}}'
    )
    body = json.loads(raw)
    assert isinstance(body, dict)
    return body


def _credits_body(total: str, used: str) -> dict[str, object]:
    body = json.loads('{"data":{"total_credits":' + total + ',"total_usage":' + used + "}}")
    assert isinstance(body, dict)
    return body


def _ok(status: int, body: dict[str, object] | None = None) -> httpx.Response:
    if body is None:
        return httpx.Response(status)
    return httpx.Response(status, json=body)


class Script:
    def __init__(self, steps: list[object]) -> None:
        self.steps = list(steps)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if not self.steps:
            raise AssertionError(f"unexpected request {request.method} {request.url.path}")
        step = self.steps.pop(0)
        if isinstance(step, BaseException):
            raise step
        if not isinstance(step, httpx.Response):
            raise AssertionError("script step is not a response")
        return step


def _paths(script: Script) -> list[str]:
    return [request.url.path for request in script.requests]


def _assert_clean(*blobs: str) -> None:
    for blob in blobs:
        assert sample_value() not in blob
        for sentinel in SENTINELS:
            assert sentinel not in blob


async def _read(
    script: Script,
    *,
    minimum: float = 10,
    base_url: str = "https://openrouter.ai/api",
    credentials: str | None = None,
    model_env_key: str = "",
) -> tuple[CreditObservation, str, Script]:
    supplied = sample_value() if credentials is None else credentials
    async with httpx.AsyncClient(transport=httpx.MockTransport(script)) as client:
        observation, digest = await observe(
            WorkerConfig(
                credentials=supplied,
                model_base_url=base_url,
                model_env_key=model_env_key,
                factory_min_model_credit_usd=minimum,
            ),
            client=client,
            environ={"CURIE_OPENROUTER_API_URL": API},
        )
    return observation, digest, script


def _high_pair() -> list[object]:
    return [
        _ok(200, _key_body(ABOVE, ABOVE)),
        _ok(200, _credits_body(HIGH_TOTAL, HIGH_USED)),
    ]


def test_both_halves_at_or_above_the_minimum_are_sufficient(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    observation, digest, script = asyncio.run(_read(Script(_high_pair())))
    assert observation == CreditObservation("sufficient", None)
    assert digest == subject_digest("openrouter", "openrouter.ai", Decimal("10"))
    assert _paths(script) == ["/api/v1/key", "/api/v1/credits"]
    _assert_clean(repr(observation), caplog.text)


def test_null_key_limit_with_a_sufficient_balance_is_sufficient() -> None:
    observation, _, script = asyncio.run(
        _read(
            Script(
                [
                    _ok(200, _key_body("null", "null")),
                    _ok(200, _credits_body(HIGH_TOTAL, HIGH_USED)),
                ]
            )
        )
    )
    assert observation == CreditObservation("sufficient", None)
    assert _paths(script) == ["/api/v1/key", "/api/v1/credits"]
    _assert_clean(repr(observation))


def test_key_half_below_the_minimum_is_insufficient() -> None:
    observation, _, script = asyncio.run(
        _read(
            Script(
                [
                    _ok(200, _key_body(ABOVE, BELOW)),
                    _ok(200, _credits_body(HIGH_TOTAL, HIGH_USED)),
                ]
            )
        )
    )
    assert observation == CreditObservation("insufficient", None)
    assert _paths(script) == ["/api/v1/key", "/api/v1/credits"]
    _assert_clean(repr(observation))


def test_account_half_below_the_minimum_is_insufficient() -> None:
    observation, _, script = asyncio.run(
        _read(
            Script(
                [
                    _ok(200, _key_body(ABOVE, ABOVE)),
                    _ok(200, _credits_body(LOW_TOTAL, LOW_USED)),
                ]
            )
        )
    )
    assert observation == CreditObservation("insufficient", None)
    _assert_clean(repr(observation))
    assert script.requests


def test_key_401_is_credential_rejected_and_does_not_call_credits() -> None:
    observation, _, script = asyncio.run(
        _read(Script([_ok(401, {"error": {"message": "User not found.", "code": 401}})]))
    )
    assert observation == CreditObservation("unknown", "credential_rejected")
    assert _paths(script) == ["/api/v1/key"]
    _assert_clean(repr(observation))


@pytest.mark.parametrize(
    "key_step,credits_step",
    [
        (httpx.ReadTimeout("timed out"), _ok(200, _credits_body(HIGH_TOTAL, HIGH_USED))),
        (_ok(429), _ok(200, _credits_body(HIGH_TOTAL, HIGH_USED))),
        (_ok(503), _ok(200, _credits_body(HIGH_TOTAL, HIGH_USED))),
        (_ok(200, _key_body(ABOVE, ABOVE)), httpx.ReadTimeout("timed out")),
        (_ok(200, _key_body(ABOVE, ABOVE)), _ok(429)),
        (_ok(200, _key_body(ABOVE, ABOVE)), _ok(503)),
        (_ok(200, _key_body(ABOVE, ABOVE)), _ok(403)),
        (_ok(200, _key_body(ABOVE, ABOVE)), _ok(401)),
    ],
)
def test_an_unreadable_half_with_no_low_half_is_unknown(
    key_step: object, credits_step: object
) -> None:
    observation, _, script = asyncio.run(_read(Script([key_step, credits_step])))
    assert observation == CreditObservation("unknown", "unreadable")
    assert [
        request.url.path
        for request in script.requests
        if request.url.path.endswith("/chat/completions")
    ] == []
    assert all(path.endswith("/key") or path.endswith("/credits") for path in _paths(script))
    _assert_clean(repr(observation))


def test_credits_unreadable_with_key_below_minimum_is_insufficient() -> None:
    observation, _, script = asyncio.run(
        _read(Script([_ok(200, _key_body(ABOVE, BELOW)), _ok(503)]))
    )
    assert observation == CreditObservation("insufficient", None)
    assert _paths(script) == ["/api/v1/key", "/api/v1/credits"]
    _assert_clean(repr(observation))


def test_minimum_zero_accepts_any_key_the_provider_accepts() -> None:
    observation, _, script = asyncio.run(
        _read(Script([_ok(200, _key_body(ABOVE, BELOW)), _ok(503)]), minimum=0)
    )
    assert observation == CreditObservation("sufficient", None)
    assert _paths(script) == ["/api/v1/key", "/api/v1/credits"]
    _assert_clean(repr(observation))


def test_requests_are_only_key_and_credits() -> None:
    _, _, script = asyncio.run(_read(Script(_high_pair())))
    assert [(request.method, request.url.path) for request in script.requests] == [
        ("GET", "/api/v1/key"),
        ("GET", "/api/v1/credits"),
    ]
    for request in script.requests:
        assert request.url.host == "credit.fixture.test"
        assert "messages" not in request.url.path
        assert request.headers["authorization"] == f"Bearer {sample_value()}"


def test_other_host_makes_no_request_and_is_provider_unsupported() -> None:
    observation, digest, script = asyncio.run(
        _read(Script(_high_pair()), base_url="https://example.test/v1")
    )
    assert observation == CreditObservation("unknown", "provider_unsupported")
    assert script.requests == []
    assert digest == subject_digest("unsupported", "example.test", Decimal("10"))
    _assert_clean(repr(observation))


def test_absent_base_url_with_an_openrouter_key_reads_openrouter() -> None:
    observation, digest, script = asyncio.run(_read(Script(_high_pair()), base_url=""))
    assert observation == CreditObservation("sufficient", None)
    assert _paths(script) == ["/api/v1/key", "/api/v1/credits"]
    assert digest == subject_digest("openrouter", "openrouter.ai", Decimal("10"))


def test_absent_base_url_without_an_openrouter_key_makes_no_request() -> None:
    observation, _, script = asyncio.run(
        _read(Script(_high_pair()), base_url="", credentials="placeholder-token")
    )
    assert observation == CreditObservation("unknown", "provider_unsupported")
    assert script.requests == []


def test_empty_key_on_openrouter_is_unreadable_without_a_request() -> None:
    observation, _, script = asyncio.run(_read(Script(_high_pair()), credentials=""))
    assert observation == CreditObservation("unknown", "unreadable")
    assert script.requests == []


def test_a_refused_credential_declaration_is_unreadable_without_a_request() -> None:
    observation, _, script = asyncio.run(_read(Script(_high_pair()), model_env_key="CURIE_API_KEY"))
    assert observation == CreditObservation("unknown", "unreadable")
    assert script.requests == []


def test_negative_minimum_is_refused_and_zero_is_accepted() -> None:
    with pytest.raises(ValidationError):
        WorkerConfig(factory_min_model_credit_usd=-1)
    assert WorkerConfig(factory_min_model_credit_usd=0).factory_min_model_credit_usd == 0
    assert WorkerConfig().factory_min_model_credit_usd == 10


def test_unsafe_schema_names_make_no_request() -> None:
    script = Script(_high_pair())

    async def tick() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(script)) as client:
            loop = FactoryCreditLoop(
                engine=object(),  # type: ignore[arg-type]
                config=WorkerConfig(
                    db_schema="curie-bad",
                    credentials=sample_value(),
                    model_base_url="https://openrouter.ai/api",
                ),
                client=client,
            )
            await loop.tick()

    with pytest.raises(ValueError, match="safe identifier"):
        asyncio.run(tick())
    assert script.requests == []


def _engine():
    return create_async_engine(_DB_URL, poolclass=NullPool)


async def _seed(engine, *, kind: str, address: str, adapter: str | None = None) -> uuid.UUID:
    agent_id = uuid.uuid4()
    async with engine.begin() as connection:
        await connection.execute(
            text(f"INSERT INTO {_SCHEMA}.agents (id, name) VALUES (:id, :name)"),
            {"id": agent_id, "name": f"credit-{agent_id.hex[:8]}"},
        )
        await connection.execute(
            text(
                f"INSERT INTO {_SCHEMA}.agent_channels "
                "(id, agent_id, kind, address, adapter) "
                "VALUES (:id, :agent_id, :kind, :address, :adapter)"
            ),
            {
                "id": uuid.uuid4(),
                "agent_id": agent_id,
                "kind": kind,
                "address": address,
                "adapter": adapter,
            },
        )
    return agent_id


async def _drop(engine, agent_ids: list[uuid.UUID]) -> None:
    async with engine.begin() as connection:
        for agent_id in agent_ids:
            await connection.execute(
                text(f"DELETE FROM {_SCHEMA}.agent_channels WHERE agent_id = :id"),
                {"id": agent_id},
            )
            await connection.execute(
                text(f"DELETE FROM {_SCHEMA}.factory_fact_observations WHERE agent_id = :id"),
                {"id": agent_id},
            )
            await connection.execute(
                text(f"DELETE FROM {_SCHEMA}.agents WHERE id = :id"),
                {"id": agent_id},
            )


async def _rows(engine, agent_ids: list[uuid.UUID]) -> list[dict[str, object]]:
    if not agent_ids:
        return []
    found: list[dict[str, object]] = []
    async with engine.connect() as connection:
        for agent_id in agent_ids:
            result = await connection.execute(
                text(
                    f"SELECT kind, agent_id, scope, item, subject_digest, state, reason, "
                    f"observed_at, expires_at FROM {_SCHEMA}.factory_fact_observations "
                    "WHERE agent_id = :id"
                ),
                {"id": agent_id},
            )
            found.extend(dict(row) for row in result.mappings())
    return found


def test_one_tick_writes_one_row_per_factory_agent_and_refreshes_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG, logger="curie_worker.factory_credit")

    async def go() -> None:
        engine = _engine()
        agent_ids: list[uuid.UUID] = []
        try:
            await pg_connect_or_skip(engine)
            github = await _seed(
                engine, kind="github", address=f"acme/credit-{uuid.uuid4().hex[:8]}"
            )
            both = await _seed(engine, kind="github", address=f"acme/credit-{uuid.uuid4().hex[:8]}")
            slack = await _seed(
                engine,
                kind="slack",
                address=f"slack-credit-{uuid.uuid4().hex[:8]}",
                adapter="default",
            )
            agent_ids.extend([github, both, slack])
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.agent_channels "
                        "(id, agent_id, kind, address, adapter) "
                        "VALUES (:id, :agent_id, 'slack', :address, 'default')"
                    ),
                    {
                        "id": uuid.uuid4(),
                        "agent_id": both,
                        "address": f"slack-also-{uuid.uuid4().hex[:8]}",
                    },
                )
            clock = datetime(2026, 11, 13, 12, 0, tzinfo=UTC)
            script = Script(_high_pair() + _high_pair())

            def now() -> datetime:
                return clock

            async with httpx.AsyncClient(transport=httpx.MockTransport(script)) as client:
                loop = FactoryCreditLoop(
                    engine,
                    WorkerConfig(
                        db_schema=_SCHEMA,
                        credentials=sample_value(),
                        model_base_url="https://openrouter.ai/api",
                        factory_min_model_credit_usd=10,
                    ),
                    client=client,
                    clock=now,
                    environ={"CURIE_OPENROUTER_API_URL": API},
                )
                await loop.tick()
                written = await _rows(engine, agent_ids)
                by_agent = {row["agent_id"]: row for row in written}
                assert set(by_agent) == {github, both}
                digest = subject_digest("openrouter", "openrouter.ai", Decimal("10"))
                for row in by_agent.values():
                    assert row["kind"] == "model_credit"
                    assert row["scope"] == "" and row["item"] == ""
                    assert row["state"] == "sufficient" and row["reason"] is None
                    assert row["subject_digest"] == digest
                    assert row["expires_at"] - row["observed_at"] == timedelta(seconds=900)
                assert "factory credit observation updated" in caplog.text
                caplog.clear()
                clock = datetime(2026, 11, 13, 12, 0, 10, tzinfo=UTC)
                await loop.tick()
                refreshed = {row["agent_id"]: row for row in await _rows(engine, agent_ids)}
                assert refreshed[github]["observed_at"] == clock
                assert refreshed[github]["expires_at"] - refreshed[github][
                    "observed_at"
                ] == timedelta(seconds=900)
                assert "factory credit observation unchanged" in caplog.text
                assert "factory credit observation updated" not in caplog.text
                blob = repr(written) + repr(refreshed) + caplog.text
                _assert_clean(blob)
                assert _paths(script) == [
                    "/api/v1/key",
                    "/api/v1/credits",
                    "/api/v1/key",
                    "/api/v1/credits",
                ]
        finally:
            if agent_ids:
                await _drop(engine, agent_ids)
            await engine.dispose()

    asyncio.run(go())


def test_a_held_lock_skips_the_read_and_the_write() -> None:
    async def go() -> None:
        engine = _engine()
        agent_ids: list[uuid.UUID] = []
        holder = None
        try:
            await pg_connect_or_skip(engine)
            agent_ids.append(
                await _seed(engine, kind="github", address=f"acme/locked-{uuid.uuid4().hex[:8]}")
            )
            script = Script(_high_pair())
            holder = await engine.connect()
            locked = (
                await holder.execute(
                    text(_LOCK_SQL),
                    {"classid": LOCK_CLASS_ID, "objid": LOCK_OBJECT_ID},
                )
            ).scalar()
            await holder.commit()
            assert locked
            async with httpx.AsyncClient(transport=httpx.MockTransport(script)) as client:
                loop = FactoryCreditLoop(
                    engine,
                    WorkerConfig(
                        db_schema=_SCHEMA,
                        credentials=sample_value(),
                        model_base_url="https://openrouter.ai/api",
                    ),
                    client=client,
                    environ={"CURIE_OPENROUTER_API_URL": API},
                )
                await loop.tick()
            assert script.requests == []
            assert await _rows(engine, agent_ids) == []
        finally:
            if holder is not None:
                await holder.execute(
                    text(_UNLOCK_SQL),
                    {"classid": LOCK_CLASS_ID, "objid": LOCK_OBJECT_ID},
                )
                await holder.commit()
                await holder.close()
            if agent_ids:
                await _drop(engine, agent_ids)
            await engine.dispose()

    asyncio.run(go())


def test_a_read_exception_leaves_the_previous_row_and_the_loop_running() -> None:
    async def go() -> None:
        engine = _engine()
        agent_ids: list[uuid.UUID] = []
        try:
            await pg_connect_or_skip(engine)
            agent_id = await _seed(
                engine, kind="github", address=f"acme/boom-{uuid.uuid4().hex[:8]}"
            )
            agent_ids.append(agent_id)
            observed = datetime(2026, 11, 13, 8, 0, tzinfo=UTC)
            digest = "sha256:" + "ab" * 32
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f"INSERT INTO {_SCHEMA}.factory_fact_observations "
                        "(kind, agent_id, scope, item, subject_digest, state, reason, "
                        "observed_at, expires_at) "
                        "VALUES ('model_credit', :agent_id, '', '', :digest, 'sufficient', NULL, "
                        ":observed_at, :expires_at)"
                    ),
                    {
                        "agent_id": agent_id,
                        "digest": digest,
                        "observed_at": observed,
                        "expires_at": observed + timedelta(seconds=900),
                    },
                )
            stop = asyncio.Event()

            def boom(request: httpx.Request) -> httpx.Response:
                stop.set()
                raise RuntimeError("credit read failed")

            async with httpx.AsyncClient(transport=httpx.MockTransport(boom)) as client:
                loop = FactoryCreditLoop(
                    engine,
                    WorkerConfig(
                        db_schema=_SCHEMA,
                        credentials=sample_value(),
                        model_base_url="https://openrouter.ai/api",
                    ),
                    client=client,
                    interval_seconds=300,
                    environ={"CURIE_OPENROUTER_API_URL": API},
                )
                await asyncio.wait_for(loop.run_forever(stop), timeout=2)
            rows = await _rows(engine, agent_ids)
            assert len(rows) == 1
            assert rows[0]["state"] == "sufficient"
            assert rows[0]["subject_digest"] == digest
            assert rows[0]["observed_at"] == observed
            _assert_clean(repr(rows))
        finally:
            if agent_ids:
                await _drop(engine, agent_ids)
            await engine.dispose()

    asyncio.run(go())
