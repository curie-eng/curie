"""Probe the worker's own provider allowance while a provider gate waits on it (#5426).

A factory retry blocked on ``provider_credit``, ``provider_usage_reset`` or
``provider_credential`` names the provider scope that failed. The API releases
such a block only on a fresh Valkey observation of the current scope (see
``curie_internal.provider_gate_ledger``). This loop keeps that observation:

1. Every tick it registers the scope this worker's model env derives as the
   provider's current scope.
2. When a waiting successor's predecessor failed on this scope for credit or
   usage, or failed with a rejected credential on another scope of the same
   provider, and the provider has a non-generating read, one worker takes the
   scope's probe lease, reads OpenRouter GET /api/v1/key once and records the
   reading.

The credential, the response body and the amounts are never logged, stored or
returned. Only the provider name and the reading's outcome are logged.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime

import httpx
from curie_internal.provider_gate_ledger import (
    claim_probe,
    find_provider,
    read_scope,
    record_probe,
    register_current,
)
from curie_internal.provider_scope import (
    CURIE_MODEL_AUTH_MODE,
    PROBED_PROVIDERS,
    AllowanceReading,
    ProviderScope,
    parse_openrouter_key,
    provider_scope,
    scope_credential,
)
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from .binding import apply_model_env
from .config import WorkerConfig
from .factory_credit import _api_url

logger = logging.getLogger(__name__)

TICK_SECONDS = 15.0
READ_DEADLINE_SECONDS = 10.0
# A scope probed this recently is not probed again.
PROBE_INTERVAL_MS = 30_000
_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PROBED_BLOCKS = ("provider_credit", "provider_usage_reset", "provider_credential")
_ALLOWANCE_REASONS = frozenset({"credit_exhausted", "usage_reset"})
_CREDENTIAL_REASON = "credential_rejected"


async def read_openrouter_allowance(
    key: str, *, api_url: str, client: httpx.AsyncClient
) -> AllowanceReading:
    """One GET ``{api_url}/key`` with ``key`` as the Bearer token, parsed.

    Contract:
    https://openrouter.ai/docs/api/api-reference/api-keys/get-current-api-key
    A redirect is not followed. A transport error, a body that is not JSON, or
    no answer within ``READ_DEADLINE_SECONDS`` is unreadable.
    """

    status: int | None = None
    body: object = None
    try:
        async with asyncio.timeout(READ_DEADLINE_SECONDS):
            response = await client.get(
                f"{api_url}/key",
                headers={"Authorization": f"Bearer {key}"},
                follow_redirects=False,
            )
            status = response.status_code
            try:
                body = response.json()
            except ValueError:
                body = None
    except (httpx.HTTPError, TimeoutError):
        status, body = None, None
    return parse_openrouter_key(status, body)


def _gates_sql(schema: str) -> str:
    blocks = ", ".join(f"'{block}'" for block in _PROBED_BLOCKS)
    return (
        "SELECT DISTINCT p.retry_condition_reason, p.retry_condition_scope_key "
        f"FROM {schema}.execution_requests s "
        f"JOIN {schema}.execution_requests p ON p.id = s.retry_of_request_id "
        "WHERE s.status = 'waiting' "
        "AND s.started_at IS NULL "
        f"AND s.blocked_on IN ({blocks}) "
        "AND s.wait_deadline > now() "
        "AND p.retry_condition_scope_key IS NOT NULL "
        "AND p.retry_condition_retry_at IS NULL"
    )


class ProviderProbeLoop:
    """Register the worker's provider scope and probe it when a gate waits on it."""

    def __init__(
        self,
        engine: AsyncEngine,
        config: WorkerConfig,
        redis: Redis,
        *,
        client: httpx.AsyncClient | None = None,
        clock: Callable[[], datetime] | None = None,
        owner: str | None = None,
        tick_seconds: float = TICK_SECONDS,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self._engine = engine
        self._config = config
        self._redis = redis
        self._prefix = config.key_prefix
        self._client = client
        self._clock = clock or (lambda: datetime.now(UTC))
        self._owner = owner or f"provider-probe:{uuid.uuid4().hex}"
        self._tick_seconds = tick_seconds
        self._environ = environ

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        """Tick until ``stop``. One failed tick logs and waits. It does not exit."""

        stop = stop or asyncio.Event()
        owns_client = self._client is None
        if owns_client:
            self._client = httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=False)
        logger.info("provider probe loop started interval=%ss", self._tick_seconds)
        try:
            while not stop.is_set():
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("provider probe tick failed; retrying next interval")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._tick_seconds)
                except TimeoutError:
                    continue
        finally:
            if owns_client and self._client is not None:
                await self._client.aclose()
                self._client = None
            logger.info("provider probe loop stopped")

    def _now_ms(self) -> int:
        now = self._clock()
        if now.utcoffset() is None:
            now = now.replace(tzinfo=UTC)
        return int(now.timestamp() * 1000)

    def _env(self) -> dict[str, str]:
        env: dict[str, str] = {}
        apply_model_env(env, self._config)
        # A personal installation runs WorkItems on their owners' grants, never on
        # this worker's service credential, so it has no scope to register.
        if self._config.factory_model_auth_mode == "personal":
            env[CURIE_MODEL_AUTH_MODE] = "personal"
        return env

    async def tick(self) -> None:
        """Register the current scope and probe it once if a gate waits on it."""

        env = self._env()
        scope = provider_scope(env)
        if scope is None:
            return
        await register_current(self._redis, self._prefix, scope, self._now_ms())
        if scope.provider not in PROBED_PROVIDERS:
            return
        if not await self._probe_needed(scope):
            return
        if await self._recently_probed(scope):
            return
        token = await claim_probe(
            self._redis, self._prefix, scope.provider, scope.scope_key, self._owner
        )
        if token is None:
            return
        # A peer may have recorded between the check above and the claim. The
        # lease then lapses on its own; that only delays the next probe.
        if await self._recently_probed(scope):
            return
        credential = scope_credential(env)
        if not credential:
            return
        reading = await self._read(credential)
        recorded = await record_probe(
            self._redis,
            self._prefix,
            scope.provider,
            scope.scope_key,
            token,
            self._now_ms(),
            reading,
        )
        logger.info("provider probe read provider=%s outcome=%s", scope.provider, reading.outcome)
        if not recorded:
            logger.debug("provider probe reading refused; the lease lapsed first")

    async def _read(self, credential: str) -> AllowanceReading:
        api_url = _api_url(os.environ if self._environ is None else self._environ)
        if self._client is not None:
            return await read_openrouter_allowance(credential, api_url=api_url, client=self._client)
        async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=False) as client:
            return await read_openrouter_allowance(credential, api_url=api_url, client=client)

    async def _recently_probed(self, scope: ProviderScope) -> bool:
        observation = await read_scope(self._redis, self._prefix, scope.provider, scope.scope_key)
        if observation is None or observation.probed_at_ms is None:
            return False
        return self._now_ms() - observation.probed_at_ms < PROBE_INTERVAL_MS

    async def _probe_needed(self, scope: ProviderScope) -> bool:
        schema = self._config.db_schema
        if _SCHEMA_NAME.fullmatch(schema) is None:
            raise ValueError("db_schema is not a safe identifier")
        async with self._engine.connect() as connection:
            rows = (await connection.execute(text(_gates_sql(schema)))).all()
        for reason, failed_scope in rows:
            if reason in _ALLOWANCE_REASONS and failed_scope == scope.scope_key:
                return True
        for reason, failed_scope in rows:
            if (
                reason == _CREDENTIAL_REASON
                and failed_scope != scope.scope_key
                and await find_provider(self._redis, self._prefix, failed_scope) == scope.provider
            ):
                return True
        return False
