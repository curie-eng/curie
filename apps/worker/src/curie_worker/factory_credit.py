"""Record one model_credit fact per factory agent from the worker's own credential.

The OpenRouter response shapes this reader accepts are the ones recorded in
``cli/src/openrouter_credit.rs`` and ``cli/tests/openrouter_credit.rs``. The
comparison is the only value that leaves this module. Amounts and the key are
not stored, logged, or returned.

An explicit model base URL whose host is not ``openrouter.ai`` is
``provider_unsupported`` and makes no request, including when the credential
uses an OpenRouter prefix. When ``apply_model_env`` leaves the base URL unset
and the credential is one the runner would send to OpenRouter, the host used
for the digest and the read is ``openrouter.ai``. That is the host the runner
selects when it consumes the env this module builds.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlsplit

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from .binding import (
    BASE_URL_ENV,
    CREDENTIALS_ENV,
    MODEL_ENV_KEY_ENV,
    apply_model_env,
)
from .config import WorkerConfig

logger = logging.getLogger(__name__)

INTERVAL_SECONDS = 300
EXPIRY_SECONDS = 900
LOCK_CLASS_ID = 5581
LOCK_OBJECT_ID = 300
DEFAULT_OPENROUTER_API = "https://openrouter.ai/api/v1"
OPENROUTER_API_ENV = "CURIE_OPENROUTER_API_URL"
OPENROUTER_HOST = "openrouter.ai"
_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
_LOCK = text("SELECT pg_try_advisory_lock(CAST(:classid AS integer), CAST(:objid AS integer))")
_UNLOCK = text("SELECT pg_advisory_unlock(CAST(:classid AS integer), CAST(:objid AS integer))")
_LOCK_ARGS = {"classid": LOCK_CLASS_ID, "objid": LOCK_OBJECT_ID}


@dataclass(frozen=True)
class CreditObservation:
    """The comparison result. ``repr`` names state and reason only."""

    state: str
    reason: str | None

    def __repr__(self) -> str:
        return f"CreditObservation(state={self.state!r}, reason={self.reason!r})"


def canonical_minimum(value: Decimal) -> str:
    """One spelling for 10 and 10.0 so the digest does not change with formatting."""

    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def subject_digest(provider: str, host: str, minimum: Decimal) -> str:
    """``sha256:`` over the provider name, the base URL host, and the minimum."""

    payload = {
        "host": host,
        "minimum_usd": canonical_minimum(minimum),
        "provider": provider,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _minimum(value: float) -> Decimal:
    return Decimal(str(value))


def _host(base_url: str) -> str:
    if not base_url:
        return ""
    return (urlsplit(base_url).hostname or "").lower()


def _declared_names(env: Mapping[str, str]) -> tuple[str, ...] | None:
    """The credential names ``resolve_credential`` would walk, or None if refused.

    The walk is local. Importing ``curie_runner`` pulls the Claude adapter,
    which the worker is not allowed to import.
    """

    raw = env.get(MODEL_ENV_KEY_ENV, "").strip()
    if not raw:
        return (CREDENTIALS_ENV,)
    try:
        decoded: object = json.loads(raw)
    except ValueError:
        decoded = raw
    if isinstance(decoded, str):
        names: list[object] = [decoded]
    elif isinstance(decoded, list):
        names = list(decoded)
    else:
        return None
    if not all(isinstance(item, str) for item in names):
        return None
    keys = tuple(item.strip() for item in names if isinstance(item, str) and item.strip())
    if not keys:
        return None
    for key in keys:
        if key.startswith("CURIE_") and key != CREDENTIALS_ENV:
            return None
    return keys


def _credential(env: Mapping[str, str]) -> str | None:
    """The first non-empty declared credential, ``""`` when none, None when refused."""

    names = _declared_names(env)
    if names is None:
        return None
    for name in names:
        value = env.get(name)
        if value:
            return value
    return ""


def _api_url(environ: Mapping[str, str]) -> str:
    raw = environ.get(OPENROUTER_API_ENV, "").strip()
    return (raw or DEFAULT_OPENROUTER_API).rstrip("/")


def _number(value: object) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return Decimal(str(value))


def _key_half(payload: object, minimum: Decimal) -> str:
    """``ok``, ``low``, or ``unreadable``. A JSON null limit satisfies the half."""

    if not isinstance(payload, dict):
        return "unreadable"
    data = payload.get("data")
    if not isinstance(data, dict):
        return "unreadable"
    if "limit" in data and data["limit"] is None:
        return "ok"
    remaining = _number(data.get("limit_remaining"))
    if remaining is None:
        return "unreadable"
    return "ok" if remaining >= minimum else "low"


def _account_half(payload: object, minimum: Decimal) -> str:
    if not isinstance(payload, dict):
        return "unreadable"
    data = payload.get("data")
    if not isinstance(data, dict):
        return "unreadable"
    total = _number(data.get("total_credits"))
    used = _number(data.get("total_usage"))
    if total is None or used is None:
        return "unreadable"
    remaining = total - used
    return "ok" if remaining >= minimum else "low"


def _json(response: httpx.Response) -> object | None:
    try:
        body = response.json()
    except ValueError:
        return None
    if isinstance(body, (dict, list, str, int, float)) or body is None:
        return body
    return None


async def _get(client: httpx.AsyncClient, url: str, key: str) -> httpx.Response | None:
    try:
        return await client.get(url, headers={"Authorization": f"Bearer {key}"})
    except httpx.HTTPError:
        return None


def _classify(
    *,
    minimum: Decimal,
    key_code: int | None,
    key_body: object | None,
    credits_code: int | None,
    credits_body: object | None,
) -> CreditObservation:
    if key_code == 401:
        return CreditObservation("unknown", "credential_rejected")
    # A zero minimum still reads both endpoints. Any key /key accepts is sufficient.
    if minimum == 0 and key_code is not None and 200 <= key_code < 300:
        return CreditObservation("sufficient", None)
    key_status = "unreadable"
    if key_code is not None and 200 <= key_code < 300:
        key_status = _key_half(key_body, minimum)
    account_status = "unreadable"
    if credits_code is not None and 200 <= credits_code < 300:
        account_status = _account_half(credits_body, minimum)
    if key_status == "low" or account_status == "low":
        return CreditObservation("insufficient", None)
    if key_status == "ok" and account_status == "ok":
        return CreditObservation("sufficient", None)
    return CreditObservation("unknown", "unreadable")


async def read_openrouter(
    key: str,
    *,
    minimum: Decimal,
    api_url: str,
    client: httpx.AsyncClient,
) -> CreditObservation:
    """GET ``/key`` and, unless that answer is 401, GET ``/credits``."""

    key_response = await _get(client, f"{api_url}/key", key)
    key_code = None if key_response is None else key_response.status_code
    key_body = None if key_response is None else _json(key_response)
    if key_code == 401:
        return CreditObservation("unknown", "credential_rejected")
    credits_response = await _get(client, f"{api_url}/credits", key)
    credits_code = None if credits_response is None else credits_response.status_code
    credits_body = None if credits_response is None else _json(credits_response)
    return _classify(
        minimum=minimum,
        key_code=key_code,
        key_body=key_body,
        credits_code=credits_code,
        credits_body=credits_body,
    )


async def observe(
    config: WorkerConfig,
    *,
    client: httpx.AsyncClient,
    environ: Mapping[str, str] | None = None,
) -> tuple[CreditObservation, str]:
    """Resolve the credential through ``apply_model_env`` and classify one read.

    Returns the observation and the subject digest. No request is made for an
    unsupported host, a missing key, or a refused credential declaration.
    """

    env: dict[str, str] = {}
    apply_model_env(env, config)
    host = _host(env.get(BASE_URL_ENV, ""))
    credential = _credential(env)
    minimum = _minimum(config.factory_min_model_credit_usd)
    if credential is None:
        return (
            CreditObservation("unknown", "unreadable"),
            subject_digest("unsupported", host, minimum),
        )
    if host == OPENROUTER_HOST or (host == "" and credential.startswith("sk-or-")):
        provider_host = OPENROUTER_HOST
        digest = subject_digest("openrouter", provider_host, minimum)
        if not credential:
            return CreditObservation("unknown", "unreadable"), digest
        observation = await read_openrouter(
            credential,
            minimum=minimum,
            api_url=_api_url(os.environ if environ is None else environ),
            client=client,
        )
        return observation, digest
    return (
        CreditObservation("unknown", "provider_unsupported"),
        subject_digest("unsupported", host, minimum),
    )


class FactoryCreditLoop:
    """One locked credit read on start and every ``interval_seconds`` after that."""

    def __init__(
        self,
        engine: AsyncEngine,
        config: WorkerConfig,
        *,
        client: httpx.AsyncClient | None = None,
        interval_seconds: float = INTERVAL_SECONDS,
        clock: Callable[[], datetime] | None = None,
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self._engine = engine
        self._config = config
        self._schema = config.db_schema
        self._client = client
        self._interval = interval_seconds
        self._clock = clock or (lambda: datetime.now(UTC))
        self._environ = environ

    async def run_forever(self, stop: asyncio.Event | None = None) -> None:
        """Observe until ``stop``. One failed tick logs and waits. It does not exit."""

        stop = stop or asyncio.Event()
        owns_client = self._client is None
        if owns_client:
            self._client = httpx.AsyncClient(timeout=_TIMEOUT)
        logger.info("factory credit loop started interval=%ss", self._interval)
        try:
            while not stop.is_set():
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("factory credit observation failed; retrying next interval")
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._interval)
                except TimeoutError:
                    continue
        finally:
            if owns_client and self._client is not None:
                await self._client.aclose()
                self._client = None
            logger.info("factory credit loop stopped")

    async def tick(self) -> None:
        """Read once and upsert one row per GitHub-bound agent. Skip if the lock is held."""

        if _SCHEMA_NAME.fullmatch(self._schema) is None:
            raise ValueError("db_schema is not a safe identifier")
        if self._client is not None:
            await self._locked_tick(self._client)
            return
        client = httpx.AsyncClient(timeout=_TIMEOUT)
        try:
            await self._locked_tick(client)
        finally:
            await client.aclose()

    async def _locked_tick(self, client: httpx.AsyncClient) -> None:
        schema = self._schema
        async with self._engine.connect() as connection:
            locked = (await connection.execute(_LOCK, _LOCK_ARGS)).scalar()
            await connection.commit()
            if not locked:
                logger.debug("factory credit observation skipped; lock held")
                return
            try:
                observation, digest = await observe(
                    self._config, client=client, environ=self._environ
                )
                observed_at = self._clock()
                if observed_at.utcoffset() is None:
                    observed_at = observed_at.replace(tzinfo=UTC)
                expires_at = observed_at + timedelta(seconds=EXPIRY_SECONDS)
                async with connection.begin():
                    await _write(
                        connection,
                        schema,
                        observation,
                        digest,
                        observed_at,
                        expires_at,
                    )
            finally:
                await connection.execute(_UNLOCK, _LOCK_ARGS)
                await connection.commit()


async def _write(
    connection: AsyncConnection,
    schema: str,
    observation: CreditObservation,
    digest: str,
    observed_at: datetime,
    expires_at: datetime,
) -> None:
    agents = (
        (
            await connection.execute(
                text(f"SELECT DISTINCT agent_id FROM {schema}.agent_channels WHERE kind = 'github'")
            )
        )
        .scalars()
        .all()
    )
    if not agents:
        logger.debug("factory credit observation has no factory agents")
        return
    existing = {
        row.agent_id: (row.state, row.reason, row.subject_digest)
        for row in (
            await connection.execute(
                text(
                    f"SELECT agent_id, state, reason, subject_digest "
                    f"FROM {schema}.factory_fact_observations "
                    "WHERE kind = 'model_credit' AND scope = '' AND item = ''"
                )
            )
        )
    }
    current = (observation.state, observation.reason, digest)
    changed = any(existing.get(agent_id) != current for agent_id in agents)
    statement = text(
        f"INSERT INTO {schema}.factory_fact_observations "
        "(kind, agent_id, scope, item, subject_digest, state, reason, observed_at, expires_at) "
        "VALUES ('model_credit', :agent_id, '', '', :subject_digest, :state, :reason, "
        ":observed_at, :expires_at) "
        "ON CONFLICT (kind, agent_id, scope, item) DO UPDATE SET "
        "subject_digest = EXCLUDED.subject_digest, "
        "state = EXCLUDED.state, "
        "reason = EXCLUDED.reason, "
        "observed_at = EXCLUDED.observed_at, "
        "expires_at = EXCLUDED.expires_at"
    )
    for agent_id in agents:
        await connection.execute(
            statement,
            {
                "agent_id": agent_id,
                "subject_digest": digest,
                "state": observation.state,
                "reason": observation.reason,
                "observed_at": observed_at,
                "expires_at": expires_at,
            },
        )
    if changed:
        logger.info(
            "factory credit observation updated agents=%s state=%s reason=%s",
            len(agents),
            observation.state,
            observation.reason,
        )
    else:
        logger.debug(
            "factory credit observation unchanged agents=%s state=%s reason=%s",
            len(agents),
            observation.state,
            observation.reason,
        )
