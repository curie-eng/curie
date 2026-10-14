"""Token usage and estimated cost per factory run (#3223).

The runner reports each model's tokens at turn end over the request-bound
``work_item.progress`` token. Each (request, turn, model) is stored once, with
an estimate priced from a public model price list when one matches. A model
with no price keeps its tokens and no estimate, so the cost is never a guess.

Reads sum every request of a WorkItem (retries and CI-fix rounds are requests
on it) for the operator route and the terminal status comment's usage line.

Prices: OpenRouter's public model list, https://openrouter.ai/docs/api-reference/list-available-models
(per-token USD strings under ``pricing``).
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Any, Literal, Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.schemas.workitems import (
    WorkItemUsageModel,
    WorkItemUsageOut,
    WorkItemUsagePriceSource,
    WorkItemUsageRequest,
    WorkItemUsageRole,
)

from .config import get_settings
from .models import ExecutionRequest, ExecutionRequestModelUsage

logger = logging.getLogger(__name__)

MAX_MODELS_PER_REPORT = 20
MAX_TOKENS = 10**12
PRICE_TTL_S = 6 * 3600
# A failed fetch is retried no sooner than this, so a down price list does not
# add a network round trip to every report.
PRICE_FAILURE_TTL_S = 300
_FETCH_TIMEOUT_S = 10.0
_COST_QUANTUM = Decimal("0.000001")
# A trailing context-window token on a model id, such as ``[1m]`` (#3992).
_CONTEXT_WINDOW_SUFFIX = re.compile(r"\[[^\[\]]*\]$")


# --- wire -------------------------------------------------------------------------


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ModelUsageEntry(_Strict):
    model: str = Field(min_length=1, max_length=200)
    input_tokens: int = Field(ge=0, le=MAX_TOKENS)
    cached_input_tokens: int = Field(ge=0, le=MAX_TOKENS)
    cache_write_tokens: int = Field(ge=0, le=MAX_TOKENS)
    output_tokens: int = Field(ge=0, le=MAX_TOKENS)
    # From the runner's SDK observation; authoritative over primary_model.
    role: Literal["implementer", "reviewer"] | None = None


class UsageReport(_Strict):
    turn_id: str = Field(min_length=1, max_length=200)
    primary_model: str | None = Field(default=None, max_length=200)
    models: list[ModelUsageEntry] = Field(max_length=MAX_MODELS_PER_REPORT)


# --- pricing ----------------------------------------------------------------------


@dataclass(frozen=True, kw_only=True)
class Price:
    """Per-token USD rates for one model, and where and when they were read."""

    prompt: Decimal
    completion: Decimal
    cache_read: Decimal
    cache_write: Decimal
    source: str
    as_of: datetime


class PriceBook(Protocol):
    async def price(self, model: str) -> Price | None: ...


def _normalize(name: str) -> str:
    return name.rsplit("/", 1)[-1].lower().replace(".", "-")


def _rate(value: Any) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        rate = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not rate.is_finite() or rate < 0:
        return None
    return rate


def _rates(entry: dict[str, Any]) -> dict[str, Decimal] | None:
    pricing = entry.get("pricing")
    if not isinstance(pricing, dict):
        return None
    prompt = _rate(pricing.get("prompt"))
    completion = _rate(pricing.get("completion"))
    if prompt is None or completion is None:
        return None
    cache_read = _rate(pricing.get("input_cache_read"))
    cache_write = _rate(pricing.get("input_cache_write"))
    return {
        "prompt": prompt,
        "completion": completion,
        "cache_read": prompt if cache_read is None else cache_read,
        "cache_write": prompt if cache_write is None else cache_write,
    }


def match_price(models_payload: dict[str, Any], model: str) -> dict[str, Decimal] | None:
    """Rates for ``model``: exact id first, else a unique normalized-name match.

    A context-window suffix such as ``[1m]`` is priced at its base model's rates.
    """

    data = models_payload.get("data") if isinstance(models_payload, dict) else None
    if not isinstance(data, list):
        return None
    model = _CONTEXT_WINDOW_SUFFIX.sub("", model) or model
    entries = [e for e in data if isinstance(e, dict) and isinstance(e.get("id"), str)]
    for entry in entries:
        if entry["id"] == model:
            return _rates(entry)
    wanted = _normalize(model)
    matches = [entry for entry in entries if _normalize(entry["id"]) == wanted]
    if len(matches) != 1:
        return None
    return _rates(matches[0])


def estimate_cost(
    price: Price,
    *,
    input_tokens: int,
    cached_input_tokens: int,
    cache_write_tokens: int,
    output_tokens: int,
) -> Decimal:
    return (
        input_tokens * price.prompt
        + cached_input_tokens * price.cache_read
        + cache_write_tokens * price.cache_write
        + output_tokens * price.completion
    )


class OpenRouterPriceBook:
    """Reads an OpenRouter-shaped model list, cached in process for 6 h.

    Any fetch or parse failure yields no price for every model until the next
    attempt, which is at least ``PRICE_FAILURE_TTL_S`` later.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._payload: dict[str, Any] | None = None
        self._fetched_at: datetime | None = None
        self._expires = 0.0
        self._lock = asyncio.Lock()

    async def _load(self) -> tuple[dict[str, Any], datetime] | None:
        async with self._lock:
            if time.monotonic() >= self._expires:
                self._payload, self._fetched_at = await self._fetch()
                ttl = PRICE_TTL_S if self._payload is not None else PRICE_FAILURE_TTL_S
                self._expires = time.monotonic() + ttl
            if self._payload is None or self._fetched_at is None:
                return None
            return self._payload, self._fetched_at

    async def _fetch(self) -> tuple[dict[str, Any] | None, datetime | None]:
        try:
            async with httpx.AsyncClient(timeout=_FETCH_TIMEOUT_S) as client:
                response = await client.get(self._url)
                response.raise_for_status()
                payload = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            logger.warning("factory price list fetch failed: %s", type(exc).__name__)
            return None, None
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            logger.warning("factory price list has no data list")
            return None, None
        return payload, datetime.now(UTC)

    async def price(self, model: str) -> Price | None:
        loaded = await self._load()
        if loaded is None:
            return None
        payload, fetched_at = loaded
        rates = match_price(payload, model)
        if rates is None:
            return None
        return Price(**rates, source=self._url, as_of=fetched_at)


class _NoPriceBook:
    async def price(self, model: str) -> Price | None:
        return None


_BOOKS: dict[str, OpenRouterPriceBook] = {}


def get_price_book() -> PriceBook:
    """FastAPI dependency: the configured price book, one per source URL."""

    url = get_settings().factory_price_source_url.strip()
    if not url:
        return _NoPriceBook()
    book = _BOOKS.get(url)
    if book is None:
        book = _BOOKS[url] = OpenRouterPriceBook(url)
    return book


# --- record -----------------------------------------------------------------------


def _role(model: str, primary: str | None, count: int) -> str:
    if model == primary or (primary is None and count == 1):
        return "implementer"
    return "reviewer"


async def record_usage(
    session: AsyncSession,
    *,
    request_id: uuid.UUID,
    body: UsageReport,
    price_book: PriceBook,
) -> bool:
    """Store the report on ``request_id``. False when the request does not exist.

    A replayed (request, turn, model) is a no-op. Usage is a fact, so it is
    recorded whatever the request's status.
    """

    found = await session.scalar(
        select(ExecutionRequest.id).where(ExecutionRequest.id == request_id)
    )
    if found is None:
        return False
    rows: list[dict[str, Any]] = []
    for entry in body.models:
        price = await price_book.price(entry.model)
        cost: Decimal | None = None
        if price is not None:
            cost = estimate_cost(
                price,
                input_tokens=entry.input_tokens,
                cached_input_tokens=entry.cached_input_tokens,
                cache_write_tokens=entry.cache_write_tokens,
                output_tokens=entry.output_tokens,
            ).quantize(_COST_QUANTUM, rounding=ROUND_HALF_UP)
        rows.append(
            {
                "execution_request_id": request_id,
                "turn_id": body.turn_id,
                "model": entry.model,
                "role": entry.role or _role(entry.model, body.primary_model, len(body.models)),
                "input_tokens": entry.input_tokens,
                "cached_input_tokens": entry.cached_input_tokens,
                "cache_write_tokens": entry.cache_write_tokens,
                "output_tokens": entry.output_tokens,
                "estimated_cost_usd": cost,
                "price_source": price.source if price is not None else None,
                "price_as_of": price.as_of if price is not None else None,
            }
        )
    if rows:
        await session.execute(
            insert(ExecutionRequestModelUsage)
            .values(rows)
            .on_conflict_do_nothing(
                index_elements=["execution_request_id", "turn_id", "model", "role"]
            )
        )
    await session.commit()
    return True


# --- read -------------------------------------------------------------------------


def _tokens(row: ExecutionRequestModelUsage) -> int:
    return row.input_tokens + row.cached_input_tokens + row.cache_write_tokens + row.output_tokens


def _add(total: Decimal | None, cost: Decimal | None) -> Decimal | None:
    if cost is None:
        return total
    return cost if total is None else total + cost


async def work_item_usage(session: AsyncSession, work_item_id: uuid.UUID) -> WorkItemUsageOut:
    """Usage summed over every request of the WorkItem. PR fields are left unset."""

    rows = list(
        await session.scalars(
            select(ExecutionRequestModelUsage)
            .join(
                ExecutionRequest,
                ExecutionRequest.id == ExecutionRequestModelUsage.execution_request_id,
            )
            .where(ExecutionRequest.work_item_id == work_item_id)
            .order_by(ExecutionRequest.sequence, ExecutionRequestModelUsage.id)
        )
    )
    roles: dict[str, WorkItemUsageRole] = {}
    models: dict[tuple[str, str], WorkItemUsageModel] = {}
    requests: dict[uuid.UUID, WorkItemUsageRequest] = {}
    sources: dict[str, datetime] = {}
    total: Decimal | None = None
    complete = True
    for row in rows:
        tokens = _tokens(row)
        # Zero tokens cost zero whether or not a price matched.
        cost = row.estimated_cost_usd
        if cost is None and not tokens:
            cost = Decimal(0)
        complete = complete and cost is not None
        total = _add(total, cost)
        role = roles.setdefault(row.role, WorkItemUsageRole())
        role.tokens += tokens
        role.estimated_cost_usd = _add(role.estimated_cost_usd, cost)
        role.cost_complete = role.cost_complete and cost is not None
        model = models.setdefault(
            (row.model, row.role),
            WorkItemUsageModel.model_validate({"model": row.model, "role": row.role}),
        )
        model.input_tokens += row.input_tokens
        model.cached_input_tokens += row.cached_input_tokens
        model.cache_write_tokens += row.cache_write_tokens
        model.output_tokens += row.output_tokens
        model.estimated_cost_usd = _add(model.estimated_cost_usd, cost)
        request = requests.setdefault(
            row.execution_request_id,
            WorkItemUsageRequest(request_id=row.execution_request_id),
        )
        request.tokens += tokens
        request.estimated_cost_usd = _add(request.estimated_cost_usd, cost)
        if row.price_source is not None and row.price_as_of is not None:
            seen = sources.get(row.price_source)
            if seen is None or row.price_as_of > seen:
                sources[row.price_source] = row.price_as_of
    # A started run with no usage row reported nothing: not a zero.
    reported = select(ExecutionRequestModelUsage.execution_request_id).distinct()
    missing = (
        await session.scalar(
            select(func.count())
            .select_from(ExecutionRequest)
            .where(
                ExecutionRequest.work_item_id == work_item_id,
                ExecutionRequest.started_at.is_not(None),
                ExecutionRequest.id.not_in(reported),
            )
        )
        or 0
    )
    return WorkItemUsageOut(
        work_item_id=work_item_id,
        total_tokens=sum(role.tokens for role in roles.values()),
        estimated_cost_usd=total,
        cost_complete=complete and missing == 0,
        requests_without_usage=missing,
        roles=roles,
        models=list(models.values()),
        requests=list(requests.values()),
        price_sources=[
            WorkItemUsagePriceSource(source=source, as_of=as_of)
            for source, as_of in sorted(sources.items())
        ],
    )


def _money(value: Decimal) -> str:
    return f"${value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,}"


def _role_text(label: str, role: WorkItemUsageRole) -> str:
    if role.estimated_cost_usd is None or not role.cost_complete:
        cost = "cost unknown"
    else:
        cost = _money(role.estimated_cost_usd)
    return f"{label} {role.tokens:,} tokens ({cost})"


def usage_line(usage: WorkItemUsageOut) -> str | None:
    """The terminal comment's one ``Usage:`` line; None when no run started."""

    if not usage.total_tokens and not usage.models:
        if not usage.requests_without_usage:
            return None
        runs = "run" if usage.requests_without_usage == 1 else "runs"
        return f"Usage: not reported for {usage.requests_without_usage} {runs}"
    parts: list[str] = []
    if "implementer" in usage.roles:
        parts.append(_role_text("implementer", usage.roles["implementer"]))
    if "reviewer" in usage.roles:
        parts.append(_role_text("reviewers", usage.roles["reviewer"]))
    if usage.estimated_cost_usd is None:
        parts.append("total cost unknown")
    elif usage.cost_complete:
        parts.append(f"total {_money(usage.estimated_cost_usd)}")
    else:
        parts.append(f"total at least {_money(usage.estimated_cost_usd)}")
    line = "Usage: " + ", ".join(parts)
    if usage.price_sources:
        names = ", ".join(entry.source for entry in usage.price_sources)
        as_of = max(entry.as_of for entry in usage.price_sources).astimezone(UTC).date()
        line += f" (estimated from {names} prices as of {as_of.isoformat()})"
    if usage.requests_without_usage:
        runs = "run" if usage.requests_without_usage == 1 else "runs"
        line += f", usage not reported for {usage.requests_without_usage} {runs}"
    # A source URL is operator configuration, never model text, but keep the
    # line one line whatever it holds.
    return " ".join(line.split())
