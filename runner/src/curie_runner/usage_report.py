"""Per-model token usage for a factory run's cost line (#3223).

At each turn's ``ResultMessage`` boundary the runner POSTs the turn's per-model
token counts to ``<progress_url>/usage`` over the same request-bound progress
token. A failure is logged and never fails the turn.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from typing import Any, Protocol

import aiohttp
from claude_agent_sdk import AssistantMessage, ResultMessage

logger = logging.getLogger(__name__)

USAGE_PATH = "/usage"
_TIMEOUT_SECONDS = 10.0

# ResultMessage.model_usage camelCase key -> wire key.
_MODEL_USAGE_KEYS = {
    "inputTokens": "input_tokens",
    "cacheReadInputTokens": "cached_input_tokens",
    "cacheCreationInputTokens": "cache_write_tokens",
    "outputTokens": "output_tokens",
}
# ResultMessage.usage snake_case key -> wire key.
_USAGE_KEYS = {
    "input_tokens": "input_tokens",
    "cache_read_input_tokens": "cached_input_tokens",
    "cache_creation_input_tokens": "cache_write_tokens",
    "output_tokens": "output_tokens",
}


IMPLEMENTER = "implementer"
REVIEWER = "reviewer"
_WIRE_KEYS = ("input_tokens", "cached_input_tokens", "cache_write_tokens", "output_tokens")

Observed = Mapping[tuple[str, str], Mapping[str, int]]


class UsageSink(Protocol):
    def observe(self, message: AssistantMessage) -> None: ...

    async def report(self, message: ResultMessage, primary_model: str | None) -> None: ...


def _count(raw: object) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        return 0
    return max(int(raw), 0)


def _entry(model: str, raw: dict[str, Any], keys: dict[str, str]) -> dict[str, Any]:
    entry: dict[str, Any] = {"model": model}
    for source, wire in keys.items():
        entry[wire] = _count(raw.get(source))
    return entry


def _turn_id(message: ResultMessage, models: list[dict[str, Any]]) -> str:
    uuid = getattr(message, "uuid", None)
    if isinstance(uuid, str) and uuid:
        return uuid
    digest = hashlib.sha256(
        json.dumps(
            [message.session_id, message.num_turns, message.duration_ms, models],
            sort_keys=True,
        ).encode()
    ).hexdigest()[:16]
    return f"{message.session_id}:{digest}"


def _split(
    model: str,
    totals: dict[str, Any],
    observed: Observed,
    primary_model: str | None,
) -> list[dict[str, Any]]:
    """Split one model's turn totals into implementer and reviewer entries.

    The role comes from the SDK: an ``AssistantMessage`` with a
    ``parent_tool_use_id`` is a subagent (reviewer) message.
    """

    reviewer_seen = observed.get((REVIEWER, model))
    implementer_seen = (IMPLEMENTER, model) in observed
    if reviewer_seen is None:
        return [{**totals, "role": IMPLEMENTER}]
    if not implementer_seen and model != primary_model:
        return [{**totals, "role": REVIEWER}]
    reviewer = {"model": model, "role": REVIEWER}
    implementer = {"model": model, "role": IMPLEMENTER}
    for key in _WIRE_KEYS:
        total = totals[key]
        part = min(_count(reviewer_seen.get(key)), total)
        reviewer[key] = part
        implementer[key] = max(total - part, 0)
    return [entry for entry in (implementer, reviewer) if any(entry[key] for key in _WIRE_KEYS)]


def build_usage_body(
    message: ResultMessage,
    primary_model: str | None,
    observed: Observed | None = None,
) -> dict[str, Any] | None:
    """The wire body for one turn, or None when the message carries no usage.

    ``observed`` carries the turn's per-message ``(role, model)`` usage; when it
    is given every entry carries a ``role``, when omitted entries carry none.
    """

    seen: Observed = observed if observed is not None else {}
    models: list[dict[str, Any]] = []
    model_usage = getattr(message, "model_usage", None)
    if isinstance(model_usage, dict) and model_usage:
        for model, raw in model_usage.items():
            if isinstance(model, str) and model and isinstance(raw, dict):
                models.extend(
                    _split(model, _entry(model, raw, _MODEL_USAGE_KEYS), seen, primary_model)
                )
    else:
        usage = getattr(message, "usage", None)
        if isinstance(usage, dict) and usage and primary_model:
            models.extend(
                _split(
                    primary_model, _entry(primary_model, usage, _USAGE_KEYS), seen, primary_model
                )
            )
    if not models:
        return None
    if observed is None:
        for entry in models:
            entry.pop("role", None)
    return {
        "turn_id": _turn_id(message, models),
        "primary_model": primary_model,
        "models": models,
    }


class UsageReporter:
    """POSTs a turn's usage. The token rides ``X-API-Key`` and is never logged."""

    def __init__(self, url: str, token: str) -> None:
        self._url = url
        self._token = token
        self._observed: dict[tuple[str, str], dict[str, int]] = {}

    def observe(self, message: AssistantMessage) -> None:
        """Accumulate one assistant message's usage under its SDK-given role."""

        usage = getattr(message, "usage", None)
        model = getattr(message, "model", None)
        if not isinstance(model, str) or not model:
            return
        # Keep the role even without per-message counts, so the result's
        # per-model totals for a subagent model are not booked to the implementer.
        if not isinstance(usage, dict):
            usage = {}
        role = REVIEWER if getattr(message, "parent_tool_use_id", None) is not None else IMPLEMENTER
        bucket = self._observed.setdefault((role, model), dict.fromkeys(_WIRE_KEYS, 0))
        for source, wire in _USAGE_KEYS.items():
            bucket[wire] += _count(usage.get(source))

    async def report(self, message: ResultMessage, primary_model: str | None) -> None:
        observed, self._observed = self._observed, {}
        try:
            body = build_usage_body(message, primary_model, observed=observed)
        except Exception as exc:  # never fail the turn over a cost line
            logger.warning("usage report build failure: %s", type(exc).__name__)
            return
        if body is None:
            return
        timeout = aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS)
        status: int | None = None
        for _attempt in range(2):
            try:
                async with (
                    aiohttp.ClientSession(timeout=timeout) as session,
                    session.post(
                        self._url, json=body, headers={"X-API-Key": self._token}
                    ) as response,
                ):
                    status = response.status
            except (aiohttp.ClientError, TimeoutError) as exc:
                logger.warning("usage report transport failure: %s", type(exc).__name__)
                status = None
                continue
            if status < 500:
                break
        if status is None or not 200 <= status < 300:
            logger.warning("usage report not recorded: status=%s", status)
