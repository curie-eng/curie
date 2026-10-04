"""Per-model token usage for a factory run's cost line (#3223).

At each turn's ``ResultMessage`` boundary the runner POSTs that turn's per-model
token counts to ``<progress_url>/usage`` over the same request-bound progress
token. A failure is logged and never fails the turn.

In streaming input mode the SDK's ``model_usage`` is the running total for the
whole call, not the turn that just finished. The reporter subtracts the previous
total for the same session before it posts, so a later turn does not store the
earlier turn again. See the Agent SDK cost guide:
https://code.claude.com/docs/en/agent-sdk/cost-tracking
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Protocol, cast

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


def _parse_model_usage(model_usage: dict[Any, Any]) -> dict[str, dict[str, int]]:
    parsed: dict[str, dict[str, int]] = {}
    for model, raw in model_usage.items():
        if isinstance(model, str) and model and isinstance(raw, dict):
            parsed[model] = {
                wire: _count(raw.get(source)) for source, wire in _MODEL_USAGE_KEYS.items()
            }
    return parsed


def _wire_to_model_usage(counts: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
    source_for = {wire: source for source, wire in _MODEL_USAGE_KEYS.items()}
    return {
        model: {source_for[wire]: value for wire, value in per_model.items()}
        for model, per_model in counts.items()
    }


def _cumulative_decreased(
    previous: Mapping[str, Mapping[str, int]],
    parsed: Mapping[str, Mapping[str, int]],
) -> bool:
    for model, counts in parsed.items():
        prior = previous.get(model)
        if prior is not None and any(counts[key] < prior[key] for key in _WIRE_KEYS):
            return True
    return False


def _turn_counts(
    previous: Mapping[str, Mapping[str, int]],
    parsed: Mapping[str, Mapping[str, int]],
) -> dict[str, dict[str, int]]:
    delta: dict[str, dict[str, int]] = {}
    for model, counts in parsed.items():
        prior = previous.get(model, {})
        turned = {key: counts[key] - prior.get(key, 0) for key in _WIRE_KEYS}
        if any(value > 0 for value in turned.values()):
            delta[model] = turned
    return delta


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
    represented: set[str] = set()
    model_usage = getattr(message, "model_usage", None)
    if isinstance(model_usage, dict) and model_usage:
        for model, raw in model_usage.items():
            if isinstance(model, str) and model and isinstance(raw, dict):
                represented.add(model)
                split = _split(model, _entry(model, raw, _MODEL_USAGE_KEYS), seen, primary_model)
                # A zero cumulative entry means the model gained nothing this turn.
                models.extend(e for e in split if any(e[key] for key in _WIRE_KEYS))
    else:
        usage = getattr(message, "usage", None)
        if isinstance(usage, dict) and usage and primary_model:
            represented.add(primary_model)
            models.extend(
                _split(
                    primary_model, _entry(primary_model, usage, _USAGE_KEYS), seen, primary_model
                )
            )
    for (role, model), counts in seen.items():
        if role == REVIEWER and model and model not in represented:
            entry: dict[str, Any] = {"model": model, "role": REVIEWER}
            entry.update({key: _count(counts.get(key)) for key in _WIRE_KEYS})
            if any(entry[key] for key in _WIRE_KEYS):
                models.append(entry)
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
        self._seen_message_ids: set[tuple[str, str, str]] = set()
        # Session-cumulative model_usage already accepted by the API.
        # A new session id, or a drop in any count, means the SDK restarted the total.
        self._cumulative: dict[str, dict[str, int]] = {}
        self._cumulative_session: str | None = None
        # Reviewer observations reported before they appear in cumulative totals.
        # Queued bodies count too: their replay must retain the same usage.
        self._unmatched_reviewer: dict[str, dict[str, int]] = {}
        # Bodies whose POST has not been accepted, each with the baseline that
        # becomes current once that body is accepted. Replay keeps the original
        # turn id and roles. record_usage treats a replayed turn as a no-op.
        self._queue: list[tuple[dict[str, Any], str | None, dict[str, dict[str, int]]]] = []
        self._speculative: tuple[str | None, dict[str, dict[str, int]]] | None = None

    def observe(self, message: AssistantMessage) -> None:
        """Accumulate one assistant message's usage under its SDK-given role.

        The Claude Agent SDK can yield several ``AssistantMessage`` objects for
        one API response, each repeating ``message_id`` and ``usage`` (one per
        content block when the model emits parallel tools). Count each
        ``message_id`` once per ``(role, model)``. Messages with no id still
        accumulate, because there is nothing to deduplicate.
        """

        usage = getattr(message, "usage", None)
        model = getattr(message, "model", None)
        if not isinstance(model, str) or not model:
            return
        # Keep the role even without per-message counts, so the result's
        # per-model totals for a subagent model are not booked to the implementer.
        if not isinstance(usage, dict):
            usage = {}
        role = REVIEWER if getattr(message, "parent_tool_use_id", None) is not None else IMPLEMENTER
        message_id = getattr(message, "message_id", None)
        if isinstance(message_id, str) and message_id:
            seen = (role, model, message_id)
            if seen in self._seen_message_ids:
                return
            self._seen_message_ids.add(seen)
        bucket = self._observed.setdefault((role, model), dict.fromkeys(_WIRE_KEYS, 0))
        for source, wire in _USAGE_KEYS.items():
            bucket[wire] += _count(usage.get(source))

    def _isolate_turn(
        self,
        message: ResultMessage,
        parsed: dict[str, dict[str, int]],
        previous: Mapping[str, Mapping[str, int]],
        previous_session: str | None,
    ) -> tuple[ResultMessage | None, str | None, dict[str, dict[str, int]]]:
        """Return this turn's model_usage and the baseline after this snapshot.

        ``model_usage`` on a streaming-input result is the call's running total.
        The same session keeps that total until ``/clear``, a new session, or a
        zeroed crash result restarts it. The top-level ``usage`` field is already
        this turn only, so this adjustment applies only when ``model_usage`` is set.
        """

        session_id = getattr(message, "session_id", None)
        session_id = session_id if isinstance(session_id, str) else None
        restarted = session_id != previous_session or _cumulative_decreased(previous, parsed)
        if restarted:
            self._unmatched_reviewer.clear()
            turn = {model: dict(counts) for model, counts in parsed.items()}
            baseline = {model: dict(counts) for model, counts in parsed.items()}
        else:
            turn = _turn_counts(previous, parsed)
            baseline = {model: dict(counts) for model, counts in {**previous, **parsed}.items()}
        for model, counts in list(turn.items()):
            unmatched = self._unmatched_reviewer.get(model)
            if unmatched is None:
                continue
            for key in _WIRE_KEYS:
                caught_up = min(counts[key], unmatched[key])
                counts[key] -= caught_up
                unmatched[key] -= caught_up
            if not any(counts.values()):
                del turn[model]
            if not any(unmatched.values()):
                del self._unmatched_reviewer[model]
        if not turn:
            return None, session_id, baseline
        isolated = replace(message, model_usage=cast(Any, _wire_to_model_usage(turn)), usage={})
        return isolated, session_id, baseline

    def _remember(self, session_id: str | None, baseline: dict[str, dict[str, int]]) -> None:
        self._speculative = (session_id, baseline)
        if not self._queue:
            self._cumulative_session = session_id
            self._cumulative = baseline

    async def _post(self, body: dict[str, Any]) -> bool:
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
        if status is not None and 200 <= status < 300:
            return True
        logger.warning("usage report not recorded: status=%s", status)
        return False

    async def _flush(self) -> None:
        """POST queued turn bodies in order. Stop at the first one still unaccepted."""

        while self._queue:
            body, session_id, baseline = self._queue[0]
            if not await self._post(body):
                return
            self._queue.pop(0)
            self._cumulative_session = session_id
            self._cumulative = baseline
            if not self._queue:
                # A newer snapshot may only have caught up observed usage and
                # produced no body. Do not rewind it when an older replay lands.
                self._cumulative_session, self._cumulative = self._speculative or (
                    session_id,
                    baseline,
                )
                self._speculative = (self._cumulative_session, self._cumulative)

    async def report(self, message: ResultMessage, primary_model: str | None) -> None:
        observed, self._observed = self._observed, {}
        self._seen_message_ids.clear()
        try:
            model_usage = getattr(message, "model_usage", None)
            parsed: dict[str, dict[str, int]] = {}
            if isinstance(model_usage, dict) and model_usage:
                parsed = _parse_model_usage(model_usage)
            if parsed:
                previous_session, previous = self._speculative or (
                    self._cumulative_session,
                    self._cumulative,
                )
                isolated, session_id, baseline = self._isolate_turn(
                    message, parsed, previous, previous_session
                )
                if isolated is None:
                    isolated = replace(message, model_usage={}, usage={})
                body = build_usage_body(isolated, primary_model, observed=observed)
                if body is not None:
                    included = _parse_model_usage(isolated.model_usage or {})
                    for entry in body["models"]:
                        model = entry["model"]
                        if entry["role"] == REVIEWER and model not in included:
                            unmatched = self._unmatched_reviewer.setdefault(
                                model, dict.fromkeys(_WIRE_KEYS, 0)
                            )
                            for key in _WIRE_KEYS:
                                unmatched[key] += entry[key]
                    self._queue.append((body, session_id, baseline))
                self._remember(session_id, baseline)
            else:
                body = build_usage_body(message, primary_model, observed=observed)
                if body is not None:
                    # Per-turn ``usage`` is not a running total, so it does not move
                    # the model_usage baseline. Carry the baseline the queue already
                    # implies, or a flush would rewind an accepted snapshot.
                    session_id, baseline = self._speculative or (
                        self._cumulative_session,
                        self._cumulative,
                    )
                    copied = {model: dict(counts) for model, counts in baseline.items()}
                    self._queue.append((body, session_id, copied))
        except Exception as exc:  # never fail the turn over a cost line
            logger.warning("usage report build failure: %s", type(exc).__name__)
            return
        await self._flush()
