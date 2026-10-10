"""Per-model token usage for a factory run's cost line (#3223).

At each turn's ``ResultMessage`` boundary the runner POSTs that turn's per-model
token counts to ``<progress_url>/usage`` over the same request-bound progress
token. A failure is logged and never fails the turn.

In streaming input mode the SDK's ``model_usage`` is the running total for the
whole call, not the turn that just finished. The reporter subtracts the previous
total for the same session before it posts, so a later turn does not store the
earlier turn again. See the Agent SDK cost guide:
https://code.claude.com/docs/en/agent-sdk/cost-tracking

A turn that ends without a ``ResultMessage`` (a closed stream, a cancellation,
an iterator error) posts the per-message counts it observed under a fresh
``unfinished:`` turn id, and a later drain of that turn's leftover result only
advances the baseline, so its tokens are not counted again (#4190).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections.abc import Collection, Mapping
from dataclasses import replace
from typing import Any, Protocol, cast
from uuid import uuid4

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

# A trailing context-window token on a model id, such as ``[1m]`` (#3992).
_CONTEXT_WINDOW_SUFFIX = re.compile(r"\[[^\[\]]*\]$")


class UsageSink(Protocol):
    def observe(self, message: AssistantMessage) -> None: ...

    async def report(self, message: ResultMessage, primary_model: str | None) -> None: ...

    async def report_unfinished(self, primary_model: str | None) -> None: ...

    def absorb(self, message: ResultMessage) -> None: ...


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


def _base_model(model: str) -> str:
    return _CONTEXT_WINDOW_SUFFIX.sub("", model) or model


def _usage_key(model: str, keys: Collection[str]) -> str:
    """The ``model_usage`` key an observed model id belongs to.

    The SDK keys ``model_usage`` with the configured id, which can end in a
    context-window token such as ``[1m]``, while ``AssistantMessage.model``
    carries the base id. An id with no exact key pairs with the one key that
    shares its base; with none or several it stays as it is.
    """

    if model in keys:
        return model
    base = _base_model(model)
    candidates = [key for key in keys if _base_model(key) == base]
    return candidates[0] if len(candidates) == 1 else model


def _paired(observed: Observed, keys: Collection[str]) -> dict[tuple[str, str], dict[str, int]]:
    paired: dict[tuple[str, str], dict[str, int]] = {}
    for (role, model), counts in observed.items():
        bucket = paired.setdefault((role, _usage_key(model, keys)), dict.fromkeys(_WIRE_KEYS, 0))
        for key in _WIRE_KEYS:
            bucket[key] += _count(counts.get(key))
    return paired


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

    models: list[dict[str, Any]] = []
    represented: set[str] = set()
    model_usage = getattr(message, "model_usage", None)
    if isinstance(model_usage, dict) and model_usage:
        keys = {model for model in model_usage if isinstance(model, str) and model}
        seen: Observed = _paired(observed or {}, keys)
        primary = _usage_key(primary_model, keys) if primary_model else primary_model
        for model, raw in model_usage.items():
            if isinstance(model, str) and model and isinstance(raw, dict):
                represented.add(model)
                split = _split(model, _entry(model, raw, _MODEL_USAGE_KEYS), seen, primary)
                # A zero cumulative entry means the model gained nothing this turn.
                models.extend(e for e in split if any(e[key] for key in _WIRE_KEYS))
    else:
        seen = observed if observed is not None else {}
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
        # The SDK session of this turn's latest observed message.
        self._observed_session: str | None = None
        # Session-cumulative model_usage already accepted by the API.
        # A new session id, or a drop in any count, means the SDK restarted the total.
        self._cumulative: dict[str, dict[str, int]] = {}
        self._cumulative_session: str | None = None
        # Usage posted before it appears in cumulative totals, per model: the
        # reviewer entries of report bodies, and every observed count of an
        # unfinished body. Queued bodies count too: their replay must retain
        # the same usage.
        self._unmatched_reviewer: dict[str, dict[str, int]] = {}
        # The SDK session an unfinished turn's counts came from.
        self._unmatched_session: str | None = None
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
        session_id = getattr(message, "session_id", None)
        if isinstance(session_id, str) and session_id:
            self._observed_session = session_id
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
            # Unfinished counts tagged with this SDK session carry onto any restart
            # of it: a reset that came before them would otherwise post them twice.
            carried = session_id is not None and session_id == self._unmatched_session
            if not carried:
                self._unmatched_reviewer.clear()
                self._unmatched_session = None
            turn = {model: dict(counts) for model, counts in parsed.items()}
            baseline = {model: dict(counts) for model, counts in parsed.items()}
        else:
            turn = _turn_counts(previous, parsed)
            baseline = {model: dict(counts) for model, counts in {**previous, **parsed}.items()}
        for reviewed, unmatched in list(self._unmatched_reviewer.items()):
            model = _usage_key(reviewed, turn)
            counts = turn.get(model)
            if counts is None:
                continue
            for key in _WIRE_KEYS:
                caught_up = min(counts[key], unmatched[key])
                counts[key] -= caught_up
                unmatched[key] -= caught_up
            if not any(counts.values()):
                del turn[model]
            if not any(unmatched.values()):
                del self._unmatched_reviewer[reviewed]
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
        self._observed_session = None
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
        except Exception as exc:  # noqa: BLE001 - never fail the turn over a cost line
            logger.warning("usage report build failure: %s", type(exc).__name__)
            return
        await self._flush()

    async def report_unfinished(self, primary_model: str | None) -> None:
        """Post the observed per-message counts of a turn that has no result.

        Called for every turn ending; a turn that reached its result already
        emptied the observations in ``report``, so this posts nothing more.
        The body carries a fresh turn id and does not move the baseline.
        """

        observed, self._observed = self._observed, {}
        self._seen_message_ids.clear()
        observed_session, self._observed_session = self._observed_session, None
        try:
            models: list[dict[str, Any]] = []
            for (role, model), counts in observed.items():
                entry: dict[str, Any] = {"model": model, "role": role}
                entry.update({key: _count(counts.get(key)) for key in _WIRE_KEYS})
                if any(entry[key] for key in _WIRE_KEYS):
                    models.append(entry)
            if not models:
                return
            # Every posted count, whatever its role, reaches model_usage later, in
            # the drained result or a following turn; catch up by model total there.
            owner = self._unmatched_session or (
                self._speculative[0] if self._speculative else self._cumulative_session
            )
            # Counts left from another SDK session can never catch up in this one.
            if (
                self._unmatched_reviewer
                and observed_session is not None
                and owner is not None
                and observed_session != owner
            ):
                self._unmatched_reviewer.clear()
            self._unmatched_session = observed_session
            for entry in models:
                unmatched = self._unmatched_reviewer.setdefault(
                    entry["model"], dict.fromkeys(_WIRE_KEYS, 0)
                )
                for key in _WIRE_KEYS:
                    unmatched[key] += entry[key]
            body: dict[str, Any] = {
                "turn_id": f"unfinished:{uuid4().hex}",
                "primary_model": primary_model,
                "models": models,
            }
            session_id, baseline = self._speculative or (
                self._cumulative_session,
                self._cumulative,
            )
            copied = {model: dict(counts) for model, counts in baseline.items()}
            self._queue.append((body, session_id, copied))
        except Exception as exc:  # noqa: BLE001 - never fail the turn over a cost line
            logger.warning("usage report build failure: %s", type(exc).__name__)
            return
        await self._flush()

    def absorb(self, message: ResultMessage) -> None:
        """Advance the baseline from a result that is not reported.

        An abandoned turn's leftover result is drained by the next turn. Its
        tokens were already posted as an unfinished body, so its cumulative
        ``model_usage`` only moves the baseline the next turn subtracts from.
        """

        self._observed = {}
        self._seen_message_ids.clear()
        self._observed_session = None
        try:
            model_usage = getattr(message, "model_usage", None)
            parsed: dict[str, dict[str, int]] = {}
            if isinstance(model_usage, dict) and model_usage:
                parsed = _parse_model_usage(model_usage)
            if not parsed:
                return
            previous_session, previous = self._speculative or (
                self._cumulative_session,
                self._cumulative,
            )
            _, session_id, baseline = self._isolate_turn(
                message, parsed, previous, previous_session
            )
            self._remember(session_id, baseline)
        except Exception as exc:  # noqa: BLE001 - never fail the turn over a cost line
            logger.warning("usage absorb failure: %s", type(exc).__name__)
