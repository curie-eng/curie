"""Durable, harness-neutral structured conversation history.

ADR-0119 replaces the legacy rendered boot-prompt transcript with ordered
role/content messages. The durable record is provider-neutral: user and assistant
roles, opaque JSON content blocks (including tool calls/results), terminal and
approval context, plus an explicit stable summary record at compaction boundaries.
The Claude adapter materializes these records into its provider-native resume
envelope at boot. It may also persist opaque checkpoint/delta entries as a
matching-harness cache optimization; those entries are never authoritative and
another harness reconstructs from the portable messages alone.

Design (ADR-0029):

- **History lives outside the sandbox** (ADR-0003, stateless-first). An unplanned
  runner-pod restart is a new pod with empty scratch, so a restarted thread must
  rehydrate from an external, durable resource reached over the network at boot.
- **The backing reuses the durable state store** landed for #23/#248 and #264
  (``apps/api`` ``/agents/{agent_id}/state/{namespace}/{key}``, Postgres JSONB),
  rather than inventing a new datastore. The thread's transcript is the
  log-shaped key ``.../state/transcript/<thread_key>``: the ``append`` endpoint
  gives the append-only write and ``get`` gives load.
- **Harness-agnostic storage.** A harness must consume the structured prefix or
  explicitly declare the capability absent. Rendering this data into a system
  prompt is not a supported fallback.

``CURIE_HISTORY_REF`` resolution: the ref is the URL of the thread's transcript
key on the state API (e.g. ``http://api:8000/agents/<id>/state/transcript/<thread>``).
The runner authenticates with ``CURIE_HISTORY_TOKEN`` (a runner-local knob, like
``CURIE_MEMORY_TOKEN`` -- NOT part of the frozen ACI ``SessionConfig``, so no
frozen-contract change). An ``s3://`` or other scheme is reserved for a future
loader and rejected loudly today.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Protocol, cast, runtime_checkable

import aiohttp
from aci_protocol import BootEnv

logger = logging.getLogger(__name__)

# Runner-local env carrying the bearer the state API expects (X-API-Key). Not a
# model credential and not part of the frozen ACI SessionConfig -- resolved the
# same way as CURIE_MEMORY_TOKEN and the other runner-local knobs. The worker
# declares and renders it, so the name is read from that one declaration (#488).
HISTORY_TOKEN_ENV = BootEnv.env_key("history_token")


class HistoryError(RuntimeError):
    """A history reference could not be resolved or dereferenced."""


class StructuredReplayUnsupported(HistoryError):
    """The selected harness cannot consume a recovered structured prefix."""


JsonContent = str | list[dict[str, Any]]

# The state API caps one JSON value at 64 KiB. Transcript appends store a JSON
# array, so a turn must fit with that array's brackets rather than merely fit as
# a standalone object. Accumulated-log compaction is a separate concern: this
# bound prevents a single large first turn from being rejected before any
# durable conversation exists.
HISTORY_VALUE_MAX_BYTES = 65_536

_STRUCTURAL_BLOCK_FIELDS = {
    "type",
    "id",
    "tool_use_id",
    "name",
    "is_error",
}


def _json_copy(value: JsonContent) -> JsonContent:
    """Return a detached JSON-safe copy of message content."""

    return cast("JsonContent", json.loads(json.dumps(value)))


@dataclass(frozen=True)
class ConversationMessage:
    """One portable prior message, preserving its role and content blocks."""

    role: str
    content: JsonContent

    def __post_init__(self) -> None:
        if self.role not in ("user", "assistant"):
            raise HistoryError(f"unsupported conversation role: {self.role!r}")
        if not isinstance(self.content, (str, list)):
            raise HistoryError("conversation message content must be a string or block list")
        if isinstance(self.content, list) and not all(
            isinstance(block, Mapping) for block in self.content
        ):
            raise HistoryError("conversation content blocks must be JSON objects")

    def to_dict(self) -> dict[str, Any]:
        return {"role": self.role, "content": _json_copy(self.content)}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ConversationMessage:
        role = data.get("role")
        content = data.get("content")
        if not isinstance(role, str) or not isinstance(content, (str, list)):
            raise HistoryError("invalid structured conversation message")
        blocks: JsonContent
        if isinstance(content, str):
            blocks = content
        elif all(isinstance(block, Mapping) for block in content):
            blocks = [dict(block) for block in content]
        else:
            raise HistoryError("conversation content blocks must be JSON objects")
        return cls(role=role, content=blocks)


@dataclass(frozen=True)
class ApprovalContext:
    """Durable approval/suspend context carried with the turn that paused."""

    summary: str | None = None
    route: str | None = None
    gate_kind: str | None = None
    granted_tool: str | None = None
    decision: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "summary": self.summary,
            "route": self.route,
            "gate_kind": self.gate_kind,
            "granted_tool": self.granted_tool,
            "decision": self.decision,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ApprovalContext:
        def optional_string(name: str) -> str | None:
            value = data.get(name)
            return value if isinstance(value, str) else None

        return cls(
            summary=optional_string("summary"),
            route=optional_string("route"),
            gate_kind=optional_string("gate_kind"),
            granted_tool=optional_string("granted_tool"),
            decision=optional_string("decision"),
        )


@dataclass(frozen=True)
class HarnessReplayState:
    """Optional opaque harness-native checkpoint or append delta.

    Portable role/content messages remain authoritative. This state is an
    optimization a matching harness may consume to restore provider-native
    request shape (and therefore its message cache); every other harness ignores
    it and replays the portable messages.
    """

    harness: str
    kind: str
    entries: tuple[dict[str, Any], ...]

    def __post_init__(self) -> None:
        if not self.harness:
            raise HistoryError("harness replay state requires a harness name")
        if self.kind not in ("checkpoint", "delta"):
            raise HistoryError(f"invalid harness replay state kind: {self.kind!r}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "harness": self.harness,
            "kind": self.kind,
            "entries": json.loads(json.dumps(self.entries)),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> HarnessReplayState:
        harness = data.get("harness")
        kind = data.get("kind")
        entries = data.get("entries")
        if (
            not isinstance(harness, str)
            or not isinstance(kind, str)
            or not isinstance(entries, list)
            or not all(isinstance(entry, Mapping) for entry in entries)
        ):
            raise HistoryError("invalid harness replay state")
        return cls(
            harness=harness,
            kind=kind,
            entries=tuple(dict(entry) for entry in entries),
        )


@dataclass(frozen=True)
class TurnRecord:
    """One durable turn with a legacy projection and structured messages.

    ``ts`` is set at append time (RFC3339 UTC) so a reloaded transcript keeps its
    order and a turn is timestamped for debugging. The pair is the minimal
    harness-agnostic unit: any harness can render it as prior context.
    """

    user: str
    assistant: str
    ts: str = ""
    messages: tuple[ConversationMessage, ...] = ()
    status: str = "done"
    approval: ApprovalContext | None = None
    harness_replay: HarnessReplayState | None = None

    def __post_init__(self) -> None:
        if not self.messages:
            object.__setattr__(
                self,
                "messages",
                (
                    ConversationMessage(role="user", content=self.user),
                    ConversationMessage(
                        role="assistant",
                        content=[{"type": "text", "text": self.assistant}],
                    ),
                ),
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "turn",
            "user": self.user,
            "assistant": self.assistant,
            "ts": self.ts,
            "messages": [message.to_dict() for message in self.messages],
            "status": self.status,
            "approval": self.approval.to_dict() if self.approval is not None else None,
            "harness_replay": (
                self.harness_replay.to_dict() if self.harness_replay is not None else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TurnRecord:
        raw_messages = data.get("messages")
        messages: tuple[ConversationMessage, ...] = ()
        if raw_messages is not None:
            if not isinstance(raw_messages, list) or not all(
                isinstance(item, Mapping) for item in raw_messages
            ):
                raise HistoryError("invalid structured conversation messages")
            messages = tuple(
                ConversationMessage.from_dict(item)
                for item in raw_messages
            )
        raw_approval = data.get("approval")
        raw_harness_replay = data.get("harness_replay")
        return cls(
            user=str(data.get("user", "")),
            assistant=str(data.get("assistant", "")),
            ts=str(data.get("ts", "")),
            messages=messages,
            status=str(data.get("status", "done")),
            approval=(
                ApprovalContext.from_dict(raw_approval)
                if isinstance(raw_approval, Mapping)
                else None
            ),
            harness_replay=(
                HarnessReplayState.from_dict(raw_harness_replay)
                if isinstance(raw_harness_replay, Mapping)
                else None
            ),
        )


def _state_value_size(record: Mapping[str, Any]) -> int:
    """Return the state API's encoded size for a one-record transcript."""

    return len(json.dumps([record], separators=(",", ":")).encode("utf-8"))


def _digest_marker(value: str) -> str:
    encoded = value.encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return (
        "[history payload omitted; "
        f"sha256={digest}; original_bytes={len(encoded)}]"
    )


def _collect_text_payloads(
    value: Any,
    *,
    path: tuple[str | int, ...],
    priority: int,
    candidates: dict[str, tuple[int, list[tuple[str | int, ...]]]],
) -> None:
    """Collect replaceable string leaves without touching structural fields."""

    if isinstance(value, str):
        existing = candidates.get(value)
        if existing is None:
            candidates[value] = (priority, [path])
        else:
            existing_priority, paths = existing
            paths.append(path)
            candidates[value] = (min(existing_priority, priority), paths)
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _collect_text_payloads(
                item,
                path=(*path, index),
                priority=priority,
                candidates=candidates,
            )
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            _collect_text_payloads(
                item,
                path=(*path, str(key)),
                priority=priority,
                candidates=candidates,
            )


def _turn_text_payloads(
    record: dict[str, Any],
) -> dict[str, tuple[int, list[tuple[str | int, ...]]]]:
    """Index portable text by reduction priority and stable object path."""

    candidates: dict[str, tuple[int, list[tuple[str | int, ...]]]] = {}

    # The legacy pair mirrors the structured messages. Grouping by value means
    # both projections receive the same marker if either copy must be bounded.
    for field in ("user", "assistant"):
        value = record.get(field)
        if isinstance(value, str):
            _collect_text_payloads(
                value,
                path=(field,),
                priority=3,
                candidates=candidates,
            )

    approval = record.get("approval")
    if isinstance(approval, Mapping) and isinstance(approval.get("summary"), str):
        _collect_text_payloads(
            approval["summary"],
            path=("approval", "summary"),
            priority=2,
            candidates=candidates,
        )

    messages = record.get("messages")
    if not isinstance(messages, list):
        return candidates
    for message_index, message in enumerate(messages):
        if not isinstance(message, Mapping):
            continue
        content = message.get("content")
        content_path: tuple[str | int, ...] = (
            "messages",
            message_index,
            "content",
        )
        if isinstance(content, str):
            _collect_text_payloads(
                content,
                path=content_path,
                priority=3,
                candidates=candidates,
            )
            continue
        if not isinstance(content, list):
            continue
        for block_index, block in enumerate(content):
            if not isinstance(block, Mapping):
                continue
            block_path = (*content_path, block_index)
            block_type = block.get("type")
            if block_type == "tool_result" and "content" in block:
                _collect_text_payloads(
                    block["content"],
                    path=(*block_path, "content"),
                    priority=0,
                    candidates=candidates,
                )
            elif block_type == "text" and "text" in block:
                _collect_text_payloads(
                    block["text"],
                    path=(*block_path, "text"),
                    priority=1,
                    candidates=candidates,
                )

            for key, item in block.items():
                if key in _STRUCTURAL_BLOCK_FIELDS:
                    continue
                if block_type == "tool_result" and key == "content":
                    continue
                if block_type == "text" and key == "text":
                    continue
                _collect_text_payloads(
                    item,
                    path=(*block_path, str(key)),
                    priority=2,
                    candidates=candidates,
                )
    return candidates


def _replace_path(
    root: dict[str, Any], path: tuple[str | int, ...], replacement: str
) -> None:
    target: Any = root
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = replacement


def bound_turn_record(
    record: TurnRecord, *, max_value_bytes: int = HISTORY_VALUE_MAX_BYTES
) -> TurnRecord:
    """Bound one turn for a whole state value without losing message order.

    Native harness replay is discarded first because portable messages are the
    authority across sandbox replacement. If that is insufficient, textual
    payloads are replaced with deterministic digest and byte-count markers:
    tool results first, then assistant text and other content, while role,
    message, and block order remain intact. If that irreducible structure alone
    exceeds the cap, fail before the transcript store sees an append.
    """

    if max_value_bytes <= 0:
        raise HistoryError("history value byte cap must be positive")

    raw = record.to_dict()
    if _state_value_size(raw) <= max_value_bytes:
        return record

    raw["harness_replay"] = None
    if _state_value_size(raw) <= max_value_bytes:
        return TurnRecord.from_dict(raw)

    candidates = _turn_text_payloads(raw)
    ordered: list[tuple[int, int, str, str, list[tuple[str | int, ...]]]] = []
    for original, (priority, paths) in candidates.items():
        marker = _digest_marker(original)
        original_size = len(json.dumps(original).encode("utf-8"))
        marker_size = len(json.dumps(marker).encode("utf-8"))
        savings = (original_size - marker_size) * len(paths)
        if savings > 0:
            ordered.append(
                (
                    priority,
                    -savings,
                    hashlib.sha256(original.encode("utf-8")).hexdigest(),
                    marker,
                    paths,
                )
            )

    for _priority, _negative_savings, _digest, marker, paths in sorted(ordered):
        for path in paths:
            _replace_path(raw, path, marker)
        if _state_value_size(raw) <= max_value_bytes:
            return TurnRecord.from_dict(raw)

    irreducible_size = _state_value_size(raw)
    raise HistoryError(
        "history turn cannot fit without dropping portable message roles or order: "
        f"minimum value is {irreducible_size} bytes, cap is {max_value_bytes} bytes"
    )


@dataclass(frozen=True)
class SummaryRecord:
    """A stable compaction boundary plus the un-compacted structured tail."""

    content: str
    digest: str
    source_turns: int
    through_ts: str
    tail: tuple[TurnRecord, ...] = ()
    ts: str = ""

    @property
    def messages(self) -> tuple[ConversationMessage, ...]:
        prefix = (
            ConversationMessage(
                role="user",
                content=(
                    "# Durable conversation summary\n\n"
                    "This summary was persisted at an explicit compaction boundary.\n\n"
                    f"{self.content}"
                ),
            ),
            ConversationMessage(
                role="assistant",
                content=[
                    {
                        "type": "text",
                        "text": "I will preserve this stable summary as prior context.",
                    }
                ],
            ),
        )
        return prefix + tuple(message for turn in self.tail for message in turn.messages)

    def to_dict(self) -> dict[str, Any]:
        return {
            "type": "summary",
            "content": self.content,
            "digest": self.digest,
            "source_turns": self.source_turns,
            "through_ts": self.through_ts,
            "tail": [turn.to_dict() for turn in self.tail],
            "ts": self.ts,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SummaryRecord:
        raw_tail = data.get("tail")
        if raw_tail is not None and (
            not isinstance(raw_tail, list)
            or not all(isinstance(item, Mapping) for item in raw_tail)
        ):
            raise HistoryError("invalid structured summary tail")
        tail = (
            tuple(TurnRecord.from_dict(item) for item in raw_tail)
            if isinstance(raw_tail, list)
            else ()
        )
        return cls(
            content=str(data.get("content", "")),
            digest=str(data.get("digest", "")),
            source_turns=int(data.get("source_turns", 0)),
            through_ts=str(data.get("through_ts", "")),
            tail=tail,
            ts=str(data.get("ts", "")),
        )


HistoryRecord = TurnRecord | SummaryRecord


@dataclass(frozen=True)
class ConversationReplay:
    """The exact portable prefix a fresh harness must reconstruct."""

    messages: tuple[ConversationMessage, ...] = ()
    source_turns: int = 0
    summary_digest: str | None = None
    harness_replay: HarnessReplayState | None = None

    @property
    def present(self) -> bool:
        return bool(self.messages)


def close_suspended_tool_calls(
    messages: Sequence[ConversationMessage],
) -> tuple[ConversationMessage, ...]:
    """Close dangling permission-gated tool calls with a denial result.

    An interrupting permission denial can end the harness iterator immediately
    after its ``tool_use`` message. Provider APIs require every prior tool call
    to have a following ``tool_result`` before another user message can be
    submitted. Persist an explicit, truthful negative result for only the calls
    that remain unmatched; a result already supplied by the harness is preserved
    byte-for-byte and this operation is idempotent.
    """

    pending: dict[str, None] = {}
    for message in messages:
        if not isinstance(message.content, list):
            continue
        for block in message.content:
            block_type = block.get("type")
            if message.role == "assistant" and block_type == "tool_use":
                tool_use_id = block.get("id")
                if isinstance(tool_use_id, str):
                    pending[tool_use_id] = None
            elif message.role == "user" and block_type == "tool_result":
                tool_use_id = block.get("tool_use_id")
                if isinstance(tool_use_id, str):
                    pending.pop(tool_use_id, None)
    if not pending:
        return tuple(messages)
    return (
        *messages,
        ConversationMessage(
            role="user",
            content=[
                {
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": "Tool call was not executed; awaiting human approval.",
                    "is_error": True,
                }
                for tool_use_id in pending
            ],
        ),
    )


@runtime_checkable
class TranscriptStore(Protocol):
    """The history port: load prior turns, append the latest one.

    Deliberately narrow -- no query language and no in-place rewrite. A concrete
    store dereferences a ``history_ref`` to a durable, rehydratable backing that
    lives outside the sandbox; compaction is itself an append-only summary.
    """

    async def load(self) -> list[HistoryRecord]:
        """Return prior turns, oldest first (empty when none)."""
        ...

    async def append(self, record: HistoryRecord) -> None:
        """Durably append one turn; it must survive an unplanned restart."""
        ...


class NullTranscriptStore:
    """The no-history store used when ``CURIE_HISTORY_REF`` is unset.

    ``load`` yields nothing and ``append`` is a silent no-op, so the boot and
    per-turn paths are uniform whether or not a thread has a transcript ref.
    """

    async def load(self) -> list[HistoryRecord]:
        return []

    async def append(self, record: HistoryRecord) -> None:  # noqa: ARG002 - null sink
        return None


class StateApiTranscriptStore:
    """Transcript backed by the durable state store (#23/#248/#264), the default.

    ``history_ref`` is the URL of the thread's transcript key on the state API
    (``.../agents/<id>/state/transcript/<thread_key>``). Load is a GET of that
    key; append is a POST to the key's ``/append`` endpoint. The state API
    enforces the size caps and the Postgres JSONB backing gives durability across
    an unplanned restart for free.
    """

    def __init__(self, key_url: str, token: str | None) -> None:
        # Normalize to no trailing slash so the /append URL composes cleanly.
        self._key_url = key_url.rstrip("/")
        self._token = token

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self._token} if self._token else {}

    async def load(self) -> list[HistoryRecord]:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(self._key_url, headers=self._headers()) as resp:
                if resp.status == 404:
                    # No transcript written yet -- a fresh thread, not an error.
                    return []
                if resp.status != 200:
                    body = await resp.text()
                    raise HistoryError(f"history load failed: {resp.status} {body[:200]}")
                payload = await resp.json()
        value = payload.get("value")
        if not isinstance(value, list):
            raise HistoryError("transcript log is not a JSON array")
        records: list[HistoryRecord] = []
        for item in value:
            if not isinstance(item, Mapping):
                raise HistoryError("invalid transcript record: expected a JSON object")
            if item.get("type") == "summary":
                records.append(SummaryRecord.from_dict(item))
            elif "user" in item:
                records.append(TurnRecord.from_dict(item))
            else:
                raise HistoryError("invalid transcript record: unknown record shape")
        return records

    async def append(self, record: HistoryRecord) -> None:
        timeout = aiohttp.ClientTimeout(total=15)
        body = json.dumps({"item": record.to_dict()})
        headers = {**self._headers(), "Content-Type": "application/json"}
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                f"{self._key_url}/append", data=body, headers=headers
            ) as resp:
                if resp.status not in (200, 201):
                    text = await resp.text()
                    raise HistoryError(f"history append failed: {resp.status} {text[:200]}")


def resolve_history(history_ref: str | None, env: Mapping[str, str]) -> TranscriptStore:
    """Resolve ``CURIE_HISTORY_REF`` to a concrete ``TranscriptStore`` at boot.

    An absent ref yields the ``NullTranscriptStore`` (history is optional). An
    ``http(s)://`` ref is the state-API transcript-key URL and yields the default
    ``StateApiTranscriptStore``. Any other scheme (an old SDK ``resume`` id,
    ``s3://``, ...) is reserved for a future loader and rejected loudly rather
    than silently dropped, so a misconfigured ref fails visibly at boot.
    """

    if not history_ref:
        return NullTranscriptStore()
    if history_ref.startswith(("http://", "https://")):
        return StateApiTranscriptStore(history_ref, env.get(HISTORY_TOKEN_ENV))
    raise HistoryError(
        f"unsupported CURIE_HISTORY_REF scheme: {history_ref!r} "
        "(only http(s):// state-API refs are implemented today)"
    )


def _replay_bytes(messages: Sequence[ConversationMessage]) -> int:
    return len(
        json.dumps(
            [message.to_dict() for message in messages],
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    )


def _fold_harness_replay(turns: Sequence[TurnRecord]) -> HarnessReplayState | None:
    """Fold the latest checkpoint and its following deltas into one checkpoint."""

    harness: str | None = None
    entries: list[dict[str, Any]] = []
    checkpoint_seen = False
    for turn in turns:
        state = turn.harness_replay
        if state is None:
            continue
        if state.kind == "checkpoint":
            harness = state.harness
            entries = [json.loads(json.dumps(entry)) for entry in state.entries]
            checkpoint_seen = True
        elif checkpoint_seen and state.harness == harness:
            entries.extend(json.loads(json.dumps(entry)) for entry in state.entries)
    if not checkpoint_seen or harness is None:
        return None
    return HarnessReplayState(harness=harness, kind="checkpoint", entries=tuple(entries))


def _turn_summary_line(turn: TurnRecord) -> str:
    tool_names: list[str] = []
    tool_results: list[str] = []
    for message in turn.messages:
        if not isinstance(message.content, list):
            continue
        for block in message.content:
            if block.get("type") == "tool_use":
                tool_names.append(str(block.get("name") or "unknown"))
            elif block.get("type") == "tool_result":
                result = str(block.get("content") or "")
                tool_results.append(result[:300])
    parts = [f"User: {turn.user}", f"Assistant: {turn.assistant}"]
    if tool_names:
        parts.append(f"Tools: {', '.join(tool_names)}")
    if tool_results:
        parts.append(f"Tool results: {' | '.join(tool_results)}")
    if turn.approval is not None:
        approval = turn.approval
        parts.append(
            "Approval: "
            f"status={turn.status} kind={approval.gate_kind or '-'} "
            f"route={approval.route or '-'} decision={approval.decision or '-'} "
            f"summary={approval.summary or '-'}"
        )
    return "\n".join(parts)


def _make_summary(
    prior: SummaryRecord | None,
    compacted: Sequence[TurnRecord],
    tail: Sequence[TurnRecord],
    *,
    max_bytes: int | None,
) -> SummaryRecord:
    portable_turns: list[dict[str, Any]] = []
    for turn in compacted:
        portable = turn.to_dict()
        portable.pop("harness_replay", None)
        portable_turns.append(portable)
    source = {
        "prior_digest": prior.digest if prior is not None else None,
        "prior_content": prior.content if prior is not None else None,
        # Provider-native checkpoint metadata (timestamps, UUIDs, working dirs)
        # is deliberately excluded: a portable summary boundary must not change
        # because the matching harness encoded the same messages differently.
        "turns": portable_turns,
    }
    canonical = json.dumps(source, separators=(",", ":"), sort_keys=True)
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    sections: list[str] = []
    if prior is not None:
        sections.append(prior.content)
    sections.extend(_turn_summary_line(turn) for turn in compacted)
    content = "\n\n".join(sections)
    # The summary is written once at this explicit boundary. Bounding it here is
    # stable: later appends never rewrite it; only a later compaction creates a
    # new record. The digest keeps omitted material falsifiable.
    budget = max(512, (max_bytes // 2) if max_bytes is not None else 8_000)
    encoded = content.encode("utf-8")
    if len(encoded) > budget:
        prefix = encoded[: max(0, budget - 96)].decode("utf-8", errors="ignore")
        content = f"{prefix}\n\n[older detail summarized; digest={digest}]"
    source_turns = (prior.source_turns if prior is not None else 0) + len(compacted)
    through_ts = compacted[-1].ts if compacted else (prior.through_ts if prior else "")
    return SummaryRecord(
        content=content,
        digest=digest,
        source_turns=source_turns,
        through_ts=through_ts,
        # A summary is a new portable prefix. Native state for its old turns is
        # both unusable and potentially large, so do not embed it in the durable
        # summary tail.
        tail=tuple(replace(turn, harness_replay=None) for turn in tail),
        ts=datetime.now(UTC).isoformat(),
    )


def build_conversation_replay(
    records: Sequence[HistoryRecord],
    *,
    max_turns: int | None = 40,
    max_bytes: int | None = 16_000,
) -> tuple[ConversationReplay, SummaryRecord | None]:
    """Build a structured prefix and, only at a boundary, a new stable summary.

    Summary records are append-only. A record embeds the short un-compacted tail,
    so appending the summary after the turns it represents does not reorder or
    rewrite stored history. Subsequent turns extend that exact replay prefix until
    one of the bounds is crossed again.
    """

    latest_summary: SummaryRecord | None = None
    latest_summary_index = -1
    for index, record in enumerate(records):
        if isinstance(record, SummaryRecord):
            latest_summary = record
            latest_summary_index = index

    appended_turns = [
        record
        for record in records[latest_summary_index + 1 :]
        if isinstance(record, TurnRecord)
    ]
    active_turns = [
        *((latest_summary.tail) if latest_summary is not None else ()),
        *appended_turns,
    ]

    if latest_summary is not None:
        current_messages = latest_summary.messages + tuple(
            message for turn in appended_turns for message in turn.messages
        )
        source_turns = latest_summary.source_turns + len(active_turns)
    else:
        current_messages = tuple(
            message for turn in active_turns for message in turn.messages
        )
        source_turns = len(active_turns)

    over_turns = max_turns is not None and len(active_turns) > max_turns
    over_bytes = max_bytes is not None and _replay_bytes(current_messages) > max_bytes
    if not over_turns and not over_bytes:
        # A summary changes the portable prefix. Native state from its embedded
        # tail still represents the pre-summary conversation and is unusable;
        # only a post-summary turn's fresh checkpoint may restore that shape.
        replay_state_turns = appended_turns if latest_summary is not None else active_turns
        return (
            ConversationReplay(
                messages=current_messages,
                source_turns=source_turns,
                summary_digest=latest_summary.digest if latest_summary else None,
                harness_replay=_fold_harness_replay(replay_state_turns),
            ),
            None,
        )

    if not active_turns:
        return ConversationReplay(), None
    keep_count = max(
        1,
        min(len(active_turns), (max_turns or len(active_turns)) // 2),
    )
    compacted = active_turns[:-keep_count]
    tail: Sequence[TurnRecord] = active_turns[-keep_count:]
    if not compacted:
        compacted = active_turns[:-1]
        tail = active_turns[-1:]
    summary = _make_summary(latest_summary, compacted, tail, max_bytes=max_bytes)

    # If a byte-only bound is still exceeded, move more of the tail into the
    # stable summary until the remaining replay fits or one latest turn remains.
    while max_bytes is not None and len(summary.tail) > 1:
        if _replay_bytes(summary.messages) <= max_bytes:
            break
        compacted = [*compacted, summary.tail[0]]
        tail = summary.tail[1:]
        summary = _make_summary(latest_summary, compacted, tail, max_bytes=max_bytes)

    return (
        ConversationReplay(
            messages=summary.messages,
            source_turns=summary.source_turns + len(summary.tail),
            summary_digest=summary.digest,
            # The explicit compaction boundary intentionally changes the prefix.
            # The matching harness writes a new checkpoint after the first turn
            # over this synthetic summary.
            harness_replay=None,
        ),
        summary,
    )


# Sane structured-replay caps. Crossing one creates a new durable summary;
# ordinary appends never move or rewrite the already-cached prefix.
DEFAULT_REPLAY_MAX_TURNS = 40
DEFAULT_REPLAY_MAX_BYTES = 16_000


def utcnow_iso() -> str:
    """An RFC3339 UTC timestamp for a turn record's ``ts``."""
    return datetime.now(UTC).isoformat()
