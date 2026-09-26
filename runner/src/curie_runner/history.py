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


class HistoryAppendError(HistoryError):
    """The state API refused a transcript append."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(status)


class HistoryCapacityError(HistoryAppendError):
    """The state API refused a transcript append because a byte cap was reached."""


class HistoryConflictError(HistoryAppendError):
    """A compaction rewrite lost its compare-and-set race to a concurrent write."""


class StructuredReplayUnsupported(HistoryError):
    """The selected harness cannot consume a recovered structured prefix."""


JsonContent = str | list[dict[str, Any]]

# Headroom every runner transcript write leaves free under the value cap (#2927).
# The worker appends a publication outcome to the same key without a reserve;
# that record's text is capped near 2000 characters, so one always fits.
HISTORY_APPEND_RESERVE_BYTES = 8_192

# Compare-and-set attempts for one capacity compaction before giving up.
_COMPACTION_ATTEMPTS = 3

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

    return _value_size([record])


def _digest_marker(value: str) -> str:
    encoded = value.encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return (
        "[history payload omitted; "
        f"sha256={digest}; original_bytes={len(encoded)}]"
    )


def _add_text_payload(
    value: str,
    *,
    path: tuple[str | int, ...],
    priority: int,
    candidates: dict[str, tuple[int, list[tuple[str | int, ...]]]],
) -> None:
    """Index one explicitly allowed text value, without walking provider objects."""

    existing = candidates.get(value)
    if existing is None:
        candidates[value] = (priority, [path])
    else:
        existing_priority, paths = existing
        paths.append(path)
        candidates[value] = (min(existing_priority, priority), paths)


def _turn_text_payloads(
    record: dict[str, Any],
    *,
    tool_results_only: bool,
) -> dict[str, tuple[int, list[tuple[str | int, ...]]]]:
    """Index portable text by reduction priority and stable object path."""

    candidates: dict[str, tuple[int, list[tuple[str | int, ...]]]] = {}

    messages = record.get("messages")
    if not isinstance(messages, list):
        return candidates
    first_user = next(
        (i for i, message in enumerate(messages) if message["role"] == "user"), None
    )
    final_assistant = next(
        (i for i in range(len(messages) - 1, -1, -1) if messages[i]["role"] == "assistant"),
        None,
    )
    # Keep both legacy projections and the full boundary messages exact. Text
    # elsewhere may share their value without making these paths replaceable.
    for message_index, message in enumerate(messages):
        if message_index in (first_user, final_assistant):
            continue
        if not isinstance(message, Mapping):
            continue
        content = message.get("content")
        content_path: tuple[str | int, ...] = (
            "messages",
            message_index,
            "content",
        )
        if isinstance(content, str):
            if not tool_results_only:
                _add_text_payload(
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
                if tool_results_only:
                    result_content = block["content"]
                    if isinstance(result_content, str):
                        _add_text_payload(
                            result_content,
                            path=(*block_path, "content"),
                            priority=0,
                            candidates=candidates,
                        )
                    elif isinstance(result_content, list):
                        for nested_index, nested in enumerate(result_content):
                            if (
                                isinstance(nested, Mapping)
                                and nested.get("type") == "text"
                                and isinstance(nested.get("text"), str)
                            ):
                                _add_text_payload(
                                    nested["text"],
                                    path=(*block_path, "content", nested_index, "text"),
                                    priority=0,
                                    candidates=candidates,
                                )
            elif (
                not tool_results_only
                and block_type == "text"
                and isinstance(block.get("text"), str)
            ):
                _add_text_payload(
                    block["text"],
                    path=(*block_path, "text"),
                    priority=1,
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


def _compact_tool_groups(raw: dict[str, Any], max_value_bytes: int) -> dict[str, Any]:
    """Omit oldest complete tool exchanges without splitting a message group."""

    messages = raw["messages"]
    uses: dict[str, list[tuple[int, int]]] = {}
    results: dict[str, list[tuple[int, int]]] = {}
    protected: set[int] = set()
    for message_index, message in enumerate(messages):
        content = message["content"]
        if not isinstance(content, list):
            continue
        for block_index, block in enumerate(content):
            block_type = block.get("type")
            if block_type not in ("tool_use", "tool_result"):
                continue
            is_use = block_type == "tool_use"
            identifier = block.get("id" if is_use else "tool_use_id")
            expected_role = "assistant" if is_use else "user"
            if not isinstance(identifier, str) or message["role"] != expected_role:
                protected.add(message_index)
                continue
            positions = uses if is_use else results
            positions.setdefault(identifier, []).append((message_index, block_index))

    # Calls sharing an assistant or result message form one group. Keeping an
    # unmatched call protects its whole group, including pending approval data.
    neighbors: dict[int, set[int]] = {}
    for identifier in uses.keys() | results.keys():
        calls = uses.get(identifier, [])
        replies = results.get(identifier, [])
        if len(calls) != 1 or len(replies) != 1 or calls[0][0] >= replies[0][0]:
            protected.update(index for index, _block in [*calls, *replies])
            continue
        call_index, reply_index = calls[0][0], replies[0][0]
        neighbors.setdefault(call_index, set()).add(reply_index)
        neighbors.setdefault(reply_index, set()).add(call_index)

    first_user = next(
        (i for i, message in enumerate(messages) if message["role"] == "user"), None
    )
    if first_user is not None:
        protected.add(first_user)
    final_assistant = next(
        (i for i in range(len(messages) - 1, -1, -1) if messages[i]["role"] == "assistant"),
        None,
    )
    if final_assistant is not None:
        protected.add(final_assistant)
    groups: list[set[int]] = []
    visited: set[int] = set()
    for index in sorted(neighbors):
        if index in visited:
            continue
        pending = [index]
        group: set[int] = set()
        while pending:
            member = pending.pop()
            if member in group:
                continue
            group.add(member)
            pending.extend(neighbors[member] - group)
        visited.update(group)
        groups.append(group)

    removed: set[int] = set()
    candidate = raw
    # Keep the most recent exchange even when it is complete. Earlier groups
    # remain in their original order, and only empty messages are discarded.
    for group in groups[:-1]:
        if group & protected:
            continue
        removed.update(group)
        omitted: list[dict[str, Any]] = []
        kept: list[dict[str, Any]] = []
        marker_content: list[dict[str, Any]] | None = None
        call_count = 0
        for index, message in enumerate(messages):
            if index not in removed:
                kept.append(message)
                continue
            content = message["content"]
            retained_blocks: list[dict[str, Any]] = []
            omitted_blocks: list[dict[str, Any]] = []
            for block in content:
                if block.get("type") in (
                    "tool_use", "tool_result", "thinking", "redacted_thinking"
                ):
                    omitted_blocks.append(block)
                    call_count += block.get("type") == "tool_use"
                else:
                    retained_blocks.append(block)
            omitted.append({"role": message["role"], "content": omitted_blocks})
            if marker_content is None:
                marker_content = retained_blocks
                kept.append({"role": message["role"], "content": retained_blocks})
            elif retained_blocks:
                kept.append({"role": message["role"], "content": retained_blocks})
        digest = hashlib.sha256(
            json.dumps(omitted, separators=(",", ":"), sort_keys=True).encode("utf-8")
        ).hexdigest()
        assert marker_content is not None
        marker_content.insert(
            0,
            {
                "type": "text",
                "text": f"[history tool groups omitted; count={call_count}; sha256={digest}]",
            },
        )
        candidate = {**raw, "messages": kept}
        if _state_value_size(candidate) <= max_value_bytes:
            return candidate
    return candidate


def bound_turn_record(
    record: TurnRecord, *, max_value_bytes: int
) -> TurnRecord:
    """Bound one turn for a whole state value without losing message order.

    Native harness replay is discarded first because portable messages are the
    authority across sandbox replacement. Then tool result text is replaced with
    deterministic digest and byte count markers, followed by summarizing older
    complete tool exchanges while keeping the most recent exchange and pending
    calls. Other safe text is reduced last. The first user, final assistant,
    legacy projections and retained opaque provider blocks remain exact. If the
    remaining structure exceeds the cap, fail before append.
    """

    if max_value_bytes <= 0:
        raise HistoryError("history value byte cap must be positive")

    raw = record.to_dict()
    if _state_value_size(raw) <= max_value_bytes:
        return record

    raw["harness_replay"] = None
    if _state_value_size(raw) <= max_value_bytes:
        return TurnRecord.from_dict(raw)

    for tool_results_only in (True, False):
        if not tool_results_only:
            raw = _compact_tool_groups(raw, max_value_bytes)
            if _state_value_size(raw) <= max_value_bytes:
                return TurnRecord.from_dict(raw)

        candidates = _turn_text_payloads(raw, tool_results_only=tool_results_only)
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
    lives outside the sandbox; replay compaction is itself an append-only
    summary. Only a store with a byte cap rewrites, and only at that cap (#2927).
    """

    async def load(self) -> list[HistoryRecord]:
        """Return prior turns, oldest first (empty when none)."""
        ...

    async def append(self, record: HistoryRecord) -> bool:
        """Append durably and report whether native replay was retained."""
        ...


class NullTranscriptStore:
    """The no-history store used when ``CURIE_HISTORY_REF`` is unset.

    ``load`` yields nothing and ``append`` is a silent no-op, so the boot and
    per-turn paths are uniform whether or not a thread has a transcript ref.
    """

    async def load(self) -> list[HistoryRecord]:
        return []

    async def append(self, record: HistoryRecord) -> bool:  # noqa: ARG002 - null sink
        return False

    async def compact(self) -> None:
        """Nothing is stored, so there is nothing to compact."""
        return None


def _parse_records(value: Sequence[Any]) -> list[HistoryRecord]:
    """Parse a stored transcript array into history records, oldest first."""

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


class StateApiTranscriptStore:
    """Transcript backed by the durable state store (#23/#248/#264), the default.

    ``history_ref`` is the URL of the thread's transcript key on the state API
    (``.../agents/<id>/state/transcript/<thread_key>``). Load is a GET of that
    key; append is a POST to the key's ``/append`` endpoint. The state API
    enforces the size caps and the Postgres JSONB backing gives durability across
    an unplanned restart for free.

    Every append asks the API to keep ``HISTORY_APPEND_RESERVE_BYTES`` free under
    the value cap (#2927). When a turn append is refused at that bound, the store
    rewrites the key with a compare-and-set PUT to ``compact_transcript_value`` of
    the fresh value plus the turn. ``compact`` does the same for boot against the
    value its last ``load`` saw.
    """

    def __init__(self, key_url: str, token: str | None) -> None:
        # Normalize to no trailing slash so the /append URL composes cleanly.
        self._key_url = key_url.rstrip("/")
        self._token = token
        self._max_value_bytes: int | None = None
        # The raw value and version the last load() saw, for boot compaction.
        self._snapshot: tuple[list[Any], int] | None = None

    def _headers(self) -> dict[str, str]:
        return {"X-API-Key": self._token} if self._token else {}

    async def _fetch(self, session: aiohttp.ClientSession) -> tuple[list[Any], int] | None:
        self._max_value_bytes = None
        async with session.get(self._key_url, headers=self._headers()) as resp:
            if resp.status not in (200, 404):
                raise HistoryError(resp.status)
            advertised_cap = resp.headers.get("X-Curie-Transcript-Max-Bytes")
            try:
                cap = int(advertised_cap) if advertised_cap is not None else 0
            except ValueError:
                raise HistoryError("invalid transcript capacity header") from None
            if advertised_cap != str(cap) or cap <= HISTORY_APPEND_RESERVE_BYTES:
                raise HistoryError("invalid transcript capacity header")
            self._max_value_bytes = cap
            if resp.status == 404:
                # No transcript written yet -- a fresh thread, not an error.
                return None
            payload = await resp.json()
        value = payload.get("value")
        if not isinstance(value, list):
            raise HistoryError("transcript log is not a JSON array")
        return value, int(payload.get("version", 0))

    async def _post(self, session: aiohttp.ClientSession, item: dict[str, Any]) -> int:
        body = json.dumps({"item": item, "reserve_bytes": HISTORY_APPEND_RESERVE_BYTES})
        headers = {**self._headers(), "Content-Type": "application/json"}
        async with session.post(f"{self._key_url}/append", data=body, headers=headers) as resp:
            return resp.status

    async def _put(
        self, session: aiohttp.ClientSession, value: list[dict[str, Any]], version: int
    ) -> int:
        body = json.dumps({"value": value, "expected_version": version})
        headers = {**self._headers(), "Content-Type": "application/json"}
        async with session.put(self._key_url, data=body, headers=headers) as resp:
            return resp.status

    async def load(self) -> list[HistoryRecord]:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            self._snapshot = await self._fetch(session)
        if self._snapshot is None:
            return []
        return _parse_records(self._snapshot[0])

    async def append(self, record: HistoryRecord) -> bool:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            if self._max_value_bytes is None:
                await self._fetch(session)
            assert self._max_value_bytes is not None
            if isinstance(record, TurnRecord):
                # A lone turn leaves the reserve even on a fresh transcript.
                try:
                    record = bound_turn_record(
                        record,
                        max_value_bytes=self._max_value_bytes - HISTORY_APPEND_RESERVE_BYTES,
                    )
                except HistoryError:
                    raise HistoryCapacityError(413) from None
            item = record.to_dict()
            status = await self._post(session, item)
            if status in (200, 201):
                return isinstance(record, TurnRecord) and record.harness_replay is not None
            if status != 413:
                raise HistoryAppendError(status)
            if isinstance(record, SummaryRecord):
                # Boot owns summary capacity: it compacts against what it loaded.
                raise HistoryCapacityError(status)
            for _attempt in range(_COMPACTION_ATTEMPTS):
                snapshot = await self._fetch(session)
                if snapshot is None:
                    # The turn was bounded to leave the reserve, so a refusal on
                    # an empty key is the cap itself, not something to compact.
                    raise HistoryCapacityError(413)
                value, version = snapshot
                assert self._max_value_bytes is not None
                status = await self._put(
                    session,
                    compact_transcript_value(
                        [*value, item],
                        max_value_bytes=self._max_value_bytes,
                        reserve_bytes=HISTORY_APPEND_RESERVE_BYTES,
                    ),
                    version,
                )
                if status in (200, 201):
                    # Capacity compaction always drops the latest native replay.
                    return False
                if status == 409:
                    continue
                if status == 413:
                    raise HistoryCapacityError(status)
                raise HistoryAppendError(status)
        raise HistoryAppendError(409)

    async def compact(self) -> None:
        """Rewrite the value the last ``load`` saw, compare-and-set on its version.

        A write since that load is a ``HistoryConflictError``: the caller reloads
        rather than compacting (or summarizing) a stale view over it.
        """

        if self._snapshot is None or self._max_value_bytes is None:
            raise HistoryError("no loaded transcript to compact")
        value, version = self._snapshot
        compacted = compact_transcript_value(
            value,
            max_value_bytes=self._max_value_bytes,
            reserve_bytes=HISTORY_APPEND_RESERVE_BYTES,
        )
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            status = await self._put(session, compacted, version)
        self._snapshot = None
        if status in (200, 201):
            return
        if status == 409:
            raise HistoryConflictError(status)
        if status == 413:
            raise HistoryCapacityError(status)
        raise HistoryAppendError(status)


def resolve_history(
    history_ref: str | None, env: Mapping[str, str]
) -> NullTranscriptStore | StateApiTranscriptStore:
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
            # A missing delta leaves a gap in every earlier checkpoint. Only a
            # later complete checkpoint can make native replay eligible.
            harness = None
            entries = []
            checkpoint_seen = False
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
    # new record. The digest keeps omitted material falsifiable. When the content
    # overflows the budget, keep the most recent bytes (the marker first, then the
    # UTF-8-safe suffix that fits) rather than the oldest: the newest compacted
    # material -- e.g. a just-recorded publication outcome -- must survive even
    # once the prior summary has saturated the budget.
    budget = max(512, (max_bytes // 2) if max_bytes is not None else 8_000)
    encoded = content.encode("utf-8")
    if len(encoded) > budget:
        marker = f"[older detail summarized; digest={digest}]\n\n"
        marker_bytes = marker.encode("utf-8")
        suffix_budget = max(0, budget - len(marker_bytes))
        suffix = encoded[-suffix_budget:].decode("utf-8", errors="ignore") if suffix_budget else ""
        content = f"{marker}{suffix}"
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
    # A summary needs at least one turn to compact and one to keep (#2927). With
    # one active turn it would compact nothing and re-embed that turn in its
    # tail, doubling the stored transcript, so that replay stays plain.
    if (not over_turns and not over_bytes) or len(active_turns) <= 1:
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


def _value_size(value: Sequence[Any]) -> int:
    return len(json.dumps(list(value), separators=(",", ":")).encode("utf-8"))


def compact_transcript_value(
    value: Sequence[Any],
    *,
    max_value_bytes: int,
    reserve_bytes: int,
) -> list[dict[str, Any]]:
    """Rewrite a stored transcript array to fit the cap with the reserve free (#2927).

    Items carrying ``publication_id`` are the worker's idempotency markers and
    are kept verbatim, first; replay ignores records before the latest summary.
    Every active turn but the latest is folded into one new summary, and the
    latest turn is kept without native replay state, bounded to what is left.
    Tool output and turn detail beyond the summary line are what is lost. A
    result that still cannot fit is a ``HistoryCapacityError``.
    """

    limit = max_value_bytes - reserve_bytes
    markers = [
        dict(item) for item in value if isinstance(item, Mapping) and "publication_id" in item
    ]
    # Parse the full value, publication markers included: each marker also
    # carries a plain "user"/"assistant" pair, so it parses as an ordinary
    # TurnRecord and takes its original position among the active turns. Its
    # raw dict (with publication_id) still lands in ``markers`` above and is
    # kept verbatim in the rewritten prefix for the worker's idempotency scan;
    # this parse additionally lets its outcome text reach the new summary (or
    # the kept tail) instead of being dropped from replay (#2927).
    records = _parse_records(value)
    latest_summary: SummaryRecord | None = None
    latest_summary_index = -1
    for index, record in enumerate(records):
        if isinstance(record, SummaryRecord):
            latest_summary = record
            latest_summary_index = index
    active_turns = [
        *(latest_summary.tail if latest_summary is not None else ()),
        *(
            record
            for record in records[latest_summary_index + 1 :]
            if isinstance(record, TurnRecord)
        ),
    ]

    prefix: list[dict[str, Any]] = list(markers)
    if active_turns[:-1] or latest_summary is not None:
        prefix.append(
            _make_summary(
                latest_summary,
                active_turns[:-1],
                (),
                max_bytes=DEFAULT_REPLAY_MAX_BYTES,
            ).to_dict()
        )
    compacted = list(prefix)
    if active_turns:
        # Joining the kept turn to a non-empty prefix costs one comma and drops
        # the kept turn's own array brackets from the one-record bound.
        budget = limit - (_value_size(prefix) - 1 if prefix else 0)
        if budget <= 2:
            raise HistoryCapacityError(413)
        try:
            kept = bound_turn_record(
                replace(active_turns[-1], harness_replay=None), max_value_bytes=budget
            )
        except HistoryError:
            raise HistoryCapacityError(413) from None
        compacted.append(kept.to_dict())
    if _value_size(compacted) > limit:
        raise HistoryCapacityError(413)
    return compacted


def utcnow_iso() -> str:
    """An RFC3339 UTC timestamp for a turn record's ``ts``."""
    return datetime.now(UTC).isoformat()
