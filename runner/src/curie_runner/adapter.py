"""The SDK adapter seam: a ModelSession protocol and its claude-agent-sdk impl.

The runner owns exactly one long-lived model session per process (one session per
sandbox), which is the source of prompt-cache affinity across turns. The session
is driven in the SDK's **streaming-input mode**: ``query`` pushes a user message
(initial or a mid-run steer), ``receive_turn`` yields the SDK messages for the
current turn until its terminal result, and ``interrupt`` is the native hard stop.
Steering is therefore first-class, not emulated: a ``query`` issued while a turn's
``receive_turn`` iterator is live is incorporated at the next loop boundary.

The protocol is the fake seam: unit tests and the conformance suite supply a
scripted ModelSession, so the model (the only external dependency) is mocked at
this boundary and nothing above it is. ``aci-protocol`` is never mocked.
"""

from __future__ import annotations

import contextlib
import json
import os
import time
import uuid
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, cast

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookMatcher,
    SdkPluginConfig,
    ServerToolResultBlock,
    ServerToolUseBlock,
    StreamEvent,
    TaskBudget,
    TextBlock,
    ThinkingBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk._cli_version import __cli_version__
from claude_agent_sdk._internal.session_store import project_key_for_directory
from claude_agent_sdk.types import (
    CanUseTool,
    McpSdkServerConfig,
    PermissionMode,
    SessionKey,
    SessionStore,
    SessionStoreEntry,
)

from .history import ConversationMessage, HarnessReplayState

_SDK_SESSION_NAMESPACE = uuid.UUID("83efb74f-f09e-4db6-b898-9ed8d7084ba8")


class _SeededSessionStore:
    """SDK mirror seeded from portable messages or an optional native checkpoint."""

    def __init__(
        self,
        key: SessionKey,
        entries: list[SessionStoreEntry],
        *,
        checkpoint_required: bool,
    ) -> None:
        self._key = key
        self._entries = json.loads(json.dumps(entries))
        self._exported_from = len(entries)
        self._checkpoint_required = checkpoint_required

    async def append(self, key: SessionKey, entries: list[SessionStoreEntry]) -> None:
        if key == self._key:
            self._entries.extend(json.loads(json.dumps(entries)))

    async def load(self, key: SessionKey) -> list[SessionStoreEntry] | None:
        return (
            cast("list[SessionStoreEntry]", json.loads(json.dumps(self._entries)))
            if key == self._key and self._entries
            else None
        )

    async def export_replay_state(self) -> HarnessReplayState | None:
        """Return a full checkpoint once, then only newly mirrored SDK entries."""

        if self._checkpoint_required:
            kind = "checkpoint"
            selected = self._entries
        else:
            kind = "delta"
            selected = self._entries[self._exported_from :]
        self._checkpoint_required = False
        self._exported_from = len(self._entries)
        if not selected:
            return None
        return HarnessReplayState(
            harness="claude",
            kind=kind,
            entries=tuple(json.loads(json.dumps(selected))),
        )


@dataclass(frozen=True)
class StructuredResume:
    """Claude SDK options needed to reconstruct one portable prefix."""

    session_id: str
    resume: str | None
    session_store: SessionStore | None
    session_key: SessionKey


def build_structured_resume(
    messages: tuple[ConversationMessage, ...],
    *,
    curie_session_id: str,
    cwd: str | None,
    harness_replay: HarnessReplayState | None = None,
) -> StructuredResume:
    """Materialize portable messages into the SDK's ephemeral resume envelope.

    Portable role/content is always sufficient. When the matching harness left
    an opaque native checkpoint, it is preferred to retain the SDK's exact
    cache-breakpoint shape; otherwise UUIDs and the local JSONL envelope are
    deterministic adapter details reconstructed on this runner. Native entries
    are an optional optimization, never Curie's portable persistence contract.
    """

    session_id = str(uuid.uuid5(_SDK_SESSION_NAMESPACE, curie_session_id))
    key: SessionKey = {
        "project_key": project_key_for_directory(cwd),
        "session_id": session_id,
    }
    if (
        harness_replay is not None
        and harness_replay.harness == "claude"
        and harness_replay.kind == "checkpoint"
        and harness_replay.entries
    ):
        native_entries = cast(
            "list[SessionStoreEntry]",
            json.loads(json.dumps(harness_replay.entries)),
        )
        store = _SeededSessionStore(
            key,
            native_entries,
            checkpoint_required=False,
        )
        return StructuredResume(
            session_id=session_id,
            resume=session_id,
            session_store=cast("SessionStore", store),
            session_key=key,
        )

    if not messages:
        store = _SeededSessionStore(key, [], checkpoint_required=True)
        return StructuredResume(
            session_id=session_id,
            resume=None,
            session_store=cast("SessionStore", store),
            session_key=key,
        )

    effective_cwd = str(Path(cwd).resolve()) if cwd is not None else os.getcwd()
    entries: list[SessionStoreEntry] = []
    parent_uuid: str | None = None
    for index, message in enumerate(messages):
        canonical = json.dumps(message.to_dict(), separators=(",", ":"), sort_keys=True)
        entry_uuid = str(uuid.uuid5(uuid.UUID(session_id), f"{index}:{canonical}"))
        entry = cast(
            "SessionStoreEntry",
            {
                "parentUuid": parent_uuid,
                "isSidechain": False,
                "userType": "external",
                "cwd": effective_cwd,
                "sessionId": session_id,
                "version": __cli_version__,
                "gitBranch": "",
                "type": message.role,
                "message": message.to_dict(),
                "uuid": entry_uuid,
                # This is adapter envelope metadata, not conversation time. Keep it
                # stable so separate runners materialize identical local transcripts.
                "timestamp": "1970-01-01T00:00:00.000Z",
            },
        )
        entries.append(entry)
        parent_uuid = entry_uuid
    store = _SeededSessionStore(key, entries, checkpoint_required=True)
    return StructuredResume(
        session_id=session_id,
        resume=session_id,
        session_store=cast("SessionStore", store),
        session_key=key,
    )


def _content_block_to_dict(block: object) -> dict[str, Any] | None:
    if isinstance(block, TextBlock):
        return {"type": "text", "text": block.text}
    if isinstance(block, ThinkingBlock):
        return {"type": "thinking", "thinking": block.thinking, "signature": block.signature}
    if isinstance(block, ToolUseBlock):
        return {"type": "tool_use", "id": block.id, "name": block.name, "input": block.input}
    if isinstance(block, ToolResultBlock):
        result: dict[str, Any] = {
            "type": "tool_result",
            "tool_use_id": block.tool_use_id,
            "content": block.content,
        }
        if block.is_error is not None:
            result["is_error"] = block.is_error
        return result
    if isinstance(block, ServerToolUseBlock):
        return {
            "type": "server_tool_use",
            "id": block.id,
            "name": block.name,
            "input": block.input,
        }
    if isinstance(block, ServerToolResultBlock):
        return {
            "type": "server_tool_result",
            "tool_use_id": block.tool_use_id,
            "content": block.content,
        }
    return None


def model_message_to_conversation(message: object) -> ConversationMessage | None:
    """Project one SDK message into Curie's portable role/content shape."""

    if isinstance(message, UserMessage):
        if isinstance(message.content, str):
            content: str | list[dict[str, Any]] = message.content
        else:
            content = [
                projected
                for block in message.content
                if (projected := _content_block_to_dict(block)) is not None
            ]
        return ConversationMessage(role="user", content=content)
    if isinstance(message, AssistantMessage):
        return ConversationMessage(
            role="assistant",
            content=[
                projected
                for block in message.content
                if (projected := _content_block_to_dict(block)) is not None
            ],
        )
    return None

_ALLOWED_PARTIAL_BOUNDARY_TYPES = frozenset(("message_start", "content_block_start"))


@dataclass(frozen=True, slots=True)
class PartialMessageBoundary:
    """Payload-free evidence that the provider began returning a message."""

    event_type: str


@dataclass(frozen=True, slots=True)
class StreamedToolUseBoundary:
    """Sanitized evidence that the provider began a tool call."""

    call_id: str = field(repr=False)
    tool_name: str
    observed_time_ns: int


class ModelSession(Protocol):
    """One long-lived model session the runner drives turn by turn."""

    async def connect(self) -> None:
        """Start the session (spawn/attach the harness), rehydrating if configured."""
        ...

    async def query(self, text: str) -> None:
        """Push a user message into the session (initial turn or mid-run steer)."""
        ...

    def receive_turn(self) -> AsyncIterator[Any]:
        """Yield SDK messages or stripped boundaries through the terminal result."""
        ...

    async def interrupt(self) -> None:
        """Hard-stop the in-flight turn at the next safe boundary."""
        ...

    async def close(self) -> None:
        """Tear down the session."""
        ...


def build_options(
    *,
    plugins: list[SdkPluginConfig],
    model: str | None,
    system_prompt: str | None,
    max_turns: int,
    max_budget_usd: float | None,
    resume: str | None,
    session_id: str | None = None,
    session_store: SessionStore | None = None,
    thinking: dict[str, Any] | None = None,
    task_budget_hint: int | None = None,
    env: dict[str, str] | None = None,
    hooks: dict[str, list[HookMatcher]] | None = None,
    mcp_servers: dict[str, McpSdkServerConfig] | None = None,
    can_use_tool: CanUseTool | None = None,
    cwd: str | None = None,
    web_search_enabled: bool = True,
    policy_disallowed_tools: Iterable[str] = (),
) -> ClaudeAgentOptions:
    """Assemble ClaudeAgentOptions for the session.

    ``resume`` is the provider-native rehydrate path (ADR-0003,
    stateless-first). For Curie's portable history it names an ephemeral SDK
    session envelope rebuilt by :func:`build_structured_resume`; it never points
    the provider at Curie's durable state URL or assumes surviving local state.

    The three ACI budget fields map to distinct SDK controls: ``max_budget_usd``
    is the daily USD cap enforced natively; ``task_budget_hint`` becomes the SDK
    ``task_budget`` so the model self-paces (ACI section 6b, a soft hint, not a
    ceiling); and the hard per-run output-token ceiling is enforced by the runner
    itself (see budget.py).
    """

    task_budget = TaskBudget(total=task_budget_hint) if task_budget_hint else None
    # The permission posture (#245, ADR-0010): with a can_use_tool callback the
    # session runs in default permission mode and every tool call is decided by
    # the callback (approval-required tools are denied and pause the run; all
    # others are allowed, preserving the pre-gate behavior). Without one there
    # is nothing to decide, so the historical bypassPermissions posture is kept
    # verbatim -- an unconfigured agent sees zero behavior change.
    permission_mode: PermissionMode = "default" if can_use_tool is not None else "bypassPermissions"
    # OMITTED, not defaulted, when the operator set nothing (#1182, ADR-0098).
    # Passing thinking=None would be a value the SDK could act on; leaving the
    # key out is the only way to say "no opinion", which is what an unconfigured
    # install has always said and must keep saying.
    thinking_option: dict[str, Any] = {"thinking": cast("Any", thinking)} if thinking else {}
    cwd_option: dict[str, Any] = {"cwd": cwd} if cwd is not None else {}
    disallowed_tools = sorted(set(policy_disallowed_tools))
    if not web_search_enabled:
        disallowed_tools = [
            "WebSearch",
            *(tool_name for tool_name in disallowed_tools if tool_name != "WebSearch"),
        ]
    return ClaudeAgentOptions(
        plugins=plugins,
        model=model,
        # Pin the SDK's complete Claude Code tool surface explicitly.  Coding
        # tools are a platform session capability, not something a bundle skill
        # opts into; ``allowed_tools`` stays empty so this does not pre-authorize
        # any call or bypass Curie's permission/approval callbacks.
        tools=cast("Any", {"type": "preset", "preset": "claude_code"}),
        allowed_tools=[],
        # Anthropic documents WebSearch as a provider-executed server tool.
        # ``disallowed_tools`` removes a tool from the model catalogue; unlike
        # ``allowed_tools`` it is not a permission preauthorization. See:
        # https://platform.claude.com/docs/en/agents-and-tools/tool-use/web-search-tool
        # https://github.com/anthropics/claude-agent-sdk-python#using-tools
        disallowed_tools=disallowed_tools,
        **thinking_option,
        **cwd_option,
        system_prompt=system_prompt,
        max_turns=max_turns,
        max_budget_usd=max_budget_usd,
        resume=resume,
        session_id=session_id if resume is None else None,
        session_store=session_store,
        session_store_flush="eager" if session_store is not None else "batched",
        task_budget=task_budget,
        permission_mode=permission_mode,
        can_use_tool=can_use_tool,
        env=env or {},
        # In-bundle PreToolUse guardrails from the manifest hooks field (#272).
        # Empty/None means no bundle hooks; the SDK default applies. The event
        # keys are the SDK's HookEvent literals (we emit only "PreToolUse").
        hooks=cast("Any", hooks),
        # In-process platform tools (the approval-request gate, ADR-0010).
        mcp_servers=cast("Any", mcp_servers or {}),
        include_partial_messages=True,
    )


class ClaudeAgentSession:
    """ModelSession backed by a real claude-agent-sdk streaming-input session."""

    def __init__(self, options: ClaudeAgentOptions) -> None:
        self._options = options
        self._client = ClaudeSDKClient(options)

    async def connect(self) -> None:
        await self._client.connect()

    async def query(self, text: str) -> None:
        await self._client.query(text)

    def receive_turn(self) -> AsyncIterator[Any]:
        async def normalized() -> AsyncIterator[Any]:
            response = cast("Any", self._client.receive_response())
            async with contextlib.aclosing(response):
                async for message in response:
                    if isinstance(message, StreamEvent):
                        event = message.event
                        event_type = (
                            event.get("type") if isinstance(event, dict) else None
                        )
                        if event_type == "content_block_start":
                            content_block = event.get("content_block")
                            if isinstance(content_block, dict):
                                call_id = content_block.get("id")
                                tool_name = content_block.get("name")
                                if (
                                    content_block.get("type") == "tool_use"
                                    and isinstance(call_id, str)
                                    and call_id
                                    and isinstance(tool_name, str)
                                    and tool_name
                                ):
                                    yield StreamedToolUseBoundary(
                                        call_id=call_id,
                                        tool_name=tool_name,
                                        observed_time_ns=time.time_ns(),
                                    )
                                    continue
                        if event_type in _ALLOWED_PARTIAL_BOUNDARY_TYPES:
                            # Do not forward the StreamEvent object: its event body,
                            # uuid, SDK session id, and parent tool id are all
                            # provider payload. Only this bounded type survives the
                            # adapter seam into session telemetry.
                            yield PartialMessageBoundary(event_type=event_type)
                        continue
                    yield message

        return normalized()

    async def interrupt(self) -> None:
        await self._client.interrupt()

    async def close(self) -> None:
        await self._client.disconnect()

    async def export_replay_state(self) -> HarnessReplayState | None:
        """Export the provider transcript checkpoint/delta mirrored this turn."""

        store = self._options.session_store
        if isinstance(store, _SeededSessionStore):
            return await store.export_replay_state()
        return None
