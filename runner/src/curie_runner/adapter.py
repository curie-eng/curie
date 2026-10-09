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
import hashlib
import json
import logging
import os
import time
import uuid
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Protocol, cast, runtime_checkable

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
    SettingSource,
)

from .history import (
    ConversationMessage,
    HarnessReplayState,
    HistoryError,
    reduce_unprovable_overlap_turns,
    validate_assistant_groups,
)
from .mcp_argv import install as install_mcp_argv_offload

install_mcp_argv_offload()

logger = logging.getLogger(__name__)

_SDK_SESSION_NAMESPACE = uuid.UUID("83efb74f-f09e-4db6-b898-9ed8d7084ba8")


def _assistant_group(identifier: object) -> str | None:
    """Bound provider identity and retain only opaque replay provenance."""

    if (
        not isinstance(identifier, str)
        or not 1 <= len(identifier) <= 256
        or any(not 33 <= ord(char) <= 126 for char in identifier)
    ):
        return None
    return hashlib.sha256(identifier.encode("ascii")).hexdigest()


def _recover_assistant_groups(
    messages: tuple[ConversationMessage, ...], checkpoint: tuple[dict[str, Any], ...]
) -> tuple[tuple[ConversationMessage, ...], bool]:
    """Recover identity only from an exact native conversation correspondence.

    Checkpoint attachments and provider envelope data never become portable
    content or current prompt authority. A partial match supplies no provenance.
    """

    native = [entry for entry in checkpoint if entry.get("type") in ("user", "assistant")]
    if len(native) != len(messages):
        return messages, False
    pairs: list[tuple[ConversationMessage, str | None]] = []
    native_to_portable: dict[str, str] = {}
    portable_to_native: dict[str, str] = {}
    native_eligible = True
    for message, entry in zip(messages, native, strict=True):
        payload = entry.get("message")
        native_content = payload.get("content") if isinstance(payload, dict) else None
        if isinstance(native_content, list):
            # Observed SDK 0.2.159 parse_message projects this direct-caller
            # envelope away when constructing ToolUseBlock. Other extras stay
            # present, so they cannot pass exact portable correspondence.
            native_content = [
                {key: value for key, value in block.items() if key != "caller"}
                if isinstance(block, dict)
                and block.get("type") == "tool_use"
                and block.get("caller") == {"type": "direct"}
                else block
                for block in native_content
            ]
        if (
            not isinstance(payload, dict)
            or entry["type"] != message.role
            or payload.get("role") != message.role
            or native_content != message.content
        ):
            return messages, False
        group = _assistant_group(payload.get("id")) if message.role == "assistant" else None
        if (
            message.role == "assistant"
            and group is None
            and (message.assistant_group is not None or payload.get("id") is not None)
        ):
            # Exact content is insufficient when the native envelope cannot
            # represent the portable grouping. Rebuild its IDs from portable.
            native_eligible = False
        if message.assistant_group is not None and group is not None:
            # A reconstructed native ID is adapter-owned, not the original
            # provider ID. Compare group correspondence, never raw ID hashes.
            if (
                native_to_portable.get(group, message.assistant_group) != message.assistant_group
                or portable_to_native.get(message.assistant_group, group) != group
            ):
                raise HistoryError("native and portable assistant group provenance disagree")
            native_to_portable[group] = message.assistant_group
            portable_to_native[message.assistant_group] = group
        pairs.append((message, group))
    recovered = tuple(
        replace(
            message,
            assistant_group=message.assistant_group
            or (native_to_portable.get(group, group) if group is not None else None),
        )
        for message, group in pairs
    )
    return recovered, native_eligible


# The CLI's built-in instructions tell the model to end commits with a
# "Co-Authored-By: Claude" trailer and PR bodies with the "Generated with
# [Claude Code]" footer (#3193). Both texts are governed by the CLI's
# ``attribution`` settings object; an empty string hides each. Delivered on
# the flag-settings layer (``ClaudeAgentOptions.settings``, the ``--settings``
# CLI flag, highest priority among user-controlled settings), and with both
# texts empty the CLI emits no attribution instruction at all, so no model --
# Claude or otherwise -- ever sees one.
_SDK_ATTRIBUTION_OFF_SETTINGS = json.dumps({"attribution": {"commit": "", "pr": ""}})
_SDK_TITLE_MODEL_ENV = "ANTHROPIC_DEFAULT_HAIKU_MODEL"
_SDK_REVIEWER_MODEL_ENV = "ANTHROPIC_DEFAULT_OPUS_MODEL"
# The settings the CLI loads, passed explicitly (#3766, ADR-0189). Today's
# loading, kept on purpose: ``[]`` would also stop a workspace ``CLAUDE.md``.
_SETTING_SOURCES: tuple[SettingSource, ...] = ("user", "project", "local")
_SDK_DISABLE_TERMINAL_TITLE_ENV = "CLAUDE_CODE_DISABLE_TERMINAL_TITLE"


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

    def request_full_checkpoint(self) -> None:
        """The prior native export was not durable; reset the delta baseline."""

        self._checkpoint_required = True


@dataclass(frozen=True)
class StructuredResume:
    """Claude SDK options needed to reconstruct one portable prefix."""

    session_id: str
    resume: str | None
    session_store: SessionStore | None
    session_key: SessionKey


def _checkpoint_keeps_system_prompt(
    entries: Iterable[dict[str, Any]], system_prompt: str | None
) -> bool:
    """Whether resuming ``entries`` leaves this boot's system prompt in force.

    The CLI records the prompt it ran under as a ``prompt_snapshot`` attachment
    entry and resends that prompt on resume instead of the one it is given
    (claude-agent-sdk 0.2.159, CLI 2.1.281). A checkpoint recorded under any
    other prompt would hide what this boot added to it, such as this turn's
    attachments. With no prompt of its own, there is nothing a restored one
    could hide.
    """

    if system_prompt is None:
        return True
    return all(
        attachment.get("systemPrompt") == [system_prompt]
        for entry in entries
        if entry.get("type") == "attachment"
        and isinstance(attachment := entry.get("attachment"), dict)
        and attachment.get("type") == "prompt_snapshot"
    )


def build_structured_resume(
    messages: tuple[ConversationMessage, ...],
    *,
    curie_session_id: str,
    cwd: str | None,
    harness_replay: HarnessReplayState | None = None,
    system_prompt: str | None = None,
) -> StructuredResume:
    """Materialize portable messages into the SDK's ephemeral resume envelope.

    Portable content and proven assistant groups are sufficient. When the
    matching harness left a complete, consistent native checkpoint, it is
    preferred to retain the SDK's exact
    cache-breakpoint shape; otherwise UUIDs and the local JSONL envelope are
    deterministic adapter details reconstructed on this runner. Native entries
    are an optional optimization, never Curie's portable persistence contract,
    so a checkpoint that would override ``system_prompt`` is set aside.
    """

    session_id = str(uuid.uuid5(_SDK_SESSION_NAMESPACE, curie_session_id))
    key: SessionKey = {
        "project_key": project_key_for_directory(cwd),
        "session_id": session_id,
    }
    checkpoint: tuple[dict[str, Any], ...] = (
        harness_replay.entries
        if harness_replay is not None
        and harness_replay.harness == "claude"
        and harness_replay.kind == "checkpoint"
        else ()
    )
    if checkpoint:
        messages, native_eligible = _recover_assistant_groups(messages, checkpoint)
        if not native_eligible:
            checkpoint = ()
    messages, reduced = reduce_unprovable_overlap_turns(messages)
    if reduced:
        # RUNNER-HISTORY-GROUP-4: the native checkpoint describes the rows this
        # replay no longer carries.
        checkpoint = ()
        logger.warning(
            "history replay kept only the text of turns whose overlapping tool calls"
            " have no provable assistant grouping session=%s turns_reduced=%d",
            curie_session_id,
            reduced,
        )
    validate_assistant_groups(messages)
    if checkpoint and not _checkpoint_keeps_system_prompt(checkpoint, system_prompt):
        logger.info(
            "native checkpoint recorded another system prompt; replaying the portable"
            " prefix session_id=%s",
            session_id,
        )
        checkpoint = ()
    if checkpoint:
        native_entries = cast(
            "list[SessionStoreEntry]",
            json.loads(json.dumps(checkpoint)),
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
        provider_message = {"role": message.role, "content": message.to_dict()["content"]}
        if message.assistant_group is not None:
            provider_message["id"] = (
                "msg_curie_" + uuid.uuid5(uuid.UUID(session_id), message.assistant_group).hex
            )
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
                "message": provider_message,
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
        nested = message.parent_tool_use_id is not None
        content = [
            projected
            for block in message.content
            if not (nested and isinstance(block, TextBlock | ThinkingBlock))
            and (projected := _content_block_to_dict(block)) is not None
        ]
        if nested and not content:
            return None
        return ConversationMessage(
            role="assistant",
            content=content,
        )
    return None


_ALLOWED_PARTIAL_BOUNDARY_TYPES = frozenset(("message_start", "content_block_start"))


@dataclass(frozen=True, slots=True)
class PartialMessageBoundary:
    """Bounded activity evidence and opaque internal replay provenance."""

    event_type: str
    assistant_group: str | None = field(default=None, repr=False)


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


@runtime_checkable
class McpServerReconnector(Protocol):
    """A session whose own MCP connections the runner can check and repair (#2634).

    Deliberately separate from ``ModelSession``: a side probe proving a
    connector reachable says nothing about the long-lived session's own MCP
    client, so before clearing a connector failure the runner asks the session
    itself. Sessions that cannot answer (the offline fake, other harnesses)
    simply do not implement this and keep the side-probe-only behavior.
    """

    async def ensure_mcp_server(self, name: str) -> bool:
        """True when ``name`` is connected, reconnecting it once if it is not."""
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
    reviewer_model: str | None = None,
    thinking: dict[str, Any] | None = None,
    task_budget_hint: int | None = None,
    env: dict[str, str] | None = None,
    hooks: dict[str, list[HookMatcher]] | None = None,
    mcp_servers: dict[str, McpSdkServerConfig] | None = None,
    can_use_tool: CanUseTool | None = None,
    cwd: str | None = None,
    web_search_enabled: bool = True,
    policy_disallowed_tools: Iterable[str] = (),
    disallowed_tools: list[str] | tuple[str, ...] | None = None,
    skills: list[str] | None = None,
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

    ``skills`` is the bundle's own skill list (``plugin.bundle_skill_names``).
    It is always passed to the SDK as a list, ``[]`` when there are none:
    leaving it unset would keep every built-in Claude Code CLI skill in the
    model's listing (#3766, ADR-0189).
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
    # The explicit operator denylist is an ordered configuration surface. Keep
    # its order stable, then append the policy projection deterministically.
    # A set-only merge reordered CURIE_DISALLOWED_TOOLS and broke parity with
    # the fake session; filtering the policy tail also deduplicates names that
    # both sources deny without changing the operator's declared order.
    explicit_disallowed = list(dict.fromkeys(disallowed_tools or ()))
    explicit_names = set(explicit_disallowed)
    disallowed_tools = [
        *explicit_disallowed,
        *sorted(set(policy_disallowed_tools) - explicit_names),
    ]
    if not web_search_enabled:
        disallowed_tools = [
            "WebSearch",
            *(tool_name for tool_name in disallowed_tools if tool_name != "WebSearch"),
        ]
    sdk_env = dict(env or {})
    # Auth has already been resolved at boot. An explicit SDK env value,
    # including an empty value that fences inherited auth, wins over the
    # process env. Reviewers use the Opus alias so install overrides do not
    # require rebuilding the bundle (#4120).
    credential = sdk_env.get(
        "CLAUDE_CODE_OAUTH_TOKEN", os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
    ) or sdk_env.get("ANTHROPIC_API_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))
    sdk_env[_SDK_REVIEWER_MODEL_ENV] = (
        reviewer_model
        if reviewer_model is not None
        else ("claude-opus-5-5" if credential.startswith("sk-ant-") else "openai/gpt-6.1-sol")
    )
    title_model = sdk_env.get(_SDK_TITLE_MODEL_ENV, os.environ.get(_SDK_TITLE_MODEL_ENV, ""))
    if not title_model.strip():
        sdk_env[_SDK_DISABLE_TERMINAL_TITLE_ENV] = "1"
        logger.info("SDK session title request skipped: %s is not configured", _SDK_TITLE_MODEL_ENV)
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
        # Only the bundle's own skills reach the model's listing (#3766,
        # ADR-0189). ``None`` would mean "every skill the CLI has", built-ins
        # such as ``update-config`` included, so it is never passed.
        skills=list(skills or []),
        # Explicit, because with ``skills`` set the SDK otherwise fills in
        # ``["user", "project"]`` and quietly drops ``local``. This keeps the
        # settings loading the runner had before ``skills`` was passed.
        setting_sources=list(_SETTING_SOURCES),
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
        env=sdk_env,
        # In-bundle PreToolUse guardrails from the manifest hooks field (#272).
        # Empty/None means no bundle hooks; the SDK default applies. The event
        # keys are the SDK's HookEvent literals (we emit only "PreToolUse").
        hooks=cast("Any", hooks),
        # In-process platform tools (the approval-request gate, ADR-0010),
        # connectors, and the bundle's own servers (plugin.bundle_mcp_servers).
        mcp_servers=cast("Any", mcp_servers or {}),
        # Only the servers above may load (#2899). Without this the CLI also
        # loads the cwd's project ``.mcp.json``, user settings and marketplace
        # plugin servers: none of them is in the capability probe, so
        # ``policy_disallowed_tools`` never covers them, and a mounted workspace
        # is agent-writable, so its ``.mcp.json`` is bundle-influenced input.
        # Strict mode also drops ``--plugin-dir`` servers, which is why callers
        # pass the bundle's servers in ``mcp_servers`` themselves.
        strict_mcp_config=True,
        include_partial_messages=True,
        # Observe usage on subagents whose replies contain no tool blocks.
        forward_subagent_text=True,
        # Commit/PR attribution off for every session this runner builds
        # (#3193); see _SDK_ATTRIBUTION_OFF_SETTINGS above.
        settings=_SDK_ATTRIBUTION_OFF_SETTINGS,
    )


class ClaudeAgentSession:
    """ModelSession backed by a real claude-agent-sdk streaming-input session."""

    def __init__(self, options: ClaudeAgentOptions) -> None:
        self._options = options
        self._client = ClaudeSDKClient(options)

    async def connect(self) -> None:
        # The SDK copies os.environ into the CLI at spawn. Install the parent
        # env (model key kept, platform tokens removed) for that copy, then
        # restore the process env so in-process clients keep what they captured.
        # The mandatory shell launcher drops the model key before user startup
        # files and SDK snapshots run. The CLI parent still authenticates.
        from .subprocess_env import (
            CLI_PARENT_MODEL_KEYS,
            cli_parent_env,
            platform_credential_names,
            sdk_shell_env,
        )

        snapshot = dict(os.environ)
        denied = platform_credential_names({**snapshot, **self._options.env})
        for key in list(self._options.env):
            if key in denied and key not in CLI_PARENT_MODEL_KEYS:
                self._options.env.pop(key, None)
        # SDK options override inherited env at spawn. Pin authority last in
        # both maps so options cannot select an unsanitized shell/interpreter.
        self._options.env.update(sdk_shell_env())
        try:
            os.environ.clear()
            os.environ.update(cli_parent_env(snapshot))
            await self._client.connect()
        finally:
            os.environ.clear()
            os.environ.update(snapshot)

    async def query(self, text: str) -> None:
        await self._client.query(text)

    def receive_turn(self) -> AsyncIterator[Any]:
        async def normalized() -> AsyncIterator[Any]:
            response = cast("Any", self._client.receive_response())
            async with contextlib.aclosing(response):
                async for message in response:
                    if isinstance(message, StreamEvent):
                        event = message.event
                        event_type = event.get("type") if isinstance(event, dict) else None
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
                            # provider payload. Only the bounded type and hashed
                            # assistant identity survive; identity is history-only
                            # and must never enter activity telemetry.
                            start = event.get("message") if event_type == "message_start" else None
                            yield PartialMessageBoundary(
                                event_type=event_type,
                                assistant_group=(
                                    _assistant_group(start.get("id"))
                                    if isinstance(start, dict)
                                    else None
                                ),
                            )
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

    def request_full_checkpoint(self) -> None:
        """Make the next native export self contained after a bounded write."""

        store = self._options.session_store
        if not isinstance(store, _SeededSessionStore):
            raise RuntimeError("native replay store is unavailable")
        store.request_full_checkpoint()

    async def ensure_mcp_server(self, name: str) -> bool:
        """Confirm the SDK session's own MCP connection to ``name`` (#2634).

        Reads the CLI's live status; a server not ``connected`` gets one
        ``reconnect_mcp_server`` and a re-read. Any exception is False, logged by
        class only: the SDK's message text can carry a header value.
        """

        try:
            if await self._mcp_server_status(name) == "connected":
                return True
            await self._client.reconnect_mcp_server(name)
            return await self._mcp_server_status(name) == "connected"
        except Exception as exc:  # noqa: BLE001 - an unconfirmed server stays excluded
            logger.warning(
                "mcp server reconnect check failed server=%s error_class=%s",
                name,
                type(exc).__name__,
            )
            return False

    async def _mcp_server_status(self, name: str) -> str | None:
        status = await self._client.get_mcp_status()
        for server in status.get("mcpServers", []):
            if server.get("name") == name:
                return server.get("status")
        return None
