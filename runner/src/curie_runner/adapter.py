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
import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any, Protocol, cast, runtime_checkable

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKClient,
    HookMatcher,
    SdkPluginConfig,
    StreamEvent,
    TaskBudget,
)
from claude_agent_sdk.types import CanUseTool, McpSdkServerConfig, PermissionMode

from .mcp_argv import install as install_mcp_argv_offload

install_mcp_argv_offload()

logger = logging.getLogger(__name__)

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
    thinking: dict[str, Any] | None = None,
    task_budget_hint: int | None = None,
    env: dict[str, str] | None = None,
    hooks: dict[str, list[HookMatcher]] | None = None,
    mcp_servers: dict[str, McpSdkServerConfig] | None = None,
    can_use_tool: CanUseTool | None = None,
    cwd: str | None = None,
    disallowed_tools: list[str] | tuple[str, ...] | None = None,
) -> ClaudeAgentOptions:
    """Assemble ClaudeAgentOptions for the session.

    ``resume`` is the rehydrate path (ADR-0003, stateless-first): when a history
    ref is supplied it is passed as the SDK ``resume`` session id so a resumed
    thread reconstructs its history from the store rather than assuming a
    surviving in-RAM process.

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
    return ClaudeAgentOptions(
        plugins=plugins,
        model=model,
        **thinking_option,
        **cwd_option,
        system_prompt=system_prompt,
        max_turns=max_turns,
        max_budget_usd=max_budget_usd,
        resume=resume,
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
        # Optional operator deny list (#2429). Empty/None keeps the SDK default
        # (no tools removed). Names are removed from the model context and cannot
        # be used even under bypassPermissions. This removes a tool, not the
        # capability behind it: it does not revoke CURIE_STATE_TOKEN, which a
        # still-permitted tool can still use to reach the same API directly.
        disallowed_tools=list(disallowed_tools or ()),
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
