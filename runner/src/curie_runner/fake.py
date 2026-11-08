"""A scripted ModelSession for tests and the conformance suite.

The fake is the mock at the adapter seam: it constructs real claude-agent-sdk
message dataclasses with canned content, so everything above it (translation,
budget, side-effect flagging, status, NDJSON, the HTTP layer) runs unmodified and
un-mocked while the model (the only external dependency) is replaced. It never
spawns the CLI or touches the network, with one explicit exception: a prompt
carrying ``[fake:connector-call]`` dials the hosted connectors the session was
handed (ADR 0270). ``aci-protocol`` is never mocked.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import socket
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

import anyio
import httpx2
from claude_agent_sdk import (
    AssistantMessage,
    ResultMessage,
    TextBlock,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)
from claude_agent_sdk.types import (
    CanUseTool,
    PermissionResultDeny,
    ToolPermissionContext,
)
from mcp import ClientSession

from .adapter import PartialMessageBoundary
from .approval import (
    APPROVAL_TOOL_NAME,
    TURN_PROGRESS_TOOL_NAME,
    ApprovalGate,
    process_approval_request,
)
from .connectors import CALLER_HEADER
from .history import ConversationMessage
from .mcp_tool_capability import _caller_refusal, _probe_error_leaves, server_streams
from .tool_access import TurnToolAccess
from .turn_progress import NOT_SHOWN_TEXT, TurnProgress


def _assistant(*blocks: Any, usage: dict[str, Any] | None = None) -> AssistantMessage:
    return AssistantMessage(content=list(blocks), model="fake-model", usage=usage)


def _result(
    *,
    text: str = "",
    is_error: bool = False,
    subtype: str = "success",
    usage: dict[str, Any] | None = None,
    terminal_reason: str | None = None,
) -> ResultMessage:
    return ResultMessage(
        subtype=subtype,
        duration_ms=1,
        duration_api_ms=1,
        is_error=is_error,
        num_turns=1,
        session_id="fake-session",
        result=text,
        usage=usage,
        terminal_reason=terminal_reason,
    )


def _tool_result(tool_use_id: str, content: Any, *, is_error: bool = False) -> UserMessage:
    """A tool's answer, on the message the SDK actually delivers it on.

    Tool results arrive on a ``UserMessage``, not on the assistant turn, which is
    why forwarding them is a separate branch in translation. A fake that skips
    this message leaves that branch unreachable offline.
    """

    return UserMessage(
        content=[ToolResultBlock(tool_use_id=tool_use_id, content=content, is_error=is_error)]
    )


def default_turn() -> list[Any]:
    """A representative successful turn: text, a side-effecting tool, then done.

    The tool answers in prose, because ``echo hi`` has nothing else to say. That
    is the honest default and it is also the not-undoable path: no structured
    reply means no prior state, so a recorded action reports that it cannot be
    put back rather than claiming it can.
    """

    return [
        _assistant(TextBlock(text="Looking into it")),
        _assistant(ToolUseBlock(id="t1", name="Bash", input={"command": "echo hi"})),
        _tool_result("t1", "hi\n"),
        _assistant(TextBlock(text="all done"), usage={"input_tokens": 20, "output_tokens": 8}),
        _result(text="all done", usage={"input_tokens": 20, "output_tokens": 8}),
    ]


# The other half of a write connector's contract: a reply that reports what it
# read and what it left, so an offline run can exercise a reversible action end
# to end (ADR-0117). Selected by marker like the approval turn below, because the
# default turn's honest answer is prose and both paths need to exist offline.
REVERSIBLE_MARKER = "[fake:reversible-tool]"

_REVERSIBLE_REPLY = {
    "ok": True,
    "summary": "scaled public/api from 3 to 10",
    # What a restore puts back, and what it is checked against. Not
    # interchangeable: comparing the live resource to ``prior`` would refuse
    # every safe undo and permit the one that is not.
    "prior": {"spec": {"replicas": 3}},
    "post": {"spec": {"replicas": 10}},
    "target": {"kind": "Deployment", "namespace": "public", "name": "api"},
}


def reversible_turn() -> list[Any]:
    """A turn whose side-effecting call reports a state that can be restored."""

    return [
        _assistant(TextBlock(text="Scaling it")),
        _assistant(
            ToolUseBlock(
                id="t1",
                name="scale_deployment",
                input={"namespace": "public", "name": "api", "replicas": 10},
            )
        ),
        _tool_result("t1", json.dumps(_REVERSIBLE_REPLY)),
        _assistant(
            TextBlock(text="scaled public/api from 3 to 10"),
            usage={"input_tokens": 20, "output_tokens": 8},
        ),
        _result(
            text="scaled public/api from 3 to 10",
            usage={"input_tokens": 20, "output_tokens": 8},
        ),
    ]


# Explicit test-only marker: a query containing it makes the fake's default
# script raise an approval request (ADR-0010), so the awaiting-approval
# lifecycle round-trips fully offline (CI, `curie skill up --fake-model`, the
# chart's sealed default pool). Everything after the marker on the same line is
# the approval summary; the routed form ``[fake:request-approval:managers]``
# additionally names an approval route (#247). Like all fake-model behavior:
# no model call, no network.
APPROVAL_MARKER = "[fake:request-approval]"
_APPROVAL_MARKER_RE = re.compile(r"\[fake:request-approval(?::([A-Za-z0-9_-]+))?\]")


def _user_message(prompt: str) -> str:
    """The person's words inside a sender frame, or the prompt when it is bare.

    The fake model judges markers in those words. The platform fence is not
    part of the request.
    """

    start_marker = "[user-message"
    end_marker = "[end-user-message"
    if start_marker not in prompt or end_marker not in prompt:
        return prompt
    start = prompt.index("\n", prompt.index(start_marker)) + 1
    end = prompt.index(end_marker)
    body = prompt[start:end]
    if body.endswith("\n"):
        body = body[:-1]
    return body


def approval_turn(summary: str, route: str | None = None) -> list[Any]:
    """A turn that calls the platform approval-request tool, then ends."""

    text = "This needs sign-off; requesting approval."
    payload: dict[str, Any] = {"summary": summary}
    if route is not None:
        payload["route"] = route
    return [
        _assistant(TextBlock(text=text)),
        _assistant(
            ToolUseBlock(
                id="t1",
                name="mcp__curie__request_approval",
                input=payload,
            )
        ),
        _result(text=text, usage={"input_tokens": 20, "output_tokens": 8}),
    ]


# Explicit test-only marker for deliberate progress (ADR 0130): the turn calls
# the platform progress tool three times through the SAME handler the SDK tool
# closes over, pausing between calls long enough for a person to steer it,
# then answers. Without a capability each call gets the tool's soft answer and
# nothing touches the network, like all fake-model behavior.
PROGRESS_DEMO_MARKER = "[fake:progress-demo]"
PROGRESS_DEMO_PAUSE_S = 2.0

_PROGRESS_DEMO_CALLS: tuple[dict[str, str], ...] = (
    {
        "update_id": "demo-investigating",
        "state": "investigating",
        "summary": "Looking into the request",
    },
    {
        "update_id": "demo-evidence",
        "state": "investigating",
        "summary": "Found the relevant evidence",
        "milestone": "evidence",
    },
    {
        "update_id": "demo-verification",
        "state": "testing",
        "summary": "Verified the result",
        "milestone": "verification",
    },
)


@dataclass(frozen=True)
class _Pause:
    """A scripted wait, so a live demo turn can be steered."""


@dataclass(frozen=True)
class _ProgressCall:
    """A scripted progress call, answered by running the real handler."""

    tool_use_id: str
    arguments: dict[str, str]


def progress_demo_turn() -> list[Any]:
    """A long turn that reports progress at three transitions, then answers."""

    script: list[Any] = [_assistant(TextBlock(text="Working through it"))]
    for index, arguments in enumerate(_PROGRESS_DEMO_CALLS, start=1):
        tool_use_id = f"p{index}"
        script.append(_Pause())
        script.append(
            _assistant(ToolUseBlock(id=tool_use_id, name=TURN_PROGRESS_TOOL_NAME, input=arguments))
        )
        script.append(_ProgressCall(tool_use_id=tool_use_id, arguments=arguments))
    script.append(_Pause())
    script.append(
        _assistant(TextBlock(text="all done"), usage={"input_tokens": 20, "output_tokens": 8})
    )
    script.append(_result(text="all done", usage={"input_tokens": 20, "output_tokens": 8}))
    return script


# Explicit test-only marker for ADR 0270 evidence: the turn dials every hosted
# connector the session was handed, in sorted name order, through the same
# standalone MCP client the boot probe uses (one ``initialize``, one
# ``tools/list``), and answers one line per connector:
#
#     connector-call <name>: ok tools=<n> caller=<fp>
#     connector-call <name>: failed <reason> caller=<fp>
#
# ``<fp>`` is the first 16 hex characters of the sha256 of the X-Curie-Caller
# value that connector was dialled with, or ``-`` without one, so a run can tie
# a dial to its token without the token being printed. ``<reason>`` is the
# caller proxy's refusal code when it refused, else the failure's exception
# class name; neither ever carries exception text. With no connectors the
# answer is ``connector-call: none``. This is the fake's only network call.
CONNECTOR_CALL_MARKER = "[fake:connector-call]"
# Per connector, read at call time. One dial bounds a connector that accepts
# and then hangs, so the turn always completes.
CONNECTOR_CALL_TIMEOUT_S = 15.0
_CONNECTOR_CALL_USAGE = {"input_tokens": 20, "output_tokens": 8}


@dataclass(frozen=True)
class _ConnectorCalls:
    """A scripted dial of every connector, answered as the turn's closing text."""


def connector_call_turn() -> list[Any]:
    """A turn that dials the session's connectors, then answers one line each."""

    return [_assistant(TextBlock(text="Calling the connectors")), _ConnectorCalls()]


def _connector_call_answer(text: str) -> list[Any]:
    return [
        _assistant(TextBlock(text=text), usage=dict(_CONNECTOR_CALL_USAGE)),
        _result(text=text, usage=dict(_CONNECTOR_CALL_USAGE)),
    ]


def _caller_fingerprint(config: Mapping[str, Any]) -> str:
    """The sent X-Curie-Caller value's sha256, first 16 hex characters, or ``-``.

    ``server_streams`` expands ``${NAME}`` against the empty env this dial
    passes, which leaves the value as written, so this is what was sent.
    """

    headers = config.get("headers")
    value = headers.get(CALLER_HEADER) if isinstance(headers, Mapping) else None
    if value is None:
        return "-"
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _connector_failure(exc: BaseException) -> str:
    """The proxy's refusal code, or the innermost failure's class name.

    Never the exception text: it can carry a header value.
    """

    refusal = _caller_refusal(exc)
    if refusal is not None:
        return refusal
    leaves = _probe_error_leaves(exc)
    return type(leaves[0] if leaves else exc).__name__


CONNECTOR_CALL_ATTEMPTS = 3
_CONNECTOR_CALL_BACKOFF_S = 0.2
# mcp's HTTP transport is httpx2 (installed with mcp; the runner image has no
# httpx): a dial that never reached the server arrives as an exception group
# whose leaf is one of these.
_TRANSPORT_FAILURES = (
    httpx2.ConnectError,
    httpx2.ConnectTimeout,
    ConnectionResetError,
    socket.gaierror,
)


def _failed_before_answer(exc: BaseException) -> bool:
    """True when every leaf is a transport failure and the proxy did not refuse."""

    if _caller_refusal(exc) is not None:
        return False
    leaves = _probe_error_leaves(exc)
    return bool(leaves) and all(isinstance(leaf, _TRANSPORT_FAILURES) for leaf in leaves)


async def _dial_connector(config: Mapping[str, Any], timeout: float) -> int:
    async with server_streams(config, plugin_dir=None, inherited_env={}) as (
        read_stream,
        write_stream,
    ):
        async with ClientSession(
            read_stream, write_stream, read_timeout_seconds=timeout
        ) as session:
            await session.initialize()
            listed = await session.list_tools()
    return len(listed.tools)


async def _retry_dial(dial: Callable[[float], Awaitable[Any]]) -> Any:
    """Run one bounded dial, retrying transport failures before any answer."""

    timeout = CONNECTOR_CALL_TIMEOUT_S
    with anyio.fail_after(timeout):
        attempt = 1
        while True:
            try:
                return await dial(timeout)
            except Exception as exc:  # noqa: BLE001 - classified below
                if attempt >= CONNECTOR_CALL_ATTEMPTS or not _failed_before_answer(exc):
                    raise
                await anyio.sleep(_CONNECTOR_CALL_BACKOFF_S * attempt)
                attempt += 1


async def _call_connector(config: Mapping[str, Any]) -> str:
    """One bounded dial: ``ok tools=<n>`` or ``failed <reason>``.

    A transport failure before the connector answers is retried, up to
    ``CONNECTOR_CALL_ATTEMPTS`` inside the one per-server timeout. A proxy
    refusal or any answered protocol error is reported at once.
    """

    try:
        tools = await _retry_dial(lambda timeout: _dial_connector(config, timeout))
    except Exception as exc:  # noqa: BLE001 - every failure is reported, none escapes
        return f"failed {_connector_failure(exc)}"
    return f"ok tools={tools}"


# ``[fake:connector-tool <connector> <tool> <json-object-args>]`` dials exactly
# one hosted connector, runs ``initialize`` then ``tools/call``, and answers one
# line (ADR 0270 identity E2E):
#
#     connector-tool <connector>/<tool>: ok <first line of first text> caller=<fp>
#     connector-tool <connector>/<tool>: refused <code> caller=<fp>
#     connector-tool <connector>/<tool>: failed <reason> caller=<fp>
#     connector-tool <connector>: failed unknown_connector
#     connector-tool: failed bad_directive
#
# ``[fake:forge-principal <value>]`` adds the literal ``X-Curie-Principal``
# header to that dial only, on a copy of the config.
CONNECTOR_TOOL_MARKER = "[fake:connector-tool"
_CONNECTOR_TOOL_RE = re.compile(r"\[fake:connector-tool (\S+) (\S+) (\{.*\})\]")
_FORGE_PRINCIPAL_RE = re.compile(r"\[fake:forge-principal (\S+)\]")
_FORGED_PRINCIPAL_HEADER = "X-Curie-Principal"


@dataclass(frozen=True)
class _ConnectorToolCall:
    """A scripted single tool call, answered as the turn's closing text."""

    connector: str | None
    tool: str = ""
    args: Mapping[str, Any] | None = None
    forged_principal: str | None = None


def connector_tool_turn(last: str) -> list[Any]:
    """A turn that calls one connector tool as the person's message directs."""

    match = _CONNECTOR_TOOL_RE.search(last)
    call = _ConnectorToolCall(connector=None)
    if match:
        try:
            args = json.loads(match.group(3))
        except ValueError:
            args = None
        if isinstance(args, dict):
            forge = _FORGE_PRINCIPAL_RE.search(last)
            call = _ConnectorToolCall(
                connector=match.group(1),
                tool=match.group(2),
                args=args,
                forged_principal=forge.group(1) if forge else None,
            )
    return [_assistant(TextBlock(text="Calling the connector tool")), call]


async def _dial_connector_tool(
    config: Mapping[str, Any], tool: str, args: Mapping[str, Any], timeout: float
) -> tuple[bool, str]:
    async with server_streams(config, plugin_dir=None, inherited_env={}) as (
        read_stream,
        write_stream,
    ):
        async with ClientSession(
            read_stream, write_stream, read_timeout_seconds=timeout
        ) as session:
            await session.initialize()
            result = await session.call_tool(tool, dict(args))
    content = getattr(result, "content", None) or []
    first = next(
        (
            block.text
            for block in content
            if getattr(block, "type", None) == "text" and isinstance(block.text, str)
        ),
        "",
    )
    return bool(getattr(result, "is_error", False)), first.split("\n", 1)[0].strip()


async def call_connector_tool(
    servers: Mapping[str, Mapping[str, Any]], call: _ConnectorToolCall
) -> str:
    """The ``[fake:connector-tool]`` answer: exactly one line."""

    if call.connector is None or call.args is None:
        return "connector-tool: failed bad_directive"
    base = servers.get(call.connector)
    if base is None:
        return f"connector-tool {call.connector}: failed unknown_connector"
    config: Mapping[str, Any] = base
    if call.forged_principal is not None:
        forged = copy.deepcopy(dict(base))
        headers = forged.get("headers")
        forged["headers"] = {
            **(dict(headers) if isinstance(headers, Mapping) else {}),
            _FORGED_PRINCIPAL_HEADER: call.forged_principal,
        }
        config = forged
    label = f"connector-tool {call.connector}/{call.tool}"
    caller = _caller_fingerprint(config)
    args = call.args
    tool = call.tool
    try:
        is_error, first = await _retry_dial(
            lambda timeout: _dial_connector_tool(config, tool, args, timeout)
        )
    except Exception as exc:  # noqa: BLE001 - every failure is reported, none escapes
        return f"{label}: failed {_connector_failure(exc)} caller={caller}"
    if not is_error:
        return f"{label}: ok {first} caller={caller}"
    if first.startswith("refused ") and len(first.split()) == 2:
        return f"{label}: refused {first.split()[1]} caller={caller}"
    return f"{label}: failed {first or 'tool_error'} caller={caller}"


async def call_connectors(servers: Mapping[str, Mapping[str, Any]]) -> str:
    """The ``[fake:connector-call]`` answer: one line per connector, by name."""

    if not servers:
        return "connector-call: none"
    lines = []
    for name in sorted(servers):
        config = servers[name]
        outcome = await _call_connector(config)
        lines.append(f"connector-call {name}: {outcome} caller={_caller_fingerprint(config)}")
    return "\n".join(lines)


class FakeModelSession:
    """A ModelSession that replays a fixed script of SDK messages per turn.

    ``script_factory`` returns the messages for the next ``receive_turn``, so a
    test can vary the script across turns. ``interrupt`` truncates the current
    turn's replay at the next boundary (``truncate_on_interrupt=True``, the
    default), emulating an SDK interrupt that aborts the iterator before a result;
    set it False to model the other real shape, where the SDK still delivers a
    terminal error result after the interrupt.

    ``emit_partial_boundaries`` is an opt-in telemetry fixture. When enabled, a
    payload-free message-start boundary immediately precedes each scripted
    assistant message. The default remains the historical exact script replay.

    ``can_use_tool`` (the #245 permission gate) lets the offline path exercise the
    same intercept the SDK applies: before each scripted ``ToolUseBlock`` is
    yielded, the permission callback runs and its DECISION is honored -- a gated
    tool's deny records the block on the shared gate (flipping the turn to
    awaiting-approval), and a deny carrying ``interrupt=True`` additionally ends
    the replay, the same way the real CLI aborts the turn (#1852; see
    ``_apply_gate``). The block itself is delivered unchanged -- matching the
    real SDK, which emits the ``tool_use`` even for a denied call -- so a gated
    turn still surfaces the tool note it would in production. Defaults None, so
    a fake constructed without it behaves exactly as before. Bundle PreToolUse
    command hooks (#272) are NOT run here: they shell out and would break the
    fake's offline no-op guarantee. Tests may inject Curie's in-process
    ``pre_tool_use_hook`` callback explicitly to exercise the real hook decision
    shape without running bundle code.

    ``tool_access`` is the session's shared per-turn tool access
    (RUNNER-TOOL-ACCESS-8). A call it refuses is decided first, before any
    emulated tool (the approval request) runs, and is answered the way the real
    CLI answers a PreToolUse deny: an error result carrying the refusal, in
    place of whatever result the script had for that call. The turn continues.

    ``connector_servers`` are the derived hosted connector entries the caller
    proxy fronts, headers already materialized. Only a prompt carrying
    ``CONNECTOR_CALL_MARKER`` reads them; without it nothing is dialled.
    """

    def __init__(
        self,
        script_factory: Callable[[], list[Any]] | None = None,
        *,
        truncate_on_interrupt: bool = True,
        can_use_tool: CanUseTool | None = None,
        pre_tool_use_hook: Callable[[Any, str | None, Any], Awaitable[dict[str, Any]]]
        | None = None,
        approval_gate: ApprovalGate | None = None,
        replay_messages: tuple[ConversationMessage, ...] = (),
        emit_partial_boundaries: bool = False,
        disallowed_tools: list[str] | tuple[str, ...] | None = None,
        turn_progress: TurnProgress | None = None,
        tool_access: TurnToolAccess | None = None,
        connector_servers: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> None:
        self._script_factory = script_factory or self._default_script
        self._connector_servers: Mapping[str, Mapping[str, Any]] = connector_servers or {}
        # The session's progress holder (ADR 0130); None when the tool is not
        # mounted, which the demo answers as "not shown", like no capability.
        self._turn_progress = turn_progress
        self._tool_access = tool_access
        # Per turn: the calls ``tool_access`` refused, whose scripted results are
        # replaced, and the refusal results not yet delivered.
        self._refused_ids: set[str] = set()
        self._pending_refusals: list[UserMessage] = []
        self._truncate_on_interrupt = truncate_on_interrupt
        self._can_use_tool = can_use_tool
        self._pre_tool_use_hook = pre_tool_use_hook
        self._emit_partial_boundaries = emit_partial_boundaries
        self._disallowed_tools = tuple(disallowed_tools or ())
        # The shared policy gate (#561): a scripted request_approval block must
        # run the SAME route-resolution decision table the real MCP tool does, or
        # the fake tier omits the sole-route auto-bind / unknown-route refusal and
        # silently widens the card -- the exact real-path regression #544 closed.
        self._approval_gate = approval_gate
        self.replay_messages = replay_messages
        self.connected = False
        self.queries: list[str] = []
        self.interrupts = 0
        self._interrupted = False
        # Set when the permission callback denies WITH interrupt=True (#1852):
        # the real CLI aborts the turn on that flag, so the replay must stop too
        # or the offline tier silently models a gate weaker than production's.
        # Strictly per-turn -- reset in ``query`` beside ``_interrupted``, so a
        # halt can never truncate an unrelated later replay.
        self._halted = False

    def _default_script(self) -> list[Any]:
        """The default per-turn script, branching on the approval marker.

        A custom ``script_factory`` bypasses this entirely, so existing tests
        keep their exact scripts; only the no-factory default (the container
        fake-model path) reacts to the marker.
        """

        last = _user_message(self.queries[-1] if self.queries else "")
        match = _APPROVAL_MARKER_RE.search(last)
        if match:
            summary = last[match.end() :].strip() or "unspecified request"
            return approval_turn(summary, route=match.group(1))
        if REVERSIBLE_MARKER in last:
            return reversible_turn()
        if PROGRESS_DEMO_MARKER in last:
            return progress_demo_turn()
        if CONNECTOR_TOOL_MARKER in last:
            return connector_tool_turn(last)
        if CONNECTOR_CALL_MARKER in last:
            return connector_call_turn()
        return default_turn()

    async def connect(self) -> None:
        self.connected = True

    async def query(self, text: str) -> None:
        self.queries.append(text)
        self._interrupted = False
        self._halted = False
        self._refused_ids = set()
        self._pending_refusals = []

    async def interrupt(self) -> None:
        self.interrupts += 1
        self._interrupted = True

    async def receive_turn(self) -> AsyncIterator[Any]:
        script = deque(self._script_factory())
        while script:
            scripted = script.popleft()
            if self._interrupted and self._truncate_on_interrupt:
                return
            message = self._without_refused_results(scripted)
            if message is None:
                continue
            if isinstance(message, _ConnectorCalls):
                # The answer exists only once every connector has answered, so
                # the dial expands in place into the turn's closing messages,
                # which then take the same path as any scripted message.
                answer = await call_connectors(self._connector_servers)
                script.extendleft(reversed(_connector_call_answer(answer)))
                continue
            if isinstance(message, _ConnectorToolCall):
                answer = await call_connector_tool(self._connector_servers, message)
                script.extendleft(reversed(_connector_call_answer(answer)))
                continue
            if isinstance(message, _Pause):
                # Read at call time, so a test can shorten the demo's waits.
                await anyio.sleep(PROGRESS_DEMO_PAUSE_S)
                continue
            if isinstance(message, _ProgressCall):
                # A refused call already got its refusal from the gate; the
                # handler's answer would be a second result for it.
                if message.tool_use_id not in self._refused_ids:
                    yield await self._answer_progress(message)
                continue
            denied_messages = await self._apply_gate(message)
            if self._emit_partial_boundaries and isinstance(message, AssistantMessage):
                yield PartialMessageBoundary(event_type="message_start")
            yield message
            refusals, self._pending_refusals = self._pending_refusals, []
            for refusal in refusals:
                yield refusal
            if denied_messages is not None:
                for denied_message in denied_messages:
                    yield denied_message
                return
            if self._halted:
                # The denied ToolUseBlock above IS delivered (the real SDK emits
                # the tool_use even for a denied call), and nothing after it is:
                # an interrupting deny aborts the turn, so any scripted text or
                # terminal result that followed never happens (#1852). This is a
                # separate mechanism from the ``_interrupted`` truncation above,
                # which models an OPERATOR stop and must keep working on its own.
                return

    async def _answer_progress(self, call: _ProgressCall) -> UserMessage:
        """The offline fake acknowledges the marker without any network I/O."""

        return _tool_result(call.tool_use_id, [{"type": "text", "text": NOT_SHOWN_TEXT}])

    async def _apply_gate(self, message: Any) -> tuple[UserMessage, ResultMessage] | None:
        """Run the permission gate over each ToolUseBlock and honor its decision.

        Mirrors the SDK: the gate decides a call before it executes, and a gated
        deny records the block on the shared ``ApprovalGate`` (flipping the turn to
        awaiting-approval). The block is delivered unchanged either way -- the real
        SDK emits the ``tool_use`` before the permission decision, so a denied call
        still surfaces as a tool note. A no-op unless a gate or a tool access is
        configured, keeping the un-gated, unrestricted fake unchanged.

        The callback's return value is READ, not discarded (#1852): a
        ``PermissionResultDeny`` with ``interrupt=True`` is forwarded by the SDK
        to the CLI as ``response_data["interrupt"]``
        (``claude_agent_sdk/_internal/query.py:474-477``), which aborts the turn.
        Recording it here is what lets the offline tier prove the halt rather
        than replay past a call production would have stopped on. A deny WITHOUT
        the flag is unchanged: the replay continues and the scripted terminal
        result still arrives.
        """

        if not isinstance(message, AssistantMessage):
            return None
        for block in message.content:
            if not isinstance(block, ToolUseBlock):
                continue
            if self._tool_access is not None:
                # @spec RUNNER-TOOL-ACCESS-8: the read-only decision comes before
                # anything this fake emulates, as the front does on the real path.
                reason = self._tool_access.refuse(block.name, block.id)
                if reason is not None:
                    self._refused_ids.add(block.id)
                    self._pending_refusals.append(
                        _tool_result(
                            block.id,
                            f"PreToolUse:{block.name} hook error: {reason}",
                            is_error=True,
                        )
                    )
                    continue
            if block.name == APPROVAL_TOOL_NAME and self._approval_gate is not None:
                # Run the real decision table so the container fake tier resolves
                # the route (sole-route auto-bind, unknown-route refusal) and sets
                # the same sticky gate flags _merge_gate_block reconciles (#561).
                # build_can_use_tool below leaves the (un-gated) approval tool
                # untouched, so this is the only thing that sets the policy fields.
                payload = block.input if isinstance(block.input, dict) else {}
                process_approval_request(self._approval_gate, payload)
            if block.name in self._disallowed_tools:
                # Mirror ClaudeAgentOptions.disallowed_tools: the named tool is
                # not usable. The ToolUseBlock is still delivered (the real SDK
                # emits it), then the turn stops so the scripted result cannot
                # execute the write (#2429).
                self._halted = True
                continue
            if self._pre_tool_use_hook is not None:
                hook_output = await self._pre_tool_use_hook(
                    {"tool_name": block.name, "tool_input": block.input},
                    block.id,
                    {"signal": None},
                )
                specific = hook_output.get("hookSpecificOutput", {})
                if (
                    specific.get("permissionDecision") == "deny"
                    and hook_output.get("continue_") is False
                ):
                    reason = specific.get("permissionDecisionReason")
                    if not isinstance(reason, str) or not reason:
                        reason = hook_output.get("stopReason")
                    if not isinstance(reason, str) or not reason:
                        reason = "tool use denied"
                    return (
                        _tool_result(
                            block.id,
                            f"PreToolUse:{block.name} hook error: {reason}",
                            is_error=True,
                        ),
                        _result(terminal_reason="hook_stopped"),
                    )
                if specific.get("permissionDecision") == "allow":
                    continue
            if self._can_use_tool is not None:
                decision = await self._can_use_tool(
                    block.name, block.input, ToolPermissionContext(tool_use_id=block.id)
                )
                if isinstance(decision, PermissionResultDeny) and decision.interrupt:
                    return (
                        _tool_result(
                            block.id,
                            "The user doesn't want to proceed with this tool use. "
                            "The tool use was rejected.",
                            is_error=True,
                        ),
                        _result(is_error=True, subtype="error_during_execution"),
                    )
        return None

    def _without_refused_results(self, message: Any) -> Any:
        """``message`` less any scripted result for a call this turn refused."""

        if not self._refused_ids or not isinstance(message, UserMessage):
            return message
        if isinstance(message.content, str):
            return message
        kept = [
            block
            for block in message.content
            if not (isinstance(block, ToolResultBlock) and block.tool_use_id in self._refused_ids)
        ]
        if not kept:
            return None
        return UserMessage(content=kept)

    async def close(self) -> None:
        self.connected = False
