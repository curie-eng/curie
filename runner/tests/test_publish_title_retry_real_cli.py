"""A titleless publish_changes retries in the same turn, on the real CLI (#4460).

The malformed-publication refusal tells the model to "Correct it and retry",
but before #4460 both gate layers stopped the turn while saying so: the
PreToolUse hook answered the deny with ``continue_`` False plus a
``stopReason``, and the permission callback denied with ``interrupt=True``.
The turn then ended before any retry, and because nothing was recorded the
session failed the turn closed as ``publication-unrecorded`` -- a model that
did exactly what the refusal asked lost the whole run to it.

This case puts the real bundled CLI between a ``SessionRunner`` and a local
stand-in for /v1/messages: no credential and no network. The stand-in answers
the main model's first request with a ``publish_changes`` call that has no
title, and the request that carries that call's tool result with a corrected
call that has one. The gate is wired the way ``__main__`` wires it: the
managed-workspace approval gate, its hook, its permission callback and its
in-process MCP server, so ``mcp__curie__publish_changes`` exists as a real
tool the CLI can call.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.approval import APPROVAL_SERVER_NAME, build_approval_gate
from curie_runner.harness.claude.approval import (
    build_approval_hook,
    build_approval_server,
    build_can_use_tool,
)
from curie_runner.session import SessionRunner
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

_MAIN_MODEL = "claude-sonnet-5"
# The stand-in never checks it; the CLI only needs a key to send.
_PLACEHOLDER_KEY = "sk-ant-placeholder"
_TITLELESS_CALL_ID = "toolu_acme_titleless1"
_RETRY_CALL_ID = "toolu_acme_retry1"
_RETRY_TITLE = "Ship the documentation update (#4460)"
_BODY = "Explain the requested documentation changes.\n"
_SESSION_ID = "session-PLACEHOLDER-publish-retry"
_SYSTEM_PROMPT = "You publish the requested changes."
_TURN_TIMEOUT_S = 120

_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_EMITTED = web.AppKey("emitted", list[tuple[str, dict[str, Any]]])

Block = list[tuple[str, dict[str, Any]]]


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _messages_of(body: dict[str, Any]) -> list[dict[str, Any]]:
    return [message for message in body.get("messages") or () if isinstance(message, dict)]


def _blocks(message: dict[str, Any]) -> list[dict[str, Any]]:
    content = message.get("content")
    if isinstance(content, list):
        return [block for block in content if isinstance(block, dict)]
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    return []


def _tool_results(body: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        block.get("tool_use_id"): block
        for message in _messages_of(body)
        if message.get("role") == "user"
        for block in _blocks(message)
        if block.get("type") == "tool_result"
    }


def _message_head(body: dict[str, Any], index: int) -> dict[str, Any]:
    return {
        "id": f"msg_stand_in_{index}",
        "type": "message",
        "role": "assistant",
        "model": body.get("model", "stand-in"),
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }


def _text_block(text: str) -> Block:
    return [
        (
            "content_block_start",
            {"type": "content_block_start", "content_block": {"type": "text", "text": ""}},
        ),
        (
            "content_block_delta",
            {"type": "content_block_delta", "delta": {"type": "text_delta", "text": text}},
        ),
    ]


def _tool_block(call_id: str, name: str, arguments: dict[str, Any]) -> Block:
    return [
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "content_block": {"type": "tool_use", "id": call_id, "name": name, "input": {}},
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "delta": {"type": "input_json_delta", "partial_json": json.dumps(arguments)},
            },
        ),
    ]


async def _stream(
    request: web.Request, body: dict[str, Any], blocks: list[Block], stop_reason: str
) -> web.StreamResponse:
    """One streamed assistant message holding ``blocks`` in order."""

    response = web.StreamResponse(headers={"content-type": "text/event-stream"})
    await response.prepare(request)
    head = _message_head(body, len(request.app[_BODIES]))
    await response.write(_sse("message_start", {"type": "message_start", "message": head}))
    for index, block in enumerate(blocks):
        for event, data in block:
            await response.write(_sse(event, {**data, "index": index}))
        await response.write(
            _sse("content_block_stop", {"type": "content_block_stop", "index": index})
        )
    for event, data in (
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ):
        await response.write(_sse(event, data))
    await response.write_eof()
    return response


async def _messages(request: web.Request) -> web.StreamResponse:
    """Main: a titleless publish call, then the corrected one, then text."""

    body = await request.json()
    request.app[_BODIES].append(body)
    if not body.get("tools"):
        # A CLI side request (title and the like): answer with plain text.
        if not body.get("stream"):
            message = _message_head(body, len(request.app[_BODIES]))
            message["content"] = [{"type": "text", "text": "acme"}]
            message["stop_reason"] = "end_turn"
            return web.json_response(message)
        return await _stream(request, body, [_text_block("acme")], "end_turn")
    answered = _tool_results(body)
    if _RETRY_CALL_ID in answered:
        return await _stream(request, body, [_text_block("publication requested")], "end_turn")
    if _TITLELESS_CALL_ID in answered:
        # The deny reached the model as a tool result: retry with a title.
        arguments = {"title": _RETRY_TITLE, "body": _BODY}
        request.app[_EMITTED].append((_RETRY_CALL_ID, arguments))
        retry = _tool_block(_RETRY_CALL_ID, PLATFORM_PUBLISH_TOOL_NAME, arguments)
        return await _stream(request, body, [retry], "tool_use")
    arguments = {"body": _BODY}
    request.app[_EMITTED].append((_TITLELESS_CALL_ID, arguments))
    titleless = _tool_block(_TITLELESS_CALL_ID, PLATFORM_PUBLISH_TOOL_NAME, arguments)
    return await _stream(request, body, [titleless], "tool_use")


async def _anything_else(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


def _is_main_request(body: dict[str, Any]) -> bool:
    return body.get("model") == _MAIN_MODEL and bool(body.get("tools"))


async def _run_one_turn(*, base_url: str, cwd: Path, gate: Any) -> list[Any]:
    """Boot one runner the way ``__main__`` does and run a single turn on it."""

    options = build_options(
        plugins=[],
        model=_MAIN_MODEL,
        system_prompt=_SYSTEM_PROMPT,
        max_turns=6,
        max_budget_usd=None,
        resume=None,
        cwd=str(cwd),
        env={
            "ANTHROPIC_API_KEY": _PLACEHOLDER_KEY,
            "ANTHROPIC_BASE_URL": base_url,
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "DISABLE_TELEMETRY": "1",
        },
        hooks=build_approval_hook(gate),
        mcp_servers={APPROVAL_SERVER_NAME: build_approval_server(gate, managed_workspace=True)},
        can_use_tool=build_can_use_tool(gate),
    )
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: ClaudeAgentSession(options),
        ceiling=0,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name=f"curie-run:{_SESSION_ID}",
        session_id=_SESSION_ID,
        model=_MAIN_MODEL,
        approval_gate=gate,
    )
    lines: list[str] = []
    await runner.start()
    try:
        with anyio.fail_after(_TURN_TIMEOUT_S):
            async for line in runner.run_turn(
                Event(type="message", text="Publish the changes.", user="U0EXAMPLE1", ts="1"),
            ):
                lines.append(line)
    finally:
        await runner.close()
    return list(parse_ndjson("".join(lines)))


def _single_final(events: list[Any]) -> Final:
    finals = [event for event in events if isinstance(event, Final)]
    assert len(finals) == 1, events
    return finals[0]


def test_a_titleless_publish_denies_and_the_retry_records_the_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC 3: the corrected retry pauses the run awaiting approval.

    Red before #4460 on the final status: the first deny stopped the turn
    (``continue_`` False in the hook, ``interrupt=True`` in the callback), the
    retry in the stand-in's next answer was never requested, and the turn
    ended classified-failure ``publication-unrecorded`` instead of
    awaiting-approval with the corrected request recorded.
    """

    from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION

    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    (cwd / "README.md").write_text("Original documentation.\n", encoding="utf-8")
    gate = build_approval_gate(operator_tools=None, policy_routes={}, managed_workspace=True)
    assert gate is not None
    assert PLATFORM_PUBLISH_TOOL_NAME in gate.required

    async def scenario() -> tuple[
        list[Any], list[dict[str, Any]], list[tuple[str, dict[str, Any]]]
    ]:
        app = web.Application()
        app[_BODIES] = []
        app[_EMITTED] = []
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            base_url = str(server.make_url("")).rstrip("/")
            monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
            events = await _run_one_turn(base_url=base_url, cwd=cwd, gate=gate)
            return events, list(app[_BODIES]), list(app[_EMITTED])

    events, bodies, emitted = anyio.run(scenario)

    # The model issued the titleless call and then, in the SAME turn after the
    # deny came back to it, the corrected one. Red before #4460: the first deny
    # stopped the turn, so the corrected call was never requested.
    assert emitted == [
        (_TITLELESS_CALL_ID, {"body": _BODY}),
        (_RETRY_CALL_ID, {"title": _RETRY_TITLE, "body": _BODY}),
    ], emitted

    # The deny really was delivered back to the model as the titleless call's
    # tool result, phrased as the correction it asks the model to make.
    main = [body for body in bodies if _is_main_request(body)]
    assert main, bodies
    first_result = next(
        block
        for body in main
        for tool_use_id, block in _tool_results(body).items()
        if tool_use_id == _TITLELESS_CALL_ID
    )
    assert "publication title must be 1-240 characters" in json.dumps(first_result.get("content"))
    assert "Correct it and retry." in json.dumps(first_result.get("content"))

    # And the corrected request is what the run parked on.
    final = _single_final(events)
    assert final.status is SessionStatus.AWAITING_APPROVAL, final
    assert final.status is not SessionStatus.CLASSIFIED_FAILURE
    assert not any(
        getattr(event, "classification", None) == PUBLICATION_UNRECORDED_CLASSIFICATION
        for event in events
    ), events
    assert final.approval_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
    assert gate.publication_title == _RETRY_TITLE
    assert gate.publication_body == _BODY
    assert gate.pending_halt is True
