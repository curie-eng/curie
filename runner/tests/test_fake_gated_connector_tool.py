"""``[fake:gated-connector-tool <connector> <tool> <json-object>]`` (ADR 0271).

The directive makes the default script emit one ToolUseBlock named
``mcp__<connector>__<tool>`` carrying exactly the JSON object, run through the
permission gate like any scripted call. A gate that requires the tool records a
pending approval and the turn awaits it. The directive only raises the gate;
nothing is dialled.
"""

from __future__ import annotations

import json
from typing import Any

import anyio
import pytest
from aci_protocol import Event, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk.types import PermissionResult, ToolPermissionContext
from curie_runner.approval import ApprovalGate
from curie_runner.connectors import CALLER_HEADER
from curie_runner.fake import FakeModelSession
from curie_runner.harness.claude.approval import build_can_use_tool
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier

_LIVE_NAME = "mcp__human-restricted__execute_approved"
_ARGS = {"note": "synthetic-note", "nested": {"approval_ticket": "cat1.x.y"}, "n": 3}


def _gated(connector: str, tool: str, args: str) -> str:
    return f"[fake:gated-connector-tool {connector} {tool} {args}]"


class _Double:
    """A connector server double that records every request it receives."""

    def __init__(self) -> None:
        self.requests: list[str] = []
        self.url = ""


async def _run(
    prompt: str, *, required: frozenset[str], double: _Double | None = None
) -> tuple[list[Any], ApprovalGate, list[tuple[str, dict[str, Any]]], FakeModelSession]:
    gate = ApprovalGate(required=required, route_by_tool={t: "ops" for t in required})
    seen: list[tuple[str, dict[str, Any]]] = []
    inner = build_can_use_tool(gate)

    async def recording(
        name: str, tool_input: dict[str, Any], ctx: ToolPermissionContext
    ) -> PermissionResult:
        seen.append((name, tool_input))
        return await inner(name, tool_input, ctx)

    servers: dict[str, dict[str, Any]] = {}
    server: TestServer | None = None
    if double is not None:

        async def answer(request: web.Request) -> web.Response:
            double.requests.append(request.path)
            return web.Response(status=500)

        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", answer)
        server = TestServer(app, host="127.0.0.1")
        await server.start_server()
        double.url = str(server.make_url("/mcp"))
        servers = {
            "human-restricted": {
                "type": "http",
                "url": double.url,
                "headers": {CALLER_HEADER: "cct.synthetic"},
            }
        }
    session = FakeModelSession(can_use_tool=recording, connector_servers=servers)
    runner = SessionRunner(
        max_usd_per_day=None,
        held_secrets=frozenset(),
        session_factory=lambda: session,
        ceiling=10_000,
        tracer=RunTracer(None),
        classifier=SideEffectClassifier(),
        trace_name="test",
        session_id="s-1",
        approval_gate=gate,
    )
    try:
        await runner.start()
        lines = [
            line
            async for line in runner.run_turn(
                Event(type="message", text=prompt, user="U1", ts="1.1")
            )
        ]
    finally:
        if server is not None:
            await server.close()
    return parse_ndjson("".join(lines)), gate, seen, session


def _drive(
    prompt: str, **kw: Any
) -> tuple[list[Any], ApprovalGate, list[tuple[str, dict[str, Any]]]]:
    events, gate, seen, _ = anyio.run(lambda: _run(prompt, **kw))
    return events, gate, seen


def test_a_required_tool_records_a_pending_approval_with_the_exact_object() -> None:
    prompt = _gated("human-restricted", "execute_approved", json.dumps(_ARGS))
    events, gate, seen = _drive(prompt, required=frozenset({_LIVE_NAME}))

    assert seen == [(_LIVE_NAME, _ARGS)]
    assert gate.pending_gate_kind == "permission"
    assert gate.pending_granted_tool == _LIVE_NAME
    assert gate.pending_granted_arguments == _ARGS
    final = events[-1]
    assert final.type == "final"
    assert final.status == SessionStatus.AWAITING_APPROVAL
    assert final.approval_granted_tool == _LIVE_NAME
    assert final.approval_granted_arguments == _ARGS


def test_the_directive_is_not_a_dial_and_no_connector_server_is_contacted() -> None:
    double = _Double()
    prompt = _gated("human-restricted", "execute_approved", json.dumps(_ARGS))
    events, _gate, seen = _drive(prompt, required=frozenset({_LIVE_NAME}), double=double)

    assert seen == [(_LIVE_NAME, _ARGS)]
    assert events[-1].status == SessionStatus.AWAITING_APPROVAL
    assert double.requests == []


def test_an_ungated_tool_passes_the_gate_and_still_dials_nothing() -> None:
    double = _Double()
    prompt = _gated("human-restricted", "record_effect", '{"a": 1}')
    events, gate, seen = _drive(prompt, required=frozenset({_LIVE_NAME}), double=double)

    assert seen == [("mcp__human-restricted__record_effect", {"a": 1})]
    assert gate.pending_granted_tool is None
    assert events[-1].status != SessionStatus.AWAITING_APPROVAL
    assert double.requests == []


@pytest.mark.parametrize(
    "directive",
    [
        "[fake:gated-connector-tool human-restricted execute_approved]",
        "[fake:gated-connector-tool human-restricted execute_approved not json]",
        '[fake:gated-connector-tool human-restricted execute_approved ["a"]]',
        '[fake:gated-connector-tool human-restricted execute_approved "s"]',
        "[fake:gated-connector-tool human-restricted execute_approved 42]",
        '[fake:gated-connector-tool human-restricted {"a": 1}]',
        "[fake:gated-connector-tool human-restricted]",
        "[fake:gated-connector-tool]",
    ],
    ids=["no_json", "bad_json", "list", "string", "number", "no_tool", "no_args", "bare"],
)
def test_a_malformed_directive_falls_back_to_the_default_turn(directive: str) -> None:
    double = _Double()
    events, gate, seen = _drive(
        f"please {directive}", required=frozenset({_LIVE_NAME}), double=double
    )

    assert not any(name.startswith("mcp__") for name, _ in seen)
    assert gate.pending_granted_tool is None
    assert events[-1].type == "final"
    assert events[-1].status == SessionStatus.DONE
    assert double.requests == []


def test_connector_tool_keeps_its_behavior_beside_the_new_directive() -> None:
    double = _Double()
    prompt = '[fake:connector-tool human-restricted record_effect {"a": 1}]'
    events, _gate, seen, _s = anyio.run(
        lambda: _run(prompt, required=frozenset({_LIVE_NAME}), double=double)
    )

    # The older directive still dials (the double answers 500, so it is a
    # failed dial) and never raises a gate decision for a tool call.
    assert seen == []
    assert double.requests
    assert "connector-tool human-restricted/record_effect: failed" in str(events[-1].text)
