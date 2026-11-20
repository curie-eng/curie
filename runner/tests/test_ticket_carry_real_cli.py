"""Approval ticket carry on the real bundled CLI (ADR 0288).

The fake tier cannot show two things the carry depends on: that the real CLI
runs the runner's PostToolUse callback for an MCP tool before the next model
call, with a ``tool_response`` the scan can read, and that the CLI honors a
PreToolUse ``updatedInput`` for an MCP tool, so the connector receives the
replaced arguments. These cases put the real bundled CLI between the runner and
a real stdio MCP connector; a local stand-in for /v1/messages scripts the
model's calls. No credential and no network.
"""

from __future__ import annotations

import base64
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any, cast

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_internal.approver_ticket import execution_arguments_sha256, sign_ticket
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.approval import ApprovalGate
from curie_runner.harness.claude.approval import build_approval_hook, build_can_use_tool
from curie_runner.session import SessionRunner
from curie_runner.ticket_carry import TicketCarry
from nacl.signing import SigningKey

_SERVER = Path(__file__).parent / "fixtures" / "mcp_ticket_server.py"
_VECTORS = json.loads(
    (Path(__file__).resolve().parents[2] / "tests" / "vectors" / "approver-ticket.json").read_text(
        encoding="utf-8"
    )
)
_SIGNING_KEY = SigningKey(hashlib.sha256(_VECTORS["fixture_seed_label"].encode("utf-8")).digest())
_ZERO = "00000000-0000-0000-0000-000000000000"
_PREPARE = "mcp__acme__prepare"
_APPLY = "mcp__acme__apply"
_ARGS = {"resource": "example-repo"}
_SCRIPT = web.AppKey("script", list[tuple[str, dict[str, Any]]])
_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_TURN_TIMEOUT_S = 90


def _ticket() -> str:
    """A synthetic ticket for ``_ARGS`` on ``acme``/``apply``, fresh for the run."""

    principal = {"tenant_id": _ZERO, "issuer": "https://id.example.com"}
    nonce = hashlib.sha256(b"real-cli").digest()
    now = int(time.time())
    return sign_ticket(
        {
            "version": 1,
            "purpose": "approval_request",
            "tenant_id": _ZERO,
            "agent_id": "00000000-0000-4000-8000-000000000002",
            "connector": "acme",
            "tool": "apply",
            "route": "acme",
            "key_id": "selector-test-1",
            "requester": {**principal, "subject": "requester"},
            "nonce": _b64(nonce),
            "issued_at": now - 5,
            "expires_at": now + 295,
            "arguments_sha256": execution_arguments_sha256(_ARGS),
            "approvers": [{**principal, "subject": "approver"}],
        },
        signing_key=_SIGNING_KEY,
    )


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _damaged(ticket: str) -> str:
    start = ticket.index("MDAwMDAw")
    return ticket[:start] + ticket[start + 68 :]


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _tool_results(body: dict[str, Any]) -> int:
    """How many tool_result blocks the CLI has sent so far in this request."""

    count = 0
    for message in body.get("messages") or ():
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list):
            count += sum(
                1
                for block in content
                if isinstance(block, dict) and block.get("type") == "tool_result"
            )
    return count


async def _messages(request: web.Request) -> web.StreamResponse:
    """The Nth scripted tool call after N tool results, then a text answer."""

    body = await request.json()
    request.app[_BODIES].append(body)
    script = request.app[_SCRIPT]
    step = _tool_results(body)
    response = web.StreamResponse(headers={"content-type": "text/event-stream"})
    await response.prepare(request)
    message = {
        "id": f"msg_stand_in_{len(request.app[_BODIES])}",
        "type": "message",
        "role": "assistant",
        "model": body.get("model", "stand-in"),
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    events: list[tuple[str, dict[str, Any]]] = [
        ("message_start", {"type": "message_start", "message": message})
    ]
    if step >= len(script):
        stop_reason = "end_turn"
        block: dict[str, Any] = {"type": "text", "text": ""}
        delta: dict[str, Any] = {"type": "text_delta", "text": "done"}
    else:
        stop_reason = "tool_use"
        name, arguments = script[step]
        block = {"type": "tool_use", "id": f"toolu_acme{step:02d}", "name": name, "input": {}}
        delta = {"type": "input_json_delta", "partial_json": json.dumps(arguments)}
    events += [
        (
            "content_block_start",
            {"type": "content_block_start", "index": 0, "content_block": block},
        ),
        ("content_block_delta", {"type": "content_block_delta", "index": 0, "delta": delta}),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]
    for event, data in events:
        await response.write(_sse(event, data))
    await response.write_eof()
    return response


async def _anything_else(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


def _run_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    gate: ApprovalGate,
    ticket: str,
    script: list[tuple[str, dict[str, Any]]],
) -> tuple[Final, list[dict[str, Any]]]:
    """One real turn wired the way ``__main__`` wires a gated session.

    Returns the turn's final and the calls that reached ``apply``.
    """

    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    calls = tmp_path / "ticket-calls.jsonl"

    async def scenario() -> Final:
        app = web.Application()
        app[_SCRIPT] = script
        app[_BODIES] = []
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            options = build_options(
                plugins=[],
                model="claude-sonnet-5",
                system_prompt="You prepare and apply test requests.",
                max_turns=4,
                max_budget_usd=None,
                resume=None,
                cwd=str(cwd),
                hooks=build_approval_hook(gate),
                can_use_tool=build_can_use_tool(gate),
                mcp_servers=cast(
                    "Any",
                    {
                        "acme": {
                            "type": "stdio",
                            "command": sys.executable,
                            "args": [str(_SERVER)],
                            "env": {
                                "CURIE_TEST_TICKET": ticket,
                                "CURIE_TEST_TICKET_CALLS": str(calls),
                            },
                        }
                    },
                ),
                env={
                    "ANTHROPIC_API_KEY": "sk-ant-placeholder",
                    "ANTHROPIC_BASE_URL": str(server.make_url("")).rstrip("/"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                },
            )
            runner = SessionRunner(
                max_usd_per_day=None,
                held_secrets=frozenset(),
                session_factory=lambda: ClaudeAgentSession(options),
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="curie-run:acme-ticket-carry",
                session_id="session-PLACEHOLDER",
                model="claude-sonnet-5",
                approval_gate=gate,
            )
            lines: list[str] = []
            await runner.start()
            try:
                with anyio.fail_after(_TURN_TIMEOUT_S):
                    async for line in runner.run_turn(
                        Event(type="message", text="apply it", user="U0EXAMPLE1", ts="1")
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            finals = [event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)]
            assert len(finals) == 1, lines
            return finals[0]

    final = anyio.run(scenario)
    received = (
        [json.loads(line) for line in calls.read_text(encoding="utf-8").splitlines()]
        if calls.exists()
        else []
    )
    return final, received


def _gate(**kw: Any) -> ApprovalGate:
    gate = ApprovalGate(
        required=frozenset({_APPLY}),
        route_by_tool={_APPLY: "acme"},
        ticket_carry=TicketCarry(),
        **kw,
    )
    gate.proxied_connector_servers = frozenset({"acme"})
    return gate


def test_the_real_cli_feeds_the_observer_and_the_approval_records_the_exact_ticket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ticket = _ticket()
    gate = _gate()
    final, received = _run_turn(
        tmp_path,
        monkeypatch,
        gate=gate,
        ticket=ticket,
        script=[(_PREPARE, _ARGS), (_APPLY, {**_ARGS, "approval_ticket": _damaged(ticket)})],
    )

    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.approval_granted_tool == _APPLY
    assert final.approval_granted_arguments == {**_ARGS, "approval_ticket": ticket}
    assert received == [], "the held call never reached the connector"


def test_the_real_cli_runs_the_approved_call_with_the_grants_exact_ticket(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ticket = _ticket()
    gate = _gate(grant_tool=_APPLY, grant_arguments={**_ARGS, "approval_ticket": ticket})
    final, received = _run_turn(
        tmp_path,
        monkeypatch,
        gate=gate,
        ticket=ticket,
        script=[(_APPLY, {**_ARGS, "approval_ticket": _damaged(ticket)})],
    )

    assert final.status is SessionStatus.DONE
    assert received == [{"tool": "apply", "arguments": {**_ARGS, "approval_ticket": ticket}}]
    assert gate.grant_tool is None
