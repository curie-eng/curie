"""Bash inside the real bundled CLI must not see platform credentials.

Docker and k8s both inject the boot env into this runner process. The Bash
tool is a child of the bundled Claude CLI, not a substrate-specific shell.
Measured on claude-agent-sdk 0.2.159 with bundled CLI 2.1.281: with
CLAUDE_CODE_SUBPROCESS_ENV_SCRUB=1 the CLI keeps ANTHROPIC_API_KEY for its
own messages call and omits it from Bash. Curie tokens are not in that
scrub list, so the runner must remove them before the CLI is spawned.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.otel import RunTracer
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier

_CALL_ID = "toolu_acme_env"
_TURN_TIMEOUT_S = 90
_MODEL_KEY = "sk-ant-PLACEHOLDER"
_SENTINELS = (
    "runner-sentinel",
    "sbx.example-state",
    "sbx.example-memory",
    "cct.example-caller",
    _MODEL_KEY,
)


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _tool_result_text(body: dict[str, Any]) -> str:
    chunks: list[str] = []
    for message in body.get("messages") or ():
        content = message.get("content") if isinstance(message, dict) else None
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and block.get("type") == "tool_result":
                chunks.append(json.dumps(block))
    return "\n".join(chunks)


async def _messages(request: web.Request) -> web.StreamResponse:
    body = await request.json()
    request.app["bodies"].append(body)
    request.app["api_keys"].append(request.headers.get("x-api-key", ""))
    response = web.StreamResponse(headers={"content-type": "text/event-stream"})
    await response.prepare(request)
    message = {
        "id": f"msg_stand_in_{len(request.app['bodies'])}",
        "type": "message",
        "role": "assistant",
        "model": body.get("model", "stand-in"),
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    if _tool_result_text(body):
        stop_reason = "end_turn"
        block: dict[str, Any] = {"type": "text", "text": ""}
        delta: dict[str, Any] = {"type": "text_delta", "text": "read"}
    else:
        stop_reason = "tool_use"
        block = {
            "type": "tool_use",
            "id": _CALL_ID,
            "name": "Bash",
            "input": {},
        }
        delta = {
            "type": "input_json_delta",
            "partial_json": json.dumps(
                {
                    "command": request.app["command"],
                    "description": "print outer and nested shell environments",
                }
            ),
        }
    events = [
        ("message_start", {"type": "message_start", "message": message}),
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


def test_bash_env_omits_platform_credentials_while_the_model_call_keeps_its_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.setenv("CURIE_RUNNER_TOKEN", "runner-sentinel")
    monkeypatch.setenv("CURIE_STATE_TOKEN", "sbx.example-state")
    monkeypatch.setenv("CURIE_MEMORY_TOKEN", "sbx.example-memory")
    monkeypatch.setenv("CURIE_CONNECTOR_CALLER_TOKEN", "cct.example-caller")
    monkeypatch.setenv("ANTHROPIC_API_KEY", _MODEL_KEY)
    monkeypatch.setenv("CURIE_MODEL", "claude-sonnet-5")
    monkeypatch.setenv("STDIO_TOKEN", "connector-sentinel")
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    startup = cwd / "startup.sh"
    startup.write_text(
        'if [ -n "${ANTHROPIC_API_KEY-}" ]; then printf bad > "$ACME_STARTUP_CAPTURE"; fi\n'
        "export CURIE_TEST_SHELL=boundary-kept\n"
    )
    untrusted = cwd / "untrusted-shell"
    untrusted.write_text('#!/bin/sh\nenv > "$ACME_UNTRUSTED_CAPTURE"\nexec /bin/bash "$@"\n')
    untrusted.chmod(0o755)
    capture = cwd / "startup-capture"
    untrusted_capture = cwd / "untrusted-capture"

    async def scenario() -> tuple[list[str], list[dict[str, Any]], list[str]]:
        app = web.Application()
        app["bodies"] = []
        app["api_keys"] = []
        app["command"] = f"source {startup}; env; bash -c env"
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            options = build_options(
                plugins=[],
                model="claude-sonnet-5",
                system_prompt="Print the environment.",
                max_turns=3,
                max_budget_usd=None,
                resume=None,
                cwd=str(cwd),
                env={
                    "ANTHROPIC_API_KEY": _MODEL_KEY,
                    "ANTHROPIC_BASE_URL": str(server.make_url("")).rstrip("/"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                    "CLAUDE_CODE_SHELL": str(untrusted),
                    "CURIE_SHELL_PYTHON": "/workspace/untrusted-python",
                    "ACME_STARTUP_CAPTURE": str(capture),
                    "ACME_UNTRUSTED_CAPTURE": str(untrusted_capture),
                },
            )
            runner = SessionRunner(
                held_secrets=frozenset(),
                session_factory=lambda: ClaudeAgentSession(options),
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="curie-run:acme-shell-env",
                session_id="session-PLACEHOLDER",
                model="claude-sonnet-5",
            )
            lines: list[str] = []
            await runner.start()
            try:
                with anyio.fail_after(_TURN_TIMEOUT_S):
                    async for line in runner.run_turn(
                        Event(type="message", text="print env", user="U0EXAMPLE1", ts="1")
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            return lines, list(app["bodies"]), list(app["api_keys"])

    lines, bodies, api_keys = anyio.run(scenario)
    finals = [event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)]
    assert len(finals) == 1, lines
    rendered = "\n".join(_tool_result_text(body) for body in bodies)
    assert "CURIE_MODEL=claude-sonnet-5" in rendered
    assert "STDIO_TOKEN=connector-sentinel" in rendered
    assert "CURIE_TEST_SHELL=boundary-kept" in rendered
    assert not capture.exists(), "startup observed a model credential before command cleanup"
    assert not untrusted_capture.exists(), "SDK options replaced the trusted shell launcher"
    assert rendered.count("CURIE_MODEL=claude-sonnet-5") >= 2
    for sentinel in _SENTINELS:
        assert sentinel not in rendered, sentinel
    assert _MODEL_KEY in api_keys
