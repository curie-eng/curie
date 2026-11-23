"""Real-CLI turns with bundle hooks on publication and approval tools.

The runner's real-CLI cases (runner/tests/test_publish_hook_deny_real_cli.py)
and the worker's handoff test (apps/worker/tests/kernel/
test_publish_hook_deny_handoff.py) both run this turn: the real bundled Claude
CLI between a ``SessionRunner`` and a local stand-in for /v1/messages, with no
credential and no network. The session is wired as ``__main__`` wires a
managed-workspace session: the approval gate, its hooks, its permission
callback and its in-process ``curie`` MCP server. The bundle is a local plugin
whose ``hooks/hooks.json`` runs a hook on ``publish_changes``, which the CLI
loads the way it loads a bundle's hooks.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.approval import (
    APPROVAL_SERVER_NAME,
    APPROVAL_TOOL_NAME,
    ApprovalGate,
    build_approval_gate,
)
from curie_runner.harness.claude.approval import (
    build_approval_hook,
    build_approval_server,
    build_can_use_tool,
)
from curie_runner.session import SessionRunner
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

_REPO = Path(__file__).resolve().parents[2]
REVIEW_GATE = _REPO / "examples" / "dark-factory" / "hooks" / "review_gate.py"

_MAIN_MODEL = "claude-sonnet-5"
# The stand-in never checks it; the CLI only needs a key to send.
_PLACEHOLDER_KEY = "sk-ant-placeholder"
_SESSION_ID = "session-PLACEHOLDER-publish-hook-deny"
_SYSTEM_PROMPT = "You publish the requested changes."
_TURN_TIMEOUT_S = 120
GIVE_UP_TEXT = "I could not publish the change."

TITLE = "Keep the documentation parser output stable"
# The live shape the Decisions grammar refuses: the period sits inside the
# closing quote, so no entry reads ``. (plan) Evidence: ``.
REFUSED_BODY = (
    "Keeps the parser output stable.\n\n## Decisions\n"
    '1. Keep the parser. Rejected: a rewrite, because it changes the "output." (plan) '
    "Evidence: the plan.\n"
)
CORRECTED_BODY = (
    "Keeps the parser output stable.\n\n## Decisions\n"
    "1. Keep the parser. Rejected: a rewrite, because it changes the output. (plan) "
    "Evidence: the plan.\n"
)
# The scripted hook denies any body carrying this marker, with this reason.
SCRIPTED_REFUSAL = "bundle gate: this publication body is refused"
APPROVAL_REFUSAL = "bundle gate: this approval request is refused"
REFUSED_SUMMARY = "Approve the first proposal"
REFUSED_REQUEST = {"summary": REFUSED_SUMMARY, "route": "ops"}
SECOND_SUMMARY = "Approve the second proposal"
APPROVED_SUMMARY = "Approve the second proposal after review"

_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_EMITTED = web.AppKey("emitted", list[tuple[str, dict[str, Any]]])
_SCRIPT = web.AppKey("script", list[dict[str, Any]])
_TOOL_NAME = web.AppKey("tool_name", str)
_FINAL_TEXT = web.AppKey("final_text", str)

Block = list[tuple[str, dict[str, Any]]]


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _blocks(message: dict[str, Any]) -> list[dict[str, Any]]:
    content = message.get("content")
    if isinstance(content, list):
        return [block for block in content if isinstance(block, dict)]
    if isinstance(content, str):
        return [{"type": "text", "text": content}]
    return []


def tool_results(body: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        block.get("tool_use_id"): block
        for message in body.get("messages") or ()
        if isinstance(message, dict) and message.get("role") == "user"
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


def _tool_block(tool_name: str, call_id: str, arguments: dict[str, Any]) -> Block:
    return [
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "content_block": {
                    "type": "tool_use",
                    "id": call_id,
                    "name": tool_name,
                    "input": {},
                },
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
    await response.write(
        _sse(
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        )
    )
    await response.write(_sse("message_stop", {"type": "message_stop"}))
    await response.write_eof()
    return response


def call_id(index: int) -> str:
    return f"toolu_publish_{index}"


async def _messages(request: web.Request) -> web.StreamResponse:
    """Emit each scripted call after the preceding call's result."""

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
    answered = tool_results(body)
    script = request.app[_SCRIPT]
    index = sum(1 for n in range(len(script)) if call_id(n) in answered)
    if index >= len(script):
        return await _stream(request, body, [_text_block(request.app[_FINAL_TEXT])], "end_turn")
    arguments = script[index]
    request.app[_EMITTED].append((call_id(index), arguments))
    tool = _tool_block(request.app[_TOOL_NAME], call_id(index), arguments)
    return await _stream(request, body, [tool], "tool_use")


async def _anything_else(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


def _plugin(root: Path, command: str, matcher: str) -> Path:
    plugin = root / "bundle"
    (plugin / ".claude-plugin").mkdir(parents=True)
    (plugin / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "acme-publish-gate", "version": "0.1.0"}), encoding="utf-8"
    )
    (plugin / "hooks").mkdir()
    (plugin / "hooks" / "hooks.json").write_text(
        json.dumps(
            {
                "hooks": {
                    "PreToolUse": [
                        {
                            "matcher": matcher,
                            "hooks": [{"type": "command", "command": command, "timeout": 10}],
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    return plugin


def scripted_hook(root: Path) -> str:
    """A command hook that denies a body carrying the refused marker, and logs every call."""

    script = root / "scripted_gate.py"
    log = root / "scripted-gate.log"
    script.write_text(
        "import json, sys\n"
        "data = json.load(sys.stdin)\n"
        f"with open({str(log)!r}, 'a') as stream:\n"
        "    stream.write(json.dumps(data.get('tool_input')) + '\\n')\n"
        "body = str((data.get('tool_input') or {}).get('body') or '')\n"
        "if '\"output.\" (plan)' in body:\n"
        "    print(json.dumps({'hookSpecificOutput': {'hookEventName': 'PreToolUse',\n"
        "        'permissionDecision': 'deny',\n"
        f"        'permissionDecisionReason': {SCRIPTED_REFUSAL!r}}}}}))\n",
        encoding="utf-8",
    )
    return f"{sys.executable} {script}"


def _load_review_gate(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setenv("DARK_FACTORY_STATE_DIR", str(state_dir))
    spec = importlib.util.spec_from_file_location("publish_hook_deny_review_gate", REVIEW_GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.syspath_prepend(str(REVIEW_GATE.parent))
    spec.loader.exec_module(module)
    return module


def seed_review_gate(
    workspace: Path, state_dir: Path, monkeypatch: pytest.MonkeyPatch, *, diff_approved: bool
) -> None:
    """The review gate's run state just before publication.

    Timing read, workflow loaded, and either an approving diff review of this
    exact checkout or no diff approval at all.
    """

    gate = _load_review_gate(state_dir, monkeypatch)
    base = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=workspace, check=True, capture_output=True, text=True
    ).stdout.strip()
    state = gate._fresh()
    state.update(
        workspace=str(workspace),
        base=base,
        workflow=True,
        stage="publish",
        timing={"deadline": int(time.time()) + 3600, "seconds": 3600},
    )
    if diff_approved:
        digest = gate.fingerprint(str(workspace), time.monotonic() + 7.0, base)
        assert digest is not None
        state["diff"] = {"round": 1, "verdict": "APPROVE"}
        state["diff_approved_fingerprint"] = digest
    gate.save(gate.state_path(_SESSION_ID), state)


def workspace_with_change(root: Path) -> Path:
    workspace = root / "workspace"
    workspace.mkdir()

    def git(*args: str) -> None:
        subprocess.run(["git", *args], cwd=workspace, check=True, capture_output=True)

    git("init", "-q")
    git("config", "user.email", "factory@example.com")
    git("config", "user.name", "Factory Example")
    (workspace / "README.md").write_text("Original documentation.\n", encoding="utf-8")
    git("add", "README.md")
    git("commit", "-q", "-m", "base")
    (workspace / "README.md").write_text("Updated documentation.\n", encoding="utf-8")
    return workspace


@dataclass
class PublishTurn:
    gate: ApprovalGate
    events: list[Any]
    bodies: list[dict[str, Any]]
    emitted: list[tuple[str, dict[str, Any]]]

    @property
    def final(self) -> Final:
        finals = [event for event in self.events if isinstance(event, Final)]
        assert len(finals) == 1, self.events
        return finals[0]

    def result_text(self, call_id: str) -> str:
        for body in self.bodies:
            block = tool_results(body).get(call_id)
            if block is not None:
                return json.dumps(block.get("content"))
        raise AssertionError(f"no tool result for {call_id}")

    @property
    def main_requests(self) -> list[dict[str, Any]]:
        return [body for body in self.bodies if body.get("tools")]


def run_publish_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    workspace: Path,
    hook_command: str,
    script: list[str],
    extra_env: dict[str, str] | None = None,
    publish_route: str | None = None,
) -> PublishTurn:
    """Run one turn on the real CLI. ``publish_route`` binds the bundle's
    approval route to ``publish_changes``, as an approval-gated bundle does."""

    routes = {} if publish_route is None else {PLATFORM_PUBLISH_TOOL_NAME: publish_route}
    gate = build_approval_gate(operator_tools=None, policy_routes=routes, managed_workspace=True)
    assert gate is not None
    assert PLATFORM_PUBLISH_TOOL_NAME in gate.required
    return _run_tool_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=hook_command,
        tool_name=PLATFORM_PUBLISH_TOOL_NAME,
        script=[{"title": TITLE, "body": body} for body in script],
        gate=gate,
        final_text=GIVE_UP_TEXT,
        prompt="Publish the changes.",
        extra_env=extra_env,
    )


def run_approval_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, allow_second: bool
) -> PublishTurn:
    script = tmp_path / "approval_gate.py"
    script.write_text(
        "import json, sys\n"
        "arguments = json.load(sys.stdin)['tool_input']\n"
        f"if arguments['summary'] == {REFUSED_SUMMARY!r}:\n"
        "    decision = {'permissionDecision': 'deny', "
        f"'permissionDecisionReason': {APPROVAL_REFUSAL!r}}}\n"
        "else:\n"
        "    decision = {'permissionDecision': 'allow', "
        f"'updatedInput': {{'summary': {APPROVED_SUMMARY!r}}}}}\n"
        "decision['hookEventName'] = 'PreToolUse'\n"
        "print(json.dumps({'hookSpecificOutput': decision}))\n",
        encoding="utf-8",
    )
    gate = build_approval_gate(operator_tools=None, policy_routes={}, managed_workspace=True)
    assert gate is not None
    second = {"summary": SECOND_SUMMARY} if allow_second else REFUSED_REQUEST
    return _run_tool_turn(
        tmp_path,
        monkeypatch,
        workspace=tmp_path,
        hook_command=shlex.join([sys.executable, str(script)]),
        tool_name=APPROVAL_TOOL_NAME,
        script=[REFUSED_REQUEST, second],
        gate=gate,
        final_text="I finished reviewing the proposals.",
        prompt="Carry out the review.",
    )


def _run_tool_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    workspace: Path,
    hook_command: str,
    tool_name: str,
    script: list[dict[str, Any]],
    gate: ApprovalGate,
    final_text: str,
    prompt: str,
    extra_env: dict[str, str] | None = None,
) -> PublishTurn:
    for name in list(os.environ):
        if name.startswith(("ANTHROPIC_", "CLAUDE_CODE_USE_")) or name in {
            "CLAUDE_CODE_OAUTH_TOKEN",
            "CURIE_CREDENTIALS",
            "CURIE_MODEL_ENV_KEY",
            "CURIE_MODEL_AUTH_MODE",
        }:
            monkeypatch.delenv(name)
    plugin = _plugin(tmp_path, hook_command, f".*{tool_name.rsplit('__', 1)[-1]}")

    async def scenario() -> PublishTurn:
        app = web.Application()
        app[_BODIES] = []
        app[_EMITTED] = []
        app[_SCRIPT] = script
        app[_TOOL_NAME] = tool_name
        app[_FINAL_TEXT] = final_text
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            base_url = str(server.make_url("")).rstrip("/")
            monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
            options = build_options(
                plugins=[{"type": "local", "path": str(plugin)}],
                model=_MAIN_MODEL,
                system_prompt=_SYSTEM_PROMPT,
                max_turns=8,
                max_budget_usd=None,
                resume=None,
                cwd=str(workspace),
                env={
                    "ANTHROPIC_API_KEY": _PLACEHOLDER_KEY,
                    "ANTHROPIC_BASE_URL": base_url,
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                    "CURIE_SESSION_ID": _SESSION_ID,
                    **(extra_env or {}),
                },
                hooks=build_approval_hook(gate),
                mcp_servers={
                    APPROVAL_SERVER_NAME: build_approval_server(gate, managed_workspace=True)
                },
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
                        Event(type="message", text=prompt, user="U0EXAMPLE1", ts="1"),
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            return PublishTurn(
                gate=gate,
                events=list(parse_ndjson("".join(lines))),
                bodies=list(app[_BODIES]),
                emitted=list(app[_EMITTED]),
            )

    return anyio.run(scenario)
