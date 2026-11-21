"""A bundle hook's deny of publish_changes holds, on the real bundled CLI.

A bundle's PreToolUse hooks (its ``hooks/hooks.json``, which the CLI loads with
the plugin) run beside the runner's own approval hook, concurrently. Before
this fix the runner recorded a publication on sight of the ``publish_changes``
call, from the stream and from its own PreToolUse hook, and asked the CLI to
stop the turn there. A bundle hook that denied the same call could not take
that back: the run parked awaiting approval on a body the bundle had refused.
The dark factory's Decisions grammar and its diff approval publish gate were
both bypassed this way.

Now the in-process tool body records the publication, and the CLI runs it only
after every PreToolUse hook allowed the call; the approval hook's PostToolUse
callback ends the turn after it ran.

Each case puts the real bundled CLI between a ``SessionRunner`` and a local
stand-in for /v1/messages: no credential and no network. The session is wired
as ``__main__`` wires a managed-workspace session: the approval gate, its
hooks, its permission callback and its in-process ``curie`` MCP server. The
bundle is a local plugin whose ``hooks/hooks.json`` runs either a scripted
deny hook or the dark factory's real ``review_gate.py`` on ``publish_changes``.

The frames of the deny-then-correct turn are pinned to
``tests/vectors/publish-hook-deny-turn.json``, which the worker's handoff test
replays through its completion path.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import anyio
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.approval import APPROVAL_SERVER_NAME, ApprovalGate, build_approval_gate
from curie_runner.harness.claude.approval import (
    build_approval_hook,
    build_approval_server,
    build_can_use_tool,
)
from curie_runner.session import PUBLICATION_UNRECORDED_CLASSIFICATION, SessionRunner
from plugin_format import PLATFORM_PUBLISH_TOOL_NAME

_REPO = Path(__file__).resolve().parents[2]
VECTOR = _REPO / "tests" / "vectors" / "publish-hook-deny-turn.json"
_REVIEW_GATE = _REPO / "examples" / "dark-factory" / "hooks" / "review_gate.py"

_MAIN_MODEL = "claude-sonnet-5"
# The stand-in never checks it; the CLI only needs a key to send.
_PLACEHOLDER_KEY = "sk-ant-placeholder"
_SESSION_ID = "session-PLACEHOLDER-publish-hook-deny"
_SYSTEM_PROMPT = "You publish the requested changes."
_TURN_TIMEOUT_S = 120
_GIVE_UP_TEXT = "I could not publish the change."

_TITLE = "Keep the documentation parser output stable"
# The live shape the Decisions grammar refuses: the period sits inside the
# closing quote, so no entry reads ``. (plan) Evidence: ``.
_REFUSED_BODY = (
    "Keeps the parser output stable.\n\n## Decisions\n"
    '1. Keep the parser. Rejected: a rewrite, because it changes the "output." (plan) '
    "Evidence: the plan.\n"
)
_CORRECTED_BODY = (
    "Keeps the parser output stable.\n\n## Decisions\n"
    "1. Keep the parser. Rejected: a rewrite, because it changes the output. (plan) "
    "Evidence: the plan.\n"
)
# The scripted hook denies any body carrying this marker, with this reason.
_SCRIPTED_REFUSAL = "bundle gate: this publication body is refused"

_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_EMITTED = web.AppKey("emitted", list[tuple[str, dict[str, Any]]])
_SCRIPT = web.AppKey("script", list[str])

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


def _tool_results(body: dict[str, Any]) -> dict[str, dict[str, Any]]:
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


def _tool_block(call_id: str, arguments: dict[str, Any]) -> Block:
    return [
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "content_block": {
                    "type": "tool_use",
                    "id": call_id,
                    "name": PLATFORM_PUBLISH_TOOL_NAME,
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


def _call_id(index: int) -> str:
    return f"toolu_publish_{index}"


async def _messages(request: web.Request) -> web.StreamResponse:
    """The model: one publish call per scripted body, each after the last's result.

    When every scripted call has its result back, the model gives up in text.
    """

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
    script = request.app[_SCRIPT]
    index = sum(1 for n in range(len(script)) if _call_id(n) in answered)
    if index >= len(script):
        return await _stream(request, body, [_text_block(_GIVE_UP_TEXT)], "end_turn")
    arguments = {"title": _TITLE, "body": script[index]}
    request.app[_EMITTED].append((_call_id(index), arguments))
    return await _stream(request, body, [_tool_block(_call_id(index), arguments)], "tool_use")


async def _anything_else(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


def _plugin(root: Path, command: str) -> Path:
    """A local bundle whose hooks.json runs ``command`` before publish_changes."""

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
                            "matcher": ".*publish_changes",
                            "hooks": [{"type": "command", "command": command, "timeout": 10}],
                        }
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    return plugin


def _scripted_hook(root: Path) -> str:
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
        f"        'permissionDecisionReason': {_SCRIPTED_REFUSAL!r}}}}}))\n",
        encoding="utf-8",
    )
    return f"{sys.executable} {script}"


def _load_review_gate(state_dir: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setenv("DARK_FACTORY_STATE_DIR", str(state_dir))
    spec = importlib.util.spec_from_file_location("publish_hook_deny_review_gate", _REVIEW_GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.syspath_prepend(str(_REVIEW_GATE.parent))
    spec.loader.exec_module(module)
    return module


def _seed_review_gate(
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


def _workspace(root: Path) -> Path:
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
class _Turn:
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
            block = _tool_results(body).get(call_id)
            if block is not None:
                return json.dumps(block.get("content"))
        raise AssertionError(f"no tool result for {call_id}")

    @property
    def main_requests(self) -> list[dict[str, Any]]:
        return [body for body in self.bodies if body.get("tools")]


def _run_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    workspace: Path,
    hook_command: str,
    script: list[str],
    extra_env: dict[str, str] | None = None,
) -> _Turn:
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    gate = build_approval_gate(operator_tools=None, policy_routes={}, managed_workspace=True)
    assert gate is not None
    assert PLATFORM_PUBLISH_TOOL_NAME in gate.required
    plugin = _plugin(tmp_path, hook_command)

    async def scenario() -> _Turn:
        app = web.Application()
        app[_BODIES] = []
        app[_EMITTED] = []
        app[_SCRIPT] = list(script)
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
                        Event(
                            type="message", text="Publish the changes.", user="U0EXAMPLE1", ts="1"
                        ),
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            return _Turn(
                gate=gate,
                events=list(parse_ndjson("".join(lines))),
                bodies=list(app[_BODIES]),
                emitted=list(app[_EMITTED]),
            )

    return anyio.run(scenario)


def _pinned(events: list[Any]) -> list[dict[str, Any]]:
    """The frames as the vector pins them: no version or token counts."""

    pinned: list[dict[str, Any]] = []
    for event in events:
        data = event.model_dump(mode="json", exclude_none=True)
        for key in ("version", "input_tokens", "output_tokens"):
            data.pop(key, None)
        pinned.append(data)
    return pinned


def _assert_published_once(turn: _Turn, call_id: str, body: str) -> None:
    final = turn.final
    assert final.status is SessionStatus.AWAITING_APPROVAL, final
    assert final.approval_granted_tool == PLATFORM_PUBLISH_TOOL_NAME
    assert final.approval_granted_arguments == {"title": _TITLE, "body": body}
    assert turn.gate.publication_title == _TITLE
    assert turn.gate.publication_body == body
    assert turn.gate.pending_halt is True
    # The halt lands right after the recorded call: the model is never asked
    # again, so no request carries that call's result.
    assert not any(call_id in _tool_results(request) for request in turn.main_requests)


def _assert_nothing_published(turn: _Turn) -> None:
    final = turn.final
    assert final.status is SessionStatus.DONE, final
    assert final.approval_summary is None
    assert final.approval_granted_tool is None
    assert final.approval_granted_arguments is None
    assert turn.gate.pending_summary is None
    assert turn.gate.publication_title is None
    assert turn.gate.publication_body is None
    assert turn.gate.pending_halt is False
    assert not any(
        getattr(event, "classification", None) == PUBLICATION_UNRECORDED_CLASSIFICATION
        for event in turn.events
    ), turn.events
    assert final.text == _GIVE_UP_TEXT


def test_a_bundle_deny_reaches_the_model_and_the_corrected_call_publishes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(a) Deny once, then allow: only the corrected body is recorded, once.

    Red before the fix: the runner recorded the refused body on sight of the
    first call and stopped the turn, so the run parked awaiting approval on
    the body the bundle refused and the model never saw the deny.
    """

    workspace = _workspace(tmp_path)
    turn = _run_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=_scripted_hook(tmp_path),
        script=[_REFUSED_BODY, _CORRECTED_BODY],
    )

    assert turn.emitted == [
        (_call_id(0), {"title": _TITLE, "body": _REFUSED_BODY}),
        (_call_id(1), {"title": _TITLE, "body": _CORRECTED_BODY}),
    ]
    # The bundle hook really ran on both calls, and its reason reached the model.
    logged = (tmp_path / "scripted-gate.log").read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["body"] for line in logged] == [_REFUSED_BODY, _CORRECTED_BODY]
    assert _SCRIPTED_REFUSAL in turn.result_text(_call_id(0))
    _assert_published_once(turn, _call_id(1), _CORRECTED_BODY)
    # The refused body reaches neither the final nor the publication record the
    # worker reads. Only the stream's side effect frames, which describe each
    # call made, carry it.
    assert '"output." (plan)' not in json.dumps(_pinned([turn.final]))

    # The worker's handoff test replays exactly these frames and this record.
    vector = json.loads(VECTOR.read_text(encoding="utf-8"))
    assert _pinned(turn.events) == vector["runner_frames"]
    assert {
        "title": turn.gate.publication_title,
        "body": turn.gate.publication_body,
    } == vector["publication"]
    assert vector["refused_body"] == _REFUSED_BODY


def test_a_bundle_that_always_denies_publishes_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """(b) Deny every time: nothing is recorded and the turn ends on its own.

    Red before the fix: the first call was recorded and the run parked
    awaiting approval on a body the bundle refused.
    """

    workspace = _workspace(tmp_path)
    turn = _run_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=_scripted_hook(tmp_path),
        script=[_REFUSED_BODY, _REFUSED_BODY],
    )

    assert [call_id for call_id, _ in turn.emitted] == [_call_id(0), _call_id(1)]
    for index in range(2):
        assert _SCRIPTED_REFUSAL in turn.result_text(_call_id(index))
    _assert_nothing_published(turn)


def test_the_review_gate_decisions_deny_holds_and_the_corrected_body_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The dark factory's real review gate refuses the live Decisions shape.

    With an approved diff of this checkout, the gate denies the refused body
    on its Decisions grammar, the model corrects it, and the corrected body is
    the one recorded. Red before the fix: the refused body was recorded.
    """

    workspace = _workspace(tmp_path)
    state_dir = tmp_path / "gate-state"
    _seed_review_gate(workspace, state_dir, monkeypatch, diff_approved=True)
    turn = _run_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=f"{sys.executable} {_REVIEW_GATE}",
        script=[_REFUSED_BODY, _CORRECTED_BODY],
        extra_env={
            "DARK_FACTORY_STATE_DIR": str(state_dir),
            "DARK_FACTORY_PROGRESS_LOG": str(tmp_path / "gate-progress.log"),
        },
    )

    assert len(turn.emitted) == 2
    assert "Invalid Decisions section" in turn.result_text(_call_id(0))
    _assert_published_once(turn, _call_id(1), _CORRECTED_BODY)


def test_the_review_gate_diff_approval_deny_holds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The diff approval publish gate: no approving diff review, nothing published.

    Red before the fix: the runner recorded the call the gate denied and the
    run parked awaiting approval without any diff review.
    """

    workspace = _workspace(tmp_path)
    state_dir = tmp_path / "gate-state"
    _seed_review_gate(workspace, state_dir, monkeypatch, diff_approved=False)
    turn = _run_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=f"{sys.executable} {_REVIEW_GATE}",
        script=[_CORRECTED_BODY],
        extra_env={
            "DARK_FACTORY_STATE_DIR": str(state_dir),
            "DARK_FACTORY_PROGRESS_LOG": str(tmp_path / "gate-progress.log"),
        },
    )

    assert len(turn.emitted) == 1
    assert "Nothing is published without a diff reviewer VERDICT: APPROVE." in turn.result_text(
        _call_id(0)
    )
    _assert_nothing_published(turn)


def test_an_allowing_bundle_publishes_once_and_halts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Liveness: a bundle hook that allows the first call publishes it once.

    Passes both ways: the halt after an allowed call is unchanged.
    """

    workspace = _workspace(tmp_path)
    turn = _run_turn(
        tmp_path,
        monkeypatch,
        workspace=workspace,
        hook_command=_scripted_hook(tmp_path),
        script=[_CORRECTED_BODY, _CORRECTED_BODY],
    )

    assert turn.emitted == [(_call_id(0), {"title": _TITLE, "body": _CORRECTED_BODY})]
    _assert_published_once(turn, _call_id(0), _CORRECTED_BODY)
