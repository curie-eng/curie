"""The bundle's skills, and only those, on the real bundled CLI (#3766, ADR-0189 Draft).

``test_harness_skills.py`` checks the option values. These cases put the real
bundled Claude Code CLI behind them, with an empty ``HOME`` and config dir, a
local stand-in for ``/v1/messages`` and a placeholder key, so what is pinned is
what the CLI does with the options: no credential and no network.

- The listing: the first model request names the bundle's skills and none of
  the CLI's built-in ones. Read from the request, not the ``init`` message,
  because ``init.skills`` lists every registered skill even when filtered. This
  is the case that catches a CLI upgrade changing how plugin skills are named
  or filtered.
- The gate: listing a skill makes the SDK add ``Skill(<name>)`` to
  ``--allowedTools``, an allow rule that skips ``can_use_tool``. A ``Skill``
  call must still reach the runner's PreToolUse approval hook, and a gate on
  ``Skill`` must still stop it.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import anyio
import claude_agent_sdk
import pytest
from aci_protocol import Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner import RunTracer, SideEffectClassifier, load_plugins
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.approval import (
    ApprovalGate,
    build_approval_gate,
    build_approval_hook,
    build_can_use_tool,
)
from curie_runner.plugin import bundle_skill_names
from curie_runner.session import SessionRunner
from curie_runner.side_effects import CLAUDE_READONLY_TOOLS
from curie_runner.tool_access import TurnToolAccess, front_can_use_tool, front_pre_tool_use_hooks

_BUNDLED_CLI = Path(claude_agent_sdk.__file__).parent / "_bundled" / "claude"
_CLI_AVAILABLE = _BUNDLED_CLI.is_file() or shutil.which("claude") is not None
pytestmark = pytest.mark.skipif(
    not _CLI_AVAILABLE, reason="requires the Claude Code CLI the SDK spawns"
)

_TURN_TIMEOUT_S = 90
_CALL_ID = "toolu_acme_skill"
_LISTING_HEADING = "The following skills are available for use with the Skill tool"
# Built-in skills that ship inside the CLI (CLI 2.1.281). ``update-config`` is
# the one that steered #3625 away from ``remember``.
_BUILT_IN_SKILLS = ("update-config", "code-review", "simplify", "loop")
_SKILL_BODY = "SKILL-BODY-PLACEHOLDER: greet the person by name."
_TOOL_CALL = web.AppKey("tool_call", dict)
_BODIES = web.AppKey("bodies", list)


def _bundle(root: Path) -> str:
    """A bundle named ``probe`` with the skills ``greet`` and ``hello``."""

    (root / ".claude-plugin").mkdir(parents=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "probe", "version": "0.1.0"}), encoding="utf-8"
    )
    for name in ("greet", "hello"):
        skill = root / "skills" / name
        skill.mkdir(parents=True)
        (skill / "SKILL.md").write_text(
            f"---\nname: {name}\ndescription: Say {name} to the person.\n---\n\n{_SKILL_BODY}\n",
            encoding="utf-8",
        )
    return str(root)


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _tool_results(body: dict[str, Any]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for message in body.get("messages") or ():
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list):
            found.extend(
                block
                for block in content
                if isinstance(block, dict) and block.get("type") == "tool_result"
            )
    return found


async def _messages(request: web.Request) -> web.StreamResponse:
    """Answer with text, or with the scripted tool call until a result comes back."""

    body = await request.json()
    request.app[_BODIES].append(body)
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
    tool_call = request.app[_TOOL_CALL]
    if tool_call and not _tool_results(body):
        stop_reason = "tool_use"
        block: dict[str, Any] = {
            "type": "tool_use",
            "id": _CALL_ID,
            "name": tool_call["name"],
            "input": {},
        }
        delta: dict[str, Any] = {
            "type": "input_json_delta",
            "partial_json": json.dumps(tool_call["input"]),
        }
    else:
        stop_reason = "end_turn"
        block = {"type": "text", "text": ""}
        delta = {"type": "text_delta", "text": "done"}
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


def _run_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    tool_call: dict[str, Any] | None,
    gate: ApprovalGate | None = None,
    seen: list[str] | None = None,
) -> tuple[Final, list[dict[str, Any]]]:
    """One real turn on the probe bundle, wired the way ``__main__`` wires it.

    ``seen`` collects the tool name of every call the approval hook is asked
    about. Returns the final and every request the stand-in received.
    """

    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(home / ".claude"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    plugin_dir = _bundle(tmp_path / "plugin")
    access = TurnToolAccess(
        CLAUDE_READONLY_TOOLS,
        requires_approval=gate.requires_approval if gate is not None else None,
    )

    approval_hooks = None
    if gate is not None:
        [matcher] = build_approval_hook(gate)["PreToolUse"]
        [approval] = matcher.hooks

        async def observed(hook_input: Any, tool_use_id: str | None, context: Any) -> Any:
            if seen is not None:
                seen.append(str(hook_input.get("tool_name")))
            return await approval(hook_input, tool_use_id, context)

        matcher.hooks = [observed]
        approval_hooks = {"PreToolUse": [matcher]}

    async def scenario() -> tuple[Final, list[dict[str, Any]]]:
        app = web.Application()
        app[_TOOL_CALL] = tool_call or {}
        app[_BODIES] = []
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            options = build_options(
                plugins=load_plugins(plugin_dir),
                model="claude-sonnet-5",
                system_prompt="You greet people.",
                max_turns=3,
                max_budget_usd=None,
                resume=None,
                cwd=str(cwd),
                hooks=front_pre_tool_use_hooks(approval_hooks, access),
                can_use_tool=(
                    front_can_use_tool(build_can_use_tool(gate), access)
                    if gate is not None
                    else None
                ),
                skills=bundle_skill_names(plugin_dir),
                env={
                    "HOME": str(home),
                    "ANTHROPIC_API_KEY": "sk-ant-placeholder",
                    "ANTHROPIC_BASE_URL": str(server.make_url("")).rstrip("/"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                },
            )
            runner = SessionRunner(
                held_secrets=frozenset(),
                session_factory=lambda: ClaudeAgentSession(options),
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="curie-run:acme-skills",
                session_id="session-PLACEHOLDER",
                model="claude-sonnet-5",
                approval_gate=gate,
                tool_access=access,
            )
            lines: list[str] = []
            await runner.start()
            try:
                with anyio.fail_after(_TURN_TIMEOUT_S):
                    async for line in runner.run_turn(
                        Event(type="message", text="greet me", user="U0EXAMPLE1", ts="1")
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            finals = [event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)]
            assert len(finals) == 1, lines
            return finals[0], list(app[_BODIES])

    return anyio.run(scenario)


def _skill_listing(body: dict[str, Any]) -> str:
    """The skill listing section of one model request, wherever the CLI put it."""

    rendered = json.dumps(body)
    start = rendered.find(_LISTING_HEADING)
    assert start != -1, "the model request carries no skill listing"
    # The section runs to the end of its text block; a JSON-escaped string ends
    # at the next unescaped quote.
    end = start
    while True:
        end = rendered.index('"', end + 1)
        if rendered[end - 1] != "\\":
            break
    return rendered[start:end]


def test_the_model_is_listed_the_bundle_skills_and_no_built_in_ones(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # revert: pass ``skills=None`` (the SDK default) -> every built-in skill,
    # ``update-config`` among them, is listed to the model again.
    final, bodies = _run_turn(tmp_path, monkeypatch, tool_call=None)

    assert final.status is SessionStatus.DONE
    listing = _skill_listing(bodies[0])
    assert "probe:greet" in listing
    assert "probe:hello" in listing
    for name in _BUILT_IN_SKILLS:
        assert name not in listing, f"the built-in skill {name!r} reached the model"


def test_a_gated_skill_call_goes_through_the_approval_hook_and_is_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # revert: leave the gate to ``can_use_tool`` alone (drop the approval hook)
    # -> the turn ends ``done`` and the gated skill loads. Checked on CLI
    # 2.1.281: the SDK's ``Skill(probe:greet)`` allow rule does skip
    # ``can_use_tool``, so the hook is the only layer that stops this call.
    # toolPolicy cannot name ``Skill`` (it covers MCP tools only, ADR-0139), so
    # the operator gate is the denial a ``Skill`` call can meet.
    gate = build_approval_gate(operator_tools=["Skill"], policy_routes={})
    assert gate is not None
    seen: list[str] = []

    final, bodies = _run_turn(
        tmp_path,
        monkeypatch,
        tool_call={"name": "Skill", "input": {"skill": "probe:greet"}},
        gate=gate,
        seen=seen,
    )

    assert "Skill" in seen, "the approval hook never saw the Skill call"
    assert final.status is SessionStatus.AWAITING_APPROVAL
    assert final.approval_granted_tool == "Skill"
    assert _CALL_ID in gate.held_call_ids
    assert _SKILL_BODY not in json.dumps(bodies), "the gated skill was loaded anyway"


def test_an_ungated_skill_call_still_loads_the_bundle_skill(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The control for the gate case: the same call with nothing gating it does
    # load the skill, so the gate case is a refusal and not a call that never ran.
    gate = build_approval_gate(operator_tools=["Write"], policy_routes={})
    assert gate is not None
    seen: list[str] = []

    final, bodies = _run_turn(
        tmp_path,
        monkeypatch,
        tool_call={"name": "Skill", "input": {"skill": "probe:greet"}},
        gate=gate,
        seen=seen,
    )

    assert "Skill" in seen, "the approval hook never saw the Skill call"
    assert final.status is SessionStatus.DONE
    assert _SKILL_BODY in json.dumps(bodies[1:]), "the listed bundle skill did not load"
