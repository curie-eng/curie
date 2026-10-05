"""A reviewer subagent out of provider credit, on the real bundled CLI (#3935).

The dark factory's reviewers run as Claude Code subagents through the ``Agent``
tool. When OpenRouter refuses a reviewer request for credit, the parent turn
recovers and ends with a successful result, so a translator that reads only the
terminal result delivers the turn and the run ends needs-human instead of out of
credits. These cases put the real bundled CLI between the runner and a local
stand-in for /v1/messages: no credential and no network. The stand-in tells the
reviewer's requests apart by ``body["model"]``.

Measured with the bundled CLI 2.1.281 (2026-10-04):

- with no env, the CLI sends ``max_tokens: 64000`` for a known Claude main model
  and 32000 for an unknown subagent model, and OpenRouter reserves credit for
  that whole ceiling;
- with ``CLAUDE_CODE_MAX_OUTPUT_TOKENS`` in the CLI env, every request, main and
  subagent, carries that value; there is no per-subagent ceiling;
- a subagent request answered HTTP 402 with OpenRouter's body is not retried,
  and the parent ends with a ``success`` ResultMessage;
- with the runner's options (``forward_subagent_text`` left False), the
  subagent's errored AssistantMessage (``parent_tool_use_id`` set,
  ``error="unknown"``, text ``API Error: 402 ...``) never reaches the runner.
  The runner sees a TaskUpdatedMessage with status ``failed`` and, for the
  ``Agent`` call, a ToolResultBlock with ``is_error=True`` whose content is
  ``Agent terminated early due to an API error: API Error: 402 This request
  requires more credits, ... (error type unknown, HTTP 402, model sent to the
  API: <subagent model>)``. Only with ``forward_subagent_text=True`` does the
  errored AssistantMessage arrive (and today's translator then emits the
  model-credit-exhausted ErrorEvent but still a DONE final);
- the CLI also sends side requests without tools (titles and the like); the
  stand-in answers those with plain text and the assertions ignore them;
- the CLI puts ``system`` role entries in ``messages``, so the stand-in ends the
  parent turn once any message carries a tool_result block.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Literal

import anyio
import pytest
from aci_protocol import ErrorEvent, Event, Final, SessionStatus, parse_ndjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from claude_agent_sdk import AgentDefinition
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options
from curie_runner.session import SessionRunner

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DOCKERFILE = _REPO_ROOT / "examples" / "dark-factory" / "runner.Dockerfile"
_CEILING_ENV = "CLAUDE_CODE_MAX_OUTPUT_TOKENS"
_CEILING_PATTERN = re.compile(rf"^ENV\s+{_CEILING_ENV}=(\d+)\s*$", re.MULTILINE)
# A verdict needs a few thousand tokens; the plan caps the bundle at 16000.
_CEILING_CAP = 16000

_MAIN_MODEL = "claude-sonnet-5"
_REVIEWER = "acme-reviewer"
_REVIEWER_MODEL = "acme-reviewer-model"
_AGENT_CALL_ID = "toolu_acme_review1"
# Recorded OpenRouter 402 body (2026-10-04), placeholder account.
_OPENROUTER_402 = {
    "error": {
        "message": (
            "This request requires more credits, or fewer max_tokens. You requested up "
            "to 64000 tokens, but can only afford 61300. To increase, visit "
            "https://openrouter.ai/settings/credits and upgrade to a paid account"
        ),
        "code": 402,
    }
}
_VERDICT = "REVIEWER: x\nVERDICT: APPROVE"

_REVIEWER_REPLY = web.AppKey("reviewer_reply", str)
_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_TURN_TIMEOUT_S = 120
_EPOCH = "acme-epoch-PLACEHOLDER"


def _dockerfile_ceiling() -> int | None:
    found = _CEILING_PATTERN.search(_DOCKERFILE.read_text(encoding="utf-8"))
    return int(found.group(1)) if found else None


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _carries_tool_result(body: dict[str, Any]) -> bool:
    for message in body.get("messages") or ():
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, list) and any(
            isinstance(block, dict) and block.get("type") == "tool_result" for block in content
        ):
            return True
    return False


def _is_side_request(body: dict[str, Any]) -> bool:
    """A CLI side request (title and the like): no tools, not the reviewer."""

    return body.get("model") != _REVIEWER_MODEL and not body.get("tools")


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


def _text_events(text: str) -> list[tuple[str, dict[str, Any]]]:
    return [
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": text},
            },
        ),
    ]


def _agent_call_events() -> list[tuple[str, dict[str, Any]]]:
    arguments = {
        "description": "Diff review round 1",
        "prompt": "review it",
        "subagent_type": _REVIEWER,
        "run_in_background": False,
    }
    return [
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "tool_use",
                    "id": _AGENT_CALL_ID,
                    "name": "Agent",
                    "input": {},
                },
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": json.dumps(arguments)},
            },
        ),
    ]


async def _stream(
    request: web.Request,
    body: dict[str, Any],
    blocks: list[tuple[str, dict[str, Any]]],
    stop_reason: str,
) -> web.StreamResponse:
    response = web.StreamResponse(headers={"content-type": "text/event-stream"})
    await response.prepare(request)
    events: list[tuple[str, dict[str, Any]]] = [
        (
            "message_start",
            {
                "type": "message_start",
                "message": _message_head(body, len(request.app[_BODIES])),
            },
        ),
        *blocks,
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


async def _messages(request: web.Request) -> web.StreamResponse:
    """Main: one Agent call, then end_turn. Reviewer: 402 or a verdict."""

    body = await request.json()
    request.app[_BODIES].append(body)
    if body.get("model") == _REVIEWER_MODEL:
        if request.app[_REVIEWER_REPLY] == "402":
            return web.json_response(_OPENROUTER_402, status=402)
        return await _stream(request, body, _text_events(_VERDICT), "end_turn")
    if _is_side_request(body):
        if not body.get("stream"):
            message = _message_head(body, len(request.app[_BODIES]))
            message["content"] = [{"type": "text", "text": "acme"}]
            message["stop_reason"] = "end_turn"
            return web.json_response(message)
        return await _stream(request, body, _text_events("acme"), "end_turn")
    if _carries_tool_result(body):
        return await _stream(request, body, _text_events("review finished"), "end_turn")
    return await _stream(request, body, _agent_call_events(), "tool_use")


async def _anything_else(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


def _run_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    reviewer_reply: Literal["402", "verdict"],
    ceiling: int | None,
) -> tuple[list[Any], list[dict[str, Any]]]:
    """One real turn in which the main model calls the reviewer subagent once.

    ``ceiling`` is set in ``os.environ``, never in the options env, so it
    travels the runner's own path (``ClaudeAgentSession.connect``) into the CLI
    the way the bundle image's ``ENV`` line does. Returns the turn's parsed
    outbound events and every /v1/messages body the stand-in received.
    """

    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    if ceiling is None:
        monkeypatch.delenv(_CEILING_ENV, raising=False)
    else:
        monkeypatch.setenv(_CEILING_ENV, str(ceiling))
    cwd = tmp_path / "workspace"
    cwd.mkdir()

    async def scenario() -> tuple[list[Any], list[dict[str, Any]]]:
        app = web.Application()
        app[_REVIEWER_REPLY] = reviewer_reply
        app[_BODIES] = []
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            options = build_options(
                plugins=[],
                model=_MAIN_MODEL,
                system_prompt="You run the acme review.",
                max_turns=4,
                max_budget_usd=None,
                resume=None,
                cwd=str(cwd),
                env={
                    "ANTHROPIC_API_KEY": "sk-ant-placeholder",
                    "ANTHROPIC_BASE_URL": str(server.make_url("")).rstrip("/"),
                    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
                    "DISABLE_TELEMETRY": "1",
                },
            )
            # build_options takes no agents parameter; the bundle's reviewers
            # reach the CLI as SDK agent definitions.
            options.agents = {
                _REVIEWER: AgentDefinition(
                    description="Reviews the acme diff and returns a verdict.",
                    prompt="You review the acme diff.",
                    model=_REVIEWER_MODEL,
                    tools=["Read"],
                )
            }
            runner = SessionRunner(
                max_usd_per_day=None,
                held_secrets=frozenset(),
                session_factory=lambda: ClaudeAgentSession(options),
                ceiling=0,
                tracer=RunTracer(None),
                classifier=SideEffectClassifier(),
                trace_name="curie-run:acme-reviewer-credit",
                session_id="session-PLACEHOLDER",
                model=_MAIN_MODEL,
            )
            lines: list[str] = []
            await runner.start()
            try:
                with anyio.fail_after(_TURN_TIMEOUT_S):
                    async for line in runner.run_turn(
                        Event(type="message", text="review it", user="U0EXAMPLE1", ts="1"),
                        turn_epoch=_EPOCH,
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            bodies: list[dict[str, Any]] = app[_BODIES]
            return list(parse_ndjson("".join(lines))), list(bodies)

    return anyio.run(scenario)


def _reviewer_bodies(bodies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [body for body in bodies if body.get("model") == _REVIEWER_MODEL]


def _model_bodies(bodies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Main and reviewer requests; side requests carry their own small budgets."""

    return [body for body in bodies if not _is_side_request(body)]


def _single_final(events: list[Any]) -> Final:
    finals = [event for event in events if isinstance(event, Final)]
    assert len(finals) == 1, events
    return finals[0]


def test_the_bundle_runner_image_caps_output_tokens() -> None:
    """AC2: the dark factory image sets a session-wide output ceiling.

    Red today: runner.Dockerfile has no ``ENV CLAUDE_CODE_MAX_OUTPUT_TOKENS``
    line, so every reviewer request asks OpenRouter to reserve 64000 tokens.
    """

    ceiling = _dockerfile_ceiling()
    assert ceiling is not None, f"no ENV {_CEILING_ENV}=<n> in {_DOCKERFILE}"
    assert 0 < ceiling <= _CEILING_CAP


def test_every_request_carries_the_bundle_ceiling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC2 on the real path: main and reviewer requests both carry the ceiling.

    Red today because the Dockerfile has no ceiling to read. Once it does, red
    again if the runner's env path (``cli_parent_env``) starts dropping the
    variable, or if a CLI upgrade stops applying it to subagent requests.
    """

    ceiling = _dockerfile_ceiling()
    assert ceiling is not None, f"no ENV {_CEILING_ENV}=<n> in {_DOCKERFILE}"
    _, bodies = _run_turn(tmp_path, monkeypatch, reviewer_reply="402", ceiling=ceiling)

    requests = _model_bodies(bodies)
    assert _reviewer_bodies(requests), bodies
    assert any(body.get("model") == _MAIN_MODEL for body in requests), bodies
    assert [body.get("max_tokens") for body in requests] == [ceiling] * len(requests)


def test_without_the_ceiling_the_reviewer_asks_above_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Negative control for AC2: the ceiling assertion is not vacuous.

    Green today. With the variable unset the reviewer asks for the CLI default
    (32000 measured for an unknown subagent model), above the bundle cap. Red if
    the CLI default ever drops under the cap, which would make the ceiling test
    pass without the image doing anything.
    """

    _, bodies = _run_turn(tmp_path, monkeypatch, reviewer_reply="402", ceiling=None)

    reviewer = _reviewer_bodies(bodies)
    assert len(reviewer) == 1, bodies
    bound = _dockerfile_ceiling() or _CEILING_CAP
    assert reviewer[0]["max_tokens"] > bound


def test_a_reviewer_402_ends_the_turn_out_of_credits(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC4: a reviewer refused for credit ends the turn as a classified failure.

    One reviewer request (the CLI does not retry a 402) holds today. Red on the
    ErrorEvent first: with the runner's options the subagent's errored
    AssistantMessage is not forwarded, so the only evidence is the Agent call's
    is_error tool result ("Agent terminated early due to an API error: API
    Error: 402 ..."), which today's translator does not classify. Red on the
    Final next: the parent recovers and ends with a success result, which
    ``_translate_result`` maps to DONE, so the worker delivers the turn instead
    of ending the run out of credits.
    """

    events, bodies = _run_turn(tmp_path, monkeypatch, reviewer_reply="402", ceiling=None)

    assert len(_reviewer_bodies(bodies)) == 1
    errors = [event for event in events if isinstance(event, ErrorEvent)]
    assert "model-credit-exhausted" in [error.classification for error in errors], events
    assert _single_final(events).status is SessionStatus.CLASSIFIED_FAILURE


def test_a_reviewer_returning_a_verdict_ends_the_turn_done(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Liveness for AC4: a reviewer that answers keeps the turn deliverable.

    Green today and must stay green: only a credit refusal turns a recovered
    turn terminal.
    """

    events, bodies = _run_turn(tmp_path, monkeypatch, reviewer_reply="verdict", ceiling=None)

    assert len(_reviewer_bodies(bodies)) == 1
    assert not [event for event in events if isinstance(event, ErrorEvent)], events
    assert _single_final(events).status is SessionStatus.DONE
