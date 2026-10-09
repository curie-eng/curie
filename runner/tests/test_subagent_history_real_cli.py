"""A turn that used a subagent resumes on a fresh runner, on the real CLI (#4336).

When an implement turn calls the ``Agent`` tool, the subagent's own prompt,
tool calls and tool results stream through the parent's SDK iterator with
``parent_tool_use_id`` set. If those rows land in the turn's portable history,
an owner_lost successor cannot boot from it (``UnprovableAssistantGroupingError``
at ``build_structured_resume``), or it replays the subagent's work as if the
parent had done it. This case puts the real bundled CLI between two
``SessionRunner`` instances and a local stand-in for /v1/messages: no
credential and no network. The stand-in tells the subagent's requests apart by
``body["model"]``.

Observed with claude-agent-sdk 0.2.159 and its bundled CLI 2.1.281
(2026-10-09):

- the subagent's first request carries one user message whose text includes the
  ``prompt`` the parent passed to ``Agent``; the stand-in answers it with two
  ``tool_use`` blocks (``Read`` and ``Glob``) in one assistant message, the CLI
  runs both, and their ``tool_result`` blocks reach the subagent's next request;
- the runner's SDK iterator receives, all with ``parent_tool_use_id`` set to the
  ``Agent`` call's id and before the parent's ``Agent`` tool_result: the
  subagent's prompt as a ``UserMessage`` holding one text block (not a string),
  the ``Read`` and ``Glob`` calls as two separate ``AssistantMessage`` objects,
  and their results as two separate ``UserMessage`` objects;
- on the code this test was written against, every one of those rows lands in
  the turn's portable history. The nested prompt then splits the replay into two
  turns, the per-turn reducer cuts the second to its visible text
  (``turns_reduced=1``), and the successor's first main request carries neither
  the ``Agent`` call nor its result;
- the CLI also sends side requests without tools (titles and the like); the
  stand-in answers those with plain text and the assertions ignore them.

The second runner boots the way ``__main__`` does: ``build_conversation_replay``
over the stored records, then ``build_structured_resume`` with the replay's
messages and native checkpoint, then ``build_options`` from its result. Each
runner gets its own ``CLAUDE_CONFIG_DIR``, so the second CLI sees no local
session file the first one left.
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
from claude_agent_sdk import AgentDefinition
from curie_runner import RunTracer, SideEffectClassifier
from curie_runner.adapter import ClaudeAgentSession, build_options, build_structured_resume
from curie_runner.history import (
    DEFAULT_REPLAY_MAX_BYTES,
    DEFAULT_REPLAY_MAX_TURNS,
    ConversationReplay,
    HistoryRecord,
    build_conversation_replay,
)
from curie_runner.session import SessionRunner

_MAIN_MODEL = "claude-sonnet-5"
_SUBAGENT = "acme-explorer"
_SUBAGENT_MODEL = "acme-explorer-model"
# The stand-in never checks it; the CLI only needs a key to send.
_PLACEHOLDER_KEY = "sk-ant-placeholder"
_AGENT_CALL_ID = "toolu_acme_agent1"
_READ_CALL_ID = "toolu_acme_read1"
_GLOB_CALL_ID = "toolu_acme_glob1"
_SUBAGENT_PROMPT = "Find the acme notes and summarize them PLACEHOLDER-SUBAGENT-PROMPT"
_NOTES_TEXT = "acme notes body PLACEHOLDER-FILE-CONTENT"
_SYSTEM_PROMPT = "You run the acme exploration."
_SESSION_ID = "session-PLACEHOLDER-subagent"
_EPOCH = "acme-epoch-PLACEHOLDER"
_TURN_TIMEOUT_S = 120

_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_WORKSPACE = web.AppKey("workspace", str)

Block = list[tuple[str, dict[str, Any]]]


class _Store:
    """The runner's transcript port, kept in memory between the two runners."""

    def __init__(self) -> None:
        self.records: list[HistoryRecord] = []

    async def load(self) -> list[HistoryRecord]:
        return list(self.records)

    async def append(self, record: HistoryRecord) -> bool:
        self.records.append(record)
        return getattr(record, "harness_replay", None) is not None


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


def _all_blocks(body: dict[str, Any], role: str | None = None) -> list[dict[str, Any]]:
    return [
        block
        for message in _messages_of(body)
        if role is None or message.get("role") == role
        for block in _blocks(message)
    ]


def _last_user_carries_tool_result(body: dict[str, Any]) -> bool:
    users = [message for message in _messages_of(body) if message.get("role") == "user"]
    return bool(users) and any(block.get("type") == "tool_result" for block in _blocks(users[-1]))


def _is_side_request(body: dict[str, Any]) -> bool:
    """A CLI side request (title and the like): no tools, not the subagent."""

    return body.get("model") != _SUBAGENT_MODEL and not body.get("tools")


def _is_main_request(body: dict[str, Any]) -> bool:
    return body.get("model") == _MAIN_MODEL and not _is_side_request(body)


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
    """Main: one Agent call, then text. Subagent: parallel Read and Glob, then text."""

    body = await request.json()
    request.app[_BODIES].append(body)
    if body.get("model") == _SUBAGENT_MODEL:
        if _last_user_carries_tool_result(body):
            return await _stream(request, body, [_text_block("the notes say acme")], "end_turn")
        workspace = request.app[_WORKSPACE]
        calls = [
            _tool_block(_READ_CALL_ID, "Read", {"file_path": f"{workspace}/notes.md"}),
            _tool_block(_GLOB_CALL_ID, "Glob", {"pattern": "*.md", "path": workspace}),
        ]
        return await _stream(request, body, calls, "tool_use")
    if _is_side_request(body):
        if not body.get("stream"):
            message = _message_head(body, len(request.app[_BODIES]))
            message["content"] = [{"type": "text", "text": "acme"}]
            message["stop_reason"] = "end_turn"
            return web.json_response(message)
        return await _stream(request, body, [_text_block("acme")], "end_turn")
    # Any tool_result means the Agent call was answered: in turn 1 by the
    # subagent, in turn 2 by the replayed history. Either way the parent is done.
    if _tool_results(body):
        return await _stream(request, body, [_text_block("exploration finished")], "end_turn")
    agent_call = _tool_block(
        _AGENT_CALL_ID,
        "Agent",
        {
            "description": "Explore the acme notes",
            "prompt": _SUBAGENT_PROMPT,
            "subagent_type": _SUBAGENT,
            "run_in_background": False,
        },
    )
    return await _stream(request, body, [agent_call], "tool_use")


async def _anything_else(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


async def _run_one_turn(
    *,
    base_url: str,
    cwd: Path,
    replay: ConversationReplay,
    store: _Store,
    text: str,
) -> list[Any]:
    """Boot one runner the way ``__main__`` does and run a single turn on it."""

    resume = build_structured_resume(
        replay.messages,
        curie_session_id=_SESSION_ID,
        cwd=str(cwd),
        harness_replay=replay.harness_replay,
        system_prompt=_SYSTEM_PROMPT,
    )
    options = build_options(
        plugins=[],
        model=_MAIN_MODEL,
        system_prompt=_SYSTEM_PROMPT,
        max_turns=6,
        max_budget_usd=None,
        resume=resume.resume,
        session_id=resume.session_id,
        session_store=resume.session_store,
        cwd=str(cwd),
        env={
            "ANTHROPIC_API_KEY": _PLACEHOLDER_KEY,
            "ANTHROPIC_BASE_URL": base_url,
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "DISABLE_TELEMETRY": "1",
        },
    )
    # build_options takes no agents parameter; bundle subagents reach the CLI
    # as SDK agent definitions.
    options.agents = {
        _SUBAGENT: AgentDefinition(
            description="Explores the acme workspace and reports what it finds.",
            prompt="You explore the acme workspace.",
            model=_SUBAGENT_MODEL,
            tools=["Read", "Glob"],
        )
    }
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
        history_store=store,
        history_resumed=replay.present,
    )
    lines: list[str] = []
    await runner.start()
    try:
        with anyio.fail_after(_TURN_TIMEOUT_S):
            async for line in runner.run_turn(
                Event(type="message", text=text, user="U0EXAMPLE1", ts="1"),
                turn_epoch=_EPOCH,
            ):
                lines.append(line)
    finally:
        await runner.close()
    return list(parse_ndjson("".join(lines)))


def _single_final(events: list[Any]) -> Final:
    finals = [event for event in events if isinstance(event, Final)]
    assert len(finals) == 1, events
    return finals[0]


def _tool_uses(body: dict[str, Any]) -> list[dict[str, Any]]:
    return [block for block in _all_blocks(body, "assistant") if block.get("type") == "tool_use"]


def _tool_results(body: dict[str, Any]) -> list[dict[str, Any]]:
    return [block for block in _all_blocks(body, "user") if block.get("type") == "tool_result"]


def _user_text(body: dict[str, Any]) -> str:
    return "\n".join(
        str(block.get("text", ""))
        for block in _all_blocks(body, "user")
        if block.get("type") == "text"
    )


def test_a_turn_that_used_a_subagent_resumes_on_a_fresh_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """AC 6: the successor boots from a subagent turn and replays only the parent's view.

    Red before #4336 on the ``Agent`` assertion: the subagent's prompt,
    ``Read``/``Glob`` calls and their results are recorded into the parent's
    portable history, the replay reducer cuts the turn they split to text, and
    the successor never sees the ``Agent`` call. Had the reducer not run,
    ``build_structured_resume`` would refuse the history or the successor would
    replay the subagent's calls; the remaining assertions pin both.
    """

    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()
    (cwd / "notes.md").write_text(f"{_NOTES_TEXT}\n", encoding="utf-8")
    store = _Store()

    async def scenario() -> tuple[list[Any], list[Any], list[dict[str, Any]], int]:
        app = web.Application()
        app[_BODIES] = []
        app[_WORKSPACE] = str(cwd.resolve())
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            base_url = str(server.make_url("")).rstrip("/")
            monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config-first"))
            first = await _run_one_turn(
                base_url=base_url,
                cwd=cwd,
                replay=ConversationReplay(),
                store=store,
                text="explore the acme notes",
            )
            first_turn_requests = len(app[_BODIES])
            # A successor pod: fresh CLI config, history only from the store.
            monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config-second"))
            replay, _summary = build_conversation_replay(
                await store.load(),
                max_turns=DEFAULT_REPLAY_MAX_TURNS,
                max_bytes=DEFAULT_REPLAY_MAX_BYTES,
            )
            second = await _run_one_turn(
                base_url=base_url,
                cwd=cwd,
                replay=replay,
                store=store,
                text="what did the notes say?",
            )
            bodies: list[dict[str, Any]] = app[_BODIES]
            return first, second, list(bodies), first_turn_requests

    first, second, bodies, split = anyio.run(scenario)
    first_bodies, second_bodies = bodies[:split], bodies[split:]

    # Turn 1 really ran a subagent, and the subagent really ran both tools.
    subagent = [body for body in first_bodies if body.get("model") == _SUBAGENT_MODEL]
    assert subagent, first_bodies
    assert _SUBAGENT_PROMPT in _user_text(subagent[0])
    answered = {
        block.get("tool_use_id"): block for body in subagent for block in _tool_results(body)
    }
    assert {_READ_CALL_ID, _GLOB_CALL_ID} <= set(answered), subagent
    assert _NOTES_TEXT in json.dumps(answered[_READ_CALL_ID].get("content"))
    assert "notes.md" in json.dumps(answered[_GLOB_CALL_ID].get("content"))
    assert _single_final(first).status is SessionStatus.DONE

    # Turn 2 boots from the stored history and replays only the parent's view.
    assert _single_final(second).status is SessionStatus.DONE
    main = [body for body in second_bodies if _is_main_request(body)]
    assert main, second_bodies
    replayed = main[0]
    calls = _tool_uses(replayed)
    results = _tool_results(replayed)
    assert [call.get("name") for call in calls if call.get("id") == _AGENT_CALL_ID] == ["Agent"]
    assert _AGENT_CALL_ID in [result.get("tool_use_id") for result in results]
    assert not [call for call in calls if call.get("name") in ("Read", "Glob")], calls
    assert not [
        result for result in results if result.get("tool_use_id") in (_READ_CALL_ID, _GLOB_CALL_ID)
    ], results
    assert _SUBAGENT_PROMPT not in _user_text(replayed)
