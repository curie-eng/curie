"""A resumed turn sends the system prompt its own boot composed.

Two turns through the real bundled Claude CLI, against a local stand-in for the
Messages API that records each request. The first turn leaves the native
checkpoint the runner persists; the second boots from it with a system prompt
that announces this turn's attachment, exactly as the runner composes one for a
follow-up message that carries a file. The CLI restores a prompt recorded in
its checkpoint on resume, so a replay that trusts that checkpoint sends the
first turn's prompt and the model is never told the file is there.

No credential and no network: the API key is a placeholder and the base URL is
the local stand-in.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import anyio
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.adapter import (
    ClaudeAgentSession,
    build_options,
    build_structured_resume,
    model_message_to_conversation,
)
from curie_runner.history import ConversationMessage, HarnessReplayState

_BODIES = web.AppKey("bodies", list[dict[str, Any]])
_BUNDLE_PROMPT = "You file finished marketing assets."
_ATTACHMENT_PREAMBLE = (
    "The message you are answering carried file attachments.\n- /attachments/all-up-messaging-v1.md"
)


def _sse(event: str, data: dict[str, Any]) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def _system_text(body: dict[str, Any]) -> str:
    system = body.get("system")
    if isinstance(system, str):
        return system
    return "\n".join(block.get("text", "") for block in system or () if isinstance(block, dict))


async def _messages(request: web.Request) -> web.StreamResponse:
    body = await request.json()
    request.app[_BODIES].append(body)
    response = web.StreamResponse(headers={"content-type": "text/event-stream"})
    await response.prepare(request)
    message = {
        "id": "msg_stand_in",
        "type": "message",
        "role": "assistant",
        "model": body.get("model", "stand-in"),
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    for event, data in (
        ("message_start", {"type": "message_start", "message": message}),
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
                "delta": {"type": "text_delta", "text": "ok"},
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": "end_turn", "stop_sequence": None},
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ):
        await response.write(_sse(event, data))
    await response.write_eof()
    return response


async def _anything_else(request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


async def _turn(
    *,
    base_url: str,
    cwd: str,
    system_prompt: str,
    prior: tuple[ConversationMessage, ...],
    replay: HarnessReplayState | None,
    query: str,
) -> tuple[list[ConversationMessage], HarnessReplayState | None]:
    resume = build_structured_resume(
        prior,
        curie_session_id="curie-thread-follow-up",
        cwd=cwd,
        harness_replay=replay,
        system_prompt=system_prompt,
    )
    options = build_options(
        plugins=[],
        model="claude-sonnet-5",
        system_prompt=system_prompt,
        max_turns=2,
        max_budget_usd=None,
        resume=resume.resume,
        session_id=resume.session_id,
        session_store=resume.session_store,
        cwd=cwd,
        env={
            "ANTHROPIC_API_KEY": "sk-ant-placeholder",
            "ANTHROPIC_BASE_URL": base_url,
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "DISABLE_TELEMETRY": "1",
        },
    )
    session = ClaudeAgentSession(options)
    await session.connect()
    try:
        await session.query(query)
        messages = [
            projected
            for message in [m async for m in session.receive_turn()]
            if (projected := model_message_to_conversation(message)) is not None
        ]
        exported = await session.export_replay_state()
    finally:
        await session.close()
    return messages, exported


def test_a_follow_up_turn_sends_the_attachment_its_boot_announced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The SDK mirrors the CLI's session file only from under the parent's own
    # config directory, so parent and CLI must agree on it.
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-config"))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    cwd = tmp_path / "workspace"
    cwd.mkdir()

    async def scenario() -> list[dict[str, Any]]:
        app = web.Application()
        app[_BODIES] = []
        app.router.add_post("/v1/messages", _messages)
        app.router.add_route("*", "/{tail:.*}", _anything_else)
        async with TestServer(app, host="127.0.0.1") as server:
            base_url = str(server.make_url("")).rstrip("/")
            first_user = ConversationMessage(role="user", content="what can you do?")
            first_reply, checkpoint = await _turn(
                base_url=base_url,
                cwd=str(cwd),
                system_prompt=_BUNDLE_PROMPT,
                prior=(),
                replay=None,
                query="what can you do?",
            )
            assert checkpoint is not None and checkpoint.kind == "checkpoint"
            first_turn_requests = len(app[_BODIES])
            await _turn(
                base_url=base_url,
                cwd=str(cwd),
                system_prompt=f"{_BUNDLE_PROMPT}\n\n{_ATTACHMENT_PREAMBLE}",
                prior=(first_user, *first_reply),
                replay=checkpoint,
                query="file the attached finished asset",
            )
            return list(app[_BODIES][first_turn_requests:])

    follow_up = anyio.run(scenario)

    assert follow_up, "the follow-up turn made no provider request"
    assert _ATTACHMENT_PREAMBLE in _system_text(follow_up[-1])
