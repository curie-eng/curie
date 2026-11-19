"""Scripted model and reply endpoints for the container crash proof."""

from __future__ import annotations

import asyncio
import json
import os

from aiohttp import web

PHASE = os.environ["CRASH_PHASE"]
GATE = asyncio.Event()
STATE = {
    "before": False,
    "during": False,
    "after": False,
    "replies": [],
    "tool_starts": 0,
    "requests": 0,
}


async def messages(request: web.Request) -> web.StreamResponse:
    body = await request.json()
    STATE["requests"] += 1
    has_result = any(
        isinstance(block, dict) and block.get("type") == "tool_result"
        for message in body.get("messages", [])
        if isinstance(message.get("content"), list)
        for block in message["content"]
    )
    stage = "after" if has_result else "before"
    STATE[stage] = True
    if PHASE == stage:
        await GATE.wait()
    response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
    await response.prepare(request)
    # https://platform.claude.com/docs/en/build-with-claude/streaming
    head = {
        "id": f"msg_crash_{STATE['requests']}",
        "type": "message",
        "role": "assistant",
        "model": body["model"],
        "content": [],
        "stop_reason": None,
        "stop_sequence": None,
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }
    if has_result:
        block = {"type": "text", "text": ""}
        delta = {"type": "text_delta", "text": "recovered"}
        reason = "end_turn"
    else:
        block = {
            "type": "tool_use",
            "id": f"toolu_crash_{STATE['requests']}",
            "name": "Bash",
            "input": {},
        }
        command = (
            "curl -fsS http://crash-provider:8089/tool/start && "
            "curl -fsS http://crash-provider:8089/tool/hold && "
            "curl -fsS http://crash-provider:8089/tool/end"
        )
        delta = {
            "type": "input_json_delta",
            "partial_json": json.dumps(
                {
                    "command": command,
                    "description": "exercise the crash recovery boundary",
                }
            ),
        }
        reason = "tool_use"
    events = [
        ("message_start", {"type": "message_start", "message": head}),
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
                "delta": {
                    "stop_reason": reason,
                    "stop_sequence": None,
                },
                "usage": {"output_tokens": 1},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]
    for event, data in events:
        await response.write(f"event: {event}\ndata: {json.dumps(data)}\n\n".encode())
    await response.write_eof()
    return response


async def tool(request: web.Request) -> web.Response:
    stage = request.match_info["stage"]
    if stage == "start":
        STATE["tool_starts"] += 1
    if stage == "hold":
        STATE["during"] = True
        if PHASE == "during":
            await GATE.wait()
    return web.Response(text="ok")


async def release(_request: web.Request) -> web.Response:
    GATE.set()
    return web.json_response({"released": True})


async def state(_request: web.Request) -> web.Response:
    return web.json_response(STATE)


async def reply(request: web.Request) -> web.Response:
    STATE["replies"].append(await request.json())
    return web.json_response({"ref": "reply-PLACEHOLDER"})


async def other(_request: web.Request) -> web.Response:
    return web.json_response({"input_tokens": 1})


if __name__ == "__main__":
    app = web.Application()
    app.add_routes(
        [
            web.post("/v1/messages", messages),
            web.get("/tool/{stage}", tool),
            web.post("/release", release),
            web.get("/state", state),
            web.post("/reply", reply),
            web.route("*", "/{tail:.*}", other),
        ]
    )
    web.run_app(app, host="0.0.0.0", port=8089, print=None)
