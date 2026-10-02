"""The platform ``get_issue`` tool for the GitHub factory (ADR 0187, #3748).

The tool mounts only when the worker injected the issue read route and its
execution scoped capability, presents the capability in its own header, and
returns the API's answer. Every HTTP case runs against a real local aiohttp
server that records what it received; nothing of ours is mocked.
"""

from __future__ import annotations

import json
from typing import Any

import anyio
import mcp.types as mcp_types
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from curie_runner.approval import ISSUE_TOOL_NAME, build_approval_server, is_platform_owned_tool
from curie_runner.issue_read import (
    CAPABILITY_HEADER,
    ISSUE_READ_TOKEN_ENV,
    ISSUE_READ_URL_ENV,
    build_issue_tool,
    resolve_issue_read,
)
from curie_runner.side_effects import PLATFORM_IDEMPOTENT_TOOLS
from curie_runner.subprocess_env import platform_credential_names

CAPABILITY = "wir.example-capability.signature"
ISSUE = {
    "repo_full_name": "acme-corp/acme-bot",
    "issue_number": 7,
    "title": "Add a flag",
    "body": "Acceptance criteria:\n\n1. It works.\n",
    "state": "open",
    "author": "octocat",
    "comments": [{"author": "hubot", "created_at": "2026-10-01T00:00:00Z", "body": "+1"}],
    "comments_truncated": False,
}


def _text(result: dict[str, Any]) -> str:
    return " ".join(str(item.get("text") or "") for item in result.get("content") or [])


class _Api:
    def __init__(self, status: int = 200, payload: Any = None) -> None:
        self.status = status
        self.payload = ISSUE if payload is None else payload
        self.received: list[tuple[dict[str, Any], str | None]] = []

    def app(self) -> web.Application:
        app = web.Application()

        async def read(request: web.Request) -> web.Response:
            self.received.append((await request.json(), request.headers.get(CAPABILITY_HEADER)))
            return web.json_response(self.payload, status=self.status)

        app.router.add_post("/work-items/issue-read", read)
        return app


def _call(api: _Api, args: dict[str, Any]) -> dict[str, Any]:
    async def go() -> dict[str, Any]:
        async with TestServer(api.app()) as server:
            tool = build_issue_tool(str(server.make_url("/work-items/issue-read")), CAPABILITY)
            return await tool.handler(args)

    return anyio.run(go)


ARGS = {"owner": "acme-corp", "repo": "acme-bot", "issue_number": 7}


def test_mounts_only_when_the_worker_injected_route_and_capability() -> None:
    assert resolve_issue_read({}) is None
    assert resolve_issue_read({ISSUE_READ_URL_ENV: "http://api/work-items/issue-read"}) is None
    assert resolve_issue_read({ISSUE_READ_TOKEN_ENV: CAPABILITY}) is None
    assert resolve_issue_read(
        {ISSUE_READ_URL_ENV: "http://api/work-items/issue-read", ISSUE_READ_TOKEN_ENV: CAPABILITY}
    ) == ("http://api/work-items/issue-read", CAPABILITY)


def test_the_curie_server_carries_get_issue_only_when_given() -> None:
    async def names(server: Any) -> set[str]:
        entry = server["instance"].get_request_handler("tools/list")
        assert entry is not None
        result = await entry.handler(None, mcp_types.PaginatedRequestParams())
        return {tool["name"] for tool in result.model_dump()["tools"]}

    tool = build_issue_tool("http://api/work-items/issue-read", CAPABILITY)
    assert ISSUE_TOOL_NAME == "mcp__curie__get_issue"
    with_tool = anyio.run(names, build_approval_server(issue_tool=tool))
    without = anyio.run(names, build_approval_server())
    assert "get_issue" not in without
    assert with_tool == without | {"get_issue"}


def test_get_issue_is_platform_owned_idempotent_and_its_token_is_scrubbed() -> None:
    assert is_platform_owned_tool(ISSUE_TOOL_NAME, state_server_mounted=False)
    assert ISSUE_TOOL_NAME in PLATFORM_IDEMPOTENT_TOOLS
    assert ISSUE_READ_TOKEN_ENV in platform_credential_names({ISSUE_READ_TOKEN_ENV: CAPABILITY})


def test_a_read_presents_the_capability_and_returns_the_issue_verbatim() -> None:
    api = _Api()
    result = _call(api, ARGS)
    assert result.get("is_error") is not True
    assert json.loads(_text(result)) == ISSUE
    assert api.received == [
        ({"repo_full_name": "acme-corp/acme-bot", "issue_number": 7}, CAPABILITY)
    ]
    assert CAPABILITY not in _text(result)


@pytest.mark.parametrize(
    "args",
    [
        {"owner": "acme-corp", "repo": "acme-bot"},
        {"owner": "", "repo": "acme-bot", "issue_number": 7},
        {"owner": "acme-corp", "repo": "acme-bot", "issue_number": True},
        {"owner": "acme-corp", "repo": "acme-bot", "issue_number": 0},
    ],
)
def test_malformed_arguments_are_refused_without_a_request(args: dict[str, Any]) -> None:
    api = _Api()
    result = _call(api, args)
    assert result.get("is_error") is True
    assert api.received == []


@pytest.mark.parametrize(
    ("status", "payload", "needle"),
    [
        (
            403,
            {
                "detail": {
                    "code": "issue_not_granted",
                    "message": "this execution may read only issue 7 of acme-corp/acme-bot",
                }
            },
            "issue 7 of acme-corp/acme-bot",
        ),
        (409, {"detail": {"code": "invalid_context"}}, "ended"),
        (503, {"detail": {"code": "issue_unavailable"}}, "Retry"),
        (401, {"detail": {"code": "invalid_capability"}}, "401"),
    ],
)
def test_a_refusal_is_a_tool_error_that_never_echoes_the_capability(
    status: int, payload: dict[str, Any], needle: str
) -> None:
    result = _call(_Api(status, payload), ARGS)
    assert result.get("is_error") is True
    assert needle in _text(result)
    assert CAPABILITY not in _text(result)


def test_an_unreachable_api_is_a_tool_error_not_an_exception() -> None:
    tool = build_issue_tool("http://127.0.0.1:9/work-items/issue-read", CAPABILITY)
    result = anyio.run(tool.handler, ARGS)
    assert result.get("is_error") is True
    assert "127.0.0.1" not in _text(result)


def test_a_response_delivered_in_separate_writes_is_read_whole() -> None:
    payload = json.dumps(ISSUE).encode()

    async def go() -> dict[str, Any]:
        app = web.Application()

        async def read(request: web.Request) -> web.StreamResponse:
            response = web.StreamResponse(headers={"Content-Type": "application/json"})
            await response.prepare(request)
            for start in range(0, len(payload), 16):
                await response.write(payload[start : start + 16])
                await response.drain()
                await anyio.sleep(0.01)
            await response.write_eof()
            return response

        app.router.add_post("/work-items/issue-read", read)
        async with TestServer(app) as server:
            tool = build_issue_tool(str(server.make_url("/work-items/issue-read")), CAPABILITY)
            return await tool.handler(ARGS)

    result = anyio.run(go)
    assert result.get("is_error") is not True, _text(result)
    assert json.loads(_text(result)) == ISSUE
