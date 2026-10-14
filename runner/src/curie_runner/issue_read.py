"""The platform ``get_issue`` tool for the factory (ADR 0187, #4358).

The sandbox holds no tracker credential. For an execution with a WorkItem the
worker injects the API's issue read route and an execution scoped capability
naming that WorkItem's issue, on whichever tracker holds it. When both are
present the runner mounts ``get_issue`` on the ``curie`` server; the tool takes
no arguments, presents the capability, and the API reads the ticket with its
own tracker credential and answers it as markdown. The capability never
reaches model text, tool results or logs.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

import aiohttp
from aci_protocol import BootEnv
from claude_agent_sdk import SdkMcpTool, tool

logger = logging.getLogger(__name__)

ISSUE_READ_URL_ENV = BootEnv.env_key("issue_read_url")
ISSUE_READ_TOKEN_ENV = BootEnv.env_key("issue_read_token")
ISSUE_TOOL = "get_issue"
CAPABILITY_HEADER = "X-Curie-Issue-Read"
_TIMEOUT_SECONDS = 30
_MAX_RESPONSE_BYTES = 4_194_304

_DESCRIPTION = (
    "Read the issue this factory run was started for, from whichever tracker "
    "holds it, as markdown with its comments, plus this execution's deadline. "
    "Takes no arguments: only this run's own issue can be read."
)
_SCHEMA: dict[str, Any] = {"type": "object", "properties": {}}


def _error(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "is_error": True}


def resolve_issue_read(env: Mapping[str, str]) -> tuple[str, str] | None:
    """The route and capability when the worker injected both, else None."""

    url = env.get(ISSUE_READ_URL_ENV, "").strip()
    token = env.get(ISSUE_READ_TOKEN_ENV, "").strip()
    if not url or not token:
        return None
    return url, token


async def _read(url: str, token: str, body: dict[str, Any]) -> tuple[int, Any]:
    async with (
        aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS), trust_env=False
        ) as session,
        session.post(
            url,
            json=body,
            headers={CAPABILITY_HEADER: token},
            allow_redirects=False,
        ) as response,
    ):
        raw = bytearray()
        async for chunk in response.content.iter_any():
            raw.extend(chunk)
            if len(raw) > _MAX_RESPONSE_BYTES:
                raise ValueError("issue read response is too large")
        return response.status, json.loads(raw)


def build_issue_tool(url: str, token: str) -> SdkMcpTool[Any]:
    """The SDK tool ``get_issue``, closed over the route and capability."""

    @tool(ISSUE_TOOL, _DESCRIPTION, _SCHEMA)
    async def get_issue(args: dict[str, Any]) -> dict[str, Any]:
        # The capability names the one issue; nothing the model passes is sent.
        del args
        try:
            status, payload = await _read(url, token, {})
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            # Never render a transport diagnostic: it can name the endpoint.
            logger.warning("issue read transport failure: %s", type(exc).__name__)
            return _error("The issue could not be read right now. Retry shortly.")
        if status == 200 and isinstance(payload, dict):
            return {"content": [{"type": "text", "text": json.dumps(payload, indent=2)}]}
        if status == 409:
            return _error("This execution has ended; its issue can no longer be read.")
        if status == 503:
            return _error("The tracker could not answer the issue read. Retry shortly.")
        return _error(f"The issue read was refused (status {status}).")

    return get_issue
