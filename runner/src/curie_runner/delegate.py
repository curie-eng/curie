"""PROTOTYPE: the ``curie-delegate`` MCP server (Draft ADR-0115, not accepted).

**This is a demo prototype, not the ADR-0115 implementation.** See
``docs/demo/ADR-0115-PROTOTYPE-NOTES.md`` for the documented deviations
(reused job-lane ``TurnSource``, no formal suspend/resume, no bundle-declared
allowlist or on-wire call chain/depth). Modeled directly on
``runner/src/curie_runner/state.py`` -- the auto-mounted-MCP-server pattern
ADR-0073 established and this prototype reuses rather than inventing a new one.

``CURIE_DELEGATE_URL`` / ``CURIE_DELEGATE_TOKEN`` are the boot-env pair
(mirrors ``CURIE_STATE_URL``/``CURIE_STATE_TOKEN``): the URL is this agent's
``.../agents/<id>/delegate/calls`` endpoint, already fully composed (unlike the
state namespace base, there is no ``<namespace>/<key>`` to append), and the
token is a scoped ADR-0033 ``delegate`` token, never the raw platform key.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote

import aiohttp
from aci_protocol import BootEnv
from claude_agent_sdk import create_sdk_mcp_server, tool
from claude_agent_sdk.types import McpSdkServerConfig

logger = logging.getLogger(__name__)

DELEGATE_SERVER_NAME = "curie-delegate"

DELEGATE_URL_ENV = BootEnv.env_key("delegate_url")
DELEGATE_TOKEN_ENV = BootEnv.env_key("delegate_token")

# CURIE_HISTORY_REF's trailing path segment is this thread's conversation key
# (binding.py composes it as .../state/transcript/<quoted thread_key>) -- a
# stable per-turn identifier already minted for a different purpose (ADR-0029),
# read here rather than adding a new BootEnv field just to restate it.
HISTORY_REF_ENV = BootEnv.env_key("history_ref")

_TIMEOUT_SECONDS = 15


class DelegateError(RuntimeError):
    """A delegate call could not be submitted to the API."""


def _ok(payload: Any) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(payload)}]}


def _err(text: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "is_error": True}


def _conversation_id_from_history_ref(history_ref: str | None) -> str | None:
    if not history_ref:
        return None
    tail = history_ref.rstrip("/").rsplit("/", 1)[-1]
    return unquote(tail) or None


class DelegateApiClient:
    """Thin async client over the (prototype) delegate-calls route.

    ``base_url`` is the fully composed ``.../agents/<id>/delegate/calls``
    endpoint. The scoped ``delegate`` token (ADR-0033) rides ``X-API-Key``,
    never logged.
    """

    def __init__(
        self, base_url: str, token: str | None, caller_conversation_id: str | None
    ) -> None:
        self._base = base_url
        self._token = token
        self._caller_conversation_id = caller_conversation_id

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["X-API-Key"] = self._token
        return headers

    @staticmethod
    def _timeout() -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS)

    async def call_agent(self, target_agent: str, message: str) -> dict[str, Any]:
        if not self._caller_conversation_id:
            raise DelegateError(
                "no conversation id available for this turn (CURIE_HISTORY_REF unset)"
            )
        body = {
            "target_agent": target_agent,
            "message": message,
            "caller_conversation_id": self._caller_conversation_id,
        }
        async with aiohttp.ClientSession(timeout=self._timeout()) as session:
            async with session.post(
                self._base, data=json.dumps(body), headers=self._headers()
            ) as resp:
                if resp.status != 201:
                    text = (await resp.text())[:200]
                    raise DelegateError(f"delegate call failed: {resp.status} {text}")
                return await resp.json()  # type: ignore[no-any-return]


_CALL_SCHEMA = {
    "type": "object",
    "properties": {
        "target_agent": {
            "type": "string",
            "description": "The name of the agent to ask.",
        },
        "message": {
            "type": "string",
            "description": "What to ask the target agent to do.",
        },
    },
    "required": ["target_agent", "message"],
}


async def op_call_agent(client: DelegateApiClient, args: dict[str, Any]) -> dict[str, Any]:
    target_agent, message = args["target_agent"], args["message"]
    try:
        result = await client.call_agent(target_agent, message)
    except (DelegateError, aiohttp.ClientError) as exc:
        return _err(f"delegate call failed: {exc}")
    return _ok(
        {
            "call_id": result.get("id"),
            "status": result.get("status"),
            "note": (
                f"Asked {target_agent!r}. This is asynchronous: the reply, if any, "
                "will arrive later as a new message in this conversation, not as "
                "this tool call's return value. (Prototype note: no formal "
                "suspend -- it is safe to end your turn now.)"
            ),
        }
    )


def build_delegate_server(client: DelegateApiClient) -> McpSdkServerConfig:
    """The in-process ``curie-delegate`` MCP server bound to ``client``.

    Auto-mounted like ``curie-state``/the approval server -- a bundle never
    ships this itself, and a bundle-declared MCP server of the same name is
    shadowed by the platform's (``connectors.py``'s platform-wins merge).
    """

    @tool("call_agent", "Ask another agent to do something.", _CALL_SCHEMA)
    async def call_agent_tool(args: dict[str, Any]) -> dict[str, Any]:
        return await op_call_agent(client, args)

    return create_sdk_mcp_server(
        name=DELEGATE_SERVER_NAME, version="1.0.0", tools=[call_agent_tool]
    )


def resolve_delegate_client(env: Mapping[str, str]) -> DelegateApiClient | None:
    """Build a ``DelegateApiClient`` from the boot env, or None when unconfigured.

    An absent ``CURIE_DELEGATE_URL`` (no platform key minted one, or an older
    worker) yields None, so the runner mounts no delegate server and an agent
    with nothing to call sees no phantom tool.
    """

    base_url = env.get(DELEGATE_URL_ENV)
    if not base_url:
        return None
    caller_conversation_id = _conversation_id_from_history_ref(env.get(HISTORY_REF_ENV))
    return DelegateApiClient(base_url, env.get(DELEGATE_TOKEN_ENV), caller_conversation_id)
