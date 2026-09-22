"""The ``curie-state`` MCP server: bundle code's door to the durable store (#249).

The durable workflow-state store (#23/#248) is the API state router over Postgres
JSONB (``apps/api`` ``/agents/{id}/state/{namespace}/{key}``). Memory (#264) and
conversation history (#20) already ride it as two fixed namespaces. This module
exposes the REST of that store to a running bundle skill two ways, without the
bundle shipping its own MCP server or a datastore of its own:

- **The auto-mounted ``curie-state`` MCP server.** ``build_state_server``
  returns an in-process SDK MCP server carrying get / set / list / delete /
  append tools, wired by the runner when the state boot environment is present.
  Like the conditionally applicable approval server (ADR-0010), it is a
  platform-supplied server rather than bundle content. A skill reads and writes state by calling
  ``mcp__curie-state__*`` -- and because the backing is Postgres JSONB outside
  the sandbox (ADR-0003, stateless-first), the data survives a suspend/resume.
- **The ``CURIE_STATE_URL`` / ``CURIE_STATE_TOKEN`` env pair** for a bundle
  script that would rather talk to the store directly (a shell/python step, not
  the model). Same URL and scoped token this module dereferences.

``CURIE_STATE_URL`` is the agent's state namespace base
(``.../agents/<id>/state``); a ``<namespace>/<key>`` is composed onto it. The
runner authenticates with ``CURIE_STATE_TOKEN``, the per-turn scoped ``state``
token (ADR-0033), presented verbatim as the ``X-API-Key`` header -- never the raw
platform key, and never logged.

``memory`` and ``transcript`` are reserved: they are the memory (#264) and
history (#20) namespaces, and a skill writing them would corrupt the agent's
learned lessons or its own conversation transcript. Every tool refuses them with
an ``is_error`` result the model can read and recover from.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import quote

import aiohttp
from aci_protocol import BootEnv
from claude_agent_sdk import create_sdk_mcp_server, tool
from claude_agent_sdk.types import McpSdkServerConfig

logger = logging.getLogger(__name__)

# The SDK server key; the SDK prefixes tool names as mcp__<server>__<tool>.
STATE_SERVER_NAME = "curie-state"

# Runner-local env carrying the state namespace base URL and the bearer the
# state API expects (X-API-Key). Both are declared BootEnv fields (#249, #488),
# so the NAMES are read from that one declaration rather than retyped here.
STATE_URL_ENV = BootEnv.env_key("state_url")
STATE_TOKEN_ENV = BootEnv.env_key("state_token")

# Namespaces already owned by the memory (#264) and history (#20) ports. A skill
# must not read or write them through the general state door, so both are fenced
# off in one place (a bundle that wants durable memory uses the remember tool).
RESERVED_NAMESPACES = frozenset({"memory", "transcript"})

# How long any single state call may take. The store is one Postgres round-trip;
# a longer hang is a fault the tool should surface rather than wedge the turn on.
_TIMEOUT_SECONDS = 15


class StateError(RuntimeError):
    """A state operation could not be completed against the store."""


def _ok(payload: Any) -> dict[str, Any]:
    """A successful tool result carrying a JSON-serialized ``payload``."""
    return {"content": [{"type": "text", "text": json.dumps(payload)}]}


def _err(text: str) -> dict[str, Any]:
    """An ``is_error`` tool result the model reads and can recover from."""
    return {"content": [{"type": "text", "text": text}], "is_error": True}


def _validate_namespace(namespace: str) -> str | None:
    """The refusal text for a reserved/empty namespace, or None when allowed."""
    if not namespace:
        return "namespace must be a non-empty string"
    if namespace in RESERVED_NAMESPACES:
        return (
            f"namespace {namespace!r} is reserved by the platform "
            f"(reserved: {', '.join(sorted(RESERVED_NAMESPACES))}); pick another name"
        )
    return None


class StateApiClient:
    """Thin async client over the API state router for one agent's namespace base.

    ``base_url`` is ``CURIE_STATE_URL`` (``.../agents/<id>/state``); a
    ``<namespace>/<key>`` path is composed onto it per call. The scoped ``state``
    token (ADR-0033) rides the ``X-API-Key`` header and is never logged. Each
    method maps one-to-one to a state-router verb; a non-success status raises
    ``StateError`` with a truncated body so the tool layer can turn it into an
    ``is_error`` result.
    """

    def __init__(self, base_url: str, token: str | None) -> None:
        # Normalize to no trailing slash so key URLs compose cleanly.
        self._base = base_url.rstrip("/")
        self._token = token

    def _key_url(self, namespace: str, key: str) -> str:
        # Percent-encode both path segments (#851). An unescaped '#' or '?' in a
        # key (or, since #840, an arbitrary namespace) is URL syntax to
        # aiohttp/yarl -- the fragment/query -- which silently truncates the path
        # so two distinct keys ("config" and "config#draft") collide on one slot
        # with no error. `safe=""` also encodes '/' so a key never spills into an
        # extra path segment. Mirrors binding.py's own `quote(..., safe="")`.
        return f"{self._base}/{quote(namespace, safe='')}/{quote(key, safe='')}"

    def _namespace_url(self, namespace: str) -> str:
        return f"{self._base}/{quote(namespace, safe='')}"

    def _headers(self, *, json_body: bool = False) -> dict[str, str]:
        headers: dict[str, str] = {}
        if self._token:
            headers["X-API-Key"] = self._token
        if json_body:
            headers["Content-Type"] = "application/json"
        return headers

    @staticmethod
    def _timeout() -> aiohttp.ClientTimeout:
        return aiohttp.ClientTimeout(total=_TIMEOUT_SECONDS)

    async def get(self, namespace: str, key: str) -> dict[str, Any] | None:
        """The stored entry ``{value, version}``, or None when it does not exist."""
        async with aiohttp.ClientSession(timeout=self._timeout()) as session:
            async with session.get(
                self._key_url(namespace, key), headers=self._headers()
            ) as resp:
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    raise StateError(await self._fail(resp, "get"))
                payload = await resp.json()
        return {"value": payload.get("value"), "version": payload.get("version")}

    async def set(
        self, namespace: str, key: str, value: Any, expected_version: int | None
    ) -> dict[str, Any]:
        """Put ``value`` at ``key``; ``expected_version`` opts into compare-and-set."""
        body: dict[str, Any] = {"value": value}
        if expected_version is not None:
            body["expected_version"] = expected_version
        async with aiohttp.ClientSession(timeout=self._timeout()) as session:
            async with session.put(
                self._key_url(namespace, key),
                data=json.dumps(body),
                headers=self._headers(json_body=True),
            ) as resp:
                if resp.status not in (200, 201):
                    raise StateError(await self._fail(resp, "set"))
                payload = await resp.json()
        return {"value": payload.get("value"), "version": payload.get("version")}

    async def append(self, namespace: str, key: str, item: Any) -> dict[str, Any]:
        """Append ``item`` to a JSON-array entry (creating it as ``[item]``)."""
        async with aiohttp.ClientSession(timeout=self._timeout()) as session:
            async with session.post(
                f"{self._key_url(namespace, key)}/append",
                data=json.dumps({"item": item}),
                headers=self._headers(json_body=True),
            ) as resp:
                if resp.status not in (200, 201):
                    raise StateError(await self._fail(resp, "append"))
                payload = await resp.json()
        return {"value": payload.get("value"), "version": payload.get("version")}

    async def list(self, namespace: str) -> list[dict[str, Any]]:
        """Every entry in ``namespace`` as ``{key, value, version}``, key-sorted."""
        async with aiohttp.ClientSession(timeout=self._timeout()) as session:
            async with session.get(
                self._namespace_url(namespace), headers=self._headers()
            ) as resp:
                if resp.status != 200:
                    raise StateError(await self._fail(resp, "list"))
                payload = await resp.json()
        entries = payload if isinstance(payload, list) else []
        return [
            {"key": e.get("key"), "value": e.get("value"), "version": e.get("version")}
            for e in entries
            if isinstance(e, Mapping)
        ]

    async def delete(self, namespace: str, key: str) -> None:
        """Delete ``key`` (idempotent: a missing key is still a success)."""
        async with aiohttp.ClientSession(timeout=self._timeout()) as session:
            async with session.delete(
                self._key_url(namespace, key), headers=self._headers()
            ) as resp:
                if resp.status not in (200, 204):
                    raise StateError(await self._fail(resp, "delete"))

    @staticmethod
    async def _fail(resp: aiohttp.ClientResponse, op: str) -> str:
        """A truncated failure message. Never includes the credential (headers)."""
        body = (await resp.text())[:200]
        return f"state {op} failed: {resp.status} {body}"


# --- Tool schemas ------------------------------------------------------------
#
# Full JSON schemas (not the shorthand type map) so ``expected_version`` is
# optional and ``value``/``item`` accept arbitrary JSON, matching the store's
# arbitrary-JSON value contract (#248).

_NAMESPACE_PROP = {
    "type": "string",
    "description": (
        "The state namespace (a bucket of keys scoped to this agent). "
        "'memory' and 'transcript' are reserved."
    ),
}
_KEY_PROP = {"type": "string", "description": "The key within the namespace."}

_GET_SCHEMA = {
    "type": "object",
    "properties": {"namespace": _NAMESPACE_PROP, "key": _KEY_PROP},
    "required": ["namespace", "key"],
}
_SET_SCHEMA = {
    "type": "object",
    "properties": {
        "namespace": _NAMESPACE_PROP,
        "key": _KEY_PROP,
        "value": {"description": "The JSON value to store (any JSON type)."},
        "expected_version": {
            "type": "integer",
            "description": (
                "Optional compare-and-set guard: the version you last read. "
                "The write is rejected with a conflict if the stored version "
                "differs. Omit for a blind write."
            ),
        },
    },
    "required": ["namespace", "key", "value"],
}
_APPEND_SCHEMA = {
    "type": "object",
    "properties": {
        "namespace": _NAMESPACE_PROP,
        "key": _KEY_PROP,
        "item": {"description": "The JSON item to append to the array at this key."},
    },
    "required": ["namespace", "key", "item"],
}
_LIST_SCHEMA = {
    "type": "object",
    "properties": {"namespace": _NAMESPACE_PROP},
    "required": ["namespace"],
}
_DELETE_SCHEMA = {
    "type": "object",
    "properties": {"namespace": _NAMESPACE_PROP, "key": _KEY_PROP},
    "required": ["namespace", "key"],
}


# --- Tool op handlers --------------------------------------------------------
#
# The five operations live as module-level async functions taking (client,
# args) so the tool bodies in build_state_server are one-line delegations and
# the exact validate -> call -> format path each tool runs is unit-testable
# without reaching into the SDK server instance. Each catches a transport/store
# failure and returns an ``is_error`` result the model reads and recovers from,
# rather than letting the exception crash the turn.


# One operation's shape, so ``_STATE_TOOL_SPECS`` below can carry its handler in
# the same row as its name and neither can be added without the other.
_StateOp = Callable[[StateApiClient, dict[str, Any]], Awaitable[dict[str, Any]]]


async def op_get(client: StateApiClient, args: dict[str, Any]) -> dict[str, Any]:
    namespace, key = args["namespace"], args["key"]
    if refusal := _validate_namespace(namespace):
        return _err(refusal)
    try:
        entry = await client.get(namespace, key)
    except (StateError, aiohttp.ClientError) as exc:
        return _err(f"state get failed: {exc}")
    if entry is None:
        return _ok({"found": False})
    return _ok({"found": True, **entry})


async def op_set(client: StateApiClient, args: dict[str, Any]) -> dict[str, Any]:
    namespace, key = args["namespace"], args["key"]
    if refusal := _validate_namespace(namespace):
        return _err(refusal)
    try:
        entry = await client.set(
            namespace, key, args["value"], args.get("expected_version")
        )
    except (StateError, aiohttp.ClientError) as exc:
        return _err(f"state set failed: {exc}")
    return _ok(entry)


async def op_append(client: StateApiClient, args: dict[str, Any]) -> dict[str, Any]:
    namespace, key = args["namespace"], args["key"]
    if refusal := _validate_namespace(namespace):
        return _err(refusal)
    try:
        entry = await client.append(namespace, key, args["item"])
    except (StateError, aiohttp.ClientError) as exc:
        return _err(f"state append failed: {exc}")
    return _ok(entry)


async def op_list(client: StateApiClient, args: dict[str, Any]) -> dict[str, Any]:
    namespace = args["namespace"]
    if refusal := _validate_namespace(namespace):
        return _err(refusal)
    try:
        entries = await client.list(namespace)
    except (StateError, aiohttp.ClientError) as exc:
        return _err(f"state list failed: {exc}")
    return _ok({"entries": entries})


async def op_delete(client: StateApiClient, args: dict[str, Any]) -> dict[str, Any]:
    namespace, key = args["namespace"], args["key"]
    if refusal := _validate_namespace(namespace):
        return _err(refusal)
    try:
        await client.delete(namespace, key)
    except (StateError, aiohttp.ClientError) as exc:
        return _err(f"state delete failed: {exc}")
    return _ok({"deleted": True})


# Every tool this server publishes, declared ONCE (#2286 adversarial round).
#
# One list, three consumers: ``build_state_server`` registers exactly these with
# the SDK, ``STATE_TOOL_NAMES`` below renders their live ``mcp__curie-state__*``
# names, and ``approval.py`` exempts those live names from a bundle ``toolPolicy``.
# Hand-maintaining the exemption set beside the registration is what the first
# #2286 fix was written to avoid -- publication exempted two literal names and
# every channel-memory tool added afterwards was denied on arrival -- and a
# second copy here would reopen it at the next tool. Adding a sixth tool means
# adding one row, and the exemption follows for free.
_STATE_TOOL_SPECS: tuple[tuple[str, str, dict[str, Any], _StateOp], ...] = (
    ("get", "Read a durable state value by namespace and key.", _GET_SCHEMA, op_get),
    (
        "set",
        "Write a durable state value, optionally with a compare-and-set version.",
        _SET_SCHEMA,
        op_set,
    ),
    (
        "append",
        "Append an item to a durable JSON-array state value.",
        _APPEND_SCHEMA,
        op_append,
    ),
    ("list", "List every key and value in a state namespace.", _LIST_SCHEMA, op_list),
    ("delete", "Delete a durable state value by namespace and key.", _DELETE_SCHEMA, op_delete),
)

# The live SDK names the tools above are published under, which is the ONLY form
# an authorization decision ever sees: the SDK prefixes an in-process server's
# tools as ``mcp__<server>__<tool>``. ``approval.py`` compares a candidate call
# against this set by EXACT equality rather than by the ``mcp__curie-state__``
# prefix, because a prefix match also accepts every tool of an ambient MCP server
# whose key merely BEGINS ``curie-state__`` (``mcp__curie-state__extra__bar``),
# and ``strict_mcp_config`` is off so the CLI loads ambient project/user servers
# beside the ones the runner mounts.
STATE_TOOL_NAMES: frozenset[str] = frozenset(
    f"mcp__{STATE_SERVER_NAME}__{tool_name}" for tool_name, _, _, _ in _STATE_TOOL_SPECS
)


def _bind_state_tool(
    tool_name: str, description: str, schema: dict[str, Any], op: _StateOp, client: StateApiClient
) -> Any:
    """Register one spec row as an SDK tool bound to ``client``.

    A function rather than a loop body so each closure captures its OWN ``op``:
    a late-binding closure over the loop variable would register five tools that
    all call whichever handler the loop finished on.
    """

    @tool(tool_name, description, schema)
    async def state_tool(args: dict[str, Any]) -> dict[str, Any]:
        return await op(client, args)

    return state_tool


def build_state_server(client: StateApiClient) -> McpSdkServerConfig:
    """The in-process ``curie-state`` MCP server bound to ``client``.

    Auto-mounted whenever the state boot environment is present so a bundle
    skill reads and writes durable, suspend/resume-surviving state without
    shipping its own MCP server. Each tool delegates to a module-level op handler
    that validates the namespace against the reserved set and turns a
    transport/store failure into an ``is_error`` result the model can recover
    from, rather than crashing the turn.

    The tool list comes from ``_STATE_TOOL_SPECS`` so the names this server
    actually publishes and the names ``approval.py`` exempts cannot drift.
    """

    return create_sdk_mcp_server(
        name=STATE_SERVER_NAME,
        version="1.0.0",
        tools=[
            _bind_state_tool(tool_name, description, schema, op, client)
            for tool_name, description, schema, op in _STATE_TOOL_SPECS
        ],
    )


def resolve_state_client(env: Mapping[str, str]) -> StateApiClient | None:
    """Build a ``StateApiClient`` from the boot env, or None when unconfigured.

    An absent ``CURIE_STATE_URL`` (fake/local with no store, or an older
    worker) yields None, so the runner mounts no state server and an agent
    without a store sees no phantom tools. A present URL yields the client; the
    token is optional (the no-key local path leaves it unset).
    """

    base_url = env.get(STATE_URL_ENV)
    if not base_url:
        return None
    return StateApiClient(base_url, env.get(STATE_TOKEN_ENV))
