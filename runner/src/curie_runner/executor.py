"""Executor mode: the runner-private ``/v1/execute`` phases (ACTION-EXECUTOR-6).

@spec ACTION-EXECUTOR-4 @spec ACTION-EXECUTOR-6 @spec ACTION-EXECUTOR-7
@spec ACTION-EXECUTOR-13 @spec ACTION-EXECUTOR-15 @spec ACTION-EXECUTOR-24.
A runner booted with ``CURIE_RUNNER_MODE=execute`` loads no harness, no model
session and no history. It serves one connector action for the worker's
executor loop in three phases, each over its own standalone MCP session:

* ``list``: ``tools/list`` only.
* ``observe``: one call of the read verb ``observe_version`` with
  ``{"target": <target>}``; returns its ``version`` string, or null.
* ``call``: exactly one ``tools/call`` of ``tool`` with the canonical argument
  text and the grant header, after the preflight. A call refused by preflight
  still spends the sandbox's one call; a connector without a URL, which the
  grant cannot ride to, is refused ``connector_not_hosted``. ``list`` stops past
  ``LIST_PAGE_LIMIT`` pages and a result past ``CALL_RESULT_MAX_BYTES`` is an
  error, neither retried.

Within one sandbox the only accepted orders are ``list``, ``list`` then
``observe`` then a restore ``call``, and ``list`` then a forward ``call``.
Every refusal is decided before any write is dialed.
The worker's client and every shape here are frozen in
``tests/vectors/runner-execute.json``. Nothing here emits an ACI frame or logs
an argument, envelope, version or result.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import anyio
from mcp import ClientSession
from mcp.types import CallToolResult, PaginatedRequestParams

from .connectors import GRANT_HEADER
from .mcp_tool_capability import server_streams

logger = logging.getLogger(__name__)

# @spec ACTION-EXECUTOR-5: runner-private, outside ``BootEnv``.
RUNNER_MODE_ENV = "CURIE_RUNNER_MODE"
EXECUTOR_MODE = "execute"

REQUEST_KEYS = frozenset(
    ("execution_id", "phase", "connector", "tool", "arguments", "grant", "target")
)
RESTORE_TOOL = "restore"
OBSERVE_TOOL = "observe_version"
_SCHEMA_TOOLS = (RESTORE_TOOL, OBSERVE_TOOL)
_DIAL_TIMEOUT_SECONDS = 15.0
# The write call's own bound. Past it the outcome is unknown, never refused.
_CALL_TIMEOUT_SECONDS = 60.0
# Bounds (runner-execute.json ``bounds``): past either, no retry.
LIST_PAGE_LIMIT = 100
CALL_RESULT_MAX_BYTES = 1048576

# Route refusals and their HTTP status (runner-execute.json ``refusals``).
_STATUS = {
    "phase_out_of_order": 409,
    "invalid_request": 400,
    "tool_not_advertised": 409,
    "restore_not_advertised": 409,
    "restore_schema_mismatch": 409,
    "arguments_mismatch": 409,
    "connector_not_hosted": 409,
    "connector_unreachable": 502,
    "call_outcome_unknown": 502,
}


class _ListTooLong(Exception):
    """The connector's tool list spans more than ``LIST_PAGE_LIMIT`` pages."""


class ExecuteRefusal(Exception):
    """A route refusal: ``{"refused": code}`` with the code's status."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code
        self.status = _STATUS[code]

    def body(self) -> dict[str, str]:
        return {"refused": self.code}


@dataclass(frozen=True)
class _Request:
    execution_id: str
    phase: str
    connector: str
    tool: str | None
    arguments: str | None
    grant: str | None
    target: dict[str, Any] | None


@dataclass(frozen=True)
class _Tool:
    name: str
    annotations: dict[str, Any]
    input_schema: dict[str, Any]

    @property
    def read_only(self) -> bool:
        return self.annotations.get("readOnlyHint") is True

    def requires(self, *names: str) -> bool:
        required = self.input_schema.get("required")
        return isinstance(required, list) and all(name in required for name in names)

    def listed(self) -> dict[str, Any]:
        entry: dict[str, Any] = {"name": self.name, "annotations": self.annotations}
        if self.name in _SCHEMA_TOOLS:
            entry["input_schema"] = self.input_schema
        return entry


def _nonempty(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def parse_request(body: object) -> _Request:
    """Exactly the frozen keys; a key the phase does not use is null."""

    if not isinstance(body, dict) or set(body) != REQUEST_KEYS:
        raise ExecuteRefusal("invalid_request")
    phase = body["phase"]
    if not (_nonempty(body["execution_id"]) and _nonempty(body["connector"])):
        raise ExecuteRefusal("invalid_request")
    tool, arguments, grant, target = (
        body["tool"],
        body["arguments"],
        body["grant"],
        body["target"],
    )
    if phase == "list":
        valid = tool is None and arguments is None and grant is None and target is None
    elif phase == "observe":
        valid = tool is None and arguments is None and grant is None and isinstance(target, dict)
    elif phase == "call":
        valid = (
            _nonempty(tool) and isinstance(arguments, str) and _nonempty(grant) and target is None
        )
    else:
        valid = False
    if not valid:
        raise ExecuteRefusal("invalid_request")
    return _Request(
        execution_id=body["execution_id"],
        phase=phase,
        connector=body["connector"],
        tool=tool,
        arguments=arguments,
        grant=grant,
        target=target,
    )


def canonical_arguments(text: str) -> dict[str, Any] | None:
    """The object ``text`` names when it is already its own canonical form.

    @spec ACTION-EXECUTOR-7: sorted keys, separators ``,`` and ``:``,
    ``ensure_ascii=False``, the caller proxy's form. Anything else is None.
    """

    def reject(constant: str) -> object:
        raise ValueError(constant)

    try:
        parsed = json.loads(text, parse_constant=reject)
    except ValueError:
        return None
    if not isinstance(parsed, dict):
        return None
    try:
        canonical = json.dumps(
            parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    except ValueError:
        return None
    return parsed if canonical == text else None


def restore_refusal(tools: Mapping[str, _Tool]) -> str | None:
    """@spec ACTION-EXECUTOR-13: the paired verbs' capability rule, or its refusal."""

    restore = tools.get(RESTORE_TOOL)
    observe = tools.get(OBSERVE_TOOL)
    if restore is None or observe is None:
        return "restore_not_advertised"
    if restore.read_only or not restore.requires("target", "prior_state"):
        return "restore_schema_mismatch"
    if not observe.read_only or not observe.requires("target"):
        return "restore_schema_mismatch"
    return None


class Executor:
    """One sandbox's phase sequence against the connectors an ordinary boot derives."""

    def __init__(
        self,
        connectors: Mapping[str, Mapping[str, Any]],
        inherited_env: Mapping[str, str] | None = None,
    ) -> None:
        self._connectors = connectors
        self._env = inherited_env
        self._lock = asyncio.Lock()
        self._done: list[str] = []
        self._tools: dict[str, _Tool] = {}
        self._execution_id: str | None = None
        self._connector: str | None = None

    @property
    def turn_active(self) -> bool:
        return self._lock.locked()

    async def handle(self, body: object) -> dict[str, Any]:
        request = parse_request(body)
        if self._lock.locked():
            # One phase at a time; a concurrent one is out of order by definition.
            raise ExecuteRefusal("phase_out_of_order")
        async with self._lock:
            self._check_order(request)
            if request.phase == "list":
                return await self._list(request)
            if request.phase == "observe":
                return await self._observe(request)
            return await self._call(request)

    def _check_order(self, request: _Request) -> None:
        done = self._done
        if request.phase == "list":
            accepted = not done
        elif request.phase == "observe":
            accepted = done == ["list"]
        elif request.tool == RESTORE_TOOL:
            # A restore always follows the version observation.
            accepted = done == ["list", "observe"]
        else:
            # A forward tool is ``list`` then ``call``; it never follows ``observe``.
            accepted = done == ["list"]
        if not accepted:
            raise ExecuteRefusal("phase_out_of_order")
        if request.phase != "list" and (
            request.execution_id != self._execution_id or request.connector != self._connector
        ):
            raise ExecuteRefusal("invalid_request")

    def _config(self, connector: str, grant: str | None = None) -> Mapping[str, Any]:
        config = self._connectors.get(connector)
        if not isinstance(config, Mapping):
            raise ExecuteRefusal("connector_unreachable")
        if grant is not None and config.get("url"):
            headers = config.get("headers")
            merged = dict(headers) if isinstance(headers, Mapping) else {}
            merged[GRANT_HEADER] = grant
            return {**config, "headers": merged}
        return config

    def _inherited(self) -> Mapping[str, str]:
        return self._env if self._env is not None else os.environ

    async def _list(self, request: _Request) -> dict[str, Any]:
        config = self._config(request.connector)
        tools: dict[str, _Tool] = {}
        try:
            with anyio.fail_after(_DIAL_TIMEOUT_SECONDS):
                async with server_streams(
                    config, plugin_dir=None, inherited_env=self._inherited()
                ) as (read_stream, write_stream):
                    async with ClientSession(
                        read_stream, write_stream, read_timeout_seconds=_DIAL_TIMEOUT_SECONDS
                    ) as session:
                        await session.initialize()
                        cursor: str | None = None
                        pages = 0
                        while True:
                            if pages >= LIST_PAGE_LIMIT:
                                raise _ListTooLong
                            pages += 1
                            result = await session.list_tools(
                                params=PaginatedRequestParams(cursor=cursor)
                            )
                            for tool in result.tools:
                                annotations = (
                                    tool.annotations.model_dump(by_alias=True, exclude_none=True)
                                    if tool.annotations is not None
                                    else {}
                                )
                                tools[tool.name] = _Tool(
                                    tool.name, annotations, dict(tool.input_schema)
                                )
                            cursor = result.next_cursor
                            if not cursor:
                                break
        except Exception as exc:  # noqa: BLE001 - any dial failure is unreachable
            logger.warning(
                "execute list failed connector=%s error_class=%s",
                request.connector,
                type(exc).__name__,
            )
            raise ExecuteRefusal("connector_unreachable") from exc
        self._tools = tools
        self._execution_id = request.execution_id
        self._connector = request.connector
        self._done.append("list")
        return {"phase": "list", "tools": [tool.listed() for tool in tools.values()]}

    async def _observe(self, request: _Request) -> dict[str, Any]:
        self._done.append("observe")
        if OBSERVE_TOOL not in self._tools:
            # Nothing to read; the API refuses an absent version as a conflict.
            return {"phase": "observe", "version": None}
        config = self._config(request.connector)
        try:
            with anyio.fail_after(_DIAL_TIMEOUT_SECONDS):
                result = await self._dial_call(config, OBSERVE_TOOL, {"target": request.target})
        except Exception as exc:  # noqa: BLE001 - a read that fails is unreachable
            logger.warning(
                "execute observe failed connector=%s error_class=%s",
                request.connector,
                type(exc).__name__,
            )
            raise ExecuteRefusal("connector_unreachable") from exc
        version: str | None = None
        if isinstance(result, CallToolResult) and not result.is_error:
            structured = result.structured_content
            if isinstance(structured, dict) and isinstance(structured.get("version"), str):
                version = structured["version"]
        return {"phase": "observe", "version": version}

    async def _call(self, request: _Request) -> dict[str, Any]:
        assert request.tool is not None and request.arguments is not None
        # The sandbox's one call is spent by any call that passed ordering, a
        # preflight refusal included, so no later call can dial.
        self._done.append("call")
        if request.tool == RESTORE_TOOL:
            refusal = restore_refusal(self._tools)
            if refusal is not None:
                raise ExecuteRefusal(refusal)
        elif request.tool not in self._tools:
            raise ExecuteRefusal("tool_not_advertised")
        arguments = canonical_arguments(request.arguments)
        if arguments is None:
            raise ExecuteRefusal("arguments_mismatch")
        config = self._config(request.connector)
        if not isinstance(config.get("url"), str) or not config.get("url"):
            # The grant header rides only to a URL connector; never dial a write
            # the caller proxy cannot verify.
            raise ExecuteRefusal("connector_not_hosted")
        config = self._config(request.connector, request.grant)
        try:
            with anyio.fail_after(_CALL_TIMEOUT_SECONDS):
                result = await self._dial_call(config, request.tool, arguments)
        except Exception as exc:  # noqa: BLE001 - the write may have landed
            logger.warning(
                "execute call outcome unknown connector=%s error_class=%s",
                request.connector,
                type(exc).__name__,
            )
            raise ExecuteRefusal("call_outcome_unknown") from exc
        if not isinstance(result, CallToolResult):
            raise ExecuteRefusal("call_outcome_unknown")
        size = len(result.model_dump_json(by_alias=True, exclude_none=True).encode("utf-8"))
        if size > CALL_RESULT_MAX_BYTES:
            # The write landed once; an oversized result is an error, never retried.
            logger.warning(
                "execute call result over bound connector=%s bytes=%d",
                request.connector,
                size,
            )
            return {"phase": "call", "is_error": True, "structured": None}
        structured = result.structured_content
        return {
            "phase": "call",
            "is_error": bool(result.is_error),
            "structured": structured if isinstance(structured, dict) else None,
        }

    async def _dial_call(
        self, config: Mapping[str, Any], tool: str, arguments: dict[str, Any]
    ) -> object:
        async with server_streams(config, plugin_dir=None, inherited_env=self._inherited()) as (
            read_stream,
            write_stream,
        ):
            async with ClientSession(read_stream, write_stream) as session:
                await session.initialize()
                return await session.call_tool(tool, arguments)


__all__ = [
    "EXECUTOR_MODE",
    "REQUEST_KEYS",
    "RUNNER_MODE_ENV",
    "ExecuteRefusal",
    "Executor",
    "canonical_arguments",
    "parse_request",
    "restore_refusal",
]
