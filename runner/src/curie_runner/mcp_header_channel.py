"""Keep runner-materialized hosted connector headers inside the runner (#5378).

The runner expands a hosted connector's ``Authorization`` bearer and its
``X-Curie-Caller`` token in memory
(``connectors.materialize_hosted_bearer_headers``,
``connectors.materialize_connector_caller_headers``) and drops their env names,
so a shell cannot read them from an environment. A resumed turn's
``X-Curie-Connector-Grant`` is held the same way. Handing those entries to the
Claude CLI as ``http`` servers would still put every value in the
``--mcp-config`` file, which belongs to the same uid as the sandbox's Bash.

Each such entry is mounted on the CLI instead as an in-process SDK MCP server
under the same key: a :class:`HostedHeaderRelay` bound at construction to that
one entry. It relays each SDK session's JSON-RPC to the connector over the
client the capability probe dials (``mcp_tool_capability.server_streams``: the
hosted HTTP client, the connector CA file, ``trust_env=False``). The CLI's
configuration names the server and nothing else, so tool names
(``mcp__<connector>__<tool>``), schemas, results, approvals and side-effect
classification are unchanged, while the header values stay in this process,
which is not dumpable. The relay is not a proxy: it dials only its own entry.

Reconnect: every ``initialize`` from the CLI dials a fresh upstream session. An
upstream that dropped is dialed again on the next request with the CLI's own
handshake replayed, and a request the connector refused because it no longer
knows the session is sent once more on the new one, as the CLI does for an HTTP
server whose session expired. Only that refused request is sent again: the
connector answered it, so it did not run. Any other request still unanswered
when the expired session is closed may have run before its answer was lost, so
it fails back to the CLI with an error and is never sent again.

Bundle servers, and remote connector headers the CLI expands from ``${VAR}``,
are not runner-materialized and keep their existing delivery.
"""

from __future__ import annotations

import contextlib
import copy
import logging
import math
import re
from collections.abc import Collection, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from typing import Any

import anyio
import anyio.abc
from mcp.shared.inbound import MCP_PROTOCOL_VERSION_HEADER
from mcp.shared.message import ClientMessageMetadata, SessionMessage
from mcp.types import (
    CONNECTION_CLOSED,
    INVALID_REQUEST,
    ErrorData,
    JSONRPCError,
    JSONRPCNotification,
    JSONRPCRequest,
    JSONRPCResponse,
    jsonrpc_message_adapter,
)

from .connectors import _GRANT_ENV, GRANT_HEADER
from .mcp_stdio import UnsafeStdioConfiguration, sanitize_mcp_servers
from .mcp_tool_capability import _probe_error_leaves, server_streams

logger = logging.getLogger(__name__)

_GRANT_PLACEHOLDER = f"${{{_GRANT_ENV}}}"
_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_INITIALIZE = "initialize"
_INITIALIZED = "notifications/initialized"
_CANCELLED = "notifications/cancelled"
_REPLAY_ID_PREFIX = "curie-relay-handshake-"
# mcp's streamable-HTTP client reports a 404 on a request that carried a session
# id as this error: the connector no longer knows the session (it restarted).
_SESSION_EXPIRED_MESSAGE = "Session terminated"
_HANDSHAKE_TIMEOUT_S = 30.0
_CLOSE_TIMEOUT_S = 5.0


def derived_header_placeholders(servers: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    """Each derived entry's header values before the runner materializes any.

    Taken before ``materialize_*``; ``mount_hosted_header_channel`` compares the
    materialized entries against it to find the values only this process holds.
    """

    snapshot: dict[str, dict[str, str]] = {}
    for name, entry in servers.items():
        headers = entry.get("headers") if isinstance(entry, Mapping) else None
        if isinstance(headers, Mapping):
            snapshot[str(name)] = {
                key: value
                for key, value in headers.items()
                if isinstance(key, str) and isinstance(value, str)
            }
    return snapshot


def _expand(value: str, env: Mapping[str, str]) -> str:
    """``${NAME}`` and ``${NAME:-default}`` as the CLI expands MCP header values."""

    def replace(match: re.Match[str]) -> str:
        found = env.get(match.group(1))
        if found:
            return found
        return match.group(2) if match.group(2) is not None else match.group(0)

    return _REFERENCE.sub(replace, value)


def mount_hosted_header_channel(
    servers: Mapping[str, Any],
    *,
    derived_headers: Mapping[str, Mapping[str, str]],
    env: Mapping[str, str],
    release: Sequence[MutableMapping[str, str]] = (),
) -> dict[str, Any]:
    """The CLI-facing map, with each runner-materialized entry mounted as a relay.

    ``servers`` is the materialized ``derive_mcp_servers`` output and is not
    changed: the approval gate, redaction, the capability probe and the
    re-probe keep reading the real entries. ``derived_headers`` is
    ``derived_header_placeholders`` taken before materialization. A header that
    held a ``${NAME}`` placeholder there and another value here was
    materialized by this process, and so is the tool grant, which only the
    relay expands, from ``env``. ``env`` is the environment the CLI would have
    expanded the entry's other ``${NAME}`` references from. When a relay takes
    the grant, its env name leaves every mapping in ``release``: no other client
    needs it.

    An entry that would carry a model credential is left as it was, so the
    session's own refusal at connect still stops it before the CLI starts.
    """

    mounted: dict[str, Any] = dict(servers)
    grant = str(env.get(_GRANT_ENV, "")).strip()
    grant_taken = False
    for name, entry in servers.items():
        if not isinstance(entry, Mapping) or not isinstance(entry.get("url"), str):
            continue
        headers = entry.get("headers")
        if not isinstance(headers, Mapping):
            continue
        before = derived_headers.get(name, {})
        held_headers: dict[str, Any] = dict(headers)
        protected: set[str] = set()
        for key, value in headers.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            prior = before.get(key)
            if prior is not None and prior != value and "${" in prior:
                protected.add(key)
            elif key == GRANT_HEADER and value == _GRANT_PLACEHOLDER and grant:
                held_headers[key] = grant
                protected.add(key)
        if not protected:
            continue
        held = copy.deepcopy(dict(entry))
        held["headers"] = held_headers
        try:
            sanitize_mcp_servers({name: held}, env)
        except UnsafeStdioConfiguration:
            logger.error("hosted connector %s carries a model credential and is not relayed", name)
            continue
        held["url"] = _expand(held["url"], env)
        held["headers"] = {
            key: _expand(value, env) if isinstance(value, str) and key not in protected else value
            for key, value in held_headers.items()
        }
        relay = HostedHeaderRelay(name, held, protected_headers=protected)
        mounted[name] = {"type": "sdk", "name": name, "instance": relay}
        grant_taken = grant_taken or GRANT_HEADER in protected
    if grant_taken:
        for target in release:
            target.pop(_GRANT_ENV, None)
    return mounted


def connector_grant_env(*envs: Mapping[str, str] | None) -> dict[str, str]:
    """The resumed turn's tool grant by its env name, taken before a mount drops it.

    The turn-start re-probe dials the real entries, whose grant header still
    names the env value; this keeps that value for the runner's own client.
    """

    for env in reversed(envs):
        value = str((env or {}).get(_GRANT_ENV, "")).strip()
        if value:
            return {_GRANT_ENV: value}
    return {}


def hosted_header_values(servers: object) -> frozenset[str]:
    """Every header value held by a relay mounted in ``servers``.

    The CLI transport refuses a command line, configuration or environment
    that carries one of these (``mcp_argv``).
    """

    if not isinstance(servers, Mapping):
        return frozenset()
    values: set[str] = set()
    for entry in servers.values():
        if isinstance(entry, Mapping) and entry.get("type") == "sdk":
            relay = entry.get("instance")
            if isinstance(relay, HostedHeaderRelay):
                values |= relay.header_values()
    return frozenset(values)


def _leaf_class(exc: BaseException) -> str:
    leaves = _probe_error_leaves(exc)
    return type(leaves[0] if leaves else exc).__name__


def _protocol_version(response: JSONRPCResponse) -> str | None:
    result = response.result
    version = result.get("protocolVersion") if isinstance(result, Mapping) else None
    return version if isinstance(version, str) and version else None


def _expired(error: JSONRPCError) -> bool:
    return error.error.code == INVALID_REQUEST and error.error.message == _SESSION_EXPIRED_MESSAGE


class HostedHeaderRelay:
    """The in-process MCP server the SDK mounts for one hosted connector entry.

    The SDK's bridge calls ``run`` with the streams of one session; each run
    keeps its own upstream connections, so a later session dials afresh. The
    entry, headers included, lives only in this object.
    """

    def __init__(
        self,
        name: str,
        entry: Mapping[str, Any],
        *,
        protected_headers: Collection[str],
    ) -> None:
        self.name = name
        self._entry = copy.deepcopy(dict(entry))
        wanted = {header.lower() for header in protected_headers}
        values: set[str] = set()
        headers = self._entry.get("headers")
        if isinstance(headers, Mapping):
            for key, value in headers.items():
                if not (isinstance(key, str) and key.lower() in wanted and isinstance(value, str)):
                    continue
                value = value.strip()
                _scheme, separator, credential = value.partition(" ")
                credential = credential.strip()
                if separator and credential:
                    values.update((value, credential))
                elif value and not separator:
                    values.add(value)
        self._values = frozenset(values)

    def __repr__(self) -> str:
        # The entry holds header values; a repr in a log line must not.
        return f"HostedHeaderRelay(name={self.name!r})"

    def header_values(self) -> frozenset[str]:
        """The protected header values, and their credential part after a scheme."""

        return self._values

    def create_initialization_options(self) -> None:
        # The connector answers ``initialize`` itself; nothing is negotiated here.
        return None

    async def run(
        self,
        read_stream: Any,
        write_stream: Any,
        initialization_options: object = None,
        *,
        raise_exceptions: bool = False,
        **_: object,
    ) -> None:
        del initialization_options, raise_exceptions
        session = _RelaySession(self.name, self._entry, write_stream)
        try:
            await session.serve(read_stream)
        except Exception as exc:  # noqa: BLE001 - class only: the text can carry a header
            logger.warning(
                "hosted connector relay stopped server=%s error_class=%s",
                self.name,
                _leaf_class(exc),
            )


@dataclass
class _Pending:
    """A request sent upstream and not yet answered."""

    message: SessionMessage | None
    retried: bool = False
    initialize: bool = False
    # Set only for a replayed handshake, which the relay answers itself.
    answered: anyio.Event | None = None
    result: JSONRPCResponse | JSONRPCError | None = None


class _Upstream:
    """One connection to the connector, for one handshake."""

    def __init__(self) -> None:
        self.pending: dict[Any, _Pending] = {}
        self.write: Any = None
        self.alive = False
        self.expired = False
        self.opened = anyio.Event()
        self.closed = anyio.Event()
        self.scope = anyio.CancelScope()

    @property
    def usable(self) -> bool:
        return self.alive and not self.expired


class _RelaySession:
    """One SDK session: the CLI's side of the relay and its upstream connections."""

    def __init__(self, name: str, entry: Mapping[str, Any], cli: Any) -> None:
        self._name = name
        self._entry = entry
        self._cli = cli
        self._lock = anyio.Lock()
        self._upstream: _Upstream | None = None
        self._handshake: JSONRPCRequest | None = None
        self._initialized = False
        self._version: str | None = None
        self._replays = 0
        self._closing = False
        self._tasks: anyio.abc.TaskGroup | None = None
        self._resend_in, self._resend_out = anyio.create_memory_object_stream[SessionMessage](
            math.inf
        )

    async def serve(self, read_stream: Any) -> None:
        async with anyio.create_task_group() as tasks:
            self._tasks = tasks
            tasks.start_soon(self._resend)
            try:
                async with read_stream:
                    async for item in read_stream:
                        if isinstance(item, SessionMessage):
                            await self._from_cli(item)
            finally:
                self._closing = True
                tasks.cancel_scope.cancel()

    # --- from the CLI ------------------------------------------------------------

    async def _from_cli(self, item: SessionMessage) -> None:
        message = item.message
        if isinstance(message, JSONRPCRequest):
            if message.method == _INITIALIZE:
                await self._handshake_from_cli(item, message)
            else:
                await self._forward(item, retried=False)
            return
        if isinstance(message, JSONRPCNotification):
            if message.method == _CANCELLED:
                await self._cancel(item, message)
                return
            if message.method == _INITIALIZED:
                self._initialized = True
        # A notification, or the CLI's answer to a request the connector sent.
        upstream = self._upstream
        if upstream is not None and upstream.usable:
            await self._write(upstream, self._stamped(item))

    async def _handshake_from_cli(self, item: SessionMessage, message: JSONRPCRequest) -> None:
        # A new handshake is a reconnect: it always gets a fresh upstream session.
        async with self._lock:
            self._handshake = message
            self._initialized = False
            self._version = None
            await self._close(self._upstream)
            upstream = await self._open()
            if upstream is None:
                await self._refuse(message.id)
                return
            await self._request(upstream, item, retried=True)

    async def _forward(self, item: SessionMessage, *, retried: bool) -> None:
        message = item.message
        if not isinstance(message, JSONRPCRequest):
            return
        async with self._lock:
            upstream = await self._current()
            if upstream is None:
                await self._refuse(message.id)
                return
            await self._request(upstream, item, retried=retried)

    async def _cancel(self, item: SessionMessage, message: JSONRPCNotification) -> None:
        raw = message.model_dump(by_alias=True, mode="json", exclude_none=True)
        params = raw.get("params")
        request_id = params.get("requestId") if isinstance(params, Mapping) else None
        upstream = self._upstream
        pending = (
            upstream.pending.pop(request_id, None)
            if upstream is not None and isinstance(request_id, (str, int))
            else None
        )
        if upstream is not None and upstream.usable:
            await self._write(upstream, self._stamped(item))
        original = pending.message if pending is not None else None
        settle = getattr(getattr(original, "metadata", None), "on_request_unanswered", None)
        if settle is not None:
            # The connector may never answer a cancelled request; settle the
            # CLI's side so it does not wait for one.
            await settle()

    async def _resend(self) -> None:
        async with self._resend_out:
            async for item in self._resend_out:
                await self._forward(item, retried=True)

    def _requeue(self, item: SessionMessage) -> None:
        # Sent once more on a fresh upstream session by ``_resend``.
        with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
            self._resend_in.send_nowait(item)

    # --- upstream connections ----------------------------------------------------

    async def _current(self) -> _Upstream | None:
        """The live upstream, dialing again with the CLI's handshake when it dropped."""

        upstream = self._upstream
        if upstream is not None and upstream.usable:
            return upstream
        handshake = self._handshake
        if handshake is None:
            return None
        await self._close(upstream)
        upstream = await self._open()
        if upstream is None:
            return None
        self._replays += 1
        replay_id = f"{_REPLAY_ID_PREFIX}{self._replays}"
        raw = handshake.model_dump(by_alias=True, mode="json", exclude_none=True)
        raw["id"] = replay_id
        pending = _Pending(message=None, answered=anyio.Event())
        upstream.pending[replay_id] = pending
        replay = SessionMessage(jsonrpc_message_adapter.validate_python(raw, by_name=False))
        if await self._write(upstream, replay):
            with anyio.move_on_after(_HANDSHAKE_TIMEOUT_S):
                assert pending.answered is not None
                await pending.answered.wait()
        upstream.pending.pop(replay_id, None)
        if not isinstance(pending.result, JSONRPCResponse):
            logger.warning("hosted connector relay could not redial server=%s", self._name)
            await self._close(upstream)
            return None
        self._version = _protocol_version(pending.result) or self._version
        if self._initialized:
            initialized = jsonrpc_message_adapter.validate_python(
                {"jsonrpc": "2.0", "method": _INITIALIZED}, by_name=False
            )
            if not await self._write(upstream, self._stamped(SessionMessage(initialized))):
                return None
        logger.info("hosted connector relay redialed server=%s", self._name)
        return upstream

    async def _open(self) -> _Upstream | None:
        assert self._tasks is not None
        upstream = _Upstream()
        self._upstream = upstream
        self._tasks.start_soon(self._serve_upstream, upstream)
        await upstream.opened.wait()
        return upstream if upstream.alive else None

    async def _close(self, upstream: _Upstream | None) -> None:
        if upstream is None:
            return
        upstream.scope.cancel()
        with anyio.move_on_after(_CLOSE_TIMEOUT_S):
            await upstream.closed.wait()

    async def _serve_upstream(self, upstream: _Upstream) -> None:
        try:
            with upstream.scope:
                async with server_streams(self._entry, plugin_dir=None, inherited_env={}) as (
                    read_stream,
                    write_stream,
                ):
                    upstream.write = write_stream
                    upstream.alive = True
                    upstream.opened.set()
                    async for item in read_stream:
                        if isinstance(item, Exception):
                            logger.warning(
                                "hosted connector relay transport error server=%s error_class=%s",
                                self._name,
                                type(item).__name__,
                            )
                            continue
                        await self._from_upstream(upstream, item)
        except Exception as exc:  # noqa: BLE001 - class only: the text can carry a header
            logger.warning(
                "hosted connector relay upstream closed server=%s error_class=%s",
                self._name,
                _leaf_class(exc),
            )
        finally:
            upstream.alive = False
            upstream.opened.set()
            with anyio.CancelScope(shield=True, deadline=anyio.current_time() + _CLOSE_TIMEOUT_S):
                await self._upstream_closed(upstream)

    async def _upstream_closed(self, upstream: _Upstream) -> None:
        pending, upstream.pending = list(upstream.pending.values()), {}
        try:
            for item in pending:
                if item.answered is not None:
                    item.answered.set()
                elif item.message is None or self._closing:
                    continue
                else:
                    # Sent and never answered, even on a session the connector
                    # refused for another request: it may have run, so it is
                    # never sent again.
                    request = item.message.message
                    if isinstance(request, JSONRPCRequest):
                        await self._refuse(request.id)
        finally:
            upstream.closed.set()

    async def _request(self, upstream: _Upstream, item: SessionMessage, *, retried: bool) -> None:
        message = item.message
        if not isinstance(message, JSONRPCRequest):
            return
        initialize = message.method == _INITIALIZE
        upstream.pending[message.id] = _Pending(
            message=item, retried=retried, initialize=initialize
        )
        outgoing = SessionMessage(message) if initialize else self._stamped(item)
        if await self._write(upstream, outgoing):
            return
        # Unsent. Answer unless the closed upstream already answered it.
        if upstream.pending.pop(message.id, None) is not None:
            await self._refuse(message.id)

    async def _write(self, upstream: _Upstream, item: SessionMessage) -> bool:
        if not upstream.alive or upstream.write is None:
            return False
        try:
            await upstream.write.send(item)
        except (anyio.ClosedResourceError, anyio.BrokenResourceError):
            return False
        return True

    def _stamped(self, item: SessionMessage) -> SessionMessage:
        # The negotiated version rides every later request, as an MCP client sends it.
        version = self._version
        metadata = (
            ClientMessageMetadata(headers={MCP_PROTOCOL_VERSION_HEADER: version})
            if version
            else None
        )
        return SessionMessage(item.message, metadata=metadata)

    # --- to the CLI --------------------------------------------------------------

    async def _from_upstream(self, upstream: _Upstream, item: SessionMessage) -> None:
        message = item.message
        if isinstance(message, (JSONRPCResponse, JSONRPCError)):
            pending = upstream.pending.pop(message.id, None) if message.id is not None else None
            if pending is not None and pending.answered is not None:
                pending.result = message
                pending.answered.set()
                return
            if pending is not None:
                if isinstance(message, JSONRPCError) and not pending.retried and _expired(message):
                    # The connector refused this request without running it:
                    # only this one is sent again, on a fresh session.
                    upstream.expired = True
                    assert pending.message is not None
                    self._requeue(pending.message)
                    return
                if pending.initialize and isinstance(message, JSONRPCResponse):
                    self._version = _protocol_version(message) or self._version
        await self._deliver(SessionMessage(message))

    async def _refuse(self, request_id: Any) -> None:
        error = JSONRPCError(
            jsonrpc="2.0",
            id=request_id,
            error=ErrorData(
                code=CONNECTION_CLOSED,
                message=f"hosted connector {self._name} is unavailable",
            ),
        )
        await self._deliver(SessionMessage(error))

    async def _deliver(self, item: SessionMessage) -> None:
        with contextlib.suppress(anyio.ClosedResourceError, anyio.BrokenResourceError):
            await self._cli.send(item)
