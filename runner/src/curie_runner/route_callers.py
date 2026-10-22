"""Host a bundle's declared route callers and hand them their route tokens.

ADR 0240 decision 4. A stdio server that a hosted connector's ``routes:`` names
in ``callers`` is not given to the CLI as a stdio entry, because the CLI would
spawn it with an env and a configuration file the runner does not control. The
runner starts the process itself and mounts it on the CLI as an in-process SDK
MCP server under the name the plugin loader would have used, so tool names,
``toolPolicy`` and approval routes do not move. That server is a transparent
JSON-RPC relay over the child's stdio.

The route tokens reach the child on one inherited pipe, named by
``CURIE_CONNECTOR_ROUTES_FD``, holding one JSON document the child reads once:
``{"<connector>": {"url": "<base URL>", "token": "ccr...."}}``. The worker
delivers the tokens as ``CURIE_CONNECTOR_ROUTE_TOKENS``, and the runner removes
that name from every env before anything else can run. After that the only
copies in the sandbox are this process's memory and the caller's.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio
import anyio.abc
import mcp.types
from mcp.shared.message import SessionMessage
from plugin_format.connector_render import service_dns

from .connectors import declared_connectors
from .mcp_tool_capability import _expand
from .subprocess_env import shell_and_hook_env

logger = logging.getLogger(__name__)

ROUTE_TOKENS_ENV = "CURIE_CONNECTOR_ROUTE_TOKENS"
ROUTES_FD_ENV = "CURIE_CONNECTOR_ROUTES_FD"
# How long a caller gets to exit once its session ends before it is killed.
_STOP_GRACE_S = 5.0
_types: Any = mcp.types


def take_route_tokens(*envs: MutableMapping[str, str]) -> dict[str, str]:
    """Remove ``CURIE_CONNECTOR_ROUTE_TOKENS`` from every env and return its tokens.

    The name leaves every mapping even when its value does not parse, so a
    malformed value is never left for a shell to read.
    """

    raw = next((env.get(ROUTE_TOKENS_ENV) for env in envs if env.get(ROUTE_TOKENS_ENV)), None)
    for env in envs:
        env.pop(ROUTE_TOKENS_ENV, None)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError:
        logger.warning("%s does not parse; no connector route is reachable", ROUTE_TOKENS_ENV)
        return {}
    if not isinstance(parsed, dict):
        logger.warning("%s is not an object; no connector route is reachable", ROUTE_TOKENS_ENV)
        return {}
    return {
        str(name): token
        for name, token in parsed.items()
        if isinstance(token, str) and token.startswith("ccr.")
    }


def route_callers(plugin_dir: str | Path | None, *, agent: str | None) -> dict[str, frozenset[str]]:
    """Each declared caller server's name, and the connectors whose routes list it.

    Read from the same validated, deploy.yaml-restricted declaration the
    runner mounts connectors from, so a connector this agent does not run
    calls nobody.
    """

    declared = declared_connectors(plugin_dir, agent=agent)
    callers: dict[str, set[str]] = {}
    for name, spec in declared.items():
        if not spec.is_hosted:
            continue
        for route in spec.routes:
            for server in route.callers:
                callers.setdefault(server, set()).add(name)
    return {server: frozenset(names) for server, names in callers.items()}


def connector_urls(
    plugin_dir: str | Path | None,
    *,
    release: str | None,
    agent: str | None,
    namespace: str | None,
) -> dict[str, str]:
    """Each hosted route connector's base URL: the Service its MCP entry dials.

    Empty without the connector scope, the same rule ``derive_mcp_servers``
    applies: there is no Service to point at.
    """

    if not (release and agent and namespace):
        return {}
    return {
        name: f"http://{service_dns(release, agent, name, namespace)}:{spec.port}"
        for name, spec in declared_connectors(plugin_dir, agent=agent).items()
        if spec.is_hosted and spec.routes
    }


@dataclass(frozen=True)
class CallerLaunch:
    """What starting one declared caller takes. Holds the route document."""

    command: str
    args: tuple[str, ...]
    env: Mapping[str, str]
    cwd: str | None
    routes_document: bytes

    def __repr__(self) -> str:
        # The document holds route tokens; a repr in a log line must not.
        return f"CallerLaunch(command={self.command!r}, args={self.args!r})"


def caller_launch(
    entry: Mapping[str, Any],
    *,
    routes: Mapping[str, Mapping[str, str]],
    source_env: Mapping[str, str],
    lock_library: str,
    cwd: str | None,
) -> CallerLaunch:
    """The launch for one caller's stdio ``entry`` (a ``bundle_mcp_servers`` value).

    The env is the shell and hook env, so no platform credential, plus the
    entry's own ``env`` block expanded from it, the env the capability probe
    gives the same server. ``LD_PRELOAD`` gains the environ lock library, so
    the process is not dumpable.
    """

    interpolation = shell_and_hook_env(source_env)
    configured = entry.get("env")
    declared: dict[str, str] = {}
    if isinstance(configured, Mapping):
        declared = {
            str(key): _expand(str(value), interpolation) for key, value in configured.items()
        }
    env = shell_and_hook_env(source_env, extra=declared)
    env.pop(ROUTE_TOKENS_ENV, None)
    existing = [
        item for item in env.get("LD_PRELOAD", "").split(":") if item and item != lock_library
    ]
    env["LD_PRELOAD"] = ":".join([lock_library, *existing])
    args = entry.get("args")
    return CallerLaunch(
        command=_expand(str(entry["command"]), interpolation),
        args=tuple(_expand(str(arg), interpolation) for arg in args)
        if isinstance(args, list)
        else (),
        env=env,
        cwd=cwd,
        routes_document=json.dumps(dict(routes), sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        ),
    )


def route_caller_lock_library(source: Mapping[str, str]) -> str | None:
    """The environ lock library a caller must load, or None when unavailable."""

    from .subprocess_env import proc_dumpable_library

    try:
        return proc_dumpable_library(source)
    except (OSError, subprocess.CalledProcessError) as exc:
        logger.error("the environ lock library is unavailable: %s", type(exc).__name__)
        return None


def _parse(line: bytes) -> SessionMessage | None:
    try:
        if hasattr(_types, "jsonrpc_message_adapter"):
            message = _types.jsonrpc_message_adapter.validate_json(line, by_name=False)
        else:
            message = _types.JSONRPCMessage.model_validate_json(line)
    except ValueError:
        logger.warning("a route caller wrote a line that is not JSON-RPC; dropped")
        return None
    return SessionMessage(message)


def _dump(item: SessionMessage) -> bytes:
    text: str = item.message.model_dump_json(by_alias=True, exclude_unset=True)
    return (text + "\n").encode("utf-8")


class CallerRelay:
    """The in-process MCP server the SDK mounts for one declared caller.

    The SDK's bridge calls ``run`` with the streams of one session. Each run
    starts the caller, hands it the route document on a fresh pipe, and relays
    every message both ways until either side ends, then stops the caller.
    """

    def __init__(self, name: str, launch: CallerLaunch) -> None:
        self.name = name
        self._launch = launch

    def create_initialization_options(self) -> None:
        # The child answers ``initialize`` itself; nothing is negotiated here.
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
        process = await self._start()
        try:
            async with anyio.create_task_group() as tasks:

                async def to_child() -> None:
                    assert process.stdin is not None
                    async with read_stream:
                        async for item in read_stream:
                            if isinstance(item, SessionMessage):
                                await process.stdin.send(_dump(item))
                    await process.stdin.aclose()
                    tasks.cancel_scope.cancel()

                async def from_child() -> None:
                    assert process.stdout is not None
                    buffer = b""
                    async for chunk in process.stdout:
                        buffer += chunk
                        *lines, buffer = buffer.split(b"\n")
                        for line in lines:
                            if line.strip():
                                parsed = _parse(line)
                                if parsed is not None:
                                    await write_stream.send(parsed)
                    tasks.cancel_scope.cancel()

                tasks.start_soon(to_child)
                tasks.start_soon(from_child)
        finally:
            with anyio.CancelScope(shield=True):
                await _stop(process)

    async def _start(self) -> anyio.abc.Process:
        launch = self._launch
        executable = shutil.which(launch.command, path=launch.env.get("PATH")) or launch.command
        read_end, write_end = os.pipe()
        try:
            try:
                os.write(write_end, launch.routes_document)
            finally:
                os.close(write_end)
            env = {**launch.env, ROUTES_FD_ENV: str(read_end)}
            return await anyio.open_process(
                [executable, *launch.args],
                stdin=-1,
                stdout=-1,
                stderr=None,
                env=env,
                cwd=launch.cwd,
                pass_fds=(read_end,),
            )
        finally:
            os.close(read_end)


async def _stop(process: anyio.abc.Process) -> None:
    if process.returncode is None:
        process.terminate()
        with anyio.move_on_after(_STOP_GRACE_S):
            await process.wait()
    if process.returncode is None:
        process.kill()
        await process.wait()
    await process.aclose()


def mount_route_callers(
    bundle_servers: MutableMapping[str, Any],
    *,
    callers: Mapping[str, frozenset[str]],
    tokens: Mapping[str, str],
    connector_urls: Mapping[str, str],
    source_env: Mapping[str, str],
    lock_library: str | None,
    cwd: str | None,
) -> frozenset[str]:
    """Replace each declared caller's stdio entry with its relay. Returns their keys.

    A caller is replaced whether or not a token reached this boot, so it is
    never handed to the CLI as a plain stdio server either way; without a
    token its document is empty and its routes are refused ``missing``.
    Without the lock library a caller is not mounted at all.
    """

    mounted: set[str] = set()
    # ``bundle_mcp_servers`` keys a server ``plugin:<bundle>:<server>``.
    by_server = {
        key.split(":", 2)[2]: key
        for key in bundle_servers
        if key.startswith("plugin:") and key.count(":") >= 2
    }
    for server, connectors in sorted(callers.items()):
        key = by_server.get(server)
        entry = bundle_servers.get(key) if key is not None else None
        if (
            key is None
            or not isinstance(entry, Mapping)
            or not isinstance(entry.get("command"), str)
        ):
            continue
        if lock_library is None:
            logger.error(
                "route caller %s is not mounted: the environ lock library is unavailable",
                server,
            )
            bundle_servers.pop(key, None)
            continue
        routes = {
            connector: {"url": connector_urls[connector], "token": tokens[connector]}
            for connector in sorted(connectors)
            if connector in tokens and connector in connector_urls
        }
        launch = caller_launch(
            entry, routes=routes, source_env=source_env, lock_library=lock_library, cwd=cwd
        )
        bundle_servers[key] = {"type": "sdk", "name": key, "instance": CallerRelay(key, launch)}
        mounted.add(key)
    return frozenset(mounted)
