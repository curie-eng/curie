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

import atexit
import json
import logging
import os
import queue
import re
import shutil
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio
import anyio.abc
import anyio.to_thread
import mcp.types
from mcp.shared.message import SessionMessage
from plugin_format.connector_render import service_dns

from .connectors import declared_connectors
from .subprocess_env import shell_and_hook_env

logger = logging.getLogger(__name__)

ROUTE_TOKENS_ENV = "CURIE_CONNECTOR_ROUTE_TOKENS"
ROUTES_FD_ENV = "CURIE_CONNECTOR_ROUTES_FD"
_types: Any = mcp.types
# The ``${NAME}`` and ``${NAME:-default}`` forms the CLI expands in an MCP
# config, so a caller the runner starts reads its config as the CLI would.
_VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _expand(value: str, env: Mapping[str, str]) -> str:
    def one(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        found = env.get(name)
        if found:
            return found
        if default is not None:
            return default
        return found if found is not None else match.group(0)

    return _VARIABLE.sub(one, value)


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
    command = _expand(str(entry["command"]), interpolation)
    return CallerLaunch(
        # Resolved now, at boot, before any turn's Bash could put an earlier
        # match on PATH.
        command=shutil.which(command, path=env.get("PATH")) or command,
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


def _as_dict(item: SessionMessage) -> dict[str, Any]:
    raw = item.message.model_dump_json(by_alias=True, exclude_unset=True)
    parsed: dict[str, Any] = json.loads(raw)
    return parsed


def proc_locked(pid: int) -> bool:
    """Whether ``pid`` is no longer dumpable, so same-uid peers cannot read it.

    The kernel refuses ``/proc/<pid>/environ`` to a same-uid reader without
    ``CAP_SYS_PTRACE`` exactly when the process is not dumpable. Where there
    is no ``/proc`` the lock cannot be observed, which counts as unlocked.
    """

    try:
        with open(f"/proc/{pid}/environ", "rb"):
            return False
    except PermissionError:
        return True
    except OSError:
        return False


_LOCK_WAIT_S = 10.0
_LOCK_POLL_S = 0.01
_QUEUE_POLL_S = 0.5
_CLOSED = object()


class _Session:
    """One SDK session attached to the caller: the messages the caller sends it."""

    def __init__(self) -> None:
        self.inbox: queue.Queue[object] = queue.Queue()

    def deliver(self, message: object) -> None:
        self.inbox.put(message)

    def next(self) -> object:
        try:
            return self.inbox.get(timeout=_QUEUE_POLL_S)
        except queue.Empty:
            return None


class _Caller:
    """The one caller process, shared by every session for the runner's life.

    Started on the first session, which precedes any turn's Bash, and never
    restarted. Request ids are remapped, so sessions cannot collide, and a
    response goes back to the session that asked.
    """

    def __init__(self, launch: CallerLaunch, lock_check: Callable[[int], bool] | None) -> None:
        self._launch = launch
        self._lock_check = lock_check
        self._guard = threading.Lock()
        self._write = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None
        self.failed = False
        self._next_id = 0
        # relay id -> (session, the session's own id, settle-on-cancel hook)
        self._pending: dict[int, tuple[_Session, object, Any]] = {}
        self._by_original: dict[tuple[int, object], int] = {}
        self._current: _Session | None = None
        self.initialize_result: dict[str, Any] | None = None
        self._initialize_ids: set[int] = set()
        self.initialized_sent = False

    def ensure_started(self) -> bool:
        with self._guard:
            if self._process is not None or self.failed:
                return not self.failed
            try:
                self._process = self._spawn()
            except OSError as exc:
                logger.error("route caller failed to start: %s", type(exc).__name__)
                self.failed = True
                return False
            threading.Thread(target=self._read, name="curie-route-caller", daemon=True).start()
            atexit.register(_kill_group, self._process)
            return True

    def _spawn(self) -> subprocess.Popen[bytes]:
        launch = self._launch
        read_end, write_end = os.pipe()
        try:
            process = subprocess.Popen(  # noqa: S603 - the bundle's own declared server
                [launch.command, *launch.args],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                env={**launch.env, ROUTES_FD_ENV: str(read_end)},
                cwd=launch.cwd,
                pass_fds=(read_end,),
                start_new_session=True,
            )
        except BaseException:
            os.close(write_end)
            raise
        finally:
            os.close(read_end)
        try:
            # The routes are written only once the caller is observed locked,
            # so a same-uid reader never finds them in a dumpable process.
            if launch.routes_document != b"{}" and self._wait_locked(process.pid):
                os.write(write_end, launch.routes_document)
            elif launch.routes_document != b"{}":
                logger.error(
                    "route caller %s was not observed locked; its routes were withheld",
                    launch.command,
                )
        finally:
            os.close(write_end)
        return process

    def _wait_locked(self, pid: int) -> bool:
        deadline = time.monotonic() + _LOCK_WAIT_S
        # Read at call time, so a test on a host without /proc can stand in.
        check = self._lock_check or proc_locked
        while time.monotonic() < deadline:
            if check(pid):
                return True
            time.sleep(_LOCK_POLL_S)
        return False

    def attach(self, session: _Session) -> None:
        with self._guard:
            self._current = session

    def detach(self, session: _Session) -> None:
        with self._guard:
            for relay_id, (owner, original, _hook) in list(self._pending.items()):
                if owner is session:
                    self._pending.pop(relay_id, None)
                    self._by_original.pop((id(session), _key(original)), None)
            if self._current is session:
                self._current = None

    def request(self, session: _Session, message: dict[str, Any], hook: Any) -> None:
        with self._guard:
            self._next_id += 1
            relay_id = self._next_id
            self._pending[relay_id] = (session, message["id"], hook)
            self._by_original[(id(session), _key(message["id"]))] = relay_id
            if message.get("method") == "initialize":
                self._initialize_ids.add(relay_id)
        self.send({**message, "id": relay_id})

    def cancel(self, session: _Session, message: dict[str, Any]) -> Any:
        """Forward a cancellation under the caller's id. Returns the settle hook."""

        params = message.get("params")
        if not isinstance(params, dict):
            return None
        original = params.get("requestId")
        with self._guard:
            relay_id = self._by_original.pop((id(session), _key(original)), None)
            owner = self._pending.pop(relay_id, None) if relay_id is not None else None
        if relay_id is None:
            return None
        self.send({**message, "params": {**params, "requestId": relay_id}})
        return owner[2] if owner is not None else None

    def send(self, message: dict[str, Any]) -> None:
        process = self._process
        if process is None or process.stdin is None:
            return
        line = (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")
        with self._write:
            try:
                process.stdin.write(line)
                process.stdin.flush()
            except OSError:
                self.failed = True

    def _read(self) -> None:
        process = self._process
        assert process is not None and process.stdout is not None
        for line in process.stdout:
            if not line.strip():
                continue
            try:
                message = json.loads(line)
            except ValueError:
                logger.warning("a route caller wrote a line that is not JSON; dropped")
                continue
            if not isinstance(message, dict):
                continue
            self._route(message)
        self.failed = True
        with self._guard:
            sessions = {owner for owner, _original, _hook in self._pending.values()}
            if self._current is not None:
                sessions.add(self._current)
            self._pending.clear()
            self._by_original.clear()
        for session in sessions:
            session.deliver(_CLOSED)

    def _route(self, message: dict[str, Any]) -> None:
        relay_id = message.get("id")
        with self._guard:
            if "method" not in message and isinstance(relay_id, int):
                owner = self._pending.pop(relay_id, None)
                if owner is None:
                    return  # cancelled, or its session is gone
                session, original, _hook = owner
                self._by_original.pop((id(session), _key(original)), None)
                if relay_id in self._initialize_ids and isinstance(message.get("result"), dict):
                    self.initialize_result = message["result"]
                target: _Session | None = session
                message = {**message, "id": original}
            else:
                target = self._current
        if target is not None:
            target.deliver(message)


def _key(value: object) -> object:
    return json.dumps(value, sort_keys=True) if not isinstance(value, (str, int)) else value


def _kill_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            pass


class CallerRelay:
    """The in-process MCP server the SDK mounts for one declared caller.

    The SDK's bridge calls ``run`` with the streams of one session. The first
    run starts the caller; every run relays its session's messages to that
    one process and the caller's messages back. A repeated ``initialize`` is
    answered from the caller's first answer, so a later session never
    restarts it.
    """

    def __init__(
        self,
        name: str,
        launch: CallerLaunch,
        *,
        lock_check: Callable[[int], bool] | None = None,
    ) -> None:
        self.name = name
        self._caller = _Caller(launch, lock_check)

    def create_initialization_options(self) -> None:
        # The caller answers ``initialize`` itself; nothing is negotiated here.
        return None

    @property
    def pid(self) -> int | None:
        process = self._caller._process
        return process.pid if process is not None else None

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
        caller = self._caller
        if not await anyio.to_thread.run_sync(caller.ensure_started):
            return
        session = _Session()
        caller.attach(session)
        try:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(self._inbound, read_stream, session, tasks)
                tasks.start_soon(self._outbound, write_stream, session, tasks)
        finally:
            caller.detach(session)

    async def _inbound(
        self, read_stream: Any, session: _Session, tasks: anyio.abc.TaskGroup
    ) -> None:
        caller = self._caller
        async with read_stream:
            async for item in read_stream:
                if not isinstance(item, SessionMessage):
                    continue
                message = _as_dict(item)
                method = message.get("method")
                hook = getattr(getattr(item, "metadata", None), "on_request_unanswered", None)
                if method == "initialize" and "id" in message:
                    if caller.initialize_result is not None:
                        session.deliver(
                            {
                                "jsonrpc": "2.0",
                                "id": message["id"],
                                "result": caller.initialize_result,
                            }
                        )
                    else:
                        caller.request(session, message, hook)
                elif method == "notifications/initialized":
                    if not caller.initialized_sent:
                        caller.initialized_sent = True
                        caller.send(message)
                elif method == "notifications/cancelled":
                    settle = caller.cancel(session, message)
                    if settle is not None:
                        await settle()
                elif method is not None and "id" in message:
                    caller.request(session, message, hook)
                else:
                    caller.send(message)
        tasks.cancel_scope.cancel()

    async def _outbound(
        self, write_stream: Any, session: _Session, tasks: anyio.abc.TaskGroup
    ) -> None:
        while True:
            message = await anyio.to_thread.run_sync(session.next, abandon_on_cancel=True)
            if message is None:
                continue
            if message is _CLOSED:
                break
            parsed = _parse(json.dumps(message).encode("utf-8"))
            if parsed is not None:
                await write_stream.send(parsed)
        tasks.cancel_scope.cancel()


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
    """Replace each declared caller that has routes this boot with its relay.

    Returns the keys replaced. A caller with no route this boot (no token,
    or no connector scope) holds nothing to protect, so the CLI starts it as
    before. Without the lock library a caller is left to the CLI with no
    routes either, rather than holding a token in a process anyone can read.
    ``cwd`` is where the caller starts: the bundle root, never a directory
    Bash writes.
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
        routes = {
            connector: {"url": connector_urls[connector], "token": tokens[connector]}
            for connector in sorted(connectors)
            if connector in tokens and connector in connector_urls
        }
        if not routes:
            continue
        if lock_library is None:
            logger.error(
                "route caller %s gets no routes: the environ lock library is unavailable",
                server,
            )
            continue
        launch = caller_launch(
            entry, routes=routes, source_env=source_env, lock_library=lock_library, cwd=cwd
        )
        bundle_servers[key] = {"type": "sdk", "name": key, "instance": CallerRelay(key, launch)}
        mounted.add(key)
    return frozenset(mounted)
