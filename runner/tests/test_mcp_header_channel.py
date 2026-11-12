"""Runner-held hosted connector headers reach the connector, never the CLI (#5378).

The runner materializes a hosted connector's ``Authorization`` bearer, its
``X-Curie-Caller`` token and a resumed turn's ``X-Curie-Connector-Grant`` in
memory. ``mcp_header_channel`` mounts each such entry on the Claude CLI as an
in-process relay, so the ``--mcp-config`` file that same-uid Bash can read names
the server and nothing else.

The containment case boots the production runner (``build_runner``) in personal
mode with the bundled native CLI behind the intercepting provider, against a
loopback connector that refuses any request without all three synthetic header
values. Native Bash, a bundle command hook and a declared stdio MCP server each
run one scanner (``hosted_header_scanner``); native Read opens the same
surfaces. The same scanner over a deliberately exposed fixture is the negative
control. The service-mode case compares the relay with the native HTTP
transport it replaces, through a connector restart and an approval-gated call.
"""

from __future__ import annotations

import contextlib
import copy
import ctypes
import json
import logging
import os
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio
import pytest
from aci_protocol import BootEnv, Event, Final, SessionStatus, SideEffectFlag, parse_ndjson
from aiohttp import web
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk._internal.sdk_mcp_bridge import SdkMcpBridge
from curie_internal.connector_endpoints import BINDING_ENV
from curie_runner import RunnerConfig
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.adapter import ClaudeAgentSession
from curie_runner.connectors import (
    _GRANT_ENV,
    CALLER_HEADER,
    GRANT_HEADER,
    materialize_connector_caller_headers,
    materialize_hosted_bearer_headers,
)
from curie_runner.mcp_argv import CredentialSafeCLITransport
from curie_runner.mcp_header_channel import (
    HostedHeaderRelay,
    derived_header_placeholders,
    hosted_header_values,
    mount_hosted_header_channel,
)
from curie_runner.mcp_stdio import UnsafeStdioConfiguration
from curie_runner.mcp_tool_capability import ConnectorCapabilityFailure, McpToolCapabilityProbe
from curie_runner.sdk_auth import resolve_sdk_env
from curie_runner.subprocess_env import lock_process_environ
from curie_telemetry.redact import held_secret_literals
from hosted_header_scanner import CATEGORIES
from mcp.types import CONNECTION_CLOSED
from personal_auth_support import (
    IMPLEMENTER_MODEL,
    OWNER_GRANT,
    REVIEWER_MODEL,
    SERVICE_API_KEY,
    RecordedRequest,
    Reply,
    TextReply,
    ToolCall,
    assert_cli_never_started,
    assert_owner_grant_only,
    installed,
    write_json,
)
from personal_token_support import PUBLIC_MARKER, native_cli_pid, require_native_prerequisites

CONNECTOR = "acme"
BUNDLE = "acme-hosted"
AGENT = "acme-dev"
BEARER_ENV = "ACME_HOSTED_TOKEN"
CHALLENGE_TOOL = f"mcp__{CONNECTOR}__challenge"
RECORD_TOOL = f"mcp__{CONNECTOR}__record"
GATED_TOOL = f"mcp__{CONNECTOR}__write_gated"
SCANNER_SERVER = f"plugin:{BUNDLE}:scanner"
SCANNER_TOOL = f"mcp__plugin_{BUNDLE}_scanner__scan"
_CALL_PREFIX = "toolu_relay_"
_REPLAY_PREFIX = "curie-relay-handshake-"
_TIMEOUT_S = 300
_BRIDGE_TIMEOUT_S = 60
_READY_TIMEOUT_S = 60
_READY_RECONNECTS = 2
_POLICY = {
    "enforcement": "curie/mcp-tool-policy@1",
    "allow": [f"{CONNECTOR}/challenge", f"{CONNECTOR}/record"],
    "approvalRequired": [f"{CONNECTOR}/write_gated"],
}
# MCP tools shapes: https://modelcontextprotocol.io/specification/2025-11-25/server/tools
_TOOLS: list[dict[str, Any]] = [
    {
        "name": "challenge",
        "description": "Return this connector's challenge.",
        "inputSchema": {"type": "object", "properties": {}},
        "annotations": {"readOnlyHint": True},
    },
    {
        "name": "record",
        "description": "Record one value.",
        "inputSchema": {
            "type": "object",
            "properties": {"value": {"type": "string", "description": "The value to record."}},
            "required": ["value"],
        },
        "annotations": {"readOnlyHint": False},
    },
    {
        "name": "write_gated",
        "description": "Write one value once it is approved.",
        "inputSchema": {
            "type": "object",
            "properties": {"value": {"type": "string", "description": "The value to write."}},
        },
        "annotations": {"readOnlyHint": False},
    },
]

_HOOK = """\
import runpy
import sys
from pathlib import Path

root = Path(__file__).resolve().parent
sys.stdin.read()
spec = root / "scan-spec.json"
if spec.is_file():
    runpy.run_path(str(root / "scan.py"), run_name="acme_scan")["run"](spec, "hook")
print("{}")
"""

_SCAN_SERVER = '''\
import json
import runpy
from pathlib import Path

from mcp.server.mcpserver import MCPServer

root = Path(__file__).resolve().parent
server = MCPServer("acme-scanner")


@server.tool()
def scan() -> str:
    """Run the containment scanner from this declared stdio server."""
    module = runpy.run_path(str(root / "scan.py"), run_name="acme_scan")
    return json.dumps(module["run"](root / "scan-spec.json", "stdio"))


server.run()
'''


@pytest.fixture
def native_prerequisites() -> str:
    return require_native_prerequisites()


@dataclass(frozen=True)
class _Sentinels:
    bearer: str
    caller: str
    grant: str

    @classmethod
    def fresh(cls) -> _Sentinels:
        nonce = uuid4().hex
        return cls(
            bearer=f"acme-bearer-sentinel-{nonce}",
            caller=f"cct.acme-caller-sentinel-{nonce}.sig",
            grant=f"ccg.acme-grant-sentinel-{nonce}.sig",
        )

    def values(self) -> tuple[str, ...]:
        return (self.bearer, self.caller, self.grant)


def _carries(data: str | bytes, values: Iterable[str]) -> bool:
    """Whether ``data`` holds a value or a base64 form of one (never prints it)."""

    text = data.decode("utf-8", "replace") if isinstance(data, bytes) else data
    return any(literal in text for literal in held_secret_literals(list(values)))


# --- the hosted connector -------------------------------------------------------------


class _HostedUpstream:
    """A loopback connector that answers only requests carrying all held headers.

    It records each request's method, id, tool, status and whether it was
    authorized, never a header value, and each write it ran in ``committed``.
    ``restart`` forgets every session, as a restarted connector does; ``halt``
    and ``resume`` close and reopen the listener on the same port; ``withhold``
    runs one write and never answers it.
    """

    def __init__(self, sentinels: _Sentinels, *, require_grant: bool) -> None:
        self._sentinels = sentinels
        self._require_grant = require_grant
        self.challenge = f"CHALLENGE-{uuid4().hex}"
        self.generation = 0
        self.calls: list[dict[str, Any]] = []
        self.committed: list[str] = []
        self._sessions: set[str] = set()
        self._runner: web.AppRunner | None = None
        self._withheld: dict[str, anyio.Event] = {}
        self._released: anyio.Event | None = None
        self.port = 0

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    async def __aenter__(self) -> _HostedUpstream:
        await self._serve()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.halt()

    async def _serve(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", self.port))
        self.port = int(listener.getsockname()[1])
        app = web.Application()
        app.router.add_route("*", "/mcp", self._handle)
        runner = web.AppRunner(app, shutdown_timeout=1.0, access_log=None)
        await runner.setup()
        await web.SockSite(runner, listener).start()
        self._runner = runner

    def restart(self) -> None:
        self.generation += 1
        self._sessions.clear()

    def withhold(self, value: str) -> anyio.Event:
        """Run the next write of ``value`` and hold its answer until ``halt``.

        Returns an event set once that write has run, so a test can act while
        its answer is outstanding.
        """

        ran = anyio.Event()
        self._withheld[value] = ran
        if self._released is None:
            self._released = anyio.Event()
        return ran

    async def halt(self) -> None:
        if self._released is not None:
            self._released.set()
        runner, self._runner = self._runner, None
        if runner is not None:
            await runner.cleanup()

    async def resume(self) -> None:
        self.restart()
        await self._serve()

    def _authorized(self, request: web.Request) -> bool:
        headers = request.headers
        held = self._sentinels
        return (
            headers.get("Authorization") == f"Bearer {held.bearer}"
            and headers.get(CALLER_HEADER) == held.caller
            and (not self._require_grant or headers.get(GRANT_HEADER) == held.grant)
        )

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        if request.method != "POST":
            return web.Response(status=405)
        message = await request.json()
        params = message.get("params") if isinstance(message.get("params"), dict) else {}
        method = message.get("method")
        record = {
            "method": method,
            "id": message.get("id"),
            "tool": params.get("name") if method == "tools/call" else None,
            "authorized": self._authorized(request),
            "generation": self.generation,
        }
        response = self._answer(request, message, params, authorized=record["authorized"])
        self.calls.append({**record, "status": response.status})
        arguments = params.get("arguments")
        value = arguments.get("value") if isinstance(arguments, dict) else None
        ran = (
            self._withheld.pop(value, None)
            if isinstance(value, str) and record["tool"] == "record" and response.status == 200
            else None
        )
        if ran is not None and self._released is not None:
            # The write ran; its answer waits until the connector halts.
            ran.set()
            await self._released.wait()
        return response

    def _answer(
        self,
        request: web.Request,
        message: dict[str, Any],
        params: dict[str, Any],
        *,
        authorized: bool,
    ) -> web.Response:
        if not authorized:
            return web.Response(status=401)
        method = message.get("method")
        if method == "initialize":
            session = uuid4().hex
            self._sessions.add(session)
            return web.json_response(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "result": {
                        "protocolVersion": params.get("protocolVersion", "2025-06-18"),
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "acme-upstream", "version": "1"},
                    },
                },
                headers={"Mcp-Session-Id": session},
            )
        if request.headers.get("Mcp-Session-Id") not in self._sessions:
            # The streamable-HTTP spelling of a session the server no longer knows.
            return web.Response(status=404)
        if "id" not in message:
            return web.Response(status=202)
        if method == "tools/list":
            return self._result(message, {"tools": _TOOLS})
        if method == "tools/call":
            name = params.get("name")
            arguments = params.get("arguments") or {}
            if name == "challenge":
                text = self.challenge
            elif name in ("record", "write_gated"):
                self.committed.append(str(arguments.get("value")))
                text = json.dumps({"recorded": arguments.get("value")})
            else:
                return self._error(message, -32602, "unknown tool")
            return self._result(
                message, {"content": [{"type": "text", "text": text}], "isError": False}
            )
        if method == "ping":
            return self._result(message, {})
        return self._error(message, -32601, "Method not found")

    @staticmethod
    def _result(message: dict[str, Any], result: dict[str, Any]) -> web.Response:
        return web.json_response({"jsonrpc": "2.0", "id": message["id"], "result": result})

    @staticmethod
    def _error(message: dict[str, Any], code: int, text: str) -> web.Response:
        return web.json_response(
            {"jsonrpc": "2.0", "id": message["id"], "error": {"code": code, "message": text}}
        )


# --- the scripted provider ---------------------------------------------------------------


@dataclass(frozen=True)
class _Step:
    name: str
    input: dict[str, Any]
    before: Callable[[], None] | None = None


@dataclass
class _Script:
    """The provider's tool calls, keyed by tool_use id rather than request count.

    Results are captured as the model received them, before ACI redaction. The
    first main-loop request's tool catalog is kept for comparison.
    """

    steps: list[_Step] = field(default_factory=list)
    issued: int = 0
    results: dict[int, str] = field(default_factory=dict)
    catalog: dict[str, dict[str, Any]] | None = None
    failures: list[str] = field(default_factory=list)

    def __call__(self, request: RecordedRequest) -> Reply:
        try:
            return self._reply(request)
        except Exception as exc:  # noqa: BLE001 - recorded; the test fails on it
            self.failures.append(type(exc).__name__)
            return TextReply(PUBLIC_MARKER)

    def _reply(self, request: RecordedRequest) -> Reply:
        tools = request.body.get("tools")
        if not isinstance(tools, list) or not tools:
            return TextReply(PUBLIC_MARKER)
        if self.catalog is None:
            self.catalog = {
                tool["name"]: {key: tool.get(key) for key in ("description", "input_schema")}
                for tool in tools
                if isinstance(tool, dict) and isinstance(tool.get("name"), str)
            }
        for block in request.tool_results():
            call_id = block.get("tool_use_id")
            if isinstance(call_id, str) and call_id.startswith(_CALL_PREFIX):
                self.results[int(call_id.removeprefix(_CALL_PREFIX))] = _result_text(block)
        index = self.issued
        if self.issued and self.issued - 1 not in self.results:
            index = self.issued - 1  # the same request again: repeat its call
        if index >= len(self.steps):
            return TextReply(PUBLIC_MARKER)
        step = self.steps[index]
        if index == self.issued:
            if step.before is not None:
                step.before()
            self.issued += 1
        return ToolCall(step.name, dict(step.input), f"{_CALL_PREFIX}{index}")


def _result_text(block: dict[str, Any]) -> str:
    content = block.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(str(item.get("text", "")) for item in content if isinstance(item, dict))
    return ""


def _connector_tools(catalog: dict[str, dict[str, Any]] | None) -> dict[str, dict[str, Any]]:
    prefix = f"mcp__{CONNECTOR}__"
    return {name: spec for name, spec in (catalog or {}).items() if name.startswith(prefix)}


# --- the production boot -----------------------------------------------------------------


def _write_bundle(root: Path, *, policy: dict[str, Any] | None = None, scanner: bool) -> Path:
    manifest: dict[str, Any] = {
        "name": BUNDLE,
        "version": "0.1.0",
        "description": "Hosted header channel fixture.",
    }
    if policy is not None:
        manifest["toolPolicy"] = policy
    root.mkdir(parents=True)
    if scanner:
        (root / "reports").mkdir()
        (root / "public.txt").write_text(PUBLIC_MARKER, encoding="utf-8")
        shutil.copyfile(Path(__file__).with_name("hosted_header_scanner.py"), root / "scan.py")
        (root / "hook.py").write_text(_HOOK, encoding="utf-8")
        (root / "scan_server.py").write_text(_SCAN_SERVER, encoding="utf-8")
        manifest["mcpServers"] = {
            "scanner": {"command": sys.executable, "args": ["-I", str(root / "scan_server.py")]}
        }
        manifest["hooks"] = {
            "PreToolUse": [
                {
                    "matcher": "Bash",
                    "hooks": [
                        {
                            "type": "command",
                            "command": shlex.join([sys.executable, "-I", str(root / "hook.py")]),
                        }
                    ],
                }
            ]
        }
    write_json(root / ".claude-plugin" / "plugin.json", manifest)
    (root / "connectors.yaml").write_text(
        f"connectors:\n  {CONNECTOR}:\n    image: example/acme-connector:1\n"
        f"    secrets: [{BEARER_ENV}]\n",
        encoding="utf-8",
    )
    return root


def _boot_env(
    monkeypatch: pytest.MonkeyPatch,
    bundle: Path,
    base_url: str,
    sentinels: _Sentinels,
    *,
    grant: bool,
) -> RunnerConfig:
    """The boot env a static hosted-endpoint install renders, with synthetic values."""

    values = {
        BootEnv.env_key("plugin_dir"): str(bundle),
        BootEnv.env_key("session_id"): str(uuid4()),
        BootEnv.env_key("sandbox_id"): "sandbox-PLACEHOLDER",
        BootEnv.env_key("budget"): json.dumps(
            {"max_output_tokens_per_run": 100000, "max_usd_per_day": 5.0}
        ),
        BootEnv.env_key("model"): IMPLEMENTER_MODEL,
        BootEnv.env_key("reviewer_model"): REVIEWER_MODEL,
        BootEnv.env_key("max_turns"): "40",
        BootEnv.env_key("connector_caller_token"): sentinels.caller,
        BINDING_ENV: json.dumps(
            {"version": 1, "agent": AGENT, "connectors": {CONNECTOR: {"base_url": base_url}}}
        ),
        BEARER_ENV: sentinels.bearer,
    }
    if grant:
        values[_GRANT_ENV] = sentinels.grant
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    return RunnerConfig.from_env(os.environ)


def _record_sessions(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Keep each session the boot builds, to read its native CLI process."""

    sessions: list[Any] = []

    class RecordingSession(ClaudeAgentSession):
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            sessions.append(self)

    monkeypatch.setattr(boot, "ClaudeAgentSession", RecordingSession)
    return sessions


def _live_config(session: Any) -> Path:
    """The ``--mcp-config`` file the transport wrote for this CLI."""

    query = session._client._query
    transport = query._transport if hasattr(query, "_transport") else query.transport
    paths = list(transport._mcp_config_tmps)
    assert len(paths) == 1, "the CLI was not given exactly one MCP configuration file"
    return Path(paths[0])


@dataclass(frozen=True)
class _Started:
    cli: int
    live: Path


@dataclass(frozen=True)
class _Turn:
    lines: list[str]
    cmdline: bytes
    live_text: str


async def _mcp_status(session: Any, name: str) -> str | None:
    status = await session._client.get_mcp_status()
    for server in status.get("mcpServers", []):
        if server.get("name") == name:
            found = server.get("status")
            return found if isinstance(found, str) else None
    return None


async def _await_mcp_ready(session: Any, servers: Iterable[str]) -> None:
    """Every named server is ``connected`` in the CLI's own MCP status, or the test fails.

    The CLI connects MCP servers in the background, so a query sent at once can
    be answered from a catalog built before they finish. Each status read is a
    control round trip to the CLI, so a server still ``pending`` is read again
    rather than slept on. One that ``failed`` gets the SDK reconnect the
    session's own repair uses (``ensure_mcp_server``), a control request that
    returns only once that attempt is over. Not connected by the deadline, or
    after the reconnects, is a failure, never a skip.
    """

    with anyio.fail_after(_READY_TIMEOUT_S):
        for name in servers:
            reconnects = 0
            while (status := await _mcp_status(session, name)) != "connected":
                if status in (None, "pending"):
                    # Not registered yet, or still connecting: read again.
                    continue
                assert reconnects < _READY_RECONNECTS, (
                    f"MCP server {name} is {status} after {reconnects} reconnects"
                )
                reconnects += 1
                with contextlib.suppress(Exception):
                    await session._client.reconnect_mcp_server(name)


async def _serve_turn(
    config: RunnerConfig,
    sessions: list[Any],
    prepare: Callable[[_Started], None],
    *,
    servers: Iterable[str],
    script: _Script,
) -> _Turn:
    """``build_runner`` as boot runs it, one turn, and the CLI's argv and config.

    The turn starts only once every server in ``servers`` is connected, and the
    script's catalog is the one the provider sees after that.
    """

    sdk_env = resolve_sdk_env(os.environ)
    # The boot probe runs its own event loop; the connector keeps serving on this one.
    runner = await anyio.to_thread.run_sync(lambda: build_runner(config, sdk_env=sdk_env))
    lines: list[str] = []
    try:
        with anyio.fail_after(_TIMEOUT_S):
            await runner.start()
            session = sessions[-1]
            await _await_mcp_ready(session, servers)
            script.catalog = None
            cli = native_cli_pid(session)
            live = _live_config(session)
            prepare(_Started(cli=cli, live=live))
            async for line in runner.run_turn(
                Event(
                    type="message",
                    text="Run the hosted connector probe.",
                    user="U0EXAMPLE1",
                    ts="1",
                )
            ):
                lines.append(line)
            cmdline = Path(f"/proc/{cli}/cmdline").read_bytes()
            live_text = live.read_text(encoding="utf-8")
    finally:
        await runner.close()
    return _Turn(lines=lines, cmdline=cmdline, live_text=live_text)


def _final(lines: list[str]) -> Final:
    finals = [event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)]
    assert len(finals) == 1, f"expected one final, got {len(finals)}"
    return finals[0]


def _flags(lines: list[str]) -> list[tuple[str | None, bool | None]]:
    return [
        (event.tool, event.failed)
        for event in parse_ndjson("".join(lines))
        if isinstance(event, SideEffectFlag)
    ]


def _list_mcp_tools_upfront(monkeypatch: pytest.MonkeyPatch) -> None:
    """Have the CLI post every MCP tool in its provider request.

    The bundled CLI otherwise defers MCP tools behind ToolSearch, so the
    ``tools`` array the provider sees names none of them (see
    ``test_channel_hidden_tools``). ``ENABLE_TOOL_SEARCH=false`` turns that off;
    it reaches the CLI through the process env, which ``cli_parent_env`` keeps,
    and is set the same way for the native control and the relay.
    """

    monkeypatch.setenv("ENABLE_TOOL_SEARCH", "false")


@contextlib.contextmanager
def _locked_environ() -> Iterator[None]:
    """The runner process's environ lock, as ``__main__.main`` takes it."""

    libc = ctypes.CDLL(None, use_errno=True)
    prior = libc.prctl(3, 0, 0, 0, 0)
    lock_process_environ()
    try:
        yield
    finally:
        if libc.prctl(4, prior, 0, 0, 0) != 0:
            raise OSError(ctypes.get_errno())


def _reports(bundle: Path, label: str) -> list[dict[str, Any]]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted((bundle / "reports").glob(f"{label}-*.json"))
    ]


def _assert_clean(report: dict[str, Any], label: str) -> None:
    for key in ("public", "public_symlink", "public_hardlink"):
        assert report[key] == PUBLIC_MARKER, f"the {label} scan did not read its {key} control"
    assert report["found"] == [], f"the {label} scan recovered a held value at {report['found']}"
    empty = [category for category in CATEGORIES if report["counts"][category] <= 0]
    assert not empty, f"the {label} scan inspected nothing for {empty}"
    assert report["live_config"] == {"type": "sdk", "keys": ["name", "type"]}, (
        f"the {label} scan saw the connector's CLI entry as {report['live_config']}"
    )


# --- personal mode: every native channel --------------------------------------------------


def test_runner_held_hosted_headers_never_reach_a_native_channel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    native_prerequisites: str,
) -> None:
    sentinels = _Sentinels.fresh()
    held = sentinels.values()
    script = _Script()
    sessions = _record_sessions(monkeypatch)
    caplog.set_level(logging.DEBUG)
    reads: dict[str, str] = {}
    read_dir = Path(tempfile.mkdtemp(prefix="curie-relay-read-"))
    with installed(tmp_path, monkeypatch, script) as installation:
        monkeypatch.setenv("CURIE_PROC_DUMPABLE_PRELOAD", native_prerequisites)
        _list_mcp_tools_upfront(monkeypatch)
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
        monkeypatch.chdir(installation.workspace)
        bundle = _write_bundle(installation.workspace / "bundle", scanner=True)
        spec_path = bundle / "scan-spec.json"

        def prepare(started: _Started) -> None:
            write_json(
                spec_path,
                {
                    "reversed": [value[::-1] for value in held],
                    "public": str(bundle / "public.txt"),
                    "roots": [str(installation.workspace), str(installation.home)],
                    "tempdir": tempfile.gettempdir(),
                    "reports": str(bundle / "reports"),
                    "cli_pid": started.cli,
                    "live_config": str(started.live),
                    "server": CONNECTOR,
                },
            )
            config_symlink = read_dir / "config.symlink"
            config_symlink.symlink_to(started.live)
            config_hardlink = read_dir / "config.hardlink"
            os.link(started.live, config_hardlink)
            environ_symlink = read_dir / "environ.symlink"
            environ_symlink.symlink_to(f"/proc/{started.cli}/environ")
            reads.update(
                {
                    "config": str(started.live),
                    "config-symlink": str(config_symlink),
                    "config-hardlink": str(config_hardlink),
                    "self-environ": "/proc/self/environ",
                    "cli-environ": f"/proc/{started.cli}/environ",
                    "cli-cmdline": f"/proc/{started.cli}/cmdline",
                    "cli-root": f"/proc/{started.cli}/root/proc/{started.cli}/environ",
                    "environ-symlink": str(environ_symlink),
                    "public": str(bundle / "public.txt"),
                }
            )
            script.steps = [
                _Step(CHALLENGE_TOOL, {}),
                _Step(
                    "Bash",
                    {
                        "command": shlex.join(
                            [sys.executable, "-I", str(bundle / "scan.py"), str(spec_path), "bash"]
                        )
                    },
                ),
                _Step(SCANNER_TOOL, {}),
                *(_Step("Read", {"file_path": target}) for target in reads.values()),
            ]

        async def scenario() -> tuple[_Turn, list[dict[str, Any]], str]:
            async with _HostedUpstream(sentinels, require_grant=True) as upstream:
                config = _boot_env(monkeypatch, bundle, upstream.base_url, sentinels, grant=True)
                turn = await _serve_turn(
                    config,
                    sessions,
                    prepare,
                    servers=(CONNECTOR, SCANNER_SERVER),
                    script=script,
                )
                return turn, list(upstream.calls), upstream.challenge

        try:
            with _locked_environ():
                turn, calls, challenge = anyio.run(scenario)
        finally:
            shutil.rmtree(read_dir, ignore_errors=True)
        model_bodies = json.dumps([request.body for request in installation.proxy.all_requests()])
        assert_owner_grant_only(installation.proxy)

    results = script.results
    assert script.failures == [], f"the provider script failed: {script.failures}"
    assert script.issued == len(script.steps), "the provider script did not issue every call"
    missing = [index for index in range(len(script.steps)) if index not in results]
    assert not missing, f"tool calls without a result: {missing}"
    # The connector answered through the relay, with every runner-held header:
    # a real connector result, not the CLI's own error for a tool it lacks.
    assert challenge in results[0], "the connector's challenge did not reach the model"
    assert any(call["tool"] == "challenge" and call["status"] == 200 for call in calls)
    assert calls and all(call["authorized"] for call in calls), "a request lacked a held header"
    assert {CHALLENGE_TOOL, SCANNER_TOOL} <= set(script.catalog or {}), "the catalog is short"
    # Every channel ran the scanner and read its public control; none recovered a value.
    assert PUBLIC_MARKER in results[1], "native Bash did not run the scanner"
    assert PUBLIC_MARKER in results[2], "the declared stdio MCP server did not run the scanner"
    for label in ("bash", "hook", "stdio"):
        reports = _reports(bundle, label)
        assert reports, f"no {label} scan ran"
        for report in reports:
            _assert_clean(report, label)
    for offset, name in enumerate(reads):
        text = results[3 + offset]
        assert not _carries(text, held), f"native Read of {name} recovered a held value"
        assert "No such file" not in text, f"native Read of {name} had no target to read"
        if name.startswith("config"):
            assert '"sdk"' in text, f"native Read of {name} did not show the relay entry"
    assert PUBLIC_MARKER in results[3 + list(reads).index("public")]
    # The CLI's configuration names the server and nothing else.
    assert json.loads(turn.live_text)["mcpServers"][CONNECTOR] == {
        "type": "sdk",
        "name": CONNECTOR,
    }
    surfaces: dict[str, str | bytes] = {
        "the MCP config file": turn.live_text,
        "the CLI argv": turn.cmdline,
        "the ACI events": "".join(turn.lines),
        "a model request": model_bodies,
        "the logs": caplog.text,
        "a log message": "\n".join(record.getMessage() for record in caplog.records),
    }
    for surface, data in surfaces.items():
        assert not _carries(data, held), f"{surface} carried a relay-held value"
    assert _final(turn.lines).status in {SessionStatus.DONE, SessionStatus.IDLE_AWAITING_INPUT}

    # Negative control: the same scanner recovers a value that is really exposed.
    negative = _negative_control(bundle, sentinels.bearer, tmp_path)
    assert negative["public"] == PUBLIC_MARKER
    recovered = {entry.split(":", 1)[0] for entry in negative["found"]}
    assert recovered == set(CATEGORIES), f"the scanner missed {set(CATEGORIES) - recovered}"


def _negative_control(bundle: Path, value: str, scratch: Path) -> dict[str, Any]:
    """Expose ``value`` in every category on purpose and scan for it."""

    fixture = Path(tempfile.gettempdir()) / f"curie-mcp-config-negative-{uuid4().hex}.json"
    fixture.write_text(
        json.dumps(
            {
                "mcpServers": {
                    CONNECTOR: {
                        "type": "http",
                        "url": "http://127.0.0.1:9/mcp",
                        "headers": {"Authorization": f"Bearer {value}"},
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    exposed_env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "ACME_EXPOSED": value}
    held_file = scratch / "held-descriptor"
    held_file.write_text(value, encoding="utf-8")
    descriptor = os.open(held_file, os.O_RDONLY)
    held_file.unlink()
    exposed = subprocess.Popen(
        [
            sys.executable,
            "-I",
            "-c",
            "import sys; print('ready', flush=True); sys.stdin.read()",
            f"--exposed={value}",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        env=exposed_env,
    )
    try:
        assert exposed.stdout is not None
        assert exposed.stdout.readline().strip() == b"ready"
        spec_path = scratch / "negative-spec.json"
        write_json(
            spec_path,
            {
                "reversed": [value[::-1]],
                "public": str(bundle / "public.txt"),
                "roots": [str(bundle)],
                "tempdir": tempfile.gettempdir(),
                "reports": str(bundle / "reports"),
                "cli_pid": exposed.pid,
                "live_config": str(fixture),
                "server": CONNECTOR,
            },
        )
        subprocess.run(
            [sys.executable, "-I", str(bundle / "scan.py"), str(spec_path), "negative"],
            env=exposed_env,
            pass_fds=(descriptor,),
            stdout=subprocess.DEVNULL,
            check=True,
            timeout=120,
        )
    finally:
        assert exposed.stdin is not None
        exposed.stdin.close()
        exposed.wait(timeout=10)
        os.close(descriptor)
        fixture.unlink(missing_ok=True)
    (report,) = _reports(bundle, "negative")
    return report


# --- service mode: the relay against the native transport it replaces -------------------------


def _unmounted(servers: dict[str, Any], **_: object) -> dict[str, Any]:
    return dict(servers)


def test_service_mode_relay_matches_the_native_http_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    native_prerequisites: str,
) -> None:
    sentinels = _Sentinels.fresh()
    held = sentinels.values()
    sessions = _record_sessions(monkeypatch)
    native_script, relay_script = _Script(), _Script()
    caplog.set_level(logging.DEBUG)
    with installed(tmp_path, monkeypatch, native_script) as installation:
        monkeypatch.setenv("CURIE_PROC_DUMPABLE_PRELOAD", native_prerequisites)
        _list_mcp_tools_upfront(monkeypatch)
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "service")
        monkeypatch.setenv("ANTHROPIC_API_KEY", SERVICE_API_KEY)
        # The native control's CLI dials the loopback connector itself.
        monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
        monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
        monkeypatch.chdir(installation.workspace)
        bundle = _write_bundle(installation.workspace / "bundle", policy=_POLICY, scanner=False)

        async def scenario() -> tuple[
            _Turn, _Turn, list[dict[str, Any]], list[dict[str, Any]], str
        ]:
            async with _HostedUpstream(sentinels, require_grant=False) as upstream:
                # The control: the same boot with the channel off, so the CLI
                # dials the connector with the headers in its own configuration.
                with monkeypatch.context() as control:
                    control.setattr(boot, "mount_hosted_header_channel", _unmounted)
                    native_script.steps = [
                        _Step(CHALLENGE_TOOL, {}),
                        _Step(RECORD_TOOL, {"value": "acme-value"}),
                        _Step(GATED_TOOL, {"value": "acme-gated"}),
                    ]
                    config = _boot_env(
                        monkeypatch, bundle, upstream.base_url, sentinels, grant=False
                    )
                    native = await _serve_turn(
                        config,
                        sessions,
                        lambda _started: None,
                        servers=(CONNECTOR,),
                        script=native_script,
                    )
                native_calls = list(upstream.calls)
                upstream.calls.clear()
                caplog.clear()
                installation.proxy.responder = relay_script
                relay_script.steps = [
                    _Step(CHALLENGE_TOOL, {}),
                    # The connector restarts and forgets the relay's session.
                    _Step(CHALLENGE_TOOL, {}, before=upstream.restart),
                    _Step(RECORD_TOOL, {"value": "acme-value"}),
                    _Step(GATED_TOOL, {"value": "acme-gated"}),
                ]
                config = _boot_env(monkeypatch, bundle, upstream.base_url, sentinels, grant=False)
                relay = await _serve_turn(
                    config,
                    sessions,
                    lambda _started: None,
                    servers=(CONNECTOR,),
                    script=relay_script,
                )
                return native, relay, native_calls, list(upstream.calls), upstream.challenge

        native, relay, native_calls, relay_calls, challenge = anyio.run(scenario)
        model_requests = installation.proxy.model_requests()
        relay_logs = caplog.text

    for name, script in (("native", native_script), ("relay", relay_script)):
        assert script.failures == [], f"{name}: the provider script failed: {script.failures}"
        assert script.issued == len(script.steps), f"{name}: not every call was issued"
        # The gated call is the last; its pending approval ends the turn, so the
        # model never gets a result for it. Every earlier call has one.
        answered = set(range(len(script.steps) - 1))
        assert answered <= set(script.results), f"{name}: a call lacks a result"
    # Each answered call is a real connector result, recorded by the connector.
    for name, calls in (("native", native_calls), ("relay", relay_calls)):
        served = [
            (call["tool"], call["status"]) for call in calls if call["method"] == "tools/call"
        ]
        assert ("challenge", 200) in served, f"{name}: the connector served no challenge"
        assert ("record", 200) in served, f"{name}: the connector served no record"
    assert '"recorded": "acme-value"' in native_script.results[1]
    # Identical tool names, descriptions and schemas.
    native_tools = _connector_tools(native_script.catalog)
    assert set(native_tools) == {CHALLENGE_TOOL, RECORD_TOOL, GATED_TOOL}
    assert _connector_tools(relay_script.catalog) == native_tools
    # Identical results, including across the connector restart.
    assert challenge in native_script.results[0]
    assert native_script.results[0] == relay_script.results[0] == relay_script.results[1]
    assert native_script.results[1] == relay_script.results[2]
    assert '"recorded": "acme-value"' in relay_script.results[2]
    redials = [
        call for call in relay_calls if call["method"] == "initialize" and call["generation"]
    ]
    assert redials and all(str(call["id"]).startswith(_REPLAY_PREFIX) for call in redials)
    restarted = [call for call in relay_calls if call["generation"] == 1]
    assert any(call["status"] == 404 for call in restarted), "the restart expired no session"
    assert [call["status"] for call in restarted if call["tool"] == "challenge"][-1] == 200
    # Identical side-effect classification and approval handling.
    assert _flags(relay.lines) == _flags(native.lines)
    assert (RECORD_TOOL, False) in _flags(relay.lines)
    assert all(tool != CHALLENGE_TOOL for tool, _failed in _flags(relay.lines))
    native_final, relay_final = _final(native.lines), _final(relay.lines)
    assert native_final.status is SessionStatus.AWAITING_APPROVAL
    assert relay_final.status is SessionStatus.AWAITING_APPROVAL
    assert relay_final.approval_summary is not None and GATED_TOOL in relay_final.approval_summary
    for calls in (native_calls, relay_calls):
        assert calls and all(call["authorized"] for call in calls)
        assert not any(call["tool"] == "write_gated" for call in calls), "a gated call ran"
    # The control's configuration carried the headers; the relay's does not.
    assert json.loads(native.live_text)["mcpServers"][CONNECTOR]["type"] == "http"
    assert _carries(native.live_text, held), "the control did not exercise the native path"
    assert json.loads(relay.live_text)["mcpServers"][CONNECTOR] == {
        "type": "sdk",
        "name": CONNECTOR,
    }
    for surface, data in (
        ("the MCP config file", relay.live_text),
        ("the CLI argv", relay.cmdline),
        ("the ACI events", "".join(relay.lines)),
        ("the logs", relay_logs),
    ):
        assert not _carries(data, held), f"{surface} carried a relay-held value"
    assert model_requests
    assert all(request.api_key == "service api key" for request in model_requests)


# --- the relay through the SDK bridge -------------------------------------------------------


def _materialized(
    base_url: str, sentinels: _Sentinels, *, extra: dict[str, Any] | None = None
) -> tuple[dict[str, Any], dict[str, dict[str, str]], dict[str, str]]:
    """Derived entries as ``build_runner`` holds them after materialization.

    Returns the entries, the pre-materialization header snapshot and the env
    the runner materialized from (with the hosted names already dropped).
    """

    caller_env = BootEnv.env_key("connector_caller_token")
    servers: dict[str, Any] = {
        CONNECTOR: {
            "type": "http",
            "url": f"{base_url}/mcp",
            "headers": {
                "Authorization": f"Bearer ${{{BEARER_ENV}}}",
                CALLER_HEADER: f"${{{caller_env}}}",
                GRANT_HEADER: f"${{{_GRANT_ENV}}}",
            },
        },
        **copy.deepcopy(extra or {}),
    }
    snapshot = derived_header_placeholders(servers)
    env = {
        BEARER_ENV: sentinels.bearer,
        caller_env: sentinels.caller,
        _GRANT_ENV: sentinels.grant,
        "ACME_REMOTE_KEY": "acme-remote-public-value",
    }
    materialize_hosted_bearer_headers(servers, env)
    materialize_connector_caller_headers(servers, env)
    return servers, snapshot, env


def _initialize(request_id: int) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "acme-cli", "version": "1"},
        },
    }


_INITIALIZED = {"jsonrpc": "2.0", "method": "notifications/initialized"}


def _challenge(request_id: int) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "challenge", "arguments": {}},
    }


def _record(request_id: int, value: str) -> dict[str, Any]:
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "record", "arguments": {"value": value}},
    }


def test_relay_redials_a_dropped_connector_and_refuses_while_it_is_down(
    caplog: pytest.LogCaptureFixture,
) -> None:
    sentinels = _Sentinels.fresh()
    caplog.set_level(logging.DEBUG)

    async def scenario() -> tuple[dict[str, Any], list[dict[str, Any]], str, str]:
        async with _HostedUpstream(sentinels, require_grant=True) as upstream:
            servers, snapshot, env = _materialized(upstream.base_url, sentinels)
            mounted = mount_hosted_header_channel(
                servers, derived_headers=snapshot, env=env, release=(env,)
            )
            relay = mounted[CONNECTOR]["instance"]
            assert isinstance(relay, HostedHeaderRelay)
            bridge = SdkMcpBridge(relay.name, relay)  # type: ignore[arg-type]
            replies: dict[str, Any] = {}
            try:
                with anyio.fail_after(_BRIDGE_TIMEOUT_S):
                    replies["initialize"] = await bridge.handle(_initialize(1))
                    await bridge.handle(_INITIALIZED)
                    replies["list"] = await bridge.handle(
                        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
                    )
                    replies["first"] = await bridge.handle(_challenge(3))
                    upstream.restart()
                    replies["restarted"] = await bridge.handle(_challenge(4))
                    await upstream.halt()
                    replies["down"] = await bridge.handle(_challenge(5))
                    await upstream.resume()
                    replies["resumed"] = await bridge.handle(_challenge(6))
                    replies["reinitialize"] = await bridge.handle(_initialize(7))
                    await bridge.handle(_INITIALIZED)
                    replies["reinitialized"] = await bridge.handle(_challenge(8))
            finally:
                await bridge.aclose()
            return replies, list(upstream.calls), upstream.challenge, repr(relay)

    replies, calls, challenge, relay_repr = anyio.run(scenario)
    assert replies["initialize"]["result"]["serverInfo"]["name"] == "acme-upstream"
    assert replies["reinitialize"]["result"]["serverInfo"]["name"] == "acme-upstream"
    listed = [tool["name"] for tool in replies["list"]["result"]["tools"]]
    assert listed == ["challenge", "record", "write_gated"]
    for key in ("first", "restarted", "resumed", "reinitialized"):
        assert replies[key]["result"]["content"][0]["text"] == challenge, key
    down = replies["down"]
    assert "result" not in down
    assert down["error"]["code"] == CONNECTION_CLOSED
    assert down["error"]["message"] == f"hosted connector {CONNECTOR} is unavailable"
    # One upstream session per handshake: the CLI's, then a replay after each
    # drop, then a new one for the CLI's second handshake.
    initializes = [
        (call["id"], call["generation"]) for call in calls if call["method"] == "initialize"
    ]
    assert initializes == [
        (1, 0),
        (f"{_REPLAY_PREFIX}1", 1),
        (f"{_REPLAY_PREFIX}2", 2),
        (7, 2),
    ]
    statuses = [call["status"] for call in calls if call["tool"] == "challenge"]
    assert statuses == [200, 404, 200, 200, 200]
    assert all(call["authorized"] for call in calls)
    for surface, data in (
        ("a bridge reply", json.dumps(replies)),
        ("the relay repr", relay_repr),
        ("the logs", caplog.text),
    ):
        assert not _carries(data, sentinels.values()), f"{surface} carried a relay-held value"


def test_session_expiry_replays_only_the_refused_call_never_a_write_in_flight(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A connector restart between running a write and answering it.

    The write's answer is lost with the old session, and a concurrent call on
    that session is refused for the expired session. Only the refused call is
    sent again; the write fails back to its caller and runs exactly once.
    """

    sentinels = _Sentinels.fresh()
    write_value = f"acme-write-{uuid4().hex}"
    caplog.set_level(logging.DEBUG)

    async def scenario() -> tuple[dict[str, Any], list[str], list[dict[str, Any]], str]:
        async with _HostedUpstream(sentinels, require_grant=True) as upstream:
            servers, snapshot, env = _materialized(upstream.base_url, sentinels)
            mounted = mount_hosted_header_channel(
                servers, derived_headers=snapshot, env=env, release=(env,)
            )
            relay = mounted[CONNECTOR]["instance"]
            assert isinstance(relay, HostedHeaderRelay)
            bridge = SdkMcpBridge(relay.name, relay)  # type: ignore[arg-type]
            replies: dict[str, Any] = {}

            async def write() -> None:
                replies["write"] = await bridge.handle(_record(3, write_value))

            try:
                with anyio.fail_after(_BRIDGE_TIMEOUT_S):
                    await bridge.handle(_initialize(1))
                    await bridge.handle(_INITIALIZED)
                    replies["list"] = await bridge.handle(
                        {"jsonrpc": "2.0", "id": 2, "method": "tools/list"}
                    )
                    ran = upstream.withhold(write_value)
                    async with anyio.create_task_group() as tasks:
                        tasks.start_soon(write)
                        await ran.wait()
                        upstream.restart()
                        replies["refused"] = await bridge.handle(_challenge(4))
                    replies["next"] = await bridge.handle(_challenge(5))
            finally:
                await bridge.aclose()
            return replies, list(upstream.committed), list(upstream.calls), upstream.challenge

    replies, committed, calls, challenge = anyio.run(scenario)
    assert committed == [write_value], "the write ran other than exactly once"
    written = replies["write"]
    assert "result" not in written, "the write was replayed after its answer was lost"
    assert written["error"]["code"] == CONNECTION_CLOSED
    assert written["error"]["message"] == f"hosted connector {CONNECTOR} is unavailable"
    for key in ("refused", "next"):
        assert replies[key]["result"]["content"][0]["text"] == challenge, key
    writes = [(call["id"], call["generation"]) for call in calls if call["tool"] == "record"]
    assert writes == [(3, 0)]
    # The refused call: once on the expired session, once on the replayed handshake's.
    challenges = [
        (call["id"], call["generation"], call["status"])
        for call in calls
        if call["tool"] == "challenge"
    ]
    assert challenges == [(4, 1, 404), (4, 1, 200), (5, 1, 200)]
    initializes = [
        (call["id"], call["generation"]) for call in calls if call["method"] == "initialize"
    ]
    assert initializes == [(1, 0), (f"{_REPLAY_PREFIX}1", 1)]
    assert all(call["authorized"] for call in calls)
    for surface, data in (("a bridge reply", json.dumps(replies)), ("the logs", caplog.text)):
        assert not _carries(data, sentinels.values()), f"{surface} carried a relay-held value"


# --- which entries are relayed --------------------------------------------------------------


def test_only_runner_materialized_headers_are_relayed_and_the_cli_config_names_the_server(
    tmp_path: Path,
) -> None:
    sentinels = _Sentinels.fresh()
    extra = {
        "remote": {
            "type": "http",
            "url": "https://remote.example.com/mcp",
            "headers": {"X-Acme-Remote": "${ACME_REMOTE_KEY}"},
        },
        "plain": {
            "type": "http",
            "url": "https://plain.example.com/mcp",
            "headers": {"X-Public": "acme-public"},
        },
        "local": {"command": sys.executable, "args": ["-c", "pass"]},
    }
    servers, snapshot, env = _materialized("http://127.0.0.1:9", sentinels, extra=extra)
    before = copy.deepcopy(servers)
    process_env = dict(env)
    mounted = mount_hosted_header_channel(
        servers, derived_headers=snapshot, env=env, release=(env, process_env)
    )
    assert servers == before, "mounting changed the entries the gate and probe read"
    entry = mounted[CONNECTOR]
    assert set(entry) == {"type", "name", "instance"}
    assert entry["type"] == "sdk" and entry["name"] == CONNECTOR
    assert isinstance(entry["instance"], HostedHeaderRelay)
    assert not _carries(repr(entry["instance"]), sentinels.values())
    assert hosted_header_values(mounted) == {
        f"Bearer {sentinels.bearer}",
        sentinels.bearer,
        sentinels.caller,
        sentinels.grant,
    }
    # The CLI keeps expanding ordinary remote ${VAR} headers; stdio is untouched.
    for name in extra:
        assert mounted[name] == extra[name]
    # The relay holds the grant now; no environment needs its name.
    assert _GRANT_ENV not in env and _GRANT_ENV not in process_env
    assert env["ACME_REMOTE_KEY"] == "acme-remote-public-value"

    transport = CredentialSafeCLITransport(
        prompt="",
        options=ClaudeAgentOptions(cli_path="/bin/true", mcp_servers=mounted),
    )
    try:
        command = transport._build_command()
        config = Path(command[command.index("--mcp-config") + 1])
        text = config.read_text(encoding="utf-8")
        written = json.loads(text)["mcpServers"]
        assert written[CONNECTOR] == {"type": "sdk", "name": CONNECTOR}
        assert written["remote"]["headers"] == {"X-Acme-Remote": "${ACME_REMOTE_KEY}"}
        assert written["plain"]["headers"] == {"X-Public": "acme-public"}
        assert not _carries(text, sentinels.values())
        assert not _carries("\0".join(command), sentinels.values())
    finally:
        anyio.run(transport.close)


def test_a_hosted_entry_carrying_the_model_grant_is_left_for_the_connect_refusal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    with installed(tmp_path, monkeypatch) as installation:
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
        caller_env = BootEnv.env_key("connector_caller_token")
        servers: dict[str, Any] = {
            CONNECTOR: {
                "type": "http",
                "url": "http://127.0.0.1:9/mcp",
                "headers": {
                    "Authorization": f"Bearer ${{{BEARER_ENV}}}",
                    CALLER_HEADER: f"${{{caller_env}}}",
                },
            }
        }
        snapshot = derived_header_placeholders(servers)
        env = {BEARER_ENV: OWNER_GRANT, caller_env: "cct.acme-public-caller.sig"}
        materialize_hosted_bearer_headers(servers, env)
        materialize_connector_caller_headers(servers, env)
        mounted = mount_hosted_header_channel(
            servers, derived_headers=snapshot, env={**os.environ, **env}
        )
        # Left as it was, so the session's own refusal still stops it.
        assert mounted[CONNECTOR] == servers[CONNECTOR]
        assert hosted_header_values(mounted) == frozenset()
        options = ClaudeAgentOptions(
            model=IMPLEMENTER_MODEL,
            cwd=str(installation.workspace),
            max_turns=1,
            permission_mode="bypassPermissions",
            strict_mcp_config=True,
            env={"ANTHROPIC_DEFAULT_OPUS_MODEL": REVIEWER_MODEL},
            mcp_servers=mounted,
        )

        async def scenario() -> None:
            session = ClaudeAgentSession(options)
            try:
                await session.connect()
            finally:
                await session.close()

        with pytest.raises(UnsafeStdioConfiguration) as refused:
            anyio.run(scenario)
        assert OWNER_GRANT not in str(refused.value)
        assert_cli_never_started(installation.proxy)
    unrelayed = [
        record
        for record in caplog.records
        if record.name == "curie_runner.mcp_header_channel" and CONNECTOR in record.getMessage()
    ]
    assert unrelayed, "the unrelayed connector was not reported"
    assert OWNER_GRANT not in caplog.text


def test_turn_start_reprobe_still_sends_the_grant_the_mount_took_from_the_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The re-probe dials the real entries, whose grant header names the env
    # value; the mount drops that name, so the boot hands the re-probe the value.
    for name in list(os.environ):
        if name.startswith("CURIE_"):
            monkeypatch.delenv(name)
    sentinels = _Sentinels.fresh()
    seen: list[dict[str, str]] = []

    async def reprobe(
        failures: tuple[ConnectorCapabilityFailure, ...],
        _servers: dict[str, Any],
        inherited_env: dict[str, str] | None = None,
    ) -> tuple[ConnectorCapabilityFailure, ...]:
        seen.append(dict(inherited_env or {}))
        return failures

    monkeypatch.setattr(boot, "reprobe_connector_failures", reprobe)
    bundle = _write_bundle(tmp_path / "bundle", scanner=False)
    config = _boot_env(monkeypatch, bundle, "http://127.0.0.1:9", sentinels, grant=True)
    failure = ConnectorCapabilityFailure(
        connector=CONNECTOR, credential_names=(), reason="probe_failed"
    )
    runner = build_runner(
        config,
        mcp_capability=McpToolCapabilityProbe(
            complete=True, has_potential_write_tool=False, tool_count=0
        ),
        connector_failures=(failure,),
    )
    assert _GRANT_ENV not in os.environ, "the grant's env name outlived the relay mount"
    assert anyio.run(runner._connector_reprobe, (failure,)) == (failure,)
    assert len(seen) == 1
    assert seen[0].get(_GRANT_ENV) == sentinels.grant
