"""Personal-token custody through the production SDK and native coding tools."""

from __future__ import annotations

import base64
import ctypes
import io
import json
import logging
import os
import shlex
import sys
from dataclasses import replace
from pathlib import Path

import anyio
import pytest
from aci_protocol import (
    Event,
    Final,
    OtelConfig,
    SideEffectFlag,
    TextDelta,
    parse_ndjson,
    to_ndjson_line,
)
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from claude_agent_sdk import ClaudeAgentOptions
from curie_runner.adapter import ClaudeAgentSession
from curie_runner.config import RunnerConfig
from curie_runner.hooks import _merge_pre_tool_use_hooks, load_bundle_hooks
from curie_runner.mcp_stdio import UnsafeStdioConfiguration
from curie_runner.mcp_tool_capability import server_streams
from curie_runner.otel import RunTracer, build_tracer_provider
from curie_runner.plugin import bundle_mcp_servers
from curie_runner.redact import OutboundRedactor, collect_held_secrets
from curie_runner.route_callers import caller_launch
from curie_runner.server import create_app
from curie_runner.session import SessionRunner
from curie_runner.side_effects import SideEffectClassifier
from curie_runner.startup_diagnostics import StderrTail
from curie_runner.subprocess_env import lock_process_environ, proc_dumpable_library
from curie_telemetry.redact import RedactingLogFilter
from personal_auth_support import (
    BUNDLED_CLI,
    IMPLEMENTER_MODEL,
    OWNER_GRANT,
    REVIEWER_MODEL,
    TURN_TIMEOUT_S,
    ErrorReply,
    TextReply,
    assert_owner_grant_only,
    installed,
    wire_session,
    write_json,
)
from personal_token_support import (
    CONNECTOR_VALUE,
    CONTAINMENT_CHANNELS,
    CONTAINMENT_PATHS,
    PUBLIC_MARKER,
    ContainmentResponder,
    assert_containment_capture,
    containment_tool,
    native_cli_pid,
    write_containment_bundle,
)


@pytest.fixture
def native_prerequisites() -> str:
    if not BUNDLED_CLI.is_file():
        raise RuntimeError("the required bundled native CLI is missing")
    library = proc_dumpable_library(os.environ)
    if not Path(library).is_file():
        raise RuntimeError("the required process-lock library is missing")
    if Path("/app/runner").is_dir():
        configured = os.environ.get("CURIE_PROC_DUMPABLE_PRELOAD", "")
        if configured != "/usr/local/lib/curie/libproc_dumpable.so":
            raise RuntimeError("candidate image must use its installed process-lock library")
        if Path(configured).stat().st_uid != 0:
            raise RuntimeError("candidate process-lock library is not root owned")
    return library


@pytest.mark.parametrize("channel", CONTAINMENT_CHANNELS)
@pytest.mark.parametrize("path", CONTAINMENT_PATHS)
def test_native_personal_token_containment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_prerequisites: str,
    channel: str,
    path: str,
) -> None:
    responder = ContainmentResponder()
    with installed(tmp_path, monkeypatch, responder) as installation:
        monkeypatch.setenv("CURIE_PROC_DUMPABLE_PRELOAD", native_prerequisites)
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        # The only sentinel delivery point is the actual personal-token env.
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
        bundle = write_containment_bundle(installation.workspace / "bundle")
        startup = bundle.root / "startup.sh"
        startup_capture = bundle.root / "startup.raw"
        startup_control = bundle.root / "startup.public"
        startup.write_text(
            'if [ -n "${CLAUDE_CODE_OAUTH_TOKEN-}" ]; then '
            f'printf "%s" "$CLAUDE_CODE_OAUTH_TOKEN" > {shlex.quote(str(startup_capture))}; fi\n'
            f"printf '%s' {shlex.quote(PUBLIC_MARKER)} > {shlex.quote(str(startup_control))}\n",
            encoding="utf-8",
        )
        wiring = wire_session(
            installation,
            reviewer_model=REVIEWER_MODEL,
            plugin_dir=str(bundle.root),
            max_turns=5,
            explicit_env={"BASH_ENV": str(startup)} if channel == "Bash" else None,
        )
        options = replace(
            wiring.options,
            hooks=_merge_pre_tool_use_hooks(
                wiring.options.hooks, load_bundle_hooks(str(bundle.root))
            ),
            mcp_servers=bundle_mcp_servers(str(bundle.root)),
            env={**wiring.options.env, "LD_PRELOAD": ""},
        )
        session = ClaudeAgentSession(options)
        runner = SessionRunner(
            held_secrets=frozenset({OWNER_GRANT}),
            session_factory=lambda: session,
            ceiling=0,
            max_usd_per_day=None,
            tracer=RunTracer(None),
            classifier=SideEffectClassifier(),
            trace_name="curie-run:personal-custody",
            session_id="session-PLACEHOLDER",
            model=options.model,
            tool_access=wiring.access,
        )

        async def scenario() -> list[str]:
            lines: list[str] = []
            try:
                with anyio.fail_after(TURN_TIMEOUT_S):
                    await runner.start()
                    responder.calls = containment_tool(
                        channel, path, bundle, native_cli_pid(session)
                    )
                    if channel == "Bash":
                        name, arguments = responder.calls[0]
                        responder.calls[0] = (
                            name,
                            {
                                **arguments,
                                "command": (
                                    f"source {shlex.quote(str(startup))}; " + arguments["command"]
                                ),
                            },
                        )
                    async for line in runner.run_turn(
                        Event(
                            type="message", text="Run the custody probe.", user="U0EXAMPLE1", ts="1"
                        )
                    ):
                        lines.append(line)
            finally:
                await runner.close()
            return lines

        libc = ctypes.CDLL(None, use_errno=True)
        prior_dumpable = libc.prctl(3, 0, 0, 0, 0)
        lock_process_environ()
        try:
            lines = anyio.run(scenario)
        finally:
            if libc.prctl(4, prior_dumpable, 0, 0, 0) != 0:
                raise OSError(ctypes.get_errno())
        finals = [event for event in parse_ndjson("".join(lines)) if isinstance(event, Final)]
        assert len(finals) == 1
        assert_owner_grant_only(installation.proxy)
        assert_containment_capture(
            channel, path, bundle, "\n".join(responder.raw_results), OWNER_GRANT
        )
        if channel == "Bash":
            assert not startup_capture.exists(), "shell startup recovered the synthetic grant"
            assert startup_control.read_text(encoding="utf-8") == PUBLIC_MARKER


_STDIO_SERVER = """\
import os
from pathlib import Path
from mcp.server.mcpserver import MCPServer
server = MCPServer("custody-stdio")
@server.tool()
def read_public() -> str:
    public = Path(os.environ["ACME_PUBLIC_FILE"]).read_text()
    return public + " " + os.environ["ACME_CONNECTOR_SECRET"]
server.run()
"""


def _stdio_entry(root: Path) -> dict[str, object]:
    public = root / "public.txt"
    public.write_text(PUBLIC_MARKER, encoding="utf-8")
    server = root / "stdio.py"
    server.write_text(_STDIO_SERVER, encoding="utf-8")
    return {
        "command": sys.executable,
        "args": [str(server)],
        "env": {
            "ACME_PUBLIC_FILE": str(public),
            "ACME_CONNECTOR_SECRET": "${ACME_CONNECTOR_SECRET}",
        },
    }


def _direct_options(workspace: Path, servers: object) -> ClaudeAgentOptions:
    return ClaudeAgentOptions(
        model=IMPLEMENTER_MODEL,
        cwd=str(workspace),
        max_turns=4,
        permission_mode="bypassPermissions",
        strict_mcp_config=True,
        env={"ANTHROPIC_DEFAULT_OPUS_MODEL": REVIEWER_MODEL},
        mcp_servers=servers,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize("source", ("bundle", "direct", "file"))
def test_stdio_configuration_is_scrubbed_at_actual_sdk_cli_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_prerequisites: str,
    source: str,
) -> None:
    responder = ContainmentResponder()
    with installed(tmp_path, monkeypatch, responder) as installation:
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
        monkeypatch.setenv("CURIE_PROC_DUMPABLE_PRELOAD", native_prerequisites)
        monkeypatch.setenv("ACME_CONNECTOR_SECRET", CONNECTOR_VALUE)
        entry = _stdio_entry(installation.workspace)
        key = "probe"
        if source == "bundle":
            root = installation.workspace / "bundle"
            write_json(
                root / ".claude-plugin" / "plugin.json",
                {
                    "name": "custody-stdio",
                    "version": "0.1.0",
                    "mcpServers": {"probe": entry},
                },
            )
            servers: object = bundle_mcp_servers(str(root))
            key = "plugin:custody-stdio:probe"
        elif source == "file":
            servers = write_json(
                installation.workspace / "servers.json", {"mcpServers": {key: entry}}
            )
        else:
            servers = {key: entry}
        options = _direct_options(installation.workspace, servers)
        session = ClaudeAgentSession(options)
        responder.calls = [(f"mcp__{key.replace(':', '_')}__read_public", {})]

        async def scenario() -> str:
            try:
                with anyio.fail_after(TURN_TIMEOUT_S):
                    await session.connect()
                    transport = session._client._transport  # noqa: SLF001
                    paths = transport._mcp_config_tmps  # noqa: SLF001
                    assert len(paths) == 1
                    serialized = Path(paths[0]).read_text(encoding="utf-8")
                    await session.query("Read the public marker through the configured server.")
                    async for _message in session.receive_turn():
                        pass
                    return serialized
            finally:
                await session.close()

        serialized = anyio.run(scenario)
        grant_in_config = OWNER_GRANT in serialized
        deferred_reference = "${" in serialized
        assert not grant_in_config, "SDK serialized a personal grant"
        assert not deferred_reference, "SDK serialized a deferred environment reference"
        mounted = json.loads(serialized)["mcpServers"][key]
        assert "env" in mounted
        assert "CLAUDE_CODE_OAUTH_TOKEN" not in mounted["env"]
        assert mounted["env"]["ACME_CONNECTOR_SECRET"] == CONNECTOR_VALUE
        assert options.strict_mcp_config is True
        raw = "\n".join(responder.raw_results)
        public_observed = PUBLIC_MARKER in raw
        connector_observed = CONNECTOR_VALUE in raw
        assert public_observed
        assert connector_observed
        assert_owner_grant_only(installation.proxy)


_UNSAFE_STDIO = (
    "env-name",
    "env-reference",
    "env-literal",
    "command-reference",
    "command-literal",
    "args-reference",
    "args-literal",
    "declared-model-target",
    "nested-reference",
)


def _unsafe_stdio(kind: str) -> dict[str, object]:
    entry: dict[str, object] = {"command": sys.executable, "args": ["-c", "print('public')"]}
    if kind == "env-name":
        entry["env"] = {"CLAUDE_CODE_OAUTH_TOKEN": "replacement"}
    elif kind == "env-reference":
        entry["env"] = {"ACME_INNOCENT": "${CLAUDE_CODE_OAUTH_TOKEN}"}
    elif kind == "env-literal":
        entry["env"] = {"ACME_INNOCENT": OWNER_GRANT}
    elif kind == "command-reference":
        entry["command"] = "${CLAUDE_CODE_OAUTH_TOKEN}"
    elif kind == "command-literal":
        entry["command"] = OWNER_GRANT
    elif kind == "args-reference":
        entry["args"] = ["${CLAUDE_CODE_OAUTH_TOKEN:-replacement}"]
    elif kind == "args-literal":
        entry["args"] = [OWNER_GRANT]
    elif kind == "declared-model-target":
        entry["env"] = {"CURIE_MODEL_ENV_KEY": "ACME_PROVIDER", "ACME_PROVIDER": "replacement"}
    elif kind == "nested-reference":
        entry["args"] = ["${ACME_MISSING:-${CLAUDE_CODE_OAUTH_TOKEN}}"]
    else:
        raise ValueError(kind)
    return entry


@pytest.mark.parametrize("consumer", ("bundle", "direct", "probe", "caller"))
@pytest.mark.parametrize("kind", _UNSAFE_STDIO)
def test_stdio_credential_reintroduction_is_refused_at_real_consumer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    consumer: str,
    kind: str,
) -> None:
    with installed(tmp_path, monkeypatch) as installation:
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
        entry = _unsafe_stdio(kind)
        with pytest.raises(UnsafeStdioConfiguration):
            if consumer == "bundle":
                root = installation.workspace / "bundle"
                write_json(
                    root / ".claude-plugin" / "plugin.json",
                    {
                        "name": "custody-stdio",
                        "version": "0.1.0",
                        "mcpServers": {"probe": entry},
                    },
                )
                bundle_mcp_servers(str(root))
            elif consumer == "caller":
                caller_launch(
                    entry, routes={}, source_env=os.environ, lock_library="unused", cwd=None
                )
            else:

                async def scenario() -> None:
                    if consumer == "probe":
                        async with server_streams(entry, plugin_dir=None, inherited_env=os.environ):
                            raise AssertionError("unsafe stdio transport opened")
                    else:
                        session = ClaudeAgentSession(
                            _direct_options(installation.workspace, {"probe": entry})
                        )
                        try:
                            await session.connect()
                        finally:
                            await session.close()

                anyio.run(scenario)
        assert installation.proxy.all_requests() == []


def test_stdio_sdk_env_cannot_hide_an_inherited_model_credential_declaration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with installed(tmp_path, monkeypatch) as installation:
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
        monkeypatch.setenv("CURIE_MODEL_ENV_KEY", "ACME_PROVIDER")
        monkeypatch.setenv("ACME_PROVIDER", "model-control-synthetic")
        options = _direct_options(
            installation.workspace,
            {
                "probe": {
                    "command": sys.executable,
                    "args": ["${ACME_PROVIDER}"],
                }
            },
        )
        options.env["CURIE_MODEL_ENV_KEY"] = ""

        async def scenario() -> None:
            session = ClaudeAgentSession(options)
            try:
                await session.connect()
            finally:
                await session.close()

        with pytest.raises(UnsafeStdioConfiguration):
            anyio.run(scenario)
        assert installation.proxy.all_requests() == []


_OPAQUE_GRANT = "custody-opaque-synthetic-value-000000???"


def _held_grant() -> frozenset[str]:
    config = RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": "/bundle",
            "CURIE_SESSION_ID": "session-PLACEHOLDER",
            "CURIE_SANDBOX_ID": "sandbox-PLACEHOLDER",
            "CURIE_BUDGET": '{"max_output_tokens_per_run":10000,"max_usd_per_day":1.0}',
        }
    )
    return collect_held_secrets(
        config,
        environments=({"CLAUDE_CODE_OAUTH_TOKEN": _OPAQUE_GRANT},),
        credential_names=(),
        connector_names=(),
        server_groups=(),
    )


@pytest.mark.parametrize(
    "encoding",
    (
        "literal",
        "base64",
        "base64-unpadded",
        "urlsafe",
        "urlsafe-unpadded",
    ),
)
def test_redaction_scrubs_actual_stderr_passthrough_logs_and_publication_serialization(
    encoding: str,
) -> None:
    held = _held_grant()
    value = _OPAQUE_GRANT
    if encoding.startswith("base64"):
        value = base64.b64encode(value.encode()).decode()
    elif encoding.startswith("urlsafe"):
        value = base64.urlsafe_b64encode(value.encode()).decode()
    if encoding.endswith("-unpadded"):
        value = value.rstrip("=")
    destination: list[str] = []
    tail = StderrTail(destination.append, partial_line_chars=1024)
    tail.start()
    tail(PUBLIC_MARKER + " " + value[:11])
    tail(value[11:])
    tail(" " + PUBLIC_MARKER)
    retained = tail.finish()
    raw_passthrough = "\n".join(destination)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.addFilter(RedactingLogFilter())
    logger = logging.getLogger("curie_runner.custody-redaction")
    prior_level = logger.level
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        logger.info("%s %s", PUBLIC_MARKER, value)
        try:
            raise ValueError(value)
        except ValueError:
            logger.exception(PUBLIC_MARKER)
    finally:
        logger.removeHandler(handler)
        logger.setLevel(prior_level)
    redactor = OutboundRedactor(held)
    publication: list[str] = []
    for part in (value[:11], value[11:]):
        publication.extend(redactor.push(to_ndjson_line(TextDelta(text=part))))
    publication.extend(
        redactor.push(
            to_ndjson_line(
                SideEffectFlag(
                    tool="mcp__curie__publish_workspace",
                    result={"publication": value, "public": PUBLIC_MARKER},
                    failed=False,
                )
            )
        )
    )
    publication.extend(redactor.push(to_ndjson_line(Final(text=PUBLIC_MARKER + " " + value))))
    results = [
        event for event in parse_ndjson("".join(publication)) if isinstance(event, SideEffectFlag)
    ]
    assert len(results) == 1
    assert results[0].redacted is True
    assert results[0].result == {
        "publication": "[REDACTED:held_secret]",
        "public": PUBLIC_MARKER,
    }
    for captured in (raw_passthrough, retained, stream.getvalue(), "".join(publication)):
        encoded_exposed = value in captured
        grant_exposed = _OPAQUE_GRANT in captured
        public_observed = PUBLIC_MARKER in captured
        placeholder_observed = "[REDACTED:held_secret]" in captured
        assert not encoded_exposed
        assert not grant_exposed
        assert public_observed
        assert placeholder_observed


@pytest.mark.parametrize("scenario", ("echo", "chunked", "error"))
def test_redaction_through_native_cli_http_aci_and_exported_spans(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    native_prerequisites: str,
    scenario: str,
) -> None:
    value = PUBLIC_MARKER + " " + _OPAQUE_GRANT
    control_complete = False

    def respond(_request: object) -> TextReply | ErrorReply:
        if scenario == "error" and control_complete:
            return ErrorReply(400, "invalid_request_error", value)
        chunks = (
            (PUBLIC_MARKER + " ", _OPAQUE_GRANT[:11], _OPAQUE_GRANT[11:])
            if scenario == "chunked"
            else None
        )
        return TextReply(value, chunks=chunks)

    with installed(tmp_path, monkeypatch, respond) as installation:
        monkeypatch.setenv("NO_PROXY", "127.0.0.1,localhost")
        monkeypatch.setenv("no_proxy", "127.0.0.1,localhost")
        monkeypatch.setenv("CURIE_MODEL_AUTH_MODE", "personal")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", _OPAQUE_GRANT)
        monkeypatch.setenv("CURIE_PROC_DUMPABLE_PRELOAD", native_prerequisites)
        held = _held_grant()
        wiring = wire_session(installation, reviewer_model=REVIEWER_MODEL)
        stderr: list[str] = []
        options = replace(wiring.options, stderr=stderr.append)

        async def run() -> tuple[str, bytes]:
            nonlocal control_complete
            exported: list[bytes] = []

            async def traces(request: web.Request) -> web.Response:
                exported.append(await request.read())
                return web.Response(body=b"", content_type="application/x-protobuf")

            app = web.Application()
            app.router.add_post("/v1/traces", traces)
            async with TestServer(app) as collector:
                provider = build_tracer_provider(
                    OtelConfig(endpoint=str(collector.make_url("/")), protocol="http/protobuf"),
                    "session-PLACEHOLDER",
                )
                assert provider is not None
                runner = SessionRunner(
                    held_secrets=held,
                    session_factory=lambda: ClaudeAgentSession(options),
                    ceiling=0,
                    max_usd_per_day=None,
                    tracer=RunTracer(provider),
                    classifier=SideEffectClassifier(),
                    trace_name="curie-run:personal-redaction",
                    session_id="session-PLACEHOLDER",
                    model=options.model,
                    tool_access=wiring.access,
                )
                async with TestClient(TestServer(create_app(runner))) as client:
                    control_output = ""
                    if scenario == "error":
                        control = await client.post(
                            "/v1/event",
                            json=Event(
                                type="message",
                                text="Run the public export control.",
                                user="U0EXAMPLE1",
                                ts="0",
                            ).model_dump(mode="json"),
                        )
                        assert control.status == 200
                        control_output = await control.text()
                        control_complete = True
                    response = await client.post(
                        "/v1/event",
                        json=Event(
                            type="message",
                            text="Run the echo probe.",
                            user="U0EXAMPLE1",
                            ts="1",
                        ).model_dump(mode="json"),
                    )
                    assert response.status == 200
                    result = await response.text()
                    if scenario == "error":
                        assert any(event.type == "error" for event in parse_ndjson(result))
                    raw = control_output + result
                    assert await anyio.to_thread.run_sync(provider.force_flush)
                return raw, b"".join(exported)

        raw, exported = anyio.run(run)
        assert parse_ndjson(raw)
        raw_exposed = _OPAQUE_GRANT in raw
        stderr_exposed = _OPAQUE_GRANT in "\n".join(stderr)
        export_exposed = _OPAQUE_GRANT.encode() in exported
        public_reply_observed = PUBLIC_MARKER in raw
        public_export_observed = PUBLIC_MARKER.encode() in exported
        redacted_export_observed = b"[REDACTED:held_secret]" in exported
        assert not raw_exposed
        assert not stderr_exposed
        assert not export_exposed
        assert public_reply_observed
        assert public_export_observed
        assert redacted_export_observed


@pytest.mark.parametrize("boundary", ("finish", "discard"))
def test_redaction_keeps_split_stderr_protected_after_initialization(boundary: str) -> None:
    _held_grant()
    forwarded: list[str] = []
    tail = StderrTail(forwarded.append, partial_line_chars=1024)
    tail.start()
    tail(PUBLIC_MARKER + " " + _OPAQUE_GRANT[:11])
    getattr(tail, boundary)()
    tail(_OPAQUE_GRANT[11:])
    tail(" " + PUBLIC_MARKER)
    raw = "\n".join(forwarded)
    grant_exposed = _OPAQUE_GRANT in raw
    suffix_exposed = _OPAQUE_GRANT[11:] in raw
    public_observed = PUBLIC_MARKER in raw
    assert not grant_exposed
    assert not suffix_exposed
    assert public_observed
