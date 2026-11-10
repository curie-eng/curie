"""Only the MCP servers Curie mounts may load in a session (#2899).

The runner sets ``strict_mcp_config``, so the CLI ignores the cwd's project
``.mcp.json``, user settings and marketplace plugin servers. Strict mode also
drops ``--plugin-dir`` servers, so the bundle's own servers are mounted by name
through ``plugin.bundle_mcp_servers`` and must keep the live tool names toolPolicy
already classifies. The real-loader cases drive the SDK's bundled CLI with an
isolated HOME and no credential: ``connect()`` initialises MCP before any model
call.
"""

from __future__ import annotations

import dataclasses
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any

import anyio
import claude_agent_sdk
import pytest
from claude_agent_sdk import ClaudeSDKClient
from curie_runner.adapter import build_options
from curie_runner.check import run_check
from curie_runner.plugin import bundle_mcp_servers, load_plugins
from plugin_format.approval_policy import effective_tool_prefix

_BUNDLED_CLI = Path(claude_agent_sdk.__file__).parent / "_bundled" / "claude"
_CLI_AVAILABLE = _BUNDLED_CLI.is_file() or shutil.which("claude") is not None

# A one-tool stdio server on the interpreter running the tests, so the real CLI
# can connect it without relying on the host's `python3` having `mcp` installed.
_SERVER = """\
from mcp.server.mcpserver import MCPServer
server = MCPServer("probe")
@server.tool()
def ping() -> str:
    return "pong"
server.run()
"""


def _options(**overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "plugins": [],
        "model": None,
        "system_prompt": None,
        "max_turns": 1,
        "max_budget_usd": None,
        "resume": None,
    }
    kwargs.update(overrides)
    return build_options(**kwargs)


def test_build_options_sets_strict_mcp_config() -> None:
    assert _options().strict_mcp_config is True
    assert _options(mcp_servers={"x": {"command": "true"}}).strict_mcp_config is True


def _bundle(root: Path, *, inline: dict[str, Any] | None, root_mcp: dict[str, Any] | None) -> Path:
    (root / ".claude-plugin").mkdir(parents=True)
    manifest: dict[str, Any] = {"name": "probe-bundle", "version": "0.1.0"}
    if inline is not None:
        manifest["mcpServers"] = inline
    (root / ".claude-plugin" / "plugin.json").write_text(json.dumps(manifest))
    if root_mcp is not None:
        (root / ".mcp.json").write_text(json.dumps({"mcpServers": root_mcp}))
    return root


def test_bundle_mcp_servers_keys_both_declaration_surfaces_as_plugin_servers(
    tmp_path: Path,
) -> None:
    root = _bundle(
        tmp_path / "b",
        inline={"alpha": {"command": "node", "args": ["${CLAUDE_PLUGIN_ROOT}/a.js", "${TOKEN}"]}},
        root_mcp={
            "beta": {
                "type": "http",
                "url": "${BETA_URL}",
                "headers": {"X": "${CLAUDE_PLUGIN_ROOT}"},
            }
        },
    )
    servers = bundle_mcp_servers(str(root))
    assert servers == {
        "plugin:probe-bundle:alpha": {
            "command": "node",
            # The plugin root is the loader-supplied variable the --mcp-config
            # path lacks; every other ${VAR} is left for the CLI to expand.
            "args": [f"{root}/a.js", "${TOKEN}"],
            "env": {"CLAUDE_PLUGIN_ROOT": str(root)},
        },
        "plugin:probe-bundle:beta": {
            "type": "http",
            "url": "${BETA_URL}",
            "headers": {"X": str(root)},
        },
    }
    # The CLI normalizes a server key to its live prefix by replacing every
    # character outside [A-Za-z0-9_-] with "_"; that must land on the prefix
    # toolPolicy and the approval gates were written against.
    for key, server in (
        ("plugin:probe-bundle:alpha", "alpha"),
        ("plugin:probe-bundle:beta", "beta"),
    ):
        live = f"mcp__{re.sub(r'[^A-Za-z0-9_-]', '_', key)}__"
        assert live == effective_tool_prefix("probe-bundle", server)


def test_bundle_mcp_servers_is_empty_without_a_bundle(tmp_path: Path) -> None:
    assert bundle_mcp_servers(None) == {}
    assert bundle_mcp_servers(str(_bundle(tmp_path / "b", inline=None, root_mcp=None))) == {}


@pytest.mark.parametrize("surface", ["inline", "root"])
@pytest.mark.parametrize("binding", [None, "", "bound-value"])
def test_optional_secret_projection_preserves_other_references_and_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, surface: str, binding: str | None
) -> None:
    monkeypatch.delenv("OPTIONAL_TOKEN", raising=False)
    if binding is not None:
        monkeypatch.setenv("OPTIONAL_TOKEN", binding)
    config = {
        "stdio": {
            "command": "node",
            "args": ["${OPTIONAL_TOKEN}"],
            "env": {
                "TOKEN": "${OPTIONAL_TOKEN}",
                "EMBEDDED": "prefix-${OPTIONAL_TOKEN}",
                "REQUIRED": "${REQUIRED_TOKEN}",
                "UNDECLARED": "${OTHER_TOKEN}",
            },
        },
        "http": {
            "type": "http",
            "url": "https://example.com/${OPTIONAL_TOKEN}",
            "headers": {"Authorization": "Bearer ${OPTIONAL_TOKEN}"},
        },
    }
    root = _bundle(
        tmp_path / "bundle",
        inline=config if surface == "inline" else None,
        root_mcp=config if surface == "root" else None,
    )
    manifest_path = root / ".claude-plugin" / "plugin.json"
    manifest = json.loads(manifest_path.read_text())
    manifest.update(secrets=["REQUIRED_TOKEN"], optionalSecrets=["OPTIONAL_TOKEN"])
    manifest_path.write_text(json.dumps(manifest))
    artifacts = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}

    servers = bundle_mcp_servers(str(root))

    stdio = servers["plugin:probe-bundle:stdio"]
    expected_env = dict(config["stdio"]["env"])
    if binding is None:
        del expected_env["TOKEN"]
    expected_env["CLAUDE_PLUGIN_ROOT"] = str(root)
    assert stdio["env"] == expected_env
    assert stdio["args"] == ["${OPTIONAL_TOKEN}"]
    assert servers["plugin:probe-bundle:http"] == config["http"]
    assert {p: p.read_bytes() for p in artifacts} == artifacts


def _write_server(tmp_path: Path) -> Path:
    script = tmp_path / "server.py"
    script.write_text(_SERVER)
    return script


def _stdio(script: Path) -> dict[str, Any]:
    return {"command": sys.executable, "args": [str(script)]}


async def _registered(options: Any) -> list[dict[str, Any]]:
    async with ClaudeSDKClient(options) as client:
        with anyio.fail_after(60):
            while True:
                status = await client.get_mcp_status()
                servers = [dict(s) for s in status.get("mcpServers", [])]
                if servers and all(s.get("status") != "pending" for s in servers):
                    return servers
                await anyio.sleep(0.5)


@pytest.fixture
def isolated_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    return home


def _session_under_test(tmp_path: Path) -> tuple[Path, Path]:
    """A bundle with one server, and a mounted workspace carrying its own .mcp.json."""

    script = _write_server(tmp_path)
    plugin_dir = _bundle(tmp_path / "bundle", inline={"own": _stdio(script)}, root_mcp=None)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / ".mcp.json").write_text(json.dumps({"mcpServers": {"ambient": _stdio(script)}}))
    return plugin_dir, workspace


@pytest.mark.skipif(not _CLI_AVAILABLE, reason="requires the Claude Code CLI the SDK spawns")
@pytest.mark.parametrize("optional,binding", [(False, None), (True, None), (True, "bound-value")])
def test_real_loader_optional_secret_child_environment(
    tmp_path: Path,
    isolated_home: Path,
    monkeypatch: pytest.MonkeyPatch,
    optional: bool,
    binding: str | None,
) -> None:
    monkeypatch.delenv("OPTIONAL_TOKEN", raising=False)
    monkeypatch.delenv("CHILD_TOKEN", raising=False)
    if binding is not None:
        monkeypatch.setenv("OPTIONAL_TOKEN", binding)
    observed = tmp_path / "observed.json"
    script = tmp_path / "server.py"
    script.write_text(
        "import os, json\nfrom pathlib import Path\n"
        f"Path({str(observed)!r}).write_text(json.dumps("
        '{"present": "CHILD_TOKEN" in os.environ, "value": os.environ.get("CHILD_TOKEN")}))\n'
        + _SERVER
    )
    root = _bundle(
        tmp_path / "bundle",
        inline={"own": {**_stdio(script), "env": {"CHILD_TOKEN": "${OPTIONAL_TOKEN}"}}},
        root_mcp=None,
    )
    manifest_path = root / ".claude-plugin" / "plugin.json"
    if optional:
        manifest = json.loads(manifest_path.read_text())
        manifest["optionalSecrets"] = ["OPTIONAL_TOKEN"]
        manifest_path.write_text(json.dumps(manifest))
    before = manifest_path.read_bytes()
    registered = anyio.run(
        _registered,
        _options(mcp_servers=bundle_mcp_servers(str(root)), cwd=str(tmp_path)),
    )
    own = next(s for s in registered if s["name"] == "plugin:probe-bundle:own")
    assert own["status"] == "connected", own
    # Observed through the real bundled Claude Code loader: an unset variable
    # is delivered as the literal placeholder. This control must not be inferred
    # from our projection; the child process records its actual environment.
    expected = (
        {"present": False, "value": None}
        if optional and binding is None
        else {"present": True, "value": binding if binding is not None else "${OPTIONAL_TOKEN}"}
    )
    assert json.loads(observed.read_text()) == expected
    assert manifest_path.read_bytes() == before


@pytest.mark.skipif(not _CLI_AVAILABLE, reason="requires the Claude Code CLI the SDK spawns")
def test_ambient_workspace_mcp_json_does_not_load_but_the_bundle_server_does(
    tmp_path: Path, isolated_home: Path
) -> None:
    plugin_dir, workspace = _session_under_test(tmp_path)
    options = _options(
        plugins=load_plugins(str(plugin_dir)),
        mcp_servers=bundle_mcp_servers(str(plugin_dir)),
        cwd=str(workspace),
    )

    registered = anyio.run(_registered, options)
    by_name = {s["name"]: s for s in registered}

    assert "ambient" not in by_name, registered
    own = by_name["plugin:probe-bundle:own"]
    assert own["status"] == "connected", own
    assert [tool["name"] for tool in own["tools"]] == ["ping"]


@pytest.mark.skipif(not _CLI_AVAILABLE, reason="requires the Claude Code CLI the SDK spawns")
def test_negative_control_without_strict_the_workspace_server_loads(
    tmp_path: Path, isolated_home: Path
) -> None:
    # Proves the fixture above is capable of failing: the same workspace, with
    # only strict mode turned back off, registers the ambient server.
    plugin_dir, workspace = _session_under_test(tmp_path)
    options = dataclasses.replace(
        _options(plugins=load_plugins(str(plugin_dir)), cwd=str(workspace)),
        strict_mcp_config=False,
    )

    names = {s["name"] for s in anyio.run(_registered, options)}

    assert "ambient" in names, names


@pytest.mark.skipif(not _CLI_AVAILABLE, reason="requires the Claude Code CLI the SDK spawns")
def test_skill_check_stays_green_under_strict_mode(tmp_path: Path, isolated_home: Path) -> None:
    # `curie skill check` runs the same build_options; strict mode must not turn
    # a working bundle red by dropping its plugin-loaded server.
    plugin_dir, _ = _session_under_test(tmp_path)

    result = anyio.run(run_check, str(plugin_dir))

    assert result["verdict"] == "green", result
    assert [row["registered"] for row in result["matches"]] == ["plugin:probe-bundle:own"]
