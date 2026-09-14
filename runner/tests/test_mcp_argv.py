"""MCP config JSON must not appear on the claude CLI argv (#2635).

Sentinels are fixtures. ``joined`` is the command line as ``/proc/pid/cmdline``.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import anyio
import pytest
from claude_agent_sdk import ClaudeAgentOptions
from curie_runner.mcp_argv import CredentialSafeCLITransport, offload_mcp_config_argv

_TMP_PREFIX = "curie-mcp-config-"


def _http_servers(headers: dict[str, str]) -> dict[str, Any]:
    return {
        "github": {
            "type": "http",
            "url": "https://example.com/mcp",
            "headers": headers,
        }
    }


def _assert_sentinel_off_argv(headers: dict[str, str], sentinel: str) -> None:
    transport = CredentialSafeCLITransport(
        prompt="",
        options=ClaudeAgentOptions(cli_path="/bin/true", mcp_servers=_http_servers(headers)),
    )
    try:
        cmd = transport._build_command()
        joined = "\0".join(cmd)
        assert sentinel not in joined
        assert "--mcp-config" in cmd
        path = Path(cmd[cmd.index("--mcp-config") + 1])
        assert path.is_file()
        assert path.stat().st_mode & 0o777 == 0o600
        assert sentinel in path.read_text(encoding="utf-8")
    finally:
        anyio.run(transport.close)


def test_hosted_bearer_absent_from_cli_argv() -> None:
    _assert_sentinel_off_argv({"Authorization": "Bearer ghp_sentinel"}, "ghp_sentinel")


def test_spawned_process_cmdline_omits_hosted_bearer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # AC2 spawn observation: read /proc/<pid>/cmdline after connect() (#2635).
    fake = tmp_path / "claude"
    fake.write_text("#!/usr/bin/env python3\nimport sys\nsys.stdin.read()\n", encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK", "1")
    transport = CredentialSafeCLITransport(
        prompt="",
        options=ClaudeAgentOptions(
            cli_path=str(fake),
            mcp_servers={
                "github": {
                    "type": "http",
                    "url": "https://example.com/mcp",
                    "headers": {"Authorization": "Bearer ghp_sentinel"},
                }
            },
        ),
    )

    async def _observe() -> None:
        try:
            await transport.connect()
            pid = transport._process.pid
            cmdline = Path(f"/proc/{pid}/cmdline").read_bytes()
            assert b"ghp_sentinel" not in cmdline
            args = cmdline.split(b"\0")
            config_path = Path(args[args.index(b"--mcp-config") + 1].decode())
            assert config_path.is_file()
            assert config_path.stat().st_mode & 0o777 == 0o600
            assert "ghp_sentinel" in config_path.read_text(encoding="utf-8")
        finally:
            await transport.close()

    anyio.run(_observe)


def test_custom_header_absent_from_cli_argv() -> None:
    _assert_sentinel_off_argv({"X-Api-Key": "custom_sentinel_2635"}, "custom_sentinel_2635")


def test_basic_auth_absent_from_cli_argv() -> None:
    sentinel = "dXNlcjpzZW50aW5lbA=="
    _assert_sentinel_off_argv({"Authorization": f"Basic {sentinel}"}, sentinel)


def test_mcp_config_path_argument_is_left_unchanged(tmp_path: Path) -> None:
    existing = tmp_path / "already.conf"
    existing.write_text("not-json-contents", encoding="utf-8")
    cmd = ["/bin/true", "--mcp-config", str(existing)]
    before = set(Path(tempfile.gettempdir()).glob(f"{_TMP_PREFIX}*"))
    result = offload_mcp_config_argv(list(cmd))
    after = set(Path(tempfile.gettempdir()).glob(f"{_TMP_PREFIX}*"))
    assert result == cmd
    assert after == before


def test_adapter_import_installs_the_transport_subclass() -> None:
    # check.py imports adapter.build_options then constructs ClaudeSDKClient (#2635).
    import claude_agent_sdk._internal.transport.subprocess_cli as subprocess_cli
    import curie_runner.adapter  # noqa: F401

    assert subprocess_cli.SubprocessCLITransport is CredentialSafeCLITransport


def test_offload_helper_rejects_sentinel_on_argv() -> None:
    payload = json.dumps(
        {"mcpServers": {"github": {"headers": {"Authorization": "Bearer ghp_sentinel"}}}}
    )
    result = offload_mcp_config_argv(["claude", "--mcp-config", payload])
    rewritten = Path(result[result.index("--mcp-config") + 1])
    try:
        assert "ghp_sentinel" not in "\0".join(result)
    finally:
        if rewritten.is_file():
            rewritten.unlink()
