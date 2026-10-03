"""MCP config JSON must not appear on the claude CLI argv (#2635).

Sentinels are fixtures. ``joined`` is the command line as ``/proc/pid/cmdline``.
"""

from __future__ import annotations

import json
import os
import select
import shlex
import tempfile
import time
from pathlib import Path
from typing import Any

import anyio
import pytest
from claude_agent_sdk import ClaudeAgentOptions

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
    from curie_runner.mcp_argv import CredentialSafeCLITransport

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


def _write_closed_executable(path: Path, body: str) -> None:
    """Write ``path``, close it, and only then make it executable."""

    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        payload = body.encode("utf-8")
        written = 0
        while written < len(payload):
            written += os.write(fd, payload[written:])
        os.fsync(fd)
    finally:
        os.close(fd)
    os.chmod(path, 0o755)


def _read_ready_pid(ready: Path) -> str:
    """Block until the child writes its pid, or 10 seconds pass."""

    fd = os.open(ready, os.O_RDONLY | os.O_NONBLOCK)
    try:
        deadline = time.monotonic() + 10
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError("spawned process never became ready")
            try:
                readable, _, _ = select.select([fd], [], [], remaining)
            except InterruptedError:
                continue
            if not readable:
                continue
            data = os.read(fd, 64)
            if data:
                return data.decode("utf-8")
    finally:
        os.close(fd)


def _release_child(release: Path) -> None:
    """Unblock a child waiting on ``release``. No reader means it already exited."""

    try:
        fd = os.open(release, os.O_WRONLY | os.O_NONBLOCK)
    except OSError:
        return
    try:
        os.write(fd, b"go\n")
    finally:
        os.close(fd)


def test_spawned_process_cmdline_omits_hosted_bearer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # AC2 spawn observation: read /proc/<pid>/cmdline after connect() (#2635).
    # The child blocks on ``release`` until that read finishes. Holding stdin
    # open is not enough: the transport closes stdin when it finishes, and the
    # process is gone before /proc can be read.
    import claude_agent_sdk._internal.transport.subprocess_cli as subprocess_cli
    import curie_runner.adapter  # noqa: F401

    ready = tmp_path / "ready"
    release = tmp_path / "release"
    os.mkfifo(ready)
    os.mkfifo(release)
    fake = tmp_path / "claude"
    quoted_ready = shlex.quote(str(ready))
    quoted_release = shlex.quote(str(release))
    _write_closed_executable(
        fake,
        f"#!/bin/sh\necho $$ > {quoted_ready}\nread -r _ < {quoted_release}\n",
    )
    monkeypatch.setenv("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK", "1")
    transport = subprocess_cli.SubprocessCLITransport(
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
            reported = await anyio.to_thread.run_sync(_read_ready_pid, ready)
            pid = int(reported.strip())
            assert transport._process is not None
            assert transport._process.pid == pid
            cmdline = Path(f"/proc/{pid}/cmdline").read_bytes()
            assert cmdline, "spawned process cmdline was empty"
            assert b"ghp_sentinel" not in cmdline
            args = [a for a in cmdline.split(b"\0") if a]
            config_path = Path(args[args.index(b"--mcp-config") + 1].decode())
            assert config_path.is_file()
            assert config_path.stat().st_mode & 0o777 == 0o600
            assert "ghp_sentinel" in config_path.read_text(encoding="utf-8")
        finally:
            await anyio.to_thread.run_sync(_release_child, release)
            await transport.close()

    anyio.run(_observe)


def test_custom_header_absent_from_cli_argv() -> None:
    _assert_sentinel_off_argv({"X-Api-Key": "custom_sentinel_2635"}, "custom_sentinel_2635")


def test_basic_auth_absent_from_cli_argv() -> None:
    sentinel = "dXNlcjpzZW50aW5lbA=="
    _assert_sentinel_off_argv({"Authorization": f"Basic {sentinel}"}, sentinel)


def test_mcp_config_path_argument_is_left_unchanged(tmp_path: Path) -> None:
    from curie_runner.mcp_argv import offload_mcp_config_argv

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
    from curie_runner.mcp_argv import CredentialSafeCLITransport

    assert subprocess_cli.SubprocessCLITransport is CredentialSafeCLITransport


def test_offload_helper_rejects_sentinel_on_argv() -> None:
    from curie_runner.mcp_argv import offload_mcp_config_argv

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
