"""MCP config JSON must not appear on the claude CLI argv (#2635).

Sentinels are fixtures. ``joined`` is the command line as ``/proc/pid/cmdline``.
A hosted header the runner materialized rides an in-process relay (#5378), so it
is absent from the config file too; ordinary headers stay in the file, which the
CLI reads.
"""

from __future__ import annotations

import json
import os
import select
import shlex
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

import anyio
import pytest
from claude_agent_sdk import ClaudeAgentOptions

_TMP_PREFIX = "curie-mcp-config-"
# What derive_mcp_servers writes for a hosted connector before the runner
# materializes the value in memory.
_HOSTED_PLACEHOLDER = {"Authorization": "Bearer ${GITHUB_PERSONAL_ACCESS_TOKEN}"}


def _http_servers(headers: dict[str, str]) -> dict[str, Any]:
    return {
        "github": {
            "type": "http",
            "url": "https://example.com/mcp",
            "headers": headers,
        }
    }


def _relayed_servers(headers: dict[str, str]) -> dict[str, Any]:
    """The CLI-facing map once the hosted-header channel mounted ``github``."""

    from curie_runner.mcp_header_channel import mount_hosted_header_channel

    mounted = mount_hosted_header_channel(
        _http_servers(headers), derived_headers={"github": _HOSTED_PLACEHOLDER}, env={}
    )
    assert mounted["github"]["type"] == "sdk"
    return mounted


def _config_files() -> set[Path]:
    return set(Path(tempfile.gettempdir()).glob(f"{_TMP_PREFIX}*"))


def _assert_sentinel_off_argv(
    headers: dict[str, str], sentinel: str, *, relayed: bool = False
) -> None:
    from curie_runner.mcp_argv import CredentialSafeCLITransport

    servers = _relayed_servers(headers) if relayed else _http_servers(headers)
    transport = CredentialSafeCLITransport(
        prompt="",
        options=ClaudeAgentOptions(cli_path="/bin/true", mcp_servers=servers),
    )
    try:
        cmd = transport._build_command()
        joined = "\0".join(cmd)
        assert sentinel not in joined
        assert "--mcp-config" in cmd
        path = Path(cmd[cmd.index("--mcp-config") + 1])
        assert path.is_file()
        assert path.stat().st_mode & 0o777 == 0o600
        written = path.read_text(encoding="utf-8")
        if relayed:
            # #5378: same-uid Bash can read this file; a relayed header is not in it.
            assert sentinel not in written
            assert json.loads(written)["mcpServers"]["github"] == {"type": "sdk", "name": "github"}
        else:
            assert sentinel in written
    finally:
        anyio.run(transport.close)


def test_hosted_bearer_absent_from_cli_argv() -> None:
    _assert_sentinel_off_argv(
        {"Authorization": "Bearer ghp_sentinel"}, "ghp_sentinel", relayed=True
    )


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
            mcp_servers=_relayed_servers({"Authorization": "Bearer ghp_sentinel"}),
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
            # #5378: the spawned CLI's config names the relay, not the header.
            written = config_path.read_text(encoding="utf-8")
            assert "ghp_sentinel" not in written
            assert json.loads(written)["mcpServers"]["github"] == {"type": "sdk", "name": "github"}
        finally:
            await anyio.to_thread.run_sync(_release_child, release)
            await transport.close()

    anyio.run(_observe)


def test_custom_header_absent_from_cli_argv() -> None:
    _assert_sentinel_off_argv({"X-Api-Key": "custom_sentinel_2635"}, "custom_sentinel_2635")


def test_cli_process_group_launcher_does_not_import_workspace_startup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from curie_runner.mcp_argv import CredentialSafeCLITransport

    marker = tmp_path / "startup-loaded"
    (tmp_path / "sitecustomize.py").write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('loaded')\n"
    )
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    monkeypatch.setenv("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK", "1")
    transport = CredentialSafeCLITransport(
        "", ClaudeAgentOptions(cli_path="/bin/true", cwd=str(tmp_path))
    )

    async def go() -> None:
        await transport.connect()
        try:
            assert transport._process is not None
            await transport._process.wait()
        finally:
            await transport.close()

    anyio.run(go)
    assert not marker.exists(), "the CLI launcher imported workspace-controlled startup code"


def test_forced_stop_kills_the_cli_process_group(tmp_path: Path, monkeypatch) -> None:
    from curie_runner.mcp_argv import CredentialSafeCLITransport
    from curie_test_support.static_shell import wait_gone, wait_until

    ready = tmp_path / "processes"
    executable = tmp_path / "cli"
    _write_closed_executable(
        executable,
        f"#!{sys.executable}\n"
        "import os, subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(600)'])\n"
        f"open({str(ready)!r}, 'w').write(str(child.pid))\n"
        "time.sleep(600)\n",
    )
    monkeypatch.setenv("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK", "1")
    transport = CredentialSafeCLITransport("", ClaudeAgentOptions(cli_path=str(executable)))

    async def go() -> None:
        await transport.connect()
        try:
            await anyio.to_thread.run_sync(wait_until, ready.exists, 5, "CLI child started")
            child_pid = int(ready.read_text())
            assert transport._process is not None
            leader_pid = transport._process.pid
            assert os.getpgid(child_pid) == leader_pid
            assert leader_pid != os.getpgrp()
            with anyio.fail_after(6):
                await transport.stop_process_group()
            assert wait_gone(leader_pid, 1)
            assert wait_gone(child_pid, 1)
        finally:
            await transport.close()

    anyio.run(go)


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


# --- #5378: the transport refuses CLI input carrying a relay-held value -------------------


@pytest.mark.parametrize("encoding", ("literal", "base64"))
@pytest.mark.parametrize("surface", ("config", "options-env"))
def test_a_relay_held_value_on_cli_input_is_refused_before_any_file(
    surface: str, encoding: str
) -> None:
    import base64

    from curie_runner.mcp_argv import CredentialSafeCLITransport, HostedHeaderExposure

    sentinel = f"relay-held-sentinel-{uuid.uuid4().hex}"
    servers = _relayed_servers({"Authorization": f"Bearer {sentinel}"})
    copied = sentinel if encoding == "literal" else base64.b64encode(sentinel.encode()).decode()
    env: dict[str, str] = {}
    if surface == "config":
        # A second, unrelayed copy of the same value beside the relay.
        servers["copy"] = {
            "type": "http",
            "url": "https://copy.example.com/mcp",
            "headers": {"X-Copy": copied},
        }
    else:
        env["ACME_COPY"] = copied
    transport = CredentialSafeCLITransport(
        prompt="", options=ClaudeAgentOptions(cli_path="/bin/true", mcp_servers=servers, env=env)
    )
    before = _config_files()
    try:
        with pytest.raises(HostedHeaderExposure) as refused:
            transport._build_command()
        assert sentinel not in str(refused.value)
        assert copied not in str(refused.value)
        assert _config_files() == before, "a refused command still wrote a config file"
    finally:
        anyio.run(transport.close)


def test_offload_refuses_a_relay_held_value_anywhere_on_the_command_line() -> None:
    from curie_runner.mcp_argv import HostedHeaderExposure, offload_mcp_config_argv

    sentinel = f"relay-held-sentinel-{uuid.uuid4().hex}"
    payload = json.dumps({"mcpServers": {"copy": {"headers": {"X-Copy": sentinel}}}})
    before = _config_files()
    for cmd in (
        ["claude", "--mcp-config", payload],
        ["claude", "--append-system-prompt", f"carry {sentinel}"],
    ):
        with pytest.raises(HostedHeaderExposure):
            offload_mcp_config_argv(cmd, protected={sentinel})
    assert _config_files() == before


def test_relay_beside_ordinary_headers_and_declared_stdio_credentials_still_builds() -> None:
    # Liveness for the refusal above: only relay-held values are refused.
    from curie_runner.mcp_argv import CredentialSafeCLITransport

    sentinel = f"relay-held-sentinel-{uuid.uuid4().hex}"
    servers = _relayed_servers({"Authorization": f"Bearer {sentinel}"})
    servers["remote"] = {
        "type": "http",
        "url": "https://remote.example.com/mcp",
        "headers": {"X-Acme-Public": "remote-literal-public-value"},
    }
    servers["local"] = {
        "command": sys.executable,
        "args": ["-c", "pass"],
        "env": {"ACME_STDIO_TOKEN": "stdio-declared-public-value"},
    }
    transport = CredentialSafeCLITransport(
        prompt="",
        options=ClaudeAgentOptions(
            cli_path="/bin/true", mcp_servers=servers, env={"ACME_PUBLIC": "public-value"}
        ),
    )
    try:
        cmd = transport._build_command()
        path = Path(cmd[cmd.index("--mcp-config") + 1])
        written = json.loads(path.read_text(encoding="utf-8"))["mcpServers"]
        assert written["github"] == {"type": "sdk", "name": "github"}
        assert written["remote"]["headers"] == {"X-Acme-Public": "remote-literal-public-value"}
        assert written["local"]["env"] == {"ACME_STDIO_TOKEN": "stdio-declared-public-value"}
        assert sentinel not in path.read_text(encoding="utf-8")
        assert sentinel not in "\0".join(cmd)
    finally:
        anyio.run(transport.close)
