"""Keep MCP config JSON off the claude CLI argv (#2635).

The SDK dumps ``mcp_servers`` onto ``--mcp-config`` as a JSON token, which
puts hosted-connector credentials on ``/proc/pid/cmdline``. Rewrite that
token to a ``0600`` tempfile in the default temp dir (``/tmp`` scratch).
"""

from __future__ import annotations

import contextlib
import os
import tempfile
from collections.abc import AsyncIterable
from typing import Any

from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk._internal.transport.subprocess_cli import (
    SubprocessCLITransport as _SDKSubprocessCLITransport,
)

_TMP_PREFIX = "curie-mcp-config-"


def _write_mcp_config_file(payload: str) -> str:
    fd, path = tempfile.mkstemp(prefix=_TMP_PREFIX, suffix=".json")
    try:
        try:
            os.fchmod(fd, 0o600)
            data = payload.encode("utf-8")
            written = 0
            while written < len(data):
                written += os.write(fd, data[written:])
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(path)
            raise
    finally:
        os.close(fd)
    return path


def offload_mcp_config_argv(cmd: list[str]) -> list[str]:
    """Rewrite two-token JSON ``--mcp-config`` values to ``0600`` temp files (#2635).

    Path-like values are left unchanged. Returns a new list.
    """
    rewritten = list(cmd)
    created: list[str] = []
    try:
        i = 0
        while i < len(rewritten):
            if rewritten[i] == "--mcp-config" and i + 1 < len(rewritten):
                value = rewritten[i + 1]
                if value.lstrip().startswith("{"):
                    path = _write_mcp_config_file(value)
                    created.append(path)
                    rewritten[i + 1] = path
                i += 2
                continue
            i += 1
    except Exception:
        for path in created:
            with contextlib.suppress(OSError):
                os.unlink(path)
        raise
    return rewritten


class CredentialSafeCLITransport(_SDKSubprocessCLITransport):
    """Spawn wrapper that offloads JSON ``--mcp-config`` argv to a tempfile (#2635)."""

    def __init__(
        self,
        prompt: str | AsyncIterable[dict[str, Any]],
        options: ClaudeAgentOptions,
    ) -> None:
        super().__init__(prompt, options)
        self._mcp_config_tmps: list[str] = []

    def _unlink_mcp_config_tmps(self) -> None:
        paths = self._mcp_config_tmps
        self._mcp_config_tmps = []
        for path in paths:
            with contextlib.suppress(OSError):
                os.unlink(path)

    def _build_command(self) -> list[str]:
        self._unlink_mcp_config_tmps()
        cmd = super()._build_command()
        rewritten = offload_mcp_config_argv(cmd)
        offloaded: list[str] = []
        for i, token in enumerate(cmd):
            if token == "--mcp-config" and i + 1 < len(cmd):
                new_val = rewritten[i + 1]
                if new_val != cmd[i + 1]:
                    offloaded.append(new_val)
        self._mcp_config_tmps = offloaded
        return rewritten

    async def close(self) -> None:
        try:
            await super().close()
        finally:
            # SDK returns early when no process was spawned; still unlink (#2635).
            self._unlink_mcp_config_tmps()


def install() -> None:
    """Patch the SDK transport class ``ClaudeSDKClient.connect`` imports (#2635)."""
    import claude_agent_sdk._internal.transport.subprocess_cli as m

    if m.SubprocessCLITransport is CredentialSafeCLITransport:
        return
    m.SubprocessCLITransport = CredentialSafeCLITransport  # type: ignore[misc]
