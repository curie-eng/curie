"""Prerequisite detection for tests that drive the real Claude Code CLI.

The locked claude-agent-sdk ships an executable CLI inside its own package and
prefers it over any `claude` on PATH. A real-SDK test is therefore runnable
whenever the SDK's own discovery resolves an executable, with or without a
global install, so the skip decision asks the SDK rather than PATH.
"""

from __future__ import annotations

import os

from claude_agent_sdk import ClaudeAgentOptions, CLINotFoundError
from claude_agent_sdk._internal.transport.subprocess_cli import SubprocessCLITransport


def sdk_cli_path() -> str | None:
    """Return the executable CLI the SDK would spawn, or None if it has none."""
    transport = SubprocessCLITransport(prompt="", options=ClaudeAgentOptions())
    try:
        path = transport._find_cli()
    except CLINotFoundError:
        return None
    return path if os.access(path, os.X_OK) else None


def sdk_cli_available() -> bool:
    return sdk_cli_path() is not None
