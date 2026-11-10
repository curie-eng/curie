"""Resolve stdio declarations before they reach any process launcher."""

from __future__ import annotations

import json
import re
from collections.abc import Collection, Mapping
from pathlib import Path
from typing import Any

from curie_telemetry.redact import held_secret_literals

from .subprocess_env import platform_credential_names, shell_and_hook_env

_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_REFERENCE_NAME = re.compile(r"\$(?:\{)?([A-Za-z_][A-Za-z0-9_]*)")


class UnsafeStdioConfiguration(ValueError):
    """A stdio declaration tries to carry a model or platform credential."""


def sanitize_stdio_server(
    entry: Mapping[str, Any],
    source_env: Mapping[str, str],
    *,
    plugin_dir: Path | None = None,
    denied_names: Collection[str] = (),
) -> dict[str, Any]:
    """Materialize permitted references from a scrubbed source, with explicit env."""

    result = dict(entry)
    command = result.get("command")
    if not isinstance(command, str):
        return result
    raw_env = result.get("env", {})
    if not isinstance(raw_env, Mapping) or not all(
        isinstance(key, str) and isinstance(value, str) for key, value in raw_env.items()
    ):
        raise UnsafeStdioConfiguration("stdio env must map names to strings")
    declarations = dict(raw_env)
    denied = (
        frozenset(denied_names)
        | platform_credential_names(source_env)
        | platform_credential_names({**source_env, **declarations})
    )
    held = held_secret_literals(
        [value for name, value in source_env.items() if name in denied and value]
    )
    interpolation = {
        name: value
        for name, value in shell_and_hook_env(source_env).items()
        if name not in denied and not any(secret in value for secret in held)
    }
    if plugin_dir is not None:
        interpolation["CLAUDE_PLUGIN_ROOT"] = str(plugin_dir)

    def materialize(value: str) -> str:
        if any(secret in value for secret in held):
            raise UnsafeStdioConfiguration("stdio configuration contains a held credential")
        if any(name in denied for name in _REFERENCE_NAME.findall(value)):
            raise UnsafeStdioConfiguration("stdio configuration references a credential")

        def expand(match: re.Match[str]) -> str:
            name = match.group(1)
            if name in denied:
                raise UnsafeStdioConfiguration("stdio configuration references a credential")
            if interpolation.get(name):
                return interpolation[name]
            if match.group(2) is not None:
                return match.group(2)
            return ""

        expanded = _REFERENCE.sub(expand, value)
        if any(name in denied for name in _REFERENCE_NAME.findall(expanded)):
            raise UnsafeStdioConfiguration("stdio configuration references a credential")
        if "${" in expanded:
            raise UnsafeStdioConfiguration("stdio configuration contains an unresolved reference")
        if any(secret in expanded for secret in held):
            raise UnsafeStdioConfiguration("stdio configuration contains a held credential")
        return expanded

    if any(name in denied for name in declarations):
        raise UnsafeStdioConfiguration("stdio env declares a credential name")
    args = result.get("args", [])
    if not isinstance(args, (list, tuple)) or not all(isinstance(arg, str) for arg in args):
        raise UnsafeStdioConfiguration("stdio args must be strings")
    result["command"] = materialize(command)
    result["args"] = [materialize(arg) for arg in args]
    result["env"] = shell_and_hook_env(
        interpolation, extra={name: materialize(value) for name, value in declarations.items()}
    )
    return result


def sanitize_mcp_servers(
    servers: Mapping[str, Any] | str | Path,
    source_env: Mapping[str, str],
    *,
    denied_names: Collection[str] = (),
) -> dict[str, Any]:
    if isinstance(servers, (str, Path)):
        try:
            payload = (
                str(servers)
                if isinstance(servers, str) and servers.lstrip().startswith("{")
                else Path(servers).read_text(encoding="utf-8")
            )
            decoded = json.loads(payload)
        except (OSError, UnicodeError, ValueError):
            raise UnsafeStdioConfiguration(
                "MCP configuration must be a readable JSON object"
            ) from None
        if not isinstance(decoded, dict):
            raise UnsafeStdioConfiguration("MCP configuration must be an object")
        servers = decoded.get("mcpServers", decoded)
        if not isinstance(servers, dict):
            raise UnsafeStdioConfiguration("MCP servers must be an object")
    return {
        name: sanitize_stdio_server(entry, source_env, denied_names=denied_names)
        if isinstance(entry, Mapping)
        else entry
        for name, entry in servers.items()
    }
