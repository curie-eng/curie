"""Validate MCP declarations before configuration or transport delivery."""

from __future__ import annotations

import json
import re
from collections.abc import Collection, Mapping
from pathlib import Path
from typing import Any

from curie_telemetry.redact import held_secret_literals

from .subprocess_env import (
    model_credential_names,
    platform_credential_names,
    shell_and_hook_env,
)

_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_REFERENCE_NAME = re.compile(r"\$(?:\{)?([A-Za-z_][A-Za-z0-9_]*)")


class UnsafeStdioConfiguration(ValueError):
    """An MCP declaration tries to carry a protected credential."""


def _refuse_credentials(
    value: Any,
    denied: Collection[str],
    held: Collection[str],
    source_env: Mapping[str, str],
) -> None:
    if isinstance(value, str):
        expanded = _REFERENCE.sub(
            lambda match: source_env.get(match.group(1))
            or (match.group(2) if match.group(2) is not None else match.group(0)),
            value,
        )
        for text in (value, expanded):
            if any(secret in text for secret in held):
                raise UnsafeStdioConfiguration("MCP configuration contains a held credential")
            if any(name in denied for name in _REFERENCE_NAME.findall(text)):
                raise UnsafeStdioConfiguration("MCP configuration references a credential")
    elif isinstance(value, Mapping):
        for key, item in value.items():
            _refuse_credentials(key, denied, held, source_env)
            _refuse_credentials(item, denied, held, source_env)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _refuse_credentials(item, denied, held, source_env)


def sanitize_stdio_server(
    entry: Mapping[str, Any],
    source_env: Mapping[str, str],
    *,
    plugin_dir: Path | None = None,
    denied_names: Collection[str] = (),
    held_values: Collection[str] = (),
    resolve_references: bool = True,
) -> dict[str, Any]:
    """Validate declarations and build only their scrubbed explicit environment."""

    result = dict(entry)
    command = result.get("command")
    if not isinstance(command, str):
        raise UnsafeStdioConfiguration("stdio command must be a string")
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
        + list(held_values)
    )
    _refuse_credentials(entry, denied, held, source_env)
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
    def resolve(value: str) -> str:
        resolved = materialize(value)
        return resolved if resolve_references else value

    result["command"] = resolve(command)
    result["args"] = [resolve(arg) for arg in args]
    explicit_env = {name: resolve(value) for name, value in declarations.items()}
    if plugin_dir is not None:
        explicit_env.setdefault("CLAUDE_PLUGIN_ROOT", str(plugin_dir))
    result["env"] = shell_and_hook_env(explicit_env)
    return result


def sanitize_mcp_servers(
    servers: Mapping[str, Any] | str | Path,
    source_env: Mapping[str, str],
    *,
    denied_names: Collection[str] = (),
    inherited_env: Mapping[str, str] | None = None,
    plugin_dir: Path | None = None,
    resolve_references: bool = True,
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
    sources = (source_env, inherited_env or {})
    model_names = frozenset().union(*(model_credential_names(env) for env in sources))
    model_values = {
        value for env in sources for name, value in env.items() if name in model_names and value
    }
    _refuse_credentials(servers, model_names, held_secret_literals(model_values), source_env)
    return {
        name: sanitize_stdio_server(
            entry,
            source_env,
            denied_names=denied_names,
            held_values=model_values,
            plugin_dir=plugin_dir,
            resolve_references=resolve_references,
        )
        if isinstance(entry, Mapping) and ("command" in entry or entry.get("type") == "stdio")
        else entry
        for name, entry in servers.items()
    }
