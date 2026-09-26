"""Inspect the standard MCP tool annotations used by the approval-tool mount.

``request_approval`` can only be useful when the session has an action it may
eventually perform. MCP already carries the relevant capability metadata on
``Tool.annotations.readOnlyHint``; probing that live surface avoids inventing a
second bundle-format declaration that can drift from what a server publishes.
The annotation is only a hint and is not used as an authorization decision:
permission gates and tool execution are unchanged. It controls whether Curie's
non-authoritative, model-invoked generic pager is advertised and which exact
runtime tool names are treated as read-only for receipts and retry safety.

Only an entirely observed, explicitly read-only surface proves that the generic
approval tool should be omitted. A complete surface with zero MCP tools also
proves omission: there is no MCP action for a human to unlock. Built-in Claude
tools are deliberately outside this MCP capability decision; an explicit
approval gate on one is handled separately by the boot path and retains the
pager. An absent hint, an uninspectable declaration, and any probe failure are
all treated as potentially write-capable, preserving the existing tool on
unknown surfaces rather than silently removing a capability.
"""

from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio
from mcp import ClientSession, StdioServerParameters
from mcp.client.sse import sse_client
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client
from mcp.shared._httpx_utils import create_mcp_http_client
from mcp.shared.exceptions import MCPError
from mcp.shared.tool_name_validation import validate_tool_name
from mcp.types import PaginatedRequestParams
from plugin_format import PluginManifest, resolve_manifest
from plugin_format.approval_policy import connector_tool_prefix, effective_tool_prefix

logger = logging.getLogger(__name__)

_VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_PROBE_TIMEOUT_SECONDS = 15

# What a hosted connector's caller proxy says when it refuses this sandbox
# (ADR-0168 decision 7), frozen in tests/vectors/connector-caller-refusal.json:
# the refusal's name under this key of the JSON-RPC error's `data`.
_CALLER_REFUSAL_KEY = "curie_caller"
_CALLER_REFUSALS = {
    "missing": "this sandbox presented no caller token",
    "invalid": "this sandbox's caller token is not valid",
    "expired": "this sandbox's caller token has expired",
    "not_admitted": "this agent is not in the connector's admits list",
}


@dataclass(frozen=True)
class ConnectorCapabilityFailure:
    """A declared-connector capability/auth failure safe to show a caller (#2519).

    Names the connector and the credential env var. Never carries a secret
    value: diagnosis keys on placeholder presence versus env emptiness, and
    probe exceptions are mapped to ``probe_failed`` without ``str(exc)``.
    """

    connector: str
    credential_names: tuple[str, ...]
    reason: str
    # For `caller_refused` only: which refusal the connector's proxy named.
    refusal: str | None = None

    def caller_message(self) -> str:
        """The exact sentence the message caller sees. Values never appear."""

        names = ", ".join(self.credential_names)
        if self.reason == "caller_refused":
            return (
                f"declared connector '{self.connector}' refused this sandbox: "
                f"{_CALLER_REFUSALS[self.refusal or 'invalid']}. Connector tools are "
                "unavailable."
            )
        if self.reason == "empty_expansion":
            detail = f"credential {names} expanded empty"
        elif self.reason == "missing_credential":
            detail = f"credential {names} is not set in the sandbox"
        elif self.credential_names:
            detail = f"credential {names}; capability probe failed"
        else:
            detail = "capability probe failed"
        return (
            f"declared connector '{self.connector}' failed MCP capability probe: "
            f"{detail}. Connector tools are unavailable."
        )


class ConnectorAvailability:
    """The live set of declared connectors whose tools are excluded (#2634).

    Shared, mutable, runner-owned state: ``build_runner`` creates one instance
    and hands it to both the connector exclusion PreToolUse hook and the
    ``SessionRunner``. The runner replaces ``failures`` after each turn-start
    re-probe, so a recovered connector's tools become callable again on the
    very next tool call without rebuilding the SDK session.
    """

    def __init__(self, failures: tuple[ConnectorCapabilityFailure, ...] = ()) -> None:
        self.failures = failures

    def failure_for_tool(self, tool_name: str) -> ConnectorCapabilityFailure | None:
        """The failure excluding ``tool_name``, or None when the tool is available."""

        for failure in self.failures:
            if tool_name.startswith(connector_tool_prefix(failure.connector)):
                return failure
        return None


@dataclass(frozen=True)
class McpToolCapabilityProbe:
    """The conservative capability conclusion for one session's MCP surface."""

    complete: bool
    has_potential_write_tool: bool
    tool_count: int
    failures: tuple[str, ...] = ()
    readonly_tools: frozenset[str] = frozenset()
    observed_tools: frozenset[str] = frozenset()
    connector_failures: tuple[ConnectorCapabilityFailure, ...] = ()


def _header_placeholders(config: Mapping[str, Any]) -> tuple[str, ...]:
    """Credential env-var names referenced by ``${NAME}`` in MCP headers."""

    headers = config.get("headers")
    if not isinstance(headers, Mapping):
        return ()
    found: list[str] = []
    for value in headers.values():
        if isinstance(value, str):
            for var in _VARIABLE.findall(value):
                if var not in found:
                    found.append(var)
    return tuple(found)


def diagnose_derived_connector_headers(
    derived_servers: Mapping[str, Mapping[str, Any]],
    inherited_env: Mapping[str, str] | None = None,
) -> tuple[ConnectorCapabilityFailure, ...]:
    """Network-free diagnosis of empty or missing header placeholders (#2519).

    Shared with #2352 via ``tests/vectors/connector-probe-diagnosis.json``.
    Inspects env presence and emptiness only; it never interpolates values.
    """

    env = dict(inherited_env or {})
    failures: list[ConnectorCapabilityFailure] = []
    for name, config in derived_servers.items():
        if not isinstance(config, Mapping):
            continue
        placeholders = _header_placeholders(config)
        if not placeholders:
            continue
        empty: list[str] = []
        missing: list[str] = []
        for var in placeholders:
            if var not in env:
                missing.append(var)
            elif not str(env[var]).strip():
                empty.append(var)
        if empty or missing:
            failures.append(
                ConnectorCapabilityFailure(
                    connector=str(name),
                    credential_names=tuple(empty + missing),
                    reason="empty_expansion" if empty else "missing_credential",
                )
            )
    return tuple(failures)


def _caller_refusal(exc: BaseException) -> str | None:
    """The caller proxy's refusal inside a probe failure, or None.

    The MCP client raises the JSON-RPC error from inside its task groups, so
    the error is found by walking the exception groups.
    """

    if isinstance(exc, MCPError) and isinstance(exc.data, Mapping):
        refusal = exc.data.get(_CALLER_REFUSAL_KEY)
        return refusal if refusal in _CALLER_REFUSALS else None
    if isinstance(exc, BaseExceptionGroup):
        for inner in exc.exceptions:
            found = _caller_refusal(inner)
            if found is not None:
                return found
    return None


def _expand(value: str, env: Mapping[str, str]) -> str:
    """Apply the ``${NAME}`` substitutions Claude applies to MCP config values."""

    return _VARIABLE.sub(lambda match: env.get(match.group(1), match.group(0)), value)


def _bundle_server_configs(
    plugin_dir: Path,
) -> list[tuple[str, dict[str, Any], str]]:
    """Read both plugin MCP declaration surfaces without collapsing duplicates.

    Every declared server must be inspectable here. In particular, the plugin
    format accepts an ``mcpServers`` path string even though Curie's runtime
    loader does not follow it. Ignoring that or a malformed server entry would
    turn an unknown surface into a false all-read-only conclusion, so these
    shapes raise and the caller conservatively retains ``request_approval``.
    """

    def append_payload(payload: object, source: str, bundle_name: str) -> None:
        if not isinstance(payload, dict):
            raise ValueError(f"{source} MCP declaration is not an object")
        servers = payload.get("mcpServers", payload)
        if not isinstance(servers, dict):
            raise ValueError(f"{source} mcpServers declaration is not an object")
        for name, config in servers.items():
            if not isinstance(config, dict):
                raise ValueError(f"{source} MCP server {name!r} is not an object")
            server_name = str(name)
            configs.append(
                (
                    server_name,
                    dict(config),
                    effective_tool_prefix(bundle_name, server_name),
                )
            )

    configs: list[tuple[str, dict[str, Any], str]] = []
    manifest_path = resolve_manifest(plugin_dir)
    bundle_name: str | None = None
    if manifest_path is not None:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest = PluginManifest.model_validate(raw)
        bundle_name = manifest.name
        if isinstance(manifest.mcpServers, str):
            raise ValueError(
                "plugin manifest mcpServers path strings cannot be inspected at runtime"
            )
        if manifest.mcpServers is not None:
            append_payload(manifest.mcpServers, "plugin manifest", bundle_name)

    root_config = plugin_dir / ".mcp.json"
    if root_config.is_file():
        if bundle_name is None:
            raise ValueError("bundle MCP declarations require a manifest name")
        raw = json.loads(root_config.read_text(encoding="utf-8"))
        append_payload(raw, ".mcp.json", bundle_name)
    return configs


@asynccontextmanager
async def _server_streams(
    config: Mapping[str, Any],
    *,
    plugin_dir: Path | None,
    inherited_env: Mapping[str, str],
) -> AsyncIterator[tuple[Any, Any]]:
    """Open one stdio, SSE, or streamable-HTTP MCP transport."""

    interpolation_env = dict(inherited_env)
    if plugin_dir is not None:
        interpolation_env["CLAUDE_PLUGIN_ROOT"] = str(plugin_dir)

    command = config.get("command")
    if isinstance(command, str) and command:
        configured_env = config.get("env")
        child_env = dict(interpolation_env)
        if isinstance(configured_env, Mapping):
            child_env.update(
                {
                    str(key): _expand(str(value), interpolation_env)
                    for key, value in configured_env.items()
                }
            )
        args = config.get("args")
        parameters = StdioServerParameters(
            command=_expand(command, interpolation_env),
            args=[_expand(str(arg), interpolation_env) for arg in args]
            if isinstance(args, list)
            else [],
            env=child_env,
            cwd=plugin_dir,
        )
        async with stdio_client(parameters) as streams:
            yield streams
        return

    url = config.get("url")
    if not isinstance(url, str) or not url:
        raise ValueError("MCP server declares neither a command nor a URL")
    expanded_url = _expand(url, interpolation_env)
    raw_headers = config.get("headers")
    headers = (
        {
            str(key): _expand(str(value), interpolation_env)
            for key, value in raw_headers.items()
        }
        if isinstance(raw_headers, Mapping)
        else None
    )
    if config.get("type") == "sse":
        async with sse_client(expanded_url, headers=headers) as streams:
            yield streams
        return

    async with create_mcp_http_client(headers=headers) as http_client:
        async with streamable_http_client(expanded_url, http_client=http_client) as streams:
            read_stream, write_stream = streams
            yield read_stream, write_stream


async def _probe_server(
    config: Mapping[str, Any],
    *,
    tool_prefix: str,
    plugin_dir: Path | None,
    inherited_env: Mapping[str, str],
) -> tuple[int, bool, frozenset[str], frozenset[str]]:
    """Return count, write capability, and exact observed/read-only tool names."""

    count = 0
    has_potential_write = False
    observed_tools: set[str] = set()
    readonly_tools: set[str] = set()
    with anyio.fail_after(_PROBE_TIMEOUT_SECONDS):
        async with _server_streams(
            config, plugin_dir=plugin_dir, inherited_env=inherited_env
        ) as (read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=_PROBE_TIMEOUT_SECONDS,
            ) as session:
                await session.initialize()
                cursor: str | None = None
                while True:
                    result = await session.list_tools(
                        params=PaginatedRequestParams(cursor=cursor)
                    )
                    if any(
                        not validate_tool_name(tool.name).is_valid
                        for tool in result.tools
                    ):
                        raise ValueError("MCP server returned a nonconforming tool name")
                    count += len(result.tools)
                    observed_tools.update(
                        f"{tool_prefix}{tool.name}" for tool in result.tools
                    )
                    if any(
                        tool.annotations is None
                        or tool.annotations.read_only_hint is not True
                        for tool in result.tools
                    ):
                        has_potential_write = True
                    readonly_tools.update(
                        f"{tool_prefix}{tool.name}"
                        for tool in result.tools
                        if tool.annotations is not None
                        and tool.annotations.read_only_hint is True
                    )
                    cursor = result.next_cursor
                    if not cursor:
                        break
    return (
        count,
        has_potential_write,
        frozenset(observed_tools),
        frozenset(readonly_tools),
    )


async def probe_mcp_tool_capability(
    plugin_dir: str | Path | None,
    derived_servers: Mapping[str, Mapping[str, Any]],
    inherited_env: Mapping[str, str] | None = None,
) -> McpToolCapabilityProbe:
    """Probe every bundle/connector MCP server and conservatively classify it.

    ``derived_servers`` is the connector map already produced for the SDK. A
    failed or unreadable source is an unknown surface, which keeps
    ``request_approval`` mounted by returning ``has_potential_write_tool=True``.
    Exact read-only names from successful sibling servers remain available.
    """

    root = Path(plugin_dir) if plugin_dir is not None else None
    env = {**os.environ, **dict(inherited_env or {})}
    failures: list[str] = []
    observations: list[tuple[int, bool, frozenset[str], frozenset[str]]] = []
    expansion_failures = diagnose_derived_connector_headers(derived_servers, env)
    skip_http = {failure.connector for failure in expansion_failures}
    connector_failures: list[ConnectorCapabilityFailure] = list(expansion_failures)

    try:
        declared = _bundle_server_configs(root) if root is not None else []
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        logger.warning("cannot inspect bundle MCP declarations; keeping approval tool: %s", exc)
        return McpToolCapabilityProbe(
            complete=False,
            has_potential_write_tool=True,
            tool_count=0,
            failures=("bundle-config",),
            connector_failures=tuple(expansion_failures),
        )

    work: list[tuple[str, Mapping[str, Any], Path | None, str]] = [
        (name, config, root, tool_prefix) for name, config, tool_prefix in declared
    ]
    derived_names: set[str] = set()
    for name, config in derived_servers.items():
        if not isinstance(config, Mapping):
            failures.append(str(name))
            logger.warning(
                "cannot inspect derived MCP declaration server=%s; keeping approval tool",
                name,
            )
            continue
        server_name = str(name)
        derived_names.add(server_name)
        work.append((server_name, config, None, connector_tool_prefix(server_name)))

    async def inspect(
        name: str,
        config: Mapping[str, Any],
        cwd: Path | None,
        tool_prefix: str,
    ) -> None:
        derived = cwd is None and name in derived_names
        if derived and name in skip_http:
            # Empty/missing expansion already diagnosed; do not dial /mcp with
            # an empty Bearer (the exception text is the leak surface).
            failures.append(name)
            return
        try:
            observations.append(
                await _probe_server(
                    config,
                    tool_prefix=tool_prefix,
                    plugin_dir=cwd,
                    inherited_env=env,
                )
            )
        except Exception as exc:
            failures.append(name)
            logger.warning(
                "MCP tool-capability probe failed server=%s; keeping approval tool: %s",
                name,
                exc,
            )
            if derived and name not in skip_http:
                refusal = _caller_refusal(exc)
                connector_failures.append(
                    ConnectorCapabilityFailure(
                        connector=name,
                        credential_names=_header_placeholders(config),
                        reason="probe_failed" if refusal is None else "caller_refused",
                        refusal=refusal,
                    )
                )

    async with anyio.create_task_group() as task_group:
        for name, config, cwd, tool_prefix in work:
            task_group.start_soon(inspect, name, config, cwd, tool_prefix)

    # Stable-side connector diagnosis tests monkeypatch the pre-catalog
    # three-field probe shape; treat its sole tool-name set as read-only while
    # retaining next's exact observed/read-only split for real probes.
    normalized = [
        (*observation, observation[2]) if len(observation) == 3 else observation
        for observation in observations
    ]
    tool_count = sum(count for count, _write, _observed, _readonly in normalized)
    has_write = bool(failures) or any(
        write for _count, write, _observed, _readonly in normalized
    )
    observed_tools = frozenset(
        tool
        for _count, _write, observed, _readonly in normalized
        for tool in observed
    )
    readonly_tools = frozenset(
        tool
        for _count, _write, _observed, observed_readonly in normalized
        for tool in observed_readonly
    )
    return McpToolCapabilityProbe(
        complete=not failures,
        has_potential_write_tool=has_write,
        tool_count=tool_count,
        failures=tuple(sorted(set(failures))),
        observed_tools=observed_tools,
        readonly_tools=readonly_tools,
        connector_failures=tuple(connector_failures),
    )


async def reprobe_connector_failures(
    failures: tuple[ConnectorCapabilityFailure, ...],
    derived_servers: Mapping[str, Mapping[str, Any]],
    inherited_env: Mapping[str, str] | None = None,
) -> tuple[ConnectorCapabilityFailure, ...]:
    """Re-dial the connectors a boot probe could not reach; return what still fails.

    Only ``probe_failed`` is re-dialed: a transient network error at boot must
    not remove a connector for the life of the process (#2634). Expansion
    failures (``empty_expansion`` / ``missing_credential``) are deterministic
    within the process and are carried through unchanged without dialing,
    because after ``materialize_hosted_bearer_headers`` an empty value is a
    literal empty Bearer and #2519 forbids sending it to ``/mcp``.

    A re-probe that still fails keeps the prior failure, credential names
    included (the materialized config no longer carries the placeholder names).
    The exception is logged by class only; its text can carry a header value.
    """

    env = {**os.environ, **dict(inherited_env or {})}
    still_failing: list[ConnectorCapabilityFailure] = []

    async def redial(failure: ConnectorCapabilityFailure) -> None:
        config = derived_servers.get(failure.connector)
        if not isinstance(config, Mapping):
            still_failing.append(failure)
            return
        try:
            await _probe_server(
                config,
                tool_prefix=connector_tool_prefix(failure.connector),
                plugin_dir=None,
                inherited_env=env,
            )
        except Exception as exc:  # noqa: BLE001 - any probe failure keeps the exclusion
            logger.warning(
                "connector re-probe failed server=%s error_class=%s",
                failure.connector,
                type(exc).__name__,
            )
            still_failing.append(failure)
            return
        logger.info("connector re-probe recovered server=%s", failure.connector)

    async with anyio.create_task_group() as task_group:
        for failure in failures:
            if failure.reason == "probe_failed":
                task_group.start_soon(redial, failure)
    unrecovered = set(still_failing)
    # Preserve the boot order so the caller-visible notice is stable turn to turn.
    return tuple(
        failure
        for failure in failures
        if failure.reason != "probe_failed" or failure in unrecovered
    )
