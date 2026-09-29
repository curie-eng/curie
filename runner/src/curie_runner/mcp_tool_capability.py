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

A connector that is merely not up yet -- a rollout window -- is not a
capability failure (#2945): the boot probe retries a failed dial with a
bounded backoff before declaring the connector unavailable, and the refusal a
caller finally sees distinguishes that bounded retry from a deterministic
misconfiguration.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
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
from mcp.shared.tool_name_validation import validate_tool_name
from mcp.types import PaginatedRequestParams
from plugin_format import PluginManifest, resolve_manifest
from plugin_format.approval_policy import connector_tool_prefix, effective_tool_prefix

logger = logging.getLogger(__name__)

_VARIABLE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_PROBE_TIMEOUT_SECONDS = 15

# Bounded retry for the boot capability probe (#2945). A failed dial is
# retried up to ``_PROBE_ATTEMPTS`` times with a doubling backoff, and each
# attempt's timeout is clamped to the budget remaining from the first dial,
# so the whole dialing sequence -- including a connector that accepts and
# then hangs -- stays under ``_PROBE_RETRY_BUDGET_SECONDS`` of wall clock:
# 30 seconds at boot, well under the worker's claim timeout
# (``claim_timeout_seconds``, 90 s), where a single unretried dial could
# already burn 15. A fast-refusing rollout connector gives up in ~6 seconds.
# The turn-start re-dial (#2634) passes ``attempts=1``, so its per-turn
# budget is unchanged.
_PROBE_ATTEMPTS = 3
_PROBE_RETRY_BACKOFF_SECONDS = 2.0
_PROBE_RETRY_BUDGET_SECONDS = 30.0


class _ProbeConfigurationError(ValueError):
    """Deterministic rejection of a malformed MCP declaration (#2945).

    Raised where the probe inspects its own inputs -- a server that declares
    neither a command nor a URL -- or the served MCP contract, a
    nonconforming tool name. Another dial cannot change the declaration, so
    the retry wrapper does not retry it and the caller records
    ``probe_misconfigured``, a refusal distinguishable from a rollout
    window's ``probe_failed``. A ``ValueError`` subclass because other
    callers of these helpers already treat that shape as an unknown surface.
    """


class _ProbeFailure(Exception):
    """A connector that stayed unreachable across the bounded retry (#2945).

    Carries the number of dials actually made. The message names only the
    last error's class, never its text: probe exceptions can carry a header
    value, and the boot probe's warning formats this exception directly, so
    its ``__str__`` is a leak surface and stays class-only (#2634).
    """

    def __init__(self, attempts: int, last_error: BaseException) -> None:
        plural = "s" if attempts != 1 else ""
        super().__init__(
            f"capability probe failed after {attempts} attempt{plural}; "
            f"last error {type(last_error).__name__}"
        )
        self.attempts = attempts
        self.last_error = last_error


def _probe_error_leaves(exc: BaseException) -> tuple[BaseException, ...]:
    """Flatten exception groups down to the leaf causes (#2945).

    A probe error raised inside the transport or session task groups arrives
    wrapped in a ``BaseExceptionGroup``; retry and misconfiguration decisions
    must look at the leaves, not at the group.
    """

    if isinstance(exc, BaseExceptionGroup):
        return tuple(
            leaf
            for sub in exc.exceptions
            for leaf in _probe_error_leaves(sub)
        )
    return (exc,)


@dataclass(frozen=True)
class ConnectorCapabilityFailure:
    """A declared-connector capability/auth failure safe to show a caller (#2519).

    Names the connector and the credential env var. Never carries a secret
    value: diagnosis keys on placeholder presence versus env emptiness, and
    probe exceptions are mapped to ``probe_failed`` without ``str(exc)``.

    ``attempts`` is the number of dials the probe that recorded this failure
    made (#2945): the boot probe's bounded retry count. The turn-start
    re-dial is single-attempt and does not update it, so a connector still
    down on a later turn keeps reporting the boot count.
    """

    connector: str
    credential_names: tuple[str, ...]
    reason: str
    attempts: int = 1

    def caller_message(self) -> str:
        """The exact sentence the message caller sees. Values never appear."""

        names = ", ".join(self.credential_names)
        if self.reason == "empty_expansion":
            detail = f"credential {names} expanded empty"
        elif self.reason == "missing_credential":
            detail = f"credential {names} is not set in the sandbox"
        elif self.reason == "probe_misconfigured":
            # Deterministic rejection the retry cannot clear (#2945): the
            # declaration or the served tool names are invalid, which no
            # connector rollout window would produce.
            detail = "capability probe rejected the connector as misconfigured"
        else:
            # probe_failed: the connector stayed unreachable across the
            # boot probe's bounded retry. The count distinguishes this from
            # the deterministic refusals above (#2945).
            detail = "capability probe failed"
            if self.attempts > 1:
                detail += f" after {self.attempts} attempts"
        if self.credential_names and self.reason not in (
            "empty_expansion",
            "missing_credential",
        ):
            detail = f"credential {names}; {detail}"
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
        raise _ProbeConfigurationError(
            "MCP server declares neither a command nor a URL"
        )
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


async def _probe_server_once(
    config: Mapping[str, Any],
    *,
    tool_prefix: str,
    plugin_dir: Path | None,
    inherited_env: Mapping[str, str],
    timeout_seconds: float = _PROBE_TIMEOUT_SECONDS,
) -> tuple[int, bool, frozenset[str], frozenset[str]]:
    """One dial: count, write capability, and observed/read-only tool names.

    ``timeout_seconds`` defaults to the standalone per-dial timeout; the
    retry wrapper clamps it to the budget remaining so a hanging connector
    cannot run past the deadline.
    """

    count = 0
    has_potential_write = False
    observed_tools: set[str] = set()
    readonly_tools: set[str] = set()
    with anyio.fail_after(timeout_seconds):
        async with _server_streams(
            config, plugin_dir=plugin_dir, inherited_env=inherited_env
        ) as (read_stream, write_stream):
            async with ClientSession(
                read_stream,
                write_stream,
                read_timeout_seconds=timeout_seconds,
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
                        raise _ProbeConfigurationError(
                            "MCP server returned a nonconforming tool name"
                        )
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


async def _probe_server(
    config: Mapping[str, Any],
    *,
    tool_prefix: str,
    plugin_dir: Path | None,
    inherited_env: Mapping[str, str],
    attempts: int = _PROBE_ATTEMPTS,
) -> tuple[int, bool, frozenset[str], frozenset[str]]:
    """Dial one server, retrying a transient failure with bounded backoff (#2945).

    A connector that is merely not up yet -- a rollout window -- is not a
    capability failure, so a failed dial is retried under the bounded attempt
    count and the wall-clock budget before ``_ProbeFailure`` (carrying the
    dials actually made) escapes and the caller records ``probe_failed``.
    Each attempt's timeout is clamped to the budget remaining from the first
    dial, and no further attempt starts once that budget is spent, so the
    sequence stays under ``_PROBE_RETRY_BUDGET_SECONDS`` however the failures
    mix fast refusals with hangs. A deterministic misconfiguration
    (``_ProbeConfigurationError``, unwrapped from any transport exception
    group) is re-raised on the first dial: another dial cannot change the
    declaration. Cancellation is a BaseException in both anyio backends and
    propagates untouched. ``attempts=1`` reproduces the pre-retry single dial,
    which the turn-start re-dial (#2634) uses to keep its per-turn budget
    unchanged.
    """

    started = time.monotonic()
    dialed = 0
    last_error: BaseException | None = None
    while dialed < attempts:
        remaining = _PROBE_RETRY_BUDGET_SECONDS - (time.monotonic() - started)
        if dialed and remaining <= 0:
            # The budget is spent; one more dial could only start past the
            # bound, so the failure stands with the dials already made.
            assert last_error is not None
            raise _ProbeFailure(dialed, last_error) from last_error
        timeout = min(_PROBE_TIMEOUT_SECONDS, remaining)
        dialed += 1
        try:
            return await _probe_server_once(
                config,
                tool_prefix=tool_prefix,
                plugin_dir=plugin_dir,
                inherited_env=inherited_env,
                timeout_seconds=timeout,
            )
        except Exception as exc:  # noqa: BLE001 - any dial failure is retryable
            last_error = exc
            leaves = _probe_error_leaves(exc)
            if leaves and all(
                isinstance(leaf, _ProbeConfigurationError) for leaf in leaves
            ):
                raise leaves[0] from exc
            if dialed >= attempts:
                raise _ProbeFailure(dialed, exc) from exc
            backoff = _PROBE_RETRY_BACKOFF_SECONDS * 2 ** (dialed - 1)
            if time.monotonic() - started + backoff > _PROBE_RETRY_BUDGET_SECONDS:
                raise _ProbeFailure(dialed, exc) from exc
            # Error class only: the exception text can carry a header value
            # (#2634 leak discipline).
            logger.warning(
                "MCP capability probe attempt %d/%d failed error_class=%s; "
                "retrying in %.1fs",
                dialed,
                attempts,
                type(exc).__name__,
                backoff,
            )
            await anyio.sleep(backoff)
    raise AssertionError("unreachable: the loop always returns or raises")


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
        except _ProbeConfigurationError as exc:
            failures.append(name)
            logger.warning(
                "MCP tool-capability probe rejected server=%s; "
                "keeping approval tool: %s",
                name,
                exc,
            )
            if derived and name not in skip_http:
                connector_failures.append(
                    ConnectorCapabilityFailure(
                        connector=name,
                        credential_names=_header_placeholders(config),
                        reason="probe_misconfigured",
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
                connector_failures.append(
                    ConnectorCapabilityFailure(
                        connector=name,
                        credential_names=_header_placeholders(config),
                        reason="probe_failed",
                        # The real dial count from the retry wrapper; a plain
                        # exception (patched fakes) keeps the single dial it
                        # reports.
                        attempts=exc.attempts if isinstance(exc, _ProbeFailure) else 1,
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
    literal empty Bearer and #2519 forbids sending it to ``/mcp``. A
    ``probe_misconfigured`` failure is deterministic too (#2945): the
    declaration and the served tool names cannot change into a different
    answer from this process, so it is carried through like an expansion
    failure -- a server-side fix picked up by a connector rollout needs a
    runner restart.

    Each re-dial is a single attempt (``attempts=1``, #2945): the boot
    probe's bounded retry is a boot-budget affordance, and the turn-start
    re-dial stays inside the session's recovery budget. A re-probe that
    still fails keeps the prior failure, credential names and boot attempt
    count included (the materialized config no longer carries the
    placeholder names). The exception is logged by class only; its text can
    carry a header value.
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
                attempts=1,
            )
        except Exception as exc:  # noqa: BLE001 - any probe failure keeps the exclusion
            # ``_ProbeFailure`` wraps the last dial error; log that class so
            # the turn-time record keeps the real network error, and never
            # the text, which can carry a header value.
            cause = exc.last_error if isinstance(exc, _ProbeFailure) else exc
            logger.warning(
                "connector re-probe failed server=%s error_class=%s",
                failure.connector,
                type(cause).__name__,
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
