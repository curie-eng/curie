"""Mount the connectors a bundle declared, so the author writes no URL.

ADR-0086 decided the bundle declares connectors and Curie derives, among other
things, "the URL it writes into the agent's MCP configuration". This is that
write. The author declares intent in ``connectors.yaml``; the URL of the Service
Curie created never appears in the repository.

That matters because the URL is unknowable to the author. It is
``<release>-<agent>-mcp-<connector>.<namespace>.svc.cluster.local``, and the
release name, the agent name, and the namespace are all install-time facts
assigned by whoever ran ``cluster up`` and ``cluster deploy``. Since #1116 the
name is agent-scoped, so a hand-written URL is now *guaranteed* wrong for at
least one of two agents sharing a bundle.

Derivation reuses ``plugin_format.connector_render.mcp_entry`` -- the same
function the API calls when it renders the Service. One function, so the URL the
agent dials and the Service that exists cannot disagree. Re-deriving the format
here would be a second copy of that rule, free to drift, and the failure would
be a connection refused at turn time.

The entries ride ``ClaudeAgentOptions.mcp_servers``, alongside Curie's approval
and state servers -- platform-supplied servers, which is exactly what these are.
Nothing rewrites ``.mcp.json``; the bundle stays the read-only artifact that was
deployed.

Name collisions are rejected at validation on both axes: ``plugin_format``
rejects a bundle that declares one name in both ``connectors.yaml`` and its own
MCP config (#1118), and it rejects a name matching one of Curie's own platform
servers -- ``curie``, ``curie-state`` -- via
``plugin_format.connectors.RESERVED_CONNECTOR_NAMES`` (#1200). Precedence is
nevertheless resolved here, by ``build_mcp_servers``, deliberately toward the
platform, so a residual collision that somehow reaches the runtime fails safe
rather than silently displacing the approval server.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Collection, MutableMapping
from pathlib import Path
from typing import Any

import yaml
from aci_protocol import BootEnv
from plugin_format.connector_render import (
    AmbiguousObjectName,
    mcp_entry,
    unhosted_mcp_entry,
)
from plugin_format.connectors import CONNECTORS_FILE, ConnectorsFile, validate_connectors
from plugin_format.yaml_loader import safe_load_unique

logger = logging.getLogger(__name__)


def _read(plugin_dir: str | Path) -> ConnectorsFile | None:
    path = Path(plugin_dir) / CONNECTORS_FILE
    if not path.is_file():
        return None
    try:
        data = safe_load_unique(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        # Deploy already validated this file, so reaching here means the bundle
        # changed underneath us. Log and mount nothing rather than fail the
        # turn: the agent loses the connector's tools, which is visible, where a
        # crashed boot loses the whole session.
        logger.warning("connectors.yaml unreadable, mounting no connectors: %s", exc)
        return None
    parsed, errors = validate_connectors(data)
    if errors or parsed is None:
        logger.warning(
            "connectors.yaml did not validate, mounting no connectors: %s",
            "; ".join(code for code, _ in errors),
        )
        return None
    return parsed


def derive_mcp_servers(
    plugin_dir: str | Path | None,
    *,
    release: str | None,
    agent: str | None,
    namespace: str | None,
) -> dict[str, Any]:
    """The MCP server entries for this bundle's declared connectors.

    Empty when the bundle declares none, or when the connector scope is absent.
    An absent scope is not a degraded boot: the ``skill`` tier hosts nothing, so
    there is no Service to point at and a declared connector is correctly not
    exercisable there (#1093). A hosted connector declared with no scope is
    logged, because on a cluster that combination means the worker stopped
    sending the scope and the agent has silently lost its tools.
    """

    if plugin_dir is None:
        return {}
    declared = _read(plugin_dir)
    if declared is None or not declared.connectors:
        return {}

    if not (release and agent and namespace):
        # The scope is emitted as a set or not at all (BootEnv, ACI 0.2.8), so
        # this is "no scope", never a partial one.
        stranded = sorted(
            name
            for name, spec in declared.connectors.items()
            if spec.is_hosted and not spec.unhosted_url
        )
        if stranded:
            logger.info(
                "no connector scope on the boot env and no unhosted_url; these are "
                "declared but not exercisable in this tier (%s)",
                ", ".join(stranded),
            )
        # A remote connector carries its own absolute url, and a hosted one may
        # declare where to reach it when Curie is not hosting it (#1160). Both
        # stay mountable here; a hosted connector with neither mounts nothing,
        # which is the honest answer rather than a URL resolving nowhere.
        entries = {}
        for name, spec in sorted(declared.connectors.items()):
            entry = unhosted_mcp_entry(spec)
            if entry is not None:
                entries[name] = entry
        return entries

    try:
        return {
            name: mcp_entry(release, agent, namespace, name, spec)
            for name, spec in sorted(declared.connectors.items())
        }
    except AmbiguousObjectName as exc:
        # The same trade `_read` makes above, for the same reason. The CONNECTOR
        # half of this rule is already fail-soft here, because `_read` runs
        # `validate_connectors` and returns None on any error. The AGENT half
        # has no such path: the name arrives on the boot env, is never validated
        # in this module, and goes straight into the comprehension above, where
        # `object_name` fails closed on a name that forges its `-mcp-` join
        # (#1446). Uncaught, that crashes the boot -- and a crashed boot loses
        # the whole session, for every turn, until someone reads a stack trace
        # out of a sandbox pod's logs. Mounting nothing costs the agent this
        # connector's tools, which is visible in its tool list and fixed by
        # recreating the agent under a non-forging name.
        #
        # Narrow on purpose: `AmbiguousObjectName`, never a bare `ValueError`.
        # Widening it would turn an unrelated programming error into a silent
        # "mounted no connectors", which is the exact class of silent failure
        # #1446 is about.
        logger.warning(
            "agent name %r forges the connector object-name join, mounting no connectors: %s",
            agent,
            exc,
        )
        return {}


def build_mcp_servers(platform: dict[str, Any], derived: dict[str, Any]) -> dict[str, Any]:
    """Merge the platform's own MCP servers with the bundle's, platform winning.

    The order is the safety property, not a style choice (#1200). Both maps are
    plain dict keys on one channel, so on a collision somebody wins. Losing a
    connector is a visible, diagnosable failure -- the agent's tool list is
    missing something the bundle declared. Losing ``request_approval`` is a
    silent one: a skill that calls it fails, or a self-imposed approval never
    raises and the turn proceeds ungated. So the platform key wins.
    """

    return {**derived, **platform}


_BEARER_PLACEHOLDER = re.compile(r"^Bearer \$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def materialize_hosted_bearer_headers(
    servers: dict[str, Any],
    env: MutableMapping[str, str],
) -> frozenset[str]:
    """Expand hosted ``Bearer ${NAME}`` headers in memory and drop NAME from env.

    ``derive_mcp_servers`` / ``mcp_entry`` keep the placeholder so a value never
    lands on disk. The MCP client still needs the real header, so the runner
    expands it here and unsets the name before the SDK session (and Bash) can
    read the process environment (#2559). Names that were never in ``env`` stay
    as the placeholder (the empty-Bearer gap, #2519). Unrelated secrets are
    left in ``env`` so ADR-0009 stdio / remote ``${VAR}`` expansion still works.
    """

    dropped: set[str] = set()
    for server in servers.values():
        if not isinstance(server, dict):
            continue
        headers = server.get("headers")
        if not isinstance(headers, dict):
            continue
        auth = headers.get("Authorization")
        if not isinstance(auth, str):
            continue
        match = _BEARER_PLACEHOLDER.fullmatch(auth)
        if match is None:
            continue
        name = match.group(1)
        value = env.get(name)
        if value is None:
            continue
        headers["Authorization"] = f"Bearer {value}"
        dropped.add(name)
    drop_connector_secret_names(env, dropped)
    return frozenset(dropped)


def drop_connector_secret_names(env: MutableMapping[str, str], names: Collection[str]) -> None:
    """Remove hosted Bearer names from a process or spawn env mapping."""

    for name in names:
        env.pop(name, None)
    marker = BootEnv.env_key("connector_secret_keys")
    raw = env.get(marker)
    if raw and names:
        remaining = [key for key in raw.split(",") if key and key not in names]
        if remaining:
            env[marker] = ",".join(sorted(remaining))
        else:
            env.pop(marker, None)
