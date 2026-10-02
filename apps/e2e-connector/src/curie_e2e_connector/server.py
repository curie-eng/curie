"""MCP server for the platform end to end connector.

The sandbox receives the tool names and the Service URL. The kubeconfig stays
in this process. Run and work item come from the caller proxy headers, which
the proxy sets only after it verifies the signed token (ADR 0178).
"""

# NOTE: no `from __future__ import annotations`. MCPServer introspects tool
# signatures with issubclass(annotation, Context), and stringized annotations
# raise TypeError at import time.

import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from curie_e2e_connector.contract import (
    KUBECONFIG_MOUNT,
    REFUSAL_MISCONFIGURED,
    RUN_HEADER,
    WORK_ITEM_HEADER,
)
from curie_e2e_connector.kube import ClusterApi, HttpxCluster, client_from_kubeconfig
from curie_e2e_connector.namespace import (
    Caller,
    Install,
    create_environment,
    destroy_environment,
    require_caller,
)

mcp = MCPServer("e2e")

_WRITE = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=True,
    idempotent_hint=True,
    open_world_hint=True,
)


def _header(headers: Mapping[str, str] | None, name: str) -> str:
    if not headers:
        return ""
    wanted = name.lower()
    for key, value in headers.items():
        if key.lower() == wanted:
            return value.strip()
    return ""


def caller_from_headers(headers: Mapping[str, str] | None) -> Caller:
    return require_caller(_header(headers, RUN_HEADER), _header(headers, WORK_ITEM_HEADER))


def install_from_env() -> Install:
    def need(name: str) -> str:
        value = os.environ.get(name, "").strip()
        if not value:
            raise ToolError(f"{REFUSAL_MISCONFIGURED}: {name} is empty")
        return value

    try:
        ttl = int(os.environ.get("E2E_TTL_SECONDS", "3600"))
    except ValueError as exc:
        raise ToolError(f"{REFUSAL_MISCONFIGURED}: E2E_TTL_SECONDS is not an integer") from exc
    return Install(
        namespace_prefix=need("E2E_NAMESPACE_PREFIX"),
        owner_label_key=need("E2E_OWNER_LABEL_KEY"),
        owner_label_value=need("E2E_OWNER_LABEL_VALUE"),
        service_account=need("E2E_SERVICE_ACCOUNT"),
        service_account_namespace=need("E2E_SERVICE_ACCOUNT_NAMESPACE"),
        worker_cluster_role=need("E2E_WORKER_CLUSTER_ROLE"),
        ttl_seconds=ttl,
        pod_security=os.environ.get("E2E_POD_SECURITY", "baseline").strip() or "baseline",
    )


def cluster_from_env() -> ClusterApi:
    path = os.environ.get("E2E_KUBECONFIG", KUBECONFIG_MOUNT)
    timeout = float(os.environ.get("E2E_KUBE_TIMEOUT_SECONDS", "30"))
    return HttpxCluster(client_from_kubeconfig(path, timeout))


def _reply(payload: dict[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True)


@mcp.tool(annotations=_WRITE)
def env_create(
    ctx: Context,
    allow: list[dict[str, Any]] | None = None,
    ttl_seconds: int | None = None,
) -> str:
    """Create this run's namespace on the test cluster.

    The namespace name is the connector prefix plus this run's id. It is
    labelled with the run and the work item from the signed caller token.
    The call also installs a ResourceQuota, a LimitRange, a default deny
    NetworkPolicy, and a TTL annotation. ``allow`` adds egress rules and
    nothing else: each item is ``{"cidr", "port", "protocol"}`` or
    ``{"dns": true}``.

    You do not receive a kubeconfig. Later tools in this connector act only
    inside the namespace this call returns.
    """

    caller = caller_from_headers(ctx.headers)
    result = create_environment(
        cluster_from_env(),
        install_from_env(),
        caller,
        allow,
        ttl_seconds,
        now=datetime.now(UTC),
    )
    return _reply(result)


@mcp.tool(annotations=_WRITE)
def env_destroy(ctx: Context, namespace: str) -> str:
    """Delete this run's namespace.

    Refuses any namespace whose name or run label is not the signed run.
    A refusal deletes nothing.
    """

    caller = caller_from_headers(ctx.headers)
    result = destroy_environment(cluster_from_env(), install_from_env(), caller, namespace)
    return _reply(result)


def main() -> None:
    mcp.run(
        transport="streamable-http",
        host=os.environ.get("BIND_ADDRESS", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
        streamable_http_path="/mcp",
    )


if __name__ == "__main__":
    main()
