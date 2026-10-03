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
import secrets
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

import anyio.from_thread
from mcp.server.mcpserver import Context, MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from curie_e2e_connector.build import (
    DEFAULT_SOURCE_HOSTS,
    BuildConfig,
    BuildRequest,
    build_image,
)
from curie_e2e_connector.contract import (
    BUILD_CACHE_MOUNT,
    DEFAULT_BUILDER_IMAGE,
    DEFAULT_GIT_IMAGE,
    DEFAULT_PUSH_IMAGE,
    KUBECONFIG_MOUNT,
    REFUSAL_MISCONFIGURED,
    REGISTRY_PUSH_MOUNT,
    RUN_HEADER,
    RUN_TIMEOUT_S,
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
from curie_e2e_connector.registry import (
    DockerRegistry,
    RegistrySettings,
    parse_docker_config,
    registry_client,
)
from curie_e2e_connector.workload import deploy as deploy_manifests
from curie_e2e_connector.workload import list_events, read_logs, run_command

mcp = MCPServer("e2e")

_WRITE = ToolAnnotations(
    read_only_hint=False,
    destructive_hint=True,
    idempotent_hint=True,
    open_world_hint=True,
)

_READ = ToolAnnotations(
    read_only_hint=True,
    destructive_hint=False,
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


def _env(name: str) -> str:
    return os.environ.get(name, "").strip()


def _env_list(name: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in _env(name).split(",") if item.strip())


def _env_bool(name: str) -> bool:
    value = _env(name).lower()
    if value in ("", "0", "false", "no"):
        return False
    if value in ("1", "true", "yes"):
        return True
    raise ToolError(f"{REFUSAL_MISCONFIGURED}: {name} is not a boolean")


def registry_settings_from_env() -> RegistrySettings:
    return RegistrySettings(
        prefix=_env("E2E_REGISTRY"),
        insecure=_env_bool("E2E_REGISTRY_INSECURE"),
        token_hosts=_env_list("E2E_REGISTRY_TOKEN_HOSTS"),
    )


def build_config_from_env() -> BuildConfig:
    try:
        timeout = int(_env("E2E_BUILD_TIMEOUT_SECONDS") or "1200")
    except ValueError as exc:
        raise ToolError(
            f"{REFUSAL_MISCONFIGURED}: E2E_BUILD_TIMEOUT_SECONDS is not an integer"
        ) from exc
    config = BuildConfig(
        registry=registry_settings_from_env(),
        cache_repo=_env("E2E_BUILD_CACHE_REPO").rstrip("/"),
        builder_image=_env("E2E_BUILDER_IMAGE") or DEFAULT_BUILDER_IMAGE,
        git_image=_env("E2E_GIT_IMAGE") or DEFAULT_GIT_IMAGE,
        push_image=_env("E2E_PUSH_IMAGE") or DEFAULT_PUSH_IMAGE,
        timeout_seconds=timeout,
        source_hosts=_env_list("E2E_SOURCE_HOSTS") or DEFAULT_SOURCE_HOSTS,
    )
    config.validate()
    return config


def _optional_file(path: str) -> str | None:
    """The contents of an optional connector secret file, or None when it is not mounted."""

    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read()
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError) as exc:
        raise ToolError(f"{REFUSAL_MISCONFIGURED}: could not read {path}") from exc


def registry_from_env() -> DockerRegistry:
    timeout = float(os.environ.get("E2E_REGISTRY_TIMEOUT_SECONDS", "30"))
    return DockerRegistry(
        parse_docker_config(_optional_file(REGISTRY_PUSH_MOUNT)),
        settings=registry_settings_from_env(),
        client=registry_client(timeout),
    )


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
    result = destroy_environment(
        cluster_from_env(),
        install_from_env(),
        caller,
        namespace,
        registry=registry_from_env(),
        now=lambda: datetime.now(UTC),
        sleep=time.sleep,
    )
    return _reply(result)


@mcp.tool(annotations=_WRITE)
def image_build(
    ctx: Context,
    context: str,
    repository: str,
    commit: str,
    dockerfile: str = "Dockerfile",
    platforms: list[str] | None = None,
    name: str = "app",
) -> str:
    """Build a container image from a published commit in this run's namespace.

    Contract: build a commit fetchable by SHA from an allowlisted https host.
    ``repository`` is the https clone URL (default allowlist: github.com,
    public repositories only) and ``commit`` is the full 40 character SHA.
    ``context`` and ``dockerfile`` are paths relative to the repository root;
    ``platforms`` takes at most one entry such as ``linux/arm64``; ``name`` is
    the image name inside this run's registry namespace.

    A commit that exists only in your sandbox workspace cannot be built yet:
    push or publish it first. The build runs as a Job in the namespace
    env_create returned, and you never receive a registry credential.

    Result: ``{"images": [{"name": "<registry>/<namespace>/<name>",
    "digest": "sha256:<hex>"}]}``. Deploy by digest.
    """

    caller = caller_from_headers(ctx.headers)
    config = build_config_from_env()
    request = BuildRequest.parse(
        context,
        dockerfile,
        platforms,
        repository,
        commit,
        name,
        source_hosts=config.source_hosts,
    )
    total = float(config.timeout_seconds)

    def on_poll(elapsed: float) -> None:
        # Sync tools run in an anyio worker thread, so progress hops back to
        # the event loop. Progress is best effort and never fails the build.
        try:
            anyio.from_thread.run(ctx.report_progress, min(elapsed, total), total)
        except Exception:  # noqa: BLE001
            pass

    result = build_image(
        cluster_from_env(),
        install_from_env(),
        config,
        caller,
        request,
        push_config_text=_optional_file(REGISTRY_PUSH_MOUNT),
        cache_config_text=_optional_file(BUILD_CACHE_MOUNT),
        clock=time.monotonic,
        sleep=time.sleep,
        build_id=secrets.token_hex(4),
        on_poll=on_poll,
    )
    return _reply(result)


@mcp.tool(annotations=_WRITE)
def deploy(ctx: Context, manifests: str) -> str:
    """Apply Kubernetes manifests in this run's namespace.

    ``manifests`` is YAML: one or more documents, or a ``kind: List``. Every
    object lands in the namespace env_create returned, labelled with this run.
    An object that already exists is replaced. Every container image must be
    pinned by digest (``<ref>@sha256:<hex>``, as image_build returns it).

    Result: ``{"namespace": "<name>", "applied": [{"kind", "name"}]}``.

    A refusal applies nothing. Cluster scoped objects (CustomResourceDefinition,
    ClusterRole, ClusterRoleBinding, webhooks, PriorityClass, Namespace and
    any other kind the cluster serves outside a namespace) are refused with
    ``e2e_cluster_scoped_object``: that change cannot be proven here and needs
    CI only proof. NetworkPolicy, ResourceQuota, LimitRange, the build's
    objects are refused too. A Role may grant only get, list and watch, never
    on secrets or pods (including pods subresources); a RoleBinding may name
    only such a Role.
    """

    caller = caller_from_headers(ctx.headers)
    result = deploy_manifests(cluster_from_env(), install_from_env(), caller, manifests)
    return _reply(result)


@mcp.tool(annotations=_WRITE)
def run(ctx: Context, command: list[str], image: str) -> str:
    """Run one command to completion as a Job in this run's namespace.

    ``command`` is the argv, 1 to 64 strings; use ``["sh", "-c", "..."]`` for
    a shell line. ``image`` must be pinned by digest. The Job gets no service
    account token, never retries, and stops after 1200 seconds.

    Result: ``{"exit_code": <int>, "stdout": "...", "stderr": "..."}``.
    Kubernetes merges stdout and stderr into one log, so ``stdout`` is that
    log (its last 64 KiB) and ``stderr`` is the termination message or reason,
    such as ``OOMKilled``. A non zero exit is a result, not an error.
    """

    caller = caller_from_headers(ctx.headers)
    total = float(RUN_TIMEOUT_S)

    def on_poll(elapsed: float) -> None:
        # Progress is best effort and never fails the run.
        try:
            anyio.from_thread.run(ctx.report_progress, min(elapsed, total), total)
        except Exception:  # noqa: BLE001
            pass

    result = run_command(
        cluster_from_env(),
        install_from_env(),
        caller,
        command,
        image,
        clock=time.monotonic,
        sleep=time.sleep,
        run_id=secrets.token_hex(4),
        on_poll=on_poll,
    )
    return _reply(result)


@mcp.tool(annotations=_READ)
def logs(ctx: Context, pod: str, container: str | None = None, tail: int | None = None) -> str:
    """Read the last lines of a pod's log in this run's namespace.

    ``container`` picks one container of a multi container pod. ``tail`` is
    the line count, 1 to 5000 (default 500). Result: ``{"logs": "..."}``, at
    most the last 64 KiB. image_build pods are refused; image_build reports
    their failures itself.
    """

    caller = caller_from_headers(ctx.headers)
    result = read_logs(cluster_from_env(), install_from_env(), caller, pod, container, tail)
    return _reply(result)


@mcp.tool(annotations=_READ)
def events(ctx: Context, involved: str | None = None) -> str:
    """List recent Kubernetes events in this run's namespace, oldest first.

    ``involved`` limits them to one object by name, such as a pod. Result:
    ``{"events": [{"reason", "message", "type", "count"}]}``, at most 200.
    Use it when a deploy applied but its pods never became ready.
    """

    caller = caller_from_headers(ctx.headers)
    result = list_events(cluster_from_env(), install_from_env(), caller, involved)
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
