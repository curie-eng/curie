"""Decide whether a bundle's end to end connector may render (ADR 0176).

The connector package is not imported here. The API image must not carry the
MCP server, and the names below are checked against
``tests/vectors/e2e-connector-sandbox.json``.
"""

from __future__ import annotations

from dataclasses import dataclass

from plugin_format.connectors import ConnectorsFile, ConnectorSpec

CONNECTOR_NAME = "e2e"
SENTINEL_IMAGE = "curie-e2e-connector"
KUBECONFIG_SECRET = "E2E_CLUSTER_KUBECONFIG"
KUBECONFIG_MOUNT = "/secrets/kubeconfig"
REGISTRY_PUSH_SECRET = "E2E_REGISTRY_PUSH_CONFIG"
REGISTRY_PUSH_MOUNT = "/secrets/registry/config.json"
BUILD_CACHE_SECRET = "E2E_BUILD_CACHE_CONFIG"
BUILD_CACHE_MOUNT = "/secrets/registry-cache/config.json"
WITHHELD_FROM_SANDBOX = (KUBECONFIG_SECRET, REGISTRY_PUSH_SECRET, BUILD_CACHE_SECRET)
SERVER_MODULE = "curie_e2e_connector"
REFUSAL_NOT_CONFIGURED = "e2e_connector_not_configured"
REFUSAL_MISCONFIGURED = "e2e_connector_misconfigured"

PLATFORM_ENV = (
    "E2E_KUBECONFIG",
    "E2E_NAMESPACE_PREFIX",
    "E2E_OWNER_LABEL_KEY",
    "E2E_OWNER_LABEL_VALUE",
    "E2E_SERVICE_ACCOUNT",
    "E2E_SERVICE_ACCOUNT_NAMESPACE",
    "E2E_WORKER_CLUSTER_ROLE",
    "E2E_TTL_SECONDS",
    "E2E_POD_SECURITY",
    "E2E_REGISTRY",
    "E2E_BUILD_CACHE_REPO",
    "E2E_REGISTRY_INSECURE",
    "E2E_REGISTRY_TOKEN_HOSTS",
    "E2E_BUILDER_IMAGE",
    "E2E_GIT_IMAGE",
    "E2E_PUSH_IMAGE",
    "E2E_BUILD_TIMEOUT_SECONDS",
    "E2E_SOURCE_HOSTS",
    "PORT",
)


@dataclass(frozen=True)
class E2EInstall:
    """What the factory release knows about its separate test cluster."""

    enabled: bool = False
    image: str = ""
    namespace_prefix: str = "curie-e2e-"
    owner_label_key: str = "curietech.ai/e2e-owner"
    owner_label_value: str = ""
    service_account: str = ""
    service_account_namespace: str = ""
    worker_cluster_role: str = ""
    ttl_seconds: int = 3600
    pod_security: str = "baseline"
    registry: str = ""
    build_cache_repo: str = ""
    registry_insecure: bool = False
    registry_token_hosts: str = ""
    builder_image: str = ""
    git_image: str = ""
    push_image: str = ""
    build_timeout_seconds: int = 1200
    source_hosts: str = "github.com"
    port: int = 8000

    def platform_env(self) -> dict[str, str]:
        return {
            "E2E_KUBECONFIG": KUBECONFIG_MOUNT,
            "E2E_NAMESPACE_PREFIX": self.namespace_prefix,
            "E2E_OWNER_LABEL_KEY": self.owner_label_key,
            "E2E_OWNER_LABEL_VALUE": self.owner_label_value,
            "E2E_SERVICE_ACCOUNT": self.service_account,
            "E2E_SERVICE_ACCOUNT_NAMESPACE": self.service_account_namespace,
            "E2E_WORKER_CLUSTER_ROLE": self.worker_cluster_role,
            "E2E_TTL_SECONDS": str(self.ttl_seconds),
            "E2E_POD_SECURITY": self.pod_security,
            "E2E_REGISTRY": self.registry,
            "E2E_BUILD_CACHE_REPO": self.build_cache_repo,
            "E2E_REGISTRY_INSECURE": "true" if self.registry_insecure else "false",
            "E2E_REGISTRY_TOKEN_HOSTS": self.registry_token_hosts,
            "E2E_BUILDER_IMAGE": self.builder_image,
            "E2E_GIT_IMAGE": self.git_image,
            "E2E_PUSH_IMAGE": self.push_image,
            "E2E_BUILD_TIMEOUT_SECONDS": str(self.build_timeout_seconds),
            "E2E_SOURCE_HOSTS": self.source_hosts,
            "PORT": str(self.port),
        }


def prepare_connectors(connectors: ConnectorsFile, install: E2EInstall) -> ConnectorsFile:
    """Return the file to render, or refuse a declared connector this install cannot host."""

    spec = connectors.connectors.get(CONNECTOR_NAME)
    if spec is None:
        return connectors
    if not install.enabled:
        raise ValueError(
            f"{REFUSAL_NOT_CONFIGURED}: this installation has no test cluster configured, "
            "so it renders no end to end connector. Remove the e2e connector from "
            "connectors.yaml, or enable e2eConnector on the factory release and store "
            "E2E_CLUSTER_KUBECONFIG with `curie secrets`."
        )
    missing = [
        name
        for name, value in (
            ("image", install.image),
            ("namespace prefix", install.namespace_prefix),
            ("owner label key", install.owner_label_key),
            ("owner label value", install.owner_label_value),
            ("service account", install.service_account),
            ("service account namespace", install.service_account_namespace),
            ("worker cluster role", install.worker_cluster_role),
        )
        if not str(value).strip()
    ]
    bad_bounds = (
        install.ttl_seconds < 60
        or install.pod_security not in ("baseline", "restricted")
        or not 60 <= install.build_timeout_seconds <= 3600
    )
    if missing or bad_bounds:
        detail = ", ".join(missing) if missing else "ttl or pod security"
        raise ValueError(f"{REFUSAL_MISCONFIGURED}: {detail}")
    if not spec.is_hosted:
        raise ValueError(
            f"{REFUSAL_NOT_CONFIGURED}: the e2e connector must be a hosted connector "
            f"with image {SENTINEL_IMAGE}"
        )
    files = dict(spec.secret_files)
    files[KUBECONFIG_SECRET] = KUBECONFIG_MOUNT
    declared = {item if isinstance(item, str) else item.name for item in spec.secrets} | set(files)
    # A push or cache config mounts only when the bundle declares it, and only
    # at its fixed path, so the connector never reads a bundle chosen location.
    for name, mount in (
        (REGISTRY_PUSH_SECRET, REGISTRY_PUSH_MOUNT),
        (BUILD_CACHE_SECRET, BUILD_CACHE_MOUNT),
    ):
        if name in declared:
            files[name] = mount
    secrets = [
        item
        for item in spec.secrets
        if (item if isinstance(item, str) else item.name) not in WITHHELD_FROM_SANDBOX
    ]
    env = {key: value for key, value in spec.env.items() if key not in PLATFORM_ENV}
    env.update(install.platform_env())
    prepared = spec.model_copy(
        update={
            "image": install.image,
            "args": [],
            "secret_files": files,
            "secrets": secrets,
            "env": env,
            "unhosted_url": None,
            "port": install.port,
        }
    )
    updated = dict(connectors.connectors)
    updated[CONNECTOR_NAME] = prepared
    return connectors.model_copy(update={"connectors": updated})


def pin_server_command(objects: list[dict[str, object]], install: E2EInstall) -> None:
    """Point the e2e server container at the platform module in the worker image."""

    command = ["python", "-m", SERVER_MODULE]
    for obj in objects:
        if obj.get("kind") != "Deployment":
            continue
        metadata = obj.get("metadata")
        name = metadata.get("name") if isinstance(metadata, dict) else ""
        if not isinstance(name, str) or not name.endswith(f"-mcp-{CONNECTOR_NAME}"):
            continue
        spec = obj.get("spec")
        if not isinstance(spec, dict):
            continue
        template = spec.get("template")
        if not isinstance(template, dict):
            continue
        pod = template.get("spec")
        if not isinstance(pod, dict):
            continue
        containers = pod.get("containers")
        if not isinstance(containers, list):
            continue
        for container in containers:
            if isinstance(container, dict) and container.get("name") == "server":
                container["command"] = command
                container["args"] = []
                container["image"] = install.image


def declared_e2e(connectors: ConnectorsFile) -> ConnectorSpec | None:
    return connectors.connectors.get(CONNECTOR_NAME)
