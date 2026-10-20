"""Pure shared application manifests and independently owned member reach.

This contract does not activate a workload. A future applier must preserve
application ownership instead of stamping dedicated connector ownership.
Sandbox writers must supply immutable agent UUID labels before activation;
the root must also supply DNS, provider, grant-store and data-tier policies.
The database egress rendered here is only one additive network-policy grant.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import unicodedata
from copy import deepcopy
from decimal import Decimal, DecimalException
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, field_validator, model_validator

from .connectors import parse_quantity
from .reserved_env import SECRET_NAME_RE, is_reserved_boot_env_name

__all__ = [
    "DatabaseTarget",
    "SecretKeyRef",
    "SharedApplication",
    "SharedMember",
    "render_application",
    "render_member_reach",
]

_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_LABEL_PART = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?")
_IMAGE_PATH_PART = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
_IMAGE = re.compile(
    rf"(?:[a-z0-9]+(?:[.-][a-z0-9]+)*(?::[0-9]{{1,5}})?/)?"
    rf"{_IMAGE_PATH_PART}(?:/{_IMAGE_PATH_PART})*@sha256:[0-9a-f]{{64}}"
)
_APPLICATION_LABEL = "curietech.ai/shared-application"
_INSTALLATION_LABEL = "curietech.ai/shared-installation"
_AGENT_LABEL = "curietech.ai/agent-id"
_COMPONENT = "shared-connector-application"
_PROXY_SECRET_DIR = PurePosixPath("/run/curie-shared-proxy")
_SNAPSHOT_DIR = PurePosixPath("/run/curie-shared-snapshot")
_PROXY_DEFAULTS = {
    "requests": {"cpu": "25m", "memory": "128Mi"},
    "limits": {"cpu": "1000m", "memory": "512Mi"},
}
_SERVER_DEFAULTS = {
    "requests": {"cpu": "10m", "memory": "64Mi"},
    "limits": {"cpu": "500m", "memory": "256Mi"},
}


def _dns_label(value: str) -> str:
    if not _DNS_LABEL.fullmatch(value):
        raise ValueError("expected a DNS label of at most 63 characters")
    return value


def _dns_subdomain(value: str) -> str:
    if len(value) > 253 or not all(_DNS_LABEL.fullmatch(part) for part in value.split(".")):
        raise ValueError("expected a DNS subdomain of at most 253 characters")
    return value


def _canonical_uuid(value: str) -> str:
    parsed = UUID(value)
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("expected a canonical lowercase non-nil UUID")
    return value


def _has_control(value: str) -> bool:
    return any(unicodedata.category(char) in {"Cc", "Cf", "Cs"} for char in value)


def _labels(value: dict[str, str]) -> dict[str, str]:
    for key, label in value.items():
        if key == "curie.dev/connector-owner":
            raise ValueError("dedicated connector ownership cannot select shared resources")
        prefix, separator, name = key.rpartition("/")
        if separator:
            _dns_subdomain(prefix)
        else:
            name = key
        if not _LABEL_PART.fullmatch(name) or (label and not _LABEL_PART.fullmatch(label)):
            raise ValueError("invalid Kubernetes label key or value")
    return value


def _origin(value: str) -> str:
    if any(char.isspace() for char in value) or _has_control(value):
        raise ValueError("provider origins cannot contain whitespace or controls")
    parsed = urlsplit(value)
    host = parsed.hostname
    if (
        not value.startswith("https://")
        or not host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or "?" in value
        or "#" in value
    ):
        raise ValueError("provider origins must be HTTPS origins without credentials or paths")
    if ":" in host:
        ipaddress.IPv6Address(host)
        authority = f"[{host}]"
    else:
        _dns_subdomain(host)
        authority = host
    port = parsed.port
    if port is not None:
        if not 1 <= port <= 65535:
            raise ValueError("origin port is out of range")
        authority += f":{port}"
    if parsed.netloc.lower() != authority.lower():
        raise ValueError("invalid provider origin authority")
    return value


def _resources(value: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    if value.keys() != {"requests", "limits"} or any(
        side.keys() != {"cpu", "memory"} for side in value.values()
    ):
        raise ValueError("resources require CPU and memory requests and limits")
    for resource in ("cpu", "memory"):
        quantities = []
        for side in ("requests", "limits"):
            text = value[side][resource]
            try:
                quantity = parse_quantity(text) if len(text) <= 64 else None
                if quantity is None or not quantity.is_finite() or quantity > 2**63 - 1:
                    raise ValueError("resources require positive bounded Kubernetes quantities")
                if resource == "cpu" and (
                    quantity < Decimal("0.001")
                    or quantity * 1000 != (quantity * 1000).to_integral_value()
                ):
                    raise ValueError("CPU quantities cannot be finer than one millicore")
            except DecimalException as exc:
                raise ValueError("invalid Kubernetes resource quantity") from exc
            quantities.append(quantity)
        if quantities[0] > quantities[1]:
            raise ValueError("resource requests cannot exceed limits")
    return value


_DnsLabel = Annotated[str, AfterValidator(_dns_label)]
_CanonicalUuid = Annotated[str, AfterValidator(_canonical_uuid)]
_Port = Annotated[int, Field(ge=1, le=65535)]
_LabelSelector = Annotated[dict[str, str], AfterValidator(_labels)]
_ResourceBounds = Annotated[dict[str, dict[str, str]], AfterValidator(_resources)]


class _Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        strict=True,
        frozen=True,
        validate_default=True,
        revalidate_instances="always",
    )


class SecretKeyRef(_Contract):
    """One required key projected from an existing Secret."""

    name: Annotated[str, AfterValidator(_dns_subdomain)]
    key: str

    @field_validator("key")
    @classmethod
    def valid_key(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_.-]{1,253}", value) or value in {".", ".."}:
            raise ValueError("expected a single Secret data key")
        return value


class DatabaseTarget(_Contract):
    """Exactly one namespaced pod selector or set of canonical CIDRs."""

    namespace: _DnsLabel | None = None
    pod_labels: _LabelSelector | None = None
    cidrs: tuple[str, ...] = ()
    port: _Port

    @field_validator("cidrs")
    @classmethod
    def valid_cidrs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for cidr in value:
            network = ipaddress.ip_network(cidr, strict=True)
            if network.prefixlen == 0 or str(network) != cidr:
                raise ValueError("database CIDRs must be canonical and cannot use /0")
        if len(set(value)) != len(value):
            raise ValueError("database CIDRs must be unique")
        return value

    @model_validator(mode="after")
    def exact_target(self) -> Self:
        if self.pod_labels is not None:
            if not self.pod_labels or self.namespace is None or self.cidrs:
                raise ValueError("database pod labels require a namespace and exclude CIDRs")
        elif self.namespace is not None or not self.cidrs:
            raise ValueError("database target requires namespaced pod labels or CIDRs")
        return self


class SharedApplication(_Contract):
    """Root-owned, immutable configuration with no member or alias state."""

    installation: str
    application: _CanonicalUuid
    release: _DnsLabel
    namespace: _DnsLabel
    app_name: _DnsLabel
    server_image: str
    proxy_image: str
    server_port: _Port = 8000
    proxy_port: _Port = 8480
    server_args: tuple[str, ...] = ()
    server_env: dict[str, str] = Field(default_factory=dict)
    resolver_socket: str = "/run/curie-resolver/resolver.sock"
    encryption_key: SecretKeyRef
    database_dsn: SecretKeyRef
    dsn_encoding: Literal["bundled-sealed", "managed-raw"]
    grant_store: SecretKeyRef
    provider_origins: tuple[str, ...]
    credential_slots: tuple[str, ...]
    database_target: DatabaseTarget
    snapshot_seal: SecretKeyRef | None = None
    server_resources: _ResourceBounds = Field(default_factory=lambda: deepcopy(_SERVER_DEFAULTS))
    proxy_resources: _ResourceBounds = Field(default_factory=lambda: deepcopy(_PROXY_DEFAULTS))

    @field_validator("installation")
    @classmethod
    def valid_installation(cls, value: str) -> str:
        if (
            not value
            or len(value.encode("utf-8")) > 128
            or any(char.isspace() for char in value)
            or _has_control(value)
        ):
            raise ValueError(
                "installation must be opaque UTF-8 within 128 bytes without whitespace"
            )
        return value

    @field_validator("server_image", "proxy_image")
    @classmethod
    def locked_image(cls, value: str) -> str:
        if not _IMAGE.fullmatch(value):
            raise ValueError("images require a repository and a lowercase sha256 digest")
        return value

    @field_validator("server_args")
    @classmethod
    def literal_args(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any("\x00" in item for item in value):
            raise ValueError("server arguments cannot contain NUL")
        return value

    @field_validator("server_env")
    @classmethod
    def literal_env(cls, value: dict[str, str]) -> dict[str, str]:
        for name, literal in value.items():
            if not SECRET_NAME_RE.fullmatch(name):
                raise ValueError("server environment keys must be environment variable names")
            if is_reserved_boot_env_name(name) or name.startswith("SHARED_"):
                raise ValueError("server environment cannot override platform or shared keys")
            if "\x00" in literal:
                raise ValueError("server environment values cannot contain NUL")
        return value

    @field_validator("resolver_socket")
    @classmethod
    def dedicated_socket(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            not path.is_absolute()
            or str(path) != value
            or ".." in path.parts
            or path.parent == PurePosixPath("/")
            or len(value.encode("utf-8")) > 107
            or any(char.isspace() for char in value)
            or _has_control(value)
        ):
            raise ValueError(
                "resolver socket requires a canonical absolute path in its own directory"
            )
        for secret_dir in (_PROXY_SECRET_DIR, _SNAPSHOT_DIR):
            if path.parent.is_relative_to(secret_dir) or secret_dir.is_relative_to(path.parent):
                raise ValueError("resolver socket directory must be separate from Secret mounts")
        return value

    @field_validator("provider_origins")
    @classmethod
    def valid_origins(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        for origin in value:
            _origin(origin)
        if len({origin.lower() for origin in value}) != len(value):
            raise ValueError("provider origins must be unique")
        return value

    @field_validator("credential_slots")
    @classmethod
    def valid_slots(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", slot) for slot in value):
            raise ValueError("credential slots must be simple identifiers")
        if len(set(value)) != len(value):
            raise ValueError("credential slots must be unique")
        return value

    @model_validator(mode="after")
    def isolated_inputs(self) -> Self:
        if self.server_port == self.proxy_port:
            raise ValueError("server and proxy ports must differ")
        if self.snapshot_seal is not None and self.snapshot_seal.name in {
            self.encryption_key.name,
            self.database_dsn.name,
            self.grant_store.name,
        }:
            raise ValueError("snapshot Secret name must differ from every proxy Secret name")
        return self


class SharedMember(_Contract):
    """An immutable member UUID and its exact root-owned sandbox selector."""

    agent: _CanonicalUuid
    runner_labels: _LabelSelector

    @model_validator(mode="after")
    def exact_runner(self) -> Self:
        if (
            self.runner_labels.get(_AGENT_LABEL) != self.agent
            or self.runner_labels.get("app.kubernetes.io/component") != "runner-sandbox"
        ):
            raise ValueError("runner selector must pin the member UUID and sandbox component")
        for key in ("app.kubernetes.io/name", "app.kubernetes.io/instance"):
            _dns_label(self.runner_labels.get(key, ""))
        return self


def _object_name(
    application: SharedApplication,
    role: str,
    member: SharedMember | None = None,
) -> str:
    identity = [
        application.namespace,
        application.release,
        application.installation,
        application.application,
        member.agent if member is not None else None,
    ]
    encoded = json.dumps(identity, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return f"shared-{role}-{digest[:32]}"


def _application_labels(application: SharedApplication) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": _object_name(application, "app"),
        "app.kubernetes.io/part-of": application.release,
        "app.kubernetes.io/component": _COMPONENT,
        _APPLICATION_LABEL: application.application,
        _INSTALLATION_LABEL: (
            hashlib.sha256(application.installation.encode("utf-8")).hexdigest()[:40]
        ),
    }


def _object(
    application: SharedApplication,
    kind: str,
    role: str,
    spec: dict[str, Any],
    member: SharedMember | None = None,
) -> dict[str, Any]:
    labels = _application_labels(application)
    if member is not None:
        labels[_AGENT_LABEL] = member.agent
    return {
        "apiVersion": {
            "Deployment": "apps/v1",
            "Service": "v1",
            "NetworkPolicy": "networking.k8s.io/v1",
        }[kind],
        "kind": kind,
        "metadata": {
            "name": _object_name(application, role, member),
            "namespace": application.namespace,
            "labels": labels,
        },
        "spec": spec,
    }


def _policy(
    application: SharedApplication,
    role: str,
    labels: dict[str, str],
    direction: Literal["Ingress", "Egress"],
    rules: list[dict[str, Any]],
    member: SharedMember | None = None,
) -> dict[str, Any]:
    spec = {
        "podSelector": {"matchLabels": dict(labels)},
        "policyTypes": [direction],
        direction.lower(): rules,
    }
    return _object(application, "NetworkPolicy", role, spec, member)


def _secret_volume(name: str, reference: SecretKeyRef) -> dict[str, Any]:
    return {
        "name": name,
        "secret": {
            "secretName": reference.name,
            "optional": False,
            "defaultMode": 0o440,
            "items": [{"key": reference.key, "path": "value"}],
        },
    }


def _mount(name: str, path: str, *, read_only: bool = True) -> dict[str, Any]:
    return {"name": name, "mountPath": path, "readOnly": read_only}


def _environment(values: dict[str, str]) -> list[dict[str, str]]:
    return [{"name": name, "value": value} for name, value in sorted(values.items())]


def _container(
    name: str,
    image: str,
    port: int,
    env: dict[str, str],
    mounts: list[dict[str, Any]],
    resources: dict[str, dict[str, str]],
) -> dict[str, Any]:
    return {
        "name": name,
        "image": image,
        "ports": [{"name": name, "containerPort": port, "protocol": "TCP"}],
        "env": _environment(env),
        "volumeMounts": mounts,
        "securityContext": {
            "runAsNonRoot": True,
            "runAsUser": 10001,
            "runAsGroup": 10001,
            "allowPrivilegeEscalation": False,
            "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]},
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "resources": deepcopy(resources),
    }


def render_application(application: SharedApplication) -> list[dict[str, Any]]:
    """Render the member-independent workload, Service and isolation policies."""

    application = SharedApplication.model_validate(deepcopy(application.model_dump(mode="python")))
    labels = _application_labels(application)
    socket_dir = str(PurePosixPath(application.resolver_socket).parent)
    server_env = {
        **application.server_env,
        "CURIE_SHARED_RESOLVER_SOCKET": application.resolver_socket,
    }
    server_mounts = [_mount("resolver", socket_dir, read_only=True)]
    proxy_mounts = [_mount("resolver", socket_dir, read_only=False)]
    volumes: list[dict[str, Any]] = [{"name": "resolver", "emptyDir": {}}]
    proxy_env = {
        "CURIE_SHARED_INSTALLATION": application.installation,
        "CURIE_SHARED_APPLICATION": application.application,
        "CURIE_SHARED_PROXY_PORT": str(application.proxy_port),
        "CURIE_SHARED_UPSTREAM_URL": f"http://127.0.0.1:{application.server_port}",
        "CURIE_SHARED_RESOLVER_SOCKET": application.resolver_socket,
        "CURIE_SHARED_DSN_ENCODING": application.dsn_encoding,
        "CURIE_SHARED_PROVIDER_ORIGINS": json.dumps(
            application.provider_origins, separators=(",", ":")
        ),
        "CURIE_SHARED_CREDENTIAL_SLOTS": json.dumps(
            application.credential_slots, separators=(",", ":")
        ),
    }
    for name, reference, env_name in (
        ("encryption-key", application.encryption_key, "CURIE_SHARED_ENCRYPTION_KEY_FILE"),
        ("database-dsn", application.database_dsn, "CURIE_SHARED_DATABASE_DSN_FILE"),
        ("grant-store", application.grant_store, "CURIE_SHARED_GRANT_STORE_FILE"),
    ):
        mount_path = str(_PROXY_SECRET_DIR / name)
        volumes.append(_secret_volume(name, reference))
        proxy_mounts.append(_mount(name, mount_path))
        proxy_env[env_name] = f"{mount_path}/value"
    if application.snapshot_seal is not None:
        volumes.append(_secret_volume("snapshot-seal", application.snapshot_seal))
        server_mounts.append(_mount("snapshot-seal", str(_SNAPSHOT_DIR)))
        server_env["CURIE_SHARED_SNAPSHOT_SEAL_FILE"] = str(_SNAPSHOT_DIR / "value")
    server = _container(
        "server", application.server_image, application.server_port,
        server_env, server_mounts, application.server_resources,
    )
    server["args"] = list(application.server_args)
    proxy = _container(
        "proxy", application.proxy_image, application.proxy_port,
        proxy_env, proxy_mounts, application.proxy_resources,
    )
    proxy["readinessProbe"] = {
        "tcpSocket": {"port": "proxy"},
        "periodSeconds": 5,
        "timeoutSeconds": 2,
        "failureThreshold": 3,
    }
    deployment_spec = {
        "replicas": 1,
        "selector": {"matchLabels": dict(labels)},
        "template": {
            "metadata": {"labels": dict(labels)},
            "spec": {
                "automountServiceAccountToken": False,
                "shareProcessNamespace": False,
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": 10001,
                    "runAsGroup": 10001,
                    "fsGroup": 10001,
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                "containers": [server, proxy],
                "volumes": volumes,
            },
        },
    }
    deployment = _object(application, "Deployment", "app", deployment_spec)
    service = _object(application, "Service", "service", {
        "type": "ClusterIP",
        "selector": dict(labels),
        "ports": [{
            "name": "proxy",
            "protocol": "TCP",
            "port": application.proxy_port,
            "targetPort": "proxy",
        }],
    })
    target = application.database_target
    peers = (
        [{
            "namespaceSelector": {
                "matchLabels": {"kubernetes.io/metadata.name": target.namespace}
            },
            "podSelector": {"matchLabels": dict(target.pod_labels)},
        }]
        if target.pod_labels is not None
        else [{"ipBlock": {"cidr": cidr}} for cidr in target.cidrs]
    )
    return [
        deployment,
        service,
        _policy(application, "deny", labels, "Ingress", []),
        _policy(application, "database", labels, "Egress", [{
            "to": peers,
            "ports": [{"protocol": "TCP", "port": target.port}],
        }]),
    ]


def render_member_reach(
    application: SharedApplication,
    member: SharedMember,
) -> list[dict[str, Any]]:
    """Render only a member's proxy ingress and sandbox egress allowances."""

    application = SharedApplication.model_validate(deepcopy(application.model_dump(mode="python")))
    member = SharedMember.model_validate(deepcopy(member.model_dump(mode="python")))
    if (
        member.runner_labels["app.kubernetes.io/instance"] != application.release
        or member.runner_labels["app.kubernetes.io/name"] != application.app_name
    ):
        raise ValueError(
            "member runner selector does not belong to this application's root release"
        )
    labels = _application_labels(application)
    return [
        _policy(application, "member-ingress", labels, "Ingress", [{
            "from": [{"podSelector": {"matchLabels": dict(member.runner_labels)}}],
            "ports": [{"protocol": "TCP", "port": application.proxy_port}],
        }], member),
        _policy(application, "member-egress", member.runner_labels, "Egress", [{
            "to": [{"podSelector": {"matchLabels": dict(labels)}}],
            "ports": [{"protocol": "TCP", "port": application.proxy_port}],
        }], member),
    ]
