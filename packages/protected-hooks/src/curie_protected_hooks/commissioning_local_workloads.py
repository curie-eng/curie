"""Production API ingress of a local commissioning installation, @spec PROTECTED-COMMISSIONING-6.

Clause 6 has the provisioner install, before the campaign, the production API
ingress at its final pod template with the runtime directory it reads. The
API process is the ordinary ``python -m curie_api`` image; it serves the
protected ingress only because ``CURIE_PROTECTED_RUNTIME_DIR`` names the
directory the merged writer renders. Design decision 1 keeps all of this out
of the chart: the provisioner renders every object here.

The API also needs its ordinary backing services to boot: Postgres at a
servable schema revision, an S3 store for bundles and an ordinary Valkey.
They run beside the protected broker from the repository's pinned images and
hold no protected key. Every object carries the installation owner label and
is created through ``LocalCluster``, so the ledger owns it.
"""

from __future__ import annotations

import base64
import secrets
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from typing import Any, NoReturn, Self

from .authority_records import Manifest
from .broker_transport import EnqueueCredential, MetadataReaderCredential, SourceWriterCredential
from .commissioning_local_bootstrap import ProvisionerGrant
from .commissioning_local_broker import BrokerUser
from .commissioning_local_installation import OWNER_LABEL, LocalCluster
from .commissioning_provisioner import _pinned
from .runtime_directory_writer import RuntimeDirectoryContents, render_runtime_files

__all__ = [
    "API_NAME",
    "POSTGRES_IMAGE",
    "RUNTIME_DIR",
    "RUSTFS_IMAGE",
    "VALKEY_IMAGE",
    "LocalIngress",
    "ServiceSecrets",
    "WorkloadsRefused",
    "api_objects",
    "migration_pod",
    "runtime_secret",
    "service_objects",
]

# @spec PROTECTED-COMMISSIONING-6: the repository's pinned backing images.
POSTGRES_IMAGE = (
    "postgres:16.15-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685"
)
VALKEY_IMAGE = (
    "valkey/valkey:8.1.10-alpine@sha256:"
    "d2e18f3410b6f616de1417f570fa55261af2898b9c5b2cfb6781ce2373ea43d1"
)
RUSTFS_IMAGE = (
    "rustfs/rustfs:1.0.0-beta.12@sha256:"
    "41fe89380f4120a337790c02af192c3fe7bb55c3edc2e6e9357b487b47c6ab21"
)
API_NAME = "curie-api"
RUNTIME_DIR = "/var/run/curie/protected"
_RUNTIME_SECRET = API_NAME + "-protected-runtime"
_SERVICES_SECRET = "curie-services"
_APP = "app.kubernetes.io/name"
# Readiness the runtime directory's bootstrap.json bounds (60 seconds).
MAX_READINESS_MS = 60000


class WorkloadsRefused(RuntimeError):
    """Redacted workload refusal, @spec PROTECTED-COMMISSIONING-6."""

    def __init__(self) -> None:
        """Never echo a secret, object or command output, @spec PROTECTED-COMMISSIONING-6."""
        super().__init__("Commissioning local workloads refused")


def _refuse() -> NoReturn:
    raise WorkloadsRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


@dataclass(frozen=True, slots=True, repr=False)
class ServiceSecrets:
    """Fresh per-installation secrets of the API and its backing services.

    @spec PROTECTED-COMMISSIONING-6.
    """

    postgres_password: str = field(repr=False)
    valkey_password: str = field(repr=False)
    s3_access_key: str = field(repr=False)
    s3_secret_key: str = field(repr=False)
    api_key: str = field(repr=False)
    internal_worker_token: str = field(repr=False)
    approval_chat_attester_secret: str = field(repr=False)
    remediation_authority_key: str = field(repr=False)
    github_webhook_secret: str = field(repr=False)

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-6."""
        return "ServiceSecrets()"

    @classmethod
    def fresh(cls) -> Self:
        """@spec PROTECTED-COMMISSIONING-6."""
        return cls(**{member.name: secrets.token_hex(24) for member in fields(cls)})


def _labels(owner: str, name: str) -> dict[str, str]:
    return {OWNER_LABEL: owner, _APP: name}


def _metadata(owner: str, namespace: str, name: str) -> dict[str, Any]:
    return {"name": name, "namespace": namespace, "labels": _labels(owner, name)}


def _b64(value: str | bytes) -> str:
    raw = value.encode("ascii") if isinstance(value, str) else value
    return base64.b64encode(raw).decode("ascii")


def _locked(uid: int, **extra: Any) -> dict[str, Any]:
    """The restricted container security context, @spec PROTECTED-COMMISSIONING-6."""
    return {
        "runAsNonRoot": True,
        "runAsUser": uid,
        "runAsGroup": uid,
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
        **extra,
    }


def _pod_spec(
    uid: int, containers: list[dict[str, Any]], volumes: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "automountServiceAccountToken": False,
        "enableServiceLinks": False,
        "securityContext": {"fsGroup": uid, "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": containers,
        "volumes": volumes,
    }


def _scratch(*names: str) -> list[dict[str, Any]]:
    return [{"name": name, "emptyDir": {"sizeLimit": "256Mi"}} for name in names]


def _secret_env(name: str, key: str) -> dict[str, Any]:
    return {"name": name, "valueFrom": {"secretKeyRef": {"name": _SERVICES_SECRET, "key": key}}}


def _service(owner: str, namespace: str, name: str, port: int) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata(owner, namespace, name),
        "spec": {
            "type": "ClusterIP",
            "selector": _labels(owner, name),
            "ports": [{"name": "tcp", "port": port, "targetPort": port}],
        },
    }


def _pod(
    owner: str,
    namespace: str,
    name: str,
    uid: int,
    container: dict[str, Any],
    volumes: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": _metadata(owner, namespace, name),
        "spec": {"restartPolicy": "Always", **_pod_spec(uid, [container], volumes)},
    }


def service_objects(owner: str, namespace: str, secret: ServiceSecrets) -> list[dict[str, Any]]:
    """The backing-service Secret, then Postgres, ordinary Valkey and S3.

    @spec PROTECTED-COMMISSIONING-6.
    """
    _require(type(secret) is ServiceSecrets)
    for image in (POSTGRES_IMAGE, VALKEY_IMAGE, RUSTFS_IMAGE):
        _pinned(image)
    values = {member.name: getattr(secret, member.name) for member in fields(ServiceSecrets)}
    postgres = {
        "name": "postgres",
        "image": POSTGRES_IMAGE,
        "env": [
            {"name": "POSTGRES_USER", "value": "postgres"},
            {"name": "POSTGRES_DB", "value": "postgres"},
            {"name": "PGDATA", "value": "/var/lib/postgresql/data/pgdata"},
            _secret_env("POSTGRES_PASSWORD", "postgres_password"),
        ],
        "ports": [{"containerPort": 5432}],
        "readinessProbe": {
            "exec": {"command": ["pg_isready", "-U", "postgres"]},
            "periodSeconds": 3,
        },
        "securityContext": _locked(70),
        "volumeMounts": [
            {"name": "data", "mountPath": "/var/lib/postgresql/data"},
            {"name": "run", "mountPath": "/var/run/postgresql"},
            {"name": "tmp", "mountPath": "/tmp"},
        ],
    }
    valkey = {
        "name": "valkey",
        "image": VALKEY_IMAGE,
        "command": ["valkey-server"],
        "args": ["--dir", "/data", "--requirepass", "$(VALKEY_PASSWORD)", "--save", ""],
        "env": [_secret_env("VALKEY_PASSWORD", "valkey_password")],
        "ports": [{"containerPort": 6379}],
        "readinessProbe": {"tcpSocket": {"port": 6379}, "periodSeconds": 3},
        "securityContext": _locked(999),
        "volumeMounts": [{"name": "data", "mountPath": "/data"}],
    }
    rustfs = {
        "name": "rustfs",
        "image": RUSTFS_IMAGE,
        "env": [
            {"name": "RUSTFS_VOLUMES", "value": "/data"},
            {"name": "RUSTFS_ADDRESS", "value": "0.0.0.0:9000"},
            {"name": "RUSTFS_CONSOLE_ENABLE", "value": "false"},
            {"name": "RUSTFS_OBS_LOG_DIRECTORY", "value": "/logs"},
            _secret_env("RUSTFS_ACCESS_KEY", "s3_access_key"),
            _secret_env("RUSTFS_SECRET_KEY", "s3_secret_key"),
        ],
        "ports": [{"containerPort": 9000}],
        "readinessProbe": {"tcpSocket": {"port": 9000}, "periodSeconds": 3},
        "securityContext": _locked(10001),
        "volumeMounts": [
            {"name": "data", "mountPath": "/data"},
            {"name": "logs", "mountPath": "/logs"},
            {"name": "tmp", "mountPath": "/tmp"},
        ],
    }
    return [
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": _SERVICES_SECRET,
                "namespace": namespace,
                "labels": {OWNER_LABEL: owner},
            },
            "type": "Opaque",
            "immutable": True,
            "data": {key: _b64(value) for key, value in values.items()},
        },
        _service(owner, namespace, "curie-postgres", 5432),
        _pod(owner, namespace, "curie-postgres", 70, postgres, _scratch("data", "run", "tmp")),
        _service(owner, namespace, "curie-valkey", 6379),
        _pod(owner, namespace, "curie-valkey", 999, valkey, _scratch("data")),
        _service(owner, namespace, "curie-s3", 9000),
        _pod(owner, namespace, "curie-s3", 10001, rustfs, _scratch("data", "logs", "tmp")),
    ]


def _api_env(namespace: str) -> list[dict[str, Any]]:
    """The API settings: production mode with every secret set and backing services named."""
    host = "." + namespace + ".svc.cluster.local"
    return [
        {"name": "ENVIRONMENT", "value": "prod"},
        {"name": "HOME", "value": "/tmp"},
        _secret_env("POSTGRES_PASSWORD", "postgres_password"),
        {
            "name": "DATABASE_URL",
            "value": "postgresql+asyncpg://postgres:$(POSTGRES_PASSWORD)@curie-postgres"
            + host
            + ":5432/postgres",
        },
        {"name": "DB_SCHEMA", "value": "curie"},
        {"name": "VALKEY_HOST", "value": "curie-valkey" + host},
        {"name": "VALKEY_PORT", "value": "6379"},
        {"name": "VALKEY_TLS", "value": "false"},
        _secret_env("VALKEY_PASSWORD", "valkey_password"),
        {"name": "S3_ENDPOINT_URL", "value": "http://curie-s3" + host + ":9000"},
        {"name": "S3_REGION", "value": "us-east-1"},
        _secret_env("S3_ACCESS_KEY", "s3_access_key"),
        _secret_env("S3_SECRET_KEY", "s3_secret_key"),
        _secret_env("API_KEY", "api_key"),
        _secret_env("CURIE_INTERNAL_WORKER_TOKEN", "internal_worker_token"),
        _secret_env("CURIE_APPROVAL_CHAT_ATTESTER_SECRET", "approval_chat_attester_secret"),
        _secret_env("CURIE_REMEDIATION_AUTHORITY_KEY", "remediation_authority_key"),
        _secret_env("GITHUB_WEBHOOK_SECRET", "github_webhook_secret"),
        {"name": "FORWARDED_ALLOW_IPS", "value": ""},
    ]


def migration_pod(owner: str, namespace: str, api_image: str) -> dict[str, Any]:
    """One run of the API image's schema upgrade, @spec PROTECTED-COMMISSIONING-6."""
    _pinned(api_image)
    container = {
        "name": "migrate",
        "image": api_image,
        "command": ["python", "-m", "curie_api.schema_compat", "upgrade"],
        "env": _api_env(namespace),
        "securityContext": _locked(1000),
        "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
    }
    pod = _pod(owner, namespace, "curie-migrate", 1000, container, _scratch("tmp"))
    pod["spec"]["restartPolicy"] = "Never"
    return pod


def runtime_secret(owner: str, namespace: str, files: Mapping[str, bytes]) -> dict[str, Any]:
    """The rendered runtime directory as the immutable Secret the API mounts.

    @spec PROTECTED-COMMISSIONING-6 PROTECTED-HOOK-SOURCE-11.
    """
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": _RUNTIME_SECRET,
            "namespace": namespace,
            "labels": {OWNER_LABEL: owner},
        },
        "type": "Opaque",
        "immutable": True,
        "data": {name: _b64(raw) for name, raw in sorted(files.items())},
    }


def api_objects(owner: str, namespace: str, api_image: str) -> list[dict[str, Any]]:
    """The API Service and Deployment at their final template, @spec PROTECTED-COMMISSIONING-6.

    The runtime directory is the Secret mounted read only; the API reads the
    Secret volume's ``..data`` layout, which the shared reader accepts.
    """
    _pinned(api_image)
    container = {
        "name": "api",
        "image": api_image,
        "env": [
            *_api_env(namespace),
            {"name": "CURIE_PROTECTED_RUNTIME_DIR", "value": RUNTIME_DIR},
        ],
        "ports": [{"containerPort": 8000, "name": "http"}],
        "readinessProbe": {
            "httpGet": {"path": "/ready", "port": 8000},
            "periodSeconds": 5,
            "timeoutSeconds": 3,
            "failureThreshold": 24,
        },
        "livenessProbe": {
            "httpGet": {"path": "/health", "port": 8000},
            "initialDelaySeconds": 30,
            "periodSeconds": 15,
        },
        "resources": {
            "requests": {"cpu": "200m", "memory": "384Mi"},
            "limits": {"memory": "1Gi"},
        },
        "securityContext": _locked(1000),
        "volumeMounts": [
            {"name": "runtime", "mountPath": RUNTIME_DIR, "readOnly": True},
            {"name": "tmp", "mountPath": "/tmp"},
        ],
    }
    volumes = [
        {"name": "runtime", "secret": {"secretName": _RUNTIME_SECRET, "defaultMode": 0o440}},
        *_scratch("tmp"),
    ]
    return [
        _service(owner, namespace, API_NAME, 8000),
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": _metadata(owner, namespace, API_NAME),
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": _labels(owner, API_NAME)},
                "template": {
                    "metadata": {"labels": _labels(owner, API_NAME)},
                    "spec": _pod_spec(1000, [container], volumes),
                },
            },
        },
    ]


def _credential(kind: type, user: BrokerUser) -> Any:
    return kind(username=user.username, password=user.password)


class LocalIngress:
    """Install the API's backing services, schema, runtime directory and API.

    @spec PROTECTED-COMMISSIONING-6.
    """

    def __init__(self, cluster: LocalCluster, *, namespace: str) -> None:
        """@spec PROTECTED-COMMISSIONING-6."""
        _require(type(cluster) is LocalCluster)
        self.cluster, self.namespace = cluster, namespace
        self.secrets = ServiceSecrets.fresh()

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-6."""
        return "LocalIngress()"

    def _wait(self, *args: str, timeout: int = 300) -> None:
        self.cluster.kubectl(
            "wait", *args, "--namespace", self.namespace, "--timeout", f"{timeout}s",
            timeout=timeout + 30,
        )  # fmt: skip

    def install(
        self,
        *,
        api_image: str,
        manifest: Manifest,
        ca_pem: str,
        grant: ProvisionerGrant,
    ) -> None:
        """Services, then the schema, then the runtime Secret, then the API.

        The runtime directory carries M, the broker CA and the production
        control reader, source writer and enqueue credentials the
        administrator bootstrap granted for exactly this placement; the writer
        parses its own output with the API's readers first.
        @spec PROTECTED-COMMISSIONING-6.
        """
        _require(type(grant) is ProvisionerGrant)
        workload = grant.workload
        files = render_runtime_files(
            RuntimeDirectoryContents(
                manifest=manifest,
                ca_pem=ca_pem,
                max_readiness_ms=MAX_READINESS_MS,
                control_reader=_credential(MetadataReaderCredential, workload["control_reader"]),
                source_writer=_credential(SourceWriterCredential, workload["source_writer"]),
                enqueue=_credential(EnqueueCredential, workload["enqueue"]),
            )
        )
        owner = self.cluster.owner
        for item in service_objects(owner, self.namespace, self.secrets):
            self.cluster.create(item)
        for name in ("curie-postgres", "curie-valkey", "curie-s3"):
            self._wait("--for=condition=Ready", "pod/" + name)
        self.cluster.create(migration_pod(owner, self.namespace, api_image))
        self._wait("--for=jsonpath={.status.phase}=Succeeded", "pod/curie-migrate", timeout=600)
        self.cluster.create(runtime_secret(owner, self.namespace, files))
        for item in api_objects(owner, self.namespace, api_image):
            self.cluster.create(item)
        self.cluster.kubectl(
            "rollout", "status", "deployment/" + API_NAME, "--namespace", self.namespace,
            "--timeout", "600s", timeout=630,
        )  # fmt: skip
