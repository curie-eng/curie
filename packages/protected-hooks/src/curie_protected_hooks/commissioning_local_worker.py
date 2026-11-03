"""Production protected worker of a local installation, @spec PROTECTED-COMMISSIONING-6.

Clause 6 has the provisioner install, before the campaign, the protected
worker at its final pod template, image, mounts and Secret references, with
the artifact measurement file in its ``measurements`` directory. The worker is
the unchanged ``python -m curie_worker.protected_entry`` composition; it reads
only regular files from one runtime directory (it refuses a Secret volume's
symlink layout), so an init container copies the named files of the
provisioner's immutable Secret, and nothing of the volume's own layout, into
an ``emptyDir`` that the worker mounts read only.

The worker's ``process.json`` pins the launch mediator: its origin, CA,
server name and certificate SPKI, and the worker's own client certificate.
The mediator has no production executable composition yet. The
administrator bootstrap mints the launch identity and keeps each private key
in its own Secret: the mediator's for the mediator workload a later slice
installs, the worker's for the worker to mount. The provisioner holds only the
public half. The worker's
template and files are therefore final before the campaign; without a
Qualification and Readiness it refuses before it contacts the mediator.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any, NoReturn

from .authority_records import Manifest
from .commissioning_local_bootstrap import (
    MEDIATOR_NAME,
    WORKER_CLIENT_SECRET,
    WORKER_NAME,
    LaunchPublic,
    ProvisionerGrant,
    mediator_name,
    worker_namespace,
)
from .commissioning_local_broker import BrokerUser
from .commissioning_local_installation import OWNER_LABEL, LocalCluster
from .commissioning_local_workloads import (
    API_NAME,
    ServiceSecrets,
    _labels,
    _locked,
    _metadata,
    _pod_spec,
    _scratch,
)
from .commissioning_provisioner import _pinned

__all__ = [
    "MEDIATOR_NAME",
    "MEDIATOR_PORT",
    "RUNTIME_DIR",
    "SOURCE_DIR",
    "WORKER_NAME",
    "LocalWorker",
    "WorkerRefused",
    "worker_files",
    "worker_deployment",
    "worker_objects",
]

# @spec PROTECTED-COMMISSIONING-6
RUNTIME_DIR = "/run/curie/protected-worker"
MEDIATOR_PORT = 8443
_RUNTIME_SECRET = WORKER_NAME + "-runtime"
SOURCE_DIR = "/run/curie/protected-worker-source"
_FIXED = (
    "manifest.json",
    "execution_config.json",
    "worker.json",
    "process.json",
    "ca.pem",
    "launch-ca.pem",
)
_ADDRESSED = re.compile(r"(measurements|projections)/[0-9a-f]{64}\.json")
# The administrator's Secret: the worker's own launch client certificate and key.
_CLIENT = ("launch-client.pem", "launch-client.key")


class WorkerRefused(RuntimeError):
    """Redacted worker refusal, @spec PROTECTED-COMMISSIONING-6."""

    def __init__(self) -> None:
        """Never echo a credential, file or command output, @spec PROTECTED-COMMISSIONING-6."""
        super().__init__("Commissioning local worker refused")


def _refuse() -> NoReturn:
    raise WorkerRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


def worker_principal_id(manifest: Manifest) -> str:
    """The worker's launch principal, bound to the runtime, @spec PROTECTED-COMMISSIONING-6."""
    return "worker/" + str(manifest.as_dict()["runtime_id"])


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()


def worker_files(
    *,
    manifest: Manifest,
    execution_config: bytes,
    projection: bytes,
    measurement_file: tuple[str, bytes],
    ca_pem: str,
    worker: BrokerUser,
    launch: LaunchPublic,
    namespace: str,
    secrets: ServiceSecrets,
) -> dict[str, bytes]:
    """The worker's runtime directory: relative path to bytes, @spec PROTECTED-COMMISSIONING-6.

    ``manifest.json``, ``execution_config.json`` and ``worker.json`` are what
    ``load_worker_bootstrap`` reads; ``process.json`` is ``load_process_settings``'s
    grammar; ``ca.pem`` is the broker CA; the launch files are the paths
    ``process.json`` names (the client certificate and key come from the
    administrator's own Secret, never from here); the measurement and
    projection are content-addressed.
    """
    _require(type(manifest) is Manifest and type(worker) is BrokerUser)
    m = manifest.as_dict()
    host = "." + namespace + ".svc.cluster.local"
    process = {
        "schema_version": 1,
        "database_url": "postgresql+asyncpg://postgres:"
        + secrets.postgres_password
        + "@curie-postgres"
        + host
        + ":5432/postgres",
        "db_schema": "curie",
        "api_base_url": "http://" + API_NAME + host + ":8000",
        "internal_worker_token": secrets.internal_worker_token,
        "adapter_credentials": {},
        "slack": {"token": "", "base_url": "", "trusted_origins": [], "identity_tokens": {}},
        "launch_tls": {
            "origin": f"https://{launch.server_name}:{MEDIATOR_PORT}",
            "ca_file": RUNTIME_DIR + "/launch-ca.pem",
            "server_name": launch.server_name,
            "server_spki_sha256": launch.server_spki_sha256,
            "client_certificate": RUNTIME_DIR + "/launch-client.pem",
            "client_key": RUNTIME_DIR + "/launch-client.key",
            "worker_principal_id": worker_principal_id(manifest),
        },
    }
    name, measurement = measurement_file
    return {
        "manifest.json": manifest.canonical_bytes,
        "execution_config.json": execution_config,
        "worker.json": _canonical(
            {
                "schema_version": 1,
                "credential_ref": m["credential_refs"]["worker"],
                "worker": {"username": worker.username, "password": worker.password},
            }
        ),
        "process.json": _canonical(process),
        "ca.pem": ca_pem.encode("ascii"),
        "launch-ca.pem": launch.ca_pem.encode("ascii"),
        "measurements/" + name: measurement,
        "projections/" + hashlib.sha256(projection).hexdigest() + ".json": projection,
    }


def _b64(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _key(path: str) -> str:
    return path.replace("/", ".")


def _runtime_paths(paths: Any) -> tuple[str, ...]:
    """The runtime directory's grammar: the fixed files and one of each addressed file."""
    _require(isinstance(paths, (list, tuple, set, frozenset, Mapping)))
    chosen = tuple(sorted(paths))
    _require(all(type(p) is str for p in chosen))
    addressed = [p for p in chosen if _ADDRESSED.fullmatch(p)]
    _require(sorted(p.split("/", 1)[0] for p in addressed) == ["measurements", "projections"])
    _require(sorted(set(chosen) - set(addressed)) == sorted(_FIXED))
    return chosen


def _stage(paths: tuple[str, ...]) -> list[str]:
    """Copy each named file, so the volume's ``..data`` layout never reaches the worker."""
    named = sorted((*paths, *_CLIENT))
    steps = ["set -eu", f"mkdir {RUNTIME_DIR}/measurements {RUNTIME_DIR}/projections"]
    steps += [f"cp -L {SOURCE_DIR}/{p} {RUNTIME_DIR}/{p}" for p in named]
    return ["sh", "-c", "; ".join(steps)]


def worker_deployment(
    owner: str,
    namespace: str,
    worker_image: str,
    paths: Any,
) -> dict[str, Any]:
    """The worker Deployment for one runtime file set.

    The Deployment restarts the worker; before activation each start refuses,
    and it starts once the records it requires exist (clause 6). The init
    container copies the named files of the provisioner's runtime Secret and
    of the administrator's client-key Secret into an ``emptyDir``. The
    administrator renders this itself before it creates the worker.
    @spec PROTECTED-COMMISSIONING-6.
    """
    _pinned(worker_image)
    chosen = _runtime_paths(paths)
    stage = {
        "name": "stage",
        "image": worker_image,
        "command": _stage(chosen),
        "securityContext": _locked(1000),
        "volumeMounts": [
            {"name": "source", "mountPath": SOURCE_DIR, "readOnly": True},
            {"name": "runtime", "mountPath": RUNTIME_DIR},
        ],
    }
    worker = {
        "name": "worker",
        "image": worker_image,
        "command": ["python", "-m", "curie_worker.protected_entry", "--runtime-dir", RUNTIME_DIR],
        "env": [{"name": "HOME", "value": "/tmp"}],
        "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"memory": "1Gi"}},
        "securityContext": _locked(1000),
        "volumeMounts": [
            {"name": "runtime", "mountPath": RUNTIME_DIR, "readOnly": True},
            {"name": "tmp", "mountPath": "/tmp"},
        ],
    }
    volumes = [
        {
            "name": "source",
            "projected": {
                "defaultMode": 0o440,
                "sources": [
                    {
                        "secret": {
                            "name": _RUNTIME_SECRET,
                            "items": [{"key": _key(path), "path": path} for path in chosen],
                        }
                    },
                    {
                        "secret": {
                            "name": WORKER_CLIENT_SECRET,
                            "items": [{"key": name, "path": name} for name in _CLIENT],
                        }
                    },
                ],
            },
        },
        *_scratch("runtime", "tmp"),
    ]
    spec = _pod_spec(1000, [worker], volumes)
    spec["initContainers"] = [stage]
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata(owner, namespace, WORKER_NAME),
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": _labels(owner, WORKER_NAME)},
            "template": {"metadata": {"labels": _labels(owner, WORKER_NAME)}, "spec": spec},
        },
    }


def deployment_paths(deployment: Any) -> tuple[str, ...]:
    """The runtime file set a rendered worker Deployment names, @spec PROTECTED-COMMISSIONING-6."""
    try:
        volumes = deployment["spec"]["template"]["spec"]["volumes"]
        (source,) = [v for v in volumes if v["name"] == "source"]
        items = source["projected"]["sources"][0]["secret"]["items"]
        return _runtime_paths([item["path"] for item in items])
    except (KeyError, TypeError, ValueError, IndexError):
        _refuse()


def worker_objects(
    owner: str,
    namespace: str,
    worker_image: str,
    files: Mapping[str, bytes],
) -> list[dict[str, Any]]:
    """The worker runtime Secret and the worker Deployment, @spec PROTECTED-COMMISSIONING-6."""
    deployment = worker_deployment(owner, namespace, worker_image, files)
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": _RUNTIME_SECRET,
            "namespace": namespace,
            "labels": {OWNER_LABEL: owner},
        },
        "type": "Opaque",
        "immutable": True,
        "data": {_key(path): _b64(raw) for path, raw in sorted(files.items())},
    }
    return [secret, deployment]


class LocalWorker:
    """Install the protected worker at its final template, @spec PROTECTED-COMMISSIONING-6."""

    def __init__(self, cluster: LocalCluster, *, namespace: str) -> None:
        """``namespace`` is the protected namespace; the worker runs in its worker namespace.

        @spec PROTECTED-COMMISSIONING-6.
        """
        _require(type(cluster) is LocalCluster)
        self.cluster, self.namespace = cluster, namespace
        self.worker_namespace = worker_namespace(namespace)

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-6."""
        return "LocalWorker()"

    def install(
        self,
        *,
        worker_image: str,
        manifest: Manifest,
        execution_config: bytes,
        projection: bytes,
        measurement_file: tuple[str, bytes],
        ca_pem: str,
        grant: ProvisionerGrant,
        secrets: ServiceSecrets,
    ) -> tuple[dict[str, bytes], dict[str, Any]]:
        """Render the files, write the runtime Secret, return the Deployment template.

        The provisioner owns the runtime directory; the administrator creates
        the returned Deployment (``AdministratorBootstrap.install_worker``).
        Returns the rendered files and the template. @spec PROTECTED-COMMISSIONING-6.
        """
        _require(type(grant) is ProvisionerGrant and "worker" in grant.workload)
        _require(grant.launch.server_name == mediator_name(self.namespace))
        files = worker_files(
            manifest=manifest,
            execution_config=execution_config,
            projection=projection,
            measurement_file=measurement_file,
            ca_pem=ca_pem,
            worker=grant.workload["worker"],
            launch=grant.launch,
            namespace=self.namespace,
            secrets=secrets,
        )
        secret, deployment = worker_objects(
            self.cluster.owner, self.worker_namespace, worker_image, files
        )
        self.cluster.create(secret)
        return files, deployment
