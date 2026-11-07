"""Generated assets for the identity E2E harness.

The private CA and provider certificate, the connector caller key pair, the
test provider's Kubernetes objects, the API's ``api-runtime.json`` document
and the default scripted roster. Each is consumed by product code, which the
harness tests use as the judge.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from identity_target import HERE, REPO, HarnessError, run

VECTORS = REPO / "tests" / "vectors" / "http-identity-v1.json"
PROVIDER_SCRIPT = HERE / "fake_idp.py"
OWNER_LABEL = "curietech.ai/identity-e2e"
DOMAIN = "example.test"
INBOX_DOMAIN = "inbox.example.test"
ISSUER = "https://identity-e2e.example.test/directory"
MEMBER_GROUP = "identity-e2e-members"
CREDENTIAL_ID = "identity-e2e"
CONTEXT_KEY_ID = "identity-e2e-context"
LOOKUP_PORT = 8443
SINK_PORT = 8080

# domain; no real directory is named.
DEFAULT_ROSTER: dict[str, Any] = {
    "issuer": ISSUER,
    "unknown": "unresolved",
    "senders": {
        "alice@example.test": {
            "subject": "identity-e2e-alice",
            "sender_kind": "human",
            "account_status": "active",
            "groups": [MEMBER_GROUP],
        },
        "bob@example.test": {
            "subject": "identity-e2e-bob",
            "sender_kind": "human",
            "account_status": "active",
            "groups": [MEMBER_GROUP],
        },
        "dana@example.test": {
            "subject": "identity-e2e-dana",
            "sender_kind": "human",
            "account_status": "active",
            "groups": [],
        },
        "carol@example.test": {
            "subject": "identity-e2e-carol",
            "sender_kind": "human",
            "account_status": "disabled",
            "groups": [MEMBER_GROUP],
        },
        "robot@example.test": {
            "subject": "identity-e2e-robot",
            "sender_kind": "service",
            "account_status": "active",
            "groups": [MEMBER_GROUP],
        },
    },
}


# --- certificates and keys -----------------------------------------------------------


def openssl(*args: str) -> None:
    run(["openssl", *args], timeout=60)


def make_certificates(directory: Path, hosts: Sequence[str]) -> None:
    """A private CA and a server certificate for ``hosts``, under ``directory``."""

    ca_key, ca_cert = directory / "ca.key", directory / "ca.pem"
    key, csr, cert = directory / "server.key", directory / "server.csr", directory / "server.pem"
    ext = directory / "server.ext"
    old = os.umask(0o077)
    try:
        openssl(
            "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(ca_key)
        )
        openssl(
            "req",
            "-x509",
            "-new",
            "-key",
            str(ca_key),
            "-sha256",
            "-days",
            "30",
            "-subj",
            "/CN=identity-e2e test CA",
            "-out",
            str(ca_cert),
            "-addext",
            "basicConstraints=critical,CA:TRUE,pathlen:0",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-addext",
            "subjectKeyIdentifier=hash",
        )
        openssl(
            "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048", "-out", str(key)
        )
        openssl("req", "-new", "-key", str(key), "-subj", f"/CN={hosts[0]}", "-out", str(csr))
        ext.write_text(
            "basicConstraints=critical,CA:FALSE\n"
            "keyUsage=critical,digitalSignature,keyEncipherment\n"
            "extendedKeyUsage=serverAuth\n"
            "subjectKeyIdentifier=hash\n"
            "authorityKeyIdentifier=keyid\n"
            "subjectAltName=" + ",".join(f"DNS:{host}" for host in hosts) + "\n"
        )
        openssl(
            "x509",
            "-req",
            "-in",
            str(csr),
            "-CA",
            str(ca_cert),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
            "-days",
            "30",
            "-sha256",
            "-extfile",
            str(ext),
            "-out",
            str(cert),
        )
    finally:
        os.umask(old)
    for leftover in (csr, ext, directory / "ca.srl"):
        leftover.unlink(missing_ok=True)


def ed25519_pair(directory: Path) -> tuple[str, str]:
    """A connector caller key pair as standard base64 (seed, public key)."""

    private = directory / "caller.key"
    old = os.umask(0o077)
    try:
        openssl("genpkey", "-algorithm", "ed25519", "-outform", "DER", "-out", str(private))
        seed_der = private.read_bytes()
        public = subprocess.run(  # noqa: S603
            [
                "openssl",
                "pkey",
                "-inform",
                "DER",
                "-in",
                str(private),
                "-pubout",
                "-outform",
                "DER",
            ],
            capture_output=True,
            check=False,
            timeout=60,
        )
    finally:
        os.umask(old)
        private.unlink(missing_ok=True)
    if public.returncode != 0:
        raise HarnessError("openssl could not derive the Ed25519 public key")
    public_raw = public.stdout
    # PKCS#8 Ed25519 is 48 bytes ending in the 32-byte seed; SPKI is 44 bytes
    # ending in the 32-byte public key (RFC 8410).
    if len(seed_der) != 48 or len(public_raw) != 44:
        raise HarnessError("openssl produced an unexpected Ed25519 encoding")
    return (
        base64.b64encode(seed_der[-32:]).decode("ascii"),
        base64.b64encode(public_raw[-32:]).decode("ascii"),
    )


# --- manifests -----------------------------------------------------------------------


def _labels(release: str) -> dict[str, str]:
    return {OWNER_LABEL: release, "app.kubernetes.io/managed-by": "curie-identity-e2e"}


def secret_manifest(name: str, release: str, data: Mapping[str, bytes]) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "labels": _labels(release)},
        "type": "Opaque",
        "data": {key: base64.b64encode(value).decode("ascii") for key, value in data.items()},
    }


def provider_manifests(
    *,
    prefix: str,
    release: str,
    image: str,
    roster: Mapping[str, Any],
    certificate: bytes,
    private_key: bytes,
    bearer: str,
    adapter_secret: str,
) -> list[dict[str, Any]]:
    """The test provider's ConfigMap, Secret, Deployment and Service."""

    code = {
        "fake_idp.py": PROVIDER_SCRIPT.read_text(),
        "vectors.json": VECTORS.read_text(),
        "roster.json": json.dumps(roster, indent=2, sort_keys=True),
    }
    digest = hashlib.sha256(
        json.dumps(code, sort_keys=True).encode() + certificate + bearer.encode()
    ).hexdigest()[:16]
    selector = {"app.kubernetes.io/name": "identity-e2e-provider", OWNER_LABEL: release}
    return [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": f"{prefix}-idp-code", "labels": _labels(release)},
            "data": code,
        },
        secret_manifest(
            f"{prefix}-idp",
            release,
            {
                "tls.crt": certificate,
                "tls.key": private_key,
                "bearer": bearer.encode(),
                "adapter-secret": adapter_secret.encode(),
            },
        ),
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": f"{prefix}-idp", "labels": _labels(release)},
            "spec": {
                "replicas": 1,
                "selector": {"matchLabels": selector},
                "template": {
                    "metadata": {
                        "labels": {**selector, **_labels(release)},
                        "annotations": {"curietech.ai/identity-e2e-config": digest},
                    },
                    "spec": {
                        "automountServiceAccountToken": False,
                        "securityContext": {
                            "runAsNonRoot": True,
                            "runAsUser": 65532,
                            "seccompProfile": {"type": "RuntimeDefault"},
                        },
                        "containers": [
                            {
                                "name": "provider",
                                "image": image,
                                "imagePullPolicy": "IfNotPresent",
                                "command": [
                                    "python",
                                    "/idp/code/fake_idp.py",
                                    "--roster",
                                    "/idp/code/roster.json",
                                    "--vectors",
                                    "/idp/code/vectors.json",
                                    "--bearer-file",
                                    "/idp/secret/bearer",
                                    "--adapter-secret-file",
                                    "/idp/secret/adapter-secret",
                                    "--cert",
                                    "/idp/secret/tls.crt",
                                    "--key",
                                    "/idp/secret/tls.key",
                                    "--lookup-port",
                                    str(LOOKUP_PORT),
                                    "--sink-port",
                                    str(SINK_PORT),
                                ],
                                "ports": [
                                    {"name": "lookup", "containerPort": LOOKUP_PORT},
                                    {"name": "sink", "containerPort": SINK_PORT},
                                ],
                                "readinessProbe": {
                                    "httpGet": {"path": "/healthz", "port": SINK_PORT},
                                    "periodSeconds": 2,
                                },
                                "resources": {
                                    "requests": {"cpu": "10m", "memory": "48Mi"},
                                    "limits": {"cpu": "500m", "memory": "256Mi"},
                                },
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "volumeMounts": [
                                    {"name": "code", "mountPath": "/idp/code", "readOnly": True},
                                    {
                                        "name": "secret",
                                        "mountPath": "/idp/secret",
                                        "readOnly": True,
                                    },
                                ],
                            }
                        ],
                        "volumes": [
                            {"name": "code", "configMap": {"name": f"{prefix}-idp-code"}},
                            {
                                "name": "secret",
                                "secret": {"secretName": f"{prefix}-idp", "defaultMode": 0o444},
                            },
                        ],
                    },
                },
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": f"{prefix}-idp", "labels": _labels(release)},
            "spec": {
                "selector": selector,
                "ports": [
                    {"name": "lookup", "port": LOOKUP_PORT, "targetPort": LOOKUP_PORT},
                    {"name": "sink", "port": SINK_PORT, "targetPort": SINK_PORT},
                ],
            },
        },
    ]


def runtime_document(
    *,
    tenant_id: str,
    selection: Mapping[str, Any],
    bearer: str,
    registration: Mapping[str, Any],
    receiver_key: str,
    context_key: str,
) -> dict[str, Any]:
    """The protected api-runtime.json (identity/http_initializer.py)."""

    return {
        "schema_version": 1,
        "providers": [
            {
                "configuration": {
                    "schema_version": 1,
                    "tenant_id": tenant_id,
                    "version": 1,
                    "selection": dict(selection),
                },
                "bearer": bearer,
            }
        ],
        "receivers": [],
        "email_receivers": [{"registration": dict(registration), "key": receiver_key}],
        "context_keys": [{"key_id": CONTEXT_KEY_ID, "generation": 1, "key": context_key}],
        "signing_key": {"key_id": CONTEXT_KEY_ID, "generation": 1},
    }


def role_filter() -> dict[str, Any]:
    """The opt-in invocation filter: roster members of MEMBER_GROUP only."""

    return {"entries": [{"kind": "issuer_group", "group": {"issuer": ISSUER, "id": MEMBER_GROUP}}]}


def get_path(values: Mapping[str, Any], path: str) -> Any:
    node: Any = values
    for part in path.split("."):
        if not isinstance(node, Mapping) or part not in node:
            return None
        node = node[part]
    return node


def set_path(values: dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    node = values
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value


def restored_values(current: Mapping[str, Any], previous: Mapping[str, Any]) -> dict[str, Any]:
    """The release's current user values with each harness path put back.

    A path that was absent before ``up`` is removed rather than set to null:
    under ``--reuse-values`` Helm kept rendering a nulled key, so ``down``
    applies the whole result with ``--reset-values``. Values anyone else
    changed since ``up``, such as image tags, are kept.
    """

    values = copy.deepcopy(dict(current))
    for path, value in previous.items():
        if value is not None:
            set_path(values, path, value)
            continue
        parts = path.split(".")
        node: Any = values
        for part in parts[:-1]:
            node = node.get(part) if isinstance(node, dict) else None
        if isinstance(node, dict):
            node.pop(parts[-1], None)
    return values
