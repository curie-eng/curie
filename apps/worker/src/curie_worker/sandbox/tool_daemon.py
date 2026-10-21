"""One claim's Docker daemon (ADR 0217): objects, certificates, ping, retirement.

A repository whose runner pool sets ``docker`` gets a daemon Pod, a headless
Service, an ingress-only NetworkPolicy and a server TLS Secret in the chart's
daemon namespace, all named exactly the claim and created before the claim.
Each claim mints its own CA, server and client certificates, so a runner's
client authenticates to its own daemon and no other. Everything Kubernetes
goes through the ``SandboxClient`` seam as plain manifests; this module never
logs PEM material, only exception classes.

Creation order is NetworkPolicy, Service, Pod, Secret. The Secret is owned by
the Pod's uid (same namespace), so garbage collection removes it with exactly
that Pod and the worker never deletes a Secret by name. Retirement deletes the
NetworkPolicy last and only after the Pod and Service deletes succeeded, which
leaves a listable labelled anchor for any unfinished retirement.
"""

from __future__ import annotations

import logging
import ssl
import tempfile
import time
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from .claim_tokens import CLAIM_LABEL, DOCKER_CERT_DIR
from .types import (
    MANAGED_BY_LABEL,
    MANAGED_BY_VALUE,
    ClaimTimeoutError,
    DaemonObjectKind,
    KubeTransientError,
    ListableDaemonKind,
    SandboxClient,
    ToolDaemonMount,
)

if TYPE_CHECKING:
    from .daemon_config import ToolDaemonConfig

logger = logging.getLogger(__name__)

EGRESS_LABEL = "curietech.ai/docker-egress"
CLIENT_FILES = ("ca.pem", "cert.pem", "key.pem")
_INSTANCE_LABEL = "app.kubernetes.io/instance"
_COMPONENT_LABEL = "app.kubernetes.io/component"
_REQUEST_TIMEOUT_S = 5.0
# Readiness sends no further ping with less than this left (substrate's rule).
_MIN_POLL_BUDGET_S = 0.5
_MAX_RESPONSE_BYTES = 1 << 20
# Certificate validity: backdated for clock skew, and past the absolute claim
# lifetime by an hour so a daemon never outlives its certificate.
_NOT_BEFORE_SKEW = timedelta(minutes=5)
_NOT_AFTER_SKEW = timedelta(hours=1)
_LISTED_KINDS: tuple[ListableDaemonKind, ...] = ("Pod", "Service", "NetworkPolicy")


def DAEMON_ARGS(port: int) -> list[str]:  # noqa: N802 - the chart's fixed argv, named as one
    """dockerd's argv, exactly as the chart's pod admission policy requires."""

    return [
        "dockerd",
        "--userns-remap=default",
        "--authorization-plugin=curie-authz",
        f"--host=tcp://0.0.0.0:{port}",
        "--tlsverify",
        "--tlscacert=/certs/ca.pem",
        "--tlscert=/certs/server-cert.pem",
        "--tlskey=/certs/server-key.pem",
    ]


def service_host(claim: str, namespace: str) -> str:
    """The daemon's headless Service name, the server certificate's first SAN."""

    return f"{claim}.{namespace}.svc"


# -- certificates -----------------------------------------------------------------


@dataclass(frozen=True)
class ClaimCertificates:
    """One claim's TLS files: the daemon's Secret and the runner's client set."""

    server_files: Mapping[str, str] = field(repr=False)
    client_files: Mapping[str, str] = field(repr=False)


def _pem_key(key: ec.EllipticCurvePrivateKey) -> str:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()


def _pem_cert(cert: x509.Certificate) -> str:
    return cert.public_bytes(serialization.Encoding.PEM).decode()


def _key_usage(*, cert_sign: bool) -> x509.KeyUsage:
    return x509.KeyUsage(
        digital_signature=not cert_sign,
        content_commitment=False,
        key_encipherment=False,
        data_encipherment=False,
        key_agreement=False,
        key_cert_sign=cert_sign,
        crl_sign=cert_sign,
        encipher_only=False,
        decipher_only=False,
    )


def issue(claim: str, namespace: str, lifetime_s: int) -> ClaimCertificates:
    """Mint an independent CA plus server and client leaves for ``claim``.

    Three P-256 keys. The CA key signs both leaves and is then discarded, so
    nothing can issue another certificate this daemon trusts. The server leaf
    names the claim's headless Service; the client leaf is client-auth only.
    """

    now = datetime.now(UTC)
    start = now - _NOT_BEFORE_SKEW
    end = now + timedelta(seconds=lifetime_s) + _NOT_AFTER_SKEW
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, f"curie-docker-ca {claim}")])
    ca_ski = x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key())
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(start)
        .not_valid_after(end)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(_key_usage(cert_sign=True), critical=True)
        .add_extension(ca_ski, critical=False)
        .sign(ca_key, hashes.SHA256())
    )

    def leaf(purpose: x509.ObjectIdentifier, names: list[str]) -> tuple[str, str]:
        key = ec.generate_private_key(ec.SECP256R1())
        builder = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, claim)]))
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(start)
            .not_valid_after(end)
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(_key_usage(cert_sign=False), critical=True)
            .add_extension(x509.ExtendedKeyUsage([purpose]), critical=False)
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(ca_ski), False
            )
        )
        if names:
            dns = [x509.DNSName(name) for name in names]
            builder = builder.add_extension(x509.SubjectAlternativeName(dns), critical=False)
        return _pem_cert(builder.sign(ca_key, hashes.SHA256())), _pem_key(key)

    host = service_host(claim, namespace)
    server_cert, server_key = leaf(
        ExtendedKeyUsageOID.SERVER_AUTH, [host, f"{host}.cluster.local", f"{claim}.{namespace}"]
    )
    client_cert, client_key = leaf(ExtendedKeyUsageOID.CLIENT_AUTH, [])
    ca_pem = _pem_cert(ca)
    return ClaimCertificates(
        server_files={
            "ca.pem": ca_pem,
            "server-cert.pem": server_cert,
            "server-key.pem": server_key,
        },
        client_files={"ca.pem": ca_pem, "cert.pem": client_cert, "key.pem": client_key},
    )


# -- manifests ----------------------------------------------------------------------


def object_labels(config: ToolDaemonConfig, claim: str) -> dict[str, str]:
    """Claim, managed-by and the chart's release selector labels."""

    return {
        CLAIM_LABEL: claim,
        MANAGED_BY_LABEL: MANAGED_BY_VALUE,
        **config.selector_labels.model_dump(by_alias=True),
    }


def _metadata(config: ToolDaemonConfig, claim: str, **extra: str) -> dict[str, Any]:
    return {"name": claim, "labels": {**object_labels(config, claim), **extra}}


def _selector(config: ToolDaemonConfig, claim: str | None) -> str:
    """This release's managed daemon objects: of ``claim``, or of any claim."""

    claim_term = CLAIM_LABEL if claim is None else f"{CLAIM_LABEL}={claim}"
    instance = config.selector_labels.instance
    return f"{claim_term},{MANAGED_BY_LABEL}={MANAGED_BY_VALUE},{_INSTANCE_LABEL}={instance}"


def _network_policy(config: ToolDaemonConfig, claim: str, release_namespace: str) -> dict[str, Any]:
    # The peer's extra key is the runner pod's own claim label, so only this
    # claim's runner reaches this claim's daemon, and only on the daemon port.
    peer = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": release_namespace}},
        "podSelector": {
            "matchLabels": {
                _COMPONENT_LABEL: "runner-sandbox",
                _INSTANCE_LABEL: config.selector_labels.instance,
                CLAIM_LABEL: claim,
            }
        },
    }
    return {
        "apiVersion": "networking.k8s.io/v1",
        "kind": "NetworkPolicy",
        "metadata": _metadata(config, claim),
        "spec": {
            "podSelector": {"matchLabels": {CLAIM_LABEL: claim}},
            "policyTypes": ["Ingress"],
            "ingress": [{"from": [peer], "ports": [{"protocol": "TCP", "port": config.port}]}],
        },
    }


def _service(config: ToolDaemonConfig, claim: str) -> dict[str, Any]:
    port = config.port
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata(config, claim),
        "spec": {
            "clusterIP": "None",
            "selector": {CLAIM_LABEL: claim, _COMPONENT_LABEL: "docker-daemon"},
            "ports": [{"name": "docker", "port": port, "targetPort": port, "protocol": "TCP"}],
        },
    }


def _pod(config: ToolDaemonConfig, claim: str, egress_class: str) -> dict[str, Any]:
    container = {
        "name": "dockerd",
        "image": config.image,
        "args": DAEMON_ARGS(config.port),
        "securityContext": {"privileged": True},
        "ports": [{"containerPort": config.port, "protocol": "TCP"}],
        "resources": config.resources.model_dump(by_alias=True),
        "volumeMounts": [
            {"name": "data", "mountPath": "/var/lib/docker"},
            {"name": "certs", "mountPath": "/certs", "readOnly": True},
        ],
    }
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": _metadata(config, claim, **{EGRESS_LABEL: egress_class}),
        "spec": {
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "restartPolicy": "Never",
            # The absolute claim lifetime; renewals never extend it.
            "activeDeadlineSeconds": config.max_claim_lifetime_seconds,
            "containers": [container],
            "volumes": [
                {"name": "data", "emptyDir": {"sizeLimit": config.storage_size_limit}},
                {"name": "certs", "secret": {"secretName": claim}},
            ],
        },
    }


def _secret(
    config: ToolDaemonConfig, claim: str, pod_uid: str, files: Mapping[str, str]
) -> dict[str, Any]:
    owner = {"apiVersion": "v1", "kind": "Pod", "name": claim, "uid": pod_uid}
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {**_metadata(config, claim), "ownerReferences": [owner]},
        "stringData": dict(files),
    }


# -- provisioning and retirement ---------------------------------------------------


def _bound(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ClaimTimeoutError("the tool daemon's time budget is spent")
    return min(_REQUEST_TIMEOUT_S, remaining)


type ObjectRef = tuple[DaemonObjectKind, str, str]


def _delete(
    client: SandboxClient, namespace: str, objects: Iterable[ObjectRef], deadline: float
) -> None:
    """Delete each ``(kind, name, uid)``, the NetworkPolicy only after the rest succeeded.

    A failure does not stop the remaining deletes; the first is raised.
    """

    first: Exception | None = None
    for kind, name, uid in sorted(objects, key=lambda ref: ref[0] == "NetworkPolicy"):
        if kind == "NetworkPolicy" and first is not None:
            continue
        try:
            client.delete_daemon_object(
                namespace, kind, name, uid=uid, request_timeout_seconds=_bound(deadline)
            )
        except Exception as exc:  # noqa: BLE001 - keep going, raise the first below
            first = first or exc
    if first is not None:
        raise first


@dataclass(frozen=True)
class Provisioned:
    """A claim's created daemon: where it answers and what its runner mounts."""

    config: ToolDaemonConfig
    claim: str
    mount: ToolDaemonMount
    # (kind, name, uid) of what this attempt created, for an exact rollback.
    created: tuple[ObjectRef, ...]

    @property
    def host(self) -> str:
        return service_host(self.claim, self.config.namespace)


def provision(
    client: SandboxClient,
    config: ToolDaemonConfig,
    *,
    claim: str,
    release_namespace: str,
    egress_class: str,
    deadline: float,
) -> Provisioned:
    """Create the claim's four daemon objects, each bounded by ``deadline``.

    Any failure deletes exactly the objects this call created, by uid, and
    raises the original error; a 409 on the first create therefore leaves an
    existing daemon at this name untouched.
    """

    certificates = issue(claim, config.namespace, config.max_claim_lifetime_seconds)
    namespace = config.namespace
    created: list[ObjectRef] = []
    try:
        for manifest in (
            _network_policy(config, claim, release_namespace),
            _service(config, claim),
            _pod(config, claim, egress_class),
        ):
            uid = client.create_daemon_object(
                namespace, manifest, request_timeout_seconds=_bound(deadline)
            )
            created.append((manifest["kind"], claim, uid))
        pod_uid = created[-1][2]
        client.create_daemon_object(
            namespace,
            _secret(config, claim, pod_uid, certificates.server_files),
            request_timeout_seconds=_bound(deadline),
        )
    except Exception:
        rollback(client, namespace, created)
        raise
    host = service_host(claim, namespace)
    mount = ToolDaemonMount(
        files=dict(certificates.client_files),
        env={
            "DOCKER_HOST": f"tcp://{host}:{config.port}",
            "DOCKER_PUBLISHED_HOST": host,
            "DOCKER_TLS_VERIFY": "1",
            "DOCKER_CERT_PATH": DOCKER_CERT_DIR,
        },
        active_deadline_seconds=config.max_claim_lifetime_seconds,
    )
    return Provisioned(config=config, claim=claim, mount=mount, created=tuple(created))


def rollback(client: SandboxClient, namespace: str, created: Iterable[ObjectRef]) -> bool:
    """Delete exactly ``created`` in reverse; True when every delete succeeded.

    A failure is logged by class and left to the orphan sweep.
    """

    try:
        _delete(client, namespace, reversed(list(created)), time.monotonic() + _REQUEST_TIMEOUT_S)
    except Exception as exc:  # noqa: BLE001 - never mask the caller's error
        logger.warning("tool daemon rollback left objects (%s)", type(exc).__name__)
        return False
    return True


def retire(client: SandboxClient, config: ToolDaemonConfig, claim: str, *, deadline: float) -> None:
    """Delete ``claim``'s daemon objects, found by exact claim and managed-by labels.

    Idempotent: a retired claim lists nothing. Raises the first failure, with
    the NetworkPolicy left in place as the anchor for a retry.
    """

    selector = _selector(config, claim)
    found = [
        (view.kind, view.name, view.uid)
        for kind in _LISTED_KINDS
        for view in client.list_daemon_objects(
            config.namespace,
            kind,
            label_selector=selector,
            request_timeout_seconds=_bound(deadline),
        )
    ]
    _delete(client, config.namespace, found, deadline)


def sweep_orphans(
    client: SandboxClient,
    config: ToolDaemonConfig,
    *,
    keep: set[str],
    max_age_s: float,
    now: datetime,
    claim_exists: Callable[[str], bool],
) -> list[str]:
    """Retire daemons whose claim never appeared or is gone; return their claims.

    A claim's objects are candidates only when it is not in ``keep``, every
    object's age is known and even the youngest is strictly older than
    ``max_age_s``. ``claim_exists`` then re-reads that one claim; a present
    claim or a failed read spares it. A failed list raises before any delete.
    """

    deadline = time.monotonic() + _REQUEST_TIMEOUT_S
    ages: dict[str, list[datetime | None]] = {}
    for kind in _LISTED_KINDS:
        for view in client.list_daemon_objects(
            config.namespace,
            kind,
            label_selector=_selector(config, None),
            request_timeout_seconds=_bound(deadline),
        ):
            if view.claim:
                ages.setdefault(view.claim, []).append(view.created_at)
    retired: list[str] = []
    for claim, created in ages.items():
        known = [age for age in created if age is not None]
        if claim in keep or len(known) < len(created):
            continue
        if now - max(known) <= timedelta(seconds=max_age_s):
            continue
        try:
            if claim_exists(claim):
                continue
            retire(client, config, claim, deadline=time.monotonic() + _REQUEST_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001 - spared now, retried next tick
            logger.warning("tool daemon sweep spared claim %s (%s)", claim, type(exc).__name__)
            continue
        retired.append(claim)
    return retired


# -- the authenticated ping ----------------------------------------------------------


def _dechunk(body: bytes) -> bytes | None:
    """A chunked body's payload; None while incomplete."""

    out = bytearray()
    while True:
        size_line, sep, rest = body.partition(b"\r\n")
        if not sep:
            return None
        size = int(size_line.split(b";", 1)[0], 16)
        if size == 0:
            return bytes(out) if rest.startswith(b"\r\n") else None
        if len(rest) < size + 2:
            return None
        out += rest[:size]
        body = rest[size + 2 :]


def _response(data: bytes, *, eof: bool) -> tuple[int, bytes] | None:
    """Parse one HTTP/1.1 response; None while more bytes are needed."""

    head, sep, body = data.partition(b"\r\n\r\n")
    if not sep:
        if eof:
            raise ValueError("the daemon closed before a complete response header")
        return None
    status_line, *lines = head.decode("latin-1").split("\r\n")
    status = int(status_line.split(" ", 2)[1])
    headers = {k.strip().lower(): v.strip() for k, _, v in (line.partition(":") for line in lines)}
    if "chunked" in headers.get("transfer-encoding", "").lower():
        decoded = _dechunk(body)
        if decoded is not None:
            return status, decoded
    elif "content-length" in headers:
        length = int(headers["content-length"])
        if len(body) >= length:
            return status, body[:length]
    elif eof:
        return status, body
    if eof:
        raise ValueError("the daemon closed mid-response")
    return None


def _context(files: Mapping[str, str]) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(cadata=files["ca.pem"])
    # ssl loads a client chain only from paths: a private directory, removed
    # as soon as the chain is loaded.
    with tempfile.TemporaryDirectory() as tmp:
        cert, key = Path(tmp, "cert.pem"), Path(tmp, "key.pem")
        cert.write_text(files["cert.pem"])
        key.touch(mode=0o600)
        key.write_text(files["key.pem"])
        context.load_cert_chain(cert, key)
    return context


def request(
    client: SandboxClient,
    namespace: str,
    pod: str,
    port: int,
    server_hostname: str,
    files: Mapping[str, str],
    path: str,
    timeout: float,
) -> tuple[int, bytes]:
    """GET ``path`` from the daemon over mutual TLS, all within ``timeout`` seconds.

    The stream is the client seam's (an API port-forward on Kubernetes); the
    daemon must present a certificate for ``server_hostname`` from the claim's
    CA and accepts only that CA's client certificate.
    """

    deadline = time.monotonic() + timeout
    context = _context(files)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("no time left to reach the tool daemon")
    with client.open_daemon_stream(namespace, pod, port, timeout_seconds=remaining) as raw:
        raw.settimeout(max(0.001, deadline - time.monotonic()))
        with context.wrap_socket(raw, server_hostname=server_hostname) as tls:
            message = f"GET {path} HTTP/1.1\r\nHost: {server_hostname}\r\nConnection: close\r\n\r\n"
            tls.sendall(message.encode())
            data = b""
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("the tool daemon did not answer in time")
                tls.settimeout(remaining)
                chunk = tls.recv(65536)
                data += chunk
                if len(data) > _MAX_RESPONSE_BYTES:
                    raise ValueError("the tool daemon's answer is too large")
                answer = _response(data, eof=not chunk)
                if answer is not None:
                    return answer


def endpoint_present(
    client: SandboxClient, config: ToolDaemonConfig, claim: str, *, deadline: float
) -> bool:
    """Whether ``claim``'s Pod is ready and its Service and policy exist. Fails closed.

    The ping reaches the Pod through the API, so it cannot see an unready or
    terminating Pod (out of the Service's endpoints), a deleted Service (the
    runner's DOCKER_HOST) or policy (the runner's only path to the daemon).
    """

    selector = _selector(config, claim)
    try:
        pod = client.get_daemon_pod(
            config.namespace, claim, request_timeout_seconds=_bound(deadline)
        )
        return pod is not None and pod.ready and all(
            client.list_daemon_objects(
                config.namespace,
                kind,
                label_selector=selector,
                request_timeout_seconds=_bound(deadline),
            )
            for kind in ("Service", "NetworkPolicy")
        )
    except Exception as exc:  # noqa: BLE001 - unknown is not present
        logger.warning("tool daemon endpoint of %s unreadable (%s)", claim, type(exc).__name__)
        return False


def ping(
    client: SandboxClient,
    namespace: str,
    pod: str,
    port: int,
    server_hostname: str,
    files: Mapping[str, str],
    timeout: float,
) -> bool:
    """Whether the daemon answers ``/_ping`` to this claim's certificate. Fails closed."""

    try:
        status, body = request(
            client, namespace, pod, port, server_hostname, files, "/_ping", timeout
        )
    except Exception as exc:  # noqa: BLE001 - any failure is "not healthy"
        logger.info("tool daemon %s ping failed (%s)", pod, type(exc).__name__)
        return False
    return status == 200 and body.strip() == b"OK"


def await_ready(
    client: SandboxClient, provisioned: Provisioned, deadline: float, sleeps: Iterator[float]
) -> None:
    """Wait, within the claim's own ``deadline``, for an authenticated ping.

    A Pod that is gone or has finished fails at once; a transient read is no
    observation.
    """

    config = provisioned.config
    claim = provisioned.claim
    polled = False
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or (polled and remaining < _MIN_POLL_BUDGET_S):
            break
        polled = True
        try:
            pod = client.get_daemon_pod(
                config.namespace, claim, request_timeout_seconds=min(_REQUEST_TIMEOUT_S, remaining)
            )
        except KubeTransientError as exc:
            logger.info("transient read of tool daemon %s (%s)", claim, type(exc).__name__)
        else:
            if pod is None or pod.phase in ("Failed", "Succeeded"):
                raise ClaimTimeoutError(f"tool daemon for claim {claim} stopped before it answered")
            remaining = deadline - time.monotonic()
            if (
                pod.phase == "Running"
                and remaining > 0
                and ping(
                    client,
                    config.namespace,
                    claim,
                    config.port,
                    provisioned.host,
                    provisioned.mount.files,
                    min(config.ping_timeout_seconds, remaining),
                )
            ):
                return
        time.sleep(max(0.0, min(next(sleeps), deadline - time.monotonic())))
    raise ClaimTimeoutError(
        f"tool daemon for claim {claim} did not answer an authenticated ping in time"
    )
