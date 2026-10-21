"""Protected broker of a local commissioning installation, @spec PROTECTED-COMMISSIONING-1.

The provisioner installs one TLS Valkey Pod in the installation's protected
namespace. It starts with only its default user, holding the administrator's
one-time installer password; the administrator bootstrap
(``commissioning_local_bootstrap``) then creates every ACL user with the exact
rules G hashes and turns the installer off. The candidate users start off
with no rights: G holds their design decision 9 templates, which
``enable_candidates`` instantiates for one permit's lane. The provisioner
holds only usernames and rules (``Principal``) and its own credential.

The broker's CA and server key are generated per installation and the server
certificate names the in-cluster Service, which is the manifest's
``tls_server_name``. The Pod has no controller: a restart would change
``run_id``, which every pinned session refuses, and would drop every ACL user,
so nothing restarts it silently. The provisioner reaches the broker through a
port-forward of its own kubeconfig and still checks the server name, SPKI pin
and ``run_id``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import select
import socket
import ssl
import subprocess
import threading
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn, Self

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from redis import ConnectionPool, Redis
from redis.backoff import NoBackoff
from redis.connection import SSLConnection
from redis.retry import Retry

from .authority_records import _BROKER, _object
from .commissioning_acl import (
    CANDIDATE_ROLES,
    commissioning_acl_rules,
    commissioning_acl_template,
    derive_commissioning_lane,
    instantiate_candidate_rules,
)
from .commissioning_local_installation import OWNER_LABEL, LocalCluster
from .commissioning_provisioner import (
    GUARD_PRINCIPALS,
    BrokerAclRecipes,
    ProvisionerProfile,
    _pinned,
    _principal_credentials,
    _rules_digest,
    parse_guard_config,
)

__all__ = [
    "BROKER_NAME",
    "BROKER_PORT",
    "INSTALLATION_PRINCIPALS",
    "VALKEY_VERSION",
    "BrokerMaterial",
    "BrokerRefused",
    "BrokerUser",
    "LocalBroker",
    "broker_objects",
    "check_guard_binding",
    "enable_candidates",
    "pinned_session",
    "broker_principals",
    "CANDIDATE_IDLE",
    "Principal",
    "server_name",
]

# @spec PROTECTED-COMMISSIONING-1
BROKER_NAME = "commissioning-broker"
BROKER_PORT = 6379
VALKEY_VERSION = "8.1.10"
# The one broker user outside G: the provisioner, the party G and the observer check.
INSTALLATION_PRINCIPALS = ("provisioner",)
# Design decision 9: candidates stay off with no rules until a campaign's permit.
CANDIDATE_IDLE = ("-@all", "resetkeys", "resetchannels", "clearselectors")
# Valkey's default user holds the one-time installer password until enrollment ends.
_INSTALLER_KEY = "password"
_SECRET = BROKER_NAME + "-tls"
_APP_LABEL = "app.kubernetes.io/name"
_VALKEY_UID = 999
_CERT_DAYS = 30
_FORWARD = re.compile(rb"Forwarding from 127\.0\.0\.1:([0-9]{1,5}) -> 6379")


class BrokerRefused(RuntimeError):
    """Redacted broker refusal, @spec PROTECTED-COMMISSIONING-1."""

    def __init__(self) -> None:
        """Never echo a credential, rule or command output, @spec PROTECTED-COMMISSIONING-1."""
        super().__init__("Commissioning broker refused")


def _refuse() -> NoReturn:
    raise BrokerRefused() from None


def _require(condition: bool) -> None:
    if not condition:
        _refuse()


@dataclass(frozen=True, slots=True)
class Principal:
    """One ACL user as the provisioner knows it: username and rules, no password.

    @spec PROTECTED-COMMISSIONING-1.
    """

    username: str
    rules: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BrokerUser:
    """One ACL user's credential: the credential ID as username, its rules and password.

    @spec PROTECTED-COMMISSIONING-1.
    """

    username: str
    rules: tuple[str, ...]
    password: str = field(repr=False)


def broker_principals(
    recipes: BrokerAclRecipes,
    profile: ProvisionerProfile,
    *,
    provisioner_id: str,
) -> dict[str, Principal]:
    """Every broker user's username and rules, @spec PROTECTED-COMMISSIONING-1.

    The broker runs before B, G and M exist, because M pins its live
    ``run_id``. G's principals therefore take the credential IDs the profile
    gives B and G (the observer's), and the recipes the provisioner will hash
    into G; ``check_guard_binding`` confirms both once G is built. For the
    candidates those recipes are design decision 9 templates. The provisioner
    takes its merged commissioning recipe. Usernames are pairwise distinct and
    never ``default``. Passwords are the administrator bootstrap's.
    """
    try:
        _require(type(profile) is ProvisionerProfile)
        credentials = _principal_credentials(
            {
                "production_credentials": profile.production_credentials,
                "issuer_identity": profile.issuer_identity,
                "control_identity": profile.control_identity,
                "qualification_publisher_identity": profile.qualification_publisher_identity,
                "candidate_credentials": profile.candidate_credentials,
            }
        )
        credentials["observer"] = profile.observer_identity
        names = {name: credentials[name]["id"] for name in GUARD_PRINCIPALS}
        names.update(provisioner=provisioner_id)
        rules = {name: recipes.rules(name) for name in GUARD_PRINCIPALS}
        rules.update({name: commissioning_acl_rules(name) for name in INSTALLATION_PRINCIPALS})
        _require(len(set(names.values())) == len(names) and "default" not in names.values())
        principals = {}
        for name, username in names.items():
            _require(
                type(username) is str and re.fullmatch(r"[\x21-\x7e]{1,256}", username) is not None
            )
            _rules_digest(rules[name])
            principals[name] = Principal(username, tuple(rules[name]))
        return principals
    except BrokerRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse()


def check_guard_binding(
    principals: Mapping[str, Principal | BrokerUser], guard_config: bytes
) -> None:
    """Every G principal's username and rules are the ones G hashed.

    @spec PROTECTED-COMMISSIONING-1 PROTECTED-COMMISSIONING-2.
    """
    try:
        acl = parse_guard_config(guard_config)["broker_acl"]
        for name in GUARD_PRINCIPALS:
            _require(acl[name]["credential"]["id"] == principals[name].username)
            _require(acl[name]["rules_sha256"] == _rules_digest(principals[name].rules))
    except BrokerRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse()


def _spki_sha256(certificate: x509.Certificate) -> str:
    spki = certificate.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return hashlib.sha256(spki).hexdigest()


@dataclass(frozen=True, slots=True, repr=False)
class BrokerMaterial:
    """Per-installation CA and server key; the CA key is discarded after signing.

    @spec PROTECTED-COMMISSIONING-1.
    """

    server_name: str
    ca_pem: str
    certificate_pem: str
    key_pem: str = field(repr=False)
    spki_sha256: str

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-1."""
        return "BrokerMaterial()"

    @classmethod
    def generate(cls, server_name: str) -> Self:
        """@spec PROTECTED-COMMISSIONING-1."""
        now = datetime.now(UTC)
        ca_key = ec.generate_private_key(ec.SECP256R1())
        ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "commissioning-broker-ca")])
        ca = (
            x509.CertificateBuilder()
            .subject_name(ca_name)
            .issuer_name(ca_name)
            .public_key(ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=_CERT_DAYS))
            .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
            .add_extension(
                x509.KeyUsage(False, False, False, False, False, True, True, False, False), True
            )
            .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
            .sign(ca_key, hashes.SHA256())
        )
        key = ec.generate_private_key(ec.SECP256R1())
        leaf = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, BROKER_NAME)]))
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=_CERT_DAYS))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(server_name)]), False)
            .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
            .add_extension(
                x509.KeyUsage(True, False, False, False, False, False, False, False, False), True
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
            )
            .sign(ca_key, hashes.SHA256())
        )
        pem = serialization.Encoding.PEM
        return cls(
            server_name,
            ca.public_bytes(pem).decode("ascii"),
            leaf.public_bytes(pem).decode("ascii"),
            key.private_bytes(
                pem, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            ).decode("ascii"),
            _spki_sha256(leaf),
        )


def server_name(namespace: str) -> str:
    """The broker Service's cluster DNS name, @spec PROTECTED-COMMISSIONING-1."""
    return f"{BROKER_NAME}.{namespace}.svc.cluster.local"


def _b64(value: str | bytes) -> str:
    raw = value.encode("ascii") if type(value) is str else value
    assert type(raw) is bytes
    return base64.b64encode(raw).decode("ascii")


def _namespace(owner: str, namespace: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {
                OWNER_LABEL: owner,
                "pod-security.kubernetes.io/enforce": "restricted",
                "pod-security.kubernetes.io/enforce-version": "latest",
            },
        },
    }


def broker_objects(
    *, owner: str, namespace: str, image: str, material: BrokerMaterial, installer_secret: str
) -> list[dict[str, Any]]:
    """Namespace, Secret, Service and Pod, in creation order, @spec PROTECTED-COMMISSIONING-1.

    The image is pinned by digest. The Pod runs ``valkey-server`` directly as
    the image's unprivileged user with a read-only root filesystem, no service
    account token, no persistence and TLS only.
    """
    _pinned(image)
    _require(material.server_name == server_name(namespace))
    owned = {OWNER_LABEL: owner}
    selector = {_APP_LABEL: BROKER_NAME, OWNER_LABEL: owner}
    return [
        _namespace(owner, namespace),
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": _SECRET, "namespace": namespace, "labels": owned},
            "type": "Opaque",
            "immutable": True,
            "data": {
                "ca.crt": _b64(material.ca_pem),
                "tls.crt": _b64(material.certificate_pem),
                "tls.key": _b64(material.key_pem),
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {"name": BROKER_NAME, "namespace": namespace, "labels": owned},
            "spec": {
                "type": "ClusterIP",
                "selector": selector,
                "ports": [{"name": "tls", "port": BROKER_PORT, "targetPort": BROKER_PORT}],
            },
        },
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": BROKER_NAME, "namespace": namespace, "labels": selector},
            "spec": {
                "restartPolicy": "Never",
                "automountServiceAccountToken": False,
                "enableServiceLinks": False,
                "securityContext": {
                    "runAsNonRoot": True,
                    "runAsUser": _VALKEY_UID,
                    "runAsGroup": _VALKEY_UID,
                    "fsGroup": _VALKEY_UID,
                    "seccompProfile": {"type": "RuntimeDefault"},
                },
                "containers": [
                    {
                        "name": "valkey",
                        "image": image,
                        "command": ["valkey-server"],
                        "args": [
                            "--port",
                            "0",
                            "--tls-port",
                            str(BROKER_PORT),
                            "--tls-cert-file",
                            "/tls/tls.crt",
                            "--tls-key-file",
                            "/tls/tls.key",
                            "--tls-ca-cert-file",
                            "/tls/ca.crt",
                            "--tls-auth-clients",
                            "no",
                            "--requirepass",
                            "$(INSTALLER_PASSWORD)",
                            "--save",
                            "",
                            "--appendonly",
                            "no",
                            "--dir",
                            "/data",
                        ],  # fmt: skip
                        "env": [
                            {
                                "name": "INSTALLER_PASSWORD",
                                "valueFrom": {
                                    "secretKeyRef": {
                                        "name": installer_secret,
                                        "key": _INSTALLER_KEY,
                                    }
                                },
                            }
                        ],
                        "ports": [{"containerPort": BROKER_PORT, "name": "tls"}],
                        "readinessProbe": {
                            "tcpSocket": {"port": BROKER_PORT},
                            "periodSeconds": 2,
                        },
                        "resources": {
                            "requests": {"cpu": "100m", "memory": "64Mi"},
                            "limits": {"cpu": "250m", "memory": "128Mi"},
                        },
                        "securityContext": {
                            "allowPrivilegeEscalation": False,
                            "readOnlyRootFilesystem": True,
                            "capabilities": {"drop": ["ALL"]},
                        },
                        "volumeMounts": [
                            {"name": "tls", "mountPath": "/tls", "readOnly": True},
                            {"name": "data", "mountPath": "/data"},
                        ],
                    }
                ],
                "volumes": [
                    {"name": "tls", "secret": {"secretName": _SECRET, "defaultMode": 0o440}},
                    {"name": "data", "emptyDir": {"sizeLimit": "16Mi"}},
                ],
            },
        },
    ]


class _ForwardedConnection(SSLConnection):
    """TLS to the Service name over the provisioner's port-forward, SPKI pinned."""

    def __init__(self, *, dial: tuple[str, int], spki_sha256: str, **kwargs: Any) -> None:
        self._dial = dial
        self._spki = spki_sha256
        super().__init__(**kwargs)

    def _connect(self) -> socket.socket:
        sock = socket.create_connection(self._dial, timeout=self.socket_connect_timeout)
        sock.settimeout(self.socket_timeout)
        try:
            secured: ssl.SSLSocket = self._wrap_socket_with_ssl(sock)  # type: ignore[no-untyped-call]
            certificate = secured.getpeercert(binary_form=True)
            _require(certificate is not None)
            assert certificate is not None
            observed = _spki_sha256(x509.load_der_x509_certificate(certificate))
            _require(hmac.compare_digest(observed, self._spki))
            return secured
        except Exception:
            sock.close()
            raise


def pinned_session(
    material: BrokerMaterial, dial: tuple[str, int], user: BrokerUser, *, run_id: str | None = None
) -> Redis:
    """A TLS session as ``user`` to the broker's Service name, SPKI pinned.

    ``dial`` is where the socket connects (the provisioner's port-forward);
    the certificate must still name the Service and match the pin. With
    ``run_id`` the live epoch must equal it. @spec PROTECTED-COMMISSIONING-1.
    """
    pool = ConnectionPool(
        connection_class=_ForwardedConnection,
        dial=dial,
        spki_sha256=material.spki_sha256,
        host=material.server_name,
        port=BROKER_PORT,
        username=user.username,
        password=user.password,
        ssl_ca_data=material.ca_pem,
        ssl_cert_reqs=ssl.CERT_REQUIRED,
        ssl_check_hostname=True,
        socket_timeout=5.0,
        socket_connect_timeout=5.0,
        retry=Retry(NoBackoff(), 0),
    )
    client = Redis(connection_pool=pool, single_connection_client=True)
    if run_id is not None:
        try:
            _require(client.info("server").get("run_id") == run_id)
        except BaseException:
            client.close()
            raise
    return client


def enable_candidates(
    session: Redis, principals: Mapping[str, Principal | BrokerUser], permit: Any
) -> None:
    """Enable the candidates with their templates instantiated for the permit's lane.

    Design decision 9: each candidate gets exactly its template for the lane
    the permit derives, and keeps the password hash installed with it. Runs
    on a provisioner session, the one principal holding ``ACL SETUSER``.
    @spec PROTECTED-COMMISSIONING-1.
    """
    try:
        lane = derive_commissioning_lane(permit)
        for name in CANDIDATE_ROLES:
            _require(principals[name].rules == commissioning_acl_template(name))
            rules = instantiate_candidate_rules(name, lane)
            reply = session.execute_command(  # type: ignore[no-untyped-call]
                "ACL", "SETUSER", principals[name].username, "on", *rules
            )
            _require(reply in (b"OK", "OK", True))
    except BrokerRefused:
        raise
    except Exception:  # noqa: BLE001  One redacted refusal.
        _refuse()


def _drain(stream: Any) -> None:
    """Read and discard: kubectl logs one line per forwarded connection."""
    try:
        for _ in iter(lambda: stream.read(65536), b""):
            pass
    except (OSError, ValueError):
        pass


def _start_forward(
    argv: list[str], environment: Mapping[str, str]
) -> tuple[subprocess.Popen[bytes], int]:
    """Start a port-forward and return it with its loopback port.

    The first line must announce the forward. Every later line is drained on
    a daemon thread for the life of the process, so the pipe never fills and
    stalls the forward. Any failure stops the process. @spec PROTECTED-COMMISSIONING-1.
    """
    process = subprocess.Popen(  # noqa: S603  argv comes from ClusterCommands' allowlist.
        argv, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, env=dict(environment)
    )
    try:
        assert process.stdout is not None
        ready, _, _ = select.select([process.stdout], [], [], 30)
        line = process.stdout.readline() if ready else b""
        match = _FORWARD.match(line)
        _require(match is not None)
        assert match is not None
        threading.Thread(target=_drain, args=(process.stdout,), daemon=True).start()
        return process, int(match.group(1))
    except BaseException:
        _stop(process)
        raise


def _stop(process: subprocess.Popen[bytes]) -> None:
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)


class LocalBroker:
    """Install the broker and open pinned provisioner-side sessions.

    @spec PROTECTED-COMMISSIONING-1.
    """

    def __init__(self, cluster: LocalCluster, *, namespace: str) -> None:
        """@spec PROTECTED-COMMISSIONING-1."""
        _require(type(cluster) is LocalCluster)
        self.cluster = cluster
        self.namespace = namespace
        self.identity: dict[str, Any] | None = None
        self._material: BrokerMaterial | None = None
        self._forward: subprocess.Popen[bytes] | None = None
        self._port: int | None = None

    def __repr__(self) -> str:
        """@spec PROTECTED-COMMISSIONING-1."""
        return "LocalBroker()"

    def install_namespace(self) -> None:
        """The protected namespace, before any credential Secret exists in it.

        @spec PROTECTED-COMMISSIONING-1.
        """
        self.cluster.create(_namespace(self.cluster.owner, self.namespace))

    def install(self, *, image: str, installer_secret: str) -> None:
        """Create the TLS Secret, Service and Pod, wait for TLS, open the forward.

        The broker starts with only its default user, holding the
        administrator's one-time installer password from ``installer_secret``.
        A failure after the forward opens closes it. @spec PROTECTED-COMMISSIONING-1.
        """
        material = BrokerMaterial.generate(server_name(self.namespace))
        self._material = material
        objects = broker_objects(
            owner=self.cluster.owner,
            namespace=self.namespace,
            image=image,
            material=material,
            installer_secret=installer_secret,
        )
        for manifest in objects[1:]:
            self.cluster.create(manifest)
        self.cluster.kubectl(
            "wait",
            "--for=condition=Ready",
            "pod/" + BROKER_NAME,
            "--namespace",
            self.namespace,
            "--timeout",
            "180s",
            timeout=200,
        )
        self._open_forward()

    def observe(self, provisioner: BrokerUser) -> dict[str, Any]:
        """The live ``BrokerIdentity`` M pins, read as the enrolled provisioner.

        @spec PROTECTED-COMMISSIONING-1.
        """
        material = self.material
        try:
            session = self.session(provisioner)
            try:
                whoami = session.execute_command("ACL", "WHOAMI")  # type: ignore[no-untyped-call]
                _require(whoami == provisioner.username.encode())
                info = session.info("server")
                _require(info.get("valkey_version") == VALKEY_VERSION)
                run_id = info.get("run_id")
            finally:
                session.close()
            identity = {
                "instance_id": str(uuid.uuid4()),
                "endpoint": {"host": material.server_name, "port": BROKER_PORT},
                "tls_server_name": material.server_name,
                "tls_spki_sha256": material.spki_sha256,
                "run_id": run_id,
                "database": 0,
            }
            _object(identity, _BROKER)
        except Exception:  # noqa: BLE001  One redacted refusal; the forward never outlives it.
            self.close()
            _refuse()
        self.identity = identity
        return identity

    @property
    def material(self) -> BrokerMaterial:
        """The broker's public TLS identity and installation CA, @spec PROTECTED-COMMISSIONING-1."""
        _require(self._material is not None)
        assert self._material is not None
        return self._material

    @property
    def dial(self) -> tuple[str, int]:
        """The provisioner's loopback forward to the broker, @spec PROTECTED-COMMISSIONING-1."""
        _require(self._port is not None)
        assert self._port is not None
        return ("127.0.0.1", self._port)

    @property
    def ca_pem(self) -> str:
        """The installation CA every pinned client trusts, @spec PROTECTED-COMMISSIONING-1."""
        _require(self._material is not None)
        assert self._material is not None
        return self._material.ca_pem

    def _open_forward(self) -> None:
        argv = self.cluster.commands.kubectl_argv(
            "port-forward",
            "pod/" + BROKER_NAME,
            ":" + str(BROKER_PORT),
            "--namespace",
            self.namespace,
        )
        self._forward, self._port = _start_forward(
            argv, self.cluster.commands.environment(os.environ)
        )

    def session(self, user: BrokerUser, *, run_id: str | None = None) -> Redis:
        """A pinned session as ``user`` over the port-forward, @spec PROTECTED-COMMISSIONING-1."""
        _require(self._material is not None and self._port is not None)
        assert self._material is not None and self._port is not None
        return pinned_session(self._material, ("127.0.0.1", self._port), user, run_id=run_id)

    def start_campaign(
        self, provisioner: BrokerUser, principals: Mapping[str, Principal], permit: Any
    ) -> None:
        """``enable_candidates`` on a provisioner session, @spec PROTECTED-COMMISSIONING-1."""
        session = self.session(provisioner)
        try:
            enable_candidates(session, principals, permit)
        finally:
            session.close()

    def close(self) -> None:
        """Stop the port-forward; the objects are the cluster ledger's to remove.

        @spec PROTECTED-COMMISSIONING-1.
        """
        process, self._forward, self._port = self._forward, None, None
        if process is not None:
            _stop(process)
