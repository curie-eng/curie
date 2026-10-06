"""Actual authenticated TLS transport, @spec PROTECTED-HOOK-ADMISSION-6/7 PROTECTED-HOOK-
LANE-2/3/SOURCE-6."""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import ipaddress
import json
import os
import secrets
import shutil
import socket
import subprocess
import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from curie_protected_hooks.authority_records import parse_manifest
from curie_protected_hooks.source_policy_records import policy_fingerprint
from redis import Redis
from redis.backoff import NoBackoff
from redis.exceptions import RedisError
from redis.retry import Retry

AGENT = "11111111-1111-4111-8111-111111111111"
HOOK = "incident"
CONTROL = "protected:control:runtime:manifest"
SOURCE = f"protected:source:{AGENT}:{HOOK}"


class FixtureSecret(str):
    """Keep disposable secrets out of assertion locals, @spec PROTECTED-HOOK-ADMISSION-1/7
    PROTECTED-HOOK-LANE-3."""

    def __repr__(self):
        """@spec PROTECTED-HOOK-ADMISSION-1/7 PROTECTED-HOOK-LANE-3."""
        return "<fixture-secret>"


def fail_safely(message):
    """No third party exception locals, @spec PROTECTED-HOOK-ADMISSION-1/7 PROTECTED-HOOK-
    LANE-3."""
    raise pytest.fail.Exception(message, pytrace=False) from None


def write_private(path, payload):
    """Suppress secret-bearing file errors, @spec PROTECTED-HOOK-ADMISSION-1/7 PROTECTED-
    HOOK-LANE-3."""
    try:
        if isinstance(payload, bytes):
            path.write_bytes(payload)
        else:
            path.write_text(payload)
        path.chmod(0o600)
    except OSError:
        fail_safely("owned TLS private file operation failed")


def certificate(private, ca_key, ca_cert, name="127.0.0.1", basename="server"):
    """Independent ephemeral leaf, @spec PROTECTED-HOOK-ADMISSION-6/7 PROTECTED-HOOK-
    LANE-2/3."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    san = x509.IPAddress(ipaddress.ip_address(name))
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fixture")]))
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), False)
        .add_extension(x509.SubjectAlternativeName([san]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    for filename, payload in (
        (basename + ".crt", cert.public_bytes(serialization.Encoding.PEM)),
        (
            basename + ".key",
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
        ),
    ):
        path = private / filename
        write_private(path, payload)
    spki = key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return hashlib.sha256(spki).hexdigest()


class AdmissionBroker:
    """Private disposable fixture ownership, @spec PROTECTED-HOOK-ADMISSION-6/7 PROTECTED-
    HOOK-LANE-2/3."""

    def __repr__(self):
        """@spec PROTECTED-HOOK-ADMISSION-1/7 PROTECTED-HOOK-LANE-3."""
        return "<owned-tls-broker>"

    def docker(self, *args, allow_port_collision=False):
        """Anonymous owned daemon operations, @spec PROTECTED-HOOK-ADMISSION-1/7 PROTECTED-
        HOOK-LANE-3."""
        try:
            result = subprocess.run(
                ["docker", *args], env=self.env, capture_output=True, text=True, timeout=60
            )
        except (OSError, subprocess.TimeoutExpired):
            fail_safely("owned TLS Docker operation failed")
        if result.returncode:
            if (
                allow_port_collision
                and args[0] == "run"
                and any(
                    marker in result.stderr.lower()
                    for marker in ("address already in use", "port is already allocated")
                )
            ):
                return None
            fail_safely("owned TLS Docker operation refused")
        return result.stdout

    def remove_owned(self, owner, cidfile):
        """Validate each failed or finished resource, @spec PROTECTED-HOOK-ADMISSION-1/7
        PROTECTED-HOOK-LANE-3."""
        owned = cidfile.read_text().strip() if cidfile.exists() else self.cid
        if owned:
            assert len(owned) == 64 and all(char in "0123456789abcdef" for char in owned)
            identity = json.loads(self.docker("inspect", owned))[0]
            assert identity["Id"] == owned
            assert identity["Config"]["Labels"]["curie.test.owner"] == owner
            self.docker("rm", "-f", owned)
            assert owned not in self.docker("ps", "-aq", "--no-trunc").splitlines()
        cidfile.unlink(missing_ok=True)
        self.cid = None

    def command(self, *args):
        """Sanitize provisioning failures, @spec PROTECTED-HOOK-ADMISSION-1/7 PROTECTED-HOOK-
        LANE-3."""
        try:
            return self.admin.execute_command(*args)
        except RedisError:
            fail_safely("owned TLS provisioner operation failed")

    def raw_command(self, *args):
        """Exact RESP arrays without Redis callbacks, @spec PROTECTED-HOOK-ADMISSION-4/5/7."""
        connection = None
        try:
            connection = self.admin.connection_pool.get_connection()
            connection.send_command(*args)
            return connection.read_response()
        except RedisError:
            fail_safely("owned admission raw provisioner operation failed")
        finally:
            if connection is not None:
                self.admin.connection_pool.release(connection)

    def ready(self):
        """Actual pinned TLS broker readiness, @spec PROTECTED-HOOK-ADMISSION-6/7 PROTECTED-
        HOOK-LANE-2/3."""
        self.admin.close()
        for attempt in range(60):
            try:
                self.admin.ping()
                return
            except RedisError as error:
                if attempt == 59:
                    fail_safely("owned TLS broker did not become ready: " + type(error).__name__)
                time.sleep(0.1)

    def restart(self):
        """Owned epoch or certificate replacement, @spec PROTECTED-HOOK-ADMISSION-6/7
        PROTECTED-HOOK-LANE-2/3."""
        self.docker("restart", self.cid)
        binding = FixtureSecret(self.docker("port", self.cid, "6379/tcp").strip())
        assert binding == FixtureSecret(f"127.0.0.1:{self.port}")
        self.ready()

    def replace_leaf(self):
        """Actual live TLS context replacement, @spec PROTECTED-HOOK-ADMISSION-6/7 PROTECTED-
        HOOK-LANE-2/3."""
        old_run_id = self.command("INFO", "server")["run_id"]
        self.pin = certificate(self.private, self.ca_key, self.ca_cert, basename="rotated")
        self.command(
            "CONFIG", "SET", "tls-cert-file", "/tls/rotated.crt", "tls-key-file", "/tls/rotated.key"
        )
        self.ready()
        assert self.command("INFO", "server")["run_id"] == old_run_id

    def manifest(self, **changes):
        """Independent closed manifest oracle, @spec PROTECTED-HOOK-LANE-2."""
        identity = {
            "instance_id": "11111111-1111-4111-8111-111111111111",
            "endpoint": {"host": "127.0.0.1", "port": self.port},
            "tls_server_name": "127.0.0.1",
            "tls_spki_sha256": self.pin,
            "run_id": self.command("INFO", "server")["run_id"],
            "database": 0,
        }
        identity.update(changes)
        value = {
            "schema_version": 1,
            "runtime_id": "33333333-3333-4333-8333-333333333333",
            "runtime_generation": "1",
            "broker_identity": identity,
            "worker_image_digest": "sha256:" + "d" * 64,
            "runner_image_digest": "sha256:" + "e" * 64,
            "bundle_digest": {"sha256": "f" * 64, "object_identity": "bundle/example"},
            "execution_config_digest": "1" * 64,
            "qualification_id": "44444444-4444-4444-8444-444444444444",
            "substrate": {
                "kind": "docker",
                "authority_domain_id": "55555555-5555-4555-8555-555555555555",
                "launch_identity": "launch/example",
                "launch_config_sha256": "2" * 64,
            },
            "guard_identity": {
                "control_id": "22222222-2222-4222-8222-222222222222",
                "revision": "1",
                "config_sha256": "c" * 64,
            },
            "credential_refs": {
                role: {"id": "credential/example-" + role, "generation": "1"}
                for role in ("enqueue", "worker", "verifier")
            },
        }
        return parse_manifest(json.dumps(value).encode())


@pytest.fixture(scope="module")
def admission_service(tmp_path_factory):
    """Real TLS, disabled default, owned cleanup, @spec PROTECTED-HOOK-ADMISSION-6/7
    PROTECTED-HOOK-LANE-2/3."""
    if shutil.which("docker") is None:
        fail_safely("Docker is required for actual TLS transport verification")
    fixture = AdmissionBroker()
    fixture.private = tmp_path_factory.mktemp("protected-admission-tls")
    fixture.private.chmod(0o700)
    fixture.cid = None
    fixture.admin = None
    cidfile = fixture.private / "container.cid"
    owner = "curie-admission-test-" + uuid4().hex
    try:
        config = fixture.private / "docker"
        config.mkdir(mode=0o700)
        fixture.env = {key: FixtureSecret(value) for key, value in os.environ.items()}
        fixture.env["DOCKER_CONFIG"] = str(config)
        fixture.env.pop("DOCKER_AUTH_CONFIG", None)
        fixture.env.pop("DOCKER_CONTEXT", None)
        fixture.docker("ps", "--format", "{{.ID}}")
        fixture.passwords = {
            role: FixtureSecret(secrets.token_hex(24)) for role in ("enqueue", "verifier")
        }
        provisioner_password = FixtureSecret(secrets.token_hex(24))
        fixture.ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        now = datetime.now(UTC)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fixture-ca")])
        fixture.ca_cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(subject)
            .public_key(fixture.ca_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
            .add_extension(
                x509.SubjectKeyIdentifier.from_public_key(fixture.ca_key.public_key()), False
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(fixture.ca_key.public_key()),
                False,
            )
            .add_extension(
                x509.KeyUsage(True, False, False, False, False, True, True, False, False), True
            )
            .sign(fixture.ca_key, hashes.SHA256())
        )
        fixture.ca_pem = FixtureSecret(
            fixture.ca_cert.public_bytes(serialization.Encoding.PEM).decode()
        )
        fixture.pin = certificate(fixture.private, fixture.ca_key, fixture.ca_cert)
        ca_path = fixture.private / "ca.crt"
        write_private(ca_path, fixture.ca_pem)
        acl = fixture.private / "users.acl"
        write_private(
            acl,
            "user default off\nuser provisioner on >"
            + provisioner_password
            + " ~* &* +@all\n"
            + "".join(
                "user " + role + " on >" + secret + " " + " ".join(baseline_rules(role)) + "\n"
                for role, secret in fixture.passwords.items()
            ),
        )
        for _attempt in range(5):
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                fixture.port = probe.getsockname()[1]
            started = fixture.docker(
                "run",
                "-d",
                "--name",
                owner,
                "--cidfile",
                str(cidfile),
                "--label",
                "curie.test.owner=" + owner,
                "--user",
                f"{os.getuid()}:{os.getgid()}",
                "-p",
                f"127.0.0.1:{fixture.port}:6379",
                "-v",
                str(fixture.private) + ":/tls:ro",
                "valkey/valkey:8.1.10-alpine",
                "valkey-server",
                "--port",
                "0",
                "--tls-port",
                "6379",
                "--tls-cert-file",
                "/tls/server.crt",
                "--tls-key-file",
                "/tls/server.key",
                "--tls-ca-cert-file",
                "/tls/ca.crt",
                "--tls-auth-clients",
                "no",
                "--aclfile",
                "/tls/users.acl",
                "--save",
                "",
                "--appendonly",
                "no",
                allow_port_collision=True,
            )
            if started is not None:
                break
            fixture.remove_owned(owner, cidfile)
        else:
            fail_safely("owned TLS fixture could not reserve a stable loopback port")
        fixture.cid = cidfile.read_text().strip()
        binding = FixtureSecret(fixture.docker("port", fixture.cid, "6379/tcp").strip())
        assert binding == FixtureSecret(f"127.0.0.1:{fixture.port}")
        fixture.admin = Redis(
            host="127.0.0.1",
            port=fixture.port,
            username="provisioner",
            password=provisioner_password,
            ssl=True,
            ssl_ca_certs=str(ca_path),
            ssl_cert_reqs="required",
            ssl_check_hostname=True,
            protocol=3,
            socket_timeout=2,
            socket_connect_timeout=2,
            retry=Retry(NoBackoff(), 0),
        )
        fixture.ready()
        assert fixture.command("INFO", "server")["valkey_version"] == "8.1.10"
        yield fixture
    finally:
        try:
            if fixture.admin is not None:
                fixture.admin.close()
        finally:
            try:
                fixture.remove_owned(owner, cidfile)
            finally:
                for path in fixture.private.iterdir():
                    if path.is_file():
                        path.unlink()


RESET = ("-@all", "resetkeys", "resetchannels", "clearselectors")
HANDSHAKE = ("+auth", "+hello", "+ping", "+client|setname", "+client|setinfo")


def baseline_rules(role):
    """Independent measured bootstrap, @spec PROTECTED-HOOK-ADMISSION-6/7."""
    reads = (
        "%R~protected:source:*",
        "%R~protected:control:*",
        "+get",
        "+type",
        "+info|server",
        "+time",
    )
    if role == "verifier":
        return (*RESET, *HANDSHAKE, *reads, "(+set %W~protected:control:readiness:*)")
    return (
        *RESET,
        *HANDSHAKE,
        *reads,
        "%RW~protected:admission:intent:*",
        "%RW~protected:admission:state:*",
        "%RW~protected:admission:commit:*",
        "%RW~protected:admission:recovery:*",
        "%RW~protected:admission:binding:*",
        "%RW~protected:admission:quota",
        "%RW~curie:runs",
        "+eval",
        "+set",
        "+del",
        "+zadd",
        "+zcard",
        "+zrem",
        "+zscore",
        "+xinfo|stream",
        "+xrange",
        "+xadd",
        (
            "(+eval %RW~protected:source:* %RW~protected:control:* "
            "%RW~protected:admission:intent:* %RW~protected:admission:state:* "
            "%RW~protected:admission:commit:* %RW~protected:admission:recovery:* "
            "%RW~protected:admission:binding:* "
            "%RW~protected:admission:quota %RW~curie:runs)"
        ),
    )


def canonical(value):
    """Independent canonical oracle, @spec PROTECTED-HOOK-ADMISSION-2/3."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
        "ascii"
    )


def module(name):
    """Missing product is an assertion in the test body, @spec PROTECTED-HOOK-ADMISSION-7."""
    fullname = "curie_protected_hooks." + name
    assert importlib.util.find_spec(fullname) is not None, (
        "protected admission module absent: " + name
    )
    return importlib.import_module(fullname)


def queued_payload(**changes):
    """Unchanged real ACI turn bytes, @spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-
    SOURCE-8."""
    from aci_protocol import QueuedTurn

    value = dict(
        event_id="event/example",
        conversation_id="conversation/example",
        author="hook/example",
        text="anonymous prompt",
        received_at="2026-10-03T00:00:00Z",
        source="webhook",
        attachments=[],
        hook_run=None,
        tool_access="read-only",
        reply_handle=dict(
            kind="slack",
            channel="channel/example",
            placeholder=None,
            adapter="adapter/example",
            endpoint=None,
        ),
    )
    value.update(changes)
    return QueuedTurn.model_validate(value).model_dump_json().encode()


def source_policy():
    """Exact closed policy, @spec PROTECTED-HOOK-ADMISSION-2 PROTECTED-HOOK-SOURCE-6."""
    return dict(
        agent_id=AGENT,
        hook=HOOK,
        generation="1",
        operation_id="22222222-2222-4222-8222-222222222222",
        legacy_generation="0",
        mode="protected",
        tool_access="read-only",
        runtime_id="33333333-3333-4333-8333-333333333333",
        qualification_id="44444444-4444-4444-8444-444444444444",
        bundle_digest="f" * 64,
    )


def request(
    records,
    *,
    delivery="delivery/example",
    policy=None,
    payload=None,
    requested=None,
    body="a" * 64,
):
    """Caller authenticates policy before admission, @spec PROTECTED-HOOK-ADMISSION-1/2."""
    return records.AdmissionRequest(
        identity=records.DeliveryIdentity(agent_id=AGENT, hook=HOOK, delivery_id=delivery),
        source_policy=policy or source_policy(),
        requested_tool_access=requested,
        request_body_sha256=body,
        queued_payload=queued_payload() if payload is None else payload,
    )


def install(broker, acl):
    """Only test provisioner installs product recipes, @spec PROTECTED-HOOK-ADMISSION-6."""
    for role in ("enqueue", "verifier"):
        broker.command("ACL", "SETUSER", role, *acl.admission_acl_rules(role))


def client(broker, role="enqueue"):
    """Explicit caller scoped TLS connection, @spec PROTECTED-HOOK-ADMISSION-1 PROTECTED-
    HOOK-LANE-2."""
    return Redis(
        host="127.0.0.1",
        port=broker.port,
        username=role,
        password=broker.passwords[role],
        ssl=True,
        ssl_ca_certs=str(broker.private / "ca.crt"),
        ssl_cert_reqs="required",
        ssl_check_hostname=True,
        protocol=3,
        socket_timeout=2,
        socket_connect_timeout=2,
        retry=Retry(NoBackoff(), 0),
    )


def seed(broker):
    """Independent provisioner authority, @spec PROTECTED-HOOK-ADMISSION-4 PROTECTED-HOOK-
    LANE-2 PROTECTED-HOOK-SOURCE-6."""
    m = broker.manifest().as_dict()
    md = hashlib.sha256(canonical(m)).hexdigest()
    q = {
        key: m[key]
        for key in (
            "schema_version",
            "runtime_id",
            "runtime_generation",
            "qualification_id",
            "broker_identity",
            "execution_config_digest",
            "guard_identity",
        )
    }
    q.update(
        qualification_generation="1",
        manifest_digest=md,
        measurement_record_id="measurement/qualification",
    )
    now = broker.command("TIME")
    now_ms = int(now[0]) * 1000 + int(now[1]) // 1000
    r = {
        key: q[key]
        for key in (
            "schema_version",
            "runtime_id",
            "runtime_generation",
            "qualification_id",
            "qualification_generation",
            "broker_identity",
            "guard_identity",
            "manifest_digest",
        )
    }
    r.update(
        verifier_identity=m["credential_refs"]["verifier"],
        issued_at_ms=str(now_ms),
        expires_at_ms=str(now_ms + 60000),
        measurement_record_id="measurement/readiness",
    )
    selection = dict(
        schema_version=1,
        runtime_id=m["runtime_id"],
        runtime_generation=m["runtime_generation"],
        manifest_digest=md,
        qualification_id=m["qualification_id"],
        qualification_generation="1",
        broker_run_id=m["broker_identity"]["run_id"],
        admission_open=True,
    )
    p = source_policy()
    source = dict(
        floor=p["generation"],
        operation_id=p["operation_id"],
        active=dict(
            generation=p["generation"],
            operation_id=p["operation_id"],
            mode="protected",
            policy_fingerprint=policy_fingerprint(p),
        ),
    )
    values = {
        SOURCE: source,
        "protected:control:selection:" + m["runtime_id"]: selection,
        "protected:control:manifest:" + md: m,
        "protected:control:qualification:" + m["qualification_id"] + ":1": q,
        "protected:control:readiness:" + m["runtime_id"] + ":" + m["runtime_generation"]: r,
    }
    for key, value in values.items():
        broker.command("SET", key, canonical(value))
    return values


def snapshot(broker):
    """Exact all-key/value/type/TTL snapshot, @spec PROTECTED-HOOK-ADMISSION-4/5/7."""
    return {
        key: (broker.command("TYPE", key), broker.command("DUMP", key), broker.command("PTTL", key))
        for key in broker.command("KEYS", "*")
    }


def facade(broker, atomic, limit=100):
    """Explicit trusted immutable tuple: the provisioner's trusted manifest in place of a
    bare broker identity, @spec PROTECTED-HOOK-ADMISSION-1."""
    return atomic.AtomicAdmission(
        client(broker),
        trusted_manifest=broker.manifest(),
        trusted_max_readiness_ms=60000,
        backlog_limit=limit,
    )


def safe_error(error, broker):
    """Public exception confidentiality oracle, @spec PROTECTED-HOOK-ADMISSION-1."""
    assert str(error) == "protected admission unavailable"
    assert error.__cause__ is None
    assert error.__context__ is None or error.__suppress_context__
    for secret in (
        *broker.passwords.values(),
        broker.ca_pem,
        "127.0.0.1",
        str(broker.port),
        "anonymous prompt",
    ):
        assert secret not in repr(error)


@pytest.fixture
def admission_broker(admission_service):
    """Fresh state and baseline independent of product, @spec PROTECTED-HOOK-ADMISSION-7."""
    admission_service.command("FLUSHDB")
    for role in ("enqueue", "verifier"):
        admission_service.command("ACL", "SETUSER", role, *baseline_rules(role))
    yield admission_service
    for role in ("enqueue", "verifier"):
        admission_service.command("CLIENT", "KILL", "USER", role, "SKIPME", "yes")


class ResponseDropRelay:
    """Owned TLS transport drops one actual commit reply, @spec PROTECTED-HOOK-ADMISSION-1/7."""

    def __init__(self, broker, commit_key):
        """@spec PROTECTED-HOOK-ADMISSION-1/7."""
        self.broker = broker
        self.commit_key = commit_key
        self.listener = None
        self.downstream = None
        self.upstream = None
        self.thread = None
        self.dropped = False
        self.error = False

    def __repr__(self):
        """@spec PROTECTED-HOOK-ADMISSION-1."""
        return "<owned-response-drop-relay>"

    @staticmethod
    def frame(reader):
        """Only framing, never synthesize broker replies, @spec PROTECTED-HOOK-ADMISSION-7."""
        first = reader.readline(1048576)
        if not first or not first.endswith(b"\r\n"):
            raise OSError("closed transport")
        tag, body = first[:1], first[1:-2]
        if tag in (b"$", b"!", b"="):
            size = int(body)
            if size == -1:
                return first
            if not 0 <= size <= 1048576:
                raise OSError("frame bound")
            rest = reader.read(size + 2)
            if len(rest) != size + 2:
                raise OSError("closed transport")
            return first + rest
        if tag in (b"*", b"~", b">", b"%", b"|"):
            count = int(body)
            if count == -1:
                return first
            if not 0 <= count <= 1024:
                raise OSError("frame bound")
            if tag in (b"%", b"|"):
                count *= 2
            return first + b"".join(ResponseDropRelay.frame(reader) for _ in range(count))
        if tag not in (b"+", b"-", b":", b"_", b"#", b",", b"("):
            raise OSError("unsupported frame")
        return first

    def run(self):
        """Real execution before response loss, @spec PROTECTED-HOOK-ADMISSION-7."""
        import ssl

        try:
            server = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server.minimum_version = ssl.TLSVersion.TLSv1_2
            server.load_cert_chain(
                str(self.broker.private / "server.crt"), str(self.broker.private / "server.key")
            )
            peer, _ = self.listener.accept()
            peer.settimeout(5)
            self.downstream = server.wrap_socket(peer, server_side=True)
            trusted = ssl.create_default_context(cafile=str(self.broker.private / "ca.crt"))
            trusted.minimum_version = ssl.TLSVersion.TLSv1_2
            self.upstream = trusted.wrap_socket(
                socket.create_connection(("127.0.0.1", self.broker.port), timeout=5),
                server_hostname="127.0.0.1",
            )
            with (
                self.downstream.makefile("rb") as commands,
                self.upstream.makefile("rb") as replies,
            ):
                while True:
                    command = self.frame(commands)
                    self.upstream.sendall(command)
                    reply = self.frame(replies)
                    if b"\r\n$4\r\nEVAL\r\n" in command[:32] and self.broker.command(
                        "EXISTS", self.commit_key
                    ):
                        self.dropped = True
                        self.downstream.shutdown(socket.SHUT_RDWR)
                        break
                    self.downstream.sendall(reply)
        except (OSError, ValueError, RedisError):
            if not self.dropped:
                self.error = True
        finally:
            for connection in (self.downstream, self.upstream):
                if connection is not None:
                    connection.close()

    def __enter__(self):
        """Register cleanup before opening owned sockets, @spec PROTECTED-HOOK-ADMISSION-7."""
        import threading

        try:
            self.listener = socket.socket()
            self.listener.bind(("127.0.0.1", 0))
            self.port = self.listener.getsockname()[1]
            self.listener.settimeout(5)
            self.listener.listen(1)
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
            return self
        except OSError:
            self.__exit__(None, None, None)
            fail_safely("owned response relay startup failed")

    def __exit__(self, *_):
        """Only owned sockets and thread cleanup, @spec PROTECTED-HOOK-ADMISSION-7."""
        for connection in (self.listener, self.downstream, self.upstream):
            if connection is not None:
                connection.close()
        if self.thread is not None:
            self.thread.join(timeout=6)
            if self.thread.is_alive() or self.error:
                fail_safely("owned response relay transport failed")
