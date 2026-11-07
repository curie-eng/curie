"""Black-box portable container proof, with real TLS and scoped Valkey ACLs."""

from __future__ import annotations

import base64
import datetime as dt
import io
import json
import os
import subprocess
import tarfile
import threading
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from curie_worker import caller_token, connector_grant
from nacl.signing import SigningKey

ROOT = Path(__file__).resolve().parents[3]
PASSWORD = "container-test-password"


def wait_until(check: Any, timeout: int = 30) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        threading.Event().wait(0.05)
    raise RuntimeError("container readiness deadline expired")


def certificates(directory: Path) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "connector-test")])
    now = dt.datetime.now(dt.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=1))
        .not_valid_after(now + dt.timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("valkey"), x509.DNSName("localhost")]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    (directory / "cert.pem").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    (directory / "key.pem").write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )


@dataclass
class Host:
    docker: Any
    container: str
    base: str
    uploads: str
    seed: str
    ca: Path


@pytest.fixture(scope="module")
def host(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Host]:
    directory = tmp_path_factory.mktemp("connector-host")
    directory.chmod(0o755)
    certificates(directory)
    token = uuid.uuid4().hex[:12]
    label = f"curie-connector-test={token}"
    network = f"connector-test-{token}"
    image = f"connector-host-test:{token}"
    containers = [f"connector-valkey-{token}", f"connector-host-{token}"]
    context = os.environ.get("CURIE_TEST_DOCKER_CONTEXT")
    prefix = ["docker", *(["--context", context] if context else [])]

    def docker(*args: str, check: bool = True, data: bytes | None = None) -> Any:
        result = subprocess.run(
            [*prefix, *args], input=data, capture_output=True, timeout=600, check=False
        )
        if check and result.returncode:
            raise RuntimeError(
                f"container command {args[0]} failed: {result.stderr.decode()[-2000:]}"
            )
        return result

    try:
        docker("version")
        archive = io.BytesIO()
        files = subprocess.check_output(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=ROOT
        )
        with tarfile.open(fileobj=archive, mode="w:gz") as tar:
            for relative in files.decode().split("\0"):
                path = ROOT / relative
                if relative and path.is_file():
                    tar.add(path, arcname=relative, recursive=False)
        docker(
            "build",
            "--label",
            label,
            "-t",
            image,
            "-f",
            "apps/worker/Connector.Dockerfile",
            "-",
            data=archive.getvalue(),
        )
        docker("network", "create", "--label", label, network)
        (directory / "users.acl").write_text(
            f"user default off\nuser reviewer on >{PASSWORD} ~connector-grant:reviewer:* +set\n"
        )
        (directory / "valkey.conf").write_text(
            "port 0\ntls-port 6379\n"
            "tls-cert-file /config/cert.pem\ntls-key-file /config/key.pem\n"
            "tls-ca-cert-file /config/cert.pem\ntls-auth-clients no\n"
            'aclfile /config/users.acl\nsave ""\nappendonly no\n'
        )
        docker(
            "create",
            "--name",
            containers[0],
            "--label",
            label,
            "--network",
            network,
            "--network-alias",
            "valkey",
            "valkey/valkey:8.1.10-alpine",
            "valkey-server",
            "/config/valkey.conf",
        )
        docker("cp", str(directory), f"{containers[0]}:/config")
        docker("start", containers[0])
        wait_until(
            lambda: (
                docker(
                    "exec",
                    containers[0],
                    "valkey-cli",
                    "--tls",
                    "--cacert",
                    "/config/cert.pem",
                    "--user",
                    "reviewer",
                    "--pass",
                    PASSWORD,
                    "SET",
                    "connector-grant:reviewer:ready",
                    "1",
                    "EX",
                    "1",
                    check=False,
                ).stdout.strip()
                == b"OK"
            )
        )
        seed_bytes = os.urandom(32)
        seed = base64.b64encode(seed_bytes).decode()
        public = base64.b64encode(bytes(SigningKey(seed_bytes).verify_key)).decode()
        config = {
            "version": 1,
            "connector": "reviewer",
            "listener": {
                "host": "0.0.0.0",
                "port": 8080,
                "tls": {"cert_file": "/config/cert.pem", "key_file": "/config/key.pem"},
            },
            "implementation": {
                "command": ["python3", "/opt/connector/implementation.py"],
                "port": 8000,
            },
            "public_keys": [public],
            "admits": ["acme-dev"],
            "gated_tools": ["reviewer/write*"],
            "routes": [],
            "grant_store": {
                "host": "valkey",
                "username": "reviewer",
                "ca_file": "/config/cert.pem",
            },
            "upload": {
                "listener": {"host": "0.0.0.0", "port": 8081},
                "upstream_port": 8001,
                "path": "/v1/upload",
                "max_body_bytes": 1024,
            },
        }
        (directory / "serve.json").write_text(json.dumps(config))
        docker(
            "create",
            "--name",
            containers[1],
            "--label",
            label,
            "--network",
            network,
            "-p",
            "127.0.0.1::8080",
            "-p",
            "127.0.0.1::8081",
            "-e",
            "CURIE_CONNECTOR_SERVE_CONFIG=/config/serve.json",
            "-e",
            f"CURIE_CONNECTOR_GRANT_STORE_PASSWORD={PASSWORD}",
            image,
        )
        docker("cp", str(directory), f"{containers[1]}:/config")
        docker(
            "cp",
            str(ROOT / "apps/worker/tests/fixtures/connector_host_implementation.py"),
            f"{containers[1]}:/opt/connector/implementation.py",
        )
        docker("start", containers[1])
        wait_until(
            lambda: (
                docker("exec", containers[1], "test", "-f", "/tmp/ready", check=False).returncode
                == 0
            )
        )

        def address(port: int, scheme: str) -> str:
            published = docker("port", containers[1], str(port)).stdout.decode().strip()
            return f"{scheme}://localhost:{published.rsplit(':', 1)[1]}"

        yield Host(
            docker,
            containers[1],
            address(8080, "https"),
            address(8081, "http"),
            seed,
            directory / "cert.pem",
        )
    finally:
        for container in reversed(containers):
            docker("rm", "-f", container, check=False)
        docker("network", "rm", network, check=False)
        docker("image", "rm", image, check=False)
        assert not docker("ps", "-aq", "--filter", f"label={label}").stdout.strip()
        assert not docker("network", "ls", "-q", "--filter", f"label={label}").stdout.strip()
        assert not docker("image", "ls", "-q", "--filter", f"label={label}").stdout.strip()


def test_static_connector_container_authorization_and_shutdown(host: Host) -> None:
    import ssl

    for ca, key, password in [
        (None, "connector-grant:reviewer:tls-control", PASSWORD),
        ("/config/cert.pem", "connector-grant:other:acl-control", PASSWORD),
        ("/config/cert.pem", "connector-grant:reviewer:auth-control", "wrong-password"),
    ]:
        script = (
            "from curie_connector_proxy.server import _ValkeyGrantStore; "
            f"store = _ValkeyGrantStore('rediss://reviewer:{password}@valkey:6379/0', "
            f"ca_file={ca!r}, key_prefix=''); "
            f"assert not store.spend({key!r}, 60)"
        )
        host.docker("exec", host.container, "python3", "-c", script)
    expires = int(time.time()) + 300
    caller = caller_token.mint(host.seed, agent="acme-dev", exp=expires)
    wrong = caller_token.mint(host.seed, agent="other", exp=expires)
    args = {"value": 1}

    def grant(exp: int) -> str:
        return connector_grant.mint(
            host.seed,
            agent="acme-dev",
            connector="reviewer",
            tool="write",
            args=connector_grant.canonical_arguments(args),
            exp=exp,
            jti=uuid.uuid4().hex,
        )

    call = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": "write", "arguments": args},
    }
    valid = grant(expires)
    with httpx.Client(
        verify=ssl.create_default_context(cafile=str(host.ca)), trust_env=False
    ) as client:
        for presented, capability in [
            (wrong, valid),
            (caller, None),
            (caller, grant(expires - 600)),
        ]:
            headers = {"X-Curie-Caller": presented}
            if capability:
                headers["X-Curie-Connector-Grant"] = capability
            assert client.post(host.base + "/mcp", json=call, headers=headers).status_code == 403
            assert (
                host.docker("exec", host.container, "test", "!", "-f", "/tmp/executions").returncode
                == 0
            )
        headers = {"X-Curie-Caller": caller, "X-Curie-Connector-Grant": valid}
        response = client.post(host.base + "/mcp", json=call, headers=headers)
        assert response.status_code == 200 and response.json() == {"executions": 1}
        assert client.post(host.base + "/mcp", json=call, headers=headers).status_code == 403
        assert host.docker("exec", host.container, "cat", "/tmp/executions").stdout == b"1"
        upload = caller_token.mint_upload(
            host.seed, agent="acme-dev", connector="reviewer", exp=expires
        )
        assert client.post(
            host.uploads + "/v1/upload", content=b"file", headers={"X-Curie-Caller": upload}
        ).json() == {"uploaded": 4}
        assert (
            client.post(host.uploads + "/v1/upload", content=b"bad", headers=headers).status_code
            == 403
        )
        assert (
            client.post(
                host.uploads + "/mcp", json=call, headers={"X-Curie-Caller": upload}
            ).status_code
            == 403
        )
        assert (
            client.post(
                host.base + "/mcp", json=call, headers={"X-Curie-Caller": upload}
            ).status_code
            == 403
        )
        assert host.docker("exec", host.container, "cat", "/tmp/upload").stdout == b"file"
        assert host.docker("exec", host.container, "id", "-u").stdout.strip() == b"65532"
    host.docker("kill", "--signal", "TERM", host.container)
    assert host.docker("wait", host.container).stdout.strip() == b"0"
    receipt = host.docker("cp", f"{host.container}:/tmp/terminated", "-").stdout
    with tarfile.open(fileobj=io.BytesIO(receipt)) as tar:
        signal_receipt = tar.extractfile(tar.getmembers()[0])
        assert signal_receipt is not None and signal_receipt.read() == b"SIGTERM"
    print(
        "valid caller+grant executed once; refusals executed zero times; "
        "upload scope held; SIGTERM exited 0"
    )
