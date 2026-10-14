"""Synthetic authenticated external endpoints; no broker/registry/issuer substitutes."""

from __future__ import annotations

import ipaddress
import ssl
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

from aiohttp import web
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


def certificates(root: Path):
    root.mkdir(mode=0o700, exist_ok=True)
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "synthetic-ca")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), False)
        .add_extension(
            x509.KeyUsage(True, False, False, False, False, True, True, False, False), True
        )
        .sign(ca_key, hashes.SHA256())
    )
    (root / "ca.pem").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    for role, usage in [
        ("server", ExtendedKeyUsageOID.SERVER_AUTH),
        ("worker", ExtendedKeyUsageOID.CLIENT_AUTH),
        ("unknown", ExtendedKeyUsageOID.CLIENT_AUTH),
    ]:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cert = (
            x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, role)]))
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(hours=1))
            .add_extension(
                x509.SubjectAlternativeName(
                    [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
                ),
                critical=False,
            )
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), False
            )
            .add_extension(
                x509.KeyUsage(True, False, True, False, False, False, False, False, False), True
            )
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
            .sign(ca_key, hashes.SHA256())
        )
        (root / (role + ".pem")).write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        (root / (role + ".key")).write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
        (root / (role + ".key")).chmod(0o600)
    return root


def context(root: Path, role: str = "worker"):
    ctx = ssl.create_default_context(cafile=str(root / "ca.pem"))
    ctx.load_cert_chain(str(root / (role + ".pem")), str(root / (role + ".key")))
    return ctx


def server_context(root: Path):
    ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH, cafile=str(root / "ca.pem"))
    ctx.load_cert_chain(str(root / "server.pem"), str(root / "server.key"))
    ctx.verify_mode = ssl.CERT_REQUIRED
    return ctx


@asynccontextmanager
async def endpoint(root, handler):
    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_context(root))
    try:
        await site.start()
        yield "https://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
    finally:
        await runner.cleanup()
