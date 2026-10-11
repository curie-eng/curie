"""The code host trust bundle and the credential the API issues with it (#3831).

ADR 0197: the operator mounts one PEM bundle for a self-managed code host. The
API's code host clients trust it beside the public roots, and every repository
credential names its path so the worker, the sandbox and the publication Job
trust the same file. Unset, nothing changes.
"""

from __future__ import annotations

import asyncio
import base64
import datetime as dt
import ssl
from pathlib import Path
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from curie_api.code_host_trust import code_host_verify
from curie_api.config import Settings
from curie_api.forges.types import CredentialHeader, CredentialScope
from curie_api.repository_access import issue_repository_credential

REPO = "acme-corp/acme-bot"
HEADER = "Basic " + base64.b64encode(b"x-access-token:fixture-token").decode()


def _ca_pem(common_name: str) -> bytes:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    now = dt.datetime.now(dt.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


def test_no_bundle_keeps_the_default_verification() -> None:
    assert code_host_verify(Settings(code_host_ca_bundle="")) is True


def test_a_bundle_is_trusted_beside_the_public_roots(tmp_path: Path) -> None:
    bundle = tmp_path / "ca.crt"
    bundle.write_bytes(_ca_pem("Curie Test Code Host CA"))

    context = code_host_verify(Settings(code_host_ca_bundle=str(bundle)))

    assert isinstance(context, ssl.SSLContext)
    subjects = [
        dict(part[0] for part in cert["subject"]).get("commonName")
        for cert in context.get_ca_certs()
    ]
    assert "Curie Test Code Host CA" in subjects
    # The public roots stay: a bundle holding only the private CA must not cut
    # the API off from every other host it reaches through the same client.
    assert len(subjects) > 1


def test_a_missing_bundle_fails_loudly_rather_than_trusting_less(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        code_host_verify(Settings(code_host_ca_bundle=str(tmp_path / "absent.crt")))


def _issue(monkeypatch: pytest.MonkeyPatch, settings: Settings, scope: CredentialScope) -> Any:
    monkeypatch.setattr(
        "curie_api.forges.github.code_host.resolve_repository_credential",
        lambda repo, _settings: (f"https://github.com/{repo}.git", HEADER),
    )
    return asyncio.run(
        issue_repository_credential(
            settings,
            None,
            repo_full_name=REPO,
            project_id=None,
            scope=scope,  # type: ignore[arg-type]
        )
    )


@pytest.mark.parametrize("scope", list(CredentialScope))
def test_the_github_credential_keeps_today_header_and_names_its_origin(
    monkeypatch: pytest.MonkeyPatch, scope: CredentialScope
) -> None:
    issued = _issue(monkeypatch, Settings(code_host_ca_bundle=""), scope)

    assert issued.authorization_header == HEADER
    assert issued.header_form is CredentialHeader.AUTHORIZATION_BASIC
    assert issued.origin == "https://github.com"
    assert issued.clone_url == f"https://github.com/{REPO}.git"
    assert issued.ca_bundle_ref is None


def test_the_credential_names_the_mounted_bundle(monkeypatch: pytest.MonkeyPatch) -> None:
    issued = _issue(
        monkeypatch,
        Settings(code_host_ca_bundle="/etc/curie/code-host-trust/ca.crt"),
        CredentialScope.PUSH,
    )

    assert issued.ca_bundle_ref == "/etc/curie/code-host-trust/ca.crt"
