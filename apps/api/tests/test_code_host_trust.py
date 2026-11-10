"""The code host trust bundle and the credential the API issues with it (#3831).

ADR 0197: the operator mounts one PEM bundle for a self-managed code host. The
API's code host clients trust it beside the public roots, and every repository
credential names its path so the worker, the sandbox and the publication Job
trust the same file. Unset, nothing changes.
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import datetime as dt
import ssl
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import NameOID
from curie_api.code_host_trust import code_host_verify, verify_for_bundle
from curie_api.config import Settings
from curie_api.forges.types import CredentialHeader, CredentialScope
from curie_api.github_app import GitHubAuth, GitHubCredentials
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


def _subjects(context: ssl.SSLContext) -> list[str | None]:
    return [
        dict(part[0] for part in cert["subject"]).get("commonName")
        for cert in context.get_ca_certs()
    ]


def test_code_host_verify_hands_its_bundle_to_verify_for_bundle(tmp_path: Path) -> None:
    # One bundle-to-verify mapping serves both the Settings path and an explicit
    # GitHubAuth (#4397 R1 finding 5), so the two constructors cannot diverge.
    assert verify_for_bundle("") is True
    bundle = tmp_path / "ca.crt"
    bundle.write_bytes(_ca_pem("Curie Handoff CA"))

    direct = verify_for_bundle(str(bundle))
    through_settings = code_host_verify(Settings(code_host_ca_bundle=str(bundle)))

    assert isinstance(direct, ssl.SSLContext)
    assert isinstance(through_settings, ssl.SSLContext)
    assert "Curie Handoff CA" in _subjects(direct)
    assert "Curie Handoff CA" in _subjects(through_settings)


def test_github_auth_from_settings_carries_the_bundle() -> None:
    auth = GitHubAuth.from_settings(Settings(code_host_ca_bundle="/etc/curie/trust/ca.crt"))

    assert auth.ca_bundle == "/etc/curie/trust/ca.crt"


def test_github_credentials_verify_with_the_auth_bundle_not_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bundle = tmp_path / "ca.crt"
    bundle.write_bytes(_ca_pem("Curie Auth Bundle CA"))
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    settings = Settings(github_app_id="12345", github_app_private_key=pem, code_host_ca_bundle="")
    # The settings carry no bundle; only the auth does. The mint clients must
    # trust the auth's bundle, proving the handoff reads `auth.ca_bundle`.
    auth = dataclasses.replace(GitHubAuth.from_settings(settings), ca_bundle=str(bundle))
    verifies: list[Any] = []
    real_client = httpx.Client

    def github(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/installation"):
            return httpx.Response(200, json={"id": 7})
        return httpx.Response(
            201, json={"token": "fixture-token", "expires_at": "2999-01-01T00:00:00Z"}
        )

    def client(*_: Any, **kwargs: Any) -> httpx.Client:
        verifies.append(kwargs.get("verify"))
        return real_client(transport=httpx.MockTransport(github))

    monkeypatch.setattr("curie_api.github_app.httpx.Client", client)
    credentials = GitHubCredentials(auth=auth)

    credentials.fresh_installation_token(REPO)
    assert credentials.token_for(REPO) == "fixture-token"

    assert verifies
    for verify in verifies:
        assert isinstance(verify, ssl.SSLContext)
        assert "Curie Auth Bundle CA" in _subjects(verify)


def _issue(monkeypatch: pytest.MonkeyPatch, settings: Settings, scope: CredentialScope) -> Any:
    monkeypatch.setattr(
        "curie_api.forges.github.code_host.resolve_repository_credential",
        lambda repo, *, html_base, credentials: (f"{html_base}/{repo}.git", HEADER),
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
