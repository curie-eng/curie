"""The worker reads the GitHub stub CA once and hands it to the publication Job."""

from __future__ import annotations

import datetime
from pathlib import Path
from typing import Any

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from curie_worker import run as run_module
from curie_worker.config import WorkerConfig
from curie_worker.reply_sink import ReplySinkRouter

SETTING = "CURIE_PUBLICATION_GITHUB_CA_FILE"
TRUNCATED_PEM = (
    "-----BEGIN CERTIFICATE-----\n"
    "MIIBszCCAVmgAwIBAgIUcurieTestStubCaPlaceholderAAAAAAAAAAAwCgYIKoZI\n"
    "-----END CERTIFICATE-----\n"
)


def _genuine_ca_pem() -> str:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "curie-github-stub-test-ca")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode("ascii")


GITHUB_CA_PEM = _genuine_ca_pem()


class _Replies:
    async def post(self, *_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("no reply is posted while building the loop")


def _build(monkeypatch: pytest.MonkeyPatch, ca_file: str) -> Any:
    monkeypatch.delenv(SETTING, raising=False)
    monkeypatch.setattr(run_module, "KubernetesPublicationCluster", lambda _namespace: object())
    monkeypatch.setattr(run_module, "PostgresPublicationStore", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(run_module, "GitHubPublicationLookup", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        run_module, "PublicationCredentialClient", lambda *_args, **_kwargs: object()
    )
    replies = _Replies()
    router = ReplySinkRouter(adapters={"slack": replies}, default=replies)
    dependency: Any = object()
    return run_module._build_publication_loop(  # noqa: SLF001
        WorkerConfig(
            publication_enabled=True,
            publication_github_ca_file=ca_file,
            api_base_url="https://api.example.com",
            internal_worker_token="fixture-publication-worker-token",
        ),
        {"CURIE_SANDBOX_SUBSTRATE": "kubernetes"},
        dependency,
        router,
        dependency,
        dependency,
    )


def test_missing_github_ca_file_refuses_to_build_the_loop(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(Exception, match=SETTING):
        _build(monkeypatch, str(tmp_path / "absent-ca.pem"))


def test_github_ca_file_without_a_pem_certificate_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text("this is not a certificate\n", encoding="utf-8")

    with pytest.raises(Exception, match=SETTING):
        _build(monkeypatch, str(ca_file))


def test_github_ca_file_with_marker_only_garbage_is_refused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text(TRUNCATED_PEM, encoding="utf-8")

    with pytest.raises(Exception, match=SETTING):
        _build(monkeypatch, str(ca_file))


def test_configured_github_ca_reaches_the_publication_job_settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    ca_file = tmp_path / "ca.pem"
    ca_file.write_text(GITHUB_CA_PEM, encoding="utf-8")

    loop = _build(monkeypatch, str(ca_file))

    assert loop is not None
    job_settings = loop._reconciler._job_settings  # noqa: SLF001
    assert job_settings.github_ca_pem == GITHUB_CA_PEM


def test_empty_github_ca_setting_leaves_the_job_on_default_trust(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = _build(monkeypatch, "")

    assert loop is not None
    assert loop._reconciler._job_settings.github_ca_pem is None  # noqa: SLF001
