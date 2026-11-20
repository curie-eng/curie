"""The shared S3 client builder (#501): path-style addressing, offline.

The credential tests below (#1559) pin the key-free BYO object-store path: an
omitted credential must reach botocore's provider chain (IRSA / instance role /
ambient profile), not be signed as an explicit empty-string identity. Botocore's
web-identity provider returns DEFERRED credentials, so it resolves the provider
without ever calling STS. Everything here stays offline and needs no mocks.
"""

import os
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from aci_protocol.s3 import build_s3_client
from botocore.exceptions import PartialCredentialsError


@pytest.fixture
def web_identity_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Only an IRSA-style web identity is available to the provider chain.

    Every ``AWS_*``/``S3_*`` var is scrubbed first so an ambient developer
    profile or a CI runner's own credentials cannot decide the result; what is
    left is the exact env a pod running under IRSA sees. The token file's
    contents are never read here (the provider defers the STS exchange), so a
    placeholder is enough.
    """
    for name in list(os.environ):
        if name.startswith(("AWS_", "S3_")):
            monkeypatch.delenv(name, raising=False)
    token_file = tmp_path / "token"
    token_file.write_text("placeholder-web-identity-token")
    monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::000000000000:role/curie-bundles")
    monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(token_file))
    monkeypatch.setenv("AWS_REGION", "us-east-1")


def test_build_s3_client_is_path_style() -> None:
    # boto3.client() does no network I/O until a call is made, so this asserts the
    # construction contract offline: path-style addressing (load-bearing for RustFS)
    # and the endpoint the caller passed.
    client = build_s3_client(
        endpoint_url="http://rustfs.test:9000",
        access_key="ak",
        secret_key="sk",
        region="us-east-1",
    )
    assert client.meta.config.s3["addressing_style"] == "path"
    assert client.meta.endpoint_url == "http://rustfs.test:9000"
    assert client.meta.region_name == "us-east-1"


def test_empty_credentials_resolve_through_the_provider_chain(
    web_identity_env: None,
) -> None:
    # An empty string is STILL an explicit credential to botocore; only None means
    # "use the provider chain". So the builder must translate omitted-in-config to
    # None, and the proof is which provider actually resolved: assert on the
    # signer's resolved method rather than on the config string, because a config
    # default of "" alone leaves the key-free path just as inert.
    client = build_s3_client(
        endpoint_url="https://s3.example.com:443",
        access_key="",
        secret_key="",
        region="us-east-1",
    )
    credentials = client._request_signer._credentials
    assert credentials.method == "assume-role-with-web-identity"


def test_explicit_credentials_still_win_over_the_chain(web_identity_env: None) -> None:
    # The guard against "fixing" the chain by ignoring configured credentials: a
    # RustFS/MinIO deployment supplies a static key pair and it must be the one
    # that signs, even with an ambient web identity sitting right next to it.
    client = build_s3_client(
        endpoint_url="https://s3.example.com:443",
        access_key="ak",
        secret_key="sk",
        region="us-east-1",
    )
    credentials = client._request_signer._credentials
    assert credentials.method == "explicit"
    assert credentials.access_key == "ak"


def test_half_a_credential_is_refused_not_silently_downgraded(
    web_identity_env: None,
) -> None:
    # Half a credential is an operator mistake, and it must fail loudly at
    # construction rather than sign with a partial identity or quietly fall back
    # to the ambient web identity (which would mask a typo'd secret key as a
    # working deployment until the wrong role's permissions bit).
    with pytest.raises(PartialCredentialsError):
        build_s3_client(
            endpoint_url="https://s3.example.com:443",
            access_key="ak",
            secret_key="",
            region="us-east-1",
        )


@pytest.mark.parametrize(
    ("endpoint_url", "expected_prefix"),
    [
        ("https://s3.us-east-1.amazonaws.com", "https://s3.us-east-1.amazonaws.com/bkt/"),
        ("http://rustfs.test:9000", "http://rustfs.test:9000/bkt/"),
    ],
)
def test_presigned_get_url_is_sigv4_and_path_style(endpoint_url: str, expected_prefix: str) -> None:
    # SSE-KMS buckets reject SigV2 presigned URLs, and `meta.config.signature_version`
    # can report s3v4 while the presigner still emits SigV2, so assert the URL itself.
    client = build_s3_client(
        endpoint_url=endpoint_url,
        access_key="AKIAEXAMPLE",
        secret_key="example-secret",
        region="us-east-1",
    )
    url = client.generate_presigned_url(
        "get_object", Params={"Bucket": "bkt", "Key": "a/b.txt"}, ExpiresIn=60
    )
    assert url.startswith(expected_prefix + "a/b.txt?")
    query = parse_qs(urlsplit(url).query)
    assert query["X-Amz-Algorithm"] == ["AWS4-HMAC-SHA256"]
    assert "X-Amz-Credential" in query
    assert "X-Amz-Signature" in query
    assert query["X-Amz-Expires"] == ["60"]
    assert "AWSAccessKeyId" not in query
    assert "Signature" not in query
