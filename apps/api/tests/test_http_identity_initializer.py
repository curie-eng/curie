"""API operator initializer for HTTP identity, from protected file to served turn.

@spec HTTP-INVOCATION-EXECUTION-1/15/20.
The provider is the owned synthetic TLS directory; no real directory, key custody or
cluster mount is qualified here.
"""

from __future__ import annotations

import base64
import copy
import importlib
import json
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.identity.http_invocation import HttpInvocationRuntime
from curie_api.identity.http_transport import HttpIdentityClient
from fastapi.testclient import TestClient
from principal_adoption_support import selection

from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

FILE = "api-runtime.json"
TENANT = "55555555-5555-4555-8555-555555555555"
RECEIVER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
IDENTITY = "11111111-1111-4111-8111-111111111111"
INSTALLATION = "22222222-2222-4222-8222-222222222222"
BINDING = "33333333-3333-4333-8333-333333333333"
NAMESPACE = "44444444-4444-4444-8444-444444444444"


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-20: fail during execution, never collection."""
    name = "curie_api.identity.http_initializer"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "API HTTP identity initializer is not implemented"
        raise
    for symbol in (
        "HttpIdentityInitializationError",
        "load_http_identity_runtime",
        "configured_http_identity_runtime",
    ):
        assert hasattr(module, symbol), f"API HTTP identity initializer missing {symbol}"
    return module


def key(material: bytes) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-20: unpadded canonical base64url."""
    return base64.urlsafe_b64encode(material).rstrip(b"=").decode("ascii")


def document(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20: one complete synthetic operator document."""
    value: dict[str, Any] = {
        "schema_version": 1,
        "providers": [{
            "configuration": {
                "schema_version": 1, "tenant_id": TENANT, "version": 1,
                "selection": selection().model_dump(mode="json"),
            },
            "bearer": "SyntheticInitializerBearer",
        }],
        "receivers": [{
            "registration": {
                "schema_version": 1, "receiver_id": RECEIVER, "generation": 1,
                "tenant_id": TENANT, "channel_identity_id": IDENTITY,
                "channel_identity_name": "synthetic-receiver",
                "provider_installation_id": INSTALLATION,
                "namespace": {
                    "id": NAMESPACE, "authority": "", "kind": "slack_workspace",
                    "key": "T0EXAMPLE1",
                },
                "installation_account": "T0EXAMPLE1", "bot_user_id": "U0EXAMPLEBOT",
                "binding_ids": [BINDING], "human_native_ids": ["U0EXAMPLE1"],
            },
            "key": key(b"r" * 32),
        }],
        "context_keys": [{"key_id": "synthetic-context", "generation": 1, "key": key(b"c" * 32)}],
        "signing_key": {"key_id": "synthetic-context", "generation": 1},
    }
    value.update(changes)
    return value


def write(directory: Path, value: Any, *, mode: int = 0o440) -> Path:
    """@spec HTTP-INVOCATION-EXECUTION-20: the mounted operator file."""
    path = directory / FILE
    if path.exists():
        path.chmod(0o600)
    path.write_text(value if isinstance(value, str) else json.dumps(value))
    path.chmod(mode)
    return path


def refused(module: Any, directory: Path) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-20: fixed stage, no chaining or supplied values."""
    with pytest.raises(module.HttpIdentityInitializationError) as caught:
        module.load_http_identity_runtime(str(directory))
    error = caught.value
    assert error.__cause__ is None and error.__suppress_context__
    rendered = str(error) + repr(error)
    for secret in ("SyntheticInitializerBearer", key(b"r" * 32), key(b"c" * 32), str(directory)):
        assert secret not in rendered
    assert error.stage in {"unconfigured_directory", "unsafe_file", "invalid_document",
                           "invalid_runtime"}
    assert str(error) == "http identity initialization refused: " + error.stage
    return str(error.stage)


def test_complete_operator_document_builds_the_runtime(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/20: providers, receivers and core keys together."""
    module = api()
    write(tmp_path, document())
    runtime = module.load_http_identity_runtime(str(tmp_path))
    assert type(runtime) is HttpInvocationRuntime
    assert repr(runtime) == "HttpInvocationRuntime()"


def mutate(path: str, value: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-20: replace or delete one nested member."""
    result = document()
    *parents, leaf = path.split(".")
    node: Any = result
    for part in parents:
        node = node[int(part)] if part.isdigit() else node[part]
    target: Any = int(leaf) if leaf.isdigit() else leaf
    if value is KeyError:
        del node[target]
    else:
        node[target] = value
    return result


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ("schema_version", True),
        ("schema_version", 2),
        ("schema_version", KeyError),
        ("providers", []),
        ("receivers", []),
        ("context_keys", []),
        ("signing_key", KeyError),
        ("unexpected", "member"),
        ("providers.0.bearer", "not a bearer"),
        ("providers.0.unexpected", 1),
        ("providers.0.configuration.selection", {"mode": "disabled"}),
        ("providers.0.configuration.version", "1"),
        ("receivers.0.key", key(b"r" * 31)),
        ("receivers.0.key", key(b"r" * 32) + "="),
        ("receivers.0.registration.unexpected", 1),
        ("receivers.0.registration.namespace.authority", "grid"),
        ("receivers.0.registration.binding_ids", []),
        ("context_keys.0.generation", True),
        ("context_keys.0.key_id", "not/a/key"),
        ("signing_key.unexpected", 1),
    ],
)
def test_partial_or_invalid_documents_refuse_as_documents(
    tmp_path: Path, path: str, value: Any,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: strict fields, nothing defaulted or ignored."""
    module = api()
    write(tmp_path, mutate(path, value))
    assert refused(module, tmp_path) == "invalid_document"


def test_runtime_conflicts_refuse_as_runtime(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/20: absent signing key, duplicate pairs or material."""
    module = api()
    cases = [
        document(signing_key={"key_id": "synthetic-context", "generation": 2}),
        document(context_keys=[
            {"key_id": "synthetic-context", "generation": 1, "key": key(b"c" * 32)},
            {"key_id": "synthetic-context", "generation": 1, "key": key(b"d" * 32)},
        ]),
        document(context_keys=[
            {"key_id": "synthetic-context", "generation": 1, "key": key(b"r" * 32)},
        ]),
        document(receivers=[document()["receivers"][0]] * 2),
    ]
    duplicate_provider = document()
    duplicate_provider["providers"] = duplicate_provider["providers"] * 2
    cases.append(duplicate_provider)
    for case in cases:
        write(tmp_path, case)
        assert refused(module, tmp_path) == "invalid_runtime"


def test_file_custody_and_json_strictness_refuse_before_registrations(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: unsafe files and noncanonical JSON."""
    module = api()
    assert refused(module, tmp_path / "absent") == "unconfigured_directory"
    assert refused(module, tmp_path) == "unsafe_file"
    write(tmp_path, document(), mode=0o444)
    assert refused(module, tmp_path) == "unsafe_file"
    raw = json.dumps(document())
    write(tmp_path, raw[:-1] + ',"schema_version":1}')
    assert refused(module, tmp_path) == "invalid_document"
    write(tmp_path, raw.replace('"generation": 1', '"generation": NaN', 1))
    assert refused(module, tmp_path) == "invalid_document"


def test_settings_select_the_initializer_only_when_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: absent setting is the legacy install."""
    module = api()
    monkeypatch.delenv("CURIE_HTTP_IDENTITY_DIR", raising=False)
    get_settings.cache_clear()
    try:
        assert get_settings().http_identity_dir is None
        assert module.configured_http_identity_runtime(get_settings()) is None
        monkeypatch.setenv("CURIE_HTTP_IDENTITY_DIR", "")
        get_settings.cache_clear()
        with pytest.raises(module.HttpIdentityInitializationError) as caught:
            module.configured_http_identity_runtime(get_settings())
        assert caught.value.stage == "unconfigured_directory"
        write(tmp_path, document())
        monkeypatch.setenv("CURIE_HTTP_IDENTITY_DIR", str(tmp_path))
        get_settings.cache_clear()
        runtime = module.configured_http_identity_runtime(get_settings())
        assert type(runtime) is HttpInvocationRuntime
    finally:
        get_settings.cache_clear()


def test_configured_app_passes_only_an_initialized_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/20: unconfigured keeps create_app() exactly."""
    api()
    from curie_api import main

    calls: list[dict[str, Any]] = []
    monkeypatch.setattr(main, "create_app", lambda **kwargs: calls.append(kwargs) or "app")
    monkeypatch.delenv("CURIE_HTTP_IDENTITY_DIR", raising=False)
    get_settings.cache_clear()
    try:
        assert main.create_configured_app() == "app"
        assert calls == [{}]
        write(tmp_path, document())
        monkeypatch.setenv("CURIE_HTTP_IDENTITY_DIR", str(tmp_path))
        get_settings.cache_clear()
        assert main.create_configured_app() == "app"
        assert list(calls[1]) == ["http_identity_runtime"]
        assert type(calls[1]["http_identity_runtime"]) is HttpInvocationRuntime
        write(tmp_path, document(providers=[]))
        with pytest.raises(api().HttpIdentityInitializationError):
            main.create_configured_app()
        assert len(calls) == 2
    finally:
        get_settings.cache_clear()


def test_initialized_api_serves_a_selected_turn_through_the_provider_client(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,
    tmp_path: Path,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/20: file-initialized runtime reaches the provider."""
    module = api()
    from curie_api.main import create_app

    scope = membership._scope(synthetic_provider.url)
    write(tmp_path, {
        "schema_version": 1,
        "providers": [{
            "configuration": scope["configuration"].model_dump(mode="json"),
            "bearer": membership.BEARER,
        }],
        "receivers": [{
            "registration": {
                "schema_version": 1, "receiver_id": scope["receiver_id"], "generation": 1,
                "tenant_id": scope["tenant"], "channel_identity_id": scope["identity"],
                "channel_identity_name": scope["adapter"],
                "provider_installation_id": scope["installation"],
                "namespace": scope["namespace"].model_dump(mode="json"),
                "installation_account": membership.ACCOUNT, "bot_user_id": "U0EXAMPLEBOT",
                "binding_ids": [scope["binding"]], "human_native_ids": [membership.SENDER],
            },
            "key": key(scope["receiver_key"]),
        }],
        "context_keys": [
            {"key_id": "synthetic-context", "generation": 1, "key": key(scope["context_key"])},
        ],
        "signing_key": {"key_id": "synthetic-context", "generation": 1},
    })
    runtime = module.load_http_identity_runtime(str(tmp_path))
    identity_client = HttpIdentityClient(tls_context=synthetic_provider.tls)
    try:
        app = create_app(http_identity_runtime=runtime, http_identity_client=identity_client)
        with TestClient(app) as client:
            assert app.state.http_identity_runtime is runtime
            assert app.state.http_identity_client is identity_client
            token = membership._credential(client, scope)
            body, proof = membership._submission(scope)
            accepted = membership._post(client, token, body, proof)
            assert accepted.status_code == 200, accepted.text
            assert len(synthetic_provider.requests) == 1
            observed = copy.deepcopy(synthetic_provider.requests[0])
            assert observed["scope"]["tenant_id"] == scope["tenant"]
            assert observed["scope"]["sender"]["native_id"] == membership.SENDER
            entries = valkey.xrange(runs_stream)
            assert len(entries) == 1 and accepted.json()["stream_id"] == entries[0][0]
            assert "curie_identity_context_v1" in entries[0][1]
            uuid.UUID(observed["request_id"])
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


CA_FILE = "provider-ca.pem"


def certificate_pem(*, ca: bool = True, name: str = "Synthetic initializer CA") -> str:
    """@spec HTTP-INVOCATION-EXECUTION-20: an independently generated synthetic certificate."""
    from datetime import UTC, datetime, timedelta

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID

    now = datetime.now(UTC)
    signer = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    cert = (
        x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
        .public_key(signer.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1)).not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .sign(signer, hashes.SHA256())
    )
    return cert.public_bytes(serialization.Encoding.PEM).decode("ascii")


def write_ca(directory: Path, value: str | bytes, *, mode: int = 0o440) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: the optional trust anchor beside the runtime file."""
    path = directory / CA_FILE
    if path.exists():
        path.chmod(0o600)
    path.write_bytes(value.encode() if isinstance(value, str) else value)
    path.chmod(mode)


def tls_api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-20: fail during execution, never collection."""
    module = api()
    assert hasattr(module, "load_provider_tls_context"), "provider trust anchor is not loaded"
    return module


def test_absent_trust_anchor_keeps_default_trust(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: no provider-ca.pem means the default client."""
    module = tls_api()
    write(tmp_path, document())
    assert module.load_provider_tls_context(str(tmp_path)) is None


def test_trust_anchor_is_the_only_store_with_hostname_verification(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: only the supplied CAs, verified hostnames."""
    import ssl

    module = tls_api()
    write_ca(tmp_path, certificate_pem() + certificate_pem(name="Synthetic second CA"))
    context = module.load_provider_tls_context(str(tmp_path))
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED and context.check_hostname is True
    assert context.minimum_version >= ssl.TLSVersion.TLSv1_2
    assert context.cert_store_stats() == {"x509": 2, "crl": 0, "x509_ca": 2}


@pytest.mark.parametrize(
    "bundle",
    [
        "",
        "\n",
        "not a certificate\n",
        "TRAILING",
        "LEAF",
        "KEY",
        "BADBASE64",
        "TOO_MANY",
        "NON_ASCII",
    ],
)
def test_malformed_trust_anchor_stops_startup(tmp_path: Path, bundle: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: empty, malformed or non-CA bundles refuse."""
    module = tls_api()
    good = certificate_pem()
    value: str | bytes = {
        "TRAILING": good + "trailing text\n",
        "LEAF": certificate_pem(ca=False),
        "KEY": "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n",
        "BADBASE64": good.replace(good.splitlines()[1], "!" + good.splitlines()[1][1:], 1),
        "TOO_MANY": good * 17,
        "NON_ASCII": good.encode() + "é".encode(),
    }.get(bundle, bundle)
    write_ca(tmp_path, value)
    with pytest.raises(module.HttpIdentityInitializationError) as caught:
        module.load_provider_tls_context(str(tmp_path))
    assert caught.value.stage == "invalid_document" and caught.value.__cause__ is None
    assert good.splitlines()[1] not in str(caught.value) + repr(caught.value)


def test_unsafe_trust_anchor_file_refuses(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: the same custody checks as the runtime file."""
    module = tls_api()
    write_ca(tmp_path, certificate_pem(), mode=0o644)
    with pytest.raises(module.HttpIdentityInitializationError) as caught:
        module.load_provider_tls_context(str(tmp_path))
    assert caught.value.stage == "unsafe_file"
    (tmp_path / CA_FILE).chmod(0o600)
    (tmp_path / CA_FILE).unlink()
    (tmp_path / CA_FILE).mkdir()
    with pytest.raises(module.HttpIdentityInitializationError) as caught:
        module.load_provider_tls_context(str(tmp_path))
    assert caught.value.stage == "unsafe_file"


def test_configured_app_supplies_a_client_only_for_a_trust_anchor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/20: the anchor reaches create_app as its client."""
    module = tls_api()
    from curie_api import main

    calls: list[dict[str, Any]] = []

    def record(**kwargs: Any) -> str:
        """@spec HTTP-INVOCATION-EXECUTION-20."""
        calls.append(kwargs)
        return "app"

    monkeypatch.setattr(main, "create_app", record)
    write(tmp_path, document())
    write_ca(tmp_path, certificate_pem())
    monkeypatch.setenv("CURIE_HTTP_IDENTITY_DIR", str(tmp_path))
    get_settings.cache_clear()
    try:
        main.create_configured_app()
        assert sorted(calls[0]) == ["http_identity_client", "http_identity_runtime"]
        assert type(calls[0]["http_identity_client"]) is HttpIdentityClient
        assert type(calls[0]["http_identity_runtime"]) is HttpInvocationRuntime
        write_ca(tmp_path, "")
        with pytest.raises(module.HttpIdentityInitializationError):
            main.create_configured_app()
        assert len(calls) == 1
    finally:
        get_settings.cache_clear()


@pytest.mark.parametrize("anchor", ["own", "absent", "other"])
def test_private_ca_provider_is_reached_only_through_its_configured_anchor(
    clean_db: None,
    valkey: redis.Redis,
    runs_stream: str,
    synthetic_provider: SimpleNamespace,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    anchor: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-15/20: the served app trusts only the mounted CA."""
    tls_api()
    from curie_api.main import create_configured_app

    scope = membership._scope(synthetic_provider.url)
    operator = tmp_path / "operator"
    operator.mkdir()
    write(operator, {
        "schema_version": 1,
        "providers": [{
            "configuration": scope["configuration"].model_dump(mode="json"),
            "bearer": membership.BEARER,
        }],
        "receivers": [{
            "registration": {
                "schema_version": 1, "receiver_id": scope["receiver_id"], "generation": 1,
                "tenant_id": scope["tenant"], "channel_identity_id": scope["identity"],
                "channel_identity_name": scope["adapter"],
                "provider_installation_id": scope["installation"],
                "namespace": scope["namespace"].model_dump(mode="json"),
                "installation_account": membership.ACCOUNT, "bot_user_id": "U0EXAMPLEBOT",
                "binding_ids": [scope["binding"]], "human_native_ids": [membership.SENDER],
            },
            "key": key(scope["receiver_key"]),
        }],
        "context_keys": [
            {"key_id": "synthetic-context", "generation": 1, "key": key(scope["context_key"])},
        ],
        "signing_key": {"key_id": "synthetic-context", "generation": 1},
    })
    if anchor == "own":
        # The owned synthetic provider's private CA, written by its TLS rig.
        write_ca(operator, (tmp_path / "ca.pem").read_text())
    elif anchor == "other":
        write_ca(operator, certificate_pem(name="Synthetic unrelated CA"))
    monkeypatch.setenv("CURIE_HTTP_IDENTITY_DIR", str(operator))
    get_settings.cache_clear()
    try:
        app = create_configured_app()
        with TestClient(app) as client:
            assert type(app.state.http_identity_runtime) is HttpInvocationRuntime
            token = membership._credential(client, scope)
            body, proof = membership._submission(scope)
            response = membership._post(client, token, body, proof)
            entries = valkey.xrange(runs_stream)
            if anchor == "own":
                assert response.status_code == 200, response.text
                assert len(synthetic_provider.requests) == 1
                assert len(entries) == 1 and "curie_identity_context_v1" in entries[0][1]
            else:
                assert response.status_code == 503, response.text
                assert response.json() == {"detail": "identity_authority_unavailable"}
                assert synthetic_provider.requests == [] and entries == []
    finally:
        get_settings.cache_clear()
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)
