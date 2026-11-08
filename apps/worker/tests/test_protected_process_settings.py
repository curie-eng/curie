"""Explicit synthetic process inputs; no installation or execution proof."""

import importlib
import importlib.util
import sys

import pytest

from .protected_configuration_vectors import canonical


def product():
    name = "curie_worker.protected_entry"
    assert importlib.util.find_spec(name) is not None, "protected process entry absent"
    return importlib.import_module(name)


def value():
    return {
        "schema_version": 2,
        "database_url": "postgresql+asyncpg://postgres@127.0.0.1:35432/postgres",
        "db_schema": "acme",
        "api_base_url": "https://api.example.com",
        "internal_worker_token": "synthetic-worker-token",
        "adapter_credentials": {"acme-adapter": "synthetic-adapter-token"},
        "channel_gateway": {
            "url": "https://gateway.example.com",
            "token": "synthetic-gateway-token",
            "allow_insecure_http": False,
            "identities": ["adapter/example"],
        },
        "launch_tls": {
            "origin": "https://mediator.example.com",
            "ca_file": "/private/acme/ca.pem",
            "server_name": "mediator.example.com",
            "server_spki_sha256": "a" * 64,
            "client_certificate": "/private/acme/client.pem",
            "client_key": "/private/acme/client.key",
            "worker_principal_id": "worker/example",
        },
    }


def test_explicit_process_inputs_are_closed_and_redacted(tmp_path, monkeypatch):
    monkeypatch.setenv("EXAMPLE_PRIVATE_CREDENTIAL_SOURCE", "forbidden-model-secret")
    (tmp_path / "process.json").write_bytes(canonical(value()))
    settings = product().load_process_settings(str(tmp_path))
    assert settings.database_url.endswith(":35432/postgres")
    assert settings.launch_tls.worker_principal_id == "worker/example"
    assert settings.gateway_identities == ("adapter/example",)
    assert settings.gateway_url == "https://gateway.example.com"
    assert "synthetic" not in repr(settings)
    assert "forbidden-model-secret" not in repr(settings)


@pytest.mark.parametrize("mutation", ["extra", "missing", "empty-token", "dialect", "schema"])
def test_process_inputs_refuse_missing_or_unsupported_authority(tmp_path, mutation):
    settings = value()
    if mutation == "extra":
        settings["model_credentials"] = "forbidden"
    elif mutation == "missing":
        del settings["launch_tls"]
    elif mutation == "empty-token":
        settings["internal_worker_token"] = ""
    elif mutation == "dialect":
        settings["database_url"] = "sqlite:///ordinary.db"
    else:
        settings["db_schema"] = "invalid;schema"
    (tmp_path / "process.json").write_bytes(canonical(settings))
    with pytest.raises(product().WorkerBootstrapInvalid) as error:
        product().load_process_settings(str(tmp_path))
    assert error.value.__context__ is None


def test_main_startup_failure_detaches_private_exception_graph(tmp_path, monkeypatch):
    missing = tmp_path / "synthetic-private-runtime"
    monkeypatch.setattr(sys, "argv", ["protected-worker", "--runtime-dir", str(missing)])
    with pytest.raises(SystemExit) as error:
        product().main()
    assert error.value.__context__ is None
    assert error.value.__cause__ is None
    assert str(missing) not in str(error.value)


@pytest.mark.parametrize("mutation", ["environment-token", "file-token", "http", "shared-token"])
def test_protected_process_refuses_provider_credentials_and_implicit_http(
    tmp_path,
    monkeypatch,
    mutation,
):
    settings = value()
    if mutation == "environment-token":
        monkeypatch.setenv("SLACK_BOT_TOKEN", "private-sentinel")
    elif mutation == "file-token":
        settings["channel_gateway"]["token"] = "xoxb-private-sentinel"
    elif mutation == "http":
        settings["channel_gateway"]["url"] = "http://gateway.example.com"
    else:
        settings["slack"] = {"token": "private-sentinel"}
    (tmp_path / "process.json").write_bytes(canonical(settings))
    with pytest.raises(product().WorkerBootstrapInvalid) as error:
        product().load_process_settings(str(tmp_path))
    assert "private-sentinel" not in str(error.value)
    assert error.value.__context__ is None
