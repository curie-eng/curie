"""Synthetic private-mount bootstrap inputs, never runtime qualification."""

import importlib
import importlib.util
import os

import pytest

from .protected_configuration_vectors import canonical, execution_profile, manifest_for


def product():
    name = "curie_worker.protected_worker_bootstrap"
    assert importlib.util.find_spec(name) is not None, "protected worker bootstrap absent"
    return importlib.import_module(name)


def staged(root):
    profile = execution_profile()
    manifest = manifest_for(profile)
    root.mkdir()
    (root / "manifest.json").write_bytes(manifest.canonical_bytes)
    (root / "execution_config.json").write_bytes(canonical(profile))
    worker = {
        "schema_version": 1,
        "credential_ref": {"id": "credential/example-worker", "generation": "1"},
        "worker": {"username": "acme-worker", "password": "synthetic-only-password"},
    }
    (root / "worker.json").write_bytes(canonical(worker))
    return manifest, worker


def test_loader_binds_selected_worker_and_profile_without_reading_other_roles(tmp_path):
    root = tmp_path / "runtime"
    manifest, _ = staged(root)
    os.mkfifo(root / "source_writer.json")
    os.mkfifo(root / "enqueue.json")
    loaded = product().load_worker_bootstrap(str(root))
    assert loaded.manifest.canonical_bytes == manifest.canonical_bytes
    assert loaded.execution.digest == manifest.as_dict()["execution_config_digest"]
    assert loaded.credential.username == "acme-worker"
    assert "synthetic-only-password" not in repr(loaded)


@pytest.mark.parametrize("mutation", ["stale", "default", "extra", "duplicate", "profile"])
def test_loader_refuses_stale_role_and_substituted_profile(tmp_path, mutation):
    root = tmp_path / "runtime"
    _, worker = staged(root)
    if mutation == "stale":
        worker["credential_ref"]["generation"] = "2"
    elif mutation == "default":
        worker["worker"]["username"] = "default"
    elif mutation == "extra":
        worker["source_writer"] = {"username": "forbidden", "password": "forbidden"}
    elif mutation == "duplicate":
        (root / "worker.json").write_text('{"schema_version":1,' + canonical(worker).decode()[1:])
    else:
        profile = execution_profile()
        profile["runner"]["model"] = "substituted-model"
        (root / "execution_config.json").write_bytes(canonical(profile))
    if mutation not in {"duplicate", "profile"}:
        (root / "worker.json").write_bytes(canonical(worker))
    with pytest.raises(product().WorkerBootstrapInvalid) as error:
        product().load_worker_bootstrap(str(root))
    assert error.value.__context__ is None
    assert "synthetic-only-password" not in str(error.value)


@pytest.mark.parametrize("kind", ["missing", "fifo", "symlink", "oversize"])
def test_loader_refuses_unsafe_or_unbounded_files(tmp_path, kind):
    root = tmp_path / "runtime"
    staged(root)
    path = root / "worker.json"
    raw = path.read_bytes()
    path.unlink()
    if kind == "fifo":
        os.mkfifo(path)
    elif kind == "symlink":
        target = tmp_path / "other-worker.json"
        target.write_bytes(raw)
        path.symlink_to(target)
    elif kind == "oversize":
        path.write_bytes(b" " * 65537)
    with pytest.raises(product().WorkerBootstrapInvalid):
        product().load_worker_bootstrap(str(root))


def test_loader_refuses_symlink_directory(tmp_path):
    root = tmp_path / "runtime"
    staged(root)
    alias = tmp_path / "alias"
    alias.symlink_to(root, target_is_directory=True)
    with pytest.raises(product().WorkerBootstrapInvalid):
        product().load_worker_bootstrap(str(alias))


def test_protected_configuration_ignores_ordinary_environment(tmp_path, monkeypatch):
    root = tmp_path / "runtime"
    staged(root)
    bootstrap = product().load_worker_bootstrap(str(root))
    module = importlib.import_module("curie_worker.protected_run")
    assert hasattr(module, "configuration_for_bootstrap"), "isolated composition config absent"
    for name, value in {
        "KEY_PREFIX": "ordinary-worker",
        "CURIE_RUNS_STREAM": "ordinary-stream",
        "VALKEY_HOST": "foreign.example.com",
        "VALKEY_PASSWORD": "ordinary-secret",
        "EXAMPLE_PRIVATE_CREDENTIAL_SOURCE": "model-secret-forbidden",
        "CURIE_FAKE_MODEL": "1",
        "CURIE_REMEDIATION_ENABLED": "1",
        "DATABASE_URL": "postgresql+asyncpg://forbidden.example.com/forbidden",
        "CURIE_INTERNAL_WORKER_TOKEN": "ordinary-token",
        "CURIE_API_URL": "https://foreign.example.com",
    }.items():
        monkeypatch.setenv(name, value)
    config = module.configuration_for_bootstrap(
        bootstrap,
        database_url="postgresql+asyncpg://postgres@127.0.0.1:35432/postgres",
        db_schema="acme",
        api_base_url="https://api.example.com",
        internal_worker_token="synthetic-protected-token",
        adapter_credentials={"acme-adapter": "synthetic-adapter-secret"},
    )
    assert config.stream == "curie:runs"
    assert config.key_prefix.startswith("protected:lane:worker:")
    assert config.consumer_group.startswith("protected-")
    assert config.dead_letter_stream_name() == config.key_prefix + ":dead"
    assert config.credentials == ""
    assert config.valkey_password == ""
    assert config.fake_model is False
    assert config.remediation_enabled is False
    assert config.max_attempts == 1
    assert 0 < config.read_block_ms <= 1000
    assert config.api_base_url == "https://api.example.com"
    assert config.database_url == "postgresql+asyncpg://postgres@127.0.0.1:35432/postgres"
    assert config.internal_worker_token == "synthetic-protected-token"
    assert config.adapter_credentials == {"acme-adapter": "synthetic-adapter-secret"}
