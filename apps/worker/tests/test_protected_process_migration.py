"""The operator migration rewrites a file that the runtime then accepts."""

import json
import os
import stat
import subprocess
from pathlib import Path

import pytest

from .test_protected_process_settings import product, value

ROOT = Path(__file__).resolve().parents[3]
CLI = Path(os.environ.get("CURIE_BIN", ROOT / "cli/target/debug/curie"))


def old_settings():
    settings = value()
    settings["schema_version"] = 1
    gateway = settings.pop("channel_gateway")
    settings["slack"] = {
        "token": "xoxb-private-migration-sentinel",
        "base_url": "",
        "trusted_origins": [],
        "identity_tokens": {"adapter/example": "xoxb-private-identity-sentinel"},
    }
    gateway["identities"] = ["default", "adapter/example"]
    return settings, gateway


def run(tmp_path, settings, gateway):
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    process = runtime / "process.json"
    process.write_text(json.dumps(settings))
    config = tmp_path / "gateway.json"
    config.write_text(json.dumps(gateway))
    result = subprocess.run(
        [
            str(CLI),
            "migrate-protected-process",
            "--json",
            "--runtime-dir",
            str(runtime),
            "--gateway-config",
            str(config),
        ],
        capture_output=True,
        text=True,
        env={name: member for name, member in os.environ.items() if not name.startswith("SLACK_")},
    )
    assert "private-migration-sentinel" not in result.stdout + result.stderr
    assert "private-identity-sentinel" not in result.stdout + result.stderr
    return runtime, process, result


def test_migration_removes_raw_credentials_and_runtime_accepts_result(tmp_path):
    settings, gateway = old_settings()
    expected = {key: member for key, member in settings.items() if key != "slack"}
    expected.update(schema_version=2, channel_gateway=gateway)
    runtime, process, result = run(tmp_path, settings, gateway)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {"schema_version": 2, "migrated": True}
    assert json.loads(process.read_bytes()) == expected
    assert stat.S_IMODE(process.stat().st_mode) == 0o600
    actual = product().load_process_settings(str(runtime))
    assert actual.gateway_identities == ("default", "adapter/example")
    assert actual.launch_tls.worker_principal_id == settings["launch_tls"]["worker_principal_id"]
    assert sorted(path.name for path in runtime.iterdir()) == ["process.json"]


@pytest.mark.parametrize(
    "mutation",
    ["v2", "bad-url", "raw-token", "missing-identity", "unknown-field", "malformed-source"],
)
def test_migration_refuses_invalid_inputs_without_changing_source(tmp_path, mutation):
    settings, gateway = old_settings()
    if mutation == "v2":
        settings["schema_version"] = 2
    elif mutation == "bad-url":
        gateway["url"] = "http://gateway.example.com"
    elif mutation == "raw-token":
        gateway["token"] = settings["slack"]["token"]
    elif mutation == "missing-identity":
        gateway["identities"] = ["default"]
    elif mutation == "unknown-field":
        settings["unexpected"] = "private-migration-sentinel"
    else:
        settings["slack"]["identity_tokens"] = []
    _, process, result = run(tmp_path, settings, gateway)
    assert result.returncode == 1
    assert json.loads(result.stdout)["fix"]
    assert json.loads(process.read_bytes()) == settings
    assert sorted(path.name for path in process.parent.iterdir()) == ["process.json"]


@pytest.mark.parametrize(
    "mutation", ["symlink", "duplicate", "duplicate-map", "oversized", "malformed"]
)
def test_cli_migration_refuses_unsafe_source_and_preserves_bytes(tmp_path, mutation):
    settings, gateway = old_settings()
    runtime, process, result = run(tmp_path, settings, gateway)
    assert result.returncode == 0
    raw = json.dumps(settings)
    if mutation == "duplicate":
        raw = raw.replace('"schema_version": 1', '"schema_version": 1, "schema_version": 1')
    elif mutation == "duplicate-map":
        raw = raw.replace('"acme-adapter":', '"acme-adapter": "duplicate", "acme-adapter":')
    elif mutation == "oversized":
        raw += " " * 65536
    elif mutation == "malformed":
        raw = '{"schema_version":'
    process.write_text(raw)
    if mutation == "symlink":
        target = tmp_path / "source.json"
        process.rename(target)
        process.symlink_to(target)
    result = subprocess.run([
        str(CLI), "migrate-protected-process", "--json", "--runtime-dir", str(runtime),
        "--gateway-config", str(tmp_path / "gateway.json"),
    ], capture_output=True, text=True, env={"PATH": os.defpath})
    assert result.returncode == 1
    assert json.loads(result.stdout)["fix"]
    assert process.read_text() == raw
    assert "private-migration-sentinel" not in result.stdout + result.stderr
