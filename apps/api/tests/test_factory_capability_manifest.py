"""A deployed factory capability claim is the claim in its exact bundle."""

import hashlib
import io
import json
import tarfile
from pathlib import Path
from typing import Any

import pytest
from curie_api import bundles, factory_capabilities
from curie_api.config import get_settings
from curie_api.storage import build_s3_client
from plugin_format import connector_lock
from plugin_format.connectors import ConnectorBuild

IMAGE = "ghcr.io/acme-corp/acme-bot@sha256:" + "1" * 64
BASE = "ghcr.io/acme-corp/acme-base@sha256:" + "2" * 64


def test_bundle_digest_matches_shared_cross_language_vector(tmp_path: Path) -> None:
    vector = json.loads(
        (Path(__file__).parents[3] / "tests/vectors/factory-capability-bundle.json").read_text()
    )
    for name, content in vector["files"].items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    assert factory_capabilities.bundle_input_digest(tmp_path) == vector["expected_sha256"]


def _archive(root: Path) -> bytes:
    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as archive:
        for path in sorted(root.rglob("*")):
            if path.is_file():
                archive.add(path, arcname=f"acme-bot/{path.relative_to(root)}")
    return out.getvalue()


def _bundle(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    root = tmp_path / "acme-bot"
    (root / ".claude-plugin").mkdir(parents=True)
    (root / "skills" / "example").mkdir(parents=True)
    (root / "runner").mkdir()
    (root / "verification").mkdir()
    (root / ".claude-plugin/plugin.json").write_text(
        '{"name":"acme-bot","version":"0.1.0"}', encoding="utf-8"
    )
    (root / "skills/example/SKILL.md").write_text(
        "---\nname: example\ndescription: an example\n---\n\n# Example\n", encoding="utf-8"
    )
    (root / "runner/Dockerfile").write_text(
        "ARG CURIE_RUNNER_IMAGE\nFROM ${CURIE_RUNNER_IMAGE}\n", encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(
        "runner:\n  build:\n    context: runner\n    platforms: [linux/amd64, linux/arm64]\n",
        encoding="utf-8",
    )
    build = ConnectorBuild.model_validate(
        {"context": "runner", "platforms": ["linux/amd64", "linux/arm64"]}
    )
    source_digest = connector_lock.source_digest_of(root / "runner", build)
    (root / "connectors.lock.yaml").write_text(
        "version: 1\nconnectors: {}\nrunner:\n"
        f"  image: {IMAGE}\n  base: {BASE}\n  delivery: registry\n"
        "  platforms: [linux/amd64, linux/arm64]\n"
        f"  source_digest: {source_digest}\n",
        encoding="utf-8",
    )
    inputs = {
        "required_executables": [
            {"name": "cargo fmt", "command": ["cargo", "fmt", "--version"]},
            {"name": "cargo clippy", "command": ["cargo", "clippy", "--version"]},
        ],
        "registry_hosts": ["crates.io"],
        "services": [],
        "egress_class": "restricted",
    }
    (root / "verification/capability-inputs.json").write_text(
        json.dumps(inputs), encoding="utf-8"
    )
    manifest = {
        "schema_version": 1,
        "runner_image": IMAGE,
        "bundle_input_sha256": factory_capabilities.bundle_input_digest(root),
        "executables": [
            {
                "name": item["name"],
                "command": item["command"],
                "path": "/usr/bin/cargo",
                "version": "1.0",
            }
            for item in inputs["required_executables"]
        ],
        "registry_hosts": inputs["registry_hosts"],
        "services": [],
        "connectors": [],
        "verification": {"source": "repository", "checks": [], "lockfile_installs": False},
        "egress_class": "restricted",
        "publication": {
            "github_directory_writable": False,
            "code_host_credential_in_sandbox": False,
        },
    }
    (root / "verification/capabilities.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    return root, manifest


def test_manifest_matches_inputs_and_runner_lock(tmp_path: Path) -> None:
    root, manifest = _bundle(tmp_path)
    archive = _archive(root)
    _, _, validation = bundles.extract_and_validate(archive, tmp_path / "validated")
    assert validation.valid, validation.errors
    result = factory_capabilities.read_manifest(
        archive, get_settings(), expected_bundle_sha256=hashlib.sha256(archive).hexdigest()
    )
    assert result is not None
    expected = factory_capabilities.CapabilityManifest.model_validate(manifest)
    assert result.manifest == expected


def test_bundle_verification_checks_take_precedence(tmp_path: Path) -> None:
    root, manifest = _bundle(tmp_path)
    checks = [
        {
            "id": "fmt",
            "paths": ["**/*.rs"],
            "command": ["cargo", "fmt", "--check"],
            "delegated_to": "rust-tests",
        }
    ]
    (root / "verification/checks.json").write_text(
        json.dumps({"checks": checks, "lockfile_installs": True}), encoding="utf-8"
    )
    manifest["verification"] = {
        "source": "bundle",
        "checks": checks,
        "lockfile_installs": True,
    }
    manifest["bundle_input_sha256"] = factory_capabilities.bundle_input_digest(root)
    (root / "verification/capabilities.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    read = factory_capabilities.read_manifest(_archive(root), get_settings())
    assert read is not None
    assert read.manifest.verification.source == "bundle"
    assert read.manifest.verification.checks[0].id == "fmt"


@pytest.mark.parametrize(
    "change", ["bundle", "runner", "tool", "registry", "connector", "verification"]
)
def test_manifest_refuses_false_claims(tmp_path: Path, change: str) -> None:
    root, manifest = _bundle(tmp_path)
    if change == "bundle":
        (root / "skills/example/SKILL.md").write_text("changed", encoding="utf-8")
    elif change == "runner":
        manifest["runner_image"] = "ghcr.io/acme-corp/acme-bot@sha256:" + "3" * 64
    elif change == "tool":
        manifest["executables"][0]["command"] = ["cargo", "--version"]
    elif change == "registry":
        manifest["registry_hosts"] = ["example.com"]
    elif change == "connector":
        manifest["connectors"] = ["undeclared"]
    elif change == "verification":
        manifest["verification"] = {
            "source": "bundle",
            "checks": [
                {"id": "fmt", "paths": ["**/*.rs"], "command": ["cargo", "fmt", "--check"]}
            ],
            "lockfile_installs": False,
        }
    (root / "verification/capabilities.json").write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(factory_capabilities.CapabilityUnknown):
        factory_capabilities.read_manifest(_archive(root), get_settings())


def test_manifest_missing_is_unknown_for_read_but_allowed_for_legacy_upload(tmp_path: Path) -> None:
    root, _ = _bundle(tmp_path)
    (root / "verification/capabilities.json").unlink()
    archive = _archive(root)
    assert factory_capabilities.read_manifest(archive, get_settings(), required=False) is None
    with pytest.raises(factory_capabilities.CapabilityUnknown, match="absent"):
        factory_capabilities.read_manifest(archive, get_settings())


def test_deployment_read_round_trip(
    tmp_path: Path, client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    root, manifest = _bundle(tmp_path)
    archive = _archive(root)
    agent = client.post(
        "/agents",
        json={"name": "acme-bot", "channel": {"kind": "slack", "address": "C0EXAMPLE1"}},
        headers=auth_headers,
    )
    assert agent.status_code == 201, agent.text
    agent_id = agent.json()["id"]
    version = client.post(
        f"/agents/{agent_id}/versions",
        json={"version_label": "v1", "created_by": "example"},
        headers=auth_headers,
    )
    assert version.status_code == 201, version.text
    version_id = version.json()["id"]
    upload = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("acme-bot.tar.gz", archive)},
        headers=auth_headers,
    )
    assert upload.status_code == 201, upload.text
    deployment = client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=auth_headers,
    )
    assert deployment.status_code == 201, deployment.text
    read = client.get(
        f"/deployments/{deployment.json()['id']}/capabilities", headers=auth_headers
    )
    assert read.status_code == 200, read.text
    assert read.json()["manifest"]["runner_image"] == manifest["runner_image"]
    assert read.json()["bundle_sha256"] == hashlib.sha256(archive).hexdigest()
    assert "credentials" not in read.text
    assert client.get(f"/deployments/{deployment.json()['id']}/capabilities").status_code == 401
    settings = get_settings()
    build_s3_client(settings).delete_object(
        Bucket=settings.bundle_bucket, Key=upload.json()["bundle_ref"]
    )
    missing = client.get(
        f"/deployments/{deployment.json()['id']}/capabilities", headers=auth_headers
    )
    assert missing.status_code == 409
    assert missing.json()["detail"] == "environment_unknown"
