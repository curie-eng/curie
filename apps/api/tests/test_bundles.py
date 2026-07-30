"""Plugin bundle pipeline: intake unit tests + real MinIO/Postgres round trip.

Nothing is mocked: the round-trip test uploads to real MinIO and records to real
Postgres from the compose stack (per B2 constraints).
"""

import hashlib
import io
import tarfile
import zipfile
from pathlib import Path
from typing import Any

from curie_api import bundles

MANIFEST = '{"name": "demo-plugin", "version": "0.1.0"}'


def _skill(name: str) -> str:
    return f"---\nname: {name}\ndescription: does {name} things\n---\n\n# {name}\n"


def _valid_files() -> dict[str, str]:
    return {
        ".claude-plugin/plugin.json": MANIFEST,
        "skills/alpha/SKILL.md": _skill("alpha"),
        "skills/beta/SKILL.md": _skill("beta"),
    }


def _tar_gz(files: dict[str, str], top: str = "demo-plugin") -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for rel, content in files.items():
            data = content.encode("utf-8")
            info = tarfile.TarInfo(f"{top}/{rel}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def _zip(files: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for rel, content in files.items():
            zf.writestr(rel, content)
    return buf.getvalue()


# --- pure intake tests (no MinIO/Postgres) -------------------------------


def test_valid_tar_gz_passes_validation(tmp_path: Path) -> None:
    ext, content_type, result = bundles.extract_and_validate(_tar_gz(_valid_files()), tmp_path)
    assert ext == ".tar.gz"
    assert content_type == "application/gzip"
    assert result.valid, result.errors


def test_valid_zip_passes_validation(tmp_path: Path) -> None:
    ext, _, result = bundles.extract_and_validate(_zip(_valid_files()), tmp_path)
    assert ext == ".zip"
    assert result.valid, result.errors


def test_missing_skill_frontmatter_is_rejected(tmp_path: Path) -> None:
    files = _valid_files()
    files["skills/beta/SKILL.md"] = "# beta with no frontmatter\n"
    _, _, result = bundles.extract_and_validate(_tar_gz(files), tmp_path)
    assert not result.valid
    codes = {e.code for e in result.errors}
    assert "skill.frontmatter_missing" in codes


def test_bad_manifest_is_rejected(tmp_path: Path) -> None:
    files = _valid_files()
    files[".claude-plugin/plugin.json"] = '{"version": "0.1.0"}'  # no name
    _, _, result = bundles.extract_and_validate(_tar_gz(files), tmp_path)
    assert not result.valid
    assert any(e.code.startswith("manifest.") for e in result.errors)


def test_read_bundle_text_files_includes_bare_root_manifest() -> None:
    # Upload validation accepts a manifest at bare root `plugin.json`, so the
    # files listing must surface it too -- not only the `.claude-plugin/`
    # location.
    files = {
        "plugin.json": MANIFEST,
        "skills/alpha/SKILL.md": _skill("alpha"),
    }
    returned = dict(bundles.read_bundle_text_files(_tar_gz(files)))
    assert returned["plugin.json"] == MANIFEST


def test_non_archive_is_rejected(tmp_path: Path) -> None:
    try:
        bundles.extract_and_validate(b"not an archive", tmp_path)
    except bundles.UnsupportedArchive:
        return
    raise AssertionError("expected UnsupportedArchive")


# --- full round trip against real MinIO + Postgres -----------------------


def _create_version(client: Any, headers: dict[str, str]) -> tuple[str, str]:
    agent = client.post(
        "/agents",
        json={"name": "bundle-agent", "slack_channel": "C000000B01"},
        headers=headers,
    ).json()
    version = client.post(
        f"/agents/{agent['id']}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=headers,
    ).json()
    return agent["id"], version["id"]


def test_upload_store_fetch_round_trip(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    archive = _tar_gz(_valid_files())
    url = f"/agents/{agent_id}/versions/{version_id}/bundle"

    resp = client.put(url, files={"file": ("demo.tar.gz", archive)}, headers=auth_headers)
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["bundle_ref"] == f"bundles/{agent_id}/{version_id}.tar.gz"
    assert body["bundle_sha256"] == hashlib.sha256(archive).hexdigest()
    assert body["size_bytes"] == len(archive)

    # The version now advertises the stored bundle.
    version = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()[0]
    assert version["bundle_ref"] == body["bundle_ref"]
    assert version["bundle_sha256"] == body["bundle_sha256"]

    # Fetch by version returns the exact bytes uploaded.
    got = client.get(url, headers=auth_headers)
    assert got.status_code == 200
    assert got.content == archive
    assert hashlib.sha256(got.content).hexdigest() == body["bundle_sha256"]


def test_bundles_are_immutable(client: Any, auth_headers: dict[str, str], clean_db: None) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    url = f"/agents/{agent_id}/versions/{version_id}/bundle"
    archive = _tar_gz(_valid_files())

    first = client.put(url, files={"file": ("demo.tar.gz", archive)}, headers=auth_headers)
    assert first.status_code == 201
    second = client.put(url, files={"file": ("demo.tar.gz", archive)}, headers=auth_headers)
    assert second.status_code == 409


def test_malformed_bundle_returns_actionable_errors(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    files = _valid_files()
    files["skills/beta/SKILL.md"] = "# no frontmatter\n"
    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("demo.tar.gz", _tar_gz(files))},
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert detail["detail"] == "bundle failed validation"
    codes = {e["code"] for e in detail["errors"]}
    assert "skill.frontmatter_missing" in codes


def test_fetch_missing_bundle_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    resp = client.get(f"/agents/{agent_id}/versions/{version_id}/bundle", headers=auth_headers)
    assert resp.status_code == 404


def test_read_version_files_returns_bundle_text_surfaces(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # After a real upload to MinIO, the files endpoint returns the bundle's text
    # surfaces (manifest + skill docs) with bundle-relative paths and content, so
    # the UI can render the authored bundle without pulling the raw archive.
    agent_id, version_id = _create_version(client, auth_headers)
    files = _valid_files()
    files["evals/cases.json"] = '[{"name": "greets", "input": "hi"}]'
    archive = _tar_gz(files)
    put = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("demo.tar.gz", archive)},
        headers=auth_headers,
    )
    assert put.status_code == 201, put.text

    resp = client.get(f"/agents/{agent_id}/versions/{version_id}/files", headers=auth_headers)
    assert resp.status_code == 200, resp.text
    returned = {f["path"]: f["content"] for f in resp.json()["files"]}
    assert set(returned) == {
        ".claude-plugin/plugin.json",
        "evals/cases.json",
        "skills/alpha/SKILL.md",
        "skills/beta/SKILL.md",
    }
    assert returned[".claude-plugin/plugin.json"] == MANIFEST
    assert returned["skills/alpha/SKILL.md"] == _skill("alpha")
    assert returned["evals/cases.json"] == '[{"name": "greets", "input": "hi"}]'


def test_read_version_files_missing_bundle_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # A version with no bundle stored yet has nothing to read.
    agent_id, version_id = _create_version(client, auth_headers)
    resp = client.get(f"/agents/{agent_id}/versions/{version_id}/files", headers=auth_headers)
    assert resp.status_code == 404


def test_read_version_files_unknown_version_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, _ = _create_version(client, auth_headers)
    missing = "00000000-0000-0000-0000-000000000000"
    resp = client.get(f"/agents/{agent_id}/versions/{missing}/files", headers=auth_headers)
    assert resp.status_code == 404
