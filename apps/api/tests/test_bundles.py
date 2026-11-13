"""Plugin bundle pipeline: intake unit tests + real RustFS/Postgres round trip.

Nothing is mocked: the round-trip test uploads to real RustFS and records to real
Postgres from the compose stack (per B2 constraints).
"""

import asyncio
import hashlib
import io
import tarfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

from curie_api import bundles
from curie_api.config import get_settings
from curie_api.storage import BundleStore
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

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


# --- pure intake tests (no RustFS/Postgres) -------------------------------


def test_valid_tar_gz_passes_validation(tmp_path: Path) -> None:
    ext, content_type, result = bundles.extract_and_validate(
        _tar_gz(_valid_files()), tmp_path
    )
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


# A valid tracker result declaration (ADR 0277 decision 1, #5585); the parser
# and its vectors are covered in test_tracker_result_declaration.py.
RESULT_DECLARATION = (
    '{"schema_version": 1, "contract": "tracker_result", '
    '"results": ["ticket", "findings"], "implementation_label": "curie-factory"}'
)


def test_a_malformed_result_declaration_is_a_validation_issue(tmp_path: Path) -> None:
    files = _valid_files()
    files["workflow/result.json"] = '{"schema_version": 1, "contract": "tracker_result"}'
    _, _, result = bundles.extract_and_validate(_tar_gz(files), tmp_path)
    assert not result.valid
    assert [(e.code, e.location) for e in result.errors] == [
        ("tracker_result.missing_field", "workflow/result.json (results)")
    ]


def test_a_valid_result_declaration_passes_extraction_validation(tmp_path: Path) -> None:
    # Intake validation accepts it; `deploy.validate_archive` refuses it after.
    files = _valid_files()
    files["workflow/result.json"] = RESULT_DECLARATION
    _, _, result = bundles.extract_and_validate(_zip(files), tmp_path)
    assert result.valid, result.errors


def test_a_result_declaration_path_that_is_a_directory_is_refused(tmp_path: Path) -> None:
    files = _valid_files()
    files["workflow/result.json/inner.json"] = RESULT_DECLARATION
    _, _, result = bundles.extract_and_validate(_tar_gz(files), tmp_path)
    assert not result.valid
    assert [e.code for e in result.errors] == ["tracker_result.unreadable"]


# --- full round trip against real RustFS + Postgres -----------------------


def _create_version(client: Any, headers: dict[str, str]) -> tuple[str, str]:
    agent = client.post(
        "/agents",
        json={"name": "bundle-agent", "channel": {"kind": "slack", "address": "C000000B01"}},
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

    resp = client.put(
        url, files={"file": ("demo.tar.gz", archive)}, headers=auth_headers
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["bundle_ref"] == f"bundles/{agent_id}/{version_id}.tar.gz"
    assert body["bundle_sha256"] == hashlib.sha256(archive).hexdigest()
    assert body["size_bytes"] == len(archive)

    # The version now advertises the stored bundle.
    version = client.get(
        f"/agents/{agent_id}/versions", headers=auth_headers
    ).json()[0]
    assert version["bundle_ref"] == body["bundle_ref"]
    assert version["bundle_sha256"] == body["bundle_sha256"]

    # Fetch by version returns the exact bytes uploaded.
    got = client.get(url, headers=auth_headers)
    assert got.status_code == 200
    assert got.content == archive
    assert hashlib.sha256(got.content).hexdigest() == body["bundle_sha256"]


def test_bundles_are_immutable(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    url = f"/agents/{agent_id}/versions/{version_id}/bundle"
    archive = _tar_gz(_valid_files())

    first = client.put(
        url, files={"file": ("demo.tar.gz", archive)}, headers=auth_headers
    )
    assert first.status_code == 201
    second = client.put(
        url, files={"file": ("demo.tar.gz", archive)}, headers=auth_headers
    )
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
    resp = client.get(
        f"/agents/{agent_id}/versions/{version_id}/bundle", headers=auth_headers
    )
    assert resp.status_code == 404


def test_read_version_files_returns_bundle_text_surfaces(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # After a real upload to RustFS, the files endpoint returns the bundle's text
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

    resp = client.get(
        f"/agents/{agent_id}/versions/{version_id}/files", headers=auth_headers
    )
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
    resp = client.get(
        f"/agents/{agent_id}/versions/{version_id}/files", headers=auth_headers
    )
    assert resp.status_code == 404


def test_read_version_files_unknown_version_is_404(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, _ = _create_version(client, auth_headers)
    missing = "00000000-0000-0000-0000-000000000000"
    resp = client.get(
        f"/agents/{agent_id}/versions/{missing}/files", headers=auth_headers
    )
    assert resp.status_code == 404


# --- tracker result declaration refusal (ADR 0277, #5585) -----------------


def _store_directly(agent_id: str, version_id: str, data: bytes) -> None:
    """Attach ``data`` to the version without passing intake.

    Simulates a bundle stored before intake refused a tracker result
    declaration: written straight to the object store under a fresh key (keys
    are write-once) and pointed at by the version row, as
    ``test_sealing_key_custody_paths.py`` does for custody.
    """

    async def run() -> None:
        settings = get_settings()
        key = f"bundles/{agent_id}/{version_id}-legacy-{uuid.uuid4().hex[:6]}.tar.gz"
        await BundleStore(settings).put(key, data, "application/gzip")
        engine = create_async_engine(settings.database_url)
        try:
            async with engine.begin() as conn:
                await conn.execute(
                    text(
                        "UPDATE curie.agent_versions SET bundle_ref = :ref, "
                        "bundle_sha256 = :sha WHERE id = :id"
                    ),
                    {
                        "ref": key,
                        "sha": hashlib.sha256(data).hexdigest(),
                        "id": uuid.UUID(version_id),
                    },
                )
        finally:
            await engine.dispose()

    asyncio.run(run())


def _count_rows(table: str, agent_id: str) -> int:
    async def run() -> int:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                value = await conn.scalar(
                    text(f"SELECT count(*) FROM curie.{table} WHERE agent_id = :agent_id"),
                    {"agent_id": uuid.UUID(agent_id)},
                )
        finally:
            await engine.dispose()
        return int(value or 0)

    return asyncio.run(run())


def _deploy(client: Any, headers: dict[str, str], agent_id: str, version_id: str) -> Any:
    return client.post(
        "/deployments",
        json={"agent_id": agent_id, "version_id": version_id, "environment": "dev"},
        headers=headers,
    )


def test_upload_with_a_result_declaration_is_refused_and_leaves_nothing_behind(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    url = f"/agents/{agent_id}/versions/{version_id}/bundle"
    files = _valid_files()
    files["workflow/result.json"] = RESULT_DECLARATION

    resp = client.put(url, files={"file": ("demo.tar.gz", _tar_gz(files))}, headers=auth_headers)

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["code"] == "tracker_result_not_enabled"
    # The version made for this upload is gone, no object was stored, and
    # nothing was deployed or queued for the agent.
    assert client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json() == []
    assert client.get(url, headers=auth_headers).status_code == 404
    assert _count_rows("deployments", agent_id) == 0
    assert _count_rows("work_items", agent_id) == 0


def test_a_refused_upload_onto_a_deployed_version_keeps_the_version(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The documented deploy-then-attach order: a deployment already references
    # the bundleless version, so the refusal must not delete it.
    agent_id, version_id = _create_version(client, auth_headers)
    deployed = _deploy(client, auth_headers, agent_id, version_id)
    assert deployed.status_code == 201, deployed.text
    files = _valid_files()
    files["workflow/result.json"] = RESULT_DECLARATION

    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("demo.tar.gz", _tar_gz(files))},
        headers=auth_headers,
    )

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["code"] == "tracker_result_not_enabled"
    versions = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()
    assert [(v["id"], v["bundle_ref"]) for v in versions] == [(version_id, None)]
    assert _count_rows("deployments", agent_id) == 1


def test_the_same_bundle_without_the_result_declaration_uploads_and_deploys(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    archive = _tar_gz(_valid_files())

    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("demo.tar.gz", archive)},
        headers=auth_headers,
    )

    assert resp.status_code == 201, resp.text
    assert resp.json()["bundle_sha256"] == hashlib.sha256(archive).hexdigest()
    deployed = _deploy(client, auth_headers, agent_id, version_id)
    assert deployed.status_code == 201, deployed.text
    assert _count_rows("deployments", agent_id) == 1


def test_upload_with_a_malformed_result_declaration_reports_validation_first(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    files = _valid_files()
    files["workflow/result.json"] = (
        '{"schema_version": true, "contract": "tracker_result", "results": ["findings"]}'
    )

    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("demo.tar.gz", _tar_gz(files))},
        headers=auth_headers,
    )

    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert detail["detail"] == "bundle failed validation"
    assert [e["code"] for e in detail["errors"]] == ["tracker_result.schema_version"]
    assert "tracker_result_not_enabled" not in resp.text


def test_deploying_a_stored_bundle_with_a_result_declaration_is_refused(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    files = _valid_files()
    files["workflow/result.json"] = RESULT_DECLARATION
    _store_directly(agent_id, version_id, _tar_gz(files))

    resp = _deploy(client, auth_headers, agent_id, version_id)

    assert resp.status_code == 422, resp.text
    assert resp.json()["detail"]["code"] == "tracker_result_not_enabled"
    assert _count_rows("deployments", agent_id) == 0
    assert _count_rows("work_items", agent_id) == 0


def test_deploying_a_stored_bundle_with_a_malformed_result_declaration_reports_it(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    files = _valid_files()
    files["workflow/result.json"] = "[]"
    _store_directly(agent_id, version_id, _tar_gz(files))

    resp = _deploy(client, auth_headers, agent_id, version_id)

    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert detail["detail"] == "bundle failed validation"
    assert [e["code"] for e in detail["errors"]] == ["tracker_result.not_object"]
    assert _count_rows("deployments", agent_id) == 0


def test_deploying_the_same_stored_bundle_without_the_declaration_succeeds(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The paired control: direct seeding itself deploys.
    agent_id, version_id = _create_version(client, auth_headers)
    _store_directly(agent_id, version_id, _tar_gz(_valid_files()))

    resp = _deploy(client, auth_headers, agent_id, version_id)

    assert resp.status_code == 201, resp.text
    assert _count_rows("deployments", agent_id) == 1
