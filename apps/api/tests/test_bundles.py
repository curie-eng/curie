"""Plugin bundle pipeline: intake unit tests + real RustFS/Postgres round trip.

Nothing is mocked: the round-trip test uploads to real RustFS and records to real
Postgres from the compose stack (per B2 constraints).
"""

import asyncio
import hashlib
import importlib.util
import io
import json
import sys
import tarfile
import uuid
import zipfile
from pathlib import Path
from typing import Any

import pytest
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


# A bundle that still carries the retired result declaration. Its content is
# irrelevant: presence alone is refused.
RETIRED_DECLARATION = {"workflow/result.json": "{}"}
RETIRED_DECLARATION_DIRECTORY = {"workflow/result.json/inner.json": "{}"}


def test_a_retired_result_declaration_is_a_validation_issue(tmp_path: Path) -> None:
    _, _, result = bundles.extract_and_validate(
        _tar_gz({**_valid_files(), **RETIRED_DECLARATION}), tmp_path
    )

    assert not result.valid
    assert [(e.code, e.location, e.message) for e in result.errors] == [
        (
            "bundle.result_declaration_retired",
            "workflow/result.json",
            bundles.RETIRED_RESULT_DECLARATION_MESSAGE,
        )
    ]


def test_retired_result_declaration_message_names_the_binding_step(tmp_path: Path) -> None:
    _, _, result = bundles.extract_and_validate(
        _tar_gz({**_valid_files(), **RETIRED_DECLARATION}), tmp_path
    )

    message = result.errors[0].message
    assert bundles.RETIRED_RESULT_DECLARATION_DOCS_HEADING in message
    assert "Before upgrading: an existing triage binding" in message
    assert "remove" in message
    assert "replace" in message


def test_retired_result_declaration_heading_exists_in_the_operations_guide() -> None:
    guide = Path(__file__).resolve().parents[3] / "docs" / "operations.md"
    headings = {
        line.lstrip("#").strip() for line in guide.read_text().splitlines() if line.startswith("#")
    }

    assert bundles.RETIRED_RESULT_DECLARATION_DOCS_HEADING in headings


def test_a_result_declaration_path_that_is_a_directory_is_refused(tmp_path: Path) -> None:
    _, _, result = bundles.extract_and_validate(
        _tar_gz({**_valid_files(), **RETIRED_DECLARATION_DIRECTORY}), tmp_path
    )

    assert not result.valid
    assert [e.code for e in result.errors] == ["bundle.result_declaration_retired"]


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


# --- retired result declaration refusal (#5804) ---------------------------


def _store_directly(agent_id: str, version_id: str, data: bytes) -> None:
    """Attach ``data`` to the version without passing intake.

    Simulates a bundle an earlier build stored with ``workflow/result.json``:
    written straight to the object store under a fresh key (keys
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


def _upload(
    client: Any, headers: dict[str, str], agent_id: str, version_id: str, data: bytes
) -> Any:
    return client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("demo.tar.gz", data)},
        headers=headers,
    )


def _validation_codes(response: Any) -> list[str]:
    detail = response.json()["detail"]
    assert detail["detail"] == "bundle failed validation", detail
    return [e["code"] for e in detail["errors"]]


def _attached_bundle_ref(client: Any, headers: dict[str, str], agent_id: str) -> Any:
    (version,) = client.get(f"/agents/{agent_id}/versions", headers=headers).json()
    return version["bundle_ref"]


def test_upload_refuses_a_retired_result_declaration(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)

    refused = _upload(
        client,
        auth_headers,
        agent_id,
        version_id,
        _tar_gz({**_valid_files(), **RETIRED_DECLARATION}),
    )

    assert refused.status_code == 422, refused.text
    assert _validation_codes(refused) == ["bundle.result_declaration_retired"]
    assert _attached_bundle_ref(client, auth_headers, agent_id) is None
    fetched = client.get(f"/agents/{agent_id}/versions/{version_id}/bundle", headers=auth_headers)
    assert fetched.status_code == 404
    # Liveness: the same bundle without the file attaches to the same version.
    accepted = _upload(client, auth_headers, agent_id, version_id, _tar_gz(_valid_files()))
    assert accepted.status_code == 201, accepted.text
    assert _attached_bundle_ref(client, auth_headers, agent_id) == accepted.json()["bundle_ref"]


def test_upload_refuses_a_retired_result_declaration_directory(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)

    refused = _upload(
        client,
        auth_headers,
        agent_id,
        version_id,
        _tar_gz({**_valid_files(), **RETIRED_DECLARATION_DIRECTORY}),
    )

    assert refused.status_code == 422, refused.text
    assert _validation_codes(refused) == ["bundle.result_declaration_retired"]
    assert _attached_bundle_ref(client, auth_headers, agent_id) is None


def test_deploy_refuses_a_stored_retired_result_declaration(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    _store_directly(agent_id, version_id, _tar_gz({**_valid_files(), **RETIRED_DECLARATION}))

    refused = _deploy(client, auth_headers, agent_id, version_id)

    assert refused.status_code == 422, refused.text
    assert _validation_codes(refused) == ["bundle.result_declaration_retired"]
    assert _count_rows("deployments", agent_id) == 0
    # Liveness: the same version repointed at the bytes without the file deploys.
    _store_directly(agent_id, version_id, _tar_gz(_valid_files()))
    deployed = _deploy(client, auth_headers, agent_id, version_id)
    assert deployed.status_code == 201, deployed.text
    assert _count_rows("deployments", agent_id) == 1


def test_deploy_refuses_a_stored_retired_result_declaration_directory(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    _store_directly(
        agent_id, version_id, _tar_gz({**_valid_files(), **RETIRED_DECLARATION_DIRECTORY})
    )

    refused = _deploy(client, auth_headers, agent_id, version_id)

    assert refused.status_code == 422, refused.text
    assert _validation_codes(refused) == ["bundle.result_declaration_retired"]
    assert _count_rows("deployments", agent_id) == 0


# The live chain judge reads a deploy refusal out of the CLI's `--json` error
# envelope; it is stdlib only, so it loads by path.
JUDGE_PATH = (
    Path(__file__).resolve().parents[3] / "tools" / "factory-e2e" / "live_github_chain_judge.py"
)


def _judge() -> Any:
    spec = importlib.util.spec_from_file_location("live_github_chain_judge", JUDGE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(spec.name, None)
    return module


REFUSAL_VECTOR = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "vectors"
    / "bundle-upload-refusal-cli-output.json"
)


def _refusal_cases() -> dict[str, Any]:
    cases: dict[str, Any] = json.loads(REFUSAL_VECTOR.read_text())["cases"]
    return cases


def test_the_chain_judge_reads_the_retired_code_from_the_cli_upload_refusal(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """The producer chain the live judge consumes, hop by hop.

    The API's real 422 body for each case must equal the vector's
    ``api_body_text``; ``cli/tests/bundle_refusal_output.rs`` pins that the
    built binary prints the vector's ``cli_stdout`` for exactly that body. The
    judge is then fed that recorded CLI output, never a shape written here.
    """

    judge = _judge()
    cases = _refusal_cases()
    # A refused upload attaches nothing, so every case can target one version.
    agent_id, version_id = _create_version(client, auth_headers)
    for name, case in cases.items():
        files = {**_valid_files(), **case["bundle_files"]}
        response = _upload(client, auth_headers, agent_id, version_id, _tar_gz(files))
        if response.status_code != case["api_status"] or response.text != case["api_body_text"]:
            pytest.fail(f"{name}: the API answered {response.status_code} {response.text}")

    retired = cases["retired_declaration"]["cli_stdout"]
    assert judge.BUNDLE_RETIRED_CODE in judge.deploy_refusal_codes(retired)
    # Counterexample: the CLI's real output for a different validation refusal.
    other = cases["skill_without_frontmatter"]["cli_stdout"]
    assert judge.BUNDLE_RETIRED_CODE not in judge.deploy_refusal_codes(other)


def test_deploying_the_same_stored_bundle_without_the_declaration_succeeds(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    # The paired control: direct seeding itself deploys.
    agent_id, version_id = _create_version(client, auth_headers)
    _store_directly(agent_id, version_id, _tar_gz(_valid_files()))

    resp = _deploy(client, auth_headers, agent_id, version_id)

    assert resp.status_code == 201, resp.text
    assert _count_rows("deployments", agent_id) == 1
