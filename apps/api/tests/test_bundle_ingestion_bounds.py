"""Bundle ingestion bounds (ADR-0059 decision 3, #757).

Covers the three "done when" cases: an oversized upload rejected before it is
buffered into memory, a zip-bomb-shaped archive refused with nothing written
(the archive-level guard itself is unit-tested in
``packages/plugin-format/tests/test_archive.py``; this file only exercises the
API's own use of it), and an already-stored ("legacy") bundle that exceeds the
current caps failing at deploy time with an actionable message.
"""

import asyncio
import hashlib
import io
import tarfile
import uuid
import zipfile
from typing import Any

import pytest
from curie_api import crud
from curie_api import deploy as deploy_module
from curie_api.config import Settings, get_settings
from curie_api.routers import bundles as bundles_router
from curie_api.storage import BundleStore
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

MANIFEST = '{"name": "demo-plugin", "version": "0.1.0"}'


def _tar_plain(files: dict[str, bytes], top: str = "demo-plugin") -> bytes:
    """An UNCOMPRESSED tar so the wire size is deterministic (no gzip shrinking
    filler content out from under a size assertion)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:") as tf:
        for rel, content in files.items():
            info = tarfile.TarInfo(f"{top}/{rel}")
            info.size = len(content)
            tf.addfile(info, io.BytesIO(content))
    return buf.getvalue()


def _create_version(client: Any, headers: dict[str, str]) -> tuple[str, str]:
    agent = client.post(
        "/agents",
        json={"name": "bundle-bounds-agent", "slack_channel": "C0000BND1"},
        headers=headers,
    ).json()
    version = client.post(
        f"/agents/{agent['id']}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=headers,
    ).json()
    return agent["id"], version["id"]


# --- upload size cap, enforced before buffering --------------------------


def test_oversized_upload_is_rejected_before_buffering(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    max_bytes = 200
    monkeypatch.setattr(
        bundles_router,
        "get_settings",
        lambda: Settings(bundle_upload_max_bytes=max_bytes),
    )
    agent_id, version_id = _create_version(client, auth_headers)

    calls: list[uuid.UUID] = []
    real_get_version = crud.get_version

    async def _tracking_get_version(session: Any, vid: uuid.UUID) -> Any:
        calls.append(vid)
        return await real_get_version(session, vid)

    monkeypatch.setattr(crud, "get_version", _tracking_get_version)

    # An uncompressed archive carrying 2000 filler bytes: well over the 200
    # byte cap regardless of tar's own header/padding overhead.
    archive = _tar_plain(
        {
            ".claude-plugin/plugin.json": MANIFEST.encode(),
            "big.bin": b"x" * 2000,
        }
    )
    assert len(archive) > max_bytes

    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("big.tar", archive)},
        headers=auth_headers,
    )
    assert resp.status_code == 413, resp.text

    # The size gate ran before the version lookup: no DB work happened for a
    # request that was always going to be rejected.
    assert calls == []

    # Nothing was stored.
    version = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()[0]
    assert version["bundle_ref"] is None


def test_upload_at_the_limit_is_accepted(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = {
        ".claude-plugin/plugin.json": MANIFEST.encode(),
        "skills/greeter/SKILL.md": (b"---\nname: greeter\ndescription: greets\n---\n"),
    }
    archive = _tar_plain(files)
    monkeypatch.setattr(
        bundles_router,
        "get_settings",
        lambda: Settings(bundle_upload_max_bytes=len(archive)),
    )
    agent_id, version_id = _create_version(client, auth_headers)

    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("demo.tar", archive)},
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text


def _zip_bomb_shaped(size: int = 200_000) -> bytes:
    """A single highly-compressible entry: small on the wire, huge once
    expanded -- well under the (generous) default upload-size cap, so this
    exercises the extraction-time ratio guard specifically, not the upload
    body-size gate."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("zeros.bin", b"\x00" * size)
    return buf.getvalue()


def test_zip_bomb_upload_is_refused_with_nothing_written(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    archive = _zip_bomb_shaped()
    assert len(archive) < 2_000  # tiny on the wire; clears the upload size gate

    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("bomb.zip", archive)},
        headers=auth_headers,
    )
    assert resp.status_code == 400, resp.text
    assert "compression ratio" in resp.json()["detail"]

    version = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()[0]
    assert version["bundle_ref"] is None


def _many_member_tar_gz(count: int, top: str = "demo-plugin") -> bytes:
    """A tar.gz of ``count`` zero-byte members plus a manifest. gzip shrinks the
    repetitive headers so the wire size stays tiny (clearing the upload gate),
    while the declared member count is what the member-count cap must catch."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        manifest = MANIFEST.encode()
        info = tarfile.TarInfo(f"{top}/.claude-plugin/plugin.json")
        info.size = len(manifest)
        tf.addfile(info, io.BytesIO(manifest))
        for i in range(count):
            member = tarfile.TarInfo(f"{top}/f{i}")
            member.size = 0
            tf.addfile(member)
    return buf.getvalue()


def test_many_member_upload_is_refused_by_member_count_cap(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        deploy_module,
        "get_settings",
        lambda: Settings(bundle_max_members=10),
    )
    agent_id, version_id = _create_version(client, auth_headers)
    archive = _many_member_tar_gz(200)
    assert len(archive) < 4_000  # tiny on the wire; clears the upload size gate

    resp = client.put(
        f"/agents/{agent_id}/versions/{version_id}/bundle",
        files={"file": ("many.tar.gz", archive)},
        headers=auth_headers,
    )
    assert resp.status_code == 400, resp.text
    assert "member-count" in resp.json()["detail"]

    version = client.get(f"/agents/{agent_id}/versions", headers=auth_headers).json()[0]
    assert version["bundle_ref"] is None


# --- legacy bundle revalidated against the CURRENT caps at deploy time ---


def _store_legacy_bundle(agent_id: str, version_id: str, data: bytes) -> None:
    """Write bytes directly to the store and attach them to the version,
    bypassing the upload endpoint entirely -- simulating a bundle written
    before ADR-0059's size/ratio caps existed (or under looser ones)."""

    async def _run() -> None:
        settings = get_settings()
        store = BundleStore(settings)
        key = f"bundles/{agent_id}/{version_id}.tar"
        await store.put(key, data, "application/x-tar")
        engine = create_async_engine(settings.database_url)
        maker = async_sessionmaker(engine, expire_on_commit=False)
        async with maker() as session:
            version = await crud.get_version(session, uuid.UUID(version_id))
            assert version is not None
            await crud.attach_bundle(session, version, key, hashlib.sha256(data).hexdigest())
        await engine.dispose()

    asyncio.run(_run())


def test_legacy_oversized_bundle_fails_at_deploy_time_with_actionable_message(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent_id, version_id = _create_version(client, auth_headers)
    # An entirely ordinary bundle by today's standards -- this test simulates it
    # having been written back when the caps were looser or absent, so the
    # CURRENT cap (set tight here) must catch it at deploy time.
    data = _tar_plain(
        {
            ".claude-plugin/plugin.json": MANIFEST.encode(),
            "big.bin": b"x" * 5000,
        }
    )
    _store_legacy_bundle(agent_id, version_id, data)

    monkeypatch.setattr(
        deploy_module,
        "get_settings",
        lambda: Settings(bundle_max_uncompressed_bytes=1000),
    )

    resp = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version_id,
            "environment": "dev",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 422, resp.text
    detail = resp.json()["detail"]
    assert "size/ratio" in detail
    assert version_id in detail  # actionable: names the affected version
    assert "rebuilt and re-uploaded" in detail

    # Nothing was deployed.
    deployments = client.get(
        "/deployments", params={"agent_id": agent_id}, headers=auth_headers
    ).json()
    assert deployments == []


def test_legacy_bundle_within_current_caps_deploys_normally(
    client: Any,
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The revalidation is a real gate, not a rubber stamp: a bundle that
    genuinely fits under the current caps still deploys."""
    agent_id, version_id = _create_version(client, auth_headers)
    data = _tar_plain({".claude-plugin/plugin.json": MANIFEST.encode()})
    _store_legacy_bundle(agent_id, version_id, data)

    monkeypatch.setattr(
        deploy_module,
        "get_settings",
        lambda: Settings(bundle_max_uncompressed_bytes=1_000_000),
    )

    resp = client.post(
        "/deployments",
        json={
            "agent_id": agent_id,
            "version_id": version_id,
            "environment": "dev",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text


def test_version_with_no_bundle_yet_deploys_without_revalidation(
    client: Any, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A version created but not yet bundled (the pre-B2 residue case) is not
    blocked by the revalidation -- it is a no-op when bundle_ref is None."""
    agent = client.post(
        "/agents",
        json={"name": "bundleless-agent", "slack_channel": "C0000BND2"},
        headers=auth_headers,
    ).json()
    version = client.post(
        f"/agents/{agent['id']}/versions",
        json={"version_label": "v1", "created_by": "bconn"},
        headers=auth_headers,
    ).json()
    assert version["bundle_ref"] is None

    resp = client.post(
        "/deployments",
        json={
            "agent_id": agent["id"],
            "version_id": version["id"],
            "environment": "dev",
        },
        headers=auth_headers,
    )
    assert resp.status_code == 201, resp.text
