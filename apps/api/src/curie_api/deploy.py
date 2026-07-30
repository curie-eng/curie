"""Shared bundle persistence used by the upload endpoint (B2) and git flow (J1).

Validation and storage are split so a caller can reject an invalid bundle before
creating any database rows, then store the validated bytes under the immutable
per-version key.
"""

import hashlib
import tempfile
import uuid
from pathlib import Path

import plugin_format
from sqlalchemy.ext.asyncio import AsyncSession

from . import bundles, crud
from .config import Settings, get_settings
from .models import AgentVersion
from .schemas import BundleOut
from .storage import ObjectStore


class BundleInvalid(Exception):
    """A bundle failed plugin-format validation; carries the actionable errors."""

    def __init__(self, errors: list[dict[str, str]]) -> None:
        super().__init__("bundle failed validation")
        self.errors = errors


class BundleTooLarge(Exception):
    """An already-stored bundle fails the CURRENT size/ratio caps.

    Raised by ``revalidate_stored_bundle`` -- the backward-compatibility case
    ADR-0059 decision 3 commits to: a bundle stored before these caps existed
    (or under looser ones) must be rejected here, at deploy time, with an
    actionable message, rather than surfacing later as an opaque
    init-container failure or a mid-extract eviction on the node.
    """


def validate_archive(data: bytes, settings: Settings | None = None) -> tuple[str, str]:
    """Validate an archive's bytes. Returns (extension, content_type).

    Raises ``bundles.UnsupportedArchive`` if the bytes are not a zip/tar(.gz),
    ``BundleInvalid`` if the plugin bundle fails validation, and (via
    ``safe_extract``) ``bundles.UnsupportedArchive`` again if the archive
    exceeds the configured uncompressed-size or compression-ratio cap.
    """

    settings = settings or get_settings()
    with tempfile.TemporaryDirectory() as tmp:
        extension, content_type, result = bundles.extract_and_validate(
            data,
            Path(tmp),
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
    if not result.valid:
        raise BundleInvalid([e.model_dump() for e in result.errors])
    return extension, content_type


async def revalidate_stored_bundle(
    store: ObjectStore, version: AgentVersion, settings: Settings | None = None
) -> None:
    """Re-check an already-stored bundle against the CURRENT size/ratio caps.

    A no-op when the version carries no bundle yet. Otherwise fetches the
    immutable bytes and reruns the same pre-scan ``safe_extract`` applies
    (unsafe entries, uncompressed-size and compression-ratio caps) via
    ``plugin_format.check_archive_bounds``, which extracts nothing -- cheap
    enough to run on every deploy/promote. Called before a version becomes
    deployable (``crud.create_deployment_row``'s callers), so a legacy bundle
    that predates these caps, or was stored under looser ones, fails here with
    a clear ``BundleTooLarge`` instead of only surfacing once some sandbox
    substrate tries to fetch and extract it.
    """

    if version.bundle_ref is None:
        return
    settings = settings or get_settings()
    data = await store.get(version.bundle_ref)
    try:
        plugin_format.check_archive_bounds(
            data,
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
    except plugin_format.UnsupportedArchive as exc:
        raise BundleTooLarge(
            f"stored bundle for version {version.id} fails the current bundle "
            f"size/ratio limits and must be rebuilt and re-uploaded: {exc}"
        ) from exc


async def store_bundle(
    store: ObjectStore,
    session: AsyncSession,
    agent_id: uuid.UUID,
    version: AgentVersion,
    data: bytes,
    extension: str,
    content_type: str,
) -> BundleOut:
    """Store validated bytes under the immutable key and record them."""

    key = f"bundles/{agent_id}/{version.id}{extension}"
    digest = hashlib.sha256(data).hexdigest()
    await store.put(key, data, content_type)
    await crud.attach_bundle(session, version, key, digest)
    return BundleOut(
        version_id=version.id,
        bundle_ref=key,
        bundle_sha256=digest,
        size_bytes=len(data),
    )
