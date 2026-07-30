"""Plugin bundle upload and fetch-by-version.

Upload validates the archive via the frozen plugin_format validator, then stores
the original bytes immutably (write-once per version). Fetch returns those exact
bytes so a runner can pull a bundle by version.
"""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Response, UploadFile, status

from .. import bundles, crud, deploy
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep, StoreDep
from ..models import AgentVersion
from ..schemas import BundleOut

# Chunk size for the bounded read below; arbitrary, just small enough that a
# rejected oversized upload never holds more than one chunk's worth of the
# file in memory at once.
_UPLOAD_READ_CHUNK_BYTES = 1024 * 1024

router = APIRouter(
    prefix="/agents/{agent_id}/versions/{version_id}/bundle",
    tags=["bundles"],
    dependencies=[Depends(require_api_key)],
)

# Stored key extension -> content type, longest suffix first.
_CONTENT_TYPES = (
    (".tar.gz", "application/gzip"),
    (".tar", "application/x-tar"),
    (".zip", "application/zip"),
)


async def _load_version(
    session: SessionDep, agent_id: uuid.UUID, version_id: uuid.UUID
) -> AgentVersion:
    version = await crud.get_version(session, version_id)
    if version is None or version.agent_id != agent_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "version not found")
    return version


def _content_type_for(key: str) -> str:
    for suffix, content_type in _CONTENT_TYPES:
        if key.endswith(suffix):
            return content_type
    return "application/octet-stream"


async def _read_bounded_upload(file: UploadFile, max_bytes: int) -> bytes:
    """Read the uploaded file's bytes, rejecting an oversized upload before it
    is buffered into memory.

    Mirrors ``_read_bounded_body`` (``routers/github.py``): the bound is
    enforced by rejecting the moment the accumulated size crosses it, rather
    than via a single ``await file.read()`` that materializes the whole upload
    into one contiguous bytes object first and only then lets a caller reject
    it -- exactly the unbounded-memory defect this closes (ADR-0059
    decision 3). Starlette's multipart parser tracks the exact number of bytes
    written for this part as ``file.size``, so that is checked first as a fast
    path with no read at all; the chunked loop below is the enforcement when a
    parser leaves ``size`` unset, and is itself bounded (never accumulates past
    ``max_bytes`` before raising). Raises 413 on an oversized upload.
    """

    if file.size is not None:
        if file.size > max_bytes:
            raise HTTPException(
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                "bundle upload exceeds the maximum size",
            )
        return await file.read()

    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = await file.read(_UPLOAD_READ_CHUNK_BYTES)
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise HTTPException(
                status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                "bundle upload exceeds the maximum size",
            )
        chunks.append(chunk)
    return b"".join(chunks)


@router.put("", response_model=BundleOut, status_code=status.HTTP_201_CREATED)
async def upload_bundle(
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    session: SessionDep,
    store: StoreDep,
    file: UploadFile,
) -> BundleOut:
    # Enforced before touching the DB or anything else: an oversized upload is
    # rejected on the size gate alone, not after a wasted version lookup.
    data = await _read_bounded_upload(file, get_settings().bundle_upload_max_bytes)

    version = await _load_version(session, agent_id, version_id)
    if version.bundle_ref is not None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "bundle already stored for this version (bundles are immutable)",
        )

    try:
        extension, content_type = deploy.validate_archive(data)
    except bundles.UnsupportedArchive as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    except deploy.BundleInvalid as exc:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            {"detail": "bundle failed validation", "errors": exc.errors},
        ) from exc

    return await deploy.store_bundle(
        store, session, agent_id, version, data, extension, content_type
    )


@router.get("")
async def download_bundle(
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    session: SessionDep,
    store: StoreDep,
) -> Response:
    version = await _load_version(session, agent_id, version_id)
    if version.bundle_ref is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no bundle stored for this version")
    data = await store.get(version.bundle_ref)
    return Response(content=data, media_type=_content_type_for(version.bundle_ref))
