"""The worker's UI evidence upload for one stored publication (plan 5.2).

The worker relays the runner's index, then each bound image's bytes, then a
seal. Nothing here reads the code publication beyond its base and lineage, so a
refused upload never changes the pull request. Bodies are read bounded before
parsing: the index to 8 MiB and each image to the size its entry declared.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Awaitable
from typing import NoReturn

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError

from curie_api.crud import ui_evidence as crud_ui_evidence
from curie_api.crud.ui_evidence import EvidenceRefused
from curie_api.schemas.ui_evidence import (
    MAX_INDEX_BYTES,
    UiEvidenceImageOut,
    UiEvidenceIndexIn,
    UiEvidenceIndexOut,
    UiEvidenceSealOut,
)
from curie_api.wirebody import read_bounded_body

from ..auth import require_internal_worker_token
from ..deps import SessionDep

_NO_STORE = {"Cache-Control": "no-store"}
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


def _no_store(response: Response) -> None:
    response.headers.update(_NO_STORE)


router = APIRouter(
    prefix="/v1/internal/publications",
    tags=["internal-publications"],
    dependencies=[Depends(require_internal_worker_token), Depends(_no_store)],
)


def _refuse(status_code: int, code: str, message: str) -> NoReturn:
    raise HTTPException(
        status_code, {"code": f"ui_evidence.{code}", "message": message}, headers=_NO_STORE
    )


async def _stored[T](call: Awaitable[T]) -> T:
    """Await a crud call, answering its refusal or a storage failure."""

    try:
        return await call
    except EvidenceRefused as refusal:
        raise HTTPException(
            refusal.status, {"code": refusal.code, "message": refusal.message}, headers=_NO_STORE
        ) from None
    except SQLAlchemyError:
        _refuse(
            status.HTTP_503_SERVICE_UNAVAILABLE, "storage_unavailable", "evidence storage failed"
        )


@router.put(
    "/{publication_id}/ui-evidence",
    response_model=UiEvidenceIndexOut,
    status_code=status.HTTP_201_CREATED,
)
async def put_publication_ui_evidence(
    publication_id: uuid.UUID, request: Request, session: SessionDep, response: Response
) -> UiEvidenceIndexOut:
    """Store the evidence index once; an identical replay answers 200."""

    raw = await read_bounded_body(request, MAX_INDEX_BYTES, subject="evidence index body")
    try:
        index = UiEvidenceIndexIn.model_validate(json.loads(raw))
    except (ValueError, RecursionError, ValidationError):
        _refuse(
            status.HTTP_422_UNPROCESSABLE_ENTITY, "invalid_index", "the evidence index is invalid"
        )
    stored = await _stored(crud_ui_evidence.store_index(session, publication_id, index))
    if not stored.created:
        response.status_code = status.HTTP_200_OK
    return UiEvidenceIndexOut(status=stored.status, expected=stored.expected)


@router.put(
    "/{publication_id}/ui-evidence/images/{ordinal}",
    response_model=UiEvidenceImageOut,
    status_code=status.HTTP_201_CREATED,
)
async def put_publication_ui_evidence_image(
    publication_id: uuid.UUID,
    ordinal: int,
    request: Request,
    session: SessionDep,
    response: Response,
    digest: str = Header(alias="X-Curie-Evidence-Sha256", min_length=64, max_length=64),
) -> UiEvidenceImageOut:
    """Store one expected image whose bytes match its entry exactly."""

    expected = await _stored(crud_ui_evidence.expected_image(session, publication_id, ordinal))
    if digest.lower() != expected.sha256:
        _refuse(status.HTTP_409_CONFLICT, "digest_mismatch", "the digest is not the entry's")
    try:
        content = await read_bounded_body(request, expected.size, subject="evidence image body")
    except HTTPException:
        # Longer than its entry: not the entry's bytes.
        _refuse(status.HTTP_409_CONFLICT, "digest_mismatch", "the bytes are not the entry's")
    if len(content) != expected.size or hashlib.sha256(content).hexdigest() != expected.sha256:
        _refuse(status.HTTP_409_CONFLICT, "digest_mismatch", "the bytes are not the entry's")
    if not content.startswith(_PNG_SIGNATURE):
        _refuse(status.HTTP_409_CONFLICT, "not_png", "the image is not a PNG")
    created = await _stored(crud_ui_evidence.store_image(session, publication_id, ordinal, content))
    if not created:
        response.status_code = status.HTTP_200_OK
    return UiEvidenceImageOut(ordinal=ordinal, stored=True)


@router.post("/{publication_id}/ui-evidence/seal", response_model=UiEvidenceSealOut)
async def seal_publication_ui_evidence(
    publication_id: uuid.UUID, request: Request, session: SessionDep
) -> UiEvidenceSealOut:
    """Close the upload; the reconciler may publish it from now on."""

    # The body is ``{}``; it carries nothing, so it is read only to be bounded.
    await read_bounded_body(request, 1024, subject="evidence seal body")
    sealed, stored, missing = await _stored(crud_ui_evidence.seal(session, publication_id))
    return UiEvidenceSealOut(status=sealed, stored=stored, missing=missing)
