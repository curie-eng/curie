"""Database access for publication UI evidence (migration 0146).

Uploads store one index per publication, then each expected image, then a
seal. The reconciler claims rows and lineages with leases and CAS writes, and
image bytes are cleared on every terminal row. Nothing here writes a
publication or a lineage row.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import ColumnElement, Update, and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import (
    UI_EVIDENCE_TERMINAL,
    Publication,
    PublicationLineageUiEvidence,
    PublicationUiEvidence,
    PublicationUiEvidenceImage,
    ThreadPublicationLineage,
)
from ..schemas.ui_evidence import MAX_TOTAL_BYTES, UiEvidenceImageIn, UiEvidenceIndexIn

UPLOAD_WINDOW = timedelta(seconds=60)
PUBLISH_WINDOW = timedelta(seconds=120)
LEASE = timedelta(seconds=60)
_REFUSED_PUBLICATION = frozenset({"denied", "expired", "failed"})
_NON_TERMINAL = ("receiving", "ready", "publishing")
# Index fields stored under their own name on the evidence row.
_INDEX_COLUMNS = {
    *("manifest_state", "manifest_refusal", "reason", "base_sha", "manifest_base_sha"),
    *("manifest_head_sha", "manifest_candidate_tree", "candidate_tree", "head_tree"),
    *("candidate_dirty", "warnings"),
}


class EvidenceRefused(Exception):
    """An upload the API will not take: an HTTP status and a fixed code."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.code = f"ui_evidence.{code}"
        self.message = message


@dataclass(frozen=True)
class IndexStored:
    created: bool
    status: str
    expected: list[int]


@dataclass(frozen=True)
class ExpectedImage:
    size: int
    sha256: str


def bound_by_rule(image: UiEvidenceImageIn, index: UiEvidenceIndexIn, base_sha: str) -> bool:
    """Decision 2, against the publication's own base: a ``before`` image binds
    only to that base, an ``after`` image only to the run's candidate tree (by
    the manifest's tree or, for a clean candidate, by its head's tree)."""

    if image.state == "before":
        return index.manifest_base_sha is not None and index.manifest_base_sha == base_sha
    if image.state != "after" or index.candidate_tree is None:
        return False
    if index.manifest_candidate_tree == index.candidate_tree:
        return True
    return index.candidate_dirty is False and index.head_tree == index.candidate_tree


def index_digest(index: UiEvidenceIndexIn) -> str:
    canonical = json.dumps(
        index.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def _entry_row(
    publication_id: uuid.UUID, image: UiEvidenceImageIn, index: UiEvidenceIndexIn, base_sha: str
) -> PublicationUiEvidenceImage:
    refusal = image.refusal
    bound = image.bound and refusal is None and bound_by_rule(image, index, base_sha)
    if refusal is None and not bound:
        refusal = "unbound"
    if refusal is None and (image.size is None or image.sha256 is None):
        refusal, bound = "invalid_entry", False
    return PublicationUiEvidenceImage(
        publication_id=publication_id,
        ordinal=image.ordinal,
        path=image.path or "",
        label=image.label or "",
        state=image.state,
        size_bytes=image.size,
        sha256=image.sha256,
        refusal=refusal,
        bound=bound,
    )


async def _expected_images(
    session: AsyncSession, publication_id: uuid.UUID
) -> list[tuple[int, datetime | None]]:
    rows = await session.execute(
        select(PublicationUiEvidenceImage.ordinal, PublicationUiEvidenceImage.stored_at)
        .where(
            PublicationUiEvidenceImage.publication_id == publication_id,
            PublicationUiEvidenceImage.refusal.is_(None),
            PublicationUiEvidenceImage.bound.is_(True),
        )
        .order_by(PublicationUiEvidenceImage.ordinal)
    )
    return [(ordinal, stored_at) for ordinal, stored_at in rows]


async def _existing(session: AsyncSession, publication_id: uuid.UUID, digest: str) -> IndexStored:
    row = await session.get(PublicationUiEvidence, publication_id)
    assert row is not None
    if row.index_sha256 != digest:
        raise EvidenceRefused(409, "index_conflict", "a different evidence index is stored")
    expected = [ordinal for ordinal, _ in await _expected_images(session, publication_id)]
    return IndexStored(False, row.status, expected)


async def store_index(
    session: AsyncSession, publication_id: uuid.UUID, index: UiEvidenceIndexIn
) -> IndexStored:
    """Store the first index for a publication; a replay of it is accepted and
    a different one is refused, so the first index stands."""

    publication = await session.get(Publication, publication_id)
    if publication is None:
        raise EvidenceRefused(404, "publication_not_found", "publication not found")
    if publication.status in _REFUSED_PUBLICATION or publication.lineage_id is None:
        raise EvidenceRefused(409, "publication_terminal", "the publication will not be published")
    if index.base_sha != publication.base_sha:
        raise EvidenceRefused(409, "base_mismatch", "the evidence names another base")
    digest = index_digest(index)
    if await session.get(PublicationUiEvidence, publication_id) is not None:
        return await _existing(session, publication_id, digest)
    images = [
        _entry_row(publication_id, image, index, publication.base_sha) for image in index.images
    ]
    expected = [image for image in images if image.refusal is None]
    if sum(image.size_bytes or 0 for image in expected) > MAX_TOTAL_BYTES:
        raise EvidenceRefused(422, "total_limit", "the evidence passes 100 MiB in total")
    ordinals = sorted(image.ordinal for image in expected)
    try:
        session.add(
            PublicationUiEvidence(
                publication_id=publication_id,
                lineage_id=publication.lineage_id,
                status="receiving",
                index_sha256=digest,
                manifest_status=index.status,
                upload_deadline_at=func.now() + UPLOAD_WINDOW,
                **index.model_dump(include=_INDEX_COLUMNS),
            )
        )
        await session.flush()
        session.add_all(images)
        await session.execute(
            insert(PublicationLineageUiEvidence)
            .values(lineage_id=publication.lineage_id)
            .on_conflict_do_nothing(index_elements=["lineage_id"])
        )
        await session.commit()
    except IntegrityError:
        # A concurrent upload of the same publication won the insert.
        await session.rollback()
        return await _existing(session, publication_id, digest)
    return IndexStored(True, "receiving", ordinals)


async def expected_image(
    session: AsyncSession, publication_id: uuid.UUID, ordinal: int
) -> ExpectedImage:
    """The entry an image upload must match, or the refusal for it."""

    image = await session.get(PublicationUiEvidenceImage, (publication_id, ordinal))
    if image is None:
        raise EvidenceRefused(404, "image_not_found", "no such evidence image")
    if image.refusal is not None or not image.bound or image.size_bytes is None or not image.sha256:
        raise EvidenceRefused(409, "image_not_expected", "this image is not expected")
    return ExpectedImage(image.size_bytes, image.sha256)


async def _locked_row(
    session: AsyncSession, publication_id: uuid.UUID
) -> PublicationUiEvidence | None:
    # The row lock serializes image stores with the seal.
    row: PublicationUiEvidence | None = await session.scalar(
        select(PublicationUiEvidence)
        .where(PublicationUiEvidence.publication_id == publication_id)
        .with_for_update()
    )
    return row


async def store_image(
    session: AsyncSession, publication_id: uuid.UUID, ordinal: int, content: bytes
) -> bool:
    """Store one verified image: True when stored now, False when it already was."""

    row = await _locked_row(session, publication_id)
    image = await session.get(PublicationUiEvidenceImage, (publication_id, ordinal))
    if row is None or image is None:
        raise EvidenceRefused(404, "image_not_found", "no such evidence image")
    if image.stored_at is not None:
        await session.rollback()
        return False
    if row.status == "ready":
        raise EvidenceRefused(409, "sealed", "the evidence upload is sealed")
    if row.status != "receiving":
        raise EvidenceRefused(409, "not_receiving", "the evidence upload is closed")
    image.content = content
    image.stored_at = func.now()
    row.total_bytes = PublicationUiEvidence.total_bytes + len(content)
    row.version = PublicationUiEvidence.version + 1
    row.updated_at = func.now()
    await session.commit()
    return True


async def seal(
    session: AsyncSession, publication_id: uuid.UUID
) -> tuple[str, list[int], list[int]]:
    """Move ``receiving`` to ``ready``; any later state answers as it is."""

    row = await _locked_row(session, publication_id)
    if row is None:
        raise EvidenceRefused(404, "evidence_not_found", "no evidence index for the publication")
    if row.status == "receiving":
        row.status = "ready"
        row.version = PublicationUiEvidence.version + 1
        row.updated_at = func.now()
    status = row.status
    images = await _expected_images(session, publication_id)
    await session.commit()
    stored = [ordinal for ordinal, at in images if at is not None]
    missing = [ordinal for ordinal, at in images if at is None]
    return status, stored, missing


# --- reconciler ------------------------------------------------------------------


def _terminal_values(status: str, **values: object) -> dict[str, object]:
    return {
        "status": status,
        "terminal_at": func.now(),
        "updated_at": func.now(),
        "lease_owner": None,
        "lease_expires_at": None,
        "version": PublicationUiEvidence.version + 1,
        **values,
    }


async def sweep(session: AsyncSession) -> None:
    """Close rows that can no longer publish and clear every terminal row's bytes.

    A row whose publication was refused, or whose lineage ended, is abandoned;
    a row whose publication is no longer the lineage's latest revision is
    superseded.
    """

    async def close(status: str, *where: ColumnElement[bool], **values: object) -> None:
        await session.execute(
            update(PublicationUiEvidence)
            .where(PublicationUiEvidence.status.in_(_NON_TERMINAL), *where)
            .values(**_terminal_values(status, **values))
            .execution_options(synchronize_session=False)
        )

    await close(
        "abandoned",
        PublicationUiEvidence.publication_id == Publication.id,
        Publication.status.in_(_REFUSED_PUBLICATION),
        error="publication_not_published",
    )
    await close(
        "abandoned",
        PublicationUiEvidence.lineage_id == ThreadPublicationLineage.id,
        ThreadPublicationLineage.status != "open",
        error="lineage_ended",
    )
    await close(
        "superseded",
        PublicationUiEvidence.publication_id == Publication.id,
        Publication.lineage_id == ThreadPublicationLineage.id,
        Publication.status == "succeeded",
        ThreadPublicationLineage.latest_revision != Publication.revision_number,
    )
    await session.execute(
        update(PublicationUiEvidenceImage)
        .where(
            PublicationUiEvidenceImage.publication_id == PublicationUiEvidence.publication_id,
            PublicationUiEvidence.status.in_(UI_EVIDENCE_TERMINAL),
            PublicationUiEvidenceImage.content.is_not(None),
        )
        .values(content=None)
        .execution_options(synchronize_session=False)
    )
    await session.commit()


def publish_ready() -> ColumnElement[bool]:
    """The rows with a publish obligation (plan 5.6 item 3.1)."""

    now = func.now()
    return and_(
        or_(
            PublicationUiEvidence.status == "ready",
            and_(
                PublicationUiEvidence.status == "receiving",
                PublicationUiEvidence.upload_deadline_at < now,
            ),
            and_(
                PublicationUiEvidence.status == "publishing",
                or_(
                    PublicationUiEvidence.lease_expires_at.is_(None),
                    PublicationUiEvidence.lease_expires_at < now,
                ),
            ),
        ),
        Publication.id == PublicationUiEvidence.publication_id,
        ThreadPublicationLineage.id == Publication.lineage_id,
        Publication.status == "succeeded",
        Publication.result_url == ThreadPublicationLineage.pr_url,
        ThreadPublicationLineage.status == "open",
        ThreadPublicationLineage.pr_number.is_not(None),
        ThreadPublicationLineage.head_sha.is_not(None),
        ThreadPublicationLineage.latest_revision == Publication.revision_number,
    )


async def claim_lineage(session: AsyncSession, lineage_id: uuid.UUID, owner: str) -> int | None:
    """Take the lineage's evidence lease, creating its row; the new version, or
    None while another live owner holds it."""

    await session.execute(
        insert(PublicationLineageUiEvidence)
        .values(lineage_id=lineage_id)
        .on_conflict_do_nothing(index_elements=["lineage_id"])
    )
    version = await session.scalar(
        update(PublicationLineageUiEvidence)
        .where(
            PublicationLineageUiEvidence.lineage_id == lineage_id,
            or_(
                PublicationLineageUiEvidence.lease_expires_at.is_(None),
                PublicationLineageUiEvidence.lease_expires_at < func.now(),
                PublicationLineageUiEvidence.lease_owner == owner,
            ),
        )
        .values(
            lease_owner=owner,
            lease_expires_at=func.now() + LEASE,
            version=PublicationLineageUiEvidence.version + 1,
            updated_at=func.now(),
        )
        .returning(PublicationLineageUiEvidence.version)
    )
    await session.commit()
    return version


async def update_lineage(
    session: AsyncSession,
    lineage_id: uuid.UUID,
    owner: str,
    version: int,
    *,
    renew: bool = True,
    **values: object,
) -> int | None:
    """One CAS write to the lineage's evidence row under its lease; renews the
    lease unless released. The new version, or None when the lease was lost."""

    changed = await _cas_lineage(session, lineage_id, owner, version, renew=renew, **values)
    await session.commit()
    return changed


async def _cas_lineage(
    session: AsyncSession,
    lineage_id: uuid.UUID,
    owner: str,
    version: int,
    *,
    renew: bool,
    **values: object,
) -> int | None:
    lease: dict[str, object] = (
        {"lease_expires_at": func.now() + LEASE}
        if renew
        else {"lease_owner": None, "lease_expires_at": None}
    )
    changed = await session.scalar(
        update(PublicationLineageUiEvidence)
        .where(
            PublicationLineageUiEvidence.lineage_id == lineage_id,
            PublicationLineageUiEvidence.version == version,
            PublicationLineageUiEvidence.lease_owner == owner,
        )
        .values(
            version=PublicationLineageUiEvidence.version + 1,
            updated_at=func.now(),
            **lease,
            **values,
        )
        .returning(PublicationLineageUiEvidence.version)
    )
    return changed


def _row_cas(publication_id: uuid.UUID, version: int) -> Update:
    return update(PublicationUiEvidence).where(
        PublicationUiEvidence.publication_id == publication_id,
        PublicationUiEvidence.version == version,
        PublicationUiEvidence.status.not_in(UI_EVIDENCE_TERMINAL),
    )


async def update_row(
    session: AsyncSession, publication_id: uuid.UUID, version: int, **values: object
) -> int | None:
    """One CAS write to an evidence row; the new version, or None when it moved."""

    changed = await session.scalar(
        _row_cas(publication_id, version)
        .values(version=PublicationUiEvidence.version + 1, updated_at=func.now(), **values)
        .returning(PublicationUiEvidence.version)
    )
    await session.commit()
    return changed


async def finish_row(
    session: AsyncSession, publication_id: uuid.UUID, version: int, status: str, **values: object
) -> bool:
    """Settle a row terminal by CAS and clear its image bytes in the same commit."""

    if not await _settle_row(session, publication_id, version, status, **values):
        await session.rollback()
        return False
    await session.commit()
    return True


async def finish_row_with_comment(
    session: AsyncSession,
    publication_id: uuid.UUID,
    version: int,
    status: str,
    lineage: tuple[uuid.UUID, str, int],
    lineage_values: dict[str, object],
    **values: object,
) -> int | None:
    """Record a written comment on the lineage (``lineage`` is its id, lease
    owner and version) and settle the row terminal in one commit, so a crash
    never leaves one recorded without the other. The lineage's new version, or
    None when either CAS lost."""

    lineage_id, owner, lineage_version = lineage
    changed = await _cas_lineage(
        session, lineage_id, owner, lineage_version, renew=True, **lineage_values
    )
    if changed is None or not await _settle_row(session, publication_id, version, status, **values):
        await session.rollback()
        return None
    await session.commit()
    return changed


async def _settle_row(
    session: AsyncSession, publication_id: uuid.UUID, version: int, status: str, **values: object
) -> bool:
    changed = await session.scalar(
        _row_cas(publication_id, version)
        .values(**_terminal_values(status, **values))
        .returning(PublicationUiEvidence.version)
    )
    if changed is None:
        return False
    await session.execute(
        update(PublicationUiEvidenceImage)
        .where(PublicationUiEvidenceImage.publication_id == publication_id)
        .values(content=None)
    )
    return True


async def claim_row(
    session: AsyncSession, publication_id: uuid.UUID, version: int, owner: str
) -> tuple[int, float] | None:
    """Move the row to ``publishing`` under ``owner``: its new version and the
    seconds left in its publish window, which starts at the first claim."""

    claimed = (
        await session.execute(
            _row_cas(publication_id, version)
            .values(
                status="publishing",
                attempts=PublicationUiEvidence.attempts + 1,
                publish_deadline_at=func.coalesce(
                    PublicationUiEvidence.publish_deadline_at, func.now() + PUBLISH_WINDOW
                ),
                lease_owner=owner,
                lease_expires_at=func.now() + LEASE,
                version=PublicationUiEvidence.version + 1,
                updated_at=func.now(),
            )
            .returning(
                PublicationUiEvidence.version,
                func.extract("epoch", PublicationUiEvidence.publish_deadline_at - func.now()),
            )
        )
    ).first()
    await session.commit()
    return None if claimed is None else (int(claimed[0]), float(claimed[1]))
