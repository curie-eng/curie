"""Database access for publication UI evidence (migration 0146).

The upload half stores what the worker relays: one index per publication,
then each expected image, then a seal. The reconciler half claims rows and
lineages with leases and compare-and-set writes, and clears image bytes on
every terminal row. Nothing here writes a publication or a lineage row.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import ColumnElement, and_, func, or_, select, update
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
    stored: bool


# --- the binding rule ----------------------------------------------------------


def bound_by_rule(image: UiEvidenceImageIn, index: UiEvidenceIndexIn, base_sha: str) -> bool:
    """Decision 2, as the API applies it against the publication's own base.

    A ``before`` image is bound only to the publication base. An ``after``
    image is bound only to the candidate tree the run computed, by the
    manifest's tree or, for a clean candidate, by the tree of its head.
    """

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
) -> dict[str, object]:
    refusal = image.refusal
    bound = image.bound and refusal is None and bound_by_rule(image, index, base_sha)
    if refusal is None and not bound:
        refusal = "unbound"
    if refusal is None and (image.size is None or image.sha256 is None):
        refusal, bound = "invalid_entry", False
    return {
        "publication_id": publication_id,
        "ordinal": image.ordinal,
        "path": image.path or "",
        "label": image.label or "",
        "state": image.state,
        "size_bytes": image.size,
        "sha256": image.sha256,
        "refusal": refusal,
        "bound": bound,
    }


def _expected(rows: list[dict[str, object]]) -> list[int]:
    return sorted(int(str(row["ordinal"])) for row in rows if row["refusal"] is None)


# --- upload ----------------------------------------------------------------------


async def _expected_ordinals(session: AsyncSession, publication_id: uuid.UUID) -> list[int]:
    rows = await session.scalars(
        select(PublicationUiEvidenceImage.ordinal)
        .where(
            PublicationUiEvidenceImage.publication_id == publication_id,
            PublicationUiEvidenceImage.refusal.is_(None),
            PublicationUiEvidenceImage.bound.is_(True),
        )
        .order_by(PublicationUiEvidenceImage.ordinal)
    )
    return list(rows)


async def _existing(session: AsyncSession, publication_id: uuid.UUID, digest: str) -> IndexStored:
    row = await session.get(PublicationUiEvidence, publication_id)
    assert row is not None
    if row.index_sha256 != digest:
        raise EvidenceRefused(409, "index_conflict", "a different evidence index is stored")
    return IndexStored(False, row.status, await _expected_ordinals(session, publication_id))


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
    rows = [
        _entry_row(publication_id, image, index, publication.base_sha) for image in index.images
    ]
    expected_bytes = sum(int(str(row["size_bytes"])) for row in rows if row["refusal"] is None)
    if expected_bytes > MAX_TOTAL_BYTES:
        raise EvidenceRefused(422, "total_limit", "the evidence passes 100 MiB in total")
    lineage_id = publication.lineage_id
    try:
        session.add(
            PublicationUiEvidence(
                publication_id=publication_id,
                lineage_id=lineage_id,
                status="receiving",
                index_sha256=digest,
                manifest_state=index.manifest_state,
                manifest_refusal=index.manifest_refusal,
                manifest_status=index.status,
                reason=index.reason,
                base_sha=index.base_sha,
                manifest_base_sha=index.manifest_base_sha,
                manifest_head_sha=index.manifest_head_sha,
                manifest_candidate_tree=index.manifest_candidate_tree,
                candidate_tree=index.candidate_tree,
                head_tree=index.head_tree,
                candidate_dirty=index.candidate_dirty,
                warnings=list(index.warnings),
                upload_deadline_at=func.now() + UPLOAD_WINDOW,
            )
        )
        await session.flush()
        for row in rows:
            session.add(PublicationUiEvidenceImage(**row))
        await session.execute(
            insert(PublicationLineageUiEvidence)
            .values(lineage_id=lineage_id)
            .on_conflict_do_nothing(index_elements=["lineage_id"])
        )
        await session.commit()
    except IntegrityError:
        # A concurrent upload of the same publication won the insert.
        await session.rollback()
        return await _existing(session, publication_id, digest)
    return IndexStored(True, "receiving", _expected(rows))


async def expected_image(
    session: AsyncSession, publication_id: uuid.UUID, ordinal: int
) -> ExpectedImage:
    """The entry an image upload must match, or the refusal for it."""

    image = await session.get(PublicationUiEvidenceImage, (publication_id, ordinal))
    if image is None:
        raise EvidenceRefused(404, "image_not_found", "no such evidence image")
    if image.refusal is not None or not image.bound or image.size_bytes is None or not image.sha256:
        raise EvidenceRefused(409, "image_not_expected", "this image is not expected")
    return ExpectedImage(image.size_bytes, image.sha256, image.stored_at is not None)


async def store_image(
    session: AsyncSession, publication_id: uuid.UUID, ordinal: int, content: bytes
) -> bool:
    """Store one verified image. Returns True when stored now, False when the
    same bytes were already stored. Serialized with the seal by a row lock."""

    row = await session.scalar(
        select(PublicationUiEvidence)
        .where(PublicationUiEvidence.publication_id == publication_id)
        .with_for_update()
    )
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

    row = await session.scalar(
        select(PublicationUiEvidence)
        .where(PublicationUiEvidence.publication_id == publication_id)
        .with_for_update()
    )
    if row is None:
        raise EvidenceRefused(404, "evidence_not_found", "no evidence index for the publication")
    if row.status == "receiving":
        row.status = "ready"
        row.version = PublicationUiEvidence.version + 1
        row.updated_at = func.now()
    status = row.status
    images = list(
        await session.execute(
            select(PublicationUiEvidenceImage.ordinal, PublicationUiEvidenceImage.stored_at)
            .where(
                PublicationUiEvidenceImage.publication_id == publication_id,
                PublicationUiEvidenceImage.refusal.is_(None),
                PublicationUiEvidenceImage.bound.is_(True),
            )
            .order_by(PublicationUiEvidenceImage.ordinal)
        )
    )
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

    Each step is one statement. A row whose publication was refused, or whose
    lineage ended, is abandoned; a row whose publication is no longer the
    lineage's latest revision is superseded.
    """

    open_rows = PublicationUiEvidence.status.in_(_NON_TERMINAL)
    await session.execute(
        update(PublicationUiEvidence)
        .where(
            open_rows,
            PublicationUiEvidence.publication_id == Publication.id,
            Publication.status.in_(_REFUSED_PUBLICATION),
        )
        .values(**_terminal_values("abandoned", error="publication_not_published"))
        .execution_options(synchronize_session=False)
    )
    await session.execute(
        update(PublicationUiEvidence)
        .where(
            open_rows,
            PublicationUiEvidence.lineage_id == ThreadPublicationLineage.id,
            ThreadPublicationLineage.status != "open",
        )
        .values(**_terminal_values("abandoned", error="lineage_ended"))
        .execution_options(synchronize_session=False)
    )
    await session.execute(
        update(PublicationUiEvidence)
        .where(
            open_rows,
            PublicationUiEvidence.publication_id == Publication.id,
            Publication.lineage_id == ThreadPublicationLineage.id,
            Publication.status == "succeeded",
            ThreadPublicationLineage.latest_revision != Publication.revision_number,
        )
        .values(**_terminal_values("superseded"))
        .execution_options(synchronize_session=False)
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
    await session.commit()
    return changed


async def update_row(
    session: AsyncSession,
    publication_id: uuid.UUID,
    version: int,
    **values: object,
) -> int | None:
    """One CAS write to an evidence row; the new version, or None when it moved."""

    changed = await session.scalar(
        update(PublicationUiEvidence)
        .where(
            PublicationUiEvidence.publication_id == publication_id,
            PublicationUiEvidence.version == version,
            PublicationUiEvidence.status.not_in(UI_EVIDENCE_TERMINAL),
        )
        .values(version=PublicationUiEvidence.version + 1, updated_at=func.now(), **values)
        .returning(PublicationUiEvidence.version)
    )
    await session.commit()
    return changed


async def finish_row(
    session: AsyncSession, publication_id: uuid.UUID, version: int, status: str, **values: object
) -> bool:
    """Settle a row terminal by CAS and clear its image bytes in the same commit."""

    changed = await session.scalar(
        update(PublicationUiEvidence)
        .where(
            PublicationUiEvidence.publication_id == publication_id,
            PublicationUiEvidence.version == version,
            PublicationUiEvidence.status.not_in(UI_EVIDENCE_TERMINAL),
        )
        .values(**_terminal_values(status, **values))
        .returning(PublicationUiEvidence.version)
    )
    if changed is None:
        await session.rollback()
        return False
    await session.execute(
        update(PublicationUiEvidenceImage)
        .where(PublicationUiEvidenceImage.publication_id == publication_id)
        .values(content=None)
    )
    await session.commit()
    return True
