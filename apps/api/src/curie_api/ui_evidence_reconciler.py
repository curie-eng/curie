"""Publish a publication's optional UI evidence as one pull request comment.

The worker uploads screenshots after the publication exists
(`curie_api.routers.publication_ui_evidence`). This loop publishes them only
once the code result is recorded: the publication ``succeeded`` with the
lineage's pull request and is still the lineage's latest revision. It only
SELECTs publications and lineages and never edits a pull request, so evidence
can never change, retry or fail the code publication.

Each pass:

1. Sweeps rows that can no longer publish (refused publication, ended lineage,
   superseded revision) and clears every terminal row's image bytes.
2. Takes a lease per lineage (compare-and-set on its evidence row), so one
   process at a time writes a lineage's ref and comment.
3. Publishes a ready row within its 120 s publish window: it confirms the pull
   request head's tree is the captured candidate before an ``after`` image is
   shown, stores the shown images through `CodeHost.store_evidence`, and
   upserts the one marked comment through `MarkedComments.upsert_marked`.
4. Rewrites the comment when a later revision has no evidence, and deletes
   the evidence ref when the lineage merges or closes.

Every phase is idempotent: the evidence commit is content addressed with a
fixed author and date, the ref is force-moved to the same commit, and the
comment is found by its marker. A process that dies keeps its lease until it
lapses, and the next pass resumes the row.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

import httpx
from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.crud import ui_evidence as crud
from curie_api.factory_rediscovery import publishing_code_host
from curie_api.forges.capabilities import Operation, supports
from curie_api.forges.errors import ForgeError, Unavailable, Unsupported
from curie_api.forges.github.code_host import PATH_ID_PREFIX
from curie_api.forges.ports import CodeHost, MarkedComments
from curie_api.forges.types import EvidenceFile, PullRequestRef, ReplyTarget, RepositoryRef

from .config import Settings
from .models import (
    UI_EVIDENCE_TERMINAL,
    Publication,
    PublicationLineageUiEvidence,
    PublicationUiEvidence,
    PublicationUiEvidenceImage,
    ThreadPublicationLineage,
)
from .repository_access import lineage_repository
from .ui_evidence_comment import (
    Entry,
    render_evidence,
    render_stale,
    render_unsupported,
    ui_evidence_marker,
)

logger = logging.getLogger(__name__)

RATE_LIMITED = "github_rate_limited"
# The code host cannot tell its own comment apart, so nothing is written.
OWNERSHIP_UNAVAILABLE = "comment_ownership_unavailable"
# The worker's upload window (60 s) plus a margin, before a revision with no
# evidence is taken to have none.
NO_EVIDENCE_AFTER_S = 90
# The last seconds of the publish window are kept for the outcome comment.
_COMMENT_RESERVE_S = 10.0
_LEASE_MARGIN_S = 5.0
_PHASE_CAP_S = float(crud.LEASE.total_seconds()) - _LEASE_MARGIN_S


class _LeaseLost(Exception):
    """Another owner holds the lineage or row now; stop without writing."""


@dataclass
class PassResult:
    published: int = 0
    rewritten: int = 0
    deleted: int = 0
    skipped: int = 0
    rate_limited: bool = False


@dataclass(frozen=True)
class _Work:
    lineage_id: uuid.UUID
    kind: str
    publication_id: uuid.UUID | None = None


@dataclass
class _Lease:
    lineage_id: uuid.UUID
    version: int


@dataclass(frozen=True)
class _Image:
    entry: Entry
    refusal: str | None
    bound: bool
    sha256: str | None
    content: bytes | None
    stored: bool
    url: str | None


@dataclass
class _Publish:
    publication_id: uuid.UUID
    lineage_id: uuid.UUID
    version: int
    status: str
    manifest_state: str
    manifest_status: str | None
    reason: str | None
    base_sha: str
    candidate_tree: str | None
    evidence_commit_sha: str | None
    comment_written: bool
    revision: int
    head_sha: str
    pr_number: int
    stamp: datetime
    repository: RepositoryRef
    ref_commit_sha: str | None
    images: list[_Image] = field(default_factory=list)


@dataclass(frozen=True)
class _Lineage:
    lineage_id: uuid.UUID
    status: str
    head_sha: str | None
    pr_number: int | None
    revision: int
    repository: RepositoryRef
    ref_commit_sha: str | None
    failure: str | None


class UiEvidenceReconciler:
    """Publish uploaded UI evidence and keep each pull request's comment current."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        settings: Settings,
        client: httpx.AsyncClient,
        *,
        interval_seconds: float,
        batch_limit: int = 20,
        code_host_for: Callable[[RepositoryRef], CodeHost] | None = None,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("the UI evidence reconcile interval must be positive")
        if batch_limit <= 0:
            raise ValueError("the UI evidence reconcile batch limit must be positive")
        self._sessionmaker = sessionmaker
        self._settings = settings
        self._interval = interval_seconds
        self._batch_limit = batch_limit
        self._code_host_for = code_host_for or (
            lambda repository: publishing_code_host(settings, client, repository)
        )
        # One lease owner per reconciler instance (one process).
        self._owner = f"ui-evidence-{uuid.uuid4().hex}"

    # Selection -----------------------------------------------------------------

    async def _work(self) -> list[_Work]:
        async with self._sessionmaker() as session:
            await crud.sweep(session)
            publish = list(
                await session.execute(
                    select(PublicationUiEvidence.lineage_id, PublicationUiEvidence.publication_id)
                    .where(crud.publish_ready())
                    .order_by(PublicationUiEvidence.created_at)
                    .limit(self._batch_limit)
                )
            )
            latest = (
                select(Publication.id)
                .where(
                    Publication.lineage_id == ThreadPublicationLineage.id,
                    Publication.revision_number == ThreadPublicationLineage.latest_revision,
                    Publication.status == "succeeded",
                    Publication.result_url == ThreadPublicationLineage.pr_url,
                )
                .correlate(ThreadPublicationLineage)
            )
            no_evidence = latest.where(
                Publication.created_at < func.now() - timedelta(seconds=NO_EVIDENCE_AFTER_S),
                ~select(PublicationUiEvidence.publication_id)
                .where(PublicationUiEvidence.publication_id == Publication.id)
                .exists(),
            ).exists()
            failed = latest.where(
                select(PublicationUiEvidence.publication_id)
                .where(
                    PublicationUiEvidence.publication_id == Publication.id,
                    PublicationUiEvidence.status == "failed",
                )
                .exists()
            ).exists()
            stale = list(
                await session.scalars(
                    select(ThreadPublicationLineage.id)
                    .join(
                        PublicationLineageUiEvidence,
                        PublicationLineageUiEvidence.lineage_id == ThreadPublicationLineage.id,
                    )
                    .where(
                        ThreadPublicationLineage.status == "open",
                        ThreadPublicationLineage.pr_number.is_not(None),
                        ThreadPublicationLineage.head_sha.is_not(None),
                        PublicationLineageUiEvidence.comment_revision.is_not(None),
                        PublicationLineageUiEvidence.comment_revision
                        < ThreadPublicationLineage.latest_revision,
                        or_(no_evidence, failed),
                    )
                    .order_by(ThreadPublicationLineage.id)
                    .limit(self._batch_limit)
                )
            )
            ended = list(
                await session.scalars(
                    select(ThreadPublicationLineage.id)
                    .join(
                        PublicationLineageUiEvidence,
                        PublicationLineageUiEvidence.lineage_id == ThreadPublicationLineage.id,
                    )
                    .where(
                        ThreadPublicationLineage.status.in_(("merged", "closed")),
                        PublicationLineageUiEvidence.ref_commit_sha.is_not(None),
                    )
                    .order_by(ThreadPublicationLineage.id)
                    .limit(self._batch_limit)
                )
            )
            await session.rollback()
        work: list[_Work] = []
        seen: set[uuid.UUID] = set()
        for lineage_id, publication_id in publish:
            if lineage_id not in seen:
                seen.add(lineage_id)
                work.append(_Work(lineage_id, "publish", publication_id))
        for kind, ids in (("stale", stale), ("ended", ended)):
            for lineage_id in ids:
                if lineage_id not in seen:
                    seen.add(lineage_id)
                    work.append(_Work(lineage_id, kind))
        return work

    # Leases --------------------------------------------------------------------

    async def _write_lineage(self, lease: _Lease, *, renew: bool = True, **values: object) -> None:
        async with self._sessionmaker() as session:
            version = await crud.update_lineage(
                session, lease.lineage_id, self._owner, lease.version, renew=renew, **values
            )
        if version is None:
            raise _LeaseLost
        lease.version = version

    async def _release(self, lease: _Lease) -> None:
        try:
            await self._write_lineage(lease, renew=False)
        except _LeaseLost:
            pass

    async def _write_row(self, state: _Publish, **values: object) -> None:
        async with self._sessionmaker() as session:
            version = await crud.update_row(session, state.publication_id, state.version, **values)
        if version is None:
            raise _LeaseLost
        state.version = version

    async def _finish(self, state: _Publish, status: str, **values: object) -> None:
        async with self._sessionmaker() as session:
            if not await crud.finish_row(
                session, state.publication_id, state.version, status, **values
            ):
                raise _LeaseLost

    # One pass ------------------------------------------------------------------

    async def run_once(self) -> PassResult:
        """One bounded pass over the lineages with evidence work."""

        result = PassResult()
        for work in await self._work():
            async with self._sessionmaker() as session:
                version = await crud.claim_lineage(session, work.lineage_id, self._owner)
            if version is None:
                result.skipped += 1
                continue
            lease = _Lease(work.lineage_id, version)
            try:
                if work.kind == "publish":
                    assert work.publication_id is not None
                    await self._publish(lease, work.publication_id, result)
                elif work.kind == "stale":
                    await self._stale(lease, result)
                else:
                    await self._ended(lease, result)
            except _LeaseLost:
                result.skipped += 1
                continue
            except (ForgeError, TimeoutError) as exc:
                result.skipped += 1
                if isinstance(exc, Unavailable) and str(exc) == RATE_LIMITED:
                    result.rate_limited = True
                logger.warning(
                    "UI evidence for lineage %s not published this pass: %s",
                    work.lineage_id,
                    exc if isinstance(exc, ForgeError) else "timeout",
                )
            except Exception:
                result.skipped += 1
                logger.exception("UI evidence work for lineage %s failed", work.lineage_id)
            await self._release(lease)
            if result.rate_limited:
                break
        return result

    async def run_forever(self) -> None:
        while True:
            # The first pass waits one interval, so a short-lived app never runs one.
            await asyncio.sleep(self._interval)
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - a failed pass must not end the loop
                logger.exception("UI evidence reconcile pass failed")

    # Shared --------------------------------------------------------------------

    def _comments(self, code_host: CodeHost, repository: RepositoryRef) -> MarkedComments:
        scoped = getattr(code_host, "marked_comments_for", None)
        if callable(scoped):
            comments: MarkedComments = scoped(repository)
            return comments
        return code_host.marked_comments

    async def _comment_side(
        self, code_host: CodeHost, repository: RepositoryRef
    ) -> tuple[MarkedComments, RepositoryRef]:
        """The marked comments of ``repository``'s pull requests and the
        repository they address. Reads only; raises `Unsupported` when the code
        host cannot tell its own comment apart (no configured App id)."""

        if repository.project_id.startswith(PATH_ID_PREFIX):
            # A lineage recorded before its repository's immutable id was
            # captured: the comment target needs that id.
            repository = await code_host.resolve_repository(repository.project_id)
        return self._comments(code_host, repository), repository

    async def _upsert(
        self,
        side: tuple[MarkedComments, RepositoryRef],
        lineage_id: uuid.UUID,
        pr_number: int,
        body: str,
    ) -> None:
        comments, repository = side
        target = ReplyTarget(
            "pull_request", pull_request=PullRequestRef(repository, str(pr_number))
        )
        await comments.upsert_marked(target, ui_evidence_marker(lineage_id), body)

    async def _lineage_now(self, lineage_id: uuid.UUID) -> _Lineage | None:
        """The lineage as it is now, with its evidence row and the failure, if
        any, of its latest revision's evidence."""

        async with self._sessionmaker() as session:
            lineage = await session.get(ThreadPublicationLineage, lineage_id)
            evidence = await session.get(PublicationLineageUiEvidence, lineage_id)
            if lineage is None or evidence is None:
                await session.rollback()
                return None
            failed = (
                await session.execute(
                    select(PublicationUiEvidence.error)
                    .join(Publication, Publication.id == PublicationUiEvidence.publication_id)
                    .where(
                        Publication.lineage_id == lineage_id,
                        Publication.revision_number == lineage.latest_revision,
                        PublicationUiEvidence.status == "failed",
                    )
                )
            ).first()
            repository = await lineage_repository(session, self._settings, lineage)
            snapshot = _Lineage(
                lineage_id=lineage.id,
                status=lineage.status,
                head_sha=lineage.head_sha,
                pr_number=lineage.pr_number,
                revision=lineage.latest_revision,
                repository=repository,
                ref_commit_sha=evidence.ref_commit_sha,
                failure=(failed[0] or "failed") if failed is not None else None,
            )
            await session.rollback()
        return snapshot

    async def _delete_ref(
        self, code_host: CodeHost, lease: _Lease, repository: RepositoryRef
    ) -> bool:
        if not supports(code_host.capabilities, Operation.DELETE_EVIDENCE):
            return False
        async with asyncio.timeout(_PHASE_CAP_S):
            await code_host.delete_evidence(repository, key=str(lease.lineage_id))
        await self._write_lineage(lease, ref_commit_sha=None)
        return True

    # Obligations ---------------------------------------------------------------

    async def _ended(self, lease: _Lease, result: PassResult) -> None:
        """A merged or closed lineage keeps its comment and loses its ref."""

        lineage = await self._lineage_now(lease.lineage_id)
        if lineage is None or lineage.status == "open" or lineage.ref_commit_sha is None:
            return
        if await self._delete_ref(
            self._code_host_for(lineage.repository), lease, lineage.repository
        ):
            result.deleted += 1

    async def _stale(self, lease: _Lease, result: PassResult) -> None:
        """A later revision has no evidence: the comment says so, and the ref
        holding an earlier revision's images is deleted."""

        lineage = await self._lineage_now(lease.lineage_id)
        if (
            lineage is None
            or lineage.status != "open"
            or lineage.head_sha is None
            or lineage.pr_number is None
        ):
            return
        code_host = self._code_host_for(lineage.repository)
        try:
            side = await self._comment_side(code_host, lineage.repository)
        except Unsupported:
            return
        if lineage.ref_commit_sha is not None:
            await self._delete_ref(code_host, lease, lineage.repository)
        body = render_stale(
            head_sha=lineage.head_sha, revision=lineage.revision, failure=lineage.failure
        )
        await self._write_lineage(lease)
        async with asyncio.timeout(_PHASE_CAP_S):
            await self._upsert(side, lineage.lineage_id, lineage.pr_number, body)
        await self._write_lineage(lease, comment_revision=lineage.revision, comment_kind="stale")
        result.rewritten += 1

    async def _load(self, publication_id: uuid.UUID) -> _Publish | None:
        """The row with everything publishing it reads, or None when it no
        longer has a publish obligation."""

        async with self._sessionmaker() as session:
            found = (
                await session.execute(
                    select(
                        PublicationUiEvidence,
                        Publication,
                        ThreadPublicationLineage,
                        PublicationLineageUiEvidence,
                    )
                    .select_from(PublicationUiEvidence)
                    .join(
                        PublicationLineageUiEvidence,
                        PublicationLineageUiEvidence.lineage_id == PublicationUiEvidence.lineage_id,
                    )
                    .where(
                        PublicationUiEvidence.publication_id == publication_id,
                        crud.publish_ready(),
                    )
                )
            ).first()
            if found is None:
                await session.rollback()
                return None
            row, publication, lineage, evidence = found
            assert lineage.head_sha is not None and lineage.pr_number is not None
            repository = await lineage_repository(session, self._settings, lineage)
            images = await session.scalars(
                select(PublicationUiEvidenceImage)
                .where(PublicationUiEvidenceImage.publication_id == publication_id)
                .order_by(PublicationUiEvidenceImage.ordinal)
            )
            state = _Publish(
                publication_id=row.publication_id,
                lineage_id=row.lineage_id,
                version=row.version,
                status=row.status,
                manifest_state=row.manifest_state,
                manifest_status=row.manifest_status,
                reason=row.reason,
                base_sha=publication.base_sha,
                candidate_tree=row.candidate_tree,
                evidence_commit_sha=row.evidence_commit_sha,
                comment_written=row.comment_written_at is not None,
                revision=lineage.latest_revision,
                head_sha=lineage.head_sha,
                pr_number=lineage.pr_number,
                stamp=publication.created_at,
                repository=repository,
                ref_commit_sha=evidence.ref_commit_sha,
                images=[
                    _Image(
                        entry=Entry(image.ordinal, image.state, image.label, image.path),
                        refusal=image.refusal,
                        bound=image.bound,
                        sha256=image.sha256,
                        content=image.content,
                        stored=image.stored_at is not None and image.content is not None,
                        url=image.url,
                    )
                    for image in images
                ],
            )
            await session.rollback()
        return state

    async def _claim_row(self, state: _Publish) -> float:
        """Move the row to ``publishing`` under this owner; the seconds left in
        its publish window, which starts at the first claim."""

        async with self._sessionmaker() as session:
            claimed = (
                await session.execute(
                    update(PublicationUiEvidence)
                    .where(
                        PublicationUiEvidence.publication_id == state.publication_id,
                        PublicationUiEvidence.version == state.version,
                        PublicationUiEvidence.status.not_in(UI_EVIDENCE_TERMINAL),
                    )
                    .values(
                        status="publishing",
                        attempts=PublicationUiEvidence.attempts + 1,
                        publish_deadline_at=func.coalesce(
                            PublicationUiEvidence.publish_deadline_at,
                            func.now() + crud.PUBLISH_WINDOW,
                        ),
                        lease_owner=self._owner,
                        lease_expires_at=func.now() + crud.LEASE,
                        version=PublicationUiEvidence.version + 1,
                        updated_at=func.now(),
                    )
                    .returning(
                        PublicationUiEvidence.version,
                        func.extract(
                            "epoch", PublicationUiEvidence.publish_deadline_at - func.now()
                        ),
                    )
                )
            ).first()
            await session.commit()
        if claimed is None:
            raise _LeaseLost
        state.version = int(claimed[0])
        return float(claimed[1])

    async def _publish(self, lease: _Lease, publication_id: uuid.UUID, result: PassResult) -> None:
        state = await self._load(publication_id)
        if state is None:
            return
        left = await self._claim_row(state)
        if left <= 0:
            await self._timed_out(state, lease)
            return
        try:
            async with asyncio.timeout(max(min(left - _COMMENT_RESERVE_S, _PHASE_CAP_S), 1.0)):
                await self._publish_phases(state, lease, result)
        except (ForgeError, TimeoutError):
            # Retried on a later pass until the publish window closes.
            await self._write_row(state, lease_owner=None, lease_expires_at=None)
            raise

    def _refusals(self, state: _Publish, head_tree: str | None) -> dict[int, str]:
        """Why each entry is not shown; an entry absent here may be shown."""

        refusals: dict[int, str] = {}
        for image in state.images:
            ordinal = image.entry.ordinal
            if image.refusal is not None:
                refusals[ordinal] = image.refusal
            elif not image.bound:
                refusals[ordinal] = "unbound"
            elif not image.stored:
                refusals[ordinal] = "not_stored"
            elif image.entry.state == "after" and (
                head_tree is None or head_tree != state.candidate_tree
            ):
                refusals[ordinal] = "head_changed"
        return refusals

    async def _publish_phases(self, state: _Publish, lease: _Lease, result: PassResult) -> None:
        repository = state.repository
        code_host = self._code_host_for(repository)
        try:
            side = await self._comment_side(code_host, repository)
        except Unsupported:
            # No comment could be told apart as ours: write nothing at all.
            await self._finish(state, "unsupported", error=OWNERSHIP_UNAVAILABLE)
            return
        if not supports(code_host.capabilities, Operation.STORE_EVIDENCE):
            captured = sum(1 for image in state.images if image.refusal is None and image.bound)
            await self._upsert(
                side,
                state.lineage_id,
                state.pr_number,
                render_unsupported(head_sha=state.head_sha, captured=captured),
            )
            await self._write_lineage(
                lease, comment_revision=state.revision, comment_kind="unsupported"
            )
            await self._finish(
                state, "unsupported", outcome="unsupported", comment_written_at=func.now()
            )
            result.published += 1
            return

        head_tree: str | None = None
        if any(
            image.entry.state == "after" and image.refusal is None and image.bound and image.stored
            for image in state.images
        ):
            # An after image is current only when the pushed head's tree is C.
            head_tree = (await code_host.read_commit(repository, state.head_sha)).tree
        refusals = self._refusals(state, head_tree)
        shown = [image for image in state.images if image.entry.ordinal not in refusals]
        expected = [image for image in state.images if image.refusal is None and image.bound]
        # An index arrived but no image was stored: the outcome comment says
        # so, and no ref is stored.
        incomplete = bool(expected) and not any(image.stored for image in expected)

        urls: dict[int, str] = {}
        if shown:
            urls = await self._stored_urls(state, lease, code_host, shown)
        elif state.ref_commit_sha is not None:
            await self._delete_ref(code_host, lease, repository)

        current = await self._lineage_now(state.lineage_id)
        if (
            current is None
            or current.status != "open"
            or current.revision != state.revision
            or current.head_sha != state.head_sha
        ):
            # A newer revision owns the pull request now; this comment would be stale.
            await self._finish(state, "superseded")
            return

        rendered = render_evidence(
            marker=ui_evidence_marker(state.lineage_id),
            manifest_state=state.manifest_state,
            manifest_status=state.manifest_status,
            reason=state.reason,
            head_sha=state.head_sha,
            revision=state.revision,
            base_sha=state.base_sha,
            entries=[image.entry for image in state.images],
            urls=urls,
            refusals=refusals,
            outcome="failed" if incomplete else None,
        )
        await self._write_lineage(lease)
        await self._upsert(side, state.lineage_id, state.pr_number, rendered.body)
        await self._write_row(state, comment_written_at=func.now())
        await self._write_lineage(
            lease,
            comment_revision=state.revision,
            comment_kind="failed" if incomplete else "evidence",
        )
        if incomplete:
            await self._finish(state, "failed", error="upload_incomplete", outcome=rendered.outcome)
        else:
            await self._finish(state, "published", outcome=rendered.outcome)
        result.published += 1

    async def _stored_urls(
        self, state: _Publish, lease: _Lease, code_host: CodeHost, shown: list[_Image]
    ) -> dict[int, str]:
        """Host the shown images under the lineage's ref; a replay reuses the
        commit already recorded on both the row and the lineage."""

        if (
            state.evidence_commit_sha is not None
            and state.evidence_commit_sha == state.ref_commit_sha
            and all(image.url for image in shown)
        ):
            return {image.entry.ordinal: str(image.url) for image in shown}
        files = {
            image.entry.ordinal: EvidenceFile(
                f"{image.entry.state}/{image.entry.ordinal:04d}.png",
                image.content or b"",
                image.sha256 or "",
            )
            for image in shown
        }
        await self._write_lineage(lease)
        stored = await code_host.store_evidence(
            state.repository,
            key=str(state.lineage_id),
            files=list(files.values()),
            message=f"Curie UI evidence for publication {state.publication_id}\n",
            stamp=state.stamp,
        )
        urls = {ordinal: stored.urls[item.path] for ordinal, item in files.items()}
        async with self._sessionmaker() as session:
            for ordinal, url in urls.items():
                await session.execute(
                    update(PublicationUiEvidenceImage)
                    .where(
                        PublicationUiEvidenceImage.publication_id == state.publication_id,
                        PublicationUiEvidenceImage.ordinal == ordinal,
                    )
                    .values(url=url)
                )
            await session.commit()
        await self._write_row(state, evidence_commit_sha=stored.commit_sha)
        await self._write_lineage(lease, ref_commit_sha=stored.commit_sha)
        return urls

    async def _timed_out(self, state: _Publish, lease: _Lease) -> None:
        """The publish window closed. A row whose comment is written is
        published; otherwise one last outcome comment says the images ran out
        of time, and the row fails."""

        if state.comment_written:
            await self._finish(state, "published", error="timeout")
            return
        refusals = {
            image.entry.ordinal: image.refusal or ("timeout" if image.bound else "unbound")
            for image in state.images
        }
        rendered = render_evidence(
            marker=ui_evidence_marker(state.lineage_id),
            manifest_state=state.manifest_state,
            manifest_status=state.manifest_status,
            reason=state.reason,
            head_sha=state.head_sha,
            revision=state.revision,
            base_sha=state.base_sha,
            entries=[image.entry for image in state.images],
            urls={},
            refusals=refusals,
        )
        written = False
        try:
            async with asyncio.timeout(_COMMENT_RESERVE_S):
                code_host = self._code_host_for(state.repository)
                side = await self._comment_side(code_host, state.repository)
                await self._upsert(side, state.lineage_id, state.pr_number, rendered.body)
            written = True
        except Unsupported:
            await self._finish(state, "unsupported", error=OWNERSHIP_UNAVAILABLE)
            return
        except (ForgeError, TimeoutError) as exc:
            logger.warning(
                "UI evidence outcome for publication %s not written: %s",
                state.publication_id,
                exc if isinstance(exc, ForgeError) else "timeout",
            )
        if written:
            await self._write_lineage(lease, comment_revision=state.revision, comment_kind="failed")
            await self._finish(
                state,
                "failed",
                error="timeout",
                outcome=rendered.outcome,
                comment_written_at=func.now(),
            )
            return
        await self._finish(state, "failed", error="timeout")


def start_ui_evidence_reconciler(
    sessionmaker: async_sessionmaker[AsyncSession],
    settings: Settings,
    client: httpx.AsyncClient,
) -> asyncio.Task[None] | None:
    """Start the loop when its interval is positive."""

    interval = settings.ui_evidence_reconcile_interval_s
    if interval <= 0:
        return None
    reconciler = UiEvidenceReconciler(sessionmaker, settings, client, interval_seconds=interval)
    return asyncio.create_task(reconciler.run_forever())
