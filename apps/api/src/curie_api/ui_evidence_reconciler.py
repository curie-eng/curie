"""Publish a publication's optional UI evidence as one pull request comment.

Evidence publishes only once the code result is recorded: the publication
``succeeded`` with the lineage's pull request and is still the lineage's latest
revision. This loop only SELECTs publications and lineages and never edits a
pull request, so evidence can never change, retry or fail the code publication.

One process at a time writes a lineage's ref and comment (a CAS lease on its
evidence row). Every phase is idempotent: the evidence commit is content
addressed with a fixed author and date, the ref is force-moved to the same
commit, and the comment is found by its marker, so a lapsed lease resumes.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

import httpx
from sqlalchemy import ColumnElement, and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from curie_api.crud import ui_evidence as crud
from curie_api.factory_rediscovery import publishing_code_host
from curie_api.forges.capabilities import Operation, supports
from curie_api.forges.errors import ForgeError, Unavailable, Unsupported
from curie_api.forges.hosts import resolve_stored
from curie_api.forges.ports import CodeHost, MarkedComments
from curie_api.forges.types import (
    EvidenceFile,
    PullRequestRef,
    PullRequestState,
    ReplyTarget,
    RepositoryRef,
)

from .config import Settings
from .models import (
    Publication,
    PublicationLineageUiEvidence,
    PublicationUiEvidence,
    PublicationUiEvidenceImage,
    ThreadPublicationLineage,
)
from .repository_access import lineage_repository
from .ui_evidence_comment import (
    Rendered,
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
# A phase ends before the lease it runs under can lapse.
_PHASE_CAP_S = float(crud.LEASE.total_seconds()) - 5.0
_BATCH_LIMIT = 20

Post = Callable[[str], Awaitable[None]]


class _LeaseLost(Exception):
    """Another owner holds the lineage or row now; stop without writing."""


@dataclass
class PassResult:
    published: int = 0
    rewritten: int = 0
    deleted: int = 0
    skipped: int = 0
    rate_limited: bool = False


@dataclass
class _Lease:
    lineage_id: uuid.UUID
    version: int


@dataclass
class _Publish:
    # Detached snapshots: the session that loaded them is closed, not rolled
    # back, so their loaded columns stay readable.
    row: PublicationUiEvidence
    images: list[PublicationUiEvidenceImage]
    version: int
    base_sha: str
    revision: int
    head_sha: str
    pr_number: int
    stamp: datetime
    repository: RepositoryRef
    ref_commit_sha: str | None
    # The code host may hold this lineage's ref: one is recorded, or a store
    # began and its commit was never recorded.
    ref_held: bool


@dataclass(frozen=True)
class _Lineage:
    status: str
    head_sha: str | None
    pr_number: int | None
    revision: int
    repository: RepositoryRef
    ref_commit_sha: str | None
    ref_held: bool
    failure: str | None


def _render(
    state: _Publish, urls: dict[int, str], refusals: dict[int, str], outcome: str | None = None
) -> Rendered:
    return render_evidence(
        marker=ui_evidence_marker(state.row.lineage_id),
        manifest_state=state.row.manifest_state,
        manifest_status=state.row.manifest_status,
        reason=state.row.reason,
        head_sha=state.head_sha,
        revision=state.revision,
        base_sha=state.base_sha,
        entries=state.images,
        urls=urls,
        refusals=refusals,
        outcome=outcome,
    )


def _stored(image: PublicationUiEvidenceImage) -> bool:
    return image.stored_at is not None and image.content is not None


def _refusal(image: PublicationUiEvidenceImage) -> str | None:
    if image.refusal is not None:
        return image.refusal
    if not image.bound:
        return "unbound"
    return None if _stored(image) else "not_stored"


class UiEvidenceReconciler:
    """Publish uploaded UI evidence and keep each pull request's comment current."""

    def __init__(
        self,
        sessionmaker: async_sessionmaker[AsyncSession],
        settings: Settings,
        client: httpx.AsyncClient,
        *,
        interval_seconds: float,
    ) -> None:
        self._sessionmaker = sessionmaker
        self._settings = settings
        self._interval = interval_seconds
        self._code_host_for: Callable[[RepositoryRef], CodeHost] = lambda repository: (
            publishing_code_host(settings, client, repository)
        )
        # One lease owner per reconciler instance (one process).
        self._owner = f"ui-evidence-{uuid.uuid4().hex}"

    async def _work(self) -> dict[uuid.UUID, uuid.UUID | str]:
        """Each lineage with evidence work: the publication to publish, else
        ``stale`` or ``ended``."""

        async with self._sessionmaker() as session:
            await crud.sweep(session)
            publish = list(
                await session.execute(
                    select(PublicationUiEvidence.lineage_id, PublicationUiEvidence.publication_id)
                    .where(crud.publish_ready())
                    .order_by(PublicationUiEvidence.created_at)
                    .limit(_BATCH_LIMIT)
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
            evidence = select(PublicationUiEvidence.publication_id).where(
                PublicationUiEvidence.publication_id == Publication.id
            )
            no_evidence = latest.where(
                Publication.created_at < func.now() - timedelta(seconds=NO_EVIDENCE_AFTER_S),
                ~evidence.exists(),
            ).exists()
            failed = latest.where(
                evidence.where(PublicationUiEvidence.status == "failed").exists()
            ).exists()

            async def lineages(*where: ColumnElement[bool]) -> list[uuid.UUID]:
                return list(
                    await session.scalars(
                        select(ThreadPublicationLineage.id)
                        .join(
                            PublicationLineageUiEvidence,
                            PublicationLineageUiEvidence.lineage_id == ThreadPublicationLineage.id,
                        )
                        .where(*where)
                        .order_by(ThreadPublicationLineage.id)
                        .limit(_BATCH_LIMIT)
                    )
                )

            stale = await lineages(
                ThreadPublicationLineage.status == "open",
                ThreadPublicationLineage.pr_number.is_not(None),
                ThreadPublicationLineage.head_sha.is_not(None),
                # The pull request may carry something the latest revision did
                # not record: a comment posted and never recorded, or a ref or
                # comment recorded for an earlier revision or never at all.
                or_(
                    PublicationLineageUiEvidence.comment_pending,
                    and_(
                        or_(
                            PublicationLineageUiEvidence.comment_revision.is_(None),
                            PublicationLineageUiEvidence.comment_revision
                            < ThreadPublicationLineage.latest_revision,
                        ),
                        or_(
                            PublicationLineageUiEvidence.ref_pending,
                            PublicationLineageUiEvidence.ref_commit_sha.is_not(None),
                            PublicationLineageUiEvidence.comment_revision.is_not(None),
                        ),
                    ),
                ),
                or_(no_evidence, failed),
            )
            ended = await lineages(
                ThreadPublicationLineage.status.in_(("merged", "closed")),
                or_(
                    PublicationLineageUiEvidence.ref_commit_sha.is_not(None),
                    PublicationLineageUiEvidence.ref_pending,
                ),
            )
        work: dict[uuid.UUID, uuid.UUID | str] = {}
        for lineage_id, publication_id in publish:
            work.setdefault(lineage_id, publication_id)
        for kind, ids in (("stale", stale), ("ended", ended)):
            for lineage_id in ids:
                work.setdefault(lineage_id, kind)
        return work

    async def _write_lineage(self, lease: _Lease, *, renew: bool = True, **values: object) -> None:
        async with self._sessionmaker() as session:
            version = await crud.update_lineage(
                session, lease.lineage_id, self._owner, lease.version, renew=renew, **values
            )
        if version is None:
            raise _LeaseLost
        lease.version = version

    async def _write_row(self, state: _Publish, **values: object) -> None:
        async with self._sessionmaker() as session:
            version = await crud.update_row(
                session, state.row.publication_id, state.version, **values
            )
        if version is None:
            raise _LeaseLost
        state.version = version

    async def _finish(self, state: _Publish, status: str, **values: object) -> None:
        async with self._sessionmaker() as session:
            if not await crud.finish_row(
                session, state.row.publication_id, state.version, status, **values
            ):
                raise _LeaseLost

    async def _finish_with_comment(
        self, state: _Publish, lease: _Lease, status: str, kind: str, **values: object
    ) -> None:
        """The comment is written: record it on the lineage and settle the row
        in one commit."""

        async with self._sessionmaker() as session:
            version = await crud.finish_row_with_comment(
                session,
                state.row.publication_id,
                state.version,
                status,
                (lease.lineage_id, self._owner, lease.version),
                {
                    "comment_revision": state.revision,
                    "comment_kind": kind,
                    "comment_pending": False,
                },
                comment_written_at=func.now(),
                **values,
            )
        if version is None:
            raise _LeaseLost
        lease.version = version

    @staticmethod
    async def _head_is_candidate(code_host: CodeHost, state: _Publish) -> bool:
        """An after image is current only when the pull request is open, its
        live head is the recorded head, and that commit's tree is the candidate."""

        pull = await code_host.read_pull_request(
            PullRequestRef(state.repository, str(state.pr_number))
        )
        if pull.state is not PullRequestState.OPEN or pull.head_sha != state.head_sha:
            return False
        head_tree = (await code_host.read_commit(state.repository, pull.head_sha)).tree
        return head_tree is not None and head_tree == state.row.candidate_tree

    async def run_once(self) -> PassResult:
        """One bounded pass over the lineages with evidence work."""

        result = PassResult()
        for lineage_id, work in (await self._work()).items():
            async with self._sessionmaker() as session:
                version = await crud.claim_lineage(session, lineage_id, self._owner)
            if version is None:
                result.skipped += 1
                continue
            lease = _Lease(lineage_id, version)
            try:
                if isinstance(work, uuid.UUID):
                    await self._publish(lease, work, result)
                elif work == "stale":
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
                    lineage_id,
                    exc if isinstance(exc, ForgeError) else "timeout",
                )
            except Exception:
                result.skipped += 1
                logger.exception("UI evidence work for lineage %s failed", lineage_id)
            try:
                await self._write_lineage(lease, renew=False)
            except _LeaseLost:
                pass
            if result.rate_limited:
                break
        return result

    async def run_forever(self) -> None:
        while True:
            # The first pass waits one interval, so a short-lived app never runs one.
            await asyncio.sleep(self._interval)
            try:
                await self.run_once()
            except Exception:  # noqa: BLE001 - a failed pass must not end the loop
                logger.exception("UI evidence reconcile pass failed")

    async def _poster(
        self, code_host: CodeHost, repository: RepositoryRef, lineage_id: uuid.UUID, pr_number: int
    ) -> Post:
        """Upserts the lineage's marked comment. Reads only until called; raises
        `Unsupported` when the code host cannot tell its own comment apart."""

        # The comment target needs the repository's current immutable id; one
        # whose identity changed since the lineage was recorded is refused.
        repository = await resolve_stored(code_host, repository)
        scoped = getattr(code_host, "marked_comments_for", None)
        comments: MarkedComments = (
            scoped(repository) if callable(scoped) else code_host.marked_comments
        )
        target = ReplyTarget(
            "pull_request", pull_request=PullRequestRef(repository, str(pr_number))
        )
        marker = ui_evidence_marker(lineage_id)

        async def post(body: str) -> None:
            await comments.upsert_marked(target, marker, body)

        return post

    async def _lineage_now(self, lineage_id: uuid.UUID) -> _Lineage | None:
        async with self._sessionmaker() as session:
            lineage = await session.get(ThreadPublicationLineage, lineage_id)
            evidence = await session.get(PublicationLineageUiEvidence, lineage_id)
            if lineage is None or evidence is None:
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
            return _Lineage(
                status=lineage.status,
                head_sha=lineage.head_sha,
                pr_number=lineage.pr_number,
                revision=lineage.latest_revision,
                repository=await lineage_repository(session, self._settings, lineage),
                ref_commit_sha=evidence.ref_commit_sha,
                ref_held=evidence.ref_commit_sha is not None or evidence.ref_pending,
                failure=(failed[0] or "failed") if failed is not None else None,
            )

    async def _delete_ref(
        self, code_host: CodeHost, lease: _Lease, repository: RepositoryRef
    ) -> bool:
        if not supports(code_host.capabilities, Operation.DELETE_EVIDENCE):
            return False
        async with asyncio.timeout(_PHASE_CAP_S):
            await code_host.delete_evidence(repository, key=str(lease.lineage_id))
        await self._write_lineage(lease, ref_commit_sha=None, ref_pending=False)
        return True

    async def _ended(self, lease: _Lease, result: PassResult) -> None:
        """A merged or closed lineage keeps its comment and loses its ref."""

        lineage = await self._lineage_now(lease.lineage_id)
        if lineage is None or lineage.status == "open" or not lineage.ref_held:
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
            post = await self._poster(
                code_host, lineage.repository, lease.lineage_id, lineage.pr_number
            )
        except Unsupported:
            return
        if lineage.ref_held:
            await self._delete_ref(code_host, lease, lineage.repository)
        body = render_stale(
            head_sha=lineage.head_sha, revision=lineage.revision, failure=lineage.failure
        )
        await self._write_lineage(lease)
        async with asyncio.timeout(_PHASE_CAP_S):
            await post(body)
        await self._write_lineage(
            lease, comment_revision=lineage.revision, comment_kind="stale", comment_pending=False
        )
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
                return None
            row, publication, lineage, evidence = found
            assert lineage.head_sha is not None and lineage.pr_number is not None
            images = await session.scalars(
                select(PublicationUiEvidenceImage)
                .where(PublicationUiEvidenceImage.publication_id == publication_id)
                .order_by(PublicationUiEvidenceImage.ordinal)
            )
            return _Publish(
                row=row,
                images=list(images),
                version=row.version,
                base_sha=publication.base_sha,
                revision=lineage.latest_revision,
                head_sha=lineage.head_sha,
                pr_number=lineage.pr_number,
                stamp=publication.created_at,
                repository=await lineage_repository(session, self._settings, lineage),
                ref_commit_sha=evidence.ref_commit_sha,
                ref_held=evidence.ref_commit_sha is not None or evidence.ref_pending,
            )

    async def _claim_row(self, state: _Publish) -> float:
        """Seconds left in the row's publish window, once claimed by this owner."""

        async with self._sessionmaker() as session:
            claimed = await crud.claim_row(
                session, state.row.publication_id, state.version, self._owner
            )
        if claimed is None:
            raise _LeaseLost
        state.version, left = claimed
        return left

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

    async def _publish_phases(self, state: _Publish, lease: _Lease, result: PassResult) -> None:
        repository = state.repository
        code_host = self._code_host_for(repository)
        try:
            post = await self._poster(code_host, repository, state.row.lineage_id, state.pr_number)
        except Unsupported:
            # No comment could be told apart as ours: write nothing at all.
            await self._finish(state, "unsupported", error=OWNERSHIP_UNAVAILABLE)
            return
        expected = [image for image in state.images if image.refusal is None and image.bound]
        if not supports(code_host.capabilities, Operation.STORE_EVIDENCE):
            await self._write_lineage(lease, comment_pending=True)
            await post(render_unsupported(head_sha=state.head_sha, captured=len(expected)))
            await self._finish_with_comment(
                state, lease, "unsupported", "unsupported", outcome="unsupported"
            )
            result.published += 1
            return

        refusals = {image.ordinal: code for image in state.images if (code := _refusal(image))}
        after = [
            image
            for image in state.images
            if image.state == "after" and image.ordinal not in refusals
        ]
        if after and not await self._head_is_candidate(code_host, state):
            refusals.update({image.ordinal: "head_changed" for image in after})
        shown = [image for image in state.images if image.ordinal not in refusals]
        # An index arrived but no image was stored: the outcome comment says
        # so, and no ref is stored.
        incomplete = bool(expected) and not any(_stored(image) for image in expected)

        urls: dict[int, str] = {}
        if shown:
            urls = await self._stored_urls(state, lease, code_host, shown)
        elif state.ref_held:
            await self._delete_ref(code_host, lease, repository)

        current = await self._lineage_now(state.row.lineage_id)
        if (
            current is None
            or current.status != "open"
            or current.revision != state.revision
            or current.head_sha != state.head_sha
        ):
            # A newer revision owns the pull request now; this comment would be stale.
            await self._finish(state, "superseded")
            return

        rendered = _render(state, urls, refusals, "failed" if incomplete else None)
        await self._write_lineage(lease, comment_pending=True)
        await post(rendered.body)
        if incomplete:
            await self._finish_with_comment(
                state,
                lease,
                "failed",
                "failed",
                error="upload_incomplete",
                outcome=rendered.outcome,
            )
        else:
            await self._finish_with_comment(
                state, lease, "published", "evidence", outcome=rendered.outcome
            )
        result.published += 1

    async def _stored_urls(
        self,
        state: _Publish,
        lease: _Lease,
        code_host: CodeHost,
        shown: list[PublicationUiEvidenceImage],
    ) -> dict[int, str]:
        """Host the shown images under the lineage's ref; a replay reuses the
        commit already recorded on both the row and the lineage."""

        if (
            state.row.evidence_commit_sha is not None
            and state.row.evidence_commit_sha == state.ref_commit_sha
            and all(image.url for image in shown)
        ):
            return {image.ordinal: str(image.url) for image in shown}
        files = {
            image.ordinal: EvidenceFile(
                f"{image.state}/{image.ordinal:04d}.png",
                image.content or b"",
                image.sha256 or "",
            )
            for image in shown
        }
        # Durable before the store: a crash after it still leaves the ref
        # findable by ended-lineage cleanup.
        await self._write_lineage(lease, ref_pending=True)
        stored = await code_host.store_evidence(
            state.repository,
            key=str(state.row.lineage_id),
            files=list(files.values()),
            message=f"Curie UI evidence for publication {state.row.publication_id}\n",
            stamp=state.stamp,
        )
        urls = {ordinal: stored.urls[item.path] for ordinal, item in files.items()}
        async with self._sessionmaker() as session:
            for ordinal, url in urls.items():
                await session.execute(
                    update(PublicationUiEvidenceImage)
                    .where(
                        PublicationUiEvidenceImage.publication_id == state.row.publication_id,
                        PublicationUiEvidenceImage.ordinal == ordinal,
                    )
                    .values(url=url)
                )
            await session.commit()
        await self._write_row(state, evidence_commit_sha=stored.commit_sha)
        await self._write_lineage(lease, ref_commit_sha=stored.commit_sha, ref_pending=False)
        return urls

    async def _timed_out(self, state: _Publish, lease: _Lease) -> None:
        """The publish window closed: one last outcome comment says the images
        ran out of time, and the row fails."""

        await self._write_lineage(lease, comment_pending=True)
        rendered = _render(
            state,
            {},
            {
                image.ordinal: image.refusal or ("timeout" if image.bound else "unbound")
                for image in state.images
            },
        )
        try:
            async with asyncio.timeout(_COMMENT_RESERVE_S):
                code_host = self._code_host_for(state.repository)
                post = await self._poster(
                    code_host, state.repository, state.row.lineage_id, state.pr_number
                )
                await post(rendered.body)
        except Unsupported:
            await self._finish(state, "unsupported", error=OWNERSHIP_UNAVAILABLE)
            return
        except (ForgeError, TimeoutError) as exc:
            logger.warning(
                "UI evidence outcome for publication %s not written: %s",
                state.row.publication_id,
                exc if isinstance(exc, ForgeError) else "timeout",
            )
            await self._finish(state, "failed", error="timeout")
            return
        await self._finish_with_comment(
            state, lease, "failed", "failed", error="timeout", outcome=rendered.outcome
        )


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
