"""Pull request, branch and commit calls the publication worker makes, through CodeHost.

ADR 0197 "Two ports" item 6: the worker holds no forge code. It names one
stored publication; the API derives the repository, branch, revision, title,
body, draft flag and base from that row, acts through `CodeHost`, and returns
the facts as data. Every route takes the internal worker credential
(``X-Curie-Worker-Token``), as every other ``/v1/internal/publications`` route.

A refusal is a 409 with a ``code`` and a ``message`` the worker reports; a code
host that cannot answer is a 503; a refused or missing credential is a 502.
No response ever carries a provider payload or a credential.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import NoReturn, cast

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from redis.exceptions import RedisError
from sqlalchemy import select

from curie_api.crud import publication_queries as crud_publication_queries
from curie_api.factory_rediscovery import REFUSING, publishing_code_host, rediscover
from curie_api.forges.errors import ForgeError, NotFound
from curie_api.forges.github.code_host import GitHubCodeHost
from curie_api.forges.hosts import resolve_stored
from curie_api.forges.ports import CodeHost
from curie_api.forges.trackers import tracker_for_work_item
from curie_api.forges.types import (
    GITHUB,
    PullRequest,
    PullRequestRef,
    PullRequestState,
    RepositoryRef,
)
from curie_api.publication_pulls import (
    PullRequestContract,
    PullRequestRefused,
    adopt_or_open,
    update_metadata,
)
from curie_api.publication_verification import (
    forge_failure,
    publication_base_ref,
    verify_publication_commit,
)
from curie_api.schemas.publications import (
    PublicationBranchHeadOut,
    PublicationPullRecover,
    PublicationPullRequestOut,
    PublicationRevisionOut,
    PublicationRevisionVerify,
)

from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..models import ExecutionRequest, Publication, ThreadPublicationLineage, WorkItem
from ..repository_access import lineage_repository

# The worker's pull request, branch and commit calls (ADR 0197, "Two ports" item 6).
router = APIRouter(
    prefix="/v1/internal/publications",
    tags=["internal-publications"],
    dependencies=[Depends(require_internal_worker_token)],
)

logger = logging.getLogger(__name__)

_ACTIVE = frozenset({"approved", "launching", "running"})
_PUBLICATION_LABELS_TTL_S = 30 * 24 * 60 * 60


def _refuse(code: str, message: str) -> NoReturn:
    raise HTTPException(
        status.HTTP_409_CONFLICT,
        {"code": f"publication.{code}", "message": message},
        headers={"Cache-Control": "no-store"},
    )


async def _stored(
    session: SessionDep, publication_id: uuid.UUID
) -> tuple[Publication, ThreadPublicationLineage]:
    publication = await crud_publication_queries.get_publication(session, publication_id)
    if publication is None or publication.lineage is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "publication lineage not found",
            headers={"Cache-Control": "no-store"},
        )
    return publication, publication.lineage


async def _code_host(
    request: Request, session: SessionDep, lineage: ThreadPublicationLineage
) -> tuple[CodeHost, RepositoryRef]:
    """The lineage's repository (`lineage_repository`) and the code host it
    names: the install's GitHub for a GitHub repository, else its forge entry."""

    settings = get_settings()
    client = request.app.state.http_client
    repository = await lineage_repository(session, settings, lineage)
    return publishing_code_host(settings, client, repository), repository


def _out(pull: PullRequest) -> PublicationPullRequestOut:
    return PublicationPullRequestOut(
        number=int(pull.ref.number),
        url=pull.url,
        state=pull.state.value,
        head_sha=pull.head_sha,
        head_ref=pull.head_ref,
        updated_at=pull.updated_at,
    )


@router.get("/{publication_id}/pull-request", response_model=PublicationPullRequestOut)
async def read_publication_pull_request(
    publication_id: uuid.UUID,
    request: Request,
    session: SessionDep,
    response: Response,
    pr_number: int = Query(gt=0),
) -> PublicationPullRequestOut:
    """The stored pull request, read now by its number and nothing else."""

    response.headers["Cache-Control"] = "no-store"
    publication, lineage = await _stored(session, publication_id)
    if lineage.pr_number != pr_number:
        _refuse("lineage_stale", "the stored pull request number differs from the lineage")
    code_host, repository = await _code_host(request, session, lineage)
    try:
        pull = await code_host.read_pull_request(PullRequestRef(repository, str(pr_number)))
    except ForgeError as exc:
        raise forge_failure(exc) from None
    return _out(pull)


@router.get("/{publication_id}/branch-head", response_model=PublicationBranchHeadOut)
async def read_publication_branch_head(
    publication_id: uuid.UUID,
    request: Request,
    session: SessionDep,
    response: Response,
) -> PublicationBranchHeadOut:
    """The deterministic lineage branch's head, creating nothing."""

    response.headers["Cache-Control"] = "no-store"
    publication, lineage = await _stored(session, publication_id)
    code_host, repository = await _code_host(request, session, lineage)
    try:
        base_branch = await publication_base_ref(session, code_host, publication, repository)
        head = await code_host.branch_head(repository, lineage.branch)
    except ForgeError as exc:
        raise forge_failure(exc) from None
    return PublicationBranchHeadOut(head_sha=head, base_branch=base_branch)


@router.post("/{publication_id}/revision-commit", response_model=PublicationRevisionOut)
async def verify_publication_revision(
    publication_id: uuid.UUID,
    data: PublicationRevisionVerify,
    request: Request,
    session: SessionDep,
    response: Response,
) -> PublicationRevisionOut:
    """Prove the marked revision and its authoritative publication parent."""

    response.headers["Cache-Control"] = "no-store"
    publication, lineage = await _stored(session, publication_id)
    if data.revision_id != publication.id or data.expected_parent != (
        publication.expected_prior_head
    ):
        _refuse("revision_mismatch", "the verification names another publication revision")
    code_host, repository = await _code_host(request, session, lineage)
    try:
        base_branch = await publication_base_ref(session, code_host, publication, repository)
        verified = await verify_publication_commit(
            code_host,
            repository,
            publication,
            commit_sha=data.commit_sha,
            base_branch=base_branch,
        )
    except PullRequestRefused as refused:
        _refuse(refused.code, refused.message)
    except ForgeError as exc:
        raise forge_failure(exc) from None
    return PublicationRevisionOut(commit_sha=verified.commit_sha, parent_sha=verified.parent_sha)


def _no_other_publication(
    request: Request,
    session: SessionDep,
    publication: Publication,
    lineage: ThreadPublicationLineage,
    code_host: CodeHost,
) -> Callable[[], Awaitable[None]]:
    """The check `adopt_or_open` runs right before it opens a new pull request.

    For a publication of a tracker-admitted WorkItem, it refuses
    ``publication_exists`` when the code host already lists another open
    factory publication for the issue (or more than one), so a publication
    that appeared between
    admission and now is never doubled (#4629). A completed one does not
    block: only an open publication is a duplicate.
    """

    async def check() -> None:
        if publication.execution_request_id is None:
            return
        item = await session.scalar(
            select(WorkItem)
            .join(ExecutionRequest, ExecutionRequest.work_item_id == WorkItem.id)
            .where(ExecutionRequest.id == publication.execution_request_id)
        )
        if item is None:
            return
        try:
            tracker = tracker_for_work_item(
                request.app.state.forges,
                request.app.state.http_client,
                item.tracker_issue,
                item.repository,
                item.code_host_installation_id,
            )
        except NotFound:
            if item.tracker_issue.kind == GITHUB:
                # A GitHub WorkItem on an install with no GitHub tracker has no
                # factory intake and no tracker to name its issue.
                return
            raise
        # The WorkItem's recorded repository, with its immutable id, is the one
        # its issue's references are judged against.
        found = await rediscover(
            tracker,
            code_host,
            item.tracker_issue,
            item.repository,
            event_at=None,
            exclude_branch=lineage.branch,
        )
        if found.verdict in REFUSING:
            links = ", ".join(p.url for p in found.publications)
            what = (
                "an open pull request from a factory publication branch whose author "
                "cannot be verified"
                if found.verdict == "unconfirmed"
                else "an open factory pull request"
            )
            raise PullRequestRefused(
                "publication_exists",
                f"the issue already has {what} ({found.verdict}): {links}",
            )

    return check


async def _apply_publication_labels(
    request: Request,
    publication: Publication,
    code_host: CodeHost,
    repository: RepositoryRef,
    pull: PullRequest,
) -> None:
    if repository.kind != GITHUB or pull.state != PullRequestState.OPEN or pull.draft:
        return
    labels = get_settings().factory_publication_labels.get(repository.path.casefold())
    if not labels:
        return
    marker = f"curie:publication-labels:{publication.id}"
    try:
        valkey = request.app.state.valkey
        if await valkey.exists(marker):
            return
        await cast(GitHubCodeHost, code_host).add_pull_request_labels(pull.ref, labels)
        await valkey.set(marker, "1", ex=_PUBLICATION_LABELS_TTL_S)
    except (ForgeError, RedisError) as exc:
        logger.warning(
            "Publication labels failed publication_id=%s repository=%s error=%s",
            publication.id,
            repository.path,
            type(exc).__name__,
        )


@router.post(
    "/{publication_id}/pull-request",
    response_model=PublicationPullRequestOut,
    responses={
        204: {"description": "Publication branch absent, or no pull request in adoption-only mode"},
    },
)
async def recover_publication_pull_request(
    publication_id: uuid.UUID,
    data: PublicationPullRecover,
    request: Request,
    session: SessionDep,
    response: Response,
) -> PublicationPullRequestOut | Response:
    """Adopt the branch's pull request, or open one unless ``adopt_only`` is true.

    The contract is the stored publication's title, body and draft flag, the
    lineage branch, and the admitted WorkItem's base or else the repository's
    default branch (`curie_api.publication_pulls.adopt_or_open`).

    An absent branch, or no pull request in adoption-only mode, returns 204.
    """

    response.headers["Cache-Control"] = "no-store"
    publication, lineage = await _stored(session, publication_id)
    if publication.status not in _ACTIVE:
        _refuse("not_approved", "the publication is not approved for a pull request")
    if lineage.status != "open":
        _refuse("lineage_terminal", "the pull request for this thread is merged or closed")
    code_host, repository = await _code_host(request, session, lineage)
    try:
        base_branch = await publication_base_ref(session, code_host, publication, repository)
        if lineage.head_sha is None or lineage.pr_number is None:
            head = await code_host.branch_head(repository, lineage.branch)
            if head is None:
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            if head != data.expected_head_sha:
                _refuse(
                    "branch_moved", "the publication branch no longer matches the expected commit"
                )
            await verify_publication_commit(
                code_host,
                repository,
                publication,
                commit_sha=data.expected_head_sha,
                base_branch=base_branch,
            )
        contract = PullRequestContract(
            branch=lineage.branch,
            base=base_branch,
            title=publication.title,
            body=publication.body,
            draft=publication.open_as_draft,
        )
        pull = await adopt_or_open(
            code_host,
            repository,
            contract,
            expected_head_sha=data.expected_head_sha,
            adopt_only=data.adopt_only,
            before_open=_no_other_publication(request, session, publication, lineage, code_host),
        )
    except PullRequestRefused as refused:
        _refuse(refused.code, refused.message)
    except ForgeError as exc:
        raise forge_failure(exc) from None
    if pull is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    await _apply_publication_labels(request, publication, code_host, repository, pull)
    return _out(pull)


@router.post("/{publication_id}/pull-request/metadata", response_model=PublicationPullRequestOut)
async def update_publication_pull_request_metadata(
    publication_id: uuid.UUID,
    request: Request,
    session: SessionDep,
    response: Response,
) -> PublicationPullRequestOut:
    """Apply a metadata-only revision's title and body to its stored pull request.

    A metadata-only revision has nothing to push, so no publication Job runs
    and this update is its whole effect (ADR 0197, "Two ports" item 6). The
    contract and the observed digests come from the stored publication. A
    merged or closed pull request is answered unchanged, with its state, so
    the worker records the lineage terminal.
    """

    response.headers["Cache-Control"] = "no-store"
    publication, lineage = await _stored(session, publication_id)
    if publication.status not in _ACTIVE:
        _refuse("not_approved", "the publication is not approved for a pull request")
    if lineage.status != "open":
        _refuse("lineage_terminal", "the pull request for this thread is merged or closed")
    if publication.patch_bytes != b"":
        # A patch is pushed by the Job; a purged revision has nothing to apply.
        _refuse("not_metadata_only", "the publication is not a metadata-only revision")
    if (
        lineage.pr_number is None
        or publication.observed_title_sha256 is None
        or publication.observed_body_sha256 is None
    ):
        _refuse(
            "metadata_unobserved",
            "a metadata-only revision needs a stored pull request and its observed metadata",
        )
    assert lineage.pr_number is not None
    assert publication.observed_title_sha256 is not None
    assert publication.observed_body_sha256 is not None
    code_host, repository = await _code_host(request, session, lineage)
    try:
        resolved = await resolve_stored(code_host, repository)
        contract = PullRequestContract(
            branch=lineage.branch,
            base=await publication_base_ref(session, code_host, publication, resolved),
            title=publication.title,
            body=publication.body,
            draft=publication.open_as_draft,
        )
        pull = await update_metadata(
            code_host,
            PullRequestRef(resolved, str(lineage.pr_number)),
            contract,
            expected_head_sha=publication.base_sha,
            observed_title_sha256=publication.observed_title_sha256,
            observed_body_sha256=publication.observed_body_sha256,
        )
    except PullRequestRefused as refused:
        _refuse(refused.code, refused.message)
    except ForgeError as exc:
        raise forge_failure(exc) from None
    return _out(pull)
