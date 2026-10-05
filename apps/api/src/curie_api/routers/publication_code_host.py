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

import uuid
from typing import NoReturn

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import select

from curie_api.crud import publication_queries as crud_publication_queries
from curie_api.forges.errors import ForgeError, NotFound, Unauthorized, Unavailable
from curie_api.forges.hosts import code_host_for, repository_ref, resolve_stored
from curie_api.forges.ports import CodeHost
from curie_api.forges.types import PullRequest, PullRequestRef, RepositoryRef
from curie_api.publication_pulls import (
    PullRequestContract,
    PullRequestRefused,
    adopt_or_open,
    revision_refusal,
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

router = APIRouter(dependencies=[Depends(require_internal_worker_token)])

_ACTIVE = frozenset({"approved", "launching", "running"})


def _refuse(code: str, message: str) -> NoReturn:
    raise HTTPException(
        status.HTTP_409_CONFLICT,
        {"code": f"publication.{code}", "message": message},
        headers={"Cache-Control": "no-store"},
    )


def _forge_failure(exc: ForgeError) -> HTTPException:
    """A fixed, payload-free answer for a code host failure."""

    reason = str(exc) or type(exc).__name__
    if isinstance(exc, Unauthorized):
        code, status_code = "code_host_credential_refused", status.HTTP_502_BAD_GATEWAY
    elif isinstance(exc, NotFound):
        code, status_code = "code_host_not_found", status.HTTP_404_NOT_FOUND
    elif isinstance(exc, Unavailable):
        code, status_code = "code_host_unavailable", status.HTTP_503_SERVICE_UNAVAILABLE
    else:
        code, status_code = "code_host_refused", status.HTTP_502_BAD_GATEWAY
    return HTTPException(
        status_code,
        {"code": f"publication.{code}", "message": f"the code host answered {reason}"},
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


def _repository(publication: Publication, lineage: ThreadPublicationLineage) -> RepositoryRef:
    return repository_ref(
        get_settings(),
        path=publication.repo_full_name,
        project_id=lineage.github_repository_id,
    )


def _code_host(request: Request) -> CodeHost:
    return code_host_for(get_settings(), request.app.state.http_client)


def _out(pull: PullRequest) -> PublicationPullRequestOut:
    return PublicationPullRequestOut(
        number=int(pull.ref.number),
        url=pull.url,
        state=pull.state.value,
        head_sha=pull.head_sha,
        head_ref=pull.head_ref,
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
    try:
        pull = await _code_host(request).read_pull_request(
            PullRequestRef(_repository(publication, lineage), str(pr_number))
        )
    except ForgeError as exc:
        raise _forge_failure(exc) from None
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
    try:
        head = await _code_host(request).branch_head(
            _repository(publication, lineage), lineage.branch
        )
    except ForgeError as exc:
        raise _forge_failure(exc) from None
    return PublicationBranchHeadOut(head_sha=head)


@router.post("/{publication_id}/revision-commit", response_model=PublicationRevisionOut)
async def verify_publication_revision(
    publication_id: uuid.UUID,
    data: PublicationRevisionVerify,
    request: Request,
    session: SessionDep,
    response: Response,
) -> PublicationRevisionOut:
    """Prove a remote commit is this revision's marked commit on its expected parent."""

    response.headers["Cache-Control"] = "no-store"
    publication, lineage = await _stored(session, publication_id)
    if data.revision_id != publication.id or data.expected_parent != (
        publication.expected_prior_head
    ):
        _refuse("revision_mismatch", "the verification names another publication revision")
    try:
        commit = await _code_host(request).read_commit(
            _repository(publication, lineage), data.commit_sha
        )
    except ForgeError as exc:
        raise _forge_failure(exc) from None
    refusal = revision_refusal(
        commit,
        commit_sha=data.commit_sha,
        revision_id=publication.id,
        expected_parent=data.expected_parent,
    )
    if refusal is not None:
        _refuse("revision_mismatch", refusal)
    return PublicationRevisionOut(commit_sha=commit.sha)


async def _base_ref(
    session: SessionDep, code_host: CodeHost, publication: Publication, repository: RepositoryRef
) -> str:
    """The admitted WorkItem's base, else the repository's default branch."""

    if publication.execution_request_id is not None:
        base = await session.scalar(
            select(WorkItem.base_branch)
            .join(ExecutionRequest, ExecutionRequest.work_item_id == WorkItem.id)
            .where(ExecutionRequest.id == publication.execution_request_id)
        )
        if base:
            return base
    default_branch = (await resolve_stored(code_host, repository)).default_branch
    if not default_branch:
        raise Unavailable("default_branch_unknown")
    return default_branch


@router.post(
    "/{publication_id}/pull-request",
    response_model=PublicationPullRequestOut,
    responses={204: {"description": "The publication branch does not exist"}},
)
async def recover_publication_pull_request(
    publication_id: uuid.UUID,
    data: PublicationPullRecover,
    request: Request,
    session: SessionDep,
    response: Response,
) -> PublicationPullRequestOut | Response:
    """Adopt the branch's pull request, or open it when the branch exists.

    The contract is the stored publication's title, body and draft flag, the
    lineage branch, and the admitted WorkItem's base or else the repository's
    default branch (`curie_api.publication_pulls.adopt_or_open`).
    """

    response.headers["Cache-Control"] = "no-store"
    publication, lineage = await _stored(session, publication_id)
    if publication.status not in _ACTIVE:
        _refuse("not_approved", "the publication is not approved for a pull request")
    if lineage.status != "open":
        _refuse("lineage_terminal", "the pull request for this thread is merged or closed")
    code_host = _code_host(request)
    repository = _repository(publication, lineage)
    try:
        contract = PullRequestContract(
            branch=lineage.branch,
            base=await _base_ref(session, code_host, publication, repository),
            title=publication.title,
            body=publication.body,
            draft=publication.open_as_draft,
        )
        pull = await adopt_or_open(
            code_host, repository, contract, expected_head_sha=data.expected_head_sha
        )
    except PullRequestRefused as refused:
        _refuse(refused.code, refused.message)
    except ForgeError as exc:
        raise _forge_failure(exc) from None
    if pull is None:
        return Response(status_code=204, headers={"Cache-Control": "no-store"})
    return _out(pull)
