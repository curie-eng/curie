"""Durable approval-gated publication control plane.

The API stores private patch state and resolves credentials. Kubernetes and
GitHub side effects belong to the trusted worker publication reconciler.
"""

import re
import uuid
from typing import Any, Literal, cast

import httpx
from curie_telemetry import TRACEPARENT_STREAM_FIELD, canonicalize_traceparent
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy.exc import IntegrityError
from starlette.concurrency import run_in_threadpool

from .. import crud
from ..auth import (
    require_api_key,
    require_internal_worker_token,
)
from ..config import get_settings
from ..deps import SessionDep
from ..models import PublicationReviewReservation, ThreadPublicationLineage
from ..publication_authority import (
    AuthorityRefused,
    AuthorityUnavailable,
    verify_publication_identity,
)
from ..repo_full_name import repo_url_path
from ..repository_auth import resolve_repository_credential
from ..schemas import (
    PublicationCreate,
    PublicationLineageAdvance,
    PublicationLineageOut,
    PublicationOut,
    RepositoryCredentialOut,
    ReviewRevisionCancel,
    ReviewRevisionOut,
    ReviewRevisionReserve,
)
from ..workspace_policy import credential_mode, repository_is_allowed

router = APIRouter(
    prefix="/publications",
    tags=["publications"],
    dependencies=[Depends(require_api_key)],
)
internal_router = APIRouter(prefix="/v1/internal/publications", tags=["internal-publications"])

_GITHUB_API_VERSION = "2022-11-28"
_FULL_COMMIT_SHA = re.compile(r"[0-9a-fA-F]{40}")
_GITHUB_UNAVAILABLE_DETAIL = {
    "code": "publication.github_unavailable",
    "message": (
        "GitHub could not verify this thread's pull request. Try again later; "
        "no model turn or publication was started."
    ),
}


async def _publication_lineage_out(
    session: SessionDep,
    lineage: ThreadPublicationLineage,
) -> PublicationLineageOut:
    """Render the one safe private-state fact alongside public lineage data."""

    has_pending_revision = await crud.publication_lineage_has_pending_revision(
        session, lineage
    )
    has_pending_outcome = await crud.publication_lineage_has_pending_outcome(
        session, lineage
    )
    visible_outcome_revision = (
        await crud.publication_lineage_visible_outcome_revision(session, lineage)
    )
    return PublicationLineageOut.model_validate(lineage).model_copy(
        update={
            "has_pending_revision": has_pending_revision,
            "has_pending_outcome": has_pending_outcome,
            "visible_outcome_revision": visible_outcome_revision,
        }
    )


def _validated_github_pr_truth(
    payload: Any,
    *,
    repo_full_name: str,
    pr_number: int,
    pr_url: str,
    branch: str,
) -> tuple[str, str]:
    """Validate identity-bound GitHub fields and return state plus head SHA."""

    if not isinstance(payload, dict):
        raise ValueError("GitHub returned an invalid pull request body")
    number = payload.get("number")
    html_url = payload.get("html_url")
    remote_state = payload.get("state")
    merged = payload.get("merged")
    head = payload.get("head")
    expected_url = f"https://github.com/{repo_full_name}/pull/{pr_number}"
    if (
        not isinstance(number, int)
        or isinstance(number, bool)
        or number != pr_number
        or html_url != pr_url
        or pr_url != expected_url
        or not isinstance(head, dict)
        or head.get("ref") != branch
    ):
        raise ValueError("GitHub pull request identity differs from the stored lineage")
    actual_head_sha = head.get("sha")
    if not isinstance(actual_head_sha, str) or _FULL_COMMIT_SHA.fullmatch(
        actual_head_sha
    ) is None:
        raise ValueError("GitHub returned an invalid pull request head")
    if remote_state not in ("open", "closed") or not isinstance(merged, bool):
        raise ValueError("GitHub returned an invalid pull request state")
    if merged and remote_state != "closed":
        raise ValueError("GitHub returned an inconsistent pull request state")
    state = "merged" if merged else ("closed" if remote_state == "closed" else "open")
    return state, actual_head_sha.lower()


async def _refresh_publication_lineage_from_github(
    request: Request,
    session: SessionDep,
    lineage: ThreadPublicationLineage,
) -> ThreadPublicationLineage:
    """Refresh a stored PR by number while keeping credentials API-private."""

    if lineage.pr_number is None and lineage.pr_url is None:
        return lineage
    if lineage.pr_number is None or lineage.pr_url is None:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {
                "code": "publication.lineage_stale",
                "message": "stored pull request identity is incomplete",
            },
        )

    settings = get_settings()
    try:
        _, authorization_header = await run_in_threadpool(
            resolve_repository_credential, lineage.repo_full_name, settings
        )
    except Exception as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            {
                "code": "publication.github_unavailable",
                "message": "operator repository credential could not be resolved",
            },
        ) from exc

    url = (
        f"{settings.github_api_url.rstrip('/')}"
        f"/repos/{repo_url_path(lineage.repo_full_name)}/pulls/{lineage.pr_number}"
    )
    try:
        response = await request.app.state.http_client.get(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": authorization_header,
                "X-GitHub-Api-Version": _GITHUB_API_VERSION,
            },
            # This request carries operator authority. Refuse every redirect at
            # this callsite even when the shared client follows redirects for a
            # different API consumer.
            follow_redirects=False,
        )
    except httpx.HTTPError as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            _GITHUB_UNAVAILABLE_DETAIL,
        ) from exc
    if response.status_code != status.HTTP_200_OK:
        # A missing PR, rejected credential, rate limit, redirect, or upstream
        # failure is not verified-open lineage. Keep the durable row unchanged
        # and make the caller refuse this turn before route adoption/model use.
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            _GITHUB_UNAVAILABLE_DETAIL,
        )
    try:
        remote_state, actual_head_sha = _validated_github_pr_truth(
            response.json(),
            repo_full_name=lineage.repo_full_name,
            pr_number=lineage.pr_number,
            pr_url=lineage.pr_url,
            branch=lineage.branch,
        )
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            {
                "code": "publication.github_invalid_response",
                "message": "GitHub returned pull request facts that do not match the lineage",
            },
        ) from exc

    if actual_head_sha != lineage.head_sha and (
        await crud.publication_lineage_has_inflight_push(session, lineage)
    ):
        # The authorized revision may have pushed its exact commit while its
        # lineage CAS is still pending. Keep the durable expected head as the
        # authority until that writer proves and records the new commit.
        return lineage

    expected_head_sha = lineage.head_sha
    if expected_head_sha is None:
        try:
            lineage = await crud.initialize_publication_lineage_head(
                session,
                lineage,
                expected_version=lineage.version,
                head_sha=actual_head_sha,
            )
        except crud.PublicationLineageConflict as exc:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                {"code": exc.code, "message": exc.message},
            ) from exc
        expected_head_sha = lineage.head_sha
    assert expected_head_sha is not None
    if remote_state in ("merged", "closed") and lineage.status == "open":
        try:
            lineage = await crud.mark_publication_lineage_terminal(
                session,
                lineage,
                expected_version=lineage.version,
                expected_head_sha=expected_head_sha,
                state=remote_state,
            )
        except crud.PublicationLineageConflict as exc:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                {"code": exc.code, "message": exc.message},
            ) from exc
    if actual_head_sha != expected_head_sha:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {
                "code": "publication.lineage_stale",
                "message": "GitHub pull request head differs from the stored lineage",
                "expected_head_sha": expected_head_sha,
                "actual_head_sha": actual_head_sha,
            },
        )
    return lineage


@internal_router.post(
    "",
    response_model=PublicationOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_internal_worker_token)],
)
async def create_publication(
    data: PublicationCreate,
    request: Request,
    session: SessionDep,
    response: Response,
) -> PublicationOut:
    try:
        patch = data.decoded_patch()
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    patch_limit_bytes = get_settings().publication_patch_max_bytes
    if len(patch) > patch_limit_bytes:
        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            f"publication patch exceeds the {patch_limit_bytes}-byte limit",
        )
    traceparent = canonicalize_traceparent(
        request.headers.get(TRACEPARENT_STREAM_FIELD)
    )
    try:
        publication, created = await crud.create_publication(
            session,
            data,
            patch=patch,
            traceparent=traceparent,
        )
    except crud.PublicationReplayConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except crud.PublicationLineageConflict as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {"code": exc.code, "message": exc.message},
        ) from exc
    except LookupError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except Exception:
        await session.rollback()
        raise
    if not created:
        response.status_code = status.HTTP_200_OK
    return PublicationOut.model_validate(publication)


@internal_router.get(
    "/lineage",
    response_model=PublicationLineageOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def get_publication_lineage(
    deployment_id: uuid.UUID,
    conversation_id: str,
    repo_full_name: str,
    request: Request,
    session: SessionDep,
) -> PublicationLineageOut:
    try:
        lineage = await crud.get_thread_publication_lineage(
            session,
            deployment_id=deployment_id,
            conversation_id=conversation_id,
            repo_full_name=repo_full_name,
        )
    except LookupError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    if lineage is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "publication lineage not found")
    lineage = await _refresh_publication_lineage_from_github(request, session, lineage)
    return await _publication_lineage_out(session, lineage)


@internal_router.patch(
    "/{publication_id}/lineage",
    response_model=PublicationLineageOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def advance_publication_lineage(
    publication_id: uuid.UUID,
    data: PublicationLineageAdvance,
    session: SessionDep,
    request: Request,
) -> PublicationLineageOut:
    try:
        publication = await crud.get_publication(session, publication_id)
        if publication is None or publication.lineage is None:
            raise LookupError("publication lineage not found")
        identity = await verify_publication_identity(
            publication.lineage, data, get_settings(), request.app.state.http_client
        )
        lineage = await crud.advance_publication_lineage(
            session, publication_id, data, identity=identity
        )
    except AuthorityUnavailable:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, _GITHUB_UNAVAILABLE_DETAIL
        ) from None
    except AuthorityRefused:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {
                "code": "publication.lineage_stale",
                "message": "current GitHub publication identity was refused",
            },
        ) from None
    except IntegrityError:
        await session.rollback()
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {
                "code": "publication.revision_conflict",
                "message": "another lineage already owns this immutable GitHub identity",
            },
        ) from None
    except LookupError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc)) from exc
    except crud.PublicationLineageConflict as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {"code": exc.code, "message": exc.message},
        ) from exc
    return await _publication_lineage_out(session, lineage)


@router.get("", response_model=list[PublicationOut])
async def list_publications(session: SessionDep, limit: int = 100) -> list[PublicationOut]:
    rows = await crud.list_publications(session, limit=min(max(limit, 1), 200))
    return [PublicationOut.model_validate(row) for row in rows]


@router.get("/{publication_id}", response_model=PublicationOut)
async def get_publication(
    publication_id: uuid.UUID, session: SessionDep
) -> PublicationOut:
    publication = await crud.get_publication(session, publication_id)
    if publication is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "publication not found")
    return PublicationOut.model_validate(publication)


@internal_router.post(
    "/{publication_id}/credential",
    response_model=RepositoryCredentialOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def redeem_publication_credential(
    publication_id: uuid.UUID,
    session: SessionDep,
    response: Response,
) -> RepositoryCredentialOut:
    response.headers["Cache-Control"] = "no-store"
    publication = await crud.get_publication(session, publication_id)
    repo = publication.repo_full_name if publication is not None else None
    deployment_id = publication.deployment_id if publication is not None else None

    async def refused(code: int, detail: str | dict[str, str]) -> None:
        audit_detail = detail if isinstance(detail, str) else detail["message"]
        await crud.append_credential_redemption_audit(
            session,
            purpose="publication_push",
            outcome="refused",
            deployment_id=deployment_id,
            publication_id=publication.id if publication is not None else None,
            repo_full_name=repo,
            detail=audit_detail,
        )
        raise HTTPException(code, detail, headers={"Cache-Control": "no-store"})

    if publication is None:
        await refused(status.HTTP_404_NOT_FOUND, "publication not found")
    assert publication is not None and repo is not None
    if publication.lineage is not None and publication.lineage.status != "open":
        await refused(
            status.HTTP_409_CONFLICT,
            {
                "code": "publication.lineage_terminal",
                "message": (
                    "the pull request for this thread is merged or closed; start a new thread"
                ),
            },
        )
    if publication.status not in ("approved", "launching", "running"):
        await refused(
            status.HTTP_409_CONFLICT,
            "publication must be approved before a write credential can be redeemed",
        )
    deployment = await crud.get_deployment(session, publication.deployment_id)
    approval = await crud.get_approval(session, publication.approval_id)
    if deployment is None or approval is None:
        await refused(status.HTTP_409_CONFLICT, "publication workspace binding is absent")
    assert deployment is not None and approval is not None
    workspace_conversation_id = (
        publication.workspace_conversation_id
        if publication.workspace_conversation_id is not None
        else publication.lineage.conversation_id
        if publication.lineage is not None
        else None
    )
    if workspace_conversation_id is None:
        await refused(
            status.HTTP_409_CONFLICT,
            "publication canonical workspace identity is absent",
        )
    assert workspace_conversation_id is not None
    selected = await crud.get_thread_workspace(
        session,
        agent_id=deployment.agent_id,
        conversation_id=workspace_conversation_id,
    )
    settings = get_settings()
    if (
        selected is None
        or selected.repo_full_name.casefold() != repo.casefold()
        or not repository_is_allowed(repo, settings.github_repo_allowlist)
    ):
        await refused(
            status.HTTP_403_FORBIDDEN,
            "publication repository is no longer authorized for this thread",
        )
    try:
        clone_url, authorization_header = await run_in_threadpool(
            resolve_repository_credential, repo, settings
        )
    except Exception as exc:
        await crud.append_credential_redemption_audit(
            session,
            purpose="publication_push",
            outcome="refused",
            deployment_id=publication.deployment_id,
            publication_id=publication.id,
            repo_full_name=repo,
            detail="operator credential resolution failed",
        )
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            "operator repository credential could not be resolved",
            headers={"Cache-Control": "no-store"},
        ) from exc
    await crud.append_credential_redemption_audit(
        session,
        purpose="publication_push",
        outcome="issued",
        deployment_id=publication.deployment_id,
        publication_id=publication.id,
        repo_full_name=repo,
        detail=(
            "server-derived repository credential issued via "
            + credential_mode(
                app_id=settings.github_app_id,
                app_private_key=settings.github_app_private_key,
                token=settings.github_token,
            )
        ),
    )
    return RepositoryCredentialOut(
        repo_full_name=repo,
        clone_url=clone_url,
        authorization_header=authorization_header,
    )


def _review_revision_out(
    row: PublicationReviewReservation, lineage: ThreadPublicationLineage
) -> ReviewRevisionOut:
    assert lineage.reply_conversation_id is not None
    assert lineage.github_repository_id is not None
    assert lineage.github_installation_id is not None
    assert lineage.github_pr_node_id is not None
    assert lineage.pr_number is not None
    assert lineage.base_ref is not None
    return ReviewRevisionOut(
        revision_id=row.id,
        lineage_id=lineage.id,
        agent_id=lineage.agent_id,
        conversation_id=lineage.conversation_id,
        reply_conversation_id=lineage.reply_conversation_id,
        binding_id=row.binding_id,
        binding_generation=row.binding_generation,
        repository_id=lineage.github_repository_id,
        installation_id=lineage.github_installation_id,
        pr_node_id=lineage.github_pr_node_id,
        base_ref=lineage.base_ref,
        repo_full_name=lineage.repo_full_name,
        pr_number=lineage.pr_number,
        branch=lineage.branch,
        base_sha=lineage.base_sha,
        expected_head_sha=row.expected_head_sha,
        lineage_version=row.lineage_version,
        revision_number=row.revision_number,
        version=row.version,
        status=cast(Literal["reserved", "consumed", "cancelled"], row.status),
    )


@internal_router.post(
    "/review-reservations",
    response_model=ReviewRevisionOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def reserve_review_revision(
    data: ReviewRevisionReserve, session: SessionDep, response: Response
) -> ReviewRevisionOut:
    response.headers["Cache-Control"] = "no-store"
    try:
        row, lineage, created = await crud.reserve_review_revision(session, data)
        await session.commit()
    except crud.PublicationLineageConflict as exc:
        await session.rollback()
        raise HTTPException(
            status.HTTP_409_CONFLICT, {"code": exc.code, "message": exc.message}
        ) from None
    response.status_code = 201 if created else 200
    return _review_revision_out(row, lineage)


@internal_router.post(
    "/review-reservations/{reservation_id}/cancel",
    response_model=ReviewRevisionOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def cancel_review_revision(
    reservation_id: uuid.UUID, data: ReviewRevisionCancel, session: SessionDep, response: Response
) -> ReviewRevisionOut:
    response.headers["Cache-Control"] = "no-store"
    try:
        row = await crud.cancel_review_revision(
            session,
            reservation_id,
            origin_key=data.origin_key,
            expected_version=data.expected_version,
        )
        lineage = await session.get(ThreadPublicationLineage, row.lineage_id)
        assert lineage is not None
        await session.commit()
    except LookupError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "review reservation not found") from None
    except crud.PublicationLineageConflict as exc:
        await session.rollback()
        raise HTTPException(
            status.HTTP_409_CONFLICT, {"code": exc.code, "message": exc.message}
        ) from None
    return _review_revision_out(row, lineage)
