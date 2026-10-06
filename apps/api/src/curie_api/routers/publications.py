"""Durable approval-gated publication control plane.

The API stores private patch state and resolves credentials. Kubernetes and
GitHub side effects belong to the trusted worker publication reconciler.
"""

import asyncio
import logging
import re
import time
import uuid
from typing import Any, Literal, cast

from aci_protocol import PublicationContext
from curie_telemetry import TRACEPARENT_STREAM_FIELD, canonicalize_traceparent
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from curie_api.crud import agents as crud_agents
from curie_api.crud import approvals as crud_approvals
from curie_api.crud import deployments as crud_deployments
from curie_api.crud import errors as crud_errors
from curie_api.crud import lineages as crud_lineages
from curie_api.crud import publication_queries as crud_publication_queries
from curie_api.crud import publications as crud_publications
from curie_api.crud import workspaces as crud_workspaces
from curie_api.schemas.publications import (
    PublicationContextMint,
    PublicationCreate,
    PublicationLineageAdvance,
    PublicationLineageOut,
    PublicationOut,
    ReviewRevisionCancel,
    ReviewRevisionOut,
    ReviewRevisionReserve,
)
from curie_api.schemas.workspaces import RepositoryCredentialOut

from .. import factory_ci, factory_progress
from ..auth import (
    require_api_key,
    require_internal_worker_token,
)
from ..config import get_settings
from ..deps import SessionDep
from ..forges.errors import ForgeError, Unauthorized, Unavailable
from ..forges.hosts import code_host_for, pull_request_ref
from ..forges.types import GITHUB, CredentialScope
from ..models import (
    ExecutionRequest,
    Publication,
    PublicationReviewReservation,
    ThreadPublicationLineage,
)
from ..publication_authority import (
    AuthorityRefused,
    AuthorityUnavailable,
    PublicationRemoteTerminal,
    verify_publication_identity,
)
from ..publication_policy import policy_still_authorizes
from ..publication_precheck_token import PublicationPrecheckClaims, metadata_digest, mint
from ..publication_truth import (
    PRECHECK_TIMEOUT_SECONDS,
    PublicationPrecheckRefused,
    PublicationPrecheckUnavailable,
    read_publication_authority,
    read_publication_metadata,
)
from ..repository_access import issue_repository_credential
from ..workspace_policy import credential_mode, repository_is_allowed
from .publication_precheck import precheck_error

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/publications",
    tags=["publications"],
    dependencies=[Depends(require_api_key)],
)
internal_router = APIRouter(prefix="/v1/internal/publications", tags=["internal-publications"])

# Code host reasons for a pull request whose facts do not match the request.
_INVALID_PULL_REASONS = frozenset({"malformed_response", "pull_request_mismatch"})
_FULL_COMMIT_SHA = re.compile(r"[0-9a-fA-F]{40}")
_GITHUB_UNAVAILABLE_DETAIL = {
    "code": "publication.github_unavailable",
    "message": (
        "GitHub could not verify this thread's pull request. Try again later; "
        "no model turn or publication was started."
    ),
}


@internal_router.post(
    "/precheck/context",
    response_model=PublicationContext,
    responses={204: {"description": "The running execution has no existing pull request"}},
    dependencies=[Depends(require_internal_worker_token)],
)
async def mint_publication_context(
    data: PublicationContextMint,
    request: Request,
    response: Response,
    session: SessionDep,
) -> PublicationContext | Response:
    response.headers["Cache-Control"] = "no-store"
    settings = get_settings()
    try:
        async with asyncio.timeout(PRECHECK_TIMEOUT_SECONDS):
            authority = await read_publication_authority(
                session,
                github_html_base=settings.github_html_base,
                deployment_id=data.deployment_id,
                work_item_id=data.work_item_id,
                execution_request_id=data.execution_request_id,
                runtime_epoch=data.runtime_epoch,
            )
            if authority is None:
                return Response(status_code=204, headers={"Cache-Control": "no-store"})
            if authority.has_inflight_push:
                raise PublicationPrecheckUnavailable
            metadata = await read_publication_metadata(
                authority, settings=settings, client=request.app.state.http_client
            )
            current = await read_publication_authority(
                session,
                github_html_base=settings.github_html_base,
                deployment_id=data.deployment_id,
                work_item_id=data.work_item_id,
                execution_request_id=data.execution_request_id,
                runtime_epoch=data.runtime_epoch,
            )
            if current is None:
                raise PublicationPrecheckRefused
            if current.has_inflight_push:
                raise PublicationPrecheckUnavailable
            if current != authority or int(current.execution_deadline.timestamp()) <= time.time():
                raise PublicationPrecheckRefused
    except PublicationPrecheckRefused:
        raise precheck_error(
            409, "invalid_context", "publication execution authority is no longer current"
        ) from None
    except (PublicationPrecheckUnavailable, TimeoutError):
        raise precheck_error(
            503, "precheck_unavailable", "current publication metadata could not be verified"
        ) from None
    claims = PublicationPrecheckClaims(
        scope="publication.precheck",
        agent_id=authority.agent_id,
        deployment_id=authority.deployment_id,
        work_item_id=authority.work_item_id,
        execution_request_id=authority.execution_request_id,
        runtime_epoch=authority.runtime_epoch,
        conversation_id=authority.conversation_id,
        lineage_id=authority.lineage_id,
        lineage_version=authority.lineage_version,
        expected_head=authority.expected_head,
        queued_event_id=data.queued_event_id,
        observed_title_sha256=metadata_digest(metadata.title),
        observed_body_sha256=metadata_digest(metadata.body),
        observed_at=metadata.observed_at,
        iat=int(metadata.observed_at.timestamp()),
        exp=int(authority.execution_deadline.timestamp()),
    )
    return PublicationContext(
        agent_id=claims.agent_id,
        deployment_id=claims.deployment_id,
        work_item_id=claims.work_item_id,
        execution_request_id=claims.execution_request_id,
        runtime_epoch=claims.runtime_epoch,
        conversation_id=claims.conversation_id,
        lineage_id=claims.lineage_id,
        lineage_version=claims.lineage_version,
        expected_head=claims.expected_head,
        queued_event_id=claims.queued_event_id,
        observed_title=metadata.title,
        observed_body_sha256=claims.observed_body_sha256,
        observed_at=metadata.observed_at,
        precheck_url=str(request.url_for("compare_publication_metadata")),
        capability=mint(settings.api_key, claims),
    )


async def _publication_lineage_out(
    session: SessionDep,
    lineage: ThreadPublicationLineage,
) -> PublicationLineageOut:
    """Render the one safe private-state fact alongside public lineage data."""

    has_pending_revision = await crud_lineages.publication_lineage_has_pending_revision(
        session, lineage
    )
    has_pending_outcome = await crud_lineages.publication_lineage_has_pending_outcome(
        session, lineage
    )
    visible_outcome_revision = await crud_lineages.publication_lineage_visible_outcome_revision(
        session, lineage
    )
    return PublicationLineageOut.model_validate(lineage).model_copy(
        update={
            "has_pending_revision": has_pending_revision,
            "has_pending_outcome": has_pending_outcome,
            "visible_outcome_revision": visible_outcome_revision,
        }
    )


async def _refresh_publication_lineage_from_github(
    request: Request,
    session: SessionDep,
    lineage: ThreadPublicationLineage,
) -> ThreadPublicationLineage:
    """Refresh a stored PR by number through the code host; credentials stay API-private."""

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
    code_host = code_host_for(settings, request.app.state.http_client)
    try:
        pull = await code_host.read_pull_request(
            pull_request_ref(
                settings,
                path=lineage.repo_full_name,
                project_id=lineage.repository_project_id,
                number=lineage.pr_number,
            )
        )
    except Unauthorized as exc:
        if str(exc) == "credential_unresolved":
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                {
                    "code": "publication.github_unavailable",
                    "message": "operator repository credential could not be resolved",
                },
            ) from None
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, _GITHUB_UNAVAILABLE_DETAIL
        ) from None
    except Unavailable as exc:
        if str(exc) not in _INVALID_PULL_REASONS:
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, _GITHUB_UNAVAILABLE_DETAIL
            ) from None
        pull = None
    except ForgeError:
        # A missing PR, rejected credential, rate limit, redirect, or upstream
        # failure is not verified-open lineage. Keep the durable row unchanged
        # and make the caller refuse this turn before route adoption/model use.
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, _GITHUB_UNAVAILABLE_DETAIL
        ) from None
    expected_url = f"{settings.github_html_base}/{lineage.repo_full_name}/pull/{lineage.pr_number}"
    if (
        pull is None
        or pull.url != lineage.pr_url
        or lineage.pr_url != expected_url
        or pull.head_ref != lineage.branch
        or _FULL_COMMIT_SHA.fullmatch(pull.head_sha) is None
    ):
        raise HTTPException(
            status.HTTP_502_BAD_GATEWAY,
            {
                "code": "publication.github_invalid_response",
                "message": "GitHub returned pull request facts that do not match the lineage",
            },
        )
    remote_state, actual_head_sha = pull.state.value, pull.head_sha.lower()

    if actual_head_sha != lineage.head_sha and (
        await crud_lineages.publication_lineage_has_inflight_push(session, lineage)
    ):
        # The authorized revision may have pushed its exact commit while its
        # lineage CAS is still pending. Keep the durable expected head as the
        # authority until that writer proves and records the new commit.
        return lineage

    expected_head_sha = lineage.head_sha
    if expected_head_sha is None:
        try:
            lineage = await crud_lineages.initialize_publication_lineage_head(
                session,
                lineage,
                expected_version=lineage.version,
                head_sha=actual_head_sha,
            )
        except crud_errors.PublicationLineageConflict as exc:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                {"code": exc.code, "message": exc.message},
            ) from exc
        expected_head_sha = lineage.head_sha
    assert expected_head_sha is not None
    if remote_state in ("merged", "closed") and lineage.status == "open":
        try:
            lineage = await crud_lineages.mark_publication_lineage_terminal(
                session,
                lineage,
                expected_version=lineage.version,
                expected_head_sha=expected_head_sha,
                state=remote_state,
            )
        except crud_errors.PublicationLineageConflict as exc:
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
    settings = get_settings()
    patch_limit_bytes = settings.publication_patch_max_bytes
    if len(patch) > patch_limit_bytes:
        raise HTTPException(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            f"publication patch exceeds the {patch_limit_bytes}-byte limit",
        )
    traceparent = canonicalize_traceparent(request.headers.get(TRACEPARENT_STREAM_FIELD))

    if data.work_item_request_id is not None:
        prior_paths = (
            await session.scalars(
                select(Publication.changed_paths).where(
                    Publication.execution_request_id == data.work_item_request_id,
                    Publication.status == "succeeded",
                )
            )
        ).all()
        changed_paths = [
            path for paths in prior_paths for path in paths
        ] + data.changed_paths
        python_ci = factory_ci.python_ci_policy(settings, data.repo_full_name)
        unselected = factory_ci.unselected_python_path(changed_paths, python_ci)
        if unselected is not None:
            raise HTTPException(
                status.HTTP_409_CONFLICT,
                {
                    "code": "publication.required_python_ci_unselected",
                    "message": f"required Python CI does not select {unselected}",
                },
            )
        if factory_ci.python_paths(changed_paths):
            try:
                observations = await factory_progress.read_verification_observations(
                    session, data.work_item_request_id
                )
            except ValueError as exc:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    {
                        "code": "publication.verification_preflight_unreadable",
                        "message": "stored verification preflight is unreadable",
                    },
                ) from exc
            if not observations:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    {
                        "code": "publication.verification_preflight_missing",
                        "message": "verification preflight observation is missing",
                    },
                )
            # A Python check can be declared under any id, so every stored check
            # counts: any failure refuses a Python change and any unavailable check
            # stamps the unavailable disclosure. Only when nothing was unavailable
            # does a missing ``python`` check stamp the not-declared pair.
            if factory_progress.failed_verification(observations) is not None:
                raise HTTPException(
                    status.HTTP_409_CONFLICT,
                    {
                        "code": "publication.verification_preflight_failed",
                        "message": "verification preflight failed; rerun after fixing the failure",
                    },
                )
            pending_proof = (
                f"{python_ci.check} had not reported when this pull request was opened; "
                "the issue status comment reports its result."
                if python_ci is not None
                else (
                    "Repository CI had not reported when this pull request was opened; "
                    "the issue status comment reports its result."
                )
            )
            statements: tuple[str, ...] = ()
            if any(observation.outcome == "unavailable" for observation in observations):
                statements = (
                    "In-sandbox verification was unavailable.",
                    pending_proof,
                )
            elif factory_progress.python_verification(observations) is None:
                statements = (
                    "The repository declares no in-sandbox verification check with id `python`.",
                    pending_proof,
                )
            body = data.body or ""
            missing = [statement for statement in statements if statement not in body]
            if missing:
                body = f"{body.rstrip()}\n\n{'\n'.join(missing)}"
                data = data.model_copy(update={"body": body})

    async def metadata_check() -> None:
        if patch:
            return
        if (
            data.work_item_request_id is None
            or data.work_item_runtime_epoch is None
            or data.observed_title is None
            or data.observed_body_sha256 is None
            or data.observed_lineage_id is None
            or data.observed_lineage_version is None
            or data.title is None
            or data.body is None
        ):
            raise crud_errors.PublicationLineageConflict(
                "publication.metadata_context_required",
                "metadata-only publication requires a current factory observation",
            )
        execution = await session.get(ExecutionRequest, data.work_item_request_id)
        if execution is None:
            raise PublicationPrecheckRefused
        authority = await read_publication_authority(
            session,
            github_html_base=settings.github_html_base,
            deployment_id=data.deployment_id,
            work_item_id=execution.work_item_id,
            execution_request_id=data.work_item_request_id,
            runtime_epoch=data.work_item_runtime_epoch,
        )
        if (
            authority is None
            or authority.has_inflight_push
            or authority.conversation_id != data.conversation_id
            or authority.repo_full_name.casefold() != data.repo_full_name.casefold()
            or authority.lineage_id != data.observed_lineage_id
            or authority.lineage_version != data.observed_lineage_version
            or authority.expected_head != data.base_sha
        ):
            raise PublicationPrecheckRefused
        metadata = await read_publication_metadata(
            authority, settings=settings, client=request.app.state.http_client
        )
        current = await read_publication_authority(
            session,
            github_html_base=settings.github_html_base,
            deployment_id=data.deployment_id,
            work_item_id=execution.work_item_id,
            execution_request_id=data.work_item_request_id,
            runtime_epoch=data.work_item_runtime_epoch,
        )
        if current != authority:
            raise PublicationPrecheckRefused
        if (
            metadata_digest(metadata.title) != metadata_digest(data.observed_title)
            or metadata_digest(metadata.body) != data.observed_body_sha256
        ):
            raise PublicationPrecheckRefused
        if (metadata.title, metadata.body) == (data.title, data.body):
            raise crud_errors.PublicationLineageConflict(
                "publication.no_change",
                "neither files nor pull request metadata changed",
            )

    try:
        publication, created = await crud_publications.create_publication(
            session,
            data,
            patch=patch,
            metadata_check=metadata_check,
            traceparent=traceparent,
        )
    except PublicationPrecheckRefused as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {
                "code": "publication.metadata_stale",
                "message": "pull request metadata or execution authority changed",
            },
        ) from exc
    except (PublicationPrecheckUnavailable, TimeoutError) as exc:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            {
                "code": "publication.metadata_unavailable",
                "message": "current pull request metadata could not be verified",
            },
        ) from exc
    except crud_errors.PublicationReplayConflict as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except crud_errors.PublicationLineageConflict as exc:
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
        lineage = await crud_lineages.get_thread_publication_lineage(
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
    settings = get_settings()
    try:
        publication = await crud_publication_queries.get_publication(session, publication_id)
        if publication is None or publication.lineage is None:
            raise LookupError("publication lineage not found")
        conflict = crud_lineages.publication_lineage_outcome_conflict(
            publication,
            publication.lineage,
            data,
            github_html_base=settings.github_html_base,
        )
        if conflict is not None:
            raise conflict
        identity = await verify_publication_identity(
            publication.lineage,
            data,
            settings,
            request.app.state.http_client,
        )
        lineage = await crud_lineages.advance_publication_lineage(
            session,
            publication_id,
            data,
            github_html_base=settings.github_html_base,
            identity=identity,
        )
    except PublicationRemoteTerminal as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {
                "code": "publication.lineage_terminal",
                "message": (
                    "the pull request for this thread is merged or closed; start a new thread"
                ),
                "observed_state": exc.state,
            },
        ) from None
    except AuthorityUnavailable:
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            _GITHUB_UNAVAILABLE_DETAIL,
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
                "message": ("another lineage already owns this immutable GitHub identity"),
            },
        ) from None
    except LookupError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc)) from exc
    except crud_errors.PublicationLineageConflict as exc:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {"code": exc.code, "message": exc.message},
        ) from exc
    await _replay_held_review_feedback(request, lineage)
    return await _publication_lineage_out(session, lineage)


async def _replay_held_review_feedback(
    request: Request, lineage: ThreadPublicationLineage
) -> None:
    """Admit review feedback held while this lineage awaited identity (#2962).

    Best effort: the reconciler retries every pass, so a failure here only
    delays the review and never fails the committed lineage advance.
    """
    if (
        not get_settings().github_review_ingress_enabled
        or lineage.status != "open"
        or lineage.code_host_kind != GITHUB
        or lineage.repository_project_id is None
        or lineage.pr_number is None
    ):
        return
    try:
        async with asyncio.timeout(10):
            await request.app.state.github_review_reconciler.replay_held(
                repository_id=int(lineage.repository_project_id),
                pr_number=lineage.pr_number,
            )
    except Exception:  # noqa: BLE001 - existing broad catch retained
        logger.warning("held GitHub review replay after identity failed; reconciler retries")


@router.get("", response_model=list[PublicationOut])
async def list_publications(session: SessionDep, limit: int = 100) -> list[PublicationOut]:
    rows = await crud_publication_queries.list_publications(session, limit=min(max(limit, 1), 200))
    return [PublicationOut.model_validate(row) for row in rows]


@router.get("/{publication_id}", response_model=PublicationOut)
async def get_publication(publication_id: uuid.UUID, session: SessionDep) -> PublicationOut:
    publication = await crud_publication_queries.get_publication(session, publication_id)
    if publication is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "publication not found")
    return PublicationOut.model_validate(publication)


def _credential_issue_detail(settings: Any, approval: Any) -> str:
    """Name the credential mode, and the policy when the platform resolved it."""

    detail = "server-derived repository credential issued via " + credential_mode(
        app_id=settings.github_app_id,
        app_private_key=settings.github_app_private_key,
        token=settings.github_token,
    )
    identity = getattr(approval, "policy_identity", None)
    if identity:
        detail += f" under {identity} version {approval.policy_version} by {approval.resolved_by}"
    return detail


@internal_router.post(
    "/{publication_id}/credential",
    response_model=RepositoryCredentialOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def redeem_publication_credential(
    publication_id: uuid.UUID,
    request: Request,
    session: SessionDep,
    response: Response,
) -> RepositoryCredentialOut:
    response.headers["Cache-Control"] = "no-store"
    publication = await crud_publication_queries.get_publication(session, publication_id)
    repo = publication.repo_full_name if publication is not None else None
    deployment_id = publication.deployment_id if publication is not None else None

    async def refused(code: int, detail: str | dict[str, str]) -> None:
        audit_detail = detail if isinstance(detail, str) else detail["message"]
        await crud_publications.append_credential_redemption_audit(
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
    deployment = await crud_deployments.get_deployment(session, publication.deployment_id)
    approval = await crud_approvals.get_approval(session, publication.approval_id)
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
    selected = await crud_workspaces.get_thread_workspace(
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
    agent = await crud_agents.get_agent(session, deployment.agent_id)
    if not policy_still_authorizes(agent, approval):
        await refused(
            status.HTTP_409_CONFLICT,
            {
                "code": "publication.policy_revoked",
                "message": "publication policy no longer authorizes this approval",
            },
        )
    cancelled = await crud_publications.publication_cancellation_conflict(
        session,
        agent_id=deployment.agent_id,
        conversation_id=workspace_conversation_id,
    )
    if cancelled is not None:
        await refused(
            status.HTTP_409_CONFLICT,
            {"code": cancelled.code, "message": cancelled.message},
        )
    try:
        issued = await issue_repository_credential(
            settings,
            request.app.state.http_client,
            repo_full_name=repo,
            project_id=(
                publication.lineage.repository_project_id
                if publication.lineage is not None
                else None
            ),
            scope=CredentialScope.PUSH,
        )
    except Exception as exc:
        await crud_publications.append_credential_redemption_audit(
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
    await crud_publications.append_credential_redemption_audit(
        session,
        purpose="publication_push",
        outcome="issued",
        deployment_id=publication.deployment_id,
        publication_id=publication.id,
        repo_full_name=repo,
        detail=_credential_issue_detail(settings, approval),
    )
    return RepositoryCredentialOut(
        repo_full_name=repo,
        clone_url=issued.clone_url,
        authorization_header=issued.authorization_header,
        origin=issued.origin,
        header_form=issued.header_form,
        ca_bundle_ref=issued.ca_bundle_ref,
    )


def _review_revision_out(
    row: PublicationReviewReservation,
    lineage: ThreadPublicationLineage,
) -> ReviewRevisionOut:
    assert lineage.reply_conversation_id is not None
    assert lineage.repository_project_id is not None
    assert lineage.code_host_installation_id is not None
    assert lineage.code_host_pr_id is not None
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
        repository_id=int(lineage.repository_project_id),
        installation_id=lineage.code_host_installation_id,
        pr_node_id=lineage.code_host_pr_id,
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
    data: ReviewRevisionReserve,
    session: SessionDep,
    response: Response,
) -> ReviewRevisionOut:
    response.headers["Cache-Control"] = "no-store"
    try:
        row, lineage, created = await crud_lineages.reserve_review_revision(session, data)
        await session.commit()
    except crud_errors.PublicationLineageConflict as exc:
        await session.rollback()
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {"code": exc.code, "message": exc.message},
        ) from None
    response.status_code = 201 if created else 200
    return _review_revision_out(row, lineage)


@internal_router.post(
    "/review-reservations/{reservation_id}/cancel",
    response_model=ReviewRevisionOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def cancel_review_revision(
    reservation_id: uuid.UUID,
    data: ReviewRevisionCancel,
    session: SessionDep,
    response: Response,
) -> ReviewRevisionOut:
    response.headers["Cache-Control"] = "no-store"
    try:
        row = await crud_lineages.cancel_review_revision(
            session,
            reservation_id,
            origin_key=data.origin_key,
            expected_version=data.expected_version,
        )
        lineage = await session.get(
            ThreadPublicationLineage,
            row.lineage_id,
            populate_existing=True,
        )
        assert lineage is not None
        await session.commit()
    except LookupError:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "review reservation not found",
        ) from None
    except crud_errors.PublicationLineageConflict as exc:
        await session.rollback()
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {"code": exc.code, "message": exc.message},
        ) from None
    return _review_revision_out(row, lineage)

