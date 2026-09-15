"""Worker-only repository selection and credential redemption."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, Response, status
from starlette.concurrency import run_in_threadpool

from .. import crud
from ..auth import require_internal_worker_token
from ..config import get_settings
from ..deps import SessionDep
from ..models import Deployment
from ..repository_auth import resolve_repository_credential
from ..schemas import (
    RepositoryCredentialOut,
    WorkspaceCredentialRequest,
    WorkspaceSelectionOut,
    WorkspaceSelectionRequest,
)
from ..workspace_policy import credential_mode, repository_is_allowed

router = APIRouter(prefix="/v1/internal/workspaces", tags=["internal-workspaces"])


async def _workspace_deployment(
    session: SessionDep, deployment_id: uuid.UUID
) -> Deployment:
    deployment = await crud.get_deployment(session, deployment_id)
    if deployment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "deployment not found")
    return deployment


def _require_allowed(repo_full_name: str) -> None:
    if not repository_is_allowed(
        repo_full_name, get_settings().github_repo_allowlist
    ):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "repository is not allowed for runtime workspaces",
        )


@router.post(
    "/{deployment_id}/selection",
    response_model=WorkspaceSelectionOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def select_workspace_repository(
    deployment_id: uuid.UUID,
    data: WorkspaceSelectionRequest,
    session: SessionDep,
) -> WorkspaceSelectionOut:
    """Select once per agent/thread, or validate and reuse the winner."""

    deployment = await _workspace_deployment(session, deployment_id)
    selected = await crud.get_thread_workspace(
        session,
        agent_id=deployment.agent_id,
        conversation_id=data.conversation_id,
    )
    if selected is None:
        if data.repo_full_name is None:
            return WorkspaceSelectionOut(repo_full_name=None)
        _require_allowed(data.repo_full_name)
        selected, _ = await crud.select_thread_workspace(
            session,
            agent_id=deployment.agent_id,
            deployment_id=deployment.id,
            conversation_id=data.conversation_id,
            repo_full_name=data.repo_full_name,
            selected_by=data.author,
        )
    if (
        data.repo_full_name is not None
        and selected.repo_full_name.casefold() != data.repo_full_name.casefold()
    ):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            {
                "code": "workspace.selection_conflict",
                "message": "This conversation already selected a different repository.",
            },
        )
    _require_allowed(selected.repo_full_name)
    return WorkspaceSelectionOut(
        repo_full_name=selected.repo_full_name,
        revision=selected.revision,
    )


@router.post(
    "/{deployment_id}/credential",
    response_model=RepositoryCredentialOut,
    dependencies=[Depends(require_internal_worker_token)],
)
async def redeem_workspace_credential(
    deployment_id: uuid.UUID,
    data: WorkspaceCredentialRequest,
    session: SessionDep,
    response: Response,
) -> RepositoryCredentialOut:
    response.headers["Cache-Control"] = "no-store"
    deployment = await crud.get_deployment(session, deployment_id)
    if deployment is None:
        await crud.append_credential_redemption_audit(
            session,
            purpose="workspace_clone",
            outcome="refused",
            deployment_id=None,
            publication_id=None,
            repo_full_name=None,
            detail="deployment not found",
        )
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            "deployment not found",
            headers={"Cache-Control": "no-store"},
        )
    selected = await crud.get_thread_workspace(
        session,
        agent_id=deployment.agent_id,
        conversation_id=data.conversation_id,
    )
    repo = selected.repo_full_name if selected is not None else None
    if repo is None:
        await crud.append_credential_redemption_audit(
            session,
            purpose="workspace_clone",
            outcome="refused",
            deployment_id=deployment.id,
            publication_id=None,
            repo_full_name=None,
            detail="conversation has no selected repository workspace",
        )
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "conversation has no selected repository workspace",
            headers={"Cache-Control": "no-store"},
        )
    settings = get_settings()
    if not repository_is_allowed(repo, settings.github_repo_allowlist):
        await crud.append_credential_redemption_audit(
            session,
            purpose="workspace_clone",
            outcome="refused",
            deployment_id=deployment.id,
            publication_id=None,
            repo_full_name=repo,
            detail="repository is not allowed for runtime workspaces",
        )
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "repository is not allowed for runtime workspaces",
            headers={"Cache-Control": "no-store"},
        )
    try:
        clone_url, authorization_header = await run_in_threadpool(
            resolve_repository_credential, repo, settings
        )
    except Exception as exc:
        await crud.append_credential_redemption_audit(
            session,
            purpose="workspace_clone",
            outcome="refused",
            deployment_id=deployment.id,
            publication_id=None,
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
        purpose="workspace_clone",
        outcome="issued",
        deployment_id=deployment.id,
        publication_id=None,
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
        revision=selected.revision,
    )
