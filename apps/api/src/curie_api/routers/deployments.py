"""Deployments of a version to an environment."""

import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from starlette.concurrency import run_in_threadpool

from curie_api.crud import agents as crud_agents
from curie_api.crud import deployments as crud_deployments
from curie_api.crud import versions as crud_versions
from curie_api.schemas.deployments import DeploymentCreate, DeploymentOut

from .. import deploy, factory_capabilities
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep, StoreDep

router = APIRouter(
    prefix="/deployments",
    tags=["deployments"],
    dependencies=[Depends(require_api_key)],
)


@router.post("", response_model=DeploymentOut, status_code=status.HTTP_201_CREATED)
async def create_deployment(
    data: DeploymentCreate, session: SessionDep, store: StoreDep
) -> DeploymentOut:
    agent = await crud_agents.get_agent(session, data.agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if (
        "workspace_enabled" in data.model_fields_set
        and data.workspace_enabled is None
    ):
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_ENTITY,
            "workspace_enabled must be true or false when provided",
        )
    version = await crud_versions.get_version(session, data.version_id)
    if version is None or version.agent_id != data.agent_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "version not found")
    # Revalidate the stored bundle against the CURRENT size/ratio caps before
    # this version becomes deployable -- catches a bundle stored before these
    # caps existed, or under looser ones (ADR-0059 decision 3).
    try:
        await deploy.revalidate_stored_bundle(store, version)
    except deploy.BundleTooLarge as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    if version.bundle_ref is not None:
        bundle_data = await store.get(version.bundle_ref)
        try:
            await run_in_threadpool(
                factory_capabilities.read_manifest,
                bundle_data,
                get_settings(),
                expected_bundle_sha256=version.bundle_sha256,
                required=False,
                require_registry_image=True,
            )
        except factory_capabilities.CapabilityUnknown as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    # @spec ACTION-EXECUTOR-16: a bundle stored before intake refused a
    # non-SecretRef sealing key declaration is refused here too, before any row.
    try:
        await deploy.check_stored_sealing_key_custody(store, version)
    except (deploy.BundleTooLarge, deploy.SealingKeyCustody) as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    # The declared/bound approval-route join (#2436): this is the first moment
    # both halves are known for a specific agent, and the moment the version
    # becomes the thing that boots. AFTER the bounds check above, so an over-cap
    # legacy bundle keeps reporting `bundle.too_large` rather than surfacing as
    # an extraction failure in here (ADR-0059 decision 3's error contract), and
    # in FRONT of the row creation, so a refusal leaves nothing behind.
    try:
        await deploy.check_approval_route_bindings(store, version, agent.approval_routes)
    except deploy.ApprovalRoutesUnbound as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(exc)) from exc
    deployment = await crud_deployments.create_deployment(session, data)
    return DeploymentOut.model_validate(deployment)


@router.get("", response_model=list[DeploymentOut])
async def list_deployments(
    session: SessionDep, agent_id: uuid.UUID | None = None
) -> list[DeploymentOut]:
    deployments = await crud_deployments.list_deployments(session, agent_id)
    return [DeploymentOut.model_validate(d) for d in deployments]


@router.get("/{deployment_id}", response_model=DeploymentOut)
async def get_deployment(
    deployment_id: uuid.UUID, session: SessionDep
) -> DeploymentOut:
    deployment = await crud_deployments.get_deployment(session, deployment_id)
    if deployment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "deployment not found")
    return DeploymentOut.model_validate(deployment)


@router.get(
    "/{deployment_id}/capabilities", response_model=factory_capabilities.CapabilityRead
)
async def read_deployment_capabilities(
    deployment_id: uuid.UUID, session: SessionDep, store: StoreDep
) -> factory_capabilities.CapabilityRead:
    deployment = await crud_deployments.get_deployment(session, deployment_id)
    if deployment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "deployment not found")
    version = await crud_versions.get_version(session, deployment.version_id)
    if version is None or version.bundle_ref is None or version.bundle_sha256 is None:
        raise HTTPException(status.HTTP_409_CONFLICT, "environment_unknown")
    try:
        bundle_data = await store.get(version.bundle_ref)
    except Exception as exc:
        # The object-store port permits different backends and does not fix an
        # exception type for absent/unreadable bytes. No readable bundle means
        # admission cannot establish this deployment's environment.
        raise HTTPException(status.HTTP_409_CONFLICT, "environment_unknown") from exc
    try:
        claim = await run_in_threadpool(
            factory_capabilities.read_manifest,
            bundle_data,
            get_settings(),
            expected_bundle_sha256=version.bundle_sha256,
            require_registry_image=True,
        )
    except factory_capabilities.CapabilityUnknown as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, "environment_unknown") from exc
    assert claim is not None
    return claim


@router.delete("/{deployment_id}", status_code=status.HTTP_204_NO_CONTENT)
async def end_deployment(deployment_id: uuid.UUID, session: SessionDep) -> None:
    deployment = await crud_deployments.get_deployment(session, deployment_id)
    if deployment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "deployment not found")
    await crud_deployments.end_deployment(session, deployment)
