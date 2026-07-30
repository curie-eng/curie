"""Agents and their versions."""

import functools
import tempfile
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.exc import IntegrityError
from starlette.concurrency import run_in_threadpool

from .. import bundles, crud
from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep, StoreDep
from ..schemas import (
    AgentCreate,
    AgentOut,
    AgentUpdate,
    BundleFile,
    BundleFiles,
    ConnectorManifests,
    VersionCreate,
    VersionOut,
    enforce_behavior_packs_size,
)

router = APIRouter(prefix="/agents", tags=["agents"], dependencies=[Depends(require_api_key)])

# Postgres SQLSTATE for a unique_violation. asyncpg exposes it (and the
# violated constraint's name) as plain attributes on the wrapped driver
# exception -- there is no psycopg-style `.diag` namespace.
_UNIQUE_VIOLATION = "23505"

# The real unique constraints on `agents` (from the alembic migrations), mapped
# to the human message for each. Any other unique violation falls back to a
# generic message.
_UNIQUE_CONSTRAINT_MESSAGES = {
    "agents_name_key": "an agent with that name already exists",
    "ix_agents_repo_full_name": "an agent for that repository already exists",
    # #38: one agent per channel. Without this the create succeeded and the
    # second agent was silently shadowed by the worker's resolver at runtime.
    "agents_slack_channel_key": (
        "another agent is already bound to that Slack channel; one agent per "
        "channel (move or delete the other agent, or pick another channel)"
    ),
}


def _driver_diag(exc: IntegrityError, attr: str) -> str | None:
    """Read an asyncpg diagnostic field, walking the `__cause__` chain.

    asyncpg surfaces `sqlstate` on SQLAlchemy's DBAPI wrapper (`exc.orig`) but
    exposes `constraint_name` only on the underlying `asyncpg` error one link
    down the `__cause__` chain. Walk both so either shape resolves; guard
    against a cyclic chain.
    """
    obj = getattr(exc, "orig", None)
    seen: set[int] = set()
    while obj is not None and id(obj) not in seen:
        seen.add(id(obj))
        value = getattr(obj, attr, None)
        if value is not None:
            return str(value)
        obj = getattr(obj, "__cause__", None)
    return None


def classify_integrity_error(exc: IntegrityError) -> tuple[int, str] | None:
    """Map a real unique-constraint violation to a `(409, message)` conflict.

    Only a genuine unique_violation (SQLSTATE 23505) is a caller conflict; a
    NOT NULL or FK violation is a server fault and must surface as a 500, so
    this returns `None` for those (the caller re-raises). The human message is
    chosen by the violated constraint's name from asyncpg's structured fields,
    not by substring-matching the stringified driver error.
    """
    if _driver_diag(exc, "sqlstate") != _UNIQUE_VIOLATION:
        return None
    constraint_name = _driver_diag(exc, "constraint_name")
    message = "agent violates a uniqueness constraint"
    if constraint_name is not None:
        message = _UNIQUE_CONSTRAINT_MESSAGES.get(constraint_name, message)
    return status.HTTP_409_CONFLICT, message


@router.post("", response_model=AgentOut, status_code=status.HTTP_201_CREATED)
async def create_agent(data: AgentCreate, session: SessionDep) -> AgentOut:
    # Reject oversized behavior packs (#936) before we touch the DB.
    if data.behavior_packs is not None:
        enforce_behavior_packs_size(data.behavior_packs)
    # name and repo_full_name are unique. A collision is a caller conflict (409),
    # not a server fault: catch the DB IntegrityError and map it, rather than
    # letting it bubble as an opaque 500. A non-unique violation (NOT NULL, FK)
    # is a genuine server fault -- re-raise it so it surfaces as a 500.
    try:
        agent = await crud.create_agent(session, data)
    except IntegrityError as exc:
        await session.rollback()
        classified = classify_integrity_error(exc)
        if classified is None:
            raise
        status_code, message = classified
        raise HTTPException(status_code, message) from exc
    return AgentOut.model_validate(agent)


@router.get("", response_model=list[AgentOut])
async def list_agents(session: SessionDep) -> list[AgentOut]:
    agents = await crud.list_agents(session)
    return [AgentOut.model_validate(a) for a in agents]


@router.get("/{agent_id}", response_model=AgentOut)
async def get_agent(agent_id: uuid.UUID, session: SessionDep) -> AgentOut:
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return AgentOut.model_validate(agent)


@router.patch("/{agent_id}", response_model=AgentOut)
async def update_agent(agent_id: uuid.UUID, data: AgentUpdate, session: SessionDep) -> AgentOut:
    # Lets a redeploy move an existing agent's Slack channel (the CLI only sends
    # this when --slack-channel was passed explicitly). An omitted field is a
    # no-op so the agent's current channel is preserved.
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if data.slack_channel is not None:
        # slack_channel is unique (one agent per channel, #38), so this is the one
        # PATCHable field that can collide with another agent. Classify it the same
        # way create does -- a caller conflict is a 409, not an opaque 500 -- so the
        # constraint cannot be sidestepped by creating on a free channel and then
        # moving onto a taken one. (Before #38 no PATCHable field was unique, which
        # is why this path had no IntegrityError handler.)
        try:
            agent = await crud.update_agent_channel(session, agent, data.slack_channel)
        except IntegrityError as exc:
            await session.rollback()
            classified = classify_integrity_error(exc)
            if classified is None:
                raise
            status_code, message = classified
            raise HTTPException(status_code, message) from exc
    if data.model is not None:
        agent = await crud.update_agent_model(session, agent, data.model)
    if data.approval_required_tools is not None:
        # Omitted leaves the gates unchanged; an explicit [] clears them (#245).
        agent = await crud.update_agent_approval_tools(session, agent, data.approval_required_tools)
    if data.approval_routes is not None:
        # Omitted leaves the bindings unchanged; an explicit {} clears them (#247).
        agent = await crud.update_agent_approval_routes(
            session,
            agent,
            {name: b.model_dump() for name, b in data.approval_routes.items()},
        )
    if data.secrets is not None:
        # Omitted leaves the secrets unchanged; an explicit {} clears them (#429).
        agent = await crud.update_agent_secrets(session, agent, data.secrets)
    return AgentOut.model_validate(agent)


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(agent_id: uuid.UUID, session: SessionDep) -> None:
    # Deleting an agent cascades its versions and deployments rows (bundle
    # objects in MinIO are left as-is, out of scope). Refuse while a deployment
    # is still active so a live agent cannot be pulled out from under Slack
    # traffic; the caller must stop it (kill/undeploy) first.
    agent = await crud.get_agent(session, agent_id)
    if agent is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    if await crud.agent_has_active_deployment(session, agent_id):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "agent has an active deployment; stop it before deleting",
        )
    await crud.delete_agent(session, agent_id)


@router.post(
    "/{agent_id}/versions",
    response_model=VersionOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_version(
    agent_id: uuid.UUID, data: VersionCreate, session: SessionDep
) -> VersionOut:
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    version = await crud.create_version(session, agent_id, data)
    return VersionOut.model_validate(version)


@router.get("/{agent_id}/versions", response_model=list[VersionOut])
async def list_versions(agent_id: uuid.UUID, session: SessionDep) -> list[VersionOut]:
    if await crud.get_agent(session, agent_id) is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    versions = await crud.list_versions(session, agent_id)
    return [VersionOut.model_validate(v) for v in versions]


@router.get("/{agent_id}/versions/{version_id}/connectors", response_model=ConnectorManifests)
async def read_version_connectors(
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    session: SessionDep,
    store: StoreDep,
    release: str,
    namespace: str,
    app_name: str,
) -> ConnectorManifests:
    """Render this version's declared connectors into Kubernetes objects.

    Read-only and side-effect free: the API computes the manifests and returns
    them; the CALLER applies them with its own cluster credentials. That split
    is deliberate -- rendering is a pure function, so the API needs no cluster
    access for it, and this service (which receives internet webhooks) keeps the
    read-only `pods: list` + `pods/log: get` RBAC it has today (ADR-0086).

    `release`, `namespace`, and `app_name` are supplied by the caller because
    they are install-time facts the API does not know: the Helm release name and
    nameOverride live with whoever ran `cluster up`, not in the bundle.
    """

    version = await crud.get_version(session, version_id)
    if version is None or version.agent_id != agent_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "version not found")
    if version.bundle_ref is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no bundle stored for this version")
    data = await store.get(version.bundle_ref)
    settings = get_settings()

    def _render() -> ConnectorManifests:
        with tempfile.TemporaryDirectory() as tmp:
            bundles.extract_and_validate(
                data,
                Path(tmp),
                max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
                max_compression_ratio=settings.bundle_max_compression_ratio,
                max_members=settings.bundle_max_members,
            )
            declared = bundles.read_connectors(Path(tmp))
            secret_name = f"{release}-connector-secrets"
            return ConnectorManifests(
                manifests=bundles.render_connector_manifests(
                    declared,
                    release=release,
                    namespace=namespace,
                    app_name=app_name,
                    secret_name=secret_name,
                ),
                mcp_entries=bundles.connector_mcp_entries(
                    declared, release=release, namespace=namespace
                ),
            )

    return await run_in_threadpool(_render)


@router.get("/{agent_id}/versions/{version_id}/files", response_model=BundleFiles)
async def read_version_files(
    agent_id: uuid.UUID,
    version_id: uuid.UUID,
    session: SessionDep,
    store: StoreDep,
) -> BundleFiles:
    # The UI reads a version's authored text (skills, manifest, eval cases) to
    # render the bundle without pulling the raw archive. 404 covers a missing
    # agent, a version that is not this agent's, and a version with no bundle
    # stored yet -- there is nothing to read in any of those cases.
    version = await crud.get_version(session, version_id)
    if version is None or version.agent_id != agent_id:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "version not found")
    if version.bundle_ref is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no bundle stored for this version")
    data = await store.get(version.bundle_ref)
    settings = get_settings()
    read = functools.partial(
        bundles.read_bundle_text_files,
        max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
        max_compression_ratio=settings.bundle_max_compression_ratio,
        max_members=settings.bundle_max_members,
    )
    files = await run_in_threadpool(read, data)
    return BundleFiles(files=[BundleFile(path=p, content=c) for p, c in files])
