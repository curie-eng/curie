"""Generic static input compatibility against immutable deployed identities.

These checks compare opaque identifiers and digests, never coworker semantics.
Raw tracker text is transient and is not persisted or included in errors.
"""

from __future__ import annotations

import asyncio
import hashlib
import tempfile
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
from botocore.exceptions import BotoCoreError, ClientError
from plugin_format import UnsupportedArchive
from plugin_format.input_contracts import (
    INPUT_CONTRACT_ENFORCEMENT,
    InputContracts,
    InputContractsInvalid,
    parse_input_contracts,
    read_input_contracts,
    require_input_contract,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.concurrency import run_in_threadpool

from . import bundles
from .config import get_settings
from .forges.errors import ForgeError
from .forges.registry import forges_registry
from .forges.trackers import tracker_for_work_item
from .github_app import GitHubAppError
from .models import AgentVersion, Deployment, Environment, ExecutionRequest, WorkItem
from .storage import BundleStore


class InputCompatibilityError(ValueError):
    def __init__(self, code: str = "input_contract_unknown") -> None:
        self.code = code
        super().__init__(code)


async def _selected_deployment(
    session: AsyncSession, agent_id: uuid.UUID
) -> tuple[Deployment, AgentVersion] | None:
    result = await session.execute(
        select(Deployment, AgentVersion)
        .join(AgentVersion, AgentVersion.id == Deployment.version_id)
        .where(
            Deployment.agent_id == agent_id,
            Deployment.status == "active",
            AgentVersion.agent_id == agent_id,
        )
        .order_by(
            (Deployment.environment == Environment.prod).desc(),
            Deployment.deployed_at.desc(),
            Deployment.id.desc(),
        )
        .limit(1)
    )
    row = result.first()
    return None if row is None else (row[0], row[1])


def _stored_declaration(data: bytes, expected_digest: str) -> InputContracts | None:
    if hashlib.sha256(data).hexdigest() != expected_digest:
        raise InputCompatibilityError
    settings = get_settings()
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        bundles.extract_stored_bundle(
            data,
            root,
            max_uncompressed_bytes=settings.bundle_max_uncompressed_bytes,
            max_compression_ratio=settings.bundle_max_compression_ratio,
            max_members=settings.bundle_max_members,
        )
        return read_input_contracts(
            bundles.bundle_root(root),
            enforcements=frozenset({INPUT_CONTRACT_ENFORCEMENT}),
        )


async def _version_declaration(version: AgentVersion) -> InputContracts | None:
    try:
        if version.input_contracts is not None:
            return parse_input_contracts(version.input_contracts)
        # NULL may be a stored version from before the cache column existed.
        # Read its immutable archive rather than treating unknown as legacy.
        reference = getattr(version, "bundle_ref", None)
        if reference is None:
            return None
        if not version.bundle_sha256:
            raise InputCompatibilityError
        async with asyncio.timeout(20):
            data = await BundleStore(get_settings()).get(reference)
            return await run_in_threadpool(_stored_declaration, data, version.bundle_sha256)
    except (
        InputContractsInvalid, UnsupportedArchive, OSError, ValueError, TimeoutError,
        BotoCoreError, ClientError,
    ):
        raise InputCompatibilityError from None


async def deployment_contracts(session: AsyncSession, deployment_id: uuid.UUID) -> dict[str, Any]:
    deployment = await session.get(Deployment, deployment_id)
    if deployment is None or deployment.status != "active":
        raise InputCompatibilityError
    selected = await _selected_deployment(session, deployment.agent_id)
    if selected is None or selected[0].id != deployment.id:
        raise InputCompatibilityError
    version = selected[1]
    declaration = await _version_declaration(version)
    if declaration is None:
        raise InputCompatibilityError
    if not version.bundle_sha256:
        raise InputCompatibilityError
    return {
        "interface": INPUT_CONTRACT_ENFORCEMENT,
        "status": "active",
        "enforced": True,
        "deployment_id": str(deployment.id),
        "agent_id": str(deployment.agent_id),
        "version_id": str(version.id),
        "bundle_sha256": version.bundle_sha256,
        "accepted_contracts": list(declaration.accepts),
    }


async def _read_input_body(facts: Any) -> str:
    try:
        async with asyncio.timeout(20):
            async with httpx.AsyncClient(timeout=5) as client:
                tracker = tracker_for_work_item(
                    forges_registry(get_settings()),
                    client,
                    facts.issue,
                    facts.repository,
                    facts.code_host_installation_id,
                    timeout_s=5,
                )
                return await tracker.read_input_body(facts.issue)
    except (ForgeError, GitHubAppError, TimeoutError, httpx.HTTPError, ValueError):
        raise InputCompatibilityError from None


async def check_admission_input(session: AsyncSession, facts: Any) -> dict[str, Any] | None:
    selected = await _selected_deployment(session, facts.agent_id)
    if selected is None:
        return None
    deployment, version = selected
    declaration = await _version_declaration(version)
    if declaration is None:
        return None
    if not version.bundle_sha256:
        raise InputCompatibilityError
    body = await _read_input_body(facts)
    try:
        envelope = require_input_contract(
            body,
            declaration,
            deployment_id=str(deployment.id),
            bundle_sha256=version.bundle_sha256,
        )
    except InputContractsInvalid:
        raise InputCompatibilityError("input_contract_mismatch") from None
    return {
        "interface": INPUT_CONTRACT_ENFORCEMENT,
        "contract": envelope.contract,
        "deployment_id": str(deployment.id),
        "version_id": str(version.id),
        "bundle_sha256": version.bundle_sha256,
        "body_sha256": envelope.body_sha256,
    }


async def check_start_input(
    session: AsyncSession,
    item: WorkItem,
    request: ExecutionRequest,
    *,
    runtime_deployment_id: uuid.UUID | None,
    runtime_version_id: uuid.UUID | None,
    runtime_bundle_sha256: str | None,
) -> None:
    receipt = request.input_contract_receipt
    if receipt is None:
        if getattr(request, "started_at", None) is not None:
            return
        selected = await _selected_deployment(session, item.agent_id)
        if selected is not None and await _version_declaration(selected[1]) is not None:
            raise InputCompatibilityError
        return
    if not isinstance(receipt, dict):
        raise InputCompatibilityError
    if (
        str(runtime_deployment_id) != receipt.get("deployment_id")
        or str(runtime_version_id) != receipt.get("version_id")
        or runtime_bundle_sha256 != receipt.get("bundle_sha256")
    ):
        raise InputCompatibilityError("input_contract_mismatch")
    facts = SimpleNamespace(
        agent_id=item.agent_id,
        issue=item.tracker_issue,
        repository=item.repository,
        code_host_installation_id=item.code_host_installation_id,
    )
    current = await check_admission_input(session, facts)
    if current != receipt:
        raise InputCompatibilityError("input_contract_mismatch")
