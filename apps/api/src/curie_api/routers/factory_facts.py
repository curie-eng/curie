"""Operator read of the same advisory factory facts contract (ADR 0280)."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import ValidationError

from ..auth import require_api_key
from ..config import get_settings
from ..deps import SessionDep
from ..factory_facts import FACTS_DEADLINE_SECONDS, read_facts
from ..factory_facts_schema import FactoryFacts, FactsRequest
from ..forges.errors import ForgeError
from ..forges.hosts import resolve_factory_repository
from ..forges.registry import forges_registry
from ..github_app import GitHubAppError, GitHubInstallationRefused
from ..github_review_events import FeedbackIgnored, FeedbackUnavailable
from ..repo_full_name import InvalidRepoFullName, normalize_repo_full_name

router = APIRouter(tags=["factory-facts"], dependencies=[Depends(require_api_key)])


@router.get("/factory/facts", response_model=FactoryFacts)
async def get_factory_facts(
    request: Request,
    session: SessionDep,
    repository: str,
    trigger_label: str,
    executables: str = "",
    services: str = "",
) -> FactoryFacts:
    try:
        async with asyncio.timeout(FACTS_DEADLINE_SECONDS):
            return await _get_factory_facts(
                request, session, repository, trigger_label, executables, services
            )
    except HTTPException:
        raise
    except Exception:  # noqa: BLE001 - return only the fixed unavailable code on a guard failure
        raise HTTPException(503, {"code": "facts_unavailable"}) from None


async def _get_factory_facts(
    request: Request,
    session: SessionDep,
    repository: str,
    trigger_label: str,
    executables: str,
    services: str,
) -> FactoryFacts:
    settings = get_settings()
    try:
        repository = normalize_repo_full_name(repository)
        query = FactsRequest(
            schema_version=1,
            trigger_label=trigger_label,
            executables=executables.split(",") if executables else [],
            services=services.split(",") if services else [],
        )
    except (ValidationError, InvalidRepoFullName):
        raise HTTPException(422, {"code": "invalid_facts_request"}) from None
    try:
        registry = forges_registry(settings)
        repo = await resolve_factory_repository(registry, request.app.state.http_client, repository)
    except (
        ForgeError,
        GitHubAppError,
        GitHubInstallationRefused,
        FeedbackIgnored,
        FeedbackUnavailable,
    ):
        raise HTTPException(503, {"code": "facts_unavailable"}) from None
    return await read_facts(
        session,
        settings,
        request.app.state.valkey,
        repository=repository,
        repository_id=repo.project_id,
        query=query,
        issue_number=None,
    )
