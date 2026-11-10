"""``GET /forges/credentials/health``: whether each forge accepts its credential (#4630).

Platform key or console session (`require_api_key`), read-only. Each
configured code host and tracker is probed with one read through its adapter
(`curie_api.forge_credential_health`); the report carries no credential bytes.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from ..auth import require_api_key
from ..config import get_settings
from ..forge_credential_health import ForgeCredentialHealthReport, credential_health
from ..forges.registry import forges_registry

router = APIRouter(tags=["forges"], dependencies=[Depends(require_api_key)])


@router.get("/forges/credentials/health", response_model=ForgeCredentialHealthReport)
async def get_forge_credential_health() -> ForgeCredentialHealthReport:
    return await credential_health(forges_registry(get_settings()))
