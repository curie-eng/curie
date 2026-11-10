"""The open `/config` endpoint: it exposes the configurable org name.

Like `/health`, this route carries no `require_api_key` dependency so the UI can
read the workspace name before the user has supplied a key. The org name comes
from `get_settings()`, which reads it from the environment at first-call time
(env vars are fixed at pod start in practice).
"""

from fastapi import APIRouter

from curie_api.schemas.agents import AppConfig

from ..config import get_settings

router = APIRouter(tags=["config"])


@router.get("/config", response_model=AppConfig)
async def get_config() -> AppConfig:
    return AppConfig(org_name=get_settings().org_name)
