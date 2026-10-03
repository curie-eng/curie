"""Principal resolution over HTTP (#2910, ADR 0155 step 5).

Platform key only. The dispatcher has no database access, so it asks here
which principal a Slack user is. The route stays thin: every rule lives in
``curie_api.identity.service``, and an unresolved answer is a 200.
"""

from dataclasses import asdict

from fastapi import APIRouter, Depends

from ..auth import require_api_key
from ..deps import SessionDep
from ..identity import find_provider_installation, resolve_principal
from ..provider_installations import DEFAULT_TENANT_ID
from ..schemas import PrincipalResolutionOut, PrincipalResolveIn

router = APIRouter(
    prefix="/identity",
    tags=["identity"],
    dependencies=[Depends(require_api_key)],
)


@router.post("/resolve", response_model=PrincipalResolutionOut)
async def resolve_identity(data: PrincipalResolveIn, session: SessionDep) -> PrincipalResolutionOut:
    tenant_id = data.tenant_id or DEFAULT_TENANT_ID
    installation_id = data.provider_installation_id
    if installation_id is None:
        # The validator guarantees the (provider, external account) pair here.
        assert data.provider is not None and data.external_account_id is not None
        installation_id = await find_provider_installation(
            session,
            tenant_id=tenant_id,
            provider=data.provider,
            external_account_id=data.external_account_id,
        )
        if installation_id is None:
            return PrincipalResolutionOut(
                status="unresolved",
                principal_id=None,
                reason="installation_not_found",
                provider_installation_id=None,
            )
    resolution = await resolve_principal(
        session,
        tenant_id=tenant_id,
        provider_installation_id=installation_id,
        provider_subject=data.provider_subject,
    )
    return PrincipalResolutionOut(**asdict(resolution))
