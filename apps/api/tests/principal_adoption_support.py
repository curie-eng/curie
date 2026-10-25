"""Synthetic persistence fixtures; no provider authenticity or rollout claim.

@spec HTTP-PRINCIPAL-ADOPTION-V1-1 to HTTP-PRINCIPAL-ADOPTION-V1-13.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import importlib
import json
import secrets
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

from curie_api.config import get_settings
from curie_api.crud import console
from curie_api.identity import connect
from curie_api.models import (
    ConsoleSession,
    IdentityConnectToken,
    IdentityNamespace,
    Principal,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from curie_api.oidc import OidcClaims
from curie_api.schemas.http_authority import HttpSelectionV1
from curie_api.schemas.http_identity import LookupRequestV1
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

ISSUER = "https://identity.example.test/directory"
NOW = datetime(2030, 1, 1, 12, 0, 1, tzinfo=UTC)
VECTORS = json.loads(
    (Path(__file__).resolve().parents[3] / "tests/vectors/http-identity-v1.json").read_text()
)


def api() -> ModuleType:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3. Missing API fails in the test body."""
    name = "curie_api.principal_authority"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "principal_authority persistence service is not implemented"
        raise
    for symbol in (
        "read_authority",
        "adopt_http_authority",
        "replace_http_configuration",
        "capture_http_authority",
        "apply_http_principal",
        "PrincipalAuthorityRefusal",
    ):
        assert hasattr(module, symbol), f"missing principal authority interface: {symbol}"
    return module


def run(body: Callable[[async_sessionmaker[AsyncSession]], Awaitable[None]]) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3. Use the launcher's disposable DB."""

    async def check() -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-3. Use the launcher's disposable DB."""
        engine = create_async_engine(get_settings().database_url)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with asyncio.timeout(15):
                await body(factory)
        finally:
            await engine.dispose()

    asyncio.run(check())


def selection(**changes: Any) -> HttpSelectionV1:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2."""
    value = dict(
        mode="http",
        issuer=ISSUER,
        lookup_url="https://lookup.example.test/v1/lookup",
        credential_ref={"id": "synthetic-secret-ref", "generation": 1},
        max_identity_age_ms=2000,
        max_membership_age_ms=3000,
        max_clock_skew_ms=0,
    )
    value.update(changes)
    return HttpSelectionV1.model_validate(value)


async def seed(
    factory: async_sessionmaker[AsyncSession],
    *,
    local: bool = False,
    status: str | None = None,
    kind: str = "human",
) -> tuple[uuid.UUID, uuid.UUID | None]:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2 HTTP-PRINCIPAL-ADOPTION-V1-5."""
    async with factory() as s, s.begin():
        tenant = Tenant(deployment_id=f"synthetic-adoption-{uuid.uuid4()}", status="active")
        s.add(tenant)
        await s.flush()
        person = None
        if local:
            s.add(
                TenantIdentityProvider(
                    tenant_id=tenant.id,
                    issuer=ISSUER,
                    governed_domains=["example.test"],
                    managed_by="synthetic-directory",
                    version=3,
                    identity_epoch=7,
                )
            )
        if status:
            person = Principal(
                tenant_id=tenant.id,
                idp_issuer=ISSUER,
                idp_subject="SampleSubject",
                type=kind,
                status=status,
                authorization_version=4,
                display_name="Stored name",
                email="stored@example.test",
                last_seen_at=NOW,
            )
            s.add(person)
            await s.flush()
        return tenant.id, person.id if person else None


async def adopt(
    factory: async_sessionmaker[AsyncSession], tenant: uuid.UUID, *, local: bool = False
) -> Any:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-5."""
    async with factory() as s, s.begin():
        row = await s.get(TenantIdentityProvider, tenant)
        return await api().adopt_http_authority(
            s,
            tenant_id=tenant,
            expected_version=3 if local else 0,
            expected_lifecycle_generation=row.lifecycle_generation if row else 0,
            selection=selection(),
        )


async def capture(factory: async_sessionmaker[AsyncSession], tenant: uuid.UUID) -> Any:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6."""
    return await api().capture_http_authority(factory, tenant_id=tenant)


def lookup(
    captured: Any,
    *,
    kind: str = "human",
    status: str = "active",
    outcome: str | None = None,
    as_of: datetime | None = None,
) -> tuple[LookupRequestV1, dict[str, Any]]:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6 HTTP-PRINCIPAL-ADOPTION-V1-7."""
    req = copy.deepcopy(VECTORS["request"])
    req["scope"]["tenant_id"] = captured.configuration.tenant_id
    req["scope"]["expected_issuer"] = captured.configuration.selection.issuer
    req["authority_version"] = captured.configuration.version
    req["request_id"] = str(uuid.uuid4())
    response = copy.deepcopy(VECTORS["cases"][0]["response"])
    response.update(copy.deepcopy(req))
    response["outcome"] = outcome or ("resolved" if status == "active" else "inactive")
    response["sender_kind"] = kind
    response["identity"].update(
        kind=kind, account_status=status, issuer=req["scope"]["expected_issuer"]
    )
    if as_of:
        response["identity_observation"]["as_of"] = as_of.isoformat()
        response["membership"]["observation"]["as_of"] = as_of.isoformat()
    if response["outcome"] != "resolved":
        response["membership"] = None
    if response["outcome"] in {"unresolved", "unavailable"}:
        response["identity"] = None
        response["sender_kind"] = "unknown"
    if response["outcome"] == "unavailable":
        response["identity_observation"] = None
    return LookupRequestV1.model_validate(req), response


async def apply(
    s: AsyncSession, captured: Any, *, clock: Callable[[], datetime] = lambda: NOW, **facts: Any
) -> Any:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6."""
    request, response = lookup(captured, **facts)
    return await api().apply_http_principal(
        s,
        capture=captured,
        request=request,
        response_bytes=json.dumps(response).encode(),
        clock=clock,
    )


async def cookie(
    s: AsyncSession,
    principal: uuid.UUID | None,
    *,
    revoked: datetime | None = None,
    consumed: bool = True,
) -> tuple[str, ConsoleSession]:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-8 HTTP-PRINCIPAL-ADOPTION-V1-11."""
    code = uuid.uuid4().hex
    moment = datetime.now(UTC).replace(tzinfo=None)
    row = ConsoleSession(
        principal_id=principal,
        subject=None if principal else "U0EXAMPLE1",
        login_code_hash=console.hash_console_credential(code),
        login_code_expires_at=moment + timedelta(hours=1),
        session_token_hash=console.hash_console_credential(uuid.uuid4().hex) if consumed else None,
        session_expires_at=moment + timedelta(hours=12) if consumed else None,
        consumed_at=moment if consumed else None,
        revoked_at=revoked,
    )
    s.add(row)
    await s.flush()
    return code, row


def claims(*, issuer: str = ISSUER, subject: str = "SampleSubject") -> OidcClaims:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-10."""
    return OidcClaims(
        issuer=issuer,
        subject=subject,
        email="new@example.test",
        display_name="Fresh name",
        raw={"email_verified": True},
    )


async def team(s: AsyncSession, tenant: uuid.UUID, *, synced: bool = True) -> Team:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-10."""
    row = Team(
        tenant_id=tenant,
        source="idp_group",
        external_id="synthetic-group",
        name="Synthetic group",
        managed_by="synthetic-directory",
        epoch=1 if synced else 0,
        digest=hashlib.sha256(b"").hexdigest() if synced else None,
        last_synced_at=NOW if synced else None,
        last_attempt_at=NOW if synced else None,
    )
    s.add(row)
    await s.flush()
    return row


async def blocked(observer: AsyncSession, waiter: int, holder: int) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-4. Observe our own backends, not timing."""
    async with asyncio.timeout(4):
        while True:
            blockers = await observer.scalar(text("SELECT pg_blocking_pids(:pid)"), {"pid": waiter})
            if holder in blockers:
                return
            await asyncio.sleep(0.01)


async def pid(s: AsyncSession) -> int:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-4."""
    return int(await s.scalar(text("SELECT pg_backend_pid()")))


async def principal(s: AsyncSession, tenant: uuid.UUID) -> Principal | None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-7."""
    return await s.scalar(
        select(Principal)
        .where(
            Principal.tenant_id == tenant,
            Principal.idp_issuer == ISSUER,
            Principal.idp_subject == "SampleSubject",
        )
        .execution_options(populate_existing=True)
    )


async def connection_fixture(s: AsyncSession, tenant: uuid.UUID) -> tuple[str, IdentityNamespace]:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-10."""
    namespace = IdentityNamespace(
        tenant_id=tenant, provider="slack", authority="", kind="slack_workspace", key="T0EXAMPLE1"
    )
    s.add(namespace)
    await s.flush()
    token = secrets.token_urlsafe(32)
    s.add(
        IdentityConnectToken(
            token_hash=connect.hash_token(token),
            tenant_id=tenant,
            identity_namespace_id=namespace.id,
            provider_native_id="U0EXAMPLE1",
            idp_issuer=ISSUER,
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
    )
    await s.flush()
    return token, namespace
