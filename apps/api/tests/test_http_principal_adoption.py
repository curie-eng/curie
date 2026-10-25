"""Independent synthetic persistence contract against isolated real Postgres.

@spec HTTP-PRINCIPAL-ADOPTION-V1-1 to HTTP-PRINCIPAL-ADOPTION-V1-13.
No HTTP provider, credential binding, caller activation or old-binary claim.
"""

from __future__ import annotations

import ast
import dataclasses
import json
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from curie_api import identity_audit
from curie_api.crud import console
from curie_api.models import (
    ConsoleSession,
    IdentityAuditEvent,
    IdentityLink,
    Principal,
    PrincipalTeam,
    Tenant,
    TenantIdentityProvider,
)
from curie_api.schemas.http_authority import HttpSelectionV1
from principal_adoption_support import (
    ISSUER,
    NOW,
    adopt,
    api,
    apply,
    capture,
    claims,
    cookie,
    lookup,
    principal,
    run,
    seed,
    selection,
)
from sqlalchemy import event, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


def test_no_row_read_and_existing_local_projection_are_exact(clean_db: None) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-2."""
        absent, _ = await seed(f)
        local, _ = await seed(f, local=True)
        async with f() as s:
            assert await service.read_authority(s, tenant_id=absent) is None
            result = await service.read_authority(s, tenant_id=local)
            assert result.model_dump(mode="json") == {
                "schema_version": 1,
                "tenant_id": str(local),
                "version": 3,
                "selection": {
                    "mode": "local_projection",
                    "issuer": ISSUER,
                    "governed_domains": ["example.test"],
                    "managed_by": "synthetic-directory",
                },
            }
            row = await s.get(TenantIdentityProvider, local)
            assert row.http_selection is None and row.lifecycle_generation == 0
            assert (
                row.issuer,
                row.governed_domains,
                row.managed_by,
                row.version,
                row.identity_epoch,
            ) == (ISSUER, ["example.test"], "synthetic-directory", 3, 7)

    run(body)


@pytest.mark.parametrize("local", [False, True])
def test_adoption_is_flush_only_preserves_scope_and_has_bounded_audit(
    clean_db: None, local: bool
) -> None:
    """
    @spec HTTP-PRINCIPAL-ADOPTION-V1-2 HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-5
    HTTP-PRINCIPAL-ADOPTION-V1-13.
    """
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-2 HTTP-PRINCIPAL-ADOPTION-V1-3."""
        tenant, person_id = await seed(f, local=local, status="active")
        async with f() as s:
            async with s.begin():
                result = await service.adopt_http_authority(
                    s,
                    tenant_id=tenant,
                    expected_version=3 if local else 0,
                    expected_lifecycle_generation=0,
                    selection=selection(),
                )
                assert s.in_transaction()
                assert result.configuration.version == (4 if local else 1)
                assert result.lifecycle_generation == 1 and result.revoked_session_count == 0
                async with f() as observer:
                    stored = await observer.get(TenantIdentityProvider, tenant)
                    assert (stored.http_selection if stored else None) is None
                row = await s.get(TenantIdentityProvider, tenant)
                assert row.issuer == ISSUER and row.identity_epoch == (7 if local else 0)
                assert row.governed_domains == (["example.test"] if local else None)
                assert row.managed_by == ("synthetic-directory" if local else None)
                person_row = await principal(s, tenant)
                assert (
                    person_row.id,
                    person_row.status,
                    person_row.type,
                    person_row.authorization_version,
                ) == (person_id, "active", "human", 4)
                for model in (IdentityLink, PrincipalTeam):
                    assert (
                        await s.scalar(
                            select(func.count()).select_from(model).where(model.tenant_id == tenant)
                        )
                        == 0
                    )
                audit = (
                    await s.scalars(
                        select(IdentityAuditEvent).where(IdentityAuditEvent.tenant_id == tenant)
                    )
                ).one()
                assert audit.event_type == "identity.http_authority_adopted"
                assert audit.details == {
                    "authority_version": result.configuration.version,
                    "lifecycle_generation": 1,
                    "revoked_session_count": 0,
                }
        captured = await capture(f, tenant)
        assert captured.configuration == result.configuration and captured.lifecycle_generation == 1
        assert set(
            vars(captured)
            if hasattr(captured, "__dict__")
            else {
                field.name: getattr(captured, field.name) for field in dataclasses.fields(captured)
            }
        ) == {"configuration", "lifecycle_generation"}
        with pytest.raises((AttributeError, TypeError, ValueError)):
            captured.lifecycle_generation = 123

    run(body)


@pytest.mark.parametrize("operation", ["adopt", "replace", "apply"])
def test_outer_rollback_erases_success_and_audit(clean_db: None, operation: str) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-8."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-8."""
        tenant, _ = await seed(f)
        if operation != "adopt":
            await adopt(f, tenant)
        captured = await capture(f, tenant) if operation == "apply" else None
        async with f() as s:
            await s.begin()
            if operation == "adopt":
                await service.adopt_http_authority(
                    s,
                    tenant_id=tenant,
                    expected_version=0,
                    expected_lifecycle_generation=0,
                    selection=selection(),
                )
            elif operation == "replace":
                await service.replace_http_configuration(
                    s,
                    tenant_id=tenant,
                    expected_version=1,
                    expected_lifecycle_generation=1,
                    selection=selection(),
                )
            else:
                await apply(s, captured)
            assert s.in_transaction()
            await s.rollback()
        async with f() as s:
            row = await s.get(TenantIdentityProvider, tenant)
            assert (
                row is None
                if operation == "adopt"
                else (row.version, row.lifecycle_generation) == (1, 1)
            )
            assert await principal(s, tenant) is None
            events = list(
                await s.scalars(
                    select(IdentityAuditEvent.event_type).where(
                        IdentityAuditEvent.tenant_id == tenant
                    )
                )
            )
            assert events == ([] if operation == "adopt" else ["identity.http_authority_adopted"])

    run(body)


@pytest.mark.parametrize("field", ["expected_version", "expected_lifecycle_generation"])
@pytest.mark.parametrize("bad", [True, False, -1, 0.0, "0", None])
def test_expected_cas_values_are_strict_without_partial_commit(
    clean_db: None, field: str, bad: Any
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, _ = await seed(f)
        async with f() as s, s.begin():
            values = dict(
                tenant_id=tenant,
                expected_version=0,
                expected_lifecycle_generation=0,
                selection=selection(),
            )
            values[field] = bad
            with pytest.raises(service.PrincipalAuthorityRefusal) as exc:
                await service.adopt_http_authority(s, **values)
            assert str(exc.value) == "invalid_authority_input"
        async with f() as s:
            assert await s.get(TenantIdentityProvider, tenant) is None
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(IdentityAuditEvent)
                    .where(IdentityAuditEvent.tenant_id == tenant)
                )
                == 0
            )

    run(body)


@pytest.mark.parametrize("attack", ["construct", "nested"])
def test_selection_is_revalidated_and_error_contains_only_code(clean_db: None, attack: str) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, _ = await seed(f)
        candidate = selection()
        if attack == "construct":
            candidate = HttpSelectionV1.model_construct(
                **(
                    candidate.model_dump()
                    | {"lookup_url": "http://synthetic-private-path/secret-marker"}
                )
            )
        else:
            object.__setattr__(candidate.credential_ref, "generation", True)
        async with f() as s, s.begin():
            with pytest.raises(service.PrincipalAuthorityRefusal) as exc:
                await service.adopt_http_authority(
                    s,
                    tenant_id=tenant,
                    expected_version=0,
                    expected_lifecycle_generation=0,
                    selection=candidate,
                )
            assert str(exc.value) == "invalid_authority_input"
            assert exc.value.args == ("invalid_authority_input",)
        async with f() as s:
            assert await s.get(TenantIdentityProvider, tenant) is None

    run(body)


@pytest.mark.parametrize(
    ("version", "generation", "issuer", "expected"),
    [
        (0, 1, ISSUER, "stale_authority_version"),
        (1, 0, ISSUER, "stale_lifecycle_generation"),
        (1, 1, ISSUER, "authority_already_adopted"),
    ],
)
def test_adoption_refusals_preserve_owner(
    clean_db: None, version: int, generation: int, issuer: str, expected: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-5 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-5 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        async with f() as s, s.begin():
            with pytest.raises(service.PrincipalAuthorityRefusal, match=f"^{expected}$"):
                await service.adopt_http_authority(
                    s,
                    tenant_id=tenant,
                    expected_version=version,
                    expected_lifecycle_generation=generation,
                    selection=selection(issuer=issuer),
                )
        row = await capture(f, tenant)
        assert (row.configuration.version, row.lifecycle_generation) == (1, 1)

    run(body)


def test_local_adoption_never_switches_issuer_and_inactive_tenant_refuses(clean_db: None) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-5."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-5."""
        tenant, _ = await seed(f, local=True)
        async with f() as s, s.begin():
            with pytest.raises(
                service.PrincipalAuthorityRefusal, match="^authority_scope_mismatch$"
            ):
                await service.adopt_http_authority(
                    s,
                    tenant_id=tenant,
                    expected_version=3,
                    expected_lifecycle_generation=0,
                    selection=selection(issuer="https://other.example.test"),
                )
        async with f() as s, s.begin():
            (await s.get(Tenant, tenant)).status = "suspended"
        async with f() as s, s.begin():
            with pytest.raises(service.PrincipalAuthorityRefusal, match="^tenant_unavailable$"):
                await service.adopt_http_authority(
                    s,
                    tenant_id=tenant,
                    expected_version=3,
                    expected_lifecycle_generation=0,
                    selection=selection(),
                )

    run(body)


@pytest.mark.parametrize(
    "changed",
    [
        {},
        {"lookup_url": "https://other.example.test/lookup"},
        {"credential_ref": {"id": "new-synthetic-ref", "generation": 2}},
        {"max_identity_age_ms": 1000},
        {"max_membership_age_ms": 1000},
        {"max_clock_skew_ms": 1},
    ],
)
def test_replacement_advances_both_versions_and_invalidates_capture(
    clean_db: None, changed: dict[str, Any]
) -> None:
    """
    @spec HTTP-PRINCIPAL-ADOPTION-V1-5 HTTP-PRINCIPAL-ADOPTION-V1-6
    HTTP-PRINCIPAL-ADOPTION-V1-12.
    """
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-5 HTTP-PRINCIPAL-ADOPTION-V1-6"""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        old = await capture(f, tenant)
        async with f() as s, s.begin():
            result = await service.replace_http_configuration(
                s,
                tenant_id=tenant,
                expected_version=1,
                expected_lifecycle_generation=1,
                selection=selection(**changed),
            )
            assert (result.configuration.version, result.lifecycle_generation) == (2, 2)
        async with f() as s, s.begin():
            with pytest.raises(
                service.PrincipalAuthorityRefusal, match="^stale_authority_version$"
            ):
                await apply(s, old)
        async with f() as s:
            assert await principal(s, tenant) is None
            audits = list(
                await s.scalars(
                    select(IdentityAuditEvent).where(
                        IdentityAuditEvent.event_type == "identity.http_authority_changed",
                        IdentityAuditEvent.tenant_id == tenant,
                    )
                )
            )
            assert len(audits) == 1 and audits[0].details == {
                "authority_version": 2,
                "lifecycle_generation": 2,
            }

    run(body)


@pytest.mark.parametrize(
    ("kind", "status"),
    [("human", "active"), ("service", "active"), ("human", "disabled"), ("service", "revoked")],
)
def test_canonical_create_stable_uuid_status_version_generation_and_audit(
    clean_db: None, kind: str, status: str
) -> None:
    """
    @spec HTTP-PRINCIPAL-ADOPTION-V1-7 HTTP-PRINCIPAL-ADOPTION-V1-8
    HTTP-PRINCIPAL-ADOPTION-V1-13.
    """
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-7 HTTP-PRINCIPAL-ADOPTION-V1-8"""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        first = await capture(f, tenant)
        async with f() as s, s.begin():
            result = await apply(s, first, kind=kind, status=status)
            person_row = await principal(s, tenant)
            assert result.principal_id == person_row.id and isinstance(person_row.id, uuid.UUID)
            assert (person_row.type, person_row.status, person_row.authorization_version) == (
                kind,
                status,
                1,
            )
            assert result.authorization_version == 1 and result.lifecycle_generation == 2
            assert result.outcome == ("resolved" if status == "active" else "inactive")
            audit = (
                await s.scalars(
                    select(IdentityAuditEvent).where(
                        IdentityAuditEvent.event_type == "identity.http_principal_observed",
                        IdentityAuditEvent.tenant_id == tenant,
                    )
                )
            ).one()
            assert audit.principal_id == person_row.id
            assert audit.details == {
                "status": status,
                "authorization_version": 1,
                "lifecycle_generation": 2,
                "revoked_session_count": 0,
            }
            saved_id = person_row.id
        async with f() as s, s.begin():
            result = await apply(s, await capture(f, tenant), kind=kind, status=status)
            assert (
                result.principal_id,
                result.authorization_version,
                result.lifecycle_generation,
            ) == (saved_id, 1, 3)
        async with f() as s, s.begin():
            result = await apply(
                s,
                await capture(f, tenant),
                kind=kind,
                status="revoked" if status == "active" else "active",
            )
            assert (
                result.principal_id,
                result.authorization_version,
                result.lifecycle_generation,
            ) == (saved_id, 2, 4)

    run(body)


@pytest.mark.parametrize(
    ("stored", "observed", "code"),
    [
        ("human", "service", "principal_kind_conflict"),
        ("service", "human", "principal_kind_conflict"),
        ("human", "agent", "unsupported_principal_kind"),
    ],
)
def test_kind_conflict_never_retags_or_leaves_partial_audit(
    clean_db: None, stored: str, observed: str, code: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-7 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-7 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, saved_id = await seed(f, status="active", kind=stored)
        await adopt(f, tenant)
        cap = await capture(f, tenant)
        async with f() as s, s.begin():
            with pytest.raises(service.PrincipalAuthorityRefusal, match=f"^{code}$"):
                await apply(s, cap, kind=observed)
        async with f() as s:
            row = await principal(s, tenant)
            assert (row.id, row.type, row.status, row.authorization_version) == (
                saved_id,
                stored,
                "active",
                4,
            )
            assert (row.display_name, row.email, row.last_seen_at) == (
                "Stored name",
                "stored@example.test",
                NOW,
            )
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(IdentityAuditEvent)
                    .where(IdentityAuditEvent.tenant_id == tenant)
                )
                == 1
            )

    run(body)


def test_unresolved_writes_nothing_and_does_not_consume_generation(clean_db: None) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-7."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-7."""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        cap = await capture(f, tenant)
        async with f() as s, s.begin():
            result = await apply(s, cap, outcome="unresolved")
            assert (
                result.outcome,
                result.principal_id,
                result.authorization_version,
                result.lifecycle_generation,
            ) == ("unresolved", None, None, 1)
            assert await principal(s, tenant) is None
        assert (await capture(f, tenant)).lifecycle_generation == 1
        async with f() as s, s.begin():
            assert (await apply(s, cap)).lifecycle_generation == 2

    run(body)


@pytest.mark.parametrize("inactive", ["disabled", "revoked"])
def test_adoption_revokes_already_inactive_without_status_delta_or_legacy_effect(
    clean_db: None, inactive: str
) -> None:
    """
    @spec HTTP-PRINCIPAL-ADOPTION-V1-5 HTTP-PRINCIPAL-ADOPTION-V1-8
    HTTP-PRINCIPAL-ADOPTION-V1-11.
    """
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-5 HTTP-PRINCIPAL-ADOPTION-V1-8"""
        tenant, person_id = await seed(f, local=True, status=inactive)
        old = datetime(2020, 1, 1)
        async with f() as s, s.begin():
            _, live = await cookie(s, person_id)
            _, unexchanged = await cookie(s, person_id, consumed=False)
            _, revoked = await cookie(s, person_id, revoked=old)
            _, legacy = await cookie(s, None)
            foreign = Principal(
                tenant_id=tenant,
                idp_issuer="https://foreign.example.test",
                idp_subject="SampleSubject",
                type="human",
                status="disabled",
            )
            s.add(foreign)
            await s.flush()
            _, foreign_cookie = await cookie(s, foreign.id)
        result = await adopt(f, tenant, local=True)
        assert result.revoked_session_count == 2
        async with f() as s:
            assert (await s.get(Principal, person_id)).authorization_version == 4
            assert (await s.get(ConsoleSession, live.id)).revoked_at is not None
            assert (await s.get(ConsoleSession, unexchanged.id)).revoked_at is not None
            assert (await s.get(ConsoleSession, revoked.id)).revoked_at == old
            assert (await s.get(ConsoleSession, legacy.id)).revoked_at is None
            assert (await s.get(ConsoleSession, foreign_cookie.id)).revoked_at is None

    run(body)


def test_repeat_inactive_catches_inconsistent_cookie_and_reactivation_never_revives(
    clean_db: None,
) -> None:
    """
    @spec HTTP-PRINCIPAL-ADOPTION-V1-7 HTTP-PRINCIPAL-ADOPTION-V1-8
    HTTP-PRINCIPAL-ADOPTION-V1-11.
    """
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-7 HTTP-PRINCIPAL-ADOPTION-V1-8"""
        tenant, person_id = await seed(f, status="active")
        await adopt(f, tenant)
        async with f() as s, s.begin():
            _, first = await cookie(s, person_id)
        async with f() as s, s.begin():
            await apply(s, await capture(f, tenant), status="disabled")
        async with f() as s, s.begin():
            _, inconsistent = await cookie(s, person_id)
            first_stamp = (await s.get(ConsoleSession, first.id)).revoked_at
        async with f() as s, s.begin():
            result = await apply(s, await capture(f, tenant), status="disabled")
            assert result.authorization_version == 5
        async with f() as s, s.begin():
            await apply(s, await capture(f, tenant), status="active")
        async with f() as s:
            assert (await s.get(ConsoleSession, first.id)).revoked_at == first_stamp
            assert (await s.get(ConsoleSession, inconsistent.id)).revoked_at is not None
            fresh = await console.resolve_principal(s, claims(), tenant_id=tenant)
            token, new = await console.create_principal_console_session(s, fresh)
            assert token and new.revoked_at is None

    run(body)


@pytest.mark.parametrize("failure", ["audit", "session"])
def test_injected_failure_rolls_back_status_generation_revocations_and_audit(
    clean_db: None, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    """
    @spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-8
    HTTP-PRINCIPAL-ADOPTION-V1-13.
    """
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-8"""
        tenant, person_id = await seed(f, status="active")
        await adopt(f, tenant)
        async with f() as s, s.begin():
            _, row = await cookie(s, person_id)
        cap = await capture(f, tenant)
        original = identity_audit.record

        def fail_audit(*args: Any, **kwargs: Any) -> Any:
            """@spec HTTP-PRINCIPAL-ADOPTION-V1-8."""
            if kwargs.get("event_type") == "identity.http_principal_observed":
                raise RuntimeError("synthetic audit failure")
            return original(*args, **kwargs)

        def fail_session(
            conn: Any, cursor: Any, statement: str, params: Any, context: Any, many: bool
        ) -> None:
            """@spec HTTP-PRINCIPAL-ADOPTION-V1-8."""
            if statement.lstrip().upper().startswith("UPDATE") and "console_sessions" in statement:
                raise RuntimeError("synthetic session failure")

        if failure == "audit":
            monkeypatch.setattr(identity_audit, "record", fail_audit)
        async with f() as s:
            engine = s.bind.sync_engine
            if failure == "session":
                event.listen(engine, "before_cursor_execute", fail_session)
            try:
                async with s.begin():
                    with pytest.raises(RuntimeError, match="synthetic .* failure"):
                        await apply(s, cap, status="disabled")
            finally:
                if failure == "session":
                    event.remove(engine, "before_cursor_execute", fail_session)
        async with f() as s:
            person_row = await s.get(Principal, person_id)
            assert (person_row.status, person_row.authorization_version) == ("active", 4)
            assert (await s.get(TenantIdentityProvider, tenant)).lifecycle_generation == 1
            assert (await s.get(ConsoleSession, row.id)).revoked_at is None
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(IdentityAuditEvent)
                    .where(IdentityAuditEvent.tenant_id == tenant)
                )
                == 1
            )

    run(body)


@pytest.mark.parametrize("mutation", ["issuer", "selection", "corrupt", "local_null"])
def test_persisted_invalid_state_refuses_without_local_fallback(
    clean_db: None, mutation: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-2 HTTP-PRINCIPAL-ADOPTION-V1-6."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-2 HTTP-PRINCIPAL-ADOPTION-V1-6."""
        tenant, _ = await seed(f, local=True)
        await adopt(f, tenant, local=True)
        async with f() as s, s.begin():
            row = await s.get(TenantIdentityProvider, tenant)
            if mutation == "issuer":
                row.issuer = "https://other.example.test"
            elif mutation == "selection":
                row.http_selection = selection(issuer="https://other.example.test").model_dump(
                    mode="json"
                )
            elif mutation == "corrupt":
                row.http_selection = {"mode": "http", "synthetic-private-path": "secret-marker"}
            else:
                # The database must reject a local NULL even for direct SQL.
                with pytest.raises(Exception) as exc:
                    async with s.begin_nested():
                        await s.execute(
                            text(
                                "UPDATE curie.tenant_identity_providers "
                                "SET http_selection=NULL, managed_by=NULL WHERE tenant_id=:id"
                            ),
                            {"id": tenant},
                        )
                assert "CheckViolation" in str(exc.value) or "check constraint" in str(exc.value)
                return
        async with f() as s:
            with pytest.raises(
                service.PrincipalAuthorityRefusal, match="^invalid_authority_state$"
            ):
                await service.read_authority(s, tenant_id=tenant)
        with pytest.raises(service.PrincipalAuthorityRefusal, match="^invalid_authority_state$"):
            await capture(f, tenant)

    run(body)


def test_new_service_has_no_production_consumer_transport_or_secret_loader() -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-1 HTTP-PRINCIPAL-ADOPTION-V1-12."""
    service = api()
    root = Path(__file__).resolve().parents[3]
    source = Path(service.__file__).read_text()
    tree = ast.parse(source)
    imports = [node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))]
    names = {node.module or "" for node in imports if isinstance(node, ast.ImportFrom)} | {
        alias.name for node in imports if isinstance(node, ast.Import) for alias in node.names
    }
    assert not any(
        name.split(".")[0] in {"httpx", "requests", "aiohttp", "socket"} for name in names
    )
    for path in [
        *(root / "apps/api/src/curie_api/routers").rglob("*.py"),
        *(root / "apps/worker/src").rglob("*.py"),
        *(root / "apps/dispatcher/src").rglob("*.py"),
    ]:
        code = path.read_text()
        assert "apply_http_principal" not in code and "adopt_http_authority" not in code, str(
            path.relative_to(root)
        )
    forbidden = {"read_bytes", "read_text", "getenv", "open", "load_secret", "resolve_credential"}
    calls = {
        node.func.id if isinstance(node.func, ast.Name) else node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, (ast.Name, ast.Attribute))
    }
    assert not calls & forbidden
    assert "urllib.request" not in names
    assignments = {
        item.id
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        for item in ast.walk(node)
        if isinstance(item, ast.Name) and isinstance(item.ctx, ast.Store)
    }
    assert not assignments & {"active_consumer", "credential_checksum", "ready", "qualified"}


@pytest.mark.parametrize(
    ("change", "code"),
    [
        ("endpoint", "stale_authority_version"),
        ("reference", "stale_authority_version"),
        ("credential_generation", "stale_authority_version"),
        ("freshness", "stale_authority_version"),
        ("version", "stale_authority_version"),
        ("local_owner", "authority_scope_mismatch"),
        ("tenant", "tenant_unavailable"),
        ("lifecycle", "stale_lifecycle_generation"),
    ],
)
def test_complete_capture_cas_detects_unversioned_internal_changes(
    clean_db: None, change: str, code: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6. Corrupt-state injection uses real product columns."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-6. Corrupt-state injection uses real product columns."""
        tenant, _ = await seed(f, local=True)
        await adopt(f, tenant, local=True)
        cap = await capture(f, tenant)
        async with f() as s, s.begin():
            row = await s.get(TenantIdentityProvider, tenant)
            value = dict(row.http_selection)
            if change == "endpoint":
                value["lookup_url"] = "https://other.example.test/lookup"
            elif change == "reference":
                value["credential_ref"] = {"id": "other-synthetic-ref", "generation": 1}
            elif change == "credential_generation":
                value["credential_ref"] = {"id": "synthetic-secret-ref", "generation": 2}
            elif change == "freshness":
                value["max_clock_skew_ms"] = 2
            elif change == "version":
                row.version += 1
            elif change == "local_owner":
                row.http_selection = None
            elif change == "tenant":
                (await s.get(Tenant, tenant)).status = "suspended"
            else:
                row.lifecycle_generation += 1
            if change in {"endpoint", "reference", "credential_generation", "freshness"}:
                row.http_selection = value
        async with f() as s, s.begin():
            with pytest.raises(service.PrincipalAuthorityRefusal, match=f"^{code}$"):
                await apply(s, cap)
        async with f() as s:
            assert await principal(s, tenant) is None
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(IdentityAuditEvent)
                    .where(IdentityAuditEvent.tenant_id == tenant)
                )
                == 1
            )

    run(body)


@pytest.mark.parametrize(
    ("attack", "code"),
    [
        ("request_version", "stale_authority_version"),
        ("request_tenant", "authority_scope_mismatch"),
        ("request_issuer", "authority_scope_mismatch"),
        ("echo_sender", "identity_facts_scope_mismatch"),
        ("echo_request", "identity_facts_scope_mismatch"),
        ("echo_purpose", "identity_facts_scope_mismatch"),
        ("malformed", "identity_facts_invalid_response"),
        ("unavailable", "identity_facts_unavailable"),
        ("identity_expiry", "identity_facts_identity_stale"),
        ("membership_expiry", "identity_facts_membership_stale"),
    ],
)
def test_request_echo_parse_and_expiry_refuse_before_any_write(
    clean_db: None, attack: str, code: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6 HTTP-PRINCIPAL-ADOPTION-V1-13."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-6 HTTP-PRINCIPAL-ADOPTION-V1-13."""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        cap = await capture(f, tenant)
        request, response = lookup(cap, outcome="unavailable" if attack == "unavailable" else None)
        request_value = request.model_dump(mode="json")
        if attack == "request_version":
            request_value["authority_version"] += 1
        elif attack == "request_tenant":
            request_value["scope"]["tenant_id"] = str(uuid.uuid4())
        elif attack == "request_issuer":
            request_value["scope"]["expected_issuer"] = "https://other.example.test"
        elif attack == "echo_sender":
            response["scope"]["sender"]["native_id"] = "OtherAccount"
        elif attack == "echo_request":
            response["request_id"] = str(uuid.uuid4())
        elif attack == "echo_purpose":
            response["purpose"] = "approval"
        elif attack == "identity_expiry":
            response["identity_observation"]["as_of"] = "2030-01-01T11:59:58.999999Z"
        elif attack == "membership_expiry":
            response["membership"]["observation"]["as_of"] = "2030-01-01T11:59:57.999999Z"
        request = type(request).model_validate(request_value)
        raw = (
            b"{synthetic malformed response"
            if attack == "malformed"
            else json.dumps(response).encode()
        )
        async with f() as s, s.begin():
            with pytest.raises(service.PrincipalAuthorityRefusal, match=f"^{code}$"):
                await service.apply_http_principal(
                    s, capture=cap, request=request, response_bytes=raw, clock=lambda: NOW
                )
        async with f() as s:
            assert await principal(s, tenant) is None
            assert (await s.get(TenantIdentityProvider, tenant)).lifecycle_generation == 1
            assert (
                await s.scalar(
                    select(func.count())
                    .select_from(IdentityAuditEvent)
                    .where(IdentityAuditEvent.tenant_id == tenant)
                )
                == 1
            )

    run(body)


@pytest.mark.parametrize("clock_value", [datetime(2030, 1, 1), "2030-01-01T12:00:01Z", None])
def test_invalid_clock_is_bounded_and_writes_nothing(clean_db: None, clock_value: Any) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-6."""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        cap = await capture(f, tenant)
        async with f() as s, s.begin():
            with pytest.raises(
                service.PrincipalAuthorityRefusal, match="^invalid_authority_input$"
            ):
                await apply(s, cap, clock=lambda: clock_value)
        assert (await capture(f, tenant)).lifecycle_generation == 1

    run(body)


def test_sequential_older_but_fresh_result_is_the_explicit_source_ordering_limit(
    clean_db: None,
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-12. Core generation is not a provider revision."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-12. Core generation is not a provider revision."""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        async with f() as s, s.begin():
            first = await apply(s, await capture(f, tenant), status="disabled", as_of=NOW)
        async with f() as s, s.begin():
            second = await apply(
                s,
                await capture(f, tenant),
                status="active",
                as_of=NOW - timedelta(milliseconds=500),
            )
            assert first.principal_id == second.principal_id
            assert second.authorization_version == 2 and second.lifecycle_generation == 3
        async with f() as s:
            assert (await principal(s, tenant)).status == "active"

    run(body)


@pytest.mark.parametrize("kind", ["human", "service"])
def test_matching_existing_principal_keeps_uuid_and_profile_on_success(
    clean_db: None, kind: str
) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-7."""
    api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-7."""
        tenant, person_id = await seed(f, status="active", kind=kind)
        await adopt(f, tenant)
        cap = await capture(f, tenant)
        async with f() as s, s.begin():
            result = await apply(s, cap, kind=kind)
            person_row = await principal(s, tenant)
            assert (result.principal_id, result.authorization_version) == (person_id, 4)
            assert (person_row.display_name, person_row.email, person_row.last_seen_at) == (
                "Stored name",
                "stored@example.test",
                NOW,
            )
            assert result.lifecycle_generation == 2

    run(body)


@pytest.mark.parametrize("attack", ["capture", "request"])
def test_constructed_capture_and_request_are_revalidated(clean_db: None, attack: str) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-6."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-3 HTTP-PRINCIPAL-ADOPTION-V1-6."""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        cap = await capture(f, tenant)
        request, response = lookup(cap)
        if attack == "capture":
            object.__setattr__(cap.configuration.selection.credential_ref, "generation", True)
        else:
            request = type(request).model_construct(**(request.__dict__ | {"purpose": "login"}))
        async with f() as s, s.begin():
            with pytest.raises(
                service.PrincipalAuthorityRefusal, match="^invalid_authority_input$"
            ):
                await service.apply_http_principal(
                    s,
                    capture=cap,
                    request=request,
                    response_bytes=json.dumps(response).encode(),
                    clock=lambda: NOW,
                )
        async with f() as s:
            assert await principal(s, tenant) is None
            assert (await s.get(TenantIdentityProvider, tenant)).lifecycle_generation == 1

    run(body)


@pytest.mark.parametrize("mode", ["local", "foreign_issuer", "reverse"])
def test_replace_requires_same_http_owner_and_never_reverses(clean_db: None, mode: str) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-5 HTTP-PRINCIPAL-ADOPTION-V1-12."""
    from curie_api.schemas.http_authority import LocalProjectionSelectionV1

    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-5 HTTP-PRINCIPAL-ADOPTION-V1-12."""
        tenant, _ = await seed(f, local=True)
        if mode != "local":
            await adopt(f, tenant, local=True)
        value: Any = (
            selection(issuer="https://other.example.test")
            if mode == "foreign_issuer"
            else selection()
        )
        if mode == "reverse":
            value = LocalProjectionSelectionV1(
                mode="local_projection",
                issuer=ISSUER,
                governed_domains=["example.test"],
                managed_by="synthetic-directory",
            )
        expected = {
            "local": "http_authority_required",
            "foreign_issuer": "authority_scope_mismatch",
            "reverse": "invalid_authority_input",
        }[mode]
        async with f() as s, s.begin():
            with pytest.raises(service.PrincipalAuthorityRefusal, match=f"^{expected}$"):
                await service.replace_http_configuration(
                    s,
                    tenant_id=tenant,
                    expected_version=3 if mode == "local" else 4,
                    expected_lifecycle_generation=0 if mode == "local" else 1,
                    selection=value,
                )

    run(body)


def test_all_canonical_insert_status_and_principal_session_mints_are_in_inventory() -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-9 HTTP-PRINCIPAL-ADOPTION-V1-11."""
    api()
    root = Path(__file__).resolve().parents[3] / "apps/api/src/curie_api"
    owners = {
        "directory_service.py",
        "directory_sync.py",
        "crud/console.py",
        "principal_authority.py",
    }
    observed: set[str] = set()
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                function = node.func.id if isinstance(node.func, ast.Name) else ""
                canonical = function == "Principal" or (
                    function in {"insert", "update"}
                    and node.args
                    and isinstance(node.args[0], ast.Name)
                    and node.args[0].id == "Principal"
                )
                session_mint = function == "ConsoleSession" and any(
                    item.arg == "principal_id" for item in node.keywords
                )
                if canonical or session_mint:
                    owner = str(path.relative_to(root))
                    observed.add(owner)
                    assert owner in owners, (
                        f"add participation test for canonical/session writer: {owner}"
                    )
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                uses_principal = any(
                    isinstance(item, ast.Name) and item.id == "Principal" for item in ast.walk(node)
                )
                assigns_status = any(
                    isinstance(item, ast.Attribute)
                    and item.attr == "status"
                    and isinstance(item.ctx, ast.Store)
                    for item in ast.walk(node)
                )
                if uses_principal and assigns_status:
                    owner = str(path.relative_to(root))
                    observed.add(owner)
                    assert owner in owners, f"add participation test for status writer: {owner}"
    assert owners <= observed


@pytest.mark.parametrize("observation", ["identity", "membership"])
def test_exact_t1_age_limit_is_accepted_independently(clean_db: None, observation: str) -> None:
    """@spec HTTP-PRINCIPAL-ADOPTION-V1-6. Preserve HTTP-IDENTITY-V1-4 equality."""
    service = api()

    async def body(f: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-PRINCIPAL-ADOPTION-V1-6."""
        tenant, _ = await seed(f)
        await adopt(f, tenant)
        cap = await capture(f, tenant)
        request, response = lookup(cap)
        if observation == "identity":
            response["identity_observation"]["as_of"] = "2030-01-01T11:59:59Z"
        else:
            response["membership"]["observation"]["as_of"] = "2030-01-01T11:59:58Z"
        async with f() as s, s.begin():
            result = await service.apply_http_principal(
                s,
                capture=cap,
                request=request,
                response_bytes=json.dumps(response).encode(),
                clock=lambda: NOW,
            )
            assert result.outcome == "resolved"
            assert result.principal_id is not None
            assert (result.authorization_version, result.lifecycle_generation) == (1, 2)

    run(body)
