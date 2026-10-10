"""Slack identity reports: #3039's core, and the guards it needs on #2909's routes.

The dispatcher calls ``auth.test`` with each Slack identity's own token and
reports the answer (ADR 0198 decision 4). ``report_slack_identity`` then, in
one transaction:

- records the answer as ``attributes["slack_auth_test"]``, the reserved key
  only this path writes;
- finds or creates the installation ``(tenant, slack, '', team_id)``;
- attaches an unattached identity, clears ``installation_mismatch`` when the
  identity is already attached to that installation, or sets it (keeping the
  attachment) when it is attached elsewhere;
- finds or creates the home namespace ``(tenant, slack, '', slack_workspace,
  team_id)``.

Lock order everywhere is installation row, then identity row (plan r4.2), so
concurrent reports, PATCHes and installation retargets serialize. The
concurrency tests drive two real PostgreSQL sessions on separate engines.

Every test here commits; a local fixture truncates the four tables and removes
the tenants this module creates, before and after each test.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

MARK = "isr-test-"
DEFAULT_TENANT_ID = "00000000-0000-0000-0000-000000000001"
HOME = "THOME0001"
OTHER = "TOTHER001"
MOVED = "TMOVED001"
GRID = "EGRID0001"
REPORT = "/identity/slack-reports"
IDENTITIES = "/channel-identities"
INSTALLATIONS = "/provider-installations"
RESOLVE = "/identity/resolve"
AUTH_TEST_CAPTURE: dict[str, Any] = json.loads(
    (Path(__file__).resolve().parent / "fixtures" / "slack_auth_test_capture.json").read_text()
)
EVIDENCE_KEYS = {
    "team_id",
    "enterprise_id",
    "enterprise_id_present",
    "is_enterprise_install",
    "reported_at",
}


# --- committed-state helpers -----------------------------------------------


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(statement), params or {})
                if not result.returns_rows:
                    return []
                return [dict(row) for row in result.mappings().all()]
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _cleanup() -> None:
    # Tolerates the new tables being absent, so a missing migration fails the
    # test body rather than every fixture.
    _sql(
        "DO $$ BEGIN "
        "IF to_regclass('curie.identity_links') IS NOT NULL THEN "
        "TRUNCATE curie.identity_links, curie.identity_namespaces; END IF; "
        "TRUNCATE curie.channel_identities, curie.provider_installations; END $$"
    )
    _sql("DELETE FROM curie.principals WHERE idp_subject LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.tenants WHERE deployment_id LIKE :mark", {"mark": f"{MARK}%"})


@pytest.fixture
def clean(migrated: None) -> Iterator[None]:
    _cleanup()
    yield
    _cleanup()


def _tenant() -> uuid.UUID:
    tenant_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"{MARK}{tenant_id}"},
    )
    return tenant_id


def _installation(tenant_id: uuid.UUID | str, team: str, *, authority: str = "") -> uuid.UUID:
    installation_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.provider_installations "
        "(id, tenant_id, provider, authority, external_account_id) "
        "VALUES (:id, :t, 'slack', :authority, :team)",
        {"id": installation_id, "t": tenant_id, "authority": authority, "team": team},
    )
    return installation_id


def _identity(
    tenant_id: uuid.UUID | str,
    *,
    name: str = "default",
    installation_id: uuid.UUID | None = None,
    mismatch: bool = False,
    attributes: dict[str, Any] | None = None,
) -> uuid.UUID:
    identity_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.channel_identities (id, tenant_id, provider, name, credential_ref, "
        "attributes, installation_mismatch, provider_installation_id) VALUES (:id, :t, "
        "'slack', :name, 'env:SLACK_BOT_TOKEN', CAST(:attributes AS jsonb), :mismatch, "
        ":installation)",
        {
            "id": identity_id,
            "t": tenant_id,
            "name": name,
            "attributes": json.dumps(
                attributes
                if attributes is not None
                else {"app_token_ref": "env:CURIE_SLACK_APP_TOKEN"}
            ),
            "mismatch": mismatch,
            "installation": installation_id,
        },
    )
    return identity_id


def _identity_row(identity_id: uuid.UUID | str) -> dict[str, Any]:
    (row,) = _sql(
        "SELECT provider_installation_id, installation_mismatch, attributes "
        "FROM curie.channel_identities WHERE id = :id",
        {"id": identity_id},
    )
    return row


def _installations(tenant_id: uuid.UUID | str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT id, provider, authority, external_account_id, status "
        "FROM curie.provider_installations WHERE tenant_id = :t ORDER BY external_account_id",
        {"t": tenant_id},
    )


def _namespaces(tenant_id: uuid.UUID | str) -> list[dict[str, Any]]:
    return _sql(
        "SELECT id, provider, authority, kind, key, status "
        "FROM curie.identity_namespaces WHERE tenant_id = :t ORDER BY key",
        {"t": tenant_id},
    )


def _report_kwargs(team: str = HOME, **overrides: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "team_id": team,
        "enterprise_id": None,
        "enterprise_id_present": True,
        "is_enterprise_install": False,
    }
    kwargs.update(overrides)
    return kwargs


async def _in_session(body: Callable[[AsyncSession], Awaitable[Any]]) -> Any:
    """One real session on its own engine (its own connection pool)."""

    engine = create_async_engine(get_settings().database_url)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            return await body(session)
    finally:
        await engine.dispose()


def _report(tenant_id: uuid.UUID | str, name: str = "default", **kwargs: Any) -> Any:
    from curie_api.identity.attach import report_slack_identity

    async def body(session: AsyncSession) -> Any:
        return await report_slack_identity(
            session,
            tenant_id=uuid.UUID(str(tenant_id)),
            name=name,
            **_report_kwargs(**kwargs),
        )

    return asyncio.run(_in_session(body))


# --- report_slack_identity, in process --------------------------------------


def test_first_report_attaches_and_creates_installation_and_namespace(clean: None) -> None:
    tenant_id = _tenant()
    identity_id = _identity(tenant_id)

    result = _report(tenant_id)

    assert result.identity_id == identity_id
    assert result.installation_mismatch is False
    (installation,) = _installations(tenant_id)
    assert installation["id"] == result.provider_installation_id
    assert (installation["provider"], installation["authority"]) == ("slack", "")
    assert installation["external_account_id"] == HOME
    assert installation["status"] == "connected"
    (namespace,) = _namespaces(tenant_id)
    assert namespace["id"] == result.namespace_id
    assert (namespace["provider"], namespace["authority"], namespace["kind"]) == (
        "slack",
        "",
        "slack_workspace",
    )
    assert (namespace["key"], namespace["status"]) == (HOME, "active")

    row = _identity_row(identity_id)
    assert row["provider_installation_id"] == installation["id"]
    assert row["installation_mismatch"] is False
    # The evidence is merged in; existing attributes survive.
    assert row["attributes"]["app_token_ref"] == "env:CURIE_SLACK_APP_TOKEN"
    evidence = row["attributes"]["slack_auth_test"]
    assert set(evidence) == EVIDENCE_KEYS
    assert evidence["team_id"] == HOME
    assert evidence["enterprise_id"] is None
    assert evidence["enterprise_id_present"] is True
    assert evidence["is_enterprise_install"] is False
    assert datetime.fromisoformat(evidence["reported_at"]).tzinfo is not None


def test_report_stores_what_auth_test_said_verbatim(clean: None) -> None:
    tenant_id = _tenant()
    identity_id = _identity(tenant_id)
    _report(tenant_id, enterprise_id=GRID, is_enterprise_install=True)
    evidence = _identity_row(identity_id)["attributes"]["slack_auth_test"]
    assert evidence["enterprise_id"] == GRID
    assert evidence["is_enterprise_install"] is True

    _report(tenant_id, enterprise_id=None, enterprise_id_present=False, is_enterprise_install=None)
    evidence = _identity_row(identity_id)["attributes"]["slack_auth_test"]
    assert evidence["enterprise_id"] is None
    assert evidence["enterprise_id_present"] is False
    assert evidence["is_enterprise_install"] is None


def test_repeat_report_is_idempotent(clean: None) -> None:
    tenant_id = _tenant()
    identity_id = _identity(tenant_id)
    first = _report(tenant_id)
    second = _report(tenant_id)
    assert second.provider_installation_id == first.provider_installation_id
    assert second.namespace_id == first.namespace_id
    assert second.installation_mismatch is False
    assert len(_installations(tenant_id)) == 1
    assert len(_namespaces(tenant_id)) == 1
    assert _identity_row(identity_id)["provider_installation_id"] == first.provider_installation_id


def test_report_reuses_an_existing_installation_and_namespace(clean: None) -> None:
    tenant_id = _tenant()
    installation_id = _installation(tenant_id, HOME)
    namespace_id = uuid.uuid4()
    _sql(
        "INSERT INTO curie.identity_namespaces (id, tenant_id, provider, kind, key) "
        "VALUES (:id, :t, 'slack', 'slack_workspace', :key)",
        {"id": namespace_id, "t": tenant_id, "key": HOME},
    )
    identity_id = _identity(tenant_id)
    result = _report(tenant_id)
    assert result.provider_installation_id == installation_id
    assert result.namespace_id == namespace_id
    assert _identity_row(identity_id)["provider_installation_id"] == installation_id


def test_report_never_crosses_a_tenant(clean: None) -> None:
    mine, theirs = _tenant(), _tenant()
    their_installation = _installation(theirs, HOME)
    identity_id = _identity(mine)
    result = _report(mine)
    assert result.provider_installation_id != their_installation
    assert _identity_row(identity_id)["provider_installation_id"] == result.provider_installation_id
    assert len(_installations(mine)) == 1


def test_same_team_report_clears_mismatch(clean: None) -> None:
    tenant_id = _tenant()
    installation_id = _installation(tenant_id, HOME)
    identity_id = _identity(tenant_id, installation_id=installation_id, mismatch=True)
    result = _report(tenant_id)
    assert result.installation_mismatch is False
    row = _identity_row(identity_id)
    assert row["installation_mismatch"] is False
    assert row["provider_installation_id"] == installation_id


def test_different_team_report_sets_mismatch_and_keeps_attachment(clean: None) -> None:
    tenant_id = _tenant()
    installation_id = _installation(tenant_id, HOME)
    identity_id = _identity(tenant_id, installation_id=installation_id)
    result = _report(tenant_id, team=OTHER)
    assert result.installation_mismatch is True
    row = _identity_row(identity_id)
    assert row["installation_mismatch"] is True
    assert row["provider_installation_id"] == installation_id
    assert row["attributes"]["slack_auth_test"]["team_id"] == OTHER


def test_report_for_an_unknown_identity_raises(clean: None) -> None:
    from curie_api.identity.attach import IdentityNotFound

    tenant_id = _tenant()
    _identity(tenant_id, name="someone-else")
    with pytest.raises(IdentityNotFound):
        _report(tenant_id, name="missing")
    assert _installations(tenant_id) == []
    assert _namespaces(tenant_id) == []


# --- concurrency: two real sessions on separate engines --------------------


async def _gather(*bodies: Callable[[AsyncSession], Awaitable[Any]]) -> list[Any]:
    return await asyncio.gather(*(_in_session(b) for b in bodies), return_exceptions=True)


def _report_body(tenant_id: uuid.UUID, team: str) -> Callable[[AsyncSession], Awaitable[Any]]:
    async def body(session: AsyncSession) -> Any:
        from curie_api.identity.attach import report_slack_identity

        return await report_slack_identity(
            session, tenant_id=tenant_id, name="default", **_report_kwargs(team)
        )

    return body


@pytest.mark.parametrize("attempt", range(3))
def test_concurrent_reports_with_different_teams_stay_consistent(clean: None, attempt: int) -> None:
    tenant_id = _tenant()
    identity_id = _identity(tenant_id)
    results = asyncio.run(_gather(_report_body(tenant_id, HOME), _report_body(tenant_id, OTHER)))
    for result in results:
        assert not isinstance(result, BaseException), repr(result)

    row = _identity_row(identity_id)
    installations = {i["id"]: i for i in _installations(tenant_id)}
    # One installation per reported team, never a duplicate.
    assert sorted(i["external_account_id"] for i in installations.values()) == [HOME, OTHER]
    attached = installations[row["provider_installation_id"]]
    evidence_team = row["attributes"]["slack_auth_test"]["team_id"]
    assert evidence_team in (HOME, OTHER)
    # Exactly one attachment, and the flag says whether it matches the evidence.
    assert row["installation_mismatch"] is (attached["external_account_id"] != evidence_team)
    assert row["installation_mismatch"] is True  # whichever came second disagreed


@pytest.mark.parametrize("attempt", range(3))
def test_concurrent_report_and_attributes_patch_keep_the_evidence(
    clean: None, attempt: int
) -> None:
    from curie_api.channel_identities import get_identity, update_identity
    from curie_api.schemas.channel_identities import ChannelIdentityUpdate

    tenant_id = _tenant()
    identity_id = _identity(tenant_id, attributes={"stale": "x"})

    async def patch(session: AsyncSession) -> Any:
        # Load first, as the router does: the snapshot predates the report.
        identity = await get_identity(session, identity_id)
        await asyncio.sleep(0)
        return await update_identity(
            session,
            identity,
            ChannelIdentityUpdate(attributes={"app_token_ref": "env:CURIE_SLACK_APP_TOKEN__2"}),
        )

    results = asyncio.run(_gather(_report_body(tenant_id, HOME), patch))
    for result in results:
        assert not isinstance(result, BaseException), repr(result)

    attributes = _identity_row(identity_id)["attributes"]
    assert attributes["app_token_ref"] == "env:CURIE_SLACK_APP_TOKEN__2"
    assert attributes["slack_auth_test"]["team_id"] == HOME
    assert set(attributes["slack_auth_test"]) == EVIDENCE_KEYS
    assert "stale" not in attributes


@pytest.mark.parametrize("attempt", range(3))
def test_concurrent_attach_and_installation_retarget_are_ordered(clean: None, attempt: int) -> None:
    from curie_api.provider_installations import (
        InstallationConflict,
        get_installation,
        update_installation,
    )
    from curie_api.schemas.provider_installations import ProviderInstallationUpdate

    tenant_id = _tenant()
    installation_id = _installation(tenant_id, HOME)
    identity_id = _identity(tenant_id)

    async def retarget(session: AsyncSession) -> Any:
        installation = await get_installation(session, installation_id)
        await asyncio.sleep(0)
        return await update_installation(
            session, installation, ProviderInstallationUpdate(external_account_id=MOVED)
        )

    report_result, retarget_result = asyncio.run(_gather(_report_body(tenant_id, HOME), retarget))
    assert not isinstance(report_result, BaseException), repr(report_result)
    if isinstance(retarget_result, BaseException):
        # The report attached first: the retarget is refused, not applied.
        assert isinstance(retarget_result, InstallationConflict), repr(retarget_result)
        (installation,) = _installations(tenant_id)
        assert installation["external_account_id"] == HOME
    row = _identity_row(identity_id)
    attached = {i["id"]: i for i in _installations(tenant_id)}[row["provider_installation_id"]]
    # Either order leaves the identity on an installation that IS its team.
    assert attached["external_account_id"] == HOME
    assert row["installation_mismatch"] is False
    if not isinstance(retarget_result, BaseException):
        # The retarget went first: the report found or created HOME afresh.
        assert row["provider_installation_id"] != installation_id


# --- the report route -------------------------------------------------------


@pytest.fixture
def api(clean: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    monkeypatch.setenv("SLACK_BOT_TOKEN", "")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        get_settings.cache_clear()


def _route_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"name": "default", **_report_kwargs()}
    body.update(overrides)
    return body


def test_report_route_requires_platform_key(api: TestClient) -> None:
    assert api.post(REPORT, json=_route_body()).status_code == 401
    assert api.post(REPORT, json=_route_body(), headers={"X-API-Key": "wrong"}).status_code == 401


def test_report_route_attaches(api: TestClient, auth_headers: dict[str, str]) -> None:
    identity_id = _identity(DEFAULT_TENANT_ID)
    response = api.post(REPORT, json=_route_body(), headers=auth_headers)
    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body) == {
        "identity_id",
        "provider_installation_id",
        "namespace_id",
        "installation_mismatch",
    }
    assert body["identity_id"] == str(identity_id)
    assert body["installation_mismatch"] is False
    assert (
        str(_identity_row(identity_id)["provider_installation_id"])
        == (body["provider_installation_id"])
    )
    assert [str(n["id"]) for n in _namespaces(DEFAULT_TENANT_ID)] == [body["namespace_id"]]


def test_report_route_explicit_tenant(api: TestClient, auth_headers: dict[str, str]) -> None:
    tenant_id = _tenant()
    identity_id = _identity(tenant_id, name="support")
    response = api.post(
        REPORT, json=_route_body(tenant_id=str(tenant_id), name="support"), headers=auth_headers
    )
    assert response.status_code == 200, response.text
    assert response.json()["identity_id"] == str(identity_id)
    assert _installations(DEFAULT_TENANT_ID) == []


def test_report_route_unknown_identity_is_404(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    # A known identity in another tenant first, so a 404 below means "no such
    # identity", not "no such route".
    tenant_id = _tenant()
    _identity(tenant_id)
    known = api.post(REPORT, json=_route_body(tenant_id=str(tenant_id)), headers=auth_headers)
    assert known.status_code == 200, known.text
    response = api.post(REPORT, json=_route_body(name="missing"), headers=auth_headers)
    assert response.status_code == 404
    assert _installations(DEFAULT_TENANT_ID) == []


CANARY = "canary-report-9f3a"


@pytest.mark.parametrize(
    "drop_or_set",
    [
        ("drop", "enterprise_id"),
        ("drop", "enterprise_id_present"),
        ("drop", "is_enterprise_install"),
        ("drop", "team_id"),
        ("set", {"team_id": "tbad"}),
        ("set", {"team_id": "T0"}),
        ("set", {"team_id": ""}),
        ("set", {"name": ""}),
        ("set", {"enterprise_id_present": None}),
        ("set", {"surprise": "x"}),
        ("set", {"enterprise_id": 123}),
        ("set", {"enterprise_id": True}),
        ("set", {"enterprise_id": [CANARY]}),
        ("set", {"enterprise_id": {"id": CANARY}}),
        ("set", {"team_id": "T" + "Z" * 64}),
        ("set", {"enterprise_id": "E" + "Z" * 64}),
    ],
    ids=[
        "missing-enterprise-id",
        "missing-enterprise-id-present",
        "missing-is-enterprise-install",
        "missing-team",
        "lowercase-team",
        "short-team",
        "empty-team",
        "empty-name",
        "null-enterprise-id-present",
        "extra-field",
        "enterprise-id-number",
        "enterprise-id-bool",
        "enterprise-id-list",
        "enterprise-id-object",
        "team-id-over-64",
        "enterprise-id-over-64",
    ],
)
def test_report_route_malformed_body_is_redacted_422(
    api: TestClient, auth_headers: dict[str, str], drop_or_set: tuple[str, Any]
) -> None:
    _identity(DEFAULT_TENANT_ID, name=CANARY)
    action, value = drop_or_set
    body = _route_body(name=CANARY)
    if action == "drop":
        body.pop(value)
    else:
        body.update(value)
    response = api.post(REPORT, json=body, headers=auth_headers)
    assert response.status_code == 422, response.text
    assert CANARY not in response.text
    assert "Z" * 64 not in response.text
    detail = response.json()["detail"]
    assert isinstance(detail, list) and detail
    for entry in detail:
        assert {"loc", "msg", "type"} <= set(entry)
        assert "input" not in entry
        assert "ctx" not in entry
    assert _installations(DEFAULT_TENANT_ID) == []


def test_report_route_ids_up_to_64_are_accepted(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    _identity(DEFAULT_TENANT_ID)
    response = api.post(
        REPORT,
        json=_route_body(team_id="T" + "Z" * 63, enterprise_id="E" + "Z" * 63),
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text


# --- guards on #2909's channel identity and installation routes -------------


def _create_identity(api: TestClient, headers: dict[str, str], **body: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"provider": "slack", "credential_ref": "env:SLACK_BOT_TOKEN"}
    payload.update(body)
    response = api.post(IDENTITIES, json=payload, headers=headers)
    assert response.status_code == 201, response.text
    return response.json()


def _route_report(api: TestClient, headers: dict[str, str], **overrides: Any) -> dict[str, Any]:
    response = api.post(REPORT, json=_route_body(**overrides), headers=headers)
    assert response.status_code == 200, response.text
    return response.json()


def _get_identity(api: TestClient, headers: dict[str, str], identity_id: str) -> dict[str, Any]:
    response = api.get(f"{IDENTITIES}/{identity_id}", headers=headers)
    assert response.status_code == 200, response.text
    return response.json()


def _mention_resolve_body(identity_name: str) -> dict[str, Any]:
    """An in-context app_mention, as the dispatcher sends it (capture shape)."""

    return {
        "provider": "slack",
        "channel_identity": identity_name,
        "slack": {
            "delivery": "app_mention",
            "user_id": "UALICE001",
            "event_team": HOME,
            "is_ext_shared_channel": False,
            "enterprise_ids": [],
        },
    }


FORGED = {
    "team_id": HOME,
    "enterprise_id": None,
    "enterprise_id_present": True,
    "is_enterprise_install": False,
    "reported_at": "2026-10-05T00:00:00+00:00",
}


def test_create_identity_cannot_set_the_reserved_key(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    response = api.post(
        IDENTITIES,
        json={"provider": "slack", "attributes": {"slack_auth_test": FORGED}},
        headers=auth_headers,
    )
    assert response.status_code == 422, response.text
    for entry in response.json()["detail"]:
        assert "input" not in entry
    assert api.get(IDENTITIES, headers=auth_headers).json() == []


def test_patch_identity_cannot_set_the_reserved_key(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create_identity(api, auth_headers)
    response = api.patch(
        f"{IDENTITIES}/{created['id']}",
        json={"attributes": {"app_token_ref": "env:X", "slack_auth_test": FORGED}},
        headers=auth_headers,
    )
    assert response.status_code == 422, response.text
    for entry in response.json()["detail"]:
        assert "input" not in entry
    assert _get_identity(api, auth_headers, created["id"])["attributes"] == {}


def test_patch_replacing_attributes_preserves_the_evidence(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create_identity(api, auth_headers, attributes={"old": "value"})
    _route_report(api, auth_headers)
    evidence = _get_identity(api, auth_headers, created["id"])["attributes"]["slack_auth_test"]

    response = api.patch(
        f"{IDENTITIES}/{created['id']}",
        json={"attributes": {"app_token_ref": "env:CURIE_SLACK_APP_TOKEN"}},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["attributes"] == {
        "app_token_ref": "env:CURIE_SLACK_APP_TOKEN",
        "slack_auth_test": evidence,
    }


def test_patch_unrelated_field_keeps_the_evidence(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create_identity(api, auth_headers)
    _route_report(api, auth_headers)
    response = api.patch(
        f"{IDENTITIES}/{created['id']}", json={"scopes": ["chat:write"]}, headers=auth_headers
    )
    assert response.status_code == 200, response.text
    assert "slack_auth_test" in response.json()["attributes"]


def test_patch_changing_credential_drops_the_evidence(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create_identity(api, auth_headers, attributes={"app_token_ref": "env:A"})
    _route_report(api, auth_headers)
    response = api.patch(
        f"{IDENTITIES}/{created['id']}",
        json={"credential_ref": "env:SLACK_BOT_TOKEN_ROTATED"},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    attributes = response.json()["attributes"]
    assert "slack_auth_test" not in attributes
    assert attributes["app_token_ref"] == "env:A"


def test_patch_changing_name_drops_the_evidence(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """L1 (rev 4.3): the evidence was reported under the old name's token; a
    rename drops it, as a credential change does, and an in-context mention
    then has nothing to stand on."""

    created = _create_identity(api, auth_headers, attributes={"app_token_ref": "env:A"})
    _route_report(api, auth_headers)
    # With the evidence, the mention gets past the subject step.
    before = api.post(RESOLVE, json=_mention_resolve_body("default"), headers=auth_headers)
    assert before.status_code == 200, before.text
    assert before.json()["reason"] == "no_link", before.json()

    response = api.patch(
        f"{IDENTITIES}/{created['id']}", json={"name": "renamed-bot"}, headers=auth_headers
    )
    assert response.status_code == 200, response.text
    attributes = response.json()["attributes"]
    assert "slack_auth_test" not in attributes
    assert attributes["app_token_ref"] == "env:A"

    # The rename also quarantines the attachment (fix review r5 #1), so the
    # refusal comes at the installation step, before any subject derivation.
    after = api.post(RESOLVE, json=_mention_resolve_body("renamed-bot"), headers=auth_headers)
    assert after.status_code == 200, after.text
    assert (after.json()["status"], after.json()["reason"]) == (
        "unresolved",
        "installation_mismatch",
    )


def _interaction_resolve_body(identity_name: str) -> dict[str, Any]:
    return {
        "provider": "slack",
        "channel_identity": identity_name,
        "slack": {
            "delivery": "block_actions",
            "user_id": "UALICE001",
            "interaction_user_team_id": "THOME0001",
        },
    }


def test_renaming_an_attached_identity_quarantines_it_until_a_matching_report(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """Fix review r5 #1: a rename means the row may now stand for another
    token, so an attached identity is quarantined. Interactions need no stored
    evidence, so without the quarantine they would still pass the installation
    check against the old attachment. A matching report under the new name
    clears it."""

    created = _create_identity(api, auth_headers, attributes={"app_token_ref": "env:A"})
    _route_report(api, auth_headers)
    before = api.post(RESOLVE, json=_interaction_resolve_body("default"), headers=auth_headers)
    assert before.json()["reason"] == "no_link", before.json()

    renamed = api.patch(
        f"{IDENTITIES}/{created['id']}", json={"name": "renamed-bot"}, headers=auth_headers
    )
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["installation_mismatch"] is True

    quarantined = api.post(
        RESOLVE, json=_interaction_resolve_body("renamed-bot"), headers=auth_headers
    )
    assert (quarantined.json()["status"], quarantined.json()["reason"]) == (
        "unresolved",
        "installation_mismatch",
    )

    _route_report(api, auth_headers, name="renamed-bot")
    cleared = api.post(RESOLVE, json=_interaction_resolve_body("renamed-bot"), headers=auth_headers)
    assert cleared.json()["reason"] == "no_link", cleared.json()


def test_renaming_an_unattached_identity_sets_no_quarantine(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create_identity(api, auth_headers)
    renamed = api.patch(
        f"{IDENTITIES}/{created['id']}", json={"name": "renamed-bot"}, headers=auth_headers
    )
    assert renamed.status_code == 200, renamed.text
    assert renamed.json()["installation_mismatch"] is False


def test_captured_auth_test_report_lets_an_in_context_mention_through(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """Rev 4.3: the observed auth.test (enterprise_id absent,
    is_enterprise_install false), reported as the dispatcher maps it, is
    non-Grid evidence: the mention gets past the subject step (no link is
    seeded, so it stops at no_link rather than subject_unidentified)."""

    _create_identity(api, auth_headers)
    report = _route_report(
        api,
        auth_headers,
        team_id=AUTH_TEST_CAPTURE["team_id"],
        enterprise_id=None,
        enterprise_id_present="enterprise_id" in AUTH_TEST_CAPTURE,
        is_enterprise_install=AUTH_TEST_CAPTURE["is_enterprise_install"],
    )
    assert report["installation_mismatch"] is False
    response = api.post(RESOLVE, json=_mention_resolve_body("default"), headers=auth_headers)
    assert response.status_code == 200, response.text
    assert (response.json()["status"], response.json()["reason"]) == ("unresolved", "no_link")


def test_patch_reattach_and_detach_recompute_mismatch(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create_identity(api, auth_headers)
    home = _route_report(api, auth_headers)["provider_installation_id"]
    # A later report from another team: attached to HOME, evidence says OTHER.
    other_report = _route_report(api, auth_headers, team_id=OTHER)
    assert other_report["installation_mismatch"] is True
    others = [i for i in _installations(DEFAULT_TENANT_ID) if i["external_account_id"] == OTHER]
    other = str(others[0]["id"]) if others else str(_installation(DEFAULT_TENANT_ID, OTHER))
    path = f"{IDENTITIES}/{created['id']}"

    # Reattach to the installation the evidence names: the mismatch clears.
    matching = api.patch(path, json={"provider_installation_id": other}, headers=auth_headers)
    assert matching.status_code == 200, matching.text
    assert matching.json()["provider_installation_id"] == other
    assert matching.json()["installation_mismatch"] is False

    # Reattach to one the evidence does not name: the mismatch is set.
    back = api.patch(path, json={"provider_installation_id": home}, headers=auth_headers)
    assert back.status_code == 200, back.text
    assert back.json()["installation_mismatch"] is True

    # Detach: nothing to mismatch.
    detached = api.patch(path, json={"provider_installation_id": None}, headers=auth_headers)
    assert detached.status_code == 200, detached.text
    assert detached.json()["provider_installation_id"] is None
    assert detached.json()["installation_mismatch"] is False


def test_patch_reattach_compares_authority_too(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    """The evidence names ('', team); an installation of the same team under
    another authority is not it."""

    created = _create_identity(api, auth_headers)
    _route_report(api, auth_headers)
    elsewhere = str(_installation(DEFAULT_TENANT_ID, HOME, authority="slack.example.gov"))
    response = api.patch(
        f"{IDENTITIES}/{created['id']}",
        json={"provider_installation_id": elsewhere},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["installation_mismatch"] is True


def test_patch_reattach_without_evidence_leaves_mismatch_alone(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    created = _create_identity(api, auth_headers)
    installation = str(_installation(DEFAULT_TENANT_ID, HOME))
    response = api.patch(
        f"{IDENTITIES}/{created['id']}",
        json={"provider_installation_id": installation},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["installation_mismatch"] is False


@pytest.mark.parametrize(
    "change",
    [{"external_account_id": MOVED}, {"authority": "slack.example.gov"}],
    ids=["external-account-id", "authority"],
)
def test_installation_retarget_while_attached_is_409(
    api: TestClient, auth_headers: dict[str, str], change: dict[str, str]
) -> None:
    _create_identity(api, auth_headers)
    installation_id = _route_report(api, auth_headers)["provider_installation_id"]
    path = f"{INSTALLATIONS}/{installation_id}"
    response = api.patch(path, json=change, headers=auth_headers)
    assert response.status_code == 409, response.text
    unchanged = api.get(path, headers=auth_headers).json()
    assert (unchanged["external_account_id"], unchanged["authority"]) == (HOME, "")

    # A cosmetic change to an attached installation is still allowed.
    renamed = api.patch(path, json={"display_name": "Home"}, headers=auth_headers)
    assert renamed.status_code == 200, renamed.text


def test_installation_retarget_when_unattached_is_allowed(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    installation_id = _installation(DEFAULT_TENANT_ID, HOME)
    response = api.patch(
        f"{INSTALLATIONS}/{installation_id}",
        json={"external_account_id": MOVED},
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["external_account_id"] == MOVED
