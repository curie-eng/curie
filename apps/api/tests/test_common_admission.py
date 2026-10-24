"""Real-route preservation evidence for the common invocation composition."""

import asyncio
import inspect
import logging
from collections.abc import Iterator
from typing import Any

import pytest
from curie_api import admission, team_authorization
from curie_api.config import get_settings
from curie_api.models import (
    Agent,
    AgentAccessPolicy,
    AgentChannel,
    ChannelIdentity,
    IdentityAuditEvent,
    IdentityConnectToken,
    IdentityLink,
    IdentityNamespace,
    Principal,
    PrincipalTeam,
    ProviderInstallation,
    Team,
    Tenant,
    TenantIdentityProvider,
)
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from .test_binding_caller_allowlist import (
    LISTED,
    LISTED_USER,
    OTHER_USER,
    _email_agent,
    _refused_points,
    _slack_agent,
    _sql,
    _turn,
    channels_client,  # noqa: F401 - real app fixture
    refused_metrics,  # noqa: F401 - real metric reader fixture
)
from .test_team_authorization import ISSUER, seed


@pytest.fixture
def team_route(clean_db: None) -> Iterator[dict[str, Any]]:
    """@spec COMMON-ADMISSION-EXTRACTION-2: scoped, current synthetic identity."""
    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def prepare() -> dict[str, Any]:
        async with factory() as session:
            person, team, _, agent, binding, evidence = await seed(session)
            await session.commit()
            return {
                "person": person,
                "team": team,
                "agent": agent,
                "binding": binding,
                "evidence": evidence.model_dump(),
            }

    route = asyncio.run(prepare())
    try:
        yield route
    finally:

        async def cleanup() -> None:
            async with factory() as session:
                models: tuple[Any, ...] = (
                    IdentityConnectToken,
                    IdentityAuditEvent,
                    IdentityLink,
                    PrincipalTeam,
                    AgentAccessPolicy,
                    AgentChannel,
                    ChannelIdentity,
                    ProviderInstallation,
                    IdentityNamespace,
                    Agent,
                    Team,
                    Principal,
                    TenantIdentityProvider,
                    Tenant,
                )
                for model in models:
                    key = model.id if model is Tenant else model.tenant_id
                    await session.execute(delete(model).where(key == route["person"].tenant_id))
                await session.commit()
            await engine.dispose()

        asyncio.run(cleanup())


def _query(route: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    binding = route["binding"]
    return {
        "kind": binding.kind,
        "address": binding.address,
        "adapter": binding.adapter,
        "callers": [LISTED_USER],
        "slack": route["evidence"],
        **overrides,
    }


@pytest.mark.parametrize("surface", ["port-email", "port-slack", "query-email", "query-slack"])
def test_both_http_routes_delegate_once_with_their_existing_evidence(
    channels_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    clean_db: None,
    monkeypatch: pytest.MonkeyPatch,
    surface: str,
) -> None:
    """@spec COMMON-ADMISSION-EXTRACTION-1"""
    service = getattr(admission, "evaluate_invocation", None)
    assert callable(service), "both routes need the shared admission.evaluate_invocation service"
    calls = []

    async def observed(*args: Any, **kwargs: Any) -> Any:
        bound = inspect.signature(service).bind(*args, **kwargs)
        bound.apply_defaults()
        calls.append(bound.arguments)
        return await service(*args, **kwargs)

    monkeypatch.setattr(admission, "evaluate_invocation", observed)
    is_email = surface.endswith("email")
    agent = (_email_agent if is_email else _slack_agent)(channels_client, auth_headers)
    binding = _sql("SELECT * FROM curie.agent_channels WHERE agent_id = :id", {"id": agent})[0]
    author = LISTED if is_email else LISTED_USER
    if surface.startswith("port"):
        result = channels_client.post(
            "/channels/turns",
            json=_turn(author, kind=binding["kind"], address=binding["address"]),
            headers=auth_headers,
        )
    else:
        payload = {"kind": binding["kind"], "address": binding["address"], "callers": [author]}
        if not is_email:
            payload["slack"] = {
                "delivery": "app_mention",
                "user_id": author,
                "event_user_team": "T0EXAMPLE1",
            }
        result = channels_client.post("/channels/admission", json=payload, headers=auth_headers)
    assert result.status_code == 200, result.text
    assert len(calls) == 1
    values = list(calls[0].values())
    rows = [value for value in values if isinstance(value, AgentChannel)]
    assert [row.id for row in rows] == [binding["id"]]
    assert [author] in values
    assert calls[0].get("email") == (author if surface == "port-email" else None)
    slack = calls[0].get("slack")
    assert (slack.model_dump() if slack else None) == (
        payload["slack"] if surface == "query-slack" else None
    )


@pytest.mark.parametrize(
    ("listed", "sender", "member", "allowed"),
    [
        (True, LISTED_USER, True, True),
        (False, LISTED_USER, True, False),
        (True, LISTED_USER, False, False),
        (None, LISTED_USER, False, False),
        (True, OTHER_USER, True, False),
    ],
)
def test_admission_preserves_list_team_and_and_the_evidence_sender_check(
    channels_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    team_route: dict[str, Any],
    listed: bool | None,
    sender: str,
    member: bool,
    allowed: bool,
) -> None:
    """@spec COMMON-ADMISSION-EXTRACTION-2"""
    route = team_route
    _sql(
        "UPDATE curie.agent_channels SET allowed_callers = CAST(:callers AS jsonb) WHERE id = :id",
        {
            "callers": None if listed is None else f'["{LISTED_USER if listed else OTHER_USER}"]',
            "id": route["binding"].id,
        },
    )
    if not member:
        _sql("DELETE FROM curie.principal_teams WHERE team_id = :id", {"id": route["team"].id})
    evidence = {**route["evidence"], "user_id": sender}
    response = channels_client.post(
        "/channels/admission", json=_query(route, slack=evidence), headers=auth_headers
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"allowed": allowed, "restricted": True, "install_restricted": True}
    assert response.headers["cache-control"] == "private, no-store"


def test_query_slack_identity_is_not_invented_for_a_channel_port_author(
    channels_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    team_route: dict[str, Any],
    valkey: Any,
    runs_stream: str,
    refused_metrics: Any,  # noqa: F811
) -> None:
    """@spec COMMON-ADMISSION-EXTRACTION-1 @spec COMMON-ADMISSION-EXTRACTION-4"""
    route = team_route
    member = channels_client.post("/channels/admission", json=_query(route), headers=auth_headers)
    assert member.json()["allowed"] is True
    body = _turn(LISTED_USER, kind="slack", address=route["binding"].address)
    prefixes = [f"curie:channel:{kind}:{route['binding'].id}:*" for kind in ("delivery", "backlog")]

    def owned_keys() -> set[str]:
        return {key for pattern in prefixes for key in valkey.scan_iter(match=pattern)}

    before = owned_keys()
    refused = channels_client.post("/channels/turns", json=body, headers=auth_headers)
    assert refused.status_code == 403 and refused.json() == {"detail": "caller_not_allowed"}
    assert owned_keys() == before
    assert valkey.xlen(runs_stream) == 0
    assert _refused_points(refused_metrics) == []
    assert (
        _sql(
            "SELECT * FROM curie.identity_audit_events WHERE tenant_id = :id",
            {"id": route["person"].tenant_id},
        )
        == []
    )
    assert (
        _sql(
            "SELECT * FROM curie.identity_connect_tokens WHERE tenant_id = :id",
            {"id": route["person"].tenant_id},
        )
        == []
    )
    _sql(
        "UPDATE curie.agent_access_policies SET invocation_team_id = NULL WHERE agent_id = :id",
        {"id": route["agent"].id},
    )
    try:
        accepted = channels_client.post("/channels/turns", json=body, headers=auth_headers)
        duplicate = channels_client.post("/channels/turns", json=body, headers=auth_headers)
        assert accepted.status_code == duplicate.status_code == 200
        assert accepted.json()["duplicate"] is False and duplicate.json()["duplicate"] is True
        assert valkey.xlen(runs_stream) == 1
    finally:
        owned = owned_keys() - before
        if owned:
            valkey.delete(*owned)


@pytest.mark.parametrize("surface", ["port", "query"])
def test_list_refusal_observation_precedes_surface_specific_team_work(
    channels_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    team_route: dict[str, Any],
    refused_metrics: Any,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    surface: str,
) -> None:
    """@spec COMMON-ADMISSION-EXTRACTION-3"""
    route = team_route
    _sql(
        "UPDATE curie.agent_channels SET allowed_callers = '[\"U0EXAMPLE2\"]'::jsonb "
        "WHERE id = :id",
        {"id": route["binding"].id},
    )
    original = team_authorization.check_binding
    seen = []

    async def observed(*args: Any, **kwargs: Any) -> Any:
        refusals = [r for r in caplog.records if "caller refused" in r.getMessage()]
        assert [r.levelno for r in refusals] == [logging.DEBUG]
        seen.append(True)
        return await original(*args, **kwargs)

    monkeypatch.setattr(team_authorization, "check_binding", observed)
    with caplog.at_level(logging.DEBUG, logger="curie_api.routers.channels"):
        if surface == "port":
            result = channels_client.post(
                "/channels/turns",
                json=_turn(LISTED_USER, kind="slack", address=route["binding"].address),
                headers=auth_headers,
            )
            assert result.status_code == 403
            assert seen == []
        else:
            result = channels_client.post(
                "/channels/admission", json=_query(route), headers=auth_headers
            )
            assert result.status_code == 200 and not result.json()["allowed"]
            assert seen == [True]
    refusals = [r for r in caplog.records if "caller refused" in r.getMessage()]
    assert [r.levelno for r in refusals] == [logging.INFO if surface == "port" else logging.DEBUG]
    assert LISTED_USER not in refusals[0].getMessage()
    assert _refused_points(refused_metrics) == (
        [(1, {"service.name": "curie-api", "reason": "caller_not_allowed"})]
        if surface == "port"
        else []
    )
    audit = _sql(
        "SELECT * FROM curie.identity_audit_events WHERE tenant_id = :id",
        {"id": route["person"].tenant_id},
    )
    if surface == "query":
        assert len(audit) == 1
        assert audit[0]["actor"] == "channel-admission"
        assert audit[0]["principal_id"] == route["person"].id
        assert audit[0]["team_id"] == route["team"].id
        assert audit[0]["details"] == {"reason": "member", "epoch": 1, "digest": "example-digest"}
    else:
        assert audit == []
    assert "connect_url" not in result.json()


def test_query_does_not_gain_email_resolution_from_its_callers(
    channels_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    team_route: dict[str, Any],
) -> None:
    """@spec COMMON-ADMISSION-EXTRACTION-1"""
    route = team_route
    engine = create_async_engine(get_settings().database_url, poolclass=NullPool)

    async def prepare() -> None:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            binding = await session.get(AgentChannel, route["binding"].id)
            assert binding is not None
            binding.kind, binding.address = "email", "assistant@example.com"
            binding.endpoint, binding.adapter = "https://adapter.example.com/", "example-mail"
            ns = IdentityNamespace(
                tenant_id=binding.tenant_id,
                provider="email",
                authority=ISSUER,
                kind="email_domain",
                key="example.com",
            )
            session.add(ns)
            await session.flush()
            session.add(
                IdentityLink(
                    tenant_id=binding.tenant_id,
                    identity_namespace_id=ns.id,
                    provider_native_id="member@example.com",
                    principal_id=route["person"].id,
                    verification_source="idp_directory",
                    created_by_directory="example-directory",
                )
            )
            await session.commit()
        await engine.dispose()

    asyncio.run(prepare())
    query = channels_client.post(
        "/channels/admission",
        headers=auth_headers,
        json={
            "kind": "email",
            "address": "assistant@example.com",
            "callers": ["member@example.com"],
        },
    )
    assert query.status_code == 200 and query.json()["allowed"] is False
    port = channels_client.post(
        "/channels/turns",
        headers=auth_headers,
        json=_turn("member@example.com", address="assistant@example.com"),
    )
    assert port.status_code == 200, port.text


def test_a_second_listed_caller_cannot_supply_the_linked_senders_authority(
    channels_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    team_route: dict[str, Any],
) -> None:
    """@spec COMMON-ADMISSION-EXTRACTION-2"""
    route = team_route
    _sql(
        "UPDATE curie.agent_channels SET allowed_callers = '[\"U0EXAMPLE2\"]'::jsonb "
        "WHERE id = :id",
        {"id": route["binding"].id},
    )
    response = channels_client.post(
        "/channels/admission", json=_query(route, callers=[OTHER_USER]), headers=auth_headers
    )
    # The list admits OTHER_USER and the team admits the evidence's LISTED_USER.
    # The latter is absent from callers, so their two authorities cannot combine.
    assert response.status_code == 200
    assert response.json() == {"allowed": False, "restricted": True, "install_restricted": True}


@pytest.mark.parametrize("case", ["eligible", "list_refused", "sender_absent", "oidc_off", "http"])
def test_unlinked_sender_connection_keeps_its_existing_eligibility_conditions(
    channels_client: TestClient,  # noqa: F811
    auth_headers: dict[str, str],
    team_route: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    """@spec COMMON-ADMISSION-EXTRACTION-3"""
    route = team_route
    settings = get_settings()
    monkeypatch.setattr(settings, "oidc_issuer", "" if case == "oidc_off" else ISSUER)
    monkeypatch.setattr(
        settings,
        "oidc_redirect_uri",
        ("http" if case == "http" else "https") + "://console.example.com/console/oidc/callback",
    )
    if case == "list_refused":
        _sql(
            "UPDATE curie.agent_channels SET allowed_callers = '[\"U0EXAMPLE1\"]'::jsonb "
            "WHERE id = :id",
            {"id": route["binding"].id},
        )
    response = channels_client.post(
        "/channels/admission",
        headers=auth_headers,
        json=_query(
            route,
            callers=[LISTED_USER if case == "sender_absent" else OTHER_USER],
            slack={**route["evidence"], "user_id": OTHER_USER},
        ),
    )
    assert response.status_code == 200, response.text
    assert response.json()["allowed"] is False
    assert response.headers["cache-control"] == "private, no-store"
    tokens = _sql(
        "SELECT * FROM curie.identity_connect_tokens WHERE tenant_id = :id",
        {"id": route["person"].tenant_id},
    )
    if case == "eligible":
        assert response.json()["connect_url"].startswith(
            "https://console.example.com/identity/connect/"
        )
        assert len(tokens) == 1 and tokens[0]["provider_native_id"] == OTHER_USER
    else:
        assert "connect_url" not in response.json()
        assert tokens == []
