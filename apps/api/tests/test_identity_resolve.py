"""Principal resolution: the ordered checks and Slack's sender mapping (#2910).

ADR 0198 decision 7 fixes the order of the checks; the first failure answers
and nothing after it runs. ADR 0201 moves Slack's field mapping into the
realization, under these rules (plan r4, revisions 4.1 and 4.2):

- an interaction (``block_actions``, ``view_submission``) uses the documented
  ``user.team_id``;
- an event uses a documented ``user_team`` when it is present; one that
  disagrees with ``event.team`` is ``namespace_conflict``;
- otherwise ``event.team`` is used only for an ``app_mention`` in a channel
  positively marked not externally shared, outside Enterprise Grid by the
  identity's stored ``auth.test`` evidence, and only when it agrees with the
  receiving installation's team;
- any Enterprise Grid signal refuses, whatever else is present;
- nothing is ever guessed: not from an email, a display name, the
  installation's team, or a case-folded id.

The Slack cases are built from ``fixtures/slack_identity_capture.json``, the
anonymised capture of a real non-Grid workspace (ADR 0201 decision 3).

The in-process tests seed a fresh tenant inside a transaction that is always
rolled back; the session under test joins it through a savepoint. The route
tests commit, and a local fixture removes what they wrote.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from curie_api.config import get_settings
from curie_api.main import create_app
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

MARK = "ires-test-"
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "slack_identity_capture.json"
CAPTURE: dict[str, dict[str, Any]] = {
    record["delivery"]: record for record in json.loads(FIXTURE.read_text())
}
AUTH_TEST_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "slack_auth_test_capture.json"
AUTH_TEST_CAPTURE: dict[str, Any] = json.loads(AUTH_TEST_FIXTURE.read_text())
HOME = "THOME0001"
OTHER = "TOTHER001"
ALICE = "UALICE001"
GRID = "EGRID0001"
REPORTED_AT = "2026-10-05T00:00:00+00:00"

NON_GRID_EVIDENCE: dict[str, Any] = {
    "team_id": HOME,
    "enterprise_id": None,
    "enterprise_id_present": True,
    "is_enterprise_install": False,
    "reported_at": REPORTED_AT,
}
# What the dispatcher stores for the observed auth.test (rev 4.3): enterprise_id
# absent, so not present and None; is_enterprise_install present and false.
CAPTURED_EVIDENCE: dict[str, Any] = {
    "team_id": AUTH_TEST_CAPTURE["team_id"],
    "enterprise_id": AUTH_TEST_CAPTURE.get("enterprise_id"),
    "enterprise_id_present": "enterprise_id" in AUTH_TEST_CAPTURE,
    "is_enterprise_install": AUTH_TEST_CAPTURE["is_enterprise_install"],
    "reported_at": REPORTED_AT,
}
_UNSET = object()


def test_reasons_vocabulary() -> None:
    from curie_api.identity.service import REASONS

    assert REASONS == (
        "linked",
        "identity_not_found",
        "identity_inactive",
        "installation_mismatch",
        "installation_unattached",
        "installation_disconnected",
        "subject_unidentified",
        "namespace_conflict",
        "provider_unsupported",
        "namespace_unknown",
        "namespace_disabled",
        "no_link",
        "linked_to_bot",
        "principal_inactive",
    )


def test_fixture_is_anonymised() -> None:
    """The committed capture holds placeholders only (ADR 0201 decision 3)."""

    raw = FIXTURE.read_text()
    assert set(CAPTURE) == {"app_mention", "block_actions", "view_submission"}
    for forbidden in ('"text"', '"name"', '"channel"', '"api_app_id"', '"token"', "xox"):
        assert forbidden not in raw, forbidden
    mention = CAPTURE["app_mention"]
    assert mention["envelope"]["is_ext_shared_channel"] is False
    assert "user_team" not in mention["event"]
    assert mention["event"]["team"] == mention["envelope"]["team_id"] == HOME
    for kind in ("block_actions", "view_submission"):
        payload = CAPTURE[kind]["payload"]
        assert payload["enterprise"] is None
        assert payload["user"]["team_id"] == payload["team"]["id"] == HOME


def test_auth_test_capture_is_the_observed_non_grid_shape() -> None:
    """The observed auth.test of an ordinary workspace (rev 4.3): enterprise_id
    absent, is_enterprise_install present and false. Placeholders only."""

    assert set(AUTH_TEST_CAPTURE) == {
        "ok",
        "team_id",
        "user_id",
        "bot_id",
        "is_enterprise_install",
        "team",
        "url",
        "user",
    }
    assert "enterprise_id" not in AUTH_TEST_CAPTURE
    assert AUTH_TEST_CAPTURE["ok"] is True
    assert AUTH_TEST_CAPTURE["is_enterprise_install"] is False
    assert AUTH_TEST_CAPTURE["team_id"] == HOME
    for key in ("team", "url", "user"):
        assert AUTH_TEST_CAPTURE[key] == "<redacted>"
    assert "xox" not in AUTH_TEST_FIXTURE.read_text()
    assert CAPTURED_EVIDENCE == {
        "team_id": HOME,
        "enterprise_id": None,
        "enterprise_id_present": False,
        "is_enterprise_install": False,
        "reported_at": REPORTED_AT,
    }


# --- evidence built from the fixture ---------------------------------------


def _enterprise_ids(*candidates: Any) -> list[str]:
    return [value for value in candidates if isinstance(value, str) and value]


def mention_evidence(**overrides: Any) -> dict[str, Any]:
    record = CAPTURE["app_mention"]
    envelope, event = record["envelope"], record["event"]
    evidence = {
        "delivery": event["type"],
        "user_id": event["user"],
        "event_user_team": event.get("user_team"),
        "event_team": event.get("team"),
        "is_ext_shared_channel": envelope.get("is_ext_shared_channel"),
        "enterprise_ids": _enterprise_ids(
            envelope.get("enterprise_id"),
            event.get("enterprise_id"),
            *(a.get("enterprise_id") for a in envelope.get("authorizations", [])),
        ),
    }
    evidence.update(overrides)
    return evidence


def interaction_evidence(kind: str = "block_actions", **overrides: Any) -> dict[str, Any]:
    payload = CAPTURE[kind]["payload"]
    enterprise = payload.get("enterprise") or {}
    evidence = {
        "delivery": payload["type"],
        "user_id": payload["user"]["id"],
        "interaction_user_team_id": payload["user"].get("team_id"),
        "enterprise_ids": _enterprise_ids(
            enterprise.get("id"),
            payload["team"].get("enterprise_id"),
            payload["user"].get("enterprise_id"),
        ),
    }
    evidence.update(overrides)
    return evidence


def test_fixture_evidence_is_valid_slack_evidence() -> None:
    from curie_api.identity.slack import SlackEvidence

    mention = SlackEvidence(**mention_evidence())
    assert mention.delivery == "app_mention"
    assert mention.user_id == ALICE
    assert mention.event_team == HOME
    assert mention.event_user_team is None
    assert mention.is_ext_shared_channel is False
    assert mention.enterprise_ids == []
    for kind in ("block_actions", "view_submission"):
        interaction = SlackEvidence(**interaction_evidence(kind))
        assert interaction.interaction_user_team_id == HOME
        assert interaction.enterprise_ids == []


def test_slack_evidence_forbids_unknown_fields() -> None:
    from curie_api.identity.slack import SlackEvidence
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        SlackEvidence(**mention_evidence(source_team=HOME))


_LONG = "U" + "X" * 256  # 257 characters


@pytest.mark.parametrize(
    "field",
    [
        "delivery",
        "user_id",
        "event_user_team",
        "event_team",
        "interaction_user_team_id",
    ],
)
def test_slack_evidence_strings_are_capped_at_256(field: str) -> None:
    from curie_api.identity.slack import SlackEvidence
    from pydantic import ValidationError

    SlackEvidence(**mention_evidence(**{field: "U" + "X" * 255}))
    with pytest.raises(ValidationError):
        SlackEvidence(**mention_evidence(**{field: _LONG}))


def test_slack_evidence_enterprise_ids_are_capped() -> None:
    from curie_api.identity.slack import SlackEvidence
    from pydantic import ValidationError

    SlackEvidence(**mention_evidence(enterprise_ids=[f"E{i:08d}" for i in range(32)]))
    with pytest.raises(ValidationError):
        SlackEvidence(**mention_evidence(enterprise_ids=[f"E{i:08d}" for i in range(33)]))
    with pytest.raises(ValidationError):
        SlackEvidence(**mention_evidence(enterprise_ids=["E" + "X" * 256]))


def test_slack_constants() -> None:
    from curie_api.identity.slack import (
        AUTH_TEST_KEY,
        INTERACTION_DELIVERIES,
        OBSERVED_EVENT_TEAM_DELIVERIES,
    )

    assert AUTH_TEST_KEY == "slack_auth_test"
    assert frozenset({"block_actions", "view_submission"}) == INTERACTION_DELIVERIES
    assert frozenset({"app_mention"}) == OBSERVED_EVENT_TEAM_DELIVERIES


# --- the world a resolution runs against -----------------------------------


@dataclass
class World:
    tenant_id: uuid.UUID
    identity_name: str
    identity_id: uuid.UUID | None
    installation_id: uuid.UUID | None
    namespace_id: uuid.UUID | None
    principal_id: uuid.UUID
    creator_id: uuid.UUID


async def _exec(
    conn: AsyncConnection, statement: str, params: dict[str, Any] | None = None
) -> list[dict[str, Any]]:
    result = await conn.execute(text(statement), params or {})
    if not result.returns_rows:
        return []
    return [dict(row) for row in result.mappings().all()]


async def _principal(
    conn: AsyncConnection,
    tenant_id: uuid.UUID,
    *,
    status: str = "active",
    email: str | None = None,
    display_name: str | None = None,
    idp_subject: str | None = None,
) -> uuid.UUID:
    principal_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.principals (id, tenant_id, idp_subject, type, status, email, "
        "display_name) VALUES (:id, :t, :sub, 'human', :status, :email, :display_name)",
        {
            "id": principal_id,
            "t": tenant_id,
            "sub": idp_subject or f"{MARK}{principal_id}",
            "status": status,
            "email": email,
            "display_name": display_name,
        },
    )
    return principal_id


async def build_world(
    conn: AsyncConnection,
    *,
    provider: str = "slack",
    identity: bool = True,
    identity_status: str = "active",
    attached: bool = True,
    installation_status: str = "connected",
    installation_authority: str = "",
    installation_team: str = HOME,
    mismatch: bool = False,
    evidence: Any = _UNSET,
    namespace_key: str | None = HOME,
    namespace_status: str = "active",
    link: str | None = "principal",
    link_native: str = ALICE,
    principal_status: str = "active",
) -> World:
    """Seed one tenant with an identity, its installation, a namespace and a link.

    Every argument defaults to the resolvable case, so a test changes exactly
    the one thing it is about.
    """
    tenant_id = uuid.uuid4()
    await _exec(
        conn,
        "INSERT INTO curie.tenants (id, deployment_id, status) VALUES (:id, :d, 'active')",
        {"id": tenant_id, "d": f"{MARK}{tenant_id}"},
    )
    creator_id = await _principal(conn, tenant_id)
    principal_id = await _principal(conn, tenant_id, status=principal_status)

    installation_id: uuid.UUID | None = None
    if attached:
        installation_id = uuid.uuid4()
        disconnected = installation_status == "disconnected"
        await _exec(
            conn,
            "INSERT INTO curie.provider_installations (id, tenant_id, provider, authority, "
            "external_account_id, status, disconnected_at) VALUES (:id, :t, :provider, "
            ":authority, :account, :status, CASE WHEN :disc THEN now() END)",
            {
                "id": installation_id,
                "t": tenant_id,
                "provider": provider,
                "authority": installation_authority,
                "account": installation_team,
                "status": installation_status,
                "disc": disconnected,
            },
        )

    name = f"bot-{uuid.uuid4().hex[:8]}"
    identity_id: uuid.UUID | None = None
    if identity:
        identity_id = uuid.uuid4()
        attributes: dict[str, Any] = {"app_token_ref": "env:CURIE_SLACK_APP_TOKEN"}
        stored = NON_GRID_EVIDENCE if evidence is _UNSET else evidence
        if stored is not None:
            attributes["slack_auth_test"] = stored
        await _exec(
            conn,
            "INSERT INTO curie.channel_identities (id, tenant_id, provider, name, status, "
            "attributes, installation_mismatch, provider_installation_id) VALUES (:id, :t, "
            ":provider, :name, :status, CAST(:attributes AS jsonb), :mismatch, :installation)",
            {
                "id": identity_id,
                "t": tenant_id,
                "provider": provider,
                "name": name,
                "status": identity_status,
                "attributes": json.dumps(attributes),
                "mismatch": mismatch,
                "installation": installation_id,
            },
        )

    namespace_id: uuid.UUID | None = None
    if namespace_key is not None:
        namespace_id = uuid.uuid4()
        await _exec(
            conn,
            "INSERT INTO curie.identity_namespaces (id, tenant_id, provider, authority, kind, "
            "key, status) VALUES (:id, :t, 'slack', :authority, 'slack_workspace', :key, :status)",
            {
                "id": namespace_id,
                "t": tenant_id,
                "authority": installation_authority,
                "key": namespace_key,
                "status": namespace_status,
            },
        )
        if link is not None:
            bot_id: uuid.UUID | None = None
            if link == "bot":
                bot_id = uuid.uuid4()
                await _exec(
                    conn,
                    "INSERT INTO curie.agents (id, tenant_id, name) VALUES (:id, :t, :name)",
                    {"id": bot_id, "t": tenant_id, "name": f"{MARK}{bot_id}"},
                )
            await _exec(
                conn,
                "INSERT INTO curie.identity_links (id, tenant_id, identity_namespace_id, "
                "provider_native_id, principal_id, bot_id, verification_source, "
                "created_by_principal_id, revoked_at) VALUES (:id, :t, :ns, :native, :p, :bot, "
                "'admin_mapped', :creator, CASE WHEN :revoked THEN now() END)",
                {
                    "id": uuid.uuid4(),
                    "t": tenant_id,
                    "ns": namespace_id,
                    "native": link_native,
                    "p": None if link == "bot" else principal_id,
                    "bot": bot_id,
                    "creator": creator_id,
                    "revoked": link == "revoked",
                },
            )
    return World(
        tenant_id=tenant_id,
        identity_name=name,
        identity_id=identity_id,
        installation_id=installation_id,
        namespace_id=namespace_id,
        principal_id=principal_id,
        creator_id=creator_id,
    )


def _rolled_back(body: Callable[[AsyncConnection], Awaitable[Any]]) -> Any:
    async def run() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                trans = await conn.begin()
                try:
                    return await body(conn)
                finally:
                    await trans.rollback()
        finally:
            await engine.dispose()

    return asyncio.run(run())


async def _resolve(
    conn: AsyncConnection,
    world: World,
    slack: dict[str, Any] | None,
    *,
    provider: str = "slack",
    name: str | None = None,
) -> Any:
    from curie_api.identity.service import resolve_principal
    from curie_api.identity.slack import SlackEvidence

    session = AsyncSession(bind=conn, join_transaction_mode="create_savepoint")
    try:
        return await resolve_principal(
            session,
            tenant_id=world.tenant_id,
            provider=provider,
            channel_identity=name if name is not None else world.identity_name,
            slack=None if slack is None else SlackEvidence(**slack),
        )
    finally:
        await session.close()


def resolve_in_world(
    slack: dict[str, Any] | None = None,
    *,
    provider: str = "slack",
    name: str | None = None,
    **world_kwargs: Any,
) -> tuple[Any, World]:
    evidence = mention_evidence() if slack is None and provider == "slack" else slack

    async def body(conn: AsyncConnection) -> tuple[Any, World]:
        world = await build_world(conn, provider=provider, **world_kwargs)
        return await _resolve(conn, world, evidence, provider=provider, name=name), world

    return _rolled_back(body)


def _assert_unresolved(result: Any, reason: str) -> None:
    from curie_api.identity.service import REASONS

    assert reason in REASONS
    assert (result.status, result.reason) == ("unresolved", reason), result
    assert result.principal_id is None


# --- the resolvable baseline and every reason, in step order ---------------


@pytest.mark.parametrize(
    "evidence",
    [
        mention_evidence(),
        interaction_evidence("block_actions"),
        interaction_evidence("view_submission"),
    ],
    ids=["app_mention", "block_actions", "view_submission"],
)
def test_fixture_deliveries_resolve(migrated: None, evidence: dict[str, Any]) -> None:
    result, world = resolve_in_world(evidence)
    assert result.status == "resolved"
    assert result.reason == "linked"
    assert result.principal_id == world.principal_id
    assert result.channel_identity_id == world.identity_id
    assert result.namespace_id == world.namespace_id


def test_step1_identity_not_found(migrated: None) -> None:
    result, _ = resolve_in_world(name="no-such-identity")
    _assert_unresolved(result, "identity_not_found")
    assert result.channel_identity_id is None


def test_step1_identity_of_another_provider_is_not_found(migrated: None) -> None:
    """The identity is keyed by (tenant, provider, name): a Slack lookup never
    finds a GitHub identity of the same name."""

    async def body(conn: AsyncConnection) -> Any:
        world = await build_world(conn, provider="github")
        return await _resolve(conn, world, mention_evidence(), provider="slack")

    _assert_unresolved(_rolled_back(body), "identity_not_found")


def test_step1_identity_of_another_tenant_is_not_found(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> Any:
        world = await build_world(conn)
        elsewhere = await build_world(conn)
        world.tenant_id = elsewhere.tenant_id
        return await _resolve(conn, world, mention_evidence())

    _assert_unresolved(_rolled_back(body), "identity_not_found")


@pytest.mark.parametrize("status", ["disabled", "revoked"])
def test_step2_identity_inactive(migrated: None, status: str) -> None:
    result, _ = resolve_in_world(identity_status=status)
    _assert_unresolved(result, "identity_inactive")


def test_step2_inactive_beats_unattached(migrated: None) -> None:
    result, _ = resolve_in_world(identity_status="disabled", attached=False)
    _assert_unresolved(result, "identity_inactive")


def test_step3_installation_mismatch(migrated: None) -> None:
    result, _ = resolve_in_world(mismatch=True)
    _assert_unresolved(result, "installation_mismatch")


def test_step3_mismatch_beats_disconnected(migrated: None) -> None:
    result, _ = resolve_in_world(mismatch=True, installation_status="disconnected")
    _assert_unresolved(result, "installation_mismatch")


def test_step3_installation_unattached(migrated: None) -> None:
    result, _ = resolve_in_world(attached=False)
    _assert_unresolved(result, "installation_unattached")


def test_step3_installation_disconnected(migrated: None) -> None:
    result, _ = resolve_in_world(installation_status="disconnected")
    _assert_unresolved(result, "installation_disconnected")


def test_step3_disconnected_beats_subject_checks(migrated: None) -> None:
    result, _ = resolve_in_world(mention_evidence(user_id=None), installation_status="disconnected")
    _assert_unresolved(result, "installation_disconnected")


def test_step4_provider_unsupported(migrated: None) -> None:
    result, _ = resolve_in_world(provider="github", slack=None)
    _assert_unresolved(result, "provider_unsupported")


def test_step4_provider_unsupported_comes_after_identity_checks(migrated: None) -> None:
    disabled, _ = resolve_in_world(provider="github", identity_status="disabled")
    _assert_unresolved(disabled, "identity_inactive")
    unattached, _ = resolve_in_world(provider="github", attached=False)
    _assert_unresolved(unattached, "installation_unattached")


def test_step5_namespace_unknown(migrated: None) -> None:
    result, _ = resolve_in_world(namespace_key=None)
    _assert_unresolved(result, "namespace_unknown")


def test_step5_namespace_is_scoped_by_installation_authority(migrated: None) -> None:
    """``authority`` comes from the receiving installation, never the event: a
    namespace held under another authority is not this one."""

    async def body(conn: AsyncConnection) -> Any:
        world = await build_world(conn)
        await _exec(
            conn,
            "UPDATE curie.identity_namespaces SET authority = 'slack.example.gov' WHERE id = :id",
            {"id": world.namespace_id},
        )
        return await _resolve(conn, world, interaction_evidence())

    _assert_unresolved(_rolled_back(body), "namespace_unknown")


def test_step6_namespace_disabled(migrated: None) -> None:
    result, _ = resolve_in_world(namespace_status="disabled")
    _assert_unresolved(result, "namespace_disabled")


def test_step7_no_link(migrated: None) -> None:
    result, _ = resolve_in_world(link=None)
    _assert_unresolved(result, "no_link")


def test_step7_revoked_link_is_ignored(migrated: None) -> None:
    result, _ = resolve_in_world(link="revoked")
    _assert_unresolved(result, "no_link")


def test_step7_link_for_another_user_is_no_link(migrated: None) -> None:
    result, _ = resolve_in_world(link_native="UBOB00001")
    _assert_unresolved(result, "no_link")


def test_step7_linked_to_bot(migrated: None) -> None:
    result, _ = resolve_in_world(link="bot")
    _assert_unresolved(result, "linked_to_bot")


@pytest.mark.parametrize("status", ["disabled", "revoked"])
def test_step7_principal_inactive(migrated: None, status: str) -> None:
    result, _ = resolve_in_world(principal_status=status)
    _assert_unresolved(result, "principal_inactive")


# --- Slack: the app_mention path (context-established sender team) --------


@pytest.mark.parametrize(
    "overrides",
    [
        {"is_ext_shared_channel": True},
        {"is_ext_shared_channel": None},
    ],
    ids=["externally-shared", "shared-unknown"],
)
def test_mention_outside_a_known_unshared_channel_is_unidentified(
    migrated: None, overrides: dict[str, Any]
) -> None:
    result, _ = resolve_in_world(mention_evidence(**overrides))
    _assert_unresolved(result, "subject_unidentified")


@pytest.mark.parametrize(
    "stored",
    [
        None,
        {**NON_GRID_EVIDENCE, "is_enterprise_install": None},
        {**NON_GRID_EVIDENCE, "is_enterprise_install": True},
        {**NON_GRID_EVIDENCE, "enterprise_id": GRID},
        {**NON_GRID_EVIDENCE, "team_id": OTHER},
        {**CAPTURED_EVIDENCE, "is_enterprise_install": None},
        {**CAPTURED_EVIDENCE, "is_enterprise_install": True},
        {**CAPTURED_EVIDENCE, "enterprise_id": GRID, "enterprise_id_present": True},
        {**CAPTURED_EVIDENCE, "team_id": OTHER},
        {k: v for k, v in NON_GRID_EVIDENCE.items() if k != "is_enterprise_install"},
        {**NON_GRID_EVIDENCE, "is_enterprise_install": "false"},
        "not-an-object",
    ],
    ids=[
        "no-evidence",
        "enterprise-install-unknown",
        "enterprise-install",
        "grid-enterprise",
        "stale-team",
        "captured-enterprise-install-unknown",
        "captured-enterprise-install",
        "captured-grid-enterprise",
        "captured-stale-team",
        "enterprise-install-key-missing",
        "enterprise-install-not-a-bool",
        "malformed",
    ],
)
def test_mention_without_positive_non_grid_evidence_is_unidentified(
    migrated: None, stored: Any
) -> None:
    result, _ = resolve_in_world(mention_evidence(), evidence=stored)
    _assert_unresolved(result, "subject_unidentified")


@pytest.mark.parametrize(
    "stored",
    [
        CAPTURED_EVIDENCE,
        {k: v for k, v in NON_GRID_EVIDENCE.items() if k != "enterprise_id"},
    ],
    ids=["captured-enterprise-id-absent", "enterprise-id-key-missing"],
)
def test_mention_with_absent_enterprise_id_evidence_resolves(migrated: None, stored: Any) -> None:
    """Rev 4.3: the observed auth.test of an ordinary workspace omits
    ``enterprise_id``. Absent (or null) plus ``is_enterprise_install: false``
    for this installation's team is positive non-Grid evidence."""

    result, world = resolve_in_world(mention_evidence(), evidence=stored)
    assert (result.status, result.reason) == ("resolved", "linked"), result
    assert result.principal_id == world.principal_id


def test_mention_under_a_non_empty_authority_is_unidentified(migrated: None) -> None:
    result, _ = resolve_in_world(mention_evidence(), installation_authority="slack.example.gov")
    _assert_unresolved(result, "subject_unidentified")


def test_mention_with_any_enterprise_id_is_unidentified(migrated: None) -> None:
    result, _ = resolve_in_world(mention_evidence(enterprise_ids=[GRID]))
    _assert_unresolved(result, "subject_unidentified")


def test_mention_team_disagreeing_with_installation_is_a_conflict(migrated: None) -> None:
    result, _ = resolve_in_world(mention_evidence(event_team=OTHER))
    _assert_unresolved(result, "namespace_conflict")


def test_mention_without_event_team_is_unidentified(migrated: None) -> None:
    """No fallback to the installation's team when the sender field is absent."""

    result, _ = resolve_in_world(mention_evidence(event_team=None))
    _assert_unresolved(result, "subject_unidentified")


@pytest.mark.parametrize("delivery", ["message", "reaction_added", "app_home_opened"])
def test_event_team_is_not_used_outside_app_mention(migrated: None, delivery: str) -> None:
    """``event.team`` was observed only on app_mention; a direct message without
    ``user_team`` stays unresolved until its own capture exists."""

    result, _ = resolve_in_world(mention_evidence(delivery=delivery))
    _assert_unresolved(result, "subject_unidentified")


# --- Slack: the documented user_team path ----------------------------------


def test_message_with_user_team_resolves(migrated: None) -> None:
    result, world = resolve_in_world(
        mention_evidence(delivery="message", event_user_team=HOME, event_team=None)
    )
    assert (result.status, result.principal_id) == ("resolved", world.principal_id)


def test_user_team_is_the_sender_namespace(migrated: None) -> None:
    """A documented sender team names the sender's own namespace, not the
    installation's: this tenant holds no namespace for it."""

    result, _ = resolve_in_world(
        mention_evidence(delivery="message", event_user_team=OTHER, event_team=None)
    )
    _assert_unresolved(result, "namespace_unknown")


def test_user_team_needs_no_mention_context(migrated: None) -> None:
    result, world = resolve_in_world(
        mention_evidence(event_user_team=HOME, is_ext_shared_channel=None)
    )
    assert (result.status, result.principal_id) == ("resolved", world.principal_id)


def test_user_team_disagreeing_with_event_team_is_a_conflict(migrated: None) -> None:
    result, _ = resolve_in_world(mention_evidence(event_user_team=OTHER, event_team=HOME))
    _assert_unresolved(result, "namespace_conflict")


def test_user_team_under_grid_is_unidentified(migrated: None) -> None:
    payload_grid, _ = resolve_in_world(
        mention_evidence(delivery="message", event_user_team=HOME, enterprise_ids=[GRID])
    )
    _assert_unresolved(payload_grid, "subject_unidentified")
    stored_grid, _ = resolve_in_world(
        mention_evidence(delivery="message", event_user_team=HOME),
        evidence={**NON_GRID_EVIDENCE, "enterprise_id": GRID},
    )
    _assert_unresolved(stored_grid, "subject_unidentified")


# --- Slack: interactions ----------------------------------------------------


def test_interaction_needs_no_stored_evidence(migrated: None) -> None:
    """``user.team_id`` is documented; the interaction path is not gated on the
    mention path's context evidence."""

    result, world = resolve_in_world(interaction_evidence(), evidence=None)
    assert (result.status, result.principal_id) == ("resolved", world.principal_id)


def test_interaction_without_user_team_is_unidentified(migrated: None) -> None:
    result, _ = resolve_in_world(interaction_evidence(interaction_user_team_id=None))
    _assert_unresolved(result, "subject_unidentified")


def test_interaction_ignores_event_team(migrated: None) -> None:
    result, _ = resolve_in_world(
        interaction_evidence(interaction_user_team_id=None, event_team=HOME)
    )
    _assert_unresolved(result, "subject_unidentified")


def test_interaction_with_other_team_is_the_sender_namespace(migrated: None) -> None:
    result, _ = resolve_in_world(interaction_evidence(interaction_user_team_id=OTHER))
    _assert_unresolved(result, "namespace_unknown")


@pytest.mark.parametrize("kind", ["block_actions", "view_submission"])
def test_interaction_under_grid_is_unidentified(migrated: None, kind: str) -> None:
    payload_grid, _ = resolve_in_world(interaction_evidence(kind, enterprise_ids=[GRID]))
    _assert_unresolved(payload_grid, "subject_unidentified")
    stored_grid, _ = resolve_in_world(
        interaction_evidence(kind), evidence={**NON_GRID_EVIDENCE, "enterprise_id": GRID}
    )
    _assert_unresolved(stored_grid, "subject_unidentified")


# --- Slack: the subject ------------------------------------------------------


@pytest.mark.parametrize("user_id", [None, "", "   "], ids=["missing", "empty", "blank"])
@pytest.mark.parametrize("make", [mention_evidence, interaction_evidence], ids=["event", "ui"])
def test_blank_user_is_unidentified(
    migrated: None, user_id: str | None, make: Callable[..., dict[str, Any]]
) -> None:
    result, _ = resolve_in_world(make(user_id=user_id))
    _assert_unresolved(result, "subject_unidentified")


def test_slack_without_evidence_is_unidentified(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> Any:
        world = await build_world(conn)
        return await _resolve(conn, world, None)

    _assert_unresolved(_rolled_back(body), "subject_unidentified")


# --- never guess ---------------------------------------------------------


def test_no_guess_from_principal_attributes(migrated: None) -> None:
    """A principal whose email, display name or IdP subject equals the Slack id
    is not a link."""

    async def body(conn: AsyncConnection) -> Any:
        world = await build_world(conn, link=None)
        await _principal(conn, world.tenant_id, email=ALICE, display_name=ALICE, idp_subject=ALICE)
        await _principal(conn, world.tenant_id, email=f"{ALICE.lower()}@example.com")
        return await _resolve(conn, world, mention_evidence())

    _assert_unresolved(_rolled_back(body), "no_link")


@pytest.mark.parametrize("variant", [ALICE.lower(), "Ualice001", f" {ALICE}", f"{ALICE} "])
def test_no_guess_from_user_id_variants(migrated: None, variant: str) -> None:
    result, _ = resolve_in_world(interaction_evidence(user_id=variant))
    assert result.status == "unresolved"
    assert result.principal_id is None


@pytest.mark.parametrize("variant", [HOME.lower(), "Thome0001"])
def test_no_guess_from_team_case_variants(migrated: None, variant: str) -> None:
    for evidence in (
        interaction_evidence(interaction_user_team_id=variant),
        mention_evidence(delivery="message", event_user_team=variant, event_team=None),
        mention_evidence(event_team=variant),
    ):
        result, _ = resolve_in_world(evidence)
        assert result.status == "unresolved", evidence
        assert result.principal_id is None


def test_link_in_another_tenant_is_not_consulted(migrated: None) -> None:
    async def body(conn: AsyncConnection) -> Any:
        mine = await build_world(conn, link=None)
        await build_world(conn)  # another tenant links the same team and user
        return await _resolve(conn, mine, mention_evidence())

    _assert_unresolved(_rolled_back(body), "no_link")


# --- the route ---------------------------------------------------------------


ROUTE = "/identity/resolve"


def _sql(statement: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async def run() -> list[dict[str, Any]]:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                return await _exec(conn, statement, params)
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
    _sql("DELETE FROM curie.agents WHERE name LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.principals WHERE idp_subject LIKE :mark", {"mark": f"{MARK}%"})
    _sql("DELETE FROM curie.tenants WHERE deployment_id LIKE :mark", {"mark": f"{MARK}%"})


@pytest.fixture
def api(migrated: None, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    _cleanup()
    monkeypatch.setenv("SLACK_BOT_TOKEN", "")
    get_settings.cache_clear()
    try:
        with TestClient(create_app()) as test_client:
            yield test_client
    finally:
        get_settings.cache_clear()
        _cleanup()


def _committed_world(**kwargs: Any) -> World:
    async def run() -> World:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.begin() as conn:
                return await build_world(conn, **kwargs)
        finally:
            await engine.dispose()

    return asyncio.run(run())


def _body(world: World | None, **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "provider": "slack",
        "channel_identity": world.identity_name if world else "no-such-identity",
        "slack": mention_evidence(),
    }
    if world is not None:
        body["tenant_id"] = str(world.tenant_id)
    body.update(overrides)
    return body


def test_route_requires_platform_key(api: TestClient) -> None:
    assert api.post(ROUTE, json=_body(None)).status_code == 401
    assert api.post(ROUTE, json=_body(None), headers={"X-API-Key": "wrong"}).status_code == 401


def test_route_unresolved_is_200(api: TestClient, auth_headers: dict[str, str]) -> None:
    response = api.post(ROUTE, json=_body(None), headers=auth_headers)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "unresolved",
        "reason": "identity_not_found",
        "principal_id": None,
        "channel_identity_id": None,
        "namespace_id": None,
    }


def test_route_resolves_a_fixture_mention(api: TestClient, auth_headers: dict[str, str]) -> None:
    world = _committed_world()
    response = api.post(ROUTE, json=_body(world), headers=auth_headers)
    assert response.status_code == 200, response.text
    assert response.json() == {
        "status": "resolved",
        "reason": "linked",
        "principal_id": str(world.principal_id),
        "channel_identity_id": str(world.identity_id),
        "namespace_id": str(world.namespace_id),
    }


def test_route_resolves_a_fixture_interaction(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    world = _committed_world()
    response = api.post(
        ROUTE,
        json=_body(world, slack=interaction_evidence("view_submission")),
        headers=auth_headers,
    )
    assert response.status_code == 200, response.text
    assert response.json()["principal_id"] == str(world.principal_id)


def test_route_non_slack_provider_is_unsupported(
    api: TestClient, auth_headers: dict[str, str]
) -> None:
    world = _committed_world(provider="github")
    response = api.post(
        ROUTE, json=_body(world, provider="github", slack=None), headers=auth_headers
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "unresolved"
    assert response.json()["reason"] == "provider_unsupported"
    assert response.json()["principal_id"] is None


CANARY = "canary-resolve-9f3a"


@pytest.mark.parametrize(
    "body",
    [
        {**_body(None), "extra": CANARY},
        {**_body(None), "slack": {**mention_evidence(), "source_team": CANARY}},
        {**_body(None), "channel_identity": ""},
        {**_body(None), "provider": "myspace", "note": CANARY},
        {k: v for k, v in _body(None).items() if k != "channel_identity"} | {"x": CANARY},
        {**_body(None), "slack": {"user_id": CANARY}},
        {**_body(None), "slack": mention_evidence(user_id=CANARY + "X" * 256)},
        {**_body(None), "slack": mention_evidence(event_team=CANARY + "X" * 256)},
        {
            **_body(None),
            "slack": mention_evidence(enterprise_ids=[f"{CANARY}{i:04d}" for i in range(33)]),
        },
        {**_body(None), "slack": mention_evidence(enterprise_ids=[CANARY + "X" * 256])},
    ],
    ids=[
        "extra-top-level",
        "extra-evidence-field",
        "empty-identity",
        "bad-provider",
        "missing-identity",
        "evidence-without-delivery",
        "evidence-user-over-256",
        "evidence-team-over-256",
        "enterprise-ids-over-32",
        "enterprise-id-over-256",
    ],
)
def test_route_malformed_body_is_redacted_422(
    api: TestClient, auth_headers: dict[str, str], body: dict[str, Any]
) -> None:
    response = api.post(ROUTE, json=body, headers=auth_headers)
    assert response.status_code == 422, response.text
    assert CANARY not in response.text
    detail = response.json()["detail"]
    assert isinstance(detail, list) and detail
    for entry in detail:
        assert {"loc", "msg", "type"} <= set(entry)
        assert "input" not in entry
        assert "ctx" not in entry
