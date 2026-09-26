"""Every other API reader of a channel route resolves by its IDENTITY
(ADR-0168 decision 3), not the raw `adapter` column.

Migration 0061 stores the default Slack identity by name, `'default'`, but a
value arriving over the WIRE -- from an older caller, or a handle queued
before the upgrade -- can still name none. Every reader below compares a
route's identity through `route_identity`: an omitted adapter, a NULL and
`'default'` all mean the same route. Each test targets exactly one reader,
seeding the STORED form and, where the reader compares against a wire value,
sending None from that side -- the one mismatch a raw `!=` would manufacture,
since a NULL never equals the string `'default'`.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from alembic import command
from curie_api import crud
from curie_api.config import get_settings
from curie_api.migration_fence import DECLARATIONS_ENV, HONORED_ACTION, load_declarations
from curie_api.routers import approval_recovery
from curie_api.schemas import PublicationCreate
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from apps.api.tests.test_channels import (
    EMAIL_ADAPTER,
    EMAIL_ENDPOINT,
    _bind,
    _binding_row,
    _channel,
    _email_channel,
    _mint,
    _post_turn,
    _turn,
    _turns_on,
)
from apps.api.tests.test_channels import channels_client as channels_client
from apps.api.tests.test_channels import valkey as valkey
from apps.api.tests.test_migration_fence import (
    BELOW_0022,
    REVISION_0022,
    _at,
    _audit_rows,
    _declaration,
    _reply_identity,
    _seed_approval,
    _write_declarations,
)
from apps.api.tests.test_publications import (
    FIRST_REVISION_SHA,
    WORKER_HEADERS,
    _create_deployment,
    _publication_payload,
    _reserve_review,
    _verified_lineage,
    _workspace_identity,
)
from apps.api.tests.test_publications import publication_stack as publication_stack
from apps.api.tests.test_publications import review_lineage_app as review_lineage_app

# --------------------------------------------------------------------------
# routers/channels.py: `_resolve_binding` (ingress) and `mint_channel_token`,
# both now delegating to `crud.binding_for_route`.
# --------------------------------------------------------------------------


def test_channel_ingress_resolves_slack_none_address_to_the_default_row(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """`crud.binding_for_route(session, "slack", None, address)` -- what
    `routers/channels.py:_resolve_binding` now delegates to for the ingress
    (`POST /channels/turns`, whose body never carries an adapter, plan D4.1) --
    resolves the pair's one stored row, exactly as the raw
    `select(...).where(kind==, address==)` `_resolve_binding` ran before this
    task.
    """

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="slack-ingress-identity",
        channel=_channel("slack", "C0EXAMPLE1"),
    )
    stored = _binding_row(agent_id)
    assert stored["adapter"] == "default"  # the default identity's stored form

    async def resolve() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            async with sessionmaker() as session:
                return await crud.binding_for_route(session, "slack", None, "C0EXAMPLE1")
        finally:
            await engine.dispose()

    binding = asyncio.run(resolve())
    assert binding is not None
    assert binding.id == stored["id"]
    assert binding.adapter == "default"


def test_channel_ingress_resolves_a_non_slack_binding_end_to_end(
    channels_client: TestClient,
    auth_headers: dict[str, str],
    valkey: Any,
    runs_stream: str,
    clean_db: None,
) -> None:
    """Drives the REAL ingress (`POST /channels/turns`) for a non-Slack
    binding: every other ingress test here names a Slack address, whose
    custom-transport fallback in `crud.matching_bindings` can mask a
    positional argument mix-up at
    `_resolve_binding`'s call site (`_resolve_binding(session, body.kind,
    None, body.address)`) -- swap `adapter` and `address` there and a Slack
    test can still pass by falling back to the endpoint-carrying row, while a
    non-Slack pair, which has no such fallback, resolves to nothing and 404s.
    """

    address = f"ingress-identity-{uuid.uuid4().hex[:8]}@example.test"
    _bind(
        channels_client,
        auth_headers,
        name="email-ingress-identity",
        channel=_email_channel(address),
    )
    token = _mint(channels_client, auth_headers, kind="email", address=address)

    resp = _post_turn(channels_client, token, _turn("email", address))
    assert resp.status_code == 200, resp.text

    (turn,) = _turns_on(valkey, runs_stream)
    assert turn.reply_handle.kind == "email"
    assert turn.reply_handle.endpoint == EMAIL_ENDPOINT
    assert turn.reply_handle.adapter == EMAIL_ADAPTER


def test_token_mint_resolves_the_default_row_whether_adapter_is_omitted_or_wire_default(
    channels_client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """`ChannelTokenRequest.adapter` (ADR-0168 decision 3): omitting it keeps
    minting for the row every caller before the ADR minted for, and naming it
    explicitly as `'default'` -- the identity the row stores -- must mint for
    the SAME row, not 404 it. An API
    without the field 422s the second request (`ChannelBinding.model_config`
    forbids extra fields), which is why the CLI leaves a default out.
    """

    agent_id = _bind(
        channels_client,
        auth_headers,
        name="slack-mint-identity",
        channel=_channel("slack", "C0EXAMPLE1"),
    )
    stored = _binding_row(agent_id)
    assert stored["adapter"] == "default"

    omitted = channels_client.post(
        "/channels/token",
        json={"kind": "slack", "address": "C0EXAMPLE1", "ttl_s": 60},
        headers=auth_headers,
    )
    assert omitted.status_code == 200, omitted.text

    wire_default = channels_client.post(
        "/channels/token",
        json={"kind": "slack", "address": "C0EXAMPLE1", "adapter": "default", "ttl_s": 60},
        headers=auth_headers,
    )
    assert wire_default.status_code == 200, wire_default.text

    wire_other = channels_client.post(
        "/channels/token",
        json={"kind": "slack", "address": "C0EXAMPLE1", "adapter": "second", "ttl_s": 60},
        headers=auth_headers,
    )
    assert wire_other.status_code == 404, wire_other.text


# --------------------------------------------------------------------------
# crud.py: `create_publication`'s lineage-adoption binding lookup, compared
# through `route_identity` instead of the raw `!=`.
# --------------------------------------------------------------------------


def test_create_publication_keeps_the_binding_when_reply_adapter_names_none(
    publication_stack: tuple[TestClient, str], auth_headers: dict[str, str], clean_db: None
) -> None:
    """`binding.adapter` (stored `'default'`) and `data.reply_adapter` (None)
    name the SAME Slack identity, so `create_publication` must adopt the
    binding into the new lineage rather than treat the route as unbound.

    `PublicationCreate`'s own validator already resolves an omitted adapter to
    `'default'` (schemas.py `_valid_reply_route`), so this sets `reply_adapter`
    on the validated model directly (`validate_assignment` is off) to hand
    `crud.create_publication` what a caller that has not gone through that
    resolution would: the raw value the reader has to resolve for itself.
    """

    client, _ = publication_stack
    deployment = _create_deployment(client, auth_headers)
    payload = _publication_payload(deployment["id"])

    selected = client.post(
        f"/v1/internal/workspaces/{payload['deployment_id']}/selection",
        json={
            "conversation_id": _workspace_identity(payload),
            "author": payload["author"],
            "repo_full_name": payload["repo_full_name"],
        },
        headers=WORKER_HEADERS,
    )
    assert selected.status_code == 200, selected.text

    data = PublicationCreate.model_validate(payload)
    assert data.reply_adapter == "default"  # the schema's own resolution
    data.reply_adapter = None  # simulate the wire value `crud` must resolve

    async def create() -> uuid.UUID:
        engine = create_async_engine(get_settings().database_url)
        try:
            sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            async with sessionmaker() as session:
                publication, created = await crud.create_publication(
                    session, data, patch=data.decoded_patch()
                )
                assert created is True
                await session.commit()
                return publication.lineage_id
        finally:
            await engine.dispose()

    lineage_id = asyncio.run(create())

    async def read_binding_id() -> Any:
        engine = create_async_engine(get_settings().database_url)
        try:
            async with engine.connect() as conn:
                result = await conn.execute(
                    text(
                        "SELECT binding_id FROM curie.thread_publication_lineages "
                        "WHERE id = :id"
                    ),
                    {"id": lineage_id},
                )
                return result.scalar_one()
        finally:
            await engine.dispose()

    binding_id = asyncio.run(read_binding_id())
    assert binding_id is not None, "the omitted reply_adapter dropped the binding"


def test_review_revision_accepts_an_omitted_reply_adapter(
    review_lineage_app: tuple[TestClient, dict[str, Any], str],
    auth_headers: dict[str, str],
) -> None:
    """`create_publication`'s review-revision comparison (`crud.py`, the
    `_require_review_binding` branch) must not refuse a revision that merely
    SPELLS the reserved binding's identity differently: the original
    publication's Slack binding is stored as `'default'` and this revision
    names none -- the same identity, through `route_identity`.

    Goes around the HTTP body the same way the create_publication test does:
    `PublicationCreate`'s own validator resolves an omitted adapter to
    `'default'`, so reaching the raw value `crud.create_publication` has to
    resolve means setting it directly on an already-validated model.
    """

    client, truth, _ = review_lineage_app
    deployment, _, lineage = _verified_lineage(client, truth, auth_headers)
    reservation = _reserve_review(client, lineage, "review:no-adapter")
    assert reservation.status_code == 201, reservation.text

    payload = _publication_payload(
        deployment["id"], conversation_id=lineage["conversation_id"], base_sha=FIRST_REVISION_SHA
    )
    payload.update(reply_conversation_id="review-original", review_origin_key="review:no-adapter")
    data = PublicationCreate.model_validate(payload)
    assert data.reply_adapter == "default"  # the schema's own resolution
    data.reply_adapter = None  # simulate the wire value `crud` must resolve

    async def create() -> uuid.UUID:
        engine = create_async_engine(get_settings().database_url)
        try:
            sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            async with sessionmaker() as session:
                publication, created = await crud.create_publication(
                    session, data, patch=data.decoded_patch()
                )
                await session.commit()
                return publication.id
        finally:
            await engine.dispose()

    revision_id = asyncio.run(create())
    assert str(revision_id) == reservation.json()["revision_id"]


# --------------------------------------------------------------------------
# routers/approval_recovery.py: `_binding_facts`'s adapter-backed-kinds set.
# --------------------------------------------------------------------------


def test_approval_recovery_does_not_list_slack_among_adapter_backed_kinds(
    client: TestClient, auth_headers: dict[str, str], clean_db: None
) -> None:
    """A Slack row's `adapter` is stored as NULL (the default identity,
    ADR-0168 decision 3; `route_identity`), so a bare Slack row must NOT read
    as adapter-authenticated egress. The fix in `_binding_facts` is defensive
    rather than a live-bug fix: `agent_channels_route_pair_ck` (0024) makes
    "adapter set, no endpoint" unstorable for ANY kind, so the only Slack row
    this installation can ever store with a truthy `adapter` is the pre-ADR
    custom-transport form, WITH an endpoint -- and that form's adapter IS a
    real egress credential, so it must stay in the set. Both reachable shapes
    are tested.
    """

    default_identity = client.post(
        "/agents",
        json={
            "name": f"recovery-identity-default-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": "C0EXAMPLE1"},
        },
        headers=auth_headers,
    )
    assert default_identity.status_code == 201, default_identity.text
    default_agent_id = default_identity.json()["id"]

    custom_transport = client.post(
        "/agents",
        json={
            "name": f"recovery-identity-custom-{uuid.uuid4().hex[:8]}",
            "channel": {
                "kind": "slack",
                "address": "C0EXAMPLE2",
                "endpoint": "http://127.0.0.1:1",
                "adapter": "proof-offline",
            },
        },
        headers=auth_headers,
    )
    assert custom_transport.status_code == 201, custom_transport.text
    custom_agent_id = custom_transport.json()["id"]

    async def read_adapter_kinds(agent_id: str) -> set[str]:
        engine = create_async_engine(get_settings().database_url)
        try:
            sessionmaker = async_sessionmaker(engine, expire_on_commit=False)
            async with sessionmaker() as session:
                _has_binding, adapter_kinds = await approval_recovery._binding_facts(
                    session, uuid.UUID(agent_id)
                )
                return adapter_kinds
        finally:
            await engine.dispose()

    assert "slack" not in asyncio.run(read_adapter_kinds(default_agent_id))
    assert "slack" in asyncio.run(read_adapter_kinds(custom_agent_id))


# --------------------------------------------------------------------------
# migration_fence.py: `load_declarations` accepts NULL or 'default' for a
# Slack declaration's `reply_adapter` and refuses any other name.
# --------------------------------------------------------------------------


def test_fence_accepts_wire_default_on_a_slack_declaration_and_refuses_other_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    approval_id = uuid.uuid4()
    accepted_path = tmp_path / "accepted.json"
    accepted_path.write_text(
        json.dumps(
            {
                "declarations": [
                    {
                        "approval_id": str(approval_id),
                        "reply_kind": "slack",
                        "reply_adapter": "default",
                        "actor": "U0OPERATOR",
                        "reason": "raised on the default Slack identity",
                    }
                ]
            }
        )
    )
    monkeypatch.setenv(DECLARATIONS_ENV, str(accepted_path))
    accepted = load_declarations()
    # Normalized to the stored form, with the operator's own spelling kept.
    assert accepted[str(approval_id)].reply_adapter == "default"
    assert accepted[str(approval_id)].reply_adapter_as_written == "default"

    null_path = tmp_path / "null.json"
    null_path.write_text(
        json.dumps(
            {
                "declarations": [
                    {
                        "approval_id": str(approval_id),
                        "reply_kind": "slack",
                        "reply_adapter": None,
                        "actor": "U0OPERATOR",
                        "reason": "raised on the default Slack identity",
                    }
                ]
            }
        )
    )
    monkeypatch.setenv(DECLARATIONS_ENV, str(null_path))
    from_null = load_declarations()
    assert from_null[str(approval_id)].reply_adapter == "default"
    assert from_null[str(approval_id)].reply_adapter_as_written is None

    refused_path = tmp_path / "refused.json"
    refused_path.write_text(
        json.dumps(
            {
                "declarations": [
                    {
                        "approval_id": str(approval_id),
                        "reply_kind": "slack",
                        "reply_adapter": "second",
                        "actor": "U0OPERATOR",
                        "reason": "raised on a named non-default identity",
                    }
                ]
            }
        )
    )
    monkeypatch.setenv(DECLARATIONS_ENV, str(refused_path))
    with pytest.raises(RuntimeError) as caught:
        load_declarations()
    message = str(caught.value)
    assert "'second'" in message, message


def test_honor_declarations_stores_a_slack_declaration_as_the_default_identity(
    isolated_migration_db: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Slack declaration names the default identity, and honoring it writes
    that name into `approvals.reply_adapter` at the revision that honors it --
    the stored form 0061 backfills everywhere else. The audit keeps the
    normalized value and the operator's own spelling."""

    cfg = _at(BELOW_0022)
    orphan = _seed_approval(reply_channel="nobody@example.test", summary="no binding")
    _write_declarations(
        tmp_path,
        monkeypatch,
        [
            _declaration(
                orphan,
                reply_kind="slack",
                reply_adapter="default",
                reason="raised on the default Slack identity, named explicitly",
            )
        ],
    )

    command.upgrade(cfg, REVISION_0022)

    assert _reply_identity(orphan) == ("slack", "default")
    (honored,) = [r for r in _audit_rows(orphan) if r.action == HONORED_ACTION]
    assert honored.evidence["declared_reply_adapter"] == "default"
    assert honored.evidence["declared_reply_adapter_as_written"] == "default"

    command.upgrade(cfg, "head")

    assert _reply_identity(orphan) == ("slack", "default")


def test_honor_declarations_admits_a_non_slack_adapter_named_default(
    isolated_migration_db: None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`default` is an ordinary adapter slug for any kind but Slack, and 0022
    honored one before the Slack identity had a name."""

    cfg = _at(BELOW_0022)
    orphan = _seed_approval(reply_channel="nobody@example.test", summary="no binding")
    _write_declarations(
        tmp_path,
        monkeypatch,
        [_declaration(orphan, reply_kind="email", reply_adapter="default")],
    )

    command.upgrade(cfg, REVISION_0022)

    assert _reply_identity(orphan) == ("email", "default")
