"""Shared ADR 0272 approver-DM fixtures; this module holds no tests.

@spec APPROVER-DM-1 APPROVER-DM-2 APPROVER-DM-3.

Builds on ``approval_answer_support`` (the roster TLS directory, a real adopted
HTTP tenant with one Slack binding, a served bundle gating
``mcp__access__execute`` on the owning route, the requester flag on and the
restricted approver role) and ``approver_ticket_support`` (signed approver
tickets, operator route writes). Three additions:

1. Besides the binding's own receiver (``scope["main"]``), the API registers two
   more Slack receivers that list the same binding: ``scope["other"]``, a second
   bot identity on the same installation and namespace, and ``scope["foreign"]``,
   a bot identity on another installation and workspace namespace. A third
   identity row, ``scope["unregistered"]``, exists in the tenant with no
   receiver registration at all.
2. Route writes put ``{"mode": "approver_dm", "identity": <name>}`` on the
   owning route through the real ``PATCH /agents/{id}``.
3. Helpers for the pinned internal operations: DM creation (``card_channel``
   null, ``delivery`` in the response), card registration
   (``POST /v1/internal/identity/approvals/{id}/card``) and Slack answers whose
   receiver, installation, interaction channel and actor are each chosen by the
   test.

Synthetic, reserved example values only; Slack ids use the ``*0EXAMPLE*`` shape.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from _migration_support import sql_dicts, sql_rows
from approval_answer_support import (
    HUMANS,
    NATIVE_A,
    _turn,
    default_roster,
    no_wake,
    still_pending,
)
from approver_ticket_support import (
    APPROVER_A,
    ROUTE,
    carried,
    dynamic_request,
    prepare,
    principal_claims,
    registration,
    route_map,
    selector_key,
    ticket_payload,
    worker_headers,
    write_routes,
)
from curie_api import adapter_principal, approval_principal
from curie_api.config import get_settings
from curie_api.identity.http_credential_registry import build_provider_credential_registry
from curie_api.identity.http_transport import HttpIdentityClient, ProviderBearer
from curie_api.identity.slack import SlackEvidence
from curie_api.main import create_app
from curie_api.models import ChannelIdentity, IdentityNamespace, ProviderInstallation
from curie_api.schemas.http_identity import NamespaceV1
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from fastapi.testclient import TestClient
from role_policy_support import run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_email_receivers as email_receivers
from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_approval_answers import PROOF_HEADER, slack_evidence
from apps.api.tests.test_http_approval_creation import (
    RECORD_FIELDS,
    canonical,
    digest,
    opened,
    rows,
)

DM_MODE = "approver_dm"
CREATION_PATH = "/v1/internal/identity/approvals"
DM_CHANNEL = "D0EXAMPLEDM1"
OTHER_DM_CHANNEL = "D0EXAMPLEDM2"
FOREIGN_ACCOUNT = "T0EXAMPLEFRGN"
INVALID_PRINCIPAL = "missing or invalid approval principal"
ARGS: dict[str, Any] = {"account": "acct-example-1", "change": {"role": "reader"}, "count": 2}
CREATION_KEYS = {"schema_version", "request_id", "id", "status", "duplicate", "requested_by"}
CARD_KEYS = {"schema_version", "request_id", "approval_id", "status", "card_channel", "duplicate"}
CARD_EVIDENCE_KEYS = {
    "kind", "identity", "provider_installation_id", "namespace_id", "recipient", "channel",
}
SELECTION_FIELDS = (
    "selection_mode", "selection_requester", "selection_connector", "selection_tool",
    "selection_key_id", "selection_nonce", "selection_arguments_sha256", "selected_approvers",
)
SEALED_ORIGIN_FIELDS = (
    "approval_id", "creation_request_sha256", "source_context_sha256", "source_payload_sha256",
    "original_payload_sha256", "source_event_id", "original_event_id", "conversation_id",
    "scope", "canonical", "principal",
)


# --- extra bot identities and the receiving runtime ------------------------------------


def add_identity(
    scope: dict[str, Any], name: str, *, account: str | None = None,
) -> dict[str, Any]:
    """One active Slack ChannelIdentity row in the tenant; on a new installation for ``account``.

    Returns the receiver-shaped description the runtime and proofs use.
    @spec APPROVER-DM-1.
    """
    found: dict[str, Any] = {}

    async def persist(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec APPROVER-DM-1."""
        tenant = uuid.UUID(scope["tenant"])
        async with factory() as session, session.begin():
            installation_id = uuid.UUID(scope["installation"])
            namespace = scope["namespace"]
            if account is not None:
                installation = ProviderInstallation(
                    tenant_id=tenant, provider="slack", authority="",
                    external_account_id=account, status="connected",
                )
                row = IdentityNamespace(
                    tenant_id=tenant, provider="slack", authority="",
                    kind="slack_workspace", key=account, status="active",
                )
                session.add_all([installation, row])
                await session.flush()
                installation_id = installation.id
                namespace = NamespaceV1(
                    id=str(row.id), authority="", kind="slack_workspace", key=account,
                )
            identity = ChannelIdentity(
                tenant_id=tenant, provider="slack", name=name,
                provider_installation_id=installation_id, status="active", attributes={},
            )
            session.add(identity)
            await session.flush()
            found.update(
                identity=str(identity.id), adapter=name, installation=str(installation_id),
                namespace=namespace, account=account or scope["account"],
            )

    run(persist)
    return found


def add_receivers(scope: dict[str, Any]) -> None:
    """The binding's receiver plus two more registrations and one unregistered identity.

    @spec APPROVER-DM-1 APPROVER-DM-3.
    """
    scope["main"] = {
        "receiver_id": scope["receiver_id"], "receiver_key": scope["receiver_key"],
        "identity": scope["identity"], "adapter": scope["adapter"],
        "installation": scope["installation"], "namespace": scope["namespace"],
        "account": scope["account"],
    }
    other = add_identity(scope, "synthetic-dm-other-" + uuid.uuid4().hex[:12])
    other.update(receiver_id=str(uuid.uuid4()), receiver_key=b"o" * 32)
    foreign = add_identity(
        scope, "synthetic-dm-foreign-" + uuid.uuid4().hex[:12], account=FOREIGN_ACCOUNT,
    )
    foreign.update(receiver_id=str(uuid.uuid4()), receiver_key=b"f" * 32)
    scope["other"], scope["foreign"] = other, foreign
    scope["unregistered"] = add_identity(
        scope, "synthetic-dm-unregistered-" + uuid.uuid4().hex[:12],
    )


def _registration_v1(module: Any, scope: dict[str, Any], receiver: dict[str, Any]) -> Any:
    """@spec APPROVER-DM-3: every registration lists the binding and every roster human."""
    return module.ReceiverRegistrationV1(
        schema_version=1, receiver_id=receiver["receiver_id"], generation=1,
        tenant_id=scope["tenant"], channel_identity_id=receiver["identity"],
        channel_identity_name=receiver["adapter"],
        provider_installation_id=receiver["installation"], namespace=receiver["namespace"],
        installation_account=receiver["account"], bot_user_id="U0EXAMPLEBOT",
        binding_ids=(scope["binding"],), human_native_ids=HUMANS,
    )


def dm_runtime(scope: dict[str, Any]) -> Any:
    """The operator registrations: the binding's receiver, ``other`` and ``foreign``.

    @spec APPROVER-DM-1 APPROVER-DM-3.
    """
    module = membership._runtime_api()
    providers = build_provider_credential_registry(
        ((scope["configuration"], ProviderBearer(membership.BEARER)),)
    )
    receivers = tuple(
        (_registration_v1(module, scope, scope[name]), scope[name]["receiver_key"])
        for name in ("main", "other", "foreign")
    )
    return module.build_http_invocation_runtime(
        providers, receivers, (("synthetic-context", 1, scope["context_key"]),),
        signing_key=("synthetic-context", 1),
    )


@contextmanager
def dm_queued(
    provider: SimpleNamespace, valkey: redis.Redis, stream: str,
) -> Iterator[SimpleNamespace]:
    """``approval_answer_support.queued`` with the three-receiver runtime.

    @spec APPROVER-DM-1.
    """
    scope = membership._scope(provider.url)
    add_receivers(scope)
    identity_client = HttpIdentityClient(tls_context=provider.tls)
    try:
        with TestClient(create_app(
            http_identity_runtime=dm_runtime(scope), http_identity_client=identity_client,
        )) as client:
            credential = membership._credential(client, scope)
            submitted = membership._post(client, credential, *membership._submission(scope))
            assert submitted.status_code == 200, submitted.text
            entries = valkey.xrange(stream)
            assert len(entries) == 1
            fields = entries[0][1]
            capsule, payload = fields["curie_identity_context_v1"], fields["payload"]
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),),
            )
            context = codec.open_invocation_context(capsule, ring)
            assert isinstance(context, codec.InvocationContextV1)
            yield SimpleNamespace(
                client=client, scope=scope, capsule=capsule, context=context,
                payload=payload, entries=entries,
            )
        assert identity_client.close_status().state == "closed"
    finally:
        keys = membership._keys(valkey, scope["binding"])
        if keys:
            valkey.delete(*keys)


@pytest.fixture
def dm_case(
    clean_db: None, valkey: redis.Redis, runs_stream: str, synthetic_provider: SimpleNamespace,
) -> Iterator[SimpleNamespace]:
    """@spec APPROVER-DM-1: an eligible agent with one ingress turn; no DM route yet.

    The test writes the DM route itself, so a missing route mode fails in the
    test body, not in setup.
    """
    provider = synthetic_provider
    provider.roster.update(default_roster())
    with dm_queued(provider, valkey, runs_stream) as case:
        case.provider, case.valkey, case.stream = provider, valkey, runs_stream
        case.key = selector_key()
        case.turn_used = False
        prepare(case.client, case.scope)
        yield case


# --- operator route writes -----------------------------------------------------------


def dm_resolution(identity: str) -> dict[str, Any]:
    """@spec APPROVER-DM-1: the pinned route mode, exactly."""
    return {"mode": DM_MODE, "identity": identity}


def dm_routes(
    scope: dict[str, Any], key: Any, *, identity: str | None = None, ticket: bool = True,
    route: str = ROUTE, resolution: Any = None, notification: dict[str, Any] | None = None,
    approvers: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The full replacement map with a DM resolution on ``route``.

    ``ticket`` registers the selector on the owning route; ``resolution``
    replaces the DM object wholesale (shape refusals).
    @spec APPROVER-DM-1.
    """
    ticketed = registration(key, scope["role_version"]) if ticket else None
    routes = route_map(scope["address"], ticket=ticketed)
    routes[route]["resolution"] = (
        dm_resolution(identity or scope["adapter"]) if resolution is None else resolution
    )
    if notification is not None:
        routes[route]["notification"] = notification
    if approvers is not None:
        routes[route]["approvers"] = approvers
    return routes


def write_dm(client: TestClient, scope: dict[str, Any], routes: dict[str, Any]) -> Any:
    """One versioned operator write; tracks the configuration version. @spec APPROVER-DM-1."""
    response = write_routes(client, scope["agent"], routes, expected=scope["config_version"])
    if response.status_code == 200:
        scope["config_version"] = response.json()["connector_requester_identity_version"]
    return response


def configured_dm(case: SimpleNamespace, **options: Any) -> dict[str, Any]:
    """@spec APPROVER-DM-1: a DM route write that must be accepted."""
    response = write_dm(case.client, case.scope, dm_routes(case.scope, case.key, **options))
    assert response.status_code == 200, response.text
    return dict(response.json())


# --- tickets and creation ----------------------------------------------------------------


def account(
    receiver: dict[str, Any], native: str, *, installation: str | None = None,
    namespace: Any = None,
) -> dict[str, Any]:
    """A ticket recipient account on ``receiver``'s installation and namespace by default.

    @spec APPROVER-DM-1.
    """
    space = namespace or receiver["namespace"]
    return {
        "provider": "slack",
        "installation_id": installation or receiver["installation"],
        "namespace": {
            "id": space.id, "authority": space.authority, "kind": space.kind, "key": space.key,
        },
        "account_id": native,
    }


def selected(case: SimpleNamespace, subject: str, *accounts: dict[str, Any]) -> dict[str, Any]:
    """One selected approver claim, with recipient accounts only when given.

    @spec APPROVER-DM-1.
    """
    claim = principal_claims(case.scope["tenant"], subject)
    if accounts:
        claim["recipient_accounts"] = list(accounts)
    return claim


def default_selection(case: SimpleNamespace) -> list[dict[str, Any]]:
    """@spec APPROVER-DM-1: approver A with one account on the binding's receiver."""
    return [selected(case, APPROVER_A, account(case.scope["main"], NATIVE_A))]


def dm_request(
    case: SimpleNamespace, approvers: list[dict[str, Any]] | None = None, *,
    turn: SimpleNamespace | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """A DM creation body (``card_channel`` null) and its ticket claims.

    @spec APPROVER-DM-1 APPROVER-DM-2.
    """
    payload = ticket_payload(case, ARGS)
    payload["approvers"] = default_selection(case) if approvers is None else approvers
    request = dynamic_request(turn or _turn(case), carried(case, ARGS, payload=payload))
    request["approval"]["card_channel"] = None
    return request, payload


def create(client: TestClient, request: dict[str, Any]) -> Any:
    """@spec APPROVER-DM-1: the internal creation owner with the worker token."""
    return client.post(CREATION_PATH, json=request, headers=worker_headers())


def expected_delivery(receiver: dict[str, Any], native: str = NATIVE_A) -> dict[str, Any]:
    """@spec APPROVER-DM-2: the pinned ``delivery`` block for ``receiver``'s identity."""
    return {
        "mode": DM_MODE, "identity": receiver["adapter"],
        "provider_installation_id": receiver["installation"], "recipient": native,
    }


def dm_created(
    response: Any, request: dict[str, Any], delivery: dict[str, Any], *,
    duplicate: bool = False,
) -> dict[str, Any]:
    """A DM creation response: the ordinary keys plus exactly the pinned ``delivery``.

    @spec APPROVER-DM-2.
    """
    assert response.status_code == (200 if duplicate else 201), response.text
    assert response.headers["cache-control"] == "no-store"
    body = response.json()
    assert set(body) == CREATION_KEYS | {"delivery"}, body
    assert body["request_id"] == request["request_id"]
    assert body["duplicate"] is duplicate and body["status"] == "pending", body
    assert body["delivery"] == delivery, body
    return dict(body)


def refused_creation(
    case: SimpleNamespace, request: dict[str, Any], payload: dict[str, Any] | None,
    status: int, code: str,
) -> None:
    """Exact status and code, no row for the event and the ticket nonce unspent.

    @spec APPROVER-DM-1.
    """
    response = create(case.client, request)
    assert response.status_code == status, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"detail": code}, response.text
    assert rows(request["approval"]["dedupe_key"]) == []
    if payload is not None:
        assert nonce_uses(case.scope["agent"], payload["nonce"]) == 0


def nonce_uses(agent_id: str, nonce: str) -> int:
    """@spec APPROVER-DM-1: approvals of this agent spending ``nonce``."""
    found = sql_dicts(
        "SELECT count(*) AS n FROM curie.approvals WHERE agent_id = :agent "
        "AND selection_nonce = :nonce",
        {"agent": uuid.UUID(agent_id), "nonce": nonce},
    )
    return int(found[0]["n"])


def dm_row(
    case: SimpleNamespace, approvers: list[dict[str, Any]] | None = None, *,
    receiver: str = "main",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create one pending DM approval; returns (stored row, creation request).

    @spec APPROVER-DM-1 APPROVER-DM-2.
    """
    request, _ = dm_request(case, approvers)
    dm_created(create(case.client, request), request, expected_delivery(case.scope[receiver]))
    found = rows(request["approval"]["dedupe_key"])
    assert len(found) == 1, found
    assert found[0]["card_channel"] is None and found[0]["selection_mode"] == "dynamic"
    return found[0], request


def set_identity_status(identity_id: str, status: str) -> None:
    """@spec APPROVER-DM-1: an identity row taken out of (or back into) service."""
    sql_rows(
        "UPDATE curie.channel_identities SET status = :status WHERE id = :id",
        {"status": status, "id": uuid.UUID(identity_id)},
    )


# --- card registration ---------------------------------------------------------------------


def card_path(approval_id: Any) -> str:
    """@spec APPROVER-DM-2."""
    return f"{CREATION_PATH}/{approval_id}/card"


def card_body(
    request: dict[str, Any], approval_id: Any, *, card_identity: str,
    channel: str = DM_CHANNEL, recipient: str = NATIVE_A, **changes: Any,
) -> dict[str, Any]:
    """The pinned registration request for the creation ``request``. @spec APPROVER-DM-2."""
    body = {
        "schema_version": 1, "request_id": str(uuid.uuid4()), "approval_id": str(approval_id),
        "dedupe_key": request["approval"]["dedupe_key"],
        "identity": json.loads(json.dumps(request["identity"])),
        "card_identity": card_identity, "recipient": recipient, "channel": channel,
    }
    body.update(changes)
    return body


def post_card(
    client: TestClient, approval_id: Any, body: dict[str, Any],
    headers: dict[str, str] | None = None,
) -> Any:
    """@spec APPROVER-DM-2: the internal registration endpoint, worker token by default."""
    return client.post(
        card_path(approval_id), json=body,
        headers=worker_headers() if headers is None else headers,
    )


def card_registered(
    response: Any, body: dict[str, Any], *, status: str = "pending",
    channel: str | None = DM_CHANNEL, duplicate: bool = False,
) -> dict[str, Any]:
    """The pinned registration response. @spec APPROVER-DM-2."""
    assert response.status_code == 200, response.text
    assert response.headers["cache-control"] == "no-store"
    out = response.json()
    assert set(out) == CARD_KEYS, out
    assert out["schema_version"] == 1
    assert (out["request_id"], out["approval_id"]) == (body["request_id"], body["approval_id"])
    assert (out["status"], out["card_channel"], out["duplicate"]) == (status, channel, duplicate)
    return dict(out)


def register_dm_card(
    case: SimpleNamespace, row: dict[str, Any], request: dict[str, Any], *,
    receiver: str = "main", channel: str = DM_CHANNEL,
) -> dict[str, Any]:
    """Register ``channel`` as the DM card; returns the stored row afterwards.

    @spec APPROVER-DM-2.
    """
    body = card_body(
        request, row["id"], card_identity=case.scope[receiver]["adapter"], channel=channel,
    )
    card_registered(post_card(case.client, row["id"], body), body, channel=channel)
    found = rows(request["approval"]["dedupe_key"])
    assert len(found) == 1 and found[0]["card_channel"] == channel, found
    return found[0]


def card_audits(approval_id: Any) -> list[dict[str, Any]]:
    """@spec APPROVER-DM-2: the ``card_registered`` audit rows, in order."""
    return sql_dicts(
        "SELECT action, actor, authorizer, authorized, evidence "
        "FROM curie.approval_audit_entries WHERE approval_id = :id "
        "AND action = 'card_registered' ORDER BY created_at, id",
        {"id": uuid.UUID(str(approval_id))},
    )


def refused_exactly(response: Any, status: int, code: str) -> None:
    """@spec APPROVER-DM-2: bounded detail, never cached."""
    assert response.status_code == status, response.text
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == {"detail": code}, response.text


# --- answers -----------------------------------------------------------------------------


def dm_answer(
    case: SimpleNamespace, approval_id: Any, native: str, *, channel: str,
    receiver: str = "main", decision: str = "approved", team: str | None = None,
) -> Any:
    """``native`` clicks a card in ``channel`` as seen by ``receiver``'s bot.

    The construction of ``approval_answer_support.answer_headers`` with the
    interaction channel, the receiving registration (identity, installation,
    key) and the actor's workspace each chosen by the caller.
    @spec APPROVER-DM-3.
    """
    scope = case.scope
    chosen = scope[receiver]
    body = json.dumps({"decision": decision}, separators=(",", ":")).encode()
    evidence = slack_evidence(user_id=native, interaction_user_team_id=team or chosen["account"])
    apr = approval_principal.mint(
        get_settings().approval_chat_attester_secret, subject=native, kind="chat",
        actor_channel=channel, approval_id=str(approval_id),
        scope=approval_principal.APPROVE_SCOPE, exp=int(time.time()) + 60,
        slack=SlackEvidence.model_validate(evidence),
    )
    proof = {
        "schema_version": 1, "purpose": "approval_answer", "operation": "resolve",
        "receiver_id": chosen["receiver_id"], "receiver_generation": 1,
        "channel_identity_id": chosen["identity"],
        "provider_installation_id": chosen["installation"],
        "approval_id": str(approval_id),
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "attestation_sha256": hashlib.sha256(apr.encode("ascii")).hexdigest(),
        "native_channel_id": channel,
        "slack": {**evidence, "enterprise_ids": ()},
    }
    ring = codec.build_identity_keyring(
        "receiver", ((chosen["receiver_id"], 1, chosen["receiver_key"]),),
    )
    token = wire.sign_approval_answer_proof(wire.ApprovalAnswerProofV1.model_validate(proof), ring)
    return case.client.post(
        f"/approvals/{approval_id}/resolve", content=body,
        headers={
            "X-Curie-Approval-Principal": apr, PROOF_HEADER: token,
            "Content-Type": "application/json",
        },
    )


def email_answer(
    case: SimpleNamespace, approval_id: Any, sender: str = "approver-a@example.com",
) -> Any:
    """A mail adapter forwarding an email reply with a signed email answer proof.

    The proof is signed by an email receiver the API never registered, which is
    the strongest proof this suite can sign for a Slack-origin binding.
    @spec APPROVER-DM-3.
    """
    scope = case.scope
    body = json.dumps({"decision": "approved"}, separators=(",", ":")).encode()
    receiver_id = str(uuid.uuid4())
    proof = wire.EmailApprovalAnswerProofV1.model_validate({
        "schema_version": 1, "purpose": "approval_answer", "operation": "resolve",
        "receiver_id": receiver_id, "receiver_generation": 1, "binding_id": scope["binding"],
        "approval_id": str(approval_id), "sender": sender,
        "body_sha256": hashlib.sha256(body).hexdigest(),
    })
    ring = codec.build_identity_keyring("receiver", ((receiver_id, 1, b"e" * 32),))
    headers = {
        "X-Curie-Adapter-Principal": adapter_principal.mint(
            get_settings().api_key, subject=email_receivers.MAIL_ADAPTER,
            bindings=[scope["binding"]], exp=int(time.time()) + 60,
        ),
        "X-Curie-Approval-Actor": sender,
        "Content-Type": "application/json",
        PROOF_HEADER: wire.sign_email_approval_answer_proof(proof, ring),
    }
    return case.client.post(f"/approvals/{approval_id}/resolve", content=body, headers=headers)


def no_effect(case: SimpleNamespace, approval_id: Any, *, card_channel: str | None) -> None:
    """A refused answer leaves the row pending on its card and publishes nothing.

    @spec APPROVER-DM-3.
    """
    still_pending(approval_id)
    no_wake(case, approval_id)
    found = sql_dicts(
        "SELECT card_channel FROM curie.approvals WHERE id = :id",
        {"id": uuid.UUID(str(approval_id))},
    )
    assert found == [{"card_channel": card_channel}], found


def unauthenticated(
    case: SimpleNamespace, approval_id: Any, response: Any, *, card_channel: str | None,
) -> None:
    """An interaction that is not proof for this card: 401, no effect. @spec APPROVER-DM-3."""
    assert response.status_code == 401, response.text
    assert response.json() == {"detail": INVALID_PRINCIPAL}, response.text
    no_effect(case, approval_id, card_channel=card_channel)


def lapse(case: SimpleNamespace, row: dict[str, Any]) -> dict[str, Any]:
    """A pending dynamic row past its deadline, validly resealed as the expiry owner needs.

    ``test_http_approval_legacy_writers.lapse`` for a dynamic row: the record
    digest covers the selection fields too. @spec APPROVER-DM-2.
    """
    module, ring, capsule = opened(case, row)
    lapsed = {**row, "expires_at": datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=5)}
    data = capsule.model_dump(mode="json")
    data["approval_record_sha256"] = digest(canonical({
        key: lapsed[key] for key in RECORD_FIELDS + SELECTION_FIELDS
    }))
    token = module.seal_approval_context(module.ApprovalContextV1.model_validate(data), ring)
    sql_rows(
        "UPDATE curie.approvals SET expires_at = :at, identity_context = :token WHERE id = :id",
        {"at": lapsed["expires_at"], "token": token, "id": uuid.UUID(str(row["id"]))},
    )
    found = rows(row["dedupe_key"])
    assert len(found) == 1, found
    return found[0]
