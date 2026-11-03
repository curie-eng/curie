"""Email-origin approvals answered by email reply under the HTTP approver role.

An email turn admitted on a signed email receiver proof raises an approval as
before; the mail adapter forwards an email reply with a purpose-bound email
approval answer proof, and the agent's approver role decides, fresh. Real ASGI
routes, owned synthetic TLS directory, PostgreSQL and Valkey; synthetic
``example.com`` addresses only.
@spec HTTP-APPROVAL-CONTINUITY-4/7/11/15 HTTP-INVOCATION-EXECUTION-29.
"""

from __future__ import annotations

import hashlib
import json
import ssl
import time
import uuid
from collections.abc import Iterator
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api import adapter_principal
from curie_api.config import get_settings
from curie_api.identity.http_credential_registry import build_provider_credential_registry
from curie_api.identity.http_transport import HttpIdentityClient, ProviderBearer
from curie_api.main import create_app
from curie_api.models import Agent, AgentChannel, ChannelIdentity
from curie_api.resumequeue import resume_event_id
from curie_internal import identity_approval as wire
from curie_internal import identity_context as codec
from fastapi.testclient import TestClient
from role_policy_support import run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_email_receivers as email_receivers
from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_approval_answers import answer, approver_role, resolve, unchanged
from apps.api.tests.test_http_approval_creation import opened, request_for, rows
from apps.api.tests.test_http_approval_grants import resume_check, resume_token
from apps.api.tests.test_http_approval_legacy_writers import audits, created
from apps.api.tests.test_http_invocation_membership import SENDER
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider
from apps.api.tests.test_http_turn_authority import assert_response, post_check

EMAIL_SENDER = email_receivers.SENDER
PROOF_HEADER = "X-Curie-Approval-Receiver-Proof"
ROUTE = "synthetic-email-card-route"


def _default_identity(slack: dict[str, Any]) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-7: a fixed card of an email turn speaks as ``default``."""

    async def rename(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-7."""
        async with factory() as session, session.begin():
            identity = await session.get(ChannelIdentity, uuid.UUID(slack["identity"]))
            binding = await session.get(AgentChannel, uuid.UUID(slack["binding"]))
            assert identity is not None and binding is not None
            identity.name = "default"
            binding.adapter = "default"

    run(rename)
    slack["adapter"] = "default"


def _runtime(module: ModuleType, email: dict[str, Any]) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-29: one Slack and one email receiver."""
    slack = email["slack"]
    providers = build_provider_credential_registry(
        ((slack["configuration"], ProviderBearer(membership.BEARER)),)
    )
    registration = module.ReceiverRegistrationV1.model_validate({
        "schema_version": 1, "receiver_id": slack["receiver_id"], "generation": 1,
        "tenant_id": slack["tenant"], "channel_identity_id": slack["identity"],
        "channel_identity_name": slack["adapter"],
        "provider_installation_id": slack["installation"], "namespace": slack["namespace"],
        "installation_account": slack["account"], "bot_user_id": "U0EXAMPLEBOT",
        "binding_ids": (slack["binding"],), "human_native_ids": (SENDER,),
    })
    return module.build_http_invocation_runtime(
        providers,
        ((registration, slack["receiver_key"]),),
        (("synthetic-context", 1, slack["context_key"]),),
        signing_key=("synthetic-context", 1),
        email_receivers=(
            (email_receivers._registration(module, email), email_receivers.EMAIL_KEY),
        ),
    )


@pytest.fixture
def email_case(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> Iterator[SimpleNamespace]:
    """@spec HTTP-INVOCATION-EXECUTION-29: one email turn queued through the actual ingress."""
    provider = synthetic_provider
    email = email_receivers._email_scope(provider.url)
    slack = email["slack"]
    _default_identity(slack)
    try:
        with TestClient(create_app(
            http_identity_runtime=_runtime(email_receivers._email_api(), email),
            http_identity_client=HttpIdentityClient(tls_context=provider.tls),
        )) as client:
            body = email_receivers._body()
            token = email_receivers._email_credential(client, email)
            mailed = email_receivers._post(
                client, token, body, email_receivers._proof(email, body),
            )
            assert mailed.status_code == 200, mailed.text
            case = email_receivers._queued(valkey, runs_stream, slack["context_key"])
            assert case.context.scope.provider == "email"
            case.client, case.provider, case.scope, case.email = client, provider, slack, email
            case.request = request_for(case)
            assert case.request["approval"]["reply_kind"] == "email"
            assert case.request["approval"]["card_channel"] == email_receivers.INBOX
            yield case
    finally:
        email_receivers._cleanup(valkey, email)


def email_answer(
    case: SimpleNamespace, *, decision: str = "approved", actor: str = EMAIL_SENDER,
    signed_body: bytes | None = None, key: bytes = email_receivers.EMAIL_KEY,
    proof: str | None = "answer", **changes: Any,
) -> tuple[bytes, dict[str, str]]:
    """@spec HTTP-APPROVAL-CONTINUITY-15: what a mail adapter forwards for one email reply."""
    email = case.email
    body = json.dumps({"decision": decision}, separators=(",", ":")).encode()
    headers = {
        "X-Curie-Adapter-Principal": adapter_principal.mint(
            get_settings().api_key, subject=email_receivers.MAIL_ADAPTER,
            bindings=[email["binding"]], exp=int(time.time()) + 60,
        ),
        "X-Curie-Approval-Actor": actor,
        "Content-Type": "application/json",
    }
    if proof == "invocation":
        # A valid email invocation proof over the same bytes and sender.
        invocation = codec.EmailReceiverProofV1.model_validate({
            "schema_version": 1, "receiver_id": email["receiver_id"], "receiver_generation": 1,
            "binding_id": email["binding"], "sender": actor.lower(),
            "delivery_id": "<synthetic-reply@example.com>",
            "body_sha256": hashlib.sha256(body).hexdigest(),
        })
        ring = codec.build_identity_keyring(
            "receiver", ((email["receiver_id"], 1, email_receivers.EMAIL_KEY),),
        )
        headers[PROOF_HEADER] = codec.sign_email_receiver_proof(invocation, ring)
    elif proof == "answer":
        value = {
            "schema_version": 1, "purpose": "approval_answer", "operation": "resolve",
            "receiver_id": email["receiver_id"], "receiver_generation": 1,
            "binding_id": email["binding"], "approval_id": str(case.row["id"]),
            "sender": actor.lower(),
            "body_sha256": hashlib.sha256(body if signed_body is None else signed_body)
            .hexdigest(),
            **changes,
        }
        ring = codec.build_identity_keyring("receiver", ((email["receiver_id"], 1, key),))
        headers[PROOF_HEADER] = wire.sign_email_approval_answer_proof(
            wire.EmailApprovalAnswerProofV1.model_validate(value), ring,
        )
    return body, headers


def test_email_answer_by_an_approver_group_member_approves_and_resumes_by_email(
    email_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4/11/15: fresh role check, email actor, email reply."""
    case = email_case
    case.row = created(case)
    assert (case.row["reply_kind"], case.row["card_channel"]) == ("email", email_receivers.INBOX)
    _, _, capsule = opened(case, case.row)
    assert (capsule.scope.provider, capsule.scope.native_id) == ("email", EMAIL_SENDER)
    approver_role(case)
    stream = valkey.xrange(runs_stream)
    before = len(case.provider.requests)

    response = resolve(case, *email_answer(case))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "approved"
    assert response.json()["resolved_by"] == EMAIL_SENDER
    lookups = case.provider.requests[before:]
    assert sorted(item["purpose"] for item in lookups) == [
        "approval", "approval", "invocation", "invocation",
    ]
    assert {item["scope"]["sender"]["native_id"] for item in lookups} == {EMAIL_SENDER}
    row = rows(case.row["dedupe_key"])[0]
    assert row["status"] == "approved" and row["resumed_at"] is not None
    assert [entry["action"] for entry in audits(row["id"])] == ["resolved"]
    _, _, saved = opened(case, row)
    assert saved.actor_scope is not None
    assert (saved.actor_scope.provider, saved.actor_scope.native_id) == ("email", EMAIL_SENDER)
    assert saved.actor_receiver_id == case.email["receiver_id"]
    assert saved.actor_independent_human is False

    added = valkey.xrange(runs_stream)[len(stream):]
    assert len(added) == 1 and "curie_identity_context_v1" in added[0][1]
    resumed = QueuedTurn.model_validate_json(added[0][1]["payload"])
    assert resumed.event_id == resume_event_id(case.row["id"])
    assert resumed.reply_handle is not None
    assert (resumed.reply_handle.kind, resumed.reply_handle.channel) == (
        "email", email_receivers.INBOX,
    )
    valkey.xdel(runs_stream, added[0][0])

    # The same reply again is settled, not a second answer.
    again = resolve(case, *email_answer(case))
    assert again.status_code == 409, again.text

    # The worker's resume current check continues for both email subjects.
    case.approval_id = str(case.row["id"])
    token, payload = resume_token(case, saved)
    checked = len(case.provider.requests)
    request = resume_check(case, token, payload, stage="prepare")
    assert_response(
        post_check(case.client, request), request, decision="continue",
        reason="current_authority",
    )
    assert sorted(item["purpose"] for item in case.provider.requests[checked:]) == [
        "approval", "invocation",
    ]


def test_email_answer_by_a_sender_outside_the_approver_group_is_refused(
    email_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-15: invocation permission never becomes approval."""
    case = email_case
    case.row = created(case)
    approver_role(case, group="SyntheticApproverOnlyGroup")
    before = len(case.provider.requests)
    response = resolve(case, *email_answer(case))
    assert response.status_code == 403, response.text
    assert response.json() == {"detail": "caller_not_allowed"}
    unchanged(case, before + 2)


@pytest.mark.parametrize(
    ("attempt", "status"),
    [
        ("foreign_key", 401), ("other_approval", 401), ("other_decision", 401),
        ("other_sender", 401), ("other_binding", 401), ("invocation", 401), ("missing", 403),
    ],
)
def test_forged_replayed_or_wrong_purpose_email_proof_is_refused_before_lookup(
    email_case: SimpleNamespace, attempt: str, status: int,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-15: the proof binds approval, decision and sender."""
    case = email_case
    case.row = created(case)
    approver_role(case)
    before = len(case.provider.requests)
    rejected = json.dumps({"decision": "rejected"}, separators=(",", ":")).encode()
    submitted = {
        "foreign_key": lambda: email_answer(case, key=b"f" * 32),
        "other_approval": lambda: email_answer(case, approval_id=str(uuid.uuid4())),
        "other_decision": lambda: email_answer(case, signed_body=rejected),
        "other_sender": lambda: email_answer(case, sender=email_receivers.OTHER_SENDER),
        "other_binding": lambda: email_answer(case, binding_id=case.scope["binding"]),
        "invocation": lambda: email_answer(case, proof="invocation"),
        "missing": lambda: email_answer(case, proof=None),
    }[attempt]()
    response = resolve(case, *submitted)
    assert response.status_code == status, response.text
    unchanged(case, before)


def test_provider_outage_during_an_email_answer_fails_closed(
    email_case: SimpleNamespace,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-15: an unreachable provider is 503, never a pass."""
    case = email_case
    case.row = created(case)
    approver_role(case)
    state = case.client.app.state
    trusted = state.http_identity_client
    # A client that does not trust the synthetic provider's certificate observes nothing.
    state.http_identity_client = HttpIdentityClient(tls_context=ssl.create_default_context())
    try:
        before = len(case.provider.requests)
        response = resolve(case, *email_answer(case))
        assert response.status_code == 503, response.text
        assert response.json() == {"detail": "authority_unavailable"}
        unchanged(case, before)
    finally:
        state.http_identity_client = trusted


def test_email_turn_with_a_fixed_slack_card_is_answered_in_slack(
    email_case: SimpleNamespace, valkey: redis.Redis, runs_stream: str,
) -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-4/7: a fixed Slack card keeps the Slack answer."""
    case = email_case
    channel = case.scope["address"]

    async def route(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-APPROVAL-CONTINUITY-4."""
        async with factory() as session, session.begin():
            agent = await session.get(Agent, uuid.UUID(case.scope["agent"]))
            assert agent is not None
            agent.approval_routes = {ROUTE: {
                "resolution": {"kind": "slack", "address": channel},
                "approvers": {"users": [SENDER]},
            }}

    run(route)
    case.request["approval"].update(route=ROUTE, card_channel=channel)
    case.row = created(case)
    approver_role(case)
    stream = valkey.xrange(runs_stream)
    # An email reply cannot answer a card shown in Slack.
    before = len(case.provider.requests)
    assert resolve(case, *email_answer(case)).status_code == 404
    unchanged(case, before)
    response = resolve(case, *answer(case))
    assert response.status_code == 200, response.text
    assert response.json()["resolved_by"] == SENDER
    added = valkey.xrange(runs_stream)[len(stream):]
    assert len(added) == 1
    resumed = QueuedTurn.model_validate_json(added[0][1]["payload"])
    assert resumed.reply_handle is not None and resumed.reply_handle.kind == "email"
    valkey.xdel(runs_stream, added[0][0])


def test_email_approval_answer_proofs_are_purpose_separated() -> None:
    """@spec HTTP-APPROVAL-CONTINUITY-15: no proof verifies under another purpose."""
    receiver = str(uuid.uuid4())
    ring = codec.build_identity_keyring("receiver", ((receiver, 1, email_receivers.EMAIL_KEY),))
    answer_proof = wire.sign_email_approval_answer_proof(
        wire.EmailApprovalAnswerProofV1.model_validate({
            "schema_version": 1, "purpose": "approval_answer", "operation": "resolve",
            "receiver_id": receiver, "receiver_generation": 1, "binding_id": str(uuid.uuid4()),
            "approval_id": str(uuid.uuid4()), "sender": EMAIL_SENDER, "body_sha256": "0" * 64,
        }),
        ring,
    )
    assert answer_proof.startswith("curie-email-approval-answer-v1.")
    verified = wire.verify_email_approval_answer_proof(answer_proof, ring)
    assert isinstance(verified, wire.EmailApprovalAnswerProofV1)
    assert codec.verify_email_receiver_proof(answer_proof, ring) == codec.ContextRefusal(
        code="invalid_receiver_proof",
    )
    assert wire.verify_approval_answer_proof(answer_proof, ring) == codec.ContextRefusal(
        code="invalid_receiver_proof",
    )
    invocation = codec.sign_email_receiver_proof(
        codec.EmailReceiverProofV1.model_validate({
            "schema_version": 1, "receiver_id": receiver, "receiver_generation": 1,
            "binding_id": str(uuid.uuid4()), "sender": EMAIL_SENDER,
            "delivery_id": "<synthetic@example.com>", "body_sha256": "0" * 64,
        }),
        ring,
    )
    assert wire.verify_email_approval_answer_proof(invocation, ring) == codec.ContextRefusal(
        code="invalid_receiver_proof",
    )
    with pytest.raises(ValueError, match="invalid_receiver_proof"):
        wire.sign_email_approval_answer_proof(
            wire.EmailApprovalAnswerProofV1.model_construct(), ring,  # type: ignore[call-arg]
        )
