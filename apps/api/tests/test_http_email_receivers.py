"""Email receivers for HTTP identity, from operator file to served turn.

@spec HTTP-INVOCATION-EXECUTION-29.
Synthetic ``example.com`` domains and addresses only. No mail adapter, message
authentication or real directory is qualified here.
"""

from __future__ import annotations

import copy
import hashlib
import json
import uuid
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_api.config import get_settings
from curie_api.identity.http_credential_registry import build_provider_credential_registry
from curie_api.identity.http_invocation import HttpInvocationRuntime
from curie_api.identity.http_transport import HttpIdentityClient, ProviderBearer
from curie_api.main import create_app
from curie_api.models import AgentChannel
from curie_internal import identity_context as codec
from fastapi.testclient import TestClient
from role_policy_support import run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_identity_initializer as initializer
from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests import test_http_turn_authority as authority
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

DOMAIN = "example.com"
UNTRUSTED_DOMAIN = "other.example.com"
AUTHOR = "Acme.Person@Example.com"
SENDER = "acme.person@example.com"
OTHER_SENDER = "acme.other@example.com"
INBOX = "acme-agent@example.com"
MAIL_ADAPTER = "acme-mail"
EMAIL_KEY = b"e" * 32


def _email_codec() -> ModuleType:
    """@spec HTTP-INVOCATION-EXECUTION-29: missing features fail in the test body."""
    for symbol in (
        "EmailReceiverProofV1", "sign_email_receiver_proof", "verify_email_receiver_proof",
    ):
        assert hasattr(codec, symbol), f"email receiver proof missing {symbol}"
    return codec


def _email_api() -> ModuleType:
    """@spec HTTP-INVOCATION-EXECUTION-29: missing features fail in the test body."""
    module = membership._runtime_api()
    assert hasattr(module, "EmailReceiverRegistrationV1"), "email registration missing"
    return module


def _email_scope(endpoint: str) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-29: an email binding of a migrated, filtered agent.

    The agent's migrated invocation role makes the email binding require HTTP
    identity (criterion 22); no ChannelIdentity, installation or namespace row
    exists for email.
    """
    scope = membership._scope(endpoint)
    email: dict[str, Any] = {
        "tenant": scope["tenant"],
        "agent": scope["agent"],
        "configuration": scope["configuration"],
        "context_key": scope["context_key"],
        "address": INBOX,
        "adapter": MAIL_ADAPTER,
        "receiver_id": str(uuid.uuid4()),
        "identity": str(uuid.uuid4()),
        "installation": str(uuid.uuid4()),
        "namespace": {
            "id": str(uuid.uuid4()), "authority": "", "kind": "email_domain", "key": DOMAIN,
        },
        "slack_binding": scope["binding"],
        "slack": scope,
    }

    async def persist(factory: async_sessionmaker[AsyncSession]) -> None:
        """@spec HTTP-INVOCATION-EXECUTION-29: one email route for the same agent."""
        async with factory() as session, session.begin():
            binding = AgentChannel(
                tenant_id=uuid.UUID(scope["tenant"]),
                agent_id=uuid.UUID(scope["agent"]),
                kind="email",
                address=INBOX,
                adapter=MAIL_ADAPTER,
                endpoint="https://mail.example.com/reply",
                allowed_callers=None,
            )
            session.add(binding)
            await session.flush()
            email["binding"] = str(binding.id)

    run(persist)
    return email


def _registration(module: ModuleType, scope: dict[str, Any], **changes: Any) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-29: the operator's email registration."""
    value = {
        "schema_version": 1,
        "receiver_id": scope["receiver_id"],
        "generation": 1,
        "tenant_id": scope["tenant"],
        "channel_identity_id": scope["identity"],
        "provider_installation_id": scope["installation"],
        "binding_ids": [scope["binding"]],
        "domains": [scope["namespace"]],
    }
    value.update(changes)
    return module.EmailReceiverRegistrationV1.model_validate_json(json.dumps(value))


def _email_runtime(module: ModuleType, scope: dict[str, Any]) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-29: an email-only runtime, no Slack receiver."""
    providers = build_provider_credential_registry(
        ((scope["configuration"], ProviderBearer(membership.BEARER)),)
    )
    return module.build_http_invocation_runtime(
        providers,
        (),
        (("synthetic-context", 1, scope["context_key"]),),
        signing_key=("synthetic-context", 1),
        email_receivers=((_registration(module, scope), EMAIL_KEY),),
    )


def _email_credential(client: TestClient, scope: dict[str, Any]) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-29: a platform-key-minted channel token."""
    response = client.post(
        "/channels/token",
        headers={"X-API-Key": get_settings().api_key},
        json={"kind": "email", "address": INBOX, "adapter": MAIL_ADAPTER, "ttl_s": 60},
    )
    assert response.status_code == 200, response.text
    return str(response.json()["token"])


def _body(*, author: str = AUTHOR, delivery_id: str | None = None,
          text: str = "Synthetic email invocation") -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-29: the exact TurnIn bytes the adapter submits."""
    return json.dumps(
        {
            "kind": "email",
            "address": INBOX,
            "delivery_id": delivery_id or "<synthetic-" + uuid.uuid4().hex + "@example.com>",
            "conversation_id": "synthetic-thread@example.com",
            "author": author,
            "text": text,
            "reply_ref": "",
            "attachments": [],
        },
        separators=(",", ":"),
    ).encode()


def _proof(scope: dict[str, Any], body: bytes, **changes: Any) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-29: the adapter signs what it authenticated."""
    wire = _email_codec()
    parsed = json.loads(body)
    value = {
        "schema_version": 1,
        "receiver_id": scope["receiver_id"],
        "receiver_generation": 1,
        "binding_id": scope["binding"],
        "sender": parsed["author"].lower(),
        "delivery_id": parsed["delivery_id"],
        "body_sha256": hashlib.sha256(body).hexdigest(),
    }
    value.update(changes)
    proof = wire.EmailReceiverProofV1.model_validate_json(json.dumps(value))
    ring = codec.build_identity_keyring("receiver", ((scope["receiver_id"], 1, EMAIL_KEY),))
    return str(wire.sign_email_receiver_proof(proof, ring))


def _post(client: TestClient, token: str, body: bytes, proof: str | None) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-29: the actual channel ingress route."""
    headers = {"X-API-Key": token, "Content-Type": "application/json"}
    if proof is not None:
        headers["X-Curie-Receiver-Proof"] = proof
    return client.post("/channels/turns", content=body, headers=headers)


def _cleanup(valkey: redis.Redis, scope: dict[str, Any]) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-12: remove only this test's delivery state."""
    for binding in (scope["binding"], scope["slack_binding"]):
        keys = membership._keys(valkey, binding)
        if keys:
            valkey.delete(*keys)


def _client(provider: SimpleNamespace, scope: dict[str, Any]) -> TestClient:
    """@spec HTTP-INVOCATION-EXECUTION-29: the email-only runtime and provider client."""
    return TestClient(create_app(
        http_identity_runtime=_email_runtime(_email_api(), scope),
        http_identity_client=HttpIdentityClient(tls_context=provider.tls),
    ))


def _assert_untouched(
    valkey: redis.Redis, stream: str, provider: SimpleNamespace, scope: dict[str, Any],
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: no provider call, claim, quota or queue write."""
    assert provider.requests == []
    assert membership._keys(valkey, scope["binding"]) == set()
    assert valkey.xlen(stream) == 0


@pytest.mark.parametrize(
    "attempt", ["no_proof", "other_author", "other_body", "slack_proof", "foreign_key"],
)
def test_unproven_email_sender_refuses_before_provider_or_delivery_state(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, attempt: str,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: a channel token alone never names the sender."""
    scope = _email_scope(synthetic_provider.url)
    try:
        with _client(synthetic_provider, scope) as client:
            token = _email_credential(client, scope)
            body = _body()
            proof: str | None = _proof(scope, body)
            if attempt == "no_proof":
                proof = None
            elif attempt == "other_author":
                # A proof signed for one address, submitted under another author.
                body = _body(author=OTHER_SENDER, delivery_id=json.loads(body)["delivery_id"])
                proof = _proof(scope, body, sender=SENDER)
            elif attempt == "other_body":
                body = body.replace(b"Synthetic email invocation", b"Synthetic other request")
            elif attempt == "slack_proof":
                slack = codec.ReceiverProofV1(
                    schema_version=1, receiver_id=scope["receiver_id"], receiver_generation=1,
                    channel_identity_id=scope["identity"],
                    provider_installation_id=scope["installation"],
                    binding_id=scope["binding"], binding_generation=1,
                    delivery_id=json.loads(body)["delivery_id"], native_channel_id=INBOX,
                    conversation_id="synthetic-thread@example.com",
                    body_sha256=hashlib.sha256(body).hexdigest(),
                    slack=codec.ReceiverSlackEvidenceV1(
                        delivery="app_mention", user_id=SENDER, event_user_team=None,
                        event_team=membership.ACCOUNT, interaction_user_team_id=None,
                        is_ext_shared_channel=False, enterprise_ids=(),
                    ),
                )
                proof = codec.sign_receiver_proof(slack, codec.build_identity_keyring(
                    "receiver", ((scope["receiver_id"], 1, EMAIL_KEY),),
                ))
            else:
                wire = _email_codec()
                value = wire.EmailReceiverProofV1.model_validate_json(json.dumps({
                    "schema_version": 1, "receiver_id": scope["receiver_id"],
                    "receiver_generation": 1, "binding_id": scope["binding"],
                    "sender": SENDER, "delivery_id": json.loads(body)["delivery_id"],
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                }))
                proof = wire.sign_email_receiver_proof(value, codec.build_identity_keyring(
                    "receiver", ((scope["receiver_id"], 1, b"f" * 32),),
                ))
            response = _post(client, token, body, proof)
            assert response.status_code == 401, response.text
            assert response.json() == {"detail": "invalid_http_receiver"}
            _assert_untouched(valkey, runs_stream, synthetic_provider, scope)
    finally:
        _cleanup(valkey, scope)


def test_signed_email_sender_queues_one_context_in_its_domain_namespace(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: lowercased address, registered domain namespace."""
    provider = synthetic_provider
    scope = _email_scope(provider.url)
    try:
        with _client(provider, scope) as client:
            token = _email_credential(client, scope)
            body = _body()
            proof = _proof(scope, body)
            accepted = _post(client, token, body, proof)
            assert accepted.status_code == 200, accepted.text
            assert accepted.json()["duplicate"] is False
            entries = valkey.xrange(runs_stream)
            assert len(entries) == 1
            fields = entries[0][1]
            assert set(fields) == {"payload", "curie_identity_context_v1"}
            sender = {
                "provider": "email",
                "channel_identity_id": scope["identity"],
                "provider_installation_id": scope["installation"],
                "namespace": scope["namespace"],
                "native_id": SENDER,
            }
            assert len(provider.requests) == 1
            assert provider.requests[0]["scope"]["sender"] == sender
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),),
            )
            context = codec.open_invocation_context(fields["curie_identity_context_v1"], ring)
            assert isinstance(context, codec.InvocationContextV1)
            assert context.receiver_id == scope["receiver_id"]
            assert context.receiver_generation == 1
            assert context.independent_human is False
            assert context.scope.model_dump() == {
                "tenant_id": scope["tenant"],
                "agent_id": scope["agent"],
                "binding_id": scope["binding"],
                "binding_generation": context.scope.binding_generation,
                "channel_identity_id": scope["identity"],
                "provider_installation_id": scope["installation"],
                "provider": "email",
                "native_id": SENDER,
                "namespace": scope["namespace"],
            }
            turn = QueuedTurn.model_validate_json(fields["payload"])
            assert turn.author == AUTHOR and turn.reply_handle.kind == "email"
            assert turn.author_principal is not None

            # The same message resubmitted is a duplicate, not a second turn.
            again = _post(client, token, body, proof)
            assert again.status_code == 200, again.text
            assert again.json() == {
                "event_id": accepted.json()["event_id"],
                "stream_id": accepted.json()["stream_id"],
                "duplicate": True,
            }
            assert valkey.xrange(runs_stream) == entries

            # Another body under the same Message-ID is a conflicting intent.
            changed = _body(delivery_id=json.loads(body)["delivery_id"], text="Synthetic change")
            conflict = _post(client, token, changed, _proof(scope, changed))
            assert conflict.status_code == 409, conflict.text
            assert conflict.json() == {"detail": "delivery_intent_conflict"}
            assert valkey.xrange(runs_stream) == entries
    finally:
        _cleanup(valkey, scope)


def test_untrusted_email_domain_is_a_caller_refusal_before_lookup(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: an unlisted domain never reaches the provider."""
    scope = _email_scope(synthetic_provider.url)
    try:
        with _client(synthetic_provider, scope) as client:
            token = _email_credential(client, scope)
            body = _body(author="acme.person@" + UNTRUSTED_DOMAIN)
            response = _post(client, token, body, _proof(scope, body))
            assert response.status_code == 403, response.text
            assert response.json() == {"detail": "caller_not_allowed"}
            _assert_untouched(valkey, runs_stream, synthetic_provider, scope)
    finally:
        _cleanup(valkey, scope)


def test_current_check_reconstructs_the_email_sender_only_from_its_registration(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: the worker check refuses an unlisted namespace."""
    provider = synthetic_provider
    scope = _email_scope(provider.url)
    try:
        with _client(provider, scope) as client:
            token = _email_credential(client, scope)
            body = _body()
            assert _post(client, token, body, _proof(scope, body)).status_code == 200
            fields = valkey.xrange(runs_stream)[0][1]
            ring = codec.build_identity_keyring(
                "context", (("synthetic-context", 1, scope["context_key"]),),
            )
            context = codec.open_invocation_context(fields["curie_identity_context_v1"], ring)
            assert isinstance(context, codec.InvocationContextV1)
            invocation = SimpleNamespace(
                capsule=fields["curie_identity_context_v1"], payload=fields["payload"],
                context=context,
            )
            first = authority.request_for(invocation)
            authority.assert_response(
                authority.post_check(client, first), first,
                decision="continue", reason="current_authority",
            )
            assert len(provider.requests) == 2
            assert provider.requests[1]["scope"]["sender"]["native_id"] == SENDER

            # A context naming a domain descriptor the registration does not list.
            unlisted = context.model_copy(update={"scope": context.scope.model_copy(update={
                "namespace": codec.ContextNamespaceV1(
                    id=str(uuid.uuid4()), authority="", kind="email_domain", key=DOMAIN,
                ),
            })})
            forged = SimpleNamespace(
                capsule=codec.seal_invocation_context(
                    codec.InvocationContextV1.model_validate(unlisted.model_dump()), ring,
                ),
                payload=fields["payload"], context=unlisted,
            )
            second = authority.request_for(forged)
            authority.assert_response(
                authority.post_check(client, second), second,
                decision="refuse", reason="invalid_context",
            )
            assert len(provider.requests) == 2
    finally:
        _cleanup(valkey, scope)


def test_email_proofs_and_slack_proofs_are_purpose_separated() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: distinct signed prefixes and a strict sender."""
    wire = _email_codec()
    receiver = str(uuid.uuid4())
    ring = codec.build_identity_keyring("receiver", ((receiver, 1, EMAIL_KEY),))
    value = {
        "schema_version": 1, "receiver_id": receiver, "receiver_generation": 1,
        "binding_id": str(uuid.uuid4()), "sender": SENDER,
        "delivery_id": "<synthetic@example.com>", "body_sha256": "0" * 64,
    }
    token = wire.sign_email_receiver_proof(
        wire.EmailReceiverProofV1.model_validate_json(json.dumps(value)), ring,
    )
    assert token.startswith("curie-email-receiver-v1.")
    assert isinstance(wire.verify_email_receiver_proof(token, ring), wire.EmailReceiverProofV1)
    assert isinstance(codec.verify_receiver_proof(token, ring), codec.ContextRefusal)
    for sender in ("Acme.Person@example.com", "acme@person@example.com", "@example.com",
                   "acme.person@", "acme person@example.com", "acme.person"):
        with pytest.raises(ValueError):
            wire.EmailReceiverProofV1.model_validate_json(json.dumps({**value, "sender": sender}))


# --- operator file -------------------------------------------------------------

EMAIL_RECEIVER = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
EMAIL_IDENTITY = "66666666-6666-4666-8666-666666666666"
EMAIL_INSTALLATION = "77777777-7777-4777-8777-777777777777"
EMAIL_BINDING = "88888888-8888-4888-8888-888888888888"
EMAIL_NAMESPACE = "99999999-9999-4999-8999-999999999999"


def email_entry(**changes: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-29: one synthetic email receiver entry."""
    registration: dict[str, Any] = {
        "schema_version": 1, "receiver_id": EMAIL_RECEIVER, "generation": 1,
        "tenant_id": initializer.TENANT, "channel_identity_id": EMAIL_IDENTITY,
        "provider_installation_id": EMAIL_INSTALLATION, "binding_ids": [EMAIL_BINDING],
        "domains": [{"id": EMAIL_NAMESPACE, "authority": "", "kind": "email_domain",
                     "key": DOMAIN}],
    }
    registration.update(changes)
    return {"registration": registration, "key": initializer.key(EMAIL_KEY)}


def test_email_only_operator_document_builds_the_runtime(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: an installation may register email receivers only."""
    module = initializer.api()
    for document in (
        initializer.document(receivers=[], email_receivers=[email_entry()]),
        initializer.document(email_receivers=[email_entry()]),
        initializer.document(),
    ):
        initializer.write(tmp_path, document)
        runtime = module.load_http_identity_runtime(str(tmp_path))
        assert type(runtime) is HttpInvocationRuntime


@pytest.mark.parametrize(
    "document",
    [
        pytest.param(initializer.document(receivers=[]), id="neither"),
        pytest.param(initializer.document(receivers=[], email_receivers=[]), id="empty-email"),
        pytest.param(initializer.document(email_receivers=[email_entry(unexpected=1)]),
                     id="unknown-field"),
        pytest.param(initializer.document(email_receivers=[email_entry(domains=[])]),
                     id="no-domain"),
        pytest.param(initializer.document(email_receivers=[email_entry(domains=[
            {"id": str(uuid.UUID(int=index + 1)), "authority": "", "kind": "email_domain",
             "key": f"d{index}.example.com"} for index in range(17)
        ])]), id="seventeen-domains"),
        pytest.param(initializer.document(email_receivers=[email_entry(domains=[
            {"id": EMAIL_NAMESPACE, "authority": "", "kind": "email_domain",
             "key": "Example.com"},
        ])]), id="uppercase-domain"),
        pytest.param(initializer.document(email_receivers=[email_entry(domains=[
            {"id": EMAIL_NAMESPACE, "authority": "issuer", "kind": "email_domain",
             "key": DOMAIN},
        ])]), id="domain-authority"),
        pytest.param(initializer.document(email_receivers=[email_entry(domains=[
            {"id": EMAIL_NAMESPACE, "authority": "", "kind": "slack_workspace",
             "key": DOMAIN},
        ])]), id="domain-kind"),
        pytest.param(initializer.document(email_receivers=[email_entry(domains=[
            {"id": EMAIL_NAMESPACE, "authority": "", "kind": "email_domain", "key": DOMAIN},
            {"id": str(uuid.uuid4()), "authority": "", "kind": "email_domain", "key": DOMAIN},
        ])]), id="duplicate-domain"),
        pytest.param(initializer.document(email_receivers=[email_entry(
            binding_ids=[EMAIL_BINDING, EMAIL_BINDING],
        )]), id="duplicate-binding"),
    ],
)
def test_defective_email_receivers_refuse_as_documents(
    tmp_path: Path, document: dict[str, Any],
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: strict registrations; a file needs a receiver."""
    module = initializer.api()
    initializer.write(tmp_path, document)
    assert initializer.refused(module, tmp_path) == "invalid_document"


def test_conflicting_email_receivers_refuse_as_runtime(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: keys, domains and ids unique across the file."""
    module = initializer.api()
    slack = initializer.document()["receivers"][0]["registration"]
    second = email_entry(
        receiver_id=str(uuid.uuid4()), channel_identity_id=str(uuid.uuid4()),
        provider_installation_id=str(uuid.uuid4()), binding_ids=[str(uuid.uuid4())],
        domains=[{"id": str(uuid.uuid4()), "authority": "", "kind": "email_domain",
                  "key": "mail.example.com"}],
    )
    second["key"] = initializer.key(b"g" * 32)
    cases = {
        "slack-key": [{**email_entry(), "key": initializer.key(b"r" * 32)}],
        "context-key": [{**email_entry(), "key": initializer.key(b"c" * 32)}],
        "slack-receiver-id": [email_entry(receiver_id=slack["receiver_id"])],
        "slack-identity-id": [email_entry(channel_identity_id=slack["channel_identity_id"])],
        "slack-installation-id": [
            email_entry(provider_installation_id=slack["provider_installation_id"]),
        ],
        "slack-binding": [email_entry(binding_ids=[slack["binding_ids"][0]])],
        "slack-namespace-id": [email_entry(domains=[{
            "id": slack["namespace"]["id"], "authority": "", "kind": "email_domain",
            "key": DOMAIN,
        }])],
        "same-key": [email_entry(), {**second, "key": initializer.key(EMAIL_KEY)}],
        "same-receiver": [email_entry(), {**second, "registration": {
            **second["registration"], "receiver_id": EMAIL_RECEIVER, "generation": 2,
        }}],
        "same-domain": [email_entry(), {**second, "registration": {
            **second["registration"], "domains": email_entry()["registration"]["domains"],
        }}],
        "same-binding": [email_entry(), {**second, "registration": {
            **second["registration"], "binding_ids": [EMAIL_BINDING],
        }}],
        "same-identity": [email_entry(), {**second, "registration": {
            **second["registration"], "channel_identity_id": EMAIL_IDENTITY,
        }}],
    }
    for name, entries in cases.items():
        initializer.write(tmp_path, initializer.document(email_receivers=copy.deepcopy(entries)))
        assert initializer.refused(module, tmp_path) == "invalid_runtime", name
    initializer.write(tmp_path, initializer.document(email_receivers=[email_entry(), second]))
    assert type(module.load_http_identity_runtime(str(tmp_path))) is HttpInvocationRuntime


def test_email_key_equal_to_the_platform_key_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: the platform key never signs an email sender."""
    module = initializer.api()
    spelling = initializer.key(b"p" * 32)
    for platform_key in (spelling, "p" * 32):
        monkeypatch.setenv("API_KEY", platform_key)
        get_settings.cache_clear()
        try:
            initializer.write(tmp_path, initializer.document(
                email_receivers=[{**email_entry(), "key": spelling}],
            ))
            assert initializer.refused(module, tmp_path) == "invalid_runtime"
        finally:
            monkeypatch.delenv("API_KEY")
            get_settings.cache_clear()


def test_operator_file_email_receiver_serves_a_signed_turn(
    tmp_path: Path, clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: an email-only file admits a signed sender."""
    module = initializer.api()
    scope = _email_scope(synthetic_provider.url)
    configuration = scope["configuration"].model_dump(mode="json")
    document = initializer.document(
        providers=[{"configuration": configuration, "bearer": membership.BEARER}],
        receivers=[],
        context_keys=[{"key_id": "synthetic-context", "generation": 1,
                       "key": initializer.key(scope["context_key"])}],
        email_receivers=[email_entry(
            receiver_id=scope["receiver_id"], tenant_id=scope["tenant"],
            channel_identity_id=scope["identity"],
            provider_installation_id=scope["installation"],
            binding_ids=[scope["binding"]], domains=[scope["namespace"]],
        )],
    )
    initializer.write(tmp_path, document)
    runtime = module.load_http_identity_runtime(str(tmp_path))
    try:
        with TestClient(create_app(
            http_identity_runtime=runtime,
            http_identity_client=HttpIdentityClient(tls_context=synthetic_provider.tls),
        )) as client:
            token = _email_credential(client, scope)
            body = _body()
            accepted = _post(client, token, body, _proof(scope, body))
            assert accepted.status_code == 200, accepted.text
            assert valkey.xlen(runs_stream) == 1
            assert synthetic_provider.requests[0]["scope"]["sender"]["native_id"] == SENDER
    finally:
        _cleanup(valkey, scope)


# --- one runtime with trusted Slack partners and email receivers ----------------

PARTNER = "T0EXAMPLEPARTNER"


def _partner_entry() -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-24: an operator-declared partner workspace."""
    return {"id": str(uuid.uuid4()), "authority": "", "kind": "slack_workspace", "key": PARTNER}


def _combined_runtime(module: ModuleType, email: dict[str, Any], partner: dict[str, Any]) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-24/29: a Slack receiver trusting a partner beside email."""
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
        "binding_ids": (slack["binding"],), "human_native_ids": (membership.SENDER,),
        "trusted_namespaces": (partner,),
    })
    return module.build_http_invocation_runtime(
        providers,
        ((registration, slack["receiver_key"]),),
        (("synthetic-context", 1, slack["context_key"]),),
        signing_key=("synthetic-context", 1),
        email_receivers=((_registration(module, email), EMAIL_KEY),),
    )


def _queued(valkey: redis.Redis, stream: str, context_key: bytes) -> SimpleNamespace:
    """@spec HTTP-INVOCATION-EXECUTION-16: the newest queued turn and its sealed context."""
    fields = valkey.xrange(stream)[-1][1]
    ring = codec.build_identity_keyring("context", (("synthetic-context", 1, context_key),))
    context = codec.open_invocation_context(fields["curie_identity_context_v1"], ring)
    assert isinstance(context, codec.InvocationContextV1)
    return SimpleNamespace(
        capsule=fields["curie_identity_context_v1"], payload=fields["payload"], context=context,
    )


def test_trusted_partner_and_email_sender_share_one_runtime(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24/29: each source keeps its own capture rules.

    A trusted partner's Connect mention is captured in the partner namespace
    and an email sender in its domain namespace, through the same runtime, and
    the worker current check continues for both.
    """
    provider = synthetic_provider
    email = _email_scope(provider.url)
    slack = email["slack"]
    partner = _partner_entry()
    try:
        with TestClient(create_app(
            http_identity_runtime=_combined_runtime(_email_api(), email, partner),
            http_identity_client=HttpIdentityClient(tls_context=provider.tls),
        )) as client:
            mention = codec.ReceiverSlackEvidenceV1.model_validate({
                "delivery": "app_mention", "user_id": membership.SENDER,
                "event_user_team": PARTNER, "event_team": membership.ACCOUNT,
                "interaction_user_team_id": None, "is_ext_shared_channel": True,
                "enterprise_ids": (), "event_source_team": PARTNER,
            })
            token = membership._credential(client, slack)
            accepted = membership._post(
                client, token, *membership._submission(slack, slack=mention),
            )
            assert accepted.status_code == 200, accepted.text
            partnered = _queued(valkey, runs_stream, slack["context_key"])
            assert partnered.context.scope.provider == "slack"
            assert partnered.context.scope.namespace.model_dump() == partner

            body = _body()
            mailed = _post(client, _email_credential(client, email), body, _proof(email, body))
            assert mailed.status_code == 200, mailed.text
            emailed = _queued(valkey, runs_stream, slack["context_key"])
            assert emailed.context.scope.provider == "email"
            assert emailed.context.scope.namespace.model_dump() == email["namespace"]
            assert len(provider.requests) == 2

            for invocation in (partnered, emailed):
                check = authority.request_for(invocation)
                authority.assert_response(
                    authority.post_check(client, check), check,
                    decision="continue", reason="current_authority",
                )
            assert len(provider.requests) == 4
            assert [
                request["scope"]["sender"]["namespace"] for request in provider.requests[2:]
            ] == [partner, email["namespace"]]
    finally:
        _cleanup(valkey, email)


def test_email_domain_ids_differ_from_trusted_slack_namespace_ids(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-24/29: one namespace id names one namespace."""
    module = initializer.api()
    document = initializer.document(email_receivers=[email_entry()])
    slack = document["receivers"][0]["registration"]
    slack["trusted_namespaces"] = [{
        "id": str(uuid.uuid4()), "authority": "", "kind": "slack_workspace", "key": PARTNER,
    }]
    initializer.write(tmp_path, document)
    assert type(module.load_http_identity_runtime(str(tmp_path))) is HttpInvocationRuntime
    slack["trusted_namespaces"][0]["id"] = EMAIL_NAMESPACE
    initializer.write(tmp_path, document)
    assert initializer.refused(module, tmp_path) == "invalid_runtime"
