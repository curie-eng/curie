"""Email receivers sharing trusted domains, and the mail adapter's signed post.

@spec HTTP-INVOCATION-EXECUTION-29 HTTP-INVOCATION-EXECUTION-30.
Several mailboxes trust the same sender domains under the same namespace ids,
so a person keeps one principal whichever mailbox they write to; each mailbox
keeps its own key and bindings. Synthetic ``example.test`` domains and the
shared ``example.com`` fixtures only.
"""

from __future__ import annotations

import base64
import copy
import hashlib
import json
import threading
import uuid
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.identity.http_credential_registry import build_provider_credential_registry
from curie_api.identity.http_invocation import HttpInvocationRuntime
from curie_api.identity.http_transport import HttpIdentityClient, ProviderBearer
from curie_api.main import create_app
from curie_api.models import AgentChannel
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.config import MailAdapterConfig
from fastapi.testclient import TestClient
from role_policy_support import run
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from apps.api.tests import test_http_email_receivers as email
from apps.api.tests import test_http_identity_initializer as initializer
from apps.api.tests import test_http_invocation_membership as membership
from apps.api.tests.test_http_invocation_membership import synthetic_provider as synthetic_provider

CORP = {
    "id": "0c000000-0000-4000-8000-000000000001", "authority": "", "kind": "email_domain",
    "key": "corp.example.test",
}
PARTNER = {
    "id": "0c000000-0000-4000-8000-000000000002", "authority": "", "kind": "email_domain",
    "key": "partner.example.test",
}


def _mailbox(index: int, domains: list[dict[str, Any]]) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-29: one mailbox with its own ids, binding and key."""
    entry = email.email_entry(
        receiver_id=f"0a00000{index}-0000-4000-8000-000000000000",
        channel_identity_id=f"0b00000{index}-0000-4000-8000-000000000000",
        provider_installation_id=f"0d00000{index}-0000-4000-8000-000000000000",
        binding_ids=[f"0e00000{index}-0000-4000-8000-000000000000"],
        domains=copy.deepcopy(domains),
    )
    entry["key"] = initializer.key(bytes([0x40 + index]) * 32)
    return entry


def _six_mailboxes() -> list[dict[str, Any]]:
    """Four mailboxes trust both domains, one only corp, one only partner."""
    return [
        *(_mailbox(index, [CORP, PARTNER]) for index in range(4)),
        _mailbox(4, [CORP]),
        _mailbox(5, [PARTNER]),
    ]


def test_six_email_receivers_sharing_two_domains_load(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: one namespace id per domain, shared by mailboxes."""
    module = initializer.api()
    for slack in (True, False):
        document = initializer.document(email_receivers=_six_mailboxes())
        if not slack:
            document["receivers"] = []
        initializer.write(tmp_path, document)
        assert type(module.load_http_identity_runtime(str(tmp_path))) is HttpInvocationRuntime


def test_a_contradictory_domain_mapping_refuses(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: one domain, one namespace id, everywhere."""
    module = initializer.api()
    slack = initializer.document()["receivers"][0]["registration"]
    cases = {
        "domain-under-two-ids": {**CORP, "id": "0c000000-0000-4000-8000-000000000003"},
        "id-for-two-domains": {**CORP, "key": "other.example.test"},
        "slack-namespace-id": {**CORP, "id": slack["namespace"]["id"]},
    }
    for name, contradiction in cases.items():
        mailboxes = _six_mailboxes()
        mailboxes[5] = _mailbox(5, [PARTNER, contradiction])
        initializer.write(tmp_path, initializer.document(email_receivers=mailboxes))
        assert initializer.refused(module, tmp_path) == "invalid_runtime", name
    # Every per-mailbox rule still holds with the domains shared.
    per_mailbox = {
        "same-receiver": {"receiver_id": "0a000000-0000-4000-8000-000000000000"},
        "same-identity": {"channel_identity_id": "0b000000-0000-4000-8000-000000000000"},
        "same-installation": {
            "provider_installation_id": "0d000000-0000-4000-8000-000000000000",
        },
        "same-binding": {"binding_ids": ["0e000000-0000-4000-8000-000000000000"]},
    }
    for name, change in per_mailbox.items():
        mailboxes = _six_mailboxes()
        mailboxes[5]["registration"].update(change)
        initializer.write(tmp_path, initializer.document(email_receivers=mailboxes))
        assert initializer.refused(module, tmp_path) == "invalid_runtime", name
    mailboxes = _six_mailboxes()
    mailboxes[5]["key"] = mailboxes[0]["key"]
    initializer.write(tmp_path, initializer.document(email_receivers=mailboxes))
    assert initializer.refused(module, tmp_path) == "invalid_runtime", "same-key"


# --- admission with a shared domain -------------------------------------------------

SECOND_INBOX = "acme-second@example.com"
SECOND_ADAPTER = "acme-mail-second"
SECOND_KEY = b"s" * 32


def _second_mailbox(scope: dict[str, Any]) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-29: another mailbox of the same agent, same domain."""
    second: dict[str, Any] = {
        "receiver_id": str(uuid.uuid4()),
        "identity": str(uuid.uuid4()),
        "installation": str(uuid.uuid4()),
    }

    async def persist(factory: async_sessionmaker[AsyncSession]) -> None:
        async with factory() as session, session.begin():
            binding = AgentChannel(
                tenant_id=uuid.UUID(scope["tenant"]),
                agent_id=uuid.UUID(scope["agent"]),
                kind="email",
                address=SECOND_INBOX,
                adapter=SECOND_ADAPTER,
                endpoint="https://mail.example.com/second-reply",
                allowed_callers=None,
            )
            session.add(binding)
            await session.flush()
            second["binding"] = str(binding.id)

    run(persist)
    return {**scope, **second}


def _shared_runtime(scope: dict[str, Any], second: dict[str, Any]) -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-29: two mailboxes, one shared domain namespace."""
    module = email._email_api()
    providers = build_provider_credential_registry(
        ((scope["configuration"], ProviderBearer(membership.BEARER)),)
    )
    return module.build_http_invocation_runtime(
        providers,
        (),
        (("synthetic-context", 1, scope["context_key"]),),
        signing_key=("synthetic-context", 1),
        email_receivers=(
            (email._registration(module, scope), email.EMAIL_KEY),
            (email._registration(
                module, scope, receiver_id=second["receiver_id"],
                channel_identity_id=second["identity"],
                provider_installation_id=second["installation"],
                binding_ids=[second["binding"]],
            ), SECOND_KEY),
        ),
    )


def _token(client: TestClient, address: str, adapter: str) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-29: a platform-key-minted channel token."""
    response = client.post(
        "/channels/token",
        headers={"X-API-Key": get_settings().api_key},
        json={"kind": "email", "address": address, "adapter": adapter, "ttl_s": 60},
    )
    assert response.status_code == 200, response.text
    return str(response.json()["token"])


def _second_body() -> bytes:
    """@spec HTTP-INVOCATION-EXECUTION-29: a turn the second mailbox received."""
    return json.dumps(
        {**json.loads(email._body()), "address": SECOND_INBOX}, separators=(",", ":"),
    ).encode()


def _signed(owner: dict[str, Any], key: bytes, binding: str, body: bytes) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-29: a proof under one mailbox's key."""
    codec = email._email_codec()
    parsed = json.loads(body)
    value = codec.EmailReceiverProofV1(
        schema_version=1, receiver_id=owner["receiver_id"], receiver_generation=1,
        binding_id=binding, sender=parsed["author"].lower(),
        delivery_id=parsed["delivery_id"],
        body_sha256=hashlib.sha256(body).hexdigest(),
    )
    ring = codec.build_identity_keyring("receiver", ((owner["receiver_id"], 1, key),))
    return str(codec.sign_email_receiver_proof(value, ring))


def _cleanup(valkey: redis.Redis, *bindings: str) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-12: remove only this test's delivery state."""
    for binding in bindings:
        keys = membership._keys(valkey, binding)
        if keys:
            valkey.delete(*keys)


def test_a_shared_domain_never_lets_one_mailbox_speak_for_another(
    clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-29: the proof stays bound to its receiver and binding."""
    scope = email._email_scope(synthetic_provider.url)
    second = _second_mailbox(scope)
    try:
        with TestClient(create_app(
            http_identity_runtime=_shared_runtime(scope, second),
            http_identity_client=HttpIdentityClient(tls_context=synthetic_provider.tls),
        )) as client:
            first_token = _token(client, email.INBOX, email.MAIL_ADAPTER)
            second_token = _token(client, SECOND_INBOX, SECOND_ADAPTER)
            body = _second_body()
            crossed = {
                # The first mailbox's key naming the second mailbox's binding.
                "foreign-binding": (second_token, _signed(
                    scope, email.EMAIL_KEY, second["binding"], body,
                )),
                # The first mailbox's own proof under the second mailbox's token.
                "foreign-token": (second_token, _signed(
                    scope, email.EMAIL_KEY, scope["binding"], body,
                )),
                # The second mailbox's id with the first mailbox's key.
                "foreign-key": (second_token, _signed(
                    second, email.EMAIL_KEY, second["binding"], body,
                )),
            }
            for name, (token, proof) in crossed.items():
                response = email._post(client, token, body, proof)
                assert response.status_code == 401, (name, response.text)
                assert response.json() == {"detail": "invalid_http_receiver"}, name
            assert synthetic_provider.requests == []
            assert valkey.xlen(runs_stream) == 0

            mine = email._body()
            first = email._post(
                client, first_token, mine,
                _signed(scope, email.EMAIL_KEY, scope["binding"], mine),
            )
            assert first.status_code == 200, first.text
            again = email._post(
                client, second_token, body,
                _signed(second, SECOND_KEY, second["binding"], body),
            )
            assert again.status_code == 200, again.text
            assert valkey.xlen(runs_stream) == 2
            senders = [request["scope"]["sender"] for request in synthetic_provider.requests]
            assert [sender["namespace"] for sender in senders] == [scope["namespace"]] * 2
            assert {sender["native_id"] for sender in senders} == {email.SENDER}
    finally:
        _cleanup(valkey, scope["binding"], second["binding"], scope["slack_binding"])


# --- the mail adapter's own signed post ---------------------------------------------


class _Relay(BaseHTTPRequestHandler):
    """Records the adapter's exact request and acknowledges it, like an ingress would."""

    protocol_version = "HTTP/1.0"

    def do_POST(self) -> None:
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.server.captured.append((self.path, dict(self.headers.items()), raw))  # type: ignore[attr-defined]
        payload = json.dumps({"event_id": "relay", "stream_id": None, "duplicate": False})
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload.encode())

    def log_message(self, *_args: Any) -> None:
        return None


@pytest.fixture
def relay() -> Iterator[Any]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Relay)
    server.captured = []  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()


def test_the_mail_adapters_signed_post_is_admitted_by_the_real_route(
    tmp_path: Path, clean_db: None, valkey: redis.Redis, runs_stream: str,
    synthetic_provider: SimpleNamespace, relay: Any,
) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-30: the adapter's exact request, byte for byte."""
    scope = email._email_scope(synthetic_provider.url)
    try:
        with email._client(synthetic_provider, scope) as client:
            token = email._email_credential(client, scope)
            config = MailAdapterConfig(
                agentmail_api_key="am_synthetic",
                agentmail_inbox=email.INBOX,
                api_base_url=f"http://127.0.0.1:{relay.server_address[1]}",
                channel_token=token,
                egress_secret="synthetic-egress",
                allowed_senders=("example.com",),
                state_path=str(tmp_path / "mail-state.sqlite3"),
                ingress_attempts=1,
                receiver_id=scope["receiver_id"],
                receiver_generation=1,
                receiver_key=base64.urlsafe_b64encode(email.EMAIL_KEY).rstrip(b"=").decode(),
            )
            adapter = MailAdapter(config)
            try:
                message_id = "<synthetic-" + uuid.uuid4().hex + "@example.com>"
                assert adapter.post_turn({
                    "kind": "email",
                    "address": adapter.inbox_address,
                    "delivery_id": message_id,
                    "conversation_id": "synthetic-thread",
                    "author": email.SENDER,
                    "text": "Synthetic subject\n\nSynthetic email invocation",
                    "reply_ref": message_id,
                }) == "accepted"
            finally:
                adapter.close()
            ((path, headers, raw),) = relay.captured
            assert path == "/channels/turns"
            response = client.post(path, content=raw, headers={
                "Content-Type": headers["Content-Type"],
                "X-API-Key": headers["X-Api-Key"],
                "X-Curie-Receiver-Proof": headers["X-Curie-Receiver-Proof"],
            })
            assert response.status_code == 200, response.text
            assert response.json()["duplicate"] is False
            assert valkey.xlen(runs_stream) == 1
            sender = synthetic_provider.requests[0]["scope"]["sender"]
            assert sender["native_id"] == email.SENDER
            assert sender["namespace"] == scope["namespace"]
    finally:
        _cleanup(valkey, scope["binding"], scope["slack_binding"])
