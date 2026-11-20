"""Email receivers sharing trusted domains, and the mail adapter's signed post.

@spec HTTP-INVOCATION-EXECUTION-29 HTTP-INVOCATION-EXECUTION-30.
Several mailboxes trust the same sender domains under the same namespace ids,
so a person keeps one principal whichever mailbox they write to; each mailbox
keeps its own key and bindings. Synthetic ``example.test`` domains and the
shared ``example.com`` fixtures only.
"""

from __future__ import annotations

import copy
import hashlib
import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
import redis
from curie_api.config import get_settings
from curie_api.identity.http_invocation import HttpInvocationRuntime
from fastapi.testclient import TestClient

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


def _token(client: TestClient, address: str, adapter: str) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-29: a platform-key-minted channel token."""
    response = client.post(
        "/channels/token",
        headers={"X-API-Key": get_settings().api_key},
        json={"kind": "email", "address": address, "adapter": adapter, "ttl_s": 60},
    )
    assert response.status_code == 200, response.text
    return str(response.json()["token"])


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
