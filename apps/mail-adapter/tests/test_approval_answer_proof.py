"""The adapter signs an email approval answer proof when its receiver is configured.

@spec HTTP-APPROVAL-CONTINUITY-15. A verified reply is driven through the real
adapter, the fake AgentMail server behind `VerifyingTestProvider` and the fake
platform, which records the exact resolve bytes. The proof is checked with the
platform's own verifier against every value the API compares it with: the
approval in the path, the received bytes, the actor header and the binding.
Synthetic ``example.com`` values only.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from _support import (
    ALLOWED_SENDER,
    CHANNEL_TOKEN,
    IngressState,
    MailState,
    approval_card,
    completed,
    post_event,
    update,
)
from _verifying_provider import VerifyingTestProvider, verified
from curie_channel_gateway.mail.adapter import APPROVAL_REF_PATTERN, MailAdapter
from curie_channel_gateway.mail.config import MailAdapterConfig
from curie_internal import identity_approval
from curie_internal import identity_context as codec

PRINCIPAL = "adp.test-payload.test-signature"
APPROVER = "approver@example.com"
APPROVAL = "00000000-0000-4000-8000-0000000000c1"
RECEIVER = "00000000-0000-4000-8000-0000000000a1"
BINDING = "00000000-0000-4000-8000-0000000000b1"
KEY = bytes(range(32))
KEY_TEXT = base64.urlsafe_b64encode(KEY).rstrip(b"=").decode("ascii")
PROOF_HEADER = "X-Curie-Approval-Receiver-Proof"
HUMAN_HEADERS = {"From": ALLOWED_SENDER, "Message-ID": "<human@example.com>"}
APPROVER_HEADERS = {"From": APPROVER, "Message-ID": "<approver@example.com>"}


def _token(claims: dict[str, Any]) -> str:
    """A channel token in the platform's shape; only the platform verifies its signature."""
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"chn.{payload}.synthetic-signature"


BOUND_TOKEN = _token(
    {"channel_id": BINDING, "generation": 1, "scope": "channel.enqueue", "exp": 4102444800},
)
RECEIVER_SETTINGS = {"receiver_id": RECEIVER, "receiver_generation": 2, "receiver_key": KEY_TEXT}


@pytest.fixture
def make_verifying(
    make_config: Callable[..., MailAdapterConfig],
) -> Iterator[Callable[..., MailAdapter]]:
    built: list[MailAdapter] = []

    def _make(**overrides: object) -> MailAdapter:
        config = make_config(
            adapter_principal=PRINCIPAL, allowed_senders=(ALLOWED_SENDER, APPROVER), **overrides
        )
        instance = MailAdapter(config, provider=VerifyingTestProvider(config))
        built.append(instance)
        return instance

    yield _make
    for instance in built:
        instance.shutdown.set()
        instance.close()


def _answer(
    mail: MailState, adapter: MailAdapter, url: str, approval_id: str, word: str
) -> None:
    """A verified asking turn, its card, then the approver's verified reply."""
    mail.add_inbound(
        "msg-1", "thr-1", sender=ALLOWED_SENDER, text="Please send the quote",
        headers=HUMAN_HEADERS,
    )
    mail.verdicts["msg-1"] = verified("example.com")
    adapter.poll_once()
    assert post_event(url, update(f"Awaiting approval ({approval_id}): Send the quote"))[0] == 200
    assert post_event(url, approval_card(approval_id, approvers=[APPROVER]))[0] == 200
    assert post_event(url, completed("ev-1", outcome="awaiting-approval"))[0] == 200
    (request_email,) = mail.replies_to("msg-1")
    (reference,) = APPROVAL_REF_PATTERN.findall(request_email)
    mail.add_inbound(
        "msg-answer", "thr-1", sender=APPROVER,
        text=f"{word}\nChecked the numbers",
        full_text=f"{word}\nChecked the numbers\n\n> Approval reference: {reference}",
        headers=APPROVER_HEADERS, cc=[ALLOWED_SENDER],
    )
    mail.verdicts["msg-answer"] = verified("example.com")
    adapter.poll_once()


@pytest.mark.parametrize(("word", "decision"), [("APPROVE", "approved"), ("REJECT", "rejected")])
def test_a_configured_receiver_signs_the_exact_resolve_bytes_it_posts(
    mail: MailState,
    ingress: IngressState,
    make_verifying: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
    word: str,
    decision: str,
) -> None:
    """The proof binds purpose, receiver, binding, approval, sender and the bytes."""
    adapter = make_verifying(channel_token=BOUND_TOKEN, **RECEIVER_SETTINGS)
    _answer(mail, adapter, serve_egress(adapter) + "/", APPROVAL, word)

    ((path, headers, body),) = ingress.resolves
    (raw,) = ingress.resolve_raw_bodies
    assert path == f"/approvals/{APPROVAL}/resolve"
    assert body == {"decision": decision, "note": "Checked the numbers"}
    assert headers["X-Curie-Adapter-Principal"] == PRINCIPAL
    actor = headers["X-Curie-Approval-Actor"]
    assert actor == APPROVER
    ring = codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))
    proof = identity_approval.verify_email_approval_answer_proof(headers[PROOF_HEADER], ring)
    assert isinstance(proof, identity_approval.EmailApprovalAnswerProofV1)
    assert proof.model_dump() == {
        "schema_version": 1,
        "purpose": "approval_answer",
        "operation": "resolve",
        "receiver_id": RECEIVER,
        "receiver_generation": 2,
        "binding_id": BINDING,
        "approval_id": APPROVAL,
        "sender": actor.strip().lower(),
        "body_sha256": hashlib.sha256(raw).hexdigest(),
    }
    # Its own prefix: it is not a turn's proof, and another key refuses it.
    assert isinstance(codec.verify_email_receiver_proof(headers[PROOF_HEADER], ring),
                      codec.ContextRefusal)
    other = codec.build_identity_keyring("receiver", ((RECEIVER, 2, b"x" * 32),))
    assert isinstance(
        identity_approval.verify_email_approval_answer_proof(headers[PROOF_HEADER], other),
        codec.ContextRefusal,
    )
    assert adapter.state.delivery("msg-answer") == {"state": "answered", "turn": None}


def test_an_answer_the_signer_refuses_is_never_posted_unsigned(
    mail: MailState,
    ingress: IngressState,
    make_verifying: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
) -> None:
    """An approval id that is not the platform's UUID cannot be signed for."""
    adapter = make_verifying(channel_token=BOUND_TOKEN, **RECEIVER_SETTINGS)
    _answer(mail, adapter, serve_egress(adapter) + "/", "appr-1", "APPROVE")

    assert ingress.resolves == []
    (notice,) = mail.replies_to("msg-answer")
    assert "could not be accepted" in notice
    assert adapter.state.delivery("msg-answer") == {"state": "answered", "turn": None}


def test_without_a_receiver_the_answer_is_carried_as_before_and_logged(
    mail: MailState,
    ingress: IngressState,
    make_verifying: Callable[..., MailAdapter],
    serve_egress: Callable[[MailAdapter], str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Legacy installs see the resolve they always saw, and the log says it is unsigned."""
    adapter = make_verifying(channel_token=CHANNEL_TOKEN)
    with caplog.at_level(logging.INFO, logger="curie_channel_gateway.mail"):
        _answer(mail, adapter, serve_egress(adapter) + "/", APPROVAL, "APPROVE")

    ((path, headers, body),) = ingress.resolves
    (raw,) = ingress.resolve_raw_bodies
    assert path == f"/approvals/{APPROVAL}/resolve"
    assert headers.get(PROOF_HEADER) is None
    assert headers["X-Curie-Approval-Actor"] == APPROVER
    assert raw == json.dumps({"decision": "approved", "note": "Checked the numbers"}).encode()
    assert any(
        "without an email approval answer proof" in record.getMessage()
        for record in caplog.records
    )
    # The log names no mail content.
    assert not any(APPROVER in record.getMessage() for record in caplog.records)
