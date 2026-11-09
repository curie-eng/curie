"""The adapter signs an email receiver proof only when its receiver is configured.

@spec HTTP-INVOCATION-EXECUTION-30. The platform ingress is the shared fake
server, which records the exact bytes it received. The proof is checked with
the platform's own verifier, so the adapter cannot pass by agreeing with a
second canonicalisation of its own. Synthetic ``example.test`` values only.
"""

from __future__ import annotations

import base64
import hashlib
import json
from collections.abc import Callable
from typing import Any

import pytest
from _support import ALLOWED_SENDER, CHANNEL_TOKEN, INBOX, IngressState
from curie_channel_gateway.mail.adapter import MailAdapter
from curie_channel_gateway.mail.config import MailAdapterConfig
from curie_internal import identity_context as codec
from pydantic import ValidationError

RECEIVER = "00000000-0000-4000-8000-0000000000a1"
BINDING = "00000000-0000-4000-8000-0000000000b1"
KEY = bytes(range(32))
KEY_TEXT = base64.urlsafe_b64encode(KEY).rstrip(b"=").decode("ascii")
PROOF_HEADER = "X-Curie-Receiver-Proof"


def _token(claims: dict[str, Any]) -> str:
    """A channel token in the platform's shape; only the platform verifies its signature."""
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"chn.{payload}.synthetic-signature"


BOUND_TOKEN = _token(
    {"channel_id": BINDING, "generation": 1, "scope": "channel.enqueue", "exp": 4102444800},
)


def _turn(message_id: str = "<synthetic-1@mail.example.test>") -> dict[str, str]:
    """The turn the intake path builds, with the sender already lowercased."""
    return {
        "kind": "email",
        "address": INBOX,
        "delivery_id": message_id,
        "conversation_id": "synthetic-thread",
        "author": ALLOWED_SENDER,
        "text": "Quarterly plan\n\nplease summarize",
        "reply_ref": message_id,
    }


def _receiver(**changes: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "receiver_id": RECEIVER, "receiver_generation": 2, "receiver_key": KEY_TEXT,
    }
    value.update(changes)
    return value


def test_without_receiver_settings_the_post_is_unchanged(
    ingress: IngressState, adapter: MailAdapter,
) -> None:
    """Legacy bindings see the request they always saw: no proof, the same bytes."""
    turn = _turn()
    assert adapter.post_turn(turn) == "accepted"
    ((headers, body),) = ingress.requests
    assert headers.get(PROOF_HEADER) is None
    assert headers["X-API-Key"] == CHANNEL_TOKEN
    assert body == turn
    assert ingress.raw_bodies == [json.dumps(turn).encode()]


@pytest.mark.parametrize(
    "settings",
    [
        pytest.param({"receiver_id": RECEIVER}, id="id-only"),
        pytest.param({"receiver_key": KEY_TEXT}, id="key-only"),
        pytest.param(_receiver(receiver_generation=None), id="no-generation"),
        pytest.param(_receiver(receiver_key=""), id="no-key"),
        pytest.param(_receiver(receiver_id=""), id="no-id"),
        pytest.param(_receiver(receiver_id="not-a-uuid"), id="bad-id"),
        pytest.param(_receiver(receiver_id=RECEIVER.upper()), id="uppercase-id"),
        pytest.param(_receiver(receiver_generation=0), id="zero-generation"),
        pytest.param(_receiver(receiver_key=KEY_TEXT + "="), id="padded-key"),
        pytest.param(_receiver(receiver_key=KEY_TEXT[:-1]), id="short-key"),
    ],
)
def test_partial_or_invalid_receiver_settings_refuse_startup(
    make_config: Callable[..., MailAdapterConfig], settings: dict[str, Any],
) -> None:
    """All three or none; the refusal never echoes the supplied key."""
    with pytest.raises(ValidationError) as caught:
        make_config(**settings)
    rendered = str(caught.value) + repr(caught.value)
    assert not any(KEY_TEXT[start:start + 8] in rendered for start in range(len(KEY_TEXT) - 7))


def test_the_receiver_key_stays_out_of_the_configuration_repr(
    make_config: Callable[..., MailAdapterConfig],
) -> None:
    config = make_config(**_receiver())
    assert KEY_TEXT not in repr(config) and KEY_TEXT not in str(config)


def test_a_configured_receiver_signs_the_exact_bytes_it_posts(
    ingress: IngressState, make_adapter: Callable[..., MailAdapter],
) -> None:
    """The proof binds receiver, binding, sender, delivery and the bytes on the wire."""
    adapter = make_adapter(channel_token=BOUND_TOKEN, **_receiver())
    try:
        turn = _turn()
        assert adapter.post_turn(turn) == "accepted"
        # The same turn read back from the journal in another key order.
        assert adapter.post_turn(dict(reversed(list(turn.items())))) == "accepted"
    finally:
        adapter.close()
    first, second = ingress.raw_bodies
    assert first == second
    assert json.loads(first) == turn
    (first_headers, _), (second_headers, _) = ingress.requests
    assert first_headers["X-API-Key"] == BOUND_TOKEN
    token = first_headers[PROOF_HEADER]
    assert token == second_headers[PROOF_HEADER]
    ring = codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))
    proof = codec.verify_email_receiver_proof(token, ring)
    assert isinstance(proof, codec.EmailReceiverProofV1)
    assert proof.model_dump() == {
        "schema_version": 1,
        "receiver_id": RECEIVER,
        "receiver_generation": 2,
        "binding_id": BINDING,
        "sender": ALLOWED_SENDER,
        "delivery_id": turn["delivery_id"],
        "body_sha256": hashlib.sha256(first).hexdigest(),
    }
    other = codec.build_identity_keyring("receiver", ((RECEIVER, 2, b"x" * 32),))
    assert isinstance(codec.verify_email_receiver_proof(token, other), codec.ContextRefusal)


def test_a_turn_the_signer_refuses_is_never_posted_unsigned(
    ingress: IngressState, make_adapter: Callable[..., MailAdapter],
) -> None:
    """A token without a binding claim cannot be signed for; the turn is settled."""
    adapter = make_adapter(channel_token=CHANNEL_TOKEN, **_receiver())
    try:
        assert adapter.post_turn(_turn()) == "refused"
    finally:
        adapter.close()
    assert ingress.requests == [] and ingress.attempts == 0
