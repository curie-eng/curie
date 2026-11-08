"""Optional signed ``channel_type`` keeps every mention and answer proof byte-identical.

@spec HTTP-INVOCATION-EXECUTION-26. Synthetic ids only.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any

from curie_internal import identity_approval as approval_wire
from curie_internal import identity_context as codec

KEY = bytes(range(32))
RECEIVER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
UUID = "11111111-1111-4111-8111-111111111111"
PREFIX = "curie-receiver-v1"
EVIDENCE: dict[str, Any] = {
    "delivery": "app_mention", "user_id": "U0EXAMPLEPERSON", "event_user_team": None,
    "event_team": "T0EXAMPLEHOME", "interaction_user_team_id": None,
    "is_ext_shared_channel": False, "enterprise_ids": (),
}
DIRECT: dict[str, Any] = {
    "delivery": "message", "event_user_team": "T0EXAMPLEHOME", "event_team": "T0EXAMPLEHOME",
    "is_ext_shared_channel": False,
}


def proof(**slack: Any) -> dict[str, Any]:
    """@spec HTTP-INVOCATION-EXECUTION-26."""
    return {
        "schema_version": 1, "receiver_id": RECEIVER, "receiver_generation": 2,
        "channel_identity_id": UUID, "provider_installation_id": UUID,
        "binding_id": UUID, "binding_generation": 7, "delivery_id": "event-1",
        "native_channel_id": "D0EXAMPLEDIRECT", "conversation_id": "conversation-1",
        "body_sha256": hashlib.sha256(b"synthetic submitted bytes").hexdigest(),
        "slack": {**EVIDENCE, **slack},
    }


def ring() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-26."""
    return codec.build_identity_keyring("receiver", ((RECEIVER, 2, KEY),))


def token_for(wire: dict[str, Any]) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-26: independent standard-library signing oracle."""
    raw = json.dumps(wire, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    segment = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    message = (PREFIX + "." + segment).encode("ascii")
    mac = base64.urlsafe_b64encode(hmac.digest(KEY, message, "sha256")).rstrip(b"=").decode()
    return PREFIX + "." + segment + "." + mac


def test_mention_without_channel_type_signs_exactly_the_existing_bytes() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: no key, not even a null, on a mention."""
    value = codec.ReceiverProofV1.model_validate(proof())
    assert value.slack.channel_type is None
    assert "channel_type" not in value.model_dump(mode="json")["slack"]
    token = codec.sign_receiver_proof(value, ring())
    assert token == token_for(proof())
    with_source = proof(event_source_team="T0EXAMPLEPARTNER")
    assert codec.sign_receiver_proof(
        codec.ReceiverProofV1.model_validate(with_source), ring(),
    ) == token_for(with_source)


def test_direct_message_channel_type_is_signed_and_verified() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26."""
    wire = proof(**DIRECT, channel_type="im")
    value = codec.ReceiverProofV1.model_validate(wire)
    token = codec.sign_receiver_proof(value, ring())
    assert token == token_for(wire)
    verified = codec.verify_receiver_proof(token, ring())
    assert isinstance(verified, codec.ReceiverProofV1)
    assert verified.slack.channel_type == "im"
    assert verified.slack.event_source_team is None


def test_explicit_null_channel_type_is_not_canonical() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: a valid MAC cannot rescue a noncanonical null."""
    refused = codec.verify_receiver_proof(token_for(proof(channel_type=None)), ring())
    assert isinstance(refused, codec.ContextRefusal)


def test_answer_proof_evidence_omits_the_absent_channel_type() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-26: interactions carry no conversation type."""
    value = approval_wire.ApprovalAnswerProofV1.model_validate({
        "schema_version": 1, "purpose": "approval_answer", "operation": "resolve",
        "receiver_id": RECEIVER, "receiver_generation": 2, "channel_identity_id": UUID,
        "provider_installation_id": UUID, "approval_id": UUID,
        "body_sha256": "0" * 64, "attestation_sha256": "0" * 64, "native_channel_id": "CExample",
        "slack": {**EVIDENCE, "delivery": "block_actions", "event_user_team": None,
                  "event_team": None, "interaction_user_team_id": "T0EXAMPLEHOME",
                  "is_ext_shared_channel": None},
    })
    assert "channel_type" not in value.model_dump(mode="json")["slack"]
