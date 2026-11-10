"""Canonical protected transcript identity, @spec PROTECTED-HOOK-LANE-7."""

import hashlib
import json

from aci_protocol.turn import route_identity
from channel_protocol import scoped_conversation_id
from curie_protected_hooks.worker_verification import Dispatchable

from .binding import binding_adapter_for_handle


def protected_history_key(delivery: Dispatchable) -> str:
    """Keep actual reply coordinates while partitioning private durable history.

    This derives a key from an already verified delivery; it does not establish
    dispatch authority or mint a credential. The existing state binding claim
    scopes the canonical protected address, including runtime generation and
    the actual channel and identity. @spec PROTECTED-HOOK-LANE-7.
    """
    if type(delivery) is not Dispatchable or delivery.turn.reply_handle is None:
        raise ValueError("protected history requires a verified routed delivery")
    envelope = delivery.envelope.as_dict()
    handle = delivery.turn.reply_handle
    encoded = json.dumps(
        [
            "protected-history-v1",
            envelope["runtime_id"],
            envelope["runtime_generation"],
            handle.kind,
            handle.channel,
            route_identity(handle.kind, binding_adapter_for_handle(handle)),
        ],
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    return scoped_conversation_id(
        "@protected", hashlib.sha256(encoded).hexdigest(), envelope["execution_session_key"]
    )
