"""Pure ADR 0271 preparation verification, without approval-creation wiring."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import Any

from curie_internal.approver_ticket import ApprovalTicket, TicketError, verify_preparation
from nacl.signing import VerifyKey

from .action_forward import canonical_arguments


def verify_ticket(
    *,
    public_key: VerifyKey,
    key_id: str,
    arguments: dict[str, Any],
    clock: Callable[[], int],
) -> ApprovalTicket:
    """Check signed preparation facts using the production argument canonicalizer.

    The only ticket source is arguments["approval_ticket"].

    Route locking, registered key lookup, original-requester comparison, current
    identity/ceiling checks and atomic nonce spending belong to creation owners.
    This result alone never authorizes creation or execution. Delayed execution
    must not call this preparation-expiry check after approval acceptance.
    """
    try:
        if type(arguments) is not dict:
            raise TypeError
        execution = {key: value for key, value in arguments.items() if key != "approval_ticket"}
        digest = hashlib.sha256(canonical_arguments(execution).encode("utf-8")).hexdigest()
    except (ValueError, TypeError, RecursionError):
        raise TicketError("invalid_arguments") from None
    token = arguments.get("approval_ticket")
    if type(token) is not str:
        raise TicketError("invalid_ticket")
    return verify_preparation(
        token, public_key=public_key, key_id=key_id, now=clock(), arguments_sha256=digest
    )
