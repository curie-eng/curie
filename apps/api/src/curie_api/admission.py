"""Who may start a turn through a binding: the one admission check (ADR 0175).

Each binding may carry `allowed_callers`, a list of exact caller ids. This
module holds the ONE function that decides whether a caller is let in, and
every channel asks it: the channel port (`POST /channels/turns`) calls it
directly after verifying the adapter's token, and the Slack dispatcher, which
has no database, asks through `POST /channels/admission`. A second copy of the
rule anywhere else is the drift this module exists to prevent.

The check runs before anything happens: before a delivery is claimed, before a
turn is queued, before the Slack placeholder is posted. Inside the turn is too
late, because the turn is what reads the bot's credentials and the model is not
a security boundary.

A refused caller gets nothing back from the channel (decision 3). Operators see
the refusal as a log line naming the binding and the reason, never the message,
and as the `curie.turn.refused` counter.

When #2914 lands, its principal check becomes this function's body and each
entry becomes an identity link; the callers of `admit` stay as they are.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum

from .models import AgentChannel
from .schemas import normalize_caller_id


class AdmissionReason(StrEnum):
    """Why `admit` answered as it did, as a stable log token."""

    #: No binding answers the route. Admitted: the list lives on the binding,
    #: so with no binding there is no list, and routing decides what happens to
    #: the turn exactly as it did before ADR 0175.
    UNBOUND = "unbound"
    #: The binding carries no list (NULL): everyone may talk to the bot.
    OPEN = "open"
    #: One of the caller's ids is on the binding's list.
    LISTED = "listed"
    #: The binding carries a list and none of the caller's ids is on it.
    CALLER_NOT_ALLOWED = "caller_not_allowed"


@dataclass(frozen=True)
class AdmissionDecision:
    """The answer for one caller on one binding.

    Attributes:
        allowed: whether the caller may start a turn.
        restricted: whether the binding carries a list at all. The dispatcher
            caches an unrestricted route as open to everyone, so an install
            that never sets a list makes one admission call per route rather
            than one per message.
        reason: why, as a stable token for logs and tests.
    """

    allowed: bool
    restricted: bool
    reason: AdmissionReason


def admit(binding: AgentChannel | None, caller_ids: Iterable[str]) -> AdmissionDecision:
    """Decide whether a caller may start a turn through ``binding``.

    Pure: no I/O, no logging, so both entry points can call it inside their own
    request handling and log in their own words. Any one of the caller's ids
    matching an entry lets the caller in (a bot-sent Slack message is asked with
    both the sender and the bot id). Ids are compared in the binding kind's
    comparison form (`schemas.normalize_caller_id`), the same form the list is
    stored in.

    Args:
        binding: the binding row the route resolved to, or None when unbound.
        caller_ids: every id the channel reports for the caller. Blank ids are
            ignored, so a delivery with no sender cannot match anything.

    Returns:
        The decision, with whether the binding is restricted at all.
    """

    if binding is None:
        return AdmissionDecision(allowed=True, restricted=False, reason=AdmissionReason.UNBOUND)
    listed = binding.allowed_callers
    if listed is None:
        return AdmissionDecision(allowed=True, restricted=False, reason=AdmissionReason.OPEN)
    allowed = set(listed)
    for caller in caller_ids:
        if caller and normalize_caller_id(binding.kind, caller) in allowed:
            return AdmissionDecision(allowed=True, restricted=True, reason=AdmissionReason.LISTED)
    return AdmissionDecision(
        allowed=False, restricted=True, reason=AdmissionReason.CALLER_NOT_ALLOWED
    )
