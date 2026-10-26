"""Invocation admission (ADRs 0175/0220; COMMON-ADMISSION-EXTRACTION-1)."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from sqlalchemy.ext.asyncio import AsyncSession

from curie_api.schemas.channels import normalize_caller_id

from . import team_authorization
from .identity.slack import SlackEvidence
from .models import AgentChannel
from .team_authorization import TeamDecision


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
    comparison form (`schemas.channels.normalize_caller_id`), the same form the list is
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


@dataclass(frozen=True)
class InvocationDecision:
    """@spec COMMON-ADMISSION-EXTRACTION-1: facts retained for transport handling."""

    allowed: bool
    restricted: bool
    list_decision: AdmissionDecision
    team_decision: TeamDecision | None


async def evaluate_invocation(
    session: AsyncSession,
    binding: AgentChannel | None,
    caller_ids: Sequence[str],
    *,
    surface: Literal["channel-port", "admission"],
    slack: SlackEvidence | None = None,
    email: str | None = None,
    on_list_refusal: Callable[[AdmissionDecision], None] | None = None,
) -> InvocationDecision:
    """Compose existing invocation checks.

    @spec COMMON-ADMISSION-EXTRACTION-1
    @spec COMMON-ADMISSION-EXTRACTION-2
    @spec COMMON-ADMISSION-EXTRACTION-3

    The synchronous observer lets each transport retain its refusal observation
    before team I/O, including when that I/O fails.
    """
    decision = admit(binding, caller_ids)
    if not decision.allowed:
        if on_list_refusal is not None:
            on_list_refusal(decision)
        if surface == "channel-port":
            return InvocationDecision(False, decision.restricted, decision, None)
    if binding is None:
        return InvocationDecision(decision.allowed, decision.restricted, decision, None)

    human = await team_authorization.check_binding(
        session, binding, role="invocation", slack=slack, email=email
    )
    allowed = decision.allowed and human.allowed
    if surface == "admission" and human.restricted and slack is not None:
        allowed = allowed and slack.user_id in caller_ids
    return InvocationDecision(
        allowed, decision.restricted or human.restricted, decision, human
    )
