"""Route a reserved Slack voice delivery using its verified ingress receipt."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Final, TypeGuard

from curie_internal.slack_mention_receipt import (
    SlackMentionReceipt,
    voice_reply_text,
)

from .factory_steering import (
    ADMITTED,
    STEER_PATH,
    FactorySteeringClient,
    SteeringReply,
    SteeringUnavailable,
    _Turn,
)

VOICE_PATH: Final = "/v1/internal/factory-slack/voice"
_PENDING_STATUS: Final = "I could not verify the status of this voice feedback yet."
_STATUS: Final = {
    "pending": "Preparing your transcript. Review the draft in this thread when it arrives.",
    "draft": "Review the edited draft in this thread before confirming it.",
    "rejected": "Draft rejected.",
}
_CONFIRMED: Final = {
    "queued": "Your confirmed feedback is queued for this run.",
    "applied": "This run applied your confirmed feedback.",
}
_REFUSALS: Final = {
    "disabled": "Voice feedback is disabled on this installation.",
    "invalid_source": "This message could not be verified as voice feedback.",
    "source_changed": "The original clip changed. Submit the clip again to start a new draft.",
    "forbidden": "This account cannot give voice feedback on this run.",
    "invalid_media": "This clip could not be transcribed.",
    "media_limit": "This clip exceeds the voice feedback limits.",
    "no_speech": "No speech was recognized in the clip.",
    "not_found": "This draft could not be found.",
    "expired": "This draft expired.",
    "closed": "This draft is closed.",
    "stale_version": "Use the code from the latest version of this draft.",
    "invalid_text": "The voice feedback reply was invalid.",
    "target_mismatch": "The feedback names a different work item.",
    "admission_refused": "This run refused the confirmed feedback.",
}


class VoiceSteeringReply(SteeringReply):
    """A fixed voice status for the originating thread."""


def voice_refused(reason: str) -> VoiceSteeringReply:
    return VoiceSteeringReply(
        outcome="refused", text=_REFUSALS.get(reason, "This voice feedback request was refused.")
    )


def voice_status(answer: dict[str, Any]) -> VoiceSteeringReply:
    """Render only fixed status text. The API delivers transcripts directly to the origin."""
    outcome = answer.get("outcome")
    if outcome == "refused":
        reason = answer.get("reason")
        if isinstance(reason, str) and reason != "unavailable":
            return voice_refused(reason)
    elif outcome == "confirmed":
        disposition = answer.get("disposition")
        if isinstance(disposition, str) and disposition in _CONFIRMED:
            return VoiceSteeringReply(outcome=ADMITTED, text=_CONFIRMED[disposition])
    elif isinstance(outcome, str) and outcome in _STATUS:
        return VoiceSteeringReply(outcome=ADMITTED, text=_STATUS[outcome])
    raise SteeringUnavailable("factory voice status unavailable", pending_text=_PENDING_STATUS)


def matches_voice_receipt(
    receipt: SlackMentionReceipt | None, turn: _Turn
) -> TypeGuard[SlackMentionReceipt]:
    return (
        receipt is not None
        and FactorySteeringClient._kickoff_matches(receipt, turn)
        and receipt.message_ts != turn.root_ts
        and (
            receipt.version == 3
            or (
                receipt.version == 1
                and receipt.bot_user_id is not None
                and voice_reply_text(receipt.body, receipt.bot_user_id) is not None
            )
        )
    )


async def decide_voice_receipt(
    client: FactorySteeringClient,
    turn: _Turn,
    receipt: SlackMentionReceipt,
    *,
    deadline: float | None,
    record_intent: Callable[[], Awaitable[None]],
) -> VoiceSteeringReply:
    """Send a matched voice receipt and render its fixed status."""
    answer = await client._decide(
        turn,
        receipt,
        url=client._steer_url.removesuffix(STEER_PATH) + VOICE_PATH,
        deadline=deadline,
        record_intent=record_intent,
        pending_text=_PENDING_STATUS,
    )
    return voice_status(answer)
