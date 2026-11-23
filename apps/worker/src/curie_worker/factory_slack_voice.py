"""Route a reserved Slack voice delivery using its verified ingress receipt."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Final

from curie_internal.slack_mention_receipt import (
    MalformedMentionReceiptError,
    SlackMentionReceipt,
    read_receipt,
)
from redis.exceptions import RedisError

from .factory_steering import (
    _ACTION_EVENT_PREFIX,
    ADMITTED,
    STEER_PATH,
    FactorySteeringClient,
    MappedRoot,
    SteeringReply,
    SteeringUnavailable,
    _http_selected,
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
    "unconfigured": "Voice feedback is unavailable on this worker.",
    "budget_expired": "This voice feedback request ran out of time.",
    "not_mapped": "Voice feedback needs a mapped factory run thread.",
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


def _refused(reason: str) -> SteeringReply:
    return SteeringReply(
        outcome="refused", text=_REFUSALS.get(reason, "This voice feedback request was refused.")
    )


def voice_status(answer: dict[str, Any]) -> SteeringReply:
    """Render only fixed status text. The API delivers transcripts directly to the origin."""
    outcome = answer.get("outcome")
    if outcome == "refused":
        reason = answer.get("reason")
        if isinstance(reason, str) and reason != "unavailable":
            return _refused(reason)
    elif outcome == "confirmed":
        disposition = answer.get("disposition")
        if isinstance(disposition, str) and disposition in _CONFIRMED:
            return SteeringReply(outcome=ADMITTED, text=_CONFIRMED[disposition])
    elif isinstance(outcome, str) and outcome in _STATUS:
        return SteeringReply(outcome=ADMITTED, text=_STATUS[outcome])
    raise SteeringUnavailable("factory voice status unavailable", pending_text=_PENDING_STATUS)


async def decide_voice(
    client: FactorySteeringClient,
    turn: _Turn,
    *,
    mapping: MappedRoot | None,
    deadline: float | None,
    record_intent: Callable[[], Awaitable[None]],
) -> SteeringReply:
    if mapping is None:
        return _refused("not_mapped")
    event = turn.qevent
    if (
        mapping.kickoff_replay
        or _http_selected()
        or event.event_id.startswith(_ACTION_EVENT_PREFIX)
        or event.attachments
    ):
        return _refused("invalid_source")
    refusal = client._precheck(event, deadline, "budget_expired", "unconfigured")
    if refusal is not None:
        return _refused(refusal)
    try:
        receipt = await read_receipt(client._redis, event.event_id)
    except MalformedMentionReceiptError:
        return _refused("invalid_source")
    except RedisError as exc:
        raise SteeringUnavailable("factory voice receipt unavailable") from exc
    return await decide_voice_receipt(
        client, turn, receipt, deadline=deadline, record_intent=record_intent
    )


async def decide_voice_receipt(
    client: FactorySteeringClient,
    turn: _Turn,
    receipt: SlackMentionReceipt | None,
    *,
    deadline: float | None,
    record_intent: Callable[[], Awaitable[None]],
) -> SteeringReply:
    """Send the stored human receipt if it matches this queued turn and reply with its status."""
    if (
        receipt is None
        or not client._kickoff_matches(receipt, turn)
        or receipt.message_ts == turn.root_ts
        or not (receipt.version == 3 or (receipt.version == 1 and receipt.bot_user_id is not None))
    ):
        return _refused("invalid_source")

    answer = await client._decide(
        turn,
        receipt,
        url=client._steer_url.removesuffix(STEER_PATH) + VOICE_PATH,
        deadline=deadline,
        record_intent=record_intent,
        pending_text=_PENDING_STATUS,
    )
    return voice_status(answer)
