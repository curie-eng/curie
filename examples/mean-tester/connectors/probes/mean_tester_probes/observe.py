"""Turn one probe's Slack thread into what a person would have seen.

Pure functions over `conversations.replies` messages, so every rule the skill
judges by (ADR 0169 d5) is decided here, testably, and not left to the prompt.
"""

from dataclasses import asdict, dataclass

PROBE_MARK = "[mean test]"
APPROVAL_ACTION_PREFIX = "curie-approval-"
PLACEHOLDER_TEXTS = frozenset({
    "On it. Working on your request.",  # dispatcher placeholder_text default
    "Working on it...",  # worker booting_text default
})
PLATFORM_FAILURE_MARKERS = (
    "This agent is at capacity right now",
    "This agent does not have an active deployment yet",
    "No agent is configured for this ",
    "I ran into a problem and could not finish this request",
)


@dataclass(frozen=True)
class Observation:
    final: bool
    text: str | None
    approval_card: bool
    failure_marker: str | None
    replied_after_s: float | None

    def as_dict(self) -> dict:
        return asdict(self)


def _has_approval_card(message: dict) -> bool:
    for block in message.get("blocks") or []:
        if block.get("type") != "actions":
            continue
        for element in block.get("elements") or []:
            if str(element.get("action_id", "")).startswith(APPROVAL_ACTION_PREFIX):
                return True
    return False


def observe(
    messages: list[dict], target_user: str, probe_ts: str, now: float, settle_s: float
) -> Observation:
    replies = [m for m in messages if m.get("user") == target_user and m.get("ts") != probe_ts]
    card = any(_has_approval_card(m) for m in replies)
    answers = [m for m in replies if (m.get("text") or "").strip() not in PLACEHOLDER_TEXTS]
    if not answers:
        return Observation(False, None, card, None, None)
    last = answers[-1]
    text = last.get("text") or ""
    changed_at = float((last.get("edited") or {}).get("ts") or last["ts"])
    marker = next((m for m in PLATFORM_FAILURE_MARKERS if m in text), None)
    return Observation(
        final=now - changed_at >= settle_s,
        text=text,
        approval_card=card,
        failure_marker=marker,
        replied_after_s=round(float(answers[0]["ts"]) - float(probe_ts), 3),
    )
