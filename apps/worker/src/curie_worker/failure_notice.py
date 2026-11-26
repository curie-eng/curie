"""The plain sentence a person sees when a turn fails (#5984).

The kernel delivers a failed turn as a marked record: a first line
``curie-turn-failure: <class>`` (#3401), then the operator's account of the
failure, its guidance and the event id. Machine consumers need that record:
the cluster message relay, which only an API key holder reads, is how
``curie message`` and cluster eval tell a failed turn from a task reply.

A person in a chat channel cannot act on it. The reply sink therefore replaces
a marked record with one plain sentence, chosen by failure class, on every
route except the relay. The class, guidance and event id stay in the worker's
escalation log, the failed-turn telemetry and the completion outcome.
"""

from __future__ import annotations

import logging

from channel_protocol.reply import ReplyEvent, ReplyUpdate

logger = logging.getLogger(__name__)

# Frozen with tests/vectors/turn-failure-reply.json, like the kernel's
# TURN_FAILURE_REPLY_PREFIX. Kept apart from the kernel so the reply sink,
# which the kernel imports, does not import the kernel back.
_REPLY_PREFIX = "curie-turn-failure:"

TOO_LARGE = (
    "I couldn't finish this request. It was too large to complete in one go. "
    "Please try again, or split it into smaller parts."
)
TOO_LONG = (
    "I couldn't finish this request because it took too long. "
    "Please try again, or split it into smaller parts."
)
MAY_HAVE_ACTED = (
    "I couldn't finish this request. An earlier attempt may already have made "
    "changes, so I did not try again automatically. Please check the result "
    "before asking again."
)
APPROVAL_UNAVAILABLE = (
    "I couldn't finish this request because the approval it needs could not be "
    "set up. Please ask an administrator to check this agent's approval settings."
)
NOT_ALLOWED = (
    "I couldn't finish this request because it needed an action this conversation does not allow."
)
GENERIC = "I couldn't finish this request. Please try again in a few minutes."

#: Every sentence a failed turn can show a person. A consumer that only sees
#: the delivered text (the local stub) recognizes a failed turn by these.
PLAIN_FAILURE_REPLIES = (
    TOO_LARGE,
    TOO_LONG,
    MAY_HAVE_ACTED,
    APPROVAL_UNAVAILABLE,
    NOT_ALLOWED,
    GENERIC,
)

_BY_CLASS = {
    "budget-exceeded": TOO_LARGE,
    "max-turns": TOO_LARGE,
    "history-persistence-error": TOO_LARGE,
    "delivery-deadline": TOO_LONG,
    "runner-timeout": TOO_LONG,
    "runner-timeout-unconfirmed": TOO_LONG,
    "requester-window": TOO_LONG,
    "prior-side-effect": MAY_HAVE_ACTED,
    "tool-access-violation": NOT_ALLOWED,
}


def failure_class(text: str) -> str | None:
    """The class on a marked failure record, or None for any other text.

    The same rule as ``curie_worker.kernel.failures.failure_class_from_reply``:
    only a first line of exactly one token after the prefix counts.
    """

    if not text or not text.strip():
        return None
    line = text.lstrip("\n").splitlines()[0].strip()
    if not line.startswith(_REPLY_PREFIX):
        return None
    token = line[len(_REPLY_PREFIX) :].strip()
    if not token or any(char.isspace() for char in token):
        return None
    return token


def plain_failure_text(failure_class: str) -> str:
    """The sentence a person sees for a turn that failed with ``failure_class``."""

    if failure_class.startswith("approval-"):
        return APPROVAL_UNAVAILABLE
    return _BY_CLASS.get(failure_class, GENERIC)


def for_person(event: ReplyEvent) -> ReplyEvent:
    """``event`` with a marked failure record replaced by its plain sentence.

    Only an answer-text update is rewritten. A verbatim answer, a posted
    message, a settled card, a progress edit and every other event pass
    unchanged.
    """

    if not isinstance(event, ReplyUpdate) or event.text is None or event.text_mode is not None:
        return event
    token = failure_class(event.text)
    if token is None:
        return event
    logger.info(
        "replacing a failed turn's record with the plain notice for a person failure_class=%s",
        token,
    )
    return event.model_copy(update={"text": plain_failure_text(token)})
