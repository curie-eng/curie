"""The adapter itself: the poll path in, the reply path out, and the state between.

Two halves, and neither one knows anything about Slack:

- **Ingress.** ``poll_loop`` lists an AgentMail inbox and POSTs each new message
  to the platform's channel ingress (``POST /channels/turns``) under the scoped
  channel token, with the AgentMail ``message_id`` as the ``delivery_id`` so a
  retry is idempotent.
- **Egress.** ``send_reply`` turns one ``turn.completed`` into one threaded
  AgentMail reply. Completion delivery is at-least-once, so the send is deduped
  on ``event_id`` twice: a bounded in-memory map (the fast path) and a marker
  line in the thread itself (survives a restart).

All routing state lives on the instance, guarded by one lock, and all of it is
process-local: this service runs as a single replica.

The inbound gate is two checks, in this order, and they are not equivalent:

1. ``provider_authenticated`` rejects the provider's own verdict labels. This is
   defense in depth behind the provider's filtering (AgentMail drops mail whose
   authentication headers are present and explicitly fail, and excludes these
   categories from list results by default), and in a correct install it never
   fires.
2. ``sender_allowed`` filters an attacker-controlled ``From`` header. It is a
   filter and it authenticates nobody. See README.md for what that does and does
   not buy an operator.
"""

from __future__ import annotations

import email.utils
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Iterable
from typing import Any

from .agentmail import AgentMailClient, request
from .config import MailAdapterConfig

logger = logging.getLogger(__name__)

CHANNEL_KIND = "email"
EVENT_MARKER = "X-Curie-Event:"
EMPTY_REPLY_TEXT = "Curie processed your message but produced no text"

# Bounds on the two maps that grow with traffic. `seen` is the one an
# unauthenticated stranger can grow (every polled id is inserted before the
# allow-list decision), so an unbounded set is a memory-growth path anyone who
# learns a public inbox address can walk. Eviction costs one redundant ingress
# POST, which the platform answers as a duplicate.
SEEN_MAX = 5000
REPLIED_MAX = 1000

# The messages whose body fetch failed and is waiting on another attempt. Bounded
# FIFO like the other two: it is fed from the poll listing, so the same stranger
# who can grow `seen` can grow this one. One pass admits at most
# POLL_LIMIT * POLL_MAX_PAGES messages, so the bound is that with headroom.
RETRY_MAX = 200

# Total body fetches one message gets before it is abandoned. A provider that
# will not serve a body is not a transient failure after a handful of tries, and
# without a budget the message is refetched on every pass forever: at the 30
# second HTTP timeout in agentmail.py a hundred such messages hold the sole
# poller for the best part of an hour and starve healthy mail.
BODY_ATTEMPT_MAX = 5

# The provider verdicts that reject a message outright. `trash` is deliberately
# absent: it is an operator action on their own mailbox, not a verdict about the
# sender. `sent` is the adapter's own outbound mail and is skipped earlier,
# without a rejection warning.
REJECTED_LABELS = frozenset({"unauthenticated", "spam", "blocked"})

PRIME_LIMIT = 50
POLL_LIMIT = 20

# How many listing pages one poll pass will walk before giving up and leaving the
# rest to the next pass. Following `next_page_token` is what stops a flood of
# newer mail from starving the message behind it, but an unbounded walk against a
# large or hostile inbox is its own denial of service: it holds the poll loop for
# as long as the provider keeps handing out pages. Five pages is POLL_LIMIT * 5
# messages of catch-up per pass, which drains a burst in a few passes without
# letting any single pass run unbounded. The token the pass stopped on is carried
# to the next one (`page_cursor`), because a cap that throws the token away moves
# the starvation rather than removing it: every later pass restarts at page one,
# finds seen mail there, and stops.
POLL_MAX_PAGES = 5

# The listing backoff after a provider 429, so a rate limit does not become a
# tight loop against a third-party API.
BACKOFF_STEP_SECONDS = 5.0
BACKOFF_MAX_SECONDS = 60.0


class MailAdapter:
    """One AgentMail inbox bridged to one Curie channel binding."""

    def __init__(self, config: MailAdapterConfig, client: AgentMailClient | None = None) -> None:
        self.config = config
        self.client = client if client is not None else AgentMailClient(config)
        # message_ids already polled, bounded FIFO.
        self.seen: OrderedDict[str, bool] = OrderedDict()
        # conversation_id (AgentMail thread_id) -> {"text": the latest reply text}.
        # Written only by handle_inbound, only after both inbound checks pass, so
        # a forged completion naming a rejected sender's thread finds no record.
        self.conversations: dict[str, dict[str, str | None]] = {}
        # event_ids already replied to, bounded FIFO (the fast dedupe path).
        self.replied_event_ids: OrderedDict[str, bool] = OrderedDict()
        # event_ids whose send is in progress and whose outcome is not yet known.
        self.in_flight: set[str] = set()
        # message_id -> (listing summary, body fetches spent), bounded FIFO. The
        # retry is driven off this map rather than off the listing, so it does
        # not depend on the message coming round to a readable page again.
        self.body_retry: OrderedDict[str, tuple[dict[str, Any], int]] = OrderedDict()
        # The `next_page_token` the last pass stopped on, or None to start at the
        # newest page. One token, so the resume state is a single string.
        self.page_cursor: str | None = None
        self.shutdown = threading.Event()
        self.lock = threading.Lock()

    # -- ingress ------------------------------------------------------------

    def prime(self) -> None:
        """Mark everything already in the inbox seen: this is a live adapter, not a backfill.

        A restart therefore drops mail that arrived while the pod was down, which
        is the documented behavior and not an oversight.

        Fail-closed: returning on a failed listing would leave `seen` empty and the
        poll loop would then post the whole backlog as new turns, so a stale
        allow-listed message would trigger an agent turn on every restart. The
        listing is retried, with the same backoff the poll loop uses, until it
        succeeds or shutdown is requested. Nothing is polled until it does.
        """
        backoff = 0.0
        while not self.shutdown.is_set():
            status, page = self.client.list_messages(PRIME_LIMIT)
            if status == 200 and isinstance(page, dict):
                for message in page.get("messages", []):
                    self._mark_seen(str(message["message_id"]))
                logger.info("prime: %d pre-existing message(s) marked seen", len(self.seen))
                return
            backoff = min(backoff * 2 + BACKOFF_STEP_SECONDS, BACKOFF_MAX_SECONDS)
            logger.warning(
                "prime: list -> %s %s; retrying in %ss, nothing is polled until it succeeds",
                status,
                str(page)[:200],
                backoff,
            )
            self.shutdown.wait(backoff)

    def poll_loop(self) -> None:
        """Prime once, then poll until shutdown, backing off on a provider 429."""
        self.prime()
        backoff = 0.0
        while not self.shutdown.is_set():
            self.shutdown.wait(self.config.poll_interval_seconds + backoff)
            if self.shutdown.is_set():
                return
            status = self.poll_once()
            if status == 429:
                backoff = min(backoff * 2 + BACKOFF_STEP_SECONDS, BACKOFF_MAX_SECONDS)
                logger.warning("poll: 429 rate limited, backing off %ss", backoff)
            else:
                backoff = 0.0

    def poll_once(self) -> int:
        """One listing pass. Returns the provider's list status.

        The listing is "most recent first" and paginated, so reading only the
        newest page and stopping starves an older message behind a page of newer
        traffic: the newer mail occupies that page on every later poll too.
        Pagination is followed until a page carries mail already seen, or until
        POLL_MAX_PAGES bounds the catch-up, and a pass stopped by that bound
        resumes from the token it stopped on rather than from the newest page.
        """
        self._retry_bodies()
        status = 200
        pending: list[dict[str, Any]] = []
        page_token = self.page_cursor
        for _ in range(POLL_MAX_PAGES):
            status, page = self.client.list_messages(POLL_LIMIT, page_token)
            if status != 200 or not isinstance(page, dict):
                logger.warning("poll: list -> %s %s", status, str(page)[:200])
                # A token the provider will not read is not worth resuming from
                # on every later pass; the next one starts at the newest page.
                self.page_cursor = None
                return status
            messages = list(page.get("messages", []))
            pending.extend(messages)
            page_token = page.get("next_page_token") or None
            if page_token is None or any(str(m["message_id"]) in self.seen for m in messages):
                page_token = None  # caught up: the next pass starts at the top
                break
        else:
            logger.warning(
                "poll: stopped after %d pages with more to read; the next pass resumes here",
                POLL_MAX_PAGES,
            )
        self.page_cursor = page_token
        for message in reversed(pending):
            message_id = str(message["message_id"])
            if "sent" in _labels(message):
                continue  # our own outbound reply echoing back
            if message_id in self.seen:
                continue
            self._mark_seen(message_id)
            try:
                if not self.handle_inbound(message):
                    # A transient failure must not burn the message. It stays in
                    # `seen` and is retried by id: releasing it instead leaves the
                    # retry to a listing position that may never come round again.
                    self._defer_body(message_id, message, 1)
            except Exception:
                logger.exception("poll: handling %s failed", message_id)
        return status

    def _retry_bodies(self) -> None:
        """Re-attempt the messages whose body fetch failed, within their budget."""
        with self.lock:
            waiting = list(self.body_retry.items())
        for message_id, (message, attempts) in waiting:
            settled = True
            try:
                settled = self.handle_inbound(message)
            except Exception:
                logger.exception("poll: retrying %s failed", message_id)
            if settled:
                with self.lock:
                    self.body_retry.pop(message_id, None)
                continue
            if attempts + 1 >= BODY_ATTEMPT_MAX:
                logger.error(
                    "body fetch for %s failed %d times; abandoning it", message_id, attempts + 1
                )
                with self.lock:
                    self.body_retry.pop(message_id, None)
                continue
            self._defer_body(message_id, message, attempts + 1)

    def handle_inbound(self, message: dict[str, Any]) -> bool:
        """Gate one polled message, then post it to the platform's channel ingress.

        Returns False when the message must be tried again on a later poll (a
        transient provider failure), and True when it is settled: posted, or
        rejected by one of the two inbound checks.
        """
        message_id = str(message["message_id"])
        conversation_id = str(message.get("thread_id") or message_id)
        labels = _labels(message)
        if not self.provider_authenticated(labels):
            logger.warning(
                "rejected message_id=%s: provider labels %s",
                message_id,
                ", ".join(sorted(set(labels) & REJECTED_LABELS)),
            )
            return True
        sender = str(message.get("from") or "")
        if not self.sender_allowed(sender):
            logger.warning(
                "rejected message_id=%s: sender %r is not on CURIE_MAIL_ALLOWED_SENDERS",
                message_id,
                sender,
            )
            return True

        status, full = self.client.get_message(message_id)
        if status != 200 or not isinstance(full, dict):
            # Posting the subject alone would hand the agent a permanently
            # truncated turn that no later poll repairs.
            logger.warning(
                "body fetch for %s -> %s %s; leaving it for the next poll",
                message_id,
                status,
                str(full)[:200],
            )
            return False
        # `text` and `preview` are absent on an HTML-only forward (Gmail and
        # Outlook send those), and the provider's guidance is to treat `html` as
        # the primary content source. https://docs.agentmail.to/messages
        body = (
            full.get("extracted_text")
            or full.get("text")
            or full.get("extracted_html")
            or full.get("html")
            or ""
        )
        with self.lock:
            # setdefault, not assignment: a second message in a thread whose first
            # turn has already emitted its answer must not throw that text away,
            # or turn one sends the empty fallback instead.
            self.conversations.setdefault(conversation_id, {"text": None})
        logger.info("inbound message_id=%s conversation_id=%s", message_id, conversation_id)
        self.post_turn(
            {
                "kind": CHANNEL_KIND,
                "address": self.config.agentmail_inbox,
                "delivery_id": message_id,
                "conversation_id": conversation_id,
                "author": _bare_address(sender) or "unknown@unknown",
                "text": f"{message.get('subject') or ''}\n\n{body}",
                "reply_ref": message_id,
            }
        )
        return True

    def provider_authenticated(self, labels: Iterable[str]) -> bool:
        """Whether the provider's own verdict admits this message.

        Curie authenticates no sender itself; this consumes AgentMail's decision.
        """
        return not set(labels) & REJECTED_LABELS

    def sender_allowed(self, from_header: str) -> bool:
        """Whether the `From` header matches the configured allow-list.

        A filter on an attacker-controlled header, not authentication. An entry is
        a full address, a bare domain (no subdomain matching), or the literal `*`.
        """
        address = _bare_address(from_header)
        domain = address.rpartition("@")[2]
        for entry in self.config.allowed_senders:
            candidate = entry.strip().lower()
            if candidate == "*":
                return True
            if "@" in candidate:
                if candidate == address:
                    return True
            elif candidate and candidate == domain:
                return True
        return False

    def post_turn(self, turn: dict[str, Any]) -> None:
        """One ingress POST, retried on TRANSPORT failure only.

        Retry is safe because the platform keys idempotency on `delivery_id`; a
        response that arrived is final, duplicate or not, and is never re-sent.
        """
        url = f"{self.config.api_base_url.rstrip('/')}/channels/turns"
        headers = {"X-API-Key": self.config.channel_token}
        for attempt in range(1, self.config.ingress_attempts + 1):
            status, out = request("POST", url, turn, headers)
            if status == 0:
                logger.warning("ingress transport failure on attempt %d: %s", attempt, out)
                time.sleep(self.config.ingress_retry_delay_seconds)
                continue
            logger.info("ingress %s for delivery_id=%s", status, turn["delivery_id"])
            return
        logger.error("ingress unreachable; dropped delivery_id=%s", turn["delivery_id"])

    # -- egress -------------------------------------------------------------

    def record_text(self, conversation_id: str, text: str | None, *, append: bool = False) -> None:
        """A `reply.update` edits in place (latest wins); a `reply.post` appends.

        Text is keyed by conversation, not by message, because the platform keeps
        one live session per conversation and `reply.post` accumulates within it.
        """
        if not conversation_id or not text:
            return
        with self.lock:
            record = self.conversations.get(conversation_id)
            if record is None:
                logger.info("no inbound record for conversation_id=%s; ignoring", conversation_id)
                return
            existing = record["text"]
            record["text"] = f"{existing}\n\n{text}" if append and existing else text

    def _clear_text(self, conversation_id: str) -> None:
        """Drop the text a send just emailed; the record itself stays.

        The two halves have to hold together: the record is not reset on inbound
        (setdefault above), so an answer already emitted by an in-flight turn is
        never erased by the next message arriving, and it is cleared here, once
        the provider has actually accepted it, so a later turn that only appends
        (a `reply.post` approval card is the ordinary one) does not email the
        previous turn's answer a second time above its own.
        """
        with self.lock:
            record = self.conversations.get(conversation_id)
            if record is not None:
                record["text"] = None

    def thread_carries(self, conversation_id: str, event_id: str) -> bool | None:
        """The durable half of the dedupe: is this event's marker already in the thread?

        None means the thread could not be read at all, which is not the same
        answer as "not present": treating it as absent sends the correspondent a
        second copy once the fast path has been lost to a restart or an eviction.
        """
        status, thread = self.client.get_thread(conversation_id)
        if status != 200 or not isinstance(thread, dict):
            logger.warning("thread listing -> %s; the durable dedupe check could not run", status)
            return None
        marker = f"{EVENT_MARKER} {event_id}"
        for message in thread.get("messages", []):
            for field in ("extracted_text", "text", "preview"):
                if marker in (message.get(field) or ""):
                    return True
        return False

    def send_reply(self, event_id: str, conversation_id: str, reply_ref: str | None) -> int:
        """Send one threaded reply. Returns the status the egress endpoint must ack.

        200 means the platform can consider the completion delivered, 502 that the
        send did not happen and the turn should be retried, 503 that a concurrent
        duplicate is still in flight and the outcome is not yet known.
        Nothing is recorded as replied until the provider has accepted it.
        """
        with self.lock:
            if event_id in self.replied_event_ids:
                logger.info("reply skipped: event_id=%s already replied to", event_id)
                return 200
            if event_id in self.in_flight:
                logger.info("reply deferred: event_id=%s is already in flight", event_id)
                return 503
            if not reply_ref:
                logger.info("reply skipped: event_id=%s carries no reply_ref", event_id)
                return 200
            record = self.conversations.get(conversation_id)
            text = None if record is None else record["text"]
            self.in_flight.add(event_id)

        try:
            # The durable check runs before the record gate, not after it. The
            # record is process-local and a restart erases it, so answering 502
            # off a missing record alone refuses an already-delivered completion
            # on every redelivery until the platform dead-letters it. The marker
            # in the thread outlives the process and is the better answer.
            carries = self.thread_carries(conversation_id, event_id)
            if carries is None:
                logger.warning(
                    "reply not sent: the thread for event_id=%s could not be read", event_id
                )
                return 502
            if carries:
                logger.info("reply skipped: thread already carries event_id=%s", event_id)
                self._mark_replied(event_id)
                return 200
            if record is None:
                # The thread does not prove delivery, so this is either a forged
                # conversation_id or a restart that erased the record before the
                # send, and the adapter cannot tell them apart. Nothing was sent,
                # so acking 200 would make the worker clear its durable
                # completion record and lose the email with no dead letter.
                logger.warning("reply not sent: no record for conversation_id=%s", conversation_id)
                return 502
            body = f"{text or EMPTY_REPLY_TEXT}\n\n{EVENT_MARKER} {event_id}"
            status, out = self.client.reply(reply_ref, body)
            if 200 <= status < 300:
                logger.info("reply sent for event_id=%s in_reply_to=%s", event_id, reply_ref)
                self._mark_replied(event_id)
                self._clear_text(conversation_id)
                return 200
            logger.warning(
                "reply for event_id=%s failed at the provider: %s %s",
                event_id,
                status,
                str(out)[:200],
            )
            return 502
        finally:
            # Every exit path, the unexpected-exception one included: a leaked
            # event_id makes every later redelivery of that turn take the
            # in-flight branch and never send.
            with self.lock:
                self.in_flight.discard(event_id)

    # -- bounded state ------------------------------------------------------

    def _mark_seen(self, message_id: str) -> None:
        with self.lock:
            self.seen[message_id] = True
            while len(self.seen) > SEEN_MAX:
                self.seen.popitem(last=False)

    def _defer_body(self, message_id: str, message: dict[str, Any], attempts: int) -> None:
        with self.lock:
            self.body_retry[message_id] = (message, attempts)
            while len(self.body_retry) > RETRY_MAX:
                self.body_retry.popitem(last=False)

    def _mark_replied(self, event_id: str) -> None:
        with self.lock:
            self.replied_event_ids[event_id] = True
            while len(self.replied_event_ids) > REPLIED_MAX:
                self.replied_event_ids.popitem(last=False)


def _labels(message: dict[str, Any]) -> list[str]:
    return [str(label).strip().lower() for label in (message.get("labels") or [])]


def _bare_address(from_header: str) -> str:
    """The address out of a `From` header, lowercased. `parseaddr` returns "" on garbage."""
    return email.utils.parseaddr(from_header)[1].strip().lower()
