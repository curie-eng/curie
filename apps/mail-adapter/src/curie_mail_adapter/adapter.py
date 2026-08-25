"""AgentMail ingress and email egress backed by one durable local state store."""

from __future__ import annotations

import email.utils
import hashlib
import logging
import secrets
import threading
import time
from collections import OrderedDict
from collections.abc import Iterable
from typing import Any

from .agentmail import AgentMailClient, request
from .config import MailAdapterConfig
from .state import MailState

logger = logging.getLogger(__name__)

CHANNEL_KIND = "email"
EVENT_MARKER = "X-Curie-Event:"
EMPTY_REPLY_TEXT = "Curie processed your message but produced no text"

SEEN_MAX = 5000
BODY_ATTEMPT_MAX = 5
PRIME_LIMIT = 50
POLL_LIMIT = 20
POLL_MAX_PAGES = 5
BACKOFF_STEP_SECONDS = 5.0
BACKOFF_MAX_SECONDS = 60.0
REJECTED_LABELS = frozenset({"unauthenticated", "spam", "blocked"})


class MailAdapter:
    """One AgentMail inbox bridged to one Curie channel binding."""

    def __init__(self, config: MailAdapterConfig, client: AgentMailClient | None = None) -> None:
        self.config = config
        self.client = client if client is not None else AgentMailClient(config)
        self.state = MailState(
            config.state_path,
            max_pending=config.max_pending_deliveries,
            max_bytes=config.max_state_bytes,
        )
        self.shutdown = threading.Event()
        self.ready = threading.Event()
        self.lock = self.state.lock
        self.owner = secrets.token_hex(16)
        self.seen: OrderedDict[str, bool] = OrderedDict()
        for message_id in self.state.known_message_ids():
            self._mark_seen(message_id)
        # Opaque pagination is a discovery hint only. Durable pending ids, never
        # this cursor, are the source of truth across a restart.
        self.page_cursor: str | None = None

    # -- startup and ingress ------------------------------------------------

    def startup(self) -> None:
        """Complete the one-time prime or restart confirmation, then become ready."""
        self.prime()
        if not self.shutdown.is_set():
            self.ready.set()

    def prime(self) -> None:
        """Prime only a new store; a restart confirms and resumes instead."""
        if self.state.is_primed():
            self._confirm_restart()
            return

        backoff = 0.0
        while not self.shutdown.is_set():
            page_token: str | None = None
            staged: list[dict[str, Any]] = []
            succeeded = True
            seen_tokens: set[str] = set()
            while not self.shutdown.is_set():
                status, page = self.client.list_messages(PRIME_LIMIT, page_token)
                if status != 200 or not isinstance(page, dict):
                    succeeded = False
                    logger.warning("prime: list failed with status=%s", status)
                    break
                staged.extend(item for item in page.get("messages", []) if isinstance(item, dict))
                next_token = page.get("next_page_token") or None
                if next_token is None:
                    break
                next_token = str(next_token)
                if next_token in seen_tokens:
                    succeeded = False
                    logger.warning("prime: provider repeated a pagination token")
                    break
                seen_tokens.add(next_token)
                page_token = next_token
            if succeeded:
                for message in staged:
                    if "sent" in _labels(message):
                        continue
                    message_id = str(message.get("message_id") or "")
                    if not message_id:
                        logger.warning("prime: ignoring provider item without a message id")
                        continue
                    rejection = self._listing_rejection(message)
                    admission = self.state.record_terminal(
                        message_id, "rejected" if rejection is not None else "primed"
                    )
                    if admission == "full":
                        succeeded = False
                        logger.error("prime: durable state capacity reached before completion")
                        break
                    self._mark_seen(message_id)
                    if admission == "admitted" and rejection is not None:
                        self._log_listing_rejection(message_id, rejection)
            if succeeded:
                self.state.finish_prime()
                logger.info("prime: %d pre-existing message(s) recorded", len(staged))
                return
            backoff = min(backoff * 2 + BACKOFF_STEP_SECONDS, BACKOFF_MAX_SECONDS)
            logger.warning("prime: retrying in %ss; readiness remains false", backoff)
            self.shutdown.wait(backoff)

    def _confirm_restart(self) -> None:
        """Make one successful provider pass before declaring a replacement ready."""
        backoff = 0.0
        while not self.shutdown.is_set():
            status = self.poll_once()
            if status == 200:
                return
            backoff = min(backoff * 2 + BACKOFF_STEP_SECONDS, BACKOFF_MAX_SECONDS)
            logger.warning("restart confirmation -> %s; retrying in %ss", status, backoff)
            self.shutdown.wait(backoff)

    def poll_loop(self) -> None:
        self.startup()
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
        """Retry durable work, then perform one bounded discovery pass."""
        self._retry_pending()
        status = 200
        pending: list[dict[str, Any]] = []
        page_token = self.page_cursor
        for _ in range(POLL_MAX_PAGES):
            status, page = self.client.list_messages(POLL_LIMIT, page_token)
            if status != 200 or not isinstance(page, dict):
                logger.warning("poll: list failed with status=%s", status)
                self.page_cursor = None
                return status
            messages = [item for item in page.get("messages", []) if isinstance(item, dict)]
            pending.extend(messages)
            page_token = page.get("next_page_token") or None
            if page_token is None or any(
                str(item.get("message_id")) in self.seen for item in messages
            ):
                page_token = None
                break
            page_token = str(page_token)
        else:
            logger.warning("poll: page budget reached; the next pass resumes discovery")
        self.page_cursor = page_token

        for message in reversed(pending):
            if "sent" in _labels(message):
                continue
            message_id = str(message.get("message_id") or "")
            if not message_id or message_id in self.seen:
                continue
            rejection = self._listing_rejection(message)
            if rejection is not None:
                admission = self.state.record_terminal(message_id, "rejected")
                if admission == "full":
                    logger.warning(
                        "back-pressure: refusing correlation=%s before acceptance or mark-seen",
                        _correlation(message_id),
                    )
                    continue
                self._mark_seen(message_id)
                if admission == "admitted":
                    self._log_listing_rejection(message_id, rejection)
                continue
            admission = self.state.admit(message)
            if admission == "full":
                logger.warning(
                    "back-pressure: refusing correlation=%s before acceptance or mark-seen",
                    _correlation(message_id),
                )
                continue
            self._mark_seen(message_id)
            if admission == "known":
                known = self.state.delivery(message_id)
                if known and known["state"] == "accepted" and known["turn"] is not None:
                    self._deliver_turn(message_id, known["turn"])
                continue
            try:
                self.handle_inbound(message)
            except Exception:
                logger.error(
                    "poll: handling correlation=%s failed unexpectedly",
                    _correlation(message_id),
                )
        return status

    def _retry_pending(self) -> None:
        for pending in self.state.pending():
            try:
                if pending["state"] == "body_pending":
                    self.handle_inbound(pending["summary"])
                elif pending["turn"] is not None:
                    self._deliver_turn(pending["message_id"], pending["turn"])
            except Exception:
                logger.error(
                    "poll: retrying correlation=%s failed unexpectedly",
                    _correlation(pending["message_id"]),
                )

    def handle_inbound(self, message: dict[str, Any]) -> bool:
        """Gate, fetch, durably admit, and attempt one provider message."""
        message_id = str(message["message_id"])
        conversation_id = str(message.get("thread_id") or message_id)
        labels = _labels(message)
        if not self.provider_authenticated(labels):
            logger.warning(
                "rejected correlation=%s: provider labels %s",
                _correlation(message_id),
                ", ".join(sorted(set(labels) & REJECTED_LABELS)),
            )
            self.state.settle_without_turn(message_id, "rejected")
            return True
        sender = str(message.get("from") or "")
        if not self.sender_allowed(sender):
            logger.warning(
                "rejected correlation=%s: sender is not on CURIE_MAIL_ALLOWED_SENDERS",
                _correlation(message_id),
            )
            self.state.settle_without_turn(message_id, "rejected")
            return True

        status, full = self.client.get_message(message_id)
        if status != 200 or not isinstance(full, dict):
            if isinstance(full, dict) and full.get("error") == (
                "response body exceeds configured byte limit"
            ):
                self.state.settle_without_turn(message_id, "oversize")
                logger.warning(
                    "body correlation=%s exceeds CURIE_MAIL_MAX_BODY_BYTES",
                    _correlation(message_id),
                )
                return True
            backing_off = self.state.body_failed(message_id, abandon_after=BODY_ATTEMPT_MAX)
            logger.warning(
                "body fetch correlation=%s failed with status=%s; %s",
                _correlation(message_id),
                status,
                "backing off while pending" if backing_off else "leaving pending",
            )
            return backing_off
        body = (
            full.get("extracted_text")
            or full.get("text")
            or full.get("extracted_html")
            or full.get("html")
            or ""
        )
        text = f"{message.get('subject') or ''}\n\n{body}"
        if len(text.encode("utf-8")) > self.config.max_body_bytes:
            self.state.settle_without_turn(message_id, "oversize")
            logger.warning(
                "body correlation=%s exceeds CURIE_MAIL_MAX_BODY_BYTES",
                _correlation(message_id),
            )
            return True
        turn = {
            "kind": CHANNEL_KIND,
            "address": self.config.agentmail_inbox,
            "delivery_id": message_id,
            "conversation_id": conversation_id,
            "author": _bare_address(sender) or "unknown@unknown",
            "text": text,
            "reply_ref": message_id,
        }
        self.state.store_turn(message_id, turn)
        logger.info("inbound admitted correlation=%s", _correlation(message_id))
        return self._deliver_turn(message_id, turn)

    def _listing_rejection(self, message: dict[str, Any]) -> tuple[str, str] | None:
        """Return the first failed listing gate without retaining its payload."""
        labels = _labels(message)
        rejected_labels = ", ".join(sorted(set(labels) & REJECTED_LABELS))
        if rejected_labels:
            return ("provider", rejected_labels)
        if not self.sender_allowed(str(message.get("from") or "")):
            return ("sender", "")
        return None

    def _log_listing_rejection(
        self, message_id: str, rejection: tuple[str, str]
    ) -> None:
        gate, detail = rejection
        if gate == "provider":
            logger.warning(
                "rejected correlation=%s: provider labels %s",
                _correlation(message_id),
                detail,
            )
            return
        logger.warning(
            "rejected correlation=%s: sender is not on CURIE_MAIL_ALLOWED_SENDERS",
            _correlation(message_id),
        )

    def _deliver_turn(self, message_id: str, turn: dict[str, Any]) -> bool:
        settled = self.post_turn(turn)
        if settled:
            self.state.accept_ingress(message_id)
        else:
            self.state.defer_ingress(message_id, 0.0)
        return settled

    def provider_authenticated(self, labels: Iterable[str]) -> bool:
        return not set(labels) & REJECTED_LABELS

    def sender_allowed(self, from_header: str) -> bool:
        address = _bare_address(from_header)
        domain = address.rpartition("@")[2]
        for entry in self.config.allowed_senders:
            candidate = entry.strip().lower()
            if candidate == "*" or candidate == address:
                return True
            if "@" not in candidate and candidate and candidate == domain:
                return True
        return False

    def post_turn(self, turn: dict[str, Any]) -> bool:
        """Return True only for the platform's terminal 200 admission."""
        url = f"{self.config.api_base_url.rstrip('/')}/channels/turns"
        headers = {"X-API-Key": self.config.channel_token}
        for attempt in range(1, self.config.ingress_attempts + 1):
            result = request(
                "POST",
                url,
                turn,
                headers,
                max_response_bytes=self.config.max_body_bytes,
            )
            if result.status == 0:
                logger.warning("ingress transport failure on attempt=%d", attempt)
                if attempt < self.config.ingress_attempts:
                    time.sleep(self.config.ingress_retry_delay_seconds)
                continue
            logger.info(
                "ingress status=%s correlation=%s",
                result.status,
                _correlation(str(turn["delivery_id"])),
            )
            if result.status == 200:
                return True
            if result.status == 429:
                retry_after = _retry_after_seconds(result.headers)
                if retry_after > 0:
                    self.shutdown.wait(retry_after)
            return False
        logger.warning(
            "ingress unreachable; correlation=%s remains pending",
            _correlation(str(turn["delivery_id"])),
        )
        return False

    # -- egress -------------------------------------------------------------

    def record_text(
        self,
        conversation_id: str,
        reply_ref: str | None,
        text: str | None,
        *,
        append: bool = False,
    ) -> int:
        """Persist text against the exact reply ref; return the HTTP ack status."""
        if not conversation_id or not text:
            return 200
        chosen_ref = reply_ref
        if not chosen_ref:
            refs = self.state.live_reply_refs(conversation_id)
            if len(refs) != 1:
                logger.info(
                    "reply post deferred: correlation=%s has %d live reply refs",
                    _correlation(conversation_id),
                    len(refs),
                )
                return 503
            chosen_ref = refs[0]
        outcome = self.state.record_text(
            conversation_id,
            chosen_ref,
            text,
            append=append,
            max_bytes=self.config.max_reply_bytes,
        )
        if outcome == "too_large":
            return 413
        if outcome == "missing":
            logger.info(
                "reply update deferred: no active admitted owner for correlation=%s",
                _correlation(f"{conversation_id}\0{chosen_ref}"),
            )
            return 503
        return 200

    def thread_carries(self, conversation_id: str, event_id: str) -> bool | None:
        status, thread = self.client.get_thread(conversation_id)
        if status != 200 or not isinstance(thread, dict):
            logger.warning("thread listing -> %s; durable witness unreadable", status)
            return None
        marker = f"{EVENT_MARKER} {event_id}"
        for message in thread.get("messages", []):
            if not isinstance(message, dict):
                continue
            for field in ("extracted_text", "text", "preview"):
                if marker in str(message.get(field) or ""):
                    return True
        return False

    def send_reply(self, event_id: str, conversation_id: str, reply_ref: str | None) -> int:
        """Apply the provider-witness four-way recovery decision."""
        if not reply_ref:
            logger.info(
                "reply skipped: correlation=%s carries no reply_ref",
                _correlation(event_id),
            )
            return 200
        claim = self.state.claim_event(event_id, conversation_id, reply_ref, self.owner)
        if claim == "done":
            return 200
        if claim == "busy":
            return 503
        try:
            carries = self.thread_carries(conversation_id, event_id)
            if carries is None:
                return 502
            if carries:
                self.state.finish_event(event_id)
                self.state.finish_reply(conversation_id, reply_ref)
                return 200
            exists, text = self.state.reply_text(conversation_id, reply_ref)
            if not exists:
                logger.warning(
                    "reply not sent: no admitted record for correlation=%s",
                    _correlation(f"{conversation_id}\0{reply_ref}"),
                )
                return 502
            body = f"{text or EMPTY_REPLY_TEXT}\n\n{EVENT_MARKER} {event_id}"
            if len(body.encode("utf-8")) > self.config.max_reply_bytes:
                logger.warning(
                    "reply correlation=%s exceeds CURIE_MAIL_MAX_REPLY_BYTES",
                    _correlation(event_id),
                )
                return 502
            status, _out = self.client.reply(reply_ref, body)
            if 200 <= status < 300:
                self.state.finish_event(event_id)
                if exists:
                    self.state.finish_reply(conversation_id, reply_ref)
                logger.info("reply sent correlation=%s", _correlation(event_id))
                return 200
            logger.warning(
                "reply correlation=%s failed at the provider with status=%s",
                _correlation(event_id),
                status,
            )
            return 502
        finally:
            self.state.release_event(event_id, self.owner)

    # -- bounded fast caches ------------------------------------------------

    def _mark_seen(self, message_id: str) -> None:
        with self.lock:
            self.seen[message_id] = True
            while len(self.seen) > SEEN_MAX:
                self.seen.popitem(last=False)

    def close(self) -> None:
        self.state.close()


def _labels(message: dict[str, Any]) -> list[str]:
    return [str(label).strip().lower() for label in (message.get("labels") or [])]


def _bare_address(from_header: str) -> str:
    return email.utils.parseaddr(from_header)[1].strip().lower()


def _retry_after_seconds(headers: dict[str, str]) -> float:
    value = next((value for key, value in headers.items() if key.lower() == "retry-after"), "0")
    try:
        delay = float(value)
    except ValueError:
        try:
            delay = email.utils.parsedate_to_datetime(value).timestamp() - time.time()
        except (TypeError, ValueError, OverflowError):
            delay = 0.0
    return min(BACKOFF_MAX_SECONDS, max(0.0, delay))


def _correlation(value: str) -> str:
    """A stable one-way token for joining logs without exposing provider identifiers."""
    return hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()[:16]
